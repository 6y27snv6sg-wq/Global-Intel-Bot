import asyncio
import logging
import os
import re
import time
import urllib.parse
from typing import Dict, List, Any, Set

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

from google import genai
from google.genai import types

from news_engine import (
    collect_news,
    search_news,
    build_ai_context,
)


# ============================================================
# SETTINGS & LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)

log = logging.getLogger("optimized_news_bot")

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()

GEMINI_MODEL = "gemini-3.5-flash"

NEWS_COLLECTION_TIMEOUT = 20
GEMINI_TIMEOUT = 30
MAX_SEARCH_RESULTS = 25
CACHE_TTL = 300  # 5 دقائق

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")

if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY is missing")

ai_client = genai.Client(api_key=GEMINI_API_KEY)


# ============================================================
# STATE & CACHE MANAGEMENT
# ============================================================

class SimpleCache:
    def __init__(self, ttl: int = 300):
        self.ttl = ttl
        self._cache: Dict[str, Any] = {}
        self._timestamps: Dict[str, float] = {}

    def get(self, key: str):
        if key in self._cache:
            if time.time() - self._timestamps[key] < self.ttl:
                return self._cache[key]
            else:
                del self._cache[key]
                del self._timestamps[key]
        return None

    def set(self, key: str, value: Any):
        self._cache[key] = value
        self._timestamps[key] = time.time()


NEWS_CACHE = SimpleCache(ttl=CACHE_TTL)
USER_LOCKS: Dict[int, asyncio.Lock] = {}

# قائمة المستخدمين المكتومين للتنبيهات العاجلة
MUTED_USERS: Set[int] = set()


# ============================================================
# LINK SANITIZER (منع تشوه الشاشة واستبدال المعطل بالبحث)
# ============================================================

def build_safe_link(title: str, source: str, raw_url: str) -> str:
    """استبدال الروابط المعطلة برابط بحث آمن ومباشر في محرك البحث"""
    if raw_url and "news.google.com" not in raw_url and raw_url.startswith("http"):
        return raw_url
    
    # تحويل الرابط التوجيهي أو المعطل إلى رابط بحث آمن في Google Search
    query = f"{title} {source}"
    encoded_query = urllib.parse.quote_plus(query)
    return f"https://www.google.com/search?q={encoded_query}"


# ============================================================
# TOPICS & KEYBOARDS (إضافة المال والاقتصاد)
# ============================================================

TOPICS = {
    "urgent": ("🚨 عاجل وبيانات رسمية", "تصريح رسمي عاجل بيانات وزارة الخارجية"),
    "economy": ("📈 الاقتصاد والمال", "الاقتصاد الأسواق التضخم الفائدة البنك المركزي الأسهم"),
    "foreign": ("🏛 الخارجية والدبلوماسية", "وزارة الخارجية تصريح بيان رسمي دبلوماسي"),
    "gulf": ("🇸🇦 الخليج والشرق الأوسط", "أخبار الخليج العربي الخارجية السعودية الإمارات"),
    "energy": ("⚡ النفط والطاقة", "أخبار النفط أوبك بلس وزارة الطاقة أسواق الخام"),
    "world": ("🌍 العالم والسياسة", "أخبار العالم التصريحات الدولية القمة"),
    "security": ("🛡 الدفاع والأمن", "أخبار الدفاع الأمن العسكري الاتفاقيات الأمنية"),
}


def main_keyboard(user_id: int):
    rows = []
    items = list(TOPICS.items())

    for i in range(0, len(items), 2):
        row = [InlineKeyboardButton(label, callback_data=f"topic:{key}:1") for key, (label, _) in items[i:i + 2]]
        rows.append(row)

    # زر التحكم بالتنبيهات العاجلة (تفعيل / إيقاف)
    is_muted = user_id in MUTED_USERS
    alert_btn_text = "🔔 تفعيل التنبيهات العاجلة" if is_muted else "🔕 إيقاف التنبيهات العاجلة"
    alert_action = "unmute_alerts" if is_muted else "mute_alerts"

    rows.append([InlineKeyboardButton(alert_btn_text, callback_data=alert_action)])
    rows.append([InlineKeyboardButton("🔄 تحديث الأخبار", callback_data="refresh")])
    return InlineKeyboardMarkup(rows)


# ============================================================
# GEMINI ON-DEMAND ENGINE (تحليل عند الطلب فقط)
# ============================================================

ANALYSIS_PROMPT = """
أنت محرر ومحلل أخبار تنفيذي.
قم بتحليل القائمة التالية بأسلوب دقيق ومختصر جداً:

🎯 **الخلاصة والتحليل السياسي/الاقتصادي:**
• أهم حدث والدلالة المباشرة له.
• التأثير المتوقع أو البعد المستقبلي.

⚠️ اقتصر على 100 كلمة فقط دون إطالة.
"""

async def analyze_with_gemini(items) -> str:
    """استدعاء Gemini الموفر للتوكنز (عند النقر على زر التحليل فقط)"""
    # نرسل العناوين والمصادر فقط لتقليل التوكنز لأقصى درجة
    context_lines = [f"- {getattr(i, 'title', '')} [{getattr(i, 'source', '')}]" for i in items[:6]]
    context_text = "\n".join(context_lines)

    prompt = f"{ANALYSIS_PROMPT}\nالأخبار:\n{context_text}"

    try:
        response = await asyncio.wait_for(
            asyncio.to_thread(
                ai_client.models.generate_content,
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(thinking_level="low")
                ),
            ),
            timeout=GEMINI_TIMEOUT,
        )
        return (getattr(response, "text", None) or "").strip()
    except Exception as e:
        log.warning(f"Gemini Analysis Error: {e}")
        return "⚠️ تعذر إتمام التحليل حالياً، حاول مجدداً لاحقاً."


# ============================================================
# NEWS ENGINE & REPORT GENERATOR
# ============================================================

async def get_fresh_news():
    cached = NEWS_CACHE.get("all_news")
    if cached is not None:
        return cached

    try:
        items = await asyncio.wait_for(
            collect_news(max_items=100),
            timeout=NEWS_COLLECTION_TIMEOUT,
        )
        if items:
            NEWS_CACHE.set("all_news", items)
        return items or []
    except Exception:
        log.exception("News Collection Error")
        return []


def generate_base_report(items, page: int = 1, per_page: int = 5, is_muted: bool = False):
    """عرض الأخبار بسرعة فائقة عبر البوت وبدون توكنز"""
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    page_items = items[start_idx:end_idx]

    lines = []
    
    # الشريط العلوي للتنبيهات العاجلة
    if not is_muted:
        lines.append("🔔 **[شريط التنبيهات العاجلة: مفعّل]**\n")
    else:
        lines.append("🔕 **[شريط التنبيهات العاجلة: مكتوم]**\n")

    lines.append("📰 **أبرز التغطيات والبيانات:**\n")

    for item in page_items:
        title = getattr(item, "title", "") or getattr(item, "caption", "") or ""
        source = getattr(item, "source", "") or "مصدر رسمي"
        raw_url = getattr(item, "url", "") or getattr(item, "link", "") or ""
        
        safe_url = build_safe_link(title, source, raw_url)

        if title:
            entry = f"• **{title}**\n  📍 *المصدر:* `{source}` | [🔗 قراءة الخبر]({safe_url})"
            lines.append(entry + "\n")

    return "\n".join(lines)


# ============================================================
# HANDLERS
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        "🏛 **منصة الأخبار والبيانات الرسمية الشاملة**\n\n"
        "اختر القطاع المطلوبة أو استخدم خيار التنبيهات والتحليل:",
        reply_markup=main_keyboard(user_id),
        parse_mode="Markdown"
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    user_id = update.effective_user.id

    # التعامل مع أزرار التنبيهات
    if data == "mute_alerts":
        MUTED_USERS.add(user_id)
        await query.message.reply_text("🔕 تم إيقاف التنبيهات العاجلة بنجاح.", reply_markup=main_keyboard(user_id))
        return

    if data == "unmute_alerts":
        MUTED_USERS.discard(user_id)
        await query.message.reply_text("🔔 تم تفعيل التنبيهات العاجلة بنجاح.", reply_markup=main_keyboard(user_id))
        return

    if data == "home":
        await query.message.reply_text("📰 القائمة الرئيسية:", reply_markup=main_keyboard(user_id))
        return

    if data == "refresh":
        NEWS_CACHE.set("all_news", None)
        await query.message.reply_text("🔄 تم تحديث موجز الأخبار بنجاح!", reply_markup=main_keyboard(user_id))
        return

    # زر طلب تحليل الذكاء الاصطناعي (Gemini On-Demand)
    if data.startswith("analyze:"):
        key = data.split(":")[1]
        status = await query.message.reply_text("🧠 جاري إرسال البيانات إلى Gemini لتحليلها وتلخيصها...")
        
        items = await get_fresh_news()
        _, topic_text = TOPICS.get(key, ("", ""))
        results = search_news(items, topic_text, max_results=10) if items else []

        if results:
            analysis = await analyze_with_gemini(results)
            await status.edit_text(f"🧠 **التحليل التنفيذي (Gemini):**\n\n{analysis}", parse_mode="Markdown")
        else:
            await status.edit_text("⚠️ لا توجد بيانات كافية للتحليل.")
        return

    # التصفح العادي للأخبار
    parts = data.split(":")
    if len(parts) < 3 or parts[0] != "topic":
        return

    key, page = parts[1], int(parts[2])
    if key not in TOPICS:
        return

    _, topic_text = TOPICS[key]
    lock = USER_LOCKS.setdefault(user_id, asyncio.Lock())

    if lock.locked():
        await query.message.reply_text("⏳ جاري التحميل...")
        return

    async with lock:
        status = await query.message.reply_text("📡 جاري ترتيب الأخبار وتنقية الروابط...")

        try:
            items = await get_fresh_news()
            results = search_news(items, topic_text, max_results=MAX_SEARCH_RESULTS) if items else []

            if not results:
                await status.edit_text("🔎 لا توجد أخبار جديدة في هذا القطاع حالياً.")
                return

            is_muted = user_id in MUTED_USERS
            report = generate_base_report(results, page=page, per_page=5, is_muted=is_muted)

            total_pages = (len(results) + 4) // 5
            nav_buttons = []
            
            # أزرار الإضافة والتنقل
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton("➕ إضافية (أخبار أكثر)", callback_data=f"topic:{key}:{page+1}"))
            
            # زر استخدام التحليل الذكي عند الطلب فقط
            nav_buttons.append(InlineKeyboardButton("🧠 طلب تحليل الذكاء الاصطناعي", callback_data=f"analyze:{key}"))

            rows = [nav_buttons, [InlineKeyboardButton("🔙 القائمة الرئيسية", callback_data="home")]]

            await status.edit_text(
                report,
                reply_markup=InlineKeyboardMarkup(rows),
                disable_web_page_preview=True,
                parse_mode="Markdown"
            )

        except Exception:
            log.exception("Button Handler Exception")
            await status.edit_text("⚠️حدث خطأ أثناء عرض التقرير.")


async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text:
        return

    user_id = update.effective_user.id
    lock = USER_LOCKS.setdefault(user_id, asyncio.Lock())

    if lock.locked():
        await update.message.reply_text("⏳ جاري البحث...")
        return

    async with lock:
        status = await update.message.reply_text(f"🔎 جاري البحث عن: '{text}'...")

        try:
            items = await get_fresh_news()
            results = search_news(items, text, max_results=MAX_SEARCH_RESULTS) if items else []

            if not results:
                await status.edit_text("🔎 لم أجد نتائج مطابقة لبحثك.")
                return

            is_muted = user_id in MUTED_USERS
            report = generate_base_report(results, page=1, per_page=5, is_muted=is_muted)
            
            await status.edit_text(
                report,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 القائمة الرئيسية", callback_data="home")]]),
                disable_web_page_preview=True,
                parse_mode="Markdown"
            )
        except Exception:
            log.exception("Message Search Exception")
            await status.edit_text("⚠️ حدث خطأ أثناء البحث.")


# ============================================================
# MAIN
# ============================================================

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler, pattern=r"^(topic:|home|refresh|mute_alerts|unmute_alerts|analyze:)"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_user_message))

    log.info("Optimized Bot Started...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
