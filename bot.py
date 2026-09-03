import asyncio
import logging
import os
import re
import time
from typing import Dict, List, Any

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

log = logging.getLogger("news_bot")

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()

GEMINI_MODEL = "gemini-3.5-flash"

NEWS_COLLECTION_TIMEOUT = 20
GEMINI_TIMEOUT = 45
MAX_SEARCH_RESULTS = 25
CACHE_TTL = 300  # 5 دقائق للكاش

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")

if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY is missing")


# ============================================================
# GEMINI CLIENT
# ============================================================

ai_client = genai.Client(api_key=GEMINI_API_KEY)


# ============================================================
# SIMPLE CACHE SYSTEM
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


# ============================================================
# MARKDOWN ESCAPE & UTILS
# ============================================================

def escape_markdown(text: str) -> str:
    """تطهير النصوص لضمان عدم كسر تنسيق Telegram MarkdownV2"""
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", text)


# ============================================================
# TOPICS (المناطق والأقسام المحدثة)
# ============================================================

TOPICS = {
    "urgent": ("🚨 عاجل", "عاجل آخر الأخبار والتطورات"),
    "world": ("🌍 العالم", "أهم أخبار العالم"),
    "gulf": ("🇸🇦 الخليج", "أخبار الخليج العربي"),
    "america": ("🇺🇸 أمريكا", "أخبار الولايات المتحدة"),
    "canada": ("🇨🇦 كندا", "أخبار كندا"),
    "latin": ("🇧🇷 أمريكا الجنوبية", "أخبار أمريكا اللاتينية والجنوبية"),
    "europe": ("🇪🇺 أوروبا", "أخبار أوروبا"),
    "asia": ("🌏 آسيا", "أخبار آسيا"),
    "africa": ("🌍 أفريقيا", "أخبار القارة الأفريقية"),
    "australia": ("🇦🇺 أستراليا", "أخبار أستراليا ونيوزيلندا"),
    "energy": ("⚡ الطاقة", "أخبار النفط والطاقة"),
    "security": ("🛡 الأمن", "أخبار الأمن والدفاع"),
    "foreign": ("🌐 السياسة", "السياسة والعلاقات الدولية"),
}


def main_keyboard():
    rows = []
    items = list(TOPICS.items())

    for i in range(0, len(items), 2):
        rows.append([
            InlineKeyboardButton(label, callback_data=f"topic:{key}:1")
            for key, (label, _) in items[i:i + 2]
        ])

    return InlineKeyboardMarkup(rows)


# ============================================================
# GREETING & NEWS QUERY DETECTION
# ============================================================

GREETING_RE = re.compile(
    r"^\s*("
    r"السلام عليكم|السلام عليكم ورحمة الله وبركاته|"
    r"وعليكم السلام|مرحبا|مرحباً|هلا|هلا والله|"
    r"اهلا|أهلا|أهلين|اهلين|"
    r"صباح الخير|مساء الخير|"
    r"شكرا|شكراً|مشكور|مشكورة|"
    r"يعطيك العافية|يعطيك العافيه|"
    r"تمام|ممتاز|حلو|"
    r"كيفك|كيف حالك|"
    r"hello|hi|hey|thanks|thank you"
    r")\s*[!.،,؟?]*\s*$",
    re.IGNORECASE,
)

NEWS_TERMS = {
    "خبر", "أخبار", "اخبار", "عاجل", "آخر الأخبار", "اخر الاخبار",
    "اليوم", "الآن", "الان", "حدث", "أحداث", "احداث", "الوضع",
    "تطورات", "مستجدات", "سياسة", "سياسي", "حرب", "هجوم", "ضربة",
    "قصف", "صراع", "أزمة", "ازمة", "روسيا", "أوكرانيا", "امريكا",
    "أمريكا", "الصين", "آسيا", "إيران", "ايران", "إسرائيل", "اسرائيل",
    "غزة", "السعودية", "الخليج", "الإمارات", "الامارات", "قطر",
    "الكويت", "البحرين", "عمان", "تركيا", "بريطانيا", "أوروبا",
    "اوروبا", "النفط", "الطاقة", "أوبك", "اوبك", "اقتصاد", "اقتصادية",
    "دولار", "أسواق", "اسواق", "أمن", "امن", "دفاع", "عسكري",
    "عسكرية", "مفاوضات", "اجتماع", "رئيس", "وزير", "وزارة",
    "تصريح", "بيان", "انتخابات", "برلمان", "أفريقيا", "افريقيا",
    "كندا", "أستراليا", "استراليا", "أمريكا الجنوبية", "امريكا الجنوبية",
    "البرازيل", "الأرجنتين", "news", "breaking", "latest", "world",
    "russia", "ukraine", "usa", "america", "china", "iran", "israel",
    "gulf", "oil", "energy", "africa", "canada", "australia",
}


def is_greeting(text: str) -> bool:
    return bool(GREETING_RE.match(text.strip()))


def is_news_query(text: str) -> bool:
    t = text.strip().lower()
    if any(term.lower() in t for term in NEWS_TERMS):
        return True

    patterns = (
        r"\bما الجديد\b", r"\bماذا يحدث\b", r"\bماذا حدث\b",
        r"\bما الذي يحدث\b", r"\bآخر المستجدات\b", r"\bاخر المستجدات\b",
        r"\bآخر التطورات\b", r"\bاخر التطورات\b", r"\bهل هناك\b",
        r"\bهل حدث\b", r"\bهل يوجد\b", r"\bwhat happened\b",
        r"\bwhat's happening\b", r"\bwhat is happening\b",
        r"\blatest news\b", r"\bbreaking news\b",
    )
    return any(re.search(pattern, t, re.I) for pattern in patterns)


# ============================================================
# GEMINI REQUEST & FALLBACK
# ============================================================

async def ask_gemini(prompt: str) -> str:
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
        log.warning(f"Gemini Unavailable / Fallback mode active: {e}")
        return ""


# ============================================================
# NEWS COLLECTION WITH CACHING
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
        log.exception("News collection error")
        return []


async def search_user_news(text: str):
    items = await get_fresh_news()
    if not items:
        return []
    try:
        return search_news(items, text, max_results=MAX_SEARCH_RESULTS)
    except Exception:
        log.exception("News search error")
        return []


# ============================================================
# REPORT GENERATION & PAGINATION (تصفح الأخبار)
# ============================================================

REPORT_PROMPT = """
أنت محلل أخبار دقيق ومختصر.
مهمتك تحليل الأخبار المتاحة فقط بدون زيادة أو اختراع.

صيغة الإجابة:
🚨 الخلاصة أو العنوان الرئيسي

• أهم نقطة
• أهم نقطة

📌 المصادر:
- اسم المصدر: الرابط
"""


def generate_fallback_report(items, page: int = 1, per_page: int = 5):
    """التقرير الاحتياطي مع دعم تقسيم الصفحة (Pagination)"""
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    page_items = items[start_idx:end_idx]

    lines = ["📰 **أبرز الأخبار المتاحة:**\n"]

    for item in page_items:
        title = getattr(item, "title", "") or getattr(item, "caption", "") or ""
        source = getattr(item, "source", "") or "مصدر إخباري"
        url = getattr(item, "url", "") or getattr(item, "link", "") or ""

        if title:
            entry = f"• **{title}**\n  📍 *{source}*"
            if url:
                entry += f" | [🔗 رابط الخبر]({url})"
            lines.append(entry + "\n")

    return "\n".join(lines)


async def generate_report(question: str, items, page: int = 1, per_page: int = 5) -> str:
    if not items:
        return "🔎 لم أجد أخباراً مرتبطة مباشرة بسؤالك ضمن المصادر المتاحة حالياً."

    # محاولة استخدام الذكاء الاصطناعي في الصفحة الأولى
    if page == 1:
        context = build_ai_context(items[:10])
        prompt = f"{REPORT_PROMPT}\nالسؤال: {question}\nالأخبار المتاحة:\n{context}"
        ai_result = await ask_gemini(prompt)
        if ai_result:
            return ai_result

    # الانتقال إلى العرض البديل الذكي تلقائياً عند توقف Gemini أو عند الانتقال للصفحة 2+
    return generate_fallback_report(items, page=page, per_page=per_page)


def build_pagination_keyboard(key: str, current_page: int, total_items: int, per_page: int = 5):
    """إنشاء زر التصفح للتنقل بين الأخبار"""
    buttons = []
    total_pages = (total_items + per_page - 1) // per_page

    if current_page < total_pages:
        buttons.append(
            InlineKeyboardButton(
                "➕ عرض المزيد من الأخبار",
                callback_data=f"topic:{key}:{current_page + 1}"
            )
        )

    rows = []
    if buttons:
        rows.append(buttons)
    rows.append([InlineKeyboardButton("🔙 القائمة الرئيسية", callback_data="home")])

    return InlineKeyboardMarkup(rows)


# ============================================================
# SAFE MESSAGE SENDING
# ============================================================

async def send_long_message(message_or_query, text: str, reply_markup=None):
    if not text:
        return

    sender = getattr(message_or_query, "message", message_or_query)

    try:
        # المحاولة الأولى مع التنسيق المعياري
        for i in range(0, len(text), 3900):
            chunk = text[i:i + 3900]
            await sender.reply_text(
                chunk,
                reply_markup=reply_markup if (i + 3900 >= len(text)) else None,
                disable_web_page_preview=True,
                parse_mode="Markdown"
            )
    except Exception:
        # Fail-Soft protection: إزالة التنسيق في حال وجود رموز ضارة
        clean_text = text.replace("*", "").replace("_", "").replace("`", "")
        for i in range(0, len(clean_text), 3900):
            chunk = clean_text[i:i + 3900]
            await sender.reply_text(
                chunk,
                reply_markup=reply_markup if (i + 3900 >= len(clean_text)) else None,
                disable_web_page_preview=True
            )


# ============================================================
# HANDLERS
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📰 مرحباً بك في بوت الأخبار العالمي الشامل.\n\n"
        "اختر القطاع أو المنطقة الجغرافية، أو اكتب سؤالك الإخباري مباشرة:",
        reply_markup=main_keyboard(),
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data

    if data == "home":
        await query.message.reply_text("📰 القائمة الرئيسية:", reply_markup=main_keyboard())
        return

    parts = data.split(":")
    if len(parts) < 3 or parts[0] != "topic":
        return

    key = parts[1]
    page = int(parts[2])

    if key not in TOPICS:
        return

    _, topic_text = TOPICS[key]
    user_id = update.effective_user.id

    lock = USER_LOCKS.setdefault(user_id, asyncio.Lock())

    if lock.locked():
        await query.message.reply_text("⏳ يوجد طلب قيد المعالجة. انتظر لحظات.")
        return

    async with lock:
        status = await query.message.reply_text("📡 جاري تحديث الأخبار...")

        try:
            items = await get_fresh_news()

            if not items:
                await status.edit_text("⚠️ تعذر الوصول إلى مصادر الأخبار حالياً.")
                return

            results = search_news(items, topic_text, max_results=MAX_SEARCH_RESULTS)

            if not results:
                await status.edit_text("🔎 لا توجد أخبار جديدة مرتبطة بهذا القسم حالياً.")
                return

            report = await generate_report(topic_text, results, page=page)
            kb = build_pagination_keyboard(key, page, len(results))

            await send_long_message(query.message, report, reply_markup=kb)

            try:
                await status.delete()
            except Exception:
                pass

        except Exception:
            log.exception("Button Handler Exception")
            try:
                await status.edit_text("⚠️ تعذر جلب التقرير حالياً. يرجى المحاولة لاحقاً.")
            except Exception:
                await query.message.reply_text("⚠️ تعذر جلب التقرير حالياً.")


async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text:
        return

    if is_greeting(text):
        reply = "وعليكم السلام ورحمة الله وبركاته، أهلاً بك!" if "السلام" in text else "أهلاً بك!"
        await update.message.reply_text(
            f"{reply}\nاختر قسماً أو اكتب سؤالك الإخباري:",
            reply_markup=main_keyboard()
        )
        return

    if not is_news_query(text):
        await update.message.reply_text(
            "أنا مخصص لمتابعة وتلخيص الأخبار العالمية.\n"
            "يمكنك اختيار قسم من الأزرار أعلاه أو كتابة استفسار إخباري (مثال: أخبار كندا، أستراليا، النفط).",
            reply_markup=main_keyboard()
        )
        return

    user_id = update.effective_user.id
    lock = USER_LOCKS.setdefault(user_id, asyncio.Lock())

    if lock.locked():
        await update.message.reply_text("⏳ يوجد طلب قيد المعالجة. انتظر لحظة.")
        return

    async with lock:
        status = await update.message.reply_text("📡 جاري البحث في كافة الوكالات...")

        try:
            results = await search_user_news(text)

            if not results:
                await status.edit_text("🔎 لم أجد أخباراً مرتبطة بسؤالك في التغطيات الحالية.")
                return

            report = await generate_report(text, results, page=1)
            await send_long_message(update.message, report)

            try:
                await status.delete()
            except Exception:
                pass

        except Exception:
            log.exception("User Message Exception")
            try:
                await status.edit_text("⚠️ حدث خطأ أثناء معالجة طلبك.")
            except Exception:
                await update.message.reply_text("⚠️ حدث خطأ أثناء معالجة طلبك.")


# ============================================================
# MAIN & INIT
# ============================================================

async def post_init(application):
    me = await application.bot.get_me()
    log.info("Telegram connected successfully: @%s", me.username)


def main():
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler, pattern=r"^(topic:|home)"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_user_message))

    log.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
