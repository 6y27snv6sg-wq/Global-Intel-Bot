import asyncio
import html
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
    build_ai_context,
    search_news,
)


# ============================================================
# الإعدادات
# ============================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)

log = logging.getLogger("pro_news_bot")

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()

GEMINI_MODEL = "gemini-3.5-flash"

NEWS_COLLECTION_TIMEOUT = 20
GEMINI_TIMEOUT = 30
MAX_SEARCH_RESULTS = 25
CACHE_TTL = 300

if not BOT_TOKEN or not GEMINI_API_KEY:
    raise RuntimeError(
        "Missing TELEGRAM_BOT_TOKEN or GEMINI_API_KEY"
    )

ai_client = genai.Client(
    api_key=GEMINI_API_KEY
)


# ============================================================
# الذاكرة المؤقتة
# ============================================================

class SimpleCache:

    def __init__(self, ttl: int = 300):
        self.ttl = ttl
        self._cache: Dict[str, Any] = {}
        self._timestamps: Dict[str, float] = {}

    def get(self, key: str):

        value = self._cache.get(key)

        if value is None:
            return None

        timestamp = self._timestamps.get(key, 0)

        if time.time() - timestamp < self.ttl:
            return value

        self._cache.pop(key, None)
        self._timestamps.pop(key, None)

        return None

    def set(self, key: str, value: Any):

        if value is None:
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)
            return

        self._cache[key] = value
        self._timestamps[key] = time.time()


NEWS_CACHE = SimpleCache(CACHE_TTL)

USER_LOCKS: Dict[int, asyncio.Lock] = {}
MUTED_USERS: Set[int] = set()


# ============================================================
# التصنيفات
# ============================================================

TOPICS = {

    "econ": (
        "📈 الاقتصاد والطاقة والأسواق",
        [
            "أسهم",
            "بورصة",
            "الذهب",
            "معادن",
            "الفيدرالي",
            "فائدة",
            "عملات رقمية",
            "بيتكوين",
            "تداول",
            "النفط",
            "أوبك",
            "خام",
            "تضخم",
            "أسواق المال",
            "برنت",
            "اقتصاد",
            "بنك مركزي",
        ],
    ),

    "forg": (
        "🏛 البيانات والتصريحات الرسمية",
        [
            "وزارة",
            "وزير",
            "المتحدث",
            "بيان رسمي",
            "تصريح رسمي",
            "بيان صحفي",
            "مصدر مسؤول",
            "رئاسة الوزراء",
            "الديوان",
            "وزارة الخارجية",
            "حكومة",
        ],
    ),

    "urg": (
        "🚨 عاجل وبيانات طارئة",
        [
            "عاجل",
            "بيان هام",
            "تصريح عاجل",
            "طارئ",
        ],
    ),

    "gulf": (
        "🇸🇦 الخليج والشرق الأوسط",
        [
            "الخليج",
            "السعودية",
            "الإمارات",
            "قطر",
            "الكويت",
            "البحرين",
            "عمان",
            "الرياض",
            "أبوظبي",
            "العراق",
            "مصر",
            "الأردن",
            "إيران",
            "تركيا",
        ],
    ),

    "wrld": (
        "🌍 العالم والسياسة",
        [
            "دولية",
            "قمة",
            "أمريكا",
            "أوروبا",
            "الصين",
            "روسيا",
            "واشنطن",
            "بكين",
            "آسيا",
            "أفريقيا",
            "أستراليا",
            "أمريكا الجنوبية",
        ],
    ),

    "secu": (
        "🛡 الدفاع والأمن",
        [
            "الدفاع",
            "الأمن القومي",
            "تسليح",
            "مناورات",
            "عسكري",
            "جيش",
            "أمن",
        ],
    ),
}


# ============================================================
# أدوات
# ============================================================

def normalize_text(text: str) -> str:

    text = str(text or "").lower()

    text = re.sub(
        r"[^\w\s\u0600-\u06FF-]",
        " ",
        text,
    )

    return re.sub(
        r"\s+",
        " ",
        text,
    ).strip()


def strict_search_news(
    items: list,
    keywords_list: list,
    max_results: int = 25,
) -> list:

    results = []

    normalized_keywords = [
        normalize_text(keyword)
        for keyword in keywords_list
        if normalize_text(keyword)
    ]

    for item in items:

        title = normalize_text(
            getattr(item, "title", "")
        )

        summary = normalize_text(
            getattr(item, "summary", "")
        )

        source = normalize_text(
            getattr(item, "source", "")
        )

        searchable = (
            f"{title} {summary} {source}"
        )

        if any(
            keyword in searchable
            for keyword in normalized_keywords
        ):
            results.append(item)

    return results[:max_results]


def build_safe_link(
    title: str,
    source: str,
    raw_url: str,
) -> str:

    raw_url = str(raw_url or "").strip()

    if (
        raw_url.startswith("http://")
        or raw_url.startswith("https://")
    ):

        if "news.google.com" not in raw_url:
            return raw_url

    clean_title = re.sub(
        r"[^\w\s\u0600-\u06FF]",
        " ",
        title or "",
    )

    query = (
        f"{clean_title} {source}"
    ).strip()

    return (
        "https://www.google.com/search?q="
        + urllib.parse.quote_plus(query)
    )


def html_link(
    title: str,
    source: str,
    url: str,
) -> str:

    safe_url = build_safe_link(
        title,
        source,
        url,
    )

    return (
        f'<a href="{html.escape(safe_url, quote=True)}">'
        f"🔗 قراءة الخبر"
        f"</a>"
    )


# ============================================================
# لوحة البداية
# ============================================================

def main_keyboard(user_id: int):

    rows = []

    items = list(TOPICS.items())

    for i in range(
        0,
        len(items),
        2,
    ):

        row = []

        for key, (label, _) in items[
            i:i + 2
        ]:

            row.append(
                InlineKeyboardButton(
                    label,
                    callback_data=f"t:{key}:1",
                )
            )

        rows.append(row)

    is_muted = user_id in MUTED_USERS

    if is_muted:

        alert_text = (
            "🔔 تفعيل التنبيهات المنبثقة"
        )

        alert_action = "unmute_alerts"

    else:

        alert_text = (
            "🔕 إيقاف التنبيهات المنبثقة"
        )

        alert_action = "mute_alerts"

    rows.append(
        [
            InlineKeyboardButton(
                alert_text,
                callback_data=alert_action,
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                "🔄 تحديث الأخبار",
                callback_data="refresh",
            )
        ]
    )

    return InlineKeyboardMarkup(rows)


# ============================================================
# Gemini
# ============================================================

ANALYSIS_PROMPT = """
أنت محلل إخباري تنفيذي.

حلل الأخبار التي سأعطيك إياها فقط.

المطلوب:
• أهم التطورات.
• ما الذي تغير؟
• الأثر المباشر.
• الأثر المحتمل لاحقاً.
• إذا كانت الأخبار متعارضة، اذكر ذلك.
• لا تخترع أي معلومة.
• لا تضف أسماء أو أرقاماً غير موجودة في البيانات.
• لا تعتبر توقعك حقيقة مؤكدة.

اكتب التحليل بالعربية وبحد أقصى 120 كلمة.
"""


async def analyze_with_gemini(items) -> str:

    context = build_ai_context(
        items,
        limit=8,
    )

    prompt = (
        f"{ANALYSIS_PROMPT}\n\n"
        f"البيانات:\n{context}"
    )

    try:

        response = await asyncio.wait_for(
            asyncio.to_thread(
                ai_client.models.generate_content,
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(
                        thinking_level="low"
                    ),
                ),
            ),
            timeout=GEMINI_TIMEOUT,
        )

        return (
            getattr(response, "text", None)
            or ""
        ).strip()

    except Exception as exc:

        log.warning(
            "Gemini Analysis Error: %s",
            exc,
        )

        return (
            "⚠️ تعذر إتمام التحليل حالياً."
        )


# ============================================================
# جلب الأخبار
# ============================================================

async def get_fresh_news():

    cached = NEWS_CACHE.get(
        "all_news"
    )

    if cached is not None:
        return cached

    try:

        items = await asyncio.wait_for(
            collect_news(
                max_items=100
            ),
            timeout=NEWS_COLLECTION_TIMEOUT,
        )

        if items:
            NEWS_CACHE.set(
                "all_news",
                items,
            )

        return items or []

    except Exception:

        log.exception(
            "News Collection Error"
        )

        return []


# ============================================================
# التقرير
# ============================================================

def generate_base_report(
    items,
    page: int = 1,
    per_page: int = 5,
):

    start = (
        page - 1
    ) * per_page

    page_items = items[
        start:start + per_page
    ]

    lines = [
        "📰 <b>أبرز التغطيات والبيانات المتخصصة</b>",
        "",
    ]

    for item in page_items:

        title = (
            getattr(item, "title", "")
            or getattr(item, "caption", "")
            or ""
        )

        source = (
            getattr(item, "source", "")
            or "مصدر إخباري"
        )

        published = (
            getattr(item, "published_at", "")
            or ""
        )

        url = (
            getattr(item, "url", "")
            or getattr(item, "link", "")
            or ""
        )

        if not title:
            continue

        lines.append(
            f"• <b>{html.escape(title)}</b>"
        )

        lines.append(
            f"  📍 <i>المصدر:</i> "
            f"{html.escape(source)}"
        )

        if published:

            lines.append(
                f"  🕒 {html.escape(published)}"
            )

        lines.append(
            f"  {html_link(title, source, url)}"
        )

        lines.append("")

    return "\n".join(lines)


# ============================================================
# Start
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    user_id = update.effective_user.id

    await update.message.reply_text(
        "🏛 <b>منصة الأخبار والبيانات الرسمية الشاملة</b>\n\n"
        "اختر القطاع المطلوب لمتابعة التغطية "
        "الحية والمتخصصة حول العالم:",
        reply_markup=main_keyboard(user_id),
        parse_mode="HTML",
    )


# ============================================================
# الأزرار
# ============================================================

async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query
    user_id = update.effective_user.id
    data = query.data or ""

    # --------------------------------------------------------
    # التنبيهات
    # --------------------------------------------------------

    if data == "mute_alerts":

        MUTED_USERS.add(user_id)

        await query.answer(
            text="🔕 تم إيقاف التنبيهات المنبثقة العلوية",
            show_alert=True,
        )

        await query.message.edit_reply_markup(
            reply_markup=main_keyboard(user_id)
        )

        return

    if data == "unmute_alerts":

        MUTED_USERS.discard(user_id)

        await query.answer(
            text="🔔 تم تفعيل التنبيهات المنبثقة العلوية",
            show_alert=True,
        )

        await query.message.edit_reply_markup(
            reply_markup=main_keyboard(user_id)
        )

        return

    # --------------------------------------------------------
    # الرئيسية
    # --------------------------------------------------------

    if data == "home":

        await query.answer()

        await query.message.reply_text(
            "📰 <b>القائمة الرئيسية:</b>",
            reply_markup=main_keyboard(user_id),
            parse_mode="HTML",
        )

        return

    # --------------------------------------------------------
    # تحديث
    # --------------------------------------------------------

    if data == "refresh":

        NEWS_CACHE.set(
            "all_news",
            None,
        )

        await query.answer(
            text="🔄 سيتم جلب أحدث الأخبار الآن",
            show_alert=True,
        )

        return

    # --------------------------------------------------------
    # تحليل اختياري
    # --------------------------------------------------------

    if data.startswith("analyze:"):

        await query.answer()

        key = data.split(
            ":",
            1,
        )[1]

        status = await query.message.reply_text(
            "🧠 جاري تحليل البيانات المتاحة..."
        )

        items = await get_fresh_news()

        _, keywords = TOPICS.get(
            key,
            ("", []),
        )

        results = strict_search_news(
            items,
            keywords,
            max_results=8,
        )

        if not results:

            await status.edit_text(
                "⚠️ لا توجد بيانات كافية للتحليل."
            )

            return

        analysis = await analyze_with_gemini(
            results
        )

        await status.edit_text(
            "🧠 <b>التحليل التنفيذي</b>\n\n"
            + html.escape(analysis),
            parse_mode="HTML",
        )

        return

    # --------------------------------------------------------
    # الأقسام
    # --------------------------------------------------------

    parts = data.split(":")

    if (
        len(parts) != 3
        or parts[0] != "t"
    ):
        await query.answer()
        return

    key = parts[1]

    try:
        page = int(parts[2])
    except ValueError:
        await query.answer()
        return

    if key not in TOPICS:

        await query.answer()
        return

    lock = USER_LOCKS.setdefault(
        user_id,
        asyncio.Lock(),
    )

    if lock.locked():

        await query.answer(
            text="⏳ جاري التحميل...",
            show_alert=False,
        )

        return

    await query.answer()

    async with lock:

        status = await query.message.reply_text(
            "📡 جاري فرز الأخبار حسب التخصص..."
        )

        try:

            items = await get_fresh_news()

            _, keywords = TOPICS[key]

            results = strict_search_news(
                items,
                keywords,
                max_results=MAX_SEARCH_RESULTS,
            )

            # ------------------------------------------------
            # التنبيه العاجل
            # ------------------------------------------------

            if (
                key == "urg"
                and results
                and user_id not in MUTED_USERS
            ):

                top_news = getattr(
                    results[0],
                    "title",
                    "خبر عاجل جديد!",
                )

                await query.message.reply_text(
                    "🚨 <b>تنبيه عاجل</b>\n\n"
                    + html.escape(
                        top_news[:500]
                    ),
                    parse_mode="HTML",
                )

            if not results:

                await status.edit_text(
                    "🔎 لا توجد أخبار جديدة "
                    "تندرج تحت هذا التخصص حالياً."
                )

                return

            report = generate_base_report(
                results,
                page=page,
                per_page=5,
            )

            total_pages = (
                len(results) + 4
            ) // 5

            nav_buttons = []

            if page < total_pages:

                nav_buttons.append(
                    InlineKeyboardButton(
                        "➕ إضافية",
                        callback_data=(
                            f"t:{key}:{page + 1}"
                        ),
                    )
                )

            # التحليل يبقى اختيارياً
            nav_buttons.append(
                InlineKeyboardButton(
                    "🧠 تحليل البيانات",
                    callback_data=(
                        f"analyze:{key}"
                    ),
                )
            )

            rows = [nav_buttons]

            rows.append(
                [
                    InlineKeyboardButton(
                        "🔙 القائمة الرئيسية",
                        callback_data="home",
                    )
                ]
            )

            await status.edit_text(
                report,
                reply_markup=InlineKeyboardMarkup(
                    rows
                ),
                disable_web_page_preview=True,
                parse_mode="HTML",
            )

        except Exception:

            log.exception(
                "Button Handler Exception"
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء عرض البيانات."
            )


# ============================================================
# البحث اليدوي
# ============================================================

async def handle_user_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    text = (
        update.message.text or ""
    ).strip()

    if not text:
        return

    user_id = update.effective_user.id

    lock = USER_LOCKS.setdefault(
        user_id,
        asyncio.Lock(),
    )

    if lock.locked():

        await update.message.reply_text(
            "⏳ جاري البحث..."
        )

        return

    async with lock:

        status = await update.message.reply_text(
            "🔎 جاري البحث في كافة التغطيات عن:\n"
            f"<b>{html.escape(text)}</b>..."
            ,
            parse_mode="HTML",
        )

        try:

            items = await get_fresh_news()

            # البحث بالكلمات بدلاً من مطابقة
            # الجملة كاملة حرفياً
            words = [
                word
                for word in re.split(
                    r"\s+",
                    text,
                )
                if len(word) >= 2
            ]

            results = search_news(
                items,
                words,
                max_results=MAX_SEARCH_RESULTS,
            )

            if not results:

                await status.edit_text(
                    "🔎 لم أجد نتائج مطابقة لبحثك."
                )

                return

            report = generate_base_report(
                results,
                page=1,
                per_page=5,
            )

            await status.edit_text(
                report,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🔙 القائمة الرئيسية",
                                callback_data="home",
                            )
                        ]
                    ]
                ),
                disable_web_page_preview=True,
                parse_mode="HTML",
            )

        except Exception:

            log.exception(
                "Message Search Exception"
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء البحث."
            )


# ============================================================
# التشغيل
# ============================================================

def main():

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .build()
    )

    app.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            button_handler,
            pattern=(
                r"^(t:.*|home|refresh|"
                r"mute_alerts|unmute_alerts|"
                r"analyze:.*)$"
            ),
        )
    )

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_user_message,
        )
    )

    log.info(
        "Pro News Bot launched successfully."
    )

    app.run_polling(
        drop_pending_updates=True
    )


if __name__ == "__main__":
    main()
