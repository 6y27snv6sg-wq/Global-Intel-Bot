# ============================================================
# bot.py
# Global News Intelligence Telegram Bot
# ============================================================

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
    search_news,
    hybrid_search_news,
    build_ai_context,
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)

log = logging.getLogger("global_intel_bot")


# ============================================================
# ENVIRONMENT
# ============================================================

BOT_TOKEN = (
    os.getenv("TELEGRAM_BOT_TOKEN") or ""
).strip()

GEMINI_API_KEY = (
    os.getenv("GEMINI_API_KEY") or ""
).strip()


GEMINI_MODEL = "gemini-3.5-flash"

NEWS_COLLECTION_TIMEOUT = 25
GEMINI_TIMEOUT = 35

MAX_SEARCH_RESULTS = 25
CACHE_TTL = 300

PER_PAGE = 5


# ============================================================
# REQUIRED ENVIRONMENT VARIABLES
# ============================================================

if not BOT_TOKEN:
    raise RuntimeError(
        "Missing TELEGRAM_BOT_TOKEN"
    )

if not GEMINI_API_KEY:
    raise RuntimeError(
        "Missing GEMINI_API_KEY"
    )


# ============================================================
# GEMINI CLIENT
# ============================================================

ai_client = genai.Client(
    api_key=GEMINI_API_KEY
)


# ============================================================
# SIMPLE CACHE
# ============================================================

class SimpleCache:

    def __init__(
        self,
        ttl: int = 300,
    ):
        self.ttl = ttl
        self._cache: Dict[str, Any] = {}
        self._timestamps: Dict[str, float] = {}

    def get(
        self,
        key: str,
    ):
        if key not in self._cache:
            return None

        timestamp = self._timestamps.get(
            key,
            0,
        )

        if time.time() - timestamp >= self.ttl:

            self._cache.pop(
                key,
                None,
            )

            self._timestamps.pop(
                key,
                None,
            )

            return None

        return self._cache[key]

    def set(
        self,
        key: str,
        value: Any,
    ):

        # None means explicit invalidation.
        if value is None:

            self._cache.pop(
                key,
                None,
            )

            self._timestamps.pop(
                key,
                None,
            )

            return

        self._cache[key] = value
        self._timestamps[key] = time.time()


NEWS_CACHE = SimpleCache(
    ttl=CACHE_TTL
)


# ============================================================
# USER STATE
# ============================================================

USER_LOCKS: Dict[int, asyncio.Lock] = {}

MUTED_USERS: Set[int] = set()


# ============================================================
# TOPICS
# ============================================================

TOPICS = {

    "econ": (
        "📈 اقتصاد وأسواق",
        [
            "اقتصاد",
            "أسواق",
            "أسهم",
            "بورصة",
            "تداول",
            "سوق المال",
            "ذهب",
            "نفط",
            "بترول",
            "خام",
            "برنت",
            "أوبك",
            "غاز",
            "طاقة",
            "وزارة الطاقة",
            "تضخم",
            "فائدة",
            "بنك مركزي",
            "مصرف مركزي",
            "عملات",
            "دولار",
            "يورو",
            "بيتكوين",
            "إيثريوم",
            "كريبتو",
            "عملات رقمية",
            "استثمار",
            "اقتصادي",
        ],
    ),

    "forg": (
        "🏛 بيانات رسمية",
        [
            "وزارة",
            "وزير",
            "وزارة الخارجية",
            "الخارجية",
            "وزارة الدفاع",
            "رئاسة الوزراء",
            "رئيس الوزراء",
            "الرئاسة",
            "رئيس",
            "حكومة",
            "بيان رسمي",
            "تصريح رسمي",
            "بيان صحفي",
            "مصدر مسؤول",
            "المتحدث الرسمي",
            "وكالة الأنباء",
        ],
    ),

    "urg": (
        "🚨 عاجل",
        [
            "عاجل",
            "طارئ",
            "بيان عاجل",
            "تطور عاجل",
            "تطورات عاجلة",
            "هجوم",
            "انفجار",
            "زلزال",
            "إخلاء",
            "تحذير",
        ],
    ),

    "gulf": (
        "🌍 الشرق الأوسط",
        [
            "السعودية",
            "الإمارات",
            "قطر",
            "الكويت",
            "البحرين",
            "عمان",
            "اليمن",
            "العراق",
            "إيران",
            "سوريا",
            "لبنان",
            "الأردن",
            "فلسطين",
            "إسرائيل",
            "مصر",
            "تركيا",
            "الخليج",
            "الشرق الأوسط",
        ],
    ),

    "wrld": (
        "🌐 العالم",
        [
            "العالم",
            "دولي",
            "دولية",
            "أمريكا",
            "الولايات المتحدة",
            "كندا",
            "المكسيك",
            "أوروبا",
            "بريطانيا",
            "فرنسا",
            "ألمانيا",
            "إيطاليا",
            "إسبانيا",
            "روسيا",
            "أوكرانيا",
            "الصين",
            "اليابان",
            "الهند",
            "أستراليا",
            "أفريقيا",
            "البرازيل",
            "الأرجنتين",
            "آسيا",
        ],
    ),

    "secu": (
        "🛡 دفاع وأمن",
        [
            "دفاع",
            "وزارة الدفاع",
            "أمن",
            "أمن قومي",
            "أمن وطني",
            "عسكري",
            "جيش",
            "قوات",
            "تسليح",
            "أسلحة",
            "مناورات",
            "عملية عسكرية",
            "ضربة",
            "هجوم",
            "دفاع جوي",
            "بحرية",
            "سلاح الجو",
            "حدود",
            "إرهاب",
        ],
    ),
}


# ============================================================
# KEYBOARD
# ============================================================

def main_keyboard(
    user_id: int,
) -> InlineKeyboardMarkup:

    rows = []

    topic_items = list(
        TOPICS.items()
    )

    for index in range(
        0,
        len(topic_items),
        2,
    ):

        pair = topic_items[
            index:index + 2
        ]

        row = []

        for key, (label, _) in pair:

            row.append(
                InlineKeyboardButton(
                    label,
                    callback_data=(
                        f"t:{key}:1"
                    ),
                )
            )

        rows.append(row)

    if user_id in MUTED_USERS:

        alert_label = "🔔 التنبيهات"

    else:

        alert_label = "🔕 التنبيهات"

    rows.append(
        [
            InlineKeyboardButton(
                alert_label,
                callback_data="toggle_alerts",
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                "🔄 تحديث",
                callback_data="refresh",
            ),
            InlineKeyboardButton(
                "➕ المزيد",
                callback_data="more",
            ),
        ]
    )

    return InlineKeyboardMarkup(
        rows
    )


# ============================================================
# LINK HANDLING
# ============================================================

def build_safe_link(
    title: str,
    source: str,
    raw_url: str,
) -> str:

    raw_url = (
        raw_url or ""
    ).strip()

    if (
        raw_url
        and raw_url.startswith(
            (
                "http://",
                "https://",
            )
        )
        and "news.google.com" not in raw_url
    ):

        return raw_url

    clean_title = re.sub(
        r"[^\w\u0600-\u06ff\s]",
        " ",
        title or "",
    )

    clean_title = re.sub(
        r"\s+",
        " ",
        clean_title,
    ).strip()

    query = (
        f"{clean_title} {source}"
    ).strip()

    if not query:

        query = title or "news"

    encoded = urllib.parse.quote_plus(
        query
    )

    return (
        "https://www.google.com/search?q="
        f"{encoded}"
    )


# ============================================================
# TELEGRAM HTML ESCAPE
# ============================================================

def safe_html(
    text: str,
) -> str:

    return html.escape(
        str(text or ""),
        quote=False,
    )


# ============================================================
# NEWS CACHE
# ============================================================

async def get_fresh_news(
    force_refresh: bool = False,
):

    if not force_refresh:

        cached = NEWS_CACHE.get(
            "all_news"
        )

        if cached is not None:

            return cached

    try:

        items = await asyncio.wait_for(
            collect_news(
                max_items=150
            ),
            timeout=NEWS_COLLECTION_TIMEOUT,
        )

        if items:

            NEWS_CACHE.set(
                "all_news",
                items,
            )

        return items or []

    except asyncio.CancelledError:

        raise

    except Exception:

        log.exception(
            "News Collection Error"
        )

        return []


# ============================================================
# TOPIC FILTER
# ============================================================

def topic_results(
    items,
    keywords,
):

    if not items:
        return []

    return search_news(
        items,
        keywords,
        max_results=MAX_SEARCH_RESULTS,
    )


# ============================================================
# REPORT GENERATOR
# ============================================================

def generate_base_report(
    items,
    page: int = 1,
    per_page: int = PER_PAGE,
    heading: str = "📰 أبرز الأخبار والتغطيات",
):

    if not items:

        return (
            "<b>🔎 لا توجد نتائج مطابقة.</b>"
        )

    start_index = (
        page - 1
    ) * per_page

    end_index = (
        start_index + per_page
    )

    page_items = items[
        start_index:end_index
    ]

    lines = [
        f"<b>{safe_html(heading)}</b>",
        "",
    ]

    for item in page_items:

        title = (
            getattr(
                item,
                "title",
                "",
            )
            or ""
        )

        source = (
            getattr(
                item,
                "source",
                "",
            )
            or "مصدر غير محدد"
        )

        raw_url = (
            getattr(
                item,
                "url",
                "",
            )
            or ""
        )

        published = (
            getattr(
                item,
                "published_at",
                "",
            )
            or ""
        )

        region = (
            getattr(
                item,
                "region",
                "",
            )
            or ""
        )

        safe_url = build_safe_link(
            title,
            source,
            raw_url,
        )

        entry_lines = [
            f"• <b>{safe_html(title)}</b>",
            (
                f"  📍 {safe_html(source)}"
            ),
        ]

        if region:

            entry_lines.append(
                f"  🌐 {safe_html(region)}"
            )

        if published:

            entry_lines.append(
                f"  🕒 {safe_html(published)}"
            )

        entry_lines.append(
            f'  <a href="{safe_html(safe_url)}">'
            "🔗 قراءة الخبر"
            "</a>"
        )

        lines.append(
            "\n".join(
                entry_lines
            )
        )

        lines.append("")

    return "\n".join(
        lines
    )


# ============================================================
# NAVIGATION KEYBOARD
# ============================================================

def result_keyboard(
    key: str,
    page: int,
    total_items: int,
):

    total_pages = max(
        1,
        (
            len(range(total_items))
            + PER_PAGE
            - 1
        )
        // PER_PAGE,
    )

    nav = []

    if page < total_pages:

        nav.append(
            InlineKeyboardButton(
                "➕ المزيد",
                callback_data=(
                    f"t:{key}:{page + 1}"
                ),
            )
        )

    nav.append(
        InlineKeyboardButton(
            "🧠 تحليل",
            callback_data=(
                f"analyze:{key}"
            ),
        )
    )

    rows = [
        nav,
        [
            InlineKeyboardButton(
                "🔙 الرئيسية",
                callback_data="home",
            )
        ],
    ]

    return InlineKeyboardMarkup(
        rows
    )


# ============================================================
# SEARCH RESULT KEYBOARD
# ============================================================

def search_keyboard():

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🔙 الرئيسية",
                    callback_data="home",
                )
            ]
        ]
    )


# ============================================================
# GEMINI ANALYSIS
# ============================================================

ANALYSIS_PROMPT = """
أنت محلل استخبارات إخبارية ومحرر تنفيذي.

حلل الأخبار المعطاة فقط، ولا تخترع معلومات غير موجودة.

أخرج النتيجة بالعربية وبصيغة تنفيذية مختصرة:

🧠 التحليل التنفيذي

• التطورات الرئيسية
• الأطراف والجهات المعنية
• التأثير المباشر
• الدلالات المحتملة
• ما الذي يستحق المتابعة

اجعل التحليل دقيقاً ومباشراً.
الحد الأقصى 120 كلمة.
"""


async def analyze_with_gemini(
    items,
) -> str:

    if not items:

        return (
            "⚠️ لا توجد بيانات كافية للتحليل."
        )

    context = build_ai_context(
        items,
        limit=6,
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
                    )
                ),
            ),
            timeout=GEMINI_TIMEOUT,
        )

        text = (
            getattr(
                response,
                "text",
                None,
            )
            or ""
        ).strip()

        if not text:

            return (
                "⚠️ لم يُرجع Gemini تحليلاً."
            )

        return text

    except asyncio.CancelledError:

        raise

    except Exception as exc:

        log.warning(
            "Gemini Analysis Error: %s",
            exc,
        )

        return (
            "⚠️ تعذر إتمام التحليل حالياً."
        )


# ============================================================
# START
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not update.effective_user:

        return

    user_id = (
        update.effective_user.id
    )

    if not update.message:

        return

    await update.message.reply_text(
        (
            "<b>🏛 منصة الأخبار والبيانات "
            "الرسمية الشاملة</b>\n\n"
            "اختر القسم المطلوب، أو اكتب "
            "أي كلمة للبحث في كامل التغطية."
        ),
        reply_markup=main_keyboard(
            user_id
        ),
        parse_mode="HTML",
    )


# ============================================================
# BUTTON HANDLER
# ============================================================

async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query

    if not query:

        return

    user = update.effective_user

    if not user:

        await query.answer()
        return

    user_id = user.id
    data = query.data or ""

    # --------------------------------------------------------
    # Alerts
    # --------------------------------------------------------

    if data == "toggle_alerts":

        if user_id in MUTED_USERS:

            MUTED_USERS.discard(
                user_id
            )

            await query.answer(
                text=(
                    "🔔 تم تفعيل التنبيهات "
                    "المنبثقة"
                ),
                show_alert=True,
            )

        else:

            MUTED_USERS.add(
                user_id
            )

            await query.answer(
                text=(
                    "🔕 تم إيقاف التنبيهات "
                    "المنبثقة"
                ),
                show_alert=True,
            )

        try:

            await query.message.edit_reply_markup(
                reply_markup=main_keyboard(
                    user_id
                )
            )

        except Exception:

            log.exception(
                "Failed to update alert keyboard"
            )

        return

    # --------------------------------------------------------
    # Home
    # --------------------------------------------------------

    if data == "home":

        await query.answer()

        try:

            await query.message.edit_text(
                (
                    "<b>📰 القائمة الرئيسية</b>\n\n"
                    "اختر القسم المطلوب، أو اكتب "
                    "أي كلمة للبحث في كامل التغطية."
                ),
                reply_markup=main_keyboard(
                    user_id
                ),
                parse_mode="HTML",
            )

        except Exception:

            await query.message.reply_text(
                (
                    "<b>📰 القائمة الرئيسية</b>"
                ),
                reply_markup=main_keyboard(
                    user_id
                ),
                parse_mode="HTML",
            )

        return

    # --------------------------------------------------------
    # Refresh
    # --------------------------------------------------------

    if data == "refresh":

        await query.answer(
            text="🔄 جارٍ تحديث الأخبار...",
            show_alert=False,
        )

        NEWS_CACHE.set(
            "all_news",
            None,
        )

        lock = USER_LOCKS.setdefault(
            user_id,
            asyncio.Lock(),
        )

        if lock.locked():

            await query.answer(
                text="⏳ التحديث جارٍ بالفعل.",
                show_alert=False,
            )

            return

        async with lock:

            try:

                await get_fresh_news(
                    force_refresh=True
                )

                await query.answer(
                    text="✅ تم تحديث الأخبار.",
                    show_alert=True,
                )

            except Exception:

                log.exception(
                    "Refresh Error"
                )

                await query.answer(
                    text="⚠️ تعذر تحديث الأخبار.",
                    show_alert=True,
                )

        return

    # --------------------------------------------------------
    # More
    # --------------------------------------------------------

    if data == "more":

        await query.answer()

        await query.message.reply_text(
            (
                "<b>➕ خيارات إضافية</b>\n\n"
                "اكتب أي كلمة أو اسم دولة أو "
                "موضوع للبحث في كامل الأخبار.\n\n"
                "مثال:\n"
                "<code>السعودية</code>\n"
                "<code>السعودية النفط</code>\n"
                "<code>أمريكا الفائدة</code>"
            ),
            parse_mode="HTML",
        )

        return

    # --------------------------------------------------------
    # Analysis
    # --------------------------------------------------------

    if data.startswith(
        "analyze:"
    ):

        await query.answer()

        key = data.split(
            ":",
            1,
        )[1]

        if key not in TOPICS:

            await query.message.reply_text(
                "⚠️ القسم غير معروف."
            )

            return

        status = await query.message.reply_text(
            "🧠 جارٍ تحليل البيانات..."
        )

        try:

            items = await get_fresh_news()

            _, keywords = TOPICS[
                key
            ]

            results = topic_results(
                items,
                keywords,
            )

            if not results:

                await status.edit_text(
                    (
                        "⚠️ لا توجد بيانات كافية "
                        "للتحليل حالياً."
                    )
                )

                return

            analysis = await analyze_with_gemini(
                results
            )

            await status.edit_text(
                (
                    "<b>🧠 التحليل التنفيذي</b>\n\n"
                    f"{safe_html(analysis)}"
                ),
                parse_mode="HTML",
            )

        except Exception:

            log.exception(
                "Analysis Handler Error"
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء التحليل."
            )

        return

    # --------------------------------------------------------
    # Topic
    # --------------------------------------------------------

    if data.startswith(
        "t:"
    ):

        parts = data.split(":")

        if len(parts) != 3:

            await query.answer()
            return

        key = parts[1]

        try:

            page = int(
                parts[2]
            )

        except ValueError:

            await query.answer()
            return

        if key not in TOPICS:

            await query.answer()
            return

        await query.answer()

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

        async with lock:

            status = await query.message.reply_text(
                "📡 جارٍ فرز الأخبار..."
            )

            try:

                items = await get_fresh_news()

                _, keywords = TOPICS[
                    key
                ]

                results = topic_results(
                    items,
                    keywords,
                )

                # ------------------------------------------------
                # Urgent top alert
                # ------------------------------------------------

                if (
                    key == "urg"
                    and results
                    and user_id not in MUTED_USERS
                ):

                    top_title = (
                        getattr(
                            results[0],
                            "title",
                            "",
                        )
                        or "خبر عاجل جديد"
                    )

                    # answerCallbackQuery must only be
                    # used once for the same callback.
                    # Therefore the alert is sent as a
                    # message here instead of answering
                    # the callback a second time.
                    await query.message.reply_text(
                        (
                            "🚨 <b>عاجل</b>\n\n"
                            f"{safe_html(top_title[:500])}"
                        ),
                        parse_mode="HTML",
                    )

                if not results:

                    await status.edit_text(
                        (
                            "🔎 لا توجد أخبار مطابقة "
                            "لهذا القسم حالياً."
                        )
                    )

                    return

                label = TOPICS[
                    key
                ][0]

                report = generate_base_report(
                    results,
                    page=page,
                    per_page=PER_PAGE,
                    heading=label,
                )

                await status.edit_text(
                    report,
                    reply_markup=result_keyboard(
                        key,
                        page,
                        len(results),
                    ),
                    disable_web_page_preview=True,
                    parse_mode="HTML",
                )

            except Exception:

                log.exception(
                    "Topic Handler Error"
                )

                await status.edit_text(
                    "⚠️ حدث خطأ أثناء عرض البيانات."
                )

        return

    # --------------------------------------------------------
    # Unknown callback
    # --------------------------------------------------------

    await query.answer()


# ============================================================
# USER SEARCH
# ============================================================

async def handle_user_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not update.message:

        return

    text = (
        update.message.text or ""
    ).strip()

    if not text:

        return

    user = update.effective_user

    if not user:

        return

    user_id = user.id

    lock = USER_LOCKS.setdefault(
        user_id,
        asyncio.Lock(),
    )

    if lock.locked():

        await update.message.reply_text(
            "⏳ يوجد بحث جارٍ، انتظر قليلاً."
        )

        return

    async with lock:

        status = await update.message.reply_text(
            (
                "🔎 جارٍ البحث في كامل "
                f"التغطية عن:\n<b>{safe_html(text)}</b>"
            ),
            parse_mode="HTML",
        )

        try:

            # ----------------------------------------------------
            # IMPORTANT:
            #
            # This search is NOT connected to the
            # currently selected topic.
            #
            # It searches the complete news cache
            # and automatically goes online if fewer
            # than 3 useful results are available.
            # ----------------------------------------------------

            items = await get_fresh_news()

            results = await hybrid_search_news(
                items,
                text,
                max_results=MAX_SEARCH_RESULTS,
            )

            # ----------------------------------------------------
            # Never show unrelated latest news.
            # ----------------------------------------------------

            if not results:

                await status.edit_text(
                    (
                        "🔎 لم أجد نتائج مطابقة لبحثك.\n\n"
                        "جرّب اسم دولة أو شخص أو موضوع "
                        "بكلمات أوضح."
                    )
                )

                return

            report = generate_base_report(
                results,
                page=1,
                per_page=PER_PAGE,
                heading=(
                    f"🔎 نتائج البحث: "
                    f"{text}"
                ),
            )

            await status.edit_text(
                report,
                reply_markup=search_keyboard(),
                disable_web_page_preview=True,
                parse_mode="HTML",
            )

        except asyncio.CancelledError:

            raise

        except Exception:

            log.exception(
                "Message Search Exception"
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء البحث."
            )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):

    error = context.error

    log.error(
        "Unhandled Telegram error: %s",
        error,
        exc_info=True,
    )


# ============================================================
# MAIN
# ============================================================

def main():

    log.info(
        "Starting Global Intel Bot..."
    )

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .build()
    )

    # --------------------------------------------------------
    # Commands
    # --------------------------------------------------------

    app.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    # --------------------------------------------------------
    # Buttons
    # --------------------------------------------------------

    app.add_handler(
        CallbackQueryHandler(
            button_handler,
            pattern=(
                r"^(?:"
                r"t:.*|"
                r"home|"
                r"refresh|"
                r"more|"
                r"toggle_alerts|"
                r"analyze:.*"
                r")$"
            ),
        )
    )

    # --------------------------------------------------------
    # Free-text search
    # --------------------------------------------------------

    app.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND,
            handle_user_message,
        )
    )

    # --------------------------------------------------------
    # Errors
    # --------------------------------------------------------

    app.add_error_handler(
        error_handler
    )

    log.info(
        "Global Intel Bot launched successfully."
    )

    # --------------------------------------------------------
    # Polling
    # --------------------------------------------------------

    app.run_polling(
        drop_pending_updates=True
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
