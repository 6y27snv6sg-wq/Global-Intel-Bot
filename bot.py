import asyncio
import html
import logging
import os
import re
import time
import urllib.parse
from collections import deque
from typing import Dict, List, Any, Set

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
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

log = logging.getLogger("pro_news_bot")


# ============================================================
# ENVIRONMENT
# ============================================================

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()

GEMINI_MODEL = "gemini-3.5-flash"


# ============================================================
# TIMEOUTS / LIMITS
# ============================================================

NEWS_COLLECTION_TIMEOUT = 25
GEMINI_TIMEOUT = 35

MAX_SEARCH_RESULTS = 25
PER_PAGE = 5

CACHE_TTL = 300

# فحص الأخبار العاجلة كل 3 دقائق
URGENT_MONITOR_INTERVAL = 180

# تأخير بسيط بعد إنشاء خط الأساس
URGENT_INITIAL_DELAY = 30

# أقصى عدد مفاتيح محفوظة للأخبار التي تم إرسالها
MAX_SENT_URGENT_KEYS = 500


# ============================================================
# BASIC VALIDATION
# ============================================================

if not BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")


# ============================================================
# GEMINI
# ============================================================

ai_client = None

if GEMINI_API_KEY:
    try:
        ai_client = genai.Client(api_key=GEMINI_API_KEY)
        log.info("Gemini analysis layer enabled.")
    except Exception:
        log.exception(
            "Failed to initialize Gemini. "
            "Bot will continue without AI."
        )
        ai_client = None
else:
    log.warning(
        "GEMINI_API_KEY not found. "
        "Bot will continue without AI."
    )


# ============================================================
# SIMPLE MEMORY CACHE
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


NEWS_CACHE = SimpleCache(ttl=CACHE_TTL)


# ============================================================
# USER STATE
# ============================================================

USER_LOCKS: Dict[int, asyncio.Lock] = {}

# المستخدمون المسجلون لاستقبال التنبيهات
ALERT_USERS: Set[int] = set()

# المستخدمون الذين أوقفوا التنبيهات
MUTED_USERS: Set[int] = set()

# الأخبار العاجلة التي سبق إرسالها
SENT_URGENT_KEYS = deque(
    maxlen=MAX_SENT_URGENT_KEYS
)

# لمنع تشغيل المراقب أكثر من مرة
URGENT_MONITOR_STARTED = False

# لمنع إرسال الأخبار الموجودة عند بداية التشغيل
URGENT_BASELINE_READY = False


# ============================================================
# TOPICS
# ============================================================

TOPICS = {
    "econ": (
        "📈 اقتصاد وأسواق",
        [
            "اقتصاد",
            "الاقتصادية",
            "أسواق",
            "أسهم",
            "بورصة",
            "الذهب",
            "الفيدرالي",
            "فائدة",
            "عملات",
            "عملات رقمية",
            "بيتكوين",
            "تداول",
            "النفط",
            "أوبك",
            "خام",
            "تضخم",
            "أسواق المال",
            "برنت",
            "طاقة",
            "غاز",
            "استثمار",
        ],
    ),
    "forg": (
        "🏛 بيانات رسمية",
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
            "الحكومة",
            "الرئاسة",
        ],
    ),
    "urg": (
        "🚨 عاجل",
        [
            "عاجل",
            "طارئ",
            "هجوم",
            "انفجار",
            "قصف",
            "صاروخ",
            "زلزال",
            "اشتباك",
            "غارة",
            "إخلاء",
            "حالة طوارئ",
            "تحذير عاجل",
            "هجوم مسلح",
            "أزمة",
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
            "العراق",
            "إيران",
            "اليمن",
            "سوريا",
            "لبنان",
            "الأردن",
            "فلسطين",
            "إسرائيل",
            "الخليج",
            "الشرق الأوسط",
        ],
    ),
    "wrld": (
        "🌐 العالم",
        [
            "أمريكا",
            "الولايات المتحدة",
            "أوروبا",
            "الصين",
            "روسيا",
            "أوكرانيا",
            "واشنطن",
            "بكين",
            "موسكو",
            "الهند",
            "اليابان",
            "أستراليا",
            "أفريقيا",
            "أمريكا الجنوبية",
            "دولية",
            "قمة",
            "دولي",
        ],
    ),
    "secu": (
        "🛡 دفاع وأمن",
        [
            "الدفاع",
            "الأمن القومي",
            "تسليح",
            "مناورات",
            "عسكري",
            "جيش",
            "قوات",
            "أمن",
            "دفاع",
            "قاعدة عسكرية",
            "أسلحة",
            "صاروخ",
            "طيران عسكري",
        ],
    ),
}


# ============================================================
# USER REGISTRATION
# ============================================================

def register_user(user_id: int):
    ALERT_USERS.add(user_id)


# ============================================================
# TEXT HELPERS
# ============================================================

def safe_html(value: Any) -> str:
    return html.escape(str(value or ""))


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()

    # إزالة التشكيل
    text = re.sub(
        r"[\u064B-\u065F\u0670]",
        "",
        text,
    )

    replacements = {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ى": "ي",
        "ة": "ه",
        "ؤ": "و",
        "ئ": "ي",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    text = re.sub(
        r"[^\w\s\u0600-\u06FF-]",
        " ",
        text,
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    )

    return text.strip()


# ============================================================
# SAFE URL
# ============================================================

def build_safe_link(
    title: str,
    source: str,
    raw_url: str,
) -> str:

    raw_url = str(raw_url or "").strip()

    if re.match(
        r"^https?://",
        raw_url,
        re.IGNORECASE,
    ):
        if "news.google.com" not in raw_url.lower():
            return raw_url

    clean_title = re.sub(
        r"[^\w\s\u0600-\u06FF-]",
        " ",
        title or "",
    )

    clean_title = re.sub(
        r"\s+",
        " ",
        clean_title,
    ).strip()

    query = f"{clean_title} {source}".strip()

    return (
        "https://www.google.com/search?q="
        + urllib.parse.quote_plus(query)
    )


# ============================================================
# NEWS ITEM HELPERS
# ============================================================

def get_item_title(item) -> str:
    return (
        getattr(item, "title", "")
        or getattr(item, "caption", "")
        or ""
    ).strip()


def get_item_source(item) -> str:
    return (
        getattr(item, "source", "")
        or "مصدر إخباري"
    ).strip()


def get_item_url(item) -> str:
    return (
        getattr(item, "url", "")
        or getattr(item, "link", "")
        or ""
    ).strip()


def get_item_summary(item) -> str:
    return (
        getattr(item, "summary", "")
        or getattr(item, "description", "")
        or ""
    ).strip()


# ============================================================
# NEWS COLLECTION
# ============================================================

async def get_fresh_news(
    force_refresh: bool = False,
):

    if not force_refresh:
        cached = NEWS_CACHE.get("all_news")

        if cached is not None:
            return cached

    try:
        # مهم:
        # collect_news دالة async ولذلك نستعمل await مباشرة.
        items = await asyncio.wait_for(
            collect_news(max_items=150),
            timeout=NEWS_COLLECTION_TIMEOUT,
        )

        items = items or []

        if items:
            NEWS_CACHE.set(
                "all_news",
                items,
            )

        return items

    except asyncio.TimeoutError:
        log.warning(
            "News collection timed out."
        )
        return []

    except Exception:
        log.exception(
            "News Collection Error"
        )
        return []


# ============================================================
# TOPIC FILTER
# ============================================================

def topic_filter(
    items: list,
    keywords: List[str],
    max_results: int = 25,
) -> list:

    results = []

    normalized_keywords = [
        normalize_text(k)
        for k in keywords
        if k
    ]

    for item in items:

        title = normalize_text(
            get_item_title(item)
        )

        summary = normalize_text(
            get_item_summary(item)
        )

        source = normalize_text(
            get_item_source(item)
        )

        combined = (
            f"{title} "
            f"{summary} "
            f"{source}"
        )

        if any(
            keyword in combined
            for keyword in normalized_keywords
        ):
            results.append(item)

        if len(results) >= max_results:
            break

    return results


# ============================================================
# REPORT
# ============================================================

def generate_base_report(
    items,
    page: int = 1,
    per_page: int = PER_PAGE,
    heading: str = "📰 أبرز التغطيات والبيانات",
):

    start_idx = (
        (page - 1)
        * per_page
    )

    end_idx = start_idx + per_page

    page_items = items[
        start_idx:end_idx
    ]

    lines = [
        f"<b>{safe_html(heading)}</b>",
        "",
    ]

    for item in page_items:

        title = get_item_title(item)
        source = get_item_source(item)
        raw_url = get_item_url(item)

        if not title:
            continue

        safe_url = build_safe_link(
            title,
            source,
            raw_url,
        )

        lines.append(
            f"• <b>{safe_html(title)}</b>\n"
            f"  📍 المصدر: "
            f"<code>{safe_html(source)}</code>\n"
            f'  <a href="{safe_html(safe_url)}">'
            f"🔗 قراءة الخبر"
            f"</a>"
        )

        lines.append("")

    return "\n".join(lines)


# ============================================================
# RESULT KEYBOARD
# ============================================================

def result_keyboard(
    key: str,
    page: int,
    total_items: int,
):

    total_pages = max(
        1,
        (
            total_items
            + PER_PAGE
            - 1
        )
        // PER_PAGE,
    )

    rows = []

    navigation = []

    if page > 1:
        navigation.append(
            InlineKeyboardButton(
                "⬅️ السابقة",
                callback_data=(
                    f"t:{key}:{page - 1}"
                ),
            )
        )

    if page < total_pages:
        navigation.append(
            InlineKeyboardButton(
                "➕ المزيد",
                callback_data=(
                    f"t:{key}:{page + 1}"
                ),
            )
        )

    if navigation:
        rows.append(navigation)

    rows.append(
        [
            InlineKeyboardButton(
                "🧠 تحليل",
                callback_data=(
                    f"analyze:{key}"
                ),
            ),
            InlineKeyboardButton(
                "🔙 الرئيسية",
                callback_data="home",
            ),
        ]
    )

    return InlineKeyboardMarkup(rows)


# ============================================================
# MAIN KEYBOARD
# ============================================================

def main_keyboard(
    user_id: int,
):

    rows = []

    items = list(TOPICS.items())

    for i in range(
        0,
        len(items),
        2,
    ):

        row = []

        for key, (
            label,
            _,
        ) in items[
            i:i + 2
        ]:

            row.append(
                InlineKeyboardButton(
                    label,
                    callback_data=(
                        f"t:{key}:1"
                    ),
                )
            )

        rows.append(row)

    muted = (
        user_id
        in MUTED_USERS
    )

    alert_text = (
        "🔔 التنبيهات"
        if muted
        else
        "🔕 التنبيهات"
    )

    rows.append(
        [
            InlineKeyboardButton(
                alert_text,
                callback_data=(
                    "toggle_alerts"
                ),
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

    return InlineKeyboardMarkup(rows)


# ============================================================
# GEMINI PROMPT
# ============================================================

ANALYSIS_PROMPT = """
أنت محلل أخبار واستراتيجي معلومات.

حلل البيانات الإخبارية المعطاة فقط.

قدم:

1. أهم التطورات.
2. الدلالة المباشرة.
3. التأثير المحتمل.
4. ما الذي يستحق المتابعة.

اكتب بالعربية بأسلوب تنفيذي واضح.

لا تخترع أي معلومة غير موجودة في البيانات.

الحد الأقصى 120 كلمة.
"""


# ============================================================
# GEMINI ANALYSIS
# ============================================================

async def analyze_with_gemini(
    items,
) -> str:

    if not ai_client:
        return (
            "ℹ️ طبقة التحليل بالذكاء "
            "الاصطناعي غير متاحة حالياً.\n"
            "البوت مستمر في جمع الأخبار "
            "والبحث عنها بشكل طبيعي."
        )

    try:

        try:
            context_text = build_ai_context(
                items[:8]
            )

        except Exception:

            context_text = "\n".join(
                (
                    f"- {get_item_title(item)}"
                    f" | {get_item_source(item)}"
                )
                for item in items[:8]
            )

        prompt = (
            f"{ANALYSIS_PROMPT}\n\n"
            f"البيانات:\n"
            f"{context_text}"
        )

        response = await asyncio.wait_for(
            asyncio.to_thread(
                ai_client.models.generate_content,
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    thinking_config=(
                        types.ThinkingConfig(
                            thinking_level="low"
                        )
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
                "⚠️ لم يُرجع Gemini "
                "تحليلاً صالحاً."
            )

        return text

    except asyncio.TimeoutError:

        log.warning(
            "Gemini analysis timeout."
        )

        return (
            "⚠️ انتهت مهلة التحليل. "
            "الأخبار نفسها ما زالت تعمل "
            "بشكل طبيعي."
        )

    except Exception:

        log.exception(
            "Gemini Analysis Error"
        )

        return (
            "⚠️ تعذر التحليل بالذكاء "
            "الاصطناعي حالياً.\n"
            "البوت مستمر في جمع الأخبار "
            "والبحث عنها بشكل طبيعي."
        )


# ============================================================
# URGENT NEWS
# ============================================================

URGENT_STRONG_TERMS = {
    "عاجل",
    "طارئ",
    "هجوم",
    "هجوم مسلح",
    "انفجار",
    "قصف",
    "صاروخ",
    "زلزال",
    "غارة",
    "اشتباك",
    "إخلاء",
    "حالة طوارئ",
    "تحذير عاجل",
    "استهداف",
    "هجمات",
    "غارات",
    "اندلاع القتال",
    "اندلاع اشتباكات",
    "إطلاق النار",
    "اغتيال",
}


URGENT_CONTEXT_TERMS = {
    "الحكومة",
    "وزارة",
    "الدفاع",
    "الداخلية",
    "رئاسة",
    "الرئاسة",
    "الجيش",
    "القوات المسلحة",
    "الأمن",
    "الشرطة",
    "الطيران",
    "الحدود",
    "مطار",
    "مضيق",
    "سفارة",
    "دبلوماسي",
    "طاقة",
    "نفط",
}


def urgent_score(
    item,
) -> int:

    title = normalize_text(
        get_item_title(item)
    )

    summary = normalize_text(
        get_item_summary(item)
    )

    source = normalize_text(
        get_item_source(item)
    )

    if not title:
        return 0

    score = 0

    title_strong = 0
    context_hits = 0

    for term in URGENT_STRONG_TERMS:

        normalized = normalize_text(
            term
        )

        if normalized in title:
            score += 5
            title_strong += 1

        elif normalized in summary:
            score += 2

    for term in URGENT_CONTEXT_TERMS:

        normalized = normalize_text(
            term
        )

        if normalized in title:
            score += 2
            context_hits += 1

        elif normalized in summary:
            score += 1

    source_lower = source.lower()

    trusted_urgent_sources = (
        "العربية",
        "الجزيرة",
        "سكاي نيوز",
        "الشرق",
        "رويترز",
        "reuters",
        "bbc",
        "فرانس 24",
        "فرانس24",
    )

    if any(
        src in source_lower
        for src in trusted_urgent_sources
    ):
        score += 2

    if (
        title_strong >= 1
        and context_hits >= 1
    ):
        score += 5

    return score


# ============================================================
# URGENT UNIQUE KEY
# ============================================================

def urgent_key(
    item,
) -> str:

    title = normalize_text(
        get_item_title(item)
    )

    source = normalize_text(
        get_item_source(item)
    )

    raw = (
        f"{title}|{source}"
    )

    return raw[:500]


# ============================================================
# FIND URGENT
# ============================================================

def find_new_urgent_news(
    items,
    limit: int = 3,
):

    candidates = []

    for item in items:

        score = urgent_score(item)

        if score < 8:
            continue

        key = urgent_key(item)

        if not key:
            continue

        if key in SENT_URGENT_KEYS:
            continue

        candidates.append(
            (
                score,
                item,
            )
        )

    candidates.sort(
        key=lambda x: x[0],
        reverse=True,
    )

    return [
        item
        for _, item
        in candidates[:limit]
    ]


# ============================================================
# FORMAT URGENT ALERT
# ============================================================

def format_urgent_alert(
    item,
) -> str:

    title = get_item_title(item)
    source = get_item_source(item)
    raw_url = get_item_url(item)

    safe_url = build_safe_link(
        title,
        source,
        raw_url,
    )

    return (
        "🚨 <b>تنبيه عاجل</b>\n\n"
        f"<b>{safe_html(title)}</b>\n\n"
        f"📍 المصدر: "
        f"<code>{safe_html(source)}</code>\n"
        f'<a href="{safe_html(safe_url)}">'
        f"🔗 قراءة الخبر"
        f"</a>"
    )


# ============================================================
# INITIAL BASELINE
# ============================================================

async def initialize_urgent_baseline():
    """
    أول تشغيل:
    يجلب الأخبار الحالية ويضع الأخبار العاجلة
    الموجودة مسبقاً في قائمة 'شوهدت'.

    الهدف:
    لا نرسل خبراً قديماً كأنه خبر جديد.
    """

    global URGENT_BASELINE_READY

    if URGENT_BASELINE_READY:
        return

    log.info(
        "Creating urgent-news startup baseline..."
    )

    try:

        items = await get_fresh_news(
            force_refresh=True
        )

        if not items:
            log.warning(
                "Could not create urgent baseline "
                "because no news was collected."
            )

            return

        existing_urgent = []

        for item in items:

            score = urgent_score(item)

            if score < 8:
                continue

            key = urgent_key(item)

            if not key:
                continue

            existing_urgent.append(key)

        for key in existing_urgent:
            SENT_URGENT_KEYS.append(key)

        URGENT_BASELINE_READY = True

        log.info(
            "Urgent baseline ready. "
            "Marked %d existing urgent stories as seen.",
            len(existing_urgent),
        )

    except Exception:

        log.exception(
            "Failed to create urgent-news baseline."
        )


# ============================================================
# AUTOMATIC URGENT MONITOR
# ============================================================

async def urgent_monitor(
    application: Application,
):

    log.info(
        "Automatic urgent-news monitor started. "
        "Interval=%ss",
        URGENT_MONITOR_INTERVAL,
    )

    # --------------------------------------------------------
    # مهم جداً:
    # إنشاء خط أساس قبل إرسال أي تنبيه
    # --------------------------------------------------------

    await initialize_urgent_baseline()

    # --------------------------------------------------------
    # إعطاء النظام فترة قصيرة للاستقرار
    # --------------------------------------------------------

    await asyncio.sleep(
        URGENT_INITIAL_DELAY
    )

    while True:

        try:

            if ALERT_USERS:

                log.info(
                    "Urgent monitor: checking "
                    "fresh news for %d user(s).",
                    len(ALERT_USERS),
                )

                items = await get_fresh_news(
                    force_refresh=True
                )

                if items:

                    urgent_items = (
                        find_new_urgent_news(
                            items,
                            limit=3,
                        )
                    )

                    for item in urgent_items:

                        key = urgent_key(
                            item
                        )

                        if not key:
                            continue

                        # تسجيل الخبر قبل الإرسال
                        # لمنع التكرار
                        SENT_URGENT_KEYS.append(
                            key
                        )

                        message = (
                            format_urgent_alert(
                                item
                            )
                        )

                        for user_id in list(
                            ALERT_USERS
                        ):

                            if (
                                user_id
                                in MUTED_USERS
                            ):
                                continue

                            try:

                                await application.bot.send_message(
                                    chat_id=user_id,
                                    text=message,
                                    parse_mode="HTML",
                                    disable_web_page_preview=False,
                                    disable_notification=False,
                                )

                                log.info(
                                    "Automatic urgent "
                                    "alert sent to user %s",
                                    user_id,
                                )

                            except Exception:

                                log.exception(
                                    "Failed to send "
                                    "urgent alert "
                                    "to user %s",
                                    user_id,
                                )

        except asyncio.CancelledError:

            log.info(
                "Urgent monitor stopped."
            )

            raise

        except Exception:

            log.exception(
                "Unexpected error "
                "in urgent monitor."
            )

        await asyncio.sleep(
            URGENT_MONITOR_INTERVAL
        )


# ============================================================
# POST INIT
# ============================================================

async def post_init(
    application: Application,
):

    global URGENT_MONITOR_STARTED

    if URGENT_MONITOR_STARTED:
        return

    URGENT_MONITOR_STARTED = True

    application.create_task(
        urgent_monitor(application),
        name="urgent-news-monitor",
    )


# ============================================================
# /START
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    user = update.effective_user

    if not user:
        return

    user_id = user.id

    register_user(user_id)

    await update.message.reply_text(
        "🏛 <b>منصة الأخبار والبيانات "
        "الرسمية الشاملة</b>\n\n"
        "اختر القطاع المطلوب لمتابعة "
        "التغطية الحية والمتخصصة.\n\n"
        "🚨 التنبيهات العاجلة تعمل تلقائياً "
        "ويمكن إيقافها من الزر.",
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
        return

    user_id = user.id

    register_user(user_id)

    data = query.data or ""


    # ========================================================
    # ALERT TOGGLE
    # ========================================================

    if data == "toggle_alerts":

        if user_id in MUTED_USERS:

            MUTED_USERS.discard(
                user_id
            )

            # Popup حقيقي
            await query.answer(
                text=(
                    "🔔 تم تفعيل "
                    "التنبيهات العاجلة."
                ),
                show_alert=True,
            )

        else:

            MUTED_USERS.add(
                user_id
            )

            # Popup حقيقي
            await query.answer(
                text=(
                    "🔕 تم إيقاف "
                    "التنبيهات العاجلة."
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
            pass

        return


    # ========================================================
    # HOME
    # ========================================================

    if data == "home":

        await query.answer(
            text="🏠 الرئيسية",
            show_alert=False,
        )

        await query.message.reply_text(
            "📰 <b>القائمة الرئيسية</b>",
            reply_markup=main_keyboard(
                user_id
            ),
            parse_mode="HTML",
        )

        return


    # ========================================================
    # REFRESH
    # ========================================================

    if data == "refresh":

        await query.answer(
            text=(
                "🔄 جاري تحديث "
                "الأخبار..."
            ),
            show_alert=True,
        )

        NEWS_CACHE.set(
            "all_news",
            None,
        )

        # إرسال رسالة حالة
        status = await query.message.reply_text(
            "📡 جاري جلب آخر الأخبار..."
        )

        try:

            items = await get_fresh_news(
                force_refresh=True
            )

            if not items:

                await status.edit_text(
                    "⚠️ تعذر الحصول على "
                    "الأخبار حالياً."
                )

                return

            await status.edit_text(
                "✅ تم تحديث الأخبار بنجاح.\n\n"
                "اختر القسم المطلوب من القائمة "
                "الرئيسية.",
                reply_markup=main_keyboard(
                    user_id
                ),
            )

        except Exception:

            log.exception(
                "Refresh Handler Exception"
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء تحديث الأخبار."
            )

        return


    # ========================================================
    # MORE
    # ========================================================

    if data == "more":

        await query.answer(
            text="🔎 البحث متاح الآن",
            show_alert=False,
        )

        await query.message.reply_text(
            "➕ <b>المزيد</b>\n\n"
            "يمكنك البحث مباشرة بكتابة "
            "اسم دولة أو مدينة أو موضوع.\n\n"
            "مثال:\n"
            "السعودية\n"
            "السعودية النفط\n"
            "ألمانيا\n"
            "البنك المركزي الأوروبي",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔙 الرئيسية",
                            callback_data="home",
                        )
                    ]
                ]
            ),
        )

        return


    # ========================================================
    # ANALYSIS
    # ========================================================

    if data.startswith("analyze:"):

        # إجابة واحدة فقط للـ callback
        await query.answer(
            text="🧠 جاري تجهيز التحليل...",
            show_alert=False,
        )

        key = data.split(
            ":",
            1,
        )[1]

        if key not in TOPICS:
            return

        status = await query.message.reply_text(
            "🧠 جاري تحليل البيانات..."
        )

        try:

            items = await get_fresh_news()

            if not items:

                await status.edit_text(
                    "⚠️ لا توجد بيانات "
                    "متاحة للتحليل حالياً."
                )

                return

            _, keywords = TOPICS[key]

            results = topic_filter(
                items,
                keywords,
                max_results=8,
            )

            if not results:

                await status.edit_text(
                    "⚠️ لا توجد بيانات "
                    "كافية للتحليل."
                )

                return

            analysis = await analyze_with_gemini(
                results
            )

            await status.edit_text(
                "🧠 <b>التحليل التنفيذي</b>\n\n"
                + safe_html(analysis),
                parse_mode="HTML",
            )

        except Exception:

            log.exception(
                "Analysis Handler Exception"
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء التحليل."
            )

        return


    # ========================================================
    # TOPIC / PAGINATION
    # ========================================================

    if data.startswith("t:"):

        parts = data.split(":")

        if len(parts) != 3:

            await query.answer()
            return

        _, key, page_text = parts

        try:

            page = int(page_text)

        except ValueError:

            await query.answer()
            return

        if key not in TOPICS:

            await query.answer()
            return

        # ----------------------------------------------------
        # مهم:
        # إجابة واحدة فقط.
        # لا نستخدم query.answer مرة ثانية إذا كان lock مقفول.
        # ----------------------------------------------------

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

        await query.answer(
            text="📡 جاري تحميل الأخبار...",
            show_alert=False,
        )

        async with lock:

            status = await query.message.reply_text(
                "📡 جاري جمع وفرز الأخبار..."
            )

            try:

                items = await get_fresh_news()

                if not items:

                    await status.edit_text(
                        "⚠️ تعذر الحصول على "
                        "الأخبار حالياً."
                    )

                    return

                _, keywords = TOPICS[key]

                results = topic_filter(
                    items,
                    keywords,
                    max_results=MAX_SEARCH_RESULTS,
                )

                if not results:

                    await status.edit_text(
                        "🔎 لا توجد أخبار مناسبة "
                        "لهذا القسم حالياً."
                    )

                    return

                report = generate_base_report(
                    results,
                    page=page,
                    per_page=PER_PAGE,
                    heading=TOPICS[key][0],
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
                    "Topic Handler Exception"
                )

                await status.edit_text(
                    "⚠️ حدث خطأ أثناء عرض البيانات."
                )

        return


    # ========================================================
    # UNKNOWN CALLBACK
    # ========================================================

    await query.answer(
        text="⚠️ أمر غير معروف.",
        show_alert=False,
    )


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
        update.message.text
        or ""
    ).strip()

    if not text:
        return

    user = update.effective_user

    if not user:
        return

    user_id = user.id

    register_user(user_id)

    lock = USER_LOCKS.setdefault(
        user_id,
        asyncio.Lock(),
    )

    if lock.locked():

        await update.message.reply_text(
            "⏳ يوجد بحث جارٍ حالياً..."
        )

        return

    async with lock:

        status = await update.message.reply_text(
            "🔎 جاري البحث في كافة "
            "التغطيات عن:\n"
            f"<b>{safe_html(text)}</b>...",
            parse_mode="HTML",
        )

        try:

            # ------------------------------------------------
            # البحث هنا مستقل تماماً عن القسم الحالي.
            # لا يتم تمرير TOPICS أو أي قسم سابق.
            # ------------------------------------------------

            results = await hybrid_search_news(
                text,
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
                per_page=PER_PAGE,
                heading=(
                    f"🔎 نتائج البحث: {text}"
                ),
            )

            await status.edit_text(
                report,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🔙 الرئيسية",
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
# ERROR HANDLER
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):

    log.exception(
        "Unhandled Telegram error:",
        exc_info=context.error,
    )


# ============================================================
# MAIN
# ============================================================

def main():

    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            button_handler,
            pattern=(
                r"^(t:.*|home|refresh|more|"
                r"toggle_alerts|analyze:.*)$"
            ),
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND,
            handle_user_message,
        )
    )

    application.add_error_handler(
        error_handler
    )

    log.info(
        "Pro News Bot launched successfully."
    )

    log.info(
        "Automatic urgent monitor interval: %s seconds.",
        URGENT_MONITOR_INTERVAL,
    )

    application.run_polling(
        drop_pending_updates=True
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
