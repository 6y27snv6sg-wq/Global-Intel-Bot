import asyncio
import hashlib
import html
import logging
import os
import re
import time
import urllib.parse
from collections import deque
from typing import Any, Dict, List, Set

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
    hybrid_search_news,
    build_ai_context,
    is_topic_match,
    deduplicate_news,
)

try:
    from deep_translator import GoogleTranslator
except Exception:
    GoogleTranslator = None

try:
    from themes import (
        CUSTOM_EMOJI_PACK,
        custom_emoji_html,
        load_custom_emoji_ids,
        status_theme,
        theme_emoji,
    )
except Exception:
    CUSTOM_EMOJI_PACK = None

    def custom_emoji_html(*args, **kwargs):
        return ""

    async def load_custom_emoji_ids(*args, **kwargs):
        return {}

    def status_theme(status):
        return status

    def theme_emoji(theme):
        return ""


logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)

log = logging.getLogger("pro_news_bot")

BOT_TOKEN = (
    os.getenv("TELEGRAM_BOT_TOKEN") or ""
).strip()

GEMINI_API_KEY = (
    os.getenv("GEMINI_API_KEY") or ""
).strip()

GEMINI_MODEL = os.getenv(
    "GEMINI_MODEL",
    "gemini-3.5-flash",
).strip()

NEWS_COLLECTION_TIMEOUT = 25
SEARCH_TIMEOUT = 14
GEMINI_TIMEOUT = 35
TRANSLATION_TIMEOUT = 12

MAX_SEARCH_RESULTS = 25
PER_PAGE = 5
CACHE_TTL = 300

URGENT_MONITOR_INTERVAL = 180
URGENT_INITIAL_DELAY = 30
MAX_SENT_URGENT_KEYS = 500

if not BOT_TOKEN:
    raise RuntimeError(
        "Missing TELEGRAM_BOT_TOKEN"
    )


ai_client = None

if GEMINI_API_KEY:
    try:
        ai_client = genai.Client(
            api_key=GEMINI_API_KEY
        )
        log.info(
            "Gemini analysis layer enabled."
        )
    except Exception:
        log.exception(
            "Failed to initialize Gemini."
        )
else:
    log.warning(
        "GEMINI_API_KEY not found."
    )


class SimpleCache:
    def __init__(self, ttl=300):
        self.ttl = ttl
        self._cache = {}
        self._timestamps = {}

    def get(self, key):
        value = self._cache.get(key)

        if value is None:
            return None

        if (
            time.time()
            - self._timestamps.get(key, 0)
            < self.ttl
        ):
            return value

        self._cache.pop(key, None)
        self._timestamps.pop(key, None)

        return None

    def set(self, key, value):
        if value is None:
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)
            return

        self._cache[key] = value
        self._timestamps[key] = time.time()


NEWS_CACHE = SimpleCache(CACHE_TTL)
TRANSLATION_CACHE = SimpleCache(86400)

USER_SEARCH_RESULTS: Dict[
    int,
    List[Any],
] = {}

USER_LOCKS: Dict[
    int,
    asyncio.Lock,
] = {}

ALERT_USERS: Set[int] = set()
MUTED_USERS: Set[int] = set()

SENT_URGENT_KEYS = deque(
    maxlen=MAX_SENT_URGENT_KEYS
)

CUSTOM_EMOJI_IDS = {}

URGENT_MONITOR_STARTED = False
URGENT_BASELINE_READY = False
URGENT_MONITOR_TASK = None

TRANSLATION_SEMAPHORE = asyncio.Semaphore(3)


TOPICS = {
    "econ": (
        "📈 اقتصاد وأسواق",
        [
            "اقتصاد", "اقتصادي", "أسواق", "أسهم",
            "بورصة", "الذهب", "فائدة", "عملات",
            "عملات رقمية", "بيتكوين", "تداول",
            "نفط", "أوبك", "خام", "تضخم",
            "أسواق المال", "برنت", "طاقة",
            "غاز", "استثمار", "سندات",
            "ميزانية", "ناتج محلي",
            "بنك مركزي", "دولار",
            "واردات", "صادرات", "استثمارات",
        ],
    ),
    "forg": (
        "🏛 بيانات رسمية",
        [
            "بيان رسمي",
            "تصريح رسمي",
            "بيان صحفي",
            "المتحدث الرسمي",
            "المتحدث باسم",
            "مصدر مسؤول",
            "أعلنت الوزارة",
            "أعلن الوزير",
            "قالت الوزارة",
            "قال الوزير",
            "وزارة الخارجية",
            "وزارة الدفاع",
            "وزارة الداخلية",
            "رئاسة الوزراء",
            "الديوان الملكي",
            "الحكومة تعلن",
            "الحكومة تؤكد",
            "الرئاسة تعلن",
            "الرئاسة تؤكد",
            "السفير",
            "المتحدث",
        ],
    ),
    "urg": (
        "🚨 عاجل",
        [
            "عاجل", "طارئ", "هجوم",
            "انفجار", "قصف", "صاروخ",
            "زلزال", "اشتباك",
            "غارة", "إخلاء",
            "حالة طوارئ",
            "تحذير عاجل",
            "هجوم مسلح",
            "أزمة", "استهداف",
            "غارات", "إطلاق النار",
        ],
    ),
    "gulf": (
        "🌍 الشرق الأوسط",
        [
            "السعودية", "الإمارات", "قطر",
            "الكويت", "البحرين", "عمان",
            "العراق", "إيران", "اليمن",
            "سوريا", "لبنان", "الأردن",
            "فلسطين", "إسرائيل",
            "الخليج", "الشرق الأوسط",
        ],
    ),
    "wrld": (
        "🌐 العالم",
        [
            "أمريكا", "الولايات المتحدة",
            "أوروبا", "الصين", "روسيا",
            "أوكرانيا", "واشنطن", "بكين",
            "موسكو", "الهند", "اليابان",
            "أستراليا", "أفريقيا",
            "أمريكا الجنوبية", "دولية",
            "قمة",
        ],
    ),
    "secu": (
        "🛡 دفاع وأمن",
        [
            "الدفاع", "الأمن القومي",
            "تسليح", "مناورات", "عسكري",
            "جيش", "قوات", "أمن",
            "دفاع", "قاعدة عسكرية",
            "أسلحة", "صاروخ",
            "طيران عسكري", "قصف",
            "هجوم", "اشتباك",
            "غارة", "استهداف",
            "عملية عسكرية",
            "عمليات عسكرية",
            "قوات خاصة",
            "دفاع جوي",
            "منظومة دفاع",
            "ذخائر", "مقاتلات",
            "طائرات مسيرة",
        ],
    ),
}


SEARCH_ALIASES = {
    "بريطانيا": [
        "المملكة المتحدة",
        "UK",
        "United Kingdom",
    ],
    "انجلترا": [
        "إنجلترا",
        "بريطانيا",
        "المملكة المتحدة",
    ],
    "امريكا": [
        "أمريكا",
        "الولايات المتحدة",
        "USA",
        "United States",
    ],
    "السعوديه": [
        "السعودية",
        "المملكة العربية السعودية",
        "Saudi Arabia",
    ],
    "الامارات": [
        "الإمارات",
        "الإمارات العربية المتحدة",
        "UAE",
    ],
    "روسيا": [
        "روسيا",
        "Russia",
    ],
    "اوكرانيا": [
        "أوكرانيا",
        "Ukraine",
    ],
    "الصين": [
        "الصين",
        "China",
    ],
    "اليابان": [
        "اليابان",
        "Japan",
    ],
    "المانيا": [
        "ألمانيا",
        "Germany",
    ],
    "فرنسا": [
        "فرنسا",
        "France",
    ],
    "ايران": [
        "إيران",
        "Iran",
    ],
    "اسرائيل": [
        "إسرائيل",
        "Israel",
    ],
}


def register_user(user_id):
    ALERT_USERS.add(user_id)


def safe_html(value):
    return html.escape(
        str(value or "")
    )


def normalize_text(value):
    text = (
        str(value or "")
        .strip()
        .lower()
    )

    text = re.sub(
        r"[\u064B-\u065F\u0670]",
        "",
        text,
    )

    for old, new in {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ى": "ي",
        "ة": "ه",
        "ؤ": "و",
        "ئ": "ي",
    }.items():
        text = text.replace(
            old,
            new,
        )

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


def get_item_title(item):
    return (
        getattr(item, "title", "")
        or getattr(item, "caption", "")
        or ""
    ).strip()


def get_item_source(item):
    return (
        getattr(item, "source", "")
        or "مصدر إخباري"
    ).strip()


def get_item_url(item):
    return (
        getattr(item, "url", "")
        or getattr(item, "link", "")
        or ""
    ).strip()


def get_item_summary(item):
    return (
        getattr(item, "summary", "")
        or getattr(item, "description", "")
        or ""
    ).strip()


def expand_search_query(query):
    normalized = normalize_text(query)

    values = [query]

    for key, aliases in SEARCH_ALIASES.items():
        if normalize_text(key) in normalized:
            values.extend(aliases)

    seen = set()
    result = []

    for value in values:
        marker = normalize_text(value)

        if marker and marker not in seen:
            seen.add(marker)
            result.append(value)

    return " ".join(result)


def build_safe_link(
    title,
    source,
    raw_url,
):
    raw_url = str(
        raw_url or ""
    ).strip()

    if re.match(
        r"^https?://",
        raw_url,
        re.I,
    ):
        parsed = urllib.parse.urlparse(
            raw_url
        )

        if parsed.hostname:
            return raw_url

    return (
        "https://www.google.com/search?q="
        + urllib.parse.quote_plus(
            f"{title} {source}".strip()
        )
    )


def visual(theme):
    try:
        return custom_emoji_html(
            CUSTOM_EMOJI_IDS.get(theme),
            theme_emoji(theme),
        )
    except Exception:
        return ""


def status_visual(status):
    try:
        return visual(
            status_theme(status)
        )
    except Exception:
        return ""


async def initialize_custom_emoji_pack(
    application,
):
    global CUSTOM_EMOJI_IDS

    try:
        if CUSTOM_EMOJI_PACK:
            CUSTOM_EMOJI_IDS = (
                await load_custom_emoji_ids(
                    application.bot,
                    CUSTOM_EMOJI_PACK,
                )
            )
    except Exception:
        CUSTOM_EMOJI_IDS = {}

        log.exception(
            "Custom emoji pack failed; fallback enabled."
        )


async def send_status_theme(
    message,
    status,
):
    return None


async def get_fresh_news(
    force_refresh=False,
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

    except asyncio.TimeoutError:
        log.warning(
            "News collection timed out."
        )
        return []

    except Exception:
        log.exception(
            "News collection failed."
        )
        return []


def topic_filter(
    items,
    topic_key,
    max_results=25,
):
    if topic_key not in TOPICS:
        return []

    scored = []

    for item in items:
        if not is_topic_match(
            item,
            topic_key,
        ):
            continue

        title = normalize_text(
            get_item_title(item)
        )

        summary = normalize_text(
            get_item_summary(item)
        )

        score = 0

        _, keywords = TOPICS[
            topic_key
        ]

        for keyword in keywords:
            normalized_keyword = (
                normalize_text(keyword)
            )

            if not normalized_keyword:
                continue

            if normalized_keyword in title:
                score += 12

            elif normalized_keyword in summary:
                score += 4

        if topic_key == "urg":
            score += 20

        if (
            topic_key == "forg"
            and getattr(
                item,
                "official",
                False,
            )
        ):
            score += 12

        scored.append(
            (
                score
                + float(
                    getattr(
                        item,
                        "relevance_score",
                        0,
                    )
                    or 0
                ),
                item,
            )
        )

    scored.sort(
        key=lambda x: x[0],
        reverse=True,
    )

    return deduplicate_news(
        [
            item
            for _, item in scored[
                :max_results * 2
            ]
        ]
    )[:max_results]


def _contains_arabic(text):
    return bool(
        re.search(
            r"[\u0600-\u06FF]",
            text or "",
        )
    )


async def translate_title(title):
    title = (
        title or ""
    ).strip()

    if not title:
        return title

    if (
        _contains_arabic(title)
        and not re.search(
            r"[A-Za-z]",
            title,
        )
    ):
        return title

    key = normalize_text(title)

    cached = TRANSLATION_CACHE.get(
        key
    )

    if cached:
        return cached

    # Gemini is deliberately NOT used for translation.
    if GoogleTranslator is None:
        return title

    try:
        async with TRANSLATION_SEMAPHORE:
            translated = await asyncio.wait_for(
                asyncio.to_thread(
                    lambda: GoogleTranslator(
                        source="auto",
                        target="ar",
                    ).translate(title)
                ),
                timeout=TRANSLATION_TIMEOUT,
            )

        translated = (
            translated or ""
        ).strip()

        if translated and _contains_arabic(
            translated
        ):
            TRANSLATION_CACHE.set(
                key,
                translated,
            )
            return translated

    except Exception:
        log.warning(
            "Independent title translation failed; "
            "using original title."
        )

    return title


async def prepare_display_titles(
    items,
):
    pairs = await asyncio.gather(
        *(
            translate_title(
                get_item_title(item)
            )
            for item in items
        ),
        return_exceptions=True,
    )

    result = {}

    for item, value in zip(
        items,
        pairs,
    ):
        title = get_item_title(item)

        if (
            isinstance(value, Exception)
            or not value
        ):
            result[id(item)] = title
        else:
            result[id(item)] = value

    return result


def generate_base_report(
    items,
    page=1,
    per_page=5,
    heading="📰 الأخبار",
    display_titles=None,
):
    start = max(
        0,
        (page - 1) * per_page,
    )

    page_items = items[
        start:start + per_page
    ]

    display_titles = (
        display_titles or {}
    )

    lines = [
        f"<b>{safe_html(heading)}</b>",
        "",
    ]

    for item in page_items:
        title = display_titles.get(
            id(item),
            get_item_title(item),
        )

        source = get_item_source(
            item
        )

        if not title:
            continue

        safe_url = build_safe_link(
            get_item_title(item),
            source,
            get_item_url(item),
        )

        lines.append(
            f"• <b>{safe_html(title)}</b>\n"
            f"  📍 المصدر: "
            f"<code>{safe_html(source)}</code>\n"
            f'  <a href="{safe_html(safe_url)}">'
            f"🔗 قراءة الخبر</a>"
        )

        lines.append("")

    return "\n".join(
        lines
    ).strip()


def result_keyboard(
    key,
    page,
    total_items,
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

    nav = []

    if page > 1:
        nav.append(
            InlineKeyboardButton(
                "⬅️ السابقة",
                callback_data=(
                    f"t:{key}:{page-1}"
                ),
            )
        )

    if page < total_pages:
        nav.append(
            InlineKeyboardButton(
                "➕ المزيد",
                callback_data=(
                    f"t:{key}:{page+1}"
                ),
            )
        )

    rows = (
        [nav]
        if nav
        else []
    )

    rows.append(
        [
            InlineKeyboardButton(
                "🧠 تحليل",
                callback_data=(
                    f"analyze:{key}"
                ),
            ),
            InlineKeyboardButton(
                "🏠 مركز الأخبار",
                callback_data="home",
            ),
        ]
    )

    return InlineKeyboardMarkup(
        rows
    )


def search_result_keyboard(
    user_id,
    page,
):
    results = USER_SEARCH_RESULTS.get(
        user_id,
        [],
    )

    total_pages = max(
        1,
        (
            len(results)
            + PER_PAGE
            - 1
        )
        // PER_PAGE,
    )

    nav = []

    if page > 1:
        nav.append(
            InlineKeyboardButton(
                "⬅️ السابقة",
                callback_data=(
                    f"s:{page-1}"
                ),
            )
        )

    if page < total_pages:
        nav.append(
            InlineKeyboardButton(
                "➕ المزيد",
                callback_data=(
                    f"s:{page+1}"
                ),
            )
        )

    rows = (
        [nav]
        if nav
        else []
    )

    rows.append(
        [
            InlineKeyboardButton(
                "🏠 مركز الأخبار",
                callback_data="home",
            )
        ]
    )

    return InlineKeyboardMarkup(
        rows
    )


def main_keyboard(user_id):
    rows = []

    topic_items = list(
        TOPICS.items()
    )

    for i in range(
        0,
        len(topic_items),
        2,
    ):
        rows.append(
            [
                InlineKeyboardButton(
                    label,
                    callback_data=(
                        f"t:{key}:1"
                    ),
                )
                for key, (
                    label,
                    _,
                ) in topic_items[
                    i:i + 2
                ]
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                (
                    "🔔 التنبيهات"
                    if user_id
                    in MUTED_USERS
                    else "🔕 التنبيهات"
                ),
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

    return InlineKeyboardMarkup(
        rows
    )


ANALYSIS_PROMPT = """
أنت محلل أخبار واستراتيجي معلومات.
حلل البيانات المعطاة فقط.

قدم:
1. أهم التطورات.
2. الدلالة المباشرة.
3. التأثير المحتمل.
4. ما يستحق المتابعة.

اكتب بالعربية التنفيذية الواضحة.
لا تخترع أي معلومة غير موجودة.
الحد الأقصى 120 كلمة.
"""


async def analyze_with_gemini(
    items,
):
    if not ai_client:
        return (
            "ℹ️ طبقة التحليل غير متاحة حالياً، "
            "لكن جمع الأخبار والبحث يعملان."
        )

    try:
        context = build_ai_context(
            items[:8]
        )

        response = await asyncio.wait_for(
            asyncio.to_thread(
                ai_client.models.generate_content,
                model=GEMINI_MODEL,
                contents=(
                    f"{ANALYSIS_PROMPT}\n\n"
                    f"البيانات:\n{context}"
                ),
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

        return (
            getattr(
                response,
                "text",
                None,
            )
            or ""
        ).strip() or (
            "⚠️ لم يُرجع التحليل نتيجة."
        )

    except Exception:
        log.exception(
            "Gemini analysis failed."
        )

        return (
            "⚠️ تعذر التحليل بالذكاء الاصطناعي حالياً."
        )


URGENT_STRONG_TERMS = {
    "عاجل", "طارئ", "هجوم", "انفجار",
    "قصف", "صاروخ", "زلزال", "غارة",
    "اشتباك", "إخلاء", "استهداف",
    "هجمات", "غارات", "اندلاع القتال",
    "اندلاع اشتباكات", "إطلاق النار",
    "اغتيال",
}


def urgent_score(item):
    title = normalize_text(
        get_item_title(item)
    )

    summary = normalize_text(
        get_item_summary(item)
    )

    score = 0
    strong = 0

    for term in URGENT_STRONG_TERMS:
        n = normalize_text(term)

        if n in title:
            score += 5
            strong += 1

        elif n in summary:
            score += 2

    if (
        strong
        and getattr(
            item,
            "trust_score",
            0,
        ) >= 70
    ):
        score += 3

    return score


def urgent_key(item):
    return (
        f"{normalize_text(get_item_title(item))}"
        f"|{normalize_text(get_item_source(item))}"
    )[:500]


def find_new_urgent_news(
    items,
    limit=3,
):
    candidates = []

    for item in items:
        score = urgent_score(
            item
        )

        key = urgent_key(
            item
        )

        if (
            score >= 8
            and key
            and key not in SENT_URGENT_KEYS
        ):
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
        for _, item in candidates[
            :limit
        ]
    ]


async def format_urgent_alert(
    item,
):
    title = await translate_title(
        get_item_title(item)
    )

    source = get_item_source(
        item
    )

    url = build_safe_link(
        get_item_title(item),
        source,
        get_item_url(item),
    )

    return (
        f"{visual('urgent')} "
        f"<b>تنبيه عاجل</b>\n\n"
        f"<b>{safe_html(title)}</b>\n\n"
        f"📍 المصدر: "
        f"<code>{safe_html(source)}</code>\n"
        f'<a href="{safe_html(url)}">'
        f"🔗 قراءة الخبر</a>"
    )


async def initialize_urgent_baseline():
    global URGENT_BASELINE_READY

    if URGENT_BASELINE_READY:
        return

    items = await get_fresh_news(
        force_refresh=True
    )

    for item in items:
        if urgent_score(item) >= 8:
            key = urgent_key(item)

            if key:
                SENT_URGENT_KEYS.append(
                    key
                )

    URGENT_BASELINE_READY = True


async def urgent_monitor(
    application,
):
    await initialize_urgent_baseline()

    await asyncio.sleep(
        URGENT_INITIAL_DELAY
    )

    while True:
        try:
            if ALERT_USERS:
                items = await get_fresh_news(
                    force_refresh=True
                )

                for item in find_new_urgent_news(
                    items
                ):
                    key = urgent_key(
                        item
                    )

                    message = (
                        await format_urgent_alert(
                            item
                        )
                    )

                    delivered = False

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
                            )

                            delivered = True

                        except Exception:
                            log.exception(
                                "Urgent alert send failed for %s",
                                user_id,
                            )

                    if delivered:
                        SENT_URGENT_KEYS.append(
                            key
                        )

        except asyncio.CancelledError:
            raise

        except Exception:
            log.exception(
                "Urgent monitor error."
            )

        await asyncio.sleep(
            URGENT_MONITOR_INTERVAL
        )


async def post_init(
    application,
):
        global URGENT_MONITOR_STARTED, URGENT_MONITOR_TASK

    

    if URGENT_MONITOR_STARTED:
        return

    URGENT_MONITOR_STARTED = True

    await initialize_custom_emoji_pack(
        application
    )

    URGENT_MONITOR_TASK = (
        asyncio.create_task(
            urgent_monitor(
                application
            ),
            name="urgent-news-monitor",
        )
    )


async def post_stop(
    application,
):
    global (
        URGENT_MONITOR_STARTED,
        URGENT_MONITOR_TASK,
    )

    task = URGENT_MONITOR_TASK

    URGENT_MONITOR_TASK = None
    URGENT_MONITOR_STARTED = False

    if task and not task.done():
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass


async def start(
    update,
    context,
):
    user = update.effective_user

    if not user or not update.message:
        return

    register_user(
        user.id
    )

    await update.message.reply_text(
        (
            f"{visual('world')} "
            f"<b>مركز الأخبار</b>\n\n"
            "اختر القطاع المطلوب لمتابعة "
            "التغطية الحية والمتخصصة.\n\n"
            "🚨 التنبيهات العاجلة تعمل تلقائياً "
            "ويمكن إيقافها."
        ),
        reply_markup=main_keyboard(
            user.id
        ),
        parse_mode="HTML",
    )


async def show_topic(
    query,
    user_id,
    key,
    page,
):
    lock = USER_LOCKS.setdefault(
        user_id,
        asyncio.Lock(),
    )

    if lock.locked():
        await query.answer(
            "⏳ جاري التحميل...",
            show_alert=False,
        )
        return

    await query.answer(
        "📡 جاري تحميل الأخبار...",
        show_alert=False,
    )

    async with lock:
        status = await query.message.reply_text(
            "📡 جاري جمع وفرز الأخبار..."
        )

        try:
            items = await get_fresh_news()

            results = topic_filter(
                items,
                key,
                MAX_SEARCH_RESULTS,
            )

            if not results:
                await status.edit_text(
                    "🔎 لا توجد أخبار مناسبة "
                    "لهذا القسم حالياً."
                )
                return

            display_titles = (
                await prepare_display_titles(
                    results
                )
            )

            report = generate_base_report(
                results,
                page,
                PER_PAGE,
                TOPICS[key][0],
                display_titles,
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
                "Topic handler failed."
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء عرض البيانات."
            )


async def show_search_page(
    query,
    user_id,
    page,
):
    results = USER_SEARCH_RESULTS.get(
        user_id,
        [],
    )

    if not results:
        await query.message.reply_text(
            "🔎 لا توجد نتائج بحث محفوظة."
        )
        return

    display_titles = (
        await prepare_display_titles(
            results
        )
    )

    report = generate_base_report(
        results,
        page,
        PER_PAGE,
        (
            f"{status_visual('search')} "
            "نتائج البحث"
        ),
        display_titles,
    )

    await query.message.reply_text(
        report,
        reply_markup=search_result_keyboard(
            user_id,
            page,
        ),
        disable_web_page_preview=True,
        parse_mode="HTML",
    )


async def button_handler(
    update,
    context,
):
    query = update.callback_query
    user = update.effective_user

    if not query or not user:
        return

    user_id = user.id

    register_user(
        user_id
    )

    data = query.data or ""

    if data == "toggle_alerts":
        if user_id in MUTED_USERS:
            MUTED_USERS.discard(
                user_id
            )

            await query.answer(
                "🔔 تم تفعيل التنبيهات العاجلة.",
                show_alert=True,
            )

        else:
            MUTED_USERS.add(
                user_id
            )

            await query.answer(
                "🔕 تم إيقاف التنبيهات العاجلة.",
                show_alert=True,
            )

        try:
            await query.message.edit_reply_markup(
                main_keyboard(user_id)
            )
        except Exception:
            pass

        return

    if data == "home":
        await query.answer(
            "🏠 مركز الأخبار"
        )

        await query.message.reply_text(
            "🌐 <b>مركز الأخبار</b>",
            reply_markup=main_keyboard(
                user_id
            ),
            parse_mode="HTML",
        )

        return

    if data == "refresh":
        await query.answer(
            "🔄 جاري تحديث الأخبار...",
            show_alert=True,
        )

        NEWS_CACHE.set(
            "all_news",
            None,
        )

        status = await query.message.reply_text(
            "📡 جاري جلب آخر الأخبار..."
        )

        try:
            items = await get_fresh_news(
                force_refresh=True
            )

            await status.edit_text(
                (
                    f"✅ تم تحديث الأخبار بنجاح "
                    f"({len(items)} خبر).\n\n"
                    "اختر القسم المطلوب."
                ),
                reply_markup=main_keyboard(
                    user_id
                ),
            )

        except Exception:
            log.exception(
                "Refresh failed."
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء تحديث الأخبار."
            )

        return

    if data == "more":
        await query.answer(
            "🔎 البحث متاح الآن"
        )

        await query.message.reply_text(
            (
                "➕ <b>المزيد</b>\n\n"
                "اكتب مباشرة اسم دولة أو مدينة أو موضوع.\n\n"
                "أمثلة:\n"
                "السعودية\n"
                "السعودية النفط\n"
                "بريطانيا\n"
                "ألمانيا\n"
                "البنك المركزي الأوروبي"
            ),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🏠 مركز الأخبار",
                            callback_data="home",
                        )
                    ]
                ]
            ),
        )

        return

    if data.startswith("s:"):
        try:
            page = max(
                1,
                int(
                    data.split(
                        ":",
                        1,
                    )[1]
                ),
            )

        except ValueError:
            await query.answer(
                "⚠️ صفحة غير صالحة."
            )
            return

        await query.answer(
            "📄 جاري عرض النتائج..."
        )

        await show_search_page(
            query,
            user_id,
            page,
        )

        return

    if data.startswith("analyze:"):
        key = data.split(
            ":",
            1,
        )[1]

        if key not in TOPICS:
            return

        await query.answer(
            "🧠 جاري تجهيز التحليل..."
        )

        status = await query.message.reply_text(
            "🧠 جاري تحليل البيانات..."
        )

        try:
            items = await get_fresh_news()

            results = topic_filter(
                items,
                key,
                8,
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
                (
                    "🧠 <b>التحليل التنفيذي</b>\n\n"
                    + safe_html(analysis)
                ),
                parse_mode="HTML",
            )

        except Exception:
            log.exception(
                "Analysis failed."
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء التحليل."
            )

        return

    if data.startswith("t:"):
        parts = data.split(":")

        if len(parts) != 3:
            return

        _, key, page_text = parts

        if key not in TOPICS:
            return

        try:
            page = max(
                1,
                int(page_text),
            )

        except ValueError:
            return

        await show_topic(
            query,
            user_id,
            key,
            page,
        )

        return


async def handle_user_message(
    update,
    context,
):
    if not update.message:
        return

    text = (
        update.message.text or ""
    ).strip()

    user = update.effective_user

    if not text or not user:
        return

    user_id = user.id

    register_user(
        user_id
    )

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
            (
                f"{status_visual('search')} "
                "جاري البحث في كافة التغطيات عن:\n"
                f"<b>{safe_html(text)}</b>..."
            ),
            parse_mode="HTML",
        )

        try:
            items = await get_fresh_news()

            query_text = expand_search_query(
                text
            )

            results = await asyncio.wait_for(
                hybrid_search_news(
                    items,
                    query_text,
                    MAX_SEARCH_RESULTS,
                ),
                timeout=SEARCH_TIMEOUT,
            )

            if not results:
                fresh = await get_fresh_news(
                    force_refresh=True
                )

                results = await asyncio.wait_for(
                    hybrid_search_news(
                        fresh,
                        query_text,
                        MAX_SEARCH_RESULTS,
                    ),
                    timeout=SEARCH_TIMEOUT,
                )

            if not results:
                USER_SEARCH_RESULTS.pop(
                    user_id,
                    None,
                )

                await status.edit_text(
                    "🔎 لم أجد نتائج مطابقة لبحثك."
                )

                return

            USER_SEARCH_RESULTS[
                user_id
            ] = deduplicate_news(
                results
            )

            display_titles = (
                await prepare_display_titles(
                    USER_SEARCH_RESULTS[
                        user_id
                    ]
                )
            )

            report = generate_base_report(
                USER_SEARCH_RESULTS[
                    user_id
                ],
                1,
                PER_PAGE,
                f"🔎 نتائج البحث: {text}",
                display_titles,
            )

            await status.edit_text(
                report,
                reply_markup=search_result_keyboard(
                    user_id,
                    1,
                ),
                disable_web_page_preview=True,
                parse_mode="HTML",
            )

        except asyncio.TimeoutError:
            await status.edit_text(
                "⏳ البحث استغرق وقتاً أطول من المتوقع. "
                "حاول مرة أخرى."
            )

        except Exception:
            log.exception(
                "Search failed."
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء البحث."
            )


async def error_handler(
    update,
    context,
):
    log.error(
        "Unhandled Telegram error: %r",
        context.error,
        exc_info=True,
    )


def main():
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_stop(post_stop)
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
                r"^(t:.*|s:\d+|home|refresh|"
                r"more|toggle_alerts|analyze:.*)$"
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

    port = int(
        os.getenv(
            "PORT",
            "8080",
        )
    )

    public_domain = os.getenv(
        "RAILWAY_PUBLIC_DOMAIN",
        "worker-production-347b.up.railway.app",
    )

    webhook_path = hashlib.sha256(
        BOT_TOKEN.encode("utf-8")
    ).hexdigest()

    webhook_url = (
        f"https://{public_domain}"
        f"/{webhook_path}"
    )

    log.info(
        "Starting Telegram webhook on port %s",
        port,
    )

    application.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path=webhook_path,
        webhook_url=webhook_url,
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
