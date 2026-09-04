import asyncio
import html
import logging
import os
import re
import time
import urllib.parse
from collections import deque
from typing import Any, Dict, List, Set

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
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

from themes import (
    theme_file,
    status_theme,
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

# يبقى التحليل اختيارياً ولا يعمل تلقائياً.
GEMINI_MODEL = "gemini-3.5-flash"


# ============================================================
# LIMITS / TIMEOUTS
# ============================================================

NEWS_COLLECTION_TIMEOUT = 25
SEARCH_TIMEOUT = NEWS_COLLECTION_TIMEOUT + 35
GEMINI_TIMEOUT = 35

MAX_SEARCH_RESULTS = 25
PER_PAGE = 5
CACHE_TTL = 300

URGENT_MONITOR_INTERVAL = 180
URGENT_INITIAL_DELAY = 30
MAX_SENT_URGENT_KEYS = 500


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
        log.exception("Failed to initialize Gemini. Bot will continue without AI.")
        ai_client = None
else:
    log.warning("GEMINI_API_KEY not found. Bot will continue without AI.")


# ============================================================
# CACHE
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

# نتائج البحث الأخيرة لكل مستخدم، حتى يعمل زر "المزيد" في البحث.
USER_SEARCH_RESULTS: Dict[int, List[Any]] = {}


# ============================================================
# USER STATE
# ============================================================

USER_LOCKS: Dict[int, asyncio.Lock] = {}
ALERT_USERS: Set[int] = set()
MUTED_USERS: Set[int] = set()

SENT_URGENT_KEYS = deque(maxlen=MAX_SENT_URGENT_KEYS)

URGENT_MONITOR_STARTED = False
URGENT_BASELINE_READY = False


# ============================================================
# TOPICS
# ============================================================

TOPICS = {
    "econ": (
        "📈 اقتصاد وأسواق",
        [
            "اقتصاد", "الاقتصادية", "أسواق", "أسهم", "بورصة", "الذهب",
            "الفيدرالي", "فائدة", "عملات", "عملات رقمية", "بيتكوين",
            "تداول", "النفط", "أوبك", "خام", "تضخم", "أسواق المال",
            "برنت", "طاقة", "غاز", "استثمار",
        ],
    ),
    "forg": (
        "🏛 بيانات رسمية",
        [
            "وزارة", "وزير", "المتحدث", "بيان رسمي", "تصريح رسمي",
            "بيان صحفي", "مصدر مسؤول", "رئاسة الوزراء", "الديوان",
            "الحكومة", "الرئاسة",
        ],
    ),
    "urg": (
        "🚨 عاجل",
        [
            "عاجل", "طارئ", "هجوم", "انفجار", "قصف", "صاروخ",
            "زلزال", "اشتباك", "غارة", "إخلاء", "حالة طوارئ",
            "تحذير عاجل", "هجوم مسلح", "أزمة",
        ],
    ),
    "gulf": (
        "🌍 الشرق الأوسط",
        [
            "السعودية", "الإمارات", "قطر", "الكويت", "البحرين", "عمان",
            "العراق", "إيران", "اليمن", "سوريا", "لبنان", "الأردن",
            "فلسطين", "إسرائيل", "الخليج", "الشرق الأوسط",
        ],
    ),
    "wrld": (
        "🌐 العالم",
        [
            "أمريكا", "الولايات المتحدة", "أوروبا", "الصين", "روسيا",
            "أوكرانيا", "واشنطن", "بكين", "موسكو", "الهند", "اليابان",
            "أستراليا", "أفريقيا", "أمريكا الجنوبية", "دولية", "قمة", "دولي",
        ],
    ),
    "secu": (
        "🛡 دفاع وأمن",
        [
            "الدفاع", "الأمن القومي", "تسليح", "مناورات", "عسكري", "جيش",
            "قوات", "أمن", "دفاع", "قاعدة عسكرية", "أسلحة", "صاروخ",
            "طيران عسكري",
        ],
    ),
}


# ============================================================
# SEARCH ALIASES
# ============================================================

# توسعة المصطلحات الشائعة حتى لا يعتمد البحث على الصياغة العربية الوحيدة.
SEARCH_ALIASES = {
    "بريطانيا": ["المملكة المتحدة", "بريطانيا", "UK", "United Kingdom"],
    "انجلترا": ["إنجلترا", "انجلترا", "بريطانيا", "المملكة المتحدة"],
    "بريطانيا العظمى": ["بريطانيا", "المملكة المتحدة", "UK"],
    "امريكا": ["أمريكا", "الولايات المتحدة", "USA", "United States"],
    "الولايات المتحده": ["الولايات المتحدة", "أمريكا", "USA"],
    "السعوديه": ["السعودية", "المملكة العربية السعودية", "Saudi Arabia"],
    "الامارات": ["الإمارات", "الإمارات العربية المتحدة", "UAE"],
    "قطر": ["قطر", "Qatar"],
    "الكويت": ["الكويت", "Kuwait"],
    "روسيا": ["روسيا", "Russia"],
    "اوكرانيا": ["أوكرانيا", "Ukraine"],
    "الصين": ["الصين", "China"],
    "اليابان": ["اليابان", "Japan"],
    "الهند": ["الهند", "India"],
    "استراليا": ["أستراليا", "Australia"],
    "المانيا": ["ألمانيا", "Germany"],
    "فرنسا": ["فرنسا", "France"],
    "ايطاليا": ["إيطاليا", "Italy"],
    "اسبانيا": ["إسبانيا", "Spain"],
    "تركيا": ["تركيا", "Turkey"],
    "ايران": ["إيران", "Iran"],
    "اسرائيل": ["إسرائيل", "Israel"],
    "فلسطين": ["فلسطين", "Palestine"],
    "مصر": ["مصر", "Egypt"],
}


def expand_search_query(query: str) -> str:
    """
    يضيف مرادفات الدول والمصطلحات الشائعة إلى الاستعلام.
    لا يربط البحث بالقسم المختار.
    """
    normalized = normalize_text(query)
    additions: List[str] = []

    for key, aliases in SEARCH_ALIASES.items():
        if normalize_text(key) in normalized:
            additions.extend(aliases)

    if not additions:
        return query

    unique = []
    seen = set()

    for value in [query, *additions]:
        marker = normalize_text(value)
        if marker and marker not in seen:
            seen.add(marker)
            unique.append(value)

    return " ".join(unique)


# ============================================================
# HELPERS
# ============================================================

def register_user(user_id: int):
    ALERT_USERS.add(user_id)


def safe_html(value: Any) -> str:
    return html.escape(str(value or ""))


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()

    text = re.sub(r"[\u064B-\u065F\u0670]", "", text)

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

    text = re.sub(r"[^\w\s\u0600-\u06FF-]", " ", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


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
# SAFE URL
# ============================================================

def build_safe_link(title: str, source: str, raw_url: str) -> str:
    raw_url = str(raw_url or "").strip()

    if re.match(r"^https?://", raw_url, re.IGNORECASE):
        parsed = urllib.parse.urlparse(raw_url)
        hostname = (parsed.hostname or "").lower()

        # الروابط الخارجية المباشرة مسموحة فقط إذا كان لها hostname صالح.
        if hostname and hostname != "news.google.com" and not hostname.endswith(".news.google.com"):
            return raw_url

    clean_title = re.sub(
        r"[^\w\s\u0600-\u06FF-]",
        " ",
        title or "",
    )
    clean_title = re.sub(r"\s+", " ", clean_title).strip()

    query = f"{clean_title} {source}".strip()

    return (
        "https://www.google.com/search?q="
        + urllib.parse.quote_plus(query)
    )


# ============================================================
# VISUAL THEMES
# ============================================================

async def send_theme(message, theme: str) -> bool:
    """
    يرسل الثيم المتحرك إذا كان ملفه موجودًا وصالحًا للإرسال.
    إذا لم يوجد الملف أو فشل الإرسال، يعود False بدون تعطيل البوت.
    """
    try:
        media = theme_file(theme)

        if media is None:
            return False

        await message.reply_sticker(
            sticker=InputFile(media, filename=media.name),
        )

        return True

    except Exception:
        log.warning(
            "Theme '%s' could not be sent; continuing without visual theme.",
            theme,
            exc_info=True,
        )
        return False


async def send_status_theme(message, status: str) -> bool:
    """إرسال الثيم المرتبط بحالة تشغيلية داخل البوت."""
    return await send_theme(
        message,
        status_theme(status),
    )


# ============================================================
# NEWS COLLECTION
# ============================================================

async def get_fresh_news(force_refresh: bool = False):
    if not force_refresh:
        cached = NEWS_CACHE.get("all_news")
        if cached is not None:
            return cached

    try:
        # collect_news في news_engine.py دالة async.
        items = await asyncio.wait_for(
            collect_news(max_items=150),
            timeout=NEWS_COLLECTION_TIMEOUT,
        )

        items = items or []

        if items:
            NEWS_CACHE.set("all_news", items)

        return items

    except asyncio.TimeoutError:
        log.warning("News collection timed out.")
        return []

    except Exception:
        log.exception("News Collection Error")
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

    scored = []

    for item in items:
        title = normalize_text(get_item_title(item))
        summary = normalize_text(get_item_summary(item))
        source = normalize_text(get_item_source(item))

        combined = f"{title} {summary} {source}"

        score = 0
        for keyword in normalized_keywords:
            if keyword and keyword in title:
                score += 10
            elif keyword and keyword in combined:
                score += 3

        if score:
            scored.append((score, item))

    scored.sort(key=lambda pair: pair[0], reverse=True)

    for _, item in scored[:max_results]:
        results.append(item)

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
    start_idx = max(0, (page - 1) * per_page)
    end_idx = start_idx + per_page
    page_items = items[start_idx:end_idx]

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

        safe_url = build_safe_link(title, source, raw_url)

        lines.append(
            f"• <b>{safe_html(title)}</b>\n"
            f"  📍 المصدر: <code>{safe_html(source)}</code>\n"
            f'  <a href="{safe_html(safe_url)}">🔗 قراءة الخبر</a>'
        )
        lines.append("")

    return "\n".join(lines).strip()


# ============================================================
# KEYBOARDS
# ============================================================

def result_keyboard(key: str, page: int, total_items: int):
    total_pages = max(
        1,
        (total_items + PER_PAGE - 1) // PER_PAGE,
    )

    rows = []
    navigation = []

    if page > 1:
        navigation.append(
            InlineKeyboardButton(
                "⬅️ السابقة",
                callback_data=f"t:{key}:{page - 1}",
            )
        )

    if page < total_pages:
        navigation.append(
            InlineKeyboardButton(
                "➕ المزيد",
                callback_data=f"t:{key}:{page + 1}",
            )
        )

    if navigation:
        rows.append(navigation)

    rows.append(
        [
            InlineKeyboardButton(
                "🧠 تحليل",
                callback_data=f"analyze:{key}",
            ),
            InlineKeyboardButton(
                "🏠 مركز الأخبار",
                callback_data="home",
            ),
        ]
    )

    return InlineKeyboardMarkup(rows)


def search_result_keyboard(user_id: int, page: int):
    results = USER_SEARCH_RESULTS.get(user_id, [])
    total_pages = max(
        1,
        (len(results) + PER_PAGE - 1) // PER_PAGE,
    )

    rows = []
    navigation = []

    if page > 1:
        navigation.append(
            InlineKeyboardButton(
                "⬅️ السابقة",
                callback_data=f"s:{page - 1}",
            )
        )

    if page < total_pages:
        navigation.append(
            InlineKeyboardButton(
                "➕ المزيد",
                callback_data=f"s:{page + 1}",
            )
        )

    if navigation:
        rows.append(navigation)

    rows.append(
        [
            InlineKeyboardButton(
                "🏠 مركز الأخبار",
                callback_data="home",
            )
        ]
    )

    return InlineKeyboardMarkup(rows)


def main_keyboard(user_id: int):
    rows = []
    items = list(TOPICS.items())

    for i in range(0, len(items), 2):
        row = []

        for key, (label, _) in items[i:i + 2]:
            row.append(
                InlineKeyboardButton(
                    label,
                    callback_data=f"t:{key}:1",
                )
            )

        rows.append(row)

    muted = user_id in MUTED_USERS
    alert_text = "🔔 التنبيهات" if muted else "🔕 التنبيهات"

    rows.append(
        [
            InlineKeyboardButton(
                alert_text,
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

    return InlineKeyboardMarkup(rows)


# ============================================================
# GEMINI
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


async def analyze_with_gemini(items) -> str:
    if not ai_client:
        return (
            "ℹ️ طبقة التحليل بالذكاء الاصطناعي غير متاحة حالياً.\n"
            "البوت مستمر في جمع الأخبار والبحث عنها بشكل طبيعي."
        )

    try:
        try:
            context_text = build_ai_context(items[:8])
        except Exception:
            context_text = "\n".join(
                f"- {get_item_title(item)} | {get_item_source(item)}"
                for item in items[:8]
            )

        prompt = (
            f"{ANALYSIS_PROMPT}\n\n"
            f"البيانات:\n{context_text}"
        )

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

        text = (getattr(response, "text", None) or "").strip()

        if not text:
            return "⚠️ لم يُرجع Gemini تحليلاً صالحاً."

        return text

    except asyncio.TimeoutError:
        log.warning("Gemini analysis timeout.")
        return (
            "⚠️ انتهت مهلة التحليل. "
            "الأخبار نفسها ما زالت تعمل بشكل طبيعي."
        )

    except Exception:
        log.exception("Gemini Analysis Error")
        return (
            "⚠️ تعذر التحليل بالذكاء الاصطناعي حالياً.\n"
            "البوت مستمر في جمع الأخبار والبحث عنها بشكل طبيعي."
        )


# ============================================================
# URGENT NEWS
# ============================================================

URGENT_STRONG_TERMS = {
    "عاجل", "طارئ", "هجوم", "هجوم مسلح", "انفجار", "قصف", "صاروخ",
    "زلزال", "غارة", "اشتباك", "إخلاء", "حالة طوارئ", "تحذير عاجل",
    "استهداف", "هجمات", "غارات", "اندلاع القتال", "اندلاع اشتباكات",
    "إطلاق النار", "اغتيال",
}

URGENT_CONTEXT_TERMS = {
    "الحكومة", "وزارة", "الدفاع", "الداخلية", "رئاسة", "الرئاسة",
    "الجيش", "القوات المسلحة", "الأمن", "الشرطة", "الطيران",
    "الحدود", "مطار", "مضيق", "سفارة", "دبلوماسي", "طاقة", "نفط",
}


def urgent_score(item) -> int:
    title = normalize_text(get_item_title(item))
    summary = normalize_text(get_item_summary(item))
    source = normalize_text(get_item_source(item))

    if not title:
        return 0

    score = 0
    title_strong = 0
    context_hits = 0

    for term in URGENT_STRONG_TERMS:
        normalized = normalize_text(term)

        if normalized in title:
            score += 5
            title_strong += 1
        elif normalized in summary:
            score += 2

    for term in URGENT_CONTEXT_TERMS:
        normalized = normalize_text(term)

        if normalized in title:
            score += 2
            context_hits += 1
        elif normalized in summary:
            score += 1

    trusted_sources = (
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

    if any(src in source for src in trusted_sources):
        score += 2

    if title_strong >= 1 and context_hits >= 1:
        score += 5

    return score


def urgent_key(item) -> str:
    title = normalize_text(get_item_title(item))
    source = normalize_text(get_item_source(item))
    return f"{title}|{source}"[:500]


def find_new_urgent_news(items, limit: int = 3):
    candidates = []

    for item in items:
        score = urgent_score(item)

        if score < 8:
            continue

        key = urgent_key(item)

        if not key or key in SENT_URGENT_KEYS:
            continue

        candidates.append((score, item))

    candidates.sort(key=lambda pair: pair[0], reverse=True)

    return [item for _, item in candidates[:limit]]


def format_urgent_alert(item) -> str:
    title = get_item_title(item)
    source = get_item_source(item)
    safe_url = build_safe_link(title, source, get_item_url(item))

    return (
        "🚨 <b>تنبيه عاجل</b>\n\n"
        f"<b>{safe_html(title)}</b>\n\n"
        f"📍 المصدر: <code>{safe_html(source)}</code>\n"
        f'<a href="{safe_html(safe_url)}">🔗 قراءة الخبر</a>'
    )


async def initialize_urgent_baseline():
    global URGENT_BASELINE_READY

    if URGENT_BASELINE_READY:
        return

    log.info("Creating urgent-news startup baseline...")

    try:
        items = await get_fresh_news(force_refresh=True)

        if not items:
            log.warning("Could not create urgent baseline because no news was collected.")
            return

        existing = []

        for item in items:
            if urgent_score(item) < 8:
                continue

            key = urgent_key(item)
            if key:
                existing.append(key)

        for key in existing:
            SENT_URGENT_KEYS.append(key)

        URGENT_BASELINE_READY = True

        log.info(
            "Urgent baseline ready. Marked %d existing urgent stories as seen.",
            len(existing),
        )

    except Exception:
        log.exception("Failed to create urgent-news baseline.")


async def urgent_monitor(application: Application):
    log.info(
        "Automatic urgent-news monitor started. Interval=%ss",
        URGENT_MONITOR_INTERVAL,
    )

    await initialize_urgent_baseline()
    await asyncio.sleep(URGENT_INITIAL_DELAY)

    while True:
        try:
            if ALERT_USERS:
                items = await get_fresh_news(force_refresh=True)

                if items:
                    urgent_items = find_new_urgent_news(items, limit=3)

                    for item in urgent_items:
                        key = urgent_key(item)

                        if not key:
                            continue

                        message = format_urgent_alert(item)

                        # نسجل بعد نجاح إرسال واحد على الأقل،
                        # حتى لا نخسر الخبر إذا فشل الإرسال بالكامل.
                        delivered = False

                        for user_id in list(ALERT_USERS):
                            if user_id in MUTED_USERS:
                                continue

                            try:
                                await application.bot.send_message(
                                    chat_id=user_id,
                                    text=message,
                                    parse_mode="HTML",
                                    disable_web_page_preview=False,
                                    disable_notification=False,
                                )
                                delivered = True

                            except Exception:
                                log.exception(
                                    "Failed to send urgent alert to user %s",
                                    user_id,
                                )

                        if delivered:
                            SENT_URGENT_KEYS.append(key)

        except asyncio.CancelledError:
            log.info("Urgent monitor stopped.")
            raise

        except Exception:
            log.exception("Unexpected error in urgent monitor.")

        await asyncio.sleep(URGENT_MONITOR_INTERVAL)


# ============================================================
# POST INIT
# ============================================================

async def post_init(application: Application):
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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not user or not update.message:
        return

    user_id = user.id
    register_user(user_id)

    await update.message.reply_text(
        "🌐 <b>مركز الأخبار</b>\n\n"
        "اختر القطاع المطلوب لمتابعة التغطية الحية والمتخصصة.\n\n"
        "🚨 التنبيهات العاجلة تعمل تلقائياً ويمكن إيقافها من الزر.",
        reply_markup=main_keyboard(user_id),
        parse_mode="HTML",
    )


# ============================================================
# TOPIC DISPLAY
# ============================================================

async def show_topic(
    query,
    user_id: int,
    key: str,
    page: int,
):
    lock = USER_LOCKS.setdefault(user_id, asyncio.Lock())

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
        await send_status_theme(query.message, "monitoring")
        status = await query.message.reply_text(
            "📡 جاري جمع وفرز الأخبار..."
        )

        try:
            items = await get_fresh_news()

            if not items:
                await status.edit_text(
                    "⚠️ تعذر الحصول على الأخبار حالياً."
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
                    "🔎 لا توجد أخبار مناسبة لهذا القسم حالياً."
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
            log.exception("Topic Handler Exception")
            await status.edit_text(
                "⚠️ حدث خطأ أثناء عرض البيانات."
            )


# ============================================================
# SEARCH DISPLAY
# ============================================================

async def show_search_page(
    query,
    user_id: int,
    page: int,
):
    results = USER_SEARCH_RESULTS.get(user_id, [])

    if not results:
        await query.message.reply_text(
            "🔎 لا توجد نتائج بحث محفوظة. اكتب البحث من جديد."
        )
        return

    report = generate_base_report(
        results,
        page=page,
        per_page=PER_PAGE,
        heading="🔎 نتائج البحث",
    )

    await query.message.reply_text(
        report,
        reply_markup=search_result_keyboard(user_id, page),
        disable_web_page_preview=True,
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

    # --------------------------------------------------------
    # ALERT TOGGLE
    # --------------------------------------------------------

    if data == "toggle_alerts":
        if user_id in MUTED_USERS:
            MUTED_USERS.discard(user_id)
            await query.answer(
                text="🔔 تم تفعيل التنبيهات العاجلة.",
                show_alert=True,
            )
        else:
            MUTED_USERS.add(user_id)
            await query.answer(
                text="🔕 تم إيقاف التنبيهات العاجلة.",
                show_alert=True,
            )

        try:
            await query.message.edit_reply_markup(
                reply_markup=main_keyboard(user_id)
            )
        except Exception:
            pass

        return

    # --------------------------------------------------------
    # HOME
    # --------------------------------------------------------

    if data == "home":
        await query.answer(
            text="🏠 مركز الأخبار",
            show_alert=False,
        )

        await query.message.reply_text(
            "🌐 <b>مركز الأخبار</b>",
            reply_markup=main_keyboard(user_id),
            parse_mode="HTML",
        )
        return

    # --------------------------------------------------------
    # REFRESH
    # --------------------------------------------------------

    if data == "refresh":
        await query.answer(
            text="🔄 جاري تحديث الأخبار...",
            show_alert=True,
        )

        NEWS_CACHE.set("all_news", None)

        await send_status_theme(query.message, "monitoring")
        status = await query.message.reply_text(
            "📡 جاري جلب آخر الأخبار..."
        )

        try:
            items = await get_fresh_news(force_refresh=True)

            if not items:
                await status.edit_text(
                    "⚠️ تعذر الحصول على الأخبار حالياً."
                )
                return

            await status.edit_text(
                "✅ تم تحديث الأخبار بنجاح.\n\n"
                "اختر القسم المطلوب من مركز الأخبار.",
                reply_markup=main_keyboard(user_id),
            )

        except Exception:
            log.exception("Refresh Handler Exception")
            await status.edit_text(
                "⚠️ حدث خطأ أثناء تحديث الأخبار."
            )

        return

    # --------------------------------------------------------
    # MORE / SEARCH INSTRUCTION
    # --------------------------------------------------------

    if data == "more":
        await query.answer(
            text="🔎 البحث متاح الآن",
            show_alert=False,
        )

        await query.message.reply_text(
            "➕ <b>المزيد</b>\n\n"
            "اكتب مباشرة اسم دولة أو مدينة أو موضوع.\n\n"
            "أمثلة:\n"
            "السعودية\n"
            "السعودية النفط\n"
            "بريطانيا\n"
            "ألمانيا\n"
            "البنك المركزي الأوروبي",
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

    # --------------------------------------------------------
    # SEARCH PAGINATION
    # --------------------------------------------------------

    if data.startswith("s:"):
        try:
            page = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("⚠️ صفحة غير صالحة.", show_alert=False)
            return

        await query.answer(
            text="📄 جاري عرض النتائج...",
            show_alert=False,
        )

        await show_search_page(
            query,
            user_id,
            max(1, page),
        )
        return

    # --------------------------------------------------------
    # ANALYSIS
    # --------------------------------------------------------

    if data.startswith("analyze:"):
        await query.answer(
            text="🧠 جاري تجهيز التحليل...",
            show_alert=False,
        )

        key = data.split(":", 1)[1]

        if key not in TOPICS:
            return

        await send_status_theme(query.message, "analysis")
        status = await query.message.reply_text(
            "🧠 جاري تحليل البيانات..."
        )

        try:
            items = await get_fresh_news()

            if not items:
                await status.edit_text(
                    "⚠️ لا توجد بيانات متاحة للتحليل حالياً."
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
                    "⚠️ لا توجد بيانات كافية للتحليل."
                )
                return

            analysis = await analyze_with_gemini(results)

            await status.edit_text(
                "🧠 <b>التحليل التنفيذي</b>\n\n"
                + safe_html(analysis),
                parse_mode="HTML",
            )

        except Exception:
            log.exception("Analysis Handler Exception")
            await status.edit_text(
                "⚠️ حدث خطأ أثناء التحليل."
            )

        return

    # --------------------------------------------------------
    # TOPIC / PAGINATION
    # --------------------------------------------------------

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

        await show_topic(
            query,
            user_id,
            key,
            max(1, page),
        )
        return

    await query.answer(
        text="⚠️ أمر غير معروف.",
        show_alert=False,
    )


# ============================================================
# DIRECT USER SEARCH
# ============================================================

async def handle_user_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message:
        return

    text = (update.message.text or "").strip()

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
        await send_status_theme(update.message, "search")
        status = await update.message.reply_text(
            "🔎 جاري البحث في كافة التغطيات عن:\n"
            f"<b>{safe_html(text)}</b>...",
            parse_mode="HTML",
        )

        try:
            # =================================================
            # الإصلاح الأساسي:
            # hybrid_search_news في news_engine.py تستقبل:
            # (items, query, max_results)
            #
            # الخطأ السابق كان تمرير:
            # hybrid_search_news(text, max_results=...)
            #
            # وهذا سبب TypeError وبالتالي ظهور:
            # ⚠️ حدث خطأ أثناء البحث.
            # =================================================

            items = await get_fresh_news()

            expanded_query = expand_search_query(text)

            results = await asyncio.wait_for(
                hybrid_search_news(
                    items,
                    expanded_query,
                    max_results=MAX_SEARCH_RESULTS,
                ),
                timeout=SEARCH_TIMEOUT,
            )

            # إذا كان البحث المحلي/الأونلاين لم يجد شيئاً،
            # نجرب جمعاً جديداً مرة واحدة فقط.
            if not results:
                fresh_items = await get_fresh_news(
                    force_refresh=True
                )

                if fresh_items:
                    results = await asyncio.wait_for(
                        hybrid_search_news(
                            fresh_items,
                            expanded_query,
                            max_results=MAX_SEARCH_RESULTS,
                        ),
                        timeout=SEARCH_TIMEOUT,
                    )

            if not results:
                USER_SEARCH_RESULTS.pop(user_id, None)

                await status.edit_text(
                    "🔎 لم أجد نتائج مطابقة لبحثك."
                )
                return

            USER_SEARCH_RESULTS[user_id] = list(results)

            report = generate_base_report(
                results,
                page=1,
                per_page=PER_PAGE,
                heading=f"🔎 نتائج البحث: {text}",
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
            log.warning(
                "Search timed out for user %s: %s",
                user_id,
                text,
            )

            await status.edit_text(
                "⏳ البحث استغرق وقتاً أطول من المتوقع.\n"
                "حاول مرة أخرى بعد قليل."
            )

        except Exception:
            log.exception(
                "Message Search Exception for query=%r",
                text,
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء البحث.\n"
                "راجع سجل التشغيل لمعرفة الخطأ التفصيلي."
            )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
    log.error(
        "Unhandled Telegram error: %r",
        context.error,
        exc_info=(
            type(context.error),
            context.error,
            context.error.__traceback__
            if context.error
            else None,
        ),
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
        CommandHandler("start", start)
    )

    application.add_handler(
        CallbackQueryHandler(
            button_handler,
            pattern=(
                r"^(t:.*|s:\d+|home|refresh|more|"
                r"toggle_alerts|analyze:.*)$"
            ),
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_user_message,
        )
    )

    application.add_error_handler(error_handler)

    log.info("Pro News Bot launched successfully.")
    log.info("Visual theme layer loaded; missing theme assets are safely ignored.")
    log.info(
        "Automatic urgent monitor interval: %s seconds.",
        URGENT_MONITOR_INTERVAL,
    )

    application.run_polling(
        drop_pending_updates=True
    )


if __name__ == "__main__":
    main()
