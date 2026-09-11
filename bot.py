import asyncio
import base64
import hashlib
import html
import difflib
import logging
import os
import re
import time
import json
from pathlib import Path
import urllib.parse
import zlib
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from google import genai
from google.genai import types

from news_engine import (
    NewsItem,
    build_ai_context,
    collect_news,
    collect_breaking_news,
    deduplicate_news,
    is_topic_match,
    search_news,
    search_news_online,
    translate_news_titles,
)

try:
    from intel_sources import collect_social_intel, source_health as get_social_source_health
except Exception:
    collect_social_intel = None
    get_social_source_health = None
    logging.getLogger("pro_news_bot").exception(
        "intel_sources import failed; social provider disabled."
    )

try:
    from direct_sources import collect_direct_radar, get_direct_source_health
except Exception:
    collect_direct_radar = None
    get_direct_source_health = None
    logging.getLogger("pro_news_bot").exception(
        "direct_sources import failed; direct radar disabled."
    )

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
logging.getLogger("httpx").setLevel(logging.CRITICAL)
logging.getLogger("httpcore").setLevel(logging.CRITICAL)

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash").strip()

# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------
# The owner ID is deliberately fixed to the bot founder's Telegram numeric ID.
# It can still be overridden with OWNER_TELEGRAM_ID in Railway if ever needed.
OWNER_ID = int(os.getenv("OWNER_TELEGRAM_ID", "375794122"))
ACCESS_STATE_PATH = Path(
    os.getenv("ACCESS_STATE_PATH", "/data/telegram_access.json")
)

# Dynamic roles. The owner is never stored as an admin and cannot be removed.
ADMINS: Set[int] = set()
APPROVED_USERS: Set[int] = {OWNER_ID}
BLOCKED_USERS: Set[int] = set()
PENDING_USERS: Dict[int, Dict[str, Any]] = {}

# Free durable access backup: the latest access state is mirrored into a pinned
# message in the owner's existing private Telegram chat. This adds no external
# service and survives Railway container replacement because Telegram is already
# the bot's transport. Local JSON remains the fast primary runtime copy.
ACCESS_CLOUD_MARKER = "GLOBAL_INTEL_ACCESS_V1"
ACCESS_STATE_UPDATED_AT = 0
ACCESS_BOT = None
ACCESS_CLOUD_SYNC_TASK = None
ACCESS_CLOUD_SYNC_DIRTY = False


def _safe_user_record(user):
    return {
        "id": int(user.id),
        "username": (user.username or "").strip(),
        "first_name": (user.first_name or "").strip(),
        "last_name": (user.last_name or "").strip(),
        "requested_at": int(time.time()),
    }


def _env_id_set(name):
    """Optional durable bootstrap IDs kept in Railway environment variables."""
    result = set()
    for raw in (os.getenv(name, "") or "").replace(";", ",").split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            result.add(int(raw))
        except ValueError:
            log.warning("Ignoring invalid Telegram ID in %s", name)
    result.discard(OWNER_ID)
    return result


BOOTSTRAP_APPROVED_USERS = _env_id_set("APPROVED_TELEGRAM_IDS")
BOOTSTRAP_ADMINS = _env_id_set("ADMIN_TELEGRAM_IDS")


def _access_payload(updated_at=None):
    """Build the canonical access-control snapshot."""
    if updated_at is None:
        updated_at = ACCESS_STATE_UPDATED_AT
    return {
        "version": 1,
        "updated_at": int(updated_at or 0),
        "admins": sorted(int(x) for x in ADMINS if int(x) != OWNER_ID),
        "approved_users": sorted(int(x) for x in (APPROVED_USERS | {OWNER_ID})),
        "blocked_users": sorted(int(x) for x in BLOCKED_USERS if int(x) != OWNER_ID),
        "pending_users": {str(int(k)): dict(v or {}) for k, v in PENDING_USERS.items()},
    }


def _normalize_access_state():
    """Enforce mutually-exclusive roles after any restore."""
    ADMINS.discard(OWNER_ID)
    BLOCKED_USERS.discard(OWNER_ID)
    ADMINS.difference_update(BLOCKED_USERS)
    APPROVED_USERS.difference_update(BLOCKED_USERS)
    APPROVED_USERS.add(OWNER_ID)
    for uid in list(PENDING_USERS):
        if uid == OWNER_ID or uid in ADMINS or uid in APPROVED_USERS or uid in BLOCKED_USERS:
            PENDING_USERS.pop(uid, None)


def _apply_access_payload(raw, merge_bootstrap=True):
    """Validate and apply one access snapshot. Returns its revision timestamp."""
    global ADMINS, APPROVED_USERS, BLOCKED_USERS, PENDING_USERS, ACCESS_STATE_UPDATED_AT
    if not isinstance(raw, dict):
        raise ValueError("access snapshot is not an object")

    admins = {int(x) for x in (raw.get("admins") or []) if int(x) != OWNER_ID}
    approved = {int(x) for x in (raw.get("approved_users") or [])}
    blocked = {int(x) for x in (raw.get("blocked_users") or []) if int(x) != OWNER_ID}
    pending_raw = raw.get("pending_users") or {}
    if not isinstance(pending_raw, dict):
        pending_raw = {}
    pending = {int(k): dict(v or {}) for k, v in pending_raw.items()}

    if merge_bootstrap:
        admins.update(BOOTSTRAP_ADMINS)
        approved.update(BOOTSTRAP_APPROVED_USERS)
    approved.add(OWNER_ID)

    ADMINS = admins
    APPROVED_USERS = approved
    BLOCKED_USERS = blocked
    PENDING_USERS = pending
    _normalize_access_state()
    ACCESS_STATE_UPDATED_AT = max(0, int(raw.get("updated_at") or 0))
    return ACCESS_STATE_UPDATED_AT


def load_access_state():
    """Load the local runtime copy; Telegram backup is restored in post_init."""
    global ADMINS, APPROVED_USERS, BLOCKED_USERS, PENDING_USERS, ACCESS_STATE_UPDATED_AT
    ADMINS = set(BOOTSTRAP_ADMINS)
    APPROVED_USERS = {OWNER_ID} | set(BOOTSTRAP_APPROVED_USERS)
    BLOCKED_USERS = set()
    PENDING_USERS = {}
    ACCESS_STATE_UPDATED_AT = 0
    try:
        if not ACCESS_STATE_PATH.exists():
            log.info(
                "Access state file not found at %s; startup will try the Telegram owner backup.",
                ACCESS_STATE_PATH,
            )
            return
        raw = json.loads(ACCESS_STATE_PATH.read_text(encoding="utf-8"))
        _apply_access_payload(raw, merge_bootstrap=True)
        if not ACCESS_STATE_UPDATED_AT:
            try:
                ACCESS_STATE_UPDATED_AT = int(ACCESS_STATE_PATH.stat().st_mtime)
            except OSError:
                ACCESS_STATE_UPDATED_AT = 0
    except Exception:
        log.exception("Access state could not be loaded; startup will try the Telegram owner backup.")
        ADMINS = set(BOOTSTRAP_ADMINS)
        APPROVED_USERS = {OWNER_ID} | set(BOOTSTRAP_APPROVED_USERS)
        BLOCKED_USERS = set()
        PENDING_USERS = {}
        ACCESS_STATE_UPDATED_AT = 0


def _encode_access_cloud_payload(payload):
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    packed = zlib.compress(raw, level=9)
    token = base64.urlsafe_b64encode(packed).decode("ascii")
    return f"🔐 {ACCESS_CLOUD_MARKER}\n{token}"


def _decode_access_cloud_payload(text):
    text = (text or "").strip()
    prefix = f"🔐 {ACCESS_CLOUD_MARKER}\n"
    if not text.startswith(prefix):
        return None
    token = text[len(prefix):].strip()
    if not token:
        return None
    packed = base64.urlsafe_b64decode(token.encode("ascii"))
    raw = zlib.decompress(packed)
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict) or int(payload.get("version") or 0) != 1:
        return None
    return payload


def _write_access_state_file(payload):
    try:
        ACCESS_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = ACCESS_STATE_PATH.with_suffix(ACCESS_STATE_PATH.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(ACCESS_STATE_PATH)
        return True
    except Exception:
        log.warning("Local access-state copy could not be written at %s.", ACCESS_STATE_PATH, exc_info=True)
        return False


def _schedule_access_cloud_sync():
    """Debounced async mirror to Telegram; never blocks an owner/admin action."""
    global ACCESS_CLOUD_SYNC_TASK, ACCESS_CLOUD_SYNC_DIRTY
    if ACCESS_BOT is None:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    ACCESS_CLOUD_SYNC_DIRTY = True
    if ACCESS_CLOUD_SYNC_TASK is None or ACCESS_CLOUD_SYNC_TASK.done():
        ACCESS_CLOUD_SYNC_TASK = asyncio.create_task(
            _access_cloud_sync_worker(),
            name="access-state-telegram-backup",
        )


def save_access_state(sync_cloud=True):
    """Save locally now and mirror to Telegram without delaying the bot UI."""
    global ACCESS_STATE_UPDATED_AT
    ACCESS_STATE_UPDATED_AT = max(int(time.time()), int(ACCESS_STATE_UPDATED_AT or 0) + 1)
    payload = _access_payload(ACCESS_STATE_UPDATED_AT)
    _write_access_state_file(payload)
    if sync_cloud:
        _schedule_access_cloud_sync()


async def _sync_access_state_to_telegram(bot):
    """Persist the canonical snapshot in the owner's private Telegram chat."""
    payload = _access_payload()
    text = _encode_access_cloud_payload(payload)
    if len(text) > 4000:
        # This should require hundreds of accounts; fail closed rather than
        # publishing a truncated/invalid authorization snapshot.
        raise RuntimeError("access backup exceeds Telegram message size")

    chat = await bot.get_chat(OWNER_ID)
    pinned = getattr(chat, "pinned_message", None)
    pinned_text = ((getattr(pinned, "text", None) or getattr(pinned, "caption", None) or "") if pinned else "")

    if pinned and pinned_text.startswith(f"🔐 {ACCESS_CLOUD_MARKER}\n"):
        if pinned_text != text:
            await bot.edit_message_text(chat_id=OWNER_ID, message_id=pinned.message_id, text=text)
        # Re-pin silently so our state is the most recent pinned message.
        await bot.pin_chat_message(chat_id=OWNER_ID, message_id=pinned.message_id, disable_notification=True)
        return pinned.message_id

    msg = await bot.send_message(chat_id=OWNER_ID, text=text, disable_notification=True)
    await bot.pin_chat_message(chat_id=OWNER_ID, message_id=msg.message_id, disable_notification=True)
    return msg.message_id


async def _access_cloud_sync_worker():
    global ACCESS_CLOUD_SYNC_DIRTY

    failures = 0
    while True:
        ACCESS_CLOUD_SYNC_DIRTY = False
        bot = ACCESS_BOT
        if bot is None:
            return

        try:
            await _sync_access_state_to_telegram(bot)
            failures = 0
        except asyncio.CancelledError:
            raise
        except Exception:
            # Keep the runtime/local authorization state active and retry a few
            # times independently. This avoids losing a recent access change if
            # Telegram has a transient failure and Railway restarts before the
            # owner makes another access-control change.
            failures += 1
            log.warning(
                "Could not mirror access state to Telegram (attempt %s/3).",
                failures,
                exc_info=True,
            )
            if failures < 3:
                await asyncio.sleep(2 ** (failures - 1))
                ACCESS_CLOUD_SYNC_DIRTY = True

        if not ACCESS_CLOUD_SYNC_DIRTY:
            return
        await asyncio.sleep(0)


async def restore_access_state_from_telegram(bot):
    """Restore the newest state from the owner's pinned bot message at startup."""
    global ACCESS_STATE_UPDATED_AT
    try:
        chat = await bot.get_chat(OWNER_ID)
        pinned = getattr(chat, "pinned_message", None)
        text = (getattr(pinned, "text", None) or getattr(pinned, "caption", None) or "") if pinned else ""
        cloud = _decode_access_cloud_payload(text)
        if cloud is None:
            # First run with this mechanism: preserve whatever local/bootstrap
            # state exists and establish the Telegram backup after startup.
            return False

        cloud_updated = max(0, int(cloud.get("updated_at") or 0))
        if cloud_updated >= int(ACCESS_STATE_UPDATED_AT or 0):
            _apply_access_payload(cloud, merge_bootstrap=True)
            _write_access_state_file(_access_payload())
            log.info("Access state restored from Telegram owner backup (%s).", cloud_updated)
            return True
        return False
    except asyncio.CancelledError:
        raise
    except Exception:
        # Startup continues with local/bootstrap roles; never make Telegram backup
        # availability a prerequisite for the bot itself to start.
        log.warning("Telegram access backup could not be restored; using local/bootstrap state.", exc_info=True)
        return False


def is_owner(user_id):
    return int(user_id) == OWNER_ID


def is_authorized(user_id):
    uid = int(user_id)
    return uid == OWNER_ID or uid in ADMINS or uid in APPROVED_USERS


def access_role(user_id):
    uid = int(user_id)
    if uid == OWNER_ID:
        return "owner"
    if uid in BLOCKED_USERS:
        return "blocked"
    if uid in ADMINS:
        return "admin"
    if uid in APPROVED_USERS:
        return "user"
    if uid in PENDING_USERS:
        return "pending"
    return "guest"


load_access_state()

NEWS_COLLECTION_TIMEOUT = 25
ONLINE_SEARCH_TIMEOUT = 6
CALLBACK_ACK_TIMEOUT = 1.5
CALLBACK_DEDUP_TTL = 60
CALLBACK_ACTION_DEBOUNCE = 5
GEMINI_TIMEOUT = 35

MAX_SEARCH_RESULTS = 25
MAX_TOPIC_RESULTS = 60
PER_PAGE = 5
CACHE_TTL = 300
HOT_CACHE_LIMIT = 260
SOCIAL_CACHE_TTL = 180
DIRECT_CACHE_TTL = 90
SOCIAL_REFRESH_INTERVAL = 180
DIRECT_REFRESH_INTERVAL = 30
PROVIDER_LOOP_INTERVAL = 30
SOCIAL_PROVIDER_TIMEOUT = 12
DIRECT_PROVIDER_TIMEOUT = 10
PROVIDER_TRANSLATION_BUDGET = 5.0

URGENT_MONITOR_INTERVAL = 30
URGENT_INITIAL_DELAY = 8
BREAKING_LANE_TIMEOUT = 8
MAX_SENT_URGENT_KEYS = 500
MAX_RECENT_URGENT_EVENTS = 300
RECENT_URGENT_EVENT_TTL = 12 * 3600

if not BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")

ai_client = None
if GEMINI_API_KEY:
    try:
        ai_client = genai.Client(api_key=GEMINI_API_KEY)
        log.info("Gemini analysis layer enabled.")
    except Exception:
        log.exception("Failed to initialize Gemini analysis layer.")
else:
    log.warning("GEMINI_API_KEY not found; explicit analysis will be unavailable.")


class SimpleCache:
    def __init__(self, ttl=300):
        self.ttl = ttl
        self._cache = {}
        self._timestamps = {}

    def get(self, key):
        value = self._cache.get(key)
        if value is None:
            return None
        if time.time() - self._timestamps.get(key, 0) < self.ttl:
            return value
        self._cache.pop(key, None)
        self._timestamps.pop(key, None)
        return None

    def peek(self, key):
        return self._cache.get(key)

    def set(self, key, value):
        if value is None:
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)
            return
        self._cache[key] = value
        self._timestamps[key] = time.time()


NEWS_CACHE = SimpleCache(CACHE_TTL)
BREAKING_CACHE = SimpleCache(max(CACHE_TTL, URGENT_MONITOR_INTERVAL * 4))
SOCIAL_CACHE = SimpleCache(SOCIAL_CACHE_TTL)
DIRECT_CACHE = SimpleCache(DIRECT_CACHE_TTL)
HOT_VIEW_CACHE = SimpleCache(24 * 3600)
USER_SEARCH_RESULTS: Dict[int, List[Any]] = {}
USER_SEARCH_QUERY: Dict[int, str] = {}
USER_TOPIC_RESULTS: Dict[str, List[Any]] = {}
USER_SEEN_TOPIC_EVENTS: Dict[str, List[Any]] = {}
MAX_SEEN_TOPIC_EVENTS = 120
USER_LOCKS: Dict[int, asyncio.Lock] = {}
ALERT_USERS: Set[int] = set()
MUTED_USERS: Set[int] = set()
SENT_URGENT_KEYS = deque(maxlen=MAX_SENT_URGENT_KEYS)
RECENT_URGENT_EVENTS = deque(maxlen=MAX_RECENT_URGENT_EVENTS)

CUSTOM_EMOJI_IDS = {}
URGENT_MONITOR_STARTED = False
URGENT_BASELINE_READY = False
URGENT_MONITOR_TASK = None
BACKGROUND_TASKS: Set[asyncio.Task] = set()
NEWS_COLLECTION_TASK = None
PROVIDER_REFRESH_TASK = None
PROVIDER_MONITOR_TASK = None
PROVIDER_MONITOR_STARTED = False
LAST_SOCIAL_REFRESH = 0.0
LAST_DIRECT_REFRESH = 0.0
SOCIAL_PROVIDER_HEALTH = {}
DIRECT_PROVIDER_HEALTH = {}
LAST_NEWS_REFRESH = 0.0
SEEN_CALLBACK_IDS: Dict[str, float] = {}
RECENT_CALLBACK_ACTIONS: Dict[str, float] = {}

TOPICS = {
    "econ": (
        "📈 اقتصاد وأسواق",
        [
            "اقتصاد", "اقتصادي", "أسواق", "أسهم", "بورصة", "الذهب",
            "فائدة", "عملات", "عملات رقمية", "بيتكوين", "تداول",
            "نفط", "أوبك", "خام", "تضخم", "أسواق المال", "برنت",
            "طاقة", "غاز", "استثمار", "سندات", "ميزانية", "ناتج محلي",
            "بنك مركزي", "دولار", "واردات", "صادرات", "استثمارات",
        ],
    ),
    "forg": (
        "🏛 بيانات رسمية",
        [
            "بيان رسمي", "تصريح رسمي", "بيان صحفي", "المتحدث الرسمي",
            "المتحدث باسم", "مصدر مسؤول", "أعلنت الوزارة", "أعلن الوزير",
            "قالت الوزارة", "قال الوزير", "وزارة الخارجية", "وزارة الدفاع",
            "وزارة الداخلية", "رئاسة الوزراء", "الديوان الملكي",
            "الحكومة تعلن", "الحكومة تؤكد", "الرئاسة تعلن", "الرئاسة تؤكد",
        ],
    ),
    "urg": (
        "🚨 عاجل",
        [
            "عاجل", "طارئ", "هجوم", "انفجار", "قصف", "صاروخ", "زلزال",
            "اشتباك", "غارة", "إخلاء", "حالة طوارئ", "تحذير عاجل",
            "هجوم مسلح", "أزمة", "استهداف", "غارات", "إطلاق النار",
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
            "أستراليا", "أفريقيا", "أمريكا الجنوبية", "دولية", "قمة",
        ],
    ),
    "secu": (
        "🛡 دفاع وأمن",
        [
            "الدفاع", "الأمن القومي", "تسليح", "مناورات", "عسكري", "جيش",
            "قوات", "أمن", "دفاع", "قاعدة عسكرية", "أسلحة", "صاروخ",
            "طيران عسكري", "قصف", "هجوم", "اشتباك", "غارة", "استهداف",
            "عملية عسكرية", "عمليات عسكرية", "قوات خاصة", "دفاع جوي",
            "منظومة دفاع", "ذخائر", "مقاتلات", "طائرات مسيرة",
        ],
    ),
}

SEARCH_ALIASES = {
    "بريطانيا": ["المملكة المتحدة", "UK", "United Kingdom"],
    "انجلترا": ["إنجلترا", "بريطانيا", "المملكة المتحدة"],
    "امريكا": ["أمريكا", "الولايات المتحدة", "USA", "United States"],
    "السعوديه": ["السعودية", "المملكة العربية السعودية", "Saudi Arabia"],
    "الامارات": ["الإمارات", "الإمارات العربية المتحدة", "UAE"],
    "روسيا": ["روسيا", "Russia"],
    "اوكرانيا": ["أوكرانيا", "Ukraine"],
    "الصين": ["الصين", "China"],
    "اليابان": ["اليابان", "Japan"],
    "المانيا": ["ألمانيا", "Germany"],
    "فرنسا": ["فرنسا", "France"],
    "ايران": ["إيران", "Iran"],
    "اسرائيل": ["إسرائيل", "Israel"],
}


def register_user(user_id):
    """Register only authorized users for alerts. Revoked/blocked users stay out."""
    uid = int(user_id)
    if is_authorized(uid) and uid not in BLOCKED_USERS:
        ALERT_USERS.add(uid)
    else:
        ALERT_USERS.discard(uid)
        MUTED_USERS.discard(uid)


def remove_runtime_user(user_id):
    uid = int(user_id)
    ALERT_USERS.discard(uid)
    MUTED_USERS.discard(uid)
    USER_SEARCH_RESULTS.pop(uid, None)
    USER_SEARCH_QUERY.pop(uid, None)
    USER_LOCKS.pop(uid, None)
    # Topic history keys use a user prefix in this bot. Remove defensively.
    prefix = f"{uid}:"
    for mapping in (USER_TOPIC_RESULTS, USER_SEEN_TOPIC_EVENTS):
        for key in list(mapping):
            if str(key).startswith(prefix):
                mapping.pop(key, None)


def safe_html(value):
    return html.escape(str(value or ""))


def normalize_text(value):
    text = str(value or "").strip().lower()
    text = re.sub(r"[\u064B-\u065F\u0670]", "", text)
    for old, new in {
        "أ": "ا", "إ": "ا", "آ": "ا", "ى": "ي",
        "ة": "ه", "ؤ": "و", "ئ": "ي",
    }.items():
        text = text.replace(old, new)
    text = re.sub(r"[^\w\s\u0600-\u06FF-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def get_item_title(item):
    return (getattr(item, "title", "") or getattr(item, "caption", "") or "").strip()


def get_item_source(item):
    return (getattr(item, "source", "") or "مصدر إخباري").strip()


def get_item_url(item):
    return (getattr(item, "url", "") or getattr(item, "link", "") or "").strip()


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


def build_safe_link(title, source, raw_url):
    raw_url = str(raw_url or "").strip()
    if re.match(r"^https?://", raw_url, re.I):
        parsed = urllib.parse.urlparse(raw_url)
        if parsed.hostname:
            return raw_url
    return "https://www.google.com/search?q=" + urllib.parse.quote_plus(
        f"{title} {source}".strip()
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
        return visual(status_theme(status))
    except Exception:
        return ""


def track_task(coro, name):
    task = asyncio.create_task(coro, name=name)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    return task


async def safe_query_answer(query, text=None, show_alert=False):
    """Acknowledge Telegram callbacks without allowing ACK latency to block UI work."""
    try:
        await asyncio.wait_for(
            query.answer(text=text, show_alert=show_alert),
            timeout=CALLBACK_ACK_TIMEOUT,
        )
        return True
    except asyncio.TimeoutError:
        log.info("Callback acknowledgement timed out; continuing action.")
    except Exception as exc:
        log.info("Callback acknowledgement failed; continuing action: %s", type(exc).__name__)
    return False


def claim_callback(query, user_id, data):
    """Return True exactly once for a callback delivery/action within the debounce window."""
    now = time.monotonic()

    # Prune occasionally so the dictionaries stay bounded during long runtimes.
    if len(SEEN_CALLBACK_IDS) > 512:
        cutoff = now - CALLBACK_DEDUP_TTL
        for key, seen_at in list(SEEN_CALLBACK_IDS.items()):
            if seen_at < cutoff:
                SEEN_CALLBACK_IDS.pop(key, None)

    if len(RECENT_CALLBACK_ACTIONS) > 512:
        cutoff = now - CALLBACK_ACTION_DEBOUNCE
        for key, seen_at in list(RECENT_CALLBACK_ACTIONS.items()):
            if seen_at < cutoff:
                RECENT_CALLBACK_ACTIONS.pop(key, None)

    callback_id = str(getattr(query, "id", "") or "").strip()
    if callback_id:
        seen_at = SEEN_CALLBACK_IDS.get(callback_id)
        if seen_at is not None and now - seen_at < CALLBACK_DEDUP_TTL:
            log.info("Duplicate callback delivery ignored: %s", data)
            return False
        SEEN_CALLBACK_IDS[callback_id] = now

    message = getattr(query, "message", None)
    chat_id = getattr(getattr(message, "chat", None), "id", "")
    message_id = getattr(message, "message_id", "")
    action_key = f"{user_id}:{chat_id}:{message_id}:{data}"
    seen_at = RECENT_CALLBACK_ACTIONS.get(action_key)
    if seen_at is not None and now - seen_at < CALLBACK_ACTION_DEBOUNCE:
        log.info("Repeated callback action ignored: %s", data)
        return False

    RECENT_CALLBACK_ACTIONS[action_key] = now
    return True


async def initialize_custom_emoji_pack(application):
    global CUSTOM_EMOJI_IDS
    try:
        if CUSTOM_EMOJI_PACK:
            CUSTOM_EMOJI_IDS = await load_custom_emoji_ids(
                application.bot,
                CUSTOM_EMOJI_PACK,
            )
    except Exception:
        CUSTOM_EMOJI_IDS = {}
        log.exception("Custom emoji pack failed; fallback enabled.")


def _parse_provider_datetime(value):
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _provider_domain(url):
    try:
        return (urllib.parse.urlparse(str(url or "")).hostname or "").removeprefix("www.").lower()
    except Exception:
        return ""


def _provider_event_text(event):
    return normalize_text(
        " ".join(
            str(event.get(key, "") or "")
            for key in ("title", "original_title", "summary", "source_type", "organization", "organization_ar")
        )
    )


def _provider_topic(event, provider):
    """Assign one and only one specialist section from event substance."""
    text = _provider_event_text(event)
    route = str(event.get("routing_hint", "") or "")

    economy_terms = (
        "sanction", "ofac", "designation", "asset freeze", "financial", "tariff",
        "central bank", "interest rate", "market", "econom", "trade", "energy price",
        "عقوبات", "تجميد اصول", "تجميد الأصول", "اقتصاد", "اسواق", "أسواق",
        "بنك مركزي", "فائده", "فائدة", "تعرفه", "تعرفة", "تجاره", "تجارة",
    )
    security_terms = (
        "military", "defence", "defense", "missile", "drone", "airstrike", "navy",
        "warship", "troops", "attack", "boarding", "hijack", "maritime security",
        "security incident", "conflict", "weapon", "peacekeeping", "nato",
        "عسكري", "دفاع", "صاروخ", "طائره مسيره", "طائرة مسيرة", "قصف", "غاره",
        "غارة", "هجوم", "اشتباك", "قوات", "سلاح", "امن بحري", "أمن بحري",
    )
    condemnation_terms = (
        "condemn", "denounce", "statement on", "expresses concern", "calls for",
        "ادان", "إدان", "يدين", "تدين", "تعرب عن قلق", "يدعو الى", "يدعو إلى",
    )
    breaking_terms = (
        "explosion", "airstrike", "missile strike", "armed attack", "evacuation",
        "earthquake", "hijacked", "under attack", "fired upon",
        "انفجار", "قصف", "غاره", "غارة", "هجوم مسلح", "اخلاء", "إخلاء",
        "زلزال", "اختطاف سفينه", "اختطاف سفينة", "اطلاق النار", "إطلاق النار",
    )

    # Breaking is deliberately narrow. Official condemnations do not become breaking.
    published = _parse_provider_datetime(event.get("published"))
    recent = True
    if published is not None:
        now = datetime.now(timezone.utc)
        try:
            recent = abs((now - published.astimezone(timezone.utc)).total_seconds()) <= 6 * 3600
        except Exception:
            recent = True
    status_strength = int(event.get("event_status_strength", 0) or 0)
    primary = str(event.get("source_role", "") or "") in {"primary", "primary_sensor"}
    a_plus = str(event.get("source_grade", "") or "") == "A+"
    concrete_breaking = any(normalize_text(t) in text for t in breaking_terms)
    condemnation = any(normalize_text(t) in text for t in condemnation_terms)
    if concrete_breaking and recent and not condemnation and (
        (provider == "direct_radar" and status_strength >= 80)
        or (provider == "social_intel" and primary and a_plus)
    ):
        return "urg"

    # A diplomatic condemnation/appeal is an official statement even when it
    # mentions an attack, missile, or conflict in the quoted subject matter.
    if condemnation:
        return "forg"

    if any(normalize_text(t) in text for t in economy_terms):
        return "econ"
    if any(normalize_text(t) in text for t in security_terms):
        return "secu"

    # Routing hints are fallback evidence only, never the first classifier.
    if route == "economy_markets":
        return "econ"
    if route == "defense_security":
        return "secu"
    if route == "breaking" and concrete_breaking and recent and not condemnation:
        return "urg"
    return "forg"


def _provider_trust(event):
    grade = str(event.get("source_grade", "") or "")
    role = str(event.get("source_role", "") or "")
    if grade == "A+":
        base = 99.0
    elif grade == "A":
        base = 94.0
    else:
        base = 88.0
    if role in {"support", "support_sensor"}:
        base -= 2.0
    return base


def _provider_to_news_item(event, provider):
    title = str(event.get("title", "") or "").strip()
    if not title:
        return None
    content_url = str(event.get("content_url", "") or "").strip()
    raw_url = str(event.get("url", "") or content_url).strip()
    display_url = content_url if content_url.startswith(("http://", "https://")) else raw_url
    if not display_url:
        return None

    if provider == "social_intel":
        source = str(
            event.get("organization_ar")
            or event.get("organization")
            or event.get("handle")
            or "مصدر رسمي"
        ).strip()
    else:
        source = str(
            event.get("source_name_ar")
            or event.get("source_name")
            or "مصدر إنذار رسمي"
        ).strip()

    attribution = str(event.get("attribution_ar", "") or "").strip()
    summary = str(event.get("summary", "") or "").strip()
    if attribution and attribution not in summary:
        summary = f"{attribution}. {summary}".strip(" .")

    published = _parse_provider_datetime(event.get("published"))
    topic = _provider_topic(event, provider)
    item = NewsItem(
        title=title,
        url=display_url,
        source=source,
        summary=summary,
        published=published,
        category=topic,
        region=str(event.get("region", "") or event.get("entity_ar", "") or "").strip(),
        original_title=str(event.get("original_title", "") or title).strip(),
        domain=_provider_domain(display_url),
        trust_score=_provider_trust(event),
        urgency_score=20.0 if topic == "urg" else 0.0,
        relevance_score=float(event.get("priority", 0) or 0) / 10.0,
        official=True,
        search_text="",
        alternate_sources=[],
        discovery_domain_hint=_provider_domain(display_url),
        official_source_id="",
        publication_evidence=f"{provider}:{event.get('source_id', '')}",
    )
    item.search_text = normalize_text(
        f"{item.title} {item.original_title} {item.summary} {item.source} {item.region}"
    )
    setattr(item, "_provider_kind", provider)
    setattr(item, "_exclusive_topic", topic)
    setattr(item, "_source_role", str(event.get("source_role", "") or ""))
    setattr(item, "_source_grade", str(event.get("source_grade", "") or ""))
    setattr(item, "_source_priority", int(event.get("priority", 0) or 0))
    setattr(item, "_dedup_group", str(event.get("dedup_group", "") or ""))
    setattr(item, "_canonical_event_url", content_url or display_url)
    setattr(item, "_event_status", str(event.get("event_status", "") or ""))
    setattr(item, "_event_status_strength", int(event.get("event_status_strength", 0) or 0))
    return item


async def _adapt_provider_events(events, provider):
    items = []
    for event in list(events or []):
        if not isinstance(event, dict):
            continue
        try:
            item = _provider_to_news_item(event, provider)
            if item:
                items.append(item)
        except Exception:
            log.exception("Provider event adapter failed provider=%s", provider)
    if items:
        try:
            await translate_news_titles(items, budget=PROVIDER_TRANSLATION_BUDGET)
        except Exception:
            log.exception("Provider title translation failed provider=%s", provider)

    # Preserve uncertainty in the visible headline for direct operational reports.
    if provider == "direct_radar":
        labels = {
            "preliminary": "معلومات أولية",
            "third_party_report": "بلاغ من طرف ثالث",
            "under_investigation": "قيد التحقيق",
        }
        for item in items:
            label = labels.get(getattr(item, "_event_status", ""))
            if label and not item.title.startswith(label):
                item.title = f"{label}: {item.title}"
                item.search_text = normalize_text(
                    f"{item.title} {item.original_title} {item.summary} {item.source} {item.region}"
                )
    return items


def _provider_preference(item):
    provider = str(getattr(item, "_provider_kind", "") or "")
    if provider == "direct_radar":
        provider_rank = 4
    elif bool(getattr(item, "official", False)) and not provider:
        provider_rank = 3
    elif provider == "social_intel":
        role = str(getattr(item, "_source_role", "") or "")
        provider_rank = 3 if role == "primary" else 2
    else:
        provider_rank = 1
    return (
        provider_rank,
        int(getattr(item, "_event_status_strength", 0) or 0),
        int(getattr(item, "_source_priority", 0) or 0),
        float(getattr(item, "trust_score", 0) or 0),
        _published_seconds(item) or 0,
    )


def _canonical_provider_url(item):
    value = str(getattr(item, "_canonical_event_url", "") or get_item_url(item) or "").strip()
    if not value:
        return ""
    try:
        parsed = urllib.parse.urlsplit(value)
        host = (parsed.hostname or "").lower().removeprefix("www.")
        path = re.sub(r"/+", "/", parsed.path or "/").rstrip("/")
        return f"{host}{path}" if host else ""
    except Exception:
        return ""


async def _run_news_collection():
    global LAST_NEWS_REFRESH
    try:
        items = await asyncio.wait_for(
            collect_news(max_items=150),
            timeout=NEWS_COLLECTION_TIMEOUT,
        )
        if items:
            items = deduplicate_events(items, limit=150)
            NEWS_CACHE.set("all_news", items)
            _invalidate_hot_view()
            LAST_NEWS_REFRESH = time.monotonic()
            return items
    except asyncio.TimeoutError:
        log.warning("News collection timed out; keeping last available cache.")
    except Exception:
        log.exception("News collection failed; keeping last available cache.")
    return NEWS_CACHE.peek("all_news") or []


async def collect_and_cache_news():
    """Single-flight base collector: concurrent refreshes share one task."""
    global NEWS_COLLECTION_TASK
    task = NEWS_COLLECTION_TASK
    if task is None or task.done():
        task = asyncio.create_task(_run_news_collection(), name="shared-news-collection")
        NEWS_COLLECTION_TASK = task
    try:
        return await asyncio.shield(task)
    finally:
        if NEWS_COLLECTION_TASK is task and task.done():
            NEWS_COLLECTION_TASK = None


async def _refresh_social_provider():
    global LAST_SOCIAL_REFRESH, SOCIAL_PROVIDER_HEALTH
    if collect_social_intel is None:
        return SOCIAL_CACHE.peek("social_news") or []
    try:
        events = await asyncio.wait_for(collect_social_intel(), timeout=SOCIAL_PROVIDER_TIMEOUT)
        if get_social_source_health is not None:
            SOCIAL_PROVIDER_HEALTH = get_social_source_health()
        items = await _adapt_provider_events(events, "social_intel")
        if items:
            SOCIAL_CACHE.set("social_news", deduplicate_events(items, limit=80))
            _invalidate_hot_view()
        LAST_SOCIAL_REFRESH = time.monotonic()
    except asyncio.TimeoutError:
        log.info("Social provider timed out; keeping previous cache.")
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Social provider failed; keeping previous cache.")
    return SOCIAL_CACHE.peek("social_news") or []


async def _refresh_direct_provider(force=False):
    global LAST_DIRECT_REFRESH, DIRECT_PROVIDER_HEALTH
    if collect_direct_radar is None:
        return DIRECT_CACHE.peek("direct_news") or []
    try:
        events = await asyncio.wait_for(
            collect_direct_radar(force=force),
            timeout=DIRECT_PROVIDER_TIMEOUT,
        )
        if get_direct_source_health is not None:
            DIRECT_PROVIDER_HEALTH = get_direct_source_health()
        items = await _adapt_provider_events(events, "direct_radar")
        if items:
            DIRECT_CACHE.set("direct_news", deduplicate_events(items, limit=80))
            _invalidate_hot_view()
        LAST_DIRECT_REFRESH = time.monotonic()
    except asyncio.TimeoutError:
        log.info("Direct radar timed out; keeping previous cache.")
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Direct radar failed; keeping previous cache.")
    return DIRECT_CACHE.peek("direct_news") or []


def _has_arabic_text(value):
    return bool(re.search(r"[\u0600-\u06FF]", str(value or "")))


def _provider_title_ready(item):
    """Only merge translated provider headlines into the Arabic hot view.

    Filtering happens BEFORE cross-provider deduplication so an untranslated
    social/direct duplicate can never replace a translated native-news item and
    then disappear from the topic result.
    """
    provider = str(getattr(item, "_provider_kind", "") or "")
    if provider not in {"social_intel", "direct_radar"}:
        return True
    return _has_arabic_text(get_item_title(item))


def _invalidate_hot_view():
    HOT_VIEW_CACHE.set("merged", None)


def get_cached_news_view(limit=HOT_CACHE_LIMIT):
    """Instant read-only merged snapshot; never performs network I/O."""
    cached = HOT_VIEW_CACHE.peek("merged")
    if cached is not None:
        return list(cached[:limit])

    breaking = BREAKING_CACHE.peek("breaking_news") or []
    direct = [
        item for item in (DIRECT_CACHE.peek("direct_news") or [])
        if _provider_title_ready(item)
    ]
    social = [
        item for item in (SOCIAL_CACHE.peek("social_news") or [])
        if _provider_title_ready(item)
    ]
    broad = NEWS_CACHE.peek("all_news") or []

    merged = deduplicate_events(
        list(direct) + list(breaking) + list(social) + list(broad),
        limit=HOT_CACHE_LIMIT,
    )
    HOT_VIEW_CACHE.set("merged", list(merged))
    return list(merged[:limit])


def _provider_due(last_refresh, interval):
    return not last_refresh or (time.monotonic() - last_refresh) >= interval


async def _run_all_source_refresh(force=False):
    jobs = []
    if force or _provider_due(LAST_NEWS_REFRESH, CACHE_TTL):
        jobs.append(collect_and_cache_news())
    if force or _provider_due(LAST_SOCIAL_REFRESH, SOCIAL_REFRESH_INTERVAL):
        jobs.append(_refresh_social_provider())
    if force or _provider_due(LAST_DIRECT_REFRESH, DIRECT_REFRESH_INTERVAL):
        jobs.append(_refresh_direct_provider(force=force))
    if jobs:
        results = await asyncio.gather(*jobs, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                log.error("Independent provider refresh failed: %r", result)
    return get_cached_news_view()


async def refresh_all_sources(force=False):
    """Global single-flight refresh with force-upgrade semantics.

    Concurrent callers share one cycle. If a manual/forced refresh arrives while
    a non-forced cycle is already running, exactly one forced cycle follows it.
    """
    global PROVIDER_REFRESH_TASK

    task = PROVIDER_REFRESH_TASK
    created_here = False
    if task is None or task.done():
        task = asyncio.create_task(
            _run_all_source_refresh(force=force),
            name="all-source-refresh",
        )
        task._force_refresh = bool(force)
        PROVIDER_REFRESH_TASK = task
        created_here = True

    running_force = bool(getattr(task, "_force_refresh", False))
    try:
        result = await asyncio.shield(task)
    finally:
        if PROVIDER_REFRESH_TASK is task and task.done():
            PROVIDER_REFRESH_TASK = None

    if force and not running_force and not created_here:
        return await refresh_all_sources(force=True)

    return result


def trigger_background_refresh(force=False):
    """Schedule enrichment and return immediately; safe for button handlers."""
    global PROVIDER_REFRESH_TASK
    task = PROVIDER_REFRESH_TASK
    if task is not None and not task.done():
        return task

    task = asyncio.create_task(
        _run_all_source_refresh(force=force),
        name="background-hot-cache-refresh",
    )
    task._force_refresh = bool(force)
    PROVIDER_REFRESH_TASK = task
    BACKGROUND_TASKS.add(task)

    def _done(done_task):
        global PROVIDER_REFRESH_TASK
        BACKGROUND_TASKS.discard(done_task)
        if PROVIDER_REFRESH_TASK is done_task:
            PROVIDER_REFRESH_TASK = None

    task.add_done_callback(_done)
    return task


async def provider_monitor():
    """Continuously refill hot caches without putting network work on UI callbacks."""
    while True:
        try:
            await refresh_all_sources(force=False)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Provider monitor cycle failed.")
        await asyncio.sleep(PROVIDER_LOOP_INTERVAL)


async def get_fresh_news(force_refresh=False):
    if force_refresh:
        return await refresh_all_sources(force=True)
    cached = get_cached_news_view()
    if cached:
        if (
            _provider_due(LAST_NEWS_REFRESH, CACHE_TTL)
            or _provider_due(LAST_SOCIAL_REFRESH, SOCIAL_REFRESH_INTERVAL)
            or _provider_due(LAST_DIRECT_REFRESH, DIRECT_REFRESH_INTERVAL)
        ):
            trigger_background_refresh(force=False)
        return cached
    return await refresh_all_sources(force=False)


def _balance_topic_sources(items, limit):
    """Re-rank every five-item page without dropping any valid story.

    At most two items from the same source are placed on a page when enough
    source diversity exists. If diversity is insufficient, the remaining best
    ranked items fill the page. The full result set is preserved.
    """
    remaining = list(items or [])[:limit]
    balanced = []

    while remaining and len(balanced) < limit:
        selected = []
        selected_indices = set()
        counts = {}

        for idx, item in enumerate(remaining):
            source_key = (
                normalize_text(get_item_source(item))
                or _provider_domain(get_item_url(item))
                or "unknown"
            )
            if counts.get(source_key, 0) >= 2:
                continue
            selected.append(item)
            selected_indices.add(idx)
            counts[source_key] = counts.get(source_key, 0) + 1
            if len(selected) >= PER_PAGE:
                break

        if len(selected) < PER_PAGE:
            for idx, item in enumerate(remaining):
                if idx in selected_indices:
                    continue
                selected.append(item)
                selected_indices.add(idx)
                if len(selected) >= PER_PAGE:
                    break

        balanced.extend(selected)
        remaining = [
            item for idx, item in enumerate(remaining)
            if idx not in selected_indices
        ]

    return balanced[:limit]


def _engine_topic(item):
    """Read the engine/provider topic without making it the final UI section."""
    forced = str(getattr(item, "_exclusive_topic", "") or "")
    if forced in TOPICS:
        return forced
    for key in ("urg", "secu", "econ", "forg", "gulf", "wrld"):
        if is_topic_match(item, key):
            return key
    return ""


def _contains_any(text, terms):
    text = normalize_text(text)
    return any(normalize_text(term) in text for term in terms if term)


def _resolved_topic(item):
    """Resolve every story to exactly one visible section.

    This is the single routing authority used by all six topic buttons.
    It prevents cross-section duplication while allowing general geographic
    stories to escape an overly broad engine specialist label.
    """
    forced = str(getattr(item, "_exclusive_topic", "") or "")
    if forced in TOPICS:
        return forced

    engine = _engine_topic(item)
    title = normalize_text(get_item_title(item))
    source = normalize_text(get_item_source(item))
    region = normalize_text(str(getattr(item, "region", "") or ""))

    econ_sources = (
        "بنك", "bank", "central bank", "reserve bank", "treasury",
        "وزارة المالية", "ministry of finance", "sama",
    )
    security_sources = (
        "وزارة الدفاع", "الدفاع", "وزارة الداخلية", "القوات المسلحة",
        "الجيش", "ministry of defense", "ministry of defence",
        "army", "navy", "air force",
    )
    official_sources = (
        "وزارة الخارجية", "الخارجية", "foreign ministry",
        "ministry of foreign affairs", "state department",
        "الرئاسة", "presidency", "الحكومة", "government",
        "الديوان الملكي",
    )
    explicit_official_terms = (
        "بيان رسمي", "تصريح رسمي", "بيان صحفي", "المتحدث الرسمي",
        "المتحدث باسم", "مصدر مسؤول", "أعلنت الوزارة", "اعلنت الوزارة",
        "قالت الوزارة", "أعلن الوزير", "اعلن الوزير", "قال الوزير",
    )

    # Institution identity is stronger than a broad engine label.
    if _contains_any(source, econ_sources) or _contains_any(title, TOPICS["econ"][1]):
        return "econ"
    if _contains_any(source, security_sources):
        return "secu"

    # Breaking must be explicit; ordinary attacks/security stories stay Security.
    if engine == "urg" and _contains_any(title, TOPICS["urg"][1]):
        return "urg"

    if _contains_any(title, TOPICS["secu"][1]):
        return "secu"

    # For official registry items, preserve the engine's specialist desk after
    # correcting obvious economy/security institutions above.
    if getattr(item, "official", False) and engine in {"econ", "forg", "urg", "secu"}:
        return engine

    if _contains_any(source, official_sources) or _contains_any(title, explicit_official_terms):
        return "forg"

    # Geographic browsing is the home for general news, including stories the
    # engine labelled too broadly as economy/diplomacy/security without explicit
    # specialist evidence.
    geo_text = f"{title} {region}"
    if (
        engine == "gulf"
        or "الشرق الاوسط" in region
        or _contains_any(geo_text, TOPICS["gulf"][1])
    ):
        return "gulf"
    if engine == "wrld" or _contains_any(geo_text, TOPICS["wrld"][1]):
        return "wrld"
    if region and region not in {"عام", "عالمي", "global", "غير محدد", "unknown"}:
        return "wrld"

    # Preserve otherwise-unplaceable specialist items instead of dropping them.
    if engine in {"econ", "forg", "urg", "secu"}:
        return engine
    return ""


def topic_filter(items, topic_key, max_results=25):
    if topic_key not in TOPICS:
        return []

    scored = []
    for item in items:
        if _resolved_topic(item) != topic_key:
            continue

        title = normalize_text(get_item_title(item))
        summary = normalize_text(get_item_summary(item))
        score = 0
        _, keywords = TOPICS[topic_key]

        for kw in keywords:
            nkw = normalize_text(kw)
            if not nkw:
                continue
            if nkw in title:
                score += 12
            elif nkw in summary:
                score += 4

        if topic_key == "urg":
            score += 20
        if topic_key == "forg" and getattr(item, "official", False):
            score += 12

        score += float(getattr(item, "relevance_score", 0) or 0)
        score += float(getattr(item, "trust_score", 0) or 0) * 0.03
        scored.append((score, item))

    scored.sort(key=lambda x: x[0], reverse=True)
    ranked = [item for _, item in scored[:max_results * 3]]
    if topic_key == "urg":
        return deduplicate_urgent_events(ranked, limit=max_results)
    deduped = deduplicate_events(ranked, limit=max_results)
    return _balance_topic_sources(deduped, max_results)


def generate_base_report(
    items,
    page=1,
    per_page=5,
    heading="📰 الأخبار",
    heading_html=None,
    subheading=None,
):
    start = max(0, (page - 1) * per_page)
    page_items = items[start:start + per_page]

    if heading_html:
        lines = [f"<b>{heading_html}</b>"]
    else:
        lines = [f"<b>{safe_html(heading)}</b>"]

    if subheading:
        lines.extend(["", safe_html(subheading)])

    lines.append("")

    for item in page_items:
        title = get_item_title(item)
        source = get_item_source(item)
        if not title:
            continue

        safe_url = build_safe_link(
            get_item_title(item),
            source,
            get_item_url(item),
        )
        lines.append(
            f"• <b>{safe_html(title)}</b>\n"
            f"  📍 المصدر: <code>{safe_html(source)}</code>\n"
            f'  <a href="{safe_html(safe_url)}">🔗 قراءة الخبر</a>'
        )
        lines.append("")

    return "\n".join(lines).strip()


def result_keyboard(key, page, total_items):
    total_pages = max(1, (total_items + PER_PAGE - 1) // PER_PAGE)
    nav = []

    if page > 1:
        nav.append(
            InlineKeyboardButton(
                "⬅️ السابقة",
                callback_data=f"t:{key}:{page-1}",
            )
        )
    if page < total_pages:
        nav.append(
            InlineKeyboardButton(
                "➕ المزيد",
                callback_data=f"t:{key}:{page+1}",
            )
        )

    rows = [nav] if nav else []
    rows.append([
        InlineKeyboardButton("🧠 تحليل", callback_data=f"analyze:{key}"),
        InlineKeyboardButton("🏠 مركز الأخبار", callback_data="home"),
    ])
    return InlineKeyboardMarkup(rows)


def search_result_keyboard(user_id, page):
    results = USER_SEARCH_RESULTS.get(user_id, [])
    total_pages = max(1, (len(results) + PER_PAGE - 1) // PER_PAGE)
    nav = []

    if page > 1:
        nav.append(
            InlineKeyboardButton(
                "⬅️ السابقة",
                callback_data=f"s:{page-1}",
            )
        )
    if page < total_pages:
        nav.append(
            InlineKeyboardButton(
                "➕ المزيد",
                callback_data=f"s:{page+1}",
            )
        )

    rows = [nav] if nav else []
    rows.append([
        InlineKeyboardButton("🏠 مركز الأخبار", callback_data="home")
    ])
    return InlineKeyboardMarkup(rows)


def main_keyboard(user_id):
    rows = []
    topic_items = list(TOPICS.items())

    for i in range(0, len(topic_items), 2):
        rows.append([
            InlineKeyboardButton(label, callback_data=f"t:{key}:1")
            for key, (label, _) in topic_items[i:i + 2]
        ])

    rows.append([
        InlineKeyboardButton(
            "🔔 التنبيهات" if user_id in MUTED_USERS else "🔕 التنبيهات",
            callback_data="toggle_alerts",
        )
    ])
    rows.append([
        InlineKeyboardButton("🔄 تحديث", callback_data="refresh"),
        InlineKeyboardButton("➕ المزيد", callback_data="more"),
    ])
    if is_owner(user_id):
        rows.append([
            InlineKeyboardButton("👑 إدارة الوصول", callback_data="owner:panel")
        ])
    return InlineKeyboardMarkup(rows)



def owner_panel_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 طلبات الدخول", callback_data="owner:pending"),
            InlineKeyboardButton("👥 المستخدمون", callback_data="owner:users"),
        ],
        [
            InlineKeyboardButton("🛡 الأدمن", callback_data="owner:admins"),
            InlineKeyboardButton("⛔ المحظورون", callback_data="owner:blocked"),
        ],
        [InlineKeyboardButton("🏠 مركز الأخبار", callback_data="home")],
    ])


def _display_user(meta, user_id):
    meta = meta or {}
    name = " ".join(
        x for x in [meta.get("first_name", ""), meta.get("last_name", "")] if x
    ).strip()
    username = (meta.get("username") or "").strip()
    pieces = []
    if name:
        pieces.append(name)
    if username:
        pieces.append(f"@{username}")
    pieces.append(str(user_id))
    return " | ".join(pieces)


async def notify_owner_access_request(context, user, first_request=True):
    if not first_request:
        return
    meta = _safe_user_record(user)
    label = _display_user(meta, user.id)
    keyboard = InlineKeyboardMarkup([[ 
        InlineKeyboardButton("✅ قبول", callback_data=f"owner:approve:{user.id}"),
        InlineKeyboardButton("❌ رفض", callback_data=f"owner:reject:{user.id}"),
        InlineKeyboardButton("⛔ حظر", callback_data=f"owner:block:{user.id}"),
    ]])
    try:
        await context.bot.send_message(
            chat_id=OWNER_ID,
            text=(
                "🔐 <b>طلب دخول جديد</b>\n\n"
                f"{safe_html(label)}\n"
                "هذا الحساب لا يملك أي صلاحية حتى توافق عليه."
            ),
            parse_mode="HTML",
            reply_markup=keyboard,
        )
    except Exception:
        log.exception("Could not notify owner about access request from %s", user.id)


async def deny_or_request_access(update, context):
    """Return True when the caller must be stopped before any bot feature runs."""
    user = update.effective_user
    if not user:
        return True
    uid = int(user.id)
    if is_authorized(uid) and uid not in BLOCKED_USERS:
        return False

    remove_runtime_user(uid)
    role = access_role(uid)
    query = update.callback_query
    message = update.effective_message

    if role == "blocked":
        text = "⛔ هذا الحساب محظور من استخدام البوت."
        if query:
            try:
                await query.answer(text, show_alert=True)
            except Exception:
                pass
        elif message:
            await message.reply_text(text)
        return True

    first_request = uid not in PENDING_USERS
    if first_request:
        PENDING_USERS[uid] = _safe_user_record(user)
        save_access_state()
    await notify_owner_access_request(context, user, first_request=first_request)

    text = (
        "🔒 <b>البوت خاص</b>\n\n"
        "تم تسجيل طلب الدخول. لن تتاح الأخبار أو البحث أو التنبيهات "
        "إلا بعد موافقة مالك البوت."
    )
    if query:
        try:
            await query.answer("🔒 لا تملك صلاحية الدخول.", show_alert=True)
        except Exception:
            pass
    elif message:
        await message.reply_text(text, parse_mode="HTML")
    return True


async def show_owner_panel(message):
    await message.reply_text(
        "👑 <b>لوحة المالك</b>\n\n"
        f"طلبات معلقة: {len(PENDING_USERS)}\n"
        f"المستخدمون المعتمدون: {len(APPROVED_USERS - {OWNER_ID})}\n"
        f"الأدمن: {len(ADMINS)}\n"
        f"المحظورون: {len(BLOCKED_USERS)}\n\n"
        "إضافة وحذف الأدمن بيد الـOwner فقط.",
        parse_mode="HTML",
        reply_markup=owner_panel_keyboard(),
    )


async def owner_callback(query, context, data, actor_id):
    if not is_owner(actor_id):
        await safe_query_answer(query, "⛔ هذه الصلاحية للمالك فقط.", show_alert=True)
        return

    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "panel"

    if action == "panel":
        await safe_query_answer(query, "👑 لوحة المالك")
        await show_owner_panel(query.message)
        return

    if action in {"approve", "reject", "block", "unblock", "promote", "demote", "revoke"}:
        if len(parts) < 3:
            await safe_query_answer(query, "⚠️ طلب غير صالح.", show_alert=True)
            return
        try:
            target = int(parts[2])
        except ValueError:
            await safe_query_answer(query, "⚠️ معرف غير صالح.", show_alert=True)
            return
        if target == OWNER_ID:
            await safe_query_answer(query, "👑 لا يمكن تعديل صلاحية المالك.", show_alert=True)
            return

        if action == "approve":
            BLOCKED_USERS.discard(target)
            PENDING_USERS.pop(target, None)
            APPROVED_USERS.add(target)
            register_user(target)
            result = "✅ تم قبول المستخدم."
            try:
                await context.bot.send_message(target, "✅ تمت الموافقة على دخولك للبوت. أرسل /start للبدء.")
            except Exception:
                pass
        elif action == "reject":
            PENDING_USERS.pop(target, None)
            APPROVED_USERS.discard(target)
            ADMINS.discard(target)
            remove_runtime_user(target)
            result = "❌ تم رفض الطلب."
        elif action == "block":
            PENDING_USERS.pop(target, None)
            APPROVED_USERS.discard(target)
            ADMINS.discard(target)
            BLOCKED_USERS.add(target)
            remove_runtime_user(target)
            result = "⛔ تم طرد المستخدم وحظره فوراً."
            try:
                await context.bot.send_message(target, "⛔ تم إلغاء صلاحية وصولك إلى البوت.")
            except Exception:
                pass
        elif action == "unblock":
            BLOCKED_USERS.discard(target)
            # Unblocking does not silently grant access; user must request again.
            result = "✅ تم رفع الحظر. يحتاج المستخدم موافقة جديدة للدخول."
        elif action == "promote":
            if target not in APPROVED_USERS:
                await safe_query_answer(query, "⚠️ اعتمد المستخدم أولاً.", show_alert=True)
                return
            APPROVED_USERS.discard(target)
            ADMINS.add(target)
            register_user(target)
            result = "🛡 تم تعيين المستخدم Admin."
        elif action == "demote":
            ADMINS.discard(target)
            APPROVED_USERS.add(target)
            register_user(target)
            result = "👤 تمت إزالة صلاحية Admin وبقي كمستخدم معتمد."
        else:  # revoke
            ADMINS.discard(target)
            APPROVED_USERS.discard(target)
            PENDING_USERS.pop(target, None)
            remove_runtime_user(target)
            result = "🚪 تم سحب صلاحية الدخول. يمكنه تقديم طلب جديد لاحقاً."

        save_access_state()
        await safe_query_answer(query, result, show_alert=True)
        await show_owner_panel(query.message)
        return

    if action == "pending":
        await safe_query_answer(query, "🟢 طلبات الدخول")
        if not PENDING_USERS:
            await query.message.reply_text("لا توجد طلبات دخول معلقة.", reply_markup=owner_panel_keyboard())
            return
        for uid, meta in list(PENDING_USERS.items())[:20]:
            await query.message.reply_text(
                f"🔐 {safe_html(_display_user(meta, uid))}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[ 
                    InlineKeyboardButton("✅ قبول", callback_data=f"owner:approve:{uid}"),
                    InlineKeyboardButton("❌ رفض", callback_data=f"owner:reject:{uid}"),
                    InlineKeyboardButton("⛔ حظر", callback_data=f"owner:block:{uid}"),
                ]]),
            )
        return

    if action == "users":
        await safe_query_answer(query, "👥 المستخدمون")
        users = sorted((APPROVED_USERS - {OWNER_ID}) - ADMINS)
        if not users:
            await query.message.reply_text("لا يوجد مستخدمون معتمدون حالياً.", reply_markup=owner_panel_keyboard())
            return
        for uid in users[:30]:
            await query.message.reply_text(
                f"👤 <code>{uid}</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[ 
                    InlineKeyboardButton("🛡 جعله Admin", callback_data=f"owner:promote:{uid}"),
                    InlineKeyboardButton("🚪 سحب الدخول", callback_data=f"owner:revoke:{uid}"),
                    InlineKeyboardButton("⛔ حظر", callback_data=f"owner:block:{uid}"),
                ]]),
            )
        return

    if action == "admins":
        await safe_query_answer(query, "🛡 الأدمن")
        if not ADMINS:
            await query.message.reply_text("لا يوجد Admins حالياً. رقّ مستخدماً معتمداً من قائمة المستخدمين.", reply_markup=owner_panel_keyboard())
            return
        for uid in sorted(ADMINS):
            await query.message.reply_text(
                f"🛡 Admin: <code>{uid}</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[ 
                    InlineKeyboardButton("👤 إزالة Admin", callback_data=f"owner:demote:{uid}"),
                    InlineKeyboardButton("⛔ حظر", callback_data=f"owner:block:{uid}"),
                ]]),
            )
        return

    if action == "blocked":
        await safe_query_answer(query, "⛔ المحظورون")
        if not BLOCKED_USERS:
            await query.message.reply_text("لا يوجد مستخدمون محظورون.", reply_markup=owner_panel_keyboard())
            return
        for uid in sorted(BLOCKED_USERS)[:30]:
            await query.message.reply_text(
                f"⛔ <code>{uid}</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[ 
                    InlineKeyboardButton("✅ رفع الحظر", callback_data=f"owner:unblock:{uid}"),
                ]]),
            )
        return

    await safe_query_answer(query, "⚠️ إجراء غير معروف.", show_alert=True)


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


async def analyze_with_gemini(items):
    """The only normal news path allowed to call Gemini."""
    if not ai_client:
        return "ℹ️ طبقة التحليل غير متاحة حالياً، لكن جمع الأخبار والبحث يعملان."

    try:
        context = build_ai_context(items[:8])
        response = await asyncio.wait_for(
            asyncio.to_thread(
                ai_client.models.generate_content,
                model=GEMINI_MODEL,
                contents=f"{ANALYSIS_PROMPT}\n\nالبيانات:\n{context}",
                config=types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(
                        thinking_level="low"
                    )
                ),
            ),
            timeout=GEMINI_TIMEOUT,
        )
        return (
            getattr(response, "text", None) or ""
        ).strip() or "⚠️ لم يُرجع التحليل نتيجة."
    except Exception:
        log.exception("Gemini analysis failed.")
        return "⚠️ تعذر التحليل بالذكاء الاصطناعي حالياً."


URGENT_STRONG_TERMS = {
    "عاجل", "طارئ", "هجوم", "انفجار", "قصف", "صاروخ", "زلزال",
    "غارة", "اشتباك", "إخلاء", "استهداف", "هجمات", "غارات",
    "اندلاع القتال", "اندلاع اشتباكات", "إطلاق النار", "اغتيال",
}

# Generic newsroom labels are discovery hints, not enough by themselves to
# justify an alert. A concrete event term, official/high-authority source, or
# independent corroboration must carry the publication decision.
URGENT_GENERIC_TERMS = {"عاجل", "طارئ"}
URGENT_SINGLE_SOURCE_TRUST = 94
URGENT_EXCEPTIONAL_SOURCE_TRUST = 92
URGENT_EXCEPTIONAL_SCORE = 13
URGENT_CORROBORATION_MIN_TRUST = 82


def urgent_score(item):
    title = normalize_text(get_item_title(item))
    summary = normalize_text(get_item_summary(item))
    score = 0
    strong = 0

    for term in URGENT_STRONG_TERMS:
        n = normalize_text(term)
        if n in title:
            score += 5
            strong += 1
        elif n in summary:
            score += 2

    if strong and getattr(item, "trust_score", 0) >= 70:
        score += 3
    return score


URGENT_KEY_STOPWORDS = {
    "عاجل", "خبر", "اخبار", "تحديث", "جديد", "الان", "اليوم",
    "breaking", "news", "urgent", "update", "latest",
    "قال", "قالت", "يقول", "بحسب", "عن", "على", "في", "من", "الى",
    "مع", "بعد", "قبل", "هذا", "هذه", "ذلك", "التي", "الذي",
    "وسائل", "وسايل", "اعلام", "تقارير", "تقرير", "دوي", "العاصمه", "مدينه",
    "تشهد", "يهز",
}


def urgent_event_tokens(item):
    title = normalize_text(get_item_title(item))
    tokens = []
    for token in title.split():
        if len(token) < 3:
            continue
        bare = token[2:] if token.startswith("ال") and len(token) > 4 else token
        if token in URGENT_KEY_STOPWORDS or bare in URGENT_KEY_STOPWORDS:
            continue
        # Arabic tanween can leave a trailing alef after diacritics are stripped
        # (e.g. "انفجاراً" -> "انفجارا"). Canonicalize that lightweight form
        # only for longer tokens used by the urgent-event matcher.
        if re.search(r"[\u0600-\u06FF]", token) and len(token) >= 5 and token.endswith("ا"):
            token = token[:-1]
        tokens.append(token)
    return set(tokens)


def same_urgent_event(a, b):
    """High-precision urgent-event matcher used across feed cycles and UI views.

    Breaking headlines often gain prefixes, source labels, or a few extra words on
    every polling cycle.  This matcher is intentionally more tolerant than the
    general-news matcher, while conflicting material numbers keep real updates
    separate.
    """
    na = normalize_text(get_item_title(a))
    nb = normalize_text(get_item_title(b))
    if not na or not nb:
        return False
    if na == nb:
        return True

    nums_a = set(re.findall(r"(?<!\w)\d{2,}(?!\w)", na))
    nums_b = set(re.findall(r"(?<!\w)\d{2,}(?!\w)", nb))
    if nums_a and nums_b and nums_a.isdisjoint(nums_b):
        return False

    ta = urgent_event_tokens(a)
    tb = urgent_event_tokens(b)
    if not ta or not tb:
        return urgent_key(a) == urgent_key(b)

    common = ta & tb
    smaller = min(len(ta), len(tb))
    union = ta | tb
    containment = len(common) / max(1, smaller)
    jaccard = len(common) / max(1, len(union))
    similarity = difflib.SequenceMatcher(None, na, nb).ratio()

    return (
        (len(common) >= 3 and (containment >= 0.55 or jaccard >= 0.42))
        or (len(common) >= 2 and containment >= 0.80 and jaccard >= 0.50)
        or (len(common) >= 2 and similarity >= 0.82)
    )


def urgent_key(item):
    """Publisher-independent fingerprint for an urgent headline."""
    tokens = sorted(urgent_event_tokens(item))
    if tokens:
        return " ".join(tokens)[:500]
    return normalize_text(get_item_title(item))[:500]


EVENT_DEDUP_STOPWORDS = URGENT_KEY_STOPWORDS | {
    "مصدر", "مصادر", "رسمي", "رسميه", "تصريح", "بيان", "اعلان",
    "اعلن", "اعلنت", "يعلن", "تقول", "قالت", "قال", "اكد", "اكدت",
    "reports", "report", "says", "said", "official", "statement",
    "according", "via", "live", "developing", "exclusive",
}


def news_event_tokens(item):
    """Meaningful headline tokens used for display-level event clustering.

    Engine deduplication intentionally stays conservative.  This second layer is
    stricter about repeated coverage of the same event so the user sees one
    representative story while corroborating publishers are retained as
    alternate sources.
    """
    title = normalize_text(get_item_title(item))
    return {
        token for token in title.split()
        if len(token) >= 3 and token not in EVENT_DEDUP_STOPWORDS
    }


def news_event_numbers(item):
    """Extract material numbers so different casualty/price updates stay separate."""
    return set(re.findall(r"(?<!\w)\d{2,}(?!\w)", normalize_text(get_item_title(item))))


def _published_seconds(item):
    value = getattr(item, "published", None)
    try:
        return float(value.timestamp()) if value else None
    except Exception:
        return None


def same_news_event(a, b):
    """High-precision event duplicate check for differently worded headlines.

    It requires strong lexical overlap and, when both timestamps are available,
    temporal proximity.  Conflicting material numbers prevent accidental merging
    of later factual updates (for example a changed casualty count).
    """
    na = normalize_text(get_item_title(a))
    nb = normalize_text(get_item_title(b))
    if not na or not nb:
        return False
    if na == nb:
        return True

    canonical_a = _canonical_provider_url(a)
    canonical_b = _canonical_provider_url(b)
    if canonical_a and canonical_b and canonical_a == canonical_b:
        return True

    ta = news_event_tokens(a)
    tb = news_event_tokens(b)
    if not ta or not tb:
        return False

    nums_a = news_event_numbers(a)
    nums_b = news_event_numbers(b)
    if nums_a and nums_b and nums_a.isdisjoint(nums_b):
        return False

    common = ta & tb
    smaller = min(len(ta), len(tb))
    union = ta | tb
    containment = len(common) / max(1, smaller)
    jaccard = len(common) / max(1, len(union))

    pa = _published_seconds(a)
    pb = _published_seconds(b)
    if pa is not None and pb is not None and abs(pa - pb) > 24 * 3600:
        # Exact/near-exact syndicated headlines can cross midnight, but a weaker
        # match after a full day is more likely to be a genuine follow-up.
        return len(common) >= 5 and containment >= 0.85 and jaccard >= 0.70

    return (
        (len(common) >= 4 and containment >= 0.72)
        or (len(common) >= 5 and jaccard >= 0.52)
        or (len(common) >= 3 and containment >= 0.88)
    )




def _topic_event_seen(user_id, topic_key, item):
    """Return True when this user has already been shown the same event in a topic."""
    seen_key = f"{user_id}:{topic_key}"
    seen = USER_SEEN_TOPIC_EVENTS.get(seen_key, [])
    matcher = same_urgent_event if topic_key == "urg" else same_news_event
    return any(matcher(item, prior) for prior in seen)


def _filter_unseen_topic_events(user_id, topic_key, items):
    """Keep only events not previously displayed to this user for this topic."""
    return [item for item in items if not _topic_event_seen(user_id, topic_key, item)]


def _remember_topic_events(user_id, topic_key, items):
    """Record displayed events with bounded per-user/topic memory."""
    if not items:
        return
    seen_key = f"{user_id}:{topic_key}"
    existing = USER_SEEN_TOPIC_EVENTS.setdefault(seen_key, [])
    matcher = same_urgent_event if topic_key == "urg" else same_news_event
    for item in items:
        if any(matcher(item, prior) for prior in existing):
            continue
        existing.append(item)
    if len(existing) > MAX_SEEN_TOPIC_EVENTS:
        del existing[:-MAX_SEEN_TOPIC_EVENTS]


def _merge_event_sources(primary, duplicate):
    """Preserve corroborating publishers on the representative event item."""
    values = list(getattr(primary, "alternate_sources", None) or [])
    values.append(get_item_source(duplicate))
    values.extend(list(getattr(duplicate, "alternate_sources", None) or []))

    primary_marker = normalize_text(get_item_source(primary))
    seen = {primary_marker} if primary_marker else set()
    merged = []
    for value in values:
        label = str(value or "").strip()
        marker = normalize_text(label)
        if not marker or marker in seen:
            continue
        seen.add(marker)
        merged.append(label)

    try:
        setattr(primary, "alternate_sources", merged)
    except Exception:
        pass


def deduplicate_events(items, limit=None):
    """Collapse repeated coverage and retain the closest/highest-authority source."""
    ordered = sorted(list(items or []), key=_provider_preference, reverse=True)
    base = list(deduplicate_news(ordered))
    unique = []
    for item in base:
        matched_index = None
        for idx, kept in enumerate(unique):
            if same_news_event(item, kept):
                matched_index = idx
                break
        if matched_index is not None:
            kept = unique[matched_index]
            if _provider_preference(item) > _provider_preference(kept):
                _merge_event_sources(item, kept)
                unique[matched_index] = item
            else:
                _merge_event_sources(kept, item)
            continue
        unique.append(item)
    return unique[:limit] if limit is not None else unique


def deduplicate_urgent_events(items, limit=None):
    """Collapse differently worded urgent coverage to one canonical event."""
    base = deduplicate_events(items or [])
    unique = []
    for item in base:
        matched = None
        for kept in unique:
            if same_urgent_event(item, kept):
                matched = kept
                break
        if matched is not None:
            _merge_event_sources(matched, item)
            continue
        unique.append(item)
    return unique[:limit] if limit is not None else unique


def _prune_recent_urgent_events(now=None):
    now = time.monotonic() if now is None else now
    while RECENT_URGENT_EVENTS:
        seen_at, _ = RECENT_URGENT_EVENTS[0]
        if now - seen_at <= RECENT_URGENT_EVENT_TTL:
            break
        RECENT_URGENT_EVENTS.popleft()


def urgent_event_seen_recently(item):
    """True when the same semantic urgent event was already delivered recently."""
    _prune_recent_urgent_events()
    return any(same_urgent_event(item, prior) for _, prior in RECENT_URGENT_EVENTS)


def remember_urgent_event(item):
    """Remember one delivered/baselined event without growing an unbounded history."""
    now = time.monotonic()
    _prune_recent_urgent_events(now)
    for idx, (seen_at, prior) in enumerate(RECENT_URGENT_EVENTS):
        if same_urgent_event(item, prior):
            _merge_event_sources(prior, item)
            RECENT_URGENT_EVENTS[idx] = (now, prior)
            return
    RECENT_URGENT_EVENTS.append((now, item))


def urgent_source_names(item):
    """Return unique publisher labels preserved by engine-level deduplication."""
    values = [get_item_source(item)] + list(
        getattr(item, "alternate_sources", None) or []
    )
    seen = set()
    result = []
    for value in values:
        label = str(value or "").strip()
        marker = normalize_text(label)
        if not marker or marker in seen:
            continue
        seen.add(marker)
        result.append(label)
    return result


def urgent_concrete_signal_count(item):
    """Count event-bearing urgent terms while ignoring generic alert labels."""
    title = normalize_text(get_item_title(item))
    summary = normalize_text(get_item_summary(item))
    count = 0
    for term in URGENT_STRONG_TERMS - URGENT_GENERIC_TERMS:
        needle = normalize_text(term)
        if needle and (needle in title or needle in summary):
            count += 1
    return count


def urgent_precision_state(item):
    """Decide whether a breaking signal is publishable without sacrificing trust.

    Returns a short state label or None. The gate deliberately separates
    discovery speed from publication confidence:
      - original official/high-authority publishers can publish immediately
        when the headline contains a concrete event signal;
      - two preserved independent publishers corroborating the same event can
        publish when the primary source is at least established-news quality;
      - a single very strong major-news report may publish only when both source
        trust and event intensity are exceptional.
    Generic words such as "عاجل" alone never satisfy the gate.
    """
    score = urgent_score(item)
    if score < 8:
        return None

    concrete = urgent_concrete_signal_count(item)
    if concrete <= 0:
        return None

    trust = float(getattr(item, "trust_score", 0) or 0)
    official = bool(getattr(item, "official", False))
    source_count = len(urgent_source_names(item))

    if official and trust >= URGENT_SINGLE_SOURCE_TRUST:
        return "official"

    if trust >= URGENT_SINGLE_SOURCE_TRUST:
        return "high_authority"

    if source_count >= 2 and trust >= URGENT_CORROBORATION_MIN_TRUST:
        return "corroborated"

    if (
        trust >= URGENT_EXCEPTIONAL_SOURCE_TRUST
        and score >= URGENT_EXCEPTIONAL_SCORE
        and concrete >= 2
    ):
        return "exceptional_single"

    return None


def find_new_urgent_news(items, limit=3):
    candidates = []
    for item in deduplicate_urgent_events(items):
        score = urgent_score(item)
        state = urgent_precision_state(item)
        key = urgent_key(item)
        if (
            state
            and key
            and key not in SENT_URGENT_KEYS
            and not urgent_event_seen_recently(item)
        ):
            candidates.append((score, state, item))

    candidates.sort(
        key=lambda x: (
            x[0],
            float(getattr(x[2], "trust_score", 0) or 0),
            len(urgent_source_names(x[2])),
        ),
        reverse=True,
    )

    # Keep one representative per event even when publishers use different
    # wording. Higher urgency/trust stays first because candidates are ranked.
    unique = []
    for _, state, item in candidates:
        if any(same_urgent_event(item, kept) for kept in unique):
            continue
        setattr(item, "_urgent_precision_state", state)
        unique.append(item)
        if len(unique) >= limit:
            break
    return unique


def urgent_verification_label(item):
    state = getattr(item, "_urgent_precision_state", "") or urgent_precision_state(item)
    if state == "official":
        return "مصدر رسمي"
    if state == "corroborated":
        return "تأكيد متعدد المصادر"
    if state == "high_authority":
        return "مصدر عالي الموثوقية"
    if state == "exceptional_single":
        return "مصدر رئيسي • إشارة قوية"
    return "مراجعة آلية"


async def format_urgent_alert(item):
    # Titles are already canonicalized/translated by news_engine.
    title = get_item_title(item)
    source = get_item_source(item)
    verification = urgent_verification_label(item)
    url = build_safe_link(
        get_item_title(item),
        source,
        get_item_url(item),
    )
    return (
        f"{visual('urgent')} <b>تنبيه عاجل</b>\n\n"
        f"<b>{safe_html(title)}</b>\n\n"
        f"✓ {safe_html(verification)}\n"
        f"📍 المصدر: <code>{safe_html(source)}</code>\n"
        f'<a href="{safe_html(url)}">🔗 قراءة الخبر</a>'
    )


async def _run_breaking_lane():
    """Read only the lightweight direct-feed lane under a hard latency bound."""
    try:
        return await asyncio.wait_for(
            collect_breaking_news(max_items=40),
            timeout=BREAKING_LANE_TIMEOUT,
        )
    except asyncio.TimeoutError:
        log.info("Breaking lane timed out; next cycle will retry.")
    except Exception:
        log.exception("Breaking lane collection failed.")
    return []


def _merge_breaking_into_cache(items):
    """Update the fast overlay as canonical events, not raw feed-cycle headlines."""
    if not items:
        return
    cached = BREAKING_CACHE.get("breaking_news") or []
    BREAKING_CACHE.set(
        "breaking_news",
        deduplicate_urgent_events(list(items) + list(cached), limit=80),
    )
    _invalidate_hot_view()


async def initialize_urgent_baseline():
    global URGENT_BASELINE_READY
    if URGENT_BASELINE_READY:
        return

    # Seed from the lightweight lane only. A restart must not launch the heavy
    # global collector just to establish which alerts already exist.
    items = await _run_breaking_lane()
    _merge_breaking_into_cache(items)
    for item in items:
        # Baseline only publishable events. A weak single-source signal is not
        # marked as sent, so a later corroborating source can still trigger it.
        if urgent_precision_state(item):
            key = urgent_key(item)
            if key:
                SENT_URGENT_KEYS.append(key)
                remember_urgent_event(item)

    URGENT_BASELINE_READY = True


async def urgent_monitor(application):
    await initialize_urgent_baseline()
    await asyncio.sleep(URGENT_INITIAL_DELAY)

    while True:
        started = time.monotonic()
        try:
            if ALERT_USERS:
                # Fast lane only: direct public RSS feeds. The heavy collector
                # stays on its own cadence and can never delay an urgent alert.
                items = await _run_breaking_lane()
                _merge_breaking_into_cache(items)
                alerts = find_new_urgent_news(items)
                publishable = sum(
                    1 for item in deduplicate_urgent_events(items)
                    if urgent_precision_state(item)
                )
                log.info(
                    "Urgent precision lane items=%d publishable=%d alerts=%d elapsed=%.2fs",
                    len(items), publishable, len(alerts), time.monotonic() - started,
                )

                for item in alerts:
                    key = urgent_key(item)
                    message = await format_urgent_alert(item)
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
                            )
                            delivered = True
                        except Exception:
                            log.exception(
                                "Urgent alert send failed for %s",
                                user_id,
                            )

                    if delivered:
                        SENT_URGENT_KEYS.append(key)
                        remember_urgent_event(item)

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Urgent monitor error.")

        # Start-to-start cadence: a slow network cycle does not accumulate drift,
        # while still guaranteeing at least a short pause between feed polls.
        elapsed = time.monotonic() - started
        await asyncio.sleep(max(5, URGENT_MONITOR_INTERVAL - elapsed))


async def post_init(application):
    global URGENT_MONITOR_STARTED, URGENT_MONITOR_TASK
    global PROVIDER_MONITOR_STARTED, PROVIDER_MONITOR_TASK
    global ACCESS_BOT
    if URGENT_MONITOR_STARTED:
        return

    # Railway's container filesystem is disposable. Restore the durable access
    # snapshot from the owner's existing Telegram private chat before admitting
    # users or constructing the alert-recipient set. Telegram failure is nonfatal.
    ACCESS_BOT = application.bot
    await restore_access_state_from_telegram(application.bot)
    _schedule_access_cloud_sync()

    # Restore only authorized recipients. Old/revoked users are never reconnected.
    ALERT_USERS.clear()
    ALERT_USERS.update((APPROVED_USERS | ADMINS | {OWNER_ID}) - BLOCKED_USERS)

    URGENT_MONITOR_STARTED = True
    await initialize_custom_emoji_pack(application)
    URGENT_MONITOR_TASK = asyncio.create_task(
        urgent_monitor(application),
        name="urgent-news-monitor",
    )
    if not PROVIDER_MONITOR_STARTED:
        PROVIDER_MONITOR_STARTED = True
        PROVIDER_MONITOR_TASK = asyncio.create_task(
            provider_monitor(),
            name="provider-hot-cache-monitor",
        )

    # Warm the merged Hot Cache immediately after startup without delaying startup.
    trigger_background_refresh(force=False)


async def post_stop(application):
    global URGENT_MONITOR_STARTED, URGENT_MONITOR_TASK
    global PROVIDER_MONITOR_STARTED, PROVIDER_MONITOR_TASK, PROVIDER_REFRESH_TASK
    global NEWS_COLLECTION_TASK
    global ACCESS_BOT, ACCESS_CLOUD_SYNC_TASK

    access_task = ACCESS_CLOUD_SYNC_TASK
    ACCESS_CLOUD_SYNC_TASK = None
    ACCESS_BOT = None
    task = URGENT_MONITOR_TASK
    provider_task = PROVIDER_MONITOR_TASK
    PROVIDER_MONITOR_TASK = None
    PROVIDER_MONITOR_STARTED = False
    refresh_task = PROVIDER_REFRESH_TASK
    PROVIDER_REFRESH_TASK = None
    news_task = NEWS_COLLECTION_TASK
    NEWS_COLLECTION_TASK = None
    URGENT_MONITOR_TASK = None
    URGENT_MONITOR_STARTED = False

    background_pending = {
        bg_task for bg_task in list(BACKGROUND_TASKS)
        if not bg_task.done()
    }
    for bg_task in background_pending:
        bg_task.cancel()
    if background_pending:
        await asyncio.gather(*background_pending, return_exceptions=True)
    BACKGROUND_TASKS.clear()

    if access_task and not access_task.done():
        access_task.cancel()
        try:
            await access_task
        except asyncio.CancelledError:
            pass

    for extra_task in (provider_task, refresh_task, news_task):
        if extra_task and not extra_task.done():
            extra_task.cancel()
            try:
                await extra_task
            except asyncio.CancelledError:
                pass

    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def start(update, context):
    user = update.effective_user
    if not user or not update.message:
        return

    if await deny_or_request_access(update, context):
        return

    register_user(user.id)
    await update.message.reply_text(
        f"{visual('world')} <b>GLOBAL INTEL | مركز الأخبار</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "الرصد العالمي نشط.\n"
        "اختر مسار المتابعة، وستظهر الأخبار المتاحة أولاً "
        "بينما تستمر التغطية في الخلفية.\n\n"
        "🚨 التنبيهات العاجلة تعمل تلقائياً ويمكن إيقافها.",
        reply_markup=main_keyboard(user.id),
        parse_mode="HTML",
    )


async def send_topic_update(message, key, previous_results, user_id=None):
    """Refresh a topic in the background and send only meaningful additions."""
    try:
        fresh = await get_fresh_news(force_refresh=True)
        current = topic_filter(fresh, key, MAX_TOPIC_RESULTS)
        if not current:
            return

        matcher = same_urgent_event if key == "urg" else same_news_event
        additions = [
            item for item in current
            if not any(matcher(item, previous) for previous in previous_results)
        ]
        additions = deduplicate_events(additions, limit=PER_PAGE)
        if user_id is not None:
            additions = _filter_unseen_topic_events(user_id, key, additions)

        if not additions:
            return

        report = generate_base_report(
            additions,
            1,
            PER_PAGE,
            heading_html=(
                f"{status_visual('monitoring')} "
                f"{safe_html('تحديث التغطية')}"
            ),
            subheading=f"+{len(additions)} أخبار جديدة في {TOPICS[key][0]}",
        )
        await message.reply_text(
            report,
            disable_web_page_preview=True,
            parse_mode="HTML",
        )
        if user_id is not None:
            _remember_topic_events(user_id, key, additions)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Progressive topic update failed.")


async def show_topic(query, user_id, key, page):
    """Instant topic pagination from a stable cache snapshot.

    Opening a populated section never starts a new collection cycle. Page 2+
    always uses the same result snapshot created on page 1, so "المزيد" is
    pure pagination and cannot be delayed or reshuffled by background refreshes.
    """
    await safe_query_answer(query)

    snapshot_key = f"{user_id}:{key}"

    try:
        # Page 1 takes a fresh snapshot from the already-populated shared cache.
        # Later pages must use that same snapshot for stable, instant pagination.
        raw_results = []
        if page == 1:
            cached = get_cached_news_view()
            raw_results = topic_filter(cached, key, MAX_TOPIC_RESULTS)
            results = _filter_unseen_topic_events(user_id, key, raw_results)
            if results:
                USER_TOPIC_RESULTS[snapshot_key] = list(results)
        else:
            results = USER_TOPIC_RESULTS.get(snapshot_key, [])
            if not results:
                cached = get_cached_news_view()
                results = topic_filter(cached, key, MAX_TOPIC_RESULTS)
                if results:
                    USER_TOPIC_RESULTS[snapshot_key] = list(results)

        if results:
            total_pages = max(1, (len(results) + PER_PAGE - 1) // PER_PAGE)
            page = min(page, total_pages)

            report = generate_base_report(
                results,
                page,
                PER_PAGE,
                heading=TOPICS[key][0],
                subheading=(
                    f"{len(results)} خبر متاح • "
                    f"الصفحة {page} من {total_pages}"
                ),
            )
            await query.message.reply_text(
                report,
                reply_markup=result_keyboard(key, page, len(results)),
                disable_web_page_preview=True,
                parse_mode="HTML",
            )
            start = (page - 1) * PER_PAGE
            _remember_topic_events(user_id, key, results[start:start + PER_PAGE])

            # UI callbacks are cache-only. The shared provider monitor owns
            # refresh scheduling so button presses never start heavy network work.
            return

        # The topic has cached stories, but this user has already seen them.
        # Do not resend the same event; search for genuine additions in background.
        if page == 1 and raw_results:
            await query.message.reply_text(
                f"<b>{safe_html(TOPICS[key][0])}</b>\n\n"
                "لا توجد أخبار جديدة منذ آخر عرض.",
                parse_mode="HTML",
            )
            track_task(
                send_topic_update(query.message, key, raw_results, user_id),
                f"topic-refresh-{user_id}-{key}",
            )
            return

        # No cached result exists. Only page 1 may start background discovery.
        if page == 1:
            await query.message.reply_text(
                f"{status_visual('monitoring')} <b>{safe_html(TOPICS[key][0])}</b>\n\n"
                "◌ لا توجد نتائج جاهزة في الذاكرة الآن.\n"
                "📡 جاري توسيع التغطية في الخلفية...",
                parse_mode="HTML",
            )
            track_task(
                send_topic_update(query.message, key, [], user_id),
                f"topic-refresh-{user_id}-{key}",
            )
        else:
            await query.message.reply_text(
                f"<b>{safe_html(TOPICS[key][0])}</b>\n\n"
                "لا توجد أخبار إضافية محفوظة لهذه الصفحة.",
                parse_mode="HTML",
            )

    except Exception:
        log.exception("Topic handler failed.")
        await query.message.reply_text(
            "⚠️ تعذر عرض هذا القسم الآن. "
            "البوت مستمر ويمكنك فتح قسم آخر."
        )


async def show_search_page(query, user_id, page):
    results = USER_SEARCH_RESULTS.get(user_id, [])
    if not results:
        await query.message.reply_text("🔎 لا توجد نتائج بحث محفوظة.")
        return

    report = generate_base_report(
        results,
        page,
        PER_PAGE,
        heading=f"🔎 نتائج البحث: {USER_SEARCH_QUERY.get(user_id, '')}",
    )
    await query.message.reply_text(
        report,
        reply_markup=search_result_keyboard(user_id, page),
        disable_web_page_preview=True,
        parse_mode="HTML",
    )


async def progressive_online_search(
    message,
    status,
    user_id,
    raw_query,
    query_text,
    local_results,
):
    """Online discovery is additive and never blocks the first visible state."""
    try:
        online = await asyncio.wait_for(
            search_news_online(query_text, MAX_SEARCH_RESULTS),
            timeout=ONLINE_SEARCH_TIMEOUT,
        )
    except asyncio.TimeoutError:
        log.info("Online search timed out; keeping available results.")
        if not local_results:
            try:
                await status.edit_text(
                    f"🔎 <b>{safe_html(raw_query)}</b>\n\n"
                    "لم يتم العثور على نتائج خلال جولة البحث الحالية.",
                    parse_mode="HTML",
                )
            except Exception:
                pass
        return
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Online search failed; keeping available results.")
        if not local_results:
            try:
                await status.edit_text(
                    f"🔎 <b>{safe_html(raw_query)}</b>\n\n"
                    "تعذر إكمال البحث العالمي الآن. حاول مرة أخرى بعد قليل.",
                    parse_mode="HTML",
                )
            except Exception:
                pass
        return

    online = deduplicate_events(online or [], limit=MAX_SEARCH_RESULTS)
    local_keys = {urgent_key(item) for item in local_results}
    additions = [
        item for item in online
        if urgent_key(item) not in local_keys
    ]

    merged = deduplicate_events(
        local_results + additions,
        limit=MAX_SEARCH_RESULTS,
    )
    USER_SEARCH_RESULTS[user_id] = merged
    USER_SEARCH_QUERY[user_id] = raw_query

    if not local_results:
        if not merged:
            await status.edit_text(
                f"🔎 <b>{safe_html(raw_query)}</b>\n\n"
                "لم يتم العثور على نتائج في التغطية المتاحة حالياً.",
                parse_mode="HTML",
            )
            return

        report = generate_base_report(
            merged,
            1,
            PER_PAGE,
            heading=f"🔎 {raw_query}",
            subheading=f"✓ تم العثور على {len(merged)} نتائج",
        )
        await status.edit_text(
            report,
            reply_markup=search_result_keyboard(user_id, 1),
            disable_web_page_preview=True,
            parse_mode="HTML",
        )
        return

    if not additions:
        return

    first_additions = additions[:PER_PAGE]
    report = generate_base_report(
        first_additions,
        1,
        PER_PAGE,
        heading_html=(
            f"{status_visual('monitoring')} "
            f"{safe_html('تحديث البحث')}"
        ),
        subheading=f"+{len(additions)} نتائج إضافية حديثة عن: {raw_query}",
    )
    await message.reply_text(
        report,
        reply_markup=search_result_keyboard(user_id, 1),
        disable_web_page_preview=True,
        parse_mode="HTML",
    )


async def button_handler(update, context):
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return

    user_id = user.id
    data = query.data or ""

    if await deny_or_request_access(update, context):
        return

    register_user(user_id)
    log.info("Callback received: %s", data)

    if data.startswith("owner:"):
        await owner_callback(query, context, data, user_id)
        return

    if not claim_callback(query, user_id, data):
        return

    if data == "toggle_alerts":
        if user_id in MUTED_USERS:
            MUTED_USERS.discard(user_id)
            await safe_query_answer(
                query,
                "🔔 تم تفعيل التنبيهات العاجلة.",
                show_alert=True,
            )
        else:
            MUTED_USERS.add(user_id)
            await safe_query_answer(
                query,
                "🔕 تم إيقاف التنبيهات العاجلة.",
                show_alert=True,
            )
        try:
            await query.message.edit_reply_markup(main_keyboard(user_id))
        except Exception:
            pass
        return

    if data == "home":
        await safe_query_answer(query, "🏠 مركز الأخبار")
        await query.message.reply_text(
            f"{visual('world')} <b>GLOBAL INTEL | مركز الأخبار</b>\n\n"
            "اختر القسم المطلوب. الأخبار المتاحة تظهر أولاً "
            "والرصد يستمر في الخلفية.",
            reply_markup=main_keyboard(user_id),
            parse_mode="HTML",
        )
        return

    if data == "refresh":
        await safe_query_answer(query, "🔄 بدأ التحديث", show_alert=False)

        cached = get_cached_news_view()
        await query.message.reply_text(
            f"{status_visual('monitoring')} <b>تحديث التغطية</b>\n\n"
            f"● المتاح الآن: {len(cached)} خبر\n"
            "◌ جاري توسيع التغطية في الخلفية...",
            parse_mode="HTML",
            reply_markup=main_keyboard(user_id),
        )

        async def refresh_and_notify():
            before = len(cached)
            fresh = await get_fresh_news(force_refresh=True)
            after = len(fresh)
            try:
                await query.message.reply_text(
                    f"✅ <b>اكتملت جولة التحديث</b>\n\n"
                    f"الأخبار المتاحة الآن: {after}"
                    + (
                        f"\n+{max(0, after - before)} إضافة جديدة"
                        if after > before else ""
                    ),
                    parse_mode="HTML",
                    reply_markup=main_keyboard(user_id),
                )
            except Exception:
                log.exception("Refresh completion message failed.")

        track_task(
            refresh_and_notify(),
            f"manual-refresh-{user_id}",
        )
        return

    if data == "more":
        await safe_query_answer(query, "🔎 البحث متاح الآن")
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
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "🏠 مركز الأخبار",
                        callback_data="home",
                    )
                ]
            ]),
        )
        return

    if data.startswith("s:"):
        try:
            page = max(1, int(data.split(":", 1)[1]))
        except ValueError:
            await safe_query_answer(query, "⚠️ صفحة غير صالحة.")
            return

        await safe_query_answer(query, "📄 جاري عرض النتائج...")
        await show_search_page(query, user_id, page)
        return

    if data.startswith("analyze:"):
        key = data.split(":", 1)[1]
        if key not in TOPICS:
            return

        await safe_query_answer(query, "🧠 جاري تجهيز التحليل...")
        status = await query.message.reply_text(
            "🧠 جاري تحليل البيانات..."
        )

        try:
            items = get_cached_news_view()
            if not items:
                trigger_background_refresh(force=False)
                await status.edit_text(
                    "🧠 لا توجد بيانات جاهزة للتحليل الآن.\n"
                    "📡 جاري تحديث التغطية في الخلفية، ثم أعد المحاولة بعد قليل."
                )
                return

            results = topic_filter(items, key, 8)
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
            log.exception("Analysis failed.")
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
            page = max(1, int(page_text))
        except ValueError:
            return

        await show_topic(query, user_id, key, page)
        return


async def handle_user_message(update, context):
    if not update.message:
        return

    text = (update.message.text or "").strip()
    user = update.effective_user
    if not text or not user:
        return

    if await deny_or_request_access(update, context):
        return

    user_id = user.id
    register_user(user_id)
    lock = USER_LOCKS.setdefault(user_id, asyncio.Lock())

    if lock.locked():
        await update.message.reply_text(
            "⏳ يوجد طلب جارٍ حالياً. ستظهر نتيجته فور توفرها."
        )
        return

    async with lock:
        status = await update.message.reply_text(
            f"🔎 <b>{safe_html(text)}</b>\n\n"
            "◌ البحث مستمر...",
            parse_mode="HTML",
        )

        try:
            query_text = expand_search_query(text)

            # User search has priority over the heavy global collector.
            # Use whatever cache already exists, but never start a full collection
            # while the user's direct search is running. Online discovery below
            # provides fresh results independently.
            cached = get_cached_news_view()

            local_results = await search_news(
                cached,
                query_text,
                MAX_SEARCH_RESULTS,
            )
            local_results = deduplicate_events(local_results, limit=MAX_SEARCH_RESULTS)

            USER_SEARCH_QUERY[user_id] = text

            if local_results:
                USER_SEARCH_RESULTS[user_id] = local_results
                report = generate_base_report(
                    local_results,
                    1,
                    PER_PAGE,
                    heading=f"🔎 {text}",
                    subheading=f"✓ تم العثور على {len(local_results)} نتائج متاحة الآن",
                )
                await status.edit_text(
                    report,
                    reply_markup=search_result_keyboard(user_id, 1),
                    disable_web_page_preview=True,
                    parse_mode="HTML",
                )
            else:
                USER_SEARCH_RESULTS[user_id] = []
                await status.edit_text(
                    f"🔎 <b>{safe_html(text)}</b>\n\n"
                    "📡 جاري توسيع التغطية العالمية...",
                    parse_mode="HTML",
                )

            track_task(
                progressive_online_search(
                    update.message,
                    status,
                    user_id,
                    text,
                    query_text,
                    local_results,
                ),
                f"online-search-{user_id}",
            )

        except Exception:
            log.exception("Search failed.")
            await status.edit_text(
                "⚠️ تعذر تنفيذ هذا البحث الآن. "
                "البوت مستمر ويمكنك المحاولة بعبارة أخرى."
            )


async def admin_command(update, context):
    user = update.effective_user
    if not user or not update.message:
        return
    if await deny_or_request_access(update, context):
        return
    if not is_owner(user.id):
        await update.message.reply_text("⛔ لوحة إدارة الوصول للـOwner فقط.")
        return
    await show_owner_panel(update.message)


async def error_handler(update, context):
    error = context.error
    exc_info = None
    if isinstance(error, BaseException):
        exc_info = (type(error), error, error.__traceback__)
    log.error(
        "Unhandled Telegram error: %r",
        error,
        exc_info=exc_info,
    )


def main():
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_stop(post_stop)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin_command))
    application.add_handler(
        CallbackQueryHandler(
            button_handler,
            pattern=(
                r"^(t:.*|s:\d+|home|refresh|more|"
                r"toggle_alerts|analyze:.*|owner:.*)$"
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

    port = int(os.getenv("PORT", "8080"))
    public_domain = os.getenv(
        "RAILWAY_PUBLIC_DOMAIN",
        "worker-production-347b.up.railway.app",
    )
    webhook_path = hashlib.sha256(
        BOT_TOKEN.encode("utf-8")
    ).hexdigest()
    webhook_url = f"https://{public_domain}/{webhook_path}"

    log.info("Starting Telegram webhook on port %s", port)
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
