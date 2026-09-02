import asyncio
import hashlib
import html
import logging
import re

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import aiohttp
import feedparser


# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger(__name__)


# ============================================================
# SETTINGS
# ============================================================

REQUEST_TIMEOUT = 7
SOURCE_TIMEOUT = 5
GLOBAL_COLLECTION_TIMEOUT = 14

MAX_ITEMS_PER_SOURCE = 20
MAX_NEWS_ITEMS = 150
MAX_SEARCH_RESULTS = 15

MAX_SUMMARY_LENGTH = 700

MAX_RSS_BYTES = 3 * 1024 * 1024
MAX_HTML_BYTES = 2 * 1024 * 1024

USER_AGENT = (
    "Mozilla/5.0 (compatible; GlobalIntelBot/2.0; "
    "+https://www.google.com/bot.html)"
)


# ============================================================
# SOURCE DEFINITIONS
# ============================================================

@dataclass(frozen=True)
class SourceConfig:
    name: str
    url: str
    source_type: str
    country: str
    trust: float
    language: str = "ar"


# ------------------------------------------------------------
# RSS SOURCES
# ------------------------------------------------------------

RSS_SOURCES = [
    SourceConfig(
        name="SPA",
        url="https://www.spa.gov.sa/rss",
        source_type="official",
        country="Saudi Arabia",
        trust=1.00,
    ),
    SourceConfig(
        name="France24 Arabic",
        url="https://www.france24.com/ar/rss",
        source_type="international",
        country="France",
        trust=0.88,
    ),
    SourceConfig(
        name="BBC Arabic",
        url="https://feeds.bbci.co.uk/arabic/rss.xml",
        source_type="international",
        country="United Kingdom",
        trust=0.88,
    ),
    SourceConfig(
        name="Türkiye MFA",
        url="https://www.mfa.gov.tr/rss.en.mfa",
        source_type="foreign_ministry",
        country="Türkiye",
        trust=1.00,
        language="en",
    ),
    SourceConfig(
        name="Türkiye MFA Arabic",
        url="https://www.mfa.gov.tr/rss.ar.mfa",
        source_type="foreign_ministry",
        country="Türkiye",
        trust=1.00,
        language="ar",
    ),
]


# ------------------------------------------------------------
# HTML / OFFICIAL SOURCES
# ------------------------------------------------------------

HTML_SOURCES = [
    # Saudi Arabia
    SourceConfig(
        name="Saudi MOFA News",
        url="https://www.mofa.gov.sa/en/ministry/news",
        source_type="foreign_ministry",
        country="Saudi Arabia",
        trust=1.00,
        language="en",
    ),
    SourceConfig(
        name="Saudi MOFA Statements",
        url="https://www.mofa.gov.sa/en/ministry/statements",
        source_type="foreign_ministry",
        country="Saudi Arabia",
        trust=1.00,
        language="en",
    ),

    # Qatar
    SourceConfig(
        name="Qatar MOFA News",
        url="https://mofa.gov.qa/en/latest-articles",
        source_type="foreign_ministry",
        country="Qatar",
        trust=1.00,
        language="en",
    ),
    SourceConfig(
        name="Qatar MOFA Statements",
        url="https://mofa.gov.qa/en/latest-articles/statements",
        source_type="foreign_ministry",
        country="Qatar",
        trust=1.00,
        language="en",
    ),

    # UAE
    SourceConfig(
        name="UAE MOFA MediaHub",
        url="https://www.mofa.gov.ae/en/mediahub/news",
        source_type="foreign_ministry",
        country="UAE",
        trust=1.00,
        language="en",
    ),

    # UK
    SourceConfig(
        name="UK FCDO",
        url="https://www.gov.uk/search/news-and-communications",
        source_type="foreign_ministry",
        country="United Kingdom",
        trust=0.98,
        language="en",
    ),

    # Egypt
    SourceConfig(
        name="Egypt MFA",
        url="https://www.mfa.gov.eg/en/Ministry/News",
        source_type="foreign_ministry",
        country="Egypt",
        trust=1.00,
        language="en",
    ),

    # Kuwait
    SourceConfig(
        name="Kuwait MFA",
        url="https://www.mofa.gov.kw/",
        source_type="foreign_ministry",
        country="Kuwait",
        trust=1.00,
        language="ar",
    ),

    # China
    SourceConfig(
        name="China MFA",
        url="https://www.mfa.gov.cn/eng/",
        source_type="foreign_ministry",
        country="China",
        trust=0.98,
        language="en",
    ),
]


# ============================================================
# KEYWORDS
# ============================================================

BREAKING_KEYWORDS = {
    "عاجل",
    "عاجلة",
    "طارئ",
    "طوارئ",
    "بيان عاجل",
    "هجوم",
    "هجمات",
    "انفجار",
    "انفجارات",
    "اشتباك",
    "اشتباكات",
    "قصف",
    "صاروخ",
    "صواريخ",
    "مسيرة",
    "مسيرات",
    "اغتيال",
    "مقتل",
    "وفاة",
    "زلزال",
    "تسونامي",
    "إخلاء",
    "تحذير",
    "إغلاق",
    "حظر",
    "عقوبات",
    "war",
    "attack",
    "missile",
    "strike",
    "breaking",
    "emergency",
}


TOPIC_KEYWORDS = {
    "urgent": {
        "عاجل", "عاجلة", "طارئ", "هجوم", "قصف", "صاروخ",
        "صواريخ", "مسيرة", "انفجار", "اشتباك", "اغتيال",
        "تحذير", "طوارئ", "breaking", "attack", "missile",
        "strike", "emergency",
    },

    "world": {
        "العالم", "دولي", "دولية", "الأمم المتحدة",
        "مجلس الأمن", "أمريكا", "أوروبا", "روسيا", "الصين",
        "إيران", "إسرائيل", "أوكرانيا", "غزة",
        "united nations", "security council",
    },

    "gulf": {
        "الخليج", "مجلس التعاون", "السعودية", "الإمارات",
        "قطر", "الكويت", "البحرين", "عمان",
        "gulf", "gcc", "saudi", "uae", "qatar",
        "kuwait", "bahrain", "oman",
    },

    "america": {
        "أمريكا", "الولايات المتحدة", "واشنطن",
        "ترامب", "البيت الأبيض", "الكونغرس",
        "united states", "washington", "trump",
        "white house", "congress",
    },

    "europe": {
        "أوروبا", "الاتحاد الأوروبي", "بريطانيا",
        "فرنسا", "ألمانيا", "إيطاليا", "بروكسل",
        "europe", "european union", "uk", "france",
        "germany", "italy", "brussels",
    },

    "asia": {
        "آسيا", "الصين", "اليابان", "الهند", "كوريا",
        "باكستان", "تركيا", "تايوان",
        "asia", "china", "japan", "india", "korea",
        "pakistan", "turkey", "taiwan",
    },

    "energy": {
        "النفط", "الغاز", "أوبك", "أوبك+", "طاقة",
        "أسعار النفط", "برميل", "النفط الخام",
        "oil", "gas", "opec", "energy", "crude",
        "barrel",
    },

    "security": {
        "أمن", "أمني", "دفاع", "دفاعي", "عسكري",
        "جيش", "أسلحة", "صواريخ", "طائرات",
        "دفاع جوي", "حلف", "تحالف",
        "security", "defense", "military", "missile",
        "air defense", "alliance",
    },

    "foreign": {
        "الخارجية", "وزارة الخارجية", "وزير الخارجية",
        "سفير", "سفارة", "بيان", "تصريح",
        "اجتماع", "مباحثات", "محادثات", "اتصال",
        "foreign ministry", "foreign minister",
        "embassy", "statement", "diplomatic",
        "talks",
    },
}


# ============================================================
# QUERY EXPANSION
# ============================================================

QUERY_ALIASES = {
    "السعودية": [
        "السعودية",
        "المملكة العربية السعودية",
        "الرياض",
        "Saudi Arabia",
        "Saudi",
        "Riyadh",
    ],
    "الرياض": [
        "الرياض",
        "السعودية",
        "Riyadh",
        "Saudi Arabia",
    ],
    "الإمارات": [
        "الإمارات",
        "الإمارات العربية المتحدة",
        "أبوظبي",
        "دبي",
        "UAE",
        "Abu Dhabi",
        "Dubai",
    ],
    "قطر": [
        "قطر",
        "الدوحة",
        "Qatar",
        "Doha",
    ],
    "الكويت": [
        "الكويت",
        "Kuwait",
    ],
    "البحرين": [
        "البحرين",
        "Bahrain",
    ],
    "عمان": [
        "عمان",
        "مسقط",
        "Oman",
        "Muscat",
    ],
    "أمريكا": [
        "أمريكا",
        "الولايات المتحدة",
        "واشنطن",
        "USA",
        "United States",
        "Washington",
    ],
    "تركيا": [
        "تركيا",
        "تركيا",
        "أنقرة",
        "Türkiye",
        "Turkey",
        "Ankara",
    ],
    "إيران": [
        "إيران",
        "طهران",
        "Iran",
        "Tehran",
    ],
    "الصين": [
        "الصين",
        "بكين",
        "China",
        "Beijing",
    ],
    "بريطانيا": [
        "بريطانيا",
        "المملكة المتحدة",
        "لندن",
        "UK",
        "United Kingdom",
        "London",
    ],
    "إسرائيل": [
        "إسرائيل",
        "تل أبيب",
        "Israel",
        "Tel Aviv",
    ],
}


# ============================================================
# DATA MODEL
# ============================================================

@dataclass
class NewsItem:
    title: str
    url: str
    source: str
    country: str = ""
    summary: str = ""
    published: Optional[datetime] = None
    category: str = "world"
    importance: float = 0.0
    relevance: float = 0.0
    source_trust: float = 0.5
    is_breaking: bool = False
    event_id: str = ""


# ============================================================
# TEXT HELPERS
# ============================================================

def clean_text(value: str) -> str:
    if not value:
        return ""

    value = html.unescape(value)

    value = re.sub(
        r"<script.*?</script>",
        " ",
        value,
        flags=re.I | re.S,
    )

    value = re.sub(
        r"<style.*?</style>",
        " ",
        value,
        flags=re.I | re.S,
    )

    value = re.sub(r"<[^>]+>", " ", value)

    value = re.sub(r"\s+", " ", value)

    return value.strip()


def normalize_text(value: str) -> str:
    value = clean_text(value).lower()

    replacements = {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ة": "ه",
        "ى": "ي",
        "ؤ": "و",
        "ئ": "ي",
    }

    for old, new in replacements.items():
        value = value.replace(old, new)

    value = re.sub(r"[^\w\s\u0600-\u06ff-]", " ", value)
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def tokenize(value: str) -> set:
    return {
        token
        for token in normalize_text(value).split()
        if len(token) >= 2
    }


def make_event_id(title: str) -> str:
    normalized = normalize_text(title)

    words = sorted(
        word
        for word in normalized.split()
        if len(word) > 2
    )

    base = " ".join(words[:18])

    return hashlib.sha1(
        base.encode("utf-8")
    ).hexdigest()[:16]


def parse_date(value) -> Optional[datetime]:
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = parsedate_to_datetime(str(value))
        except Exception:
            dt = None

    if dt is None:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def recency_score(published: Optional[datetime]) -> float:
    if not published:
        return 0.25

    now = datetime.now(timezone.utc)

    age_hours = max(
        0,
        (now - published).total_seconds() / 3600,
    )

    if age_hours <= 1:
        return 1.00

    if age_hours <= 3:
        return 0.95

    if age_hours <= 6:
        return 0.90

    if age_hours <= 12:
        return 0.82

    if age_hours <= 24:
        return 0.72

    if age_hours <= 48:
        return 0.55

    if age_hours <= 72:
        return 0.40

    if age_hours <= 168:
        return 0.20

    return 0.05


# ============================================================
# QUERY EXPANSION
# ============================================================

def expand_query(query: str) -> List[str]:
    query = clean_text(query)

    if not query:
        return []

    normalized_query = normalize_text(query)

    terms = {query}

    for key, aliases in QUERY_ALIASES.items():
        if normalize_text(key) in normalized_query:
            terms.update(aliases)

    return list(terms)


# ============================================================
# CATEGORY DETECTION
# ============================================================

def detect_category(text: str) -> str:
    normalized = normalize_text(text)

    scores = {}

    for category, keywords in TOPIC_KEYWORDS.items():
        score = 0

        for keyword in keywords:
            if normalize_text(keyword) in normalized:
                score += 1

        scores[category] = score

    if not scores:
        return "world"

    best_category = max(
        scores,
        key=scores.get,
    )

    if scores[best_category] <= 0:
        return "world"

    return best_category


# ============================================================
# BREAKING DETECTION
# ============================================================

def detect_breaking(text: str) -> bool:
    normalized = normalize_text(text)

    return any(
        normalize_text(keyword) in normalized
        for keyword in BREAKING_KEYWORDS
    )


# ============================================================
# IMPORTANCE
# ============================================================

def calculate_importance(item: NewsItem) -> float:
    score = 0.0

    score += item.source_trust * 4.0
    score += recency_score(item.published) * 3.0

    if item.is_breaking:
        score += 3.0

    if item.category in {
        "urgent",
        "security",
        "foreign",
        "energy",
    }:
        score += 1.0

    return round(score, 3)


# ============================================================
# RELEVANCE
# ============================================================

def calculate_relevance(
    item: NewsItem,
    query: str,
) -> float:

    if not query:
        return 0.0

    expanded_terms = expand_query(query)

    if not expanded_terms:
        return 0.0

    document = normalize_text(
        f"{item.title} {item.summary} {item.source}"
    )

    document_tokens = tokenize(document)

    score = 0.0

    for term in expanded_terms:
        normalized_term = normalize_text(term)

        if not normalized_term:
            continue

        if normalized_term in document:
            score += 2.0

        term_tokens = tokenize(normalized_term)

        if term_tokens:
            overlap = len(
                term_tokens.intersection(
                    document_tokens
                )
            )

            score += overlap * 0.75

    # Normalize approximately to 0-10.
    return round(
        min(score, 10.0),
        3,
    )


# ============================================================
# RSS FETCH
# ============================================================

async def fetch_rss_source(
    session: aiohttp.ClientSession,
    source: SourceConfig,
) -> List[NewsItem]:

    logger.info(
        "Starting RSS source: %s",
        source.name,
    )

    items = []

    try:
        async with session.get(
            source.url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": (
                    "application/rss+xml, "
                    "application/xml, "
                    "text/xml, "
                    "text/html"
                ),
            },
            timeout=SOURCE_TIMEOUT,
        ) as response:

            if response.status != 200:
                logger.warning(
                    "RSS %s returned HTTP %s",
                    source.name,
                    response.status,
                )
                return []

            content = await read_limited_response(
                response,
                MAX_RSS_BYTES,
            )

        feed = feedparser.parse(content)

        for entry in feed.entries[
            :MAX_ITEMS_PER_SOURCE
        ]:

            title = clean_text(
                entry.get("title", "")
            )

            url = clean_text(
                entry.get("link", "")
            )

            if not title or not url:
                continue

            summary = clean_text(
                entry.get(
                    "summary",
                    entry.get(
                        "description",
                        "",
                    ),
                )
            )

            if len(summary) > MAX_SUMMARY_LENGTH:
                summary = (
                    summary[:MAX_SUMMARY_LENGTH]
                    + "..."
                )

            published = parse_date(
                entry.get("published")
                or entry.get("updated")
                or entry.get("created")
            )

            category = detect_category(
                f"{title} {summary}"
            )

            breaking = detect_breaking(
                f"{title} {summary}"
            )

            item = NewsItem(
                title=title,
                url=url,
                source=source.name,
                country=source.country,
                summary=summary,
                published=published,
                category=category,
                source_trust=source.trust,
                is_breaking=breaking,
            )

            item.event_id = make_event_id(
                item.title
            )

            item.importance = (
                calculate_importance(item)
            )

            items.append(item)

        logger.info(
            "RSS completed: %s -> %d items",
            source.name,
            len(items),
        )

        return items

    except asyncio.CancelledError:
        raise

    except Exception:
        logger.exception(
            "RSS failed: %s",
            source.name,
        )
        return []


# ============================================================
# LIMITED RESPONSE
# ============================================================

async def read_limited_response(
    response: aiohttp.ClientResponse,
    max_bytes: int,
) -> bytes:

    chunks = []
    total = 0

    async for chunk in response.content.iter_chunked(
        64 * 1024
    ):
        if not chunk:
            continue

        remaining = max_bytes - total

        if remaining <= 0:
            break

        if len(chunk) > remaining:
            chunk = chunk[:remaining]

        chunks.append(chunk)
        total += len(chunk)

        if total >= max_bytes:
            break

    return b"".join(chunks)


# ============================================================
# HTML LINK EXTRACTION
# ============================================================

def extract_links(
    base_url: str,
    content: str,
) -> List[tuple]:

    links = []

    pattern = re.compile(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>'
        r"(.*?)"
        r"</a>",
        flags=re.I | re.S,
    )

    for match in pattern.finditer(content):

        href = html.unescape(
            match.group(1)
        )

        anchor = clean_text(
            match.group(2)
        )

        if not href or not anchor:
            continue

        absolute_url = urljoin(
            base_url,
            href,
        )

        parsed = urlparse(
            absolute_url
        )

        if parsed.scheme not in {
            "http",
            "https",
        }:
            continue

        if len(anchor) < 15:
            continue

        links.append(
            (
                anchor,
                absolute_url,
            )
        )

    return links


def looks_like_news_url(url: str) -> bool:

    path = urlparse(url).path.lower()

    keywords = {
        "/news",
        "/statement",
        "/statements",
        "/press",
        "/media",
        "/article",
        "/articles",
        "/story",
        "/stories",
        "/latest",
        "/release",
        "/releases",
        "/detail",
        "/details",
    }

    return any(
        keyword in path
        for keyword in keywords
    )


# ============================================================
# HTML FETCH
# ============================================================

async def fetch_html_source(
    session: aiohttp.ClientSession,
    source: SourceConfig,
) -> List[NewsItem]:

    logger.info(
        "Starting HTML source: %s",
        source.name,
    )

    items = []

    try:
        async with session.get(
            source.url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": (
                    "text/html,"
                    "application/xhtml+xml"
                ),
            },
            timeout=SOURCE_TIMEOUT,
        ) as response:

            if response.status != 200:
                logger.warning(
                    "HTML %s returned HTTP %s",
                    source.name,
                    response.status,
                )
                return []

            raw = await read_limited_response(
                response,
                MAX_HTML_BYTES,
            )

            encoding = (
                response.charset
                or "utf-8"
            )

            content = raw.decode(
                encoding,
                errors="replace",
            )

        links = extract_links(
            source.url,
            content,
        )

        seen_urls = set()

        for title, url in links:

            if url in seen_urls:
                continue

            seen_urls.add(url)

            if not looks_like_news_url(url):
                continue

            if len(items) >= MAX_ITEMS_PER_SOURCE:
                break

            category = detect_category(
                title
            )

            breaking = detect_breaking(
                title
            )

            item = NewsItem(
                title=title,
                url=url,
                source=source.name,
                country=source.country,
                summary="",
                published=None,
                category=category,
                source_trust=source.trust,
                is_breaking=breaking,
            )

            item.event_id = make_event_id(
                title
            )

            item.importance = (
                calculate_importance(item)
            )

            items.append(item)

        logger.info(
            "HTML completed: %s -> %d items",
            source.name,
            len(items),
        )

        return items

    except asyncio.CancelledError:
        raise

    except Exception:
        logger.exception(
            "HTML failed: %s",
            source.name,
        )
        return []


# ============================================================
# SAFE SOURCE RUNNER
# ============================================================

async def run_source_safely(
    source_name: str,
    coroutine,
) -> List[NewsItem]:

    try:
        return await asyncio.wait_for(
            coroutine,
            timeout=SOURCE_TIMEOUT,
        )

    except asyncio.TimeoutError:
        logger.warning(
            "Source timeout: %s",
            source_name,
        )
        return []

    except asyncio.CancelledError:
        raise

    except Exception:
        logger.exception(
            "Source error: %s",
            source_name,
        )
        return []


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate_items(
    items: List[NewsItem],
) -> List[NewsItem]:

    result = []

    seen_urls = set()
    seen_hashes = set()

    for item in items:

        normalized_title = normalize_text(
            item.title
        )

        title_hash = hashlib.sha1(
            normalized_title.encode(
                "utf-8"
            )
        ).hexdigest()

        if item.url in seen_urls:
            continue

        if title_hash in seen_hashes:
            continue

        seen_urls.add(item.url)
        seen_hashes.add(title_hash)

        result.append(item)

    return result


# ============================================================
# EVENT CLUSTERING
# ============================================================

def cluster_events(
    items: List[NewsItem],
) -> List[NewsItem]:

    clusters = {}

    for item in items:

        tokens = tokenize(
            item.title
        )

        if not tokens:
            continue

        best_key = None
        best_overlap = 0

        for key, existing_tokens in clusters.items():

            overlap = len(
                tokens.intersection(
                    existing_tokens
                )
            )

            if overlap > best_overlap:
                best_overlap = overlap
                best_key = key

        if best_key is not None and best_overlap >= 3:

            # Keep the stronger item.
            existing = next(
                (
                    x
                    for x in items
                    if x.event_id == best_key
                ),
                None,
            )

            if existing:
                if item.importance > existing.importance:
                    existing.event_id = item.event_id

            continue

        clusters[item.event_id] = tokens

    return items


# ============================================================
# SORTING
# ============================================================

def sort_items(
    items: List[NewsItem],
) -> List[NewsItem]:

    for item in items:
        item.importance = (
            calculate_importance(item)
        )

    return sorted(
        items,
        key=lambda item: (
            item.importance,
            item.published.timestamp()
            if item.published
            else 0,
        ),
        reverse=True,
    )


# ============================================================
# INTERNAL COLLECTION
# ============================================================

async def _collect_news_internal(
    session: aiohttp.ClientSession,
) -> List[NewsItem]:

    tasks = []

    for source in RSS_SOURCES:

        tasks.append(
            asyncio.create_task(
                run_source_safely(
                    source.name,
                    fetch_rss_source(
                        session,
                        source,
                    ),
                )
            )
        )

    for source in HTML_SOURCES:

        tasks.append(
            asyncio.create_task(
                run_source_safely(
                    source.name,
                    fetch_html_source(
                        session,
                        source,
                    ),
                )
            )
        )

    logger.info(
        "Started %d news sources",
        len(tasks),
    )

    done, pending = await asyncio.wait(
        tasks,
        timeout=GLOBAL_COLLECTION_TIMEOUT,
        return_when=asyncio.ALL_COMPLETED,
    )

    logger.info(
        "News collection finished: done=%d pending=%d",
        len(done),
        len(pending),
    )

    for task in pending:
        task.cancel()

    if pending:
        await asyncio.gather(
            *pending,
            return_exceptions=True,
        )

    all_items = []

    for task in done:

        try:
            result = task.result()

            if result:
                all_items.extend(result)

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "News source task failed"
            )

    return all_items


# ============================================================
# PUBLIC COLLECTION FUNCTION
# ============================================================

async def collect_news(
    max_items: int = MAX_NEWS_ITEMS,
) -> List[NewsItem]:

    logger.info(
        "========== NEWS COLLECTION START =========="
    )

    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT,
        connect=3,
        sock_connect=3,
        sock_read=5,
    )

    connector = aiohttp.TCPConnector(
        limit=15,
        ssl=True,
        ttl_dns_cache=300,
    )

    all_items = []

    try:

        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
        ) as session:

            try:

                all_items = await asyncio.wait_for(
                    _collect_news_internal(
                        session
                    ),
                    timeout=GLOBAL_COLLECTION_TIMEOUT + 2,
                )

            except asyncio.TimeoutError:

                logger.warning(
                    "GLOBAL NEWS COLLECTION TIMEOUT"
                )

            except Exception:

                logger.exception(
                    "NEWS COLLECTION INTERNAL ERROR"
                )

    except Exception:

        logger.exception(
            "NEWS SESSION ERROR"
        )

    all_items = deduplicate_items(
        all_items
    )

    all_items = cluster_events(
        all_items
    )

    all_items = sort_items(
        all_items
    )

    result = all_items[:max_items]

    logger.info(
        "Final news items: %d",
        len(result),
    )

    logger.info(
        "========== NEWS COLLECTION END =========="
    )

    return result


# ============================================================
# SEARCH NEWS
# ============================================================

def search_news(
    items: List[NewsItem],
    query: str,
    max_results: int = MAX_SEARCH_RESULTS,
) -> List[NewsItem]:

    query = clean_text(query)

    if not query:
        return []

    logger.info(
        "Searching %d news items for query: %s",
        len(items),
        query,
    )

    scored = []

    for item in items:

        relevance = calculate_relevance(
            item,
            query,
        )

        item.relevance = relevance

        if relevance <= 0:
            continue

        # Final score:
        # relevance is dominant.
        final_score = (
            relevance * 2.5
            + item.source_trust * 2.0
            + recency_score(item.published) * 2.0
            + (
                1.5
                if item.is_breaking
                else 0
            )
        )

        scored.append(
            (
                final_score,
                item,
            )
        )

    scored.sort(
        key=lambda x: x[0],
        reverse=True,
    )

    results = [
        item
        for _, item in scored[
            :max_results
        ]
    ]

    logger.info(
        "Search selected %d relevant items",
        len(results),
    )

    return results


# ============================================================
# TOPIC SEARCH
# ============================================================

def search_topic(
    items: List[NewsItem],
    topic: str,
    max_results: int = MAX_SEARCH_RESULTS,
) -> List[NewsItem]:

    keywords = TOPIC_KEYWORDS.get(
        topic,
        set(),
    )

    if not keywords:
        return []

    scored = []

    for item in items:

        text = normalize_text(
            f"{item.title} {item.summary}"
        )

        matches = sum(
            1
            for keyword in keywords
            if normalize_text(keyword) in text
        )

        if matches <= 0:
            continue

        score = (
            matches * 2.0
            + item.source_trust * 2.0
            + recency_score(item.published) * 2.0
            + (
                2.0
                if item.is_breaking
                else 0
            )
        )

        scored.append(
            (
                score,
                item,
            )
        )

    scored.sort(
        key=lambda x: x[0],
        reverse=True,
    )

    return [
        item
        for _, item in scored[
            :max_results
        ]
    ]


# ============================================================
# AI CONTEXT
# ============================================================

def build_ai_context(
    items: List[NewsItem],
    max_items: int = 12,
) -> str:

    if not items:
        return (
            "لا توجد أخبار ذات صلة متاحة "
            "في المصادر التي تم فحصها."
        )

    selected = items[
        :max_items
    ]

    blocks = []

    for index, item in enumerate(
        selected,
        start=1,
    ):

        published = ""

        if item.published:
            published = (
                item.published
                .astimezone(timezone.utc)
                .strftime(
                    "%Y-%m-%d %H:%M UTC"
                )
            )

        breaking = (
            " | عاجل"
            if item.is_breaking
            else ""
        )

        block = (
            f"[{index}] "
            f"{item.title}\n"
            f"المصدر: {item.source}\n"
            f"الدولة: {item.country}\n"
            f"التاريخ: {published or 'غير محدد'}"
            f"{breaking}\n"
            f"الأهمية: {item.importance:.1f}\n"
            f"الصلة: {item.relevance:.1f}\n"
            f"الرابط: {item.url}"
        )

        if item.summary:
            block += (
                f"\nالملخص: {item.summary}"
            )

        blocks.append(block)

    return "\n\n".join(blocks)


# ============================================================
# TELEGRAM FORMAT
# ============================================================

def format_news_for_telegram(
    items: List[NewsItem],
    max_items: int = 10,
) -> str:

    if not items:
        return (
            "لا توجد أخبار ذات صلة حاليًا "
            "في المصادر التي تم فحصها."
        )

    lines = []

    for item in items[:max_items]:

        prefix = "🚨 " if item.is_breaking else "• "

        lines.append(
            f"{prefix}{item.title}\n"
            f"المصدر: {item.source}\n"
            f"{item.url}"
        )

    return "\n\n".join(lines)


# ============================================================
# RELEVANCE CHECK
# ============================================================

def has_relevant_news(
    items: List[NewsItem],
    query: str,
    minimum_score: float = 1.5,
) -> bool:

    results = search_news(
        items,
        query,
        max_results=1,
    )

    if not results:
        return False

    return (
        results[0].relevance
        >= minimum_score
    )


# ============================================================
# COLLECTION + QUERY HELPER
# ============================================================

async def collect_and_search(
    query: str,
    max_items: int = MAX_SEARCH_RESULTS,
) -> List[NewsItem]:

    items = await collect_news(
        max_items=MAX_NEWS_ITEMS
    )

    return search_news(
        items,
        query,
        max_results=max_items,
    )


# ============================================================
# DIAGNOSTICS
# ============================================================

def get_source_statistics(
    items: List[NewsItem],
) -> dict:

    stats = {}

    for item in items:

        if item.source not in stats:
            stats[item.source] = 0

        stats[item.source] += 1

    return stats


def get_category_statistics(
    items: List[NewsItem],
) -> dict:

    stats = {}

    for item in items:

        if item.category not in stats:
            stats[item.category] = 0

        stats[item.category] += 1

    return stats


# ============================================================
# TEST
# ============================================================

async def test_collection():

    items = await collect_news()

    print(
        f"Collected: {len(items)}"
    )

    print(
        get_source_statistics(items)
    )

    for item in items[:10]:

        print(
            "\n"
            f"{item.title}\n"
            f"{item.source}\n"
            f"{item.url}"
        )


if __name__ == "__main__":
    asyncio.run(
        test_collection()
    )
