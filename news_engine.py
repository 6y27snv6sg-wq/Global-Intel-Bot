import asyncio
import hashlib
import html
import logging
import re

from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

import aiohttp
import feedparser


# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger(__name__)


# ============================================================
# SETTINGS
# ============================================================

REQUEST_TIMEOUT = 8
SOURCE_TIMEOUT = 6
GLOBAL_COLLECTION_TIMEOUT = 18

MAX_ITEMS_PER_SOURCE = 20
MAX_NEWS_ITEMS = 180
MAX_SEARCH_RESULTS = 15

MAX_SUMMARY_LENGTH = 650

MAX_RSS_BYTES = 3 * 1024 * 1024
MAX_HTML_BYTES = 2 * 1024 * 1024

# Shared news cache.
NEWS_CACHE_TTL = 300  # 5 minutes

# Query result cache.
QUERY_CACHE_TTL = 90  # 1.5 minutes

# How long old cached news may be served during a refresh.
STALE_CACHE_TTL = 900  # 15 minutes

# Maximum simultaneous network source requests.
MAX_NETWORK_CONCURRENCY = 12

# Google News query feeds.
MAX_GOOGLE_QUERY_SOURCES = 10

# Keep AI context compact.
MAX_AI_CONTEXT_CHARS = 16000

USER_AGENT = (
    "Mozilla/5.0 (compatible; GlobalIntelBot/3.0; "
    "+https://www.google.com/bot.html)"
)


# ============================================================
# GLOBAL CONCURRENCY
# ============================================================

_network_semaphore = asyncio.Semaphore(
    MAX_NETWORK_CONCURRENCY
)

_collection_lock = asyncio.Lock()


# ============================================================
# CACHE
# ============================================================

@dataclass
class CacheEntry:
    value: object
    created_at: datetime


_news_cache: Optional[CacheEntry] = None

_query_cache: Dict[str, CacheEntry] = {}

_cache_lock = asyncio.Lock()


def _cache_age(entry: Optional[CacheEntry]) -> float:
    if entry is None:
        return 999999.0

    return (
        datetime.now(timezone.utc)
        - entry.created_at
    ).total_seconds()


def _query_cache_key(query: str) -> str:
    normalized = normalize_text(query)

    return hashlib.sha1(
        normalized.encode("utf-8")
    ).hexdigest()


# ============================================================
# SOURCE MODEL
# ============================================================

@dataclass(frozen=True)
class SourceConfig:
    name: str
    url: str
    source_type: str
    country: str
    trust: float
    language: str = "ar"
    region: str = "world"
    priority: int = 5


# ============================================================
# RSS SOURCES
# ============================================================

RSS_SOURCES = [

    # --------------------------------------------------------
    # Saudi Arabia
    # --------------------------------------------------------

    SourceConfig(
        name="SPA",
        url="https://www.spa.gov.sa/rss",
        source_type="official",
        country="Saudi Arabia",
        trust=1.00,
        language="ar",
        region="gulf",
        priority=1,
    ),

    # --------------------------------------------------------
    # Arabic / International
    # --------------------------------------------------------

    SourceConfig(
        name="France24 Arabic",
        url="https://www.france24.com/ar/rss",
        source_type="international",
        country="France",
        trust=0.88,
        language="ar",
        region="world",
        priority=3,
    ),

    SourceConfig(
        name="BBC Arabic",
        url="https://feeds.bbci.co.uk/arabic/rss.xml",
        source_type="international",
        country="United Kingdom",
        trust=0.90,
        language="ar",
        region="world",
        priority=3,
    ),

    # --------------------------------------------------------
    # Türkiye
    # --------------------------------------------------------

    SourceConfig(
        name="Türkiye MFA",
        url="https://www.mfa.gov.tr/rss.en.mfa",
        source_type="foreign_ministry",
        country="Türkiye",
        trust=1.00,
        language="en",
        region="asia",
        priority=1,
    ),

    SourceConfig(
        name="Türkiye MFA Arabic",
        url="https://www.mfa.gov.tr/rss.ar.mfa",
        source_type="foreign_ministry",
        country="Türkiye",
        trust=1.00,
        language="ar",
        region="asia",
        priority=1,
    ),
]


# ============================================================
# OFFICIAL HTML SOURCES
# ============================================================

HTML_SOURCES = [

    # --------------------------------------------------------
    # Saudi Arabia
    # --------------------------------------------------------

    SourceConfig(
        name="Saudi MOFA News",
        url="https://www.mofa.gov.sa/en/ministry/news",
        source_type="foreign_ministry",
        country="Saudi Arabia",
        trust=1.00,
        language="en",
        region="gulf",
        priority=1,
    ),

    SourceConfig(
        name="Saudi MOFA Statements",
        url="https://www.mofa.gov.sa/en/ministry/statements",
        source_type="foreign_ministry",
        country="Saudi Arabia",
        trust=1.00,
        language="en",
        region="gulf",
        priority=1,
    ),

    # --------------------------------------------------------
    # Qatar
    # --------------------------------------------------------

    SourceConfig(
        name="Qatar MOFA News",
        url="https://mofa.gov.qa/en/latest-articles",
        source_type="foreign_ministry",
        country="Qatar",
        trust=1.00,
        language="en",
        region="gulf",
        priority=1,
    ),

    SourceConfig(
        name="Qatar MOFA Statements",
        url="https://mofa.gov.qa/en/latest-articles/statements",
        source_type="foreign_ministry",
        country="Qatar",
        trust=1.00,
        language="en",
        region="gulf",
        priority=1,
    ),

    # --------------------------------------------------------
    # UAE
    # --------------------------------------------------------

    SourceConfig(
        name="UAE MOFA MediaHub",
        url="https://www.mofa.gov.ae/en/mediahub/news",
        source_type="foreign_ministry",
        country="UAE",
        trust=1.00,
        language="en",
        region="gulf",
        priority=1,
    ),

    # --------------------------------------------------------
    # Kuwait
    # --------------------------------------------------------

    SourceConfig(
        name="Kuwait MFA",
        url="https://www.mofa.gov.kw/",
        source_type="foreign_ministry",
        country="Kuwait",
        trust=1.00,
        language="ar",
        region="gulf",
        priority=1,
    ),

    # --------------------------------------------------------
    # Egypt
    # --------------------------------------------------------

    SourceConfig(
        name="Egypt MFA",
        url="https://www.mfa.gov.eg/en/Ministry/News",
        source_type="foreign_ministry",
        country="Egypt",
        trust=1.00,
        language="en",
        region="middle_east",
        priority=1,
    ),

    # --------------------------------------------------------
    # United Kingdom
    # --------------------------------------------------------

    SourceConfig(
        name="UK FCDO",
        url="https://www.gov.uk/search/news-and-communications",
        source_type="foreign_ministry",
        country="United Kingdom",
        trust=0.98,
        language="en",
        region="europe",
        priority=1,
    ),

    # --------------------------------------------------------
    # China
    # --------------------------------------------------------

    SourceConfig(
        name="China MFA",
        url="https://www.mfa.gov.cn/eng/xw/zyxw/index.html",
        source_type="foreign_ministry",
        country="China",
        trust=0.98,
        language="en",
        region="asia",
        priority=1,
    ),
]


# ============================================================
# GOOGLE NEWS AGGREGATOR SOURCES
# ============================================================

GOOGLE_NEWS_QUERIES = [

    # Arabic global radar
    ("Google News Arabic", "أهم الأخبار OR عاجل"),

    # Saudi
    ("Google News Saudi", "السعودية OR الرياض"),

    # Gulf
    ("Google News Gulf", "الخليج OR السعودية OR الإمارات OR قطر OR الكويت"),

    # Middle East
    ("Google News Middle East", "الشرق الأوسط OR إيران OR إسرائيل OR غزة"),

    # Asia
    ("Google News Asia", "الصين OR اليابان OR الهند OR كوريا OR تايوان"),

    # Russia / Eurasia
    ("Google News Russia", "روسيا OR أوكرانيا OR موسكو OR كييف"),

    # Europe
    ("Google News Europe", "أوروبا OR بريطانيا OR فرنسا OR ألمانيا"),

    # America
    ("Google News America", "أمريكا OR الولايات المتحدة OR واشنطن"),

    # Energy
    ("Google News Energy", "النفط OR الغاز OR أوبك OR الطاقة"),

    # Breaking
    ("Google News Breaking", "عاجل OR هجوم OR انفجار OR صاروخ OR زلزال"),
]


# ============================================================
# GOOGLE NEWS DOMAIN RADAR
# ============================================================

GOOGLE_NEWS_DOMAIN_QUERIES = [

    # Arabic TV / major media
    ("Google News Al Arabiya", "site:alarabiya.net"),
    ("Google News Al Hadath", "site:alarabiya.net"),
    ("Google News Al Jazeera", "site:aljazeera.net"),
    ("Google News Sky Arabia", "site:skynewsarabia.com"),

    # International TV / agencies
    ("Google News Reuters", "site:reuters.com"),
    ("Google News BBC", "site:bbc.com"),
    ("Google News France24", "site:france24.com"),
    ("Google News Anadolu", "site:aa.com.tr"),

    # Asia
    ("Google News China", "site:scmp.com OR site:globaltimes.cn"),
    ("Google News India", "site:thehindu.com OR site:hindustantimes.com"),

    # Russia / Eurasia
    ("Google News Russia International", "site:tass.com OR site:rt.com"),
]


# ============================================================
# KEYWORDS
# ============================================================

BREAKING_KEYWORDS = {
    "عاجل",
    "عاجلة",
    "خبر عاجل",
    "بيان عاجل",
    "طارئ",
    "طوارئ",
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
    "زلزال",
    "تسونامي",
    "إخلاء",
    "تحذير",
    "إغلاق",
    "حظر",
    "عقوبات",
    "اندلاع",
    "حرب",
    "ضربة",
    "ضربات",
    "war",
    "attack",
    "missile",
    "strike",
    "breaking",
    "emergency",
    "explosion",
    "earthquake",
    "tsunami",
}


TOPIC_KEYWORDS = {

    "urgent": {
        "عاجل",
        "عاجلة",
        "طارئ",
        "هجوم",
        "قصف",
        "صاروخ",
        "صواريخ",
        "مسيرة",
        "انفجار",
        "اشتباك",
        "اغتيال",
        "تحذير",
        "طوارئ",
        "زلزال",
        "تسونامي",
        "breaking",
        "attack",
        "missile",
        "strike",
        "emergency",
    },

    "world": {
        "العالم",
        "دولي",
        "دولية",
        "الأمم المتحدة",
        "مجلس الأمن",
        "أمريكا",
        "أوروبا",
        "روسيا",
        "الصين",
        "إيران",
        "إسرائيل",
        "أوكرانيا",
        "غزة",
        "united nations",
        "security council",
    },

    "gulf": {
        "الخليج",
        "مجلس التعاون",
        "السعودية",
        "الإمارات",
        "قطر",
        "الكويت",
        "البحرين",
        "عمان",
        "gulf",
        "gcc",
        "saudi",
        "uae",
        "qatar",
        "kuwait",
        "bahrain",
        "oman",
    },

    "america": {
        "أمريكا",
        "الولايات المتحدة",
        "واشنطن",
        "ترامب",
        "البيت الأبيض",
        "الكونغرس",
        "united states",
        "washington",
        "trump",
        "white house",
        "congress",
    },

    "europe": {
        "أوروبا",
        "الاتحاد الأوروبي",
        "بريطانيا",
        "فرنسا",
        "ألمانيا",
        "إيطاليا",
        "بروكسل",
        "europe",
        "european union",
        "uk",
        "france",
        "germany",
        "italy",
        "brussels",
    },

    "asia": {
        "آسيا",
        "الصين",
        "اليابان",
        "الهند",
        "كوريا",
        "كوريا الجنوبية",
        "باكستان",
        "تركيا",
        "تايوان",
        "إندونيسيا",
        "ماليزيا",
        "asia",
        "china",
        "japan",
        "india",
        "korea",
        "pakistan",
        "turkey",
        "taiwan",
        "indonesia",
        "malaysia",
    },

    "russia": {
        "روسيا",
        "موسكو",
        "بوتين",
        "أوكرانيا",
        "كييف",
        "بيلاروسيا",
        "القوقاز",
        "آسيا الوسطى",
        "russia",
        "moscow",
        "putin",
        "ukraine",
        "kyiv",
        "belarus",
        "caucasus",
    },

    "energy": {
        "النفط",
        "الغاز",
        "أوبك",
        "أوبك+",
        "طاقة",
        "أسعار النفط",
        "برميل",
        "النفط الخام",
        "oil",
        "gas",
        "opec",
        "energy",
        "crude",
        "barrel",
    },

    "security": {
        "أمن",
        "أمني",
        "دفاع",
        "دفاعي",
        "عسكري",
        "جيش",
        "أسلحة",
        "صواريخ",
        "طائرات",
        "دفاع جوي",
        "حلف",
        "تحالف",
        "security",
        "defense",
        "military",
        "missile",
        "air defense",
        "alliance",
    },

    "foreign": {
        "الخارجية",
        "وزارة الخارجية",
        "وزير الخارجية",
        "سفير",
        "سفارة",
        "بيان",
        "تصريح",
        "اجتماع",
        "مباحثات",
        "محادثات",
        "اتصال",
        "foreign ministry",
        "foreign minister",
        "embassy",
        "statement",
        "diplomatic",
        "talks",
    },
}


# ============================================================
# QUERY ALIASES
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

    "اليابان": [
        "اليابان",
        "طوكيو",
        "Japan",
        "Tokyo",
    ],

    "الهند": [
        "الهند",
        "نيودلهي",
        "India",
        "New Delhi",
    ],

    "روسيا": [
        "روسيا",
        "موسكو",
        "Russia",
        "Moscow",
        "Putin",
        "بوتين",
    ],

    "أوكرانيا": [
        "أوكرانيا",
        "كييف",
        "Ukraine",
        "Kyiv",
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

    "الصين": [
        "الصين",
        "بكين",
        "China",
        "Beijing",
    ],

    "كوريا": [
        "كوريا",
        "كوريا الجنوبية",
        "سيول",
        "South Korea",
        "Seoul",
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

    region: str = "world"

    importance: float = 0.0

    relevance: float = 0.0

    source_trust: float = 0.5

    source_type: str = "international"

    is_breaking: bool = False

    event_id: str = ""

    corroboration_count: int = 1

    last_updated: Optional[datetime] = None

    metadata: Dict = field(
        default_factory=dict
    )


# ============================================================
# TEXT HELPERS
# ============================================================

def clean_text(value: str) -> str:

    if not value:
        return ""

    value = html.unescape(
        str(value)
    )

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

    value = re.sub(
        r"<[^>]+>",
        " ",
        value,
    )

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    return value.strip()


def normalize_text(value: str) -> str:

    value = clean_text(
        value
    ).lower()

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
        value = value.replace(
            old,
            new,
        )

    value = re.sub(
        r"[^\w\s\u0600-\u06ff-]",
        " ",
        value,
    )

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    return value.strip()


def tokenize(value: str) -> set:

    return {
        token
        for token in normalize_text(
            value
        ).split()
        if len(token) >= 2
    }


def normalize_url(url: str) -> str:

    if not url:
        return ""

    parsed = urlparse(
        url.strip()
    )

    if not parsed.scheme:
        return url.strip()

    return (
        f"{parsed.scheme.lower()}://"
        f"{parsed.netloc.lower()}"
        f"{parsed.path}"
    )


def make_event_id(title: str) -> str:

    normalized = normalize_text(
        title
    )

    words = [
        word
        for word in normalized.split()
        if len(word) > 2
    ]

    base = " ".join(
        sorted(words)[:20]
    )

    if not base:
        base = normalized

    return hashlib.sha1(
        base.encode("utf-8")
    ).hexdigest()[:16]


# ============================================================
# DATE PARSING
# ============================================================

def parse_date(value) -> Optional[datetime]:

    if not value:
        return None

    if isinstance(
        value,
        datetime,
    ):
        dt = value

    else:

        try:
            dt = parsedate_to_datetime(
                str(value)
            )

        except Exception:

            dt = None

            text = str(value)

            iso_match = re.search(
                r"\d{4}-\d{2}-\d{2}"
                r"(?:[T ]\d{2}:\d{2}"
                r"(?::\d{2})?)?",
                text,
            )

            if iso_match:

                try:
                    iso_value = (
                        iso_match.group(0)
                        .replace(
                            " ",
                            "T",
                        )
                    )

                    dt = datetime.fromisoformat(
                        iso_value
                    )

                except Exception:
                    dt = None

    if dt is None:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(
            tzinfo=timezone.utc
        )

    return dt.astimezone(
        timezone.utc
    )


# ============================================================
# RECENCY
# ============================================================

def recency_score(
    published: Optional[datetime],
) -> float:

    if not published:
        return 0.20

    now = datetime.now(
        timezone.utc
    )

    age_hours = max(
        0,
        (
            now - published
        ).total_seconds()
        / 3600,
    )

    if age_hours <= 1:
        return 1.00

    if age_hours <= 3:
        return 0.96

    if age_hours <= 6:
        return 0.91

    if age_hours <= 12:
        return 0.84

    if age_hours <= 24:
        return 0.74

    if age_hours <= 48:
        return 0.56

    if age_hours <= 72:
        return 0.40

    if age_hours <= 168:
        return 0.20

    return 0.05


# ============================================================
# QUERY EXPANSION
# ============================================================

def expand_query(
    query: str,
) -> List[str]:

    query = clean_text(
        query
    )

    if not query:
        return []

    normalized_query = normalize_text(
        query
    )

    terms = {
        query
    }

    for key, aliases in QUERY_ALIASES.items():

        if normalize_text(
            key
        ) in normalized_query:

            terms.update(
                aliases
            )

    return list(
        terms
    )


# ============================================================
# CATEGORY
# ============================================================

def detect_category(
    text: str,
) -> str:

    normalized = normalize_text(
        text
    )

    scores = {}

    for category, keywords in TOPIC_KEYWORDS.items():

        score = 0

        for keyword in keywords:

            normalized_keyword = (
                normalize_text(
                    keyword
                )
            )

            if (
                normalized_keyword
                and normalized_keyword
                in normalized
            ):
                score += 1

        scores[
            category
        ] = score

    if not scores:
        return "world"

    best_category = max(
        scores,
        key=scores.get,
    )

    if scores[
        best_category
    ] <= 0:

        return "world"

    return best_category


# ============================================================
# REGION
# ============================================================

def detect_region(
    text: str,
) -> str:

    normalized = normalize_text(
        text
    )

    region_keywords = {

        "gulf": {
            "السعودية",
            "الإمارات",
            "قطر",
            "الكويت",
            "البحرين",
            "عمان",
            "الخليج",
            "gcc",
            "gulf",
        },

        "middle_east": {
            "الشرق الأوسط",
            "إيران",
            "العراق",
            "سوريا",
            "لبنان",
            "الأردن",
            "فلسطين",
            "إسرائيل",
            "اليمن",
            "غزة",
        },

        "asia": {
            "الصين",
            "اليابان",
            "الهند",
            "كوريا",
            "تايوان",
            "باكستان",
            "إندونيسيا",
            "ماليزيا",
            "آسيا",
        },

        "russia": {
            "روسيا",
            "موسكو",
            "أوكرانيا",
            "كييف",
            "بيلاروسيا",
            "القوقاز",
            "آسيا الوسطى",
        },

        "europe": {
            "أوروبا",
            "بريطانيا",
            "فرنسا",
            "ألمانيا",
            "إيطاليا",
            "إسبانيا",
            "بروكسل",
        },

        "america": {
            "أمريكا",
            "الولايات المتحدة",
            "كندا",
            "واشنطن",
            "ترامب",
        },
    }

    scores = {}

    for region, keywords in region_keywords.items():

        score = sum(
            1
            for keyword in keywords
            if normalize_text(
                keyword
            ) in normalized
        )

        scores[
            region
        ] = score

    if not scores:
        return "world"

    best = max(
        scores,
        key=scores.get,
    )

    if scores[
        best
    ] <= 0:

        return "world"

    return best


# ============================================================
# BREAKING
# ============================================================

def detect_breaking(
    text: str,
) -> bool:

    normalized = normalize_text(
        text
    )

    return any(
        normalize_text(
            keyword
        ) in normalized
        for keyword
        in BREAKING_KEYWORDS
    )


# ============================================================
# IMPORTANCE
# ============================================================

def calculate_importance(
    item: NewsItem,
) -> float:

    score = 0.0

    score += (
        item.source_trust
        * 4.0
    )

    score += (
        recency_score(
            item.published
        )
        * 3.0
    )

    if item.is_breaking:
        score += 3.5

    if item.category in {
        "urgent",
        "security",
        "foreign",
        "energy",
    }:
        score += 1.0

    if item.corroboration_count >= 2:
        score += min(
            item.corroboration_count
            * 0.5,
            2.0,
        )

    return round(
        score,
        3,
    )


# ============================================================
# RELEVANCE
# ============================================================

def calculate_relevance(
    item: NewsItem,
    query: str,
) -> float:

    if not query:
        return 0.0

    expanded_terms = expand_query(
        query
    )

    if not expanded_terms:
        return 0.0

    document = normalize_text(
        " ".join(
            [
                item.title,
                item.summary,
                item.source,
                item.country,
                item.region,
                item.category,
            ]
        )
    )

    document_tokens = tokenize(
        document
    )

    score = 0.0

    for term in expanded_terms:

        normalized_term = (
            normalize_text(
                term
            )
        )

        if not normalized_term:
            continue

        if (
            normalized_term
            in document
        ):
            score += 2.0

        term_tokens = tokenize(
            normalized_term
        )

        if term_tokens:

            overlap = len(
                term_tokens.intersection(
                    document_tokens
                )
            )

            score += (
                overlap * 0.75
            )

    if item.is_breaking:
        score += 0.5

    score += (
        recency_score(
            item.published
        )
        * 0.5
    )

    return round(
        min(
            score,
            10.0,
        ),
        3,
    )


# ============================================================
# SOURCE ITEM BUILDER
# ============================================================

def build_news_item(
    source: SourceConfig,
    title: str,
    url: str,
    summary: str = "",
    published=None,
) -> Optional[NewsItem]:

    title = clean_text(
        title
    )

    url = normalize_url(
        clean_text(url)
    )

    summary = clean_text(
        summary
    )

    if not title or not url:
        return None

    if len(title) < 8:
        return None

    if len(summary) > MAX_SUMMARY_LENGTH:

        summary = (
            summary[
                :MAX_SUMMARY_LENGTH
            ]
            + "..."
        )

    combined = (
        f"{title} {summary}"
    )

    item = NewsItem(
        title=title,
        url=url,
        source=source.name,
        country=source.country,
        summary=summary,
        published=parse_date(
            published
        ),
        category=detect_category(
            combined
        ),
        region=(
            source.region
            if source.region != "world"
            else detect_region(
                combined
            )
        ),
        source_trust=source.trust,
        source_type=source.source_type,
        is_breaking=detect_breaking(
            combined
        ),
    )

    item.event_id = make_event_id(
        item.title
    )

    item.importance = (
        calculate_importance(
            item
        )
    )

    return item


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

        remaining = (
            max_bytes - total
        )

        if remaining <= 0:
            break

        if len(chunk) > remaining:
            chunk = chunk[
                :remaining
            ]

        chunks.append(
            chunk
        )

        total += len(
            chunk
        )

        if total >= max_bytes:
            break

    return b"".join(
        chunks
    )


# ============================================================
# NETWORK REQUEST
# ============================================================

async def _get(
    session: aiohttp.ClientSession,
    url: str,
    max_bytes: int,
) -> Optional[Tuple[bytes, str]]:

    try:

        async with _network_semaphore:

            async with session.get(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": (
                        "application/rss+xml,"
                        "application/xml,"
                        "text/xml,"
                        "application/xhtml+xml,"
                        "text/html"
                    ),
                },
                timeout=SOURCE_TIMEOUT,
                allow_redirects=True,
            ) as response:

                if response.status != 200:

                    logger.warning(
                        "HTTP %s for %s",
                        response.status,
                        url,
                    )

                    return None

                raw = await read_limited_response(
                    response,
                    max_bytes,
                )

                encoding = (
                    response.charset
                    or "utf-8"
                )

                return (
                    raw,
                    encoding,
                )

    except asyncio.CancelledError:
        raise

    except Exception as exc:

        logger.warning(
            "Request failed for %s: %s",
            url,
            exc,
        )

        return None


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

    result = []

    try:

        response = await _get(
            session,
            source.url,
            MAX_RSS_BYTES,
        )

        if not response:
            return []

        raw, _ = response

        feed = feedparser.parse(
            raw
        )

        for entry in feed.entries[
            :MAX_ITEMS_PER_SOURCE
        ]:

            item = build_news_item(
                source=source,
                title=entry.get(
                    "title",
                    "",
                ),
                url=entry.get(
                    "link",
                    "",
                ),
                summary=entry.get(
                    "summary",
                    entry.get(
                        "description",
                        "",
                    ),
                ),
                published=(
                    entry.get(
                        "published"
                    )
                    or entry.get(
                        "updated"
                    )
                    or entry.get(
                        "created"
                    )
                ),
            )

            if item:
                result.append(
                    item
                )

        logger.info(
            "RSS completed: %s -> %d",
            source.name,
            len(result),
        )

        return result

    except asyncio.CancelledError:
        raise

    except Exception:

        logger.exception(
            "RSS failed: %s",
            source.name,
        )

        return []


# ============================================================
# HTML LINK EXTRACTION
# ============================================================

def extract_links(
    base_url: str,
    content: str,
) -> List[Tuple[str, str, str]]:

    links = []

    pattern = re.compile(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>'
        r"(.*?)"
        r"</a>",
        flags=re.I | re.S,
    )

    for match in pattern.finditer(
        content
    ):

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

        # Try to capture a nearby date.
        start = max(
            0,
            match.start() - 700,
        )

        end = min(
            len(content),
            match.end() + 700,
        )

        surrounding = clean_text(
            content[
                start:end
            ]
        )

        date_match = re.search(
            r"\b"
            r"(20\d{2})[-/]"
            r"(0?[1-9]|1[0-2])[-/]"
            r"(0?[1-9]|[12]\d|3[01])"
            r"\b",
            surrounding,
        )

        published = (
            date_match.group(0)
            if date_match
            else None
        )

        links.append(
            (
                anchor,
                absolute_url,
                published or "",
            )
        )

    return links


# ============================================================
# NEWS URL FILTER
# ============================================================

def looks_like_news_url(
    url: str,
) -> bool:

    parsed = urlparse(
        url
    )

    path = parsed.path.lower()

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
        "/xw/",
        "/zyxw/",
        "/search/",
    }

    if any(
        keyword in path
        for keyword in keywords
    ):
        return True

    # Many modern news sites use
    # date-based URLs.
    if re.search(
        r"/20\d{2}/"
        r"(0?[1-9]|1[0-2])/"
        r"(0?[1-9]|[12]\d|3[01])",
        path,
    ):
        return True

    return False


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

    result = []

    try:

        response = await _get(
            session,
            source.url,
            MAX_HTML_BYTES,
        )

        if not response:
            return []

        raw, encoding = response

        content = raw.decode(
            encoding,
            errors="replace",
        )

        links = extract_links(
            source.url,
            content,
        )

        seen_urls = set()

        for (
            title,
            url,
            published,
        ) in links:

            normalized_url = (
                normalize_url(
                    url
                )
            )

            if (
                normalized_url
                in seen_urls
            ):
                continue

            if not looks_like_news_url(
                normalized_url
            ):
                continue

            seen_urls.add(
                normalized_url
            )

            item = build_news_item(
                source=source,
                title=title,
                url=normalized_url,
                published=published,
            )

            if not item:
                continue

            result.append(
                item
            )

            if len(result) >= (
                MAX_ITEMS_PER_SOURCE
            ):
                break

        logger.info(
            "HTML completed: %s -> %d",
            source.name,
            len(result),
        )

        return result

    except asyncio.CancelledError:
        raise

    except Exception:

        logger.exception(
            "HTML failed: %s",
            source.name,
        )

        return []


# ============================================================
# GOOGLE NEWS RSS
# ============================================================

def google_news_url(
    query: str,
    language: str = "ar",
    country: str = "SA",
) -> str:

    encoded = quote(
        query
    )

    return (
        "https://news.google.com/rss/search"
        f"?q={encoded}"
        f"&hl={language}"
        f"&gl={country}"
        f"&ceid={country}:{language}"
    )


async def fetch_google_news_query(
    session: aiohttp.ClientSession,
    name: str,
    query: str,
) -> List[NewsItem]:

    source = SourceConfig(
        name=name,
        url=google_news_url(
            query
        ),
        source_type="aggregator",
        country="International",
        trust=0.82,
        language="ar",
        region=detect_region(
            query
        ),
        priority=4,
    )

    return await fetch_rss_source(
        session,
        source,
    )


async def fetch_google_news_radar(
    session: aiohttp.ClientSession,
) -> List[NewsItem]:

    tasks = []

    for name, query in (
        GOOGLE_NEWS_QUERIES
        + GOOGLE_NEWS_DOMAIN_QUERIES
    )[
        :MAX_GOOGLE_QUERY_SOURCES
    ]:

        tasks.append(
            asyncio.create_task(
                run_source_safely(
                    name,
                    fetch_google_news_query(
                        session,
                        name,
                        query,
                    ),
                )
            )
        )

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )

    items = []

    for result in results:

        if isinstance(
            result,
            Exception,
        ):
            continue

        if result:
            items.extend(
                result
            )

    return items


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

        url_key = normalize_url(
            item.url
        )

        title_key = normalize_text(
            item.title
        )

        title_hash = hashlib.sha1(
            title_key.encode(
                "utf-8"
            )
        ).hexdigest()

        if url_key in seen_urls:
            continue

        if title_hash in seen_hashes:
            continue

        seen_urls.add(
            url_key
        )

        seen_hashes.add(
            title_hash
        )

        result.append(
            item
        )

    return result


# ============================================================
# EVENT CLUSTERING
# ============================================================

def cluster_events(
    items: List[NewsItem],
) -> List[NewsItem]:

    if not items:
        return []

    clusters: List[
        Tuple[set, NewsItem]
    ] = []

    for item in items:

        tokens = tokenize(
            item.title
        )

        if not tokens:
            continue

        best_cluster = None
        best_overlap = 0

        for cluster in clusters:

            cluster_tokens = (
                cluster[0]
            )

            overlap = len(
                tokens.intersection(
                    cluster_tokens
                )
            )

            if overlap > best_overlap:
                best_overlap = overlap
                best_cluster = cluster

        if (
            best_cluster is not None
            and best_overlap >= 3
        ):

            best_cluster[0].update(
                tokens
            )

            representative = (
                best_cluster[1]
            )

            representative.corroboration_count += 1

            # Prefer the stronger source.
            if (
                item.importance
                > representative.importance
            ):

                old_event_id = (
                    representative.event_id
                )

                item.event_id = (
                    old_event_id
                    or item.event_id
                )

                best_cluster = (
                    best_cluster[0],
                    item,
                )

            continue

        clusters.append(
            (
                set(tokens),
                item,
            )
        )

    result = []

    for _, representative in clusters:

        representative.importance = (
            calculate_importance(
                representative
            )
        )

        result.append(
            representative
        )

    # Preserve any isolated items
    # that were skipped due to no tokens.
    known = {
        id(item)
        for item in result
    }

    for item in items:

        if id(item) not in known:
            result.append(
                item
            )

    return result


# ============================================================
# SORTING
# ============================================================

def sort_items(
    items: List[NewsItem],
) -> List[NewsItem]:

    for item in items:

        item.importance = (
            calculate_importance(
                item
            )
        )

    return sorted(
        items,
        key=lambda item: (
            item.is_breaking,
            item.importance,
            item.published.timestamp()
            if item.published
            else 0,
            item.source_trust,
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

    # Official / direct RSS.
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

    # Official HTML.
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

    # Google News radar.
    tasks.append(
        asyncio.create_task(
            run_source_safely(
                "Google News Radar",
                fetch_google_news_radar(
                    session
                ),
            )
        )
    )

    logger.info(
        "Started %d news collection tasks",
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
                all_items.extend(
                    result
                )

        except asyncio.CancelledError:
            raise

        except Exception:

            logger.exception(
                "Collection task failed"
            )

    return all_items


# ============================================================
# REAL COLLECTION
# ============================================================

async def _perform_collection() -> List[NewsItem]:

    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT,
        connect=3,
        sock_connect=3,
        sock_read=6,
    )

    connector = aiohttp.TCPConnector(
        limit=MAX_NETWORK_CONCURRENCY,
        ssl=True,
        ttl_dns_cache=300,
    )

    try:

        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
        ) as session:

            items = await asyncio.wait_for(
                _collect_news_internal(
                    session
                ),
                timeout=(
                    GLOBAL_COLLECTION_TIMEOUT
                    + 2
                ),
            )

            return items

    except asyncio.TimeoutError:

        logger.warning(
            "GLOBAL NEWS COLLECTION TIMEOUT"
        )

        return []

    except Exception:

        logger.exception(
            "NEWS COLLECTION INTERNAL ERROR"
        )

        return []


# ============================================================
# PUBLIC COLLECTION
# ============================================================

async def collect_news(
    max_items: int = MAX_NEWS_ITEMS,
) -> List[NewsItem]:

    global _news_cache

    logger.info(
        "========== NEWS COLLECTION START =========="
    )

    # --------------------------------------------------------
    # Fast cache.
    # --------------------------------------------------------

    async with _cache_lock:

        cached = _news_cache

        if (
            cached is not None
            and _cache_age(cached)
            <= NEWS_CACHE_TTL
        ):

            logger.info(
                "NEWS CACHE HIT age=%.1fs",
                _cache_age(cached),
            )

            items = list(
                cached.value
            )

            return items[
                :max_items
            ]

    # --------------------------------------------------------
    # Only ONE process/task refreshes the cache.
    # --------------------------------------------------------

    async with _collection_lock:

        # Another user may have refreshed
        # it while we were waiting.
        async with _cache_lock:

            cached = _news_cache

            if (
                cached is not None
                and _cache_age(cached)
                <= NEWS_CACHE_TTL
            ):

                items = list(
                    cached.value
                )

                return items[
                    :max_items
                ]

        logger.info(
            "NEWS CACHE MISS -> refreshing"
        )

        all_items = await _perform_collection()

        all_items = deduplicate_items(
            all_items
        )

        all_items = cluster_events(
            all_items
        )

        all_items = sort_items(
            all_items
        )

        # ----------------------------------------------------
        # Successful refresh.
        # ----------------------------------------------------

        if all_items:

            async with _cache_lock:

                _news_cache = CacheEntry(
                    value=list(
                        all_items
                    ),
                    created_at=(
                        datetime.now(
                            timezone.utc
                        )
                    ),
                )

            logger.info(
                "News cache refreshed: %d items",
                len(all_items),
            )

        else:

            # ------------------------------------------------
            # Fail-soft: return stale data.
            # ------------------------------------------------

            async with _cache_lock:

                cached = _news_cache

                if (
                    cached is not None
                    and _cache_age(cached)
                    <= STALE_CACHE_TTL
                ):

                    logger.warning(
                        "Collection failed. "
                        "Serving stale cache age=%.1fs",
                        _cache_age(
                            cached
                        ),
                    )

                    all_items = list(
                        cached.value
                    )

    result = all_items[
        :max_items
    ]

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

    query = clean_text(
        query
    )

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

        item.relevance = (
            relevance
        )

        if relevance <= 0:
            continue

        final_score = (
            relevance * 2.8
            + item.source_trust * 2.0
            + recency_score(
                item.published
            ) * 2.2
            + (
                2.0
                if item.is_breaking
                else 0
            )
            + min(
                item.corroboration_count
                * 0.35,
                1.5,
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

    topic = (
        topic.strip().lower()
    )

    keywords = TOPIC_KEYWORDS.get(
        topic,
        set(),
    )

    # Russia is a separate topic.
    if topic == "russia":
        keywords = (
            TOPIC_KEYWORDS["russia"]
        )

    if not keywords:
        return []

    scored = []

    for item in items:

        text = normalize_text(
            f"{item.title} "
            f"{item.summary}"
        )

        matches = sum(
            1
            for keyword in keywords
            if normalize_text(
                keyword
            ) in text
        )

        if matches <= 0:
            continue

        score = (
            matches * 2.0
            + item.source_trust * 2.0
            + recency_score(
                item.published
            ) * 2.0
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
    max_items: int = 10,
) -> str:

    if not items:

        return (
            "لا توجد أخبار ذات صلة "
            "متاحة في المصادر التي تم فحصها."
        )

    selected = items[
        :max_items
    ]

    blocks = []

    current_chars = 0

    for index, item in enumerate(
        selected,
        start=1,
    ):

        published = ""

        if item.published:

            published = (
                item.published
                .astimezone(
                    timezone.utc
                )
                .strftime(
                    "%Y-%m-%d %H:%M UTC"
                )
            )

        flags = []

        if item.is_breaking:
            flags.append(
                "عاجل"
            )

        if (
            item.corroboration_count
            >= 2
        ):
            flags.append(
                f"مؤكد من {item.corroboration_count} مصادر"
            )

        flag_text = ""

        if flags:
            flag_text = (
                " | "
                + " | ".join(
                    flags
                )
            )

        summary = (
            item.summary
            or ""
        )

        # Compress long summaries.
        if len(summary) > 300:

            summary = (
                summary[:300]
                + "..."
            )

        block = (
            f"[{index}] "
            f"{item.title}\n"
            f"المصدر: {item.source}\n"
            f"الدولة: {item.country}\n"
            f"المنطقة: {item.region}\n"
            f"الوقت: "
            f"{published or 'غير محدد'}"
            f"{flag_text}\n"
            f"الثقة: "
            f"{item.source_trust:.2f}\n"
            f"الأهمية: "
            f"{item.importance:.1f}\n"
            f"الصلة: "
            f"{item.relevance:.1f}\n"
            f"الرابط: {item.url}"
        )

        if summary:
            block += (
                f"\nالملخص: {summary}"
            )

        # Prevent oversized AI context.
        projected = (
            current_chars
            + len(block)
            + 2
        )

        if (
            projected
            > MAX_AI_CONTEXT_CHARS
        ):
            break

        blocks.append(
            block
        )

        current_chars = projected

    return "\n\n".join(
        blocks
    )


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

    for item in items[
        :max_items
    ]:

        prefix = (
            "🚨 "
            if item.is_breaking
            else "• "
        )

        confirmation = ""

        if (
            item.corroboration_count
            >= 2
        ):

            confirmation = (
                f"\nتأكيد: "
                f"{item.corroboration_count} مصادر"
            )

        lines.append(
            f"{prefix}{item.title}\n"
            f"المصدر: {item.source}"
            f"{confirmation}\n"
            f"{item.url}"
        )

    return "\n\n".join(
        lines
    )


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
# QUERY CACHE
# ============================================================

async def cached_query(
    query: str,
) -> Optional[List[NewsItem]]:

    key = _query_cache_key(
        query
    )

    async with _cache_lock:

        entry = _query_cache.get(
            key
        )

        if (
            entry is None
            or _cache_age(entry)
            > QUERY_CACHE_TTL
        ):
            return None

        return list(
            entry.value
        )


async def save_query_cache(
    query: str,
    results: List[NewsItem],
) -> None:

    key = _query_cache_key(
        query
    )

    async with _cache_lock:

        _query_cache[
            key
        ] = CacheEntry(
            value=list(
                results
            ),
            created_at=(
                datetime.now(
                    timezone.utc
                )
            ),
        )

        # Keep cache bounded.
        if len(
            _query_cache
        ) > 300:

            oldest_key = min(
                _query_cache,
                key=lambda k:
                    _query_cache[
                        k
                    ].created_at,
            )

            _query_cache.pop(
                oldest_key,
                None,
            )


# ============================================================
# COLLECTION + QUERY
# ============================================================

async def collect_and_search(
    query: str,
    max_items: int = MAX_SEARCH_RESULTS,
) -> List[NewsItem]:

    cached_results = await cached_query(
        query
    )

    if cached_results is not None:

        logger.info(
            "QUERY CACHE HIT: %s",
            query,
        )

        return cached_results[
            :max_items
        ]

    items = await collect_news(
        max_items=MAX_NEWS_ITEMS
    )

    results = search_news(
        items,
        query,
        max_results=max_items,
    )

    await save_query_cache(
        query,
        results,
    )

    return results


# ============================================================
# BREAKING RADAR
# ============================================================

def get_breaking_news(
    items: List[NewsItem],
    max_results: int = 10,
) -> List[NewsItem]:

    breaking = [
        item
        for item in items
        if item.is_breaking
    ]

    breaking.sort(
        key=lambda item: (
            item.importance,
            recency_score(
                item.published
            ),
            item.corroboration_count,
        ),
        reverse=True,
    )

    return breaking[
        :max_results
    ]


# ============================================================
# IMPORTANT NEWS RADAR
# ============================================================

def get_top_news(
    items: List[NewsItem],
    max_results: int = 10,
) -> List[NewsItem]:

    sorted_items = sort_items(
        list(items)
    )

    return sorted_items[
        :max_results
    ]


# ============================================================
# SOURCE STATISTICS
# ============================================================

def get_source_statistics(
    items: List[NewsItem],
) -> dict:

    stats = {}

    for item in items:

        stats[
            item.source
        ] = (
            stats.get(
                item.source,
                0,
            )
            + 1
        )

    return stats


# ============================================================
# CATEGORY STATISTICS
# ============================================================

def get_category_statistics(
    items: List[NewsItem],
) -> dict:

    stats = {}

    for item in items:

        stats[
            item.category
        ] = (
            stats.get(
                item.category,
                0,
            )
            + 1
        )

    return stats


# ============================================================
# REGION STATISTICS
# ============================================================

def get_region_statistics(
    items: List[NewsItem],
) -> dict:

    stats = {}

    for item in items:

        stats[
            item.region
        ] = (
            stats.get(
                item.region,
                0,
            )
            + 1
        )

    return stats


# ============================================================
# CACHE STATUS
# ============================================================

async def get_cache_status() -> dict:

    async with _cache_lock:

        news_age = (
            _cache_age(
                _news_cache
            )
            if _news_cache
            else None
        )

        return {
            "news_cache": (
                _news_cache
                is not None
            ),
            "news_cache_age": news_age,
            "query_cache_size": len(
                _query_cache
            ),
        }


# ============================================================
# CLEAR CACHE
# ============================================================

async def clear_news_cache() -> None:

    global _news_cache

    async with _cache_lock:

        _news_cache = None
        _query_cache.clear()

    logger.info(
        "News and query caches cleared."
    )


# ============================================================
# TEST
# ============================================================

async def test_collection():

    items = await collect_news()

    print(
        f"Collected: {len(items)}"
    )

    print(
        "Sources:"
    )

    print(
        get_source_statistics(
            items
        )
    )

    print(
        "\nRegions:"
    )

    print(
        get_region_statistics(
            items
        )
    )

    print(
        "\nTop news:"
    )

    for item in items[:10]:

        print(
            "\n"
            f"{item.title}\n"
            f"Source: {item.source}\n"
            f"Region: {item.region}\n"
            f"Breaking: {item.is_breaking}\n"
            f"Importance: {item.importance}\n"
            f"{item.url}"
        )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    asyncio.run(
        test_collection()
    )
