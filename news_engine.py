import asyncio
import html
import logging
import re
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp
import feedparser

log = logging.getLogger("news_engine")

FETCH_TIMEOUT = 7
MAX_FEED_ITEMS = 30
MAX_ONLINE_QUERIES = 6
COLLECTION_CONCURRENCY = 10
ROTATION_WINDOW_SECONDS = 300

TRUSTED_FEEDS = {
    "الجزيرة": "https://www.aljazeera.net/aljazeerarss/a7c1866f-6829-4883-8441-358d731800bc/43316f44-8e12-4320-b4c2-a22f6654b321",
    "سكاي نيوز عربية": "https://www.skynewsarabia.com/rss/v1/news.xml",
    "CNBC عربية": "https://www.cnbcarabia.com/rss.xml",
    "Investing": "https://sa.investing.com/rss/news.rss",
    "وكالة الأنباء السعودية": "https://www.spa.gov.sa/rss.xml",
    "BBC عربي": "https://feeds.bbci.co.uk/arabic/rss.xml",
    "DW عربي": "https://rss.dw.com/rdf/rss-ar-all",
    "France24 عربي": "https://www.france24.com/ar/rss",
    "EIA": "https://www.eia.gov/rss/todayinenergy.xml",
}

ADDITIONAL_TRUSTED_FEEDS = {
    "NASA": "https://www.nasa.gov/rss/dyn/breaking_news.rss",
    "UN News": "https://news.un.org/feed/subscribe/en/news/all/rss.xml",
}

MAJOR_NEWS_DOMAINS = {
    "reuters.com": 96,
    "apnews.com": 95,
    "bbc.com": 92,
    "bbc.co.uk": 92,
    "aljazeera.net": 88,
    "skynewsarabia.com": 86,
    "alarabiya.net": 86,
    "asharq.com": 86,
    "france24.com": 85,
    "dw.com": 84,
    "cnbcarabia.com": 82,
    "bloomberg.com": 91,
    "ft.com": 91,
    "wsj.com": 90,
    "nytimes.com": 88,
    "spa.gov.sa": 94,
    "eia.gov": 94,
    "nasa.gov": 94,
    "who.int": 94,
    "un.org": 94,
    "nato.int": 94,
    "opec.org": 94,
}

OFFICIAL_DOMAIN_HINTS = (
    ".gov",
    ".gob.",
    ".go.",
    ".mil",
    ".mod.",
    "government",
    "gov.uk",
    "bund.de",
    "admin.ch",
    "europa.eu",
    "un.org",
    "nato.int",
    "who.int",
    "imf.org",
    "worldbank.org",
    "ecb.europa.eu",
    "bis.org",
    "opec.org",
)

REGIONS = {
    "الشرق الأوسط": [
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
    ],
    "آسيا": [
        "الصين",
        "اليابان",
        "الهند",
        "كوريا الجنوبية",
        "كوريا الشمالية",
        "إندونيسيا",
        "ماليزيا",
        "سنغافورة",
        "تايلاند",
        "فيتنام",
        "الفلبين",
        "باكستان",
        "بنغلاديش",
        "تايوان",
        "أفغانستان",
        "نيبال",
    ],
    "أوروبا": [
        "بريطانيا",
        "المملكة المتحدة",
        "فرنسا",
        "ألمانيا",
        "إيطاليا",
        "إسبانيا",
        "البرتغال",
        "هولندا",
        "بلجيكا",
        "سويسرا",
        "النمسا",
        "بولندا",
        "أوكرانيا",
        "روسيا",
        "السويد",
        "النرويج",
        "الدنمارك",
        "فنلندا",
        "اليونان",
    ],
    "أفريقيا": [
        "المغرب",
        "الجزائر",
        "تونس",
        "ليبيا",
        "السودان",
        "إثيوبيا",
        "كينيا",
        "نيجيريا",
        "جنوب أفريقيا",
        "غانا",
        "تنزانيا",
        "الصومال",
        "السنغال",
        "أنغولا",
        "النيجر",
        "مالي",
        "تشاد",
    ],
    "أمريكا الشمالية": [
        "الولايات المتحدة",
        "أمريكا",
        "كندا",
        "المكسيك",
        "كوبا",
        "بنما",
    ],
    "أمريكا الجنوبية": [
        "البرازيل",
        "الأرجنتين",
        "تشيلي",
        "كولومبيا",
        "بيرو",
        "فنزويلا",
        "الإكوادور",
        "بوليفيا",
        "أوروغواي",
        "باراغواي",
    ],
}

COUNTRY_EN = {
    "السعودية": "Saudi Arabia",
    "الإمارات": "United Arab Emirates",
    "قطر": "Qatar",
    "الكويت": "Kuwait",
    "البحرين": "Bahrain",
    "عمان": "Oman",
    "اليمن": "Yemen",
    "العراق": "Iraq",
    "إيران": "Iran",
    "سوريا": "Syria",
    "لبنان": "Lebanon",
    "الأردن": "Jordan",
    "فلسطين": "Palestine",
    "إسرائيل": "Israel",
    "مصر": "Egypt",
    "تركيا": "Turkey",
    "الصين": "China",
    "اليابان": "Japan",
    "الهند": "India",
    "روسيا": "Russia",
    "أوكرانيا": "Ukraine",
    "بريطانيا": "United Kingdom",
    "المملكة المتحدة": "United Kingdom",
    "فرنسا": "France",
    "ألمانيا": "Germany",
    "إيطاليا": "Italy",
    "إسبانيا": "Spain",
    "الولايات المتحدة": "United States",
    "أمريكا": "United States",
    "كندا": "Canada",
    "المكسيك": "Mexico",
    "البرازيل": "Brazil",
    "الأرجنتين": "Argentina",
    "كولومبيا": "Colombia",
    "فنزويلا": "Venezuela",
    "السودان": "Sudan",
    "نيبال": "Nepal",
}

COUNTRY_ALIASES = {
    "Saudi Arabia": "السعودية",
    "United Arab Emirates": "الإمارات",
    "Qatar": "قطر",
    "Kuwait": "الكويت",
    "Bahrain": "البحرين",
    "Oman": "عمان",
    "Yemen": "اليمن",
    "Iraq": "العراق",
    "Iran": "إيران",
    "Syria": "سوريا",
    "Lebanon": "لبنان",
    "Jordan": "الأردن",
    "Palestine": "فلسطين",
    "Israel": "إسرائيل",
    "Egypt": "مصر",
    "Turkey": "تركيا",
    "China": "الصين",
    "Japan": "اليابان",
    "India": "الهند",
    "Russia": "روسيا",
    "Ukraine": "أوكرانيا",
    "United Kingdom": "بريطانيا",
    "France": "فرنسا",
    "Germany": "ألمانيا",
    "Italy": "إيطاليا",
    "Spain": "إسبانيا",
    "United States": "الولايات المتحدة",
    "Canada": "كندا",
    "Mexico": "المكسيك",
    "Brazil": "البرازيل",
    "Argentina": "الأرجنتين",
    "Colombia": "كولومبيا",
    "Venezuela": "فنزويلا",
    "Sudan": "السودان",
    "Nepal": "نيبال",
}

ECON_TERMS = [
    "اقتصاد", "اقتصادي", "أسواق", "سوق", "أسهم", "سهم", "بورصة",
    "الذهب", "فائدة", "عملات", "دولار", "بيتكوين", "تداول", "نفط",
    "أوبك", "خام", "تضخم", "برنت", "طاقة", "غاز", "استثمار", "سندات",
    "ميزانية", "ناتج محلي", "بنك مركزي", "صادرات", "واردات",
    "أسعار المستهلك", "أسعار المنتجين", "استحواذ", "أرباح",
    "oil", "crude", "opec", "brent", "energy", "natural gas", "lng",
    "economy", "economic", "markets", "market", "stocks", "equities",
    "stock exchange", "inflation", "interest rates", "gold", "dollar",
    "usd", "bitcoin", "crypto", "investment", "bonds", "budget", "gdp",
    "central bank", "exports", "imports", "earnings", "acquisition",
]

ECON_EXCLUDE = [
    "إنقاذ", "انقاذ", "زلزال", "وفاة", "تعازي", "يعزي", "يعزّي",
    "حادث", "غرق", "انتشال", "إنقاذ عمال", "منجم", "نفق", "فيضانات",
    "طقس", "rescue", "earthquake", "death", "funeral", "accident",
    "drowning", "flood", "weather",
]

SECURITY_TERMS = [
    "عسكري", "جيش", "قوات", "دفاع", "أمن", "الأمن القومي", "تسليح",
    "أسلحة", "سلاح", "صاروخ", "صواريخ", "قصف", "غارة", "غارات", "هجوم",
    "اشتباك", "مناورات", "قاعدة عسكرية", "طيران عسكري", "مقاتلات",
    "طائرات مسيرة", "ذخائر", "دفاع جوي", "عملية عسكرية",
    "عمليات عسكرية", "قوات خاصة", "استهداف", "إطلاق النار", "قتال",
    "معارك", "أسطول",
    "military", "army", "forces", "defense", "defence", "security",
    "weapons", "weapon", "missile", "missiles", "airstrike", "strike",
    "attack", "fighting", "battle", "battles", "combat", "drone",
    "drones", "ammunition", "air defense", "military operation",
    "troops", "navy", "warship",
]

SECURITY_SOCIAL_EXCLUDE = [
    "يعزي", "يعزّي", "تعازي", "وفاة والده", "وفاة والدته",
    "وفاة شقيق", "وفاة عمه", "تهنئة", "ترقية", "تعيين", "استقبال",
    "زيارة تفقدية", "احتفال", "condolences", "condolence",
    "promotion", "appointment", "welcomes", "ceremony", "inspection visit",
]

OFFICIAL_ENTITY_TERMS = [
    "وزارة", "وزارة الخارجية", "وزارة الدفاع", "وزارة الداخلية",
    "وزارة المالية", "وزارة الطاقة", "وزارة الصحة", "وزارة الإعلام",
    "الخارجية", "الوزير", "السفير", "السفارة", "المبعوث", "الرئاسة",
    "الرئيس", "رئيس الوزراء", "رئاسة الوزراء", "الديوان الملكي",
    "الحكومة", "المتحدث",
    "government", "ministry", "minister", "foreign ministry",
    "defense ministry", "defence ministry", "ambassador", "embassy",
    "envoy", "president", "prime minister", "spokesperson",
]

OFFICIAL_ACTION_TERMS = [
    "بيان", "بيان رسمي", "بيان صحفي", "تصريح", "تصريح رسمي",
    "تصريح صحفي", "المتحدث الرسمي", "المتحدث باسم", "مصدر مسؤول",
    "قال", "قالت", "أكد", "أكدت", "يؤكد", "تؤكد", "أعلن", "أعلنت",
    "يعلن", "تعلن", "صرح", "صرحت", "أوضح", "أوضحت", "شدد", "شددت",
    "دعا", "دعت", "حذر", "حذرت", "ندد", "نددت", "رحب", "رحبت",
    "يدعم", "تدعم", "دعم", "توجيه", "توجيهات", "وجه", "وجهت",
    "يوجه", "توجه", "أصدر", "أصدرت", "اعتماد", "اعتمد", "اعتمدت",
    "أطلق", "أطلقت", "يطلق", "تطلق", "إطلاق", "دشن", "دشنت",
    "تدشين", "افتتح", "افتتحت", "افتتاح", "وقع", "وقعت", "توقيع",
    "launch", "launched", "unveils", "unveiled", "introduces",
    "introduced", "signs", "signed", "official statement",
    "press statement", "press release", "statement", "spokesperson",
    "said", "says", "announced", "announces", "confirmed", "confirms",
    "stated", "states", "declared", "called for", "warned", "welcomed",
    "supported", "supports", "directed", "orders", "ordered", "issued",
]

OFFICIAL_EXCLUDE_TERMS = [
    "ما حقيقة", "حقيقة الوثيقة", "وثيقة متداولة", "وثيقة مزعومة",
    "وثيقة مزورة", "وثيقة مفبركة", "يزعم أنها", "يزعم أنه", "المتداول",
    "متداول", "شائعة", "شائعات", "تحقق", "تدقيق", "نفى صحة", "نفي صحة",
    "هل صحيح", "حول بيان", "بشأن بيان", "تقرير عن بيان",
    "تقرير حول بيان", "قراءة في بيان", "تحليل بيان", "تعليق على بيان",
    "fact check", "fact-check", "rumor", "rumour", "alleged",
    "purported", "verification", "misinformation", "fake document",
]

URGENT_TERMS = [
    "عاجل", "طارئ", "هجوم", "انفجار", "قصف", "صاروخ", "زلزال",
    "اشتباك", "غارة", "إخلاء", "حالة طوارئ", "تحذير عاجل", "استهداف",
    "غارات", "إطلاق النار", "breaking", "urgent", "attack", "explosion",
    "airstrike", "missile", "earthquake", "evacuation", "emergency",
    "warning", "strike", "gunfire",
]

DIGEST_TERMS = [
    "أهم الأخبار", "أبرز الأخبار", "حصاد الأخبار", "موجز الأخبار",
    "أخبار العالم حتى", "أهم الأخبار العالمية والعربية",
    "most important news", "top news", "news roundup",
    "world news roundup", "top stories", "daily roundup",
    "news digest", "latest news roundup",
]

QUERY_ALIASES = {
    "نفط": ["oil", "crude oil", "brent", "opec"],
    "النفط": ["oil", "crude oil", "brent", "opec"],
    "طاقة": ["energy", "oil", "gas"],
    "غاز": ["gas", "natural gas", "lng"],
    "اقتصاد": ["economy", "economic"],
    "أسواق": ["markets", "market"],
    "أسهم": ["stocks", "equities"],
    "بورصة": ["stock exchange", "equities"],
    "تضخم": ["inflation"],
    "فائدة": ["interest rates", "rate decision"],
    "ذهب": ["gold"],
    "دولار": ["dollar", "USD"],
    "بيتكوين": ["bitcoin", "crypto"],
    "اقتصادي": ["economic", "economy"],
    "أمن": ["security"],
    "دفاع": ["defense", "defence"],
    "عسكري": ["military"],
    "صاروخ": ["missile"],
    "قصف": ["airstrike", "strike"],
    "هجوم": ["attack"],
    "وزارة الخارجية": ["foreign ministry"],
    "بيان رسمي": ["official statement"],
}

LANGUAGE_PROFILES = {
    "ar": ("ar", "SA", "SA:ar"),
    "en": ("en", "US", "US:en"),
    "zh": ("zh-CN", "CN", "CN:zh-Hans"),
    "ru": ("ru", "RU", "RU:ru"),
    "fr": ("fr", "FR", "FR:fr"),
    "de": ("de", "DE", "DE:de"),
    "es": ("es", "ES", "ES:es"),
    "it": ("it", "IT", "IT:it"),
    "pt": ("pt-BR", "BR", "BR:pt-419"),
}


def normalize_text(value):
    text = str(value or "").lower().strip()
    text = re.sub(r"[\u064B-\u065F\u0670]", "", text)

    for old, new in {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ى": "ي",
        "ة": "ه",
        "ؤ": "و",
        "ئ": "ي",
    }.items():
        text = text.replace(old, new)

    text = re.sub(r"[^\w\s\u0600-\u06FF-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def tokenize(value):
    return {x for x in normalize_text(value).split() if len(x) > 2}


def similarity_score(a, b):
    sa, sb = tokenize(a), tokenize(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(1, len(sa | sb))


def normalized_title(value):
    return normalize_text(value)


def _text_has_any(text, terms):
    n = normalize_text(text)
    return any(normalize_text(term) in n for term in terms)


def detect_language_family(text):
    text = str(text or "")

    if re.search(r"[\u0600-\u06FF]", text):
        return "ar"
    if re.search(r"[\u4E00-\u9FFF]", text):
        return "zh"
    if re.search(r"[\u0400-\u04FF]", text):
        return "ru"
    if re.search(r"[\u00C0-\u024F]", text):
        return "latin"
    return "en"


def detect_region(text):
    n = normalize_text(text)

    for region, places in REGIONS.items():
        for place in places:
            if normalize_text(place) in n:
                return region

    for country_en, country_ar in COUNTRY_ALIASES.items():
        if normalize_text(country_en) in n:
            for region, places in REGIONS.items():
                if any(
                    normalize_text(country_ar) == normalize_text(place)
                    for place in places
                ):
                    return region

    return ""


def parse_date(value):
    if not value:
        return None

    try:
        dt = parsedate_to_datetime(value)
        return (
            dt.astimezone(timezone.utc)
            if dt.tzinfo
            else dt.replace(tzinfo=timezone.utc)
        )
    except Exception:
        return None


def _published_close(a, b, hours=8):
    if not a or not b:
        return True
    return abs((a - b).total_seconds()) <= hours * 3600


EVENT_FAMILIES = {
    "military": SECURITY_TERMS,
    "oil": [
        "نفط", "خام", "أوبك", "برنت", "oil", "crude", "opec", "brent",
    ],
    "markets": [
        "أسواق", "أسهم", "بورصة", "تداول", "markets", "stocks", "equities",
    ],
    "earthquake": [
        "زلزال", "earthquake",
    ],
    "rescue": [
        "إنقاذ", "إنقاذ عمال", "rescue", "rescued",
    ],
    "diplomatic": [
        "وزارة الخارجية", "الخارجية", "سفير", "السفير", "مبعوث",
        "foreign ministry", "ambassador", "envoy",
    ],
    "official_statement": [
        "بيان", "تصريح", "أعلن", "أكد", "statement", "announced",
        "confirmed", "issued",
    ],
}


def event_family(text):
    n = normalize_text(text)
    scores = {}

    for family, terms in EVENT_FAMILIES.items():
        hits = score_terms(n, terms)
        if hits:
            scores[family] = hits

    if not scores:
        return ""

    return max(scores.items(), key=lambda x: x[1])[0]


def same_event(a, b):
    na = normalized_title(a.title)
    nb = normalized_title(b.title)

    if not na or not nb:
        return False

    if na == nb:
        return True

    sim = similarity_score(na, nb)

    if sim >= 0.78:
        return True

    if sim >= 0.58 and a.region and a.region == b.region:
        return True

    # Cross-language event matching.
    # It is deliberately conservative: region + event family + close
    # publication time are required to avoid merging unrelated stories.
    if a.region and b.region and a.region == b.region:
        family_a = event_family(f"{a.title} {a.summary}")
        family_b = event_family(f"{b.title} {b.summary}")

        if (
            family_a
            and family_a == family_b
            and _published_close(a.published, b.published, hours=8)
        ):
            return True

    return False


@dataclass
class NewsItem:
    title: str
    url: str
    source: str
    summary: str = ""
    published: Optional[datetime] = None
    category: str = "general"
    region: str = ""
    original_title: str = ""
    domain: str = ""
    trust_score: float = 50.0
    urgency_score: float = 0.0
    relevance_score: float = 0.0
    official: bool = False
    search_text: str = ""

    def __post_init__(self):
        self.title = (self.title or "").strip()
        self.original_title = self.original_title or self.title
        self.source = (self.source or "مصدر إخباري").strip()

        self.summary = re.sub(
            r"<[^>]+>",
            " ",
            self.summary or "",
        ).strip()

        self.domain = (
            self.domain
            or urlparse(self.url).netloc
            or ""
        ).lower().replace("www.", "")

        combined = f"{self.title} {self.summary}"

        self.region = self.region or detect_region(combined)

        self.official = (
            self.official
            or is_official_source(self.source, self.domain)
        )

        self.trust_score = max(
            self.trust_score,
            source_trust(self.source, self.domain),
        )

        self.urgency_score = score_terms(
            combined,
            URGENT_TERMS,
        )

        self.search_text = normalize_text(
            f"{self.title} {self.original_title} "
            f"{self.summary} {self.source} {self.region}"
        )


def is_official_source(source, domain):
    d = normalize_text(domain)
    s = normalize_text(source)

    if any(
        d.endswith(hint) or hint in d
        for hint in OFFICIAL_DOMAIN_HINTS
    ):
        return True

    known_official_sources = [
        "وكاله الانباء السعوديه",
        "eia",
        "nasa",
        "un news",
    ]

    return any(
        normalize_text(name) in s
        for name in known_official_sources
    )


def source_trust(source, domain):
    d = normalize_text(domain)

    for key, score in MAJOR_NEWS_DOMAINS.items():
        if d.endswith(normalize_text(key)):
            return score

    s = normalize_text(source)

    if "وكاله الانباء السعوديه" in s:
        return 94

    if "رويترز" in s or "reuters" in s:
        return 96

    if "بي بي سي" in s or "bbc" in s:
        return 92

    return 50


def score_terms(text, terms):
    n = normalize_text(text)
    return sum(
        1
        for term in terms
        if normalize_text(term) in n
    )


def _is_digest(title):
    return _text_has_any(title, DIGEST_TERMS)


def _security_signal(title, summary=""):
    t = normalize_text(title)
    s = normalize_text(summary)

    if _text_has_any(t, SECURITY_SOCIAL_EXCLUDE):
        return False

    return (
        score_terms(t, SECURITY_TERMS) >= 1
        or score_terms(s, SECURITY_TERMS) >= 2
    )


def _economy_signal(title, summary=""):
    t = normalize_text(title)
    s = normalize_text(summary)

    title_hits = score_terms(t, ECON_TERMS)
    summary_hits = score_terms(s, ECON_TERMS)
    negative = score_terms(f"{t} {s}", ECON_EXCLUDE)

    if negative >= 1 and title_hits == 0:
        return False

    if title_hits >= 1:
        return True

    return summary_hits >= 2 and negative == 0


def _official_signal(title, summary, item=None):
    t = normalize_text(title)
    s = normalize_text(summary)
    text = f"{t} {s}"

    if score_terms(text, OFFICIAL_EXCLUDE_TERMS) > 0:
        return False

    entity_title = score_terms(t, OFFICIAL_ENTITY_TERMS)
    entity_summary = score_terms(s, OFFICIAL_ENTITY_TERMS)

    action_title = score_terms(t, OFFICIAL_ACTION_TERMS)
    action_summary = score_terms(s, OFFICIAL_ACTION_TERMS)

    entity_present = (
        entity_title >= 1 or entity_summary >= 1
    )

    if not entity_present:
        return False

    generic_meeting_terms = [
        "محادثات", "مباحثات", "اجتماع", "اجتماعات", "لقاء", "لقاءات",
        "مؤتمر", "قمة", "مشاركة", "مشاركة متوقعة", "سيبحث", "ستبحث",
        "يناقش", "تبحث", "talks", "meeting", "meetings", "discussions",
        "summit", "conference", "participation", "expected participation",
        "will discuss", "will address",
    ]

    generic_meeting = (
        score_terms(t, generic_meeting_terms) > 0
        and action_title == 0
    )

    if generic_meeting:
        return False

    # السفير/المسؤول وحده لا يكفي.
    # يجب أن يرتبط بتصريح أو موقف أو فعل.
    if entity_title >= 1 and action_title >= 1:
        return True

    if action_title >= 1 and entity_summary >= 1:
        return True

    if entity_title >= 1 and action_summary >= 1:
        return True

    if (
        item is not None
        and item.official
        and (
            action_title >= 1
            or action_summary >= 1
        )
    ):
        return True

    return False


def classify_item(item):
    title = normalize_text(item.title)
    summary = normalize_text(item.summary)
    text = f"{title} {summary}"

    if _is_digest(item.title):
        item.category = "general"
        return item

    econ = (
        score_terms(title, ECON_TERMS) * 3
        + score_terms(summary, ECON_TERMS)
    )

    if score_terms(text, ECON_EXCLUDE):
        econ -= 5

    sec = (
        score_terms(title, SECURITY_TERMS) * 3
        + score_terms(summary, SECURITY_TERMS)
    )

    if score_terms(title, SECURITY_SOCIAL_EXCLUDE):
        sec -= 12

    official_signal = _official_signal(
        title,
        summary,
        item,
    )

    official = (
        score_terms(title, OFFICIAL_ENTITY_TERMS) * 2
        + score_terms(title, OFFICIAL_ACTION_TERMS) * 3
        + score_terms(summary, OFFICIAL_ENTITY_TERMS)
        + score_terms(summary, OFFICIAL_ACTION_TERMS)
    )

    if item.official:
        official += 1

    urgent = (
        score_terms(title, URGENT_TERMS) * 2
        + score_terms(summary, URGENT_TERMS)
    )

    if (
        _security_signal(title, summary)
        and sec >= max(econ, 4)
        and not (
            official_signal
            and official > sec + 2
        )
    ):
        item.category = "secu"

    elif (
        official_signal
        and official >= max(econ, sec, 4)
    ):
        item.category = "forg"

    elif (
        _economy_signal(title, summary)
        and econ >= max(sec, official, 4)
    ):
        item.category = "econ"

    elif urgent >= 4:
        item.category = "urg"

    else:
        item.category = "general"

    return item


def is_topic_match(item, topic_key):
    title = item.title or ""
    summary = item.summary or ""

    if _is_digest(title):
        return False

    if topic_key == "econ":
        return _economy_signal(title, summary) and not (
            _security_signal(title, summary)
            and score_terms(title, SECURITY_TERMS) >= 1
            and score_terms(title, ECON_TERMS) <= 1
        )

    if topic_key == "secu":
        return _security_signal(title, summary)

    if topic_key == "forg":
        return _official_signal(title, summary, item)

    if topic_key == "urg":
        return (
            score_terms(title, URGENT_TERMS) >= 1
            or score_terms(summary, URGENT_TERMS) >= 2
        )

    text = normalize_text(f"{title} {summary}")

    if topic_key == "gulf":
        return any(
            normalize_text(x) in text
            for x in REGIONS["الشرق الأوسط"]
        )

    if topic_key == "wrld":
        return bool(item.region) or any(
            normalize_text(x) in text
            for x in [
                "امريكا",
                "الولايات المتحده",
                "اوروبا",
                "الصين",
                "روسيا",
                "اوكرانيا",
                "الهند",
                "اليابان",
            ]
        )

    return False


def _item_preference(item):
    return (
        float(item.trust_score or 0),
        float(item.relevance_score or 0),
        item.published.timestamp() if item.published else 0,
    )


def deduplicate_news(items):
    unique = []
    seen_urls = set()

    for item in items:
        if _is_digest(item.title):
            continue

        url_key = item.url.strip().lower()

        if url_key and url_key in seen_urls:
            continue

        if url_key:
            seen_urls.add(url_key)

        duplicate = False

        for index, existing in enumerate(unique):
            if same_event(item, existing):
                if _item_preference(item) > _item_preference(existing):
                    unique[index] = item

                duplicate = True
                break

        if not duplicate:
            unique.append(item)

    return unique


def _extract_google_source(entry):
    source_info = entry.get("source")

    source_title = ""
    source_href = ""

    if isinstance(source_info, dict):
        source_title = str(
            source_info.get("title") or ""
        ).strip()
        source_href = str(
            source_info.get("href") or ""
        ).strip()
    else:
        source_title = str(
            getattr(source_info, "title", "") or ""
        ).strip()
        source_href = str(
            getattr(source_info, "href", "") or ""
        ).strip()

    return source_title, source_href


def parse_entry(entry, source="مصدر إخباري", category="general"):
    title = html.unescape(
        str(entry.get("title", "") or "").strip()
    )

    url = str(
        entry.get("link", "") or ""
    ).strip()

    if not title or not url:
        return None

    summary = html.unescape(
        str(
            entry.get("summary")
            or entry.get("description")
            or ""
        )
    )

    published = parse_date(
        entry.get("published")
        or entry.get("updated")
        or ""
    )

    actual_source_title, actual_source_href = _extract_google_source(entry)

    final_source = (
        actual_source_title
        or source
        or "مصدر إخباري"
    )

    domain = ""

    if actual_source_href:
        domain = urlparse(
            actual_source_href
        ).netloc.replace("www.", "").lower()

    if not domain:
        domain = urlparse(
            url
        ).netloc.replace("www.", "").lower()

    item = NewsItem(
        title=title,
        original_title=title,
        url=url,
        source=final_source,
        summary=summary,
        published=published,
        category=category,
        domain=domain,
    )

    return classify_item(item)


async def fetch_feed(session, source, url):
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(
                total=FETCH_TIMEOUT
            ),
            headers={
                "User-Agent": "Global-Intel-Bot/3.0"
            },
        ) as response:

            if response.status != 200:
                return []

            data = await response.read()

        parsed = feedparser.parse(data)
        items = []

        for entry in parsed.entries[:MAX_FEED_ITEMS]:
            item = parse_entry(
                entry,
                source,
            )

            if item:
                items.append(item)

        return items

    except Exception:
        log.exception("Feed failed: %s", source)
        return []


def google_news_url(
    query,
    language="ar",
):
    profile = LANGUAGE_PROFILES.get(
        language,
        LANGUAGE_PROFILES["ar"],
    )

    hl, gl, ceid = profile

    q = urllib.parse.quote_plus(query)

    return (
        "https://news.google.com/rss/search"
        f"?q={q}"
        f"&hl={hl}"
        f"&gl={gl}"
        f"&ceid={ceid}"
    )


def build_query_variants(query):
    normalized = normalize_text(query)
    variants = [query]

    for key, aliases in QUERY_ALIASES.items():
        if normalize_text(key) in normalized:
            variants.extend(aliases)

    for arabic_country, english_country in COUNTRY_EN.items():
        if normalize_text(arabic_country) in normalized:
            variants.append(
                query.replace(
                    arabic_country,
                    english_country,
                )
            )

    # If user searches using an English country name, add Arabic form.
    for english_country, arabic_country in COUNTRY_ALIASES.items():
        if normalize_text(english_country) in normalized:
            variants.append(
                query.replace(
                    english_country,
                    arabic_country,
                )
            )

    # Always include a compact English equivalent for Arabic searches
    # containing known countries/subjects.
    english_parts = []

    for arabic_country, english_country in COUNTRY_EN.items():
        if normalize_text(arabic_country) in normalized:
            english_parts.append(english_country)

    for key, aliases in QUERY_ALIASES.items():
        if normalize_text(key) in normalized and aliases:
            english_parts.append(aliases[0])

    if english_parts:
        variants.append(" ".join(english_parts))

    seen = set()
    result = []

    for value in variants:
        marker = normalize_text(value)
        if marker and marker not in seen:
            seen.add(marker)
            result.append(value)

    return result[:MAX_ONLINE_QUERIES]


def languages_for_query(query):
    family = detect_language_family(query)

    if family == "ar":
        return ["ar", "en"]

    if family == "zh":
        return ["zh", "en"]

    if family == "ru":
        return ["ru", "en"]

    if family == "latin":
        return ["en", "ar"]

    return ["en", "ar"]


async def search_news_online(
    query,
    max_results=25,
):
    variants = build_query_variants(query)
    languages = languages_for_query(query)

    requests_plan = []

    for index, q in enumerate(variants):
        language = languages[
            index % len(languages)
        ]
        requests_plan.append(
            (q, language)
        )

    async with aiohttp.ClientSession() as session:
        groups = await asyncio.gather(
            *(
                fetch_feed(
                    session,
                    "Google News",
                    google_news_url(
                        q,
                        language,
                    ),
                )
                for q, language in requests_plan
            ),
            return_exceptions=True,
        )

    items = []

    for group in groups:
        if isinstance(group, list):
            items.extend(group)

    items = [
        item
        for item in deduplicate_news(items)
        if not _is_digest(item.title)
    ]

    return rank_search_results(
        items,
        query,
    )[:max_results]


def _query_token_variants(query):
    tokens = tokenize(query)
    expanded = set(tokens)

    for key, aliases in QUERY_ALIASES.items():
        key_tokens = tokenize(key)

        if key_tokens & tokens:
            for alias in aliases:
                expanded.update(
                    tokenize(alias)
                )

    for arabic, english in COUNTRY_EN.items():
        if normalize_text(arabic) in normalize_text(query):
            expanded.update(
                tokenize(english)
            )

    for english, arabic in COUNTRY_ALIASES.items():
        if normalize_text(english) in normalize_text(query):
            expanded.update(
                tokenize(arabic)
            )

    return expanded


def rank_search_results(items, query):
    q_tokens = _query_token_variants(query)
    ranked = []

    for item in items:
        title_tokens = tokenize(item.title)
        summary_tokens = tokenize(item.summary)

        title_hits = len(
            q_tokens & title_tokens
        )

        summary_hits = len(
            q_tokens & summary_tokens
        )

        exact_phrase = (
            normalize_text(query)
            in normalize_text(item.title)
        )

        score = (
            title_hits * 12
            + summary_hits * 3
            + (25 if exact_phrase else 0)
            + item.trust_score * 0.04
        )

        item.relevance_score = score
        ranked.append((score, item))

    ranked.sort(
        key=lambda x: x[0],
        reverse=True,
    )

    return [
        item
        for _, item in ranked
    ]


async def search_news(
    items,
    query,
    max_results=25,
):
    return rank_search_results(
        [
            item
            for item in deduplicate_news(items)
            if not _is_digest(item.title)
        ],
        query,
    )[:max_results]


async def hybrid_search_news(
    items,
    query,
    max_results=25,
):
    local = await search_news(
        items,
        query,
        max_results,
    )

    online = await search_news_online(
        query,
        max_results,
    )

    merged = [
        item
        for item in deduplicate_news(
            local + online
        )
        if not _is_digest(item.title)
    ]

    ranked = rank_search_results(
        merged,
        query,
    )

    q_tokens = _query_token_variants(query)
    filtered = []

    for item in ranked:
        title_hits = len(
            q_tokens & tokenize(item.title)
        )

        summary_hits = len(
            q_tokens & tokenize(item.summary)
        )

        if title_hits or summary_hits:
            filtered.append(item)

    return filtered[:max_results]


async def collect_news(max_items=150):
    feeds = {
        **TRUSTED_FEEDS,
        **ADDITIONAL_TRUSTED_FEEDS,
    }

    async with aiohttp.ClientSession() as session:
        groups = await asyncio.gather(
            *(
                fetch_feed(
                    session,
                    source,
                    url,
                )
                for source, url in feeds.items()
            ),
            return_exceptions=True,
        )

    items = []

    for group in groups:
        if isinstance(group, list):
            items.extend(group)

    discovery_queries = [
        "السعودية اقتصاد أسواق نفط",
        "Saudi Arabia economy oil markets",
        "الشرق الأوسط أمن دفاع",
        "Middle East security defense",
        "بيانات رسمية وزارة خارجية",
        "official statement foreign ministry",
    ]

    try:
        discovery = await asyncio.gather(
            *(
                search_news_online(
                    q,
                    12,
                )
                for q in discovery_queries
            ),
            return_exceptions=True,
        )

        for group in discovery:
            if isinstance(group, list):
                items.extend(group)

    except Exception:
        log.exception("Discovery failed.")

    items = [
        item
        for item in deduplicate_news(items)
        if not _is_digest(item.title)
    ]

    items.sort(
        key=lambda x: (
            float(x.relevance_score or 0),
            float(x.trust_score or 0),
            x.published.timestamp()
            if x.published
            else 0,
        ),
        reverse=True,
    )

    return items[:max_items]


def build_ai_context(items):
    lines = []

    for i, item in enumerate(items, 1):
        published = (
            item.published.isoformat()
            if item.published
            else "غير متاح"
        )

        lines.append(
            f"{i}. العنوان: {item.title}\n"
            f"المصدر: {item.source}\n"
            f"المنطقة: {item.region or 'غير محددة'}\n"
            f"التاريخ: {published}\n"
            f"الملخص: {item.summary[:700]}"
        )

    return "\n\n".join(lines)
