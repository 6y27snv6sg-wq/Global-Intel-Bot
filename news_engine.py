# ============================================================
# news_engine.py
# Global News Intelligence Engine
# ============================================================

import asyncio
import html
import logging
import re
import time
import urllib.parse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import aiohttp
import feedparser


log = logging.getLogger("news_engine")

FETCH_TIMEOUT = 5
MAX_FEED_ITEMS = 25
MAX_ONLINE_QUERIES = 6
MAX_GDELT_RECORDS = 40
SEARCH_TIMEOUT = 14
COLLECTION_CONCURRENCY = 10
ROTATION_WINDOW_SECONDS = 300


# ============================================================
# SOURCE INTELLIGENCE
# ============================================================

TRUSTED_FEEDS: Dict[str, str] = {
    "الجزيرة": (
        "https://www.aljazeera.net/aljazeerarss/"
        "a7c1866f-6829-4883-8441-358d731800bc/"
        "43316f44-8e12-4320-b4c2-a22f6654b321"
    ),
    "سكاي نيوز عربية": "https://www.skynewsarabia.com/rss/v1/news.xml",
    "CNBC عربية": "https://www.cnbcarabia.com/rss.xml",
    "Investing": "https://sa.investing.com/rss/news.rss",
    "وكالة الأنباء السعودية": "https://www.spa.gov.sa/rss.xml",
    "BBC عربي": "https://feeds.bbci.co.uk/arabic/rss.xml",
    "DW عربي": "https://rss.dw.com/rdf/rss-ar-all",
    "France24 عربي": "https://www.france24.com/ar/rss",
    "EIA": "https://www.eia.gov/rss/todayinenergy.xml",
}

# Direct feeds are deliberately kept small and high-value. Discovery is
# expanded through Google News and GDELT so the engine is not locked to them.
ADDITIONAL_TRUSTED_FEEDS: Dict[str, str] = {
    "NASA": "https://www.nasa.gov/rss/dyn/breaking_news.rss",
    "UN News": "https://news.un.org/feed/subscribe/en/news/all/rss.xml",
}

OFFICIAL_DOMAIN_HINTS = (
    ".gov", ".gov.", ".gob.", ".go.", ".govt.", ".mil", ".mod.",
    "government", "gov.uk", "bund.de", "admin.ch", "europa.eu",
    "un.org", "nato.int", "who.int", "imf.org", "worldbank.org",
    "ecb.europa.eu", "bis.org", "opec.org",
)

OFFICIAL_SOURCE_NAMES = (
    "government", "ministry", "foreign ministry", "defense ministry",
    "ministry of defence", "presidency", "prime minister", "prime ministry",
    "central bank", "reserve bank", "national bank", "official gazette",
    "armed forces", "military", "security council", "police", "customs",
    "وزارة", "حكومة", "رئاسة", "رئاسة الوزراء", "الخارجية", "الدفاع",
    "البنك المركزي", "الجيش", "القوات المسلحة", "الأمن",
)

MAJOR_NEWS_DOMAINS = {
    "reuters.com": 96,
    "apnews.com": 95,
    "bbc.com": 92,
    "bbc.co.uk": 92,
    "aljazeera.net": 88,
    "aljazeera.com": 88,
    "skynewsarabia.com": 86,
    "alarabiya.net": 86,
    "asharq.com": 86,
    "france24.com": 85,
    "dw.com": 84,
    "cnbc.com": 82,
    "cnbcarabia.com": 82,
    "bloomberg.com": 91,
    "ft.com": 91,
    "wsj.com": 90,
    "nytimes.com": 88,
    "washingtonpost.com": 87,
    "theguardian.com": 84,
    "economist.com": 86,
    "spglobal.com": 84,
    "investing.com": 70,
    "spa.gov.sa": 94,
    "eia.gov": 94,
    "nasa.gov": 94,
    "who.int": 94,
    "un.org": 94,
    "nato.int": 94,
    "opec.org": 94,
}

SOURCE_NAME_SCORES = {
    "وكالة الأنباء السعودية": 94,
    "Reuters": 96,
    "Associated Press": 95,
    "AP": 95,
    "BBC": 92,
    "الجزيرة": 88,
    "العربية": 86,
    "سكاي نيوز عربية": 86,
    "الشرق": 86,
    "France24": 85,
    "DW": 84,
    "EIA": 94,
}


# ============================================================
# GLOBAL COVERAGE
# ============================================================

REGIONS: Dict[str, List[str]] = {
    "آسيا": [
        "الصين", "اليابان", "الهند", "كوريا الجنوبية", "كوريا الشمالية",
        "إندونيسيا", "ماليزيا", "سنغافورة", "تايلاند", "فيتنام", "الفلبين",
        "باكستان", "بنغلاديش", "تايوان", "أفغانستان", "منغوليا", "نيبال",
        "سريلانكا", "ميانمار", "كمبوديا", "لاوس", "بروناي", "بوتان",
    ],
    "الشرق الأوسط": [
        "السعودية", "الإمارات", "قطر", "الكويت", "البحرين", "عمان", "اليمن",
        "العراق", "إيران", "سوريا", "لبنان", "الأردن", "فلسطين", "إسرائيل",
        "مصر", "تركيا", "قبرص",
    ],
    "أفريقيا": [
        "مصر", "الجزائر", "المغرب", "تونس", "ليبيا", "السودان", "إثيوبيا",
        "كينيا", "نيجيريا", "جنوب أفريقيا", "غانا", "تنزانيا", "الصومال",
        "السنغال", "أنغولا", "زيمبابوي", "زامبيا", "أوغندا", "رواندا",
        "موزمبيق", "ناميبيا", "بوتسوانا", "الكاميرون", "ساحل العاج", "النيجر",
        "مالي", "تشاد", "الكونغو", "الغابون", "تنزانيا", "موريشيوس",
    ],
    "أوروبا": [
        "بريطانيا", "المملكة المتحدة", "فرنسا", "ألمانيا", "إيطاليا", "إسبانيا",
        "البرتغال", "هولندا", "بلجيكا", "سويسرا", "النمسا", "بولندا", "أوكرانيا",
        "روسيا", "السويد", "النرويج", "الدنمارك", "فنلندا", "اليونان", "رومانيا",
        "التشيك", "المجر", "أيرلندا", "صربيا", "بلغاريا", "كرواتيا", "سلوفاكيا",
        "سلوفينيا", "أيسلندا", "ليتوانيا", "لاتفيا", "إستونيا", "مولدوفا",
        "بيلاروسيا", "البوسنة", "ألبانيا", "مقدونيا الشمالية", "كوسوفو",
    ],
    "أستراليا والمحيط الهادئ": [
        "أستراليا", "نيوزيلندا", "فيجي", "بابوا غينيا الجديدة", "جزر سليمان",
        "ساموا", "تونغا", "فانواتو", "بالاو", "ميكرونيزيا",
    ],
    "أمريكا الشمالية": [
        "الولايات المتحدة", "أمريكا", "كندا", "المكسيك", "غواتيمالا", "بنما",
        "كوستاريكا", "كوبا", "جمهورية الدومينيكان", "هايتي", "جامايكا",
    ],
    "أمريكا الجنوبية": [
        "البرازيل", "الأرجنتين", "تشيلي", "كولومبيا", "بيرو", "فنزويلا",
        "الإكوادور", "بوليفيا", "أوروغواي", "باراغواي", "غيانا", "سورينام",
    ],
}

COUNTRY_EN: Dict[str, str] = {
    "السعودية": "Saudi Arabia", "الإمارات": "United Arab Emirates", "قطر": "Qatar",
    "الكويت": "Kuwait", "البحرين": "Bahrain", "عمان": "Oman", "اليمن": "Yemen",
    "العراق": "Iraq", "إيران": "Iran", "سوريا": "Syria", "لبنان": "Lebanon",
    "الأردن": "Jordan", "فلسطين": "Palestine", "إسرائيل": "Israel", "مصر": "Egypt",
    "تركيا": "Turkey", "الصين": "China", "اليابان": "Japan", "الهند": "India",
    "كوريا الجنوبية": "South Korea", "كوريا الشمالية": "North Korea", "إندونيسيا": "Indonesia",
    "ماليزيا": "Malaysia", "سنغافورة": "Singapore", "تايلاند": "Thailand", "فيتنام": "Vietnam",
    "الفلبين": "Philippines", "باكستان": "Pakistan", "بنغلاديش": "Bangladesh", "تايوان": "Taiwan",
    "أفغانستان": "Afghanistan", "روسيا": "Russia", "أوكرانيا": "Ukraine", "بريطانيا": "United Kingdom",
    "المملكة المتحدة": "United Kingdom", "فرنسا": "France", "ألمانيا": "Germany", "إيطاليا": "Italy",
    "إسبانيا": "Spain", "البرتغال": "Portugal", "هولندا": "Netherlands", "بلجيكا": "Belgium",
    "سويسرا": "Switzerland", "النمسا": "Austria", "بولندا": "Poland", "السويد": "Sweden",
    "النرويج": "Norway", "الدنمارك": "Denmark", "فنلندا": "Finland", "اليونان": "Greece",
    "رومانيا": "Romania", "التشيك": "Czech Republic", "المجر": "Hungary", "أيرلندا": "Ireland",
    "أستراليا": "Australia", "نيوزيلندا": "New Zealand", "الولايات المتحدة": "United States",
    "أمريكا": "United States", "كندا": "Canada", "المكسيك": "Mexico", "البرازيل": "Brazil",
    "الأرجنتين": "Argentina", "تشيلي": "Chile", "كولومبيا": "Colombia", "بيرو": "Peru",
    "فنزويلا": "Venezuela", "الإكوادور": "Ecuador", "بوليفيا": "Bolivia", "أوروغواي": "Uruguay",
    "باراغواي": "Paraguay", "جنوب أفريقيا": "South Africa", "نيجيريا": "Nigeria", "إثيوبيا": "Ethiopia",
    "كينيا": "Kenya", "المغرب": "Morocco", "الجزائر": "Algeria", "تونس": "Tunisia",
    "ليبيا": "Libya", "السودان": "Sudan", "الصومال": "Somalia", "السنغال": "Senegal",
}

QUERY_ALIASES: Dict[str, List[str]] = {
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
    "بنك مركزي": ["central bank", "monetary policy"],
    "ذهب": ["gold"],
    "دولار": ["dollar", "USD"],
    "بيتكوين": ["bitcoin", "crypto"],
    "حرب": ["war", "conflict"],
    "هجوم": ["attack", "strike", "assault"],
    "صاروخ": ["missile", "rocket"],
    "طائرة": ["aircraft", "plane"],
    "مسيرة": ["drone", "UAV"],
    "دفاع": ["defense", "defence", "military"],
    "أمن": ["security"],
    "عسكري": ["military"],
    "جيش": ["army", "military"],
    "عاجل": ["breaking", "urgent", "developing"],
    "حكومة": ["government"],
    "وزارة الخارجية": ["foreign ministry", "foreign affairs ministry"],
    "وزارة الدفاع": ["defense ministry", "defence ministry"],
    "بيان رسمي": ["official statement", "statement"],
    "انتخابات": ["election", "elections"],
    "رئيس": ["president"],
    "رئيس الوزراء": ["prime minister", "premier"],
}

GLOBAL_QUERIES: List[Tuple[str, str]] = [
    ('"وزارة" OR "حكومة" OR "رئاسة الوزراء" OR "وزارة الخارجية" OR "وزارة الدفاع" OR "بيان رسمي"', "official"),
    ('"اقتصاد" OR "أسواق" OR "أسهم" OR "بورصة" OR "نفط" OR "أوبك" OR "برنت" OR "غاز" OR "ذهب" OR "تضخم" OR "فائدة" OR "بنك مركزي" OR "بيتكوين"', "economy_energy"),
    ('"دفاع" OR "أمن" OR "عسكري" OR "جيش" OR "مناورات" OR "أمن قومي"', "security"),
    ('"عاجل" OR "طارئ" OR "بيان عاجل" OR "تطورات عاجلة" OR breaking OR urgent', "urgent"),
]

GLOBAL_QUERIES_EN: List[Tuple[str, str]] = [
    ('"government" OR "foreign ministry" OR "defense ministry" OR "official statement"', "official"),
    ('"economy" OR "markets" OR "stocks" OR "oil" OR "OPEC" OR "Brent" OR "gas" OR "gold" OR "inflation" OR "interest rates" OR "central bank" OR "bitcoin"', "economy_energy"),
    ('"defense" OR "defence" OR "security" OR "military" OR "army" OR "national security"', "security"),
    ('"breaking" OR "urgent" OR "developing" OR "emergency" OR "attack" OR "explosion"', "urgent"),
]


# ============================================================
# DATA MODEL
# ============================================================

class NewsItem:
    def __init__(
        self,
        title: str,
        source: str,
        url: str,
        published_at: str = "",
        category: str = "general",
        summary: str = "",
        region: str = "",
        original_title: str = "",
        language: str = "",
        trust_score: Optional[float] = None,
        urgency_score: Optional[float] = None,
        relevance_score: Optional[float] = None,
        official: bool = False,
        source_domain: str = "",
        independent_sources: int = 1,
        event_key: str = "",
    ):
        self.title = self.clean_text(title)
        self.source = self.clean_text(source)
        self.url = url.strip() if url else ""
        self.published_at = self.clean_text(published_at)
        self.category = category or "general"
        self.summary = self.clean_text(summary)
        self.region = region or detect_region(f"{title} {summary}")
        self.original_title = self.clean_text(original_title or title)
        self.language = language or detect_language(self.original_title)
        self.source_domain = source_domain or extract_domain(self.url)
        self.official = bool(official or is_official_source(self.source, self.source_domain))
        self.trust_score = float(trust_score if trust_score is not None else source_trust_score(self.source, self.source_domain, self.official))
        self.urgency_score = float(urgency_score if urgency_score is not None else urgency_score_text(f"{self.title} {self.summary}"))
        self.relevance_score = float(relevance_score if relevance_score is not None else 0.0)
        self.independent_sources = max(1, int(independent_sources or 1))
        self.event_key = event_key or normalized_title(self.title)
        self.search_text = " ".join(
            p for p in (self.title, self.original_title, self.summary, self.source, self.region, self.category, self.source_domain)
            if p
        ).lower()

    @staticmethod
    def clean_text(text: str) -> str:
        if not text:
            return ""
        text = html.unescape(str(text))
        text = re.sub(r"<script.*?</script>", " ", text, flags=re.I | re.S)
        text = re.sub(r"<style.*?</style>", " ", text, flags=re.I | re.S)
        text = re.sub(r"<[^>]+>", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    def __repr__(self) -> str:
        return f"<NewsItem title='{self.title[:50]}...' source='{self.source}'>"


# ============================================================
# NORMALIZATION / SCORING
# ============================================================

ARABIC_STOPWORDS = {
    "من", "في", "على", "إلى", "عن", "مع", "هذا", "هذه", "ذلك", "تلك", "التي", "الذي",
    "و", "أو", "ثم", "أن", "إن", "كان", "كانت", "هو", "هي", "هم", "هم", "بعد", "قبل", "بين",
    "لدى", "لـ", "فيه", "فيها", "ما", "ماذا", "هل", "قد", "تم", "يتم", "خلال", "حول", "ضمن",
}
EN_STOPWORDS = {
    "the", "a", "an", "of", "in", "on", "at", "to", "for", "and", "or", "with", "from", "by",
    "is", "are", "was", "were", "be", "been", "this", "that", "these", "those", "after", "before",
}

URGENT_TERMS = {
    "عاجل": 20, "عاجلة": 20, "طارئ": 18, "طارئة": 18, "هجوم": 16, "انفجار": 17,
    "قصف": 17, "ضربة": 16, "صاروخ": 16, "غارة": 16, "إخلاء": 14, "قتلى": 13,
    "وفاة": 10, "زلزال": 18, "تسونامي": 22, "إطلاق نار": 16, "حالة طوارئ": 20,
    "breaking": 20, "urgent": 18, "developing": 14, "attack": 16, "strike": 16,
    "explosion": 17, "missile": 16, "earthquake": 18, "tsunami": 22, "emergency": 20,
    "evacuation": 14, "killed": 13, "shooting": 16,
}

IMPORTANCE_TERMS = {
    "رئيس": 7, "رئيس الوزراء": 8, "ملك": 8, "حكومة": 7, "وزارة الخارجية": 9,
    "وزارة الدفاع": 9, "بيان رسمي": 8, "بنك مركزي": 8, "أوبك": 9, "نفط": 7,
    "حرب": 10, "هدنة": 9, "اتفاق": 7, "عقوبات": 8, "انتخابات": 7, "طاقة": 7,
    "president": 7, "prime minister": 8, "king": 8, "government": 7, "foreign ministry": 9,
    "defense ministry": 9, "official statement": 8, "central bank": 8, "opec": 9,
    "oil": 7, "war": 10, "ceasefire": 9, "agreement": 7, "sanctions": 8, "election": 7,
}


def normalize_search_text(text: str) -> str:
    text = html.unescape(str(text or "")).lower()
    replacements = {
        "أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا", "ى": "ي", "ؤ": "و", "ئ": "ي", "ة": "ه",
        "ـ": "", "ً": "", "ٌ": "", "ٍ": "", "َ": "", "ُ": "", "ِ": "", "ّ": "", "ْ": "",
    }
    for a, b in replacements.items():
        text = text.replace(a, b)
    text = re.sub(r"[\u200f\u200e]", "", text)
    text = re.sub(r"[^\w\s\u0600-\u06ff-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def tokenize_query(query: str) -> List[str]:
    normalized = normalize_search_text(query)
    tokens = re.findall(r"[\w\u0600-\u06ff-]+", normalized)
    return [t for t in tokens if len(t) > 1 and t not in ARABIC_STOPWORDS and t not in EN_STOPWORDS]


def normalized_title(title: str) -> str:
    text = normalize_search_text(title)
    text = re.sub(r"\b(?:reuters|bbc|ap|associated press)\b", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()[:240]


def title_fingerprint(title: str) -> str:
    tokens = tokenize_query(title)
    return " ".join(sorted(set(tokens)))[:300]


def similarity_score(a: str, b: str) -> float:
    aa = set(tokenize_query(a))
    bb = set(tokenize_query(b))
    if not aa or not bb:
        return 0.0
    return len(aa & bb) / max(1, len(aa | bb))


def detect_language(text: str) -> str:
    if not text:
        return ""
    ar = len(re.findall(r"[\u0600-\u06ff]", text))
    en = len(re.findall(r"[A-Za-z]", text))
    if ar and ar >= en * 0.35:
        return "ar"
    if en:
        return "en"
    return ""


def extract_domain(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def is_valid_http_url(url: str) -> bool:
    if not url:
        return False
    try:
        p = urlparse(url.strip())
        return p.scheme.lower() in {"http", "https"} and bool(p.netloc)
    except Exception:
        return False


def is_official_source(source: str, domain: str) -> bool:
    s = normalize_search_text(source)
    d = (domain or "").lower()
    if any(x in s for x in OFFICIAL_SOURCE_NAMES):
        return True
    return any(h in d for h in OFFICIAL_DOMAIN_HINTS)


def source_trust_score(source: str, domain: str, official: bool = False) -> float:
    d = (domain or "").lower()
    s = source or ""
    for known, score in MAJOR_NEWS_DOMAINS.items():
        if d == known or d.endswith("." + known):
            return float(score)
    for name, score in SOURCE_NAME_SCORES.items():
        if normalize_search_text(name) in normalize_search_text(s):
            return float(score)
    if official or is_official_source(s, d):
        return 88.0
    if d.endswith(".gov") or ".gov." in d or d.endswith(".mil"):
        return 88.0
    if d:
        return 55.0
    return 45.0


def freshness_score(published_at: str) -> float:
    dt = parse_datetime(published_at)
    if not dt:
        return 0.0
    age_h = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)
    if age_h <= 1:
        return 25.0
    if age_h <= 3:
        return 22.0
    if age_h <= 6:
        return 18.0
    if age_h <= 12:
        return 14.0
    if age_h <= 24:
        return 9.0
    if age_h <= 48:
        return 4.0
    return 0.0


def urgency_score_text(text: str) -> float:
    n = normalize_search_text(text)
    score = 0.0
    for term, weight in URGENT_TERMS.items():
        if normalize_search_text(term) in n:
            score += weight
    return min(35.0, score)


def importance_score_text(text: str) -> float:
    n = normalize_search_text(text)
    score = 0.0
    for term, weight in IMPORTANCE_TERMS.items():
        if normalize_search_text(term) in n:
            score += weight
    return min(22.0, score)


def score_news_item(item: NewsItem, keywords: Optional[Sequence[str]] = None) -> float:
    query_tokens = [normalize_search_text(x) for x in (keywords or []) if x]
    title = normalize_search_text(item.title)
    summary = normalize_search_text(item.summary)
    source = normalize_search_text(item.source)
    region = normalize_search_text(item.region)
    text = normalize_search_text(item.search_text)

    relevance = 0.0
    matched = 0
    for token in query_tokens:
        if token in title:
            relevance += 12
            matched += 1
        elif token in summary:
            relevance += 7
            matched += 1
        elif token in text:
            relevance += 4
            matched += 1
    if query_tokens and matched == len(query_tokens):
        relevance += 16
    if query_tokens and title and all(t in title for t in query_tokens):
        relevance += 10

    freshness = freshness_score(item.published_at)
    urgency = item.urgency_score or urgency_score_text(f"{title} {summary}")
    importance = importance_score_text(f"{title} {summary}")
    trust = min(100.0, max(0.0, item.trust_score)) * 0.32
    official_bonus = 9.0 if item.official else 0.0
    verification_bonus = min(15.0, max(0, item.independent_sources - 1) * 7.5)
    source_bonus = 3.0 if source else 0.0
    region_bonus = 2.0 if region else 0.0

    item.relevance_score = relevance
    item.urgency_score = urgency
    return trust + freshness + urgency + importance + relevance + official_bonus + verification_bonus + source_bonus + region_bonus


def parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    return None


def detect_region(text: str) -> str:
    n = normalize_search_text(text)
    for region, countries in REGIONS.items():
        for country in countries:
            if normalize_search_text(country) in n:
                return region
    return ""


# ============================================================
# QUERY EXPANSION
# ============================================================

async def translate_query_to_english(query: str, session: Optional[aiohttp.ClientSession] = None) -> str:
    """Best-effort free translation used only to widen discovery.

    Failure is intentionally non-fatal; Arabic search remains authoritative.
    """
    if not query or not re.search(r"[\u0600-\u06ff]", query):
        return query.strip()
    tokens = tokenize_query(query)
    mapped: List[str] = []
    for token in tokens:
        for ar, aliases in QUERY_ALIASES.items():
            if normalize_search_text(ar) == token:
                mapped.extend(aliases[:2])
                break
        else:
            for ar, en in COUNTRY_EN.items():
                if normalize_search_text(ar) == token:
                    mapped.append(en)
                    break
    if mapped:
        return " ".join(dict.fromkeys(mapped))

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()
    try:
        endpoint = "https://translate.googleapis.com/translate_a/single"
        params = {
            "client": "gtx", "sl": "ar", "tl": "en", "dt": "t", "q": query,
        }
        async with session.get(endpoint, params=params, timeout=aiohttp.ClientTimeout(total=6)) as response:
            if response.status != 200:
                return query
            data = await response.json(content_type=None)
            parts = data[0] if isinstance(data, list) and data else []
            translated = " ".join(str(p[0]) for p in parts if isinstance(p, list) and p and p[0])
            return translated.strip() or query
    except Exception:
        return query
    finally:
        if own_session:
            await session.close()


def expand_search_query(query: str) -> Dict[str, List[str]]:
    raw = str(query or "").strip()
    ar_tokens = tokenize_query(raw)
    english: List[str] = []
    arabic: List[str] = [raw] if raw else []

    normalized_aliases = {normalize_search_text(k): v for k, v in QUERY_ALIASES.items()}
    normalized_countries = {normalize_search_text(k): v for k, v in COUNTRY_EN.items()}
    for token in ar_tokens:
        english.extend(normalized_aliases.get(token, []))
        if token in normalized_countries:
            english.append(normalized_countries[token])

    if english:
        english = list(dict.fromkeys(english))
        joined = " ".join(english[:6])
        english_queries = [joined]
        if len(english) >= 2:
            english_queries.append('"' + '" "'.join(english[:3]) + '"')
    else:
        english_queries = [raw] if raw else []

    if raw and len(ar_tokens) > 1:
        arabic.append(" OR ".join(f'"{t}"' for t in ar_tokens[:6]))
    if raw and len(ar_tokens) >= 2:
        arabic.append(" ".join(f'"{t}"' for t in ar_tokens[:4]))

    return {
        "arabic": list(dict.fromkeys(arabic))[:MAX_ONLINE_QUERIES],
        "english": list(dict.fromkeys(english_queries))[:MAX_ONLINE_QUERIES],
    }


# ============================================================
# RSS FETCHING
# ============================================================

async def fetch_rss_feed(
    session: aiohttp.ClientSession,
    source_name: str,
    url: str,
    category: str = "direct",
    region: str = "",
    language: str = "",
) -> List[NewsItem]:
    items: List[NewsItem] = []
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
            allow_redirects=True,
            headers={"User-Agent": "Global-Intel-Bot/1.0 news-engine"},
        ) as response:
            if response.status != 200:
                log.warning("RSS [%s] returned HTTP %s", source_name, response.status)
                return items
            content = await response.text(errors="ignore")
        parsed = feedparser.parse(content)
        for entry in parsed.entries[:MAX_FEED_ITEMS]:
            title = entry.get("title", "") or ""
            link = entry.get("link", "") or ""
            if not title:
                continue
            published = entry.get("published", "") or entry.get("updated", "") or ""
            summary = entry.get("summary", "") or entry.get("description", "") or ""
            detected_region = region or detect_region(f"{title} {summary}")
            domain = extract_domain(link)
            official = is_official_source(source_name, domain)
            item = NewsItem(
                title=title,
                source=source_name,
                url=link,
                published_at=published,
                category=category,
                summary=summary,
                region=detected_region,
                language=language or detect_language(title),
                official=official,
                source_domain=domain,
            )
            item.trust_score = source_trust_score(source_name, domain, official)
            item.urgency_score = urgency_score_text(f"{title} {summary}")
            items.append(item)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning("Error fetching RSS [%s]: %s", source_name, exc)
    return items


# ============================================================
# GOOGLE NEWS RSS
# ============================================================

GOOGLE_NEWS_BASE = "https://news.google.com/rss/search?q={query}&hl={hl}&gl={gl}&ceid={ceid}"


def google_news_url(query: str, language: str) -> str:
    if language == "en":
        return GOOGLE_NEWS_BASE.format(query=urllib.parse.quote_plus(query), hl="en-US", gl="US", ceid="US:en")
    return GOOGLE_NEWS_BASE.format(query=urllib.parse.quote_plus(query), hl="ar", gl="SA", ceid="SA:ar")


async def fetch_google_news_topic(
    session: aiohttp.ClientSession,
    query: str,
    category: str = "general",
    region: str = "",
    language: str = "ar",
) -> List[NewsItem]:
    items: List[NewsItem] = []
    try:
        async with session.get(
            google_news_url(query, language),
            timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
            allow_redirects=True,
            headers={"User-Agent": "Global-Intel-Bot/1.0"},
        ) as response:
            if response.status != 200:
                return items
            content = await response.text(errors="ignore")
        parsed = feedparser.parse(content)
        for entry in parsed.entries[:MAX_FEED_ITEMS]:
            raw_title = entry.get("title", "") or ""
            link = entry.get("link", "") or ""
            if not raw_title:
                continue
            source = "تغطية إخبارية"
            source_data = entry.get("source")
            if isinstance(source_data, dict):
                source = source_data.get("title", source) or source
            title = raw_title.rsplit(" - ", 1)[0].strip() if " - " in raw_title else raw_title.strip()
            published = entry.get("published", "") or entry.get("updated", "") or ""
            summary = entry.get("summary", "") or entry.get("description", "") or ""
            detected_region = region or detect_region(f"{title} {summary} {source}")
            domain = extract_domain(link)
            official = is_official_source(source, domain)
            item = NewsItem(
                title=title,
                original_title=title,
                source=source,
                url=link,
                published_at=published,
                category=category,
                summary=summary,
                region=detected_region,
                language=language,
                official=official,
                source_domain=domain,
            )
            item.trust_score = source_trust_score(source, domain, official)
            item.urgency_score = urgency_score_text(f"{title} {summary}")
            items.append(item)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning("Error fetching Google News [%s]: %s", query[:100], exc)
    return items


# ============================================================
# GDELT DISCOVERY
# ============================================================

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"


async def fetch_gdelt_news(
    session: aiohttp.ClientSession,
    query: str,
    category: str = "general",
    region: str = "",
    max_records: int = MAX_GDELT_RECORDS,
) -> List[NewsItem]:
    if not query:
        return []
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": str(min(200, max_records)),
        "timespan": "24h",
        "sort": "datedesc",
    }
    items: List[NewsItem] = []
    try:
        async with session.get(
            GDELT_DOC_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
            headers={"User-Agent": "Global-Intel-Bot/1.0"},
        ) as response:
            if response.status != 200:
                return items
            data = await response.json(content_type=None)
        articles = data.get("articles", []) if isinstance(data, dict) else []
        for article in articles[:max_records]:
            title = article.get("title", "") or ""
            url = article.get("url", "") or ""
            if not title or not is_valid_http_url(url):
                continue
            source = article.get("domain", "") or article.get("sourcecountry", "") or "GDELT"
            published = article.get("seendate", "") or article.get("date", "") or ""
            if len(published) == 14 and published.isdigit():
                try:
                    dt = datetime.strptime(published, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                    published = dt.strftime("%a, %d %b %Y %H:%M:%S +0000")
                except Exception:
                    pass
            language = article.get("language", "") or "en"
            domain = extract_domain(url)
            official = is_official_source(source, domain)
            item = NewsItem(
                title=title,
                original_title=title,
                source=source,
                url=url,
                published_at=published,
                category=category,
                summary=article.get("socialimage", "") or "",
                region=region or detect_region(title),
                language=language,
                official=official,
                source_domain=domain,
            )
            item.trust_score = source_trust_score(source, domain, official)
            item.urgency_score = urgency_score_text(title)
            items.append(item)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning("Error fetching GDELT [%s]: %s", query[:100], exc)
    return items


# ============================================================
# DEDUPLICATION / EVENT VERIFICATION
# ============================================================

def same_event(a: NewsItem, b: NewsItem) -> bool:
    if not a.title or not b.title:
        return False
    na = normalized_title(a.title)
    nb = normalized_title(b.title)
    if na == nb:
        return True
    sim = similarity_score(na, nb)
    if sim >= 0.72:
        return True
    # Strong overlap in the core title plus same region is usually the same event.
    if sim >= 0.55 and a.region and a.region == b.region:
        return True
    return False


def deduplicate_news(items: Sequence[NewsItem]) -> List[NewsItem]:
    # First collapse exact/near-identical URLs and normalized titles.
    unique: List[NewsItem] = []
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    for item in items:
        if not item.title:
            continue
        url_key = item.url.strip().lower() if item.url else ""
        title_key = normalized_title(item.title)
        if url_key and url_key in seen_urls:
            continue
        if title_key and title_key in seen_titles:
            continue
        if url_key:
            seen_urls.add(url_key)
        if title_key:
            seen_titles.add(title_key)
        unique.append(item)

    # Then cluster genuinely similar headlines into events.
    clusters: List[List[NewsItem]] = []
    for item in unique:
        placed = False
        for cluster in clusters:
            if same_event(item, cluster[0]):
                cluster.append(item)
                placed = True
                break
        if not placed:
            clusters.append([item])

    output: List[NewsItem] = []
    for cluster in clusters:
        # Prefer Arabic presentation when two sources are otherwise comparable;
        # otherwise prefer the stronger/fresher source.
        cluster.sort(
            key=lambda x: (
                x.trust_score + (4 if x.language == "ar" else 0),
                freshness_score(x.published_at),
                1 if x.official else 0,
                len(x.summary),
            ),
            reverse=True,
        )
        primary = cluster[0]
        trusted_domains = {
            c.source_domain for c in cluster
            if c.source_domain and c.trust_score >= 70
        }
        primary.independent_sources = max(1, len(trusted_domains))
        primary.event_key = title_fingerprint(primary.title) or normalized_title(primary.title)
        output.append(primary)
    return output


def rank_news(items: Sequence[NewsItem], keywords: Optional[Sequence[str]] = None) -> List[NewsItem]:
    ranked = list(items)
    ranked.sort(key=lambda x: score_news_item(x, keywords), reverse=True)
    return ranked


# ============================================================
# GLOBAL COLLECTION
# ============================================================

def _rotated_countries() -> List[Tuple[str, str]]:
    flat: List[Tuple[str, str]] = []
    for region, countries in REGIONS.items():
        for country in countries:
            flat.append((region, country))
    if not flat:
        return []
    slot = int(time.time() // ROTATION_WINDOW_SECONDS)
    offset = (slot * 7) % len(flat)
    return flat[offset:] + flat[:offset]


def _country_queries(limit: int = 10) -> List[Tuple[str, str]]:
    selected = _rotated_countries()[:limit]
    result: List[Tuple[str, str]] = []
    for region, country in selected:
        en = COUNTRY_EN.get(country, country)
        result.append((f'"{country}" OR "{en}"', region))
    return result


async def _gather_limited(coros: Iterable[Any], limit: int = COLLECTION_CONCURRENCY) -> List[Any]:
    semaphore = asyncio.Semaphore(limit)

    async def run(coro: Any) -> Any:
        async with semaphore:
            return await coro

    tasks = [asyncio.create_task(run(c)) for c in coros]
    if not tasks:
        return []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    clean: List[Any] = []
    for result in results:
        if isinstance(result, Exception):
            log.warning("Collection task failed: %s", result)
            continue
        clean.append(result)
    return clean


async def collect_news(max_items: int = 150) -> List[NewsItem]:
    timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks: List[Any] = []
        for name, url in TRUSTED_FEEDS.items():
            tasks.append(fetch_rss_feed(session, name, url))
        for name, url in ADDITIONAL_TRUSTED_FEEDS.items():
            tasks.append(fetch_rss_feed(session, name, url, category="official"))

        # Global discovery: Arabic + English channels.
        for query, category in GLOBAL_QUERIES:
            tasks.append(fetch_google_news_topic(session, query, category=category, language="ar"))
        for query, category in GLOBAL_QUERIES_EN:
            tasks.append(fetch_google_news_topic(session, query, category=category, language="en"))
            tasks.append(fetch_gdelt_news(session, query, category=category))

        # Rotating country coverage prevents a huge burst while ensuring the
        # full country list is covered across repeated collection cycles.
        for query, region in _country_queries(limit=10):
            tasks.append(fetch_google_news_topic(session, query, category="regional", region=region, language="ar"))
            tasks.append(fetch_google_news_topic(session, query, category="regional", region=region, language="en"))
            tasks.append(fetch_gdelt_news(session, query, category="regional", region=region, max_records=18))

        batches = await _gather_limited(tasks)

    all_items: List[NewsItem] = []
    for batch in batches:
        if isinstance(batch, list):
            all_items.extend(batch)

    unique = deduplicate_news(all_items)
    ranked = rank_news(unique)
    return ranked[:max_items]


# ============================================================
# ONLINE SEARCH
# ============================================================

async def search_news_online(query: str, max_results: int = 25) -> List[NewsItem]:
    query = str(query or "").strip()
    if not query:
        return []

    expansion = expand_search_query(query)
    arabic_queries = expansion.get("arabic", [])
    english_queries = expansion.get("english", [])

    timeout = aiohttp.ClientTimeout(total=SEARCH_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=16, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks: List[Any] = []
        for q in arabic_queries[:4]:
            tasks.append(fetch_google_news_topic(session, q, category="search", language="ar"))
        for q in english_queries[:4]:
            tasks.append(fetch_google_news_topic(session, q, category="search", language="en"))
        for q in (arabic_queries[:2] + english_queries[:2]):
            tasks.append(fetch_gdelt_news(session, q, category="search", max_records=35))

        # For Arabic free-form queries, add a best-effort English translation.
        if re.search(r"[\u0600-\u06ff]", query):
            translated = await translate_query_to_english(query, session)
            if translated and translated not in english_queries:
                tasks.append(fetch_google_news_topic(session, translated, category="search", language="en"))
                tasks.append(fetch_gdelt_news(session, translated, category="search", max_records=35))

        batches = await _gather_limited(tasks, limit=8)

    collected: List[NewsItem] = []
    for batch in batches:
        if isinstance(batch, list):
            collected.extend(batch)

    unique = deduplicate_news(collected)
    ranked = rank_news(unique, tokenize_query(query) + expansion.get("english", []))
    return ranked[:max_results]


async def search_news(items: Sequence[NewsItem], keywords: Any, max_results: int = 25) -> List[NewsItem]:
    if isinstance(keywords, str):
        query = keywords.strip()
        tokens = tokenize_query(query)
    else:
        tokens = [str(x) for x in (keywords or [])]
        query = " ".join(tokens)

    if not query:
        return []

    local: List[NewsItem] = []
    for item in items or []:
        score = score_news_item(item, tokens)
        if score <= 0:
            continue
        n = normalize_search_text(item.search_text)
        if any(normalize_search_text(t) in n for t in tokens):
            local.append(item)

    return rank_news(local, tokens)[:max_results]


async def hybrid_search_news(
    items: Sequence[NewsItem],
    query: str,
    max_results: int = 25,
) -> List[NewsItem]:
    query = str(query or "").strip()
    if not query:
        return []

    tokens = tokenize_query(query)
    local = await search_news(items, query, max_results=max_results)

    # Always perform online discovery. This is intentional: manual search must
    # not be trapped inside the currently cached topic.
    online = await search_news_online(query, max_results=max_results)

    merged: List[NewsItem] = []
    seen_urls: set[str] = set()
    for item in list(local) + list(online):
        key = item.url or (item.source_domain + "|" + normalized_title(item.title))
        if key in seen_urls:
            continue
        seen_urls.add(key)
        merged.append(item)

    verified = deduplicate_news(merged)
    ranked = rank_news(verified, tokens)
    return ranked[:max_results]


# ============================================================
# AI CONTEXT / SEARCH URL
# ============================================================

def build_ai_context(items: Sequence[NewsItem], limit: int = 12) -> str:
    lines: List[str] = []
    for idx, item in enumerate(list(items)[:limit], 1):
        verification = f"{item.independent_sources} مصادر مستقلة" if item.independent_sources > 1 else "مصدر واحد"
        lines.append(
            f"{idx}. {item.title}\n"
            f"المصدر: {item.source} | الثقة: {item.trust_score:.0f}/100 | {verification}\n"
            f"التصنيف: {item.category} | المنطقة: {item.region or 'غير محددة'}\n"
            f"التاريخ: {item.published_at or 'غير محدد'}\n"
            f"الرابط: {item.url}"
        )
    return "\n\n".join(lines)


def build_search_url(query: str, language: str = "ar") -> str:
    if language == "en":
        return google_news_url(query, "en")
    return google_news_url(query, "ar")


# ============================================================
# COMPATIBILITY HELPERS
# ============================================================

async def get_fresh_news(max_items: int = 150) -> List[NewsItem]:
    return await collect_news(max_items=max_items)


__all__ = [
    "NewsItem",
    "TRUSTED_FEEDS",
    "REGIONS",
    "GLOBAL_QUERIES",
    "GLOBAL_QUERIES_EN",
    "collect_news",
    "get_fresh_news",
    "detect_region",
    "is_valid_http_url",
    "fetch_rss_feed",
    "fetch_google_news_topic",
    "fetch_gdelt_news",
    "search_news",
    "search_news_online",
    "hybrid_search_news",
    "build_ai_context",
    "build_search_url",
    "score_news_item",
    "rank_news",
    "deduplicate_news",
    "expand_search_query",
    "translate_query_to_english",
]
