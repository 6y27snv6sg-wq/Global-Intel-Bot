import asyncio
import html
import logging
import os
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
    "reuters.com": 96, "apnews.com": 95, "bbc.com": 92, "bbc.co.uk": 92,
    "aljazeera.net": 88, "skynewsarabia.com": 86, "alarabiya.net": 86,
    "asharq.com": 86, "france24.com": 85, "dw.com": 84, "cnbcarabia.com": 82,
    "bloomberg.com": 91, "ft.com": 91, "wsj.com": 90, "nytimes.com": 88,
    "spa.gov.sa": 94, "eia.gov": 94, "nasa.gov": 94, "who.int": 94,
    "un.org": 94, "nato.int": 94, "opec.org": 94,
}

OFFICIAL_DOMAIN_HINTS = (
    ".gov", ".gob.", ".go.", ".mil", ".mod.", "government", "gov.uk",
    "bund.de", "admin.ch", "europa.eu", "un.org", "nato.int", "who.int",
    "imf.org", "worldbank.org", "ecb.europa.eu", "bis.org", "opec.org",
)

REGIONS = {
    "الشرق الأوسط": ["السعودية","الإمارات","قطر","الكويت","البحرين","عمان","اليمن","العراق","إيران","سوريا","لبنان","الأردن","فلسطين","إسرائيل","مصر","تركيا"],
    "آسيا": ["الصين","اليابان","الهند","كوريا الجنوبية","كوريا الشمالية","إندونيسيا","ماليزيا","سنغافورة","تايلاند","فيتنام","الفلبين","باكستان","بنغلاديش","تايوان","أفغانستان","نيبال"],
    "أوروبا": ["بريطانيا","المملكة المتحدة","فرنسا","ألمانيا","إيطاليا","إسبانيا","البرتغال","هولندا","بلجيكا","سويسرا","النمسا","بولندا","أوكرانيا","روسيا","السويد","النرويج","الدنمارك","فنلندا","اليونان"],
    "أفريقيا": ["المغرب","الجزائر","تونس","ليبيا","السودان","إثيوبيا","كينيا","نيجيريا","جنوب أفريقيا","غانا","تنزانيا","الصومال","السنغال","أنغولا","النيجر","مالي","تشاد"],
    "أمريكا الشمالية": ["الولايات المتحدة","أمريكا","كندا","المكسيك","كوبا","بنما"],
    "أمريكا الجنوبية": ["البرازيل","الأرجنتين","تشيلي","كولومبيا","بيرو","فنزويلا","الإكوادور","بوليفيا","أوروغواي","باراغواي"],
}

ECON_TERMS = [
    "اقتصاد","اقتصادي","أسواق","سوق","أسهم","سهم","بورصة","الذهب","فائدة",
    "عملات","دولار","بيتكوين","تداول","نفط","أوبك","خام","تضخم","برنت",
    "طاقة","غاز","استثمار","سندات","ميزانية","ناتج محلي","بنك مركزي",
    "صادرات","واردات","أسعار المستهلك","أسعار المنتجين","استحواذ","أرباح",
]
ECON_EXCLUDE = [
    "إنقاذ","انقاذ","زلزال","وفاة","تعازي","يعزي","يعزّي","حادث","غرق",
    "انتشال","إنقاذ عمال","منجم","نفق","فيضانات","طقس",
]

SECURITY_TERMS = [
    "عسكري","جيش","قوات","دفاع","أمن","الأمن القومي","تسليح","أسلحة","سلاح",
    "صاروخ","صواريخ","قصف","غارة","غارات","هجوم","اشتباك","مناورات","قاعدة عسكرية",
    "طيران عسكري","مقاتلات","طائرات مسيرة","ذخائر","دفاع جوي","عملية عسكرية",
    "عمليات عسكرية","قوات خاصة","استهداف","إطلاق النار","قتال","معارك","أسطول",
]
SECURITY_SOCIAL_EXCLUDE = [
    "يعزي","يعزّي","تعازي","وفاة والده","وفاة والدته","وفاة شقيق","وفاة عمه",
    "تهنئة","ترقية","تعيين","استقبال","زيارة تفقدية","احتفال",
]

OFFICIAL_TERMS = [
    "بيان رسمي","تصريح رسمي","بيان صحفي","المتحدث الرسمي","المتحدث باسم",
    "مصدر مسؤول","أعلنت الوزارة","أعلن الوزير","قالت الوزارة","قال الوزير",
    "وزارة الخارجية","وزارة الدفاع","وزارة الداخلية","رئاسة الوزراء",
    "الديوان الملكي","الحكومة تعلن","الحكومة تؤكد","الرئاسة تعلن","الرئاسة تؤكد",
    "foreign ministry","ministry said","government said","official statement",
    "spokesperson","state department",
]

URGENT_TERMS = [
    "عاجل","طارئ","هجوم","انفجار","قصف","صاروخ","زلزال","اشتباك","غارة",
    "إخلاء","حالة طوارئ","تحذير عاجل","استهداف","غارات","إطلاق النار",
]

COUNTRY_EN = {
    "السعودية":"Saudi Arabia","الإمارات":"United Arab Emirates","قطر":"Qatar",
    "الكويت":"Kuwait","البحرين":"Bahrain","عمان":"Oman","اليمن":"Yemen",
    "العراق":"Iraq","إيران":"Iran","سوريا":"Syria","لبنان":"Lebanon",
    "الأردن":"Jordan","فلسطين":"Palestine","إسرائيل":"Israel","مصر":"Egypt",
    "تركيا":"Turkey","الصين":"China","اليابان":"Japan","الهند":"India",
    "روسيا":"Russia","أوكرانيا":"Ukraine","بريطانيا":"United Kingdom",
    "المملكة المتحدة":"United Kingdom","فرنسا":"France","ألمانيا":"Germany",
    "إيطاليا":"Italy","إسبانيا":"Spain","الولايات المتحدة":"United States",
    "أمريكا":"United States","كندا":"Canada","المكسيك":"Mexico",
    "البرازيل":"Brazil","الأرجنتين":"Argentina","كولومبيا":"Colombia",
    "فنزويلا":"Venezuela","السودان":"Sudan","نيبال":"Nepal",
}

QUERY_ALIASES = {
    "نفط":["oil","crude oil","brent","opec"], "النفط":["oil","crude oil","brent","opec"],
    "طاقة":["energy","oil","gas"], "غاز":["gas","natural gas","lng"],
    "اقتصاد":["economy","economic"], "أسواق":["markets","market"],
    "أسهم":["stocks","equities"], "بورصة":["stock exchange","equities"],
    "تضخم":["inflation"], "فائدة":["interest rates","rate decision"],
    "ذهب":["gold"], "دولار":["dollar","USD"], "بيتكوين":["bitcoin","crypto"],
}

def normalize_text(value):
    text = str(value or "").lower().strip()
    text = re.sub(r"[\u064B-\u065F\u0670]", "", text)
    for old, new in {"أ":"ا","إ":"ا","آ":"ا","ى":"ي","ة":"ه","ؤ":"و","ئ":"ي"}.items():
        text = text.replace(old, new)
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s\u0600-\u06FF-]", " " , text)).strip()

def tokenize(value):
    return {x for x in normalize_text(value).split() if len(x) > 2}

def normalized_title(value):
    return normalize_text(value)

def title_fingerprint(value):
    return " ".join(sorted(tokenize(value)))

def similarity_score(a, b):
    sa, sb = tokenize(a), tokenize(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(1, len(sa | sb))

def same_event(a, b):
    na, nb = normalized_title(a.title), normalized_title(b.title)
    if not na or not nb:
        return False
    if na == nb:
        return True
    sim = similarity_score(na, nb)
    if sim >= 0.78:
        return True
    if sim >= 0.58 and a.region and a.region == b.region:
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
        self.summary = re.sub(r"<[^>]+>", " ", self.summary or "").strip()
        self.domain = (self.domain or urlparse(self.url).netloc or "").lower().replace("www.", "")
        combined = f"{self.title} {self.summary}"
        self.region = self.region or detect_region(combined)
        self.official = self.official or is_official_source(self.source, self.domain)
        self.trust_score = max(self.trust_score, source_trust(self.source, self.domain))
        self.urgency_score = score_terms(combined, URGENT_TERMS)
        self.search_text = normalize_text(
            f"{self.title} {self.original_title} {self.summary} {self.source} {self.region}"
        )

def detect_region(text):
    n = normalize_text(text)
    for region, places in REGIONS.items():
        if any(normalize_text(place) in n for place in places):
            return region
    return ""

def is_official_source(source, domain):
    s = normalize_text(source)
    d = normalize_text(domain)
    return any(x in s or x in d for x in [normalize_text(v) for v in OFFICIAL_TERMS]) or any(
        hint in domain for hint in OFFICIAL_DOMAIN_HINTS
    )

def source_trust(source, domain):
    for key, score in MAJOR_NEWS_DOMAINS.items():
        if domain.endswith(key):
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
    return sum(1 for term in terms if normalize_text(term) in n)

def classify_item(item):
    title = normalize_text(item.title)
    summary = normalize_text(item.summary)
    text = f"{title} {summary}"
    title_score = lambda terms: sum(3 if normalize_text(t) in title else 0 for t in terms)
    body_score = lambda terms: sum(1 if normalize_text(t) in text else 0 for t in terms)

    econ = title_score(ECON_TERMS) + body_score(ECON_TERMS)
    if any(normalize_text(x) in text for x in ECON_EXCLUDE):
        econ -= 5

    sec = title_score(SECURITY_TERMS) + body_score(SECURITY_TERMS)
    if any(normalize_text(x) in title for x in SECURITY_SOCIAL_EXCLUDE):
        sec -= 12

    official = title_score(OFFICIAL_TERMS) + body_score(OFFICIAL_TERMS)
    if item.official:
        official += 5

    urgent = title_score(URGENT_TERMS) * 2 + body_score(URGENT_TERMS)

    scores = {"econ": econ, "secu": sec, "forg": official, "urg": urgent}
    best = max(scores, key=scores.get)
    if scores[best] >= 4:
        item.category = best
    else:
        item.category = "general"
    return item

def is_topic_match(item, topic_key):
    title = normalize_text(item.title)
    summary = normalize_text(item.summary)
    text = f"{title} {summary}"

    if topic_key == "econ":
        positive = score_terms(title, ECON_TERMS) * 3 + score_terms(summary, ECON_TERMS)
        negative = score_terms(text, ECON_EXCLUDE) * 5
        return positive >= 4 and positive > negative

    if topic_key == "secu":
        positive = score_terms(title, SECURITY_TERMS) * 3 + score_terms(summary, SECURITY_TERMS)
        negative = score_terms(title, SECURITY_SOCIAL_EXCLUDE) * 8
        return positive >= 5 and positive > negative

    if topic_key == "forg":
        official_hits = score_terms(title, OFFICIAL_TERMS) * 3 + score_terms(summary, OFFICIAL_TERMS)
        return official_hits >= 4 or (item.official and official_hits >= 2)

    if topic_key == "urg":
        return score_terms(title, URGENT_TERMS) >= 1 or (
            score_terms(summary, URGENT_TERMS) >= 2
        )

    if topic_key == "gulf":
        gulf = REGIONS["الشرق الأوسط"]
        return any(normalize_text(x) in text for x in gulf)

    if topic_key == "wrld":
        return bool(item.region) or any(
            normalize_text(x) in text
            for x in ["امريكا","الولايات المتحده","اوروبا","الصين","روسيا","اوكرانيا","الهند","اليابان"]
        )

    return False

def deduplicate_news(items):
    unique = []
    seen_urls = set()
    for item in items:
        url_key = item.url.strip().lower()
        if url_key and url_key in seen_urls:
            continue
        if url_key:
            seen_urls.add(url_key)
        duplicate = False
        for existing in unique:
            if same_event(item, existing):
                if (
                    item.trust_score > existing.trust_score
                    or (
                        item.published and existing.published
                        and item.published > existing.published
                    )
                ):
                    unique.remove(existing)
                    unique.append(item)
                duplicate = True
                break
        if not duplicate:
            unique.append(item)
    return unique

def parse_date(value):
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def parse_entry(entry, source, category="general"):
    title = html.unescape(str(entry.get("title", "") or "").strip())
    url = str(entry.get("link", "") or "").strip()
    if not title or not url:
        return None
    summary = html.unescape(str(entry.get("summary", "") or entry.get("description", "") or ""))
    published = parse_date(entry.get("published") or entry.get("updated") or "")
    item = NewsItem(title=title, original_title=title, url=url, source=source,
                    summary=summary, published=published, category=category)
    return classify_item(item)

async def fetch_feed(session, source, url):
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
            headers={"User-Agent": "Global-Intel-Bot/2.0"},
        ) as response:
            if response.status != 200:
                return []
            data = await response.read()
        parsed = feedparser.parse(data)
        items = []
        for entry in parsed.entries[:MAX_FEED_ITEMS]:
            item = parse_entry(entry, source)
            if item:
                items.append(item)
        return items
    except Exception:
        log.exception("Feed failed: %s", source)
        return []

def google_news_url(query):
    q = urllib.parse.quote_plus(query)
    return f"https://news.google.com/rss/search?q={q}&hl=ar&gl=SA&ceid=SA:ar"

async def search_news_online(query, max_results=25):
    queries = [query]
    normalized = normalize_text(query)
    for key, aliases in QUERY_ALIASES.items():
        if normalize_text(key) in normalized:
            queries.extend(aliases[:3])
    queries = list(dict.fromkeys(queries))[:MAX_ONLINE_QUERIES]

    async with aiohttp.ClientSession() as session:
        groups = await asyncio.gather(
            *(fetch_feed(session, f"بحث: {q}", google_news_url(q)) for q in queries),
            return_exceptions=True,
        )
    items = []
    for group in groups:
        if isinstance(group, list):
            items.extend(group)
    return rank_search_results(deduplicate_news(items), query)[:max_results]

def rank_search_results(items, query):
    q_tokens = tokenize(query)
    ranked = []
    for item in items:
        title_tokens = tokenize(item.title)
        summary_tokens = tokenize(item.summary)
        title_hits = len(q_tokens & title_tokens)
        summary_hits = len(q_tokens & summary_tokens)
        exact_phrase = normalize_text(query) in normalize_text(item.title)
        peripheral = len(q_tokens & tokenize(item.search_text))
        score = (
            title_hits * 12
            + summary_hits * 3
            + peripheral * 0.5
            + (25 if exact_phrase else 0)
            + item.trust_score * 0.04
        )
        item.relevance_score = score
        ranked.append((score, item))
    ranked.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in ranked]

async def search_news(items, query, max_results=25):
    return rank_search_results(deduplicate_news(items), query)[:max_results]

async def hybrid_search_news(items, query, max_results=25):
    local = await search_news(items, query, max_results)
    online = await search_news_online(query, max_results)
    merged = deduplicate_news(local + online)
    ranked = rank_search_results(merged, query)
    # لا نعرض نتائج لا تحمل أي صلة فعلية بالاستعلام.
    q_tokens = tokenize(query)
    filtered = []
    for item in ranked:
        title_hits = len(q_tokens & tokenize(item.title))
        summary_hits = len(q_tokens & tokenize(item.summary))
        if title_hits or summary_hits or normalize_text(query) in item.search_text:
            filtered.append(item)
    return filtered[:max_results]

async def collect_news(max_items=150):
    feeds = {**TRUSTED_FEEDS, **ADDITIONAL_TRUSTED_FEEDS}
    async with aiohttp.ClientSession() as session:
        groups = await asyncio.gather(
            *(fetch_feed(session, source, url) for source, url in feeds.items()),
            return_exceptions=True,
        )
    items = []
    for group in groups:
        if isinstance(group, list):
            items.extend(group)

    # اكتشاف إضافي عام، مع الحفاظ على سقف الاستهلاك.
    discovery_queries = [
        "أهم الأخبار العالمية",
        "السعودية اقتصاد أسواق نفط",
        "الشرق الأوسط أمن دفاع",
        "بيانات رسمية حكومات",
        "world news economy security",
    ]
    try:
        discovery = await asyncio.gather(
            *(search_news_online(q, 15) for q in discovery_queries),
            return_exceptions=True,
        )
        for group in discovery:
            if isinstance(group, list):
                items.extend(group)
    except Exception:
        log.exception("Discovery failed.")

    items = deduplicate_news(items)
    items.sort(
        key=lambda x: (
            float(x.relevance_score or 0),
            float(x.trust_score or 0),
            x.published.timestamp() if x.published else 0,
        ),
        reverse=True,
    )
    return items[:max_items]

def build_ai_context(items):
    lines = []
    for i, item in enumerate(items, 1):
        published = item.published.isoformat() if item.published else "غير متاح"
        lines.append(
            f"{i}. العنوان: {item.title}\n"
            f"المصدر: {item.source}\n"
            f"المنطقة: {item.region or 'غير محددة'}\n"
            f"التاريخ: {published}\n"
            f"الملخص: {item.summary[:700]}"
        )
    return "\n\n".join(lines)
