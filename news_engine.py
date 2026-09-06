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

    # High-value international broadcasters / TV newsrooms.
    # These feeds are official publisher feeds; Telegram keeps only the
    # translated headline and links to the original article.
    "CNA": "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml",
    "Euronews": "https://feeds.euronews.com/rss/en/home",
    "Africanews": "https://www.africanews.com/feed/rss",
}

MAJOR_NEWS_DOMAINS = {
    "reuters.com": 96, "apnews.com": 95, "bbc.com": 92, "bbc.co.uk": 92,
    "aljazeera.net": 88, "skynewsarabia.com": 86, "alarabiya.net": 86,
    "asharq.com": 86, "france24.com": 85, "dw.com": 84, "cnbcarabia.com": 82,
    "bloomberg.com": 91, "ft.com": 91, "wsj.com": 90, "nytimes.com": 88,
    "spa.gov.sa": 94, "eia.gov": 94, "nasa.gov": 94, "who.int": 94,
    "un.org": 94, "nato.int": 94, "opec.org": 94,

    # High-value regional/international broadcasters.
    "channelnewsasia.com": 90, "euronews.com": 88,
    "africanews.com": 88, "nhk.or.jp": 88, "abc.net.au": 88,
    "cbc.ca": 88, "skynews.com": 88,
}

OFFICIAL_DOMAIN_HINTS = (
    ".gov", ".gob.", ".go.", ".mil", ".mod.", "government", "gov.uk",
    "bund.de", "admin.ch", "europa.eu", "un.org", "nato.int", "who.int",
    "imf.org", "worldbank.org", "ecb.europa.eu", "bis.org", "opec.org",
)

# Explicit official government / foreign-ministry domains.
# These receive the highest source priority and are never treated as ordinary media.
OFFICIAL_SOURCE_DOMAINS = {
    "mofa.gov.sa", "mofa.gov.ae", "mofa.gov.qa", "mofa.gov.kw",
    "fm.gov.om", "mofa.gov.bh", "mfa.gov.eg", "mfa.gov.tr",
    "state.gov", "gov.uk", "diplomatie.gouv.fr", "auswaertiges-amt.de",
    "esteri.it", "exteriores.gob.es", "mfa.gov.cn", "mofa.go.jp",
    "mea.gov.in", "mid.ru", "mfa.gov.ua", "dfat.gov.au",
    "international.gc.ca", "mofa.gov.kr", "mofa.gov.np",
}

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
    "oil","crude","opec","brent","energy","natural gas","lng","economy",
    "economic","markets","market","stocks","equities","stock exchange",
    "inflation","interest rates","gold","dollar","usd","bitcoin","crypto",
    "investment","bonds","budget","gdp","central bank","exports","imports",
    "earnings","acquisition",
]

ECON_EXCLUDE = [
    "إنقاذ","انقاذ","زلزال","وفاة","تعازي","يعزي","يعزّي","حادث","غرق",
    "انتشال","إنقاذ عمال","منجم","نفق","فيضانات","طقس",
    "rescue","earthquake","death","funeral","accident","drowning","flood",
    "weather",
]

SECURITY_TERMS = [
    "عسكري","جيش","قوات","دفاع","أمن","الأمن القومي","تسليح","أسلحة","سلاح",
    "صاروخ","صواريخ","قصف","غارة","غارات","هجوم","اشتباك","مناورات","قاعدة عسكرية",
    "طيران عسكري","مقاتلات","طائرات مسيرة","ذخائر","دفاع جوي","عملية عسكرية",
    "عمليات عسكرية","قوات خاصة","استهداف","إطلاق النار","قتال","معارك","أسطول",
    "military","army","forces","defense","defence","security","weapons","weapon",
    "missile","missiles","airstrike","airstrike","strike","attack","fighting",
    "battle","battles","combat","drone","drones","ammunition","air defense",
    "military operation","troops","navy","warship",
]

SECURITY_SOCIAL_EXCLUDE = [
    "يعزي","يعزّي","تعازي","وفاة والده","وفاة والدته","وفاة شقيق","وفاة عمه",
    "تهنئة","ترقية","تعيين","استقبال","زيارة تفقدية","احتفال",
    "condolences","condolence","promotion","appointment","welcomes","ceremony",
    "inspection visit",
]

OFFICIAL_TERMS = [
    "بيان رسمي","تصريح رسمي","بيان صحفي","المتحدث الرسمي","المتحدث باسم",
    "مصدر مسؤول","أعلنت الوزارة","أعلن الوزير","قالت الوزارة","قال الوزير",
    "وزارة الخارجية","وزارة الدفاع","وزارة الداخلية","وزارة المالية",
    "وزارة الطاقة","وزارة الصحة","وزارة الإعلام","رئاسة الوزراء",
    "الديوان الملكي","الحكومة تعلن","الحكومة تؤكد","الرئاسة تعلن","الرئاسة تؤكد",
    "السفير","السفارة","المبعوث","الخارجية","الوزارة","الوزير",
    "foreign ministry","ministry said","government said","official statement",
    "press statement","spokesperson","state department","ambassador","embassy",
    "envoy","president said","prime minister said","minister said","ministry",
]

URGENT_TERMS = [
    "عاجل","طارئ","هجوم","انفجار","قصف","صاروخ","زلزال","اشتباك","غارة",
    "إخلاء","حالة طوارئ","تحذير عاجل","استهداف","غارات","إطلاق النار",
    "breaking","urgent","attack","explosion","airstrike","missile","earthquake",
    "evacuation","emergency","warning","strike","gunfire",
]

# Generic digest/roundup articles are not individual news events.
DIGEST_TERMS = [
    "أهم الأخبار","أبرز الأخبار","حصاد الأخبار","موجز الأخبار",
    "أخبار العالم حتى","أهم الأخبار العالمية والعربية",
    "most important news","top news","news roundup","world news roundup",
    "top stories","daily roundup","news digest","latest news roundup",
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
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s\u0600-\u06FF-]", " ", text)).strip()


# Arabic display/normalization layer.
# This uses the public Google Translate endpoint only for title translation.
# Gemini is deliberately NOT used here.
_TRANSLATION_CACHE: dict[str, str] = {}
_TRANSLATION_CACHE_MAX = 1500
_TRANSLATION_TIMEOUT = 8
_TRANSLATION_ENDPOINT = "https://translate.googleapis.com/translate_a/single"


def _needs_arabic_translation(title: str) -> bool:
    text = str(title or "").strip()
    if not text:
        return False
    arabic = len(re.findall(r"[\u0600-\u06FF]", text))
    latin = len(re.findall(r"[A-Za-z]", text))
    # Arabic-dominant titles are already suitable for display.
    return latin >= 3 and latin > arabic


async def translate_title_to_arabic(session, title: str) -> str:
    title = (title or "").strip()
    if not _needs_arabic_translation(title):
        return title

    key = normalize_text(title)
    cached = _TRANSLATION_CACHE.get(key)
    if cached:
        return cached

    params = {
        "client": "gtx",
        "sl": "auto",
        "tl": "ar",
        "dt": "t",
        "q": title,
    }

    for attempt in range(2):
        try:
            timeout = aiohttp.ClientTimeout(total=_TRANSLATION_TIMEOUT)
            async with session.get(
                _TRANSLATION_ENDPOINT,
                params=params,
                timeout=timeout,
                headers={"User-Agent": "Mozilla/5.0"},
            ) as response:
                if response.status != 200:
                    raise RuntimeError(f"translation_http_{response.status}")
                payload = await response.json(content_type=None)

            translated = "".join(
                part[0]
                for part in (payload[0] if isinstance(payload, list) else [])
                if isinstance(part, list) and part and part[0]
            ).strip()

            if translated and _needs_arabic_translation(translated) is False:
                if len(_TRANSLATION_CACHE) >= _TRANSLATION_CACHE_MAX:
                    _TRANSLATION_CACHE.pop(next(iter(_TRANSLATION_CACHE)))
                _TRANSLATION_CACHE[key] = translated
                return translated

        except Exception:
            if attempt == 1:
                log.debug("Title translation unavailable for: %s", title, exc_info=True)
            await asyncio.sleep(0.25)

    return title


async def translate_news_titles(items):
    """Translate foreign-language titles to Arabic without using Gemini.

    The original title is preserved in ``original_title``. The Arabic title
    becomes ``title`` so classification, deduplication and Telegram display
    operate on the same canonical language.
    """
    if not items:
        return items

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *(translate_title_to_arabic(session, item.title) for item in items),
            return_exceptions=True,
        )

    for item, translated in zip(items, results):
        if isinstance(translated, Exception) or not translated:
            continue
        if translated != item.title:
            item.original_title = item.original_title or item.title
            item.title = translated
            item.search_text = normalize_text(
                f"{item.title} {item.original_title} {item.summary} "
                f"{item.source} {item.region}"
            )

    return items




# Cross-language event identity layer.
# These aliases are deliberately small and high-signal: they are used only to
# connect likely translations of the same event, not to classify news.
EVENT_ALIASES = {
    "saudi_arabia": ["السعودية", "المملكة العربية السعودية", "saudi arabia", "saudi"],
    "iran": ["ايران", "إيران", "iran"],
    "israel": ["اسرائيل", "إسرائيل", "israel"],
    "united_states": ["الولايات المتحدة", "امريكا", "أمريكا", "united states", "u.s.", "us", "usa"],
    "russia": ["روسيا", "russia"],
    "ukraine": ["اوكرانيا", "أوكرانيا", "ukraine"],
    "china": ["الصين", "china"],
    "qatar": ["قطر", "qatar"],
    "uae": ["الامارات", "الإمارات", "united arab emirates", "uae"],
    "oil": ["نفط", "النفط", "خام", "الخام", "oil", "crude", "crude oil"],
    "brent": ["برنت", "brent"],
    "wti": ["غرب تكساس", "خام غرب تكساس", "wti", "west texas intermediate"],
    "gas": ["غاز", "الغاز", "gas", "natural gas", "lng"],
    "exports": ["صادرات", "تصدير", "exports", "export"],
    "imports": ["واردات", "استيراد", "imports", "import"],
    "prices": ["اسعار", "أسعار", "سعر", "prices", "price"],
    "rise": ["يرتفع", "ارتفع", "ارتفاع", "تصعد", "صعد", "ارتفاعا", "rise", "rises", "rose", "increase", "increases", "increased", "surge", "surges"],
    "fall": ["ينخفض", "انخفض", "انخفاض", "تهبط", "هبط", "هبوط", "fall", "falls", "fell", "decline", "declines", "declined", "drop", "drops", "cut", "cuts", "cutting", "lower", "lowers", "lowered", "خفض", "خفضت", "خفض", "خفضا"],
    "attack": ["هجوم", "هجومًا", "هجوما", "استهداف", "قصف", "غارة", "هجوم", "attack", "attacks", "attacked", "strike", "strikes", "airstrike"],
    "missile": ["صاروخ", "صواريخ", "missile", "missiles"],
    "ceasefire": ["وقف إطلاق النار", "وقف اطلاق النار", "هدنة", "ceasefire", "truce"],
    "sanctions": ["عقوبات", "sanctions", "sanction"],
    "agreement": ["اتفاق", "اتفاقية", "اتفاقا", "agreement", "deal", "accord"],
    "talks": ["محادثات", "مفاوضات", "talks", "negotiations", "negotiation"],
    "interest_rates": ["فائدة", "الفائدة", "اسعار الفائدة", "أسعار الفائدة", "interest rates", "interest rate"],
    "inflation": ["تضخم", "التضخم", "inflation"],
    "stock_market": ["اسهم", "أسهم", "بورصة", "سوق الاسهم", "سوق الأسهم", "stocks", "equities", "stock market"],
    "gold": ["ذهب", "الذهب", "gold"],
    "bitcoin": ["بيتكوين", "بتكوين", "bitcoin"],
}

def _event_aliases(text):
    n = normalize_text(text)
    found = set()
    for canonical, aliases in EVENT_ALIASES.items():
        for alias in aliases:
            a = normalize_text(alias)
            if a and re.search(rf"(?<!\\w){re.escape(a)}(?!\\w)", n):
                found.add(canonical)
                break
    return found

def _event_identity(item):
    title = item.title or ""
    summary = item.summary or ""
    original = item.original_title or ""
    aliases = _event_aliases(f"{title} {original} {summary}")
    return aliases

def _published_close(a, b, hours=36):
    if not a.published or not b.published:
        return True
    try:
        return abs((a.published - b.published).total_seconds()) <= hours * 3600
    except Exception:
        return True

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

    # Exact original-language identity catches syndicated copies.
    oa = normalized_title(a.original_title)
    ob = normalized_title(b.original_title)
    if oa and ob and oa == ob:
        return True

    # Strong text similarity remains the primary same-language signal.
    sim = similarity_score(na, nb)
    if sim >= 0.78:
        return True
    if sim >= 0.58 and a.region and a.region == b.region:
        return True

    # Cross-language identity: compare language-neutral, high-signal event
    # concepts rather than raw words. This is deliberately conservative.
    ia = _event_identity(a)
    ib = _event_identity(b)
    common = ia & ib
    if len(common) < 2:
        return False

    # Do not merge unrelated stories merely because they share a country and
    # a broad topic. Require either a commodity/topic + action, or a security
    # event + actor/event marker.
    topic = {"oil", "brent", "wti", "gas", "exports", "imports", "prices",
             "interest_rates", "inflation", "stock_market", "gold", "bitcoin"}
    actions = {"rise", "fall", "attack", "missile", "ceasefire", "sanctions",
               "agreement", "talks"}
    actors = {"saudi_arabia", "iran", "israel", "united_states", "russia",
              "ukraine", "china", "qatar", "uae"}

    topic_hit = bool(common & topic)
    action_hit = bool(common & actions)
    actor_hit = bool(common & actors)

    # Time is an important guard against merging recurring stories.
    if not _published_close(a, b):
        return False

    if topic_hit and action_hit and (actor_hit or a.region == b.region):
        return True

    if actor_hit and action_hit and (a.region == b.region or
                                     bool(common & {"missile", "ceasefire", "sanctions", "agreement", "talks"})):
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

def _domain_matches(domain, candidates):
    d = (domain or "").lower().split(":")[0].split("/")[0].strip(".")
    return any(d == candidate or d.endswith("." + candidate) for candidate in candidates)

def is_official_source(source, domain):
    # Official status is determined from the publisher domain, not from words
    # appearing in a title or an arbitrary source label. This prevents media
    # pages that merely mention a ministry from being promoted to "official".
    d = (domain or "").lower()
    return _domain_matches(d, OFFICIAL_SOURCE_DOMAINS) or any(
        hint in d for hint in OFFICIAL_DOMAIN_HINTS
    )

def source_trust(source, domain):
    if _domain_matches(domain, OFFICIAL_SOURCE_DOMAINS):
        return 99
    for key, score in MAJOR_NEWS_DOMAINS.items():
        if domain.endswith(key):
            return score
    d = (domain or "").lower()
    if any(hint in d for hint in OFFICIAL_DOMAIN_HINTS):
        return 98
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

def _is_digest(title):
    n = normalize_text(title)
    return any(normalize_text(term) in n for term in DIGEST_TERMS)

def _security_signal(title, summary=""):
    t = normalize_text(title)
    s = normalize_text(summary)
    if any(normalize_text(x) in t for x in SECURITY_SOCIAL_EXCLUDE):
        return False
    return score_terms(t, SECURITY_TERMS) >= 1 or score_terms(s, SECURITY_TERMS) >= 2

def _economy_signal(title, summary=""):
    t = normalize_text(title)
    s = normalize_text(summary)

    # Economic relevance must come from the article itself, never from
    # the search source/query string.
    title_hits = score_terms(t, ECON_TERMS)
    summary_hits = score_terms(s, ECON_TERMS)
    negative = score_terms(f"{t} {s}", ECON_EXCLUDE)

    if negative >= 1 and title_hits == 0:
        return False

    # A clear economic term in the title is sufficient.
    if title_hits >= 1:
        return True

    # Otherwise require multiple economic signals in the summary.
    return summary_hits >= 2 and negative == 0

def _official_signal(title, summary, item=None):
    t = normalize_text(title)
    s = normalize_text(summary)

    title_hits = score_terms(t, OFFICIAL_TERMS)
    summary_hits = score_terms(s, OFFICIAL_TERMS)

    # A real official marker in the headline is enough.
    if title_hits >= 1:
        return True

    # Do not treat generic "government forces" / "government" as an
    # official statement. Require explicit statement language.
    explicit = [
        "بيان","تصريح","المتحدث","قالت الوزارة","قال الوزير",
        "اعلنت الوزارة","أعلنت الوزارة","السفير","الخارجية",
        "statement","spokesperson","ministry said","minister said",
        "ambassador","foreign ministry","official statement",
    ]
    if any(normalize_text(x) in s for x in explicit) and summary_hits >= 1:
        return True

    if item is not None and item.official and summary_hits >= 1:
        return True

    return False

def classify_item(item):
    title = normalize_text(item.title)
    summary = normalize_text(item.summary)
    text = f"{title} {summary}"

    if _is_digest(item.title):
        item.category = "general"
        return item

    econ = (score_terms(title, ECON_TERMS) * 3
            + score_terms(summary, ECON_TERMS))
    if score_terms(text, ECON_EXCLUDE):
        econ -= 5

    sec = (score_terms(title, SECURITY_TERMS) * 3
           + score_terms(summary, SECURITY_TERMS))
    if score_terms(title, SECURITY_SOCIAL_EXCLUDE):
        sec -= 12

    official = (score_terms(title, OFFICIAL_TERMS) * 3
                + score_terms(summary, OFFICIAL_TERMS))
    if item.official:
        official += 2

    urgent = (score_terms(title, URGENT_TERMS) * 2
              + score_terms(summary, URGENT_TERMS))

    # Explicit category assignment. Security/social and official stories
    # must not be promoted to economy merely because they mention oil/energy.
    if _security_signal(title, summary) and sec >= max(econ, official, 4):
        item.category = "secu"
    elif _official_signal(title, summary, item) and official >= max(econ, sec, 4):
        item.category = "forg"
    elif _economy_signal(title, summary) and econ >= max(sec, official, 4):
        item.category = "econ"
    elif urgent >= 4:
        item.category = "urg"
    else:
        item.category = "general"

    return item

def is_topic_match(item, topic_key):
    title = item.title or ""
    summary = item.summary or ""

    # This is intentionally the first gate for every section.
    # Generic digest articles can never enter any section.
    if _is_digest(title):
        return False

    if topic_key == "econ":
        # Section classification uses title/summary only.
        # It NEVER uses source/search_text.
        return _economy_signal(title, summary) and not (
            _security_signal(title, summary)
            and score_terms(normalize_text(title), SECURITY_TERMS) >= 1
            and score_terms(normalize_text(title), ECON_TERMS) <= 1
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

    if topic_key == "gulf":
        text = normalize_text(f"{title} {summary}")
        gulf = REGIONS["الشرق الأوسط"]
        return any(normalize_text(x) in text for x in gulf)

    if topic_key == "wrld":
        text = normalize_text(f"{title} {summary}")
        return bool(item.region) or any(
            normalize_text(x) in text
            for x in ["امريكا","الولايات المتحده","اوروبا","الصين","روسيا","اوكرانيا","الهند","اليابان"]
        )

    return False

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

    summary = html.unescape(
        str(entry.get("summary", "") or entry.get("description", "") or "")
    )
    published = parse_date(entry.get("published") or entry.get("updated") or "")

    item = NewsItem(
        title=title,
        original_title=title,
        url=url,
        source=source,
        summary=summary,
        published=published,
        category=category,
    )
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
            *(
                fetch_feed(
                    session,
                    f"بحث: {q}",
                    google_news_url(q)
                )
                for q in queries
            ),
            return_exceptions=True,
        )

    items = []
    for group in groups:
        if isinstance(group, list):
            items.extend(group)

    # Canonicalize foreign-language titles before final deduplication.
    # This makes Arabic/English versions of the same event compete as one item.
    items = await translate_news_titles(items)
    items = [
        item for item in deduplicate_news(items)
        if not _is_digest(item.title)
    ]

    return rank_search_results(items, query)[:max_results]

def rank_search_results(items, query):
    q_tokens = tokenize(query)
    ranked = []

    for item in items:
        title_tokens = tokenize(item.title)
        summary_tokens = tokenize(item.summary)

        title_hits = len(q_tokens & title_tokens)
        summary_hits = len(q_tokens & summary_tokens)

        exact_phrase = normalize_text(query) in normalize_text(item.title)

        # IMPORTANT:
        # Do not count source/search_text as topical relevance.
        # "بحث: السعودية اقتصاد أسواق نفط" must not make an unrelated
        # article look economic.
        score = (
            title_hits * 12
            + summary_hits * 3
            + (25 if exact_phrase else 0)
            + item.trust_score * 0.04
        )

        item.relevance_score = score
        ranked.append((score, item))

    ranked.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in ranked]

async def search_news(items, query, max_results=25):
    return rank_search_results(
        [
            item for item in deduplicate_news(items)
            if not _is_digest(item.title)
        ],
        query,
    )[:max_results]

async def hybrid_search_news(items, query, max_results=25):
    local = await search_news(items, query, max_results)
    online = await search_news_online(query, max_results)

    merged = [
        item for item in deduplicate_news(local + online)
        if not _is_digest(item.title)
    ]

    ranked = rank_search_results(merged, query)

    # Search relevance comes ONLY from title/summary.
    # Source names and search query labels are deliberately ignored.
    q_tokens = tokenize(query)
    filtered = []

    for item in ranked:
        title_hits = len(q_tokens & tokenize(item.title))
        summary_hits = len(q_tokens & tokenize(item.summary))

        if title_hits or summary_hits:
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

    # Do not use generic "أهم الأخبار العالمية" discovery here.
    # Those roundup pages were the direct cause of digest articles
    # leaking into topic sections.
    discovery_queries = [
        "السعودية اقتصاد أسواق نفط",
        "الشرق الأوسط أمن دفاع",
        "بيانات رسمية وزارة خارجية",
        "world economy markets oil",
        "world security defense",

        # Strong regional broadcasters / international TV newsrooms.
        "site:channelnewsasia.com Asia breaking news",
        "site:euronews.com Europe world breaking news",
        "site:africanews.com Africa breaking news",
        "site:nhk.or.jp Japan world news",
        "site:abc.net.au/news Australia world news",
        "site:cbc.ca/news Canada world news",
        "site:skynews.com world breaking news",

        # Official-source discovery is kept separate from general media discovery.
        "site:mofa.gov.sa وزارة الخارجية السعودية",
        "site:state.gov foreign policy statement",
        "site:mfa.gov.cn foreign ministry statement",
        "site:mofa.go.jp foreign ministry statement",
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

    # Canonicalize every foreign title before the final event-level dedup.
    # This prevents the same story arriving in Arabic and English from being
    # emitted twice merely because the source language differs.
    items = await translate_news_titles(items)
    items = [
        item for item in deduplicate_news(items)
        if not _is_digest(item.title)
    ]

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
