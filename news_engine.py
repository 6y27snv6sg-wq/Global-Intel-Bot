import asyncio
import html
import json
import logging
import os
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

import aiohttp
import feedparser

log = logging.getLogger("news_engine")

FETCH_TIMEOUT = 4
FETCH_CONNECT_TIMEOUT = 2
FEED_PARSE_TIMEOUT = 1.5
MAX_FEED_BYTES = 900_000
FEED_PARSE_WORKERS = 2
FEED_CIRCUIT_FAILURES = 3
FEED_CIRCUIT_COOLDOWN = 600
COLLECTION_CONCURRENCY = 20
MAX_FEED_ITEMS = 30
MAX_ONLINE_QUERIES = 6
ONLINE_SEARCH_BUDGET = 3.5
SEARCH_TRANSLATION_BUDGET = 1.0
LOCAL_SEARCH_TRANSLATION_BUDGET = 0.6
SEARCH_TRANSLATION_RESULT_CAP = 10
DISCOVERY_BUDGET = 4.5
DISCOVERY_CONCURRENCY = 8
ROTATION_WINDOW_SECONDS = 300
DATE_ENRICH_TIMEOUT = 2.5
DATE_ENRICH_CONCURRENCY = 6
DATE_ENRICH_MAX_CANDIDATES = 12
DATE_ENRICH_BUDGET = 2.5
GENERAL_TRANSLATION_BUDGET = 2.0
GENERAL_CANDIDATE_CAP = 100
OFFICIAL_INDEX_TIMEOUT = 3.0
OFFICIAL_INDEX_CONCURRENCY = 24
OFFICIAL_INDEX_MAX_LINKS = 12
OFFICIAL_INTERACTIVE_MAX_LINKS_PER_INDEX = 6
OFFICIAL_INTERACTIVE_BUDGET = 5.5
OFFICIAL_COLLECTION_BUDGET = 12.0
OFFICIAL_MAX_PAGE_BYTES = 900_000
OFFICIAL_SOURCES_PER_MEMBER_PER_CYCLE = 3
OFFICIAL_CACHE_ITEMS_PER_SOURCE = 20

# Fast breaking-news lane: direct publisher feeds only.  This path is designed
# for frequent lightweight polling and deliberately excludes Google discovery,
# official page crawling, date enrichment, and AI analysis.
BREAKING_FEED_CONCURRENCY = 12

_FEED_FAILURE_STATE = {}
_OFFICIAL_PROFILE_ROUND = 0
_OFFICIAL_RESULT_CACHE = {}
# feedparser is pure-Python and can monopolize the GIL when many feeds parse at once.
# Keep RSS parsing on a small dedicated pool so background collectors cannot starve
# Telegram callbacks or the main asyncio loop.
_FEED_PARSE_EXECUTOR = ThreadPoolExecutor(
    max_workers=FEED_PARSE_WORKERS,
    thread_name_prefix="feed-parser",
)
BREAKING_TRANSLATION_BUDGET = 1.2
BREAKING_MAX_PER_FEED = 12

# Current-news policy: the live platform contains only today and the previous
# three UTC calendar days. Historical research belongs to a separate path.
CURRENT_NEWS_LOOKBACK_DAYS = 3

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
    "cgtn.com": 90, "cctv.com": 89, "rt.com": 88, "tass.com": 90,
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
    "earnings","acquisition", "reserve bank", "monetary policy",
    "liquidity adjustment facility", "liquidity facility", "reverse repo",
    "repo rate", "open market operation",
    "بنك احتياطي", "سياسة نقدية", "تسهيلات السيولة", "إعادة الشراء",
    "عمليات السوق المفتوحة",
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

# Hard-noise patterns: service/SEO pages rather than intelligence-grade news.
LOW_VALUE_HARD_TERMS = [
    "بث مباشر", "البث المباشر", "شاهد مباشر", "شاهد البث المباشر",
    "مشاهدة مباشرة", "مشاهدة البث المباشر", "مشاهدة مباراة",
    "شاهد المباراة", "رابط المباراة", "روابط المباراة", "live stream",
    "watch live", "streaming link", "live score", "نتيجة مباشرة",
]

SPORT_TERMS = [
    "مباراة", "دوري", "بطولة", "كأس", "منتخب", "فريق", "هدف", "لاعب",
    "مدرب", "سباق", "فورمولا", "formula 1", "football", "soccer",
    "match", "league", "cup", "tournament", "team", "player", "race",
]

CULTURE_ROUTINE_TERMS = [
    "معرض", "متحف", "مهرجان", "حفلة", "فيلم", "مسرح", "تراث",
    "exhibition", "museum", "festival", "concert", "film", "heritage",
]

PROTOCOL_TERMS = [
    "يهنئ", "تهنئ", "تهنئة", "يعزي", "تعازي", "ذكرى الاستقلال",
    "congratulates", "congratulations", "condolences", "independence day",
]

HIGH_IMPACT_TERMS = [
    "حرب", "هجوم", "قصف", "صاروخ", "انفجار", "زلزال", "فيضانات",
    "طوارئ", "عقوبات", "انتخابات", "استقالة", "إقالة", "مفاوضات",
    "اتفاق", "أزمة", "احتجاج", "اعتقال", "اغتيال", "انقلاب",
    "war", "attack", "strike", "missile", "explosion", "earthquake",
    "flood", "emergency", "sanctions", "election", "resigns",
    "negotiations", "agreement", "crisis", "protest", "arrest", "coup",
]

SEARCH_INTENT_TERMS = {
    "sport": SPORT_TERMS + ["رياضة", "رياضي", "sports"],
    "culture": CULTURE_ROUTINE_TERMS + ["ثقافة", "ثقافي", "culture", "arts"],
    "economy": ECON_TERMS,
    "security": SECURITY_TERMS,
}

EVENT_CLASS_TERMS = {
    "fatality_rescue": ["وفاة", "جثة", "ضحية", "الضحايا", "انتشال", "إنقاذ", "انقاذ", "موت",
                        "death", "dead", "body", "victim", "rescue"],
    "attack": ["هجوم", "قصف", "غارة", "صاروخ", "انفجار", "استهداف",
               "attack", "airstrike", "missile", "explosion", "strike"],
    "politics": ["انتخابات", "استقالة", "إقالة", "تعيين", "حكومة", "رئيس", "وزير",
                 "election", "resign", "appointed", "government", "president", "minister"],
    "economy": ECON_TERMS,
}

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
_TRANSLATION_TIMEOUT = 3
_TRANSLATION_ENDPOINT = "https://translate.googleapis.com/translate_a/single"


def _needs_arabic_translation(title: str) -> bool:
    text = str(title or "").strip()
    if not text:
        return False
    arabic = len(re.findall(r"[\u0600-\u06FF]", text))
    latin = len(re.findall(r"[A-Za-z]", text))
    # Arabic-dominant titles are already suitable for display.
    return latin >= 3 and latin > arabic


def _translation_input(title: str) -> str:
    """Remove redundant parenthesized acronyms before machine translation.

    Publisher headlines commonly spell out a term and then append an acronym,
    e.g. ``Liquidity Adjustment Facility (LAF)``. Translators may reinterpret
    that acronym as an unrelated organization. The full phrase is retained, so
    stripping only the parenthesized acronym loses no headline meaning.
    """
    text = str(title or "").strip()
    return re.sub(r"(?<=\w)\s*\([A-Z][A-Z0-9&.-]{1,9}\)", "", text).strip()


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
        "q": _translation_input(title),
    }

    for attempt in range(1):
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


async def translate_news_titles(items, budget=None):
    """Translate titles concurrently while preserving partial successes.

    A slow translation must never block the search page. Tasks that finish
    inside ``budget`` are applied immediately; unfinished tasks are cancelled.
    This avoids the previous all-or-nothing ``gather`` behaviour where one slow
    title could cause every completed translation to be discarded by an outer
    timeout.
    """
    if not items:
        return items

    connector = aiohttp.TCPConnector(limit=max(4, min(12, len(items))), ttl_dns_cache=60)
    async with aiohttp.ClientSession(connector=connector) as session:
        task_map = {
            asyncio.create_task(translate_title_to_arabic(session, item.title)): item
            for item in items
        }
        if not task_map:
            return items

        try:
            done, pending = await asyncio.wait(
                task_map.keys(),
                timeout=budget,
            )
    
            for task in done:
                item = task_map[task]
                try:
                    translated = task.result()
                except Exception:
                    continue
                if translated and translated != item.title:
                    item.original_title = item.original_title or item.title
                    item.title = translated
                    item.search_text = normalize_text(
                        f"{item.title} {item.original_title} {item.summary} "
                        f"{item.source} {item.region}"
                    )
    
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
                log.warning("Search title translation budget exceeded for %s title(s); skipped unfinished translations.", len(pending))
        finally:
            unfinished = [task for task in task_map if not task.done()]
            for task in unfinished:
                task.cancel()
            if unfinished:
                await asyncio.gather(*unfinished, return_exceptions=True)

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

EVENT_STOPWORDS = {
    "عاجل", "خبر", "اخبار", "اليوم", "الان", "جديد", "تحديث",
    "قال", "قالت", "يقول", "بحسب", "حول", "خلال", "بعد", "قبل",
    "الى", "على", "عن", "في", "من", "مع", "هذا", "هذه",
    "ذلك", "التي", "الذي", "وهو", "وهي",
    "breaking", "news", "latest", "update", "says", "said", "according",
    "after", "before", "with", "from", "into", "over", "amid", "the",
}


def tokenize(value):
    return {
        x for x in normalize_text(value).split()
        if len(x) > 2 and x not in EVENT_STOPWORDS
    }


def normalized_title(value):
    return normalize_text(value)


def title_fingerprint(value):
    return " ".join(sorted(tokenize(value)))


def similarity_score(a, b):
    sa, sb = tokenize(a), tokenize(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / max(1, len(sa | sb))


def _title_numbers(value):
    return set(re.findall(r"\b\d+(?:[.,]\d+)?%?\b", normalize_text(value)))


def _first_keyword_window(value, size=7):
    """First meaningful headline words after stopword cleanup."""
    words = [
        x for x in normalize_text(value).split()
        if len(x) > 2 and x not in EVENT_STOPWORDS
    ]
    return words[:size]


def _first_window_event_match(a, b, size=7):
    """Use the first seven meaningful words as an extra same-event signal.

    It is never sufficient alone: full-title overlap, time proximity and
    conflicting-number guards remain mandatory.
    """
    wa = _first_keyword_window(a.title, size)
    wb = _first_keyword_window(b.title, size)
    if len(wa) < 4 or len(wb) < 4:
        return False

    sa, sb = set(wa), set(wb)
    common = sa & sb
    containment = len(common) / max(1, min(len(sa), len(sb)))

    nums_a = _title_numbers(a.title)
    nums_b = _title_numbers(b.title)
    if nums_a and nums_b and nums_a.isdisjoint(nums_b):
        return False

    if not _published_close(a, b, hours=30):
        return False

    full_a = tokenize(a.title)
    full_b = tokenize(b.title)
    full_common = full_a & full_b
    full_containment = len(full_common) / max(1, min(len(full_a), len(full_b)))

    return (
        len(common) >= 5
        and containment >= 0.72
        and len(full_common) >= 5
        and full_containment >= 0.58
    )


def _source_names(item):
    names = []
    for value in [item.source] + list(item.alternate_sources or []):
        value = str(value or "").strip()
        if value and value not in names:
            names.append(value)
    return names


def _merge_sources(primary, secondary):
    """Keep the strongest story but preserve all publisher names."""
    merged = _source_names(primary)
    for value in _source_names(secondary):
        if value not in merged:
            merged.append(value)

    if merged:
        primary.source = merged[0]
        primary.alternate_sources = merged[1:]
    return primary


def display_sources(item):
    """Human-readable merged source label for Telegram/UI callers."""
    return " • ".join(_source_names(item))


def _lexical_event_match(a, b):
    """Conservative paraphrase detection without AI."""
    ta = tokenize(a.title)
    tb = tokenize(b.title)
    if not ta or not tb:
        return False

    common = ta & tb
    smaller = min(len(ta), len(tb))
    union = ta | tb
    containment = len(common) / max(1, smaller)
    jaccard = len(common) / max(1, len(union))

    nums_a = _title_numbers(a.title)
    nums_b = _title_numbers(b.title)
    if nums_a and nums_b and nums_a.isdisjoint(nums_b):
        return False

    if not _published_close(a, b, hours=30):
        return False

    return len(common) >= 4 and (
        containment >= 0.68 or jaccard >= 0.50
    )

def same_event(a, b):
    if getattr(a, "official_source_id", "") or getattr(b, "official_source_id", ""):
        return a.url.split("#", 1)[0] == b.url.split("#", 1)[0]
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

    # First seven meaningful words are an additional signal, never the only one.
    if _first_window_event_match(a, b):
        return True

    # Strong paraphrase match for the same news cycle.
    if _lexical_event_match(a, b):
        return True

    # Event-class + shared-entity fingerprint catches heavily reworded coverage.
    if _semantic_event_match(a, b):
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
    alternate_sources: Optional[List[str]] = None
    discovery_domain_hint: str = ""
    official_source_id: str = ""
    publication_evidence: str = ""

    def __post_init__(self):
        self.title = (self.title or "").strip()
        self.original_title = self.original_title or self.title
        self.source = (self.source or "مصدر إخباري").strip()
        if self.alternate_sources is None:
            self.alternate_sources = []
        self.alternate_sources = [
            str(x).strip() for x in self.alternate_sources
            if str(x).strip()
        ]
        self.summary = re.sub(r"<[^>]+>", " ", self.summary or "").strip()
        self.domain = (self.domain or urlparse(self.url).netloc or "").lower().replace("www.", "")
        self.discovery_domain_hint = (self.discovery_domain_hint or "").lower().strip(".").replace("www.", "")
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

    # Precision + recall:
    # - A direct statement/release marker is sufficient.
    # - An official actor must normally be paired with an official action.
    # - A direct official publisher may qualify when its headline itself
    #   describes an official action.
    # Publisher status alone is NEVER sufficient, so general UN/news reports
    # do not leak into the official-statements section.
    document_markers = [
        "بيان رسمي", "تصريح رسمي", "بيان صحفي", "مؤتمر صحفي",
        "المتحدث الرسمي", "المتحدث باسم", "مصدر مسؤول",
        "official statement", "press statement", "press release", "joint statement",
        "media note", "fact sheet", "press conference", "readout", "remarks by",
        "statement by", "briefing by", "spokesperson", "spokesperson's remarks",
        "regular press conference", "communique", "communiqué", "declaration",
        "تصريح", "احاطة", "إحاطة", "مذكرة إعلامية", "مذكره اعلاميه",
    ]

    institution_markers = [
        "وزارة الخارجيه", "وزارة الدفاع", "وزارة الداخليه",
        "وزارة الماليه", "وزارة الطاقه", "وزارة الصحه",
        "رئاسه الوزراء", "الديوان الملكي",
        "foreign ministry", "ministry of foreign affairs",
        "state department",
    ]

    official_actors = [
        "الحكومه", "الرئاسه", "الوزاره", "الوزير", "السفير",
        "السفاره", "المبعوث", "رئيس الوزراء", "الرئيس",
        "الامين العام",
        "government", "president", "prime minister", "minister",
        "ambassador", "embassy", "envoy", "secretary general",
        "secretary-general",
    ]

    official_actions = [
        "اعلن", "اعلنت", "اكد", "اكدت", "صرح", "صرحت",
        "قال", "قالت", "حذر", "حذرت", "ادان", "ادانت",
        "نفى", "نفت", "اصدر", "اصدرت", "كشف", "كشفت",
        "يدعو", "دعا", "تدعو", "رحب", "رحبت", "قرر", "قررت",
        "استقبل", "استقبلت", "بحث", "بحثت", "ناقش", "ناقشت",
        "اجتمع", "اجتمعت", "التقى", "التقت", "اتصال", "لقاء",
        "announced", "said", "confirmed", "stated", "warned",
        "condemned", "denied", "issued", "called for", "welcomed",
        "met", "meets", "meeting", "meeting with", "received", "receives",
        "discussed", "discusses", "held talks", "spoke with", "courtesy call",
        "telephone conversation", "phone call", "consultations", "signed", "signing",
        "visited", "visit", "participated", "participates",
    ]

    if any(normalize_text(x) in t for x in document_markers):
        return True

    institution_hit = any(normalize_text(x) in t for x in institution_markers)
    actor_hit = any(normalize_text(x) in t for x in official_actors)
    action_hit = any(normalize_text(x) in t for x in official_actions)

    if (institution_hit or actor_hit) and action_hit:
        return True

    # Attribution headlines such as "الخارجية التركية: ..." qualify, but
    # ordinary phrases such as "القوى الخارجية" do not.
    raw_title = str(title or "")
    if ":" in raw_title:
        prefix = normalize_text(raw_title.split(":", 1)[0])
        foreign_ministry_prefix = (
            prefix == "الخارجيه"
            or prefix.startswith("الخارجيه ")
        )
        if (
            foreign_ministry_prefix
            or any(normalize_text(x) in prefix for x in institution_markers + official_actors)
        ):
            return True

    # Direct official domains are trusted as publishers, but still require
    # an action-bearing headline. This admits genuine ministry/government
    # releases while rejecting generic institutional news reports.
    if item is not None and item.official and action_hit:
        return True

    return False

def _direct_official_statement(item):
    """Precision gate for the Official Statements section.

    Ministries publish official material under several newsroom forms: releases,
    remarks, briefings, meetings, courtesy calls, consultations and communiqués.
    Verified original-publisher provenance therefore carries more weight than a
    narrow keyword such as ``statement``.  Ordinary media mentions still fail.
    """
    if (getattr(item, "official_source_id", "") in OFFICIAL_SOURCE_REGISTRY
            and getattr(item, "publication_evidence", "")
            and _is_current_news(item)):
        profile = OFFICIAL_SOURCE_REGISTRY[item.official_source_id]
        if profile.get("statement_index") and _domain_matches(item.domain, profile["domains"]):
            return True
    profile = OFFICIAL_SOURCE_REGISTRY.get(getattr(item, "official_source_id", ""))
    return bool(profile and getattr(item, "publication_evidence", "")
                and _is_current_news(item)
                and _domain_matches(item.domain, profile["domains"])
                and _official_signal(item.original_title or item.title, item.summary, item))


URGENT_SECTION_TERMS = [
    "عاجل", "خبر عاجل", "تحذير عاجل", "حالة طوارئ", "إخلاء فوري",
    "breaking", "breaking news", "urgent", "state of emergency",
    "emergency declared", "immediate evacuation",
    "زلزال", "earthquake", "تسونامي", "tsunami",
]

ROUTINE_INSTITUTIONAL_PATTERNS = [
    # Internal staffing, fellowships and ceremonial publicity. Central-bank
    # market operations are not noise: they belong exclusively to Economy.
    r"\bappoints? (?:a )?new (?:chief financial officer|finance director)\b",
    r"\bnational armaments director appoints\b",
    r"\bfellowship (?:programme|program)\b",
    r"\byouth fellowship\b",
    r"\b(?:to celebrate|commemorates?|marks?) (?:the )?.{0,35}\banniversary\b",
    r"\bيعين مديرا ماليا جديدا\b",
    r"\bبرنامج زماله الشباب\b",
    r"\b(?:للاحتفال|يحتفل|سيحتفل|يحيي) .{0,35}\بالذكري\b",
]

SUBSTANTIVE_POLICY_TERMS = [
    "strategy", "policy", "decision", "interest rate", "rate decision",
    "sanctions", "agreement", "treaty", "ceasefire", "legislation",
    "budget", "security", "defence", "defense", "military", "emergency",
    "استراتيجية", "سياسة", "قرار", "سعر الفائدة", "عقوبات", "اتفاق",
    "معاهدة", "وقف إطلاق النار", "تشريع", "ميزانية", "أمن", "دفاع",
    "عسكري", "طوارئ",
]


def _classification_text(item):
    """Use trustworthy pre-translation text for every section decision."""
    original = str(getattr(item, "original_title", "") or "").strip()
    title = original or str(getattr(item, "title", "") or "").strip()
    summary = str(getattr(item, "summary", "") or "").strip()
    return title, summary


def _routine_institutional_noise(item, title, summary):
    """Reject routine publisher notices while retaining substantive policy."""
    text = normalize_text(f"{title} {summary}")
    if not any(re.search(pattern, text, re.I) for pattern in ROUTINE_INSTITUTIONAL_PATTERNS):
        return False
    return score_terms(text, SUBSTANTIVE_POLICY_TERMS) == 0


def _exclusive_topic_key(item):
    """Assign exactly one specialist section, or None for general noise."""
    title, summary = _classification_text(item)
    if not title or _is_digest(title) or _hard_low_value(item):
        return None
    if _routine_institutional_noise(item, title, summary):
        return None

    normalized_title = normalize_text(title)
    normalized_summary = normalize_text(summary)

    # Keep urgent deliberately narrow. Ordinary attack/missile coverage stays
    # on the security desk unless it explicitly signals a live emergency.
    if (score_terms(normalized_title, URGENT_SECTION_TERMS) >= 1
            or score_terms(normalized_summary, URGENT_SECTION_TERMS) >= 2):
        return "urg"

    security = _security_signal(title, summary)
    economy = _economy_signal(title, summary)
    if security and economy:
        security_score = (
            score_terms(normalized_title, SECURITY_TERMS) * 3
            + score_terms(normalized_summary, SECURITY_TERMS)
        )
        economy_score = (
            score_terms(normalized_title, ECON_TERMS) * 3
            + score_terms(normalized_summary, ECON_TERMS)
        )
        if score_terms(normalized_title, SECURITY_SOCIAL_EXCLUDE):
            security_score -= 12
        if score_terms(f"{normalized_title} {normalized_summary}", ECON_EXCLUDE):
            economy_score -= 5
        return "secu" if security_score >= economy_score else "econ"
    if security:
        return "secu"
    if economy:
        return "econ"

    # Official is a provenance desk for releases that do not belong to a more
    # specific specialist desk. Thus defence releases go only to Security and
    # central-bank releases go only to Economy, while diplomatic/government
    # statements remain here.
    if _direct_official_statement(item):
        return "forg"

    # Regional/world desks are fallbacks after the specialist desks.
    # Recompute geography from the same original evidence. ``item.region`` may
    # have been inferred from a later display translation and is not evidence.
    region = detect_region(f"{title} {summary}")
    if region == "الشرق الأوسط":
        return "gulf"
    if region:
        return "wrld"

    world_markers = [
        "الأمم المتحدة", "الاتحاد الأوروبي", "الاتحاد الأفريقي", "الناتو",
        "united nations", "european union", "african union", "nato",
        "دولي", "عالمي", "international", "global",
    ]
    if score_terms(f"{normalized_title} {normalized_summary}", world_markers):
        return "wrld"
    return None


def classify_item(item):
    item.category = _exclusive_topic_key(item) or "general"
    return item


def is_topic_match(item, topic_key):
    if topic_key not in {"econ", "forg", "urg", "gulf", "wrld", "secu"}:
        return False
    return _exclusive_topic_key(item) == topic_key

def deduplicate_news(items):
    """Global event-level deduplication with source preservation.

    This function is intentionally central: search, sections, collection and
    urgent-news paths that call it all receive the same anti-duplication logic.
    """
    unique = []
    seen_urls = {}

    for item in items:
        if _is_digest(item.title) or _hard_low_value(item):
            continue

        url_key = item.url.strip().lower()

        # Same URL: merge publisher metadata instead of silently discarding it.
        if url_key and url_key in seen_urls:
            existing = seen_urls[url_key]
            _merge_sources(existing, item)
            continue

        duplicate = False
        for idx, existing in enumerate(unique):
            if not same_event(item, existing):
                continue

            item_is_better = (
                item.trust_score > existing.trust_score
                or (
                    item.trust_score == existing.trust_score
                    and item.published and (
                        not existing.published or item.published > existing.published
                    )
                )
            )

            if item_is_better:
                _merge_sources(item, existing)
                unique[idx] = item
                if existing.url:
                    seen_urls[existing.url.strip().lower()] = item
                if url_key:
                    seen_urls[url_key] = item
            else:
                _merge_sources(existing, item)
                if url_key:
                    seen_urls[url_key] = existing

            duplicate = True
            break

        if not duplicate:
            unique.append(item)
            if url_key:
                seen_urls[url_key] = item

    return unique

def parse_date(value):
    """Parse common RSS/Atom date representations without trusting missing dates."""
    if not value:
        return None
    try:
        # feedparser may expose time.struct_time for *_parsed fields.
        if hasattr(value, "tm_year"):
            import calendar
            return datetime.fromtimestamp(calendar.timegm(value), tz=timezone.utc)
        if isinstance(value, (tuple, list)) and len(value) >= 6:
            import calendar
            return datetime.fromtimestamp(calendar.timegm(tuple(value[:9])), tz=timezone.utc)
        text = str(value).strip()
        dt = parsedate_to_datetime(text)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        # ISO-8601 is common in Atom/JSON-derived feeds and is not accepted by
        # parsedate_to_datetime in every form.
        try:
            text = str(value).strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(text)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None


def _entry_date(entry):
    """Resolve publication time from standard RSS/Atom fields, strongest first."""
    for key in (
        "published_parsed", "updated_parsed", "created_parsed",
        "published", "updated", "created", "date", "dc_date",
    ):
        dt = parse_date(entry.get(key))
        if dt is not None:
            return dt
    return None


def _entry_publisher(entry, fallback_source):
    """Prefer the real publisher embedded in Google News RSS entries."""
    raw = entry.get("source")
    publisher = ""

    if isinstance(raw, dict):
        publisher = str(raw.get("title") or raw.get("name") or "").strip()
    elif raw:
        try:
            publisher = str(getattr(raw, "title", "") or raw).strip()
        except Exception:
            publisher = ""

    # Google News discovery labels are implementation details, not publishers.
    if publisher:
        return publisher
    return fallback_source

def _entry_source_domain(entry):
    """Return the publisher domain embedded by discovery feeds when available.

    Google News article links are wrappers, but its RSS ``source`` element
    normally carries the publisher URL.  Using that domain prevents a media
    article that merely mentions a ministry from impersonating the ministry.
    """
    raw = entry.get("source")
    candidates = []
    if isinstance(raw, dict):
        candidates.extend((raw.get("href"), raw.get("url"), raw.get("link")))
    elif raw:
        for attr in ("href", "url", "link"):
            try:
                candidates.append(getattr(raw, attr, None))
            except Exception:
                pass
    for value in candidates:
        if not value:
            continue
        domain = (urlparse(str(value)).netloc or "").lower().replace("www.", "")
        if domain:
            return domain
    return ""


def _query_intent(query):
    nq = normalize_text(query)
    for intent, terms in SEARCH_INTENT_TERMS.items():
        if any(normalize_text(term) in nq for term in terms):
            return intent
    return "general"


def _hard_low_value(item):
    text = normalize_text(f"{item.title} {item.original_title}")
    return any(normalize_text(term) in text for term in LOW_VALUE_HARD_TERMS)


def _content_value_adjustment(item, query):
    """Context-sensitive news-value score; generic rules, no country-specific tuning."""
    intent = _query_intent(query)
    text = normalize_text(f"{item.title} {item.original_title} {item.summary}")
    title = normalize_text(f"{item.title} {item.original_title}")
    delta = 0.0

    if any(normalize_text(x) in text for x in HIGH_IMPACT_TERMS):
        delta += 24
    if _security_signal(item.title, item.summary):
        delta += 18
    if _economy_signal(item.title, item.summary):
        delta += 14
    if _official_signal(item.title, item.summary, item):
        delta += 12
    if item.official:
        delta += 8

    sport = any(normalize_text(x) in title for x in SPORT_TERMS)
    culture = any(normalize_text(x) in title for x in CULTURE_ROUTINE_TERMS)
    protocol = any(normalize_text(x) in title for x in PROTOCOL_TERMS)

    if sport:
        delta += 24 if intent == "sport" else -40
    if culture:
        delta += 18 if intent == "culture" else -12
    if protocol and intent == "general":
        delta -= 16

    return delta


def _event_classes(item):
    text = normalize_text(f"{item.title} {item.original_title} {item.summary}")
    return {
        name for name, terms in EVENT_CLASS_TERMS.items()
        if any(normalize_text(term) in text for term in terms)
    }


def _semantic_event_match(a, b):
    """Conservative event match for heavily reworded headlines."""
    if not _published_close(a, b, hours=30):
        return False

    classes = _event_classes(a) & _event_classes(b)
    if not classes:
        return False

    ta = tokenize(a.title)
    tb = tokenize(b.title)
    common = ta & tb
    if len(common) < 2:
        return False

    # Require at least one fairly specific shared token, not just short glue words.
    specific = {t for t in common if len(t) >= 4 and t not in EVENT_STOPWORDS}
    if not specific:
        return False

    nums_a = _title_numbers(a.title)
    nums_b = _title_numbers(b.title)
    if nums_a and nums_b and nums_a.isdisjoint(nums_b):
        return False

    # Same detected region is a useful guard when titles are sparse.
    same_region = bool(a.region and b.region and a.region == b.region)
    containment = len(common) / max(1, min(len(ta), len(tb)))
    return (same_region and len(common) >= 2 and containment >= 0.22) or len(common) >= 3


def _is_non_article_result(item):
    """Reject homepages, section pages and generic portal entries."""
    title = normalize_text(item.title)
    original = normalize_text(item.original_title)
    combined = f"{title} {original}"

    generic_titles = (
        "الموقع الرسمي",
        "official website",
        "home page",
        "homepage",
        "الرئيسيه",
        "الرئيسية",
        "ministry of foreign affairs",
    )

    parsed = urlparse(item.url or "")
    path = (parsed.path or "").strip("/")

    # Generic ministry/portal titles with no article-specific wording.
    if any(normalize_text(x) in combined for x in generic_titles):
        article_signals = (
            "يدين", "تدين", "يعرب", "تعلن", "اعلنت", "أعلنت", "بيان",
            "تصريح", "اجتماع", "استقبل", "بحث", "ناقش", "اتصال",
            "condemns", "statement", "meeting", "announces", "minister",
        )
        if not any(normalize_text(x) in combined for x in article_signals):
            return True

    # Root or near-root URLs from discovery are usually portals, not stories.
    if not path or path.lower() in {"ar", "en", "arabic", "english", "home", "index"}:
        if len(tokenize(item.title)) <= 9:
            return True

    return False


def _known_country_names():
    """All configured country names, including compound names, as full entities."""
    names = set(COUNTRY_EN.keys())
    for places in REGIONS.values():
        names.update(places)
    return {normalize_text(x) for x in names if normalize_text(x)}



# Foreign-ministry entity registry. This is data-driven: the resolver applies
# the same institution + country rule to every configured country rather than
# special-casing the country that exposed a bug.
FOREIGN_MINISTRY_REGISTRY = {
    "saudi_arabia": {
        "country_aliases": ("السعودية", "السعوديه", "المملكة العربية السعودية", "السعودية", "Saudi Arabia", "Saudi"),
        "adjectives": ("السعودية", "السعوديه", "Saudi"),
        "domains": ("mofa.gov.sa",),
        "publication_paths": ("/ministry/statements", "/ministry/news"),
        "publication_terms": ("بيان", "تصريح", "وزير الخارجية", "اجتماع", "اتصال"),
        "publisher_name": "وزارة الخارجية السعودية",
        "index_urls": (
            "https://www.mofa.gov.sa/ar/ministry/statements/Pages/default.aspx",
            "https://www.mofa.gov.sa/ar/ministry/news/Pages/default.aspx",
        ),
    },
    "united_states": {
        "country_aliases": ("الولايات المتحدة", "الولايات المتحده", "أمريكا", "امريكا", "United States", "USA", "U.S.", "US"),
        "adjectives": ("الأمريكية", "الامريكيه", "الأميركية", "الاميركيه", "American", "U.S.", "US"),
        "domains": ("state.gov",),
        "institution_aliases": ("U.S. State Department", "US State Department", "United States Department of State", "Department of State", "State Department"),
        "publication_paths": ("/releases/", "/briefings/", "/remarks/"),
        "publication_terms": ("press release", "statement", "readout", "remarks", "briefing", "media note"),
    },
    "united_kingdom": {
        "country_aliases": ("بريطانيا", "المملكة المتحدة", "المملكه المتحده", "United Kingdom", "UK", "Britain"),
        "adjectives": ("البريطانية", "البريطانيه", "British", "UK"),
        "domains": ("gov.uk",),
        "institution_aliases": ("Foreign, Commonwealth & Development Office", "FCDO"),
        "publication_paths": ("/government/news/", "/government/speeches/", "/government/publications/"),
        "publication_terms": ("Foreign Commonwealth Development Office", "FCDO", "press release", "statement", "speech"),
    },
    "france": {
        "country_aliases": ("فرنسا", "France"),
        "adjectives": ("الفرنسية", "الفرنسيه", "French"),
        "domains": ("diplomatie.gouv.fr",),
        "publication_paths": ("/presse/", "/declarations-officielles-et-interventions",
                              "/presse-et-ressources/decouvrir-et-informer/actualites/"),
        "publication_terms": ("communiqué", "declaration", "déclaration", "point de presse", "entretien"),
        "publisher_name": "France Diplomatie",
        "index_urls": ("https://www.diplomatie.gouv.fr/fr/presse/espace-presse/declarations-officielles-et-interventions",),
    },
    "china": {
        "country_aliases": ("الصين", "China"),
        "adjectives": ("الصينية", "الصينيه", "Chinese"),
        "domains": ("mfa.gov.cn",),
        "publication_paths": ("/xw/fyrbt/", "/xw/wjbxw/", "/eng/xw/fyrbt/"),
        "publication_terms": ("spokesperson remarks", "regular press conference", "foreign ministry", "statement"),
        "publisher_name": "Ministry of Foreign Affairs of China",
        "index_urls": ("https://www.mfa.gov.cn/eng/xw/fyrbt/",),
    },
    "russia": {
        "country_aliases": ("روسيا", "Russia"),
        "adjectives": ("الروسية", "الروسيه", "Russian"),
        "domains": ("mid.ru",),
        "publication_terms": ("statement", "briefing", "comment", "meeting", "foreign ministry"),
    },
    "germany": {
        "country_aliases": ("ألمانيا", "المانيا", "Germany"),
        "adjectives": ("الألمانية", "الالمانيه", "German"),
        "domains": ("auswaertiges-amt.de",),
    },
    "italy": {
        "country_aliases": ("إيطاليا", "ايطاليا", "Italy"),
        "adjectives": ("الإيطالية", "الايطاليه", "Italian"),
        "domains": ("esteri.it",),
    },
    "spain": {
        "country_aliases": ("إسبانيا", "اسبانيا", "Spain"),
        "adjectives": ("الإسبانية", "الاسبانيه", "Spanish"),
        "domains": ("exteriores.gob.es",),
    },
    "turkey": {
        "country_aliases": ("تركيا", "Turkey", "Türkiye"),
        "adjectives": ("التركية", "التركيه", "Turkish"),
        "domains": ("mfa.gov.tr",),
    },
    "egypt": {
        "country_aliases": ("مصر", "Egypt"),
        "adjectives": ("المصرية", "المصريه", "Egyptian"),
        "domains": ("mfa.gov.eg",),
    },
    "uae": {
        "country_aliases": ("الإمارات", "الامارات", "الإمارات العربية المتحدة", "United Arab Emirates", "UAE"),
        "adjectives": ("الإماراتية", "الاماراتيه", "Emirati", "UAE"),
        "domains": ("mofa.gov.ae",),
    },
    "qatar": {
        "country_aliases": ("قطر", "Qatar"),
        "adjectives": ("القطرية", "القطريه", "Qatari"),
        "domains": ("mofa.gov.qa",),
    },
    "kuwait": {
        "country_aliases": ("الكويت", "Kuwait"),
        "adjectives": ("الكويتية", "الكويتيه", "Kuwaiti"),
        "domains": ("mofa.gov.kw",),
    },
    "bahrain": {
        "country_aliases": ("البحرين", "Bahrain"),
        "adjectives": ("البحرينية", "البحرينيه", "Bahraini"),
        "domains": ("mofa.gov.bh",),
    },
    "oman": {
        "country_aliases": ("عمان", "سلطنة عمان", "سلطنه عمان", "Oman"),
        "adjectives": ("العمانية", "العمانيه", "Omani"),
        "domains": ("fm.gov.om",),
    },
    "japan": {
        "country_aliases": ("اليابان", "Japan"),
        "adjectives": ("اليابانية", "اليابانيه", "Japanese"),
        "domains": ("mofa.go.jp",),
        "publication_paths": ("/press/release/", "/press/kaiken/"),
        "publication_terms": ("press release", "meeting", "courtesy call", "statement", "telephone talk"),
        "publisher_name": "Ministry of Foreign Affairs of Japan",
        "index_urls": ("https://www.mofa.go.jp/press/release/index.html",),
    },
    "india": {
        "country_aliases": ("الهند", "India"),
        "adjectives": ("الهندية", "الهنديه", "Indian"),
        "domains": ("mea.gov.in",),
    },
    "south_korea": {
        "country_aliases": ("كوريا الجنوبية", "كوريا الجنوبيه", "South Korea", "Republic of Korea"),
        "adjectives": ("الكورية الجنوبية", "الكوريه الجنوبيه", "South Korean", "Korean"),
        "domains": ("mofa.go.kr",),
    },
    "australia": {
        "country_aliases": ("أستراليا", "استراليا", "Australia"),
        "adjectives": ("الأسترالية", "الاستراليه", "Australian"),
        "domains": ("dfat.gov.au",),
    },
    "canada": {
        "country_aliases": ("كندا", "Canada"),
        "adjectives": ("الكندية", "الكنديه", "Canadian"),
        "domains": ("international.gc.ca",),
    },
    "ukraine": {
        "country_aliases": ("أوكرانيا", "اوكرانيا", "Ukraine"),
        "adjectives": ("الأوكرانية", "الاوكرانيه", "Ukrainian"),
        "domains": ("mfa.gov.ua",),
    },
}


# Registry coverage and successful collection are reported separately.
# Membership never supplies a ranking bonus.
G20_MEMBERS = frozenset((
    "argentina", "australia", "brazil", "canada", "china", "france",
    "germany", "india", "indonesia", "italy", "japan", "mexico", "russia",
    "saudi_arabia", "south_africa", "south_korea", "turkey",
    "united_kingdom", "united_states", "european_union", "african_union",
))
# Public publisher indexes, not a declaration of successful live retrieval.
# Refused/dynamic pages remain visible in diagnostics; no internal API fallback.
_OFFICIAL_PUBLIC_SOURCES = """argentina|foreign_affairs|الخارجية الأرجنتينية|https://www.cancilleria.gob.ar/es/comunicados-oficiales|/comunicados/;/actualidad/|1
argentina|central_bank|البنك المركزي الأرجنتيني|https://www.bcra.gob.ar/noticias/||0
australia|foreign_affairs|الخارجية الأسترالية|https://www.dfat.gov.au/news/departmental-media-releases|/news/|1
australia|central_bank|البنك الاحتياطي الأسترالي|https://www.rba.gov.au/media-releases/|/media-releases/|1
brazil|foreign_affairs|الخارجية البرازيلية|https://www.gov.br/mre/en/contact-us/press-area/press-releases|/press-releases/|1
brazil|government|الرئاسة البرازيلية|https://www.gov.br/planalto/en/latest-news|/latest-news/|0
canada|foreign_affairs|الخارجية الكندية|https://www.canada.ca/en/global-affairs/news.html|/global-affairs/news/|1
canada|central_bank|بنك كندا|https://www.bankofcanada.ca/press/||1
canada|government|رئيس الوزراء الكندي|https://www.pm.gc.ca/en/news|/news/|1
china|foreign_affairs|الخارجية الصينية|https://www.mfa.gov.cn/eng/xw/fyrbt/|/fyrbt/|1
china|government|الحكومة الصينية|https://english.www.gov.cn/news/|/news/;/policies/|0
france|foreign_affairs|الخارجية الفرنسية|https://www.diplomatie.gouv.fr/fr/presse/espace-presse/declarations-officielles-et-interventions|/declarations-officielles-et-interventions/;/actualites/|1
france|central_bank|بنك فرنسا|https://www.banque-france.fr/en/news|/news/|0
france|government|الرئاسة الفرنسية|https://www.elysee.fr/toutes-les-actualites||0
germany|foreign_affairs|الخارجية الألمانية|https://www.auswaertiges-amt.de/en/newsroom/news|/newsroom/news/|1
germany|central_bank|البنك الاتحادي الألماني|https://www.bundesbank.de/en/press/press-releases|/press/press-releases/|1
india|foreign_affairs|الخارجية الهندية|https://www.mea.gov.in/press-releases|/press-releases|1
india|central_bank|البنك الاحتياطي الهندي|https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx|bs_pressrelease|1
india|official_agency|مكتب المعلومات الصحفية الهندي|https://www.pib.gov.in/allRel.aspx?reg=48&lang=2|pressrelease|1
indonesia|foreign_affairs|الخارجية الإندونيسية|https://kemlu.go.id/||1
indonesia|central_bank|بنك إندونيسيا|https://www.bi.go.id/en/default.aspx|/news-release/|1
italy|foreign_affairs|الخارجية الإيطالية|https://www.esteri.it/en/sala_stampa/archivionotizie/comunicati/|/archivionotizie/comunicati/|1
italy|central_bank|بنك إيطاليا|https://www.bancaditalia.it/media/comunicati/index.html?com.dotmarketing.htmlpage.language=1|/media/comunicati/|1
japan|foreign_affairs|الخارجية اليابانية|https://www.mofa.go.jp/press/release/index.html|/press/release/|1
japan|central_bank|بنك اليابان|https://www.boj.or.jp/en/whatsnew/index.htm|/en/|0
mexico|foreign_affairs|الخارجية المكسيكية|https://www.gob.mx/sre/archivo/prensa|/sre/prensa/|1
mexico|finance|المالية المكسيكية|https://www.gob.mx/shcp/archivo/prensa|/shcp/prensa/|1
russia|foreign_affairs|الخارجية الروسية|https://www.mid.ru/en/press_service/spokesman/official_statement/|/press_service/|1
russia|central_bank|بنك روسيا|https://www.cbr.ru/eng/news/|/eng/press/;/eng/dkp/;/eng/news/|0
saudi_arabia|foreign_affairs|الخارجية السعودية|https://www.mofa.gov.sa/ar/ministry/statements/Pages/default.aspx|/ministry/statements/|1
saudi_arabia|central_bank|البنك المركزي السعودي|https://www.sama.gov.sa/ar-SA/MediaCenter/News/Pages/AllNews.aspx|/news/|1
south_africa|foreign_affairs|الخارجية الجنوب أفريقية|https://dirco.gov.za/media-statements/||1
south_africa|central_bank|البنك الاحتياطي الجنوب أفريقي|https://www.resbank.co.za/en/home/publications/media-releases|/publications/|1
south_korea|foreign_affairs|الخارجية الكورية الجنوبية|https://www.mofa.go.kr/eng/brd/m_5676/list.do|/m_5676/view.do|1
south_korea|central_bank|بنك كوريا|https://www.bok.or.kr/eng/bbs/E0000634/list.do?menuNo=400069|/e0000634/view.do|1
turkey|foreign_affairs|الخارجية التركية|https://www.mfa.gov.tr/sub.en.mfa?ad9093da-8e71-4678-a1b6-05f297baadc4=||1
turkey|central_bank|البنك المركزي التركي|https://www.tcmb.gov.tr/wps/wcm/connect/en/tcmb+en/main+menu/announcements/press+releases|/press|1
united_kingdom|foreign_affairs|الخارجية البريطانية|https://www.gov.uk/government/organisations/foreign-commonwealth-development-office|/government/news/;/government/speeches/|1
united_kingdom|central_bank|بنك إنجلترا|https://www.bankofengland.co.uk/news|/news/|1
united_kingdom|defence|الدفاع البريطانية|https://www.gov.uk/government/organisations/ministry-of-defence|/government/news/;/government/speeches/|1
united_states|foreign_affairs|الخارجية الأمريكية|https://www.state.gov/releases|/releases/;/briefings/;/remarks/|1
united_states|central_bank|الاحتياطي الفيدرالي الأمريكي|https://www.federalreserve.gov/newsevents/pressreleases/{yyyy}-press.htm|/newsevents/pressreleases/|1
united_states|finance|الخزانة الأمريكية|https://home.treasury.gov/news/press-releases|/news/press-releases/|1
european_union|council|مجلس الاتحاد الأوروبي|https://www.consilium.europa.eu/en/press/press-releases/|/press/press-releases/|1
european_union|central_bank|البنك المركزي الأوروبي|https://www.ecb.europa.eu/press/pubbydate/html/index.en.html?name_of_publication=Press%20release|/press/pr/|1
african_union|commission|مفوضية الاتحاد الأفريقي|https://au.int/en/press-releases|/pressreleases/|1
african_union|peace_security|مجلس السلم والأمن الأفريقي|https://www.peaceau.org/en/|/article/|1
argentina|government|الحكومة الأرجنتينية|https://www.argentina.gob.ar/noticias|/noticias/|0
argentina|defence|الدفاع الأرجنتينية|https://www.argentina.gob.ar/defensa/noticias|/defensa/noticias/|1
argentina|economy|الاقتصاد الأرجنتينية|https://www.argentina.gob.ar/economia/noticias|/economia/noticias/|1
australia|government|رئاسة الوزراء الأسترالية|https://www.pm.gov.au/media|/media/|1
australia|defence|الدفاع الأسترالية|https://www.defence.gov.au/news-events/releases|/news-events/releases/|1
australia|finance|الخزانة الأسترالية|https://treasury.gov.au/media-release|/media-release/|1
brazil|central_bank|البنك المركزي البرازيلي|https://www.bcb.gov.br/en/about/pressreleases|/pressreleases/|1
brazil|defence|الدفاع البرازيلية|https://www.gov.br/defesa/pt-br/centrais-de-conteudo/noticias|/noticias/|1
brazil|finance|المالية البرازيلية|https://www.gov.br/fazenda/pt-br/assuntos/noticias|/assuntos/noticias/|1
canada|defence|الدفاع الكندية|https://www.canada.ca/en/department-national-defence/news.html|/department-national-defence/news/|1
canada|finance|المالية الكندية|https://www.canada.ca/en/department-finance/news.html|/department-finance/news/|1
china|central_bank|بنك الشعب الصيني|https://www.pbc.gov.cn/en/3688110/index.html|/en/|0
china|defence|الدفاع الصينية|https://eng.mod.gov.cn/xb/News_213114/|/news_213114/|0
china|finance|المالية الصينية|https://www.mof.gov.cn/en/News/|/en/news/|0
france|defence|الدفاع الفرنسية|https://www.defense.gouv.fr/actualites|/actualites/|0
france|finance|الاقتصاد والمالية الفرنسية|https://www.economie.gouv.fr/actualites|/actualites/|0
germany|government|الحكومة الألمانية|https://www.bundesregierung.de/breg-en/news|/breg-en/news/|0
germany|defence|الدفاع الألمانية|https://www.bmvg.de/en/news|/en/|0
germany|finance|المالية الألمانية|https://www.bundesfinanzministerium.de/Content/EN/Standardartikel/Press_Room/Press-Releases/press-releases.html|/press-releases/|1
india|government|رئاسة الوزراء الهندية|https://www.pmindia.gov.in/en/news_updates/|/news_updates/|1
india|defence|الدفاع الهندية|https://www.pib.gov.in/AllRel.aspx?reg=3&lang=2|pressrelease|1
india|finance|المالية الهندية|https://www.finmin.gov.in/news|/news/|0
indonesia|government|الرئاسة الإندونيسية|https://www.presidenri.go.id/siaran-pers/|/siaran-pers/|1
indonesia|defence|الدفاع الإندونيسية|https://www.kemhan.go.id/category/berita|/category/berita/|0
indonesia|finance|المالية الإندونيسية|https://www.kemenkeu.go.id/informasi-publik/publikasi/berita-utama|/berita-utama/|0
italy|government|الحكومة الإيطالية|https://www.governo.it/en/media|/en/|0
italy|defence|الدفاع الإيطالية|https://www.difesa.it/eng/primo-piano/Pagine/default.aspx|/eng/primo-piano/|0
italy|finance|الاقتصاد والمالية الإيطالية|https://www.mef.gov.it/en/ufficio-stampa/comunicati/|/ufficio-stampa/comunicati/|1
japan|government|رئاسة الوزراء اليابانية|https://japan.kantei.go.jp/ongoingtopics/index.html|/ongoingtopics/|0
japan|defence|الدفاع اليابانية|https://www.mod.go.jp/en/article/|/en/article/|0
japan|finance|المالية اليابانية|https://www.mof.go.jp/english/policy/index.htm|/english/|0
mexico|government|الرئاسة المكسيكية|https://www.gob.mx/presidencia/archivo/prensa|/presidencia/prensa/|1
mexico|defence|الدفاع المكسيكية|https://www.gob.mx/defensa/archivo/prensa|/defensa/prensa/|1
mexico|central_bank|بنك المكسيك|https://www.banxico.org.mx/publications-and-press/|/publications-and-press/|0
russia|government|الحكومة الروسية|https://government.ru/en/news/|/en/news/|0
russia|defence|الدفاع الروسية|https://eng.mil.ru/en/news_page/country.htm|/news_page/|0
russia|finance|المالية الروسية|https://minfin.gov.ru/en/press-center/|/press-center/|0
saudi_arabia|government|وكالة الأنباء السعودية|https://www.spa.gov.sa/en|/en/|0
saudi_arabia|defence|الدفاع السعودية|https://www.mod.gov.sa/MediaCenter/Pages/default.aspx|/mediacenter/|0
saudi_arabia|finance|المالية السعودية|https://www.mof.gov.sa/en/mediacenter/news/Pages/default.aspx|/mediacenter/news/|1
south_africa|government|رئاسة جنوب أفريقيا|https://www.thepresidency.gov.za/press-statements|/press-statements/|1
south_africa|defence|الدفاع الجنوب أفريقية|https://www.dod.mil.za/news|/news/|0
south_africa|finance|الخزانة الجنوب أفريقية|https://www.treasury.gov.za/comm_media/press/|/comm_media/press/|1
south_korea|government|رئاسة كوريا الجنوبية|https://www.president.go.kr/newsroom/|/newsroom/|0
south_korea|defence|الدفاع الكورية الجنوبية|https://www.mnd.go.kr/mbshome/mbs/mndEN/subview.jsp?id=mndEN_020100000000|/mnden/|0
south_korea|finance|الاقتصاد والمالية الكورية|https://english.mofe.go.kr/pc/selectTbPressCenterList.do?boardCd=N0001|/pc/|1
turkey|government|الرئاسة التركية|https://www.tccb.gov.tr/en/news/542/|/en/news/|0
turkey|defence|الدفاع التركية|https://www.msb.gov.tr/SlaytHaber/|/slaythaber/|0
turkey|finance|الخزانة والمالية التركية|https://www.hmb.gov.tr/haberler|/haberler/|0
united_kingdom|government|رئاسة الوزراء البريطانية|https://www.gov.uk/government/organisations/prime-ministers-office-10-downing-street|/government/news/;/government/speeches/|1
united_kingdom|finance|الخزانة البريطانية|https://www.gov.uk/government/organisations/hm-treasury|/government/news/;/government/publications/|1
united_states|government|البيت الأبيض|https://www.whitehouse.gov/briefing-room/|/briefing-room/|1
united_states|defence|الدفاع الأمريكية|https://www.defense.gov/News/Releases/|/news/releases/|1
european_union|commission|المفوضية الأوروبية|https://ec.europa.eu/commission/presscorner/home/en|/commission/presscorner/|1
european_union|foreign_affairs|جهاز العمل الخارجي الأوروبي|https://www.eeas.europa.eu/eeas/press-material_en|/eeas/|1
european_union|defence|وكالة الدفاع الأوروبية|https://eda.europa.eu/news-and-events/news|/news-and-events/news/|0
african_union|official_agency|وكالة نيباد للتنمية|https://www.nepad.org/news|/news/|0"""
OFFICIAL_SOURCE_REGISTRY = {}
for _row in _OFFICIAL_PUBLIC_SOURCES.splitlines():
    _member, _institution, _name, _url, _paths, _dedicated = _row.split("|")
    _source_id = f"{_member}:{_institution}:0"
    _base = FOREIGN_MINISTRY_REGISTRY.get(_member, {}) if _institution == "foreign_affairs" else {}
    OFFICIAL_SOURCE_REGISTRY[_source_id] = {
        **_base, "source_id": _source_id, "member_id": _member,
        "institution": _institution, "scope": "g20", "publisher_name": _name,
        "domains": ((urlparse(_url).hostname or "").removeprefix("www."),),
        "index_urls": (_url,), "publication_paths": tuple(filter(None, _paths.split(";"))),
        "statement_index": _dedicated == "1",
    }
    if _institution == "foreign_affairs":
        FOREIGN_MINISTRY_REGISTRY[_member] = {
            **_base, "country_aliases": _base.get("country_aliases", (_member.replace("_", " "),)),
            "adjectives": _base.get("adjectives", ()),
            **{k: OFFICIAL_SOURCE_REGISTRY[_source_id][k] for k in
               ("domains", "index_urls", "publication_paths", "publisher_name")},
        }


for _source in OFFICIAL_SOURCE_REGISTRY.values():
    if _source["member_id"] == "saudi_arabia" and _source["institution"] == "foreign_affairs":
        _source["date_order"] = "mdy"
    elif _source["member_id"] in {"germany", "france", "italy", "brazil", "argentina"}:
        _source["date_order"] = "dmy"
    _source["date_group_headings"] = _source["source_id"] in {
        "japan:foreign_affairs:0", "india:central_bank:0",
        "united_states:central_bank:0", "european_union:council:0",
    }



_AR_MONTHS = {
    "يناير": 1, "فبراير": 2, "مارس": 3, "أبريل": 4, "ابريل": 4,
    "مايو": 5, "يونيو": 6, "يوليو": 7, "أغسطس": 8, "اغسطس": 8,
    "سبتمبر": 9, "أكتوبر": 10, "اكتوبر": 10, "نوفمبر": 11, "ديسمبر": 12,
}
_EN_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}
_FR_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}


class _OfficialAnchorParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links = []
        self._href = None
        self._text = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        attrs = dict(attrs)
        self._href = attrs.get("href")
        self._text = []

    def handle_data(self, data):
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag.lower() != "a" or self._href is None:
            return
        title = re.sub(r"\s+", " ", " ".join(self._text)).strip()
        if self._href and title:
            self.links.append((self._href.strip(), title))
        self._href = None
        self._text = []


def _official_index_url(template, now=None):
    now = now or datetime.now(timezone.utc)
    return str(template).format(yyyymm=now.strftime("%Y%m"), yyyy=now.strftime("%Y"), mm=now.strftime("%m"))


def _official_link_title(href, anchor_title):
    """Return a human article title from a public index link.

    Some official portals use a generic anchor such as ``قراءة المزيد`` for
    every article while the public article URL itself contains the headline.
    In that case derive the title from the visible public URL slug instead of
    discarding the link.  No hidden endpoint or non-public metadata is used.
    """
    title = re.sub(r"\s+", " ", html.unescape(str(anchor_title or ""))).strip()
    generic = {
        normalize_text("قراءة المزيد"), normalize_text("اقرأ المزيد"),
        normalize_text("المزيد"), normalize_text("read more"),
        normalize_text("learn more"), normalize_text("more"),
    }
    if title and normalize_text(title) not in generic and len(tokenize(title)) >= 3:
        return title

    try:
        path = urllib.parse.unquote(urlparse(str(href or "")).path or "")
    except Exception:
        path = ""
    leaf = path.rstrip("/").rsplit("/", 1)[-1]
    leaf = re.sub(r"\.(?:aspx?|html?|php)$", "", leaf, flags=re.I)
    leaf = re.sub(r"[-_]+", " ", leaf)
    leaf = re.sub(r"\s+", " ", leaf).strip()
    return leaf if len(tokenize(leaf)) >= 3 else title


def _official_article_links(index_html, index_url, profile, max_links=None):
    """Extract article links from a verified ministry publication index."""
    parser = _OfficialAnchorParser()
    try:
        parser.feed(index_html or "")
    except Exception:
        return []

    document = _OfficialDocumentParser(index_html)
    domains = set(profile.get("domains", ()))
    paths = [str(x).strip("/").lower() for x in profile.get("publication_paths", ()) if str(x).strip("/")]
    results = []
    seen = set()
    link_cap = OFFICIAL_INDEX_MAX_LINKS if max_links is None else max(1, int(max_links))
    visible_links = []
    for node in document.nodes:
        if node["tag"] != "a":
            continue
        parent, hidden = node, False
        while parent:
            attrs = parent["attrs"]
            if (parent["tag"] in document.HIDDEN or "hidden" in attrs
                    or attrs.get("aria-hidden") == "true"
                    or "display:none" in attrs.get("style", "").replace(" ", "").lower()):
                hidden = True
                break
            parent = parent["parent"]
        if not hidden:
            visible_links.append((node["attrs"].get("href", ""), document.visible(node)))
    for href, title in visible_links:
        absolute = urljoin(index_url, href)
        title = _official_link_title(absolute, title)
        if len(tokenize(title)) < 3 or normalize_text(title) in {normalize_text("قراءة المزيد"), "read more"}:
            title = _official_card_title(document, index_url, absolute) or title
        parsed = urlparse(absolute)
        domain = (parsed.hostname or "").lower().replace("www.", "")
        path = (parsed.path or "").lower().strip("/")
        if parsed.scheme not in {"http", "https"} or not _domain_matches(domain, domains):
            continue
        if absolute.split("#", 1)[0].rstrip("/") == index_url.rstrip("/"):
            continue
        if paths and not any(p in path for p in paths):
            continue
        leaf = path.rsplit("/", 1)[-1]
        if not leaf or leaf in {"default.aspx", "index.html", "index.htm", "index"}:
            continue
        if len(tokenize(title)) < 3 and not (len(title) >= 10 and re.search(r"[\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af]", title)):
            continue
        if not paths:
            hint_profile = {**profile, "resolved_index_url": index_url}
            if not _official_index_date_hint(index_html, title, hint_profile, absolute, document):
                # Generic portals must associate links with an actual article card.
                matching = [n for n in document.nodes if n["tag"] == "a" and urljoin(index_url, n["attrs"].get("href", "")) == absolute]
                card = False
                for node in matching:
                    for _ in range(5):
                        node = node["parent"]
                        if not node:
                            break
                        if node["tag"] == "article":
                            card = True
                if not card and path.rsplit("/", 1)[-1].count("-") < 4:
                    continue
        key = absolute.split("#", 1)[0]
        if key in seen:
            continue
        seen.add(key)
        results.append((absolute.split("#", 1)[0], title))
        if len(results) >= link_cap:
            break
    return results


class _OfficialDocumentParser(HTMLParser):
    """Small bounded HTML tree for visible publisher evidence only."""
    VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input",
            "link", "meta", "param", "source", "track", "wbr"}
    HIDDEN = {"script", "style", "noscript", "template", "nav", "footer"}

    def __init__(self, text):
        super().__init__(convert_charrefs=True)
        self.root = {"tag": "root", "attrs": {}, "children": [], "parent": None}
        self.stack = [self.root]
        self.nodes = []
        self.feed(text[:OFFICIAL_MAX_PAGE_BYTES])
        self.close()

    def handle_starttag(self, tag, attrs):
        if len(self.nodes) >= 25000:
            raise ValueError("official_document_too_complex")
        # Common HTML optional closing tags.
        if tag in {"li", "p"} and self.stack[-1]["tag"] == tag:
            self.stack.pop()
        node = {"tag": tag, "attrs": dict(attrs), "children": [], "parent": self.stack[-1]}
        self.stack[-1]["children"].append(node)
        self.nodes.append(node)
        if tag not in self.VOID:
            self.stack.append(node)

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i]["tag"] == tag:
                del self.stack[i:]
                break

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in self.VOID:
            self.handle_endtag(tag)

    def handle_data(self, data):
        self.stack[-1]["children"].append(data)

    @classmethod
    def visible(cls, node):
        parts = []
        pending = [node]
        while pending:
            current = pending.pop()
            if isinstance(current, str):
                parts.append(current)
                continue
            attrs = current["attrs"]
            if (current["tag"] in cls.HIDDEN or "hidden" in attrs
                    or attrs.get("aria-hidden") == "true"
                    or re.search(r"display\s*:\s*none", attrs.get("style", ""), re.I)):
                continue
            pending.extend(reversed(current["children"]))
        return re.sub(r"\s+", " ", " ".join(parts)).strip()


def _official_calendar_dates(value, date_order=None):
    """Explicit Gregorian dates; never guess the year or convert Hijri approximately."""
    value = str(value).translate(str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789"))
    found = set()
    def add(year, month, day):
        try:
            found.add(datetime(int(year), int(month), int(day), tzinfo=timezone.utc))
        except (TypeError, ValueError):
            pass
    for year, month, day in re.findall(r"\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b", value):
        add(year, month, day)
    extra = {}
    for month_names in (
        "enero febrero marzo abril mayo junio julio agosto septiembre octubre noviembre diciembre",
        "janeiro fevereiro março abril maio junho julho agosto setembro outubro novembro dezembro",
        "gennaio febbraio marzo aprile maggio giugno luglio agosto settembre ottobre novembre dicembre",
        "januar februar märz april mai juni juli august september oktober november dezember",
        "января февраля марта апреля мая июня июля августа сентября октября ноября декабря",
        "januari februari maret april mei juni juli agustus september oktober november desember",
        "ocak şubat mart nisan mayıs haziran temmuz ağustos eylül ekim kasım aralık",
    ):
        extra.update({name: i for i, name in enumerate(month_names.split(), 1)})
    extra.update({name[:3]: i for name, i in _EN_MONTHS.items()})
    extra["sept"] = 9
    value = re.sub(r"(?<=\d)\.\s", " ", value)
    value = re.sub(r"\bde\b", " ", value, flags=re.I)
    value = re.sub(r"([A-Za-z]{3,4})\.", r"\1", value)
    for year, month, day in re.findall(r"(20\d{2})[年.]\s*(\d{1,2})[月.]\s*(\d{1,2})", value):
        add(year, month, day)
    for year, month, day in re.findall(r"(20\d{2})년\s*(\d{1,2})월\s*(\d{1,2})일", value):
        add(year, month, day)
    if date_order in {"dmy", "mdy"}:
        for first, second, year in re.findall(r"\b(\d{1,2})[/.](\d{1,2})[/.](20\d{2})\b", value):
            add(year, second if date_order == "dmy" else first, first if date_order == "dmy" else second)
    months = {**extra, **_AR_MONTHS, **_FR_MONTHS, **_EN_MONTHS}
    names = "|".join(sorted(map(re.escape, months), key=len, reverse=True))
    for day, month, year in re.findall(rf"(\d{{1,2}})\s+({names})\s+(20\d{{2}})", value, re.I):
        add(year, months[month.lower()], day)
    for month, day, year in re.findall(rf"({names})\s+(\d{{1,2}}),?\s+(20\d{{2}})", value, re.I):
        add(year, months[month.lower()], day)
    return found


def _official_card_title(document, base, target):
    for anchor in document.nodes:
        if anchor["tag"] != "a" or urljoin(base, anchor["attrs"].get("href", "")) != target:
            continue
        parent = anchor["parent"]
        for _ in range(5):
            if not parent or parent["tag"] in {"root", "body", "main", "ul", "ol"}:
                break
            pending = [parent]
            headings = []
            while pending:
                node = pending.pop()
                if isinstance(node, str):
                    continue
                if node["tag"] in {"h2", "h3", "h4"}:
                    headings.append(document.visible(node))
                pending.extend(node["children"])
            if len(headings) == 1:
                return headings[0]
            if len(headings) > 1:
                break
            parent = parent["parent"]
    return ""


def _official_index_date_hint(index_html, title, profile, article_url=None, document=None):
    """Accept dates only from the smallest card tied to this exact article URL."""
    if not article_url:
        return None
    document = document or _OfficialDocumentParser(index_html)
    base = profile.get("resolved_index_url", "")
    target = article_url.split("#", 1)[0]
    anchors = [n for n in document.nodes if n["tag"] == "a"
               and urljoin(base, n["attrs"].get("href", "")).split("#", 1)[0] == target]
    evidence = set()
    for anchor in anchors:
        parent = anchor["parent"]
        for _ in range(5):
            if not parent or parent["tag"] in {"root", "body", "main", "ul", "ol"}:
                break
            text = document.visible(parent)
            if len(text) > 1800:
                break
            date_text = text.replace(document.visible(anchor), "")
            dates = _official_calendar_dates(date_text, profile.get("date_order"))
            if not dates and profile.get("member_id") == "turkey" and profile.get("institution") == "foreign_affairs":
                dates = _official_calendar_dates(document.visible(anchor))
            if dates:
                # Any other substantive anchor makes attribution ambiguous.
                pending = [parent]
                other = False
                while pending:
                    node = pending.pop()
                    if isinstance(node, str):
                        continue
                    if node["tag"] == "a":
                        href = node["attrs"].get("href", "")
                        if href and not href.startswith("#"):
                            url = urljoin(base, href).split("#", 1)[0]
                            if url != target:
                                other = True
                    pending.extend(node["children"])
                if not other and len(dates) == 1 and not re.search(r"updated|modified|mis à jour|تحديث", text, re.I):
                    evidence.update(dates)
                break
            parent = parent["parent"]
    if not evidence and profile.get("date_group_headings") and anchors:
        # A date heading may label several sibling releases (e.g. Japan/RBI).
        # It must belong to an ancestor of the link, never to a neighbouring card.
        anchor = anchors[0]
        ancestors, parent = [], anchor["parent"]
        while parent:
            ancestors.append(parent)
            parent = parent["parent"]
        for node in document.nodes:
            if node is anchor:
                break
            if node["tag"] not in {"h2", "h3", "h4", "b", "strong", "p", "div"}:
                continue
            if not any(node["parent"] is ancestor for ancestor in ancestors):
                continue
            value = document.visible(node)
            if not re.fullmatch(r"(?:[A-Za-z]{3,9}\.?\s+\d{1,2},?\s+20\d{2}|\d{1,2}\s+[A-Za-z]{3,9}\s+20\d{2}|20\d{2}-\d{2}-\d{2})", value):
                continue
            dates = _official_calendar_dates(value)
            if len(dates) == 1:
                evidence = dates
        # Some publisher indexes use a date row followed by several release
        # rows. Accept the closest preceding date only when both rows share a
        # bounded container such as a table or section. This covers RBI-style
        # grouped lists without borrowing a date from a neighbouring article.
        if not evidence:
            anchor = anchors[0]
            anchor_pos = document.nodes.index(anchor)
            anchor_ancestors = []
            parent = anchor["parent"]
            while parent:
                anchor_ancestors.append(parent)
                parent = parent["parent"]
            for node in reversed(document.nodes[max(0, anchor_pos - 160):anchor_pos]):
                if node["tag"] not in {"h2", "h3", "h4", "b", "strong", "p", "div", "span", "time", "td", "th"}:
                    continue
                value = document.visible(node)
                if not re.fullmatch(r"(?:[A-Za-z]{3,9}\.?(?:\s+|\s*,\s*)\d{1,2},?\s+20\d{2}|\d{1,2}\s+[A-Za-z]{3,9}\s+20\d{2}|20\d{2}-\d{2}-\d{2})", value):
                    continue
                node_ancestors = []
                parent = node["parent"]
                while parent:
                    node_ancestors.append(parent)
                    parent = parent["parent"]
                common = next((candidate for candidate in anchor_ancestors
                               if any(candidate is other for other in node_ancestors)), None)
                if common is None or common["tag"] in {"root", "body", "main"}:
                    continue
                dates = _official_calendar_dates(value, profile.get("date_order"))
                if len(dates) == 1:
                    evidence = dates
                    break
    return next(iter(evidence)) if len(evidence) == 1 else None


def _visible_official_date(text, profile):
    """Read visible publication labels, excluding scripts and modification dates."""
    if not text:
        return None
    document = _OfficialDocumentParser(text)
    visible = document.visible(document.root)
    # Label-specific captures stop before a separate modification timestamp.
    labels = r"Published(?: on)?|Publié le|Le\s*:|Publicado(?: em| el)?|Pubblicato(?: il)?|Veröffentlicht(?: am)?|(?<![A-Za-z])Date(?![A-Za-z])|تاريخ النشر|نشر بتاريخ|الموافق"
    for match in re.finditer(rf"(?:{labels})\s*:?\s*(.{{1,65}})", visible, re.I):
        if re.search(r"updated|modified|mis à jour|تحديث", visible[max(0, match.start() - 20):match.start()], re.I):
            continue
        value = re.split(r"updated|modified|mis à jour|تحديث", match.group(1), flags=re.I)[0]
        dates = _official_calendar_dates(value, profile.get("date_order"))
        if len(dates) == 1:
            return next(iter(dates))
    # Several central banks display a compact standalone publication line.
    # Require an explicit release marker so an event date in body text cannot
    # become publication evidence.
    for node in document.nodes:
        if node["tag"] not in {"p", "div", "span", "time"}:
            continue
        value = document.visible(node)
        if len(value) > 90 or not re.search(r"press release|media release|communiqu[eé]", value, re.I):
            continue
        dates = _official_calendar_dates(value, profile.get("date_order"))
        if len(dates) == 1:
            return next(iter(dates))
    # Japan prints the release date as a standalone line just after the title.
    # Restrict this to that publisher and to a small element containing only a date.
    if _domain_matches("mofa.go.jp", set(profile.get("domains", ()))):
        for node in document.nodes:
            if node["tag"] not in {"p", "span", "div", "time"}:
                continue
            value = document.visible(node)
            if re.fullmatch(r"[A-Za-z]+\s+\d{1,2},\s+20\d{2}", value):
                dates = _official_calendar_dates(value)
                if len(dates) == 1:
                    return next(iter(dates))
    return None


async def _read_official_page(session, url, profile, semaphore):
    """Public HTML/RSS only, with allowlisted redirects checked before requesting."""
    async with semaphore:
        async with asyncio.timeout(OFFICIAL_INDEX_TIMEOUT):
            for _ in range(4):
                parsed = urlparse(url)
                if (parsed.scheme not in {"http", "https"} or parsed.username or parsed.password
                        or not _domain_matches((parsed.hostname or "").removeprefix("www."), profile["domains"])):
                    return None
                timeout = aiohttp.ClientTimeout(total=OFFICIAL_INDEX_TIMEOUT, connect=FETCH_CONNECT_TIMEOUT)
                async with session.get(url, timeout=timeout, allow_redirects=False,
                                       headers={"User-Agent": "Global-Intel-Bot/2.0"}) as response:
                    if response.status in {301, 302, 303, 307, 308}:
                        location = response.headers.get("Location")
                        if not location:
                            return None
                        url = urljoin(url, location)
                        continue
                    if response.status != 200:
                        log.info("Official page rejected source=%s status=%s", profile.get("source_id", ""), response.status)
                        return None
                    mime = response.headers.get("Content-Type", "").lower()
                    if mime and not any(t in mime for t in ("html", "xml", "text/plain", "rss", "atom")):
                        return None
                    chunks, size = [], 0
                    async for chunk in response.content.iter_chunked(65536):
                        size += len(chunk)
                        if size > OFFICIAL_MAX_PAGE_BYTES:
                            log.info("Official page rejected source=%s reason=oversized", profile.get("source_id", ""))
                            return None
                        chunks.append(chunk)
                    body = b"".join(chunks).decode(response.charset or "utf-8", errors="replace")
                    return str(response.url), (response.url.host or "").removeprefix("www."), body
    return None


def _official_public_alternates(document, base, profile):
    """Follow only a feed/current-year archive actually advertised on this page."""
    feeds, archives = [], []
    year = str(datetime.now(timezone.utc).year)
    for node in document.nodes:
        attrs = node["attrs"]
        href = attrs.get("href", "")
        if not href or node["tag"] not in {"a", "link"}:
            continue
        url = urljoin(base, href).split("#", 1)[0]
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not _domain_matches((parsed.hostname or "").removeprefix("www."), profile["domains"]):
            continue
        if url.rstrip("/") == base.rstrip("/"):
            continue
        if node["tag"] == "link" and "alternate" in attrs.get("rel", "").lower() and attrs.get("type", "").lower() in {"application/rss+xml", "application/atom+xml"}:
            feeds.append(url)
        elif node["tag"] == "a" and document.visible(node).strip() == year and year in parsed.path:
            if parsed.path.startswith(urlparse(base).path.rsplit("/", 1)[0]):
                archives.append(url)
    return list(dict.fromkeys(feeds))[:1], list(dict.fromkeys(archives))[:1]


def _official_feed_items(body, profile):
    """Published RSS/Atom timestamps are evidence; updated-only feeds are excluded."""
    parsed = feedparser.parse(body)
    result = []
    for entry in parsed.entries[:40]:
        published = parse_date(entry.get("published")) if entry.get("published") else None
        url, title = entry.get("link", ""), re.sub(r"<[^>]+>", "", html.unescape(entry.get("title", ""))).strip()
        target = urlparse(url)
        if not title or published is None or target.scheme not in {"http", "https"}:
            continue
        domain = (target.hostname or "").removeprefix("www.")
        if not _domain_matches(domain, profile["domains"]):
            continue
        paths = profile.get("publication_paths", ())
        if paths and not any(path.lower().strip("/") in target.path.lower() for path in paths):
            continue
        item = NewsItem(title=title, original_title=title, url=url,
            source=profile["publisher_name"], published=published, domain=domain,
            official=True, trust_score=99.0, official_source_id=profile["source_id"],
            publication_evidence="publisher_advertised_feed_publication_date")
        if _is_current_news(item):
            result.append(classify_item(item))
    return result


async def _fetch_official_article(session, profile, url, title, semaphore, published_hint=None):
    try:
        page = await _read_official_page(session, url, profile, semaphore)
        if page is None:
            return None
        final_url, final_domain, body = page
        published = _visible_official_date(body, profile)
        evidence = "visible_article_publication_date"
        if published is None:
            published = published_hint
            evidence = "visible_index_card_date"
        if published is None:
            log.info("Official article skipped domain=%s reason=no_publication_date", final_domain)
            return None
        item = NewsItem(
            title=title, original_title=title, url=final_url,
            source=profile.get("publisher_name") or final_domain,
            published=published, domain=final_domain, official=True, trust_score=99.0,
            official_source_id=profile.get("source_id", ""), publication_evidence=evidence,
        )
        if not _is_current_news(item):
            log.info("Official article skipped domain=%s reason=outside_current_window date=%s",
                     final_domain, published.date().isoformat())
            return None
        return classify_item(item)
    except (asyncio.TimeoutError, aiohttp.ClientError):
        log.info("Official article unavailable source=%s", profile.get("source_id", ""))
        return None
    except Exception:
        log.exception("Official article parse failed source=%s", profile.get("source_id", ""))
        return None


async def _fetch_official_index(session, profile, index_url, semaphore, max_links=None, result_sink=None, article_semaphore=None):
    try:
        page = await _read_official_page(session, index_url, profile, semaphore)
        if page is None:
            return []
        final_url, _, body = page
        profile = {**profile, "resolved_index_url": final_url}
        links = _official_article_links(body, final_url, profile, max_links=max_links)
        document = _OfficialDocumentParser(body)
        log.info("Official public index source=%s links=%d", profile.get("source_id", ""), len(links))
        dated_links = [(url, title, _official_index_date_hint(
            body, title, profile, article_url=url, document=document)) for url, title in links]
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Official index failed source=%s", profile.get("source_id", ""))
        return []

    items = []
    async def collect_alternates():
        feeds, archives = _official_public_alternates(document, final_url, profile)
        for url in feeds + (archives if len(links) < 3 else []):
            try:
                page = await _read_official_page(session, url, profile, article_semaphore or semaphore)
                if page is None:
                    continue
                alternative_url, _, alternative_body = page
                if url in feeds:
                    found = _official_feed_items(alternative_body, profile)
                    items.extend(found)
                    if result_sink is not None:
                        result_sink.extend(found)
                else:
                    doc = _OfficialDocumentParser(alternative_body)
                    child_profile = {**profile, "resolved_index_url": alternative_url}
                    for article_url, title in _official_article_links(alternative_body, alternative_url, child_profile, max_links):
                        hint = _official_index_date_hint(alternative_body, title, child_profile, article_url, doc)
                        if hint is not None:
                            await collect_one(article_url, title, hint)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.info("Official advertised alternative unavailable source=%s", profile["source_id"])

    async def collect_one(url, title, hint):
        if hint is not None:
            # The exact dated public card is sufficient original-publisher evidence.
            item = NewsItem(title=title, original_title=title, url=url,
                source=profile["publisher_name"], published=hint,
                domain=(urlparse(url).hostname or "").removeprefix("www."),
                official=True, trust_score=99.0, official_source_id=profile["source_id"],
                publication_evidence="visible_index_card_date")
            item = classify_item(item) if _is_current_news(item) else None
        else:
            if urlparse(url).path.lower().endswith((".pdf", ".xls", ".xlsx", ".zip")):
                return
            item = await _fetch_official_article(session, profile, url, title, article_semaphore or semaphore, hint)
        if item is not None:
            items.append(item)
            # Publish each completed article immediately, before sibling tasks finish.
            if result_sink is not None:
                result_sink.append(item)

    bounded_links, unknown = [], 0
    for entry in dated_links:
        if entry[2] is None:
            unknown += 1
            if unknown > 2:
                continue
        bounded_links.append(entry)
    tasks = [asyncio.create_task(collect_alternates())] + [asyncio.create_task(collect_one(*entry)) for entry in bounded_links]
    try:
        if tasks:
            await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        log.info("Official source completed source=%s accepted=%d", profile.get("source_id", ""), len(items))
    return items


def official_source_coverage():
    return {member: sum(p["member_id"] == member for p in OFFICIAL_SOURCE_REGISTRY.values())
            for member in sorted(G20_MEMBERS)}


_OFFICIAL_INSTITUTION_PRIORITY = {
    "government": 0,
    "foreign_affairs": 1,
    "defence": 2,
    "finance": 3,
    "economy": 3,
    "central_bank": 4,
    "council": 5,
    "commission": 5,
    "peace_security": 5,
    "official_agency": 6,
}


def _official_collection_profiles(country_id=None, per_member=None, round_index=0):
    """Return a fair institution rotation without giving any G20 member priority."""
    by_member = {}
    for profile in OFFICIAL_SOURCE_REGISTRY.values():
        member = profile["member_id"]
        if country_id is None or member == country_id:
            by_member.setdefault(member, []).append(profile)
    for profiles in by_member.values():
        profiles.sort(key=lambda profile: (
            _OFFICIAL_INSTITUTION_PRIORITY.get(profile["institution"], 99),
            profile["source_id"],
        ))

    members = sorted(by_member)
    if members:
        offset = int(time.time() // ROTATION_WINDOW_SECONDS) % len(members)
        members = members[offset:] + members[:offset]

    if per_member is not None:
        selected = {}
        cap = max(1, int(per_member))
        for member, profiles in by_member.items():
            if len(profiles) <= cap:
                selected[member] = profiles
                continue
            start = (max(0, int(round_index)) * cap) % len(profiles)
            selected[member] = [
                profiles[(start + index) % len(profiles)]
                for index in range(cap)
            ]
        by_member = selected

    # One source per member per round, rather than exhausting one member first.
    return [by_member[m][i] for i in range(max((len(v) for v in by_member.values()), default=0))
            for m in members if i < len(by_member[m])]


async def _collect_official_profiles(profiles, budget, max_links):
    started = time.monotonic()
    results = []
    connector = aiohttp.TCPConnector(limit=OFFICIAL_INDEX_CONCURRENCY + 8, limit_per_host=3, ttl_dns_cache=60)
    semaphore = asyncio.Semaphore(OFFICIAL_INDEX_CONCURRENCY)
    article_semaphore = asyncio.Semaphore(8)
    tasks = []
    pending_count = 0
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            for profile in profiles:
                for template in profile["index_urls"]:
                    tasks.append(asyncio.create_task(_fetch_official_index(
                        session, profile, _official_index_url(template), semaphore,
                        max_links=max_links, result_sink=results, article_semaphore=article_semaphore)))
            if tasks:
                _, pending = await asyncio.wait(tasks, timeout=budget)
                pending_count = len(pending)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    unique = {}
    for item in results:
        if _is_current_news(item):
            unique.setdefault(item.url.split("#", 1)[0], item)
    result = sorted(unique.values(), key=lambda item: item.published, reverse=True)
    log.info("Official collection sources=%d completed=%d timed_out=%d accepted=%d elapsed=%.2fs",
             len(tasks), len(tasks) - pending_count, pending_count, len(result), time.monotonic() - started)
    return result


async def collect_official_publisher_news():
    global _OFFICIAL_PROFILE_ROUND

    round_index = _OFFICIAL_PROFILE_ROUND
    _OFFICIAL_PROFILE_ROUND += 1
    profiles = _official_collection_profiles(
        per_member=OFFICIAL_SOURCES_PER_MEMBER_PER_CYCLE,
        round_index=round_index,
    )
    coverage = official_source_coverage()
    log.info(
        "Official registry g20_covered=%d g20_total=%d sources=%d polled=%d round=%d",
        sum(bool(n) for n in coverage.values()), len(coverage),
        len(OFFICIAL_SOURCE_REGISTRY), len(profiles), round_index,
    )
    fresh = await _collect_official_profiles(
        profiles, OFFICIAL_COLLECTION_BUDGET, OFFICIAL_INDEX_MAX_LINKS
    )

    # Preserve verified results from earlier institution rotations. This gives
    # the UI broad ministry coverage without launching the full registry at once.
    fresh_by_source = {}
    for item in fresh:
        source_id = getattr(item, "official_source_id", "")
        if source_id:
            fresh_by_source.setdefault(source_id, []).append(item)
    for source_id, new_items in fresh_by_source.items():
        combined = list(new_items) + list(_OFFICIAL_RESULT_CACHE.get(source_id, ()))
        by_url = {}
        for item in combined:
            if not _is_current_news(item):
                continue
            key = item.url.split("#", 1)[0]
            if key and key not in by_url:
                by_url[key] = item
        _OFFICIAL_RESULT_CACHE[source_id] = sorted(
            by_url.values(), key=lambda item: item.published, reverse=True
        )[:OFFICIAL_CACHE_ITEMS_PER_SOURCE]

    merged = {}
    for source_id, cached_items in list(_OFFICIAL_RESULT_CACHE.items()):
        current = [item for item in cached_items if _is_current_news(item)]
        if current:
            _OFFICIAL_RESULT_CACHE[source_id] = current
            for item in current:
                merged.setdefault(item.url.split("#", 1)[0], item)
        else:
            _OFFICIAL_RESULT_CACHE.pop(source_id, None)
    for item in fresh:
        merged.setdefault(item.url.split("#", 1)[0], item)

    result = sorted(merged.values(), key=lambda item: item.published, reverse=True)
    log.info(
        "Official rolling cache sources=%d accepted=%d",
        len(_OFFICIAL_RESULT_CACHE), len(result),
    )
    return result


async def collect_official_institution_news(country_id):
    return await _collect_official_profiles(
        [p for p in _official_collection_profiles(str(country_id or ""))
         if p["institution"] == "foreign_affairs"],
        OFFICIAL_INTERACTIVE_BUDGET - 0.5, OFFICIAL_INTERACTIVE_MAX_LINKS_PER_INDEX)


def _foreign_ministry_country_profile(query):
    nq = normalize_text(query)
    foreign_markers = (
        normalize_text("وزارة الخارجية"), normalize_text("وزارة خارجيه"),
        normalize_text("الخارجية"), "foreign ministry",
        "ministry of foreign affairs", "state department",
        "foreign commonwealth development office", "fcdo",
    )
    if not any(marker in nq for marker in foreign_markers):
        return None

    for country_id, raw in FOREIGN_MINISTRY_REGISTRY.items():
        country_terms = {
            normalize_text(x)
            for x in raw.get("country_aliases", ()) + raw.get("adjectives", ())
            if normalize_text(x)
        }
        institution_aliases = {
            normalize_text(x) for x in raw.get("institution_aliases", ())
            if normalize_text(x)
        }
        if not any(term in nq for term in country_terms) and not any(term in nq for term in institution_aliases):
            continue

        aliases = set(institution_aliases)
        source_aliases = set(institution_aliases)
        for country in country_terms:
            aliases.update({
                normalize_text(f"وزارة الخارجية {country}"),
                normalize_text(f"وزارة خارجيه {country}"),
                normalize_text(f"الخارجية {country}"),
                normalize_text(f"{country} وزارة الخارجية"),
                normalize_text(f"{country} foreign ministry"),
                normalize_text(f"{country} ministry of foreign affairs"),
                normalize_text(f"ministry of foreign affairs {country}"),
            })
            source_aliases.update({
                normalize_text(f"{country} foreign ministry"),
                normalize_text(f"{country} ministry of foreign affairs"),
                normalize_text(f"ministry of foreign affairs {country}"),
            })

        domains = {str(x).lower().strip(".") for x in raw.get("domains", ()) if x}
        publication_paths = tuple(str(x).strip() for x in raw.get("publication_paths", ()) if str(x).strip())
        publication_terms = tuple(str(x).strip() for x in raw.get("publication_terms", ()) if str(x).strip())
        search_queries = []

        # Institution searches are publisher-first.  Google News may return large
        # archives for a bare ministry phrase, so every registry-generated probe
        # is both constrained to the authoritative publisher and pre-filtered to
        # the live-news window.  The strict publication-date gate below remains
        # authoritative; ``when`` only improves discovery quality.
        freshness_probe = f"when:{CURRENT_NEWS_LOOKBACK_DAYS + 1}d"
        for domain in sorted(domains):
            for path in publication_paths[:2]:
                path = "/" + path.strip("/") + "/"
                term = publication_terms[0] if publication_terms else "foreign ministry"
                search_queries.append(f"site:{domain}{path} {term} {freshness_probe}")
            for term in publication_terms[:4]:
                search_queries.append(f"site:{domain} {term} {freshness_probe}")
            if not publication_terms:
                search_queries.extend([
                    f"site:{domain} press release {freshness_probe}",
                    f"site:{domain} statement {freshness_probe}",
                    f"site:{domain} meeting {freshness_probe}",
                ])

        # Keep the user's wording only as a final recall fallback.  It must not
        # displace the authoritative site probes from the bounded query budget.
        search_queries.append(query)

        return {
            "aliases": {x for x in aliases if x},
            "exclude": set(),
            "kind": "institution",
            "domains": domains,
            "source_aliases": {x for x in source_aliases if x},
            "search_queries": list(dict.fromkeys(x for x in search_queries if x)),
            "publication_paths": publication_paths,
            "publication_terms": publication_terms,
            "country_id": country_id,
        }
    return None

# Explicit disambiguation only where one valid geopolitical entity name is a
# strict substring of another. The resolver remains generic for all other
# entities; these profiles prevent false positives that token matching cannot
# safely distinguish.
ENTITY_DISAMBIGUATION = {
    normalize_text("جمهورية الكونغو"): {
        "aliases": [
            "جمهورية الكونغو", "الكونغو",
            "Republic of the Congo", "Congo Republic", "Congo-Brazzaville",
        ],
        "exclude": [
            "جمهورية الكونغو الديمقراطية", "الكونغو الديمقراطية",
            "Democratic Republic of the Congo", "DR Congo", "DRC",
            "Congo-Kinshasa",
        ],
    },
    normalize_text("الكونغو"): {
        "aliases": [
            "جمهورية الكونغو", "الكونغو",
            "Republic of the Congo", "Congo Republic", "Congo-Brazzaville",
        ],
        "exclude": [
            "جمهورية الكونغو الديمقراطية", "الكونغو الديمقراطية",
            "Democratic Republic of the Congo", "DR Congo", "DRC",
            "Congo-Kinshasa",
        ],
    },
    normalize_text("جمهورية الكونغو الديمقراطية"): {
        "aliases": [
            "جمهورية الكونغو الديمقراطية", "الكونغو الديمقراطية",
            "Democratic Republic of the Congo", "DR Congo", "DRC",
            "Congo-Kinshasa",
        ],
        "exclude": ["Republic of the Congo", "Congo-Brazzaville"],
    },
    normalize_text("الكونغو الديمقراطية"): {
        "aliases": [
            "جمهورية الكونغو الديمقراطية", "الكونغو الديمقراطية",
            "Democratic Republic of the Congo", "DR Congo", "DRC",
            "Congo-Kinshasa",
        ],
        "exclude": ["Republic of the Congo", "Congo-Brazzaville"],
    },
}


def _country_aliases_from_query(query):
    nq = normalize_text(query)
    aliases = set()
    for ar_name in _known_country_names():
        if ar_name and ar_name in nq:
            aliases.add(ar_name)

    for ar_name, en_name in COUNTRY_EN.items():
        ar = normalize_text(ar_name)
        en = normalize_text(en_name)
        if (ar and ar in nq) or (en and en in nq):
            aliases.add(ar)
            aliases.add(en)

    if "السعود" in nq or "saudi" in nq:
        aliases.update(normalize_text(x) for x in (
            "السعودية", "المملكة العربية السعودية", "Saudi Arabia", "Saudi"
        ))
    return {x for x in aliases if x}


def _institution_entity_profile(query):
    """Resolve foreign-ministry searches as one country-bound institution."""
    configured = _foreign_ministry_country_profile(query)
    if configured:
        return configured

    nq = normalize_text(query)
    foreign_markers = (
        normalize_text("وزارة الخارجية"), normalize_text("الخارجية"),
        "foreign ministry", "ministry of foreign affairs",
    )
    if not any(marker in nq for marker in foreign_markers):
        return None

    countries = _country_aliases_from_query(query)
    if not countries:
        return None

    aliases = {nq}
    for country in countries:
        aliases.update({
            normalize_text(f"وزارة الخارجية {country}"),
            normalize_text(f"وزارة خارجية {country}"),
            normalize_text(f"الخارجية {country}"),
            normalize_text(f"{country} وزارة الخارجية"),
            normalize_text(f"{country} foreign ministry"),
            normalize_text(f"{country} ministry of foreign affairs"),
            normalize_text(f"ministry of foreign affairs {country}"),
        })

    return {
        "aliases": {x for x in aliases if x},
        "exclude": set(),
        "kind": "institution",
        "domains": set(),
        "search_queries": [],
    }

def _is_broad_official_discovery_query(query):
    """Official discovery queries are topics, not one atomic named entity."""
    nq = normalize_text(query)
    if not nq:
        return False

    markers = (
        "بيانات رسميه", "بيان رسمي", "تصريح رسمي", "بيان صحفي",
        "وزاره خارجيه", "وزاره الخارجيه", "الخارجيه",
        "official statement", "press statement", "press release",
        "foreign ministry", "ministry of foreign affairs",
    )
    if not any(normalize_text(marker) in nq for marker in markers):
        return False

    # Country-specific searches should keep the strict entity anchor.
    return not bool(_country_aliases_from_query(query))


def _query_entity_profile(query):
    """Build one atomic entity profile for search anchoring."""
    nq = normalize_text(query)

    # Longest exact geopolitical phrase wins before shorter substring aliases.
    for key in sorted(ENTITY_DISAMBIGUATION, key=len, reverse=True):
        if nq == key:
            raw = ENTITY_DISAMBIGUATION[key]
            return {
                "aliases": {normalize_text(x) for x in raw["aliases"] if normalize_text(x)},
                "exclude": {normalize_text(x) for x in raw["exclude"] if normalize_text(x)},
                "kind": "geopolitical",
            }

    institution = _institution_entity_profile(query)
    if institution:
        return institution

    aliases = _country_aliases_from_query(query)
    if aliases:
        return {"aliases": aliases, "exclude": set(), "kind": "country"}

    # Broad official discovery is a topic search, not a single entity phrase.
    # Without this exception, a query such as "بيانات رسمية وزارة خارجية"
    # becomes one impossible exact entity anchor and all raw results are
    # discarded before ranking.
    if _is_broad_official_discovery_query(query):
        return {"aliases": set(), "exclude": set(), "kind": "official_discovery"}

    # Generic short entity fallback: preserve the complete phrase atomically.
    if _query_intent(query) == "general":
        q_words = [x for x in nq.split() if x]
        if 1 <= len(q_words) <= 5:
            return {"aliases": {nq}, "exclude": set(), "kind": "generic"}

    return {"aliases": set(), "exclude": set(), "kind": "none"}


def _query_country_terms(query):
    """Backward-compatible alias accessor used by ranking code."""
    return _query_entity_profile(query)["aliases"]


def _entity_haystack(item):
    return normalize_text(
        f"{item.title} {item.original_title} {item.summary} "
        f"{item.source} {item.domain} {item.url}"
    )


def _country_anchor_match(item, query):
    """Require the resolved entity and reject explicitly conflicting entities."""
    profile = _query_entity_profile(query)
    aliases = profile["aliases"]
    if not aliases:
        return True

    haystack = _entity_haystack(item)
    if any(term and term in haystack for term in profile["exclude"]):
        return False

    domains = profile.get("domains", set())
    if domains and (
        _domain_matches(item.domain, domains)
        or _domain_matches(getattr(item, "discovery_domain_hint", ""), domains)
    ):
        return True

    return any(term and term in haystack for term in aliases)

def _institution_source_match(item, profile):
    """For a named institution, require evidence of the original publisher.

    Google News RSS frequently omits the publisher URL from ``source`` even
    when the discovery query itself is an explicit ``site:official-domain``
    query.  In that case the bounded discovery provenance is valid publisher
    evidence; ordinary media results remain rejected.  This is registry-driven
    and therefore applies uniformly to every configured institution.
    """
    if profile.get("kind") != "institution":
        return True
    domains = profile.get("domains", set())
    if domains and (
        _domain_matches(item.domain, domains)
        or _domain_matches(getattr(item, "discovery_domain_hint", ""), domains)
    ):
        return True
    source = normalize_text(item.source)
    source_aliases = profile.get("source_aliases", set())
    return bool(source and any(alias and alias in source for alias in source_aliases))


def _official_publisher_provenance(item):
    """True only when the item is tied to a configured original official publisher."""
    return (
        _domain_matches(item.domain, OFFICIAL_SOURCE_DOMAINS)
        or _domain_matches(getattr(item, "discovery_domain_hint", ""), OFFICIAL_SOURCE_DOMAINS)
    )


def _country_centrality_score(item, query):
    """Measure whether the resolved entity is the subject or a side detail."""
    profile = _query_entity_profile(query)
    terms = profile["aliases"]
    if not terms:
        return 0.0

    title = normalize_text(item.title)
    original = normalize_text(item.original_title)
    summary = normalize_text(item.summary)

    if any(term and term in f"{title} {original} {summary}" for term in profile["exclude"]):
        return -90.0

    best = -40.0
    for term in terms:
        if not term:
            continue
        for value, base in ((title, 34.0), (original, 30.0)):
            pos = value.find(term)
            if pos >= 0:
                length = max(1, len(value))
                relative = pos / length
                positional = 22.0 if relative <= 0.18 else (10.0 if relative <= 0.50 else 0.0)
                best = max(best, base + positional)
        if term in summary:
            best = max(best, 8.0)

    # Institutional searches need stronger centrality because loose ministry /
    # country co-occurrence is especially noisy.
    if profile["kind"] == "institution" and best < 30.0:
        best -= 28.0

    return best



def _current_news_cutoff(now=None):
    """UTC midnight of the oldest calendar day allowed in the live product."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    from datetime import timedelta
    return today - timedelta(days=CURRENT_NEWS_LOOKBACK_DAYS)


def _freshness_state(item, now=None):
    """Return current/stale/future/unknown without doing network I/O."""
    published = item.published
    if published is None:
        return "unknown"
    try:
        if published.tzinfo is None:
            published = published.replace(tzinfo=timezone.utc)
        else:
            published = published.astimezone(timezone.utc)
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        else:
            now = now.astimezone(timezone.utc)
        if published > now:
            return "future"
        return "current" if published >= _current_news_cutoff(now) else "stale"
    except Exception:
        return "unknown"


def _is_current_news(item, now=None):
    """Authoritative live-news gate after any bounded date enrichment."""
    return _freshness_state(item, now) == "current"


def _jsonld_date_published(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if str(key).lower() == "datepublished":
                dt = parse_date(child)
                if dt is not None:
                    return dt
        for child in value.values():
            dt = _jsonld_date_published(child)
            if dt is not None:
                return dt
    elif isinstance(value, list):
        for child in value:
            dt = _jsonld_date_published(child)
            if dt is not None:
                return dt
    return None


def _extract_publication_date_from_html(text):
    """Extract explicit publication time; dateModified alone is never accepted."""
    if not text:
        return None

    # JSON-LD datePublished is the strongest portable signal.
    for raw in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        text, flags=re.I | re.S,
    ):
        try:
            payload = json.loads(html.unescape(raw).strip())
        except Exception:
            continue
        dt = _jsonld_date_published(payload)
        if dt is not None:
            return dt

    # Explicit publication meta fields.  Deliberately exclude modified-time keys.
    meta_patterns = (
        r'<meta[^>]+(?:property|name|itemprop)=["\'](?:article:published_time|datePublished|datepublished|pubdate|publish-date|publish_date|publication_date|date)["\'][^>]+content=["\']([^"\']+)',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name|itemprop)=["\'](?:article:published_time|datePublished|datepublished|pubdate|publish-date|publish_date|publication_date|date)["\']',
    )
    for pattern in meta_patterns:
        for value in re.findall(pattern, text, flags=re.I):
            dt = parse_date(html.unescape(value).strip())
            if dt is not None:
                return dt

    # HTML5 time element, but only when it is explicitly publication-oriented
    # or when there is no modified marker in the element itself.
    for tag in re.findall(r'<time\b[^>]*datetime=["\'][^"\']+["\'][^>]*>', text, flags=re.I):
        if re.search(r'updated|modified', tag, flags=re.I):
            continue
        match = re.search(r'datetime=["\']([^"\']+)', tag, flags=re.I)
        if match:
            dt = parse_date(html.unescape(match.group(1)).strip())
            if dt is not None:
                return dt
    return None


async def _fetch_publication_date(session, item, semaphore):
    """Best-effort original-page publication-date verification."""
    if not item.url:
        return None
    try:
        async with semaphore:
            timeout = aiohttp.ClientTimeout(
                total=DATE_ENRICH_TIMEOUT, connect=min(FETCH_CONNECT_TIMEOUT, DATE_ENRICH_TIMEOUT)
            )
            async with session.get(
                item.url, timeout=timeout, allow_redirects=True,
                headers={"User-Agent": "Global-Intel-Bot/2.0"},
            ) as response:
                if response.status != 200:
                    return None
                final_domain = (response.url.host or "").lower().replace("www.", "")
                # A Google News wrapper is not the publisher page; never use its
                # metadata as evidence that an article is current.
                if final_domain == "news.google.com" or final_domain.endswith(".news.google.com"):
                    return None
                content_type = (response.headers.get("Content-Type") or "").lower()
                if "html" not in content_type and "xhtml" not in content_type:
                    return None
                body = await response.text(errors="ignore")
                return _extract_publication_date_from_html(body[:700000])
    except (asyncio.TimeoutError, aiohttp.ClientError):
        return None
    except Exception:
        return None


async def _enrich_unknown_dates(items, limit=DATE_ENRICH_MAX_CANDIDATES,
                                budget=DATE_ENRICH_BUDGET):
    """Verify dates only for a bounded set of promising unknown-date items."""
    unknown = [item for item in items if _freshness_state(item) == "unknown"][:limit]
    if not unknown:
        return items

    connector = aiohttp.TCPConnector(
        limit=DATE_ENRICH_CONCURRENCY, limit_per_host=2, ttl_dns_cache=60
    )
    semaphore = asyncio.Semaphore(DATE_ENRICH_CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            asyncio.create_task(_fetch_publication_date(session, item, semaphore))
            for item in unknown
        ]
        task_items = dict(zip(tasks, unknown))
        try:
            done, pending = await asyncio.wait(tasks, timeout=budget)
            for task in done:
                result = task.result() if not task.cancelled() else None
                if isinstance(result, datetime):
                    task_items[task].published = result
        finally:
            unfinished = [task for task in tasks if not task.done()]
            for task in unfinished:
                task.cancel()
            if unfinished:
                await asyncio.gather(*unfinished, return_exceptions=True)
    return items


def _displayable_arabic(item):
    """User-facing search results must have an Arabic-suitable headline."""
    return not _needs_arabic_translation(item.title)


def _post_translation_search_filter(items, query):
    """Final invariant gate after translation and before Telegram display."""
    clean = []
    for item in items:
        if _is_digest(item.title) or _is_non_article_result(item) or _hard_low_value(item):
            continue
        if not _is_current_news(item):
            continue
        if not _country_anchor_match(item, query):
            continue
        if not _displayable_arabic(item):
            continue
        clean.append(item)
    return clean

def parse_entry(entry, source, category="general"):
    title = html.unescape(str(entry.get("title", "") or "").strip())
    url = str(entry.get("link", "") or "").strip()

    if not title or not url:
        return None

    summary = html.unescape(
        str(entry.get("summary", "") or entry.get("description", "") or "")
    )
    published = _entry_date(entry)

    publisher = _entry_publisher(entry, source)
    publisher_domain = _entry_source_domain(entry)

    item = NewsItem(
        title=title,
        original_title=title,
        url=url,
        source=publisher,
        summary=summary,
        published=published,
        category=category,
        domain=publisher_domain,
    )
    return classify_item(item)

def _feed_circuit_open(source):
    state = _FEED_FAILURE_STATE.get(source)
    if not state:
        return False
    failures, blocked_until = state
    now = time.monotonic()
    if blocked_until and now < blocked_until:
        return True
    if blocked_until and now >= blocked_until:
        _FEED_FAILURE_STATE.pop(source, None)
    return False


def _record_feed_success(source):
    _FEED_FAILURE_STATE.pop(source, None)


def _record_feed_failure(source):
    failures, blocked_until = _FEED_FAILURE_STATE.get(source, (0, 0.0))
    failures += 1
    if failures >= FEED_CIRCUIT_FAILURES:
        blocked_until = time.monotonic() + FEED_CIRCUIT_COOLDOWN
        failures = 0
        log.warning(
            "Feed circuit opened for %s; cooldown=%ss",
            source, FEED_CIRCUIT_COOLDOWN,
        )
    _FEED_FAILURE_STATE[source] = (failures, blocked_until)


async def fetch_feed(session, source, url):
    """Fetch one source without allowing parsing/network stalls to block Telegram.

    RSS parsing is CPU/synchronous work in feedparser.  Running it directly in the
    event loop previously meant an 8-second asyncio timeout could not fire until a
    malformed/large feed finished parsing.  Parse off-thread, cap feed bytes, and
    apply a short circuit breaker to repeatedly failing sources.
    """
    if _feed_circuit_open(source):
        return []

    try:
        timeout = aiohttp.ClientTimeout(
            total=FETCH_TIMEOUT,
            connect=FETCH_CONNECT_TIMEOUT,
            sock_connect=FETCH_CONNECT_TIMEOUT,
            sock_read=FETCH_TIMEOUT,
        )
        async with session.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "Global-Intel-Bot/2.0"},
        ) as response:
            if response.status != 200:
                log.warning("Feed HTTP %s: %s", response.status, source)
                _record_feed_failure(source)
                return []
            data = await response.content.read(MAX_FEED_BYTES + 1)
            if len(data) > MAX_FEED_BYTES:
                log.warning("Feed too large; skipped: %s", source)
                _record_feed_failure(source)
                return []

        # Do not submit an unbounded number of pure-Python parsers to the
        # interpreter's default executor.  Under simultaneous full collection +
        # breaking polling, 10+ feedparser jobs contend for the GIL and can make
        # even the Home button appear frozen for ~20 seconds.  A tiny dedicated
        # pool isolates parser CPU from Telegram's event loop.
        async with asyncio.timeout(FEED_PARSE_TIMEOUT):
            loop = asyncio.get_running_loop()
            parsed = await loop.run_in_executor(
                _FEED_PARSE_EXECUTOR, feedparser.parse, data
            )
        items = []
        for entry in parsed.entries[:MAX_FEED_ITEMS]:
            item = parse_entry(entry, source)
            if item:
                items.append(item)
        _record_feed_success(source)
        return items

    except asyncio.TimeoutError:
        log.warning("Feed timeout; skipped: %s", source)
        _record_feed_failure(source)
        return []
    except aiohttp.ClientError as exc:
        log.warning("Feed connection error; skipped %s: %s", source, exc)
        _record_feed_failure(source)
        return []
    except Exception as exc:
        log.warning("Feed failed; skipped %s: %s", source, exc)
        _record_feed_failure(source)
        return []

def _ensure_live_google_query(query):
    """Constrain every Google News discovery probe to the live-news window.

    The product has no historical-search path: only today plus the previous
    three calendar days are eligible. Google News otherwise frequently returns
    deep archive matches for broad and site: queries, which are then correctly
    rejected by the strict freshness gate. Applying ``when:4d`` at discovery
    time reduces archive noise without weakening the authoritative publication
    date check.
    """
    text = str(query or "").strip()
    if not text:
        return text
    if re.search(r"(?:^|\s)when:\d+d(?:\s|$)", text, flags=re.I):
        return text
    return f"{text} when:{CURRENT_NEWS_LOOKBACK_DAYS + 1}d"


def google_news_url(query):
    q = urllib.parse.quote_plus(_ensure_live_google_query(query))
    return f"https://news.google.com/rss/search?q={q}&hl=ar&gl=SA&ceid=SA:ar"

async def search_news_online(query, max_results=25):
    """
    Best-effort online discovery with a strict user-facing latency budget.

    Relevance filtering happens before expensive event-level deduplication,
    and only a small bounded set of foreign-language headlines is translated.
    Slow queries/translations are cancelled while already-completed results
    are preserved.
    """
    started = time.monotonic()
    queries = [query]
    normalized = normalize_text(query)

    for key, aliases in QUERY_ALIASES.items():
        if normalize_text(key) in normalized:
            queries.extend(aliases[:3])

    entity_profile = _query_entity_profile(query)
    institution_domains = set()
    institution_queries = []
    direct_official_task = None
    if entity_profile.get("kind") == "institution":
        institution_domains = set(entity_profile.get("domains", set()))
        institution_queries = list(entity_profile.get("search_queries", []))
        country_id = entity_profile.get("country_id")
        if country_id and FOREIGN_MINISTRY_REGISTRY.get(country_id, {}).get("index_urls"):
            # Run the original publisher in parallel with Google discovery.
            # This makes an explicit ministry search independent of cache state
            # and search-engine index dates.
            direct_official_task = asyncio.create_task(
                collect_official_institution_news(country_id)
            )
        # Reserve the bounded online budget for authoritative publisher probes.
        # The previous set-based merge destroyed order and could truncate every
        # site: query before it ran, leaving only broad media searches.
        queries = institution_queries + [query]

    queries = list(dict.fromkeys(queries))[:MAX_ONLINE_QUERIES]

    # google_news_url() applies the same live window to every query, including
    # generic category/background discovery. Institution-generated queries may
    # already contain the constraint; _ensure_live_google_query is idempotent.

    def _institution_query_domain(discovery_query):
        """Return authoritative provenance only for registry-generated site queries."""
        if not institution_domains or discovery_query not in institution_queries:
            return ""
        match = re.search(r"(?:^|\s)site:([^\s]+)", discovery_query, flags=re.I)
        if not match:
            return ""
        raw_site = match.group(1).strip()
        candidate = (urlparse("https://" + raw_site).hostname or raw_site.split("/", 1)[0]).lower().strip(".")
        return candidate if _domain_matches(candidate, institution_domains) else ""

    connector = aiohttp.TCPConnector(
        limit=MAX_ONLINE_QUERIES,
        limit_per_host=2,
        ttl_dns_cache=60,
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        task_queries = {}
        task_domain_hints = {}
        tasks = []
        for q in queries:
            task = asyncio.create_task(
                fetch_feed(session, f"بحث: {q}", google_news_url(q))
            )
            tasks.append(task)
            task_queries[task] = q
            task_domain_hints[task] = _institution_query_domain(q)
        done, pending = await asyncio.wait(tasks, timeout=ONLINE_SEARCH_BUDGET)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    raw_items = []
    for task in done:
        try:
            group = task.result()
            if not isinstance(group, list):
                continue
            discovery_query = task_queries.get(task, "")
            site_domain = task_domain_hints.get(task, "")
            for item in group:
                # Preserve publisher provenance only for a registry-generated
                # site query whose domain belongs to the resolved institution.
                # Broad user/media queries can never manufacture this hint.
                if site_domain:
                    item.discovery_domain_hint = site_domain
                    item.official = True
                    item.trust_score = max(item.trust_score, 98.0)
                raw_items.append(item)
        except Exception:
            continue

    if direct_official_task is not None:
        try:
            direct_items = await asyncio.wait_for(
                asyncio.shield(direct_official_task), timeout=OFFICIAL_INTERACTIVE_BUDGET
            )
            raw_items.extend(direct_items)
        except asyncio.TimeoutError:
            direct_official_task.cancel()
            await asyncio.gather(direct_official_task, return_exceptions=True)
            log.warning("Direct official institution lookup exceeded bounded search budget")
        except Exception as exc:
            log.warning("Direct official institution lookup skipped: %s", exc)

    if not raw_items:
        log.info("Online search timing query=%r raw=0 total=%.3fs", query, time.monotonic() - started)
        return []

    # Cheap relevance gates FIRST.  deduplicate_news performs semantic pairwise
    # comparisons, so running it across every unrelated feed item creates an
    # avoidable O(n^2) delay on the interactive path.
    q_tokens = tokenize(query)
    candidates = []
    profile = _query_entity_profile(query)
    rejected = {"noise": 0, "stale_future": 0, "anchor": 0, "publisher": 0, "relevance": 0}
    for item in raw_items:
        if _is_digest(item.title) or _is_non_article_result(item) or _hard_low_value(item):
            rejected["noise"] += 1
            continue
        freshness = _freshness_state(item)
        if freshness in {"stale", "future"}:
            rejected["stale_future"] += 1
            continue
        if not _country_anchor_match(item, query):
            rejected["anchor"] += 1
            continue
        if profile.get("kind") == "institution" and not _institution_source_match(item, profile):
            rejected["publisher"] += 1
            continue
        title_hits = len(q_tokens & tokenize(item.title))
        original_hits = len(q_tokens & tokenize(item.original_title))
        summary_hits = len(q_tokens & tokenize(item.summary))
        institution_match = profile.get("kind") == "institution"
        official_discovery_match = (
            profile.get("kind") == "official_discovery"
            and _direct_official_statement(item)
        )
        if institution_match or official_discovery_match or title_hits or original_hits or summary_hits >= 2:
            candidates.append(item)
        else:
            rejected["relevance"] += 1

    pre_date_candidates = len(candidates)
    unknown_before_enrich = sum(1 for item in candidates if _freshness_state(item) == "unknown")

    # Missing dates get one bounded chance to prove freshness from the original
    # publisher page. Known stale/future items were already rejected above.
    candidates = await _enrich_unknown_dates(candidates)
    candidates = [item for item in candidates if _is_current_news(item)]

    if not candidates:
        log.info(
            "Online search timing query=%r raw=%d pre_date=%d unknown=%d candidates=0 rejected=%s total=%.3fs",
            query, len(raw_items), pre_date_candidates, unknown_before_enrich, rejected, time.monotonic() - started,
        )
        return []

    ranked = rank_search_results(deduplicate_news(candidates), query)[:max_results]

    # Native-Arabic headlines require no network translation and should never
    # wait behind foreign-language titles. Translate only the strongest bounded
    # foreign subset, then merge and re-rank.
    native_arabic = [item for item in ranked if not _needs_arabic_translation(item.title)]
    foreign = [item for item in ranked if _needs_arabic_translation(item.title)]
    foreign = foreign[:SEARCH_TRANSLATION_RESULT_CAP]
    if foreign:
        foreign = await translate_news_titles(foreign, budget=SEARCH_TRANSLATION_BUDGET)

    final_items = _post_translation_search_filter(native_arabic + foreign, query)
    result = rank_search_results(final_items, query)[:max_results]
    log.info(
        "Online search timing query=%r raw=%d candidates=%d result=%d total=%.3fs",
        query, len(raw_items), len(candidates), len(result), time.monotonic() - started,
    )
    return result

def _source_bucket(item):
    # Diversity is based on the representative publisher, while merged
    # alternate sources remain attached to the event.
    source = normalize_text(item.source)
    domain = (item.domain or "").lower().replace("www.", "")
    return source or domain or "unknown"


def diversify_search_results(items, first_window=5, per_source=1):
    """Prevent one publisher from monopolizing the leading search results.

    This reorders; it does not delete. Deferred stories remain available later.
    """
    if not items:
        return items

    selected = []
    deferred = []
    counts = {}

    for item in items:
        bucket = _source_bucket(item)
        if len(selected) < first_window and counts.get(bucket, 0) >= per_source:
            deferred.append(item)
            continue

        selected.append(item)
        counts[bucket] = counts.get(bucket, 0) + 1

    target = min(first_window, len(items))
    if len(selected) < target:
        need = target - len(selected)
        selected.extend(deferred[:need])
        deferred = deferred[need:]

    return selected + deferred


def rank_search_results(items, query):
    q_tokens = tokenize(query)
    normalized_query = normalize_text(query)
    ranked = []

    for item in items:
        title_tokens = tokenize(item.title)
        original_tokens = tokenize(item.original_title)
        summary_tokens = tokenize(item.summary)

        title_hits = len(q_tokens & title_tokens)
        original_hits = len(q_tokens & original_tokens)
        summary_hits = len(q_tokens & summary_tokens)

        title_text = normalize_text(item.title)
        original_text = normalize_text(item.original_title)
        exact_phrase = (
            normalized_query in title_text
            or normalized_query in original_text
        )

        # Ranking order:
        # 1) actual topical match in article text,
        # 2) official/original publisher,
        # 3) trusted publisher,
        # 4) freshness as a tie-breaker.
        score = (
            title_hits * 16
            + original_hits * 12
            + summary_hits * 2
            + (30 if exact_phrase else 0)
            + (18 if item.official else 0)
            + item.trust_score * 0.08
            + _content_value_adjustment(item, query)
            + _country_centrality_score(item, query)
        )

        # Freshness matters, but it must not overpower relevance/news value.
        if item.published:
            try:
                age_hours = max(0.0, (datetime.now(timezone.utc) - item.published).total_seconds() / 3600)
                score += max(-18.0, 14.0 - min(age_hours, 192.0) * 0.16)
            except Exception:
                pass

        if _country_anchor_match(item, query):
            score += 12
        else:
            score -= 80

        item.relevance_score = score
        published_ts = item.published.timestamp() if item.published else 0
        ranked.append((
            1 if item.official else 0,
            score,
            item.trust_score,
            published_ts,
            item,
        ))

    ranked.sort(key=lambda x: x[:-1], reverse=True)
    ordered = [row[-1] for row in ranked]
    return diversify_search_results(ordered)

async def search_news(items, query, max_results=25):
    """Fast local/cache search for the first progressive result batch.

    The critical optimization is to filter the cache before semantic
    deduplication. This prevents an unrelated large cache from turning a simple
    query into hundreds/thousands of pairwise event comparisons.
    """
    started = time.monotonic()
    q_tokens = tokenize(query)
    candidates = []
    profile = _query_entity_profile(query)

    # Do NOT deduplicate the whole cache here. Relevance gates are much cheaper
    # and normally reduce the working set dramatically.
    for item in items:
        if _is_digest(item.title) or _is_non_article_result(item) or _hard_low_value(item):
            continue
        if not _is_current_news(item):
            continue
        if not _country_anchor_match(item, query):
            continue
        if profile.get("kind") == "institution" and not _institution_source_match(item, profile):
            continue

        title_hits = len(q_tokens & tokenize(item.title))
        original_hits = len(q_tokens & tokenize(item.original_title))
        summary_hits = len(q_tokens & tokenize(item.summary))
        institution_match = profile.get("kind") == "institution"
        if institution_match or title_hits or original_hits or summary_hits >= 2:
            candidates.append(item)

    if not candidates:
        log.info("Local search timing query=%r cache=%d candidates=0 total=%.3fs", query, len(items), time.monotonic() - started)
        return []

    ranked = rank_search_results(deduplicate_news(candidates), query)[:max_results]

    native_arabic = [item for item in ranked if not _needs_arabic_translation(item.title)]
    foreign = [item for item in ranked if _needs_arabic_translation(item.title)]
    foreign = foreign[:SEARCH_TRANSLATION_RESULT_CAP]
    if foreign:
        foreign = await translate_news_titles(foreign, budget=LOCAL_SEARCH_TRANSLATION_BUDGET)

    ranked = _post_translation_search_filter(native_arabic + foreign, query)
    result = rank_search_results(ranked, query)[:max_results]
    log.info(
        "Local search timing query=%r cache=%d candidates=%d result=%d total=%.3fs",
        query, len(items), len(candidates), len(result), time.monotonic() - started,
    )
    return result

async def hybrid_search_news(items, query, max_results=25):
    """
    Compatibility path: local results always survive online timeout/failure.

    The Telegram layer now calls local and online search separately to deliver
    results progressively, but this function stays safe for any older caller.
    """
    local = await search_news(items, query, max_results)

    try:
        online = await asyncio.wait_for(
            search_news_online(query, max_results),
            timeout=ONLINE_SEARCH_BUDGET + 1,
        )
    except Exception:
        online = []

    merged = [
        item for item in deduplicate_news(local + online)
        if not _is_digest(item.title)
    ]

    ranked = rank_search_results(merged, query)
    q_tokens = tokenize(query)
    filtered = []

    for item in ranked:
        if _hard_low_value(item) or not _country_anchor_match(item, query) or not _displayable_arabic(item):
            continue

        title_hits = len(q_tokens & tokenize(item.title))
        original_hits = len(q_tokens & tokenize(item.original_title))
        summary_hits = len(q_tokens & tokenize(item.summary))

        if title_hits or original_hits or summary_hits:
            filtered.append(item)

    return filtered[:max_results]

def _breaking_signal_score(item):
    """Return a lightweight event score for the fast breaking-news lane.

    The goal is recall from direct publisher feeds, not final alert severity.
    Telegram applies its stricter urgent threshold before notifying users.
    """
    title = normalize_text(getattr(item, "title", ""))
    summary = normalize_text(getattr(item, "summary", ""))
    score = 0
    for term in URGENT_TERMS:
        needle = normalize_text(term)
        if not needle:
            continue
        if needle in title:
            score += 3
        elif needle in summary:
            score += 1
    return score


async def collect_breaking_news(max_items=30):
    """Lightweight direct-feed collector for low-latency breaking alerts.

    This intentionally reads only already-configured public publisher RSS feeds.
    It does not run search-engine discovery or direct-page collectors, so it can
    be polled frequently without turning the full news engine into a hot loop.
    """
    feeds = {**TRUSTED_FEEDS, **ADDITIONAL_TRUSTED_FEEDS}
    connector = aiohttp.TCPConnector(
        limit=BREAKING_FEED_CONCURRENCY,
        limit_per_host=2,
        ttl_dns_cache=60,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            asyncio.create_task(fetch_feed(session, source, url))
            for source, url in feeds.items()
        ]
        groups = await asyncio.gather(*tasks, return_exceptions=True)

    candidates = []
    for group in groups:
        if not isinstance(group, list):
            continue
        for item in group[:BREAKING_MAX_PER_FEED]:
            if _is_digest(item.title) or _is_non_article_result(item) or _hard_low_value(item):
                continue
            if not _is_current_news(item):
                continue
            if _breaking_signal_score(item) <= 0:
                continue
            candidates.append(item)

    # Deduplicate before translation to keep the hot path small, then translate
    # only the few signal-bearing titles needed for Telegram's Arabic alert UI.
    candidates = deduplicate_news(candidates)
    candidates.sort(
        key=lambda item: (
            _breaking_signal_score(item),
            float(getattr(item, "trust_score", 0) or 0),
            item.published.timestamp() if item.published else 0,
        ),
        reverse=True,
    )
    candidates = candidates[:max_items]
    candidates = await translate_news_titles(
        candidates, budget=BREAKING_TRANSLATION_BUDGET
    )

    log.info(
        "Breaking lane feeds=%d candidates=%d returned=%d",
        len(feeds), len(candidates), min(len(candidates), max_items),
    )
    return candidates[:max_items]


async def _collect_general_news(max_items=150):
    feeds = {**TRUSTED_FEEDS, **ADDITIONAL_TRUSTED_FEEDS}

    connector = aiohttp.TCPConnector(
        limit=COLLECTION_CONCURRENCY,
        limit_per_host=2,
        ttl_dns_cache=60,
    )
    async with aiohttp.ClientSession(connector=connector) as session:
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
        "بيان رسمي وزارة الخارجية",
        "foreign ministry official statement",
        "press release foreign ministry",
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

        # China / Russia: major television and national newsrooms.
        # Site discovery is used instead of guessing unstable RSS endpoints.
        "site:cgtn.com China world breaking news",
        "site:cctv.com China world news",
        "site:rt.com Russia world breaking news",
        "site:tass.com Russia world news",

        # Official-source discovery is kept separate from general media discovery.
        "site:mofa.gov.sa/ar/ministry/statements بيان وزارة الخارجية السعودية",
        "site:state.gov/releases press release State Department",
        "site:mfa.gov.cn/eng/xw/fyrbt Foreign Ministry press conference",
        "site:mofa.go.jp/press/release foreign ministry press release",
    ]

    try:
        # Background discovery is intentionally much leaner than interactive
        # search.  The old path called search_news_online() once per discovery
        # query; each call could expand aliases, create its own session, enrich
        # dates, deduplicate and translate.  With many global probes that
        # multiplied network requests and CPU work before one fresh item could
        # reach the cache.
        #
        # The collector now performs one public Google News RSS request per
        # discovery query through a shared bounded session.  Expensive date
        # verification, translation and event deduplication happen once below
        # across the merged candidate set.  Fast sources therefore surface
        # immediately while slow probes are cancelled at the common budget.
        discovery_connector = aiohttp.TCPConnector(
            limit=DISCOVERY_CONCURRENCY,
            limit_per_host=DISCOVERY_CONCURRENCY,
            ttl_dns_cache=60,
        )
        async with aiohttp.ClientSession(connector=discovery_connector) as discovery_session:
            discovery_tasks = [
                asyncio.create_task(
                    fetch_feed(discovery_session, f"بحث: {q}", google_news_url(q))
                )
                for q in discovery_queries
            ]
            try:
                done, pending = await asyncio.wait(discovery_tasks, timeout=DISCOVERY_BUDGET)
            finally:
                unfinished = [task for task in discovery_tasks if not task.done()]
                for task in unfinished:
                    task.cancel()
                if unfinished:
                    await asyncio.gather(*unfinished, return_exceptions=True)

        discovery_added = 0
        for task in done:
            try:
                group = task.result()
                if not isinstance(group, list):
                    continue
                for item in group:
                    # Keep only article-like candidates here.  Freshness is
                    # enforced authoritatively after the shared date-enrichment
                    # pass below, so unknown-date originals still get one chance
                    # to prove they are current.
                    if _is_digest(item.title) or _is_non_article_result(item) or _hard_low_value(item):
                        continue
                    if _freshness_state(item) in {"stale", "future"}:
                        continue
                    items.append(item)
                    discovery_added += 1
            except Exception:
                continue

        log.info(
            "Background discovery queries=%d completed=%d pending=%d added=%d budget=%.1fs",
            len(discovery_queries), len(done), len(pending), discovery_added, DISCOVERY_BUDGET,
        )

    except Exception:
        log.exception("Discovery failed; returning available direct-feed news.")

    # Bound work before verification, translation and semantic dedup. Discovery
    # can otherwise delay completed official results past the cache timeout.
    by_url = {}
    for item in items:
        key = item.url.split("#", 1)[0].strip().lower()
        if not key:
            continue
        existing = by_url.get(key)
        if existing is None or item.trust_score > existing.trust_score:
            by_url[key] = item
    items = list(by_url.values())
    items.sort(key=lambda item: (
        1 if _freshness_state(item) == "current" else 0,
        float(item.trust_score or 0),
        item.published.timestamp() if item.published else 0,
    ), reverse=True)
    items = items[:GENERAL_CANDIDATE_CAP]

    # Unknown-date candidates are verified once, in a bounded concurrent pass,
    # before the authoritative current-news gate.  This keeps strict freshness
    # without silently discarding otherwise valid current publisher pages.
    items = await _enrich_unknown_dates(items)

    # Canonicalize every foreign title before the final event-level dedup.
    # This prevents the same story arriving in Arabic and English from being
    # emitted twice merely because the source language differs.
    items = [
        item for item in deduplicate_news(items)
        if not _is_digest(item.title) and _is_current_news(item)
    ]

    items.sort(
        key=lambda x: (
            1 if x.official else 0,
            float(x.relevance_score or 0),
            float(x.trust_score or 0),
            x.published.timestamp() if x.published else 0,
        ),
        reverse=True,
    )
    items = items[:max_items]
    items = await translate_news_titles(items, budget=GENERAL_TRANSLATION_BUDGET)

    return items

async def collect_news(max_items=150):
    """Independent collectors share a deadline below the worker's 25s timeout."""
    tasks = [asyncio.create_task(_collect_general_news(max_items), name="general-news"),
             asyncio.create_task(collect_official_publisher_news(), name="official-news")]
    items = []
    try:
        done, _ = await asyncio.wait(tasks, timeout=20.0)
        for task in tasks:
            if task in done:
                try:
                    items.extend(task.result())
                except Exception:
                    log.exception("News collector failed")
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    official = [item for item in items if _direct_official_statement(item)]
    if official:
        try:
            await translate_news_titles(official, budget=2.0)
        except asyncio.TimeoutError:
            log.info("Official translation budget reached; original titles retained")
    for item in official:
        # Neutral date priority for the existing bot's score-based topic filter.
        age = (datetime.now(timezone.utc).date() - item.published.date()).days
        item.relevance_score = (4 - age) * 1000 + _official_importance(item)
    ordered = sorted(official, key=lambda x: (x.published.date(), _official_importance(x), x.published), reverse=True)
    remaining = [item for item in items if not getattr(item, "official_source_id", "")]
    # Keep registry provenance when a general feed also carries the same URL.
    seen, result = set(), []
    for item in ordered + remaining:
        key = item.url.split("#", 1)[0]
        if key not in seen:
            seen.add(key)
            result.append(item)
    log.info("News collection completed official=%d general=%d returned=%d",
             len(ordered), len(remaining), min(len(result), max_items))
    return result[:max_items]


def _official_importance(item):
    text = normalize_text((item.original_title or item.title) + " " + item.summary)
    groups = (
        ("ceasefire", "وقف إطلاق النار", "sanctions", "عقوبات", "emergency", "طوارئ"),
        ("interest rate", "سعر الفائدة", "monetary policy", "سياسة نقدية", "قرار"),
        ("joint statement", "بيان مشترك", "agreement", "اتفاق", "security", "أمن"),
    )
    return sum(20 for group in groups if any(normalize_text(word) in text for word in group))


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
