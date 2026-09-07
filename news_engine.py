import asyncio
import html
import json
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

FETCH_TIMEOUT = 4
FETCH_CONNECT_TIMEOUT = 2
COLLECTION_CONCURRENCY = 20
MAX_FEED_ITEMS = 30
MAX_ONLINE_QUERIES = 6
ONLINE_SEARCH_BUDGET = 3.5
SEARCH_TRANSLATION_BUDGET = 1.0
LOCAL_SEARCH_TRANSLATION_BUDGET = 0.6
SEARCH_TRANSLATION_RESULT_CAP = 10
DISCOVERY_BUDGET = 6
ROTATION_WINDOW_SECONDS = 300
DATE_ENRICH_TIMEOUT = 2.5
DATE_ENRICH_CONCURRENCY = 6
DATE_ENRICH_MAX_CANDIDATES = 12

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
        "official statement", "press statement", "press release",
        "readout", "remarks by", "briefing by", "spokesperson",
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
        "met", "meets", "received", "receives", "discussed", "discusses",
        "held talks", "spoke with",
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

    Government/foreign-ministry publishers may qualify on an official action.
    Broad institutional newsrooms (for example UN News) require an explicit
    statement/release marker so ordinary reporting cannot occupy this section.
    """
    if not _official_signal(item.title, item.summary, item):
        return False
    if _domain_matches(item.domain, OFFICIAL_SOURCE_DOMAINS):
        return True

    title = normalize_text(item.title)
    document_markers = (
        "بيان رسمي", "تصريح رسمي", "بيان صحفي", "مؤتمر صحفي",
        "official statement", "press statement", "press release",
        "readout", "remarks by", "briefing by",
    )
    return item.official and any(normalize_text(x) in title for x in document_markers)


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
        return _direct_official_statement(item)

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
    },
    "united_states": {
        "country_aliases": ("الولايات المتحدة", "الولايات المتحده", "أمريكا", "امريكا", "United States", "USA", "U.S.", "US"),
        "adjectives": ("الأمريكية", "الامريكيه", "الأميركية", "الاميركيه", "American", "U.S.", "US"),
        "domains": ("state.gov",),
        "institution_aliases": ("U.S. State Department", "US State Department", "United States Department of State", "Department of State", "State Department"),
    },
    "united_kingdom": {
        "country_aliases": ("بريطانيا", "المملكة المتحدة", "المملكه المتحده", "United Kingdom", "UK", "Britain"),
        "adjectives": ("البريطانية", "البريطانيه", "British", "UK"),
        "domains": ("gov.uk",),
        "institution_aliases": ("Foreign, Commonwealth & Development Office", "FCDO"),
    },
    "france": {
        "country_aliases": ("فرنسا", "France"),
        "adjectives": ("الفرنسية", "الفرنسيه", "French"),
        "domains": ("diplomatie.gouv.fr",),
    },
    "china": {
        "country_aliases": ("الصين", "China"),
        "adjectives": ("الصينية", "الصينيه", "Chinese"),
        "domains": ("mfa.gov.cn",),
    },
    "russia": {
        "country_aliases": ("روسيا", "Russia"),
        "adjectives": ("الروسية", "الروسيه", "Russian"),
        "domains": ("mid.ru",),
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
        search_queries = []
        # Keep the user's wording and add stable English/official-domain discovery.
        search_queries.append(query)
        english_country = next(
            (x for x in raw.get("country_aliases", ()) if re.search(r"[A-Za-z]", str(x))),
            "",
        )
        if english_country:
            search_queries.extend([
                f"{english_country} foreign ministry",
                f"{english_country} ministry of foreign affairs",
            ])
        search_queries.extend(f"site:{domain}" for domain in domains)

        return {
            "aliases": {x for x in aliases if x},
            "exclude": set(),
            "kind": "institution",
            "domains": domains,
            "source_aliases": {x for x in source_aliases if x},
            "search_queries": list(dict.fromkeys(x for x in search_queries if x)),
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


async def _enrich_unknown_dates(items, limit=DATE_ENRICH_MAX_CANDIDATES):
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
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for item, result in zip(unknown, results):
        if isinstance(result, datetime):
            item.published = result
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

async def fetch_feed(session, source, url):
    """Fetch one source in isolation; a failed source never blocks collection."""
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
                return []
            data = await response.read()

        parsed = feedparser.parse(data)
        items = []
        for entry in parsed.entries[:MAX_FEED_ITEMS]:
            item = parse_entry(entry, source)
            if item:
                items.append(item)
        return items

    except asyncio.TimeoutError:
        log.warning("Feed timeout; skipped: %s", source)
        return []
    except aiohttp.ClientError as exc:
        log.warning("Feed connection error; skipped %s: %s", source, exc)
        return []
    except Exception as exc:
        log.warning("Feed failed; skipped %s: %s", source, exc)
        return []

def google_news_url(query):
    q = urllib.parse.quote_plus(query)
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
    if entity_profile.get("kind") == "institution":
        queries.extend(entity_profile.get("search_queries", []))

    queries = list(dict.fromkeys(queries))[:MAX_ONLINE_QUERIES]

    connector = aiohttp.TCPConnector(
        limit=MAX_ONLINE_QUERIES,
        limit_per_host=2,
        ttl_dns_cache=60,
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        task_queries = {}
        tasks = []
        for q in queries:
            task = asyncio.create_task(
                fetch_feed(session, f"بحث: {q}", google_news_url(q))
            )
            tasks.append(task)
            task_queries[task] = q
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
            site_match = re.match(r"^\s*site:([^\s]+)\s*$", discovery_query, flags=re.I)
            site_domain = site_match.group(1).lower().strip(".") if site_match else ""
            for item in group:
                # Preserve exact discovery provenance only for pure site:domain
                # queries.  It is never inferred from a broad query or title.
                if site_domain:
                    item.discovery_domain_hint = site_domain
                raw_items.append(item)
        except Exception:
            continue

    if not raw_items:
        log.info("Online search timing query=%r raw=0 total=%.3fs", query, time.monotonic() - started)
        return []

    # Cheap relevance gates FIRST.  deduplicate_news performs semantic pairwise
    # comparisons, so running it across every unrelated feed item creates an
    # avoidable O(n^2) delay on the interactive path.
    q_tokens = tokenize(query)
    candidates = []
    profile = _query_entity_profile(query)
    for item in raw_items:
        if _is_digest(item.title) or _is_non_article_result(item) or _hard_low_value(item):
            continue
        freshness = _freshness_state(item)
        if freshness in {"stale", "future"}:
            continue
        if not _country_anchor_match(item, query):
            continue
        if profile.get("kind") == "institution" and not _institution_source_match(item, profile):
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

    # Missing dates get one bounded chance to prove freshness from the original
    # publisher page. Known stale/future items were already rejected above.
    candidates = await _enrich_unknown_dates(candidates)
    candidates = [item for item in candidates if _is_current_news(item)]

    if not candidates:
        log.info("Online search timing query=%r raw=%d candidates=0 total=%.3fs", query, len(raw_items), time.monotonic() - started)
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

async def collect_news(max_items=150):
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
        "site:mofa.gov.sa وزارة الخارجية السعودية",
        "site:state.gov foreign policy statement",
        "site:mfa.gov.cn foreign ministry statement",
        "site:mofa.go.jp foreign ministry statement",
    ]

    try:
        discovery_tasks = [
            asyncio.create_task(search_news_online(q, 15))
            for q in discovery_queries
        ]
        done, pending = await asyncio.wait(
            discovery_tasks,
            timeout=DISCOVERY_BUDGET,
        )

        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        for task in done:
            try:
                group = task.result()
                if isinstance(group, list):
                    items.extend(group)
            except Exception:
                continue

    except Exception:
        log.exception("Discovery failed; returning available direct-feed news.")

    # Unknown-date candidates are verified once, in a bounded concurrent pass,
    # before the authoritative current-news gate.  This keeps strict freshness
    # without silently discarding otherwise valid current publisher pages.
    items = await _enrich_unknown_dates(items)

    # Canonicalize every foreign title before the final event-level dedup.
    # This prevents the same story arriving in Arabic and English from being
    # emitted twice merely because the source language differs.
    items = await translate_news_titles(items)
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
