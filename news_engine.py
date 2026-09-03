# ============================================================
# news_engine.py
# Global News Intelligence Engine
# ============================================================

import asyncio
import html
import logging
import re
import urllib.parse
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import aiohttp
import feedparser


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("news_engine")


# ============================================================
# SETTINGS
# ============================================================

FETCH_TIMEOUT = 10
MAX_FEED_ITEMS = 20
MAX_ONLINE_QUERIES = 4


# ============================================================
# TRUSTED DIRECT RSS SOURCES
# ============================================================

TRUSTED_FEEDS: Dict[str, str] = {

    # --------------------------------------------------------
    # Middle East / Arabic
    # --------------------------------------------------------

    "العربية": (
        "https://www.alarabiya.net/.well-known/rss/urgent.xml"
    ),

    "الجزيرة": (
        "https://www.aljazeera.net/aljazeerarss/"
        "a7c1866f-6829-4883-8441-358d731800bc/"
        "43316f44-8e12-4320-b4c2-a22f6654b321"
    ),

    "سكاي نيوز عربية": (
        "https://www.skynewsarabia.com/rss/v1/news.xml"
    ),

    "الشرق": (
        "https://asharq.com/rss/"
    ),

    "CNBC عربية": (
        "https://www.cnbcarabia.com/rss.xml"
    ),

    "الشرق اقتصاد": (
        "https://economy.asharq.com/rss/"
    ),

    "Investing": (
        "https://sa.investing.com/rss/news.rss"
    ),

    "وكالة الأنباء السعودية": (
        "https://www.spa.gov.sa/rss.xml"
    ),

    # --------------------------------------------------------
    # International
    # --------------------------------------------------------

    "BBC عربي": (
        "https://feeds.bbci.co.uk/arabic/rss.xml"
    ),

    "DW عربي": (
        "https://rss.dw.com/rdf/rss-ar-all"
    ),

    "France24 عربي": (
        "https://www.france24.com/ar/rss"
    ),

    # --------------------------------------------------------
    # Energy
    # --------------------------------------------------------

    "EIA": (
        "https://www.eia.gov/rss/todayinenergy.xml"
    ),
}


# ============================================================
# GOOGLE NEWS
# ============================================================

GOOGLE_NEWS_BASE = (
    "https://news.google.com/rss/search"
    "?q={query}&hl=ar&gl=SA&ceid=SA:ar"
)


# ============================================================
# GLOBAL REGIONS
# ============================================================

REGIONS: Dict[str, List[str]] = {

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
        "منغوليا",
    ],

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

    "أفريقيا": [
        "مصر",
        "الجزائر",
        "المغرب",
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
        "زيمبابوي",
        "زامبيا",
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
        "رومانيا",
        "التشيك",
        "المجر",
        "أيرلندا",
    ],

    "أستراليا والمحيط الهادئ": [
        "أستراليا",
        "نيوزيلندا",
        "فيجي",
        "بابوا غينيا الجديدة",
        "جزر سليمان",
        "ساموا",
    ],

    "أمريكا الشمالية": [
        "الولايات المتحدة",
        "أمريكا",
        "كندا",
        "المكسيك",
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


# ============================================================
# GLOBAL SEARCH QUERIES
# ============================================================

GLOBAL_QUERIES: List[Tuple[str, str]] = [

    # Official / governments
    (
        '"وزارة" OR "حكومة" OR "رئاسة الوزراء" '
        'OR "وزارة الخارجية" OR "وزارة الدفاع" '
        'OR "بيان رسمي"',
        "official"
    ),

    # Economy + energy = ONE category
    (
        '"اقتصاد" OR "أسواق" OR "أسهم" OR "بورصة" '
        'OR "نفط" OR "أوبك" OR "برنت" OR "غاز" '
        'OR "ذهب" OR "تضخم" OR "فائدة" '
        'OR "بنك مركزي" OR "بيتكوين" OR "عملات رقمية"',
        "economy_energy"
    ),

    # Security
    (
        '"دفاع" OR "أمن" OR "عسكري" OR "جيش" '
        'OR "مناورات" OR "أمن قومي"',
        "security"
    ),

    # Breaking
    (
        '"عاجل" OR "طارئ" OR "بيان عاجل" '
        'OR "تطورات عاجلة"',
        "urgent"
    ),
]


# ============================================================
# NEWS ITEM
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
    ):
        self.title = self.clean_text(title)
        self.source = self.clean_text(source)
        self.url = url.strip() if url else ""
        self.published_at = self.clean_text(published_at)
        self.category = category
        self.summary = self.clean_text(summary)
        self.region = region

        self.search_text = " ".join(
            part for part in [
                self.title,
                self.summary,
                self.source,
                self.region,
                self.category,
            ]
            if part
        ).lower()

    @staticmethod
    def clean_text(text: str) -> str:

        if not text:
            return ""

        text = html.unescape(str(text))

        text = re.sub(
            r"<script.*?</script>",
            " ",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )

        text = re.sub(
            r"<style.*?</style>",
            " ",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )

        text = re.sub(
            r"<[^>]+>",
            " ",
            text,
        )

        text = re.sub(
            r"\s+",
            " ",
            text,
        )

        return text.strip()

    def __repr__(self):

        return (
            f"<NewsItem "
            f"title='{self.title[:50]}...' "
            f"source='{self.source}'>"
        )


# ============================================================
# REGION DETECTION
# ============================================================

def detect_region(text: str) -> str:

    if not text:
        return ""

    lowered = text.lower()

    for region, countries in REGIONS.items():

        for country in countries:

            if country.lower() in lowered:
                return region

    return ""


# ============================================================
# URL VALIDATION
# ============================================================

def is_valid_http_url(url: str) -> bool:

    if not url:
        return False

    url = url.strip()

    if not re.match(
        r"^https?://",
        url,
        flags=re.IGNORECASE,
    ):
        return False

    return True


# ============================================================
# FETCH RSS
# ============================================================

async def fetch_rss_feed(
    session: aiohttp.ClientSession,
    source_name: str,
    url: str,
) -> List[NewsItem]:

    items: List[NewsItem] = []

    try:

        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(
                total=FETCH_TIMEOUT
            ),
            allow_redirects=True,
        ) as response:

            if response.status != 200:

                log.warning(
                    "RSS [%s] returned HTTP %s",
                    source_name,
                    response.status,
                )

                return items

            content = await response.text(
                errors="ignore"
            )

            parsed = feedparser.parse(content)

            for entry in parsed.entries[
                :MAX_FEED_ITEMS
            ]:

                title = (
                    entry.get("title", "")
                    or ""
                )

                link = (
                    entry.get("link", "")
                    or ""
                )

                published = (
                    entry.get("published", "")
                    or entry.get("updated", "")
                    or ""
                )

                summary = (
                    entry.get("summary", "")
                    or entry.get("description", "")
                    or ""
                )

                if not title:
                    continue

                region = detect_region(
                    f"{title} {summary}"
                )

                items.append(
                    NewsItem(
                        title=title,
                        source=source_name,
                        url=link,
                        published_at=published,
                        category="direct",
                        summary=summary,
                        region=region,
                    )
                )

    except asyncio.CancelledError:

        raise

    except Exception as exc:

        log.warning(
            "Error fetching RSS [%s]: %s",
            source_name,
            exc,
        )

    return items


# ============================================================
# GOOGLE NEWS RSS
# ============================================================

async def fetch_google_news_topic(
    session: aiohttp.ClientSession,
    query: str,
    category: str = "general",
    region: str = "",
) -> List[NewsItem]:

    items: List[NewsItem] = []

    encoded_query = urllib.parse.quote_plus(
        query
    )

    target_url = GOOGLE_NEWS_BASE.format(
        query=encoded_query
    )

    try:

        async with session.get(
            target_url,
            timeout=aiohttp.ClientTimeout(
                total=FETCH_TIMEOUT
            ),
            allow_redirects=True,
        ) as response:

            if response.status != 200:

                log.warning(
                    "Google News [%s] returned HTTP %s",
                    query[:80],
                    response.status,
                )

                return items

            content = await response.text(
                errors="ignore"
            )

            parsed = feedparser.parse(content)

            for entry in parsed.entries[
                :MAX_FEED_ITEMS
            ]:

                title = (
                    entry.get("title", "")
                    or ""
                )

                link = (
                    entry.get("link", "")
                    or ""
                )

                published = (
                    entry.get("published", "")
                    or entry.get("updated", "")
                    or ""
                )

                summary = (
                    entry.get("summary", "")
                    or entry.get("description", "")
                    or ""
                )

                source = "تغطية إخبارية"

                source_data = entry.get(
                    "source"
                )

                if isinstance(
                    source_data,
                    dict,
                ):
                    source = (
                        source_data.get(
                            "title",
                            source,
                        )
                        or source
                    )

                # Google News frequently appends:
                # " - Source Name"
                if " - " in title:

                    title = title.rsplit(
                        " - ",
                        1,
                    )[0].strip()

                detected_region = (
                    region
                    or detect_region(
                        f"{title} {summary}"
                    )
                )

                if title:

                    items.append(
                        NewsItem(
                            title=title,
                            source=source,
                            url=link,
                            published_at=published,
                            category=category,
                            summary=summary,
                            region=detected_region,
                        )
                    )

    except asyncio.CancelledError:

        raise

    except Exception as exc:

        log.warning(
            "Error fetching Google News [%s]: %s",
            query[:80],
            exc,
        )

    return items


# ============================================================
# DEDUPLICATION
# ============================================================

def normalize_title(title: str) -> str:

    title = (
        title
        or ""
    ).lower()

    title = re.sub(
        r"[^\w\u0600-\u06ff\s]",
        " ",
        title,
    )

    title = re.sub(
        r"\s+",
        " ",
        title,
    ).strip()

    # Use enough of the title to avoid
    # duplicate articles while not merging
    # unrelated stories.
    return title[:120]


def deduplicate_news(
    items: List[NewsItem],
) -> List[NewsItem]:

    unique: List[NewsItem] = []
    seen = set()

    for item in items:

        key = normalize_title(
            item.title
        )

        if not key:
            continue

        if key in seen:
            continue

        seen.add(key)
        unique.append(item)

    return unique


# ============================================================
# GLOBAL COLLECTION
# ============================================================

async def collect_news(
    max_items: int = 100,
) -> List[NewsItem]:

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(compatible; GlobalIntelBot/1.0)"
        ),
        "Accept": (
            "application/rss+xml, "
            "application/xml, "
            "text/xml, "
            "text/html;q=0.9, "
            "*/*;q=0.8"
        ),
    }

    all_news: List[NewsItem] = []

    async with aiohttp.ClientSession(
        headers=headers
    ) as session:

        tasks = []

        # ----------------------------------------------------
        # Direct trusted feeds
        # ----------------------------------------------------

        for source_name, url in TRUSTED_FEEDS.items():

            tasks.append(
                fetch_rss_feed(
                    session,
                    source_name,
                    url,
                )
            )

        # ----------------------------------------------------
        # Global topic queries
        # ----------------------------------------------------

        for query, category in GLOBAL_QUERIES:

            tasks.append(
                fetch_google_news_topic(
                    session,
                    query,
                    category,
                )
            )

        # ----------------------------------------------------
        # Regional queries
        # ----------------------------------------------------

        for region, countries in REGIONS.items():

            # Use a representative set of countries
            # to maintain global coverage without
            # creating an excessive number of requests.
            selected = countries[:8]

            if not selected:
                continue

            region_query = " OR ".join(
                f'"{country}"'
                for country in selected
            )

            tasks.append(
                fetch_google_news_topic(
                    session,
                    region_query,
                    "regional",
                    region,
                )
            )

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        for result in results:

            if isinstance(
                result,
                list,
            ):

                all_news.extend(
                    result
                )

    all_news = deduplicate_news(
        all_news
    )

    log.info(
        "Successfully collected %s unique news items.",
        len(all_news),
    )

    return all_news[:max_items]


# ============================================================
# SEARCH TOKENIZATION
# ============================================================

ARABIC_STOPWORDS = {
    "في",
    "من",
    "على",
    "إلى",
    "عن",
    "مع",
    "هذا",
    "هذه",
    "ذلك",
    "تلك",
    "هو",
    "هي",
    "و",
    "أو",
    "ثم",
    "بعد",
    "قبل",
    "ما",
    "ماذا",
    "هل",
    "أن",
    "إن",
    "كان",
    "كانت",
    "لـ",
    "له",
    "لها",
    "التي",
    "الذي",
    "هناك",
}


def normalize_search_text(
    text: str,
) -> str:

    text = (
        text
        or ""
    ).lower()

    # Remove punctuation while keeping
    # Arabic characters.
    text = re.sub(
        r"[^\w\u0600-\u06ff\s]",
        " ",
        text,
    )

    # Normalize common Arabic variants.
    replacements = {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ى": "ي",
        "ة": "ه",
    }

    for old, new in replacements.items():

        text = text.replace(
            old,
            new,
        )

    text = re.sub(
        r"\s+",
        " ",
        text,
    ).strip()

    return text


def tokenize_query(
    query: str,
) -> List[str]:

    normalized = normalize_search_text(
        query
    )

    tokens = []

    for token in normalized.split():

        if len(token) < 2:
            continue

        if token in ARABIC_STOPWORDS:
            continue

        tokens.append(token)

    return list(
        dict.fromkeys(tokens)
    )


# ============================================================
# SEARCH SCORING
# ============================================================

def score_news_item(
    item: NewsItem,
    query_tokens: List[str],
) -> int:

    if not query_tokens:
        return 0

    title = normalize_search_text(
        item.title
    )

    summary = normalize_search_text(
        item.summary
    )

    source = normalize_search_text(
        item.source
    )

    region = normalize_search_text(
        item.region
    )

    score = 0

    for token in query_tokens:

        # Strongest match: title
        if token in title:

            score += 10

        # Useful match: summary
        if token in summary:

            score += 4

        # Source / region
        if token in source:

            score += 2

        if token in region:

            score += 2

    # Bonus when ALL tokens appear somewhere
    searchable = (
        f"{title} "
        f"{summary} "
        f"{source} "
        f"{region}"
    )

    if all(
        token in searchable
        for token in query_tokens
    ):

        score += 8

    return score


# ============================================================
# LOCAL SEARCH
# ============================================================

def search_news(
    items: List[NewsItem],
    keywords: List[str],
    max_results: int = 25,
) -> List[NewsItem]:

    if not items:
        return []

    if not keywords:
        return []

    # This function supports both:
    #
    # search_news(items, ["السعودية"])
    #
    # and:
    #
    # search_news(items, ["السعودية", "النفط"])
    #
    query_text = " ".join(
        str(k)
        for k in keywords
        if k
    )

    query_tokens = tokenize_query(
        query_text
    )

    if not query_tokens:
        return []

    scored = []

    for index, item in enumerate(items):

        score = score_news_item(
            item,
            query_tokens,
        )

        if score > 0:

            scored.append(
                (
                    score,
                    index,
                    item,
                )
            )

    scored.sort(
        key=lambda x: (
            -x[0],
            x[1],
        )
    )

    return [
        item
        for _, _, item in scored[
            :max_results
        ]
    ]


# ============================================================
# ONLINE SEARCH
# ============================================================

async def search_news_online(
    query: str,
    max_results: int = 25,
) -> List[NewsItem]:

    query = (
        query
        or ""
    ).strip()

    if not query:
        return []

    tokens = tokenize_query(
        query
    )

    if not tokens:
        return []

    queries: List[str] = []

    # --------------------------------------------------------
    # 1. Original phrase
    # --------------------------------------------------------

    queries.append(
        query
    )

    # --------------------------------------------------------
    # 2. Quoted phrase
    # --------------------------------------------------------

    if len(tokens) > 1:

        queries.append(
            f'"{query}"'
        )

    # --------------------------------------------------------
    # 3. Individual meaningful words
    # --------------------------------------------------------

    if len(tokens) > 1:

        queries.append(
            " OR ".join(
                f'"{token}"'
                for token in tokens
            )
        )

    # --------------------------------------------------------
    # 4. Limited two-token combinations
    # --------------------------------------------------------

    if len(tokens) >= 2:

        queries.append(
            f'"{tokens[0]}" "{tokens[1]}"'
        )

    queries = list(
        dict.fromkeys(
            queries[
                :MAX_ONLINE_QUERIES
            ]
        )
    )

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(compatible; GlobalIntelBot/1.0)"
        )
    }

    collected: List[NewsItem] = []

    async with aiohttp.ClientSession(
        headers=headers
    ) as session:

        tasks = [
            fetch_google_news_topic(
                session,
                search_query,
                "search",
            )
            for search_query in queries
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        for result in results:

            if isinstance(
                result,
                list,
            ):

                collected.extend(
                    result
                )

    collected = deduplicate_news(
        collected
    )

    # IMPORTANT:
    #
    # Do not return unrelated "latest" news.
    # Every returned item must actually match
    # the requested search terms.
    ranked = search_news(
        collected,
        tokens,
        max_results=max_results,
    )

    return ranked


# ============================================================
# HYBRID SEARCH
# ============================================================

async def hybrid_search_news(
    items: List[NewsItem],
    query: str,
    max_results: int = 25,
) -> List[NewsItem]:

    query = (
        query
        or ""
    ).strip()

    if not query:
        return []

    # --------------------------------------------------------
    # Search existing cache first.
    # --------------------------------------------------------

    local_results = search_news(
        items,
        tokenize_query(query),
        max_results=max_results,
    )

    # --------------------------------------------------------
    # If we already have enough results,
    # do not waste an online request.
    # --------------------------------------------------------

    if len(local_results) >= 3:

        return local_results

    # --------------------------------------------------------
    # Fresh online search.
    # --------------------------------------------------------

    online_results = await search_news_online(
        query,
        max_results=max_results,
    )

    # --------------------------------------------------------
    # Merge local + online results.
    # --------------------------------------------------------

    merged = deduplicate_news(
        local_results + online_results
    )

    # Re-rank the merged collection.
    return search_news(
        merged,
        tokenize_query(query),
        max_results=max_results,
    )


# ============================================================
# AI CONTEXT
# ============================================================

def build_ai_context(
    items: List[NewsItem],
    limit: int = 6,
) -> str:

    context_lines = []

    for item in items[:limit]:

        title = item.title or "بدون عنوان"
        source = item.source or "مصدر غير محدد"
        date = item.published_at or "غير محدد"
        region = item.region or "غير محدد"
        url = item.url or ""

        context_lines.append(
            f"- العنوان: {title}\n"
            f"  المصدر: {source}\n"
            f"  المنطقة: {region}\n"
            f"  التاريخ: {date}\n"
            f"  الرابط: {url}"
        )

    return "\n".join(
        context_lines
    )


# ============================================================
# UTILITY
# ============================================================

def build_search_url(
    query: str,
) -> str:

    encoded = urllib.parse.quote_plus(
        query.strip()
    )

    return (
        "https://www.google.com/search?q="
        f"{encoded}"
    )


# ============================================================
# COMPATIBILITY
# ============================================================

__all__ = [
    "NewsItem",
    "collect_news",
    "search_news",
    "search_news_online",
    "hybrid_search_news",
    "build_ai_context",
    "build_search_url",
    "TRUSTED_FEEDS",
    "REGIONS",
]
