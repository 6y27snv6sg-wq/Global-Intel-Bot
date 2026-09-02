import asyncio
import hashlib
import html
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional
from urllib.parse import urljoin

import aiohttp
import feedparser


# ============================================================
# SETTINGS
# ============================================================

REQUEST_TIMEOUT = 12
MAX_ITEMS_PER_SOURCE = 15
MAX_NEWS_ITEMS = 100
MAX_SUMMARY_LENGTH = 650

USER_AGENT = (
    "Mozilla/5.0 (compatible; LiveNewsBot/2.0; "
    "+https://telegram.org)"
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# SOURCE PRIORITY
# ============================================================

SOURCE_PRIORITY = {
    "official": 100,
    "official_agency": 90,
    "international_agency": 80,
    "news_channel": 70,
    "news_site": 60,
}


# ============================================================
# NEWS MODEL
# ============================================================

@dataclass
class NewsItem:
    title: str
    summary: str
    source: str
    source_type: str
    country: str
    category: str
    url: str
    published_at: Optional[datetime]
    priority: int
    event_id: str


# ============================================================
# RSS SOURCES
# ============================================================

RSS_SOURCES = [
    {
        "name": "SPA",
        "url": "https://www.spa.gov.sa/rss",
        "type": "official_agency",
        "country": "Saudi Arabia",
    },
    {
        "name": "France24 Arabic",
        "url": "https://www.france24.com/ar/rss",
        "type": "news_channel",
        "country": "France",
    },
    {
        "name": "BBC Arabic",
        "url": "https://feeds.bbci.co.uk/arabic/rss.xml",
        "type": "news_channel",
        "country": "United Kingdom",
    },
]


# ============================================================
# OFFICIAL HTML SOURCES
# ============================================================

HTML_SOURCES = [
    {
        "name": "Saudi MOFA News",
        "url": "https://www.mofa.gov.sa/en/ministry/news/Pages/default.aspx",
        "type": "official",
        "country": "Saudi Arabia",
        "category": "foreign_affairs",
    },
    {
        "name": "Saudi MOFA Statements",
        "url": "https://www.mofa.gov.sa/en/ministry/statements/Pages/default.aspx",
        "type": "official",
        "country": "Saudi Arabia",
        "category": "foreign_affairs",
    },
    {
        "name": "Qatar MOFA News",
        "url": "https://mofa.gov.qa/en/all-mofa-news",
        "type": "official",
        "country": "Qatar",
        "category": "foreign_affairs",
    },
    {
        "name": "UAE MOFA News",
        "url": "https://www.mofa.gov.ae/en/mediahub/news",
        "type": "official",
        "country": "United Arab Emirates",
        "category": "foreign_affairs",
    },
    {
        "name": "UK FCDO",
        "url": "https://www.gov.uk/government/organisations/foreign-commonwealth-development-office",
        "type": "official",
        "country": "United Kingdom",
        "category": "foreign_affairs",
    },
]


# ============================================================
# BREAKING KEYWORDS
# ============================================================

BREAKING_KEYWORDS = [
    "عاجل",
    "هجوم",
    "هجوم مسلح",
    "صاروخ",
    "صواريخ",
    "قصف",
    "انفجار",
    "حرب",
    "هدنة",
    "وقف إطلاق النار",
    "اشتباكات",
    "تصعيد",
    "عقوبات",
    "اتفاق",
    "اتفاقية",
    "أزمة",
    "طوارئ",
    "مقتل",
    "قتلى",
    "إصابة",
    "إصابات",
    "استهداف",
    "ضربة",
    "ضربات",
    "breaking",
    "attack",
    "missile",
    "missiles",
    "strike",
    "strikes",
    "war",
    "ceasefire",
    "sanctions",
    "agreement",
    "explosion",
    "conflict",
]


# ============================================================
# CATEGORY KEYWORDS
# ============================================================

CATEGORY_KEYWORDS = {
    "security": [
        "هجوم",
        "صاروخ",
        "قصف",
        "حرب",
        "اشتباكات",
        "عسكري",
        "دفاع",
        "أمن",
        "attack",
        "missile",
        "war",
        "military",
        "defense",
        "security",
    ],
    "energy": [
        "نفط",
        "النفط",
        "أوبك",
        "أوبك+",
        "غاز",
        "طاقة",
        "خام",
        "oil",
        "opec",
        "opec+",
        "gas",
        "energy",
        "crude",
    ],
    "economy": [
        "اقتصاد",
        "اقتصادية",
        "تجارة",
        "استثمار",
        "أسواق",
        "بنك",
        "فائدة",
        "تضخم",
        "عملة",
        "economy",
        "trade",
        "investment",
        "markets",
        "bank",
        "interest",
        "inflation",
        "currency",
    ],
    "politics": [
        "انتخابات",
        "رئيس",
        "حكومة",
        "برلمان",
        "سياسة",
        "سياسي",
        "president",
        "government",
        "parliament",
        "election",
        "politics",
    ],
    "foreign_affairs": [
        "وزارة الخارجية",
        "وزير الخارجية",
        "محادثات",
        "مباحثات",
        "اتصال",
        "زيارة",
        "بيان",
        "تصريح",
        "foreign ministry",
        "foreign minister",
        "talks",
        "statement",
        "diplomatic",
    ],
}


# ============================================================
# CLEAN HTML
# ============================================================

def clean_html(text: str) -> str:

    if not text:
        return ""

    text = html.unescape(text)

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


# ============================================================
# CLEAN TITLE
# ============================================================

def clean_title(title: str) -> str:

    title = clean_html(title)

    title = re.sub(
        r"^\s*(عاجل|breaking)\s*[:\-|]\s*",
        "",
        title,
        flags=re.IGNORECASE,
    )

    return title.strip()


# ============================================================
# CLEAN SUMMARY
# ============================================================

def clean_summary(summary: str) -> str:

    summary = clean_html(summary)

    if not summary:
        return ""

    noise_patterns = [
        r"اقرأ المزيد",
        r"لمزيد من التفاصيل",
        r"تابعونا",
        r"اشترك",
        r"subscribe",
        r"read more",
        r"follow us",
        r"share",
    ]

    for pattern in noise_patterns:

        summary = re.sub(
            pattern,
            " ",
            summary,
            flags=re.IGNORECASE,
        )

    summary = re.sub(
        r"\s+",
        " ",
        summary,
    ).strip()

    if len(summary) > MAX_SUMMARY_LENGTH:

        summary = (
            summary[:MAX_SUMMARY_LENGTH]
            .rsplit(" ", 1)[0]
            + "..."
        )

    return summary


# ============================================================
# DATE PARSER
# ============================================================

def parse_datetime(value) -> Optional[datetime]:

    if not value:
        return None

    if isinstance(value, datetime):

        dt = value

    else:

        value = str(value).strip()

        dt = None

        try:
            dt = parsedate_to_datetime(value)
        except Exception:
            pass

        if dt is None:

            formats = [
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
            ]

            for fmt in formats:

                try:
                    dt = datetime.strptime(
                        value,
                        fmt,
                    )
                    break

                except Exception:
                    continue

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
# NORMALIZATION
# ============================================================

def normalize_for_hash(text: str) -> str:

    text = clean_html(text).lower()

    text = re.sub(
        r"[^\w\u0600-\u06ff\s]",
        " ",
        text,
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    ).strip()

    return text


# ============================================================
# EVENT ID
# ============================================================

def make_event_id(
    title: str,
    url: str = "",
) -> str:

    base = (
        normalize_for_hash(title)
        + "|"
        + normalize_for_hash(url)
    )

    return hashlib.sha256(
        base.encode("utf-8")
    ).hexdigest()[:24]


# ============================================================
# CATEGORY DETECTION
# ============================================================

def detect_category(
    title: str,
    summary: str,
) -> str:

    text = (
        f"{title} {summary}"
    ).lower()

    scores = {}

    for category, keywords in CATEGORY_KEYWORDS.items():

        score = 0

        for keyword in keywords:

            if keyword.lower() in text:
                score += 1

        scores[category] = score

    best_category = max(
        scores,
        key=scores.get,
    )

    if scores[best_category] == 0:
        return "general"

    return best_category


# ============================================================
# RSS FETCH
# ============================================================

async def fetch_rss_source(
    session: aiohttp.ClientSession,
    source: dict,
) -> List[NewsItem]:

    items = []

    try:

        async with session.get(
            source["url"],
            timeout=REQUEST_TIMEOUT,
            headers={
                "User-Agent": USER_AGENT,
            },
        ) as response:

            if response.status != 200:

                logger.warning(
                    "RSS failed: %s -> HTTP %s",
                    source["name"],
                    response.status,
                )

                return []

            content = await response.read()

        feed = feedparser.parse(
            content
        )

        for entry in feed.entries[
            :MAX_ITEMS_PER_SOURCE
        ]:

            title = clean_title(
                entry.get(
                    "title",
                    "",
                )
            )

            if not title:
                continue

            summary = clean_summary(
                entry.get(
                    "summary",
                    entry.get(
                        "description",
                        "",
                    ),
                )
            )

            url = entry.get(
                "link",
                source["url"],
            )

            published_at = parse_datetime(
                entry.get(
                    "published",
                    entry.get(
                        "updated",
                        None,
                    ),
                )
            )

            category = detect_category(
                title,
                summary,
            )

            priority = SOURCE_PRIORITY.get(
                source["type"],
                50,
            )

            event_id = make_event_id(
                title,
                url,
            )

            items.append(
                NewsItem(
                    title=title,
                    summary=summary,
                    source=source["name"],
                    source_type=source["type"],
                    country=source["country"],
                    category=category,
                    url=url,
                    published_at=published_at,
                    priority=priority,
                    event_id=event_id,
                )
            )

    except Exception as exc:

        logger.exception(
            "RSS error: %s: %s",
            source["name"],
            exc,
        )

    return items


# ============================================================
# HTML LINK EXTRACTION
# ============================================================

def extract_links_from_html(
    html_text: str,
    base_url: str,
    source: dict,
) -> List[NewsItem]:

    results = []

    pattern = re.compile(
        r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )

    matches = pattern.findall(
        html_text
    )

    seen_urls = set()

    for href, raw_text in matches:

        title = clean_title(
            clean_html(raw_text)
        )

        if not title:
            continue

        if len(title) < 25:
            continue

        if len(title) > 300:
            continue

        url = urljoin(
            base_url,
            html.unescape(
                href
            ).strip(),
        )

        if url in seen_urls:
            continue

        seen_urls.add(url)

        lowered = url.lower()

        looks_like_article = any(
            token in lowered
            for token in [
                "/news/",
                "/statement",
                "/statements/",
                "/mediahub/",
                "/latest",
                "/details/",
                "/press",
                "/article",
            ]
        )

        if not looks_like_article:
            continue

        category = (
            source.get("category")
            or detect_category(
                title,
                "",
            )
        )

        priority = SOURCE_PRIORITY.get(
            source["type"],
            50,
        )

        results.append(
            NewsItem(
                title=title,
                summary="",
                source=source["name"],
                source_type=source["type"],
                country=source["country"],
                category=category,
                url=url,
                published_at=None,
                priority=priority,
                event_id=make_event_id(
                    title,
                    url,
                ),
            )
        )

        if len(results) >= MAX_ITEMS_PER_SOURCE:
            break

    return results


# ============================================================
# HTML FETCH
# ============================================================

async def fetch_html_source(
    session: aiohttp.ClientSession,
    source: dict,
) -> List[NewsItem]:

    try:

        async with session.get(
            source["url"],
            timeout=REQUEST_TIMEOUT,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": (
                    "text/html,"
                    "application/xhtml+xml,"
                    "application/xml;q=0.9,"
                    "*/*;q=0.8"
                ),
            },
        ) as response:

            if response.status != 200:

                logger.warning(
                    "HTML failed: %s -> HTTP %s",
                    source["name"],
                    response.status,
                )

                return []

            text = await response.text(
                errors="ignore"
            )

        return extract_links_from_html(
            text,
            source["url"],
            source,
        )

    except Exception as exc:

        logger.exception(
            "HTML error: %s: %s",
            source["name"],
            exc,
        )

        return []


# ============================================================
# TITLE SIMILARITY
# ============================================================

def similarity_key(
    title: str,
) -> set:

    normalized = normalize_for_hash(
        title
    )

    words = normalized.split()

    return {
        word
        for word in words
        if len(word) >= 3
    }


def title_similarity(
    first: str,
    second: str,
) -> float:

    a = similarity_key(first)
    b = similarity_key(second)

    if not a or not b:
        return 0.0

    intersection = len(
        a & b
    )

    union = len(
        a | b
    )

    if union == 0:
        return 0.0

    return intersection / union


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate_news(
    items: List[NewsItem],
) -> List[NewsItem]:

    items = sorted(
        items,
        key=lambda item: (
            item.priority,
            item.published_at.timestamp()
            if item.published_at
            else 0,
        ),
        reverse=True,
    )

    result = []

    seen_ids = set()

    for item in items:

        if item.event_id in seen_ids:
            continue

        duplicate = False

        for existing in result:

            similarity = title_similarity(
                item.title,
                existing.title,
            )

            if similarity >= 0.72:

                duplicate = True
                break

        if duplicate:
            continue

        seen_ids.add(
            item.event_id
        )

        result.append(item)

    return result


# ============================================================
# IMPORTANCE
# ============================================================

def calculate_importance(
    item: NewsItem,
) -> int:

    score = item.priority

    text = (
        item.title
        + " "
        + item.summary
    ).lower()

    for keyword in BREAKING_KEYWORDS:

        if keyword.lower() in text:

            score += 25
            break

    if item.published_at:

        age_hours = (
            datetime.now(timezone.utc)
            - item.published_at
        ).total_seconds() / 3600

        if age_hours <= 1:
            score += 25

        elif age_hours <= 6:
            score += 18

        elif age_hours <= 24:
            score += 10

        elif age_hours <= 72:
            score += 3

    return score


# ============================================================
# SORT
# ============================================================

def sort_news(
    items: List[NewsItem],
) -> List[NewsItem]:

    return sorted(
        items,
        key=lambda item: (
            calculate_importance(item),
            item.published_at.timestamp()
            if item.published_at
            else 0,
        ),
        reverse=True,
    )


# ============================================================
# COLLECT ALL NEWS
# ============================================================

async def collect_news(
    max_items: int = MAX_NEWS_ITEMS,
) -> List[NewsItem]:

    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT
    )

    connector = aiohttp.TCPConnector(
        limit=15,
        ssl=False,
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:

        tasks = []

        for source in RSS_SOURCES:

            tasks.append(
                fetch_rss_source(
                    session,
                    source,
                )
            )

        for source in HTML_SOURCES:

            tasks.append(
                fetch_html_source(
                    session,
                    source,
                )
            )

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

    all_items = []

    for result in results:

        if isinstance(
            result,
            Exception,
        ):

            logger.error(
                "Collector task failed: %s",
                result,
            )

            continue

        all_items.extend(
            result
        )

    logger.info(
        "Collected raw items: %s",
        len(all_items),
    )

    all_items = deduplicate_news(
        all_items
    )

    all_items = sort_news(
        all_items
    )

    all_items = all_items[
        :max_items
    ]

    logger.info(
        "Final news items: %s",
        len(all_items),
    )

    return all_items


# ============================================================
# SEARCH
# ============================================================

def search_news(
    items: List[NewsItem],
    query: str,
    limit: int = 15,
) -> List[NewsItem]:

    query_words = similarity_key(
        query
    )

    if not query_words:
        return items[:limit]

    scored = []

    for item in items:

        title_words = similarity_key(
            item.title
        )

        summary_words = similarity_key(
            item.summary
        )

        title_matches = len(
            query_words
            & title_words
        )

        summary_matches = len(
            query_words
            & summary_words
        )

        score = (
            title_matches * 5
            + summary_matches * 2
            + item.priority / 100
        )

        if score > 1:

            scored.append(
                (
                    score,
                    calculate_importance(
                        item
                    ),
                    item,
                )
            )

    scored.sort(
        key=lambda x: (
            x[0],
            x[1],
        ),
        reverse=True,
    )

    return [
        item
        for _, _, item
        in scored[:limit]
    ]


# ============================================================
# AI CONTEXT
# ============================================================

def build_ai_context(
    items: List[NewsItem],
    max_items: int = 20,
) -> str:

    selected = sort_news(
        items
    )[:max_items]

    if not selected:

        return (
            "لا توجد أخبار متاحة حاليًا."
        )

    lines = []

    for index, item in enumerate(
        selected,
        start=1,
    ):

        date_text = ""

        if item.published_at:

            date_text = (
                item.published_at
                .astimezone()
                .strftime(
                    "%Y-%m-%d %H:%M"
                )
            )

        lines.append(
            (
                f"[{index}]\n"
                f"العنوان: {item.title}\n"
                f"المصدر: {item.source}\n"
                f"نوع المصدر: {item.source_type}\n"
                f"الدولة: {item.country}\n"
                f"التصنيف: {item.category}\n"
                f"الوقت: {date_text}\n"
                f"الرابط: {item.url}\n"
                f"الملخص: {item.summary}"
            )
        )

    return "\n\n".join(
        lines
    )


# ============================================================
# TELEGRAM FORMAT
# ============================================================

def format_news_for_telegram(
    items: List[NewsItem],
    limit: int = 10,
) -> str:

    selected = sort_news(
        items
    )[:limit]

    if not selected:

        return (
            "لا توجد أخبار متاحة حاليًا."
        )

    lines = [
        "📰 آخر الأخبار",
        "",
    ]

    category_map = {
        "security": "أمن",
        "energy": "طاقة",
        "economy": "اقتصاد",
        "politics": "سياسة",
        "foreign_affairs": "خارجية",
        "general": "عام",
    }

    for item in selected:

        category = category_map.get(
            item.category,
            "عام",
        )

        lines.append(
            f"• [{category}] {item.title}"
        )

        lines.append(
            f"  المصدر: {item.source}"
        )

        if item.url:

            lines.append(
                f"  {item.url}"
            )

        lines.append("")

    return "\n".join(
        lines
    ).strip()


# ============================================================
# TEST
# ============================================================

async def test():

    print("=" * 60)
    print("LIVE NEWS ENGINE TEST")
    print("=" * 60)

    items = await collect_news(
        max_items=30
    )

    print(
        f"\nCollected: {len(items)} items\n"
    )

    for index, item in enumerate(
        items[:20],
        start=1,
    ):

        print(
            f"{index}. "
            f"[{item.source}] "
            f"{item.title}"
        )

        print(
            f"   Category: {item.category}"
        )

        print(
            f"   Priority: "
            f"{calculate_importance(item)}"
        )

        print(
            f"   URL: {item.url}"
        )

        print("-" * 60)


# ============================================================
# DIRECT TEST
# ============================================================

if __name__ == "__main__":
    asyncio.run(test())
