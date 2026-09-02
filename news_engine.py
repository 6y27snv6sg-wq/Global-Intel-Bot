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
# الإعدادات
# ============================================================

# الحد الأقصى لطلب HTTP الواحد
REQUEST_TIMEOUT = 6

# الحد الأقصى للمصدر الواحد
SOURCE_TIMEOUT = 4

# الحد الأقصى للعملية الكاملة لجمع الأخبار
GLOBAL_COLLECTION_TIMEOUT = 10

MAX_ITEMS_PER_SOURCE = 15
MAX_NEWS_ITEMS = 100
MAX_SUMMARY_LENGTH = 650

# حماية من تحميل صفحات ضخمة
MAX_RSS_BYTES = 2 * 1024 * 1024
MAX_HTML_BYTES = 1 * 1024 * 1024

USER_AGENT = (
    "Mozilla/5.0 (compatible; LiveNewsBot/3.2; +https://telegram.org)"
)

logger = logging.getLogger(__name__)


# ============================================================
# أولوية المصادر
# ============================================================

SOURCE_PRIORITY = {
    "official": 100,
    "official_agency": 90,
    "international_agency": 80,
    "news_channel": 70,
    "news_site": 60,
}


# ============================================================
# مصادر RSS
# ============================================================

RSS_SOURCES = [
    {
        "name": "وكالة الأنباء السعودية",
        "url": "https://www.spa.gov.sa/rss",
        "source_type": "official_agency",
        "country": "السعودية",
    },
    {
        "name": "France 24 Arabic",
        "url": "https://www.france24.com/ar/rss",
        "source_type": "news_channel",
        "country": "فرنسا",
    },
    {
        "name": "BBC Arabic",
        "url": "https://feeds.bbci.co.uk/arabic/rss.xml",
        "source_type": "news_channel",
        "country": "بريطانيا",
    },
]


# ============================================================
# المصادر الرسمية
# ============================================================

HTML_SOURCES = [
    {
        "name": "وزارة الخارجية السعودية",
        "url": "https://www.mofa.gov.sa/en/ministry/news/Pages/default.aspx",
        "source_type": "official",
        "country": "السعودية",
    },
    {
        "name": "وزارة الخارجية السعودية - البيانات",
        "url": "https://www.mofa.gov.sa/en/ministry/statements/Pages/default.aspx",
        "source_type": "official",
        "country": "السعودية",
    },
    {
        "name": "وزارة الخارجية القطرية",
        "url": "https://mofa.gov.qa/en/all-mofa-news",
        "source_type": "official",
        "country": "قطر",
    },
    {
        "name": "وزارة الخارجية الإماراتية",
        "url": "https://www.mofa.gov.ae/en/mediahub/news",
        "source_type": "official",
        "country": "الإمارات",
    },
    {
        "name": "وزارة الخارجية البريطانية",
        "url": "https://www.gov.uk/government/organisations/foreign-commonwealth-development-office",
        "source_type": "official",
        "country": "بريطانيا",
    },
]


# ============================================================
# كلمات الأخبار العاجلة
# ============================================================

BREAKING_KEYWORDS = [
    "عاجل",
    "هجوم",
    "هجمات",
    "صاروخ",
    "صواريخ",
    "قصف",
    "انفجار",
    "انفجارات",
    "حرب",
    "اشتباك",
    "اشتباكات",
    "تصعيد",
    "غارة",
    "غارات",
    "هدنة",
    "وقف إطلاق النار",
    "إطلاق النار",
    "اغتيال",
    "قتلى",
    "إصابة",
    "إصابات",
    "عقوبات",
    "اتفاق",
    "أزمة",
    "طوارئ",
    "زلزال",
    "فيضانات",
    "إعصار",
    "breaking",
    "urgent",
    "missile",
    "attack",
    "war",
    "strike",
    "explosion",
    "ceasefire",
    "sanctions",
]


# ============================================================
# التصنيفات
# ============================================================

CATEGORY_KEYWORDS = {
    "security": [
        "أمن",
        "أمني",
        "عسكري",
        "جيش",
        "هجوم",
        "صاروخ",
        "قصف",
        "حرب",
        "اشتباك",
        "دفاع",
        "طائرة",
        "مسيرة",
        "درون",
        "غارة",
        "حدود",
    ],
    "energy": [
        "نفط",
        "أوبك",
        "أوبك+",
        "غاز",
        "طاقة",
        "برميل",
        "إنتاج النفط",
        "أسعار النفط",
        "الوقود",
    ],
    "economy": [
        "اقتصاد",
        "اقتصادية",
        "أسواق",
        "بورصة",
        "سهم",
        "أسهم",
        "تجارة",
        "استثمار",
        "استثمارات",
        "دولار",
        "ريال",
        "تضخم",
        "فائدة",
        "بنك",
    ],
    "politics": [
        "سياسة",
        "سياسي",
        "انتخابات",
        "حكومة",
        "رئيس",
        "برلمان",
        "وزير",
        "حزب",
        "قرار",
    ],
    "foreign_affairs": [
        "وزارة الخارجية",
        "وزير الخارجية",
        "سفير",
        "سفارة",
        "بيان",
        "تصريح",
        "مباحثات",
        "مفاوضات",
        "اتفاق",
        "علاقات",
        "دبلوماسي",
        "دبلوماسية",
    ],
}


# ============================================================
# نموذج الخبر
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
# تطبيع النص
# ============================================================

def normalize_arabic(text: str) -> str:
    if not text:
        return ""

    text = str(text)

    text = re.sub(
        r"[\u0610-\u061A\u064B-\u065F\u0670]",
        "",
        text,
    )

    text = text.replace("ـ", "")

    text = re.sub(
        r"[أإآا]",
        "ا",
        text,
    )

    text = text.replace("ى", "ي")

    text = text.replace("ة", "ه")

    text = re.sub(
        r"[^\w\s]",
        " ",
        text,
        flags=re.UNICODE,
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    ).strip()

    return text.lower()


def normalize_for_hash(text: str) -> str:
    return normalize_arabic(text)


# ============================================================
# تنظيف HTML
# ============================================================

def clean_html(text: str) -> str:
    if not text:
        return ""

    text = html.unescape(str(text))

    text = re.sub(
        r"<script\b[^>]*>.*?</script>",
        " ",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )

    text = re.sub(
        r"<style\b[^>]*>.*?</style>",
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


def clean_title(title: str) -> str:
    title = clean_html(title)

    title = re.sub(
        r"^\s*(عاجل|breaking)\s*[:\-–—]?\s*",
        "",
        title,
        flags=re.IGNORECASE,
    )

    return title.strip()


def clean_summary(summary: str) -> str:
    summary = clean_html(summary)

    noise_patterns = [
        r"اقرأ المزيد",
        r"لمزيد من التفاصيل",
        r"تابعونا",
        r"للمزيد",
        r"المصدر:",
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
# التاريخ
# ============================================================

def parse_datetime(value) -> Optional[datetime]:
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.astimezone(timezone.utc)

    try:
        value = str(value).strip()

        if not value:
            return None

        try:
            dt = parsedate_to_datetime(value)

            if dt.tzinfo is None:
                dt = dt.replace(
                    tzinfo=timezone.utc
                )

            return dt.astimezone(timezone.utc)

        except Exception:
            pass

        try:
            dt = datetime.fromisoformat(
                value.replace(
                    "Z",
                    "+00:00",
                )
            )

            if dt.tzinfo is None:
                dt = dt.replace(
                    tzinfo=timezone.utc
                )

            return dt.astimezone(timezone.utc)

        except Exception:
            pass

        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(
                    value,
                    fmt,
                )

                return dt.replace(
                    tzinfo=timezone.utc
                )

            except ValueError:
                continue

    except Exception:
        pass

    return None


def parse_feed_entry_datetime(entry) -> Optional[datetime]:

    for field in (
        "published_parsed",
        "updated_parsed",
        "created_parsed",
    ):
        value = entry.get(field)

        if value:
            try:
                return datetime(
                    value.tm_year,
                    value.tm_mon,
                    value.tm_mday,
                    value.tm_hour,
                    value.tm_min,
                    value.tm_sec,
                    tzinfo=timezone.utc,
                )
            except Exception:
                pass

    for field in (
        "published",
        "updated",
        "created",
        "pubDate",
    ):
        value = entry.get(field)

        dt = parse_datetime(value)

        if dt:
            return dt

    return None


# ============================================================
# معرف الحدث
# ============================================================

def make_event_id(
    title: str,
    url: str = "",
) -> str:

    normalized_title = normalize_for_hash(
        title
    )

    raw = normalized_title or normalize_for_hash(
        url
    )

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()[:24]


# ============================================================
# التصنيف
# ============================================================

def detect_category(
    title: str,
    summary: str = "",
) -> str:

    text = normalize_arabic(
        f"{title} {summary}"
    )

    scores = {}

    for category, keywords in CATEGORY_KEYWORDS.items():

        score = 0

        for keyword in keywords:

            if normalize_arabic(keyword) in text:
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
# خبر عاجل؟
# ============================================================

def is_breaking_news(
    title: str,
    summary: str = "",
) -> bool:

    text = normalize_arabic(
        f"{title} {summary}"
    )

    for keyword in BREAKING_KEYWORDS:

        if normalize_arabic(keyword) in text:
            return True

    return False


# ============================================================
# قراءة استجابة محدودة الحجم
# ============================================================

async def read_limited_response(
    response: aiohttp.ClientResponse,
    max_bytes: int,
) -> bytes:

    chunks = []
    total = 0

    while total < max_bytes:

        remaining = max_bytes - total

        chunk = await response.content.read(
            min(64 * 1024, remaining)
        )

        if not chunk:
            break

        chunks.append(chunk)
        total += len(chunk)

    return b"".join(chunks)


# ============================================================
# جلب RSS
# ============================================================

async def fetch_rss_source(
    session: aiohttp.ClientSession,
    source: dict,
) -> List[NewsItem]:

    results = []

    try:

        logger.info(
            "Starting RSS source: %s",
            source["name"],
        )

        async with session.get(
            source["url"],
            headers={
                "User-Agent": USER_AGENT
            },
        ) as response:

            if response.status != 200:

                logger.warning(
                    "RSS %s returned HTTP %s",
                    source["name"],
                    response.status,
                )

                return []

            content = await read_limited_response(
                response,
                MAX_RSS_BYTES,
            )

        if not content:
            return []

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

            link = entry.get(
                "link",
                "",
            )

            if not link:
                continue

            published_at = parse_feed_entry_datetime(
                entry
            )

            results.append(
                NewsItem(
                    title=title,
                    summary=summary,
                    source=source["name"],
                    source_type=source["source_type"],
                    country=source["country"],
                    category=detect_category(
                        title,
                        summary,
                    ),
                    url=link,
                    published_at=published_at,
                    priority=SOURCE_PRIORITY.get(
                        source["source_type"],
                        50,
                    ),
                    event_id=make_event_id(
                        title,
                        link,
                    ),
                )
            )

        logger.info(
            "RSS completed: %s -> %d items",
            source["name"],
            len(results),
        )

    except asyncio.CancelledError:
        logger.info(
            "RSS cancelled: %s",
            source["name"],
        )
        raise

    except Exception as exc:

        logger.warning(
            "RSS failed: %s -> %s",
            source["name"],
            exc,
        )

    return results


# ============================================================
# استخراج روابط HTML
# ============================================================

def extract_links_from_html(
    page_html: str,
    base_url: str,
) -> List[tuple]:

    results = []

    if not page_html:
        return results

    pattern = re.compile(
        r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        flags=re.IGNORECASE | re.DOTALL,
    )

    for match in pattern.finditer(
        page_html
    ):

        href = html.unescape(
            match.group(1).strip()
        )

        title = clean_title(
            match.group(2)
        )

        if not href or not title:
            continue

        if len(title) < 15:
            continue

        full_url = urljoin(
            base_url,
            href,
        )

        lowered_url = full_url.lower()

        interesting = any(
            token in lowered_url
            for token in (
                "/news/",
                "/statement",
                "/mediahub/",
                "/press",
                "/article",
                "/story",
            )
        )

        if not interesting:
            continue

        results.append(
            (
                title,
                full_url,
            )
        )

    return results


# ============================================================
# جلب HTML
# ============================================================

async def fetch_html_source(
    session: aiohttp.ClientSession,
    source: dict,
) -> List[NewsItem]:

    results = []

    try:

        logger.info(
            "Starting HTML source: %s",
            source["name"],
        )

        async with session.get(
            source["url"],
            headers={
                "User-Agent": USER_AGENT
            },
        ) as response:

            if response.status != 200:

                logger.warning(
                    "HTML %s returned HTTP %s",
                    source["name"],
                    response.status,
                )

                return []

            content = await read_limited_response(
                response,
                MAX_HTML_BYTES,
            )

        if not content:
            return []

        page_html = content.decode(
            "utf-8",
            errors="ignore",
        )

        links = extract_links_from_html(
            page_html,
            source["url"],
        )

        seen_urls = set()

        for title, link in links:

            if link in seen_urls:
                continue

            seen_urls.add(link)

            results.append(
                NewsItem(
                    title=title,
                    summary="",
                    source=source["name"],
                    source_type=source["source_type"],
                    country=source["country"],
                    category=detect_category(
                        title
                    ),
                    url=link,
                    published_at=None,
                    priority=SOURCE_PRIORITY.get(
                        source["source_type"],
                        50,
                    ),
                    event_id=make_event_id(
                        title,
                        link,
                    ),
                )
            )

            if len(results) >= MAX_ITEMS_PER_SOURCE:
                break

        logger.info(
            "HTML completed: %s -> %d items",
            source["name"],
            len(results),
        )

    except asyncio.CancelledError:
        logger.info(
            "HTML cancelled: %s",
            source["name"],
        )
        raise

    except Exception as exc:

        logger.warning(
            "HTML failed: %s -> %s",
            source["name"],
            exc,
        )

    return results


# ============================================================
# تشابه العناوين
# ============================================================

def similarity_key(
    text: str,
) -> set:

    normalized = normalize_arabic(
        text
    )

    words = re.findall(
        r"\b[\w\u0600-\u06FF]{3,}\b",
        normalized,
        flags=re.UNICODE,
    )

    return set(words)


def title_similarity(
    title_a: str,
    title_b: str,
) -> float:

    a = similarity_key(title_a)
    b = similarity_key(title_b)

    if not a or not b:
        return 0.0

    union = len(a | b)

    if union == 0:
        return 0.0

    return len(a & b) / union


# ============================================================
# إزالة النسخ المتطابقة
# ============================================================

def deduplicate_news(
    items: List[NewsItem],
) -> List[NewsItem]:

    if not items:
        return []

    sorted_items = sorted(
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

    seen_urls = set()
    seen_titles = set()

    for item in sorted_items:

        normalized_url = (
            item.url.strip().lower()
        )

        normalized_title = normalize_for_hash(
            item.title
        )

        if normalized_url in seen_urls:
            continue

        if normalized_title in seen_titles:
            continue

        seen_urls.add(
            normalized_url
        )

        seen_titles.add(
            normalized_title
        )

        result.append(item)

    return result


# ============================================================
# تجميع الأحداث المتشابهة
# ============================================================

def cluster_events(
    items: List[NewsItem],
) -> List[NewsItem]:

    if not items:
        return []

    clusters = []

    for item in items:

        assigned = False

        for cluster in clusters:

            representative = cluster[0]

            if title_similarity(
                item.title,
                representative.title,
            ) >= 0.82:

                item.event_id = (
                    representative.event_id
                )

                cluster.append(
                    item
                )

                assigned = True
                break

        if not assigned:

            clusters.append(
                [item]
            )

    return items


# ============================================================
# حساب أهمية الخبر
# ============================================================

def calculate_importance(
    item: NewsItem,
) -> float:

    score = float(
        item.priority
    )

    if is_breaking_news(
        item.title,
        item.summary,
    ):
        score += 25

    if item.published_at:

        now = datetime.now(
            timezone.utc
        )

        age_seconds = (
            now - item.published_at
        ).total_seconds()

        age_seconds = max(
            0,
            age_seconds,
        )

        age_hours = (
            age_seconds / 3600
        )

        if age_hours <= 1:
            score += 25

        elif age_hours <= 6:
            score += 18

        elif age_hours <= 24:
            score += 10

        elif age_hours <= 72:
            score += 3

        else:
            score -= min(
                20,
                (age_hours - 72) / 24,
            )

    return max(
        0,
        score,
    )


# ============================================================
# ترتيب الأخبار
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
# تشغيل مصدر مع مهلة مستقلة
# ============================================================

async def run_source_safely(
    source_name: str,
    coroutine,
):

    try:

        return await asyncio.wait_for(
            coroutine,
            timeout=SOURCE_TIMEOUT,
        )

    except asyncio.TimeoutError:

        logger.warning(
            "SOURCE TIMEOUT: %s",
            source_name,
        )

        return []

    except asyncio.CancelledError:
        raise

    except Exception as exc:

        logger.warning(
            "SOURCE ERROR: %s -> %s",
            source_name,
            exc,
        )

        return []


# ============================================================
# جمع الأخبار الداخلي
# ============================================================

async def _collect_news_internal(
    session: aiohttp.ClientSession,
) -> List[NewsItem]:

    all_items = []

    tasks = []

    # --------------------------------------------------------
    # RSS
    # --------------------------------------------------------

    for source in RSS_SOURCES:

        task = asyncio.create_task(
            run_source_safely(
                source["name"],
                fetch_rss_source(
                    session,
                    source,
                ),
            )
        )

        tasks.append(task)

    # --------------------------------------------------------
    # HTML
    # --------------------------------------------------------

    for source in HTML_SOURCES:

        task = asyncio.create_task(
            run_source_safely(
                source["name"],
                fetch_html_source(
                    session,
                    source,
                ),
            )
        )

        tasks.append(task)

    if not tasks:
        return []

    logger.info(
        "Started %d news sources",
        len(tasks),
    )

    # --------------------------------------------------------
    # الانتظار بمهلة إجمالية
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # قراءة المصادر التي انتهت
    # --------------------------------------------------------

    for task in done:

        if task.cancelled():
            continue

        try:

            result = task.result()

            if result:
                all_items.extend(
                    result
                )

        except Exception as exc:

            logger.warning(
                "Completed source task failed: %s",
                exc,
            )

    # --------------------------------------------------------
    # إلغاء المصادر التي ما زالت معلقة
    # --------------------------------------------------------

    if pending:

        logger.warning(
            "Cancelling %d pending source tasks",
            len(pending),
        )

        for task in pending:
            task.cancel()

        await asyncio.gather(
            *pending,
            return_exceptions=True,
        )

    return all_items


# ============================================================
# جمع الأخبار
# ============================================================

async def collect_news(
    max_items: int = MAX_NEWS_ITEMS,
) -> List[NewsItem]:

    logger.info(
        "========== NEWS COLLECTION START =========="
    )

    all_items = []

    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT,
        connect=3,
        sock_connect=3,
        sock_read=4,
    )

    connector = aiohttp.TCPConnector(
        limit=10,
        ssl=True,
        ttl_dns_cache=300,
    )

    try:

        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                "User-Agent": USER_AGENT
            },
        ) as session:

            # ------------------------------------------------
            # حماية إضافية للعملية كاملة
            # ------------------------------------------------

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

                all_items = []

    except asyncio.CancelledError:
        raise

    except Exception as exc:

        logger.exception(
            "collect_news failed: %s",
            exc,
        )

    # ========================================================
    # المعالجة
    # ========================================================

    logger.info(
        "Raw news items: %d",
        len(all_items),
    )

    all_items = deduplicate_news(
        all_items
    )

    logger.info(
        "After deduplication: %d",
        len(all_items),
    )

    all_items = cluster_events(
        all_items
    )

    all_items = sort_news(
        all_items
    )

    final_items = all_items[
        :max_items
    ]

    logger.info(
        "Final news items: %d",
        len(final_items),
    )

    logger.info(
        "========== NEWS COLLECTION END =========="
    )

    return final_items


# ============================================================
# البحث
# ============================================================

def search_news(
    items: List[NewsItem],
    query: str,
    max_results: int = 15,
) -> List[NewsItem]:

    if not items or not query:
        return []

    query_normalized = normalize_arabic(
        query
    )

    query_tokens = set(
        query_normalized.split()
    )

    if not query_tokens:
        return []

    scored = []

    for item in items:

        title_normalized = normalize_arabic(
            item.title
        )

        summary_normalized = normalize_arabic(
            item.summary
        )

        title_tokens = set(
            title_normalized.split()
        )

        summary_tokens = set(
            summary_normalized.split()
        )

        title_overlap = len(
            query_tokens
            & title_tokens
        )

        summary_overlap = len(
            query_tokens
            & summary_tokens
        )

        score = (
            title_overlap * 6
            + summary_overlap * 2
            + item.priority / 100
        )

        if query_normalized in title_normalized:
            score += 8

        elif query_normalized in summary_normalized:
            score += 3

        if (
            title_overlap > 0
            or summary_overlap > 0
        ):
            score += (
                calculate_importance(item)
                / 100
            )

        if score > 1:
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
# سياق Gemini
# ============================================================

def build_ai_context(
    news_items: List[NewsItem],
    max_items: int = 20,
) -> str:

    if not news_items:
        return (
            "لا توجد أخبار متاحة حالياً."
        )

    chunks = []

    for index, item in enumerate(
        news_items[:max_items],
        start=1,
    ):

        if item.published_at:

            published = (
                item.published_at.strftime(
                    "%Y-%m-%d %H:%M UTC"
                )
            )

        else:
            published = "غير محدد"

        chunks.append(
            f"""
الخبر {index}
العنوان: {item.title}
المصدر: {item.source}
نوع المصدر: {item.source_type}
الدولة: {item.country}
التصنيف: {item.category}
وقت النشر: {published}
معرف الحدث: {item.event_id}
الرابط: {item.url}
الملخص: {item.summary or "لا يوجد ملخص"}
""".strip()
        )

    return "\n\n".join(
        chunks
    )


# ============================================================
# تنسيق Telegram
# ============================================================

def format_news_for_telegram(
    items: List[NewsItem],
    max_items: int = 10,
) -> str:

    if not items:
        return (
            "لا توجد أخبار متاحة حالياً."
        )

    chunks = []

    for index, item in enumerate(
        items[:max_items],
        start=1,
    ):

        breaking = ""

        if is_breaking_news(
            item.title,
            item.summary,
        ):
            breaking = "🚨 "

        published = (
            item.published_at.strftime(
                "%Y-%m-%d %H:%M UTC"
            )
            if item.published_at
            else "غير محدد"
        )

        text = (
            f"{breaking}{index}. {item.title}\n"
            f"المصدر: {item.source}\n"
            f"التصنيف: {item.category}\n"
            f"الوقت: {published}\n"
        )

        if item.summary:
            text += (
                f"الملخص: {item.summary}\n"
            )

        text += (
            f"الرابط: {item.url}"
        )

        chunks.append(text)

    return "\n\n".join(
        chunks
    )


# ============================================================
# اختبار
# ============================================================

async def test_news_engine():

    logger.info(
        "Starting news engine test..."
    )

    started = asyncio.get_running_loop().time()

    items = await collect_news(
        max_items=20
    )

    elapsed = (
        asyncio.get_running_loop().time()
        - started
    )

    logger.info(
        "Collected %d news items in %.2f seconds.",
        len(items),
        elapsed,
    )

    if items:

        logger.info(
            "\n%s",
            format_news_for_telegram(
                items,
                max_items=5,
            ),
        )

    else:

        logger.warning(
            "No news items collected."
        )

    return items


# ============================================================
# تشغيل مباشر
# ============================================================

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    asyncio.run(
        test_news_engine()
    )
