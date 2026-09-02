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

REQUEST_TIMEOUT = 12
MAX_ITEMS_PER_SOURCE = 15
MAX_NEWS_ITEMS = 100
MAX_SUMMARY_LENGTH = 650

USER_AGENT = (
    "Mozilla/5.0 (compatible; LiveNewsBot/3.0; +https://telegram.org)"
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
# المصادر الرسمية HTML
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
    "تصعيد عسكري",
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
# تصنيفات الأخبار
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
        "الطاقة",
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
# أدوات تنظيف النص
# ============================================================

def normalize_arabic(text: str) -> str:
    """
    تطبيع النص العربي لتقليل اختلافات الكتابة أثناء البحث والمقارنة.
    """

    if not text:
        return ""

    text = str(text)

    # إزالة التشكيل
    text = re.sub(r"[\u0610-\u061A\u064B-\u065F\u0670]", "", text)

    # إزالة التطويل
    text = text.replace("ـ", "")

    # توحيد الألف
    text = re.sub(r"[أإآا]", "ا", text)

    # توحيد الياء والألف المقصورة
    text = text.replace("ى", "ي")

    # توحيد التاء المربوطة
    text = text.replace("ة", "ه")

    # إزالة علامات الترقيم
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)

    # توحيد المسافات
    text = re.sub(r"\s+", " ", text).strip()

    return text.lower()


def normalize_for_hash(text: str) -> str:
    return normalize_arabic(text)


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

    text = re.sub(r"<[^>]+>", " ", text)

    text = re.sub(r"\s+", " ", text)

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

    summary = re.sub(r"\s+", " ", summary).strip()

    if len(summary) > MAX_SUMMARY_LENGTH:
        summary = summary[:MAX_SUMMARY_LENGTH].rsplit(" ", 1)[0]
        summary += "..."

    return summary


# ============================================================
# التاريخ والوقت
# ============================================================

def parse_datetime(value) -> Optional[datetime]:
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    try:
        value = str(value).strip()

        if not value:
            return None

        # RFC / RSS
        try:
            dt = parsedate_to_datetime(value)

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            return dt.astimezone(timezone.utc)
        except Exception:
            pass

        # ISO
        iso_value = value.replace("Z", "+00:00")

        try:
            dt = datetime.fromisoformat(iso_value)

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

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
                dt = datetime.strptime(value, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

    except Exception:
        pass

    return None


def parse_feed_entry_datetime(entry) -> Optional[datetime]:
    """
    يحاول قراءة الوقت من الحقول المنظمة في RSS أولاً،
    ثم ينتقل إلى النص.
    """

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

def make_event_id(title: str, url: str = "") -> str:
    """
    ينشئ معرفاً مستقراً للحدث اعتماداً على العنوان.
    لا نعتمد على الرابط وحده حتى لا يصبح الخبر نفسه
    حدثاً مختلفاً بسبب اختلاف رابط المصدر.
    """

    normalized_title = normalize_for_hash(title)

    raw = normalized_title

    if not raw:
        raw = normalize_for_hash(url)

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()[:24]


# ============================================================
# التصنيف
# ============================================================

def detect_category(title: str, summary: str = "") -> str:
    text = normalize_arabic(
        f"{title} {summary}"
    )

    scores = {}

    for category, keywords in CATEGORY_KEYWORDS.items():
        score = 0

        for keyword in keywords:
            keyword_normalized = normalize_arabic(keyword)

            if keyword_normalized in text:
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
# اكتشاف الأخبار العاجلة
# ============================================================

def is_breaking_news(title: str, summary: str = "") -> bool:
    text = normalize_arabic(
        f"{title} {summary}"
    )

    for keyword in BREAKING_KEYWORDS:
        keyword_normalized = normalize_arabic(keyword)

        if keyword_normalized in text:
            return True

    return False


# ============================================================
# RSS
# ============================================================

async def fetch_rss_source(
    session: aiohttp.ClientSession,
    source: dict,
) -> List[NewsItem]:

    results = []

    try:
        async with session.get(
            source["url"],
            headers={"User-Agent": USER_AGENT},
        ) as response:

            if response.status != 200:
                logger.warning(
                    "RSS source returned HTTP %s: %s",
                    response.status,
                    source["name"],
                )
                return []

            content = await response.read()

        feed = feedparser.parse(content)

        entries = feed.entries[:MAX_ITEMS_PER_SOURCE]

        for entry in entries:

            title = clean_title(
                entry.get("title", "")
            )

            if not title:
                continue

            summary = clean_summary(
                entry.get(
                    "summary",
                    entry.get("description", ""),
                )
            )

            link = entry.get("link", "")

            if not link:
                continue

            published_at = parse_feed_entry_datetime(
                entry
            )

            category = detect_category(
                title,
                summary,
            )

            priority = SOURCE_PRIORITY.get(
                source["source_type"],
                50,
            )

            event_id = make_event_id(
                title
            )

            results.append(
                NewsItem(
                    title=title,
                    summary=summary,
                    source=source["name"],
                    source_type=source["source_type"],
                    country=source["country"],
                    category=category,
                    url=link,
                    published_at=published_at,
                    priority=priority,
                    event_id=event_id,
                )
            )

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        logger.warning(
            "RSS fetch failed for %s: %s",
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

    for match in pattern.finditer(page_html):

        href = html.unescape(
            match.group(1).strip()
        )

        raw_title = match.group(2)

        title = clean_title(raw_title)

        if not href or not title:
            continue

        if len(title) < 15:
            continue

        full_url = urljoin(
            base_url,
            href,
        )

        lowered_url = full_url.lower()

        interesting_path = any(
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

        if not interesting_path:
            continue

        results.append(
            (
                title,
                full_url,
            )
        )

    return results


# ============================================================
# HTML Sources
# ============================================================

async def fetch_html_source(
    session: aiohttp.ClientSession,
    source: dict,
) -> List[NewsItem]:

    results = []

    try:
        async with session.get(
            source["url"],
            headers={"User-Agent": USER_AGENT},
        ) as response:

            if response.status != 200:
                logger.warning(
                    "HTML source returned HTTP %s: %s",
                    response.status,
                    source["name"],
                )
                return []

            page_html = await response.text(
                errors="ignore"
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

            summary = ""

            category = detect_category(
                title,
                summary,
            )

            priority = SOURCE_PRIORITY.get(
                source["source_type"],
                50,
            )

            event_id = make_event_id(
                title
            )

            results.append(
                NewsItem(
                    title=title,
                    summary=summary,
                    source=source["name"],
                    source_type=source["source_type"],
                    country=source["country"],
                    category=category,
                    url=link,
                    published_at=None,
                    priority=priority,
                    event_id=event_id,
                )
            )

            if len(results) >= MAX_ITEMS_PER_SOURCE:
                break

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        logger.warning(
            "HTML fetch failed for %s: %s",
            source["name"],
            exc,
        )

    return results


# ============================================================
# مقارنة العناوين
# ============================================================

def similarity_key(text: str) -> set:
    normalized = normalize_arabic(text)

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

    intersection = len(a & b)
    union = len(a | b)

    if union == 0:
        return 0.0

    return intersection / union


# ============================================================
# إزالة التكرار
# ============================================================

def deduplicate_news(
    items: List[NewsItem],
) -> List[NewsItem]:

    if not items:
        return []

    # ترتيب أولي حتى نحافظ على المصدر الأقوى
    # عند وجود نسخة مطابقة فعلاً.
    sorted_items = sorted(
        items,
        key=lambda x: (
            x.priority,
            x.published_at.timestamp()
            if x.published_at
            else 0,
        ),
        reverse=True,
    )

    result = []

    seen_urls = set()
    seen_exact_titles = set()

    for item in sorted_items:

        normalized_title = normalize_for_hash(
            item.title
        )

        normalized_url = item.url.strip().lower()

        # نفس الرابط = نفس الخبر
        if normalized_url in seen_urls:
            continue

        # نفس العنوان حرفياً تقريباً = نسخة مكررة
        if normalized_title in seen_exact_titles:
            continue

        seen_urls.add(normalized_url)
        seen_exact_titles.add(normalized_title)

        result.append(item)

    # لا نحذف الأخبار المختلفة فقط لأنها تتحدث
    # عن نفس الحدث؛ نريد أن تبقى لدينا مصادر متعددة
    # ليستطيع Gemini المقارنة بينها.
    return result


# ============================================================
# ربط الأخبار المتشابهة بالحدث نفسه
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

            similarity = title_similarity(
                item.title,
                representative.title,
            )

            # عتبة مرتفعة حتى لا ندمج أخباراً
            # مختلفة بالخطأ.
            if similarity >= 0.82:

                item.event_id = representative.event_id
                cluster.append(item)
                assigned = True
                break

        if not assigned:
            clusters.append([item])

    return items


# ============================================================
# أهمية الخبر
# ============================================================

def calculate_importance(
    item: NewsItem,
) -> float:

    score = float(item.priority)

    text = f"{item.title} {item.summary}"

    if is_breaking_news(text):
        score += 25

    if item.published_at:

        now = datetime.now(timezone.utc)

        age_seconds = (
            now - item.published_at
        ).total_seconds()

        # خبر مستقبلي بسبب خطأ مصدر الوقت
        # لا نعاقبه.
        age_seconds = max(
            0,
            age_seconds,
        )

        age_hours = age_seconds / 3600

        if age_hours <= 1:
            score += 25

        elif age_hours <= 6:
            score += 18

        elif age_hours <= 24:
            score += 10

        elif age_hours <= 72:
            score += 3

        else:
            # الأخبار القديمة جداً لا تختفي،
            # لكن تحصل على وزن أقل.
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
# جمع الأخبار
# ============================================================

async def collect_news(
    max_items: int = MAX_NEWS_ITEMS,
) -> List[NewsItem]:

    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT,
        connect=5,
        sock_read=REQUEST_TIMEOUT,
    )

    connector = aiohttp.TCPConnector(
        limit=15,
    )

    all_items = []

    try:

        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                "User-Agent": USER_AGENT,
            },
        ) as session:

            rss_tasks = [
                fetch_rss_source(
                    session,
                    source,
                )
                for source in RSS_SOURCES
            ]

            html_tasks = [
                fetch_html_source(
                    session,
                    source,
                )
                for source in HTML_SOURCES
            ]

            results = await asyncio.gather(
                *(rss_tasks + html_tasks),
                return_exceptions=True,
            )

            for result in results:

                if isinstance(
                    result,
                    Exception,
                ):
                    logger.warning(
                        "Source task failed: %s",
                        result,
                    )
                    continue

                if result:
                    all_items.extend(result)

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        logger.exception(
            "News collection failed: %s",
            exc,
        )

    # إزالة النسخ المتطابقة
    all_items = deduplicate_news(
        all_items
    )

    # ربط الأخبار المتشابهة بالحدث نفسه
    # مع الإبقاء على كل المصادر.
    all_items = cluster_events(
        all_items
    )

    # ترتيب حسب الأهمية والحداثة
    all_items = sort_news(
        all_items
    )

    return all_items[:max_items]


# ============================================================
# البحث في الأخبار
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
            query_tokens & title_tokens
        )

        summary_overlap = len(
            query_tokens & summary_tokens
        )

        score = (
            title_overlap * 6
            + summary_overlap * 2
            + item.priority / 100
        )

        # تطابق العبارة كاملة
        if query_normalized in title_normalized:
            score += 8

        elif query_normalized in summary_normalized:
            score += 3

        # البحث عن تطابق جزئي مهم
        if title_overlap > 0 or summary_overlap > 0:
            score += calculate_importance(item) / 100

        if score > 1:
            scored.append(
                (
                    score,
                    item,
                )
            )

    scored.sort(
        key=lambda pair: pair[0],
        reverse=True,
    )

    return [
        item
        for _, item in scored[:max_results]
    ]


# ============================================================
# بناء سياق Gemini
# ============================================================

def build_ai_context(
    news_items: List[NewsItem],
    max_items: int = 20,
) -> str:

    if not news_items:
        return "لا توجد أخبار متاحة حالياً."

    selected = news_items[:max_items]

    chunks = []

    for index, item in enumerate(
        selected,
        start=1,
    ):

        if item.published_at:
            published = item.published_at.strftime(
                "%Y-%m-%d %H:%M UTC"
            )
        else:
            published = "غير محدد"

        chunk = (
            f"الخبر {index}\n"
            f"العنوان: {item.title}\n"
            f"المصدر: {item.source}\n"
            f"نوع المصدر: {item.source_type}\n"
            f"الدولة: {item.country}\n"
            f"التصنيف: {item.category}\n"
            f"وقت النشر: {published}\n"
            f"معرف الحدث: {item.event_id}\n"
            f"الرابط: {item.url}\n"
            f"الملخص: {item.summary or 'لا يوجد ملخص'}"
        )

        chunks.append(chunk)

    return "\n\n".join(chunks)


# ============================================================
# تنسيق الأخبار لتليجرام
# ============================================================

def format_news_for_telegram(
    items: List[NewsItem],
    max_items: int = 10,
) -> str:

    if not items:
        return "لا توجد أخبار متاحة حالياً."

    chunks = []

    for index, item in enumerate(
        items[:max_items],
        start=1,
    ):

        if item.published_at:
            published = item.published_at.strftime(
                "%Y-%m-%d %H:%M UTC"
            )
        else:
            published = "غير محدد"

        breaking = (
            "عاجل"
            if is_breaking_news(
                item.title,
                item.summary,
            )
            else ""
        )

        header = (
            f"{index}. {item.title}"
        )

        if breaking:
            header = f"🚨 {breaking}: {header}"

        chunks.append(
            "\n".join(
                [
                    header,
                    f"المصدر: {item.source}",
                    f"التصنيف: {item.category}",
                    f"الوقت: {published}",
                    (
                        f"الملخص: {item.summary}"
                        if item.summary
                        else ""
                    ),
                    f"الرابط: {item.url}",
                ]
            )
        )

    return "\n\n".join(chunks)


# ============================================================
# اختبار المحرك
# ============================================================

async def test_news_engine():
    logger.info(
        "Starting news engine test..."
    )

    items = await collect_news(
        max_items=20
    )

    logger.info(
        "Collected %d news items.",
        len(items),
    )

    if items:
        logger.info(
            "\n%s",
            format_news_for_telegram(
                items,
                max_items=5,
            ),
        )

    return items


# ============================================================
# تشغيل مباشر للاختبار
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
