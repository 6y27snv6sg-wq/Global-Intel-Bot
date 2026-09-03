import asyncio
import html
import logging
import re
import urllib.parse
from typing import List, Dict, Tuple

import aiohttp
import feedparser


log = logging.getLogger("news_engine")

FETCH_TIMEOUT = 8
MAX_PER_FEED = 20

GOOGLE_NEWS_BASE = (
    "https://news.google.com/rss/search?"
    "q={query}&hl=ar&gl=SA&ceid=SA:ar"
)


# ============================================================
# المصادر المباشرة
# ============================================================

TRUSTED_FEEDS: Dict[str, str] = {
    "العربية": "https://www.alarabiya.net/.well-known/rss/urgent.xml",
    "الجزيرة": (
        "https://www.aljazeera.net/aljazeerarss/"
        "a7c1866f-6829-4883-8441-358d731800bc/"
        "43316f44-8e12-4320-b4c2-a22f6654b321"
    ),
    "سكاي نيوز عربية": (
        "https://www.skynewsarabia.com/rss/v1/news.xml"
    ),
    "الشرق": "https://asharq.com/rss/",
    "CNBC عربية": "https://www.cnbcarabia.com/rss.xml",
    "الشرق اقتصاد": "https://economy.asharq.com/rss/",
    "Investing": "https://sa.investing.com/rss/news.rss",
    "واس": "https://www.spa.gov.sa/rss.xml",
    "BBC Arabic": "https://feeds.bbci.co.uk/arabic/rss.xml",
    "DW Arabic": "https://rss.dw.com/xml/rss-ar-all",
    "France24 Arabic": "https://www.france24.com/ar/rss",
    "EIA": "https://www.eia.gov/rss/todayinenergy.xml",
}


# ============================================================
# المناطق العالمية
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
        "هونغ كونغ",
        "كازاخستان",
        "أوزبكستان",
        "آسيا",
    ],

    "الشرق الأوسط": [
        "السعودية",
        "الإمارات",
        "قطر",
        "الكويت",
        "البحرين",
        "عمان",
        "العراق",
        "الأردن",
        "لبنان",
        "سوريا",
        "اليمن",
        "مصر",
        "إيران",
        "تركيا",
        "إسرائيل",
        "فلسطين",
        "الشرق الأوسط",
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
        "السنغال",
        "الصومال",
        "أفريقيا",
    ],

    "أوروبا": [
        "بريطانيا",
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
        "اليونان",
        "النرويج",
        "السويد",
        "الدنمارك",
        "فنلندا",
        "أوكرانيا",
        "روسيا",
        "أيرلندا",
        "رومانيا",
        "التشيك",
        "الاتحاد الأوروبي",
        "أوروبا",
    ],

    "أستراليا والمحيط الهادئ": [
        "أستراليا",
        "نيوزيلندا",
        "بابوا غينيا الجديدة",
        "فيجي",
        "المحيط الهادئ",
    ],

    "أمريكا الشمالية": [
        "الولايات المتحدة",
        "أمريكا",
        "كندا",
        "المكسيك",
        "بنما",
        "كوبا",
        "جامايكا",
        "الكاريبي",
        "أمريكا الشمالية",
    ],

    "أمريكا الجنوبية": [
        "البرازيل",
        "الأرجنتين",
        "تشيلي",
        "كولومبيا",
        "بيرو",
        "بوليفيا",
        "الإكوادور",
        "أوروغواي",
        "باراغواي",
        "فنزويلا",
        "أمريكا الجنوبية",
    ],
}


# ============================================================
# الاستعلامات العامة
# ============================================================

GLOBAL_QUERIES: List[Tuple[str, str]] = [
    (
        'وزارة الخارجية OR "وزارة الخارجية" OR '
        '"Ministry of Foreign Affairs" OR "Foreign Ministry"',
        "official",
    ),
    (
        'حكومة OR "بيان رسمي" OR "تصريح رسمي" OR '
        '"Government statement" OR "official statement"',
        "official",
    ),
    (
        'أسهم OR بورصة OR تداول OR "داو جونز" OR '
        '"ناسداك" OR "S&P 500" OR أسواق',
        "economy",
    ),
    (
        'بيتكوين OR إيثريوم OR كريبتو OR "عملات رقمية"',
        "crypto",
    ),
    (
        'النفط OR أوبك OR برنت OR غاز OR طاقة',
        "energy",
    ),
    (
        'تضخم OR فائدة OR "بنك مركزي" OR اقتصاد OR عملات',
        "economy",
    ),
    (
        'عاجل OR "بيان عاجل" OR "خبر عاجل"',
        "urgent",
    ),
]


# ============================================================
# نموذج الخبر
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
        self.url = str(url or "").strip()
        self.published_at = self.clean_text(published_at)
        self.category = category
        self.summary = self.clean_text(summary)
        self.region = self.clean_text(region)

    @staticmethod
    def clean_text(text: str) -> str:
        if not text:
            return ""

        text = html.unescape(str(text))
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)

        return text.strip()

    def search_text(self) -> str:
        return " ".join(
            [
                self.title,
                self.source,
                self.summary,
                self.region,
            ]
        ).lower()


# ============================================================
# أدوات التطبيع
# ============================================================

def normalize_text(text: str) -> str:
    text = str(text or "").lower()

    # توحيد بعض الحروف العربية
    replacements = {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ى": "ي",
        "ة": "ه",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    text = re.sub(
        r"[^\w\s\u0600-\u06FF-]",
        " ",
        text,
    )

    return re.sub(
        r"\s+",
        " ",
        text,
    ).strip()


def title_key(title: str) -> str:
    return normalize_text(title)[:180]


def deduplicate(items: List[NewsItem]) -> List[NewsItem]:
    output = []
    seen = set()

    for item in items:
        key = title_key(item.title)

        if not key or key in seen:
            continue

        seen.add(key)
        output.append(item)

    return output


# ============================================================
# جلب RSS
# ============================================================

async def fetch_rss_feed(
    session: aiohttp.ClientSession,
    source_name: str,
    url: str,
) -> List[NewsItem]:

    items = []

    try:
        timeout = aiohttp.ClientTimeout(
            total=FETCH_TIMEOUT
        )

        async with session.get(
            url,
            timeout=timeout,
            allow_redirects=True,
        ) as response:

            if response.status != 200:
                log.warning(
                    "RSS %s -> HTTP %s",
                    source_name,
                    response.status,
                )
                return items

            content = await response.text(
                errors="ignore"
            )

            parsed = feedparser.parse(content)

            for entry in parsed.entries[
                :MAX_PER_FEED
            ]:

                title = entry.get(
                    "title",
                    "",
                )

                link = entry.get(
                    "link",
                    "",
                )

                if not title or not link:
                    continue

                published = (
                    entry.get(
                        "published",
                        "",
                    )
                    or entry.get(
                        "updated",
                        "",
                    )
                    or ""
                )

                summary = (
                    entry.get(
                        "summary",
                        "",
                    )
                    or entry.get(
                        "description",
                        "",
                    )
                    or ""
                )

                items.append(
                    NewsItem(
                        title=title,
                        source=source_name,
                        url=link,
                        published_at=published,
                        summary=summary,
                    )
                )

    except Exception as exc:
        log.warning(
            "RSS error [%s]: %s",
            source_name,
            exc,
        )

    return items


# ============================================================
# Google News RSS
# ============================================================

async def fetch_google_news_topic(
    session: aiohttp.ClientSession,
    query: str,
    category: str = "general",
    region: str = "",
    limit: int = 20,
) -> List[NewsItem]:

    items = []

    encoded = urllib.parse.quote_plus(
        query
    )

    url = GOOGLE_NEWS_BASE.format(
        query=encoded
    )

    try:
        timeout = aiohttp.ClientTimeout(
            total=FETCH_TIMEOUT
        )

        async with session.get(
            url,
            timeout=timeout,
            allow_redirects=True,
        ) as response:

            if response.status != 200:
                log.warning(
                    "Google News -> HTTP %s",
                    response.status,
                )
                return items

            content = await response.text(
                errors="ignore"
            )

            parsed = feedparser.parse(content)

            for entry in parsed.entries[
                :limit
            ]:

                title = entry.get(
                    "title",
                    "",
                )

                link = entry.get(
                    "link",
                    "",
                )

                if not title or not link:
                    continue

                source_data = entry.get(
                    "source",
                    {},
                )

                source = ""

                if hasattr(
                    source_data,
                    "get",
                ):
                    source = (
                        source_data.get(
                            "title",
                            "",
                        )
                        or ""
                    )

                published = (
                    entry.get(
                        "published",
                        "",
                    )
                    or entry.get(
                        "updated",
                        "",
                    )
                    or ""
                )

                if " - " in title:
                    title = title.rsplit(
                        " - ",
                        1,
                    )[0]

                items.append(
                    NewsItem(
                        title=title,
                        source=source or "Google News",
                        url=link,
                        published_at=published,
                        category=category,
                        region=region,
                    )
                )

    except Exception as exc:
        log.warning(
            "Google News error [%s]: %s",
            query,
            exc,
        )

    return items


# ============================================================
# التجميع العام
# ============================================================

async def collect_news(
    max_items: int = 100,
) -> List[NewsItem]:

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "Chrome/120 Safari/537.36"
        )
    }

    tasks = []

    async with aiohttp.ClientSession(
        headers=headers
    ) as session:

        # المصادر المباشرة
        for source_name, url in TRUSTED_FEEDS.items():
            tasks.append(
                fetch_rss_feed(
                    session,
                    source_name,
                    url,
                )
            )

        # الاستعلامات العامة
        for query, category in GLOBAL_QUERIES:
            tasks.append(
                fetch_google_news_topic(
                    session,
                    query,
                    category,
                )
            )

        # التغطية الإقليمية
        for region, countries in REGIONS.items():

            query = " OR ".join(
                f'"{country}"'
                for country in countries
            )

            tasks.append(
                fetch_google_news_topic(
                    session,
                    query,
                    "regional",
                    region,
                    limit=20,
                )
            )

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

    all_news = []

    for result in results:
        if isinstance(result, list):
            all_news.extend(result)

    all_news = deduplicate(
        all_news
    )

    log.info(
        "Collected %s unique global news items.",
        len(all_news),
    )

    return all_news[:max_items]


# ============================================================
# البحث داخل الأخبار الموجودة
# ============================================================

def search_news(
    items: List[NewsItem],
    keywords: List[str],
    max_results: int = 25,
) -> List[NewsItem]:

    if not items or not keywords:
        return []

    query_words = [
        normalize_text(word)
        for word in keywords
        if normalize_text(word)
    ]

    if not query_words:
        return []

    scored = []

    for item in items:

        title = normalize_text(
            item.title
        )

        body = normalize_text(
            f"{item.summary} "
            f"{item.source} "
            f"{item.region}"
        )

        score = 0

        for word in query_words:

            if word in title:
                score += 5

            elif word in body:
                score += 2

        if score > 0:
            scored.append(
                (
                    score,
                    item,
                )
            )

    scored.sort(
        key=lambda value: value[0],
        reverse=True,
    )

    return [
        item
        for _, item in scored[
            :max_results
        ]
    ]


# ============================================================
# البحث العالمي المستقل
# ============================================================

async def search_news_online(
    query: str,
    max_results: int = 25,
) -> List[NewsItem]:

    query = str(query or "").strip()

    if not query:
        return []

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "Chrome/120 Safari/537.36"
        )
    }

    queries = [
        query,
        f'"{query}"',
    ]

    # إذا كان البحث متعدد الكلمات،
    # نضيف نسخة OR حتى لا يكون البحث ضيقًا جدًا.
    words = [
        word
        for word in re.split(
            r"\s+",
            query,
        )
        if len(word) >= 2
    ]

    if len(words) > 1:
        queries.append(
            " OR ".join(
                f'"{word}"'
                for word in words
            )
        )

    async with aiohttp.ClientSession(
        headers=headers
    ) as session:

        tasks = [
            fetch_google_news_topic(
                session,
                q,
                "search",
                "",
                limit=max_results,
            )
            for q in queries
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

    combined = []

    for result in results:

        if isinstance(result, list):
            combined.extend(result)

    combined = deduplicate(
        combined
    )

    # ترتيب النتائج حسب مطابقة كلمات البحث
    return search_news(
        combined,
        words or [query],
        max_results=max_results,
    )


# ============================================================
# سياق Gemini
# ============================================================

def build_ai_context(
    items: List[NewsItem],
    limit: int = 8,
) -> str:

    lines = []

    for index, item in enumerate(
        items[:limit],
        start=1,
    ):

        lines.append(
            f"{index}. "
            f"العنوان: {item.title}\n"
            f"المصدر: {item.source}\n"
            f"التاريخ: "
            f"{item.published_at or 'غير متوفر'}\n"
            f"المنطقة: "
            f"{item.region or 'عالمية'}\n"
            f"الرابط: {item.url}"
        )

    return "\n\n".join(lines)
