import asyncio
import html
import logging
import re
import urllib.parse
from typing import List, Dict, Tuple

import aiohttp
import feedparser


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("news_engine")


FETCH_TIMEOUT = 8
MAX_PER_FEED = 20


# ============================================================
# مصادر الأخبار المباشرة
# ============================================================

TRUSTED_FEEDS: Dict[str, str] = {
    # العربية والشرق الأوسط
    "العربية": "https://www.alarabiya.net/.well-known/rss/urgent.xml",
    "الجزيرة": "https://www.aljazeera.net/aljazeerarss/a7c1866f-6829-4883-8441-358d731800bc/43316f44-8e12-4320-b4c2-a22f6654b321",
    "سكاي نيوز عربية": "https://www.skynewsarabia.com/rss/v1/news.xml",
    "الشرق": "https://asharq.com/rss/",
    "CNBC عربية": "https://www.cnbcarabia.com/rss.xml",
    "الشرق اقتصاد": "https://economy.asharq.com/rss/",
    "الاستثمار": "https://sa.investing.com/rss/news.rss",
    "واس": "https://www.spa.gov.sa/rss.xml",

    # دولية
    "BBC Arabic": "https://feeds.bbci.co.uk/arabic/rss.xml",
    "DW Arabic": "https://rss.dw.com/xml/rss-ar-all",
    "France24 Arabic": "https://www.france24.com/ar/rss",
    "EIA": "https://www.eia.gov/rss/todayinenergy.xml",
}


# ============================================================
# مناطق العالم
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
        "آسيا الوسطى",
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
        "الاتحاد الأوروبي",
    ],

    "أستراليا والمحيط الهادئ": [
        "أستراليا",
        "نيوزيلندا",
        "بابوا غينيا الجديدة",
        "المحيط الهادئ",
    ],

    "أمريكا الشمالية": [
        "الولايات المتحدة",
        "أمريكا",
        "كندا",
        "المكسيك",
        "بنما",
        "كوبا",
        "الكاريبي",
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
    ],
}


# ============================================================
# تصنيفات اقتصادية ورسمية
# ============================================================

GLOBAL_QUERIES: List[Tuple[str, str]] = [
    (
        'وزارة الخارجية OR "وزارة الخارجية" OR '
        '"Ministry of Foreign Affairs" OR "Foreign Ministry"',
        "official",
    ),
    (
        'حكومة OR حكومة رسمية OR رئاسة الوزراء OR '
        '"Government statement" OR "official statement"',
        "official",
    ),
    (
        'أسواق OR أسهم OR بورصة OR تداول OR '
        '"داو جونز" OR "ناسداك" OR "S&P 500"',
        "markets",
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
        'تضخم OR فائدة OR بنك مركزي OR اقتصاد OR عملات',
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
        self.url = url.strip() if url else ""
        self.published_at = self.clean_text(published_at)
        self.category = category
        self.summary = self.clean_text(summary)
        self.region = region

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

    def __repr__(self):
        return (
            f"<NewsItem "
            f"title='{self.title[:40]}...' "
            f"source='{self.source}'>"
        )


# ============================================================
# RSS
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
            timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
            allow_redirects=True,
        ) as response:

            if response.status != 200:
                log.warning(
                    "RSS [%s] returned HTTP %s",
                    source_name,
                    response.status,
                )
                return items

            content = await response.text(errors="ignore")
            parsed = feedparser.parse(content)

            for entry in parsed.entries[:MAX_PER_FEED]:

                title = entry.get("title", "")
                link = entry.get("link", "")

                if not title or not link:
                    continue

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
            "Error fetching RSS [%s]: %s",
            source_name,
            exc,
        )

    return items


# ============================================================
# Google News
# ============================================================

GOOGLE_NEWS_BASE = (
    "https://news.google.com/rss/search?"
    "q={query}&hl=ar&gl=SA&ceid=SA:ar"
)


async def fetch_google_news_topic(
    session: aiohttp.ClientSession,
    query: str,
    category: str,
    region: str = "",
) -> List[NewsItem]:

    encoded_query = urllib.parse.quote(query)
    target_url = GOOGLE_NEWS_BASE.format(query=encoded_query)

    items: List[NewsItem] = []

    try:
        async with session.get(
            target_url,
            timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
            allow_redirects=True,
        ) as response:

            if response.status != 200:
                return items

            content = await response.text(errors="ignore")
            parsed = feedparser.parse(content)

            for entry in parsed.entries[:MAX_PER_FEED]:

                title = entry.get("title", "")
                link = entry.get("link", "")

                source_obj = entry.get("source", {})
                source = (
                    source_obj.get("title", "")
                    if hasattr(source_obj, "get")
                    else ""
                )

                published = (
                    entry.get("published", "")
                    or entry.get("updated", "")
                    or ""
                )

                if " - " in title:
                    title = title.rsplit(" - ", 1)[0]

                if not title or not link:
                    continue

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
# تجميع أخبار العالم
# ============================================================

async def collect_news(max_items: int = 100) -> List[NewsItem]:

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "Chrome/120 Safari/537.36"
        )
    }

    all_news: List[NewsItem] = []
    seen_keys = set()

    async with aiohttp.ClientSession(
        headers=headers
    ) as session:

        tasks = []

        # المصادر المباشرة
        for source_name, url in TRUSTED_FEEDS.items():
            tasks.append(
                fetch_rss_feed(
                    session,
                    source_name,
                    url,
                )
            )

        # الأخبار العالمية
        for query, category in GLOBAL_QUERIES:
            tasks.append(
                fetch_google_news_topic(
                    session,
                    query,
                    category,
                )
            )

        # بحث إقليمي
        for region, countries in REGIONS.items():

            region_query = " OR ".join(
                f'"{country}"'
                for country in countries[:12]
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

            if not isinstance(result, list):
                continue

            for item in result:

                if not item.title:
                    continue

                # إزالة التكرار بطريقة أفضل من أول 30 حرف
                normalized = re.sub(
                    r"\W+",
                    " ",
                    item.title.lower(),
                ).strip()

                key = normalized[:160]

                if key in seen_keys:
                    continue

                seen_keys.add(key)
                all_news.append(item)

    log.info(
        "Collected %s unique global news items.",
        len(all_news),
    )

    return all_news[:max_items]


# ============================================================
# البحث
# ============================================================

def search_news(
    items: List[NewsItem],
    keywords: List[str],
    max_results: int = 25,
) -> List[NewsItem]:

    if not items or not keywords:
        return []

    normalized_keywords = [
        str(keyword).strip().lower()
        for keyword in keywords
        if str(keyword).strip()
    ]

    if not normalized_keywords:
        return []

    scored = []

    for item in items:

        text = item.search_text()

        score = 0

        for keyword in normalized_keywords:

            if keyword in text:
                score += 1

        if score:
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
        for _, item in scored[:max_results]
    ]


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
            f"التاريخ: {item.published_at or 'غير متوفر'}\n"
            f"المنطقة: {item.region or 'عالمية'}\n"
            f"الرابط: {item.url}"
        )

    return "\n\n".join(lines)
