import asyncio
import logging
import re
import urllib.parse
from datetime import datetime
from typing import List, Dict, Any, Optional
import aiohttp
import feedparser

# ============================================================
# LOGGING & CONFIGURATION
# ============================================================

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("news_engine")

# وقت الانتظار الأقصى لكل جلب (بالثواني)
FETCH_TIMEOUT = 8

# ============================================================
# RELIABLE SOURCES & RSS FEEDS (المصادر الموثوقة والقنوات)
# ============================================================

TRUSTED_FEEDS = {
    # 📺 القنوات الإخبارية والتلفزيونية (للعاجل والتغطيات الحية)
    "alarabiya_urgent": "https://www.alarabiya.net/.well-known/rss/urgent.xml",
    "aljazeera_urgent": "https://www.aljazeera.net/aljazeerarss/a7c1866f-6829-4883-8441-358d731800bc/43316f44-8e12-4320-b4c2-a22f6654b321",
    "skynews_breaking": "https://www.skynewsarabia.com/rss/v1/news.xml",
    "asharq_news": "https://asharq.com/rss/",
    "cnbc_arabia": "https://www.cnbcarabia.com/rss.xml",

    # 📈 أسواق المال، الاقتصاد، والعملات الرقمية
    "bloomberg_asharq": "https://economy.asharq.com/rss/",
    "investing_arabic": "https://sa.investing.com/rss/news.rss",
    
    # 🏛 وكالات الأنباء الرسمية
    "spa_official": "https://www.spa.gov.sa/rss.xml",
}

# محركات Google News المخصصة للتخصصات الدقيقة
GOOGLE_NEWS_BASE = "https://news.google.com/rss/search?q={query}&hl=ar&gl=SA&ceid=SA:ar"


# ============================================================
# DATA MODEL (هيكلة البيانات الموحدة)
# ============================================================

class NewsItem:
    def __init__(self, title: str, source: str, url: str, published_at: str = "", category: str = "general"):
        self.title = self.clean_text(title)
        self.source = source
        self.url = url
        self.published_at = published_at
        self.category = category

    @staticmethod
    def clean_text(text: str) -> str:
        """تنظيف النصوص من أوسمة HTML والتنسيقات الزائدة"""
        if not text:
            return ""
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def __repr__(self):
        return f"<NewsItem title='{self.title[:30]}...' source='{self.source}'>"


# ============================================================
# ASYNC FETCHERS (محركات الجلب المتوازية السريعة)
# ============================================================

async def fetch_rss_feed(session: aiohttp.ClientSession, source_name: str, url: str) -> List[NewsItem]:
    """جلب وتحليل خلاصات RSS المباشرة بسرعة فائقة"""
    items = []
    try:
        async with session.get(url, timeout=FETCH_TIMEOUT) as response:
            if response.status == 200:
                content = await response.text()
                parsed = feedparser.parse(content)
                for entry in parsed.entries[:15]:
                    title = entry.get("title", "")
                    link = entry.get("link", "")
                    pub_date = entry.get("published", "") or entry.get("updated", "")
                    
                    if title and link:
                        items.append(NewsItem(
                            title=title,
                            source=source_name,
                            url=link,
                            published_at=pub_date
                        ))
    except Exception as e:
        log.warning(f"Error fetching RSS [{source_name}]: {e}")
    return items


async def fetch_google_news_topic(session: aiohttp.ClientSession, query: str, category: str) -> List[NewsItem]:
    """جلب مستهدف من Google News يضمن أحدث التغطيات في الأسواق والتصريحات"""
    encoded_query = urllib.parse.quote(query)
    target_url = GOOGLE_NEWS_BASE.format(query=encoded_query)
    items = []
    
    try:
        async with session.get(target_url, timeout=FETCH_TIMEOUT) as response:
            if response.status == 200:
                content = await response.text()
                parsed = feedparser.parse(content)
                for entry in parsed.entries[:20]:
                    title = entry.get("title", "")
                    link = entry.get("link", "")
                    source = entry.get("source", {}).get("title", "تغطية إخبارية")
                    
                    # تنظيف عنوان الخبر من اسم المصدر المكرر في Google News
                    if " - " in title:
                        title = title.rsplit(" - ", 1)[0]

                    if title and link:
                        items.append(NewsItem(
                            title=title,
                            source=source,
                            url=link,
                            category=category
                        ))
    except Exception as e:
        log.warning(f"Error fetching Google News for [{query}]: {e}")
    return items


# ============================================================
# CORE ENGINE API (الواجهة الرئيسية للتجميع والفرز)
# ============================================================

async def collect_news(max_items: int = 100) -> List[NewsItem]:
    """تجميع الأخبار المباشرة من كافة القنوات والمصادر مع منع التكرار"""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    all_news: List[NewsItem] = []
    seen_titles = set()

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = []

        # 1. جلب خلاصات RSS للقنوات والمصادر الموثوقة
        for src_name, url in TRUSTED_FEEDS.items():
            tasks.append(fetch_rss_feed(session, src_name, url))

        # 2. جلب مخصص ومباشر لأسواق المال والعملات والوزارات
        custom_queries = [
            ("أسهم OR بورصة OR "داو جونز" OR "ناسداك" OR "نيكاي" OR "تداول" OR "الأسواق الأمريكية" OR "الأسواق الأوروبية" OR "الأسواق الآسيوية"", "markets"),
            ("بيتكوين OR "عملات رقمية" OR "كريبتو" OR "إيثريوم"", "crypto"),
            ("وزارة OR وزير OR "تصريح رسمي" OR "بيان صحفي" OR "مصدر مسؤول"", "official_statements"),
            ("النفط OR أوبك OR برنت OR "وزارة الطاقة"", "energy")
        ]

        for query, cat in custom_queries:
            tasks.append(fetch_google_news_topic(session, query, cat))

        # تنفيذ كافة مهام الجلب بالتوازي
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, list):
                for item in res:
                    # إزالة التكرار بناءً على تشابه أول 30 حرفاً من العنوان
                    normalized_title = item.title[:30].strip().lower()
                    if normalized_title not in seen_titles:
                        seen_titles.add(normalized_title)
                        all_news.append(item)

    log.info(f"Successfully collected {len(all_news)} unique news items.")
    return all_news[:max_items]


def search_news(items: List[NewsItem], keywords: List[str], max_results: int = 25) -> List[NewsItem]:
    """دالة البحث والتصفية الصارمة للتخصصات"""
    filtered = []
    for item in items:
        title = item.title.lower()
        if any(kw.lower() in title for kw in keywords):
            filtered.append(item)
    return filtered[:max_results]


def build_ai_context(items: List[NewsItem], limit: int = 6) -> str:
    """تجهيز سياق الأخبار لنماذج الذكاء الاصطناعي مثل Gemini"""
    context_lines = [f"- {item.title} [{item.source}]" for item in items[:limit]]
    return "\n".join(context_lines)
