import asyncio
import logging
import os
import re
from typing import Dict

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

from google import genai
from google.genai import types

from news_engine import (
    collect_news,
    search_news,
    build_ai_context,
)


# ============================================================
# SETTINGS
# ============================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)

log = logging.getLogger("news_bot")

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()

GEMINI_MODEL = "gemini-3.5-flash"

NEWS_COLLECTION_TIMEOUT = 20
GEMINI_TIMEOUT = 45
MAX_SEARCH_RESULTS = 15


if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")

if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY is missing")


# ============================================================
# GEMINI
# ============================================================

ai_client = genai.Client(
    api_key=GEMINI_API_KEY
)


# ============================================================
# USER LOCKS
# ============================================================

USER_LOCKS: Dict[int, asyncio.Lock] = {}


# ============================================================
# TOPICS
# ============================================================

TOPICS = {
    "urgent": (
        "🚨 عاجل",
        "عاجل آخر الأخبار والتطورات",
    ),
    "world": (
        "🌍 العالم",
        "أهم أخبار العالم",
    ),
    "gulf": (
        "🇸🇦 الخليج",
        "أخبار الخليج",
    ),
    "america": (
        "🇺🇸 أمريكا",
        "أخبار الولايات المتحدة",
    ),
    "europe": (
        "🇪🇺 أوروبا",
        "أخبار أوروبا",
    ),
    "asia": (
        "🌏 آسيا",
        "أخبار آسيا",
    ),
    "energy": (
        "⚡ الطاقة",
        "أخبار النفط والطاقة",
    ),
    "security": (
        "🛡 الأمن",
        "أخبار الأمن والدفاع",
    ),
    "foreign": (
        "🌐 السياسة",
        "السياسة والعلاقات الدولية",
    ),
}


def main_keyboard():
    rows = []
    items = list(TOPICS.items())

    for i in range(0, len(items), 2):
        rows.append([
            InlineKeyboardButton(
                label,
                callback_data=f"topic:{key}",
            )
            for key, (label, _) in items[i:i + 2]
        ])

    return InlineKeyboardMarkup(rows)


# ============================================================
# GREETING DETECTION
# ============================================================

GREETING_RE = re.compile(
    r"^\s*("
    r"السلام عليكم|السلام عليكم ورحمة الله وبركاته|"
    r"وعليكم السلام|مرحبا|مرحباً|هلا|هلا والله|"
    r"اهلا|أهلا|أهلين|اهلين|"
    r"صباح الخير|مساء الخير|"
    r"شكرا|شكراً|مشكور|مشكورة|"
    r"يعطيك العافية|يعطيك العافيه|"
    r"تمام|ممتاز|حلو|"
    r"كيفك|كيف حالك|"
    r"hello|hi|hey|thanks|thank you"
    r")\s*[!.،,؟?]*\s*$",
    re.IGNORECASE,
)


def is_greeting(text: str) -> bool:
    return bool(
        GREETING_RE.match(
            text.strip()
        )
    )


# ============================================================
# NEWS QUERY DETECTION
# ============================================================

NEWS_TERMS = {
    "خبر",
    "أخبار",
    "اخبار",
    "عاجل",
    "آخر الأخبار",
    "اخر الاخبار",
    "اليوم",
    "الآن",
    "الان",
    "حدث",
    "أحداث",
    "احداث",
    "الوضع",
    "تطورات",
    "مستجدات",
    "سياسة",
    "سياسي",
    "حرب",
    "هجوم",
    "ضربة",
    "قصف",
    "صراع",
    "أزمة",
    "ازمة",
    "روسيا",
    "أوكرانيا",
    "امريكا",
    "أمريكا",
    "الصين",
    "آسيا",
    "إيران",
    "ايران",
    "إسرائيل",
    "اسرائيل",
    "غزة",
    "السعودية",
    "الخليج",
    "الإمارات",
    "الامارات",
    "قطر",
    "الكويت",
    "البحرين",
    "عمان",
    "تركيا",
    "بريطانيا",
    "أوروبا",
    "اوروبا",
    "النفط",
    "الطاقة",
    "أوبك",
    "اوبك",
    "اقتصاد",
    "اقتصادية",
    "دولار",
    "أسواق",
    "اسواق",
    "أمن",
    "امن",
    "دفاع",
    "عسكري",
    "عسكرية",
    "مفاوضات",
    "اجتماع",
    "رئيس",
    "وزير",
    "وزارة",
    "تصريح",
    "بيان",
    "انتخابات",
    "برلمان",
    "news",
    "breaking",
    "latest",
    "world",
    "russia",
    "ukraine",
    "usa",
    "america",
    "china",
    "iran",
    "israel",
    "gulf",
    "oil",
    "energy",
}


def is_news_query(text: str) -> bool:
    t = text.strip().lower()

    if any(
        term.lower() in t
        for term in NEWS_TERMS
    ):
        return True

    patterns = (
        r"\bما الجديد\b",
        r"\bماذا يحدث\b",
        r"\bماذا حدث\b",
        r"\bما الذي يحدث\b",
        r"\bآخر المستجدات\b",
        r"\bاخر المستجدات\b",
        r"\bآخر التطورات\b",
        r"\bاخر التطورات\b",
        r"\bهل هناك\b",
        r"\bهل حدث\b",
        r"\bهل يوجد\b",
        r"\bwhat happened\b",
        r"\bwhat's happening\b",
        r"\bwhat is happening\b",
        r"\blatest news\b",
        r"\bbreaking news\b",
    )

    return any(
        re.search(
            pattern,
            t,
            re.I,
        )
        for pattern in patterns
    )


# ============================================================
# GEMINI REQUEST
# ============================================================

async def ask_gemini(prompt: str) -> str:
    try:
        response = await asyncio.wait_for(
            asyncio.to_thread(
                ai_client.models.generate_content,
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(
                        thinking_level="low"
                    )
                ),
            ),
            timeout=GEMINI_TIMEOUT,
        )

        text = (
            getattr(
                response,
                "text",
                None,
            )
            or ""
        ).strip()

        return text or "تعذر إنشاء التحليل حالياً."

    except asyncio.TimeoutError:
        log.warning("Gemini timeout")
        return ""

    except Exception:
        log.exception("Gemini error")
        return ""


# ============================================================
# NEWS COLLECTION
# ============================================================

async def get_fresh_news():
    """
    collect_news() في news_engine.py هي async.
    لذلك يجب استدعاؤها مباشرة باستخدام await.
    """

    try:
        return await asyncio.wait_for(
            collect_news(
                max_items=100
            ),
            timeout=NEWS_COLLECTION_TIMEOUT,
        )

    except asyncio.TimeoutError:
        log.warning(
            "News collection timeout"
        )
        return []

    except Exception:
        log.exception(
            "News collection error"
        )
        return []


# ============================================================
# USER NEWS SEARCH
# ============================================================

async def search_user_news(
    text: str,
):
    items = await get_fresh_news()

    if not items:
        return []

    try:
        return search_news(
            items,
            text,
            max_results=MAX_SEARCH_RESULTS,
        )

    except Exception:
        log.exception(
            "News search error"
        )
        return []


# ============================================================
# AI REPORT
# ============================================================

REPORT_PROMPT = """
أنت محلل أخبار دقيق ومختصر.

مهمتك تحليل الأخبار التي يرسلها محرك الأخبار فقط.

القواعد الصارمة:

1. لا تخترع أي معلومة غير موجودة في البيانات.
2. لا تعتبر الاستنتاج حقيقة.
3. لا تكرر الخبر نفسه.
4. لا تضع حشواً أو مقدمات طويلة.
5. إذا كانت الأخبار غير كافية للإجابة، قل بوضوح إن المعلومات المتاحة غير كافية.
6. إذا لم توجد أخبار مرتبطة بالسؤال، لا تستخدم أخباراً عشوائية.
7. أعط الأولوية للأحدث والأكثر أهمية والمصادر الأكثر موثوقية.
8. عند وجود اختلاف بين المصادر، اذكر الاختلاف باختصار.
9. استخدم العربية الواضحة والمباشرة.
10. لا تكتب تحليلاً مطولاً إلا إذا كان السؤال يحتاج ذلك.

صيغة الإجابة:

🚨 العنوان أو الخلاصة

• أهم نقطة
• أهم نقطة
• أهم نقطة

📌 المصادر:
- اسم المصدر: الرابط

لا تذكر إلا المعلومات المرتبطة بالسؤال.
"""


async def generate_report(
    question: str,
    items,
) -> str:

    if not items:
        return (
            "🔎 لم أجد أخباراً مرتبطة مباشرة "
            "بسؤالك ضمن المصادر المتاحة حالياً."
        )

    context = build_ai_context(
        items
    )

    prompt = f"""
{REPORT_PROMPT}

السؤال:
{question}

الأخبار المتاحة:
{context}
"""

    result = await ask_gemini(
        prompt
    )

    if result:
        return result

    lines = [
        "📰 أبرز الأخبار المرتبطة بسؤالك:\n"
    ]

    for item in items[:8]:

        title = (
            getattr(
                item,
                "title",
                "",
            )
            or ""
        )

        source = (
            getattr(
                item,
                "source",
                "",
            )
            or ""
        )

        url = (
            getattr(
                item,
                "url",
                "",
            )
            or ""
        )

        if title:
            lines.append(
                f"• {title}"
            )

            if source:
                lines.append(
                    f"  المصدر: {source}"
                )

            if url:
                lines.append(
                    f"  {url}"
                )

    return "\n".join(
        lines
    )


# ============================================================
# LONG TELEGRAM MESSAGE
# ============================================================

async def send_long_message(
    update: Update,
    text: str,
):

    if not text:
        return

    for i in range(
        0,
        len(text),
        3900,
    ):
        await update.message.reply_text(
            text[i:i + 3900]
        )


# ============================================================
# /START
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    await update.message.reply_text(
        "📰 مرحباً بك في بوت الأخبار.\n\n"
        "اختر مجالاً من الأزرار أو اكتب سؤالك مباشرة.",
        reply_markup=main_keyboard(),
    )


# ============================================================
# BUTTON HANDLER
# ============================================================

async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query

    await query.answer()

    key = query.data.split(
        ":",
        1,
    )[1]

    if key not in TOPICS:
        return

    _, topic_text = TOPICS[key]

    user_id = (
        update.effective_user.id
    )

    lock = USER_LOCKS.setdefault(
        user_id,
        asyncio.Lock(),
    )

    if lock.locked():
        await query.message.reply_text(
            "⏳ يوجد طلب قيد المعالجة. "
            "انتظر اكتماله."
        )
        return

    async with lock:

        status = await query.message.reply_text(
            "📡 جاري البحث عن الأخبار..."
        )

        try:

            # FIX:
            # collect_news() async
            items = await get_fresh_news()

            if not items:
                await status.edit_text(
                    "⚠️ تعذر الوصول إلى "
                    "مصادر الأخبار حالياً."
                )
                return

            results = search_news(
                items,
                topic_text,
                max_results=MAX_SEARCH_RESULTS,
            )

            if not results:
                await status.edit_text(
                    "🔎 لا توجد أخبار مرتبطة "
                    "بهذا المجال حالياً."
                )
                return

            report = await generate_report(
                topic_text,
                results,
            )

            await status.delete()

            await query.message.reply_text(
                report
            )

        except Exception:

            log.exception(
                "Button handler error"
            )

            await status.edit_text(
                "⚠️ حدث خطأ مؤقت أثناء "
                "معالجة الطلب."
            )


# ============================================================
# USER MESSAGE HANDLER
# ============================================================

async def handle_user_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    text = (
        update.message.text
        or ""
    ).strip()

    if not text:
        return

    # --------------------------------------------------------
    # Greeting
    # --------------------------------------------------------

    if is_greeting(text):

        if "السلام" in text:
            message = (
                "وعليكم السلام، أهلاً بك.\n"
                "اكتب سؤالك الإخباري أو اختر أحد الأقسام."
            )
        else:
            message = (
                "أهلاً بك.\n"
                "اكتب سؤالك الإخباري أو اختر أحد الأقسام."
            )

        await update.message.reply_text(
            message,
            reply_markup=main_keyboard(),
        )

        return

    # --------------------------------------------------------
    # Non-news message
    # --------------------------------------------------------

    if not is_news_query(text):

        await update.message.reply_text(
            "أنا مخصص للأخبار والتحليل الإخباري.\n"
            "اكتب مثلاً:\n\n"
            "• آخر أخبار روسيا؟\n"
            "• ماذا يحدث في آسيا؟\n"
            "• أهم أخبار الخليج اليوم؟\n"
            "• هل هناك تطورات جديدة في إيران؟",
            reply_markup=main_keyboard(),
        )

        return

    # --------------------------------------------------------
    # User lock
    # --------------------------------------------------------

    user_id = (
        update.effective_user.id
    )

    lock = USER_LOCKS.setdefault(
        user_id,
        asyncio.Lock(),
    )

    if lock.locked():

        await update.message.reply_text(
            "⏳ يوجد طلب قيد المعالجة. "
            "انتظر اكتماله."
        )

        return

    async with lock:

        status = await update.message.reply_text(
            "📡 جاري البحث عن أخبار مرتبطة بسؤالك..."
        )

        try:

            results = await search_user_news(
                text
            )

            if not results:

                await status.edit_text(
                    "🔎 لم أجد أخباراً مرتبطة "
                    "مباشرة بسؤالك ضمن المصادر "
                    "المتاحة حالياً."
                )

                return

            report = await generate_report(
                text,
                results,
            )

            await status.delete()

            await send_long_message(
                update,
                report,
            )

        except Exception:

            log.exception(
                "User message handler error"
            )

            await status.edit_text(
                "⚠️ حدث خطأ مؤقت أثناء "
                "معالجة الطلب."
            )


# ============================================================
# POST INIT
# ============================================================

async def post_init(
    application,
):

    me = await application.bot.get_me()

    log.info(
        "Telegram connected: @%s",
        me.username,
    )


# ============================================================
# MAIN
# ============================================================

def main():

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            button_handler,
            pattern=r"^topic:",
        )
    )

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_user_message,
        )
    )

    log.info(
        "Bot starting..."
    )

    app.run_polling(
        drop_pending_updates=True
    )


if __name__ == "__main__":
    main()
