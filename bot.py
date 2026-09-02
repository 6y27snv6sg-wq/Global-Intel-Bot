import asyncio
import logging
import os
from typing import Dict

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("LiveNewsBot")


# ============================================================
# ENVIRONMENT
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

GEMINI_MODEL = "gemini-3.5-flash"

NEWS_COLLECTION_TIMEOUT = 20
MAX_SEARCH_RESULTS = 15


if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN or TELEGRAM_BOT_TOKEN is missing."
    )

if not GEMINI_API_KEY:
    raise RuntimeError(
        "GEMINI_API_KEY is missing."
    )


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


def get_user_lock(user_id: int) -> asyncio.Lock:

    if user_id not in USER_LOCKS:
        USER_LOCKS[user_id] = asyncio.Lock()

    return USER_LOCKS[user_id]


# ============================================================
# TOPICS
# ============================================================

TOPICS = {
    "urgent": "الأخبار العاجلة والتطورات المهمة الآن",
    "world": "الأخبار العالمية",
    "gulf": "أخبار الخليج العربي",
    "america": "أخبار الولايات المتحدة والأمريكتين",
    "europe": "أخبار أوروبا",
    "asia": "أخبار آسيا",
    "energy": "الطاقة والنفط والغاز والأسواق المرتبطة بها",
    "security": "الأمن والدفاع والصراعات والتطورات العسكرية",
    "foreign": "السياسة الخارجية والعلاقات الدولية",
}


# ============================================================
# KEYBOARDS
# ============================================================

def main_keyboard() -> InlineKeyboardMarkup:

    keyboard = [
        [
            InlineKeyboardButton(
                "🚨 عاجل",
                callback_data="topic:urgent",
            ),
            InlineKeyboardButton(
                "🌍 العالم",
                callback_data="topic:world",
            ),
        ],
        [
            InlineKeyboardButton(
                "🇸🇦 الخليج",
                callback_data="topic:gulf",
            ),
            InlineKeyboardButton(
                "🇺🇸 أمريكا",
                callback_data="topic:america",
            ),
        ],
        [
            InlineKeyboardButton(
                "🇪🇺 أوروبا",
                callback_data="topic:europe",
            ),
            InlineKeyboardButton(
                "🌏 آسيا",
                callback_data="topic:asia",
            ),
        ],
        [
            InlineKeyboardButton(
                "⛽ الطاقة",
                callback_data="topic:energy",
            ),
            InlineKeyboardButton(
                "🛡 الأمن",
                callback_data="topic:security",
            ),
        ],
        [
            InlineKeyboardButton(
                "🌐 السياسة الخارجية",
                callback_data="topic:foreign",
            ),
        ],
        [
            InlineKeyboardButton(
                "🔄 مقارنة",
                callback_data="compare",
            ),
        ],
    ]

    return InlineKeyboardMarkup(
        keyboard
    )


def back_keyboard() -> InlineKeyboardMarkup:

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬅️ رجوع",
                    callback_data="back",
                )
            ]
        ]
    )


# ============================================================
# GEMINI
# ============================================================

async def ask_gemini(
    prompt: str,
) -> str:

    logger.info(
        "Starting Gemini request..."
    )

    try:

        response = await asyncio.to_thread(
            ai_client.models.generate_content,
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(
                    thinking_level="low"
                )
            ),
        )

        text = getattr(
            response,
            "text",
            None,
        )

        if not text:

            logger.warning(
                "Gemini returned an empty response."
            )

            return (
                "لم توجد إجابة كافية من البيانات المتاحة."
            )

        logger.info(
            "Gemini request completed."
        )

        return text.strip()

    except Exception as exc:

        logger.exception(
            "Gemini request failed: %s",
            exc,
        )

        return (
            "حدث خطأ أثناء تحليل الأخبار بواسطة الذكاء الاصطناعي."
        )


# ============================================================
# NEWS COLLECTION
# ============================================================

async def get_fresh_news():

    logger.info(
        "Starting fresh news collection..."
    )

    try:

        news = await asyncio.wait_for(
            collect_news(
                max_items=100
            ),
            timeout=NEWS_COLLECTION_TIMEOUT,
        )

        logger.info(
            "Fresh news collection completed: %d items",
            len(news),
        )

        return news

    except asyncio.TimeoutError:

        logger.error(
            "Fresh news collection timed out after %s seconds.",
            NEWS_COLLECTION_TIMEOUT,
        )

        return []

    except Exception as exc:

        logger.exception(
            "Fresh news collection failed: %s",
            exc,
        )

        return []


# ============================================================
# أسلوب Gemini
# ============================================================

REPORT_STYLE_RULES = """
قواعد الأسلوب:

- اكتب كمحلل أخبار محترف.
- ابدأ بالمعلومة مباشرة.
- كن مختصرًا ودقيقًا.
- لا تستخدم مقدمات إنشائية.
- لا تستخدم عبارات فلسفية أو عامة.
- لا تكرر المعلومة.
- لا تشرح ما هو واضح.
- لا تضف كلامًا لزيادة طول الإجابة.
- لا تختلق معلومات أو مصادر أو أرقامًا.
- لا تحول الاحتمال إلى حقيقة.
- لا تقدم رأيًا شخصيًا.
- لا تتنبأ من عندك.
- التحليل يكون فقط إذا كان مدعومًا بالمعلومات.
- كل نقطة يجب أن تضيف معلومة جديدة.
- إذا لم توجد معلومات كافية، قل ذلك بوضوح.
- لا تستخدم عبارات مثل:
  "في ظل التطورات المتسارعة"
  أو "مما لا شك فيه"
  أو "الأيام القادمة ستكشف".
- لا تختم بعبارات إنشائية.
- التقرير يجب أن يكون قابلًا للقراءة بسرعة.
"""


# ============================================================
# REPORT
# ============================================================

async def generate_report(
    topic: str,
    news_items,
) -> str:

    if not news_items:

        return (
            "لا توجد أخبار حديثة كافية لإعداد التقرير."
        )

    context = build_ai_context(
        news_items
    )

    prompt = f"""
أنت محلل أخبار واستخبارات مفتوحة المصدر.

الموضوع:
{topic}

حلل الأخبار الموجودة في السياق فقط.

{REPORT_STYLE_RULES}

استخدم الهيكل التالي:

🚨 الخلاصة
2 إلى 3 جمل فقط.

📰 الأخبار المهمة
3 إلى 6 نقاط فقط.

🔎 التحليل
2 إلى 4 نقاط فقط.

📌 التأثير المحتمل
نقطتان أو ثلاث كحد أقصى.

⚠️ ملاحظة
اذكر فقط المعلومات غير المؤكدة أو النقص المهم.
إذا لم توجد، اكتب:
لا توجد ملاحظات مهمة.

السياق:
{context}
"""

    return await ask_gemini(
        prompt
    )


# ============================================================
# SEND LONG MESSAGE
# ============================================================

async def send_long_message(
    update: Update,
    text: str,
    reply_markup=None,
):

    if not text:
        text = "لم يتم إنشاء محتوى."

    chunks = [
        text[i:i + 3900]
        for i in range(
            0,
            len(text),
            3900,
        )
    ]

    if not chunks:
        chunks = [
            "لم يتم إنشاء محتوى."
        ]

    for index, chunk in enumerate(
        chunks
    ):

        markup = (
            reply_markup
            if index == len(chunks) - 1
            else None
        )

        if update.callback_query:

            await update.callback_query.message.reply_text(
                chunk,
                reply_markup=markup,
            )

        elif update.message:

            await update.message.reply_text(
                chunk,
                reply_markup=markup,
            )


# ============================================================
# START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not update.message:
        return

    await update.message.reply_text(
        "مرحبًا بك في نظام استخبارات الأخبار.\n\n"
        "اختر المجال الذي تريد تحليله:",
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

    if not query:
        return

    await query.answer()

    user_id = query.from_user.id
    callback_data = query.data

    lock = get_user_lock(
        user_id
    )

    if lock.locked():

        await query.answer(
            "يوجد تحليل جارٍ بالفعل، انتظر قليلًا.",
            show_alert=True,
        )

        return

    async with lock:

        # ----------------------------------------------------
        # BACK
        # ----------------------------------------------------

        if callback_data == "back":

            await query.edit_message_text(
                "اختر المجال الذي تريد تحليله:",
                reply_markup=main_keyboard(),
            )

            return

        # ----------------------------------------------------
        # COMPARE
        # ----------------------------------------------------

        if callback_data == "compare":

            await query.edit_message_text(
                "أرسل موضوعين أو حدثين للمقارنة بينهما.\n\n"
                "مثال:\n"
                "قارن بين تطورات الطاقة في الخليج وأوروبا.",
                reply_markup=back_keyboard(),
            )

            return

        # ----------------------------------------------------
        # TOPIC
        # ----------------------------------------------------

        if not callback_data.startswith(
            "topic:"
        ):
            return

        topic_key = callback_data.split(
            ":",
            1,
        )[1]

        topic = TOPICS.get(
            topic_key
        )

        if not topic:

            await query.edit_message_text(
                "الموضوع غير معروف.",
                reply_markup=back_keyboard(),
            )

            return

        await query.edit_message_text(
            "📡 جاري فحص الأخبار الحديثة ثم تحليلها..."
        )

        logger.info(
            "BUTTON: collecting news for user %s",
            user_id,
        )

        fresh_items = await get_fresh_news()

        logger.info(
            "BUTTON: collection returned %d items",
            len(fresh_items),
        )

        try:

            selected_items = search_news(
                fresh_items,
                topic,
                max_results=MAX_SEARCH_RESULTS,
            )

        except Exception as exc:

            logger.exception(
                "BUTTON: search_news failed: %s",
                exc,
            )

            selected_items = []

        # ----------------------------------------------------
        # زر المجال يمكنه استخدام الأخبار العامة
        # ----------------------------------------------------

        if len(selected_items) < 3:

            selected_items = sorted(
                fresh_items,
                key=lambda item: getattr(
                    item,
                    "importance",
                    0,
                ),
                reverse=True,
            )[:MAX_SEARCH_RESULTS]

        logger.info(
            "BUTTON: selected %d news items",
            len(selected_items),
        )

        if not selected_items:

            await query.edit_message_text(
                "لا توجد أخبار كافية حاليًا.",
                reply_markup=back_keyboard(),
            )

            return

        await query.edit_message_text(
            "🧠 تم جمع الأخبار.\n\n"
            "جاري التحليل المختصر..."
        )

        logger.info(
            "BUTTON: starting Gemini report..."
        )

        report = await generate_report(
            topic,
            selected_items,
        )

        logger.info(
            "BUTTON: Gemini report completed."
        )

        try:

            await query.edit_message_text(
                "📊 تم إعداد التقرير."
            )

        except Exception:
            pass

        await send_long_message(
            update,
            report,
            reply_markup=back_keyboard(),
        )


# ============================================================
# DIRECT QUESTION
# ============================================================

async def handle_user_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not update.message:
        return

    if not update.message.text:
        return

    user_id = update.effective_user.id
    user_text = update.message.text.strip()

    if not user_text:
        return

    lock = get_user_lock(
        user_id
    )

    if lock.locked():

        await update.message.reply_text(
            "يوجد تحليل جارٍ بالفعل، انتظر حتى يكتمل."
        )

        return

    async with lock:

        status_message = await update.message.reply_text(
            "📡 جاري البحث عن أخبار مرتبطة بسؤالك..."
        )

        logger.info(
            "MESSAGE: collecting news for user %s",
            user_id,
        )

        fresh_items = await get_fresh_news()

        logger.info(
            "MESSAGE: collection returned %d items",
            len(fresh_items),
        )

        # ----------------------------------------------------
        # بحث مخصص للسؤال
        # ----------------------------------------------------

        try:

            selected_items = search_news(
                fresh_items,
                user_text,
                max_results=MAX_SEARCH_RESULTS,
            )

        except Exception as exc:

            logger.exception(
                "MESSAGE: search_news failed: %s",
                exc,
            )

            selected_items = []

        logger.info(
            "MESSAGE: selected %d relevant items for query '%s'",
            len(selected_items),
            user_text,
        )

        # ----------------------------------------------------
        # مهم:
        # لا نستخدم الأخبار العامة كـ fallback هنا.
        # ----------------------------------------------------

        if not selected_items:

            await status_message.edit_text(
                "لم أجد في الأخبار الحالية معلومات مرتبطة "
                "بموضوع سؤالك.\n\n"
                "لن أخلط أخبارًا غير مرتبطة بالسؤال "
                "لإعطاء إجابة مصطنعة."
            )

            return

        # ----------------------------------------------------
        # Gemini
        # ----------------------------------------------------

        context_text = build_ai_context(
            selected_items
        )

        prompt = f"""
أنت محلل أخبار واستخبارات مفتوحة المصدر.

سؤال المستخدم:
{user_text}

تم اختيار الأخبار التالية لأنها مرتبطة بالسؤال.

مهم جدًا:
- لا تستخدم أخبارًا خارج السياق.
- لا تخترع معلومات.
- لا تفترض أن كل خبر في السياق يجيب عن السؤال.
- استخدم فقط الأخبار التي لها علاقة مباشرة بالسؤال.
- إذا كانت المعلومات لا تكفي للإجابة الكاملة، قل ذلك بوضوح.
- لا تحول التحليل إلى توقعات غير مدعومة.
- لا تكرر الخبر عدة مرات.

{REPORT_STYLE_RULES}

ابدأ بالإجابة المباشرة.

استخدم هذا الهيكل عند الحاجة:

الخلاصة:
الإجابة المباشرة في 2 إلى 3 جمل.

أهم ما ورد:
• النقطة الأولى.
• النقطة الثانية.
• النقطة الثالثة.

التحليل:
نقاط مختصرة فقط إذا كان هناك تحليل مفيد.

ملاحظة:
فقط إذا كان هناك نقص أو عدم تأكد مهم.

الأخبار المرتبطة بالسؤال:
{context_text}
"""

        await status_message.edit_text(
            "🧠 وجدت أخبارًا مرتبطة بالسؤال.\n\n"
            "جاري تحليلها باختصار..."
        )

        answer = await ask_gemini(
            prompt
        )

        try:

            await status_message.delete()

        except Exception:
            pass

        await send_long_message(
            update,
            answer,
            reply_markup=back_keyboard(),
        )


# ============================================================
# POST INIT
# ============================================================

async def post_init(
    application,
):

    logger.info(
        "Verifying Telegram connection..."
    )

    try:

        bot_info = await application.bot.get_me()

        logger.info(
            "Telegram connection verified successfully."
        )

        logger.info(
            "Bot username: @%s",
            bot_info.username,
        )

    except Exception as exc:

        logger.exception(
            "Telegram connection verification failed: %s",
            exc,
        )

        raise


# ============================================================
# MAIN
# ============================================================

def main():

    logger.info(
        "Starting Live News Intelligence Bot..."
    )

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(
        CommandHandler(
            "start",
            start_command,
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            button_handler,
            pattern=r"^(topic:|compare$|back$)",
        )
    )

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_user_message,
        )
    )

    logger.info(
        "Starting Telegram polling..."
    )

    app.run_polling(
        drop_pending_updates=True
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
