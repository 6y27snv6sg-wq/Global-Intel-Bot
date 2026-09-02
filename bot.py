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
# GEMINI CLIENT
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
    "urgent": "أهم الأخبار العاجلة والتطورات المهمة الآن",
    "world": "أهم الأخبار العالمية",
    "gulf": "أخبار الخليج العربي",
    "america": "أخبار الولايات المتحدة والأمريكتين",
    "europe": "أخبار أوروبا",
    "asia": "أخبار آسيا",
    "energy": "الطاقة والنفط والغاز والأسواق المرتبطة بها",
    "security": "الأمن والدفاع والصراعات والتطورات العسكرية",
    "foreign": "السياسة الخارجية والعلاقات الدولية",
}


# ============================================================
# TELEGRAM KEYBOARD
# ============================================================

def main_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                "🚨 عاجل",
                callback_data="topic:urgent"
            ),
            InlineKeyboardButton(
                "🌍 العالم",
                callback_data="topic:world"
            ),
        ],
        [
            InlineKeyboardButton(
                "🇸🇦 الخليج",
                callback_data="topic:gulf"
            ),
            InlineKeyboardButton(
                "🇺🇸 أمريكا",
                callback_data="topic:america"
            ),
        ],
        [
            InlineKeyboardButton(
                "🇪🇺 أوروبا",
                callback_data="topic:europe"
            ),
            InlineKeyboardButton(
                "🌏 آسيا",
                callback_data="topic:asia"
            ),
        ],
        [
            InlineKeyboardButton(
                "⛽ الطاقة",
                callback_data="topic:energy"
            ),
            InlineKeyboardButton(
                "🛡 الأمن",
                callback_data="topic:security"
            ),
        ],
        [
            InlineKeyboardButton(
                "🌐 السياسة الخارجية",
                callback_data="topic:foreign"
            ),
        ],
        [
            InlineKeyboardButton(
                "🔄 مقارنة",
                callback_data="compare"
            ),
        ],
    ]

    return InlineKeyboardMarkup(keyboard)


def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬅️ رجوع",
                    callback_data="back"
                )
            ]
        ]
    )


# ============================================================
# GEMINI
# ============================================================

async def ask_gemini(prompt: str) -> str:
    logger.info("Starting Gemini request...")

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

        text = getattr(response, "text", None)

        if not text:
            logger.warning("Gemini returned an empty response.")
            return "لم يتمكن النظام من الحصول على تحليل من Gemini."

        logger.info("Gemini request completed.")

        return text.strip()

    except Exception as exc:
        logger.exception(
            "Gemini request failed: %s",
            exc
        )

        return (
            "حدث خطأ أثناء تحليل الأخبار بواسطة الذكاء الاصطناعي.\n\n"
            f"التفاصيل: {exc}"
        )


# ============================================================
# NEWS COLLECTION
# ============================================================

async def get_fresh_news():
    logger.info("Starting fresh news collection...")

    try:
        news = await asyncio.wait_for(
            collect_news(max_items=100),
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
            exc
        )

        return []


# ============================================================
# REPORT GENERATION
# ============================================================

async def generate_report(topic: str, news_items) -> str:
    if not news_items:
        return (
            "لم أتمكن من الحصول على أخبار حديثة كافية "
            "لإعداد التقرير حاليًا."
        )

    context = build_ai_context(news_items)

    prompt = f"""
أنت محلل أخبار واستخبارات مفتوحة المصدر.

الموضوع المطلوب:
{topic}

اعتمد فقط على الأخبار والبيانات الموجودة في السياق المرفق.

المطلوب:
1. تحديد أهم التطورات.
2. ترتيبها حسب الأهمية.
3. توضيح ما هو مؤكد وما هو غير مؤكد.
4. ربط الأحداث ببعضها عند وجود علاقة واضحة.
5. توضيح التأثير المحتمل.
6. عدم اختلاق أي معلومات غير موجودة في المصادر.
7. إذا كانت المعلومات غير كافية، اذكر ذلك بوضوح.

اكتب التقرير بالعربية بشكل واضح ومختصر.

استخدم هذا الهيكل:

🚨 الخلاصة
أهم ما يجب معرفته.

📰 أهم التطورات
- التطور الأول
- التطور الثاني
- التطور الثالث

🔎 التحليل
تحليل مختصر لما تعنيه التطورات.

📌 التأثير المحتمل
ما الذي قد يحدث أو يتغير بناءً على المعطيات الحالية.

⚠️ مستوى اليقين
وضح إن كانت المعلومات مؤكدة أو أولية أو تحتاج إلى متابعة.

مصادر الأخبار:
{context}
"""

    return await ask_gemini(prompt)


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
        for i in range(0, len(text), 3900)
    ]

    if not chunks:
        chunks = ["لم يتم إنشاء محتوى."]

    for index, chunk in enumerate(chunks):
        if update.callback_query:
            await update.callback_query.message.reply_text(
                chunk,
                reply_markup=(
                    reply_markup
                    if index == len(chunks) - 1
                    else None
                ),
            )
        elif update.message:
            await update.message.reply_text(
                chunk,
                reply_markup=(
                    reply_markup
                    if index == len(chunks) - 1
                    else None
                ),
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

    lock = get_user_lock(user_id)

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
                "ميزة المقارنة تحتاج إلى تحديد موضوعين "
                "أو حدثين للمقارنة بينهما.\n\n"
                "أرسل سؤالك مباشرة، مثل:\n"
                "قارن بين تطورات الطاقة في الخليج وأوروبا.",
                reply_markup=back_keyboard(),
            )

            return

        # ----------------------------------------------------
        # TOPIC
        # ----------------------------------------------------

        if not callback_data.startswith("topic:"):
            return

        topic_key = callback_data.split(
            ":",
            1
        )[1]

        topic = TOPICS.get(topic_key)

        if not topic:
            await query.edit_message_text(
                "الموضوع غير معروف.",
                reply_markup=back_keyboard(),
            )

            return

        await query.edit_message_text(
            "📡 جاري فحص الأخبار الحديثة ثم تحليلها...\n\n"
            "قد يستغرق ذلك عدة ثوانٍ."
        )

        logger.info(
            "BUTTON: collecting news for user %s",
            user_id,
        )

        try:
            fresh_items = await get_fresh_news()

            logger.info(
                "BUTTON: collection returned %d items",
                len(fresh_items),
            )

        except Exception as exc:
            logger.exception(
                "BUTTON: collection error: %s",
                exc
            )

            fresh_items = []

        # ----------------------------------------------------
        # SEARCH
        # ----------------------------------------------------

        try:
            selected_items = search_news(
                fresh_items,
                topic,
                max_results=MAX_SEARCH_RESULTS,
            )

        except Exception as exc:
            logger.exception(
                "BUTTON: search_news failed: %s",
                exc
            )

            selected_items = []

        # ----------------------------------------------------
        # FALLBACK
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
                "لم أتمكن من العثور على أخبار حديثة "
                "كافية لهذا الموضوع حاليًا.",
                reply_markup=back_keyboard(),
            )

            return

        # ----------------------------------------------------
        # GEMINI
        # ----------------------------------------------------

        await query.edit_message_text(
            "🧠 تم جمع الأخبار.\n\n"
            "جاري تحليلها وربط التطورات..."
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

        # ----------------------------------------------------
        # SEND REPORT
        # ----------------------------------------------------

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
# USER MESSAGE HANDLER
# ============================================================

async def handle_user_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message or not update.message.text:
        return

    user_id = update.effective_user.id
    user_text = update.message.text.strip()

    if not user_text:
        return

    lock = get_user_lock(user_id)

    if lock.locked():
        await update.message.reply_text(
            "يوجد تحليل جارٍ بالفعل، انتظر حتى يكتمل."
        )
        return

    async with lock:

        status_message = await update.message.reply_text(
            "📡 جاري فحص الأخبار الحديثة ثم تحليل سؤالك..."
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

        try:
            selected_items = search_news(
                fresh_items,
                user_text,
                max_results=MAX_SEARCH_RESULTS,
            )

        except Exception as exc:
            logger.exception(
                "MESSAGE: search_news failed: %s",
                exc
            )

            selected_items = []

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

        if not selected_items:

            await status_message.edit_text(
                "لم أتمكن من الحصول على أخبار حديثة "
                "كافية للإجابة عن سؤالك."
            )

            return

        context_text = build_ai_context(
            selected_items
        )

        prompt = f"""
أنت محلل أخبار واستخبارات مفتوحة المصدر.

سؤال المستخدم:
{user_text}

حلل السؤال اعتمادًا على الأخبار الحديثة الموجودة
في السياق أدناه.

القواعد:
- لا تختلق معلومات.
- لا تقدم ادعاءات غير موجودة في المصادر.
- فرّق بين الحقائق والتحليل والاستنتاج.
- إذا كانت البيانات غير كافية، قل ذلك بوضوح.
- ركز على الأخبار الحديثة ذات الصلة.
- أجب بالعربية.

السياق الإخباري:
{context_text}
"""

        await status_message.edit_text(
            "🧠 تم جمع الأخبار.\n\n"
            "جاري تحليل سؤالك..."
        )

        answer = await ask_gemini(prompt)

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

async def post_init(application):
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
            exc
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

    # --------------------------------------------------------
    # COMMANDS
    # --------------------------------------------------------

    app.add_handler(
        CommandHandler(
            "start",
            start_command,
        )
    )

    # --------------------------------------------------------
    # BUTTONS
    # --------------------------------------------------------

    app.add_handler(
        CallbackQueryHandler(
            button_handler,
            pattern=r"^(topic:|compare$|back$)",
        )
    )

    # --------------------------------------------------------
    # USER TEXT
    # --------------------------------------------------------

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
