import asyncio
import logging
import os
from collections import defaultdict

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
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


# ============================================================
# ENVIRONMENT
# ============================================================

# BOT_TOKEN هو الاسم المفضل الجديد.
# TELEGRAM_BOT_TOKEN يبقى مدعوماً حتى لا ينكسر إعداد Railway الحالي.
BOT_TOKEN = (
    os.getenv("BOT_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


if not BOT_TOKEN:
    raise RuntimeError(
        "ERROR: BOT_TOKEN / TELEGRAM_BOT_TOKEN is missing!"
    )


if not GEMINI_API_KEY:
    raise RuntimeError(
        "ERROR: GEMINI_API_KEY is missing!"
    )


# ============================================================
# GEMINI
# ============================================================

ai_client = genai.Client(
    api_key=GEMINI_API_KEY
)

GEMINI_MODEL = "gemini-3.5-flash"


# ============================================================
# SETTINGS
# ============================================================

MAX_NEWS_FOR_AI = 20
MAX_SEARCH_RESULTS = 15
MAX_HISTORY = 6

TELEGRAM_MAX_LENGTH = 3900

NEWS_COLLECTION_TIMEOUT = 20


# ============================================================
# USER LOCKS
# ============================================================

USER_LOCKS = defaultdict(asyncio.Lock)


# ============================================================
# TOPICS
# ============================================================

TOPICS = {
    "urgent": (
        "عاجل هجوم صاروخ قصف انفجار حرب تصعيد"
    ),
    "world": (
        "العالم دولي دولية أزمة اتفاق"
    ),
    "gulf": (
        "السعودية الخليج العربي قطر الإمارات"
    ),
    "america": (
        "أمريكا الولايات المتحدة واشنطن"
    ),
    "europe": (
        "أوروبا بريطانيا فرنسا ألمانيا"
    ),
    "asia": (
        "آسيا الصين اليابان الهند روسيا"
    ),
    "energy": (
        "نفط أوبك أوبك+ غاز طاقة أسواق اقتصاد"
    ),
    "security": (
        "أمن صراع حرب هجوم عسكري صاروخ"
    ),
    "foreign": (
        "وزارة الخارجية وزير الخارجية بيان تصريح"
    ),
}


# ============================================================
# KEYBOARD
# ============================================================

def get_main_keyboard():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "🔴 عاجل الآن",
                callback_data="topic:urgent",
            )
        ],
        [
            InlineKeyboardButton(
                "🌍 العالم",
                callback_data="topic:world",
            )
        ],
        [
            InlineKeyboardButton(
                "🇸🇦 الخليج والعالم العربي",
                callback_data="topic:gulf",
            )
        ],
        [
            InlineKeyboardButton(
                "🇺🇸 أمريكا",
                callback_data="topic:america",
            ),
            InlineKeyboardButton(
                "🇪🇺 أوروبا",
                callback_data="topic:europe",
            ),
        ],
        [
            InlineKeyboardButton(
                "🌏 آسيا",
                callback_data="topic:asia",
            )
        ],
        [
            InlineKeyboardButton(
                "🛢️ الطاقة والأسواق",
                callback_data="topic:energy",
            )
        ],
        [
            InlineKeyboardButton(
                "🛡️ الأمن والصراعات",
                callback_data="topic:security",
            )
        ],
        [
            InlineKeyboardButton(
                "🏛️ بيانات وزارات الخارجية",
                callback_data="topic:foreign",
            )
        ],
        [
            InlineKeyboardButton(
                "⚡ تحليل مقارن شامل",
                callback_data="compare",
            )
        ],
    ])


def get_back_keyboard():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "🔙 العودة للقائمة الرئيسية",
                callback_data="back",
            )
        ]
    ])


# ============================================================
# TEXT SPLITTER
# ============================================================

def split_text_safely(
    text: str,
    max_length: int = TELEGRAM_MAX_LENGTH,
):

    if not text:
        return [""]

    chunks = []
    paragraphs = text.split("\n")
    current = ""

    for paragraph in paragraphs:

        paragraph = paragraph.strip()

        candidate = (
            paragraph
            if not current
            else current + "\n" + paragraph
        )

        if len(candidate) <= max_length:

            current = candidate
            continue

        if current:

            chunks.append(current)
            current = ""

        while len(paragraph) > max_length:

            cut = paragraph.rfind(
                " ",
                0,
                max_length,
            )

            if cut < 100:
                cut = max_length

            chunks.append(
                paragraph[:cut].strip()
            )

            paragraph = paragraph[cut:].strip()

        current = paragraph

    if current:
        chunks.append(current)

    return chunks or [""]


# ============================================================
# SEND LONG MESSAGE
# ============================================================

async def send_long_message(
    update: Update,
    text: str,
    query=None,
    keyboard=None,
):

    chunks = split_text_safely(text)

    try:

        if query:

            await query.edit_message_text(
                text=chunks[0]
            )

            for chunk in chunks[1:-1]:

                await update.get_bot().send_message(
                    chat_id=update.effective_chat.id,
                    text=chunk,
                )

            if len(chunks) > 1:

                await update.get_bot().send_message(
                    chat_id=update.effective_chat.id,
                    text=chunks[-1],
                    reply_markup=keyboard,
                )

            elif keyboard:

                await query.edit_message_reply_markup(
                    reply_markup=keyboard
                )

        else:

            for index, chunk in enumerate(chunks):

                await update.get_bot().send_message(
                    chat_id=update.effective_chat.id,
                    text=chunk,
                    reply_markup=(
                        keyboard
                        if index == len(chunks) - 1
                        else None
                    ),
                )

    except Exception as exc:

        logger.exception(
            "Telegram send error: %s",
            exc,
        )


# ============================================================
# NEWS COLLECTION
# ============================================================

async def get_fresh_news(
    max_items=100,
):

    try:

        logger.info(
            "Starting fresh news collection..."
        )

        items = await asyncio.wait_for(
            collect_news(
                max_items=max_items
            ),
            timeout=NEWS_COLLECTION_TIMEOUT,
        )

        logger.info(
            "Fresh news collected: %s",
            len(items),
        )

        return items or []

    except asyncio.TimeoutError:

        logger.error(
            "Fresh news collection timed out after %s seconds.",
            NEWS_COLLECTION_TIMEOUT,
        )

        return []

    except Exception as exc:

        logger.exception(
            "News collection failed: %s",
            exc,
        )

        return []


# ============================================================
# GEMINI
# ============================================================

async def ask_gemini(
    prompt: str,
):

    try:

        logger.info(
            "Sending request to Gemini..."
        )

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

        if response and response.text:

            logger.info(
                "Gemini response received."
            )

            return response.text.strip()

        return (
            "لم يُرجع نموذج الذكاء الاصطناعي "
            "إجابة نصية."
        )

    except Exception as exc:

        logger.exception(
            "Gemini API error: %s",
            exc,
        )

        raise


# ============================================================
# GENERATE REPORT
# ============================================================

async def generate_report(
    news_items,
    report_type="normal",
):

    context = build_ai_context(
        news_items,
        max_items=MAX_NEWS_FOR_AI,
    )

    if not context:

        return (
            "لم يتم العثور على بيانات إخبارية "
            "كافية للتحليل."
        )

    if report_type == "compare":

        prompt = f"""
أنت محلل سياسي واقتصادي محترف متخصص
في مقارنة المصادر الإخبارية.

حلل البيانات التالية فقط.

القواعد:

1. حدد أهم الأحداث.
2. قارن الروايات بين المصادر.
3. حدد الحقائق المشتركة.
4. حدد الاختلافات بين التغطيات.
5. أبرز البيانات الرسمية.
6. ميز بوضوح بين:
   - بيان رسمي
   - وكالة أنباء
   - قناة أو موقع إخباري
   - استنتاج تحليلي
7. لا تخترع أي معلومة.
8. لا تعتبر غياب الخبر دليلاً على عدم حدوثه.
9. إذا كانت البيانات غير كافية، قل ذلك.
10. لا تكرر الأحداث المتشابهة.

مصادر الأخبار:

----------------
{context}
----------------

اكتب بالعربية.

الهيكل:

الملخص التنفيذي

أبرز التطورات

المواقف الرسمية

مقارنة التغطية

ما هو مؤكد

ما هو غير مؤكد

القراءة التحليلية

اجعل التقرير مركزاً وواضحاً.
"""

    else:

        prompt = f"""
أنت محلل سياسي واقتصادي محترف.

حلل الأخبار الحديثة التالية.

القواعد الصارمة:

- اعتمد فقط على البيانات الموجودة.
- لا تختلق أسماء أو تصريحات أو أرقاماً.
- انسب المعلومات إلى مصادرها.
- أعط الأولوية للبيانات الرسمية.
- افصل الخبر المؤكد عن التحليل.
- إذا كان المصدر وكالة أو قناة،
  اذكر أنه تقرير إعلامي.
- لا تكرر نفس الحدث.
- تجاهل الحشو.
- ركز على ماذا حدث ومتى ومن قال ماذا.
- إذا لم توجد معلومات كافية، قل ذلك صراحة.

الأخبار:

----------------
{context}
----------------

اكتب تقريراً عربياً مختصراً ومنظماً:

1. أهم المستجدات
2. المواقف الرسمية
3. الأطراف المعنية
4. الحقائق المؤكدة
5. ما يحتاج إلى تأكيد
6. الدلالة المحتملة

لا تضف معلومات من خارج البيانات.
"""

    return await ask_gemini(prompt)


# ============================================================
# STARTUP VERIFICATION
# ============================================================

async def post_init(application):

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

        logger.error(
            "Telegram authentication failed: %s",
            exc,
        )

        raise


# ============================================================
# START
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    context.user_data.clear()

    await update.message.reply_text(
        "مرحباً بك في نظام الرصد الإخباري المباشر.\n\n"
        "المحرك يجلب الأخبار الحديثة من المصادر "
        "المتاحة، ثم يفرزها ويزيل التكرار قبل التحليل.\n\n"
        "اختر ملفاً أو اكتب سؤالك مباشرة.",
        reply_markup=get_main_keyboard(),
    )


# ============================================================
# RESET
# ============================================================

async def reset(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    context.user_data.clear()

    await update.message.reply_text(
        "تمت إعادة ضبط جلسة الرصد والمحادثة.",
        reply_markup=get_main_keyboard(),
    )


# ============================================================
# BUTTON HANDLER
# ============================================================

async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query

    try:
        await query.answer()
    except Exception:
        pass

    data = query.data

    # --------------------------------------------------------
    # العودة
    # --------------------------------------------------------

    if data == "back":

        await query.edit_message_text(
            text=(
                "اختر أحد الملفات أو اكتب سؤالك "
                "مباشرة:"
            ),
            reply_markup=get_main_keyboard(),
        )

        return

    # --------------------------------------------------------
    # نوع التقرير
    # --------------------------------------------------------

    if data == "compare":

        topic_key = None
        report_type = "compare"

    elif data.startswith("topic:"):

        topic_key = data.split(":", 1)[1]

        if topic_key not in TOPICS:

            await query.edit_message_text(
                text="الخيار غير معروف.",
                reply_markup=get_back_keyboard(),
            )

            return

        report_type = "normal"

    else:

        return

    user_id = update.effective_user.id

    async with USER_LOCKS[user_id]:

        # ----------------------------------------------------
        # رسالة الحالة
        # ----------------------------------------------------

        try:

            await query.edit_message_text(
                text=(
                    "📡 جاري جمع الأخبار الحديثة...\n\n"
                    "• فحص المصادر\n"
                    "• إزالة التكرار\n"
                    "• ترتيب الأخبار\n"
                    "• تجهيز البيانات للتحليل"
                )
            )

        except Exception as exc:

            logger.warning(
                "Could not update button status: %s",
                exc,
            )

        # ----------------------------------------------------
        # جمع الأخبار
        # ----------------------------------------------------

        logger.info(
            "BUTTON: collecting news for user %s",
            user_id,
        )

        fresh_items = await get_fresh_news(
            max_items=100
        )

        logger.info(
            "BUTTON: collection returned %d items",
            len(fresh_items),
        )

        if not fresh_items:

            await query.edit_message_text(
                text=(
                    "تعذر الحصول على أخبار من المصادر "
                    "حالياً.\n\n"
                    "قد يكون أحد المصادر متوقفاً أو "
                    "محجوباً مؤقتاً."
                ),
                reply_markup=get_back_keyboard(),
            )

            return

        # ----------------------------------------------------
        # البحث داخل الأخبار
        # ----------------------------------------------------

        try:

            if topic_key:

                keywords = TOPICS[topic_key]

                selected_items = search_news(
                    fresh_items,
                    keywords,
                    max_results=MAX_SEARCH_RESULTS,
                )

                if len(selected_items) < 3:

                    selected_items = fresh_items[
                        :MAX_SEARCH_RESULTS
                    ]

            else:

                selected_items = fresh_items[
                    :MAX_SEARCH_RESULTS
                ]

            logger.info(
                "BUTTON: selected %d news items",
                len(selected_items),
            )

        except Exception as exc:

            logger.exception(
                "News search failed: %s",
                exc,
            )

            await query.edit_message_text(
                text=(
                    "تم جمع الأخبار، لكن حدث خطأ "
                    "أثناء تصنيفها."
                ),
                reply_markup=get_back_keyboard(),
            )

            return

        # ----------------------------------------------------
        # تحليل Gemini
        # ----------------------------------------------------

        try:

            await query.edit_message_text(
                text=(
                    f"🧠 تم العثور على "
                    f"{len(selected_items)} خبراً مناسباً.\n\n"
                    "جاري تحليلها ومقارنة المصادر..."
                )
            )

        except Exception as exc:

            logger.warning(
                "Could not update Gemini status: %s",
                exc,
            )

        try:

            logger.info(
                "BUTTON: starting Gemini report..."
            )

            reply_text = await generate_report(
                selected_items,
                report_type,
            )

            logger.info(
                "BUTTON: Gemini report completed."
            )

        except Exception as exc:

            logger.exception(
                "Gemini report error: %s",
                exc,
            )

            reply_text = (
                "تعذر تحليل الأخبار بواسطة "
                "نموذج الذكاء الاصطناعي حالياً.\n\n"
                "يرجى المحاولة مرة أخرى بعد قليل."
            )

        # ----------------------------------------------------
        # حفظ الجلسة
        # ----------------------------------------------------

        context.user_data[
            "current_report"
        ] = reply_text

        context.user_data[
            "latest_news"
        ] = selected_items

        context.user_data[
            "chat_history"
        ] = []

        context.user_data[
            "last_topic"
        ] = data

        # ----------------------------------------------------
        # إرسال التقرير
        # ----------------------------------------------------

        await send_long_message(
            update,
            reply_text,
            query=query,
            keyboard=get_back_keyboard(),
        )


# ============================================================
# USER MESSAGE
# ============================================================

async def handle_user_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    user_text = (
        update.message.text.strip()
        if update.message
        else ""
    )

    if not user_text:
        return

    user_id = update.effective_user.id

    async with USER_LOCKS[user_id]:

        thinking_message = await update.message.reply_text(
            "📡 جاري فحص الأخبار الحديثة ثم تحليل سؤالك..."
        )

        # ----------------------------------------------------
        # جمع الأخبار
        # ----------------------------------------------------

        fresh_items = await get_fresh_news(
            max_items=100
        )

        # ----------------------------------------------------
        # البحث
        # ----------------------------------------------------

        try:

            matching_items = search_news(
                fresh_items,
                user_text,
                max_results=MAX_SEARCH_RESULTS,
            )

        except Exception as exc:

            logger.exception(
                "User news search failed: %s",
                exc,
            )

            matching_items = []

        # ----------------------------------------------------
        # التقرير السابق
        # ----------------------------------------------------

        current_report = context.user_data.get(
            "current_report",
            "",
        )

        # ----------------------------------------------------
        # التاريخ
        # ----------------------------------------------------

        history = context.user_data.setdefault(
            "chat_history",
            [],
        )

        recent_history = history[-MAX_HISTORY:]

        conversation_text = ""

        for item in recent_history:

            conversation_text += (
                f"\nالمستخدم: {item['user']}\n"
                f"المحلل: {item['assistant']}\n"
            )

        # ----------------------------------------------------
        # سياق الأخبار
        # ----------------------------------------------------

        if matching_items:

            news_context = build_ai_context(
                matching_items,
                max_items=MAX_SEARCH_RESULTS,
            )

        elif fresh_items:

            news_context = build_ai_context(
                fresh_items,
                max_items=12,
            )

        else:

            news_context = (
                "لم يتم الحصول على أخبار حديثة "
                "من المصادر الحالية."
            )

        # ----------------------------------------------------
        # Prompt
        # ----------------------------------------------------

        prompt = f"""
أنت مساعد رصد إخباري وتحليل سياسي واقتصادي.

السؤال الحالي للمستخدم:

{user_text}

========================
الأخبار الحديثة التي جُمعت الآن
========================

{news_context}

========================
التقرير السابق - إن وجد
========================

{
    current_report
    if current_report
    else
    "لا يوجد تقرير سابق."
}

========================
المحادثة السابقة
========================

{
    conversation_text
    if conversation_text
    else
    "لا توجد محادثة سابقة."
}

========================
قواعد الإجابة
========================

1. السؤال الحالي هو الأولوية.
2. استخدم الأخبار الحديثة أولاً.
3. لا تجعل التقرير السابق مصدراً وحيداً.
4. إذا كان السؤال عن حدث جديد، استخدم الأخبار الحالية.
5. إذا كان السؤال عن تصريح أو موقف رسمي:
   - اذكر الجهة.
   - اذكر المصدر.
   - وضح أنه موقف رسمي إذا كان كذلك.
6. إذا كان المصدر وكالة أو قناة:
   - انسب المعلومة للمصدر.
7. لا تقدم تقريراً إعلامياً كحقيقة رسمية.
8. لا تخترع أي معلومة.
9. لا تملأ الفراغ بالتخمين.
10. إذا لم توجد معلومات كافية قل:
   "لم أجد في المصادر التي تم فحصها معلومات كافية للإجابة بدقة."
11. إذا قدمت استنتاجاً، اكتب:
   "استنتاج تحليلي:"
12. لا تعيد التقرير السابق كاملاً.
13. لا تكرر الأخبار.
14. أجب بالعربية.
15. كن مباشراً ومختصراً.
16. لا تضف معلومات من خارج البيانات المتاحة.

أجب الآن عن سؤال المستخدم.
"""

        # ----------------------------------------------------
        # Gemini
        # ----------------------------------------------------

        try:

            reply_text = await ask_gemini(
                prompt
            )

            history.append({
                "user": user_text,
                "assistant": reply_text,
            })

            context.user_data[
                "chat_history"
            ] = history[-MAX_HISTORY:]

            context.user_data[
                "latest_news"
            ] = (
                matching_items
                if matching_items
                else fresh_items[:12]
            )

            context.user_data[
                "current_report"
            ] = reply_text

        except Exception as exc:

            logger.exception(
                "Gemini conversation error: %s",
                exc,
            )

            reply_text = (
                "تعذر معالجة السؤال بواسطة "
                "نموذج الذكاء الاصطناعي حالياً.\n\n"
                "يرجى المحاولة مرة أخرى."
            )

        # ----------------------------------------------------
        # حذف رسالة الانتظار
        # ----------------------------------------------------

        try:

            await thinking_message.delete()

        except Exception:
            pass

        # ----------------------------------------------------
        # إرسال الإجابة
        # ----------------------------------------------------

        await send_long_message(
            update,
            reply_text,
            keyboard=get_back_keyboard(),
        )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update,
    context: ContextTypes.DEFAULT_TYPE,
):

    logger.error(
        "Unhandled exception",
        exc_info=context.error,
    )


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
    # Commands
    # --------------------------------------------------------

    app.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    app.add_handler(
        CommandHandler(
            "reset",
            reset,
        )
    )

    # --------------------------------------------------------
    # Buttons
    # --------------------------------------------------------

    app.add_handler(
        CallbackQueryHandler(
            button_handler,
            pattern=r"^(topic:|compare$|back$)",
        )
    )

    # --------------------------------------------------------
    # Text messages
    # --------------------------------------------------------

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_user_message,
        )
    )

    # --------------------------------------------------------
    # Errors
    # --------------------------------------------------------

    app.add_error_handler(
        error_handler
    )

    # --------------------------------------------------------
    # Polling
    # --------------------------------------------------------

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
