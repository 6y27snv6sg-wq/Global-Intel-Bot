import asyncio
import logging
import os

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

# ============================================================
# NEWS ENGINE
# ============================================================

from news_engine import (
    collect_news,
    search_news,
    build_ai_context,
    format_news_for_telegram,
)


# ============================================================
# SETTINGS
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not TELEGRAM_BOT_TOKEN:
    raise ValueError(
        "ERROR: TELEGRAM_BOT_TOKEN is missing!"
    )

if not GEMINI_API_KEY:
    raise ValueError(
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
# LIMITS
# ============================================================

MAX_NEWS_FOR_AI = 20
MAX_SEARCH_RESULTS = 15
MAX_HISTORY = 6


# ============================================================
# MAIN KEYBOARD
# ============================================================

def get_main_keyboard():

    return InlineKeyboardMarkup([

        [
            InlineKeyboardButton(
                "🔴 عاجل الآن",
                callback_data="topic_عاجل هجوم صاروخ قصف انفجار حرب تصعيد"
            )
        ],

        [
            InlineKeyboardButton(
                "🌍 العالم",
                callback_data="topic_العالم دولي دولية أزمة اتفاق"
            )
        ],

        [
            InlineKeyboardButton(
                "🇸🇦 الخليج والعالم العربي",
                callback_data="topic_السعودية الخليج العربي قطر الإمارات"
            )
        ],

        [
            InlineKeyboardButton(
                "🇺🇸 أمريكا",
                callback_data="topic_أمريكا الولايات المتحدة واشنطن"
            ),

            InlineKeyboardButton(
                "🇪🇺 أوروبا",
                callback_data="topic_أوروبا بريطانيا فرنسا ألمانيا"
            ),
        ],

        [
            InlineKeyboardButton(
                "🌏 آسيا",
                callback_data="topic_آسيا الصين اليابان الهند روسيا"
            )
        ],

        [
            InlineKeyboardButton(
                "🛢️ الطاقة والأسواق",
                callback_data="topic_نفط أوبك أوبك+ غاز طاقة أسواق اقتصاد"
            )
        ],

        [
            InlineKeyboardButton(
                "🛡️ الأمن والصراعات",
                callback_data="topic_أمن صراع حرب هجوم عسكري صاروخ"
            )
        ],

        [
            InlineKeyboardButton(
                "🏛️ بيانات وزارات الخارجية",
                callback_data="topic_وزارة الخارجية وزير الخارجية بيان تصريح"
            )
        ],

        [
            InlineKeyboardButton(
                "⚡ تحليل مقارن شامل",
                callback_data="compare_all"
            )
        ],
    ])


# ============================================================
# BACK BUTTON
# ============================================================

def get_back_keyboard():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "🔙 العودة للقائمة الرئيسية",
                callback_data="back_to_menu",
            )
        ]
    ])


# ============================================================
# SAFE MESSAGE SPLITTER
# ============================================================

def split_text_safely(
    text,
    max_length=3900,
):

    if not text:
        return [""]

    chunks = []

    paragraphs = text.split("\n")

    current = ""

    for paragraph in paragraphs:

        candidate = (
            paragraph
            if not current
            else current + "\n" + paragraph
        )

        if len(candidate) <= max_length:

            current = candidate
            continue

        if current:

            chunks.append(
                current
            )

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

            paragraph = (
                paragraph[cut:]
                .strip()
            )

        current = paragraph

    if current:

        chunks.append(
            current
        )

    return chunks or [""]


# ============================================================
# TELEGRAM SEND
# ============================================================

async def send_long_message(
    update,
    text,
    query=None,
    keyboard=None,
):

    chunks = split_text_safely(
        text
    )

    try:

        if query:

            await query.edit_message_text(
                text=chunks[0]
            )

            if len(chunks) > 1:

                for chunk in chunks[1:-1]:

                    await context_bot_send(
                        update,
                        chunk,
                    )

                await context_bot_send(
                    update,
                    chunks[-1],
                    keyboard,
                )

            elif keyboard:

                await query.edit_message_reply_markup(
                    reply_markup=keyboard
                )

        else:

            for index, chunk in enumerate(
                chunks
            ):

                if index == len(chunks) - 1:

                    await context_bot_send(
                        update,
                        chunk,
                        keyboard,
                    )

                else:

                    await context_bot_send(
                        update,
                        chunk,
                    )

    except Exception as exc:

        logger.exception(
            "Telegram send error: %s",
            exc,
        )


async def context_bot_send(
    update,
    text,
    keyboard=None,
):

    await update.get_bot().send_message(
        chat_id=update.effective_chat.id,
        text=text,
        reply_markup=keyboard,
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

        items = await collect_news(
            max_items=max_items
        )

        logger.info(
            "Fresh news collected: %s",
            len(items),
        )

        return items

    except Exception as exc:

        logger.exception(
            "News collection failed: %s",
            exc,
        )

        return []


# ============================================================
# GEMINI ANALYSIS
# ============================================================

async def ask_gemini(
    prompt,
):

    response = await asyncio.to_thread(
        ai_client.models.generate_content,
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                thinking_level=types.ThinkingLevel.LOW
            )
        ),
    )

    return (
        response.text
        if response and response.text
        else "لم يُرجع نموذج الذكاء الاصطناعي إجابة."
    )


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

    if report_type == "compare":

        prompt = f"""
أنت محلل سياسي واقتصادي محترف.

لديك الآن بيانات أخبار حديثة جمعت من مصادر
متعددة.

حلل البيانات التالية فقط.

المطلوب:

1. تحديد أهم الأحداث.
2. مقارنة الروايات بين المصادر.
3. تحديد الحقائق المشتركة.
4. تحديد الاختلافات في التغطية.
5. إبراز البيانات الرسمية.
6. التمييز بوضوح بين:
   - بيان رسمي
   - تقرير وكالة أنباء
   - تقرير قناة أو موقع إخباري
   - استنتاج تحليلي
7. لا تخترع أي معلومة.
8. لا تعتبر غياب الخبر دليلاً على عدم حدوثه.
9. إذا كانت البيانات غير كافية، قل ذلك صراحة.
10. ركز على الأخبار الحديثة والمهمة فقط.

مصادر الأخبار:

----------------
{context}
----------------

اكتب بالعربية.

ابدأ بملخص تنفيذي قصير، ثم:
- أبرز التطورات
- المواقف الرسمية
- مقارنة التغطية
- ما هو مؤكد
- ما هو غير مؤكد
- قراءة تحليلية مختصرة

لا تكرر الأخبار المتشابهة.
"""

    else:

        prompt = f"""
أنت محلل سياسي واقتصادي محترف.

حلل الأخبار الحديثة التالية.

القواعد الصارمة:

- اعتمد فقط على البيانات الموجودة.
- لا تختلق أسماء أو تصريحات أو أرقاماً.
- نسب كل معلومة إلى مصدرها.
- أعط الأولوية للبيانات الرسمية.
- افصل الخبر المؤكد عن التحليل.
- إذا كان الخبر من وكالة أو قناة، اذكر أنه تقرير إعلامي.
- لا تكرر نفس الحدث عدة مرات.
- تجاهل الحشو.
- ركز على ما حدث ومتى ومن قال ماذا.
- إذا لم توجد معلومات كافية، قل ذلك بوضوح.

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

    return await ask_gemini(
        prompt
    )


# ============================================================
# /START
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    context.user_data.clear()

    await update.message.reply_text(
        "مرحباً بك في نظام الرصد الإخباري المباشر.\n\n"
        "المحرك يجلب الأخبار الحديثة من المصادر "
        "المتاحة ثم يفرزها ويزيل التكرار قبل التحليل.\n\n"
        "اختر ملفاً أو اكتب سؤالك مباشرة.",
        reply_markup=get_main_keyboard(),
    )


# ============================================================
# /RESET
# ============================================================

async def reset(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    context.user_data.clear()

    await update.message.reply_text(
        "تمت إعادة ضبط جلسة الرصد والمحادثة."
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

    data = query.data

    # --------------------------------------------------------
    # BACK
    # --------------------------------------------------------

    if data == "back_to_menu":

        await query.edit_message_text(
            text=(
                "اختر أحد الملفات أو اكتب سؤالك "
                "مباشرة:"
            ),
            reply_markup=get_main_keyboard(),
        )

        return

    # --------------------------------------------------------
    # LOADING
    # --------------------------------------------------------

    await query.edit_message_text(
        text=(
            "📡 جاري جمع الأخبار الحديثة...\n\n"
            "• فحص المصادر\n"
            "• إزالة التكرار\n"
            "• ترتيب الأخبار حسب الأهمية\n"
            "• تجهيز البيانات للتحليل"
        )
    )

    # --------------------------------------------------------
    # FRESH NEWS
    # --------------------------------------------------------

    fresh_items = await get_fresh_news(
        max_items=100
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

    # --------------------------------------------------------
    # SELECT TOPIC
    # --------------------------------------------------------

    if data.startswith("topic_"):

        keywords = data.replace(
            "topic_",
            "",
            1,
        )

        selected_items = search_news(
            fresh_items,
            keywords,
            limit=MAX_SEARCH_RESULTS,
        )

        # إذا لم يجد البحث نتائج قوية،
        # نستخدم أحدث الأخبار كاحتياط.
        if len(selected_items) < 3:

            selected_items = fresh_items[
                :MAX_SEARCH_RESULTS
            ]

        report_type = "normal"

    elif data == "compare_all":

        selected_items = fresh_items[
            :MAX_SEARCH_RESULTS
        ]

        report_type = "compare"

    else:

        return

    # --------------------------------------------------------
    # SHOW STATUS
    # --------------------------------------------------------

    try:

        await query.edit_message_text(
            text=(
                f"🧠 تم العثور على "
                f"{len(selected_items)} خبراً مناسباً.\n\n"
                "جاري تحليلها ومقارنة المصادر..."
            )
        )

    except Exception:

        pass

    # --------------------------------------------------------
    # AI
    # --------------------------------------------------------

    try:

        reply_text = await generate_report(
            selected_items,
            report_type,
        )

    except Exception as exc:

        logger.exception(
            "Gemini report error: %s",
            exc,
        )

        reply_text = (
            "حدث خطأ أثناء تحليل الأخبار بواسطة Gemini.\n\n"
            f"التفاصيل التقنية:\n{str(exc)}"
        )

    # --------------------------------------------------------
    # SAVE SESSION
    # --------------------------------------------------------

    context.user_data["current_report"] = (
        reply_text
    )

    context.user_data["latest_news"] = (
        selected_items
    )

    context.user_data["chat_history"] = []

    context.user_data["last_topic"] = data

    # --------------------------------------------------------
    # SEND
    # --------------------------------------------------------

    await send_long_message(
        update,
        reply_text,
        query=query,
        keyboard=get_back_keyboard(),
    )


# ============================================================
# DIRECT USER QUESTION
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

    # --------------------------------------------------------
    # THINKING MESSAGE
    # --------------------------------------------------------

    thinking_message = await update.message.reply_text(
        "📡 جاري فحص الأخبار الحديثة ثم تحليل سؤالك..."
    )

    # --------------------------------------------------------
    # ALWAYS GET FRESH NEWS
    # --------------------------------------------------------

    fresh_items = await get_fresh_news(
        max_items=100
    )

    # --------------------------------------------------------
    # SEARCH CURRENT NEWS
    # --------------------------------------------------------

    matching_items = search_news(
        fresh_items,
        user_text,
        limit=MAX_SEARCH_RESULTS,
    )

    # --------------------------------------------------------
    # PREVIOUS REPORT
    # --------------------------------------------------------

    current_report = context.user_data.get(
        "current_report",
        "",
    )

    # --------------------------------------------------------
    # HISTORY
    # --------------------------------------------------------

    history = context.user_data.setdefault(
        "chat_history",
        [],
    )

    recent_history = history[
        -MAX_HISTORY:
    ]

    conversation_text = ""

    for item in recent_history:

        conversation_text += (
            f"\nالمستخدم: {item['user']}\n"
            f"المحلل: {item['assistant']}\n"
        )

    # --------------------------------------------------------
    # BUILD CURRENT NEWS CONTEXT
    # --------------------------------------------------------

    if matching_items:

        news_context = build_ai_context(
            matching_items,
            max_items=MAX_SEARCH_RESULTS,
        )

    elif fresh_items:

        # لا يوجد تطابق قوي،
        # لكن لا نعود للتقرير القديم وحده.
        news_context = build_ai_context(
            fresh_items,
            max_items=12,
        )

    else:

        news_context = (
            "لم يتم الحصول على أخبار حديثة "
            "من المصادر الحالية."
        )

    # --------------------------------------------------------
    # AI PROMPT
    # --------------------------------------------------------

    prompt = f"""
أنت مساعد رصد إخباري وتحليل سياسي واقتصادي.

السؤال الحالي للمستخدم:

{user_text}

========================
الأخبار الحديثة التي جرى جمعها الآن
========================

{news_context}

========================
التقرير السابق - إن وجد
========================

{current_report if current_report else "لا يوجد تقرير سابق."}

========================
المحادثة السابقة
========================

{conversation_text if conversation_text else "لا توجد محادثة سابقة."}

========================
قواعد الإجابة
========================

1. السؤال الحالي هو الأولوية.
2. استخدم الأخبار الحديثة التي جُمعت الآن قبل التقرير السابق.
3. لا تجعل التقرير السابق مصدراً وحيداً للمعلومة.
4. إذا كان السؤال عن حدث جديد، أجب من الأخبار الحالية.
5. إذا كان السؤال لا علاقة له بالتقرير السابق، لا تجبر الإجابة على استخدامه.
6. إذا كان السؤال عن تصريح أو موقف رسمي:
   - اذكر الجهة.
   - اذكر المصدر.
   - وضح أنه موقف رسمي إذا كان كذلك.
7. إذا كان المصدر وكالة أنباء أو قناة:
   - انسب المعلومة للمصدر.
   - لا تقدمها كحقيقة رسمية إلا إذا كان هناك مصدر رسمي.
8. لا تخترع أي معلومة.
9. لا تملأ الفراغ بتخمين.
10. إذا لم نجد معلومات كافية، قل:
   "لم أجد في المصادر التي تم فحصها معلومات كافية للإجابة بدقة."
11. إذا قدمت استنتاجاً، ضع بوضوح:
   "استنتاج تحليلي:"
12. لا تعيد التقرير السابق كاملاً.
13. لا تكرر نفس الخبر.
14. أجب بالعربية.
15. كن مباشراً ومختصراً قدر الإمكان.

أجب الآن عن سؤال المستخدم.
"""

    # --------------------------------------------------------
    # GEMINI
    # --------------------------------------------------------

    try:

        reply_text = await ask_gemini(
            prompt
        )

        # Save history
        history.append({
            "user": user_text,
            "assistant": reply_text,
        })

        context.user_data["chat_history"] = (
            history[-MAX_HISTORY:]
        )

        # Keep latest fresh news in session
        context.user_data["latest_news"] = (
            matching_items
            if matching_items
            else fresh_items[:12]
        )

    except Exception as exc:

        logger.exception(
            "Gemini conversation error: %s",
            exc,
        )

        reply_text = (
            "حدث خطأ أثناء معالجة السؤال.\n\n"
            f"التفاصيل التقنية:\n{str(exc)}"
        )

    # --------------------------------------------------------
    # DELETE THINKING MESSAGE
    # --------------------------------------------------------

    try:

        await thinking_message.delete()

    except Exception:

        pass

    # --------------------------------------------------------
    # SEND ANSWER
    # --------------------------------------------------------

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
# STARTUP
# ============================================================

def main():

    logger.info(
        "Starting Live News Intelligence Bot..."
    )

    app = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .build()
    )

    # Commands
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

    # Buttons
    app.add_handler(
        CallbackQueryHandler(
            button_handler,
            pattern=r"^(topic_|compare_all|back_to_menu)",
        )
    )

    # Direct questions
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_user_message,
        )
    )

    # Errors
    app.add_error_handler(
        error_handler
    )

    logger.info(
        "Live News Intelligence Bot is running."
    )

    app.run_polling(
        drop_pending_updates=True
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
