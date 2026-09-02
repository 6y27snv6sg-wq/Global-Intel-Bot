import asyncio
import os
import logging

import aiohttp
import feedparser

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
# الإعدادات
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not TELEGRAM_BOT_TOKEN:
    raise ValueError("ERROR: TELEGRAM_BOT_TOKEN is missing!")

if not GEMINI_API_KEY:
    raise ValueError("ERROR: GEMINI_API_KEY is missing!")


# ============================================================
# Gemini
# ============================================================

ai_client = genai.Client(api_key=GEMINI_API_KEY)

GEMINI_MODEL = "gemini-3.5-flash"


# ============================================================
# مصادر الأخبار
# ============================================================

GLOBAL_FEEDS = {
    "🇸🇦 وكالة الأنباء السعودية (واس)":
        "https://www.spa.gov.sa/rss.php",

    "🇶🇦 وكالة الأنباء القطرية (قنا)":
        "https://www.qna.org.qa/Rss/News",

    "🇨🇳 وكالة أنباء الصين (شينخوا)":
        "http://www.xinhuanet.com/english/rss/worldrss.xml",

    "🇺🇸 رويترز (دولية وعربية)":
        "https://www.reutersagency.com/feed/?best-topics=political-general&post_type=best",

    "🇷🇺 وكالة أنباء روسيا (TASS)":
        "https://tass.com/rss/v2.xml",

    "🇫🇷 فرانس 24 (أوروبية/عربية)":
        "https://www.france24.com/ar/rss",

    "🇬🇧 بي بي سي عربي (بريطانية)":
        "http://feeds.bbci.co.uk/arabic/rss.xml",
}


# ============================================================
# جلب مصدر واحد
# ============================================================

async def fetch_single_feed(
    session,
    source_name,
    url,
    keywords_list,
):

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:

        async with session.get(
            url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as response:

            if response.status != 200:

                logger.warning(
                    "Feed %s returned HTTP %s",
                    source_name,
                    response.status,
                )

                return ""

            content = await response.text()

            feed = feedparser.parse(content)

            extracted = []

            for entry in feed.entries[:6]:

                title = entry.get(
                    "title",
                    "",
                ).strip()

                summary = entry.get(
                    "summary",
                    "",
                ).strip()

                full_text = (
                    f"{title} {summary}"
                ).lower()

                if any(
                    word.lower() in full_text
                    for word in keywords_list
                    if word.strip()
                ):

                    extracted.append(
                        f"\n[المصدر: {source_name}]\n"
                        f"العنوان: {title}\n"
                        f"التفاصيل: {summary[:500]}\n"
                    )

            return "".join(extracted)

    except asyncio.TimeoutError:

        logger.warning(
            "Timeout while fetching %s",
            source_name,
        )

        return ""

    except Exception as e:

        logger.error(
            "Error fetching %s: %s",
            source_name,
            e,
        )

        return ""


# ============================================================
# جلب جميع الأخبار بالتوازي
# ============================================================

async def fetch_all_news(keywords):

    search_words = keywords.split()

    async with aiohttp.ClientSession() as session:

        tasks = [
            fetch_single_feed(
                session,
                source,
                url,
                search_words,
            )
            for source, url in GLOBAL_FEEDS.items()
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

    collected = []

    for result in results:

        if isinstance(result, str) and result:
            collected.append(result)

    collected_text = "\n".join(collected)

    if collected_text:
        return collected_text

    return (
        "لم يتم العثور على تغطية مباشرة "
        "بالكلمات المفتاحية المطلوبة."
    )


# ============================================================
# القائمة الرئيسية
# ============================================================

def get_main_keyboard():

    return InlineKeyboardMarkup([

        [
            InlineKeyboardButton(
                "🛢️ ملف الطاقة والنفط والأسواق",
                callback_data="topic_طاقة نفط اقتصاد اسعار",
            )
        ],

        [
            InlineKeyboardButton(
                "🛡️ ملف الصراعات والتوترات العسكرية",
                callback_data="topic_صراعات حرب جيوسياسة جيش",
            )
        ],

        [
            InlineKeyboardButton(
                "💱 البنوك المركزية والسياسة المالية",
                callback_data="topic_بنك فدرالي فائدة عملات",
            ),

            InlineKeyboardButton(
                "🌐 تصريحات وزارات الخارجية",
                callback_data="topic_خارجية سفير بيان رسمي",
            ),
        ],

        [
            InlineKeyboardButton(
                "⚡ تحليل مقارن شامل (عربي ودولي)",
                callback_data="compare_all",
            )
        ],
    ])


# ============================================================
# تقسيم الرسائل الطويلة
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
# إرسال رسالة طويلة
# ============================================================

async def send_long_message(
    update,
    text,
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

                await context_bot_send(
                    update,
                    chunk,
                )

            if len(chunks) > 1:

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

            for index, chunk in enumerate(chunks):

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

    except Exception as e:

        logger.error(
            "Telegram Reply Error: %s",
            e,
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
# /start
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    # تنظيف السياق عند بدء جلسة جديدة
    context.user_data.clear()

    await update.message.reply_text(
        "مرحباً بك في نظام موجز الأحداث الدولية "
        "والإحاطة الاستخباراتي الذكي 🌐🏛️\n\n"
        "اختر أحد الملفات الاستراتيجية أدناه:",
        reply_markup=get_main_keyboard(),
    )


# ============================================================
# /reset
# ============================================================

async def reset(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    context.user_data.clear()

    await update.message.reply_text(
        "تمت إعادة ضبط سياق المحادثة."
    )


# ============================================================
# إنشاء تقرير
# ============================================================

async def generate_report(
    raw_news,
    report_type,
):

    if report_type == "normal":

        prompt = f"""
أنت محلل استخباري سياسي واقتصادي محترف.

حلل الأخبار التالية الصادرة من مصادر عربية
ودولية.

المطلوب:

1. تلخيص أهم المستجدات.
2. تحديد الأطراف المعنية.
3. توضيح المواقف الرسمية.
4. التمييز بين الخبر المؤكد والتحليل.
5. عدم اختلاق أي معلومات.
6. إذا كانت البيانات غير كافية، وضح ذلك.

الأخبار الخام:

{raw_news}

اكتب التقرير باللغة العربية بأسلوب مهني
ومركز ومنظم بعناوين واضحة.
"""

    else:

        prompt = f"""
أنت محلل استخباري رفيع المستوى.

أعد تقريراً بعنوان:

تحليل مقارنة الروايات الإعلامية

اعتمد فقط على الأخبار الموجودة أدناه.

قارن بين تناول المصادر العربية
والغربية والروسية والصينية.

وضح:

- نقاط الاتفاق.
- نقاط الاختلاف.
- اختلاف صياغة الأحداث.
- المواقف الرسمية.
- الحقائق المشتركة.
- الروايات والتفسيرات المختلفة.
- المعلومات غير المؤكدة.

لا تخترع معلومات غير موجودة.

الأخبار الخام:

{raw_news}

اكتب التقرير باللغة العربية
بشكل مهني ومنظم.
"""

    response = await asyncio.to_thread(
        ai_client.models.generate_content,
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.3,
        ),
    )

    return response.text or (
        "لم يُرجع نموذج الذكاء الاصطناعي "
        "نصاً صالحاً."
    )


# ============================================================
# معالجة الأزرار
# ============================================================

async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query

    await query.answer()

    data = query.data

    # --------------------------------------------------------
    # العودة للقائمة
    # --------------------------------------------------------

    if data == "back_to_menu":

        await query.edit_message_text(
            text="اختر أحد الملفات الاستراتيجية أدناه:",
            reply_markup=get_main_keyboard(),
        )

        return

    # --------------------------------------------------------
    # حالة التحميل
    # --------------------------------------------------------

    await query.edit_message_text(
        text="📡 جاري مسح الشبكة الرصدية "
             "وسحب الأخبار بالتوازي..."
    )

    # --------------------------------------------------------
    # الأخبار
    # --------------------------------------------------------

    if data.startswith("topic_"):

        keywords = data.replace(
            "topic_",
            "",
            1,
        )

        raw_news = await fetch_all_news(
            keywords
        )

        report_type = "normal"

    elif data == "compare_all":

        raw_news = await fetch_all_news(
            "أزمة حرب اتفاق تصريح عقوبات"
        )

        report_type = "compare"

    else:
        return

    # --------------------------------------------------------
    # Gemini
    # --------------------------------------------------------

    try:

        reply_text = await generate_report(
            raw_news,
            report_type,
        )

    except Exception as e:

        logger.exception(
            "Gemini API Error"
        )

        reply_text = (
            "عذراً، حدث خطأ أثناء تحليل "
            "البيانات بواسطة Gemini.\n\n"
            f"تفاصيل الخطأ:\n{str(e)}"
        )

    # --------------------------------------------------------
    # حفظ التقرير في ذاكرة الجلسة
    # --------------------------------------------------------

    context.user_data["current_report"] = reply_text

    context.user_data["chat_history"] = []

    context.user_data["last_topic"] = data

    # --------------------------------------------------------
    # زر العودة
    # --------------------------------------------------------

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "🔙 العودة للقائمة الرئيسية",
                callback_data="back_to_menu",
            )
        ]
    ])

    await send_long_message(
        update,
        reply_text,
        query=query,
        keyboard=keyboard,
    )


# ============================================================
# المحادثة المباشرة
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
    # هل يوجد تقرير سابق؟
    # --------------------------------------------------------

    current_report = context.user_data.get(
        "current_report"
    )

    if not current_report:

        await update.message.reply_text(
            "استخدم أحد الملفات الاستراتيجية أولاً، "
            "ثم اكتب سؤالك وسأكمل معك التحليل."
            ,
            reply_markup=get_main_keyboard(),
        )

        return

    # --------------------------------------------------------
    # إظهار حالة التفكير
    # --------------------------------------------------------

    thinking_message = await update.message.reply_text(
        "🧠 جاري تحليل سؤالك..."
    )

    # --------------------------------------------------------
    # تاريخ المحادثة
    # --------------------------------------------------------

    history = context.user_data.setdefault(
        "chat_history",
        [],
    )

    # نحتفظ بآخر 6 رسائل فقط
    recent_history = history[-6:]

    conversation_text = ""

    for item in recent_history:

        conversation_text += (
            f"\nالمستخدم: {item['user']}\n"
            f"المحلل: {item['assistant']}\n"
        )

    # --------------------------------------------------------
    # Prompt
    # --------------------------------------------------------

    prompt = f"""
أنت محلل استخباري سياسي واقتصادي.

أنت الآن في محادثة متابعة مع المستخدم.

التقرير الأساسي الذي تم إنشاؤه سابقاً:

---------------- REPORT ----------------

{current_report}

-------------- END REPORT --------------

سجل المحادثة السابقة:

{conversation_text}

سؤال المستخدم الحالي:

{user_text}

المطلوب:

- أجب مباشرة عن سؤال المستخدم.
- استخدم التقرير السابق كسياق أساسي.
- لا تعيد التقرير كاملاً إلا إذا طلب المستخدم ذلك.
- إذا كان السؤال يحتاج استنتاجاً، وضح أنه استنتاج.
- لا تختلق حقائق غير موجودة في التقرير.
- إذا كانت المعلومة غير موجودة، قل بوضوح إنها غير متوفرة في البيانات الحالية.
- اجعل الإجابة باللغة العربية.
"""

    # --------------------------------------------------------
    # Gemini
    # --------------------------------------------------------

    try:

        response = await asyncio.to_thread(
            ai_client.models.generate_content,
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.3,
            ),
        )

        reply_text = response.text or (
            "لم يتم الحصول على إجابة."
        )

        # حفظ المحادثة
        history.append({
            "user": user_text,
            "assistant": reply_text,
        })

        # الاحتفاظ بآخر 6 تبادلات
        context.user_data["chat_history"] = (
            history[-6:]
        )

    except Exception as e:

        logger.exception(
            "Gemini conversation error"
        )

        reply_text = (
            "حدث خطأ أثناء معالجة سؤالك.\n\n"
            f"{str(e)}"
        )

    # --------------------------------------------------------
    # حذف رسالة التفكير
    # --------------------------------------------------------

    try:

        await thinking_message.delete()

    except Exception:

        pass

    # --------------------------------------------------------
    # إرسال الإجابة
    # --------------------------------------------------------

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "🔙 العودة للقائمة الرئيسية",
                callback_data="back_to_menu",
            )
        ]
    ])

    await send_long_message(
        update,
        reply_text,
        keyboard=keyboard,
    )


# ============================================================
# معالج الأخطاء
# ============================================================

async def error_handler(
    update,
    context: ContextTypes.DEFAULT_TYPE,
):

    logger.exception(
        "Unhandled exception:",
        exc_info=context.error,
    )


# ============================================================
# التشغيل
# ============================================================

def main():

    app = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .build()
    )

    # الأوامر
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

    # الأزرار
    app.add_handler(
        CallbackQueryHandler(
            button_handler,
            pattern=r"^(topic_|compare_all|back_to_menu)",
        )
    )

    # الرسائل النصية العادية
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_user_message,
        )
    )

    # الأخطاء
    app.add_error_handler(
        error_handler
    )

    logger.info(
        "🌐 نظام الإحاطة الاستخباراتي يعمل الآن..."
    )

    app.run_polling(
        drop_pending_updates=True
    )


if __name__ == "__main__":
    main()
