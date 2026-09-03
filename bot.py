import asyncio
import logging
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from google import genai
from google.genai import types
from news_engine import collect_news, search_news, build_ai_context

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("LiveNewsBot")

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()
GEMINI_MODEL = "gemini-3.5-flash"
NEWS_COLLECTION_TIMEOUT = 20
MAX_SEARCH_RESULTS = 15

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is missing.")
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY is missing.")

ai_client = genai.Client(api_key=GEMINI_API_KEY)
USER_LOCKS = {}

def get_user_lock(user_id):
    if user_id not in USER_LOCKS:
        USER_LOCKS[user_id] = asyncio.Lock()
    return USER_LOCKS[user_id]

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

def main_keyboard():
    k = [
        [
            InlineKeyboardButton("🚨 عاجل", callback_data="topic:urgent"),
            InlineKeyboardButton("🌍 العالم", callback_data="topic:world"),
        ],
        [
            InlineKeyboardButton("🇸🇦 الخليج", callback_data="topic:gulf"),
            InlineKeyboardButton("🇺🇸 أمريكا", callback_data="topic:america"),
        ],
        [
            InlineKeyboardButton("🇪🇺 أوروبا", callback_data="topic:europe"),
            InlineKeyboardButton("🌏 آسيا", callback_data="topic:asia"),
        ],
        [
            InlineKeyboardButton("⛽ الطاقة", callback_data="topic:energy"),
            InlineKeyboardButton("🛡 الأمن", callback_data="topic:security"),
        ],
        [InlineKeyboardButton("🌐 السياسة الخارجية", callback_data="topic:foreign")],
        [InlineKeyboardButton("🔄 مقارنة", callback_data="compare")],
    ]
    return InlineKeyboardMarkup(k)

def back_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ رجوع", callback_data="back")]
    ])

REPORT_STYLE_RULES = """
قواعد الأسلوب:
- ابدأ بالمعلومة مباشرة.
- كن مختصرًا ودقيقًا.
- لا تستخدم مقدمات إنشائية أو عبارات فلسفية.
- لا تكرر المعلومات.
- لا تضف كلامًا لزيادة الطول.
- لا تختلق معلومات أو مصادر أو أرقامًا.
- لا تحول الاحتمال إلى حقيقة.
- لا تقدم رأيًا شخصيًا أو توقعات غير مدعومة.
- التحليل فقط إذا كان مدعومًا بالمعلومات.
- إذا لم تكفِ المعلومات، قل ذلك بوضوح.
- لا تستخدم عبارات مثل: في ظل التطورات المتسارعة، مما لا شك فيه، الأيام القادمة ستكشف.
- لا تختم بعبارات إنشائية.
"""

async def ask_gemini(prompt):
    logger.info("Starting Gemini request...")
    try:
        response = await asyncio.wait_for(
            asyncio.to_thread(
                ai_client.models.generate_content,
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(thinking_level="low")
                ),
            ),
            timeout=45,
        )
        text = getattr(response, "text", None)
        if not text:
            return "لم توجد إجابة كافية من البيانات المتاحة."
        logger.info("Gemini request completed.")
        return text.strip()
    except asyncio.TimeoutError:
        logger.error("Gemini request timed out.")
        return "تعذر إكمال التحليل الذكي حاليًا بسبب بطء خدمة التحليل."
    except Exception as exc:
        logger.exception("Gemini request failed: %s", exc)
        return "تعذر تحليل الأخبار بالذكاء الاصطناعي حاليًا."

async def get_fresh_news():
    logger.info("Starting news collection...")
    try:
        news = await asyncio.wait_for(
            collect_news(max_items=100),
            timeout=NEWS_COLLECTION_TIMEOUT,
        )
        logger.info("News collection completed: %d items", len(news))
        return news
    except asyncio.TimeoutError:
        logger.error("News collection timed out after %s seconds.", NEWS_COLLECTION_TIMEOUT)
    except Exception as exc:
        logger.exception("News collection failed: %s", exc)
    return []

async def generate_report(topic, news_items):
    if not news_items:
        return "لا توجد أخبار حديثة كافية لإعداد التقرير."

    context = build_ai_context(news_items)
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
    return await ask_gemini(prompt)

async def send_long_message(update, text, reply_markup=None):
    text = text or "لم يتم إنشاء محتوى."
    chunks = [text[i:i + 3900] for i in range(0, len(text), 3900)] or ["لم يتم إنشاء محتوى."]
    for i, chunk in enumerate(chunks):
        markup = reply_markup if i == len(chunks) - 1 else None
        if update.callback_query:
            await update.callback_query.message.reply_text(chunk, reply_markup=markup)
        elif update.message:
            await update.message.reply_text(chunk, reply_markup=markup)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text(
            "مرحبًا بك في نظام استخبارات الأخبار.\n\nاختر المجال الذي تريد تحليله:",
            reply_markup=main_keyboard(),
        )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    await query.answer()
    user_id = query.from_user.id
    data = query.data
    lock = get_user_lock(user_id)

    if lock.locked():
        await query.answer("يوجد تحليل جارٍ بالفعل، انتظر قليلًا.", show_alert=True)
        return

    async with lock:
        if data == "back":
            await query.edit_message_text(
                "اختر المجال الذي تريد تحليله:",
                reply_markup=main_keyboard(),
            )
            return

        if data == "compare":
            await query.edit_message_text(
                "أرسل موضوعين أو حدثين للمقارنة بينهما.\n\n"
                "مثال:\nقارن بين تطورات الطاقة في الخليج وأوروبا.",
                reply_markup=back_keyboard(),
            )
            return

        if not data.startswith("topic:"):
            return

        topic_key = data.split(":", 1)[1]
        topic = TOPICS.get(topic_key)

        if not topic:
            await query.edit_message_text(
                "الموضوع غير معروف.",
                reply_markup=back_keyboard(),
            )
            return

        await query.edit_message_text("📡 جاري فحص الأخبار الحديثة ثم تحليلها...")
        fresh = await get_fresh_news()

        try:
            selected = search_news(
                fresh,
                topic,
                max_results=MAX_SEARCH_RESULTS,
            )
        except Exception as exc:
            logger.exception("Topic search failed: %s", exc)
            selected = []

        if len(selected) < 3:
            selected = sorted(
                fresh,
                key=lambda x: getattr(x, "importance", 0),
                reverse=True,
            )[:MAX_SEARCH_RESULTS]

        if not selected:
            await query.edit_message_text(
                "لا توجد أخبار كافية حاليًا.",
                reply_markup=back_keyboard(),
            )
            return

        await query.edit_message_text("🧠 تم جمع الأخبار.\n\nجاري التحليل المختصر...")
        report = await generate_report(topic, selected)

        try:
            await query.edit_message_text("📊 تم إعداد التقرير.")
        except Exception:
            pass

        await send_long_message(
            update,
            report,
            reply_markup=back_keyboard(),
        )

async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user_id = update.effective_user.id
    user_text = update.message.text.strip()
    if not user_text:
        return

    lock = get_user_lock(user_id)

    if lock.locked():
        await update.message.reply_text("يوجد تحليل جارٍ بالفعل، انتظر حتى يكتمل.")
        return

    async with lock:
        status = await update.message.reply_text(
            "📡 جاري البحث عن أخبار مرتبطة بسؤالك..."
        )

        fresh = await get_fresh_news()

        try:
            selected = search_news(
                fresh,
                user_text,
                max_results=MAX_SEARCH_RESULTS,
            )
        except Exception as exc:
            logger.exception("Question search failed: %s", exc)
            selected = []

        logger.info(
            "Selected %d relevant items for query '%s'",
            len(selected),
            user_text,
        )

        if not selected:
            await status.edit_text(
                "لم أجد في الأخبار الحالية معلومات مرتبطة بموضوع سؤالك.\n\n"
                "لن أخلط أخبارًا غير مرتبطة بالسؤال لإعطاء إجابة مصطنعة."
            )
            return

        context_text = build_ai_context(selected)

        prompt = f"""
أنت محلل أخبار واستخبارات مفتوحة المصدر.

سؤال المستخدم:
{user_text}

استخدم الأخبار المرتبطة بالسؤال فقط.

مهم:
- لا تستخدم أخبارًا خارج السياق.
- لا تخترع معلومات.
- لا تفترض أن كل خبر يجيب عن السؤال.
- إذا لم تكفِ المعلومات، قل ذلك بوضوح.
- لا تحول التحليل إلى توقعات غير مدعومة.
- لا تكرر الخبر.

{REPORT_STYLE_RULES}

ابدأ بالإجابة المباشرة.

الخلاصة:
2 إلى 3 جمل.

أهم ما ورد:
• نقاط مختصرة.

التحليل:
فقط إذا كان مفيدًا ومدعومًا.

ملاحظة:
فقط عند وجود نقص أو عدم تأكد مهم.

الأخبار:
{context_text}
"""

        await status.edit_text(
            "🧠 وجدت أخبارًا مرتبطة بالسؤال.\n\nجاري تحليلها باختصار..."
        )

        answer = await ask_gemini(prompt)

        try:
            await status.delete()
        except Exception:
            pass

        await send_long_message(
            update,
            answer,
            reply_markup=back_keyboard(),
        )

async def post_init(application):
    logger.info("Verifying Telegram connection...")
    bot_info = await application.bot.get_me()
    logger.info("Telegram connection verified: @%s", bot_info.username)

def main():
    logger.info("Starting Live News Intelligence Bot...")

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command))
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

    logger.info("Starting Telegram polling...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
