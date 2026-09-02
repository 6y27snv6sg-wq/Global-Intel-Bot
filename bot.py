import asyncio
import os
import aiohttp
import feedparser
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, CallbackQueryHandler
from google import genai
from google.genai import types

# 🔒 قراءة المفاتيح من متغيرات البيئة بدلاً من التشفير الصلب
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not TELEGRAM_BOT_TOKEN or not GEMINI_API_KEY:
    raise ValueError("ERROR: TELEGRAM_BOT_TOKEN or GEMINI_API_KEY is missing!")

ai_client = genai.Client(api_key=GEMINI_API_KEY)

GLOBAL_FEEDS = {
    "🇸🇦 وكالة الأنباء السعودية (واس)": "https://www.spa.gov.sa/rss.php",
    "🇶🇦 وكالة الأنباء القطرية (قنا)": "https://www.qna.org.qa/Rss/News",
    "🇨🇳 وكالة أنباء الصين (شينخوا)": "http://www.xinhuanet.com/english/rss/worldrss.xml",
    "🇺🇸 رويترز (دولية وعربية)": "https://www.reutersagency.com/feed/?best-topics=political-general&post_type=best",
    "🇷🇺 وكالة أنباء روسيا (TASS)": "https://tass.com/rss/v2.xml",
    "🇫🇷 فرانس 24 (أوروبية/عربية)": "https://www.france24.com/ar/rss",
    "🇬🇧 بي بي سي عربي (بريطانية)": "http://feeds.bbci.co.uk/arabic/rss.xml"
}

async def fetch_single_feed(session, source_name, url, keywords_list):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        async with session.get(url, headers=headers, timeout=6) as response:
            if response.status != 200:
                return ""
            content = await response.text()
            feed = feedparser.parse(content)
            
            extracted = ""
            for entry in feed.entries[:4]:
                title = entry.get('title', '')
                summary = entry.get('summary', '')
                full_text = f"{title} {summary}"
                
                if any(word.lower() in full_text.lower() for word in keywords_list):
                    extracted += f"\n[المصدر: {source_name}] - {title}\nالتفاصيل: {summary[:200]}...\n"
            return extracted
    except Exception:
        return ""

async def fetch_all_news(keywords):
    search_words = keywords.split()
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_single_feed(session, source, url, search_words) for source, url in GLOBAL_FEEDS.items()]
        results = await asyncio.gather(*tasks)
    
    collected_text = "".join(results)
    return collected_text if collected_text else "لم يتم العثور على تغطية مباشرة بالكلمات المفتاحية المطلوبة في النشرة الأخيرة."

def get_main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛢️ ملف الطاقة والنفط والأسواق", callback_data="topic_طاقة نفط اقتصاد اسعار")],
        [InlineKeyboardButton("🛡️ ملف الصراعات والتوترات العسكرية", callback_data="topic_صراعات حرب جيوسياسة جيش")],
        [InlineKeyboardButton("💱 البنوك المركزية والسياسة المالية", callback_data="topic_بنك فدرالي فائدة عملات"),
         InlineKeyboardButton("🌐 تصريحات وزارات الخارجية", callback_data="topic_خارجية سفير بيان رسمي")],
        [InlineKeyboardButton("⚡ تحليل مقارن شامل (عربي ودولي)", callback_data="compare_all")]
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "مرحباً بك في نظام موجز الأحداث الدولية الإحاطة الاستخباراتي الذكي 🌐🏛️\n"
        "اختر أحد الملفات الاستراتيجية أدناه:",
        reply_markup=get_main_keyboard()
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    await query.edit_message_text(text="📡 جاري مسح الشبكة الرصدية وسحب الأخبار بالتوازي...")

    if data.startswith("topic_"):
        keywords = data.replace("topic_", "", 1)
        raw_news = await fetch_all_news(keywords)
        
        prompt = f"""
        أنت محلل استخباري سياسي واقتصادي. قم بتحليل الأخبار التالية الصادرة من وكالات عربية ودولية، وصيغ تقريراً احترافياً دقيقاً باللغة العربية:
        
        الأخبار الخام:
        {raw_news}
        
        اكتب تقريراً مركزاً يوضح المستجدات، الأطراف المعنية، والمواقف الرسمية.
        """
    elif data == "compare_all":
        raw_news = await fetch_all_news("أزمة حرب اتفاق تصريح عقوبات")
        
        prompt = f"""
        أنت محلل استخباري رفيع المستوى. قم بإعداد "تقرير مقارنة الروايات (Conflict of Narrative)" بناءً على الأخبار الواردة من الوكالات العربية والوكالات العالمية:
        
        الأخبار الخام:
        {raw_news}
        
        وضح للمستخدم: كيف تتناول المصادر العربية الرسمية الحدث مقارنة بالتناول الغربي والروسي والصيني؟ ما هي نقاط الالتقاء والاختلاف بين الروايات؟
        """

    try:
        response = ai_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.3)
        )
        reply_text = response.text
    except Exception as e:
        reply_text = f"عذراً، حدث خطأ أثناء معالجة البيانات: {str(e)}"

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="back_to_menu")]])

    if len(reply_text) > 4000:
        chunks = [reply_text[i:i+4000] for i in range(0, len(reply_text), 4000)]
        await query.edit_message_text(text=chunks[0])
        for chunk in chunks[1:-1]:
            await context.bot.send_message(chat_id=query.message.chat_id, text=chunk)
        await context.bot.send_message(chat_id=query.message.chat_id, text=chunks[-1], reply_markup=keyboard)
    else:
        await query.edit_message_text(text=reply_text, reply_markup=keyboard)

async def back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(text="اختر أحد الملفات الاستراتيجية أدناه:", reply_markup=get_main_keyboard())

if __name__ == '__main__':
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler, pattern="^topic_|^compare_all$"))
    app.add_handler(CallbackQueryHandler(back_to_menu, pattern="^back_to_menu$"))
    
    print("🌐 نظام الإحاطة الاستخباراتي يعمل الآن...")
    app.run_polling()
