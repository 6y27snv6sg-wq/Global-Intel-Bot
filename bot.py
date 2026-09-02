# =========================================================
# تقسيم النص مع الحفاظ على الكلمات وبنية Markdown قدر الإمكان
# =========================================================

def split_text_safely(
    text,
    max_length=3900
):
    """
    تقسيم النص إلى أجزاء مناسبة لحد Telegram.
    يحاول التقسيم بالترتيب التالي:
    1. فواصل الأسطر.
    2. المسافات بين الكلمات.
    3. التقسيم الإجباري عند الحاجة.

    كما يحاول عدم تقسيم كتل Markdown المغلقة
    مثل ``` أو علامات التنسيق الشائعة.
    """

    if not text:
        return [
            "لم يصل رد من النظام."
        ]

    text = str(text).strip()

    if len(text) <= max_length:
        return [text]

    chunks = []
    remaining = text

    while len(remaining) > max_length:

        candidate = remaining[:max_length]

        # أولوية التقسيم عند نهاية فقرة
        split_at = candidate.rfind("\n\n")

        # ثم عند نهاية سطر
        if split_at < max_length // 3:
            split_at = candidate.rfind("\n")

        # ثم عند مسافة بين الكلمات
        if split_at < max_length // 3:
            split_at = candidate.rfind(" ")

        # إذا لم نجد موضعًا مناسبًا، نقسم إجباريًا
        if split_at <= 0:
            split_at = max_length

        chunk = remaining[:split_at].rstrip()
        remaining = remaining[split_at:].lstrip()

        if chunk:
            chunks.append(chunk)

    if remaining:
        chunks.append(remaining)

    # محاولة موازنة كتل Markdown البرمجية بين الأجزاء.
    # Telegram قد يرفض الرسالة إذا احتوت على Markdown غير مكتمل.
    balanced_chunks = []
    code_block_open = False

    for chunk in chunks:

        if code_block_open:
            chunk = "```\n" + chunk

        fence_count = chunk.count("```")

        if fence_count % 2 == 1:
            chunk = chunk + "\n```"
            code_block_open = not code_block_open

        balanced_chunks.append(chunk)

    return balanced_chunks


# =========================================================
# إرسال آمن للرسائل الطويلة
# =========================================================

async def send_long_message(
    update,
    text,
    query=None,
    keyboard=None
):
    """
    إرسال نص طويل بأمان.

    - يلتزم بحد Telegram للرسائل النصية.
    - لا يعتمد على edit_message_text وحدها.
    - إذا فشل تعديل الرسالة، يرسل رسالة جديدة.
    - يقسم النص عند فواصل مناسبة دون قطع الكلمات قدر الإمكان.
    - يرسل لوحة المفاتيح في آخر رسالة فقط.
    - يحاول أولًا الإرسال كنص عادي لتجنب أخطاء Markdown.
    """

    max_length = 3900

    chunks = split_text_safely(
        text,
        max_length=max_length
    )

    bot = update.get_bot()
    chat_id = update.effective_chat.id

    async def send_new_message(
        message_text,
        reply_markup=None
    ):
        return await bot.send_message(
            chat_id=chat_id,
            text=message_text,
            reply_markup=reply_markup
        )

    # إذا كان الرد ناتجًا عن زر، نحاول تعديل رسالة الحالة.
    # عند الفشل نرسل رسالة جديدة بدل توقف الدالة.
    if query and chunks:

        try:
            await query.edit_message_text(
                text=chunks[0]
            )

        except Exception as error:
            logger.warning(
                "Failed to edit Telegram message; "
                "sending a new message instead: %s",
                error
            )

            try:
                await send_new_message(
                    chunks[0]
                )

            except Exception as send_error:
                logger.error(
                    "Failed to send fallback Telegram message: %s",
                    send_error
                )

                return

        # إرسال الأجزاء الوسطى
        for chunk in chunks[1:-1]:

            try:
                await send_new_message(
                    chunk
                )

            except Exception as error:
                logger.error(
                    "Failed to send message chunk: %s",
                    error
                )

                # محاولة إرسال الجزء نفسه دون أي لوحة مفاتيح
                try:
                    await send_new_message(
                        "تعذر إرسال جزء من التقرير."
                    )
                except Exception:
                    pass

                return

        # إرسال الجزء الأخير مع لوحة المفاتيح
        if len(chunks) > 1:

            try:
                await send_new_message(
                    chunks[-1],
                    reply_markup=keyboard
                )

            except Exception as error:
                logger.error(
                    "Failed to send final message chunk: %s",
                    error
                )

                try:
                    await send_new_message(
                        chunks[-1]
                    )
                except Exception:
                    pass

        elif keyboard:

            try:
                await send_new_message(
                    "يمكنك الآن متابعة السؤال مباشرة.",
                    reply_markup=keyboard
                )

            except Exception as error:
                logger.error(
                    "Failed to send follow-up keyboard: %s",
                    error
                )

        return

    # الإرسال العادي عندما لا توجد CallbackQuery
    for index, chunk in enumerate(chunks):

        is_last = index == len(chunks) - 1

        try:
            await send_new_message(
                chunk,
                reply_markup=(
                    keyboard
                    if is_last
                    else None
                )
            )

        except Exception as error:
            logger.error(
                "Failed to send Telegram message chunk: %s",
                error
            )

            # محاولة أخيرة برسالة مختصرة
            try:
                await send_new_message(
                    "تعذر إرسال الرد كاملًا بسبب خطأ في Telegram."
                )
            except Exception:
                pass

            return
