async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    try:
        await query.answer()
    except Exception:
        log.exception("Callback answer error")

    data = query.data or ""

    if not data.startswith("topic:"):
        log.warning("Unknown callback: %s", data)
        return

    key = data.split(":", 1)[1]

    if key not in TOPICS:
        log.warning("Unknown topic key: %s", key)
        return

    _, topic_text = TOPICS[key]

    user_id = update.effective_user.id
    lock = USER_LOCKS.setdefault(user_id, asyncio.Lock())

    if lock.locked():
        try:
            await query.message.reply_text(
                "⏳ يوجد طلب قيد المعالجة. انتظر اكتماله."
            )
        except Exception:
            log.exception("Busy message error")
        return

    # نؤكد للمستخدم فوراً أن الزر استجاب
    try:
        await query.edit_message_text(
            f"📡 جاري البحث عن:\n{topic_text}\n\n"
            "قد يستغرق جمع الأخبار بضع ثوانٍ..."
        )
    except Exception:
        log.exception("Status edit error")

    async with lock:
        try:
            items = await get_fresh_news()

            if not items:
                await query.message.reply_text(
                    "⚠️ تعذر الوصول إلى مصادر الأخبار حالياً."
                )
                return

            results = search_news(
                items,
                topic_text,
                max_results=MAX_SEARCH_RESULTS,
            )

            if not results:
                await query.message.reply_text(
                    f"🔎 لا توجد أخبار مرتبطة بـ {topic_text} حالياً."
                )
                return

            report = await generate_report(topic_text, results)

            await send_callback_message(query, report)

        except asyncio.CancelledError:
            raise

        except Exception:
            log.exception(
                "Button handler error | user=%s | topic=%s",
                user_id,
                key,
            )

            try:
                await query.message.reply_text(
                    "⚠️ حدث خطأ مؤقت أثناء معالجة الطلب."
                )
            except Exception:
                log.exception("Error message failed")
