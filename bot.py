async def handle_user_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    text = (
        update.message.text or ""
    ).strip()

    if not text:
        return

    user_id = update.effective_user.id

    lock = USER_LOCKS.setdefault(
        user_id,
        asyncio.Lock(),
    )

    if lock.locked():
        await update.message.reply_text(
            "⏳ جاري البحث..."
        )
        return

    async with lock:

        status = await update.message.reply_text(
            "🔎 جاري البحث العالمي عن:\n"
            f"<b>{html.escape(text)}</b>...",
            parse_mode="HTML",
        )

        try:

            # =================================================
            # المرحلة الأولى:
            # البحث داخل الأخبار الموجودة حاليًا
            # =================================================

            items = await get_fresh_news()

            words = [
                word
                for word in re.split(
                    r"\s+",
                    text,
                )
                if len(word.strip()) >= 2
            ]

            results = search_news(
                items,
                words or [text],
                max_results=MAX_SEARCH_RESULTS,
            )

            # =================================================
            # المرحلة الثانية:
            # إذا لم تكن النتائج كافية،
            # نبدأ بحثًا جديدًا مستقلًا عبر الإنترنت
            # =================================================

            if len(results) < 3:

                online_results = (
                    await search_news_online(
                        text,
                        max_results=MAX_SEARCH_RESULTS,
                    )
                )

                if online_results:

                    combined = (
                        results
                        + online_results
                    )

                    results = search_news(
                        combined,
                        words or [text],
                        max_results=MAX_SEARCH_RESULTS,
                    )

            # =================================================
            # لا يوجد شيء
            # =================================================

            if not results:

                await status.edit_text(
                    "🔎 لم أجد نتائج مطابقة "
                    "لطلبك حاليًا."
                )

                return

            # =================================================
            # التقرير
            # =================================================

            report = generate_base_report(
                results,
                page=1,
                per_page=5,
            )

            await status.edit_text(
                report,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🔙 الرئيسية",
                                callback_data="home",
                            )
                        ]
                    ]
                ),
                disable_web_page_preview=True,
                parse_mode="HTML",
            )

        except Exception:

            log.exception(
                "Independent Search Exception"
            )

            await status.edit_text(
                "⚠️ حدث خطأ أثناء البحث. "
                "حاول مرة أخرى."
            )
