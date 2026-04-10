from aiogram import F, Router
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardButton
from aiogram.types import InlineKeyboardMarkup
from sqlalchemy import func
from sqlalchemy.future import select

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.database import AsyncSessionLocal
from arbitrage_bot.core.observability import snapshot_counters
from arbitrage_bot.models.orm import Alert
from arbitrage_bot.models.orm import Subscription
from arbitrage_bot.models.orm import TelegramChat
from arbitrage_bot.models.orm import User
from arbitrage_bot.models.orm import UserPreference
from arbitrage_bot.tg_bot.localization import translate
from arbitrage_bot.tg_bot.preferences import clear_ui_state
from arbitrage_bot.tg_bot.preferences import ensure_telegram_user
from arbitrage_bot.tg_bot.preferences import format_home_text
from arbitrage_bot.tg_bot.preferences import format_preferences_text
from arbitrage_bot.tg_bot.preferences import format_setting_prompt
from arbitrage_bot.tg_bot.preferences import get_ui_state
from arbitrage_bot.tg_bot.preferences import get_user_language
from arbitrage_bot.tg_bot.preferences import get_user_preferences
from arbitrage_bot.tg_bot.preferences import reset_user_preferences
from arbitrage_bot.tg_bot.preferences import set_user_language
from arbitrage_bot.tg_bot.preferences import set_user_preference
from arbitrage_bot.tg_bot.preferences import set_ui_state
from arbitrage_bot.tg_bot.preferences import toggle_mute

router = Router()

@router.message(Command("start"))
async def cmd_start(message):
    async with AsyncSessionLocal() as session:
        await ensure_telegram_user(
            session,
            message.chat.id,
            chat_type=getattr(message.chat, "type", "private"),
        )
        language = await get_user_language(session, message.chat.id)

    if language is None:
        await message.answer(
            "🌐 Please choose your language:",
            reply_markup=_build_language_keyboard(),
        )
        return

    async with AsyncSessionLocal() as session:
        preferences = await get_user_preferences(session, message.chat.id)
        await clear_ui_state(session, message.chat.id)

    await message.answer(
        format_home_text(preferences),
        reply_markup=_build_home_keyboard(preferences, chat_id=message.chat.id),
    )



@router.callback_query(F.data.startswith("tg_lang:"))
async def on_language_callback(callback):
    language = callback.data.split(":", 1)[1]
    if language not in {"en", "ru"}:
        await _safe_answer_callback(callback)
        return

    async with AsyncSessionLocal() as session:
        preferences = await set_user_language(session, callback.message.chat.id, language)
        await clear_ui_state(session, callback.message.chat.id)

    await _safe_edit_text(
        callback,
        format_home_text(preferences),
        reply_markup=_build_home_keyboard(preferences, chat_id=callback.message.chat.id),
    )
    await _safe_answer_callback(callback)


@router.callback_query(F.data.startswith("tg_nav:"))
async def on_nav_callback(callback):
    action = callback.data.split(":", 1)[1]

    async with AsyncSessionLocal() as session:
        if action == "home":
            preferences = await get_user_preferences(session, callback.message.chat.id)
            await clear_ui_state(session, callback.message.chat.id)
            await _safe_edit_text(
                callback,
                format_home_text(preferences),
                reply_markup=_build_home_keyboard(preferences, chat_id=callback.message.chat.id),
            )
        elif action == "settings":
            preferences = await get_user_preferences(session, callback.message.chat.id)
            await clear_ui_state(session, callback.message.chat.id)
            await _safe_edit_text(
                callback,
                format_preferences_text(preferences),
                reply_markup=_build_settings_keyboard(preferences),
            )
        elif action == "toggle_mute":
            preferences = await toggle_mute(session, callback.message.chat.id)
            await clear_ui_state(session, callback.message.chat.id)
            lang = preferences.get("language")
            muted = preferences.get("muted", False)
            toast = (
                translate(lang, "⏸ Alerts paused", "⏸ Алерты поставлены на паузу")
                if muted
                else translate(lang, "▶️ Alerts resumed", "▶️ Алерты снова включены")
            )
            await _safe_edit_text(
                callback,
                format_home_text(preferences),
                reply_markup=_build_home_keyboard(preferences, chat_id=callback.message.chat.id),
            )
            try:
                await callback.answer(toast, show_alert=False)
                return
            except TelegramBadRequest:
                pass
        elif action == "stats":
            if not _is_admin_chat(callback.message.chat.id):
                await _safe_answer_callback(callback)
                return
            stats = await _load_admin_stats(session)
            await clear_ui_state(session, callback.message.chat.id)
            await _safe_edit_text(
                callback,
                _format_admin_stats_text(stats),
                reply_markup=_build_stats_keyboard(),
            )
        elif action == "reset":
            preferences = await reset_user_preferences(session, callback.message.chat.id)
            await clear_ui_state(session, callback.message.chat.id)
            lang = preferences.get("language")
            await _safe_edit_text(
                callback,
                translate(
                    lang,
                    "Your settings were reset.\n\n"
                    "All Telegram filters are disabled for your chat, so you will receive every alert that passes system checks.\n\n",
                    "Ваши настройки сброшены.\n\n"
                    "Все Telegram-фильтры для этого чата отключены, поэтому вы будете получать все алерты, которые проходят системные проверки.\n\n",
                ) +
                f"{format_preferences_text(preferences)}",
                reply_markup=_build_settings_keyboard(preferences),
            )

    await _safe_answer_callback(callback)


@router.callback_query(F.data.startswith("tg_edit:"))
async def on_edit_callback(callback):
    field_name = callback.data.split(":", 1)[1]

    async with AsyncSessionLocal() as session:
        preferences = await get_user_preferences(session, callback.message.chat.id)
        await set_ui_state(
            session,
            callback.message.chat.id,
            {
                "mode": "awaiting_value",
                "field_name": field_name,
                "prompt_message_id": callback.message.message_id,
            },
        )

    await _safe_edit_text(
        callback,
        format_setting_prompt(field_name, preferences),
        reply_markup=_build_prompt_keyboard(preferences),
    )
    await _safe_answer_callback(callback)


@router.message()
async def on_plain_text_setting(message):
    text = (message.text or "").strip()
    if text == "/start":
        return

    async with AsyncSessionLocal() as session:
        ui_state = await get_ui_state(session, message.chat.id)

        if ui_state and ui_state.get("mode") == "awaiting_value":
            if not text:
                language = await get_user_language(session, message.chat.id)
                await message.answer(translate(language, "Enter a number or `off`.", "Введите число или `выкл`."))
                return

            field_name = ui_state.get("field_name")
            preferences = await get_user_preferences(session, message.chat.id)
            lang = preferences.get("language")
            try:
                value = _parse_setting_value(field_name, text, language=lang)
            except ValueError as exc:
                await message.answer(str(exc))
                return

            await _apply_setting_update(message, field_name, value)
            return

        if not await _has_started_bot(session, message.chat.id):
            language = await get_user_language(session, message.chat.id)
            await message.answer(_format_inactive_chat_text(language=language))
            return

        preferences = await get_user_preferences(session, message.chat.id)
        await clear_ui_state(session, message.chat.id)

    await message.answer(
        _format_unhandled_message_text(preferences),
        reply_markup=_build_home_keyboard(preferences, chat_id=message.chat.id),
    )


async def _apply_setting_update(message, field_name, value):
    async with AsyncSessionLocal() as session:
        ui_state = await get_ui_state(session, message.chat.id)
        preferences = await set_user_preference(session, message.chat.id, field_name, value)
        await clear_ui_state(session, message.chat.id)

    lang = preferences.get("language")
    prompt_message_id = None
    if ui_state and ui_state.get("mode") == "awaiting_value":
        prompt_message_id = ui_state.get("prompt_message_id")

    text = (
        f"{_settings_success_label(field_name, language=lang)} "
        f"{translate(lang, 'updated to', 'обновлён до')} "
        f"{_format_success_value(field_name, value, language=lang)}.\n\n"
        f"{format_preferences_text(preferences)}"
    )

    if prompt_message_id is not None:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=prompt_message_id,
                text=text,
                reply_markup=_build_settings_keyboard(preferences),
            )
            await _safe_delete_message(message)
            return
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return

    await message.answer(
        text,
        reply_markup=_build_settings_keyboard(preferences),
    )
    await _safe_delete_message(message)


def _build_language_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🇬🇧 English",
                    callback_data="tg_lang:en",
                ),
                InlineKeyboardButton(
                    text="🇷🇺 Русский",
                    callback_data="tg_lang:ru",
                ),
            ],
        ]
    )


def _build_home_keyboard(preferences=None, chat_id=None):
    lang = (preferences or {}).get("language")
    muted = (preferences or {}).get("muted", False)
    toggle_text = (
        translate(lang, "▶️ Resume", "▶️ Возобновить")
        if muted
        else translate(lang, "⏸ Pause", "⏸ Пауза")
    )
    rows = [
        [
            InlineKeyboardButton(
                text=toggle_text,
                callback_data="tg_nav:toggle_mute",
            ),
        ],
        [
            InlineKeyboardButton(
                text=translate(lang, "Settings", "Настройки"),
                callback_data="tg_nav:settings",
            ),
        ],
    ]

    if _is_admin_chat(chat_id):
        rows.append(
            [
                InlineKeyboardButton(
                    text="Stats",
                    callback_data="tg_nav:stats",
                ),
            ]
        )

    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_settings_keyboard(preferences=None):
    lang = (preferences or {}).get("language")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"→ {translate(lang, 'Min ROI', 'Мин. ROI')}",
                    callback_data="tg_edit:min_roi_percent",
                ),
                InlineKeyboardButton(
                    text=f"→ {translate(lang, 'Min volume', 'Мин. объём')}",
                    callback_data="tg_edit:min_capital_usd",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"→ {translate(lang, 'Max volume', 'Макс. объём')}",
                    callback_data="tg_edit:max_capital_usd",
                ),
                InlineKeyboardButton(
                    text=f"→ {translate(lang, 'Min profit', 'Мин. прибыль')}",
                    callback_data="tg_edit:min_profit_usd",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"→ {translate(lang, 'Polymarket balance', 'Баланс Polymarket')}",
                    callback_data="tg_edit:max_polymarket_capital_usd",
                ),
                InlineKeyboardButton(
                    text=f"→ {translate(lang, 'Predict.Fun balance', 'Баланс Predict.Fun')}",
                    callback_data="tg_edit:max_predict_fun_capital_usd",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"→ {translate(lang, 'Max market end', 'Макс. срок рынка')}",
                    callback_data="tg_edit:max_days_to_close",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=translate(lang, "Reset all", "Сбросить всё"),
                    callback_data="tg_nav:reset",
                ),
                InlineKeyboardButton(
                    text=translate(lang, "← Back", "← Назад"),
                    callback_data="tg_nav:home",
                ),
            ],
        ]
    )


def _build_prompt_keyboard(preferences=None):
    lang = (preferences or {}).get("language")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=translate(lang, "← Back", "← Назад"),
                    callback_data="tg_nav:settings",
                ),
            ]
        ]
    )


def _build_stats_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Refresh",
                    callback_data="tg_nav:stats",
                ),
                InlineKeyboardButton(
                    text="← Back",
                    callback_data="tg_nav:home",
                ),
            ]
        ]
    )


def _parse_setting_value(field_name, raw_value, language=None):
    value = raw_value.strip().lower()
    if value in {"off", "выкл"}:
        return None

    if field_name == "min_roi_percent":
        try:
            parsed = float(value)
        except (ValueError, TypeError):
            raise ValueError(translate(language, "Enter a number, e.g. 1.5", "Введите число, например 1.5"))
        if parsed < 0:
            raise ValueError(translate(language, "ROI must be zero or greater.", "ROI должен быть не меньше нуля."))
        return parsed

    if field_name in {"min_capital_usd", "max_capital_usd", "max_polymarket_capital_usd", "max_predict_fun_capital_usd", "min_profit_usd"}:
        try:
            parsed = float(value)
        except (ValueError, TypeError):
            raise ValueError(translate(language, "Enter a number, e.g. 50", "Введите число, например 50"))
        if parsed <= 0:
            raise ValueError(translate(language, "Value must be greater than zero.", "Значение должно быть больше нуля."))
        return parsed

    if field_name == "max_days_to_close":
        try:
            parsed = int(value)
        except (ValueError, TypeError):
            raise ValueError(translate(language, "Enter a whole number, e.g. 30", "Введите целое число, например 30"))
        if parsed <= 0:
            raise ValueError(translate(language, "Max market end must be greater than zero days.", "Макс. срок рынка должен быть больше нуля дней."))
        return parsed

    raise ValueError(translate(language, "Unsupported setting.", "Неподдерживаемая настройка."))


async def _safe_edit_text(callback, text, reply_markup):
    try:
        await callback.message.edit_text(
            text,
            reply_markup=reply_markup,
        )
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            raise


async def _safe_delete_message(message):
    try:
        await message.delete()
    except TelegramBadRequest:
        return


async def _safe_answer_callback(callback):
    try:
        await callback.answer()
    except TelegramBadRequest as exc:
        error_text = str(exc).lower()
        if "query is too old" not in error_text and "query id is invalid" not in error_text:
            raise


def _format_success_value(field_name, value, language=None):
    if value is None:
        return translate(language, "off", "выкл")

    if field_name == "min_roi_percent":
        return f"{float(value):.2f}%"

    if field_name in {"min_capital_usd", "max_capital_usd", "max_polymarket_capital_usd", "max_predict_fun_capital_usd", "min_profit_usd"}:
        return f"${float(value):.0f}"

    if field_name == "max_days_to_close":
        return translate(language, f"{int(value)} days", f"{int(value)} дн.")

    return str(value)


def _settings_success_label(field_name, language=None):
    ru_labels = {
        "min_roi_percent": "Мин. ROI",
        "min_capital_usd": "Мин. объём",
        "max_capital_usd": "Макс. объём",
        "max_polymarket_capital_usd": "Баланс Polymarket",
        "max_predict_fun_capital_usd": "Баланс Predict.Fun",
        "min_profit_usd": "Мин. прибыль",
        "max_days_to_close": "Макс. срок рынка",
    }
    return translate(language, {
        "min_roi_percent": "Min ROI",
        "min_capital_usd": "Min volume",
        "max_capital_usd": "Max volume",
        "max_polymarket_capital_usd": "Polymarket balance",
        "max_predict_fun_capital_usd": "Predict.Fun balance",
        "min_profit_usd": "Min profit",
        "max_days_to_close": "Max market end",
    }[field_name], ru_labels[field_name])


def _is_admin_chat(chat_id):
    if chat_id is None:
        return False
    return str(chat_id) in settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS


async def _has_started_bot(db_session, chat_id):
    stmt = select(TelegramChat.id).where(TelegramChat.chat_id == str(chat_id))
    result = await db_session.execute(stmt)
    return result.first() is not None


def _format_inactive_chat_text(language=None):
    return translate(language, "Press /start to activate the bot and open the menu.", "Нажмите /start, чтобы активировать бота и открыть меню.")


def _format_unhandled_message_text(preferences):
    lang = preferences.get("language")
    return (
        f"{translate(lang, 'Use the buttons below to control the bot.', 'Управляйте ботом через кнопки ниже.')}\n"
        f"{translate(lang, 'To change filters, open Settings and enter a number only after selecting a field.', 'Чтобы изменить фильтры, откройте Настройки и вводите число только после выбора нужного поля.')}\n\n"
        f"{format_home_text(preferences)}"
    )


async def _load_admin_stats(db_session):
    users_stmt = (
        select(
            func.count(func.distinct(TelegramChat.chat_id)).label("total"),
            func.count(func.distinct(TelegramChat.chat_id)).filter(UserPreference.muted.is_(True)).label("paused"),
        )
        .select_from(TelegramChat)
        .join(User, TelegramChat.user_id == User.id)
        .outerjoin(UserPreference, UserPreference.user_id == User.id)
        .where(
            User.status == "active",
        )
    )
    alerts_stmt = select(
        func.count(Alert.id).filter(Alert.status == "sent").label("sent"),
        func.count(Alert.id).filter(Alert.status.in_(("cancelled", "failed"))).label("dropped"),
    )
    reasons_stmt = (
        select(
            Alert.error_message,
            func.count(Alert.id).label("count"),
        )
        .where(
            Alert.status.in_(("cancelled", "failed")),
            Alert.error_message.is_not(None),
        )
        .group_by(Alert.error_message)
        .order_by(func.count(Alert.id).desc(), Alert.error_message.asc())
        .limit(8)
    )

    users_row = (await db_session.execute(users_stmt)).one()
    alerts_row = (await db_session.execute(alerts_stmt)).one()
    reason_rows = (await db_session.execute(reasons_stmt)).all()
    runtime_metrics = snapshot_counters()

    total_users = int(users_row.total or 0)
    paused_users = int(users_row.paused or 0)
    active_users = max(0, total_users - paused_users)

    runtime_drop_reasons = {}
    for key, value in sorted(runtime_metrics.items()):
        if key.startswith("fanout.drop."):
            runtime_drop_reasons[key.removeprefix("fanout.drop.")] = int(value)
        elif key == "telegram.alert_cancelled_preferences":
            runtime_drop_reasons["cancelled_preferences"] = int(value)
        elif key == "telegram.alert_cancelled_revalidation":
            runtime_drop_reasons["stale_after_revalidation"] = int(value)
        elif key == "telegram.alert_send_failed":
            runtime_drop_reasons["send_failed"] = int(value)

    return {
        "users": {
            "total": total_users,
            "active": active_users,
            "paused": paused_users,
        },
        "alerts": {
            "sent": int(alerts_row.sent or 0),
            "dropped": int(alerts_row.dropped or 0),
        },
        "alert_drop_reasons": [
            {
                "reason": str(error_message),
                "count": int(count),
            }
            for error_message, count in reason_rows
        ],
        "runtime_drop_reasons": runtime_drop_reasons,
    }


def _format_admin_stats_text(stats):
    users = stats["users"]
    alerts = stats["alerts"]
    lines = [
        "📊 Bot stats",
        "",
        "👥 Users:",
        f"• 🧮 Total: {users['total']}",
        f"• ✅ Active: {users['active']}",
        f"• ⏸ Paused: {users['paused']}",
        "",
        "🚨 Alerts:",
        f"• 📤 Sent: {alerts['sent']}",
        f"• 🗑 Dropped: {alerts['dropped']}",
    ]

    alert_drop_reasons = stats.get("alert_drop_reasons") or []
    if alert_drop_reasons:
        lines.extend(
            [
                "",
                "🧾 Drop reasons (all time):",
            ]
        )
        for item in alert_drop_reasons:
            lines.append(f"• {item['reason']}: {item['count']}")

    runtime_drop_reasons = stats.get("runtime_drop_reasons") or {}
    if runtime_drop_reasons:
        lines.extend(
            [
                "",
                "⚙️ Drop reasons (since restart):",
            ]
        )
        for reason, count in runtime_drop_reasons.items():
            lines.append(f"• {reason}: {count}")

    return "\n".join(lines)