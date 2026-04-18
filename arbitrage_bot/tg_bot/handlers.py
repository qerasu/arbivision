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
from arbitrage_bot.tg_bot.preferences import get_setting_label
from arbitrage_bot.tg_bot.preferences import get_user_language
from arbitrage_bot.tg_bot.preferences import get_user_preferences
from arbitrage_bot.tg_bot.preferences import iter_settings_fields
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
                reply_markup=_build_settings_keyboard(preferences, chat_id=callback.message.chat.id),
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
                    "All Telegram filters are disabled for this chat.\n\n"
                    "You will receive every alert that passes system checks.\n\n",
                    "Ваши настройки сброшены.\n\n"
                    "Для этого чата отключены все Telegram-фильтры.\n\n"
                    "Вы будете получать все алерты, которые проходят системные проверки.\n\n",
                ) +
                f"{format_preferences_text(preferences)}",
                reply_markup=_build_settings_keyboard(preferences, chat_id=callback.message.chat.id),
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

            await _apply_setting_update(message, field_name, value, ui_state)
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


async def _apply_setting_update(message, field_name, value, ui_state):
    async with AsyncSessionLocal() as session:
        preferences = await set_user_preference(session, message.chat.id, field_name, value)
        await clear_ui_state(session, message.chat.id)

    lang = preferences.get("language")
    prompt_message_id = None
    if ui_state and ui_state.get("mode") == "awaiting_value":
        prompt_message_id = ui_state.get("prompt_message_id")

    text = (
        f"{get_setting_label(field_name, language=lang)} "
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
                reply_markup=_build_settings_keyboard(preferences, chat_id=message.chat.id),
            )
            await _safe_delete_message(message)
            return
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return

    await message.answer(
        text,
        reply_markup=_build_settings_keyboard(preferences, chat_id=message.chat.id),
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


def _build_settings_keyboard(preferences=None, chat_id=None):
    lang = (preferences or {}).get("language")
    fields = list(iter_settings_fields())
    rows = []
    for index in range(0, len(fields), 2):
        chunk = fields[index:index + 2]
        row = []
        for field in chunk:
            row.append(
                InlineKeyboardButton(
                    text=f"→ {get_setting_label(field['name'], language=lang)}",
                    callback_data=f"tg_edit:{field['name']}",
                )
            )
        rows.append(row)
    rows.append(
        [
            InlineKeyboardButton(
                text=translate(lang, "Disable all", "Отключить всё"),
                callback_data="tg_nav:reset",
            ),
            InlineKeyboardButton(
                text=translate(lang, "← Back", "← Назад"),
                callback_data="tg_nav:home",
            ),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


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

    if field_name in {"min_days_to_close", "max_days_to_close"}:
        try:
            parsed = int(value)
        except (ValueError, TypeError):
            raise ValueError(translate(language, "Enter a whole number, e.g. 30", "Введите целое число, например 30"))
        if parsed <= 0:
            if field_name == "min_days_to_close":
                raise ValueError(translate(language, "Min market end must be greater than zero days.", "Мин. срок рынка должен быть больше нуля дней."))
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

    if field_name in {"min_days_to_close", "max_days_to_close"}:
        return translate(language, f"{int(value)} days", f"{int(value)} дн.")

    return str(value)


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
    users_row = (await db_session.execute(users_stmt)).one()
    runtime_metrics = snapshot_counters()

    total_users = int(users_row.total or 0)
    paused_users = int(users_row.paused or 0)
    active_users = max(0, total_users - paused_users)

    runtime_alert_drop_reasons = {}
    runtime_opportunity_filter_reasons = {}
    for key, value in sorted(runtime_metrics.items()):
        if key.startswith("fanout.drop."):
            runtime_opportunity_filter_reasons[key.removeprefix("fanout.drop.")] = int(value)
        elif key == "telegram.alert_cancelled_preferences":
            runtime_alert_drop_reasons["cancelled_by_updated_preferences"] = int(value)
        elif key == "telegram.alert_cancelled_revalidation":
            runtime_alert_drop_reasons["cancelled_after_revalidation"] = int(value)
        elif key == "telegram.alert_send_failed":
            runtime_alert_drop_reasons["send_failed"] = int(value)
        elif key == "telegram.alert_repeat_suppressed":
            runtime_alert_drop_reasons["repeat_suppressed"] = int(value)

    runtime_sent = int(runtime_metrics.get("telegram.alert_sent", 0))
    runtime_dropped = sum(runtime_alert_drop_reasons.values())

    return {
        "users": {
            "total": total_users,
            "active": active_users,
            "paused": paused_users,
        },
        "alerts": {
            "sent": runtime_sent,
            "dropped": runtime_dropped,
        },
        "alert_drop_reasons": [],
        "runtime_alert_drop_reasons": runtime_alert_drop_reasons,
        "runtime_opportunity_filter_reasons": runtime_opportunity_filter_reasons,
    }


def _format_admin_stats_text(stats):
    users = stats["users"]
    alerts = stats["alerts"]

    runtime_opportunity_filter_reasons = stats.get("runtime_opportunity_filter_reasons") or {}
    total_filtered = sum(runtime_opportunity_filter_reasons.values())

    lines = [
        "📊 Bot stats",
        "",
        "👥 Users:",
        f"• 🧮 Total: {users['total']}",
        f"• ✅ Active: {users['active']}",
        f"• ⏸ Paused: {users['paused']}",
        "",
        "🚨 Alerts:",
        "• Runtime:",
        f"• 📤 Sent: {alerts['sent']}",
        f"• 🗑 Dropped: {alerts['dropped']}",
        f"• 🧹 Filtered: {total_filtered}",
    ]

    alert_drop_reasons = stats.get("alert_drop_reasons") or []
    if alert_drop_reasons:
        lines.extend(
            [
                "",
                "🧾 Alert cancellations/failures:",
            ]
        )
        for item in alert_drop_reasons:
            lines.append(f"• {item['reason']}: {item['count']}")

    runtime_alert_drop_reasons = stats.get("runtime_alert_drop_reasons") or {}
    if runtime_alert_drop_reasons:
        lines.extend(
            [
                "",
                "⚙️ Delivery cancellations (since restart (хуй)):",
            ]
        )
        for reason, count in runtime_alert_drop_reasons.items():
            lines.append(f"• {reason}: {count}")

    if runtime_opportunity_filter_reasons:
        lines.extend(
            [
                "",
                "🧹 Fanout filter blocks (since restart):",
            ]
        )
        for reason, count in runtime_opportunity_filter_reasons.items():
            lines.append(f"• {reason}: {count}")

    return "\n".join(lines)