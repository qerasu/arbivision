from datetime import datetime, timezone

from sqlalchemy import select

from arbitrage_bot.core.config import settings
from arbitrage_bot.models.orm import Settings

GLOBAL_SETTINGS_KEY = "tg_alert_prefs:global"
UI_STATE_KEY_PREFIX = "tg_ui_state:"
DEFAULT_PREFERENCES = {
    "min_roi_percent": 0.1,
    "max_capital_usd": 140.0,
    "max_days_to_close": 30,
}
FIELD_LABELS = {
    "min_roi_percent": "Min ROI",
    "max_capital_usd": "Max volume",
    "max_days_to_close": "Max market end",
}
DATETIME_FIELDS = (
    "endDate",
    "end_date",
    "endTime",
    "end_time",
    "closeDate",
    "close_date",
    "closeTime",
    "close_time",
    "closedTime",
    "closed_time",
    "expiration",
    "expirationTime",
    "expiration_time",
    "expiresAt",
    "expires_at",
    "resolveDate",
    "resolve_date",
    "resolutionDate",
    "resolution_date",
)


def default_preferences():
    return dict(DEFAULT_PREFERENCES)


async def get_global_preferences(db_session):
    stmt = select(Settings).where(Settings.key == GLOBAL_SETTINGS_KEY)
    result = await db_session.execute(stmt)
    setting = result.scalars().first()
    if not setting or not isinstance(setting.value_json, dict):
        return default_preferences()
    preferences = default_preferences()
    preferences.update(setting.value_json)
    return preferences


async def get_ui_state(db_session, chat_id):
    stmt = select(Settings).where(Settings.key == _ui_state_key(chat_id))
    result = await db_session.execute(stmt)
    setting = result.scalars().first()
    if not setting or not isinstance(setting.value_json, dict):
        return None
    return setting.value_json


async def set_global_preference(db_session, field_name, field_value):
    preferences = await get_global_preferences(db_session)
    preferences[field_name] = field_value
    return await _save_global_preferences(db_session, preferences)


async def reset_global_preferences(db_session):
    preferences = {
        "min_roi_percent": None,
        "max_capital_usd": None,
        "max_days_to_close": None,
    }
    return await _save_global_preferences(db_session, preferences)


async def set_ui_state(db_session, chat_id, state):
    key = _ui_state_key(chat_id)
    stmt = select(Settings).where(Settings.key == key)
    result = await db_session.execute(stmt)
    setting = result.scalars().first()

    if setting is None:
        setting = Settings(
            key=key,
            value_json=state,
        )
        db_session.add(setting)
    else:
        setting.value_json = state
        setting.updated_at = datetime.utcnow()

    await db_session.commit()
    return state


async def clear_ui_state(db_session, chat_id):
    return await set_ui_state(db_session, chat_id, {})


async def _save_global_preferences(db_session, preferences):
    stmt = select(Settings).where(Settings.key == GLOBAL_SETTINGS_KEY)
    result = await db_session.execute(stmt)
    setting = result.scalars().first()
    if setting is None:
        setting = Settings(
            key=GLOBAL_SETTINGS_KEY,
            value_json=preferences,
        )
        db_session.add(setting)
    else:
        setting.value_json = preferences
        setting.updated_at = datetime.utcnow()

    await db_session.commit()
    return preferences


def format_preferences_text(preferences):
    min_roi = _format_percent(effective_min_roi(preferences), fallback="off")
    max_capital = _format_money(preferences.get("max_capital_usd"), fallback="off")
    max_days = _format_days(preferences.get("max_days_to_close"))
    return (
        "⚙️ Global alert settings\n\n"
        f"📈 Min ROI\nCurrent: {min_roi}\n\n"
        f"💵 Volume\nCurrent: {max_capital}\n\n"
        f"⏳ Expires\nCurrent: {max_days}"
    )


def format_home_text(preferences):
    return (
        "🔎 Arbitrage Scanner\n\n"
        "Monitors Polymarket and Predict.Fun for spread inefficiencies.\n\n"
        "🟢 Status: Active\n"
        "Filters are applied globally to all alerts.\n\n"
        "Your filters:\n"
        f"• 📈 Min ROI: {_format_percent(effective_min_roi(preferences), fallback='off')}\n"
        f"• 💵 Max volume: {_format_money(preferences.get('max_capital_usd'), fallback='off')} USD\n"
        f"• ⏳ Max market end: {_format_days(preferences.get('max_days_to_close'))}"
    )


def format_status_text(preferences):
    return (
        "📡 Arbitrage Scanner\n\n"
        "Current bot status.\n\n"
        "🟢 Status: Active\n"
        "🔄 Monitoring is running in the background.\n"
        "📬 Telegram alerts are enabled."
    )


def format_setting_prompt(field_name, preferences):
    label = FIELD_LABELS[field_name]
    current_value = _format_field_value(field_name, preferences)
    description = {
        "min_roi_percent": "Enter the minimum ROI percentage required to receive a signal.",
        "max_capital_usd": "Enter the maximum volume in USD allowed for an alert.",
        "max_days_to_close": "Enter the maximum number of days until market expiry.",
    }[field_name]

    return (
        "⚙️ Arbitrage Scanner\n\n"
        f"✏️ Change: {label}\n"
        f"→ current value: {current_value}\n\n"
        f"{description}\n\n"
        "Send `off` to disable this filter."
    )


def filter_reason_for_preferences(opportunity, market_a, market_b, preferences, now=None):
    min_roi = effective_min_roi(preferences)
    if min_roi is not None and opportunity.net_roi * 100 < float(min_roi):
        return "min_roi"

    max_capital = preferences.get("max_capital_usd")
    if max_capital is not None and opportunity.capital_required > float(max_capital):
        return "max_capital"

    max_days = preferences.get("max_days_to_close")
    if max_days is None:
        return None

    close_at = extract_pair_close_datetime(market_a, market_b)
    if close_at is None:
        return None

    reference_now = now or datetime.now(timezone.utc)
    if close_at.tzinfo is None:
        close_at = close_at.replace(tzinfo=timezone.utc)
    if reference_now.tzinfo is None:
        reference_now = reference_now.replace(tzinfo=timezone.utc)

    delta_days = (close_at - reference_now).total_seconds() / 86400
    if delta_days > float(max_days):
        return "max_days_to_close"

    return None


def effective_min_roi(preferences):
    if "min_roi_percent" not in preferences:
        return float(settings.MIN_ROI_PERCENT)

    min_roi = preferences.get("min_roi_percent")
    if min_roi is None:
        return None

    return float(min_roi)


def _format_field_value(field_name, preferences):
    if field_name == "min_roi_percent":
        return _format_percent(effective_min_roi(preferences), fallback="off")
    if field_name == "max_capital_usd":
        return f"{_format_money(preferences.get(field_name), fallback='off')} USD"
    return _format_days(preferences.get(field_name))


def extract_pair_close_datetime(market_a, market_b):
    datetimes = []
    for market in (market_a, market_b):
        raw_payload = getattr(market, "raw_payload_json", None) or {}
        parsed = _extract_market_close_datetime(raw_payload)
        if parsed is not None:
            datetimes.append(parsed)
    if not datetimes:
        return None
    return max(datetimes)


def _extract_market_close_datetime(raw_payload):
    for field_name in DATETIME_FIELDS:
        parsed = _parse_datetime_value(raw_payload.get(field_name))
        if parsed is not None:
            return parsed
    return None


def _parse_datetime_value(value):
    if value in (None, ""):
        return None

    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return _parse_datetime_value(int(text))

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_money(value, fallback):
    if value is None:
        return fallback
    rounded = round(float(value), 2)
    if rounded.is_integer():
        return f"${int(rounded)}"
    return f"${rounded:.2f}"


def _format_percent(value, fallback):
    if value is None:
        return fallback
    return f"{float(value):.2f}%"


def _format_days(value):
    if value is None:
        return "off"
    return f"{int(value)} days"


def _ui_state_key(chat_id):
    return f"{UI_STATE_KEY_PREFIX}{chat_id}"
