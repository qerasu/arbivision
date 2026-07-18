import asyncio
import html
import json
import math
from types import SimpleNamespace
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlparse
from urllib.parse import urlunparse
from datetime import datetime
from datetime import timezone
from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand
from aiogram.types import LinkPreviewOptions
from aiogram.types import MenuButtonCommands
from cachetools import TTLCache
from cachetools import TLRUCache
from sqlalchemy.exc import ProgrammingError

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.core.observability import incr_counter
from arbitrage_bot.core.redis import get_redis
from arbitrage_bot.services.system_notifier import format_error_details, send_system_error_notification
from arbitrage_bot.services.system_notifier import is_transient_network_error
from arbitrage_bot.tg_bot import handlers
from arbitrage_bot.tg_bot.localization import translate
from arbitrage_bot.tg_bot.preferences import default_preferences
from arbitrage_bot.tg_bot.preferences import extract_pair_close_datetime
from arbitrage_bot.tg_bot.preferences import filter_reason_for_preferences

log = get_logger("tg_bot")
_shared_dp = None
_shared_delivery_bot = None
_DELIVERY_DEDUPE_TTL_SECONDS = max(86400, int(settings.ALERTS_DEDUPE_TTL_SECONDS))
_EVENT_REPEAT_TTL_SECONDS = max(86400, int(settings.ALERTS_DEDUPE_TTL_SECONDS))
# ponytail: process-local fallback covers Redis outages only until restart
_delivery_dedupe_fallback = TTLCache(maxsize=5000, ttl=_DELIVERY_DEDUPE_TTL_SECONDS)
_alert_event_fallback = TLRUCache(
    maxsize=5000,
    ttu=lambda _key, value, now: now + value[1],
)


def setup_bot():
    global _shared_dp
    token = settings.TELEGRAM_BOT_TOKEN

    if not token:
        # allow tests to initialize without a real bot token
        return None, None

    bot = Bot(token=token)

    if _shared_dp is None:
        _shared_dp = Dispatcher()
        _shared_dp.include_router(handlers.router)

    return bot, _shared_dp


def _get_delivery_bot():
    global _shared_delivery_bot
    token = settings.TELEGRAM_BOT_TOKEN
    if not token:
        return None
    if _shared_delivery_bot is None:
        _shared_delivery_bot = Bot(token=token)
    return _shared_delivery_bot


def _build_bot_commands():
    return [
        BotCommand(command="start", description="open menu"),
        BotCommand(command="stats", description="show stats"),
    ]


async def _configure_bot_ui(bot):
    await bot.set_my_commands(_build_bot_commands())
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands())


def _format_alert_message(opportunity, pair, market_a, market_b, language=None, is_repeat=False):
    direction = _describe_direction(opportunity.direction, pair)
    title = html.escape(market_a.title or market_b.title or "ARBITRAGE OPPORTUNITY")
    profit = _format_money(opportunity.net_profit)
    spread = f"{opportunity.net_roi * 100:.2f}%"
    capital = _format_money(opportunity.capital_required)
    shares = _format_shares(opportunity.shares)
    leg_1_cost = _format_money(opportunity.avg_price_leg_1 * opportunity.shares)
    leg_2_cost = _format_money(opportunity.avg_price_leg_2 * opportunity.shares)
    leg_1_price = _format_leg_price_details(opportunity, 1, language=language)
    leg_2_price = _format_leg_price_details(opportunity, 2, language=language)
    volumes_ratio = _format_volumes_ratio(opportunity.avg_price_leg_1, opportunity.avg_price_leg_2, language=language)
    expires = _format_expiry_line(market_a, market_b, language=language)
    links = _format_market_links(market_a, market_b)
    repeat_notice = ""
    if is_repeat:
        repeat_notice = f"{_format_repeat_alert_notice(language)}\n\n"

    return (
        f"🚨 {title}\n\n"
        f"{repeat_notice}"
        f"💰 {translate(language, 'Profit', 'Прибыль')}: {profit}\n"
        f"📈 {translate(language, 'Spread', 'Спред')}: {spread}\n"
        f"💵 {translate(language, 'Volume', 'Объём')}: {capital}\n"
        f"{expires}\n\n"
        f"🧾 {translate(language, f'Buy {shares} shares each', f'Купить по {shares} shares')}:\n"
        f"• {direction['leg_1_label']} {translate(language, 'on', 'на')} Polymarket: {leg_1_price} = {leg_1_cost}\n"
        f"• {direction['leg_2_label']} {translate(language, 'on', 'на')} Predict.Fun: {leg_2_price} = {leg_2_cost}\n"
        f"📊 {translate(language, 'Volumes ratio', 'Соотношение объёмов')}: {volumes_ratio}x\n\n"
        f"🔗 {translate(language, 'Open markets', 'Открыть рынки')}:\n{links}"
    )


def _build_alert_digest_message(items, language=None):
    sorted_items = list(items)
    header = translate(
        language,
        f"📋 {len(sorted_items)} arbitrage opportunities",
        f"📋 Арбитражные возможности: {len(sorted_items)}",
    )
    lines = [header]
    text_length = len(header)
    shown_items = []
    for index, item in enumerate(sorted_items, start=1):
        opportunity = item["prepared_opportunity"]
        market_a = item["market_a"]
        market_b = item["market_b"]
        title = str(getattr(market_a, "title", "") or getattr(market_b, "title", "") or "Arbitrage")
        if len(title) > 60:
            title = f"{title[:57]}..."
        repeat_marker = "🔄 " if item["is_repeat"] else ""
        metrics = (
            f"📈 ROI {float(getattr(opportunity, 'net_roi', 0.0) or 0.0) * 100:.2f}% · "
            f"💰 {translate(language, 'Profit', 'Прибыль')} "
            f"{_format_money(getattr(opportunity, 'net_profit', 0.0))} · "
            f"💵 {translate(language, 'Volume', 'Объём')} "
            f"{_format_money(getattr(opportunity, 'capital_required', 0.0))}"
        )
        direction = _describe_direction(getattr(opportunity, "direction", None), item["pair"])
        shares = _format_shares(getattr(opportunity, "shares", 0.0))
        orders = translate(
            language,
            f"🧾 Buy {shares} shares each: Polymarket {html.escape(direction['leg_1_label'])} · "
            f"Predict.Fun {html.escape(direction['leg_2_label'])}",
            f"🧾 Купить по {shares} shares: Polymarket {html.escape(direction['leg_1_label'])} · "
            f"Predict.Fun {html.escape(direction['leg_2_label'])}",
        )
        links = _format_market_links(market_a, market_b)
        line = (
            f"\n<b>{index}. {repeat_marker}{html.escape(title)}</b>\n"
            f"{metrics}\n"
            f"{orders}\n"
            f"🔗 {links}"
        )
        visible_line_length = len(title) + len(metrics) + len(orders) + len("\n🔗 Polymarket | Predict.Fun")
        if shown_items and text_length + visible_line_length > 3600:
            break
        lines.append(line)
        text_length += visible_line_length
        shown_items.append(item)
    remaining_items = sorted_items[len(shown_items):]
    remaining_count = len(remaining_items)
    if remaining_count:
        lines.append(translate(
            language,
            f"\n\n…and {remaining_count} more",
            f"\n\n…и ещё {remaining_count}",
        ))
    return "".join(lines), shown_items, remaining_items


def _format_repeat_alert_notice(language=None):
    return translate(
        language,
        "🔄 Update: market state improved since your previous alert.",
        "🔄 Обновление: рыночная ситуация улучшилась с прошлого алерта.",
    )


def _describe_direction(direction, pair=None):
    mapping = getattr(pair, "outcome_mapping_json", None) or {}
    market_a = mapping.get("market_a") or {}
    market_b = mapping.get("market_b") or {}

    direction_map = {
        "A_yes_B_no": {
            "leg_1_label": market_a.get("yes_label") or "YES",
            "leg_2_label": market_b.get("no_label") or "NO",
        },
        "A_no_B_yes": {
            "leg_1_label": market_a.get("no_label") or "NO",
            "leg_2_label": market_b.get("yes_label") or "YES",
        },
    }

    return direction_map.get(
        direction,
        {
            "leg_1_label": "LEG 1",
            "leg_2_label": "LEG 2",
        },
    )


def _format_expiry_line(market_a, market_b, language=None):
    close_at = extract_pair_close_datetime(market_a, market_b)
    if close_at is None:
        return translate(language, "⏳ Ends in: Unknown", "⏳ Окончание: неизвестно")

    if close_at.tzinfo is None:
        close_at = close_at.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    remaining_days = max(
        0,
        math.ceil((close_at - now).total_seconds() / 86400),
    )

    return translate(
        language,
        f"⏳ Ends on: {close_at.date().isoformat()} (in {remaining_days} days)",
        f"⏳ Завершится: {close_at.date().isoformat()} (через {remaining_days} дн.)",
    )


def _format_market_links(market_a, market_b):
    parts = []

    for market in (market_a, market_b):
        platform_label = "Polymarket" if market.platform == "polymarket" else "Predict.Fun"
        url = _build_market_url(market)
        if url:
            parts.append(f'<a href="{html.escape(url)}">{platform_label}</a>')
        else:
            parts.append(platform_label)

    return " | ".join(parts)


def _build_market_url(market):
    platform = (market.platform or "").lower()
    slug = market.slug or ""
    raw_payload = getattr(market, "raw_payload_json", None) or {}

    for key in ("url", "marketUrl", "market_url", "shareUrl", "share_url"):
        value = raw_payload.get(key)
        if value:
            return _append_referral_params(_normalize_market_url(str(value), platform), platform)

    # some adapters already supply a complete market url as the slug
    if slug.startswith("http://") or slug.startswith("https://"):
        return _append_referral_params(slug, platform)

    if platform == "polymarket" and slug:
        return _append_referral_params(f"https://polymarket.com/market/{slug}", platform)

    if platform == "predict_fun":
        if slug:
            return _append_referral_params(f"https://predict.fun/market/{slug}", platform)
        if getattr(market, "platform_market_id", None):
            return _append_referral_params(f"https://predict.fun/market/{market.platform_market_id}", platform)

    return None


def _normalize_market_url(value, platform):
    url = str(value or "").strip()
    if not url:
        return None

    if url.startswith("http://") or url.startswith("https://"):
        return url

    if not url.startswith("/"):
        return url

    if platform == "polymarket":
        return f"https://polymarket.com{url}"

    if platform == "predict_fun":
        return f"https://predict.fun{url}"

    return url


def _append_referral_params(url, platform):
    if not url:
        return url

    parsed = urlparse(url)
    query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))

    if platform == "predict_fun":
        query_params["ref"] = "077A2"
    elif platform == "polymarket":
        query_params["r"] = "qerasuu"
    else:
        return url

    return urlunparse(parsed._replace(query=urlencode(query_params)))


def _format_money(value):
    rounded = round(float(value), 2)

    if rounded.is_integer():
        return f"${int(rounded)}"

    return f"${rounded:.2f}"


def _format_price(value):
    return f"${float(value):.3f}"


def _format_leg_price_details(opportunity, leg_index, language=None):
    avg_price = float(getattr(opportunity, f"avg_price_leg_{leg_index}", 0.0) or 0.0)
    calc_payload = getattr(opportunity, "calculation_json", None) or {}
    best_price = calc_payload.get(f"best_price_leg_{leg_index}")

    if best_price is None:
        return translate(language, f"effective price {_format_price(avg_price)}", f"эфф. цена {_format_price(avg_price)}")

    best_price_value = float(best_price)
    if abs(best_price_value - avg_price) < 0.0005:
        return translate(language, f"effective price {_format_price(avg_price)}", f"эфф. цена {_format_price(avg_price)}")

    return translate(
        language,
        f"effective price {_format_price(avg_price)} (best ask {_format_price(best_price_value)})",
        f"эфф. цена {_format_price(avg_price)} (лучший ask {_format_price(best_price_value)})",
    )


def _format_shares(value):
    rounded = round(float(value), 2)

    if rounded.is_integer():
        return str(int(rounded))

    return f"{rounded:.2f}"


def _format_volumes_ratio(price_leg_1, price_leg_2, language=None):
    p1 = float(price_leg_1)
    p2 = float(price_leg_2)
    if p1 <= 0 or p2 <= 0:
        return translate(language, "N/A", "н/д")

    if p1 > p2:
        ratio = p1 / p2
    else:
        ratio = p2 / p1

    return f"{ratio:.2f}"


def _is_missing_table_error(exc):
    if not isinstance(exc, ProgrammingError):
        return False

    sqlstate = getattr(getattr(exc, "orig", None), "sqlstate", None)
    if sqlstate == "42P01":
        return True

    details = format_error_details(exc).lower()

    return "does not exist" in details and "relation" in details


async def _send_alert(bot, alert, opportunity, pair, market_a, market_b_row, preferences=None, is_repeat=False):
    if await _is_duplicate_delivery(alert):
        alert.status = "sent"
        alert.next_retry_at = None
        alert.sent_at = datetime.now(timezone.utc)
        alert.error_message = "delivery deduped after restart"
        return

    language = _extract_language_from_preferences(preferences)

    await bot.send_message(
        chat_id=alert.telegram_chat_id,
        text=_format_alert_message(opportunity, pair, market_a, market_b_row, language=language, is_repeat=is_repeat),
        parse_mode="HTML",
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )
    await _store_delivery_marker(alert)
    alert.status = "sent"
    alert.attempt_count = int(getattr(alert, "attempt_count", 0) or 0) + 1
    alert.next_retry_at = None
    alert.sent_at = datetime.now(timezone.utc)
    alert.error_message = None
    incr_counter("telegram.alert_sent")
    incr_counter("telegram.alert_send_success")


def _delivery_dedupe_key(alert):
    message_hash = str(getattr(alert, "message_hash", "") or "")
    chat_id = str(getattr(alert, "telegram_chat_id", "") or "")
    return f"telegram-delivery:{chat_id}:{message_hash}"


async def _is_duplicate_delivery(alert):
    message_hash = str(getattr(alert, "message_hash", "") or "")
    chat_id = str(getattr(alert, "telegram_chat_id", "") or "")
    if not message_hash or not chat_id:
        return False

    fallback_marker = _delivery_dedupe_fallback.get(_delivery_dedupe_key(alert))
    try:
        redis = get_redis()
        if redis is None:
            return bool(fallback_marker)
        return bool(await redis.get(_delivery_dedupe_key(alert))) or bool(fallback_marker)
    except Exception:
        return bool(fallback_marker)


async def _load_duplicate_delivery_keys(alerts):
    keys = list(dict.fromkeys(
        _delivery_dedupe_key(alert)
        for alert in alerts
        if getattr(alert, "message_hash", None) and getattr(alert, "telegram_chat_id", None)
    ))
    duplicate_keys = {
        key
        for key in keys
        if _delivery_dedupe_fallback.get(key)
    }
    try:
        redis = get_redis()
        if redis is None or not keys:
            return duplicate_keys
        values = await redis.mget(keys) if hasattr(redis, "mget") else await asyncio.gather(
            *(redis.get(key) for key in keys)
        )
        duplicate_keys.update(key for key, value in zip(keys, values) if value)
    except Exception:
        pass
    return duplicate_keys


async def _store_delivery_marker(alert, pipeline=None):
    message_hash = str(getattr(alert, "message_hash", "") or "")
    chat_id = str(getattr(alert, "telegram_chat_id", "") or "")
    if not message_hash or not chat_id:
        return

    _delivery_dedupe_fallback[_delivery_dedupe_key(alert)] = True
    try:
        if pipeline is not None:
            pipeline.set(
                _delivery_dedupe_key(alert),
                "1",
                ex=_DELIVERY_DEDUPE_TTL_SECONDS,
            )
            return
        redis = get_redis()
        if redis is None:
            return
        await redis.set(
            _delivery_dedupe_key(alert),
            "1",
            ex=_DELIVERY_DEDUPE_TTL_SECONDS,
        )
    except Exception:
        pass


def _alert_event_state_key(alert, opportunity, pair=None):
    chat_id = str(getattr(alert, "telegram_chat_id", "") or "")
    pair_hash = str(getattr(opportunity, "pair_hash", "") or getattr(pair, "pair_hash", "") or "")
    direction = str(getattr(opportunity, "direction", "") or "")
    if not chat_id or not pair_hash or not direction:
        return None

    return f"telegram-alert-event:{chat_id}:{pair_hash}:{direction}"


def _build_alert_event_state(alert, opportunity):
    return {
        "message_hash": str(getattr(alert, "message_hash", "") or ""),
        "net_profit": float(getattr(opportunity, "net_profit", 0.0) or 0.0),
        "net_roi": float(getattr(opportunity, "net_roi", 0.0) or 0.0),
        "shares": float(getattr(opportunity, "shares", 0.0) or 0.0),
    }


def _parse_alert_event_state(raw_value):
    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None

    if not isinstance(parsed, dict):
        return None

    try:
        return {
            "message_hash": str(parsed.get("message_hash", "") or ""),
            "net_profit": float(parsed["net_profit"]),
            "net_roi": float(parsed["net_roi"]),
            "shares": float(parsed.get("shares", 0.0) or 0.0),
        }
    except (KeyError, TypeError, ValueError):
        return None


def _alert_event_ttl_seconds(market_a, market_b, now=None):
    close_at = extract_pair_close_datetime(market_a, market_b)
    if close_at is None:
        return _EVENT_REPEAT_TTL_SECONDS
    if close_at.tzinfo is None:
        close_at = close_at.replace(tzinfo=timezone.utc)

    current_time = now or datetime.now(timezone.utc)
    return max(
        _EVENT_REPEAT_TTL_SECONDS,
        math.ceil((close_at - current_time).total_seconds()),
    )


async def _load_alert_event_state(alert, opportunity, pair=None):
    chat_id = str(getattr(alert, "telegram_chat_id", "") or "")
    states = await load_alert_event_states([chat_id], opportunity, pair=pair)
    return states.get(chat_id)


async def load_alert_event_states(chat_ids, opportunity, pair=None):
    pair_hash = str(getattr(opportunity, "pair_hash", "") or getattr(pair, "pair_hash", "") or "")
    direction = str(getattr(opportunity, "direction", "") or "")
    states = await load_alert_event_states_batch(chat_ids, [opportunity], pairs=[pair])
    return states.get((pair_hash, direction), {})


async def load_alert_event_states_batch(
    chat_ids,
    opportunities,
    pairs=None,
    require_complete=False,
):
    chat_ids = {str(chat_id or "") for chat_id in chat_ids if chat_id}
    pairs = list(pairs or [])
    state_keys = {}
    raw_values = {}
    result = {}
    for index, opportunity in enumerate(opportunities):
        pair = pairs[index] if index < len(pairs) else None
        pair_hash = str(getattr(opportunity, "pair_hash", "") or getattr(pair, "pair_hash", "") or "")
        direction = str(getattr(opportunity, "direction", "") or "")
        opportunity_key = (pair_hash, direction)
        result[opportunity_key] = {}
        for chat_id in chat_ids:
            state_key = _alert_event_state_key(
                SimpleNamespace(telegram_chat_id=chat_id),
                opportunity,
                pair=pair,
            )
            if state_key is None:
                continue
            item_key = (opportunity_key, chat_id)
            state_keys[item_key] = state_key
            fallback_state = _alert_event_fallback.get(state_key)
            if fallback_state:
                raw_values[item_key] = fallback_state[0]

    batch_loaded = not state_keys
    try:
        redis = get_redis()
        if redis is not None and state_keys:
            keys = list(state_keys.values())
            if hasattr(redis, "mget"):
                redis_values = await redis.mget(keys)
            else:
                redis_values = await asyncio.gather(*(redis.get(key) for key in keys))
            batch_loaded = True
            for item_key, raw_value in zip(state_keys, redis_values):
                if raw_value:
                    raw_values[item_key] = raw_value
    except Exception:
        pass

    if require_complete and not batch_loaded:
        return {}

    for (opportunity_key, chat_id), raw_value in raw_values.items():
        state = _parse_alert_event_state(raw_value)
        if state is not None:
            result[opportunity_key][chat_id] = state
    return result


async def _store_alert_event_state(
    alert,
    event_opportunity,
    sent_opportunity,
    pair=None,
    market_a=None,
    market_b=None,
    pipeline=None,
):
    state_key = _alert_event_state_key(alert, event_opportunity, pair=pair)
    if state_key is None:
        return

    ttl_seconds = _alert_event_ttl_seconds(market_a, market_b)
    raw_state = json.dumps(_build_alert_event_state(alert, sent_opportunity))
    _alert_event_fallback[state_key] = (raw_state, ttl_seconds)
    try:
        if pipeline is not None:
            pipeline.setex(state_key, ttl_seconds, raw_state)
            return
        redis = get_redis()
        if redis is None:
            return
        await redis.setex(
            state_key,
            ttl_seconds,
            raw_state,
        )
    except Exception:
        pass


async def _execute_redis_pipeline(pipeline):
    if pipeline is None:
        return
    try:
        await pipeline.execute()
    except Exception:
        pass


def should_send_repeat_alert(last_state, alert, opportunity):
    current_message_hash = str(getattr(alert, "message_hash", "") or "")
    if current_message_hash and current_message_hash == last_state["message_hash"]:
        return False, "repeat suppressed: already notified for current market state"

    profit_diff = float(getattr(opportunity, "net_profit", 0.0) or 0.0) - last_state["net_profit"]
    roi_diff = float(getattr(opportunity, "net_roi", 0.0) or 0.0) - last_state["net_roi"]
    min_profit_delta = float(settings.ALERTS_DELTA_PROFIT_THRESHOLD_USD)
    min_roi_delta = float(settings.ALERTS_DELTA_ROI_THRESHOLD_PERCENT) / 100.0
    if profit_diff >= min_profit_delta or roi_diff >= min_roi_delta:
        return True, None

    return False, "repeat suppressed: market state change below resend threshold"


def _clone_opportunity(opportunity):
    payload = {
        "direction": getattr(opportunity, "direction", None),
        "avg_price_leg_1": getattr(opportunity, "avg_price_leg_1", 0.0),
        "avg_price_leg_2": getattr(opportunity, "avg_price_leg_2", 0.0),
        "shares": getattr(opportunity, "shares", 0.0),
        "capital_required": getattr(opportunity, "capital_required", 0.0),
        "gross_profit": getattr(opportunity, "gross_profit", 0.0),
        "net_profit": getattr(opportunity, "net_profit", 0.0),
        "gross_roi": getattr(opportunity, "gross_roi", 0.0),
        "net_roi": getattr(opportunity, "net_roi", 0.0),
        "calculation_json": getattr(opportunity, "calculation_json", None),
    }
    return SimpleNamespace(**payload)


def _recalculate_opportunity_from_directions(opportunity, directions, calculator, preferences=None):
    max_capital = None
    max_polymarket_capital = None
    max_predict_fun_capital = None
    if preferences is not None:
        max_capital = preferences.get("max_capital_usd")
        max_polymarket_capital = preferences.get("max_polymarket_capital_usd")
        max_predict_fun_capital = preferences.get("max_predict_fun_capital_usd")
    calc_results = calculator.calculate_opportunities(
        directions,
        max_capital=max_capital,
        max_polymarket_capital=max_polymarket_capital,
        max_predict_fun_capital=max_predict_fun_capital,
    )
    current_result = next(
        (
            result
            for result in calc_results
            if result.get("direction") == opportunity.direction
        ),
        None,
    )
    if current_result is None:
        return None

    snapshot = _clone_opportunity(opportunity)
    _apply_calc_result_to_opportunity(snapshot, current_result)
    return snapshot


async def send_alert_immediately(
    alert,
    opportunity,
    pair,
    market_a,
    market_b,
    preferences,
    directions,
    calculator,
    prepared_opportunity=None,
    event_state_loaded=False,
    event_state=None,
):
    bot = _get_delivery_bot()
    if bot is None:
        return False

    prepared = await _prepare_alert_delivery(
        alert,
        opportunity,
        pair,
        market_a,
        market_b,
        preferences,
        directions,
        calculator,
        prepared_opportunity=prepared_opportunity,
        event_state_loaded=event_state_loaded,
        event_state=event_state,
    )
    if prepared is None:
        return False
    prepared_opportunity, current_preferences, is_repeat = prepared

    try:
        await _send_alert(
            bot,
            alert,
            prepared_opportunity,
            pair,
            market_a,
            market_b,
            preferences=current_preferences,
            is_repeat=is_repeat,
        )
        await _store_alert_event_state(
            alert,
            opportunity,
            prepared_opportunity,
            pair=pair,
            market_a=market_a,
            market_b=market_b,
        )
        if is_repeat:
            incr_counter("telegram.alert_repeat_sent")
        return True
    except Exception as exc:
        alert.attempt_count = int(getattr(alert, "attempt_count", 0) or 0) + 1
        alert.status = "failed"
        alert.next_retry_at = None
        alert.error_message = str(exc)
        incr_counter("telegram.alert_failed")
        incr_counter("telegram.alert_send_failed")
        return False


async def send_alert_digest(items, calculator):
    bot = _get_delivery_bot()
    if bot is None:
        return []

    items = list(items)
    duplicate_keys = await _load_duplicate_delivery_keys(
        item["delivery"]["alert"]
        for item in items
    )
    prepared_items = []
    successful_opportunities = []
    duplicate_pipeline = None
    if duplicate_keys:
        try:
            redis = get_redis()
            if redis is not None and hasattr(redis, "pipeline"):
                duplicate_pipeline = redis.pipeline()
        except Exception:
            pass
    for item in items:
        delivery = item["delivery"]
        alert = delivery["alert"]
        prepared = await _prepare_alert_delivery(
            alert,
            item["opportunity"],
            item["pair"],
            item["market_a"],
            item["market_b"],
            delivery["preferences"],
            item["directions"],
            calculator,
            prepared_opportunity=delivery.get("opportunity"),
            event_state_loaded="event_state" in delivery,
            event_state=delivery.get("event_state"),
        )
        if prepared is None:
            continue
        prepared_opportunity, current_preferences, is_repeat = prepared
        if _delivery_dedupe_key(alert) in duplicate_keys:
            alert.status = "sent"
            alert.next_retry_at = None
            alert.sent_at = datetime.now(timezone.utc)
            alert.error_message = "delivery deduped after restart"
            await _store_alert_event_state(
                alert,
                item["opportunity"],
                prepared_opportunity,
                pair=item["pair"],
                market_a=item["market_a"],
                market_b=item["market_b"],
                pipeline=duplicate_pipeline,
            )
            successful_opportunities.append(item["opportunity"])
            continue
        prepared_items.append({
            **item,
            "alert": alert,
            "prepared_opportunity": prepared_opportunity,
            "current_preferences": current_preferences,
            "is_repeat": is_repeat,
        })

    if not prepared_items:
        await _execute_redis_pipeline(duplicate_pipeline)
        return successful_opportunities

    pending_items = sorted(
        prepared_items,
        key=lambda item: (
            float(getattr(item["prepared_opportunity"], "net_roi", 0.0) or 0.0),
            float(getattr(item["prepared_opportunity"], "net_profit", 0.0) or 0.0),
        ),
        reverse=True,
    )
    while pending_items:
        failed_items = pending_items
        try:
            language = _extract_language_from_preferences(pending_items[0]["current_preferences"])
            text, sent_items, remaining_items = _build_alert_digest_message(
                pending_items,
                language=language,
            )
            failed_items = sent_items + remaining_items
            await bot.send_message(
                chat_id=sent_items[0]["alert"].telegram_chat_id,
                text=text,
                parse_mode="HTML",
                link_preview_options=LinkPreviewOptions(is_disabled=True),
            )
        except Exception as exc:
            for item in failed_items:
                alert = item["alert"]
                alert.attempt_count = int(getattr(alert, "attempt_count", 0) or 0) + 1
                alert.status = "failed"
                alert.next_retry_at = None
                alert.error_message = str(exc)
            await _execute_redis_pipeline(duplicate_pipeline)
            incr_counter("telegram.digest_failed")
            return successful_opportunities

        now = datetime.now(timezone.utc)
        pipeline = duplicate_pipeline
        duplicate_pipeline = None
        if pipeline is None:
            try:
                redis = get_redis()
                if redis is not None and hasattr(redis, "pipeline"):
                    pipeline = redis.pipeline()
            except Exception:
                pass
        for item in sent_items:
            alert = item["alert"]
            await _store_delivery_marker(alert, pipeline=pipeline)
            alert.status = "sent"
            alert.attempt_count = int(getattr(alert, "attempt_count", 0) or 0) + 1
            alert.next_retry_at = None
            alert.sent_at = now
            alert.error_message = None
            await _store_alert_event_state(
                alert,
                item["opportunity"],
                item["prepared_opportunity"],
                pair=item["pair"],
                market_a=item["market_a"],
                market_b=item["market_b"],
                pipeline=pipeline,
            )
            if item["is_repeat"]:
                incr_counter("telegram.alert_repeat_sent")
            incr_counter("telegram.alert_sent")
            incr_counter("telegram.alert_send_success")
            successful_opportunities.append(item["opportunity"])
        await _execute_redis_pipeline(pipeline)
        incr_counter("telegram.digest_sent")
        pending_items = remaining_items

    return successful_opportunities


async def _prepare_alert_delivery(
    alert,
    opportunity,
    pair,
    market_a,
    market_b,
    preferences,
    directions,
    calculator,
    prepared_opportunity=None,
    event_state_loaded=False,
    event_state=None,
):
    current_preferences = _build_runtime_preferences(preferences)
    if bool(current_preferences.get("muted")):
        alert.status = "cancelled"
        alert.next_retry_at = None
        alert.error_message = "filtered by updated preferences"
        incr_counter("telegram.alert_cancelled_preferences")
        return None

    if prepared_opportunity is None:
        prepared_opportunity = _recalculate_opportunity_from_directions(
            opportunity,
            directions,
            calculator,
            preferences=current_preferences,
        )
    if prepared_opportunity is None:
        alert.status = "cancelled"
        alert.next_retry_at = None
        alert.error_message = "opportunity is no longer available"
        incr_counter("telegram.alert_cancelled_revalidation")
        return None

    if prepared_opportunity is opportunity:
        filter_reason = filter_reason_for_preferences(
            prepared_opportunity,
            market_a,
            market_b,
            current_preferences,
        )
        if filter_reason:
            alert.status = "cancelled"
            alert.next_retry_at = None
            alert.error_message = f"filtered by updated preferences: {filter_reason}"
            incr_counter("telegram.alert_cancelled_preferences")
            return None

    last_state = event_state
    if not event_state_loaded:
        last_state = await _load_alert_event_state(alert, opportunity, pair=pair)
    is_repeat = last_state is not None
    if last_state is not None:
        should_send_repeat, suppress_reason = should_send_repeat_alert(last_state, alert, prepared_opportunity)
        if not should_send_repeat:
            alert.status = "suppressed"
            alert.next_retry_at = None
            alert.error_message = suppress_reason
            incr_counter("telegram.alert_repeat_suppressed")
            return None

    return prepared_opportunity, current_preferences, is_repeat


def _apply_calc_result_to_opportunity(opportunity, calc_result):
    opportunity.avg_price_leg_1 = calc_result["avg_price_leg_1"]
    opportunity.avg_price_leg_2 = calc_result["avg_price_leg_2"]
    opportunity.shares = calc_result["shares"]
    opportunity.capital_required = calc_result["capital_required"]
    opportunity.gross_profit = calc_result["gross_profit"]
    opportunity.net_profit = calc_result["net_profit"]
    opportunity.gross_roi = calc_result["gross_roi"]
    opportunity.net_roi = calc_result["net_roi"]
    opportunity.calculation_json = calc_result


def _build_runtime_preferences(preferences):
    values = default_preferences()
    if preferences is None:
        values["muted"] = False
        return values

    if isinstance(preferences, dict):
        values.update(preferences)
        values["muted"] = bool(values.get("muted", False))
        return values

    values.update(
        {
            "min_roi_percent": preferences.min_roi_percent,
            "min_capital_usd": preferences.min_capital_usd,
            "max_capital_usd": preferences.max_capital_usd,
            "max_polymarket_capital_usd": preferences.max_polymarket_capital_usd,
            "max_predict_fun_capital_usd": preferences.max_predict_fun_capital_usd,
            "min_profit_usd": preferences.min_profit_usd,
            "min_days_to_close": preferences.min_days_to_close,
            "max_days_to_close": preferences.max_days_to_close,
            "muted": preferences.muted,
        }
    )
    return values


def _extract_language_from_preferences(preferences):
    if preferences is None:
        return None
    if isinstance(preferences, dict):
        return preferences.get("language")
    return getattr(preferences, "language", None)


async def start_polling():
    while True:
        try:
            bot, dp = setup_bot()
            if not bot or not dp:
                return

            try:
                await bot.delete_webhook(drop_pending_updates=False)
                await _configure_bot_ui(bot)
                await dp.start_polling(bot)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if is_transient_network_error(exc):
                    log.warning("polling interrupted by network issue", error=format_error_details(exc))
                else:
                    log.error("polling failed", error=format_error_details(exc))
                    try:
                        await send_system_error_notification("telegram", "start polling", exc)
                    except Exception:
                        pass
            finally:
                await bot.session.close()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("fatal error in polling loop", error=format_error_details(e))

        await asyncio.sleep(5)


async def close_shared_delivery_bot():
    global _shared_delivery_bot
    if _shared_delivery_bot is not None:
        await _shared_delivery_bot.session.close()
        _shared_delivery_bot = None


if __name__ == "__main__":
    asyncio.run(start_polling())
