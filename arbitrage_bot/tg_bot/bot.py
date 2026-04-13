import asyncio
import html
import math
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


def setup_bot():
    global _shared_dp
    token = settings.TELEGRAM_BOT_TOKEN

    if not token:
        # stub for tests without token
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
    ]


async def _configure_bot_ui(bot):
    await bot.set_my_commands(_build_bot_commands())
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands())


def _format_alert_message(opportunity, pair, market_a, market_b, language=None):
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

    return (
        f"🚨 {title}\n\n"
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
            parts.append(f'<a href="{url}">{platform_label}</a>')
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

    # slug is already a full url in some cases
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


async def _send_alert(bot, alert, opportunity, pair, market_a, market_b_row, preferences=None):
    if await _is_duplicate_delivery(alert):
        alert.status = "sent"
        alert.next_retry_at = None
        alert.sent_at = datetime.now(timezone.utc)
        alert.error_message = "delivery deduped after restart"
        return

    language = _extract_language_from_preferences(preferences)

    await bot.send_message(
        chat_id=alert.telegram_chat_id,
        text=_format_alert_message(opportunity, pair, market_a, market_b_row, language=language),
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

    try:
        redis = await get_redis()
        if redis is None:
            return False
        return bool(await redis.get(_delivery_dedupe_key(alert)))
    except Exception:
        return False


async def _store_delivery_marker(alert):
    message_hash = str(getattr(alert, "message_hash", "") or "")
    chat_id = str(getattr(alert, "telegram_chat_id", "") or "")
    if not message_hash or not chat_id:
        return

    try:
        redis = await get_redis()
        if redis is None:
            return
        await redis.set(
            _delivery_dedupe_key(alert),
            "1",
            ex=_DELIVERY_DEDUPE_TTL_SECONDS,
        )
    except Exception:
        pass


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
    return type("OpportunitySnapshot", (), payload)()


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


async def send_alert_immediately(alert, opportunity, pair, market_a, market_b, preferences, directions, calculator, prepared_opportunity=None):
    bot = _get_delivery_bot()
    if bot is None:
        return False

    current_preferences = _build_runtime_preferences(preferences)
    if bool(current_preferences.get("muted")):
        alert.status = "cancelled"
        alert.next_retry_at = None
        alert.error_message = "filtered by updated preferences"
        incr_counter("telegram.alert_cancelled_preferences")
        return False

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
        return False

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
            return False

    try:
        await _send_alert(bot, alert, prepared_opportunity, pair, market_a, market_b, preferences=current_preferences)
        return True
    except Exception as exc:
        alert.attempt_count = int(getattr(alert, "attempt_count", 0) or 0) + 1
        alert.status = "failed"
        alert.next_retry_at = None
        alert.error_message = str(exc)
        incr_counter("telegram.alert_failed")
        incr_counter("telegram.alert_send_failed")
        return False


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