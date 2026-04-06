import asyncio
import html
import math
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlparse
from urllib.parse import urlunparse
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand
from aiogram.types import LinkPreviewOptions
from aiogram.types import MenuButtonCommands
from sqlalchemy import or_, select
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import aliased

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.database import AsyncSessionLocal
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.core.observability import incr_counter
from arbitrage_bot.models.orm import Alert, ArbOpportunity, Market, MarketPair, UserPreference
from arbitrage_bot.services.calculator import ArbitrageCalculator
from arbitrage_bot.services.orderbook import OrderbookService
from arbitrage_bot.services.system_notifier import format_error_details, send_system_error_notification
from arbitrage_bot.tg_bot import handlers
from arbitrage_bot.tg_bot.preferences import default_preferences
from arbitrage_bot.tg_bot.preferences import extract_pair_close_datetime
from arbitrage_bot.tg_bot.preferences import filter_reason_for_preferences

log = get_logger("tg_bot")
_shared_dp = None


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


def _build_bot_commands():
    return [
        BotCommand(command="start", description="open menu"),
    ]


async def _configure_bot_ui(bot):
    await bot.set_my_commands(_build_bot_commands())
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands())


def _format_alert_message(opportunity, pair, market_a, market_b):
    direction = _describe_direction(opportunity.direction, pair)
    title = html.escape(market_a.title or market_b.title or "ARBITRAGE OPPORTUNITY")
    profit = _format_money(opportunity.net_profit)
    spread = f"{opportunity.net_roi * 100:.2f}%"
    capital = _format_money(opportunity.capital_required)
    shares = _format_shares(opportunity.shares)
    leg_1_cost = _format_money(opportunity.avg_price_leg_1 * opportunity.shares)
    leg_2_cost = _format_money(opportunity.avg_price_leg_2 * opportunity.shares)
    volumes_ratio = _format_volumes_ratio(opportunity.avg_price_leg_1, opportunity.avg_price_leg_2)
    expires = _format_expiry_line(market_a, market_b)
    links = _format_market_links(market_a, market_b)

    return (
        f"🚨 {title}\n\n"
        f"💰 Profit: {profit}\n"
        f"📈 Spread: {spread}\n"
        f"💵 Volume: {capital}\n"
        f"{expires}\n\n"
        f"🧾 Buy {shares} shares each:\n"
        f"• {direction['leg_1_label']} on Polymarket @ {_format_price(opportunity.avg_price_leg_1)} = {leg_1_cost}\n"
        f"• {direction['leg_2_label']} on Predict.Fun @ {_format_price(opportunity.avg_price_leg_2)} = {leg_2_cost}\n"
        f"📊 Volumes ratio: {volumes_ratio}x\n\n"
        f"🔗 Open markets:\n{links}"
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


def _format_expiry_line(market_a, market_b):
    close_at = extract_pair_close_datetime(market_a, market_b)
    if close_at is None:
        return "⏳ Ends in: Unknown"

    if close_at.tzinfo is None:
        close_at = close_at.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    remaining_days = max(
        0,
        math.ceil((close_at - now).total_seconds() / 86400),
    )

    return f"⏳ Ends on: {close_at.date().isoformat()} (in {remaining_days} days)"


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
            return _append_referral_params(str(value), platform)

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


def _format_shares(value):
    rounded = round(float(value), 2)

    if rounded.is_integer():
        return str(int(rounded))

    return f"{rounded:.2f}"


def _format_volumes_ratio(price_leg_1, price_leg_2):
    p1 = float(price_leg_1)
    p2 = float(price_leg_2)
    if p1 <= 0 or p2 <= 0:
        return "N/A"

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


async def _send_alert(bot, alert, opportunity, pair, market_a, market_b_row):
    await bot.send_message(
        chat_id=alert.telegram_chat_id,
        text=_format_alert_message(opportunity, pair, market_a, market_b_row),
        parse_mode="HTML",
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )
    alert.status = "sent"
    alert.attempt_count = int(getattr(alert, "attempt_count", 0) or 0) + 1
    alert.next_retry_at = None
    alert.sent_at = datetime.now(timezone.utc)
    alert.error_message = None
    incr_counter("telegram.alert_sent")


def _mark_alert_retry(alert, exc, now):
    attempt_count = int(getattr(alert, "attempt_count", 0) or 0) + 1
    alert.attempt_count = attempt_count
    alert.error_message = str(exc)

    if attempt_count >= settings.TELEGRAM_DELIVERY_MAX_ATTEMPTS:
        alert.status = "failed"
        alert.next_retry_at = None
        incr_counter("telegram.alert_failed")
        return

    alert.status = "retry"
    alert.next_retry_at = now + timedelta(seconds=settings.TELEGRAM_DELIVERY_RETRY_SECONDS)
    incr_counter("telegram.alert_retry")


def _apply_calc_result_to_opportunity(opportunity, calc_result):
    opportunity.price_leg_1 = calc_result["avg_price_leg_1"]
    opportunity.price_leg_2 = calc_result["avg_price_leg_2"]
    opportunity.avg_price_leg_1 = calc_result["avg_price_leg_1"]
    opportunity.avg_price_leg_2 = calc_result["avg_price_leg_2"]
    opportunity.shares = calc_result["shares"]
    opportunity.capital_required = calc_result["capital_required"]
    opportunity.gross_profit = calc_result["gross_profit"]
    opportunity.net_profit = calc_result["net_profit"]
    opportunity.gross_roi = calc_result["gross_roi"]
    opportunity.net_roi = calc_result["net_roi"]
    opportunity.calculation_json = calc_result


async def _revalidate_alert_opportunity(session, opportunity, pair, orderbook_service, calculator):
    orderbooks_data = await orderbook_service.fetch_orderbooks_for_pairs([pair], session)
    if not orderbooks_data:
        return False

    current_item = orderbooks_data[0]
    calc_results = calculator.calculate_opportunities(current_item.get("directions"))
    if not calc_results:
        return False

    current_result = next(
        (
            result
            for result in calc_results
            if result.get("direction") == opportunity.direction
        ),
        None,
    )
    if current_result is None:
        return False

    _apply_calc_result_to_opportunity(opportunity, current_result)
    return True


async def _claim_deliverable_alert(session, now):
    market_b_alias = aliased(Market)
    stmt = (
        select(Alert, ArbOpportunity, MarketPair, Market, market_b_alias, UserPreference)
        .join(ArbOpportunity, Alert.opportunity_id == ArbOpportunity.id)
        .join(MarketPair, ArbOpportunity.market_pair_id == MarketPair.id)
        .join(Market, MarketPair.market_id_a == Market.id)
        .join(market_b_alias, MarketPair.market_id_b == market_b_alias.id)
        .outerjoin(UserPreference, Alert.user_id == UserPreference.user_id)
        .where(
            Alert.status.in_(["queued", "retry"]),
            or_(Alert.next_retry_at.is_(None), Alert.next_retry_at <= now),
        )
        .order_by(Alert.id)
        .limit(1)
        .with_for_update(skip_locked=True, of=Alert)
    )
    result = await session.execute(stmt)
    row = result.first()
    if row is None:
        return None

    alert, _, _, _, _, _ = row
    alert.status = "processing"
    return row


def _build_runtime_preferences(preferences):
    values = default_preferences()
    if preferences is None:
        values["muted"] = False
        return values

    values.update(
        {
            "min_roi_percent": preferences.min_roi_percent,
            "max_capital_usd": preferences.max_capital_usd,
            "max_days_to_close": preferences.max_days_to_close,
            "muted": preferences.muted,
        }
    )
    return values


def _should_skip_alert_for_current_preferences(alert, opportunity, market_a, market_b, preferences):
    if getattr(alert, "user_id", None) is None:
        return False

    current_preferences = _build_runtime_preferences(preferences)
    if current_preferences.get("muted"):
        return True

    return bool(
        filter_reason_for_preferences(
            opportunity,
            market_a,
            market_b,
            current_preferences,
        )
    )


async def _drain_queued_alerts(bot):
    orderbook_service = OrderbookService()
    calculator = ArbitrageCalculator()
    try:
        while True:
            try:
                async with AsyncSessionLocal() as session:
                    for _ in range(20):
                        now = datetime.now(timezone.utc)
                        row = await _claim_deliverable_alert(session, now)
                        if row is None:
                            break

                        alert, opportunity, pair, market_a, market_b_row, preferences = row
                        if _should_skip_alert_for_current_preferences(
                            alert,
                            opportunity,
                            market_a,
                            market_b_row,
                            preferences,
                        ):
                            alert.status = "cancelled"
                            alert.next_retry_at = None
                            alert.error_message = "filtered by updated preferences"
                            incr_counter("telegram.alert_cancelled_preferences")
                            await session.commit()
                            continue
                        try:
                            is_still_valid = await _revalidate_alert_opportunity(
                                session,
                                opportunity,
                                pair,
                                orderbook_service,
                                calculator,
                            )
                        except Exception as exc:
                            _mark_alert_retry(alert, exc, now)
                            incr_counter("telegram.revalidation_error")
                            await session.commit()
                            continue
                        if not is_still_valid:
                            alert.status = "cancelled"
                            alert.next_retry_at = None
                            alert.error_message = "opportunity is no longer available"
                            incr_counter("telegram.alert_cancelled_revalidation")
                            await session.commit()
                            continue
                        try:
                            await _send_alert(bot, alert, opportunity, pair, market_a, market_b_row)
                        except Exception as exc:
                            _mark_alert_retry(alert, exc, now)
                        await session.commit()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if _is_missing_table_error(exc):
                    log.info("waiting for database migrations")
                    await asyncio.sleep(settings.TELEGRAM_ALERTS_POLL_SECONDS)
                    continue
                log.error("alert loop error", error=format_error_details(exc))
                await send_system_error_notification("telegram", "alert loop", exc)

            await asyncio.sleep(settings.TELEGRAM_ALERTS_POLL_SECONDS)
    finally:
        await orderbook_service.close()


async def start_polling():
    while True:
        try:
            bot, dp = setup_bot()
            if not bot or not dp:
                return

            sender_task = asyncio.create_task(_drain_queued_alerts(bot))
            try:
                await bot.delete_webhook(drop_pending_updates=False)
                await _configure_bot_ui(bot)
                await dp.start_polling(bot)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("polling failed", error=format_error_details(exc))
                try:
                    await send_system_error_notification("telegram", "start polling", exc)
                except Exception:
                    pass
            finally:
                sender_task.cancel()
                await asyncio.gather(sender_task, return_exceptions=True)
                await bot.session.close()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("fatal error in polling loop", error=format_error_details(e))

        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(start_polling())