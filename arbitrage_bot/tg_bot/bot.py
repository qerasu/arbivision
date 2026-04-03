import asyncio
import html
import math
from datetime import datetime
from datetime import timezone
from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand
from aiogram.types import LinkPreviewOptions
from aiogram.types import MenuButtonCommands
from sqlalchemy import select
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import aliased

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.database import AsyncSessionLocal
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.models.orm import Alert, ArbOpportunity, Market, MarketPair
from arbitrage_bot.services.system_notifier import format_error_details, send_system_error_notification
from arbitrage_bot.tg_bot import handlers
from arbitrage_bot.tg_bot.preferences import extract_pair_close_datetime

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
        BotCommand(command="status", description="show current bot status"),
        BotCommand(command="settings", description="open global alert settings"),
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
            return str(value)

    # slug is already a full url in some cases
    if slug.startswith("http://") or slug.startswith("https://"):
        return slug

    if platform == "polymarket" and slug:
        return f"https://polymarket.com/market/{slug}"

    if platform == "predict_fun":
        if slug:
            return f"https://predict.fun/market/{slug}"
        if getattr(market, "platform_market_id", None):
            return f"https://predict.fun/market/{market.platform_market_id}"

    return None


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
    alert.sent_at = datetime.now(timezone.utc)
    alert.error_message = None


async def _drain_queued_alerts(bot):
    market_b_alias = aliased(Market)

    while True:
        try:
            async with AsyncSessionLocal() as session:
                stmt = (
                    select(Alert, ArbOpportunity, MarketPair, Market, market_b_alias)
                    .join(ArbOpportunity, Alert.opportunity_id == ArbOpportunity.id)
                    .join(MarketPair, ArbOpportunity.market_pair_id == MarketPair.id)
                    .join(Market, MarketPair.market_id_a == Market.id)
                    .join(market_b_alias, MarketPair.market_id_b == market_b_alias.id)
                    .where(Alert.status.in_(["queued", "retry"]))
                    .order_by(Alert.id)
                    .limit(20)
                )
                result = await session.execute(stmt)
                rows = result.all()

                for alert, opportunity, pair, market_a, market_b_row in rows:
                    try:
                        await _send_alert(bot, alert, opportunity, pair, market_a, market_b_row)
                    except Exception as exc:
                        if alert.status == "retry":
                            alert.status = "failed"
                        else:
                            alert.status = "retry"
                        alert.error_message = str(exc)
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