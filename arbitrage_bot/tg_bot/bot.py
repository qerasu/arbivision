import asyncio
import math
from datetime import datetime
from datetime import timezone
from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand
from aiogram.types import MenuButtonCommands
from sqlalchemy import select
from sqlalchemy.orm import aliased

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.database import AsyncSessionLocal
from arbitrage_bot.models.orm import Alert, ArbOpportunity, Market, MarketPair
from arbitrage_bot.services.system_notifier import format_error_details, send_system_error_notification
from arbitrage_bot.tg_bot import handlers
from arbitrage_bot.tg_bot.preferences import extract_pair_close_datetime


def setup_bot():
    token = settings.TELEGRAM_BOT_TOKEN

    if not token:
        # stub for tests without token
        return None, None

    bot = Bot(token=token)
    dp = Dispatcher()

    dp.include_router(handlers.router)
    return bot, dp


def _build_bot_commands():
    return [
        BotCommand(command="start", description="open the main screen"),
        BotCommand(command="status", description="show current bot status"),
        BotCommand(command="settings", description="open global alert settings"),
        BotCommand(command="reset", description="reset filters"),
    ]


async def _configure_bot_ui(bot):
    await bot.set_my_commands(_build_bot_commands())
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands())


def _format_alert_message(opportunity, pair, market_a, market_b):
    direction = _describe_direction(opportunity.direction)
    title = market_a.title or market_b.title or "Arbitrage Opportunity"
    profit = _format_money(opportunity.net_profit)
    spread = f"{opportunity.net_roi * 100:.2f}%"
    capital = _format_money(opportunity.capital_required)
    shares = _format_shares(opportunity.shares)
    leg_1_cost = _format_money(opportunity.avg_price_leg_1 * opportunity.shares)
    leg_2_cost = _format_money(opportunity.avg_price_leg_2 * opportunity.shares)
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
        f"• {direction['leg_2_label']} on Predict.Fun @ {_format_price(opportunity.avg_price_leg_2)} = {leg_2_cost}\n\n"
        f"🔗 Open markets:\n{links}"
    )


def _describe_direction(direction):
    mapping = {
        "A_yes_B_no": {
            "leg_1_label": "YES",
            "leg_2_label": "NO",
        },
        "A_no_B_yes": {
            "leg_1_label": "NO",
            "leg_2_label": "YES",
        },
    }

    return mapping.get(
        direction,
        {
            "leg_1_label": "LEG 1",
            "leg_2_label": "LEG 2",
        },
    )


def _format_expiry_line(market_a, market_b):
    close_at = extract_pair_close_datetime(market_a, market_b)
    if close_at is None:
        return "⏳ Expires: Unknown"

    if close_at.tzinfo is None:
        close_at = close_at.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    remaining_days = max(
        0,
        math.ceil((close_at - now).total_seconds() / 86400),
    )
    return f"⏳ Expires: {close_at.date().isoformat()} ({remaining_days} days)"


def _format_market_links(market_a, market_b):
    labels = []

    for market in (market_a, market_b):
        platform_label = "Polymarket" if market.platform == "polymarket" else "Predict.Fun"
        url = _build_market_url(market)
        if url:
            labels.append(f"{platform_label}: {url}")
        else:
            labels.append(f"{platform_label}: unavailable")

    return "\n".join(labels)


def _build_market_url(market):
    platform = (market.platform or "").lower()
    slug = market.slug or ""
    raw_payload = getattr(market, "raw_payload_json", None) or {}

    for key in ("url", "marketUrl", "market_url", "shareUrl", "share_url"):
        value = raw_payload.get(key)
        if value:
            return str(value)

    if platform == "polymarket" and slug:
        return f"https://polymarket.com/event/{slug}"

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


async def _drain_queued_alerts(bot):
    market_b = aliased(Market)

    while True:
        try:
            async with AsyncSessionLocal() as session:
                stmt = (
                    select(Alert, ArbOpportunity, MarketPair, Market, market_b)
                    .join(ArbOpportunity, Alert.opportunity_id == ArbOpportunity.id)
                    .join(MarketPair, ArbOpportunity.market_pair_id == MarketPair.id)
                    .join(Market, MarketPair.market_id_a == Market.id)
                    .join(market_b, MarketPair.market_id_b == market_b.id)
                    .where(Alert.status == "queued")
                    .order_by(Alert.id)
                    .limit(20)
                )
                result = await session.execute(stmt)
                rows = result.all()

                for alert, opportunity, pair, market_a, market_b_row in rows:
                    try:
                        await bot.send_message(
                            chat_id=alert.telegram_chat_id,
                            text=_format_alert_message(
                                opportunity,
                                pair,
                                market_a,
                                market_b_row,
                            ),
                        )
                        alert.status = "sent"
                        alert.error_message = None
                    except Exception as exc:
                        alert.status = "failed"
                        alert.error_message = str(exc)
                    await session.commit()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"telegram alert loop error: {format_error_details(exc)}")
            await send_system_error_notification("telegram", "alert loop", exc)

        await asyncio.sleep(settings.TELEGRAM_ALERTS_POLL_SECONDS)


async def start_polling():
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
        print(f"telegram polling failed: {format_error_details(exc)}")
        await send_system_error_notification("telegram", "start polling", exc)
    finally:
        sender_task.cancel()
        await asyncio.gather(sender_task, return_exceptions=True)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(start_polling())
