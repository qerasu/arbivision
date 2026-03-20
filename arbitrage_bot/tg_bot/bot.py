import asyncio
from aiogram import Bot, Dispatcher
from sqlalchemy import select
from sqlalchemy.orm import aliased

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.database import AsyncSessionLocal
from arbitrage_bot.models.orm import Alert, ArbOpportunity, Market, MarketPair
from arbitrage_bot.tg_bot import handlers


def setup_bot():
    token = settings.TELEGRAM_BOT_TOKEN
    
    if not token:
        # stub for tests without token
        return None, None
        
    bot = Bot(token=token)
    dp = Dispatcher()
    
    dp.include_router(handlers.router)
    return bot, dp


def _format_alert_message(opportunity, pair, market_a, market_b):
    profit = f"{opportunity.net_profit:.2f}"
    roi = f"{opportunity.net_roi * 100:.2f}"
    capital = f"{opportunity.capital_required:.2f}"
    shares = f"{opportunity.shares:.2f}"
    score = f"{pair.match_score:.2f}"

    return (
        "arbitrage opportunity found\n"
        f"{market_a.platform}: {market_a.title}\n"
        f"{market_b.platform}: {market_b.title}\n"
        f"net profit: ${profit}\n"
        f"net roi: {roi}%\n"
        f"capital required: ${capital}\n"
        f"shares: {shares}\n"
        f"match score: {score}"
    )


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
            print(f"telegram alert loop error: {exc}")

        await asyncio.sleep(settings.TELEGRAM_ALERTS_POLL_SECONDS)


async def start_polling():
    bot, dp = setup_bot()
    if not bot or not dp:
        return

    sender_task = asyncio.create_task(_drain_queued_alerts(bot))
    try:
        await dp.start_polling(bot)
    finally:
        sender_task.cancel()
        await asyncio.gather(sender_task, return_exceptions=True)
        await bot.session.close()
        

if __name__ == "__main__":
    asyncio.run(start_polling())
