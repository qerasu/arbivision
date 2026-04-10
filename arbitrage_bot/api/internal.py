from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.future import select

from arbitrage_bot.core.database import get_db
from arbitrage_bot.models.orm import Alert
from arbitrage_bot.models.orm import ArbOpportunity
from arbitrage_bot.models.orm import Market
from arbitrage_bot.models.orm import MarketPair

router = APIRouter()


@router.get("/health")
async def health_check():
    return {"status": "ok"}


@router.get("/status")
async def status_check(db=Depends(get_db)):
    markets_stmt = select(
        func.count(Market.id).label("total"),
        func.count().filter(Market.status == "active").label("active"),
    )
    pairs_stmt = select(
        func.count(MarketPair.id).label("total"),
        func.count().filter(
            MarketPair.status.in_(("approved", "auto_approved"))
        ).label("approved"),
    )
    opportunities_stmt = select(func.count(ArbOpportunity.id))
    queued_fanout_stmt = select(func.count(ArbOpportunity.id)).where(
        ArbOpportunity.fanout_status.in_(("queued", "retry"))
    )
    alerts_stmt = select(func.count(Alert.id)).where(Alert.status == "queued")

    markets_row = (await db.execute(markets_stmt)).one()
    pairs_row = (await db.execute(pairs_stmt)).one()
    opportunities_total = (await db.execute(opportunities_stmt)).scalar_one()
    queued_fanout = (await db.execute(queued_fanout_stmt)).scalar_one()
    queued_alerts = (await db.execute(alerts_stmt)).scalar_one()

    return {
        "status": "ok",
        "service": "arbitrage-alert-bot",
        "market_counts": {
            "total": markets_row.total,
            "active": markets_row.active,
        },
        "pair_counts": {
            "total": pairs_row.total,
            "approved": pairs_row.approved,
        },
        "opportunity_counts": {
            "total": opportunities_total,
            "queued_fanout": queued_fanout,
        },
        "alert_counts": {
            "queued": queued_alerts,
        },
    }
