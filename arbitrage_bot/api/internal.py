from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.future import select

from arbitrage_bot.core.database import get_db
from arbitrage_bot.core.observability import snapshot_counters
from arbitrage_bot.models.orm import Market
from arbitrage_bot.models.orm import MarketPair

router = APIRouter()


@router.get("/health")
async def health_check():
    return {"status": "ok"}


@router.get("/status")
async def status_check(db=Depends(get_db)):
    runtime_metrics = snapshot_counters()
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

    markets_row = (await db.execute(markets_stmt)).one()
    pairs_row = (await db.execute(pairs_stmt)).one()
    opportunities_total = int(runtime_metrics.get("worker.opportunities_created", 0))
    filtered_opportunities = int(runtime_metrics.get("fanout.opportunity_filtered_all_targets", 0))
    sent_alerts = int(runtime_metrics.get("telegram.alert_sent", 0))

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
            "filtered_runtime": filtered_opportunities,
        },
        "alert_counts": {
            "sent_runtime": sent_alerts,
        },
    }