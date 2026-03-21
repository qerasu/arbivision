from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import func
from sqlalchemy.future import select
from arbitrage_bot.core.database import get_db
from arbitrage_bot.core.config import settings
from arbitrage_bot.models.orm import Alert
from arbitrage_bot.models.orm import ArbOpportunity
from arbitrage_bot.models.orm import Market
from arbitrage_bot.models.orm import MarketPair

router = APIRouter()


def require_admin_token(x_admin_token: str | None = Header(default=None)):
    expected_token = settings.ADMIN_API_TOKEN
    if not expected_token or x_admin_token != expected_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="unauthorized",
        )


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
    alerts_stmt = select(func.count(Alert.id)).where(Alert.status == "queued")

    markets_row = (await db.execute(markets_stmt)).one()
    pairs_row = (await db.execute(pairs_stmt)).one()
    opportunities_total = (await db.execute(opportunities_stmt)).scalar_one()
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
        },
        "alert_counts": {
            "queued": queued_alerts,
        },
    }


@router.get("/admin/pairs")
async def get_pairs(
    status="manual_review",
    db=Depends(get_db),
    _=Depends(require_admin_token),
):
    stmt = select(MarketPair).where(MarketPair.status == status)
    result = await db.execute(stmt)
    pairs = result.scalars().all()

    return {
        "data": [
            {
                "id": pair.id,
                "market_id_a": pair.market_id_a,
                "market_id_b": pair.market_id_b,
                "pair_hash": pair.pair_hash,
                "status": pair.status,
                "match_score": pair.match_score,
                "match_reason_json": pair.match_reason_json,
                "created_at": pair.created_at.isoformat() if pair.created_at else None,
            }
            for pair in pairs
        ]
    }


@router.post("/admin/pairs/{pair_id}/approve")
async def approve_pair(pair_id, db=Depends(get_db), _=Depends(require_admin_token)):
    stmt = select(MarketPair).where(MarketPair.id == pair_id)
    result = await db.execute(stmt)
    pair = result.scalars().first()

    if pair:
        pair.status = "approved"
        await db.commit()
        return {"status": "success", "pair_id": pair_id}

    return {"status": "not_found"}
