import asyncio
import secrets

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.security import HTTPBearer
from sqlalchemy.future import select
from sqlalchemy import text, func

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.database import get_db
from arbitrage_bot.core.observability import snapshot_counters
from arbitrage_bot.core.redis import get_redis
from arbitrage_bot.models.orm import Market
from arbitrage_bot.models.orm import MarketPair

router = APIRouter()
_bearer = HTTPBearer(auto_error=False)


def require_internal_api_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
):
    expected_token = settings.ADMIN_API_TOKEN
    if not expected_token:
        raise HTTPException(status_code=503, detail="internal API token is not configured")

    if credentials is None or not secrets.compare_digest(credentials.credentials, expected_token):
        raise HTTPException(
            status_code=401,
            detail="invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


@router.get("/health")
async def health_check(db=Depends(get_db)):
    try:
        await db.execute(text("SELECT 1"))
    except Exception as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc

    redis_status = "degraded"
    redis = get_redis()
    if redis is not None:
        try:
            await asyncio.wait_for(redis.ping(), timeout=1.0)
            redis_status = "ok"
        except Exception:
            pass

    return {
        "status": "ok",
        "database": "ok",
        "redis": redis_status,
    }


@router.get("/status", dependencies=[Depends(require_internal_api_token)])
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
