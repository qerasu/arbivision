from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import func
from sqlalchemy.future import select
from arbitrage_bot.core.database import get_db
from arbitrage_bot.core.config import settings
from arbitrage_bot.models.orm import Alert
from arbitrage_bot.models.orm import ArbOpportunity
from arbitrage_bot.models.orm import Market
from arbitrage_bot.models.orm import MarketPair
from arbitrage_bot.services.matcher import MatcherService

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
    status="auto_approved",
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
async def approve_pair(pair_id: int, db=Depends(get_db), _=Depends(require_admin_token)):
    stmt = select(MarketPair).where(MarketPair.id == pair_id)
    result = await db.execute(stmt)
    pair = result.scalars().first()

    if not pair:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="pair not found")

    pair.status = "approved"
    await db.commit()
    return {"status": "success", "pair_id": pair_id}


@router.get("/admin/matcher/debug")
async def debug_matcher(
    market_id: int,
    limit: int = 10,
    db=Depends(get_db),
    _=Depends(require_admin_token),
):
    source_stmt = select(Market).where(Market.id == market_id)
    source_result = await db.execute(source_stmt)
    source_market = source_result.scalars().first()

    if source_market is None:
        return {"status": "not_found", "market_id": market_id}

    target_platform = "predict_fun" if source_market.platform == "polymarket" else "polymarket"
    candidate_stmt = select(Market).where(
        Market.platform == target_platform,
        Market.status == "active",
    )
    candidate_result = await db.execute(candidate_stmt)
    candidate_markets = candidate_result.scalars().all()

    matcher = MatcherService()
    source_signature = matcher.build_market_signature(source_market)
    debug_items = []

    for candidate_market in candidate_markets:
        candidate_signature = matcher.build_market_signature(candidate_market)
        shared_token_count = len(source_signature["tokens"].intersection(candidate_signature["tokens"]))
        rank_score = matcher.candidate_rank_score(
            source_signature,
            candidate_signature,
            shared_token_count,
        )

        if source_market.platform == "polymarket":
            decision = matcher.explain_match(
                source_market,
                candidate_market,
                poly_signature=source_signature,
                pf_signature=candidate_signature,
            )
        else:
            decision = matcher.explain_match(
                candidate_market,
                source_market,
                poly_signature=candidate_signature,
                pf_signature=source_signature,
            )

        debug_items.append(
            {
                "market_id": candidate_market.id,
                "platform": candidate_market.platform,
                "platform_market_id": candidate_market.platform_market_id,
                "title": candidate_market.title,
                "rank_score": round(rank_score, 4),
                "shared_token_count": shared_token_count,
                "matched": decision["matched"],
                "match_score": round(decision["score"], 4),
                "reject_reason": decision["reason"]["reject_reason"],
                "match_reason_json": decision["reason"],
            }
        )

    safe_limit = max(1, min(int(limit), 25))
    ranked_items = sorted(
        debug_items,
        key=lambda item: (
            item["rank_score"],
            item["match_score"],
            item["shared_token_count"],
            item["market_id"],
        ),
        reverse=True,
    )

    return {
        "status": "ok",
        "source_market": {
            "id": source_market.id,
            "platform": source_market.platform,
            "platform_market_id": source_market.platform_market_id,
            "title": source_market.title,
        },
        "data": ranked_items[:safe_limit],
    }