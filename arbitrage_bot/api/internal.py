from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.future import select
from arbitrage_bot.core.database import get_db
from arbitrage_bot.core.config import settings
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
