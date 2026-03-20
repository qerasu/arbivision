import asyncio
from datetime import datetime
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError

from arbitrage_bot.adapters.polymarket import PolymarketAdapter
from arbitrage_bot.adapters.predict_fun import PredictFunAdapter
from arbitrage_bot.models.orm import Market


class IngestionService:
    def __init__(self, db_session):
        self.db = db_session
        self.polymarket = PolymarketAdapter()
        self.predict_fun = PredictFunAdapter()


    async def close(self):
        await self.polymarket.close()
        await self.predict_fun.close()


    def _map_polymarket_market(self, market):
        title = market.get("title") or market.get("question") or market.get("name") or ""
        active = market.get("active")
        closed = market.get("closed")
        tradable = market.get("tradable")

        if tradable is None:
            tradable = bool(active) and not bool(closed)

        return {
            "platform": "polymarket",
            "platform_market_id": str(market.get("id")),
            "status": "active" if tradable else "closed",
            "tradable": bool(tradable),
            "title": title,
            "normalized_title": title.lower(),
            "description": market.get("description") or market.get("details") or "",
            "outcomes_json": market.get("outcomes") or market.get("tokens") or [],
            "raw_payload_json": dict(market),
            "category": market.get("category") or market.get("groupItemTitle") or "",
            "slug": market.get("slug") or market.get("ticker") or ""
        }


    def _map_predict_fun_market(self, market):
        # minimal mapping for predict.fun
        return {
            "platform": "predict_fun",
            "platform_market_id": str(market.get("id")),
            "status": market.get("status", "unknown").lower(),
            "tradable": market.get("status") == "ACTIVE",
            "title": market.get("name", ""),
            "normalized_title": market.get("name", "").lower(),
            "description": market.get("description", ""),
            "outcomes_json": market.get("outcomes", []),
            "raw_payload_json": dict(market),
            "category": market.get("category", ""),
            "slug": market.get("slug", "")
        }


    async def sync_markets(self):
        # sync markets with polymarket
        try:
            poly_data = await self.polymarket.fetch_markets()
            for item in poly_data.get("data", poly_data) if isinstance(poly_data, dict) else poly_data:
                if isinstance(item, dict):
                    mapped = self._map_polymarket_market(item)
                    await self._upsert_market(mapped)
            await self.db.commit()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # only log if not a shutdown/connection error
            if "connection is closed" not in str(e).lower() and "[errno 61]" not in str(e).lower():
                print(self._format_source_error("polymarket", "markets sync", e))
            await self.db.rollback()

        # sync markets with predict.fun
        try:
            pf_data = await self.predict_fun.fetch_markets()
            for item in pf_data.get("data", pf_data) if isinstance(pf_data, dict) else pf_data:
                if isinstance(item, dict):
                    mapped = self._map_predict_fun_market(item)
                    await self._upsert_market(mapped)
            await self.db.commit()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # only log if not a shutdown/connection error
            if "connection is closed" not in str(e).lower() and "[errno 61]" not in str(e).lower():
                print(self._format_source_error("predict.fun", "markets sync", e))
            await self.db.rollback()


    def _format_source_error(self, source, operation, error):
        return f"[{source}] {operation} failed: {type(error).__name__}: {error}"


    async def _upsert_market(self, data):
        stmt = select(Market).where(
            Market.platform == data["platform"],
            Market.platform_market_id == data["platform_market_id"]
        )
        result = await self.db.execute(stmt)
        market = result.scalars().first()

        if market:
            self._apply_market_updates(market, data)
            return

        try:
            async with self.db.begin_nested():
                self.db.add(Market(**data))
                await self.db.flush()
        except IntegrityError:
            # another worker may insert the same market between select and flush
            existing = await self.db.execute(stmt)
            existing_market = existing.scalars().first()
            if existing_market is not None:
                self._apply_market_updates(existing_market, data)


    def _apply_market_updates(self, market, data):
        market.status = data["status"]
        market.tradable = data["tradable"]
        market.title = data["title"]
        market.normalized_title = data["normalized_title"]
        market.description = data["description"]
        market.outcomes_json = data["outcomes_json"]
        market.raw_payload_json = data["raw_payload_json"]
        market.category = data["category"]
        market.slug = data["slug"]
        market.updated_at = datetime.utcnow()
