import asyncio
import json
from datetime import datetime, timezone
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError

from arbitrage_bot.adapters.polymarket import PolymarketAdapter
from arbitrage_bot.adapters.predict_fun import PredictFunAdapter
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.models.orm import Market
from arbitrage_bot.services.normalizer import NormalizerService
from arbitrage_bot.services.system_notifier import format_compact_error, send_system_error_notification

log = get_logger("ingestion")


class IngestionService:

    UPSERT_LOOKUP_BATCH_SIZE = 1000

    def __init__(self, db_session):
        self.db = db_session
        self.polymarket = PolymarketAdapter()
        self.predict_fun = PredictFunAdapter()
        self.normalizer = NormalizerService()


    async def close(self):
        await self.polymarket.close()
        await self.predict_fun.close()


    def _normalize_outcome_label(self, value):
        return self.normalizer.normalize_outcome_label(value)


    def _normalize_outcomes(self, outcomes):
        if isinstance(outcomes, str):
            stripped = outcomes.strip()
            if not stripped:
                outcomes = []
            else:
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    outcomes = [outcomes]
                else:
                    outcomes = parsed

        normalized = []

        for index, outcome in enumerate(outcomes or []):
            if isinstance(outcome, str):
                label = outcome
                normalized.append(
                    {
                        "id": str(index),
                        "label": label,
                        "slug": self._normalize_outcome_label(label),
                    }
                )
                continue

            if not isinstance(outcome, dict):
                continue

            label = (
                outcome.get("label")
                or outcome.get("name")
                or outcome.get("title")
                or outcome.get("outcome")
                or outcome.get("value")
                or ""
            )
            
            outcome_id = (
                outcome.get("id")
                or outcome.get("token_id")
                or outcome.get("tokenId")
                or outcome.get("onChainId")
                or outcome.get("asset_id")
                or outcome.get("assetId")
                or outcome.get("contract_id")
                or outcome.get("contractId")
                or outcome.get("slug")
                or index
            )

            normalized_item = {
                "id": str(outcome_id),
                "label": str(label),
                "slug": self._normalize_outcome_label(
                    outcome.get("slug") or label
                ),
            }

            for source_key, target_key in (
                ("token_id", "token_id"),
                ("tokenId", "token_id"),
                ("onChainId", "on_chain_id"),
                ("asset_id", "asset_id"),
                ("assetId", "asset_id"),
                ("contract_id", "contract_id"),
                ("contractId", "contract_id"),
            ):
                value = outcome.get(source_key)
                if value is not None:
                    normalized_item[target_key] = str(value)

            normalized.append(normalized_item)

        return normalized


    def _parse_json_list(self, value):
        if isinstance(value, list):
            return value

        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return []
            return parsed if isinstance(parsed, list) else []

        return []


    def _map_polymarket_market(self, market):
        title = market.get("title") or market.get("question") or market.get("name") or ""
        active = market.get("active")
        closed = market.get("closed")
        tradable = market.get("tradable")
        normalized_outcomes = self._normalize_outcomes(
            market.get("outcomes") or market.get("tokens") or []
        )
        clob_token_ids = self._parse_json_list(market.get("clobTokenIds"))

        for index, token_id in enumerate(clob_token_ids):
            if index >= len(normalized_outcomes):
                break
            normalized_outcomes[index]["id"] = str(token_id)
            normalized_outcomes[index]["clob_token_id"] = str(token_id)

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
            "outcomes_json": normalized_outcomes,
            "raw_payload_json": dict(market),
            "category": market.get("category") or market.get("groupItemTitle") or "",
            "slug": market.get("slug") or market.get("ticker") or ""
        }


    def _map_predict_fun_market(self, market):
        title = market.get("question") or market.get("title") or market.get("name") or ""
        trading_status = str(market.get("tradingStatus") or "").upper()
        market_status = str(market.get("status") or "").upper()

        tradable = (
            trading_status == "OPEN"
            or market_status == "ACTIVE"
        )

        status = "active" if tradable else (trading_status or market_status or "unknown").lower()

        return {
            "platform": "predict_fun",
            "platform_market_id": str(market.get("id")),
            "status": status,
            "tradable": tradable,
            "title": title,
            "normalized_title": title.lower(),
            "description": market.get("description", ""),
            "outcomes_json": self._normalize_outcomes(market.get("outcomes", [])),
            "raw_payload_json": dict(market),
            "category": market.get("category") or market.get("categorySlug") or "",
            "slug": market.get("slug") or market.get("categorySlug") or ""
        }


    async def sync_markets(self):
        results = await asyncio.gather(
            self.polymarket.fetch_markets(),
            self.predict_fun.fetch_markets(),
            return_exceptions=True,
        )

        await self._sync_source(
            "polymarket",
            results[0],
            self._map_polymarket_market,
        )
        await self._sync_source(
            "predict.fun",
            results[1],
            self._map_predict_fun_market,
        )


    def _format_source_error(self, source, operation, error):
        return f"[{source}] {operation} failed: {format_compact_error(error)}"


    def _chunked(self, values, chunk_size):
        for index in range(0, len(values), chunk_size):
            yield values[index:index + chunk_size]


    async def _sync_source(self, source_name, payload_or_exc, mapper):
        try:
            if isinstance(payload_or_exc, BaseException):
                raise payload_or_exc

            raw_items = payload_or_exc.get("data", payload_or_exc) if isinstance(payload_or_exc, dict) else payload_or_exc
            mapped_items = [mapper(item) for item in raw_items if isinstance(item, dict)]
            for chunk in self._chunked(mapped_items, 1000):
                await self._upsert_markets(chunk)
                await self.db.commit()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if "connection is closed" not in str(e).lower() and "[errno 61]" not in str(e).lower():
                log.warning("markets sync failed", source=source_name, error=self._format_source_error(source_name, "markets sync", e))
                await send_system_error_notification(source_name, "markets sync", e)
            await self.db.rollback()


    async def _upsert_markets(self, items):
        if not items:
            return

        platform = items[0]["platform"]
        market_ids = [
            item["platform_market_id"]
            for item in items
        ]
        existing_by_key = {}

        for market_ids_chunk in self._chunked(
            market_ids,
            self.UPSERT_LOOKUP_BATCH_SIZE,
        ):
            stmt = select(Market).where(
                Market.platform == platform,
                Market.platform_market_id.in_(market_ids_chunk),
            )
            existing_markets = (await self.db.execute(stmt)).scalars().all()

            for market in existing_markets:
                existing_by_key[(market.platform, market.platform_market_id)] = market

        for item in items:
            key = (item["platform"], item["platform_market_id"])
            market = existing_by_key.get(key)
            if market is not None:
                self._apply_market_updates(market, item)
            else:
                self.db.add(Market(**item))

        try:
            await self.db.flush()
        except IntegrityError:
            await self.db.rollback()
            for item in items:
                await self._upsert_market(item)


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
        market.updated_at = datetime.now(timezone.utc)