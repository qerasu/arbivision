import asyncio
import json
import time
from datetime import datetime, timezone
from sqlalchemy import Text, cast, func, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError

from arbitrage_bot.adapters.polymarket import PolymarketAdapter
from arbitrage_bot.adapters.predict_fun import PredictFunAdapter
from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.models.orm import Market
from arbitrage_bot.services.normalizer import NormalizerService
from arbitrage_bot.services.system_notifier import format_compact_error, send_system_error_notification
from arbitrage_bot.services.system_notifier import is_transient_network_error

log = get_logger("ingestion")
_source_last_sync_completed_at = {}


class IngestionService:

    UPSERT_LOOKUP_BATCH_SIZE = 1000

    def __init__(self, db_session):
        self.db = db_session
        self.polymarket = PolymarketAdapter()
        self.predict_fun = PredictFunAdapter()
        self.normalizer = NormalizerService()
        self._changed_market_ids_by_platform = self._empty_changed_market_ids()


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
            normalized_outcome = normalized_outcomes[index]
            clob_token_id = str(token_id)
            normalized_outcome["clob_token_id"] = clob_token_id

            has_explicit_token_id = any(
                normalized_outcome.get(field_name)
                for field_name in (
                    "token_id",
                    "asset_id",
                    "contract_id",
                    "on_chain_id",
                )
            )
            uses_fallback_index = normalized_outcome.get("id") == str(index)

            if uses_fallback_index or not has_explicit_token_id:
                normalized_outcome["id"] = clob_token_id

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
        self._changed_market_ids_by_platform = self._empty_changed_market_ids()
        source_jobs = []
        now = time.monotonic()

        if self._should_sync_source("polymarket", now):
            source_jobs.append(
                (
                    "polymarket",
                    self.polymarket.fetch_markets(),
                    self._map_polymarket_market,
                )
            )

        if self._should_sync_source("predict.fun", now):
            source_jobs.append(
                (
                    "predict.fun",
                    self.predict_fun.fetch_markets(),
                    self._map_predict_fun_market,
                )
            )

        if not source_jobs:
            return self._build_sync_result(False)

        results = await asyncio.gather(
            *(job[1] for job in source_jobs),
            return_exceptions=True,
        )

        for (source_name, _, mapper), result in zip(source_jobs, results):
            synced = await self._sync_source(
                source_name,
                result,
                mapper,
            )
            if synced:
                _source_last_sync_completed_at[source_name] = time.monotonic()

        return self._build_sync_result(True)


    def _empty_changed_market_ids(self):
        return {
            "polymarket": set(),
            "predict_fun": set(),
        }


    def _build_sync_result(self, synced):
        return {
            "synced": bool(synced),
            "changed_market_ids_by_platform": {
                platform: set(market_ids)
                for platform, market_ids in self._changed_market_ids_by_platform.items()
            },
        }


    def _source_platform_name(self, source_name):
        return str(source_name or "").replace(".", "_")


    def _format_source_error(self, source, operation, error):
        return f"[{source}] {operation} failed: {format_compact_error(error)}"


    def _chunked(self, values, chunk_size):
        for index in range(0, len(values), chunk_size):
            yield values[index:index + chunk_size]


    def _should_sync_source(self, source_name, now):
        min_interval = max(
            float(settings.MARKET_SYNC_INTERVAL_SECONDS),
            float(settings.MARKET_REFRESH_SECONDS),
        )
        last_completed_at = _source_last_sync_completed_at.get(source_name)
        if last_completed_at is None:
            return True
        return (now - last_completed_at) >= min_interval


    async def _sync_source(self, source_name, payload_or_exc, mapper):
        try:
            if isinstance(payload_or_exc, BaseException):
                raise payload_or_exc

            raw_items = payload_or_exc.get("data", payload_or_exc) if isinstance(payload_or_exc, dict) else payload_or_exc
            mapped_items = [mapper(item) for item in raw_items if isinstance(item, dict)]
            platform = mapped_items[0]["platform"] if mapped_items else self._source_platform_name(source_name)
            mapped_items, duplicate_count = self._dedupe_market_items(mapped_items)
            if duplicate_count:
                log.warning(
                    "duplicate markets removed before upsert",
                    source=source_name,
                    duplicate_rows=duplicate_count,
                )
            if not mapped_items:
                self._changed_market_ids_by_platform.setdefault(platform, set())
                return True
            changed_market_ids = set()
            for chunk in self._chunked(mapped_items, 1000):
                changed_market_ids.update(await self._upsert_markets(chunk))
                await self.db.commit()
            changed_market_ids.update(await self._mark_missing_markets_closed(
                platform,
                {item["platform_market_id"] for item in mapped_items},
            ))
            await self.db.commit()
            self._changed_market_ids_by_platform.setdefault(platform, set()).update(changed_market_ids)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if "connection is closed" not in str(e).lower() and "[errno 61]" not in str(e).lower():
                log.warning("markets sync failed", source=source_name, error=self._format_source_error(source_name, "markets sync", e))
                if not is_transient_network_error(e):
                    await send_system_error_notification(source_name, "markets sync", e)
            await self.db.rollback()
            return False


    def _dedupe_market_items(self, items):
        deduped = {}
        duplicate_count = 0

        for item in items:
            key = (item["platform"], item["platform_market_id"])
            if key in deduped:
                duplicate_count += 1
            deduped[key] = item

        return list(deduped.values()), duplicate_count


    async def _upsert_markets(self, items):
        if not items:
            return set()

        items, _ = self._dedupe_market_items(items)

        if self._supports_postgresql_upsert():
            return await self._upsert_markets_postgresql(items)

        platform = items[0]["platform"]
        market_ids = [
            item["platform_market_id"]
            for item in items
        ]
        existing_by_key = {}
        created_markets = []
        changed_market_ids = set()

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
                if self._apply_market_updates(market, item):
                    changed_market_ids.add(market.id)
            else:
                market = Market(**item)
                self.db.add(market)
                created_markets.append(market)

        try:
            await self.db.flush()
        except IntegrityError:
            await self.db.rollback()
            for item in items:
                market_id = await self._upsert_market(item)
                if market_id is not None:
                    changed_market_ids.add(market_id)
            return changed_market_ids

        for market in created_markets:
            if getattr(market, "id", None) is not None:
                changed_market_ids.add(market.id)

        return changed_market_ids


    def _supports_postgresql_upsert(self):
        bind = None
        try:
            bind = self.db.get_bind()
        except Exception:
            bind = getattr(self.db, "bind", None)
        dialect = getattr(bind, "dialect", None)
        if dialect is None:
            dialect = getattr(getattr(bind, "sync_engine", None), "dialect", None)
        return getattr(dialect, "name", None) == "postgresql"


    async def _upsert_markets_postgresql(self, items):
        now = datetime.now(timezone.utc)
        rows = [self._market_row_for_upsert(item, now) for item in items]
        insert_stmt = pg_insert(Market).values(rows)
        excluded = insert_stmt.excluded
        update_fields = {
            "status": excluded.status,
            "tradable": excluded.tradable,
            "title": excluded.title,
            "normalized_title": excluded.normalized_title,
            "description": excluded.description,
            "outcomes_json": excluded.outcomes_json,
            "raw_payload_json": excluded.raw_payload_json,
            "category": excluded.category,
            "slug": excluded.slug,
            "updated_at": excluded.updated_at,
        }
        diff_condition = or_(
            Market.status.is_distinct_from(excluded.status),
            Market.tradable.is_distinct_from(excluded.tradable),
            Market.title.is_distinct_from(excluded.title),
            Market.normalized_title.is_distinct_from(excluded.normalized_title),
            Market.description.is_distinct_from(excluded.description),
            cast(Market.outcomes_json, Text).is_distinct_from(cast(excluded.outcomes_json, Text)),
            func.md5(cast(Market.raw_payload_json, Text)).is_distinct_from(
                func.md5(cast(excluded.raw_payload_json, Text))
            ),
            Market.category.is_distinct_from(excluded.category),
            Market.slug.is_distinct_from(excluded.slug),
        )
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=[Market.platform, Market.platform_market_id],
            set_=update_fields,
            where=diff_condition,
        ).returning(Market.id)
        result = await self.db.execute(stmt)
        return {market_id for market_id, in result.all()}


    def _market_row_for_upsert(self, item, now):
        return {
            "platform": item["platform"],
            "platform_market_id": item["platform_market_id"],
            "status": item["status"],
            "tradable": item["tradable"],
            "title": item["title"],
            "normalized_title": item["normalized_title"],
            "description": item["description"],
            "outcomes_json": item["outcomes_json"],
            "raw_payload_json": item["raw_payload_json"],
            "category": item["category"],
            "slug": item["slug"],
            "created_at": now,
            "updated_at": now,
        }


    async def _upsert_market(self, data):
        stmt = select(Market).where(
            Market.platform == data["platform"],
            Market.platform_market_id == data["platform_market_id"]
        )
        result = await self.db.execute(stmt)
        market = result.scalars().first()

        if market:
            if self._apply_market_updates(market, data):
                return market.id
            return None

        try:
            async with self.db.begin_nested():
                market = Market(**data)
                self.db.add(market)
                await self.db.flush()
                return market.id
        except IntegrityError:
            existing = await self.db.execute(stmt)
            existing_market = existing.scalars().first()
            if existing_market is not None:
                if self._apply_market_updates(existing_market, data):
                    return existing_market.id
        return None


    def _apply_market_updates(self, market, data):
        if not self._market_fields_changed(market, data):
            return False
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
        return True


    def _market_fields_changed(self, market, data):
        return any(
            (
                market.status != data["status"],
                market.tradable != data["tradable"],
                market.title != data["title"],
                market.normalized_title != data["normalized_title"],
                market.description != data["description"],
                market.outcomes_json != data["outcomes_json"],
                market.raw_payload_json != data["raw_payload_json"],
                market.category != data["category"],
                market.slug != data["slug"],
            )
        )


    async def _mark_missing_markets_closed(self, platform, seen_market_ids):
        if not platform:
            return set()

        stmt = select(Market).where(
            Market.platform == platform,
            Market.status == "active",
        )
        active_markets = (await self.db.execute(stmt)).scalars().all()
        if not active_markets:
            return set()

        closed_market_ids = set()
        for market in active_markets:
            if market.platform_market_id in seen_market_ids:
                continue
            market.status = "closed"
            market.tradable = False
            market.updated_at = datetime.now(timezone.utc)
            market_id = getattr(market, "id", None)
            if market_id is not None:
                closed_market_ids.add(market_id)

        return closed_market_ids
