import asyncio
import hashlib
import json
import time
from contextvars import ContextVar
from datetime import datetime, timezone
from sqlalchemy import Text, cast, or_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.future import select

from arbitrage_bot.adapters.polymarket import PolymarketAdapter
from arbitrage_bot.adapters.predict_fun import PredictFunAdapter
from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.models.orm import Market
from arbitrage_bot.services.normalizer import NormalizerService
from arbitrage_bot.services.system_notifier import format_compact_error, send_system_error_notification
from arbitrage_bot.services.system_notifier import is_transient_network_error
from arbitrage_bot.services.operations_monitor import record_duplicate_markets

log = get_logger("ingestion")


class IngestionService:

    UPSERT_LOOKUP_BATCH_SIZE = 500
    MARKET_DEFINITION_RAW_FIELDS = (
        "conditionId",
        "polymarketConditionIds",
        "title",
        "name",
        "groupItemTitle",
        "question",
        "description",
        "category",
        "subcategory",
        "endDate",
        "end_date",
        "endTime",
        "end_time",
        "closeDate",
        "close_date",
        "closeTime",
        "close_time",
        "closedTime",
        "closed_time",
        "expiration",
        "expirationTime",
        "expiration_time",
        "expiresAt",
        "expires_at",
        "resolveDate",
        "resolve_date",
        "resolutionDate",
        "resolution_date",
        "url",
        "marketUrl",
        "market_url",
        "shareUrl",
        "share_url",
    )

    def __init__(self, db_session, session_factory=None):
        self.db = db_session
        self.session_factory = session_factory
        self._source_db_session = ContextVar("ingestion_source_db", default=None)
        self.polymarket = PolymarketAdapter()
        self.predict_fun = PredictFunAdapter()
        self.normalizer = NormalizerService()
        self._changed_market_ids_by_platform = self._empty_changed_market_ids()
        self._source_last_sync_completed_at = {}
        self._source_last_full_sync_attempted_at = {}
        self._market_definition_fingerprints = {}


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
        market_id = market.get("id")
        if market_id is None or not str(market_id).strip():
            return None

        title = market.get("title") or market.get("question") or market.get("name") or ""
        active = market.get("active")
        closed = market.get("closed")
        tradable = market.get("tradable")
        explicitly_closed = tradable is False or closed is True

        if closed is True:
            tradable = False
        elif tradable is None:
            tradable = bool(active) and not bool(closed)

        if not tradable:
            if not explicitly_closed:
                return None
            return {
                "platform": "polymarket",
                "platform_market_id": str(market_id),
                "status": "closed",
                "tradable": False,
                "title": title,
                "normalized_title": title.lower(),
                "description": market.get("description") or market.get("details") or "",
                "outcomes_json": [],
                "raw_payload_json": dict(market),
                "category": market.get("category") or market.get("groupItemTitle") or "",
                "slug": market.get("slug") or market.get("ticker") or "",
            }

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

        return {
            "platform": "polymarket",
            "platform_market_id": str(market_id),
            "status": "active",
            "tradable": True,
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
        successful_sources = []
        now = time.monotonic()

        if self._should_sync_source("polymarket", now):
            polymarket_full_sync = self._should_run_polymarket_full_sync(now)
            if polymarket_full_sync:
                self._source_last_full_sync_attempted_at["polymarket"] = now
            source_jobs.append(
                (
                    "polymarket",
                    lambda: self._sync_source_pages(
                        "polymarket",
                        self.polymarket.iter_market_pages(
                            max_pages=None if polymarket_full_sync else settings.POLYMARKET_INCREMENTAL_MAX_PAGES
                        ),
                        self._map_polymarket_market,
                        self.polymarket,
                        skip_unchanged=True,
                    ),
                )
            )

        if self._should_sync_source("predict.fun", now):
            source_jobs.append(
                (
                    "predict.fun",
                    lambda: self._fetch_and_sync_source(
                        "predict.fun",
                        self.predict_fun.fetch_markets(),
                        self._map_predict_fun_market,
                        self.predict_fun,
                    ),
                )
            )

        if not source_jobs:
            return self._build_sync_result(False)

        results = await asyncio.gather(
            *(self._run_source_job(job) for _, job in source_jobs),
            return_exceptions=True,
        )

        for (source_name, _), result in zip(source_jobs, results):
            synced = False if isinstance(result, BaseException) else bool(result)
            if synced:
                successful_sources.append(source_name)
                self._source_last_sync_completed_at[source_name] = time.monotonic()

        return self._build_sync_result(
            bool(successful_sources),
            attempted=bool(source_jobs),
            successful_sources=successful_sources,
        )


    async def _run_source_job(self, job):
        if self.session_factory is None:
            return await job()

        async with self.session_factory() as session:
            token = self._source_db_session.set(session)
            try:
                return await job()
            finally:
                self._source_db_session.reset(token)


    def _current_db(self):
        session = self._source_db_session.get()
        return session if session is not None else self.db


    def _empty_changed_market_ids(self):
        return {
            "polymarket": set(),
            "predict_fun": set(),
        }


    def _build_sync_result(self, synced, attempted=False, successful_sources=None):
        return {
            "synced": bool(synced),
            "attempted": bool(attempted),
            "successful_sources": list(successful_sources or []),
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
        last_completed_at = self._source_last_sync_completed_at.get(source_name)
        if last_completed_at is None:
            return True
        return (now - last_completed_at) >= min_interval


    def _should_run_polymarket_full_sync(self, now):
        full_interval = max(
            float(settings.POLYMARKET_FULL_SYNC_INTERVAL_SECONDS),
            float(settings.MARKET_SYNC_INTERVAL_SECONDS),
            float(settings.MARKET_REFRESH_SECONDS),
        )
        last_attempted_at = self._source_last_full_sync_attempted_at.get("polymarket")
        if last_attempted_at is None:
            return True
        return (now - last_attempted_at) >= full_interval


    async def _fetch_and_sync_source(self, source_name, fetch_coro, mapper, adapter):
        try:
            payload = await fetch_coro
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            payload = exc

        return await self._sync_source(source_name, payload, mapper, adapter)


    async def _sync_source(self, source_name, payload_or_exc, mapper, adapter=None):
        async def pages():
            if isinstance(payload_or_exc, BaseException):
                raise payload_or_exc

            raw_items = payload_or_exc.get("data", payload_or_exc) if isinstance(payload_or_exc, dict) else payload_or_exc
            yield raw_items or []

        return await self._sync_source_pages(source_name, pages(), mapper, adapter)


    async def _sync_source_pages(
        self,
        source_name,
        pages,
        mapper,
        adapter=None,
        skip_unchanged=False,
    ):
        db = self._current_db()
        try:
            platform = self._source_platform_name(source_name)
            seen_market_keys = set()
            seen_market_ids = set()
            changed_market_ids = set()
            duplicate_count = 0
            duplicate_market_ids = set()
            sample_market_ids = []
            pending_items = []
            unchanged_count = 0
            fetched_market_rows = 0
            rejected_market_rows = 0

            async def upsert_pending_items(raw_chunk):
                nonlocal duplicate_count, platform, rejected_market_rows, unchanged_count

                mapped_items = []
                for item in raw_chunk:
                    if not isinstance(item, dict):
                        continue
                    mapped_item = mapper(item)
                    if mapped_item is not None:
                        mapped_items.append(mapped_item)
                    else:
                        rejected_market_rows += 1
                mapped_items, chunk_duplicate_count, chunk_duplicate_metadata = self._dedupe_market_items(mapped_items)
                duplicate_count += chunk_duplicate_count
                duplicate_market_ids.update(chunk_duplicate_metadata["market_ids"])
                for market_id in chunk_duplicate_metadata["sample_market_ids"]:
                    if len(sample_market_ids) < 5:
                        sample_market_ids.append(market_id)

                unique_items = []
                for item in mapped_items:
                    platform = item["platform"]
                    market_id = item["platform_market_id"]
                    key = (platform, market_id)
                    if key in seen_market_keys:
                        duplicate_count += 1
                        duplicate_market_ids.add(market_id)
                        if len(sample_market_ids) < 5:
                            sample_market_ids.append(market_id)
                        continue
                    seen_market_keys.add(key)
                    seen_market_ids.add(market_id)
                    unique_items.append(item)

                if not unique_items:
                    return

                items_to_upsert, fingerprints = self._filter_unchanged_market_items(
                    platform,
                    unique_items,
                    skip_unchanged=skip_unchanged,
                )
                unchanged_count += len(unique_items) - len(items_to_upsert)
                if not items_to_upsert:
                    return

                batch_changed_market_ids = await self._upsert_markets(items_to_upsert)
                await db.commit()
                self._market_definition_fingerprints.setdefault(platform, {}).update(
                    fingerprints
                )
                changed_market_ids.update(batch_changed_market_ids)
                self._changed_market_ids_by_platform.setdefault(platform, set()).update(
                    batch_changed_market_ids
                )

            async for raw_items in pages:
                if not isinstance(raw_items, list):
                    raw_items = list(raw_items or [])
                fetched_market_rows += sum(
                    isinstance(item, dict) for item in raw_items
                )
                pending_items.extend(raw_items)
                while len(pending_items) >= self.UPSERT_LOOKUP_BATCH_SIZE:
                    raw_chunk = pending_items[:self.UPSERT_LOOKUP_BATCH_SIZE]
                    del pending_items[:self.UPSERT_LOOKUP_BATCH_SIZE]
                    await upsert_pending_items(raw_chunk)

            if pending_items:
                await upsert_pending_items(pending_items)

            is_partial = getattr(adapter, "last_fetch_partial", False)
            is_complete = adapter is None or getattr(adapter, "last_fetch_complete", False)
            if duplicate_count:
                log.info(
                    "duplicate markets removed before upsert",
                    source=source_name,
                    duplicate_rows=duplicate_count,
                    duplicate_distinct_markets=len(duplicate_market_ids),
                    sample_market_ids=sample_market_ids,
                )
                await record_duplicate_markets(source_name, duplicate_count)
            else:
                await record_duplicate_markets(source_name, 0)
            if unchanged_count:
                log.info(
                    "unchanged markets skipped before upsert",
                    source=source_name,
                    skipped_markets=unchanged_count,
                )
            if rejected_market_rows:
                log.warning(
                    "sync completed with rejected market rows, skipping stale market detection",
                    source=source_name,
                    fetched_markets=fetched_market_rows,
                    rejected_markets=rejected_market_rows,
                )
                return False
            if not seen_market_keys:
                self._changed_market_ids_by_platform.setdefault(platform, set())
                if is_partial:
                    log.warning(
                        "partial sync completed with no market rows, skipping stale market detection",
                        source=source_name,
                        fetched_markets=0,
                    )
                    return False
                if not is_complete:
                    log.info(
                        "incremental sync completed with no market rows, skipping stale market detection",
                        source=source_name,
                        fetched_markets=0,
                    )
                    return True
                if not fetched_market_rows:
                    return True
                stale_market_ids = await self._mark_missing_markets_closed(
                    platform,
                    set(),
                )
                await db.commit()
                self._market_definition_fingerprints[platform] = {}
                changed_market_ids.update(stale_market_ids)
                self._changed_market_ids_by_platform.setdefault(platform, set()).update(
                    stale_market_ids
                )
                return True
            if is_partial:
                log.warning(
                    "partial sync completed, skipping stale market detection",
                    source=source_name,
                    fetched_markets=len(seen_market_keys),
                )
            elif not is_complete:
                log.info(
                    "incremental sync completed, skipping stale market detection",
                    source=source_name,
                    fetched_markets=len(seen_market_keys),
                )
            else:
                stale_market_ids = await self._mark_missing_markets_closed(
                    platform,
                    seen_market_ids,
                )
                await db.commit()
                cached_fingerprints = self._market_definition_fingerprints.get(
                    platform,
                    {},
                )
                self._market_definition_fingerprints[platform] = {
                    market_id: fingerprint
                    for market_id, fingerprint in cached_fingerprints.items()
                    if market_id in seen_market_ids
                }
                changed_market_ids.update(stale_market_ids)
                self._changed_market_ids_by_platform.setdefault(platform, set()).update(
                    stale_market_ids
                )
            self._changed_market_ids_by_platform.setdefault(platform, set()).update(changed_market_ids)
            return not is_partial
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if "connection is closed" not in str(e).lower() and "[errno 61]" not in str(e).lower():
                log.warning("markets sync failed", source=source_name, error=self._format_source_error(source_name, "markets sync", e))
                if not is_transient_network_error(e):
                    await send_system_error_notification(source_name, "markets sync", e)
            await db.rollback()
            return False


    def _filter_unchanged_market_items(
        self,
        platform,
        items,
        skip_unchanged,
    ):
        cached_fingerprints = self._market_definition_fingerprints.setdefault(
            platform,
            {},
        )
        pending_items = []
        fingerprints = {}

        for item in items:
            market_id = str(item["platform_market_id"])
            fingerprint = self._market_definition_fingerprint(item)
            if skip_unchanged and cached_fingerprints.get(market_id) == fingerprint:
                continue
            pending_items.append(item)
            fingerprints[market_id] = fingerprint

        return pending_items, fingerprints


    def _market_definition_fingerprint(self, item):
        raw_payload = item.get("raw_payload_json")
        if not isinstance(raw_payload, dict):
            raw_payload = {}
        relevant_raw_payload = {
            field_name: raw_payload[field_name]
            for field_name in self.MARKET_DEFINITION_RAW_FIELDS
            if field_name in raw_payload
        }
        definition = {
            "status": item.get("status"),
            "tradable": item.get("tradable"),
            "title": item.get("title"),
            "normalized_title": item.get("normalized_title"),
            "description": item.get("description"),
            "outcomes_json": item.get("outcomes_json"),
            "category": item.get("category"),
            "slug": item.get("slug"),
            "raw_payload_json": relevant_raw_payload,
        }
        encoded = json.dumps(
            definition,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
        return hashlib.blake2b(encoded, digest_size=16).digest()


    def _dedupe_market_items(self, items):
        deduped = {}
        duplicate_count = 0
        duplicate_market_ids = set()
        sample_market_ids = []

        for item in items:
            key = (item["platform"], item["platform_market_id"])
            if key in deduped:
                duplicate_count += 1
                duplicate_market_ids.add(item["platform_market_id"])
                if len(sample_market_ids) < 5:
                    sample_market_ids.append(item["platform_market_id"])
            deduped[key] = item

        return list(deduped.values()), duplicate_count, {
            "distinct_market_ids": len(duplicate_market_ids),
            "sample_market_ids": sample_market_ids,
            "market_ids": duplicate_market_ids,
        }


    async def _upsert_markets(self, items):
        if not items:
            return set()

        return await self._upsert_markets_postgresql(items)


    async def _upsert_markets_postgresql(self, items):
        db = self._current_db()
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
            Market.category.is_distinct_from(excluded.category),
            Market.slug.is_distinct_from(excluded.slug),
            *(
                cast(Market.raw_payload_json[field_name], Text).is_distinct_from(
                    cast(excluded.raw_payload_json[field_name], Text)
                )
                for field_name in self.MARKET_DEFINITION_RAW_FIELDS
            ),
        )
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=[Market.platform, Market.platform_market_id],
            set_=update_fields,
            where=diff_condition,
        ).returning(Market.id)
        result = await db.execute(stmt)
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


    async def _mark_missing_markets_closed(self, platform, seen_market_ids):
        if not platform:
            return set()

        stmt = select(Market.id, Market.platform_market_id).where(
            Market.platform == platform,
            Market.status == "active",
        )
        db = self._current_db()
        rows = (await db.execute(stmt)).all()
        if not rows:
            return set()

        to_close_ids = [
            market_id
            for market_id, platform_market_id in rows
            if platform_market_id not in seen_market_ids
        ]
        if not to_close_ids:
            return set()

        now = datetime.now(timezone.utc)
        for chunk in self._chunked(to_close_ids, 1000):
            await db.execute(
                update(Market)
                .where(Market.id.in_(chunk))
                .values(status="closed", tradable=False, updated_at=now)
            )

        return set(to_close_ids)
