import asyncio
import hashlib
import json
import time
import traceback
from cachetools import TTLCache
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime, timezone
from datetime import timedelta
from types import SimpleNamespace
from arbitrage_bot.core.database import AsyncSessionLocal
from arbitrage_bot.core.redis import get_redis
from arbitrage_bot.models.orm import Market, MarketPair
from arbitrage_bot.services.ingestion import IngestionService
from arbitrage_bot.services.matcher import MatcherService
from arbitrage_bot.services.orderbook import OrderbookService
from arbitrage_bot.services.calculator import ArbitrageCalculator
from arbitrage_bot.services.alert_manager import AlertManager
from arbitrage_bot.services.fanout_manager import FanoutManager
from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.core.observability import incr_counter
from arbitrage_bot.services.system_notifier import format_error_details, send_system_error_notification
from arbitrage_bot.services.operations_monitor import record_worker_cycle
from arbitrage_bot.tg_bot.bot import send_alert_immediately
from arbitrage_bot.tg_bot.preferences import extract_pair_close_datetime
from sqlalchemy.future import select
from sqlalchemy import and_, delete, or_
from sqlalchemy.exc import IntegrityError

log = get_logger("worker")
_EMPTY_COUNTS_MAX_SIZE = 1000
_SIGNATURE_CACHE_MAX_SIZE = 20000
_EMPTY_COUNT_TTL_SECONDS = max(settings.MARKET_REFRESH_SECONDS * 20, 3600)


@dataclass
class WorkerState:
    pair_empty_counts: TTLCache = field(default_factory=lambda: TTLCache(maxsize=_EMPTY_COUNTS_MAX_SIZE, ttl=_EMPTY_COUNT_TTL_SECONDS))
    market_signature_cache: TTLCache = field(default_factory=lambda: TTLCache(maxsize=_SIGNATURE_CACHE_MAX_SIZE, ttl=_EMPTY_COUNT_TTL_SECONDS))
    hot_pair_hashes: list = field(default_factory=list)
    candidate_context_loaded: bool = False
    candidate_pairs: list = field(default_factory=list)
    candidate_market_map: dict = field(default_factory=dict)
    last_full_pair_rematch_completed_at: float | None = None
    last_db_cleanup_completed_at: float | None = None
    pair_cycle_offsets: dict = field(default_factory=dict)


class AlertRetryQueue:
    def __init__(self, calculator):
        max_size = max(1, int(settings.TELEGRAM_ALERT_RETRY_QUEUE_MAX_SIZE or 1))
        self._calculator = calculator
        self._queue = asyncio.PriorityQueue(maxsize=max_size)
        self._wake_event = asyncio.Event()
        self._sequence = 0


    def enqueue(self, item):
        alert = item["delivery"]["alert"]
        attempt_count = int(getattr(alert, "attempt_count", 0) or 0)
        max_attempts = max(1, int(settings.TELEGRAM_ALERT_RETRY_MAX_ATTEMPTS or 1))
        if attempt_count >= max_attempts:
            alert.next_retry_at = None
            incr_counter("worker.retry_exhausted")
            return False

        base_delay = max(0.0, float(settings.TELEGRAM_ALERT_RETRY_BASE_DELAY_SECONDS or 0.0))
        delay = base_delay * (2 ** max(0, attempt_count - 1))
        due_at = time.monotonic() + delay
        self._sequence += 1

        try:
            self._queue.put_nowait((due_at, self._sequence, item))
        except asyncio.QueueFull:
            alert.next_retry_at = None
            incr_counter("worker.retry_queue_full")
            return False

        alert.status = "retry_queued"
        alert.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
        self._wake_event.set()
        incr_counter("worker.retry_queued")
        return True


    async def run(self):
        while True:
            if self._queue.empty():
                self._wake_event.clear()
                if self._queue.empty():
                    await self._wake_event.wait()
                continue

            due_at, sequence, item = self._queue.get_nowait()
            delay = due_at - time.monotonic()
            if delay > 0:
                self._queue.put_nowait((due_at, sequence, item))
                self._queue.task_done()
                self._wake_event.clear()
                try:
                    await asyncio.wait_for(self._wake_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                continue

            try:
                sent = await _retry_alert_delivery(item, self._calculator)
                alert = item["delivery"]["alert"]
                if not sent and getattr(alert, "status", None) == "failed":
                    self.enqueue(item)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error("retry delivery failed", error=format_error_details(e))
                incr_counter("worker.retry_send_failed")
            finally:
                self._queue.task_done()


def _snapshot_market(market):
    return SimpleNamespace(
        id=getattr(market, "id", None),
        platform=getattr(market, "platform", None),
        platform_market_id=getattr(market, "platform_market_id", None),
        status=getattr(market, "status", None),
        tradable=getattr(market, "tradable", None),
        title=getattr(market, "title", None),
        normalized_title=getattr(market, "normalized_title", None),
        description=getattr(market, "description", None),
        outcomes_json=getattr(market, "outcomes_json", None),
        raw_payload_json=getattr(market, "raw_payload_json", None),
        category=getattr(market, "category", None),
        slug=getattr(market, "slug", None),
        updated_at=getattr(market, "updated_at", None),
        created_at=getattr(market, "created_at", None),
    )


def _snapshot_pair(pair):
    return SimpleNamespace(
        id=getattr(pair, "id", None),
        market_id_a=getattr(pair, "market_id_a", None),
        market_id_b=getattr(pair, "market_id_b", None),
        pair_hash=getattr(pair, "pair_hash", None),
        status=getattr(pair, "status", None),
        match_score=getattr(pair, "match_score", None),
        match_reason_json=getattr(pair, "match_reason_json", None),
        outcome_mapping_json=getattr(pair, "outcome_mapping_json", None),
        created_at=getattr(pair, "created_at", None),
    )


async def run_sync_loop(state=None):
    runtime_state = state or WorkerState()
    ingestion = IngestionService(db_session=None)
    matcher = MatcherService()
    orderbook_service = OrderbookService()
    calculator = ArbitrageCalculator()
    retry_queue = AlertRetryQueue(calculator)
    retry_task = asyncio.create_task(retry_queue.run())
    try:
        while True:
            try:
                incr_counter("worker.cycle_started")
                async with AsyncSessionLocal() as session:
                    ingestion.db = session
                    alert_manager = AlertManager(session)
                    fanout_manager = FanoutManager(session)
                    await _run_cycle(
                        session,
                        runtime_state,
                        ingestion,
                        matcher,
                        orderbook_service,
                        calculator,
                        alert_manager,
                        fanout_manager,
                        retry_queue,
                    )
                incr_counter("worker.cycle_completed")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(
                    "sync loop error",
                    error=format_error_details(e),
                    traceback=traceback.format_exc(),
                )
                incr_counter("worker.cycle_failed")
                await send_system_error_notification("worker", "sync loop", e)
                
            await asyncio.sleep(settings.MARKET_REFRESH_SECONDS)
    finally:
        retry_task.cancel()
        await asyncio.gather(retry_task, return_exceptions=True)
        await ingestion.close()
        await orderbook_service.close()


async def _run_cycle(db, state, ingestion, matcher, orderbook_service, calculator, alert_manager, fanout_manager, retry_queue=None):
    sync_result = await ingestion.sync_markets()
    if _should_run_full_pair_rematch(time.monotonic(), state):
        _invalidate_candidate_context_cache(state)
        hot_pair_hashes = await _upsert_market_pairs(db, matcher, None, state)
        _queue_hot_pairs(state, hot_pair_hashes)
        _mark_full_pair_rematch_completed(state)
    else:
        changed_market_ids_by_platform = _extract_changed_market_ids_by_platform(sync_result)
        if _has_changed_market_ids(changed_market_ids_by_platform):
            _invalidate_candidate_context_cache(state)
            hot_pair_hashes = await _upsert_market_pairs(
                db,
                matcher,
                changed_market_ids_by_platform,
                state,
            )
            _queue_hot_pairs(state, hot_pair_hashes)
    cycle_stats = await _process_candidates(
        db,
        orderbook_service,
        calculator,
        alert_manager,
        fanout_manager,
        state,
        retry_queue,
    )
    log.info(
        "worker cycle summary",
        approved_pairs=cycle_stats["approved_pairs"],
        active_pairs=cycle_stats["active_pairs"],
        pairs_with_books=cycle_stats["pairs_with_books"],
        skipped_pairs=cycle_stats["skipped_pairs"],
        opportunities=cycle_stats["opportunities"],
        deliverable_opportunities=cycle_stats["deliverable_opportunities"],
    )
    await record_worker_cycle(
        active_pairs=cycle_stats["active_pairs"],
        pairs_with_books=cycle_stats["pairs_with_books"],
        opportunities=cycle_stats["opportunities"],
        deliverable_opportunities=cycle_stats["deliverable_opportunities"],
    )
    await _run_database_cleanup_if_due(db, state)


def _extract_changed_market_ids_by_platform(sync_result):
    if not isinstance(sync_result, dict):
        return {
            "polymarket": set(),
            "predict_fun": set(),
        }

    changed_market_ids_by_platform = sync_result.get("changed_market_ids_by_platform") or {}
    return {
        "polymarket": set(changed_market_ids_by_platform.get("polymarket") or []),
        "predict_fun": set(changed_market_ids_by_platform.get("predict_fun") or []),
    }


def _has_changed_market_ids(changed_market_ids_by_platform):
    return any(changed_market_ids_by_platform.values())


def _should_run_full_pair_rematch(now, state):
    interval = max(
        float(settings.MATCHER_FULL_REMATCH_INTERVAL_SECONDS),
        float(settings.MARKET_REFRESH_SECONDS),
    )
    if interval <= 0:
        return True

    if state.last_full_pair_rematch_completed_at is None:
        return True

    return (now - state.last_full_pair_rematch_completed_at) >= interval


def _mark_full_pair_rematch_completed(state):
    state.last_full_pair_rematch_completed_at = time.monotonic()


def _should_run_db_cleanup(now, state):
    interval = float(settings.DB_CLEANUP_INTERVAL_SECONDS)
    if interval <= 0:
        return False

    if state.last_db_cleanup_completed_at is None:
        return True

    return (now - state.last_db_cleanup_completed_at) >= interval


def _mark_db_cleanup_completed(state, now=None):
    state.last_db_cleanup_completed_at = time.monotonic() if now is None else float(now)


async def _run_database_cleanup_if_due(db, state):
    now = time.monotonic()
    if not _should_run_db_cleanup(now, state):
        return

    try:
        deleted_pairs, deleted_markets = await _cleanup_database_records(db, state)
        _mark_db_cleanup_completed(state, now=now)
        log.info(
            "database cleanup completed",
            deleted_pairs=deleted_pairs,
            deleted_markets=deleted_markets,
        )
        incr_counter("worker.db_cleanup_completed")
        if deleted_pairs:
            incr_counter("worker.db_cleanup_pairs_deleted", deleted_pairs)
        if deleted_markets:
            incr_counter("worker.db_cleanup_markets_deleted", deleted_markets)
    except Exception as exc:
        log.error(
            "database cleanup failed",
            error=format_error_details(exc),
        )
        incr_counter("worker.db_cleanup_failed")
        try:
            await db.rollback()
        except Exception:
            pass
        await send_system_error_notification("worker", "database cleanup", exc)


async def _cleanup_database_records(db, state):
    retention_seconds = max(
        float(settings.DB_CLEANUP_RETENTION_SECONDS),
        float(settings.DB_CLEANUP_INTERVAL_SECONDS),
    )
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=retention_seconds)

    stale_pairs_stmt = select(MarketPair.id, MarketPair.pair_hash).where(
        MarketPair.status.in_(["stale", "failed"]),
        MarketPair.created_at < cutoff,
    )
    stale_pair_rows = (await db.execute(stale_pairs_stmt)).all()
    stale_pair_ids = [pair_id for pair_id, _ in stale_pair_rows]
    stale_pair_hashes = [pair_hash for _, pair_hash in stale_pair_rows]

    if stale_pair_ids:
        await db.execute(delete(MarketPair).where(MarketPair.id.in_(stale_pair_ids)))
        for pair_hash in stale_pair_hashes:
            await _clear_empty_count(pair_hash, state)

    referenced_subquery = select(MarketPair.market_id_a).union(
        select(MarketPair.market_id_b)
    )
    closed_markets_stmt = select(Market.id).where(
        Market.status != "active",
        Market.updated_at < cutoff,
        ~Market.id.in_(referenced_subquery),
    )
    stale_market_rows = (await db.execute(closed_markets_stmt)).all()
    stale_market_ids = [market_id for market_id, in stale_market_rows]

    if stale_market_ids:
        await db.execute(delete(Market).where(Market.id.in_(stale_market_ids)))

    if stale_pair_ids or stale_market_ids:
        await db.commit()

    return len(stale_pair_ids), len(stale_market_ids)


def _invalidate_candidate_context_cache(state):
    state.candidate_context_loaded = False
    state.candidate_pairs = []
    state.candidate_market_map = {}


async def _load_candidate_context(db, state, force_refresh=False):
    if state.candidate_context_loaded and not force_refresh:
        return (
            list(state.candidate_pairs),
            dict(state.candidate_market_map),
        )

    pair_stmt = select(MarketPair).where(
        MarketPair.status.in_(["auto_approved", "approved"])
    )
    pairs = (await db.execute(pair_stmt)).scalars().all()
    market_map = await _load_market_map_for_pairs(db, pairs)
    pair_snapshots = [_snapshot_pair(pair) for pair in pairs]
    market_snapshots = {
        market_id: _snapshot_market(market)
        for market_id, market in market_map.items()
    }
    state.candidate_context_loaded = True
    state.candidate_pairs = list(pair_snapshots)
    state.candidate_market_map = dict(market_snapshots)
    return pair_snapshots, market_snapshots


async def _upsert_market_pairs(db, matcher, changed_market_ids_by_platform, state):
    full_rematch = changed_market_ids_by_platform is None
    changed_poly_ids = set()
    changed_pf_ids = set()
    changed_market_ids = set()
    if not full_rematch:
        changed_poly_ids = set(changed_market_ids_by_platform.get("polymarket") or [])
        changed_pf_ids = set(changed_market_ids_by_platform.get("predict_fun") or [])
        changed_market_ids = changed_poly_ids.union(changed_pf_ids)
        if not changed_market_ids:
            return set()

    poly_markets, pf_markets = await _load_active_markets_by_platform(db)
    active_market_ids = {
        market.id
        for market in poly_markets
    }.union(
        market.id
        for market in pf_markets
    )
    poly_by_id = {market.id: market for market in poly_markets}
    pf_by_id = {market.id: market for market in pf_markets}
    poly_changed = list(poly_markets) if full_rematch else [
        poly_by_id[market_id]
        for market_id in changed_poly_ids
        if market_id in poly_by_id
    ]
    pf_changed = [] if full_rematch else [
        pf_by_id[market_id]
        for market_id in changed_pf_ids
        if market_id in pf_by_id
    ]

    matched_pairs = {}
    pair_limit = settings.MAX_MARKET_PAIRS_PER_LOOP
    reached_limit = False

    if poly_changed and pf_markets:
        poly_signatures = _build_cached_market_signatures(poly_changed, matcher, state)
        pf_signatures = _build_cached_market_signatures(pf_markets, matcher, state)
        pf_index = _build_candidate_index_from_signatures(pf_signatures)
        reached_limit = _match_changed_markets(
            poly_changed,
            poly_signatures,
            pf_index,
            matcher,
            matched_pairs,
            pair_limit,
            poly_is_source=True,
        )

    if pf_changed and poly_markets and not reached_limit:
        pf_signatures = _build_cached_market_signatures(pf_changed, matcher, state)
        poly_signatures = _build_cached_market_signatures(poly_markets, matcher, state)
        poly_index = _build_candidate_index_from_signatures(poly_signatures)
        reached_limit = _match_changed_markets(
            pf_changed,
            pf_signatures,
            poly_index,
            matcher,
            matched_pairs,
            pair_limit,
            poly_is_source=False,
        )

    if full_rematch:
        existing_pairs = await _load_active_pairs(db)
    else:
        existing_pairs = await _load_pairs_for_market_ids(db, changed_market_ids)
    if reached_limit:
        existing_pairs = [
            pair
            for pair in existing_pairs
            if pair.pair_hash in matched_pairs
        ]
    new_pairs, has_updates, hot_pair_hashes = _reconcile_market_pairs(existing_pairs, matched_pairs)
    if has_updates:
        stale_pairs = [pair for pair in existing_pairs if pair.status == "stale"]
        await _clear_empty_counts_for_pairs(stale_pairs, state)
    if not new_pairs and not has_updates:
        _prune_market_signature_cache(state, active_market_ids)
        return hot_pair_hashes

    if new_pairs:
        db.add_all(new_pairs)
    try:
        await db.flush()
        await db.commit()
    except IntegrityError:
        await db.rollback()
        hot_pair_hashes = set()

    _prune_market_signature_cache(state, active_market_ids)
    return hot_pair_hashes


async def _load_active_markets_by_platform(db):
    poly_stmt = select(Market).where(
        and_(Market.platform == "polymarket", Market.status == "active")
    )
    pf_stmt = select(Market).where(
        and_(Market.platform == "predict_fun", Market.status == "active")
    )

    poly_markets = (await db.execute(poly_stmt)).scalars().all()
    pf_markets = (await db.execute(pf_stmt)).scalars().all()
    return poly_markets, pf_markets


async def _load_active_pairs(db):
    stmt = select(MarketPair).where(
        MarketPair.status.in_(["auto_approved", "approved"])
    )
    return (await db.execute(stmt)).scalars().all()


async def _load_pairs_for_market_ids(db, market_ids):
    if not market_ids:
        return []

    batch_size = 10000
    market_id_list = list(market_ids)
    all_pairs = []

    for offset in range(0, len(market_id_list), batch_size):
        chunk = market_id_list[offset:offset + batch_size]
        stmt = select(MarketPair).where(
            or_(
                MarketPair.market_id_a.in_(chunk),
                MarketPair.market_id_b.in_(chunk),
            )
        )
        pairs = (await db.execute(stmt)).scalars().all()
        all_pairs.extend(pairs)

    return all_pairs


def _match_changed_markets(changed_markets, changed_signatures, candidate_index, matcher, matched_pairs, pair_limit, poly_is_source):
    reached_limit = False

    for market in changed_markets:
        if reached_limit:
            break
        market_signature = changed_signatures.get(market.id)
        if market_signature is None:
            continue
        for candidate_signature in _candidate_markets_for_signature(
            market_signature,
            matcher,
            candidate_index,
        ):
            if poly_is_source:
                pair = matcher.match_candidates(
                    market,
                    candidate_signature["market"],
                    poly_signature=market_signature,
                    pf_signature=candidate_signature,
                )
            else:
                pair = matcher.match_candidates(
                    candidate_signature["market"],
                    market,
                    poly_signature=candidate_signature,
                    pf_signature=market_signature,
                )
            if pair:
                matched_pairs[pair.pair_hash] = pair
            if pair_limit and len(matched_pairs) >= pair_limit:
                reached_limit = True
                break

    return reached_limit


def _reconcile_market_pairs(existing_pairs, matched_pairs_by_hash):
    existing_by_hash = {pair.pair_hash: pair for pair in existing_pairs}
    new_pairs = []
    has_updates = False
    hot_pair_hashes = set()

    for pair_hash, matched_pair in matched_pairs_by_hash.items():
        existing_pair = existing_by_hash.pop(pair_hash, None)
        if existing_pair is None:
            new_pairs.append(matched_pair)
            hot_pair_hashes.add(pair_hash)
            continue

        if _refresh_existing_pair(existing_pair, matched_pair):
            has_updates = True
            hot_pair_hashes.add(pair_hash)

    if _mark_stale_pairs(existing_by_hash.values()):
        has_updates = True

    return new_pairs, has_updates, hot_pair_hashes


def _refresh_existing_pair(existing_pair, matched_pair):
    changed = False

    if existing_pair.match_score != matched_pair.match_score:
        existing_pair.match_score = matched_pair.match_score
        changed = True

    if existing_pair.match_reason_json != matched_pair.match_reason_json:
        existing_pair.match_reason_json = matched_pair.match_reason_json
        changed = True

    if existing_pair.outcome_mapping_json != matched_pair.outcome_mapping_json:
        existing_pair.outcome_mapping_json = matched_pair.outcome_mapping_json
        changed = True

    if existing_pair.status != "approved" and existing_pair.status != matched_pair.status:
        existing_pair.status = matched_pair.status
        changed = True

    return changed


def _mark_stale_pairs(pairs):
    changed = False
    active_statuses = {"auto_approved", "approved"}

    for pair in pairs:
        if pair.status in active_statuses:
            pair.status = "stale"
            changed = True

    return changed


def _queue_hot_pairs(state, pair_hashes):
    if not pair_hashes:
        return

    queued = [
        pair_hash
        for pair_hash in state.hot_pair_hashes
        if pair_hash not in pair_hashes
    ]
    queued.extend(pair_hash for pair_hash in pair_hashes if pair_hash)
    max_size = max(1, int(settings.HOT_PAIR_QUEUE_MAX_SIZE or 1))
    state.hot_pair_hashes = queued[-max_size:]
    incr_counter("worker.hot_pairs_queued", len(pair_hashes))


def _select_active_pairs_for_cycle(pairs, market_map, state):
    pair_limit = int(settings.MAX_ACTIVE_PAIRS_PER_CYCLE or 0)
    if not pairs:
        return []

    pair_by_hash = {pair.pair_hash: pair for pair in pairs}
    hot_pairs = [
        pair_by_hash[pair_hash]
        for pair_hash in state.hot_pair_hashes
        if pair_hash in pair_by_hash
    ]
    hot_hashes = {pair.pair_hash for pair in hot_pairs}
    regular_pairs = [
        pair
        for pair in pairs
        if pair.pair_hash not in hot_hashes
    ]

    if pair_limit > 0:
        hot_pairs = hot_pairs[:pair_limit]
        remaining_limit = pair_limit - len(hot_pairs)
        regular_pairs = _limit_active_pairs_for_cycle(
            regular_pairs,
            market_map,
            state,
            pair_limit=remaining_limit,
        )
    else:
        regular_pairs = _limit_active_pairs_for_cycle(regular_pairs, market_map, state)

    selected_pairs = hot_pairs + regular_pairs
    if hot_pairs:
        incr_counter("worker.hot_pairs_selected", len(hot_pairs))
    return selected_pairs


def _mark_hot_pairs_processed(state, pairs):
    if not state.hot_pair_hashes or not pairs:
        return

    processed_hashes = {pair.pair_hash for pair in pairs}
    state.hot_pair_hashes = [
        pair_hash
        for pair_hash in state.hot_pair_hashes
        if pair_hash not in processed_hashes
    ]


def _record_timing(metric_name, started_at):
    elapsed_ms = max(0, int((time.monotonic() - started_at) * 1000))
    incr_counter(f"{metric_name}_ms_total", elapsed_ms)
    incr_counter(f"{metric_name}_count")


async def _process_candidates(db, orderbook_service, calculator, alert_manager, fanout_manager, state, retry_queue=None):
    pairs, market_map = await _load_candidate_context(db, state)
    if not pairs:
        return {
            "approved_pairs": 0,
            "active_pairs": 0,
            "pairs_with_books": 0,
            "skipped_pairs": 0,
            "opportunities": 0,
            "deliverable_opportunities": 0,
        }

    active_pairs = await _filter_skippable_pairs(pairs, state)
    active_pairs = _select_active_pairs_for_cycle(active_pairs, market_map, state)
    incr_counter("worker.active_pairs_loaded", len(active_pairs))
    if not active_pairs:
        return {
            "approved_pairs": len(pairs),
            "active_pairs": 0,
            "pairs_with_books": 0,
            "skipped_pairs": len(pairs),
            "opportunities": 0,
            "deliverable_opportunities": 0,
        }

    delivery_targets = await fanout_manager.get_delivery_targets()
    pair_fetch_concurrency = max(1, int(settings.ORDERBOOK_PREDICT_FUN_CONCURRENCY or 1))
    semaphore = asyncio.Semaphore(pair_fetch_concurrency)

    async def process_pair(pair):
        queued_at = time.monotonic()
        async with semaphore:
            _record_timing("worker.timing.pair_queue_wait", queued_at)
            return await _process_candidate_pair(
                orderbook_service,
                calculator,
                pair,
                market_map,
                delivery_targets,
            )

    pair_results = await asyncio.gather(
        *(process_pair(pair) for pair in active_pairs),
    )
    _mark_hot_pairs_processed(state, active_pairs)
    pairs_with_data = {
        result["pair_hash"]
        for result in pair_results
        if result["has_orderbooks"]
    }
    await _update_empty_counts(active_pairs, pairs_with_data, state)

    if _should_send_immediately():
        telegram_started_at = time.monotonic()
        await _send_all_deliveries(pair_results, calculator, retry_queue)
        _record_timing("worker.timing.telegram_send", telegram_started_at)

    return {
        "approved_pairs": len(pairs),
        "active_pairs": len(active_pairs),
        "pairs_with_books": sum(1 for result in pair_results if result["has_orderbooks"]),
        "skipped_pairs": max(0, len(active_pairs) - len(pairs_with_data)),
        "opportunities": sum(result["opportunities"] for result in pair_results),
        "deliverable_opportunities": sum(result["deliverable_opportunities"] for result in pair_results),
    }


async def _process_candidate_pair(
    orderbook_service,
    calculator,
    pair,
    market_map,
    delivery_targets,
):
    pair_stats = {
        "pair_hash": pair.pair_hash,
        "has_orderbooks": False,
        "opportunities": 0,
        "deliverable_opportunities": 0,
        "deliveries": [],
    }
    try:
        orderbook_started_at = time.monotonic()
        async with AsyncSessionLocal() as db:
            orderbooks_data = await orderbook_service.fetch_orderbooks_for_pairs(
                [pair],
                db,
                market_map=market_map,
            )
        _record_timing("worker.timing.orderbook_fetch", orderbook_started_at)
    except Exception as e:
        _record_timing("worker.timing.orderbook_fetch", orderbook_started_at)
        log.error(
            "failed to fetch orderbook for pair",
            pair_id=pair.id,
            error=format_error_details(e),
        )
        incr_counter("worker.pair_orderbook_failed")
        await send_system_error_notification("worker", f"fetch orderbook pair {pair.id}", e)
        return pair_stats

    for item in orderbooks_data:
        item_pair = item.get("pair") or pair
        if getattr(item_pair, "pair_hash", None) != pair.pair_hash:
            continue

        market_a = market_map.get(item_pair.market_id_a)
        market_b = market_map.get(item_pair.market_id_b)
        if market_a is None or market_b is None:
            continue

        pair_stats["has_orderbooks"] = True
        incr_counter("worker.pairs_with_orderbooks")

        directions = item.get("directions")
        calculate_started_at = time.monotonic()
        calc_results = calculator.calculate_opportunities(directions)
        _record_timing("worker.timing.calculate", calculate_started_at)
        if not calc_results:
            incr_counter("calculator.drop.no_profitable_directions")
            continue
        incr_counter("worker.calc_positive_spread", len(calc_results))

        for calc_result in calc_results:
            try:
                async with AsyncSessionLocal() as db:
                    alert_manager = AlertManager(db)
                    opportunity = await alert_manager.process_opportunity(item_pair, calc_result)
                    if not opportunity:
                        continue
                    incr_counter("worker.opportunities_created")

                    fanout_manager = FanoutManager(db)
                    fanout_started_at = time.monotonic()
                    deliveries = await fanout_manager.create_alert_deliveries(
                        opportunity,
                        market_a,
                        market_b,
                        delivery_targets=delivery_targets,
                        directions=directions,
                        calculator=calculator,
                    )
                    _record_timing("worker.timing.fanout", fanout_started_at)

                    if deliveries:
                        pair_stats["deliverable_opportunities"] += 1
                        pair_stats["deliveries"].append({
                            "deliveries": deliveries,
                            "opportunity": opportunity,
                            "pair": item_pair,
                            "market_a": market_a,
                            "market_b": market_b,
                            "directions": directions,
                        })

                    incr_counter("worker.opportunity_processed")
                    pair_stats["opportunities"] += 1
            except Exception as e:
                log.error(
                    "failed to process opportunity",
                    pair_id=pair.id,
                    error=format_error_details(e),
                )
                incr_counter("worker.opportunity_failed")
                await send_system_error_notification("worker", "process opportunity", e)

    return pair_stats


async def _send_all_deliveries(pair_results, calculator, retry_queue=None):
    all_delivery_batches = [
        delivery_batch
        for result in pair_results
        for delivery_batch in result.get("deliveries", [])
    ]

    if not all_delivery_batches:
        return

    send_concurrency = max(1, int(settings.TELEGRAM_SEND_CONCURRENCY or 1))
    semaphore = asyncio.Semaphore(send_concurrency)
    successful_opportunities = []

    async def send_batch(delivery_batch):
        async with semaphore:
            sent_count = await _send_delivery_alerts(
                delivery_batch["deliveries"],
                delivery_batch["opportunity"],
                delivery_batch["pair"],
                delivery_batch["market_a"],
                delivery_batch["market_b"],
                delivery_batch["directions"],
                calculator,
                retry_queue,
            )
            if sent_count > 0:
                successful_opportunities.append(delivery_batch["opportunity"])
            return sent_count

    results = await asyncio.gather(
        *(send_batch(batch) for batch in all_delivery_batches),
        return_exceptions=True,
    )

    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            log.error("delivery batch failed", batch_index=idx, error=format_error_details(result))

    if successful_opportunities:
        try:
            async with AsyncSessionLocal() as db:
                alert_manager = AlertManager(db)
                for opportunity in successful_opportunities:
                    await alert_manager.finalize_opportunity(opportunity)
        except Exception as e:
            log.error("failed to finalize opportunities", error=format_error_details(e))


async def _send_delivery_alerts(
    deliveries,
    opportunity,
    pair,
    market_a,
    market_b,
    directions,
    calculator,
    retry_queue=None,
):
    send_results = []

    for delivery in deliveries:
        try:
            sent = await send_alert_immediately(
                delivery["alert"],
                opportunity,
                pair,
                market_a,
                market_b,
                delivery["preferences"],
                directions,
                calculator,
                prepared_opportunity=delivery.get("opportunity"),
            )
            if sent:
                incr_counter("worker.immediate_send_success")
                send_results.append(1)
            else:
                incr_counter("worker.immediate_send_failed")
                send_results.append(0)
                if retry_queue is not None and getattr(delivery["alert"], "status", None) == "failed":
                    retry_queue.enqueue({
                        "delivery": delivery,
                        "opportunity": opportunity,
                        "pair": pair,
                        "market_a": market_a,
                        "market_b": market_b,
                        "directions": directions,
                    })
        except Exception as e:
            log.error("delivery send failed", error=format_error_details(e))
            incr_counter("worker.immediate_send_failed")
            send_results.append(0)

    return sum(send_results)


async def _retry_alert_delivery(item, calculator):
    delivery = item["delivery"]
    sent = await send_alert_immediately(
        delivery["alert"],
        item["opportunity"],
        item["pair"],
        item["market_a"],
        item["market_b"],
        delivery["preferences"],
        item["directions"],
        calculator,
        prepared_opportunity=delivery.get("opportunity"),
    )
    if not sent:
        incr_counter("worker.retry_send_failed")
        return False

    incr_counter("worker.retry_send_success")
    try:
        async with AsyncSessionLocal() as db:
            await AlertManager(db).finalize_opportunity(item["opportunity"])
    except Exception as e:
        log.error("failed to finalize retried opportunity", error=format_error_details(e))

    return True


def _pair_empty_count_key(pair_hash):
    return f"worker:pair-empty-count:{pair_hash}"


def _market_signature_fingerprint(market):
    outcomes_hash = hashlib.md5(
        json.dumps(getattr(market, "outcomes_json", None), sort_keys=True, ensure_ascii=True).encode()
    ).hexdigest()
    payload_hash = hashlib.md5(
        json.dumps(getattr(market, "raw_payload_json", None), sort_keys=True, ensure_ascii=True).encode()
    ).hexdigest()
    return (
        getattr(market, "title", None),
        getattr(market, "category", None),
        outcomes_hash,
        payload_hash,
        getattr(market, "status", None),
        getattr(market, "updated_at", None),
    )


def _build_cached_market_signatures(markets, matcher, state):
    signatures = {}

    for market in markets:
        market_snapshot = _snapshot_market(market)
        fingerprint = _market_signature_fingerprint(market_snapshot)
        cached_entry = state.market_signature_cache.get(market.id)
        if cached_entry is not None and cached_entry["fingerprint"] == fingerprint:
            cached_entry["last_seen_at"] = time.monotonic()
            cached_entry["signature"]["market"] = market_snapshot
            signatures[market.id] = cached_entry["signature"]
            continue

        signature = matcher.build_market_signature(market_snapshot)
        signature["market"] = market_snapshot
        state.market_signature_cache[market.id] = {
            "fingerprint": fingerprint,
            "signature": signature,
            "last_seen_at": time.monotonic(),
        }
        signatures[market.id] = signature

    return signatures


def _build_candidate_index_from_signatures(signatures):
    index = {
        "tokens": {},
        "condition_ids": {},
    }

    for signature in signatures.values():
        for token in signature["tokens"]:
            index["tokens"].setdefault(token, []).append(signature)

        for condition_id in signature["condition_ids"]:
            index["condition_ids"].setdefault(condition_id, []).append(signature)

    return index


def _prune_market_signature_cache(state, *market_groups):
    active_market_ids = set()
    for group in market_groups:
        for item in group:
            if isinstance(item, (int, str)):
                active_market_ids.add(int(item))
                continue
            market_id = getattr(item, "id", None)
            if market_id is not None:
                active_market_ids.add(market_id)
    stale_market_ids = [
        market_id
        for market_id in state.market_signature_cache
        if market_id not in active_market_ids
    ]
    for market_id in stale_market_ids:
        state.market_signature_cache.pop(market_id, None)


async def _get_empty_counts(pair_hashes, state):
    if not pair_hashes:
        return {}

    try:
        redis = get_redis()
        if redis is not None:
            keys = [_pair_empty_count_key(pair_hash) for pair_hash in pair_hashes]
            values = await redis.mget(keys)
            return {
                pair_hash: int(value or 0)
                for pair_hash, value in zip(pair_hashes, values)
            }
    except Exception as exc:
        log.debug("redis empty count fetch failed", error=str(exc))

    counts = {}
    for pair_hash in pair_hashes:
        entry = state.pair_empty_counts.get(pair_hash)
        counts[pair_hash] = int(entry[0]) if entry else 0

    return counts


def _get_empty_count_client():
    return get_redis()


async def _clear_empty_count(pair_hash, state):
    try:
        redis = get_redis()
        if redis is not None:
            await redis.delete(_pair_empty_count_key(pair_hash))
    except Exception:
        pass

    state.pair_empty_counts.pop(pair_hash, None)


async def _increment_empty_count(pair_hash, state):
    now = time.monotonic()
    try:
        redis = get_redis()
        if redis is not None:
            key = _pair_empty_count_key(pair_hash)
            pipe = redis.pipeline()
            pipe.incr(key)
            pipe.expire(key, _EMPTY_COUNT_TTL_SECONDS)
            results = await pipe.execute()
            return int(results[0])
    except Exception as exc:
        log.debug("redis increment empty count failed", error=str(exc))
    entry = state.pair_empty_counts.get(pair_hash)
    count = (int(entry[0]) if entry else 0) + 1
    state.pair_empty_counts[pair_hash] = (count, now)
    return count


async def _clear_empty_counts_for_pairs(pairs, state):
    for pair in pairs:
        if getattr(pair, "status", None) == "stale":
            await _clear_empty_count(pair.pair_hash, state)


async def _filter_skippable_pairs(pairs, state):
    empty_counts = await _get_empty_counts([pair.pair_hash for pair in pairs], state)
    active = []
    for pair in pairs:
        empty_count = empty_counts.get(pair.pair_hash, 0)
        if empty_count >= settings.EMPTY_ORDERBOOK_THRESHOLD:
            continue
        active.append(pair)
    return active


def _should_send_immediately():
    return settings.APP_RUNTIME_MODE in {"all", "worker"}


def _limit_active_pairs_for_cycle(pairs, market_map, state, pair_limit=None):
    if pair_limit is None:
        pair_limit = int(settings.MAX_ACTIVE_PAIRS_PER_CYCLE or 0)
    if pair_limit <= 0 or len(pairs) <= pair_limit:
        return list(pairs)

    prioritized_pairs = sorted(
        ((_pair_cycle_priority(pair, market_map), pair) for pair in pairs),
        key=lambda item: item[0],
    )
    live_pairs = [
        pair for priority, pair in prioritized_pairs
        if priority[0] == 0
    ]
    backlog_pairs = [
        pair for priority, pair in prioritized_pairs
        if priority[0] != 0
    ]

    selected_pairs = []
    if live_pairs:
        live_limit = min(pair_limit, len(live_pairs))
        selected_pairs.extend(
            _take_rotating_window(live_pairs, live_limit, state, "live")
        )

    remaining_limit = pair_limit - len(selected_pairs)
    if remaining_limit > 0 and backlog_pairs:
        selected_pairs.extend(
            _take_rotating_window(backlog_pairs, remaining_limit, state, "backlog")
        )

    return selected_pairs


def _take_rotating_window(pairs, limit, state, bucket_name):
    if limit <= 0 or not pairs:
        return []

    offset = int(state.pair_cycle_offsets.get(bucket_name, 0) or 0) % len(pairs)
    rotated_pairs = pairs[offset:] + pairs[:offset]
    selected_pairs = rotated_pairs[:limit]
    state.pair_cycle_offsets[bucket_name] = (offset + limit) % len(pairs)
    return selected_pairs


def _pair_cycle_priority(pair, market_map):
    market_a = market_map.get(pair.market_id_a)
    market_b = market_map.get(pair.market_id_b)
    close_at = extract_pair_close_datetime(market_a, market_b)
    if close_at is not None and close_at.tzinfo is None:
        close_at = close_at.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    is_live_window = False
    if close_at is not None:
        is_live_window = close_at <= now + timedelta(hours=24)

    if close_at is None:
        return (1, datetime.max.replace(tzinfo=timezone.utc), pair.id)

    return (0 if is_live_window else 1, close_at, pair.id)


async def _update_empty_counts(checked_pairs, pairs_with_data, state):
    redis = _get_empty_count_client()
    if redis is not None:
        try:
            pipe = redis.pipeline()
            for pair in checked_pairs:
                key = _pair_empty_count_key(pair.pair_hash)
                if pair.pair_hash in pairs_with_data:
                    pipe.delete(key)
                    continue
                pipe.incr(key)
                pipe.expire(key, _EMPTY_COUNT_TTL_SECONDS)
            await pipe.execute()
            return
        except Exception:
            pass

    for pair in checked_pairs:
        if pair.pair_hash in pairs_with_data:
            await _clear_empty_count(pair.pair_hash, state)
        else:
            await _increment_empty_count(pair.pair_hash, state)


def _candidate_markets_for_signature(source_signature, matcher, candidate_index):
    direct_matches = []
    seen_direct_ids = set()

    for condition_id in source_signature.get("condition_ids", []):
        for candidate_signature in candidate_index.get("condition_ids", {}).get(condition_id, []):
            market = candidate_signature["market"]
            if market.id in seen_direct_ids:
                continue
            seen_direct_ids.add(market.id)
            direct_matches.append(candidate_signature)

    if direct_matches:
        return direct_matches

    candidates_by_id = {}
    shared_token_count = defaultdict(int)

    for token in source_signature["tokens"]:
        for candidate_signature in candidate_index.get("tokens", {}).get(token, []):
            market = candidate_signature["market"]
            candidates_by_id[market.id] = candidate_signature
            shared_token_count[market.id] += 1

    if not candidates_by_id:
        return []

    ranked_candidates = sorted(
        candidates_by_id.values(),
        key=lambda candidate_signature: (
            matcher.candidate_rank_score(
                source_signature,
                candidate_signature,
                shared_token_count[candidate_signature["market"].id],
            ),
            len(getattr(candidate_signature["market"], "title", "")),
        ),
        reverse=True,
    )
    return ranked_candidates[:matcher.max_ranked_candidates]


async def _load_market_map_for_pairs(db, pairs):
    market_ids = set()
    for pair in pairs:
        market_ids.add(pair.market_id_a)
        market_ids.add(pair.market_id_b)

    if not market_ids:
        return {}

    stmt = select(Market).where(Market.id.in_(market_ids))
    markets = (await db.execute(stmt)).scalars().all()
    return {market.id: market for market in markets}


if __name__ == "__main__":
    asyncio.run(run_sync_loop())
