import asyncio
import time
from collections import defaultdict
import json
from datetime import datetime, timezone
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
from sqlalchemy.future import select
from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError

log = get_logger("worker")
_EMPTY_COUNTS_MAX_SIZE = 1000
_SIGNATURE_CACHE_MAX_SIZE = 20000
# pair_hash -> (count, monotonic_timestamp)
_pair_empty_counts = {}
_market_signature_cache = {}
_candidate_context_cache = {
    "loaded": False,
    "pairs": [],
    "market_map": {},
}
_last_full_pair_rematch_completed_at = None
_EMPTY_COUNT_TTL_SECONDS = max(settings.MARKET_REFRESH_SECONDS * 20, 3600)


async def run_sync_loop():
    # infinite market update loop
    while True:
        try:
            incr_counter("worker.cycle_started")
            async with AsyncSessionLocal() as session:
                await _run_cycle(session)
            incr_counter("worker.cycle_completed")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # logging sync error
            log.error("sync loop error", error=format_error_details(e))
            incr_counter("worker.cycle_failed")
            await send_system_error_notification("worker", "sync loop", e)
            
        await asyncio.sleep(settings.MARKET_REFRESH_SECONDS)


async def _run_cycle(db):
    ingestion = IngestionService(db)
    matcher = MatcherService()
    orderbook_service = OrderbookService()
    calculator = ArbitrageCalculator()
    alert_manager = AlertManager(db)
    fanout_manager = FanoutManager(db)

    try:
        sync_result = await ingestion.sync_markets()
        if _should_run_full_pair_rematch(time.monotonic()):
            _invalidate_candidate_context_cache()
            await _upsert_market_pairs(db, matcher, None)
            _mark_full_pair_rematch_completed()
        elif sync_result is True:
            _invalidate_candidate_context_cache()
            await _upsert_market_pairs(db, matcher, None)
            _mark_full_pair_rematch_completed()
        else:
            changed_market_ids_by_platform = _extract_changed_market_ids_by_platform(sync_result)
            if _has_changed_market_ids(changed_market_ids_by_platform):
                _invalidate_candidate_context_cache()
                await _upsert_market_pairs(
                    db,
                    matcher,
                    changed_market_ids_by_platform,
                )
        cycle_stats = await _process_candidates(
            db,
            orderbook_service,
            calculator,
            alert_manager,
            fanout_manager,
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
    finally:
        await ingestion.close()
        await orderbook_service.close()


def _extract_changed_market_ids_by_platform(sync_result):
    if not isinstance(sync_result, dict):
        return {
            "polymarket": set(),
            "predict_fun": set(),
        }

    result = sync_result
    changed_market_ids_by_platform = result.get("changed_market_ids_by_platform") or {}
    return {
        "polymarket": set(changed_market_ids_by_platform.get("polymarket") or []),
        "predict_fun": set(changed_market_ids_by_platform.get("predict_fun") or []),
    }


def _has_changed_market_ids(changed_market_ids_by_platform):
    return any(changed_market_ids_by_platform.values())


def _should_run_full_pair_rematch(now):
    interval = max(
        float(settings.MATCHER_FULL_REMATCH_INTERVAL_SECONDS),
        float(settings.MARKET_REFRESH_SECONDS),
    )
    if interval <= 0:
        return True

    if _last_full_pair_rematch_completed_at is None:
        return True

    return (now - _last_full_pair_rematch_completed_at) >= interval


def _mark_full_pair_rematch_completed():
    global _last_full_pair_rematch_completed_at
    _last_full_pair_rematch_completed_at = time.monotonic()


def _invalidate_candidate_context_cache():
    _candidate_context_cache["loaded"] = False
    _candidate_context_cache["pairs"] = []
    _candidate_context_cache["market_map"] = {}


async def _load_candidate_context(db, force_refresh=False):
    if _candidate_context_cache["loaded"] and not force_refresh:
        return (
            list(_candidate_context_cache["pairs"]),
            dict(_candidate_context_cache["market_map"]),
        )

    pair_stmt = select(MarketPair).where(
        MarketPair.status.in_(["auto_approved", "approved"])
    )
    pairs = (await db.execute(pair_stmt)).scalars().all()
    market_map = await _load_market_map_for_pairs(db, pairs)
    _candidate_context_cache["loaded"] = True
    _candidate_context_cache["pairs"] = list(pairs)
    _candidate_context_cache["market_map"] = dict(market_map)
    return pairs, market_map


async def _upsert_market_pairs(db, matcher, changed_market_ids_by_platform):
    full_rematch = changed_market_ids_by_platform is None
    changed_poly_ids = set()
    changed_pf_ids = set()
    changed_market_ids = set()
    if not full_rematch:
        changed_poly_ids = set(changed_market_ids_by_platform.get("polymarket") or [])
        changed_pf_ids = set(changed_market_ids_by_platform.get("predict_fun") or [])
        changed_market_ids = changed_poly_ids.union(changed_pf_ids)
        if not changed_market_ids:
            return

    poly_markets, pf_markets = await _load_active_markets_by_platform(db)
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
        poly_signatures = _build_cached_market_signatures(poly_changed, matcher)
        pf_signatures = _build_cached_market_signatures(pf_markets, matcher)
        pf_index = _build_candidate_index_from_signatures(pf_signatures)
        reached_limit = _match_changed_polymarket_markets(
            poly_changed,
            poly_signatures,
            pf_index,
            matcher,
            matched_pairs,
            pair_limit,
        )

    if pf_changed and poly_markets and not reached_limit:
        pf_signatures = _build_cached_market_signatures(pf_changed, matcher)
        poly_signatures = _build_cached_market_signatures(poly_markets, matcher)
        poly_index = _build_candidate_index_from_signatures(poly_signatures)
        _match_changed_predict_fun_markets(
            pf_changed,
            pf_signatures,
            poly_index,
            matcher,
            matched_pairs,
            pair_limit,
        )

    if full_rematch:
        existing_pairs = await _load_active_pairs(db)
    else:
        existing_pairs = await _load_pairs_for_market_ids(db, changed_market_ids)
    new_pairs, has_updates = _reconcile_market_pairs(existing_pairs, matched_pairs)
    if has_updates:
        stale_pairs = [pair for pair in existing_pairs if pair.status == "stale"]
        await _clear_empty_counts_for_pairs(stale_pairs)
    if not new_pairs and not has_updates:
        _prune_market_signature_cache(poly_markets, pf_markets)
        return

    if new_pairs:
        db.add_all(new_pairs)
    try:
        await db.flush()
        await db.commit()
    except IntegrityError:
        await db.rollback()

    _prune_market_signature_cache(poly_markets, pf_markets)


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

    # postgresql ограничивает количество параметров запроса до 32767,
    # два IN-клауза удваивают число, поэтому батчим по 10000
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


def _match_changed_polymarket_markets(changed_markets, changed_signatures, candidate_index, matcher, matched_pairs, pair_limit):
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
            pair = matcher.match_candidates(
                market,
                candidate_signature["market"],
                poly_signature=market_signature,
                pf_signature=candidate_signature,
            )
            if pair:
                matched_pairs[pair.pair_hash] = pair
            if pair_limit and len(matched_pairs) >= pair_limit:
                reached_limit = True
                break

    return reached_limit


def _match_changed_predict_fun_markets(changed_markets, changed_signatures, candidate_index, matcher, matched_pairs, pair_limit):
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

    for pair_hash, matched_pair in matched_pairs_by_hash.items():
        existing_pair = existing_by_hash.pop(pair_hash, None)
        if existing_pair is None:
            new_pairs.append(matched_pair)
            continue

        if _refresh_existing_pair(existing_pair, matched_pair):
            has_updates = True

    if _mark_stale_pairs(existing_by_hash.values()):
        has_updates = True

    return new_pairs, has_updates


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


async def _process_candidates(db, orderbook_service, calculator, alert_manager, fanout_manager):
    pairs, market_map = await _load_candidate_context(db)
    if not pairs:
        return {
            "approved_pairs": 0,
            "active_pairs": 0,
            "pairs_with_books": 0,
            "skipped_pairs": 0,
            "opportunities": 0,
            "deliverable_opportunities": 0,
        }

    active_pairs = await _filter_skippable_pairs(pairs)
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

    orderbooks_data = await orderbook_service.fetch_orderbooks_for_pairs(
        active_pairs,
        db,
        market_map=market_map,
    )
    pairs_with_data = {item["pair"].pair_hash for item in orderbooks_data}
    await _update_empty_counts(active_pairs, pairs_with_data)
    pairs_with_books_count = 0
    opportunity_count = 0
    deliverable_opportunity_count = 0
    delivery_targets = await fanout_manager.get_delivery_targets()
    for item in orderbooks_data:
        pair = item["pair"]
        market_a = market_map.get(pair.market_id_a)
        market_b = market_map.get(pair.market_id_b)
        if market_a is None or market_b is None:
            continue

        pairs_with_books_count += 1
        incr_counter("worker.pairs_with_orderbooks")

        directions = item.get("directions")
        calc_results = calculator.calculate_opportunities(directions)
        if not calc_results:
            incr_counter("calculator.drop.no_profitable_directions")
            continue
        incr_counter("worker.calc_positive_spread", len(calc_results))

        for calc_result in calc_results:
            try:
                opportunity = await alert_manager.process_opportunity(pair, calc_result)
                if not opportunity:
                    continue
                incr_counter("worker.opportunities_created")
                deliveries = await fanout_manager.create_alert_deliveries(
                    opportunity,
                    market_a,
                    market_b,
                    delivery_targets=delivery_targets,
                    skip_existing_lookup=True,
                    directions=directions,
                    calculator=calculator,
                )
                for delivery in deliveries:
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
                    else:
                        incr_counter("worker.immediate_send_failed")
                if deliveries:
                    deliverable_opportunity_count += 1
                opportunity.fanout_status = "processed"
                opportunity.fanout_processed_at = datetime.now(timezone.utc)
                opportunity.fanout_error_message = None
                await db.commit()
                await alert_manager.finalize_opportunity(opportunity)
                incr_counter("worker.opportunity_processed")
                opportunity_count += 1
            except Exception as e:
                log.error(
                    "failed to process opportunity",
                    pair_id=pair.id,
                    error=format_error_details(e),
                )
                incr_counter("worker.opportunity_failed")
                try:
                    await db.rollback()
                except Exception:
                    pass
                await send_system_error_notification("worker", f"process opportunity pair {pair.id}", e)

    return {
        "approved_pairs": len(pairs),
        "active_pairs": len(active_pairs),
        "pairs_with_books": pairs_with_books_count,
        "skipped_pairs": max(0, len(active_pairs) - pairs_with_books_count),
        "opportunities": opportunity_count,
        "deliverable_opportunities": deliverable_opportunity_count,
    }


def _pair_empty_count_key(pair_hash):
    return f"worker:pair-empty-count:{pair_hash}"


def _market_signature_fingerprint(market):
    return (
        getattr(market, "title", None),
        getattr(market, "category", None),
        json.dumps(getattr(market, "outcomes_json", None), sort_keys=True, ensure_ascii=True),
        json.dumps(getattr(market, "raw_payload_json", None), sort_keys=True, ensure_ascii=True),
        getattr(market, "status", None),
        getattr(market, "updated_at", None),
    )


def _build_cached_market_signatures(markets, matcher):
    signatures = {}

    for market in markets:
        fingerprint = _market_signature_fingerprint(market)
        cached_entry = _market_signature_cache.get(market.id)
        if cached_entry is not None and cached_entry["fingerprint"] == fingerprint:
            cached_entry["last_seen_at"] = time.monotonic()
            cached_entry["signature"]["market"] = market
            signatures[market.id] = cached_entry["signature"]
            continue

        signature = matcher.build_market_signature(market)
        _market_signature_cache[market.id] = {
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


def _prune_market_signature_cache(*market_groups):
    active_market_ids = {
        market.id
        for group in market_groups
        for market in group
    }
    stale_market_ids = [
        market_id
        for market_id in _market_signature_cache
        if market_id not in active_market_ids
    ]
    for market_id in stale_market_ids:
        _market_signature_cache.pop(market_id, None)

    if len(_market_signature_cache) <= _SIGNATURE_CACHE_MAX_SIZE:
        return

    oldest_market_ids = sorted(
        _market_signature_cache,
        key=lambda market_id: _market_signature_cache[market_id]["last_seen_at"],
    )
    for market_id in oldest_market_ids[: max(1, len(oldest_market_ids) // 4)]:
        _market_signature_cache.pop(market_id, None)


async def _get_empty_counts(pair_hashes):
    if not pair_hashes:
        return {}

    try:
        redis = await get_redis()
        if redis is not None:
            keys = [_pair_empty_count_key(pair_hash) for pair_hash in pair_hashes]
            values = await redis.mget(keys)
            return {
                pair_hash: int(value or 0)
                for pair_hash, value in zip(pair_hashes, values)
            }
    except Exception:
        pass

    counts = {}
    for pair_hash in pair_hashes:
        entry = _pair_empty_counts.get(pair_hash)
        counts[pair_hash] = int(entry[0]) if entry else 0

    return counts


async def _get_empty_count_client():
    try:
        redis = await get_redis()
        if redis is not None:
            return redis
    except Exception:
        pass

    return None


async def _clear_empty_count(pair_hash):
    try:
        redis = await get_redis()
        if redis is not None:
            await redis.delete(_pair_empty_count_key(pair_hash))
    except Exception:
        pass

    _pair_empty_counts.pop(pair_hash, None)


async def _increment_empty_count(pair_hash):
    now = time.monotonic()
    try:
        redis = await get_redis()
        if redis is not None:
            key = _pair_empty_count_key(pair_hash)
            pipe = redis.pipeline()
            pipe.incr(key)
            pipe.expire(key, _EMPTY_COUNT_TTL_SECONDS)
            results = await pipe.execute()
            return int(results[0])
    except Exception:
        pass
    entry = _pair_empty_counts.get(pair_hash)
    count = (int(entry[0]) if entry else 0) + 1
    _pair_empty_counts[pair_hash] = (count, now)
    return count


async def _clear_empty_counts_for_pairs(pairs):
    for pair in pairs:
        if getattr(pair, "status", None) == "stale":
            await _clear_empty_count(pair.pair_hash)


async def _filter_skippable_pairs(pairs):
    empty_counts = await _get_empty_counts([pair.pair_hash for pair in pairs])
    active = []
    for pair in pairs:
        empty_count = empty_counts.get(pair.pair_hash, 0)
        if empty_count >= settings.EMPTY_ORDERBOOK_THRESHOLD:
            continue
        active.append(pair)
    return active


async def _update_empty_counts(checked_pairs, pairs_with_data):
    redis = await _get_empty_count_client()
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
            await _clear_empty_count(pair.pair_hash)
        else:
            await _increment_empty_count(pair.pair_hash)

    # evict oldest entries when dict grows too large
    if len(_pair_empty_counts) > _EMPTY_COUNTS_MAX_SIZE:
        sorted_keys = sorted(
            _pair_empty_counts,
            key=lambda k: _pair_empty_counts[k][1],
        )
        for key in sorted_keys[:len(sorted_keys) // 2]:
            _pair_empty_counts.pop(key, None)


def _candidate_markets_for_poly(poly_signature, matcher, pf_index):
    return _candidate_markets_for_signature(poly_signature, matcher, pf_index)


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