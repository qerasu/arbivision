import asyncio
import time
from collections import defaultdict
import json
from arbitrage_bot.core.database import AsyncSessionLocal
from arbitrage_bot.core.redis import get_redis
from arbitrage_bot.models.orm import Market, MarketPair
from arbitrage_bot.services.ingestion import IngestionService
from arbitrage_bot.services.matcher import MatcherService
from arbitrage_bot.services.orderbook import OrderbookService
from arbitrage_bot.services.calculator import ArbitrageCalculator
from arbitrage_bot.services.alert_manager import AlertManager
from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.core.observability import incr_counter
from arbitrage_bot.services.system_notifier import format_error_details, send_system_error_notification
from sqlalchemy.future import select
from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError

log = get_logger("worker")
_EMPTY_COUNTS_MAX_SIZE = 1000
_SIGNATURE_CACHE_MAX_SIZE = 20000
# pair_hash -> (count, monotonic_timestamp)
_pair_empty_counts = {}
_market_signature_cache = {}
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

    try:
        await ingestion.sync_markets()
        await _upsert_market_pairs(db, matcher)
        cycle_stats = await _process_candidates(db, orderbook_service, calculator, alert_manager)
        log.info(
            "worker cycle summary",
            approved_pairs=cycle_stats["approved_pairs"],
            active_pairs=cycle_stats["active_pairs"],
            pairs_with_books=cycle_stats["pairs_with_books"],
            skipped_pairs=cycle_stats["skipped_pairs"],
            opportunities=cycle_stats["opportunities"],
        )
    finally:
        await ingestion.close()
        await orderbook_service.close()


async def _upsert_market_pairs(db, matcher):
    poly_stmt = select(Market).where(
        and_(Market.platform == "polymarket", Market.status == "active")
    )
    pf_stmt = select(Market).where(
        and_(Market.platform == "predict_fun", Market.status == "active")
    )

    poly_markets = (await db.execute(poly_stmt)).scalars().all()
    pf_markets = (await db.execute(pf_stmt)).scalars().all()
    if not poly_markets or not pf_markets:
        return

    poly_signatures = _build_cached_market_signatures(poly_markets, matcher)
    pf_signatures = _build_cached_market_signatures(pf_markets, matcher)
    pf_index = _build_candidate_index_from_signatures(pf_signatures)
    matched_pairs = {}
    pair_limit = settings.MAX_MARKET_PAIRS_PER_LOOP
    reached_limit = False
    for poly_market in poly_markets:
        if reached_limit:
            break
        poly_signature = poly_signatures.get(poly_market.id)
        if poly_signature is None:
            continue
        for pf_signature in _candidate_markets_for_poly(poly_signature, matcher, pf_index):
            pair = matcher.match_candidates(
                poly_market,
                pf_signature["market"],
                poly_signature=poly_signature,
                pf_signature=pf_signature,
            )
            if pair:
                matched_pairs[pair.pair_hash] = pair
            if pair_limit and len(matched_pairs) >= pair_limit:
                reached_limit = True
                break

    if not matched_pairs:
        active_stmt = select(MarketPair).where(MarketPair.status.in_(["auto_approved", "approved"]))
        existing_pairs = (await db.execute(active_stmt)).scalars().all()
        has_changes = _mark_stale_pairs(existing_pairs)
        if has_changes:
            await _clear_empty_counts_for_pairs(existing_pairs)
        if not has_changes:
            return
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
        return

    relevant_hashes = list(matched_pairs.keys())
    stmt = select(MarketPair).where(
        or_(
            MarketPair.status.in_(["auto_approved", "approved"]),
            MarketPair.pair_hash.in_(relevant_hashes)
        )
    )
    existing_pairs = (await db.execute(stmt)).scalars().all()
    new_pairs, has_updates = _reconcile_market_pairs(existing_pairs, matched_pairs)
    if has_updates:
        stale_pairs = [pair for pair in existing_pairs if pair.status == "stale"]
        await _clear_empty_counts_for_pairs(stale_pairs)
    if not new_pairs and not has_updates:
        return

    if new_pairs:
        db.add_all(new_pairs)
    try:
        await db.flush()
        await db.commit()
    except IntegrityError:
        # protect against concurrent workers creating same pair
        await db.rollback()

    _prune_market_signature_cache(poly_markets, pf_markets)


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


async def _process_candidates(db, orderbook_service, calculator, alert_manager):
    pair_stmt = select(MarketPair).where(
        MarketPair.status.in_(["auto_approved", "approved"])
    )
    pairs = (await db.execute(pair_stmt)).scalars().all()
    if not pairs:
        return {
            "approved_pairs": 0,
            "active_pairs": 0,
            "pairs_with_books": 0,
            "skipped_pairs": 0,
            "opportunities": 0,
        }

    active_pairs = await _filter_skippable_pairs(pairs)
    if not active_pairs:
        return {
            "approved_pairs": len(pairs),
            "active_pairs": 0,
            "pairs_with_books": 0,
            "skipped_pairs": len(pairs),
            "opportunities": 0,
        }

    market_map = await _load_market_map_for_pairs(db, active_pairs)
    orderbooks_data = await orderbook_service.fetch_orderbooks_for_pairs(active_pairs, db)

    pairs_with_data = {item["pair"].pair_hash for item in orderbooks_data}
    await _update_empty_counts(active_pairs, pairs_with_data)
    opportunity_count = 0
    for item in orderbooks_data:
        pair = item["pair"]
        directions = item.get("directions")
        calc_results = calculator.calculate_opportunities(directions)
        if not calc_results:
            continue

        market_a = market_map.get(pair.market_id_a)
        market_b = market_map.get(pair.market_id_b)
        for calc_result in calc_results:
            try:
                await alert_manager.process_opportunity(
                    pair,
                    calc_result,
                    market_a=market_a,
                    market_b=market_b,
                )
                incr_counter("worker.opportunity_processed")
                opportunity_count += 1
            except Exception as e:
                log.error(
                    "failed to process opportunity",
                    pair_id=pair.id,
                    error=format_error_details(e),
                )
                incr_counter("worker.opportunity_failed")
                await send_system_error_notification("worker", f"process opportunity pair {pair.id}", e)

    return {
        "approved_pairs": len(pairs),
        "active_pairs": len(active_pairs),
        "pairs_with_books": len(orderbooks_data),
        "skipped_pairs": max(0, len(active_pairs) - len(orderbooks_data)),
        "opportunities": opportunity_count,
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


async def _get_empty_count(pair_hash):
    try:
        redis = await get_redis()
        if redis is not None:
            value = await redis.get(_pair_empty_count_key(pair_hash))
            if value is None:
                return 0
            return int(value)
    except Exception:
        pass

    entry = _pair_empty_counts.get(pair_hash)
    if not entry:
        return 0
    return int(entry[0])


async def _set_empty_count(pair_hash, count):
    now = time.monotonic()
    try:
        redis = await get_redis()
        if redis is not None:
            await redis.setex(
                _pair_empty_count_key(pair_hash),
                _EMPTY_COUNT_TTL_SECONDS,
                str(count),
            )
    except Exception:
        _pair_empty_counts[pair_hash] = (count, now)
    else:
        _pair_empty_counts[pair_hash] = (count, now)


async def _clear_empty_count(pair_hash):
    try:
        redis = await get_redis()
        if redis is not None:
            await redis.delete(_pair_empty_count_key(pair_hash))
    except Exception:
        pass

    _pair_empty_counts.pop(pair_hash, None)


async def _clear_empty_counts_for_pairs(pairs):
    for pair in pairs:
        if getattr(pair, "status", None) == "stale":
            await _clear_empty_count(pair.pair_hash)


async def _filter_skippable_pairs(pairs):
    active = []
    for pair in pairs:
        empty_count = await _get_empty_count(pair.pair_hash)
        if empty_count >= settings.EMPTY_ORDERBOOK_THRESHOLD:
            continue
        active.append(pair)
    return active


async def _update_empty_counts(checked_pairs, pairs_with_data):
    now = time.monotonic()
    for pair in checked_pairs:
        if pair.pair_hash in pairs_with_data:
            await _clear_empty_count(pair.pair_hash)
        else:
            prev_count = await _get_empty_count(pair.pair_hash)
            await _set_empty_count(pair.pair_hash, prev_count + 1)

    # evict oldest entries when dict grows too large
    if len(_pair_empty_counts) > _EMPTY_COUNTS_MAX_SIZE:
        sorted_keys = sorted(
            _pair_empty_counts,
            key=lambda k: _pair_empty_counts[k][1],
        )
        for key in sorted_keys[:len(sorted_keys) // 2]:
            _pair_empty_counts.pop(key, None)


def _candidate_markets_for_poly(poly_signature, matcher, pf_index):
    direct_matches = []
    seen_direct_ids = set()

    for condition_id in poly_signature.get("condition_ids", []):
        for pf_signature in pf_index.get("condition_ids", {}).get(condition_id, []):
            market = pf_signature["market"]
            if market.id in seen_direct_ids:
                continue
            seen_direct_ids.add(market.id)
            direct_matches.append(pf_signature)

    if direct_matches:
        return direct_matches

    candidates_by_id = {}
    shared_token_count = defaultdict(int)

    for token in poly_signature["tokens"]:
        for pf_signature in pf_index.get("tokens", {}).get(token, []):
            market = pf_signature["market"]
            candidates_by_id[market.id] = pf_signature
            shared_token_count[market.id] += 1

    if not candidates_by_id:
        return []

    ranked_candidates = sorted(
        candidates_by_id.values(),
        key=lambda pf_signature: (
            matcher.candidate_rank_score(
                poly_signature,
                pf_signature,
                shared_token_count[pf_signature["market"].id],
            ),
            len(getattr(pf_signature["market"], "title", "")),
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