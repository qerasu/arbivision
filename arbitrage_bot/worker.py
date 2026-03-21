import asyncio
from collections import defaultdict
from arbitrage_bot.core.database import AsyncSessionLocal
from arbitrage_bot.models.orm import Market, MarketPair
from arbitrage_bot.services.ingestion import IngestionService
from arbitrage_bot.services.matcher import MatcherService
from arbitrage_bot.services.orderbook import OrderbookService
from arbitrage_bot.services.calculator import ArbitrageCalculator
from arbitrage_bot.services.alert_manager import AlertManager
from arbitrage_bot.core.config import settings
from arbitrage_bot.services.system_notifier import format_error_details, send_system_error_notification
from arbitrage_bot.tg_bot.preferences import get_global_preferences
from sqlalchemy.future import select
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError


async def run_sync_loop():
    # infinite market update loop
    while True:
        try:
            async with AsyncSessionLocal() as session:
                await _run_cycle(session)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # logging sync error
            print(f"error in sync loop: {format_error_details(e)}")
            await send_system_error_notification("worker", "sync loop", e)
        await asyncio.sleep(settings.MARKET_REFRESH_SECONDS)


async def _run_cycle(db):
    ingestion = IngestionService(db)
    matcher = MatcherService(db)
    orderbook_service = OrderbookService()
    calculator = ArbitrageCalculator()
    alert_manager = AlertManager(db)

    try:
        await ingestion.sync_markets()
        await _upsert_market_pairs(db, matcher)
        await _process_candidates(db, orderbook_service, calculator, alert_manager)
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

    pf_index = matcher.build_candidate_index(pf_markets)
    matched_pairs = {}
    for poly_market in poly_markets:
        for pf_market in _candidate_markets_for_poly(poly_market, matcher, pf_index, pf_markets):
            pair = matcher.match_candidates(poly_market, pf_market)
            if pair:
                matched_pairs[pair.pair_hash] = pair
            if settings.MAX_MARKET_PAIRS_PER_LOOP and len(matched_pairs) >= settings.MAX_MARKET_PAIRS_PER_LOOP:
                break
        if settings.MAX_MARKET_PAIRS_PER_LOOP and len(matched_pairs) >= settings.MAX_MARKET_PAIRS_PER_LOOP:
            break

    if not matched_pairs:
        existing_pairs = (await db.execute(select(MarketPair))).scalars().all()
        has_changes = _mark_stale_pairs(existing_pairs)
        if not has_changes:
            return
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
        return

    existing_pairs = (await db.execute(select(MarketPair))).scalars().all()
    new_pairs, has_updates = _reconcile_market_pairs(existing_pairs, matched_pairs)
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
    active_statuses = {"candidate", "manual_review", "auto_approved", "approved"}

    for pair in pairs:
        if pair.status in active_statuses and pair.status != "stale":
            pair.status = "stale"
            changed = True

    return changed


async def _process_candidates(db, orderbook_service, calculator, alert_manager):
    pair_stmt = select(MarketPair).where(
        MarketPair.status.in_(["auto_approved", "approved"])
    )
    pairs = (await db.execute(pair_stmt)).scalars().all()
    if not pairs:
        return

    preferences = await get_global_preferences(db)
    market_map = await _load_market_map_for_pairs(db, pairs)
    orderbooks_data = await orderbook_service.fetch_orderbooks_for_pairs(pairs, db)
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
                    preferences=preferences,
                )
            except Exception as e:
                print(
                    f"failed to process opportunity for pair {pair.id}: "
                    f"{format_error_details(e)}"
                )
                await send_system_error_notification("worker", f"process opportunity pair {pair.id}", e)


def _extract_asks(orderbook_data):
    if not isinstance(orderbook_data, dict):
        return []

    asks = (
        orderbook_data.get("asks")
        or orderbook_data.get("sell")
        or orderbook_data.get("sell_orders")
        or []
    )

    if not asks and isinstance(orderbook_data.get("orderbook"), dict):
        asks = (
            orderbook_data["orderbook"].get("asks")
            or orderbook_data["orderbook"].get("sell")
            or orderbook_data["orderbook"].get("sell_orders")
            or []
        )

    levels = []
    if isinstance(asks, dict):
        asks = [(price, size) for price, size in asks.items()]

    for level in asks:
        parsed = _extract_level(level)
        if parsed:
            levels.append(parsed)

    return sorted(levels, key=lambda item: item[0])


def _extract_level(level):
    if isinstance(level, dict):
        price = level.get("price", None)
        if price is None:
            price = level.get("p", None)
        if price is None:
            price = level.get("rate", None)

        size = level.get("size", None)
        if size is None:
            size = level.get("s", None)
        if size is None:
            size = level.get("quantity", None)
        if size is None:
            size = level.get("qty", None)

        if price is None or size is None:
            return None

        try:
            return float(price), float(size)
        except (TypeError, ValueError):
            return None

    if isinstance(level, (list, tuple)) and len(level) >= 2:
        try:
            return float(level[0]), float(level[1])
        except (TypeError, ValueError):
            return None

    return None


def _candidate_markets_for_poly(poly_market, matcher, pf_index, fallback_markets):
    signature = matcher.build_market_signature(poly_market)
    candidates_by_id = {}
    shared_token_count = defaultdict(int)

    for token in signature["tokens"]:
        for pf_signature in pf_index.get(token, []):
            market = pf_signature["market"]
            candidates_by_id[market.id] = market
            shared_token_count[market.id] += 1

    if not candidates_by_id:
        return fallback_markets

    ranked_candidates = sorted(
        candidates_by_id.values(),
        key=lambda market: (
            shared_token_count[market.id],
            len(getattr(market, "title", "")),
        ),
        reverse=True,
    )
    return ranked_candidates


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
