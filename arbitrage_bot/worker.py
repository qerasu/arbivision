import asyncio
from arbitrage_bot.core.database import AsyncSessionLocal
from arbitrage_bot.models.orm import Market, MarketPair
from arbitrage_bot.services.ingestion import IngestionService
from arbitrage_bot.services.matcher import MatcherService
from arbitrage_bot.services.orderbook import OrderbookService
from arbitrage_bot.services.calculator import ArbitrageCalculator
from arbitrage_bot.services.alert_manager import AlertManager
from arbitrage_bot.core.config import settings
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
            print(f"error in sync loop: {e}")
        
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

    matched_pairs = []
    for poly_market in poly_markets:
        for pf_market in pf_markets:
            pair = matcher.match_candidates(poly_market, pf_market)
            if pair:
                matched_pairs.append(pair)
            if settings.MAX_MARKET_PAIRS_PER_LOOP and len(matched_pairs) >= settings.MAX_MARKET_PAIRS_PER_LOOP:
                break
        if settings.MAX_MARKET_PAIRS_PER_LOOP and len(matched_pairs) >= settings.MAX_MARKET_PAIRS_PER_LOOP:
            break

    if not matched_pairs:
        return

    pair_hashes = [pair.pair_hash for pair in matched_pairs]
    existing_rows = await db.execute(select(MarketPair.pair_hash).where(MarketPair.pair_hash.in_(pair_hashes)))
    existing = {row[0] for row in existing_rows.all()}

    new_pairs = [pair for pair in matched_pairs if pair.pair_hash not in existing]
    if not new_pairs:
        return

    db.add_all(new_pairs)
    try:
        await db.flush()
        await db.commit()
    except IntegrityError:
        # protect against concurrent workers creating same pair
        await db.rollback()


async def _process_candidates(db, orderbook_service, calculator, alert_manager):
    pair_stmt = select(MarketPair).where(
        MarketPair.status.in_(["auto_approved", "approved"])
    )
    pairs = (await db.execute(pair_stmt)).scalars().all()
    if not pairs:
        return

    orderbooks_data = await orderbook_service.fetch_orderbooks_for_pairs(pairs, db)
    for item in orderbooks_data:
        pair = item["pair"]
        poly_asks = _extract_asks(item["poly"])
        pf_asks = _extract_asks(item["pf"])

        if not poly_asks or not pf_asks:
            continue

        calc_result = calculator.calculate_opportunity(poly_asks, pf_asks)
        if not calc_result:
            continue

        try:
            await alert_manager.process_opportunity(pair, calc_result)
        except Exception as e:
            print(f"failed to process opportunity for pair {pair.id}: {e}")


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


if __name__ == "__main__":
    asyncio.run(run_sync_loop())
