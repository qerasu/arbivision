import asyncio
from arbitrage_bot.adapters.polymarket import PolymarketAdapter
from arbitrage_bot.adapters.predict_fun import PredictFunAdapter
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.models.orm import Market
from arbitrage_bot.services.system_notifier import format_compact_error
from sqlalchemy.future import select

log = get_logger("orderbook")


class OrderbookService:

    def __init__(self):
        self.polymarket = PolymarketAdapter()
        self.predict_fun = PredictFunAdapter()
        self._pair_fetch_concurrency = 4


    async def close(self):
        await self.polymarket.close()
        await self.predict_fun.close()


    async def fetch_orderbooks_for_pairs(self, market_pairs, db_session):
        market_id_map = {}
        market_ids = []
        for pair in market_pairs:
            market_ids.append(pair.market_id_a)
            market_ids.append(pair.market_id_b)

        if market_ids:
            stmt = select(Market.id, Market.platform, Market.platform_market_id).where(
                Market.id.in_(market_ids)
            )
            result = await db_session.execute(stmt)
            for market_id, platform, platform_market_id in result.all():
                market_id_map[market_id] = {
                    "platform": platform,
                    "platform_market_id": platform_market_id,
                }

        semaphore = asyncio.Semaphore(self._pair_fetch_concurrency)
        tasks = [
            self._fetch_pair_orderbooks(
                pair,
                market_id_map,
                semaphore,
            )
            for pair in market_pairs
        ]
        results = await asyncio.gather(*tasks)
        return [item for item in results if item is not None]


    async def _fetch_pair_orderbooks(self, pair, market_id_map, semaphore):
        async with semaphore:
            poly_platform_id, pf_platform_id = self._resolve_platform_market_ids(pair, market_id_map)

            if not poly_platform_id or not pf_platform_id:
                log.warning(
                    "missing platform market id",
                    pair_id=pair.id,
                    poly_id=poly_platform_id or "missing",
                    pf_id=pf_platform_id or "missing",
                )
                return None

            try:
                pf_ob = await self.predict_fun.fetch_orderbook(pf_platform_id)
            except Exception as exc:
                log.warning(
                    "orderbook fetch failed",
                    pair_id=pair.id,
                    source="predict.fun",
                    error=format_compact_error(exc),
                )
                return None

            directions = await self._build_direction_books(pair, pf_ob)
            if not directions:
                log.warning(
                    "directional books unavailable",
                    pair_id=pair.id,
                )
                return None

            return {
                "pair": pair,
                "poly": None,
                "pf": pf_ob,
                "poly_market_id": poly_platform_id,
                "pf_market_id": pf_platform_id,
                "directions": directions,
            }


    def _resolve_platform_market_ids(self, pair, market_id_map):
        pair_markets = [
            market_id_map.get(pair.market_id_a),
            market_id_map.get(pair.market_id_b),
        ]
        platform_ids = {}

        for market_info in pair_markets:
            if not market_info:
                continue
            platform = market_info["platform"]
            if platform not in platform_ids:
                platform_ids[platform] = market_info["platform_market_id"]

        return platform_ids.get("polymarket"), platform_ids.get("predict_fun")


    async def _build_direction_books(self, pair, pf_orderbook_payload):
        mapping = getattr(pair, "outcome_mapping_json", None) or {}
        market_a = mapping.get("market_a") or {}

        poly_yes_id = market_a.get("yes")
        poly_no_id = market_a.get("no")
        pf_yes_asks, pf_no_asks = self._extract_predict_fun_directional_asks(pf_orderbook_payload)
        if not poly_yes_id or not poly_no_id or not pf_yes_asks or not pf_no_asks:
            return None

        poly_books = await self.polymarket.fetch_books([poly_yes_id, poly_no_id])
        poly_book_map = {
            str(item.get("asset_id")): item
            for item in poly_books
            if isinstance(item, dict) and item.get("asset_id") is not None
        }
        poly_yes_asks = self._extract_asks(poly_book_map.get(str(poly_yes_id)))
        poly_no_asks = self._extract_asks(poly_book_map.get(str(poly_no_id)))
        if not poly_yes_asks or not poly_no_asks:
            return None

        return {
            "A_yes_B_no": {
                "poly": poly_yes_asks,
                "pf": pf_no_asks,
            },
            "A_no_B_yes": {
                "poly": poly_no_asks,
                "pf": pf_yes_asks,
            },
        }


    def _extract_predict_fun_directional_asks(self, orderbook_payload):
        payload = orderbook_payload.get("data", orderbook_payload) if isinstance(orderbook_payload, dict) else {}
        yes_asks = self._extract_asks(payload)
        yes_bids = self._extract_bids(payload)
        no_asks = sorted(
            [(max(0.0, 1.0 - price), size) for price, size in yes_bids if 0.0 <= price <= 1.0 and size > 0],
            key=lambda item: item[0],
        )
        return yes_asks, no_asks


    def _extract_bids(self, orderbook_data):
        if not isinstance(orderbook_data, dict):
            return []

        bids = orderbook_data.get("bids") or orderbook_data.get("buy") or orderbook_data.get("buy_orders") or []
        levels = []
        if isinstance(bids, dict):
            bids = [(price, size) for price, size in bids.items()]

        for level in bids:
            parsed = self._extract_level(level)
            if parsed:
                levels.append(parsed)

        return sorted(levels, key=lambda item: item[0], reverse=True)


    def _extract_asks(self, orderbook_data):
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
            parsed = self._extract_level(level)
            if parsed:
                levels.append(parsed)

        return sorted(levels, key=lambda item: item[0])


    def _first_not_none(self, *values):
        return next((v for v in values if v is not None), None)


    def _extract_level(self, level):
        if isinstance(level, dict):
            price = self._first_not_none(
                level.get("price"),
                level.get("p"),
                level.get("rate"),
            )
            size = self._first_not_none(
                level.get("size"),
                level.get("s"),
                level.get("quantity"),
                level.get("qty"),
            )

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