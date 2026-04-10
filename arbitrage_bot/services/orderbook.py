import asyncio
import time

import httpx
from arbitrage_bot.adapters.polymarket import PolymarketAdapter
from arbitrage_bot.adapters.predict_fun import PredictFunAdapter
from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.core.observability import incr_counter
from arbitrage_bot.models.orm import Market
from arbitrage_bot.services.system_notifier import format_compact_error
from sqlalchemy.future import select

log = get_logger("orderbook")
_CACHE_MISS = object()
_predict_fun_orderbook_cache = {}
_polymarket_book_cache = {}


class OrderbookService:
    def __init__(self):
        self.polymarket = PolymarketAdapter()
        self.predict_fun = PredictFunAdapter()
        self._pair_fetch_concurrency = settings.ORDERBOOK_PREDICT_FUN_CONCURRENCY
        self._cache_ttl_seconds = settings.ORDERBOOK_CACHE_TTL_SECONDS
        self._cache_max_items = settings.ORDERBOOK_CACHE_MAX_ITEMS
        self._polymarket_batch_size = settings.ORDERBOOK_POLYMARKET_BATCH_SIZE


    async def close(self):
        await self.polymarket.close()
        await self.predict_fun.close()


    async def fetch_orderbooks_for_pairs(self, market_pairs, db_session, market_map=None):
        prepared_pairs = []
        for pair in market_pairs:
            poly_platform_id, pf_platform_id = await self._resolve_pair_platform_market_ids(
                pair,
                db_session,
                market_map=market_map,
            )
            if not poly_platform_id or not pf_platform_id:
                self._record_missing_platform_market_id(
                    pair,
                    poly_platform_id,
                    pf_platform_id,
                )
                continue

            prepared_pairs.append(
                {
                    "pair": pair,
                    "poly_market_id": poly_platform_id,
                    "pf_market_id": pf_platform_id,
                }
            )

        if not prepared_pairs:
            return []

        pf_orderbooks, pf_drop_reasons = await self._fetch_predict_fun_orderbooks(prepared_pairs)
        retained_pairs = []
        for item in prepared_pairs:
            pf_market_id = item["pf_market_id"]
            if pf_orderbooks.get(pf_market_id) is not None:
                retained_pairs.append(item)
                continue
            drop_reason = pf_drop_reasons.get(pf_market_id)
            if drop_reason:
                incr_counter(f"orderbook.drop.{drop_reason}")
        prepared_pairs = retained_pairs
        if not prepared_pairs:
            return []

        polymarket_books = await self._fetch_polymarket_books(prepared_pairs)
        results = []

        for item in prepared_pairs:
            pair = item["pair"]
            pf_market_id = item["pf_market_id"]
            pf_ob = pf_orderbooks.get(pf_market_id)
            if pf_ob is None:
                continue

            directions, drop_reason = self._build_direction_books(
                pair,
                pf_ob,
                polymarket_books,
            )
            if not directions:
                log.debug(
                    "directional books unavailable",
                    pair_id=pair.id,
                )
                incr_counter("orderbook.directional_books_unavailable")
                if drop_reason:
                    incr_counter(f"orderbook.drop.{drop_reason}")
                continue

            results.append(
                {
                    "pair": pair,
                    "poly": None,
                    "pf": pf_ob,
                    "poly_market_id": item["poly_market_id"],
                    "pf_market_id": pf_market_id,
                    "directions": directions,
                }
            )

        return results


    async def diagnose_pair(self, pair, db_session):
        market_id_map = await self._load_market_id_map(
            [pair.market_id_a, pair.market_id_b],
            db_session,
        )

        poly_platform_id, pf_platform_id = self._resolve_platform_market_ids(pair, market_id_map)
        if not poly_platform_id or not pf_platform_id:
            return {
                "stage": "orderbook",
                "reason": "missing_platform_market_id",
                "poly_market_id": poly_platform_id,
                "pf_market_id": pf_platform_id,
            }

        pf_orderbook_payload, drop_reason = await self._fetch_predict_fun_orderbook_with_reason(
            pf_platform_id,
        )
        if drop_reason:
            return {
                "stage": "orderbook",
                "reason": drop_reason,
                "pf_market_id": pf_platform_id,
            }

        poly_yes_id, poly_no_id = self._polymarket_token_ids_for_pair(pair)
        if not poly_yes_id or not poly_no_id:
            return {
                "stage": "orderbook",
                "reason": "missing_outcome_mapping",
            }

        polymarket_books = await self._fetch_polymarket_books(
            [
                {
                    "pair": pair,
                    "poly_market_id": poly_platform_id,
                    "pf_market_id": pf_platform_id,
                }
            ]
        )
        directions, direction_drop_reason = self._build_direction_books(
            pair,
            pf_orderbook_payload,
            polymarket_books,
        )
        if not directions:
            payload = {
                "stage": "orderbook",
                "reason": direction_drop_reason,
            }
            if direction_drop_reason == "polymarket_yes_asks_missing":
                payload["token_id"] = str(poly_yes_id)
            elif direction_drop_reason == "polymarket_no_asks_missing":
                payload["token_id"] = str(poly_no_id)
            return payload

        return {
            "stage": "ready",
            "reason": None,
            "directions": directions,
        }


    async def _load_market_id_map(self, market_ids, db_session):
        if not market_ids:
            return {}

        stmt = select(Market.id, Market.platform, Market.platform_market_id).where(
            Market.id.in_(market_ids)
        )
        result = await db_session.execute(stmt)
        return {
            market_id: {
                "platform": platform,
                "platform_market_id": platform_market_id,
            }
            for market_id, platform, platform_market_id in result.all()
        }


    async def _resolve_pair_platform_market_ids(self, pair, db_session, market_map=None):
        if market_map is not None:
            market_a = market_map.get(pair.market_id_a)
            market_b = market_map.get(pair.market_id_b)
            return self._resolve_platform_market_ids_from_rows(market_a, market_b)

        market_id_map = await self._load_market_id_map(
            [pair.market_id_a, pair.market_id_b],
            db_session,
        )
        return self._resolve_platform_market_ids(pair, market_id_map)


    def _polymarket_token_ids_for_pair(self, pair):
        mapping = getattr(pair, "outcome_mapping_json", None) or {}
        market_a = mapping.get("market_a") or {}
        return market_a.get("yes"), market_a.get("no")


    async def _fetch_predict_fun_orderbook_with_reason(self, market_id, semaphore=None):
        cached_value = self._get_cache_value(_predict_fun_orderbook_cache, market_id)
        if cached_value is _CACHE_MISS:
            return None, "predict_fun_market_not_found"
        if cached_value is not None:
            return cached_value, None

        if semaphore is None:
            return await self._fetch_predict_fun_orderbook_uncached(market_id)

        async with semaphore:
            cached_value = self._get_cache_value(_predict_fun_orderbook_cache, market_id)
            if cached_value is _CACHE_MISS:
                return None, "predict_fun_market_not_found"
            if cached_value is not None:
                return cached_value, None
            return await self._fetch_predict_fun_orderbook_uncached(market_id)


    async def _fetch_predict_fun_orderbook_uncached(self, market_id):
        try:
            payload = await self.predict_fun.fetch_orderbook(market_id)
        except Exception as exc:
            if self._is_missing_predict_fun_market(exc):
                log.debug(
                    "predict.fun market orderbook not found",
                    source="predict.fun",
                    market_id=market_id,
                )
                incr_counter("orderbook.fetch.predict_fun_market_not_found")
                self._set_cache_value(_predict_fun_orderbook_cache, market_id, _CACHE_MISS)
                return None, "predict_fun_market_not_found"
            log.warning(
                "orderbook fetch failed",
                source="predict.fun",
                market_id=market_id,
                error=format_compact_error(exc),
            )
            incr_counter("orderbook.fetch.predict_fun_fetch_failed")
            return None, "predict_fun_fetch_failed"

        self._set_cache_value(_predict_fun_orderbook_cache, market_id, payload)
        return payload, None
    async def _fetch_predict_fun_orderbooks(self, prepared_pairs):
        unique_market_ids = {
            item["pf_market_id"]
            for item in prepared_pairs
            if item.get("pf_market_id")
        }
        if not unique_market_ids:
            return {}, {}

        semaphore = asyncio.Semaphore(self._pair_fetch_concurrency)
        tasks = [
            self._fetch_single_predict_fun_orderbook(market_id, semaphore)
            for market_id in sorted(unique_market_ids)
        ]
        results = await asyncio.gather(*tasks)
        orderbooks = {}
        drop_reasons = {}

        for market_id, payload, drop_reason in results:
            if payload is not None:
                orderbooks[market_id] = payload
            elif drop_reason:
                drop_reasons[market_id] = drop_reason

        return orderbooks, drop_reasons


    async def _fetch_single_predict_fun_orderbook(self, market_id, semaphore):
        payload, drop_reason = await self._fetch_predict_fun_orderbook_with_reason(
            market_id,
            semaphore=semaphore,
        )
        return market_id, payload, drop_reason


    async def _fetch_polymarket_books(self, prepared_pairs):
        token_ids = set()
        for item in prepared_pairs:
            pair_mapping = getattr(item["pair"], "outcome_mapping_json", None) or {}
            market_a = pair_mapping.get("market_a") or {}
            poly_yes_id = market_a.get("yes")
            poly_no_id = market_a.get("no")
            if poly_yes_id:
                token_ids.add(str(poly_yes_id))
            if poly_no_id:
                token_ids.add(str(poly_no_id))

        if not token_ids:
            return {}

        missing_token_ids = []
        books = {}
        for token_id in sorted(token_ids):
            cached_value = self._get_cache_value(_polymarket_book_cache, token_id)
            if cached_value is _CACHE_MISS:
                continue
            if cached_value is None:
                missing_token_ids.append(token_id)
                continue
            books[token_id] = cached_value

        for chunk in self._chunked(missing_token_ids, self._polymarket_batch_size):
            fetched_books = await self.polymarket.fetch_books(chunk)
            fetched_by_token_id = {
                str(item.get("asset_id")): item
                for item in fetched_books
                if isinstance(item, dict) and item.get("asset_id") is not None
            }

            for token_id in chunk:
                book = fetched_by_token_id.get(token_id)
                if book is None:
                    incr_counter("orderbook.polymarket_book_missing")
                    self._set_cache_value(_polymarket_book_cache, token_id, _CACHE_MISS)
                    continue
                self._set_cache_value(_polymarket_book_cache, token_id, book)
                books[token_id] = book

        return books


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


    def _resolve_platform_market_ids_from_rows(self, market_a, market_b):
        platform_ids = {}

        for market in (market_a, market_b):
            if market is None:
                continue
            platform = getattr(market, "platform", None)
            platform_market_id = getattr(market, "platform_market_id", None)
            if platform and platform_market_id and platform not in platform_ids:
                platform_ids[platform] = platform_market_id

        return platform_ids.get("polymarket"), platform_ids.get("predict_fun")


    def _record_missing_platform_market_id(self, pair, poly_platform_id, pf_platform_id):
        log.warning(
            "missing platform market id",
            pair_id=pair.id,
            poly_id=poly_platform_id or "missing",
            pf_id=pf_platform_id or "missing",
        )
        incr_counter("orderbook.missing_platform_market_id")
        incr_counter("orderbook.drop.missing_platform_market_id")


    def _is_missing_predict_fun_market(self, exc):
        if isinstance(exc, httpx.HTTPStatusError):
            response = getattr(exc, "response", None)
            return getattr(response, "status_code", None) == 404

        return "404" in str(exc)


    def _build_direction_books(self, pair, pf_orderbook_payload, polymarket_books):
        mapping = getattr(pair, "outcome_mapping_json", None) or {}
        market_a = mapping.get("market_a") or {}

        poly_yes_id = market_a.get("yes")
        poly_no_id = market_a.get("no")
        pf_yes_asks, pf_no_asks = self._extract_predict_fun_directional_asks(pf_orderbook_payload)

        if not poly_yes_id or not poly_no_id:
            return None, "missing_outcome_mapping"
        if not pf_yes_asks:
            return None, "predict_fun_yes_asks_missing"
        if not pf_no_asks:
            return None, "predict_fun_no_asks_missing"

        poly_yes_asks = self._extract_asks(polymarket_books.get(str(poly_yes_id)))
        poly_no_asks = self._extract_asks(polymarket_books.get(str(poly_no_id)))

        if not poly_yes_asks:
            return None, "polymarket_yes_asks_missing"
        if not poly_no_asks:
            return None, "polymarket_no_asks_missing"

        return {
            "A_yes_B_no": {
                "poly": poly_yes_asks,
                "pf": pf_no_asks,
            },
            "A_no_B_yes": {
                "poly": poly_no_asks,
                "pf": pf_yes_asks,
            },
        }, None


    def _extract_predict_fun_directional_asks(self, orderbook_payload):
        payload = orderbook_payload.get("data", orderbook_payload) if isinstance(orderbook_payload, dict) else {}
        yes_asks = self._extract_asks(payload)
        yes_bids = self._extract_bids(payload)
        no_asks = sorted(
            [(max(0.0, 1.0 - price), size) for price, size in yes_bids if 0.0 <= price <= 1.0 and size > 0],
            key=lambda item: item[0],
        )
        return yes_asks, no_asks


    def _chunked(self, values, chunk_size):
        for index in range(0, len(values), chunk_size):
            yield values[index:index + chunk_size]


    def _get_cache_value(self, cache, key):
        entry = cache.get(key)
        if not entry:
            return None

        value, expires_at = entry
        if expires_at <= time.monotonic():
            cache.pop(key, None)
            return None

        return value


    def _set_cache_value(self, cache, key, value):
        cache[key] = (
            value,
            time.monotonic() + self._cache_ttl_seconds,
        )
        self._trim_cache(cache)


    def _trim_cache(self, cache):
        if len(cache) <= self._cache_max_items:
            return

        expired_keys = [
            key
            for key, (_, expires_at) in cache.items()
            if expires_at <= time.monotonic()
        ]
        for key in expired_keys:
            cache.pop(key, None)

        if len(cache) <= self._cache_max_items:
            return

        keys_by_expiry = sorted(cache, key=lambda item_key: cache[item_key][1])
        for key in keys_by_expiry[: max(1, len(keys_by_expiry) // 4)]:
            cache.pop(key, None)


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