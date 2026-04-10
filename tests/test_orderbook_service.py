import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from arbitrage_bot.core.observability import reset_counters
from arbitrage_bot.core.observability import snapshot_counters
from arbitrage_bot.services import orderbook as orderbook_module
from arbitrage_bot.services.orderbook import OrderbookService


class FakeResult:
    def __init__(self, rows):
        self._rows = rows


    def all(self):
        return self._rows


class FakeDbSession:
    def __init__(self, rows):
        self.rows = rows


    async def execute(self, stmt):
        return FakeResult(self.rows)


class OrderbookServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        orderbook_module._predict_fun_orderbook_cache.clear()
        orderbook_module._polymarket_book_cache.clear()
        reset_counters()


    async def test_fetches_orderbooks_when_pair_market_sides_are_reversed(self):
        service = OrderbookService()
        service.predict_fun.fetch_orderbook = AsyncMock(
            return_value={"data": {"asks": [[0.5, 2]], "bids": [[0.4, 3]]}}
        )
        service.polymarket.fetch_books = AsyncMock(
            return_value=[
                {"asset_id": "poly-yes", "asks": [{"price": "0.4", "size": "2"}]},
                {"asset_id": "poly-no", "asks": [{"price": "0.6", "size": "3"}]},
            ]
        )
        service.polymarket.close = AsyncMock()
        service.predict_fun.close = AsyncMock()

        pair = SimpleNamespace(
            id=9,
            market_id_a=200,
            market_id_b=100,
            outcome_mapping_json={
                "market_a": {"yes": "poly-yes", "no": "poly-no"},
                "market_b": {"yes": "pf-yes", "no": "pf-no"},
            },
        )
        db = FakeDbSession(
            [
                (100, "polymarket", "poly-100"),
                (200, "predict_fun", "pf-200"),
            ]
        )
        result = await service.fetch_orderbooks_for_pairs([pair], db)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["poly_market_id"], "poly-100")
        self.assertEqual(result[0]["pf_market_id"], "pf-200")
        service.predict_fun.fetch_orderbook.assert_awaited_once_with("pf-200")
        service.polymarket.fetch_books.assert_awaited_once_with(["poly-no", "poly-yes"])


    async def test_builds_directional_books_from_mapping(self):
        service = OrderbookService()
        pair = SimpleNamespace(
            outcome_mapping_json={
                "market_a": {"yes": "poly-yes", "no": "poly-no"},
                "market_b": {"yes": "pf-yes", "no": "pf-no"},
            }
        )
        pf_payload = {
            "data": {
                "asks": [[0.2, 5], [0.3, 4]],
                "bids": [[0.7, 6], [0.6, 7]],
            }
        }
        polymarket_books = {
            "poly-yes": {"asset_id": "poly-yes", "asks": [{"price": "0.4", "size": "2"}]},
            "poly-no": {"asset_id": "poly-no", "asks": [{"price": "0.6", "size": "3"}]},
        }

        directions, drop_reason = service._build_direction_books(pair, pf_payload, polymarket_books)

        self.assertEqual(
            directions,
            {
                "A_yes_B_no": {
                    "poly": [(0.4, 2.0)],
                    "pf": [(0.30000000000000004, 6.0), (0.4, 7.0)],
                },
                "A_no_B_yes": {
                    "poly": [(0.6, 3.0)],
                    "pf": [(0.2, 5.0), (0.3, 4.0)],
                },
            },
        )
        self.assertIsNone(drop_reason)


    async def test_records_specific_drop_reason_for_missing_polymarket_no_asks(self):
        service = OrderbookService()
        service.predict_fun.fetch_orderbook = AsyncMock(
            return_value={"data": {"asks": [[0.2, 5]], "bids": [[0.7, 6]]}}
        )
        service.polymarket.fetch_books = AsyncMock(
            return_value=[
                {"asset_id": "poly-yes", "asks": [{"price": "0.4", "size": "2"}]},
                {"asset_id": "poly-no", "asks": []},
            ]
        )

        pair = SimpleNamespace(
            id=9,
            market_id_a=200,
            market_id_b=100,
            outcome_mapping_json={
                "market_a": {"yes": "poly-yes", "no": "poly-no"},
                "market_b": {"yes": "pf-yes", "no": "pf-no"},
            },
        )
        db = FakeDbSession(
            [
                (100, "polymarket", "poly-100"),
                (200, "predict_fun", "pf-200"),
            ]
        )

        result = await service.fetch_orderbooks_for_pairs([pair], db)
        counters = snapshot_counters()

        self.assertEqual(result, [])
        self.assertEqual(counters["orderbook.directional_books_unavailable"], 1)
        self.assertEqual(counters["orderbook.drop.polymarket_no_asks_missing"], 1)


    async def test_missing_predict_fun_orderbook_is_treated_as_absent_market(self):
        service = OrderbookService()
        request = httpx.Request("GET", "https://api.predict.fun/v1/markets/9212/orderbook")
        response = httpx.Response(404, request=request)
        service.predict_fun.fetch_orderbook = AsyncMock(
            side_effect=httpx.HTTPStatusError("not found", request=request, response=response)
        )
        service.polymarket.close = AsyncMock()
        service.predict_fun.close = AsyncMock()

        pair = SimpleNamespace(
            id=9,
            market_id_a=200,
            market_id_b=100,
            outcome_mapping_json={
                "market_a": {"yes": "poly-yes", "no": "poly-no"},
                "market_b": {"yes": "pf-yes", "no": "pf-no"},
            },
        )
        db = FakeDbSession(
            [
                (100, "polymarket", "poly-100"),
                (200, "predict_fun", "9212"),
            ]
        )

        with patch("arbitrage_bot.services.orderbook.log.debug") as debug_mock:
            result = await service.fetch_orderbooks_for_pairs([pair], db)

        self.assertEqual(result, [])
        debug_mock.assert_called_once()
        counters = snapshot_counters()
        self.assertEqual(counters["orderbook.fetch.predict_fun_market_not_found"], 1)
        self.assertEqual(counters["orderbook.drop.predict_fun_market_not_found"], 1)


    async def test_predict_fun_fetch_failure_counts_pair_drop_reason(self):
        service = OrderbookService()
        service.predict_fun.fetch_orderbook = AsyncMock(side_effect=RuntimeError("timeout"))

        pair = SimpleNamespace(
            id=9,
            market_id_a=200,
            market_id_b=100,
            outcome_mapping_json={
                "market_a": {"yes": "poly-yes", "no": "poly-no"},
                "market_b": {"yes": "pf-yes", "no": "pf-no"},
            },
        )
        db = FakeDbSession(
            [
                (100, "polymarket", "poly-100"),
                (200, "predict_fun", "9212"),
            ]
        )

        result = await service.fetch_orderbooks_for_pairs([pair], db)
        counters = snapshot_counters()

        self.assertEqual(result, [])
        self.assertEqual(counters["orderbook.fetch.predict_fun_fetch_failed"], 1)
        self.assertEqual(counters["orderbook.drop.predict_fun_fetch_failed"], 1)


    async def test_batches_polymarket_books_across_pairs(self):
        service = OrderbookService()
        service.predict_fun.fetch_orderbook = AsyncMock(
            side_effect=[
                {"data": {"asks": [[0.2, 5]], "bids": [[0.7, 6]]}},
                {"data": {"asks": [[0.3, 4]], "bids": [[0.6, 7]]}},
            ]
        )
        service.polymarket.fetch_books = AsyncMock(
            return_value=[
                {"asset_id": "poly-yes-1", "asks": [{"price": "0.4", "size": "2"}]},
                {"asset_id": "poly-no-1", "asks": [{"price": "0.6", "size": "3"}]},
                {"asset_id": "poly-yes-2", "asks": [{"price": "0.41", "size": "4"}]},
                {"asset_id": "poly-no-2", "asks": [{"price": "0.59", "size": "5"}]},
            ]
        )

        pairs = [
            SimpleNamespace(
                id=9,
                market_id_a=200,
                market_id_b=100,
                outcome_mapping_json={
                    "market_a": {"yes": "poly-yes-1", "no": "poly-no-1"},
                    "market_b": {"yes": "pf-yes-1", "no": "pf-no-1"},
                },
            ),
            SimpleNamespace(
                id=10,
                market_id_a=201,
                market_id_b=101,
                outcome_mapping_json={
                    "market_a": {"yes": "poly-yes-2", "no": "poly-no-2"},
                    "market_b": {"yes": "pf-yes-2", "no": "pf-no-2"},
                },
            ),
        ]
        db = FakeDbSession(
            [
                (100, "polymarket", "poly-100"),
                (101, "polymarket", "poly-101"),
                (200, "predict_fun", "pf-200"),
                (201, "predict_fun", "pf-201"),
            ]
        )

        result = await service.fetch_orderbooks_for_pairs(pairs, db)

        self.assertEqual(len(result), 2)
        service.polymarket.fetch_books.assert_awaited_once_with(
            ["poly-no-1", "poly-no-2", "poly-yes-1", "poly-yes-2"]
        )


    async def test_reuses_short_ttl_cache_for_orderbooks(self):
        service = OrderbookService()
        service.predict_fun.fetch_orderbook = AsyncMock(
            return_value={"data": {"asks": [[0.2, 5]], "bids": [[0.7, 6]]}}
        )
        service.polymarket.fetch_books = AsyncMock(
            return_value=[
                {"asset_id": "poly-yes", "asks": [{"price": "0.4", "size": "2"}]},
                {"asset_id": "poly-no", "asks": [{"price": "0.6", "size": "3"}]},
            ]
        )
        pair = SimpleNamespace(
            id=9,
            market_id_a=200,
            market_id_b=100,
            outcome_mapping_json={
                "market_a": {"yes": "poly-yes", "no": "poly-no"},
                "market_b": {"yes": "pf-yes", "no": "pf-no"},
            },
        )
        db = FakeDbSession(
            [
                (100, "polymarket", "poly-100"),
                (200, "predict_fun", "pf-200"),
            ]
        )

        first = await service.fetch_orderbooks_for_pairs([pair], db)
        second = await service.fetch_orderbooks_for_pairs([pair], db)

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        service.predict_fun.fetch_orderbook.assert_awaited_once_with("pf-200")
        service.polymarket.fetch_books.assert_awaited_once_with(["poly-no", "poly-yes"])