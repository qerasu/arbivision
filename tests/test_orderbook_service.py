import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

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


class FakeRedis:


    def __init__(self):
        self.setex_calls = []


    async def setex(self, key, ttl, value):
        self.setex_calls.append((key, ttl, value))


class OrderbookServiceTests(unittest.IsolatedAsyncioTestCase):


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
        redis = FakeRedis()

        with patch("arbitrage_bot.services.orderbook.get_redis", return_value=redis):
            result = await service.fetch_orderbooks_for_pairs([pair], db)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["poly_market_id"], "poly-100")
        self.assertEqual(result[0]["pf_market_id"], "pf-200")
        service.predict_fun.fetch_orderbook.assert_awaited_once_with("pf-200")
        service.polymarket.fetch_books.assert_awaited_once_with(["poly-yes", "poly-no"])
        self.assertEqual(len(redis.setex_calls), 1)


    async def test_builds_directional_books_from_mapping(self):
        service = OrderbookService()
        service.polymarket.fetch_books = AsyncMock(
            return_value=[
                {"asset_id": "poly-yes", "asks": [{"price": "0.4", "size": "2"}]},
                {"asset_id": "poly-no", "asks": [{"price": "0.6", "size": "3"}]},
            ]
        )

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

        directions = await service._build_direction_books(pair, pf_payload)

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