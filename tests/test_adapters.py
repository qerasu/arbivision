import unittest
from unittest.mock import AsyncMock

from arbitrage_bot.adapters.polymarket import PolymarketAdapter
from arbitrage_bot.adapters.predict_fun import PredictFunAdapter


class PolymarketAdapterTests(unittest.IsolatedAsyncioTestCase):


    async def test_fetch_markets_collects_all_pages(self):
        adapter = PolymarketAdapter()
        adapter._get_json = AsyncMock(
            side_effect=[
                [{"id": "1"}, {"id": "2"}],
                [{"id": "3"}],
            ]
        )
        adapter.page_limit = 2
        adapter.max_pages = 5
        adapter.close = AsyncMock()

        result = await adapter.fetch_markets()

        self.assertEqual(result, [{"id": "1"}, {"id": "2"}, {"id": "3"}])
        self.assertEqual(adapter._get_json.await_count, 2)


    async def test_fetch_markets_stops_if_same_page_repeats(self):
        adapter = PolymarketAdapter()
        adapter._get_json = AsyncMock(
            side_effect=[
                [{"id": "1"}, {"id": "2"}],
                [{"id": "1"}, {"id": "2"}],
            ]
        )
        adapter.page_limit = 2
        adapter.max_pages = 5
        adapter.close = AsyncMock()

        result = await adapter.fetch_markets()

        self.assertEqual(result, [{"id": "1"}, {"id": "2"}])
        self.assertEqual(adapter._get_json.await_count, 2)


class PredictFunAdapterTests(unittest.IsolatedAsyncioTestCase):


    async def test_fetch_markets_collects_all_pages(self):
        adapter = PredictFunAdapter()
        adapter.recent_start_id = None
        adapter._get_json = AsyncMock(
            side_effect=[
                {
                    "data": [
                        {"id": "10", "status": "REGISTERED", "tradingStatus": "OPEN", "isVisible": True},
                        {"id": "20", "status": "RESOLVED", "tradingStatus": "CLOSED", "isVisible": True},
                    ],
                    "cursor": "next-page",
                },
                {
                    "data": [
                        {"id": "30", "status": "REGISTERED", "tradingStatus": "OPEN", "isVisible": True},
                    ],
                    "cursor": None,
                },
            ]
        )
        adapter.page_limit = 2
        adapter.max_pages = 5
        adapter.close = AsyncMock()

        result = await adapter.fetch_markets()

        self.assertEqual(
            result,
            [
                {"id": "10", "status": "REGISTERED", "tradingStatus": "OPEN", "isVisible": True},
                {"id": "30", "status": "REGISTERED", "tradingStatus": "OPEN", "isVisible": True},
            ],
        )
        self.assertEqual(adapter._get_json.await_count, 2)

        first_call = adapter._get_json.await_args_list[0]
        self.assertEqual(first_call.args[0], "/markets")
        self.assertEqual(first_call.kwargs["params"]["first"], 2)
        self.assertNotIn("status", first_call.kwargs["params"])

        second_call = adapter._get_json.await_args_list[1]
        self.assertEqual(second_call.kwargs["params"]["after"], "next-page")


    async def test_fetch_markets_starts_from_recent_cursor_by_default(self):
        adapter = PredictFunAdapter()
        adapter._get_json = AsyncMock(
            return_value={"data": [], "cursor": None}
        )
        adapter.page_limit = 2
        adapter.max_pages = 1
        adapter.close = AsyncMock()

        await adapter.fetch_markets()

        first_call = adapter._get_json.await_args_list[0]
        self.assertEqual(first_call.kwargs["params"]["after"], adapter._encode_cursor(adapter.recent_start_id))