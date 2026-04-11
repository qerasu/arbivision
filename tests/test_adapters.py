import unittest
from unittest.mock import AsyncMock

import httpx
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


    async def test_fetch_markets_stops_after_failed_page_and_marks_partial(self):
        adapter = PolymarketAdapter()
        adapter._get_json = AsyncMock(
            side_effect=[
                [{"id": "1"}, {"id": "2"}],
                RuntimeError("timeout"),
            ]
        )
        adapter.page_limit = 2
        adapter.max_pages = 5
        adapter.close = AsyncMock()

        with self.assertLogs("arbitrage_bot.adapters.polymarket", level="WARNING") as log_context:
            result = await adapter.fetch_markets()

        self.assertEqual(result, [{"id": "1"}, {"id": "2"}])
        self.assertTrue(adapter.last_fetch_partial)
        self.assertEqual(adapter._get_json.await_count, 2)
        self.assertEqual(len(log_context.output), 1)
        self.assertIn("polymarket page fetch failed (offset=2), stopping pagination: timeout", log_context.output[0])


    async def test_fetch_markets_marks_incomplete_when_page_budget_is_hit(self):
        adapter = PolymarketAdapter()
        adapter._get_json = AsyncMock(
            side_effect=[
                [{"id": "1"}, {"id": "2"}],
                [{"id": "3"}, {"id": "4"}],
            ]
        )
        adapter.page_limit = 2

        result = await adapter.fetch_markets(max_pages=1)

        self.assertEqual(result, [{"id": "1"}, {"id": "2"}])
        self.assertFalse(adapter.last_fetch_partial)
        self.assertFalse(adapter.last_fetch_complete)
        self.assertEqual(adapter._get_json.await_count, 1)


    async def test_get_json_uses_curl_fallback_for_remote_protocol_error(self):
        adapter = PolymarketAdapter()
        adapter.client.get = AsyncMock(side_effect=httpx.RemoteProtocolError("boom"))
        adapter._curl_get_json = AsyncMock(return_value={"data": []})

        result = await adapter._get_json("/markets", params={"limit": 1})

        self.assertEqual(result, {"data": []})
        adapter._curl_get_json.assert_awaited_once()


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
        self.assertNotIn("after", first_call.kwargs["params"])


    async def test_get_json_uses_curl_fallback_for_read_timeout(self):
        adapter = PredictFunAdapter()
        adapter.client.get = AsyncMock(side_effect=httpx.ReadTimeout("timeout"))
        adapter._curl_get_json = AsyncMock(return_value={"data": []})

        result = await adapter._get_json("/markets", params={"first": 1})

        self.assertEqual(result, {"data": []})
        adapter._curl_get_json.assert_awaited_once()
