import unittest
from datetime import datetime
from datetime import timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

from arbitrage_bot.services import ingestion as ingestion_module
from arbitrage_bot.services.ingestion import IngestionService


class IngestionOutcomeNormalizationTests(unittest.TestCase):
    def setUp(self):
        ingestion_module._source_last_sync_completed_at.clear()
        self.service = IngestionService(db_session=None)


    def test_normalizes_string_outcomes(self):
        result = self.service._normalize_outcomes(["Yes", "No"])

        self.assertEqual(
            result,
            [
                {"id": "0", "label": "Yes", "slug": "yes"},
                {"id": "1", "label": "No", "slug": "no"},
            ],
        )


    def test_normalizes_dict_outcomes_and_preserves_ids(self):
        result = self.service._normalize_outcomes(
            [
                {"id": 11, "label": "Yes", "token_id": "poly-yes"},
                {"contractId": "pf-no", "name": "No"},
            ]
        )

        self.assertEqual(result[0]["id"], "11")
        self.assertEqual(result[0]["slug"], "yes")
        self.assertEqual(result[0]["token_id"], "poly-yes")
        self.assertEqual(result[1]["id"], "pf-no")
        self.assertEqual(result[1]["slug"], "no")
        self.assertEqual(result[1]["contract_id"], "pf-no")


    def test_parses_json_string_outcomes_before_normalizing(self):
        result = self.service._normalize_outcomes('["Yes", "No"]')

        self.assertEqual(
            result,
            [
                {"id": "0", "label": "Yes", "slug": "yes"},
                {"id": "1", "label": "No", "slug": "no"},
            ],
        )


    def test_maps_predict_fun_active_market_from_trading_status(self):
        mapped = self.service._map_predict_fun_market(
            {
                "id": 77,
                "title": "Spurs",
                "question": "Knicks vs. Spurs",
                "tradingStatus": "OPEN",
                "status": "RESOLVED",
                "categorySlug": "nba",
                "imageUrl": "https://example.test/image",
                "outcomes": [
                    {"onChainId": "yes-1", "name": "Yes"},
                    {"onChainId": "no-1", "name": "No"},
                ],
            }
        )

        self.assertEqual(mapped["status"], "active")
        self.assertTrue(mapped["tradable"])
        self.assertEqual(mapped["title"], "Knicks vs. Spurs")
        self.assertEqual(mapped["category"], "nba")
        self.assertEqual(mapped["outcomes_json"][0]["id"], "yes-1")
        self.assertEqual(mapped["outcomes_json"][1]["slug"], "no")


    def test_map_polymarket_market_keeps_explicit_outcome_token_ids(self):
        mapped = self.service._map_polymarket_market(
            {
                "id": 88,
                "title": "Trail Blazers vs Nuggets",
                "tradable": True,
                "clobTokenIds": '["clob-a", "clob-b"]',
                "outcomes": [
                    {"label": "Trail Blazers", "token_id": "explicit-a"},
                    {"label": "Nuggets", "token_id": "explicit-b"},
                ],
            }
        )

        self.assertEqual(mapped["outcomes_json"][0]["id"], "explicit-a")
        self.assertEqual(mapped["outcomes_json"][1]["id"], "explicit-b")
        self.assertEqual(mapped["outcomes_json"][0]["clob_token_id"], "clob-a")
        self.assertEqual(mapped["outcomes_json"][1]["clob_token_id"], "clob-b")


class IngestionLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        ingestion_module._source_last_sync_completed_at.clear()
        ingestion_module._source_last_full_sync_completed_at.clear()


    async def test_mark_missing_markets_closed_marks_absent_active_market_as_closed(self):
        missing_market = SimpleNamespace(
            platform="predict_fun",
            platform_market_id="9212",
            status="active",
            tradable=True,
            updated_at=None,
        )
        present_market = SimpleNamespace(
            platform="predict_fun",
            platform_market_id="9213",
            status="active",
            tradable=True,
            updated_at=None,
        )

        class FakeScalarResult:
            def __init__(self, items):
                self._items = items


            def scalars(self):
                return self


            def all(self):
                return list(self._items)


        class FakeDbSession:
            async def execute(self, stmt):
                return FakeScalarResult([missing_market, present_market])


        service = IngestionService(db_session=FakeDbSession())
        before = datetime.now(timezone.utc)

        await service._mark_missing_markets_closed("predict_fun", {"9213"})

        self.assertEqual(missing_market.status, "closed")
        self.assertFalse(missing_market.tradable)
        self.assertGreaterEqual(missing_market.updated_at, before)
        self.assertEqual(present_market.status, "active")
        self.assertTrue(present_market.tradable)


    async def test_sync_markets_skips_full_resync_within_interval(self):
        class FakeDbSession:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        service = IngestionService(db_session=FakeDbSession())
        service.polymarket.fetch_markets = AsyncMock(return_value=[])
        service.predict_fun.fetch_markets = AsyncMock(return_value=[])
        service._sync_source = AsyncMock(return_value=True)

        with patch.object(ingestion_module.settings, "MARKET_SYNC_INTERVAL_SECONDS", 300.0), patch.object(
            ingestion_module.settings,
            "MARKET_REFRESH_SECONDS",
            60,
        ):
            first = await service.sync_markets()
            second = await service.sync_markets()

        self.assertTrue(first["synced"])
        self.assertTrue(first["attempted"])
        self.assertEqual(set(first["successful_sources"]), {"polymarket", "predict.fun"})
        self.assertFalse(second["synced"])
        self.assertFalse(second["attempted"])
        self.assertEqual(second["successful_sources"], [])
        self.assertEqual(service.polymarket.fetch_markets.await_count, 1)
        self.assertEqual(service.predict_fun.fetch_markets.await_count, 1)


    async def test_sync_markets_retries_partial_source_on_next_cycle(self):
        class FakeDbSession:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        service = IngestionService(db_session=FakeDbSession())
        service.polymarket.fetch_markets = AsyncMock(return_value=[])
        service.predict_fun.fetch_markets = AsyncMock(return_value=[])
        service._sync_source = AsyncMock(side_effect=[False, True, False])

        with patch.object(ingestion_module.settings, "MARKET_SYNC_INTERVAL_SECONDS", 300.0), patch.object(
            ingestion_module.settings,
            "MARKET_REFRESH_SECONDS",
            60,
        ):
            first = await service.sync_markets()
            second = await service.sync_markets()

        self.assertTrue(first["synced"])
        self.assertFalse(second["synced"])
        self.assertEqual(set(first["successful_sources"]), {"predict.fun"})
        self.assertEqual(second["successful_sources"], [])
        self.assertEqual(service.polymarket.fetch_markets.await_count, 2)
        self.assertEqual(service.predict_fun.fetch_markets.await_count, 1)


    async def test_sync_markets_reports_failed_attempt_when_all_sources_fail(self):
        class FakeDbSession:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        service = IngestionService(db_session=FakeDbSession())
        service.polymarket.fetch_markets = AsyncMock(return_value=[])
        service.predict_fun.fetch_markets = AsyncMock(return_value=[])
        service._sync_source = AsyncMock(return_value=False)

        with patch.object(ingestion_module.settings, "MARKET_SYNC_INTERVAL_SECONDS", 0.0), patch.object(
            ingestion_module.settings,
            "MARKET_REFRESH_SECONDS",
            0,
        ):
            result = await service.sync_markets()

        self.assertFalse(result["synced"])
        self.assertTrue(result["attempted"])
        self.assertEqual(result["successful_sources"], [])


    async def test_sync_markets_uses_incremental_polymarket_fetch_between_full_syncs(self):
        class FakeDbSession:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        service = IngestionService(db_session=FakeDbSession())
        service.polymarket.fetch_markets = AsyncMock(return_value=[])
        service.predict_fun.fetch_markets = AsyncMock(return_value=[])
        service._sync_source = AsyncMock(return_value=True)
        service.polymarket.last_fetch_complete = True

        with patch.object(ingestion_module.settings, "MARKET_SYNC_INTERVAL_SECONDS", 0.0), patch.object(
            ingestion_module.settings,
            "MARKET_REFRESH_SECONDS",
            0,
        ), patch.object(
            ingestion_module.settings,
            "POLYMARKET_FULL_SYNC_INTERVAL_SECONDS",
            1800.0,
        ), patch.object(
            ingestion_module.settings,
            "POLYMARKET_INCREMENTAL_MAX_PAGES",
            7,
        ):
            await service.sync_markets()
            await service.sync_markets()

        first_call = service.polymarket.fetch_markets.await_args_list[0]
        second_call = service.polymarket.fetch_markets.await_args_list[1]
        self.assertIsNone(first_call.kwargs["max_pages"])
        self.assertEqual(second_call.kwargs["max_pages"], 7)


    async def test_sync_markets_retries_full_polymarket_sync_until_complete(self):
        class FakeDbSession:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        service = IngestionService(db_session=FakeDbSession())
        service.polymarket.fetch_markets = AsyncMock(return_value=[])
        service.predict_fun.fetch_markets = AsyncMock(return_value=[])
        service._sync_source = AsyncMock(return_value=True)

        with patch.object(ingestion_module.settings, "MARKET_SYNC_INTERVAL_SECONDS", 0.0), patch.object(
            ingestion_module.settings,
            "MARKET_REFRESH_SECONDS",
            0,
        ), patch.object(
            ingestion_module.settings,
            "POLYMARKET_FULL_SYNC_INTERVAL_SECONDS",
            1800.0,
        ), patch.object(
            ingestion_module.settings,
            "POLYMARKET_INCREMENTAL_MAX_PAGES",
            7,
        ):
            service.polymarket.last_fetch_complete = False
            await service.sync_markets()
            service.polymarket.last_fetch_complete = True
            await service.sync_markets()

        first_call = service.polymarket.fetch_markets.await_args_list[0]
        second_call = service.polymarket.fetch_markets.await_args_list[1]
        self.assertIsNone(first_call.kwargs["max_pages"])
        self.assertIsNone(second_call.kwargs["max_pages"])


    async def test_sync_source_dedupes_duplicate_market_rows_before_upsert(self):
        class FakeDbSession:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        service = IngestionService(db_session=FakeDbSession())
        service._upsert_markets = AsyncMock(return_value=set())
        service._mark_missing_markets_closed = AsyncMock(return_value=set())

        payload = [
            {"id": "100", "title": "first copy"},
            {"id": "100", "title": "second copy"},
            {"id": "101", "title": "unique"},
        ]

        def mapper(item):
            return {
                "platform": "polymarket",
                "platform_market_id": str(item["id"]),
                "status": "active",
                "tradable": True,
                "title": item["title"],
                "normalized_title": item["title"].lower(),
                "description": "",
                "outcomes_json": [],
                "raw_payload_json": dict(item),
                "category": "",
                "slug": "",
            }

        synced = await service._sync_source("polymarket", payload, mapper)

        self.assertTrue(synced)
        upserted_items = service._upsert_markets.await_args_list[0].args[0]
        self.assertEqual(len(upserted_items), 2)
        self.assertEqual(
            [item["platform_market_id"] for item in upserted_items],
            ["100", "101"],
        )
        self.assertEqual(upserted_items[0]["title"], "second copy")
        service._mark_missing_markets_closed.assert_awaited_once_with(
            "polymarket",
            {"100", "101"},
        )


    async def test_sync_source_partial_payload_skips_stale_detection_and_returns_incomplete(self):
        class FakeDbSession:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        service = IngestionService(db_session=FakeDbSession())
        service._upsert_markets = AsyncMock(return_value={101})
        service._mark_missing_markets_closed = AsyncMock(return_value=set())
        adapter = SimpleNamespace(last_fetch_partial=True)

        payload = [
            {"id": "100", "title": "only page"},
        ]

        def mapper(item):
            return {
                "platform": "polymarket",
                "platform_market_id": str(item["id"]),
                "status": "active",
                "tradable": True,
                "title": item["title"],
                "normalized_title": item["title"].lower(),
                "description": "",
                "outcomes_json": [],
                "raw_payload_json": dict(item),
                "category": "",
                "slug": "",
            }

        synced = await service._sync_source("polymarket", payload, mapper, adapter=adapter)

        self.assertFalse(synced)
        service._upsert_markets.assert_awaited_once()
        service._mark_missing_markets_closed.assert_not_awaited()
        self.assertEqual(service._changed_market_ids_by_platform["polymarket"], {101})


    async def test_sync_source_incomplete_payload_skips_stale_detection_but_counts_as_success(self):
        class FakeDbSession:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        service = IngestionService(db_session=FakeDbSession())
        service._upsert_markets = AsyncMock(return_value={101})
        service._mark_missing_markets_closed = AsyncMock(return_value=set())
        adapter = SimpleNamespace(last_fetch_partial=False, last_fetch_complete=False)

        payload = [
            {"id": "100", "title": "top page"},
        ]

        def mapper(item):
            return {
                "platform": "polymarket",
                "platform_market_id": str(item["id"]),
                "status": "active",
                "tradable": True,
                "title": item["title"],
                "normalized_title": item["title"].lower(),
                "description": "",
                "outcomes_json": [],
                "raw_payload_json": dict(item),
                "category": "",
                "slug": "",
            }

        synced = await service._sync_source("polymarket", payload, mapper, adapter=adapter)

        self.assertTrue(synced)
        service._upsert_markets.assert_awaited_once()
        service._mark_missing_markets_closed.assert_not_awaited()
        self.assertEqual(service._changed_market_ids_by_platform["polymarket"], {101})


    async def test_sync_source_with_empty_payload_skips_mass_closing_markets(self):
        class FakeDbSession:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        service = IngestionService(db_session=FakeDbSession())
        service._upsert_markets = AsyncMock(return_value=set())
        service._mark_missing_markets_closed = AsyncMock(return_value=set())

        synced = await service._sync_source("predict.fun", [], service._map_predict_fun_market)

        self.assertTrue(synced)
        service._upsert_markets.assert_not_awaited()
        service._mark_missing_markets_closed.assert_not_awaited()
        self.assertEqual(
            service._changed_market_ids_by_platform["predict_fun"],
            set(),
        )


    async def test_sync_source_partial_empty_payload_does_not_count_as_success(self):
        class FakeDbSession:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        service = IngestionService(db_session=FakeDbSession())
        service._upsert_markets = AsyncMock(return_value=set())
        service._mark_missing_markets_closed = AsyncMock(return_value=set())
        adapter = SimpleNamespace(last_fetch_partial=True)

        synced = await service._sync_source(
            "polymarket",
            [],
            service._map_polymarket_market,
            adapter=adapter,
        )

        self.assertFalse(synced)
        service._upsert_markets.assert_not_awaited()
        service._mark_missing_markets_closed.assert_not_awaited()
        self.assertEqual(
            service._changed_market_ids_by_platform["polymarket"],
            set(),
        )


    def test_apply_market_updates_skips_unchanged_market_payload(self):
        service = IngestionService(db_session=None)
        updated_at = datetime(2026, 4, 8, tzinfo=timezone.utc)
        market = SimpleNamespace(
            status="active",
            tradable=True,
            title="Market",
            normalized_title="market",
            description="desc",
            outcomes_json=[{"id": "1", "label": "Yes"}],
            raw_payload_json={"id": "1", "title": "Market"},
            category="sports",
            slug="market",
            updated_at=updated_at,
        )
        data = {
            "status": "active",
            "tradable": True,
            "title": "Market",
            "normalized_title": "market",
            "description": "desc",
            "outcomes_json": [{"id": "1", "label": "Yes"}],
            "raw_payload_json": {"id": "1", "title": "Market"},
            "category": "sports",
            "slug": "market",
        }

        changed = service._apply_market_updates(market, data)

        self.assertFalse(changed)
        self.assertEqual(market.updated_at, updated_at)
