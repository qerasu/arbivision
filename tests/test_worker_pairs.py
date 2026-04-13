import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

from arbitrage_bot.core.observability import reset_counters
from arbitrage_bot.core.observability import snapshot_counters
from arbitrage_bot.services.matcher import MatcherService
from arbitrage_bot import worker as worker_module
from arbitrage_bot.worker import WorkerState, _build_cached_market_signatures, _build_candidate_index_from_signatures, _candidate_markets_for_poly, _filter_skippable_pairs, _load_candidate_context, _mark_stale_pairs, _process_candidates, _prune_market_signature_cache, _reconcile_market_pairs, _run_cycle, _update_empty_counts, _upsert_market_pairs


class WorkerPairLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.state = WorkerState()


    def test_reconcile_updates_existing_pair_and_keeps_manual_approval(self):
        existing_pair = SimpleNamespace(
            pair_hash="pair-1",
            status="approved",
            match_score=0.71,
            match_reason_json={"old": True},
            outcome_mapping_json={"market_a": {"yes": "old-y", "no": "old-n"}},
        )
        matched_pair = SimpleNamespace(
            pair_hash="pair-1",
            status="auto_approved",
            match_score=0.91,
            match_reason_json={"old": False},
            outcome_mapping_json={"market_a": {"yes": "new-y", "no": "new-n"}},
        )

        new_pairs, has_updates = _reconcile_market_pairs(
            [existing_pair],
            {"pair-1": matched_pair},
        )

        self.assertEqual(new_pairs, [])
        self.assertTrue(has_updates)
        self.assertEqual(existing_pair.status, "approved")
        self.assertEqual(existing_pair.match_score, 0.91)
        self.assertEqual(existing_pair.match_reason_json, {"old": False})
        self.assertEqual(existing_pair.outcome_mapping_json, {"market_a": {"yes": "new-y", "no": "new-n"}})


    def test_reconcile_marks_unmatched_pairs_as_stale(self):
        existing_pair = SimpleNamespace(
            pair_hash="pair-1",
            status="auto_approved",
            match_score=0.88,
            match_reason_json={"title": "old"},
            outcome_mapping_json={"market_a": {"yes": "old-y", "no": "old-n"}},
        )

        new_pairs, has_updates = _reconcile_market_pairs([existing_pair], {})

        self.assertEqual(new_pairs, [])
        self.assertTrue(has_updates)
        self.assertEqual(existing_pair.status, "stale")


    def test_reconcile_creates_new_pairs(self):
        matched_pair = SimpleNamespace(
            pair_hash="pair-2",
            status="auto_approved",
            match_score=0.91,
            match_reason_json={"title": "new"},
            outcome_mapping_json={"market_a": {"yes": "poly-y", "no": "poly-n"}},
        )

        new_pairs, has_updates = _reconcile_market_pairs([], {"pair-2": matched_pair})

        self.assertEqual(new_pairs, [matched_pair])
        self.assertFalse(has_updates)


    def test_limit_active_pairs_for_cycle_prioritizes_closest_market_end(self):
        pair_soon = SimpleNamespace(id=1, pair_hash="pair-soon", market_id_a=10, market_id_b=20)
        pair_late = SimpleNamespace(id=2, pair_hash="pair-late", market_id_a=30, market_id_b=40)
        market_map = {
            10: SimpleNamespace(raw_payload_json={"endDate": "2026-04-11T12:00:00+00:00"}),
            20: SimpleNamespace(raw_payload_json={"resolveDate": "2026-04-11T12:05:00+00:00"}),
            30: SimpleNamespace(raw_payload_json={"endDate": "2026-04-14T12:00:00+00:00"}),
            40: SimpleNamespace(raw_payload_json={"resolveDate": "2026-04-14T12:05:00+00:00"}),
        }

        with patch.object(worker_module.settings, "MAX_ACTIVE_PAIRS_PER_CYCLE", 1):
            limited = worker_module._limit_active_pairs_for_cycle(
                [pair_late, pair_soon],
                market_map,
                self.state,
            )

        self.assertEqual(limited, [pair_soon])


    def test_limit_active_pairs_for_cycle_rotates_within_same_bucket(self):
        pair_a = SimpleNamespace(id=1, pair_hash="pair-a", market_id_a=10, market_id_b=20)
        pair_b = SimpleNamespace(id=2, pair_hash="pair-b", market_id_a=30, market_id_b=40)
        pair_c = SimpleNamespace(id=3, pair_hash="pair-c", market_id_a=50, market_id_b=60)
        market_map = {
            10: SimpleNamespace(raw_payload_json={"endDate": "2026-04-11T12:00:00+00:00"}),
            20: SimpleNamespace(raw_payload_json={"resolveDate": "2026-04-11T12:05:00+00:00"}),
            30: SimpleNamespace(raw_payload_json={"endDate": "2026-04-11T12:10:00+00:00"}),
            40: SimpleNamespace(raw_payload_json={"resolveDate": "2026-04-11T12:15:00+00:00"}),
            50: SimpleNamespace(raw_payload_json={"endDate": "2026-04-11T12:20:00+00:00"}),
            60: SimpleNamespace(raw_payload_json={"resolveDate": "2026-04-11T12:25:00+00:00"}),
        }

        with patch.object(worker_module.settings, "MAX_ACTIVE_PAIRS_PER_CYCLE", 2):
            first = worker_module._limit_active_pairs_for_cycle(
                [pair_a, pair_b, pair_c],
                market_map,
                self.state,
            )
            second = worker_module._limit_active_pairs_for_cycle(
                [pair_a, pair_b, pair_c],
                market_map,
                self.state,
            )

        self.assertEqual([pair.pair_hash for pair in first], ["pair-a", "pair-b"])
        self.assertEqual([pair.pair_hash for pair in second], ["pair-c", "pair-a"])


    def test_mark_stale_pairs_changes_only_active_statuses(self):
        stale_pair = SimpleNamespace(status="stale", pair_hash="h-stale")
        approved_pair = SimpleNamespace(status="approved", pair_hash="h-approved")
        failed_pair = SimpleNamespace(status="failed", pair_hash="h-failed")

        changed = _mark_stale_pairs([stale_pair, approved_pair, failed_pair])

        self.assertTrue(changed)
        self.assertEqual(stale_pair.status, "stale")
        self.assertEqual(approved_pair.status, "stale")
        self.assertEqual(failed_pair.status, "failed")


    def test_candidate_markets_for_poly_limits_ranked_candidates(self):
        matcher = MatcherService()
        matcher.max_ranked_candidates = 2
        poly_market = SimpleNamespace(
            id=1,
            title="Alpha Beta Gamma",
            outcomes_json=[],
            raw_payload_json={},
            category="sports",
        )
        pf_markets = [
            SimpleNamespace(id=10, title="Alpha Beta One", outcomes_json=[], raw_payload_json={}, category="sports"),
            SimpleNamespace(id=11, title="Alpha Beta Two", outcomes_json=[], raw_payload_json={}, category="sports"),
            SimpleNamespace(id=12, title="Alpha Beta Three", outcomes_json=[], raw_payload_json={}, category="sports"),
        ]

        pf_index = matcher.build_candidate_index(pf_markets)
        poly_signature = matcher.build_market_signature(poly_market)

        candidates = _candidate_markets_for_poly(poly_signature, matcher, pf_index)

        self.assertEqual(len(candidates), 2)


    def test_candidate_markets_for_poly_uses_coarse_ranking_signals(self):
        matcher = MatcherService()
        poly_market = SimpleNamespace(
            id=1,
            title="Grizzlies vs Hornets March 15 2026",
            outcomes_json=[],
            raw_payload_json={},
            category="nba",
        )
        pf_markets = [
            SimpleNamespace(id=10, title="Grizzlies vs Hornets March 15 2026", outcomes_json=[], raw_payload_json={}, category="nba"),
            SimpleNamespace(id=11, title="Grizzlies vs Hornets", outcomes_json=[], raw_payload_json={}, category="politics"),
        ]

        pf_index = matcher.build_candidate_index(pf_markets)
        poly_signature = matcher.build_market_signature(poly_market)

        candidates = _candidate_markets_for_poly(poly_signature, matcher, pf_index)

        self.assertEqual(candidates[0]["market"].id, 10)


    def test_build_cached_market_signatures_reuses_unchanged_market_signature(self):
        matcher = Mock()
        matcher.build_market_signature.side_effect = lambda market: {
            "market": market,
            "tokens": {market.title.lower()},
            "condition_ids": [],
        }
        market = SimpleNamespace(
            id=1,
            title="Alpha",
            category="sports",
            outcomes_json=[],
            raw_payload_json={},
            status="active",
            updated_at="v1",
        )

        first = _build_cached_market_signatures([market], matcher, self.state)
        second = _build_cached_market_signatures([market], matcher, self.state)

        self.assertEqual(matcher.build_market_signature.call_count, 1)
        self.assertIs(first[1], second[1])


    def test_build_cached_market_signatures_rebuilds_changed_market_signature(self):
        matcher = Mock()
        matcher.build_market_signature.side_effect = lambda market: {
            "market": market,
            "tokens": {market.title.lower()},
            "condition_ids": [],
        }
        market = SimpleNamespace(
            id=1,
            title="Alpha",
            category="sports",
            outcomes_json=[],
            raw_payload_json={},
            status="active",
            updated_at="v1",
        )

        _build_cached_market_signatures([market], matcher, self.state)
        market.updated_at = "v2"
        signatures = _build_cached_market_signatures([market], matcher, self.state)

        self.assertEqual(matcher.build_market_signature.call_count, 2)
        self.assertEqual(signatures[1]["market"].updated_at, "v2")


    def test_build_candidate_index_from_signatures_uses_prebuilt_signatures(self):
        signatures = {
            1: {
                "market": SimpleNamespace(id=1),
                "tokens": {"alpha", "beta"},
                "condition_ids": ["cond-1"],
            },
            2: {
                "market": SimpleNamespace(id=2),
                "tokens": {"beta", "gamma"},
                "condition_ids": ["cond-2"],
            },
        }

        index = _build_candidate_index_from_signatures(signatures)

        self.assertEqual(len(index["tokens"]["beta"]), 2)
        self.assertEqual(index["condition_ids"]["cond-1"][0]["market"].id, 1)


    def test_prune_market_signature_cache_removes_missing_market_ids(self):
        self.state.market_signature_cache[1] = {
            "fingerprint": ("alpha",),
            "signature": {"market": SimpleNamespace(id=1)},
            "last_seen_at": 1.0,
        }
        self.state.market_signature_cache[2] = {
            "fingerprint": ("beta",),
            "signature": {"market": SimpleNamespace(id=2)},
            "last_seen_at": 2.0,
        }

        _prune_market_signature_cache(self.state, [SimpleNamespace(id=2)], [])

        self.assertNotIn(1, self.state.market_signature_cache)
        self.assertIn(2, self.state.market_signature_cache)


    def test_upsert_market_pairs_matches_only_changed_markets(self):
        class FakeDb:
            def __init__(self):
                self.added = []
                self.commit_calls = 0
                self.rollback_calls = 0
                self.flush_calls = 0


            def add_all(self, items):
                self.added.extend(items)


            async def flush(self):
                self.flush_calls += 1


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        poly_markets = [
            SimpleNamespace(id=1, title="poly one", category="sports", outcomes_json=[], raw_payload_json={}, status="active", updated_at="v1"),
            SimpleNamespace(id=2, title="poly two", category="sports", outcomes_json=[], raw_payload_json={}, status="active", updated_at="v1"),
        ]
        pf_markets = [
            SimpleNamespace(id=10, title="pf one", category="sports", outcomes_json=[], raw_payload_json={}, status="active", updated_at="v1"),
            SimpleNamespace(id=11, title="pf two", category="sports", outcomes_json=[], raw_payload_json={}, status="active", updated_at="v1"),
            SimpleNamespace(id=12, title="pf three", category="sports", outcomes_json=[], raw_payload_json={}, status="active", updated_at="v1"),
        ]
        matcher = Mock()
        matcher.max_ranked_candidates = 25
        matcher.build_market_signature.side_effect = lambda market: {
            "market": market,
            "tokens": {"shared", market.title},
            "condition_ids": [],
            "category_tokens": {"sports"},
            "entities": {"dates": [], "numbers": []},
            "participants": [],
            "kind": "single",
        }
        matcher.candidate_rank_score.return_value = 1.0
        matcher.match_candidates.side_effect = lambda poly_market, pf_market, **kwargs: SimpleNamespace(
            pair_hash=f"{poly_market.id}-{pf_market.id}",
            status="auto_approved",
            match_score=0.9,
            match_reason_json={"ok": True},
            outcome_mapping_json={"market_a": {}},
        )
        fake_db = FakeDb()

        with patch(
            "arbitrage_bot.worker._load_active_markets_by_platform",
            new=AsyncMock(return_value=(poly_markets, pf_markets)),
        ), patch(
            "arbitrage_bot.worker._load_pairs_for_market_ids",
            new=AsyncMock(return_value=[]),
        ):
            asyncio.run(
                _upsert_market_pairs(
                    fake_db,
                    matcher,
                    {
                        "polymarket": {1},
                        "predict_fun": set(),
                    },
                    self.state,
                )
            )

        self.assertEqual(matcher.match_candidates.call_count, 3)
        self.assertEqual(len(fake_db.added), 3)
        self.assertEqual(fake_db.commit_calls, 1)


class WorkerCandidateContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_load_candidate_context_caches_snapshots_instead_of_original_orm_objects(self):
        original_pair = SimpleNamespace(
            id=1,
            market_id_a=10,
            market_id_b=20,
            pair_hash="pair-1",
            status="approved",
            match_score=0.9,
            match_reason_json={"ok": True},
            outcome_mapping_json={"market_a": {}},
        )
        original_market = SimpleNamespace(
            id=10,
            platform="polymarket",
            platform_market_id="poly-10",
            status="active",
            tradable=True,
            title="Alpha",
            normalized_title="alpha",
            description="desc",
            outcomes_json=[],
            raw_payload_json={"endDate": "2026-04-14T12:00:00+00:00"},
            category="sports",
            slug="alpha",
            updated_at="v1",
            created_at="v0",
        )

        class FakeScalars:
            def __init__(self, values):
                self._values = values


            def all(self):
                return list(self._values)


        class FakeExecuteResult:
            def __init__(self, values):
                self._values = values


            def scalars(self):
                return FakeScalars(self._values)


        class FakeDb:
            async def execute(self, _stmt):
                return FakeExecuteResult([original_pair])


        state = WorkerState()

        with patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={10: original_market}),
        ):
            pairs, market_map = await _load_candidate_context(FakeDb(), state)

        self.assertIsNot(pairs[0], original_pair)
        self.assertIsNot(market_map[10], original_market)
        self.assertEqual(pairs[0].pair_hash, original_pair.pair_hash)
        self.assertEqual(market_map[10].platform_market_id, original_market.platform_market_id)


class FakePipeline:
    def __init__(self, redis):
        self._redis = redis
        self._commands = []


    def incr(self, key):
        self._commands.append(("incr", key))
        return self


    def expire(self, key, ttl):
        self._commands.append(("expire", key, ttl))
        return self


    def delete(self, key):
        self._commands.append(("delete", key))
        return self


    async def execute(self):
        results = []
        for cmd in self._commands:
            if cmd[0] == "incr":
                key = cmd[1]
                current = int(self._redis.data.get(key, "0"))
                current += 1
                self._redis.data[key] = str(current)
                results.append(current)
            elif cmd[0] == "expire":
                results.append(True)
            elif cmd[0] == "delete":
                self._redis.data.pop(cmd[1], None)
                results.append(1)
        return results


class FakeRedis:
    def __init__(self):
        self.data = {}


    async def get(self, key):
        return self.data.get(key)


    async def mget(self, keys):
        return [self.data.get(key) for key in keys]


    async def setex(self, key, ttl, value):
        self.data[key] = value


    async def delete(self, key):
        self.data.pop(key, None)


    def pipeline(self):
        return FakePipeline(self)


class WorkerEmptyOrderbookStateTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        reset_counters()
        self.state = WorkerState()
        self.system_error_patcher = patch(
            "arbitrage_bot.worker.send_system_error_notification",
            new=AsyncMock(return_value=False),
        )
        self.system_error_patcher.start()


    def tearDown(self):
        self.system_error_patcher.stop()


    async def test_run_cycle_skips_pair_rebuild_when_market_sync_was_not_needed(self):
        fake_db = SimpleNamespace()
        ingestion = SimpleNamespace(
            sync_markets=AsyncMock(return_value=False),
            close=AsyncMock(),
        )
        orderbook_service = SimpleNamespace(close=AsyncMock())

        with patch("arbitrage_bot.worker.IngestionService", return_value=ingestion), patch(
            "arbitrage_bot.worker.MatcherService",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.OrderbookService",
            return_value=orderbook_service,
        ), patch(
            "arbitrage_bot.worker.ArbitrageCalculator",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker._upsert_market_pairs",
            new=AsyncMock(),
        ) as upsert_mock, patch(
            "arbitrage_bot.worker._process_candidates",
            new=AsyncMock(
                return_value={
                    "approved_pairs": 0,
                    "active_pairs": 0,
                    "pairs_with_books": 0,
                    "skipped_pairs": 0,
                    "opportunities": 0,
                    "deliverable_opportunities": 0,
                }
            ),
        ) as process_mock, patch(
            "arbitrage_bot.worker._should_run_full_pair_rematch",
            return_value=False,
        ):
            await _run_cycle(fake_db, self.state)

        upsert_mock.assert_not_awaited()
        process_mock.assert_awaited_once()
        ingestion.close.assert_awaited_once()
        orderbook_service.close.assert_awaited_once()


    async def test_run_cycle_performs_full_pair_rematch_even_without_market_changes(self):
        fake_db = SimpleNamespace()
        ingestion = SimpleNamespace(
            sync_markets=AsyncMock(return_value=False),
            close=AsyncMock(),
        )
        orderbook_service = SimpleNamespace(close=AsyncMock())

        with patch("arbitrage_bot.worker.IngestionService", return_value=ingestion), patch(
            "arbitrage_bot.worker.MatcherService",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.OrderbookService",
            return_value=orderbook_service,
        ), patch(
            "arbitrage_bot.worker.ArbitrageCalculator",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker._upsert_market_pairs",
            new=AsyncMock(),
        ) as upsert_mock, patch(
            "arbitrage_bot.worker._process_candidates",
            new=AsyncMock(
                return_value={
                    "approved_pairs": 0,
                    "active_pairs": 0,
                    "pairs_with_books": 0,
                    "skipped_pairs": 0,
                    "opportunities": 0,
                    "deliverable_opportunities": 0,
                }
            ),
        ), patch(
            "arbitrage_bot.worker._should_run_full_pair_rematch",
            return_value=True,
        ):
            await _run_cycle(fake_db, self.state)

        upsert_mock.assert_awaited_once()
        self.assertIsNone(upsert_mock.await_args.args[2])
        ingestion.close.assert_awaited_once()
        orderbook_service.close.assert_awaited_once()


    async def test_run_cycle_uses_incremental_pair_rebuild_for_changed_market_ids(self):
        fake_db = SimpleNamespace()
        ingestion = SimpleNamespace(
            sync_markets=AsyncMock(
                return_value={
                    "synced": True,
                    "attempted": True,
                    "successful_sources": ["polymarket"],
                    "changed_market_ids_by_platform": {
                        "polymarket": {11},
                        "predict_fun": set(),
                    },
                }
            ),
            close=AsyncMock(),
        )
        orderbook_service = SimpleNamespace(close=AsyncMock())

        with patch("arbitrage_bot.worker.IngestionService", return_value=ingestion), patch(
            "arbitrage_bot.worker.MatcherService",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.OrderbookService",
            return_value=orderbook_service,
        ), patch(
            "arbitrage_bot.worker.ArbitrageCalculator",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker._upsert_market_pairs",
            new=AsyncMock(),
        ) as upsert_mock, patch(
            "arbitrage_bot.worker._process_candidates",
            new=AsyncMock(
                return_value={
                    "approved_pairs": 0,
                    "active_pairs": 0,
                    "pairs_with_books": 0,
                    "skipped_pairs": 0,
                    "opportunities": 0,
                    "deliverable_opportunities": 0,
                }
            ),
        ), patch(
            "arbitrage_bot.worker._should_run_full_pair_rematch",
            return_value=False,
        ):
            await _run_cycle(fake_db, self.state)

        self.assertEqual(
            upsert_mock.await_args.args[2],
            {
                "polymarket": {11},
                "predict_fun": set(),
            },
        )
        ingestion.close.assert_awaited_once()
        orderbook_service.close.assert_awaited_once()


    async def test_run_cycle_skips_pair_rebuild_when_sync_had_no_market_changes(self):
        fake_db = SimpleNamespace()
        ingestion = SimpleNamespace(
            sync_markets=AsyncMock(
                return_value={
                    "synced": True,
                    "attempted": True,
                    "successful_sources": ["polymarket"],
                    "changed_market_ids_by_platform": {
                        "polymarket": set(),
                        "predict_fun": set(),
                    },
                }
            ),
            close=AsyncMock(),
        )
        orderbook_service = SimpleNamespace(close=AsyncMock())

        with patch("arbitrage_bot.worker.IngestionService", return_value=ingestion), patch(
            "arbitrage_bot.worker.MatcherService",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.OrderbookService",
            return_value=orderbook_service,
        ), patch(
            "arbitrage_bot.worker.ArbitrageCalculator",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker._upsert_market_pairs",
            new=AsyncMock(),
        ) as upsert_mock, patch(
            "arbitrage_bot.worker._process_candidates",
            new=AsyncMock(
                return_value={
                    "approved_pairs": 0,
                    "active_pairs": 0,
                    "pairs_with_books": 0,
                    "skipped_pairs": 0,
                    "opportunities": 0,
                    "deliverable_opportunities": 0,
                }
            ),
        ), patch(
            "arbitrage_bot.worker._should_run_full_pair_rematch",
            return_value=False,
        ):
            await _run_cycle(fake_db, self.state)

        upsert_mock.assert_not_awaited()
        ingestion.close.assert_awaited_once()
        orderbook_service.close.assert_awaited_once()


    async def test_process_candidates_reuses_cached_pair_context_between_calls(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            def __init__(self):
                self.execute_calls = 0


            async def execute(self, stmt):
                self.execute_calls += 1
                compiled = str(stmt)
                if "FROM market_pairs" in compiled:
                    return FakeScalarResult(
                        [SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20)]
                    )
                if "FROM markets" in compiled:
                    return FakeScalarResult(
                        [
                            SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                            SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
                        ]
                    )
                raise AssertionError(f"unexpected stmt: {compiled}")


        fake_db = FakeDb()
        orderbook_service = SimpleNamespace(fetch_orderbooks_for_pairs=AsyncMock(return_value=[]))
        calculator = SimpleNamespace(calculate_opportunities=Mock(return_value=[]))
        alert_manager = SimpleNamespace(process_opportunity=AsyncMock(), finalize_opportunity=AsyncMock())
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(return_value=[]),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[])):
            await _process_candidates(fake_db, orderbook_service, calculator, alert_manager, fanout_manager, self.state)
            await _process_candidates(fake_db, orderbook_service, calculator, alert_manager, fanout_manager, self.state)

        self.assertEqual(fake_db.execute_calls, 2)


    async def test_filter_skippable_pairs_reads_threshold_from_redis(self):
        fake_redis = FakeRedis()
        fake_redis.data["worker:pair-empty-count:pair-1"] = "3"
        pairs = [
            SimpleNamespace(pair_hash="pair-1"),
            SimpleNamespace(pair_hash="pair-2"),
        ]

        with patch(
            "arbitrage_bot.worker.get_redis",
            new=AsyncMock(return_value=fake_redis),
        ):
            active_pairs = await _filter_skippable_pairs(pairs, self.state)

        self.assertEqual([pair.pair_hash for pair in active_pairs], ["pair-2"])


    async def test_update_empty_counts_persists_to_redis(self):
        fake_redis = FakeRedis()
        checked_pairs = [
            SimpleNamespace(pair_hash="pair-1"),
            SimpleNamespace(pair_hash="pair-2"),
        ]

        with patch(
            "arbitrage_bot.worker.get_redis",
            new=AsyncMock(return_value=fake_redis),
        ):
            await _update_empty_counts(checked_pairs, {"pair-2"}, self.state)

        self.assertEqual(fake_redis.data["worker:pair-empty-count:pair-1"], "1")
        self.assertNotIn("worker:pair-empty-count:pair-2", fake_redis.data)


    async def test_process_candidates_counts_calculator_drop_reason(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def execute(self, stmt):
                return FakeScalarResult([SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20)])


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        fake_db = FakeDb()
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {
                        "pair": SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
                        "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.7, 2)]}},
                    }
                ],
            )
        )
        calculator = SimpleNamespace(calculate_opportunities=Mock(return_value=[]))
        alert_manager = SimpleNamespace(
            process_opportunity=AsyncMock(),
            finalize_opportunity=AsyncMock(),
        )
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(return_value=[]),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[
            SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
        ])), patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={
                10: SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                20: SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
            }),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(),
        ):
            result = await _process_candidates(fake_db, orderbook_service, calculator, alert_manager, fanout_manager, self.state)

        self.assertEqual(result["opportunities"], 0)
        counters = snapshot_counters()
        self.assertEqual(counters["worker.active_pairs_loaded"], 1)
        self.assertEqual(counters["worker.pairs_with_orderbooks"], 1)
        self.assertEqual(counters["calculator.drop.no_profitable_directions"], 1)


    async def test_process_candidates_sends_opportunity_immediately_when_delivery_exists(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def execute(self, stmt):
                return FakeScalarResult([SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20)])


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        fake_db = FakeDb()
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {
                        "pair": SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
                        "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.5, 2)]}},
                    }
                ]
            )
        )
        calculator = SimpleNamespace(
            calculate_opportunities=Mock(
                return_value=[
                    {
                        "direction": "A_yes_B_no",
                        "avg_price_leg_1": 0.40,
                        "avg_price_leg_2": 0.50,
                        "shares": 10.0,
                        "capital_required": 9.0,
                        "gross_profit": 1.0,
                        "net_profit": 2.0,
                        "gross_roi": 0.11,
                        "net_roi": 0.22,
                    }
                ]
            )
        )
        alert_manager = SimpleNamespace(
            process_opportunity=AsyncMock(return_value=SimpleNamespace(id=55, fanout_status="queued")),
            finalize_opportunity=AsyncMock(),
        )
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(return_value=[{"alert": SimpleNamespace(id=88), "preferences": {}}]),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch.object(worker_module.settings, "APP_RUNTIME_MODE", "worker"), patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[
            SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
        ])), patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={
                10: SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                20: SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
            }),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker._persist_delivery_alert",
            new=AsyncMock(),
        ) as send_mock:
            result = await _process_candidates(
                fake_db,
                orderbook_service,
                calculator,
                alert_manager,
                fanout_manager,
                self.state,
            )

        self.assertEqual(result["opportunities"], 1)
        self.assertEqual(result["deliverable_opportunities"], 1)
        fanout_manager.get_delivery_targets.assert_awaited_once()
        fanout_manager.create_alert_deliveries.assert_awaited_once()
        alert_manager.finalize_opportunity.assert_awaited_once()
        send_mock.assert_awaited_once()
        counters = snapshot_counters()
        self.assertEqual(counters["worker.opportunities_created"], 1)


    async def test_process_candidates_sends_immediately_in_all_mode(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def execute(self, stmt):
                return FakeScalarResult([SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20)])


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        fake_db = FakeDb()
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {
                        "pair": SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
                        "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.5, 2)]}},
                    }
                ]
            )
        )
        calculator = SimpleNamespace(
            calculate_opportunities=Mock(
                return_value=[
                    {
                        "direction": "A_yes_B_no",
                        "avg_price_leg_1": 0.40,
                        "avg_price_leg_2": 0.50,
                        "shares": 10.0,
                        "capital_required": 9.0,
                        "gross_profit": 1.0,
                        "net_profit": 2.0,
                        "gross_roi": 0.11,
                        "net_roi": 0.22,
                    }
                ]
            )
        )
        alert_manager = SimpleNamespace(
            process_opportunity=AsyncMock(return_value=SimpleNamespace(id=55, fanout_status="queued")),
            finalize_opportunity=AsyncMock(),
        )
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(return_value=[{"alert": SimpleNamespace(id=88), "preferences": {}}]),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch.object(worker_module.settings, "APP_RUNTIME_MODE", "all"), patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[
            SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
        ])), patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={
                10: SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                20: SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
            }),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker._persist_delivery_alert",
            new=AsyncMock(),
        ) as send_mock:
            result = await _process_candidates(
                fake_db,
                orderbook_service,
                calculator,
                alert_manager,
                fanout_manager,
                self.state,
            )

        self.assertEqual(result["opportunities"], 1)
        self.assertEqual(result["deliverable_opportunities"], 1)
        self.assertEqual(fake_db.commit_calls, 2)
        send_mock.assert_awaited_once()


    async def test_process_candidates_counts_filtered_delivery_without_send(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def execute(self, stmt):
                return FakeScalarResult(
                    [
                        SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
                        SimpleNamespace(id=2, pair_hash="pair-2", market_id_a=30, market_id_b=40),
                    ]
                )


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        fake_db = FakeDb()
        orderbook_payload = {
            "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.5, 2)]}},
        }
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {"pair": SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20), **orderbook_payload},
                    {"pair": SimpleNamespace(id=2, pair_hash="pair-2", market_id_a=30, market_id_b=40), **orderbook_payload},
                ]
            )
        )
        calculator = SimpleNamespace(
            calculate_opportunities=Mock(
                return_value=[
                    {
                        "direction": "A_yes_B_no",
                        "avg_price_leg_1": 0.40,
                        "avg_price_leg_2": 0.50,
                        "shares": 10.0,
                        "capital_required": 9.0,
                        "gross_profit": 1.0,
                        "net_profit": 2.0,
                        "gross_roi": 0.11,
                        "net_roi": 0.22,
                    }
                ]
            )
        )

        async def process_opportunity(pair, calc_result):
            return SimpleNamespace(id=100 + pair.id, fanout_status="queued")


        alert_manager = SimpleNamespace(
            process_opportunity=AsyncMock(side_effect=process_opportunity),
            finalize_opportunity=AsyncMock(),
        )
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(
                side_effect=[
                    [{"alert": SimpleNamespace(id=201), "preferences": {}}],
                    [],
                ]
            ),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch.object(worker_module.settings, "APP_RUNTIME_MODE", "worker"), patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[
            SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
            SimpleNamespace(id=2, pair_hash="pair-2", market_id_a=30, market_id_b=40),
        ])), patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={
                10: SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                20: SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
                30: SimpleNamespace(id=30, platform="polymarket", platform_market_id="poly-30"),
                40: SimpleNamespace(id=40, platform="predict_fun", platform_market_id="pf-40"),
            }),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker._persist_delivery_alert",
            new=AsyncMock(),
        ) as send_mock:
            result = await _process_candidates(
                fake_db,
                orderbook_service,
                calculator,
                alert_manager,
                fanout_manager,
                self.state,
            )

        self.assertEqual(result["opportunities"], 2)
        self.assertEqual(result["deliverable_opportunities"], 1)
        fanout_manager.get_delivery_targets.assert_awaited_once()
        self.assertEqual(fanout_manager.create_alert_deliveries.await_count, 2)
        self.assertEqual(alert_manager.finalize_opportunity.await_count, 2)
        send_mock.assert_awaited_once()


    async def test_process_candidates_keeps_opportunity_without_send_when_no_delivery_exists(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def execute(self, stmt):
                return FakeScalarResult(
                    [
                        SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
                        SimpleNamespace(id=2, pair_hash="pair-2", market_id_a=30, market_id_b=40),
                    ]
                )


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        fake_db = FakeDb()
        orderbook_payload = {
            "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.5, 2)]}},
        }
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {"pair": SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20), **orderbook_payload},
                    {"pair": SimpleNamespace(id=2, pair_hash="pair-2", market_id_a=30, market_id_b=40), **orderbook_payload},
                ]
            )
        )
        calculator = SimpleNamespace(
            calculate_opportunities=Mock(
                return_value=[
                    {
                        "direction": "A_yes_B_no",
                        "avg_price_leg_1": 0.40,
                        "avg_price_leg_2": 0.50,
                        "shares": 10.0,
                        "capital_required": 9.0,
                        "gross_profit": 1.0,
                        "net_profit": 2.0,
                        "gross_roi": 0.11,
                        "net_roi": 0.22,
                    }
                ]
            )
        )

        async def process_opportunity(pair, calc_result):
            return SimpleNamespace(id=100 + pair.id, fanout_status="queued")


        alert_manager = SimpleNamespace(
            process_opportunity=AsyncMock(side_effect=process_opportunity),
            finalize_opportunity=AsyncMock(),
        )
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(return_value=[]),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[
            SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
            SimpleNamespace(id=2, pair_hash="pair-2", market_id_a=30, market_id_b=40),
        ])), patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={
                10: SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                20: SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
                30: SimpleNamespace(id=30, platform="polymarket", platform_market_id="poly-30"),
                40: SimpleNamespace(id=40, platform="predict_fun", platform_market_id="pf-40"),
            }),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker._persist_delivery_alert",
            new=AsyncMock(),
        ) as send_mock:
            result = await _process_candidates(
                fake_db,
                orderbook_service,
                calculator,
                alert_manager,
                fanout_manager,
                self.state,
            )

        self.assertEqual(result["opportunities"], 2)
        self.assertEqual(result["deliverable_opportunities"], 0)
        self.assertEqual(alert_manager.process_opportunity.await_count, 2)
        self.assertEqual(alert_manager.finalize_opportunity.await_count, 2)
        self.assertEqual(fanout_manager.create_alert_deliveries.await_count, 2)
        send_mock.assert_not_awaited()


    async def test_process_candidates_passes_prepared_delivery_opportunity_to_immediate_send(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            commit = AsyncMock()
            rollback = AsyncMock()


            async def execute(self, stmt):
                return FakeScalarResult([SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20)])


        fake_db = FakeDb()
        pair = SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20)
        prepared_delivery_opportunity = SimpleNamespace(direction="A_yes_B_no", capital_required=4.65, shares=5.0)
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {
                        "pair": pair,
                        "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.5, 2)]}},
                    }
                ]
            ),
        )
        calculator = SimpleNamespace(
            calculate_opportunities=Mock(
                return_value=[
                    {
                        "direction": "A_yes_B_no",
                        "avg_price_leg_1": 0.40,
                        "avg_price_leg_2": 0.50,
                        "shares": 10.0,
                        "capital_required": 9.0,
                        "gross_profit": 1.0,
                        "net_profit": 2.0,
                        "gross_roi": 0.11,
                        "net_roi": 0.22,
                    }
                ]
            )
        )

        async def process_opportunity(_pair, _calc_result):
            return SimpleNamespace(id=101, direction="A_yes_B_no", fanout_status="queued")


        alert_manager = SimpleNamespace(
            process_opportunity=AsyncMock(side_effect=process_opportunity),
            finalize_opportunity=AsyncMock(),
        )
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(
                return_value=[
                    {
                        "alert": SimpleNamespace(id=501),
                        "preferences": {},
                        "opportunity": prepared_delivery_opportunity,
                    }
                ]
            ),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch.object(worker_module.settings, "APP_RUNTIME_MODE", "worker"), patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[pair])), patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={
                10: SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                20: SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
            }),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(return_value=True),
        ), patch(
            "arbitrage_bot.worker._persist_delivery_alert",
            new=AsyncMock(),
        ) as send_mock:
            await _process_candidates(
                fake_db,
                orderbook_service,
                calculator,
                alert_manager,
                fanout_manager,
                self.state,
            )

        self.assertEqual(send_mock.await_count, 1)


    async def test_process_candidates_uses_batched_orderbook_fetch(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            async def execute(self, stmt):
                return FakeScalarResult([SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20)])


        fake_db = FakeDb()
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {
                        "pair": SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
                        "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.5, 2)]}},
                    }
                ]
            ),
        )
        calculator = SimpleNamespace(calculate_opportunities=Mock(return_value=[]))
        alert_manager = SimpleNamespace(
            process_opportunity=AsyncMock(),
            finalize_opportunity=AsyncMock(),
        )
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(return_value=[]),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[
            SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
        ])), patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={
                10: SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                20: SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
            }),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ):
            await _process_candidates(fake_db, orderbook_service, calculator, alert_manager, fanout_manager, self.state)

        orderbook_service.fetch_orderbooks_for_pairs.assert_awaited_once()