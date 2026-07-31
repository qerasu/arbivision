import asyncio
import contextlib
import time
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from unittest.mock import Mock
from unittest.mock import patch

from arbitrage_bot.core.observability import reset_counters
from arbitrage_bot.core.observability import snapshot_counters
from arbitrage_bot.services.matcher import MatcherService
from arbitrage_bot import worker as worker_module
from arbitrage_bot.worker import AlertRetryQueue, WorkerState, _build_cached_market_signatures, _build_candidate_index_from_signatures, _candidate_markets_for_signature, _cleanup_database_records, _filter_skippable_pairs, _load_candidate_context, _mark_db_cleanup_completed, _mark_stale_pairs, _process_candidates, _prune_market_signature_cache, _reconcile_market_pairs, _run_cycle, _run_market_sync_cycle, _send_delivery_alerts, _should_run_db_cleanup, _update_empty_counts, _upsert_market_pairs


def _fake_session_context(fake_db):
    @contextlib.asynccontextmanager
    async def _session_ctx():
        yield fake_db
    return _session_ctx

class WorkerPairLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.state = WorkerState()


    def test_load_active_markets_batches_large_id_sets(self):
        class FakeResult:
            def scalars(self):
                return self


            def all(self):
                return []

        statements = []

        async def execute(stmt):
            statements.append(stmt)
            return FakeResult()

        db = SimpleNamespace(execute=execute)

        result = asyncio.run(
            worker_module._load_active_markets(
                db,
                "polymarket",
                range(10001),
            )
        )

        self.assertEqual(result, [])
        self.assertEqual(len(statements), 2)
        batch_lengths = sorted(
            len(value)
            for stmt in statements
            for value in stmt.compile().params.values()
            if isinstance(value, list)
        )
        self.assertEqual(batch_lengths, [1, 10000])


    def test_market_sync_invalidates_candidate_cache_after_pair_upsert(self):
        self.state.candidate_context_loaded = True
        ingestion = SimpleNamespace(sync_markets=AsyncMock(return_value={}))

        async def upsert_pairs(*_args, **_kwargs):
            self.assertTrue(self.state.candidate_context_loaded)
            return set(), True

        with (
            patch("arbitrage_bot.worker._upsert_market_pairs", side_effect=upsert_pairs),
            patch("arbitrage_bot.worker._run_database_cleanup_if_due", new=AsyncMock()),
            patch("arbitrage_bot.worker._persist_full_pair_rematch_completion", new=AsyncMock()),
        ):
            asyncio.run(_run_market_sync_cycle(AsyncMock(), self.state, ingestion, MatcherService()))

        self.assertFalse(self.state.candidate_context_loaded)


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

        new_pairs, has_updates, hot_pair_hashes = _reconcile_market_pairs(
            [existing_pair],
            {"pair-1": matched_pair},
        )

        self.assertEqual(new_pairs, [])
        self.assertTrue(has_updates)
        self.assertEqual(hot_pair_hashes, {"pair-1"})
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

        new_pairs, has_updates, hot_pair_hashes = _reconcile_market_pairs([existing_pair], {})

        self.assertEqual(new_pairs, [])
        self.assertTrue(has_updates)
        self.assertEqual(hot_pair_hashes, set())
        self.assertEqual(existing_pair.status, "stale")


    def test_reconcile_reactivates_matching_stale_pair(self):
        existing_pair = SimpleNamespace(
            pair_hash="pair-1",
            status="stale",
            match_score=0.88,
            match_reason_json={"title": "old"},
            outcome_mapping_json={"market_a": {"yes": "old-y", "no": "old-n"}},
        )
        matched_pair = SimpleNamespace(
            pair_hash="pair-1",
            status="auto_approved",
            match_score=0.91,
            match_reason_json={"title": "new"},
            outcome_mapping_json={"market_a": {"yes": "new-y", "no": "new-n"}},
        )

        new_pairs, has_updates, hot_pair_hashes = _reconcile_market_pairs(
            [existing_pair],
            {"pair-1": matched_pair},
        )

        self.assertEqual(new_pairs, [])
        self.assertTrue(has_updates)
        self.assertEqual(hot_pair_hashes, {"pair-1"})
        self.assertEqual(existing_pair.status, "auto_approved")


    def test_reconcile_creates_new_pairs(self):
        matched_pair = SimpleNamespace(
            pair_hash="pair-2",
            status="auto_approved",
            match_score=0.91,
            match_reason_json={"title": "new"},
            outcome_mapping_json={"market_a": {"yes": "poly-y", "no": "poly-n"}},
        )

        new_pairs, has_updates, hot_pair_hashes = _reconcile_market_pairs([], {"pair-2": matched_pair})

        self.assertEqual(new_pairs, [matched_pair])
        self.assertFalse(has_updates)
        self.assertEqual(hot_pair_hashes, {"pair-2"})


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


    def test_select_active_pairs_for_cycle_prioritizes_hot_pairs_before_rotation(self):
        pair_a = SimpleNamespace(id=1, pair_hash="pair-a", market_id_a=10, market_id_b=20)
        pair_b = SimpleNamespace(id=2, pair_hash="pair-b", market_id_a=30, market_id_b=40)
        pair_c = SimpleNamespace(id=3, pair_hash="pair-c", market_id_a=50, market_id_b=60)
        market_map = {
            10: SimpleNamespace(raw_payload_json={}),
            20: SimpleNamespace(raw_payload_json={}),
            30: SimpleNamespace(raw_payload_json={}),
            40: SimpleNamespace(raw_payload_json={}),
            50: SimpleNamespace(raw_payload_json={}),
            60: SimpleNamespace(raw_payload_json={}),
        }
        worker_module._queue_hot_pairs(self.state, {"pair-c"})

        with patch.object(worker_module.settings, "MAX_ACTIVE_PAIRS_PER_CYCLE", 2):
            selected = worker_module._select_active_pairs_for_cycle(
                [pair_a, pair_b, pair_c],
                market_map,
                self.state,
            )

        self.assertEqual([pair.pair_hash for pair in selected], ["pair-c", "pair-a"])


    def test_mark_hot_pairs_processed_removes_selected_hashes(self):
        self.state.hot_pair_hashes = ["pair-a", "pair-b", "pair-c"]

        worker_module._mark_hot_pairs_processed(
            self.state,
            [
                SimpleNamespace(pair_hash="pair-a"),
                SimpleNamespace(pair_hash="pair-c"),
            ],
        )

        self.assertEqual(self.state.hot_pair_hashes, ["pair-b"])


    def test_mark_stale_pairs_changes_only_active_statuses(self):
        stale_pair = SimpleNamespace(status="stale", pair_hash="h-stale")
        approved_pair = SimpleNamespace(status="approved", pair_hash="h-approved")
        failed_pair = SimpleNamespace(status="failed", pair_hash="h-failed")

        changed = _mark_stale_pairs([stale_pair, approved_pair, failed_pair])

        self.assertTrue(changed)
        self.assertEqual(stale_pair.status, "stale")
        self.assertEqual(approved_pair.status, "stale")
        self.assertEqual(failed_pair.status, "failed")


    def test_should_run_db_cleanup_on_first_cycle(self):
        self.assertTrue(_should_run_db_cleanup(10.0, self.state))


    def test_should_run_db_cleanup_after_interval_elapsed(self):
        _mark_db_cleanup_completed(self.state, now=10.0)

        with patch.object(worker_module.settings, "DB_CLEANUP_INTERVAL_SECONDS", 300.0):
            self.assertFalse(_should_run_db_cleanup(200.0, self.state))
            self.assertTrue(_should_run_db_cleanup(310.0, self.state))


    def test_candidate_markets_for_signature_limits_ranked_candidates(self):
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

        candidates = _candidate_markets_for_signature(poly_signature, matcher, pf_index)

        self.assertEqual(len(candidates), 2)


    def test_candidate_markets_for_signature_uses_coarse_ranking_signals(self):
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

        candidates = _candidate_markets_for_signature(poly_signature, matcher, pf_index)

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
            "arbitrage_bot.worker._load_active_markets",
            new=AsyncMock(side_effect=[pf_markets, poly_markets[:1]]),
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


    def test_full_rematch_discards_polymarket_batches_from_signature_cache(self):
        poly_markets = [
            SimpleNamespace(id=market_id, title=f"poly {market_id}", category="sports", outcomes_json=[], raw_payload_json={}, status="active", updated_at="v1")
            for market_id in (1, 2)
        ]
        pf_market = SimpleNamespace(id=10, title="pf", category="sports", outcomes_json=[], raw_payload_json={}, status="active", updated_at="v1")
        matcher = Mock()
        matcher.max_ranked_candidates = 25
        matcher.build_market_signature.side_effect = lambda market: {
            "market": market,
            "tokens": {"shared"},
            "condition_ids": [],
        }
        matcher.candidate_rank_score.return_value = 1.0
        matcher.match_candidates.side_effect = lambda poly, pf, **kwargs: SimpleNamespace(
            pair_hash=f"{poly.id}-{pf.id}",
            status="auto_approved",
            match_score=0.9,
            match_reason_json={"ok": True},
            outcome_mapping_json={"market_a": {}},
        )
        fake_db = SimpleNamespace(
            add_all=Mock(),
            flush=AsyncMock(),
            commit=AsyncMock(),
            rollback=AsyncMock(),
        )
        stale_pair = SimpleNamespace(
            pair_hash="1-10",
            status="stale",
            match_score=0.8,
            match_reason_json={"old": True},
            outcome_mapping_json={"market_a": {}},
        )

        async def poly_batches():
            for market in poly_markets:
                yield [market]

        with patch(
            "arbitrage_bot.worker._load_active_markets",
            new=AsyncMock(return_value=[pf_market]),
        ), patch(
            "arbitrage_bot.worker._iter_active_market_batches",
            return_value=poly_batches(),
        ), patch(
            "arbitrage_bot.worker._load_existing_pairs",
            new=AsyncMock(return_value=[stale_pair]),
        ):
            asyncio.run(_upsert_market_pairs(fake_db, matcher, None, self.state))

        self.assertEqual(set(self.state.market_signature_cache), {pf_market.id})
        self.assertEqual(stale_pair.status, "auto_approved")
        self.assertEqual(len(fake_db.add_all.call_args.args[0]), 1)


    def test_upsert_market_pairs_keeps_unvisited_pairs_active_when_limit_is_hit(self):
        poly_market = SimpleNamespace(
            id=1,
            title="poly",
            category="sports",
            outcomes_json=[],
            raw_payload_json={},
            status="active",
            updated_at="v1",
        )
        pf_markets = [
            SimpleNamespace(
                id=market_id,
                title=f"pf {market_id}",
                category="sports",
                outcomes_json=[],
                raw_payload_json={},
                status="active",
                updated_at="v1",
            )
            for market_id in (10, 11)
        ]
        existing_pairs = [
            SimpleNamespace(
                pair_hash=f"1-{market.id}",
                status="auto_approved",
                match_score=0.9,
                match_reason_json={"ok": True},
                outcome_mapping_json={"market_a": {}},
            )
            for market in pf_markets
        ]
        matcher = Mock()
        matcher.max_ranked_candidates = 25
        matcher.build_market_signature.side_effect = lambda market: {
            "market": market,
            "tokens": {"shared"},
            "condition_ids": [],
        }
        matcher.candidate_rank_score.return_value = 1.0
        matcher.match_candidates.side_effect = lambda poly, pf, **kwargs: SimpleNamespace(
            pair_hash=f"{poly.id}-{pf.id}",
            status="auto_approved",
            match_score=0.9,
            match_reason_json={"ok": True},
            outcome_mapping_json={"market_a": {}},
        )

        with patch.object(worker_module.settings, "MAX_MARKET_PAIRS_PER_LOOP", 1), patch(
            "arbitrage_bot.worker._load_active_markets",
            new=AsyncMock(side_effect=[pf_markets, [poly_market]]),
        ), patch(
            "arbitrage_bot.worker._load_pairs_for_market_ids",
            new=AsyncMock(return_value=existing_pairs),
        ):
            asyncio.run(
                _upsert_market_pairs(
                    Mock(),
                    matcher,
                    {
                        "polymarket": {1},
                        "predict_fun": set(),
                    },
                    self.state,
                )
            )

        self.assertEqual([pair.status for pair in existing_pairs], ["auto_approved", "auto_approved"])


class WorkerDatabaseCleanupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.state = WorkerState()


    async def test_cleanup_database_records_deletes_old_stale_pairs_and_unused_closed_markets(self):
        class FakeRowResult:
            def __init__(self, rows):
                self.rows = rows


            def all(self):
                return list(self.rows)


        class FakeDb:
            def __init__(self):
                self.commit_calls = 0
                self.statements = []


            async def execute(self, stmt):
                compiled = str(stmt)
                self.statements.append(compiled)
                if compiled.startswith("SELECT market_pairs.id, market_pairs.pair_hash"):
                    return FakeRowResult([(11, "pair-old")])
                if "SELECT market_pairs.market_id_a" in compiled and "UNION" in compiled:
                    return FakeRowResult([(5,)])
                if compiled.startswith("SELECT markets.id"):
                    return FakeRowResult([(7,)])
                if compiled.startswith("DELETE FROM market_pairs"):
                    return FakeRowResult([])
                if compiled.startswith("DELETE FROM markets"):
                    return FakeRowResult([])
                raise AssertionError(f"unexpected stmt: {compiled}")


            async def commit(self):
                self.commit_calls += 1


        fake_db = FakeDb()

        with patch.object(worker_module.settings, "DB_CLEANUP_INTERVAL_SECONDS", 10800.0), patch.object(
            worker_module.settings,
            "DB_CLEANUP_RETENTION_SECONDS",
            10800.0,
        ), patch(
            "arbitrage_bot.worker._clear_empty_count",
            new=AsyncMock(),
        ) as clear_empty_count_mock:
            deleted_pairs, deleted_markets = await _cleanup_database_records(fake_db, self.state)

        self.assertEqual(deleted_pairs, 1)
        self.assertEqual(deleted_markets, 1)
        self.assertEqual(fake_db.commit_calls, 1)
        clear_empty_count_mock.assert_awaited_once_with("pair-old", self.state)
        self.assertTrue(any(stmt.startswith("DELETE FROM market_pairs") for stmt in fake_db.statements))
        self.assertTrue(any(stmt.startswith("DELETE FROM markets") for stmt in fake_db.statements))


    async def test_cleanup_database_records_skips_commit_when_nothing_to_delete(self):
        class FakeRowResult:
            def __init__(self, rows):
                self.rows = rows


            def all(self):
                return list(self.rows)


        class FakeDb:
            def __init__(self):
                self.commit_calls = 0


            async def execute(self, stmt):
                compiled = str(stmt)
                if compiled.startswith("SELECT market_pairs.id, market_pairs.pair_hash"):
                    return FakeRowResult([])
                if "SELECT market_pairs.market_id_a" in compiled and "UNION" in compiled:
                    return FakeRowResult([])
                if compiled.startswith("SELECT markets.id"):
                    return FakeRowResult([])
                raise AssertionError(f"unexpected stmt: {compiled}")


            async def commit(self):
                self.commit_calls += 1


        fake_db = FakeDb()

        with patch(
            "arbitrage_bot.worker._clear_empty_count",
            new=AsyncMock(),
        ) as clear_empty_count_mock:
            deleted_pairs, deleted_markets = await _cleanup_database_records(fake_db, self.state)

        self.assertEqual(deleted_pairs, 0)
        self.assertEqual(deleted_markets, 0)
        self.assertEqual(fake_db.commit_calls, 0)
        clear_empty_count_mock.assert_not_awaited()


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


    async def test_recent_persisted_rematch_defers_startup_rematch(self):
        setting = SimpleNamespace(value_json={"completed_at": 100.0})
        result = SimpleNamespace(scalars=Mock(return_value=SimpleNamespace(first=Mock(return_value=setting))))
        db = SimpleNamespace(execute=AsyncMock(return_value=result))

        with patch("arbitrage_bot.worker.time.time", return_value=105.0), patch(
            "arbitrage_bot.worker.time.monotonic", return_value=205.0,
        ), patch.object(worker_module.settings, "MATCHER_FULL_REMATCH_INTERVAL_SECONDS", 10.0):
            resumed = await worker_module._restore_full_pair_rematch_schedule(
                db,
                self.state,
            )

        self.assertTrue(resumed)
        self.assertEqual(self.state.last_full_pair_rematch_completed_at, 200.0)


    async def test_stale_persisted_rematch_keeps_startup_rematch_due(self):
        setting = SimpleNamespace(value_json={"completed_at": 90.0})
        result = SimpleNamespace(scalars=Mock(return_value=SimpleNamespace(first=Mock(return_value=setting))))
        db = SimpleNamespace(execute=AsyncMock(return_value=result))

        with patch("arbitrage_bot.worker.time.time", return_value=105.0), patch.object(
            worker_module.settings, "MATCHER_FULL_REMATCH_INTERVAL_SECONDS", 10.0,
        ):
            resumed = await worker_module._restore_full_pair_rematch_schedule(db, self.state)

        self.assertFalse(resumed)
        self.assertIsNone(self.state.last_full_pair_rematch_completed_at)


    async def test_future_persisted_rematch_keeps_startup_rematch_due(self):
        setting = SimpleNamespace(value_json={"completed_at": 106.0})
        result = SimpleNamespace(scalars=Mock(return_value=SimpleNamespace(first=Mock(return_value=setting))))
        db = SimpleNamespace(execute=AsyncMock(return_value=result))

        with patch("arbitrage_bot.worker.time.time", return_value=105.0):
            resumed = await worker_module._restore_full_pair_rematch_schedule(db, self.state)

        self.assertFalse(resumed)
        self.assertIsNone(self.state.last_full_pair_rematch_completed_at)


    async def test_invalid_persisted_rematch_keeps_startup_rematch_due(self):
        setting = SimpleNamespace(value_json={"completed_at": "nan"})
        result = SimpleNamespace(scalars=Mock(return_value=SimpleNamespace(first=Mock(return_value=setting))))
        db = SimpleNamespace(execute=AsyncMock(return_value=result))

        with patch("arbitrage_bot.worker.time.time", return_value=105.0):
            resumed = await worker_module._restore_full_pair_rematch_schedule(db, self.state)

        self.assertFalse(resumed)
        self.assertIsNone(self.state.last_full_pair_rematch_completed_at)


    async def test_persist_full_rematch_marks_state_only_after_commit(self):
        added = []
        db = SimpleNamespace(
            execute=AsyncMock(
                return_value=SimpleNamespace(
                    scalars=Mock(return_value=SimpleNamespace(first=Mock(return_value=None)))
                )
            ),
            add=added.append,
            commit=AsyncMock(),
        )

        with patch("arbitrage_bot.worker.time.time", return_value=100.0), patch(
            "arbitrage_bot.worker.time.monotonic", return_value=200.0,
        ):
            await worker_module._persist_full_pair_rematch_completion(db, self.state)

        db.commit.assert_awaited_once()
        self.assertEqual(added[0].key, "worker:full_pair_rematch")
        self.assertEqual(added[0].value_json, {"completed_at": 100.0})
        self.assertEqual(self.state.last_full_pair_rematch_completed_at, 200.0)


    async def test_failed_marker_commit_keeps_full_rematch_due(self):
        db = SimpleNamespace(
            execute=AsyncMock(
                return_value=SimpleNamespace(
                    scalars=Mock(return_value=SimpleNamespace(first=Mock(return_value=None)))
                )
            ),
            add=Mock(),
            commit=AsyncMock(side_effect=RuntimeError("database unavailable")),
        )

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await worker_module._persist_full_pair_rematch_completion(db, self.state)

        self.assertIsNone(self.state.last_full_pair_rematch_completed_at)


    async def test_limited_full_rematch_is_not_persisted(self):
        ingestion = SimpleNamespace(sync_markets=AsyncMock(return_value={}))
        persist = AsyncMock()

        with patch(
            "arbitrage_bot.worker._upsert_market_pairs",
            new=AsyncMock(return_value=(set(), False)),
        ), patch(
            "arbitrage_bot.worker._persist_full_pair_rematch_completion",
            new=persist,
        ), patch(
            "arbitrage_bot.worker._run_database_cleanup_if_due",
            new=AsyncMock(),
        ):
            await worker_module._run_market_sync_cycle(
                SimpleNamespace(),
                self.state,
                ingestion,
                SimpleNamespace(),
            )

        persist.assert_not_awaited()
        self.assertIsNotNone(self.state.last_full_pair_rematch_completed_at)


    async def test_run_sync_loop_checks_candidates_while_market_sync_is_running(self):
        sync_started = asyncio.Event()
        candidate_started = asyncio.Event()
        wait_forever = asyncio.Event()
        fake_db = SimpleNamespace()
        ingestion = SimpleNamespace(close=AsyncMock())
        orderbook_service = SimpleNamespace(close=AsyncMock())
        retry_queue = SimpleNamespace(run=AsyncMock(side_effect=wait_forever.wait))

        async def run_market_sync(*args):
            sync_started.set()
            await wait_forever.wait()

        async def run_candidates(*args):
            await sync_started.wait()
            candidate_started.set()
            raise asyncio.CancelledError

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
            "arbitrage_bot.worker.AlertRetryQueue",
            return_value=retry_queue,
        ), patch(
            "arbitrage_bot.worker.AsyncSessionLocal",
            new=_fake_session_context(fake_db),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker._run_market_sync_loop",
            new=run_market_sync,
        ), patch(
            "arbitrage_bot.worker._run_candidate_cycle",
            new=run_candidates,
        ):
            with self.assertRaises(asyncio.CancelledError):
                await worker_module.run_sync_loop(self.state)

        self.assertTrue(candidate_started.is_set())
        ingestion.close.assert_awaited_once()
        orderbook_service.close.assert_awaited_once()


    async def test_run_sync_loop_backs_off_after_cycle_failure(self):
        wait_forever = asyncio.Event()
        ingestion = SimpleNamespace(close=AsyncMock())
        orderbook_service = SimpleNamespace(
            close=AsyncMock(),
            wait_for_updates=AsyncMock(),
        )
        retry_queue = SimpleNamespace(run=AsyncMock(side_effect=wait_forever.wait))

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
            "arbitrage_bot.worker.AlertRetryQueue",
            return_value=retry_queue,
        ), patch(
            "arbitrage_bot.worker.AsyncSessionLocal",
            new=_fake_session_context(SimpleNamespace()),
        ), patch(
            "arbitrage_bot.worker._run_market_sync_loop",
            new=AsyncMock(side_effect=wait_forever.wait),
        ), patch(
            "arbitrage_bot.worker._run_candidate_cycle",
            new=AsyncMock(side_effect=RuntimeError("database unavailable")),
        ), patch(
            "arbitrage_bot.worker.asyncio.sleep",
            new=AsyncMock(side_effect=asyncio.CancelledError),
        ) as sleep_mock:
            with self.assertRaises(asyncio.CancelledError):
                await worker_module.run_sync_loop(self.state)

        sleep_mock.assert_awaited_once_with(worker_module.settings.MARKET_REFRESH_SECONDS)
        orderbook_service.wait_for_updates.assert_not_awaited()


    async def test_run_sync_loop_limits_successful_cycle_frequency(self):
        wait_forever = asyncio.Event()
        ingestion = SimpleNamespace(close=AsyncMock())
        orderbook_service = SimpleNamespace(
            close=AsyncMock(),
            wait_for_updates=AsyncMock(return_value=True),
        )
        retry_queue = SimpleNamespace(run=AsyncMock(side_effect=wait_forever.wait))

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
            "arbitrage_bot.worker.AlertRetryQueue",
            return_value=retry_queue,
        ), patch(
            "arbitrage_bot.worker.AsyncSessionLocal",
            new=_fake_session_context(SimpleNamespace()),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=SimpleNamespace(),
        ), patch(
            "arbitrage_bot.worker._run_market_sync_loop",
            new=AsyncMock(side_effect=wait_forever.wait),
        ), patch(
            "arbitrage_bot.worker._run_candidate_cycle",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker.time",
            new=SimpleNamespace(monotonic=Mock(side_effect=[100.0, 101.0])),
        ), patch(
            "arbitrage_bot.worker.asyncio.sleep",
            new=AsyncMock(side_effect=asyncio.CancelledError),
        ) as sleep_mock:
            with self.assertRaises(asyncio.CancelledError):
                await worker_module.run_sync_loop(self.state)

        orderbook_service.wait_for_updates.assert_awaited_once_with(
            worker_module.settings.MARKET_REFRESH_SECONDS
        )
        sleep_mock.assert_awaited_once_with(
            worker_module.settings.MARKET_REFRESH_SECONDS - 1.0
        )
        ingestion.close.assert_awaited_once()
        orderbook_service.close.assert_awaited_once()


    async def test_run_cycle_skips_pair_rebuild_when_market_sync_was_not_needed(self):
        fake_db = SimpleNamespace()
        events = []
        ingestion = SimpleNamespace(
            sync_markets=AsyncMock(side_effect=lambda: events.append("sync") or False)
        )
        matcher = SimpleNamespace()
        orderbook_service = SimpleNamespace()
        calculator = SimpleNamespace()
        alert_manager = SimpleNamespace()
        fanout_manager = SimpleNamespace()

        with patch(
            "arbitrage_bot.worker._upsert_market_pairs",
            new=AsyncMock(return_value=(set(), True)),
        ) as upsert_mock, patch(
            "arbitrage_bot.worker._process_candidates",
            new=AsyncMock(
                side_effect=lambda *args: events.append("process") or {
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
            await _run_cycle(fake_db, self.state, ingestion, matcher, orderbook_service, calculator, alert_manager, fanout_manager)

        upsert_mock.assert_not_awaited()
        process_mock.assert_awaited_once()
        self.assertEqual(events[:2], ["process", "sync"])


    async def test_run_cycle_performs_full_pair_rematch_even_without_market_changes(self):
        fake_db = SimpleNamespace()
        ingestion = SimpleNamespace(sync_markets=AsyncMock(return_value=False))
        matcher = SimpleNamespace()
        orderbook_service = SimpleNamespace()
        calculator = SimpleNamespace()
        alert_manager = SimpleNamespace()
        fanout_manager = SimpleNamespace()

        with patch(
            "arbitrage_bot.worker._upsert_market_pairs",
            new=AsyncMock(return_value=(set(), True)),
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
        ), patch(
            "arbitrage_bot.worker._persist_full_pair_rematch_completion",
            new=AsyncMock(),
        ):
            await _run_cycle(fake_db, self.state, ingestion, matcher, orderbook_service, calculator, alert_manager, fanout_manager)

        upsert_mock.assert_awaited_once()
        self.assertIsNone(upsert_mock.await_args.args[2])


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
        )
        matcher = SimpleNamespace()
        orderbook_service = SimpleNamespace()
        calculator = SimpleNamespace()
        alert_manager = SimpleNamespace()
        fanout_manager = SimpleNamespace()

        with patch(
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
            await _run_cycle(fake_db, self.state, ingestion, matcher, orderbook_service, calculator, alert_manager, fanout_manager)

        self.assertEqual(
            upsert_mock.await_args.args[2],
            {
                "polymarket": {11},
                "predict_fun": set(),
            },
        )


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
        )
        matcher = SimpleNamespace()
        orderbook_service = SimpleNamespace()
        calculator = SimpleNamespace()
        alert_manager = SimpleNamespace()
        fanout_manager = SimpleNamespace()

        with patch(
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
            await _run_cycle(fake_db, self.state, ingestion, matcher, orderbook_service, calculator, alert_manager, fanout_manager)

        upsert_mock.assert_not_awaited()


    async def test_run_cycle_runs_database_cleanup_when_due(self):
        fake_db = SimpleNamespace()
        ingestion = SimpleNamespace(sync_markets=AsyncMock(return_value=False))
        matcher = SimpleNamespace()
        orderbook_service = SimpleNamespace()
        calculator = SimpleNamespace()
        alert_manager = SimpleNamespace()
        fanout_manager = SimpleNamespace()

        with patch(
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
        ), patch(
            "arbitrage_bot.worker._cleanup_database_records",
            new=AsyncMock(return_value=(2, 3)),
        ) as cleanup_mock, patch(
            "arbitrage_bot.worker.send_system_error_notification",
            new=AsyncMock(return_value=False),
        ) as system_error_mock:
            await _run_cycle(fake_db, self.state, ingestion, matcher, orderbook_service, calculator, alert_manager, fanout_manager)

        cleanup_mock.assert_awaited_once_with(fake_db, self.state)
        system_error_mock.assert_not_awaited()
        self.assertIsNotNone(self.state.last_db_cleanup_completed_at)


    async def test_run_cycle_skips_database_cleanup_when_not_due(self):
        fake_db = SimpleNamespace()
        ingestion = SimpleNamespace(sync_markets=AsyncMock(return_value=False))
        matcher = SimpleNamespace()
        orderbook_service = SimpleNamespace()
        calculator = SimpleNamespace()
        alert_manager = SimpleNamespace()
        fanout_manager = SimpleNamespace()
        self.state.last_db_cleanup_completed_at = time.monotonic()

        with patch(
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
        ), patch.object(
            worker_module.settings,
            "DB_CLEANUP_INTERVAL_SECONDS",
            10800.0,
        ), patch(
            "arbitrage_bot.worker._cleanup_database_records",
            new=AsyncMock(return_value=(2, 3)),
        ) as cleanup_mock:
            await _run_cycle(fake_db, self.state, ingestion, matcher, orderbook_service, calculator, alert_manager, fanout_manager)

        cleanup_mock.assert_not_awaited()


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
            first = await _process_candidates(fake_db, orderbook_service, calculator, alert_manager, fanout_manager, self.state)
            second = await _process_candidates(fake_db, orderbook_service, calculator, alert_manager, fanout_manager, self.state)

        self.assertEqual(fake_db.execute_calls, 2)
        for result in (first, second):
            self.assertIn("setup_ms", result)
            self.assertEqual(result["orderbook_fetch_ms"], 0)
            self.assertEqual(result["pair_processing_ms"], 0)


    async def test_process_candidates_records_setup_timing_without_pairs(self):
        with patch(
            "arbitrage_bot.worker._load_candidate_context",
            new=AsyncMock(return_value=([], {})),
        ), patch(
            "arbitrage_bot.worker.time.monotonic",
            side_effect=[10.0, 10.5],
        ):
            result = await _process_candidates(
                SimpleNamespace(),
                SimpleNamespace(),
                SimpleNamespace(),
                SimpleNamespace(),
                SimpleNamespace(),
                self.state,
            )

        self.assertEqual(result["setup_ms"], 500)
        self.assertEqual(result["orderbook_fetch_ms"], 0)
        self.assertEqual(result["pair_processing_ms"], 0)


    async def test_filter_skippable_pairs_probes_one_quarantined_pair(self):
        fake_redis = FakeRedis()
        fake_redis.data["worker:pair-empty-count:pair-1"] = "3"
        pairs = [
            SimpleNamespace(pair_hash="pair-1"),
            SimpleNamespace(pair_hash="pair-2"),
        ]

        with patch(
            "arbitrage_bot.worker.get_redis",
            new=MagicMock(return_value=fake_redis),
        ):
            first_active_pairs = await _filter_skippable_pairs(pairs, self.state)
            second_active_pairs = await _filter_skippable_pairs(pairs, self.state)

        self.assertEqual(
            [pair.pair_hash for pair in first_active_pairs],
            ["pair-2", "pair-1"],
        )
        self.assertEqual(
            [pair.pair_hash for pair in second_active_pairs],
            ["pair-2"],
        )


    async def test_update_empty_counts_persists_to_redis(self):
        fake_redis = FakeRedis()
        checked_pairs = [
            SimpleNamespace(pair_hash="pair-1"),
            SimpleNamespace(pair_hash="pair-2"),
        ]

        with patch(
            "arbitrage_bot.worker.get_redis",
            new=MagicMock(return_value=fake_redis),
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
        ), patch(
            "arbitrage_bot.worker.AsyncSessionLocal",
            new=_fake_session_context(fake_db),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=alert_manager,
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=fanout_manager,
        ):
            result = await _process_candidates(fake_db, orderbook_service, calculator, alert_manager, fanout_manager, self.state)

        self.assertEqual(result["opportunities"], 0)
        self.assertGreaterEqual(result["setup_ms"], 0)
        self.assertGreaterEqual(result["orderbook_fetch_ms"], 0)
        self.assertGreaterEqual(result["pair_processing_ms"], 0)
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
        ) as send_mock, patch(
            "arbitrage_bot.worker.AsyncSessionLocal",
            new=_fake_session_context(fake_db),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=alert_manager,
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=fanout_manager,
        ):
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


    async def test_process_candidates_batches_ready_pairs_without_waiting_for_slower_pair(self):
        fast_pair_a = SimpleNamespace(id=1, pair_hash="fast-a", market_id_a=10, market_id_b=20)
        fast_pair_b = SimpleNamespace(id=2, pair_hash="fast-b", market_id_a=30, market_id_b=40)
        slow_pair = SimpleNamespace(id=3, pair_hash="slow", market_id_a=50, market_id_b=60)
        fast_pair_release = asyncio.Event()
        slow_pair_release = asyncio.Event()
        alert_sent = asyncio.Event()
        fast_pair_count = 0
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(return_value=[
                {"pair": fast_pair_a},
                {"pair": fast_pair_b},
                {"pair": slow_pair},
            ]),
        )
        fanout_manager = SimpleNamespace(
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        async def process_pair(*args):
            nonlocal fast_pair_count
            pair = args[3]
            if pair is slow_pair:
                await slow_pair_release.wait()
            else:
                fast_pair_count += 1
                if fast_pair_count == 2:
                    fast_pair_release.set()
                await fast_pair_release.wait()
            return {
                "pair_hash": pair.pair_hash,
                "has_orderbooks": True,
                "opportunities": 1,
                "deliverable_opportunities": 0 if pair is slow_pair else 1,
                "deliveries": [] if pair is slow_pair else [{}],
            }

        async def send_deliveries(*args, **kwargs):
            alert_sent.set()
            return True

        with patch.object(worker_module.settings, "APP_RUNTIME_MODE", "worker"), patch.object(
            worker_module,
            "_ALERT_BATCH_WINDOW_SECONDS",
            0.01,
        ), patch(
            "arbitrage_bot.worker._load_candidate_context",
            new=AsyncMock(return_value=([fast_pair_a, fast_pair_b, slow_pair], {})),
        ), patch(
            "arbitrage_bot.worker._filter_skippable_pairs",
            new=AsyncMock(return_value=[fast_pair_a, fast_pair_b, slow_pair]),
        ), patch(
            "arbitrage_bot.worker._process_candidate_pair",
            new=AsyncMock(side_effect=process_pair),
        ), patch(
            "arbitrage_bot.worker._send_all_deliveries",
            new=AsyncMock(side_effect=send_deliveries),
        ) as send_mock, patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ):
            task = asyncio.create_task(_process_candidates(
                SimpleNamespace(),
                orderbook_service,
                SimpleNamespace(),
                SimpleNamespace(),
                fanout_manager,
                self.state,
            ))
            await asyncio.wait_for(alert_sent.wait(), timeout=1)
            self.assertFalse(task.done())
            self.assertEqual(len(send_mock.await_args.args[0]), 2)
            slow_pair_release.set()
            await task

        send_mock.assert_awaited_once()


    async def test_process_candidates_keeps_other_pairs_when_one_fails(self):
        failed_pair = SimpleNamespace(id=1, pair_hash="failed", market_id_a=10, market_id_b=20)
        healthy_pair = SimpleNamespace(id=2, pair_hash="healthy", market_id_a=30, market_id_b=40)
        self.state.hot_pair_hashes = ["failed", "healthy"]
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(return_value=[
                {"pair": failed_pair},
                {"pair": healthy_pair},
            ]),
        )
        fanout_manager = SimpleNamespace(get_delivery_targets=AsyncMock(return_value=[]))

        async def process_pair(*args):
            pair = args[3]
            if pair is failed_pair:
                raise ValueError("bad pair")
            return {
                "pair_hash": pair.pair_hash,
                "has_orderbooks": True,
                "opportunities": 1,
                "deliverable_opportunities": 0,
                "deliveries": [],
            }

        with patch(
            "arbitrage_bot.worker._load_candidate_context",
            new=AsyncMock(return_value=([failed_pair, healthy_pair], {})),
        ), patch(
            "arbitrage_bot.worker._filter_skippable_pairs",
            new=AsyncMock(return_value=[failed_pair, healthy_pair]),
        ), patch(
            "arbitrage_bot.worker._process_candidate_pair",
            new=AsyncMock(side_effect=process_pair),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ) as update_empty_counts_mock:
            result = await _process_candidates(
                SimpleNamespace(),
                orderbook_service,
                SimpleNamespace(),
                SimpleNamespace(),
                fanout_manager,
                self.state,
            )

        self.assertEqual(result["active_pairs"], 2)
        self.assertEqual(result["pairs_with_books"], 1)
        self.assertEqual(result["opportunities"], 1)
        self.assertEqual(snapshot_counters()["worker.pair_failed"], 1)
        self.assertEqual(update_empty_counts_mock.await_args.args[1], {"failed", "healthy"})
        self.assertEqual(self.state.hot_pair_hashes, ["failed"])


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
        ) as send_mock, patch(
            "arbitrage_bot.worker.AsyncSessionLocal",
            new=_fake_session_context(fake_db),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=alert_manager,
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=fanout_manager,
        ):
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
        self.assertEqual(fake_db.commit_calls, 0)
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
        ) as send_mock, patch(
            "arbitrage_bot.worker.AsyncSessionLocal",
            new=_fake_session_context(fake_db),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=alert_manager,
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=fanout_manager,
        ):
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
        self.assertEqual(alert_manager.finalize_opportunity.await_count, 1)
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
        ) as send_mock, patch(
            "arbitrage_bot.worker.AsyncSessionLocal",
            new=_fake_session_context(fake_db),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=alert_manager,
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=fanout_manager,
        ):
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
        self.assertEqual(alert_manager.finalize_opportunity.await_count, 0)
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
        ) as send_mock, patch(
            "arbitrage_bot.worker.AsyncSessionLocal",
            new=_fake_session_context(fake_db),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=alert_manager,
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=fanout_manager,
        ):
            await _process_candidates(
                fake_db,
                orderbook_service,
                calculator,
                alert_manager,
                fanout_manager,
                self.state,
            )

        self.assertEqual(send_mock.await_count, 1)


    async def test_process_candidates_fetches_orderbooks_in_one_batch(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            async def execute(self, stmt):
                return FakeScalarResult(
                    [
                        SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
                        SimpleNamespace(id=2, pair_hash="pair-2", market_id_a=30, market_id_b=40),
                    ]
                )


        fake_db = FakeDb()

        async def fetch_orderbooks(pairs, *_args, **_kwargs):
            return [
                {
                    "pair": pair,
                    "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.5, 2)]}},
                }
                for pair in pairs
            ]


        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(side_effect=fetch_orderbooks),
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
            "arbitrage_bot.worker.AsyncSessionLocal",
            new=_fake_session_context(fake_db),
        ), patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=alert_manager,
        ), patch(
            "arbitrage_bot.worker.FanoutManager",
            return_value=fanout_manager,
        ):
            await _process_candidates(fake_db, orderbook_service, calculator, alert_manager, fanout_manager, self.state)

        orderbook_service.fetch_orderbooks_for_pairs.assert_awaited_once()
        self.assertEqual(
            [pair.pair_hash for pair in orderbook_service.fetch_orderbooks_for_pairs.await_args.args[0]],
            ["pair-1", "pair-2"],
        )


class AlertRetryQueueTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        reset_counters()


    def test_retry_queue_applies_backoff_and_size_limit(self):
        first_alert = SimpleNamespace(status="failed", attempt_count=2, next_retry_at=None)
        second_alert = SimpleNamespace(status="failed", attempt_count=1, next_retry_at=None)
        before_enqueue = datetime.now(timezone.utc)

        with patch.object(
            worker_module.settings,
            "TELEGRAM_ALERT_RETRY_BASE_DELAY_SECONDS",
            5,
        ), patch.object(
            worker_module.settings,
            "TELEGRAM_ALERT_RETRY_QUEUE_MAX_SIZE",
            1,
        ):
            retry_queue = AlertRetryQueue(SimpleNamespace())
            first_queued = retry_queue.enqueue({"delivery": {"alert": first_alert}})
            after_enqueue = datetime.now(timezone.utc)
            second_queued = retry_queue.enqueue({"delivery": {"alert": second_alert}})

        self.assertTrue(first_queued)
        self.assertFalse(second_queued)
        self.assertGreaterEqual(first_alert.next_retry_at, before_enqueue + timedelta(seconds=10))
        self.assertLessEqual(first_alert.next_retry_at, after_enqueue + timedelta(seconds=10))
        self.assertEqual(snapshot_counters().get("worker.retry_queue_full"), 1)


    async def test_failed_delivery_is_queued(self):
        alert = SimpleNamespace(status="failed", attempt_count=1, next_retry_at=None)
        delivery = {
            "alert": alert,
            "preferences": SimpleNamespace(),
            "opportunity": SimpleNamespace(),
        }
        retry_queue = MagicMock()

        with patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(return_value=False),
        ):
            sent_count = await _send_delivery_alerts(
                [delivery],
                SimpleNamespace(),
                SimpleNamespace(),
                SimpleNamespace(),
                SimpleNamespace(),
                {},
                SimpleNamespace(),
                retry_queue,
            )

        self.assertEqual(sent_count, 0)
        retry_queue.enqueue.assert_called_once()


    async def test_delivery_exception_is_queued(self):
        alert = SimpleNamespace(status="queued", attempt_count=0, next_retry_at=None)
        delivery = {
            "alert": alert,
            "preferences": SimpleNamespace(),
            "opportunity": SimpleNamespace(),
        }
        retry_queue = MagicMock()
        retry_queue.enqueue.return_value = True

        with patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(side_effect=RuntimeError("telegram unavailable")),
        ):
            sent_count = await _send_delivery_alerts(
                [delivery],
                SimpleNamespace(),
                SimpleNamespace(),
                SimpleNamespace(),
                SimpleNamespace(),
                {},
                SimpleNamespace(),
                retry_queue,
            )

        self.assertEqual(sent_count, 0)
        self.assertEqual(alert.status, "failed")
        self.assertEqual(alert.attempt_count, 1)
        retry_queue.enqueue.assert_called_once()


    def test_deliveries_completed_requires_at_least_one_sent_alert(self):
        self.assertFalse(
            worker_module._deliveries_completed(
                [
                    SimpleNamespace(status="cancelled"),
                    SimpleNamespace(status="suppressed"),
                ]
            )
        )
        self.assertTrue(
            worker_module._deliveries_completed(
                [
                    SimpleNamespace(status="sent"),
                    SimpleNamespace(status="suppressed"),
                ]
            )
        )


    async def test_successful_delivery_waits_for_failed_delivery_before_finalizing(self):
        opportunity = SimpleNamespace()
        deliveries = [
            {
                "alert": SimpleNamespace(status="queued", attempt_count=0, next_retry_at=None),
                "preferences": {},
                "opportunity": SimpleNamespace(),
            },
            {
                "alert": SimpleNamespace(status="queued", attempt_count=0, next_retry_at=None),
                "preferences": {},
                "opportunity": SimpleNamespace(),
            },
        ]
        pair_results = [{
            "deliveries": [{
                "deliveries": deliveries,
                "opportunity": opportunity,
                "pair": SimpleNamespace(),
                "market_a": SimpleNamespace(),
                "market_b": SimpleNamespace(),
                "directions": {},
            }],
        }]
        retry_queue = MagicMock()
        retry_queue.enqueue.side_effect = lambda item: setattr(
            item["delivery"]["alert"],
            "status",
            "retry_queued",
        ) or True
        alert_manager = SimpleNamespace(finalize_opportunity=AsyncMock())

        with patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(side_effect=[True, False]),
        ), patch("arbitrage_bot.worker.AlertManager", return_value=alert_manager):
            await worker_module._send_all_deliveries(
                pair_results,
                SimpleNamespace(),
                retry_queue,
            )

        alert_manager.finalize_opportunity.assert_not_awaited()
        retry_queue.enqueue.assert_called_once()

        retry_item = retry_queue.enqueue.call_args.args[0]
        with patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(return_value=True),
        ), patch("arbitrage_bot.worker.AlertManager", return_value=alert_manager):
            await worker_module._retry_alert_delivery(retry_item, SimpleNamespace())

        alert_manager.finalize_opportunity.assert_awaited_once_with(opportunity)


    async def test_digest_without_terminal_status_queues_each_delivery(self):
        opportunity = SimpleNamespace()
        deliveries = [
            {
                "alert": SimpleNamespace(
                    telegram_chat_id="1001",
                    status="queued",
                    attempt_count=0,
                    next_retry_at=None,
                ),
                "preferences": {},
                "opportunity": SimpleNamespace(),
            }
            for _ in range(2)
        ]
        pair_results = [{
            "deliveries": [{
                "deliveries": deliveries,
                "opportunity": opportunity,
                "pair": SimpleNamespace(),
                "market_a": SimpleNamespace(),
                "market_b": SimpleNamespace(),
                "directions": {},
            }],
        }]
        retry_queue = MagicMock()
        retry_queue.enqueue.side_effect = lambda item: setattr(
            item["delivery"]["alert"],
            "status",
            "retry_queued",
        ) or True
        alert_manager = SimpleNamespace(finalize_opportunity=AsyncMock())

        with patch(
            "arbitrage_bot.worker.send_alert_digest",
            new=AsyncMock(return_value=[]),
        ), patch("arbitrage_bot.worker.AlertManager", return_value=alert_manager):
            await worker_module._send_all_deliveries(
                pair_results,
                SimpleNamespace(),
                retry_queue,
            )

        self.assertEqual(retry_queue.enqueue.call_count, 2)
        self.assertTrue(all(delivery["alert"].status == "retry_queued" for delivery in deliveries))
        alert_manager.finalize_opportunity.assert_not_awaited()


    async def test_multiple_opportunities_for_same_chat_use_digest(self):
        opportunities = [SimpleNamespace(name="one"), SimpleNamespace(name="two")]
        pair_results = [{
            "deliveries": [{
                "deliveries": [{
                    "alert": SimpleNamespace(telegram_chat_id="1001"),
                    "preferences": {},
                    "opportunity": opportunity,
                }],
                "opportunity": opportunity,
                "pair": SimpleNamespace(),
                "market_a": SimpleNamespace(),
                "market_b": SimpleNamespace(),
                "directions": {},
            }],
        } for opportunity in opportunities]
        alert_manager = SimpleNamespace(finalize_opportunity=AsyncMock())

        async def send_digest(items, _calculator):
            for item in items:
                item["delivery"]["alert"].status = "sent"
            return opportunities

        with patch(
            "arbitrage_bot.worker.send_alert_digest",
            new=AsyncMock(side_effect=send_digest),
        ) as digest_mock, patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(),
        ) as immediate_mock, patch(
            "arbitrage_bot.worker.AlertManager",
            return_value=alert_manager,
        ):
            had_deliveries = await worker_module._send_all_deliveries(
                pair_results,
                SimpleNamespace(),
            )

        self.assertTrue(had_deliveries)
        digest_mock.assert_awaited_once()
        immediate_mock.assert_not_awaited()
        self.assertEqual(alert_manager.finalize_opportunity.await_count, 2)


    async def test_delivery_concurrency_is_applied_to_recipients(self):
        active = 0
        peak = 0

        async def send(*_args, **_kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            return True

        deliveries = [
            {
                "alert": SimpleNamespace(status="queued"),
                "preferences": {},
                "opportunity": SimpleNamespace(),
            }
            for _ in range(2)
        ]
        with patch("arbitrage_bot.worker.send_alert_immediately", new=send):
            sent_count = await _send_delivery_alerts(
                deliveries,
                SimpleNamespace(),
                SimpleNamespace(),
                SimpleNamespace(),
                SimpleNamespace(),
                {},
                SimpleNamespace(),
                send_semaphore=asyncio.Semaphore(2),
            )

        self.assertEqual(sent_count, 2)
        self.assertEqual(peak, 2)


    async def test_successful_retry_finalizes_opportunity(self):
        opportunity = SimpleNamespace()
        item = {
            "delivery": {
                "alert": SimpleNamespace(),
                "preferences": {},
                "opportunity": SimpleNamespace(),
            },
            "opportunity": opportunity,
            "pair": SimpleNamespace(),
            "market_a": SimpleNamespace(),
            "market_b": SimpleNamespace(),
            "directions": {},
        }
        alert_manager = SimpleNamespace(finalize_opportunity=AsyncMock())

        with patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(return_value=True),
        ), patch("arbitrage_bot.worker.AlertManager", return_value=alert_manager):
            sent = await worker_module._retry_alert_delivery(item, SimpleNamespace())

        self.assertTrue(sent)
        alert_manager.finalize_opportunity.assert_awaited_once_with(opportunity)


    async def test_retry_queue_stops_after_max_attempts(self):
        alert = SimpleNamespace(status="failed", attempt_count=1, next_retry_at=None)
        item = {"delivery": {"alert": alert}}
        exhausted = asyncio.Event()

        async def fail_delivery(_item, _calculator):
            alert.attempt_count += 1
            alert.status = "failed"
            if alert.attempt_count == 3:
                exhausted.set()
            return False

        with patch.object(worker_module.settings, "TELEGRAM_ALERT_RETRY_MAX_ATTEMPTS", 3), patch.object(
            worker_module.settings,
            "TELEGRAM_ALERT_RETRY_BASE_DELAY_SECONDS",
            0,
        ), patch.object(
            worker_module,
            "_retry_alert_delivery",
            new=fail_delivery,
        ):
            retry_queue = AlertRetryQueue(SimpleNamespace())
            retry_queue.enqueue(item)
            retry_task = asyncio.create_task(retry_queue.run())
            try:
                await asyncio.wait_for(exhausted.wait(), timeout=1)
                await asyncio.sleep(0)
            finally:
                retry_task.cancel()
                await asyncio.gather(retry_task, return_exceptions=True)

        self.assertEqual(alert.attempt_count, 3)
        self.assertIsNone(alert.next_retry_at)
        self.assertEqual(snapshot_counters().get("worker.retry_exhausted"), 1)


    async def test_retry_queue_task_can_be_cancelled_during_backoff(self):
        alert = SimpleNamespace(status="failed", attempt_count=1, next_retry_at=None)
        item = {"delivery": {"alert": alert}}

        with patch.object(
            worker_module.settings,
            "TELEGRAM_ALERT_RETRY_BASE_DELAY_SECONDS",
            60,
        ):
            retry_queue = AlertRetryQueue(SimpleNamespace())
            retry_queue.enqueue(item)
            retry_task = asyncio.create_task(retry_queue.run())
            await asyncio.sleep(0)
            retry_task.cancel()

            with self.assertRaises(asyncio.CancelledError):
                await retry_task
