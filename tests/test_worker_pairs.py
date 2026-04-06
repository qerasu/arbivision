import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

from arbitrage_bot.services.matcher import MatcherService
from arbitrage_bot import worker as worker_module
from arbitrage_bot.worker import _build_cached_market_signatures, _build_candidate_index_from_signatures, _candidate_markets_for_poly, _filter_skippable_pairs, _mark_stale_pairs, _prune_market_signature_cache, _reconcile_market_pairs, _update_empty_counts


class WorkerPairLifecycleTests(unittest.TestCase):
    def setUp(self):
        worker_module._market_signature_cache.clear()


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

        first = _build_cached_market_signatures([market], matcher)
        second = _build_cached_market_signatures([market], matcher)

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

        _build_cached_market_signatures([market], matcher)
        market.updated_at = "v2"
        signatures = _build_cached_market_signatures([market], matcher)

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
        worker_module._market_signature_cache[1] = {
            "fingerprint": ("alpha",),
            "signature": {"market": SimpleNamespace(id=1)},
            "last_seen_at": 1.0,
        }
        worker_module._market_signature_cache[2] = {
            "fingerprint": ("beta",),
            "signature": {"market": SimpleNamespace(id=2)},
            "last_seen_at": 2.0,
        }

        _prune_market_signature_cache([SimpleNamespace(id=2)], [])

        self.assertNotIn(1, worker_module._market_signature_cache)
        self.assertIn(2, worker_module._market_signature_cache)


class FakeRedis:
    def __init__(self):
        self.data = {}


    async def get(self, key):
        return self.data.get(key)


    async def setex(self, key, ttl, value):
        self.data[key] = value


    async def delete(self, key):
        self.data.pop(key, None)


class WorkerEmptyOrderbookStateTests(unittest.IsolatedAsyncioTestCase):
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
            active_pairs = await _filter_skippable_pairs(pairs)

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
            await _update_empty_counts(checked_pairs, {"pair-2"})

        self.assertEqual(fake_redis.data["worker:pair-empty-count:pair-1"], "1")
        self.assertNotIn("worker:pair-empty-count:pair-2", fake_redis.data)