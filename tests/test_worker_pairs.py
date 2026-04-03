import unittest
from types import SimpleNamespace

from arbitrage_bot.services.matcher import MatcherService
from arbitrage_bot.worker import _candidate_markets_for_poly, _mark_stale_pairs, _reconcile_market_pairs


class WorkerPairLifecycleTests(unittest.TestCase):
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