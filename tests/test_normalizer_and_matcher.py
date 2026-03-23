import unittest
from types import SimpleNamespace

from arbitrage_bot.services.matcher import MatcherService
from arbitrage_bot.services.normalizer import NormalizerService


class NormalizerServiceTests(unittest.TestCase):


    def test_normalize_text_removes_punctuation_and_collapses_spaces(self):
        service = NormalizerService()

        normalized = service.normalize_text("Will BTC hit $100,000?!  Soon.")

        self.assertEqual(normalized, "will btc hit 100000 soon")


    def test_extract_entities_finds_dates_and_numbers(self):
        service = NormalizerService()

        entities = service.extract_entities("Will ETH reach 5000 by March 15, 2026?")

        self.assertEqual(entities["dates"], ["march 15, 2026"])
        self.assertEqual(entities["numbers"], ["5000"])


class MatcherServiceTests(unittest.TestCase):


    def setUp(self):
        self.matcher = MatcherService()


    def test_auto_approves_close_match(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Will Bitcoin price exceed 100k in 2026",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Bitcoin price exceed 100k in 2026",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={},
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNotNone(pair)
        self.assertEqual(pair.status, "auto_approved")
        self.assertGreaterEqual(pair.match_score, 0.85)
        self.assertEqual(
            pair.outcome_mapping_json,
            {
                "market_a": {"yes": "poly-y", "no": "poly-n"},
                "market_a": {"yes": "poly-y", "no": "poly-n", "yes_label": "Yes", "no_label": "No"},
                "market_b": {"yes": "pf-y", "no": "pf-n", "yes_label": "Yes", "no_label": "No"},
                "is_inverted": False,
                "confidence": "high",
            },
        )


    def test_auto_approves_direct_condition_match_with_non_binary_labels(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Memphis Grizzlies vs Charlotte Hornets",
            outcomes_json=[{"id": "poly-a", "label": "Grizzlies"}, {"id": "poly-b", "label": "Hornets"}],
            raw_payload_json={"conditionId": "cond-1"},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Grizzlies vs. Hornets",
            outcomes_json=[{"id": "pf-a", "label": "Grizzlies"}, {"id": "pf-b", "label": "Hornets"}],
            raw_payload_json={"polymarketConditionIds": ["cond-1"]},
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNotNone(pair)
        self.assertEqual(pair.status, "auto_approved")
        self.assertEqual(pair.match_score, 1.0)
        self.assertEqual(
            pair.outcome_mapping_json,
            {
                "market_a": {"yes": "poly-a", "no": "poly-b", "yes_label": "Grizzlies", "no_label": "Hornets"},
                "market_b": {"yes": "pf-a", "no": "pf-b", "yes_label": "Grizzlies", "no_label": "Hornets"},
                "is_inverted": False,
                "confidence": "medium",
            },
        )


    def test_matches_team_event_when_titles_are_not_identical(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Memphis Grizzlies vs Charlotte Hornets",
            outcomes_json=[
                {"id": "poly-a", "label": "Memphis Grizzlies"},
                {"id": "poly-b", "label": "Charlotte Hornets"},
            ],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Grizzlies vs. Hornets",
            outcomes_json=[
                {"id": "pf-a", "label": "Grizzlies"},
                {"id": "pf-b", "label": "Hornets"},
            ],
            raw_payload_json={},
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNotNone(pair)
        self.assertEqual(pair.status, "auto_approved")
        self.assertGreaterEqual(pair.match_score, 0.8)
        self.assertEqual(
            pair.outcome_mapping_json,
            {
                "market_a": {
                    "yes": "poly-a",
                    "no": "poly-b",
                    "yes_label": "Memphis Grizzlies",
                    "no_label": "Charlotte Hornets",
                },
                "market_b": {
                    "yes": "pf-a",
                    "no": "pf-b",
                    "yes_label": "Grizzlies",
                    "no_label": "Hornets",
                },
                "is_inverted": False,
                "confidence": "high",
            },
        )


    def test_matches_binary_head_to_head_market_against_named_matchup(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Will Memphis Grizzlies beat Charlotte Hornets?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Grizzlies vs. Hornets",
            outcomes_json=[
                {"id": "pf-a", "label": "Grizzlies"},
                {"id": "pf-b", "label": "Hornets"},
            ],
            raw_payload_json={},
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNotNone(pair)
        self.assertEqual(pair.status, "auto_approved")
        self.assertEqual(
            pair.outcome_mapping_json,
            {
                "market_a": {"yes": "poly-y", "no": "poly-n", "yes_label": "Yes", "no_label": "No"},
                "market_b": {"yes": "pf-a", "no": "pf-b", "yes_label": "Grizzlies", "no_label": "Hornets"},
                "is_inverted": False,
                "confidence": "medium",
            },
        )


    def test_matches_named_matchup_against_binary_head_to_head_market(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Memphis Grizzlies vs Charlotte Hornets",
            outcomes_json=[
                {"id": "poly-a", "label": "Memphis Grizzlies"},
                {"id": "poly-b", "label": "Charlotte Hornets"},
            ],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Will Grizzlies beat Hornets?",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={},
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNotNone(pair)
        self.assertEqual(pair.status, "auto_approved")
        self.assertEqual(
            pair.outcome_mapping_json,
            {
                "market_a": {
                    "yes": "poly-a",
                    "no": "poly-b",
                    "yes_label": "Memphis Grizzlies",
                    "no_label": "Charlotte Hornets",
                },
                "market_b": {"yes": "pf-y", "no": "pf-n", "yes_label": "Yes", "no_label": "No"},
                "is_inverted": False,
                "confidence": "medium",
            },
        )


    def test_rejects_matchup_against_single_team_future(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Will the Memphis Grizzlies win the NBA Western Conference Finals?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Grizzlies vs. Hornets",
            outcomes_json=[
                {"id": "pf-a", "label": "Grizzlies"},
                {"id": "pf-b", "label": "Hornets"},
            ],
            raw_payload_json={},
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)


    def test_rejects_markets_with_different_numbers(self):
        poly_market = SimpleNamespace(id=10, title="Will Bitcoin price exceed 100k in 2026", outcomes_json=[], raw_payload_json={})
        pf_market = SimpleNamespace(id=20, title="Will Bitcoin price exceed 90k in 2026", outcomes_json=[], raw_payload_json={})

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)


    def test_rejects_partial_match_without_auto_approval(self):
        poly_market = SimpleNamespace(id=10, title="Will Bitcoin exceed 100k in 2026", outcomes_json=[], raw_payload_json={})
        pf_market = SimpleNamespace(id=20, title="Bitcoin exceed 100k 2026 today", outcomes_json=[], raw_payload_json={})

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)


    def test_rejects_high_score_match_without_full_auto_approval_signal(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Will Bitcoin price exceed 100k in 2026",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Bitcoin price exceed 100k in 2026 today",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={},
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)


    def test_rejects_high_score_match_when_outcome_mapping_is_missing(self):
        poly_market = SimpleNamespace(id=10, title="Will Bitcoin price exceed 100k in 2026", outcomes_json=[], raw_payload_json={})
        pf_market = SimpleNamespace(id=20, title="Bitcoin price exceed 100k in 2026", outcomes_json=[], raw_payload_json={})

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)


    def test_explain_match_reports_rejection_reason_for_number_mismatch(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Will Bitcoin price exceed 100k in 2026",
            outcomes_json=[],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Will Bitcoin price exceed 90k in 2026",
            outcomes_json=[],
            raw_payload_json={},
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "number_mismatch")


    def test_rejects_nvidia_third_largest_vs_largest(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Will NVIDIA be the third-largest company in the world by market cap on March 31?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="NVIDIA largest company on Mar 31",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={},
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)


    def test_rejects_pga_category_vs_specific_player_market(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Will Tommy Fleetwood win the 2026 Masters tournament?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="PGA Major Champs 2026",
            outcomes_json=[
                {"id": "pf-a", "label": "Scottie Scheffler"},
                {"id": "pf-b", "label": "Rory McIlroy"},
            ],
            raw_payload_json={},
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)


    def test_rejects_womens_ncaa_vs_mens_ncaa(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Will Duke win the 2026 Women's NCAA Tournament?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="2026 Men's NCAA Tournament Winner",
            outcomes_json=[
                {"id": "pf-a", "label": "Duke"},
                {"id": "pf-b", "label": "Auburn"},
            ],
            raw_payload_json={},
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)


    def test_rejects_different_markets_with_same_generic_outcomes(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Canada's population Up or Down this year?",
            outcomes_json=[{"id": "poly-a", "label": "Up"}, {"id": "poly-b", "label": "Down"}],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="BTC/USD Up or Down - March 22",
            outcomes_json=[{"id": "pf-a", "label": "Up"}, {"id": "pf-b", "label": "Down"}],
            raw_payload_json={},
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)