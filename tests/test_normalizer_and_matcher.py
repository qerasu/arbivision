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

        self.assertEqual(entities["dates"], ["march 15 2026"])
        self.assertEqual(entities["numbers"], ["5000"])


    def test_extract_entities_distinguishes_month_from_specific_day(self):
        service = NormalizerService()

        monthly_entities = service.extract_entities("What price will Bitcoin hit in April?")
        daily_entities = service.extract_entities("Will Bitcoin reach 75000 on Apr. 11?")

        self.assertEqual(monthly_entities["dates"], ["april"])
        self.assertEqual(daily_entities["dates"], ["april 11"])
        self.assertEqual(daily_entities["numbers"], ["75000"])


class MatcherServiceTests(unittest.TestCase):
    def setUp(self):
        self.matcher = MatcherService()


    def test_rejects_close_match_without_full_confidence(self):
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

        self.assertIsNone(pair)


    def test_auto_approves_direct_condition_match_with_non_binary_labels(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Memphis Grizzlies vs Charlotte Hornets",
            slug="memphis-grizzlies-vs-charlotte-hornets",
            outcomes_json=[{"id": "poly-a", "label": "Grizzlies"}, {"id": "poly-b", "label": "Hornets"}],
            raw_payload_json={"conditionId": "cond-1"},
            category="sports",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Grizzlies vs. Hornets",
            slug="grizzlies-vs-hornets",
            outcomes_json=[{"id": "pf-a", "label": "Grizzlies"}, {"id": "pf-b", "label": "Hornets"}],
            raw_payload_json={"polymarketConditionIds": ["cond-1"]},
            category="sports",
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


    def test_direct_condition_match_uses_group_item_subject_for_binary_markets(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Which company has the best AI model end of April?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={"conditionId": "cond-1", "groupItemTitle": "Alibaba"},
            category="tech",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Will Alibaba have the best AI model at the end of April?",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={"polymarketConditionIds": ["cond-1"]},
            category="tech",
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNotNone(pair)
        self.assertEqual(pair.status, "auto_approved")
        self.assertEqual(pair.match_score, 1.0)


    def test_rejects_direct_condition_match_when_binary_subjects_differ(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Which company has the best AI model end of April?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={"conditionId": "cond-1", "groupItemTitle": "Alibaba"},
            category="tech",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Will DeepSeek have the best AI model at the end of April?",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={"polymarketConditionIds": ["cond-1"]},
            category="tech",
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "direct_condition_context_mismatch")


    def test_rejects_direct_condition_match_when_market_variants_differ(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Spread: Celtics (-6.5)",
            slug="nba-cha-bos-2026-04-07-spread-home-6pt5",
            outcomes_json=[{"id": "poly-a", "label": "Hornets"}, {"id": "poly-b", "label": "Celtics"}],
            raw_payload_json={"conditionId": "cond-1", "groupItemTitle": "Spread -6.5"},
            category="sports",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Hornets vs. Celtics",
            slug="nba-cha-bos-2026-04-07",
            outcomes_json=[{"id": "pf-a", "label": "Hornets"}, {"id": "pf-b", "label": "Celtics"}],
            raw_payload_json={"polymarketConditionIds": ["cond-1"]},
            category="sports",
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)


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
        self.assertEqual(pair.match_score, 0.6)
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


    def test_rejects_matchups_sharing_only_one_team_name(self):
        poly_market = SimpleNamespace(
            id=10,
            title="CR Brasil vs Athletic Club",
            outcomes_json=[
                {"id": "poly-a", "label": "CR Brasil"},
                {"id": "poly-b", "label": "Athletic Club"},
                {"id": "poly-c", "label": "Draw"},
            ],
            raw_payload_json={},
            category="brazil serie b",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Athletic Club vs. Villarreal",
            outcomes_json=[
                {"id": "pf-a", "label": "Athletic Club"},
                {"id": "pf-b", "label": "Villarreal"},
                {"id": "pf-c", "label": "Draw"},
            ],
            raw_payload_json={},
            category="laliga",
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "participant_mismatch")


    def test_rejects_series_market_against_single_game_market(self):
        poly_market = SimpleNamespace(
            id=10,
            title="NBA Playoffs: Who Will Win Series? - Lakers vs. Rockets",
            outcomes_json=[
                {"id": "poly-a", "label": "Lakers"},
                {"id": "poly-b", "label": "Rockets"},
            ],
            raw_payload_json={},
            category="sports",
            description="",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Rockets vs. Lakers",
            outcomes_json=[
                {"id": "pf-a", "label": "Rockets"},
                {"id": "pf-b", "label": "Lakers"},
            ],
            raw_payload_json={
                "description": (
                    "In the upcoming NBA game, scheduled for April 18 at 8:30PM ET: "
                    "If the Rockets win, the market will resolve to Rockets. "
                    "If the Lakers win, the market will resolve to Lakers."
                ),
            },
            category="sports",
            description="",
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "event_granularity_mismatch")


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


    def test_rejects_spread_market_against_moneyline_matchup(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Spread: Celtics (-6.5)",
            slug="nba-cha-bos-2026-04-07-spread-home-6pt5",
            outcomes_json=[
                {"id": "poly-a", "label": "Hornets"},
                {"id": "poly-b", "label": "Celtics"},
            ],
            raw_payload_json={"groupItemTitle": "Spread -6.5", "question": "Spread: Celtics (-6.5)"},
            category="sports",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Hornets vs. Celtics",
            slug="nba-cha-bos-2026-04-07",
            outcomes_json=[
                {"id": "pf-a", "label": "Hornets"},
                {"id": "pf-b", "label": "Celtics"},
            ],
            raw_payload_json={"question": "Hornets vs. Celtics"},
            category="sports",
        )

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)


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


    def test_rejects_monthly_bitcoin_market_against_specific_day_market(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Will Bitcoin reach 75000 on April 11?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Will Bitcoin reach 75000 in April?",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={},
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "date_mismatch")


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


    def test_rejects_exact_threshold_vs_at_least_threshold(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Will annual inflation increase by 2.8% in March?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Will annual inflation be at least 2.8% in March?",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={},
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "comparison_mismatch")


    def test_rejects_exact_threshold_vs_unicode_gte_threshold(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Will annual inflation increase by 2.8% in March?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={"question": "Will annual inflation increase by 2.8% in March?"},
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Will annual inflation increase by ≥2.8% in March?",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={"question": "Will annual inflation increase by ≥2.8% in March?"},
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "comparison_mismatch")


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


    def test_rejects_chinese_ai_company_market_against_generic_ai_model_market(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Best Chinese AI Company end of April?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={"groupItemTitle": "Alibaba"},
            category="tech",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Will Alibaba have the best AI model at the end of April?",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={},
            category="tech",
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "market_context_mismatch")


    def test_rejects_math_ai_model_market_against_generic_ai_model_market(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Which company has the best Math AI model end of April?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={"groupItemTitle": "DeepSeek"},
            category="tech",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Will DeepSeek have the best AI model at the end of April?",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={},
            category="tech",
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "market_context_mismatch")


    def test_rejects_generic_title_match_when_category_adds_missing_qualifier(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Which company has the best AI model end of April?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={"groupItemTitle": "Alibaba"},
            category="china",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Will Alibaba have the best AI model at the end of April?",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={},
            category="tech",
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "market_context_mismatch")


    def test_rejects_generic_title_match_when_subcategory_adds_missing_qualifier(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Which company has the best AI model end of April?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={"groupItemTitle": "DeepSeek", "subcategory": "math"},
            category="tech",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Will DeepSeek have the best AI model at the end of April?",
            outcomes_json=[{"id": "pf-y", "label": "Yes"}, {"id": "pf-n", "label": "No"}],
            raw_payload_json={},
            category="tech",
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "market_context_mismatch")


    def test_rejects_halftime_market_against_full_match_draw_market(self):
        poly_market = SimpleNamespace(
            id=10,
            title="Manchester United FC vs. Leeds United FC: Draw at halftime?",
            outcomes_json=[{"id": "poly-y", "label": "Yes"}, {"id": "poly-n", "label": "No"}],
            raw_payload_json={"groupItemTitle": "Halftime Result"},
            category="sports",
        )
        pf_market = SimpleNamespace(
            id=20,
            title="Manchester United FC vs. Leeds United FC",
            outcomes_json=[
                {"id": "pf-home", "label": "Manchester United FC"},
                {"id": "pf-draw", "label": "Draw"},
                {"id": "pf-away", "label": "Leeds United FC"},
            ],
            raw_payload_json={"question": "Manchester United FC vs. Leeds United FC"},
            category="sports",
        )

        decision = self.matcher.explain_match(poly_market, pf_market)

        self.assertFalse(decision["matched"])
        self.assertEqual(decision["reason"]["reject_reason"], "market_scope_mismatch")
