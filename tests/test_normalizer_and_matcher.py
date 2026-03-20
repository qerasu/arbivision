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
        self.assertEqual(entities["numbers"], ["5000", "15", "2026"])


class MatcherServiceTests(unittest.TestCase):
    def setUp(self):
        self.matcher = MatcherService(db_session=None)

    def test_auto_approves_close_match(self):
        poly_market = SimpleNamespace(id=10, title="Will Bitcoin price exceed 100k in 2026")
        pf_market = SimpleNamespace(id=20, title="Bitcoin price exceed 100k in 2026")

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNotNone(pair)
        self.assertEqual(pair.status, "auto_approved")
        self.assertGreaterEqual(pair.match_score, 0.85)

    def test_rejects_markets_with_different_numbers(self):
        poly_market = SimpleNamespace(id=10, title="Will Bitcoin price exceed 100k in 2026")
        pf_market = SimpleNamespace(id=20, title="Will Bitcoin price exceed 90k in 2026")

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNone(pair)

    def test_marks_partial_match_for_manual_review(self):
        poly_market = SimpleNamespace(id=10, title="Will Bitcoin exceed 100k in 2026")
        pf_market = SimpleNamespace(id=20, title="Bitcoin exceed 100k 2026 today")

        pair = self.matcher.match_candidates(poly_market, pf_market)

        self.assertIsNotNone(pair)
        self.assertEqual(pair.status, "manual_review")
        self.assertGreaterEqual(pair.match_score, 0.65)
        self.assertLess(pair.match_score, 0.85)
