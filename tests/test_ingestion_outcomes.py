import unittest

from arbitrage_bot.services.ingestion import IngestionService


class IngestionOutcomeNormalizationTests(unittest.TestCase):


    def setUp(self):
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
                "title": "Knicks vs. Spurs",
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