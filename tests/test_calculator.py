import unittest

from arbitrage_bot.services.calculator import ArbitrageCalculator


class ArbitrageCalculatorTests(unittest.TestCase):


    def test_calculates_weighted_opportunity_across_multiple_levels(self):
        calculator = ArbitrageCalculator()

        result = calculator.calculate_opportunity(
            poly_asks=[(0.40, 10), (0.45, 5)],
            pf_asks=[(0.50, 6), (0.52, 10)],
        )

        self.assertIsNotNone(result)
        self.assertAlmostEqual(result["shares"], 15.0)
        self.assertAlmostEqual(result["capital_required"], 13.93)
        self.assertAlmostEqual(result["avg_price_leg_1"], 6.25 / 15)
        self.assertAlmostEqual(result["avg_price_leg_2"], 7.68 / 15)
        self.assertAlmostEqual(result["gross_profit"], 1.07)
        self.assertAlmostEqual(result["net_profit"], 1.07)
        self.assertAlmostEqual(result["gross_roi"], 1.07 / 13.93)
        self.assertAlmostEqual(result["net_roi"], 1.07 / 13.93)


    def test_returns_none_when_first_profitable_level_does_not_exist(self):
        calculator = ArbitrageCalculator()

        result = calculator.calculate_opportunity(
            poly_asks=[(0.60, 10)],
            pf_asks=[(0.45, 10)],
        )

        self.assertIsNone(result)


    def test_returns_none_for_invalid_negative_prices(self):
        calculator = ArbitrageCalculator()

        result = calculator.calculate_opportunity(
            poly_asks=[(-0.10, 10)],
            pf_asks=[(0.50, 10)],
        )

        self.assertIsNone(result)


    def test_calculates_multiple_directions(self):
        calculator = ArbitrageCalculator()

        results = calculator.calculate_opportunities(
            {
                "A_yes_B_no": {
                    "poly": [(0.40, 10)],
                    "pf": [(0.50, 10)],
                },
                "A_no_B_yes": {
                    "poly": [(0.60, 10)],
                    "pf": [(0.45, 10)],
                },
            }
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["direction"], "A_yes_B_no")