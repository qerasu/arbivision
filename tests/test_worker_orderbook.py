import unittest

from arbitrage_bot.worker import _extract_asks


class ExtractAsksTests(unittest.TestCase):


    def test_extracts_and_sorts_levels_from_plain_asks(self):
        orderbook = {
            "asks": [
                {"price": "0.55", "size": "4"},
                {"price": "0.50", "size": "2"},
            ]
        }

        result = _extract_asks(orderbook)

        self.assertEqual(result, [(0.5, 2.0), (0.55, 4.0)])


    def test_extracts_levels_from_nested_orderbook(self):
        orderbook = {
            "orderbook": {
                "sell_orders": [
                    {"p": "0.44", "qty": "10"},
                    {"p": "0.41", "qty": "3"},
                ]
            }
        }

        result = _extract_asks(orderbook)

        self.assertEqual(result, [(0.41, 3.0), (0.44, 10.0)])


    def test_skips_invalid_entries(self):
        orderbook = {
            "asks": [
                {"price": "bad", "size": "1"},
                {"size": "2"},
                (0.45, 5),
            ]
        }

        result = _extract_asks(orderbook)

        self.assertEqual(result, [(0.45, 5.0)])