import unittest
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import SimpleNamespace

from arbitrage_bot.tg_bot.preferences import default_preferences
from arbitrage_bot.tg_bot.preferences import effective_min_roi
from arbitrage_bot.tg_bot.preferences import extract_pair_close_datetime
from arbitrage_bot.tg_bot.preferences import filter_reason_for_preferences
from arbitrage_bot.tg_bot.preferences import format_preferences_text


class TelegramPreferencesTests(unittest.TestCase):


    def test_filter_reason_returns_none_when_opportunity_passes_all_filters(self):
        opportunity = SimpleNamespace(net_roi=0.12, capital_required=100.0)
        now = datetime(2026, 3, 21, tzinfo=timezone.utc)
        market = SimpleNamespace(raw_payload_json={"endDate": (now + timedelta(days=7)).isoformat()})

        reason = filter_reason_for_preferences(
            opportunity,
            market,
            market,
            default_preferences(),
            now=now,
        )

        self.assertIsNone(reason)


    def test_default_preferences_use_expected_values(self):
        preferences = default_preferences()

        self.assertIsNone(preferences["min_roi_percent"])
        self.assertIsNone(preferences["max_capital_usd"])
        self.assertEqual(preferences["max_days_to_close"], 30)


    def test_filter_reason_blocks_by_min_roi(self):
        opportunity = SimpleNamespace(net_roi=0.009, capital_required=100.0)
        market = SimpleNamespace(raw_payload_json={})

        reason = filter_reason_for_preferences(
            opportunity,
            market,
            market,
            {"min_roi_percent": 1.0, "max_capital_usd": None, "max_days_to_close": None},
            now=datetime(2026, 3, 21, tzinfo=timezone.utc),
        )

        self.assertEqual(reason, "min_roi")


    def test_filter_reason_blocks_by_max_capital(self):
        opportunity = SimpleNamespace(net_roi=0.20, capital_required=600.0)
        market = SimpleNamespace(raw_payload_json={})

        reason = filter_reason_for_preferences(
            opportunity,
            market,
            market,
            {"min_roi_percent": None, "max_capital_usd": 500.0, "max_days_to_close": None},
            now=datetime(2026, 3, 21, tzinfo=timezone.utc),
        )

        self.assertEqual(reason, "max_capital")


    def test_filter_reason_blocks_by_max_days_to_close(self):
        now = datetime(2026, 3, 21, tzinfo=timezone.utc)
        opportunity = SimpleNamespace(net_roi=0.20, capital_required=200.0)
        market_a = SimpleNamespace(raw_payload_json={"endDate": (now + timedelta(days=3)).isoformat()})
        market_b = SimpleNamespace(raw_payload_json={"resolveDate": (now + timedelta(days=10)).isoformat()})

        reason = filter_reason_for_preferences(
            opportunity,
            market_a,
            market_b,
            {"min_roi_percent": None, "max_capital_usd": None, "max_days_to_close": 7},
            now=now,
        )

        self.assertEqual(reason, "max_days_to_close")


    def test_extract_pair_close_datetime_uses_latest_known_market_datetime(self):
        now = datetime(2026, 3, 21, tzinfo=timezone.utc)
        market_a = SimpleNamespace(raw_payload_json={"endDate": (now + timedelta(days=5)).isoformat()})
        market_b = SimpleNamespace(raw_payload_json={"closeTime": (now + timedelta(days=9)).isoformat()})

        close_at = extract_pair_close_datetime(market_a, market_b)

        self.assertEqual(close_at, now + timedelta(days=9))


    def test_format_preferences_text_shows_human_readable_values(self):
        text = format_preferences_text(
            {
                "min_roi_percent": 2.5,
                "max_capital_usd": 500.0,
                "max_days_to_close": 7,
            }
        )

        self.assertIn("Global alert settings", text)
        self.assertIn("Min ROI\nCurrent: 2.50%", text)
        self.assertIn("Volume\nCurrent: $500", text)
        self.assertIn("Max market end\nCurrent: 7 days", text)
        self.assertNotIn("Use text commands:", text)


    def test_format_preferences_text_keeps_decimal_volume_when_needed(self):
        text = format_preferences_text(
            {
                "min_roi_percent": 2.5,
                "max_capital_usd": 140.5,
                "max_days_to_close": 7,
            }
        )

        self.assertIn("Volume\nCurrent: $140.50", text)


    def test_effective_min_roi_falls_back_to_system_default(self):
        value = effective_min_roi(default_preferences())

        self.assertIsNone(value)


    def test_effective_min_roi_returns_none_when_filters_are_reset(self):
        value = effective_min_roi(
            {
                "min_roi_percent": None,
                "max_capital_usd": None,
                "max_days_to_close": None,
            }
        )

        self.assertIsNone(value)