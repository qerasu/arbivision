import unittest
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy.exc import IntegrityError

from arbitrage_bot.tg_bot.preferences import default_preferences
from arbitrage_bot.tg_bot.preferences import ensure_telegram_user
from arbitrage_bot.tg_bot.preferences import effective_min_roi
from arbitrage_bot.tg_bot.preferences import extract_pair_close_datetime
from arbitrage_bot.tg_bot.preferences import filter_reason_for_preferences
from arbitrage_bot.tg_bot.preferences import format_preferences_text


class TelegramPreferencesTests(unittest.TestCase):
    def test_filter_reason_returns_none_when_opportunity_passes_all_filters(self):
        opportunity = SimpleNamespace(net_roi=0.12, capital_required=100.0)
        now = datetime(2026, 3, 21, tzinfo=timezone.utc)
        market = SimpleNamespace(raw_payload_json={"endDate": (now + timedelta(days=3)).isoformat()})

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

        self.assertEqual(preferences["min_roi_percent"], 1)
        self.assertEqual(preferences["min_capital_usd"], 10)
        self.assertEqual(preferences["max_capital_usd"], 150)
        self.assertIsNone(preferences["min_profit_usd"])
        self.assertEqual(preferences["max_days_to_close"], 5)


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


    def test_filter_reason_blocks_by_min_capital(self):
        opportunity = SimpleNamespace(net_roi=0.20, capital_required=40.0, net_profit=20.0)
        market = SimpleNamespace(raw_payload_json={})

        reason = filter_reason_for_preferences(
            opportunity,
            market,
            market,
            {"min_roi_percent": None, "min_capital_usd": 50.0, "max_capital_usd": None, "min_profit_usd": None, "max_days_to_close": None},
            now=datetime(2026, 3, 21, tzinfo=timezone.utc),
        )

        self.assertEqual(reason, "min_capital")


    def test_filter_reason_blocks_by_min_profit(self):
        opportunity = SimpleNamespace(net_roi=0.20, capital_required=200.0, net_profit=7.0)
        market = SimpleNamespace(raw_payload_json={})

        reason = filter_reason_for_preferences(
            opportunity,
            market,
            market,
            {"min_roi_percent": None, "min_capital_usd": None, "max_capital_usd": None, "min_profit_usd": 10.0, "max_days_to_close": None},
            now=datetime(2026, 3, 21, tzinfo=timezone.utc),
        )

        self.assertEqual(reason, "min_profit")


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
                "min_capital_usd": 50.0,
                "max_capital_usd": 500.0,
                "min_profit_usd": 10.0,
                "max_days_to_close": 7,
            }
        )

        self.assertIn("Your alert settings", text)
        self.assertIn("Min ROI\nCurrent: 2.50%", text)
        self.assertIn("Min volume\nCurrent: $50", text)
        self.assertIn("Volume\nCurrent: $500", text)
        self.assertIn("Min profit\nCurrent: $10", text)
        self.assertIn("Max market end\nCurrent: 7 days", text)
        self.assertNotIn("Use text commands:", text)


    def test_format_preferences_text_keeps_decimal_volume_when_needed(self):
        text = format_preferences_text(
            {
                "min_roi_percent": 2.5,
                "min_capital_usd": 40.25,
                "max_capital_usd": 140.5,
                "min_profit_usd": 7.5,
                "max_days_to_close": 7,
            }
        )

        self.assertIn("Volume\nCurrent: $140.50", text)
        self.assertIn("Min volume\nCurrent: $40.25", text)
        self.assertIn("Min profit\nCurrent: $7.50", text)


    def test_effective_min_roi_returns_default(self):
        value = effective_min_roi(default_preferences())

        self.assertEqual(value, 1.0)


    def test_effective_min_roi_returns_none_when_filters_are_reset(self):
        value = effective_min_roi(
            {
                "min_roi_percent": None,
                "max_capital_usd": None,
                "max_days_to_close": None,
            }
        )

        self.assertIsNone(value)


class FakeScalarResult:
    def __init__(self, value):
        self.value = value


    def scalars(self):
        return self


    def first(self):
        return self.value


class FakeTelegramPreferencesDb:
    def __init__(self, telegram_chat=None, preference=None, subscription=None):
        self.telegram_chat = telegram_chat
        self.preference = preference
        self.subscription = subscription
        self.added = []
        self.flush_calls = 0
        self.commit_calls = 0
        self.rollback_calls = 0
        self.fail_commit_once = False


    def add(self, item):
        self.added.append(item)
        if item.__class__.__name__ == "TelegramChat":
            self.telegram_chat = item
        elif item.__class__.__name__ == "UserPreference":
            self.preference = item
        elif item.__class__.__name__ == "Subscription":
            self.subscription = item
        elif item.__class__.__name__ == "User" and getattr(item, "id", None) is None:
            item.id = 1


    async def flush(self):
        self.flush_calls += 1


    async def commit(self):
        self.commit_calls += 1
        if self.fail_commit_once:
            self.fail_commit_once = False
            raise IntegrityError("duplicate", None, None)


    async def rollback(self):
        self.rollback_calls += 1


    async def execute(self, stmt):
        compiled = str(stmt)
        if "FROM telegram_chats" in compiled:
            return FakeScalarResult(self.telegram_chat)
        if "FROM user_preferences" in compiled:
            return FakeScalarResult(self.preference)
        if "FROM subscriptions" in compiled:
            return FakeScalarResult(self.subscription)
        raise AssertionError(f"unexpected stmt: {compiled}")


class TelegramEnsureUserTests(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_telegram_user_reactivates_existing_subscription(self):
        telegram_chat = SimpleNamespace(user_id=7, chat_id="1442867742")
        preference = SimpleNamespace(user_id=7)
        subscription = SimpleNamespace(user_id=7, channel="telegram", destination="1442867742", status="paused")
        db = FakeTelegramPreferencesDb(
            telegram_chat=telegram_chat,
            preference=preference,
            subscription=subscription,
        )

        result = await ensure_telegram_user(db, 1442867742)

        self.assertIs(result, telegram_chat)
        self.assertEqual(subscription.status, "active")
        self.assertEqual(db.commit_calls, 1)


    async def test_ensure_telegram_user_retries_after_integrity_error(self):
        db = FakeTelegramPreferencesDb()
        db.fail_commit_once = True

        existing_chat = SimpleNamespace(user_id=1, chat_id="1442867742")
        existing_pref = SimpleNamespace(user_id=1)
        existing_subscription = SimpleNamespace(
            user_id=1,
            channel="telegram",
            destination="1442867742",
            status="active",
        )

        original_commit = db.commit

        async def commit_with_existing_records():
            if db.fail_commit_once:
                db.telegram_chat = existing_chat
                db.preference = existing_pref
                db.subscription = existing_subscription
            await original_commit()

        db.commit = commit_with_existing_records

        with patch("arbitrage_bot.tg_bot.preferences.User") as user_cls:
            user_cls.return_value = SimpleNamespace(id=1)
            result = await ensure_telegram_user(db, 1442867742)

        self.assertIs(result, existing_chat)
        self.assertEqual(db.rollback_calls, 1)
        self.assertEqual(db.commit_calls, 1)