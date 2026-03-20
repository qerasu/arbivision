import unittest
from types import SimpleNamespace
from unittest.mock import patch

from arbitrage_bot.core.config import settings
from arbitrage_bot.services.alert_manager import AlertManager


class FakeRedis:
    def __init__(self, initial_data=None):
        self.data = dict(initial_data or {})
        self.setex_calls = []

    async def get(self, key):
        return self.data.get(key)

    async def setex(self, key, ttl, value):
        self.data[key] = value
        self.setex_calls.append((key, ttl, value))


class FakeDbSession:
    def __init__(self):
        self.added = []
        self.flush_calls = 0
        self.commit_calls = 0

    def add(self, item):
        self.added.append(item)
        if getattr(item, "id", None) is None:
            item.id = len(self.added)

    async def flush(self):
        self.flush_calls += 1

    async def commit(self):
        self.commit_calls += 1


class AlertManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_creates_opportunity_and_alerts_then_updates_dedupe_cache(self):
        db = FakeDbSession()
        redis = FakeRedis()
        manager = AlertManager(db)
        pair = SimpleNamespace(id=7, pair_hash="pair-123")
        calc_result = {
            "avg_price_leg_1": 0.40,
            "avg_price_leg_2": 0.50,
            "shares": 10.0,
            "capital_required": 9.0,
            "gross_profit": 1.0,
            "net_profit": 7.5,
            "gross_roi": 0.11,
            "net_roi": 0.12,
        }

        original_chat_ids = settings.TELEGRAM_DEFAULT_CHAT_IDS
        settings.TELEGRAM_DEFAULT_CHAT_IDS = ["1001", "1002"]
        try:
            with patch("arbitrage_bot.services.alert_manager.get_redis", return_value=redis):
                alerts = await manager.process_opportunity(pair, calc_result)
        finally:
            settings.TELEGRAM_DEFAULT_CHAT_IDS = original_chat_ids

        self.assertEqual(len(alerts), 2)
        self.assertEqual(db.flush_calls, 1)
        self.assertEqual(db.commit_calls, 1)
        self.assertEqual(len(redis.setex_calls), 1)
        self.assertEqual(alerts[0].status, "queued")
        self.assertEqual(alerts[0].telegram_chat_id, "1001")
        self.assertEqual(alerts[1].telegram_chat_id, "1002")

    async def test_skips_alert_when_profit_deltas_are_below_threshold(self):
        db = FakeDbSession()
        redis = FakeRedis(
            {
                "alert-dedupe:pair-123:A_yes_B_no": (
                    '{"net_profit": 10.0, "net_roi": 0.2, "shares": 5.0}'
                )
            }
        )
        manager = AlertManager(db)
        pair = SimpleNamespace(id=7, pair_hash="pair-123")
        calc_result = {
            "avg_price_leg_1": 0.40,
            "avg_price_leg_2": 0.50,
            "shares": 10.0,
            "capital_required": 9.0,
            "gross_profit": 1.0,
            "net_profit": 11.0,
            "gross_roi": 0.11,
            "net_roi": 0.201,
        }

        with patch("arbitrage_bot.services.alert_manager.get_redis", return_value=redis):
            result = await manager.process_opportunity(pair, calc_result)

        self.assertFalse(result)
        self.assertEqual(db.commit_calls, 0)
        self.assertEqual(redis.setex_calls, [])

    async def test_skips_alert_when_profit_is_below_minimum(self):
        db = FakeDbSession()
        redis = FakeRedis()
        manager = AlertManager(db)
        pair = SimpleNamespace(id=7, pair_hash="pair-123")
        calc_result = {
            "avg_price_leg_1": 0.40,
            "avg_price_leg_2": 0.50,
            "shares": 10.0,
            "capital_required": 9.0,
            "gross_profit": 1.0,
            "net_profit": 1.0,
            "gross_roi": 0.11,
            "net_roi": 0.50,
        }

        with patch("arbitrage_bot.services.alert_manager.get_redis", return_value=redis):
            result = await manager.process_opportunity(pair, calc_result)

        self.assertFalse(result)
        self.assertEqual(db.commit_calls, 0)
