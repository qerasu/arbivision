import unittest
from types import SimpleNamespace
from unittest.mock import patch

from arbitrage_bot.core.config import settings
from arbitrage_bot.models.orm import Market
from arbitrage_bot.models.orm import Settings as OrmSettings
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


    async def delete(self, key):
        self.data.pop(key, None)


class FakeDbSession:


    def __init__(self):
        self.added = []
        self.flush_calls = 0
        self.commit_calls = 0
        self.rollback_calls = 0
        self.fail_commit = False
        self.global_preferences = None


    def add(self, item):
        self.added.append(item)
        if getattr(item, "id", None) is None:
            item.id = len(self.added)
        if isinstance(item, OrmSettings):
            self.global_preferences = item


    async def flush(self):
        self.flush_calls += 1


    async def commit(self):
        if self.fail_commit:
            raise RuntimeError("commit failed")
        self.commit_calls += 1


    async def rollback(self):
        self.rollback_calls += 1


    async def execute(self, stmt):
        compiled = str(stmt)
        if "FROM settings" in compiled:
            return FakeScalarResult(
                [self.global_preferences] if self.global_preferences is not None else []
            )
        raise AssertionError(f"unexpected stmt: {compiled}")


class FakeScalarResult:


    def __init__(self, items):
        self.items = items


    def scalars(self):
        return self


    def first(self):
        return self.items[0] if self.items else None


    def all(self):
        return list(self.items)


class AlertManagerTests(unittest.IsolatedAsyncioTestCase):


    def _build_markets(self):
        return (
            Market(
                id=101,
                platform="polymarket",
                platform_market_id="poly-101",
                status="active",
                tradable=True,
                title="market a",
                normalized_title="market a",
                description="",
                outcomes_json=[],
                raw_payload_json={"endDate": "2026-03-25T00:00:00+00:00"},
                category="",
                slug="",
            ),
            Market(
                id=202,
                platform="predict_fun",
                platform_market_id="pf-202",
                status="active",
                tradable=True,
                title="market b",
                normalized_title="market b",
                description="",
                outcomes_json=[],
                raw_payload_json={"resolveDate": "2026-03-26T00:00:00+00:00"},
                category="",
                slug="",
            ),
        )


    async def test_creates_opportunity_and_alerts_then_updates_dedupe_cache(self):
        db = FakeDbSession()
        redis = FakeRedis()
        manager = AlertManager(db)
        pair = SimpleNamespace(id=7, pair_hash="pair-123", market_id_a=101, market_id_b=202)
        market_a, market_b = self._build_markets()
        calc_result = {
            "direction": "A_yes_B_no",
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
                alerts = await manager.process_opportunity(pair, calc_result, market_a=market_a, market_b=market_b)
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
        pair = SimpleNamespace(id=7, pair_hash="pair-123", market_id_a=101, market_id_b=202)
        market_a, market_b = self._build_markets()
        calc_result = {
            "direction": "A_yes_B_no",
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
            result = await manager.process_opportunity(pair, calc_result, market_a=market_a, market_b=market_b)

        self.assertFalse(result)
        self.assertEqual(db.commit_calls, 0)
        self.assertEqual(redis.setex_calls, [])


    async def test_skips_alert_when_roi_is_below_minimum(self):
        db = FakeDbSession()
        redis = FakeRedis()
        manager = AlertManager(db)
        pair = SimpleNamespace(id=7, pair_hash="pair-123", market_id_a=101, market_id_b=202)
        market_a, market_b = self._build_markets()
        calc_result = {
            "direction": "A_yes_B_no",
            "avg_price_leg_1": 0.40,
            "avg_price_leg_2": 0.50,
            "shares": 10.0,
            "capital_required": 9.0,
            "gross_profit": 1.0,
            "net_profit": 1.0,
            "gross_roi": 0.11,
            "net_roi": 0.0001,
        }

        with patch("arbitrage_bot.services.alert_manager.get_redis", return_value=redis):
            result = await manager.process_opportunity(pair, calc_result, market_a=market_a, market_b=market_b)

        self.assertFalse(result)
        self.assertEqual(db.commit_calls, 0)


    async def test_rolls_back_and_clears_dedupe_if_commit_fails(self):
        db = FakeDbSession()
        db.fail_commit = True
        redis = FakeRedis()
        manager = AlertManager(db)
        pair = SimpleNamespace(id=7, pair_hash="pair-123", market_id_a=101, market_id_b=202)
        market_a, market_b = self._build_markets()
        calc_result = {
            "direction": "A_no_B_yes",
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
        settings.TELEGRAM_DEFAULT_CHAT_IDS = ["1001"]
        try:
            with patch("arbitrage_bot.services.alert_manager.get_redis", return_value=redis):
                with self.assertRaisesRegex(RuntimeError, "commit failed"):
                    await manager.process_opportunity(pair, calc_result, market_a=market_a, market_b=market_b)
        finally:
            settings.TELEGRAM_DEFAULT_CHAT_IDS = original_chat_ids

        self.assertEqual(db.rollback_calls, 1)
        self.assertEqual(redis.data, {})


    async def test_skips_alert_when_global_max_capital_blocks_it(self):
        db = FakeDbSession()
        db.global_preferences = OrmSettings(
            key="tg_alert_prefs:global",
            value_json={
                "min_roi_percent": None,
                "max_capital_usd": 5.0,
                "max_days_to_close": None,
            },
        )
        redis = FakeRedis()
        manager = AlertManager(db)
        pair = SimpleNamespace(id=7, pair_hash="pair-123", market_id_a=101, market_id_b=202)
        market_a, market_b = self._build_markets()
        calc_result = {
            "direction": "A_yes_B_no",
            "avg_price_leg_1": 0.40,
            "avg_price_leg_2": 0.50,
            "shares": 10.0,
            "capital_required": 9.0,
            "gross_profit": 1.0,
            "net_profit": 7.5,
            "gross_roi": 0.11,
            "net_roi": 0.12,
        }

        with patch("arbitrage_bot.services.alert_manager.get_redis", return_value=redis):
            result = await manager.process_opportunity(pair, calc_result, market_a=market_a, market_b=market_b)

        self.assertFalse(result)
        self.assertEqual(db.commit_calls, 0)


    async def test_uses_provided_preferences_without_fetching_from_db(self):
        db = FakeDbSession()
        redis = FakeRedis()
        manager = AlertManager(db)
        pair = SimpleNamespace(id=7, pair_hash="pair-123", market_id_a=101, market_id_b=202)
        market_a, market_b = self._build_markets()
        calc_result = {
            "direction": "A_yes_B_no",
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
        settings.TELEGRAM_DEFAULT_CHAT_IDS = ["1001"]
        try:
            with patch(
                "arbitrage_bot.services.alert_manager.get_global_preferences",
                side_effect=AssertionError("should not be called"),
            ), patch(
                "arbitrage_bot.services.alert_manager.get_redis",
                return_value=redis,
            ):
                alerts = await manager.process_opportunity(
                    pair,
                    calc_result,
                    market_a=market_a,
                    market_b=market_b,
                    preferences={
                        "min_roi_percent": 0.1,
                        "max_capital_usd": 140.0,
                        "max_days_to_close": 30,
                    },
                )
        finally:
            settings.TELEGRAM_DEFAULT_CHAT_IDS = original_chat_ids

        self.assertEqual(len(alerts), 1)
