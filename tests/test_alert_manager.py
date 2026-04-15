import unittest
from types import SimpleNamespace
from unittest.mock import patch

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


class AlertManagerTests(unittest.IsolatedAsyncioTestCase):
    def _build_calc_result(self, *, net_profit=7.5, net_roi=0.12):
        return {
            "direction": "A_yes_B_no",
            "avg_price_leg_1": 0.40,
            "avg_price_leg_2": 0.50,
            "shares": 10.0,
            "capital_required": 9.0,
            "gross_profit": 1.0,
            "net_profit": net_profit,
            "gross_roi": 0.11,
            "net_roi": net_roi,
        }


    async def test_creates_runtime_opportunity_and_updates_dedupe_cache_on_finalize(self):
        redis = FakeRedis()
        manager = AlertManager(db_session=None)
        pair = SimpleNamespace(id=7, pair_hash="pair-123")

        with patch("arbitrage_bot.services.alert_manager.get_redis", return_value=redis):
            opportunity = await manager.process_opportunity(pair, self._build_calc_result())
            await manager.finalize_opportunity(opportunity)

        self.assertEqual(opportunity.market_pair_id, 7)
        self.assertEqual(opportunity.direction, "A_yes_B_no")
        self.assertTrue(opportunity.message_hash)
        self.assertEqual(len(redis.setex_calls), 1)


    async def test_skips_opportunity_when_profit_deltas_are_below_threshold(self):
        redis = FakeRedis(
            {
                "alert-dedupe:pair-123:A_yes_B_no": (
                    '{"net_profit": 10.0, "net_roi": 0.2, "shares": 5.0}'
                )
            }
        )
        manager = AlertManager(db_session=None)
        pair = SimpleNamespace(id=7, pair_hash="pair-123")

        with patch("arbitrage_bot.services.alert_manager.get_redis", return_value=redis):
            result = await manager.process_opportunity(
                pair,
                self._build_calc_result(net_profit=11.0, net_roi=0.201),
            )

        self.assertFalse(result)
        self.assertEqual(redis.setex_calls, [])


    async def test_uses_stable_message_hash_for_same_payload(self):
        redis = FakeRedis()
        manager = AlertManager(db_session=None)
        pair = SimpleNamespace(id=7, pair_hash="pair-123")

        with patch("arbitrage_bot.services.alert_manager.get_redis", return_value=redis):
            first = await manager.process_opportunity(pair, self._build_calc_result(net_profit=12.0, net_roi=0.22))
            second = await manager.process_opportunity(pair, self._build_calc_result(net_profit=12.0, net_roi=0.22))

        self.assertEqual(first.message_hash, second.message_hash)


    async def test_finalize_without_metadata_is_noop(self):
        redis = FakeRedis()
        manager = AlertManager(db_session=None)
        opportunity = SimpleNamespace(direction="A_yes_B_no")

        with patch("arbitrage_bot.services.alert_manager.get_redis", return_value=redis):
            await manager.finalize_opportunity(opportunity)

        self.assertEqual(redis.setex_calls, [])
