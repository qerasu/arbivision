import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

from arbitrage_bot.core.observability import reset_counters
from arbitrage_bot.core.observability import snapshot_counters
from arbitrage_bot.models.orm import Market
from arbitrage_bot.services.fanout_manager import FanoutManager


class FakeDbSession:
    pass


class FanoutManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        FanoutManager._cache_value = None
        FanoutManager._cache_expires_at = 0.0
        reset_counters()


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


    async def test_fanout_creates_alerts_only_for_eligible_targets(self):
        manager = FanoutManager(FakeDbSession())
        opportunity = SimpleNamespace(
            pair_hash="pair-1",
            direction="A_yes_B_no",
            message_hash="hash-1",
            net_roi=0.12,
            capital_required=10.0,
            net_profit=5.0,
        )
        market_a, market_b = self._build_markets()

        with patch.object(
            manager,
            "_get_delivery_targets",
            new=AsyncMock(
                return_value=[
                    {
                        "user_id": 11,
                        "subscription_id": 21,
                        "telegram_chat_id": "1001",
                        "preferences": {},
                    },
                    {
                        "user_id": 12,
                        "subscription_id": 22,
                        "telegram_chat_id": "1002",
                        "preferences": {"min_roi_percent": 50.0},
                    },
                ]
            ),
        ):
            deliveries = await manager.create_alert_deliveries(opportunity, market_a, market_b)

        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["alert"].telegram_chat_id, "1001")
        self.assertEqual(deliveries[0]["alert"].message_hash, "hash-1")
        counters = snapshot_counters()
        self.assertEqual(counters["fanout.alerts_created"], 1)


    async def test_fanout_counts_muted_target_drop_reason(self):
        manager = FanoutManager(FakeDbSession())
        opportunity = SimpleNamespace(
            pair_hash="pair-1",
            direction="A_yes_B_no",
            message_hash="hash-1",
            net_roi=0.12,
            capital_required=10.0,
            net_profit=5.0,
        )
        market_a, market_b = self._build_markets()

        with patch.object(
            manager,
            "_get_delivery_targets",
            new=AsyncMock(
                return_value=[
                    {
                        "telegram_chat_id": "1001",
                        "preferences": {"muted": True},
                    }
                ]
            ),
        ):
            deliveries = await manager.create_alert_deliveries(opportunity, market_a, market_b)

        self.assertEqual(deliveries, [])
        self.assertEqual(snapshot_counters()["fanout.drop.muted"], 1)


    async def test_get_delivery_targets_reuses_short_ttl_cache(self):
        manager = FanoutManager(FakeDbSession())
        targets = [
            {
                "user_id": 11,
                "subscription_id": 21,
                "telegram_chat_id": "1001",
                "preferences": {},
            }
        ]

        with patch(
            "arbitrage_bot.services.fanout_manager.get_telegram_alert_targets",
            new=AsyncMock(return_value=targets),
        ) as targets_mock:
            first = await manager._get_delivery_targets()
            second = await manager._get_delivery_targets()

        self.assertEqual(first, targets)
        self.assertEqual(second, targets)
        self.assertEqual(targets_mock.await_count, 1)


    async def test_create_alert_deliveries_uses_target_specific_recalculation(self):
        manager = FanoutManager(FakeDbSession())
        opportunity = SimpleNamespace(
            pair_hash="pair-1",
            direction="A_yes_B_no",
            message_hash="hash-1",
            net_roi=0.12,
            capital_required=10.0,
            net_profit=5.0,
            avg_price_leg_1=0.4,
            avg_price_leg_2=0.5,
            shares=10.0,
            gross_profit=5.0,
            gross_roi=0.1,
            calculation_json=None,
        )
        market_a, market_b = self._build_markets()
        calculator = SimpleNamespace(
            calculate_opportunities=lambda directions, max_capital=None, max_polymarket_capital=None, max_predict_fun_capital=None: [
                {
                    "direction": "A_yes_B_no",
                    "avg_price_leg_1": 0.41,
                    "avg_price_leg_2": 0.52,
                    "shares": 5.0,
                    "capital_required": 4.65,
                    "gross_profit": 0.35,
                    "net_profit": 0.35,
                    "gross_roi": 0.075,
                    "net_roi": 0.075,
                }
            ]
        )

        deliveries = await manager.create_alert_deliveries(
            opportunity,
            market_a,
            market_b,
            delivery_targets=[
                {
                    "user_id": 11,
                    "subscription_id": 21,
                    "telegram_chat_id": "1001",
                    "preferences": {"max_capital_usd": 4.65},
                }
            ],
            directions={"A_yes_B_no": {"poly": [(0.41, 12)], "pf": [(0.52, 12)]}},
            calculator=calculator,
        )

        self.assertEqual(len(deliveries), 1)
        self.assertAlmostEqual(deliveries[0]["opportunity"].capital_required, 4.65)
        self.assertAlmostEqual(deliveries[0]["opportunity"].shares, 5.0)
