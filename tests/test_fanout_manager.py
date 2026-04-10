import unittest
from datetime import timedelta
from datetime import datetime
from datetime import timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

from sqlalchemy.exc import IntegrityError

from arbitrage_bot.core.observability import reset_counters
from arbitrage_bot.core.observability import snapshot_counters
from arbitrage_bot.models.orm import Alert
from arbitrage_bot.models.orm import Market
from arbitrage_bot.services.fanout_manager import FanoutManager


class FakeScalarResult:
    def __init__(self, items):
        self.items = items


    def scalars(self):
        return self


    def all(self):
        return list(self.items)


class FakeTupleResult:
    def __init__(self, rows=None):
        self.rows = list(rows or [])


    def first(self):
        return self.rows[0] if self.rows else None


    def all(self):
        return list(self.rows)


class FakeNestedTransaction:
    async def __aenter__(self):
        return self


    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeDbSession:
    def __init__(self):
        self.added = []
        self.flush_calls = 0
        self.commit_calls = 0
        self.claim_rows = None
        self.fail_on_chat_ids = set()


    def add(self, item):
        self.added.append(item)


    async def flush(self):
        self.flush_calls += 1
        if self.added and isinstance(self.added[-1], Alert):
            chat_id = self.added[-1].telegram_chat_id
            if chat_id in self.fail_on_chat_ids:
                raise IntegrityError("duplicate", None, None)


    async def commit(self):
        self.commit_calls += 1


    def begin_nested(self):
        return FakeNestedTransaction()


    async def execute(self, stmt):
        compiled = str(stmt)
        if "SELECT alerts.telegram_chat_id" in compiled:
            return FakeScalarResult([])
        if "FROM arb_opportunities" in compiled:
            return FakeTupleResult(self.claim_rows)
        raise AssertionError(f"unexpected stmt: {compiled}")


class FanoutManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        FanoutManager._cache_value = None
        FanoutManager._cache_expires_at = 0.0
        reset_counters()


    async def test_fanout_creates_alerts_only_for_eligible_targets(self):
        db = FakeDbSession()
        manager = FanoutManager(db)
        opportunity = SimpleNamespace(
            id=7,
            net_roi=0.12,
            capital_required=10.0,
        )
        pair = SimpleNamespace(id=5)
        market_a = Market(
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
        )
        market_b = Market(
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
        )

        with patch.object(
            manager,
            "_get_delivery_targets",
            new=AsyncMock(
                return_value=[
                    {
                        "user_id": 11,
                        "subscription_id": 21,
                        "telegram_chat_id": "1001",
                        "preferences": {
                            "min_roi_percent": None,
                            "max_capital_usd": None,
                            "max_days_to_close": None,
                        },
                    },
                    {
                        "user_id": 12,
                        "subscription_id": 22,
                        "telegram_chat_id": "1002",
                        "preferences": {
                            "min_roi_percent": 50.0,
                            "max_capital_usd": None,
                            "max_days_to_close": None,
                        },
                    },
                ]
            ),
        ):
            created_count = await manager._fanout_opportunity(opportunity, market_a, market_b)

        self.assertEqual(created_count, 1)
        self.assertEqual(db.flush_calls, 1)
        self.assertEqual(len(db.added), 1)
        self.assertIsInstance(db.added[0], Alert)
        self.assertEqual(db.added[0].telegram_chat_id, "1001")
        counters = snapshot_counters()
        self.assertEqual(counters["fanout.alerts_created"], 1)
        self.assertNotIn("fanout.drop.min_roi", counters)


    async def test_fanout_counts_muted_target_drop_reason(self):
        db = FakeDbSession()
        manager = FanoutManager(db)
        opportunity = SimpleNamespace(
            id=7,
            net_roi=0.12,
            capital_required=10.0,
            net_profit=5.0,
        )
        pair = SimpleNamespace(id=5)
        market_a = Market(
            id=101,
            platform="polymarket",
            platform_market_id="poly-101",
            status="active",
            tradable=True,
            title="market a",
            normalized_title="market a",
            description="",
            outcomes_json=[],
            raw_payload_json={},
            category="",
            slug="",
        )
        market_b = Market(
            id=202,
            platform="predict_fun",
            platform_market_id="pf-202",
            status="active",
            tradable=True,
            title="market b",
            normalized_title="market b",
            description="",
            outcomes_json=[],
            raw_payload_json={},
            category="",
            slug="",
        )

        with patch.object(
            manager,
            "_get_delivery_targets",
            new=AsyncMock(
                return_value=[
                    {
                        "user_id": 11,
                        "subscription_id": 21,
                        "telegram_chat_id": "1001",
                        "preferences": {"muted": True},
                    }
                ]
            ),
        ):
            created_count = await manager._fanout_opportunity(opportunity, market_a, market_b)

        self.assertEqual(created_count, 0)
        self.assertEqual(snapshot_counters()["fanout.drop.muted"], 1)


    async def test_fanout_counts_shared_drop_reason_once_per_opportunity(self):
        db = FakeDbSession()
        manager = FanoutManager(db)
        opportunity = SimpleNamespace(
            id=7,
            net_roi=0.01,
            capital_required=10.0,
            net_profit=5.0,
        )
        pair = SimpleNamespace(id=5)
        market_a = Market(
            id=101,
            platform="polymarket",
            platform_market_id="poly-101",
            status="active",
            tradable=True,
            title="market a",
            normalized_title="market a",
            description="",
            outcomes_json=[],
            raw_payload_json={},
            category="",
            slug="",
        )
        market_b = Market(
            id=202,
            platform="predict_fun",
            platform_market_id="pf-202",
            status="active",
            tradable=True,
            title="market b",
            normalized_title="market b",
            description="",
            outcomes_json=[],
            raw_payload_json={},
            category="",
            slug="",
        )

        with patch.object(
            manager,
            "_get_delivery_targets",
            new=AsyncMock(
                return_value=[
                    {
                        "user_id": 11,
                        "subscription_id": 21,
                        "telegram_chat_id": "1001",
                        "preferences": {"min_roi_percent": 5.0},
                    },
                    {
                        "user_id": 12,
                        "subscription_id": 22,
                        "telegram_chat_id": "1002",
                        "preferences": {"min_roi_percent": 5.0},
                    },
                ]
            ),
        ):
            created_count = await manager._fanout_opportunity(opportunity, market_a, market_b)

        self.assertEqual(created_count, 0)
        self.assertEqual(snapshot_counters()["fanout.drop.min_roi"], 1)


    async def test_fanout_skips_duplicate_alert_insert_without_failing_batch(self):
        db = FakeDbSession()
        db.fail_on_chat_ids.add("1002")
        manager = FanoutManager(db)
        opportunity = SimpleNamespace(
            id=7,
            net_roi=0.12,
            capital_required=10.0,
        )
        pair = SimpleNamespace(id=5)
        market_a = Market(
            id=101,
            platform="polymarket",
            platform_market_id="poly-101",
            status="active",
            tradable=True,
            title="market a",
            normalized_title="market a",
            description="",
            outcomes_json=[],
            raw_payload_json={},
            category="",
            slug="",
        )
        market_b = Market(
            id=202,
            platform="predict_fun",
            platform_market_id="pf-202",
            status="active",
            tradable=True,
            title="market b",
            normalized_title="market b",
            description="",
            outcomes_json=[],
            raw_payload_json={},
            category="",
            slug="",
        )

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
                        "preferences": {},
                    },
                ]
            ),
        ):
            created_count = await manager._fanout_opportunity(opportunity, market_a, market_b)

        self.assertEqual(created_count, 1)


    async def test_claim_pending_opportunities_marks_rows_as_processing(self):
        db = FakeDbSession()
        opportunity = SimpleNamespace(id=7, fanout_status="queued", fanout_error_message="boom")
        db.claim_rows = [
            (
                opportunity,
                SimpleNamespace(id=5),
                SimpleNamespace(id=101),
                SimpleNamespace(id=202),
            )
        ]
        manager = FanoutManager(db)

        rows = await manager._claim_pending_opportunities(limit=10)

        self.assertEqual(len(rows), 1)
        self.assertEqual(opportunity.fanout_status, "processing")
        self.assertIsNone(opportunity.fanout_error_message)


    async def test_get_delivery_targets_reuses_short_ttl_cache(self):
        db = FakeDbSession()
        manager = FanoutManager(db)
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


    async def test_process_pending_opportunities_fetches_delivery_targets_once_per_batch(self):
        db = FakeDbSession()
        manager = FanoutManager(db)
        opportunities = [
            (
                SimpleNamespace(id=7, fanout_status="queued", fanout_error_message=None),
                SimpleNamespace(id=5),
                SimpleNamespace(id=101),
                SimpleNamespace(id=202),
            ),
            (
                SimpleNamespace(id=8, fanout_status="queued", fanout_error_message=None),
                SimpleNamespace(id=6),
                SimpleNamespace(id=103),
                SimpleNamespace(id=204),
            ),
        ]
        claim_mock = AsyncMock(return_value=opportunities)
        fanout_mock = AsyncMock(return_value=1)

        with patch.object(manager, "_claim_pending_opportunities", new=claim_mock), patch.object(
            manager,
            "_fanout_opportunity",
            new=fanout_mock,
        ), patch.object(
            manager,
            "_get_delivery_targets",
            new=AsyncMock(return_value=[{"telegram_chat_id": "1001", "preferences": {}}]),
        ) as targets_mock:
            processed_count = await manager.process_pending_opportunities(limit=10)

        self.assertEqual(processed_count, 2)
        self.assertEqual(targets_mock.await_count, 1)
        self.assertEqual(db.commit_calls, 1)


    async def test_create_alert_deliveries_skips_existing_lookup_for_fresh_opportunity(self):
        db = FakeDbSession()
        db.execute = AsyncMock(side_effect=AssertionError("unexpected existing alert lookup"))
        manager = FanoutManager(db)
        opportunity = SimpleNamespace(
            id=7,
            net_roi=0.12,
            capital_required=10.0,
            net_profit=5.0,
        )
        market_a = Market(
            id=101,
            platform="polymarket",
            platform_market_id="poly-101",
            status="active",
            tradable=True,
            title="market a",
            normalized_title="market a",
            description="",
            outcomes_json=[],
            raw_payload_json={},
            category="",
            slug="",
        )
        market_b = Market(
            id=202,
            platform="predict_fun",
            platform_market_id="pf-202",
            status="active",
            tradable=True,
            title="market b",
            normalized_title="market b",
            description="",
            outcomes_json=[],
            raw_payload_json={},
            category="",
            slug="",
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
                    "preferences": {},
                }
            ],
            skip_existing_lookup=True,
        )

        self.assertEqual(len(deliveries), 1)
        self.assertEqual(db.flush_calls, 1)


class TelegramDeliveryRetryTests(unittest.TestCase):
    def test_mark_alert_retry_sets_retry_with_backoff(self):
        from arbitrage_bot.tg_bot.bot import _mark_alert_retry

        alert = SimpleNamespace(
            status="queued",
            attempt_count=0,
            next_retry_at=None,
            error_message=None,
        )
        now = datetime(2026, 4, 3, tzinfo=timezone.utc)

        with patch("arbitrage_bot.tg_bot.bot.settings.TELEGRAM_DELIVERY_MAX_ATTEMPTS", 3), patch(
            "arbitrage_bot.tg_bot.bot.settings.TELEGRAM_DELIVERY_RETRY_SECONDS",
            15.0,
        ):
            _mark_alert_retry(alert, RuntimeError("boom"), now)

        self.assertEqual(alert.status, "retry")
        self.assertEqual(alert.attempt_count, 1)
        self.assertEqual(alert.next_retry_at, now + timedelta(seconds=15))
        self.assertEqual(alert.error_message, "boom")


    def test_mark_alert_retry_marks_failed_after_limit(self):
        from arbitrage_bot.tg_bot.bot import _mark_alert_retry

        alert = SimpleNamespace(
            status="retry",
            attempt_count=2,
            next_retry_at=None,
            error_message=None,
        )
        now = datetime(2026, 4, 3, tzinfo=timezone.utc)

        with patch("arbitrage_bot.tg_bot.bot.settings.TELEGRAM_DELIVERY_MAX_ATTEMPTS", 3):
            _mark_alert_retry(alert, RuntimeError("boom"), now)

        self.assertEqual(alert.status, "failed")
        self.assertEqual(alert.attempt_count, 3)
        self.assertIsNone(alert.next_retry_at)