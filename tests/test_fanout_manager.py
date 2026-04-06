import unittest
from datetime import timedelta
from datetime import datetime
from datetime import timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

from sqlalchemy.exc import IntegrityError

from arbitrage_bot.models.orm import Alert
from arbitrage_bot.models.orm import Market
from arbitrage_bot.services import fanout_manager as fanout_manager_module
from arbitrage_bot.services.fanout_manager import FanoutManager


class FakeScalarResult:
    def __init__(self, items):
        self.items = items


    def scalars(self):
        return self


    def all(self):
        return list(self.items)


class FakeTupleResult:
    def __init__(self, row=None):
        self.row = row


    def first(self):
        return self.row


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
        self.claim_row = None
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
            return FakeTupleResult(self.claim_row)
        raise AssertionError(f"unexpected stmt: {compiled}")


class FanoutManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        fanout_manager_module._delivery_targets_cache["value"] = None
        fanout_manager_module._delivery_targets_cache["expires_at"] = 0.0


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
            created_count = await manager._fanout_opportunity(opportunity, pair, market_a, market_b)

        self.assertEqual(created_count, 1)
        self.assertEqual(db.flush_calls, 1)
        self.assertEqual(len(db.added), 1)
        self.assertIsInstance(db.added[0], Alert)
        self.assertEqual(db.added[0].telegram_chat_id, "1001")


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
            created_count = await manager._fanout_opportunity(opportunity, pair, market_a, market_b)

        self.assertEqual(created_count, 1)


    async def test_claim_pending_opportunity_marks_row_as_processing(self):
        db = FakeDbSession()
        opportunity = SimpleNamespace(
            id=7,
            fanout_status="queued",
            fanout_error_message="boom",
        )
        db.claim_row = (
            opportunity,
            SimpleNamespace(id=5),
            SimpleNamespace(id=101),
            SimpleNamespace(id=202),
        )
        manager = FanoutManager(db)

        row = await manager._claim_pending_opportunity()

        self.assertIsNotNone(row)
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
        claim_mock = AsyncMock(side_effect=[opportunities[0], opportunities[1], None])
        fanout_mock = AsyncMock(return_value=1)

        with patch.object(manager, "_claim_pending_opportunity", new=claim_mock), patch.object(
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