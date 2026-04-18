import unittest
from time import monotonic
from unittest.mock import AsyncMock
from unittest.mock import patch

from arbitrage_bot.services import operations_monitor


class OperationsMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        operations_monitor.reset_monitor_state()


    async def asyncTearDown(self):
        operations_monitor.reset_monitor_state()


    async def test_duplicate_markets_warning_and_recovery(self):
        with patch(
            "arbitrage_bot.services.operations_monitor.send_system_notification",
            new=AsyncMock(),
        ) as send_mock:
            await operations_monitor.record_duplicate_markets("polymarket", 60)
            await operations_monitor.record_duplicate_markets("polymarket", 0)

        self.assertEqual(send_mock.await_count, 2)
        self.assertEqual(send_mock.await_args_list[0].kwargs["level"], "warning")
        self.assertEqual(send_mock.await_args_list[1].kwargs["level"], "recovery")


    async def test_duplicate_markets_streak_warning(self):
        with patch(
            "arbitrage_bot.services.operations_monitor.send_system_notification",
            new=AsyncMock(),
        ) as send_mock:
            for _ in range(5):
                await operations_monitor.record_duplicate_markets("polymarket", 25)

        send_mock.assert_awaited_once()
        self.assertEqual(send_mock.await_args.kwargs["level"], "warning")


    async def test_orderbook_coverage_alerts_and_recovers(self):
        with patch(
            "arbitrage_bot.services.operations_monitor.send_system_notification",
            new=AsyncMock(),
        ) as send_mock:
            for _ in range(3):
                await operations_monitor.record_worker_cycle(
                    active_pairs=100,
                    pairs_with_books=80,
                    opportunities=0,
                    deliverable_opportunities=0,
                )
            await operations_monitor.record_worker_cycle(
                active_pairs=100,
                pairs_with_books=95,
                opportunities=0,
                deliverable_opportunities=0,
            )

        self.assertEqual(send_mock.await_count, 2)
        self.assertEqual(send_mock.await_args_list[0].kwargs["level"], "warning")
        self.assertEqual(send_mock.await_args_list[1].kwargs["level"], "recovery")


    async def test_deliverable_stall_escalates_and_recovers(self):
        with patch(
            "arbitrage_bot.services.operations_monitor.send_system_notification",
            new=AsyncMock(),
        ) as send_mock:
            for _ in range(10):
                await operations_monitor.record_worker_cycle(
                    active_pairs=100,
                    pairs_with_books=95,
                    opportunities=7,
                    deliverable_opportunities=0,
                )
            await operations_monitor.record_worker_cycle(
                active_pairs=100,
                pairs_with_books=95,
                opportunities=7,
                deliverable_opportunities=2,
            )

        self.assertEqual(send_mock.await_count, 3)
        self.assertEqual(send_mock.await_args_list[0].kwargs["level"], "warning")
        self.assertEqual(send_mock.await_args_list[1].kwargs["level"], "critical")
        self.assertEqual(send_mock.await_args_list[2].kwargs["level"], "recovery")


    async def test_telegram_connectivity_alerts_and_recovers(self):
        with patch(
            "arbitrage_bot.services.operations_monitor.send_system_notification",
            new=AsyncMock(return_value=True),
        ) as send_mock:
            operations_monitor.record_telegram_polling_failure(
                "Failed to fetch updates - TelegramNetworkError: HTTP Client says - ClientConnectorError: Cannot connect"
            )
            first_failure_at = operations_monitor._telegram_state["first_failure_at"]
            await operations_monitor.evaluate_telegram_connectivity(now=first_failure_at + 181.0)
            await operations_monitor.evaluate_telegram_connectivity(now=first_failure_at + 241.0)

        self.assertEqual(send_mock.await_count, 2)
        self.assertEqual(send_mock.await_args_list[0].kwargs["level"], "warning")
        self.assertEqual(send_mock.await_args_list[1].kwargs["level"], "recovery")


    async def test_telegram_connectivity_ignores_request_timeout_failures(self):
        with patch(
            "arbitrage_bot.services.operations_monitor.send_system_notification",
            new=AsyncMock(return_value=True),
        ) as send_mock:
            operations_monitor.record_telegram_polling_failure(
                "Failed to fetch updates - TelegramNetworkError: HTTP Client says - Request timeout error"
            )
            await operations_monitor.evaluate_telegram_connectivity(now=monotonic() + 600.0)

        send_mock.assert_not_awaited()
        self.assertIsNone(operations_monitor._telegram_state["first_failure_at"])
        self.assertIsNone(operations_monitor._telegram_state["last_failure_at"])


    async def test_telegram_connectivity_sends_only_recovery_when_warning_delivery_failed(self):
        with patch(
            "arbitrage_bot.services.operations_monitor.send_system_notification",
            new=AsyncMock(side_effect=[False, True]),
        ) as send_mock:
            operations_monitor.record_telegram_polling_failure(
                "Failed to fetch updates - TelegramNetworkError: HTTP Client says - ClientConnectorError: Cannot connect"
            )
            first_failure_at = operations_monitor._telegram_state["first_failure_at"]
            await operations_monitor.evaluate_telegram_connectivity(now=first_failure_at + 181.0)

            self.assertFalse(operations_monitor._telegram_state["active"])
            self.assertTrue(operations_monitor._telegram_state["outage_detected"])

            await operations_monitor.evaluate_telegram_connectivity(now=first_failure_at + 241.0)

        self.assertEqual(send_mock.await_count, 2)
        self.assertEqual(send_mock.await_args_list[0].kwargs["level"], "warning")
        self.assertEqual(send_mock.await_args_list[1].kwargs["level"], "recovery")
        self.assertFalse(operations_monitor._telegram_state["active"])
        self.assertFalse(operations_monitor._telegram_state["outage_detected"])
        self.assertIsNone(operations_monitor._telegram_state["first_failure_at"])
        self.assertIsNone(operations_monitor._telegram_state["last_failure_at"])