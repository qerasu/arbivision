import unittest
from time import monotonic
from unittest.mock import patch

from arbitrage_bot.services import operations_monitor


class OperationsMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        operations_monitor.reset_monitor_state()


    async def asyncTearDown(self):
        operations_monitor.reset_monitor_state()


    async def test_duplicate_markets_warning_and_recovery_logs_locally_without_telegram(self):
        with patch("arbitrage_bot.services.operations_monitor.log.warning") as warning_mock, patch(
            "arbitrage_bot.services.operations_monitor.log.info"
        ) as info_mock:
            await operations_monitor.record_duplicate_markets("polymarket", 60)
            await operations_monitor.record_duplicate_markets("polymarket", 0)

        warning_mock.assert_called_once()
        info_mock.assert_called_once()
        self.assertEqual(warning_mock.call_args.kwargs["monitor_level"], "warning")
        self.assertEqual(info_mock.call_args.kwargs["monitor_level"], "recovery")


    async def test_duplicate_markets_critical_logs_locally_without_telegram(self):
        with patch("arbitrage_bot.services.operations_monitor.log.critical") as critical_mock:
            await operations_monitor.record_duplicate_markets("polymarket", 239)

        critical_mock.assert_called_once()
        self.assertEqual(critical_mock.call_args.kwargs["monitor_level"], "critical")
        self.assertEqual(critical_mock.call_args.kwargs["duplicate_rows"], 239)


    async def test_duplicate_markets_streak_warning(self):
        with patch("arbitrage_bot.services.operations_monitor.log.warning") as warning_mock:
            for _ in range(5):
                await operations_monitor.record_duplicate_markets("polymarket", 25)

        warning_mock.assert_called_once()
        self.assertEqual(warning_mock.call_args.kwargs["monitor_level"], "warning")


    async def test_orderbook_coverage_updates_stats_without_telegram_alerts(self):
        for _ in range(3):
            await operations_monitor.record_worker_cycle(
                active_pairs=100,
                pairs_with_books=80,
                opportunities=0,
                deliverable_opportunities=0,
            )
        degraded = operations_monitor.snapshot_monitor_state()["orderbook_coverage"]
        await operations_monitor.record_worker_cycle(
            active_pairs=100,
            pairs_with_books=95,
            opportunities=0,
            deliverable_opportunities=0,
        )

        recovered = operations_monitor.snapshot_monitor_state()["orderbook_coverage"]
        self.assertEqual(degraded["severity"], "warning")
        self.assertEqual(degraded["active_pairs"], 100)
        self.assertEqual(degraded["pairs_with_books"], 80)
        self.assertEqual(recovered["severity"], None)
        self.assertEqual(recovered["ratio"], 0.95)


    async def test_deliverable_stall_updates_stats_without_telegram_alerts(self):
        for _ in range(5):
            await operations_monitor.record_worker_cycle(
                active_pairs=100,
                pairs_with_books=95,
                opportunities=7,
                deliverable_opportunities=0,
            )
        warning = operations_monitor.snapshot_monitor_state()["deliverable_opportunities"]
        for _ in range(5):
            await operations_monitor.record_worker_cycle(
                active_pairs=100,
                pairs_with_books=95,
                opportunities=7,
                deliverable_opportunities=0,
            )
        critical = operations_monitor.snapshot_monitor_state()["deliverable_opportunities"]
        await operations_monitor.record_worker_cycle(
            active_pairs=100,
            pairs_with_books=95,
            opportunities=7,
            deliverable_opportunities=2,
        )

        recovered = operations_monitor.snapshot_monitor_state()["deliverable_opportunities"]
        self.assertEqual(warning["severity"], "warning")
        self.assertEqual(warning["streak"], 5)
        self.assertEqual(critical["severity"], "critical")
        self.assertEqual(critical["streak"], 10)
        self.assertEqual(recovered["severity"], None)
        self.assertEqual(recovered["deliverable_opportunities"], 2)


    async def test_telegram_connectivity_updates_stats_and_recovers_without_telegram_notifications(self):
        operations_monitor.record_telegram_polling_failure(
            "Failed to fetch updates - TelegramNetworkError: HTTP Client says - ClientConnectorError: Cannot connect"
        )
        first_failure_at = operations_monitor._telegram_state["first_failure_at"]
        await operations_monitor.evaluate_telegram_connectivity(now=first_failure_at + 181.0)
        degraded = operations_monitor.snapshot_monitor_state()["telegram_polling"]
        await operations_monitor.evaluate_telegram_connectivity(now=first_failure_at + 241.0)

        recovered = operations_monitor.snapshot_monitor_state()["telegram_polling"]
        self.assertEqual(degraded["severity"], "warning")
        self.assertTrue(degraded["outage_detected"])
        self.assertEqual(recovered["severity"], "ok")
        self.assertFalse(recovered["outage_detected"])


    async def test_telegram_connectivity_ignores_request_timeout_failures(self):
        operations_monitor.record_telegram_polling_failure(
            "Failed to fetch updates - TelegramNetworkError: HTTP Client says - Request timeout error"
        )
        await operations_monitor.evaluate_telegram_connectivity(now=monotonic() + 600.0)

        self.assertIsNone(operations_monitor._telegram_state["first_failure_at"])
        self.assertIsNone(operations_monitor._telegram_state["last_failure_at"])


    async def test_telegram_connectivity_transitions_state_without_notifications(self):
        operations_monitor.record_telegram_polling_failure(
            "Failed to fetch updates - TelegramNetworkError: HTTP Client says - ClientConnectorError: Cannot connect"
        )
        first_failure_at = operations_monitor._telegram_state["first_failure_at"]
        await operations_monitor.evaluate_telegram_connectivity(now=first_failure_at + 181.0)

        self.assertTrue(operations_monitor._telegram_state["active"])
        self.assertTrue(operations_monitor._telegram_state["outage_detected"])

        await operations_monitor.evaluate_telegram_connectivity(now=first_failure_at + 241.0)

        self.assertFalse(operations_monitor._telegram_state["active"])
        self.assertFalse(operations_monitor._telegram_state["outage_detected"])
        self.assertIsNone(operations_monitor._telegram_state["first_failure_at"])
        self.assertIsNone(operations_monitor._telegram_state["last_failure_at"])
