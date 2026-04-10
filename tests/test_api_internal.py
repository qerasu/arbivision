import unittest
from types import SimpleNamespace

from arbitrage_bot.api.internal import health_check
from arbitrage_bot.api.internal import status_check


class FakeResult:
    def __init__(self, row):
        self._row = row


    def one(self):
        return self._row


    def scalar_one(self):
        return self._row


class FakeDb:
    def __init__(self, rows):
        self._rows = iter(rows)


    async def execute(self, _stmt):
        return FakeResult(next(self._rows))


class InternalApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_health_check_returns_ok(self):
        payload = await health_check()

        self.assertEqual(payload, {"status": "ok"})


    async def test_status_check_returns_compact_runtime_summary(self):
        db = FakeDb(
            [
                SimpleNamespace(total=100, active=42),
                SimpleNamespace(total=12, approved=5),
                3,
                1,
                2,
            ]
        )

        payload = await status_check(db=db)

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "arbitrage-alert-bot")
        self.assertEqual(payload["market_counts"]["total"], 100)
        self.assertEqual(payload["market_counts"]["active"], 42)
        self.assertEqual(payload["pair_counts"]["total"], 12)
        self.assertEqual(payload["pair_counts"]["approved"], 5)
        self.assertEqual(payload["opportunity_counts"]["total"], 3)
        self.assertEqual(payload["opportunity_counts"]["queued_fanout"], 1)
        self.assertEqual(payload["alert_counts"]["queued"], 2)
        self.assertNotIn("runtime_metrics", payload)
