import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

import httpx
from fastapi import HTTPException
from arbitrage_bot.api.internal import health_check
from arbitrage_bot.api.internal import status_check
from arbitrage_bot.core.database import get_db
from arbitrage_bot.main import app


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
    async def asyncTearDown(self):
        app.dependency_overrides.clear()


    async def test_health_check_returns_ok(self):
        with patch("arbitrage_bot.api.internal.get_redis", return_value=None):
            payload = await health_check(db=FakeDb([1]))

        self.assertEqual(
            payload,
            {"status": "ok", "database": "ok", "redis": "degraded"},
        )


    async def test_health_check_pings_redis(self):
        redis = SimpleNamespace(ping=AsyncMock(return_value=True))

        with patch("arbitrage_bot.api.internal.get_redis", return_value=redis):
            payload = await health_check(db=FakeDb([1]))

        self.assertEqual(payload["redis"], "ok")
        redis.ping.assert_awaited_once()


    async def test_health_check_degrades_when_redis_ping_fails(self):
        redis = SimpleNamespace(ping=AsyncMock(side_effect=RuntimeError("down")))

        with patch("arbitrage_bot.api.internal.get_redis", return_value=redis):
            payload = await health_check(db=FakeDb([1]))

        self.assertEqual(payload["redis"], "degraded")


    async def test_health_check_returns_503_when_database_is_unavailable(self):
        db = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("down")))

        with self.assertRaises(HTTPException) as raised:
            await health_check(db=db)

        self.assertEqual(raised.exception.status_code, 503)


    async def test_status_check_returns_compact_runtime_summary(self):
        db = FakeDb(
            [
                SimpleNamespace(total=100, active=42),
                SimpleNamespace(total=12, approved=5),
            ]
        )

        with patch(
            "arbitrage_bot.api.internal.snapshot_counters",
            return_value={
                "worker.opportunities_created": 3,
                "fanout.opportunity_filtered_all_targets": 1,
                "telegram.alert_sent": 2,
            },
        ):
            payload = await status_check(db=db)

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "arbitrage-alert-bot")
        self.assertEqual(payload["market_counts"]["total"], 100)
        self.assertEqual(payload["market_counts"]["active"], 42)
        self.assertEqual(payload["pair_counts"]["total"], 12)
        self.assertEqual(payload["pair_counts"]["approved"], 5)
        self.assertEqual(payload["opportunity_counts"]["total"], 3)
        self.assertEqual(payload["opportunity_counts"]["filtered_runtime"], 1)
        self.assertEqual(payload["alert_counts"]["sent_runtime"], 2)
        self.assertNotIn("runtime_metrics", payload)


    async def test_status_requires_configured_bearer_token(self):
        app.dependency_overrides[get_db] = lambda: FakeDb(
            [
                SimpleNamespace(total=100, active=42),
                SimpleNamespace(total=12, approved=5),
            ]
        )
        transport = httpx.ASGITransport(app=app)

        with patch("arbitrage_bot.api.internal.settings.ADMIN_API_TOKEN", "secret"):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                missing = await client.get("/api/status")
                invalid = await client.get(
                    "/api/status",
                    headers={"Authorization": "Bearer wrong"},
                )
                valid = await client.get(
                    "/api/status",
                    headers={"Authorization": "Bearer secret"},
                )

        self.assertEqual(missing.status_code, 401)
        self.assertEqual(invalid.status_code, 401)
        self.assertEqual(valid.status_code, 200)


    async def test_status_fails_closed_without_configured_token(self):
        transport = httpx.ASGITransport(app=app)

        with patch("arbitrage_bot.api.internal.settings.ADMIN_API_TOKEN", ""):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/api/status")

        self.assertEqual(response.status_code, 503)
