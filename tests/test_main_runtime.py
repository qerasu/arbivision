import unittest
from contextlib import asynccontextmanager
from unittest.mock import Mock
from unittest.mock import patch

from arbitrage_bot import main as main_module


class MainRuntimeModeTests(unittest.IsolatedAsyncioTestCase):
    async def _collect_runtime_targets(self, mode):
        captured = []

        @asynccontextmanager
        async def fake_managed_runtime(*coroutines):
            captured.extend(coroutines)
            yield

        with patch.object(main_module.settings, "APP_RUNTIME_MODE", mode), patch(
            "arbitrage_bot.main.run_worker_runtime",
            new=Mock(return_value="worker"),
        ), patch(
            "arbitrage_bot.main.run_fanout_runtime",
            new=Mock(return_value="fanout"),
        ), patch(
            "arbitrage_bot.main.run_telegram_runtime",
            new=Mock(return_value="telegram"),
        ), patch(
            "arbitrage_bot.main.managed_runtime",
            new=fake_managed_runtime,
        ):
            async with main_module.lifespan(None):
                pass

        return captured


    async def test_lifespan_starts_worker_and_telegram_in_all_mode(self):
        coroutines = await self._collect_runtime_targets("all")

        self.assertEqual(coroutines, ["worker", "telegram"])


    async def test_lifespan_starts_only_worker_in_worker_mode(self):
        coroutines = await self._collect_runtime_targets("worker")

        self.assertEqual(coroutines, ["worker"])


    async def test_lifespan_starts_only_fanout_in_fanout_mode(self):
        coroutines = await self._collect_runtime_targets("fanout")

        self.assertEqual(coroutines, ["fanout"])


    async def test_lifespan_starts_only_telegram_in_telegram_mode(self):
        coroutines = await self._collect_runtime_targets("telegram")

        self.assertEqual(coroutines, ["telegram"])