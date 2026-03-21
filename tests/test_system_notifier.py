import unittest
from unittest.mock import AsyncMock, patch

from arbitrage_bot.core.config import settings
from arbitrage_bot.services import system_notifier
from arbitrage_bot.services.ingestion import IngestionService


class FakeDbSession:


    def __init__(self):
        self.rollback_calls = 0
        self.commit_calls = 0


    async def rollback(self):
        self.rollback_calls += 1


    async def commit(self):
        self.commit_calls += 1


class SystemNotifierTests(unittest.IsolatedAsyncioTestCase):


    async def asyncSetUp(self):
        system_notifier._last_sent_at.clear()
        self.original_token = settings.TELEGRAM_BOT_TOKEN
        self.original_default_chat_ids = settings.TELEGRAM_DEFAULT_CHAT_IDS
        self.original_error_chat_ids = settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS
        self.original_cooldown = settings.TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS

        settings.TELEGRAM_BOT_TOKEN = "token"
        settings.TELEGRAM_DEFAULT_CHAT_IDS = ["1001"]
        settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS = []
        settings.TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS = 300.0


    async def asyncTearDown(self):
        settings.TELEGRAM_BOT_TOKEN = self.original_token
        settings.TELEGRAM_DEFAULT_CHAT_IDS = self.original_default_chat_ids
        settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS = self.original_error_chat_ids
        settings.TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS = self.original_cooldown
        system_notifier._last_sent_at.clear()


    async def test_sends_system_error_message_to_telegram(self):
        fake_bot = AsyncMock()
        fake_bot.session.close = AsyncMock()

        with patch("arbitrage_bot.services.system_notifier.Bot", return_value=fake_bot):
            sent = await system_notifier.send_system_error_notification(
                "polymarket",
                "markets sync",
                RuntimeError("boom"),
            )

        self.assertTrue(sent)
        fake_bot.send_message.assert_awaited_once()
        _, kwargs = fake_bot.send_message.await_args
        self.assertEqual(kwargs["chat_id"], "1001")
        self.assertIn("system error", kwargs["text"])
        self.assertIn("source: polymarket", kwargs["text"])
        self.assertIn("operation: markets sync", kwargs["text"])
        self.assertIn("details: boom", kwargs["text"])
        fake_bot.session.close.assert_awaited_once()


    async def test_skips_duplicate_error_during_cooldown(self):
        fake_bot = AsyncMock()
        fake_bot.session.close = AsyncMock()

        with patch("arbitrage_bot.services.system_notifier.Bot", return_value=fake_bot):
            first = await system_notifier.send_system_error_notification(
                "worker",
                "sync loop",
                RuntimeError("boom"),
            )
            second = await system_notifier.send_system_error_notification(
                "worker",
                "sync loop",
                RuntimeError("boom"),
            )

        self.assertTrue(first)
        self.assertFalse(second)
        fake_bot.send_message.assert_awaited_once()


    async def test_strips_httpx_reference_link_from_details(self):
        fake_bot = AsyncMock()
        fake_bot.session.close = AsyncMock()

        error = RuntimeError(
            "Client error '400 Bad Request' for url 'https://api.example.com/test'\n"
            "For more information check: https://example.com/docs/400"
        )

        with patch("arbitrage_bot.services.system_notifier.Bot", return_value=fake_bot):
            sent = await system_notifier.send_system_error_notification(
                "predict.fun",
                "markets sync",
                error,
            )

        self.assertTrue(sent)
        _, kwargs = fake_bot.send_message.await_args
        self.assertIn("details: Client error '400 Bad Request' for url 'https://api.example.com/test'", kwargs["text"])
        self.assertNotIn("For more information check:", kwargs["text"])


    def test_compacts_sqlalchemy_style_error_details(self):
        error = RuntimeError(
            "insert failed [SQL: INSERT INTO markets VALUES (...)] "
            "[parameters: ('a', 'b', 'c')] "
            "(Background on this error at: https://sqlalche.me/e/20/dbapi)"
        )

        details = system_notifier.format_error_details(error)

        self.assertEqual(details, "insert failed")


    async def test_ingestion_reports_source_error_to_telegram(self):
        db = FakeDbSession()
        service = IngestionService(db)
        service.polymarket.fetch_markets = AsyncMock(side_effect=RuntimeError("gamma down"))
        service.predict_fun.fetch_markets = AsyncMock(return_value=[])
        service.polymarket.close = AsyncMock()
        service.predict_fun.close = AsyncMock()

        with patch(
            "arbitrage_bot.services.ingestion.send_system_error_notification",
            new=AsyncMock(),
        ) as send_mock:
            await service.sync_markets()

        send_mock.assert_awaited_once()
        _, args, kwargs = send_mock.mock_calls[0]
        self.assertEqual(args[0], "polymarket")
        self.assertEqual(args[1], "markets sync")
        self.assertEqual(str(args[2]), "gamma down")
        self.assertEqual(db.rollback_calls, 1)
