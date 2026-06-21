import unittest
from unittest.mock import AsyncMock, MagicMock, patch

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
        system_notifier._shared_bot = None
        self.original_token = settings.TELEGRAM_BOT_TOKEN
        self.original_default_chat_ids = settings.TELEGRAM_DEFAULT_CHAT_IDS
        self.original_error_chat_ids = settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS
        self.original_cooldown = settings.TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS

        settings.TELEGRAM_BOT_TOKEN = "token"
        settings.TELEGRAM_DEFAULT_CHAT_IDS = frozenset({"1001"})
        settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS = frozenset()
        settings.TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS = 300.0
        
        self.redis_patcher = patch("arbitrage_bot.services.system_notifier.get_redis", new=MagicMock(return_value=None))
        self.redis_patcher.start()


    async def asyncTearDown(self):
        self.redis_patcher.stop()
        settings.TELEGRAM_BOT_TOKEN = self.original_token
        settings.TELEGRAM_DEFAULT_CHAT_IDS = self.original_default_chat_ids
        settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS = self.original_error_chat_ids
        settings.TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS = self.original_cooldown
        system_notifier._last_sent_at.clear()
        system_notifier._shared_bot = None


    async def test_sends_system_error_message_to_telegram(self):
        fake_bot = AsyncMock()

        with patch.object(system_notifier, "_get_shared_bot", return_value=fake_bot), patch(
            "arbitrage_bot.services.system_notifier.get_redis",
            new=MagicMock(return_value=None),
        ):
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


    async def test_skips_duplicate_error_during_cooldown(self):
        fake_bot = AsyncMock()

        fake_redis = AsyncMock()
        fake_redis.set = AsyncMock(side_effect=[True, None])

        with patch.object(system_notifier, "_get_shared_bot", return_value=fake_bot), patch(
            "arbitrage_bot.services.system_notifier.get_redis",
            new=MagicMock(return_value=fake_redis),
        ):
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
        self.assertEqual(fake_redis.set.await_count, 2)


    async def test_falls_back_to_memory_dedupe_when_redis_fails(self):
        fake_bot = AsyncMock()

        with patch.object(system_notifier, "_get_shared_bot", return_value=fake_bot), patch(
            "arbitrage_bot.services.system_notifier.get_redis",
            new=MagicMock(side_effect=RuntimeError("redis down")),
        ):
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

        error = RuntimeError(
            "Client error '400 Bad Request' for url 'https://api.example.com/test'\n"
            "For more information check: https://example.com/docs/400"
        )

        with patch.object(system_notifier, "_get_shared_bot", return_value=fake_bot):
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


    def test_uses_traceback_location_when_error_message_is_empty(self):
        def trigger_not_implemented():
            raise NotImplementedError()

        try:
            trigger_not_implemented()
        except NotImplementedError as error:
            details = system_notifier.format_error_details(error)

        self.assertIn("raised at", details)
        self.assertIn("in trigger_not_implemented", details)


    def test_detects_transient_network_error_from_ssl_record_layer_failure(self):
        error = RuntimeError(
            "ClientOSError: [Errno 1] [SSL: RECORD_LAYER_FAILURE] record layer failure (_ssl.c:2710)"
        )

        self.assertTrue(system_notifier.is_transient_network_error(error))


    def test_does_not_mark_generic_runtime_error_as_transient_network_issue(self):
        self.assertFalse(system_notifier.is_transient_network_error(RuntimeError("gamma down")))


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


    async def test_ingestion_skips_system_notification_for_transient_network_error(self):
        db = FakeDbSession()
        service = IngestionService(db)
        service.polymarket.fetch_markets = AsyncMock(
            side_effect=RuntimeError(
                "SSLError: [SSL: RECORD_LAYER_FAILURE] record layer failure (_ssl.c:2710)"
            )
        )
        service.predict_fun.fetch_markets = AsyncMock(return_value=[])
        service.polymarket.close = AsyncMock()
        service.predict_fun.close = AsyncMock()

        with patch(
            "arbitrage_bot.services.ingestion.send_system_error_notification",
            new=AsyncMock(),
        ) as send_mock:
            await service.sync_markets()

        send_mock.assert_not_awaited()
        self.assertEqual(db.rollback_calls, 1)
