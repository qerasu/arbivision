import asyncio
import unittest
from unittest.mock import AsyncMock
from unittest.mock import patch

from arbitrage_bot.core import redis as redis_module


class RedisConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        await redis_module.close_redis()


    async def test_init_retries_after_startup_failure(self):
        client = AsyncMock()
        from_url = AsyncMock(
            side_effect=[
                ConnectionError("redis unavailable"),
                client,
            ]
        )

        with patch.object(redis_module.aioredis, "from_url", new=from_url), patch.object(
            redis_module,
            "_REDIS_RETRY_SECONDS",
            0,
        ):
            result = await redis_module.init_redis()
            for _ in range(5):
                await asyncio.sleep(0)

        self.assertIsNone(result)
        self.assertIs(redis_module.get_redis(), client)
        self.assertEqual(from_url.await_count, 2)
        client.ping.assert_awaited_once()
