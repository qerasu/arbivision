import asyncio
import redis.asyncio as aioredis
from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger

log = get_logger("redis")

_redis_client = None
_redis_init_attempted = False
_redis_lock = asyncio.Lock()


async def init_redis():
    global _redis_client, _redis_init_attempted

    async with _redis_lock:
        if _redis_init_attempted:
            return _redis_client

        _redis_init_attempted = True

        try:
            _redis_client = await aioredis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=10
            )
            await _redis_client.ping()
            log.info("redis connection initialized", url=settings.redis_url)
            return _redis_client
        except Exception as exc:
            log.warning(
                "redis connection failed, service will run with degraded functionality",
                error=str(exc),
            )
            _redis_client = None
            return None


async def close_redis():
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
        log.info("redis connection closed")


def get_redis():
    return _redis_client