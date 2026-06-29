import asyncio
import redis.asyncio as aioredis
from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger

log = get_logger("redis")

_REDIS_RETRY_SECONDS = 5
_redis_client = None
_redis_retry_task = None
_redis_lock = asyncio.Lock()


async def init_redis():
    global _redis_client, _redis_retry_task

    async with _redis_lock:
        if _redis_client is not None:
            return _redis_client

        client = None
        try:
            client = await aioredis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=10
            )
            await client.ping()
            _redis_client = client
            log.info("redis connection initialized", url=settings.redis_url)
            return _redis_client
        except Exception as exc:
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass
            log.warning(
                "redis connection failed, service will run with degraded functionality",
                error=str(exc),
            )
            _redis_client = None
            if _redis_retry_task is None or _redis_retry_task.done():
                _redis_retry_task = asyncio.create_task(_retry_redis())
            return None


async def _retry_redis():
    global _redis_retry_task

    try:
        while _redis_client is None:
            await asyncio.sleep(_REDIS_RETRY_SECONDS)
            await init_redis()
    finally:
        _redis_retry_task = None


async def close_redis():
    global _redis_client, _redis_retry_task

    if _redis_retry_task is not None:
        retry_task = _redis_retry_task
        _redis_retry_task = None
        retry_task.cancel()
        await asyncio.gather(retry_task, return_exceptions=True)

    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
        log.info("redis connection closed")


def get_redis():
    return _redis_client
