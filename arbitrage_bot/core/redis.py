# setting redis with aioredis
import redis.asyncio as aioredis
from arbitrage_bot.core.config import settings

# initialize a global redis connection pool
redis_client = aioredis.from_url(
    settings.redis_url,
    encoding="utf-8",
    decode_responses=True,
    max_connections=10
)


async def get_redis():
    return redis_client