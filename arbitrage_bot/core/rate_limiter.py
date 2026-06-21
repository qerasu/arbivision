import asyncio
import time


class TokenBucketRateLimiter:
    def __init__(self, tokens_per_second, max_tokens):
        self.rate = float(tokens_per_second)
        self.max_tokens = int(max_tokens)
        self.tokens = float(max_tokens)
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()


    async def acquire(self, tokens=1):
        async with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.last_update = now

                self.tokens = min(
                    self.max_tokens,
                    self.tokens + elapsed * self.rate
                )

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                deficit = tokens - self.tokens
                wait_time = deficit / self.rate
                await asyncio.sleep(wait_time)


    def try_acquire(self, tokens=1):
        now = time.monotonic()
        elapsed = now - self.last_update

        available = min(
            self.max_tokens,
            self.tokens + elapsed * self.rate
        )

        if available >= tokens:
            self.last_update = now
            self.tokens = available - tokens
            return True

        return False
