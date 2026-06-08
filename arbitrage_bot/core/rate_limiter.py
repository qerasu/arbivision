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


class SlidingWindowRateLimiter:
    def __init__(self, max_requests, window_seconds):
        self.max_requests = max_requests
        self.window_seconds = float(window_seconds)
        self.requests = []
        self._lock = asyncio.Lock()


    async def acquire(self):
        async with self._lock:
            while True:
                now = time.monotonic()
                cutoff = now - self.window_seconds

                self.requests = [ts for ts in self.requests if ts > cutoff]

                if len(self.requests) < self.max_requests:
                    self.requests.append(now)
                    return

                oldest = self.requests[0]
                wait_time = oldest + self.window_seconds - now
                if wait_time > 0:
                    await asyncio.sleep(wait_time)