import asyncio
import json
import logging
import subprocess
from urllib.parse import urlencode

import httpx
from arbitrage_bot.adapters.base import BaseAdapter
from arbitrage_bot.core.rate_limiter import TokenBucketRateLimiter

_log = logging.getLogger(__name__)


class PolymarketAdapter(BaseAdapter):
    base_url = "https://gamma-api.polymarket.com"
    clob_base_url = "https://clob.polymarket.com"
    page_limit = 500
    max_pages = 200
    curl_max_attempts = 2
    curl_max_time_seconds = 8
    curl_connect_timeout_seconds = 3
    fallback_errors = (
        httpx.ConnectError,
        httpx.ConnectTimeout,
        httpx.ReadTimeout,
        httpx.RemoteProtocolError,
    )

    def __init__(self):
        timeout = httpx.Timeout(8.0, connect=3.0)
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)
        self.clob_client = httpx.AsyncClient(base_url=self.clob_base_url, timeout=10.0)
        self.last_fetch_partial = False
        self.last_fetch_complete = True
        self.rate_limiter = TokenBucketRateLimiter(tokens_per_second=10.0, max_tokens=20)


    async def close(self):
        await self.client.aclose()
        await self.clob_client.aclose()


    async def fetch_markets(self, max_pages=None):
        all_items = []
        offset = 0
        previous_batch_ids = None
        had_failures = False
        reached_page_limit = False
        page_budget = self.max_pages if max_pages is None else max(0, int(max_pages))

        for page_index in range(page_budget):
            params = {
                "limit": self.page_limit,
                "offset": offset,
                "active": "true",
                "closed": "false",
            }

            try:
                payload = await self._get_json("/markets", params=params)
            except Exception as exc:
                had_failures = True
                _log.warning(
                    "polymarket page fetch failed (offset=%d), stopping pagination: %s",
                    offset,
                    exc,
                )
                break

            items = self._extract_items(payload)

            if items is None:
                if all_items:
                    had_failures = True
                    break
                self.last_fetch_partial = False
                self.last_fetch_complete = True
                return payload
            if not items:
                break

            batch_ids = tuple(str(item.get("id")) for item in items if isinstance(item, dict))

            if previous_batch_ids is not None and batch_ids == previous_batch_ids:
                reached_page_limit = True
                break

            all_items.extend(items)
            if len(items) < self.page_limit:
                break
            if page_index + 1 >= page_budget:
                reached_page_limit = True
                break

            previous_batch_ids = batch_ids
            offset += self.page_limit

        self.last_fetch_partial = had_failures
        self.last_fetch_complete = not had_failures and not reached_page_limit
        return all_items


    async def fetch_orderbook(self, market_id):
        return await self._get_json(f"/markets/{market_id}")


    async def fetch_books(self, token_ids):
        await self.rate_limiter.acquire()
        payload = [{"token_id": str(token_id)} for token_id in token_ids]
        try:
            response = await self.clob_client.post("/books", json=payload)
            response.raise_for_status()
            return response.json()
        except self.fallback_errors as exc:
            return await self._curl_post_books(token_ids, original_exc=exc)


    async def _curl_post_books(self, token_ids, original_exc=None):
        url = f"{self.clob_base_url}/books"
        body = json.dumps([{"token_id": str(tid)} for tid in token_ids])

        return await self._run_curl_json(
            [
                "-X", "POST",
                "-H", "Content-Type: application/json",
                "-d", body,
                url,
            ],
            url,
            original_exc=original_exc,
        )


    async def _get_json(self, path, params=None):
        await self.rate_limiter.acquire()
        try:
            response = await self.client.get(path, params=params)
            response.raise_for_status()
            return response.json()
        except self.fallback_errors as exc:
            return await self._curl_get_json(path, params=params, original_exc=exc)


    async def _curl_get_json(self, path, params=None, original_exc=None):
        url = f"{self.base_url}{path}"
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"

        return await self._run_curl_json([url], url, original_exc=original_exc)


    async def _run_curl_json(self, extra_args, url, original_exc=None):
        last_detail = None

        for attempt in range(1, self.curl_max_attempts + 1):
            returncode, stdout, stderr = await self._run_curl_process(
                [
                    "curl",
                    "--silent",
                    "--show-error",
                    "--fail",
                    "--location",
                    "--connect-timeout",
                    str(self.curl_connect_timeout_seconds),
                    "--max-time",
                    str(self.curl_max_time_seconds),
                    *extra_args,
                ]
            )

            if returncode == 0:
                return json.loads(stdout)

            last_detail = stderr.decode().strip() or repr(original_exc)
            if attempt < self.curl_max_attempts:
                await asyncio.sleep(0.75 * attempt)

        raise RuntimeError(f"curl fallback failed for {url}: {last_detail}") from original_exc


    async def _run_curl_process(self, args):
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            return proc.returncode, stdout, stderr
        except NotImplementedError:
            completed = await asyncio.to_thread(
                subprocess.run,
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            return completed.returncode, completed.stdout, completed.stderr


    def _extract_items(self, payload):
        if isinstance(payload, dict):
            data = payload.get("data", payload)
            return data if isinstance(data, list) else None

        if isinstance(payload, list):
            return payload

        return None
