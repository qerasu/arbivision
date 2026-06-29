import asyncio
import base64
import json
import subprocess

import httpx
from arbitrage_bot.adapters.base import BaseAdapter
from arbitrage_bot.core.rate_limiter import TokenBucketRateLimiter


class PredictFunAdapter(BaseAdapter):
    base_url = "https://api.predict.fun/v1"
    page_limit = 100
    max_pages = 200
    recent_start_id = None
    curl_max_attempts = 3
    curl_max_time_seconds = 20
    curl_connect_timeout_seconds = 5
    orderbook_timeout_seconds = 4
    orderbook_connect_timeout_seconds = 2
    orderbook_curl_max_attempts = 2
    orderbook_curl_max_time_seconds = 4
    orderbook_curl_connect_timeout_seconds = 2
    fallback_errors = (
        httpx.ConnectError,
        httpx.ConnectTimeout,
        httpx.ReadTimeout,
        httpx.RemoteProtocolError,
    )

    def __init__(self):
        from arbitrage_bot.core.config import settings
        headers = {}
        if settings.PREDICT_FUN_API_KEY:
            headers["x-api-key"] = settings.PREDICT_FUN_API_KEY
        self.headers = headers
        limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
        timeout = httpx.Timeout(10.0, connect=5.0)
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            headers=headers,
            limits=limits,
        )
        self.last_fetch_partial = False
        self.last_fetch_complete = True
        self.rate_limiter = TokenBucketRateLimiter(tokens_per_second=15.0, max_tokens=30)


    async def close(self):
        await self.client.aclose()


    async def fetch_markets(self):
        all_items = []
        cursor = self._encode_cursor(self.recent_start_id) if self.recent_start_id else None
        previous_batch_ids = None
        self.last_fetch_partial = False
        self.last_fetch_complete = False

        for _ in range(self.max_pages):
            params = {
                "first": self.page_limit,
            }
            if cursor:
                params["after"] = cursor

            payload = await self._get_json("/markets", params=params)
            items = self._extract_items(payload)
            if items is None:
                return payload
            if not items:
                self.last_fetch_complete = True
                break

            batch_ids = tuple(str(item.get("id")) for item in items if isinstance(item, dict))
            if previous_batch_ids is not None and batch_ids == previous_batch_ids:
                break

            all_items.extend(item for item in items if self._is_open_market(item))
            cursor = self._extract_cursor(payload)
            if len(items) < self.page_limit or not cursor:
                self.last_fetch_complete = True
                break

            previous_batch_ids = batch_ids

        return all_items


    async def fetch_orderbook(self, market_id):
        return await self._get_json(
            f"/markets/{market_id}/orderbook",
            timeout=httpx.Timeout(
                self.orderbook_timeout_seconds,
                connect=self.orderbook_connect_timeout_seconds,
            ),
            curl_max_attempts=self.orderbook_curl_max_attempts,
            curl_max_time_seconds=self.orderbook_curl_max_time_seconds,
            curl_connect_timeout_seconds=self.orderbook_curl_connect_timeout_seconds,
        )


    async def _get_json(self, path, params=None, timeout=None, curl_max_attempts=None, curl_max_time_seconds=None, curl_connect_timeout_seconds=None):
        await self.rate_limiter.acquire()
        try:
            request_kwargs = {"params": params}
            if timeout is not None:
                request_kwargs["timeout"] = timeout
            response = await self.client.get(path, **request_kwargs)
            response.raise_for_status()
            return response.json()
        except self.fallback_errors as exc:
            return await self._curl_get_json(
                path,
                params=params,
                original_exc=exc,
                curl_max_attempts=curl_max_attempts,
                curl_max_time_seconds=curl_max_time_seconds,
                curl_connect_timeout_seconds=curl_connect_timeout_seconds,
            )


    async def _curl_get_json(self, path, params=None, original_exc=None, curl_max_attempts=None, curl_max_time_seconds=None, curl_connect_timeout_seconds=None):
        url = f"{self.base_url}{path}"
        if params:
            from urllib.parse import urlencode

            url = f"{url}?{urlencode(params, doseq=True)}"

        max_attempts = self.curl_max_attempts if curl_max_attempts is None else max(1, int(curl_max_attempts))
        max_time_seconds = self.curl_max_time_seconds if curl_max_time_seconds is None else float(curl_max_time_seconds)
        connect_timeout_seconds = (
            self.curl_connect_timeout_seconds
            if curl_connect_timeout_seconds is None
            else float(curl_connect_timeout_seconds)
        )

        curl_args = [
            "curl",
            "--silent",
            "--show-error",
            "--fail",
            "--location",
            "--connect-timeout", str(connect_timeout_seconds),
            "--max-time", str(max_time_seconds),
        ]

        header_payload = None
        if self.headers:
            curl_args.extend(["--header", "@-"])
            header_payload = "\n".join(
                f"{key}: {value}"
                for key, value in self.headers.items()
            ).encode()

        curl_args.append(url)

        last_detail = None
        for attempt in range(1, max_attempts + 1):
            returncode, stdout, stderr = await self._run_curl_process(
                curl_args,
                stdin_payload=header_payload,
            )

            if returncode == 0:
                return json.loads(stdout)

            last_detail = stderr.decode().strip() or repr(original_exc)
            if attempt < max_attempts:
                await asyncio.sleep(0.75 * attempt)

        raise RuntimeError(f"curl fallback failed for {url}: {last_detail}") from original_exc


    async def _run_curl_process(self, args, stdin_payload=None):
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.PIPE if stdin_payload is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate(stdin_payload)
            return proc.returncode, stdout, stderr
        except NotImplementedError:
            completed = await asyncio.to_thread(
                subprocess.run,
                args,
                input=stdin_payload,
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


    def _extract_cursor(self, payload):
        if isinstance(payload, dict):
            cursor = payload.get("cursor")
            return cursor if cursor else None
        return None


    def _encode_cursor(self, market_id):
        return base64.b64encode(str(market_id).encode()).decode()


    def _is_open_market(self, item):
        if not isinstance(item, dict):
            return False

        return (
            item.get("status") == "REGISTERED"
            and item.get("tradingStatus") == "OPEN"
            and item.get("isVisible") is not False
        )
