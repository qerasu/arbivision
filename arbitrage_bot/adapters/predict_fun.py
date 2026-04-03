import asyncio
import base64
import json

import httpx
from arbitrage_bot.adapters.base import BaseAdapter


class PredictFunAdapter(BaseAdapter):
    base_url = "https://api.predict.fun/v1"
    page_limit = 100
    max_pages = 100
    recent_start_id = None
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


    async def close(self):
        await self.client.aclose()


    async def fetch_markets(self):
        all_items = []
        cursor = self._encode_cursor(self.recent_start_id) if self.recent_start_id else None
        previous_batch_ids = None

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
                break

            batch_ids = tuple(str(item.get("id")) for item in items if isinstance(item, dict))
            if previous_batch_ids is not None and batch_ids == previous_batch_ids:
                break

            all_items.extend(item for item in items if self._is_open_market(item))
            cursor = self._extract_cursor(payload)
            if len(items) < self.page_limit or not cursor:
                break

            previous_batch_ids = batch_ids

        return all_items


    async def fetch_orderbook(self, market_id):
        return await self._get_json(f"/markets/{market_id}/orderbook")


    async def _get_json(self, path, params=None):
        try:
            response = await self.client.get(path, params=params)
            response.raise_for_status()
            return response.json()
        except self.fallback_errors as exc:
            return await self._curl_get_json(path, params=params, original_exc=exc)


    async def _curl_get_json(self, path, params=None, original_exc=None):
        url = f"{self.base_url}{path}"
        if params:
            from urllib.parse import urlencode

            url = f"{url}?{urlencode(params, doseq=True)}"

        cmd = [
            "curl",
            "--silent",
            "--show-error",
            "--fail",
            "--location",
            "--max-time",
            "10",
        ]
        for key, value in self.headers.items():
            cmd.extend(["-H", f"{key}: {value}"])
        cmd.append(url)

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            detail = stderr.decode().strip() or repr(original_exc)
            raise RuntimeError(f"curl fallback failed for {url}: {detail}") from original_exc

        return json.loads(stdout)


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