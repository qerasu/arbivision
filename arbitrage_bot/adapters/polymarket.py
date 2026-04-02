import asyncio
import json
from urllib.parse import urlencode

import httpx
from arbitrage_bot.adapters.base import BaseAdapter


class PolymarketAdapter(BaseAdapter):
    base_url = "https://gamma-api.polymarket.com"
    clob_base_url = "https://clob.polymarket.com"
    page_limit = 100
    max_pages = 100
    fallback_errors = (
        httpx.ConnectError,
        httpx.ReadTimeout,
        httpx.RemoteProtocolError,
    )

    def __init__(self):
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)
        self.clob_client = httpx.AsyncClient(base_url=self.clob_base_url, timeout=10.0)


    async def close(self):
        await self.client.aclose()
        await self.clob_client.aclose()


    async def fetch_markets(self):
        all_items = []
        offset = 0
        previous_batch_ids = None

        for _ in range(self.max_pages):
            params = {
                "limit": self.page_limit,
                "offset": offset,
                "active": "true",
                "closed": "false",
            }
            payload = await self._get_json("/markets", params=params)
            items = self._extract_items(payload)

            if items is None:
                return payload
            if not items:
                break

            batch_ids = tuple(str(item.get("id")) for item in items if isinstance(item, dict))

            if previous_batch_ids is not None and batch_ids == previous_batch_ids:
                break

            all_items.extend(items)
            if len(items) < self.page_limit:
                break

            previous_batch_ids = batch_ids
            offset += self.page_limit

        return all_items


    async def fetch_orderbook(self, market_id):
        return await self._get_json(f"/markets/{market_id}")


    async def fetch_books(self, token_ids):
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

        proc = await asyncio.create_subprocess_exec(
            "curl",
            "--silent",
            "--show-error",
            "--fail",
            "--location",
            "--max-time",
            "10",
            "-X", "POST",
            "-H", "Content-Type: application/json",
            "-d", body,
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            detail = stderr.decode().strip() or repr(original_exc)
            raise RuntimeError(f"curl fallback failed for {url}: {detail}") from original_exc

        return json.loads(stdout)


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
            url = f"{url}?{urlencode(params, doseq=True)}"

        proc = await asyncio.create_subprocess_exec(
            "curl",
            "--silent",
            "--show-error",
            "--fail",
            "--location",
            "--max-time",
            "10",
            url,
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