import asyncio
import json
from urllib.parse import urlencode

import httpx
from arbitrage_bot.adapters.base import BaseAdapter


class PolymarketAdapter(BaseAdapter):
    base_url = "https://gamma-api.polymarket.com"


    def __init__(self):
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)


    async def close(self):
        await self.client.aclose()


    async def fetch_markets(self):
        params = {"limit": 100, "active": "true", "closed": "false"}
        return await self._get_json("/markets", params=params)


    async def fetch_orderbook(self, market_id):
        return await self._get_json(f"/markets/{market_id}")


    async def _get_json(self, path, params=None):
        try:
            response = await self.client.get(path, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.ConnectError as exc:
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
