import asyncio
import json
from collections import defaultdict

import aiohttp

from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.core.observability import incr_counter


log = get_logger("orderbook_stream")


class OrderbookStream:
    predict_url = "wss://ws.predict.fun/ws"
    polymarket_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


    def __init__(self, predict_api_key):
        self.predict_api_key = predict_api_key
        self.predict_books = {}
        self.polymarket_books = {}
        self.dirty_pair_hashes = set()
        self._predict_pairs = defaultdict(set)
        self._polymarket_pairs = defaultdict(set)
        self._desired_predict_ids = set()
        self._desired_polymarket_ids = set()
        self._subscribed_predict_ids = set()
        self._subscribed_polymarket_ids = set()
        self._predict_requests = {}
        self._predict_request_id = 0
        self._predict_connected = False
        self._polymarket_connected = False
        self._predict_ws = None
        self._polymarket_ws = None
        self._predict_subscription_event = asyncio.Event()
        self._polymarket_subscription_event = asyncio.Event()
        self._update_event = asyncio.Event()
        self._tasks = []
        self._session = None


    @property
    def ready(self):
        return self._predict_connected and self._polymarket_connected


    def register_pairs(self, prepared_pairs):
        predict_pairs = defaultdict(set)
        polymarket_pairs = defaultdict(set)
        known_pair_hashes = set()

        for item in prepared_pairs:
            pair = item["pair"]
            pair_hash = str(getattr(pair, "pair_hash", "") or "")
            if not pair_hash:
                continue

            known_pair_hashes.add(pair_hash)
            predict_id = str(item.get("pf_market_id") or "")
            if predict_id:
                predict_pairs[predict_id].add(pair_hash)

            mapping = getattr(pair, "outcome_mapping_json", None) or {}
            market_a = mapping.get("market_a") or {}
            for token_id in (market_a.get("yes"), market_a.get("no")):
                if token_id:
                    polymarket_pairs[str(token_id)].add(pair_hash)

        previous_pair_hashes = {
            pair_hash
            for pair_hashes in self._predict_pairs.values()
            for pair_hash in pair_hashes
        }
        self._predict_pairs = predict_pairs
        self._polymarket_pairs = polymarket_pairs
        self._desired_predict_ids = set(predict_pairs)
        self._desired_polymarket_ids = set(polymarket_pairs)
        self.predict_books = {
            market_id: book
            for market_id, book in self.predict_books.items()
            if market_id in self._desired_predict_ids
        }
        self.polymarket_books = {
            token_id: book
            for token_id, book in self.polymarket_books.items()
            if token_id in self._desired_polymarket_ids
        }
        self.dirty_pair_hashes.intersection_update(known_pair_hashes)
        self.dirty_pair_hashes.update(known_pair_hashes - previous_pair_hashes)

        self._ensure_started()
        self._predict_subscription_event.set()
        self._polymarket_subscription_event.set()


    def consume_pairs(self, pairs):
        self.dirty_pair_hashes.difference_update(
            str(getattr(pair, "pair_hash", "") or "")
            for pair in pairs
        )


    def get_predict_book(self, market_id):
        if not self._predict_connected:
            return None
        return self.predict_books.get(str(market_id))


    def get_polymarket_book(self, token_id):
        if not self._polymarket_connected:
            return None
        return self.polymarket_books.get(str(token_id))


    def seed_predict_book(self, market_id, payload):
        if isinstance(payload, dict):
            self.predict_books[str(market_id)] = payload


    def seed_polymarket_book(self, token_id, payload):
        if isinstance(payload, dict):
            self.polymarket_books[str(token_id)] = payload


    async def wait_for_update(self, timeout, debounce_seconds=0.25):
        if self.ready and self.dirty_pair_hashes:
            await asyncio.sleep(debounce_seconds)
            return True

        self._update_event.clear()
        if self.ready and self.dirty_pair_hashes:
            return True

        try:
            await asyncio.wait_for(self._update_event.wait(), timeout=max(0.0, float(timeout)))
        except asyncio.TimeoutError:
            return False

        await asyncio.sleep(debounce_seconds)
        return True


    async def close(self):
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        if self._session is not None:
            await self._session.close()
            self._session = None


    def _ensure_started(self):
        if self._tasks:
            return

        self._tasks = [
            asyncio.create_task(self._run_predict_stream()),
            asyncio.create_task(self._run_polymarket_stream()),
        ]


    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session


    async def _run_predict_stream(self):
        if not self.predict_api_key:
            return

        delay = 1.0
        while True:
            try:
                session = await self._get_session()
                async with session.ws_connect(
                    self.predict_url,
                    headers={"x-api-key": self.predict_api_key},
                    autoping=True,
                ) as ws:
                    self._predict_ws = ws
                    self._predict_connected = True
                    self._subscribed_predict_ids.clear()
                    self._predict_requests.clear()
                    self._wake_if_ready()
                    incr_counter("orderbook.stream.predict_fun_connected")
                    delay = 1.0
                    subscription_task = asyncio.create_task(
                        self._predict_subscription_loop(ws)
                    )
                    try:
                        async for message in ws:
                            if message.type == aiohttp.WSMsgType.TEXT:
                                await self._handle_predict_message(ws, message.json())
                            elif message.type in {
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.ERROR,
                            }:
                                break
                    finally:
                        subscription_task.cancel()
                        await asyncio.gather(subscription_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("predict.fun websocket disconnected", error=str(exc))
                incr_counter("orderbook.stream.predict_fun_disconnected")
            finally:
                self._predict_ws = None
                self._predict_connected = False
                self._subscribed_predict_ids.clear()

            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


    async def _run_polymarket_stream(self):
        delay = 1.0
        while True:
            try:
                session = await self._get_session()
                async with session.ws_connect(self.polymarket_url, autoping=True) as ws:
                    self._polymarket_ws = ws
                    self._polymarket_connected = True
                    self._subscribed_polymarket_ids.clear()
                    self._wake_if_ready()
                    incr_counter("orderbook.stream.polymarket_connected")
                    delay = 1.0
                    subscription_task = asyncio.create_task(
                        self._polymarket_subscription_loop(ws)
                    )
                    ping_task = asyncio.create_task(self._polymarket_ping_loop(ws))
                    try:
                        async for message in ws:
                            if message.type == aiohttp.WSMsgType.TEXT:
                                if message.data == "PING":
                                    await ws.send_str("PONG")
                                    continue
                                if message.data == "PONG":
                                    continue
                                payload = json.loads(message.data)
                                self._handle_polymarket_message(payload)
                            elif message.type in {
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.ERROR,
                            }:
                                break
                    finally:
                        subscription_task.cancel()
                        ping_task.cancel()
                        await asyncio.gather(
                            subscription_task,
                            ping_task,
                            return_exceptions=True,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("polymarket websocket disconnected", error=str(exc))
                incr_counter("orderbook.stream.polymarket_disconnected")
            finally:
                self._polymarket_ws = None
                self._polymarket_connected = False
                self._subscribed_polymarket_ids.clear()

            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


    async def _predict_subscription_loop(self, ws):
        self._predict_subscription_event.set()
        while self._predict_ws is ws:
            await self._predict_subscription_event.wait()
            self._predict_subscription_event.clear()
            removed = sorted(self._subscribed_predict_ids - self._desired_predict_ids)
            for market_id in removed:
                self._predict_request_id += 1
                await ws.send_json({
                    "method": "unsubscribe",
                    "requestId": self._predict_request_id,
                    "params": [f"predictOrderbook/{market_id}"],
                })
                self._subscribed_predict_ids.discard(market_id)

            pending = sorted(self._desired_predict_ids - self._subscribed_predict_ids)
            for market_id in pending:
                self._predict_request_id += 1
                request_id = self._predict_request_id
                await ws.send_json({
                    "method": "subscribe",
                    "requestId": request_id,
                    "params": [f"predictOrderbook/{market_id}"],
                })
                self._predict_requests[request_id] = market_id
                self._subscribed_predict_ids.add(market_id)
                if request_id % 25 == 0:
                    await asyncio.sleep(0)


    async def _polymarket_subscription_loop(self, ws):
        self._polymarket_subscription_event.set()
        while self._polymarket_ws is ws:
            await self._polymarket_subscription_event.wait()
            self._polymarket_subscription_event.clear()
            removed = sorted(
                self._subscribed_polymarket_ids - self._desired_polymarket_ids
            )
            if removed:
                await ws.send_json({
                    "operation": "unsubscribe",
                    "assets_ids": removed,
                })
                self._subscribed_polymarket_ids.difference_update(removed)

            pending = sorted(
                self._desired_polymarket_ids - self._subscribed_polymarket_ids
            )
            if not pending:
                continue

            if not self._subscribed_polymarket_ids:
                await ws.send_json({
                    "assets_ids": pending,
                    "type": "market",
                    "custom_feature_enabled": True,
                })
            else:
                await ws.send_json({
                    "operation": "subscribe",
                    "assets_ids": pending,
                    "custom_feature_enabled": True,
                })
            self._subscribed_polymarket_ids.update(pending)


    async def _polymarket_ping_loop(self, ws):
        while self._polymarket_ws is ws:
            await asyncio.sleep(10)
            await ws.send_str("PING")


    async def _handle_predict_message(self, ws, message):
        if not isinstance(message, dict):
            return

        if message.get("type") == "R":
            request_id = message.get("requestId")
            market_id = self._predict_requests.pop(request_id, None)
            if message.get("success") is False and market_id:
                self._subscribed_predict_ids.discard(market_id)
                log.warning(
                    "predict.fun websocket subscription failed",
                    market_id=market_id,
                    error=message.get("error"),
                )
            return

        if message.get("type") != "M":
            return

        topic = str(message.get("topic") or "")
        if topic == "heartbeat":
            await ws.send_json({"method": "heartbeat", "data": message.get("data")})
            return
        if not topic.startswith("predictOrderbook/"):
            return

        market_id = topic.split("/", 1)[1]
        payload = message.get("data")
        if not isinstance(payload, dict):
            return

        self.predict_books[market_id] = payload
        self._mark_dirty(self._predict_pairs.get(market_id, ()))
        incr_counter("orderbook.stream.predict_fun_updates")


    def _handle_polymarket_message(self, message):
        if isinstance(message, list):
            for item in message:
                self._handle_polymarket_message(item)
            return
        if not isinstance(message, dict):
            return

        event_type = message.get("event_type")
        if event_type == "book":
            token_id = str(message.get("asset_id") or "")
            if not token_id:
                return
            self.polymarket_books[token_id] = message
            self._mark_dirty(self._polymarket_pairs.get(token_id, ()))
            incr_counter("orderbook.stream.polymarket_updates")
            return

        if event_type != "price_change":
            return

        for change in message.get("price_changes") or []:
            if not isinstance(change, dict):
                continue
            token_id = str(change.get("asset_id") or "")
            if not token_id:
                continue
            book = self.polymarket_books.get(token_id)
            if book is not None:
                self._apply_polymarket_change(book, change)
            self._mark_dirty(self._polymarket_pairs.get(token_id, ()))
            incr_counter("orderbook.stream.polymarket_updates")


    def _apply_polymarket_change(self, book, change):
        side = str(change.get("side") or "").upper()
        if side not in {"BUY", "SELL"}:
            return
        side_key = "bids" if side == "BUY" else "asks"
        price = change.get("price")
        size = change.get("size")
        price_key = self._price_key(price)
        if price_key is None or size is None:
            return

        levels = book.get(side_key)
        if not isinstance(levels, list):
            levels = []
            book[side_key] = levels
        try:
            is_empty = float(size) == 0.0
        except (TypeError, ValueError):
            return

        matching_indexes = [
            index
            for index, level in enumerate(levels)
            if isinstance(level, dict)
            and self._price_key(level.get("price")) == price_key
        ]
        if is_empty:
            for index in reversed(matching_indexes):
                levels.pop(index)
            return

        updated_level = {"price": str(price), "size": str(size)}
        if not matching_indexes:
            levels.append(updated_level)
            return

        levels[matching_indexes[0]] = updated_level
        for index in reversed(matching_indexes[1:]):
            levels.pop(index)


    def _price_key(self, value):
        try:
            return format(float(value), ".12g")
        except (TypeError, ValueError):
            return None


    def _mark_dirty(self, pair_hashes):
        self.dirty_pair_hashes.update(pair_hashes)
        if self.ready and pair_hashes:
            self._update_event.set()


    def _wake_if_ready(self):
        if self.ready:
            self._update_event.set()
