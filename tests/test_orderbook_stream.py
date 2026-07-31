import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from arbitrage_bot.services.orderbook import OrderbookService
from arbitrage_bot.services.orderbook_stream import OrderbookStream


class OrderbookStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_stream_updates_books_and_marks_only_affected_pair_dirty(self):
        stream = OrderbookStream("key")
        pair = SimpleNamespace(
            pair_hash="pair-1",
            outcome_mapping_json={
                "market_a": {"yes": "poly-yes", "no": "poly-no"},
            },
        )
        prepared = [{
            "pair": pair,
            "pf_market_id": "101",
            "poly_market_id": "poly-market",
        }]

        with patch.object(stream, "_ensure_started"):
            stream.register_pairs(prepared)
        stream.consume_pairs([pair])
        stream._predict_connected = True
        stream._polymarket_connected = True
        ws = SimpleNamespace(send_json=AsyncMock())

        await stream._handle_predict_message(ws, {
            "type": "M",
            "topic": "predictOrderbook/101",
            "data": {"asks": [[0.5, 2]], "bids": [[0.4, 3]]},
        })
        self.assertEqual(stream.predict_books["101"]["asks"], [[0.5, 2]])
        self.assertEqual(stream.dirty_pair_hashes, {"pair-1"})

        stream.consume_pairs([pair])
        stream._handle_polymarket_message({
            "event_type": "book",
            "asset_id": "poly-yes",
            "asks": [{"price": "0.40", "size": "2"}],
            "bids": [],
        })
        asks = stream.polymarket_books["poly-yes"]["asks"]
        stream._handle_polymarket_message({
            "event_type": "price_change",
            "price_changes": [{
                "asset_id": "poly-yes",
                "price": "0.4",
                "size": "3",
                "side": "SELL",
            }],
        })
        self.assertIs(stream.polymarket_books["poly-yes"]["asks"], asks)
        self.assertEqual(asks, [{"price": "0.4", "size": "3"}])

        stream._handle_polymarket_message({
            "event_type": "price_change",
            "price_changes": [{
                "asset_id": "poly-yes",
                "price": "0.4",
                "size": "0",
                "side": "SELL",
            }],
        })

        self.assertIs(stream.polymarket_books["poly-yes"]["asks"], asks)
        self.assertEqual(stream.polymarket_books["poly-yes"]["asks"], [])
        self.assertEqual(stream.dirty_pair_hashes, {"pair-1"})


    def test_service_skips_clean_pairs_after_stream_initialization(self):
        service = OrderbookService()
        stream = OrderbookStream("key")
        stream._predict_connected = True
        stream._polymarket_connected = True
        service._stream = stream
        service._last_stream_reconcile_at = time.monotonic()
        pair = SimpleNamespace(
            id=1,
            pair_hash="pair-1",
            market_id_a=10,
            market_id_b=20,
            outcome_mapping_json={
                "market_a": {"yes": "poly-yes", "no": "poly-no"},
            },
        )
        market_map = {
            10: SimpleNamespace(platform="polymarket", platform_market_id="poly-market"),
            20: SimpleNamespace(platform="predict_fun", platform_market_id="101"),
        }

        with patch.object(stream, "_ensure_started"):
            first = service.prepare_pairs_for_cycle([pair], market_map)
            service.consume_pair_updates(first)
            second = service.prepare_pairs_for_cycle([pair], market_map)
            forced = service.prepare_pairs_for_cycle(
                [pair],
                market_map,
                force_pair_hashes={"pair-1"},
            )

        self.assertEqual(first, [pair])
        self.assertEqual(second, [])
        self.assertEqual(forced, [pair])
