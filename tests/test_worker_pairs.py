import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

from arbitrage_bot.core.observability import reset_counters
from arbitrage_bot.core.observability import snapshot_counters
from arbitrage_bot.services.matcher import MatcherService
from arbitrage_bot import worker as worker_module
from arbitrage_bot.worker import _build_cached_market_signatures, _build_candidate_index_from_signatures, _candidate_markets_for_poly, _filter_skippable_pairs, _mark_stale_pairs, _process_candidates, _prune_market_signature_cache, _reconcile_market_pairs, _update_empty_counts


class WorkerPairLifecycleTests(unittest.TestCase):
    def setUp(self):
        worker_module._market_signature_cache.clear()


    def test_reconcile_updates_existing_pair_and_keeps_manual_approval(self):
        existing_pair = SimpleNamespace(
            pair_hash="pair-1",
            status="approved",
            match_score=0.71,
            match_reason_json={"old": True},
            outcome_mapping_json={"market_a": {"yes": "old-y", "no": "old-n"}},
        )
        matched_pair = SimpleNamespace(
            pair_hash="pair-1",
            status="auto_approved",
            match_score=0.91,
            match_reason_json={"old": False},
            outcome_mapping_json={"market_a": {"yes": "new-y", "no": "new-n"}},
        )

        new_pairs, has_updates = _reconcile_market_pairs(
            [existing_pair],
            {"pair-1": matched_pair},
        )

        self.assertEqual(new_pairs, [])
        self.assertTrue(has_updates)
        self.assertEqual(existing_pair.status, "approved")
        self.assertEqual(existing_pair.match_score, 0.91)
        self.assertEqual(existing_pair.match_reason_json, {"old": False})
        self.assertEqual(existing_pair.outcome_mapping_json, {"market_a": {"yes": "new-y", "no": "new-n"}})


    def test_reconcile_marks_unmatched_pairs_as_stale(self):
        existing_pair = SimpleNamespace(
            pair_hash="pair-1",
            status="auto_approved",
            match_score=0.88,
            match_reason_json={"title": "old"},
            outcome_mapping_json={"market_a": {"yes": "old-y", "no": "old-n"}},
        )

        new_pairs, has_updates = _reconcile_market_pairs([existing_pair], {})

        self.assertEqual(new_pairs, [])
        self.assertTrue(has_updates)
        self.assertEqual(existing_pair.status, "stale")


    def test_reconcile_creates_new_pairs(self):
        matched_pair = SimpleNamespace(
            pair_hash="pair-2",
            status="auto_approved",
            match_score=0.91,
            match_reason_json={"title": "new"},
            outcome_mapping_json={"market_a": {"yes": "poly-y", "no": "poly-n"}},
        )

        new_pairs, has_updates = _reconcile_market_pairs([], {"pair-2": matched_pair})

        self.assertEqual(new_pairs, [matched_pair])
        self.assertFalse(has_updates)


    def test_mark_stale_pairs_changes_only_active_statuses(self):
        stale_pair = SimpleNamespace(status="stale", pair_hash="h-stale")
        approved_pair = SimpleNamespace(status="approved", pair_hash="h-approved")
        failed_pair = SimpleNamespace(status="failed", pair_hash="h-failed")

        changed = _mark_stale_pairs([stale_pair, approved_pair, failed_pair])

        self.assertTrue(changed)
        self.assertEqual(stale_pair.status, "stale")
        self.assertEqual(approved_pair.status, "stale")
        self.assertEqual(failed_pair.status, "failed")


    def test_candidate_markets_for_poly_limits_ranked_candidates(self):
        matcher = MatcherService()
        matcher.max_ranked_candidates = 2
        poly_market = SimpleNamespace(
            id=1,
            title="Alpha Beta Gamma",
            outcomes_json=[],
            raw_payload_json={},
            category="sports",
        )
        pf_markets = [
            SimpleNamespace(id=10, title="Alpha Beta One", outcomes_json=[], raw_payload_json={}, category="sports"),
            SimpleNamespace(id=11, title="Alpha Beta Two", outcomes_json=[], raw_payload_json={}, category="sports"),
            SimpleNamespace(id=12, title="Alpha Beta Three", outcomes_json=[], raw_payload_json={}, category="sports"),
        ]

        pf_index = matcher.build_candidate_index(pf_markets)
        poly_signature = matcher.build_market_signature(poly_market)

        candidates = _candidate_markets_for_poly(poly_signature, matcher, pf_index)

        self.assertEqual(len(candidates), 2)


    def test_candidate_markets_for_poly_uses_coarse_ranking_signals(self):
        matcher = MatcherService()
        poly_market = SimpleNamespace(
            id=1,
            title="Grizzlies vs Hornets March 15 2026",
            outcomes_json=[],
            raw_payload_json={},
            category="nba",
        )
        pf_markets = [
            SimpleNamespace(id=10, title="Grizzlies vs Hornets March 15 2026", outcomes_json=[], raw_payload_json={}, category="nba"),
            SimpleNamespace(id=11, title="Grizzlies vs Hornets", outcomes_json=[], raw_payload_json={}, category="politics"),
        ]

        pf_index = matcher.build_candidate_index(pf_markets)
        poly_signature = matcher.build_market_signature(poly_market)

        candidates = _candidate_markets_for_poly(poly_signature, matcher, pf_index)

        self.assertEqual(candidates[0]["market"].id, 10)


    def test_build_cached_market_signatures_reuses_unchanged_market_signature(self):
        matcher = Mock()
        matcher.build_market_signature.side_effect = lambda market: {
            "market": market,
            "tokens": {market.title.lower()},
            "condition_ids": [],
        }
        market = SimpleNamespace(
            id=1,
            title="Alpha",
            category="sports",
            outcomes_json=[],
            raw_payload_json={},
            status="active",
            updated_at="v1",
        )

        first = _build_cached_market_signatures([market], matcher)
        second = _build_cached_market_signatures([market], matcher)

        self.assertEqual(matcher.build_market_signature.call_count, 1)
        self.assertIs(first[1], second[1])


    def test_build_cached_market_signatures_rebuilds_changed_market_signature(self):
        matcher = Mock()
        matcher.build_market_signature.side_effect = lambda market: {
            "market": market,
            "tokens": {market.title.lower()},
            "condition_ids": [],
        }
        market = SimpleNamespace(
            id=1,
            title="Alpha",
            category="sports",
            outcomes_json=[],
            raw_payload_json={},
            status="active",
            updated_at="v1",
        )

        _build_cached_market_signatures([market], matcher)
        market.updated_at = "v2"
        signatures = _build_cached_market_signatures([market], matcher)

        self.assertEqual(matcher.build_market_signature.call_count, 2)
        self.assertEqual(signatures[1]["market"].updated_at, "v2")


    def test_build_candidate_index_from_signatures_uses_prebuilt_signatures(self):
        signatures = {
            1: {
                "market": SimpleNamespace(id=1),
                "tokens": {"alpha", "beta"},
                "condition_ids": ["cond-1"],
            },
            2: {
                "market": SimpleNamespace(id=2),
                "tokens": {"beta", "gamma"},
                "condition_ids": ["cond-2"],
            },
        }

        index = _build_candidate_index_from_signatures(signatures)

        self.assertEqual(len(index["tokens"]["beta"]), 2)
        self.assertEqual(index["condition_ids"]["cond-1"][0]["market"].id, 1)


    def test_prune_market_signature_cache_removes_missing_market_ids(self):
        worker_module._market_signature_cache[1] = {
            "fingerprint": ("alpha",),
            "signature": {"market": SimpleNamespace(id=1)},
            "last_seen_at": 1.0,
        }
        worker_module._market_signature_cache[2] = {
            "fingerprint": ("beta",),
            "signature": {"market": SimpleNamespace(id=2)},
            "last_seen_at": 2.0,
        }

        _prune_market_signature_cache([SimpleNamespace(id=2)], [])

        self.assertNotIn(1, worker_module._market_signature_cache)
        self.assertIn(2, worker_module._market_signature_cache)


class FakePipeline:
    def __init__(self, redis):
        self._redis = redis
        self._commands = []


    def incr(self, key):
        self._commands.append(("incr", key))
        return self


    def expire(self, key, ttl):
        self._commands.append(("expire", key, ttl))
        return self


    async def execute(self):
        results = []
        for cmd in self._commands:
            if cmd[0] == "incr":
                key = cmd[1]
                current = int(self._redis.data.get(key, "0"))
                current += 1
                self._redis.data[key] = str(current)
                results.append(current)
            elif cmd[0] == "expire":
                results.append(True)
        return results


class FakeRedis:
    def __init__(self):
        self.data = {}


    async def get(self, key):
        return self.data.get(key)


    async def setex(self, key, ttl, value):
        self.data[key] = value


    async def delete(self, key):
        self.data.pop(key, None)


    def pipeline(self):
        return FakePipeline(self)


class WorkerEmptyOrderbookStateTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        reset_counters()


    async def test_filter_skippable_pairs_reads_threshold_from_redis(self):
        fake_redis = FakeRedis()
        fake_redis.data["worker:pair-empty-count:pair-1"] = "3"
        pairs = [
            SimpleNamespace(pair_hash="pair-1"),
            SimpleNamespace(pair_hash="pair-2"),
        ]

        with patch(
            "arbitrage_bot.worker.get_redis",
            new=AsyncMock(return_value=fake_redis),
        ):
            active_pairs = await _filter_skippable_pairs(pairs)

        self.assertEqual([pair.pair_hash for pair in active_pairs], ["pair-2"])


    async def test_update_empty_counts_persists_to_redis(self):
        fake_redis = FakeRedis()
        checked_pairs = [
            SimpleNamespace(pair_hash="pair-1"),
            SimpleNamespace(pair_hash="pair-2"),
        ]

        with patch(
            "arbitrage_bot.worker.get_redis",
            new=AsyncMock(return_value=fake_redis),
        ):
            await _update_empty_counts(checked_pairs, {"pair-2"})

        self.assertEqual(fake_redis.data["worker:pair-empty-count:pair-1"], "1")
        self.assertNotIn("worker:pair-empty-count:pair-2", fake_redis.data)


    async def test_process_candidates_counts_calculator_drop_reason(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            async def execute(self, stmt):
                return FakeScalarResult([SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20)])


        fake_db = FakeDb()
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {
                        "pair": SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
                        "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.7, 2)]}},
                    }
                ],
            )
        )
        calculator = SimpleNamespace(calculate_opportunities=Mock(return_value=[]))
        alert_manager = SimpleNamespace(process_opportunity=AsyncMock())
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(return_value=[]),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[
            SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
        ])), patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={
                10: SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                20: SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
            }),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(),
        ):
            result = await _process_candidates(fake_db, orderbook_service, calculator, alert_manager, fanout_manager)

        self.assertEqual(result["opportunities"], 0)
        counters = snapshot_counters()
        self.assertEqual(counters["worker.active_pairs_loaded"], 1)
        self.assertEqual(counters["worker.pairs_with_orderbooks"], 1)
        self.assertEqual(counters["calculator.drop.no_profitable_directions"], 1)


    async def test_process_candidates_persists_warmup_opportunity_without_fanout_or_send(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            async def execute(self, stmt):
                return FakeScalarResult([SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20)])


        fake_db = FakeDb()
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {
                        "pair": SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
                        "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.5, 2)]}},
                    }
                ]
            )
        )
        calculator = SimpleNamespace(
            calculate_opportunities=Mock(
                return_value=[
                    {
                        "direction": "A_yes_B_no",
                        "avg_price_leg_1": 0.40,
                        "avg_price_leg_2": 0.50,
                        "shares": 10.0,
                        "capital_required": 9.0,
                        "gross_profit": 1.0,
                        "net_profit": 2.0,
                        "gross_roi": 0.11,
                        "net_roi": 0.22,
                    }
                ]
            )
        )
        alert_manager = SimpleNamespace(
            process_opportunity=AsyncMock(return_value=SimpleNamespace(id=55, fanout_status="suppressed"))
        )
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(return_value=[]),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[
            SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
        ])), patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={
                10: SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                20: SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
            }),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(),
        ) as send_mock:
            result = await _process_candidates(
                fake_db,
                orderbook_service,
                calculator,
                alert_manager,
                fanout_manager,
                suppress_alerts=True,
            )

        self.assertEqual(result["opportunities"], 1)
        fanout_manager.get_delivery_targets.assert_not_awaited()
        fanout_manager.create_alert_deliveries.assert_not_awaited()
        send_mock.assert_not_awaited()
        counters = snapshot_counters()
        self.assertEqual(counters["worker.opportunity_warmup_persisted"], 1)


    async def test_process_candidates_limits_warmup_promotions_but_keeps_fresh_opportunities(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            def __init__(self):
                self.commit_calls = 0
                self.rollback_calls = 0


            async def execute(self, stmt):
                return FakeScalarResult(
                    [
                        SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
                        SimpleNamespace(id=2, pair_hash="pair-2", market_id_a=30, market_id_b=40),
                        SimpleNamespace(id=3, pair_hash="pair-3", market_id_a=50, market_id_b=60),
                    ]
                )


            async def commit(self):
                self.commit_calls += 1


            async def rollback(self):
                self.rollback_calls += 1


        fake_db = FakeDb()
        orderbook_payload = {
            "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.5, 2)]}},
        }
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {"pair": SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20), **orderbook_payload},
                    {"pair": SimpleNamespace(id=2, pair_hash="pair-2", market_id_a=30, market_id_b=40), **orderbook_payload},
                    {"pair": SimpleNamespace(id=3, pair_hash="pair-3", market_id_a=50, market_id_b=60), **orderbook_payload},
                ]
            )
        )
        calculator = SimpleNamespace(
            calculate_opportunities=Mock(
                return_value=[
                    {
                        "direction": "A_yes_B_no",
                        "avg_price_leg_1": 0.40,
                        "avg_price_leg_2": 0.50,
                        "shares": 10.0,
                        "capital_required": 9.0,
                        "gross_profit": 1.0,
                        "net_profit": 2.0,
                        "gross_roi": 0.11,
                        "net_roi": 0.22,
                    }
                ]
            )
        )

        async def process_opportunity(pair, calc_result, suppress_alert=False, allow_suppressed_promotion=True):
            if pair.id == 1:
                return SimpleNamespace(id=101, fanout_status="queued", _delivery_action="promoted")
            if pair.id == 2:
                self.assertFalse(allow_suppressed_promotion)
                return SimpleNamespace(id=102, fanout_status="suppressed", _delivery_action="deferred")
            return SimpleNamespace(id=103, fanout_status="queued", _delivery_action="queued")


        alert_manager = SimpleNamespace(
            process_opportunity=AsyncMock(side_effect=process_opportunity)
        )
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(return_value=[]),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[
            SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
            SimpleNamespace(id=2, pair_hash="pair-2", market_id_a=30, market_id_b=40),
            SimpleNamespace(id=3, pair_hash="pair-3", market_id_a=50, market_id_b=60),
        ])), patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={
                10: SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                20: SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
                30: SimpleNamespace(id=30, platform="polymarket", platform_market_id="poly-30"),
                40: SimpleNamespace(id=40, platform="predict_fun", platform_market_id="pf-40"),
                50: SimpleNamespace(id=50, platform="polymarket", platform_market_id="poly-50"),
                60: SimpleNamespace(id=60, platform="predict_fun", platform_market_id="pf-60"),
            }),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ), patch(
            "arbitrage_bot.worker.send_alert_immediately",
            new=AsyncMock(),
        ) as send_mock, patch(
            "arbitrage_bot.worker.settings.WARMUP_PROMOTION_LIMIT_PER_CYCLE",
            1,
        ):
            result = await _process_candidates(
                fake_db,
                orderbook_service,
                calculator,
                alert_manager,
                fanout_manager,
                suppress_alerts=False,
            )

        self.assertEqual(result["opportunities"], 2)
        fanout_manager.get_delivery_targets.assert_awaited_once()
        self.assertEqual(fanout_manager.create_alert_deliveries.await_count, 2)
        send_mock.assert_not_awaited()
        counters = snapshot_counters()
        self.assertEqual(counters["worker.opportunity_warmup_deferred"], 1)


    async def test_process_candidates_uses_batched_orderbook_fetch(self):
        class FakeScalarResult:
            def __init__(self, items):
                self.items = items


            def scalars(self):
                return self


            def all(self):
                return list(self.items)


        class FakeDb:
            async def execute(self, stmt):
                return FakeScalarResult([SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20)])


        fake_db = FakeDb()
        orderbook_service = SimpleNamespace(
            fetch_orderbooks_for_pairs=AsyncMock(
                return_value=[
                    {
                        "pair": SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
                        "directions": {"A_yes_B_no": {"poly": [(0.4, 2)], "pf": [(0.5, 2)]}},
                    }
                ]
            ),
            fetch_orderbook_for_pair=AsyncMock(side_effect=AssertionError("sequential orderbook fetch should not be used")),
        )
        calculator = SimpleNamespace(calculate_opportunities=Mock(return_value=[]))
        alert_manager = SimpleNamespace(process_opportunity=AsyncMock())
        fanout_manager = SimpleNamespace(
            create_alert_deliveries=AsyncMock(return_value=[]),
            get_delivery_targets=AsyncMock(return_value=[]),
        )

        with patch("arbitrage_bot.worker._filter_skippable_pairs", new=AsyncMock(return_value=[
            SimpleNamespace(id=1, pair_hash="pair-1", market_id_a=10, market_id_b=20),
        ])), patch(
            "arbitrage_bot.worker._load_market_map_for_pairs",
            new=AsyncMock(return_value={
                10: SimpleNamespace(id=10, platform="polymarket", platform_market_id="poly-10"),
                20: SimpleNamespace(id=20, platform="predict_fun", platform_market_id="pf-20"),
            }),
        ), patch(
            "arbitrage_bot.worker._update_empty_counts",
            new=AsyncMock(),
        ):
            await _process_candidates(fake_db, orderbook_service, calculator, alert_manager, fanout_manager)

        orderbook_service.fetch_orderbooks_for_pairs.assert_awaited_once()
        orderbook_service.fetch_orderbook_for_pair.assert_not_awaited()
