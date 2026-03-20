import json
from arbitrage_bot.core.redis import get_redis
from arbitrage_bot.adapters.polymarket import PolymarketAdapter
from arbitrage_bot.adapters.predict_fun import PredictFunAdapter
from arbitrage_bot.models.orm import Market
from sqlalchemy.future import select


class OrderbookService:
    def __init__(self):
        self.polymarket = PolymarketAdapter()
        self.predict_fun = PredictFunAdapter()


    async def close(self):
        await self.polymarket.close()
        await self.predict_fun.close()


    async def fetch_orderbooks_for_pairs(self, market_pairs, db_session):
        market_id_map = {}
        market_ids = []
        for pair in market_pairs:
            market_ids.append(pair.market_id_a)
            market_ids.append(pair.market_id_b)

        if market_ids:
            stmt = select(Market.id, Market.platform, Market.platform_market_id).where(
                Market.id.in_(market_ids)
            )
            result = await db_session.execute(stmt)
            for market_id, platform, platform_market_id in result.all():
                market_id_map[(market_id, platform)] = platform_market_id

        redis = await get_redis()
        result_pairs = []

        for pair in market_pairs:
            poly_platform_id = market_id_map.get((pair.market_id_a, "polymarket"))
            pf_platform_id = market_id_map.get((pair.market_id_b, "predict_fun"))

            if not poly_platform_id or not pf_platform_id:
                print(
                    self._format_pair_error(
                        pair,
                        poly_platform_id,
                        pf_platform_id,
                        "missing platform market id",
                    )
                )
                continue

            try:
                poly_ob = await self.polymarket.fetch_orderbook(poly_platform_id)
            except Exception as exc:
                print(
                    self._format_pair_error(
                        pair,
                        poly_platform_id,
                        pf_platform_id,
                        f"polymarket orderbook fetch failed: {type(exc).__name__}: {exc}",
                    )
                )
                continue

            try:
                pf_ob = await self.predict_fun.fetch_orderbook(pf_platform_id)
            except Exception as exc:
                # one pair failing should not block the whole batch
                print(
                    self._format_pair_error(
                        pair,
                        poly_platform_id,
                        pf_platform_id,
                        f"predict.fun orderbook fetch failed: {type(exc).__name__}: {exc}",
                    )
                )
                continue

            poly_key = f"ob:polymarket:{poly_platform_id}"
            pf_key = f"ob:predict_fun:{pf_platform_id}"

            await redis.setex(poly_key, 60, json.dumps(poly_ob))
            await redis.setex(pf_key, 60, json.dumps(pf_ob))

            result_pairs.append(
                {
                    "pair": pair,
                    "poly": poly_ob,
                    "pf": pf_ob,
                    "poly_market_id": poly_platform_id,
                    "pf_market_id": pf_platform_id,
                }
            )

        return result_pairs


    def _format_pair_error(self, pair, poly_platform_id, pf_platform_id, reason):
        return (
            f"[orderbook] pair_id={pair.id} "
            f"polymarket_market_id={poly_platform_id or 'missing'} "
            f"predict_fun_market_id={pf_platform_id or 'missing'} "
            f"reason={reason}"
        )


    async def fetch_and_cache_orderbooks(self, market_pairs, db_session):
        await self.fetch_orderbooks_for_pairs(market_pairs, db_session)


    async def get_cached_orderbook(self, platform, market_id):
        redis = await get_redis()
        key = f"ob:{platform}:{market_id}"
        
        data = await redis.get(key)
        if data:
            return json.loads(data)

        return None
