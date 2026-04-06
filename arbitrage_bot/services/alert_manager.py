import json

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.redis import get_redis
from arbitrage_bot.models.orm import ArbOpportunity


class AlertManager:
    def __init__(self, db_session):
        self.db = db_session
        self.dedupe_ttl = settings.ALERTS_DEDUPE_TTL_SECONDS
        self.delta_profit = settings.ALERTS_DELTA_PROFIT_THRESHOLD_USD
        self.delta_roi = settings.ALERTS_DELTA_ROI_THRESHOLD_PERCENT / 100.0


    async def process_opportunity(self, pair, calc_result, market_a=None, market_b=None, preferences=None):
        direction = calc_result["direction"]
        redis = await get_redis()
        dedupe_key = f"alert-dedupe:{pair.pair_hash}:{direction}"

        last_alert_data = None
        if redis is not None:
            try:
                last_alert_data = await redis.get(dedupe_key)
            except Exception:
                last_alert_data = None

        if last_alert_data:
            last_state = json.loads(last_alert_data)
            profit_diff = calc_result["net_profit"] - last_state["net_profit"]
            roi_diff = calc_result["net_roi"] - last_state["net_roi"]

            # smart deduplication
            if profit_diff < self.delta_profit and roi_diff < self.delta_roi:
                return False

        opp = ArbOpportunity(
            market_pair_id=pair.id,
            direction=direction,
            price_leg_1=calc_result["avg_price_leg_1"],
            price_leg_2=calc_result["avg_price_leg_2"],
            avg_price_leg_1=calc_result["avg_price_leg_1"],
            avg_price_leg_2=calc_result["avg_price_leg_2"],
            shares=calc_result["shares"],
            capital_required=calc_result["capital_required"],
            gross_profit=calc_result["gross_profit"],
            net_profit=calc_result["net_profit"],
            gross_roi=calc_result["gross_roi"],
            net_roi=calc_result["net_roi"],
            calculation_json=calc_result,
            fanout_status="queued",
        )
        self.db.add(opp)
        await self.db.flush()

        state_to_save = {
            "net_profit": calc_result["net_profit"],
            "net_roi": calc_result["net_roi"],
            "shares": calc_result["shares"]
        }
        try:
            await self.db.commit()
        except Exception:
            await self.db.rollback()
            raise

        # write dedupe key after successful commit to prevent
        # skipping alerts when the transaction rolls back
        if redis is not None:
            try:
                await redis.setex(dedupe_key, self.dedupe_ttl, json.dumps(state_to_save))
            except Exception:
                pass
        return opp