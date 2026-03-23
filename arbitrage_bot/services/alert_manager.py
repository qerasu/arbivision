import json
from types import SimpleNamespace

from arbitrage_bot.core.redis import get_redis
from arbitrage_bot.core.config import settings
from arbitrage_bot.models.orm import ArbOpportunity, Alert
from arbitrage_bot.tg_bot.preferences import filter_reason_for_preferences
from arbitrage_bot.tg_bot.preferences import get_global_preferences


class AlertManager:

    def __init__(self, db_session):
        self.db = db_session
        self.dedupe_ttl = settings.ALERTS_DEDUPE_TTL_SECONDS
        self.delta_profit = settings.ALERTS_DELTA_PROFIT_THRESHOLD_USD
        self.delta_roi = settings.ALERTS_DELTA_ROI_THRESHOLD_PERCENT / 100.0


    async def process_opportunity(self, pair, calc_result, market_a=None, market_b=None, preferences=None):
        if preferences is None:
            preferences = await get_global_preferences(self.db)
        filter_reason = self._get_global_filter_reason(
            calc_result,
            preferences,
            market_a,
            market_b,
        )
        if filter_reason:
            return False

        direction = calc_result["direction"]
        redis = await get_redis()
        dedupe_key = f"alert-dedupe:{pair.pair_hash}:{direction}"

        last_alert_data = await redis.get(dedupe_key)
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
            calculation_json=calc_result
        )
        self.db.add(opp)
        await self.db.flush()

        state_to_save = {
            "net_profit": calc_result["net_profit"],
            "net_roi": calc_result["net_roi"],
            "shares": calc_result["shares"]
        }
        dedupe_written = False

        try:
            alerts = await self._create_alert(opp)
            await redis.setex(dedupe_key, self.dedupe_ttl, json.dumps(state_to_save))
            dedupe_written = True
            await self.db.commit()
            return alerts
        except Exception:
            await self.db.rollback()
            if dedupe_written:
                await redis.delete(dedupe_key)
            raise


    async def _create_alert(self, opp):
        chat_ids = settings.TELEGRAM_DEFAULT_CHAT_IDS
        alerts = []
        for chat_id in chat_ids:
            alert = Alert(
                opportunity_id=opp.id,
                telegram_chat_id=chat_id,
                message_hash=str(opp.id),
                status="queued"
            )
            self.db.add(alert)
            alerts.append(alert)

        return alerts


    def _get_global_filter_reason(self, calc_result, preferences, market_a, market_b):
        if market_a is None or market_b is None:
            return None

        opportunity_view = SimpleNamespace(
            net_roi=calc_result["net_roi"],
            capital_required=calc_result["capital_required"],
        )
        return filter_reason_for_preferences(
            opportunity_view,
            market_a,
            market_b,
            preferences,
        )