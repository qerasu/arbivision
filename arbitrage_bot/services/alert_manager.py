import hashlib
import json
from types import SimpleNamespace

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.core.redis import get_redis

log = get_logger("alert_manager")


class AlertManager:
    def __init__(self, db_session):
        self.db = db_session
        self.dedupe_ttl = settings.ALERTS_DEDUPE_TTL_SECONDS
        self.delta_profit = settings.ALERTS_DELTA_PROFIT_THRESHOLD_USD
        self.delta_roi = settings.ALERTS_DELTA_ROI_THRESHOLD_PERCENT / 100.0


    async def process_opportunity(self, pair, calc_result):
        direction = calc_result["direction"]
        redis = await get_redis()
        dedupe_key = f"alert-dedupe:{pair.pair_hash}:{direction}"
        state_to_save = self._build_dedupe_state(calc_result)

        last_alert_data = None
        if redis is not None:
            try:
                last_alert_data = await redis.get(dedupe_key)
            except Exception:
                last_alert_data = None

        if last_alert_data:
            last_state = self._parse_dedupe_state(last_alert_data)
            if last_state is not None:
                profit_diff = calc_result["net_profit"] - last_state["net_profit"]
                roi_diff = calc_result["net_roi"] - last_state["net_roi"]

                if self._is_change_insignificant(profit_diff, roi_diff):
                    log.debug(
                        "opportunity skipped: insignificant delta",
                        pair_id=pair.id,
                        direction=direction,
                        profit_diff=round(profit_diff, 4),
                        roi_diff=round(roi_diff, 6),
                        threshold_profit=self.delta_profit,
                        threshold_roi=self.delta_roi,
                    )
                    return False

        opportunity = self._build_opportunity(pair, calc_result, state_to_save)
        self._attach_dedupe_state(opportunity, dedupe_key, state_to_save)
        return opportunity


    async def finalize_opportunity(self, opportunity):
        dedupe_key = getattr(opportunity, "_dedupe_key", None)
        state_to_save = getattr(opportunity, "_dedupe_state", None)
        if not dedupe_key or state_to_save is None:
            return

        try:
            redis = await get_redis()
        except Exception:
            redis = None
        await self._store_dedupe_state(redis, dedupe_key, state_to_save)
        self._clear_dedupe_state(opportunity)


    def _build_opportunity(self, pair, calc_result, state_to_save):
        payload = {
            "id": None,
            "market_pair_id": getattr(pair, "id", None),
            "pair_hash": getattr(pair, "pair_hash", None),
            "direction": calc_result["direction"],
            "price_leg_1": calc_result["avg_price_leg_1"],
            "price_leg_2": calc_result["avg_price_leg_2"],
            "avg_price_leg_1": calc_result["avg_price_leg_1"],
            "avg_price_leg_2": calc_result["avg_price_leg_2"],
            "shares": calc_result["shares"],
            "capital_required": calc_result["capital_required"],
            "gross_profit": calc_result["gross_profit"],
            "net_profit": calc_result["net_profit"],
            "gross_roi": calc_result["gross_roi"],
            "net_roi": calc_result["net_roi"],
            "calculation_json": calc_result,
            "message_hash": self._build_message_hash(pair, calc_result, state_to_save),
        }
        return SimpleNamespace(**payload)


    def _build_message_hash(self, pair, calc_result, state_to_save):
        raw_payload = {
            "pair_hash": getattr(pair, "pair_hash", None),
            "direction": calc_result["direction"],
            "state": state_to_save,
        }
        encoded = json.dumps(raw_payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


    def _attach_dedupe_state(self, opportunity, dedupe_key, state_to_save):
        setattr(opportunity, "_dedupe_key", dedupe_key)
        setattr(opportunity, "_dedupe_state", state_to_save)


    def _clear_dedupe_state(self, opportunity):
        if hasattr(opportunity, "_dedupe_key"):
            delattr(opportunity, "_dedupe_key")
        if hasattr(opportunity, "_dedupe_state"):
            delattr(opportunity, "_dedupe_state")


    def _build_dedupe_state(self, calc_result):
        return {
            "net_profit": calc_result["net_profit"],
            "net_roi": calc_result["net_roi"],
            "shares": calc_result["shares"],
        }


    def _parse_dedupe_state(self, raw_value):
        try:
            parsed = json.loads(raw_value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

        if not isinstance(parsed, dict):
            return None

        try:
            return {
                "net_profit": float(parsed["net_profit"]),
                "net_roi": float(parsed["net_roi"]),
                "shares": float(parsed.get("shares", 0.0) or 0.0),
            }
        except (KeyError, TypeError, ValueError):
            return None


    def _is_change_insignificant(self, profit_diff, roi_diff):
        return abs(profit_diff) < self.delta_profit and abs(roi_diff) < self.delta_roi


    async def _store_dedupe_state(self, redis, dedupe_key, state_to_save):
        if redis is None:
            return

        try:
            await redis.setex(dedupe_key, self.dedupe_ttl, json.dumps(state_to_save))
        except Exception:
            pass
