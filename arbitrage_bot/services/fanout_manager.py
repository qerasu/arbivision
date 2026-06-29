import time
from types import SimpleNamespace

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.core.observability import incr_counter
from arbitrage_bot.tg_bot.preferences import filter_reason_for_preferences
from arbitrage_bot.tg_bot.preferences import get_global_preferences
from arbitrage_bot.tg_bot.preferences import get_telegram_alert_targets

log = get_logger("fanout_manager")

# share cached targets across short-lived manager instances
_delivery_targets_cache = None
_delivery_targets_cache_expires_at = 0.0


class FanoutManager:
    # class-level aliases for setUp compatibility in tests
    @property
    def _cache_value(self):
        return _delivery_targets_cache


    @_cache_value.setter
    def _cache_value(self, value):
        global _delivery_targets_cache
        _delivery_targets_cache = value


    @property
    def _cache_expires_at(self):
        return _delivery_targets_cache_expires_at


    @_cache_expires_at.setter
    def _cache_expires_at(self, value):
        global _delivery_targets_cache_expires_at
        _delivery_targets_cache_expires_at = value


    def __init__(self, db_session):
        self.db = db_session


    async def create_alert_deliveries(self, opportunity, market_a, market_b, delivery_targets=None, directions=None, calculator=None):
        return await self._create_alert_deliveries(
            opportunity,
            market_a,
            market_b,
            delivery_targets=delivery_targets,
            directions=directions,
            calculator=calculator,
        )


    async def get_delivery_targets(self):
        return await self._get_delivery_targets()


    async def _create_alert_deliveries(self, opportunity, market_a, market_b, delivery_targets=None, directions=None, calculator=None):
        targets = delivery_targets if delivery_targets is not None else await self._get_delivery_targets()
        eligible_targets, drop_reasons = self._filter_targets(
            opportunity,
            targets,
            market_a,
            market_b,
            directions=directions,
            calculator=calculator,
        )
        if not eligible_targets:
            incr_counter("fanout.opportunity_filtered_all_targets")
            for drop_reason in sorted(drop_reasons):
                incr_counter(f"fanout.drop.{drop_reason}")
            log.debug(
                "opportunity filtered: no eligible targets",
                pair_id=getattr(opportunity, "market_pair_id", None),
                direction=getattr(opportunity, "direction", None),
                net_roi_pct=round(getattr(opportunity, "net_roi", 0.0) * 100, 2),
                net_profit=round(float(getattr(opportunity, "net_profit", 0.0)), 2),
                capital_required=round(float(getattr(opportunity, "capital_required", 0.0)), 2),
                drop_reasons=sorted(drop_reasons),
                total_targets=len(targets),
            )
            return []

        deliveries = []
        existing_chat_ids = set()
        for target in eligible_targets:
            chat_id = target["telegram_chat_id"]
            if chat_id in existing_chat_ids:
                continue

            alert = SimpleNamespace(
                user_id=target.get("user_id"),
                subscription_id=target.get("subscription_id"),
                telegram_chat_id=chat_id,
                message_hash=getattr(opportunity, "message_hash", None) or self._fallback_message_hash(opportunity),
                status="queued",
                attempt_count=0,
                next_retry_at=None,
                sent_at=None,
                error_message=None,
            )
            deliveries.append(
                {
                    "alert": alert,
                    "preferences": target.get("preferences") or {},
                    "opportunity": target.get("prepared_opportunity") or self._snapshot_opportunity(opportunity),
                }
            )
            existing_chat_ids.add(chat_id)
            incr_counter("fanout.alerts_created")

        return deliveries


    async def _get_delivery_targets(self):
        global _delivery_targets_cache, _delivery_targets_cache_expires_at
        now = time.monotonic()
        if _delivery_targets_cache is not None and _delivery_targets_cache_expires_at > now:
            incr_counter("fanout.delivery_targets_cache_hit")
            return _delivery_targets_cache
        incr_counter("fanout.delivery_targets_cache_miss")

        targets = await get_telegram_alert_targets(self.db)
        if targets:
            self._set_delivery_targets_cache(targets)
            return targets

        legacy_preferences = await get_global_preferences(self.db)
        targets = [
            {
                "user_id": None,
                "subscription_id": None,
                "telegram_chat_id": chat_id,
                "preferences": legacy_preferences,
            }
            for chat_id in settings.TELEGRAM_DEFAULT_CHAT_IDS
        ]
        self._set_delivery_targets_cache(targets)
        return targets


    def _set_delivery_targets_cache(self, targets):
        global _delivery_targets_cache, _delivery_targets_cache_expires_at
        _delivery_targets_cache = [dict(target) for target in targets]
        _delivery_targets_cache_expires_at = time.monotonic() + settings.FANOUT_TARGET_CACHE_TTL_SECONDS


    def _filter_targets(self, opportunity, targets, market_a, market_b, directions=None, calculator=None):
        eligible_targets = []
        drop_reasons = set()

        for target in targets:
            if not target.get("telegram_chat_id"):
                continue
            preferences = target.get("preferences") or {}
            if preferences.get("muted"):
                drop_reasons.add("muted")
                log.debug(
                    "target filtered: muted",
                    chat_id=target.get("telegram_chat_id"),
                )
                continue
            prepared_opportunity = opportunity
            if directions is not None and calculator is not None:
                prepared_opportunity = self._prepare_opportunity_for_target(
                    opportunity,
                    directions,
                    calculator,
                    preferences,
                )
                if prepared_opportunity is None:
                    drop_reasons.add("opportunity_unavailable")
                    log.debug(
                        "target filtered: opportunity_unavailable after capital recalc",
                        chat_id=target.get("telegram_chat_id"),
                        max_capital=preferences.get("max_capital_usd"),
                        direction=getattr(opportunity, "direction", None),
                    )
                    continue
            filter_reason = filter_reason_for_preferences(
                prepared_opportunity,
                market_a,
                market_b,
                preferences,
            )
            if filter_reason:
                drop_reasons.add(filter_reason)
                log.debug(
                    "target filtered: preference mismatch",
                    chat_id=target.get("telegram_chat_id"),
                    filter_reason=filter_reason,
                    net_roi_pct=round(getattr(prepared_opportunity, "net_roi", 0.0) * 100, 2),
                    net_profit=round(float(getattr(prepared_opportunity, "net_profit", 0.0)), 2),
                    capital_required=round(float(getattr(prepared_opportunity, "capital_required", 0.0)), 2),
                    pref_min_roi=preferences.get("min_roi_percent"),
                    pref_min_days=preferences.get("min_days_to_close"),
                    pref_max_days=preferences.get("max_days_to_close"),
                    pref_min_profit=preferences.get("min_profit_usd"),
                )
                continue
            target_payload = dict(target)
            target_payload["prepared_opportunity"] = prepared_opportunity
            eligible_targets.append(target_payload)

        return eligible_targets, drop_reasons


    def _prepare_opportunity_for_target(self, opportunity, directions, calculator, preferences=None):
        max_capital = None
        max_polymarket_capital = None
        max_predict_fun_capital = None
        if preferences is not None:
            max_capital = preferences.get("max_capital_usd")
            max_polymarket_capital = preferences.get("max_polymarket_capital_usd")
            max_predict_fun_capital = preferences.get("max_predict_fun_capital_usd")

        calc_results = calculator.calculate_opportunities(
            directions,
            max_capital=max_capital,
            max_polymarket_capital=max_polymarket_capital,
            max_predict_fun_capital=max_predict_fun_capital,
        )
        if not calc_results:
            return None

        current_result = next(
            (
                result
                for result in calc_results
                if result.get("direction") == opportunity.direction
            ),
            None,
        )
        if current_result is None:
            return None

        payload = {
            "direction": getattr(opportunity, "direction", None),
            "avg_price_leg_1": current_result["avg_price_leg_1"],
            "avg_price_leg_2": current_result["avg_price_leg_2"],
            "shares": current_result["shares"],
            "capital_required": current_result["capital_required"],
            "gross_profit": current_result["gross_profit"],
            "net_profit": current_result["net_profit"],
            "gross_roi": current_result["gross_roi"],
            "net_roi": current_result["net_roi"],
            "calculation_json": current_result,
            "message_hash": getattr(opportunity, "message_hash", None),
        }
        return SimpleNamespace(**payload)


    def _snapshot_opportunity(self, opportunity):
        payload = {
            "direction": getattr(opportunity, "direction", None),
            "avg_price_leg_1": getattr(opportunity, "avg_price_leg_1", 0.0),
            "avg_price_leg_2": getattr(opportunity, "avg_price_leg_2", 0.0),
            "shares": getattr(opportunity, "shares", 0.0),
            "capital_required": getattr(opportunity, "capital_required", 0.0),
            "gross_profit": getattr(opportunity, "gross_profit", 0.0),
            "net_profit": getattr(opportunity, "net_profit", 0.0),
            "gross_roi": getattr(opportunity, "gross_roi", 0.0),
            "net_roi": getattr(opportunity, "net_roi", 0.0),
            "calculation_json": getattr(opportunity, "calculation_json", None),
            "message_hash": getattr(opportunity, "message_hash", None),
        }
        return SimpleNamespace(**payload)


    def _fallback_message_hash(self, opportunity):
        return f"{getattr(opportunity, 'pair_hash', 'unknown')}:{getattr(opportunity, 'direction', 'unknown')}"
