from datetime import datetime, timezone
import time
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import aliased

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.core.observability import incr_counter
from arbitrage_bot.models.orm import Alert
from arbitrage_bot.models.orm import ArbOpportunity
from arbitrage_bot.models.orm import Market
from arbitrage_bot.models.orm import MarketPair
from arbitrage_bot.tg_bot.preferences import filter_reason_for_preferences
from arbitrage_bot.tg_bot.preferences import get_global_preferences
from arbitrage_bot.tg_bot.preferences import get_telegram_alert_targets

log = get_logger("fanout_manager")



class FanoutManager:
    _cache_value = None
    _cache_expires_at = 0.0

    def __init__(self, db_session):
        self.db = db_session


    async def process_pending_opportunities(self, limit=50):
        delivery_targets = await self._get_delivery_targets()
        processed_count = 0
        rows = await self._claim_pending_opportunities(limit)
        if not rows:
            return 0

        for row in rows:
            opportunity, pair, market_a, market_b = row
            try:
                incr_counter("fanout.opportunity_claimed")
                created_alerts = await self._fanout_opportunity(
                    opportunity,
                    market_a,
                    market_b,
                    delivery_targets=delivery_targets,
                )
                opportunity.fanout_status = "processed"
                opportunity.fanout_processed_at = datetime.now(timezone.utc)
                opportunity.fanout_error_message = None
                processed_count += created_alerts
                incr_counter("fanout.opportunity_processed")
            except Exception as exc:
                if opportunity.fanout_status == "retry":
                    opportunity.fanout_status = "failed"
                    incr_counter("fanout.opportunity_failed")
                else:
                    opportunity.fanout_status = "retry"
                    incr_counter("fanout.opportunity_retry")
                opportunity.fanout_error_message = str(exc)

        await self.db.commit()

        return processed_count


    async def _claim_pending_opportunities(self, limit):
        market_b_alias = aliased(Market)
        stmt = (
            select(ArbOpportunity, MarketPair, Market, market_b_alias)
            .join(MarketPair, ArbOpportunity.market_pair_id == MarketPair.id)
            .join(Market, MarketPair.market_id_a == Market.id)
            .join(market_b_alias, MarketPair.market_id_b == market_b_alias.id)
            .where(ArbOpportunity.fanout_status.in_(["queued", "retry"]))
            .order_by(ArbOpportunity.id)
            .limit(limit)
            .with_for_update(skip_locked=True, of=ArbOpportunity)
        )
        result = await self.db.execute(stmt)
        rows = result.all()
        if not rows:
            return []

        for opportunity, _, _, _ in rows:
            opportunity.fanout_status = "processing"
            opportunity.fanout_error_message = None

        return rows


    async def _fanout_opportunity(self, opportunity, market_a, market_b, delivery_targets=None):
        deliveries = await self._create_alert_deliveries(
            opportunity,
            market_a,
            market_b,
            delivery_targets=delivery_targets,
        )
        return len(deliveries)


    async def create_alert_deliveries(self, opportunity, market_a, market_b, delivery_targets=None, skip_existing_lookup=False, directions=None, calculator=None):
        return await self._create_alert_deliveries(
            opportunity,
            market_a,
            market_b,
            delivery_targets=delivery_targets,
            skip_existing_lookup=skip_existing_lookup,
            directions=directions,
            calculator=calculator,
        )


    async def get_delivery_targets(self):
        return await self._get_delivery_targets()


    async def _create_alert_deliveries(self, opportunity, market_a, market_b, delivery_targets=None, skip_existing_lookup=False, directions=None, calculator=None):
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
                opportunity_id=getattr(opportunity, "id", None),
                pair_id=getattr(opportunity, "market_pair_id", None),
                direction=getattr(opportunity, "direction", None),
                net_roi_pct=round(getattr(opportunity, "net_roi", 0.0) * 100, 2),
                net_profit=round(float(getattr(opportunity, "net_profit", 0.0)), 2),
                capital_required=round(float(getattr(opportunity, "capital_required", 0.0)), 2),
                drop_reasons=sorted(drop_reasons),
                total_targets=len(targets),
            )
            return []

        existing_chat_ids = set()
        if not skip_existing_lookup:
            existing_stmt = select(Alert.telegram_chat_id).where(Alert.opportunity_id == opportunity.id)
            existing_result = await self.db.execute(existing_stmt)
            existing_chat_ids = set(existing_result.scalars().all())

        deliveries = []
        for target in eligible_targets:
            chat_id = target["telegram_chat_id"]
            if chat_id in existing_chat_ids:
                continue

            alert = SimpleNamespace(
                opportunity_id=opportunity.id,
                user_id=target.get("user_id"),
                subscription_id=target.get("subscription_id"),
                telegram_chat_id=chat_id,
                message_hash=str(opportunity.id),
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
            incr_counter("fanout.alert_created")
            incr_counter("fanout.alerts_created")

        return deliveries


    async def _get_delivery_targets(self):
        now = time.monotonic()
        if FanoutManager._cache_value is not None and FanoutManager._cache_expires_at > now:
            incr_counter("fanout.delivery_targets_cache_hit")
            return FanoutManager._cache_value
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
        FanoutManager._cache_value = [dict(target) for target in targets]
        FanoutManager._cache_expires_at = time.monotonic() + settings.FANOUT_TARGET_CACHE_TTL_SECONDS


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
        }
        return SimpleNamespace(**payload)