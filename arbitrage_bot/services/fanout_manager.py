from datetime import datetime, timezone
import time

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import aliased

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.observability import incr_counter
from arbitrage_bot.models.orm import Alert
from arbitrage_bot.models.orm import ArbOpportunity
from arbitrage_bot.models.orm import Market
from arbitrage_bot.models.orm import MarketPair
from arbitrage_bot.tg_bot.preferences import filter_reason_for_preferences
from arbitrage_bot.tg_bot.preferences import get_global_preferences
from arbitrage_bot.tg_bot.preferences import get_telegram_alert_targets



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


    async def create_alert_deliveries(self, opportunity, market_a, market_b, delivery_targets=None, skip_existing_lookup=False):
        return await self._create_alert_deliveries(
            opportunity,
            market_a,
            market_b,
            delivery_targets=delivery_targets,
            skip_existing_lookup=skip_existing_lookup,
        )


    async def get_delivery_targets(self):
        return await self._get_delivery_targets()


    async def _create_alert_deliveries(self, opportunity, market_a, market_b, delivery_targets=None, skip_existing_lookup=False):
        targets = delivery_targets if delivery_targets is not None else await self._get_delivery_targets()
        eligible_targets, drop_reasons = self._filter_targets(opportunity, targets, market_a, market_b)
        if not eligible_targets:
            incr_counter("fanout.opportunity_filtered_all_targets")
            for drop_reason in sorted(drop_reasons):
                incr_counter(f"fanout.drop.{drop_reason}")
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

            try:
                alert = Alert(
                    opportunity_id=opportunity.id,
                    user_id=target.get("user_id"),
                    subscription_id=target.get("subscription_id"),
                    telegram_chat_id=chat_id,
                    message_hash=str(opportunity.id),
                    status="queued",
                    attempt_count=0,
                )
                async with self.db.begin_nested():
                    self.db.add(alert)
                    await self.db.flush()
                    deliveries.append(
                        {
                            "alert": alert,
                            "preferences": target.get("preferences") or {},
                        }
                    )
                existing_chat_ids.add(chat_id)
                incr_counter("fanout.alert_created")
                incr_counter("fanout.alerts_created")
            except IntegrityError:
                existing_chat_ids.add(chat_id)
                incr_counter("fanout.alert_duplicate")

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


    def _filter_targets(self, opportunity, targets, market_a, market_b):
        eligible_targets = []
        drop_reasons = set()

        for target in targets:
            if not target.get("telegram_chat_id"):
                continue
            preferences = target.get("preferences") or {}
            if preferences.get("muted"):
                drop_reasons.add("muted")
                continue
            filter_reason = filter_reason_for_preferences(
                opportunity,
                market_a,
                market_b,
                preferences,
                skip_max_capital=True,
            )
            if filter_reason:
                drop_reasons.add(filter_reason)
                continue
            eligible_targets.append(target)

        return eligible_targets, drop_reasons