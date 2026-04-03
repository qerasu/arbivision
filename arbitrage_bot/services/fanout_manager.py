from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import aliased

from arbitrage_bot.core.config import settings
from arbitrage_bot.models.orm import Alert
from arbitrage_bot.models.orm import ArbOpportunity
from arbitrage_bot.models.orm import Market
from arbitrage_bot.models.orm import MarketPair
from arbitrage_bot.tg_bot.preferences import filter_reason_for_preferences
from arbitrage_bot.tg_bot.preferences import get_global_preferences
from arbitrage_bot.tg_bot.preferences import get_telegram_alert_targets


class FanoutManager:
    def __init__(self, db_session):
        self.db = db_session


    async def process_pending_opportunities(self, limit=50):
        market_b_alias = aliased(Market)
        stmt = (
            select(ArbOpportunity, MarketPair, Market, market_b_alias)
            .join(MarketPair, ArbOpportunity.market_pair_id == MarketPair.id)
            .join(Market, MarketPair.market_id_a == Market.id)
            .join(market_b_alias, MarketPair.market_id_b == market_b_alias.id)
            .where(ArbOpportunity.fanout_status.in_(["queued", "retry"]))
            .order_by(ArbOpportunity.id)
            .limit(limit)
        )
        result = await self.db.execute(stmt)
        rows = result.all()
        processed_count = 0

        for opportunity, pair, market_a, market_b in rows:
            try:
                created_alerts = await self._fanout_opportunity(
                    opportunity,
                    pair,
                    market_a,
                    market_b,
                )
                opportunity.fanout_status = "processed"
                opportunity.fanout_processed_at = datetime.now(timezone.utc)
                opportunity.fanout_error_message = None
                processed_count += created_alerts
                await self.db.commit()
            except Exception as exc:
                if opportunity.fanout_status == "retry":
                    opportunity.fanout_status = "failed"
                else:
                    opportunity.fanout_status = "retry"
                opportunity.fanout_error_message = str(exc)
                await self.db.commit()

        return processed_count


    async def _fanout_opportunity(self, opportunity, pair, market_a, market_b):
        targets = await self._get_delivery_targets()
        eligible_targets = self._filter_targets(opportunity, targets, market_a, market_b)
        if not eligible_targets:
            return 0

        existing_stmt = select(Alert.telegram_chat_id).where(Alert.opportunity_id == opportunity.id)
        existing_result = await self.db.execute(existing_stmt)
        existing_chat_ids = set(existing_result.scalars().all())

        created_count = 0
        for target in eligible_targets:
            chat_id = target["telegram_chat_id"]
            if chat_id in existing_chat_ids:
                continue

            self.db.add(
                Alert(
                    opportunity_id=opportunity.id,
                    user_id=target.get("user_id"),
                    subscription_id=target.get("subscription_id"),
                    telegram_chat_id=chat_id,
                    message_hash=str(opportunity.id),
                    status="queued",
                    attempt_count=0,
                )
            )
            created_count += 1

        await self.db.flush()
        return created_count


    async def _get_delivery_targets(self):
        targets = await get_telegram_alert_targets(self.db)
        if targets:
            return targets

        legacy_preferences = await get_global_preferences(self.db)
        return [
            {
                "user_id": None,
                "subscription_id": None,
                "telegram_chat_id": chat_id,
                "preferences": legacy_preferences,
            }
            for chat_id in settings.TELEGRAM_DEFAULT_CHAT_IDS
        ]


    def _filter_targets(self, opportunity, targets, market_a, market_b):
        eligible_targets = []

        for target in targets:
            if not target.get("telegram_chat_id"):
                continue
            preferences = target.get("preferences") or {}
            if preferences.get("muted"):
                continue
            if filter_reason_for_preferences(
                opportunity,
                market_a,
                market_b,
                target.get("preferences") or {},
            ):
                continue
            eligible_targets.append(target)

        return eligible_targets