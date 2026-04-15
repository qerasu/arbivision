from datetime import datetime, timezone
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, JSON, ForeignKey, Text, UniqueConstraint, Index

Base = declarative_base()


class Market(Base):
    __tablename__ = "markets"
    __table_args__ = (
        UniqueConstraint("platform", "platform_market_id", name="uq_markets_platform_market_id"),
        Index("ix_markets_platform_status", "platform", "status"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(String, nullable=False)
    platform_market_id = Column(String, nullable=False)
    status = Column(String, nullable=False)
    tradable = Column(Boolean, default=False)
    title = Column(Text, nullable=False)
    normalized_title = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    outcomes_json = Column(JSON, nullable=False)
    raw_payload_json = Column(JSON, nullable=False)
    category = Column(String, nullable=True)
    slug = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class MarketPair(Base):
    __tablename__ = "market_pairs"
    __table_args__ = (
        Index("ix_market_pairs_status", "status"),
        Index("ix_market_pairs_market_id_a", "market_id_a"),
        Index("ix_market_pairs_market_id_b", "market_id_b"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id_a = Column(Integer, ForeignKey("markets.id"), nullable=False)
    market_id_b = Column(Integer, ForeignKey("markets.id"), nullable=False)
    pair_hash = Column(String, nullable=False, unique=True)
    status = Column(String, default="auto_approved", nullable=False)
    match_score = Column(Float, nullable=False)
    match_reason_json = Column(JSON, nullable=True)
    outcome_mapping_json = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(String, nullable=False, default="active")
    role = Column(String, nullable=False, default="user")
    plan_code = Column(String, nullable=False, default="free")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)


class TelegramChat(Base):
    __tablename__ = "telegram_chats"
    __table_args__ = (
        UniqueConstraint("chat_id", name="uq_telegram_chats_chat_id"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    chat_id = Column(String, nullable=False)
    chat_type = Column(String, nullable=False, default="private")
    is_primary = Column(Boolean, nullable=False, default=True)
    is_verified = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class UserPreference(Base):
    __tablename__ = "user_preferences"
    __table_args__ = (
        UniqueConstraint("user_id", name="uq_user_preferences_user_id"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    min_roi_percent = Column(Float, nullable=True)
    min_capital_usd = Column(Float, nullable=True)
    max_capital_usd = Column(Float, nullable=True)
    max_polymarket_capital_usd = Column(Float, nullable=True)
    max_predict_fun_capital_usd = Column(Float, nullable=True)
    min_profit_usd = Column(Float, nullable=True)
    min_days_to_close = Column(Integer, nullable=True)
    max_days_to_close = Column(Integer, nullable=True)
    muted = Column(Boolean, nullable=False, default=False)
    language = Column(String, nullable=True, default=None)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)


class Subscription(Base):
    __tablename__ = "subscriptions"
    __table_args__ = (
        UniqueConstraint("channel", "destination", name="uq_subscriptions_channel_destination"),
        Index("ix_subscriptions_user_id_status", "user_id", "status"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    channel = Column(String, nullable=False)
    destination = Column(String, nullable=False)
    status = Column(String, nullable=False, default="active")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

class SettingsRecord(Base):
    __tablename__ = "settings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String, nullable=False, unique=True)
    value_json = Column(JSON, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)


class BlacklistRule(Base):
    __tablename__ = "blacklist_rules"
    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_type = Column(Text, nullable=False)
    rule_value = Column(Text, nullable=False)
    reason = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
