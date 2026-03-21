from datetime import datetime
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, JSON, ForeignKey, Text, UniqueConstraint

Base = declarative_base()


class Market(Base):
    __tablename__ = "markets"
    __table_args__ = (
        UniqueConstraint("platform", "platform_market_id", name="uq_markets_platform_market_id"),
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
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)


class MarketEntity(Base):
    __tablename__ = "market_entities"
    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(Integer, ForeignKey("markets.id"), nullable=False)
    entity_type = Column(String, nullable=False)
    entity_value = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)


class MarketPair(Base):
    __tablename__ = "market_pairs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id_a = Column(Integer, ForeignKey("markets.id"), nullable=False)
    market_id_b = Column(Integer, ForeignKey("markets.id"), nullable=False)
    pair_hash = Column(String, nullable=False, unique=True)
    status = Column(String, default="candidate", nullable=False)
    match_score = Column(Float, nullable=False)
    match_reason_json = Column(JSON, nullable=True)
    outcome_mapping_json = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)


class ArbOpportunity(Base):
    __tablename__ = "arb_opportunities"
    id = Column(Integer, primary_key=True, autoincrement=True)
    market_pair_id = Column(Integer, ForeignKey("market_pairs.id"), nullable=False)
    direction = Column(String, nullable=False)
    price_leg_1 = Column(Float, nullable=False)
    price_leg_2 = Column(Float, nullable=False)
    avg_price_leg_1 = Column(Float, nullable=False)
    avg_price_leg_2 = Column(Float, nullable=False)
    shares = Column(Float, nullable=False)
    capital_required = Column(Float, nullable=False)
    gross_profit = Column(Float, nullable=False)
    net_profit = Column(Float, nullable=False)
    gross_roi = Column(Float, nullable=False)
    net_roi = Column(Float, nullable=False)
    calculation_json = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)


class Alert(Base):
    __tablename__ = "alerts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    opportunity_id = Column(Integer, ForeignKey("arb_opportunities.id"), nullable=False)
    telegram_chat_id = Column(String, nullable=False)
    message_hash = Column(String, nullable=False)
    status = Column(String, default="queued", nullable=False)
    sent_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)


class Settings(Base):
    __tablename__ = "settings"
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String, nullable=False, unique=True)
    value_json = Column(JSON, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class BlacklistRule(Base):
    __tablename__ = "blacklist_rules"
    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_type = Column(Text, nullable=False)
    rule_value = Column(Text, nullable=False)
    reason = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)