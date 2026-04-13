"""SQLAlchemy models for the OddsIQ database.

Six tables designed for cross-platform prediction market comparison:
  - entities / entity_aliases  → canonical team/player names + aliases
  - events / markets           → Kalshi (and future Polymarket) events
  - prices                     → historical price snapshots
  - game_scores                → live score snapshots from feeds
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Float, Boolean,
    DateTime, ForeignKey, UniqueConstraint, Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# ── Entities (canonical team/player/asset names) ─────────────────────

class Entity(Base):
    """One row per real-world team, player, league, or concept.
    The single source of truth that all platforms map into."""
    __tablename__ = "entities"

    id = Column(Integer, primary_key=True)
    canonical_name = Column(Text, nullable=False, unique=True)
    entity_type = Column(Text, nullable=False)  # 'team', 'player', 'league', 'asset'
    sport = Column(Text)       # 'Soccer', 'Basketball', etc. Nullable for non-sport.
    league = Column(Text)      # 'La Liga', 'EPL', etc.
    metadata_ = Column("metadata", JSONB, default={})  # logo_url, espn_id, etc.
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    aliases = relationship("EntityAlias", back_populates="entity", cascade="all, delete-orphan")


class EntityAlias(Base):
    """Many-to-one aliases pointing to an entity.
    'Athletic Club', 'Bilbao', 'Ath Bilbao', 'ATH' → entity #42."""
    __tablename__ = "entity_aliases"

    id = Column(Integer, primary_key=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    alias = Column(Text, nullable=False)
    source = Column(Text, nullable=False)  # 'kalshi', 'espn', 'sofascore', 'polymarket', 'manual'
    normalized = Column(Text, nullable=False)  # lowercase, accent-stripped for fast lookup

    entity = relationship("Entity", back_populates="aliases")

    __table_args__ = (
        UniqueConstraint("alias", "source", name="uq_alias_source"),
        Index("idx_alias_normalized", "normalized"),
    )


# ── Events & Markets (Kalshi, future Polymarket) ─────────────────────

class Event(Base):
    """A Kalshi event (or future Polymarket event). One event has
    many markets (outcomes)."""
    __tablename__ = "events"

    id = Column(Integer, primary_key=True)
    platform = Column(Text, nullable=False, default="kalshi")  # 'kalshi', 'polymarket'
    event_ticker = Column(Text, nullable=False)
    series_ticker = Column(Text)
    title = Column(Text, nullable=False)
    category = Column(Text)     # 'Sports', 'Crypto', 'Elections', etc.
    sport = Column(Text)        # nullable for non-sport
    subcat = Column(Text)       # 'La Liga', 'NBA Games', etc.
    kickoff_dt = Column(DateTime(timezone=True))
    exp_dt = Column(DateTime(timezone=True))
    close_dt = Column(DateTime(timezone=True))
    status = Column(Text, default="open")  # 'open', 'closed', 'settled'
    is_live = Column(Boolean, default=False)
    metadata_ = Column("metadata", JSONB, default={})
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    markets = relationship("Market", back_populates="event", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("platform", "event_ticker", name="uq_platform_event"),
    )


class Market(Base):
    """A single binary contract / outcome within an event."""
    __tablename__ = "markets"

    id = Column(Integer, primary_key=True)
    event_id = Column(Integer, ForeignKey("events.id"), nullable=False)
    ticker = Column(Text, nullable=False, unique=True)
    label = Column(Text)         # "Sevilla", "Draw", "Over 2.5", "$71,000 or above"
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=True)  # links to canonical team/player
    status = Column(Text, default="active")  # 'active', 'settled'
    result = Column(Text)        # 'yes', 'no', NULL
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    event = relationship("Event", back_populates="markets")


# ── Prices (historical time-series) ──────────────────────────────────

class Price(Base):
    """Point-in-time price snapshot for a market. Grows fast —
    plan to partition or prune rows older than 90 days."""
    __tablename__ = "prices"

    id = Column(BigInteger, primary_key=True)
    market_ticker = Column(Text, nullable=False)  # denormalized for fast writes
    yes_bid = Column(Float)
    yes_ask = Column(Float)
    no_bid = Column(Float)
    no_ask = Column(Float)
    last_price = Column(Float)
    volume = Column(Float)
    open_interest = Column(Float)
    source = Column(Text, default="rest")  # 'rest' (snapshot), 'ws' (live)
    captured_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("idx_prices_ticker_time", "market_ticker", captured_at.desc()),
    )


# ── Game Scores (live from ESPN/SofaScore/SportsDB) ──────────────────

class GameScore(Base):
    """Live score snapshot from a feed. Replaced every poll cycle."""
    __tablename__ = "game_scores"

    id = Column(Integer, primary_key=True)
    source = Column(Text, nullable=False)   # 'espn', 'sofascore', 'sportsdb'
    sport = Column(Text, nullable=False)
    league = Column(Text)
    home_entity_id = Column(Integer, ForeignKey("entities.id"), nullable=True)
    away_entity_id = Column(Integer, ForeignKey("entities.id"), nullable=True)
    home_name = Column(Text)     # raw name from feed (for debugging)
    away_name = Column(Text)
    home_score = Column(Text)
    away_score = Column(Text)
    state = Column(Text)         # 'pre', 'in', 'post'
    period = Column(Integer)
    display_clock = Column(Text)
    detail = Column(Text)
    clock_running = Column(Boolean, default=False)
    scheduled_kickoff_ms = Column(BigInteger)
    captured_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
