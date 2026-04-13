"""Database connection and session factory.

Reads DATABASE_URL from the environment (auto-injected by Railway
when the PostgreSQL plugin is linked to the service). Falls back
gracefully: if DATABASE_URL is missing, the app runs in pure
in-memory mode exactly as before.
"""
import os
import logging

log = logging.getLogger("db")

# Railway injects postgres:// but asyncpg needs postgresql+asyncpg://
_raw_url = os.environ.get("DATABASE_URL", "")
if _raw_url.startswith("postgres://"):
    DATABASE_URL = _raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
elif _raw_url.startswith("postgresql://"):
    DATABASE_URL = _raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)
else:
    DATABASE_URL = _raw_url

engine = None
async_session = None

if DATABASE_URL:
    try:
        from sqlalchemy.ext.asyncio import (
            create_async_engine,
            async_sessionmaker,
        )
        # Railway starter PostgreSQL has a 20-connection limit.
        # Keep pool small: feeds + request handlers should stay
        # well under 10 total connections.
        engine = create_async_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=2,
            pool_pre_ping=True,
        )
        async_session = async_sessionmaker(engine, expire_on_commit=False)
        log.info("database engine created (pool_size=5)")
    except Exception as e:
        log.warning("failed to create database engine: %s", e)
        engine = None
        async_session = None
else:
    log.info("DATABASE_URL not set — running in memory-only mode")


async def init_db():
    """Create all tables if they don't exist. Called once on startup.
    Safe to call repeatedly — uses CREATE TABLE IF NOT EXISTS."""
    if engine is None:
        return
    try:
        from models import Base
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        log.info("database tables ensured")
    except Exception as e:
        log.error("failed to initialize database tables: %s", e)


async def sync_events_to_db(records):
    """Upsert Kalshi events and their markets into the database.

    Called after each get_data() cache rebuild (~every 30 min).
    Uses PostgreSQL ON CONFLICT … DO UPDATE for idempotent upserts.
    Creates its own engine since this runs in a background thread
    with its own event loop (can't share the main loop's pool).
    """
    if not DATABASE_URL:
        return
    try:
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
        _engine = create_async_engine(DATABASE_URL, pool_size=2)
        _session = async_sessionmaker(_engine, expire_on_commit=False)
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from models import Event, Market
        from datetime import datetime, timezone as tz

        async with _session() as session:
            async with session.begin():
                ev_count = 0
                mk_count = 0
                for r in records:
                    et = r.get("event_ticker", "")
                    if not et:
                        continue

                    # Parse datetime strings back to datetime objects
                    def _parse_dt(val):
                        if not val:
                            return None
                        try:
                            from datetime import datetime as _dt
                            return _dt.fromisoformat(val)
                        except Exception:
                            return None

                    # Upsert event
                    ev_vals = {
                        "platform": "kalshi",
                        "event_ticker": et,
                        "series_ticker": r.get("series_ticker"),
                        "title": r.get("title", "")[:500],
                        "category": r.get("category"),
                        "sport": r.get("_sport") or None,
                        "subcat": r.get("_subcat") or None,
                        "kickoff_dt": _parse_dt(r.get("_kickoff_dt")),
                        "exp_dt": _parse_dt(r.get("_exp_dt")),
                        "close_dt": _parse_dt(r.get("_close_dt")),
                        "is_live": bool(r.get("_is_live")),
                        "updated_at": datetime.now(tz.utc),
                    }
                    stmt = pg_insert(Event).values(**ev_vals)
                    stmt = stmt.on_conflict_do_update(
                        constraint="uq_platform_event",
                        set_={
                            "title": stmt.excluded.title,
                            "category": stmt.excluded.category,
                            "sport": stmt.excluded.sport,
                            "subcat": stmt.excluded.subcat,
                            "kickoff_dt": stmt.excluded.kickoff_dt,
                            "exp_dt": stmt.excluded.exp_dt,
                            "close_dt": stmt.excluded.close_dt,
                            "is_live": stmt.excluded.is_live,
                            "updated_at": stmt.excluded.updated_at,
                        },
                    )
                    await session.execute(stmt)
                    ev_count += 1

                    # Upsert markets (outcomes)
                    for o in r.get("outcomes", []):
                        mk_ticker = o.get("ticker", "")
                        if not mk_ticker:
                            continue
                        mk_vals = {
                            "ticker": mk_ticker,
                            "label": o.get("label", "")[:200],
                            "event_id": None,  # filled by subquery below
                        }
                        # Use a subquery to get the event_id from the
                        # just-upserted event row. This avoids a
                        # separate SELECT round-trip.
                        from sqlalchemy import select, text
                        subq = select(Event.id).where(
                            Event.platform == "kalshi",
                            Event.event_ticker == et,
                        ).scalar_subquery()
                        mk_stmt = pg_insert(Market).values(
                            ticker=mk_ticker,
                            label=o.get("label", "")[:200],
                            event_id=subq,
                        )
                        mk_stmt = mk_stmt.on_conflict_do_update(
                            index_elements=["ticker"],
                            set_={
                                "label": mk_stmt.excluded.label,
                                "event_id": mk_stmt.excluded.event_id,
                            },
                        )
                        await session.execute(mk_stmt)
                        mk_count += 1

        await _engine.dispose()
        log.info("db sync: %d events, %d markets upserted", ev_count, mk_count)
    except Exception as e:
        log.error("db sync failed: %s", e)
        try:
            await _engine.dispose()
        except Exception:
            pass


async def sync_scores_to_db(source, games):
    """Replace all game_scores rows for a given source with the current
    live snapshot.  Called after each feed poll cycle (~10-30s).

    Each game dict should have: sport, league, home_display, away_display,
    home_score, away_score, state, period, display_clock, short_detail,
    clock_running, scheduled_kickoff_ms.
    """
    if not DATABASE_URL or async_session is None:
        return
    try:
        from models import GameScore
        from sqlalchemy import delete
        from datetime import datetime, timezone as tz

        async with async_session() as session:
            async with session.begin():
                # Wipe previous snapshot for this source
                await session.execute(
                    delete(GameScore).where(GameScore.source == source)
                )
                if games:
                    session.add_all([
                        GameScore(
                            source=source,
                            sport=g.get("sport", ""),
                            league=g.get("league"),
                            home_name=g.get("home_display"),
                            away_name=g.get("away_display"),
                            home_score=str(g.get("home_score", "")) if g.get("home_score") is not None else None,
                            away_score=str(g.get("away_score", "")) if g.get("away_score") is not None else None,
                            state=g.get("state"),
                            period=g.get("period"),
                            display_clock=g.get("display_clock"),
                            detail=g.get("short_detail"),
                            clock_running=bool(g.get("clock_running")),
                            scheduled_kickoff_ms=g.get("scheduled_kickoff_ms"),
                            captured_at=datetime.now(tz.utc),
                        )
                        for g in games
                    ])
        log.info("score sync [%s]: %d games written", source, len(games))
    except Exception as e:
        log.error("score sync [%s] failed: %s", source, e)


async def batch_insert_prices(rows):
    """Insert a batch of price snapshots into the prices table.

    Called every ~10s by the WebSocket flush task with accumulated
    updates. Uses a dedicated engine (same pattern as sync_events)
    since this runs in the main asyncio loop alongside the WS client.

    Each row is a dict with: market_ticker, yes_bid, yes_ask,
    no_bid, no_ask, last_price, source ('ws').
    """
    if not DATABASE_URL or not rows:
        return
    try:
        from models import Price
        async with async_session() as session:
            async with session.begin():
                session.add_all([
                    Price(
                        market_ticker=r.get("market_ticker", ""),
                        yes_bid=r.get("yes_bid"),
                        yes_ask=r.get("yes_ask"),
                        no_bid=r.get("no_bid"),
                        no_ask=r.get("no_ask"),
                        last_price=r.get("last_price"),
                        source="ws",
                    )
                    for r in rows
                ])
        log.info("price flush: %d rows inserted", len(rows))
    except Exception as e:
        log.error("price flush failed: %s", e)


async def upsert_entities(teams):
    """Upsert teams into the entities + entity_aliases tables.

    `teams` is a list of dicts from entity_seeder.extract_teams():
      canonical_name, entity_type, sport, league, aliases [...]

    Uses ON CONFLICT DO NOTHING for entities (keyed on canonical_name)
    and ON CONFLICT DO NOTHING for aliases (keyed on alias+source),
    so this is safe to call repeatedly with the same data.
    """
    if not DATABASE_URL or async_session is None or not teams:
        return
    try:
        from models import Entity, EntityAlias
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        from sqlalchemy import select

        new_entities = 0
        new_aliases = 0

        async with async_session() as session:
            async with session.begin():
                for t in teams:
                    canon = t.get("canonical_name", "")
                    if not canon:
                        continue

                    # Upsert entity (DO NOTHING on conflict — first
                    # writer wins the canonical name).
                    stmt = pg_insert(Entity).values(
                        canonical_name=canon,
                        entity_type=t.get("entity_type", "team"),
                        sport=t.get("sport"),
                        league=t.get("league"),
                    ).on_conflict_do_nothing(
                        index_elements=["canonical_name"],
                    )
                    result = await session.execute(stmt)
                    if result.rowcount > 0:
                        new_entities += 1

                    # Fetch the entity id (may have been created earlier).
                    row = await session.execute(
                        select(Entity.id).where(
                            Entity.canonical_name == canon
                        )
                    )
                    entity_id = row.scalar_one_or_none()
                    if entity_id is None:
                        continue

                    # Upsert aliases
                    for a in t.get("aliases", []):
                        alias_stmt = pg_insert(EntityAlias).values(
                            entity_id=entity_id,
                            alias=a["alias"],
                            source=a["source"],
                            normalized=a["normalized"],
                        ).on_conflict_do_nothing(
                            constraint="uq_alias_source",
                        )
                        r = await session.execute(alias_stmt)
                        if r.rowcount > 0:
                            new_aliases += 1

        if new_entities or new_aliases:
            log.info("entity seed: %d new entities, %d new aliases",
                     new_entities, new_aliases)
    except Exception as e:
        log.error("entity seed failed: %s", e)
