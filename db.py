"""Database connection and session factory.

Reads DATABASE_URL from the environment (auto-injected by Railway
when the PostgreSQL plugin is linked to the service). Also works
with external managed providers like Neon, Supabase, etc. Falls
back gracefully: if DATABASE_URL is missing, the app runs in pure
in-memory mode exactly as before.
"""
import os
import logging
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

log = logging.getLogger("db")

# asyncpg doesn't understand libpq-style query params like
# `sslmode=require` or `channel_binding=require` that Neon / Supabase
# append to their connection strings by default. Extract them from
# the URL and translate into a connect_args dict that asyncpg can
# consume.
_connect_args: dict = {}

_raw_url = os.environ.get("DATABASE_URL", "")
# Normalize scheme — Railway emits postgres://, asyncpg needs
# postgresql+asyncpg://. Managed providers usually emit
# postgresql:// which we also rewrite.
if _raw_url.startswith("postgres://"):
    _raw_url = _raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
elif _raw_url.startswith("postgresql://"):
    _raw_url = _raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)

if _raw_url:
    # Parse + strip unsupported query params, route them into
    # connect_args for asyncpg.
    try:
        parsed = urlparse(_raw_url)
        qs = dict(parse_qsl(parsed.query))
        # sslmode: asyncpg accepts ssl="require" / "allow" /
        # "disable" etc. via its own "ssl" connect kwarg. Translate
        # from libpq-style names.
        sslmode = qs.pop("sslmode", None)
        if sslmode:
            # Map libpq "sslmode=require" → asyncpg ssl="require"
            _connect_args["ssl"] = sslmode if sslmode != "disable" else False
        # channel_binding: libpq-only param, asyncpg ignores it.
        qs.pop("channel_binding", None)
        # If this is a Neon / Supabase hostname with no explicit
        # sslmode, force SSL on — both providers require it.
        if "ssl" not in _connect_args and parsed.hostname and any(
            marker in parsed.hostname
            for marker in ("neon.tech", "supabase.co", "supabase.com")
        ):
            _connect_args["ssl"] = "require"
        new_query = urlencode(qs)
        DATABASE_URL = urlunparse(parsed._replace(query=new_query))
    except Exception as e:
        log.warning("failed to parse DATABASE_URL (using raw): %s", e)
        DATABASE_URL = _raw_url
else:
    DATABASE_URL = ""

engine = None
async_session = None

if DATABASE_URL:
    try:
        from sqlalchemy.ext.asyncio import (
            create_async_engine,
            async_sessionmaker,
        )
        # Keep pool small so we stay well under providers' connection
        # limits (Railway: 20, Neon free: 100, Supabase free: 60).
        engine = create_async_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=2,
            # pool_pre_ping: issues a cheap SELECT 1 before handing
            # out a pooled connection. Catches stale/dead sockets
            # without a full exception round-trip.
            pool_pre_ping=True,
            # pool_recycle: force-recycle connections older than N
            # seconds. Defends against provider restarts that
            # invalidate every connection in the pool.
            pool_recycle=300,
            connect_args=_connect_args,
        )
        async_session = async_sessionmaker(engine, expire_on_commit=False)
        log.info(
            "database engine created (pool_size=5, pool_recycle=300, ssl=%s)",
            _connect_args.get("ssl", "default"),
        )
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
                            home_name=g.get("home_display") or g.get("home_name"),
                            away_name=g.get("away_display") or g.get("away_name"),
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

    Retries once on transient connection errors (Neon cold-start,
    stale pool, connection reset) by disposing the engine pool and
    reattempting. Tracks consecutive failures so the WS flush loop
    can surface the health via /api/ws_status.
    """
    if not DATABASE_URL or not rows:
        return
    global _flush_health
    for attempt in range(2):
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
                            volume=r.get("volume"),
                            open_interest=r.get("open_interest"),
                            source="ws",
                        )
                        for r in rows
                    ])
            log.info("price flush: %d rows inserted", len(rows))
            _flush_health["last_ok"] = __import__("time").time()
            _flush_health["consecutive_errors"] = 0
            _flush_health["last_error"] = None
            return
        except Exception as e:
            _flush_health["consecutive_errors"] = _flush_health.get("consecutive_errors", 0) + 1
            _flush_health["last_error"] = f"{type(e).__name__}: {e}"
            _flush_health["last_error_ts"] = __import__("time").time()
            log.error("price flush failed (attempt %d): %s", attempt + 1, e)
            if attempt == 0:
                # Dispose pool so next attempt gets fresh connections.
                try:
                    if engine is not None:
                        await engine.dispose()
                except Exception:
                    pass
                import asyncio as _a
                await _a.sleep(0.5)

# Tracks flush pipeline health — exposed via /api/ws_status so we
# can diagnose "WS is alive but writes aren't landing" scenarios
# without having to check Railway logs.
_flush_health: dict = {
    "last_ok": None,
    "consecutive_errors": 0,
    "last_error": None,
    "last_error_ts": None,
}


# ── Price data retention ─────────────────────────────────────────
# Neon free tier = 512 MB. At ~46 MB/hr of tick data, we can keep
# ~10 hours before hitting the wall. Default retention of 6 hours
# provides a safe margin and covers the 1H + 6H chart timeframes.
# The 24H and 7D tabs show partial data — acceptable for free tier.
# Upgrade to Neon Launch ($19/mo, 10 GB) for 7+ days of history.
PRICE_RETENTION_HOURS = int(os.environ.get("PRICE_RETENTION_HOURS", "6"))


_snapshots_table_ensured = False


async def _ensure_snapshots_table():
    """Idempotently create the snapshots table via raw DDL. Self-heals
    when a container started up before models.py was updated to include
    Snapshot, so init_db() ran without it. Uses CREATE TABLE IF NOT
    EXISTS so it's safe to call on every request (we cache after the
    first success)."""
    global _snapshots_table_ensured
    if _snapshots_table_ensured or engine is None:
        return None
    ddl = """
    CREATE TABLE IF NOT EXISTS snapshots (
        id VARCHAR(16) PRIMARY KEY,
        section TEXT NOT NULL,
        event_ticker TEXT,
        data JSONB NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        expires_at TIMESTAMP WITH TIME ZONE NOT NULL
    );
    """
    try:
        from sqlalchemy import text as _text
        async with engine.begin() as conn:
            await conn.execute(_text(ddl))
        _snapshots_table_ensured = True
        log.info("snapshots table ensured (raw DDL)")
        return None
    except Exception as e:
        log.warning("ensure snapshots table failed: %s", e)
        return str(e)[:400]


async def create_snapshot(section: str, data: dict, event_ticker: str = "",
                          ttl_days: int = 30):
    """Persist a snapshot and return (slug, None) on success or
    (None, error_message) on failure so the caller can surface the
    real error instead of swallowing it."""
    if not DATABASE_URL or async_session is None:
        return None, "database not configured (DATABASE_URL missing)"
    ensure_err = await _ensure_snapshots_table()
    if ensure_err:
        return None, f"failed to create snapshots table: {ensure_err}"
    try:
        import secrets
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        from models import Snapshot
        slug = secrets.token_urlsafe(8)[:12]
        expires = _dt.now(_tz.utc) + _td(days=ttl_days)
        async with async_session() as session:
            async with session.begin():
                session.add(Snapshot(
                    id=slug,
                    section=section,
                    event_ticker=event_ticker or None,
                    data=data,
                    expires_at=expires,
                ))
        return slug, None
    except Exception as e:
        log.error("create_snapshot failed: %s", e)
        return None, str(e)[:400]


async def get_snapshot(slug: str):
    """Return snapshot dict {section, data, created_at, expires_at,
    event_ticker} or None if not found / expired."""
    if not DATABASE_URL or async_session is None:
        return None
    await _ensure_snapshots_table()
    try:
        from datetime import datetime as _dt, timezone as _tz
        from models import Snapshot
        from sqlalchemy import select
        async with async_session() as session:
            stmt = select(Snapshot).where(Snapshot.id == slug)
            result = await session.execute(stmt)
            snap = result.scalar_one_or_none()
            if snap is None:
                return None
            if snap.expires_at and snap.expires_at < _dt.now(_tz.utc):
                return None
            return {
                "id": snap.id,
                "section": snap.section,
                "event_ticker": snap.event_ticker,
                "data": snap.data,
                "created_at": snap.created_at.isoformat() if snap.created_at else None,
                "expires_at": snap.expires_at.isoformat() if snap.expires_at else None,
            }
    except Exception as e:
        log.error("get_snapshot failed: %s", e)
        return None


async def prune_old_prices():
    """Delete price rows older than PRICE_RETENTION_HOURS. Called
    by the periodic pruning task and exposed via /api/prune."""
    if not DATABASE_URL or async_session is None:
        return 0
    try:
        from sqlalchemy import delete
        from models import Price
        from datetime import datetime, timezone as tz, timedelta
        cutoff = datetime.now(tz.utc) - timedelta(hours=PRICE_RETENTION_HOURS)
        async with async_session() as session:
            async with session.begin():
                result = await session.execute(
                    delete(Price).where(Price.captured_at < cutoff)
                )
                deleted = result.rowcount
        if deleted > 0:
            log.info("pruned %d price rows older than %dh", deleted, PRICE_RETENTION_HOURS)
        return deleted
    except Exception as e:
        log.error("price prune failed: %s", e)
        return -1


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


# ── Entity-based sport classifier ─────────────────────────────────
# Instead of relying solely on a hardcoded series_ticker → sport
# mapping in main.py, we auto-classify events by matching team
# aliases in the event title. Gets updated every entity-seed cycle.
#
# Format:  { normalized_alias_string : sport }
# Example: { "lakers": "Basketball", "arsenal": "Soccer", ... }
ALIAS_SPORT_CACHE: dict = {}


async def refresh_alias_sport_cache():
    """Load all entity aliases + their sports into ALIAS_SPORT_CACHE.
    Called after each entity seed cycle so the cache stays current."""
    if not DATABASE_URL or async_session is None:
        return
    try:
        from models import Entity, EntityAlias
        from sqlalchemy import select
        async with async_session() as session:
            # Join aliases to entities on entity_id, fetching sport
            # and the already-normalized alias string.
            stmt = select(
                EntityAlias.normalized,
                Entity.sport,
            ).join(Entity, EntityAlias.entity_id == Entity.id)
            rows = (await session.execute(stmt)).all()
            new_cache: dict = {}
            for norm, sport in rows:
                if not norm or not sport:
                    continue
                # If an alias maps to multiple sports (rare), first
                # writer wins. Could be improved with voting later.
                if norm not in new_cache:
                    new_cache[norm] = sport
            # Atomic replace so lookups never see a half-built cache.
            global ALIAS_SPORT_CACHE
            ALIAS_SPORT_CACHE = new_cache
            log.info("alias→sport cache refreshed: %d aliases", len(new_cache))
    except Exception as e:
        log.error("alias→sport cache refresh failed: %s", e)


def _normalize_title(s: str) -> str:
    """Same normalization as entity_seeder: lowercase + strip accents."""
    import unicodedata
    if not s:
        return ""
    s = unicodedata.normalize("NFD", str(s).lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.strip()


def get_sport_from_entities(title: str) -> str:
    """Synchronous lookup — scans a Kalshi event title for any
    entity alias and returns the matched entity's sport. Falls
    back to an empty string if no match.

    Intended as a fallback when the hardcoded series_ticker mapping
    returns nothing. Fast O(n) scan over aliases in the cache.
    """
    if not title or not ALIAS_SPORT_CACHE:
        return ""
    norm = _normalize_title(title)
    if not norm:
        return ""
    # Prefer longer aliases so "los angeles lakers" wins over "los".
    # Sorting aliases by length descending on every call would be
    # expensive — instead we sort once when building the cache, but
    # we can still do best-effort by checking all matches and
    # returning the longest hit.
    best_len = 0
    best_sport = ""
    for alias, sport in ALIAS_SPORT_CACHE.items():
        if len(alias) <= best_len:
            continue
        # Whole-word match: alias surrounded by word boundaries.
        # Fast check: alias must appear + not be inside a longer word.
        idx = norm.find(alias)
        if idx < 0:
            continue
        # Left boundary
        if idx > 0 and norm[idx - 1].isalnum():
            continue
        # Right boundary
        end = idx + len(alias)
        if end < len(norm) and norm[end].isalnum():
            continue
        best_len = len(alias)
        best_sport = sport
    return best_sport
