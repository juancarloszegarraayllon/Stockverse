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
