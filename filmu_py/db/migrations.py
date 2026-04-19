"""Alembic migration runner utilities for startup and local operations."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config


def should_use_async_engine(sqlalchemy_url: str) -> bool:
    """Return whether Alembic should use SQLAlchemy's async engine path.

    The local container stack uses an async PostgreSQL DSN (`postgresql+asyncpg://...`).
    Alembic's sync template cannot open that URL directly, so the env runner must switch
    to `async_engine_from_config(...)` and `connection.run_sync(...)` for those URLs.
    """

    return "+asyncpg" in sqlalchemy_url or "+aiosqlite" in sqlalchemy_url


def _build_alembic_config(postgres_dsn: str) -> Config:
    """Build in-process Alembic config for the bundled migration environment."""

    cfg = Config()
    script_location = Path(__file__).resolve().parent / "alembic"
    cfg.set_main_option("script_location", str(script_location))
    cfg.set_main_option("sqlalchemy.url", postgres_dsn)
    return cfg


def run_migrations(postgres_dsn: str, revision: str = "head") -> None:
    """Upgrade the configured database to one Alembic revision."""

    cfg = _build_alembic_config(postgres_dsn)
    command.upgrade(cfg, revision)
