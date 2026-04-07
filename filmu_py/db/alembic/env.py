"""Alembic environment for filmu-python async SQLAlchemy metadata."""

from __future__ import annotations

import asyncio
from logging.config import fileConfig
from typing import Any

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from filmu_py.db import models  # noqa: F401  # Ensure model metadata is imported.
from filmu_py.db.base import Base
from filmu_py.db.migrations import should_use_async_engine

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in offline mode."""

    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in online mode."""

    configuration = config.get_section(config.config_ini_section) or {}
    sqlalchemy_url = config.get_main_option("sqlalchemy.url") or ""

    if should_use_async_engine(sqlalchemy_url):
        asyncio.run(run_async_migrations(configuration))
        return

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


async def run_async_migrations(configuration: dict[str, str]) -> None:
    """Run migrations in online mode using SQLAlchemy's async engine path."""

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(_run_migrations_with_connection)

    await connectable.dispose()


def _run_migrations_with_connection(connection: Any) -> None:
    """Configure Alembic against one already-open sync-wrapped connection."""

    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
