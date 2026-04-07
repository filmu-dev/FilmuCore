"""Async database runtime helpers and session factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

_POOL_RECYCLE_SECONDS = 1800


class DatabaseRuntime:
    """Application-scoped SQLAlchemy async engine and session provider."""

    def __init__(self, dsn: str, *, echo: bool = False) -> None:
        self._engine: AsyncEngine = create_async_engine(
            dsn,
            echo=echo,
            future=True,
            pool_pre_ping=True,
            pool_recycle=_POOL_RECYCLE_SECONDS,
        )
        self._session_factory = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=True,
            autoflush=False,
        )

    @property
    def engine(self) -> AsyncEngine:
        """Expose the configured async SQLAlchemy engine."""

        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """Expose the configured async session factory for advanced integrations/tests."""

        return self._session_factory

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Provide a transactional async session context manager."""

        async with self.session_factory() as session:
            yield session

    async def dispose(self) -> None:
        """Gracefully dispose engine connections at application shutdown."""

        await self._engine.dispose()
