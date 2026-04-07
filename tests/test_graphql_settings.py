"""GraphQL settings and plugin registry behavior tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql import (
    GraphQLPluginRegistry,
    GraphQLResolverKind,
    create_graphql_router,
)
from filmu_py.resources import AppResources


class DummyRedis:
    """Minimal async Redis stub for non-networked GraphQL tests."""

    def ping(self, **kwargs: Any) -> bool:
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:  # pragma: no cover
        _ = close_connection_pool
        return None


class DummyDatabaseRuntime:
    """No-op DB runtime placeholder for resource wiring in tests."""

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[None, None]:
        yield None

    async def dispose(self) -> None:  # pragma: no cover
        return None


class FakeMediaService:
    """Placeholder media service for queries that do not hit item operations in these tests."""

    def __init__(self) -> None:
        self._noop = None


def _build_settings() -> Settings:
    """Return deterministic settings for GraphQL tests."""

    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
    )


def _build_test_client(registry: GraphQLPluginRegistry | None = None) -> TestClient:
    """Create a lightweight FastAPI app exposing only GraphQL router for tests."""

    plugin_registry = registry or GraphQLPluginRegistry()
    settings = _build_settings()
    redis = DummyRedis()

    app = FastAPI()
    resources = AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=FakeMediaService(),  # type: ignore[arg-type]
        graphql_plugin_registry=plugin_registry,
    )
    app.state.resources = resources
    app.include_router(create_graphql_router(plugin_registry), prefix="/graphql")

    return TestClient(app)


def test_graphql_settings_query_returns_filmu_fields() -> None:
    """`settings` query should expose the core `filmu` object with expected fields."""

    client = _build_test_client()
    query = """
        query {
          settings {
            filmu {
              version
              apiKey
              logLevel
            }
          }
        }
    """

    response = client.post("/graphql", json={"query": query})
    assert response.status_code == 200

    payload = response.json()
    assert "errors" not in payload
    filmu = payload["data"]["settings"]["filmu"]

    assert filmu["version"]
    assert filmu["apiKey"] == "a" * 32
    assert filmu["logLevel"] == "INFO"


def test_safe_register_many_skips_invalid_and_registers_valid() -> None:
    """Plugin registration safety hook should register valid classes and skip invalid entries."""

    registry = GraphQLPluginRegistry()

    class QueryResolver:
        pass

    registered, skipped = registry.safe_register_many(
        plugin_name="test-plugin",
        kind=GraphQLResolverKind.QUERY,
        resolvers=[QueryResolver, 123],
    )

    assert registered == 1
    assert len(skipped) == 1
    assert registry.resolvers_for(GraphQLResolverKind.QUERY) == [QueryResolver]


def test_safe_register_many_rejects_empty_plugin_name() -> None:
    """Empty plugin name must reject registration to enforce runtime boundary hygiene."""

    registry = GraphQLPluginRegistry()

    class QueryResolver:
        pass

    registered, skipped = registry.safe_register_many(
        plugin_name="   ",
        kind=GraphQLResolverKind.QUERY,
        resolvers=[QueryResolver],
    )

    assert registered == 0
    assert skipped == ["plugin_name is empty"]
    assert registry.resolvers_for(GraphQLResolverKind.QUERY) == []
