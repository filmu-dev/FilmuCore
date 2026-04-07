"""Legacy `triven/*` compatibility route tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.api.router import create_api_router
from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql.plugin_registry import GraphQLPluginRegistry
from filmu_py.resources import AppResources
from filmu_py.services.media import MediaItemSummaryRecord


class DummyRedis:
    """Minimal Redis stub used by route-level tests without network dependencies."""

    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None


class DummyDatabaseRuntime:
    """No-op DB runtime placeholder for application resources in tests."""

    async def dispose(self) -> None:
        return None


@dataclass
class DummyMediaService:
    """Deterministic media-service test double for legacy triven routes."""

    async def get_item_detail(
        self,
        item_identifier: str,
        *,
        media_type: str,
        extended: bool,
    ) -> MediaItemSummaryRecord | None:
        _ = extended
        if item_identifier == "tvdb-123" and media_type == "tv":
            return MediaItemSummaryRecord(
                id="show-1",
                type="show",
                title="Example Show",
                tvdb_id="tvdb-123",
                playback_attachments=None,
            )
        if item_identifier == "tmdb-123" and media_type == "movie":
            return MediaItemSummaryRecord(
                id="movie-1",
                type="movie",
                title="Example Movie",
                tmdb_id="tmdb-123",
                playback_attachments=None,
            )
        if item_identifier == "item-123" and media_type == "item":
            return MediaItemSummaryRecord(
                id="item-123",
                type="season",
                title="Example Season",
                playback_attachments=None,
            )
        return None


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
    )


def _build_client() -> TestClient:
    settings = _build_settings()
    redis = DummyRedis()

    app = FastAPI()
    app.state.resources = AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=DummyMediaService(),  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
    )
    app.include_router(create_api_router())
    return TestClient(app)


def _headers() -> dict[str, str]:
    return {"x-api-key": "a" * 32}


def test_triven_item_returns_show_for_tv_lookup() -> None:
    client = _build_client()
    response = client.get("/api/v1/triven/item/tvdb-123", headers=_headers())

    assert response.status_code == 200
    assert response.json() == {"id": "show-1", "type": "show"}


def test_triven_item_returns_movie_for_movie_lookup() -> None:
    client = _build_client()
    response = client.get("/api/v1/triven/item/tmdb-123", headers=_headers())

    assert response.status_code == 200
    assert response.json() == {"id": "movie-1", "type": "movie"}


def test_triven_item_falls_back_to_item_lookup() -> None:
    client = _build_client()
    response = client.get("/api/v1/triven/item/item-123", headers=_headers())

    assert response.status_code == 200
    assert response.json() == {"id": "item-123", "type": "season"}


def test_triven_item_returns_404_when_not_found() -> None:
    client = _build_client()
    response = client.get("/api/v1/triven/item/missing", headers=_headers())
    assert response.status_code == 404


def test_triven_item_requires_api_key() -> None:
    client = _build_client()
    response = client.get("/api/v1/triven/item/tvdb-123")
    assert response.status_code == 401
