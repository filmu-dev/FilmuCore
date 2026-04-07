"""Calendar compatibility route tests."""

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
from filmu_py.services.media import CalendarItemRecord


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
    """Deterministic media-service test double for calendar compatibility routes."""

    async def get_calendar_snapshot(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, CalendarItemRecord]:
        _ = (start_date, end_date)
        return {
            "item-1": CalendarItemRecord(
                item_id="item-1",
                tmdb_id="123",
                tvdb_id=None,
                show_title="Example Movie",
                item_type="episode",
                aired_at="2024-01-01T00:00:00+00:00",
                season=1,
                episode=1,
                last_state="Completed",
                release_data=None,
            )
        }


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


def test_calendar_route_returns_calendar_payload() -> None:
    client = _build_client()
    response = client.get(
        "/api/v1/calendar",
        params={
            "start_date": "2023-12-25T00:00:00+00:00",
            "end_date": "2024-01-10T00:00:00+00:00",
        },
        headers=_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert "item-1" in body["data"]
    assert body["data"]["item-1"]["show_title"] == "Example Movie"
    assert body["data"]["item-1"]["season"] == 1
    assert body["data"]["item-1"]["episode"] == 1


def test_calendar_route_requires_api_key() -> None:
    client = _build_client()
    response = client.get("/api/v1/calendar")
    assert response.status_code == 401
