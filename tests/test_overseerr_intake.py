from __future__ import annotations

from dataclasses import dataclass, field
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
from filmu_py.services.media import ItemActionResult


class DummyDatabaseRuntime:
    async def dispose(self) -> None:
        return None


class DummyRedis:
    async def close(self) -> None:
        return None


@dataclass
class DummyMediaService:
    requests: list[dict[str, Any]] = field(default_factory=list)
    item_ids_by_ref: dict[str, str] = field(default_factory=dict)

    async def request_items_by_identifiers(
        self,
        *,
        media_type: str,
        identifiers: list[str] | None = None,
        tmdb_ids: list[str] | None = None,
        tvdb_ids: list[str] | None = None,
        requested_seasons: list[int] | None = None,
        requested_episodes: dict[str, list[int]] | None = None,
        request_source: str = "api",
        tenant_id: str = "global",
    ) -> ItemActionResult:
        self.requests.append(
            {
                "media_type": media_type,
                "identifiers": identifiers,
                "tmdb_ids": tmdb_ids,
                "tvdb_ids": tvdb_ids,
                "requested_seasons": requested_seasons,
                "requested_episodes": requested_episodes,
                "request_source": request_source,
                "tenant_id": tenant_id,
            }
        )
        resolved = identifiers or []
        if not resolved:
            raise ValueError("no identifiers supplied for requested media type")
        identifier = resolved[0]
        item_id = self.item_ids_by_ref.setdefault(identifier, f"item-{identifier.replace(':', '-')}")
        return ItemActionResult(message="Requested 1 item.", ids=[item_id])


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
    )


def _build_client(*, media_service: DummyMediaService | None = None) -> tuple[TestClient, DummyMediaService]:
    service = media_service or DummyMediaService()
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
        media_service=service,  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
    )
    app.include_router(create_api_router())
    return TestClient(app), service


def _headers() -> dict[str, str]:
    return {
        "x-api-key": "a" * 32,
        "x-actor-roles": "platform:admin",
        "x-tenant-id": "tenant-main",
    }


def test_overseerr_request_added_movie_payload_is_accepted() -> None:
    client, service = _build_client()

    response = client.post(
        "/api/v1/webhook/overseerr",
        json={
            "notification_type": "REQUEST_ADDED",
            "subject": "Movie requested",
            "media": {"media_type": "movie", "tmdbId": "123"},
        },
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {"status": "accepted", "item_id": "item-tmdb-123"}
    assert service.requests[-1]["media_type"] == "movie"
    assert service.requests[-1]["identifiers"] == ["tmdb:123"]
    assert service.requests[-1]["requested_seasons"] is None
    assert service.requests[-1]["request_source"] == "webhook:overseerr"
    assert service.requests[-1]["tenant_id"] == "tenant-main"


def test_overseerr_request_added_tv_payload_passes_partial_seasons() -> None:
    client, service = _build_client()

    response = client.post(
        "/api/v1/webhook/overseerr",
        json={
            "notification_type": "REQUEST_ADDED",
            "subject": "Show requested",
            "media": {"media_type": "tv", "tmdbId": "456"},
            "request": {"request_id": "req-1", "seasons": [1, 2]},
        },
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {"status": "accepted", "item_id": "item-tmdb-456"}
    assert service.requests[-1]["requested_seasons"] == [1, 2]
    assert service.requests[-1]["request_source"] == "webhook:overseerr"


def test_overseerr_non_actionable_notification_is_ignored() -> None:
    client, service = _build_client()

    response = client.post(
        "/api/v1/webhook/overseerr",
        json={
            "notification_type": "TEST_NOTIFICATION",
            "subject": "Ping",
            "media": {"media_type": "movie", "tmdbId": "123"},
        },
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {"status": "ignored", "reason": "TEST_NOTIFICATION"}
    assert service.requests == []


def test_overseerr_missing_tmdb_id_returns_422() -> None:
    client, _service = _build_client()

    response = client.post(
        "/api/v1/webhook/overseerr",
        json={
            "notification_type": "REQUEST_ADDED",
            "subject": "Broken payload",
            "media": {"media_type": "movie"},
        },
        headers=_headers(),
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "missing tmdbId in media block"


def test_overseerr_missing_api_key_returns_401() -> None:
    client, _service = _build_client()

    response = client.post(
        "/api/v1/webhook/overseerr",
        json={
            "notification_type": "REQUEST_ADDED",
            "subject": "Movie requested",
            "media": {"media_type": "movie", "tmdbId": "123"},
        },
    )

    assert response.status_code == 401


def test_overseerr_requires_admin_role() -> None:
    client, _service = _build_client()

    response = client.post(
        "/api/v1/webhook/overseerr",
        json={
            "notification_type": "REQUEST_ADDED",
            "subject": "Movie requested",
            "media": {"media_type": "movie", "tmdbId": "123"},
        },
        headers={"x-api-key": "a" * 32, "x-actor-roles": "webhook:viewer"},
    )

    assert response.status_code == 403


def test_overseerr_same_tmdb_id_is_idempotent() -> None:
    client, _service = _build_client()
    payload = {
        "notification_type": "REQUEST_ADDED",
        "subject": "Movie requested",
        "media": {"media_type": "movie", "tmdbId": "123"},
    }

    first = client.post("/api/v1/webhook/overseerr", json=payload, headers=_headers())
    second = client.post("/api/v1/webhook/overseerr", json=payload, headers=_headers())

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["item_id"] == second.json()["item_id"]
