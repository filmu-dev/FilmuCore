"""Scrape compatibility route tests."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.api.router import create_api_router
from filmu_py.api.routes import scrape as scrape_routes
from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql.plugin_registry import GraphQLPluginRegistry
from filmu_py.resources import AppResources
from filmu_py.services.media import ItemActionResult, MediaItemRecord, MediaItemSummaryRecord
from filmu_py.state.item import ItemState


class DummyRedis:
    """Minimal Redis stub used by route-level tests without network dependencies."""

    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None

    async def enqueue_job(self, function_name: str, *args: object, **kwargs: object) -> object:
        _ = (function_name, args, kwargs)
        return object()


class DummyDatabaseRuntime:
    """No-op DB runtime placeholder for application resources in tests."""

    async def dispose(self) -> None:
        return None


@dataclass
class DummyMediaService:
    """Deterministic media-service test double for scrape compatibility routes."""

    requested_refs: list[str]
    requested_payloads: list[dict[str, Any]]
    retried_ids: list[str]
    persisted_stream_candidates: list[tuple[str, tuple[str, ...], str | None]]
    transitioned: list[tuple[str, str, str | None]]
    detailed_items: dict[str, str]
    created_items: dict[str, MediaItemRecord]

    async def get_item(self, item_id: str) -> MediaItemRecord | None:
        created_item = self.created_items.get(item_id)
        if created_item is not None:
            return created_item
        if item_id == "item-1":
            return MediaItemRecord(
                id="item-1",
                external_ref="tmdb:123",
                title="Existing Movie",
                state=ItemState.REQUESTED,
            )
        if item_id == "item-indexed":
            return MediaItemRecord(
                id="item-indexed",
                external_ref="tmdb:456",
                title="Indexed Movie",
                state=ItemState.INDEXED,
            )
        if item_id == "item-terminal":
            return MediaItemRecord(
                id="item-terminal",
                external_ref="tmdb:789",
                title="Completed Movie",
                state=ItemState.COMPLETED,
            )
        if item_id == "item-tv-1":
            return MediaItemRecord(
                id="item-tv-1",
                external_ref="tvdb:777",
                title="Existing Show",
                state=ItemState.REQUESTED,
                attributes={"item_type": "show", "tvdb_id": "777"},
            )
        return None

    async def get_item_detail(
        self,
        item_identifier: str,
        *,
        media_type: str,
        extended: bool = False,
    ) -> MediaItemSummaryRecord | None:
        _ = (media_type, extended)
        resolved_id = self.detailed_items.get(item_identifier)
        if resolved_id is None:
            return None
        item = await self.get_item(resolved_id)
        if item is None:
            return None
        return MediaItemSummaryRecord(
            id=item.id,
            type="movie",
            title=item.title,
            state=item.state.value.title(),
            tmdb_id=item.attributes.get("tmdb_id")
            if isinstance(item.attributes.get("tmdb_id"), str)
            else None,
            external_ref=item.external_ref,
        )

    async def retry_items(self, ids: list[str]) -> ItemActionResult:
        self.retried_ids.extend(ids)
        return ItemActionResult(message="Items retried.", ids=ids)

    async def transition_item(
        self,
        item_id: str,
        event: Any,
        message: str | None = None,
    ) -> MediaItemRecord:
        self.transitioned.append((item_id, event.value, message))
        item = await self.get_item(item_id)
        if item is None:
            raise ValueError("Item not found")
        if event.value == "index":
            transitioned_item = MediaItemRecord(
                id=item.id,
                external_ref=item.external_ref,
                title=item.title,
                state=ItemState.INDEXED,
                attributes=item.attributes,
            )
            if item_id in self.created_items:
                self.created_items[item_id] = transitioned_item
            return transitioned_item
        return item

    async def request_item(
        self,
        external_ref: str,
        title: str | None = None,
        *,
        media_type: str | None = None,
        attributes: dict[str, object] | None = None,
        requested_seasons: list[int] | None = None,
        requested_episodes: dict[str, list[int]] | None = None,
    ) -> MediaItemRecord:
        self.requested_refs.append(external_ref)
        self.requested_payloads.append(
            {
                "external_ref": external_ref,
                "media_type": media_type,
                "requested_seasons": requested_seasons,
                "requested_episodes": requested_episodes,
                "attributes": attributes,
            }
        )
        existing_id = self.detailed_items.get(external_ref)
        if existing_id is not None:
            existing_item = await self.get_item(existing_id)
            if existing_item is not None:
                return existing_item
        created_item = MediaItemRecord(
            id=f"item-created-{len(self.created_items) + 1}",
            external_ref=external_ref,
            title=title or external_ref,
            state=ItemState.REQUESTED,
            attributes={"item_type": "movie" if media_type == "movie" else "show"},
        )
        self.created_items[created_item.id] = created_item
        self.detailed_items[external_ref] = created_item.id
        return created_item

    async def persist_parsed_stream_candidates(
        self,
        *,
        item_id: str,
        raw_titles: list[str],
        infohash: str | None = None,
    ) -> list[object]:
        self.persisted_stream_candidates.append((item_id, tuple(raw_titles), infohash))
        return []


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
    )


def _build_client() -> tuple[TestClient, DummyMediaService]:
    settings = _build_settings()
    redis = DummyRedis()
    media_service = DummyMediaService(
        requested_refs=[],
        requested_payloads=[],
        retried_ids=[],
        persisted_stream_candidates=[],
        transitioned=[],
        detailed_items={
            "tmdb:123": "item-1",
            "tmdb:456": "item-indexed",
            "tmdb:789": "item-terminal",
            "tvdb:777": "item-tv-1",
        },
        created_items={},
    )

    app = FastAPI()
    app.state.resources = AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=media_service,  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
        arq_queue_name="filmu-py",
    )

    async def fake_create_pool(*_args: object, **_kwargs: object) -> DummyRedis:
        return redis

    scrape_routes.create_pool = fake_create_pool  # type: ignore[assignment]
    app.include_router(create_api_router())
    return TestClient(app), media_service


def _headers() -> dict[str, str]:
    return {"x-api-key": "a" * 32}


def test_auto_scrape_requests_item_from_external_id() -> None:
    client, media_service = _build_client()
    response = client.post(
        "/api/v1/scrape/auto",
        json={"media_type": "movie", "tmdb_id": "123"},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {"message": "Scrape queued for Existing Movie"}
    assert media_service.transitioned == [("item-1", "index", "queued for scrape")]
    assert media_service.requested_refs == []
    assert media_service.retried_ids == []


def test_auto_scrape_retries_existing_item_by_id() -> None:
    client, media_service = _build_client()
    response = client.post(
        "/api/v1/scrape/auto",
        json={"media_type": "movie", "item_id": "item-1"},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {"message": "Scrape queued for Existing Movie"}
    assert media_service.transitioned == [("item-1", "index", "queued for scrape")]
    assert media_service.retried_ids == []
    assert media_service.requested_refs == []


def test_auto_scrape_requires_identifier() -> None:
    client, _ = _build_client()
    response = client.post(
        "/api/v1/scrape/auto",
        json={"media_type": "movie"},
        headers=_headers(),
    )

    assert response.status_code == 400
    assert "required" in response.json()["detail"]


def test_auto_scrape_returns_404_for_unknown_item_id() -> None:
    client, _ = _build_client()
    response = client.post(
        "/api/v1/scrape/auto",
        json={"media_type": "movie", "item_id": "missing"},
        headers=_headers(),
    )

    assert response.status_code == 404


def test_auto_scrape_creates_missing_tv_item_from_partial_request_payload() -> None:
    client, media_service = _build_client()
    response = client.post(
        "/api/v1/scrape/auto",
        json={
            "media_type": "tv",
            "tvdb_id": "555",
            "requested_seasons": [1, 2],
            "requested_episodes": {"1": [1, 2], "2": [1]},
        },
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {"message": "Scrape queued for tvdb:555"}
    assert media_service.requested_refs == ["tvdb:555"]
    assert media_service.requested_payloads == [
        {
            "external_ref": "tvdb:555",
            "media_type": "tv",
            "requested_seasons": [1, 2],
            "requested_episodes": {"1": [1, 2], "2": [1]},
            "attributes": None,
        }
    ]
    assert media_service.transitioned == [("item-created-1", "index", "queued for scrape")]


def test_auto_scrape_upserts_partial_scope_for_existing_tv_item() -> None:
    client, media_service = _build_client()
    response = client.post(
        "/api/v1/scrape/auto",
        json={
            "media_type": "tv",
            "tvdb_id": "777",
            "requested_seasons": [3],
            "requested_episodes": {"3": [1, 2]},
        },
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {"message": "Scrape queued for Existing Show"}
    assert media_service.requested_refs == ["tvdb:777"]
    assert media_service.requested_payloads == [
        {
            "external_ref": "tvdb:777",
            "media_type": "tv",
            "requested_seasons": [3],
            "requested_episodes": {"3": [1, 2]},
            "attributes": None,
        }
    ]
    assert media_service.transitioned == [("item-tv-1", "index", "queued for scrape")]


def test_auto_scrape_returns_422_for_terminal_state() -> None:
    client, _ = _build_client()
    response = client.post(
        "/api/v1/scrape/auto",
        json={"media_type": "movie", "tmdb_id": "789"},
        headers=_headers(),
    )

    assert response.status_code == 422


def test_scrape_routes_require_api_key() -> None:
    client, _ = _build_client()
    response = client.post("/api/v1/scrape/auto", json={"media_type": "movie", "tmdb_id": "123"})
    assert response.status_code == 401


def test_scrape_item_returns_empty_json_baseline() -> None:
    client, media_service = _build_client()
    response = client.get(
        "/api/v1/scrape",
        params={"media_type": "movie", "tmdb_id": "123"},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {
        "message": "Manually scraped streams for item tmdb:123",
        "streams": {},
    }
    assert media_service.requested_refs == []


def test_scrape_item_stream_returns_start_and_complete_events() -> None:
    client, _ = _build_client()
    response = client.get(
        "/api/v1/scrape",
        params={"media_type": "movie", "tmdb_id": "123", "stream": "true"},
        headers=_headers(),
    )

    assert response.status_code == 200
    chunks = [chunk for chunk in response.text.strip().split("\n\n") if chunk]
    payloads = [json.loads(chunk.removeprefix("data: ")) for chunk in chunks]
    assert payloads[0]["event"] == "start"
    assert payloads[0]["message"] == "Starting scrape for tmdb:123"
    assert payloads[-1]["event"] == "complete"
    assert payloads[-1]["total_streams"] == 0
    assert payloads[-1]["streams"] == {}


def test_scrape_item_requires_identifier() -> None:
    client, _ = _build_client()
    response = client.get(
        "/api/v1/scrape",
        params={"media_type": "movie"},
        headers=_headers(),
    )

    assert response.status_code == 400


def test_start_session_returns_pollable_real_scrape_payload() -> None:
    client, _ = _build_client()
    response = client.post(
        "/api/v1/scrape/start_session",
        params={"media_type": "movie", "tmdb_id": "123"},
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["session_id"]
    assert payload["torrent_info"]["infohash"] == payload["session_id"]
    assert payload["parsed_files"] == []
    assert payload["message"] == "Scrape queued for Existing Movie"


def test_session_poll_returns_current_item_state() -> None:
    client, _ = _build_client()
    start = client.post(
        "/api/v1/scrape/start_session",
        params={"media_type": "movie", "tmdb_id": "123"},
        headers=_headers(),
    )
    session_id = start.json()["session_id"]

    poll_response = client.get(
        f"/api/v1/scrape/session/{session_id}",
        headers=_headers(),
    )

    assert poll_response.status_code == 200
    assert poll_response.json() == {
        "session_id": session_id,
        "item_id": "item-1",
        "title": "Existing Movie",
        "state": "requested",
    }


def test_session_poll_returns_404_for_unknown_session() -> None:
    client, _ = _build_client()
    response = client.get(
        "/api/v1/scrape/session/missing",
        headers=_headers(),
    )

    assert response.status_code == 404


def test_session_routes_require_api_key() -> None:
    client, _ = _build_client()
    start_response = client.post(
        "/api/v1/scrape/start_session",
        params={"media_type": "movie", "tmdb_id": "123"},
    )
    assert start_response.status_code == 401
