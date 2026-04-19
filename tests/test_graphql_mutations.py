"""GraphQL mutation tests for the future-facing write surface."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from pydantic import AnyUrl, SecretStr
from starlette.requests import Request

from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql import GraphQLPluginRegistry, build_schema
from filmu_py.graphql.deps import GraphQLContext
from filmu_py.resources import AppResources
from filmu_py.services.media import (
    ConsumerPlaybackActivityRecord,
    EnrichmentResult,
    ItemActionResult,
    MediaItemRecord,
    RequestItemServiceResult,
)
from filmu_py.state.item import ItemState


class DummyRedis:
    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None


class DummyDatabaseRuntime:
    @asynccontextmanager
    async def session(self) -> AsyncGenerator[None, None]:
        yield None

    async def dispose(self) -> None:
        return None


@dataclass
class FakeMediaService:
    requested_seasons_seen: list[int] | None = None
    requested_episodes_seen: dict[str, list[int]] | None = None
    updated_setting_path: str | None = None
    updated_setting_value: Any = None
    recorded_activity_calls: list[dict[str, Any]] = field(default_factory=list)
    item: MediaItemRecord = field(
        default_factory=lambda: MediaItemRecord(
            id="item-1",
            external_ref="tmdb:123",
            title="Example Movie",
            state=ItemState.REQUESTED,
            attributes={},
        )
    )

    async def request_items_by_identifiers(
        self,
        *,
        media_type: str,
        identifiers: list[str] | None = None,
        tmdb_ids: list[str] | None = None,
        tvdb_ids: list[str] | None = None,
        requested_seasons: list[int] | None = None,
        requested_episodes: dict[str, list[int]] | None = None,
    ) -> ItemActionResult:
        _ = (media_type, tmdb_ids, tvdb_ids)
        self.requested_seasons_seen = requested_seasons
        self.requested_episodes_seen = requested_episodes
        if identifiers:
            self.item = replace(self.item, external_ref=identifiers[0])
        return ItemActionResult(message="Requested 1 item.", ids=[self.item.id])

    async def get_item(self, item_id: str) -> MediaItemRecord | None:
        return self.item if item_id == self.item.id else None

    async def request_item_with_enrichment(
        self,
        external_ref: str,
        title: str | None = None,
        *,
        media_type: str | None = None,
        attributes: dict[str, object] | None = None,
        requested_seasons: list[int] | None = None,
        requested_episodes: dict[str, list[int]] | None = None,
    ) -> RequestItemServiceResult:
        _ = (title, media_type, attributes)
        self.requested_seasons_seen = requested_seasons
        self.requested_episodes_seen = requested_episodes
        self.item = replace(self.item, external_ref=external_ref)
        return RequestItemServiceResult(
            item=self.item,
            enrichment=EnrichmentResult(
                source="tmdb",
                has_poster=True,
                has_imdb_id=True,
                has_tmdb_id=True,
                warnings=[],
            ),
        )

    async def retry_items(self, ids: list[str]) -> ItemActionResult:
        return ItemActionResult(message="Items retried.", ids=list(ids))

    async def reset_items(self, ids: list[str]) -> ItemActionResult:
        return ItemActionResult(message="Items reset.", ids=list(ids))

    async def remove_items(self, ids: list[str]) -> ItemActionResult:
        return ItemActionResult(message="Items removed.", ids=list(ids))

    async def record_consumer_playback_activity(
        self,
        *,
        item_id: str,
        tenant_id: str,
        actor_id: str,
        actor_type: str,
        activity_kind: str,
        target: str | None = None,
        device_key: str,
        device_label: str,
        session_key: str | None = None,
        position_seconds: int | None = None,
        duration_seconds: int | None = None,
        completed: bool = False,
        occurred_at: datetime | None = None,
    ) -> None:
        self.recorded_activity_calls.append(
            {
                "item_id": item_id,
                "tenant_id": tenant_id,
                "actor_id": actor_id,
                "actor_type": actor_type,
                "activity_kind": activity_kind,
                "target": target,
                "device_key": device_key,
                "device_label": device_label,
                "session_key": session_key,
                "position_seconds": position_seconds,
                "duration_seconds": duration_seconds,
                "completed": completed,
                "occurred_at": occurred_at,
            }
        )

    async def get_consumer_playback_activity(
        self,
        *,
        tenant_id: str,
        actor_id: str,
        actor_type: str,
        item_limit: int = 12,
        device_limit: int = 6,
        history_limit: int = 240,
    ) -> ConsumerPlaybackActivityRecord:
        _ = (tenant_id, actor_id, actor_type, item_limit, device_limit, history_limit)
        return ConsumerPlaybackActivityRecord(generated_at=datetime.now(UTC).isoformat())


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
    )


def _build_resources(media_service: FakeMediaService) -> AppResources:
    settings = _build_settings()
    redis = DummyRedis()
    return AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=media_service,  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
    )


def _build_context(
    resources: AppResources,
    media_service: FakeMediaService,
    *,
    headers: dict[str, str] | None = None,
) -> GraphQLContext:
    normalized_headers = {
        key.lower().encode("latin-1"): value.encode("latin-1")
        for key, value in (headers or {}).items()
    }
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/graphql",
            "headers": list(normalized_headers.items()),
            "query_string": b"",
            "app": SimpleNamespace(state=SimpleNamespace(resources=resources)),
        }
    )

    async def _settings_updater(path: str, value: Any) -> bool:
        media_service.updated_setting_path = path
        media_service.updated_setting_value = value
        return True

    return GraphQLContext(
        request=request,
        resources=resources,
        media_service=resources.media_service,
        event_bus=resources.event_bus,
        log_stream=resources.log_stream,
        settings_updater=_settings_updater,
    )


def _execute_mutation(
    query: str,
    media_service: FakeMediaService,
    *,
    headers: dict[str, str] | None = None,
) -> Any:
    resources = _build_resources(media_service)
    result = asyncio.run(
        build_schema(resources.graphql_plugin_registry).execute(
            query,
            context_value=_build_context(resources, media_service, headers=headers),
        )
    )
    return result


def test_request_item_mutation_creates_item() -> None:
    media_service = FakeMediaService()

    result = _execute_mutation(
        'mutation { requestItem(input: { externalRef: "tmdb:123", mediaType: "movie" }) { itemId enrichmentSource hasPoster hasImdbId warnings } }',
        media_service,
    )

    assert result.errors is None
    payload = result.data["requestItem"]
    assert payload["itemId"] == "item-1"
    assert payload["enrichmentSource"] == "tmdb"
    assert payload["hasPoster"] is True
    assert payload["hasImdbId"] is True
    assert payload["warnings"] == []


def test_request_item_mutation_partial_seasons() -> None:
    media_service = FakeMediaService()

    result = _execute_mutation(
        'mutation { requestItem(input: { externalRef: "tmdb:123", mediaType: "tv", requestedSeasons: [1, 2] }) { itemId enrichmentSource } }',
        media_service,
    )

    assert result.errors is None
    assert media_service.requested_seasons_seen == [1, 2]
    assert result.data["requestItem"]["itemId"] == "item-1"


def test_request_item_mutation_partial_episodes() -> None:
    media_service = FakeMediaService()

    result = _execute_mutation(
        """
        mutation {
          requestItem(
            input: {
              externalRef: "tmdb:1399"
              mediaType: "tv"
              requestedEpisodes: [
                { seasonNumber: 1, episodeNumbers: [1, 2, 3] }
                { seasonNumber: 2, episodeNumbers: [4] }
              ]
            }
          ) {
            itemId
            enrichmentSource
          }
        }
        """,
        media_service,
    )

    assert result.errors is None
    assert media_service.requested_seasons_seen == [1, 2]
    assert media_service.requested_episodes_seen == {
        "1": [1, 2, 3],
        "2": [4],
    }
    assert result.data["requestItem"]["itemId"] == "item-1"


def test_item_action_retry_transitions_state() -> None:
    media_service = FakeMediaService()

    result = _execute_mutation(
        'mutation { itemAction(input: { itemId: "item-1", action: "retry" }) { item_id to_state } }',
        media_service,
    )

    assert result.errors is None
    assert result.data["itemAction"]["to_state"] == "requested"


def test_item_action_remove() -> None:
    media_service = FakeMediaService()

    result = _execute_mutation(
        'mutation { itemAction(input: { itemId: "item-1", action: "remove" }) { item_id to_state } }',
        media_service,
    )

    assert result.errors is None
    assert result.data["itemAction"]["to_state"] == "removed"


def test_item_action_unknown_raises() -> None:
    media_service = FakeMediaService()

    result = _execute_mutation(
        'mutation { itemAction(input: { itemId: "item-1", action: "explode" }) { item_id to_state } }',
        media_service,
    )

    assert result.errors is not None


def test_update_setting_mutation() -> None:
    media_service = FakeMediaService()

    result = _execute_mutation(
        'mutation { updateSetting(input: { path: "scraping.torrentio.enabled", value: false }) }',
        media_service,
    )

    assert result.errors is None
    assert result.data["updateSetting"] is True
    assert media_service.updated_setting_path == "scraping.torrentio.enabled"
    assert media_service.updated_setting_value is False


def test_record_consumer_playback_activity_mutation_tracks_shared_activity() -> None:
    media_service = FakeMediaService()

    result = _execute_mutation(
        """
        mutation {
          recordConsumerPlaybackActivity(
            input: {
              itemId: "item-1"
              activityType: PROGRESS
              sessionKey: "session-item-1"
              positionSeconds: 184
              durationSeconds: 7200
              deviceKey: "browser-firefox"
              deviceLabel: "Firefox on Windows"
            }
          ) {
            itemId
            activityType
            success
            occurredAt
          }
        }
        """,
        media_service,
        headers={
            "x-actor-id": "user-1",
            "x-actor-type": "user",
            "x-tenant-id": "tenant-main",
        },
    )

    assert result.errors is None
    payload = result.data["recordConsumerPlaybackActivity"]
    assert payload["itemId"] == "item-1"
    assert payload["activityType"] == "progress"
    assert payload["success"] is True
    assert payload["occurredAt"]
    assert media_service.recorded_activity_calls == [
        {
            "item_id": "item-1",
            "tenant_id": "tenant-main",
            "actor_id": "user-1",
            "actor_type": "user",
            "activity_kind": "progress",
            "target": None,
            "device_key": "browser-firefox",
            "device_label": "Firefox on Windows",
            "session_key": "session-item-1",
            "position_seconds": 184,
            "duration_seconds": 7200,
            "completed": False,
            "occurred_at": datetime.fromisoformat(payload["occurredAt"]),
        }
    ]


def test_graphql_schema_includes_mutation_type() -> None:
    schema_sdl = build_schema(GraphQLPluginRegistry()).as_str()

    assert "type Mutation" in schema_sdl
    assert "requestItem" in schema_sdl
    assert "itemAction" in schema_sdl
    assert "updateSetting" in schema_sdl
    assert "recordConsumerPlaybackActivity" in schema_sdl
