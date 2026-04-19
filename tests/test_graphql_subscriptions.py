"""GraphQL compat subscription tests for the SSE-mirroring subscription surface."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import aclosing, asynccontextmanager
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr
from starlette.requests import Request
from strawberry.subscriptions import GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL
from strawberry.types.execution import PreExecutionError

from filmu_py.api.router import create_api_router
from filmu_py.api.routes import stream as stream_routes
from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql import GraphQLPluginRegistry, build_schema, create_graphql_router
from filmu_py.graphql.deps import GraphQLContext
from filmu_py.resources import AppResources
from filmu_py.services.media import (
    ItemRequestSummaryRecord,
    MediaItemSummaryRecord,
    ResolvedPlaybackAttachmentRecord,
    ResolvedPlaybackSnapshotRecord,
)


class DummyRedis:
    """Minimal async Redis stub for non-networked GraphQL subscription tests."""

    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:  # pragma: no cover
        _ = close_connection_pool
        return None


class DummyDatabaseRuntime:
    """No-op DB runtime placeholder for GraphQL subscription tests."""

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[None, None]:
        yield None

    async def dispose(self) -> None:  # pragma: no cover
        return None


class FakeMediaService:
    """Placeholder media service for subscription tests that do not hit item queries."""

    def __init__(self) -> None:
        self.detail: MediaItemSummaryRecord | None = None

    async def get_item_detail(
        self,
        item_identifier: str,
        *,
        media_type: str,
        extended: bool = False,
        tenant_id: str | None = None,
    ) -> MediaItemSummaryRecord | None:
        _ = (media_type, extended, tenant_id)
        if self.detail is None or self.detail.id != item_identifier:
            return None
        return self.detail

    async def get_stream_candidates(self, *, media_item_id: str) -> list[object]:
        _ = media_item_id
        return []

    async def get_recovery_plan(self, *, media_item_id: str) -> None:
        _ = media_item_id
        return None

    async def get_workflow_checkpoint(self, *, media_item_id: str) -> None:
        _ = media_item_id
        return None


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
    )


def _build_resources(
    registry: GraphQLPluginRegistry | None = None,
) -> tuple[GraphQLPluginRegistry, AppResources]:
    plugin_registry = registry or GraphQLPluginRegistry()
    redis = DummyRedis()
    resources = AppResources(
        settings=_build_settings(),
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=FakeMediaService(),  # type: ignore[arg-type]
        graphql_plugin_registry=plugin_registry,
    )
    return plugin_registry, resources


def _build_context(resources: AppResources) -> GraphQLContext:
    app = FastAPI()
    app.state.resources = resources
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/graphql",
            "app": app,
            "headers": [],
            "query_string": b"",
        }
    )

    async def _noop_settings_updater(path: str, value: Any) -> bool:
        _ = (path, value)
        return True

    return GraphQLContext(
        request=request,
        resources=resources,
        media_service=resources.media_service,
        event_bus=resources.event_bus,
        log_stream=resources.log_stream,
        settings_updater=_noop_settings_updater,
    )


def _build_client() -> tuple[TestClient, AppResources, GraphQLPluginRegistry]:
    registry, resources = _build_resources()
    app = FastAPI()
    app.state.resources = resources
    app.include_router(create_api_router())
    app.include_router(create_graphql_router(registry), prefix="/graphql")
    return TestClient(app), resources, registry


def _headers() -> dict[str, str]:
    return {"x-api-key": "a" * 32}


async def _collect_subscription_event(
    *,
    query: str,
    resources: AppResources,
    publish: AsyncGenerator[None, None] | None = None,
) -> dict[str, Any]:
    schema = build_schema(resources.graphql_plugin_registry)
    subscription = await schema.subscribe(query, context_value=_build_context(resources))
    if isinstance(subscription, PreExecutionError):
        pytest.fail(str(subscription.errors))

    async with aclosing(subscription):
        publisher = None
        if publish is not None:

            async def _run_publish() -> None:
                async for _ in publish:
                    return None

            publisher = asyncio.create_task(_run_publish())
        result = await asyncio.wait_for(anext(subscription), timeout=1.0)
        if publisher is not None:
            await publisher
        assert result.errors is None
        return cast(dict[str, Any], result.data)


async def _collect_query_data(
    *,
    query: str,
    resources: AppResources,
    variables: dict[str, object] | None = None,
) -> dict[str, Any]:
    schema = build_schema(resources.graphql_plugin_registry)
    result = await schema.execute(
        query,
        variable_values=variables,
        context_value=_build_context(resources),
    )
    assert result.errors is None
    return cast(dict[str, Any], result.data)


async def _publish_once(coro: Any) -> AsyncGenerator[None, None]:
    await asyncio.sleep(0)
    await coro
    yield None


@pytest.mark.asyncio
async def test_item_state_changed_subscription_receives_event() -> None:
    _, resources = _build_resources()
    payload = {
        "item_id": "item-1",
        "from_state": "requested",
        "to_state": "completed",
        "timestamp": "2026-03-20T23:00:00+00:00",
    }

    data = await _collect_subscription_event(
        query=(
            "subscription { "
            "itemStateChanged { item_id from_state to_state timestamp } "
            "}"
        ),
        resources=resources,
        publish=_publish_once(resources.event_bus.publish("item.state.changed", payload)),
    )

    assert data == {"itemStateChanged": payload}


def test_item_state_changed_fields_match_sse_payload() -> None:
    registry, resources = _build_resources()
    _ = resources
    schema_sdl = build_schema(registry).as_str()

    assert "type ItemStateChangedEvent" in schema_sdl
    assert "item_id: String!" in schema_sdl
    assert "from_state: String" in schema_sdl
    assert "to_state: String!" in schema_sdl
    assert "timestamp: String!" in schema_sdl


@pytest.mark.asyncio
async def test_log_stream_subscription_receives_log_events() -> None:
    _, resources = _build_resources()
    payload = {
        "level": "ERROR",
        "message": "boom",
        "event": "worker.failed",
        "timestamp": "2026-03-20T23:01:00+00:00",
        "worker_id": "worker-1",
        "item_id": "item-1",
        "stage": "rank_streams",
        "extra": {"error": "boom"},
    }

    async def publish() -> AsyncGenerator[None, None]:
        await asyncio.sleep(0)
        resources.log_stream.record(
            level=payload["level"],
            message="boom",
            timestamp=payload["timestamp"],
            event=payload["event"],
            worker_id=payload["worker_id"],
            item_id=payload["item_id"],
            stage=payload["stage"],
            extra=payload["extra"],
        )
        yield None

    data = await _collect_subscription_event(
        query="subscription { logStream { level message event timestamp worker_id item_id stage extra } }",
        resources=resources,
        publish=publish(),
    )

    assert data == {"logStream": payload}


@pytest.mark.asyncio
async def test_log_stream_subscription_filters_by_level_and_item_id() -> None:
    _, resources = _build_resources()

    async def publish() -> AsyncGenerator[None, None]:
        await asyncio.sleep(0)
        resources.log_stream.record(
            level="DEBUG",
            message="debug ignored",
            timestamp="2026-03-20T23:01:00+00:00",
            event="debug.event",
            item_id="item-1",
        )
        resources.log_stream.record(
            level="ERROR",
            message="wrong item ignored",
            timestamp="2026-03-20T23:01:01+00:00",
            event="error.other",
            item_id="item-2",
        )
        resources.log_stream.record(
            level="ERROR",
            message="chosen",
            timestamp="2026-03-20T23:01:02+00:00",
            event="error.match",
            worker_id="worker-1",
            item_id="item-1",
            stage="rank_streams",
            extra={"picked": True},
        )
        yield None

    data = await _collect_subscription_event(
        query='subscription { logStream(level: "ERROR", itemId: "item-1") { level message event timestamp worker_id item_id stage extra } }',
        resources=resources,
        publish=publish(),
    )

    assert data == {
        "logStream": {
            "level": "ERROR",
            "message": "chosen",
            "event": "error.match",
            "timestamp": "2026-03-20T23:01:02+00:00",
            "worker_id": "worker-1",
            "item_id": "item-1",
            "stage": "rank_streams",
            "extra": {"picked": True},
        }
    }


@pytest.mark.asyncio
async def test_notifications_subscription_receives_notifications() -> None:
    _, resources = _build_resources()
    payload = {
        "event_type": "item.completed",
        "title": "Completed",
        "message": "Movie done",
        "timestamp": "2026-03-20T23:02:00+00:00",
    }

    data = await _collect_subscription_event(
        query="subscription { notifications { event_type title message timestamp } }",
        resources=resources,
        publish=_publish_once(resources.event_bus.publish("notifications", payload)),
    )

    assert data == {"notifications": payload}


def test_sse_routes_still_work_after_subscription_wiring() -> None:
    client, resources, _ = _build_client()

    event_types = client.get("/api/v1/stream/event_types", headers=_headers())
    assert event_types.status_code == 200
    assert event_types.json() == {"event_types": ["logging"]}

    response = asyncio.run(
        stream_routes.stream_events(
            event_type="notifications",
            event_bus=resources.event_bus,
            log_stream=resources.log_stream,
        )
    )
    assert response.media_type == "text/event-stream"


def test_graphql_schema_includes_subscription_type() -> None:
    registry, _ = _build_resources()
    schema_sdl = build_schema(registry).as_str()
    router = create_graphql_router(registry)

    assert "type Subscription" in schema_sdl
    assert "itemStateChanged" in schema_sdl
    assert "logStream" in schema_sdl
    assert "notifications" in schema_sdl
    assert list(router.protocols) == [GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("item_state", "playback_ready", "expected_lifecycle_state"),
    [
        ("downloaded", True, "ready"),
        ("failed", False, "failed"),
    ],
)
async def test_item_state_subscription_stays_consistent_with_media_item_request_lifecycle(
    item_state: str,
    playback_ready: bool,
    expected_lifecycle_state: str,
) -> None:
    _, resources = _build_resources()
    detail = MediaItemSummaryRecord(
        id="item-1",
        type="movie",
        title="Ready Movie" if playback_ready else "Failed Movie",
        state=item_state,
        external_ref="tmdb:1",
        created_at="2026-04-19T10:00:00+00:00",
        updated_at="2026-04-19T10:05:00+00:00",
        request=ItemRequestSummaryRecord(
            is_partial=False,
            requested_seasons=None,
            requested_episodes=None,
            request_source="director",
        ),
        resolved_playback=(
            ResolvedPlaybackSnapshotRecord(
                direct=ResolvedPlaybackAttachmentRecord(
                    kind="remote-direct",
                    locator="https://edge.example.com/current-ready-movie",
                    source_key="persisted",
                    unrestricted_url="https://edge.example.com/current-ready-movie",
                ),
                hls=None,
                direct_ready=True,
                hls_ready=False,
                missing_local_file=False,
            )
            if playback_ready
            else None
        ),
    )
    cast(FakeMediaService, resources.media_service).detail = detail
    payload = {
        "item_id": "item-1",
        "from_state": "requested",
        "to_state": item_state,
        "timestamp": "2026-04-20T09:00:00+00:00",
    }

    event = await _collect_subscription_event(
        query=(
            "subscription { "
            "itemStateChanged { item_id from_state to_state timestamp } "
            "}"
        ),
        resources=resources,
        publish=_publish_once(resources.event_bus.publish("item.state.changed", payload)),
    )
    detail_data = await _collect_query_data(
        query=(
            "query MediaItem($id: ID!) { "
            "mediaItem(id: $id) { "
            "requestLifecycle { state playbackReady cta } "
            "} "
            "}"
        ),
        resources=resources,
        variables={"id": "item-1"},
    )

    assert event == {"itemStateChanged": payload}
    assert detail_data["mediaItem"]["requestLifecycle"]["state"] == expected_lifecycle_state
    assert detail_data["mediaItem"]["requestLifecycle"]["playbackReady"] is playback_ready
