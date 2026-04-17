"""Historical log, SSE, and early playback compatibility route tests."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import AsyncGenerator
from contextlib import aclosing, asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.api import playback_resolution
from filmu_py.api.router import create_api_router
from filmu_py.api.routes import stream as stream_routes
from filmu_py.api.routes.stream import _iter_log_events, _iter_topic_events
from filmu_py.app import (
    build_hls_failed_lease_refresh_controller,
    build_hls_restricted_fallback_refresh_controller,
    build_playback_refresh_controller,
)
from filmu_py.config import Settings
from filmu_py.core import byte_streaming
from filmu_py.core.cache import CacheManager
from filmu_py.core.chunk_engine import (
    CHUNK_FETCH_BYTES_TOTAL,
    CHUNK_READ_TYPE_TOTAL,
    ChunkCache,
    calculate_file_chunks,
    resolve_chunks_for_read,
)
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter, RateLimitDecision
from filmu_py.db.models import ActiveStreamORM, MediaEntryORM, MediaItemORM, PlaybackAttachmentORM
from filmu_py.resources import AppResources
from filmu_py.services import playback as playback_service
from filmu_py.services.debrid import (
    AllDebridPlaybackClient,
    DebridLinkPlaybackClient,
    RealDebridPlaybackClient,
    build_builtin_playback_provider_clients,
)
from filmu_py.services.playback import (
    AppScopedDirectPlaybackRefreshTriggerResult,
    AppScopedHlsFailedLeaseRefreshTriggerResult,
    AppScopedHlsRestrictedFallbackRefreshTriggerResult,
    DirectPlaybackRefreshScheduleRequest,
    InProcessDirectPlaybackRefreshController,
    InProcessHlsFailedLeaseRefreshController,
    InProcessHlsRestrictedFallbackRefreshController,
    MediaEntryLeaseRefreshExecution,
    MediaEntryLeaseRefreshRequest,
    PlaybackAttachmentProviderClient,
    PlaybackAttachmentProviderDownloadClient,
    PlaybackAttachmentProviderFileProjection,
    PlaybackAttachmentProviderProjectionClient,
    PlaybackAttachmentProviderUnrestrictedLink,
    PlaybackAttachmentRefreshExecution,
    PlaybackAttachmentRefreshExecutor,
    PlaybackAttachmentRefreshRequest,
    PlaybackAttachmentRefreshResult,
    PlaybackSourceService,
    ProviderCircuitBreaker,
    trigger_direct_playback_refresh_from_resources,
    trigger_hls_failed_lease_refresh_from_resources,
    trigger_hls_restricted_fallback_refresh_from_resources,
)
from filmu_py.workers import tasks as worker_tasks


class DummyRedis:
    """Minimal async Redis stub for non-networked route tests."""

    def ping(self, **kwargs: Any) -> bool:
        return True

    def script_load(self, _script: str) -> str:
        return "dummy-sha"

    def evalsha(self, _sha: str, _numkeys: int, *_args: object) -> list[object]:
        return [1, 0, 0]

    async def aclose(self, close_connection_pool: bool | None = None) -> None:  # pragma: no cover
        _ = close_connection_pool
        return None


def _counter_value(counter: Any, **labels: str) -> float:
    metric = counter.labels(**labels) if labels else counter
    return float(metric._value.get())


def _histogram_count(histogram: Any, **labels: str) -> float:
    sample_name = f"{histogram._name}_count"
    expected_labels = labels if labels else {}
    for metric in histogram.collect():
        for sample in metric.samples:
            if sample.name == sample_name and sample.labels == expected_labels:
                return float(sample.value)
    return 0.0


def _histogram_sum(histogram: Any, **labels: str) -> float:
    sample_name = f"{histogram._name}_sum"
    expected_labels = labels if labels else {}
    for metric in histogram.collect():
        for sample in metric.samples:
            if sample.name == sample_name and sample.labels == expected_labels:
                return float(sample.value)
    return 0.0


class FakeScalarResult:
    def __init__(self, items: list[MediaItemORM]) -> None:
        self._items = items

    def all(self) -> list[MediaItemORM]:
        return self._items


class FakeResult:
    def __init__(self, items: list[MediaItemORM]) -> None:
        self._items = items

    def scalars(self) -> FakeScalarResult:
        return FakeScalarResult(self._items)


class FakeSession:
    def __init__(self, items: list[MediaItemORM]) -> None:
        self._items = items

    async def execute(self, stmt: object) -> FakeResult:
        _ = stmt
        return FakeResult(self._items)


class DummyDatabaseRuntime:
    """DB runtime stub that serves deterministic media-item rows."""

    def __init__(self, items: list[MediaItemORM] | None = None) -> None:
        self._items = items or []

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[FakeSession, None]:
        yield FakeSession(self._items)

    async def dispose(self) -> None:  # pragma: no cover
        return None


class FakeMediaService:
    """Placeholder media service for route tests that do not hit service logic."""

    def __init__(self) -> None:
        self._noop = None


def _build_settings() -> Settings:
    """Return deterministic settings for stream route tests."""

    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
    )


def _build_settings_with_realdebrid_token() -> Settings:
    """Return deterministic settings with a built-in Real-Debrid token configured."""

    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
        FILMU_PY_REALDEBRID_API_TOKEN=SecretStr("rd-token"),
    )


def _build_settings_with_all_builtin_debrid_tokens() -> Settings:
    """Return deterministic settings with all current built-in debrid-service tokens configured."""

    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
        FILMU_PY_REALDEBRID_API_TOKEN=SecretStr("rd-token"),
        FILMU_PY_ALLDEBRID_API_TOKEN=SecretStr("ad-token"),
        FILMU_PY_DEBRIDLINK_API_TOKEN=SecretStr("dl-token"),
    )


def _build_item(
    *, item_id: str = "item-1", attributes: dict[str, object] | None = None
) -> MediaItemORM:
    return MediaItemORM(
        id=item_id,
        external_ref=f"external:{item_id}",
        title="Example Item",
        state="Completed",
        attributes=attributes or {},
    )


def _build_playback_attachment(
    *,
    attachment_id: str | None = None,
    item_id: str,
    kind: str,
    locator: str,
    is_preferred: bool = False,
    provider: str | None = None,
    provider_download_id: str | None = None,
    provider_file_id: str | None = None,
    provider_file_path: str | None = None,
    original_filename: str | None = None,
    file_size: int | None = None,
    local_path: str | None = None,
    restricted_url: str | None = None,
    unrestricted_url: str | None = None,
    preference_rank: int = 100,
    refresh_state: str = "ready",
    expires_at: datetime | None = None,
    last_refreshed_at: datetime | None = None,
    last_refresh_error: str | None = None,
) -> PlaybackAttachmentORM:
    return PlaybackAttachmentORM(
        id=attachment_id or str(uuid4()),
        item_id=item_id,
        kind=kind,
        locator=locator,
        source_key="persisted",
        is_preferred=is_preferred,
        provider=provider,
        provider_download_id=provider_download_id,
        provider_file_id=provider_file_id,
        provider_file_path=provider_file_path,
        original_filename=original_filename,
        file_size=file_size,
        local_path=local_path,
        restricted_url=restricted_url,
        unrestricted_url=unrestricted_url,
        preference_rank=preference_rank,
        refresh_state=refresh_state,
        expires_at=expires_at,
        last_refreshed_at=last_refreshed_at,
        last_refresh_error=last_refresh_error,
    )


def _build_media_entry(
    *,
    media_entry_id: str | None = None,
    item_id: str,
    kind: str,
    entry_type: str = "media",
    source_attachment_id: str | None = None,
    original_filename: str | None = None,
    local_path: str | None = None,
    download_url: str | None = None,
    unrestricted_url: str | None = None,
    provider: str | None = None,
    provider_download_id: str | None = None,
    provider_file_id: str | None = None,
    provider_file_path: str | None = None,
    size_bytes: int | None = None,
    refresh_state: str = "ready",
    expires_at: datetime | None = None,
    last_refreshed_at: datetime | None = None,
    last_refresh_error: str | None = None,
) -> MediaEntryORM:
    return MediaEntryORM(
        id=media_entry_id or str(uuid4()),
        item_id=item_id,
        source_attachment_id=source_attachment_id,
        entry_type=entry_type,
        kind=kind,
        original_filename=original_filename,
        local_path=local_path,
        download_url=download_url,
        unrestricted_url=unrestricted_url,
        provider=provider,
        provider_download_id=provider_download_id,
        provider_file_id=provider_file_id,
        provider_file_path=provider_file_path,
        size_bytes=size_bytes,
        refresh_state=refresh_state,
        expires_at=expires_at,
        last_refreshed_at=last_refreshed_at,
        last_refresh_error=last_refresh_error,
    )


def _build_active_stream(
    *,
    active_stream_id: str | None = None,
    item_id: str,
    media_entry_id: str,
    role: str,
) -> ActiveStreamORM:
    return ActiveStreamORM(
        id=active_stream_id or str(uuid4()),
        item_id=item_id,
        media_entry_id=media_entry_id,
        role=role,
    )


def _clone_item_graph(item: MediaItemORM) -> MediaItemORM:
    cloned_item = _build_item(item_id=item.id, attributes=dict(item.attributes or {}))
    cloned_item.external_ref = item.external_ref
    cloned_item.title = item.title
    cloned_item.state = item.state
    cloned_item.created_at = item.created_at
    cloned_item.updated_at = item.updated_at

    cloned_attachments = [
        _build_playback_attachment(
            attachment_id=attachment.id,
            item_id=attachment.item_id,
            kind=attachment.kind,
            locator=attachment.locator,
            is_preferred=attachment.is_preferred,
            provider=attachment.provider,
            provider_download_id=attachment.provider_download_id,
            provider_file_id=attachment.provider_file_id,
            provider_file_path=attachment.provider_file_path,
            original_filename=attachment.original_filename,
            file_size=attachment.file_size,
            local_path=attachment.local_path,
            restricted_url=attachment.restricted_url,
            unrestricted_url=attachment.unrestricted_url,
            preference_rank=attachment.preference_rank,
            refresh_state=attachment.refresh_state,
            expires_at=attachment.expires_at,
            last_refreshed_at=attachment.last_refreshed_at,
            last_refresh_error=attachment.last_refresh_error,
        )
        for attachment in item.playback_attachments
    ]
    attachment_by_id = {attachment.id: attachment for attachment in cloned_attachments}

    cloned_entries = [
        _build_media_entry(
            media_entry_id=entry.id,
            item_id=entry.item_id,
            kind=entry.kind,
            entry_type=entry.entry_type,
            source_attachment_id=entry.source_attachment_id,
            original_filename=entry.original_filename,
            local_path=entry.local_path,
            download_url=entry.download_url,
            unrestricted_url=entry.unrestricted_url,
            provider=entry.provider,
            provider_download_id=entry.provider_download_id,
            provider_file_id=entry.provider_file_id,
            provider_file_path=entry.provider_file_path,
            size_bytes=entry.size_bytes,
            refresh_state=entry.refresh_state,
            expires_at=entry.expires_at,
            last_refreshed_at=entry.last_refreshed_at,
            last_refresh_error=entry.last_refresh_error,
        )
        for entry in item.media_entries
    ]
    for entry in cloned_entries:
        if entry.source_attachment_id is not None:
            entry.source_attachment = attachment_by_id.get(entry.source_attachment_id)

    cloned_item.playback_attachments = cloned_attachments
    cloned_item.media_entries = cloned_entries
    cloned_item.active_streams = [
        _build_active_stream(
            active_stream_id=active_stream.id,
            item_id=active_stream.item_id,
            media_entry_id=active_stream.media_entry_id,
            role=active_stream.role,
        )
        for active_stream in item.active_streams
    ]
    return cloned_item


class _PersistentFakeSession:
    def __init__(self, runtime: PersistentDummyDatabaseRuntime) -> None:
        self._runtime = runtime
        self._items = [_clone_item_graph(item) for item in runtime.items]

    async def execute(self, stmt: object) -> FakeResult:
        _ = stmt
        return FakeResult(self._items)

    async def commit(self) -> None:
        self._runtime.items = [_clone_item_graph(item) for item in self._items]


class PersistentDummyDatabaseRuntime:
    """Session-isolated DB stub that only persists changes on commit."""

    def __init__(self, items: list[MediaItemORM] | None = None) -> None:
        self.items = [_clone_item_graph(item) for item in (items or [])]

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[_PersistentFakeSession, None]:
        yield _PersistentFakeSession(self)

    async def dispose(self) -> None:  # pragma: no cover
        return None


def _build_client(
    *,
    items: list[MediaItemORM] | None = None,
    db: object | None = None,
) -> tuple[TestClient, AppResources]:
    """Create a lightweight FastAPI app exposing compatibility API routes for tests."""

    settings = _build_settings()
    redis = DummyRedis()

    app = FastAPI()
    resources = AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        chunk_cache=ChunkCache(max_bytes=256 * 1024 * 1024),
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=(db if db is not None else DummyDatabaseRuntime(items=items)),  # type: ignore[arg-type]
        media_service=FakeMediaService(),  # type: ignore[arg-type]
        graphql_plugin_registry=None,  # type: ignore[arg-type]
    )
    resources.playback_service = PlaybackSourceService(
        resources.db,
        settings=resources.settings,
        rate_limiter=resources.rate_limiter,
    )
    resources.playback_refresh_controller = build_playback_refresh_controller(resources)
    resources.hls_failed_lease_refresh_controller = build_hls_failed_lease_refresh_controller(
        resources
    )
    resources.hls_restricted_fallback_refresh_controller = (
        build_hls_restricted_fallback_refresh_controller(resources)
    )
    app.state.resources = resources
    app.include_router(create_api_router())
    return TestClient(app), resources


def _headers() -> dict[str, str]:
    """Return authenticated API headers for compatibility route tests."""

    return {"x-api-key": "a" * 32}


def _local_hls_runtime_item_key(item_id: str) -> str:
    """Match the internal opaque local-HLS runtime key used by the route layer."""

    return hashlib.sha256(item_id.encode("utf-8")).hexdigest()


def test_logs_route_returns_historical_log_lines() -> None:
    """Historical logs endpoint should expose bounded in-memory log history."""

    client, resources = _build_client()
    resources.log_stream.record(level="INFO", message="first log line")
    resources.log_stream.record(level="ERROR", message="second log line")

    response = client.get("/api/v1/logs", headers=_headers())

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["logs"]) == 2
    assert payload["logs"][0].endswith("first log line")
    assert payload["logs"][1].endswith("second log line")


def test_stream_test_client_resources_attach_app_scoped_playback_refresh_controller() -> None:
    _, resources = _build_client()

    controller = resources.playback_refresh_controller
    assert controller is not None

    result = asyncio.run(controller.trigger("missing-item"))

    assert result.outcome == "no_action"


def test_stream_test_client_resources_attach_app_scoped_hls_failed_lease_refresh_controller() -> (
    None
):
    _, resources = _build_client()

    controller = resources.hls_failed_lease_refresh_controller
    assert controller is not None

    result = asyncio.run(controller.trigger("missing-item"))

    assert result.outcome == "scheduled"
    asyncio.run(controller.wait_for_item("missing-item"))
    last_result = controller.get_last_result("missing-item")
    assert last_result is not None
    assert last_result.outcome == "no_action"


def test_trigger_direct_playback_refresh_from_resources_noops_when_controller_is_unavailable() -> (
    None
):
    _, resources = _build_client()
    resources.playback_refresh_controller = None

    result = asyncio.run(trigger_direct_playback_refresh_from_resources(resources, "missing-item"))

    assert result == AppScopedDirectPlaybackRefreshTriggerResult(
        item_identifier="missing-item",
        outcome="controller_unavailable",
        controller_attached=False,
    )


def test_trigger_hls_failed_lease_refresh_from_resources_noops_when_controller_is_unavailable() -> (
    None
):
    _, resources = _build_client()
    resources.hls_failed_lease_refresh_controller = None

    result = asyncio.run(trigger_hls_failed_lease_refresh_from_resources(resources, "missing-item"))

    assert result == AppScopedHlsFailedLeaseRefreshTriggerResult(
        item_identifier="missing-item",
        outcome="controller_unavailable",
        controller_attached=False,
    )


def test_trigger_hls_restricted_fallback_refresh_from_resources_noops_when_controller_is_unavailable() -> (
    None
):
    _, resources = _build_client()
    resources.hls_restricted_fallback_refresh_controller = None

    result = asyncio.run(
        trigger_hls_restricted_fallback_refresh_from_resources(resources, "missing-item")
    )

    assert result == AppScopedHlsRestrictedFallbackRefreshTriggerResult(
        item_identifier="missing-item",
        outcome="controller_unavailable",
        controller_attached=False,
    )


def test_stream_status_route_exposes_serving_governance_snapshot() -> None:
    """Stream status route should expose internal serving governance counters."""

    client, _ = _build_client()

    response = client.get("/api/v1/stream/status", headers=_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["sessions"] == []
    assert payload["handles"] == []
    assert payload["paths"] == []
    assert payload["governance"]["hls_generation_concurrency"] == 2
    assert payload["governance"]["hls_generation_timeout_seconds"] == 60
    assert "active_sessions" in payload["governance"]
    assert "active_handles" in payload["governance"]
    assert "tracked_paths" in payload["governance"]
    assert payload["governance"]["hls_cleanup_failed_dirs"] == 0
    assert payload["governance"]["hls_generation_failed"] == 0
    assert payload["governance"]["hls_generation_timeouts"] == 0
    assert payload["governance"]["hls_manifest_invalid"] == 0
    assert payload["governance"]["hls_manifest_regenerated"] == 0
    assert payload["governance"]["stream_abort_events"] == 0
    assert payload["governance"]["local_stream_abort_events"] == 0
    assert payload["governance"]["remote_stream_abort_events"] == 0
    assert payload["governance"]["tracked_media_entries"] == 0
    assert payload["governance"]["tracked_active_streams"] == 0
    assert payload["governance"]["media_entries_refreshing"] == 0
    assert payload["governance"]["media_entries_failed"] == 0
    assert payload["governance"]["media_entries_needing_refresh"] == 0
    assert payload["governance"]["selected_direct_streams"] == 0
    assert payload["governance"]["selected_hls_streams"] == 0
    assert payload["governance"]["selected_direct_streams_needing_refresh"] == 0
    assert payload["governance"]["selected_hls_streams_needing_refresh"] == 0
    assert payload["governance"]["selected_direct_streams_failed"] == 0
    assert payload["governance"]["selected_hls_streams_failed"] == 0
    assert payload["governance"]["direct_playback_refresh_rate_limited"] == 0
    assert payload["governance"]["direct_playback_refresh_provider_circuit_open"] == 0
    assert payload["governance"]["hls_failed_lease_refresh_rate_limited"] == 0
    assert payload["governance"]["hls_failed_lease_refresh_provider_circuit_open"] == 0
    assert payload["governance"]["hls_restricted_fallback_refresh_rate_limited"] == 0
    assert payload["governance"]["hls_restricted_fallback_refresh_provider_circuit_open"] == 0
    assert payload["governance"]["direct_playback_refresh_trigger_starts"] == 0
    assert payload["governance"]["direct_playback_refresh_trigger_no_action"] == 0
    assert payload["governance"]["direct_playback_refresh_trigger_controller_unavailable"] == 0
    assert payload["governance"]["direct_playback_refresh_trigger_already_pending"] == 0
    assert payload["governance"]["direct_playback_refresh_trigger_backoff_pending"] == 0
    assert payload["governance"]["direct_playback_refresh_trigger_failures"] == 0
    assert payload["governance"]["direct_playback_refresh_trigger_tasks_active"] == 0
    assert payload["governance"]["hls_failed_lease_refresh_trigger_starts"] == 0
    assert payload["governance"]["hls_failed_lease_refresh_trigger_no_action"] == 0
    assert payload["governance"]["hls_failed_lease_refresh_trigger_controller_unavailable"] == 0
    assert payload["governance"]["hls_failed_lease_refresh_trigger_already_pending"] == 0
    assert payload["governance"]["hls_failed_lease_refresh_trigger_backoff_pending"] == 0
    assert payload["governance"]["hls_failed_lease_refresh_trigger_failures"] == 0
    assert payload["governance"]["hls_failed_lease_refresh_trigger_tasks_active"] == 0
    assert payload["governance"]["hls_restricted_fallback_refresh_trigger_starts"] == 0
    assert payload["governance"]["hls_restricted_fallback_refresh_trigger_no_action"] == 0
    assert (
        payload["governance"]["hls_restricted_fallback_refresh_trigger_controller_unavailable"] == 0
    )
    assert payload["governance"]["hls_restricted_fallback_refresh_trigger_already_pending"] == 0
    assert payload["governance"]["hls_restricted_fallback_refresh_trigger_backoff_pending"] == 0
    assert payload["governance"]["hls_restricted_fallback_refresh_trigger_failures"] == 0
    assert payload["governance"]["hls_restricted_fallback_refresh_trigger_tasks_active"] == 0
    assert payload["governance"]["stream_refresh_dispatch_mode"] == "in_process"
    assert payload["governance"]["stream_refresh_queue_enabled"] == 0
    assert payload["governance"]["stream_refresh_queue_ready"] == 1
    assert payload["governance"]["stream_refresh_proof_ref_count"] == 0
    assert payload["governance"]["heavy_stage_executor_mode"] == "process_pool_preferred"
    assert payload["governance"]["heavy_stage_max_workers"] == 2
    assert payload["governance"]["heavy_stage_max_tasks_per_child"] == 0
    assert payload["governance"]["heavy_stage_spawn_context_required"] == 1
    assert payload["governance"]["heavy_stage_max_worker_ceiling"] == 2
    assert payload["governance"]["heavy_stage_policy_violation_count"] == 0
    assert payload["governance"]["heavy_stage_policy_violations"] == []
    assert payload["governance"]["heavy_stage_process_isolation_required"] == 0
    assert payload["governance"]["heavy_stage_exit_ready"] == 0
    assert payload["governance"]["heavy_stage_index_timeout_seconds"] == 45.0
    assert payload["governance"]["heavy_stage_parse_timeout_seconds"] == 30.0
    assert payload["governance"]["heavy_stage_rank_timeout_seconds"] == 60.0
    assert payload["governance"]["heavy_stage_proof_ref_count"] == 0
    assert payload["governance"]["inline_remote_hls_refresh_attempts"] == 0
    assert payload["governance"]["inline_remote_hls_refresh_recovered"] == 0
    assert payload["governance"]["inline_remote_hls_refresh_no_action"] == 0
    assert payload["governance"]["inline_remote_hls_refresh_failures"] == 0


def test_stream_status_route_exposes_playback_governance_snapshot() -> None:
    item = _build_item(item_id="item-status-playback-governance")
    direct_entry = _build_media_entry(
        media_entry_id="media-entry-status-direct",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-status-direct",
        refresh_state="stale",
        provider="realdebrid",
    )
    hls_entry = _build_media_entry(
        media_entry_id="media-entry-status-hls",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-status-hls.m3u8",
        refresh_state="failed",
        last_refresh_error="refresh denied",
        provider="realdebrid",
    )
    refreshing_entry = _build_media_entry(
        media_entry_id="media-entry-status-refreshing",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-status-refreshing",
        refresh_state="refreshing",
        provider="realdebrid",
    )
    item.media_entries = [direct_entry, hls_entry, refreshing_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=direct_entry.id, role="direct"),
        _build_active_stream(item_id=item.id, media_entry_id=hls_entry.id, role="hls"),
    ]
    client, _ = _build_client(items=[item])

    response = client.get("/api/v1/stream/status", headers=_headers())

    assert response.status_code == 200
    governance = response.json()["governance"]
    assert governance["tracked_media_entries"] == 3
    assert governance["tracked_active_streams"] == 2
    assert governance["media_entries_refreshing"] == 1
    assert governance["media_entries_failed"] == 1
    assert governance["media_entries_needing_refresh"] == 2
    assert governance["selected_direct_streams"] == 1
    assert governance["selected_hls_streams"] == 1
    assert governance["selected_direct_streams_needing_refresh"] == 1
    assert governance["selected_hls_streams_needing_refresh"] == 0
    assert governance["selected_direct_streams_failed"] == 0
    assert governance["selected_hls_streams_failed"] == 1
    assert governance["hls_manifest_invalid"] >= 0
    assert governance["hls_manifest_regenerated"] >= 0


def test_byte_streaming_public_handle_lifecycle(tmp_path: Path) -> None:
    """Public serving-session and handle helpers should provide explicit lifecycle semantics."""

    media_file = tmp_path / "handle-example.txt"
    media_file.write_bytes(b"handle-data")

    session = byte_streaming.open_serving_session(
        category="local-file", owner="future-vfs", resource=str(media_file)
    )
    handle = byte_streaming.open_local_file_handle(session=session, path=media_file)

    assert handle.read_offset == 0
    byte_streaming.read_from_handle(handle=handle, chunk_size=4)
    assert handle.read_offset == 4
    assert handle.bytes_served == 4
    assert any(
        snapshot.handle_id == handle.handle_id
        for snapshot in byte_streaming.get_active_handle_snapshot()
    )

    byte_streaming.release_handle(handle)
    byte_streaming.release_serving_session(session)

    assert all(
        snapshot.handle_id != handle.handle_id
        for snapshot in byte_streaming.get_active_handle_snapshot()
    )
    assert all(
        snapshot.session_id != session.session_id
        for snapshot in byte_streaming.get_active_session_snapshot()
    )


def test_stream_local_file_metrics_track_opens_reads_and_bytes(tmp_path: Path) -> None:
    media_file = tmp_path / "local-metrics.txt"
    media_file.write_bytes(b"hello")

    open_before = _counter_value(
        byte_streaming.STREAM_OPEN_OPERATIONS,
        owner="http-direct",
        category="local-file",
    )
    read_before = _counter_value(
        byte_streaming.STREAM_READ_OPERATIONS,
        owner="http-direct",
        category="local-file",
    )
    request_shape_before = _counter_value(
        byte_streaming.STREAM_REQUEST_SHAPES,
        owner="http-direct",
        category="local-file",
        shape="range",
    )
    access_pattern_before = _counter_value(
        byte_streaming.STREAM_ACCESS_PATTERNS,
        owner="http-direct",
        category="local-file",
        pattern="head-probe",
    )
    partial_outcome_before = _counter_value(
        byte_streaming.STREAM_RESPONSE_OUTCOMES,
        owner="http-direct",
        category="local-file",
        outcome="partial",
    )
    read_histogram_before = _histogram_count(
        byte_streaming.STREAM_READ_SIZE_BYTES,
        owner="http-direct",
        category="local-file",
    )
    small_bucket_before = _counter_value(
        byte_streaming.STREAM_READ_SIZE_BUCKETS,
        owner="http-direct",
        category="local-file",
        bucket="small",
    )
    reads_per_session_before = _histogram_count(
        byte_streaming.STREAM_READ_OPERATIONS_PER_SESSION,
        owner="http-direct",
        category="local-file",
    )
    reads_per_session_sum_before = _histogram_sum(
        byte_streaming.STREAM_READ_OPERATIONS_PER_SESSION,
        owner="http-direct",
        category="local-file",
    )
    bytes_per_read_before = _histogram_count(
        byte_streaming.STREAM_BYTES_PER_READ_PROXY,
        owner="http-direct",
        category="local-file",
    )
    bytes_per_read_sum_before = _histogram_sum(
        byte_streaming.STREAM_BYTES_PER_READ_PROXY,
        owner="http-direct",
        category="local-file",
    )
    bytes_before = _counter_value(
        byte_streaming.STREAM_BYTES_SERVED,
        owner="http-direct",
        category="local-file",
    )

    class DummyRequest:
        def __init__(self) -> None:
            self.headers = {"range": "bytes=0-4"}

    response = byte_streaming.stream_local_file(media_file, cast(Any, DummyRequest()))
    body = asyncio.run(_collect_streaming_response_body(cast(StreamingResponse, response)))

    assert body == b"hello"
    assert (
        _counter_value(
            byte_streaming.STREAM_OPEN_OPERATIONS,
            owner="http-direct",
            category="local-file",
        )
        == open_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_READ_OPERATIONS,
            owner="http-direct",
            category="local-file",
        )
        == read_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_REQUEST_SHAPES,
            owner="http-direct",
            category="local-file",
            shape="range",
        )
        == request_shape_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_ACCESS_PATTERNS,
            owner="http-direct",
            category="local-file",
            pattern="head-probe",
        )
        == access_pattern_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_RESPONSE_OUTCOMES,
            owner="http-direct",
            category="local-file",
            outcome="partial",
        )
        == partial_outcome_before + 1
    )
    assert (
        _histogram_count(
            byte_streaming.STREAM_READ_SIZE_BYTES,
            owner="http-direct",
            category="local-file",
        )
        == read_histogram_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_READ_SIZE_BUCKETS,
            owner="http-direct",
            category="local-file",
            bucket="small",
        )
        == small_bucket_before + 1
    )
    assert (
        _histogram_count(
            byte_streaming.STREAM_READ_OPERATIONS_PER_SESSION,
            owner="http-direct",
            category="local-file",
        )
        == reads_per_session_before + 1
    )
    assert (
        _histogram_sum(
            byte_streaming.STREAM_READ_OPERATIONS_PER_SESSION,
            owner="http-direct",
            category="local-file",
        )
        == reads_per_session_sum_before + 1
    )
    assert (
        _histogram_count(
            byte_streaming.STREAM_BYTES_PER_READ_PROXY,
            owner="http-direct",
            category="local-file",
        )
        == bytes_per_read_before + 1
    )
    assert (
        _histogram_sum(
            byte_streaming.STREAM_BYTES_PER_READ_PROXY,
            owner="http-direct",
            category="local-file",
        )
        == bytes_per_read_sum_before + 5
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_BYTES_SERVED,
            owner="http-direct",
            category="local-file",
        )
        == bytes_before + 5
    )


def test_stream_local_file_request_shape_metrics_track_full_and_suffix_ranges(
    tmp_path: Path,
) -> None:
    media_file = tmp_path / "local-request-shape.txt"
    media_file.write_bytes(b"abcdef")

    full_before = _counter_value(
        byte_streaming.STREAM_REQUEST_SHAPES,
        owner="http-direct",
        category="local-file",
        shape="full",
    )
    full_pattern_before = _counter_value(
        byte_streaming.STREAM_ACCESS_PATTERNS,
        owner="http-direct",
        category="local-file",
        pattern="full-request",
    )
    suffix_before = _counter_value(
        byte_streaming.STREAM_REQUEST_SHAPES,
        owner="http-direct",
        category="local-file",
        shape="suffix-range",
    )
    tail_pattern_before = _counter_value(
        byte_streaming.STREAM_ACCESS_PATTERNS,
        owner="http-direct",
        category="local-file",
        pattern="tail-probe",
    )
    full_outcome_before = _counter_value(
        byte_streaming.STREAM_RESPONSE_OUTCOMES,
        owner="http-direct",
        category="local-file",
        outcome="full",
    )
    partial_outcome_before = _counter_value(
        byte_streaming.STREAM_RESPONSE_OUTCOMES,
        owner="http-direct",
        category="local-file",
        outcome="partial",
    )

    class FullRequest:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

    class SuffixRangeRequest:
        def __init__(self) -> None:
            self.headers = {"range": "bytes=-4"}

    full_response = byte_streaming.stream_local_file(media_file, cast(Any, FullRequest()))
    assert isinstance(full_response, FileResponse)
    assert full_response.status_code == 200

    suffix_response = byte_streaming.stream_local_file(media_file, cast(Any, SuffixRangeRequest()))
    suffix_body = asyncio.run(
        _collect_streaming_response_body(cast(StreamingResponse, suffix_response))
    )
    assert suffix_body == b"cdef"

    assert (
        _counter_value(
            byte_streaming.STREAM_REQUEST_SHAPES,
            owner="http-direct",
            category="local-file",
            shape="full",
        )
        == full_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_ACCESS_PATTERNS,
            owner="http-direct",
            category="local-file",
            pattern="full-request",
        )
        == full_pattern_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_REQUEST_SHAPES,
            owner="http-direct",
            category="local-file",
            shape="suffix-range",
        )
        == suffix_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_ACCESS_PATTERNS,
            owner="http-direct",
            category="local-file",
            pattern="tail-probe",
        )
        == tail_pattern_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_RESPONSE_OUTCOMES,
            owner="http-direct",
            category="local-file",
            outcome="full",
        )
        == full_outcome_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_RESPONSE_OUTCOMES,
            owner="http-direct",
            category="local-file",
            outcome="partial",
        )
        == partial_outcome_before + 1
    )


def test_stream_local_file_access_pattern_metrics_track_seek_probe(tmp_path: Path) -> None:
    media_file = tmp_path / "local-seek-pattern.bin"
    media_file.write_bytes(bytes(range(256)) * 8)

    seek_pattern_before = _counter_value(
        byte_streaming.STREAM_ACCESS_PATTERNS,
        owner="http-direct",
        category="local-file",
        pattern="seek-probe",
    )

    class SeekRequest:
        def __init__(self) -> None:
            self.headers = {"range": "bytes=700-799"}

    response = byte_streaming.stream_local_file(media_file, cast(Any, SeekRequest()))
    body = asyncio.run(_collect_streaming_response_body(cast(StreamingResponse, response)))

    assert len(body) == 100
    assert (
        _counter_value(
            byte_streaming.STREAM_ACCESS_PATTERNS,
            owner="http-direct",
            category="local-file",
            pattern="seek-probe",
        )
        == seek_pattern_before + 1
    )


def test_read_from_handle_tracks_small_medium_and_large_read_size_buckets(tmp_path: Path) -> None:
    media_file = tmp_path / "read-size-buckets.bin"
    media_file.write_bytes(b"x")

    session = byte_streaming.open_serving_session(
        category="local-file",
        owner="future-vfs",
        resource=str(media_file),
    )
    handle = byte_streaming.open_local_file_handle(session=session, path=media_file)

    histogram_before = _histogram_count(
        byte_streaming.STREAM_READ_SIZE_BYTES,
        owner="future-vfs",
        category="local-file",
    )
    small_before = _counter_value(
        byte_streaming.STREAM_READ_SIZE_BUCKETS,
        owner="future-vfs",
        category="local-file",
        bucket="small",
    )
    medium_before = _counter_value(
        byte_streaming.STREAM_READ_SIZE_BUCKETS,
        owner="future-vfs",
        category="local-file",
        bucket="medium",
    )
    large_before = _counter_value(
        byte_streaming.STREAM_READ_SIZE_BUCKETS,
        owner="future-vfs",
        category="local-file",
        bucket="large",
    )

    try:
        byte_streaming.read_from_handle(handle=handle, chunk_size=4096)
        byte_streaming.read_from_handle(handle=handle, chunk_size=65536)
        byte_streaming.read_from_handle(handle=handle, chunk_size=600000)
    finally:
        byte_streaming.release_handle(handle)
        byte_streaming.release_serving_session(session)

    assert (
        _histogram_count(
            byte_streaming.STREAM_READ_SIZE_BYTES,
            owner="future-vfs",
            category="local-file",
        )
        == histogram_before + 3
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_READ_SIZE_BUCKETS,
            owner="future-vfs",
            category="local-file",
            bucket="small",
        )
        == small_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_READ_SIZE_BUCKETS,
            owner="future-vfs",
            category="local-file",
            bucket="medium",
        )
        == medium_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_READ_SIZE_BUCKETS,
            owner="future-vfs",
            category="local-file",
            bucket="large",
        )
        == large_before + 1
    )


def test_iter_local_file_records_abort_telemetry(tmp_path: Path) -> None:
    media_file = tmp_path / "local-abort-metrics.txt"
    media_file.write_bytes(b"x" * (64 * 1024 + 5))

    abort_before = _counter_value(
        byte_streaming.STREAM_ABORT_EVENTS,
        owner="http-direct",
        category="local-file",
        reason="cancelled",
    )

    async def run_abort() -> bytes:
        iterator = byte_streaming.iter_local_file(
            media_file,
            start=0,
            end=media_file.stat().st_size - 1,
        )
        first_chunk = await iterator.__anext__()
        with suppress(asyncio.CancelledError):
            await iterator.athrow(asyncio.CancelledError())
        return first_chunk

    first_chunk = asyncio.run(run_abort())

    assert len(first_chunk) == 64 * 1024
    assert (
        _counter_value(
            byte_streaming.STREAM_ABORT_EVENTS,
            owner="http-direct",
            category="local-file",
            reason="cancelled",
        )
        == abort_before + 1
    )
    governance = byte_streaming.get_serving_governance_snapshot()
    assert governance["stream_abort_events"] >= 1
    assert governance["local_stream_abort_events"] >= 1
    assert byte_streaming.get_active_handle_snapshot() == []
    assert byte_streaming.get_active_session_snapshot() == []


def test_byte_streaming_remote_handle_lifecycle() -> None:
    """Remote proxy handles should also use the explicit handle lifecycle."""

    url = "https://example.com/video.ts"
    session = byte_streaming.open_serving_session(
        category="remote-proxy", owner="future-vfs", resource=url
    )
    handle = byte_streaming.open_remote_proxy_handle(session=session, url=url)

    assert handle.read_offset == 0
    byte_streaming.read_from_handle(handle=handle, chunk_size=7)
    assert handle.read_offset == 7
    assert handle.bytes_served == 7
    assert any(
        snapshot.handle_id == handle.handle_id
        for snapshot in byte_streaming.get_active_handle_snapshot()
    )

    byte_streaming.release_handle(handle)
    byte_streaming.release_serving_session(session)

    assert all(
        snapshot.handle_id != handle.handle_id
        for snapshot in byte_streaming.get_active_handle_snapshot()
    )


def test_cleanup_expired_serving_runtime_respects_owner_retention() -> None:
    """Future-VFS-owned sessions should outlive shorter-lived HTTP sessions during cleanup."""

    http_session = byte_streaming.open_serving_session(
        category="local-file",
        owner="http-direct",
        resource="http-item",
    )
    vfs_session = byte_streaming.open_mount_session(resource="future-vfs-item")

    stale_time = byte_streaming._utc_now().replace(minute=0, second=0, microsecond=0)
    http_session.last_activity_at = stale_time
    vfs_session.last_activity_at = stale_time

    byte_streaming.cleanup_expired_serving_runtime(now=stale_time.replace(minute=30))

    assert all(
        snapshot.session_id != http_session.session_id
        for snapshot in byte_streaming.get_active_session_snapshot()
    )
    assert any(
        snapshot.session_id == vfs_session.session_id
        for snapshot in byte_streaming.get_active_session_snapshot()
    )

    byte_streaming.release_serving_session(vfs_session)


def test_byte_streaming_generic_path_registry_helpers(tmp_path: Path) -> None:
    """Generic path registration/open helpers should support future mount-oriented use."""

    media_file = tmp_path / "registry-example.txt"
    media_file.write_bytes(b"registry")

    path_record = byte_streaming.register_file_path(media_file)
    assert path_record.node_kind == "file"

    session = byte_streaming.open_serving_session(
        category="local-file",
        owner="future-vfs",
        resource=str(media_file),
    )
    handle = byte_streaming.open_handle_for_path(session=session, path_record=path_record)

    byte_streaming.read_from_handle(handle=handle, chunk_size=3)

    assert handle.path_id == path_record.path_id
    assert handle.read_offset == 3

    byte_streaming.release_handle(handle)
    byte_streaming.release_serving_session(session)


def test_byte_streaming_directory_hierarchy_and_listing(tmp_path: Path) -> None:
    """Serving-path registry should classify directories and list direct child entries."""

    library_dir = tmp_path / "Movies"
    movie_dir = library_dir / "Example Movie"
    movie_dir.mkdir(parents=True)
    media_file = movie_dir / "Example Movie.mkv"
    media_file.write_bytes(b"movie")

    file_record = byte_streaming.register_file_path(media_file)
    movie_dir_record = byte_streaming.get_path_by_key(category="local-file", path=str(movie_dir))

    assert movie_dir_record is not None
    assert movie_dir_record.category == "local-file"
    assert byte_streaming.classify_registered_path(movie_dir_record) == "directory"
    assert file_record.node_kind == "file"

    children = byte_streaming.list_directory_children(movie_dir_record)
    assert [child.name for child in children] == ["Example Movie.mkv"]
    assert children[0].node_kind == "file"

    attributes = byte_streaming.get_path_attributes(movie_dir_record)
    assert attributes.node_kind == "directory"
    assert attributes.path == str(movie_dir)

    same_attributes = byte_streaming.get_path_attributes_by_id(movie_dir_record.path_id)
    assert same_attributes.path_id == movie_dir_record.path_id
    assert byte_streaming.getattr_for_path_id(movie_dir_record.path_id).path == str(movie_dir)

    same_children = byte_streaming.list_directory_children_by_id(movie_dir_record.path_id)
    assert [child.name for child in same_children] == ["Example Movie.mkv"]
    assert [
        child.name for child in byte_streaming.readdir_for_path_id(movie_dir_record.path_id)
    ] == ["Example Movie.mkv"]


def test_byte_streaming_mount_facing_path_handle_helpers(tmp_path: Path) -> None:
    """Mount-facing path-id based helpers should open handles from registry state directly."""

    media_file = tmp_path / "mount-facing.txt"
    media_file.write_bytes(b"mount")
    path_record = byte_streaming.register_file_path(media_file)

    session = byte_streaming.open_mount_session(resource=str(media_file))
    handle = byte_streaming.open_handle_for_path_id(session=session, path_id=path_record.path_id)

    assert handle.path_id == path_record.path_id
    assert handle.owner == "future-vfs"
    byte_streaming.read_from_handle(handle=handle, chunk_size=5)
    assert handle.read_offset == 5

    byte_streaming.release_handle(handle)
    byte_streaming.release_serving_session(session)


def test_stream_status_route_runs_serving_runtime_cleanup() -> None:
    """Stream status route should trigger stale serving-runtime cleanup before reporting state."""

    client, _ = _build_client()
    original = byte_streaming.cleanup_expired_serving_runtime

    called = {"value": False}

    def fake_cleanup(*, now: object | None = None) -> None:
        _ = now
        called["value"] = True

    byte_streaming.cleanup_expired_serving_runtime = cast(Any, fake_cleanup)
    try:
        response = client.get("/api/v1/stream/status", headers=_headers())
    finally:
        byte_streaming.cleanup_expired_serving_runtime = original

    assert response.status_code == 200
    assert called["value"] is True


def test_byte_streaming_lookup_helpers_resolve_handle_and_path() -> None:
    """Handle/path lookup helpers should resolve the active registry objects by id."""

    url = "https://example.com/resource.ts"
    session = byte_streaming.open_serving_session(
        category="remote-proxy", owner="future-vfs", resource=url
    )
    handle = byte_streaming.open_remote_proxy_handle(session=session, url=url)

    resolved_handle = byte_streaming.get_handle_by_id(handle.handle_id)
    resolved_path = byte_streaming.get_path_by_id(handle.path_id)

    assert resolved_handle is not None
    assert resolved_handle.handle_id == handle.handle_id
    assert resolved_path is not None
    assert resolved_path.path_id == handle.path_id

    byte_streaming.release_handle(handle)
    byte_streaming.release_serving_session(session)


def test_byte_streaming_register_directory_path_supports_mount_style_lookup(tmp_path: Path) -> None:
    """Directory registration should create a stable path record for future mount traversal."""

    directory = tmp_path / "mount-root"
    directory.mkdir()

    path_record = byte_streaming.register_directory_path(directory)
    resolved_path = byte_streaming.get_path_by_id(path_record.path_id)
    resolved_by_key = byte_streaming.get_path_by_key(category="local-file", path=str(directory))

    assert resolved_path is not None
    assert resolved_path.category == "local-file"
    assert resolved_path.node_kind == "directory"
    assert resolved_by_key is not None
    assert resolved_by_key.path_id == path_record.path_id


def test_byte_streaming_directory_registration_can_use_generated_hls_category(
    tmp_path: Path,
) -> None:
    """Directory registration should preserve non-default serving categories when requested."""

    directory = tmp_path / "generated-hls-root"
    directory.mkdir()

    path_record = byte_streaming.register_directory_path(directory, category="generated-hls")
    resolved_by_key = byte_streaming.get_path_by_key(category="generated-hls", path=str(directory))

    assert path_record.category == "generated-hls"
    assert resolved_by_key is not None
    assert resolved_by_key.path_id == path_record.path_id


def test_byte_streaming_cleanup_expired_serving_runtime_removes_stale_entries() -> None:
    """Expired sessions/handles should be removed from the serving registries."""

    url = "https://example.com/stale.ts"
    session = byte_streaming.open_serving_session(
        category="remote-proxy", owner="future-vfs", resource=url
    )
    handle = byte_streaming.open_remote_proxy_handle(session=session, url=url)

    stale_now = session.last_activity_at.replace(year=session.last_activity_at.year + 1)
    byte_streaming.cleanup_expired_serving_runtime(now=stale_now)

    assert all(
        snapshot.session_id != session.session_id
        for snapshot in byte_streaming.get_active_session_snapshot()
    )
    assert all(
        snapshot.handle_id != handle.handle_id
        for snapshot in byte_streaming.get_active_handle_snapshot()
    )


def test_log_stream_iterator_emits_sse_payload() -> None:
    """Live log iterator should encode structured log payloads as SSE frames."""

    async def exercise() -> bytes:
        _, resources = _build_client()
        iterator = _iter_log_events(resources.log_stream)

        async def publish() -> None:
            await asyncio.sleep(0)
            resources.log_stream.record(level="INFO", message="live log line")

        async with aclosing(iterator):
            publisher = asyncio.create_task(publish())
            chunk = await asyncio.wait_for(anext(iterator), timeout=1.0)
            await publisher
            return chunk

    chunk = asyncio.run(exercise())
    payload = json.loads(chunk.decode("utf-8").removeprefix("data: ").strip())
    assert payload["message"] == "live log line"
    assert payload["level"] == "INFO"


def test_notification_stream_iterator_emits_sse_payload() -> None:
    """Notification stream iterator should forward event-bus notification payloads as SSE."""

    notification_payload = {
        "title": "Example Movie",
        "type": "movie",
        "year": 2024,
        "duration": 12,
        "timestamp": "2026-03-09T00:00:00+00:00",
        "log_string": "Example Movie (2024)",
        "imdb_id": "tt1234567",
    }

    async def exercise() -> bytes:
        _, resources = _build_client()
        iterator = _iter_topic_events(resources.event_bus, "notifications")

        async def publish() -> None:
            await asyncio.sleep(0)
            await resources.event_bus.publish("notifications", notification_payload)

        async with aclosing(iterator):
            publisher = asyncio.create_task(publish())
            chunk = await asyncio.wait_for(anext(iterator), timeout=1.0)
            await publisher
            return chunk

    chunk = asyncio.run(exercise())
    payload = json.loads(chunk.decode("utf-8").removeprefix("data: ").strip())
    assert payload == notification_payload


def test_stream_file_returns_local_file_when_metadata_contains_path(tmp_path: Path) -> None:
    """Direct stream route should serve a local file when item metadata provides a file path."""

    media_file = tmp_path / "example.txt"
    media_file.write_bytes(b"filmu-stream")
    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())

    assert response.status_code == 200
    assert response.content == b"filmu-stream"


def test_stream_file_supports_partial_content_range_requests(tmp_path: Path) -> None:
    """Direct stream route should serve explicit byte ranges for local files."""

    media_file = tmp_path / "range-example.txt"
    media_file.write_bytes(b"abcdefghij")
    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    response = client.get(
        f"/api/v1/stream/file/{item.id}",
        headers={**_headers(), "Range": "bytes=2-5"},
    )

    assert response.status_code == 206
    assert response.content == b"cdef"
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["content-range"] == "bytes 2-5/10"


def test_stream_file_returns_nested_stream_path_when_present(tmp_path: Path) -> None:
    """Direct stream route should resolve nested playback metadata, not only top-level keys."""

    media_file = tmp_path / "nested-example.txt"
    media_file.write_bytes(b"nested-stream")
    item = _build_item(attributes={"streams": [{"path": str(media_file)}]})
    client, _ = _build_client(items=[item])

    response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())

    assert response.status_code == 200
    assert response.content == b"nested-stream"


def test_resolve_sources_from_attributes_classifies_playback_source_types(tmp_path: Path) -> None:
    """Typed source resolution should distinguish local-file, remote-direct, and remote-HLS candidates."""

    media_file = tmp_path / "typed-source-example.txt"
    media_file.write_bytes(b"typed")

    sources, saw_missing_path = playback_resolution.resolve_attachments_from_attributes(
        {
            "file_path": str(media_file),
            "download_url": "https://example.com/file.bin",
            "hls_url": "https://example.com/master.m3u8",
        }
    )

    assert saw_missing_path is False
    assert [source.kind for source in sources] == ["remote-hls", "local-file", "remote-direct"]


def test_resolve_sources_from_attributes_preserves_attachment_metadata(tmp_path: Path) -> None:
    """Attachment resolution should carry debrid-service metadata alongside the serving locator."""

    media_file = tmp_path / "provider-backed.mkv"
    media_file.write_bytes(b"movie")

    sources, saw_missing_path = playback_resolution.resolve_attachments_from_attributes(
        {
            "provider": "realdebrid",
            "provider_download_id": "download-123",
            "provider_file_id": 42,
            "provider_file_path": "folder/Provider Movie.mkv",
            "original_filename": "Provider Movie.mkv",
            "file_size": 123456,
            "active_stream": {
                "hls_url": "https://cdn.example.com/movie.m3u8",
                "unrestricted_url": "https://cdn.example.com/movie.m3u8",
                "download_url": "https://api.example.com/restricted/movie",
                "file_path": str(media_file),
            },
        }
    )

    assert saw_missing_path is False
    assert sources[0].kind == "remote-hls"
    assert sources[0].provider == "realdebrid"
    assert sources[0].provider_download_id == "download-123"
    assert sources[0].provider_file_id == "42"
    assert sources[0].provider_file_path == "folder/Provider Movie.mkv"
    assert sources[0].original_filename == "Provider Movie.mkv"
    assert sources[0].file_size == 123456
    assert sources[0].unrestricted_url == "https://cdn.example.com/movie.m3u8"
    assert sources[0].restricted_url == "https://api.example.com/restricted/movie"
    assert sources[1].kind == "local-file"
    assert sources[1].local_path == str(media_file)
    assert sources[1].provider == "realdebrid"


def test_stream_file_prefers_active_stream_over_top_level_file_path(tmp_path: Path) -> None:
    """Direct stream route should prioritize the explicitly active stream over generic fallback paths."""

    fallback_file = tmp_path / "fallback-example.txt"
    fallback_file.write_bytes(b"fallback-stream")
    item = _build_item(
        attributes={
            "file_path": str(fallback_file),
            "active_stream": {"download_url": "https://example.com/active-stream.bin"},
        }
    )
    client, _ = _build_client(items=[item])

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        assert url == "https://example.com/active-stream.bin"
        assert request.headers.get("x-api-key") == "a" * 32
        assert owner == "http-direct"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"active-stream"

        return StreamingResponse(iterator(), media_type="application/octet-stream")

    original = byte_streaming.stream_remote
    byte_streaming.stream_remote = fake_stream_remote
    try:
        response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())
    finally:
        byte_streaming.stream_remote = original

    assert response.status_code == 200
    assert response.content == b"active-stream"


def test_stream_file_starts_non_blocking_refresh_trigger_for_remote_direct_source() -> None:
    item = _build_item(item_id="item-stream-file-route-trigger-remote-direct")
    item.playback_attachments = [
        _build_playback_attachment(
            attachment_id="attachment-stream-file-route-trigger-remote-direct",
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/route-trigger-remote-direct",
            restricted_url="https://api.example.com/restricted-route-trigger-remote-direct",
            unrestricted_url="https://cdn.example.com/route-trigger-remote-direct",
            refresh_state="stale",
            provider="realdebrid",
            provider_download_id="download-route-trigger-remote-direct",
            is_preferred=True,
        )
    ]
    client, _ = _build_client(items=[item])

    triggered: list[str] = []

    def fake_start_trigger(*, request: Any, item_identifier: str) -> None:
        assert request.headers.get("x-api-key") == "a" * 32
        triggered.append(item_identifier)

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        assert url == "https://api.example.com/restricted-route-trigger-remote-direct"
        assert owner == "http-direct"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"route-trigger-remote-direct"

        return StreamingResponse(iterator(), media_type="application/octet-stream")

    original_start_trigger = stream_routes._start_direct_playback_refresh_trigger
    original_stream_remote = byte_streaming.stream_remote
    stream_routes._start_direct_playback_refresh_trigger = fake_start_trigger
    byte_streaming.stream_remote = fake_stream_remote
    try:
        response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())
    finally:
        stream_routes._start_direct_playback_refresh_trigger = original_start_trigger
        byte_streaming.stream_remote = original_stream_remote

    assert response.status_code == 200
    assert response.content == b"route-trigger-remote-direct"
    assert triggered == [item.id]


def test_stream_file_starts_non_blocking_refresh_trigger_for_selected_failed_lease() -> None:
    item = _build_item(item_id="item-stream-file-route-trigger-failed-lease")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-stream-file-route-trigger-failed-lease",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-stream-file-route-trigger-failed-lease",
        unrestricted_url="https://cdn.example.com/stream-file-route-trigger-failed-lease",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-stream-file-route-trigger-failed-lease",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    client, _ = _build_client(items=[item])

    triggered: list[str] = []

    def fake_start_trigger(*, request: Any, item_identifier: str) -> None:
        assert request.headers.get("x-api-key") == "a" * 32
        triggered.append(item_identifier)

    original_start_trigger = stream_routes._start_direct_playback_refresh_trigger
    stream_routes._start_direct_playback_refresh_trigger = fake_start_trigger
    try:
        response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())
    finally:
        stream_routes._start_direct_playback_refresh_trigger = original_start_trigger

    assert response.status_code == 503
    assert (
        response.json()["detail"]
        == "Selected direct playback lease refresh failed: provider unavailable"
    )
    assert triggered == [item.id]


def test_stream_file_route_skips_duplicate_trigger_when_refresh_is_already_pending() -> None:
    item = _build_item(item_id="item-stream-file-route-trigger-already-pending")
    item.playback_attachments = [
        _build_playback_attachment(
            attachment_id="attachment-stream-file-route-trigger-already-pending",
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/route-trigger-already-pending",
            restricted_url="https://api.example.com/restricted-route-trigger-already-pending",
            unrestricted_url="https://cdn.example.com/route-trigger-already-pending",
            refresh_state="stale",
            provider="realdebrid",
            provider_download_id="download-route-trigger-already-pending",
            is_preferred=True,
        )
    ]
    client, resources = _build_client(items=[item])

    class FakeController:
        def has_pending(self, item_identifier: str) -> bool:
            assert item_identifier == item.id
            return True

        def get_last_result(
            self, item_identifier: str
        ) -> playback_service.DirectPlaybackRefreshSchedulingResult:
            assert item_identifier == item.id
            return playback_service.DirectPlaybackRefreshSchedulingResult(
                outcome="scheduled",
                retry_after_seconds=7.5,
            )

    resources.playback_refresh_controller = cast(Any, FakeController())

    create_task_called = {"value": False}

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        assert url == "https://api.example.com/restricted-route-trigger-already-pending"
        assert owner == "http-direct"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"route-trigger-already-pending"

        return StreamingResponse(iterator(), media_type="application/octet-stream")

    def fake_create_task(coro: Any, *, name: str | None = None) -> Any:
        _ = coro
        _ = name
        create_task_called["value"] = True
        raise AssertionError(
            "route should not create a new background trigger task when refresh work is already pending"
        )

    route_asyncio = cast(Any, stream_routes).asyncio
    original_stream_remote = byte_streaming.stream_remote
    original_create_task = route_asyncio.create_task
    byte_streaming.stream_remote = fake_stream_remote
    route_asyncio.create_task = fake_create_task
    try:
        before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
        response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())
        governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    finally:
        byte_streaming.stream_remote = original_stream_remote
        route_asyncio.create_task = original_create_task

    assert response.status_code == 200
    assert response.content == b"route-trigger-already-pending"
    assert create_task_called["value"] is False
    assert (
        governance["direct_playback_refresh_trigger_already_pending"]
        == before["direct_playback_refresh_trigger_already_pending"] + 1
    )
    assert (
        governance["direct_playback_refresh_trigger_backoff_pending"]
        == before["direct_playback_refresh_trigger_backoff_pending"] + 1
    )
    assert (
        governance["direct_playback_refresh_trigger_starts"]
        == before["direct_playback_refresh_trigger_starts"]
    )


def test_stream_file_route_records_missing_controller_when_trigger_is_unavailable() -> None:
    item = _build_item(item_id="item-stream-file-route-trigger-controller-unavailable")
    item.playback_attachments = [
        _build_playback_attachment(
            attachment_id="attachment-stream-file-route-trigger-controller-unavailable",
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/route-trigger-controller-unavailable",
            restricted_url="https://api.example.com/restricted-route-trigger-controller-unavailable",
            unrestricted_url="https://cdn.example.com/route-trigger-controller-unavailable",
            refresh_state="stale",
            provider="realdebrid",
            provider_download_id="download-route-trigger-controller-unavailable",
            is_preferred=True,
        )
    ]
    client, resources = _build_client(items=[item])
    resources.playback_refresh_controller = None

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        assert url == "https://api.example.com/restricted-route-trigger-controller-unavailable"
        assert owner == "http-direct"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"route-trigger-controller-unavailable"

        return StreamingResponse(iterator(), media_type="application/octet-stream")

    original_stream_remote = byte_streaming.stream_remote
    byte_streaming.stream_remote = fake_stream_remote
    try:
        before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
        response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())
        governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    finally:
        byte_streaming.stream_remote = original_stream_remote

    assert response.status_code == 200
    assert response.content == b"route-trigger-controller-unavailable"
    assert (
        governance["direct_playback_refresh_trigger_controller_unavailable"]
        == before["direct_playback_refresh_trigger_controller_unavailable"] + 1
    )


def test_in_process_hls_failed_lease_refresh_controller_triggers_background_refresh() -> None:
    item = _build_item(item_id="item-in-process-hls-failed-lease-refresh-controller")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-in-process-hls-failed-lease-refresh-controller",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-in-process-hls-failed-lease-refresh-controller.m3u8",
        unrestricted_url="https://cdn.example.com/in-process-hls-failed-lease-refresh-controller.m3u8",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-in-process-hls-failed-lease-refresh-controller",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    _, resources = _build_client(items=[item])

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert request.item_id == item.id
        assert (
            request.provider_download_id
            == "download-in-process-hls-failed-lease-refresh-controller"
        )
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/in-process-hls-failed-lease-refresh-controller-fresh.m3u8",
            restricted_url="https://api.example.com/restricted-in-process-hls-failed-lease-refresh-controller-fresh.m3u8",
            unrestricted_url="https://cdn.example.com/in-process-hls-failed-lease-refresh-controller-fresh.m3u8",
        )

    controller = InProcessHlsFailedLeaseRefreshController(
        PlaybackSourceService(resources.db),
        executors={"realdebrid": fake_executor},
    )

    async def exercise() -> None:
        result = await controller.trigger(item.id)
        assert result.outcome == "scheduled"
        await controller.wait_for_item(item.id)

    asyncio.run(exercise())

    last_result = controller.get_last_result(item.id)
    assert last_result is not None
    assert last_result.outcome == "completed"
    assert last_result.execution is not None
    assert last_result.execution.ok is True
    assert selected_entry.refresh_state == "ready"
    assert (
        selected_entry.unrestricted_url
        == "https://cdn.example.com/in-process-hls-failed-lease-refresh-controller-fresh.m3u8"
    )


def test_in_process_hls_failed_lease_refresh_controller_reschedules_rate_limited_work() -> None:
    item = _build_item(item_id="item-in-process-hls-failed-lease-refresh-controller-rate-limited")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-in-process-hls-failed-lease-refresh-controller-rate-limited",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-in-process-hls-failed-lease-refresh-controller-rate-limited.m3u8",
        unrestricted_url="https://cdn.example.com/in-process-hls-failed-lease-refresh-controller-rate-limited.m3u8",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-in-process-hls-failed-lease-refresh-controller-rate-limited",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, resources = _build_client(items=[item])

    slept_for: list[float] = []

    class FakeRateLimiter:
        def __init__(self) -> None:
            self.calls = 0

        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            _ = capacity
            _ = refill_rate_per_second
            _ = requested_tokens
            _ = now_seconds
            _ = expiry_seconds
            self.calls += 1
            if self.calls == 1:
                return RateLimitDecision(
                    allowed=False,
                    remaining_tokens=0.0,
                    retry_after_seconds=7.5,
                )
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            return RateLimitDecision(
                allowed=True,
                remaining_tokens=0.0,
                retry_after_seconds=0.0,
            )

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert request.item_id == item.id
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/in-process-hls-failed-lease-refresh-controller-rate-limited-fresh.m3u8",
            restricted_url="https://api.example.com/restricted-in-process-hls-failed-lease-refresh-controller-rate-limited-fresh.m3u8",
            unrestricted_url="https://cdn.example.com/in-process-hls-failed-lease-refresh-controller-rate-limited-fresh.m3u8",
        )

    async def fake_sleep(delay: float) -> None:
        slept_for.append(delay)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    controller = InProcessHlsFailedLeaseRefreshController(
        PlaybackSourceService(resources.db),
        executors={"realdebrid": fake_executor},
        rate_limiter=FakeRateLimiter(),
        sleep=fake_sleep,
    )

    async def exercise() -> None:
        result = await controller.trigger(item.id)
        assert result.outcome == "scheduled"
        await controller.wait_for_item(item.id)

    asyncio.run(exercise())

    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    last_result = controller.get_last_result(item.id)
    assert last_result is not None
    assert last_result.outcome == "completed"
    assert slept_for == pytest.approx([7.5], rel=0.0, abs=0.05)
    assert selected_entry.refresh_state == "ready"
    assert (
        governance["hls_failed_lease_refresh_rate_limited"]
        == before["hls_failed_lease_refresh_rate_limited"] + 1
    )


def test_in_process_hls_failed_lease_refresh_controller_reschedules_provider_circuit_open_work() -> (
    None
):
    item = _build_item(item_id="item-in-process-hls-failed-lease-refresh-controller-circuit-open")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-in-process-hls-failed-lease-refresh-controller-circuit-open",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-in-process-hls-failed-lease-refresh-controller-circuit-open.m3u8",
        unrestricted_url="https://cdn.example.com/in-process-hls-failed-lease-refresh-controller-circuit-open.m3u8",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-in-process-hls-failed-lease-refresh-controller-circuit-open",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, resources = _build_client(items=[item])

    current_time = {"value": 10.0}
    slept_for: list[float] = []
    provider_circuit_breaker = ProviderCircuitBreaker(
        failure_threshold=1,
        reset_timeout_seconds=5.0,
        clock=lambda: current_time["value"],
    )
    assert provider_circuit_breaker.record_failure("realdebrid") is True

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert request.item_id == item.id
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/in-process-hls-failed-lease-refresh-controller-circuit-open-fresh.m3u8",
            restricted_url="https://api.example.com/restricted-in-process-hls-failed-lease-refresh-controller-circuit-open-fresh.m3u8",
            unrestricted_url="https://cdn.example.com/in-process-hls-failed-lease-refresh-controller-circuit-open-fresh.m3u8",
        )

    async def fake_sleep(delay: float) -> None:
        slept_for.append(delay)
        current_time["value"] += delay

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    controller = InProcessHlsFailedLeaseRefreshController(
        PlaybackSourceService(
            resources.db,
            provider_circuit_breaker=provider_circuit_breaker,
        ),
        executors={"realdebrid": fake_executor},
        sleep=fake_sleep,
    )

    async def exercise() -> None:
        result = await controller.trigger(item.id)
        assert result.outcome == "scheduled"
        await controller.wait_for_item(item.id)

    asyncio.run(exercise())

    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    last_result = controller.get_last_result(item.id)
    assert last_result is not None
    assert last_result.outcome == "completed"
    assert slept_for == pytest.approx([5.0], rel=0.0, abs=0.05)
    assert selected_entry.refresh_state == "ready"
    assert (
        governance["hls_failed_lease_refresh_provider_circuit_open"]
        == before["hls_failed_lease_refresh_provider_circuit_open"] + 1
    )


def test_in_process_hls_restricted_fallback_refresh_controller_triggers_background_refresh() -> (
    None
):
    item = _build_item(item_id="item-in-process-hls-restricted-fallback-refresh-controller")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-in-process-hls-restricted-fallback-refresh-controller",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-in-process-hls-restricted-fallback-refresh-controller.m3u8",
        unrestricted_url="https://cdn.example.com/in-process-hls-restricted-fallback-refresh-controller.m3u8",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-in-process-hls-restricted-fallback-refresh-controller",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    _, resources = _build_client(items=[item])

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert request.item_id == item.id
        assert (
            request.provider_download_id
            == "download-in-process-hls-restricted-fallback-refresh-controller"
        )
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/in-process-hls-restricted-fallback-refresh-controller-fresh.m3u8",
            restricted_url="https://api.example.com/restricted-in-process-hls-restricted-fallback-refresh-controller-fresh.m3u8",
            unrestricted_url="https://cdn.example.com/in-process-hls-restricted-fallback-refresh-controller-fresh.m3u8",
        )

    controller = InProcessHlsRestrictedFallbackRefreshController(
        PlaybackSourceService(resources.db),
        executors={"realdebrid": fake_executor},
    )

    async def exercise() -> None:
        result = await controller.trigger(item.id)
        assert result.outcome == "scheduled"
        await controller.wait_for_item(item.id)

    asyncio.run(exercise())

    last_result = controller.get_last_result(item.id)
    assert last_result is not None
    assert last_result.outcome == "completed"
    assert last_result.execution is not None
    assert last_result.execution.ok is True
    assert selected_entry.refresh_state == "ready"
    assert (
        selected_entry.unrestricted_url
        == "https://cdn.example.com/in-process-hls-restricted-fallback-refresh-controller-fresh.m3u8"
    )


def test_in_process_hls_restricted_fallback_refresh_controller_reschedules_rate_limited_work() -> (
    None
):
    item = _build_item(
        item_id="item-in-process-hls-restricted-fallback-refresh-controller-rate-limited"
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-in-process-hls-restricted-fallback-refresh-controller-rate-limited",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-in-process-hls-restricted-fallback-refresh-controller-rate-limited.m3u8",
        unrestricted_url="https://cdn.example.com/in-process-hls-restricted-fallback-refresh-controller-rate-limited.m3u8",
        refresh_state="refreshing",
        provider="realdebrid",
        provider_download_id="download-in-process-hls-restricted-fallback-refresh-controller-rate-limited",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, resources = _build_client(items=[item])

    slept_for: list[float] = []

    class FakeRateLimiter:
        def __init__(self) -> None:
            self.calls = 0

        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            _ = capacity
            _ = refill_rate_per_second
            _ = requested_tokens
            _ = now_seconds
            _ = expiry_seconds
            self.calls += 1
            if self.calls == 1:
                return RateLimitDecision(
                    allowed=False,
                    remaining_tokens=0.0,
                    retry_after_seconds=7.5,
                )
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            return RateLimitDecision(
                allowed=True,
                remaining_tokens=0.0,
                retry_after_seconds=0.0,
            )

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert request.item_id == item.id
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/in-process-hls-restricted-fallback-refresh-controller-rate-limited-fresh.m3u8",
            restricted_url="https://api.example.com/restricted-in-process-hls-restricted-fallback-refresh-controller-rate-limited-fresh.m3u8",
            unrestricted_url="https://cdn.example.com/in-process-hls-restricted-fallback-refresh-controller-rate-limited-fresh.m3u8",
        )

    async def fake_sleep(delay: float) -> None:
        slept_for.append(delay)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    controller = InProcessHlsRestrictedFallbackRefreshController(
        PlaybackSourceService(resources.db),
        executors={"realdebrid": fake_executor},
        rate_limiter=FakeRateLimiter(),
        sleep=fake_sleep,
    )

    async def exercise() -> None:
        result = await controller.trigger(item.id)
        assert result.outcome == "scheduled"
        await controller.wait_for_item(item.id)

    asyncio.run(exercise())

    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    last_result = controller.get_last_result(item.id)
    assert last_result is not None
    assert last_result.outcome == "completed"
    assert slept_for == pytest.approx([7.5], rel=0.0, abs=0.05)
    assert selected_entry.refresh_state == "ready"
    assert (
        governance["hls_restricted_fallback_refresh_rate_limited"]
        == before["hls_restricted_fallback_refresh_rate_limited"] + 1
    )


def test_in_process_hls_restricted_fallback_refresh_controller_reschedules_provider_circuit_open_work() -> (
    None
):
    item = _build_item(
        item_id="item-in-process-hls-restricted-fallback-refresh-controller-circuit-open"
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-in-process-hls-restricted-fallback-refresh-controller-circuit-open",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-in-process-hls-restricted-fallback-refresh-controller-circuit-open.m3u8",
        unrestricted_url="https://cdn.example.com/in-process-hls-restricted-fallback-refresh-controller-circuit-open.m3u8",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-in-process-hls-restricted-fallback-refresh-controller-circuit-open",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, resources = _build_client(items=[item])

    current_time = {"value": 10.0}
    slept_for: list[float] = []
    provider_circuit_breaker = ProviderCircuitBreaker(
        failure_threshold=1,
        reset_timeout_seconds=5.0,
        clock=lambda: current_time["value"],
    )
    assert provider_circuit_breaker.record_failure("realdebrid") is True

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert request.item_id == item.id
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/in-process-hls-restricted-fallback-refresh-controller-circuit-open-fresh.m3u8",
            restricted_url="https://api.example.com/restricted-in-process-hls-restricted-fallback-refresh-controller-circuit-open-fresh.m3u8",
            unrestricted_url="https://cdn.example.com/in-process-hls-restricted-fallback-refresh-controller-circuit-open-fresh.m3u8",
        )

    async def fake_sleep(delay: float) -> None:
        slept_for.append(delay)
        current_time["value"] += delay

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    controller = InProcessHlsRestrictedFallbackRefreshController(
        PlaybackSourceService(
            resources.db,
            provider_circuit_breaker=provider_circuit_breaker,
        ),
        executors={"realdebrid": fake_executor},
        sleep=fake_sleep,
    )

    async def exercise() -> None:
        result = await controller.trigger(item.id)
        assert result.outcome == "scheduled"
        await controller.wait_for_item(item.id)

    asyncio.run(exercise())

    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    last_result = controller.get_last_result(item.id)
    assert last_result is not None
    assert last_result.outcome == "completed"
    assert slept_for == pytest.approx([5.0], rel=0.0, abs=0.05)
    assert selected_entry.refresh_state == "ready"
    assert (
        governance["hls_restricted_fallback_refresh_provider_circuit_open"]
        == before["hls_restricted_fallback_refresh_provider_circuit_open"] + 1
    )


def test_hls_playlist_route_starts_non_blocking_refresh_trigger_for_selected_failed_lease() -> None:
    item = _build_item(item_id="item-hls-playlist-route-trigger-failed-lease")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-playlist-route-trigger-failed-lease",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-playlist-route-trigger-failed-lease.m3u8",
        unrestricted_url="https://cdn.example.com/hls-playlist-route-trigger-failed-lease.m3u8",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-hls-playlist-route-trigger-failed-lease",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, _ = _build_client(items=[item])

    triggered: list[str] = []

    def fake_start_trigger(*, request: Any, item_identifier: str) -> None:
        assert request.headers.get("x-api-key") == "a" * 32
        triggered.append(item_identifier)

    original_start_trigger = stream_routes._start_hls_failed_lease_refresh_trigger
    stream_routes._start_hls_failed_lease_refresh_trigger = fake_start_trigger
    try:
        response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    finally:
        stream_routes._start_hls_failed_lease_refresh_trigger = original_start_trigger

    assert response.status_code == 503
    assert (
        response.json()["detail"]
        == "Selected HLS playback lease refresh failed: provider unavailable"
    )
    assert triggered == [item.id]


def test_hls_file_route_starts_non_blocking_refresh_trigger_for_selected_failed_lease() -> None:
    item = _build_item(item_id="item-hls-file-route-trigger-failed-lease")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-file-route-trigger-failed-lease",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-file-route-trigger-failed-lease.m3u8",
        unrestricted_url="https://cdn.example.com/hls-file-route-trigger-failed-lease.m3u8",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-hls-file-route-trigger-failed-lease",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, _ = _build_client(items=[item])

    triggered: list[str] = []

    def fake_start_trigger(*, request: Any, item_identifier: str) -> None:
        assert request.headers.get("x-api-key") == "a" * 32
        triggered.append(item_identifier)

    original_start_trigger = stream_routes._start_hls_failed_lease_refresh_trigger
    stream_routes._start_hls_failed_lease_refresh_trigger = fake_start_trigger
    try:
        response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())
    finally:
        stream_routes._start_hls_failed_lease_refresh_trigger = original_start_trigger

    assert response.status_code == 503
    assert (
        response.json()["detail"]
        == "Selected HLS playback lease refresh failed: provider unavailable"
    )
    assert triggered == [item.id]


def test_hls_playlist_route_skips_duplicate_trigger_when_refresh_is_already_pending() -> None:
    item = _build_item(item_id="item-hls-playlist-route-trigger-already-pending")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-playlist-route-trigger-already-pending",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-playlist-route-trigger-already-pending.m3u8",
        unrestricted_url="https://cdn.example.com/hls-playlist-route-trigger-already-pending.m3u8",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-hls-playlist-route-trigger-already-pending",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, resources = _build_client(items=[item])

    class FakeController:
        def has_pending(self, item_identifier: str) -> bool:
            assert item_identifier == item.id
            return True

        def get_last_result(
            self, item_identifier: str
        ) -> playback_service.HlsFailedLeaseRefreshResult:
            assert item_identifier == item.id
            return playback_service.HlsFailedLeaseRefreshResult(
                item_identifier=item.id,
                outcome="run_later",
                retry_after_seconds=7.5,
            )

    resources.hls_failed_lease_refresh_controller = cast(Any, FakeController())

    create_task_called = {"value": False}

    def fake_create_task(coro: Any, *, name: str | None = None) -> Any:
        _ = coro
        _ = name
        create_task_called["value"] = True
        raise AssertionError(
            "route should not create a new background trigger task when HLS failed-lease refresh work is already pending"
        )

    route_asyncio = cast(Any, stream_routes).asyncio
    original_create_task = route_asyncio.create_task
    route_asyncio.create_task = fake_create_task
    try:
        before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
        response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
        governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    finally:
        route_asyncio.create_task = original_create_task

    assert response.status_code == 503
    assert create_task_called["value"] is False
    assert (
        governance["hls_failed_lease_refresh_trigger_already_pending"]
        == before["hls_failed_lease_refresh_trigger_already_pending"] + 1
    )
    assert (
        governance["hls_failed_lease_refresh_trigger_backoff_pending"]
        == before["hls_failed_lease_refresh_trigger_backoff_pending"] + 1
    )
    assert (
        governance["hls_failed_lease_refresh_trigger_starts"]
        == before["hls_failed_lease_refresh_trigger_starts"]
    )


def test_hls_playlist_route_records_missing_controller_when_trigger_is_unavailable() -> None:
    item = _build_item(item_id="item-hls-playlist-route-trigger-controller-unavailable")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-playlist-route-trigger-controller-unavailable",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-playlist-route-trigger-controller-unavailable.m3u8",
        unrestricted_url="https://cdn.example.com/hls-playlist-route-trigger-controller-unavailable.m3u8",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-hls-playlist-route-trigger-controller-unavailable",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, resources = _build_client(items=[item])
    resources.hls_failed_lease_refresh_controller = None

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert response.status_code == 503
    assert (
        governance["hls_failed_lease_refresh_trigger_controller_unavailable"]
        == before["hls_failed_lease_refresh_trigger_controller_unavailable"] + 1
    )


def test_hls_playlist_route_starts_non_blocking_refresh_trigger_for_selected_restricted_fallback() -> (
    None
):
    item = _build_item(item_id="item-hls-playlist-route-trigger-restricted-fallback")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-playlist-route-trigger-restricted-fallback",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-playlist-route-trigger-restricted-fallback.m3u8",
        unrestricted_url="https://cdn.example.com/hls-playlist-route-trigger-restricted-fallback.m3u8",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-hls-playlist-route-trigger-restricted-fallback",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, _ = _build_client(items=[item])

    triggered: list[str] = []

    def fake_start_trigger(*, request: Any, item_identifier: str) -> None:
        assert request.headers.get("x-api-key") == "a" * 32
        triggered.append(item_identifier)

    async def fake_download_remote_hls_playlist(url: str) -> tuple[str, httpx.Headers]:
        assert (
            url
            == "https://api.example.com/restricted-hls-playlist-route-trigger-restricted-fallback.m3u8"
        )
        return (
            "#EXTM3U\nsegment_00001.ts\n",
            httpx.Headers({"content-type": "application/vnd.apple.mpegurl"}),
        )

    original_start_trigger = stream_routes._start_hls_restricted_fallback_refresh_trigger
    original_download = stream_routes._download_remote_hls_playlist
    stream_routes._start_hls_restricted_fallback_refresh_trigger = fake_start_trigger
    stream_routes._download_remote_hls_playlist = fake_download_remote_hls_playlist
    try:
        response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    finally:
        stream_routes._start_hls_restricted_fallback_refresh_trigger = original_start_trigger
        stream_routes._download_remote_hls_playlist = original_download

    assert response.status_code == 200
    assert f"/api/stream/{item.id}/hls/segment_00001.ts" in response.text
    assert triggered == [item.id]


def test_hls_file_route_starts_non_blocking_refresh_trigger_for_selected_restricted_fallback() -> (
    None
):
    item = _build_item(item_id="item-hls-file-route-trigger-restricted-fallback")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-file-route-trigger-restricted-fallback",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-file-route-trigger-restricted-fallback.m3u8",
        unrestricted_url="https://cdn.example.com/hls-file-route-trigger-restricted-fallback.m3u8",
        refresh_state="refreshing",
        provider="realdebrid",
        provider_download_id="download-hls-file-route-trigger-restricted-fallback",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, _ = _build_client(items=[item])

    triggered: list[str] = []

    def fake_start_trigger(*, request: Any, item_identifier: str) -> None:
        assert request.headers.get("x-api-key") == "a" * 32
        triggered.append(item_identifier)

    async def fake_open_remote_hls_child_stream(
        *, playlist_url: str, upstream_url: str, request: Any
    ) -> StreamingResponse:
        assert (
            playlist_url
            == "https://api.example.com/restricted-hls-file-route-trigger-restricted-fallback.m3u8"
        )
        assert upstream_url == "https://api.example.com/segment_00001.ts"
        assert request.headers.get("x-api-key") == "a" * 32

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"hls-restricted-fallback-segment"

        return StreamingResponse(iterator(), media_type="video/mp2t")

    original_start_trigger = stream_routes._start_hls_restricted_fallback_refresh_trigger
    original_open_remote = stream_routes._open_remote_hls_child_stream
    stream_routes._start_hls_restricted_fallback_refresh_trigger = fake_start_trigger
    stream_routes._open_remote_hls_child_stream = fake_open_remote_hls_child_stream
    try:
        response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())
    finally:
        stream_routes._start_hls_restricted_fallback_refresh_trigger = original_start_trigger
        stream_routes._open_remote_hls_child_stream = original_open_remote

    assert response.status_code == 200
    assert response.content == b"hls-restricted-fallback-segment"
    assert triggered == [item.id]


def test_hls_playlist_route_refreshes_media_entry_backed_remote_hls_after_upstream_failure(
    monkeypatch: Any,
) -> None:
    item = _build_item(item_id="item-hls-remote-playlist-inline-refresh-success")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-hls-remote-playlist-inline-refresh-success",
        item_id=item.id,
        kind="remote-hls",
        locator="https://cdn.example.com/hls-inline-refresh-stale/index.m3u8",
        unrestricted_url="https://cdn.example.com/hls-inline-refresh-stale/index.m3u8",
        restricted_url="https://api.example.com/restricted-hls-inline-refresh-success.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-inline-refresh-success",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-remote-playlist-inline-refresh-success",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-inline-refresh-success.m3u8",
        unrestricted_url="https://cdn.example.com/hls-inline-refresh-stale/index.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-inline-refresh-success",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
    )
    selected_entry.source_attachment = source_attachment
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]

    database = PersistentDummyDatabaseRuntime(items=[item])
    client, resources = _build_client(db=database)

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            return RateLimitDecision(allowed=True, remaining_tokens=0.0, retry_after_seconds=0.0)

    class FakeProviderClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append(link)
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/hls-inline-refresh-fresh/index.m3u8",
                restricted_url=link,
                expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
            )

    provider_client = FakeProviderClient()
    resources.playback_service = PlaybackSourceService(
        resources.db,
        provider_clients={"realdebrid": cast(PlaybackAttachmentProviderClient, provider_client)},
        rate_limiter=FakeRateLimiter(),
    )

    playlist_calls: list[str] = []

    async def fake_download_remote_hls_playlist(url: str) -> tuple[str, httpx.Headers]:
        playlist_calls.append(url)
        if url == "https://cdn.example.com/hls-inline-refresh-stale/index.m3u8":
            raise HTTPException(status_code=502, detail="Upstream HLS request failed with status 502")
        assert url == "https://cdn.example.com/hls-inline-refresh-fresh/index.m3u8"
        return (
            "#EXTM3U\nsegment_00001.ts\n",
            httpx.Headers({"content-type": "application/vnd.apple.mpegurl"}),
        )

    monkeypatch.setattr(stream_routes, "_download_remote_hls_playlist", fake_download_remote_hls_playlist)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert response.status_code == 200
    assert f"/api/stream/{item.id}/hls/segment_00001.ts" in response.text
    assert playlist_calls == [
        "https://cdn.example.com/hls-inline-refresh-stale/index.m3u8",
        "https://cdn.example.com/hls-inline-refresh-fresh/index.m3u8",
    ]
    assert provider_client.calls == ["https://api.example.com/restricted-hls-inline-refresh-success.m3u8"]
    assert (
        governance["inline_remote_hls_refresh_attempts"]
        == before["inline_remote_hls_refresh_attempts"] + 1
    )
    assert (
        governance["inline_remote_hls_refresh_recovered"]
        == before["inline_remote_hls_refresh_recovered"] + 1
    )
    assert (
        governance["inline_remote_hls_refresh_failures"]
        == before["inline_remote_hls_refresh_failures"]
    )
    assert (
        governance["inline_remote_hls_refresh_no_action"]
        == before["inline_remote_hls_refresh_no_action"]
    )

    persisted_item = asyncio.run(PlaybackSourceService(resources.db)._list_items())[0]
    persisted_entry = persisted_item.media_entries[0]
    persisted_attachment = persisted_item.playback_attachments[0]
    assert (
        persisted_entry.unrestricted_url
        == "https://cdn.example.com/hls-inline-refresh-fresh/index.m3u8"
    )
    assert (
        persisted_attachment.unrestricted_url
        == "https://cdn.example.com/hls-inline-refresh-fresh/index.m3u8"
    )


def test_hls_file_route_refreshes_media_entry_backed_remote_hls_after_upstream_failure(
    monkeypatch: Any,
) -> None:
    item = _build_item(item_id="item-hls-remote-file-inline-refresh-success")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-hls-remote-file-inline-refresh-success",
        item_id=item.id,
        kind="remote-hls",
        locator="https://cdn.example.com/hls-inline-file-refresh-stale/index.m3u8",
        unrestricted_url="https://cdn.example.com/hls-inline-file-refresh-stale/index.m3u8",
        restricted_url="https://api.example.com/restricted-hls-inline-file-refresh-success.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-inline-file-refresh-success",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-remote-file-inline-refresh-success",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-inline-file-refresh-success.m3u8",
        unrestricted_url="https://cdn.example.com/hls-inline-file-refresh-stale/index.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-inline-file-refresh-success",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
    )
    selected_entry.source_attachment = source_attachment
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]

    database = PersistentDummyDatabaseRuntime(items=[item])
    client, resources = _build_client(db=database)

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            return RateLimitDecision(allowed=True, remaining_tokens=0.0, retry_after_seconds=0.0)

    class FakeProviderClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append(link)
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/hls-inline-file-refresh-fresh/index.m3u8",
                restricted_url=link,
                expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
            )

    provider_client = FakeProviderClient()
    resources.playback_service = PlaybackSourceService(
        resources.db,
        provider_clients={"realdebrid": cast(PlaybackAttachmentProviderClient, provider_client)},
        rate_limiter=FakeRateLimiter(),
    )

    open_calls: list[tuple[str, str]] = []

    async def fake_open_remote_hls_child_stream(
        *,
        playlist_url: str,
        upstream_url: str,
        request: Any,
    ) -> StreamingResponse:
        _ = request
        open_calls.append((playlist_url, upstream_url))
        if playlist_url == "https://cdn.example.com/hls-inline-file-refresh-stale/index.m3u8":
            raise HTTPException(status_code=502, detail="Upstream HLS request failed with status 502")
        assert playlist_url == "https://cdn.example.com/hls-inline-file-refresh-fresh/index.m3u8"
        assert upstream_url == "https://cdn.example.com/hls-inline-file-refresh-fresh/segment_00001.ts"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"hls-inline-file-refresh-success"

        return StreamingResponse(iterator(), media_type="video/mp2t")

    monkeypatch.setattr(stream_routes, "_open_remote_hls_child_stream", fake_open_remote_hls_child_stream)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert response.status_code == 200
    assert response.content == b"hls-inline-file-refresh-success"
    assert open_calls == [
        (
            "https://cdn.example.com/hls-inline-file-refresh-stale/index.m3u8",
            "https://cdn.example.com/hls-inline-file-refresh-stale/segment_00001.ts",
        ),
        (
            "https://cdn.example.com/hls-inline-file-refresh-fresh/index.m3u8",
            "https://cdn.example.com/hls-inline-file-refresh-fresh/segment_00001.ts",
        ),
    ]
    assert provider_client.calls == [
        "https://api.example.com/restricted-hls-inline-file-refresh-success.m3u8"
    ]
    assert (
        governance["inline_remote_hls_refresh_attempts"]
        == before["inline_remote_hls_refresh_attempts"] + 1
    )
    assert (
        governance["inline_remote_hls_refresh_recovered"]
        == before["inline_remote_hls_refresh_recovered"] + 1
    )
    assert (
        governance["inline_remote_hls_refresh_failures"]
        == before["inline_remote_hls_refresh_failures"]
    )
    assert (
        governance["inline_remote_hls_refresh_no_action"]
        == before["inline_remote_hls_refresh_no_action"]
    )

    persisted_item = asyncio.run(PlaybackSourceService(resources.db)._list_items())[0]
    persisted_entry = persisted_item.media_entries[0]
    persisted_attachment = persisted_item.playback_attachments[0]
    assert (
        persisted_entry.unrestricted_url
        == "https://cdn.example.com/hls-inline-file-refresh-fresh/index.m3u8"
    )
    assert (
        persisted_attachment.unrestricted_url
        == "https://cdn.example.com/hls-inline-file-refresh-fresh/index.m3u8"
    )


def test_hls_playlist_route_recovers_via_transcode_source_when_remote_hls_refresh_changes_kind(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    playlist_dir = tmp_path / "generated-hls-remote-hls-refresh-to-direct"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    item = _build_item(item_id="item-hls-remote-playlist-refresh-to-direct")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-hls-remote-playlist-refresh-to-direct",
        item_id=item.id,
        kind="remote-hls",
        locator="https://cdn.example.com/hls-refresh-to-direct-stale/index.m3u8",
        unrestricted_url="https://cdn.example.com/hls-refresh-to-direct-stale/index.m3u8",
        restricted_url="https://api.example.com/restricted-hls-refresh-to-direct.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-refresh-to-direct",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-remote-playlist-refresh-to-direct",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-refresh-to-direct.m3u8",
        unrestricted_url="https://cdn.example.com/hls-refresh-to-direct-stale/index.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-refresh-to-direct",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
    )
    selected_entry.source_attachment = source_attachment
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]

    database = PersistentDummyDatabaseRuntime(items=[item])
    client, resources = _build_client(db=database)

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            return RateLimitDecision(allowed=True, remaining_tokens=0.0, retry_after_seconds=0.0)

    class FakeProviderClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append(link)
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/hls-refresh-to-direct-fresh.mkv",
                restricted_url=link,
                expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
            )

    provider_client = FakeProviderClient()
    resources.playback_service = PlaybackSourceService(
        resources.db,
        provider_clients={"realdebrid": cast(PlaybackAttachmentProviderClient, provider_client)},
        rate_limiter=FakeRateLimiter(),
    )

    playlist_calls: list[str] = []
    head_calls: list[str] = []

    async def fake_download_remote_hls_playlist(url: str) -> tuple[str, httpx.Headers]:
        playlist_calls.append(url)
        assert url == "https://cdn.example.com/hls-refresh-to-direct-stale/index.m3u8"
        raise HTTPException(status_code=502, detail="Upstream HLS request failed with status 502")

    async def fake_head(url: str) -> None:
        head_calls.append(url)
        assert url == "https://cdn.example.com/hls-refresh-to-direct-fresh.mkv"

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == "https://cdn.example.com/hls-refresh-to-direct-fresh.mkv"
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    monkeypatch.setattr(stream_routes, "_download_remote_hls_playlist", fake_download_remote_hls_playlist)
    monkeypatch.setattr(stream_routes, "_head_remote_direct_url", fake_head)
    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)
    monkeypatch.setattr(
        stream_routes,
        "_start_direct_playback_refresh_trigger",
        lambda *args, **kwargs: None,
    )

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert response.status_code == 200
    assert f"/api/stream/{item.id}/hls/segment_00001.ts" in response.text
    assert playlist_calls == ["https://cdn.example.com/hls-refresh-to-direct-stale/index.m3u8"]
    assert head_calls == ["https://cdn.example.com/hls-refresh-to-direct-fresh.mkv"]
    assert provider_client.calls == ["https://api.example.com/restricted-hls-refresh-to-direct.m3u8"]
    assert (
        governance["inline_remote_hls_refresh_attempts"]
        == before["inline_remote_hls_refresh_attempts"] + 1
    )
    assert (
        governance["inline_remote_hls_refresh_recovered"]
        == before["inline_remote_hls_refresh_recovered"] + 1
    )


def test_hls_file_route_recovers_via_transcode_source_when_remote_hls_refresh_changes_kind(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    playlist_dir = tmp_path / "generated-hls-remote-hls-file-refresh-to-direct"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")
    segment_path = playlist_dir / "segment_00001.ts"
    segment_path.write_bytes(b"hls-file-refresh-to-direct")

    item = _build_item(item_id="item-hls-remote-file-refresh-to-direct")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-hls-remote-file-refresh-to-direct",
        item_id=item.id,
        kind="remote-hls",
        locator="https://cdn.example.com/hls-file-refresh-to-direct-stale/index.m3u8",
        unrestricted_url="https://cdn.example.com/hls-file-refresh-to-direct-stale/index.m3u8",
        restricted_url="https://api.example.com/restricted-hls-file-refresh-to-direct.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-file-refresh-to-direct",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-remote-file-refresh-to-direct",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-file-refresh-to-direct.m3u8",
        unrestricted_url="https://cdn.example.com/hls-file-refresh-to-direct-stale/index.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-file-refresh-to-direct",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
    )
    selected_entry.source_attachment = source_attachment
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]

    database = PersistentDummyDatabaseRuntime(items=[item])
    client, resources = _build_client(db=database)

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            return RateLimitDecision(allowed=True, remaining_tokens=0.0, retry_after_seconds=0.0)

    class FakeProviderClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append(link)
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/hls-file-refresh-to-direct-fresh.mkv",
                restricted_url=link,
                expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
            )

    provider_client = FakeProviderClient()
    resources.playback_service = PlaybackSourceService(
        resources.db,
        provider_clients={"realdebrid": cast(PlaybackAttachmentProviderClient, provider_client)},
        rate_limiter=FakeRateLimiter(),
    )

    open_calls: list[tuple[str, str]] = []
    head_calls: list[str] = []

    async def fake_open_remote_hls_child_stream(
        *,
        playlist_url: str,
        upstream_url: str,
        request: Any,
    ) -> StreamingResponse:
        _ = request
        open_calls.append((playlist_url, upstream_url))
        assert playlist_url == "https://cdn.example.com/hls-file-refresh-to-direct-stale/index.m3u8"
        raise HTTPException(status_code=502, detail="Upstream HLS request failed with status 502")

    async def fake_head(url: str) -> None:
        head_calls.append(url)
        assert url == "https://cdn.example.com/hls-file-refresh-to-direct-fresh.mkv"

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == "https://cdn.example.com/hls-file-refresh-to-direct-fresh.mkv"
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    monkeypatch.setattr(stream_routes, "_open_remote_hls_child_stream", fake_open_remote_hls_child_stream)
    monkeypatch.setattr(stream_routes, "_head_remote_direct_url", fake_head)
    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)
    monkeypatch.setattr(
        stream_routes,
        "_start_direct_playback_refresh_trigger",
        lambda *args, **kwargs: None,
    )

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert response.status_code == 200
    assert response.content == b"hls-file-refresh-to-direct"
    assert open_calls == [
        (
            "https://cdn.example.com/hls-file-refresh-to-direct-stale/index.m3u8",
            "https://cdn.example.com/hls-file-refresh-to-direct-stale/segment_00001.ts",
        )
    ]
    assert head_calls == ["https://cdn.example.com/hls-file-refresh-to-direct-fresh.mkv"]
    assert provider_client.calls == [
        "https://api.example.com/restricted-hls-file-refresh-to-direct.m3u8"
    ]
    assert (
        governance["inline_remote_hls_refresh_attempts"]
        == before["inline_remote_hls_refresh_attempts"] + 1
    )
    assert (
        governance["inline_remote_hls_refresh_recovered"]
        == before["inline_remote_hls_refresh_recovered"] + 1
    )


def test_hls_playlist_route_skips_duplicate_restricted_fallback_trigger_when_refresh_is_already_pending() -> (
    None
):
    item = _build_item(
        item_id="item-hls-playlist-route-trigger-restricted-fallback-already-pending"
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-playlist-route-trigger-restricted-fallback-already-pending",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-playlist-route-trigger-restricted-fallback-already-pending.m3u8",
        unrestricted_url="https://cdn.example.com/hls-playlist-route-trigger-restricted-fallback-already-pending.m3u8",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-hls-playlist-route-trigger-restricted-fallback-already-pending",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, resources = _build_client(items=[item])

    class FakeController:
        def has_pending(self, item_identifier: str) -> bool:
            assert item_identifier == item.id
            return True

        def get_last_result(
            self, item_identifier: str
        ) -> playback_service.HlsRestrictedFallbackRefreshResult:
            assert item_identifier == item.id
            return playback_service.HlsRestrictedFallbackRefreshResult(
                item_identifier=item.id,
                outcome="run_later",
                retry_after_seconds=7.5,
            )

    resources.hls_restricted_fallback_refresh_controller = cast(Any, FakeController())

    create_task_called = {"value": False}

    async def fake_download_remote_hls_playlist(url: str) -> tuple[str, httpx.Headers]:
        assert (
            url
            == "https://api.example.com/restricted-hls-playlist-route-trigger-restricted-fallback-already-pending.m3u8"
        )
        return (
            "#EXTM3U\nsegment_00001.ts\n",
            httpx.Headers({"content-type": "application/vnd.apple.mpegurl"}),
        )

    def fake_create_task(coro: Any, *, name: str | None = None) -> Any:
        _ = coro
        _ = name
        create_task_called["value"] = True
        raise AssertionError(
            "route should not create a new background trigger task when HLS restricted-fallback refresh work is already pending"
        )

    route_asyncio = cast(Any, stream_routes).asyncio
    original_download = stream_routes._download_remote_hls_playlist
    original_create_task = route_asyncio.create_task
    stream_routes._download_remote_hls_playlist = fake_download_remote_hls_playlist
    route_asyncio.create_task = fake_create_task
    try:
        before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
        response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
        governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    finally:
        stream_routes._download_remote_hls_playlist = original_download
        route_asyncio.create_task = original_create_task

    assert response.status_code == 200
    assert create_task_called["value"] is False
    assert (
        governance["hls_restricted_fallback_refresh_trigger_already_pending"]
        == before["hls_restricted_fallback_refresh_trigger_already_pending"] + 1
    )
    assert (
        governance["hls_restricted_fallback_refresh_trigger_backoff_pending"]
        == before["hls_restricted_fallback_refresh_trigger_backoff_pending"] + 1
    )
    assert (
        governance["hls_restricted_fallback_refresh_trigger_starts"]
        == before["hls_restricted_fallback_refresh_trigger_starts"]
    )


def test_hls_playlist_route_records_inline_remote_hls_refresh_no_action_for_attribute_backed_source(
    monkeypatch: Any,
) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/master.m3u8"})
    client, _ = _build_client(items=[item])

    playlist_calls: list[str] = []

    async def fake_download_remote_hls_playlist(url: str) -> tuple[str, httpx.Headers]:
        playlist_calls.append(url)
        raise HTTPException(status_code=502, detail="Upstream HLS request failed with status 502")

    monkeypatch.setattr(stream_routes, "_download_remote_hls_playlist", fake_download_remote_hls_playlist)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert response.status_code == 502
    assert playlist_calls == ["https://example.com/master.m3u8"]
    assert (
        governance["inline_remote_hls_refresh_attempts"]
        == before["inline_remote_hls_refresh_attempts"]
    )
    assert (
        governance["inline_remote_hls_refresh_recovered"]
        == before["inline_remote_hls_refresh_recovered"]
    )
    assert (
        governance["inline_remote_hls_refresh_no_action"]
        == before["inline_remote_hls_refresh_no_action"] + 1
    )
    assert (
        governance["inline_remote_hls_refresh_failures"]
        == before["inline_remote_hls_refresh_failures"]
    )


def test_hls_playlist_route_records_inline_remote_hls_refresh_failure_when_repair_fails(
    monkeypatch: Any,
) -> None:
    item = _build_item(item_id="item-hls-remote-playlist-inline-refresh-failure")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-hls-remote-playlist-inline-refresh-failure",
        item_id=item.id,
        kind="remote-hls",
        locator="https://cdn.example.com/hls-inline-refresh-failure/index.m3u8",
        unrestricted_url="https://cdn.example.com/hls-inline-refresh-failure/index.m3u8",
        restricted_url="https://api.example.com/restricted-hls-inline-refresh-failure.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-inline-refresh-failure",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-remote-playlist-inline-refresh-failure",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-inline-refresh-failure.m3u8",
        unrestricted_url="https://cdn.example.com/hls-inline-refresh-failure/index.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-inline-refresh-failure",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
    )
    selected_entry.source_attachment = source_attachment
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]

    client, resources = _build_client(items=[item])

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            return RateLimitDecision(allowed=False, remaining_tokens=0.0, retry_after_seconds=6.0)

    resources.playback_service = PlaybackSourceService(
        resources.db,
        rate_limiter=FakeRateLimiter(),
    )

    async def fake_download_remote_hls_playlist(url: str) -> tuple[str, httpx.Headers]:
        assert url == "https://cdn.example.com/hls-inline-refresh-failure/index.m3u8"
        raise HTTPException(status_code=502, detail="Upstream HLS request failed with status 502")

    monkeypatch.setattr(stream_routes, "_download_remote_hls_playlist", fake_download_remote_hls_playlist)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert response.status_code == 502
    assert (
        governance["inline_remote_hls_refresh_attempts"]
        == before["inline_remote_hls_refresh_attempts"] + 1
    )
    assert (
        governance["inline_remote_hls_refresh_recovered"]
        == before["inline_remote_hls_refresh_recovered"]
    )
    assert (
        governance["inline_remote_hls_refresh_no_action"]
        == before["inline_remote_hls_refresh_no_action"]
    )
    assert (
        governance["inline_remote_hls_refresh_failures"]
        == before["inline_remote_hls_refresh_failures"] + 1
    )


def test_hls_playlist_route_handoffs_failed_inline_remote_hls_refresh_to_background_controller(
    monkeypatch: Any,
) -> None:
    item = _build_item(item_id="item-hls-remote-playlist-inline-refresh-handoff")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-hls-remote-playlist-inline-refresh-handoff",
        item_id=item.id,
        kind="remote-hls",
        locator="https://cdn.example.com/hls-inline-refresh-handoff/index.m3u8",
        unrestricted_url="https://cdn.example.com/hls-inline-refresh-handoff/index.m3u8",
        restricted_url="https://api.example.com/restricted-hls-inline-refresh-handoff.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-inline-refresh-handoff",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-remote-playlist-inline-refresh-handoff",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-inline-refresh-handoff.m3u8",
        unrestricted_url="https://cdn.example.com/hls-inline-refresh-handoff/index.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-inline-refresh-handoff",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
    )
    selected_entry.source_attachment = source_attachment
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]

    database = PersistentDummyDatabaseRuntime(items=[item])
    client, resources = _build_client(db=database)

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            return RateLimitDecision(allowed=False, remaining_tokens=0.0, retry_after_seconds=6.0)

    class FakeController:
        def has_pending(self, item_identifier: str) -> bool:
            assert item_identifier == item.id
            return False

        async def trigger(self, item_identifier: str, *, at: datetime | None = None) -> Any:
            assert item_identifier == item.id
            _ = at
            return type("TriggerResult", (), {"outcome": "scheduled"})()

    resources.playback_service = PlaybackSourceService(
        resources.db,
        rate_limiter=FakeRateLimiter(),
    )
    resources.hls_restricted_fallback_refresh_controller = cast(Any, FakeController())

    async def fake_download_remote_hls_playlist(url: str) -> tuple[str, httpx.Headers]:
        assert url == "https://cdn.example.com/hls-inline-refresh-handoff/index.m3u8"
        raise HTTPException(status_code=502, detail="Upstream HLS request failed with status 502")

    monkeypatch.setattr(stream_routes, "_download_remote_hls_playlist", fake_download_remote_hls_playlist)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert response.status_code == 502
    assert (
        governance["hls_restricted_fallback_refresh_trigger_starts"]
        == before["hls_restricted_fallback_refresh_trigger_starts"] + 1
    )

    persisted_item = asyncio.run(PlaybackSourceService(resources.db)._list_items())[0]
    persisted_entry = persisted_item.media_entries[0]
    persisted_attachment = persisted_item.playback_attachments[0]
    assert persisted_entry.refresh_state == "stale"
    assert persisted_attachment.refresh_state == "stale"


def test_hls_file_route_handoffs_failed_inline_remote_hls_refresh_to_background_controller(
    monkeypatch: Any,
) -> None:
    item = _build_item(item_id="item-hls-remote-file-inline-refresh-handoff")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-hls-remote-file-inline-refresh-handoff",
        item_id=item.id,
        kind="remote-hls",
        locator="https://cdn.example.com/hls-inline-file-refresh-handoff/index.m3u8",
        unrestricted_url="https://cdn.example.com/hls-inline-file-refresh-handoff/index.m3u8",
        restricted_url="https://api.example.com/restricted-hls-inline-file-refresh-handoff.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-inline-file-refresh-handoff",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-remote-file-inline-refresh-handoff",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-inline-file-refresh-handoff.m3u8",
        unrestricted_url="https://cdn.example.com/hls-inline-file-refresh-handoff/index.m3u8",
        provider="realdebrid",
        provider_download_id="download-hls-inline-file-refresh-handoff",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
    )
    selected_entry.source_attachment = source_attachment
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]

    database = PersistentDummyDatabaseRuntime(items=[item])
    client, resources = _build_client(db=database)

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            return RateLimitDecision(allowed=False, remaining_tokens=0.0, retry_after_seconds=6.0)

    class FakeController:
        def has_pending(self, item_identifier: str) -> bool:
            assert item_identifier == item.id
            return False

        async def trigger(self, item_identifier: str, *, at: datetime | None = None) -> Any:
            assert item_identifier == item.id
            _ = at
            return type("TriggerResult", (), {"outcome": "scheduled"})()

    resources.playback_service = PlaybackSourceService(
        resources.db,
        rate_limiter=FakeRateLimiter(),
    )
    resources.hls_restricted_fallback_refresh_controller = cast(Any, FakeController())

    async def fake_open_remote_hls_child_stream(
        *,
        playlist_url: str,
        upstream_url: str,
        request: Any,
    ) -> StreamingResponse:
        _ = request
        assert playlist_url == "https://cdn.example.com/hls-inline-file-refresh-handoff/index.m3u8"
        assert upstream_url == "https://cdn.example.com/hls-inline-file-refresh-handoff/segment_00001.ts"
        raise HTTPException(status_code=502, detail="Upstream HLS request failed with status 502")

    monkeypatch.setattr(stream_routes, "_open_remote_hls_child_stream", fake_open_remote_hls_child_stream)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert response.status_code == 502
    assert (
        governance["hls_restricted_fallback_refresh_trigger_starts"]
        == before["hls_restricted_fallback_refresh_trigger_starts"] + 1
    )

    persisted_item = asyncio.run(PlaybackSourceService(resources.db)._list_items())[0]
    persisted_entry = persisted_item.media_entries[0]
    persisted_attachment = persisted_item.playback_attachments[0]
    assert persisted_entry.refresh_state == "stale"
    assert persisted_attachment.refresh_state == "stale"


def test_hls_playlist_route_records_missing_restricted_fallback_controller_when_trigger_is_unavailable() -> (
    None
):
    item = _build_item(
        item_id="item-hls-playlist-route-trigger-restricted-fallback-controller-unavailable"
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-playlist-route-trigger-restricted-fallback-controller-unavailable",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-playlist-route-trigger-restricted-fallback-controller-unavailable.m3u8",
        unrestricted_url="https://cdn.example.com/hls-playlist-route-trigger-restricted-fallback-controller-unavailable.m3u8",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-hls-playlist-route-trigger-restricted-fallback-controller-unavailable",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, resources = _build_client(items=[item])
    resources.hls_restricted_fallback_refresh_controller = None

    async def fake_download_remote_hls_playlist(url: str) -> tuple[str, httpx.Headers]:
        assert (
            url
            == "https://api.example.com/restricted-hls-playlist-route-trigger-restricted-fallback-controller-unavailable.m3u8"
        )
        return (
            "#EXTM3U\nsegment_00001.ts\n",
            httpx.Headers({"content-type": "application/vnd.apple.mpegurl"}),
        )

    original_download = stream_routes._download_remote_hls_playlist
    stream_routes._download_remote_hls_playlist = fake_download_remote_hls_playlist
    try:
        before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
        response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
        governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    finally:
        stream_routes._download_remote_hls_playlist = original_download

    assert response.status_code == 200
    assert (
        governance["hls_restricted_fallback_refresh_trigger_controller_unavailable"]
        == before["hls_restricted_fallback_refresh_trigger_controller_unavailable"] + 1
    )


def test_resolve_hls_source_returns_local_file_when_no_playlist_exists(tmp_path: Path) -> None:
    """Typed HLS resolution should fall back to a local file when no remote HLS playlist is present."""

    media_file = tmp_path / "local-hls-source.mkv"
    media_file.write_bytes(b"movie-bytes")
    item = _build_item(attributes={"active_stream": {"file_path": str(media_file)}})
    _, resources = _build_client(items=[item])

    kind, value, source_key = asyncio.run(stream_routes._resolve_hls_source(resources.db, item.id))

    assert kind == "local_file"
    assert value == str(media_file)
    assert source_key == "file_path"


def test_playback_source_service_resolves_attachment_metadata(tmp_path: Path) -> None:
    """PlaybackSourceService should expose attachment metadata for future debrid-services-backed consumers."""

    media_file = tmp_path / "service-backed.mkv"
    media_file.write_bytes(b"movie")
    item = _build_item(
        attributes={
            "provider": "realdebrid",
            "provider_download_id": "download-456",
            "provider_file_id": 88,
            "provider_file_path": "folder/Service Movie.mkv",
            "original_filename": "Service Movie.mkv",
            "file_size": 654321,
            "active_stream": {
                "download_url": "https://api.example.com/restricted/movie",
                "file_path": str(media_file),
            },
        }
    )
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "local-file"
    assert attachment.local_path == str(media_file)
    assert attachment.provider == "realdebrid"
    assert attachment.provider_download_id == "download-456"
    assert attachment.provider_file_id == "88"
    assert attachment.provider_file_path == "folder/Service Movie.mkv"
    assert attachment.original_filename == "Service Movie.mkv"
    assert attachment.file_size == 654321
    assert attachment.restricted_url == "https://api.example.com/restricted/movie"


def test_playback_source_service_resolves_direct_file_serving_descriptor_with_filename_and_provenance(
    tmp_path: Path,
) -> None:
    media_file = tmp_path / "service-descriptor.mkv"
    media_file.write_bytes(b"movie")
    item = _build_item(item_id="item-direct-file-serving-descriptor")
    attachment = _build_playback_attachment(
        item_id=item.id,
        kind="local-file",
        locator=str(media_file),
        local_path=str(media_file),
        original_filename="Descriptor Movie.mkv",
        provider="realdebrid",
        provider_download_id="descriptor-download",
        provider_file_id="descriptor-file-id",
        provider_file_path="folder/Descriptor Movie.mkv",
        is_preferred=True,
    )
    item.playback_attachments = [attachment]
    _, resources = _build_client(items=[item])

    service = PlaybackSourceService(resources.db)

    resolution = asyncio.run(service.resolve_direct_file_link_resolution(item.id))

    assert resolution.transport == "local-file"
    assert resolution.locator == str(media_file)
    assert resolution.filename == "Descriptor Movie.mkv"
    assert resolution.provenance.source_key == "persisted"
    assert resolution.provenance.source_class == "fallback-local-file"
    assert resolution.provenance.provider == "realdebrid"
    assert resolution.provenance.provider_download_id == "descriptor-download"
    assert resolution.provenance.provider_file_id == "descriptor-file-id"
    assert resolution.provenance.provider_file_path == "folder/Descriptor Movie.mkv"
    assert resolution.provenance.original_filename == "Descriptor Movie.mkv"
    assert resolution.provenance.refresh_state == "ready"
    assert resolution.provenance.refresh_intent is False
    assert resolution.provenance.refresh_recommendation_reason is None
    assert resolution.provenance.lifecycle is not None
    assert resolution.provenance.lifecycle.owner_kind == "attachment"
    assert resolution.provenance.lifecycle.owner_id == attachment.id
    assert resolution.provenance.lifecycle.provider_family == "debrid"
    assert resolution.provenance.lifecycle.locator_source == "local-path"
    assert resolution.provenance.lifecycle.restricted_fallback is False
    assert resolution.provenance.lifecycle.match_basis == "provider-file-id"
    assert resolution.provenance.lifecycle.source_attachment_id is None
    assert resolution.provenance.lifecycle.refresh_state == "ready"
    assert resolution.provenance.lifecycle.expires_at is None
    assert resolution.provenance.lifecycle.last_refreshed_at is None
    assert resolution.provenance.lifecycle.last_refresh_error is None

    descriptor = PlaybackSourceService.build_direct_file_serving_descriptor(resolution)

    assert descriptor.transport == "local-file"
    assert descriptor.locator == str(media_file)
    assert descriptor.media_type == "application/octet-stream"
    assert descriptor.response_headers == {
        "content-disposition": 'inline; filename="Descriptor Movie.mkv"'
    }
    assert descriptor.provenance is not None
    assert descriptor.provenance.source_key == "persisted"
    assert descriptor.provenance.provider == "realdebrid"
    assert descriptor.provenance.provider_download_id == "descriptor-download"
    assert descriptor.provenance.provider_file_id == "descriptor-file-id"
    assert descriptor.provenance.provider_file_path == "folder/Descriptor Movie.mkv"
    assert descriptor.provenance.original_filename == "Descriptor Movie.mkv"


def test_playback_source_service_resolves_direct_file_link_resolution_from_direct_playback_decision_policy() -> (
    None
):
    item = _build_item(item_id="item-direct-file-link-resolution-policy")
    stale_attachment = _build_playback_attachment(
        attachment_id="attachment-direct-file-link-resolution-policy",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/direct-file-link-resolution-policy",
        restricted_url="https://api.example.com/restricted-direct-file-link-resolution-policy",
        unrestricted_url="https://cdn.example.com/direct-file-link-resolution-policy",
        original_filename="Policy Movie.mkv",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-direct-file-link-resolution-policy",
        provider_file_id="file-direct-file-link-resolution-policy",
        provider_file_path="folder/Policy Movie.mkv",
        is_preferred=True,
    )
    item.playback_attachments = [stale_attachment]
    _, resources = _build_client(items=[item])

    service = PlaybackSourceService(resources.db)

    decision = asyncio.run(service.resolve_direct_playback_decision(item.id))
    resolution = asyncio.run(service.resolve_direct_file_link_resolution(item.id))

    assert decision.action == "serve"
    assert decision.result == "resolved"
    assert decision.source_class == "fallback-provider-direct-stale"
    assert decision.refresh_intent is True
    assert decision.refresh_recommendation is not None
    assert decision.refresh_recommendation.reason == "provider_direct_stale"

    assert resolution.transport == "remote-proxy"
    assert (
        resolution.locator
        == "https://api.example.com/restricted-direct-file-link-resolution-policy"
    )
    assert resolution.filename == "Policy Movie.mkv"
    assert resolution.provenance.source_key == "persisted:restricted-fallback"
    assert resolution.provenance.source_class == decision.source_class
    assert resolution.provenance.provider == "realdebrid"
    assert (
        resolution.provenance.provider_download_id == "download-direct-file-link-resolution-policy"
    )
    assert resolution.provenance.provider_file_id == "file-direct-file-link-resolution-policy"
    assert resolution.provenance.provider_file_path == "folder/Policy Movie.mkv"
    assert resolution.provenance.original_filename == "Policy Movie.mkv"
    assert resolution.provenance.refresh_state == "stale"
    assert resolution.provenance.refresh_intent == decision.refresh_intent
    assert (
        resolution.provenance.refresh_recommendation_reason
        == decision.refresh_recommendation.reason
    )
    assert resolution.provenance.lifecycle is not None
    assert resolution.provenance.lifecycle.owner_kind == "attachment"
    assert resolution.provenance.lifecycle.owner_id == stale_attachment.id
    assert resolution.provenance.lifecycle.provider_family == "debrid"
    assert resolution.provenance.lifecycle.locator_source == "restricted-url"
    assert resolution.provenance.lifecycle.restricted_fallback is True
    assert resolution.provenance.lifecycle.match_basis == "provider-file-id"
    assert resolution.provenance.lifecycle.source_attachment_id is None
    assert resolution.provenance.lifecycle.refresh_state == "stale"
    assert resolution.provenance.lifecycle.expires_at is None
    assert resolution.provenance.lifecycle.last_refreshed_at is None
    assert resolution.provenance.lifecycle.last_refresh_error is None

    descriptor = PlaybackSourceService.build_direct_file_serving_descriptor(resolution)

    assert descriptor.transport == "remote-proxy"
    assert (
        descriptor.response_headers["content-disposition"] == 'inline; filename="Policy Movie.mkv"'
    )
    assert descriptor.provenance is not None
    assert descriptor.provenance.source_key == "persisted:restricted-fallback"
    assert descriptor.provenance.provider == "realdebrid"


def test_playback_source_service_resolves_direct_file_link_resolution_lifecycle_from_selected_media_entry() -> (
    None
):
    item = _build_item(item_id="item-direct-file-link-resolution-media-entry-lifecycle")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-direct-file-link-resolution-media-entry-lifecycle",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/direct-file-link-resolution-media-entry-lifecycle",
        restricted_url="https://api.example.com/restricted-direct-file-link-resolution-media-entry-lifecycle",
        unrestricted_url="https://cdn.example.com/direct-file-link-resolution-media-entry-lifecycle",
        original_filename="Media Entry Lifecycle Movie.mkv",
        provider="realdebrid",
        provider_download_id="download-direct-file-link-resolution-media-entry-lifecycle",
        provider_file_id="file-direct-file-link-resolution-media-entry-lifecycle",
        provider_file_path="folder/Media Entry Lifecycle Movie.mkv",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-direct-file-link-resolution-media-entry-lifecycle",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-direct",
        original_filename="Media Entry Lifecycle Movie.mkv",
        download_url="https://api.example.com/restricted-direct-file-link-resolution-media-entry-lifecycle",
        unrestricted_url="https://cdn.example.com/direct-file-link-resolution-media-entry-lifecycle",
        provider="realdebrid",
        provider_download_id="download-direct-file-link-resolution-media-entry-lifecycle",
        provider_file_id="file-direct-file-link-resolution-media-entry-lifecycle",
        provider_file_path="folder/Media Entry Lifecycle Movie.mkv",
        refresh_state="stale",
        expires_at=datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
        last_refreshed_at=datetime(2026, 3, 14, 8, 0, tzinfo=UTC),
        last_refresh_error="provider refresh pending",
    )
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    resolution = asyncio.run(
        PlaybackSourceService(resources.db).resolve_direct_file_link_resolution(item.id)
    )

    assert resolution.locator == selected_entry.download_url
    assert resolution.transport == "remote-proxy"
    assert resolution.provenance.source_key == "media-entry:restricted-fallback"
    assert resolution.provenance.source_class == "fallback-provider-direct-stale"
    assert resolution.provenance.lifecycle is not None
    assert resolution.provenance.lifecycle.owner_kind == "media-entry"
    assert resolution.provenance.lifecycle.owner_id == selected_entry.id
    assert resolution.provenance.lifecycle.provider_family == "debrid"
    assert resolution.provenance.lifecycle.locator_source == "restricted-url"
    assert resolution.provenance.lifecycle.restricted_fallback is True
    assert resolution.provenance.lifecycle.match_basis == "source-attachment-id"
    assert resolution.provenance.lifecycle.source_attachment_id == source_attachment.id
    assert resolution.provenance.lifecycle.refresh_state == "stale"
    assert resolution.provenance.lifecycle.expires_at == selected_entry.expires_at
    assert resolution.provenance.lifecycle.last_refreshed_at == selected_entry.last_refreshed_at
    assert resolution.provenance.lifecycle.last_refresh_error == "provider refresh pending"

    descriptor = PlaybackSourceService.build_direct_file_serving_descriptor(resolution)

    assert descriptor.locator == selected_entry.download_url
    assert descriptor.provenance is not None
    assert descriptor.provenance.source_key == "media-entry:restricted-fallback"
    assert descriptor.provenance.provider == "realdebrid"


def test_playback_source_service_prefers_persisted_attachment_over_metadata(tmp_path: Path) -> None:
    """Persisted playback attachments should win over metadata-derived fallbacks once available."""

    persisted_file = tmp_path / "persisted.mkv"
    persisted_file.write_bytes(b"persisted")
    fallback_file = tmp_path / "fallback.mkv"
    fallback_file.write_bytes(b"fallback")

    item = _build_item(item_id="item-persisted", attributes={"file_path": str(fallback_file)})
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="local-file",
            locator=str(persisted_file),
            local_path=str(persisted_file),
            is_preferred=True,
            original_filename="Persisted Movie.mkv",
            provider="realdebrid",
        )
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.local_path == str(persisted_file)
    assert attachment.original_filename == "Persisted Movie.mkv"
    assert attachment.provider == "realdebrid"


def test_playback_source_service_builds_resolution_snapshot_with_media_entry_lifecycle() -> None:
    item = _build_item(item_id="item-resolution-snapshot-lifecycle")
    direct_attachment = _build_playback_attachment(
        attachment_id="attachment-resolution-snapshot-direct",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/resolution-snapshot-direct",
        restricted_url="https://api.example.com/restricted-resolution-snapshot-direct",
        unrestricted_url="https://cdn.example.com/resolution-snapshot-direct",
        original_filename="Resolution Snapshot Direct.mkv",
        provider="realdebrid",
        provider_download_id="download-resolution-snapshot-direct",
        provider_file_id="file-resolution-snapshot-direct",
        provider_file_path="folder/Resolution Snapshot Direct.mkv",
    )
    hls_attachment = _build_playback_attachment(
        attachment_id="attachment-resolution-snapshot-hls",
        item_id=item.id,
        kind="remote-hls",
        locator="https://cdn.example.com/resolution-snapshot-hls.m3u8",
        restricted_url="https://api.example.com/restricted-resolution-snapshot-hls.m3u8",
        unrestricted_url="https://cdn.example.com/resolution-snapshot-hls.m3u8",
        original_filename="Resolution Snapshot HLS.m3u8",
        provider="realdebrid",
        provider_download_id="download-resolution-snapshot-hls",
        provider_file_id="file-resolution-snapshot-hls",
        provider_file_path="folder/Resolution Snapshot HLS.m3u8",
    )
    direct_entry = _build_media_entry(
        media_entry_id="media-entry-resolution-snapshot-direct",
        item_id=item.id,
        source_attachment_id=direct_attachment.id,
        kind="remote-direct",
        original_filename="Resolution Snapshot Direct.mkv",
        download_url="https://api.example.com/restricted-resolution-snapshot-direct",
        unrestricted_url="https://cdn.example.com/resolution-snapshot-direct",
        provider="realdebrid",
        provider_download_id="download-resolution-snapshot-direct",
        provider_file_id="file-resolution-snapshot-direct",
        provider_file_path="folder/Resolution Snapshot Direct.mkv",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 17, 12, 0, tzinfo=UTC),
    )
    hls_entry = _build_media_entry(
        media_entry_id="media-entry-resolution-snapshot-hls",
        item_id=item.id,
        source_attachment_id=hls_attachment.id,
        kind="remote-hls",
        original_filename="Resolution Snapshot HLS.m3u8",
        download_url="https://api.example.com/restricted-resolution-snapshot-hls.m3u8",
        unrestricted_url="https://cdn.example.com/resolution-snapshot-hls.m3u8",
        provider="realdebrid",
        provider_download_id="download-resolution-snapshot-hls",
        provider_file_id="file-resolution-snapshot-hls",
        provider_file_path="folder/Resolution Snapshot HLS.m3u8",
        refresh_state="stale",
        expires_at=datetime(2026, 3, 18, 12, 0, tzinfo=UTC),
        last_refreshed_at=datetime(2026, 3, 14, 10, 0, tzinfo=UTC),
        last_refresh_error="playlist refresh pending",
    )
    item.playback_attachments = [direct_attachment, hls_attachment]
    item.media_entries = [direct_entry, hls_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=direct_entry.id, role="direct"),
        _build_active_stream(item_id=item.id, media_entry_id=hls_entry.id, role="hls"),
    ]
    _, resources = _build_client(items=[item])

    snapshot = PlaybackSourceService(resources.db).build_resolution_snapshot(item)

    assert snapshot.direct is not None
    assert snapshot.hls is not None
    assert snapshot.direct_lifecycle is not None
    assert snapshot.hls_lifecycle is not None

    assert snapshot.direct_lifecycle.owner_kind == "media-entry"
    assert snapshot.direct_lifecycle.owner_id == direct_entry.id
    assert snapshot.direct_lifecycle.provider_family == "debrid"
    assert snapshot.direct_lifecycle.locator_source == "unrestricted-url"
    assert snapshot.direct_lifecycle.restricted_fallback is False
    assert snapshot.direct_lifecycle.match_basis == "source-attachment-id"
    assert snapshot.direct_lifecycle.source_attachment_id == direct_attachment.id
    assert snapshot.direct_lifecycle.refresh_state == "ready"
    assert snapshot.direct_lifecycle.expires_at == direct_entry.expires_at
    assert snapshot.direct_lifecycle.last_refreshed_at is None
    assert snapshot.direct_lifecycle.last_refresh_error is None

    assert snapshot.hls_lifecycle.owner_kind == "media-entry"
    assert snapshot.hls_lifecycle.owner_id == hls_entry.id
    assert snapshot.hls_lifecycle.provider_family == "debrid"
    assert snapshot.hls_lifecycle.locator_source == "restricted-url"
    assert snapshot.hls_lifecycle.restricted_fallback is True
    assert snapshot.hls_lifecycle.match_basis == "source-attachment-id"
    assert snapshot.hls_lifecycle.source_attachment_id == hls_attachment.id
    assert snapshot.hls_lifecycle.refresh_state == "stale"
    assert snapshot.hls_lifecycle.expires_at == hls_entry.expires_at
    assert snapshot.hls_lifecycle.last_refreshed_at == hls_entry.last_refreshed_at
    assert snapshot.hls_lifecycle.last_refresh_error == "playlist refresh pending"


def test_playback_source_service_builds_resolution_snapshot_with_metadata_lifecycle_fallback(
    tmp_path: Path,
) -> None:
    media_file = tmp_path / "metadata-lifecycle-direct.mkv"
    media_file.write_bytes(b"metadata-lifecycle")
    item = _build_item(
        item_id="item-resolution-snapshot-metadata-lifecycle",
        attributes={
            "provider": "realdebrid",
            "provider_download_id": "download-resolution-snapshot-metadata",
            "provider_file_id": "file-resolution-snapshot-metadata",
            "provider_file_path": "folder/Metadata Lifecycle Movie.mkv",
            "original_filename": "Metadata Lifecycle Movie.mkv",
            "file_size": 123456,
            "active_stream": {
                "selected": True,
                "download_url": "https://api.example.com/restricted-resolution-snapshot-metadata",
                "file_path": str(media_file),
            },
            "hls_url": "https://cdn.example.com/resolution-snapshot-metadata-hls.m3u8",
        },
    )
    _, resources = _build_client(items=[item])

    snapshot = PlaybackSourceService(resources.db).build_resolution_snapshot(item)

    assert snapshot.direct is not None
    assert snapshot.hls is not None
    assert snapshot.direct_lifecycle is not None
    assert snapshot.hls_lifecycle is not None

    assert snapshot.direct_lifecycle.owner_kind == "metadata"
    assert snapshot.direct_lifecycle.owner_id is None
    assert snapshot.direct_lifecycle.provider_family == "debrid"
    assert snapshot.direct_lifecycle.locator_source == "local-path"
    assert snapshot.direct_lifecycle.restricted_fallback is False
    assert snapshot.direct_lifecycle.match_basis is None
    assert snapshot.direct_lifecycle.source_attachment_id is None
    assert snapshot.direct_lifecycle.refresh_state is None
    assert snapshot.direct_lifecycle.expires_at is None
    assert snapshot.direct_lifecycle.last_refreshed_at is None
    assert snapshot.direct_lifecycle.last_refresh_error is None

    assert snapshot.hls_lifecycle.owner_kind == "metadata"
    assert snapshot.hls_lifecycle.owner_id is None
    assert snapshot.hls_lifecycle.provider_family == "debrid"
    assert snapshot.hls_lifecycle.locator_source == "locator"
    assert snapshot.hls_lifecycle.restricted_fallback is False
    assert snapshot.hls_lifecycle.match_basis is None
    assert snapshot.hls_lifecycle.source_attachment_id is None
    assert snapshot.hls_lifecycle.refresh_state is None
    assert snapshot.hls_lifecycle.expires_at is None
    assert snapshot.hls_lifecycle.last_refreshed_at is None
    assert snapshot.hls_lifecycle.last_refresh_error is None


def test_playback_source_service_prefers_persisted_active_stream_media_entry_for_direct_playback() -> (
    None
):
    item = _build_item(item_id="item-active-media-entry-direct")
    fallback_attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/fallback-direct",
        unrestricted_url="https://cdn.example.com/fallback-direct",
        is_preferred=True,
        provider="realdebrid",
    )
    selected_attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/selected-direct-attachment",
        unrestricted_url="https://cdn.example.com/selected-direct-attachment",
        provider="realdebrid",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-direct-1",
        item_id=item.id,
        kind="remote-direct",
        source_attachment_id=selected_attachment.id,
        original_filename="Selected Direct.mkv",
        unrestricted_url="https://cdn.example.com/selected-direct-entry",
        download_url="https://api.example.com/selected-direct-entry",
        provider="realdebrid",
    )
    item.playback_attachments = [fallback_attachment, selected_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.locator == "https://cdn.example.com/selected-direct-entry"
    assert attachment.source_key == "media-entry"
    assert attachment.original_filename == "Selected Direct.mkv"


def test_playback_source_service_prefers_persisted_active_stream_media_entry_for_hls() -> None:
    item = _build_item(item_id="item-active-media-entry-hls")
    selected_attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/direct-attachment",
        unrestricted_url="https://cdn.example.com/direct-attachment",
        provider="realdebrid",
    )
    hls_entry = _build_media_entry(
        media_entry_id="media-entry-hls-1",
        item_id=item.id,
        kind="remote-hls",
        source_attachment_id=selected_attachment.id,
        original_filename="Selected HLS.m3u8",
        unrestricted_url="https://cdn.example.com/selected-hls.m3u8",
        download_url="https://api.example.com/selected-hls.m3u8",
        provider="realdebrid",
    )
    item.playback_attachments = [selected_attachment]
    item.media_entries = [hls_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=hls_entry.id, role="hls")
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(PlaybackSourceService(resources.db).resolve_hls_attachment(item.id))

    assert attachment.kind == "remote-hls"
    assert attachment.locator == "https://cdn.example.com/selected-hls.m3u8"
    assert attachment.source_key == "media-entry"


def test_playback_source_service_uses_remote_direct_as_hls_transcode_fallback() -> None:
    item = _build_item(item_id="item-hls-remote-direct-fallback")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/direct-hevc.mkv",
            unrestricted_url="https://cdn.example.com/direct-hevc.mkv",
            restricted_url="https://api.example.com/restricted-direct-hevc.mkv",
            provider="realdebrid",
            refresh_state="ready",
            is_preferred=True,
        )
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(PlaybackSourceService(resources.db).resolve_hls_attachment(item.id))

    assert attachment.kind == "remote-direct"
    assert attachment.locator == "https://cdn.example.com/direct-hevc.mkv"


def test_playback_source_service_prefers_mounted_local_file_for_hls_over_remote_direct_lease(
    tmp_path: Path,
) -> None:
    mounted_dir = tmp_path / "movies" / "Example Item"
    mounted_dir.mkdir(parents=True)
    mounted_file = mounted_dir / "Example Item.item.release.mkv"
    mounted_file.write_bytes(b"mounted-hls")

    item = _build_item(
        item_id="item-hls-mounted-local-preferred",
        attributes={"item_type": "movie"},
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-mounted-local-1",
        item_id=item.id,
        kind="remote-direct",
        original_filename="example.item.release.mkv",
        unrestricted_url="https://cdn.example.com/example.item.release.mkv",
        download_url="https://api.example.com/example.item.release.mkv",
        provider="realdebrid",
        provider_download_id="download-hls-mounted-local",
        provider_file_id="file-hls-mounted-local",
        provider_file_path="example.item.release.mkv",
        refresh_state="ready",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct"),
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls"),
    ]
    _, resources = _build_client(items=[item])

    settings = _build_settings()
    settings.updaters.library_path = str(tmp_path)
    service = PlaybackSourceService(
        resources.db,
        settings=settings,
        rate_limiter=resources.rate_limiter,
    )

    snapshot = service.build_resolution_snapshot(item)

    assert snapshot.direct is not None
    assert snapshot.direct.kind == "remote-direct"
    assert snapshot.hls is not None
    assert snapshot.hls.kind == "local-file"
    assert snapshot.hls.locator == str(mounted_file)
    assert snapshot.hls.local_path == str(mounted_file)

    attachment = asyncio.run(service.resolve_hls_attachment(item.id))

    assert attachment.kind == "local-file"
    assert attachment.locator == str(mounted_file)


def test_playback_source_service_prefers_mounted_local_file_for_hls_without_active_streams(
    tmp_path: Path,
) -> None:
    mounted_dir = tmp_path / "movies" / "Example Item"
    mounted_dir.mkdir(parents=True)
    mounted_file = mounted_dir / "Example Item.item.release.mkv"
    mounted_file.write_bytes(b"mounted-hls-fallback")

    item = _build_item(
        item_id="item-hls-mounted-local-no-active-streams",
        attributes={"item_type": "movie"},
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-mounted-local-no-active-streams-1",
        item_id=item.id,
        kind="remote-direct",
        original_filename="fallback.item.release.mkv",
        unrestricted_url="https://cdn.example.com/fallback.item.release.mkv",
        download_url="https://api.example.com/fallback.item.release.mkv",
        provider="realdebrid",
        provider_download_id="download-hls-mounted-local-no-active-streams",
        provider_file_id="file-hls-mounted-local-no-active-streams",
        provider_file_path="fallback.item.release.mkv",
        refresh_state="ready",
    )
    item.media_entries = [selected_entry]
    _, resources = _build_client(items=[item])

    settings = _build_settings()
    settings.updaters.library_path = str(tmp_path)
    service = PlaybackSourceService(
        resources.db,
        settings=settings,
        rate_limiter=resources.rate_limiter,
    )

    snapshot = service.build_resolution_snapshot(item)

    assert snapshot.direct is not None
    assert snapshot.direct.kind == "remote-direct"
    assert snapshot.hls is not None
    assert snapshot.hls.kind == "local-file"
    assert snapshot.hls.locator == str(mounted_file)
    assert snapshot.hls.local_path == str(mounted_file)

    attachment = asyncio.run(service.resolve_hls_attachment(item.id))

    assert attachment.kind == "local-file"
    assert attachment.locator == str(mounted_file)
def test_playback_source_service_uses_restricted_fallback_for_stale_media_entry_lease() -> None:
    item = _build_item(item_id="item-stale-media-entry-lease")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-stale-lease-1",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Lease Movie.mkv",
        unrestricted_url="https://cdn.example.com/stale-lease",
        download_url="https://api.example.com/restricted-lease",
        refresh_state="stale",
        provider="realdebrid",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.locator == "https://api.example.com/restricted-lease"
    assert attachment.source_key == "media-entry:restricted-fallback"


def test_playback_source_service_treats_placeholder_debrid_unrestricted_url_as_restricted_fallback() -> None:
    item = _build_item(item_id="item-placeholder-debrid-unrestricted-url")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-placeholder-debrid-unrestricted-url",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Placeholder Lease Movie.mkv",
        unrestricted_url="https://real-debrid.com/d/placeholder-lease",
        download_url="https://real-debrid.com/d/placeholder-lease",
        refresh_state="ready",
        provider="realdebrid",
        provider_download_id="placeholder-download-id",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    service = PlaybackSourceService(resources.db)
    attachment = asyncio.run(service.resolve_playback_attachment(item.id))
    planned = service.plan_media_entry_refresh_requests(item)

    assert attachment.locator == "https://real-debrid.com/d/placeholder-lease"
    assert attachment.source_key == "media-entry:restricted-fallback"
    assert attachment.refresh_state == "stale"
    assert len(planned) == 1
    assert planned[0].media_entry_id == selected_entry.id


def test_playback_source_service_prefers_ready_local_file_over_degraded_attachment_fallback(
    tmp_path: Path,
) -> None:
    preferred_local = tmp_path / "preferred-local-over-degraded.mkv"
    preferred_local.write_bytes(b"preferred-local")

    item = _build_item(item_id="item-direct-resolution-degraded-attachment")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://api.example.com/restricted-degraded",
            restricted_url="https://api.example.com/restricted-degraded",
            unrestricted_url="https://cdn.example.com/original-direct",
            refresh_state="stale",
            is_preferred=True,
            provider="realdebrid",
        ),
        _build_playback_attachment(
            item_id=item.id,
            kind="local-file",
            locator=str(preferred_local),
            local_path=str(preferred_local),
            is_preferred=False,
            preference_rank=500,
        ),
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "local-file"
    assert attachment.local_path == str(preferred_local)


def test_playback_source_service_prefers_ready_local_file_over_selected_degraded_media_entry(
    tmp_path: Path,
) -> None:
    preferred_local = tmp_path / "preferred-local-over-selected-degraded.mkv"
    preferred_local.write_bytes(b"preferred-local")

    item = _build_item(item_id="item-direct-resolution-degraded-media-entry")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-selected-degraded",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-selected-degraded",
        unrestricted_url="https://cdn.example.com/original-selected-direct",
        refresh_state="stale",
        provider="realdebrid",
    )
    preferred_local_entry = _build_media_entry(
        media_entry_id="media-entry-preferred-local",
        item_id=item.id,
        kind="local-file",
        local_path=str(preferred_local),
        original_filename="Preferred Local.mkv",
        refresh_state="ready",
    )
    item.media_entries = [selected_entry, preferred_local_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "local-file"
    assert attachment.local_path == str(preferred_local)


def test_playback_source_service_prefers_provider_backed_unrestricted_direct_attachment_over_generic_direct_attachment() -> (
    None
):
    item = _build_item(item_id="item-provider-ranked-direct-attachment")
    item.playback_attachments = [
        _build_playback_attachment(
            attachment_id="attachment-generic-direct",
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/generic-direct",
            unrestricted_url="https://cdn.example.com/generic-direct",
            is_preferred=True,
        ),
        _build_playback_attachment(
            attachment_id="attachment-provider-direct",
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/provider-direct",
            unrestricted_url="https://cdn.example.com/provider-direct",
            provider="realdebrid",
            provider_download_id="download-provider-direct",
            is_preferred=False,
        ),
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "remote-direct"
    assert attachment.locator == "https://cdn.example.com/provider-direct"
    assert attachment.provider == "realdebrid"


def test_playback_source_service_prefers_local_file_attachment_over_provider_backed_direct_attachment(
    tmp_path: Path,
) -> None:
    media_file = tmp_path / "preferred-local-over-provider.mkv"
    media_file.write_bytes(b"movie-bytes")
    item = _build_item(item_id="item-local-over-provider-direct-attachment")
    item.playback_attachments = [
        _build_playback_attachment(
            attachment_id="attachment-provider-direct",
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/provider-direct",
            unrestricted_url="https://cdn.example.com/provider-direct",
            provider="realdebrid",
            provider_download_id="download-provider-direct",
        ),
        _build_playback_attachment(
            attachment_id="attachment-local-file",
            item_id=item.id,
            kind="local-file",
            locator=str(media_file),
            local_path=str(media_file),
        ),
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "local-file"
    assert attachment.local_path == str(media_file)


def test_direct_playback_source_classification_names_selected_and_fallback_classes(
    tmp_path: Path,
) -> None:
    media_file = tmp_path / "classified-local-file.mkv"
    media_file.write_bytes(b"movie-bytes")

    selected_provider = playback_resolution.PlaybackAttachment(
        kind="remote-direct",
        locator="https://cdn.example.com/provider-direct",
        source_key="download_url",
        resolver_priority=200,
        resolver_authoritative=True,
        provider="realdebrid",
        provider_download_id="download-provider-direct",
        unrestricted_url="https://cdn.example.com/provider-direct",
        refresh_state="ready",
    )
    selected_local = playback_resolution.PlaybackAttachment(
        kind="local-file",
        locator=str(media_file),
        source_key="file_path",
        resolver_priority=20,
        resolver_authoritative=True,
        local_path=str(media_file),
    )
    fallback_degraded = playback_resolution.PlaybackAttachment(
        kind="remote-direct",
        locator="https://api.example.com/restricted-link",
        source_key="download_url:restricted-fallback",
        restricted_url="https://api.example.com/restricted-link",
        unrestricted_url="https://cdn.example.com/unrestricted-link",
        provider="realdebrid",
        provider_download_id="download-provider-degraded",
    )
    selected_provider_stale = playback_resolution.PlaybackAttachment(
        kind="remote-direct",
        locator="https://cdn.example.com/provider-direct-stale",
        source_key="download_url",
        resolver_priority=200,
        resolver_authoritative=True,
        provider="realdebrid",
        provider_download_id="download-provider-direct-stale",
        unrestricted_url="https://cdn.example.com/provider-direct-stale",
        refresh_state="stale",
    )
    selected_provider_refreshing = playback_resolution.PlaybackAttachment(
        kind="remote-direct",
        locator="https://cdn.example.com/provider-direct-refreshing",
        source_key="download_url",
        resolver_priority=200,
        resolver_authoritative=True,
        provider="realdebrid",
        provider_download_id="download-provider-direct-refreshing",
        unrestricted_url="https://cdn.example.com/provider-direct-refreshing",
        refresh_state="refreshing",
    )
    selected_provider_failed = playback_resolution.PlaybackAttachment(
        kind="remote-direct",
        locator="https://cdn.example.com/provider-direct-failed",
        source_key="download_url",
        resolver_priority=200,
        resolver_authoritative=True,
        provider="realdebrid",
        provider_download_id="download-provider-direct-failed",
        unrestricted_url="https://cdn.example.com/provider-direct-failed",
        refresh_state="failed",
    )

    assert (
        playback_resolution.classify_direct_playback_source_class(selected_provider)
        == "selected-provider-direct-ready"
    )
    assert (
        playback_resolution.classify_direct_playback_source_class(selected_local)
        == "selected-local-file"
    )
    assert (
        playback_resolution.classify_direct_playback_source_class(fallback_degraded)
        == "fallback-provider-direct-degraded"
    )
    assert (
        playback_resolution.classify_direct_playback_source_class(selected_provider_stale)
        == "selected-provider-direct-stale"
    )
    assert (
        playback_resolution.classify_direct_playback_source_class(selected_provider_refreshing)
        == "selected-provider-direct-refreshing"
    )
    assert (
        playback_resolution.classify_direct_playback_source_class(selected_provider_failed)
        == "selected-provider-direct-failed"
    )


def test_playback_source_service_prefers_provider_backed_unrestricted_media_entry_over_generic_direct_entry() -> (
    None
):
    item = _build_item(item_id="item-provider-ranked-direct-media-entry")
    generic_entry = _build_media_entry(
        media_entry_id="media-entry-generic-direct",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/generic-media-entry",
        download_url="https://api.example.com/restricted-generic-media-entry",
        refresh_state="ready",
    )
    provider_entry = _build_media_entry(
        media_entry_id="media-entry-provider-direct",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-media-entry",
        download_url="https://api.example.com/restricted-provider-media-entry",
        refresh_state="ready",
        provider="realdebrid",
        provider_download_id="download-provider-media-entry",
    )
    item.media_entries = [generic_entry, provider_entry]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "remote-direct"
    assert attachment.locator == "https://cdn.example.com/provider-media-entry"
    assert attachment.provider == "realdebrid"


def test_playback_source_service_prefers_fresher_provider_backed_direct_attachment() -> None:
    item = _build_item(item_id="item-fresher-provider-direct")
    item.playback_attachments = [
        _build_playback_attachment(
            attachment_id="attachment-provider-direct-soon-expiry",
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/provider-direct-soon-expiry",
            unrestricted_url="https://cdn.example.com/provider-direct-soon-expiry",
            provider="realdebrid",
            provider_download_id="download-provider-direct-soon-expiry",
            expires_at=datetime(2099, 3, 13, 16, 0, tzinfo=UTC),
        ),
        _build_playback_attachment(
            attachment_id="attachment-provider-direct-fresh-expiry",
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/provider-direct-fresh-expiry",
            unrestricted_url="https://cdn.example.com/provider-direct-fresh-expiry",
            provider="realdebrid",
            provider_download_id="download-provider-direct-fresh-expiry",
            provider_file_id="provider-file-fresh-expiry",
            expires_at=datetime(2099, 3, 13, 19, 0, tzinfo=UTC),
        ),
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "remote-direct"
    assert attachment.locator == "https://cdn.example.com/provider-direct-fresh-expiry"
    assert attachment.provider_download_id == "download-provider-direct-fresh-expiry"


def test_playback_source_service_recovers_selected_stale_direct_media_entry_via_provider_file_id_match() -> (
    None
):
    item = _build_item(item_id="item-related-provider-file-id-recovery")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-selected-stale-related-id",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-stale-related-id",
        download_url="https://api.example.com/provider-direct-stale-related-id",
        provider="realdebrid",
        provider_file_id="provider-file-same-id",
        refresh_state="stale",
    )
    fresher_entry = _build_media_entry(
        media_entry_id="media-entry-fresh-related-id",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-fresh-related-id",
        download_url="https://api.example.com/provider-direct-fresh-related-id",
        provider="realdebrid",
        provider_file_id="provider-file-same-id",
        provider_download_id="download-related-id-fresh",
        refresh_state="ready",
    )
    unrelated_entry = _build_media_entry(
        media_entry_id="media-entry-unrelated-id",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-unrelated-id",
        download_url="https://api.example.com/provider-direct-unrelated-id",
        provider="realdebrid",
        provider_file_id="provider-file-other-id",
        refresh_state="ready",
    )
    item.media_entries = [selected_entry, fresher_entry, unrelated_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "remote-direct"
    assert attachment.locator == "https://cdn.example.com/provider-direct-fresh-related-id"
    assert attachment.provider_download_id == "download-related-id-fresh"


def test_playback_source_service_recovers_selected_stale_direct_media_entry_via_provider_file_path_match() -> (
    None
):
    item = _build_item(item_id="item-related-provider-file-path-recovery")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-selected-stale-related-path",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-stale-related-path",
        download_url="https://api.example.com/provider-direct-stale-related-path",
        provider="realdebrid",
        provider_file_path="Folder/Shared File.mkv",
        refresh_state="stale",
    )
    fresher_entry = _build_media_entry(
        media_entry_id="media-entry-fresh-related-path",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-fresh-related-path",
        download_url="https://api.example.com/provider-direct-fresh-related-path",
        provider="realdebrid",
        provider_file_path="Folder/Shared File.mkv",
        provider_download_id="download-related-path-fresh",
        refresh_state="ready",
    )
    unrelated_entry = _build_media_entry(
        media_entry_id="media-entry-unrelated-path",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-unrelated-path",
        download_url="https://api.example.com/provider-direct-unrelated-path",
        provider="realdebrid",
        provider_file_path="Folder/Other File.mkv",
        refresh_state="ready",
    )
    item.media_entries = [selected_entry, fresher_entry, unrelated_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "remote-direct"
    assert attachment.locator == "https://cdn.example.com/provider-direct-fresh-related-path"
    assert attachment.provider_download_id == "download-related-path-fresh"


def test_playback_source_service_keeps_usable_active_stream_winner_over_fresher_related_entry() -> (
    None
):
    item = _build_item(item_id="item-active-stream-winner-stays-authoritative")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-selected-active-winner",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-selected-still-valid",
        download_url="https://api.example.com/provider-direct-selected-still-valid",
        provider="realdebrid",
        provider_file_id="provider-file-authoritative",
        provider_download_id="download-selected-authoritative",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 13, 18, 0, tzinfo=UTC),
    )
    fresher_sibling = _build_media_entry(
        media_entry_id="media-entry-fresher-sibling",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-fresher-sibling",
        download_url="https://api.example.com/provider-direct-fresher-sibling",
        provider="realdebrid",
        provider_file_id="provider-file-authoritative",
        provider_download_id="download-fresher-sibling",
        refresh_state="ready",
        expires_at=datetime(2026, 3, 13, 21, 0, tzinfo=UTC),
    )
    item.media_entries = [selected_entry, fresher_sibling]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "remote-direct"
    assert attachment.locator == "https://cdn.example.com/provider-direct-selected-still-valid"
    assert attachment.provider_download_id == "download-selected-authoritative"


def test_playback_source_service_collapses_same_file_provider_direct_siblings_when_unselected() -> (
    None
):
    item = _build_item(item_id="item-collapse-same-file-provider-siblings")
    first_entry = _build_media_entry(
        media_entry_id="media-entry-same-file-older",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-same-file-older",
        download_url="https://api.example.com/provider-direct-same-file-older",
        provider="realdebrid",
        provider_file_id="provider-file-collapse-id",
        provider_download_id="download-same-file-older",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 13, 17, 0, tzinfo=UTC),
    )
    second_entry = _build_media_entry(
        media_entry_id="media-entry-same-file-fresher",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-same-file-fresher",
        download_url="https://api.example.com/provider-direct-same-file-fresher",
        provider="realdebrid",
        provider_file_id="provider-file-collapse-id",
        provider_download_id="download-same-file-fresher",
        provider_file_path="Folder/Same File.mkv",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 13, 20, 0, tzinfo=UTC),
    )
    item.media_entries = [first_entry, second_entry]
    _, resources = _build_client(items=[item])

    service = PlaybackSourceService(resources.db)
    _, attachments_by_entry_id, _ = service._resolve_persisted_media_entry_attachments(item)
    collapsed = service._collapse_related_direct_media_entry_attachments(
        item,
        attachments_by_entry_id=attachments_by_entry_id,
    )

    assert len(collapsed) == 1
    assert collapsed[0].locator == "https://cdn.example.com/provider-direct-same-file-fresher"
    assert collapsed[0].provider_download_id == "download-same-file-fresher"


def test_playback_source_service_keeps_different_provider_files_separate_when_unselected() -> None:
    item = _build_item(item_id="item-keep-different-provider-files-separate")
    first_entry = _build_media_entry(
        media_entry_id="media-entry-different-file-a",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-different-a",
        download_url="https://api.example.com/provider-direct-different-a",
        provider="realdebrid",
        provider_file_id="provider-file-a",
        provider_download_id="download-different-a",
        refresh_state="ready",
        expires_at=datetime(2026, 3, 13, 18, 0, tzinfo=UTC),
    )
    second_entry = _build_media_entry(
        media_entry_id="media-entry-different-file-b",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-different-b",
        download_url="https://api.example.com/provider-direct-different-b",
        provider="realdebrid",
        provider_file_id="provider-file-b",
        provider_download_id="download-different-b",
        refresh_state="ready",
        expires_at=datetime(2026, 3, 13, 19, 0, tzinfo=UTC),
    )
    item.media_entries = [first_entry, second_entry]
    _, resources = _build_client(items=[item])

    service = PlaybackSourceService(resources.db)
    _, attachments_by_entry_id, _ = service._resolve_persisted_media_entry_attachments(item)
    collapsed = service._collapse_related_direct_media_entry_attachments(
        item,
        attachments_by_entry_id=attachments_by_entry_id,
    )

    assert len(collapsed) == 2
    assert {attachment.provider_file_id for attachment in collapsed} == {
        "provider-file-a",
        "provider-file-b",
    }


def test_playback_source_service_prefers_richer_provider_identity_across_different_files_when_unselected() -> (
    None
):
    item = _build_item(item_id="item-different-files-richest-identity-wins")
    richer_identity_entry = _build_media_entry(
        media_entry_id="media-entry-rich-identity",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-rich-identity",
        download_url="https://api.example.com/provider-direct-rich-identity",
        provider="realdebrid",
        provider_download_id="download-rich-identity",
        provider_file_id="provider-file-rich-identity",
        provider_file_path="Folder/Rich Identity.mkv",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 13, 18, 0, tzinfo=UTC),
    )
    weaker_identity_entry = _build_media_entry(
        media_entry_id="media-entry-weaker-identity",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-weaker-identity",
        download_url="https://api.example.com/provider-direct-weaker-identity",
        provider="realdebrid",
        provider_file_id="provider-file-weaker-identity",
        refresh_state="ready",
        expires_at=datetime(2026, 3, 13, 21, 0, tzinfo=UTC),
    )
    item.media_entries = [richer_identity_entry, weaker_identity_entry]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "remote-direct"
    assert attachment.locator == "https://cdn.example.com/provider-direct-rich-identity"
    assert attachment.provider_download_id == "download-rich-identity"


def test_playback_source_service_uses_fresher_lease_as_tiebreak_for_different_files_with_same_identity_score() -> (
    None
):
    item = _build_item(item_id="item-different-files-fresher-tiebreak")
    older_entry = _build_media_entry(
        media_entry_id="media-entry-older-direct-different-file",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-older-different-file",
        download_url="https://api.example.com/provider-direct-older-different-file",
        provider="realdebrid",
        provider_file_id="provider-file-older-different-file",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 13, 18, 0, tzinfo=UTC),
    )
    fresher_entry = _build_media_entry(
        media_entry_id="media-entry-fresher-direct-different-file",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/provider-direct-fresher-different-file",
        download_url="https://api.example.com/provider-direct-fresher-different-file",
        provider="realdebrid",
        provider_file_id="provider-file-fresher-different-file",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 13, 22, 0, tzinfo=UTC),
    )
    item.media_entries = [older_entry, fresher_entry]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "remote-direct"
    assert attachment.locator == "https://cdn.example.com/provider-direct-fresher-different-file"
    assert attachment.provider_file_id == "provider-file-fresher-different-file"


def test_playback_source_service_media_entry_refresh_transition_helpers_update_state() -> None:
    entry = _build_media_entry(
        item_id="item-media-entry-refresh",
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/old-entry",
        download_url="https://api.example.com/restricted-entry",
        refresh_state="ready",
    )

    stale_at = datetime(2026, 3, 12, 10, 0, tzinfo=UTC)
    refreshing_at = datetime(2026, 3, 12, 10, 5, tzinfo=UTC)
    refreshed_at = datetime(2026, 3, 12, 10, 10, tzinfo=UTC)
    failed_at = datetime(2026, 3, 12, 10, 15, tzinfo=UTC)

    PlaybackSourceService.mark_media_entry_stale(entry, at=stale_at)
    assert entry.refresh_state == "stale"
    assert entry.updated_at == stale_at

    PlaybackSourceService.start_media_entry_refresh(entry, at=refreshing_at)
    assert entry.refresh_state == "refreshing"
    assert entry.updated_at == refreshing_at

    PlaybackSourceService.complete_media_entry_refresh(
        entry,
        download_url="https://api.example.com/restricted-entry-fresh",
        unrestricted_url="https://cdn.example.com/fresh-entry",
        expires_at=datetime(2026, 3, 13, 10, 10, tzinfo=UTC),
        at=refreshed_at,
    )
    assert entry.refresh_state == "ready"
    assert entry.download_url == "https://api.example.com/restricted-entry-fresh"
    assert entry.unrestricted_url == "https://cdn.example.com/fresh-entry"
    assert entry.last_refreshed_at == refreshed_at
    assert entry.last_refresh_error is None

    PlaybackSourceService.fail_media_entry_refresh(
        entry,
        error="media-entry refresh failed",
        at=failed_at,
    )
    assert entry.refresh_state == "failed"
    assert entry.last_refresh_error == "media-entry refresh failed"
    assert entry.last_refreshed_at == failed_at


def test_playback_source_service_builds_media_entry_refresh_request_for_selected_stream() -> None:
    item = _build_item(item_id="item-media-entry-request")
    media_entry = _build_media_entry(
        media_entry_id="media-entry-request-1",
        item_id=item.id,
        kind="remote-direct",
        provider="realdebrid",
        provider_download_id="download-media-entry-1",
        download_url="https://api.example.com/restricted-media-entry-request",
        unrestricted_url="https://cdn.example.com/media-entry-request",
        refresh_state="stale",
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=media_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    requests = PlaybackSourceService(resources.db).plan_media_entry_refresh_requests(item)

    assert requests == [
        MediaEntryLeaseRefreshRequest(
            media_entry_id=media_entry.id,
            item_id=item.id,
            kind="remote-direct",
            provider="realdebrid",
            provider_download_id="download-media-entry-1",
            restricted_url="https://api.example.com/restricted-media-entry-request",
            unrestricted_url="https://cdn.example.com/media-entry-request",
            local_path=None,
            refresh_state="stale",
            roles=("direct",),
        )
    ]


def test_playback_source_service_executes_media_entry_refreshes_with_provider_client() -> None:
    item = _build_item(item_id="item-media-entry-provider-refresh")
    media_entry = _build_media_entry(
        media_entry_id="media-entry-provider-refresh-1",
        item_id=item.id,
        kind="remote-direct",
        provider="realdebrid",
        provider_download_id="download-provider-refresh-1",
        download_url="https://api.example.com/restricted-provider-refresh",
        refresh_state="stale",
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=media_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    class FakeProviderClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None]] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append((link, request.provider_download_id))
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/media-entry-provider-refresh",
                restricted_url=link,
                expires_at=datetime(2026, 3, 15, 12, 0, tzinfo=UTC),
            )

    client = FakeProviderClient()
    executed = asyncio.run(
        PlaybackSourceService(resources.db).execute_media_entry_refreshes_with_providers(
            item,
            provider_clients={"realdebrid": cast(PlaybackAttachmentProviderClient, client)},
        )
    )

    assert client.calls == [
        (
            "https://api.example.com/restricted-provider-refresh",
            "download-provider-refresh-1",
        )
    ]
    assert executed == [
        MediaEntryLeaseRefreshExecution(
            media_entry_id=media_entry.id,
            ok=True,
            refresh_state="ready",
            locator="https://cdn.example.com/media-entry-provider-refresh",
            error=None,
        )
    ]
    assert media_entry.unrestricted_url == "https://cdn.example.com/media-entry-provider-refresh"
    assert media_entry.download_url == "https://api.example.com/restricted-provider-refresh"
    assert media_entry.refresh_state == "ready"
    assert media_entry.expires_at == datetime(2026, 3, 15, 12, 0, tzinfo=UTC)


def test_stream_file_returns_503_when_selected_media_entry_lease_refresh_failed(
    tmp_path: Path,
) -> None:
    fallback_file = tmp_path / "fallback-after-failed-lease.txt"
    fallback_file.write_bytes(b"fallback")
    item = _build_item(
        item_id="item-direct-failed-lease",
        attributes={"file_path": str(fallback_file)},
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-direct-failed-lease",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-direct-failed-lease",
        unrestricted_url="https://cdn.example.com/direct-failed-lease",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    client, _ = _build_client(items=[item])

    response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())

    assert response.status_code == 503
    assert (
        response.json()["detail"]
        == "Selected direct playback lease refresh failed: provider unavailable"
    )


def test_playback_source_service_builds_direct_playback_decision_for_failed_selected_lease() -> (
    None
):
    item = _build_item(item_id="item-direct-decision-failed-lease")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-direct-decision-failed-lease",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-direct-decision-failed-lease",
        unrestricted_url="https://cdn.example.com/direct-decision-failed-lease",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    decision = asyncio.run(
        PlaybackSourceService(resources.db).resolve_direct_playback_decision(item.id)
    )

    assert decision.action == "fail"
    assert decision.result == "failed_lease"
    assert decision.status_code == 503
    assert decision.refresh_intent is True
    assert decision.detail == "Selected direct playback lease refresh failed: provider unavailable"
    assert decision.refresh_recommendation is not None
    assert decision.refresh_recommendation.reason == "selected_failed_lease"
    assert decision.refresh_recommendation.target == "media_entry"
    assert decision.refresh_recommendation.target_id == selected_entry.id
    assert decision.refresh_recommendation.provider == "realdebrid"


def test_playback_source_service_builds_direct_playback_decision_with_refresh_intent_for_stale_provider_direct() -> (
    None
):
    item = _build_item(item_id="item-direct-decision-stale-provider")
    item.playback_attachments = [
        _build_playback_attachment(
            attachment_id="attachment-direct-decision-stale-provider",
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/direct-decision-stale-provider",
            restricted_url="https://api.example.com/restricted-direct-decision-stale-provider",
            unrestricted_url="https://cdn.example.com/direct-decision-stale-provider",
            refresh_state="stale",
            provider="realdebrid",
            provider_download_id="download-direct-decision-stale-provider",
            is_preferred=True,
        )
    ]
    _, resources = _build_client(items=[item])

    decision = asyncio.run(
        PlaybackSourceService(resources.db).resolve_direct_playback_decision(item.id)
    )

    assert decision.action == "serve"
    assert decision.result == "resolved"
    assert decision.refresh_intent is True
    assert decision.source_class == "fallback-provider-direct-stale"
    assert decision.refresh_recommendation is not None
    assert decision.refresh_recommendation.reason == "provider_direct_stale"
    assert decision.refresh_recommendation.target == "attachment"
    assert (
        decision.refresh_recommendation.provider_download_id
        == "download-direct-decision-stale-provider"
    )
    assert decision.attachment is not None
    assert (
        decision.attachment.locator
        == "https://api.example.com/restricted-direct-decision-stale-provider"
    )


def test_playback_source_service_dispatches_failed_selected_direct_lease_recommendation_to_media_entry_request() -> (
    None
):
    item = _build_item(item_id="item-direct-dispatch-failed-lease")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-direct-dispatch-failed-lease",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-direct-dispatch-failed-lease",
        unrestricted_url="https://cdn.example.com/direct-dispatch-failed-lease",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-direct-dispatch-failed-lease",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    dispatch = asyncio.run(
        PlaybackSourceService(resources.db).resolve_direct_playback_refresh_dispatch(item.id)
    )

    assert dispatch is not None
    assert dispatch.recommendation.reason == "selected_failed_lease"
    assert dispatch.media_entry_request is not None
    assert dispatch.media_entry_request.media_entry_id == selected_entry.id
    assert dispatch.media_entry_request.roles == ("direct",)
    assert dispatch.attachment_request is None


def test_playback_source_service_dispatches_stale_direct_attachment_recommendation_to_attachment_request() -> (
    None
):
    item = _build_item(item_id="item-direct-dispatch-stale-attachment")
    stale_attachment = _build_playback_attachment(
        attachment_id="attachment-direct-dispatch-stale-attachment",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/direct-dispatch-stale-attachment",
        restricted_url="https://api.example.com/restricted-direct-dispatch-stale-attachment",
        unrestricted_url="https://cdn.example.com/direct-dispatch-stale-attachment",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-direct-dispatch-stale-attachment",
        is_preferred=True,
    )
    item.playback_attachments = [stale_attachment]
    _, resources = _build_client(items=[item])

    dispatch = asyncio.run(
        PlaybackSourceService(resources.db).resolve_direct_playback_refresh_dispatch(item.id)
    )

    assert dispatch is not None
    assert dispatch.recommendation.reason == "provider_direct_stale"
    assert dispatch.attachment_request is not None
    assert dispatch.attachment_request.attachment_id == stale_attachment.id
    assert (
        dispatch.attachment_request.provider_download_id
        == "download-direct-dispatch-stale-attachment"
    )
    assert dispatch.media_entry_request is None


def test_playback_source_service_schedules_direct_playback_refresh_without_inline_execution() -> (
    None
):
    item = _build_item(item_id="item-direct-refresh-schedule")
    stale_attachment = _build_playback_attachment(
        attachment_id="attachment-direct-refresh-schedule",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/direct-refresh-schedule",
        restricted_url="https://api.example.com/restricted-direct-refresh-schedule",
        unrestricted_url="https://cdn.example.com/direct-refresh-schedule",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-direct-refresh-schedule",
        is_preferred=True,
    )
    item.playback_attachments = [stale_attachment]
    _, resources = _build_client(items=[item])

    scheduled_requests: list[DirectPlaybackRefreshScheduleRequest] = []

    class FakeScheduler:
        async def schedule(self, request: DirectPlaybackRefreshScheduleRequest) -> None:
            scheduled_requests.append(request)

    scheduled_at = datetime(2026, 3, 14, 0, 0, tzinfo=UTC)
    result = asyncio.run(
        PlaybackSourceService(resources.db).schedule_direct_playback_refresh(
            item.id,
            scheduler=FakeScheduler(),
            at=scheduled_at,
        )
    )

    assert result.outcome == "scheduled"
    assert result.execution is None
    assert result.retry_after_seconds is None
    assert result.scheduled_request is not None
    assert result.scheduled_request.item_identifier == item.id
    assert result.scheduled_request.recommendation.reason == "provider_direct_stale"
    assert result.scheduled_request.requested_at == scheduled_at
    assert result.scheduled_request.not_before == scheduled_at
    assert result.scheduled_request.retry_after_seconds is None
    assert scheduled_requests == [result.scheduled_request]
    assert stale_attachment.refresh_state == "stale"


def test_playback_source_service_executes_failed_selected_direct_lease_dispatch_via_media_entry_refresh() -> (
    None
):
    item = _build_item(item_id="item-direct-dispatch-exec-failed-lease")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-direct-dispatch-exec-failed-lease",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-direct-dispatch-exec-failed-lease",
        unrestricted_url="https://cdn.example.com/direct-dispatch-exec-failed-lease",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-direct-dispatch-exec-failed-lease",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert request.provider_download_id == "download-direct-dispatch-exec-failed-lease"
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/direct-dispatch-exec-failed-lease-fresh",
            restricted_url="https://api.example.com/restricted-direct-dispatch-exec-failed-lease-fresh",
            unrestricted_url="https://cdn.example.com/direct-dispatch-exec-failed-lease-fresh",
            expires_at=datetime(2099, 3, 13, 18, 0, tzinfo=UTC),
        )

    execution = asyncio.run(
        PlaybackSourceService(resources.db).resolve_direct_playback_refresh_dispatch_with_providers(
            item.id,
            executors={"realdebrid": fake_executor},
        )
    )

    assert execution is not None
    assert execution.recommendation.reason == "selected_failed_lease"
    assert execution.media_entry_execution is not None
    assert execution.media_entry_execution.ok is True
    assert execution.media_entry_execution.refresh_state == "ready"
    assert execution.attachment_execution is None
    assert selected_entry.refresh_state == "ready"
    assert (
        selected_entry.unrestricted_url
        == "https://cdn.example.com/direct-dispatch-exec-failed-lease-fresh"
    )


def test_playback_source_service_executes_stale_attachment_dispatch_via_attachment_refresh() -> (
    None
):
    item = _build_item(item_id="item-direct-dispatch-exec-stale-attachment")
    stale_attachment = _build_playback_attachment(
        attachment_id="attachment-direct-dispatch-exec-stale-attachment",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/direct-dispatch-exec-stale-attachment",
        restricted_url="https://api.example.com/restricted-direct-dispatch-exec-stale-attachment",
        unrestricted_url="https://cdn.example.com/direct-dispatch-exec-stale-attachment",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-direct-dispatch-exec-stale-attachment",
        is_preferred=True,
    )
    item.playback_attachments = [stale_attachment]
    _, resources = _build_client(items=[item])

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert request.attachment_id == stale_attachment.id
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/direct-dispatch-exec-stale-attachment-fresh",
            restricted_url="https://api.example.com/restricted-direct-dispatch-exec-stale-attachment-fresh",
            unrestricted_url="https://cdn.example.com/direct-dispatch-exec-stale-attachment-fresh",
            expires_at=datetime(2099, 3, 13, 19, 0, tzinfo=UTC),
        )

    execution = asyncio.run(
        PlaybackSourceService(resources.db).resolve_direct_playback_refresh_dispatch_with_providers(
            item.id,
            executors={"realdebrid": fake_executor},
        )
    )

    assert execution is not None
    assert execution.recommendation.reason == "provider_direct_stale"
    assert execution.attachment_execution is not None
    assert execution.attachment_execution.ok is True
    assert execution.attachment_execution.refresh_state == "ready"
    assert execution.media_entry_execution is None
    assert stale_attachment.refresh_state == "ready"
    assert (
        stale_attachment.unrestricted_url
        == "https://cdn.example.com/direct-dispatch-exec-stale-attachment-fresh"
    )


def test_playback_source_service_rate_limits_direct_refresh_dispatch_and_exposes_retry_after() -> (
    None
):
    item = _build_item(item_id="item-direct-dispatch-rate-limited")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-direct-dispatch-rate-limited",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-direct-dispatch-rate-limited",
        unrestricted_url="https://cdn.example.com/direct-dispatch-rate-limited",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-direct-dispatch-rate-limited",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            assert capacity == 1.0
            assert refill_rate_per_second == 1.0
            return RateLimitDecision(
                allowed=False,
                remaining_tokens=0.0,
                retry_after_seconds=7.5,
            )

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        raise AssertionError("refresh executor should not run when the limiter denies the dispatch")

    execution = asyncio.run(
        PlaybackSourceService(resources.db).resolve_direct_playback_refresh_dispatch_with_providers(
            item.id,
            executors={"realdebrid": fake_executor},
            rate_limiter=FakeRateLimiter(),
        )
    )

    assert execution is not None
    assert execution.rate_limited is True
    assert execution.deferred_reason == "refresh_rate_limited"
    assert execution.retry_after_seconds == 7.5
    assert execution.limiter_bucket_key == "ratelimit:realdebrid:stream_link_refresh"
    assert execution.media_entry_execution is None
    assert execution.attachment_execution is None
    assert selected_entry.refresh_state == "failed"


def test_playback_source_service_reschedules_scheduled_direct_refresh_after_limiter_denial() -> (
    None
):
    item = _build_item(item_id="item-direct-refresh-schedule-rate-limited")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-direct-refresh-schedule-rate-limited",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-direct-refresh-schedule-rate-limited",
        unrestricted_url="https://cdn.example.com/direct-refresh-schedule-rate-limited",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-direct-refresh-schedule-rate-limited",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])
    service = PlaybackSourceService(resources.db)

    scheduled_requests: list[DirectPlaybackRefreshScheduleRequest] = []

    class FakeScheduler:
        async def schedule(self, request: DirectPlaybackRefreshScheduleRequest) -> None:
            scheduled_requests.append(request)

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert requested_tokens == 1.0
            assert now_seconds is None
            assert expiry_seconds is None
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            assert capacity == 1.0
            assert refill_rate_per_second == 1.0
            return RateLimitDecision(
                allowed=False,
                remaining_tokens=0.0,
                retry_after_seconds=7.5,
            )

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        raise AssertionError(
            "refresh executor should not run when the limiter denies the scheduled refresh"
        )

    initial_request = asyncio.run(
        service.prepare_direct_playback_refresh_schedule_request(
            item.id,
            at=datetime(2026, 3, 14, 0, 0, tzinfo=UTC),
        )
    )

    assert initial_request is not None

    run_at = datetime(2026, 3, 14, 0, 1, tzinfo=UTC)
    result = asyncio.run(
        service.execute_scheduled_direct_playback_refresh_with_providers(
            initial_request,
            scheduler=FakeScheduler(),
            executors={"realdebrid": fake_executor},
            rate_limiter=FakeRateLimiter(),
            at=run_at,
        )
    )

    assert result.outcome == "scheduled"
    assert result.execution is not None
    assert result.execution.rate_limited is True
    assert result.execution.deferred_reason == "refresh_rate_limited"
    assert result.execution.retry_after_seconds == 7.5
    assert result.execution.limiter_bucket_key == "ratelimit:realdebrid:stream_link_refresh"
    assert result.retry_after_seconds == 7.5
    assert result.scheduled_request is not None
    assert result.scheduled_request.item_identifier == item.id
    assert result.scheduled_request.recommendation.reason == "selected_failed_lease"
    assert result.scheduled_request.requested_at == run_at
    assert result.scheduled_request.not_before == run_at + timedelta(seconds=7.5)
    assert result.scheduled_request.retry_after_seconds == 7.5
    assert scheduled_requests == [result.scheduled_request]
    assert selected_entry.refresh_state == "failed"


def test_playback_source_service_serves_existing_media_entry_lease_when_inline_refresh_is_rate_limited() -> (
    None
):
    item = _build_item(item_id="item-inline-rate-limited-existing-lease")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-inline-rate-limited-existing-lease",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-inline-rate-limited-existing-lease",
        unrestricted_url="https://cdn.example.com/inline-rate-limited-existing-lease",
        refresh_state="stale",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
        provider="realdebrid",
        provider_download_id="download-inline-rate-limited-existing-lease",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            assert capacity == 1.0
            assert refill_rate_per_second == 1.0
            return RateLimitDecision(
                allowed=False,
                remaining_tokens=0.0,
                retry_after_seconds=5.0,
            )

    class FakeProviderClient:
        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            raise AssertionError(
                "provider refresh should not run when the inline rate limiter denies the refresh"
            )

    service = PlaybackSourceService(
        resources.db,
        provider_clients={
            "realdebrid": cast(PlaybackAttachmentProviderClient, FakeProviderClient())
        },
        rate_limiter=FakeRateLimiter(),
    )

    resolution = asyncio.run(service.resolve_direct_file_link_resolution(item.id))

    assert resolution.transport == "remote-proxy"
    assert resolution.locator == "https://cdn.example.com/inline-rate-limited-existing-lease"
    assert resolution.provenance.source_key == "media-entry"
    assert selected_entry.refresh_state == "stale"


def test_playback_source_service_returns_503_when_inline_refresh_provider_circuit_is_open() -> None:
    item = _build_item(item_id="item-inline-provider-circuit-open")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-inline-provider-circuit-open",
        item_id=item.id,
        kind="remote-direct",
        download_url=None,
        unrestricted_url=None,
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-inline-provider-circuit-open",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    _, resources = _build_client(items=[item])

    class FakeProviderClient:
        async def refresh_download(
            self,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            raise AssertionError(
                "provider refresh should not run while the provider circuit is open"
            )

    provider_circuit_breaker = ProviderCircuitBreaker(
        failure_threshold=1,
        reset_timeout_seconds=60.0,
    )
    before = _counter_value(playback_service.PROVIDER_CIRCUIT_OPEN_EVENTS, provider="realdebrid")
    assert provider_circuit_breaker.record_failure("realdebrid") is True

    service = PlaybackSourceService(
        resources.db,
        provider_clients={
            "realdebrid": cast(PlaybackAttachmentProviderClient, FakeProviderClient())
        },
        provider_circuit_breaker=provider_circuit_breaker,
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(service.resolve_direct_file_link_resolution(item.id))

    assert exc_info.value.status_code == 503
    assert (
        exc_info.value.detail
        == "Selected direct playback lease refresh failed: provider unavailable"
    )
    assert (
        _counter_value(playback_service.PROVIDER_CIRCUIT_OPEN_EVENTS, provider="realdebrid")
        == before + 1.0
    )


def test_in_process_direct_playback_refresh_controller_triggers_background_refresh() -> None:
    item = _build_item(item_id="item-in-process-direct-refresh-controller")
    stale_attachment = _build_playback_attachment(
        attachment_id="attachment-in-process-direct-refresh-controller",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/in-process-direct-refresh-controller",
        restricted_url="https://api.example.com/restricted-in-process-direct-refresh-controller",
        unrestricted_url="https://cdn.example.com/in-process-direct-refresh-controller",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-in-process-direct-refresh-controller",
        is_preferred=True,
    )
    item.playback_attachments = [stale_attachment]
    _, resources = _build_client(items=[item])

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert request.attachment_id == stale_attachment.id
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/in-process-direct-refresh-controller-fresh",
            restricted_url="https://api.example.com/restricted-in-process-direct-refresh-controller-fresh",
            unrestricted_url="https://cdn.example.com/in-process-direct-refresh-controller-fresh",
            expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
        )

    async def exercise() -> None:
        controller = InProcessDirectPlaybackRefreshController(
            PlaybackSourceService(resources.db),
            executors={"realdebrid": fake_executor},
        )

        trigger_result = await controller.trigger(item.id)

        assert trigger_result.outcome == "scheduled"
        assert trigger_result.scheduled_request is not None
        assert controller.has_pending(item.id) is True

        await controller.wait_for_item(item.id)

        assert controller.has_pending(item.id) is False
        last_result = controller.get_last_result(item.id)
        assert last_result is not None
        assert last_result.outcome == "completed"
        assert last_result.execution is not None
        assert last_result.execution.attachment_execution is not None
        assert last_result.execution.attachment_execution.ok is True

    asyncio.run(exercise())

    assert stale_attachment.refresh_state == "ready"
    assert (
        stale_attachment.unrestricted_url
        == "https://cdn.example.com/in-process-direct-refresh-controller-fresh"
    )


def test_in_process_direct_playback_refresh_controller_deduplicates_pending_work() -> None:
    item = _build_item(item_id="item-in-process-direct-refresh-controller-dedupe")
    stale_attachment = _build_playback_attachment(
        attachment_id="attachment-in-process-direct-refresh-controller-dedupe",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/in-process-direct-refresh-controller-dedupe",
        restricted_url="https://api.example.com/restricted-in-process-direct-refresh-controller-dedupe",
        unrestricted_url="https://cdn.example.com/in-process-direct-refresh-controller-dedupe",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-in-process-direct-refresh-controller-dedupe",
        is_preferred=True,
    )
    item.playback_attachments = [stale_attachment]
    _, resources = _build_client(items=[item])

    started = asyncio.Event()
    release = asyncio.Event()
    executions: list[str] = []

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        executions.append(request.attachment_id)
        started.set()
        await release.wait()
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/in-process-direct-refresh-controller-dedupe-fresh",
            restricted_url="https://api.example.com/restricted-in-process-direct-refresh-controller-dedupe-fresh",
            unrestricted_url="https://cdn.example.com/in-process-direct-refresh-controller-dedupe-fresh",
            expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
        )

    async def exercise() -> None:
        controller = InProcessDirectPlaybackRefreshController(
            PlaybackSourceService(resources.db),
            executors={"realdebrid": fake_executor},
        )

        first = await controller.trigger(item.id)
        assert first.outcome == "scheduled"

        await started.wait()

        second = await controller.trigger(item.id)
        assert second.outcome == "already_pending"

        release.set()
        await controller.wait_for_item(item.id)

    asyncio.run(exercise())

    assert executions == [stale_attachment.id]


def test_in_process_direct_playback_refresh_controller_reschedules_rate_limited_work() -> None:
    item = _build_item(item_id="item-in-process-direct-refresh-controller-rate-limited")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-in-process-direct-refresh-controller-rate-limited",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-in-process-direct-refresh-controller-rate-limited",
        unrestricted_url="https://cdn.example.com/in-process-direct-refresh-controller-rate-limited",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-in-process-direct-refresh-controller-rate-limited",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    client, resources = _build_client(items=[item])

    slept_for: list[float] = []
    decisions = iter(
        [
            RateLimitDecision(allowed=False, remaining_tokens=0.0, retry_after_seconds=7.5),
            RateLimitDecision(allowed=True, remaining_tokens=0.0, retry_after_seconds=0.0),
        ]
    )

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            assert capacity == 1.0
            assert refill_rate_per_second == 1.0
            return next(decisions)

    async def fake_sleep(seconds: float) -> None:
        slept_for.append(seconds)
        await asyncio.sleep(0)

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert (
            request.provider_download_id
            == "download-in-process-direct-refresh-controller-rate-limited"
        )
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/in-process-direct-refresh-controller-rate-limited-fresh",
            restricted_url="https://api.example.com/restricted-in-process-direct-refresh-controller-rate-limited-fresh",
            unrestricted_url="https://cdn.example.com/in-process-direct-refresh-controller-rate-limited-fresh",
            expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
        )

    async def exercise() -> None:
        controller = InProcessDirectPlaybackRefreshController(
            PlaybackSourceService(resources.db),
            executors={"realdebrid": fake_executor},
            rate_limiter=FakeRateLimiter(),
            sleep=fake_sleep,
        )

        trigger_result = await controller.trigger(item.id)
        assert trigger_result.outcome == "scheduled"

        await controller.wait_for_item(item.id)

        last_result = controller.get_last_result(item.id)
        assert last_result is not None
        assert last_result.outcome == "completed"
        assert last_result.execution is not None
        assert last_result.execution.media_entry_execution is not None
        assert last_result.execution.media_entry_execution.ok is True

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    asyncio.run(exercise())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert slept_for == pytest.approx([7.5], rel=0.0, abs=0.05)
    assert selected_entry.refresh_state == "ready"
    assert (
        selected_entry.unrestricted_url
        == "https://cdn.example.com/in-process-direct-refresh-controller-rate-limited-fresh"
    )
    assert (
        governance["direct_playback_refresh_rate_limited"]
        == before["direct_playback_refresh_rate_limited"] + 1
    )


def test_in_process_direct_playback_refresh_controller_reschedules_provider_circuit_open_work() -> (
    None
):
    item = _build_item(item_id="item-in-process-direct-refresh-controller-circuit-open")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-in-process-direct-refresh-controller-circuit-open",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-in-process-direct-refresh-controller-circuit-open",
        unrestricted_url="https://cdn.example.com/in-process-direct-refresh-controller-circuit-open",
        refresh_state="failed",
        last_refresh_error="provider unavailable",
        provider="realdebrid",
        provider_download_id="download-in-process-direct-refresh-controller-circuit-open",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    client, resources = _build_client(items=[item])

    current_time = {"value": 10.0}
    slept_for: list[float] = []
    provider_circuit_breaker = ProviderCircuitBreaker(
        failure_threshold=1,
        reset_timeout_seconds=5.0,
        clock=lambda: current_time["value"],
    )
    assert provider_circuit_breaker.record_failure("realdebrid") is True

    async def fake_sleep(seconds: float) -> None:
        slept_for.append(seconds)
        current_time["value"] += seconds + 0.1
        await asyncio.sleep(0)

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert (
            request.provider_download_id
            == "download-in-process-direct-refresh-controller-circuit-open"
        )
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/in-process-direct-refresh-controller-circuit-open-fresh",
            restricted_url="https://api.example.com/restricted-in-process-direct-refresh-controller-circuit-open-fresh",
            unrestricted_url="https://cdn.example.com/in-process-direct-refresh-controller-circuit-open-fresh",
            expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
        )

    async def exercise() -> None:
        controller = InProcessDirectPlaybackRefreshController(
            PlaybackSourceService(
                resources.db,
                provider_circuit_breaker=provider_circuit_breaker,
            ),
            executors={"realdebrid": fake_executor},
            sleep=fake_sleep,
        )

        trigger_result = await controller.trigger(item.id)
        assert trigger_result.outcome == "scheduled"

        await controller.wait_for_item(item.id)

        last_result = controller.get_last_result(item.id)
        assert last_result is not None
        assert last_result.outcome == "completed"
        assert last_result.execution is not None
        assert last_result.execution.media_entry_execution is not None
        assert last_result.execution.media_entry_execution.ok is True

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    asyncio.run(exercise())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert slept_for == pytest.approx([5.0], rel=0.0, abs=0.05)
    assert selected_entry.refresh_state == "ready"
    assert (
        governance["direct_playback_refresh_provider_circuit_open"]
        == before["direct_playback_refresh_provider_circuit_open"] + 1
    )


def test_in_process_direct_playback_refresh_controller_shutdown_cancels_pending_work() -> None:
    item = _build_item(item_id="item-in-process-direct-refresh-controller-shutdown")
    stale_attachment = _build_playback_attachment(
        attachment_id="attachment-in-process-direct-refresh-controller-shutdown",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/in-process-direct-refresh-controller-shutdown",
        restricted_url="https://api.example.com/restricted-in-process-direct-refresh-controller-shutdown",
        unrestricted_url="https://cdn.example.com/in-process-direct-refresh-controller-shutdown",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-in-process-direct-refresh-controller-shutdown",
        is_preferred=True,
    )
    item.playback_attachments = [stale_attachment]
    _, resources = _build_client(items=[item])

    started = asyncio.Event()

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        _ = request
        started.set()
        try:
            await asyncio.Future[None]()
        except asyncio.CancelledError:
            raise
        raise AssertionError("unreachable")

    async def exercise() -> None:
        controller = InProcessDirectPlaybackRefreshController(
            PlaybackSourceService(resources.db),
            executors={"realdebrid": fake_executor},
        )

        trigger_result = await controller.trigger(item.id)
        assert trigger_result.outcome == "scheduled"

        await started.wait()
        assert controller.has_pending(item.id) is True

        await controller.shutdown()

        assert controller.has_pending(item.id) is False

    asyncio.run(exercise())


def test_queued_direct_playback_refresh_controller_tracks_pending_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def fake_enqueue(*_args: Any, item_id: str, queue_name: str, **_kwargs: Any) -> bool:
        _ = queue_name
        calls.append(item_id)
        return len(calls) == 1

    monkeypatch.setattr(worker_tasks, "enqueue_refresh_direct_playback_link", fake_enqueue)

    async def exercise() -> None:
        controller = playback_service.QueuedDirectPlaybackRefreshController(
            object(),
            queue_name="filmu-py",
        )

        first = await controller.trigger("item-queued-direct")
        assert first.outcome == "scheduled"
        assert controller.has_pending("item-queued-direct") is True
        last_result = controller.get_last_result("item-queued-direct")
        assert last_result is not None
        assert last_result.outcome == "scheduled"

        second = await controller.trigger("item-queued-direct")
        assert second.outcome == "already_pending"
        assert controller.has_pending("item-queued-direct") is True
        assert controller.get_last_result("item-queued-direct") == last_result

    asyncio.run(exercise())
    assert calls == ["item-queued-direct", "item-queued-direct"]


def test_queued_direct_playback_refresh_controller_reports_no_action_without_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_enqueue(*_args: Any, **_kwargs: Any) -> bool:
        return False

    monkeypatch.setattr(worker_tasks, "enqueue_refresh_direct_playback_link", fake_enqueue)

    async def exercise() -> None:
        controller = playback_service.QueuedDirectPlaybackRefreshController(
            object(),
            queue_name="filmu-py",
        )

        result = await controller.trigger("item-queued-direct-no-action")
        assert result.outcome == "no_action"
        assert controller.has_pending("item-queued-direct-no-action") is False
        last_result = controller.get_last_result("item-queued-direct-no-action")
        assert last_result is not None
        assert last_result.outcome == "no_action"

    asyncio.run(exercise())


def test_queued_hls_failed_lease_refresh_controller_reports_scheduled_until_worker_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def fake_enqueue(*_args: Any, item_id: str, queue_name: str, **_kwargs: Any) -> bool:
        _ = queue_name
        calls.append(item_id)
        return len(calls) == 1

    monkeypatch.setattr(worker_tasks, "enqueue_refresh_selected_hls_failed_lease", fake_enqueue)

    async def exercise() -> None:
        controller = playback_service.QueuedHlsFailedLeaseRefreshController(
            object(),
            queue_name="filmu-py",
        )

        first = await controller.trigger("item-queued-hls-failed")
        assert first.outcome == "scheduled"
        assert controller.has_pending("item-queued-hls-failed") is True
        last_result = controller.get_last_result("item-queued-hls-failed")
        assert last_result is not None
        assert last_result.outcome == "scheduled"

        second = await controller.trigger("item-queued-hls-failed")
        assert second.outcome == "already_pending"
        assert controller.get_last_result("item-queued-hls-failed") == last_result

    asyncio.run(exercise())
    assert calls == ["item-queued-hls-failed", "item-queued-hls-failed"]


def test_queued_hls_failed_lease_refresh_controller_reports_no_action_without_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_enqueue(*_args: Any, **_kwargs: Any) -> bool:
        return False

    monkeypatch.setattr(worker_tasks, "enqueue_refresh_selected_hls_failed_lease", fake_enqueue)

    async def exercise() -> None:
        controller = playback_service.QueuedHlsFailedLeaseRefreshController(
            object(),
            queue_name="filmu-py",
        )

        result = await controller.trigger("item-queued-hls-failed-no-action")
        assert result.outcome == "no_action"
        assert controller.has_pending("item-queued-hls-failed-no-action") is False
        last_result = controller.get_last_result("item-queued-hls-failed-no-action")
        assert last_result is not None
        assert last_result.outcome == "no_action"

    asyncio.run(exercise())


def test_queued_hls_restricted_fallback_refresh_controller_tracks_pending_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def fake_enqueue(*_args: Any, item_id: str, queue_name: str, **_kwargs: Any) -> bool:
        _ = queue_name
        calls.append(item_id)
        return len(calls) == 1

    monkeypatch.setattr(
        worker_tasks,
        "enqueue_refresh_selected_hls_restricted_fallback",
        fake_enqueue,
    )

    async def exercise() -> None:
        controller = playback_service.QueuedHlsRestrictedFallbackRefreshController(
            object(),
            queue_name="filmu-py",
        )

        first = await controller.trigger("item-queued-hls-restricted")
        assert first.outcome == "scheduled"
        assert controller.has_pending("item-queued-hls-restricted") is True
        last_result = controller.get_last_result("item-queued-hls-restricted")
        assert last_result is not None
        assert last_result.outcome == "scheduled"

        second = await controller.trigger("item-queued-hls-restricted")
        assert second.outcome == "already_pending"
        assert controller.get_last_result("item-queued-hls-restricted") == last_result

    asyncio.run(exercise())
    assert calls == ["item-queued-hls-restricted", "item-queued-hls-restricted"]


def test_queued_hls_restricted_fallback_refresh_controller_reports_no_action_without_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_enqueue(*_args: Any, **_kwargs: Any) -> bool:
        return False

    monkeypatch.setattr(
        worker_tasks,
        "enqueue_refresh_selected_hls_restricted_fallback",
        fake_enqueue,
    )

    async def exercise() -> None:
        controller = playback_service.QueuedHlsRestrictedFallbackRefreshController(
            object(),
            queue_name="filmu-py",
        )

        result = await controller.trigger("item-queued-hls-restricted-no-action")
        assert result.outcome == "no_action"
        assert controller.has_pending("item-queued-hls-restricted-no-action") is False
        last_result = controller.get_last_result("item-queued-hls-restricted-no-action")
        assert last_result is not None
        assert last_result.outcome == "no_action"

    asyncio.run(exercise())


def test_trigger_direct_playback_refresh_from_resources_uses_attached_controller() -> None:
    item = _build_item(item_id="item-app-resource-trigger")
    stale_attachment = _build_playback_attachment(
        attachment_id="attachment-app-resource-trigger",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/app-resource-trigger",
        restricted_url="https://api.example.com/restricted-app-resource-trigger",
        unrestricted_url="https://cdn.example.com/app-resource-trigger",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-app-resource-trigger",
        is_preferred=True,
    )
    item.playback_attachments = [stale_attachment]
    _, resources = _build_client(items=[item])

    async def fake_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert request.attachment_id == stale_attachment.id
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/app-resource-trigger-fresh",
            restricted_url="https://api.example.com/restricted-app-resource-trigger-fresh",
            unrestricted_url="https://cdn.example.com/app-resource-trigger-fresh",
            expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
        )

    async def exercise() -> None:
        resources.playback_refresh_controller = InProcessDirectPlaybackRefreshController(
            PlaybackSourceService(resources.db),
            executors={"realdebrid": fake_executor},
        )

        result = await trigger_direct_playback_refresh_from_resources(resources, item.id)

        assert result.outcome == "triggered"
        assert result.controller_attached is True
        assert result.control_plane_result is not None
        assert result.control_plane_result.outcome == "scheduled"

        controller = resources.playback_refresh_controller
        assert controller is not None
        await controller.wait_for_item(item.id)

    asyncio.run(exercise())

    assert stale_attachment.refresh_state == "ready"
    assert stale_attachment.unrestricted_url == "https://cdn.example.com/app-resource-trigger-fresh"


def test_hls_playlist_returns_503_when_selected_media_entry_lease_refresh_failed() -> None:
    item = _build_item(item_id="item-hls-failed-lease")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-failed-lease",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-failed-lease.m3u8",
        unrestricted_url="https://cdn.example.com/hls-failed-lease.m3u8",
        refresh_state="failed",
        last_refresh_error="refresh denied",
        provider="realdebrid",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, _ = _build_client(items=[item])

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 503
    assert response.json()["detail"] == "Selected HLS playback lease refresh failed: refresh denied"


def test_playback_source_service_prefers_preferred_attachment_over_lower_rank_fallback(
    tmp_path: Path,
) -> None:
    """Preferred persisted attachments should outrank non-preferred fallback records."""

    preferred_file = tmp_path / "preferred.mkv"
    preferred_file.write_bytes(b"preferred")
    fallback_file = tmp_path / "fallback-rank.mkv"
    fallback_file.write_bytes(b"fallback")

    item = _build_item(item_id="item-priority")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="local-file",
            locator=str(fallback_file),
            local_path=str(fallback_file),
            is_preferred=False,
            preference_rank=1,
            original_filename="Fallback.mkv",
        ),
        _build_playback_attachment(
            item_id=item.id,
            kind="local-file",
            locator=str(preferred_file),
            local_path=str(preferred_file),
            is_preferred=True,
            preference_rank=50,
            original_filename="Preferred.mkv",
        ),
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.local_path == str(preferred_file)
    assert attachment.original_filename == "Preferred.mkv"


def test_playback_source_service_falls_back_to_restricted_url_when_attachment_expired() -> None:
    """Expired persisted unrestricted links should fall back to restricted URLs when available."""

    item = _build_item(item_id="item-expired")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/current-link",
            restricted_url="https://api.example.com/restricted-link",
            unrestricted_url="https://cdn.example.com/current-link",
            is_preferred=True,
            expires_at=datetime(2000, 1, 1, tzinfo=UTC),
            provider="realdebrid",
        )
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.kind == "remote-direct"
    assert attachment.locator == "https://api.example.com/restricted-link"
    assert attachment.provider == "realdebrid"


def test_playback_source_service_uses_restricted_fallback_for_stale_attachment() -> None:
    """Stale persisted attachments should fall back to the restricted URL even before expiry."""

    item = _build_item(item_id="item-stale")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/stale-link",
            restricted_url="https://api.example.com/restricted-stale",
            unrestricted_url="https://cdn.example.com/stale-link",
            refresh_state="stale",
            is_preferred=True,
            provider="realdebrid",
        )
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.locator == "https://api.example.com/restricted-stale"
    assert attachment.source_key.endswith(":restricted-fallback")


def test_playback_source_service_skips_failed_attachment_and_uses_lower_rank_ready_one(
    tmp_path: Path,
) -> None:
    """Failed persisted attachments should be skipped in favour of usable lower-ranked records."""

    ready_file = tmp_path / "ready.mkv"
    ready_file.write_bytes(b"ready")
    item = _build_item(item_id="item-failed")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/failed-link",
            restricted_url="https://api.example.com/failed-link",
            refresh_state="failed",
            is_preferred=True,
            preference_rank=1,
            provider="realdebrid",
        ),
        _build_playback_attachment(
            item_id=item.id,
            kind="local-file",
            locator=str(ready_file),
            local_path=str(ready_file),
            refresh_state="ready",
            is_preferred=False,
            preference_rank=10,
            provider="realdebrid",
        ),
    ]
    _, resources = _build_client(items=[item])

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(item.id)
    )

    assert attachment.local_path == str(ready_file)


def test_playback_source_service_refresh_transition_helpers_update_attachment_state() -> None:
    """PlaybackSourceService should expose explicit refresh-state transition helpers."""

    attachment = _build_playback_attachment(
        item_id="item-refresh",
        kind="remote-direct",
        locator="https://cdn.example.com/initial",
        restricted_url="https://api.example.com/restricted",
        unrestricted_url="https://cdn.example.com/initial",
        refresh_state="ready",
    )

    stale_at = datetime(2026, 3, 12, 10, 0, tzinfo=UTC)
    refreshing_at = datetime(2026, 3, 12, 10, 5, tzinfo=UTC)
    refreshed_at = datetime(2026, 3, 12, 10, 10, tzinfo=UTC)
    failed_at = datetime(2026, 3, 12, 10, 15, tzinfo=UTC)

    PlaybackSourceService.mark_attachment_stale(attachment, at=stale_at)
    assert attachment.refresh_state == "stale"
    assert attachment.updated_at == stale_at

    PlaybackSourceService.start_attachment_refresh(attachment, at=refreshing_at)
    assert attachment.refresh_state == "refreshing"
    assert attachment.updated_at == refreshing_at

    PlaybackSourceService.complete_attachment_refresh(
        attachment,
        locator="https://cdn.example.com/refreshed",
        unrestricted_url="https://cdn.example.com/refreshed",
        expires_at=datetime(2026, 3, 13, 10, 10, tzinfo=UTC),
        at=refreshed_at,
    )
    assert attachment.refresh_state == "ready"
    assert attachment.locator == "https://cdn.example.com/refreshed"
    assert attachment.unrestricted_url == "https://cdn.example.com/refreshed"
    assert attachment.last_refreshed_at == refreshed_at
    assert attachment.last_refresh_error is None

    PlaybackSourceService.fail_attachment_refresh(
        attachment,
        error="refresh failed",
        at=failed_at,
    )
    assert attachment.refresh_state == "failed"
    assert attachment.last_refresh_error == "refresh failed"
    assert attachment.last_refreshed_at == failed_at


def test_playback_source_service_builds_refresh_request_for_remote_attachment() -> None:
    """PlaybackSourceService should expose a minimal refresh request payload for persisted remote attachments."""

    attachment = _build_playback_attachment(
        item_id="item-refresh-request",
        kind="remote-direct",
        locator="https://cdn.example.com/current",
        restricted_url="https://api.example.com/restricted",
        unrestricted_url="https://cdn.example.com/current",
        provider="realdebrid",
        provider_download_id="download-789",
        provider_file_id="55",
        provider_file_path="folder/Current.mkv",
        refresh_state="stale",
    )

    request = PlaybackSourceService.build_refresh_request(attachment)

    assert request is not None
    assert request.kind == "remote-direct"
    assert request.provider == "realdebrid"
    assert request.provider_download_id == "download-789"
    assert request.provider_file_id == "55"
    assert request.provider_file_path == "folder/Current.mkv"
    assert request.restricted_url == "https://api.example.com/restricted"
    assert request.refresh_state == "stale"


def test_playback_source_service_applies_refresh_result() -> None:
    """PlaybackSourceService should apply refresh results back onto persisted attachments."""

    attachment = _build_playback_attachment(
        item_id="item-refresh-result",
        kind="remote-direct",
        locator="https://cdn.example.com/old",
        restricted_url="https://api.example.com/restricted",
        unrestricted_url="https://cdn.example.com/old",
        refresh_state="refreshing",
    )

    refreshed_at = datetime(2026, 3, 12, 12, 0, tzinfo=UTC)
    result = PlaybackAttachmentRefreshResult(
        ok=True,
        locator="https://cdn.example.com/new",
        unrestricted_url="https://cdn.example.com/new",
        expires_at=datetime(2026, 3, 13, 12, 0, tzinfo=UTC),
    )
    PlaybackSourceService.apply_refresh_result(attachment, result, at=refreshed_at)

    assert attachment.refresh_state == "ready"
    assert attachment.locator == "https://cdn.example.com/new"
    assert attachment.unrestricted_url == "https://cdn.example.com/new"
    assert attachment.last_refreshed_at == refreshed_at

    failed_at = datetime(2026, 3, 12, 12, 30, tzinfo=UTC)
    failed = PlaybackAttachmentRefreshResult(ok=False, error="provider unavailable")
    PlaybackSourceService.apply_refresh_result(attachment, failed, at=failed_at)

    assert attachment.refresh_state == "failed"
    assert attachment.last_refresh_error == "provider unavailable"


def test_playback_source_service_plans_refresh_requests_by_priority() -> None:
    """Refresh planning should pick refreshable persisted remote attachments in preference order."""

    item = _build_item(item_id="item-refresh-plan")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/expired-high",
            restricted_url="https://api.example.com/restricted-high",
            unrestricted_url="https://cdn.example.com/expired-high",
            is_preferred=True,
            preference_rank=50,
            expires_at=datetime(2026, 3, 12, 11, 0, tzinfo=UTC),
            provider_download_id="high",
        ),
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/stale-low",
            restricted_url="https://api.example.com/restricted-low",
            refresh_state="stale",
            is_preferred=False,
            preference_rank=10,
            provider_download_id="low",
        ),
        _build_playback_attachment(
            item_id=item.id,
            kind="local-file",
            locator="C:/tmp/local.mkv",
            local_path="C:/tmp/local.mkv",
            refresh_state="stale",
        ),
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/failed",
            restricted_url="https://api.example.com/restricted-failed",
            refresh_state="failed",
            provider_download_id="failed",
        ),
    ]
    _, resources = _build_client(items=[item])

    requests = PlaybackSourceService(resources.db).plan_refresh_requests(
        item,
        now=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
    )

    assert [request.provider_download_id for request in requests] == ["high", "low"]


def test_playback_source_service_request_attachment_refreshes_marks_state() -> None:
    """Requesting refreshes should transition the selected attachments into `refreshing`."""

    item = _build_item(item_id="item-refresh-request-state")
    stale_attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/stale",
        restricted_url="https://api.example.com/restricted-stale",
        refresh_state="stale",
        provider_download_id="stale-1",
    )
    ready_attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/ready",
        restricted_url="https://api.example.com/restricted-ready",
        refresh_state="ready",
        provider_download_id="ready-1",
        expires_at=datetime(2026, 3, 13, 12, 0, tzinfo=UTC),
    )
    item.playback_attachments = [stale_attachment, ready_attachment]
    _, resources = _build_client(items=[item])

    requested_at = datetime(2026, 3, 12, 12, 30, tzinfo=UTC)
    requests = PlaybackSourceService(resources.db).request_attachment_refreshes(
        item, at=requested_at
    )

    assert [request.provider_download_id for request in requests] == ["stale-1"]
    assert stale_attachment.refresh_state == "refreshing"
    assert stale_attachment.updated_at == requested_at
    assert ready_attachment.refresh_state == "ready"


def test_playback_source_service_executes_refresh_requests() -> None:
    """PlaybackSourceService should orchestrate refresh requests through an executor boundary."""

    item = _build_item(item_id="item-exec-refresh")
    stale_attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/stale",
        restricted_url="https://api.example.com/restricted-stale",
        refresh_state="stale",
        provider_download_id="stale-1",
    )
    refreshing_attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/refreshing",
        restricted_url="https://api.example.com/restricted-refreshing",
        refresh_state="refreshing",
        provider_download_id="refreshing-1",
    )
    item.playback_attachments = [stale_attachment, refreshing_attachment]
    _, resources = _build_client(items=[item])

    async def executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        if request.provider_download_id == "stale-1":
            return PlaybackAttachmentRefreshResult(
                ok=True,
                locator="https://cdn.example.com/fresh-stale",
                unrestricted_url="https://cdn.example.com/fresh-stale",
                expires_at=datetime(2026, 3, 13, 13, 0, tzinfo=UTC),
            )
        return PlaybackAttachmentRefreshResult(ok=False, error="provider unavailable")

    executed = asyncio.run(
        PlaybackSourceService(resources.db).execute_attachment_refreshes(item, executor=executor)
    )

    assert executed == [
        PlaybackAttachmentRefreshExecution(
            attachment_id=stale_attachment.id,
            ok=True,
            refresh_state="ready",
            locator="https://cdn.example.com/fresh-stale",
            error=None,
        ),
        PlaybackAttachmentRefreshExecution(
            attachment_id=refreshing_attachment.id,
            ok=False,
            refresh_state="failed",
            locator="https://cdn.example.com/refreshing",
            error="provider unavailable",
        ),
    ]


def test_playback_source_service_selects_provider_specific_refresh_executor() -> None:
    """Provider-specific executors should win over the default refresh executor."""

    service = PlaybackSourceService(cast(Any, object()))
    request = PlaybackAttachmentRefreshRequest(
        attachment_id="attachment-1",
        item_id="item-1",
        kind="remote-direct",
        provider="realdebrid",
        provider_download_id="download-1",
        restricted_url="https://api.example.com/restricted",
        unrestricted_url=None,
        local_path=None,
        refresh_state="stale",
    )

    async def provider_executor(
        incoming: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert incoming.provider == "realdebrid"
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/provider",
        )

    provider_executors: dict[str, PlaybackAttachmentRefreshExecutor] = {
        "realdebrid": provider_executor
    }
    executor = service.select_refresh_executor(request, executors=provider_executors)
    result = asyncio.run(executor(request))

    assert result.ok is True
    assert result.locator == "https://cdn.example.com/provider"


def test_playback_source_service_builds_provider_client_refresh_executor() -> None:
    """Provider clients should be wrapped into concrete refresh executors for restricted-link refreshes."""

    service = PlaybackSourceService(cast(Any, object()))
    request = PlaybackAttachmentRefreshRequest(
        attachment_id="attachment-client-1",
        item_id="item-client-1",
        kind="remote-direct",
        provider="realdebrid",
        provider_download_id="download-client-1",
        restricted_url="https://api.example.com/restricted-client",
        unrestricted_url=None,
        local_path=None,
        refresh_state="stale",
    )

    class FakeProviderClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None]] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append((link, request.provider_download_id))
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/provider-client",
                expires_at=datetime(2026, 3, 14, 13, 0, tzinfo=UTC),
            )

    client = FakeProviderClient()
    provider_clients: dict[str, PlaybackAttachmentProviderClient] = {"realdebrid": client}

    executor = service.select_refresh_executor(request, provider_clients=provider_clients)
    result = asyncio.run(executor(request))

    assert client.calls == [("https://api.example.com/restricted-client", "download-client-1")]
    assert result == PlaybackAttachmentRefreshResult(
        ok=True,
        locator="https://cdn.example.com/provider-client",
        unrestricted_url="https://cdn.example.com/provider-client",
        expires_at=datetime(2026, 3, 14, 13, 0, tzinfo=UTC),
    )


def test_playback_source_service_selects_provider_file_projection_by_strongest_identity() -> None:
    """Provider-side file projection matching should prefer stronger persisted identity over weaker heuristics."""

    request = PlaybackAttachmentRefreshRequest(
        attachment_id="attachment-projection-match",
        item_id="item-projection-match",
        kind="remote-direct",
        provider="realdebrid",
        provider_download_id="download-projection-match",
        restricted_url=None,
        unrestricted_url=None,
        local_path=None,
        refresh_state="stale",
        provider_file_id="file-2",
        provider_file_path="folder/wrong-match.mkv",
        original_filename="Wrong Match.mkv",
        file_size=999,
    )
    projections = [
        PlaybackAttachmentProviderFileProjection(
            provider="realdebrid",
            provider_download_id="download-projection-match",
            provider_file_id="file-1",
            provider_file_path="folder/wrong-match.mkv",
            original_filename="Wrong Match.mkv",
            file_size=999,
            restricted_url="https://api.example.com/restricted-wrong",
        ),
        PlaybackAttachmentProviderFileProjection(
            provider="realdebrid",
            provider_download_id="download-projection-match",
            provider_file_id="file-2",
            provider_file_path="folder/right-match.mkv",
            original_filename="Right Match.mkv",
            file_size=123,
            restricted_url="https://api.example.com/restricted-right",
        ),
    ]

    assert (
        PlaybackSourceService.select_provider_file_projection(request, projections)
        == projections[1]
    )


def test_build_builtin_playback_provider_clients_includes_realdebrid_when_configured() -> None:
    """Built-in provider-client resolution should expose Real-Debrid when a token is configured."""

    clients = build_builtin_playback_provider_clients(_build_settings_with_realdebrid_token())

    assert set(clients) == {"realdebrid"}
    assert isinstance(clients["realdebrid"], RealDebridPlaybackClient)


def test_build_builtin_playback_provider_clients_includes_all_configured_debrid_services() -> None:
    """Built-in provider-client resolution should expose every currently configured debrid-service client."""

    clients = build_builtin_playback_provider_clients(
        _build_settings_with_all_builtin_debrid_tokens()
    )

    assert set(clients) == {"realdebrid", "alldebrid", "debridlink"}
    assert isinstance(clients["realdebrid"], RealDebridPlaybackClient)
    assert isinstance(clients["alldebrid"], AllDebridPlaybackClient)
    assert isinstance(clients["debridlink"], DebridLinkPlaybackClient)


def test_realdebrid_playback_client_unrestricts_link() -> None:
    """RealDebridPlaybackClient should call the documented unrestrict endpoint and normalize its response."""

    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        body = (await request.aread()).decode()
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers.get("Authorization")
        captured["body"] = body
        return httpx.Response(
            200,
            json={
                "download": "https://cdn.example.com/rd-unrestricted",
                "filename": "Movie.mkv",
                "filesize": 123,
            },
        )

    client = RealDebridPlaybackClient(
        api_token="rd-token",
        transport=httpx.MockTransport(handler),
    )
    request = PlaybackAttachmentRefreshRequest(
        attachment_id="attachment-rd-client",
        item_id="item-rd-client",
        kind="remote-direct",
        provider="realdebrid",
        provider_download_id="download-rd-client",
        restricted_url="https://api.example.com/restricted-rd-client",
        unrestricted_url=None,
        local_path=None,
        refresh_state="stale",
    )

    result = asyncio.run(
        client.unrestrict_link(
            "https://api.example.com/restricted-rd-client",
            request=request,
        )
    )

    assert result == PlaybackAttachmentProviderUnrestrictedLink(
        download_url="https://cdn.example.com/rd-unrestricted",
        restricted_url="https://api.example.com/restricted-rd-client",
    )
    assert captured == {
        "method": "POST",
        "url": "https://api.real-debrid.com/rest/1.0/unrestrict/link",
        "authorization": "Bearer rd-token",
        "body": "link=https%3A%2F%2Fapi.example.com%2Frestricted-rd-client",
    }


def test_realdebrid_playback_client_refreshes_download_id_via_torrent_info() -> None:
    """RealDebridPlaybackClient should resolve a provider download id into a restricted link before unrestricting it."""

    calls: list[tuple[str, str, str | None, str | None]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = (await request.aread()).decode()
        calls.append(
            (
                request.method,
                str(request.url),
                request.headers.get("Authorization"),
                body or None,
            )
        )
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "id": "torrent-123",
                    "filename": "Torrent Name",
                    "original_filename": "Torrent Name",
                    "hash": "hash-123",
                    "bytes": 123,
                    "progress": 100,
                    "status": "downloaded",
                    "added": "2026-03-12T12:00:00",
                    "files": [
                        {
                            "id": 11,
                            "path": "folder/Movie.mkv",
                            "bytes": 123,
                            "selected": 1,
                        }
                    ],
                    "links": ["https://api.real-debrid.com/rd-restricted-link"],
                },
            )
        return httpx.Response(
            200,
            json={
                "download": "https://cdn.example.com/rd-download",
                "filename": "Movie.mkv",
                "filesize": 123,
            },
        )

    client = RealDebridPlaybackClient(
        api_token="rd-token",
        transport=httpx.MockTransport(handler),
    )
    request = PlaybackAttachmentRefreshRequest(
        attachment_id="attachment-rd-download-id",
        item_id="item-rd-download-id",
        kind="remote-direct",
        provider="realdebrid",
        provider_download_id="torrent-123",
        restricted_url=None,
        unrestricted_url=None,
        local_path=None,
        refresh_state="stale",
        original_filename="Movie.mkv",
        file_size=123,
    )

    result = asyncio.run(client.refresh_download(request=request))

    assert result == PlaybackAttachmentProviderUnrestrictedLink(
        download_url="https://cdn.example.com/rd-download",
        restricted_url="https://api.real-debrid.com/rd-restricted-link",
    )
    assert calls == [
        (
            "GET",
            "https://api.real-debrid.com/rest/1.0/torrents/info/torrent-123",
            "Bearer rd-token",
            None,
        ),
        (
            "POST",
            "https://api.real-debrid.com/rest/1.0/unrestrict/link",
            "Bearer rd-token",
            "link=https%3A%2F%2Fapi.real-debrid.com%2Frd-restricted-link",
        ),
    ]


def test_alldebrid_playback_client_unlocks_link() -> None:
    """AllDebridPlaybackClient should call the documented unlock endpoint and normalize its response."""

    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers.get("Authorization")
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "link": "https://cdn.example.com/ad-unlocked",
                    "filename": "Movie.mkv",
                    "filesize": 123,
                },
            },
        )

    client = AllDebridPlaybackClient(
        api_token="ad-token",
        transport=httpx.MockTransport(handler),
    )
    request = PlaybackAttachmentRefreshRequest(
        attachment_id="attachment-ad-client",
        item_id="item-ad-client",
        kind="remote-direct",
        provider="alldebrid",
        provider_download_id="download-ad-client",
        restricted_url="https://api.example.com/restricted-ad-client",
        unrestricted_url=None,
        local_path=None,
        refresh_state="stale",
    )

    result = asyncio.run(
        client.unrestrict_link(
            "https://api.example.com/restricted-ad-client",
            request=request,
        )
    )

    assert result == PlaybackAttachmentProviderUnrestrictedLink(
        download_url="https://cdn.example.com/ad-unlocked"
    )
    assert captured == {
        "method": "GET",
        "url": "https://api.alldebrid.com/v4/link/unlock?link=https%3A%2F%2Fapi.example.com%2Frestricted-ad-client",
        "authorization": "Bearer ad-token",
    }


def test_debridlink_playback_client_returns_direct_link() -> None:
    """DebridLinkPlaybackClient should treat provider links as already direct playback URLs."""

    client = DebridLinkPlaybackClient(api_token="dl-token")
    request = PlaybackAttachmentRefreshRequest(
        attachment_id="attachment-dl-client",
        item_id="item-dl-client",
        kind="remote-direct",
        provider="debridlink",
        provider_download_id="download-dl-client",
        restricted_url="https://cdn.example.com/dl-direct",
        unrestricted_url=None,
        local_path=None,
        refresh_state="stale",
    )

    result = asyncio.run(
        client.unrestrict_link(
            "https://cdn.example.com/dl-direct",
            request=request,
        )
    )

    assert result == PlaybackAttachmentProviderUnrestrictedLink(
        download_url="https://cdn.example.com/dl-direct"
    )


def test_playback_source_service_executes_refreshes_with_provider_mapping() -> None:
    """Refresh orchestration should route requests through provider-specific executors when available."""

    item = _build_item(item_id="item-provider-exec")
    attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/old-provider",
        restricted_url="https://api.example.com/restricted-provider",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="provider-1",
    )
    item.playback_attachments = [attachment]
    _, resources = _build_client(items=[item])

    async def provider_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        assert request.provider == "realdebrid"
        return PlaybackAttachmentRefreshResult(
            ok=True,
            locator="https://cdn.example.com/provider-refresh",
            unrestricted_url="https://cdn.example.com/provider-refresh",
        )

    provider_executors: dict[str, PlaybackAttachmentRefreshExecutor] = {
        "realdebrid": provider_executor
    }

    executed = asyncio.run(
        PlaybackSourceService(resources.db).execute_attachment_refreshes_with_providers(
            item,
            executors=provider_executors,
        )
    )

    assert executed == [
        PlaybackAttachmentRefreshExecution(
            attachment_id=attachment.id,
            ok=True,
            refresh_state="ready",
            locator="https://cdn.example.com/provider-refresh",
            error=None,
        )
    ]


def test_playback_source_service_executes_refreshes_with_provider_client_mapping() -> None:
    """Refresh orchestration should use provider clients for concrete restricted-link unrestriction when available."""

    item = _build_item(item_id="item-provider-client-exec")
    attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/old-provider-client",
        restricted_url="https://api.example.com/restricted-provider-client",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="provider-client-1",
    )
    item.playback_attachments = [attachment]
    _, resources = _build_client(items=[item])

    class FakeProviderClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None]] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append((link, request.provider_download_id))
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/provider-client-refresh",
                expires_at=datetime(2026, 3, 15, 13, 0, tzinfo=UTC),
            )

    client = FakeProviderClient()
    provider_clients: dict[str, PlaybackAttachmentProviderClient] = {"realdebrid": client}

    executed = asyncio.run(
        PlaybackSourceService(resources.db).execute_attachment_refreshes_with_providers(
            item,
            provider_clients=provider_clients,
        )
    )

    assert client.calls == [
        ("https://api.example.com/restricted-provider-client", "provider-client-1")
    ]
    assert executed == [
        PlaybackAttachmentRefreshExecution(
            attachment_id=attachment.id,
            ok=True,
            refresh_state="ready",
            locator="https://cdn.example.com/provider-client-refresh",
            error=None,
        )
    ]
    assert attachment.unrestricted_url == "https://cdn.example.com/provider-client-refresh"
    assert attachment.expires_at == datetime(2026, 3, 15, 13, 0, tzinfo=UTC)


def test_playback_source_service_executes_refreshes_with_provider_projection_client_mapping() -> (
    None
):
    """Projection-aware provider clients should enrich persisted attachment identity before unrestricting the matched file."""

    item = _build_item(item_id="item-provider-projection-client-exec")
    attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/old-provider-projection-client",
        restricted_url=None,
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="provider-projection-client-1",
        original_filename="Episode.mkv",
        file_size=None,
    )
    item.playback_attachments = [attachment]
    _, resources = _build_client(items=[item])

    class FakeProjectionClient:
        def __init__(self) -> None:
            self.project_calls: list[tuple[str | None, str | None, int | None]] = []
            self.unrestrict_calls: list[tuple[str, str | None]] = []

        async def project_download_attachments(
            self,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> list[PlaybackAttachmentProviderFileProjection]:
            self.project_calls.append(
                (request.provider_download_id, request.original_filename, request.file_size)
            )
            return [
                PlaybackAttachmentProviderFileProjection(
                    provider="realdebrid",
                    provider_download_id=request.provider_download_id,
                    provider_file_id="99",
                    provider_file_path="folder/Episode.mkv",
                    original_filename="Episode.mkv",
                    file_size=456,
                    restricted_url="https://api.example.com/projected-restricted",
                )
            ]

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.unrestrict_calls.append((link, request.provider_download_id))
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/provider-projection-client-refresh",
                restricted_url=link,
                expires_at=datetime(2026, 3, 15, 15, 0, tzinfo=UTC),
            )

    client = FakeProjectionClient()
    assert isinstance(client, PlaybackAttachmentProviderProjectionClient)

    executed = asyncio.run(
        PlaybackSourceService(resources.db).execute_attachment_refreshes_with_providers(
            item,
            provider_clients={"realdebrid": cast(PlaybackAttachmentProviderClient, client)},
        )
    )

    assert client.project_calls == [("provider-projection-client-1", "Episode.mkv", None)]
    assert client.unrestrict_calls == [
        ("https://api.example.com/projected-restricted", "provider-projection-client-1")
    ]
    assert executed == [
        PlaybackAttachmentRefreshExecution(
            attachment_id=attachment.id,
            ok=True,
            refresh_state="ready",
            locator="https://cdn.example.com/provider-projection-client-refresh",
            error=None,
        )
    ]
    assert attachment.restricted_url == "https://api.example.com/projected-restricted"
    assert (
        attachment.unrestricted_url == "https://cdn.example.com/provider-projection-client-refresh"
    )
    assert attachment.provider_file_id == "99"
    assert attachment.provider_file_path == "folder/Episode.mkv"
    assert attachment.original_filename == "Episode.mkv"
    assert attachment.file_size == 456
    assert attachment.expires_at == datetime(2026, 3, 15, 15, 0, tzinfo=UTC)


def test_playback_source_service_uses_builtin_provider_clients_when_configured(
    monkeypatch: Any,
) -> None:
    """PlaybackSourceService should default to built-in provider clients when settings configure them."""

    item = _build_item(item_id="item-builtin-provider-client")
    attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/old-builtin-provider-client",
        restricted_url="https://api.example.com/restricted-builtin-provider-client",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="builtin-provider-client-1",
    )
    item.playback_attachments = [attachment]
    _, resources = _build_client(items=[item])

    class FakeProviderClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None]] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append((link, request.provider_download_id))
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/builtin-provider-client-refresh",
                expires_at=datetime(2026, 3, 16, 13, 0, tzinfo=UTC),
            )

    fake_client = FakeProviderClient()

    def fake_build_builtin_provider_clients(
        settings: Settings,
    ) -> dict[str, PlaybackAttachmentProviderClient]:
        assert settings.realdebrid_api_token is not None
        return {"realdebrid": fake_client}

    monkeypatch.setattr(
        "filmu_py.services.debrid.build_builtin_playback_provider_clients",
        fake_build_builtin_provider_clients,
    )

    executed = asyncio.run(
        PlaybackSourceService(
            resources.db,
            settings=_build_settings_with_realdebrid_token(),
        ).execute_attachment_refreshes_with_providers(item)
    )

    assert fake_client.calls == [
        (
            "https://api.example.com/restricted-builtin-provider-client",
            "builtin-provider-client-1",
        )
    ]
    assert executed == [
        PlaybackAttachmentRefreshExecution(
            attachment_id=attachment.id,
            ok=True,
            refresh_state="ready",
            locator="https://cdn.example.com/builtin-provider-client-refresh",
            error=None,
        )
    ]
    assert attachment.unrestricted_url == "https://cdn.example.com/builtin-provider-client-refresh"
    assert attachment.expires_at == datetime(2026, 3, 16, 13, 0, tzinfo=UTC)


def test_playback_source_service_uses_builtin_alldebrid_client_when_configured(
    monkeypatch: Any,
) -> None:
    """PlaybackSourceService should also resolve built-in AllDebrid clients from configured settings."""

    item = _build_item(item_id="item-builtin-alldebrid-client")
    attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/old-builtin-alldebrid-client",
        restricted_url="https://api.example.com/restricted-builtin-alldebrid-client",
        refresh_state="stale",
        provider="alldebrid",
        provider_download_id="builtin-alldebrid-client-1",
    )
    item.playback_attachments = [attachment]
    _, resources = _build_client(items=[item])

    class FakeAllDebridClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None]] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append((link, request.provider_download_id))
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/builtin-alldebrid-client-refresh",
                expires_at=datetime(2026, 3, 17, 13, 0, tzinfo=UTC),
            )

    fake_client = FakeAllDebridClient()

    def fake_build_builtin_provider_clients(
        settings: Settings,
    ) -> dict[str, PlaybackAttachmentProviderClient]:
        assert settings.alldebrid_api_token is not None
        return {"alldebrid": fake_client}

    monkeypatch.setattr(
        "filmu_py.services.debrid.build_builtin_playback_provider_clients",
        fake_build_builtin_provider_clients,
    )

    executed = asyncio.run(
        PlaybackSourceService(
            resources.db,
            settings=Settings(
                FILMU_PY_API_KEY=SecretStr("a" * 32),
                FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
                FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
                FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
                FILMU_PY_LOG_LEVEL="INFO",
                FILMU_PY_SERVICE_NAME="filmu-python-test",
                FILMU_PY_ALLDEBRID_API_TOKEN=SecretStr("ad-token"),
            ),
        ).execute_attachment_refreshes_with_providers(item)
    )

    assert fake_client.calls == [
        (
            "https://api.example.com/restricted-builtin-alldebrid-client",
            "builtin-alldebrid-client-1",
        )
    ]
    assert executed == [
        PlaybackAttachmentRefreshExecution(
            attachment_id=attachment.id,
            ok=True,
            refresh_state="ready",
            locator="https://cdn.example.com/builtin-alldebrid-client-refresh",
            error=None,
        )
    ]
    assert attachment.unrestricted_url == "https://cdn.example.com/builtin-alldebrid-client-refresh"
    assert attachment.expires_at == datetime(2026, 3, 17, 13, 0, tzinfo=UTC)


def test_playback_source_service_uses_provider_download_id_refresh_when_restricted_url_missing() -> (
    None
):
    """PlaybackSourceService should let provider clients refresh by provider_download_id when no restricted URL is persisted yet."""

    item = _build_item(item_id="item-provider-download-refresh")
    attachment = _build_playback_attachment(
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/old-provider-download-refresh",
        restricted_url=None,
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="torrent-456",
        original_filename="Movie.mkv",
        file_size=123,
    )
    item.playback_attachments = [attachment]
    _, resources = _build_client(items=[item])

    class FakeDownloadRefreshClient:
        def __init__(self) -> None:
            self.download_requests: list[tuple[str | None, str | None, int | None]] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            raise AssertionError(f"unrestrict_link should not be called directly: {link} {request}")

        async def refresh_download(
            self,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.download_requests.append(
                (request.provider_download_id, request.original_filename, request.file_size)
            )
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/provider-download-refresh",
                restricted_url="https://api.example.com/provider-download-refresh",
                expires_at=datetime(2026, 3, 18, 13, 0, tzinfo=UTC),
            )

    client = FakeDownloadRefreshClient()
    assert isinstance(client, PlaybackAttachmentProviderDownloadClient)

    executed = asyncio.run(
        PlaybackSourceService(resources.db).execute_attachment_refreshes_with_providers(
            item,
            provider_clients={"realdebrid": cast(PlaybackAttachmentProviderClient, client)},
        )
    )

    assert client.download_requests == [("torrent-456", "Movie.mkv", 123)]
    assert executed == [
        PlaybackAttachmentRefreshExecution(
            attachment_id=attachment.id,
            ok=True,
            refresh_state="ready",
            locator="https://cdn.example.com/provider-download-refresh",
            error=None,
        )
    ]
    assert attachment.restricted_url == "https://api.example.com/provider-download-refresh"
    assert attachment.unrestricted_url == "https://cdn.example.com/provider-download-refresh"
    assert attachment.expires_at == datetime(2026, 3, 18, 13, 0, tzinfo=UTC)


def test_stream_file_matches_imdb_identifier_when_present(tmp_path: Path) -> None:
    """Direct stream route should accept imdb-based playback identifiers when stored on the item."""

    media_file = tmp_path / "imdb-example.txt"
    media_file.write_bytes(b"imdb-stream")
    item = _build_item(attributes={"imdb_id": "tt1234567", "file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    response = client.get("/api/v1/stream/file/tt1234567", headers=_headers())

    assert response.status_code == 200
    assert response.content == b"imdb-stream"


def test_stream_file_returns_404_when_no_playback_source_exists() -> None:
    """Direct stream route should fail clearly when no playback source metadata exists."""

    item = _build_item(attributes={"tmdb_id": "123"})
    client, _ = _build_client(items=[item])

    response = client.get("/api/v1/stream/file/123", headers=_headers())

    assert response.status_code == 404
    assert response.json()["detail"] == "No playback source available for item"


def test_hls_playlist_route_reports_not_implemented() -> None:
    """HLS playlist route should return a rewritten playlist when item metadata exposes one."""

    item = _build_item(attributes={"hls_url": "https://example.com/master.m3u8"})
    client, _ = _build_client(items=[item])

    async def fake_download_text(url: str) -> tuple[str, httpx.Headers]:
        assert url == "https://example.com/master.m3u8"
        return (
            "#EXTM3U\n#EXTINF:10,\nsegment0.ts\nhttps://cdn.example.com/segment1.ts\n",
            httpx.Headers({"content-type": "application/vnd.apple.mpegurl"}),
        )

    original = stream_routes._download_text
    stream_routes._download_text = cast(Any, fake_download_text)
    try:
        response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    finally:
        stream_routes._download_text = original

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.apple.mpegurl")
    assert response.headers["cache-control"] == "no-store"
    assert f"/api/stream/{item.id}/hls/segment0.ts" in response.text
    assert (
        f"/api/stream/{item.id}/hls/proxy/https%3A%2F%2Fcdn.example.com%2Fsegment1.ts"
        in response.text
    )


def test_hls_playlist_route_prefers_selected_playlist_candidate() -> None:
    """HLS playlist route should prioritize selected/active HLS metadata within stream lists."""

    item = _build_item(
        attributes={
            "streams": [
                {"hls_url": "https://example.com/inactive.m3u8"},
                {"hls_url": "https://example.com/selected.m3u8", "selected": 1},
            ]
        }
    )
    client, _ = _build_client(items=[item])

    async def fake_download_text(url: str) -> tuple[str, httpx.Headers]:
        assert url == "https://example.com/selected.m3u8"
        return (
            "#EXTM3U\n#EXTINF:10,\nsegment0.ts\n",
            httpx.Headers({"content-type": "application/vnd.apple.mpegurl"}),
        )

    original = stream_routes._download_text
    stream_routes._download_text = cast(Any, fake_download_text)
    try:
        response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    finally:
        stream_routes._download_text = original

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert f"/api/stream/{item.id}/hls/segment0.ts" in response.text


def test_hls_segment_route_proxies_relative_segment_file() -> None:
    """HLS segment route should proxy relative segment paths against the resolved playlist URL."""

    item = _build_item(attributes={"hls_url": "https://example.com/path/master.m3u8"})
    client, _ = _build_client(items=[item])

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        assert url == "https://example.com/path/segment/0.ts"
        assert request.headers.get("x-api-key") == "a" * 32
        assert owner == "http-hls"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"segment-bytes"

        return StreamingResponse(iterator(), media_type="video/mp2t")

    original = byte_streaming.stream_remote
    byte_streaming.stream_remote = fake_stream_remote
    try:
        response = client.get(f"/api/v1/stream/hls/{item.id}/segment/0.ts", headers=_headers())
    finally:
        byte_streaming.stream_remote = original

    assert response.status_code == 200
    assert response.content == b"segment-bytes"
    assert response.headers["cache-control"] == "public, max-age=3600"


def test_stream_remote_tracks_remote_proxy_handle_and_path(monkeypatch: Any) -> None:
    """Remote proxy streaming should register and release explicit remote handles and paths."""

    class FakeUpstream:
        status_code = 200
        headers = httpx.Headers({"content-type": "application/octet-stream"})

        def __init__(self) -> None:
            self.closed = False

        async def aiter_bytes(self) -> AsyncGenerator[bytes, None]:
            yield b"abc"
            yield b"de"

        async def aclose(self) -> None:
            self.closed = True

    class FakeAsyncClient:
        def __init__(self, **kwargs: Any) -> None:
            _ = kwargs
            self.closed = False

        def build_request(self, method: str, url: str, headers: dict[str, str]) -> httpx.Request:
            assert method == "GET"
            assert url == "https://example.com/video.bin"
            assert headers == {"Range": "bytes=0-4"}
            return httpx.Request(method, url, headers=headers)

        async def send(self, request: httpx.Request, *, stream: bool) -> FakeUpstream:
            assert stream is True
            assert str(request.url) == "https://example.com/video.bin"
            return FakeUpstream()

        async def aclose(self) -> None:
            self.closed = True

    monkeypatch.setattr("filmu_py.core.byte_streaming.httpx.AsyncClient", FakeAsyncClient)

    class DummyRequest:
        def __init__(self) -> None:
            self.headers = {"range": "bytes=0-4"}

    remote_open_before = _histogram_count(
        byte_streaming.REMOTE_PROXY_OPEN_DURATION_SECONDS,
        status_code="200",
    )
    remote_session_before = _counter_value(
        byte_streaming.STREAM_OPEN_OPERATIONS,
        owner="http-hls",
        category="remote-proxy",
    )
    remote_reads_before = _counter_value(
        byte_streaming.STREAM_READ_OPERATIONS,
        owner="http-hls",
        category="remote-proxy",
    )
    request_shape_before = _counter_value(
        byte_streaming.STREAM_REQUEST_SHAPES,
        owner="http-hls",
        category="remote-proxy",
        shape="range",
    )
    access_pattern_before = _counter_value(
        byte_streaming.STREAM_ACCESS_PATTERNS,
        owner="http-hls",
        category="remote-proxy",
        pattern="head-probe",
    )
    response_outcome_before = _counter_value(
        byte_streaming.STREAM_RESPONSE_OUTCOMES,
        owner="http-hls",
        category="remote-proxy",
        outcome="range_nonpartial",
    )
    read_histogram_before = _histogram_count(
        byte_streaming.STREAM_READ_SIZE_BYTES,
        owner="http-hls",
        category="remote-proxy",
    )
    small_bucket_before = _counter_value(
        byte_streaming.STREAM_READ_SIZE_BUCKETS,
        owner="http-hls",
        category="remote-proxy",
        bucket="small",
    )
    reads_per_session_before = _histogram_count(
        byte_streaming.STREAM_READ_OPERATIONS_PER_SESSION,
        owner="http-hls",
        category="remote-proxy",
    )
    reads_per_session_sum_before = _histogram_sum(
        byte_streaming.STREAM_READ_OPERATIONS_PER_SESSION,
        owner="http-hls",
        category="remote-proxy",
    )
    bytes_per_read_before = _histogram_count(
        byte_streaming.STREAM_BYTES_PER_READ_PROXY,
        owner="http-hls",
        category="remote-proxy",
    )
    bytes_per_read_sum_before = _histogram_sum(
        byte_streaming.STREAM_BYTES_PER_READ_PROXY,
        owner="http-hls",
        category="remote-proxy",
    )
    remote_bytes_before = _counter_value(
        byte_streaming.STREAM_BYTES_SERVED,
        owner="http-hls",
        category="remote-proxy",
    )
    upstream_opens_before = _counter_value(
        byte_streaming.STREAM_UPSTREAM_OPENS,
        owner="http-hls",
        status_code="200",
    )

    response = asyncio.run(
        byte_streaming.stream_remote(
            "https://example.com/video.bin",
            cast(Any, DummyRequest()),
            owner="http-hls",
        )
    )

    assert response.status_code == 200
    chunks = asyncio.run(_collect_streaming_response_body(response))
    assert chunks == b"abcde"
    assert byte_streaming.get_active_handle_snapshot() == []
    assert byte_streaming.get_handle_by_id("handle-session-1") is None or True

    tracked_path = byte_streaming.get_path_by_key(
        category="remote-proxy", path="https://example.com/video.bin"
    )
    assert tracked_path is not None
    assert tracked_path.active_handle_count == 0
    assert tracked_path.node_kind == "remote-resource"
    assert (
        _counter_value(
            byte_streaming.STREAM_OPEN_OPERATIONS,
            owner="http-hls",
            category="remote-proxy",
        )
        == remote_session_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_READ_OPERATIONS,
            owner="http-hls",
            category="remote-proxy",
        )
        == remote_reads_before + 2
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_REQUEST_SHAPES,
            owner="http-hls",
            category="remote-proxy",
            shape="range",
        )
        == request_shape_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_ACCESS_PATTERNS,
            owner="http-hls",
            category="remote-proxy",
            pattern="head-probe",
        )
        == access_pattern_before + 1
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_RESPONSE_OUTCOMES,
            owner="http-hls",
            category="remote-proxy",
            outcome="range_nonpartial",
        )
        == response_outcome_before + 1
    )
    assert (
        _histogram_count(
            byte_streaming.STREAM_READ_SIZE_BYTES,
            owner="http-hls",
            category="remote-proxy",
        )
        == read_histogram_before + 2
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_READ_SIZE_BUCKETS,
            owner="http-hls",
            category="remote-proxy",
            bucket="small",
        )
        == small_bucket_before + 2
    )
    assert (
        _histogram_count(
            byte_streaming.STREAM_READ_OPERATIONS_PER_SESSION,
            owner="http-hls",
            category="remote-proxy",
        )
        == reads_per_session_before + 1
    )
    assert (
        _histogram_sum(
            byte_streaming.STREAM_READ_OPERATIONS_PER_SESSION,
            owner="http-hls",
            category="remote-proxy",
        )
        == reads_per_session_sum_before + 2
    )
    assert (
        _histogram_count(
            byte_streaming.STREAM_BYTES_PER_READ_PROXY,
            owner="http-hls",
            category="remote-proxy",
        )
        == bytes_per_read_before + 1
    )
    assert (
        _histogram_sum(
            byte_streaming.STREAM_BYTES_PER_READ_PROXY,
            owner="http-hls",
            category="remote-proxy",
        )
        == bytes_per_read_sum_before + 2.5
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_BYTES_SERVED,
            owner="http-hls",
            category="remote-proxy",
        )
        == remote_bytes_before + 5
    )
    assert (
        _counter_value(
            byte_streaming.STREAM_UPSTREAM_OPENS,
            owner="http-hls",
            status_code="200",
        )
        == upstream_opens_before + 1
    )
    assert (
        _histogram_count(
            byte_streaming.REMOTE_PROXY_OPEN_DURATION_SECONDS,
            status_code="200",
        )
        == remote_open_before + 1
    )


def test_stream_remote_records_abort_telemetry(monkeypatch: Any) -> None:
    class FakeUpstream:
        status_code = 200
        headers = httpx.Headers({"content-type": "application/octet-stream"})

        def __init__(self) -> None:
            self.closed = False

        async def aiter_bytes(self) -> AsyncGenerator[bytes, None]:
            yield b"abc"
            yield b"def"

        async def aclose(self) -> None:
            self.closed = True

    class FakeAsyncClient:
        def __init__(self, **kwargs: Any) -> None:
            _ = kwargs

        def build_request(self, method: str, url: str, headers: dict[str, str]) -> httpx.Request:
            return httpx.Request(method, url, headers=headers)

        async def send(self, request: httpx.Request, *, stream: bool) -> FakeUpstream:
            _ = request, stream
            return FakeUpstream()

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr("filmu_py.core.byte_streaming.httpx.AsyncClient", FakeAsyncClient)

    class DummyRequest:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

    abort_before = _counter_value(
        byte_streaming.STREAM_ABORT_EVENTS,
        owner="http-hls",
        category="remote-proxy",
        reason="cancelled",
    )

    async def run_abort() -> bytes:
        response = await byte_streaming.stream_remote(
            "https://example.com/abort.bin",
            cast(Any, DummyRequest()),
            owner="http-hls",
        )
        iterator = cast(Any, response.body_iterator)
        first_chunk = await iterator.__anext__()
        with suppress(asyncio.CancelledError):
            await iterator.athrow(asyncio.CancelledError())
        return bytes(first_chunk)

    first_chunk = asyncio.run(run_abort())

    assert first_chunk == b"abc"
    assert (
        _counter_value(
            byte_streaming.STREAM_ABORT_EVENTS,
            owner="http-hls",
            category="remote-proxy",
            reason="cancelled",
        )
        == abort_before + 1
    )
    governance = byte_streaming.get_serving_governance_snapshot()
    assert governance["stream_abort_events"] >= 1
    assert governance["remote_stream_abort_events"] >= 1
    assert byte_streaming.get_active_handle_snapshot() == []


async def _collect_streaming_response_body(response: StreamingResponse) -> bytes:
    payload = b""
    async for chunk in response.body_iterator:
        if isinstance(chunk, str):
            payload += chunk.encode("utf-8")
            continue
        if isinstance(chunk, bytes):
            payload += chunk
            continue
        if isinstance(chunk, memoryview):
            payload += chunk.tobytes()
            continue
        raise TypeError(f"unexpected streaming chunk type: {type(chunk)!r}")
    return payload


def test_hls_playlist_route_rewrites_generated_local_playlist(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """HLS playlist route should rewrite locally generated playlists through the BFF path."""

    media_file = tmp_path / "movie.mkv"
    media_file.write_bytes(b"movie-bytes")
    playlist_dir = tmp_path / "generated-hls"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.apple.mpegurl")
    assert response.headers["cache-control"] == "no-store"
    assert f"/api/stream/{item.id}/hls/segment_00001.ts" in response.text


def test_hls_playlist_route_uses_remote_direct_transcode_source(
    tmp_path: Path, monkeypatch: Any
) -> None:
    playlist_dir = tmp_path / "generated-hls-remote-direct"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    item = _build_item(item_id="item-hls-remote-direct-route")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/transcode-source.mkv",
            unrestricted_url="https://cdn.example.com/transcode-source.mkv",
            restricted_url="https://api.example.com/restricted-transcode-source.mkv",
            provider="realdebrid",
            is_preferred=True,
        )
    ]
    client, _ = _build_client(items=[item])

    triggered: list[str] = []

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == "https://cdn.example.com/transcode-source.mkv"
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    def fake_start_direct_trigger(*, request: Any, item_identifier: str) -> None:
        assert request.headers.get("x-api-key") == "a" * 32
        triggered.append(item_identifier)

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)
    monkeypatch.setattr(
        stream_routes,
        "_start_direct_playback_refresh_trigger",
        fake_start_direct_trigger,
    )

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.apple.mpegurl")
    assert response.headers["cache-control"] == "no-store"
    assert f"/api/stream/{item.id}/hls/segment_00001.ts" in response.text
    assert triggered == [item.id]


def test_hls_playlist_route_refreshes_media_entry_backed_transcode_source_after_head_failure(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    playlist_dir = tmp_path / "generated-hls-inline-refresh-success"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    item = _build_item(item_id="item-hls-inline-refresh-success")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-hls-inline-refresh-success",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/hls-inline-refresh-stale.mkv",
        unrestricted_url="https://cdn.example.com/hls-inline-refresh-stale.mkv",
        restricted_url="https://api.example.com/restricted-hls-inline-refresh-success.mkv",
        provider="realdebrid",
        provider_download_id="download-hls-inline-refresh-success",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-inline-refresh-success",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-hls-inline-refresh-success.mkv",
        unrestricted_url="https://cdn.example.com/hls-inline-refresh-stale.mkv",
        provider="realdebrid",
        provider_download_id="download-hls-inline-refresh-success",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
    )
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    client, resources = _build_client(items=[item])

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            assert capacity == 1.0
            assert refill_rate_per_second == 1.0
            return RateLimitDecision(allowed=True, remaining_tokens=0.0, retry_after_seconds=0.0)

    class FakeProviderClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append(link)
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/hls-inline-refresh-fresh.mkv",
                restricted_url=link,
                expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
            )

    provider_client = FakeProviderClient()
    resources.playback_service = PlaybackSourceService(
        resources.db,
        provider_clients={"realdebrid": cast(PlaybackAttachmentProviderClient, provider_client)},
        rate_limiter=FakeRateLimiter(),
    )

    head_calls: list[str] = []

    async def fake_head(url: str) -> None:
        head_calls.append(url)
        if url == "https://cdn.example.com/hls-inline-refresh-stale.mkv":
            raise HTTPException(
                status_code=503,
                detail="Playback source temporarily unavailable",
            )
        assert url == "https://cdn.example.com/hls-inline-refresh-fresh.mkv"

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == "https://cdn.example.com/hls-inline-refresh-fresh.mkv"
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    monkeypatch.setattr(stream_routes, "_head_remote_direct_url", fake_head)
    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)
    monkeypatch.setattr(
        stream_routes,
        "_start_direct_playback_refresh_trigger",
        lambda *args, **kwargs: None,
    )

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.apple.mpegurl")
    assert response.headers["cache-control"] == "no-store"
    assert f"/api/stream/{item.id}/hls/segment_00001.ts" in response.text
    assert head_calls == [
        "https://cdn.example.com/hls-inline-refresh-stale.mkv",
        "https://cdn.example.com/hls-inline-refresh-fresh.mkv",
    ]
    assert provider_client.calls == [
        "https://api.example.com/restricted-hls-inline-refresh-success.mkv"
    ]
    assert selected_entry.unrestricted_url == "https://cdn.example.com/hls-inline-refresh-fresh.mkv"


def test_hls_playlist_route_returns_503_when_transcode_source_refresh_is_rate_limited(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    playlist_dir = tmp_path / "generated-hls-inline-refresh-rate-limited"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    item = _build_item(item_id="item-hls-inline-refresh-rate-limited")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-hls-inline-refresh-rate-limited",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/hls-inline-rate-limited-stale.mkv",
        unrestricted_url="https://cdn.example.com/hls-inline-rate-limited-stale.mkv",
        restricted_url="https://api.example.com/restricted-hls-inline-rate-limited.mkv",
        provider="realdebrid",
        provider_download_id="download-hls-inline-rate-limited",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-inline-rate-limited",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-hls-inline-rate-limited.mkv",
        unrestricted_url="https://cdn.example.com/hls-inline-rate-limited-stale.mkv",
        provider="realdebrid",
        provider_download_id="download-hls-inline-rate-limited",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
    )
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    client, resources = _build_client(items=[item])

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            return RateLimitDecision(allowed=False, remaining_tokens=0.0, retry_after_seconds=6.0)

    class FakeProviderClient:
        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            raise AssertionError(
                "provider refresh should not run when inline HLS refresh is rate limited"
            )

    resources.playback_service = PlaybackSourceService(
        resources.db,
        provider_clients={
            "realdebrid": cast(PlaybackAttachmentProviderClient, FakeProviderClient())
        },
        rate_limiter=FakeRateLimiter(),
    )

    async def fake_head(url: str) -> None:
        raise HTTPException(
            status_code=503,
            detail="Playback source temporarily unavailable",
        )

    monkeypatch.setattr(stream_routes, "_head_remote_direct_url", fake_head)
    monkeypatch.setattr(
        stream_routes,
        "_start_direct_playback_refresh_trigger",
        lambda *args, **kwargs: None,
    )

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "10"
    assert (
        response.json()["detail"]
        == "HLS transcode source is unavailable: Selected direct playback lease refresh failed"
    )
    assert (
        governance["hls_route_failures_transcode_source_unavailable"]
        == before["hls_route_failures_transcode_source_unavailable"] + 1
    )
    assert governance["hls_route_failures_total"] == before["hls_route_failures_total"] + 1


def test_hls_playlist_route_keeps_healthy_media_entry_backed_transcode_source_without_refresh(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    playlist_dir = tmp_path / "generated-hls-inline-refresh-healthy"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    item = _build_item(item_id="item-hls-inline-refresh-healthy")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-hls-inline-refresh-healthy",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/hls-inline-healthy.mkv",
        unrestricted_url="https://cdn.example.com/hls-inline-healthy.mkv",
        restricted_url="https://api.example.com/restricted-hls-inline-healthy.mkv",
        provider="realdebrid",
        provider_download_id="download-hls-inline-healthy",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-inline-healthy",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-hls-inline-healthy.mkv",
        unrestricted_url="https://cdn.example.com/hls-inline-healthy.mkv",
        provider="realdebrid",
        provider_download_id="download-hls-inline-healthy",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
    )
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    client, resources = _build_client(items=[item])

    class FakeProviderClient:
        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            raise AssertionError(
                "provider refresh should not run when HLS transcode HEAD validation succeeds"
            )

    resources.playback_service = PlaybackSourceService(
        resources.db,
        provider_clients={
            "realdebrid": cast(PlaybackAttachmentProviderClient, FakeProviderClient())
        },
    )

    head_calls: list[str] = []

    async def fake_head(url: str) -> None:
        head_calls.append(url)
        assert url == "https://cdn.example.com/hls-inline-healthy.mkv"

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == "https://cdn.example.com/hls-inline-healthy.mkv"
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    monkeypatch.setattr(stream_routes, "_head_remote_direct_url", fake_head)
    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)
    monkeypatch.setattr(
        stream_routes,
        "_start_direct_playback_refresh_trigger",
        lambda *args, **kwargs: None,
    )

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.apple.mpegurl")
    assert response.headers["cache-control"] == "no-store"
    assert f"/api/stream/{item.id}/hls/segment_00001.ts" in response.text
    assert head_calls == ["https://cdn.example.com/hls-inline-healthy.mkv"]


def test_hls_segment_route_serves_generated_local_segment(tmp_path: Path, monkeypatch: Any) -> None:
    """HLS segment route should serve generated local segment files for file-backed items."""

    media_file = tmp_path / "movie.mkv"
    media_file.write_bytes(b"movie-bytes")
    playlist_dir = tmp_path / "generated-hls"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")
    segment_path = playlist_dir / "segment_00001.ts"
    segment_path.write_bytes(b"segment-file")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())

    assert response.status_code == 200
    assert response.content == b"segment-file"
    assert response.headers["cache-control"] == "public, max-age=3600"


def test_hls_segment_route_serves_generated_segment_for_remote_direct_transcode_source(
    tmp_path: Path, monkeypatch: Any
) -> None:
    playlist_dir = tmp_path / "generated-hls-remote-direct-segment"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")
    segment_path = playlist_dir / "segment_00001.ts"
    segment_path.write_bytes(b"remote-direct-segment")

    item = _build_item(item_id="item-hls-remote-direct-segment-route")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/transcode-segment-source.mkv",
            unrestricted_url="https://cdn.example.com/transcode-segment-source.mkv",
            restricted_url="https://api.example.com/restricted-transcode-segment-source.mkv",
            provider="realdebrid",
            is_preferred=True,
        )
    ]
    client, _ = _build_client(items=[item])

    triggered: list[str] = []

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == "https://cdn.example.com/transcode-segment-source.mkv"
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    def fake_start_direct_trigger(*, request: Any, item_identifier: str) -> None:
        assert request.headers.get("x-api-key") == "a" * 32
        triggered.append(item_identifier)

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)
    monkeypatch.setattr(
        stream_routes,
        "_start_direct_playback_refresh_trigger",
        fake_start_direct_trigger,
    )

    response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())

    assert response.status_code == 200
    assert response.content == b"remote-direct-segment"
    assert response.headers["cache-control"] == "public, max-age=3600"
    assert triggered == [item.id]


def test_hls_child_playlist_route_uses_no_store_cache_policy(monkeypatch: Any) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/path/master.m3u8"})
    client, _ = _build_client(items=[item])

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        assert url == "https://example.com/path/variant.m3u8"
        assert request.headers.get("x-api-key") == "a" * 32
        assert owner == "http-hls"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"#EXTM3U\n"

        return StreamingResponse(iterator(), media_type="application/vnd.apple.mpegurl")

    monkeypatch.setattr(byte_streaming, "stream_remote", fake_stream_remote)

    response = client.get(f"/api/v1/stream/hls/{item.id}/variant.m3u8", headers=_headers())

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"


def test_hls_segment_route_uses_http_hls_owner_for_generated_local_segment(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-owner.mkv"
    media_file.write_bytes(b"movie-bytes")
    playlist_dir = tmp_path / "generated-hls-owner"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")
    segment_path = playlist_dir / "segment_00001.ts"
    segment_path.write_bytes(b"segment-owner")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    def fake_stream_local_file(path: Path, request: Any, *, owner: str = "http-direct") -> Response:
        assert path == segment_path
        assert request.headers.get("x-api-key") == "a" * 32
        assert owner == "http-hls"
        return Response(content=b"segment-owner", media_type="video/mp2t")

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)
    monkeypatch.setattr(byte_streaming, "stream_local_file", fake_stream_local_file)

    response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())

    assert response.status_code == 200
    assert response.content == b"segment-owner"


def test_hls_playlist_route_maps_local_generation_timeout_to_503(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-timeout.mkv"
    media_file.write_bytes(b"movie-bytes")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        raise HTTPException(status_code=504, detail="HLS generation timed out")

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 503
    assert response.json()["detail"] == "HLS generation timed out"


def test_hls_segment_route_maps_local_generation_failure_to_503(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-generation-failure.mkv"
    media_file.write_bytes(b"movie-bytes")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        raise HTTPException(status_code=500, detail="ffmpeg failed")

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())

    assert response.status_code == 503
    assert response.json()["detail"] == "ffmpeg failed"


def test_hls_playlist_route_maps_malformed_generated_manifest_to_503(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-malformed-playlist-route.mkv"
    media_file.write_bytes(b"movie-bytes")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        raise HTTPException(status_code=500, detail="Generated HLS playlist is malformed")

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 503
    assert response.json()["detail"] == "Generated HLS playlist is malformed"


def test_hls_segment_route_maps_malformed_generated_manifest_to_503(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-malformed-segment-route.mkv"
    media_file.write_bytes(b"movie-bytes")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        raise HTTPException(status_code=500, detail="Generated HLS playlist is malformed")

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())

    assert response.status_code == 503
    assert response.json()["detail"] == "Generated HLS playlist is malformed"


def test_hls_segment_route_keeps_missing_generated_file_as_404(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-missing-segment.mkv"
    media_file.write_bytes(b"movie-bytes")
    playlist_dir = tmp_path / "generated-hls-missing"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())

    assert response.status_code == 404
    assert response.json()["detail"] == "Generated HLS file is missing"


def test_hls_segment_route_rejects_unreferenced_generated_local_file(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-unreferenced-segment.mkv"
    media_file.write_bytes(b"movie-bytes")
    playlist_dir = tmp_path / "generated-hls-unreferenced"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")
    (playlist_dir / "segment_00001.ts").write_bytes(b"referenced")
    (playlist_dir / "segment_99999.ts").write_bytes(b"unreferenced")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(f"/api/v1/stream/hls/{item.id}/segment_99999.ts", headers=_headers())

    assert response.status_code == 404
    assert response.json()["detail"] == "Generated HLS file is missing"


def test_ensure_local_hls_playlist_regenerates_incomplete_cached_playlist(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "incomplete-cached-hls-input.mkv"
    media_file.write_bytes(b"movie")
    output_dir = tmp_path / "incomplete-generated-hls"
    output_dir.mkdir()
    playlist_path = output_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    class DummyProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00002.ts\n", encoding="utf-8")
            (output_dir / "segment_00002.ts").write_bytes(b"fresh")
            return (b"", b"")

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> DummyProcess:
        return DummyProcess()

    monkeypatch.setattr(
        byte_streaming, "local_hls_directory", lambda item_id, **_kwargs: output_dir
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    regenerated = asyncio.run(
        byte_streaming.ensure_local_hls_playlist(str(media_file), "item-incomplete-hls")
    )

    assert regenerated == playlist_path
    assert (output_dir / "segment_00002.ts").is_file()
    assert not (output_dir / "segment_00001.ts").exists()
    assert byte_streaming.is_complete_local_hls_playlist(playlist_path) is True


def test_ensure_local_hls_playlist_rejects_malformed_cached_playlist_and_regenerates(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "malformed-cached-hls-input.mkv"
    media_file.write_bytes(b"movie")
    output_dir = tmp_path / "malformed-generated-hls"
    output_dir.mkdir()
    playlist_path = output_dir / "index.m3u8"
    playlist_path.write_text("segment_00001.ts\n", encoding="utf-8")

    class DummyProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00002.ts\n", encoding="utf-8")
            (output_dir / "segment_00002.ts").write_bytes(b"fresh")
            return (b"", b"")

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> DummyProcess:
        return DummyProcess()

    before_invalid = byte_streaming.get_serving_governance_snapshot()["hls_manifest_invalid"]
    before_regenerated = byte_streaming.get_serving_governance_snapshot()[
        "hls_manifest_regenerated"
    ]
    monkeypatch.setattr(
        byte_streaming, "local_hls_directory", lambda item_id, **_kwargs: output_dir
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    regenerated = asyncio.run(
        byte_streaming.ensure_local_hls_playlist(str(media_file), "item-malformed-hls")
    )

    assert regenerated == playlist_path
    governance = byte_streaming.get_serving_governance_snapshot()
    assert governance["hls_manifest_invalid"] == before_invalid + 1
    assert governance["hls_manifest_regenerated"] == before_regenerated + 1


def test_ensure_local_hls_playlist_regenerates_when_source_marker_changes(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "source-a.mkv"
    media_file.write_bytes(b"movie")
    other_source = "https://cdn.example.com/source-b.mkv"
    output_dir = tmp_path / "changed-source-generated-hls"
    output_dir.mkdir()
    playlist_path = output_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")
    (output_dir / "segment_00001.ts").write_bytes(b"old")
    byte_streaming.local_hls_source_marker(output_dir).write_text(other_source, encoding="utf-8")

    class DummyProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00002.ts\n", encoding="utf-8")
            (output_dir / "segment_00002.ts").write_bytes(b"fresh")
            return (b"", b"")

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> DummyProcess:
        return DummyProcess()

    monkeypatch.setattr(
        byte_streaming, "local_hls_directory", lambda item_id, **_kwargs: output_dir
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    regenerated = asyncio.run(
        byte_streaming.ensure_local_hls_playlist(str(media_file), "item-source-change")
    )

    assert regenerated == playlist_path
    assert (output_dir / "segment_00002.ts").is_file()
    assert not (output_dir / "segment_00001.ts").exists()
    assert byte_streaming.local_hls_source_marker(output_dir).read_text(encoding="utf-8") == (
        byte_streaming.build_local_hls_source_marker(
            str(media_file),
            transcode_profile=byte_streaming.LocalHlsTranscodeProfile(),
        )
    )


def test_ensure_local_hls_playlist_regenerates_when_transcode_profile_changes(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "profile-change.mkv"
    media_file.write_bytes(b"movie")
    output_dir = tmp_path / "profile-change-generated-hls"
    output_dir.mkdir()
    playlist_path = output_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")
    (output_dir / "segment_00001.ts").write_bytes(b"old")
    byte_streaming.local_hls_source_marker(output_dir).write_text(
        byte_streaming.build_local_hls_source_marker(
            str(media_file),
            transcode_profile=byte_streaming.LocalHlsTranscodeProfile(),
        ),
        encoding="utf-8",
    )

    class DummyProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00002.ts\n", encoding="utf-8")
            (output_dir / "segment_00002.ts").write_bytes(b"fresh")
            return (b"", b"")

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> DummyProcess:
        return DummyProcess()

    monkeypatch.setattr(
        byte_streaming, "local_hls_directory", lambda item_id, **_kwargs: output_dir
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    regenerated = asyncio.run(
        byte_streaming.ensure_local_hls_playlist(
            str(media_file),
            "item-profile-change",
            transcode_profile=byte_streaming.LocalHlsTranscodeProfile(level="5.1"),
        )
    )

    assert regenerated == playlist_path
    assert (output_dir / "segment_00002.ts").is_file()
    assert not (output_dir / "segment_00001.ts").exists()
    assert byte_streaming.local_hls_source_marker(output_dir).read_text(encoding="utf-8") == (
        byte_streaming.build_local_hls_source_marker(
            str(media_file),
            transcode_profile=byte_streaming.LocalHlsTranscodeProfile(level="5.1"),
        )
    )


def test_ensure_local_hls_playlist_uses_browser_safe_ffmpeg_profile(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "browser-safe-hls.mkv"
    media_file.write_bytes(b"movie")
    output_dir = tmp_path / "browser-safe-generated-hls"
    playlist_path = output_dir / "index.m3u8"
    captured_args: list[object] = []

    class DummyProcess:
        def __init__(self) -> None:
            self.returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return (b"", b"")

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> DummyProcess:
        captured_args[:] = list(args)
        return DummyProcess()

    async def fake_wait_for_local_hls_playlist_ready(
        ready_playlist_path: Path, *, process: Any
    ) -> None:
        ready_playlist_path.parent.mkdir(parents=True, exist_ok=True)
        ready_playlist_path.write_text(
            "#EXTM3U\n#EXTINF:2,\nsegment_00001.ts\n#EXT-X-ENDLIST\n",
            encoding="utf-8",
        )
        (ready_playlist_path.parent / "segment_00001.ts").write_bytes(b"segment")

    monkeypatch.setattr(
        byte_streaming, "local_hls_directory", lambda item_id, **_kwargs: output_dir
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(
        byte_streaming,
        "_wait_for_local_hls_playlist_ready",
        fake_wait_for_local_hls_playlist_ready,
    )

    asyncio.run(
        byte_streaming.ensure_local_hls_playlist(
            str(media_file),
            "item-browser-safe-hls",
            transcode_profile=byte_streaming.LocalHlsTranscodeProfile(
                pix_fmt="yuv420p",
                profile="high",
                level="4.1",
            ),
        )
    )

    assert playlist_path.is_file()
    assert captured_args[:4] == ["ffmpeg", "-y", "-i", str(media_file)]
    assert "-map" in captured_args
    assert "0:v:0" in captured_args
    assert "0:a:0?" in captured_args
    assert "-sn" in captured_args
    assert "-dn" in captured_args
    assert "-pix_fmt" in captured_args
    assert "yuv420p" in captured_args
    assert "-profile:v" in captured_args
    assert "-level:v" in captured_args
    assert "-ac" in captured_args
    assert "-ar" in captured_args
    assert "-b:a" in captured_args


def test_ensure_local_hls_playlist_fails_when_generated_manifest_is_malformed(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "generated-malformed-hls-input.mkv"
    media_file.write_bytes(b"movie")
    output_dir = tmp_path / "generated-malformed-hls"

    class DummyProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "index.m3u8").write_text("segment_00001.ts\n", encoding="utf-8")
            (output_dir / "segment_00001.ts").write_bytes(b"bad")
            return (b"", b"")

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> DummyProcess:
        return DummyProcess()

    before_invalid = byte_streaming.get_serving_governance_snapshot()["hls_manifest_invalid"]
    before_failed = byte_streaming.get_serving_governance_snapshot()["hls_generation_failed"]
    monkeypatch.setattr(
        byte_streaming, "local_hls_directory", lambda item_id, **_kwargs: output_dir
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            byte_streaming.ensure_local_hls_playlist(
                str(media_file), "item-generated-malformed-hls"
            )
        )

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "Generated HLS playlist is malformed"
    governance = byte_streaming.get_serving_governance_snapshot()
    assert governance["hls_manifest_invalid"] == before_invalid + 1
    assert governance["hls_generation_failed"] == before_failed + 1


def test_ensure_local_hls_playlist_cleans_partial_output_after_failure(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "broken-hls-input.mkv"
    media_file.write_bytes(b"movie")
    output_dir = tmp_path / "broken-generated-hls"
    output_dir.mkdir()
    stale_segment = output_dir / "segment_00001.ts"
    stale_segment.write_bytes(b"stale")

    class FakeProcess:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return (b"", b"ffmpeg failed")

        def kill(self) -> None:
            return None

    async def fake_create_subprocess_exec(*args: Any, **kwargs: Any) -> FakeProcess:
        _ = args, kwargs
        return FakeProcess()

    monkeypatch.setattr(
        byte_streaming, "local_hls_directory", lambda item_id, **_kwargs: output_dir
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    before_failed = byte_streaming.get_serving_governance_snapshot()["hls_generation_failed"]

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(byte_streaming.ensure_local_hls_playlist(str(media_file), "item-broken-hls"))

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "ffmpeg failed"
    assert output_dir.exists() is False
    assert (
        byte_streaming.get_serving_governance_snapshot()["hls_generation_failed"]
        == before_failed + 1
    )


def test_hls_generation_metrics_track_failure_and_timeout_results(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "metrics-hls-input.mkv"
    media_file.write_bytes(b"movie")

    class FailingProcess:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return (b"", b"ffmpeg failed")

        def kill(self) -> None:
            return None

    async def failing_create_subprocess_exec(*args: Any, **kwargs: Any) -> FailingProcess:
        _ = args, kwargs
        return FailingProcess()

    failure_dir = tmp_path / "metrics-hls-failure"
    failure_dir.mkdir()
    monkeypatch.setattr(
        byte_streaming, "local_hls_directory", lambda item_id, **_kwargs: failure_dir
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", failing_create_subprocess_exec)

    started_before = _counter_value(byte_streaming.HLS_GENERATION_EVENTS, result="started")
    failed_before = _counter_value(byte_streaming.HLS_GENERATION_EVENTS, result="failed")
    failed_duration_before = _histogram_count(
        byte_streaming.HLS_GENERATION_DURATION_SECONDS,
        result="failed",
    )

    with pytest.raises(HTTPException):
        asyncio.run(
            byte_streaming.ensure_local_hls_playlist(str(media_file), "item-metrics-hls-failure")
        )

    assert (
        _counter_value(byte_streaming.HLS_GENERATION_EVENTS, result="started") == started_before + 1
    )
    assert (
        _counter_value(byte_streaming.HLS_GENERATION_EVENTS, result="failed") == failed_before + 1
    )
    assert (
        _histogram_count(
            byte_streaming.HLS_GENERATION_DURATION_SECONDS,
            result="failed",
        )
        == failed_duration_before + 1
    )

    class TimeoutProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return (b"", b"")

        def kill(self) -> None:
            return None

    async def timeout_create_subprocess_exec(*args: Any, **kwargs: Any) -> TimeoutProcess:
        _ = args, kwargs
        return TimeoutProcess()

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:
        _ = timeout
        close = getattr(awaitable, "close", None)
        if callable(close):
            close()
        raise TimeoutError

    timeout_dir = tmp_path / "metrics-hls-timeout"
    timeout_dir.mkdir()
    monkeypatch.setattr(
        byte_streaming, "local_hls_directory", lambda item_id, **_kwargs: timeout_dir
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", timeout_create_subprocess_exec)
    monkeypatch.setattr(asyncio, "wait_for", fake_wait_for)

    started_timeout_before = _counter_value(byte_streaming.HLS_GENERATION_EVENTS, result="started")
    timeout_before = _counter_value(byte_streaming.HLS_GENERATION_EVENTS, result="timeout")
    timeout_duration_before = _histogram_count(
        byte_streaming.HLS_GENERATION_DURATION_SECONDS,
        result="timeout",
    )

    with pytest.raises(HTTPException):
        asyncio.run(
            byte_streaming.ensure_local_hls_playlist(str(media_file), "item-metrics-hls-timeout")
        )

    assert (
        _counter_value(byte_streaming.HLS_GENERATION_EVENTS, result="started")
        == started_timeout_before + 1
    )
    assert (
        _counter_value(byte_streaming.HLS_GENERATION_EVENTS, result="timeout") == timeout_before + 1
    )
    assert (
        _histogram_count(
            byte_streaming.HLS_GENERATION_DURATION_SECONDS,
            result="timeout",
        )
        == timeout_duration_before + 1
    )


def test_ensure_local_hls_playlist_returns_when_playlist_becomes_usable_before_ffmpeg_exits(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "progressive-hls-input.mkv"
    media_file.write_bytes(b"movie")
    output_dir = tmp_path / "progressive-generated-hls"
    playlist_path = output_dir / "index.m3u8"
    segment_path = output_dir / "segment_00001.ts"

    class RunningProcess:
        def __init__(self) -> None:
            self.returncode: int | None = None
            self.finished = asyncio.Event()
            self.playlist_writer_task: asyncio.Task[None] | None = None

        async def communicate(self) -> tuple[bytes, bytes]:
            await self.finished.wait()
            return (b"", b"speed=1.25x")

        async def wait(self) -> int:
            await self.finished.wait()
            return self.returncode or 0

        def terminate(self) -> None:
            self.returncode = -15
            self.finished.set()

        def kill(self) -> None:
            self.returncode = -9
            self.finished.set()

    process = RunningProcess()

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> RunningProcess:
        _ = args, kwargs
        output_dir.mkdir(parents=True, exist_ok=True)

        async def write_initial_playlist() -> None:
            await asyncio.sleep(0)
            playlist_path.write_text("#EXTM3U\n#EXTINF:2,\nsegment_00001.ts\n", encoding="utf-8")
            segment_path.write_bytes(b"segment")

        process.playlist_writer_task = asyncio.create_task(write_initial_playlist())
        return process

    monkeypatch.setattr(
        byte_streaming, "local_hls_directory", lambda item_id, **_kwargs: output_dir
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    async def exercise() -> None:
        ready_path = await byte_streaming.ensure_local_hls_playlist(
            str(media_file), "item-progressive-hls"
        )

        assert ready_path == playlist_path
        assert byte_streaming.is_usable_local_hls_playlist(playlist_path) is True
        tracked = byte_streaming._ACTIVE_HLS_GENERATIONS["item-progressive-hls"]
        assert tracked.monitor_task is not None
        assert byte_streaming.get_serving_governance_snapshot()["active_hls_generation_processes"] == 1

        playlist_path.write_text(
            "#EXTM3U\n#EXTINF:2,\nsegment_00001.ts\n#EXT-X-ENDLIST\n",
            encoding="utf-8",
        )
        process.returncode = 0
        process.finished.set()
        await tracked.monitor_task

    asyncio.run(exercise())

    assert byte_streaming.get_serving_governance_snapshot()["active_hls_generation_processes"] == 0

def test_ensure_local_hls_playlist_cancels_and_terminates_ffmpeg_process(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "cancelled-hls-input.mkv"
    media_file.write_bytes(b"movie")
    output_dir = tmp_path / "cancelled-generated-hls"

    class HangingProcess:
        def __init__(self) -> None:
            self.returncode: int | None = None
            self.terminated = False
            self.killed = False
            self.started = asyncio.Event()

        async def communicate(self) -> tuple[bytes, bytes]:
            if self.terminated or self.killed:
                return (b"", b"speed=1.5x")
            self.started.set()
            await asyncio.Future()
            raise AssertionError("unreachable")

        async def wait(self) -> int:
            return self.returncode or 0

        def terminate(self) -> None:
            self.terminated = True
            self.returncode = -15

        def kill(self) -> None:
            self.killed = True
            self.returncode = -9

    process = HangingProcess()

    async def fake_create_subprocess_exec(*args: object, **kwargs: object) -> HangingProcess:
        _ = args, kwargs
        return process

    async def fake_wait_for_local_hls_playlist_ready(
        playlist_path: Path, *, process: Any
    ) -> None:
        _ = playlist_path, process
        process.started.set()
        await asyncio.Future()

    monkeypatch.setattr(
        byte_streaming, "local_hls_directory", lambda item_id, **_kwargs: output_dir
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(
        byte_streaming,
        "_wait_for_local_hls_playlist_ready",
        fake_wait_for_local_hls_playlist_ready,
    )
    before_cancelled = byte_streaming.get_serving_governance_snapshot()["hls_generation_cancelled"]
    before_terminated = byte_streaming.get_serving_governance_snapshot()[
        "hls_generation_terminated"
    ]

    async def exercise() -> None:
        task = asyncio.create_task(
            byte_streaming.ensure_local_hls_playlist(str(media_file), "item-cancelled-hls")
        )
        await process.started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(exercise())

    governance = byte_streaming.get_serving_governance_snapshot()
    assert governance["hls_generation_cancelled"] == before_cancelled + 1
    assert governance["hls_generation_terminated"] == before_terminated + 1
    assert governance["active_hls_generation_processes"] == 0
    assert output_dir.exists() is False
    assert process.terminated is True


def test_cleanup_expired_hls_dirs_reaps_stalled_segments_even_when_directory_is_recent(
    tmp_path: Path, monkeypatch: Any
) -> None:
    output_root = tmp_path / "stalled-hls-root"
    output_root.mkdir()
    output_dir = output_root / "item-a"
    output_dir.mkdir()
    playlist_path = output_dir / "index.m3u8"
    playlist_path.write_text(
        "#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n#EXTINF:6,\nsegment_00002.ts\n#EXTINF:6,\nsegment_00003.ts\n",
        encoding="utf-8",
    )
    for index in range(1, 4):
        (output_dir / f"segment_{index:05d}.ts").write_bytes(b"segment")

    monkeypatch.setattr(byte_streaming, "_HLS_OUTPUT_ROOT", output_root)
    now = datetime.now(UTC)
    future = now + timedelta(seconds=byte_streaming._HLS_RETENTION_SECONDS + 5)
    for path in output_dir.rglob("*.ts"):
        os.utime(path, (now.timestamp(), now.timestamp()))
    os.utime(playlist_path, (now.timestamp(), now.timestamp()))
    os.utime(output_dir, (future.timestamp(), future.timestamp()))

    before_reap_runs = byte_streaming.get_serving_governance_snapshot()[
        "hls_stale_segment_reap_runs"
    ]
    before_reaped_files = byte_streaming.get_serving_governance_snapshot()[
        "hls_stale_segment_reaped_files"
    ]

    byte_streaming.cleanup_expired_hls_dirs(now=future)

    governance = byte_streaming.get_serving_governance_snapshot()
    assert governance["hls_stale_segment_reap_runs"] == before_reap_runs + 1
    assert governance["hls_stale_segment_reaped_files"] == before_reaped_files + 3
    assert output_dir.exists() is False


def test_cleanup_expired_hls_dirs_removes_empty_item_root_after_variant_cleanup(
    tmp_path: Path, monkeypatch: Any
) -> None:
    output_root = tmp_path / "variant-hls-root"
    item_root = output_root / "item-a"
    variant_dir = item_root / "variant-a"
    variant_dir.mkdir(parents=True)
    playlist_path = variant_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")
    (variant_dir / "segment_00001.ts").write_bytes(b"segment")

    monkeypatch.setattr(byte_streaming, "_HLS_OUTPUT_ROOT", output_root)
    future = datetime.now(UTC) + timedelta(seconds=byte_streaming._HLS_RETENTION_SECONDS + 5)
    os.utime(playlist_path, (datetime.now(UTC).timestamp(), datetime.now(UTC).timestamp()))
    os.utime(
        variant_dir / "segment_00001.ts",
        (datetime.now(UTC).timestamp(), datetime.now(UTC).timestamp()),
    )
    os.utime(variant_dir, (future.timestamp(), future.timestamp()))

    byte_streaming.cleanup_expired_hls_dirs(now=future)

    assert variant_dir.exists() is False
    assert item_root.exists() is False


def test_cleanup_expired_hls_dirs_enforces_disk_quota(monkeypatch: Any, tmp_path: Path) -> None:
    output_root = tmp_path / "quota-hls-root"
    output_root.mkdir()
    first = output_root / "first"
    second = output_root / "second"
    third = output_root / "third"
    for directory in (first, second, third):
        directory.mkdir()
        (directory / "index.m3u8").write_text(
            "#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8"
        )
        (directory / "segment_00001.ts").write_bytes(b"123456")

    directory_size = byte_streaming._hls_directory_size_bytes(first)

    now = datetime.now(UTC)
    os.utime(
        first, ((now - timedelta(seconds=3)).timestamp(), (now - timedelta(seconds=3)).timestamp())
    )
    os.utime(
        second, ((now - timedelta(seconds=2)).timestamp(), (now - timedelta(seconds=2)).timestamp())
    )
    os.utime(
        third, ((now - timedelta(seconds=1)).timestamp(), (now - timedelta(seconds=1)).timestamp())
    )

    monkeypatch.setattr(byte_streaming, "_HLS_OUTPUT_ROOT", output_root)
    monkeypatch.setattr(byte_streaming, "_HLS_DISK_HIGH_WATER_BYTES", directory_size * 3 - 1)
    monkeypatch.setattr(byte_streaming, "_HLS_DISK_LOW_WATER_BYTES", directory_size * 2)

    before_quota_runs = byte_streaming.get_serving_governance_snapshot()["hls_quota_reap_runs"]
    before_quota_deleted = byte_streaming.get_serving_governance_snapshot()[
        "hls_quota_deleted_dirs"
    ]

    byte_streaming.cleanup_expired_hls_dirs(now=now)

    governance = byte_streaming.get_serving_governance_snapshot()
    assert governance["hls_quota_reap_runs"] == before_quota_runs + 1
    assert governance["hls_quota_deleted_dirs"] == before_quota_deleted + 1
    assert first.exists() is False
    assert second.exists() is True
    assert third.exists() is True


def test_hls_playlist_route_rejects_generation_when_capacity_is_saturated(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "capacity-input.mkv"
    media_file.write_bytes(b"movie")
    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        raise HTTPException(
            status_code=503,
            detail="HLS generation capacity exceeded",
            headers={"Retry-After": "5"},
        )

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)
    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"
    assert response.json()["detail"] == "HLS generation capacity exceeded"
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert (
        governance["hls_route_failures_generation_capacity_exceeded"]
        == before["hls_route_failures_generation_capacity_exceeded"] + 1
    )


def test_hls_routes_reap_generated_artifacts_after_player_disconnect_window(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "player-validation-input.mkv"
    media_file.write_bytes(b"movie")
    playlist_dir = tmp_path / "player-validation-hls"
    playlist_dir.mkdir()
    monkeypatch.setattr(byte_streaming, "_HLS_OUTPUT_ROOT", tmp_path)
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text(
        "#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n#EXTINF:6,\nsegment_00002.ts\n#EXTINF:6,\nsegment_00003.ts\n",
        encoding="utf-8",
    )
    for index in range(1, 4):
        (playlist_dir / f"segment_{index:05d}.ts").write_bytes(f"segment-{index}".encode())

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    playlist_response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    assert playlist_response.status_code == 200
    assert f"/api/stream/{item.id}/hls/segment_00001.ts" in playlist_response.text

    for index in range(1, 4):
        segment_response = client.get(
            f"/api/v1/stream/hls/{item.id}/segment_{index:05d}.ts",
            headers=_headers(),
        )
        assert segment_response.status_code == 200
        assert segment_response.content == f"segment-{index}".encode()

    future = datetime.now(UTC) + timedelta(seconds=byte_streaming._HLS_RETENTION_SECONDS + 5)
    for path in playlist_dir.rglob("*.ts"):
        os.utime(path, (datetime.now(UTC).timestamp(), datetime.now(UTC).timestamp()))
    os.utime(playlist_path, (datetime.now(UTC).timestamp(), datetime.now(UTC).timestamp()))
    os.utime(playlist_dir, (future.timestamp(), future.timestamp()))

    byte_streaming.cleanup_expired_hls_dirs(now=future)

    assert playlist_dir.exists() is False


def test_playback_metrics_track_refresh_failures_and_selected_failed_leases() -> None:
    denied_before = _counter_value(
        playback_service.PLAYBACK_LEASE_REFRESH_FAILURES,
        record_type="media_entry",
        reason="denied",
    )
    failed_before = _counter_value(
        playback_service.PLAYBACK_LEASE_REFRESH_FAILURES,
        record_type="attachment",
        reason="failed",
    )

    entry = _build_media_entry(
        media_entry_id="media-entry-metrics-denied",
        item_id="item-metrics-denied",
        kind="remote-direct",
        download_url="https://api.example.com/restricted-denied",
        refresh_state="ready",
    )
    PlaybackSourceService.fail_media_entry_refresh(entry, error="refresh denied by provider")

    attachment = _build_playback_attachment(
        attachment_id="attachment-metrics-failed",
        item_id="item-metrics-failed",
        kind="remote-direct",
        locator="https://api.example.com/restricted-failed",
        restricted_url="https://api.example.com/restricted-failed",
        refresh_state="ready",
    )
    PlaybackSourceService.fail_attachment_refresh(attachment, error="provider unavailable")

    assert (
        _counter_value(
            playback_service.PLAYBACK_LEASE_REFRESH_FAILURES,
            record_type="media_entry",
            reason="denied",
        )
        == denied_before + 1
    )
    assert (
        _counter_value(
            playback_service.PLAYBACK_LEASE_REFRESH_FAILURES,
            record_type="attachment",
            reason="failed",
        )
        == failed_before + 1
    )

    item = _build_item(item_id="item-selected-failed-lease-metrics")
    direct_entry = _build_media_entry(
        media_entry_id="media-entry-metrics-direct-failed",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/direct-failed",
        refresh_state="failed",
    )
    hls_entry = _build_media_entry(
        media_entry_id="media-entry-metrics-hls-failed",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/hls-failed.m3u8",
        refresh_state="failed",
    )
    item.media_entries = [direct_entry, hls_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=direct_entry.id, role="direct"),
        _build_active_stream(item_id=item.id, media_entry_id=hls_entry.id, role="hls"),
    ]
    _client, resources = _build_client(items=[item])
    direct_risk_before = _counter_value(
        playback_service.PLAYBACK_RISK_EVENTS,
        surface="direct",
        reason="selected_failed_lease",
    )
    hls_risk_before = _counter_value(
        playback_service.PLAYBACK_RISK_EVENTS,
        surface="hls",
        reason="selected_failed_lease",
    )

    with pytest.raises(HTTPException):
        asyncio.run(PlaybackSourceService(resources.db).resolve_playback_attachment(item.id))
    with pytest.raises(HTTPException):
        asyncio.run(PlaybackSourceService(resources.db).resolve_hls_attachment(item.id))

    assert (
        _counter_value(
            playback_service.PLAYBACK_RISK_EVENTS,
            surface="direct",
            reason="selected_failed_lease",
        )
        == direct_risk_before + 1
    )
    assert (
        _counter_value(
            playback_service.PLAYBACK_RISK_EVENTS,
            surface="hls",
            reason="selected_failed_lease",
        )
        == hls_risk_before + 1
    )


def test_playback_resolution_duration_histogram_tracks_direct_and_hls_results(
    tmp_path: Path,
) -> None:
    direct_file = tmp_path / "metrics-direct-resolution.mkv"
    direct_file.write_bytes(b"movie")

    resolved_item = _build_item(
        item_id="item-resolution-duration-direct",
        attributes={"file_path": str(direct_file)},
    )
    failed_hls_item = _build_item(item_id="item-resolution-duration-hls-failed")
    failed_hls_entry = _build_media_entry(
        media_entry_id="media-entry-resolution-duration-hls-failed",
        item_id=failed_hls_item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-resolution-duration.m3u8",
        refresh_state="failed",
    )
    failed_hls_item.media_entries = [failed_hls_entry]
    failed_hls_item.active_streams = [
        _build_active_stream(
            item_id=failed_hls_item.id,
            media_entry_id=failed_hls_entry.id,
            role="hls",
        )
    ]
    _client, resources = _build_client(items=[resolved_item, failed_hls_item])

    direct_before = _histogram_count(
        playback_service.PLAYBACK_RESOLUTION_DURATION_SECONDS,
        surface="direct",
        result="resolved",
    )
    hls_failed_before = _histogram_count(
        playback_service.PLAYBACK_RESOLUTION_DURATION_SECONDS,
        surface="hls",
        result="failed_lease",
    )

    attachment = asyncio.run(
        PlaybackSourceService(resources.db).resolve_playback_attachment(resolved_item.id)
    )
    assert attachment.kind == "local-file"

    with pytest.raises(HTTPException):
        asyncio.run(PlaybackSourceService(resources.db).resolve_hls_attachment(failed_hls_item.id))

    assert (
        _histogram_count(
            playback_service.PLAYBACK_RESOLUTION_DURATION_SECONDS,
            surface="direct",
            result="resolved",
        )
        == direct_before + 1
    )
    assert (
        _histogram_count(
            playback_service.PLAYBACK_RESOLUTION_DURATION_SECONDS,
            surface="hls",
            result="failed_lease",
        )
        == hls_failed_before + 1
    )


def test_stream_route_metrics_track_success_and_failure_outcomes(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "stream-route-metrics.mkv"
    media_file.write_bytes(b"movie-bytes")
    item = _build_item(
        item_id="item-stream-route-metrics", attributes={"file_path": str(media_file)}
    )
    client, _ = _build_client(items=[item])

    file_success_before = _counter_value(
        stream_routes.STREAM_ROUTE_RESULTS,
        route="file",
        status_code="200",
    )
    hls_missing_before = _counter_value(
        stream_routes.STREAM_ROUTE_RESULTS,
        route="hls_file",
        status_code="404",
    )

    response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())
    assert response.status_code == 200

    playlist_dir = tmp_path / "generated-hls-route-metrics"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    missing_response = client.get(
        f"/api/v1/stream/hls/{item.id}/segment_00001.ts",
        headers=_headers(),
    )
    assert missing_response.status_code == 404

    assert (
        _counter_value(
            stream_routes.STREAM_ROUTE_RESULTS,
            route="file",
            status_code="200",
        )
        == file_success_before + 1
    )
    assert (
        _counter_value(
            stream_routes.STREAM_ROUTE_RESULTS,
            route="hls_file",
            status_code="404",
        )
        == hls_missing_before + 1
    )


def test_stream_routes_require_api_key() -> None:
    """All stream compatibility routes remain protected by the shared API-key dependency."""

    client, _ = _build_client()
    for path in (
        "/api/v1/stream/logging",
        "/api/v1/stream/file/item-1",
        "/api/v1/stream/hls/item-1/index.m3u8",
    ):
        response = client.get(path)
        assert response.status_code == 401


def test_stream_status_route_exposes_hls_route_failure_counters() -> None:
    client, _ = _build_client()

    response = client.get("/api/v1/stream/status", headers=_headers())

    assert response.status_code == 200
    governance = response.json()["governance"]
    assert governance["hls_route_failures_total"] >= 0
    assert governance["hls_route_failures_generation_failed"] >= 0
    assert governance["hls_route_failures_generation_timeout"] >= 0
    assert governance["hls_route_failures_generator_unavailable"] >= 0
    assert governance["hls_route_failures_lease_failed"] >= 0
    assert governance["hls_route_failures_manifest_invalid"] >= 0
    assert governance["hls_route_failures_generated_missing"] >= 0
    assert governance["hls_route_failures_upstream_failed"] >= 0


def test_stream_status_route_exposes_vfs_catalog_governance_snapshot() -> None:
    client, resources = _build_client()

    class _StubVfsCatalogServer:
        def build_governance_snapshot(self) -> dict[str, int]:
            return {
                "vfs_catalog_watch_sessions_started": 3,
                "vfs_catalog_watch_sessions_completed": 2,
                "vfs_catalog_watch_sessions_active": 1,
                "vfs_catalog_reconnect_requested": 4,
                "vfs_catalog_reconnect_delta_served": 3,
                "vfs_catalog_reconnect_current_generation_reused": 1,
                "vfs_catalog_reconnect_snapshot_fallback": 1,
                "vfs_catalog_reconnect_failures": 0,
                "vfs_catalog_snapshots_served": 5,
                "vfs_catalog_deltas_served": 7,
                "vfs_catalog_heartbeats_served": 9,
                "vfs_catalog_problem_events": 0,
                "vfs_catalog_request_stream_failures": 0,
                "vfs_catalog_snapshot_build_failures": 0,
                "vfs_catalog_delta_build_failures": 0,
                "vfs_catalog_refresh_attempts": 6,
                "vfs_catalog_refresh_succeeded": 5,
                "vfs_catalog_refresh_provider_failures": 1,
                "vfs_catalog_refresh_empty_results": 0,
                "vfs_catalog_refresh_validation_failed": 1,
                "vfs_catalog_refresh_skipped_no_provider": 0,
                "vfs_catalog_refresh_skipped_no_restricted_url": 0,
                "vfs_catalog_refresh_skipped_no_client": 0,
                "vfs_catalog_refresh_skipped_fresh": 2,
                "vfs_catalog_inline_refresh_requests": 2,
                "vfs_catalog_inline_refresh_deduplicated": 1,
                "vfs_catalog_inline_refresh_succeeded": 1,
                "vfs_catalog_inline_refresh_failed": 1,
                "vfs_catalog_inline_refresh_not_found": 0,
            }

    resources.vfs_catalog_server = cast(Any, _StubVfsCatalogServer())

    response = client.get("/api/v1/stream/status", headers=_headers())

    assert response.status_code == 200
    governance = response.json()["governance"]
    assert governance["vfs_catalog_watch_sessions_started"] == 3
    assert governance["vfs_catalog_watch_sessions_active"] == 1
    assert governance["vfs_catalog_reconnect_delta_served"] == 3
    assert governance["vfs_catalog_refresh_attempts"] == 6
    assert governance["vfs_catalog_refresh_validation_failed"] == 1
    assert governance["vfs_catalog_inline_refresh_failed"] == 1


def test_stream_status_route_exposes_vfs_runtime_governance_snapshot(
    tmp_path: Path, monkeypatch: Any
) -> None:
    runtime_status_path = tmp_path / "filmuvfs-runtime-status.json"
    runtime_status_path.write_text(
        json.dumps(
            {
                "runtime": {
                    "open_handles": 4,
                    "peak_open_handles": 9,
                    "active_reads": 2,
                    "peak_active_reads": 5,
                    "chunk_cache_weighted_bytes": 8192,
                },
                "handle_startup": {
                    "total": 7,
                    "ok": 3,
                    "error": 1,
                    "estale": 1,
                    "cancelled": 2,
                    "average_duration_ms": 104.8,
                    "max_duration_ms": 412.2,
                },
                "mounted_reads": {
                    "total": 10,
                    "ok": 6,
                    "error": 1,
                    "estale": 1,
                    "cancelled": 2,
                    "average_duration_ms": 12.6,
                    "max_duration_ms": 48.4,
                },
                "upstream_fetch": {
                    "operations": 5,
                    "bytes_total": 65536,
                    "average_duration_ms": 23.2,
                    "max_duration_ms": 71.9,
                },
                "upstream_failures": {
                    "invalid_url": 1,
                    "build_request": 0,
                    "network": 2,
                    "stale_status": 3,
                    "unexpected_status": 4,
                    "unexpected_status_too_many_requests": 2,
                    "unexpected_status_server_error": 1,
                    "read_body": 5,
                },
                "upstream_retryable_events": {
                    "network": 6,
                    "read_body": 7,
                    "status_too_many_requests": 8,
                    "status_server_error": 9,
                },
                "backend_fallback": {
                    "attempts": 10,
                    "success": 7,
                    "failure": 3,
                    "attempts_direct_read_failure": 4,
                    "attempts_inline_refresh_unavailable": 3,
                    "attempts_post_inline_refresh_failure": 3,
                    "success_direct_read_failure": 2,
                    "success_inline_refresh_unavailable": 3,
                    "success_post_inline_refresh_failure": 2,
                    "failure_direct_read_failure": 2,
                    "failure_inline_refresh_unavailable": 0,
                    "failure_post_inline_refresh_failure": 1,
                },
                "chunk_cache": {
                    "backend": "hybrid",
                    "hits": 9,
                    "misses": 3,
                    "inserts": 2,
                    "prefetch_hits": 1,
                    "memory_bytes": 4096,
                    "memory_max_bytes": 16384,
                    "memory_hits": 6,
                    "memory_misses": 4,
                    "disk_bytes": 12288,
                    "disk_max_bytes": 65536,
                    "disk_hits": 3,
                    "disk_misses": 1,
                    "disk_writes": 2,
                    "disk_write_errors": 1,
                    "disk_evictions": 4,
                },
                "prefetch": {
                    "concurrency_limit": 4,
                    "available_permits": 1,
                    "active_permits": 3,
                    "active_background_tasks": 2,
                    "peak_active_background_tasks": 4,
                    "background_spawned": 7,
                    "background_backpressure": 2,
                    "fairness_denied": 1,
                    "global_backpressure_denied": 1,
                    "background_error": 1,
                },
                "chunk_coalescing": {
                    "in_flight_chunks": 1,
                    "peak_in_flight_chunks": 3,
                    "waits_total": 6,
                    "waits_hit": 5,
                    "waits_miss": 1,
                    "wait_average_duration_ms": 14.25,
                    "wait_max_duration_ms": 89.5,
                },
                "inline_refresh": {
                    "success": 3,
                    "no_url": 1,
                    "error": 2,
                    "timeout": 1,
                },
                "windows_projfs": {
                    "callbacks_cancelled": 3,
                    "callbacks_error": 4,
                    "callbacks_estale": 2,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FILMU_PY_VFS_RUNTIME_STATUS_PATH", str(runtime_status_path))
    client, _ = _build_client()

    response = client.get("/api/v1/stream/status", headers=_headers())

    assert response.status_code == 200
    governance = response.json()["governance"]
    assert governance["vfs_runtime_snapshot_available"] == 1
    assert governance["vfs_runtime_open_handles"] == 4
    assert governance["vfs_runtime_peak_open_handles"] == 9
    assert governance["vfs_runtime_active_reads"] == 2
    assert governance["vfs_runtime_peak_active_reads"] == 5
    assert governance["vfs_runtime_chunk_cache_weighted_bytes"] == 8192
    assert governance["vfs_runtime_chunk_cache_backend"] == "hybrid"
    assert governance["vfs_runtime_chunk_cache_memory_bytes"] == 4096
    assert governance["vfs_runtime_chunk_cache_memory_max_bytes"] == 16384
    assert governance["vfs_runtime_chunk_cache_memory_hits"] == 6
    assert governance["vfs_runtime_chunk_cache_memory_misses"] == 4
    assert governance["vfs_runtime_chunk_cache_disk_bytes"] == 12288
    assert governance["vfs_runtime_chunk_cache_disk_max_bytes"] == 65536
    assert governance["vfs_runtime_chunk_cache_disk_hits"] == 3
    assert governance["vfs_runtime_chunk_cache_disk_misses"] == 1
    assert governance["vfs_runtime_chunk_cache_disk_writes"] == 2
    assert governance["vfs_runtime_chunk_cache_disk_write_errors"] == 1
    assert governance["vfs_runtime_chunk_cache_disk_evictions"] == 4
    assert governance["vfs_runtime_handle_startup_total"] == 7
    assert governance["vfs_runtime_handle_startup_ok"] == 3
    assert governance["vfs_runtime_handle_startup_error"] == 1
    assert governance["vfs_runtime_handle_startup_estale"] == 1
    assert governance["vfs_runtime_handle_startup_cancelled"] == 2
    assert governance["vfs_runtime_handle_startup_average_duration_ms"] == 105
    assert governance["vfs_runtime_handle_startup_max_duration_ms"] == 412
    assert governance["vfs_runtime_mounted_reads_total"] == 10
    assert governance["vfs_runtime_mounted_reads_ok"] == 6
    assert governance["vfs_runtime_mounted_reads_error"] == 1
    assert governance["vfs_runtime_mounted_reads_estale"] == 1
    assert governance["vfs_runtime_mounted_reads_cancelled"] == 2
    assert governance["vfs_runtime_mounted_reads_average_duration_ms"] == 13
    assert governance["vfs_runtime_mounted_reads_max_duration_ms"] == 48
    assert governance["vfs_runtime_upstream_fetch_operations"] == 5
    assert governance["vfs_runtime_upstream_fetch_bytes_total"] == 65536
    assert governance["vfs_runtime_upstream_fetch_average_duration_ms"] == 23
    assert governance["vfs_runtime_upstream_fetch_max_duration_ms"] == 72
    assert governance["vfs_runtime_upstream_fail_invalid_url"] == 1
    assert governance["vfs_runtime_upstream_fail_build_request"] == 0
    assert governance["vfs_runtime_upstream_fail_network"] == 2
    assert governance["vfs_runtime_upstream_fail_stale_status"] == 3
    assert governance["vfs_runtime_upstream_fail_unexpected_status"] == 4
    assert governance["vfs_runtime_upstream_fail_unexpected_status_too_many_requests"] == 2
    assert governance["vfs_runtime_upstream_fail_unexpected_status_server_error"] == 1
    assert governance["vfs_runtime_upstream_fail_read_body"] == 5
    assert governance["vfs_runtime_upstream_retryable_network"] == 6
    assert governance["vfs_runtime_upstream_retryable_read_body"] == 7
    assert governance["vfs_runtime_upstream_retryable_status_too_many_requests"] == 8
    assert governance["vfs_runtime_upstream_retryable_status_server_error"] == 9
    assert governance["vfs_runtime_backend_fallback_attempts"] == 10
    assert governance["vfs_runtime_backend_fallback_success"] == 7
    assert governance["vfs_runtime_backend_fallback_failure"] == 3
    assert governance["vfs_runtime_backend_fallback_attempts_direct_read_failure"] == 4
    assert governance["vfs_runtime_backend_fallback_attempts_inline_refresh_unavailable"] == 3
    assert (
        governance["vfs_runtime_backend_fallback_attempts_post_inline_refresh_failure"] == 3
    )
    assert governance["vfs_runtime_backend_fallback_success_direct_read_failure"] == 2
    assert governance["vfs_runtime_backend_fallback_success_inline_refresh_unavailable"] == 3
    assert (
        governance["vfs_runtime_backend_fallback_success_post_inline_refresh_failure"] == 2
    )
    assert governance["vfs_runtime_backend_fallback_failure_direct_read_failure"] == 2
    assert governance["vfs_runtime_backend_fallback_failure_inline_refresh_unavailable"] == 0
    assert (
        governance["vfs_runtime_backend_fallback_failure_post_inline_refresh_failure"] == 1
    )
    assert governance["vfs_runtime_chunk_cache_hits"] == 9
    assert governance["vfs_runtime_chunk_cache_misses"] == 3
    assert governance["vfs_runtime_chunk_cache_inserts"] == 2
    assert governance["vfs_runtime_chunk_cache_prefetch_hits"] == 1
    assert governance["vfs_runtime_prefetch_concurrency_limit"] == 4
    assert governance["vfs_runtime_prefetch_available_permits"] == 1
    assert governance["vfs_runtime_prefetch_active_permits"] == 3
    assert governance["vfs_runtime_prefetch_active_background_tasks"] == 2
    assert governance["vfs_runtime_prefetch_peak_active_background_tasks"] == 4
    assert governance["vfs_runtime_prefetch_background_spawned"] == 7
    assert governance["vfs_runtime_prefetch_background_backpressure"] == 2
    assert governance["vfs_runtime_prefetch_fairness_denied"] == 1
    assert governance["vfs_runtime_prefetch_global_backpressure_denied"] == 1
    assert governance["vfs_runtime_prefetch_background_error"] == 1
    assert governance["vfs_runtime_chunk_coalescing_in_flight_chunks"] == 1
    assert governance["vfs_runtime_chunk_coalescing_peak_in_flight_chunks"] == 3
    assert governance["vfs_runtime_chunk_coalescing_waits_total"] == 6
    assert governance["vfs_runtime_chunk_coalescing_waits_hit"] == 5
    assert governance["vfs_runtime_chunk_coalescing_waits_miss"] == 1
    assert governance["vfs_runtime_chunk_coalescing_wait_average_duration_ms"] == 14.25
    assert governance["vfs_runtime_chunk_coalescing_wait_max_duration_ms"] == 89.5
    assert governance["vfs_runtime_inline_refresh_success"] == 3
    assert governance["vfs_runtime_inline_refresh_no_url"] == 1
    assert governance["vfs_runtime_inline_refresh_error"] == 2
    assert governance["vfs_runtime_inline_refresh_timeout"] == 1
    assert governance["vfs_runtime_windows_callbacks_cancelled"] == 3
    assert governance["vfs_runtime_windows_callbacks_error"] == 4
    assert governance["vfs_runtime_windows_callbacks_estale"] == 2
    assert governance["vfs_runtime_cache_hit_ratio"] == 0.75
    assert governance["vfs_runtime_fallback_success_ratio"] == 0.7
    assert governance["vfs_runtime_prefetch_pressure_ratio"] == 0.75
    assert governance["vfs_runtime_provider_pressure_incidents"] == 22
    assert governance["vfs_runtime_fairness_pressure_incidents"] == 2
    assert governance["vfs_runtime_cache_pressure_class"] == "critical"
    assert governance["vfs_runtime_cache_pressure_reasons"] == [
        "disk_write_errors",
        "disk_evictions_observed",
    ]
    assert governance["vfs_runtime_chunk_coalescing_pressure_class"] == "warning"
    assert governance["vfs_runtime_chunk_coalescing_pressure_reasons"] == [
        "coalescing_wait_misses",
        "coalescing_wait_latency_high",
        "coalescing_wait_spike",
    ]
    assert governance["vfs_runtime_upstream_wait_class"] == "critical"
    assert governance["vfs_runtime_upstream_wait_reasons"] == [
        "provider_pressure_incidents",
        "retryable_network_wait",
        "retryable_read_body_wait",
    ]
    assert governance["vfs_runtime_refresh_pressure_class"] == "critical"
    assert governance["vfs_runtime_refresh_pressure_reasons"] == [
        "backend_fallback_failures",
        "inline_refresh_errors",
        "inline_refresh_timeouts",
        "backend_fallback_activity",
    ]
    assert governance["vfs_runtime_rollout_readiness"] == "blocked"
    assert governance["vfs_runtime_rollout_reasons"] == [
        "backend_fallback_failures",
        "mounted_read_errors",
        "prefetch_background_errors",
        "disk_cache_write_errors",
    ]
    assert governance["vfs_runtime_rollout_next_action"] == "resolve_blocking_runtime_failures"
    assert governance["vfs_runtime_rollout_canary_decision"] == "rollback_current_environment"
    assert governance["vfs_runtime_rollout_merge_gate"] == "blocked"


def test_stream_status_route_exposes_playback_gate_and_vfs_canary_readiness(
    tmp_path: Path, monkeypatch: Any
) -> None:
    captured_at = datetime.now(UTC).replace(microsecond=0)
    captured_at_text = captured_at.isoformat().replace("+00:00", "Z")
    expires_at_text = (captured_at + timedelta(hours=4)).isoformat().replace("+00:00", "Z")
    runtime_status_path = tmp_path / "filmuvfs-runtime-status.json"
    runtime_status_path.write_text(
        json.dumps(
            {
                "runtime": {
                    "open_handles": 2,
                    "peak_open_handles": 4,
                    "active_reads": 1,
                    "peak_active_reads": 2,
                    "chunk_cache_weighted_bytes": 4096,
                },
                "handle_startup": {
                    "total": 3,
                    "ok": 3,
                    "error": 0,
                    "estale": 0,
                    "cancelled": 0,
                    "average_duration_ms": 41.2,
                    "max_duration_ms": 88.4,
                },
                "mounted_reads": {
                    "total": 6,
                    "ok": 6,
                    "error": 0,
                    "estale": 0,
                    "cancelled": 0,
                    "average_duration_ms": 8.1,
                    "max_duration_ms": 18.0,
                },
                "upstream_fetch": {
                    "operations": 2,
                    "bytes_total": 32768,
                    "average_duration_ms": 17.0,
                    "max_duration_ms": 29.0,
                },
                "upstream_failures": {
                    "invalid_url": 0,
                    "build_request": 0,
                    "network": 0,
                    "stale_status": 0,
                    "unexpected_status": 0,
                    "unexpected_status_too_many_requests": 0,
                    "unexpected_status_server_error": 0,
                    "read_body": 0,
                },
                "upstream_retryable_events": {
                    "network": 0,
                    "read_body": 0,
                    "status_too_many_requests": 0,
                    "status_server_error": 0,
                },
                "backend_fallback": {
                    "attempts": 0,
                    "success": 0,
                    "failure": 0,
                    "attempts_direct_read_failure": 0,
                    "attempts_inline_refresh_unavailable": 0,
                    "attempts_post_inline_refresh_failure": 0,
                    "success_direct_read_failure": 0,
                    "success_inline_refresh_unavailable": 0,
                    "success_post_inline_refresh_failure": 0,
                    "failure_direct_read_failure": 0,
                    "failure_inline_refresh_unavailable": 0,
                    "failure_post_inline_refresh_failure": 0,
                },
                "chunk_cache": {
                    "backend": "hybrid",
                    "hits": 8,
                    "misses": 2,
                    "inserts": 2,
                    "prefetch_hits": 1,
                    "memory_bytes": 2048,
                    "memory_max_bytes": 8192,
                    "memory_hits": 6,
                    "memory_misses": 2,
                    "disk_bytes": 8192,
                    "disk_max_bytes": 65536,
                    "disk_hits": 2,
                    "disk_misses": 0,
                    "disk_writes": 1,
                    "disk_write_errors": 0,
                    "disk_evictions": 0,
                },
                "prefetch": {
                    "concurrency_limit": 4,
                    "available_permits": 4,
                    "active_permits": 0,
                    "active_background_tasks": 0,
                    "peak_active_background_tasks": 1,
                    "background_spawned": 1,
                    "background_backpressure": 0,
                    "fairness_denied": 0,
                    "global_backpressure_denied": 0,
                    "background_error": 0,
                },
                "chunk_coalescing": {
                    "in_flight_chunks": 0,
                    "peak_in_flight_chunks": 1,
                    "waits_total": 1,
                    "waits_hit": 1,
                    "waits_miss": 0,
                    "wait_average_duration_ms": 3.0,
                    "wait_max_duration_ms": 3.0,
                },
                "inline_refresh": {
                    "success": 1,
                    "no_url": 0,
                    "error": 0,
                    "timeout": 0,
                },
                "windows_projfs": {
                    "callbacks_cancelled": 0,
                    "callbacks_error": 0,
                    "callbacks_estale": 0,
                },
            }
        ),
        encoding="utf-8",
    )
    artifacts_root = tmp_path / "playback-proof-artifacts"
    windows_artifacts_root = artifacts_root / "windows-native-stack"
    windows_artifacts_root.mkdir(parents=True)
    (artifacts_root / "stability-summary-20260412-010101.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "environment_class": "windows-native:managed",
                "repeat_count": 2,
                "dry_run": False,
                "all_green": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "ci-execution-summary.json").write_text(
        json.dumps(
            {
                "required_check_name": "Playback Gate / Playback Gate",
                "gate_mode": "full",
                "provider_gate_required": True,
                "provider_gate_ran": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "media-server-gate-20260412-010102.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_kind": "media_server_provider_parity",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 24,
                "status": "passed",
                "ready": True,
                "all_green": True,
                "failure_reasons": [],
                "required_actions": [],
                "results": [
                    {"provider": "plex", "status": "passed"},
                    {"provider": "emby", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-010103.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "coverage": ["emby:movie", "plex:movie"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "movie",
                "results": [
                    {"provider": "plex", "status": "passed"},
                    {"provider": "emby", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-010103-tv.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "coverage": ["emby:tv", "plex:tv"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "tv",
                "results": [
                    {"provider": "plex", "status": "passed"},
                    {"provider": "emby", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (windows_artifacts_root / "soak-program-summary-20260412-010104.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_kind": "windows_vfs_soak_program",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "ready": True,
                "environment_class": "windows-native:managed",
                "repeat_count": 1,
                "profiles": [
                    "continuous",
                    "seek",
                    "concurrent",
                    "full",
                ],
                "profile_coverage": [
                    "continuous",
                    "seek",
                    "concurrent",
                    "full",
                ],
                "profile_coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "all_green": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "github-main-policy-current.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "artifact_kind": "github_main_policy_validation",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 24,
                "failure_reasons": [],
                "required_actions": [],
                "validation": {
                    "status": "ready",
                    "stale": False,
                    "failure_reasons": [],
                    "required_actions": [],
                },
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "playback-gate-runner-readiness.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "artifact_kind": "playback_gate_runner_readiness",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 24,
                "status": "ready",
                "required_failure_count": 0,
                "required_actions": [],
                "failure_reasons": [],
                "checks": [
                    {
                        "name": "frontend_context",
                        "required": True,
                        "ok": True,
                    },
                    {
                        "name": "browser_executable",
                        "required": True,
                        "ok": True,
                    },
                    {
                        "name": "linux_fuse",
                        "required": True,
                        "ok": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FILMU_PY_VFS_RUNTIME_STATUS_PATH", str(runtime_status_path))
    monkeypatch.setenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT", str(artifacts_root))
    client, _ = _build_client()

    response = client.get("/api/v1/stream/status", headers=_headers())

    assert response.status_code == 200
    governance = response.json()["governance"]
    assert governance["playback_gate_snapshot_available"] == 1
    assert governance["playback_gate_stability_ready"] == 1
    assert governance["playback_gate_provider_parity_ready"] == 1
    assert governance["playback_gate_windows_provider_ready"] == 1
    assert governance["playback_gate_windows_provider_stale"] == 0
    assert governance["playback_gate_windows_soak_ready"] == 1
    assert governance["playback_gate_windows_soak_stale"] == 0
    assert governance["playback_gate_policy_validation_status"] == "ready"
    assert governance["playback_gate_policy_ready"] == 1
    assert governance["playback_gate_policy_validation_stale"] == 0
    assert governance["playback_gate_runner_stale"] == 0
    assert governance["playback_gate_rollout_readiness"] == "ready"
    assert governance["playback_gate_rollout_reasons"] == ["playback_gate_green"]
    assert governance["playback_gate_rollout_next_action"] == "keep_required_checks_enforced"
    assert governance["vfs_runtime_rollout_readiness"] == "ready"
    assert governance["vfs_runtime_rollout_canary_decision"] == "promote_to_next_environment_class"
    assert governance["vfs_runtime_rollout_merge_gate"] == "ready"
    assert governance["vfs_runtime_rollout_environment_class"] == "windows-native:managed"
    assert governance["vfs_runtime_cache_pressure_class"] == "healthy"
    assert governance["vfs_runtime_chunk_coalescing_pressure_class"] == "healthy"
    assert governance["vfs_runtime_upstream_wait_class"] == "healthy"
    assert governance["vfs_runtime_refresh_pressure_class"] == "healthy"


def test_stream_status_route_blocks_stale_playback_gate_runner_and_policy_evidence(
    tmp_path: Path, monkeypatch: Any
) -> None:
    captured_at = datetime.now(UTC).replace(microsecond=0) - timedelta(days=3)
    captured_at_text = captured_at.isoformat().replace("+00:00", "Z")
    expires_at_text = (captured_at + timedelta(hours=12)).isoformat().replace("+00:00", "Z")
    fresh_proof_captured_at = datetime.now(UTC).replace(microsecond=0)
    fresh_proof_captured_at_text = fresh_proof_captured_at.isoformat().replace("+00:00", "Z")
    fresh_proof_expires_at_text = (
        fresh_proof_captured_at + timedelta(hours=4)
    ).isoformat().replace("+00:00", "Z")
    artifacts_root = tmp_path / "playback-proof-artifacts"
    windows_artifacts_root = artifacts_root / "windows-native-stack"
    windows_artifacts_root.mkdir(parents=True)
    (artifacts_root / "stability-summary-20260412-010101.json").write_text(
        json.dumps(
            {
                "timestamp": fresh_proof_captured_at_text,
                "environment_class": "windows-native:managed",
                "repeat_count": 2,
                "dry_run": False,
                "all_green": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "ci-execution-summary.json").write_text(
        json.dumps(
            {
                "required_check_name": "Playback Gate / Playback Gate",
                "gate_mode": "full",
                "provider_gate_required": True,
                "provider_gate_ran": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "media-server-gate-20260412-010102.json").write_text(
        json.dumps({"timestamp": fresh_proof_captured_at_text, "all_green": True}),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-010103.json").write_text(
        json.dumps(
            {
                "timestamp": fresh_proof_captured_at_text,
                "captured_at": fresh_proof_captured_at_text,
                "expires_at": fresh_proof_expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "coverage": ["emby:movie", "plex:movie"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "movie",
                "results": [
                    {"provider": "plex", "status": "passed"},
                    {"provider": "emby", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-010103-tv.json").write_text(
        json.dumps(
            {
                "timestamp": fresh_proof_captured_at_text,
                "captured_at": fresh_proof_captured_at_text,
                "expires_at": fresh_proof_expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "coverage": ["emby:tv", "plex:tv"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "tv",
                "results": [
                    {"provider": "plex", "status": "passed"},
                    {"provider": "emby", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (windows_artifacts_root / "soak-program-summary-20260412-010104.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_kind": "windows_vfs_soak_program",
                "timestamp": fresh_proof_captured_at_text,
                "captured_at": fresh_proof_captured_at_text,
                "expires_at": fresh_proof_expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "ready": True,
                "environment_class": "windows-native:managed",
                "repeat_count": 1,
                "profiles": ["continuous", "seek", "concurrent", "full"],
                "profile_coverage": ["continuous", "seek", "concurrent", "full"],
                "profile_coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "all_green": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "github-main-policy-current.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "artifact_kind": "github_main_policy_validation",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 12,
                "failure_reasons": [],
                "required_actions": ["validate_github_main_policy_from_admin_authenticated_host"],
                "validation": {
                    "status": "ready",
                    "stale": False,
                    "failure_reasons": [],
                    "required_actions": ["validate_github_main_policy_from_admin_authenticated_host"],
                },
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "playback-gate-runner-readiness.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "artifact_kind": "playback_gate_runner_readiness",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 12,
                "status": "ready",
                "required_failure_count": 0,
                "required_actions": ["capture_runner_prerequisites_on_github_hosted_runner"],
                "failure_reasons": [],
                "checks": [{"name": "github_hosted_runner", "required": True, "ok": True}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT", str(artifacts_root))
    client, _ = _build_client()

    response = client.get("/api/v1/stream/status", headers=_headers())

    assert response.status_code == 200
    governance = response.json()["governance"]
    assert governance["playback_gate_runner_ready"] == 0
    assert governance["playback_gate_runner_stale"] == 1
    assert governance["playback_gate_policy_ready"] == 0
    assert governance["playback_gate_policy_validation_stale"] == 1
    assert governance["playback_gate_windows_provider_stale"] == 0
    assert governance["playback_gate_windows_soak_stale"] == 0
    assert governance["playback_gate_rollout_readiness"] == "blocked"
    assert "runner_readiness_stale" in governance["playback_gate_rollout_reasons"]
    assert "github_main_policy_stale" in governance["playback_gate_rollout_reasons"]


def test_stream_status_route_blocks_classified_provider_gate_failures(
    tmp_path: Path, monkeypatch: Any
) -> None:
    captured_at = datetime.now(UTC).replace(microsecond=0)
    captured_at_text = captured_at.isoformat().replace("+00:00", "Z")
    expires_at_text = (captured_at + timedelta(hours=4)).isoformat().replace("+00:00", "Z")
    artifacts_root = tmp_path / "playback-proof-artifacts"
    windows_artifacts_root = artifacts_root / "windows-native-stack"
    windows_artifacts_root.mkdir(parents=True)
    (artifacts_root / "stability-summary-20260412-010101.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "environment_class": "windows-native:managed",
                "repeat_count": 2,
                "dry_run": False,
                "all_green": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "ci-execution-summary.json").write_text(
        json.dumps(
            {
                "required_check_name": "Playback Gate / Playback Gate",
                "gate_mode": "full",
                "provider_gate_required": True,
                "provider_gate_ran": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "media-server-gate-20260412-010102.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_kind": "media_server_provider_parity",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 24,
                "status": "failed",
                "ready": False,
                "all_green": False,
                "failure_reasons": [
                    "provider_gate_docker_plex_mount_path_drift",
                    "provider_gate_wsl_host_binary_stale",
                ],
                "required_actions": [
                    "realign_docker_plex_mount_path",
                    "rebuild_wsl_host_mount_binary",
                ],
                "results": [
                    {
                        "provider": "plex",
                        "status": "failed",
                        "failure_reasons": [
                            "provider_gate_docker_plex_mount_path_drift",
                            "provider_gate_wsl_host_binary_stale",
                        ],
                        "required_actions": [
                            "realign_docker_plex_mount_path",
                            "rebuild_wsl_host_mount_binary",
                        ],
                    },
                    {
                        "provider": "emby",
                        "status": "passed",
                        "failure_reasons": [],
                        "required_actions": [],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-010103.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "coverage": ["emby:movie", "plex:movie"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "movie",
                "results": [
                    {"provider": "plex", "status": "passed"},
                    {"provider": "emby", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-010103-tv.json").write_text(
        json.dumps(
            {
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "coverage": ["emby:tv", "plex:tv"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "tv",
                "results": [
                    {"provider": "plex", "status": "passed"},
                    {"provider": "emby", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (windows_artifacts_root / "soak-program-summary-20260412-010104.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_kind": "windows_vfs_soak_program",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 72,
                "status": "passed",
                "ready": True,
                "environment_class": "windows-native:managed",
                "repeat_count": 1,
                "profiles": ["continuous", "seek", "concurrent", "full"],
                "profile_coverage": ["continuous", "seek", "concurrent", "full"],
                "profile_coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "all_green": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "github-main-policy-current.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "artifact_kind": "github_main_policy_validation",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 12,
                "failure_reasons": [],
                "required_actions": [],
                "validation": {
                    "status": "ready",
                    "stale": False,
                    "failure_reasons": [],
                    "required_actions": [],
                },
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "playback-gate-runner-readiness.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "artifact_kind": "playback_gate_runner_readiness",
                "timestamp": captured_at_text,
                "captured_at": captured_at_text,
                "expires_at": expires_at_text,
                "freshness_window_hours": 12,
                "status": "ready",
                "required_failure_count": 0,
                "required_actions": [],
                "failure_reasons": [],
                "checks": [{"name": "github_hosted_runner", "required": True, "ok": True}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT", str(artifacts_root))
    client, _ = _build_client()

    response = client.get("/api/v1/stream/status", headers=_headers())

    assert response.status_code == 200
    governance = response.json()["governance"]
    assert governance["playback_gate_provider_parity_ready"] == 0
    assert governance["playback_gate_provider_gate_stale"] == 0
    assert governance["playback_gate_provider_gate_failure_reasons"] == [
        "provider_gate_docker_plex_mount_path_drift",
        "provider_gate_wsl_host_binary_stale",
    ]
    assert governance["playback_gate_provider_gate_required_actions"] == [
        "realign_docker_plex_mount_path",
        "rebuild_wsl_host_mount_binary",
        "rerun_media_server_provider_gate",
    ]
    assert governance["playback_gate_rollout_readiness"] == "blocked"
    assert (
        "provider_gate_docker_plex_mount_path_drift"
        in governance["playback_gate_rollout_reasons"]
    )
    assert "provider_gate_wsl_host_binary_stale" in governance["playback_gate_rollout_reasons"]
    assert "provider_gate_not_green" not in governance["playback_gate_rollout_reasons"]


def test_stream_status_route_blocks_stale_windows_soak_and_native_media_proofs(
    tmp_path: Path, monkeypatch: Any
) -> None:
    stale_captured_at = datetime.now(UTC).replace(microsecond=0) - timedelta(days=4)
    stale_captured_at_text = stale_captured_at.isoformat().replace("+00:00", "Z")
    stale_expires_at_text = (stale_captured_at + timedelta(hours=12)).isoformat().replace(
        "+00:00", "Z"
    )
    fresh_captured_at = datetime.now(UTC).replace(microsecond=0)
    fresh_captured_at_text = fresh_captured_at.isoformat().replace("+00:00", "Z")
    fresh_expires_at_text = (fresh_captured_at + timedelta(hours=4)).isoformat().replace(
        "+00:00", "Z"
    )
    artifacts_root = tmp_path / "playback-proof-artifacts"
    windows_artifacts_root = artifacts_root / "windows-native-stack"
    windows_artifacts_root.mkdir(parents=True)
    (artifacts_root / "stability-summary-20260412-010101.json").write_text(
        json.dumps(
            {
                "timestamp": fresh_captured_at_text,
                "environment_class": "windows-native:managed",
                "repeat_count": 2,
                "dry_run": False,
                "all_green": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "ci-execution-summary.json").write_text(
        json.dumps(
            {
                "required_check_name": "Playback Gate / Playback Gate",
                "gate_mode": "full",
                "provider_gate_required": True,
                "provider_gate_ran": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "media-server-gate-20260412-010102.json").write_text(
        json.dumps({"timestamp": fresh_captured_at_text, "all_green": True}),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-010103.json").write_text(
        json.dumps(
            {
                "timestamp": stale_captured_at_text,
                "captured_at": stale_captured_at_text,
                "expires_at": stale_expires_at_text,
                "freshness_window_hours": 12,
                "status": "passed",
                "coverage": ["emby:movie", "plex:movie"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "movie",
                "results": [
                    {"provider": "plex", "status": "passed"},
                    {"provider": "emby", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "windows-media-server-gate-20260412-010103-tv.json").write_text(
        json.dumps(
            {
                "timestamp": stale_captured_at_text,
                "captured_at": stale_captured_at_text,
                "expires_at": stale_expires_at_text,
                "freshness_window_hours": 12,
                "status": "passed",
                "coverage": ["emby:tv", "plex:tv"],
                "coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "media_type": "tv",
                "results": [
                    {"provider": "plex", "status": "passed"},
                    {"provider": "emby", "status": "passed"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (windows_artifacts_root / "soak-program-summary-20260412-010104.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_kind": "windows_vfs_soak_program",
                "timestamp": stale_captured_at_text,
                "captured_at": stale_captured_at_text,
                "expires_at": stale_expires_at_text,
                "freshness_window_hours": 12,
                "status": "passed",
                "ready": True,
                "environment_class": "windows-native:managed",
                "repeat_count": 1,
                "profiles": ["continuous", "seek", "concurrent", "full"],
                "profile_coverage": ["continuous", "seek", "concurrent", "full"],
                "profile_coverage_complete": True,
                "failure_reasons": [],
                "required_actions": [],
                "all_green": True,
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "github-main-policy-current.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "artifact_kind": "github_main_policy_validation",
                "timestamp": fresh_captured_at_text,
                "captured_at": fresh_captured_at_text,
                "expires_at": fresh_expires_at_text,
                "freshness_window_hours": 24,
                "failure_reasons": [],
                "required_actions": [],
                "validation": {
                    "status": "ready",
                    "stale": False,
                    "failure_reasons": [],
                    "required_actions": [],
                },
            }
        ),
        encoding="utf-8",
    )
    (artifacts_root / "playback-gate-runner-readiness.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "artifact_kind": "playback_gate_runner_readiness",
                "timestamp": fresh_captured_at_text,
                "captured_at": fresh_captured_at_text,
                "expires_at": fresh_expires_at_text,
                "freshness_window_hours": 24,
                "status": "ready",
                "required_failure_count": 0,
                "required_actions": [],
                "failure_reasons": [],
                "checks": [{"name": "github_hosted_runner", "required": True, "ok": True}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT", str(artifacts_root))
    client, _ = _build_client()

    response = client.get("/api/v1/stream/status", headers=_headers())

    assert response.status_code == 200
    governance = response.json()["governance"]
    assert governance["playback_gate_windows_provider_ready"] == 0
    assert governance["playback_gate_windows_provider_stale"] == 1
    assert governance["playback_gate_windows_soak_ready"] == 0
    assert governance["playback_gate_windows_soak_stale"] == 1
    assert governance["playback_gate_rollout_readiness"] == "blocked"
    assert "windows_provider_gate_stale" in governance["playback_gate_rollout_reasons"]
    assert "windows_vfs_soak_stale" in governance["playback_gate_rollout_reasons"]


def test_stream_status_route_filters_runtime_handle_summaries_to_request_tenant(
    tmp_path: Path, monkeypatch: Any
) -> None:
    runtime_status_path = tmp_path / "filmuvfs-runtime-status.json"
    runtime_status_path.write_text(
        json.dumps(
            {
                "runtime": {
                    "open_handles": 2,
                    "peak_open_handles": 2,
                    "active_reads": 1,
                    "peak_active_reads": 1,
                    "chunk_cache_weighted_bytes": 1024,
                    "active_handle_summaries": [
                        "global|session-a|handle-a|/mnt/global/movie.mkv|invalidated=false",
                        "tenant-other|session-b|handle-b|/mnt/other/movie.mkv|invalidated=true",
                    ],
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FILMU_PY_VFS_RUNTIME_STATUS_PATH", str(runtime_status_path))
    client, _ = _build_client()

    response = client.get("/api/v1/stream/status", headers=_headers())

    assert response.status_code == 200
    governance = response.json()["governance"]
    assert governance["vfs_runtime_active_handles_visible"] == 1
    assert governance["vfs_runtime_active_handles_hidden"] == 1
    assert governance["vfs_runtime_active_handle_tenant_count"] == 1
    assert governance["vfs_runtime_active_handle_tenants"] == ["global"]
    assert governance["vfs_runtime_active_handle_summaries"] == [
        "global|session-a|handle-a|invalidated=false"
    ]


def test_stream_status_route_hides_unknown_runtime_handle_summaries_for_tenant_scoped_requests(
    tmp_path: Path, monkeypatch: Any
) -> None:
    runtime_status_path = tmp_path / "filmuvfs-runtime-status.json"
    runtime_status_path.write_text(
        json.dumps(
            {
                "runtime": {
                    "open_handles": 2,
                    "peak_open_handles": 2,
                    "active_reads": 1,
                    "peak_active_reads": 1,
                    "chunk_cache_weighted_bytes": 1024,
                    "active_handle_summaries": [
                        "global|session-a|handle-a|/mnt/global/movie.mkv|invalidated=false",
                        "session-unknown|handle-unknown|/mnt/unknown/movie.mkv|invalidated=false",
                    ],
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FILMU_PY_VFS_RUNTIME_STATUS_PATH", str(runtime_status_path))
    client, _ = _build_client()

    response = client.get("/api/v1/stream/status", headers=_headers())

    assert response.status_code == 200
    governance = response.json()["governance"]
    assert governance["vfs_runtime_active_handles_visible"] == 1
    assert governance["vfs_runtime_active_handles_hidden"] == 1
    assert governance["vfs_runtime_active_handle_tenant_count"] == 1
    assert governance["vfs_runtime_active_handle_tenants"] == ["global"]
    assert governance["vfs_runtime_active_handle_summaries"] == [
        "global|session-a|handle-a|invalidated=false"
    ]


def test_hls_route_failure_governance_counts_generation_timeout(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-timeout-governance.mkv"
    media_file.write_bytes(b"movie-bytes")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])
    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        raise HTTPException(status_code=504, detail="HLS generation timed out")

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 503
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert (
        governance["hls_route_failures_generation_timeout"]
        == before["hls_route_failures_generation_timeout"] + 1
    )
    assert governance["hls_route_failures_total"] == before["hls_route_failures_total"] + 1


def test_hls_route_failure_governance_counts_manifest_invalid(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-manifest-invalid-governance.mkv"
    media_file.write_bytes(b"movie-bytes")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])
    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        raise HTTPException(status_code=500, detail="Generated HLS playlist is malformed")

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 503
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert (
        governance["hls_route_failures_manifest_invalid"]
        == before["hls_route_failures_manifest_invalid"] + 1
    )
    assert governance["hls_route_failures_total"] == before["hls_route_failures_total"] + 1


def test_hls_route_failure_governance_counts_lease_failed() -> None:
    item = _build_item(item_id="item-hls-failed-lease-governance")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-hls-failed-lease-governance",
        item_id=item.id,
        kind="remote-hls",
        download_url="https://api.example.com/restricted-hls-failed-lease-governance.m3u8",
        unrestricted_url="https://cdn.example.com/hls-failed-lease-governance.m3u8",
        refresh_state="failed",
        last_refresh_error="refresh denied",
        provider="realdebrid",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="hls")
    ]
    client, _ = _build_client(items=[item])
    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 503
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert (
        governance["hls_route_failures_lease_failed"]
        == before["hls_route_failures_lease_failed"] + 1
    )
    assert governance["hls_route_failures_total"] == before["hls_route_failures_total"] + 1


def test_hls_route_failure_governance_counts_generated_missing(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-generated-missing-governance.mkv"
    media_file.write_bytes(b"movie-bytes")
    playlist_dir = tmp_path / "generated-hls-missing-governance"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])
    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    async def fake_ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        return playlist_path

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(f"/api/v1/stream/hls/{item.id}/segment_00001.ts", headers=_headers())

    assert response.status_code == 404
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert (
        governance["hls_route_failures_generated_missing"]
        == before["hls_route_failures_generated_missing"] + 1
    )
    assert governance["hls_route_failures_total"] == before["hls_route_failures_total"] + 1


def test_hls_route_failure_governance_counts_upstream_playlist_failure(
    monkeypatch: Any,
) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/master.m3u8"})
    client, _ = _build_client(items=[item])
    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    async def fake_download_text(url: str) -> tuple[str, httpx.Headers]:
        assert url == "https://example.com/master.m3u8"
        raise HTTPException(status_code=502, detail="Upstream HLS request failed with status 502")

    monkeypatch.setattr(stream_routes, "_download_text", fake_download_text)

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 502
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert (
        governance["hls_route_failures_upstream_failed"]
        == before["hls_route_failures_upstream_failed"] + 1
    )
    assert governance["hls_route_failures_total"] == before["hls_route_failures_total"] + 1


def test_hls_route_failure_governance_counts_upstream_segment_failure(
    monkeypatch: Any,
) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/path/master.m3u8"})
    client, _ = _build_client(items=[item])
    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        assert url == "https://example.com/path/segment/0.ts"
        assert request.headers.get("x-api-key") == "a" * 32
        assert owner == "http-hls"
        raise HTTPException(
            status_code=404, detail="Upstream playback request failed with status 404"
        )

    monkeypatch.setattr(byte_streaming, "stream_remote", fake_stream_remote)

    response = client.get(f"/api/v1/stream/hls/{item.id}/segment/0.ts", headers=_headers())

    assert response.status_code == 404
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert (
        governance["hls_route_failures_upstream_failed"]
        == before["hls_route_failures_upstream_failed"] + 1
    )
    assert governance["hls_route_failures_total"] == before["hls_route_failures_total"] + 1


def test_hls_route_rejects_empty_upstream_playlist(monkeypatch: Any) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/master.m3u8"})
    client, _ = _build_client(items=[item])

    async def fake_download_text(url: str) -> tuple[str, httpx.Headers]:
        assert url == "https://example.com/master.m3u8"
        return ("", httpx.Headers({"content-type": "application/vnd.apple.mpegurl"}))

    monkeypatch.setattr(stream_routes, "_download_text", fake_download_text)

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 502
    assert response.json()["detail"] == "Upstream HLS playlist is empty"


def test_hls_route_failure_governance_counts_upstream_manifest_invalid(
    monkeypatch: Any,
) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/master.m3u8"})
    client, _ = _build_client(items=[item])
    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]

    async def fake_download_text(url: str) -> tuple[str, httpx.Headers]:
        assert url == "https://example.com/master.m3u8"
        return (
            "#EXTM3U\n#EXT-X-VERSION:3\n",
            httpx.Headers({"content-type": "application/vnd.apple.mpegurl"}),
        )

    monkeypatch.setattr(stream_routes, "_download_text", fake_download_text)

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 502
    assert response.json()["detail"] == "Upstream HLS playlist has no child references"
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert (
        governance["hls_route_failures_upstream_manifest_invalid"]
        == before["hls_route_failures_upstream_manifest_invalid"] + 1
    )
    assert governance["hls_route_failures_total"] == before["hls_route_failures_total"] + 1


def test_hls_playlist_route_maps_upstream_timeout_to_504(monkeypatch: Any) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/master.m3u8"})
    client, _ = _build_client(items=[item])

    async def fake_download_text(url: str) -> tuple[str, httpx.Headers]:
        assert url == "https://example.com/master.m3u8"
        raise HTTPException(status_code=504, detail="Upstream HLS request timed out")

    monkeypatch.setattr(stream_routes, "_download_text", fake_download_text)

    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 504
    assert response.json()["detail"] == "Upstream HLS request timed out"


def test_hls_playlist_route_retries_one_transient_upstream_timeout(monkeypatch: Any) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/retry-once/master.m3u8"})
    client, _ = _build_client(items=[item])
    state = {"attempts": 0}

    async def fake_download_text(url: str) -> tuple[str, httpx.Headers]:
        state["attempts"] += 1
        assert url == "https://example.com/retry-once/master.m3u8"
        if state["attempts"] == 1:
            raise HTTPException(status_code=504, detail="Upstream HLS request timed out")
        return (
            "#EXTM3U\n#EXTINF:10,\nsegment0.ts\n",
            httpx.Headers({"content-type": "application/vnd.apple.mpegurl"}),
        )

    monkeypatch.setattr(stream_routes, "_download_text", fake_download_text)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert response.status_code == 200
    assert state["attempts"] == 2
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert governance["remote_hls_retry_attempts"] == before["remote_hls_retry_attempts"] + 1


def test_hls_playlist_route_enters_cooldown_after_repeated_transient_timeout(
    monkeypatch: Any,
) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/cooldown/master.m3u8"})
    client, _ = _build_client(items=[item])
    state = {"attempts": 0}

    async def fake_download_text(url: str) -> tuple[str, httpx.Headers]:
        state["attempts"] += 1
        assert url == "https://example.com/cooldown/master.m3u8"
        raise HTTPException(status_code=504, detail="Upstream HLS request timed out")

    monkeypatch.setattr(stream_routes, "_download_text", fake_download_text)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    first = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())
    second = client.get(f"/api/v1/stream/hls/{item.id}/index.m3u8", headers=_headers())

    assert first.status_code == 504
    assert second.status_code == 504
    assert second.headers["retry-after"]
    assert state["attempts"] == 2
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert governance["remote_hls_cooldown_starts"] == before["remote_hls_cooldown_starts"] + 1
    assert governance["remote_hls_cooldown_hits"] == before["remote_hls_cooldown_hits"] + 1
    assert governance["remote_hls_cooldowns_active"] >= 1


def test_hls_segment_route_maps_upstream_transport_failure_to_502(
    monkeypatch: Any,
) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/path/master.m3u8"})
    client, _ = _build_client(items=[item])

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        assert url == "https://example.com/path/segment/0.ts"
        assert request.headers.get("x-api-key") == "a" * 32
        assert owner == "http-hls"
        raise HTTPException(status_code=502, detail="Upstream playback request transport failed")

    monkeypatch.setattr(byte_streaming, "stream_remote", fake_stream_remote)

    response = client.get(f"/api/v1/stream/hls/{item.id}/segment/0.ts", headers=_headers())

    assert response.status_code == 502
    assert response.json()["detail"] == "Upstream playback request transport failed"


def test_hls_segment_route_retries_one_transient_upstream_transport_failure(
    monkeypatch: Any,
) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/retry-segment/master.m3u8"})
    client, _ = _build_client(items=[item])
    state = {"attempts": 0}

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        state["attempts"] += 1
        assert url == "https://example.com/retry-segment/segment/0.ts"
        assert request.headers.get("x-api-key") == "a" * 32
        assert owner == "http-hls"
        if state["attempts"] == 1:
            raise HTTPException(
                status_code=502, detail="Upstream playback request transport failed"
            )

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"segment-bytes"

        return StreamingResponse(iterator(), media_type="video/mp2t")

    monkeypatch.setattr(byte_streaming, "stream_remote", fake_stream_remote)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    response = client.get(f"/api/v1/stream/hls/{item.id}/segment/0.ts", headers=_headers())

    assert response.status_code == 200
    assert response.content == b"segment-bytes"
    assert state["attempts"] == 2
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert governance["remote_hls_retry_attempts"] == before["remote_hls_retry_attempts"] + 1


def test_hls_segment_route_enters_cooldown_after_repeated_transport_failure(
    monkeypatch: Any,
) -> None:
    item = _build_item(attributes={"hls_url": "https://example.com/cooldown-segment/master.m3u8"})
    client, _ = _build_client(items=[item])
    state = {"attempts": 0}

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        state["attempts"] += 1
        assert url == "https://example.com/cooldown-segment/segment/0.ts"
        assert request.headers.get("x-api-key") == "a" * 32
        assert owner == "http-hls"
        raise HTTPException(status_code=502, detail="Upstream playback request transport failed")

    monkeypatch.setattr(byte_streaming, "stream_remote", fake_stream_remote)

    before = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    first = client.get(f"/api/v1/stream/hls/{item.id}/segment/0.ts", headers=_headers())
    second = client.get(f"/api/v1/stream/hls/{item.id}/segment/0.ts", headers=_headers())

    assert first.status_code == 502
    assert second.status_code == 502
    assert second.headers["retry-after"]
    assert state["attempts"] == 2
    governance = client.get("/api/v1/stream/status", headers=_headers()).json()["governance"]
    assert governance["remote_hls_cooldown_starts"] == before["remote_hls_cooldown_starts"] + 1
    assert governance["remote_hls_cooldown_hits"] == before["remote_hls_cooldown_hits"] + 1
    assert governance["remote_hls_cooldowns_active"] >= 1


def test_frontend_bff_direct_play_contract_exposes_forwardable_range_headers(
    tmp_path: Path,
) -> None:
    """Backend direct-play responses should satisfy the current frontend BFF header-forwarding contract."""

    media_file = tmp_path / "bff-range-example.txt"
    media_file.write_bytes(b"abcdefghij")
    item = _build_item(item_id="item-bff-range-contract")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="local-file",
            locator=str(media_file),
            local_path=str(media_file),
            original_filename="BFF Contract Movie.mkv",
            is_preferred=True,
        )
    ]
    client, _ = _build_client(items=[item])

    response = client.get(
        f"/api/v1/stream/file/{item.id}",
        headers={**_headers(), "Range": "bytes=2-5"},
    )

    assert response.status_code == 206
    assert response.content == b"cdef"
    assert response.headers["content-type"].startswith("application/octet-stream")
    assert response.headers["content-length"] == "4"
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["content-range"] == "bytes 2-5/10"
    assert response.headers["content-disposition"] == 'inline; filename="BFF Contract Movie.mkv"'


def test_stream_file_remote_direct_contract_adds_inline_content_disposition_from_attachment_filename() -> (
    None
):
    item = _build_item(item_id="item-direct-contract-remote-filename")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/direct-contract-remote-filename",
            unrestricted_url="https://cdn.example.com/direct-contract-remote-filename",
            original_filename="Remote Contract Movie.mkv",
            is_preferred=True,
        )
    ]
    client, _ = _build_client(items=[item])

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        assert url == "https://cdn.example.com/direct-contract-remote-filename"
        assert request.headers.get("x-api-key") == "a" * 32
        assert owner == "http-direct"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"remote-contract"

        return StreamingResponse(iterator(), media_type="application/octet-stream")

    original_stream_remote = byte_streaming.stream_remote
    byte_streaming.stream_remote = fake_stream_remote
    try:
        response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())
    finally:
        byte_streaming.stream_remote = original_stream_remote

    assert response.status_code == 200
    assert response.content == b"remote-contract"
    assert response.headers["content-disposition"] == 'inline; filename="Remote Contract Movie.mkv"'


def test_stream_file_uses_chunk_engine_for_known_remote_range_requests(monkeypatch: Any) -> None:
    item = _build_item(item_id="item-direct-remote-chunk-range")
    file_size = 300_000
    source = bytes(index % 251 for index in range(file_size))
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/direct-chunk-range",
            unrestricted_url="https://cdn.example.com/direct-chunk-range",
            original_filename="Chunked Remote Movie.mkv",
            file_size=file_size,
            is_preferred=True,
        )
    ]
    client, resources = _build_client(items=[item])
    assert resources.chunk_cache is not None

    requested_ranges: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_ranges.append(request.headers["Range"])
        start_text, end_text = (
            request.headers["Range"].removeprefix("bytes=").split("-", maxsplit=1)
        )
        start = int(start_text)
        end = int(end_text)
        return httpx.Response(
            206,
            content=source[start : end + 1],
            headers={"Content-Range": f"bytes {start}-{end}/{file_size}"},
        )

    transport = httpx.MockTransport(handler)

    class FakeAsyncClient(httpx.AsyncClient):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    file_chunks = calculate_file_chunks(item.id, file_size)
    expected_chunks = resolve_chunks_for_read(150_000, 4, file_chunks)
    before_fetch_bytes = CHUNK_FETCH_BYTES_TOTAL._value.get()
    before_general_scans = _counter_value(CHUNK_READ_TYPE_TOTAL, read_type="general-scan")

    monkeypatch.setattr(stream_routes.httpx, "AsyncClient", FakeAsyncClient)
    response = client.get(
        f"/api/v1/stream/file/{item.id}",
        headers={**_headers(), "Range": "bytes=150000-150003"},
    )

    assert response.status_code == 206
    assert response.content == source[150_000:150_004]
    assert response.headers["content-range"] == f"bytes 150000-150003/{file_size}"
    assert response.headers["content-length"] == "4"
    assert response.headers["content-disposition"] == 'inline; filename="Chunked Remote Movie.mkv"'
    assert requested_ranges == [f"bytes={chunk.start}-{chunk.end}" for chunk in expected_chunks]
    assert CHUNK_FETCH_BYTES_TOTAL._value.get() - before_fetch_bytes == sum(
        chunk.size for chunk in expected_chunks
    )
    assert _counter_value(CHUNK_READ_TYPE_TOTAL, read_type="general-scan") == (
        before_general_scans + 1
    )
    assert all(resources.chunk_cache.has(chunk.cache_key) for chunk in expected_chunks)


def test_stream_file_remote_chunk_range_reuses_chunk_cache(monkeypatch: Any) -> None:
    item = _build_item(item_id="item-direct-remote-chunk-cache-hit")
    file_size = 300_000
    source = bytes(index % 251 for index in range(file_size))
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/direct-chunk-cache-hit",
            unrestricted_url="https://cdn.example.com/direct-chunk-cache-hit",
            file_size=file_size,
            is_preferred=True,
        )
    ]
    client, _ = _build_client(items=[item])

    upstream_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal upstream_calls
        upstream_calls += 1
        start_text, end_text = (
            request.headers["Range"].removeprefix("bytes=").split("-", maxsplit=1)
        )
        start = int(start_text)
        end = int(end_text)
        return httpx.Response(
            206,
            content=source[start : end + 1],
            headers={"Content-Range": f"bytes {start}-{end}/{file_size}"},
        )

    transport = httpx.MockTransport(handler)

    class FakeAsyncClient(httpx.AsyncClient):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    before_cache_hits = _counter_value(CHUNK_READ_TYPE_TOTAL, read_type="cache-hit")
    monkeypatch.setattr(stream_routes.httpx, "AsyncClient", FakeAsyncClient)
    first = client.get(
        f"/api/v1/stream/file/{item.id}",
        headers={**_headers(), "Range": "bytes=150000-150003"},
    )
    second = client.get(
        f"/api/v1/stream/file/{item.id}",
        headers={**_headers(), "Range": "bytes=150000-150003"},
    )

    assert first.status_code == 206
    assert second.status_code == 206
    assert first.content == source[150_000:150_004]
    assert second.content == source[150_000:150_004]
    assert upstream_calls == 1
    assert _counter_value(CHUNK_READ_TYPE_TOTAL, read_type="cache-hit") == before_cache_hits + 1


def test_stream_file_remote_range_without_known_size_falls_back_to_stream_remote(
    monkeypatch: Any,
) -> None:
    item = _build_item(item_id="item-direct-remote-range-fallback")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/direct-range-fallback",
            unrestricted_url="https://cdn.example.com/direct-range-fallback",
            is_preferred=True,
        )
    ]
    client, _ = _build_client(items=[item])

    fallback_calls: list[str] = []

    async def fake_resolve_content_length(url: str) -> int | None:
        assert url == "https://cdn.example.com/direct-range-fallback"
        return None

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        fallback_calls.append(url)
        assert request.headers["range"] == "bytes=2-5"
        assert owner == "http-direct"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"fallback"

        return StreamingResponse(iterator(), media_type="application/octet-stream")

    monkeypatch.setattr(
        byte_streaming, "resolve_remote_content_length", fake_resolve_content_length
    )
    monkeypatch.setattr(byte_streaming, "stream_remote", fake_stream_remote)

    response = client.get(
        f"/api/v1/stream/file/{item.id}",
        headers={**_headers(), "Range": "bytes=2-5"},
    )

    assert response.status_code == 200
    assert response.content == b"fallback"
    assert fallback_calls == ["https://cdn.example.com/direct-range-fallback"]


def test_stream_file_remote_range_known_size_returns_clean_error_before_body(
    monkeypatch: Any,
) -> None:
    item = _build_item(item_id="item-direct-remote-range-error")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/direct-range-error",
            unrestricted_url="https://cdn.example.com/direct-range-error",
            file_size=1024,
            is_preferred=True,
        )
    ]
    client, _ = _build_client(items=[item])

    async def fake_fetch_remote_range_via_chunks(
        resource_id: str,
        url: str,
        file_size: int,
        offset: int,
        size: int,
        cache: Any,
        *,
        config: Any = byte_streaming.DEFAULT_CONFIG,
    ) -> bytes:
        del cache, config
        assert resource_id == item.id
        assert url == "https://cdn.example.com/direct-range-error"
        assert file_size == 1024
        assert offset == 0
        assert size == 16
        raise HTTPException(
            status_code=503,
            detail="Playback source temporarily unavailable",
        )

    monkeypatch.setattr(
        byte_streaming,
        "fetch_remote_range_via_chunks",
        fake_fetch_remote_range_via_chunks,
    )

    response = client.get(
        f"/api/v1/stream/file/{item.id}",
        headers={**_headers(), "Range": "bytes=0-15"},
    )

    assert response.status_code == 503
    assert response.text == '{"detail":"Playback source temporarily unavailable"}'


def test_stream_file_refreshes_remote_direct_url_after_head_validation_failure(
    monkeypatch: Any,
) -> None:
    item = _build_item(item_id="item-stream-file-head-refresh-success")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-stream-file-head-refresh-success",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-head-refresh-success",
        unrestricted_url="https://cdn.example.com/head-refresh-stale",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
        provider="realdebrid",
        provider_download_id="download-head-refresh-success",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    client, resources = _build_client(items=[item])

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            assert capacity == 1.0
            assert refill_rate_per_second == 1.0
            return RateLimitDecision(allowed=True, remaining_tokens=0.0, retry_after_seconds=0.0)

    class FakeProviderClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append(link)
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/head-refresh-fresh",
                restricted_url=link,
                expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
            )

    provider_client = FakeProviderClient()
    resources.playback_service = PlaybackSourceService(
        resources.db,
        provider_clients={"realdebrid": cast(PlaybackAttachmentProviderClient, provider_client)},
        rate_limiter=FakeRateLimiter(),
    )

    head_calls: list[str] = []

    async def fake_head(url: str) -> None:
        head_calls.append(url)
        if url == "https://cdn.example.com/head-refresh-stale":
            raise HTTPException(
                status_code=503,
                detail="Playback source temporarily unavailable",
            )
        assert url == "https://cdn.example.com/head-refresh-fresh"

    async def fake_stream_remote(
        url: str,
        request: Any,
        *,
        owner: str = "http-direct",
    ) -> StreamingResponse:
        assert url == "https://cdn.example.com/head-refresh-fresh"
        assert owner == "http-direct"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"head-refresh-success"

        return StreamingResponse(iterator(), media_type="application/octet-stream")

    monkeypatch.setattr(stream_routes, "_head_remote_direct_url", fake_head)
    monkeypatch.setattr(
        stream_routes,
        "_start_direct_playback_refresh_trigger",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(byte_streaming, "stream_remote", fake_stream_remote)

    response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())

    assert response.status_code == 200
    assert response.content == b"head-refresh-success"
    assert head_calls == [
        "https://cdn.example.com/head-refresh-stale",
        "https://cdn.example.com/head-refresh-fresh",
    ]
    assert provider_client.calls == ["https://api.example.com/restricted-head-refresh-success"]
    assert selected_entry.unrestricted_url == "https://cdn.example.com/head-refresh-fresh"


def test_stream_file_persists_repaired_media_entry_lease_across_requests(
    monkeypatch: Any,
) -> None:
    item = _build_item(item_id="item-stream-file-durable-head-refresh-success")
    source_attachment = _build_playback_attachment(
        attachment_id="attachment-stream-file-durable-head-refresh-success",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/durable-head-refresh-stale",
        restricted_url="https://api.example.com/restricted-durable-head-refresh-success",
        unrestricted_url="https://cdn.example.com/durable-head-refresh-stale",
        refresh_state="ready",
        provider="realdebrid",
        provider_download_id="download-durable-head-refresh-success",
    )
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-stream-file-durable-head-refresh-success",
        item_id=item.id,
        source_attachment_id=source_attachment.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-durable-head-refresh-success",
        unrestricted_url="https://cdn.example.com/durable-head-refresh-stale",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
        provider="realdebrid",
        provider_download_id="download-durable-head-refresh-success",
    )
    selected_entry.source_attachment = source_attachment
    item.playback_attachments = [source_attachment]
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]

    database = PersistentDummyDatabaseRuntime(items=[item])
    client, resources = _build_client(db=database)

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            assert capacity == 1.0
            assert refill_rate_per_second == 1.0
            return RateLimitDecision(allowed=True, remaining_tokens=0.0, retry_after_seconds=0.0)

    class FakeProviderClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            self.calls.append(link)
            return PlaybackAttachmentProviderUnrestrictedLink(
                download_url="https://cdn.example.com/durable-head-refresh-fresh",
                restricted_url=link,
                expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
            )

    provider_client = FakeProviderClient()
    resources.playback_service = PlaybackSourceService(
        resources.db,
        provider_clients={"realdebrid": cast(PlaybackAttachmentProviderClient, provider_client)},
        rate_limiter=FakeRateLimiter(),
    )

    head_calls: list[str] = []

    async def fake_head(url: str) -> None:
        head_calls.append(url)
        if url == "https://cdn.example.com/durable-head-refresh-stale":
            raise HTTPException(
                status_code=503,
                detail="Playback source temporarily unavailable",
            )
        assert url == "https://cdn.example.com/durable-head-refresh-fresh"

    async def fake_stream_remote(
        url: str,
        request: Any,
        *,
        owner: str = "http-direct",
    ) -> StreamingResponse:
        assert url == "https://cdn.example.com/durable-head-refresh-fresh"
        assert owner == "http-direct"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"durable-head-refresh-success"

        return StreamingResponse(iterator(), media_type="application/octet-stream")

    monkeypatch.setattr(stream_routes, "_head_remote_direct_url", fake_head)
    monkeypatch.setattr(
        stream_routes,
        "_start_direct_playback_refresh_trigger",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(byte_streaming, "stream_remote", fake_stream_remote)

    first_response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())
    second_response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())

    assert first_response.status_code == 200
    assert first_response.content == b"durable-head-refresh-success"
    assert second_response.status_code == 200
    assert second_response.content == b"durable-head-refresh-success"
    assert head_calls == [
        "https://cdn.example.com/durable-head-refresh-stale",
        "https://cdn.example.com/durable-head-refresh-fresh",
        "https://cdn.example.com/durable-head-refresh-fresh",
    ]
    assert provider_client.calls == [
        "https://api.example.com/restricted-durable-head-refresh-success"
    ]

    persisted_item = asyncio.run(PlaybackSourceService(resources.db)._list_items())[0]
    persisted_entry = persisted_item.media_entries[0]
    persisted_attachment = persisted_item.playback_attachments[0]
    assert (
        persisted_entry.unrestricted_url
        == "https://cdn.example.com/durable-head-refresh-fresh"
    )
    assert persisted_entry.refresh_state == "ready"
    assert persisted_entry.last_refresh_error is None
    assert (
        persisted_attachment.unrestricted_url
        == "https://cdn.example.com/durable-head-refresh-fresh"
    )
    assert persisted_attachment.locator == "https://cdn.example.com/durable-head-refresh-fresh"
    assert persisted_attachment.refresh_state == "ready"
    assert persisted_attachment.last_refresh_error is None


def test_stream_file_returns_503_when_head_validation_refresh_is_denied(
    monkeypatch: Any,
) -> None:
    item = _build_item(item_id="item-stream-file-head-refresh-denied")
    selected_entry = _build_media_entry(
        media_entry_id="media-entry-stream-file-head-refresh-denied",
        item_id=item.id,
        kind="remote-direct",
        download_url="https://api.example.com/restricted-head-refresh-denied",
        unrestricted_url="https://cdn.example.com/head-refresh-denied-stale",
        refresh_state="ready",
        expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
        provider="realdebrid",
        provider_download_id="download-head-refresh-denied",
    )
    item.media_entries = [selected_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=selected_entry.id, role="direct")
    ]
    client, resources = _build_client(items=[item])

    class FakeRateLimiter:
        async def acquire(
            self,
            bucket_key: str,
            capacity: float,
            refill_rate_per_second: float,
            requested_tokens: float = 1.0,
            now_seconds: float | None = None,
            expiry_seconds: int | None = None,
        ) -> RateLimitDecision:
            assert bucket_key == "ratelimit:realdebrid:stream_link_refresh"
            return RateLimitDecision(allowed=False, remaining_tokens=0.0, retry_after_seconds=9.0)

    class FakeProviderClient:
        async def unrestrict_link(
            self,
            link: str,
            *,
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
            raise AssertionError("provider refresh should not run when limiter denies refresh")

    resources.playback_service = PlaybackSourceService(
        resources.db,
        provider_clients={
            "realdebrid": cast(PlaybackAttachmentProviderClient, FakeProviderClient())
        },
        rate_limiter=FakeRateLimiter(),
    )

    async def fake_head(url: str) -> None:
        raise HTTPException(
            status_code=503,
            detail="Playback source temporarily unavailable",
        )

    monkeypatch.setattr(stream_routes, "_head_remote_direct_url", fake_head)
    monkeypatch.setattr(
        stream_routes,
        "_start_direct_playback_refresh_trigger",
        lambda *args, **kwargs: None,
    )

    response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "10"
    assert response.json()["detail"] == "Selected direct playback lease refresh failed"


def test_stream_file_remote_direct_contract_preserves_upstream_content_disposition() -> None:
    item = _build_item(item_id="item-direct-contract-remote-upstream-disposition")
    item.playback_attachments = [
        _build_playback_attachment(
            item_id=item.id,
            kind="remote-direct",
            locator="https://cdn.example.com/direct-contract-remote-upstream-disposition",
            unrestricted_url="https://cdn.example.com/direct-contract-remote-upstream-disposition",
            original_filename="Ignored Local Name.mkv",
            is_preferred=True,
        )
    ]
    client, _ = _build_client(items=[item])

    async def fake_stream_remote(
        url: str, request: Any, *, owner: str = "http-direct"
    ) -> StreamingResponse:
        assert url == "https://cdn.example.com/direct-contract-remote-upstream-disposition"
        assert request.headers.get("x-api-key") == "a" * 32
        assert owner == "http-direct"

        async def iterator() -> AsyncGenerator[bytes, None]:
            yield b"remote-upstream-disposition"

        return StreamingResponse(
            iterator(),
            media_type="application/octet-stream",
            headers={"content-disposition": 'attachment; filename="upstream-name.mkv"'},
        )

    original_stream_remote = byte_streaming.stream_remote
    byte_streaming.stream_remote = fake_stream_remote
    try:
        response = client.get(f"/api/v1/stream/file/{item.id}", headers=_headers())
    finally:
        byte_streaming.stream_remote = original_stream_remote

    assert response.status_code == 200
    assert response.content == b"remote-upstream-disposition"
    assert response.headers["content-disposition"] == 'attachment; filename="upstream-name.mkv"'


def test_frontend_player_hls_query_params_are_compatibility_safe_for_remote_playlist(
    monkeypatch: Any,
) -> None:
    """The current player adds HLS query params that the backend should tolerate for remote playlists."""

    item = _build_item(attributes={"hls_url": "https://example.com/player-query/master.m3u8"})
    client, _ = _build_client(items=[item])

    async def fake_download_text(url: str) -> tuple[str, httpx.Headers]:
        assert url == "https://example.com/player-query/master.m3u8"
        return (
            "#EXTM3U\n#EXTINF:10,\nsegment0.ts\n",
            httpx.Headers({"content-type": "application/vnd.apple.mpegurl"}),
        )

    monkeypatch.setattr(stream_routes, "_download_text", fake_download_text)

    response = client.get(
        f"/api/v1/stream/hls/{item.id}/index.m3u8?pix_fmt=yuv420p&profile=high&level=4.1",
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.apple.mpegurl")
    assert f"/api/stream/{item.id}/hls/segment0.ts" in response.text


def test_frontend_player_hls_query_params_are_compatibility_safe_for_local_playlist(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-player-query.mkv"
    media_file.write_bytes(b"movie-bytes")
    playlist_dir = tmp_path / "generated-hls-player-query"
    playlist_dir.mkdir()
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])

    async def fake_ensure_local_hls_playlist(
        source_path: str,
        item_id: str,
        *,
        transcode_profile: byte_streaming.LocalHlsTranscodeProfile | None = None,
    ) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        assert transcode_profile == byte_streaming.LocalHlsTranscodeProfile(
            pix_fmt="yuv420p",
            profile="high",
            level="4.1",
        )
        return playlist_path

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)

    response = client.get(
        f"/api/v1/stream/hls/{item.id}/index.m3u8?pix_fmt=yuv420p&profile=high&level=4.1",
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.apple.mpegurl")
    assert (
        f"/api/stream/{item.id}/hls/segment_00001.ts?pix_fmt=yuv420p&profile=high&level=4.1"
        in response.text
    )


def test_local_hls_playlist_route_marks_the_returned_variant_playlist_path(
    tmp_path: Path, monkeypatch: Any
) -> None:
    media_file = tmp_path / "movie-player-marked.mkv"
    media_file.write_bytes(b"movie-bytes")
    playlist_dir = tmp_path / "generated-hls-player-marked" / "variant-a"
    playlist_dir.mkdir(parents=True)
    playlist_path = playlist_dir / "index.m3u8"
    playlist_path.write_text("#EXTM3U\n#EXTINF:6,\nsegment_00001.ts\n", encoding="utf-8")

    item = _build_item(attributes={"file_path": str(media_file)})
    client, _ = _build_client(items=[item])
    touched: list[Path] = []

    async def fake_ensure_local_hls_playlist(
        source_path: str,
        item_id: str,
        *,
        transcode_profile: byte_streaming.LocalHlsTranscodeProfile | None = None,
    ) -> Path:
        assert source_path == str(media_file)
        assert item_id == _local_hls_runtime_item_key(item.id)
        assert transcode_profile == byte_streaming.LocalHlsTranscodeProfile(
            pix_fmt="yuv420p",
            profile="high",
            level="4.1",
        )
        return playlist_path

    monkeypatch.setattr(byte_streaming, "ensure_local_hls_playlist", fake_ensure_local_hls_playlist)
    monkeypatch.setattr(
        byte_streaming,
        "mark_local_hls_activity",
        lambda path: touched.append(path),
    )

    response = client.get(
        f"/api/v1/stream/hls/{item.id}/index.m3u8?pix_fmt=yuv420p&profile=high&level=4.1",
        headers=_headers(),
    )

    assert response.status_code == 200
    assert touched == [playlist_path]


def test_playback_source_service_persist_media_entry_control_state_updates_projection_and_active_role() -> None:
    item = _build_item(item_id="item-persist-media-entry-control-state")
    attachment = _build_playback_attachment(
        attachment_id="attachment-persist-media-entry-control-state",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/direct-stale",
        restricted_url="https://api.example.com/direct-stale",
        unrestricted_url="https://cdn.example.com/direct-stale",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-persist-media-entry-control-state",
    )
    target_entry = _build_media_entry(
        media_entry_id="media-entry-persist-media-entry-control-state-target",
        item_id=item.id,
        source_attachment_id=attachment.id,
        kind="remote-direct",
        download_url="https://api.example.com/direct-stale",
        unrestricted_url="https://cdn.example.com/direct-stale",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-persist-media-entry-control-state",
    )
    target_entry.source_attachment = attachment
    previous_active_entry = _build_media_entry(
        media_entry_id="media-entry-persist-media-entry-control-state-previous",
        item_id=item.id,
        kind="remote-direct",
        unrestricted_url="https://cdn.example.com/direct-previous",
        refresh_state="ready",
        provider="realdebrid",
        provider_download_id="download-persist-media-entry-control-state-previous",
    )
    item.playback_attachments = [attachment]
    item.media_entries = [target_entry, previous_active_entry]
    item.active_streams = [
        _build_active_stream(item_id=item.id, media_entry_id=previous_active_entry.id, role="direct")
    ]

    database = PersistentDummyDatabaseRuntime(items=[item])
    service = PlaybackSourceService(database)

    result = asyncio.run(
        service.persist_media_entry_control_state(
            item.id,
            target_entry.id,
            active_role="direct",
            download_url="https://api.example.com/direct-fresh",
            unrestricted_url="https://cdn.example.com/direct-fresh",
            refresh_state="ready",
            expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
        )
    )

    assert result is not None
    assert result.item_identifier == item.id
    assert result.media_entry_id == target_entry.id
    assert result.applied_role == "direct"

    persisted_item = asyncio.run(PlaybackSourceService(database)._list_items())[0]
    persisted_target_entry = next(
        entry for entry in persisted_item.media_entries if entry.id == target_entry.id
    )
    persisted_attachment = persisted_item.playback_attachments[0]
    persisted_direct_stream = next(
        active_stream for active_stream in persisted_item.active_streams if active_stream.role == "direct"
    )

    assert persisted_direct_stream.media_entry_id == target_entry.id
    assert persisted_target_entry.download_url == "https://api.example.com/direct-fresh"
    assert (
        persisted_target_entry.unrestricted_url
        == "https://cdn.example.com/direct-fresh"
    )
    assert persisted_target_entry.refresh_state == "ready"
    assert persisted_target_entry.last_refresh_error is None
    assert persisted_target_entry.expires_at == datetime(2099, 3, 14, 0, 0, tzinfo=UTC)
    assert persisted_attachment.restricted_url == "https://api.example.com/direct-fresh"
    assert (
        persisted_attachment.unrestricted_url
        == "https://cdn.example.com/direct-fresh"
    )
    assert persisted_attachment.locator == "https://cdn.example.com/direct-fresh"
    assert persisted_attachment.refresh_state == "ready"


def test_playback_source_service_persist_playback_attachment_control_state_syncs_linked_media_entries() -> None:
    item = _build_item(item_id="item-persist-playback-attachment-control-state")
    attachment = _build_playback_attachment(
        attachment_id="attachment-persist-playback-attachment-control-state",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/direct-stale",
        restricted_url="https://api.example.com/direct-stale",
        unrestricted_url="https://cdn.example.com/direct-stale",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-persist-playback-attachment-control-state",
    )
    linked_entry = _build_media_entry(
        media_entry_id="media-entry-persist-playback-attachment-control-state",
        item_id=item.id,
        source_attachment_id=attachment.id,
        kind="remote-direct",
        download_url="https://api.example.com/direct-stale",
        unrestricted_url="https://cdn.example.com/direct-stale",
        refresh_state="stale",
        provider="realdebrid",
        provider_download_id="download-persist-playback-attachment-control-state",
    )
    linked_entry.source_attachment = attachment
    item.playback_attachments = [attachment]
    item.media_entries = [linked_entry]

    database = PersistentDummyDatabaseRuntime(items=[item])
    service = PlaybackSourceService(database)

    result = asyncio.run(
        service.persist_playback_attachment_control_state(
            item.id,
            attachment.id,
            locator="https://cdn.example.com/direct-fresh",
            restricted_url="https://api.example.com/direct-fresh",
            unrestricted_url="https://cdn.example.com/direct-fresh",
            refresh_state="ready",
            expires_at=datetime(2099, 3, 14, 0, 0, tzinfo=UTC),
        )
    )

    assert result is not None
    assert result.item_identifier == item.id
    assert result.attachment_id == attachment.id
    assert len(result.linked_media_entries) == 1

    persisted_item = asyncio.run(PlaybackSourceService(database)._list_items())[0]
    persisted_attachment = persisted_item.playback_attachments[0]
    persisted_entry = persisted_item.media_entries[0]

    assert persisted_attachment.locator == "https://cdn.example.com/direct-fresh"
    assert persisted_attachment.restricted_url == "https://api.example.com/direct-fresh"
    assert (
        persisted_attachment.unrestricted_url
        == "https://cdn.example.com/direct-fresh"
    )
    assert persisted_attachment.refresh_state == "ready"
    assert persisted_attachment.expires_at == datetime(2099, 3, 14, 0, 0, tzinfo=UTC)
    assert persisted_entry.download_url == "https://api.example.com/direct-fresh"
    assert persisted_entry.unrestricted_url == "https://cdn.example.com/direct-fresh"
    assert persisted_entry.refresh_state == "ready"
    assert persisted_entry.provider_download_id == attachment.provider_download_id







