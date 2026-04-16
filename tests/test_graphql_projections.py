"""GraphQL projection query tests for the dual-surface API strategy."""

from __future__ import annotations

import fnmatch
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

from arq.constants import in_progress_key_prefix, result_key_prefix, retry_key_prefix
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.core.runtime_lifecycle import (
    RuntimeLifecycleHealth,
    RuntimeLifecyclePhase,
    RuntimeLifecycleState,
)
from filmu_py.db.models import StreamORM
from filmu_py.graphql import GraphQLPluginRegistry, create_graphql_router
from filmu_py.plugins import TestPluginContext
from filmu_py.plugins.builtins import register_builtin_plugins
from filmu_py.plugins.manifest import PluginManifest
from filmu_py.plugins.registry import PluginCapabilityKind, PluginRegistry
from filmu_py.resources import AppResources
from filmu_py.services.media import (
    CalendarProjectionRecord,
    CalendarReleaseDataRecord,
    MediaItemRecord,
    MediaItemSpecializationRecord,
    MediaItemSummaryRecord,
    ParentIdsRecord,
    RecoveryMechanism,
    RecoveryPlanRecord,
    RecoveryTargetStage,
    StatsProjection,
)
from filmu_py.services.playback import (
    DirectPlaybackRefreshControlPlaneTriggerResult,
    DirectPlaybackRefreshRecommendation,
    DirectPlaybackRefreshScheduleRequest,
    DirectPlaybackRefreshSchedulingResult,
    HlsFailedLeaseRefreshControlPlaneTriggerResult,
    HlsFailedLeaseRefreshResult,
    HlsRestrictedFallbackRefreshControlPlaneTriggerResult,
    HlsRestrictedFallbackRefreshResult,
    MediaEntryLeaseRefreshExecution,
    PersistedMediaEntryControlMutationResult,
    PersistedPlaybackAttachmentControlMutationResult,
)
from filmu_py.services.vfs_catalog import (
    VfsCatalogCorrelationKeys,
    VfsCatalogDirectoryEntry,
    VfsCatalogEntry,
    VfsCatalogFileEntry,
    VfsCatalogSnapshot,
    VfsCatalogStats,
)
from filmu_py.state.item import ItemState


class DummyRedis:
    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None


@dataclass
class FakeOperatorRedis(DummyRedis):
    zsets: dict[str, list[tuple[str, float]]] = field(default_factory=dict)
    lists: dict[str, list[str]] = field(default_factory=dict)
    keys: set[str] = field(default_factory=set)

    def zcard(self, name: str) -> int:
        return len(self.zsets.get(name, []))

    def zcount(self, name: str, minimum: str | int, maximum: str | int) -> int:
        return len(self._filter_scores(self.zsets.get(name, []), minimum, maximum))

    def zrangebyscore(
        self,
        name: str,
        minimum: str | int,
        maximum: str | int,
        *,
        start: int = 0,
        num: int = 1,
        withscores: bool = True,
    ) -> list[tuple[str, float]]:
        _ = withscores
        rows = self._filter_scores(self.zsets.get(name, []), minimum, maximum)
        ordered = sorted(rows, key=lambda item: item[1])
        return ordered[start : start + num]

    def scan_iter(self, *, match: str) -> list[str]:
        return [key for key in sorted(self.keys) if fnmatch.fnmatch(key, match)]

    def llen(self, name: str) -> int:
        return len(self.lists.get(name, []))

    def lrange(self, name: str, start: int, stop: int) -> list[str]:
        values = list(self.lists.get(name, []))
        if not values:
            return []
        end = None if stop == -1 else stop + 1
        return values[start:end]

    def lpush(self, name: str, value: str) -> int:
        values = self.lists.setdefault(name, [])
        values.insert(0, value)
        return len(values)

    def ltrim(self, name: str, start: int, stop: int) -> bool:
        values = list(self.lists.get(name, []))
        end = None if stop == -1 else stop + 1
        self.lists[name] = values[start:end]
        return True

    @staticmethod
    def _filter_scores(
        rows: list[tuple[str, float]],
        minimum: str | int,
        maximum: str | int,
    ) -> list[tuple[str, float]]:
        def _matches_lower(score: float, boundary: str | int) -> bool:
            if boundary == "-inf":
                return True
            if isinstance(boundary, str) and boundary.startswith("("):
                return score > float(boundary[1:])
            return score >= float(boundary)

        def _matches_upper(score: float, boundary: str | int) -> bool:
            if boundary == "+inf":
                return True
            if isinstance(boundary, str) and boundary.startswith("("):
                return score < float(boundary[1:])
            return score <= float(boundary)

        return [
            row for row in rows if _matches_lower(row[1], minimum) and _matches_upper(row[1], maximum)
        ]


class DummyDatabaseRuntime:
    async def dispose(self) -> None:
        return None


@dataclass
class FakeMediaService:
    calendar_entries: list[CalendarProjectionRecord] = field(default_factory=list)
    stats: StatsProjection = field(
        default_factory=lambda: StatsProjection(
            total_items=0,
            completed_items=0,
            failed_items=0,
            incomplete_items=0,
            movies=0,
            shows=0,
            episodes=0,
        )
    )
    detail: MediaItemSummaryRecord | None = None
    stream_candidates: list[StreamORM] = field(default_factory=list)
    item_records: list[MediaItemRecord] = field(default_factory=list)
    recovery_plan: RecoveryPlanRecord | None = None

    async def get_calendar(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[CalendarProjectionRecord]:
        _ = (start_date, end_date)
        return list(self.calendar_entries)

    async def get_stats(self) -> StatsProjection:
        return self.stats

    async def get_item_detail(
        self,
        item_identifier: str,
        *,
        media_type: str,
        extended: bool = False,
    ) -> MediaItemSummaryRecord | None:
        _ = (item_identifier, media_type, extended)
        return self.detail

    async def search_items(
        self,
        *,
        limit: int = 24,
        page: int = 1,
        item_types: list[str] | None = None,
        states: list[str] | None = None,
        sort: list[str] | None = None,
        search: str | None = None,
        extended: bool = False,
    ) -> object:
        _ = (limit, page, item_types, states, sort, search, extended)

        @dataclass
        class _Page:
            items: list[MediaItemSummaryRecord]

        if self.detail is not None:
            return _Page(items=[self.detail])
        return _Page(
            items=[
                MediaItemSummaryRecord(
                    id=record.id,
                    type=str(record.attributes.get("item_type", "unknown")),
                    title=record.title,
                    state=record.state.value,
                    tmdb_id=(
                        str(record.attributes["tmdb_id"])
                        if record.attributes.get("tmdb_id") is not None
                        else None
                    ),
                    tvdb_id=(
                        str(record.attributes["tvdb_id"])
                        if record.attributes.get("tvdb_id") is not None
                        else None
                    ),
                    external_ref=record.external_ref,
                    aired_at=(
                        str(record.attributes["aired_at"])
                        if record.attributes.get("aired_at") is not None
                        else None
                    ),
                    poster_path=(
                        str(record.attributes["poster_path"])
                        if record.attributes.get("poster_path") is not None
                        else None
                    ),
                    specialization=MediaItemSpecializationRecord(
                        item_type=str(record.attributes.get("item_type", "unknown")),
                        tmdb_id=(
                            str(record.attributes["tmdb_id"])
                            if record.attributes.get("tmdb_id") is not None
                            else None
                        ),
                        tvdb_id=(
                            str(record.attributes["tvdb_id"])
                            if record.attributes.get("tvdb_id") is not None
                            else None
                        ),
                        imdb_id=(
                            str(record.attributes["imdb_id"])
                            if record.attributes.get("imdb_id") is not None
                            else None
                        ),
                        parent_ids=(
                            ParentIdsRecord(
                                tmdb_id=(
                                    str(cast(dict[str, object], record.attributes["parent_ids"]).get("tmdb_id"))
                                    if cast(dict[str, object], record.attributes["parent_ids"]).get("tmdb_id")
                                    is not None
                                    else None
                                ),
                                tvdb_id=(
                                    str(cast(dict[str, object], record.attributes["parent_ids"]).get("tvdb_id"))
                                    if cast(dict[str, object], record.attributes["parent_ids"]).get("tvdb_id")
                                    is not None
                                    else None
                                ),
                            )
                            if isinstance(record.attributes.get("parent_ids"), dict)
                            else None
                        ),
                        show_title=(
                            str(record.attributes["show_title"])
                            if record.attributes.get("show_title") is not None
                            else None
                        ),
                        season_number=(
                            int(record.attributes["season_number"])
                            if record.attributes.get("season_number") is not None
                            else None
                        ),
                        episode_number=(
                            int(record.attributes["episode_number"])
                            if record.attributes.get("episode_number") is not None
                            else None
                        ),
                    ),
                )
                for record in self.item_records[:limit]
            ]
        )

    async def get_stream_candidates(self, *, media_item_id: str) -> list[StreamORM]:
        _ = media_item_id
        return list(self.stream_candidates)

    async def get_recovery_plan(self, *, media_item_id: str) -> RecoveryPlanRecord | None:
        _ = media_item_id
        return self.recovery_plan

    async def list_items(self, limit: int = 100) -> list[MediaItemRecord]:
        return list(self.item_records[:limit])

    async def get_item(self, item_id: str) -> MediaItemRecord | None:
        return next((record for record in self.item_records if record.id == item_id), None)


@dataclass
class FakeVfsCatalogSupplier:
    snapshot: VfsCatalogSnapshot | None = None
    snapshots_by_generation: dict[int, VfsCatalogSnapshot] = field(default_factory=dict)

    async def build_snapshot(self) -> VfsCatalogSnapshot:
        if self.snapshot is None:
            raise AssertionError("test did not configure a VFS snapshot")
        return self.snapshot

    async def snapshot_for_generation(self, generation_id: int) -> VfsCatalogSnapshot | None:
        return self.snapshots_by_generation.get(generation_id)


@dataclass
class FakeReplayBackplane:
    pending_count: int = 0
    oldest_event_id: str | None = None
    latest_event_id: str | None = None
    consumer_counts: dict[str, int] = field(default_factory=dict)

    async def pending_summary(self, *, group_name: str) -> object:
        _ = group_name
        return SimpleNamespace(
            pending_count=self.pending_count,
            oldest_event_id=self.oldest_event_id,
            latest_event_id=self.latest_event_id,
            consumer_counts=dict(self.consumer_counts),
        )


@dataclass
class FakePlaybackTriggerController:
    result: object
    triggered_item_ids: list[str] = field(default_factory=list)

    async def trigger(self, item_identifier: str, *, at: datetime | None = None) -> object:
        _ = at
        self.triggered_item_ids.append(item_identifier)
        return self.result


@dataclass
class FakePlaybackService:
    stale_result: bool = True
    stale_item_ids: list[str] = field(default_factory=list)
    attachment_persist_result: PersistedPlaybackAttachmentControlMutationResult | None = None
    attachment_persist_calls: list[dict[str, object]] = field(default_factory=list)
    persist_result: PersistedMediaEntryControlMutationResult | None = None
    persist_calls: list[dict[str, object]] = field(default_factory=list)

    async def mark_selected_hls_media_entry_stale(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> bool:
        _ = at
        self.stale_item_ids.append(item_identifier)
        return self.stale_result

    async def persist_media_entry_control_state(
        self,
        item_identifier: str,
        media_entry_id: str,
        *,
        active_role: str | None = None,
        local_path: str | None = None,
        download_url: str | None = None,
        unrestricted_url: str | None = None,
        refresh_state: str | None = None,
        last_refresh_error: str | None = None,
        expires_at: datetime | None = None,
        at: datetime | None = None,
    ) -> PersistedMediaEntryControlMutationResult | None:
        _ = at
        self.persist_calls.append(
            {
                "item_identifier": item_identifier,
                "media_entry_id": media_entry_id,
                "active_role": active_role,
                "local_path": local_path,
                "download_url": download_url,
                "unrestricted_url": unrestricted_url,
                "refresh_state": refresh_state,
                "last_refresh_error": last_refresh_error,
                "expires_at": expires_at,
            }
        )
        return self.persist_result

    async def persist_playback_attachment_control_state(
        self,
        item_identifier: str,
        attachment_id: str,
        *,
        locator: str | None = None,
        local_path: str | None = None,
        restricted_url: str | None = None,
        unrestricted_url: str | None = None,
        refresh_state: str | None = None,
        last_refresh_error: str | None = None,
        expires_at: datetime | None = None,
        at: datetime | None = None,
    ) -> PersistedPlaybackAttachmentControlMutationResult | None:
        _ = at
        self.attachment_persist_calls.append(
            {
                "item_identifier": item_identifier,
                "attachment_id": attachment_id,
                "locator": locator,
                "local_path": local_path,
                "restricted_url": restricted_url,
                "unrestricted_url": unrestricted_url,
                "refresh_state": refresh_state,
                "last_refresh_error": last_refresh_error,
                "expires_at": expires_at,
            }
        )
        return self.attachment_persist_result


def _build_settings(*, settings_overrides: dict[str, Any] | None = None) -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_ARQ_ENABLED=False,
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
        **(settings_overrides or {}),
    )


def _build_client(
    media_service: FakeMediaService,
    *,
    settings_overrides: dict[str, Any] | None = None,
    vfs_catalog_supplier: FakeVfsCatalogSupplier | None = None,
    redis: DummyRedis | None = None,
    runtime_lifecycle: RuntimeLifecycleState | None = None,
    plugin_registry: object | None = None,
    plugin_settings_payload: dict[str, Any] | None = None,
    control_plane_service: object | None = None,
    control_plane_automation: object | None = None,
    replay_backplane: object | None = None,
    queued_direct_playback_refresh_controller: object | None = None,
    queued_hls_failed_lease_refresh_controller: object | None = None,
    queued_hls_restricted_fallback_refresh_controller: object | None = None,
    playback_service: object | None = None,
) -> TestClient:
    settings = _build_settings(settings_overrides=settings_overrides)
    runtime_redis = redis or DummyRedis()
    app = FastAPI()
    resources = AppResources(
        settings=settings,
        redis=runtime_redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=runtime_redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=runtime_redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=media_service,  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
        runtime_lifecycle=runtime_lifecycle or RuntimeLifecycleState(),
        plugin_registry=plugin_registry,  # type: ignore[arg-type]
        plugin_settings_payload=plugin_settings_payload,
        control_plane_service=control_plane_service,  # type: ignore[arg-type]
        control_plane_automation=control_plane_automation,
        vfs_catalog_supplier=vfs_catalog_supplier,  # type: ignore[arg-type]
        playback_service=playback_service,  # type: ignore[arg-type]
        replay_backplane=replay_backplane,
        queued_direct_playback_refresh_controller=queued_direct_playback_refresh_controller,
        queued_hls_failed_lease_refresh_controller=queued_hls_failed_lease_refresh_controller,
        queued_hls_restricted_fallback_refresh_controller=(
            queued_hls_restricted_fallback_refresh_controller
        ),
    )
    app.state.resources = resources
    app.include_router(create_graphql_router(resources.graphql_plugin_registry), prefix="/graphql")
    return TestClient(app)


def test_graphql_calendar_entries_returns_list_shape() -> None:
    client = _build_client(
        FakeMediaService(
            calendar_entries=[
                CalendarProjectionRecord(
                    item_id="item-1",
                    title="Example Show",
                    item_type="episode",
                    tmdb_id="123",
                    tvdb_id="456",
                    episode_number=2,
                    season_number=1,
                    air_date="2026-03-15T10:00:00+00:00",
                    last_state="Completed",
                    release_data=CalendarReleaseDataRecord(next_aired="2026-03-16T10:00:00+00:00"),
                    specialization=MediaItemSpecializationRecord(
                        item_type="episode",
                        tmdb_id="789",
                        tvdb_id="654",
                        imdb_id="tt1234567",
                        parent_ids=ParentIdsRecord(tmdb_id="999", tvdb_id="555"),
                        show_title="Example Show",
                        season_number=1,
                        episode_number=2,
                    ),
                )
            ]
        )
    )

    response = client.post(
        "/graphql",
        json={
            "query": "query { calendarEntries { itemId showTitle itemType airedAt lastState season episode tmdbId tvdbId imdbId parentTmdbId parentTvdbId releaseData releaseWindow { nextAired lastAired } } }"
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["calendarEntries"] == [
        {
            "itemId": "item-1",
            "showTitle": "Example Show",
            "itemType": "episode",
            "airedAt": "2026-03-15T10:00:00+00:00",
            "lastState": "Completed",
            "season": 1,
            "episode": 2,
            "tmdbId": 123,
            "tvdbId": 456,
            "imdbId": "tt1234567",
            "parentTmdbId": 999,
            "parentTvdbId": 555,
            "releaseData": '{"next_aired": "2026-03-16T10:00:00+00:00", "nextAired": null, "last_aired": null, "lastAired": null}',
            "releaseWindow": {
                "nextAired": "2026-03-16T10:00:00+00:00",
                "lastAired": None,
            },
        }
    ]


def test_graphql_observability_convergence_returns_typed_cross_process_snapshot() -> None:
    client = _build_client(
        FakeMediaService(),
        settings_overrides={
            "FILMU_PY_OTEL_ENABLED": True,
            "FILMU_PY_OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector.internal:4318",
            "FILMU_PY_LOG_SHIPPER": {
                "enabled": True,
                "type": "vector",
                "target": "opensearch://logs-filmu",
                "healthcheck_url": "https://ops.example.test/vector/health",
                "field_mapping_version": "filmu-ecs-v1",
            },
            "FILMU_PY_OBSERVABILITY": {
                "environment_shipping_enabled": True,
                "search_backend": "opensearch",
                "alerting_enabled": True,
                "rust_trace_correlation_enabled": True,
                "required_correlation_fields": [
                    "request.id",
                    "trace.id",
                    "tenant.id",
                    "vfs.session_id",
                    "vfs.daemon_id",
                    "catalog.entry_id",
                    "provider.file_id",
                    "vfs.handle_key",
                ],
                "proof_refs": ["ops/wave4/log-pipeline-rollout.md"],
            },
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  observabilityConvergence {
                    status
                    summary {
                      pipelineStageCount
                      readyStageCount
                      productionEvidenceReady
                      grpcRustTraceReady
                      otlpExportReady
                      searchIndexReady
                      alertRolloutReady
                    }
                    structuredLoggingEnabled
                    grpcBindAddress
                    grpcServiceName
                    otelEnabled
                    otelEndpointConfigured
                    otlpEndpoint
                    logShipperEnabled
                    logShipperType
                    logShipperTarget
                    logShipperTargetConfigured
                    logShipperHealthcheckConfigured
                    searchBackend
                    environmentShippingEnabled
                    alertingEnabled
                    rustTraceCorrelationEnabled
                    correlationContractComplete
                    expectedCorrelationFieldsReady
                    traceContextHeaders
                    correlationHeaders
                    sharedCrossProcessHeaders
                    expectedCorrelationFields
                    missingExpectedCorrelationFields
                    requiredCorrelationFields
                    proofRefs
                    proofArtifacts {
                      ref
                      category
                      label
                      recorded
                    }
                    pipelineStages {
                      name
                      status
                      configured
                      ready
                    }
                    requiredActions
                    remainingGaps
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["observabilityConvergence"]
    assert payload["status"] == "ready"
    assert payload["summary"] == {
        "pipelineStageCount": 5,
        "readyStageCount": 5,
        "productionEvidenceReady": True,
        "grpcRustTraceReady": True,
        "otlpExportReady": True,
        "searchIndexReady": True,
        "alertRolloutReady": True,
    }
    assert payload["structuredLoggingEnabled"] is True
    assert payload["grpcBindAddress"] == "127.0.0.1:50051"
    assert payload["grpcServiceName"] == "filmu.vfs.catalog.v1.FilmuVfsCatalogService"
    assert payload["otelEnabled"] is True
    assert payload["otelEndpointConfigured"] is True
    assert payload["otlpEndpoint"] == "http://collector.internal:4318"
    assert payload["logShipperEnabled"] is True
    assert payload["logShipperType"] == "vector"
    assert payload["logShipperTarget"] == "opensearch://logs-filmu"
    assert payload["logShipperTargetConfigured"] is True
    assert payload["logShipperHealthcheckConfigured"] is True
    assert payload["searchBackend"] == "opensearch"
    assert payload["environmentShippingEnabled"] is True
    assert payload["alertingEnabled"] is True
    assert payload["rustTraceCorrelationEnabled"] is True
    assert payload["correlationContractComplete"] is True
    assert payload["expectedCorrelationFieldsReady"] is True
    assert payload["traceContextHeaders"] == ["traceparent", "tracestate", "baggage"]
    assert payload["correlationHeaders"] == [
        "x-request-id",
        "x-tenant-id",
        "x-filmu-vfs-session-id",
        "x-filmu-vfs-daemon-id",
        "x-filmu-vfs-entry-id",
        "x-filmu-vfs-provider-file-id",
        "x-filmu-vfs-handle-key",
    ]
    assert payload["sharedCrossProcessHeaders"][0] == "traceparent"
    assert payload["expectedCorrelationFields"] == [
        "request.id",
        "trace.id",
        "tenant.id",
        "vfs.session_id",
        "vfs.daemon_id",
        "catalog.entry_id",
        "provider.file_id",
        "vfs.handle_key",
    ]
    assert payload["missingExpectedCorrelationFields"] == []
    assert payload["requiredCorrelationFields"] == [
        "request.id",
        "trace.id",
        "tenant.id",
        "vfs.session_id",
        "vfs.daemon_id",
        "catalog.entry_id",
        "provider.file_id",
        "vfs.handle_key",
    ]
    assert payload["pipelineStages"] == [
        {
            "name": "python_structured_logging",
            "status": "ready",
            "configured": True,
            "ready": True,
        },
        {
            "name": "grpc_rust_correlation",
            "status": "ready",
            "configured": True,
            "ready": True,
        },
        {
            "name": "otlp_export",
            "status": "ready",
            "configured": True,
            "ready": True,
        },
        {
            "name": "log_shipping_and_search",
            "status": "ready",
            "configured": True,
            "ready": True,
        },
        {
            "name": "alerting_and_rollout_evidence",
            "status": "ready",
            "configured": True,
            "ready": True,
        },
    ]
    assert payload["proofRefs"] == ["ops/wave4/log-pipeline-rollout.md"]
    assert payload["proofArtifacts"] == [
        {
            "ref": "ops/wave4/log-pipeline-rollout.md",
            "category": "observability_rollout",
            "label": "observability rollout proof",
            "recorded": True,
        }
    ]
    assert payload["requiredActions"] == []
    assert payload["remainingGaps"] == []


def test_graphql_observability_convergence_sanitizes_blank_proofs_and_requires_live_log_shipping() -> None:
    client = _build_client(
        FakeMediaService(),
        settings_overrides={
            "FILMU_PY_OTEL_ENABLED": True,
            "FILMU_PY_OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector.internal:4318",
            "FILMU_PY_LOG_SHIPPER": {
                "enabled": False,
                "type": "vector",
                "target": "opensearch://logs-filmu",
                "healthcheck_url": "https://ops.example.test/vector/health",
                "field_mapping_version": "filmu-ecs-v1",
            },
            "FILMU_PY_OBSERVABILITY": {
                "environment_shipping_enabled": True,
                "search_backend": "opensearch",
                "alerting_enabled": True,
                "rust_trace_correlation_enabled": True,
                "required_correlation_fields": [
                    "request.id",
                    "trace.id",
                ],
                "proof_refs": ["   ", "ops/wave4/log-pipeline-rollout.md"],
            },
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  observabilityConvergence {
                    proofRefs
                    proofArtifacts { ref category label recorded }
                    summary {
                      productionEvidenceReady
                      searchIndexReady
                      alertRolloutReady
                    }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["observabilityConvergence"]
    assert payload["proofRefs"] == ["ops/wave4/log-pipeline-rollout.md"]
    assert payload["proofArtifacts"] == [
        {
            "ref": "ops/wave4/log-pipeline-rollout.md",
            "category": "observability_rollout",
            "label": "observability rollout proof",
            "recorded": True,
        }
    ]
    assert payload["summary"] == {
        "productionEvidenceReady": True,
        "searchIndexReady": False,
        "alertRolloutReady": True,
    }


def test_graphql_plugin_integration_readiness_returns_typed_posture() -> None:
    plugin_settings = {
        "scraping": {
            "comet": {
                "enabled": True,
                "url": "https://comet.example",
                "contract_proof_refs": ["ops/plugins/comet-contract.md"],
                "soak_proof_refs": ["ops/plugins/comet-soak.md"],
            }
        },
        "content": {
            "overseerr": {
                "enabled": True,
                "url": "https://seerr.example",
                "api_key": "seerr-token",
                "contract_proof_refs": ["ops/plugins/seerr-contract.md"],
                "soak_proof_refs": ["ops/plugins/seerr-soak.md"],
            },
            "listrr": {
                "enabled": False,
                "url": "https://listrr.example",
                "list_ids": ["list-1"],
            },
        },
        "updaters": {
            "plex": {
                "enabled": True,
                "url": "https://plex.example",
                "token": "plex-token",
                "contract_proof_refs": ["ops/plugins/plex-contract.md"],
                "soak_proof_refs": ["ops/plugins/plex-soak.md"],
            }
        },
    }
    registry = PluginRegistry()
    harness = TestPluginContext(settings=plugin_settings)
    register_builtin_plugins(registry, context_provider=harness.provider())
    client = _build_client(
        FakeMediaService(),
        plugin_registry=registry,
        plugin_settings_payload=plugin_settings,
        settings_overrides={
            "FILMU_PY_SCRAPING": plugin_settings["scraping"],
            "FILMU_PY_UPDATERS": plugin_settings["updaters"],
        },
    )

    response = client.post(
        "/graphql",
        json={
                "query": """
                query {
                  pluginIntegrationReadiness(includeDisabled: false) {
                    status
                    summary {
                      totalPlugins
                      enabledPlugins
                      configuredPlugins
                      contractValidatedPlugins
                      soakValidatedPlugins
                      readyPlugins
                      missingContractProofPlugins
                      missingSoakProofPlugins
                    }
                    requiredActions
                    remainingGaps
                    plugins {
                      name
                      capabilityKind
                      status
                      registered
                      enabled
                      configured
                      ready
                      endpoint
                      endpointConfigured
                      configSource
                      requiredSettings
                      missingSettings
                      contractProofRefs
                      soakProofRefs
                      contractProofs { ref category label recorded }
                      soakProofs { ref category label recorded }
                      contractValidated
                      soakValidated
                      proofGapCount
                      requiredActions
                      remainingGaps
                    }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["pluginIntegrationReadiness"]
    assert payload["status"] == "ready"
    assert payload["summary"] == {
        "totalPlugins": 3,
        "enabledPlugins": 3,
        "configuredPlugins": 3,
        "contractValidatedPlugins": 3,
        "soakValidatedPlugins": 3,
        "readyPlugins": 3,
        "missingContractProofPlugins": 0,
        "missingSoakProofPlugins": 0,
    }
    assert payload["requiredActions"] == []
    assert payload["remainingGaps"] == []
    by_name = {entry["name"]: entry for entry in payload["plugins"]}
    assert by_name["comet"]["ready"] is True
    assert by_name["comet"]["endpoint"] == "https://comet.example"
    assert by_name["comet"]["endpointConfigured"] is True
    assert by_name["comet"]["contractValidated"] is True
    assert by_name["comet"]["soakValidated"] is True
    assert by_name["comet"]["contractProofs"] == [
        {
            "ref": "ops/plugins/comet-contract.md",
            "category": "plugin_contract",
            "label": "comet contract proof",
            "recorded": True,
        }
    ]
    assert by_name["comet"]["proofGapCount"] == 0
    assert by_name["seerr"]["configSource"] == "content.overseerr"
    assert by_name["seerr"]["contractProofRefs"] == ["ops/plugins/seerr-contract.md"]
    assert "listrr" not in by_name
    assert by_name["plex"]["ready"] is True
    assert by_name["plex"]["soakProofRefs"] == ["ops/plugins/plex-soak.md"]


def test_graphql_plugin_integration_readiness_sanitizes_blank_proof_refs() -> None:
    plugin_settings = {
        "scraping": {
            "comet": {
                "enabled": True,
                "url": "https://comet.example",
                "contract_proof_refs": ["   "],
                "soak_proof_refs": ["ops/plugins/comet-soak.md"],
            }
        }
    }
    registry = PluginRegistry()
    harness = TestPluginContext(settings=plugin_settings)
    register_builtin_plugins(registry, context_provider=harness.provider())
    client = _build_client(
        FakeMediaService(),
        plugin_registry=registry,
        plugin_settings_payload=plugin_settings,
        settings_overrides={
            "FILMU_PY_SCRAPING": plugin_settings["scraping"],
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  pluginIntegrationReadiness(includeDisabled: false) {
                    summary {
                      totalPlugins
                      contractValidatedPlugins
                      missingContractProofPlugins
                    }
                    plugins {
                      name
                      contractProofRefs
                      contractProofs { ref category label recorded }
                      contractValidated
                      soakValidated
                      proofGapCount
                    }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["pluginIntegrationReadiness"]
    assert payload["summary"] == {
        "totalPlugins": 1,
        "contractValidatedPlugins": 0,
        "missingContractProofPlugins": 1,
    }
    assert payload["plugins"] == [
        {
            "name": "comet",
            "contractProofRefs": [],
            "contractProofs": [],
            "contractValidated": False,
            "soakValidated": True,
            "proofGapCount": 1,
        }
    ]


def test_graphql_control_plane_posture_returns_typed_summary_and_automation() -> None:
    class FakeControlPlaneService:
        async def summarize_subscribers(self, *, active_within_seconds: int) -> object:
            _ = active_within_seconds
            return SimpleNamespace(
                total_subscribers=2,
                active_subscribers=1,
                stale_subscribers=1,
                error_subscribers=0,
                fenced_subscribers=0,
                ack_pending_subscribers=1,
                stream_count=1,
                group_count=1,
                node_count=1,
                tenant_count=1,
                oldest_heartbeat_age_seconds=45.0,
                status_counts={"active": 1, "stale": 1},
                required_actions=["recover_stale_control_plane_subscribers"],
                remaining_gaps=["control-plane backlog needs recovery"],
            )

        async def list_subscribers(self, *, active_within_seconds: int) -> list[object]:
            _ = active_within_seconds
            return [
                SimpleNamespace(
                    stream_name="filmu:events",
                    group_name="filmu-api",
                    consumer_name="worker-a",
                    node_id="node-a",
                    tenant_id="tenant-main",
                    status="stale",
                    last_read_offset="100-0",
                    last_delivered_event_id="120-0",
                    last_acked_event_id="110-0",
                    last_error="consumer_fenced owner=node-b contender=node-a",
                    claimed_at=datetime(2026, 4, 16, 9, 58, tzinfo=UTC),
                    last_heartbeat_at=datetime(2026, 4, 16, 9, 59, tzinfo=UTC),
                    created_at=datetime(2026, 4, 16, 9, 50, tzinfo=UTC),
                    updated_at=datetime(2026, 4, 16, 10, 0, tzinfo=UTC),
                )
            ]

    class FakeAutomation:
        def snapshot(self) -> object:
            return SimpleNamespace(
                enabled=True,
                runner_status="running",
                interval_seconds=30,
                active_within_seconds=180,
                pending_min_idle_ms=60000,
                claim_limit=25,
                max_claim_passes=2,
                consumer_group="filmu-api",
                consumer_name="automation",
                service_attached=True,
                backplane_attached=True,
                last_run_at=datetime(2026, 4, 16, 10, 0, tzinfo=UTC),
                last_success_at=datetime(2026, 4, 16, 10, 0, tzinfo=UTC),
                last_failure_at=None,
                consecutive_failures=0,
                last_error=None,
                remediation_updated_subscribers=1,
                rewound_subscribers=1,
                claimed_pending_events=2,
                claim_passes=1,
                pending_count_after=0,
                summary=None,
            )

    client = _build_client(
        FakeMediaService(),
        settings_overrides={
            "FILMU_PY_CONTROL_PLANE": {
                "event_backplane": "redis_stream",
                "proof_refs": ["ops/control-plane/redis-consumer-group-soak.md"],
                "automation": {"enabled": True},
            }
        },
        control_plane_service=FakeControlPlaneService(),
        control_plane_automation=FakeAutomation(),
        replay_backplane=FakeReplayBackplane(
            pending_count=3,
            oldest_event_id="100-0",
            latest_event_id="120-0",
            consumer_counts={"worker-a": 2, "worker-b": 1},
        ),
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  controlPlaneSummary(activeWithinSeconds: 180) {
                    totalSubscribers
                    staleSubscribers
                    ackPendingSubscribers
                    statusCounts { status count }
                    requiredActions
                    remainingGaps
                  }
                  controlPlaneSubscribers(activeWithinSeconds: 180, status: "stale", ackPending: true, fenced: true) {
                    streamName
                    groupName
                    consumerName
                    nodeId
                    tenantId
                    status
                    lastReadOffset
                    lastDeliveredEventId
                    lastAckedEventId
                    ackPending
                    fenced
                  }
                  controlPlaneAutomation {
                    enabled
                    runnerStatus
                    intervalSeconds
                    activeWithinSeconds
                    claimLimit
                    maxClaimPasses
                    consumerGroup
                    consumerName
                    serviceAttached
                    backplaneAttached
                    remediationUpdatedSubscribers
                    rewoundSubscribers
                    claimedPendingEvents
                    claimPasses
                    pendingCountAfter
                    summary {
                      totalSubscribers
                      staleSubscribers
                      ackPendingSubscribers
                    }
                    requiredActions
                    remainingGaps
                  }
                  controlPlaneRecoveryReadiness(activeWithinSeconds: 180) {
                    status
                    activeWithinSeconds
                    staleSubscribers
                    ackPendingSubscribers
                    pendingCount
                    consumerCount
                    automationEnabled
                    automationHealthy
                    replayAttached
                    proofRefs
                    proofArtifacts { ref category label recorded }
                    proofReady
                    requiredActions
                    remainingGaps
                  }
                  controlPlaneReplayBackplane {
                    status
                    eventBackplane
                    streamName
                    consumerGroup
                    replayMaxlen
                    attached
                    pendingCount
                    oldestEventId
                    latestEventId
                    consumerCounts { key count }
                    consumerCount
                    hasPendingBacklog
                    proofRefs
                    proofArtifacts { ref category label recorded }
                    proofReady
                    requiredActions
                    remainingGaps
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["controlPlaneSummary"] == {
        "totalSubscribers": 2,
        "staleSubscribers": 1,
        "ackPendingSubscribers": 1,
        "statusCounts": [
            {"status": "active", "count": 1},
            {"status": "stale", "count": 1},
        ],
        "requiredActions": ["recover_stale_control_plane_subscribers"],
        "remainingGaps": ["control-plane backlog needs recovery"],
    }
    assert payload["controlPlaneSubscribers"] == [
        {
            "streamName": "filmu:events",
            "groupName": "filmu-api",
            "consumerName": "worker-a",
            "nodeId": "node-a",
            "tenantId": "tenant-main",
            "status": "stale",
            "lastReadOffset": "100-0",
            "lastDeliveredEventId": "120-0",
            "lastAckedEventId": "110-0",
            "ackPending": True,
            "fenced": True,
        }
    ]
    assert payload["controlPlaneAutomation"] == {
        "enabled": True,
        "runnerStatus": "running",
        "intervalSeconds": 30,
        "activeWithinSeconds": 180,
        "claimLimit": 25,
        "maxClaimPasses": 2,
        "consumerGroup": "filmu-api",
        "consumerName": "automation",
        "serviceAttached": True,
        "backplaneAttached": True,
        "remediationUpdatedSubscribers": 1,
        "rewoundSubscribers": 1,
        "claimedPendingEvents": 2,
        "claimPasses": 1,
        "pendingCountAfter": 0,
        "summary": {
            "totalSubscribers": 2,
            "staleSubscribers": 1,
            "ackPendingSubscribers": 1,
        },
        "requiredActions": ["recover_stale_control_plane_subscribers"],
        "remainingGaps": ["control-plane backlog needs recovery"],
    }
    assert payload["controlPlaneReplayBackplane"] == {
        "status": "ready",
        "eventBackplane": "redis_stream",
        "streamName": "filmu:events",
        "consumerGroup": "filmu-api",
        "replayMaxlen": 10000,
        "attached": True,
        "pendingCount": 3,
        "oldestEventId": "100-0",
        "latestEventId": "120-0",
        "consumerCounts": [
            {"key": "worker-a", "count": 2},
            {"key": "worker-b", "count": 1},
        ],
        "consumerCount": 2,
        "hasPendingBacklog": True,
        "proofRefs": ["ops/control-plane/redis-consumer-group-soak.md"],
        "proofArtifacts": [
            {
                "ref": "ops/control-plane/redis-consumer-group-soak.md",
                "category": "control_plane_rollout",
                "label": "control-plane replay backplane proof",
                "recorded": True,
            }
        ],
        "proofReady": True,
        "requiredActions": [],
        "remainingGaps": [],
    }
    assert payload["controlPlaneRecoveryReadiness"] == {
        "status": "partial",
        "activeWithinSeconds": 180,
        "staleSubscribers": 1,
        "ackPendingSubscribers": 1,
        "pendingCount": 3,
        "consumerCount": 2,
        "automationEnabled": True,
        "automationHealthy": True,
        "replayAttached": True,
        "proofRefs": ["ops/control-plane/redis-consumer-group-soak.md"],
        "proofArtifacts": [
            {
                "ref": "ops/control-plane/redis-consumer-group-soak.md",
                "category": "control_plane_rollout",
                "label": "control-plane replay backplane proof",
                "recorded": True,
            }
        ],
        "proofReady": True,
        "requiredActions": ["recover_stale_control_plane_subscribers"],
        "remainingGaps": ["control-plane backlog needs recovery"],
    }


def test_graphql_control_plane_recovery_requires_healthy_automation_backplane() -> None:
    class HealthyControlPlaneService:
        async def summarize_subscribers(self, *, active_within_seconds: int) -> object:
            _ = active_within_seconds
            return SimpleNamespace(
                total_subscribers=1,
                active_subscribers=1,
                stale_subscribers=0,
                error_subscribers=0,
                fenced_subscribers=0,
                ack_pending_subscribers=0,
                stream_count=1,
                group_count=1,
                node_count=1,
                tenant_count=1,
                oldest_heartbeat_age_seconds=5.0,
                status_counts={"active": 1},
                required_actions=[],
                remaining_gaps=[],
            )

        async def list_subscribers(self, *, active_within_seconds: int) -> list[object]:
            _ = active_within_seconds
            return []

    class BackplaneDetachedAutomation:
        def snapshot(self) -> object:
            return SimpleNamespace(
                enabled=True,
                runner_status="running",
                interval_seconds=30,
                active_within_seconds=180,
                pending_min_idle_ms=60000,
                claim_limit=25,
                max_claim_passes=2,
                consumer_group="filmu-api",
                consumer_name="automation",
                service_attached=True,
                backplane_attached=False,
                last_run_at=datetime(2026, 4, 16, 10, 0, tzinfo=UTC),
                last_success_at=datetime(2026, 4, 16, 10, 0, tzinfo=UTC),
                last_failure_at=None,
                consecutive_failures=0,
                last_error=None,
                remediation_updated_subscribers=0,
                rewound_subscribers=0,
                claimed_pending_events=0,
                claim_passes=0,
                pending_count_after=0,
                summary=None,
            )

    client = _build_client(
        FakeMediaService(),
        settings_overrides={
            "FILMU_PY_CONTROL_PLANE": {
                "event_backplane": "redis_stream",
                "proof_refs": ["   "],
                "automation": {"enabled": True},
            }
        },
        control_plane_service=HealthyControlPlaneService(),
        control_plane_automation=BackplaneDetachedAutomation(),
        replay_backplane=FakeReplayBackplane(
            pending_count=0,
            oldest_event_id=None,
            latest_event_id=None,
            consumer_counts={"worker-a": 1},
        ),
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  controlPlaneReplayBackplane {
                    proofRefs
                    proofArtifacts { ref category label recorded }
                    proofReady
                    requiredActions
                  }
                  controlPlaneRecoveryReadiness(activeWithinSeconds: 180) {
                    status
                    automationHealthy
                    proofRefs
                    proofArtifacts { ref category label recorded }
                    proofReady
                    requiredActions
                    remainingGaps
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["controlPlaneReplayBackplane"] == {
        "proofRefs": [],
        "proofArtifacts": [],
        "proofReady": False,
        "requiredActions": ["record_control_plane_redis_consumer_group_evidence"],
    }
    assert payload["controlPlaneRecoveryReadiness"] == {
        "status": "partial",
        "automationHealthy": False,
        "proofRefs": [],
        "proofArtifacts": [],
        "proofReady": False,
        "requiredActions": ["record_control_plane_redis_consumer_group_evidence"],
        "remainingGaps": ["redis consumer-group rollout has no retained production evidence"],
    }


def test_graphql_downloader_plugin_event_and_governance_posture_returns_typed_surfaces() -> None:
    plugin_registry = PluginRegistry()
    plugin_registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "external-scraper",
                "version": "1.0.0",
                "api_version": "1",
                "distribution": "filesystem",
                "publisher": "community",
                "release_channel": "stable",
                "trust_level": "community",
                "sandbox_profile": "restricted",
                "tenancy_mode": "tenant",
                "entry_module": "plugin.py",
                "scraper": "ExternalScraper",
                "publishable_events": ["external.scan.completed"],
            }
        )
    )

    class ExampleHook:
        subscribed_events = frozenset({"item.completed", "item.state.changed"})

    plugin_registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "hook-plugin",
                "version": "1.0.0",
                "api_version": "1",
                "entry_module": "plugin.py",
                "event_hook": "ExampleHook",
            }
        )
    )
    plugin_registry.register_capability(
        plugin_name="external-scraper",
        kind=PluginCapabilityKind.SCRAPER,
        implementation=object(),
    )
    plugin_registry.register_capability(
        plugin_name="hook-plugin",
        kind=PluginCapabilityKind.EVENT_HOOK,
        implementation=ExampleHook(),
    )

    client = _build_client(
        FakeMediaService(),
        plugin_registry=plugin_registry,
        settings_overrides={
            "FILMU_PY_DOWNLOADERS": {
                "real_debrid": {"enabled": True, "api_key": "rd-token"},
                "all_debrid": {"enabled": True, "api_key": "ad-token"},
                "debrid_link": {"enabled": False, "api_key": ""},
            },
            "FILMU_PY_PLUGIN_RUNTIME": {
                "enforcement_mode": "report_only",
            },
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  downloaderOrchestration {
                    selectionMode
                    selectedProvider
                    multiProviderEnabled
                    pluginDownloadersRegistered
                    workerPluginDispatchReady
                    fanoutReady
                    multiContainerReady
                    requiredActions
                    remainingGaps
                    providers {
                      name
                      source
                      enabled
                      configured
                      selected
                      priority
                    }
                  }
                  pluginEvents {
                    name
                    publisher
                    publishableEvents
                    hookSubscriptions
                  }
                  pluginGovernance {
                    summary {
                      totalPlugins
                      nonBuiltinPlugins
                      unsignedExternalPlugins
                      runtimePolicyMode
                      runtimeIsolationReady
                      recommendedActions
                      remainingGaps
                      sandboxProfileCounts { key count }
                      tenancyModeCounts { key count }
                    }
                    plugins {
                      name
                      status
                      ready
                      publisher
                      releaseChannel
                      trustLevel
                      sandboxProfile
                      tenancyMode
                      signaturePresent
                      warnings
                    }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["downloaderOrchestration"] == {
        "selectionMode": "ordered_failover",
        "selectedProvider": "realdebrid",
        "multiProviderEnabled": True,
        "pluginDownloadersRegistered": 0,
        "workerPluginDispatchReady": False,
        "fanoutReady": True,
        "multiContainerReady": True,
        "requiredActions": [],
        "remainingGaps": [],
        "providers": [
            {
                "name": "realdebrid",
                "source": "builtin",
                "enabled": True,
                "configured": True,
                "selected": True,
                "priority": 1,
            },
            {
                "name": "alldebrid",
                "source": "builtin",
                "enabled": True,
                "configured": True,
                "selected": False,
                "priority": 2,
            },
            {
                "name": "debridlink",
                "source": "builtin",
                "enabled": False,
                "configured": False,
                "selected": False,
                "priority": 3,
            },
        ],
    }
    assert payload["pluginEvents"] == [
        {
            "name": "external-scraper",
            "publisher": "community",
            "publishableEvents": ["external.scan.completed"],
            "hookSubscriptions": [],
        },
        {
            "name": "hook-plugin",
            "publisher": None,
            "publishableEvents": [],
            "hookSubscriptions": ["item.completed", "item.state.changed"],
        },
    ]
    governance = payload["pluginGovernance"]
    assert governance["summary"] == {
        "totalPlugins": 2,
        "nonBuiltinPlugins": 2,
        "unsignedExternalPlugins": 2,
        "runtimePolicyMode": "report_only",
        "runtimeIsolationReady": False,
        "recommendedActions": ["require_external_plugin_signature"],
        "remainingGaps": [
            "non-builtin plugin runtime isolation exit gates are not fully satisfied",
            "operator quarantine/revocation still depends on runtime policy enforcement",
            "external plugin artifact provenance or signature verification is still incomplete",
        ],
        "sandboxProfileCounts": [{"key": "restricted", "count": 2}],
        "tenancyModeCounts": [{"key": "shared", "count": 1}, {"key": "tenant", "count": 1}],
    }
    assert governance["plugins"][0] == {
        "name": "external-scraper",
        "status": "loaded",
        "ready": True,
        "publisher": "community",
        "releaseChannel": "stable",
        "trustLevel": "community",
        "sandboxProfile": "restricted",
        "tenancyMode": "tenant",
        "signaturePresent": False,
        "warnings": [],
    }
    assert governance["plugins"][1] == {
        "name": "hook-plugin",
        "status": "loaded",
        "ready": True,
        "publisher": None,
        "releaseChannel": "stable",
        "trustLevel": "community",
        "sandboxProfile": "restricted",
        "tenancyMode": "shared",
        "signaturePresent": False,
        "warnings": [],
    }


def test_graphql_enterprise_operations_governance_returns_typed_slice_posture() -> None:
    client = _build_client(
        FakeMediaService(),
        settings_overrides={
            "FILMU_PY_OTEL_ENABLED": True,
            "FILMU_PY_OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel.example.test/v1/traces",
            "FILMU_PY_LOG_SHIPPER": {
                "enabled": True,
                "type": "vector",
                "target": "http://logs.example.test",
                "healthcheck_url": "http://logs.example.test/health",
            },
            "FILMU_PY_OBSERVABILITY": {
                "environment_shipping_enabled": True,
                "search_backend": "opensearch",
                "alerting_enabled": True,
                "rust_trace_correlation_enabled": True,
                "proof_refs": ["ops/wave4/log-pipeline-rollout.md"],
            },
            "FILMU_PY_PLUGIN_RUNTIME": {
                "enforcement_mode": "isolated_runtime_required",
                "require_strict_signatures": True,
                "require_source_digest": True,
                "proof_refs": ["ops/wave4/plugin-runtime-isolation.md"],
            },
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  enterpriseOperationsGovernance {
                    operatorLogPipeline {
                      status
                      requiredActions
                      remainingGaps
                      evidence
                    }
                    pluginRuntimeIsolation {
                      status
                      requiredActions
                      remainingGaps
                      evidence
                    }
                    identityAuthz {
                      evidence
                    }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["enterpriseOperationsGovernance"]
    assert payload["operatorLogPipeline"]["status"] == "ready"
    assert payload["operatorLogPipeline"]["requiredActions"] == []
    assert payload["operatorLogPipeline"]["remainingGaps"] == []
    assert "log_search_backend=opensearch" in payload["operatorLogPipeline"]["evidence"]
    assert "rust_trace_correlation_enabled=True" in payload["operatorLogPipeline"]["evidence"]
    assert payload["pluginRuntimeIsolation"]["status"] == "ready"
    assert payload["pluginRuntimeIsolation"]["requiredActions"] == []
    assert payload["pluginRuntimeIsolation"]["remainingGaps"] == []
    assert (
        "plugin_runtime_enforcement_mode=isolated_runtime_required"
        in payload["pluginRuntimeIsolation"]["evidence"]
    )
    assert "plugin_runtime_exit_ready=1" in payload["pluginRuntimeIsolation"]["evidence"]
    assert "resource_scope_constraint_coverage=True" in payload["identityAuthz"]["evidence"]


def test_graphql_library_stats_returns_typed_breakdown() -> None:
    client = _build_client(
        FakeMediaService(
            stats=StatsProjection(
                total_items=10,
                completed_items=4,
                failed_items=2,
                incomplete_items=4,
                movies=3,
                shows=2,
                episodes=5,
                seasons=2,
                states={"Completed": 4, "Failed": 2, "Unreleased": 0},
                activity={"2026-03-15": 10},
            )
        )
    )

    response = client.post(
        "/graphql",
        json={
            "query": "query { libraryStats { totalItems totalMovies totalShows totalSeasons totalEpisodes completedItems incompleteItems failedItems stateBreakdown activity } }"
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["libraryStats"]
    assert payload["totalItems"] == 10
    assert payload["failedItems"] == 2
    assert payload["stateBreakdown"] == '{"Completed": 4, "Failed": 2, "Unreleased": 0}'
    assert payload["activity"] == '[{"date": "2026-03-15", "count": 10}]'


def test_graphql_media_item_returns_stream_candidates() -> None:
    detail = MediaItemSummaryRecord(
        id="item-1",
        type="movie",
        title="Example Movie",
        state="Completed",
        tmdb_id="123",
        tvdb_id="456",
        created_at="2026-03-15T10:00:00+00:00",
        updated_at="2026-03-15T11:00:00+00:00",
        specialization=MediaItemSpecializationRecord(
            item_type="episode",
            tmdb_id="123",
            tvdb_id="456",
            imdb_id="tt1234567",
            parent_ids=ParentIdsRecord(tmdb_id="999", tvdb_id="555"),
            show_title="Example Show",
            season_number=2,
            episode_number=7,
        ),
        playback_attachments=[
            cast(
                Any,
                type(
                    "Attachment",
                    (),
                    {
                        "id": "attachment-1",
                        "kind": "direct",
                        "locator": "https://cdn.example.com/direct",
                        "source_key": "persisted",
                        "provider": "realdebrid",
                        "provider_download_id": "torrent-123",
                        "provider_file_id": "file-123",
                        "provider_file_path": "/downloads/Example.Movie.mkv",
                        "original_filename": "Example.Movie.mkv",
                        "file_size": 123456789,
                        "local_path": None,
                        "restricted_url": "https://api.real-debrid.com/restricted",
                        "unrestricted_url": "https://cdn.example.com/direct",
                        "is_preferred": True,
                        "preference_rank": 1,
                        "refresh_state": "ready",
                        "expires_at": "2026-03-15T12:00:00+00:00",
                        "last_refreshed_at": "2026-03-15T11:00:00+00:00",
                        "last_refresh_error": None,
                    },
                )(),
            )
        ],
        resolved_playback=cast(
            Any,
            type(
                "ResolvedPlayback",
                (),
                {
                    "direct": type(
                        "ResolvedAttachment",
                        (),
                        {
                            "kind": "direct",
                            "locator": "https://cdn.example.com/direct",
                            "source_key": "persisted",
                            "provider": "realdebrid",
                            "provider_download_id": "torrent-123",
                            "provider_file_id": "file-123",
                            "provider_file_path": "/downloads/Example.Movie.mkv",
                            "original_filename": "Example.Movie.mkv",
                            "file_size": 123456789,
                            "local_path": None,
                            "restricted_url": None,
                            "unrestricted_url": "https://cdn.example.com/direct",
                        },
                    )(),
                    "hls": None,
                    "direct_ready": True,
                    "hls_ready": False,
                    "missing_local_file": False,
                },
            )(),
        ),
        active_stream=cast(
            Any,
            type(
                "ActiveStream",
                (),
                {
                    "direct_ready": True,
                    "hls_ready": False,
                    "missing_local_file": False,
                    "direct_owner": type(
                        "ActiveOwner",
                        (),
                        {
                            "media_entry_index": 0,
                            "kind": "remote-direct",
                            "original_filename": "Example.Movie.mkv",
                            "provider": "realdebrid",
                            "provider_download_id": "torrent-123",
                            "provider_file_id": "file-123",
                            "provider_file_path": "/downloads/Example.Movie.mkv",
                        },
                    )(),
                    "hls_owner": None,
                },
            )(),
        ),
        media_entries=[
            cast(
                Any,
                type(
                    "MediaEntry",
                    (),
                    {
                        "entry_type": "media",
                        "kind": "remote-direct",
                        "original_filename": "Example.Movie.mkv",
                        "url": "https://cdn.example.com/direct",
                        "local_path": None,
                        "download_url": "https://api.real-debrid.com/restricted",
                        "unrestricted_url": "https://cdn.example.com/direct",
                        "provider": "realdebrid",
                        "provider_download_id": "torrent-123",
                        "provider_file_id": "file-123",
                        "provider_file_path": "/downloads/Example.Movie.mkv",
                        "size": 123456789,
                        "created": "2026-03-15T10:30:00+00:00",
                        "modified": "2026-03-15T11:00:00+00:00",
                        "refresh_state": "ready",
                        "expires_at": "2026-03-15T12:00:00+00:00",
                        "last_refreshed_at": "2026-03-15T11:00:00+00:00",
                        "last_refresh_error": None,
                        "active_for_direct": True,
                        "active_for_hls": False,
                        "is_active_stream": True,
                    },
                )(),
            )
        ],
    )
    selected_stream = StreamORM(
        id="stream-1",
        media_item_id="item-1",
        infohash="hash-1",
        raw_title="Example.Movie.1080p.WEB-DL",
        parsed_title={"title": "Example Movie"},
        rank=300,
        lev_ratio=1.0,
        resolution="1080p",
        selected=True,
    )
    alternate_stream = StreamORM(
        id="stream-2",
        media_item_id="item-1",
        infohash="hash-2",
        raw_title="Example.Movie.720p.WEB-DL",
        parsed_title={"title": "Example Movie"},
        rank=100,
        lev_ratio=0.9,
        resolution="720p",
        selected=False,
    )
    client = _build_client(
        FakeMediaService(
            detail=detail,
            stream_candidates=[selected_stream, alternate_stream],
            recovery_plan=RecoveryPlanRecord(
                mechanism=RecoveryMechanism.ORPHAN_RECOVERY,
                target_stage=RecoveryTargetStage.FINALIZE,
                reason="orphaned_downloaded_item",
                next_retry_at=None,
                recovery_attempt_count=2,
                is_in_cooldown=False,
            ),
        )
    )

    response = client.post(
        "/graphql",
        json={
            "query": 'query { mediaItem(id: "item-1") { id title state itemType tmdbId tvdbId imdbId parentTmdbId parentTvdbId showTitle seasonNumber episodeNumber createdAt updatedAt recoveryPlan { mechanism targetStage reason nextRetryAt recoveryAttemptCount isInCooldown } streamCandidates { id rawTitle parsedTitle resolution rankScore levRatio selected passed rejectionReason } selectedStream { id rawTitle selected } playbackAttachments { id kind sourceKey provider providerDownloadId originalFilename fileSize refreshState } resolvedPlayback { directReady hlsReady missingLocalFile direct { kind locator sourceKey providerDownloadId originalFilename } } activeStream { directReady hlsReady missingLocalFile directOwner { mediaEntryIndex kind providerDownloadId originalFilename } } mediaEntries { entryType kind originalFilename providerDownloadId size refreshState activeForDirect activeForHls isActiveStream } } }'
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["mediaItem"]
    assert payload["id"] == "item-1"
    assert payload["recoveryPlan"] == {
        "mechanism": "ORPHAN_RECOVERY",
        "targetStage": "FINALIZE",
        "reason": "orphaned_downloaded_item",
        "nextRetryAt": None,
        "recoveryAttemptCount": 2,
        "isInCooldown": False,
    }
    assert payload["tvdbId"] == 456
    assert payload["imdbId"] == "tt1234567"
    assert payload["parentTmdbId"] == 999
    assert payload["parentTvdbId"] == 555
    assert payload["showTitle"] == "Example Show"
    assert payload["seasonNumber"] == 2
    assert payload["episodeNumber"] == 7
    assert len(payload["streamCandidates"]) == 2
    assert payload["selectedStream"]["id"] == "stream-1"
    assert payload["streamCandidates"][0]["rawTitle"] == "Example.Movie.1080p.WEB-DL"
    assert payload["playbackAttachments"] == [
        {
            "id": "attachment-1",
            "kind": "direct",
            "sourceKey": "persisted",
            "provider": "realdebrid",
            "providerDownloadId": "torrent-123",
            "originalFilename": "Example.Movie.mkv",
            "fileSize": 123456789,
            "refreshState": "ready",
        }
    ]
    assert payload["resolvedPlayback"] == {
        "directReady": True,
        "hlsReady": False,
        "missingLocalFile": False,
        "direct": {
            "kind": "direct",
            "locator": "https://cdn.example.com/direct",
            "sourceKey": "persisted",
            "providerDownloadId": "torrent-123",
            "originalFilename": "Example.Movie.mkv",
        },
    }
    assert payload["activeStream"] == {
        "directReady": True,
        "hlsReady": False,
        "missingLocalFile": False,
        "directOwner": {
            "mediaEntryIndex": 0,
            "kind": "remote-direct",
            "providerDownloadId": "torrent-123",
            "originalFilename": "Example.Movie.mkv",
        },
    }
    assert payload["mediaEntries"] == [
        {
            "entryType": "media",
            "kind": "remote-direct",
            "originalFilename": "Example.Movie.mkv",
            "providerDownloadId": "torrent-123",
            "size": 123456789,
            "refreshState": "ready",
            "activeForDirect": True,
            "activeForHls": False,
            "isActiveStream": True,
        }
    ]


def test_graphql_items_exposes_media_type_and_media_kind() -> None:
    client = _build_client(
        FakeMediaService(
            item_records=[
                MediaItemRecord(
                    id="show-1",
                    external_ref="tvdb:555",
                    title="Example Show",
                    state=ItemState.REQUESTED,
                    attributes={
                        "item_type": "show",
                        "tmdb_id": "999",
                        "tvdb_id": "555",
                        "imdb_id": "tt5550001",
                        "show_title": "Example Show",
                        "poster_path": "/poster.jpg",
                    },
                )
            ]
        )
    )

    response = client.post(
        "/graphql",
        json={
            "query": "query { items(limit: 1) { id mediaType mediaKind tmdbId tvdbId imdbId showTitle posterPath } }"
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["items"] == [
        {
            "id": "show-1",
            "mediaType": "show",
            "mediaKind": "SHOW",
            "tmdbId": 999,
            "tvdbId": 555,
            "imdbId": "tt5550001",
            "showTitle": "Example Show",
            "posterPath": "/poster.jpg",
        }
    ]


def test_graphql_vfs_directory_and_entry_queries_use_catalog_snapshot() -> None:
    snapshot = VfsCatalogSnapshot(
        generation_id="7",
        published_at=datetime(2026, 4, 13, 12, 0, tzinfo=UTC),
        entries=(
            VfsCatalogEntry(
                entry_id="dir:/",
                parent_entry_id=None,
                path="/",
                name="/",
                kind="directory",
                directory=VfsCatalogDirectoryEntry(path="/"),
            ),
            VfsCatalogEntry(
                entry_id="dir:/Shows",
                parent_entry_id="dir:/",
                path="/Shows",
                name="Shows",
                kind="directory",
                directory=VfsCatalogDirectoryEntry(path="/Shows"),
            ),
            VfsCatalogEntry(
                entry_id="dir:/Shows/Example Show (2024)",
                parent_entry_id="dir:/Shows",
                path="/Shows/Example Show (2024)",
                name="Example Show (2024)",
                kind="directory",
                directory=VfsCatalogDirectoryEntry(path="/Shows/Example Show (2024)"),
            ),
            VfsCatalogEntry(
                entry_id="dir:/Shows/Example Show (2024)/Season 01",
                parent_entry_id="dir:/Shows/Example Show (2024)",
                path="/Shows/Example Show (2024)/Season 01",
                name="Season 01",
                kind="directory",
                directory=VfsCatalogDirectoryEntry(path="/Shows/Example Show (2024)/Season 01"),
            ),
            VfsCatalogEntry(
                entry_id="file:entry-1",
                parent_entry_id="dir:/Shows/Example Show (2024)/Season 01",
                path="/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
                name="Example Show S01E01.mkv",
                kind="file",
                correlation=VfsCatalogCorrelationKeys(
                    item_id="item-1",
                    media_entry_id="entry-1",
                    provider="realdebrid",
                    provider_download_id="torrent-123",
                    provider_file_path="/downloads/Example.Show.S01E01.mkv",
                ),
                file=VfsCatalogFileEntry(
                    item_id="item-1",
                    item_title="Example Show",
                    item_external_ref="tvdb:555",
                    media_entry_id="entry-1",
                    source_attachment_id="attachment-1",
                    media_type="episode",
                    transport="remote-direct",
                    locator="https://cdn.example.com/stream/entry-1",
                    unrestricted_url="https://cdn.example.com/stream/entry-1",
                    original_filename="Example Show S01E01.mkv",
                    size_bytes=987654321,
                    lease_state="ready",
                    last_refreshed_at=datetime(2026, 4, 13, 11, 55, tzinfo=UTC),
                    provider="realdebrid",
                    provider_download_id="torrent-123",
                    provider_file_path="/downloads/Example.Show.S01E01.mkv",
                    active_roles=("direct",),
                    source_key="persisted",
                    query_strategy="persisted_media_entries",
                    provider_family="debrid",
                    locator_source="unrestricted_url",
                    match_basis="provider_identity",
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=4, file_count=1, blocked_item_count=0),
    )
    client = _build_client(
        FakeMediaService(),
        vfs_catalog_supplier=FakeVfsCatalogSupplier(
            snapshot=snapshot,
            snapshots_by_generation={7: snapshot},
        ),
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  vfsDirectory(path: "/Shows/Example Show (2024)/Season 01", generationId: "7") {
                    generationId
                    path
                    searchQuery
                    entry { entryId kind path }
                    focusedEntry { entryId kind path }
                    parent { entryId kind path }
                    breadcrumbs { entryId path name kind }
                    directoryCount
                    fileCount
                    totalDirectoryCount
                    totalFileCount
                    siblingIndex
                    siblingCount
                    previousEntry { entryId kind path }
                    nextEntry { entryId kind path }
                    stats { directoryCount fileCount blockedItemCount }
                    directories { path name kind }
                    files {
                      path
                      name
                      kind
                      correlation { itemId mediaEntryId provider providerDownloadId providerFilePath }
                      file {
                        itemId
                        itemTitle
                        itemExternalRef
                        mediaEntryId
                        mediaType
                        transport
                        locator
                        unrestrictedUrl
                        originalFilename
                        sizeBytes
                        leaseState
                        lastRefreshedAt
                        activeRoles
                        sourceKey
                        queryStrategy
                        providerFamily
                        locatorSource
                        matchBasis
                      }
                    }
                  }
                  vfsCatalogEntry(path: "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv", generationId: "7") {
                    path
                    kind
                    correlation { itemId mediaEntryId providerDownloadId }
                    file { mediaEntryId providerDownloadId restrictedFallback }
                  }
                  vfsOverview(path: "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv", generationId: "7", search: "S01E01") {
                    snapshot {
                      generationId
                      stats { directoryCount fileCount blockedItemCount }
                    }
                    directory {
                      path
                      searchQuery
                      focusedEntry { entryId kind path }
                      siblingIndex
                      siblingCount
                      breadcrumbs { path name kind }
                      fileCount
                      files { path name kind }
                    }
                  }
                  vfsSearch(query: "Example Show", pathPrefix: "/Shows", generationId: "7", kind: "file", limit: 5) {
                    generationId
                    query
                    pathPrefix
                    totalMatches
                    entries { entryId path kind }
                  }
                  vfsFileContext(path: "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv", generationId: "7", search: "S01E01") {
                    generationId
                    siblingFileIndex
                    siblingFileCount
                    previousFile { entryId path kind }
                    nextFile { entryId path kind }
                    file { entryId path kind }
                    directory {
                      path
                      searchQuery
                      focusedEntry { entryId kind path }
                      fileCount
                      files { entryId path kind }
                    }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["vfsDirectory"] == {
        "generationId": "7",
        "path": "/Shows/Example Show (2024)/Season 01",
        "searchQuery": None,
        "entry": {
            "entryId": "dir:/Shows/Example Show (2024)/Season 01",
            "kind": "directory",
            "path": "/Shows/Example Show (2024)/Season 01",
        },
        "focusedEntry": {
            "entryId": "dir:/Shows/Example Show (2024)/Season 01",
            "kind": "directory",
            "path": "/Shows/Example Show (2024)/Season 01",
        },
        "parent": {
            "entryId": "dir:/Shows/Example Show (2024)",
            "kind": "directory",
            "path": "/Shows/Example Show (2024)",
        },
        "breadcrumbs": [
            {"entryId": "dir:/", "path": "/", "name": "/", "kind": "directory"},
            {"entryId": "dir:/Shows", "path": "/Shows", "name": "Shows", "kind": "directory"},
            {
                "entryId": "dir:/Shows/Example Show (2024)",
                "path": "/Shows/Example Show (2024)",
                "name": "Example Show (2024)",
                "kind": "directory",
            },
            {
                "entryId": "dir:/Shows/Example Show (2024)/Season 01",
                "path": "/Shows/Example Show (2024)/Season 01",
                "name": "Season 01",
                "kind": "directory",
            },
        ],
        "directoryCount": 0,
        "fileCount": 1,
        "totalDirectoryCount": 0,
        "totalFileCount": 1,
        "siblingIndex": 0,
        "siblingCount": 1,
        "previousEntry": None,
        "nextEntry": None,
        "stats": {
            "directoryCount": 4,
            "fileCount": 1,
            "blockedItemCount": 0,
        },
        "directories": [],
        "files": [
            {
                "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
                "name": "Example Show S01E01.mkv",
                "kind": "file",
                "correlation": {
                    "itemId": "item-1",
                    "mediaEntryId": "entry-1",
                    "provider": "realdebrid",
                    "providerDownloadId": "torrent-123",
                    "providerFilePath": "/downloads/Example.Show.S01E01.mkv",
                },
                "file": {
                    "itemId": "item-1",
                    "itemTitle": "Example Show",
                    "itemExternalRef": "tvdb:555",
                    "mediaEntryId": "entry-1",
                    "mediaType": "episode",
                    "transport": "remote-direct",
                    "locator": "https://cdn.example.com/stream/entry-1",
                    "unrestrictedUrl": "https://cdn.example.com/stream/entry-1",
                    "originalFilename": "Example Show S01E01.mkv",
                    "sizeBytes": 987654321,
                    "leaseState": "ready",
                    "lastRefreshedAt": "2026-04-13T11:55:00+00:00",
                    "activeRoles": ["direct"],
                    "sourceKey": "persisted",
                    "queryStrategy": "persisted_media_entries",
                    "providerFamily": "debrid",
                    "locatorSource": "unrestricted_url",
                    "matchBasis": "provider_identity",
                },
            }
        ],
    }
    assert payload["vfsCatalogEntry"] == {
        "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
        "kind": "file",
        "correlation": {
            "itemId": "item-1",
            "mediaEntryId": "entry-1",
            "providerDownloadId": "torrent-123",
        },
        "file": {
            "mediaEntryId": "entry-1",
            "providerDownloadId": "torrent-123",
            "restrictedFallback": False,
        },
    }
    assert payload["vfsOverview"] == {
        "snapshot": {
            "generationId": "7",
            "stats": {
                "directoryCount": 4,
                "fileCount": 1,
                "blockedItemCount": 0,
            },
        },
        "directory": {
            "path": "/Shows/Example Show (2024)/Season 01",
            "searchQuery": "S01E01",
            "focusedEntry": {
                "entryId": "file:entry-1",
                "kind": "file",
                "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
            },
            "siblingIndex": 0,
            "siblingCount": 1,
            "breadcrumbs": [
                {"path": "/", "name": "/", "kind": "directory"},
                {"path": "/Shows", "name": "Shows", "kind": "directory"},
                {
                    "path": "/Shows/Example Show (2024)",
                    "name": "Example Show (2024)",
                    "kind": "directory",
                },
                {
                    "path": "/Shows/Example Show (2024)/Season 01",
                    "name": "Season 01",
                    "kind": "directory",
                },
                {
                    "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
                    "name": "Example Show S01E01.mkv",
                    "kind": "file",
                },
            ],
            "fileCount": 1,
            "files": [
                {
                    "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
                    "name": "Example Show S01E01.mkv",
                    "kind": "file",
                }
            ],
        },
    }
    assert payload["vfsSearch"] == {
        "generationId": "7",
        "query": "Example Show",
        "pathPrefix": "/Shows",
        "totalMatches": 1,
        "entries": [
            {
                "entryId": "file:entry-1",
                "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
                "kind": "file",
            },
        ],
    }
    assert payload["vfsFileContext"] == {
        "generationId": "7",
        "siblingFileIndex": 0,
        "siblingFileCount": 1,
        "previousFile": None,
        "nextFile": None,
        "file": {
            "entryId": "file:entry-1",
            "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
            "kind": "file",
        },
        "directory": {
            "path": "/Shows/Example Show (2024)/Season 01",
            "searchQuery": "S01E01",
            "focusedEntry": {
                "entryId": "file:entry-1",
                "kind": "file",
                "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
            },
            "fileCount": 1,
            "files": [
                {
                    "entryId": "file:entry-1",
                    "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
                    "kind": "file",
                }
            ],
        },
    }


def test_graphql_vfs_snapshot_and_blocked_items_queries_use_catalog_snapshot() -> None:
    snapshot = VfsCatalogSnapshot(
        generation_id="12",
        published_at=datetime(2026, 4, 13, 12, 30, tzinfo=UTC),
        entries=(),
        stats=VfsCatalogStats(directory_count=3, file_count=5, blocked_item_count=2),
        blocked_items=(
            cast(
                Any,
                type(
                    "BlockedItem",
                    (),
                    {
                        "item_id": "item-blocked",
                        "external_ref": "tmdb:42",
                        "title": "Blocked Example",
                        "reason": "missing_media_entry",
                    },
                )(),
            ),
            cast(
                Any,
                type(
                    "BlockedItem",
                    (),
                    {
                        "item_id": "item-blocked-2",
                        "external_ref": "tvdb:84",
                        "title": "Second Blocked Example",
                        "reason": "unresolved_query",
                    },
                )(),
            ),
        ),
    )
    client = _build_client(
        FakeMediaService(),
        vfs_catalog_supplier=FakeVfsCatalogSupplier(snapshot=snapshot, snapshots_by_generation={12: snapshot}),
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  vfsSnapshot(generationId: "12") {
                    generationId
                    publishedAt
                    stats { directoryCount fileCount blockedItemCount }
                    blockedItems { itemId externalRef title reason }
                  }
                  vfsBlockedItems(generationId: "12") {
                    itemId
                    externalRef
                    title
                    reason
                  }
                  filtered: vfsBlockedItems(generationId: "12", reason: "missing_media_entry", limit: 1) {
                    itemId
                    externalRef
                    title
                    reason
                  }
                  titleFiltered: vfsBlockedItems(generationId: "12", titleQuery: "Second", externalRef: "tvdb:84") {
                    itemId
                    externalRef
                    title
                    reason
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["vfsSnapshot"] == {
        "generationId": "12",
        "publishedAt": "2026-04-13T12:30:00+00:00",
        "stats": {
            "directoryCount": 3,
            "fileCount": 5,
            "blockedItemCount": 2,
        },
        "blockedItems": [
            {
                "itemId": "item-blocked",
                "externalRef": "tmdb:42",
                "title": "Blocked Example",
                "reason": "missing_media_entry",
            },
            {
                "itemId": "item-blocked-2",
                "externalRef": "tvdb:84",
                "title": "Second Blocked Example",
                "reason": "unresolved_query",
            }
        ],
    }
    assert payload["vfsBlockedItems"] == [
        {
            "itemId": "item-blocked",
            "externalRef": "tmdb:42",
            "title": "Blocked Example",
            "reason": "missing_media_entry",
        },
        {
            "itemId": "item-blocked-2",
            "externalRef": "tvdb:84",
            "title": "Second Blocked Example",
            "reason": "unresolved_query",
        }
    ]
    assert payload["filtered"] == [
        {
            "itemId": "item-blocked",
            "externalRef": "tmdb:42",
            "title": "Blocked Example",
            "reason": "missing_media_entry",
        }
    ]
    assert payload["titleFiltered"] == [
        {
            "itemId": "item-blocked-2",
            "externalRef": "tvdb:84",
            "title": "Second Blocked Example",
            "reason": "unresolved_query",
        }
    ]


def test_graphql_vfs_catalog_rollup_surfaces_query_and_provider_aggregates() -> None:
    snapshot = VfsCatalogSnapshot(
        generation_id="13",
        published_at=datetime(2026, 4, 13, 13, 0, tzinfo=UTC),
        entries=(
            VfsCatalogEntry(
                entry_id="dir-1",
                parent_entry_id=None,
                path="/Shows",
                name="Shows",
                kind="directory",
                directory=VfsCatalogDirectoryEntry(path="/Shows"),
            ),
            VfsCatalogEntry(
                entry_id="file-1",
                parent_entry_id="dir-1",
                path="/Shows/Example Show/Season 01/Episode 01.mkv",
                name="Episode 01.mkv",
                kind="file",
                correlation=VfsCatalogCorrelationKeys(provider_file_path="/downloads/Show/S01E01.mkv"),
                file=VfsCatalogFileEntry(
                    item_id="item-1",
                    item_title="Example Show",
                    item_external_ref="tvdb:100",
                    media_entry_id="entry-1",
                    source_attachment_id=None,
                    media_type="episode",
                    transport="remote-direct",
                    locator="https://cdn.example.test/e1",
                    lease_state="ready",
                    provider="realdebrid",
                    provider_file_path="/downloads/Show/S01E01.mkv",
                    active_roles=("direct", "hls"),
                    query_strategy="persisted_media_entries",
                    provider_family="debrid",
                    locator_source="unrestricted_url",
                    restricted_fallback=False,
                ),
            ),
            VfsCatalogEntry(
                entry_id="file-2",
                parent_entry_id="dir-1",
                path="/Shows/Example Show/Season 01/Episode 02.mkv",
                name="Episode 02.mkv",
                kind="file",
                file=VfsCatalogFileEntry(
                    item_id="item-2",
                    item_title="Example Show",
                    item_external_ref="tvdb:101",
                    media_entry_id="entry-2",
                    source_attachment_id=None,
                    media_type="episode",
                    transport="remote-direct",
                    locator="https://api.example.test/restricted/e2",
                    lease_state="stale",
                    provider="stremthru",
                    active_roles=("direct",),
                    query_strategy="by-media-entry-id",
                    provider_family="stremthru",
                    locator_source="restricted_url",
                    restricted_fallback=True,
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=1, file_count=2, blocked_item_count=2),
        blocked_items=(
            cast(
                Any,
                type(
                    "BlockedItem",
                    (),
                    {
                        "item_id": "item-blocked-1",
                        "external_ref": "tmdb:999",
                        "title": "Blocked One",
                        "reason": "missing_lifecycle",
                    },
                )(),
            ),
            cast(
                Any,
                type(
                    "BlockedItem",
                    (),
                    {
                        "item_id": "item-blocked-2",
                        "external_ref": "tmdb:1000",
                        "title": "Blocked Two",
                        "reason": "unresolved_query",
                    },
                )(),
            ),
        ),
    )
    client = _build_client(
        FakeMediaService(),
        vfs_catalog_supplier=FakeVfsCatalogSupplier(snapshot=snapshot, snapshots_by_generation={13: snapshot}),
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  vfsCatalogRollup(generationId: "13") {
                    blockedReasons { key count }
                    queryStrategies { key count }
                    providerFamilies { key count }
                    leaseStates { key count }
                    locatorSources { key count }
                    restrictedFallbackFileCount
                    providerPathPreservedFileCount
                    multiRoleFileCount
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["vfsCatalogRollup"]
    assert payload["blockedReasons"] == [
        {"key": "missing_lifecycle", "count": 1},
        {"key": "unresolved_query", "count": 1},
    ]
    assert payload["queryStrategies"] == [
        {"key": "by-media-entry-id", "count": 1},
        {"key": "persisted_media_entries", "count": 1},
    ]
    assert payload["providerFamilies"] == [
        {"key": "debrid", "count": 1},
        {"key": "stremthru", "count": 1},
    ]
    assert payload["leaseStates"] == [
        {"key": "ready", "count": 1},
        {"key": "stale", "count": 1},
    ]
    assert payload["locatorSources"] == [
        {"key": "restricted_url", "count": 1},
        {"key": "unrestricted_url", "count": 1},
    ]
    assert payload["restrictedFallbackFileCount"] == 1
    assert payload["providerPathPreservedFileCount"] == 1
    assert payload["multiRoleFileCount"] == 1


def test_graphql_operator_queries_expose_runtime_queue_and_metadata_history() -> None:
    current_ms = datetime.now(UTC).timestamp() * 1000
    redis = FakeOperatorRedis(
        zsets={
            "filmu-py": [
                ("job-ready", current_ms - 30_000),
                ("job-deferred", current_ms + 45_000),
            ],
        },
        lists={
            "arq:queue-status-history:filmu-py": [
                json.dumps(
                    {
                        "observed_at": "2026-04-13T12:01:00Z",
                        "total_jobs": 5,
                        "ready_jobs": 2,
                        "deferred_jobs": 1,
                        "in_progress_jobs": 1,
                        "retry_jobs": 1,
                        "dead_letter_jobs": 2,
                        "oldest_ready_age_seconds": 12.5,
                        "next_scheduled_in_seconds": 42.0,
                        "alert_level": "critical",
                        "dead_letter_oldest_age_seconds": 420.0,
                        "dead_letter_reason_counts": {"provider_timeout": 2},
                    }
                )
            ],
            "arq:metadata-reindex-history:filmu-py": [
                json.dumps(
                    {
                        "observed_at": "2026-04-13T12:02:00Z",
                        "processed": 10,
                        "queued": 3,
                        "reconciled": 6,
                        "skipped_active": 1,
                        "failed": 1,
                        "repair_attempted": 2,
                        "repair_enriched": 1,
                        "repair_skipped_no_tmdb_id": 0,
                        "repair_failed": 1,
                        "repair_requeued": 1,
                        "repair_skipped_active": 0,
                        "outcome": "warning",
                        "run_failed": False,
                        "last_error": "provider_timeout",
                    }
                )
            ],
            "arq:dead-letter:filmu-py": [
                json.dumps({"queued_at": "2026-04-13T11:50:00Z", "reason_code": "provider_timeout"})
            ],
        },
        keys={
            f"{retry_key_prefix}filmu-py:1",
            f"{in_progress_key_prefix}filmu-py:1",
            f"{result_key_prefix}filmu-py:1",
        },
    )
    lifecycle = RuntimeLifecycleState()
    lifecycle.transition(
        RuntimeLifecyclePhase.PLUGIN_REGISTRATION,
        detail="plugins_registered",
        health=RuntimeLifecycleHealth.HEALTHY,
    )
    lifecycle.transition(
        RuntimeLifecyclePhase.STEADY_STATE,
        detail="runtime_steady",
        health=RuntimeLifecycleHealth.HEALTHY,
    )
    client = _build_client(FakeMediaService(), redis=redis, runtime_lifecycle=lifecycle)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  runtimeLifecycle {
                    phase
                    health
                    detail
                    transitions { phase health detail }
                  }
                  workerQueueStatus {
                    queueName
                    arqEnabled
                    totalJobs
                    readyJobs
                    deferredJobs
                    inProgressJobs
                    retryJobs
                    resultJobs
                    deadLetterJobs
                    alertLevel
                    deadLetterReasonCounts
                  }
                  workerQueueHistory(limit: 5, alertLevel: "critical", minDeadLetterJobs: 2, reasonCode: "provider_timeout") {
                    observedAt
                    alertLevel
                    deadLetterJobs
                    deadLetterReasonCounts
                  }
                  workerMetadataReindexStatus {
                    queueName
                    hasHistory
                    processed
                    queued
                    reconciled
                    failed
                    outcome
                    lastError
                  }
                  workerMetadataReindexHistory(limit: 5) {
                    observedAt
                    processed
                    queued
                    reconciled
                    failed
                    repairAttempted
                    repairFailed
                    outcome
                    lastError
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["runtimeLifecycle"]["phase"] == "steady_state"
    assert payload["runtimeLifecycle"]["detail"] == "runtime_steady"
    assert payload["runtimeLifecycle"]["transitions"][-1] == {
        "phase": "steady_state",
        "health": "healthy",
        "detail": "runtime_steady",
    }
    assert payload["workerQueueStatus"] == {
        "queueName": "filmu-py",
        "arqEnabled": False,
        "totalJobs": 2,
        "readyJobs": 1,
        "deferredJobs": 1,
        "inProgressJobs": 1,
        "retryJobs": 1,
        "resultJobs": 1,
        "deadLetterJobs": 1,
        "alertLevel": "critical",
        "deadLetterReasonCounts": {"provider_timeout": 1},
    }
    assert payload["workerQueueHistory"] == [
        {
            "observedAt": "2026-04-13T12:01:00Z",
            "alertLevel": "critical",
            "deadLetterJobs": 2,
            "deadLetterReasonCounts": {"provider_timeout": 2},
        }
    ]
    assert payload["workerMetadataReindexStatus"] == {
        "queueName": "filmu-py",
        "hasHistory": True,
        "processed": 10,
        "queued": 3,
        "reconciled": 6,
        "failed": 1,
        "outcome": "warning",
        "lastError": "provider_timeout",
    }
    assert payload["workerMetadataReindexHistory"] == [
        {
            "observedAt": "2026-04-13T12:02:00Z",
            "processed": 10,
            "queued": 3,
            "reconciled": 6,
            "failed": 1,
            "repairAttempted": 2,
            "repairFailed": 1,
            "outcome": "warning",
            "lastError": "provider_timeout",
        }
    ]


def test_graphql_playback_control_plane_mutations_use_shared_resources() -> None:
    direct_controller = FakePlaybackTriggerController(
        result=DirectPlaybackRefreshControlPlaneTriggerResult(
            item_identifier="item-1",
            outcome="scheduled",
            scheduling_result=DirectPlaybackRefreshSchedulingResult(
                outcome="scheduled",
                scheduled_request=DirectPlaybackRefreshScheduleRequest(
                    item_identifier="item-1",
                    recommendation=DirectPlaybackRefreshRecommendation(
                        reason="provider_direct_stale",
                        target="attachment",
                        target_id="attachment-1",
                    ),
                    requested_at=datetime(2026, 4, 13, 12, 10, tzinfo=UTC),
                    not_before=datetime(2026, 4, 13, 12, 15, tzinfo=UTC),
                    retry_after_seconds=30.0,
                ),
                retry_after_seconds=30.0,
            ),
        )
    )
    failed_controller = FakePlaybackTriggerController(
        result=HlsFailedLeaseRefreshControlPlaneTriggerResult(
            item_identifier="item-1",
            outcome="no_action",
            refresh_result=HlsFailedLeaseRefreshResult(
                item_identifier="item-1",
                outcome="completed",
                execution=MediaEntryLeaseRefreshExecution(
                    media_entry_id="entry-1",
                    ok=True,
                    refresh_state="ready",
                    locator="https://cdn.example.com/hls.m3u8",
                    error=None,
                ),
            ),
        )
    )
    restricted_controller = FakePlaybackTriggerController(
        result=HlsRestrictedFallbackRefreshControlPlaneTriggerResult(
            item_identifier="item-1",
            outcome="no_action",
            refresh_result=HlsRestrictedFallbackRefreshResult(
                item_identifier="item-1",
                outcome="run_later",
                execution=MediaEntryLeaseRefreshExecution(
                    media_entry_id="entry-2",
                    ok=False,
                    refresh_state="refreshing",
                    locator="https://restricted.example.com/hls.m3u8",
                    error="provider_circuit_open",
                ),
                retry_after_seconds=45.0,
                deferred_reason="provider_circuit_open",
            ),
        )
    )
    playback_service = FakePlaybackService(stale_result=True)
    client = _build_client(
        FakeMediaService(),
        queued_direct_playback_refresh_controller=direct_controller,
        queued_hls_failed_lease_refresh_controller=failed_controller,
        queued_hls_restricted_fallback_refresh_controller=restricted_controller,
        playback_service=playback_service,
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                mutation {
                  direct: triggerDirectPlaybackRefresh(itemId: "item-1") {
                    itemId
                    outcome
                    controllerAttached
                    controlPlaneOutcome
                    refreshOutcome
                    retryAfterSeconds
                    scheduledRequestedAt
                    scheduledNotBefore
                  }
                  failed: triggerHlsFailedLeaseRefresh(itemId: "item-1") {
                    itemId
                    outcome
                    controllerAttached
                    controlPlaneOutcome
                    refreshOutcome
                    executionOk
                    executionRefreshState
                    executionLocator
                  }
                  restricted: triggerHlsRestrictedFallbackRefresh(itemId: "item-1") {
                    itemId
                    outcome
                    controllerAttached
                    controlPlaneOutcome
                    refreshOutcome
                    executionOk
                    executionRefreshState
                    executionLocator
                    executionError
                    retryAfterSeconds
                    deferredReason
                  }
                  stale: markSelectedHlsMediaEntryStale(itemId: "item-1") {
                    itemId
                    success
                    error
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["direct"] == {
        "itemId": "item-1",
        "outcome": "triggered",
        "controllerAttached": True,
        "controlPlaneOutcome": "scheduled",
        "refreshOutcome": "scheduled",
        "retryAfterSeconds": 30.0,
        "scheduledRequestedAt": "2026-04-13T12:10:00+00:00",
        "scheduledNotBefore": "2026-04-13T12:15:00+00:00",
    }
    assert payload["failed"] == {
        "itemId": "item-1",
        "outcome": "triggered",
        "controllerAttached": True,
        "controlPlaneOutcome": "no_action",
        "refreshOutcome": "completed",
        "executionOk": True,
        "executionRefreshState": "ready",
        "executionLocator": "https://cdn.example.com/hls.m3u8",
    }
    assert payload["restricted"] == {
        "itemId": "item-1",
        "outcome": "triggered",
        "controllerAttached": True,
        "controlPlaneOutcome": "no_action",
        "refreshOutcome": "run_later",
        "executionOk": False,
        "executionRefreshState": "refreshing",
        "executionLocator": "https://restricted.example.com/hls.m3u8",
        "executionError": "provider_circuit_open",
        "retryAfterSeconds": 45.0,
        "deferredReason": "provider_circuit_open",
    }
    assert payload["stale"] == {
        "itemId": "item-1",
        "success": True,
        "error": None,
    }
    assert direct_controller.triggered_item_ids == ["item-1"]
    assert failed_controller.triggered_item_ids == ["item-1"]
    assert restricted_controller.triggered_item_ids == ["item-1"]
    assert playback_service.stale_item_ids == ["item-1"]


def test_graphql_mark_selected_hls_media_entry_stale_reports_unavailable_service() -> None:
    client = _build_client(FakeMediaService())

    response = client.post(
        "/graphql",
        json={
            "query": """
                mutation {
                  markSelectedHlsMediaEntryStale(itemId: "item-1") {
                    itemId
                    success
                    error
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["markSelectedHlsMediaEntryStale"] == {
        "itemId": "item-1",
        "success": False,
        "error": "playback_service_unavailable",
    }


def test_graphql_persist_media_entry_control_state_uses_shared_playback_service() -> None:
    persisted_item = SimpleNamespace(
        id="item-1",
        active_streams=[
            SimpleNamespace(role="direct", media_entry_id="entry-1"),
        ],
    )
    persisted_entry = SimpleNamespace(
        id="entry-1",
        entry_type="media",
        kind="remote-direct",
        original_filename="Example.Movie.mkv",
        local_path=None,
        download_url="https://api.example.com/direct-fresh",
        unrestricted_url="https://cdn.example.com/direct-fresh",
        provider="realdebrid",
        provider_download_id="download-1",
        provider_file_id="file-1",
        provider_file_path="/downloads/Example.Movie.mkv",
        size_bytes=123456789,
        created_at=datetime(2026, 4, 13, 12, 20, tzinfo=UTC),
        updated_at=datetime(2026, 4, 13, 12, 25, tzinfo=UTC),
        refresh_state="ready",
        expires_at=datetime(2026, 4, 13, 13, 0, tzinfo=UTC),
        last_refreshed_at=None,
        last_refresh_error=None,
    )
    playback_service = FakePlaybackService(
        persist_result=PersistedMediaEntryControlMutationResult(
            item_identifier="item-1",
            media_entry_id="entry-1",
            item=persisted_item,
            media_entry=persisted_entry,
            applied_role="direct",
        )
    )
    client = _build_client(FakeMediaService(), playback_service=playback_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                mutation Persist($input: PersistMediaEntryControlInput!) {
                  persistMediaEntryControlState(input: $input) {
                    itemId
                    mediaEntryId
                    success
                    error
                    appliedRole
                    mediaEntry {
                      entryType
                      kind
                      downloadUrl
                      unrestrictedUrl
                      refreshState
                      providerFileId
                      activeForDirect
                      activeForHls
                      isActiveStream
                    }
                  }
                }
            """,
            "variables": {
                "input": {
                    "itemId": "item-1",
                    "mediaEntryId": "entry-1",
                    "activeRole": "DIRECT",
                    "downloadUrl": "https://api.example.com/direct-fresh",
                    "unrestrictedUrl": "https://cdn.example.com/direct-fresh",
                    "refreshState": "ready",
                    "expiresAt": "2026-04-13T13:00:00Z",
                }
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["persistMediaEntryControlState"] == {
        "itemId": "item-1",
        "mediaEntryId": "entry-1",
        "success": True,
        "error": None,
        "appliedRole": "direct",
        "mediaEntry": {
            "entryType": "media",
            "kind": "remote-direct",
            "downloadUrl": "https://api.example.com/direct-fresh",
            "unrestrictedUrl": "https://cdn.example.com/direct-fresh",
            "refreshState": "ready",
            "providerFileId": "file-1",
            "activeForDirect": True,
            "activeForHls": False,
            "isActiveStream": True,
        },
    }
    assert playback_service.persist_calls == [
        {
            "item_identifier": "item-1",
            "media_entry_id": "entry-1",
            "active_role": "direct",
            "local_path": None,
            "download_url": "https://api.example.com/direct-fresh",
            "unrestricted_url": "https://cdn.example.com/direct-fresh",
            "refresh_state": "ready",
            "last_refresh_error": None,
            "expires_at": datetime(2026, 4, 13, 13, 0, tzinfo=UTC),
        }
    ]


def test_graphql_persist_media_entry_control_state_rejects_empty_mutation() -> None:
    client = _build_client(FakeMediaService(), playback_service=FakePlaybackService())

    response = client.post(
        "/graphql",
        json={
            "query": """
                mutation Persist($input: PersistMediaEntryControlInput!) {
                  persistMediaEntryControlState(input: $input) {
                    itemId
                    mediaEntryId
                    success
                    error
                  }
                }
            """,
            "variables": {
                "input": {
                    "itemId": "item-1",
                    "mediaEntryId": "entry-1",
                }
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["persistMediaEntryControlState"] == {
        "itemId": "item-1",
        "mediaEntryId": "entry-1",
        "success": False,
        "error": "no_changes_requested",
    }


def test_graphql_persist_playback_attachment_control_state_uses_shared_playback_service() -> None:
    persisted_item = SimpleNamespace(id="item-1")
    persisted_attachment = SimpleNamespace(
        id="attachment-1",
        kind="remote-direct",
        locator="https://cdn.example.com/direct-fresh",
        source_key="persisted",
        provider="realdebrid",
        provider_download_id="download-1",
        provider_file_id="file-1",
        provider_file_path="/downloads/Example.Movie.mkv",
        original_filename="Example.Movie.mkv",
        file_size=123456789,
        local_path=None,
        restricted_url="https://api.example.com/direct-fresh",
        unrestricted_url="https://cdn.example.com/direct-fresh",
        is_preferred=True,
        preference_rank=1,
        refresh_state="ready",
        expires_at=datetime(2026, 4, 13, 13, 30, tzinfo=UTC),
        last_refreshed_at=None,
        last_refresh_error=None,
    )
    linked_entry = SimpleNamespace(
        entry_type="media",
        kind="remote-direct",
        original_filename="Example.Movie.mkv",
        url="https://cdn.example.com/direct-fresh",
        local_path=None,
        download_url="https://api.example.com/direct-fresh",
        unrestricted_url="https://cdn.example.com/direct-fresh",
        provider="realdebrid",
        provider_download_id="download-1",
        provider_file_id="file-1",
        provider_file_path="/downloads/Example.Movie.mkv",
        size=123456789,
        created="2026-04-13T12:20:00+00:00",
        modified="2026-04-13T12:25:00+00:00",
        refresh_state="ready",
        expires_at="2026-04-13T13:30:00+00:00",
        last_refreshed_at=None,
        last_refresh_error=None,
        active_for_direct=False,
        active_for_hls=False,
        is_active_stream=False,
    )
    playback_service = FakePlaybackService(
        attachment_persist_result=PersistedPlaybackAttachmentControlMutationResult(
            item_identifier="item-1",
            attachment_id="attachment-1",
            item=persisted_item,
            attachment=persisted_attachment,
            linked_media_entries=(linked_entry,),
        )
    )
    client = _build_client(FakeMediaService(), playback_service=playback_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                mutation Persist($input: PersistPlaybackAttachmentControlInput!) {
                  persistPlaybackAttachmentControlState(input: $input) {
                    itemId
                    attachmentId
                    success
                    error
                    attachment {
                      locator
                      restrictedUrl
                      unrestrictedUrl
                      refreshState
                    }
                    linkedMediaEntries {
                      entryType
                      downloadUrl
                      unrestrictedUrl
                      refreshState
                    }
                  }
                }
            """,
            "variables": {
                "input": {
                    "itemId": "item-1",
                    "attachmentId": "attachment-1",
                    "locator": "https://cdn.example.com/direct-fresh",
                    "restrictedUrl": "https://api.example.com/direct-fresh",
                    "unrestrictedUrl": "https://cdn.example.com/direct-fresh",
                    "refreshState": "ready",
                    "expiresAt": "2026-04-13T13:30:00Z",
                }
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["persistPlaybackAttachmentControlState"] == {
        "itemId": "item-1",
        "attachmentId": "attachment-1",
        "success": True,
        "error": None,
        "attachment": {
            "locator": "https://cdn.example.com/direct-fresh",
            "restrictedUrl": "https://api.example.com/direct-fresh",
            "unrestrictedUrl": "https://cdn.example.com/direct-fresh",
            "refreshState": "ready",
        },
        "linkedMediaEntries": [
            {
                "entryType": "media",
                "downloadUrl": "https://api.example.com/direct-fresh",
                "unrestrictedUrl": "https://cdn.example.com/direct-fresh",
                "refreshState": "ready",
            }
        ],
    }
    assert playback_service.attachment_persist_calls == [
        {
            "item_identifier": "item-1",
            "attachment_id": "attachment-1",
            "locator": "https://cdn.example.com/direct-fresh",
            "local_path": None,
            "restricted_url": "https://api.example.com/direct-fresh",
            "unrestricted_url": "https://cdn.example.com/direct-fresh",
            "refresh_state": "ready",
            "last_refresh_error": None,
            "expires_at": datetime(2026, 4, 13, 13, 30, tzinfo=UTC),
        }
    ]


def test_graphql_persist_playback_attachment_control_state_rejects_empty_mutation() -> None:
    client = _build_client(FakeMediaService(), playback_service=FakePlaybackService())

    response = client.post(
        "/graphql",
        json={
            "query": """
                mutation Persist($input: PersistPlaybackAttachmentControlInput!) {
                  persistPlaybackAttachmentControlState(input: $input) {
                    itemId
                    attachmentId
                    success
                    error
                  }
                }
            """,
            "variables": {
                "input": {
                    "itemId": "item-1",
                    "attachmentId": "attachment-1",
                }
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["persistPlaybackAttachmentControlState"] == {
        "itemId": "item-1",
        "attachmentId": "attachment-1",
        "success": False,
        "error": "no_changes_requested",
    }
