"""GraphQL projection query tests for the dual-surface API strategy."""

from __future__ import annotations

import fnmatch
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from arq.constants import in_progress_key_prefix, result_key_prefix, retry_key_prefix
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.api.routes import default as default_routes
from filmu_py.api.routes import runtime_governance
from filmu_py.config import Settings
from filmu_py.core import byte_streaming
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
from filmu_py.observability_convergence import EXPECTED_CORRELATION_FIELDS
from filmu_py.plugins import TestPluginContext
from filmu_py.plugins.builtins import register_builtin_plugins
from filmu_py.plugins.manifest import PluginManifest
from filmu_py.plugins.registry import PluginCapabilityKind, PluginRegistry
from filmu_py.resources import AppResources
from filmu_py.services.access_policy import snapshot_from_settings
from filmu_py.services import governance_posture
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
    VfsCatalogDelta,
    VfsCatalogDirectoryEntry,
    VfsCatalogEntry,
    VfsCatalogFileEntry,
    VfsCatalogRemoval,
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


class FailingAuthorizationAuditService:
    async def record_decision(self, **payload: Any) -> None:
        _ = payload
        raise RuntimeError("audit store unavailable")


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


class DummyAccessPolicyService:
    def __init__(self, settings: Settings) -> None:
        self.snapshot = snapshot_from_settings(settings.access_policy)
        now = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
        self.revisions: list[Any] = [self._build_revision_record(self.snapshot, now, True)]

    def _build_revision_record(self, snapshot: Any, at: datetime, is_active: bool) -> Any:
        record = type("AccessPolicyRevisionRecord", (), {})()
        record.version = snapshot.version
        record.source = snapshot.source
        record.approval_status = "approved" if is_active else "draft"
        record.proposed_by = "tenant-main:operator-1"
        record.approved_by = "tenant-main:operator-1" if is_active else None
        record.approved_at = at if is_active else None
        record.approval_notes = "test"
        record.is_active = is_active
        record.activated_at = at
        record.created_at = at
        record.updated_at = at
        record.role_grants = snapshot.role_grants
        record.principal_roles = snapshot.principal_roles
        record.principal_scopes = snapshot.principal_scopes
        record.principal_tenant_grants = snapshot.principal_tenant_grants
        record.permission_constraints = snapshot.permission_constraints
        record.audit_decisions = snapshot.audit_decisions
        record.alerting_enabled = snapshot.alerting_enabled
        record.repeated_denial_warning_threshold = snapshot.repeated_denial_warning_threshold
        record.repeated_denial_critical_threshold = snapshot.repeated_denial_critical_threshold
        record.to_snapshot = lambda snapshot=snapshot: snapshot
        return record

    async def list_revisions(self, *, limit: int = 20) -> list[Any]:
        return self.revisions[:limit]

    async def write_revision(
        self,
        *,
        version: str,
        source: str,
        role_grants: dict[str, list[str]],
        principal_roles: dict[str, list[str]],
        principal_scopes: dict[str, list[str]],
        principal_tenant_grants: dict[str, list[str]],
        permission_constraints: dict[str, dict[str, list[str]]],
        audit_decisions: bool,
        alerting_enabled: bool,
        repeated_denial_warning_threshold: int,
        repeated_denial_critical_threshold: int,
        proposed_by: str | None = None,
        approval_notes: str | None = None,
        auto_approve: bool = False,
        activate: bool = False,
    ) -> Any:
        now = datetime(2026, 4, 11, 12, 30, tzinfo=UTC)
        if activate and not auto_approve:
            raise ValueError("access policy revision must be approved before activation")
        if activate:
            for revision in self.revisions:
                revision.is_active = False
        snapshot = type(self.snapshot)(
            version=version,
            source=source,
            role_grants=role_grants,
            principal_roles=principal_roles,
            principal_scopes=principal_scopes,
            principal_tenant_grants=principal_tenant_grants,
            permission_constraints=permission_constraints,
            audit_decisions=audit_decisions,
            alerting_enabled=alerting_enabled,
            repeated_denial_warning_threshold=repeated_denial_warning_threshold,
            repeated_denial_critical_threshold=repeated_denial_critical_threshold,
        )
        record = self._build_revision_record(snapshot, now, activate)
        record.approval_status = "approved" if auto_approve else "draft"
        record.proposed_by = proposed_by
        record.approved_by = proposed_by if auto_approve else None
        record.approved_at = now if auto_approve else None
        record.approval_notes = approval_notes
        self.snapshot = snapshot if activate else self.snapshot
        self.revisions.insert(0, record)
        return record

    async def activate_revision(self, version: str) -> Any:
        for revision in self.revisions:
            if revision.version == version and revision.approval_status not in {
                "approved",
                "bootstrap",
            }:
                raise ValueError(
                    f"access policy revision '{version}' must be approved before activation"
                )
            revision.is_active = revision.version == version
            if revision.version == version:
                self.snapshot = revision.to_snapshot()
                return revision
        raise LookupError(f"unknown access policy revision '{version}'")

    async def approve_revision(
        self,
        version: str,
        *,
        approved_by: str | None,
        approval_notes: str | None = None,
        activate: bool = False,
    ) -> Any:
        for revision in self.revisions:
            if revision.version != version:
                continue
            revision.approval_status = "approved"
            revision.approved_by = approved_by
            revision.approved_at = datetime(2026, 4, 11, 12, 45, tzinfo=UTC)
            revision.approval_notes = approval_notes
            if activate:
                for candidate in self.revisions:
                    candidate.is_active = candidate.version == version
                self.snapshot = revision.to_snapshot()
            return revision
        raise LookupError(f"unknown access policy revision '{version}'")

    async def reject_revision(
        self,
        version: str,
        *,
        rejected_by: str | None,
        approval_notes: str | None = None,
    ) -> Any:
        for revision in self.revisions:
            if revision.version != version:
                continue
            revision.approval_status = "rejected"
            revision.approved_by = rejected_by
            revision.approval_notes = approval_notes
            revision.is_active = False
            return revision
        raise LookupError(f"unknown access policy revision '{version}'")


class DummyPluginGovernanceService:
    def __init__(self) -> None:
        self.overrides: dict[str, Any] = {}

    async def list_overrides(self) -> dict[str, Any]:
        return dict(self.overrides)

    async def write_override(
        self,
        *,
        plugin_name: str,
        state: str,
        reason: str | None = None,
        notes: str | None = None,
        updated_by: str | None = None,
    ) -> Any:
        now = datetime(2026, 4, 11, 12, 50, tzinfo=UTC)
        record = type("PluginGovernanceOverrideRecord", (), {})()
        record.plugin_name = plugin_name
        record.state = state
        record.reason = reason
        record.notes = notes
        record.updated_by = updated_by
        record.created_at = now
        record.updated_at = now
        self.overrides[plugin_name] = record
        return record


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

    async def build_delta_since(self, generation_id: int) -> VfsCatalogDelta | None:
        previous = self.snapshots_by_generation.get(generation_id)
        current = self.snapshot
        if previous is None or current is None:
            return None
        previous_by_id = {entry.entry_id: entry for entry in previous.entries}
        current_by_id = {entry.entry_id: entry for entry in current.entries}
        upserts = tuple(
            entry
            for entry_id, entry in current_by_id.items()
            if previous_by_id.get(entry_id) != entry
        )
        removals = tuple(
            VfsCatalogRemoval(
                entry_id=entry.entry_id,
                path=entry.path,
                kind=entry.kind,
                correlation=entry.correlation,
            )
            for entry_id, entry in previous_by_id.items()
            if entry_id not in current_by_id
        )
        return VfsCatalogDelta(
            generation_id=current.generation_id,
            base_generation_id=previous.generation_id,
            published_at=current.published_at,
            upserts=upserts,
            removals=removals,
            stats=current.stats,
        )

    async def history_generation_ids(self) -> tuple[str, ...]:
        return tuple(str(generation_id) for generation_id in sorted(self.snapshots_by_generation))


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
class FakeVfsCatalogServer:
    counters: dict[str, int] = field(default_factory=dict)

    def build_governance_snapshot(self) -> dict[str, int]:
        return dict(self.counters)


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


def _graphql_headers(*scopes: str, roles: str = "playback:operator") -> dict[str, str]:
    return {
        "x-api-key": "a" * 32,
        "x-actor-id": "operator-1",
        "x-tenant-id": "tenant-main",
        "x-actor-roles": roles,
        "x-actor-scopes": ",".join(scopes),
    }


def _allow_graphql_control_plane_permissions(settings: Settings) -> None:
    for permission in ("settings:write", "security:policy.approve"):
        constraint = settings.access_policy.permission_constraints.setdefault(permission, {})
        route_prefixes = constraint.setdefault("route_prefixes", [])
        if "/graphql" not in route_prefixes:
            route_prefixes.append("/graphql")


def _build_client(
    media_service: FakeMediaService,
    *,
    settings_overrides: dict[str, Any] | None = None,
    vfs_catalog_supplier: FakeVfsCatalogSupplier | None = None,
    vfs_catalog_server: object | None = None,
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
        vfs_catalog_server=vfs_catalog_server,  # type: ignore[arg-type]
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
                  observabilityRolloutSummary {
                    status
                    pipelineStageCount
                    readyStageCount
                    productionEvidenceCount
                    productionEvidenceReady
                    grpcRustTraceReady
                    otlpExportReady
                    searchIndexReady
                    alertRolloutReady
                    readyStageNames
                    blockedStageNames
                    requiredActions
                    remainingGaps
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["observabilityConvergence"]["status"] == "ready"
    assert payload["observabilityConvergence"]["summary"] == {
        "pipelineStageCount": 5,
        "readyStageCount": 5,
        "productionEvidenceReady": True,
        "grpcRustTraceReady": True,
        "otlpExportReady": True,
        "searchIndexReady": True,
        "alertRolloutReady": True,
    }
    assert payload["observabilityConvergence"]["structuredLoggingEnabled"] is True
    assert payload["observabilityConvergence"]["grpcBindAddress"] == "127.0.0.1:50051"
    assert (
        payload["observabilityConvergence"]["grpcServiceName"]
        == "filmu.vfs.catalog.v1.FilmuVfsCatalogService"
    )
    assert payload["observabilityConvergence"]["otelEnabled"] is True
    assert payload["observabilityConvergence"]["otelEndpointConfigured"] is True
    assert (
        payload["observabilityConvergence"]["otlpEndpoint"]
        == "http://collector.internal:4318"
    )
    assert payload["observabilityConvergence"]["logShipperEnabled"] is True
    assert payload["observabilityConvergence"]["logShipperType"] == "vector"
    assert (
        payload["observabilityConvergence"]["logShipperTarget"]
        == "opensearch://logs-filmu"
    )
    assert payload["observabilityConvergence"]["logShipperTargetConfigured"] is True
    assert payload["observabilityConvergence"]["logShipperHealthcheckConfigured"] is True
    assert payload["observabilityConvergence"]["searchBackend"] == "opensearch"
    assert payload["observabilityConvergence"]["environmentShippingEnabled"] is True
    assert payload["observabilityConvergence"]["alertingEnabled"] is True
    assert payload["observabilityConvergence"]["rustTraceCorrelationEnabled"] is True
    assert payload["observabilityConvergence"]["correlationContractComplete"] is True
    assert payload["observabilityConvergence"]["expectedCorrelationFieldsReady"] is True
    assert payload["observabilityConvergence"]["traceContextHeaders"] == [
        "traceparent",
        "tracestate",
        "baggage",
    ]
    assert payload["observabilityConvergence"]["correlationHeaders"] == [
        "x-request-id",
        "x-tenant-id",
        "x-filmu-vfs-session-id",
        "x-filmu-vfs-daemon-id",
        "x-filmu-vfs-entry-id",
        "x-filmu-vfs-provider-file-id",
        "x-filmu-vfs-handle-key",
    ]
    assert payload["observabilityConvergence"]["sharedCrossProcessHeaders"][0] == "traceparent"
    assert payload["observabilityConvergence"]["expectedCorrelationFields"] == [
        "request.id",
        "trace.id",
        "tenant.id",
        "vfs.session_id",
        "vfs.daemon_id",
        "catalog.entry_id",
        "provider.file_id",
        "vfs.handle_key",
    ]
    assert payload["observabilityConvergence"]["missingExpectedCorrelationFields"] == []
    assert payload["observabilityConvergence"]["requiredCorrelationFields"] == [
        "request.id",
        "trace.id",
        "tenant.id",
        "vfs.session_id",
        "vfs.daemon_id",
        "catalog.entry_id",
        "provider.file_id",
        "vfs.handle_key",
    ]
    assert payload["observabilityConvergence"]["pipelineStages"] == [
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
    assert payload["observabilityConvergence"]["proofRefs"] == [
        "ops/wave4/log-pipeline-rollout.md"
    ]
    assert payload["observabilityConvergence"]["proofArtifacts"] == [
        {
            "ref": "ops/wave4/log-pipeline-rollout.md",
            "category": "observability_rollout",
            "label": "observability rollout proof",
            "recorded": True,
        }
    ]
    assert payload["observabilityConvergence"]["requiredActions"] == []
    assert payload["observabilityConvergence"]["remainingGaps"] == []
    assert payload["observabilityRolloutSummary"] == {
        "status": "ready",
        "pipelineStageCount": 5,
        "readyStageCount": 5,
        "productionEvidenceCount": 1,
        "productionEvidenceReady": True,
        "grpcRustTraceReady": True,
        "otlpExportReady": True,
        "searchIndexReady": True,
        "alertRolloutReady": True,
        "readyStageNames": [
            "python_structured_logging",
            "grpc_rust_correlation",
            "otlp_export",
            "log_shipping_and_search",
            "alerting_and_rollout_evidence",
        ],
        "blockedStageNames": [],
        "requiredActions": [],
        "remainingGaps": [],
    }


def test_graphql_observability_support_queries_return_contract_counts_inventory_actions_and_gaps() -> None:
    client = _build_client(
        FakeMediaService(),
        settings_overrides={
            "FILMU_PY_OTEL_ENABLED": True,
            "FILMU_PY_OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector.internal:4318",
            "FILMU_PY_LOG_SHIPPER": {
                "enabled": True,
                "type": "vector",
                "target": "opensearch://logs-filmu",
            },
            "FILMU_PY_OBSERVABILITY": {
                "search_backend": "opensearch",
                "environment_shipping_enabled": False,
                "alerting_enabled": False,
                "rust_trace_correlation_enabled": False,
                "required_correlation_fields": ["request.id", "trace.id", "tenant.id"],
                "proof_refs": ["   ", "ops/observability/rollout.md"],
            },
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  observabilityFieldContractSummary {
                    totalRequiredCorrelationFields
                    expectedFieldCount
                    configuredExpectedFieldCount
                    missingExpectedFieldCount
                    traceContextHeaderCount
                    correlationHeaderCount
                    sharedHeaderCount
                  }
                  observabilityStageCounts {
                    status
                    count
                  }
                  observabilityProofInventory(recorded: true) {
                    ref
                    category
                    recorded
                  }
                  observabilityActions {
                    domain
                    subject
                    severity
                    status
                    action
                  }
                  observabilityGaps {
                    domain
                    subject
                    severity
                    status
                    message
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["observabilityFieldContractSummary"] == {
        "totalRequiredCorrelationFields": 3,
        "expectedFieldCount": len(EXPECTED_CORRELATION_FIELDS),
        "configuredExpectedFieldCount": 3,
        "missingExpectedFieldCount": len(EXPECTED_CORRELATION_FIELDS) - 3,
        "traceContextHeaderCount": 3,
        "correlationHeaderCount": 7,
        "sharedHeaderCount": 10,
    }
    assert sum(row["count"] for row in payload["observabilityStageCounts"]) == 5
    assert payload["observabilityProofInventory"] == [
        {
            "ref": "ops/observability/rollout.md",
            "category": "observability_rollout",
            "recorded": True,
        }
    ]
    assert any(
        row["domain"] == "observability" and row["subject"] == "observability_convergence"
        for row in payload["observabilityActions"]
    )
    assert any(
        row["domain"] == "observability" and row["subject"] == "observability_convergence"
        for row in payload["observabilityGaps"]
    )


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
                    claimLimit
                    maxClaimPasses
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
                    pendingRecoveryReady
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
        "claimLimit": 100,
        "maxClaimPasses": 3,
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
        "pendingRecoveryReady": True,
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


def test_graphql_control_plane_support_queries_return_grouped_counts_and_feeds() -> None:
    class FakeControlPlaneService:
        async def summarize_subscribers(self, *, active_within_seconds: int) -> object:
            _ = active_within_seconds
            return SimpleNamespace(
                total_subscribers=2,
                active_subscribers=1,
                stale_subscribers=1,
                error_subscribers=0,
                fenced_subscribers=1,
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
                  controlPlaneStatusCounts(activeWithinSeconds: 180) {
                    key
                    count
                  }
                  controlPlaneConsumerSummaries(activeWithinSeconds: 180) {
                    consumerName
                    subscriberCount
                    activeSubscribers
                    ackPendingSubscribers
                    fencedSubscribers
                    errorSubscribers
                    latestHeartbeatAt
                  }
                  controlPlaneNodeCounts(activeWithinSeconds: 180) {
                    key
                    count
                  }
                  controlPlaneTenantCounts(activeWithinSeconds: 180) {
                    key
                    count
                  }
                  controlPlaneOwnershipSummary(activeWithinSeconds: 180) {
                    totalSubscribers
                    activeSubscribers
                    staleSubscribers
                    fencedSubscribers
                    ackPendingSubscribers
                    uniqueConsumers
                    uniqueNodes
                    uniqueTenants
                  }
                  controlPlaneReplayConsumerCounts {
                    key
                    count
                  }
                  controlPlaneActions(activeWithinSeconds: 180) {
                    domain
                    subject
                    action
                  }
                  controlPlaneGaps(activeWithinSeconds: 180) {
                    domain
                    subject
                    message
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["controlPlaneStatusCounts"] == [
        {"key": "active", "count": 1},
        {"key": "stale", "count": 1},
    ]
    assert payload["controlPlaneConsumerSummaries"] == [
        {
            "consumerName": "worker-a",
            "subscriberCount": 1,
            "activeSubscribers": 0,
            "ackPendingSubscribers": 1,
            "fencedSubscribers": 1,
            "errorSubscribers": 0,
            "latestHeartbeatAt": "2026-04-16T09:59:00+00:00",
        }
    ]
    assert payload["controlPlaneNodeCounts"] == [{"key": "node-a", "count": 1}]
    assert payload["controlPlaneTenantCounts"] == [{"key": "tenant-main", "count": 1}]
    assert payload["controlPlaneOwnershipSummary"] == {
        "totalSubscribers": 1,
        "activeSubscribers": 0,
        "staleSubscribers": 1,
        "fencedSubscribers": 1,
        "ackPendingSubscribers": 1,
        "uniqueConsumers": 1,
        "uniqueNodes": 1,
        "uniqueTenants": 1,
    }
    assert payload["controlPlaneReplayConsumerCounts"] == [
        {"key": "worker-a", "count": 2},
        {"key": "worker-b", "count": 1},
    ]
    assert any(
        row["domain"] == "control_plane"
        and row["subject"] == "control_plane_summary"
        and row["action"] == "recover_stale_control_plane_subscribers"
        for row in payload["controlPlaneActions"]
    )
    assert any(
        row["domain"] == "control_plane"
        and row["subject"] == "control_plane_summary"
        and row["message"] == "control-plane backlog needs recovery"
        for row in payload["controlPlaneGaps"]
    )


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
                    pendingRecoveryReady
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
        "pendingRecoveryReady": True,
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
                    selectedProviderSource
                    enabledProviderCount
                    configuredProviderCount
                    builtinEnabledProviderCount
                    pluginEnabledProviderCount
                    multiProviderEnabled
                    pluginDownloadersRegistered
                    workerPluginDispatchReady
                    orderedFailoverReady
                    fanoutReady
                    multiContainerReady
                    providerPriorityOrder
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
                    publishableEventCount
                    hookSubscriptionCount
                    wiringStatus
                  }
                  filteredPluginEvents: pluginEvents(wiringStatus: "subscriber_only") {
                    name
                    wiringStatus
                  }
                  pluginGovernance {
                    summary {
                      totalPlugins
                      nonBuiltinPlugins
                      unsignedExternalPlugins
                      scraperPlugins
                      eventHookPlugins
                      overrideCount
                      approvedOverrides
                      quarantinedOverrides
                      revokedOverrides
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
                      overrideState
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
        "selectedProviderSource": "builtin",
        "enabledProviderCount": 2,
        "configuredProviderCount": 2,
        "builtinEnabledProviderCount": 2,
        "pluginEnabledProviderCount": 0,
        "multiProviderEnabled": True,
        "pluginDownloadersRegistered": 0,
        "workerPluginDispatchReady": False,
        "orderedFailoverReady": True,
        "fanoutReady": True,
        "multiContainerReady": True,
        "providerPriorityOrder": ["realdebrid", "alldebrid", "debridlink", "stremthru"],
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
            "publishableEventCount": 1,
            "hookSubscriptionCount": 0,
            "wiringStatus": "publisher_only",
        },
        {
            "name": "hook-plugin",
            "publisher": None,
            "publishableEvents": [],
            "hookSubscriptions": ["item.completed", "item.state.changed"],
            "publishableEventCount": 0,
            "hookSubscriptionCount": 2,
            "wiringStatus": "subscriber_only",
        },
    ]
    assert payload["filteredPluginEvents"] == [
        {
            "name": "hook-plugin",
            "wiringStatus": "subscriber_only",
        }
    ]
    governance = payload["pluginGovernance"]
    assert governance["summary"] == {
        "totalPlugins": 2,
        "nonBuiltinPlugins": 2,
        "unsignedExternalPlugins": 2,
        "scraperPlugins": 1,
        "eventHookPlugins": 1,
        "overrideCount": 0,
        "approvedOverrides": 0,
        "quarantinedOverrides": 0,
        "revokedOverrides": 0,
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
        "overrideState": None,
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
        "overrideState": None,
        "warnings": [],
    }


def test_graphql_downloader_execution_evidence_returns_dead_letter_and_history_posture() -> None:
    redis = FakeOperatorRedis(
        lists={
            "arq:queue-status-history:filmu-py": [
                json.dumps(
                    {
                        "observed_at": "2026-04-16T11:10:00Z",
                        "total_jobs": 6,
                        "ready_jobs": 2,
                        "deferred_jobs": 1,
                        "in_progress_jobs": 1,
                        "retry_jobs": 2,
                        "dead_letter_jobs": 2,
                        "alert_level": "critical",
                        "dead_letter_reason_counts": {
                            "provider_timeout": 1,
                            "provider_rate_limit": 1,
                        },
                    }
                ),
                json.dumps(
                    {
                        "observed_at": "2026-04-16T11:05:00Z",
                        "total_jobs": 3,
                        "ready_jobs": 1,
                        "deferred_jobs": 0,
                        "in_progress_jobs": 1,
                        "retry_jobs": 1,
                        "dead_letter_jobs": 0,
                        "alert_level": "warning",
                        "dead_letter_reason_counts": {},
                    }
                ),
            ],
            "arq:dead-letter:filmu-py": [
                json.dumps(
                    {
                        "stage": "debrid_item",
                        "task": "debrid_item",
                        "item_id": "item-2",
                        "reason": "provider timeout",
                        "reason_code": "provider_timeout",
                        "idempotency_key": "item-2:timeout",
                        "attempt": 2,
                        "queued_at": "2026-04-16T11:12:00Z",
                        "metadata": {
                            "provider": "alldebrid",
                            "failure_kind": "timeout",
                            "selected_stream_id": "stream-2",
                            "item_request_id": "request-2",
                        },
                    }
                ),
                json.dumps(
                    {
                        "stage": "debrid_item",
                        "task": "debrid_item",
                        "item_id": "item-1",
                        "reason": "provider rate limited",
                        "reason_code": "provider_rate_limit",
                        "idempotency_key": "item-1:ratelimit",
                        "attempt": 1,
                        "queued_at": "2026-04-16T11:15:00Z",
                        "metadata": {
                            "provider": "realdebrid",
                            "failure_kind": "rate_limit",
                            "selected_stream_id": "stream-1",
                            "item_request_id": "request-1",
                            "status_code": 429,
                            "retry_after_seconds": 30,
                        },
                    }
                ),
            ],
        }
    )
    client = _build_client(
        FakeMediaService(),
        redis=redis,
        settings_overrides={
            "FILMU_PY_DOWNLOADERS": {
                "real_debrid": {"enabled": True, "api_key": "rd-token"},
                "all_debrid": {"enabled": True, "api_key": "ad-token"},
            }
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  downloaderExecutionEvidence(provider: "realdebrid", failureKind: "rate_limit", limit: 5) {
                    queueName
                    status
                    selectionMode
                    orderedFailoverReady
                    fanoutReady
                    providerCounts { key count }
                    failureKindCounts { key count }
                    deadLetterReasonCounts { key count }
                    historySummary {
                      pointCount
                      warningPointCount
                      criticalPointCount
                      maxTotalJobs
                      maxReadyJobs
                      maxRetryJobs
                      maxDeadLetterJobs
                      latestAlertLevel
                      deadLetterReasonCounts { key count }
                    }
                    recentDeadLetters {
                      itemId
                      reasonCode
                      provider
                      failureKind
                      selectedStreamId
                      itemRequestId
                      statusCode
                      retryAfterSeconds
                    }
                    requiredActions
                    remainingGaps
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["downloaderExecutionEvidence"] == {
        "queueName": "filmu-py",
        "status": "ready",
        "selectionMode": "ordered_failover",
        "orderedFailoverReady": True,
        "fanoutReady": True,
        "providerCounts": [
            {"key": "alldebrid", "count": 1},
            {"key": "realdebrid", "count": 1},
        ],
        "failureKindCounts": [
            {"key": "rate_limit", "count": 1},
            {"key": "timeout", "count": 1},
        ],
        "deadLetterReasonCounts": [
            {"key": "provider_rate_limit", "count": 1},
            {"key": "provider_timeout", "count": 1},
        ],
        "historySummary": {
            "pointCount": 2,
            "warningPointCount": 1,
            "criticalPointCount": 1,
            "maxTotalJobs": 6,
            "maxReadyJobs": 2,
            "maxRetryJobs": 2,
            "maxDeadLetterJobs": 2,
            "latestAlertLevel": "critical",
            "deadLetterReasonCounts": [
                {"key": "provider_rate_limit", "count": 1},
                {"key": "provider_timeout", "count": 1},
            ],
        },
        "recentDeadLetters": [
            {
                "itemId": "item-1",
                "reasonCode": "provider_rate_limit",
                "provider": "realdebrid",
                "failureKind": "rate_limit",
                "selectedStreamId": "stream-1",
                "itemRequestId": "request-1",
                "statusCode": 429,
                "retryAfterSeconds": 30,
            }
        ],
        "requiredActions": [],
        "remainingGaps": [],
    }


def test_graphql_downloader_execution_evidence_ignores_boolean_dead_letter_status_fields() -> None:
    redis = FakeOperatorRedis(
        lists={
            "arq:queue-status-history:filmu-py": [
                json.dumps(
                    {
                        "observed_at": "2026-04-16T11:10:00Z",
                        "total_jobs": 1,
                        "ready_jobs": 0,
                        "deferred_jobs": 0,
                        "in_progress_jobs": 0,
                        "retry_jobs": 1,
                        "dead_letter_jobs": 1,
                        "alert_level": "warning",
                        "dead_letter_reason_counts": {"provider_rate_limit": 1},
                    }
                )
            ],
            "arq:dead-letter:filmu-py": [
                json.dumps(
                    {
                        "stage": "debrid_item",
                        "task": "debrid_item",
                        "item_id": "item-bool",
                        "reason": "provider rate limited",
                        "reason_code": "provider_rate_limit",
                        "idempotency_key": "item-bool:ratelimit",
                        "attempt": 1,
                        "queued_at": "2026-04-16T11:15:00Z",
                        "metadata": {
                            "provider": "realdebrid",
                            "failure_kind": "rate_limit",
                            "selected_stream_id": "stream-bool",
                            "item_request_id": "request-bool",
                            "status_code": True,
                            "retry_after_seconds": False,
                        },
                    }
                )
            ],
        }
    )
    client = _build_client(
        FakeMediaService(),
        redis=redis,
        settings_overrides={
            "FILMU_PY_DOWNLOADERS": {
                "real_debrid": {"enabled": True, "api_key": "rd-token"},
            }
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  downloaderExecutionEvidence(limit: 5) {
                    recentDeadLetters {
                      itemId
                      statusCode
                      retryAfterSeconds
                    }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["downloaderExecutionEvidence"]["recentDeadLetters"] == [
        {
            "itemId": "item-bool",
            "statusCode": None,
            "retryAfterSeconds": None,
        }
    ]


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
    previous_snapshot = VfsCatalogSnapshot(
        generation_id="6",
        published_at=datetime(2026, 4, 13, 11, 50, tzinfo=UTC),
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
                    lease_state="refreshing",
                    last_refreshed_at=datetime(2026, 4, 13, 11, 45, tzinfo=UTC),
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
            VfsCatalogEntry(
                entry_id="file:entry-removed",
                parent_entry_id="dir:/Shows/Example Show (2024)/Season 01",
                path="/Shows/Example Show (2024)/Season 01/Example Show S01E00.mkv",
                name="Example Show S01E00.mkv",
                kind="file",
                correlation=VfsCatalogCorrelationKeys(
                    item_id="item-removed",
                    media_entry_id="entry-removed",
                    provider="realdebrid",
                ),
                file=VfsCatalogFileEntry(
                    item_id="item-removed",
                    item_title="Example Show",
                    item_external_ref="tvdb:555",
                    media_entry_id="entry-removed",
                    source_attachment_id="attachment-removed",
                    media_type="episode",
                    transport="remote-direct",
                    locator="https://cdn.example.com/stream/entry-removed",
                    lease_state="ready",
                    provider_family="debrid",
                    locator_source="locator",
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=4, file_count=2, blocked_item_count=0),
    )
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
            snapshots_by_generation={6: previous_snapshot, 7: snapshot},
        ),
        vfs_catalog_server=FakeVfsCatalogServer(
            counters={
                "vfs_catalog_watch_sessions_active": 1,
                "vfs_catalog_reconnect_requested": 2,
                "vfs_catalog_reconnect_delta_served": 1,
                "vfs_catalog_reconnect_snapshot_fallback": 0,
                "vfs_catalog_reconnect_failures": 0,
                "vfs_catalog_snapshots_served": 3,
                "vfs_catalog_deltas_served": 4,
                "vfs_catalog_heartbeats_served": 5,
                "vfs_catalog_problem_events": 0,
                "vfs_catalog_request_stream_failures": 0,
                "vfs_catalog_refresh_attempts": 6,
                "vfs_catalog_refresh_succeeded": 5,
                "vfs_catalog_refresh_provider_failures": 1,
                "vfs_catalog_refresh_validation_failed": 0,
                "vfs_catalog_inline_refresh_requests": 2,
                "vfs_catalog_inline_refresh_succeeded": 2,
                "vfs_catalog_inline_refresh_failed": 0,
            }
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
                    exactMatchCount
                    directoryMatches
                    fileMatches
                    mediaTypeCounts { key count }
                    providerFamilyCounts { key count }
                    leaseStateCounts { key count }
                    entries { entryId path kind }
                  }
                  filteredVfsSearch: vfsSearch(
                    query: "Example Show",
                    pathPrefix: "/Shows",
                    generationId: "7",
                    kind: "file",
                    mediaType: "episode",
                    providerFamily: "debrid",
                    limit: 5
                  ) {
                    totalMatches
                    entries { entryId path kind }
                  }
                  vfsCatalogGovernance {
                    status
                    counters { key count }
                    summary {
                      activeWatchSessions
                      reconnectRequests
                      reconnectDeltaServed
                      snapshotsServed
                      deltasServed
                      refreshAttempts
                      refreshSucceeded
                      refreshProviderFailures
                    }
                    requiredActions
                    remainingGaps
                  }
                  vfsCatalogDelta(baseGenerationId: "6") {
                    generationId
                    baseGenerationId
                    publishedAt
                    upsertDirectoryCount
                    upsertFileCount
                    removalDirectoryCount
                    removalFileCount
                    providerFamilyCounts { key count }
                    leaseStateCounts { key count }
                  }
                  vfsMountDiagnostics {
                    status
                    supplierAttached
                    serverAttached
                    currentGenerationId
                    currentPublishedAt
                    historyGenerationIds
                    historyGenerationCount
                    deltaHistoryReady
                    activeWatchSessions
                    snapshotsServed
                    deltasServed
                    reconnectDeltaServed
                    reconnectSnapshotFallbacks
                    reconnectFailures
                    requestStreamFailures
                    problemEvents
                    refreshProviderFailures
                    refreshValidationFailures
                    requiredActions
                    remainingGaps
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
        "exactMatchCount": 0,
        "directoryMatches": 0,
        "fileMatches": 1,
        "mediaTypeCounts": [{"key": "episode", "count": 1}],
        "providerFamilyCounts": [{"key": "debrid", "count": 1}],
        "leaseStateCounts": [{"key": "ready", "count": 1}],
        "entries": [
            {
                "entryId": "file:entry-1",
                "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
                "kind": "file",
            },
        ],
    }
    assert payload["filteredVfsSearch"] == {
        "totalMatches": 1,
        "entries": [
            {
                "entryId": "file:entry-1",
                "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
                "kind": "file",
            }
        ],
    }
    assert payload["vfsCatalogGovernance"] == {
        "status": "partial",
        "counters": [
            {"key": "vfs_catalog_deltas_served", "count": 4},
            {"key": "vfs_catalog_heartbeats_served", "count": 5},
            {"key": "vfs_catalog_inline_refresh_failed", "count": 0},
            {"key": "vfs_catalog_inline_refresh_requests", "count": 2},
            {"key": "vfs_catalog_inline_refresh_succeeded", "count": 2},
            {"key": "vfs_catalog_problem_events", "count": 0},
            {"key": "vfs_catalog_reconnect_delta_served", "count": 1},
            {"key": "vfs_catalog_reconnect_failures", "count": 0},
            {"key": "vfs_catalog_reconnect_requested", "count": 2},
            {"key": "vfs_catalog_reconnect_snapshot_fallback", "count": 0},
            {"key": "vfs_catalog_refresh_attempts", "count": 6},
            {"key": "vfs_catalog_refresh_provider_failures", "count": 1},
            {"key": "vfs_catalog_refresh_succeeded", "count": 5},
            {"key": "vfs_catalog_refresh_validation_failed", "count": 0},
            {"key": "vfs_catalog_request_stream_failures", "count": 0},
            {"key": "vfs_catalog_snapshots_served", "count": 3},
            {"key": "vfs_catalog_watch_sessions_active", "count": 1},
        ],
        "summary": {
            "activeWatchSessions": 1,
            "reconnectRequests": 2,
            "reconnectDeltaServed": 1,
            "snapshotsServed": 3,
            "deltasServed": 4,
            "refreshAttempts": 6,
            "refreshSucceeded": 5,
            "refreshProviderFailures": 1,
        },
        "requiredActions": ["reduce_vfs_catalog_refresh_provider_failures"],
        "remainingGaps": ["vfs catalog refresh provider failures were observed in the current runtime"],
    }
    assert payload["vfsCatalogDelta"] == {
        "generationId": "7",
        "baseGenerationId": "6",
        "publishedAt": "2026-04-13T12:00:00+00:00",
        "upsertDirectoryCount": 0,
        "upsertFileCount": 1,
        "removalDirectoryCount": 0,
        "removalFileCount": 1,
        "providerFamilyCounts": [{"key": "debrid", "count": 1}],
        "leaseStateCounts": [{"key": "ready", "count": 1}],
    }
    assert payload["vfsMountDiagnostics"] == {
        "status": "partial",
        "supplierAttached": True,
        "serverAttached": True,
        "currentGenerationId": "7",
        "currentPublishedAt": "2026-04-13T12:00:00+00:00",
        "historyGenerationIds": ["6", "7"],
        "historyGenerationCount": 2,
        "deltaHistoryReady": True,
        "activeWatchSessions": 1,
        "snapshotsServed": 3,
        "deltasServed": 4,
        "reconnectDeltaServed": 1,
        "reconnectSnapshotFallbacks": 0,
        "reconnectFailures": 0,
        "requestStreamFailures": 0,
        "problemEvents": 0,
        "refreshProviderFailures": 1,
        "refreshValidationFailures": 0,
        "requiredActions": ["reduce_vfs_catalog_refresh_provider_failures"],
        "remainingGaps": ["vfs catalog refresh provider failures were observed in the current runtime"],
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


def test_graphql_rollout_evidence_and_runtime_rollout_queries_return_typed_governance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "filmu_py.services.governance_posture.runtime_governance._playback_gate_governance_snapshot",
        lambda: {
            "playback_gate_environment_class": "canary",
            "playback_gate_gate_mode": "enforced",
            "playback_gate_runner_status": "ready",
            "playback_gate_runner_ready": 1,
            "playback_gate_runner_required_failures": 0,
            "playback_gate_provider_gate_required": 1,
            "playback_gate_provider_gate_ran": 1,
            "playback_gate_provider_parity_ready": 1,
            "playback_gate_windows_provider_ready": 1,
            "playback_gate_windows_provider_movie_ready": 1,
            "playback_gate_windows_provider_tv_ready": 1,
            "playback_gate_windows_provider_coverage": ["movie", "tv"],
            "playback_gate_windows_soak_ready": 1,
            "playback_gate_windows_soak_repeat_count": 4,
            "playback_gate_windows_soak_profile_coverage_complete": 1,
            "playback_gate_windows_soak_profile_coverage": [
                "continuous",
                "seek",
                "concurrent",
                "full",
            ],
            "playback_gate_policy_validation_status": "ready",
            "playback_gate_policy_ready": 1,
            "playback_gate_rollout_readiness": "ready",
            "playback_gate_rollout_reasons": ["playback_gate_green"],
            "playback_gate_rollout_next_action": "keep_required_checks_enforced",
        },
    )
    monkeypatch.setattr(
        "filmu_py.services.governance_posture.runtime_governance._vfs_runtime_governance_snapshot",
        lambda playback_gate_governance=None: {
            "vfs_runtime_snapshot_available": 1,
            "vfs_runtime_open_handles": 8,
            "vfs_runtime_active_reads": 3,
            "vfs_runtime_cache_pressure_class": "healthy",
            "vfs_runtime_refresh_pressure_class": "healthy",
            "vfs_runtime_provider_pressure_incidents": 0,
            "vfs_runtime_fairness_pressure_incidents": 0,
            "vfs_runtime_rollout_readiness": "ready",
            "vfs_runtime_rollout_next_action": "promote_to_next_environment_class",
            "vfs_runtime_rollout_canary_decision": "promote_to_next_environment_class",
            "vfs_runtime_rollout_merge_gate": "ready",
            "vfs_runtime_rollout_environment_class": "canary",
            "vfs_runtime_rollout_reasons": ["no_blocking_runtime_signals"],
        },
    )

    client = _build_client(
        FakeMediaService(),
        settings_overrides={
            "FILMU_PY_OIDC": {
                "enabled": True,
                "rollout_stage": "enforced",
                "rollout_evidence_refs": ["ops/identity/oidc-rollout.md"],
            },
            "FILMU_PY_PLUGIN_RUNTIME": {
                "proof_refs": ["ops/plugins/runtime-rollout.md"],
            },
            "FILMU_PY_OBSERVABILITY": {
                "proof_refs": ["ops/observability/rollout.md"],
            },
            "FILMU_PY_CONTROL_PLANE": {
                "event_backplane": "redis_stream",
                "proof_refs": ["ops/control-plane/replay-soak.md"],
            },
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  enterpriseRolloutEvidence {
                    status
                    totalCheckCount
                    readyCheckCount
                    requiredActions
                    remainingGaps
                    checks {
                      key
                      status
                      recorded
                      ready
                      evidenceRefs
                    }
                  }
                  playbackGateGovernance {
                    status
                    rolloutReadiness
                    environmentClass
                    runnerStatus
                    providerGateRequired
                    providerGateRan
                    windowsSoakReady
                    policyValidationStatus
                    requiredActions
                    remainingGaps
                  }
                  vfsRuntimeRollout {
                    status
                    rolloutReadiness
                    canaryDecision
                    mergeGate
                    environmentClass
                    snapshotAvailable
                    openHandles
                    activeReads
                    reasons
                    requiredActions
                    remainingGaps
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["enterpriseRolloutEvidence"]["status"] == "ready"
    assert payload["enterpriseRolloutEvidence"]["totalCheckCount"] == 7
    assert payload["enterpriseRolloutEvidence"]["readyCheckCount"] == 7
    checks = {row["key"]: row for row in payload["enterpriseRolloutEvidence"]["checks"]}
    assert checks["observability_rollout"]["evidenceRefs"] == ["ops/observability/rollout.md"]
    assert checks["control_plane_replay"]["ready"] is True
    assert payload["playbackGateGovernance"] == {
        "status": "ready",
        "rolloutReadiness": "ready",
        "environmentClass": "canary",
        "runnerStatus": "ready",
        "providerGateRequired": True,
        "providerGateRan": True,
        "windowsSoakReady": True,
        "policyValidationStatus": "ready",
        "requiredActions": ["keep_required_checks_enforced"],
        "remainingGaps": [],
    }
    assert payload["vfsRuntimeRollout"] == {
        "status": "ready",
        "rolloutReadiness": "ready",
        "canaryDecision": "promote_to_next_environment_class",
        "mergeGate": "ready",
        "environmentClass": "canary",
        "snapshotAvailable": True,
        "openHandles": 8,
        "activeReads": 3,
        "reasons": ["no_blocking_runtime_signals"],
        "requiredActions": ["promote_to_next_environment_class"],
        "remainingGaps": ["vfs rollout reason: no_blocking_runtime_signals"],
    }


def test_graphql_vfs_runtime_telemetry_uses_a_single_governance_snapshot_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    playback_gate_calls = 0
    runtime_calls = 0

    def fake_playback_gate_governance_snapshot() -> dict[str, Any]:
        nonlocal playback_gate_calls
        playback_gate_calls += 1
        return {
            "playback_gate_environment_class": "canary",
            "playback_gate_rollout_readiness": "ready",
        }

    def fake_vfs_runtime_governance_snapshot(
        playback_gate_governance: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        nonlocal runtime_calls
        runtime_calls += 1
        assert playback_gate_governance == {
            "playback_gate_environment_class": "canary",
            "playback_gate_rollout_readiness": "ready",
        }
        return {
            "vfs_runtime_snapshot_available": 1,
            "vfs_runtime_open_handles": 4,
            "vfs_runtime_active_reads": 2,
            "vfs_runtime_cache_pressure_class": "healthy",
            "vfs_runtime_refresh_pressure_class": "healthy",
            "vfs_runtime_provider_pressure_incidents": 0,
            "vfs_runtime_fairness_pressure_incidents": 0,
            "vfs_runtime_rust_handle_age_p50_ms": 10.0,
            "vfs_runtime_rust_handle_age_p95_ms": 20.0,
            "vfs_runtime_rust_handle_age_p99_ms": 30.0,
            "vfs_runtime_rust_handle_age_max_ms": 40.0,
            "vfs_runtime_mounted_reads_bucket_le_5ms": 1,
            "vfs_runtime_mounted_reads_bucket_le_25ms": 2,
            "vfs_runtime_mounted_reads_bucket_le_100ms": 3,
            "vfs_runtime_mounted_reads_bucket_le_250ms": 4,
            "vfs_runtime_mounted_reads_bucket_gt_250ms": 5,
            "vfs_runtime_rollout_readiness": "ready",
            "vfs_runtime_rollout_next_action": "promote_to_next_environment_class",
            "vfs_runtime_rollout_canary_decision": "promote_to_next_environment_class",
            "vfs_runtime_rollout_merge_gate": "ready",
            "vfs_runtime_rollout_environment_class": "canary",
            "vfs_runtime_rollout_reasons": ["no_blocking_runtime_signals"],
            "vfs_runtime_python_bytes_per_read": 0.0,
            "vfs_runtime_mounted_reads_total": 0,
            "vfs_runtime_upstream_fetch_bytes_total": 0,
        }

    monkeypatch.setattr(
        "filmu_py.services.governance_posture.runtime_governance._playback_gate_governance_snapshot",
        fake_playback_gate_governance_snapshot,
    )
    monkeypatch.setattr(
        "filmu_py.services.governance_posture.runtime_governance._vfs_runtime_governance_snapshot",
        fake_vfs_runtime_governance_snapshot,
    )
    monkeypatch.setattr(
        "filmu_py.services.governance_posture.runtime_governance._load_vfs_runtime_status_payload",
        lambda: None,
    )
    monkeypatch.setattr(governance_posture.byte_streaming, "get_active_session_snapshot", lambda: [])
    monkeypatch.setattr(governance_posture.byte_streaming, "get_active_handle_snapshot", lambda: [])

    telemetry = governance_posture.build_vfs_runtime_telemetry_posture(
        cast(AppResources, object())
    )

    assert playback_gate_calls == 1
    assert runtime_calls == 1
    assert telemetry.status == "ready"
    assert telemetry.rust_snapshot_available is True
    assert telemetry.required_actions == ["promote_to_next_environment_class"]
    assert telemetry.remaining_gaps == ["vfs rollout reason: no_blocking_runtime_signals"]


def test_graphql_vfs_runtime_telemetry_returns_cross_view_rollups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    playback_gate_calls = 0
    runtime_snapshot_calls = 0

    real_runtime_snapshot = runtime_governance._vfs_runtime_governance_snapshot

    def _playback_gate_snapshot() -> dict[str, object]:
        nonlocal playback_gate_calls
        playback_gate_calls += 1
        return {
            "playback_gate_environment_class": "windows-native:managed",
            "playback_gate_windows_soak_ready": 1,
        }

    def _runtime_snapshot(
        *,
        playback_gate_governance: dict[str, object],
    ) -> dict[str, object]:
        nonlocal runtime_snapshot_calls
        runtime_snapshot_calls += 1
        return real_runtime_snapshot(playback_gate_governance=playback_gate_governance)

    monkeypatch.setattr(
        runtime_governance,
        "_playback_gate_governance_snapshot",
        _playback_gate_snapshot,
    )
    monkeypatch.setattr(
        runtime_governance,
        "_vfs_runtime_governance_snapshot",
        _runtime_snapshot,
    )
    runtime_status_path = tmp_path / "filmuvfs-runtime-status.json"
    runtime_status_path.write_text(
        json.dumps(
            {
                "runtime": {
                    "open_handles": 3,
                    "active_reads": 1,
                    "active_handle_age_percentiles_ms": {
                        "p50_ms": 15.0,
                        "p95_ms": 80.0,
                        "p99_ms": 120.0,
                        "max_ms": 120.0,
                    },
                    "handle_depth_rollups": [
                        {
                            "tenant_id": "global",
                            "session_id": "mount-session-1",
                            "open_handles": 3,
                            "invalidated_handles": 1,
                            "average_depth": 3.5,
                            "max_depth": 5,
                            "average_age_ms": 48.0,
                            "max_age_ms": 120.0,
                        }
                    ],
                },
                "mounted_reads": {
                    "total": 4,
                    "duration_buckets": [
                        {"label": "le_5_ms", "count": 1},
                        {"label": "le_25_ms", "count": 2},
                        {"label": "le_100_ms", "count": 1},
                        {"label": "le_250_ms", "count": 0},
                        {"label": "gt_250_ms", "count": 0},
                    ],
                },
                "upstream_fetch": {
                    "operations": 4,
                    "bytes_total": 8192,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FILMU_PY_VFS_RUNTIME_STATUS_PATH", str(runtime_status_path))

    python_view_path = tmp_path / "python-runtime-view.mkv"
    python_view_path.write_bytes(b"1" * 64)
    session = byte_streaming.open_mount_session(resource=str(python_view_path))
    handle = byte_streaming.open_local_file_handle(session=session, path=python_view_path)
    handle.created_at = datetime.now(UTC) - timedelta(milliseconds=2200)
    byte_streaming.read_from_handle(handle=handle, chunk_size=2048)

    client = _build_client(FakeMediaService())
    try:
        response = client.post(
            "/graphql",
            json={
                "query": """
                    query {
                      vfsRuntimeTelemetry {
                        status
                        rustSnapshotAvailable
                        pythonActiveSessionCount
                        pythonActiveHandleCount
                        rustHandleAgeMs { p50Ms p95Ms p99Ms maxMs }
                        pythonHandleAgeMs { p50Ms p95Ms p99Ms maxMs }
                        mountedReadDurationBuckets { key count }
                        rustHandleDepthRollups {
                          tenantId
                          sessionId
                          openHandles
                          invalidatedHandles
                          averageDepth
                          maxDepth
                          averageAgeMs
                          maxAgeMs
                        }
                        pythonSessionRollups {
                          owner
                          sessionId
                          resource
                          openHandles
                          readOperations
                          bytesServed
                          averageAgeMs
                          p95AgeMs
                          averageDepth
                          maxDepth
                          bytesPerRead
                        }
                        readAmplification {
                          view
                          totalOperations
                          totalBytes
                          bytesPerRead
                        }
                        requiredActions
                        remainingGaps
                      }
                    }
                """
            },
        )

        assert response.status_code == 200
        payload = response.json()["data"]["vfsRuntimeTelemetry"]
        assert payload["status"] == "ready"
        assert payload["rustSnapshotAvailable"] is True
        assert payload["pythonActiveSessionCount"] == 1
        assert payload["pythonActiveHandleCount"] == 1
        assert payload["rustHandleAgeMs"] == {
            "p50Ms": 15.0,
            "p95Ms": 80.0,
            "p99Ms": 120.0,
            "maxMs": 120.0,
        }
        assert payload["pythonHandleAgeMs"]["p50Ms"] >= 1000.0
        assert payload["pythonHandleAgeMs"]["maxMs"] >= payload["pythonHandleAgeMs"]["p50Ms"]
        assert payload["mountedReadDurationBuckets"] == [
            {"key": "gt_250_ms", "count": 0},
            {"key": "le_100_ms", "count": 1},
            {"key": "le_250_ms", "count": 0},
            {"key": "le_25_ms", "count": 2},
            {"key": "le_5_ms", "count": 1},
        ]
        assert payload["rustHandleDepthRollups"] == [
            {
                "tenantId": "global",
                "sessionId": "mount-session-1",
                "openHandles": 3,
                "invalidatedHandles": 1,
                "averageDepth": 3.5,
                "maxDepth": 5,
                "averageAgeMs": 48.0,
                "maxAgeMs": 120.0,
            }
        ]
        assert len(payload["pythonSessionRollups"]) == 1
        python_rollup = payload["pythonSessionRollups"][0]
        assert python_rollup["owner"] == "future-vfs"
        assert python_rollup["sessionId"] == session.session_id
        assert python_rollup["resource"] == str(python_view_path)
        assert python_rollup["openHandles"] == 1
        assert python_rollup["readOperations"] == 1
        assert python_rollup["bytesServed"] == 2048
        assert python_rollup["averageAgeMs"] >= 1000.0
        assert python_rollup["p95AgeMs"] >= python_rollup["averageAgeMs"]
        expected_depth = len(
            [segment for segment in str(python_view_path).replace("\\", "/").split("/") if segment]
        )
        assert python_rollup["averageDepth"] == float(expected_depth)
        assert python_rollup["maxDepth"] == expected_depth
        assert python_rollup["bytesPerRead"] == 2048.0
        assert payload["readAmplification"] == [
            {
                "view": "rust_mount",
                "totalOperations": 4,
                "totalBytes": 8192,
                "bytesPerRead": 2048.0,
            },
            {
                "view": "python_serving",
                "totalOperations": 1,
                "totalBytes": 2048,
                "bytesPerRead": 2048.0,
            },
        ]
        assert payload["requiredActions"] == ["promote_to_next_environment_class"]
        assert payload["remainingGaps"] == ["vfs rollout reason: no_blocking_runtime_signals"]
        assert playback_gate_calls == 1
        assert runtime_snapshot_calls == 1
    finally:
        byte_streaming.release_handle(handle)
        byte_streaming.release_serving_session(session)
        tracked_path = byte_streaming.get_path_by_key(
            category="local-file",
            path=str(python_view_path),
        )
        if tracked_path is not None and tracked_path.active_handle_count == 0:
            byte_streaming._ACTIVE_PATHS.pop(tracked_path.path_id, None)
            byte_streaming._PATHS_BY_KEY.pop(
                (tracked_path.category, tracked_path.path),
                None,
            )


def test_graphql_plugin_runtime_overview_and_warnings_use_shared_posture() -> None:
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
            "listrr": {
                "enabled": True,
                "url": "https://listrr.example",
                "list_ids": [],
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
            "FILMU_PY_CONTENT": plugin_settings["content"],
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  pluginRuntimeOverview {
                    status
                    totalPlugins
                    readyPlugins
                    wiringReadyPlugins
                    contractValidatedPlugins
                    soakValidatedPlugins
                    warningCount
                    recommendedActions
                    remainingGaps
                  }
                  pluginRuntimeWarnings {
                    pluginName
                    source
                    severity
                    status
                    message
                    capabilityKind
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["pluginRuntimeOverview"]["status"] == "partial"
    assert payload["pluginRuntimeOverview"]["totalPlugins"] >= 2
    assert payload["pluginRuntimeOverview"]["contractValidatedPlugins"] >= 1
    assert payload["pluginRuntimeOverview"]["soakValidatedPlugins"] >= 1
    assert payload["pluginRuntimeOverview"]["warningCount"] >= 1
    warnings = payload["pluginRuntimeWarnings"]
    assert any(
        row["pluginName"] == "listrr"
        and row["source"] == "integration"
        and row["capabilityKind"] == "content_service"
        for row in warnings
    )


def test_graphql_downloader_execution_history_and_dead_letters_return_filtered_results() -> None:
    redis = FakeOperatorRedis(
        lists={
            "arq:queue-status-history:filmu-py": [
                json.dumps(
                    {
                        "observed_at": "2026-04-16T11:10:00Z",
                        "total_jobs": 6,
                        "ready_jobs": 2,
                        "deferred_jobs": 1,
                        "in_progress_jobs": 1,
                        "retry_jobs": 2,
                        "dead_letter_jobs": 2,
                        "alert_level": "critical",
                        "dead_letter_reason_counts": {
                            "provider_timeout": 1,
                            "provider_rate_limit": 1,
                        },
                    }
                ),
                json.dumps(
                    {
                        "observed_at": "2026-04-16T11:05:00Z",
                        "total_jobs": 3,
                        "ready_jobs": 1,
                        "deferred_jobs": 0,
                        "in_progress_jobs": 1,
                        "retry_jobs": 0,
                        "dead_letter_jobs": 0,
                        "alert_level": "ok",
                        "dead_letter_reason_counts": {},
                    }
                ),
            ],
            "arq:dead-letter:filmu-py": [
                json.dumps(
                    {
                        "stage": "debrid_item",
                        "task": "debrid_item",
                        "item_id": "item-2",
                        "reason": "provider timeout",
                        "reason_code": "provider_timeout",
                        "idempotency_key": "item-2:timeout",
                        "attempt": 2,
                        "queued_at": "2026-04-16T11:12:00Z",
                        "metadata": {
                            "provider": "alldebrid",
                            "failure_kind": "timeout",
                            "selected_stream_id": "stream-2",
                            "item_request_id": "request-2",
                        },
                    }
                ),
                json.dumps(
                    {
                        "stage": "debrid_item",
                        "task": "debrid_item",
                        "item_id": "item-1",
                        "reason": "provider rate limited",
                        "reason_code": "provider_rate_limit",
                        "idempotency_key": "item-1:ratelimit",
                        "attempt": 1,
                        "queued_at": "2026-04-16T11:15:00Z",
                        "metadata": {
                            "provider": "realdebrid",
                            "failure_kind": "rate_limit",
                            "selected_stream_id": "stream-1",
                            "item_request_id": "request-1",
                            "status_code": 429,
                            "retry_after_seconds": 30,
                        },
                    }
                ),
            ],
        }
    )
    client = _build_client(FakeMediaService(), redis=redis)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  downloaderExecutionHistory(alertLevel: "critical", reasonCode: "provider_rate_limit") {
                    observedAt
                    alertLevel
                    deadLetterJobs
                    deadLetterReasonCounts
                  }
                  downloaderExecutionDeadLetters(provider: "realdebrid", failureKind: "rate_limit") {
                    itemId
                    reasonCode
                    provider
                    failureKind
                    statusCode
                    retryAfterSeconds
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["downloaderExecutionHistory"] == [
        {
            "observedAt": "2026-04-16T11:10:00Z",
            "alertLevel": "critical",
            "deadLetterJobs": 2,
            "deadLetterReasonCounts": {
                "provider_timeout": 1,
                "provider_rate_limit": 1,
            },
        }
    ]
    assert payload["downloaderExecutionDeadLetters"] == [
        {
            "itemId": "item-1",
            "reasonCode": "provider_rate_limit",
            "provider": "realdebrid",
            "failureKind": "rate_limit",
            "statusCode": 429,
            "retryAfterSeconds": 30,
        }
    ]


def test_graphql_vfs_generation_history_returns_rollups_and_delta_counts() -> None:
    snapshot_11 = VfsCatalogSnapshot(
        generation_id="11",
        published_at=datetime(2026, 4, 16, 10, 0, tzinfo=UTC),
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
                entry_id="file:entry-1",
                parent_entry_id="dir:/",
                path="/Example.mkv",
                name="Example.mkv",
                kind="file",
                file=VfsCatalogFileEntry(
                    item_id="item-1",
                    item_title="Example",
                    item_external_ref="tmdb:1",
                    media_entry_id="entry-1",
                    source_attachment_id="attachment-1",
                    media_type="movie",
                    transport="remote-direct",
                    locator="https://cdn.example.com/1",
                    lease_state="ready",
                    query_strategy="persisted_media_entries",
                    provider_family="debrid",
                    locator_source="unrestricted_url",
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=1, file_count=1, blocked_item_count=0),
    )
    snapshot_12 = VfsCatalogSnapshot(
        generation_id="12",
        published_at=datetime(2026, 4, 16, 10, 5, tzinfo=UTC),
        entries=(
            snapshot_11.entries[0],
            snapshot_11.entries[1],
            VfsCatalogEntry(
                entry_id="file:entry-2",
                parent_entry_id="dir:/",
                path="/Example-2.mkv",
                name="Example-2.mkv",
                kind="file",
                file=VfsCatalogFileEntry(
                    item_id="item-2",
                    item_title="Example Two",
                    item_external_ref="tmdb:2",
                    media_entry_id="entry-2",
                    source_attachment_id="attachment-2",
                    media_type="movie",
                    transport="remote-direct",
                    locator="https://cdn.example.com/2",
                    lease_state="refreshing",
                    query_strategy="playback_snapshot",
                    provider_family="debrid",
                    locator_source="locator",
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=1, file_count=2, blocked_item_count=0),
    )
    client = _build_client(
        FakeMediaService(),
        vfs_catalog_supplier=FakeVfsCatalogSupplier(
            snapshot=snapshot_12,
            snapshots_by_generation={11: snapshot_11, 12: snapshot_12},
        ),
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  vfsGenerationHistory(limit: 5) {
                    generationId
                    publishedAt
                    entryCount
                    fileCount
                    queryStrategyCounts { key count }
                    leaseStateCounts { key count }
                    deltaFromPreviousAvailable
                    deltaUpsertCount
                    deltaRemovalCount
                    deltaUpsertFileCount
                    deltaRemovalFileCount
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["vfsGenerationHistory"] == [
        {
            "generationId": "12",
            "publishedAt": "2026-04-16T10:05:00+00:00",
            "entryCount": 3,
            "fileCount": 2,
            "queryStrategyCounts": [
                {"key": "persisted_media_entries", "count": 1},
                {"key": "playback_snapshot", "count": 1},
            ],
            "leaseStateCounts": [
                {"key": "ready", "count": 1},
                {"key": "refreshing", "count": 1},
            ],
            "deltaFromPreviousAvailable": True,
            "deltaUpsertCount": 1,
            "deltaRemovalCount": 0,
            "deltaUpsertFileCount": 1,
            "deltaRemovalFileCount": 0,
        },
        {
            "generationId": "11",
            "publishedAt": "2026-04-16T10:00:00+00:00",
            "entryCount": 2,
            "fileCount": 1,
            "queryStrategyCounts": [
                {"key": "persisted_media_entries", "count": 1},
            ],
            "leaseStateCounts": [
                {"key": "ready", "count": 1},
            ],
            "deltaFromPreviousAvailable": False,
            "deltaUpsertCount": 0,
            "deltaRemovalCount": 0,
            "deltaUpsertFileCount": 0,
            "deltaRemovalFileCount": 0,
        },
    ]


def test_graphql_enterprise_rollout_supporting_queries_return_typed_counts_inventory_actions_and_gaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "filmu_py.services.governance_posture.runtime_governance._playback_gate_governance_snapshot",
        lambda: {
            "playback_gate_environment_class": "canary",
            "playback_gate_gate_mode": "enforced",
            "playback_gate_runner_status": "ready",
            "playback_gate_runner_ready": 1,
            "playback_gate_runner_required_failures": 0,
            "playback_gate_provider_gate_required": 1,
            "playback_gate_provider_gate_ran": 1,
            "playback_gate_provider_parity_ready": 1,
            "playback_gate_windows_provider_ready": 1,
            "playback_gate_windows_provider_movie_ready": 1,
            "playback_gate_windows_provider_tv_ready": 1,
            "playback_gate_windows_provider_coverage": ["movie", "tv"],
            "playback_gate_windows_soak_ready": 1,
            "playback_gate_windows_soak_repeat_count": 4,
            "playback_gate_windows_soak_profile_coverage_complete": 1,
            "playback_gate_windows_soak_profile_coverage": [
                "continuous",
                "seek",
                "concurrent",
                "full",
            ],
            "playback_gate_policy_validation_status": "ready",
            "playback_gate_policy_ready": 1,
            "playback_gate_rollout_readiness": "ready",
            "playback_gate_rollout_reasons": ["playback_gate_green"],
            "playback_gate_rollout_next_action": "keep_required_checks_enforced",
        },
    )
    monkeypatch.setattr(
        "filmu_py.services.governance_posture.runtime_governance._vfs_runtime_governance_snapshot",
        lambda playback_gate_governance=None: {
            "vfs_runtime_snapshot_available": 1,
            "vfs_runtime_open_handles": 8,
            "vfs_runtime_active_reads": 3,
            "vfs_runtime_cache_pressure_class": "healthy",
            "vfs_runtime_refresh_pressure_class": "healthy",
            "vfs_runtime_provider_pressure_incidents": 0,
            "vfs_runtime_fairness_pressure_incidents": 0,
            "vfs_runtime_rollout_readiness": "ready",
            "vfs_runtime_rollout_next_action": "promote_to_next_environment_class",
            "vfs_runtime_rollout_canary_decision": "promote_to_next_environment_class",
            "vfs_runtime_rollout_merge_gate": "ready",
            "vfs_runtime_rollout_environment_class": "canary",
            "vfs_runtime_rollout_reasons": ["no_blocking_runtime_signals"],
        },
    )

    client = _build_client(
        FakeMediaService(),
        settings_overrides={
            "FILMU_PY_OIDC": {
                "enabled": True,
                "rollout_stage": "enforced",
                "rollout_evidence_refs": ["ops/identity/oidc-rollout.md"],
            },
            "FILMU_PY_PLUGIN_RUNTIME": {
                "proof_refs": ["ops/plugins/runtime-rollout.md"],
            },
            "FILMU_PY_OBSERVABILITY": {
                "proof_refs": ["ops/observability/rollout.md"],
            },
            "FILMU_PY_CONTROL_PLANE": {
                "event_backplane": "redis_stream",
                "proof_refs": ["ops/control-plane/replay-soak.md"],
            },
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  enterpriseRolloutStatusCounts {
                    status
                    count
                  }
                  enterpriseRolloutArtifactInventory(checkKey: "observability_rollout") {
                    checkKey
                    ref
                    category
                    recorded
                  }
                  enterpriseRolloutActions(domain: "playback_gate") {
                    domain
                    subject
                    action
                  }
                  enterpriseRolloutGaps(domain: "vfs_runtime_rollout") {
                    domain
                    subject
                    message
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert {"status": "ready", "count": 7} in payload["enterpriseRolloutStatusCounts"]
    assert payload["enterpriseRolloutArtifactInventory"] == [
        {
            "checkKey": "observability_rollout",
            "ref": "ops/observability/rollout.md",
            "category": "observability_rollout",
            "recorded": True,
        }
    ]
    assert payload["enterpriseRolloutActions"] == [
        {
            "domain": "playback_gate",
            "subject": "playback_gate_governance",
            "action": "keep_required_checks_enforced",
        }
    ]
    assert payload["enterpriseRolloutGaps"] == [
        {
            "domain": "vfs_runtime_rollout",
            "subject": "vfs_runtime_rollout",
            "message": "vfs rollout reason: no_blocking_runtime_signals",
        }
    ]


def test_graphql_downloader_supporting_summaries_return_typed_grouped_evidence() -> None:
    redis = FakeOperatorRedis(
        lists={
            "arq:queue-status-history:filmu-py": [
                json.dumps(
                    {
                        "observed_at": "2026-04-16T11:10:00Z",
                        "total_jobs": 6,
                        "ready_jobs": 2,
                        "deferred_jobs": 1,
                        "in_progress_jobs": 1,
                        "retry_jobs": 2,
                        "dead_letter_jobs": 2,
                        "alert_level": "critical",
                    }
                ),
                json.dumps(
                    {
                        "observed_at": "2026-04-16T11:05:00Z",
                        "total_jobs": 3,
                        "ready_jobs": 1,
                        "deferred_jobs": 0,
                        "in_progress_jobs": 1,
                        "retry_jobs": 0,
                        "dead_letter_jobs": 0,
                        "alert_level": "ok",
                    }
                ),
            ],
            "arq:dead-letter:filmu-py": [
                json.dumps(
                    {
                        "stage": "debrid_item",
                        "task": "debrid_item",
                        "item_id": "item-2",
                        "reason": "provider timeout",
                        "reason_code": "provider_timeout",
                        "idempotency_key": "item-2:timeout",
                        "attempt": 2,
                        "queued_at": "2026-04-16T11:12:00Z",
                        "metadata": {
                            "provider": "alldebrid",
                            "failure_kind": "timeout",
                            "selected_stream_id": "stream-2",
                            "item_request_id": "request-2",
                        },
                    }
                ),
                json.dumps(
                    {
                        "stage": "debrid_item",
                        "task": "debrid_item",
                        "item_id": "item-1",
                        "reason": "provider rate limited",
                        "reason_code": "provider_rate_limit",
                        "idempotency_key": "item-1:ratelimit",
                        "attempt": 1,
                        "queued_at": "2026-04-16T11:15:00Z",
                        "metadata": {
                            "provider": "realdebrid",
                            "failure_kind": "rate_limit",
                            "selected_stream_id": "stream-1",
                            "item_request_id": "request-1",
                            "status_code": 429,
                            "retry_after_seconds": 30,
                        },
                    }
                ),
            ],
        }
    )
    client = _build_client(FakeMediaService(), redis=redis)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  downloaderExecutionTrendSummary {
                    pointCount
                    okPointCount
                    criticalPointCount
                    averageReadyJobs
                    averageRetryJobs
                    averageDeadLetterJobs
                    latestAlertLevel
                  }
                  downloaderProviderSummaries(provider: "realdebrid") {
                    provider
                    sampleCount
                    reasonCodeCounts { key count }
                    statusCodeCounts { key count }
                    retryAfterHintCount
                  }
                  downloaderReasonSummaries(reasonCode: "provider_rate_limit") {
                    reasonCode
                    sampleCount
                    providerCounts { key count }
                    failureKindCounts { key count }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["downloaderExecutionTrendSummary"] == {
        "pointCount": 2,
        "okPointCount": 1,
        "criticalPointCount": 1,
        "averageReadyJobs": 1.5,
        "averageRetryJobs": 1.0,
        "averageDeadLetterJobs": 1.0,
        "latestAlertLevel": "critical",
    }
    assert payload["downloaderProviderSummaries"] == [
        {
            "provider": "realdebrid",
            "sampleCount": 1,
            "reasonCodeCounts": [{"key": "provider_rate_limit", "count": 1}],
            "statusCodeCounts": [{"key": "429", "count": 1}],
            "retryAfterHintCount": 1,
        }
    ]
    assert payload["downloaderReasonSummaries"] == [
        {
            "reasonCode": "provider_rate_limit",
            "sampleCount": 1,
            "providerCounts": [{"key": "realdebrid", "count": 1}],
            "failureKindCounts": [{"key": "rate_limit", "count": 1}],
        }
    ]


def test_graphql_downloader_support_queries_return_typed_timeline_failure_and_status_summaries() -> None:
    redis = FakeOperatorRedis(
        lists={
            "arq:queue-status-history:filmu-py": [
                json.dumps(
                    {
                        "observed_at": "2026-04-16T11:10:00Z",
                        "total_jobs": 6,
                        "ready_jobs": 2,
                        "deferred_jobs": 1,
                        "in_progress_jobs": 1,
                        "retry_jobs": 2,
                        "dead_letter_jobs": 2,
                        "alert_level": "critical",
                    }
                ),
                json.dumps(
                    {
                        "observed_at": "2026-04-16T11:05:00Z",
                        "total_jobs": 3,
                        "ready_jobs": 1,
                        "deferred_jobs": 0,
                        "in_progress_jobs": 1,
                        "retry_jobs": 0,
                        "dead_letter_jobs": 0,
                        "alert_level": "ok",
                    }
                ),
            ],
            "arq:dead-letter:filmu-py": [
                json.dumps(
                    {
                        "stage": "debrid_item",
                        "task": "debrid_item",
                        "item_id": "item-2",
                        "reason": "provider timeout",
                        "reason_code": "provider_timeout",
                        "idempotency_key": "item-2:timeout",
                        "attempt": 2,
                        "queued_at": "2026-04-16T11:12:00Z",
                        "metadata": {
                            "provider": "alldebrid",
                            "failure_kind": "timeout",
                        },
                    }
                ),
                json.dumps(
                    {
                        "stage": "debrid_item",
                        "task": "debrid_item",
                        "item_id": "item-1",
                        "reason": "provider rate limited",
                        "reason_code": "provider_rate_limit",
                        "idempotency_key": "item-1:ratelimit",
                        "attempt": 1,
                        "queued_at": "2026-04-16T11:15:00Z",
                        "metadata": {
                            "provider": "realdebrid",
                            "failure_kind": "rate_limit",
                            "status_code": 429,
                            "retry_after_seconds": 30,
                        },
                    }
                ),
            ],
        }
    )
    client = _build_client(FakeMediaService(), redis=redis)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  downloaderAlertLevelCounts(limit: 5) {
                    key
                    count
                  }
                  downloaderDeadLetterTimeline(limit: 10, bucketMinutes: 30) {
                    bucketAt
                    sampleCount
                    providerCounts { key count }
                    reasonCodeCounts { key count }
                    failureKindCounts { key count }
                  }
                  downloaderFailureKindSummaries(limit: 10) {
                    failureKind
                    sampleCount
                    providerCounts { key count }
                    reasonCodeCounts { key count }
                  }
                  downloaderStatusCodeSummaries(limit: 10) {
                    statusCode
                    sampleCount
                    providerCounts { key count }
                    reasonCodeCounts { key count }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["downloaderAlertLevelCounts"] == [
        {"key": "critical", "count": 1},
        {"key": "ok", "count": 1},
    ]
    assert payload["downloaderDeadLetterTimeline"] == [
        {
            "bucketAt": "2026-04-16T11:00:00+00:00",
            "sampleCount": 2,
            "providerCounts": [
                {"key": "alldebrid", "count": 1},
                {"key": "realdebrid", "count": 1},
            ],
            "reasonCodeCounts": [
                {"key": "provider_rate_limit", "count": 1},
                {"key": "provider_timeout", "count": 1},
            ],
            "failureKindCounts": [
                {"key": "rate_limit", "count": 1},
                {"key": "timeout", "count": 1},
            ],
        }
    ]
    assert payload["downloaderFailureKindSummaries"] == [
        {
            "failureKind": "rate_limit",
            "sampleCount": 1,
            "providerCounts": [{"key": "realdebrid", "count": 1}],
            "reasonCodeCounts": [{"key": "provider_rate_limit", "count": 1}],
        },
        {
            "failureKind": "timeout",
            "sampleCount": 1,
            "providerCounts": [{"key": "alldebrid", "count": 1}],
            "reasonCodeCounts": [{"key": "provider_timeout", "count": 1}],
        },
    ]
    assert payload["downloaderStatusCodeSummaries"] == [
        {
            "statusCode": 429,
            "sampleCount": 1,
            "providerCounts": [{"key": "realdebrid", "count": 1}],
            "reasonCodeCounts": [{"key": "provider_rate_limit", "count": 1}],
        }
    ]


def test_graphql_plugin_runtime_supporting_queries_return_rows_summaries_actions_and_gaps() -> None:
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
            "listrr": {
                "enabled": True,
                "url": "https://listrr.example",
                "list_ids": [],
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
            "FILMU_PY_CONTENT": plugin_settings["content"],
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  pluginRuntimeRows(capabilityKind: "content_service") {
                    name
                    status
                    ready
                    capabilityKinds
                    proofGapCount
                    warningCount
                  }
                  pluginRuntimeCapabilitySummaries {
                    capabilityKind
                    totalPlugins
                    readyPlugins
                    blockedPlugins
                  }
                  pluginProofCoverageSummaries(capabilityKind: "scraper") {
                    capabilityKind
                    totalPlugins
                    contractValidatedPlugins
                    soakValidatedPlugins
                    missingContractPlugins
                    missingSoakPlugins
                  }
                  pluginRuntimeActions(pluginName: "listrr") {
                    subject
                    capabilityKind
                    action
                  }
                  pluginRuntimeGaps(pluginName: "listrr") {
                    subject
                    capabilityKind
                    message
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    rows_by_name = {row["name"]: row for row in payload["pluginRuntimeRows"]}
    assert rows_by_name["listrr"] == {
        "name": "listrr",
        "status": "partial",
        "ready": False,
        "capabilityKinds": ["content_service"],
        "proofGapCount": 2,
        "warningCount": 3,
    }
    capability_rows = {row["capabilityKind"]: row for row in payload["pluginRuntimeCapabilitySummaries"]}
    assert capability_rows["content_service"]["totalPlugins"] >= 1
    assert capability_rows["scraper"]["readyPlugins"] >= 1
    assert payload["pluginProofCoverageSummaries"][0]["capabilityKind"] == "scraper"
    assert payload["pluginProofCoverageSummaries"][0]["contractValidatedPlugins"] >= 1
    assert payload["pluginProofCoverageSummaries"][0]["soakValidatedPlugins"] >= 1
    assert any(row["subject"] == "listrr" for row in payload["pluginRuntimeActions"])
    assert any(row["subject"] == "listrr" for row in payload["pluginRuntimeGaps"])


def test_graphql_plugin_runtime_support_queries_return_status_wiring_publisher_and_capability_counts() -> None:
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
    plugin_registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "hook-plugin",
                "version": "1.0.0",
                "api_version": "1",
                "distribution": "filesystem",
                "entry_module": "plugin.py",
                "event_hook": "ExampleHook",
            }
        )
    )

    class ExampleHook:
        subscribed_events = frozenset({"item.completed", "item.state.changed"})

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

    client = _build_client(FakeMediaService(), plugin_registry=plugin_registry)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  pluginRuntimeStatusCounts {
                    status
                    count
                  }
                  pluginRuntimeWiringStatusCounts {
                    key
                    count
                  }
                  pluginRuntimePublisherSummaries {
                    publisher
                    pluginCount
                    readyPlugins
                    quarantinedPlugins
                    warningCount
                    capabilityCounts { key count }
                  }
                  pluginRuntimeCapabilityActionCounts {
                    key
                    count
                  }
                  pluginRuntimeCapabilityGapCounts {
                    key
                    count
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert sum(row["count"] for row in payload["pluginRuntimeStatusCounts"]) == 2
    assert payload["pluginRuntimeWiringStatusCounts"] == [
        {"key": "publisher_only", "count": 1},
        {"key": "subscriber_only", "count": 1},
    ]
    assert payload["pluginRuntimePublisherSummaries"] == [
        {
            "publisher": "community",
            "pluginCount": 1,
            "readyPlugins": 1,
            "quarantinedPlugins": 0,
            "warningCount": 0,
            "capabilityCounts": [{"key": "scraper", "count": 1}],
        },
        {
            "publisher": "unknown",
            "pluginCount": 1,
            "readyPlugins": 1,
            "quarantinedPlugins": 0,
            "warningCount": 0,
            "capabilityCounts": [{"key": "event_hook", "count": 1}],
        },
    ]
    assert payload["pluginRuntimeCapabilityActionCounts"] == [
        {"key": "event_hook", "count": 1},
        {"key": "scraper", "count": 1},
    ]
    assert payload["pluginRuntimeCapabilityGapCounts"] == []


def test_graphql_vfs_generation_history_summary_returns_aggregate_rollups() -> None:
    snapshot_11 = VfsCatalogSnapshot(
        generation_id="11",
        published_at=datetime(2026, 4, 16, 10, 0, tzinfo=UTC),
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
                entry_id="file:entry-1",
                parent_entry_id="dir:/",
                path="/Example.mkv",
                name="Example.mkv",
                kind="file",
                file=VfsCatalogFileEntry(
                    item_id="item-1",
                    item_title="Example",
                    item_external_ref="tmdb:1",
                    media_entry_id="entry-1",
                    source_attachment_id="attachment-1",
                    media_type="movie",
                    transport="remote-direct",
                    locator="https://cdn.example.com/1",
                    lease_state="ready",
                    query_strategy="persisted_media_entries",
                    provider_family="debrid",
                    locator_source="unrestricted_url",
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=1, file_count=1, blocked_item_count=0),
    )
    snapshot_12 = VfsCatalogSnapshot(
        generation_id="12",
        published_at=datetime(2026, 4, 16, 10, 5, tzinfo=UTC),
        entries=(
            snapshot_11.entries[0],
            snapshot_11.entries[1],
            VfsCatalogEntry(
                entry_id="file:entry-2",
                parent_entry_id="dir:/",
                path="/Example-2.mkv",
                name="Example-2.mkv",
                kind="file",
                file=VfsCatalogFileEntry(
                    item_id="item-2",
                    item_title="Example Two",
                    item_external_ref="tmdb:2",
                    media_entry_id="entry-2",
                    source_attachment_id="attachment-2",
                    media_type="movie",
                    transport="remote-direct",
                    locator="https://cdn.example.com/2",
                    lease_state="refreshing",
                    query_strategy="playback_snapshot",
                    provider_family="debrid",
                    locator_source="locator",
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=1, file_count=2, blocked_item_count=0),
    )
    client = _build_client(
        FakeMediaService(),
        vfs_catalog_supplier=FakeVfsCatalogSupplier(
            snapshot=snapshot_12,
            snapshots_by_generation={11: snapshot_11, 12: snapshot_12},
        ),
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  vfsGenerationHistorySummary(limit: 5) {
                    generationCount
                    newestGenerationId
                    oldestGenerationId
                    maxEntryCount
                    maxFileCount
                    blockedGenerationCount
                    totalDeltaUpsertCount
                    totalDeltaRemovalCount
                    providerFamilyCounts { key count }
                    leaseStateCounts { key count }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["vfsGenerationHistorySummary"] == {
        "generationCount": 2,
        "newestGenerationId": "12",
        "oldestGenerationId": "11",
        "maxEntryCount": 3,
        "maxFileCount": 2,
        "blockedGenerationCount": 0,
        "totalDeltaUpsertCount": 1,
        "totalDeltaRemovalCount": 0,
        "providerFamilyCounts": [{"key": "debrid", "count": 3}],
        "leaseStateCounts": [
            {"key": "ready", "count": 2},
            {"key": "refreshing", "count": 1},
        ],
    }


def test_graphql_vfs_support_queries_return_delta_history_blocked_reasons_and_mount_feeds() -> None:
    snapshot_11 = VfsCatalogSnapshot(
        generation_id="11",
        published_at=datetime(2026, 4, 16, 10, 0, tzinfo=UTC),
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
                entry_id="file:entry-1",
                parent_entry_id="dir:/",
                path="/Example.mkv",
                name="Example.mkv",
                kind="file",
                file=VfsCatalogFileEntry(
                    item_id="item-1",
                    item_title="Example",
                    item_external_ref="tmdb:1",
                    media_entry_id="entry-1",
                    source_attachment_id="attachment-1",
                    media_type="movie",
                    transport="remote-direct",
                    locator="https://cdn.example.com/1",
                    lease_state="ready",
                    query_strategy="persisted_media_entries",
                    provider_family="debrid",
                    locator_source="unrestricted_url",
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=1, file_count=1, blocked_item_count=1),
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
        ),
    )
    snapshot_12 = VfsCatalogSnapshot(
        generation_id="12",
        published_at=datetime(2026, 4, 16, 10, 5, tzinfo=UTC),
        entries=(
            snapshot_11.entries[0],
            snapshot_11.entries[1],
            VfsCatalogEntry(
                entry_id="file:entry-2",
                parent_entry_id="dir:/",
                path="/Example-2.mkv",
                name="Example-2.mkv",
                kind="file",
                file=VfsCatalogFileEntry(
                    item_id="item-2",
                    item_title="Example Two",
                    item_external_ref="tmdb:2",
                    media_entry_id="entry-2",
                    source_attachment_id="attachment-2",
                    media_type="movie",
                    transport="remote-direct",
                    locator="https://cdn.example.com/2",
                    lease_state="refreshing",
                    query_strategy="playback_snapshot",
                    provider_family="debrid",
                    locator_source="locator",
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=1, file_count=2, blocked_item_count=1),
        blocked_items=snapshot_11.blocked_items,
    )
    client = _build_client(
        FakeMediaService(),
        vfs_catalog_supplier=FakeVfsCatalogSupplier(
            snapshot=snapshot_12,
            snapshots_by_generation={11: snapshot_11, 12: snapshot_12},
        ),
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  vfsCatalogDeltaHistory(limit: 5) {
                    generationId
                    baseGenerationId
                    publishedAt
                    upsertDirectoryCount
                    upsertFileCount
                    removalDirectoryCount
                    removalFileCount
                    providerFamilyCounts { key count }
                    leaseStateCounts { key count }
                  }
                  vfsCatalogDeltaHistorySummary(limit: 5) {
                    deltaCount
                    maxUpsertCount
                    maxRemovalCount
                    totalUpsertCount
                    totalRemovalCount
                    totalUpsertFileCount
                    totalRemovalFileCount
                    providerFamilyCounts { key count }
                    leaseStateCounts { key count }
                  }
                  vfsBlockedReasonSummaries(generationId: "12") {
                    key
                    count
                  }
                  vfsMountActions {
                    domain
                    subject
                    action
                  }
                  vfsMountGaps {
                    domain
                    subject
                    message
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["vfsCatalogDeltaHistory"] == [
        {
            "generationId": "12",
            "baseGenerationId": "11",
            "publishedAt": "2026-04-16T10:05:00+00:00",
            "upsertDirectoryCount": 0,
            "upsertFileCount": 1,
            "removalDirectoryCount": 0,
            "removalFileCount": 0,
            "providerFamilyCounts": [{"key": "debrid", "count": 1}],
            "leaseStateCounts": [{"key": "refreshing", "count": 1}],
        }
    ]
    assert payload["vfsCatalogDeltaHistorySummary"] == {
        "deltaCount": 1,
        "maxUpsertCount": 1,
        "maxRemovalCount": 0,
        "totalUpsertCount": 1,
        "totalRemovalCount": 0,
        "totalUpsertFileCount": 1,
        "totalRemovalFileCount": 0,
        "providerFamilyCounts": [{"key": "debrid", "count": 1}],
        "leaseStateCounts": [{"key": "refreshing", "count": 1}],
    }
    assert payload["vfsBlockedReasonSummaries"] == [
        {"key": "missing_lifecycle", "count": 1}
    ]
    assert any(row["domain"] == "vfs_mount" for row in payload["vfsMountActions"])
    assert any(row["domain"] == "vfs_mount" for row in payload["vfsMountGaps"])

def test_graphql_access_policy_revisions_query_and_mutations_follow_route_parity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)
    access_policy_service = DummyAccessPolicyService(resources.settings)
    resources.access_policy_service = access_policy_service
    resources.access_policy_snapshot = access_policy_service.snapshot

    audit_calls: list[dict[str, Any]] = []

    def fake_audit_action(request: Any, **kwargs: Any) -> None:
        _ = request
        audit_calls.append(dict(kwargs))

    monkeypatch.setattr(default_routes, "audit_action", fake_audit_action)

    initial_response = client.post(
        "/graphql",
        headers=_graphql_headers("settings:write"),
        json={
            "query": """
                query AccessPolicyRevisions($limit: Int!) {
                  accessPolicyRevisions(limit: $limit) {
                    activeVersion
                    revisions {
                      version
                      approvalStatus
                      isActive
                    }
                  }
                }
            """,
            "variables": {"limit": 5},
        },
    )

    assert initial_response.status_code == 200
    initial_body = initial_response.json()
    assert "errors" not in initial_body
    assert initial_body["data"]["accessPolicyRevisions"] == {
        "activeVersion": resources.settings.access_policy.version,
        "revisions": [
            {
                "version": resources.settings.access_policy.version,
                "approvalStatus": "approved",
                "isActive": True,
            }
        ],
    }

    approved_version = "2026-04-17-graphql-approval"
    approved_input = {
        "version": approved_version,
        "source": "graphql_test",
        "approvalNotes": "drafted in GraphQL",
        "roleGrants": {"platform:admin": ["settings:write", "security:policy.approve"]},
        "principalRoles": {"tenant-main:operator-1": ["platform:admin"]},
        "principalScopes": {"tenant-main:operator-1": ["settings:write"]},
        "principalTenantGrants": {"tenant-main:operator-1": ["tenant-main"]},
        "permissionConstraints": {"settings:write": {"tenant_ids": ["tenant-main"]}},
        "auditDecisions": True,
        "alertingEnabled": True,
        "repeatedDenialWarningThreshold": 4,
        "repeatedDenialCriticalThreshold": 7,
    }
    write_response = client.post(
        "/graphql",
        headers=_graphql_headers("settings:write"),
        json={
            "query": """
                mutation WriteAccessPolicyRevision($input: AccessPolicyRevisionWriteInput!) {
                  writeAccessPolicyRevision(input: $input) {
                    version
                    source
                    approvalStatus
                    proposedBy
                    approvalNotes
                    isActive
                    roleGrants
                    principalScopes
                    repeatedDenialWarningThreshold
                    repeatedDenialCriticalThreshold
                  }
                }
            """,
            "variables": {"input": approved_input},
        },
    )

    assert write_response.status_code == 200
    write_body = write_response.json()
    assert "errors" not in write_body
    assert write_body["data"]["writeAccessPolicyRevision"] == {
        "version": approved_version,
        "source": "graphql_test",
        "approvalStatus": "draft",
        "proposedBy": "tenant-main:operator-1",
        "approvalNotes": "drafted in GraphQL",
        "isActive": False,
        "roleGrants": {"platform:admin": ["settings:write", "security:policy.approve"]},
        "principalScopes": {"tenant-main:operator-1": ["settings:write"]},
        "repeatedDenialWarningThreshold": 4,
        "repeatedDenialCriticalThreshold": 7,
    }

    approve_response = client.post(
        "/graphql",
        headers=_graphql_headers("security:policy.approve"),
        json={
            "query": """
                mutation ApproveAccessPolicyRevision(
                  $version: String!
                  $input: AccessPolicyRevisionApprovalInput
                ) {
                  approveAccessPolicyRevision(version: $version, input: $input) {
                    version
                    approvalStatus
                    approvedBy
                    approvalNotes
                    isActive
                  }
                }
            """,
            "variables": {
                "version": approved_version,
                "input": {"approvalNotes": "approved in GraphQL", "activate": False},
            },
        },
    )

    assert approve_response.status_code == 200
    approve_body = approve_response.json()
    assert "errors" not in approve_body
    assert approve_body["data"]["approveAccessPolicyRevision"] == {
        "version": approved_version,
        "approvalStatus": "approved",
        "approvedBy": "tenant-main:operator-1",
        "approvalNotes": "approved in GraphQL",
        "isActive": False,
    }

    activate_response = client.post(
        "/graphql",
        headers=_graphql_headers("settings:write"),
        json={
            "query": """
                mutation ActivateAccessPolicyRevision($version: String!) {
                  activateAccessPolicyRevision(version: $version) {
                    version
                    approvalStatus
                    isActive
                  }
                }
            """,
            "variables": {"version": approved_version},
        },
    )

    assert activate_response.status_code == 200
    activate_body = activate_response.json()
    assert "errors" not in activate_body
    assert activate_body["data"]["activateAccessPolicyRevision"] == {
        "version": approved_version,
        "approvalStatus": "approved",
        "isActive": True,
    }

    rejected_version = "2026-04-17-graphql-reject"
    reject_input = {
        "version": rejected_version,
        "source": "graphql_test",
        "approvalNotes": "reject me in GraphQL",
        "roleGrants": {"playback:operator": ["playback:read"]},
        "principalRoles": {"tenant-main:operator-1": ["playback:operator"]},
        "principalScopes": {"tenant-main:operator-1": ["playback:read"]},
        "principalTenantGrants": {"tenant-main:operator-1": ["tenant-main"]},
        "permissionConstraints": {},
        "auditDecisions": False,
        "alertingEnabled": True,
        "repeatedDenialWarningThreshold": 3,
        "repeatedDenialCriticalThreshold": 5,
    }
    reject_write_response = client.post(
        "/graphql",
        headers=_graphql_headers("settings:write"),
        json={
            "query": """
                mutation WriteAccessPolicyRevision($input: AccessPolicyRevisionWriteInput!) {
                  writeAccessPolicyRevision(input: $input) {
                    version
                    approvalStatus
                  }
                }
            """,
            "variables": {"input": reject_input},
        },
    )

    assert reject_write_response.status_code == 200
    reject_write_body = reject_write_response.json()
    assert "errors" not in reject_write_body
    assert reject_write_body["data"]["writeAccessPolicyRevision"] == {
        "version": rejected_version,
        "approvalStatus": "draft",
    }

    reject_response = client.post(
        "/graphql",
        headers=_graphql_headers("security:policy.approve"),
        json={
            "query": """
                mutation RejectAccessPolicyRevision(
                  $version: String!
                  $input: AccessPolicyRevisionApprovalInput
                ) {
                  rejectAccessPolicyRevision(version: $version, input: $input) {
                    version
                    approvalStatus
                    approvedBy
                    approvalNotes
                    isActive
                  }
                }
            """,
            "variables": {
                "version": rejected_version,
                "input": {"approvalNotes": "rejected in GraphQL"},
            },
        },
    )

    assert reject_response.status_code == 200
    reject_body = reject_response.json()
    assert "errors" not in reject_body
    assert reject_body["data"]["rejectAccessPolicyRevision"] == {
        "version": rejected_version,
        "approvalStatus": "rejected",
        "approvedBy": "tenant-main:operator-1",
        "approvalNotes": "rejected in GraphQL",
        "isActive": False,
    }

    final_response = client.post(
        "/graphql",
        headers=_graphql_headers("settings:write"),
        json={
            "query": """
                query {
                  accessPolicyRevisions(limit: 10) {
                    activeVersion
                    revisions {
                      version
                      approvalStatus
                      isActive
                      approvedBy
                    }
                  }
                }
            """
        },
    )

    assert final_response.status_code == 200
    final_body = final_response.json()
    assert "errors" not in final_body
    assert final_body["data"]["accessPolicyRevisions"]["activeVersion"] == approved_version
    final_revisions = {
        row["version"]: row for row in final_body["data"]["accessPolicyRevisions"]["revisions"]
    }
    assert final_revisions[approved_version] == {
        "version": approved_version,
        "approvalStatus": "approved",
        "isActive": True,
        "approvedBy": "tenant-main:operator-1",
    }
    assert final_revisions[rejected_version] == {
        "version": rejected_version,
        "approvalStatus": "rejected",
        "isActive": False,
        "approvedBy": "tenant-main:operator-1",
    }

    assert audit_calls == [
        {
            "action": "security.access_policy.write_revision",
            "target": f"access_policy.{approved_version}",
            "details": {
                "activate": False,
                "source": "graphql_test",
                "approval_status": "draft",
            },
        },
        {
            "action": "security.access_policy.approve_revision",
            "target": f"access_policy.{approved_version}",
            "details": {"activate": False},
        },
        {
            "action": "security.access_policy.activate_revision",
            "target": f"access_policy.{approved_version}",
        },
        {
            "action": "security.access_policy.write_revision",
            "target": f"access_policy.{rejected_version}",
            "details": {
                "activate": False,
                "source": "graphql_test",
                "approval_status": "draft",
            },
        },
        {
            "action": "security.access_policy.reject_revision",
            "target": f"access_policy.{rejected_version}",
        },
    ]


def test_graphql_write_access_policy_revision_requires_settings_write() -> None:
    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)
    access_policy_service = DummyAccessPolicyService(resources.settings)
    resources.access_policy_service = access_policy_service
    resources.access_policy_snapshot = access_policy_service.snapshot

    response = client.post(
        "/graphql",
        headers=_graphql_headers("playback:read", roles="playback:operator"),
        json={
            "query": """
                mutation WriteAccessPolicyRevision($input: AccessPolicyRevisionWriteInput!) {
                  writeAccessPolicyRevision(input: $input) {
                    version
                  }
                }
            """,
            "variables": {
                "input": {
                    "version": "2026-04-17-graphql-authz",
                    "roleGrants": {},
                    "principalRoles": {},
                    "principalScopes": {},
                    "principalTenantGrants": {},
                    "permissionConstraints": {},
                }
            },
        },
    )

    assert response.status_code == 200
    assert "Authorization denied (missing_permissions)" in response.json()["errors"][0]["message"]


def test_graphql_plugin_governance_overrides_query_and_mutation_follow_route_parity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)
    resources.plugin_governance_service = DummyPluginGovernanceService()

    audit_calls: list[dict[str, Any]] = []

    def fake_audit_action(request: Any, **kwargs: Any) -> None:
        _ = request
        audit_calls.append(dict(kwargs))

    monkeypatch.setattr(default_routes, "audit_action", fake_audit_action)

    initial_response = client.post(
        "/graphql",
        headers=_graphql_headers("settings:write"),
        json={
            "query": """
                query {
                  pluginGovernanceOverrides {
                    pluginName
                    state
                  }
                }
            """
        },
    )

    assert initial_response.status_code == 200
    initial_body = initial_response.json()
    assert "errors" not in initial_body
    assert initial_body["data"]["pluginGovernanceOverrides"] == []

    mutation_response = client.post(
        "/graphql",
        headers=_graphql_headers("settings:write"),
        json={
            "query": """
                mutation WritePluginGovernanceOverride(
                  $pluginName: String!
                  $input: PluginGovernanceOverrideWriteInput!
                ) {
                  writePluginGovernanceOverride(pluginName: $pluginName, input: $input) {
                    pluginName
                    state
                    reason
                    notes
                    updatedBy
                    createdAt
                    updatedAt
                  }
                }
            """,
            "variables": {
                "pluginName": "torrentio",
                "input": {
                    "state": "quarantined",
                    "reason": "signature drift",
                    "notes": "hold until republished",
                },
            },
        },
    )

    assert mutation_response.status_code == 200
    mutation_body = mutation_response.json()
    assert "errors" not in mutation_body
    assert mutation_body["data"]["writePluginGovernanceOverride"] == {
        "pluginName": "torrentio",
        "state": "quarantined",
        "reason": "signature drift",
        "notes": "hold until republished",
        "updatedBy": "tenant-main:operator-1",
        "createdAt": "2026-04-11T12:50:00+00:00",
        "updatedAt": "2026-04-11T12:50:00+00:00",
    }

    final_response = client.post(
        "/graphql",
        headers=_graphql_headers("settings:write"),
        json={
            "query": """
                query {
                  pluginGovernanceOverrides {
                    pluginName
                    state
                    reason
                    notes
                    updatedBy
                  }
                }
            """
        },
    )

    assert final_response.status_code == 200
    final_body = final_response.json()
    assert "errors" not in final_body
    assert final_body["data"]["pluginGovernanceOverrides"] == [
        {
            "pluginName": "torrentio",
            "state": "quarantined",
            "reason": "signature drift",
            "notes": "hold until republished",
            "updatedBy": "tenant-main:operator-1",
        }
    ]
    assert audit_calls == [
        {
            "action": "security.plugin_governance.write_override",
            "target": "plugin.torrentio",
            "details": {"state": "quarantined"},
        }
    ]


def test_graphql_write_plugin_governance_override_requires_settings_write() -> None:
    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)
    resources.plugin_governance_service = DummyPluginGovernanceService()

    response = client.post(
        "/graphql",
        headers=_graphql_headers("playback:read", roles="playback:operator"),
        json={
            "query": """
                mutation WritePluginGovernanceOverride(
                  $pluginName: String!
                  $input: PluginGovernanceOverrideWriteInput!
                ) {
                  writePluginGovernanceOverride(pluginName: $pluginName, input: $input) {
                    pluginName
                  }
                }
            """,
            "variables": {
                "pluginName": "torrentio",
                "input": {"state": "revoked", "reason": "authz failure"},
            },
        },
    )

    assert response.status_code == 200
    assert "Authorization denied (missing_permissions)" in response.json()["errors"][0]["message"]

def test_graphql_vfs_rollout_control_query_and_mutation_return_persisted_state_history_and_audit(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts_root = tmp_path / "playback-proof-artifacts"
    (artifacts_root / "windows-native-stack").mkdir(parents=True)
    monkeypatch.setenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT", str(artifacts_root))
    monkeypatch.setattr(
        default_routes,
        "playback_gate_governance_snapshot",
        lambda: {"playback_gate_environment_class": "windows-native:managed"},
    )

    def fake_vfs_runtime_governance_snapshot(
        playback_gate_governance: dict[str, Any] | None = None,
        *,
        request_tenant_id: str | None = None,
        authorized_tenant_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        _ = (playback_gate_governance, request_tenant_id, authorized_tenant_ids)
        state = default_routes.build_vfs_rollout_control_state(
            default_routes.managed_windows_vfs_state_snapshot()
        )
        pause_active = bool(state.promotion_pause_active)
        rollback_active = bool(state.rollback_active)
        if rollback_active:
            return {
                "vfs_runtime_rollout_readiness": "blocked",
                "vfs_runtime_rollout_next_action": "rollback_current_environment",
                "vfs_runtime_rollout_canary_decision": "rollback_current_environment",
                "vfs_runtime_rollout_merge_gate": "blocked",
                "vfs_runtime_rollout_environment_class": state.environment_class
                or "windows-native:managed",
                "vfs_runtime_rollout_reasons": ["operator_requested_rollback"],
            }
        return {
            "vfs_runtime_rollout_readiness": "ready",
            "vfs_runtime_rollout_next_action": (
                "hold_canary_and_repeat_soak"
                if pause_active
                else "promote_to_next_environment_class"
            ),
            "vfs_runtime_rollout_canary_decision": (
                "hold_canary_and_repeat_soak"
                if pause_active
                else "promote_to_next_environment_class"
            ),
            "vfs_runtime_rollout_merge_gate": "hold" if pause_active else "ready",
            "vfs_runtime_rollout_environment_class": state.environment_class
            or "windows-native:managed",
            "vfs_runtime_rollout_reasons": (
                ["operator_requested_promotion_pause"]
                if pause_active
                else ["no_blocking_runtime_signals"]
            ),
        }

    monkeypatch.setattr(
        default_routes,
        "vfs_runtime_governance_snapshot",
        fake_vfs_runtime_governance_snapshot,
    )
    audit_calls: list[dict[str, Any]] = []

    def fake_audit_action(request: Any, **kwargs: Any) -> None:
        _ = request
        audit_calls.append(dict(kwargs))

    monkeypatch.setattr("filmu_py.graphql.resolvers.audit_action", fake_audit_action)
    client = _build_client(FakeMediaService())
    expires_at = (
        datetime(2099, 4, 17, 22, 0, tzinfo=UTC).isoformat().replace("+00:00", "Z")
    )
    mutation_response = client.post(
        "/graphql",
        headers={
            "x-api-key": "a" * 32,
            "x-actor-id": "operator-1",
            "x-tenant-id": "tenant-main",
            "x-actor-roles": "platform:admin",
            "x-actor-scopes": "backend:admin",
        },
        json={
            "query": """
                mutation Persist($input: PersistVfsRolloutControlInput!) {
                  persistVfsRolloutControl(input: $input) {
                    environmentClass
                    promotionPaused
                    promotionPauseReason
                    promotionPauseExpiresAt
                    promotionPauseActive
                    rollbackRequested
                    updatedBy
                    mergeGate
                    canaryDecision
                    history {
                      actorId
                      summary
                      promotionPauseActive
                    }
                  }
                }
            """,
            "variables": {
                "input": {
                    "environmentClass": "windows-native:managed",
                    "promotionPaused": True,
                    "promotionPauseReason": "repeat soak after GraphQL review",
                    "promotionPauseExpiresAt": expires_at,
                    "notes": "hold after GraphQL check",
                }
            },
        },
    )

    assert mutation_response.status_code == 200
    mutation_payload = mutation_response.json()["data"]["persistVfsRolloutControl"]
    assert mutation_payload == {
        "environmentClass": "windows-native:managed",
        "promotionPaused": True,
        "promotionPauseReason": "repeat soak after GraphQL review",
        "promotionPauseExpiresAt": expires_at,
        "promotionPauseActive": True,
        "rollbackRequested": False,
        "updatedBy": "tenant-main:operator-1",
        "mergeGate": "hold",
        "canaryDecision": "hold_canary_and_repeat_soak",
        "history": [
            {
                "actorId": "tenant-main:operator-1",
                "summary": "promotion pause enabled; environment updated; notes updated (windows-native:managed)",
                "promotionPauseActive": True,
            }
        ],
    }
    assert audit_calls == [
        {
            "action": "operations.vfs_rollout.write_control",
            "target": "operations.vfs_rollout",
            "details": {
                "promotion_paused": True,
                "promotion_pause_reason": "repeat soak after GraphQL review",
                "rollback_requested": None,
                "rollback_reason": None,
                "environment_class": "windows-native:managed",
            },
        }
    ]

    query_response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  vfsRolloutControl(historyLimit: 1) {
                    environmentClass
                    promotionPaused
                    promotionPauseReason
                    promotionPauseActive
                    mergeGate
                    history {
                      summary
                    }
                  }
                }
            """
        },
    )

    assert query_response.status_code == 200
    assert query_response.json()["data"]["vfsRolloutControl"] == {
        "environmentClass": "windows-native:managed",
        "promotionPaused": True,
        "promotionPauseReason": "repeat soak after GraphQL review",
        "promotionPauseActive": True,
        "mergeGate": "hold",
        "history": [
            {
                "summary": "promotion pause enabled; environment updated; notes updated (windows-native:managed)"
            }
        ],
    }


def test_graphql_authorization_audit_failures_do_not_break_allowed_rollout_mutation(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts_root = tmp_path / "playback-proof-artifacts"
    (artifacts_root / "windows-native-stack").mkdir(parents=True)
    monkeypatch.setenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT", str(artifacts_root))
    monkeypatch.setattr(
        default_routes,
        "playback_gate_governance_snapshot",
        lambda: {"playback_gate_environment_class": "windows-native:managed"},
    )
    monkeypatch.setattr(
        default_routes,
        "vfs_runtime_governance_snapshot",
        lambda playback_gate_governance=None, **kwargs: {
            "vfs_runtime_rollout_readiness": "ready",
            "vfs_runtime_rollout_next_action": "promote_to_next_environment_class",
            "vfs_runtime_rollout_canary_decision": "promote_to_next_environment_class",
            "vfs_runtime_rollout_merge_gate": "ready",
            "vfs_runtime_rollout_environment_class": "windows-native:managed",
            "vfs_runtime_rollout_reasons": ["no_blocking_runtime_signals"],
        },
    )

    client = _build_client(FakeMediaService())
    cast(Any, client.app.state.resources).authorization_audit_service = (
        FailingAuthorizationAuditService()
    )

    response = client.post(
        "/graphql",
        headers={
            "x-api-key": "a" * 32,
            "x-actor-id": "operator-1",
            "x-tenant-id": "tenant-main",
            "x-actor-roles": "platform:admin",
            "x-actor-scopes": "backend:admin",
        },
        json={
            "query": """
                mutation Persist($input: PersistVfsRolloutControlInput!) {
                  persistVfsRolloutControl(input: $input) {
                    environmentClass
                    promotionPaused
                    promotionPauseReason
                    updatedBy
                  }
                }
            """,
            "variables": {
                "input": {
                    "environmentClass": "windows-native:managed",
                    "promotionPaused": True,
                    "promotionPauseReason": "repeat soak after audit outage",
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert "errors" not in body
    assert body["data"]["persistVfsRolloutControl"] == {
        "environmentClass": "windows-native:managed",
        "promotionPaused": True,
        "promotionPauseReason": "repeat soak after audit outage",
        "updatedBy": "tenant-main:operator-1",
    }


def test_graphql_persist_vfs_rollout_control_requires_backend_admin() -> None:
    client = _build_client(FakeMediaService())

    response = client.post(
        "/graphql",
        headers={
            "x-api-key": "a" * 32,
            "x-actor-id": "operator-1",
            "x-tenant-id": "tenant-main",
            "x-actor-roles": "playback:operator",
            "x-actor-scopes": "playback:read",
        },
        json={
            "query": """
                mutation Persist($input: PersistVfsRolloutControlInput!) {
                  persistVfsRolloutControl(input: $input) {
                    environmentClass
                  }
                }
            """,
            "variables": {
                "input": {
                    "promotionPaused": True,
                    "promotionPauseReason": "unauthorized hold",
                }
            },
        },
    )

    assert response.status_code == 200
    errors = response.json()["errors"]
    assert "Authorization denied (missing_permissions)" in errors[0]["message"]
