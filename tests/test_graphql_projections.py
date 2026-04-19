"""GraphQL projection query tests for the dual-surface API strategy."""

from __future__ import annotations

import asyncio
import fnmatch
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from arq.constants import in_progress_key_prefix, result_key_prefix, retry_key_prefix
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.api.models import (
    EventTypesResponse,
    ServingGovernanceResponse,
    ServingHandleResponse,
    ServingPathResponse,
    ServingSessionResponse,
    ServingStatusResponse,
)
from filmu_py.api.routes import default as default_routes
from filmu_py.api.routes import runtime_governance
from filmu_py.config import Settings
from filmu_py.core import byte_streaming
from filmu_py.core.cache import CACHE_HITS_TOTAL, CACHE_INVALIDATIONS_TOTAL, CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.plugin_hook_queue_status import PluginHookQueueStatusStore
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
from filmu_py.services import governance_posture
from filmu_py.services.access_policy import snapshot_from_settings
from filmu_py.services.media import (
    ActiveStreamDetailRecord,
    CalendarProjectionRecord,
    CalendarReleaseDataRecord,
    ConsumerPlaybackActivityItemRecord,
    ConsumerPlaybackActivityRecord,
    ConsumerPlaybackDeviceRecord,
    ConsumerPlaybackSessionRecord,
    MediaEntryDetailRecord,
    MediaItemRecord,
    MediaItemSpecializationRecord,
    MediaItemSummaryRecord,
    ParentIdsRecord,
    PlaybackAttachmentDetailRecord,
    RecoveryMechanism,
    RecoveryPlanRecord,
    RecoveryTargetStage,
    ResolvedPlaybackAttachmentRecord,
    ResolvedPlaybackSnapshotRecord,
    RequestCandidateSeasonRecord,
    RequestCandidateSeasonSummaryRecord,
    RequestSearchCandidateRecord,
    RequestSearchLifecycleRecord,
    RequestSearchPageRecord,
    StatsProjection,
    WorkflowCheckpointRecord,
    WorkflowCheckpointStatus,
    WorkflowDrillCandidateRecord,
    WorkflowResumeStage,
    RequestItemServiceResult,
    EnrichmentResult,
    ItemRequestSummaryRecord,
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


@dataclass
class DummyRedis:
    values: dict[str, bytes] = field(default_factory=dict)
    expirations: dict[str, int] = field(default_factory=dict)

    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def get(self, name: str) -> bytes | None:
        return self.values.get(name)

    async def set(self, name: str, value: bytes | str, ex: int | None = None) -> bool:
        self.values[name] = value.encode("utf-8") if isinstance(value, str) else value
        if ex is not None:
            self.expirations[name] = ex
        else:
            self.expirations.pop(name, None)
        return True

    async def delete(self, *names: str) -> int:
        removed = 0
        for name in names:
            if name in self.values:
                removed += 1
            self.values.pop(name, None)
            self.expirations.pop(name, None)
        return removed

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


class DummyDatabaseSession:
    async def __aenter__(self) -> DummyDatabaseSession:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    async def execute(self, statement: Any) -> None:
        _ = statement
        return None

    async def commit(self) -> None:
        return None


class DummyDatabaseRuntime:
    def session(self) -> DummyDatabaseSession:
        return DummyDatabaseSession()

    async def dispose(self) -> None:
        return None


def _counter_value(counter: Any, **labels: str) -> float:
    total = 0.0
    for metric in counter.collect():
        for sample in metric.samples:
            if not sample.name.endswith("_total"):
                continue
            if all(sample.labels.get(key) == value for key, value in labels.items()):
                total += float(sample.value)
    return total


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
    detail_by_item_id: dict[str, MediaItemSummaryRecord | Any | None] = field(default_factory=dict)
    stream_candidates: list[StreamORM] = field(default_factory=list)
    item_records: list[MediaItemRecord] = field(default_factory=list)
    recovery_plan: RecoveryPlanRecord | None = None
    workflow_checkpoint: WorkflowCheckpointRecord | None = None
    detail_page: object | None = None
    search_item_calls: list[dict[str, Any]] = field(default_factory=list)
    search_item_detail_calls: list[dict[str, Any]] = field(default_factory=list)
    request_search_results: list[Any] = field(default_factory=list)
    request_search_calls: list[dict[str, Any]] = field(default_factory=list)
    request_candidate_result: Any | None = None
    request_candidate_calls: list[dict[str, Any]] = field(default_factory=list)
    request_history_page_result: Any | None = None
    request_history_page_calls: list[dict[str, Any]] = field(default_factory=list)
    request_search_page_result: Any | None = None
    request_search_page_calls: list[dict[str, Any]] = field(default_factory=list)
    request_discovery_results: list[Any] = field(default_factory=list)
    request_discovery_calls: list[dict[str, Any]] = field(default_factory=list)
    request_editorial_family_results: list[Any] = field(default_factory=list)
    request_editorial_family_calls: list[dict[str, Any]] = field(default_factory=list)
    request_release_window_results: list[Any] = field(default_factory=list)
    request_release_window_calls: list[dict[str, Any]] = field(default_factory=list)
    request_projection_group_results: list[Any] = field(default_factory=list)
    request_projection_group_calls: list[dict[str, Any]] = field(default_factory=list)
    request_discovery_page_result: Any | None = None
    request_discovery_page_calls: list[dict[str, Any]] = field(default_factory=list)
    detail_calls: list[dict[str, Any]] = field(default_factory=list)
    consumer_playback_activity: ConsumerPlaybackActivityRecord = field(
        default_factory=lambda: ConsumerPlaybackActivityRecord(
            generated_at="2026-04-19T12:00:00+00:00"
        )
    )
    consumer_playback_activity_calls: list[dict[str, Any]] = field(default_factory=list)
    request_item_calls: list[dict[str, Any]] = field(default_factory=list)

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
        tenant_id: str | None = None,
    ) -> MediaItemSummaryRecord | None:
        self.detail_calls.append(
            {
                "item_identifier": item_identifier,
                "media_type": media_type,
                "extended": extended,
                "tenant_id": tenant_id,
            }
        )
        return cast(
            MediaItemSummaryRecord | None,
            self.detail_by_item_id.get(item_identifier, self.detail),
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
        focus_item_id: str | None = None,
    ) -> ConsumerPlaybackActivityRecord:
        self.consumer_playback_activity_calls.append(
            {
                "tenant_id": tenant_id,
                "actor_id": actor_id,
                "actor_type": actor_type,
                "item_limit": item_limit,
                "device_limit": device_limit,
                "history_limit": history_limit,
                "focus_item_id": focus_item_id,
            }
        )
        return self.consumer_playback_activity

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
        tenant_id: str | None = None,
        allowed_item_ids: set[str] | None = None,
    ) -> object:
        self.search_item_calls.append(
            {
                "limit": limit,
                "page": page,
                "item_types": item_types,
                "states": states,
                "sort": sort,
                "search": search,
                "extended": extended,
                "tenant_id": tenant_id,
                "allowed_item_ids": sorted(allowed_item_ids) if allowed_item_ids is not None else None,
            }
        )

        @dataclass
        class _Page:
            items: list[MediaItemSummaryRecord]
            total_items: int
            limit: int
            page: int
            total_pages: int

        if self.detail is not None:
            detail_records = [self.detail]
            if allowed_item_ids is not None:
                detail_records = [record for record in detail_records if record.id in allowed_item_ids]
            total_items = len(detail_records)
            total_pages = max(1, (total_items + max(limit, 1) - 1) // max(limit, 1))
            return _Page(
                items=detail_records,
                total_items=total_items,
                limit=limit,
                page=page,
                total_pages=total_pages,
            )

        source_records = list(self.item_records)
        if allowed_item_ids is not None:
            source_records = [record for record in source_records if record.id in allowed_item_ids]

        records = [
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
                for record in source_records[:limit]
            ]
        total_items = len(source_records) if source_records else len(records)
        total_pages = max(1, (total_items + max(limit, 1) - 1) // max(limit, 1))
        return _Page(
            items=records,
            total_items=total_items,
            limit=limit,
            page=page,
            total_pages=total_pages,
        )

    async def search_item_details(
        self,
        *,
        limit: int = 24,
        offset: int = 0,
        states: list[str] | None = None,
        query: str | None = None,
        provider: str | None = None,
        attachment_state: str | None = None,
        stream: str | None = None,
        has_errors: bool = False,
        sort: str | None = None,
        tenant_id: str | None = None,
    ) -> object:
        _ = (
            tenant_id,
        )
        self.search_item_detail_calls.append(
            {
                "limit": limit,
                "offset": offset,
                "states": states,
                "query": query,
                "provider": provider,
                "attachment_state": attachment_state,
                "stream": stream,
                "has_errors": has_errors,
                "sort": sort,
            }
        )

        if self.detail_page is not None:
            return self.detail_page

        @dataclass
        class _Page:
            items: list[MediaItemSummaryRecord]
            total_items: int
            limit: int

        items: list[MediaItemSummaryRecord] = []
        if self.detail is not None:
            items = [self.detail]
        return _Page(items=items, total_items=len(items), limit=limit)

    async def search_request_candidates(
        self,
        *,
        query: str,
        media_type: str | None = None,
        limit: int = 12,
        tenant_id: str | None = None,
    ) -> list[Any]:
        self.request_search_calls.append(
            {
                "query": query,
                "media_type": media_type,
                "limit": limit,
                "tenant_id": tenant_id,
            }
        )
        return list(self.request_search_results[:limit])

    async def get_request_candidate(
        self,
        *,
        external_ref: str,
        media_type: str,
        tenant_id: str | None = None,
    ) -> Any | None:
        self.request_candidate_calls.append(
            {
                "external_ref": external_ref,
                "media_type": media_type,
                "tenant_id": tenant_id,
            }
        )
        return self.request_candidate_result

    async def get_request_history_page(
        self,
        *,
        media_type: str | None = None,
        limit: int = 6,
        offset: int = 0,
        tenant_id: str | None = None,
    ) -> Any:
        self.request_history_page_calls.append(
            {
                "media_type": media_type,
                "limit": limit,
                "offset": offset,
                "tenant_id": tenant_id,
            }
        )
        if self.request_history_page_result is not None:
            return self.request_history_page_result
        return SimpleNamespace(
            items=[],
            offset=offset,
            limit=limit,
            total_count=0,
            has_previous_page=offset > 0,
            has_next_page=False,
            result_window_complete=True,
        )

    async def search_request_candidates_page(
        self,
        *,
        query: str,
        media_type: str | None = None,
        limit: int = 20,
        offset: int = 0,
        tenant_id: str | None = None,
    ) -> Any:
        self.request_search_page_calls.append(
            {
                "query": query,
                "media_type": media_type,
                "limit": limit,
                "offset": offset,
                "tenant_id": tenant_id,
            }
        )
        if self.request_search_page_result is not None:
            return self.request_search_page_result
        return SimpleNamespace(
            items=[],
            offset=offset,
            limit=limit,
            total_count=0,
            has_previous_page=offset > 0,
            has_next_page=False,
            result_window_complete=True,
        )

    async def discover_request_candidates(
        self,
        *,
        limit_per_rail: int = 8,
        rail_ids: list[str] | None = None,
        tenant_id: str | None = None,
    ) -> list[Any]:
        self.request_discovery_calls.append(
            {
                "limit_per_rail": limit_per_rail,
                "rail_ids": list(rail_ids) if rail_ids is not None else None,
                "tenant_id": tenant_id,
            }
        )
        return list(self.request_discovery_results)

    async def discover_request_editorial_families(
        self,
        *,
        limit_per_family: int = 8,
        family_ids: list[str] | None = None,
        tenant_id: str | None = None,
    ) -> list[Any]:
        self.request_editorial_family_calls.append(
            {
                "limit_per_family": limit_per_family,
                "family_ids": list(family_ids) if family_ids is not None else None,
                "tenant_id": tenant_id,
            }
        )
        return list(self.request_editorial_family_results)

    async def discover_request_release_windows(
        self,
        *,
        limit_per_window: int = 8,
        window_ids: list[str] | None = None,
        tenant_id: str | None = None,
    ) -> list[Any]:
        self.request_release_window_calls.append(
            {
                "limit_per_window": limit_per_window,
                "window_ids": list(window_ids) if window_ids is not None else None,
                "tenant_id": tenant_id,
            }
        )
        return list(self.request_release_window_results)

    async def discover_request_projection_groups(
        self,
        *,
        media_type: str | None = None,
        genre: str | None = None,
        release_year: int | None = None,
        original_language: str | None = None,
        company: str | None = None,
        network: str | None = None,
        sort: str | None = None,
        limit_per_group: int = 6,
        tenant_id: str | None = None,
    ) -> list[Any]:
        self.request_projection_group_calls.append(
            {
                "media_type": media_type,
                "genre": genre,
                "release_year": release_year,
                "original_language": original_language,
                "company": company,
                "network": network,
                "sort": sort,
                "limit_per_group": limit_per_group,
                "tenant_id": tenant_id,
            }
        )
        return list(self.request_projection_group_results)

    async def discover_request_candidates_page(
        self,
        *,
        media_type: str | None = None,
        genre: str | None = None,
        release_year: int | None = None,
        original_language: str | None = None,
        company: str | None = None,
        network: str | None = None,
        sort: str | None = None,
        limit: int = 20,
        offset: int = 0,
        tenant_id: str | None = None,
    ) -> Any:
        self.request_discovery_page_calls.append(
            {
                "media_type": media_type,
                "genre": genre,
                "release_year": release_year,
                "original_language": original_language,
                "company": company,
                "network": network,
                "sort": sort,
                "limit": limit,
                "offset": offset,
                "tenant_id": tenant_id,
            }
        )
        if self.request_discovery_page_result is not None:
            return self.request_discovery_page_result
        return SimpleNamespace(
            items=[],
            offset=offset,
            limit=limit,
            total_count=0,
            has_previous_page=offset > 0,
            has_next_page=False,
            result_window_complete=True,
            facets=SimpleNamespace(
                genres=[],
                release_years=[],
                languages=[],
                sorts=[
                    SimpleNamespace(
                        value="popular",
                        label="Popular",
                        selected=True,
                    )
                ],
            ),
        )

    async def get_stream_candidates(self, *, media_item_id: str) -> list[StreamORM]:
        _ = media_item_id
        return list(self.stream_candidates)

    async def get_recovery_plan(self, *, media_item_id: str) -> RecoveryPlanRecord | None:
        _ = media_item_id
        return self.recovery_plan

    async def get_workflow_checkpoint(
        self,
        *,
        media_item_id: str,
        workflow_name: str = "item_pipeline",
    ) -> WorkflowCheckpointRecord | None:
        _ = (media_item_id, workflow_name)
        return self.workflow_checkpoint

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
        _ = (title, media_type, attributes, requested_seasons, requested_episodes)
        self.request_item_calls.append(
            {
                "external_ref": external_ref,
                "media_type": media_type,
                "requested_seasons": requested_seasons,
                "requested_episodes": requested_episodes,
            }
        )
        item_id = next(iter(self.detail_by_item_id.keys()), None) or "item-requested"
        return RequestItemServiceResult(
            item=MediaItemRecord(
                id=item_id,
                external_ref=external_ref,
                title="Requested Item",
                state=ItemState.REQUESTED,
                attributes={"item_type": media_type or "movie"},
            ),
            enrichment=EnrichmentResult(
                source="tmdb",
                has_poster=True,
                has_imdb_id=True,
                has_tmdb_id=True,
                warnings=[],
            ),
        )

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
class FakeControlPlaneService:
    summary_snapshot: object = field(
        default_factory=lambda: SimpleNamespace(
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
    )
    subscribers: list[object] = field(
        default_factory=lambda: [
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
                ack_pending=True,
                fenced=True,
            )
        ]
    )
    remediation_calls: list[int] = field(default_factory=list)
    ack_recovery_calls: list[int] = field(default_factory=list)

    async def summarize_subscribers(self, *, active_within_seconds: int) -> object:
        _ = active_within_seconds
        return self.summary_snapshot

    async def list_subscribers(self, *, active_within_seconds: int) -> list[object]:
        _ = active_within_seconds
        return list(self.subscribers)

    async def remediate_subscribers(self, *, active_within_seconds: int) -> object:
        self.remediation_calls.append(active_within_seconds)
        return SimpleNamespace(
            stale_marked_subscribers=1,
            fence_resolved_subscribers=1,
            error_recovered_subscribers=0,
            total_updated_subscribers=2,
            summary=self.summary_snapshot,
        )

    async def recover_ack_backlog(self, *, active_within_seconds: int) -> object:
        self.ack_recovery_calls.append(active_within_seconds)
        return SimpleNamespace(
            rewound_subscribers=1,
            stale_marked_subscribers=1,
            pending_without_ack_subscribers=2,
            total_updated_subscribers=2,
            summary=self.summary_snapshot,
        )
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
    claim_calls: list[dict[str, object]] = field(default_factory=list)
    claim_pending_result: object | None = None

    async def pending_summary(self, *, group_name: str) -> object:
        _ = group_name
        return SimpleNamespace(
            pending_count=self.pending_count,
            oldest_event_id=self.oldest_event_id,
            latest_event_id=self.latest_event_id,
            consumer_counts=dict(self.consumer_counts),
        )

    async def claim_pending(
        self,
        *,
        group_name: str,
        consumer_name: str,
        node_id: str,
        tenant_id: str,
        min_idle_ms: int,
        count: int,
        start_id: str,
        heartbeat_expiry_seconds: int,
    ) -> object:
        self.claim_calls.append(
            {
                "group_name": group_name,
                "consumer_name": consumer_name,
                "node_id": node_id,
                "tenant_id": tenant_id,
                "min_idle_ms": min_idle_ms,
                "count": count,
                "start_id": start_id,
                "heartbeat_expiry_seconds": heartbeat_expiry_seconds,
            }
        )
        if self.claim_pending_result is not None:
            return self.claim_pending_result
        pending_snapshot = SimpleNamespace(
            pending_count=self.pending_count,
            oldest_event_id=self.oldest_event_id,
            latest_event_id=self.latest_event_id,
            consumer_counts=dict(self.consumer_counts),
        )
        return SimpleNamespace(
            claimed_events=[],
            next_start_id="0-0",
            pending_before=pending_snapshot,
            pending_after=pending_snapshot,
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
    for permission in (
        "settings:write",
        "security:policy.approve",
        "security:apikey.rotate",
        "tenant:quota.write",
    ):
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


def _build_graphql_info(app: FastAPI, *, headers: dict[str, str] | None = None) -> Any:
    from filmu_py.graphql.deps import get_graphql_context

    normalized_headers = headers or {"x-api-key": "a" * 32}
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "path": "/graphql",
        "raw_path": b"/graphql",
        "scheme": "http",
        "query_string": b"",
        "headers": [
            (key.lower().encode("latin-1"), value.encode("latin-1"))
            for key, value in normalized_headers.items()
        ],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "root_path": "",
        "app": app,
    }
    request = Request(scope)
    return SimpleNamespace(context=get_graphql_context(request))


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
        "notifications": {
            "enabled": True,
            "webhook_url": "https://notify.example/webhook",
            "contract_proof_refs": ["ops/plugins/notifications-contract.md"],
            "soak_proof_refs": ["ops/plugins/notifications-soak.md"],
        },
        "downloaders": {
            "real_debrid": {
                "enabled": True,
                "api_key": "rd-token",
                "contract_proof_refs": ["ops/plugins/realdebrid-contract.md"],
                "soak_proof_refs": ["ops/plugins/realdebrid-soak.md"],
            },
            "all_debrid": {
                "enabled": True,
                "api_key": "ad-token",
                "contract_proof_refs": ["ops/plugins/alldebrid-contract.md"],
                "soak_proof_refs": ["ops/plugins/alldebrid-soak.md"],
            },
            "debrid_link": {
                "enabled": True,
                "api_key": "dl-token",
                "contract_proof_refs": ["ops/plugins/debridlink-contract.md"],
                "soak_proof_refs": ["ops/plugins/debridlink-soak.md"],
            },
            "stremthru": {
                "enabled": True,
                "url": "https://stremthru.example",
                "token": "st-token",
                "contract_proof_refs": ["ops/plugins/stremthru-contract.md"],
                "soak_proof_refs": ["ops/plugins/stremthru-soak.md"],
            },
        },
        "metadata": {
            "tmdb": {
                "contract_proof_refs": ["ops/plugins/tmdb-contract.md"],
                "soak_proof_refs": ["ops/plugins/tmdb-soak.md"],
            },
            "tvdb": {
                "enabled": True,
                "contract_proof_refs": ["ops/plugins/tvdb-contract.md"],
                "soak_proof_refs": ["ops/plugins/tvdb-soak.md"],
            },
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
            "FILMU_PY_DOWNLOADERS": plugin_settings["downloaders"],
            "FILMU_PY_NOTIFICATIONS": plugin_settings["notifications"],
            "TMDB_API_KEY": "tmdb-token",
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
                      verifiedPlugins
                      missingVerificationPlugins
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
                      verificationStatus
                      verificationCheckCount
                      verifiedCheckCount
                      missingVerificationChecks
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
        "totalPlugins": 10,
        "enabledPlugins": 10,
        "configuredPlugins": 10,
        "contractValidatedPlugins": 10,
        "soakValidatedPlugins": 10,
        "readyPlugins": 10,
        "missingContractProofPlugins": 0,
        "missingSoakProofPlugins": 0,
        "verifiedPlugins": 10,
        "missingVerificationPlugins": 0,
    }
    assert payload["requiredActions"] == []
    assert payload["remainingGaps"] == []
    by_name = {entry["name"]: entry for entry in payload["plugins"]}
    assert by_name["comet"]["ready"] is True
    assert by_name["comet"]["endpoint"] == "https://comet.example"
    assert by_name["comet"]["endpointConfigured"] is True
    assert by_name["comet"]["contractValidated"] is True
    assert by_name["comet"]["soakValidated"] is True
    assert by_name["comet"]["verificationStatus"] == "verified"
    assert by_name["comet"]["verificationCheckCount"] == 4
    assert by_name["comet"]["verifiedCheckCount"] == 4
    assert by_name["comet"]["missingVerificationChecks"] == []
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
    assert by_name["notifications"]["endpoint"] == "https://notify.example/webhook"
    assert by_name["tmdb"]["ready"] is True
    assert by_name["tvdb"]["ready"] is True
    assert by_name["realdebrid"]["ready"] is True
    assert by_name["alldebrid"]["ready"] is True
    assert by_name["debridlink"]["ready"] is True
    assert by_name["stremthru"]["ready"] is True


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
            "FILMU_PY_DOWNLOADERS": {
                "real_debrid": {"enabled": False, "api_key": ""},
                "all_debrid": {"enabled": False, "api_key": ""},
                "debrid_link": {"enabled": False, "api_key": ""},
                "stremthru": {"enabled": False, "url": "", "token": ""},
            },
            "FILMU_PY_NOTIFICATIONS": {
                "enabled": False,
                "service_urls": [],
                "webhook_url": None,
                "discord_webhook_url": None,
            },
            "TMDB_API_KEY": "",
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
                      verifiedPlugins
                      missingVerificationPlugins
                    }
                    plugins {
                      name
                      contractProofRefs
                      contractProofs { ref category label recorded }
                      contractValidated
                      soakValidated
                      proofGapCount
                      verificationStatus
                      verificationCheckCount
                      verifiedCheckCount
                      missingVerificationChecks
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
        "verifiedPlugins": 0,
        "missingVerificationPlugins": 1,
    }
    assert payload["plugins"] == [
        {
            "name": "comet",
            "contractProofRefs": [],
            "contractProofs": [],
            "contractValidated": False,
            "soakValidated": True,
            "proofGapCount": 1,
            "verificationStatus": "partial",
            "verificationCheckCount": 4,
            "verifiedCheckCount": 3,
            "missingVerificationChecks": ["contract_proof"],
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


def test_graphql_control_plane_hot_reads_cache_and_refresh_after_recovery_mutation() -> None:
    class FakeAutomation:
        enabled = True
        interval_seconds = 30
        active_within_seconds = 180
        claim_limit = 25
        max_claim_passes = 2
        consumer_group = "filmu-api"
        consumer_name = "automation"
        last_run_at = None
        last_success_at = None
        last_failure_at = None
        consecutive_failures = 0
        last_error = None

        def snapshot(self) -> object:
            return SimpleNamespace(
                enabled=True,
                interval_seconds=30,
                active_within_seconds=180,
                pending_min_idle_ms=60_000,
                claim_limit=25,
                max_claim_passes=2,
                consumer_group="filmu-api",
                consumer_name="automation",
                service_attached=True,
                backplane_attached=True,
                last_run_at=None,
                last_success_at=None,
                last_failure_at=None,
                consecutive_failures=0,
                last_error=None,
                remediation_updated_subscribers=1,
                rewound_subscribers=1,
                claimed_pending_events=2,
                claim_passes=1,
                pending_count_after=0,
            )

    control_plane_service = FakeControlPlaneService()
    client = _build_client(
        FakeMediaService(),
        control_plane_service=control_plane_service,
        control_plane_automation=FakeAutomation(),
        replay_backplane=FakeReplayBackplane(
            pending_count=3,
            oldest_event_id="100-0",
            latest_event_id="120-0",
            consumer_counts={"worker-a": 2},
        ),
    )
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)

    query = """
        query {
          controlPlaneSummary(activeWithinSeconds: 180) {
            totalSubscribers
            statusCounts { status count }
          }
          controlPlaneStatusCounts(activeWithinSeconds: 180) {
            key
            count
          }
        }
    """

    first_response = client.post(
        "/graphql",
        headers=_graphql_headers("backend:admin"),
        json={"query": query},
    )

    assert first_response.status_code == 200
    assert first_response.json()["data"] == {
        "controlPlaneSummary": {
            "totalSubscribers": 2,
            "statusCounts": [
                {"status": "active", "count": 1},
                {"status": "stale", "count": 1},
            ],
        },
        "controlPlaneStatusCounts": [
            {"key": "active", "count": 1},
            {"key": "stale", "count": 1},
        ],
    }

    control_plane_service.summary_snapshot = SimpleNamespace(
        total_subscribers=5,
        active_subscribers=2,
        stale_subscribers=3,
        error_subscribers=0,
        fenced_subscribers=0,
        ack_pending_subscribers=3,
        stream_count=1,
        group_count=1,
        node_count=2,
        tenant_count=1,
        oldest_heartbeat_age_seconds=90.0,
        status_counts={"active": 2, "stale": 3},
        required_actions=["recover_stale_control_plane_subscribers"],
        remaining_gaps=["control-plane backlog needs recovery"],
    )

    second_response = client.post(
        "/graphql",
        headers=_graphql_headers("backend:admin"),
        json={"query": query},
    )

    assert second_response.status_code == 200
    assert second_response.json()["data"] == first_response.json()["data"]

    mutation_response = client.post(
        "/graphql",
        headers=_graphql_headers("backend:admin"),
        json={
            "query": """
                mutation {
                  remediateControlPlaneSubscribers(activeWithinSeconds: 180) {
                    totalUpdatedSubscribers
                    summary {
                      totalSubscribers
                    }
                  }
                }
            """
        },
    )

    assert mutation_response.status_code == 200
    assert mutation_response.json()["data"]["remediateControlPlaneSubscribers"] == {
        "totalUpdatedSubscribers": 2,
        "summary": {"totalSubscribers": 5},
    }

    third_response = client.post(
        "/graphql",
        headers=_graphql_headers("backend:admin"),
        json={"query": query},
    )

    assert third_response.status_code == 200
    assert third_response.json()["data"] == {
        "controlPlaneSummary": {
            "totalSubscribers": 5,
            "statusCounts": [
                {"status": "active", "count": 2},
                {"status": "stale", "count": 3},
            ],
        },
        "controlPlaneStatusCounts": [
            {"key": "active", "count": 2},
            {"key": "stale", "count": 3},
        ],
    }


def test_graphql_control_plane_recovery_mutations_follow_route_parity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane_service = FakeControlPlaneService()
    replay_backplane = FakeReplayBackplane(
        pending_count=3,
        oldest_event_id="100-0",
        latest_event_id="120-0",
        consumer_counts={"worker-a": 2, "worker-b": 1},
        claim_pending_result=SimpleNamespace(
            claimed_events=[
                SimpleNamespace(event_id="110-0"),
                SimpleNamespace(event_id="111-0"),
            ],
            next_start_id="112-0",
            pending_before=SimpleNamespace(
                pending_count=3,
                oldest_event_id="100-0",
                latest_event_id="120-0",
                consumer_counts={"worker-a": 2, "worker-b": 1},
            ),
            pending_after=SimpleNamespace(
                pending_count=1,
                oldest_event_id="119-0",
                latest_event_id="120-0",
                consumer_counts={"recovery-ops": 1},
            ),
        ),
    )
    client = _build_client(
        FakeMediaService(),
        control_plane_service=control_plane_service,
        replay_backplane=replay_backplane,
    )

    audit_calls: list[dict[str, Any]] = []

    def fake_audit_action(request: Any, **kwargs: Any) -> None:
        _ = request
        audit_calls.append(dict(kwargs))

    monkeypatch.setattr(default_routes, "audit_action", fake_audit_action)

    response = client.post(
        "/graphql",
        headers=_graphql_headers("backend:admin"),
        json={
            "query": """
                mutation RecoverControlPlane {
                  remediateControlPlaneSubscribers(activeWithinSeconds: 180) {
                    activeWithinSeconds
                    staleMarkedSubscribers
                    fenceResolvedSubscribers
                    errorRecoveredSubscribers
                    totalUpdatedSubscribers
                    summary {
                      totalSubscribers
                      staleSubscribers
                      ackPendingSubscribers
                    }
                  }
                  recoverControlPlaneAckBacklog(activeWithinSeconds: 180) {
                    activeWithinSeconds
                    rewoundSubscribers
                    staleMarkedSubscribers
                    pendingWithoutAckSubscribers
                    totalUpdatedSubscribers
                    summary {
                      totalSubscribers
                      staleSubscribers
                      ackPendingSubscribers
                    }
                  }
                  recoverControlPlanePendingEntries(
                    input: {
                      groupName: "filmu-api"
                      consumerName: "recovery-ops"
                      minIdleMs: 60000
                      claimLimit: 25
                      activeWithinSeconds: 180
                    }
                  ) {
                    groupName
                    consumerName
                    minIdleMs
                    claimLimit
                    claimedCount
                    claimedEventIds
                    nextStartId
                    pendingCountBefore
                    pendingCountAfter
                    oldestPendingEventId
                    latestPendingEventId
                    pendingConsumerCounts { key count }
                    summary {
                      totalSubscribers
                      staleSubscribers
                      ackPendingSubscribers
                    }
                    requiredActions
                    remainingGaps
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["remediateControlPlaneSubscribers"] == {
        "activeWithinSeconds": 180,
        "staleMarkedSubscribers": 1,
        "fenceResolvedSubscribers": 1,
        "errorRecoveredSubscribers": 0,
        "totalUpdatedSubscribers": 2,
        "summary": {
            "totalSubscribers": 2,
            "staleSubscribers": 1,
            "ackPendingSubscribers": 1,
        },
    }
    assert payload["recoverControlPlaneAckBacklog"] == {
        "activeWithinSeconds": 180,
        "rewoundSubscribers": 1,
        "staleMarkedSubscribers": 1,
        "pendingWithoutAckSubscribers": 2,
        "totalUpdatedSubscribers": 2,
        "summary": {
            "totalSubscribers": 2,
            "staleSubscribers": 1,
            "ackPendingSubscribers": 1,
        },
    }
    assert payload["recoverControlPlanePendingEntries"] == {
        "groupName": "filmu-api",
        "consumerName": "recovery-ops",
        "minIdleMs": 60000,
        "claimLimit": 25,
        "claimedCount": 2,
        "claimedEventIds": ["110-0", "111-0"],
        "nextStartId": "112-0",
        "pendingCountBefore": 3,
        "pendingCountAfter": 1,
        "oldestPendingEventId": "100-0",
        "latestPendingEventId": "120-0",
        "pendingConsumerCounts": [{"key": "recovery-ops", "count": 1}],
        "summary": {
            "totalSubscribers": 2,
            "staleSubscribers": 1,
            "ackPendingSubscribers": 1,
        },
        "requiredActions": ["repeat_pending_claim_until_backlog_drained"],
        "remainingGaps": [
            "bounded claim window left replay pending entries in the consumer group"
        ],
    }
    assert control_plane_service.remediation_calls == [180]
    assert control_plane_service.ack_recovery_calls == [180]
    assert replay_backplane.claim_calls == [
        {
            "group_name": "filmu-api",
            "consumer_name": "recovery-ops",
            "node_id": "operator:operator-1",
            "tenant_id": "tenant-main",
            "min_idle_ms": 60000,
            "count": 25,
            "start_id": "0-0",
            "heartbeat_expiry_seconds": 180,
        }
    ]
    assert audit_calls == [
        {
            "action": "operations.control_plane.remediate",
            "target": "operations.control_plane",
            "details": {
                "active_within_seconds": 180,
                "total_updated_subscribers": 2,
            },
        },
        {
            "action": "operations.control_plane.ack_recovery",
            "target": "operations.control_plane",
            "details": {
                "active_within_seconds": 180,
                "rewound_subscribers": 1,
                "pending_without_ack_subscribers": 2,
                "total_updated_subscribers": 2,
            },
        },
        {
            "action": "operations.control_plane.pending_recovery",
            "target": "operations.control_plane",
            "details": {
                "group_name": "filmu-api",
                "consumer_name": "recovery-ops",
                "min_idle_ms": 60000,
                "claim_limit": 25,
                "claimed_count": 2,
                "pending_count_before": 3,
                "pending_count_after": 1,
            },
        },
    ]


def test_graphql_control_plane_pending_recovery_returns_gap_state_without_backplane() -> None:
    client = _build_client(
        FakeMediaService(),
        control_plane_service=FakeControlPlaneService(),
    )

    response = client.post(
        "/graphql",
        headers=_graphql_headers("backend:admin"),
        json={
            "query": """
                mutation {
                  recoverControlPlanePendingEntries(
                    input: { consumerName: "recovery-ops", claimLimit: 25, activeWithinSeconds: 180 }
                  ) {
                    claimedCount
                    pendingCountBefore
                    pendingCountAfter
                    requiredActions
                    remainingGaps
                    summary {
                      totalSubscribers
                    }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["recoverControlPlanePendingEntries"] == {
        "claimedCount": 0,
        "pendingCountBefore": 0,
        "pendingCountAfter": 0,
        "requiredActions": ["attach_redis_replay_backplane"],
        "remainingGaps": ["durable replay backplane is not configured"],
        "summary": {"totalSubscribers": 2},
    }


def test_graphql_control_plane_recovery_mutations_require_backend_admin() -> None:
    client = _build_client(
        FakeMediaService(),
        control_plane_service=FakeControlPlaneService(),
        replay_backplane=FakeReplayBackplane(),
    )

    response = client.post(
        "/graphql",
        headers=_graphql_headers("playback:read", roles="playback:operator"),
        json={
            "query": """
                mutation {
                  remediateControlPlaneSubscribers(activeWithinSeconds: 180) {
                    totalUpdatedSubscribers
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    assert "Authorization denied (missing_permissions)" in response.json()["errors"][0]["message"]


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
                  pluginEventsPage(limit: 1, offset: 1) {
                    totalCount
                    limit
                    offset
                    hasPreviousPage
                    hasNextPage
                    publishableEventTotal
                    hookSubscriptionTotal
                    publisherCounts { key count }
                    wiringStatusCounts { key count }
                    rows {
                      name
                      publisher
                      publishableEvents
                      hookSubscriptions
                      publishableEventCount
                      hookSubscriptionCount
                      wiringStatus
                    }
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
    assert payload["pluginEventsPage"] == {
        "totalCount": 2,
        "limit": 1,
        "offset": 1,
        "hasPreviousPage": True,
        "hasNextPage": False,
        "publishableEventTotal": 1,
        "hookSubscriptionTotal": 2,
        "publisherCounts": [{"key": "community", "count": 1}],
        "wiringStatusCounts": [
            {"key": "publisher_only", "count": 1},
            {"key": "subscriber_only", "count": 1},
        ],
        "rows": [
            {
                "name": "hook-plugin",
                "publisher": None,
                "publishableEvents": [],
                "hookSubscriptions": ["item.completed", "item.state.changed"],
                "publishableEventCount": 0,
                "hookSubscriptionCount": 2,
                "wiringStatus": "subscriber_only",
            }
        ],
    }
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


def test_graphql_plugin_events_include_queued_delivery_health() -> None:
    plugin_registry = PluginRegistry()
    plugin_registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "hook-plugin",
                "version": "1.0.0",
                "api_version": "1",
                "entry_module": "plugin.py",
                "publisher": "community",
                "event_hook": "ExampleHook",
            }
        )
    )
    hook = type(
        "Hook",
        (),
        {
            "plugin_name": "hook-plugin",
            "subscribed_events": frozenset({"item.completed"}),
        },
    )()
    plugin_registry.register_capability(
        plugin_name="hook-plugin",
        kind=PluginCapabilityKind.EVENT_HOOK,
        implementation=hook,
    )
    redis = FakeOperatorRedis()
    client = _build_client(
        FakeMediaService(),
        plugin_registry=plugin_registry,
        redis=redis,
        settings_overrides={
            "FILMU_PY_PLUGIN_RUNTIME": {
                "hook_dispatch_mode": "queued",
                "queued_hook_events": ["item.completed"],
            }
        },
    )
    resources = cast(Any, client.app.state.resources)
    resources.arq_redis = redis
    asyncio.run(
        PluginHookQueueStatusStore(redis, queue_name="filmu-py").record_delivery(
            plugin_name="hook-plugin",
            event_type="item.completed",
            queued_at_seconds=10.0,
            execution_duration_seconds=0.2,
            matched_hooks=1,
            successful_hooks=1,
            timeout_hooks=0,
            failed_hooks=0,
            attempt=2,
            now_seconds=12.5,
        )
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  pluginEvents {
                    name
                    queuedHookSubscriptions
                    queuedHookSubscriptionCount
                    hookDispatchMode
                    queuedDispatchEnabled
                    queueHealthStatus
                    queueDeliveryObserved
                    queueObservationCount
                    latestQueueLagSeconds
                    maxQueueLagSeconds
                    successfulDeliveries
                    timeoutDeliveries
                    failedDeliveries
                    retriedDeliveries
                    requiredActions
                    remainingGaps
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["pluginEvents"] == [
        {
            "name": "hook-plugin",
            "queuedHookSubscriptions": ["item.completed"],
            "queuedHookSubscriptionCount": 1,
            "hookDispatchMode": "queued",
            "queuedDispatchEnabled": True,
            "queueHealthStatus": "degraded",
            "queueDeliveryObserved": True,
            "queueObservationCount": 1,
            "latestQueueLagSeconds": 2.5,
            "maxQueueLagSeconds": 2.5,
            "successfulDeliveries": 1,
            "timeoutDeliveries": 0,
            "failedDeliveries": 0,
            "retriedDeliveries": 1,
            "requiredActions": ["stabilize_hook-plugin_queued_hook_delivery"],
            "remainingGaps": ["hook-plugin queued hook deliveries require retries or time out"],
        }
    ]


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


def test_graphql_operations_governance_returns_typed_slice_posture() -> None:
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
                  operationsGovernance {
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
    payload = response.json()["data"]["operationsGovernance"]
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


def test_graphql_operations_governance_cache_hot_read_and_refresh_on_access_policy_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from filmu_py.graphql.resolvers import CoreMutationResolver, CoreQueryResolver
    from filmu_py.graphql.types import AccessPolicyRevisionWriteInput

    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)
    access_policy_service = DummyAccessPolicyService(resources.settings)
    resources.access_policy_service = access_policy_service
    resources.access_policy_snapshot = access_policy_service.snapshot
    info = _build_graphql_info(client.app, headers=_graphql_headers("settings:write"))
    query = CoreQueryResolver()
    mutation = CoreMutationResolver()
    state = {"ready": False}

    def _slice(name: str, *, ready: bool, evidence: list[str]) -> SimpleNamespace:
        return SimpleNamespace(
            name=name,
            status="ready" if ready else "blocked",
            evidence=evidence,
            required_actions=[] if ready else [f"stabilize_{name}"],
            remaining_gaps=[] if ready else [f"{name}_gap"],
        )

    async def fake_get_plugins(request: Any) -> list[object]:
        _ = request
        return []

    async def fake_operations_governance(
        *,
        request: Any,
        plugins: list[object],
    ) -> SimpleNamespace:
        _ = (request, plugins)
        ready = state["ready"]
        return SimpleNamespace(
            generated_at="2026-04-18T12:00:00Z",
            playback_gate=_slice(
                "playback_gate",
                ready=True,
                evidence=["playback_gate_environment_class=windows-native:managed"],
            ),
            operational_evidence=_slice(
                "operational_evidence",
                ready=True,
                evidence=["artifact_inventory_ready=True"],
            ),
            identity_authz=_slice(
                "identity_authz",
                ready=ready,
                evidence=[f"resource_scope_constraint_coverage={ready}"],
            ),
            tenant_boundary=_slice(
                "tenant_boundary",
                ready=True,
                evidence=["tenant_scope_enforced=True"],
            ),
            vfs_data_plane=_slice(
                "vfs_data_plane",
                ready=True,
                evidence=["catalog_watch_ready=True"],
            ),
            distributed_control_plane=_slice(
                "distributed_control_plane",
                ready=True,
                evidence=["subscriber_fencing_ready=True"],
            ),
            runtime_lifecycle=_slice(
                "runtime_lifecycle",
                ready=True,
                evidence=["runtime_lifecycle_graph_ready=True"],
            ),
            sre_program=_slice(
                "sre_program",
                ready=True,
                evidence=["drill_inventory_ready=True"],
            ),
            operator_log_pipeline=_slice(
                "operator_log_pipeline",
                ready=ready,
                evidence=[f"log_search_backend={'opensearch' if ready else 'missing'}"],
            ),
            plugin_runtime_isolation=_slice(
                "plugin_runtime_isolation",
                ready=ready,
                evidence=[f"plugin_runtime_exit_ready={1 if ready else 0}"],
            ),
            heavy_stage_workload_isolation=_slice(
                "heavy_stage_workload_isolation",
                ready=True,
                evidence=["heavy_stage_isolation_ready=True"],
            ),
            release_metadata_performance=_slice(
                "release_metadata_performance",
                ready=True,
                evidence=["release_metadata_hot_path_budget_ready=True"],
            ),
        )

    monkeypatch.setattr(default_routes, "get_plugins", fake_get_plugins)
    monkeypatch.setattr(
        default_routes,
        "_operations_governance",
        fake_operations_governance,
    )

    async def _scenario() -> None:
        hits_before = _counter_value(CACHE_HITS_TOTAL, namespace="test")

        first = await query.operations_governance(info)
        state["ready"] = True
        second = await query.operations_governance(info)

        assert first.operator_log_pipeline.status == "blocked"
        assert "log_search_backend=missing" in first.operator_log_pipeline.evidence
        assert second.operator_log_pipeline.status == "blocked"
        assert _counter_value(CACHE_HITS_TOTAL, namespace="test") == hits_before + 1

        await mutation.write_access_policy_revision(
            info,
            AccessPolicyRevisionWriteInput(
                version="2026-04-18-operations-governance-refresh",
                source="graphql_test",
                role_grants={"platform:admin": ["settings:write"]},
                principal_roles={"tenant-main:operator-1": ["platform:admin"]},
                principal_scopes={"tenant-main:operator-1": ["settings:write"]},
                principal_tenant_grants={"tenant-main:operator-1": ["tenant-main"]},
                permission_constraints={},
            ),
        )

        third = await query.operations_governance(info)
        assert third.operator_log_pipeline.status == "ready"
        assert "log_search_backend=opensearch" in third.operator_log_pipeline.evidence
        assert "resource_scope_constraint_coverage=True" in third.identity_authz.evidence

    asyncio.run(_scenario())


def test_graphql_stream_event_types_returns_topics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from filmu_py.api.routes import stream as stream_routes

    client = _build_client(FakeMediaService())

    async def _fake_get_event_types(*, event_bus: object) -> EventTypesResponse:
        _ = event_bus
        return EventTypesResponse(event_types=["item.state.changed", "logging"])

    monkeypatch.setattr(stream_routes, "get_event_types", _fake_get_event_types)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  streamEventTypes
                }
            """
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["streamEventTypes"] == ["item.state.changed", "logging"]


def test_graphql_serving_status_returns_typed_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from filmu_py.api.routes import stream as stream_routes

    client = _build_client(FakeMediaService())

    async def _fake_get_stream_status(
        *,
        request: Request,
        db: object,
        resources: object,
    ) -> object:
        _ = request, db, resources
        return SimpleNamespace(
            sessions=[
                SimpleNamespace(
                    session_id="session-1",
                    category="remote",
                    resource="item-1",
                    started_at="2026-04-18T10:00:00Z",
                    last_activity_at="2026-04-18T10:05:00Z",
                    bytes_served=1024,
                )
            ],
            handles=[
                SimpleNamespace(
                    handle_id="handle-1",
                    session_id="session-1",
                    category="remote",
                    path="/library/item-1.mkv",
                    path_id="path-1",
                    created_at="2026-04-18T10:00:01Z",
                    last_activity_at="2026-04-18T10:05:00Z",
                    bytes_served=1024,
                    read_offset=512,
                )
            ],
            paths=[
                SimpleNamespace(
                    path_id="path-1",
                    category="remote",
                    path="/library/item-1.mkv",
                    created_at="2026-04-18T10:00:01Z",
                    last_activity_at="2026-04-18T10:05:00Z",
                    size_bytes=4096,
                    active_handle_count=1,
                )
            ],
            governance=SimpleNamespace(
                active_sessions=1,
                active_handles=1,
                tracked_paths=1,
                active_local_sessions=0,
                active_remote_sessions=1,
                active_local_handles=0,
                hls_manifest_invalid=0,
                hls_route_failures_total=0,
                hls_route_failures_upstream_failed=0,
                direct_playback_refresh_trigger_tasks_active=0,
                hls_failed_lease_refresh_trigger_tasks_active=0,
                hls_restricted_fallback_refresh_trigger_tasks_active=0,
                stream_refresh_dispatch_mode="queued",
                stream_refresh_queue_enabled=1,
                stream_refresh_queue_ready=1,
            ),
        )

    monkeypatch.setattr(stream_routes, "get_stream_status", _fake_get_stream_status)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  servingStatus {
                    sessions {
                      sessionId
                      category
                      resource
                      bytesServed
                    }
                    handles {
                      handleId
                      path
                      readOffset
                    }
                    paths {
                      pathId
                      activeHandleCount
                    }
                    governance {
                      activeSessions
                      activeHandles
                      trackedPaths
                      hlsRouteFailuresTotal
                      streamRefreshDispatchMode
                      streamRefreshQueueEnabled
                      streamRefreshQueueReady
                    }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["servingStatus"]
    assert payload["sessions"] == [
        {
            "sessionId": "session-1",
            "category": "remote",
            "resource": "item-1",
            "bytesServed": 1024,
        }
    ]
    assert payload["handles"] == [
        {
            "handleId": "handle-1",
            "path": "/library/item-1.mkv",
            "readOffset": 512,
        }
    ]
    assert payload["paths"] == [
        {
            "pathId": "path-1",
            "activeHandleCount": 1,
        }
    ]
    assert payload["governance"] == {
        "activeSessions": 1,
        "activeHandles": 1,
        "trackedPaths": 1,
        "hlsRouteFailuresTotal": 0,
        "streamRefreshDispatchMode": "queued",
        "streamRefreshQueueEnabled": True,
        "streamRefreshQueueReady": True,
    }


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
                        "source_attachment_id": "attachment-1",
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
                        "lifecycle": type(
                            "MediaEntryLifecycle",
                            (),
                            {
                                "owner_kind": "media-entry",
                                "owner_id": "entry-1",
                                "active_roles": ("direct",),
                                "source_key": "persisted",
                                "source_attachment_id": "attachment-1",
                                "provider_family": "debrid",
                                "locator_source": "unrestricted-url",
                                "match_basis": "source-attachment-id",
                                "restricted_fallback": False,
                                "refresh_state": "ready",
                                "expires_at": "2026-03-15T12:00:00+00:00",
                                "last_refreshed_at": "2026-03-15T11:00:00+00:00",
                                "last_refresh_error": None,
                                "effective_refresh_state": "ready",
                                "ready_for_direct": True,
                                "ready_for_hls": True,
                                "ready_for_playback": True,
                            },
                        )(),
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
            "query": 'query { mediaItem(id: "item-1") { id title state itemType tmdbId tvdbId imdbId parentTmdbId parentTvdbId showTitle seasonNumber episodeNumber createdAt updatedAt recoveryPlan { mechanism targetStage reason nextRetryAt recoveryAttemptCount isInCooldown } streamCandidates { id rawTitle parsedTitle resolution rankScore levRatio selected passed rejectionReason } selectedStream { id rawTitle selected } playbackAttachments { id kind sourceKey provider providerDownloadId originalFilename fileSize refreshState } resolvedPlayback { directReady hlsReady missingLocalFile direct { kind locator sourceKey providerDownloadId originalFilename } } activeStream { directReady hlsReady missingLocalFile directOwner { mediaEntryIndex kind providerDownloadId originalFilename } } mediaEntries { entryType kind originalFilename sourceAttachmentId providerDownloadId size refreshState activeForDirect activeForHls isActiveStream lifecycle { ownerKind ownerId activeRoles sourceKey sourceAttachmentId providerFamily locatorSource matchBasis restrictedFallback refreshState expiresAt lastRefreshedAt lastRefreshError effectiveRefreshState readyForDirect readyForHls readyForPlayback } } } }'
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
            "sourceAttachmentId": "attachment-1",
            "providerDownloadId": "torrent-123",
            "size": 123456789,
            "refreshState": "ready",
            "activeForDirect": True,
            "activeForHls": False,
            "isActiveStream": True,
            "lifecycle": {
                "ownerKind": "media-entry",
                "ownerId": "entry-1",
                "activeRoles": ["direct"],
                "sourceKey": "persisted",
                "sourceAttachmentId": "attachment-1",
                "providerFamily": "debrid",
                "locatorSource": "unrestricted-url",
                "matchBasis": "source-attachment-id",
                "restrictedFallback": False,
                "refreshState": "ready",
                "expiresAt": "2026-03-15T12:00:00+00:00",
                "lastRefreshedAt": "2026-03-15T11:00:00+00:00",
                "lastRefreshError": None,
                "effectiveRefreshState": "ready",
                "readyForDirect": True,
                "readyForHls": True,
                "readyForPlayback": True,
            },
        }
    ]




def test_graphql_consumer_playback_item_returns_null_when_not_visible_in_vfs() -> None:
    detail = MediaItemSummaryRecord(
        id="item-hidden",
        type="movie",
        title="Hidden Movie",
        state="completed",
        tmdb_id="680",
        tvdb_id=None,
        external_ref="tmdb:680",
        aired_at="1994-10-14T00:00:00+00:00",
        poster_path="/hidden.jpg",
        created_at="2026-04-19T10:00:00+00:00",
        updated_at="2026-04-19T10:15:00+00:00",
        specialization=MediaItemSpecializationRecord(item_type="movie", tmdb_id="680"),
    )
    service = FakeMediaService(detail=detail)
    snapshot = VfsCatalogSnapshot(
        generation_id="77",
        published_at=datetime(2026, 4, 19, 12, 0, tzinfo=UTC),
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
                entry_id="file:item-visible",
                parent_entry_id="dir:/",
                path="/Movies/Visible Movie (1999).mkv",
                name="Visible Movie (1999).mkv",
                kind="file",
                correlation=VfsCatalogCorrelationKeys(
                    item_id="item-visible",
                    media_entry_id="entry-visible",
                    tenant_id="tenant-main",
                ),
                file=VfsCatalogFileEntry(
                    item_id="item-visible",
                    item_title="Visible Movie",
                    item_external_ref="tmdb:603",
                    media_entry_id="entry-visible",
                    source_attachment_id="attachment-visible",
                    media_type="movie",
                    transport="remote-direct",
                    locator="https://cdn.example.com/stream/item-visible",
                    lease_state="ready",
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=1, file_count=1, blocked_item_count=0),
    )
    client = _build_client(
        service,
        vfs_catalog_supplier=FakeVfsCatalogSupplier(snapshot=snapshot),
    )

    response = client.post(
        "/graphql",
        headers={
            **_graphql_headers("items:read", roles="consumer:user"),
            "x-actor-id": "user-1",
            "x-actor-type": "user",
            "x-tenant-id": "tenant-main",
        },
        json={
            "query": """
            query ConsumerPlaybackItem($itemId: ID!) {
              consumerPlaybackItem(itemId: $itemId) {
                summary {
                  id
                }
              }
            }
            """,
            "variables": {"itemId": "item-hidden"},
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["consumerPlaybackItem"] is None
    assert service.detail_calls == []


def test_graphql_consumer_playback_item_returns_summary_and_detail() -> None:
    detail = MediaItemSummaryRecord(
        id="item-1",
        type="episode",
        title="Example Episode",
        state="completed",
        tmdb_id="123",
        tvdb_id="456",
        external_ref="tvdb:456",
        aired_at="2026-03-01T00:00:00+00:00",
        poster_path="/poster.jpg",
        created_at="2026-03-15T10:00:00+00:00",
        updated_at="2026-03-15T12:00:00+00:00",
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
    )
    selected_stream = StreamORM(
        id="stream-1",
        media_item_id="item-1",
        raw_title="Example.Movie.1080p.WEB-DL",
        parsed_title={"title": "Example Movie"},
        resolution="1080p",
        rank=95,
        lev_ratio=0.9,
        selected=True,
    )
    service = FakeMediaService(
        detail=detail,
        stream_candidates=[selected_stream],
        recovery_plan=RecoveryPlanRecord(
            mechanism=RecoveryMechanism.ORPHAN_RECOVERY,
            target_stage=RecoveryTargetStage.FINALIZE,
            reason="attachment_refresh_pending",
            next_retry_at=None,
            recovery_attempt_count=1,
            is_in_cooldown=False,
        ),
        consumer_playback_activity=ConsumerPlaybackActivityRecord(
            generated_at="2026-04-19T12:00:00+00:00",
            total_item_count=1,
            total_view_count=2,
            total_launch_count=1,
            total_session_count=1,
            active_session_count=1,
            items=(
                ConsumerPlaybackActivityItemRecord(
                    item_id="item-1",
                    title="Example Episode",
                    subtitle="Example Show S02E07",
                    poster_path="/poster.jpg",
                    state="completed",
                    last_activity_at="2026-04-19T11:58:00+00:00",
                    last_viewed_at="2026-04-19T11:57:00+00:00",
                    last_launched_at="2026-04-19T11:56:00+00:00",
                    view_count=2,
                    launch_count=1,
                    session_count=1,
                    active_session_count=1,
                    last_session_key="session-item-1",
                    resume_position_seconds=184,
                    duration_seconds=3600,
                    progress_percent=5.1,
                    completed=False,
                    last_target="direct",
                ),
            ),
        ),
    )
    client = _build_client(service)

    response = client.post(
        "/graphql",
        headers=_graphql_headers("backend:admin"),
        json={
            "query": 'query { consumerPlaybackItem(itemId: "item-1") { summary { id externalRef title state mediaType mediaKind tmdbId tvdbId imdbId parentTmdbId parentTvdbId showTitle seasonNumber episodeNumber posterPath airedAt } detail { id title state itemType mediaType mediaKind tmdbId tvdbId imdbId parentTmdbId parentTvdbId showTitle seasonNumber episodeNumber createdAt updatedAt recoveryPlan { mechanism targetStage reason nextRetryAt recoveryAttemptCount isInCooldown } selectedStream { id rawTitle selected } resolvedPlayback { directReady hlsReady missingLocalFile } } activity { itemId lastActivityAt viewCount launchCount sessionCount activeSessionCount lastSessionKey resumePositionSeconds durationSeconds progressPercent completed lastTarget } } }'
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["consumerPlaybackItem"]
    assert payload == {
        "summary": {
            "id": "item-1",
            "externalRef": "tvdb:456",
            "title": "Example Episode",
            "state": "completed",
            "mediaType": "episode",
            "mediaKind": "EPISODE",
            "tmdbId": 123,
            "tvdbId": 456,
            "imdbId": "tt1234567",
            "parentTmdbId": 999,
            "parentTvdbId": 555,
            "showTitle": "Example Show",
            "seasonNumber": 2,
            "episodeNumber": 7,
            "posterPath": "/poster.jpg",
            "airedAt": "2026-03-01T00:00:00+00:00",
        },
        "detail": {
            "id": "item-1",
            "title": "Example Episode",
            "state": "completed",
            "itemType": "episode",
            "mediaType": "episode",
            "mediaKind": "EPISODE",
            "tmdbId": 123,
            "tvdbId": 456,
            "imdbId": "tt1234567",
            "parentTmdbId": 999,
            "parentTvdbId": 555,
            "showTitle": "Example Show",
            "seasonNumber": 2,
            "episodeNumber": 7,
            "createdAt": "2026-03-15T10:00:00+00:00",
            "updatedAt": "2026-03-15T12:00:00+00:00",
            "recoveryPlan": {
                "mechanism": "ORPHAN_RECOVERY",
                "targetStage": "FINALIZE",
                "reason": "attachment_refresh_pending",
                "nextRetryAt": None,
                "recoveryAttemptCount": 1,
                "isInCooldown": False,
            },
            "selectedStream": {
                "id": "stream-1",
                "rawTitle": "Example.Movie.1080p.WEB-DL",
                "selected": True,
            },
            "resolvedPlayback": None,
        },
        "activity": {
            "itemId": "item-1",
            "lastActivityAt": "2026-04-19T11:58:00+00:00",
            "viewCount": 2,
            "launchCount": 1,
            "sessionCount": 1,
            "activeSessionCount": 1,
            "lastSessionKey": "session-item-1",
            "resumePositionSeconds": 184,
            "durationSeconds": 3600,
            "progressPercent": 5.1,
            "completed": False,
            "lastTarget": "direct",
        },
    }
    assert service.detail_calls[-1] == {
        "item_identifier": "item-1",
        "media_type": "item",
        "extended": True,
        "tenant_id": "tenant-main",
    }
    assert service.consumer_playback_activity_calls[-1]["tenant_id"] == "tenant-main"
    assert service.consumer_playback_activity_calls[-1]["item_limit"] == 1
    assert service.consumer_playback_activity_calls[-1]["device_limit"] == 1
    assert service.consumer_playback_activity_calls[-1]["history_limit"] == 240
    assert service.consumer_playback_activity_calls[-1]["focus_item_id"] == "item-1"


def test_graphql_media_item_exposes_compact_request_lifecycle() -> None:
    service = FakeMediaService(
        detail=MediaItemSummaryRecord(
            id="item-42",
            type="movie",
            title="Ready Movie",
            state="downloaded",
            tmdb_id="42",
            external_ref="tmdb:42",
            created_at="2026-04-19T10:00:00+00:00",
            updated_at="2026-04-19T10:05:00+00:00",
            request=ItemRequestSummaryRecord(
                is_partial=False,
                requested_seasons=None,
                requested_episodes=None,
                request_source="director",
            ),
            resolved_playback=ResolvedPlaybackSnapshotRecord(
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
            ),
        ),
    )
    client = _build_client(service)

    response = client.post(
        "/graphql",
        headers=_graphql_headers("backend:admin"),
        json={
            "query": """
                query MediaItemLifecycle($id: ID!) {
                  mediaItem(id: $id) {
                    id
                    title
                    request {
                      isPartial
                      requestSource
                    }
                    requestLifecycle {
                      requestable
                      requested
                      state
                      playbackReady
                      cta
                      statusDetail
                    }
                    resolvedPlayback {
                      directReady
                    }
                  }
                }
            """,
            "variables": {"id": "item-42"},
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["mediaItem"] == {
        "id": "item-42",
        "title": "Ready Movie",
        "request": {
            "isPartial": False,
            "requestSource": "director",
        },
        "requestLifecycle": {
            "requestable": False,
            "requested": True,
            "state": "ready",
            "playbackReady": True,
            "cta": "watch",
            "statusDetail": "Playback is ready.",
        },
        "resolvedPlayback": {"directReady": True},
    }


def test_graphql_consumer_playback_activity_returns_shared_history_and_devices() -> None:
    service = FakeMediaService(
        consumer_playback_activity=ConsumerPlaybackActivityRecord(
            generated_at="2026-04-19T12:00:00+00:00",
            total_item_count=2,
            total_view_count=4,
            total_launch_count=3,
            total_session_count=2,
            active_session_count=1,
            items=(
                ConsumerPlaybackActivityItemRecord(
                    item_id="item-1",
                    title="Example Episode",
                    subtitle="Example Show S02E07",
                    poster_path="/episode.jpg",
                    state="completed",
                    request=ItemRequestSummaryRecord(
                        is_partial=True,
                        requested_seasons=[2],
                        requested_episodes={"2": [7, 8]},
                        request_source="graphql",
                    ),
                    playback_ready=True,
                    last_activity_at="2026-04-19T11:58:00+00:00",
                    last_viewed_at="2026-04-19T11:55:00+00:00",
                    last_launched_at="2026-04-19T11:56:00+00:00",
                    view_count=3,
                    launch_count=2,
                    session_count=1,
                    active_session_count=1,
                    last_session_key="session-item-1",
                    resume_position_seconds=184,
                    duration_seconds=3600,
                    progress_percent=5.1,
                    completed=False,
                    last_target="direct",
                ),
                ConsumerPlaybackActivityItemRecord(
                    item_id="item-2",
                    title="Example Movie",
                    subtitle="Movie",
                    poster_path="/movie.jpg",
                    state="completed",
                    playback_ready=True,
                    last_activity_at="2026-04-18T20:06:00+00:00",
                    last_viewed_at="2026-04-18T20:00:00+00:00",
                    last_launched_at="2026-04-18T20:05:00+00:00",
                    view_count=1,
                    launch_count=1,
                    session_count=1,
                    active_session_count=0,
                    last_session_key="session-item-2",
                    resume_position_seconds=None,
                    duration_seconds=7200,
                    progress_percent=100.0,
                    completed=True,
                    last_target="hls",
                ),
            ),
            devices=(
                ConsumerPlaybackDeviceRecord(
                    device_key="browser-firefox",
                    device_label="Firefox on Windows",
                    last_seen_at="2026-04-19T11:56:00+00:00",
                    last_activity_at="2026-04-19T11:58:00+00:00",
                    last_viewed_at="2026-04-19T11:55:00+00:00",
                    last_launched_at="2026-04-19T11:56:00+00:00",
                    launch_count=2,
                    view_count=3,
                    session_count=1,
                    active_session_count=1,
                    last_session_key="session-item-1",
                    resume_position_seconds=184,
                    duration_seconds=3600,
                    progress_percent=5.1,
                    completed_session_count=0,
                    last_target="direct",
                ),
            ),
            recent_sessions=(
                ConsumerPlaybackSessionRecord(
                    session_key="session-item-1",
                    item_id="item-1",
                    device_key="browser-firefox",
                    device_label="Firefox on Windows",
                    started_at="2026-04-19T11:55:00+00:00",
                    last_seen_at="2026-04-19T11:58:00+00:00",
                    last_target="direct",
                    active=True,
                    resume_position_seconds=184,
                    duration_seconds=3600,
                    progress_percent=5.1,
                    completed=False,
                ),
            ),
        )
    )
    client = _build_client(service)

    response = client.post(
        "/graphql",
        headers={
            **_graphql_headers("items:read", roles="consumer:user"),
            "x-actor-id": "user-1",
            "x-actor-type": "user",
            "x-tenant-id": "tenant-main",
        },
        json={
            "query": """
            query {
              consumerPlaybackActivity(itemLimit: 2, deviceLimit: 1) {
                generatedAt
                totalItemCount
                totalViewCount
                totalLaunchCount
                totalSessionCount
                activeSessionCount
                items {
                  itemId
                  title
                  subtitle
                  posterPath
                  state
                  request {
                    isPartial
                    requestedSeasons
                    requestedEpisodes {
                      seasonNumber
                      episodeNumbers
                    }
                    requestSource
                  }
                  requestLifecycle {
                    state
                    playbackReady
                    cta
                    statusDetail
                  }
                  lastActivityAt
                  lastViewedAt
                  lastLaunchedAt
                  viewCount
                  launchCount
                  sessionCount
                  activeSessionCount
                  lastSessionKey
                  resumePositionSeconds
                  durationSeconds
                  progressPercent
                  completed
                  lastTarget
                }
                devices {
                  deviceKey
                  deviceLabel
                  lastSeenAt
                  lastActivityAt
                  lastViewedAt
                  lastLaunchedAt
                  launchCount
                  viewCount
                  sessionCount
                  activeSessionCount
                  lastSessionKey
                  resumePositionSeconds
                  durationSeconds
                  progressPercent
                  completedSessionCount
                  lastTarget
                }
                recentSessions {
                  sessionKey
                  itemId
                  deviceKey
                  deviceLabel
                  startedAt
                  lastSeenAt
                  lastTarget
                  active
                  resumePositionSeconds
                  durationSeconds
                  progressPercent
                  completed
                }
              }
            }
            """
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["consumerPlaybackActivity"] == {
        "generatedAt": "2026-04-19T12:00:00+00:00",
        "totalItemCount": 2,
        "totalViewCount": 4,
        "totalLaunchCount": 3,
        "totalSessionCount": 2,
        "activeSessionCount": 1,
        "items": [
            {
                "itemId": "item-1",
                "title": "Example Episode",
                "subtitle": "Example Show S02E07",
                "posterPath": "/episode.jpg",
                "state": "completed",
                "request": {
                    "isPartial": True,
                    "requestedSeasons": [2],
                    "requestedEpisodes": [
                        {"seasonNumber": 2, "episodeNumbers": [7, 8]}
                    ],
                    "requestSource": "graphql",
                },
                "requestLifecycle": {
                    "state": "partial_ready",
                    "playbackReady": True,
                    "cta": "watch",
                    "statusDetail": "Part of the requested scope is already playable.",
                },
                "lastActivityAt": "2026-04-19T11:58:00+00:00",
                "lastViewedAt": "2026-04-19T11:55:00+00:00",
                "lastLaunchedAt": "2026-04-19T11:56:00+00:00",
                "viewCount": 3,
                "launchCount": 2,
                "sessionCount": 1,
                "activeSessionCount": 1,
                "lastSessionKey": "session-item-1",
                "resumePositionSeconds": 184,
                "durationSeconds": 3600,
                "progressPercent": 5.1,
                "completed": False,
                "lastTarget": "direct",
            },
            {
                "itemId": "item-2",
                "title": "Example Movie",
                "subtitle": "Movie",
                "posterPath": "/movie.jpg",
                "state": "completed",
                "request": None,
                "requestLifecycle": {
                    "state": "ready",
                    "playbackReady": True,
                    "cta": "watch",
                    "statusDetail": "Playback is ready.",
                },
                "lastActivityAt": "2026-04-18T20:06:00+00:00",
                "lastViewedAt": "2026-04-18T20:00:00+00:00",
                "lastLaunchedAt": "2026-04-18T20:05:00+00:00",
                "viewCount": 1,
                "launchCount": 1,
                "sessionCount": 1,
                "activeSessionCount": 0,
                "lastSessionKey": "session-item-2",
                "resumePositionSeconds": None,
                "durationSeconds": 7200,
                "progressPercent": 100.0,
                "completed": True,
                "lastTarget": "hls",
            },
        ],
        "devices": [
            {
                "deviceKey": "browser-firefox",
                "deviceLabel": "Firefox on Windows",
                "lastSeenAt": "2026-04-19T11:56:00+00:00",
                "lastActivityAt": "2026-04-19T11:58:00+00:00",
                "lastViewedAt": "2026-04-19T11:55:00+00:00",
                "lastLaunchedAt": "2026-04-19T11:56:00+00:00",
                "launchCount": 2,
                "viewCount": 3,
                "sessionCount": 1,
                "activeSessionCount": 1,
                "lastSessionKey": "session-item-1",
                "resumePositionSeconds": 184,
                "durationSeconds": 3600,
                "progressPercent": 5.1,
                "completedSessionCount": 0,
                "lastTarget": "direct",
            }
        ],
        "recentSessions": [
            {
                "sessionKey": "session-item-1",
                "itemId": "item-1",
                "deviceKey": "browser-firefox",
                "deviceLabel": "Firefox on Windows",
                "startedAt": "2026-04-19T11:55:00+00:00",
                "lastSeenAt": "2026-04-19T11:58:00+00:00",
                "lastTarget": "direct",
                "active": True,
                "resumePositionSeconds": 184,
                "durationSeconds": 3600,
                "progressPercent": 5.1,
                "completed": False,
            }
        ],
    }
    assert service.consumer_playback_activity_calls == [
        {
            "tenant_id": "tenant-main",
            "actor_id": "user-1",
            "actor_type": "user",
            "item_limit": 2,
            "device_limit": 1,
            "history_limit": 240,
            "focus_item_id": None,
        }
    ]


def test_graphql_consumer_profile_returns_identity_library_and_playback_posture() -> None:
    service = FakeMediaService(
        stats=StatsProjection(
            total_items=42,
            completed_items=30,
            failed_items=2,
            incomplete_items=10,
            movies=18,
            shows=8,
            seasons=22,
            episodes=96,
        ),
        consumer_playback_activity=ConsumerPlaybackActivityRecord(
            generated_at="2026-04-19T12:00:00+00:00",
            total_item_count=3,
            total_view_count=5,
            total_launch_count=4,
            total_session_count=3,
            active_session_count=1,
            items=(
                ConsumerPlaybackActivityItemRecord(
                    item_id="item-1",
                    title="Example Episode",
                    resume_position_seconds=184,
                    duration_seconds=3600,
                    progress_percent=5.1,
                    completed=False,
                ),
                ConsumerPlaybackActivityItemRecord(
                    item_id="item-2",
                    title="Example Movie",
                    duration_seconds=7200,
                    progress_percent=100.0,
                    completed=True,
                ),
                ConsumerPlaybackActivityItemRecord(
                    item_id="item-3",
                    title="Provider Limited Show",
                    subtitle="Series",
                    duration_seconds=2400,
                    progress_percent=24.0,
                    completed=False,
                ),
            ),
            devices=(
                ConsumerPlaybackDeviceRecord(
                    device_key="browser-firefox",
                    device_label="Firefox on Windows",
                    last_seen_at="2026-04-19T11:56:00+00:00",
                ),
            ),
            recent_sessions=(
                ConsumerPlaybackSessionRecord(
                    session_key="session-item-1",
                    item_id="item-1",
                    device_key="browser-firefox",
                    device_label="Firefox on Windows",
                    started_at="2026-04-19T11:55:00+00:00",
                    last_seen_at="2026-04-19T11:58:00+00:00",
                    active=True,
                    resume_position_seconds=184,
                    duration_seconds=3600,
                    progress_percent=5.1,
                    completed=False,
                ),
            ),
        ),
        detail_by_item_id={
            "item-1": cast(
                MediaItemSummaryRecord,
                SimpleNamespace(
                    state="Available",
                    resolved_playback=SimpleNamespace(
                        direct_ready=True,
                        hls_ready=False,
                        missing_local_file=False,
                    ),
                    active_stream=None,
                    media_entries=[],
                ),
            ),
            "item-2": cast(
                MediaItemSummaryRecord,
                SimpleNamespace(
                    state="Failed",
                    resolved_playback=SimpleNamespace(
                        direct_ready=False,
                        hls_ready=False,
                        missing_local_file=False,
                    ),
                    active_stream=None,
                    media_entries=[
                        SimpleNamespace(
                            provider="realdebrid",
                            refresh_state="failed",
                            last_refresh_error="provider refresh denied",
                            lifecycle=SimpleNamespace(
                                ready_for_direct=False,
                                ready_for_hls=False,
                                ready_for_playback=False,
                                restricted_fallback=False,
                                effective_refresh_state="failed",
                                last_refresh_error="provider refresh denied",
                            ),
                        )
                    ],
                ),
            ),
            "item-3": cast(
                MediaItemSummaryRecord,
                SimpleNamespace(
                    state="Queued",
                    resolved_playback=SimpleNamespace(
                        direct_ready=False,
                        hls_ready=False,
                        missing_local_file=True,
                    ),
                    active_stream=None,
                    media_entries=[
                        SimpleNamespace(
                            provider="premiumize",
                            refresh_state="stale",
                            last_refresh_error=None,
                            lifecycle=SimpleNamespace(
                                ready_for_direct=False,
                                ready_for_hls=False,
                                ready_for_playback=False,
                                restricted_fallback=True,
                                effective_refresh_state="stale",
                                last_refresh_error=None,
                            ),
                        )
                    ],
                ),
            ),
        },
    )
    client = _build_client(service)

    response = client.post(
        "/graphql",
        headers={
            **_graphql_headers("items:read", roles="consumer:user"),
            "x-actor-id": "user-1",
            "x-actor-type": "user",
            "x-actor-display-name": "Ada Lovelace",
            "x-actor-email": "ada@example.com",
            "x-tenant-id": "tenant-main",
            "x-tenant-display-name": "Filmu Preview",
            "x-tenant-plan": "pro",
            "x-auth-source": "cookie",
        },
        json={
            "query": """
            query {
              consumerProfile(itemLimit: 2, deviceLimit: 2) {
                authenticated
                identity {
                  displayName
                  email
                  statusLabel
                  sourceLabel
                  actorId
                  actorType
                  authenticationMode
                }
                workspace {
                  id
                  name
                  planLabel
                  accessPolicyVersion
                  quotaPolicyVersion
                  quotaEnabled
                }
                library {
                  totalItems
                  completedItems
                  failedItems
                }
                playbackSummary {
                  activeSessionCount
                  resumeItemCount
                  completedItemCount
                  stalledItemCount
                  recentDeviceCount
                  recentSessionCount
                }
                availabilitySummary {
                  trackedItemCount
                  playbackReadyCount
                  refreshBlockedCount
                  providerLimitedCount
                  pendingCount
                }
                availabilityItems {
                  itemId
                  title
                  postureKey
                  postureLabel
                  detail
                  directReady
                  hlsReady
                  missingLocalFile
                  effectiveRefreshState
                  providerLabels
                }
                playback {
                  totalItemCount
                  activeSessionCount
                }
                postureNotes
              }
            }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["consumerProfile"]
    assert payload["authenticated"] is True
    assert payload["identity"] == {
        "displayName": "Ada Lovelace",
        "email": "ada@example.com",
        "statusLabel": "Signed in",
        "sourceLabel": "cookie",
        "actorId": "user-1",
        "actorType": "user",
        "authenticationMode": "api_key",
    }
    assert payload["workspace"]["id"] == "tenant-main"
    assert payload["workspace"]["name"] == "Filmu Preview"
    assert payload["workspace"]["planLabel"] == "Pro"
    assert isinstance(payload["workspace"]["accessPolicyVersion"], str)
    assert payload["workspace"]["quotaEnabled"] in {True, False, None}
    assert payload["library"] == {
        "totalItems": 42,
        "completedItems": 30,
        "failedItems": 2,
    }
    assert payload["playbackSummary"] == {
        "activeSessionCount": 1,
        "resumeItemCount": 1,
        "completedItemCount": 1,
        "stalledItemCount": 2,
        "recentDeviceCount": 1,
        "recentSessionCount": 1,
    }
    assert payload["availabilitySummary"] == {
        "trackedItemCount": 3,
        "playbackReadyCount": 1,
        "refreshBlockedCount": 1,
        "providerLimitedCount": 1,
        "pendingCount": 0,
    }
    assert payload["availabilityItems"] == [
        {
            "itemId": "item-1",
            "title": "Example Episode",
            "postureKey": "playback-ready",
            "postureLabel": "Playback ready",
            "detail": "Direct playback is ready.",
            "directReady": True,
            "hlsReady": False,
            "missingLocalFile": False,
            "effectiveRefreshState": None,
            "providerLabels": [],
        },
        {
            "itemId": "item-2",
            "title": "Example Movie",
            "postureKey": "refresh-blocked",
            "postureLabel": "Refresh blocked",
            "detail": "provider refresh denied",
            "directReady": False,
            "hlsReady": False,
            "missingLocalFile": False,
            "effectiveRefreshState": "failed",
            "providerLabels": ["realdebrid"],
        },
        {
            "itemId": "item-3",
            "title": "Provider Limited Show",
            "postureKey": "provider-limited",
            "postureLabel": "Provider limited",
            "detail": "Playback is waiting on a provider-backed file.",
            "directReady": False,
            "hlsReady": False,
            "missingLocalFile": True,
            "effectiveRefreshState": "stale",
            "providerLabels": ["premiumize"],
        },
    ]
    assert payload["playback"] == {
        "totalItemCount": 3,
        "activeSessionCount": 1,
    }
    assert payload["postureNotes"][0] == (
        "Authentication mode api_key is mapped to user actor user-1."
    )
    assert payload["postureNotes"][2:] == [
        "42 tracked items, 30 completed, 2 failed.",
        "1 active sessions across 1 recent devices.",
        "Recent availability window: 1 ready, 1 blocked, 1 provider-limited.",
    ]
    assert service.consumer_playback_activity_calls == [
        {
            "tenant_id": "tenant-main",
            "actor_id": "user-1",
            "actor_type": "user",
            "item_limit": 2,
            "device_limit": 2,
            "history_limit": 240,
            "focus_item_id": None,
        }
    ]
    assert service.detail_calls == [
        {
            "item_identifier": "item-1",
            "media_type": "item",
            "extended": True,
            "tenant_id": "tenant-main",
        },
        {
            "item_identifier": "item-2",
            "media_type": "item",
            "extended": True,
            "tenant_id": "tenant-main",
        },
        {
            "item_identifier": "item-3",
            "media_type": "item",
            "extended": True,
            "tenant_id": "tenant-main",
        },
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




def test_graphql_consumer_available_items_page_filters_to_requested_and_mounted_titles() -> None:
    media_service = FakeMediaService(
        item_records=[
            MediaItemRecord(
                id="movie-visible",
                external_ref="tmdb:603",
                title="The Matrix",
                state=ItemState.COMPLETED,
                attributes={
                    "item_type": "movie",
                    "tmdb_id": "603",
                    "poster_path": "/matrix.jpg",
                    "aired_at": "1999-03-31T00:00:00Z",
                },
            ),
            MediaItemRecord(
                id="movie-hidden",
                external_ref="tmdb:680",
                title="Pulp Fiction",
                state=ItemState.COMPLETED,
                attributes={
                    "item_type": "movie",
                    "tmdb_id": "680",
                    "poster_path": "/pulp.jpg",
                    "aired_at": "1994-10-14T00:00:00Z",
                },
            ),
        ]
    )
    snapshot = VfsCatalogSnapshot(
        generation_id="42",
        published_at=datetime(2026, 4, 19, 12, 0, tzinfo=UTC),
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
                entry_id="file:movie-visible",
                parent_entry_id="dir:/",
                path="/Movies/The Matrix (1999).mkv",
                name="The Matrix (1999).mkv",
                kind="file",
                correlation=VfsCatalogCorrelationKeys(
                    item_id="movie-visible",
                    media_entry_id="entry-visible",
                    tenant_id="tenant-main",
                ),
                file=VfsCatalogFileEntry(
                    item_id="movie-visible",
                    item_title="The Matrix",
                    item_external_ref="tmdb:603",
                    media_entry_id="entry-visible",
                    source_attachment_id="attachment-visible",
                    media_type="movie",
                    transport="remote-direct",
                    locator="https://cdn.example.com/stream/movie-visible",
                    lease_state="ready",
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=1, file_count=1, blocked_item_count=0),
    )
    client = _build_client(
        media_service,
        vfs_catalog_supplier=FakeVfsCatalogSupplier(snapshot=snapshot),
    )

    response = client.post(
        "/graphql",
        headers={
            **_graphql_headers("items:read", roles="consumer:user"),
            "x-actor-id": "user-1",
            "x-actor-type": "user",
            "x-tenant-id": "tenant-main",
        },
        json={
            "query": """
                query ConsumerAvailableItemsPage($limit: Int!, $page: Int!) {
                  consumerAvailableItemsPage(limit: $limit, page: $page, sort: "recent") {
                    totalCount
                    page
                    limit
                    totalPages
                    hasPreviousPage
                    hasNextPage
                    items {
                      id
                      title
                    }
                  }
                }
            """,
            "variables": {"limit": 24, "page": 1},
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["consumerAvailableItemsPage"] == {
        "totalCount": 1,
        "page": 1,
        "limit": 24,
        "totalPages": 1,
        "hasPreviousPage": False,
        "hasNextPage": False,
        "items": [
            {
                "id": "movie-visible",
                "title": "The Matrix",
            }
        ],
    }
    assert media_service.search_item_calls == [
        {
            "limit": 24,
            "page": 1,
            "item_types": None,
            "states": None,
            "sort": ["date_desc"],
            "search": None,
            "extended": False,
            "tenant_id": "tenant-main",
            "allowed_item_ids": ["movie-visible"],
        }
    ]


def test_graphql_library_items_page_uses_native_filters_and_page_metadata() -> None:
    media_service = FakeMediaService(
        item_records=[
            MediaItemRecord(
                id="movie-1",
                external_ref="tmdb:603",
                title="The Matrix",
                state=ItemState.COMPLETED,
                attributes={
                    "item_type": "movie",
                    "tmdb_id": "603",
                    "poster_path": "/matrix.jpg",
                    "aired_at": "1999-03-31T00:00:00Z",
                },
            )
        ]
    )
    client = _build_client(media_service)

    response = client.post(
        "/graphql",
        headers=_graphql_headers(),
        json={
            "query": """
                query ConsumerLibraryPage(
                  $query: String
                  $state: String
                  $itemType: String
                  $sort: String
                  $limit: Int!
                  $page: Int!
                ) {
                  libraryItemsPage(
                    query: $query
                    state: $state
                    itemType: $itemType
                    sort: $sort
                    limit: $limit
                    page: $page
                  ) {
                    totalCount
                    page
                    limit
                    totalPages
                    hasPreviousPage
                    hasNextPage
                    items {
                      id
                      title
                      mediaKind
                    }
                  }
                }
            """,
            "variables": {
                "query": "matrix",
                "state": "completed",
                "itemType": "movie",
                "sort": "relevance",
                "limit": 24,
                "page": 2,
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["libraryItemsPage"] == {
        "totalCount": 1,
        "page": 2,
        "limit": 24,
        "totalPages": 1,
        "hasPreviousPage": True,
        "hasNextPage": False,
        "items": [
            {
                "id": "movie-1",
                "title": "The Matrix",
                "mediaKind": "MOVIE",
            }
        ],
    }
    assert media_service.search_item_calls == [
        {
            "limit": 24,
            "page": 2,
            "item_types": ["movie"],
            "states": ["completed"],
            "sort": ["relevance"],
            "search": "matrix",
            "extended": False,
            "tenant_id": "tenant-main",
            "allowed_item_ids": None,
        }
    ]




def test_graphql_request_search_returns_requestable_hits() -> None:
    media_service = FakeMediaService(
        request_search_results=[
            SimpleNamespace(
                external_ref="tmdb:603",
                title="The Matrix",
                media_type="movie",
                tmdb_id="603",
                tvdb_id=None,
                imdb_id="tt0133093",
                poster_path="/matrix.jpg",
                overview="Wake up, Neo.",
                year=1999,
                is_requested=True,
                requested_item_id="item-603",
                requested_state="requested",
                requested_seasons=None,
                requested_episodes=None,
                request_source="webhook:overseerr",
                request_count=3,
                first_requested_at="2026-04-17T08:00:00Z",
                last_requested_at="2026-04-18T09:30:00Z",
                lifecycle=SimpleNamespace(
                    stage_name="debrid_item",
                    stage_status="failed",
                    provider="realdebrid",
                    provider_download_id="download-603",
                    last_error="provider timeout",
                    updated_at="2026-04-18T09:31:00Z",
                    recovery_reason="provider_timeout",
                    retry_at="2026-04-18T09:36:00Z",
                    recovery_attempt_count=2,
                    in_cooldown=True,
                ),
            ),
            SimpleNamespace(
                external_ref="tmdb:1399",
                title="Game of Thrones",
                media_type="show",
                tmdb_id="1399",
                tvdb_id=None,
                imdb_id=None,
                poster_path="/got.jpg",
                overview="Winter is coming.",
                year=2011,
                is_requested=False,
                requested_item_id=None,
                requested_state=None,
                requested_seasons=[1, 2],
                requested_episodes={"1": [1, 2]},
                request_source=None,
                request_count=0,
                first_requested_at=None,
                last_requested_at=None,
                lifecycle=None,
            ),
        ]
    )
    client = _build_client(media_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query ConsumerRequestSearch($query: String!, $limit: Int!) {
                  requestSearch(query: $query, limit: $limit) {
                    externalRef
                    title
                    mediaType
                    mediaKind
                    tmdbId
                    imdbId
                    posterPath
                    overview
                    year
                    isRequested
                    requestedItemId
                    requestedState
                    requestedSeasons
                    requestedEpisodes {
                      seasonNumber
                      episodeNumbers
                    }
                    requestSource
                    requestCount
                    firstRequestedAt
                    lastRequestedAt
                    requestLifecycle {
                      requestable
                      requested
                      state
                      playbackReady
                      cta
                      statusDetail
                    }
                    lifecycle {
                      stageName
                      stageStatus
                      provider
                      providerDownloadId
                      lastError
                      updatedAt
                      recoveryReason
                      retryAt
                      recoveryAttemptCount
                      inCooldown
                    }
                  }
                }
            """,
            "variables": {"query": "matrix", "limit": 2},
        },
    )

    assert response.status_code == 200
    assert media_service.request_search_calls[0]["query"] == "matrix"
    payload = response.json()["data"]["requestSearch"]
    assert payload == [
        {
            "externalRef": "tmdb:603",
            "title": "The Matrix",
            "mediaType": "movie",
            "mediaKind": "MOVIE",
            "tmdbId": 603,
            "imdbId": "tt0133093",
            "posterPath": "/matrix.jpg",
            "overview": "Wake up, Neo.",
            "year": 1999,
            "isRequested": True,
            "requestedItemId": "item-603",
            "requestedState": "requested",
            "requestedSeasons": None,
            "requestedEpisodes": None,
            "requestSource": "webhook:overseerr",
            "requestCount": 3,
            "firstRequestedAt": "2026-04-17T08:00:00Z",
            "lastRequestedAt": "2026-04-18T09:30:00Z",
            "requestLifecycle": {
                "requestable": False,
                "requested": True,
                "state": "failed",
                "playbackReady": False,
                "cta": "retry_later",
                "statusDetail": "provider timeout",
            },
            "lifecycle": {
                "stageName": "debrid_item",
                "stageStatus": "failed",
                "provider": "realdebrid",
                "providerDownloadId": "download-603",
                "lastError": "provider timeout",
                "updatedAt": "2026-04-18T09:31:00Z",
                "recoveryReason": "provider_timeout",
                "retryAt": "2026-04-18T09:36:00Z",
                "recoveryAttemptCount": 2,
                "inCooldown": True,
            },
        },
        {
            "externalRef": "tmdb:1399",
            "title": "Game of Thrones",
            "mediaType": "show",
            "mediaKind": "SHOW",
            "tmdbId": 1399,
            "imdbId": None,
            "posterPath": "/got.jpg",
            "overview": "Winter is coming.",
            "year": 2011,
            "isRequested": False,
            "requestedItemId": None,
            "requestedState": None,
            "requestedSeasons": [1, 2],
            "requestedEpisodes": [
                {
                    "seasonNumber": 1,
                    "episodeNumbers": [1, 2],
                }
            ],
            "requestSource": None,
            "requestCount": 0,
            "firstRequestedAt": None,
            "lastRequestedAt": None,
            "requestLifecycle": {
                "requestable": True,
                "requested": False,
                "state": "discoverable",
                "playbackReady": False,
                "cta": "request",
                "statusDetail": "Title can be requested.",
            },
            "lifecycle": None,
        },
    ]


def test_graphql_request_flow_e2e_covers_discovery_request_detail_and_playback_readiness() -> None:
    @dataclass
    class FlowMediaService(FakeMediaService):
        requested_item_id: str = "item-603"

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
            _ = (title, attributes, requested_seasons, requested_episodes)
            self.request_item_calls.append(
                {
                    "external_ref": external_ref,
                    "media_type": media_type,
                    "requested_seasons": requested_seasons,
                    "requested_episodes": requested_episodes,
                }
            )
            self.detail_by_item_id[self.requested_item_id] = MediaItemSummaryRecord(
                id=self.requested_item_id,
                type="movie",
                title="The Matrix",
                state="downloaded",
                tmdb_id="603",
                external_ref=external_ref,
                created_at="2026-04-19T12:00:00+00:00",
                updated_at="2026-04-19T12:01:00+00:00",
                request=ItemRequestSummaryRecord(
                    is_partial=False,
                    requested_seasons=None,
                    requested_episodes=None,
                    request_source="director",
                ),
                resolved_playback=ResolvedPlaybackSnapshotRecord(
                    direct=ResolvedPlaybackAttachmentRecord(
                        kind="remote-direct",
                        locator="https://edge.example.com/current-matrix",
                        source_key="persisted",
                        unrestricted_url="https://edge.example.com/current-matrix",
                    ),
                    hls=None,
                    direct_ready=True,
                    hls_ready=False,
                    missing_local_file=False,
                ),
            )
            return RequestItemServiceResult(
                item=MediaItemRecord(
                    id=self.requested_item_id,
                    external_ref=external_ref,
                    title="The Matrix",
                    state=ItemState.DOWNLOADED,
                    attributes={"item_type": media_type or "movie", "tmdb_id": "603"},
                ),
                enrichment=EnrichmentResult(
                    source="tmdb",
                    has_poster=True,
                    has_imdb_id=True,
                    has_tmdb_id=True,
                    warnings=[],
                ),
            )

    service = FlowMediaService(
        request_search_results=[
            SimpleNamespace(
                external_ref="tmdb:603",
                title="The Matrix",
                media_type="movie",
                tmdb_id="603",
                tvdb_id=None,
                imdb_id="tt0133093",
                poster_path="/matrix.jpg",
                overview="Wake up, Neo.",
                year=1999,
                is_requested=False,
                requested_item_id=None,
                requested_state=None,
                requested_seasons=None,
                requested_episodes=None,
                request_source=None,
                request_count=0,
                first_requested_at=None,
                last_requested_at=None,
                lifecycle=None,
            )
        ]
    )
    client = _build_client(service)

    discovery = client.post(
        "/graphql",
        json={
            "query": """
                query RequestSearch($query: String!) {
                  requestSearch(query: $query, limit: 1) {
                    externalRef
                    requestLifecycle {
                      state
                      cta
                    }
                  }
                }
            """,
            "variables": {"query": "matrix"},
        },
    )
    assert discovery.status_code == 200
    assert discovery.json()["data"]["requestSearch"] == [
        {
            "externalRef": "tmdb:603",
            "requestLifecycle": {"state": "discoverable", "cta": "request"},
        }
    ]

    requested = client.post(
        "/graphql",
        json={
            "query": """
                mutation RequestItem($externalRef: String!, $mediaType: String!) {
                  requestItem(input: { externalRef: $externalRef, mediaType: $mediaType }) {
                    itemId
                    enrichmentSource
                  }
                }
            """,
            "variables": {"externalRef": "tmdb:603", "mediaType": "movie"},
        },
    )
    assert requested.status_code == 200
    assert requested.json()["data"]["requestItem"] == {
        "itemId": "item-603",
        "enrichmentSource": "tmdb",
    }

    detail = client.post(
        "/graphql",
        headers=_graphql_headers("backend:admin"),
        json={
            "query": """
                query RequestedItem($id: ID!) {
                  mediaItem(id: $id) {
                    title
                    requestLifecycle {
                      state
                      playbackReady
                      cta
                    }
                    resolvedPlayback {
                      directReady
                    }
                  }
                }
            """,
            "variables": {"id": "item-603"},
        },
    )
    assert detail.status_code == 200
    assert detail.json()["data"]["mediaItem"] == {
        "title": "The Matrix",
        "requestLifecycle": {
            "state": "ready",
            "playbackReady": True,
            "cta": "watch",
        },
        "resolvedPlayback": {"directReady": True},
    }


def test_graphql_request_discovery_returns_zero_query_rails() -> None:
    media_service = FakeMediaService(
        request_discovery_results=[
            SimpleNamespace(
                rail_id="new-sci-fi-films",
                title="New sci-fi films",
                description="Fresh science-fiction film candidates with immediate request coverage.",
                query="science fiction",
                media_type="movie",
                items=[
                    SimpleNamespace(
                        external_ref="tmdb:603",
                        title="The Matrix",
                        media_type="movie",
                        tmdb_id="603",
                        tvdb_id=None,
                        imdb_id="tt0133093",
                        poster_path="/matrix.jpg",
                        overview="Wake up, Neo.",
                        year=1999,
                        is_requested=True,
                        requested_item_id="item-603",
                        requested_state="requested",
                        requested_seasons=None,
                        requested_episodes=None,
                        request_source="webhook:overseerr",
                        request_count=3,
                        first_requested_at="2026-04-17T08:00:00Z",
                        last_requested_at="2026-04-18T09:30:00Z",
                        lifecycle=None,
                    )
                ],
            ),
            SimpleNamespace(
                rail_id="prestige-series",
                title="Prestige series",
                description="Returnable drama series that benefit from scoped intake.",
                query="prestige drama",
                media_type="show",
                items=[
                    SimpleNamespace(
                        external_ref="tmdb:1399",
                        title="Game of Thrones",
                        media_type="show",
                        tmdb_id="1399",
                        tvdb_id=None,
                        imdb_id=None,
                        poster_path="/got.jpg",
                        overview="Winter is coming.",
                        year=2011,
                        is_requested=False,
                        requested_item_id=None,
                        requested_state=None,
                        requested_seasons=[1, 2],
                        requested_episodes={"1": [1, 2]},
                        request_source=None,
                        request_count=0,
                        first_requested_at=None,
                        last_requested_at=None,
                        lifecycle=None,
                    )
                ],
            ),
        ]
    )
    client = _build_client(media_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query ConsumerRequestDiscovery($limitPerRail: Int!) {
                  requestDiscovery(limitPerRail: $limitPerRail) {
                    railId
                    title
                    description
                    query
                    mediaType
                    mediaKind
                    items {
                      externalRef
                      title
                      mediaType
                      mediaKind
                      isRequested
                      rankingSignals
                    }
                  }
                }
            """,
            "variables": {"limitPerRail": 6},
        },
    )

    assert response.status_code == 200
    assert media_service.request_discovery_calls == [
        {
            "limit_per_rail": 6,
            "rail_ids": None,
            "tenant_id": "global",
        }
    ]
    assert response.json()["data"]["requestDiscovery"] == [
        {
            "railId": "new-sci-fi-films",
            "title": "New sci-fi films",
            "description": "Fresh science-fiction film candidates with immediate request coverage.",
            "query": "science fiction",
            "mediaType": "movie",
            "mediaKind": "MOVIE",
            "items": [
                {
                    "externalRef": "tmdb:603",
                    "title": "The Matrix",
                    "mediaType": "movie",
                    "mediaKind": "MOVIE",
                    "isRequested": True,
                    "rankingSignals": [],
                }
            ],
        },
        {
            "railId": "prestige-series",
            "title": "Prestige series",
            "description": "Returnable drama series that benefit from scoped intake.",
            "query": "prestige drama",
            "mediaType": "show",
            "mediaKind": "SHOW",
            "items": [
                {
                    "externalRef": "tmdb:1399",
                    "title": "Game of Thrones",
                    "mediaType": "show",
                    "mediaKind": "SHOW",
                    "isRequested": False,
                    "rankingSignals": [],
                }
            ],
        },
    ]


def test_graphql_request_editorial_families_returns_backend_owned_editorial_windows() -> None:
    media_service = FakeMediaService(
        request_editorial_family_results=[
            SimpleNamespace(
                family_id="trending-films",
                title="Trending films",
                description="Fast-moving film picks pulled from the live TMDB trend window.",
                family="trending",
                media_type="movie",
                items=[
                    SimpleNamespace(
                        external_ref="tmdb:603",
                        title="The Matrix",
                        media_type="movie",
                        tmdb_id="603",
                        tvdb_id=None,
                        imdb_id="tt0133093",
                        poster_path="/matrix.jpg",
                        overview="Wake up, Neo.",
                        year=1999,
                        is_requested=False,
                        requested_item_id=None,
                        requested_state=None,
                        requested_seasons=None,
                        requested_episodes=None,
                        request_source=None,
                        request_count=0,
                        first_requested_at=None,
                        last_requested_at=None,
                        lifecycle=None,
                    )
                ],
            ),
            SimpleNamespace(
                family_id="returning-series",
                title="Returning series",
                description="Series currently back on air and suited for scoped intake follow-through.",
                family="returning",
                media_type="show",
                items=[
                    SimpleNamespace(
                        external_ref="tmdb:1399",
                        title="Game of Thrones",
                        media_type="show",
                        tmdb_id="1399",
                        tvdb_id=None,
                        imdb_id=None,
                        poster_path="/got.jpg",
                        overview="Winter is coming.",
                        year=2011,
                        is_requested=True,
                        requested_item_id="item-1399",
                        requested_state="requested",
                        requested_seasons=[1, 2],
                        requested_episodes={"1": [1, 2]},
                        request_source="graphql",
                        request_count=3,
                        first_requested_at="2026-04-17T08:00:00Z",
                        last_requested_at="2026-04-18T09:30:00Z",
                        lifecycle=None,
                    )
                ],
            ),
        ]
    )
    client = _build_client(media_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query ConsumerRequestEditorialFamilies($limitPerFamily: Int!) {
                  requestEditorialFamilies(limitPerFamily: $limitPerFamily) {
                    familyId
                    title
                    description
                    family
                    mediaType
                    mediaKind
                    items {
                      externalRef
                      title
                      mediaType
                      mediaKind
                      isRequested
                      rankingSignals
                    }
                  }
                }
            """,
            "variables": {"limitPerFamily": 4},
        },
    )

    assert response.status_code == 200
    assert media_service.request_editorial_family_calls == [
        {
            "limit_per_family": 4,
            "family_ids": None,
            "tenant_id": "global",
        }
    ]
    assert response.json()["data"]["requestEditorialFamilies"] == [
        {
            "familyId": "trending-films",
            "title": "Trending films",
            "description": "Fast-moving film picks pulled from the live TMDB trend window.",
            "family": "trending",
            "mediaType": "movie",
            "mediaKind": "MOVIE",
            "items": [
                {
                    "externalRef": "tmdb:603",
                    "title": "The Matrix",
                    "mediaType": "movie",
                    "mediaKind": "MOVIE",
                    "isRequested": False,
                    "rankingSignals": [],
                }
            ],
        },
        {
            "familyId": "returning-series",
            "title": "Returning series",
            "description": "Series currently back on air and suited for scoped intake follow-through.",
            "family": "returning",
            "mediaType": "show",
            "mediaKind": "SHOW",
            "items": [
                {
                    "externalRef": "tmdb:1399",
                    "title": "Game of Thrones",
                    "mediaType": "show",
                    "mediaKind": "SHOW",
                    "isRequested": True,
                    "rankingSignals": [],
                }
            ],
        },
    ]


def test_graphql_request_release_windows_returns_backend_owned_temporal_windows() -> None:
    media_service = FakeMediaService(
        request_release_window_results=[
            SimpleNamespace(
                window_id="theatrical-films",
                title="Theatrical window",
                description="Films playing in the near theatrical window for quick intake decisions.",
                window="theatrical",
                media_type="movie",
                items=[
                    SimpleNamespace(
                        external_ref="tmdb:603",
                        title="The Matrix",
                        media_type="movie",
                        tmdb_id="603",
                        tvdb_id=None,
                        imdb_id="tt0133093",
                        poster_path="/matrix.jpg",
                        overview="Wake up, Neo.",
                        year=1999,
                        is_requested=False,
                        requested_item_id=None,
                        requested_state=None,
                        requested_seasons=None,
                        requested_episodes=None,
                        request_source=None,
                        request_count=0,
                        first_requested_at=None,
                        last_requested_at=None,
                        lifecycle=None,
                    )
                ],
            ),
            SimpleNamespace(
                window_id="limited-series-launches",
                title="Limited-series launches",
                description="Bounded series launches that fit short-run intake and completion loops.",
                window="limited-series",
                media_type="show",
                items=[
                    SimpleNamespace(
                        external_ref="tmdb:1399",
                        title="Game of Thrones",
                        media_type="show",
                        tmdb_id="1399",
                        tvdb_id=None,
                        imdb_id=None,
                        poster_path="/got.jpg",
                        overview="Winter is coming.",
                        year=2011,
                        is_requested=True,
                        requested_item_id="item-1399",
                        requested_state="requested",
                        requested_seasons=[1],
                        requested_episodes={"1": [1]},
                        request_source="graphql",
                        request_count=1,
                        first_requested_at="2026-04-18T09:00:00Z",
                        last_requested_at="2026-04-18T09:00:00Z",
                        lifecycle=None,
                    )
                ],
            ),
        ]
    )
    client = _build_client(media_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query ConsumerRequestReleaseWindows($limitPerWindow: Int!) {
                  requestReleaseWindows(limitPerWindow: $limitPerWindow) {
                    windowId
                    title
                    description
                    window
                    mediaType
                    mediaKind
                    items {
                      externalRef
                      title
                      mediaType
                      mediaKind
                      isRequested
                    }
                  }
                }
            """,
            "variables": {"limitPerWindow": 3},
        },
    )

    assert response.status_code == 200
    assert media_service.request_release_window_calls == [
        {
            "limit_per_window": 3,
            "window_ids": None,
            "tenant_id": "global",
        }
    ]
    assert response.json()["data"]["requestReleaseWindows"] == [
        {
            "windowId": "theatrical-films",
            "title": "Theatrical window",
            "description": "Films playing in the near theatrical window for quick intake decisions.",
            "window": "theatrical",
            "mediaType": "movie",
            "mediaKind": "MOVIE",
            "items": [
                {
                    "externalRef": "tmdb:603",
                    "title": "The Matrix",
                    "mediaType": "movie",
                    "mediaKind": "MOVIE",
                    "isRequested": False,
                }
            ],
        },
        {
            "windowId": "limited-series-launches",
            "title": "Limited-series launches",
            "description": "Bounded series launches that fit short-run intake and completion loops.",
            "window": "limited-series",
            "mediaType": "show",
            "mediaKind": "SHOW",
            "items": [
                {
                    "externalRef": "tmdb:1399",
                    "title": "Game of Thrones",
                    "mediaType": "show",
                    "mediaKind": "SHOW",
                    "isRequested": True,
                }
            ],
        },
    ]


def test_graphql_request_discovery_projections_returns_grouped_follow_up_pivots() -> None:
    media_service = FakeMediaService(
        request_projection_group_results=[
            SimpleNamespace(
                group_id="people",
                title="People around this window",
                description="Pivot through cast, creators, and directors tied to the current discovery window.",
                projection_type="person",
                items=[
                    SimpleNamespace(
                        projection_id="person:31",
                        label="Keanu Reeves",
                        projection_type="person",
                        match_count=2,
                        image_path="/keanu.jpg",
                        sample_titles=("The Matrix", "The Matrix Reloaded"),
                        local_match_count=2,
                        requested_match_count=2,
                        active_match_count=1,
                        completed_match_count=1,
                        preview_signals=(
                            "2 local matches",
                            "2 requested locally",
                            "1 resume path",
                            "1 completed locally",
                        ),
                        action=SimpleNamespace(
                            kind="query",
                            value="Keanu Reeves",
                            media_type="movie",
                        ),
                    )
                ],
            ),
            SimpleNamespace(
                group_id="companies",
                title="Companies in this window",
                description="Follow production-company clusters without dropping out of the current discover flow.",
                projection_type="company",
                items=[
                    SimpleNamespace(
                        projection_id="company:9993",
                        label="Warner Bros. Pictures",
                        projection_type="company",
                        match_count=2,
                        image_path="/wb.jpg",
                        sample_titles=("The Matrix", "The Matrix Reloaded"),
                        local_match_count=2,
                        requested_match_count=2,
                        active_match_count=1,
                        completed_match_count=1,
                        preview_signals=(
                            "2 local matches",
                            "2 requested locally",
                            "1 resume path",
                            "1 completed locally",
                        ),
                        action=SimpleNamespace(
                            kind="company",
                            value="9993",
                            media_type="movie",
                        ),
                    )
                ],
            ),
        ]
    )
    client = _build_client(media_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query ConsumerRequestDiscoveryProjections($company: String, $sort: String, $limitPerGroup: Int!) {
                  requestDiscoveryProjections(company: $company, sort: $sort, limitPerGroup: $limitPerGroup) {
                    groupId
                    title
                    description
                    projectionType
                    items {
                      projectionId
                      label
                      projectionType
                      matchCount
                      imagePath
                      sampleTitles
                      localMatchCount
                      requestedMatchCount
                      activeMatchCount
                      completedMatchCount
                      previewSignals
                      action {
                        kind
                        value
                        mediaType
                      }
                    }
                  }
                }
            """,
            "variables": {
                "company": "9993",
                "sort": "rating",
                "limitPerGroup": 4,
            },
        },
    )

    assert response.status_code == 200
    assert media_service.request_projection_group_calls == [
        {
            "media_type": None,
            "genre": None,
            "release_year": None,
            "original_language": None,
            "company": "9993",
            "network": None,
            "sort": "rating",
            "limit_per_group": 4,
            "tenant_id": "global",
        }
    ]
    assert response.json()["data"]["requestDiscoveryProjections"] == [
        {
            "groupId": "people",
            "title": "People around this window",
            "description": "Pivot through cast, creators, and directors tied to the current discovery window.",
            "projectionType": "person",
            "items": [
                {
                    "projectionId": "person:31",
                    "label": "Keanu Reeves",
                    "projectionType": "person",
                    "matchCount": 2,
                    "imagePath": "/keanu.jpg",
                    "sampleTitles": ["The Matrix", "The Matrix Reloaded"],
                    "localMatchCount": 2,
                    "requestedMatchCount": 2,
                    "activeMatchCount": 1,
                    "completedMatchCount": 1,
                    "previewSignals": [
                        "2 local matches",
                        "2 requested locally",
                        "1 resume path",
                        "1 completed locally",
                    ],
                    "action": {
                        "kind": "query",
                        "value": "Keanu Reeves",
                        "mediaType": "movie",
                    },
                }
            ],
        },
        {
            "groupId": "companies",
            "title": "Companies in this window",
            "description": "Follow production-company clusters without dropping out of the current discover flow.",
            "projectionType": "company",
            "items": [
                {
                    "projectionId": "company:9993",
                    "label": "Warner Bros. Pictures",
                    "projectionType": "company",
                    "matchCount": 2,
                    "imagePath": "/wb.jpg",
                    "sampleTitles": ["The Matrix", "The Matrix Reloaded"],
                    "localMatchCount": 2,
                    "requestedMatchCount": 2,
                    "activeMatchCount": 1,
                    "completedMatchCount": 1,
                    "previewSignals": [
                        "2 local matches",
                        "2 requested locally",
                        "1 resume path",
                        "1 completed locally",
                    ],
                    "action": {
                        "kind": "company",
                        "value": "9993",
                        "mediaType": "movie",
                    },
                }
            ],
        },
    ]


def test_graphql_request_discovery_page_returns_filters_facets_and_page_metadata() -> None:
    media_service = FakeMediaService(
        request_discovery_page_result=SimpleNamespace(
            items=[
                SimpleNamespace(
                    external_ref="tmdb:603",
                    title="The Matrix",
                    media_type="movie",
                    tmdb_id="603",
                    tvdb_id=None,
                    imdb_id="tt0133093",
                    poster_path="/matrix.jpg",
                    overview="Wake up, Neo.",
                    year=1999,
                    is_requested=False,
                    requested_item_id=None,
                    requested_state=None,
                    requested_seasons=None,
                    requested_episodes=None,
                    request_source=None,
                    request_count=0,
                    first_requested_at=None,
                    last_requested_at=None,
                    lifecycle=None,
                )
            ],
            offset=20,
            limit=1,
            total_count=48,
            has_previous_page=True,
            has_next_page=True,
            result_window_complete=False,
            facets=SimpleNamespace(
                genres=[
                    SimpleNamespace(
                        value="Science Fiction",
                        label="Science Fiction",
                        count=14,
                        selected=True,
                    )
                ],
                release_years=[
                    SimpleNamespace(
                        value="1999",
                        label="1999",
                        count=3,
                        selected=True,
                    )
                ],
                languages=[
                    SimpleNamespace(
                        value="en",
                        label="EN",
                        count=21,
                        selected=True,
                    )
                ],
                companies=[
                    SimpleNamespace(
                        value="4",
                        label="Paramount Pictures",
                        count=9,
                        selected=True,
                    )
                ],
                networks=[
                    SimpleNamespace(
                        value="49",
                        label="HBO",
                        count=4,
                        selected=False,
                    )
                ],
                sorts=[
                    SimpleNamespace(
                        value="popular",
                        label="Popular",
                        selected=False,
                    ),
                    SimpleNamespace(
                        value="rating",
                        label="Top rated",
                        selected=True,
                    ),
                ],
            ),
        )
    )
    client = _build_client(media_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query ConsumerRequestDiscoveryPage(
                  $offset: Int!
                  $limit: Int!
                  $genre: String!
                  $releaseYear: Int!
                  $originalLanguage: String!
                  $company: String!
                  $sort: String!
                ) {
                  requestDiscoveryPage(
                    mediaType: "movie"
                    offset: $offset
                    limit: $limit
                    genre: $genre
                    releaseYear: $releaseYear
                    originalLanguage: $originalLanguage
                    company: $company
                    sort: $sort
                  ) {
                    offset
                    limit
                    totalCount
                    hasPreviousPage
                    hasNextPage
                    resultWindowComplete
                    items {
                      externalRef
                      title
                      mediaType
                      mediaKind
                    }
                    facets {
                      genres {
                        value
                        label
                        count
                        selected
                      }
                      releaseYears {
                        value
                        label
                        count
                        selected
                      }
                      languages {
                        value
                        label
                        count
                        selected
                      }
                      companies {
                        value
                        label
                        count
                        selected
                      }
                      networks {
                        value
                        label
                        count
                        selected
                      }
                      sorts {
                        value
                        label
                        selected
                      }
                    }
                  }
                }
            """,
            "variables": {
                "offset": 20,
                "limit": 1,
                "genre": "Science Fiction",
                "releaseYear": 1999,
                "originalLanguage": "en",
                "company": "4",
                "sort": "rating",
            },
        },
    )

    assert response.status_code == 200
    assert media_service.request_discovery_page_calls == [
        {
            "media_type": "movie",
            "genre": "Science Fiction",
            "release_year": 1999,
            "original_language": "en",
            "company": "4",
            "network": None,
            "sort": "rating",
            "limit": 1,
            "offset": 20,
            "tenant_id": "global",
        }
    ]
    assert response.json()["data"]["requestDiscoveryPage"] == {
        "offset": 20,
        "limit": 1,
        "totalCount": 48,
        "hasPreviousPage": True,
        "hasNextPage": True,
        "resultWindowComplete": False,
        "items": [
            {
                "externalRef": "tmdb:603",
                "title": "The Matrix",
                "mediaType": "movie",
                "mediaKind": "MOVIE",
            }
        ],
        "facets": {
            "genres": [
                {
                    "value": "Science Fiction",
                    "label": "Science Fiction",
                    "count": 14,
                    "selected": True,
                }
            ],
            "releaseYears": [
                {
                    "value": "1999",
                    "label": "1999",
                    "count": 3,
                    "selected": True,
                }
            ],
            "languages": [
                {
                    "value": "en",
                    "label": "EN",
                    "count": 21,
                    "selected": True,
                }
            ],
            "companies": [
                {
                    "value": "4",
                    "label": "Paramount Pictures",
                    "count": 9,
                    "selected": True,
                }
            ],
            "networks": [
                {
                    "value": "49",
                    "label": "HBO",
                    "count": 4,
                    "selected": False,
                }
            ],
            "sorts": [
                {
                    "value": "popular",
                    "label": "Popular",
                    "selected": False,
                },
                {
                    "value": "rating",
                    "label": "Top rated",
                    "selected": True,
                },
            ],
        },
    }


def test_graphql_request_search_page_returns_ranked_page_metadata() -> None:
    media_service = FakeMediaService(
        request_search_page_result=SimpleNamespace(
            items=[
                SimpleNamespace(
                    external_ref="tmdb:603",
                    title="The Matrix",
                    media_type="movie",
                    tmdb_id="603",
                    tvdb_id=None,
                    imdb_id="tt0133093",
                    poster_path="/matrix.jpg",
                    overview="Wake up, Neo.",
                    year=1999,
                    is_requested=True,
                    requested_item_id="item-603",
                    requested_state="requested",
                    requested_seasons=None,
                    requested_episodes=None,
                    request_source="webhook:overseerr",
                    request_count=3,
                    first_requested_at="2026-04-17T08:00:00Z",
                    last_requested_at="2026-04-18T09:30:00Z",
                    lifecycle=None,
                    ranking_signals=("Requested 3x", "Resume activity"),
                ),
                SimpleNamespace(
                    external_ref="tmdb:604",
                    title="The Matrix Reloaded",
                    media_type="movie",
                    tmdb_id="604",
                    tvdb_id=None,
                    imdb_id="tt0234215",
                    poster_path="/matrix-reloaded.jpg",
                    overview="Reloaded.",
                    year=2003,
                    is_requested=False,
                    requested_item_id=None,
                    requested_state=None,
                    requested_seasons=None,
                    requested_episodes=None,
                    request_source=None,
                    request_count=0,
                    first_requested_at=None,
                    last_requested_at=None,
                    lifecycle=None,
                    ranking_signals=(),
                ),
            ],
            offset=20,
            limit=2,
            total_count=46,
            has_previous_page=True,
            has_next_page=True,
            result_window_complete=False,
        )
    )
    client = _build_client(media_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query ConsumerRequestSearchPage($query: String!, $limit: Int!, $offset: Int!) {
                  requestSearchPage(query: $query, limit: $limit, offset: $offset) {
                    offset
                    limit
                    totalCount
                    hasPreviousPage
                    hasNextPage
                    resultWindowComplete
                    items {
                      externalRef
                      title
                      mediaType
                      mediaKind
                      isRequested
                      rankingSignals
                    }
                  }
                }
            """,
            "variables": {"query": "matrix", "limit": 2, "offset": 20},
        },
    )

    assert response.status_code == 200
    assert media_service.request_search_page_calls == [
        {
            "query": "matrix",
            "media_type": None,
            "limit": 2,
            "offset": 20,
            "tenant_id": "global",
        }
    ]
    assert response.json()["data"]["requestSearchPage"] == {
        "offset": 20,
        "limit": 2,
        "totalCount": 46,
        "hasPreviousPage": True,
        "hasNextPage": True,
        "resultWindowComplete": False,
        "items": [
            {
                "externalRef": "tmdb:603",
                "title": "The Matrix",
                "mediaType": "movie",
                "mediaKind": "MOVIE",
                "isRequested": True,
                "rankingSignals": ["Requested 3x", "Resume activity"],
            },
            {
                "externalRef": "tmdb:604",
                "title": "The Matrix Reloaded",
                "mediaType": "movie",
                "mediaKind": "MOVIE",
                "isRequested": False,
                "rankingSignals": [],
            },
        ],
    }


def test_graphql_request_candidate_returns_show_season_preview() -> None:
    media_service = FakeMediaService(
        request_candidate_result=RequestSearchCandidateRecord(
            external_ref="tmdb:1399",
            title="Game of Thrones",
            media_type="show",
            tmdb_id="1399",
            poster_path="/got.jpg",
            overview="Seven kingdoms compete for the throne.",
            year=2011,
            is_requested=True,
            requested_item_id="item-1399",
            requested_state="requested",
            requested_seasons=[1, 2],
            requested_episodes={"3": [1, 2, 3]},
            request_source="webhook:overseerr",
            request_count=4,
            first_requested_at="2026-04-15T08:00:00Z",
            last_requested_at="2026-04-18T09:30:00Z",
            lifecycle=RequestSearchLifecycleRecord(
                stage_name="download",
                stage_status="queued",
                provider="real_debrid",
                provider_download_id="rd-123",
            ),
            ranking_signals=("Requested 4x",),
            season_summary=RequestCandidateSeasonSummaryRecord(
                total_seasons=4,
                released_seasons=3,
                requested_seasons=3,
                partial_seasons=1,
                local_seasons=1,
                unreleased_seasons=1,
                next_air_date="2026-05-01T00:00:00+00:00",
            ),
            season_preview=(
                RequestCandidateSeasonRecord(
                    season_number=1,
                    title="Season 1",
                    episode_count=10,
                    air_date="2011-04-17",
                    is_released=True,
                    has_local_coverage=True,
                    is_requested=True,
                    requested_episode_count=10,
                    requested_all_episodes=True,
                    status="local",
                ),
                RequestCandidateSeasonRecord(
                    season_number=3,
                    title="Season 3",
                    episode_count=10,
                    air_date="2013-03-31",
                    is_released=True,
                    has_local_coverage=False,
                    is_requested=True,
                    requested_episode_count=3,
                    requested_all_episodes=False,
                    status="partial",
                ),
                RequestCandidateSeasonRecord(
                    season_number=4,
                    title="Season 4",
                    episode_count=8,
                    air_date="2026-05-01",
                    is_released=False,
                    has_local_coverage=False,
                    is_requested=False,
                    requested_episode_count=0,
                    requested_all_episodes=False,
                    status="upcoming",
                ),
            ),
        )
    )
    client = _build_client(media_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query ConsumerRequestCandidate($externalRef: String!, $mediaType: String!) {
                  requestCandidate(externalRef: $externalRef, mediaType: $mediaType) {
                    externalRef
                    title
                    mediaType
                    seasonSummary {
                      totalSeasons
                      partialSeasons
                      localSeasons
                      nextAirDate
                    }
                    seasonPreview {
                      seasonNumber
                      title
                      status
                      hasLocalCoverage
                      isRequested
                      requestedEpisodeCount
                      requestedAllEpisodes
                    }
                  }
                }
            """,
            "variables": {"externalRef": "tmdb:1399", "mediaType": "show"},
        },
    )

    assert response.status_code == 200
    assert media_service.request_candidate_calls == [
        {
            "external_ref": "tmdb:1399",
            "media_type": "show",
            "tenant_id": "global",
        }
    ]
    assert response.json()["data"]["requestCandidate"] == {
        "externalRef": "tmdb:1399",
        "title": "Game of Thrones",
        "mediaType": "show",
        "seasonSummary": {
            "totalSeasons": 4,
            "partialSeasons": 1,
            "localSeasons": 1,
            "nextAirDate": "2026-05-01T00:00:00+00:00",
        },
        "seasonPreview": [
            {
                "seasonNumber": 1,
                "title": "Season 1",
                "status": "local",
                "hasLocalCoverage": True,
                "isRequested": True,
                "requestedEpisodeCount": 10,
                "requestedAllEpisodes": True,
            },
            {
                "seasonNumber": 3,
                "title": "Season 3",
                "status": "partial",
                "hasLocalCoverage": False,
                "isRequested": True,
                "requestedEpisodeCount": 3,
                "requestedAllEpisodes": False,
            },
            {
                "seasonNumber": 4,
                "title": "Season 4",
                "status": "upcoming",
                "hasLocalCoverage": False,
                "isRequested": False,
                "requestedEpisodeCount": 0,
                "requestedAllEpisodes": False,
            },
        ],
    }


def test_graphql_request_history_page_returns_persisted_request_window() -> None:
    media_service = FakeMediaService(
        request_history_page_result=RequestSearchPageRecord(
            items=[
                RequestSearchCandidateRecord(
                    external_ref="tmdb:1399",
                    title="Game of Thrones",
                    media_type="show",
                    tmdb_id="1399",
                    poster_path="/got.jpg",
                    overview="Seven kingdoms compete for the throne.",
                    year=2011,
                    is_requested=True,
                    requested_item_id="item-1399",
                    requested_state="requested",
                    requested_seasons=[1, 2],
                    requested_episodes={"3": [1, 2]},
                    request_source="webhook:overseerr",
                    request_count=4,
                    first_requested_at="2026-04-15T08:00:00Z",
                    last_requested_at="2026-04-18T09:30:00Z",
                    ranking_signals=("Requested 4x",),
                ),
                RequestSearchCandidateRecord(
                    external_ref="tmdb:680",
                    title="Pulp Fiction",
                    media_type="movie",
                    tmdb_id="680",
                    poster_path="/pulp-fiction.jpg",
                    overview="The lives of two mob hitmen intertwine.",
                    year=1994,
                    is_requested=True,
                    requested_item_id="item-680",
                    requested_state="requested",
                    request_source="graphql",
                    request_count=1,
                    first_requested_at="2026-04-14T08:00:00Z",
                    last_requested_at="2026-04-17T11:00:00Z",
                ),
            ],
            offset=2,
            limit=2,
            total_count=7,
            has_previous_page=True,
            has_next_page=True,
            result_window_complete=True,
        )
    )
    client = _build_client(media_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query ConsumerRequestHistoryPage($limit: Int!, $offset: Int!) {
                  requestHistoryPage(limit: $limit, offset: $offset) {
                    offset
                    limit
                    totalCount
                    hasPreviousPage
                    hasNextPage
                    items {
                      externalRef
                      title
                      mediaType
                      isRequested
                      requestCount
                    }
                  }
                }
            """,
            "variables": {"limit": 2, "offset": 2},
        },
    )

    assert response.status_code == 200
    assert media_service.request_history_page_calls == [
        {
            "media_type": None,
            "limit": 2,
            "offset": 2,
            "tenant_id": "global",
        }
    ]
    assert response.json()["data"]["requestHistoryPage"] == {
        "offset": 2,
        "limit": 2,
        "totalCount": 7,
        "hasPreviousPage": True,
        "hasNextPage": True,
        "items": [
            {
                "externalRef": "tmdb:1399",
                "title": "Game of Thrones",
                "mediaType": "show",
                "isRequested": True,
                "requestCount": 4,
            },
            {
                "externalRef": "tmdb:680",
                "title": "Pulp Fiction",
                "mediaType": "movie",
                "isRequested": True,
                "requestCount": 1,
            },
        ],
    }

def test_graphql_media_items_page_accepts_native_playback_filters_and_page_info() -> None:
    @dataclass
    class _Page:
        items: list[MediaItemSummaryRecord]
        total_items: int
        limit: int

    item = MediaItemSummaryRecord(
        id="item-1",
        type="movie",
        title="Harbor Watch",
        state="failed",
        created_at="2026-04-18T08:00:00Z",
        updated_at="2026-04-18T10:00:00Z",
        playback_attachments=[
            PlaybackAttachmentDetailRecord(
                id="attachment-1",
                kind="direct",
                locator="https://cdn.example.com/direct",
                provider="realdebrid",
                refresh_state="failed",
                last_refresh_error="provider_timeout",
            )
        ],
        resolved_playback=ResolvedPlaybackSnapshotRecord(
            direct=None,
            hls=ResolvedPlaybackAttachmentRecord(
                kind="hls",
                locator="https://cdn.example.com/hls",
                source_key="persisted",
                provider="realdebrid",
            ),
            direct_ready=False,
            hls_ready=True,
            missing_local_file=False,
        ),
        active_stream=ActiveStreamDetailRecord(
            direct_ready=False,
            hls_ready=True,
            missing_local_file=False,
        ),
        media_entries=[
            MediaEntryDetailRecord(
                provider="realdebrid",
                refresh_state="failed",
                last_refresh_error="provider_timeout",
                active_for_hls=True,
            )
        ],
    )
    media_service = FakeMediaService(
        detail_page=_Page(
            items=[item],
            total_items=1,
            limit=12,
        ),
        recovery_plan=RecoveryPlanRecord(
            mechanism=RecoveryMechanism.ORPHAN_RECOVERY,
            target_stage=RecoveryTargetStage.FINALIZE,
            reason="orphaned_downloaded_item",
            next_retry_at=None,
            recovery_attempt_count=2,
            is_in_cooldown=False,
        ),
    )
    client = _build_client(media_service)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query PlaybackRecoveryPage(
                  $state: String
                  $query: String
                  $provider: String
                  $attachmentState: String
                  $stream: String
                  $hasErrors: Boolean
                  $sort: String
                  $limit: Int!
                  $offset: Int!
                ) {
                  mediaItemsPage(
                    state: $state
                    query: $query
                    provider: $provider
                    attachmentState: $attachmentState
                    stream: $stream
                    hasErrors: $hasErrors
                    sort: $sort
                    limit: $limit
                    offset: $offset
                  ) {
                    totalCount
                    limit
                    offset
                    hasPreviousPage
                    hasNextPage
                    items {
                      id
                      title
                      state
                    }
                  }
                }
            """,
            "variables": {
                "state": "failed",
                "query": "harbor",
                "provider": "realdebrid",
                "attachmentState": "failed",
                "stream": "hls_ready",
                "hasErrors": True,
                "sort": "updated_desc",
                "limit": 12,
                "offset": 0,
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["mediaItemsPage"] == {
        "totalCount": 1,
        "limit": 12,
        "offset": 0,
        "hasPreviousPage": False,
        "hasNextPage": False,
        "items": [
            {
                "id": "item-1",
                "title": "Harbor Watch",
                "state": "failed",
            }
        ],
    }
    assert media_service.search_item_detail_calls == [
        {
            "limit": 12,
            "offset": 0,
            "states": ["failed"],
            "query": "harbor",
            "provider": "realdebrid",
            "attachment_state": "failed",
            "stream": "hls_ready",
            "has_errors": True,
            "sort": "updated_desc",
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


def test_graphql_run_item_workflow_drill_mutation_returns_route_parity_status(
    monkeypatch: Any,
) -> None:
    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)

    async def fake_run_worker_item_workflow_drill(request: Any) -> Any:
        _ = request
        return SimpleNamespace(
            queue_name="filmu-py",
            has_history=True,
            observed_at="2026-04-18T12:10:00+00:00",
            examined_checkpoints=1,
            replayed_checkpoints=1,
            compensated_checkpoints=0,
            finalize_requeues=1,
            parse_requeues=0,
            scrape_requeues=0,
            index_requeues=0,
            skipped_active=0,
            unrecoverable=0,
            failed=0,
            candidate_status_counts={"pending": 1},
            compensation_stage_counts={},
            outcome="ok",
            run_failed=False,
            last_error=None,
        )

    monkeypatch.setattr(
        default_routes,
        "run_worker_item_workflow_drill",
        fake_run_worker_item_workflow_drill,
    )

    response = client.post(
        "/graphql",
        headers=_graphql_headers("backend:admin"),
        json={
            "query": """
                mutation {
                  runItemWorkflowDrill {
                    queueName
                    hasHistory
                    replayedCheckpoints
                    compensatedCheckpoints
                    finalizeRequeues
                    candidateStatusCounts
                    outcome
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["runItemWorkflowDrill"] == {
        "queueName": "filmu-py",
        "hasHistory": True,
        "replayedCheckpoints": 1,
        "compensatedCheckpoints": 0,
        "finalizeRequeues": 1,
        "candidateStatusCounts": {"pending": 1},
        "outcome": "ok",
    }


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
                  rolloutEvidence {
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
                    windowsSoakPressureCauseBuckets
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
    assert payload["rolloutEvidence"]["status"] == "ready"
    assert payload["rolloutEvidence"]["totalCheckCount"] == 7
    assert payload["rolloutEvidence"]["readyCheckCount"] == 7
    checks = {row["key"]: row for row in payload["rolloutEvidence"]["checks"]}
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
        "windowsSoakPressureCauseBuckets": {},
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


def test_graphql_rollout_supporting_queries_return_typed_counts_inventory_actions_and_gaps(
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
                  rolloutStatusCounts {
                    status
                    count
                  }
                  rolloutArtifactInventory(checkKey: "observability_rollout") {
                    checkKey
                    ref
                    category
                    recorded
                  }
                  rolloutActions(domain: "playback_gate") {
                    domain
                    subject
                    action
                  }
                  rolloutGaps(domain: "vfs_runtime_rollout") {
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
    assert {"status": "ready", "count": 7} in payload["rolloutStatusCounts"]
    assert payload["rolloutArtifactInventory"] == [
        {
            "checkKey": "observability_rollout",
            "ref": "ops/observability/rollout.md",
            "category": "observability_rollout",
            "recorded": True,
        }
    ]
    assert payload["rolloutActions"] == [
        {
            "domain": "playback_gate",
            "subject": "playback_gate_governance",
            "action": "keep_required_checks_enforced",
        }
    ]
    assert payload["rolloutGaps"] == [
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
                  downloaderProviderSummaries(provider: "realdebrid", statusCode: 429) {
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


def test_graphql_access_policy_context_query_returns_current_actor_posture() -> None:
    client = _build_client(
        FakeMediaService(),
        settings_overrides={
            "FILMU_PY_OIDC": {
                "allow_api_key_fallback": False,
                "rollout_stage": "enforced",
            }
        },
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  accessPolicyContext {
                    authenticationMode
                    actorId
                    actorType
                    tenantId
                    authorizationTenantScope
                    authorizedTenantIds
                    oidcClaimsPresent
                    oidcTokenValidated
                    oidcAllowApiKeyFallback
                    oidcRolloutStage
                    accessPolicyVersion
                    quotaPolicyVersion
                    permissionsModel
                    policySource
                    auditMode
                    policyAlertingEnabled
                    repeatedDenialWarningThreshold
                    repeatedDenialCriticalThreshold
                    decisions {
                      name
                      allowed
                      requiredPermissions
                      targetTenantId
                    }
                    warnings
                    remainingGaps
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["accessPolicyContext"]
    assert payload["authenticationMode"] == "api_key"
    assert payload["actorId"] == "api-key:primary"
    assert payload["actorType"] == "service"
    assert payload["tenantId"] == "global"
    assert payload["authorizationTenantScope"] == "self"
    assert payload["authorizedTenantIds"] == ["global"]
    assert payload["oidcClaimsPresent"] is False
    assert payload["oidcTokenValidated"] is False
    assert payload["oidcAllowApiKeyFallback"] is False
    assert payload["oidcRolloutStage"] == "enforced"
    assert payload["accessPolicyVersion"] == "default-v1"
    assert payload["quotaPolicyVersion"] is None
    assert payload["permissionsModel"].startswith("role_scope_effective_permissions")
    assert payload["policySource"] == "settings"
    assert payload["auditMode"] == "structured_log_history_only"
    assert payload["policyAlertingEnabled"] is True
    assert payload["repeatedDenialWarningThreshold"] == 3
    assert payload["repeatedDenialCriticalThreshold"] == 5
    assert any(row["name"] == "library_read" for row in payload["decisions"])
    assert "authentication is still API-key anchored" in payload["warnings"]
    assert "oidc claims are not present on this request" in payload["warnings"]
    assert isinstance(payload["remainingGaps"], list)


def test_graphql_access_policy_audit_query_returns_records_and_alerts() -> None:
    class FakeAuthorizationAuditService:
        async def record_decision(self, **kwargs: Any) -> None:
            _ = kwargs

        async def search(
            self,
            *,
            limit: int,
            actor_id: str | None = None,
            tenant_id: str | None = None,
            target_tenant_id: str | None = None,
            permission: str | None = None,
            allowed: bool | None = None,
            reason: str | None = None,
            path_prefix: str | None = None,
        ) -> object:
            assert limit == 12
            assert actor_id is None
            assert tenant_id is None
            assert target_tenant_id is None
            assert permission == "security:policy.approve"
            assert allowed is False
            assert reason is None
            assert path_prefix is None
            return SimpleNamespace(
                total_matches=1,
                records=[
                    SimpleNamespace(
                        occurred_at=datetime(2026, 4, 18, 9, 30, tzinfo=UTC),
                        path="/api/v1/auth/policy/revisions",
                        method="POST",
                        resource_scope="access_policy",
                        actor_id="operator-1",
                        actor_type="human",
                        tenant_id="tenant-main",
                        target_tenant_id="tenant-main",
                        required_permissions=("security:policy.approve",),
                        matched_permissions=(),
                        missing_permissions=("security:policy.approve",),
                        constrained_permissions=(),
                        constraint_failures=("missing approval scope",),
                        allowed=False,
                        reason="missing_permissions",
                        tenant_scope="scoped",
                        authentication_mode="oidc",
                        access_policy_version="access-2026.04.18",
                        access_policy_source="runtime",
                        oidc_issuer="https://auth.example.test",
                        oidc_subject="subject-17",
                    )
                ],
            )

    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)
    resources.settings.access_policy.repeated_denial_warning_threshold = 1
    resources.settings.access_policy.repeated_denial_critical_threshold = 1
    resources.authorization_audit_service = FakeAuthorizationAuditService()

    response = client.post(
        "/graphql",
        headers=_graphql_headers("settings:write"),
        json={
            "query": """
                query AccessPolicyAudit(
                  $limit: Int!
                  $allowed: Boolean
                  $permission: String
                ) {
                  accessPolicyAudit(
                    limit: $limit
                    allowed: $allowed
                    permission: $permission
                  ) {
                    totalMatches
                    entries
                    records {
                      occurredAt
                      path
                      method
                      resourceScope
                      actorId
                      targetTenantId
                      requiredPermissions
                      missingPermissions
                      constraintFailures
                      allowed
                      reason
                      authenticationMode
                      accessPolicyVersion
                      accessPolicySource
                      oidcIssuer
                      oidcSubject
                      summary
                    }
                    alerts {
                      code
                      severity
                      count
                      message
                    }
                  }
                }
            """,
            "variables": {
                "limit": 12,
                "allowed": False,
                "permission": "security:policy.approve",
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["accessPolicyAudit"]
    assert payload["totalMatches"] == 1
    assert len(payload["entries"]) == 1
    assert payload["records"] == [
        {
            "occurredAt": "2026-04-18T09:30:00+00:00",
            "path": "/api/v1/auth/policy/revisions",
            "method": "POST",
            "resourceScope": "access_policy",
            "actorId": "operator-1",
            "targetTenantId": "tenant-main",
            "requiredPermissions": ["security:policy.approve"],
            "missingPermissions": ["security:policy.approve"],
            "constraintFailures": ["missing approval scope"],
            "allowed": False,
            "reason": "missing_permissions",
            "authenticationMode": "oidc",
            "accessPolicyVersion": "access-2026.04.18",
            "accessPolicySource": "runtime",
            "oidcIssuer": "https://auth.example.test",
            "oidcSubject": "subject-17",
            "summary": (
                "2026-04-18T09:30:00+00:00 denied POST /api/v1/auth/policy/revisions "
                "actor=operator-1 tenant=tenant-main->tenant-main "
                "reason=missing_permissions permissions=security:policy.approve"
            ),
        }
    ]
    assert payload["alerts"] == [
        {
            "code": "repeated_denials",
            "severity": "critical",
            "count": 1,
            "message": (
                "actor 'operator-1' saw 1 denied authorization decisions for "
                "/api/v1/auth/policy/revisions (missing_permissions) in the current result set"
            ),
        }
    ]


def test_graphql_tenant_quota_policy_query_returns_visible_limits() -> None:
    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    quota_constraint = resources.settings.access_policy.permission_constraints.setdefault(
        "tenant:quota.read",
        {},
    )
    route_prefixes = quota_constraint.setdefault("route_prefixes", [])
    if "/graphql" not in route_prefixes:
        route_prefixes.append("/graphql")
    resources.settings.tenant_quotas.enabled = True
    resources.settings.tenant_quotas.version = "quota-2026.04.18"
    resources.settings.tenant_quotas.default.api_requests_per_minute = 900
    resources.settings.tenant_quotas.default.worker_enqueues_per_minute = 180
    resources.settings.tenant_quotas.default.playback_refreshes_per_minute = 90
    resources.settings.tenant_quotas.default.provider_refreshes_per_minute = 60

    response = client.post(
        "/graphql",
        headers=_graphql_headers("tenant:quota.read"),
        json={
            "query": """
                query {
                  tenantQuotaPolicy {
                    tenantId
                    enabled
                    policyVersion
                    apiRequestsPerMinute
                    workerEnqueuesPerMinute
                    playbackRefreshesPerMinute
                    providerRefreshesPerMinute
                    enforcementPoints
                    remainingGaps
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["tenantQuotaPolicy"] == {
        "tenantId": "tenant-main",
        "enabled": True,
        "policyVersion": "quota-2026.04.18",
        "apiRequestsPerMinute": 900,
        "workerEnqueuesPerMinute": 180,
        "playbackRefreshesPerMinute": 90,
        "providerRefreshesPerMinute": 60,
        "enforcementPoints": [
            "api_request_intake",
            "worker_enqueue_policy",
            "provider_refresh_policy",
            "playback_refresh_policy",
        ],
        "remainingGaps": [
            "worker/provider/playback quota ceilings are visible but not yet enforced everywhere",
            "quota counters are Redis minute buckets, not long-horizon billing records",
        ],
    }


def test_graphql_write_tenant_quota_policy_mutation_persists_and_returns_policy() -> None:
    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)
    resources.settings.tenant_quotas.enabled = False
    resources.settings.tenant_quotas.version = "quota-2026.04.18"

    response = client.post(
        "/graphql",
        headers=_graphql_headers("tenant:quota.write"),
        json={
            "query": """
                mutation WriteTenantQuotaPolicy($input: TenantQuotaPolicyWriteInput!) {
                  writeTenantQuotaPolicy(input: $input) {
                    tenantId
                    enabled
                    policyVersion
                    apiRequestsPerMinute
                    workerEnqueuesPerMinute
                    playbackRefreshesPerMinute
                    providerRefreshesPerMinute
                    enforcementPoints
                    remainingGaps
                  }
                }
            """,
            "variables": {
                "input": {
                    "tenantId": "tenant-main",
                    "enabled": True,
                    "policyVersion": "quota-2026.04.19",
                    "apiRequestsPerMinute": 1200,
                    "workerEnqueuesPerMinute": 240,
                    "playbackRefreshesPerMinute": 120,
                    "providerRefreshesPerMinute": 80,
                }
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["writeTenantQuotaPolicy"] == {
        "tenantId": "tenant-main",
        "enabled": True,
        "policyVersion": "quota-2026.04.19",
        "apiRequestsPerMinute": 1200,
        "workerEnqueuesPerMinute": 240,
        "playbackRefreshesPerMinute": 120,
        "providerRefreshesPerMinute": 80,
        "enforcementPoints": [
            "api_request_intake",
            "worker_enqueue_policy",
            "provider_refresh_policy",
            "playback_refresh_policy",
        ],
        "remainingGaps": [
            "worker/provider/playback quota ceilings are visible but not yet enforced everywhere",
            "quota counters are Redis minute buckets, not long-horizon billing records",
        ],
    }
    assert resources.settings.tenant_quotas.enabled is True
    assert resources.settings.tenant_quotas.version == "quota-2026.04.19"
    persisted_limits = resources.settings.tenant_quotas.tenants["tenant-main"]
    assert persisted_limits.api_requests_per_minute == 1200
    assert persisted_limits.worker_enqueues_per_minute == 240
    assert persisted_limits.playback_refreshes_per_minute == 120
    assert persisted_limits.provider_refreshes_per_minute == 80


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


def test_graphql_access_policy_revisions_cache_hot_read_and_invalidate_on_mutation() -> None:
    from filmu_py.graphql.resolvers import CoreMutationResolver, CoreQueryResolver
    from filmu_py.graphql.types import AccessPolicyRevisionWriteInput

    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)
    access_policy_service = DummyAccessPolicyService(resources.settings)
    resources.access_policy_service = access_policy_service
    resources.access_policy_snapshot = access_policy_service.snapshot
    info = _build_graphql_info(client.app, headers=_graphql_headers("settings:write"))
    query = CoreQueryResolver()
    mutation = CoreMutationResolver()

    async def _scenario() -> None:
        hits_before = _counter_value(CACHE_HITS_TOTAL, namespace="test")
        invalidations_before = _counter_value(
            CACHE_INVALIDATIONS_TOTAL,
            namespace="test",
            reason="access_policy_mutation",
        )

        first_result = await query.access_policy_revisions(info, limit=1)
        second_result = await query.access_policy_revisions(info, limit=10)

        assert len(first_result.revisions) == 1
        assert _counter_value(CACHE_HITS_TOTAL, namespace="test") == hits_before + 1
        assert second_result.active_version == resources.settings.access_policy.version
        assert [row.version for row in second_result.revisions] == [
            resources.settings.access_policy.version
        ]

        await mutation.write_access_policy_revision(
            info,
            AccessPolicyRevisionWriteInput(
                version="2026-04-18-graphql-cache-refresh",
                source="graphql_test",
                role_grants={"platform:admin": ["settings:write"]},
                principal_roles={"tenant-main:operator-1": ["platform:admin"]},
                principal_scopes={"tenant-main:operator-1": ["settings:write"]},
                principal_tenant_grants={"tenant-main:operator-1": ["tenant-main"]},
                permission_constraints={},
            ),
        )

        assert (
            _counter_value(
                CACHE_INVALIDATIONS_TOTAL,
                namespace="test",
                reason="access_policy_mutation",
            )
            == invalidations_before + 1
        )

        third_result = await query.access_policy_revisions(info, limit=10)
        assert [row.version for row in third_result.revisions] == [
            "2026-04-18-graphql-cache-refresh",
            resources.settings.access_policy.version,
        ]

    asyncio.run(_scenario())


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


def test_graphql_plugin_governance_cache_hot_read_and_invalidate_on_override_write() -> None:
    from filmu_py.graphql.resolvers import CoreMutationResolver, CoreQueryResolver
    from filmu_py.graphql.types import PluginGovernanceOverrideWriteInput

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
    plugin_registry.register_capability(
        plugin_name="external-scraper",
        kind=PluginCapabilityKind.SCRAPER,
        implementation=object(),
    )
    client = _build_client(
        FakeMediaService(),
        plugin_registry=plugin_registry,
        settings_overrides={"FILMU_PY_PLUGIN_RUNTIME": {"enforcement_mode": "report_only"}},
    )
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)
    resources.plugin_governance_service = DummyPluginGovernanceService()
    info = _build_graphql_info(client.app, headers=_graphql_headers("settings:write"))
    query = CoreQueryResolver()
    mutation = CoreMutationResolver()

    async def _scenario() -> None:
        hits_before = _counter_value(CACHE_HITS_TOTAL, namespace="test")
        invalidations_before = _counter_value(
            CACHE_INVALIDATIONS_TOTAL,
            namespace="test",
            reason="plugin_governance_mutation",
        )

        first_governance = await query.plugin_governance(info)
        first_overrides = await query.plugin_governance_overrides(info)
        second_governance = await query.plugin_governance(info)
        second_overrides = await query.plugin_governance_overrides(info)

        assert first_governance.summary.override_count == 0
        assert first_overrides == []
        assert _counter_value(CACHE_HITS_TOTAL, namespace="test") == hits_before + 2
        assert second_governance.summary.override_count == 0
        assert second_governance.summary.quarantined_overrides == 0
        assert [(row.name, row.override_state) for row in second_governance.plugins] == [
            ("external-scraper", None)
        ]
        assert second_overrides == []

        await mutation.write_plugin_governance_override(
            info,
            "external-scraper",
            PluginGovernanceOverrideWriteInput(
                state="quarantined",
                reason="signature drift",
            ),
        )

        assert (
            _counter_value(
                CACHE_INVALIDATIONS_TOTAL,
                namespace="test",
                reason="plugin_governance_mutation",
            )
            == invalidations_before + 2
        )

        third_governance = await query.plugin_governance(info)
        third_overrides = await query.plugin_governance_overrides(info)
        assert third_governance.summary.override_count == 1
        assert third_governance.summary.quarantined_overrides == 1
        assert [(row.name, row.override_state) for row in third_governance.plugins] == [
            ("external-scraper", "quarantined")
        ]
        assert [(row.plugin_name, row.state) for row in third_overrides] == [
            ("external-scraper", "quarantined")
        ]

    asyncio.run(_scenario())


def test_graphql_plugin_integration_readiness_cache_hot_read_and_refresh_on_override_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from filmu_py.graphql.resolvers import CoreMutationResolver, CoreQueryResolver
    from filmu_py.graphql.types import PluginGovernanceOverrideWriteInput

    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)
    resources.plugin_governance_service = DummyPluginGovernanceService()
    info = _build_graphql_info(client.app, headers=_graphql_headers("settings:write"))
    query = CoreQueryResolver()
    mutation = CoreMutationResolver()
    state = {"ready": False}

    def _proof(ref: str, category: str) -> SimpleNamespace:
        return SimpleNamespace(
            ref=ref,
            category=category,
            label=f"{ref} proof",
            recorded=True,
        )

    async def fake_build_plugin_integration_readiness_posture(
        resources_arg: Any,
    ) -> SimpleNamespace:
        _ = resources_arg
        ready = state["ready"]
        plugin = SimpleNamespace(
            name="comet",
            capability_kind="scraper",
            status="ready" if ready else "blocked",
            registered=True,
            enabled=True,
            configured=True,
            ready=ready,
            endpoint="https://comet.example",
            endpoint_configured=True,
            config_source="scraping.comet",
            required_settings=[],
            missing_settings=[] if ready else ["api_key"],
            contract_proof_refs=["ops/plugins/comet-contract.md"],
            soak_proof_refs=["ops/plugins/comet-soak.md"] if ready else [],
            contract_proofs=[
                _proof("ops/plugins/comet-contract.md", "plugin_contract")
            ],
            soak_proofs=[_proof("ops/plugins/comet-soak.md", "plugin_soak")] if ready else [],
            contract_validated=True,
            soak_validated=ready,
            proof_gap_count=0 if ready else 1,
            verification_status="verified" if ready else "partial",
            verification_check_count=4,
            verified_check_count=4 if ready else 3,
            missing_verification_checks=[] if ready else ["soak_proof"],
            required_actions=[] if ready else ["capture_soak_proof"],
            remaining_gaps=[] if ready else ["missing_soak_proof"],
        )
        return SimpleNamespace(
            generated_at="2026-04-18T12:00:00Z",
            status="ready" if ready else "partial",
            plugins=[plugin],
            required_actions=[] if ready else ["capture_soak_proof"],
            remaining_gaps=[] if ready else ["missing_soak_proof"],
        )

    monkeypatch.setattr(
        "filmu_py.graphql.resolvers.build_plugin_integration_readiness_posture",
        fake_build_plugin_integration_readiness_posture,
    )

    async def _scenario() -> None:
        hits_before = _counter_value(CACHE_HITS_TOTAL, namespace="test")

        first = await query.plugin_integration_readiness(info, include_disabled=False)
        state["ready"] = True
        second = await query.plugin_integration_readiness(info, include_disabled=False)

        assert first.status == "partial"
        assert first.plugins[0].missing_settings == ["api_key"]
        assert second.status == "partial"
        assert second.plugins[0].missing_verification_checks == ["soak_proof"]
        assert _counter_value(CACHE_HITS_TOTAL, namespace="test") == hits_before + 1

        await mutation.write_plugin_governance_override(
            info,
            "comet",
            PluginGovernanceOverrideWriteInput(
                state="quarantined",
                reason="refresh GraphQL operator posture",
            ),
        )

        third = await query.plugin_integration_readiness(info, include_disabled=False)
        assert third.status == "ready"
        assert third.plugins[0].soak_validated is True
        assert third.plugins[0].missing_verification_checks == []

    asyncio.run(_scenario())


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


def test_graphql_execute_plugin_stream_control_mutation_reuses_route_contract() -> None:
    plugin_registry = PluginRegistry()

    class ExampleStreamControl:
        plugin_name = "stream-control-plugin"

        async def initialize(self, ctx: object) -> None:
            _ = ctx

        async def control(self, request: Any) -> Any:
            return SimpleNamespace(
                action=request.action,
                item_identifier=request.item_identifier,
                accepted=True,
                outcome="handled",
                detail=None,
                controller_attached=True,
                retry_after_seconds=None,
                metadata={"source": "test"},
            )

    plugin_registry.register_capability(
        plugin_name="stream-control-plugin",
        kind=PluginCapabilityKind.STREAM_CONTROL,
        implementation=ExampleStreamControl(),
    )

    client = _build_client(FakeMediaService(), plugin_registry=plugin_registry)
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)

    response = client.post(
        "/graphql",
        headers=_graphql_headers("backend:admin"),
        json={
            "query": """
                mutation ExecutePluginStreamControl($input: PluginStreamControlInput!) {
                  executePluginStreamControl(input: $input) {
                    pluginName
                    action
                    itemIdentifier
                    accepted
                    outcome
                    controllerAttached
                    metadata
                  }
                }
            """,
            "variables": {
                "input": {
                    "pluginName": "stream-control-plugin",
                    "action": "TRIGGER_DIRECT_PLAYBACK_REFRESH",
                    "itemIdentifier": "item-123",
                    "preferQueued": True,
                    "metadata": {"reason": "operator-test"},
                }
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["executePluginStreamControl"] == {
        "pluginName": "stream-control-plugin",
        "action": "TRIGGER_DIRECT_PLAYBACK_REFRESH",
        "itemIdentifier": "item-123",
        "accepted": True,
        "outcome": "handled",
        "controllerAttached": True,
        "metadata": {"source": "test"},
    }


def test_graphql_generate_api_key_mutation_rotates_runtime_key(
    monkeypatch: Any,
) -> None:
    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)

    async def fake_save_settings(db: Any, data: dict[str, Any]) -> None:
        db.settings_blob = dict(data)

    monkeypatch.setattr(default_routes, "save_settings", fake_save_settings)

    previous_key = resources.settings.api_key.get_secret_value()
    previous_key_id = resources.settings.api_key_id

    response = client.post(
        "/graphql",
        headers=_graphql_headers("security:apikey.rotate"),
        json={
            "query": """
                mutation {
                  generateApiKey {
                    key
                    apiKeyId
                    warning
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    body = response.json()["data"]["generateApiKey"]
    assert isinstance(body["key"], str)
    assert len(body["key"]) >= 32
    assert body["key"] != previous_key
    assert isinstance(body["apiKeyId"], str)
    assert body["apiKeyId"] != previous_key_id
    assert "Update BACKEND_API_KEY" in body["warning"]
    assert resources.settings.api_key.get_secret_value() == body["key"]
    assert resources.settings.api_key_id == body["apiKeyId"]
    assert resources.db.settings_blob is not None
    assert resources.db.settings_blob["api_key"] == body["key"]
    assert resources.db.settings_blob["api_key_id"] == body["apiKeyId"]


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
    expires_at = (datetime.now(UTC) + timedelta(hours=2)).isoformat().replace("+00:00", "Z")
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
    assert mutation_payload["environmentClass"] == "windows-native:managed"
    assert mutation_payload["promotionPaused"] is True
    assert mutation_payload["promotionPauseReason"] == "repeat soak after GraphQL review"
    assert mutation_payload["promotionPauseExpiresAt"] == expires_at
    assert mutation_payload["promotionPauseActive"] is True
    assert mutation_payload["rollbackRequested"] is False
    assert mutation_payload["updatedBy"] == "tenant-main:operator-1"
    assert mutation_payload["mergeGate"] == "hold"
    assert mutation_payload["canaryDecision"] == "hold_canary_and_repeat_soak"
    assert mutation_payload["history"][0] == {
        "actorId": "tenant-main:operator-1",
        "summary": "promotion pause enabled; environment updated; notes updated (windows-native:managed)",
        "promotionPauseActive": True,
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


def test_graphql_vfs_rollout_control_cache_hot_read_and_refresh_on_persist_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from filmu_py.graphql.resolvers import CoreMutationResolver, CoreQueryResolver
    from filmu_py.graphql.types import PersistVfsRolloutControlInput

    @dataclass
    class FakeRolloutSnapshot:
        generated_at: str
        environment_class: str
        runtime_status_path: str | None
        promotion_paused: bool
        promotion_pause_reason: str | None
        promotion_pause_expires_at: str | None
        promotion_pause_active: bool
        rollback_requested: bool
        rollback_reason: str | None
        rollback_expires_at: str | None
        rollback_active: bool
        notes: str | None
        updated_at: str
        updated_by: str | None
        rollout_readiness: str
        next_action: str
        canary_decision: str
        merge_gate: str
        reasons: list[str]
        allowed_actions: list[str]
        history: list[object]

        def model_dump(self) -> dict[str, object]:
            return {
                "generated_at": self.generated_at,
                "environment_class": self.environment_class,
                "runtime_status_path": self.runtime_status_path,
                "promotion_paused": self.promotion_paused,
                "promotion_pause_reason": self.promotion_pause_reason,
                "promotion_pause_expires_at": self.promotion_pause_expires_at,
                "promotion_pause_active": self.promotion_pause_active,
                "rollback_requested": self.rollback_requested,
                "rollback_reason": self.rollback_reason,
                "rollback_expires_at": self.rollback_expires_at,
                "rollback_active": self.rollback_active,
                "notes": self.notes,
                "updated_at": self.updated_at,
                "updated_by": self.updated_by,
                "rollout_readiness": self.rollout_readiness,
                "next_action": self.next_action,
                "canary_decision": self.canary_decision,
                "merge_gate": self.merge_gate,
                "reasons": list(self.reasons),
                "allowed_actions": list(self.allowed_actions),
                "history": list(self.history),
            }

    state: dict[str, object] = {
        "environment_class": "windows-native:managed",
        "runtime_status_path": None,
        "promotion_paused": False,
        "promotion_pause_reason": None,
        "promotion_pause_expires_at": None,
        "promotion_pause_active": False,
        "rollback_requested": False,
        "rollback_reason": None,
        "rollback_expires_at": None,
        "rollback_active": False,
        "notes": None,
        "updated_at": "2026-04-18T12:00:00Z",
        "updated_by": "tenant-main:operator-1",
        "rollout_readiness": "ready",
        "next_action": "promote_to_next_environment_class",
        "canary_decision": "promote_to_next_environment_class",
        "merge_gate": "ready",
        "reasons": ["no_blocking_runtime_signals"],
        "allowed_actions": ["hold", "rollback"],
        "history": [
                SimpleNamespace(
                    entry_id="ledger-initial",
                    recorded_at="2026-04-18T12:00:00Z",
                    action="initial",
                    actor_id="tenant-main:operator-1",
                    summary="initial rollout posture",
                    environment_class="windows-native:managed",
                    runtime_status_path=None,
                    promotion_paused=False,
                    promotion_pause_reason=None,
                    promotion_pause_expires_at=None,
                    promotion_pause_active=False,
                    rollback_requested=False,
                    rollback_reason=None,
                    rollback_expires_at=None,
                    rollback_active=False,
                    notes=None,
                )
            ],
    }

    def fake_rollout_snapshot() -> FakeRolloutSnapshot:
        return FakeRolloutSnapshot(
            generated_at="2026-04-18T12:00:00Z",
            environment_class=cast(str, state["environment_class"]),
            runtime_status_path=cast(str | None, state["runtime_status_path"]),
            promotion_paused=bool(state["promotion_paused"]),
            promotion_pause_reason=cast(str | None, state["promotion_pause_reason"]),
            promotion_pause_expires_at=cast(str | None, state["promotion_pause_expires_at"]),
            promotion_pause_active=bool(state["promotion_pause_active"]),
            rollback_requested=bool(state["rollback_requested"]),
            rollback_reason=cast(str | None, state["rollback_reason"]),
            rollback_expires_at=cast(str | None, state["rollback_expires_at"]),
            rollback_active=bool(state["rollback_active"]),
            notes=cast(str | None, state["notes"]),
            updated_at=cast(str, state["updated_at"]),
            updated_by=cast(str | None, state["updated_by"]),
            rollout_readiness=cast(str, state["rollout_readiness"]),
            next_action=cast(str, state["next_action"]),
            canary_decision=cast(str, state["canary_decision"]),
            merge_gate=cast(str, state["merge_gate"]),
            reasons=list(cast(list[str], state["reasons"])),
            allowed_actions=list(cast(list[str], state["allowed_actions"])),
            history=list(cast(list[object], state["history"])),
        )

    def fake_persist_managed_windows_vfs_state(
        updates: dict[str, object],
        *,
        actor_id: str,
    ) -> None:
        state.update(updates)
        state["updated_by"] = actor_id
        state["updated_at"] = "2026-04-18T12:05:00Z"
        promotion_paused = bool(state["promotion_paused"])
        state["promotion_pause_active"] = promotion_paused
        state["merge_gate"] = "hold" if promotion_paused else "ready"
        state["canary_decision"] = (
            "hold_canary_and_repeat_soak"
            if promotion_paused
            else "promote_to_next_environment_class"
        )
        state["next_action"] = cast(str, state["canary_decision"])
        state["reasons"] = (
            ["operator_requested_promotion_pause"]
            if promotion_paused
            else ["no_blocking_runtime_signals"]
        )
        state["history"] = [
                SimpleNamespace(
                    entry_id="ledger-persist",
                    recorded_at="2026-04-18T12:05:00Z",
                    action="persist",
                    actor_id=actor_id,
                    summary=(
                        "promotion pause enabled"
                    if promotion_paused
                    else "promotion pause cleared"
                    ),
                    environment_class=cast(str, state["environment_class"]),
                    runtime_status_path=None,
                    promotion_paused=promotion_paused,
                    promotion_pause_reason=cast(str | None, state["promotion_pause_reason"]),
                    promotion_pause_expires_at=cast(
                        str | None,
                        state["promotion_pause_expires_at"],
                    ),
                    promotion_pause_active=promotion_paused,
                    rollback_requested=False,
                    rollback_reason=None,
                    rollback_expires_at=None,
                    rollback_active=False,
                    notes=cast(str | None, state["notes"]),
                )
            ]

    monkeypatch.setattr(default_routes, "_vfs_rollout_control_response", fake_rollout_snapshot)
    monkeypatch.setattr(
        default_routes,
        "persist_managed_windows_vfs_state",
        fake_persist_managed_windows_vfs_state,
    )
    monkeypatch.setattr("filmu_py.graphql.resolvers.audit_action", lambda request, **kwargs: None)

    client = _build_client(FakeMediaService())
    resources = cast(Any, client.app.state.resources)
    _allow_graphql_control_plane_permissions(resources.settings)
    info = _build_graphql_info(client.app, headers=_graphql_headers("backend:admin"))
    query = CoreQueryResolver()
    mutation = CoreMutationResolver()

    async def _scenario() -> None:
        hits_before = _counter_value(CACHE_HITS_TOTAL, namespace="test")

        first = await query.vfs_rollout_control(info, history_limit=1)
        state["promotion_paused"] = True
        state["promotion_pause_active"] = True
        state["promotion_pause_reason"] = "out-of-band hold"
        state["merge_gate"] = "hold"
        second = await query.vfs_rollout_control(info, history_limit=1)

        assert first.promotion_paused is False
        assert second.promotion_paused is False
        assert _counter_value(CACHE_HITS_TOTAL, namespace="test") == hits_before + 1

        await mutation.persist_vfs_rollout_control(
            info,
            PersistVfsRolloutControlInput(
                environment_class="windows-native:managed",
                promotion_paused=True,
                promotion_pause_reason="repeat soak from GraphQL cache test",
                notes="hold",
            ),
        )

        third = await query.vfs_rollout_control(info, history_limit=1)
        assert third.promotion_paused is True
        assert third.promotion_pause_reason == "repeat soak from GraphQL cache test"
        assert third.merge_gate == "hold"
        assert third.history[0].summary == "promotion pause enabled"

    asyncio.run(_scenario())


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


def test_graphql_execute_vfs_rollout_action_matches_rest_guardrails(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts_root = tmp_path / "playback-proof-artifacts"
    windows_artifacts_root = artifacts_root / "windows-native-stack"
    windows_artifacts_root.mkdir(parents=True)
    (windows_artifacts_root / "filmuvfs-windows-state.json").write_text(
        json.dumps({"environment_class": "windows-native:managed"}),
        encoding="utf-8",
    )
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
            "vfs_runtime_rollout_readiness": "blocked",
            "vfs_runtime_rollout_next_action": "rollback_current_environment",
            "vfs_runtime_rollout_canary_decision": "rollback_current_environment",
            "vfs_runtime_rollout_merge_gate": "blocked",
            "vfs_runtime_rollout_environment_class": "windows-native:managed",
            "vfs_runtime_rollout_reasons": ["mounted_read_errors"],
        },
    )
    audit_calls: list[dict[str, Any]] = []

    def fake_audit_action(request: Any, **kwargs: Any) -> None:
        _ = request
        audit_calls.append(dict(kwargs))

    monkeypatch.setattr("filmu_py.graphql.resolvers.audit_action", fake_audit_action)
    client = _build_client(FakeMediaService())

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
                mutation Execute($input: ExecuteVfsRolloutActionInput!) {
                  executeVfsRolloutAction(input: $input) {
                    environmentClass
                    rollbackRequested
                    rollbackReason
                    canaryDecision
                    mergeGate
                    allowedActions
                    history {
                      action
                      summary
                    }
                  }
                }
            """,
            "variables": {
                "input": {
                    "action": "rollback",
                    "reason": "mounted reads regressed after GraphQL canary",
                    "targetEnvironmentClass": "windows-native:recovery",
                    "expectedCanaryDecision": "rollback_current_environment",
                    "expectedMergeGate": "blocked",
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert "errors" not in body
    assert body["data"]["executeVfsRolloutAction"] == {
        "environmentClass": "windows-native:recovery",
        "rollbackRequested": True,
        "rollbackReason": "mounted reads regressed after GraphQL canary",
        "canaryDecision": "rollback_current_environment",
        "mergeGate": "blocked",
        "allowedActions": ["clear_rollback"],
        "history": [
            {
                "action": "execute_rollback",
                "summary": (
                    "rollback executed: mounted reads regressed after GraphQL canary "
                    "(windows-native:recovery)"
                ),
            }
        ],
    }
    assert audit_calls == [
        {
            "action": "operations.vfs_rollout.execute_action",
            "target": "operations.vfs_rollout",
            "details": {
                "requested_action": "rollback",
                "reason": "mounted reads regressed after GraphQL canary",
                "target_environment_class": "windows-native:recovery",
                "expected_canary_decision": "rollback_current_environment",
                "expected_merge_gate": "blocked",
            },
        }
    ]


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
