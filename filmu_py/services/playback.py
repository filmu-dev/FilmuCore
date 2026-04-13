"""Playback attachment resolution service for HTTP routes and future VFS consumers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
from time import monotonic, perf_counter
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, runtime_checkable
from urllib.parse import quote

from fastapi import HTTPException, status
from prometheus_client import Counter, Histogram
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from filmu_py.api.playback_resolution import (
    DirectPlaybackSourceClass,
    PlaybackAttachment,
    PlaybackAttachmentKind,
    PlaybackAttachmentRefreshState,
    _is_degraded_direct_attachment,
    classify_direct_playback_source_class,
    is_hls_playlist_url,
    resolve_attachments_from_attributes,
    select_direct_playback_attachment,
    select_hls_playback_attachment,
)
from filmu_py.config import Settings, get_settings
from filmu_py.core.rate_limiter import RateLimitDecision
from filmu_py.db.models import ActiveStreamORM, MediaEntryORM, MediaItemORM, PlaybackAttachmentORM
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.playback_deferral_governance import (
    playback_refresh_deferral_governance_snapshot,
    record_direct_playback_refresh_deferral,
    record_selected_hls_refresh_deferral,
)

if TYPE_CHECKING:
    from filmu_py.resources import AppResources

_MATCH_ATTR_KEYS = ("tmdb_id", "tvdb_id", "imdb_id")
_ACTIVE_STREAM_ROLE_DIRECT = "direct"
_ACTIVE_STREAM_ROLE_HLS = "hls"
_PERSISTED_ATTACHMENT_KINDS: tuple[PlaybackAttachmentKind, ...] = (
    "local-file",
    "remote-direct",
    "remote-hls",
)
_PLAYBACK_REFRESH_RATE_LIMIT_CAPACITY = 1.0
_PLAYBACK_REFRESH_RATE_LIMIT_REFILL_PER_SECOND = 1.0
PLAYBACK_LEASE_REFRESH_FAILURES = Counter(
    "filmu_py_playback_lease_refresh_failures_total",
    "Count of persisted playback lease refresh failures by record type and reason class.",
    labelnames=("record_type", "reason"),
)
PLAYBACK_RISK_EVENTS = Counter(
    "filmu_py_playback_risk_events_total",
    "Count of playback-risk events encountered during attachment resolution.",
    labelnames=("surface", "reason"),
)
PROVIDER_CIRCUIT_OPEN_EVENTS = Counter(
    "filmu_py_playback_provider_circuit_open_total",
    "Count of provider playback circuit-breaker openings by provider.",
    labelnames=("provider",),
)
PLAYBACK_RESOLUTION_DURATION_SECONDS = Histogram(
    "filmu_py_playback_resolution_duration_seconds",
    "Time spent resolving direct and HLS playback attachments by surface and result.",
    labelnames=("surface", "result"),
)
SELECTED_HLS_REFRESH_DEFERRALS = Counter(
    "filmu_py_playback_selected_hls_refresh_deferrals_total",
    "Count of selected-HLS background refresh deferrals by trigger path and reason.",
    labelnames=("trigger", "reason"),
)
logger = logging.getLogger(__name__)
_ATTACHMENT_REFRESH_STATES: tuple[str, ...] = (
    "ready",
    "stale",
    "refreshing",
    "failed",
)
_MEDIA_ENTRY_REFRESH_STATES = _ATTACHMENT_REFRESH_STATES
_DEBRID_PROVIDER_KEYS: frozenset[str] = frozenset({"realdebrid", "alldebrid", "debridlink"})
_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD = 3
_PROVIDER_CIRCUIT_BREAKER_RESET_TIMEOUT_SECONDS = 30.0


@dataclass(frozen=True)
class PlaybackAttachmentRefreshRequest:
    """One persisted attachment refresh request emitted by the playback service."""

    attachment_id: str
    item_id: str
    kind: PlaybackAttachmentKind
    provider: str | None
    provider_download_id: str | None
    restricted_url: str | None
    unrestricted_url: str | None
    local_path: str | None
    refresh_state: str
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    original_filename: str | None = None
    file_size: int | None = None


@dataclass(frozen=True)
class PlaybackAttachmentRefreshResult:
    """One refresh outcome applied back onto a persisted playback attachment.

    Provider-backed refresh paths may also return stronger projected file identity so
    future refreshes can match provider-side files with fewer heuristics.
    """

    ok: bool
    locator: str | None = None
    restricted_url: str | None = None
    unrestricted_url: str | None = None
    expires_at: datetime | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    original_filename: str | None = None
    file_size: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class PlaybackAttachmentRefreshExecution:
    """One executed refresh attempt bound back to the persisted attachment identifier."""

    attachment_id: str
    ok: bool
    refresh_state: str
    locator: str
    error: str | None = None


@dataclass(frozen=True)
class MediaEntryLeaseRefreshRequest:
    """One persisted media-entry lease refresh request emitted by the playback service."""

    media_entry_id: str
    item_id: str
    kind: PlaybackAttachmentKind
    provider: str | None
    provider_download_id: str | None
    restricted_url: str | None
    unrestricted_url: str | None
    local_path: str | None
    refresh_state: str
    roles: tuple[str, ...] = ()
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    original_filename: str | None = None
    file_size: int | None = None


@dataclass(frozen=True)
class MediaEntryLeaseRefreshExecution:
    """One executed refresh attempt bound back to the persisted media-entry identifier."""

    media_entry_id: str
    ok: bool
    refresh_state: str
    locator: str | None
    error: str | None = None


@dataclass(frozen=True)
class PersistedMediaEntryControlMutationResult:
    """Outcome of one persisted media-entry control-plane write operation."""

    item_identifier: str
    media_entry_id: str
    item: MediaItemORM
    media_entry: MediaEntryORM
    applied_role: str | None = None


@dataclass(frozen=True)
class PersistedPlaybackAttachmentControlMutationResult:
    """Outcome of one persisted playback-attachment control-plane write operation."""

    item_identifier: str
    attachment_id: str
    item: MediaItemORM
    attachment: PlaybackAttachmentORM
    linked_media_entries: tuple[MediaEntryORM, ...] = ()


@dataclass(frozen=True)
class PlaybackAttachmentProviderUnrestrictedLink:
    """Normalized provider-client response for one unrestricted playback link."""

    download_url: str
    restricted_url: str | None = None
    expires_at: datetime | None = None


class PlaybackAttachmentProviderClient(Protocol):
    """Provider client that can unrestrict one persisted playback link."""

    async def unrestrict_link(
        self,
        link: str,
        *,
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentProviderUnrestrictedLink | None: ...


@runtime_checkable
class PlaybackAttachmentProviderDownloadClient(Protocol):
    """Provider client that can refresh a playback link from a provider download identifier."""

    async def refresh_download(
        self,
        *,
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentProviderUnrestrictedLink | None: ...


@dataclass(frozen=True)
class PlaybackAttachmentProviderFileProjection:
    """One provider-side file projection resolved from a provider download/container."""

    provider: str | None
    provider_download_id: str | None
    provider_file_id: str | None
    provider_file_path: str | None
    original_filename: str | None
    file_size: int | None
    restricted_url: str
    unrestricted_url: str | None = None


@dataclass(frozen=True)
class PlaybackResolutionSnapshot:
    """Resolved playback snapshot for read-model consumers such as item details and future VFS projections."""

    direct: PlaybackAttachment | None
    hls: PlaybackAttachment | None
    direct_ready: bool
    hls_ready: bool
    direct_lifecycle: DirectFileLinkLifecycleSnapshot | None = None
    hls_lifecycle: DirectFileLinkLifecycleSnapshot | None = None
    missing_local_file: bool = False


DirectPlaybackDecisionAction = Literal["serve", "fail"]
DirectPlaybackDecisionResult = Literal[
    "resolved",
    "failed_lease",
    "missing_local_file",
    "no_source",
]
DirectPlaybackRefreshSchedulingOutcome = Literal[
    "no_action",
    "scheduled",
    "completed",
    "run_later",
]
DirectPlaybackRefreshControlPlaneOutcome = Literal[
    "no_action",
    "scheduled",
    "already_pending",
]
AppScopedDirectPlaybackRefreshTriggerOutcome = Literal[
    "controller_unavailable",
    "triggered",
]
PlaybackRefreshDeferredReason = Literal["refresh_rate_limited", "provider_circuit_open"]
HlsFailedLeaseRefreshOutcome = Literal["no_action", "scheduled", "completed", "run_later"]
HlsFailedLeaseRefreshControlPlaneOutcome = Literal[
    "no_action",
    "scheduled",
    "already_pending",
]
AppScopedHlsFailedLeaseRefreshTriggerOutcome = Literal[
    "controller_unavailable",
    "triggered",
]
HlsRestrictedFallbackRefreshOutcome = Literal["no_action", "scheduled", "completed", "run_later"]
HlsRestrictedFallbackRefreshControlPlaneOutcome = Literal[
    "no_action",
    "scheduled",
    "already_pending",
]
AppScopedHlsRestrictedFallbackRefreshTriggerOutcome = Literal[
    "controller_unavailable",
    "triggered",
]
DirectPlaybackRefreshRecommendationReason = Literal[
    "selected_failed_lease",
    "provider_direct_stale",
    "provider_direct_refreshing",
    "provider_direct_failed",
    "provider_direct_degraded",
    "degraded_direct",
]
DirectPlaybackRefreshRecommendationTarget = Literal["media_entry", "attachment", "metadata"]
DirectFileServingTransport = Literal["local-file", "remote-proxy"]
DirectFileLinkOwnerKind = Literal["attachment", "media-entry", "metadata"]
DirectFileLinkProviderFamily = Literal["debrid", "provider", "none"]
DirectFileLinkLocatorSource = Literal["local-path", "unrestricted-url", "restricted-url", "locator"]
DirectFileLinkMatchBasis = Literal[
    "source-attachment-id",
    "provider-file-id",
    "provider-file-path",
    "local-path",
    "unrestricted-url",
    "restricted-url",
    "locator",
    "filename+size",
    "provider-download-id+filename",
    "provider-download-id+provider-file-path",
    "provider-download-id+file-size",
]


@dataclass(frozen=True)
class DirectPlaybackRefreshRecommendation:
    """Explicit internal refresh guidance for one direct-play decision."""

    reason: DirectPlaybackRefreshRecommendationReason
    target: DirectPlaybackRefreshRecommendationTarget
    target_id: str | None = None
    source_class: DirectPlaybackSourceClass | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    restricted_url: str | None = None
    refresh_state: PlaybackAttachmentRefreshState | None = None


@dataclass(frozen=True)
class DirectPlaybackDecision:
    """Explicit direct-play route policy decision above the shared resolver model."""

    action: DirectPlaybackDecisionAction
    result: DirectPlaybackDecisionResult
    attachment: PlaybackAttachment | None = None
    source_class: DirectPlaybackSourceClass | None = None
    status_code: int | None = None
    detail: str | None = None
    refresh_intent: bool = False
    refresh_recommendation: DirectPlaybackRefreshRecommendation | None = None


@dataclass(frozen=True)
class DirectPlaybackRefreshDispatch:
    """Translation of one direct-play refresh recommendation into existing refresh-request models."""

    recommendation: DirectPlaybackRefreshRecommendation
    media_entry_request: MediaEntryLeaseRefreshRequest | None = None
    attachment_request: PlaybackAttachmentRefreshRequest | None = None


@dataclass(frozen=True)
class DirectPlaybackRefreshDispatchExecution:
    """Execution result for one translated direct-play refresh dispatch."""

    recommendation: DirectPlaybackRefreshRecommendation
    media_entry_execution: MediaEntryLeaseRefreshExecution | None = None
    attachment_execution: PlaybackAttachmentRefreshExecution | None = None
    rate_limited: bool = False
    retry_after_seconds: float | None = None
    limiter_bucket_key: str | None = None
    deferred_reason: PlaybackRefreshDeferredReason | None = None


@dataclass(frozen=True)
class DirectPlaybackRefreshScheduleRequest:
    """One background-schedulable direct-play refresh request."""

    item_identifier: str
    recommendation: DirectPlaybackRefreshRecommendation
    requested_at: datetime
    not_before: datetime
    retry_after_seconds: float | None = None


@dataclass(frozen=True)
class DirectPlaybackRefreshSchedulingResult:
    """Scheduling-aware result above the direct-play one-shot execution seam."""

    outcome: DirectPlaybackRefreshSchedulingOutcome
    execution: DirectPlaybackRefreshDispatchExecution | None = None
    scheduled_request: DirectPlaybackRefreshScheduleRequest | None = None
    retry_after_seconds: float | None = None


@dataclass(frozen=True)
class DirectPlaybackRefreshControlPlaneTriggerResult:
    """Outcome of one in-process direct-play refresh trigger attempt."""

    item_identifier: str
    outcome: DirectPlaybackRefreshControlPlaneOutcome
    scheduling_result: DirectPlaybackRefreshSchedulingResult | None = None
    scheduled_request: DirectPlaybackRefreshScheduleRequest | None = None


@dataclass(frozen=True)
class AppScopedDirectPlaybackRefreshTriggerResult:
    """Outcome of one app-resource-boundary direct-play refresh trigger call."""

    item_identifier: str
    outcome: AppScopedDirectPlaybackRefreshTriggerOutcome
    controller_attached: bool
    control_plane_result: DirectPlaybackRefreshControlPlaneTriggerResult | None = None


@dataclass(frozen=True)
class DirectFileServingDescriptorProvenance:
    """Structured provenance for one direct-file serving decision."""

    source_key: str
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    original_filename: str | None = None
    refresh_state: PlaybackAttachmentRefreshState | None = None
    lifecycle: DirectFileLinkLifecycleSnapshot | None = None


@dataclass(frozen=True)
class DirectFileServingDescriptor:
    """Typed direct-file serving descriptor consumed by the HTTP compatibility route."""

    locator: str
    transport: DirectFileServingTransport
    media_type: str = "application/octet-stream"
    response_headers: dict[str, str] = field(default_factory=dict)
    provenance: DirectFileServingDescriptorProvenance | None = None


@dataclass(frozen=True)
class DirectFileLinkLifecycleSnapshot:
    """Internal lifecycle read model for one resolved direct-file link."""

    owner_kind: DirectFileLinkOwnerKind
    owner_id: str | None
    provider_family: DirectFileLinkProviderFamily
    locator_source: DirectFileLinkLocatorSource
    restricted_fallback: bool = False
    match_basis: DirectFileLinkMatchBasis | None = None
    source_attachment_id: str | None = None
    refresh_state: PlaybackAttachmentRefreshState | None = None
    expires_at: datetime | None = None
    last_refreshed_at: datetime | None = None
    last_refresh_error: str | None = None


@dataclass(frozen=True)
class DirectFileLinkResolutionProvenance:
    """Internal provenance for one direct-file link-resolution result."""

    source_key: str
    source_class: DirectPlaybackSourceClass | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    original_filename: str | None = None
    refresh_state: PlaybackAttachmentRefreshState | None = None
    refresh_intent: bool = False
    refresh_recommendation_reason: DirectPlaybackRefreshRecommendationReason | None = None
    lifecycle: DirectFileLinkLifecycleSnapshot | None = None


@dataclass(frozen=True)
class DirectFileLinkResolution:
    """Internal direct-file link-resolution result consumed by the serving-descriptor builder."""

    locator: str
    transport: DirectFileServingTransport
    provenance: DirectFileLinkResolutionProvenance
    filename: str | None = None
    file_size: int | None = None


@dataclass(frozen=True)
class SelectedHlsRefreshExecutionResult:
    """Shared internal outcome for one selected-HLS background refresh execution."""

    outcome: Literal["no_action", "completed", "run_later"]
    execution: MediaEntryLeaseRefreshExecution | None = None
    retry_after_seconds: float | None = None
    limiter_bucket_key: str | None = None
    deferred_reason: PlaybackRefreshDeferredReason | None = None


@dataclass(frozen=True)
class HlsFailedLeaseRefreshResult:
    """Execution result for one background-triggered selected HLS failed-lease refresh."""

    item_identifier: str
    outcome: HlsFailedLeaseRefreshOutcome
    execution: MediaEntryLeaseRefreshExecution | None = None
    retry_after_seconds: float | None = None
    limiter_bucket_key: str | None = None
    deferred_reason: PlaybackRefreshDeferredReason | None = None


@dataclass(frozen=True)
class HlsFailedLeaseRefreshControlPlaneTriggerResult:
    """Outcome of one in-process selected-HLS failed-lease refresh trigger attempt."""

    item_identifier: str
    outcome: HlsFailedLeaseRefreshControlPlaneOutcome
    refresh_result: HlsFailedLeaseRefreshResult | None = None


@dataclass(frozen=True)
class AppScopedHlsFailedLeaseRefreshTriggerResult:
    """Outcome of one app-resource-boundary selected-HLS failed-lease refresh trigger call."""

    item_identifier: str
    outcome: AppScopedHlsFailedLeaseRefreshTriggerOutcome
    controller_attached: bool
    control_plane_result: HlsFailedLeaseRefreshControlPlaneTriggerResult | None = None


@dataclass(frozen=True)
class HlsRestrictedFallbackRefreshResult:
    """Execution result for one background-triggered selected-HLS restricted-fallback refresh."""

    item_identifier: str
    outcome: HlsRestrictedFallbackRefreshOutcome
    execution: MediaEntryLeaseRefreshExecution | None = None
    retry_after_seconds: float | None = None
    limiter_bucket_key: str | None = None
    deferred_reason: PlaybackRefreshDeferredReason | None = None


@dataclass(frozen=True)
class HlsRestrictedFallbackRefreshControlPlaneTriggerResult:
    """Outcome of one in-process selected-HLS restricted-fallback refresh trigger attempt."""

    item_identifier: str
    outcome: HlsRestrictedFallbackRefreshControlPlaneOutcome
    refresh_result: HlsRestrictedFallbackRefreshResult | None = None


@dataclass(frozen=True)
class AppScopedHlsRestrictedFallbackRefreshTriggerResult:
    """Outcome of one app-resource-boundary selected-HLS restricted-fallback refresh trigger call."""

    item_identifier: str
    outcome: AppScopedHlsRestrictedFallbackRefreshTriggerOutcome
    controller_attached: bool
    control_plane_result: HlsRestrictedFallbackRefreshControlPlaneTriggerResult | None = None


class QueuedDirectPlaybackRefreshController:
    """Queue-backed dispatcher for direct-play refresh work."""

    def __init__(self, arq_redis: object, *, queue_name: str) -> None:
        self._arq_redis = arq_redis
        self._queue_name = queue_name
        self._last_results_by_item_identifier: dict[str, DirectPlaybackRefreshSchedulingResult] = {}
        self._pending_deadline_by_item_identifier: dict[str, float] = {}

    @staticmethod
    def _pending_deadline_seconds() -> float:
        """Return the bounded duplicate-suppression window for queued work."""

        return 300.0

    def has_pending(self, item_identifier: str) -> bool:
        deadline = self._pending_deadline_by_item_identifier.get(item_identifier)
        if deadline is None:
            return False
        if monotonic() >= deadline:
            self._pending_deadline_by_item_identifier.pop(item_identifier, None)
            return False
        return True

    def get_last_result(self, item_identifier: str) -> DirectPlaybackRefreshSchedulingResult | None:
        return self._last_results_by_item_identifier.get(item_identifier)

    async def shutdown(self) -> None:
        self._pending_deadline_by_item_identifier.clear()
        return None

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> DirectPlaybackRefreshControlPlaneTriggerResult:
        _ = at
        from filmu_py.workers.tasks import enqueue_refresh_direct_playback_link

        enqueued = await enqueue_refresh_direct_playback_link(
            cast(Any, self._arq_redis),
            item_id=item_identifier,
            queue_name=self._queue_name,
        )
        existing_result = self._last_results_by_item_identifier.get(item_identifier)
        still_pending = self.has_pending(item_identifier) and existing_result is not None
        if enqueued:
            scheduling_result = DirectPlaybackRefreshSchedulingResult(outcome="scheduled")
        elif still_pending:
            assert existing_result is not None
            scheduling_result = existing_result
        else:
            scheduling_result = DirectPlaybackRefreshSchedulingResult(outcome="no_action")
        self._last_results_by_item_identifier[item_identifier] = scheduling_result
        if enqueued:
            self._pending_deadline_by_item_identifier[item_identifier] = (
                monotonic() + self._pending_deadline_seconds()
            )
        return DirectPlaybackRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled" if enqueued else ("already_pending" if still_pending else "no_action"),
            scheduling_result=scheduling_result,
        )


class QueuedHlsFailedLeaseRefreshController:
    """Queue-backed dispatcher for selected-HLS failed-lease refresh work."""

    def __init__(self, arq_redis: object, *, queue_name: str) -> None:
        self._arq_redis = arq_redis
        self._queue_name = queue_name
        self._last_results_by_item_identifier: dict[str, HlsFailedLeaseRefreshResult] = {}
        self._pending_deadline_by_item_identifier: dict[str, float] = {}

    @staticmethod
    def _pending_deadline_seconds() -> float:
        """Return the bounded duplicate-suppression window for queued work."""

        return 300.0

    def has_pending(self, item_identifier: str) -> bool:
        deadline = self._pending_deadline_by_item_identifier.get(item_identifier)
        if deadline is None:
            return False
        if monotonic() >= deadline:
            self._pending_deadline_by_item_identifier.pop(item_identifier, None)
            return False
        return True

    def get_last_result(self, item_identifier: str) -> HlsFailedLeaseRefreshResult | None:
        return self._last_results_by_item_identifier.get(item_identifier)

    async def shutdown(self) -> None:
        self._pending_deadline_by_item_identifier.clear()
        return None

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> HlsFailedLeaseRefreshControlPlaneTriggerResult:
        _ = at
        from filmu_py.workers.tasks import enqueue_refresh_selected_hls_failed_lease

        enqueued = await enqueue_refresh_selected_hls_failed_lease(
            cast(Any, self._arq_redis),
            item_id=item_identifier,
            queue_name=self._queue_name,
        )
        existing_result = self._last_results_by_item_identifier.get(item_identifier)
        still_pending = self.has_pending(item_identifier) and existing_result is not None
        if enqueued:
            refresh_result = HlsFailedLeaseRefreshResult(
                item_identifier=item_identifier,
                outcome="scheduled",
            )
        elif still_pending:
            assert existing_result is not None
            refresh_result = existing_result
        else:
            refresh_result = HlsFailedLeaseRefreshResult(
                item_identifier=item_identifier,
                outcome="no_action",
            )
        self._last_results_by_item_identifier[item_identifier] = refresh_result
        if enqueued:
            self._pending_deadline_by_item_identifier[item_identifier] = (
                monotonic() + self._pending_deadline_seconds()
            )
        return HlsFailedLeaseRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled" if enqueued else ("already_pending" if still_pending else "no_action"),
            refresh_result=refresh_result,
        )


class QueuedHlsRestrictedFallbackRefreshController:
    """Queue-backed dispatcher for selected-HLS restricted-fallback refresh work."""

    def __init__(self, arq_redis: object, *, queue_name: str) -> None:
        self._arq_redis = arq_redis
        self._queue_name = queue_name
        self._last_results_by_item_identifier: dict[str, HlsRestrictedFallbackRefreshResult] = {}
        self._pending_deadline_by_item_identifier: dict[str, float] = {}

    @staticmethod
    def _pending_deadline_seconds() -> float:
        """Return the bounded duplicate-suppression window for queued work."""

        return 300.0

    def has_pending(self, item_identifier: str) -> bool:
        deadline = self._pending_deadline_by_item_identifier.get(item_identifier)
        if deadline is None:
            return False
        if monotonic() >= deadline:
            self._pending_deadline_by_item_identifier.pop(item_identifier, None)
            return False
        return True

    def get_last_result(
        self,
        item_identifier: str,
    ) -> HlsRestrictedFallbackRefreshResult | None:
        return self._last_results_by_item_identifier.get(item_identifier)

    async def shutdown(self) -> None:
        self._pending_deadline_by_item_identifier.clear()
        return None

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> HlsRestrictedFallbackRefreshControlPlaneTriggerResult:
        _ = at
        from filmu_py.workers.tasks import enqueue_refresh_selected_hls_restricted_fallback

        enqueued = await enqueue_refresh_selected_hls_restricted_fallback(
            cast(Any, self._arq_redis),
            item_id=item_identifier,
            queue_name=self._queue_name,
        )
        existing_result = self._last_results_by_item_identifier.get(item_identifier)
        still_pending = self.has_pending(item_identifier) and existing_result is not None
        if enqueued:
            refresh_result = HlsRestrictedFallbackRefreshResult(
                item_identifier=item_identifier,
                outcome="scheduled",
            )
        elif still_pending:
            assert existing_result is not None
            refresh_result = existing_result
        else:
            refresh_result = HlsRestrictedFallbackRefreshResult(
                item_identifier=item_identifier,
                outcome="no_action",
            )
        self._last_results_by_item_identifier[item_identifier] = refresh_result
        if enqueued:
            self._pending_deadline_by_item_identifier[item_identifier] = (
                monotonic() + self._pending_deadline_seconds()
            )
        return HlsRestrictedFallbackRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled" if enqueued else ("already_pending" if still_pending else "no_action"),
            refresh_result=refresh_result,
        )


@runtime_checkable
class DirectPlaybackRefreshScheduler(Protocol):
    """Minimal background scheduler contract for direct-play refresh requests."""

    async def schedule(self, request: DirectPlaybackRefreshScheduleRequest) -> None: ...


@runtime_checkable
class PlaybackRefreshRateLimiter(Protocol):
    """Minimal limiter contract for provider-backed playback refresh execution."""

    async def acquire(
        self,
        bucket_key: str,
        capacity: float,
        refill_rate_per_second: float,
        requested_tokens: float = 1.0,
        now_seconds: float | None = None,
        expiry_seconds: int | None = None,
    ) -> RateLimitDecision: ...


@dataclass(slots=True)
class ProviderCircuitState:
    """Mutable per-provider circuit-breaker state for playback link refreshes."""

    consecutive_failures: int = 0
    opened_until_monotonic: float | None = None


class ProviderCircuitBreaker:
    """Small in-memory circuit breaker for provider-backed playback refresh calls."""

    def __init__(
        self,
        *,
        failure_threshold: int = _PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD,
        reset_timeout_seconds: float = _PROVIDER_CIRCUIT_BREAKER_RESET_TIMEOUT_SECONDS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._failure_threshold = max(1, failure_threshold)
        self._reset_timeout_seconds = max(0.0, reset_timeout_seconds)
        self._clock = clock or monotonic
        self._lock = Lock()
        self._states: dict[str, ProviderCircuitState] = {}

    def _get_state(self, provider: str) -> ProviderCircuitState:
        return self._states.setdefault(provider, ProviderCircuitState())

    def is_open(self, provider: str | None) -> bool:
        if not provider:
            return False

        now = self._clock()
        with self._lock:
            state = self._states.get(provider)
            if state is None or state.opened_until_monotonic is None:
                return False
            if state.opened_until_monotonic <= now:
                state.consecutive_failures = 0
                state.opened_until_monotonic = None
                return False
            return True

    def retry_after_seconds(self, provider: str | None) -> float | None:
        if not provider:
            return None

        now = self._clock()
        with self._lock:
            state = self._states.get(provider)
            if state is None or state.opened_until_monotonic is None:
                return None
            if state.opened_until_monotonic <= now:
                state.consecutive_failures = 0
                state.opened_until_monotonic = None
                return None
            return max(0.0, state.opened_until_monotonic - now)

    def record_success(self, provider: str | None) -> None:
        if not provider:
            return

        with self._lock:
            state = self._get_state(provider)
            state.consecutive_failures = 0
            state.opened_until_monotonic = None

    def record_failure(self, provider: str | None) -> bool:
        if not provider:
            return False

        now = self._clock()
        with self._lock:
            state = self._get_state(provider)
            if state.opened_until_monotonic is not None:
                if state.opened_until_monotonic > now:
                    return False
                state.consecutive_failures = 0
                state.opened_until_monotonic = None

            state.consecutive_failures += 1
            if state.consecutive_failures < self._failure_threshold:
                return False

            state.consecutive_failures = 0
            state.opened_until_monotonic = now + self._reset_timeout_seconds

        PROVIDER_CIRCUIT_OPEN_EVENTS.labels(provider=provider).inc()
        return True


@dataclass(frozen=True)
class LinkResolutionOutcome:
    """Service-layer outcome for one persisted media-entry link-resolution attempt."""

    attachment: PlaybackAttachment | None
    refreshed: bool = False
    detail: str | None = None
    limiter_bucket_key: str | None = None
    retry_after_seconds: float | None = None


class LinkResolver:
    """Resolve one persisted media-entry lease into a current playable attachment."""

    def __init__(
        self,
        playback_service: PlaybackSourceService,
        *,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: PlaybackRefreshRateLimiter | None = None,
        provider_circuit_breaker: ProviderCircuitBreaker | None = None,
    ) -> None:
        self._playback_service = playback_service
        self._provider_clients = provider_clients or {}
        self._rate_limiter = rate_limiter
        self._provider_circuit_breaker = provider_circuit_breaker

    def can_resolve_media_entry(self, entry: MediaEntryORM) -> bool:
        request = PlaybackSourceService.build_media_entry_refresh_request(entry)
        if request is None:
            return False
        provider = request.provider
        return provider is not None and provider in self._provider_clients

    @staticmethod
    def _build_current_attachment_from_entry(
        entry: MediaEntryORM,
        *,
        now: datetime,
    ) -> PlaybackAttachment | None:
        source_attachment = entry.source_attachment
        kind = cast(PlaybackAttachmentKind, entry.kind)
        provider = entry.provider
        provider_download_id = entry.provider_download_id
        provider_file_id = entry.provider_file_id
        provider_file_path = entry.provider_file_path
        original_filename = entry.original_filename
        file_size = entry.size_bytes
        local_path = entry.local_path
        restricted_url = entry.download_url
        unrestricted_url = entry.unrestricted_url

        if source_attachment is not None:
            provider = provider or source_attachment.provider
            provider_download_id = provider_download_id or source_attachment.provider_download_id
            provider_file_id = provider_file_id or source_attachment.provider_file_id
            provider_file_path = provider_file_path or source_attachment.provider_file_path
            original_filename = original_filename or source_attachment.original_filename
            file_size = file_size if file_size is not None else source_attachment.file_size
            local_path = local_path or source_attachment.local_path
            restricted_url = restricted_url or source_attachment.restricted_url
            unrestricted_url = unrestricted_url or source_attachment.unrestricted_url

        restricted_url, unrestricted_url = PlaybackSourceService._normalize_media_entry_urls(
            provider=provider,
            restricted_url=restricted_url,
            unrestricted_url=unrestricted_url,
        )
        effective_refresh_state = PlaybackSourceService._effective_media_entry_refresh_state(
            entry.refresh_state,
            provider=provider,
            restricted_url=restricted_url,
            unrestricted_url=entry.unrestricted_url,
        )

        if kind == "local-file":
            if local_path is None or not Path(local_path).is_file():
                return None
            return PlaybackAttachment(
                kind=kind,
                locator=local_path,
                source_key="media-entry",
                provider=provider,
                provider_download_id=provider_download_id,
                provider_file_id=provider_file_id,
                provider_file_path=provider_file_path,
                original_filename=original_filename,
                file_size=file_size,
                local_path=local_path,
                restricted_url=restricted_url,
                unrestricted_url=unrestricted_url,
                expires_at=entry.expires_at,
                refresh_state=PlaybackSourceService._normalize_refresh_state(
                    effective_refresh_state
                ),
            )

        locator = unrestricted_url or restricted_url
        if locator is None:
            return None
        if entry.expires_at is not None and entry.expires_at <= now:
            return None

        final_kind: PlaybackAttachmentKind = kind
        if kind != "remote-hls" and is_hls_playlist_url(locator):
            final_kind = "remote-hls"

        return PlaybackAttachment(
            kind=final_kind,
            locator=locator,
            source_key="media-entry",
            provider=provider,
            provider_download_id=provider_download_id,
            provider_file_id=provider_file_id,
            provider_file_path=provider_file_path,
            original_filename=original_filename,
            file_size=file_size,
            local_path=local_path,
            restricted_url=restricted_url,
            unrestricted_url=unrestricted_url,
            expires_at=entry.expires_at,
            refresh_state=PlaybackSourceService._normalize_refresh_state(
                effective_refresh_state
            ),
        )

    async def resolve_media_entry(
        self,
        entry: MediaEntryORM,
        *,
        roles: tuple[str, ...] = (),
        surface: str,
        force_refresh: bool = False,
        at: datetime | None = None,
    ) -> LinkResolutionOutcome:
        requested_at = at or datetime.now(UTC)
        current_attachment = self._build_current_attachment_from_entry(entry, now=requested_at)
        media_entry_request = PlaybackSourceService.build_media_entry_refresh_request(
            entry, roles=roles
        )
        if media_entry_request is None:
            if current_attachment is not None and not force_refresh:
                return LinkResolutionOutcome(attachment=current_attachment)
            return LinkResolutionOutcome(
                attachment=None,
                detail=PlaybackSourceService._build_failed_lease_detail(entry, role=surface),
            )

        provider = media_entry_request.provider
        if provider is None or provider not in self._provider_clients:
            if current_attachment is not None and not force_refresh:
                return LinkResolutionOutcome(attachment=current_attachment)
            return LinkResolutionOutcome(
                attachment=None,
                detail=PlaybackSourceService._build_failed_lease_detail(entry, role=surface),
            )

        if self._provider_circuit_breaker is not None and self._provider_circuit_breaker.is_open(
            provider
        ):
            retry_after = self._provider_circuit_breaker.retry_after_seconds(provider)
            if current_attachment is not None and not force_refresh:
                return LinkResolutionOutcome(
                    attachment=current_attachment,
                    retry_after_seconds=retry_after,
                )
            PLAYBACK_RISK_EVENTS.labels(surface=surface, reason="provider_circuit_open").inc()
            return LinkResolutionOutcome(
                attachment=None,
                detail=PlaybackSourceService._build_failed_lease_detail(entry, role=surface),
                retry_after_seconds=retry_after,
            )

        limiter_bucket_key: str | None = None
        if self._rate_limiter is not None:
            limiter_bucket_key = PlaybackSourceService._build_refresh_rate_limit_bucket_key(
                provider
            )
            rate_limit_decision = await self._rate_limiter.acquire(
                bucket_key=limiter_bucket_key,
                capacity=_PLAYBACK_REFRESH_RATE_LIMIT_CAPACITY,
                refill_rate_per_second=_PLAYBACK_REFRESH_RATE_LIMIT_REFILL_PER_SECOND,
            )
            if not rate_limit_decision.allowed:
                if current_attachment is not None and not force_refresh:
                    return LinkResolutionOutcome(
                        attachment=current_attachment,
                        limiter_bucket_key=limiter_bucket_key,
                        retry_after_seconds=rate_limit_decision.retry_after_seconds,
                    )
                PLAYBACK_RISK_EVENTS.labels(surface=surface, reason="refresh_rate_limited").inc()
                return LinkResolutionOutcome(
                    attachment=None,
                    detail=PlaybackSourceService._build_failed_lease_detail(entry, role=surface),
                    limiter_bucket_key=limiter_bucket_key,
                    retry_after_seconds=rate_limit_decision.retry_after_seconds,
                )

        attachment_request = PlaybackSourceService._as_attachment_refresh_request(
            media_entry_request
        )
        self._playback_service.start_media_entry_refresh(entry, at=requested_at)
        executor = self._playback_service.select_refresh_executor(
            attachment_request,
            provider_clients=self._provider_clients,
        )
        result = await executor(attachment_request)
        updated_entry = self._playback_service.apply_media_entry_refresh_result(
            entry,
            result,
            at=requested_at,
        )
        await self._playback_service._persist_media_entry_projection(updated_entry)
        if not result.ok:
            error_text = updated_entry.last_refresh_error or "refresh failed"
            if "circuit open" in error_text.casefold():
                retry_after = None
                if self._provider_circuit_breaker is not None:
                    retry_after = self._provider_circuit_breaker.retry_after_seconds(provider)
                if current_attachment is not None and not force_refresh:
                    return LinkResolutionOutcome(
                        attachment=current_attachment,
                        limiter_bucket_key=limiter_bucket_key,
                        retry_after_seconds=retry_after,
                    )
                PLAYBACK_RISK_EVENTS.labels(surface=surface, reason="provider_circuit_open").inc()
                return LinkResolutionOutcome(
                    attachment=None,
                    detail=PlaybackSourceService._build_failed_lease_detail(
                        updated_entry, role=surface
                    ),
                    limiter_bucket_key=limiter_bucket_key,
                    retry_after_seconds=retry_after,
                )
            if current_attachment is not None and not force_refresh:
                return LinkResolutionOutcome(
                    attachment=current_attachment,
                    limiter_bucket_key=limiter_bucket_key,
                )
            return LinkResolutionOutcome(
                attachment=None,
                detail=PlaybackSourceService._build_failed_lease_detail(
                    updated_entry, role=surface
                ),
                limiter_bucket_key=limiter_bucket_key,
            )

        refreshed_attachment, missing_local_file = (
            PlaybackSourceService._build_attachment_from_media_entry(
                updated_entry,
                source_attachment=updated_entry.source_attachment,
                now=requested_at,
            )
        )
        if refreshed_attachment is not None:
            return LinkResolutionOutcome(
                attachment=refreshed_attachment,
                refreshed=True,
                limiter_bucket_key=limiter_bucket_key,
            )
        if current_attachment is not None and not force_refresh:
            return LinkResolutionOutcome(
                attachment=current_attachment,
                limiter_bucket_key=limiter_bucket_key,
            )
        detail = (
            "Resolved playback file is missing"
            if missing_local_file
            else PlaybackSourceService._build_failed_lease_detail(updated_entry, role=surface)
        )
        return LinkResolutionOutcome(
            attachment=None,
            detail=detail,
            limiter_bucket_key=limiter_bucket_key,
        )


_DIRECT_PLAYBACK_REFRESH_INTENT_CLASSES: frozenset[DirectPlaybackSourceClass] = frozenset(
    {
        "selected-provider-direct-stale",
        "selected-provider-direct-refreshing",
        "selected-provider-direct-failed",
        "selected-provider-direct-degraded",
        "fallback-provider-direct-stale",
        "fallback-provider-direct-refreshing",
        "fallback-provider-direct-failed",
        "fallback-provider-direct-degraded",
        "selected-degraded-direct",
        "fallback-degraded-direct",
    }
)


@runtime_checkable
class PlaybackAttachmentProviderProjectionClient(Protocol):
    """Provider client that can project provider-side file records for one download identifier."""

    async def project_download_attachments(
        self,
        *,
        request: PlaybackAttachmentRefreshRequest,
    ) -> list[PlaybackAttachmentProviderFileProjection]: ...


PlaybackAttachmentRefreshExecutor = Callable[
    [PlaybackAttachmentRefreshRequest], Coroutine[object, object, PlaybackAttachmentRefreshResult]
]


class PlaybackSourceService:
    """Resolve playback attachments from persisted item metadata."""

    def __init__(
        self,
        db: DatabaseRuntime,
        *,
        settings: Settings | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: PlaybackRefreshRateLimiter | None = None,
        provider_circuit_breaker: ProviderCircuitBreaker | None = None,
    ) -> None:
        self._db = db
        self._settings = self._resolve_settings(settings)
        self._rate_limiter = rate_limiter
        self._provider_circuit_breaker = provider_circuit_breaker or ProviderCircuitBreaker()
        self._provider_clients = provider_clients or self._build_builtin_provider_clients(
            self._settings
        )
        self._link_resolver = LinkResolver(
            self,
            provider_clients=self._provider_clients,
            rate_limiter=self._rate_limiter,
            provider_circuit_breaker=self._provider_circuit_breaker,
        )

    @staticmethod
    def _resolve_settings(settings: Settings | None) -> Settings | None:
        if settings is not None:
            return settings
        try:
            return get_settings()
        except ValidationError:
            return None

    @staticmethod
    def _build_builtin_provider_clients(
        settings: Settings | None,
    ) -> dict[str, PlaybackAttachmentProviderClient]:
        if settings is None:
            return {}

        from filmu_py.services.debrid import build_builtin_playback_provider_clients

        return build_builtin_playback_provider_clients(settings)

    @staticmethod
    def _matches_identifier(item: MediaItemORM, item_identifier: str) -> bool:
        if item.id == item_identifier or item.external_ref == item_identifier:
            return True

        attributes = cast(dict[str, object], item.attributes or {})
        for key in _MATCH_ATTR_KEYS:
            value = attributes.get(key)
            if isinstance(value, str) and value == item_identifier:
                return True
        return False

    @staticmethod
    def _basename_from_candidate(value: str | None) -> str | None:
        if not value:
            return None
        candidate = value.rsplit("/", 1)[-1]
        candidate = candidate.rsplit("\\", 1)[-1]
        return candidate or None

    @staticmethod
    def _build_inline_content_disposition(filename: str) -> str:
        if filename.isascii():
            escaped = filename.replace("\\", "\\\\").replace('"', r"\"")
            return f'inline; filename="{escaped}"'
        return f"inline; filename*=utf-8''{quote(filename)}"

    @staticmethod
    def _providers_are_compatible(left: str | None, right: str | None) -> bool:
        return left is None or right is None or left == right

    @staticmethod
    def _matching_text(left: str | None, right: str | None) -> bool:
        if left is None or right is None:
            return False
        assert left is not None
        assert right is not None
        left_text = left.strip()
        right_text = right.strip()
        return left_text != "" and left_text == right_text

    @staticmethod
    def _matching_file_size(left: int | None, right: int | None) -> bool:
        return left is not None and right is not None and left == right

    @staticmethod
    def _normalize_refresh_state(value: str | None) -> PlaybackAttachmentRefreshState | None:
        if value in {"ready", "stale", "refreshing", "failed"}:
            return cast(PlaybackAttachmentRefreshState, value)
        return None

    @staticmethod
    def _classify_direct_file_provider_family(provider: str | None) -> DirectFileLinkProviderFamily:
        if provider is None:
            return "none"
        normalized_provider = provider.strip().casefold()
        if normalized_provider == "":
            return "none"
        if normalized_provider in _DEBRID_PROVIDER_KEYS:
            return "debrid"
        return "provider"

    @staticmethod
    def _classify_direct_file_locator_source(
        locator: str,
        *,
        local_path: str | None,
        unrestricted_url: str | None,
        restricted_url: str | None,
    ) -> DirectFileLinkLocatorSource:
        if local_path is not None and locator == local_path:
            return "local-path"
        if unrestricted_url is not None and locator == unrestricted_url:
            return "unrestricted-url"
        if restricted_url is not None and locator == restricted_url:
            return "restricted-url"
        return "locator"

    @staticmethod
    def _is_direct_file_restricted_fallback(attachment: PlaybackAttachment) -> bool:
        return attachment.source_key.endswith(
            ":restricted-fallback"
        ) or _is_degraded_direct_attachment(attachment)

    @staticmethod
    def _has_placeholder_debrid_unrestricted_url(
        *,
        provider: str | None,
        restricted_url: str | None,
        unrestricted_url: str | None,
    ) -> bool:
        if restricted_url is None or unrestricted_url is None:
            return False
        normalized_provider = provider.strip().casefold() if isinstance(provider, str) else ""
        if normalized_provider not in _DEBRID_PROVIDER_KEYS:
            return False
        return restricted_url.strip() == unrestricted_url.strip()

    @staticmethod
    def _normalize_media_entry_urls(
        *,
        provider: str | None,
        restricted_url: str | None,
        unrestricted_url: str | None,
    ) -> tuple[str | None, str | None]:
        if PlaybackSourceService._has_placeholder_debrid_unrestricted_url(
            provider=provider,
            restricted_url=restricted_url,
            unrestricted_url=unrestricted_url,
        ):
            return restricted_url, None
        return restricted_url, unrestricted_url

    @staticmethod
    def _effective_media_entry_refresh_state(
        refresh_state: str,
        *,
        provider: str | None,
        restricted_url: str | None,
        unrestricted_url: str | None,
    ) -> str:
        if PlaybackSourceService._has_placeholder_debrid_unrestricted_url(
            provider=provider,
            restricted_url=restricted_url,
            unrestricted_url=unrestricted_url,
        ) and refresh_state == "ready":
            return "stale"
        return refresh_state

    @staticmethod
    def _match_basis_for_attachment_owner(
        record: PlaybackAttachmentORM,
        resolved: PlaybackAttachment,
    ) -> DirectFileLinkMatchBasis | None:
        if not PlaybackSourceService._providers_are_compatible(record.provider, resolved.provider):
            return None

        if PlaybackSourceService._matching_text(record.provider_file_id, resolved.provider_file_id):
            return "provider-file-id"
        if PlaybackSourceService._matching_text(
            record.provider_file_path, resolved.provider_file_path
        ):
            return "provider-file-path"
        if PlaybackSourceService._matching_text(record.local_path, resolved.local_path):
            return "local-path"
        if PlaybackSourceService._matching_text(record.unrestricted_url, resolved.unrestricted_url):
            return "unrestricted-url"
        if PlaybackSourceService._matching_text(record.restricted_url, resolved.restricted_url):
            return "restricted-url"
        if PlaybackSourceService._matching_text(record.locator, resolved.locator):
            return "locator"
        if PlaybackSourceService._matching_text(
            record.original_filename,
            resolved.original_filename,
        ) and PlaybackSourceService._matching_file_size(record.file_size, resolved.file_size):
            return "filename+size"

        if PlaybackSourceService._matching_text(
            record.provider_download_id,
            resolved.provider_download_id,
        ):
            if PlaybackSourceService._matching_text(
                record.original_filename, resolved.original_filename
            ):
                return "provider-download-id+filename"
            if PlaybackSourceService._matching_text(
                record.provider_file_path,
                resolved.provider_file_path,
            ):
                return "provider-download-id+provider-file-path"
            if PlaybackSourceService._matching_file_size(record.file_size, resolved.file_size):
                return "provider-download-id+file-size"

        return None

    @staticmethod
    def _match_basis_for_media_entry_owner(
        entry: MediaEntryORM,
        resolved: PlaybackAttachment,
        *,
        source_attachment_id: str | None,
    ) -> DirectFileLinkMatchBasis | None:
        if source_attachment_id is not None and entry.source_attachment_id == source_attachment_id:
            return "source-attachment-id"
        if not PlaybackSourceService._providers_are_compatible(entry.provider, resolved.provider):
            return None

        if PlaybackSourceService._matching_text(entry.provider_file_id, resolved.provider_file_id):
            return "provider-file-id"
        if PlaybackSourceService._matching_text(
            entry.provider_file_path, resolved.provider_file_path
        ):
            return "provider-file-path"
        if PlaybackSourceService._matching_text(entry.local_path, resolved.local_path):
            return "local-path"
        if PlaybackSourceService._matching_text(entry.unrestricted_url, resolved.unrestricted_url):
            return "unrestricted-url"
        if PlaybackSourceService._matching_text(entry.download_url, resolved.restricted_url):
            return "restricted-url"
        if PlaybackSourceService._matching_text(
            entry.original_filename, resolved.original_filename
        ) and (PlaybackSourceService._matching_file_size(entry.size_bytes, resolved.file_size)):
            return "filename+size"

        if PlaybackSourceService._matching_text(
            entry.provider_download_id, resolved.provider_download_id
        ):
            if PlaybackSourceService._matching_text(
                entry.original_filename, resolved.original_filename
            ):
                return "provider-download-id+filename"
            if PlaybackSourceService._matching_text(
                entry.provider_file_path,
                resolved.provider_file_path,
            ):
                return "provider-download-id+provider-file-path"
            if PlaybackSourceService._matching_file_size(entry.size_bytes, resolved.file_size):
                return "provider-download-id+file-size"

        return None

    @staticmethod
    def _find_direct_file_attachment_owner(
        item: MediaItemORM,
        attachment: PlaybackAttachment,
    ) -> tuple[PlaybackAttachmentORM | None, DirectFileLinkMatchBasis | None]:
        for record in item.playback_attachments:
            match_basis = PlaybackSourceService._match_basis_for_attachment_owner(
                record, attachment
            )
            if match_basis is not None:
                return record, match_basis
        return None, None

    @staticmethod
    def _find_direct_file_media_entry_owner(
        item: MediaItemORM,
        attachment: PlaybackAttachment,
        *,
        source_attachment_id: str | None,
    ) -> tuple[MediaEntryORM | None, DirectFileLinkMatchBasis | None]:
        for entry in item.media_entries:
            match_basis = PlaybackSourceService._match_basis_for_media_entry_owner(
                entry,
                attachment,
                source_attachment_id=source_attachment_id,
            )
            if match_basis is not None:
                return entry, match_basis
        return None, None

    @staticmethod
    def _build_metadata_direct_file_link_lifecycle(
        attachment: PlaybackAttachment,
    ) -> DirectFileLinkLifecycleSnapshot:
        return DirectFileLinkLifecycleSnapshot(
            owner_kind="metadata",
            owner_id=None,
            provider_family=PlaybackSourceService._classify_direct_file_provider_family(
                attachment.provider
            ),
            locator_source=PlaybackSourceService._classify_direct_file_locator_source(
                attachment.locator,
                local_path=attachment.local_path,
                unrestricted_url=attachment.unrestricted_url,
                restricted_url=attachment.restricted_url,
            ),
            restricted_fallback=PlaybackSourceService._is_direct_file_restricted_fallback(
                attachment
            ),
            refresh_state=attachment.refresh_state,
            expires_at=attachment.expires_at,
        )

    @staticmethod
    def _build_attachment_direct_file_link_lifecycle(
        record: PlaybackAttachmentORM,
        *,
        attachment: PlaybackAttachment,
        match_basis: DirectFileLinkMatchBasis | None,
    ) -> DirectFileLinkLifecycleSnapshot:
        provider = record.provider or attachment.provider
        return DirectFileLinkLifecycleSnapshot(
            owner_kind="attachment",
            owner_id=record.id,
            provider_family=PlaybackSourceService._classify_direct_file_provider_family(provider),
            locator_source=PlaybackSourceService._classify_direct_file_locator_source(
                attachment.locator,
                local_path=record.local_path,
                unrestricted_url=record.unrestricted_url,
                restricted_url=record.restricted_url,
            ),
            restricted_fallback=PlaybackSourceService._is_direct_file_restricted_fallback(
                attachment
            ),
            match_basis=match_basis,
            refresh_state=PlaybackSourceService._normalize_refresh_state(record.refresh_state),
            expires_at=record.expires_at,
            last_refreshed_at=record.last_refreshed_at,
            last_refresh_error=record.last_refresh_error,
        )

    @staticmethod
    def _build_media_entry_direct_file_link_lifecycle(
        entry: MediaEntryORM,
        *,
        attachment: PlaybackAttachment,
        match_basis: DirectFileLinkMatchBasis | None,
    ) -> DirectFileLinkLifecycleSnapshot:
        provider = entry.provider or attachment.provider
        return DirectFileLinkLifecycleSnapshot(
            owner_kind="media-entry",
            owner_id=entry.id,
            provider_family=PlaybackSourceService._classify_direct_file_provider_family(provider),
            locator_source=PlaybackSourceService._classify_direct_file_locator_source(
                attachment.locator,
                local_path=entry.local_path,
                unrestricted_url=entry.unrestricted_url,
                restricted_url=entry.download_url,
            ),
            restricted_fallback=PlaybackSourceService._is_direct_file_restricted_fallback(
                attachment
            ),
            match_basis=match_basis,
            source_attachment_id=entry.source_attachment_id,
            refresh_state=PlaybackSourceService._normalize_refresh_state(entry.refresh_state),
            expires_at=entry.expires_at,
            last_refreshed_at=entry.last_refreshed_at,
            last_refresh_error=entry.last_refresh_error,
        )

    @staticmethod
    def build_direct_file_link_lifecycle(
        attachment: PlaybackAttachment,
        *,
        item: MediaItemORM | None,
    ) -> DirectFileLinkLifecycleSnapshot:
        if item is None:
            return PlaybackSourceService._build_metadata_direct_file_link_lifecycle(attachment)

        owner_attachment, attachment_match_basis = (
            PlaybackSourceService._find_direct_file_attachment_owner(
                item,
                attachment,
            )
        )
        source_attachment_id = owner_attachment.id if owner_attachment is not None else None
        owner_entry, media_entry_match_basis = (
            PlaybackSourceService._find_direct_file_media_entry_owner(
                item,
                attachment,
                source_attachment_id=source_attachment_id,
            )
        )

        if attachment.source_key.startswith("media-entry"):
            if owner_entry is not None:
                return PlaybackSourceService._build_media_entry_direct_file_link_lifecycle(
                    owner_entry,
                    attachment=attachment,
                    match_basis=media_entry_match_basis,
                )
            if owner_attachment is not None:
                return PlaybackSourceService._build_attachment_direct_file_link_lifecycle(
                    owner_attachment,
                    attachment=attachment,
                    match_basis=attachment_match_basis,
                )
            return PlaybackSourceService._build_metadata_direct_file_link_lifecycle(attachment)

        if attachment.source_key.startswith("persisted"):
            if owner_attachment is not None:
                return PlaybackSourceService._build_attachment_direct_file_link_lifecycle(
                    owner_attachment,
                    attachment=attachment,
                    match_basis=attachment_match_basis,
                )
            if owner_entry is not None:
                return PlaybackSourceService._build_media_entry_direct_file_link_lifecycle(
                    owner_entry,
                    attachment=attachment,
                    match_basis=media_entry_match_basis,
                )
            return PlaybackSourceService._build_metadata_direct_file_link_lifecycle(attachment)

        if owner_entry is not None:
            return PlaybackSourceService._build_media_entry_direct_file_link_lifecycle(
                owner_entry,
                attachment=attachment,
                match_basis=media_entry_match_basis,
            )
        if owner_attachment is not None:
            return PlaybackSourceService._build_attachment_direct_file_link_lifecycle(
                owner_attachment,
                attachment=attachment,
                match_basis=attachment_match_basis,
            )
        return PlaybackSourceService._build_metadata_direct_file_link_lifecycle(attachment)

    @staticmethod
    def build_direct_file_link_resolution_provenance(
        decision: DirectPlaybackDecision,
        attachment: PlaybackAttachment,
        *,
        item: MediaItemORM | None = None,
    ) -> DirectFileLinkResolutionProvenance:
        """Build internal provenance for one direct-file link-resolution result."""

        refresh_recommendation = decision.refresh_recommendation
        return DirectFileLinkResolutionProvenance(
            source_key=attachment.source_key,
            source_class=decision.source_class,
            provider=attachment.provider,
            provider_download_id=attachment.provider_download_id,
            provider_file_id=attachment.provider_file_id,
            provider_file_path=attachment.provider_file_path,
            original_filename=attachment.original_filename,
            refresh_state=attachment.refresh_state,
            refresh_intent=decision.refresh_intent,
            refresh_recommendation_reason=(
                refresh_recommendation.reason if refresh_recommendation is not None else None
            ),
            lifecycle=PlaybackSourceService.build_direct_file_link_lifecycle(
                attachment,
                item=item,
            ),
        )

    @staticmethod
    def build_direct_file_link_resolution(
        decision: DirectPlaybackDecision,
        *,
        item: MediaItemORM | None = None,
    ) -> DirectFileLinkResolution:
        """Build one internal direct-file link-resolution result from a direct-play decision."""

        attachment = decision.attachment
        if decision.action != "serve" or attachment is None:
            raise ValueError(
                "direct playback decision must resolve one attachment to build a file link"
            )

        filename = attachment.original_filename or PlaybackSourceService._basename_from_candidate(
            attachment.local_path or attachment.provider_file_path or attachment.locator
        )
        return DirectFileLinkResolution(
            locator=attachment.locator,
            transport="local-file" if attachment.kind == "local-file" else "remote-proxy",
            provenance=PlaybackSourceService.build_direct_file_link_resolution_provenance(
                decision,
                attachment,
                item=item,
            ),
            filename=filename,
            file_size=attachment.file_size,
        )

    @staticmethod
    def build_direct_file_serving_descriptor(
        resolution: DirectFileLinkResolution,
    ) -> DirectFileServingDescriptor:
        """Build one typed direct-file serving descriptor from an internal link-resolution result."""

        response_headers: dict[str, str] = {}
        filename = resolution.filename
        if filename is not None:
            response_headers["content-disposition"] = (
                PlaybackSourceService._build_inline_content_disposition(filename)
            )
        if resolution.file_size is not None:
            response_headers["content-length"] = str(resolution.file_size)
        return DirectFileServingDescriptor(
            locator=resolution.locator,
            transport=resolution.transport,
            response_headers=response_headers,
            provenance=DirectFileServingDescriptorProvenance(
                source_key=resolution.provenance.source_key,
                provider=resolution.provenance.provider,
                provider_download_id=resolution.provenance.provider_download_id,
                provider_file_id=resolution.provenance.provider_file_id,
                provider_file_path=resolution.provenance.provider_file_path,
                original_filename=resolution.provenance.original_filename,
                refresh_state=resolution.provenance.refresh_state,
                lifecycle=resolution.provenance.lifecycle,
            ),
        )

    async def _list_items(self) -> list[MediaItemORM]:
        async with self._db.session() as session:
            return list(
                (
                    await session.execute(
                        select(MediaItemORM)
                        .options(
                            selectinload(MediaItemORM.playback_attachments),
                            selectinload(MediaItemORM.media_entries).selectinload(
                                MediaEntryORM.source_attachment
                            ),
                            selectinload(MediaItemORM.active_streams),
                        )
                        .order_by(MediaItemORM.created_at.desc())
                    )
                )
                .scalars()
                .all()
            )

    @classmethod
    def _find_item_in_scalars(
        cls,
        scalars: list[object],
        *,
        item_identifier: str,
    ) -> MediaItemORM | None:
        for candidate in scalars:
            if isinstance(candidate, MediaItemORM) and cls._matches_identifier(
                candidate, item_identifier
            ):
                return candidate
        return None

    @staticmethod
    def _result_scalars_all(result: object) -> list[object]:
        scalars = getattr(result, "scalars", None)
        if not callable(scalars):
            return []
        scalar_result = scalars()
        all_method = getattr(scalar_result, "all", None)
        if not callable(all_method):
            return []
        return list(all_method())

    @classmethod
    def _find_media_entry_in_scalars(
        cls,
        scalars: list[object],
        *,
        media_entry_id: str,
    ) -> MediaEntryORM | None:
        for candidate in scalars:
            if isinstance(candidate, MediaEntryORM) and candidate.id == media_entry_id:
                return candidate
            if isinstance(candidate, MediaItemORM):
                for entry in candidate.media_entries:
                    if entry.id == media_entry_id:
                        return entry
        return None

    @classmethod
    def _find_attachment_in_scalars(
        cls,
        scalars: list[object],
        *,
        attachment_id: str,
    ) -> PlaybackAttachmentORM | None:
        for candidate in scalars:
            if isinstance(candidate, PlaybackAttachmentORM) and candidate.id == attachment_id:
                return candidate
            if isinstance(candidate, MediaItemORM):
                for attachment in candidate.playback_attachments:
                    if attachment.id == attachment_id:
                        return attachment
        return None

    @staticmethod
    def _copy_attachment_refresh_projection(
        target: PlaybackAttachmentORM,
        source: PlaybackAttachmentORM,
    ) -> PlaybackAttachmentORM:
        target.locator = source.locator
        target.local_path = source.local_path
        target.restricted_url = source.restricted_url
        target.unrestricted_url = source.unrestricted_url
        target.provider_file_id = source.provider_file_id
        target.provider_file_path = source.provider_file_path
        target.original_filename = source.original_filename
        target.file_size = source.file_size
        target.refresh_state = source.refresh_state
        target.expires_at = source.expires_at
        target.last_refreshed_at = source.last_refreshed_at
        target.last_refresh_error = source.last_refresh_error
        target.updated_at = source.updated_at
        return target

    @staticmethod
    def _sync_media_entry_from_source_attachment(
        entry: MediaEntryORM,
        attachment: PlaybackAttachmentORM,
        *,
        at: datetime | None = None,
    ) -> MediaEntryORM:
        refreshed_at = at or attachment.updated_at or datetime.now(UTC)
        entry.local_path = attachment.local_path
        entry.download_url = attachment.restricted_url
        entry.unrestricted_url = attachment.unrestricted_url
        entry.provider = attachment.provider
        entry.provider_download_id = attachment.provider_download_id
        entry.provider_file_id = attachment.provider_file_id
        entry.provider_file_path = attachment.provider_file_path
        entry.original_filename = attachment.original_filename
        entry.size_bytes = attachment.file_size
        entry.refresh_state = attachment.refresh_state
        entry.expires_at = attachment.expires_at
        entry.last_refreshed_at = attachment.last_refreshed_at
        entry.last_refresh_error = attachment.last_refresh_error
        entry.updated_at = refreshed_at
        return entry

    @staticmethod
    def _sync_source_attachment_from_media_entry(
        entry: MediaEntryORM,
        *,
        at: datetime | None = None,
    ) -> None:
        source_attachment = entry.source_attachment
        if source_attachment is None:
            return

        refreshed_at = at or entry.updated_at or datetime.now(UTC)
        if entry.refresh_state == "failed":
            PlaybackSourceService.fail_attachment_refresh(
                source_attachment,
                error=entry.last_refresh_error or "media entry refresh failed",
                at=refreshed_at,
            )
            return
        if entry.refresh_state == "refreshing":
            PlaybackSourceService.start_attachment_refresh(source_attachment, at=refreshed_at)
            return
        if entry.refresh_state == "stale":
            PlaybackSourceService.mark_attachment_stale(source_attachment, at=refreshed_at)
            return

        locator = (
            entry.local_path
            or entry.unrestricted_url
            or entry.download_url
            or source_attachment.locator
        )
        PlaybackSourceService.complete_attachment_refresh(
            source_attachment,
            locator=locator,
            restricted_url=entry.download_url,
            unrestricted_url=entry.unrestricted_url,
            expires_at=entry.expires_at,
            provider_file_id=entry.provider_file_id,
            provider_file_path=entry.provider_file_path,
            original_filename=entry.original_filename,
            file_size=entry.size_bytes,
            at=refreshed_at,
        )

    @classmethod
    def _copy_media_entry_refresh_projection(
        cls,
        target: MediaEntryORM,
        source: MediaEntryORM,
    ) -> MediaEntryORM:
        target.local_path = source.local_path
        target.download_url = source.download_url
        target.unrestricted_url = source.unrestricted_url
        target.provider_file_id = source.provider_file_id
        target.provider_file_path = source.provider_file_path
        target.original_filename = source.original_filename
        target.size_bytes = source.size_bytes
        target.refresh_state = source.refresh_state
        target.expires_at = source.expires_at
        target.last_refreshed_at = source.last_refreshed_at
        target.last_refresh_error = source.last_refresh_error
        target.updated_at = source.updated_at
        cls._sync_source_attachment_from_media_entry(target, at=target.updated_at)
        return target

    async def _persist_media_entry_projection(self, entry: MediaEntryORM) -> None:
        async with self._db.session() as session:
            if not hasattr(session, "execute") or not hasattr(session, "commit"):
                return
            result = await session.execute(
                select(MediaEntryORM)
                .options(selectinload(MediaEntryORM.source_attachment))
                .where(MediaEntryORM.id == entry.id)
            )
            persisted_entry = self._find_media_entry_in_scalars(
                self._result_scalars_all(result),
                media_entry_id=entry.id,
            )
            if persisted_entry is None:
                return
            self._copy_media_entry_refresh_projection(persisted_entry, entry)
            await session.commit()

    async def _persist_attachment_projection(
        self,
        attachment: PlaybackAttachmentORM,
    ) -> None:
        async with self._db.session() as session:
            if not hasattr(session, "execute") or not hasattr(session, "commit"):
                return
            result = await session.execute(
                select(PlaybackAttachmentORM).where(PlaybackAttachmentORM.id == attachment.id)
            )
            persisted_attachment = self._find_attachment_in_scalars(
                self._result_scalars_all(result),
                attachment_id=attachment.id,
            )
            if persisted_attachment is None:
                return
            self._copy_attachment_refresh_projection(persisted_attachment, attachment)
            await session.commit()

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
        """Persist bounded playback-attachment URL/state changes and sync linked media entries."""

        if refresh_state is not None and refresh_state not in _ATTACHMENT_REFRESH_STATES:
            raise ValueError(f"unsupported playback attachment refresh state: {refresh_state}")

        requested_at = at or datetime.now(UTC)
        async with self._db.session() as session:
            if not hasattr(session, "execute") or not hasattr(session, "commit"):
                return None
            result = await session.execute(
                select(MediaItemORM)
                .options(
                    selectinload(MediaItemORM.playback_attachments),
                    selectinload(MediaItemORM.media_entries).selectinload(
                        MediaEntryORM.source_attachment
                    ),
                    selectinload(MediaItemORM.active_streams),
                )
                .order_by(MediaItemORM.created_at.desc())
            )
            scalars = self._result_scalars_all(result)
            item = self._find_item_in_scalars(scalars, item_identifier=item_identifier)
            if item is None:
                return None
            attachment = self._find_attachment_in_scalars([item], attachment_id=attachment_id)
            if attachment is None:
                return None

            if locator is not None:
                attachment.locator = locator
            if local_path is not None:
                attachment.local_path = local_path
            if restricted_url is not None:
                attachment.restricted_url = restricted_url
            if unrestricted_url is not None:
                attachment.unrestricted_url = unrestricted_url
            if expires_at is not None:
                attachment.expires_at = expires_at

            if refresh_state == "stale":
                self.mark_attachment_stale(attachment, at=requested_at)
            elif refresh_state == "refreshing":
                self.start_attachment_refresh(attachment, at=requested_at)
            elif refresh_state == "failed":
                self.fail_attachment_refresh(
                    attachment,
                    error=(
                        last_refresh_error
                        or attachment.last_refresh_error
                        or "playback attachment refresh failed"
                    ),
                    at=requested_at,
                )
            elif refresh_state == "ready":
                self.complete_attachment_refresh(
                    attachment,
                    locator=attachment.locator,
                    restricted_url=attachment.restricted_url,
                    unrestricted_url=attachment.unrestricted_url,
                    expires_at=attachment.expires_at,
                    provider_file_id=attachment.provider_file_id,
                    provider_file_path=attachment.provider_file_path,
                    original_filename=attachment.original_filename,
                    file_size=attachment.file_size,
                    at=requested_at,
                )
            else:
                attachment.updated_at = requested_at

            if (refresh_state is None and last_refresh_error is not None) or (refresh_state in {"stale", "refreshing"} and last_refresh_error is not None):
                attachment.last_refresh_error = last_refresh_error

            linked_entries: list[MediaEntryORM] = []
            for entry in item.media_entries:
                if entry.source_attachment_id != attachment.id:
                    continue
                self._sync_media_entry_from_source_attachment(
                    entry,
                    attachment,
                    at=attachment.updated_at,
                )
                linked_entries.append(entry)

            await session.commit()
            return PersistedPlaybackAttachmentControlMutationResult(
                item_identifier=item.id,
                attachment_id=attachment.id,
                item=item,
                attachment=attachment,
                linked_media_entries=tuple(linked_entries),
            )

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
        """Persist bounded media-entry URL/state changes and optional active-role rebinding."""

        if active_role is not None and active_role not in {
            _ACTIVE_STREAM_ROLE_DIRECT,
            _ACTIVE_STREAM_ROLE_HLS,
        }:
            raise ValueError(f"unsupported active stream role: {active_role}")
        if refresh_state is not None and refresh_state not in _MEDIA_ENTRY_REFRESH_STATES:
            raise ValueError(f"unsupported media entry refresh state: {refresh_state}")

        requested_at = at or datetime.now(UTC)
        async with self._db.session() as session:
            if not hasattr(session, "execute") or not hasattr(session, "commit"):
                return None
            result = await session.execute(
                select(MediaItemORM)
                .options(
                    selectinload(MediaItemORM.playback_attachments),
                    selectinload(MediaItemORM.media_entries).selectinload(
                        MediaEntryORM.source_attachment
                    ),
                    selectinload(MediaItemORM.active_streams),
                )
                .order_by(MediaItemORM.created_at.desc())
            )
            scalars = self._result_scalars_all(result)
            item = self._find_item_in_scalars(scalars, item_identifier=item_identifier)
            if item is None:
                return None
            entry = self._find_media_entry_in_scalars(
                [item],
                media_entry_id=media_entry_id,
            )
            if entry is None:
                return None

            if local_path is not None:
                entry.local_path = local_path
            if download_url is not None:
                entry.download_url = download_url
            if unrestricted_url is not None:
                entry.unrestricted_url = unrestricted_url
            if expires_at is not None:
                entry.expires_at = expires_at

            if refresh_state == "stale":
                self.mark_media_entry_stale(entry, at=requested_at)
            elif refresh_state == "refreshing":
                self.start_media_entry_refresh(entry, at=requested_at)
            elif refresh_state == "failed":
                self.fail_media_entry_refresh(
                    entry,
                    error=last_refresh_error or entry.last_refresh_error or "media entry refresh failed",
                    at=requested_at,
                )
            elif refresh_state == "ready":
                self.complete_media_entry_refresh(
                    entry,
                    download_url=entry.download_url,
                    unrestricted_url=entry.unrestricted_url,
                    expires_at=entry.expires_at,
                    provider_file_id=entry.provider_file_id,
                    provider_file_path=entry.provider_file_path,
                    original_filename=entry.original_filename,
                    size_bytes=entry.size_bytes,
                    at=requested_at,
                )
            else:
                entry.updated_at = requested_at

            if (refresh_state is None and last_refresh_error is not None) or (refresh_state in {"stale", "refreshing"} and last_refresh_error is not None):
                entry.last_refresh_error = last_refresh_error

            self._sync_source_attachment_from_media_entry(entry, at=entry.updated_at)

            if active_role is not None:
                matching_streams = [
                    active_stream
                    for active_stream in item.active_streams
                    if active_stream.role == active_role
                ]
                existing = matching_streams[0] if matching_streams else None
                if existing is None:
                    item.active_streams.append(
                        ActiveStreamORM(
                            item_id=item.id,
                            media_entry_id=entry.id,
                            role=active_role,
                        )
                    )
                else:
                    existing.media_entry_id = entry.id
                    existing.updated_at = requested_at

            await session.commit()
            return PersistedMediaEntryControlMutationResult(
                item_identifier=item.id,
                media_entry_id=entry.id,
                item=item,
                media_entry=entry,
                applied_role=active_role,
            )

    @staticmethod
    def _build_attachment_from_media_entry(
        entry: MediaEntryORM,
        *,
        source_attachment: PlaybackAttachmentORM | None,
        now: datetime,
    ) -> tuple[PlaybackAttachment | None, bool]:
        kind = cast(PlaybackAttachmentKind, entry.kind)
        provider = entry.provider
        provider_download_id = entry.provider_download_id
        provider_file_id = entry.provider_file_id
        provider_file_path = entry.provider_file_path
        original_filename = entry.original_filename
        file_size = entry.size_bytes
        local_path = entry.local_path
        restricted_url = entry.download_url
        unrestricted_url = entry.unrestricted_url
        source_key = "media-entry"

        if source_attachment is not None:
            provider = provider or source_attachment.provider
            provider_download_id = provider_download_id or source_attachment.provider_download_id
            provider_file_id = provider_file_id or source_attachment.provider_file_id
            provider_file_path = provider_file_path or source_attachment.provider_file_path
            original_filename = original_filename or source_attachment.original_filename
            file_size = file_size if file_size is not None else source_attachment.file_size
            local_path = local_path or source_attachment.local_path
            restricted_url = restricted_url or source_attachment.restricted_url
            unrestricted_url = unrestricted_url or source_attachment.unrestricted_url

            if (
                source_attachment.refresh_state == "failed"
                and unrestricted_url is None
                and restricted_url is None
            ):
                return None, False

        restricted_url, unrestricted_url = PlaybackSourceService._normalize_media_entry_urls(
            provider=provider,
            restricted_url=restricted_url,
            unrestricted_url=unrestricted_url,
        )
        effective_refresh_state = PlaybackSourceService._effective_media_entry_refresh_state(
            entry.refresh_state,
            provider=provider,
            restricted_url=restricted_url,
            unrestricted_url=entry.unrestricted_url,
        )

        if kind == "local-file":
            if local_path is None:
                return None, False
            if not Path(local_path).is_file():
                return None, True
            return (
                PlaybackAttachment(
                    kind=kind,
                    locator=local_path,
                    source_key=source_key,
                    provider=provider,
                    provider_download_id=provider_download_id,
                    provider_file_id=provider_file_id,
                    provider_file_path=provider_file_path,
                    original_filename=original_filename,
                    file_size=file_size,
                    local_path=local_path,
                    restricted_url=restricted_url,
                    unrestricted_url=unrestricted_url,
                    expires_at=entry.expires_at,
                    refresh_state=cast(PlaybackAttachmentRefreshState, effective_refresh_state),
                ),
                False,
            )

        if effective_refresh_state not in _MEDIA_ENTRY_REFRESH_STATES:
            return None, False
        if effective_refresh_state == "failed":
            return None, False

        requires_refresh_fallback = effective_refresh_state in {"stale", "refreshing"}
        if entry.expires_at is not None and entry.expires_at <= now:
            requires_refresh_fallback = True
        if requires_refresh_fallback:
            if restricted_url is None:
                return None, False
            fallback_kind: PlaybackAttachmentKind = (
                "remote-hls" if is_hls_playlist_url(restricted_url) else "remote-direct"
            )
            return (
                PlaybackAttachment(
                    kind=fallback_kind,
                    locator=restricted_url,
                    source_key=f"{source_key}:restricted-fallback",
                    provider=provider,
                    provider_download_id=provider_download_id,
                    provider_file_id=provider_file_id,
                    provider_file_path=provider_file_path,
                    original_filename=original_filename,
                    file_size=file_size,
                    local_path=local_path,
                    restricted_url=restricted_url,
                    unrestricted_url=unrestricted_url,
                    expires_at=entry.expires_at,
                    refresh_state=cast(PlaybackAttachmentRefreshState, effective_refresh_state),
                ),
                False,
            )

        final_locator = unrestricted_url or restricted_url
        if final_locator is None:
            return None, False
        final_kind: PlaybackAttachmentKind = (
            "remote-hls" if is_hls_playlist_url(final_locator) else "remote-direct"
        )
        return (
            PlaybackAttachment(
                kind=final_kind,
                locator=final_locator,
                source_key=source_key,
                provider=provider,
                provider_download_id=provider_download_id,
                provider_file_id=provider_file_id,
                provider_file_path=provider_file_path,
                original_filename=original_filename,
                file_size=file_size,
                local_path=local_path,
                restricted_url=restricted_url,
                unrestricted_url=unrestricted_url,
                expires_at=entry.expires_at,
                refresh_state=cast(PlaybackAttachmentRefreshState, effective_refresh_state),
            ),
            False,
        )

    def _resolve_persisted_media_entry_attachments(
        self, item: MediaItemORM
    ) -> tuple[list[PlaybackAttachment], dict[str, PlaybackAttachment], bool]:
        now = datetime.now(UTC)
        ordered_entries = sorted(
            item.media_entries,
            key=lambda entry: (entry.created_at, entry.id),
        )
        resolved: list[PlaybackAttachment] = []
        by_entry_id: dict[str, PlaybackAttachment] = {}
        saw_missing_path = False
        for entry in ordered_entries:
            attachment, entry_missing_path = self._build_attachment_from_media_entry(
                entry,
                source_attachment=entry.source_attachment,
                now=now,
            )
            if entry_missing_path:
                saw_missing_path = True
            if attachment is None:
                continue
            resolved.append(attachment)
            by_entry_id[entry.id] = attachment
        return resolved, by_entry_id, saw_missing_path

    @staticmethod
    def _select_persisted_active_stream_attachment(
        item: MediaItemORM,
        *,
        role: str,
        attachments_by_entry_id: dict[str, PlaybackAttachment],
    ) -> PlaybackAttachment | None:
        entries_by_id = {entry.id: entry for entry in item.media_entries}
        for active_stream in sorted(
            item.active_streams,
            key=lambda entry: (entry.created_at, entry.id),
        ):
            if active_stream.role != role:
                continue
            selected_entry = entries_by_id.get(active_stream.media_entry_id)
            attachment = attachments_by_entry_id.get(active_stream.media_entry_id)
            if attachment is None:
                if selected_entry is None:
                    return None
                return PlaybackSourceService._select_related_media_entry_attachment(
                    item,
                    role=role,
                    selected_entry=selected_entry,
                    attachments_by_entry_id=attachments_by_entry_id,
                )
            if role == _ACTIVE_STREAM_ROLE_DIRECT and attachment.kind == "remote-hls":
                if selected_entry is None:
                    return None
                return PlaybackSourceService._select_related_media_entry_attachment(
                    item,
                    role=role,
                    selected_entry=selected_entry,
                    attachments_by_entry_id=attachments_by_entry_id,
                )
            if role == _ACTIVE_STREAM_ROLE_DIRECT and _is_degraded_direct_attachment(attachment):
                if selected_entry is None:
                    return None
                return PlaybackSourceService._select_related_media_entry_attachment(
                    item,
                    role=role,
                    selected_entry=selected_entry,
                    attachments_by_entry_id=attachments_by_entry_id,
                )
            if role == _ACTIVE_STREAM_ROLE_HLS and attachment.kind not in {
                "remote-hls",
                "local-file",
            }:
                if selected_entry is None:
                    return None
                return PlaybackSourceService._select_related_media_entry_attachment(
                    item,
                    role=role,
                    selected_entry=selected_entry,
                    attachments_by_entry_id=attachments_by_entry_id,
                )
            return attachment
        return None

    @staticmethod
    def _media_entries_share_provider_identity(
        left: MediaEntryORM,
        right: MediaEntryORM,
    ) -> bool:
        if left.id == right.id:
            return False
        if left.provider and right.provider and left.provider != right.provider:
            return False
        if left.provider_file_id and right.provider_file_id:
            return left.provider_file_id == right.provider_file_id
        if left.provider_file_path and right.provider_file_path:
            return left.provider_file_path == right.provider_file_path
        return False

    @staticmethod
    def _select_related_media_entry_attachment(
        item: MediaItemORM,
        *,
        role: str,
        selected_entry: MediaEntryORM,
        attachments_by_entry_id: dict[str, PlaybackAttachment],
    ) -> PlaybackAttachment | None:
        related_attachments: list[PlaybackAttachment] = []
        for entry in sorted(
            item.media_entries, key=lambda candidate: (candidate.created_at, candidate.id)
        ):
            if not PlaybackSourceService._media_entries_share_provider_identity(
                entry, selected_entry
            ):
                continue
            attachment = attachments_by_entry_id.get(entry.id)
            if attachment is None:
                continue
            if role == _ACTIVE_STREAM_ROLE_DIRECT:
                if attachment.kind == "remote-hls" or _is_degraded_direct_attachment(attachment):
                    continue
            elif attachment.kind not in {"remote-hls", "local-file"}:
                continue
            related_attachments.append(attachment)

        if role == _ACTIVE_STREAM_ROLE_DIRECT:
            return select_direct_playback_attachment(related_attachments)
        return select_hls_playback_attachment(related_attachments)

    @staticmethod
    def _provider_identity_group_key(entry: MediaEntryORM) -> tuple[str, str, str] | None:
        """Return a grouping key for provider-backed direct-entry sibling collapsing."""

        if entry.kind != "remote-direct":
            return None
        provider = entry.provider or ""
        if entry.provider_file_id:
            return ("provider_file_id", provider, entry.provider_file_id)
        if entry.provider_file_path:
            return ("provider_file_path", provider, entry.provider_file_path)
        return None

    @staticmethod
    def _collapse_related_direct_media_entry_attachments(
        item: MediaItemORM,
        *,
        attachments_by_entry_id: dict[str, PlaybackAttachment],
    ) -> list[PlaybackAttachment]:
        """Collapse same-file provider-backed direct-entry sibling groups into one best candidate."""

        collapsed: list[PlaybackAttachment] = []
        emitted_group_keys: set[tuple[str, str, str]] = set()
        ordered_entries = sorted(item.media_entries, key=lambda entry: (entry.created_at, entry.id))

        for entry in ordered_entries:
            attachment = attachments_by_entry_id.get(entry.id)
            if attachment is None:
                continue

            group_key = PlaybackSourceService._provider_identity_group_key(entry)
            if group_key is None:
                collapsed.append(attachment)
                continue
            if group_key in emitted_group_keys:
                continue

            group_attachments = [
                attachments_by_entry_id[candidate.id]
                for candidate in ordered_entries
                if PlaybackSourceService._provider_identity_group_key(candidate) == group_key
                and candidate.id in attachments_by_entry_id
            ]
            selected_attachment = select_direct_playback_attachment(group_attachments)
            if selected_attachment is not None:
                collapsed.append(selected_attachment)
            emitted_group_keys.add(group_key)

        return collapsed

    @staticmethod
    def _select_non_active_direct_media_entry_attachment(
        attachments: list[PlaybackAttachment],
    ) -> PlaybackAttachment | None:
        """Select the best non-active direct media-entry attachment after same-file collapse."""

        return select_direct_playback_attachment(attachments)

    @staticmethod
    def _get_persisted_active_stream_media_entry(
        item: MediaItemORM,
        *,
        role: str,
    ) -> MediaEntryORM | None:
        entries_by_id = {entry.id: entry for entry in item.media_entries}
        for active_stream in sorted(
            item.active_streams,
            key=lambda entry: (entry.created_at, entry.id),
        ):
            if active_stream.role != role:
                continue
            return entries_by_id.get(active_stream.media_entry_id)
        return None

    @staticmethod
    def _find_media_entry_for_attachment(
        item: MediaItemORM,
        *,
        attachment: PlaybackAttachment | None,
        attachments_by_entry_id: dict[str, PlaybackAttachment],
    ) -> MediaEntryORM | None:
        if attachment is None:
            return None

        for entry in sorted(item.media_entries, key=lambda candidate: (candidate.created_at, candidate.id)):
            candidate_attachment = attachments_by_entry_id.get(entry.id)
            if candidate_attachment is None:
                continue
            if candidate_attachment is attachment or candidate_attachment == attachment:
                return entry
        return None
    @staticmethod
    def _build_failed_lease_detail(entry: MediaEntryORM, *, role: str) -> str:
        role_label = "direct" if role == _ACTIVE_STREAM_ROLE_DIRECT else "HLS"
        if entry.last_refresh_error:
            return (
                f"Selected {role_label} playback lease refresh failed: {entry.last_refresh_error}"
            )
        return f"Selected {role_label} playback lease refresh failed"

    @staticmethod
    def _classify_refresh_error(error: str) -> str:
        lowered = error.casefold()
        if "denied" in lowered or "forbidden" in lowered or "unauthor" in lowered:
            return "denied"
        return "failed"

    @staticmethod
    def _observe_resolution_duration(*, surface: str, result: str, started_at: float) -> None:
        PLAYBACK_RESOLUTION_DURATION_SECONDS.labels(surface=surface, result=result).observe(
            perf_counter() - started_at
        )

    @staticmethod
    def _attachment_from_orm(attachment: PlaybackAttachmentORM) -> PlaybackAttachment:
        if attachment.kind not in _PERSISTED_ATTACHMENT_KINDS:
            raise ValueError(f"unsupported playback attachment kind: {attachment.kind}")
        kind = cast(PlaybackAttachmentKind, attachment.kind)
        return PlaybackAttachment(
            kind=kind,
            locator=attachment.locator,
            source_key=attachment.source_key or "persisted",
            resolver_authoritative=False,
            provider=attachment.provider,
            provider_download_id=attachment.provider_download_id,
            provider_file_id=attachment.provider_file_id,
            provider_file_path=attachment.provider_file_path,
            original_filename=attachment.original_filename,
            file_size=attachment.file_size,
            local_path=attachment.local_path,
            restricted_url=attachment.restricted_url,
            unrestricted_url=attachment.unrestricted_url,
            expires_at=attachment.expires_at,
            refresh_state=cast(PlaybackAttachmentRefreshState, attachment.refresh_state),
        )

    def _resolve_persisted_attachments(
        self, item: MediaItemORM
    ) -> tuple[list[PlaybackAttachment], bool]:
        now = datetime.now(UTC)
        attachments = sorted(
            item.playback_attachments,
            key=lambda attachment: (
                attachment.refresh_state == "failed",
                not attachment.is_preferred,
                attachment.preference_rank,
                attachment.created_at,
            ),
        )
        resolved: list[PlaybackAttachment] = []
        saw_missing_path = False

        def build_restricted_fallback(record: PlaybackAttachment) -> PlaybackAttachment | None:
            if not record.restricted_url:
                return None
            fallback_kind: PlaybackAttachmentKind = (
                "remote-hls" if is_hls_playlist_url(record.restricted_url) else "remote-direct"
            )
            return PlaybackAttachment(
                kind=fallback_kind,
                locator=record.restricted_url,
                source_key=f"{record.source_key}:restricted-fallback",
                resolver_authoritative=record.resolver_authoritative,
                provider=record.provider,
                provider_download_id=record.provider_download_id,
                provider_file_id=record.provider_file_id,
                provider_file_path=record.provider_file_path,
                original_filename=record.original_filename,
                file_size=record.file_size,
                local_path=record.local_path,
                restricted_url=record.restricted_url,
                unrestricted_url=record.unrestricted_url,
                expires_at=attachment.expires_at,
                refresh_state=cast(PlaybackAttachmentRefreshState, attachment.refresh_state),
            )

        for attachment in attachments:
            if attachment.refresh_state not in _ATTACHMENT_REFRESH_STATES:
                continue
            record = self._attachment_from_orm(attachment)
            requires_refresh_fallback = attachment.refresh_state in {"stale", "refreshing"}
            if attachment.expires_at is not None and attachment.expires_at <= now:
                requires_refresh_fallback = True
            if attachment.refresh_state == "failed":
                continue
            if requires_refresh_fallback:
                fallback = build_restricted_fallback(record)
                if fallback is None:
                    continue
                record = fallback
            if record.kind == "local-file" and record.local_path is not None:
                if not Path(record.local_path).is_file():
                    saw_missing_path = True
                    continue
                record = PlaybackAttachment(
                    kind=record.kind,
                    locator=record.local_path,
                    source_key=record.source_key,
                    resolver_authoritative=record.resolver_authoritative,
                    provider=record.provider,
                    provider_download_id=record.provider_download_id,
                    provider_file_id=record.provider_file_id,
                    provider_file_path=record.provider_file_path,
                    original_filename=record.original_filename,
                    file_size=record.file_size,
                    local_path=record.local_path,
                    restricted_url=record.restricted_url,
                    unrestricted_url=record.unrestricted_url,
                    expires_at=attachment.expires_at,
                    refresh_state=cast(PlaybackAttachmentRefreshState, attachment.refresh_state),
                )
            resolved.append(record)
        return resolved, saw_missing_path

    @staticmethod
    def _attachment_needs_refresh(attachment: PlaybackAttachmentORM, *, now: datetime) -> bool:
        if attachment.kind == "local-file":
            return False
        if attachment.refresh_state in {"stale", "refreshing"}:
            return True
        if attachment.expires_at is None:
            return False
        return attachment.expires_at <= now

    @staticmethod
    def _media_entry_needs_refresh(entry: MediaEntryORM, *, now: datetime) -> bool:
        if entry.kind == "local-file":
            return False
        if PlaybackSourceService._has_placeholder_debrid_unrestricted_url(
            provider=entry.provider,
            restricted_url=entry.download_url,
            unrestricted_url=entry.unrestricted_url,
        ):
            return True
        if entry.refresh_state in {"stale", "refreshing"}:
            return True
        if entry.refresh_state == "failed":
            return False
        if entry.expires_at is None:
            return False
        return entry.expires_at <= now

    @staticmethod
    def _active_stream_roles_by_media_entry(item: MediaItemORM) -> dict[str, tuple[str, ...]]:
        ordered = sorted(
            item.active_streams,
            key=lambda active_stream: (
                0 if active_stream.role == _ACTIVE_STREAM_ROLE_DIRECT else 1,
                active_stream.created_at,
                active_stream.id,
            ),
        )
        roles_by_entry: dict[str, list[str]] = {}
        for active_stream in ordered:
            roles_by_entry.setdefault(active_stream.media_entry_id, [])
            if active_stream.role not in roles_by_entry[active_stream.media_entry_id]:
                roles_by_entry[active_stream.media_entry_id].append(active_stream.role)
        return {entry_id: tuple(roles) for entry_id, roles in roles_by_entry.items()}

    @staticmethod
    def build_media_entry_refresh_request(
        entry: MediaEntryORM,
        *,
        roles: tuple[str, ...] = (),
    ) -> MediaEntryLeaseRefreshRequest | None:
        if entry.kind == "local-file":
            return None
        if entry.kind not in _PERSISTED_ATTACHMENT_KINDS:
            return None
        if not entry.download_url and not entry.provider_download_id:
            return None
        return MediaEntryLeaseRefreshRequest(
            media_entry_id=entry.id,
            item_id=entry.item_id,
            kind=cast(PlaybackAttachmentKind, entry.kind),
            provider=entry.provider,
            provider_download_id=entry.provider_download_id,
            restricted_url=entry.download_url,
            unrestricted_url=entry.unrestricted_url,
            local_path=entry.local_path,
            refresh_state=entry.refresh_state,
            roles=roles,
            provider_file_id=entry.provider_file_id,
            provider_file_path=entry.provider_file_path,
            original_filename=entry.original_filename,
            file_size=entry.size_bytes,
        )

    @staticmethod
    def _as_attachment_refresh_request(
        request: MediaEntryLeaseRefreshRequest,
    ) -> PlaybackAttachmentRefreshRequest:
        return PlaybackAttachmentRefreshRequest(
            attachment_id=request.media_entry_id,
            item_id=request.item_id,
            kind=request.kind,
            provider=request.provider,
            provider_download_id=request.provider_download_id,
            restricted_url=request.restricted_url,
            unrestricted_url=request.unrestricted_url,
            local_path=request.local_path,
            refresh_state=request.refresh_state,
            provider_file_id=request.provider_file_id,
            provider_file_path=request.provider_file_path,
            original_filename=request.original_filename,
            file_size=request.file_size,
        )

    def plan_media_entry_refresh_requests(
        self, item: MediaItemORM, *, now: datetime | None = None
    ) -> list[MediaEntryLeaseRefreshRequest]:
        reference = now or datetime.now(UTC)
        roles_by_entry = self._active_stream_roles_by_media_entry(item)
        if not roles_by_entry:
            return []

        entries_by_id = {entry.id: entry for entry in item.media_entries}
        requests: list[MediaEntryLeaseRefreshRequest] = []
        for media_entry_id, roles in roles_by_entry.items():
            entry = entries_by_id.get(media_entry_id)
            if entry is None:
                continue
            if not self._media_entry_needs_refresh(entry, now=reference):
                continue
            request = self.build_media_entry_refresh_request(entry, roles=roles)
            if request is not None:
                requests.append(request)
        return requests

    def request_media_entry_refreshes(
        self, item: MediaItemORM, *, at: datetime | None = None
    ) -> list[MediaEntryLeaseRefreshRequest]:
        requested_at = at or datetime.now(UTC)
        requests = self.plan_media_entry_refresh_requests(item, now=requested_at)
        requested_ids = {request.media_entry_id for request in requests}
        for entry in item.media_entries:
            if entry.id in requested_ids:
                self.start_media_entry_refresh(entry, at=requested_at)
        return requests

    @staticmethod
    def apply_media_entry_refresh_result(
        entry: MediaEntryORM,
        result: PlaybackAttachmentRefreshResult,
        *,
        at: datetime | None = None,
    ) -> MediaEntryORM:
        if result.ok:
            download_url = result.restricted_url
            unrestricted_url = result.unrestricted_url
            if (
                unrestricted_url is None
                and result.locator
                and result.locator != result.restricted_url
            ):
                unrestricted_url = result.locator
            if download_url is None and unrestricted_url is None:
                download_url = result.locator
            return PlaybackSourceService.complete_media_entry_refresh(
                entry,
                download_url=download_url,
                unrestricted_url=unrestricted_url,
                expires_at=result.expires_at,
                provider_file_id=result.provider_file_id,
                provider_file_path=result.provider_file_path,
                original_filename=result.original_filename,
                size_bytes=result.file_size,
                at=at,
            )

        return PlaybackSourceService.fail_media_entry_refresh(
            entry,
            error=result.error or "media entry refresh failed",
            at=at,
        )

    async def execute_media_entry_refreshes_with_providers(
        self,
        item: MediaItemORM,
        *,
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        at: datetime | None = None,
    ) -> list[MediaEntryLeaseRefreshExecution]:
        executed: list[MediaEntryLeaseRefreshExecution] = []
        requested_at = at or datetime.now(UTC)
        requests = self.request_media_entry_refreshes(item, at=requested_at)
        entries_by_id = {entry.id: entry for entry in item.media_entries}
        resolved_provider_clients = (
            provider_clients if provider_clients is not None else self._provider_clients
        )

        for request in requests:
            entry = entries_by_id.get(request.media_entry_id)
            if entry is None:
                continue
            attachment_request = self._as_attachment_refresh_request(request)
            executor = self.select_refresh_executor(
                attachment_request,
                executors=executors,
                provider_clients=resolved_provider_clients,
            )
            result = await executor(attachment_request)
            updated = self.apply_media_entry_refresh_result(entry, result, at=requested_at)
            await self._persist_media_entry_projection(updated)
            executed.append(
                MediaEntryLeaseRefreshExecution(
                    media_entry_id=updated.id,
                    ok=result.ok,
                    refresh_state=updated.refresh_state,
                    locator=updated.unrestricted_url or updated.download_url or updated.local_path,
                    error=updated.last_refresh_error,
                )
            )

        return executed

    async def build_playback_governance_snapshot(self) -> dict[str, int]:
        """Return aggregate playback-governance counters for status/observability surfaces."""

        items = await self._list_items()
        reference = datetime.now(UTC)
        snapshot: dict[str, int] = {
            "tracked_media_entries": 0,
            "tracked_active_streams": 0,
            "media_entries_refreshing": 0,
            "media_entries_failed": 0,
            "media_entries_needing_refresh": 0,
            "selected_direct_streams": 0,
            "selected_hls_streams": 0,
            "selected_direct_streams_needing_refresh": 0,
            "selected_hls_streams_needing_refresh": 0,
            "selected_direct_streams_failed": 0,
            "selected_hls_streams_failed": 0,
            **playback_refresh_deferral_governance_snapshot(),
        }

        for item in items:
            snapshot["tracked_media_entries"] += len(item.media_entries)
            snapshot["tracked_active_streams"] += len(item.active_streams)

            for entry in item.media_entries:
                if entry.refresh_state == "refreshing":
                    snapshot["media_entries_refreshing"] += 1
                if entry.refresh_state == "failed":
                    snapshot["media_entries_failed"] += 1
                if self._media_entry_needs_refresh(entry, now=reference):
                    snapshot["media_entries_needing_refresh"] += 1

            direct_entry = self._get_persisted_active_stream_media_entry(
                item,
                role=_ACTIVE_STREAM_ROLE_DIRECT,
            )
            if direct_entry is not None:
                snapshot["selected_direct_streams"] += 1
                if direct_entry.refresh_state == "failed":
                    snapshot["selected_direct_streams_failed"] += 1
                elif self._media_entry_needs_refresh(direct_entry, now=reference):
                    snapshot["selected_direct_streams_needing_refresh"] += 1

            hls_entry = self._get_persisted_active_stream_media_entry(
                item,
                role=_ACTIVE_STREAM_ROLE_HLS,
            )
            if hls_entry is not None:
                snapshot["selected_hls_streams"] += 1
                if hls_entry.refresh_state == "failed":
                    snapshot["selected_hls_streams_failed"] += 1
                elif self._media_entry_needs_refresh(hls_entry, now=reference):
                    snapshot["selected_hls_streams_needing_refresh"] += 1

        return snapshot

    def plan_refresh_requests(
        self, item: MediaItemORM, *, now: datetime | None = None
    ) -> list[PlaybackAttachmentRefreshRequest]:
        """Return ordered refresh requests for one item without mutating attachment state."""

        reference = now or datetime.now(UTC)
        requests: list[PlaybackAttachmentRefreshRequest] = []
        ordered = sorted(
            item.playback_attachments,
            key=lambda attachment: (
                attachment.refresh_state == "failed",
                not attachment.is_preferred,
                attachment.preference_rank,
                attachment.created_at,
            ),
        )
        for attachment in ordered:
            if attachment.refresh_state == "failed":
                continue
            if not self._attachment_needs_refresh(attachment, now=reference):
                continue
            request = self.build_refresh_request(attachment)
            if request is not None:
                requests.append(request)
        return requests

    def request_attachment_refreshes(
        self, item: MediaItemORM, *, at: datetime | None = None
    ) -> list[PlaybackAttachmentRefreshRequest]:
        """Transition refreshable attachments into `refreshing` and return their request payloads."""

        requested_at = at or datetime.now(UTC)
        requests = self.plan_refresh_requests(item, now=requested_at)
        requested_ids = {request.attachment_id for request in requests}
        for attachment in item.playback_attachments:
            if attachment.id in requested_ids:
                self.start_attachment_refresh(attachment, at=requested_at)
        return requests

    @staticmethod
    async def _default_refresh_executor(
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentRefreshResult:
        """Fallback refresh executor used until real provider integrations are wired."""

        if request.unrestricted_url:
            return PlaybackAttachmentRefreshResult(
                ok=True,
                locator=request.unrestricted_url,
                unrestricted_url=request.unrestricted_url,
            )
        if request.restricted_url:
            return PlaybackAttachmentRefreshResult(
                ok=True,
                locator=request.restricted_url,
                unrestricted_url=None,
            )
        return PlaybackAttachmentRefreshResult(ok=False, error="no refreshable locator available")

    @staticmethod
    def select_provider_file_projection(
        request: PlaybackAttachmentRefreshRequest,
        projections: list[PlaybackAttachmentProviderFileProjection],
    ) -> PlaybackAttachmentProviderFileProjection | None:
        """Select the best provider-side file projection for one persisted playback attachment request."""

        if not projections:
            return None

        provider_file_id = request.provider_file_id
        if provider_file_id is not None:
            matches = [
                projection
                for projection in projections
                if projection.provider_file_id == provider_file_id
            ]
            if len(matches) == 1:
                return matches[0]

        provider_file_path = request.provider_file_path
        if provider_file_path is not None:
            matches = [
                projection
                for projection in projections
                if projection.provider_file_path == provider_file_path
            ]
            if len(matches) == 1:
                return matches[0]

        original_filename = request.original_filename
        file_size = request.file_size
        if original_filename is not None and file_size is not None:
            matches = [
                projection
                for projection in projections
                if projection.original_filename == original_filename
                and projection.file_size == file_size
            ]
            if len(matches) == 1:
                return matches[0]

        if original_filename is not None:
            matches = [
                projection
                for projection in projections
                if projection.original_filename == original_filename
            ]
            if len(matches) == 1:
                return matches[0]

        if file_size is not None:
            matches = [
                projection for projection in projections if projection.file_size == file_size
            ]
            if len(matches) == 1:
                return matches[0]

        if len(projections) == 1:
            return projections[0]

        return None

    def build_provider_client_refresh_executor(
        self,
        *,
        provider: str,
        client: PlaybackAttachmentProviderClient,
    ) -> PlaybackAttachmentRefreshExecutor:
        """Wrap one provider client into a concrete playback-attachment refresh executor."""

        async def executor(
            request: PlaybackAttachmentRefreshRequest,
        ) -> PlaybackAttachmentRefreshResult:
            if self._provider_circuit_breaker.is_open(provider):
                return PlaybackAttachmentRefreshResult(
                    ok=False,
                    error=f"{provider} circuit open",
                )

            def _provider_failure(error: str) -> PlaybackAttachmentRefreshResult:
                self._provider_circuit_breaker.record_failure(provider)
                return PlaybackAttachmentRefreshResult(ok=False, error=error)

            def _provider_success(
                result: PlaybackAttachmentRefreshResult,
            ) -> PlaybackAttachmentRefreshResult:
                self._provider_circuit_breaker.record_success(provider)
                return result

            if request.provider != provider:
                actual = request.provider or "none"
                return PlaybackAttachmentRefreshResult(
                    ok=False,
                    error=(
                        f"provider mismatch for refresh executor: expected {provider}, got {actual}"
                    ),
                )
            try:
                restricted_url = request.restricted_url
                if restricted_url:
                    unrestricted = await client.unrestrict_link(restricted_url, request=request)
                    if unrestricted is None:
                        return _provider_failure(f"{provider} did not return an unrestricted link")
                    download_url = unrestricted.download_url
                    if not download_url:
                        return _provider_failure(f"{provider} did not return an unrestricted link")

                    return _provider_success(
                        PlaybackAttachmentRefreshResult(
                            ok=True,
                            locator=download_url,
                            restricted_url=unrestricted.restricted_url,
                            unrestricted_url=download_url,
                            expires_at=unrestricted.expires_at,
                        )
                    )

                if request.provider_download_id and isinstance(
                    client, PlaybackAttachmentProviderProjectionClient
                ):
                    projections = await client.project_download_attachments(request=request)
                    projection = self.select_provider_file_projection(request, projections)
                    if projection is None:
                        return _provider_failure(
                            f"{provider} did not return a matching projected file"
                        )
                    if projection.unrestricted_url:
                        return _provider_success(
                            PlaybackAttachmentRefreshResult(
                                ok=True,
                                locator=projection.unrestricted_url,
                                restricted_url=projection.restricted_url,
                                unrestricted_url=projection.unrestricted_url,
                                provider_file_id=projection.provider_file_id,
                                provider_file_path=projection.provider_file_path,
                                original_filename=projection.original_filename,
                                file_size=projection.file_size,
                            )
                        )

                    unrestricted = await client.unrestrict_link(
                        projection.restricted_url,
                        request=request,
                    )
                    if unrestricted is None:
                        return _provider_failure(f"{provider} did not return an unrestricted link")
                    download_url = unrestricted.download_url
                    if not download_url:
                        return _provider_failure(f"{provider} did not return an unrestricted link")

                    return _provider_success(
                        PlaybackAttachmentRefreshResult(
                            ok=True,
                            locator=download_url,
                            restricted_url=unrestricted.restricted_url or projection.restricted_url,
                            unrestricted_url=download_url,
                            expires_at=unrestricted.expires_at,
                            provider_file_id=projection.provider_file_id,
                            provider_file_path=projection.provider_file_path,
                            original_filename=projection.original_filename,
                            file_size=projection.file_size,
                        )
                    )

                if request.provider_download_id and isinstance(
                    client,
                    PlaybackAttachmentProviderDownloadClient,
                ):
                    unrestricted = await client.refresh_download(request=request)
                    if unrestricted is None:
                        return _provider_failure(f"{provider} did not return an unrestricted link")
                    download_url = unrestricted.download_url
                    if not download_url:
                        return _provider_failure(f"{provider} did not return an unrestricted link")

                    return _provider_success(
                        PlaybackAttachmentRefreshResult(
                            ok=True,
                            locator=download_url,
                            restricted_url=unrestricted.restricted_url,
                            unrestricted_url=download_url,
                            expires_at=unrestricted.expires_at,
                        )
                    )
            except Exception as exc:  # pragma: no cover - defensive guard for provider clients.
                return _provider_failure(f"{provider} refresh failed: {exc}")

            return _provider_failure(
                f"{provider} refresh requires restricted_url or provider_download_id"
            )

        return executor

    def select_refresh_executor(
        self,
        request: PlaybackAttachmentRefreshRequest,
        *,
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
    ) -> PlaybackAttachmentRefreshExecutor:
        """Return the best refresh executor for one persisted attachment refresh request."""

        provider = request.provider
        if provider is not None and executors and provider in executors:
            return executors[provider]
        if provider is not None and provider_clients and provider in provider_clients:
            return self.build_provider_client_refresh_executor(
                provider=provider,
                client=provider_clients[provider],
            )
        return self._default_refresh_executor

    async def execute_attachment_refreshes(
        self,
        item: MediaItemORM,
        *,
        executor: PlaybackAttachmentRefreshExecutor,
        at: datetime | None = None,
    ) -> list[PlaybackAttachmentRefreshExecution]:
        """Plan, request, execute, and apply refresh results for one item's attachments."""

        executed: list[PlaybackAttachmentRefreshExecution] = []
        requested_at = at or datetime.now(UTC)
        requests = self.request_attachment_refreshes(item, at=requested_at)
        attachments_by_id = {attachment.id: attachment for attachment in item.playback_attachments}

        for request in requests:
            attachment = attachments_by_id.get(request.attachment_id)
            if attachment is None:
                continue
            result = await executor(request)
            updated = self.apply_refresh_result(attachment, result, at=requested_at)
            await self._persist_attachment_projection(updated)
            executed.append(
                PlaybackAttachmentRefreshExecution(
                    attachment_id=updated.id,
                    ok=result.ok,
                    refresh_state=updated.refresh_state,
                    locator=updated.locator,
                    error=updated.last_refresh_error,
                )
            )

        return executed

    async def execute_attachment_refreshes_with_providers(
        self,
        item: MediaItemORM,
        *,
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        at: datetime | None = None,
    ) -> list[PlaybackAttachmentRefreshExecution]:
        """Execute planned refreshes using provider executors or provider clients when available."""

        executed: list[PlaybackAttachmentRefreshExecution] = []
        requested_at = at or datetime.now(UTC)
        requests = self.request_attachment_refreshes(item, at=requested_at)
        attachments_by_id = {attachment.id: attachment for attachment in item.playback_attachments}
        resolved_provider_clients = (
            provider_clients if provider_clients is not None else self._provider_clients
        )

        for request in requests:
            attachment = attachments_by_id.get(request.attachment_id)
            if attachment is None:
                continue
            executor = self.select_refresh_executor(
                request,
                executors=executors,
                provider_clients=resolved_provider_clients,
            )
            result = await executor(request)
            updated = self.apply_refresh_result(attachment, result, at=requested_at)
            await self._persist_attachment_projection(updated)
            executed.append(
                PlaybackAttachmentRefreshExecution(
                    attachment_id=updated.id,
                    ok=result.ok,
                    refresh_state=updated.refresh_state,
                    locator=updated.locator,
                    error=updated.last_refresh_error,
                )
            )
        return executed

    @staticmethod
    def _resolve_mounted_media_entry_local_path(
        item: MediaItemORM,
        entry: MediaEntryORM,
        *,
        settings: Settings,
    ) -> str | None:
        library_path = settings.updaters.library_path.strip()
        if not library_path:
            return None

        filename_candidate = (
            entry.original_filename
            or entry.provider_file_path
            or entry.local_path
            or entry.unrestricted_url
            or entry.download_url
        )
        if filename_candidate is None:
            return None

        from filmu_py.services.vfs_catalog import FilmuVfsCatalogSupplier

        basename = FilmuVfsCatalogSupplier._basename_from_candidate(filename_candidate)
        if basename is None:
            return None
        safe_filename = FilmuVfsCatalogSupplier._sanitize_path_segment(basename)
        media_type = FilmuVfsCatalogSupplier._normalize_media_type(item)
        candidate_path = FilmuVfsCatalogSupplier._build_candidate_path_for_entry(
            item,
            media_entry=entry,
            media_type=media_type,
            filename=safe_filename,
        )
        candidate_parts = [part for part in candidate_path.split("/") if part]
        mounted_path = Path(library_path, *candidate_parts)
        if not mounted_path.is_file():
            return None
        return str(mounted_path)

    @staticmethod
    def _build_mounted_local_attachment(
        item: MediaItemORM,
        *,
        entry: MediaEntryORM | None,
        settings: Settings | None,
    ) -> PlaybackAttachment | None:
        if entry is None or settings is None:
            return None

        local_path = PlaybackSourceService._resolve_mounted_media_entry_local_path(
            item,
            entry,
            settings=settings,
        )
        if local_path is None:
            return None

        return PlaybackAttachment(
            kind="local-file",
            locator=local_path,
            source_key="media-entry",
            provider=entry.provider,
            provider_download_id=entry.provider_download_id,
            provider_file_id=entry.provider_file_id,
            provider_file_path=entry.provider_file_path,
            original_filename=entry.original_filename,
            file_size=entry.size_bytes,
            local_path=local_path,
            restricted_url=entry.download_url,
            unrestricted_url=entry.unrestricted_url,
            expires_at=entry.expires_at,
            refresh_state=cast(PlaybackAttachmentRefreshState, entry.refresh_state),
        )

    def build_resolution_snapshot(self, item: MediaItemORM) -> PlaybackResolutionSnapshot:
        """Return the best current direct/HLS playback attachments for one item without raising HTTP errors."""

        media_entry_attachments, media_entry_by_id, saw_missing_path = (
            self._resolve_persisted_media_entry_attachments(item)
        )
        if media_entry_attachments:
            collapsed_direct_media_entry_attachments = (
                self._collapse_related_direct_media_entry_attachments(
                    item,
                    attachments_by_entry_id=media_entry_by_id,
                )
            )
            direct_attachment = self._select_persisted_active_stream_attachment(
                item,
                role=_ACTIVE_STREAM_ROLE_DIRECT,
                attachments_by_entry_id=media_entry_by_id,
            ) or self._select_non_active_direct_media_entry_attachment(
                collapsed_direct_media_entry_attachments
            )
            hls_attachment = self._select_persisted_active_stream_attachment(
                item,
                role=_ACTIVE_STREAM_ROLE_HLS,
                attachments_by_entry_id=media_entry_by_id,
            ) or select_hls_playback_attachment(media_entry_attachments)
            mounted_local_hls_entry = (
                self._get_persisted_active_stream_media_entry(
                    item,
                    role=_ACTIVE_STREAM_ROLE_HLS,
                )
                or self._get_persisted_active_stream_media_entry(
                    item,
                    role=_ACTIVE_STREAM_ROLE_DIRECT,
                )
                or self._find_media_entry_for_attachment(
                    item,
                    attachment=hls_attachment,
                    attachments_by_entry_id=media_entry_by_id,
                )
                or self._find_media_entry_for_attachment(
                    item,
                    attachment=direct_attachment,
                    attachments_by_entry_id=media_entry_by_id,
                )
            )
            mounted_local_hls_attachment = self._build_mounted_local_attachment(
                item,
                entry=mounted_local_hls_entry,
                settings=self._settings,
            )
            if mounted_local_hls_attachment is not None and (
                hls_attachment is None or hls_attachment.kind == "remote-direct"
            ):
                hls_attachment = mounted_local_hls_attachment
            return PlaybackResolutionSnapshot(
                direct=direct_attachment,
                hls=hls_attachment,
                direct_ready=direct_attachment is not None,
                hls_ready=hls_attachment is not None,
                direct_lifecycle=(
                    self.build_direct_file_link_lifecycle(direct_attachment, item=item)
                    if direct_attachment is not None
                    else None
                ),
                hls_lifecycle=(
                    self.build_direct_file_link_lifecycle(hls_attachment, item=item)
                    if hls_attachment is not None
                    else None
                ),
                missing_local_file=saw_missing_path,
            )

        attachments, saw_missing_path = self._resolve_persisted_attachments(item)
        if not attachments:
            attributes = cast(dict[str, object], item.attributes or {})
            attachments, saw_missing_path = resolve_attachments_from_attributes(attributes)

        direct_attachment = select_direct_playback_attachment(attachments)
        hls_attachment = select_hls_playback_attachment(attachments)
        return PlaybackResolutionSnapshot(
            direct=direct_attachment,
            hls=hls_attachment,
            direct_ready=direct_attachment is not None,
            hls_ready=hls_attachment is not None,
            direct_lifecycle=(
                self.build_direct_file_link_lifecycle(direct_attachment, item=item)
                if direct_attachment is not None
                else None
            ),
            hls_lifecycle=(
                self.build_direct_file_link_lifecycle(hls_attachment, item=item)
                if hls_attachment is not None
                else None
            ),
            missing_local_file=saw_missing_path,
        )

    def build_direct_playback_decision(self, item: MediaItemORM) -> DirectPlaybackDecision:
        """Return the explicit direct-play route policy decision for one item."""

        selected_entry = self._get_persisted_active_stream_media_entry(
            item,
            role=_ACTIVE_STREAM_ROLE_DIRECT,
        )
        if selected_entry is not None and selected_entry.refresh_state == "failed":
            return DirectPlaybackDecision(
                action="fail",
                result="failed_lease",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=self._build_failed_lease_detail(
                    selected_entry,
                    role=_ACTIVE_STREAM_ROLE_DIRECT,
                ),
                refresh_intent=True,
                refresh_recommendation=DirectPlaybackRefreshRecommendation(
                    reason="selected_failed_lease",
                    target="media_entry",
                    target_id=selected_entry.id,
                    provider=selected_entry.provider,
                    provider_download_id=selected_entry.provider_download_id,
                    provider_file_id=selected_entry.provider_file_id,
                    provider_file_path=selected_entry.provider_file_path,
                    restricted_url=selected_entry.download_url,
                    refresh_state=cast(
                        PlaybackAttachmentRefreshState, selected_entry.refresh_state
                    ),
                ),
            )

        snapshot = self.build_resolution_snapshot(item)
        direct_attachment = snapshot.direct
        if direct_attachment is not None:
            source_class = classify_direct_playback_source_class(direct_attachment)
            refresh_recommendation = self._build_direct_playback_refresh_recommendation(
                direct_attachment,
                source_class,
            )
            return DirectPlaybackDecision(
                action="serve",
                result="resolved",
                attachment=direct_attachment,
                source_class=source_class,
                refresh_intent=refresh_recommendation is not None,
                refresh_recommendation=refresh_recommendation,
            )

        if snapshot.missing_local_file:
            return DirectPlaybackDecision(
                action="fail",
                result="missing_local_file",
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Resolved playback file is missing",
            )

        return DirectPlaybackDecision(
            action="fail",
            result="no_source",
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No playback source available for item",
        )

    @staticmethod
    def _recommendation_matches_media_entry(
        entry: MediaEntryORM,
        recommendation: DirectPlaybackRefreshRecommendation,
    ) -> bool:
        if recommendation.target_id is not None:
            return entry.id == recommendation.target_id
        if (
            recommendation.provider_download_id
            and entry.provider_download_id == recommendation.provider_download_id
        ):
            return True
        if (
            recommendation.provider_file_id
            and entry.provider_file_id == recommendation.provider_file_id
        ):
            return True
        if (
            recommendation.provider_file_path
            and entry.provider_file_path == recommendation.provider_file_path
        ):
            return True
        return bool(
            recommendation.restricted_url and entry.download_url == recommendation.restricted_url
        )

    @staticmethod
    def _recommendation_matches_attachment(
        attachment: PlaybackAttachmentORM,
        recommendation: DirectPlaybackRefreshRecommendation,
    ) -> bool:
        if recommendation.target_id is not None:
            return attachment.id == recommendation.target_id
        if (
            recommendation.provider_download_id
            and attachment.provider_download_id == recommendation.provider_download_id
        ):
            return True
        if (
            recommendation.provider_file_id
            and attachment.provider_file_id == recommendation.provider_file_id
        ):
            return True
        if (
            recommendation.provider_file_path
            and attachment.provider_file_path == recommendation.provider_file_path
        ):
            return True
        return bool(
            recommendation.restricted_url
            and attachment.restricted_url == recommendation.restricted_url
        )

    def build_direct_playback_refresh_dispatch(
        self,
        item: MediaItemORM,
        decision: DirectPlaybackDecision,
    ) -> DirectPlaybackRefreshDispatch | None:
        """Translate one direct-play decision into existing refresh-request models when possible."""

        recommendation = decision.refresh_recommendation
        if recommendation is None:
            return None

        if recommendation.target == "media_entry":
            roles_by_entry = self._active_stream_roles_by_media_entry(item)
            for entry in item.media_entries:
                if not self._recommendation_matches_media_entry(entry, recommendation):
                    continue
                media_entry_request = self.build_media_entry_refresh_request(
                    entry,
                    roles=roles_by_entry.get(entry.id, ()),
                )
                return DirectPlaybackRefreshDispatch(
                    recommendation=recommendation,
                    media_entry_request=media_entry_request,
                )
            return DirectPlaybackRefreshDispatch(recommendation=recommendation)

        if recommendation.target == "attachment":
            for attachment in item.playback_attachments:
                if not self._recommendation_matches_attachment(attachment, recommendation):
                    continue
                attachment_request = self.build_refresh_request(attachment)
                return DirectPlaybackRefreshDispatch(
                    recommendation=recommendation,
                    attachment_request=attachment_request,
                )
            return DirectPlaybackRefreshDispatch(recommendation=recommendation)

        return DirectPlaybackRefreshDispatch(recommendation=recommendation)

    @staticmethod
    def build_direct_playback_refresh_schedule_request(
        item_identifier: str,
        recommendation: DirectPlaybackRefreshRecommendation,
        *,
        requested_at: datetime | None = None,
        retry_after_seconds: float | None = None,
    ) -> DirectPlaybackRefreshScheduleRequest:
        """Build one background refresh schedule request from direct-play guidance."""

        scheduled_at = requested_at or datetime.now(UTC)
        normalized_retry_after = None
        if retry_after_seconds is not None:
            normalized_retry_after = max(0.0, retry_after_seconds)

        not_before = scheduled_at
        if normalized_retry_after is not None:
            not_before = scheduled_at + timedelta(seconds=normalized_retry_after)

        return DirectPlaybackRefreshScheduleRequest(
            item_identifier=item_identifier,
            recommendation=recommendation,
            requested_at=scheduled_at,
            not_before=not_before,
            retry_after_seconds=normalized_retry_after,
        )

    @staticmethod
    def _build_refresh_rate_limit_bucket_key(provider: str) -> str:
        return f"ratelimit:{provider}:stream_link_refresh"

    async def _acquire_playback_refresh_rate_limit(
        self,
        request: PlaybackAttachmentRefreshRequest,
        *,
        surface: str,
        rate_limiter: PlaybackRefreshRateLimiter | None,
    ) -> tuple[str | None, RateLimitDecision | None]:
        limiter = rate_limiter
        if limiter is None:
            return None, None

        provider = request.provider
        if provider is None:
            return None, None

        bucket_key = self._build_refresh_rate_limit_bucket_key(provider)
        decision = await limiter.acquire(
            bucket_key=bucket_key,
            capacity=_PLAYBACK_REFRESH_RATE_LIMIT_CAPACITY,
            refill_rate_per_second=_PLAYBACK_REFRESH_RATE_LIMIT_REFILL_PER_SECOND,
        )
        if not decision.allowed:
            PLAYBACK_RISK_EVENTS.labels(surface=surface, reason="refresh_rate_limited").inc()
        return bucket_key, decision

    @staticmethod
    def _infer_direct_playback_refresh_target(
        attachment: PlaybackAttachment,
    ) -> DirectPlaybackRefreshRecommendationTarget:
        if attachment.source_key.startswith("media-entry"):
            return "media_entry"
        if attachment.source_key.startswith("persisted"):
            return "attachment"
        return "metadata"

    @staticmethod
    def _build_direct_playback_refresh_recommendation(
        attachment: PlaybackAttachment,
        source_class: DirectPlaybackSourceClass,
    ) -> DirectPlaybackRefreshRecommendation | None:
        reason_by_class: dict[
            DirectPlaybackSourceClass, DirectPlaybackRefreshRecommendationReason
        ] = {
            "selected-provider-direct-stale": "provider_direct_stale",
            "selected-provider-direct-refreshing": "provider_direct_refreshing",
            "selected-provider-direct-failed": "provider_direct_failed",
            "selected-provider-direct-degraded": "provider_direct_degraded",
            "fallback-provider-direct-stale": "provider_direct_stale",
            "fallback-provider-direct-refreshing": "provider_direct_refreshing",
            "fallback-provider-direct-failed": "provider_direct_failed",
            "fallback-provider-direct-degraded": "provider_direct_degraded",
            "selected-degraded-direct": "degraded_direct",
            "fallback-degraded-direct": "degraded_direct",
        }
        reason = reason_by_class.get(source_class)
        if reason is None:
            return None
        return DirectPlaybackRefreshRecommendation(
            reason=reason,
            target=PlaybackSourceService._infer_direct_playback_refresh_target(attachment),
            source_class=source_class,
            provider=attachment.provider,
            provider_download_id=attachment.provider_download_id,
            provider_file_id=attachment.provider_file_id,
            provider_file_path=attachment.provider_file_path,
            restricted_url=attachment.restricted_url,
            refresh_state=attachment.refresh_state,
        )

    async def _resolve_direct_playback_item_decision(
        self,
        item_identifier: str,
    ) -> tuple[MediaItemORM | None, DirectPlaybackDecision]:
        """Resolve one direct-play decision plus the matched persisted item when available."""

        for item in await self._list_items():
            if self._matches_identifier(item, item_identifier):
                return item, self.build_direct_playback_decision(item)
        return None, DirectPlaybackDecision(
            action="fail",
            result="no_source",
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No playback source available for item",
        )

    async def resolve_direct_playback_decision(
        self, item_identifier: str
    ) -> DirectPlaybackDecision:
        """Resolve the explicit direct-play route policy decision for one item identifier."""

        return (await self._resolve_direct_playback_item_decision(item_identifier))[1]

    async def resolve_direct_playback_refresh_dispatch(
        self, item_identifier: str
    ) -> DirectPlaybackRefreshDispatch | None:
        """Resolve one direct-play decision and translate its refresh recommendation when possible."""

        item, decision = await self._resolve_direct_playback_item_decision(item_identifier)
        if item is None:
            return None
        return self.build_direct_playback_refresh_dispatch(item, decision)

    async def prepare_direct_playback_refresh_schedule_request(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> DirectPlaybackRefreshScheduleRequest | None:
        """Prepare one non-blocking background refresh request for direct playback."""

        dispatch = await self.resolve_direct_playback_refresh_dispatch(item_identifier)
        if dispatch is None:
            return None

        return self.build_direct_playback_refresh_schedule_request(
            item_identifier,
            dispatch.recommendation,
            requested_at=at,
        )

    async def schedule_direct_playback_refresh(
        self,
        item_identifier: str,
        *,
        scheduler: DirectPlaybackRefreshScheduler,
        at: datetime | None = None,
    ) -> DirectPlaybackRefreshSchedulingResult:
        """Schedule one direct-play refresh request without executing provider work inline."""

        request = await self.prepare_direct_playback_refresh_schedule_request(
            item_identifier, at=at
        )
        if request is None:
            return DirectPlaybackRefreshSchedulingResult(outcome="no_action")

        await scheduler.schedule(request)
        return DirectPlaybackRefreshSchedulingResult(
            outcome="scheduled",
            scheduled_request=request,
        )

    async def execute_direct_playback_refresh_dispatch_with_providers(
        self,
        item: MediaItemORM,
        dispatch: DirectPlaybackRefreshDispatch,
        *,
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: PlaybackRefreshRateLimiter | None = None,
        at: datetime | None = None,
    ) -> DirectPlaybackRefreshDispatchExecution:
        """Execute one translated direct-play refresh dispatch outside the HTTP request path."""

        requested_at = at or datetime.now(UTC)
        resolved_provider_clients = (
            provider_clients if provider_clients is not None else self._provider_clients
        )
        media_entries_by_id = {entry.id: entry for entry in item.media_entries}
        attachments_by_id = {attachment.id: attachment for attachment in item.playback_attachments}

        media_entry_request = dispatch.media_entry_request
        if media_entry_request is not None:
            entry = media_entries_by_id.get(media_entry_request.media_entry_id)
            if entry is None:
                return DirectPlaybackRefreshDispatchExecution(
                    recommendation=dispatch.recommendation
                )
            provider = media_entry_request.provider
            if provider is not None and self._provider_circuit_breaker.is_open(provider):
                retry_after = self._provider_circuit_breaker.retry_after_seconds(provider)
                PLAYBACK_RISK_EVENTS.labels(
                    surface=_ACTIVE_STREAM_ROLE_DIRECT,
                    reason="provider_circuit_open",
                ).inc()
                self._record_direct_playback_refresh_deferral(
                    reason="provider_circuit_open",
                )
                return DirectPlaybackRefreshDispatchExecution(
                    recommendation=dispatch.recommendation,
                    rate_limited=True,
                    retry_after_seconds=retry_after,
                    deferred_reason="provider_circuit_open",
                )
            attachment_request = self._as_attachment_refresh_request(media_entry_request)
            bucket_key, rate_limit_decision = await self._acquire_playback_refresh_rate_limit(
                attachment_request,
                surface=_ACTIVE_STREAM_ROLE_DIRECT,
                rate_limiter=rate_limiter,
            )
            if rate_limit_decision is not None and not rate_limit_decision.allowed:
                self._record_direct_playback_refresh_deferral(
                    reason="refresh_rate_limited",
                )
                return DirectPlaybackRefreshDispatchExecution(
                    recommendation=dispatch.recommendation,
                    rate_limited=True,
                    retry_after_seconds=rate_limit_decision.retry_after_seconds,
                    limiter_bucket_key=bucket_key,
                    deferred_reason="refresh_rate_limited",
                )
            previous_refresh_state = entry.refresh_state
            previous_last_refresh_error = entry.last_refresh_error
            previous_last_refreshed_at = entry.last_refreshed_at
            previous_updated_at = entry.updated_at
            self.start_media_entry_refresh(entry, at=requested_at)
            executor = self.select_refresh_executor(
                attachment_request,
                executors=executors,
                provider_clients=resolved_provider_clients,
            )
            result = await executor(attachment_request)
            if (
                not result.ok
                and provider is not None
                and result.error is not None
                and "circuit open" in result.error.casefold()
            ):
                entry.refresh_state = previous_refresh_state
                entry.last_refresh_error = previous_last_refresh_error
                entry.last_refreshed_at = previous_last_refreshed_at
                entry.updated_at = previous_updated_at
                self._sync_source_attachment_from_media_entry(entry, at=entry.updated_at)
                await self._persist_media_entry_projection(entry)
                retry_after = self._provider_circuit_breaker.retry_after_seconds(provider)
                PLAYBACK_RISK_EVENTS.labels(
                    surface=_ACTIVE_STREAM_ROLE_DIRECT,
                    reason="provider_circuit_open",
                ).inc()
                self._record_direct_playback_refresh_deferral(
                    reason="provider_circuit_open",
                )
                return DirectPlaybackRefreshDispatchExecution(
                    recommendation=dispatch.recommendation,
                    rate_limited=True,
                    retry_after_seconds=retry_after,
                    limiter_bucket_key=bucket_key,
                    deferred_reason="provider_circuit_open",
                )
            updated = self.apply_media_entry_refresh_result(entry, result, at=requested_at)
            await self._persist_media_entry_projection(updated)
            return DirectPlaybackRefreshDispatchExecution(
                recommendation=dispatch.recommendation,
                media_entry_execution=MediaEntryLeaseRefreshExecution(
                    media_entry_id=updated.id,
                    ok=result.ok,
                    refresh_state=updated.refresh_state,
                    locator=updated.unrestricted_url or updated.download_url or updated.local_path,
                    error=updated.last_refresh_error,
                ),
            )

        dispatch_attachment_request = dispatch.attachment_request
        if dispatch_attachment_request is not None:
            attachment = attachments_by_id.get(dispatch_attachment_request.attachment_id)
            if attachment is None:
                return DirectPlaybackRefreshDispatchExecution(
                    recommendation=dispatch.recommendation
                )
            provider = dispatch_attachment_request.provider
            if provider is not None and self._provider_circuit_breaker.is_open(provider):
                retry_after = self._provider_circuit_breaker.retry_after_seconds(provider)
                PLAYBACK_RISK_EVENTS.labels(
                    surface=_ACTIVE_STREAM_ROLE_DIRECT,
                    reason="provider_circuit_open",
                ).inc()
                self._record_direct_playback_refresh_deferral(
                    reason="provider_circuit_open",
                )
                return DirectPlaybackRefreshDispatchExecution(
                    recommendation=dispatch.recommendation,
                    rate_limited=True,
                    retry_after_seconds=retry_after,
                    deferred_reason="provider_circuit_open",
                )
            bucket_key, rate_limit_decision = await self._acquire_playback_refresh_rate_limit(
                dispatch_attachment_request,
                surface=_ACTIVE_STREAM_ROLE_DIRECT,
                rate_limiter=rate_limiter,
            )
            if rate_limit_decision is not None and not rate_limit_decision.allowed:
                self._record_direct_playback_refresh_deferral(
                    reason="refresh_rate_limited",
                )
                return DirectPlaybackRefreshDispatchExecution(
                    recommendation=dispatch.recommendation,
                    rate_limited=True,
                    retry_after_seconds=rate_limit_decision.retry_after_seconds,
                    limiter_bucket_key=bucket_key,
                    deferred_reason="refresh_rate_limited",
                )
            previous_refresh_state = attachment.refresh_state
            previous_last_refresh_error = attachment.last_refresh_error
            previous_last_refreshed_at = attachment.last_refreshed_at
            previous_updated_at = attachment.updated_at
            self.start_attachment_refresh(attachment, at=requested_at)
            executor = self.select_refresh_executor(
                dispatch_attachment_request,
                executors=executors,
                provider_clients=resolved_provider_clients,
            )
            result = await executor(dispatch_attachment_request)
            if (
                not result.ok
                and provider is not None
                and result.error is not None
                and "circuit open" in result.error.casefold()
            ):
                attachment.refresh_state = previous_refresh_state
                attachment.last_refresh_error = previous_last_refresh_error
                attachment.last_refreshed_at = previous_last_refreshed_at
                attachment.updated_at = previous_updated_at
                await self._persist_attachment_projection(attachment)
                retry_after = self._provider_circuit_breaker.retry_after_seconds(provider)
                PLAYBACK_RISK_EVENTS.labels(
                    surface=_ACTIVE_STREAM_ROLE_DIRECT,
                    reason="provider_circuit_open",
                ).inc()
                self._record_direct_playback_refresh_deferral(
                    reason="provider_circuit_open",
                )
                return DirectPlaybackRefreshDispatchExecution(
                    recommendation=dispatch.recommendation,
                    rate_limited=True,
                    retry_after_seconds=retry_after,
                    limiter_bucket_key=bucket_key,
                    deferred_reason="provider_circuit_open",
                )
            updated_attachment = self.apply_refresh_result(attachment, result, at=requested_at)
            await self._persist_attachment_projection(updated_attachment)
            return DirectPlaybackRefreshDispatchExecution(
                recommendation=dispatch.recommendation,
                attachment_execution=PlaybackAttachmentRefreshExecution(
                    attachment_id=updated_attachment.id,
                    ok=result.ok,
                    refresh_state=updated_attachment.refresh_state,
                    locator=updated_attachment.locator,
                    error=updated_attachment.last_refresh_error,
                ),
            )

        return DirectPlaybackRefreshDispatchExecution(recommendation=dispatch.recommendation)

    async def resolve_direct_playback_refresh_dispatch_with_providers(
        self,
        item_identifier: str,
        *,
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: PlaybackRefreshRateLimiter | None = None,
        at: datetime | None = None,
    ) -> DirectPlaybackRefreshDispatchExecution | None:
        """Resolve and execute one direct-play refresh dispatch outside the HTTP request path."""

        for item in await self._list_items():
            if not self._matches_identifier(item, item_identifier):
                continue
            decision = self.build_direct_playback_decision(item)
            dispatch = self.build_direct_playback_refresh_dispatch(item, decision)
            if dispatch is None:
                return None
            return await self.execute_direct_playback_refresh_dispatch_with_providers(
                item,
                dispatch,
                executors=executors,
                provider_clients=provider_clients,
                rate_limiter=rate_limiter,
                at=at,
            )
        return None

    async def execute_scheduled_direct_playback_refresh_with_providers(
        self,
        request: DirectPlaybackRefreshScheduleRequest,
        *,
        scheduler: DirectPlaybackRefreshScheduler | None = None,
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: PlaybackRefreshRateLimiter | None = None,
        at: datetime | None = None,
    ) -> DirectPlaybackRefreshSchedulingResult:
        """Execute one scheduled direct-play refresh request with optional retry-aware rescheduling."""

        requested_at = at or datetime.now(UTC)
        if request.not_before > requested_at:
            retry_after_seconds = (request.not_before - requested_at).total_seconds()
            return DirectPlaybackRefreshSchedulingResult(
                outcome="run_later",
                scheduled_request=request,
                retry_after_seconds=retry_after_seconds,
            )

        execution = await self.resolve_direct_playback_refresh_dispatch_with_providers(
            request.item_identifier,
            executors=executors,
            provider_clients=provider_clients,
            rate_limiter=rate_limiter,
            at=requested_at,
        )
        if execution is None:
            return DirectPlaybackRefreshSchedulingResult(outcome="no_action")

        if execution.rate_limited:
            scheduled_request = self.build_direct_playback_refresh_schedule_request(
                request.item_identifier,
                execution.recommendation,
                requested_at=requested_at,
                retry_after_seconds=execution.retry_after_seconds,
            )
            if scheduler is not None:
                await scheduler.schedule(scheduled_request)
                return DirectPlaybackRefreshSchedulingResult(
                    outcome="scheduled",
                    execution=execution,
                    scheduled_request=scheduled_request,
                    retry_after_seconds=execution.retry_after_seconds,
                )

            return DirectPlaybackRefreshSchedulingResult(
                outcome="run_later",
                execution=execution,
                scheduled_request=scheduled_request,
                retry_after_seconds=execution.retry_after_seconds,
            )

        return DirectPlaybackRefreshSchedulingResult(
            outcome="completed",
            execution=execution,
        )

    @staticmethod
    def _record_selected_hls_refresh_deferral(
        *,
        trigger: Literal["failed_lease", "restricted_fallback"],
        reason: PlaybackRefreshDeferredReason,
    ) -> None:
        """Record one selected-HLS background refresh deferral for status and metrics."""

        SELECTED_HLS_REFRESH_DEFERRALS.labels(trigger=trigger, reason=reason).inc()
        record_selected_hls_refresh_deferral(trigger=trigger, reason=reason)

    @staticmethod
    def _record_direct_playback_refresh_deferral(
        *,
        reason: PlaybackRefreshDeferredReason,
    ) -> None:
        """Record one direct-play background refresh deferral for status visibility."""

        record_direct_playback_refresh_deferral(reason=reason)

    async def _execute_selected_hls_media_entry_refresh_with_providers(
        self,
        *,
        item: MediaItemORM,
        selected_entry: MediaEntryORM,
        trigger: Literal["failed_lease", "restricted_fallback"],
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: PlaybackRefreshRateLimiter | None = None,
        at: datetime | None = None,
    ) -> SelectedHlsRefreshExecutionResult:
        """Execute one selected-HLS media-entry refresh with shared provider-pressure handling."""

        requested_at = at or datetime.now(UTC)
        resolved_provider_clients = (
            provider_clients if provider_clients is not None else self._provider_clients
        )
        roles_by_entry = self._active_stream_roles_by_media_entry(item)
        media_entry_request = self.build_media_entry_refresh_request(
            selected_entry,
            roles=roles_by_entry.get(selected_entry.id, ()),
        )
        if media_entry_request is None:
            return SelectedHlsRefreshExecutionResult(outcome="no_action")

        attachment_request = self._as_attachment_refresh_request(media_entry_request)
        provider = attachment_request.provider
        if provider is not None and self._provider_circuit_breaker.is_open(provider):
            retry_after = self._provider_circuit_breaker.retry_after_seconds(provider)
            PLAYBACK_RISK_EVENTS.labels(
                surface=_ACTIVE_STREAM_ROLE_HLS,
                reason="provider_circuit_open",
            ).inc()
            self._record_selected_hls_refresh_deferral(
                trigger=trigger,
                reason="provider_circuit_open",
            )
            return SelectedHlsRefreshExecutionResult(
                outcome="run_later",
                retry_after_seconds=retry_after,
                deferred_reason="provider_circuit_open",
            )

        bucket_key, rate_limit_decision = await self._acquire_playback_refresh_rate_limit(
            attachment_request,
            surface=_ACTIVE_STREAM_ROLE_HLS,
            rate_limiter=rate_limiter,
        )
        if rate_limit_decision is not None and not rate_limit_decision.allowed:
            self._record_selected_hls_refresh_deferral(
                trigger=trigger,
                reason="refresh_rate_limited",
            )
            return SelectedHlsRefreshExecutionResult(
                outcome="run_later",
                retry_after_seconds=rate_limit_decision.retry_after_seconds,
                limiter_bucket_key=bucket_key,
                deferred_reason="refresh_rate_limited",
            )

        previous_refresh_state = selected_entry.refresh_state
        previous_last_refresh_error = selected_entry.last_refresh_error
        previous_last_refreshed_at = selected_entry.last_refreshed_at
        previous_updated_at = selected_entry.updated_at

        self.start_media_entry_refresh(selected_entry, at=requested_at)
        executor = self.select_refresh_executor(
            attachment_request,
            executors=executors,
            provider_clients=resolved_provider_clients,
        )
        result = await executor(attachment_request)
        if (
            not result.ok
            and provider is not None
            and result.error is not None
            and "circuit open" in result.error.casefold()
        ):
            selected_entry.refresh_state = previous_refresh_state
            selected_entry.last_refresh_error = previous_last_refresh_error
            selected_entry.last_refreshed_at = previous_last_refreshed_at
            selected_entry.updated_at = previous_updated_at
            self._sync_source_attachment_from_media_entry(selected_entry, at=selected_entry.updated_at)
            await self._persist_media_entry_projection(selected_entry)
            retry_after = self._provider_circuit_breaker.retry_after_seconds(provider)
            PLAYBACK_RISK_EVENTS.labels(
                surface=_ACTIVE_STREAM_ROLE_HLS,
                reason="provider_circuit_open",
            ).inc()
            self._record_selected_hls_refresh_deferral(
                trigger=trigger,
                reason="provider_circuit_open",
            )
            return SelectedHlsRefreshExecutionResult(
                outcome="run_later",
                retry_after_seconds=retry_after,
                limiter_bucket_key=bucket_key,
                deferred_reason="provider_circuit_open",
            )

        updated = self.apply_media_entry_refresh_result(selected_entry, result, at=requested_at)
        await self._persist_media_entry_projection(updated)
        return SelectedHlsRefreshExecutionResult(
            outcome="completed",
            execution=MediaEntryLeaseRefreshExecution(
                media_entry_id=updated.id,
                ok=result.ok,
                refresh_state=updated.refresh_state,
                locator=updated.unrestricted_url or updated.download_url or updated.local_path,
                error=updated.last_refresh_error,
            ),
            limiter_bucket_key=bucket_key,
        )

    async def execute_selected_hls_failed_lease_refresh_with_providers(
        self,
        item_identifier: str,
        *,
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: PlaybackRefreshRateLimiter | None = None,
        at: datetime | None = None,
    ) -> HlsFailedLeaseRefreshResult:
        """Execute one selected-HLS failed-lease refresh outside the HTTP request path."""

        requested_at = at or datetime.now(UTC)

        for item in await self._list_items():
            if not self._matches_identifier(item, item_identifier):
                continue

            selected_entry = self._get_persisted_active_stream_media_entry(
                item,
                role=_ACTIVE_STREAM_ROLE_HLS,
            )
            if selected_entry is None or selected_entry.refresh_state != "failed":
                return HlsFailedLeaseRefreshResult(
                    item_identifier=item_identifier,
                    outcome="no_action",
                )

            execution = await self._execute_selected_hls_media_entry_refresh_with_providers(
                item=item,
                selected_entry=selected_entry,
                trigger="failed_lease",
                executors=executors,
                provider_clients=provider_clients,
                rate_limiter=rate_limiter,
                at=requested_at,
            )
            return HlsFailedLeaseRefreshResult(
                item_identifier=item_identifier,
                outcome=execution.outcome,
                execution=execution.execution,
                retry_after_seconds=execution.retry_after_seconds,
                limiter_bucket_key=execution.limiter_bucket_key,
                deferred_reason=execution.deferred_reason,
            )

        return HlsFailedLeaseRefreshResult(
            item_identifier=item_identifier,
            outcome="no_action",
        )

    async def execute_selected_hls_restricted_fallback_refresh_with_providers(
        self,
        item_identifier: str,
        *,
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: PlaybackRefreshRateLimiter | None = None,
        at: datetime | None = None,
    ) -> HlsRestrictedFallbackRefreshResult:
        """Execute one selected-HLS restricted-fallback refresh outside the HTTP request path."""

        requested_at = at or datetime.now(UTC)

        for item in await self._list_items():
            if not self._matches_identifier(item, item_identifier):
                continue

            selected_entry = self._get_persisted_active_stream_media_entry(
                item,
                role=_ACTIVE_STREAM_ROLE_HLS,
            )
            if selected_entry is None or selected_entry.kind != "remote-hls":
                return HlsRestrictedFallbackRefreshResult(
                    item_identifier=item_identifier,
                    outcome="no_action",
                )
            if selected_entry.refresh_state == "failed" or not self._media_entry_needs_refresh(
                selected_entry,
                now=requested_at,
            ):
                return HlsRestrictedFallbackRefreshResult(
                    item_identifier=item_identifier,
                    outcome="no_action",
                )

            resolved_attachment = self.build_resolution_snapshot(item).hls
            if (
                resolved_attachment is None
                or resolved_attachment.kind != "remote-hls"
                or resolved_attachment.source_key != "media-entry:restricted-fallback"
            ):
                return HlsRestrictedFallbackRefreshResult(
                    item_identifier=item_identifier,
                    outcome="no_action",
                )

            execution = await self._execute_selected_hls_media_entry_refresh_with_providers(
                item=item,
                selected_entry=selected_entry,
                trigger="restricted_fallback",
                executors=executors,
                provider_clients=provider_clients,
                rate_limiter=rate_limiter,
                at=requested_at,
            )
            return HlsRestrictedFallbackRefreshResult(
                item_identifier=item_identifier,
                outcome=execution.outcome,
                execution=execution.execution,
                retry_after_seconds=execution.retry_after_seconds,
                limiter_bucket_key=execution.limiter_bucket_key,
                deferred_reason=execution.deferred_reason,
            )

        return HlsRestrictedFallbackRefreshResult(
            item_identifier=item_identifier,
            outcome="no_action",
        )

    @staticmethod
    def build_refresh_request(
        attachment: PlaybackAttachmentORM,
    ) -> PlaybackAttachmentRefreshRequest | None:
        """Build the minimal refresh request payload for one persisted attachment when possible."""

        if attachment.kind == "local-file":
            return None
        if not attachment.restricted_url and not attachment.provider_download_id:
            return None
        if attachment.kind not in _PERSISTED_ATTACHMENT_KINDS:
            return None
        return PlaybackAttachmentRefreshRequest(
            attachment_id=attachment.id,
            item_id=attachment.item_id,
            kind=cast(PlaybackAttachmentKind, attachment.kind),
            provider=attachment.provider,
            provider_download_id=attachment.provider_download_id,
            provider_file_id=attachment.provider_file_id,
            provider_file_path=attachment.provider_file_path,
            restricted_url=attachment.restricted_url,
            unrestricted_url=attachment.unrestricted_url,
            local_path=attachment.local_path,
            refresh_state=attachment.refresh_state,
            original_filename=attachment.original_filename,
            file_size=attachment.file_size,
        )

    @staticmethod
    def apply_refresh_result(
        attachment: PlaybackAttachmentORM,
        result: PlaybackAttachmentRefreshResult,
        *,
        at: datetime | None = None,
    ) -> PlaybackAttachmentORM:
        """Apply one refresh result to a persisted attachment using the service transition rules."""

        if result.ok:
            locator = result.locator or attachment.locator
            return PlaybackSourceService.complete_attachment_refresh(
                attachment,
                locator=locator,
                restricted_url=result.restricted_url,
                unrestricted_url=result.unrestricted_url,
                expires_at=result.expires_at,
                provider_file_id=result.provider_file_id,
                provider_file_path=result.provider_file_path,
                original_filename=result.original_filename,
                file_size=result.file_size,
                at=at,
            )

        return PlaybackSourceService.fail_attachment_refresh(
            attachment,
            error=result.error or "refresh failed",
            at=at,
        )

    @staticmethod
    def mark_attachment_stale(
        attachment: PlaybackAttachmentORM, *, at: datetime | None = None
    ) -> PlaybackAttachmentORM:
        """Mark one persisted attachment as stale so a refresh flow can pick it up."""

        attachment.refresh_state = "stale"
        attachment.updated_at = at or datetime.now(UTC)
        return attachment

    @staticmethod
    def start_attachment_refresh(
        attachment: PlaybackAttachmentORM, *, at: datetime | None = None
    ) -> PlaybackAttachmentORM:
        """Move one persisted attachment into the refreshing state."""

        attachment.refresh_state = "refreshing"
        attachment.updated_at = at or datetime.now(UTC)
        return attachment

    @staticmethod
    def complete_attachment_refresh(
        attachment: PlaybackAttachmentORM,
        *,
        locator: str,
        restricted_url: str | None = None,
        unrestricted_url: str | None,
        expires_at: datetime | None,
        provider_file_id: str | None = None,
        provider_file_path: str | None = None,
        original_filename: str | None = None,
        file_size: int | None = None,
        at: datetime | None = None,
    ) -> PlaybackAttachmentORM:
        """Apply a successful refresh result to one persisted attachment."""

        refreshed_at = at or datetime.now(UTC)
        attachment.refresh_state = "ready"
        attachment.locator = locator
        if restricted_url is not None:
            attachment.restricted_url = restricted_url
        attachment.unrestricted_url = unrestricted_url
        attachment.expires_at = expires_at
        if provider_file_id is not None:
            attachment.provider_file_id = provider_file_id
        if provider_file_path is not None:
            attachment.provider_file_path = provider_file_path
        if original_filename is not None:
            attachment.original_filename = original_filename
        if file_size is not None:
            attachment.file_size = file_size
        attachment.last_refreshed_at = refreshed_at
        attachment.last_refresh_error = None
        attachment.updated_at = refreshed_at
        return attachment

    @staticmethod
    def fail_attachment_refresh(
        attachment: PlaybackAttachmentORM,
        *,
        error: str,
        at: datetime | None = None,
    ) -> PlaybackAttachmentORM:
        """Record a failed refresh attempt for one persisted attachment."""

        failed_at = at or datetime.now(UTC)
        attachment.refresh_state = "failed"
        attachment.last_refresh_error = error
        attachment.last_refreshed_at = failed_at
        attachment.updated_at = failed_at
        PLAYBACK_LEASE_REFRESH_FAILURES.labels(
            record_type="attachment",
            reason=PlaybackSourceService._classify_refresh_error(error),
        ).inc()
        return attachment

    @staticmethod
    def mark_media_entry_stale(
        entry: MediaEntryORM, *, at: datetime | None = None
    ) -> MediaEntryORM:
        """Mark one persisted media entry as stale so lease refresh can pick it up."""

        entry.refresh_state = "stale"
        entry.updated_at = at or datetime.now(UTC)
        PlaybackSourceService._sync_source_attachment_from_media_entry(entry, at=entry.updated_at)
        return entry

    @staticmethod
    def start_media_entry_refresh(
        entry: MediaEntryORM, *, at: datetime | None = None
    ) -> MediaEntryORM:
        """Move one persisted media entry into the refreshing state."""

        entry.refresh_state = "refreshing"
        entry.updated_at = at or datetime.now(UTC)
        PlaybackSourceService._sync_source_attachment_from_media_entry(entry, at=entry.updated_at)
        return entry

    @staticmethod
    def complete_media_entry_refresh(
        entry: MediaEntryORM,
        *,
        download_url: str | None = None,
        unrestricted_url: str | None = None,
        expires_at: datetime | None = None,
        local_path: str | None = None,
        provider_file_id: str | None = None,
        provider_file_path: str | None = None,
        original_filename: str | None = None,
        size_bytes: int | None = None,
        at: datetime | None = None,
    ) -> MediaEntryORM:
        """Apply a successful lease refresh result to one persisted media entry."""

        refreshed_at = at or datetime.now(UTC)
        entry.refresh_state = "ready"
        if download_url is not None:
            entry.download_url = download_url
        if unrestricted_url is not None:
            entry.unrestricted_url = unrestricted_url
        if local_path is not None:
            entry.local_path = local_path
        if provider_file_id is not None:
            entry.provider_file_id = provider_file_id
        if provider_file_path is not None:
            entry.provider_file_path = provider_file_path
        if original_filename is not None:
            entry.original_filename = original_filename
        if size_bytes is not None:
            entry.size_bytes = size_bytes
        entry.expires_at = expires_at
        entry.last_refreshed_at = refreshed_at
        entry.last_refresh_error = None
        entry.updated_at = refreshed_at
        PlaybackSourceService._sync_source_attachment_from_media_entry(entry, at=refreshed_at)
        return entry

    @staticmethod
    def fail_media_entry_refresh(
        entry: MediaEntryORM,
        *,
        error: str,
        at: datetime | None = None,
    ) -> MediaEntryORM:
        """Record a failed lease refresh attempt for one persisted media entry."""

        failed_at = at or datetime.now(UTC)
        entry.refresh_state = "failed"
        entry.last_refresh_error = error
        entry.last_refreshed_at = failed_at
        entry.updated_at = failed_at
        PlaybackSourceService._sync_source_attachment_from_media_entry(entry, at=failed_at)
        PLAYBACK_LEASE_REFRESH_FAILURES.labels(
            record_type="media_entry",
            reason=PlaybackSourceService._classify_refresh_error(error),
        ).inc()
        return entry

    async def _resolve_direct_playback_serving_decision(
        self,
        item_identifier: str,
    ) -> tuple[MediaItemORM | None, DirectPlaybackDecision]:
        """Resolve one serveable direct-play decision or raise the stable HTTP contract."""

        started_at = perf_counter()
        item, decision = await self._resolve_direct_playback_item_decision(item_identifier)
        if decision.action == "serve":
            self._observe_resolution_duration(
                surface=_ACTIVE_STREAM_ROLE_DIRECT,
                result=decision.result,
                started_at=started_at,
            )
            if decision.attachment is None:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Direct playback decision resolved without attachment",
                )
            return item, decision

        if decision.result == "failed_lease":
            PLAYBACK_RISK_EVENTS.labels(
                surface=_ACTIVE_STREAM_ROLE_DIRECT,
                reason="selected_failed_lease",
            ).inc()

        self._observe_resolution_duration(
            surface=_ACTIVE_STREAM_ROLE_DIRECT,
            result=decision.result,
            started_at=started_at,
        )
        raise HTTPException(
            status_code=decision.status_code or status.HTTP_404_NOT_FOUND,
            detail=decision.detail or "No playback source available for item",
        )

    async def resolve_playback_attachment(self, item_identifier: str) -> PlaybackAttachment:
        """Resolve one attachment for the direct playback route."""

        _, decision = await self._resolve_direct_playback_serving_decision(item_identifier)
        attachment = decision.attachment
        if attachment is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Direct playback decision resolved without attachment",
            )
        return attachment

    async def resolve_direct_file_link_resolution(
        self,
        item_identifier: str,
        *,
        force_refresh: bool = False,
    ) -> DirectFileLinkResolution:
        """Resolve one internal direct-file link-resolution result for the HTTP compatibility route."""

        started_at = perf_counter()
        item, decision = await self._resolve_direct_playback_item_decision(item_identifier)
        if item is None:
            self._observe_resolution_duration(
                surface=_ACTIVE_STREAM_ROLE_DIRECT,
                result="no_source",
                started_at=started_at,
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No playback source available for item",
            )

        selected_entry = self._get_persisted_active_stream_media_entry(
            item,
            role=_ACTIVE_STREAM_ROLE_DIRECT,
        )
        reference = datetime.now(UTC)
        should_attempt_selected_entry_resolution = (
            selected_entry is not None
            and selected_entry.kind != "local-file"
            and self._link_resolver.can_resolve_media_entry(selected_entry)
            and (
                force_refresh
                or selected_entry.refresh_state == "failed"
                or self._media_entry_needs_refresh(selected_entry, now=reference)
            )
        )
        if should_attempt_selected_entry_resolution and selected_entry is not None:
            selected_entry_contract_detail = self._build_failed_lease_detail(
                selected_entry,
                role=_ACTIVE_STREAM_ROLE_DIRECT,
            )
            selected_entry_refresh_state = selected_entry.refresh_state
            selected_entry_last_refreshed_at = selected_entry.last_refreshed_at
            selected_entry_last_refresh_error = selected_entry.last_refresh_error
            selected_entry_updated_at = selected_entry.updated_at
            fallback_resolution = (
                self.build_direct_file_link_resolution(decision, item=item)
                if decision.action == "serve" and decision.attachment is not None
                else None
            )
            roles = self._active_stream_roles_by_media_entry(item).get(selected_entry.id, ())
            outcome = await self._link_resolver.resolve_media_entry(
                selected_entry,
                roles=roles,
                surface=_ACTIVE_STREAM_ROLE_DIRECT,
                force_refresh=force_refresh,
                at=reference,
            )
            if outcome.attachment is not None and (outcome.refreshed or decision.result != "failed_lease"):
                resolved_attachment = outcome.attachment
                self._observe_resolution_duration(
                    surface=_ACTIVE_STREAM_ROLE_DIRECT,
                    result="resolved",
                    started_at=started_at,
                )
                return self.build_direct_file_link_resolution(
                    DirectPlaybackDecision(
                        action="serve",
                        result="resolved",
                        attachment=resolved_attachment,
                        source_class=classify_direct_playback_source_class(resolved_attachment),
                    ),
                    item=item,
                )

            if fallback_resolution is not None:
                selected_entry.refresh_state = selected_entry_refresh_state
                selected_entry.last_refreshed_at = selected_entry_last_refreshed_at
                selected_entry.last_refresh_error = selected_entry_last_refresh_error
                selected_entry.updated_at = selected_entry_updated_at
                self._observe_resolution_duration(
                    surface=_ACTIVE_STREAM_ROLE_DIRECT,
                    result=decision.result,
                    started_at=started_at,
                )
                return fallback_resolution

            if decision.result == "failed_lease":
                PLAYBACK_RISK_EVENTS.labels(
                    surface=_ACTIVE_STREAM_ROLE_DIRECT,
                    reason="selected_failed_lease",
                ).inc()
                self._observe_resolution_duration(
                    surface=_ACTIVE_STREAM_ROLE_DIRECT,
                    result="failed_lease",
                    started_at=started_at,
                )
                fresh_failure_detail = outcome.detail or self._build_failed_lease_detail(
                    selected_entry,
                    role=_ACTIVE_STREAM_ROLE_DIRECT,
                )
                if fresh_failure_detail != selected_entry_contract_detail:
                    logger.warning(
                        "direct playback selected lease refresh failed with updated provider detail",
                        extra={
                            "item_id": item.id,
                            "media_entry_id": selected_entry.id,
                            "provider": selected_entry.provider,
                            "stable_detail": selected_entry_contract_detail,
                            "fresh_detail": fresh_failure_detail,
                            "last_refresh_error": selected_entry.last_refresh_error,
                        },
                    )
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=selected_entry_contract_detail,
                )

            self._observe_resolution_duration(
                surface=_ACTIVE_STREAM_ROLE_DIRECT,
                result="failed_lease",
                started_at=started_at,
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    outcome.detail
                    or self._build_failed_lease_detail(
                        selected_entry,
                        role=_ACTIVE_STREAM_ROLE_DIRECT,
                    )
                ),
            )

        if decision.action == "serve":
            self._observe_resolution_duration(
                surface=_ACTIVE_STREAM_ROLE_DIRECT,
                result=decision.result,
                started_at=started_at,
            )
            if decision.attachment is None:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Direct playback decision resolved without attachment",
                )
            return self.build_direct_file_link_resolution(decision, item=item)

        if decision.result == "failed_lease":
            PLAYBACK_RISK_EVENTS.labels(
                surface=_ACTIVE_STREAM_ROLE_DIRECT,
                reason="selected_failed_lease",
            ).inc()

        self._observe_resolution_duration(
            surface=_ACTIVE_STREAM_ROLE_DIRECT,
            result=decision.result,
            started_at=started_at,
        )
        raise HTTPException(
            status_code=decision.status_code or status.HTTP_404_NOT_FOUND,
            detail=decision.detail or "No playback source available for item",
        )

    async def resolve_direct_file_serving_descriptor(
        self,
        item_identifier: str,
        *,
        force_refresh: bool = False,
    ) -> DirectFileServingDescriptor:
        """Resolve one typed direct-file serving descriptor for the HTTP compatibility route."""

        resolution = await self.resolve_direct_file_link_resolution(
            item_identifier,
            force_refresh=force_refresh,
        )
        return self.build_direct_file_serving_descriptor(resolution)

    async def resolve_hls_attachment(
        self,
        item_identifier: str,
        *,
        force_refresh: bool = False,
    ) -> PlaybackAttachment:
        """Resolve one attachment for the HLS route family."""

        started_at = perf_counter()
        for item in await self._list_items():
            if not self._matches_identifier(item, item_identifier):
                continue

            selected_entry = self._get_persisted_active_stream_media_entry(
                item,
                role=_ACTIVE_STREAM_ROLE_HLS,
            )
            if (
                selected_entry is not None
                and selected_entry.refresh_state == "failed"
                and not force_refresh
            ):
                PLAYBACK_RISK_EVENTS.labels(
                    surface=_ACTIVE_STREAM_ROLE_HLS,
                    reason="selected_failed_lease",
                ).inc()
                self._observe_resolution_duration(
                    surface=_ACTIVE_STREAM_ROLE_HLS,
                    result="failed_lease",
                    started_at=started_at,
                )
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=self._build_failed_lease_detail(
                        selected_entry,
                        role=_ACTIVE_STREAM_ROLE_HLS,
                    ),
                )

            snapshot = self.build_resolution_snapshot(item)
            hls_attachment = snapshot.hls
            if (
                force_refresh
                and selected_entry is not None
                and selected_entry.kind != "local-file"
                and self._link_resolver.can_resolve_media_entry(selected_entry)
                and hls_attachment is not None
                and hls_attachment.source_key.startswith("media-entry")
            ):
                roles = self._active_stream_roles_by_media_entry(item).get(selected_entry.id, ())
                outcome = await self._link_resolver.resolve_media_entry(
                    selected_entry,
                    roles=roles,
                    surface=_ACTIVE_STREAM_ROLE_HLS,
                    force_refresh=True,
                )
                if outcome.attachment is not None:
                    self._observe_resolution_duration(
                        surface=_ACTIVE_STREAM_ROLE_HLS,
                        result="resolved",
                        started_at=started_at,
                    )
                    return outcome.attachment
            if hls_attachment is not None:
                self._observe_resolution_duration(
                    surface=_ACTIVE_STREAM_ROLE_HLS,
                    result="resolved",
                    started_at=started_at,
                )
                return hls_attachment

            direct_attachment = snapshot.direct
            if direct_attachment is not None and direct_attachment.kind == "remote-direct":
                self._observe_resolution_duration(
                    surface=_ACTIVE_STREAM_ROLE_HLS,
                    result="resolved",
                    started_at=started_at,
                )
                return direct_attachment

            if snapshot.missing_local_file:
                self._observe_resolution_duration(
                    surface=_ACTIVE_STREAM_ROLE_HLS,
                    result="missing_local_file",
                    started_at=started_at,
                )
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Resolved playback file is missing",
                )

            candidate_attachments = self._resolve_persisted_attachments(item)[0]
            if not candidate_attachments:
                attributes = cast(dict[str, object], item.attributes or {})
                candidate_attachments, _ = resolve_attachments_from_attributes(attributes)

            self._observe_resolution_duration(
                surface=_ACTIVE_STREAM_ROLE_HLS,
                result="no_source",
                started_at=started_at,
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No playback source available for item",
            )

        self._observe_resolution_duration(
            surface=_ACTIVE_STREAM_ROLE_HLS,
            result="no_source",
            started_at=started_at,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No playback source available for item",
        )

    async def mark_selected_hls_media_entry_stale(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> bool:
        """Persist the selected HLS media entry as stale so background refresh can pick it up."""

        requested_at = at or datetime.now(UTC)
        for item in await self._list_items():
            if not self._matches_identifier(item, item_identifier):
                continue

            selected_entry = self._get_persisted_active_stream_media_entry(
                item,
                role=_ACTIVE_STREAM_ROLE_HLS,
            )
            if selected_entry is None or selected_entry.kind != "remote-hls":
                return False
            if selected_entry.refresh_state == "failed":
                return False
            if selected_entry.refresh_state == "ready":
                self.mark_media_entry_stale(selected_entry, at=requested_at)
                await self._persist_media_entry_projection(selected_entry)
            return True

        return False


class InProcessDirectPlaybackRefreshController(DirectPlaybackRefreshScheduler):
    """Small in-process caller that triggers scheduled direct-play refresh work in the background."""

    def __init__(
        self,
        playback_service: PlaybackSourceService,
        *,
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: PlaybackRefreshRateLimiter | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._playback_service = playback_service
        self._executors = executors
        self._provider_clients = provider_clients
        self._rate_limiter = rate_limiter
        self._sleep = sleep or asyncio.sleep
        self._tasks_by_item_identifier: dict[str, asyncio.Task[None]] = {}
        self._last_results_by_item_identifier: dict[str, DirectPlaybackRefreshSchedulingResult] = {}

    def _register_task(self, item_identifier: str, task: asyncio.Task[None]) -> None:
        self._tasks_by_item_identifier[item_identifier] = task

        def cleanup(done_task: asyncio.Task[None], *, key: str = item_identifier) -> None:
            current = self._tasks_by_item_identifier.get(key)
            if current is done_task:
                self._tasks_by_item_identifier.pop(key, None)

        task.add_done_callback(cleanup)

    def has_pending(self, item_identifier: str) -> bool:
        """Return whether one item currently has pending in-process direct-play refresh work."""

        task = self._tasks_by_item_identifier.get(item_identifier)
        return task is not None and not task.done()

    def get_last_result(self, item_identifier: str) -> DirectPlaybackRefreshSchedulingResult | None:
        """Return the latest scheduling/execution result observed for one item identifier."""

        return self._last_results_by_item_identifier.get(item_identifier)

    async def wait_for_item(self, item_identifier: str) -> None:
        """Await the currently pending background refresh task for one item when present."""

        while True:
            task = self._tasks_by_item_identifier.get(item_identifier)
            if task is None:
                return
            await task
            current = self._tasks_by_item_identifier.get(item_identifier)
            if current is task and task.done():
                self._tasks_by_item_identifier.pop(item_identifier, None)

    async def shutdown(self) -> None:
        """Cancel and drain any pending in-process direct-play refresh tasks."""

        tasks = [task for task in self._tasks_by_item_identifier.values() if not task.done()]
        if not tasks:
            self._tasks_by_item_identifier.clear()
            return

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks_by_item_identifier.clear()

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> DirectPlaybackRefreshControlPlaneTriggerResult:
        """Trigger one in-process direct-play refresh without blocking the request path."""

        if self.has_pending(item_identifier):
            return DirectPlaybackRefreshControlPlaneTriggerResult(
                item_identifier=item_identifier,
                outcome="already_pending",
                scheduling_result=self.get_last_result(item_identifier),
            )

        scheduling_result = await self._playback_service.schedule_direct_playback_refresh(
            item_identifier,
            scheduler=self,
            at=at,
        )
        self._last_results_by_item_identifier[item_identifier] = scheduling_result

        if scheduling_result.outcome != "scheduled" or scheduling_result.scheduled_request is None:
            return DirectPlaybackRefreshControlPlaneTriggerResult(
                item_identifier=item_identifier,
                outcome="no_action",
                scheduling_result=scheduling_result,
            )

        return DirectPlaybackRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled",
            scheduling_result=scheduling_result,
            scheduled_request=scheduling_result.scheduled_request,
        )

    async def schedule(self, request: DirectPlaybackRefreshScheduleRequest) -> None:
        """Schedule one direct-play refresh request to execute in-process later."""

        if self.has_pending(request.item_identifier):
            return

        task = asyncio.create_task(
            self._run_request(request),
            name=f"direct-play-refresh:{request.item_identifier}",
        )
        self._register_task(request.item_identifier, task)

    async def _run_request(self, request: DirectPlaybackRefreshScheduleRequest) -> None:
        now = datetime.now(UTC)
        effective_run_at = request.not_before if request.not_before > now else now
        delay_seconds = max(0.0, (effective_run_at - now).total_seconds())
        if delay_seconds > 0.0:
            await self._sleep(delay_seconds)

        result = (
            await self._playback_service.execute_scheduled_direct_playback_refresh_with_providers(
                request,
                scheduler=None,
                executors=self._executors,
                provider_clients=self._provider_clients,
                rate_limiter=self._rate_limiter,
                at=effective_run_at,
            )
        )
        self._last_results_by_item_identifier[request.item_identifier] = result

        follow_up_request = result.scheduled_request
        if follow_up_request is None:
            return

        current_task = asyncio.current_task()
        if current_task is not None:
            current = self._tasks_by_item_identifier.get(request.item_identifier)
            if current is current_task:
                self._tasks_by_item_identifier.pop(request.item_identifier, None)

        await self.schedule(follow_up_request)


class InProcessHlsFailedLeaseRefreshController:
    """Small in-process caller that refreshes selected failed HLS leases in the background."""

    def __init__(
        self,
        playback_service: PlaybackSourceService,
        *,
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: PlaybackRefreshRateLimiter | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._playback_service = playback_service
        self._executors = executors
        self._provider_clients = provider_clients
        self._rate_limiter = rate_limiter
        self._sleep = sleep or asyncio.sleep
        self._tasks_by_item_identifier: dict[str, asyncio.Task[None]] = {}
        self._last_results_by_item_identifier: dict[str, HlsFailedLeaseRefreshResult] = {}

    def _register_task(self, item_identifier: str, task: asyncio.Task[None]) -> None:
        self._tasks_by_item_identifier[item_identifier] = task

        def cleanup(done_task: asyncio.Task[None], *, key: str = item_identifier) -> None:
            current = self._tasks_by_item_identifier.get(key)
            if current is done_task:
                self._tasks_by_item_identifier.pop(key, None)

        task.add_done_callback(cleanup)

    def has_pending(self, item_identifier: str) -> bool:
        """Return whether one item currently has pending in-process HLS failed-lease refresh work."""

        task = self._tasks_by_item_identifier.get(item_identifier)
        return task is not None and not task.done()

    def get_last_result(self, item_identifier: str) -> HlsFailedLeaseRefreshResult | None:
        """Return the latest selected-HLS failed-lease refresh result observed for one item."""

        return self._last_results_by_item_identifier.get(item_identifier)

    async def wait_for_item(self, item_identifier: str) -> None:
        """Await the currently pending background HLS failed-lease refresh task when present."""

        while True:
            task = self._tasks_by_item_identifier.get(item_identifier)
            if task is None:
                return
            await task
            current = self._tasks_by_item_identifier.get(item_identifier)
            if current is task and task.done():
                self._tasks_by_item_identifier.pop(item_identifier, None)

    async def shutdown(self) -> None:
        """Cancel and drain any pending in-process HLS failed-lease refresh tasks."""

        tasks = [task for task in self._tasks_by_item_identifier.values() if not task.done()]
        if not tasks:
            self._tasks_by_item_identifier.clear()
            return

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks_by_item_identifier.clear()

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> HlsFailedLeaseRefreshControlPlaneTriggerResult:
        """Trigger one in-process selected-HLS failed-lease refresh without blocking the route path."""

        if self.has_pending(item_identifier):
            return HlsFailedLeaseRefreshControlPlaneTriggerResult(
                item_identifier=item_identifier,
                outcome="already_pending",
                refresh_result=self.get_last_result(item_identifier),
            )

        task = asyncio.create_task(
            self._run_item(item_identifier, at=at),
            name=f"hls-failed-lease-refresh:{item_identifier}",
        )
        self._register_task(item_identifier, task)
        return HlsFailedLeaseRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled",
        )

    async def _run_item(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        requested_at = at or datetime.now(UTC)
        normalized_retry_after = max(0.0, retry_after_seconds or 0.0)
        if normalized_retry_after > 0.0:
            await self._sleep(normalized_retry_after)
            requested_at = requested_at + timedelta(seconds=normalized_retry_after)

        result = (
            await self._playback_service.execute_selected_hls_failed_lease_refresh_with_providers(
                item_identifier,
                executors=self._executors,
                provider_clients=self._provider_clients,
                rate_limiter=self._rate_limiter,
                at=requested_at,
            )
        )
        self._last_results_by_item_identifier[item_identifier] = result

        if result.outcome != "run_later" or result.retry_after_seconds is None:
            return

        current_task = asyncio.current_task()
        if current_task is not None:
            current = self._tasks_by_item_identifier.get(item_identifier)
            if current is current_task:
                self._tasks_by_item_identifier.pop(item_identifier, None)

        follow_up_task = asyncio.create_task(
            self._run_item(
                item_identifier,
                at=requested_at,
                retry_after_seconds=result.retry_after_seconds,
            ),
            name=f"hls-failed-lease-refresh:{item_identifier}",
        )
        self._register_task(item_identifier, follow_up_task)


class InProcessHlsRestrictedFallbackRefreshController:
    """Small in-process caller that refreshes selected HLS restricted-fallback winners in the background."""

    def __init__(
        self,
        playback_service: PlaybackSourceService,
        *,
        executors: dict[str, PlaybackAttachmentRefreshExecutor] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: PlaybackRefreshRateLimiter | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._playback_service = playback_service
        self._executors = executors
        self._provider_clients = provider_clients
        self._rate_limiter = rate_limiter
        self._sleep = sleep or asyncio.sleep
        self._tasks_by_item_identifier: dict[str, asyncio.Task[None]] = {}
        self._last_results_by_item_identifier: dict[str, HlsRestrictedFallbackRefreshResult] = {}

    def _register_task(self, item_identifier: str, task: asyncio.Task[None]) -> None:
        self._tasks_by_item_identifier[item_identifier] = task

        def cleanup(done_task: asyncio.Task[None], *, key: str = item_identifier) -> None:
            current = self._tasks_by_item_identifier.get(key)
            if current is done_task:
                self._tasks_by_item_identifier.pop(key, None)

        task.add_done_callback(cleanup)

    def has_pending(self, item_identifier: str) -> bool:
        """Return whether one item currently has pending in-process HLS restricted-fallback refresh work."""

        task = self._tasks_by_item_identifier.get(item_identifier)
        return task is not None and not task.done()

    def get_last_result(self, item_identifier: str) -> HlsRestrictedFallbackRefreshResult | None:
        """Return the latest selected-HLS restricted-fallback refresh result observed for one item."""

        return self._last_results_by_item_identifier.get(item_identifier)

    async def wait_for_item(self, item_identifier: str) -> None:
        """Await the currently pending background HLS restricted-fallback refresh task when present."""

        while True:
            task = self._tasks_by_item_identifier.get(item_identifier)
            if task is None:
                return
            await task
            current = self._tasks_by_item_identifier.get(item_identifier)
            if current is task and task.done():
                self._tasks_by_item_identifier.pop(item_identifier, None)

    async def shutdown(self) -> None:
        """Cancel and drain any pending in-process HLS restricted-fallback refresh tasks."""

        tasks = [task for task in self._tasks_by_item_identifier.values() if not task.done()]
        if not tasks:
            self._tasks_by_item_identifier.clear()
            return

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks_by_item_identifier.clear()

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> HlsRestrictedFallbackRefreshControlPlaneTriggerResult:
        """Trigger one in-process selected-HLS restricted-fallback refresh without blocking the route path."""

        if self.has_pending(item_identifier):
            return HlsRestrictedFallbackRefreshControlPlaneTriggerResult(
                item_identifier=item_identifier,
                outcome="already_pending",
                refresh_result=self.get_last_result(item_identifier),
            )

        task = asyncio.create_task(
            self._run_item(item_identifier, at=at),
            name=f"hls-restricted-fallback-refresh:{item_identifier}",
        )
        self._register_task(item_identifier, task)
        return HlsRestrictedFallbackRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled",
        )

    async def _run_item(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        requested_at = at or datetime.now(UTC)
        normalized_retry_after = max(0.0, retry_after_seconds or 0.0)
        if normalized_retry_after > 0.0:
            await self._sleep(normalized_retry_after)
            requested_at = requested_at + timedelta(seconds=normalized_retry_after)

        result = await self._playback_service.execute_selected_hls_restricted_fallback_refresh_with_providers(
            item_identifier,
            executors=self._executors,
            provider_clients=self._provider_clients,
            rate_limiter=self._rate_limiter,
            at=requested_at,
        )
        self._last_results_by_item_identifier[item_identifier] = result

        if result.outcome != "run_later" or result.retry_after_seconds is None:
            return

        current_task = asyncio.current_task()
        if current_task is not None:
            current = self._tasks_by_item_identifier.get(item_identifier)
            if current is current_task:
                self._tasks_by_item_identifier.pop(item_identifier, None)

        follow_up_task = asyncio.create_task(
            self._run_item(
                item_identifier,
                at=requested_at,
                retry_after_seconds=result.retry_after_seconds,
            ),
            name=f"hls-restricted-fallback-refresh:{item_identifier}",
        )
        self._register_task(item_identifier, follow_up_task)


async def trigger_direct_playback_refresh_from_resources(
    resources: AppResources,
    item_identifier: str,
    *,
    at: datetime | None = None,
    prefer_queued: bool | None = None,
) -> AppScopedDirectPlaybackRefreshTriggerResult:
    """Trigger direct-play refresh work through the app-scoped controller when configured."""

    default_use_queued = resources.settings.stream.refresh_dispatch_mode == "queued"
    use_queued = default_use_queued if prefer_queued is None else prefer_queued
    controller = (
        resources.queued_direct_playback_refresh_controller
        if use_queued
        else resources.playback_refresh_controller
    )
    if controller is None:
        controller = (
            resources.playback_refresh_controller
            or resources.queued_direct_playback_refresh_controller
        )
    if controller is None:
        return AppScopedDirectPlaybackRefreshTriggerResult(
            item_identifier=item_identifier,
            outcome="controller_unavailable",
            controller_attached=False,
        )

    control_plane_result = await controller.trigger(item_identifier, at=at)
    return AppScopedDirectPlaybackRefreshTriggerResult(
        item_identifier=item_identifier,
        outcome="triggered",
        controller_attached=True,
        control_plane_result=control_plane_result,
    )


async def trigger_hls_failed_lease_refresh_from_resources(
    resources: AppResources,
    item_identifier: str,
    *,
    at: datetime | None = None,
    prefer_queued: bool | None = None,
) -> AppScopedHlsFailedLeaseRefreshTriggerResult:
    """Trigger selected-HLS failed-lease refresh work through the app-scoped controller when configured."""

    default_use_queued = resources.settings.stream.refresh_dispatch_mode == "queued"
    use_queued = default_use_queued if prefer_queued is None else prefer_queued
    controller = (
        resources.queued_hls_failed_lease_refresh_controller
        if use_queued
        else resources.hls_failed_lease_refresh_controller
    )
    if controller is None:
        controller = (
            resources.hls_failed_lease_refresh_controller
            or resources.queued_hls_failed_lease_refresh_controller
        )
    if controller is None:
        return AppScopedHlsFailedLeaseRefreshTriggerResult(
            item_identifier=item_identifier,
            outcome="controller_unavailable",
            controller_attached=False,
        )

    control_plane_result = await controller.trigger(item_identifier, at=at)
    return AppScopedHlsFailedLeaseRefreshTriggerResult(
        item_identifier=item_identifier,
        outcome="triggered",
        controller_attached=True,
        control_plane_result=control_plane_result,
    )


async def trigger_hls_restricted_fallback_refresh_from_resources(
    resources: AppResources,
    item_identifier: str,
    *,
    at: datetime | None = None,
    prefer_queued: bool | None = None,
) -> AppScopedHlsRestrictedFallbackRefreshTriggerResult:
    """Trigger selected-HLS restricted-fallback refresh work through the app-scoped controller when configured."""

    default_use_queued = resources.settings.stream.refresh_dispatch_mode == "queued"
    use_queued = default_use_queued if prefer_queued is None else prefer_queued
    controller = (
        resources.queued_hls_restricted_fallback_refresh_controller
        if use_queued
        else resources.hls_restricted_fallback_refresh_controller
    )
    if controller is None:
        controller = (
            resources.hls_restricted_fallback_refresh_controller
            or resources.queued_hls_restricted_fallback_refresh_controller
        )
    if controller is None:
        return AppScopedHlsRestrictedFallbackRefreshTriggerResult(
            item_identifier=item_identifier,
            outcome="controller_unavailable",
            controller_attached=False,
        )

    control_plane_result = await controller.trigger(item_identifier, at=at)
    return AppScopedHlsRestrictedFallbackRefreshTriggerResult(
        item_identifier=item_identifier,
        outcome="triggered",
        controller_attached=True,
        control_plane_result=control_plane_result,
    )




