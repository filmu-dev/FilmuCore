"""Streaming compatibility routes for logs, notifications, and early playback flows."""

# mypy: disable-error-code=untyped-decorator

from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
from asyncio.subprocess import PIPE
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, cast
from urllib.parse import quote, unquote, urljoin, urlparse

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi import Path as ApiPath
from fastapi.responses import FileResponse, Response, StreamingResponse
from prometheus_client import Counter

from filmu_py.api.deps import get_auth_context, get_db, get_event_bus, get_log_stream, get_resources
from filmu_py.api.models import (
    EventTypesResponse,
    ServingGovernanceResponse,
    ServingHandleResponse,
    ServingPathResponse,
    ServingSessionResponse,
    ServingStatusResponse,
)
from filmu_py.core import byte_streaming
from filmu_py.core.event_bus import EventBus
from filmu_py.core.log_stream import LogStreamBroker
from filmu_py.db.models import MediaItemORM
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.resources import AppResources
from filmu_py.services.playback import (
    PLAYBACK_RISK_EVENTS,
    DirectFileServingDescriptor,
    PlaybackSourceService,
    trigger_direct_playback_refresh_from_resources,
    trigger_hls_failed_lease_refresh_from_resources,
    trigger_hls_restricted_fallback_refresh_from_resources,
)
from filmu_py.services.vfs_server import build_empty_vfs_catalog_governance_snapshot

from .runtime_governance import (
    empty_playback_gate_governance_snapshot,
    playback_gate_governance_snapshot,
    runtime_pressure_requires_queued_dispatch,
    vfs_runtime_governance_snapshot,
)
from .runtime_hls_governance import (
    classify_hls_route_failure_reason,
    hls_route_failure_governance_snapshot,
    is_retryable_remote_hls_error,
    raise_remote_hls_cooldown_if_active,
    record_hls_route_failure,
    record_inline_remote_hls_refresh,
    remote_hls_recovery_governance_snapshot,
    run_remote_hls_with_retry,
    start_remote_hls_cooldown,
    validate_upstream_hls_playlist,
)
from .runtime_refresh_governance import (
    DIRECT_PLAYBACK_TRIGGER_GOVERNANCE as _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE,
)
from .runtime_refresh_governance import (
    HLS_FAILED_LEASE_TRIGGER_GOVERNANCE as _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE,
)
from .runtime_refresh_governance import (
    HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE as _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE,
)
from .runtime_refresh_governance import (
    STREAM_REFRESH_POLICY_GOVERNANCE as _STREAM_REFRESH_POLICY_GOVERNANCE,
)
from .runtime_refresh_governance import (
    direct_playback_trigger_governance_snapshot,
    hls_failed_lease_trigger_governance_snapshot,
    hls_restricted_fallback_trigger_governance_snapshot,
    record_route_refresh_trigger_pending,
    stream_refresh_policy_governance_snapshot,
)

router = APIRouter(prefix="/stream", tags=["stream"])

STREAM_ROUTE_RESULTS = Counter(
    "filmu_py_stream_route_results_total",
    "Count of stream route outcomes by route and HTTP status code.",
    labelnames=("route", "status_code"),
)
_PLAYBACK_PROOF_ARTIFACTS_ROOT = Path(__file__).resolve().parents[3] / "playback-proof-artifacts"
_MANAGED_WINDOWS_VFS_STATE_PATH = (
    _PLAYBACK_PROOF_ARTIFACTS_ROOT / "windows-native-stack" / "filmuvfs-windows-state.json"
)
_STREAM_REFRESH_LATENCY_SLO_MS = 250
_BACKGROUND_ROUTE_TASKS: set[asyncio.Task[None]] = set()
_HLS_FAILED_LEASE_BACKGROUND_ROUTE_TASKS: set[asyncio.Task[None]] = set()
_HLS_RESTRICTED_FALLBACK_BACKGROUND_ROUTE_TASKS: set[asyncio.Task[None]] = set()

_SOURCE_URL_KEYS = (
    "stream_url",
    "download_url",
    "source_url",
    "url",
    "streamUrl",
    "downloadUrl",
    "sourceUrl",
)
_SOURCE_PATH_KEYS = ("file_path", "local_path", "path", "filePath", "localPath")
_HLS_URL_KEYS = ("hls_url", "hlsUrl", "playlist_url", "playlistUrl")
_MATCH_ATTR_KEYS = ("tmdb_id", "tvdb_id", "imdb_id")
_PREFERRED_SOURCE_CONTAINER_KEYS = (
    "active_stream",
    "activeStream",
    "selected_stream",
    "selectedStream",
    "current_stream",
    "currentStream",
    "primary_stream",
    "primaryStream",
)
_SOURCE_COLLECTION_CONTAINER_KEYS = (
    "streams",
    "streamsByProvider",
    "stream_list",
    "streamList",
    "sources",
    "source_list",
    "sourceList",
    "media",
    "playback",
)
_PREFERRED_SOURCE_FLAG_KEYS = (
    "selected",
    "is_selected",
    "isSelected",
    "active",
    "is_active",
    "isActive",
    "default",
    "is_default",
    "isDefault",
    "primary",
    "is_primary",
    "isPrimary",
    "current",
    "is_current",
    "isCurrent",
)
_HLS_OUTPUT_ROOT = Path(tempfile.gettempdir()) / "filmu_py_hls"
_PREFERRED_SOURCE_CONTAINER_BONUS = 200
_SOURCE_COLLECTION_CONTAINER_BONUS = 25
_PREFERRED_SOURCE_FLAG_BONUS = 100
_PATH_SOURCE_BONUS = 20
_URL_SOURCE_BONUS = 10
_ATTACHMENT_PROVIDER_KEYS = (
    "provider",
    "provider_key",
    "providerKey",
    "service",
    "debrid_service",
    "debridService",
)
_ATTACHMENT_PROVIDER_DOWNLOAD_ID_KEYS = (
    "provider_download_id",
    "providerDownloadId",
    "download_id",
    "downloadId",
    "torrent_id",
    "torrentId",
)
_ATTACHMENT_FILENAME_KEYS = (
    "original_filename",
    "originalFilename",
    "filename",
    "file_name",
    "fileName",
    "name",
)
_ATTACHMENT_FILE_SIZE_KEYS = (
    "file_size",
    "fileSize",
    "filesize",
    "size_bytes",
    "sizeBytes",
    "size",
)
_ATTACHMENT_UNRESTRICTED_URL_KEYS = (
    "unrestricted_url",
    "unrestrictedUrl",
    "stream_url",
    "streamUrl",
    "unrestricted_link",
    "unrestrictedLink",
)
_ATTACHMENT_RESTRICTED_URL_KEYS = (
    "download_url",
    "downloadUrl",
    "source_url",
    "sourceUrl",
    "url",
)


def _local_hls_runtime_item_key(item_id: str) -> str:
    """Return an opaque internal key for local HLS cache/runtime state."""

    return hashlib.sha256(item_id.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class InlineRemoteHlsRefreshOutcome:
    """Result of one inline remote-HLS media-entry repair attempt."""

    outcome: str
    source: tuple[str, str, str] | None = None


def _record_stream_route_result(*, route: str, status_code: int) -> None:
    STREAM_ROUTE_RESULTS.labels(route=route, status_code=str(status_code)).inc()


def _record_hls_route_failure(*, reason: str) -> None:
    """Record one normalized HLS route failure by classified reason."""

    record_hls_route_failure(reason=reason)


def _classify_hls_route_failure_reason(exc: HTTPException) -> str:
    """Classify one normalized HLS route failure into a small reason taxonomy."""

    return classify_hls_route_failure_reason(exc)


def _hls_route_failure_governance_snapshot() -> dict[str, int]:
    """Return additive governance counters for normalized HLS route failures."""

    return hls_route_failure_governance_snapshot()


def _remote_hls_recovery_governance_snapshot() -> dict[str, int]:
    """Return additive governance counters for remote-HLS retry/cooldown behavior."""

    return remote_hls_recovery_governance_snapshot()


def _record_inline_remote_hls_refresh(*, event: str) -> None:
    """Record one inline remote-HLS media-entry repair event."""

    record_inline_remote_hls_refresh(event=event)


async def _allow_tenant_refresh_trigger(
    *,
    request: Request,
    quota_name: str,
) -> bool:
    """Return whether the current tenant is within the configured refresh pressure budget."""

    resources = get_resources(request)
    settings = resources.settings
    if not settings.tenant_quotas.enabled:
        return True
    auth_context = get_auth_context(request)
    limits = settings.tenant_quotas.tenants.get(auth_context.tenant_id, settings.tenant_quotas.default)
    limit = getattr(limits, quota_name, None)
    if not isinstance(limit, int) or limit <= 0:
        return True
    minute = int(datetime.now(UTC).timestamp() // 60)
    key = f"quota:tenant:{auth_context.tenant_id}:{quota_name}:{minute}"
    current = int(await cast(Any, resources.redis).incr(key))
    if current == 1 and hasattr(resources.redis, "expire"):
        await cast(Any, resources.redis).expire(key, 120)
    return current <= limit


def _direct_playback_trigger_governance_snapshot() -> dict[str, int]:
    """Return additive governance counters for route-adjacent direct-play refresh triggering."""

    active_tasks = sum(1 for task in _BACKGROUND_ROUTE_TASKS if not task.done())
    return direct_playback_trigger_governance_snapshot(active_tasks=active_tasks)


def _stream_refresh_policy_governance_snapshot() -> dict[str, int]:
    """Return route-adjacent stream refresh dispatch policy counters."""

    return stream_refresh_policy_governance_snapshot(
        stream_refresh_latency_slo_ms=_STREAM_REFRESH_LATENCY_SLO_MS
    )


def _playback_gate_governance_snapshot() -> dict[str, int | str | list[str]]:
    """Return machine-shaped playback-gate promotion posture from shared governance module."""

    return playback_gate_governance_snapshot()


def _empty_playback_gate_governance_snapshot() -> dict[str, int | str | list[str]]:
    """Return the default playback-gate promotion snapshot from shared governance module."""

    return empty_playback_gate_governance_snapshot()


def _vfs_runtime_governance_snapshot(
    playback_gate_governance: dict[str, int | str | list[str]] | None = None,
    *,
    request_tenant_id: str | None = None,
    authorized_tenant_ids: set[str] | None = None,
) -> dict[str, int | float | str | list[str]]:
    """Return additive runtime governance counters from the shared governance module."""

    return vfs_runtime_governance_snapshot(
        playback_gate_governance=playback_gate_governance,
        request_tenant_id=request_tenant_id,
        authorized_tenant_ids=authorized_tenant_ids,
    )


def _runtime_pressure_requires_queued_dispatch(
    governance: dict[str, int | float | str | list[str]],
) -> tuple[bool, bool]:
    """Return queued-dispatch recommendation and latency-SLO breach flag."""

    return runtime_pressure_requires_queued_dispatch(governance)


def _hls_failed_lease_trigger_governance_snapshot() -> dict[str, int]:
    """Return additive governance counters for route-adjacent HLS failed-lease refresh triggering."""

    active_tasks = sum(1 for task in _HLS_FAILED_LEASE_BACKGROUND_ROUTE_TASKS if not task.done())
    return hls_failed_lease_trigger_governance_snapshot(active_tasks=active_tasks)


def _hls_restricted_fallback_trigger_governance_snapshot() -> dict[str, int]:
    """Return additive governance counters for route-adjacent HLS restricted-fallback refresh triggering."""

    active_tasks = sum(
        1 for task in _HLS_RESTRICTED_FALLBACK_BACKGROUND_ROUTE_TASKS if not task.done()
    )
    return hls_restricted_fallback_trigger_governance_snapshot(active_tasks=active_tasks)


def _is_retryable_remote_hls_error(exc: HTTPException) -> bool:
    """Return whether one remote-HLS HTTP exception is safe to retry briefly."""

    return is_retryable_remote_hls_error(exc)


def _raise_remote_hls_cooldown_if_active(*, cooldown_key: str) -> None:
    """Fail fast when one remote-HLS upstream is in a short cooldown window."""

    raise_remote_hls_cooldown_if_active(cooldown_key=cooldown_key)


def _start_remote_hls_cooldown(*, cooldown_key: str, exc: HTTPException) -> None:
    """Start a short cooldown for one remote-HLS upstream after repeated transient failure."""

    start_remote_hls_cooldown(cooldown_key=cooldown_key, exc=exc)


async def _run_remote_hls_with_retry(
    *,
    cooldown_key: str,
    operation: Callable[[], Awaitable[Any]],
) -> Any:
    """Run one remote-HLS operation with a single transient retry and short cooldown."""

    return await run_remote_hls_with_retry(cooldown_key=cooldown_key, operation=operation)


def _validate_upstream_hls_playlist(playlist_text: str) -> None:
    """Validate one upstream HLS playlist before rewriting its child references."""

    validate_upstream_hls_playlist(playlist_text)


def _encode_sse(payload: dict[str, Any]) -> bytes:
    """Encode one JSON payload as a backend SSE `data:` frame."""

    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n".encode()


async def _iter_log_events(log_stream: LogStreamBroker) -> AsyncGenerator[bytes, None]:
    """Yield live log SSE frames from the in-memory log broker."""

    async for payload in log_stream.subscribe():
        yield _encode_sse(payload)


async def _iter_topic_events(event_bus: EventBus, event_type: str) -> AsyncGenerator[bytes, None]:
    """Yield live SSE frames for one event-bus topic."""

    async for envelope in event_bus.subscribe(event_type):
        yield _encode_sse(envelope.payload)


def _matches_identifier(item: MediaItemORM, item_identifier: str) -> bool:
    """Return whether one media item matches a playback identifier."""

    if item.id == item_identifier or item.external_ref == item_identifier:
        return True

    attributes = cast(dict[str, object], item.attributes or {})
    for key in _MATCH_ATTR_KEYS:
        value = attributes.get(key)
        if isinstance(value, str) and value == item_identifier:
            return True
    return False


def _apply_serving_descriptor_headers(
    *, response: Response, descriptor: DirectFileServingDescriptor
) -> None:
    """Attach one descriptor-owned response-header set without overriding upstream headers."""

    for header, value in descriptor.response_headers.items():
        response.headers.setdefault(header, value)


def _descriptor_content_length(descriptor: DirectFileServingDescriptor) -> int | None:
    """Return the known content length from one serving descriptor when present."""

    raw_value = descriptor.response_headers.get("content-length")
    if raw_value is None:
        return None
    try:
        parsed = int(raw_value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


async def _resolve_direct_file_serving_descriptor(
    db: DatabaseRuntime,
    item_identifier: str,
    *,
    request: Request,
    force_refresh: bool = False,
) -> DirectFileServingDescriptor:
    """Resolve one typed direct-file serving descriptor for the direct playback route."""

    playback_service = _resolve_playback_service(request=request, db=db)
    return await playback_service.resolve_direct_file_serving_descriptor(
        item_identifier,
        force_refresh=force_refresh,
    )


def _resolve_playback_service(
    *,
    request: Request | None,
    db: DatabaseRuntime,
) -> PlaybackSourceService:
    """Resolve the shared playback service for one route request when available."""

    if request is None:
        return PlaybackSourceService(db)

    try:
        resources = get_resources(request)
    except RuntimeError:
        return PlaybackSourceService(db)
    return resources.playback_service or PlaybackSourceService(
        db,
        settings=resources.settings,
        rate_limiter=resources.rate_limiter,
    )


def _should_validate_remote_direct_descriptor(descriptor: DirectFileServingDescriptor) -> bool:
    """Return whether one remote direct descriptor should be probed before proxy serving."""

    if descriptor.transport != "remote-proxy" or descriptor.provenance is None:
        return False
    lifecycle = descriptor.provenance.lifecycle
    if lifecycle is None or lifecycle.owner_kind != "media-entry":
        return False
    return lifecycle.provider_family in {"debrid", "provider"}


def _stable_direct_playback_refresh_detail(
    descriptor: DirectFileServingDescriptor | None = None,
) -> str:
    """Return the stable direct-play refresh failure detail expected by route callers."""

    detail = "Selected direct playback lease refresh failed"
    lifecycle = descriptor.provenance.lifecycle if descriptor and descriptor.provenance else None
    if (
        lifecycle is not None
        and lifecycle.owner_kind == "media-entry"
        and lifecycle.refresh_state == "failed"
        and lifecycle.last_refresh_error
    ):
        return f"{detail}: {lifecycle.last_refresh_error}"
    return detail


async def _head_remote_direct_url(url: str) -> None:
    """Validate one remote direct URL with a short HEAD request."""

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(2.0),
        ) as client:
            response = await client.head(url)
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Playback source temporarily unavailable",
        ) from exc
    if not response.is_success:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Playback source temporarily unavailable",
        )


def _control_plane_follow_up_result(control_plane_result: Any) -> Any:
    """Return the scheduling/refresh result payload carried by one control-plane trigger result."""

    return getattr(control_plane_result, "scheduling_result", None) or getattr(
        control_plane_result,
        "refresh_result",
        None,
    )


def _record_route_refresh_trigger_pending(
    *,
    governance: dict[str, int],
    item_identifier: str,
    controller: Any,
) -> bool:
    """Record duplicate-trigger/backoff governance when route-adjacent work is already pending."""

    return record_route_refresh_trigger_pending(
        governance=governance,
        item_identifier=item_identifier,
        controller=controller,
    )


async def _run_app_scoped_refresh_trigger(
    *,
    request: Request,
    item_identifier: str,
    governance: dict[str, int],
    trigger: Callable[..., Awaitable[Any]],
    prefer_queued: bool | None = None,
) -> None:
    """Trigger one app-scoped refresh controller and record shared route governance."""

    try:
        resources = get_resources(request)
    except RuntimeError:
        governance["controller_unavailable"] += 1
        return

    try:
        result = await trigger(
            resources,
            item_identifier,
            prefer_queued=prefer_queued,
        )
    except Exception:
        governance["failures"] += 1
        return

    if result.outcome == "controller_unavailable":
        governance["controller_unavailable"] += 1
        return

    control_plane_result = result.control_plane_result
    if control_plane_result is None:
        governance["no_action"] += 1
        return

    if control_plane_result.outcome == "already_pending":
        governance["already_pending"] += 1
        follow_up_result = _control_plane_follow_up_result(control_plane_result)
        if follow_up_result is not None and follow_up_result.retry_after_seconds is not None:
            governance["backoff_pending"] += 1
        return

    if control_plane_result.outcome == "no_action":
        governance["no_action"] += 1


def _select_refresh_dispatch_preference(
    *,
    resources: AppResources,
    queued_controller_available: bool,
) -> bool:
    """Return whether route-adjacent refresh work should prefer queued dispatch."""

    if resources.settings.stream.refresh_dispatch_mode == "queued":
        if queued_controller_available:
            return True
        _STREAM_REFRESH_POLICY_GOVERNANCE["fallback_in_process"] += 1
        return False

    runtime_governance = _vfs_runtime_governance_snapshot()
    requires_queue, latency_slo_breached = _runtime_pressure_requires_queued_dispatch(runtime_governance)
    if latency_slo_breached:
        _STREAM_REFRESH_POLICY_GOVERNANCE["latency_slo_breaches"] += 1
    if requires_queue and queued_controller_available:
        _STREAM_REFRESH_POLICY_GOVERNANCE["forced_queued"] += 1
        return True
    _STREAM_REFRESH_POLICY_GOVERNANCE["forced_in_process"] += 1
    if requires_queue and not queued_controller_available:
        _STREAM_REFRESH_POLICY_GOVERNANCE["fallback_in_process"] += 1
    return False


async def _run_direct_playback_refresh_trigger(
    *,
    request: Request,
    item_identifier: str,
    prefer_queued: bool | None = None,
) -> None:
    """Trigger app-scoped direct-play refresh work without blocking the route response path."""

    if not await _allow_tenant_refresh_trigger(
        request=request,
        quota_name="playback_refreshes_per_minute",
    ):
        _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE["no_action"] += 1
        return
    await _run_app_scoped_refresh_trigger(
        request=request,
        item_identifier=item_identifier,
        governance=_DIRECT_PLAYBACK_TRIGGER_GOVERNANCE,
        trigger=trigger_direct_playback_refresh_from_resources,
        prefer_queued=prefer_queued,
    )


def _is_selected_hls_failed_lease_error(exc: HTTPException) -> bool:
    """Return whether one HLS route exception represents a selected failed HLS lease."""

    detail = exc.detail if isinstance(exc.detail, str) else ""
    return exc.status_code == status.HTTP_503_SERVICE_UNAVAILABLE and detail.startswith(
        "Selected HLS playback lease refresh failed"
    )


async def _run_hls_failed_lease_refresh_trigger(
    *,
    request: Request,
    item_identifier: str,
    prefer_queued: bool | None = None,
) -> None:
    """Trigger app-scoped selected-HLS failed-lease refresh work without blocking the route response path."""

    if not await _allow_tenant_refresh_trigger(
        request=request,
        quota_name="provider_refreshes_per_minute",
    ):
        _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE["no_action"] += 1
        return
    await _run_app_scoped_refresh_trigger(
        request=request,
        item_identifier=item_identifier,
        governance=_HLS_FAILED_LEASE_TRIGGER_GOVERNANCE,
        trigger=trigger_hls_failed_lease_refresh_from_resources,
        prefer_queued=prefer_queued,
    )


async def _run_hls_restricted_fallback_refresh_trigger(
    *,
    request: Request,
    item_identifier: str,
    prefer_queued: bool | None = None,
) -> None:
    """Trigger app-scoped selected-HLS restricted-fallback refresh work without blocking the route response path."""

    if not await _allow_tenant_refresh_trigger(
        request=request,
        quota_name="provider_refreshes_per_minute",
    ):
        _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE["no_action"] += 1
        return
    await _run_app_scoped_refresh_trigger(
        request=request,
        item_identifier=item_identifier,
        governance=_HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE,
        trigger=trigger_hls_restricted_fallback_refresh_from_resources,
        prefer_queued=prefer_queued,
    )


def _start_hls_failed_lease_refresh_trigger(*, request: Request, item_identifier: str) -> None:
    """Schedule the app-scoped HLS failed-lease refresh trigger as fire-and-forget route-adjacent work."""

    try:
        resources = get_resources(request)
    except RuntimeError:
        _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE["controller_unavailable"] += 1
        return

    controller = (
        resources.queued_hls_failed_lease_refresh_controller
        if _select_refresh_dispatch_preference(
            resources=resources,
            queued_controller_available=resources.queued_hls_failed_lease_refresh_controller
            is not None,
        )
        else resources.hls_failed_lease_refresh_controller
    )
    if controller is None:
        controller = (
            resources.hls_failed_lease_refresh_controller
            or resources.queued_hls_failed_lease_refresh_controller
        )
    if controller is None:
        _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE["controller_unavailable"] += 1
        return

    if _record_route_refresh_trigger_pending(
        governance=_HLS_FAILED_LEASE_TRIGGER_GOVERNANCE,
        item_identifier=item_identifier,
        controller=controller,
    ):
        return

    try:
        task = asyncio.create_task(
            _run_hls_failed_lease_refresh_trigger(
                request=request,
                item_identifier=item_identifier,
                prefer_queued=(
                    controller is resources.queued_hls_failed_lease_refresh_controller
                ),
            ),
            name=f"hls-failed-lease-refresh-trigger:{item_identifier}",
        )
        _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE["starts"] += 1
        _HLS_FAILED_LEASE_BACKGROUND_ROUTE_TASKS.add(task)
        task.add_done_callback(_HLS_FAILED_LEASE_BACKGROUND_ROUTE_TASKS.discard)
    except RuntimeError:
        _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE["failures"] += 1
        return


def _start_hls_restricted_fallback_refresh_trigger(
    *, request: Request, item_identifier: str
) -> None:
    """Schedule the app-scoped HLS restricted-fallback refresh trigger as fire-and-forget route-adjacent work."""

    try:
        resources = get_resources(request)
    except RuntimeError:
        _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE["controller_unavailable"] += 1
        return

    controller = (
        resources.queued_hls_restricted_fallback_refresh_controller
        if _select_refresh_dispatch_preference(
            resources=resources,
            queued_controller_available=(
                resources.queued_hls_restricted_fallback_refresh_controller is not None
            ),
        )
        else resources.hls_restricted_fallback_refresh_controller
    )
    if controller is None:
        controller = (
            resources.hls_restricted_fallback_refresh_controller
            or resources.queued_hls_restricted_fallback_refresh_controller
        )
    if controller is None:
        _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE["controller_unavailable"] += 1
        return

    if _record_route_refresh_trigger_pending(
        governance=_HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE,
        item_identifier=item_identifier,
        controller=controller,
    ):
        return

    try:
        task = asyncio.create_task(
            _run_hls_restricted_fallback_refresh_trigger(
                request=request,
                item_identifier=item_identifier,
                prefer_queued=(
                    controller is resources.queued_hls_restricted_fallback_refresh_controller
                ),
            ),
            name=f"hls-restricted-fallback-refresh-trigger:{item_identifier}",
        )
        _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE["starts"] += 1
        _HLS_RESTRICTED_FALLBACK_BACKGROUND_ROUTE_TASKS.add(task)
        task.add_done_callback(_HLS_RESTRICTED_FALLBACK_BACKGROUND_ROUTE_TASKS.discard)
    except RuntimeError:
        _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE["failures"] += 1
        return


def _start_direct_playback_refresh_trigger(*, request: Request, item_identifier: str) -> None:
    """Schedule the app-scoped direct-play refresh trigger as fire-and-forget route-adjacent work."""

    try:
        resources = get_resources(request)
    except RuntimeError:
        _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE["controller_unavailable"] += 1
        return

    controller = (
        resources.queued_direct_playback_refresh_controller
        if _select_refresh_dispatch_preference(
            resources=resources,
            queued_controller_available=resources.queued_direct_playback_refresh_controller
            is not None,
        )
        else resources.playback_refresh_controller
    )
    if controller is None:
        controller = (
            resources.playback_refresh_controller or resources.queued_direct_playback_refresh_controller
        )
    if controller is None:
        _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE["controller_unavailable"] += 1
        return

    if _record_route_refresh_trigger_pending(
        governance=_DIRECT_PLAYBACK_TRIGGER_GOVERNANCE,
        item_identifier=item_identifier,
        controller=controller,
    ):
        return

    try:
        task = asyncio.create_task(
            _run_direct_playback_refresh_trigger(
                request=request,
                item_identifier=item_identifier,
                prefer_queued=(controller is resources.queued_direct_playback_refresh_controller),
            ),
            name=f"direct-play-refresh-trigger:{item_identifier}",
        )
        _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE["starts"] += 1
        _BACKGROUND_ROUTE_TASKS.add(task)
        task.add_done_callback(_BACKGROUND_ROUTE_TASKS.discard)
    except RuntimeError:
        _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE["failures"] += 1
        return


def _is_hls_playlist_url(url: str) -> bool:
    """Return whether one URL appears to point at an HLS playlist."""

    return urlparse(url).path.lower().endswith(".m3u8")


async def _download_text(url: str) -> tuple[str, httpx.Headers]:
    """Download one upstream text payload and return its body plus headers."""

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
            response = await client.get(url)
    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Upstream HLS request timed out",
        ) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream HLS request transport failed",
        ) from exc

    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Upstream HLS request failed with status {response.status_code}",
        )

    return response.text, response.headers


async def _download_remote_hls_playlist(url: str) -> tuple[str, httpx.Headers]:
    """Download one remote HLS playlist with bounded retry/cooldown handling."""

    return cast(
        tuple[str, httpx.Headers],
        await _run_remote_hls_with_retry(
            cooldown_key=url,
            operation=lambda: _download_text(url),
        ),
    )


async def _open_remote_hls_child_stream(
    *, playlist_url: str, upstream_url: str, request: Request
) -> StreamingResponse:
    """Open one remote HLS child stream with bounded retry/cooldown handling."""

    return cast(
        StreamingResponse,
        await _run_remote_hls_with_retry(
            cooldown_key=playlist_url,
            operation=lambda: byte_streaming.stream_remote(upstream_url, request, owner="http-hls"),
        ),
    )


def _rewrite_hls_playlist(*, playlist_text: str, item_id: str) -> str:
    """Rewrite upstream playlist references to flow through the frontend BFF proxy."""

    rewritten: list[str] = []
    for line in playlist_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            rewritten.append(line)
            continue

        if stripped.startswith(("http://", "https://")):
            rewritten.append(f"/api/stream/{item_id}/hls/proxy/{quote(stripped, safe='')}")
            continue

        rewritten.append(f"/api/stream/{item_id}/hls/{stripped.lstrip('/')}")

    return "\n".join(rewritten) + "\n"


def _apply_hls_playlist_cache_headers(response: Response) -> None:
    """Apply a conservative cache policy to HLS playlist responses."""

    response.headers["cache-control"] = "no-store"


def _apply_hls_child_cache_headers(*, response: Response, file_path: str) -> None:
    """Apply cache headers for HLS child responses based on file kind."""

    if file_path.lower().endswith(".m3u8"):
        response.headers["cache-control"] = "no-store"
        return
    response.headers["cache-control"] = "public, max-age=3600"


async def _resolve_hls_source(
    db: DatabaseRuntime,
    item_identifier: str,
    *,
    request: Request | None = None,
    force_refresh: bool = False,
) -> tuple[str, str, str]:
    """Resolve one HLS-capable playback source for the requested item.

    Order of preference:
    1. explicit HLS playlist URL metadata
    2. direct playback URL that is already an HLS playlist
    3. local file path that can be transcoded into HLS
    """

    attachment = await _resolve_playback_service(request=request, db=db).resolve_hls_attachment(
        item_identifier,
        force_refresh=force_refresh,
    )
    if attachment.kind == "remote-hls":
        return ("remote_playlist", attachment.locator, attachment.source_key)
    if attachment.kind == "local-file":
        return ("local_file", attachment.locator, attachment.source_key)
    if attachment.kind == "remote-direct":
        return ("transcode_source", attachment.locator, attachment.source_key)
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Unsupported HLS source kind",
    )


async def _refresh_remote_hls_source(
    *,
    db: DatabaseRuntime,
    item_id: str,
    request: Request,
    source_key: str,
) -> InlineRemoteHlsRefreshOutcome:
    """Force-refresh one media-entry-backed remote-HLS source after an upstream failure."""

    if not source_key.startswith("media-entry"):
        _record_inline_remote_hls_refresh(event="no_action")
        return InlineRemoteHlsRefreshOutcome(outcome="no_action")

    _record_inline_remote_hls_refresh(event="attempts")
    try:
        refreshed_kind, refreshed_value, refreshed_key = await _resolve_hls_source(
            db,
            item_id,
            request=request,
            force_refresh=True,
        )
    except HTTPException:
        return InlineRemoteHlsRefreshOutcome(outcome="failed")
    if refreshed_kind not in {"remote_playlist", "local_file", "transcode_source"}:
        _record_inline_remote_hls_refresh(event="no_action")
        return InlineRemoteHlsRefreshOutcome(outcome="no_action")
    return InlineRemoteHlsRefreshOutcome(
        outcome="retry_with_refreshed_source",
        source=(refreshed_kind, refreshed_value, refreshed_key),
    )


async def _handoff_failed_inline_remote_hls_refresh(
    *,
    db: DatabaseRuntime,
    item_id: str,
    request: Request,
    source_key: str,
) -> None:
    """Mark one selected media-entry-backed HLS source stale and trigger background recovery."""

    if not source_key.startswith("media-entry"):
        return

    playback_service = _resolve_playback_service(request=request, db=db)
    if await playback_service.mark_selected_hls_media_entry_stale(item_id):
        _start_hls_restricted_fallback_refresh_trigger(
            request=request,
            item_identifier=item_id,
        )


async def _resolve_validated_hls_transcode_source(
    *,
    db: DatabaseRuntime,
    item_id: str,
    request: Request,
    source_value: str,
    source_key: str,
) -> str:
    """Validate and refresh one HLS transcode input when it is media-entry backed."""

    if not source_key.startswith("media-entry"):
        return source_value

    try:
        await _head_remote_direct_url(source_value)
        return source_value
    except HTTPException:
        pass

    try:
        refreshed_descriptor = await _resolve_direct_file_serving_descriptor(
            db,
            item_id,
            request=request,
            force_refresh=True,
        )
        if _should_validate_remote_direct_descriptor(refreshed_descriptor):
            await _head_remote_direct_url(refreshed_descriptor.locator)
        return str(refreshed_descriptor.locator)
    except HTTPException as exc:
        PLAYBACK_RISK_EVENTS.labels(surface="hls", reason="transcode_source_unavailable").inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "HLS transcode source is unavailable: "
                f"{_stable_direct_playback_refresh_detail()}"
            ),
            headers={"Retry-After": "10"},
        ) from exc


async def _serve_hls_playlist_from_resolved_source(
    *,
    db: DatabaseRuntime,
    item_id: str,
    request: Request,
    source_kind: str,
    source_value: str,
    source_key: str,
    local_hls_transcode_profile: byte_streaming.LocalHlsTranscodeProfile | None,
) -> Response:
    """Serve one HLS playlist from a local-file or transcode source."""

    if source_kind not in {"local_file", "transcode_source"}:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unsupported HLS source kind",
        )

    if source_kind == "transcode_source":
        _start_direct_playback_refresh_trigger(request=request, item_identifier=item_id)
        source_value = await _resolve_validated_hls_transcode_source(
            db=db,
            item_id=item_id,
            request=request,
            source_value=source_value,
            source_key=source_key,
        )

    local_hls_item_key = _local_hls_runtime_item_key(item_id)
    playlist_path = (
        await byte_streaming.ensure_local_hls_playlist(
            source_value,
            local_hls_item_key,
            transcode_profile=local_hls_transcode_profile,
        )
        if local_hls_transcode_profile is not None
        else await byte_streaming.ensure_local_hls_playlist(source_value, local_hls_item_key)
    )
    playlist_text = playlist_path.read_text(encoding="utf-8")
    byte_streaming.mark_local_hls_activity(playlist_path)
    response = Response(
        content=byte_streaming.rewrite_local_hls_playlist(
            playlist_text=playlist_text,
            item_id=item_id,
            query_string=request.url.query,
        ),
        media_type="application/vnd.apple.mpegurl",
    )
    _apply_hls_playlist_cache_headers(response)
    return response


async def _serve_hls_child_from_resolved_source(
    *,
    db: DatabaseRuntime,
    item_id: str,
    file_path: str,
    request: Request,
    source_kind: str,
    source_value: str,
    source_key: str,
    local_hls_transcode_profile: byte_streaming.LocalHlsTranscodeProfile | None,
) -> FileResponse | StreamingResponse:
    """Serve one HLS child file from a local-file or transcode source."""

    if source_kind not in {"local_file", "transcode_source"}:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unsupported HLS source kind",
        )

    if source_kind == "transcode_source":
        _start_direct_playback_refresh_trigger(request=request, item_identifier=item_id)
        source_value = await _resolve_validated_hls_transcode_source(
            db=db,
            item_id=item_id,
            request=request,
            source_value=source_value,
            source_key=source_key,
        )

    local_hls_item_key = _local_hls_runtime_item_key(item_id)
    playlist_path = (
        await byte_streaming.ensure_local_hls_playlist(
            source_value,
            local_hls_item_key,
            transcode_profile=local_hls_transcode_profile,
        )
        if local_hls_transcode_profile is not None
        else await byte_streaming.ensure_local_hls_playlist(source_value, local_hls_item_key)
    )
    try:
        candidate = byte_streaming.resolve_referenced_local_hls_file(playlist_path, file_path)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_404_NOT_FOUND:
            _record_hls_route_failure(reason="generated_missing")
        raise

    if not candidate.is_file():
        _record_hls_route_failure(reason="generated_missing")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Generated HLS file is missing",
        )

    byte_streaming.mark_local_hls_activity(candidate)
    return byte_streaming.stream_local_file(candidate, request, owner="http-hls")


def _normalize_hls_route_error(exc: HTTPException) -> HTTPException:
    """Normalize HLS generation and lease-risk failures into the simpler client-facing mapping."""

    if exc.status_code in {
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        status.HTTP_501_NOT_IMPLEMENTED,
        status.HTTP_503_SERVICE_UNAVAILABLE,
        status.HTTP_504_GATEWAY_TIMEOUT,
    }:
        _record_hls_route_failure(reason=_classify_hls_route_failure_reason(exc))
        return HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=exc.detail,
            headers=exc.headers,
        )
    return exc


def _local_hls_directory(item_id: str) -> Path:
    """Return the cache directory used for generated HLS assets for one item."""

    return _HLS_OUTPUT_ROOT / hashlib.sha256(item_id.encode("utf-8")).hexdigest()


def _build_local_hls_transcode_profile(
    *,
    pix_fmt: str | None,
    profile: str | None,
    level: str | None,
) -> byte_streaming.LocalHlsTranscodeProfile:
    """Normalize incoming compatibility query params into one backend transcode contract."""

    return byte_streaming.LocalHlsTranscodeProfile(
        pix_fmt=(pix_fmt or "yuv420p").strip() or "yuv420p",
        profile=(profile or "high").strip() or "high",
        level=(level or "4.1").strip() or "4.1",
    )


async def _ensure_local_hls_playlist(source_path: str, item_id: str) -> Path:
    """Generate a local HLS playlist for one file-backed item when needed."""

    output_dir = _local_hls_directory(item_id)
    playlist_path = output_dir / "index.m3u8"
    if playlist_path.is_file():
        return playlist_path

    output_dir.mkdir(parents=True, exist_ok=True)
    segment_pattern = output_dir / "segment_%05d.ts"

    try:
        process = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            "-i",
            source_path,
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            "-f",
            "hls",
            "-hls_time",
            "6",
            "-hls_playlist_type",
            "vod",
            "-hls_segment_filename",
            str(segment_pattern),
            str(playlist_path),
            stdout=PIPE,
            stderr=PIPE,
        )
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="ffmpeg is not available for HLS generation",
        ) from exc

    _stdout, stderr = await process.communicate()
    if process.returncode != 0 or not playlist_path.is_file():
        detail = stderr.decode("utf-8", errors="replace").strip() or "HLS generation failed"
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail,
        )

    return playlist_path


def _rewrite_local_hls_playlist(*, playlist_text: str, item_id: str) -> str:
    """Rewrite generated local HLS playlists to route segment fetches through the BFF path."""

    rewritten: list[str] = []
    for line in playlist_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            rewritten.append(line)
            continue
        rewritten.append(f"/api/stream/{item_id}/hls/{stripped.lstrip('/')}")
    return "\n".join(rewritten) + "\n"


@router.get("/event_types", operation_id="stream.event_types", response_model=EventTypesResponse)
async def get_event_types(
    event_bus: Annotated[EventBus, Depends(get_event_bus)],
) -> EventTypesResponse:
    """Return the currently known compatibility stream topics."""

    event_types = sorted({"logging", *event_bus.known_topics()})
    return EventTypesResponse(event_types=event_types)


@router.get("/status", operation_id="stream.status", response_model=ServingStatusResponse)
async def get_stream_status(
    request: Request,
    db: Annotated[DatabaseRuntime, Depends(get_db)],
    resources: Annotated[AppResources, Depends(get_resources)],
) -> ServingStatusResponse:
    """Return internal serving-session/accounting state for the shared serving core."""

    byte_streaming.cleanup_expired_serving_runtime()
    playback_governance = await PlaybackSourceService(db).build_playback_governance_snapshot()
    vfs_governance = (
        resources.vfs_catalog_server.build_governance_snapshot()
        if resources.vfs_catalog_server is not None
        else build_empty_vfs_catalog_governance_snapshot()
    )
    playback_gate_governance = _playback_gate_governance_snapshot()
    auth_context = get_auth_context(request)
    vfs_runtime_governance = _vfs_runtime_governance_snapshot(
        playback_gate_governance=playback_gate_governance,
        request_tenant_id=auth_context.tenant_id,
        authorized_tenant_ids=set(auth_context.authorized_tenant_ids),
    )
    sessions = [
        ServingSessionResponse(
            session_id=session.session_id,
            category=session.category,
            resource=session.resource,
            started_at=session.started_at.isoformat(),
            last_activity_at=session.last_activity_at.isoformat(),
            bytes_served=session.bytes_served,
        )
        for session in byte_streaming.get_active_session_snapshot()
    ]
    handles = [
        ServingHandleResponse(
            handle_id=handle.handle_id,
            session_id=handle.session_id,
            category=handle.category,
            path=handle.path,
            path_id=handle.path_id,
            created_at=handle.created_at.isoformat(),
            last_activity_at=handle.last_activity_at.isoformat(),
            bytes_served=handle.bytes_served,
            read_offset=handle.read_offset,
        )
        for handle in byte_streaming.get_active_handle_snapshot()
    ]
    paths = [
        ServingPathResponse(
            path_id=path.path_id,
            category=path.category,
            path=path.path,
            created_at=path.created_at.isoformat(),
            last_activity_at=path.last_activity_at.isoformat(),
            size_bytes=path.size_bytes,
            active_handle_count=path.active_handle_count,
        )
        for path in byte_streaming.get_active_path_snapshot()
    ]
    queued_refresh_controllers_attached = int(
        resources.queued_direct_playback_refresh_controller is not None
        and resources.queued_hls_failed_lease_refresh_controller is not None
        and resources.queued_hls_restricted_fallback_refresh_controller is not None
    )
    heavy_stage_policy = resources.settings.orchestration.heavy_stage_isolation
    heavy_stage_policy_violations = heavy_stage_policy.policy_violations()
    heavy_stage_exit_ready = int(
        heavy_stage_policy.exit_ready(
            arq_enabled=resources.settings.arq_enabled,
            refresh_dispatch_mode=resources.settings.stream.refresh_dispatch_mode,
            queued_refresh_ready=bool(
                resources.settings.stream.refresh_dispatch_mode != "queued"
                or (resources.arq_redis is not None and queued_refresh_controllers_attached)
            ),
            queued_refresh_proof_refs=resources.settings.orchestration.queued_refresh_proof_refs,
        )
    )
    return ServingStatusResponse(
        sessions=sessions,
        handles=handles,
        paths=paths,
        governance=ServingGovernanceResponse.model_validate(
            {
                **byte_streaming.get_serving_governance_snapshot(),
                **_hls_route_failure_governance_snapshot(),
                **_remote_hls_recovery_governance_snapshot(),
                **_direct_playback_trigger_governance_snapshot(),
                **_hls_failed_lease_trigger_governance_snapshot(),
                **_hls_restricted_fallback_trigger_governance_snapshot(),
                **_stream_refresh_policy_governance_snapshot(),
                **playback_governance,
                **vfs_governance,
                **vfs_runtime_governance,
                **playback_gate_governance,
                "stream_refresh_dispatch_mode": resources.settings.stream.refresh_dispatch_mode,
                "stream_refresh_queue_enabled": int(
                    resources.settings.stream.refresh_dispatch_mode == "queued"
                ),
                "stream_refresh_queue_ready": int(
                    resources.settings.stream.refresh_dispatch_mode != "queued"
                    or (resources.arq_redis is not None and queued_refresh_controllers_attached)
                ),
                "stream_refresh_proof_ref_count": len(
                    resources.settings.orchestration.queued_refresh_proof_refs
                ),
                "heavy_stage_executor_mode": heavy_stage_policy.executor_mode,
                "heavy_stage_max_workers": heavy_stage_policy.max_workers,
                "heavy_stage_max_tasks_per_child": heavy_stage_policy.max_tasks_per_child,
                "heavy_stage_spawn_context_required": int(
                    heavy_stage_policy.require_spawn_context
                ),
                "heavy_stage_max_worker_ceiling": heavy_stage_policy.max_worker_ceiling,
                "heavy_stage_policy_violation_count": len(heavy_stage_policy_violations),
                "heavy_stage_policy_violations": list(heavy_stage_policy_violations),
                "heavy_stage_process_isolation_required": int(
                    heavy_stage_policy.process_isolation_required()
                ),
                "heavy_stage_exit_ready": heavy_stage_exit_ready,
                "heavy_stage_index_timeout_seconds": heavy_stage_policy.index_timeout_seconds,
                "heavy_stage_parse_timeout_seconds": heavy_stage_policy.parse_timeout_seconds,
                "heavy_stage_rank_timeout_seconds": heavy_stage_policy.rank_timeout_seconds,
                "heavy_stage_proof_ref_count": len(heavy_stage_policy.proof_refs),
                "serving_active_session_summaries": [
                    f"{session.session_id}:{session.category}:{session.resource}"
                    for session in sessions[:10]
                ],
                "vfs_runtime_active_handle_summaries": vfs_runtime_governance.get(
                    "vfs_runtime_active_handle_summaries", []
                ),
            }
        ),
    )


@router.get("/file/{item_id}", operation_id="stream.file", response_model=None)
async def stream_file(
    item_id: Annotated[str, ApiPath(min_length=1)],
    request: Request,
    db: Annotated[DatabaseRuntime, Depends(get_db)],
) -> Response | FileResponse | StreamingResponse:
    """Return the earliest direct-play compatible response for one item.

    Current compatibility behavior:
    - serve a local file directly when metadata contains a usable file path
    - proxy a remote source when metadata contains a usable source/download URL
    - otherwise return a clear 404 playback-source error
    """

    byte_streaming.cleanup_expired_serving_runtime()
    try:
        descriptor = await _resolve_direct_file_serving_descriptor(
            db,
            item_id,
            request=request,
        )
        if _should_validate_remote_direct_descriptor(descriptor):
            try:
                await _head_remote_direct_url(descriptor.locator)
            except HTTPException:
                try:
                    refreshed_descriptor = await _resolve_direct_file_serving_descriptor(
                        db,
                        item_id,
                        request=request,
                        force_refresh=True,
                    )
                    if _should_validate_remote_direct_descriptor(refreshed_descriptor):
                        await _head_remote_direct_url(refreshed_descriptor.locator)
                    descriptor = refreshed_descriptor
                except HTTPException as refresh_exc:
                    PLAYBACK_RISK_EVENTS.labels(
                        surface="direct",
                        reason="remote_direct_head_failed",
                    ).inc()
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=_stable_direct_playback_refresh_detail(descriptor),
                        headers={"Retry-After": "10"},
                    ) from refresh_exc
        response: Response | FileResponse | StreamingResponse
        if descriptor.transport == "local-file":
            response = byte_streaming.stream_local_file(Path(descriptor.locator), request)
        else:
            _start_direct_playback_refresh_trigger(request=request, item_identifier=item_id)
            range_header = request.headers.get("range")
            remote_content_length = _descriptor_content_length(descriptor)
            if range_header is not None and remote_content_length is None:
                remote_content_length = await byte_streaming.resolve_remote_content_length(
                    descriptor.locator
                )

            if range_header is not None and remote_content_length is not None:
                resources = get_resources(request)
                chunk_cache = resources.chunk_cache
                if chunk_cache is None:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Chunk cache is not initialized",
                    )
                start, end = byte_streaming.parse_byte_range(range_header, remote_content_length)
                payload = await byte_streaming.fetch_remote_range_via_chunks(
                    item_id,
                    descriptor.locator,
                    remote_content_length,
                    start,
                    (end - start) + 1,
                    chunk_cache,
                )
                response = Response(
                    content=payload,
                    status_code=status.HTTP_206_PARTIAL_CONTENT,
                    headers=byte_streaming.local_file_headers(
                        file_size=remote_content_length,
                        start=start,
                        end=end,
                    ),
                    media_type=descriptor.media_type,
                )
            else:
                response = await byte_streaming.stream_remote(
                    descriptor.locator,
                    request,
                    owner="http-direct",
                )
        _apply_serving_descriptor_headers(response=response, descriptor=descriptor)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_503_SERVICE_UNAVAILABLE:
            _start_direct_playback_refresh_trigger(request=request, item_identifier=item_id)
        _record_stream_route_result(route="file", status_code=exc.status_code)
        raise

    _record_stream_route_result(route="file", status_code=response.status_code)
    return response


@router.get("/hls/{item_id}/index.m3u8", operation_id="stream.hls_playlist")
async def get_hls_playlist(
    item_id: Annotated[str, ApiPath(min_length=1)],
    request: Request,
    db: Annotated[DatabaseRuntime, Depends(get_db)],
    pix_fmt: Annotated[str | None, Query(min_length=1)] = None,
    profile: Annotated[str | None, Query(min_length=1)] = None,
    level: Annotated[str | None, Query(min_length=1)] = None,
) -> Response:
    """Return an HLS playlist from either an upstream HLS source or local generation."""

    local_hls_transcode_profile = (
        _build_local_hls_transcode_profile(
            pix_fmt=pix_fmt,
            profile=profile,
            level=level,
        )
        if any(value is not None for value in (pix_fmt, profile, level))
        else None
    )
    byte_streaming.cleanup_expired_serving_runtime()
    try:
        source_kind, source_value, source_key = await _resolve_hls_source(
            db,
            item_id,
            request=request,
        )
    except HTTPException as exc:
        if _is_selected_hls_failed_lease_error(exc):
            _start_hls_failed_lease_refresh_trigger(request=request, item_identifier=item_id)
        raise _normalize_hls_route_error(exc) from exc
    if source_kind == "remote_playlist" and source_key == "media-entry:restricted-fallback":
        _start_hls_restricted_fallback_refresh_trigger(request=request, item_identifier=item_id)
    if source_kind in {"local_file", "transcode_source"}:
        try:
            response = await _serve_hls_playlist_from_resolved_source(
                db=db,
                item_id=item_id,
                request=request,
                source_kind=source_kind,
                source_value=source_value,
                source_key=source_key,
                local_hls_transcode_profile=local_hls_transcode_profile,
            )
        except HTTPException as exc:
            raise _normalize_hls_route_error(exc) from exc
        _record_stream_route_result(route="hls_playlist", status_code=response.status_code)
        return response

    try:
        remote_playlist_stage = "download"
        playlist_text, headers = await _download_remote_hls_playlist(source_value)
        remote_playlist_stage = "validate"
        _validate_upstream_hls_playlist(playlist_text)
    except HTTPException as exc:
        refresh_outcome = await _refresh_remote_hls_source(
            db=db,
            item_id=item_id,
            request=request,
            source_key=source_key,
        )
        if refresh_outcome.outcome != "retry_with_refreshed_source" or refresh_outcome.source is None:
            if refresh_outcome.outcome == "failed":
                await _handoff_failed_inline_remote_hls_refresh(
                    db=db,
                    item_id=item_id,
                    request=request,
                    source_key=source_key,
                )
                _record_inline_remote_hls_refresh(event="failures")
            _record_hls_route_failure(
                reason=(
                    "upstream_failed"
                    if remote_playlist_stage == "download"
                    else "upstream_manifest_invalid"
                )
            )
            _record_stream_route_result(route="hls_playlist", status_code=exc.status_code)
            raise
        refreshed_kind, refreshed_value, refreshed_source_key = refresh_outcome.source
        if refreshed_kind != "remote_playlist":
            try:
                response = await _serve_hls_playlist_from_resolved_source(
                    db=db,
                    item_id=item_id,
                    request=request,
                    source_kind=refreshed_kind,
                    source_value=refreshed_value,
                    source_key=refreshed_source_key,
                    local_hls_transcode_profile=local_hls_transcode_profile,
                )
            except HTTPException as refreshed_exc:
                _record_inline_remote_hls_refresh(event="failures")
                raise _normalize_hls_route_error(refreshed_exc) from refreshed_exc
            _record_inline_remote_hls_refresh(event="recovered")
            _record_stream_route_result(route="hls_playlist", status_code=response.status_code)
            return response
        source_value = refreshed_value
        source_key = refreshed_source_key
        try:
            remote_playlist_stage = "download"
            playlist_text, headers = await _download_remote_hls_playlist(source_value)
            remote_playlist_stage = "validate"
            _validate_upstream_hls_playlist(playlist_text)
        except HTTPException as refreshed_exc:
            await _handoff_failed_inline_remote_hls_refresh(
                db=db,
                item_id=item_id,
                request=request,
                source_key=source_key,
            )
            _record_inline_remote_hls_refresh(event="failures")
            _record_hls_route_failure(
                reason=(
                    "upstream_failed"
                    if remote_playlist_stage == "download"
                    else "upstream_manifest_invalid"
                )
            )
            _record_stream_route_result(route="hls_playlist", status_code=refreshed_exc.status_code)
            raise
        _record_inline_remote_hls_refresh(event="recovered")
    response = Response(
        content=_rewrite_hls_playlist(playlist_text=playlist_text, item_id=item_id),
        media_type=headers.get("content-type", "application/vnd.apple.mpegurl"),
    )
    _apply_hls_playlist_cache_headers(response)
    _record_stream_route_result(route="hls_playlist", status_code=response.status_code)
    return response


@router.get("/hls/{item_id}/{file_path:path}", operation_id="stream.hls_file", response_model=None)
async def get_hls_file(
    item_id: Annotated[str, ApiPath(min_length=1)],
    file_path: Annotated[str, ApiPath(min_length=1)],
    request: Request,
    db: Annotated[DatabaseRuntime, Depends(get_db)],
    pix_fmt: Annotated[str | None, Query(min_length=1)] = None,
    profile: Annotated[str | None, Query(min_length=1)] = None,
    level: Annotated[str | None, Query(min_length=1)] = None,
) -> FileResponse | StreamingResponse:
    """Serve or proxy one HLS child file for an HLS-backed item."""

    local_hls_transcode_profile = (
        _build_local_hls_transcode_profile(
            pix_fmt=pix_fmt,
            profile=profile,
            level=level,
        )
        if any(value is not None for value in (pix_fmt, profile, level))
        else None
    )
    byte_streaming.cleanup_expired_serving_runtime()
    try:
        try:
            source_kind, source_value, source_key = await _resolve_hls_source(
                db,
                item_id,
                request=request,
            )
        except HTTPException as exc:
            if _is_selected_hls_failed_lease_error(exc):
                _start_hls_failed_lease_refresh_trigger(request=request, item_identifier=item_id)
            raise _normalize_hls_route_error(exc) from exc
        if source_kind == "remote_playlist" and source_key == "media-entry:restricted-fallback":
            _start_hls_restricted_fallback_refresh_trigger(request=request, item_identifier=item_id)
        if source_kind in {"local_file", "transcode_source"}:
            try:
                response = await _serve_hls_child_from_resolved_source(
                    db=db,
                    item_id=item_id,
                    file_path=file_path,
                    request=request,
                    source_kind=source_kind,
                    source_value=source_value,
                    source_key=source_key,
                    local_hls_transcode_profile=local_hls_transcode_profile,
                )
            except HTTPException as exc:
                raise _normalize_hls_route_error(exc) from exc
        else:
            playlist_url = source_value
            if file_path.startswith("proxy/"):
                upstream_url = unquote(file_path.removeprefix("proxy/"))
            else:
                upstream_url = urljoin(playlist_url, file_path)

            try:
                response = await _open_remote_hls_child_stream(
                    playlist_url=playlist_url,
                    upstream_url=upstream_url,
                    request=request,
                )
            except HTTPException:
                refresh_outcome = await _refresh_remote_hls_source(
                    db=db,
                    item_id=item_id,
                    request=request,
                    source_key=source_key,
                )
                if (
                    refresh_outcome.outcome != "retry_with_refreshed_source"
                    or refresh_outcome.source is None
                ):
                    if refresh_outcome.outcome == "failed":
                        await _handoff_failed_inline_remote_hls_refresh(
                            db=db,
                            item_id=item_id,
                            request=request,
                            source_key=source_key,
                        )
                        _record_inline_remote_hls_refresh(event="failures")
                    _record_hls_route_failure(reason="upstream_failed")
                    raise
                refreshed_kind, refreshed_value, refreshed_source_key = refresh_outcome.source
                if refreshed_kind != "remote_playlist":
                    try:
                        response = await _serve_hls_child_from_resolved_source(
                            db=db,
                            item_id=item_id,
                            file_path=file_path,
                            request=request,
                            source_kind=refreshed_kind,
                            source_value=refreshed_value,
                            source_key=refreshed_source_key,
                            local_hls_transcode_profile=local_hls_transcode_profile,
                        )
                    except HTTPException as refreshed_exc:
                        _record_inline_remote_hls_refresh(event="failures")
                        raise _normalize_hls_route_error(refreshed_exc) from refreshed_exc
                    _record_inline_remote_hls_refresh(event="recovered")
                else:
                    source_key = refreshed_source_key
                    if file_path.startswith("proxy/"):
                        upstream_url = unquote(file_path.removeprefix("proxy/"))
                    else:
                        upstream_url = urljoin(refreshed_value, file_path)
                    try:
                        response = await _open_remote_hls_child_stream(
                            playlist_url=refreshed_value,
                            upstream_url=upstream_url,
                            request=request,
                        )
                    except HTTPException:
                        await _handoff_failed_inline_remote_hls_refresh(
                            db=db,
                            item_id=item_id,
                            request=request,
                            source_key=source_key,
                        )
                        _record_inline_remote_hls_refresh(event="failures")
                        _record_hls_route_failure(reason="upstream_failed")
                        raise
                    _record_inline_remote_hls_refresh(event="recovered")
    except HTTPException as exc:
        _record_stream_route_result(route="hls_file", status_code=exc.status_code)
        raise

    _apply_hls_child_cache_headers(response=response, file_path=file_path)
    _record_stream_route_result(route="hls_file", status_code=response.status_code)
    return response


@router.get("/{event_type}", operation_id="stream.events")
async def stream_events(
    event_type: Annotated[str, ApiPath(min_length=1, description="The type of event to stream")],
    event_bus: Annotated[EventBus, Depends(get_event_bus)],
    log_stream: Annotated[LogStreamBroker, Depends(get_log_stream)],
) -> StreamingResponse:
    """Stream one compatibility SSE topic to authenticated clients."""

    iterator = (
        _iter_log_events(log_stream)
        if event_type == "logging"
        else _iter_topic_events(event_bus, event_type)
    )
    return StreamingResponse(iterator, media_type="text/event-stream")
