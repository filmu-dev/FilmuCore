"""Streaming compatibility routes for logs, notifications, and early playback flows."""

# mypy: disable-error-code=untyped-decorator

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
from asyncio.subprocess import PIPE
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import Annotated, Any, Literal, cast
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

router = APIRouter(prefix="/stream", tags=["stream"])

STREAM_ROUTE_RESULTS = Counter(
    "filmu_py_stream_route_results_total",
    "Count of stream route outcomes by route and HTTP status code.",
    labelnames=("route", "status_code"),
)
HLS_ROUTE_FAILURE_EVENTS = Counter(
    "filmu_py_stream_hls_route_failures_total",
    "Count of normalized HLS route failures by classified reason.",
    labelnames=("reason",),
)
REMOTE_HLS_RECOVERY_EVENTS = Counter(
    "filmu_py_stream_remote_hls_recovery_total",
    "Count of remote-HLS retry/cooldown recovery events by kind.",
    labelnames=("event",),
)
INLINE_REMOTE_HLS_REFRESH_EVENTS = Counter(
    "filmu_py_stream_inline_remote_hls_refresh_total",
    "Count of inline remote-HLS media-entry repair attempts by outcome.",
    labelnames=("event",),
)
_PLAYBACK_PROOF_ARTIFACTS_ROOT = Path(__file__).resolve().parents[3] / "playback-proof-artifacts"
_MANAGED_WINDOWS_VFS_STATE_PATH = (
    _PLAYBACK_PROOF_ARTIFACTS_ROOT / "windows-native-stack" / "filmuvfs-windows-state.json"
)

_HLS_ROUTE_FAILURE_GOVERNANCE = {
    "generation_failed": 0,
    "generation_timeout": 0,
    "generation_capacity_exceeded": 0,
    "generator_unavailable": 0,
    "lease_failed": 0,
    "transcode_source_unavailable": 0,
    "manifest_invalid": 0,
    "generated_missing": 0,
    "upstream_failed": 0,
    "upstream_manifest_invalid": 0,
}
_REMOTE_HLS_RETRY_GOVERNANCE = {
    "retry_attempts": 0,
    "cooldown_starts": 0,
    "cooldown_hits": 0,
}
_INLINE_REMOTE_HLS_REFRESH_GOVERNANCE = {
    "attempts": 0,
    "recovered": 0,
    "no_action": 0,
    "failures": 0,
}
_DIRECT_PLAYBACK_TRIGGER_GOVERNANCE = {
    "starts": 0,
    "no_action": 0,
    "controller_unavailable": 0,
    "already_pending": 0,
    "backoff_pending": 0,
    "failures": 0,
}
_HLS_FAILED_LEASE_TRIGGER_GOVERNANCE = {
    "starts": 0,
    "no_action": 0,
    "controller_unavailable": 0,
    "already_pending": 0,
    "backoff_pending": 0,
    "failures": 0,
}
_HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE = {
    "starts": 0,
    "no_action": 0,
    "controller_unavailable": 0,
    "already_pending": 0,
    "backoff_pending": 0,
    "failures": 0,
}
_REMOTE_HLS_COOLDOWNS: dict[str, tuple[float, int, str]] = {}
_REMOTE_HLS_COOLDOWN_LOCK = Lock()
_REMOTE_HLS_RETRY_ATTEMPTS = 2
_REMOTE_HLS_COOLDOWN_SECONDS = 15.0
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

    HLS_ROUTE_FAILURE_EVENTS.labels(reason=reason).inc()
    _HLS_ROUTE_FAILURE_GOVERNANCE[reason] += 1


def _classify_hls_route_failure_reason(exc: HTTPException) -> str:
    """Classify one normalized HLS route failure into a small reason taxonomy."""

    detail = exc.detail if isinstance(exc.detail, str) else ""
    if detail.startswith("HLS transcode source is unavailable"):
        return "transcode_source_unavailable"
    if detail.startswith("HLS generation capacity exceeded"):
        return "generation_capacity_exceeded"
    if detail.startswith("Generated HLS playlist is"):
        return "manifest_invalid"
    if detail.startswith("Upstream HLS playlist is"):
        return "upstream_manifest_invalid"
    if exc.status_code == status.HTTP_504_GATEWAY_TIMEOUT:
        return "generation_timeout"
    if exc.status_code == status.HTTP_501_NOT_IMPLEMENTED:
        return "generator_unavailable"
    if exc.status_code == status.HTTP_503_SERVICE_UNAVAILABLE:
        return "lease_failed"
    return "generation_failed"


def _hls_route_failure_governance_snapshot() -> dict[str, int]:
    """Return additive governance counters for normalized HLS route failures."""

    return {
        "hls_route_failures_total": sum(_HLS_ROUTE_FAILURE_GOVERNANCE.values()),
        "hls_route_failures_generation_failed": _HLS_ROUTE_FAILURE_GOVERNANCE["generation_failed"],
        "hls_route_failures_generation_timeout": _HLS_ROUTE_FAILURE_GOVERNANCE[
            "generation_timeout"
        ],
        "hls_route_failures_generation_capacity_exceeded": _HLS_ROUTE_FAILURE_GOVERNANCE[
            "generation_capacity_exceeded"
        ],
        "hls_route_failures_generator_unavailable": _HLS_ROUTE_FAILURE_GOVERNANCE[
            "generator_unavailable"
        ],
        "hls_route_failures_lease_failed": _HLS_ROUTE_FAILURE_GOVERNANCE["lease_failed"],
        "hls_route_failures_transcode_source_unavailable": _HLS_ROUTE_FAILURE_GOVERNANCE[
            "transcode_source_unavailable"
        ],
        "hls_route_failures_manifest_invalid": _HLS_ROUTE_FAILURE_GOVERNANCE["manifest_invalid"],
        "hls_route_failures_generated_missing": _HLS_ROUTE_FAILURE_GOVERNANCE["generated_missing"],
        "hls_route_failures_upstream_failed": _HLS_ROUTE_FAILURE_GOVERNANCE["upstream_failed"],
        "hls_route_failures_upstream_manifest_invalid": _HLS_ROUTE_FAILURE_GOVERNANCE[
            "upstream_manifest_invalid"
        ],
    }


def _cleanup_remote_hls_cooldowns(*, now: float | None = None) -> None:
    """Drop expired remote-HLS cooldown entries."""

    current_time = monotonic() if now is None else now
    expired_keys = [
        key
        for key, (expires_at, _, _) in _REMOTE_HLS_COOLDOWNS.items()
        if expires_at <= current_time
    ]
    for key in expired_keys:
        _REMOTE_HLS_COOLDOWNS.pop(key, None)


def _remote_hls_recovery_governance_snapshot() -> dict[str, int]:
    """Return additive governance counters for remote-HLS retry/cooldown behavior."""

    with _REMOTE_HLS_COOLDOWN_LOCK:
        _cleanup_remote_hls_cooldowns()
        active_cooldowns = len(_REMOTE_HLS_COOLDOWNS)
    return {
        "remote_hls_retry_attempts": _REMOTE_HLS_RETRY_GOVERNANCE["retry_attempts"],
        "remote_hls_cooldown_starts": _REMOTE_HLS_RETRY_GOVERNANCE["cooldown_starts"],
        "remote_hls_cooldown_hits": _REMOTE_HLS_RETRY_GOVERNANCE["cooldown_hits"],
        "remote_hls_cooldowns_active": active_cooldowns,
        "inline_remote_hls_refresh_attempts": _INLINE_REMOTE_HLS_REFRESH_GOVERNANCE["attempts"],
        "inline_remote_hls_refresh_recovered": _INLINE_REMOTE_HLS_REFRESH_GOVERNANCE[
            "recovered"
        ],
        "inline_remote_hls_refresh_no_action": _INLINE_REMOTE_HLS_REFRESH_GOVERNANCE[
            "no_action"
        ],
        "inline_remote_hls_refresh_failures": _INLINE_REMOTE_HLS_REFRESH_GOVERNANCE[
            "failures"
        ],
    }


def _record_inline_remote_hls_refresh(*, event: str) -> None:
    """Record one inline remote-HLS media-entry repair event."""

    INLINE_REMOTE_HLS_REFRESH_EVENTS.labels(event=event).inc()
    _INLINE_REMOTE_HLS_REFRESH_GOVERNANCE[event] += 1


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
    return {
        "direct_playback_refresh_trigger_starts": _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE["starts"],
        "direct_playback_refresh_trigger_no_action": _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE[
            "no_action"
        ],
        "direct_playback_refresh_trigger_controller_unavailable": _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE[
            "controller_unavailable"
        ],
        "direct_playback_refresh_trigger_already_pending": _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE[
            "already_pending"
        ],
        "direct_playback_refresh_trigger_backoff_pending": _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE[
            "backoff_pending"
        ],
        "direct_playback_refresh_trigger_failures": _DIRECT_PLAYBACK_TRIGGER_GOVERNANCE["failures"],
        "direct_playback_refresh_trigger_tasks_active": active_tasks,
    }


def _empty_vfs_runtime_governance_snapshot() -> dict[str, int | float | str | list[str]]:
    """Return the default Rust runtime governance payload for /stream/status."""

    return {
        "vfs_runtime_snapshot_available": 0,
        "vfs_runtime_open_handles": 0,
        "vfs_runtime_peak_open_handles": 0,
        "vfs_runtime_active_reads": 0,
        "vfs_runtime_peak_active_reads": 0,
        "vfs_runtime_chunk_cache_weighted_bytes": 0,
        "vfs_runtime_chunk_cache_backend": "unknown",
        "vfs_runtime_chunk_cache_memory_bytes": 0,
        "vfs_runtime_chunk_cache_memory_max_bytes": 0,
        "vfs_runtime_chunk_cache_memory_hits": 0,
        "vfs_runtime_chunk_cache_memory_misses": 0,
        "vfs_runtime_chunk_cache_disk_bytes": 0,
        "vfs_runtime_chunk_cache_disk_max_bytes": 0,
        "vfs_runtime_chunk_cache_disk_hits": 0,
        "vfs_runtime_chunk_cache_disk_misses": 0,
        "vfs_runtime_chunk_cache_disk_writes": 0,
        "vfs_runtime_chunk_cache_disk_write_errors": 0,
        "vfs_runtime_chunk_cache_disk_evictions": 0,
        "vfs_runtime_handle_startup_total": 0,
        "vfs_runtime_handle_startup_ok": 0,
        "vfs_runtime_handle_startup_error": 0,
        "vfs_runtime_handle_startup_estale": 0,
        "vfs_runtime_handle_startup_cancelled": 0,
        "vfs_runtime_handle_startup_average_duration_ms": 0,
        "vfs_runtime_handle_startup_max_duration_ms": 0,
        "vfs_runtime_mounted_reads_total": 0,
        "vfs_runtime_mounted_reads_ok": 0,
        "vfs_runtime_mounted_reads_error": 0,
        "vfs_runtime_mounted_reads_estale": 0,
        "vfs_runtime_mounted_reads_cancelled": 0,
        "vfs_runtime_mounted_reads_average_duration_ms": 0,
        "vfs_runtime_mounted_reads_max_duration_ms": 0,
        "vfs_runtime_upstream_fetch_operations": 0,
        "vfs_runtime_upstream_fetch_bytes_total": 0,
        "vfs_runtime_upstream_fetch_average_duration_ms": 0,
        "vfs_runtime_upstream_fetch_max_duration_ms": 0,
        "vfs_runtime_upstream_fail_invalid_url": 0,
        "vfs_runtime_upstream_fail_build_request": 0,
        "vfs_runtime_upstream_fail_network": 0,
        "vfs_runtime_upstream_fail_stale_status": 0,
        "vfs_runtime_upstream_fail_unexpected_status": 0,
        "vfs_runtime_upstream_fail_unexpected_status_too_many_requests": 0,
        "vfs_runtime_upstream_fail_unexpected_status_server_error": 0,
        "vfs_runtime_upstream_fail_read_body": 0,
        "vfs_runtime_upstream_retryable_network": 0,
        "vfs_runtime_upstream_retryable_read_body": 0,
        "vfs_runtime_upstream_retryable_status_too_many_requests": 0,
        "vfs_runtime_upstream_retryable_status_server_error": 0,
        "vfs_runtime_backend_fallback_attempts": 0,
        "vfs_runtime_backend_fallback_success": 0,
        "vfs_runtime_backend_fallback_failure": 0,
        "vfs_runtime_backend_fallback_attempts_direct_read_failure": 0,
        "vfs_runtime_backend_fallback_attempts_inline_refresh_unavailable": 0,
        "vfs_runtime_backend_fallback_attempts_post_inline_refresh_failure": 0,
        "vfs_runtime_backend_fallback_success_direct_read_failure": 0,
        "vfs_runtime_backend_fallback_success_inline_refresh_unavailable": 0,
        "vfs_runtime_backend_fallback_success_post_inline_refresh_failure": 0,
        "vfs_runtime_backend_fallback_failure_direct_read_failure": 0,
        "vfs_runtime_backend_fallback_failure_inline_refresh_unavailable": 0,
        "vfs_runtime_backend_fallback_failure_post_inline_refresh_failure": 0,
        "vfs_runtime_chunk_cache_hits": 0,
        "vfs_runtime_chunk_cache_misses": 0,
        "vfs_runtime_chunk_cache_inserts": 0,
        "vfs_runtime_chunk_cache_prefetch_hits": 0,
        "vfs_runtime_prefetch_concurrency_limit": 0,
        "vfs_runtime_prefetch_available_permits": 0,
        "vfs_runtime_prefetch_active_permits": 0,
        "vfs_runtime_prefetch_active_background_tasks": 0,
        "vfs_runtime_prefetch_peak_active_background_tasks": 0,
        "vfs_runtime_prefetch_background_spawned": 0,
        "vfs_runtime_prefetch_background_backpressure": 0,
        "vfs_runtime_prefetch_fairness_denied": 0,
        "vfs_runtime_prefetch_global_backpressure_denied": 0,
        "vfs_runtime_prefetch_background_error": 0,
        "vfs_runtime_chunk_coalescing_in_flight_chunks": 0,
        "vfs_runtime_chunk_coalescing_peak_in_flight_chunks": 0,
        "vfs_runtime_chunk_coalescing_waits_total": 0,
        "vfs_runtime_chunk_coalescing_waits_hit": 0,
        "vfs_runtime_chunk_coalescing_waits_miss": 0,
        "vfs_runtime_chunk_coalescing_wait_average_duration_ms": 0.0,
        "vfs_runtime_chunk_coalescing_wait_max_duration_ms": 0.0,
        "vfs_runtime_inline_refresh_success": 0,
        "vfs_runtime_inline_refresh_no_url": 0,
        "vfs_runtime_inline_refresh_error": 0,
        "vfs_runtime_inline_refresh_timeout": 0,
        "vfs_runtime_windows_callbacks_cancelled": 0,
        "vfs_runtime_windows_callbacks_error": 0,
        "vfs_runtime_windows_callbacks_estale": 0,
        "vfs_runtime_cache_hit_ratio": 0.0,
        "vfs_runtime_fallback_success_ratio": 0.0,
        "vfs_runtime_prefetch_pressure_ratio": 0.0,
        "vfs_runtime_provider_pressure_incidents": 0,
        "vfs_runtime_fairness_pressure_incidents": 0,
        "vfs_runtime_cache_pressure_class": "healthy",
        "vfs_runtime_cache_pressure_reasons": [],
        "vfs_runtime_chunk_coalescing_pressure_class": "healthy",
        "vfs_runtime_chunk_coalescing_pressure_reasons": [],
        "vfs_runtime_upstream_wait_class": "healthy",
        "vfs_runtime_upstream_wait_reasons": [],
        "vfs_runtime_refresh_pressure_class": "healthy",
        "vfs_runtime_refresh_pressure_reasons": [],
        "vfs_runtime_rollout_readiness": "unknown",
        "vfs_runtime_rollout_reasons": ["runtime_snapshot_unavailable"],
        "vfs_runtime_rollout_next_action": "capture_runtime_status",
        "vfs_runtime_rollout_canary_decision": "capture_runtime_status",
        "vfs_runtime_rollout_merge_gate": "blocked",
        "vfs_runtime_rollout_environment_class": "",
        "vfs_runtime_active_handle_summaries": [],
    }


def _as_int(value: object) -> int:
    """Normalize Rust runtime JSON numbers into additive integer counters."""

    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return 0
        try:
            return int(stripped)
        except ValueError:
            try:
                return round(float(stripped))
            except ValueError:
                return 0
    return 0


def _as_float(value: object) -> float:
    """Normalize Rust runtime JSON numbers into additive float durations."""

    if isinstance(value, bool):
        return float(value)
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return 0.0
        try:
            return float(stripped)
        except ValueError:
            return 0.0
    return 0.0


def _as_str(value: object, *, default: str = "") -> str:
    """Normalize Rust runtime JSON string values into safe status payloads."""

    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return stripped
    return default


def _as_str_list(value: object) -> list[str]:
    """Normalize list-like runtime snapshot strings into bounded operator summaries."""

    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        stripped = item.strip()
        if stripped:
            normalized.append(stripped)
    return normalized[:10]


def _safe_ratio(numerator: int, denominator: int) -> float:
    """Return a bounded operator-facing ratio for additive governance counters."""

    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _pressure_class(
    *, critical: bool, warning: bool
) -> Literal["healthy", "warning", "critical"]:
    """Collapse additive runtime signals into a bounded operator pressure class."""

    if critical:
        return "critical"
    if warning:
        return "warning"
    return "healthy"


def _nested_mapping_value(payload: object, *keys: str) -> object | None:
    """Safely walk nested JSON objects loaded from the Rust runtime status file."""

    current = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _candidate_vfs_runtime_status_paths() -> list[Path]:
    """Return the preferred Rust runtime snapshot locations in precedence order."""

    paths: list[Path] = []
    env_path = os.getenv("FILMU_PY_VFS_RUNTIME_STATUS_PATH")
    if env_path and env_path.strip():
        paths.append(Path(env_path.strip()))
    try:
        state_payload = json.loads(_MANAGED_WINDOWS_VFS_STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        state_payload = None
    if isinstance(state_payload, dict):
        runtime_status_path = state_payload.get("runtime_status_path")
        if isinstance(runtime_status_path, str) and runtime_status_path.strip():
            paths.append(Path(runtime_status_path.strip()))
    paths.append(_MANAGED_WINDOWS_VFS_STATE_PATH.parent / "filmuvfs-runtime-status.json")
    unique_paths: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        normalized = path.expanduser()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_paths.append(normalized)
    return unique_paths


def _load_vfs_runtime_status_payload() -> dict[str, object] | None:
    """Load the first readable Rust runtime status JSON payload, if any."""

    for path in _candidate_vfs_runtime_status_paths():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            return cast(dict[str, object], payload)
    return None


def _candidate_playback_artifacts_roots() -> list[Path]:
    """Return playback-proof artifact roots in precedence order."""

    roots: list[Path] = []
    env_root = os.getenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT")
    if env_root and env_root.strip():
        roots.append(Path(env_root.strip()))
    roots.append(_PLAYBACK_PROOF_ARTIFACTS_ROOT)

    unique_roots: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        normalized = root.expanduser()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_roots.append(normalized)
    return unique_roots


def _candidate_github_main_policy_paths() -> list[Path]:
    """Return candidate current-policy artifact paths in precedence order."""

    paths: list[Path] = []
    env_path = os.getenv("FILMU_PY_GITHUB_MAIN_POLICY_PATH")
    if env_path and env_path.strip():
        paths.append(Path(env_path.strip()))
    for root in _candidate_playback_artifacts_roots():
        paths.append(root / "github-main-policy-current.json")

    unique_paths: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        normalized = path.expanduser()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_paths.append(normalized)
    return unique_paths


def _load_json_file(path: Path) -> dict[str, object] | None:
    """Load one JSON file if it exists and contains an object payload."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if isinstance(payload, dict):
        return cast(dict[str, object], payload)
    return None


def _load_latest_json_artifact(*, prefix: str, subdir: str | None = None) -> dict[str, object] | None:
    """Load the newest matching JSON artifact from the playback-proof artifact tree."""

    for root in _candidate_playback_artifacts_roots():
        candidate_root = root / subdir if subdir is not None else root
        try:
            matches = list(candidate_root.glob(f"{prefix}*.json"))
        except OSError:
            continue
        newest_path: Path | None = None
        newest_mtime = -1.0
        for path in matches:
            try:
                stat = path.stat()
            except OSError:
                continue
            if stat.st_mtime > newest_mtime:
                newest_mtime = stat.st_mtime
                newest_path = path
        if newest_path is None:
            continue
        payload = _load_json_file(newest_path)
        if payload is not None:
            return payload
    return None


def _load_current_github_main_policy_artifact() -> dict[str, object] | None:
    """Load the newest available GitHub main-policy validation artifact, if present."""

    for path in _candidate_github_main_policy_paths():
        payload = _load_json_file(path)
        if payload is not None:
            return payload
    return None


def _load_playback_artifact_at_relative_path(relative_path: str) -> dict[str, object] | None:
    """Load one playback-proof artifact by relative path across candidate roots."""

    for root in _candidate_playback_artifacts_roots():
        payload = _load_json_file(root / relative_path)
        if payload is not None:
            return payload
    return None


def _empty_playback_gate_governance_snapshot() -> dict[str, int | str | list[str]]:
    """Return the default playback-gate promotion snapshot."""

    return {
        "playback_gate_snapshot_available": 0,
        "playback_gate_artifact_generated_at": "",
        "playback_gate_environment_class": "",
        "playback_gate_repeat_count": 0,
        "playback_gate_gate_mode": "unknown",
        "playback_gate_provider_gate_required": 0,
        "playback_gate_provider_gate_ran": 0,
        "playback_gate_stability_ready": 0,
        "playback_gate_provider_parity_ready": 0,
        "playback_gate_windows_provider_ready": 0,
        "playback_gate_windows_soak_ready": 0,
        "playback_gate_policy_validation_status": "unverified",
        "playback_gate_policy_ready": 0,
        "playback_gate_rollout_readiness": "not_ready",
        "playback_gate_rollout_reasons": ["missing_playback_gate_artifacts"],
        "playback_gate_rollout_next_action": "run_proof_playback_gate_enterprise",
    }


def _playback_gate_governance_snapshot() -> dict[str, int | str | list[str]]:
    """Return machine-shaped playback-gate promotion posture from local artifacts."""

    governance = _empty_playback_gate_governance_snapshot()
    stability_summary = _load_latest_json_artifact(prefix="stability-summary-")
    ci_summary = _load_playback_artifact_at_relative_path("ci-execution-summary.json")
    provider_summary = _load_latest_json_artifact(prefix="media-server-gate-")
    windows_provider_summary = _load_latest_json_artifact(prefix="windows-media-server-gate-")
    windows_soak_summary = _load_latest_json_artifact(
        prefix="soak-stability-",
        subdir="windows-native-stack",
    )
    policy_summary = _load_current_github_main_policy_artifact()

    if stability_summary is not None:
        governance["playback_gate_snapshot_available"] = 1
        governance["playback_gate_artifact_generated_at"] = _as_str(
            stability_summary.get("timestamp"),
        )
        governance["playback_gate_environment_class"] = _as_str(
            stability_summary.get("environment_class"),
        )
        governance["playback_gate_repeat_count"] = _as_int(stability_summary.get("repeat_count"))
        if bool(stability_summary.get("all_green")) and not bool(stability_summary.get("dry_run")):
            governance["playback_gate_stability_ready"] = 1

    if ci_summary is not None:
        governance["playback_gate_gate_mode"] = _as_str(
            ci_summary.get("gate_mode"),
            default="unknown",
        )
        governance["playback_gate_provider_gate_required"] = _as_int(
            ci_summary.get("provider_gate_required"),
        )
        governance["playback_gate_provider_gate_ran"] = _as_int(
            ci_summary.get("provider_gate_ran"),
        )

    provider_summary_available = provider_summary is not None
    if provider_summary is not None and bool(provider_summary.get("all_green")):
        governance["playback_gate_provider_parity_ready"] = 1

    windows_provider_summary_available = windows_provider_summary is not None
    if windows_provider_summary is not None:
        results = windows_provider_summary.get("results")
        if (
            isinstance(results, list)
            and any(
                isinstance(result, dict) and result.get("status") == "passed" for result in results
            )
            and all(
                not isinstance(result, dict) or result.get("status") in {"passed", "skipped"}
                for result in results
            )
        ):
            governance["playback_gate_windows_provider_ready"] = 1

    windows_soak_summary_available = windows_soak_summary is not None
    if windows_soak_summary is not None and bool(windows_soak_summary.get("all_green")):
        governance["playback_gate_windows_soak_ready"] = 1

    if policy_summary is not None:
        validation = policy_summary.get("validation")
        if isinstance(validation, dict):
            validation_status = _as_str(validation.get("status"), default="unverified")
            governance["playback_gate_policy_validation_status"] = validation_status
            if validation_status == "ready":
                governance["playback_gate_policy_ready"] = 1

    rollout_reasons: list[str] = []
    if governance["playback_gate_snapshot_available"] == 0:
        rollout_reasons.append("missing_playback_gate_artifacts")
    elif governance["playback_gate_gate_mode"] == "dry_run":
        rollout_reasons.append("playback_gate_dry_run_mode")
    elif governance["playback_gate_stability_ready"] == 0:
        rollout_reasons.append("playback_gate_failed_or_incomplete")

    provider_gate_required = _as_int(governance["playback_gate_provider_gate_required"]) > 0
    provider_gate_ran = _as_int(governance["playback_gate_provider_gate_ran"]) > 0
    if provider_gate_required and not provider_gate_ran:
        rollout_reasons.append("provider_gate_not_run")
    elif provider_gate_ran:
        if not provider_summary_available:
            rollout_reasons.append("provider_gate_artifact_missing")
        elif _as_int(governance["playback_gate_provider_parity_ready"]) == 0:
            rollout_reasons.append("provider_gate_not_green")

    if not windows_provider_summary_available:
        rollout_reasons.append("windows_provider_gate_artifact_missing")
    elif _as_int(governance["playback_gate_windows_provider_ready"]) == 0:
        rollout_reasons.append("windows_provider_gate_not_green")

    if not windows_soak_summary_available:
        rollout_reasons.append("windows_vfs_soak_artifact_missing")
    elif _as_int(governance["playback_gate_windows_soak_ready"]) == 0:
        rollout_reasons.append("windows_vfs_soak_not_green")

    policy_status = _as_str(governance["playback_gate_policy_validation_status"], default="unverified")
    if policy_status == "not_ready":
        rollout_reasons.append("github_main_policy_not_ready")
    elif policy_status == "unverified":
        rollout_reasons.append("github_main_policy_unverified")

    blocked_reasons = {
        "playback_gate_failed_or_incomplete",
        "provider_gate_not_green",
        "windows_provider_gate_not_green",
        "windows_vfs_soak_not_green",
        "github_main_policy_not_ready",
    }
    warning_reasons = {
        "missing_playback_gate_artifacts",
        "playback_gate_dry_run_mode",
        "provider_gate_not_run",
        "provider_gate_artifact_missing",
        "windows_provider_gate_artifact_missing",
        "windows_vfs_soak_artifact_missing",
        "github_main_policy_unverified",
    }

    if any(reason in blocked_reasons for reason in rollout_reasons):
        governance["playback_gate_rollout_readiness"] = "blocked"
        governance["playback_gate_rollout_next_action"] = "resolve_failed_playback_gate_proofs"
    elif any(reason in warning_reasons for reason in rollout_reasons):
        governance["playback_gate_rollout_readiness"] = "warning"
        governance["playback_gate_rollout_next_action"] = "record_enterprise_playback_gate_evidence"
    else:
        governance["playback_gate_rollout_readiness"] = "ready"
        governance["playback_gate_rollout_next_action"] = "keep_required_checks_enforced"
        rollout_reasons.append("enterprise_playback_gate_green")

    governance["playback_gate_rollout_reasons"] = rollout_reasons
    return governance


def _apply_vfs_rollout_policy(
    governance: dict[str, int | float | str | list[str]],
    *,
    playback_gate_governance: dict[str, int | str | list[str]] | None = None,
) -> dict[str, int | float | str | list[str]]:
    """Apply canary and rollback policy to the runtime-derived VFS rollout posture."""

    canary_environment = ""
    if playback_gate_governance is not None:
        canary_environment = _as_str(
            playback_gate_governance.get("playback_gate_environment_class"),
        )

    governance["vfs_runtime_rollout_environment_class"] = canary_environment
    governance["vfs_runtime_rollout_canary_decision"] = "capture_runtime_status"
    governance["vfs_runtime_rollout_merge_gate"] = "blocked"

    if _as_int(governance["vfs_runtime_snapshot_available"]) <= 0:
        return governance

    rollout_readiness = _as_str(
        governance["vfs_runtime_rollout_readiness"],
        default="unknown",
    )
    windows_soak_ready = (
        playback_gate_governance is not None
        and _as_int(playback_gate_governance.get("playback_gate_windows_soak_ready")) > 0
    )

    if rollout_readiness == "blocked":
        governance["vfs_runtime_rollout_canary_decision"] = "rollback_current_environment"
        governance["vfs_runtime_rollout_merge_gate"] = "blocked"
    elif not windows_soak_ready:
        governance["vfs_runtime_rollout_canary_decision"] = "hold_until_windows_soak_is_green"
        governance["vfs_runtime_rollout_merge_gate"] = "hold"
        rollout_reasons = cast(list[str], governance["vfs_runtime_rollout_reasons"])
        if "windows_vfs_soak_not_green" not in rollout_reasons:
            rollout_reasons.append("windows_vfs_soak_not_green")
    elif rollout_readiness == "warning":
        governance["vfs_runtime_rollout_canary_decision"] = "hold_canary_and_repeat_soak"
        governance["vfs_runtime_rollout_merge_gate"] = "hold"
    else:
        governance["vfs_runtime_rollout_canary_decision"] = "promote_to_next_environment_class"
        governance["vfs_runtime_rollout_merge_gate"] = "ready"

    return governance


def _vfs_runtime_governance_snapshot(
    playback_gate_governance: dict[str, int | str | list[str]] | None = None,
) -> dict[str, int | float | str | list[str]]:
    """Return additive governance counters extracted from the Rust runtime snapshot."""

    payload = _load_vfs_runtime_status_payload()
    governance = _empty_vfs_runtime_governance_snapshot()
    if payload is None:
        return _apply_vfs_rollout_policy(
            governance,
            playback_gate_governance=playback_gate_governance,
        )
    governance["vfs_runtime_snapshot_available"] = 1
    governance["vfs_runtime_open_handles"] = _as_int(_nested_mapping_value(payload, "runtime", "open_handles"))
    governance["vfs_runtime_peak_open_handles"] = _as_int(
        _nested_mapping_value(payload, "runtime", "peak_open_handles")
    )
    governance["vfs_runtime_active_reads"] = _as_int(_nested_mapping_value(payload, "runtime", "active_reads"))
    governance["vfs_runtime_peak_active_reads"] = _as_int(
        _nested_mapping_value(payload, "runtime", "peak_active_reads")
    )
    governance["vfs_runtime_chunk_cache_weighted_bytes"] = _as_int(
        _nested_mapping_value(payload, "runtime", "chunk_cache_weighted_bytes")
    )
    governance["vfs_runtime_chunk_cache_backend"] = _as_str(
        _nested_mapping_value(payload, "chunk_cache", "backend"),
        default="unknown",
    )
    governance["vfs_runtime_chunk_cache_memory_bytes"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "memory_bytes")
    )
    governance["vfs_runtime_chunk_cache_memory_max_bytes"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "memory_max_bytes")
    )
    governance["vfs_runtime_chunk_cache_memory_hits"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "memory_hits")
    )
    governance["vfs_runtime_chunk_cache_memory_misses"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "memory_misses")
    )
    governance["vfs_runtime_chunk_cache_disk_bytes"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_bytes")
    )
    governance["vfs_runtime_chunk_cache_disk_max_bytes"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_max_bytes")
    )
    governance["vfs_runtime_chunk_cache_disk_hits"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_hits")
    )
    governance["vfs_runtime_chunk_cache_disk_misses"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_misses")
    )
    governance["vfs_runtime_chunk_cache_disk_writes"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_writes")
    )
    governance["vfs_runtime_chunk_cache_disk_write_errors"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_write_errors")
    )
    governance["vfs_runtime_chunk_cache_disk_evictions"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_evictions")
    )
    governance["vfs_runtime_handle_startup_total"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "total")
    )
    governance["vfs_runtime_handle_startup_ok"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "ok")
    )
    governance["vfs_runtime_handle_startup_error"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "error")
    )
    governance["vfs_runtime_handle_startup_estale"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "estale")
    )
    governance["vfs_runtime_handle_startup_cancelled"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "cancelled")
    )
    governance["vfs_runtime_handle_startup_average_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "average_duration_ms")
    )
    governance["vfs_runtime_handle_startup_max_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "max_duration_ms")
    )
    governance["vfs_runtime_mounted_reads_total"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "total")
    )
    governance["vfs_runtime_mounted_reads_ok"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "ok")
    )
    governance["vfs_runtime_mounted_reads_error"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "error")
    )
    governance["vfs_runtime_mounted_reads_estale"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "estale")
    )
    governance["vfs_runtime_mounted_reads_cancelled"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "cancelled")
    )
    governance["vfs_runtime_mounted_reads_average_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "average_duration_ms")
    )
    governance["vfs_runtime_mounted_reads_max_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "max_duration_ms")
    )
    governance["vfs_runtime_upstream_fetch_operations"] = _as_int(
        _nested_mapping_value(payload, "upstream_fetch", "operations")
    )
    governance["vfs_runtime_upstream_fetch_bytes_total"] = _as_int(
        _nested_mapping_value(payload, "upstream_fetch", "bytes_total")
    )
    governance["vfs_runtime_upstream_fetch_average_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "upstream_fetch", "average_duration_ms")
    )
    governance["vfs_runtime_upstream_fetch_max_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "upstream_fetch", "max_duration_ms")
    )
    governance["vfs_runtime_upstream_fail_invalid_url"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "invalid_url")
    )
    governance["vfs_runtime_upstream_fail_build_request"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "build_request")
    )
    governance["vfs_runtime_upstream_fail_network"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "network")
    )
    governance["vfs_runtime_upstream_fail_stale_status"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "stale_status")
    )
    governance["vfs_runtime_upstream_fail_unexpected_status"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "unexpected_status")
    )
    governance["vfs_runtime_upstream_fail_unexpected_status_too_many_requests"] = _as_int(
        _nested_mapping_value(
            payload,
            "upstream_failures",
            "unexpected_status_too_many_requests",
        )
    )
    governance["vfs_runtime_upstream_fail_unexpected_status_server_error"] = _as_int(
        _nested_mapping_value(
            payload,
            "upstream_failures",
            "unexpected_status_server_error",
        )
    )
    governance["vfs_runtime_upstream_fail_read_body"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "read_body")
    )
    governance["vfs_runtime_upstream_retryable_network"] = _as_int(
        _nested_mapping_value(payload, "upstream_retryable_events", "network")
    )
    governance["vfs_runtime_upstream_retryable_read_body"] = _as_int(
        _nested_mapping_value(payload, "upstream_retryable_events", "read_body")
    )
    governance["vfs_runtime_upstream_retryable_status_too_many_requests"] = _as_int(
        _nested_mapping_value(
            payload,
            "upstream_retryable_events",
            "status_too_many_requests",
        )
    )
    governance["vfs_runtime_upstream_retryable_status_server_error"] = _as_int(
        _nested_mapping_value(payload, "upstream_retryable_events", "status_server_error")
    )
    governance["vfs_runtime_backend_fallback_attempts"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "attempts")
    )
    governance["vfs_runtime_backend_fallback_success"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "success")
    )
    governance["vfs_runtime_backend_fallback_failure"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "failure")
    )
    governance["vfs_runtime_backend_fallback_attempts_direct_read_failure"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "attempts_direct_read_failure")
    )
    governance["vfs_runtime_backend_fallback_attempts_inline_refresh_unavailable"] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "attempts_inline_refresh_unavailable",
        )
    )
    governance[
        "vfs_runtime_backend_fallback_attempts_post_inline_refresh_failure"
    ] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "attempts_post_inline_refresh_failure",
        )
    )
    governance["vfs_runtime_backend_fallback_success_direct_read_failure"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "success_direct_read_failure")
    )
    governance["vfs_runtime_backend_fallback_success_inline_refresh_unavailable"] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "success_inline_refresh_unavailable",
        )
    )
    governance[
        "vfs_runtime_backend_fallback_success_post_inline_refresh_failure"
    ] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "success_post_inline_refresh_failure",
        )
    )
    governance["vfs_runtime_backend_fallback_failure_direct_read_failure"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "failure_direct_read_failure")
    )
    governance["vfs_runtime_backend_fallback_failure_inline_refresh_unavailable"] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "failure_inline_refresh_unavailable",
        )
    )
    governance[
        "vfs_runtime_backend_fallback_failure_post_inline_refresh_failure"
    ] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "failure_post_inline_refresh_failure",
        )
    )
    governance["vfs_runtime_chunk_cache_hits"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "hits")
    )
    governance["vfs_runtime_chunk_cache_misses"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "misses")
    )
    governance["vfs_runtime_chunk_cache_inserts"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "inserts")
    )
    governance["vfs_runtime_chunk_cache_prefetch_hits"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "prefetch_hits")
    )
    governance["vfs_runtime_prefetch_concurrency_limit"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "concurrency_limit")
    )
    governance["vfs_runtime_prefetch_available_permits"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "available_permits")
    )
    governance["vfs_runtime_prefetch_active_permits"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "active_permits")
    )
    governance["vfs_runtime_prefetch_active_background_tasks"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "active_background_tasks")
    )
    governance["vfs_runtime_prefetch_peak_active_background_tasks"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "peak_active_background_tasks")
    )
    governance["vfs_runtime_prefetch_background_spawned"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "background_spawned")
    )
    governance["vfs_runtime_prefetch_background_backpressure"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "background_backpressure")
    )
    governance["vfs_runtime_prefetch_fairness_denied"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "fairness_denied")
    )
    governance["vfs_runtime_prefetch_global_backpressure_denied"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "global_backpressure_denied")
    )
    governance["vfs_runtime_prefetch_background_error"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "background_error")
    )
    governance["vfs_runtime_chunk_coalescing_in_flight_chunks"] = _as_int(
        _nested_mapping_value(payload, "chunk_coalescing", "in_flight_chunks")
    )
    governance["vfs_runtime_chunk_coalescing_peak_in_flight_chunks"] = _as_int(
        _nested_mapping_value(payload, "chunk_coalescing", "peak_in_flight_chunks")
    )
    governance["vfs_runtime_chunk_coalescing_waits_total"] = _as_int(
        _nested_mapping_value(payload, "chunk_coalescing", "waits_total")
    )
    governance["vfs_runtime_chunk_coalescing_waits_hit"] = _as_int(
        _nested_mapping_value(payload, "chunk_coalescing", "waits_hit")
    )
    governance["vfs_runtime_chunk_coalescing_waits_miss"] = _as_int(
        _nested_mapping_value(payload, "chunk_coalescing", "waits_miss")
    )
    governance["vfs_runtime_chunk_coalescing_wait_average_duration_ms"] = _as_float(
        _nested_mapping_value(payload, "chunk_coalescing", "wait_average_duration_ms")
    )
    governance["vfs_runtime_chunk_coalescing_wait_max_duration_ms"] = _as_float(
        _nested_mapping_value(payload, "chunk_coalescing", "wait_max_duration_ms")
    )
    governance["vfs_runtime_inline_refresh_success"] = _as_int(
        _nested_mapping_value(payload, "inline_refresh", "success")
    )
    governance["vfs_runtime_inline_refresh_no_url"] = _as_int(
        _nested_mapping_value(payload, "inline_refresh", "no_url")
    )
    governance["vfs_runtime_inline_refresh_error"] = _as_int(
        _nested_mapping_value(payload, "inline_refresh", "error")
    )
    governance["vfs_runtime_inline_refresh_timeout"] = _as_int(
        _nested_mapping_value(payload, "inline_refresh", "timeout")
    )
    governance["vfs_runtime_windows_callbacks_cancelled"] = _as_int(
        _nested_mapping_value(payload, "windows_projfs", "callbacks_cancelled")
    )
    governance["vfs_runtime_windows_callbacks_error"] = _as_int(
        _nested_mapping_value(payload, "windows_projfs", "callbacks_error")
    )
    governance["vfs_runtime_windows_callbacks_estale"] = _as_int(
        _nested_mapping_value(payload, "windows_projfs", "callbacks_estale")
    )
    total_cache_lookups = (
        _as_int(governance["vfs_runtime_chunk_cache_hits"])
        + _as_int(governance["vfs_runtime_chunk_cache_misses"])
    )
    governance["vfs_runtime_cache_hit_ratio"] = _safe_ratio(
        _as_int(governance["vfs_runtime_chunk_cache_hits"]),
        total_cache_lookups,
    )
    governance["vfs_runtime_fallback_success_ratio"] = _safe_ratio(
        _as_int(governance["vfs_runtime_backend_fallback_success"]),
        _as_int(governance["vfs_runtime_backend_fallback_attempts"]),
    )
    governance["vfs_runtime_prefetch_pressure_ratio"] = _safe_ratio(
        _as_int(governance["vfs_runtime_prefetch_active_permits"]),
        _as_int(governance["vfs_runtime_prefetch_active_permits"])
        + _as_int(governance["vfs_runtime_prefetch_available_permits"]),
    )
    governance["vfs_runtime_provider_pressure_incidents"] = (
        _as_int(governance["vfs_runtime_upstream_fail_unexpected_status_too_many_requests"])
        + _as_int(governance["vfs_runtime_upstream_fail_unexpected_status_server_error"])
        + _as_int(governance["vfs_runtime_upstream_retryable_status_too_many_requests"])
        + _as_int(governance["vfs_runtime_upstream_retryable_status_server_error"])
        + _as_int(governance["vfs_runtime_prefetch_background_backpressure"])
    )
    governance["vfs_runtime_fairness_pressure_incidents"] = (
        _as_int(governance["vfs_runtime_prefetch_fairness_denied"])
        + _as_int(governance["vfs_runtime_prefetch_global_backpressure_denied"])
    )
    cache_pressure_reasons: list[str] = []
    cache_memory_pressure_ratio = _safe_ratio(
        _as_int(governance["vfs_runtime_chunk_cache_memory_bytes"]),
        _as_int(governance["vfs_runtime_chunk_cache_memory_max_bytes"]),
    )
    cache_disk_pressure_ratio = _safe_ratio(
        _as_int(governance["vfs_runtime_chunk_cache_disk_bytes"]),
        _as_int(governance["vfs_runtime_chunk_cache_disk_max_bytes"]),
    )
    if _as_int(governance["vfs_runtime_chunk_cache_disk_write_errors"]) > 0:
        cache_pressure_reasons.append("disk_write_errors")
    if max(cache_memory_pressure_ratio, cache_disk_pressure_ratio) >= 0.85:
        cache_pressure_reasons.append("cache_capacity_high")
    if _as_int(governance["vfs_runtime_chunk_cache_disk_evictions"]) > 0:
        cache_pressure_reasons.append("disk_evictions_observed")
    governance["vfs_runtime_cache_pressure_class"] = _pressure_class(
        critical=(
            _as_int(governance["vfs_runtime_chunk_cache_disk_write_errors"]) > 0
            or max(cache_memory_pressure_ratio, cache_disk_pressure_ratio) >= 0.95
        ),
        warning=bool(cache_pressure_reasons),
    )
    governance["vfs_runtime_cache_pressure_reasons"] = cache_pressure_reasons

    chunk_pressure_reasons: list[str] = []
    if _as_int(governance["vfs_runtime_chunk_coalescing_waits_miss"]) > 0:
        chunk_pressure_reasons.append("coalescing_wait_misses")
    if _as_float(governance["vfs_runtime_chunk_coalescing_wait_average_duration_ms"]) >= 10.0:
        chunk_pressure_reasons.append("coalescing_wait_latency_high")
    if _as_float(governance["vfs_runtime_chunk_coalescing_wait_max_duration_ms"]) >= 75.0:
        chunk_pressure_reasons.append("coalescing_wait_spike")
    governance["vfs_runtime_chunk_coalescing_pressure_class"] = _pressure_class(
        critical=(
            _as_int(governance["vfs_runtime_chunk_coalescing_waits_miss"]) >= 5
            or _as_float(governance["vfs_runtime_chunk_coalescing_wait_max_duration_ms"]) >= 250.0
        ),
        warning=bool(chunk_pressure_reasons),
    )
    governance["vfs_runtime_chunk_coalescing_pressure_reasons"] = chunk_pressure_reasons

    upstream_wait_reasons: list[str] = []
    if _as_int(governance["vfs_runtime_provider_pressure_incidents"]) > 0:
        upstream_wait_reasons.append("provider_pressure_incidents")
    if _as_int(governance["vfs_runtime_upstream_retryable_network"]) > 0:
        upstream_wait_reasons.append("retryable_network_wait")
    if _as_int(governance["vfs_runtime_upstream_retryable_read_body"]) > 0:
        upstream_wait_reasons.append("retryable_read_body_wait")
    if _as_int(governance["vfs_runtime_upstream_fetch_average_duration_ms"]) >= 50:
        upstream_wait_reasons.append("average_fetch_latency_high")
    if _as_int(governance["vfs_runtime_upstream_fetch_max_duration_ms"]) >= 250:
        upstream_wait_reasons.append("max_fetch_latency_high")
    governance["vfs_runtime_upstream_wait_class"] = _pressure_class(
        critical=(
            _as_int(governance["vfs_runtime_provider_pressure_incidents"]) >= 10
            or _as_int(governance["vfs_runtime_upstream_fetch_average_duration_ms"]) >= 100
            or _as_int(governance["vfs_runtime_upstream_fetch_max_duration_ms"]) >= 500
        ),
        warning=bool(upstream_wait_reasons),
    )
    governance["vfs_runtime_upstream_wait_reasons"] = upstream_wait_reasons

    refresh_pressure_reasons: list[str] = []
    if _as_int(governance["vfs_runtime_backend_fallback_failure"]) > 0:
        refresh_pressure_reasons.append("backend_fallback_failures")
    if _as_int(governance["vfs_runtime_inline_refresh_error"]) > 0:
        refresh_pressure_reasons.append("inline_refresh_errors")
    if _as_int(governance["vfs_runtime_inline_refresh_timeout"]) > 0:
        refresh_pressure_reasons.append("inline_refresh_timeouts")
    if _as_int(governance["vfs_runtime_backend_fallback_attempts"]) > 0:
        refresh_pressure_reasons.append("backend_fallback_activity")
    governance["vfs_runtime_refresh_pressure_class"] = _pressure_class(
        critical=(
            _as_int(governance["vfs_runtime_backend_fallback_failure"]) > 0
            or _as_int(governance["vfs_runtime_inline_refresh_timeout"]) >= 3
        ),
        warning=bool(refresh_pressure_reasons),
    )
    governance["vfs_runtime_refresh_pressure_reasons"] = refresh_pressure_reasons
    rollout_reasons: list[str] = []
    if _as_int(governance["vfs_runtime_backend_fallback_failure"]) > 0:
        rollout_reasons.append("backend_fallback_failures")
    if _as_int(governance["vfs_runtime_mounted_reads_error"]) > 0:
        rollout_reasons.append("mounted_read_errors")
    if _as_int(governance["vfs_runtime_prefetch_background_error"]) > 0:
        rollout_reasons.append("prefetch_background_errors")
    if _as_int(governance["vfs_runtime_chunk_cache_disk_write_errors"]) > 0:
        rollout_reasons.append("disk_cache_write_errors")
    if rollout_reasons:
        governance["vfs_runtime_rollout_readiness"] = "blocked"
        governance["vfs_runtime_rollout_next_action"] = "resolve_blocking_runtime_failures"
    else:
        if _as_int(governance["vfs_runtime_provider_pressure_incidents"]) > 0:
            rollout_reasons.append("provider_pressure_incidents")
        if _as_int(governance["vfs_runtime_fairness_pressure_incidents"]) > 0:
            rollout_reasons.append("fairness_pressure_incidents")
        if _as_int(governance["vfs_runtime_inline_refresh_error"]) > 0:
            rollout_reasons.append("inline_refresh_errors")
        if _as_int(governance["vfs_runtime_chunk_coalescing_waits_miss"]) > 0:
            rollout_reasons.append("chunk_coalescing_misses")
    if governance["vfs_runtime_rollout_readiness"] != "blocked" and rollout_reasons:
        governance["vfs_runtime_rollout_readiness"] = "warning"
        governance["vfs_runtime_rollout_next_action"] = "repeat_soak_and_tune_thresholds"
    elif governance["vfs_runtime_rollout_readiness"] != "blocked":
        governance["vfs_runtime_rollout_readiness"] = "ready"
        governance["vfs_runtime_rollout_next_action"] = "promote_to_next_environment_class"
        rollout_reasons.append("no_blocking_runtime_signals")
    governance["vfs_runtime_rollout_reasons"] = rollout_reasons
    governance["vfs_runtime_active_handle_summaries"] = _as_str_list(
        _nested_mapping_value(payload, "runtime", "active_handle_summaries")
    )
    return _apply_vfs_rollout_policy(
        governance,
        playback_gate_governance=playback_gate_governance,
    )


def _hls_failed_lease_trigger_governance_snapshot() -> dict[str, int]:
    """Return additive governance counters for route-adjacent HLS failed-lease refresh triggering."""

    active_tasks = sum(1 for task in _HLS_FAILED_LEASE_BACKGROUND_ROUTE_TASKS if not task.done())
    return {
        "hls_failed_lease_refresh_trigger_starts": _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE["starts"],
        "hls_failed_lease_refresh_trigger_no_action": _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE[
            "no_action"
        ],
        "hls_failed_lease_refresh_trigger_controller_unavailable": _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE[
            "controller_unavailable"
        ],
        "hls_failed_lease_refresh_trigger_already_pending": _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE[
            "already_pending"
        ],
        "hls_failed_lease_refresh_trigger_backoff_pending": _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE[
            "backoff_pending"
        ],
        "hls_failed_lease_refresh_trigger_failures": _HLS_FAILED_LEASE_TRIGGER_GOVERNANCE[
            "failures"
        ],
        "hls_failed_lease_refresh_trigger_tasks_active": active_tasks,
    }


def _hls_restricted_fallback_trigger_governance_snapshot() -> dict[str, int]:
    """Return additive governance counters for route-adjacent HLS restricted-fallback refresh triggering."""

    active_tasks = sum(
        1 for task in _HLS_RESTRICTED_FALLBACK_BACKGROUND_ROUTE_TASKS if not task.done()
    )
    return {
        "hls_restricted_fallback_refresh_trigger_starts": _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "starts"
        ],
        "hls_restricted_fallback_refresh_trigger_no_action": _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "no_action"
        ],
        "hls_restricted_fallback_refresh_trigger_controller_unavailable": _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "controller_unavailable"
        ],
        "hls_restricted_fallback_refresh_trigger_already_pending": _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "already_pending"
        ],
        "hls_restricted_fallback_refresh_trigger_backoff_pending": _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "backoff_pending"
        ],
        "hls_restricted_fallback_refresh_trigger_failures": _HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "failures"
        ],
        "hls_restricted_fallback_refresh_trigger_tasks_active": active_tasks,
    }


def _is_retryable_remote_hls_error(exc: HTTPException) -> bool:
    """Return whether one remote-HLS HTTP exception is safe to retry briefly."""

    if exc.status_code not in {status.HTTP_502_BAD_GATEWAY, status.HTTP_504_GATEWAY_TIMEOUT}:
        return False
    detail = exc.detail if isinstance(exc.detail, str) else ""
    return detail in {
        "Upstream HLS request timed out",
        "Upstream HLS request transport failed",
        "Upstream playback request timed out",
        "Upstream playback request transport failed",
    }


def _raise_remote_hls_cooldown_if_active(*, cooldown_key: str) -> None:
    """Fail fast when one remote-HLS upstream is in a short cooldown window."""

    current_time = monotonic()
    with _REMOTE_HLS_COOLDOWN_LOCK:
        _cleanup_remote_hls_cooldowns(now=current_time)
        cooldown = _REMOTE_HLS_COOLDOWNS.get(cooldown_key)
        if cooldown is None:
            return
        expires_at, status_code, detail = cooldown
        _REMOTE_HLS_RETRY_GOVERNANCE["cooldown_hits"] += 1
        REMOTE_HLS_RECOVERY_EVENTS.labels(event="cooldown_hit").inc()
        retry_after = max(1, int(expires_at - current_time + 0.999))
    raise HTTPException(
        status_code=status_code,
        detail=detail,
        headers={"Retry-After": str(retry_after)},
    )


def _start_remote_hls_cooldown(*, cooldown_key: str, exc: HTTPException) -> None:
    """Start a short cooldown for one remote-HLS upstream after repeated transient failure."""

    detail = exc.detail if isinstance(exc.detail, str) else "Upstream HLS request transport failed"
    with _REMOTE_HLS_COOLDOWN_LOCK:
        _cleanup_remote_hls_cooldowns()
        _REMOTE_HLS_COOLDOWNS[cooldown_key] = (
            monotonic() + _REMOTE_HLS_COOLDOWN_SECONDS,
            exc.status_code,
            detail,
        )
        _REMOTE_HLS_RETRY_GOVERNANCE["cooldown_starts"] += 1
    REMOTE_HLS_RECOVERY_EVENTS.labels(event="cooldown_start").inc()


async def _run_remote_hls_with_retry(
    *,
    cooldown_key: str,
    operation: Callable[[], Awaitable[Any]],
) -> Any:
    """Run one remote-HLS operation with a single transient retry and short cooldown."""

    _raise_remote_hls_cooldown_if_active(cooldown_key=cooldown_key)
    for attempt in range(_REMOTE_HLS_RETRY_ATTEMPTS):
        try:
            return await operation()
        except HTTPException as exc:
            if not _is_retryable_remote_hls_error(exc):
                raise
            if attempt + 1 >= _REMOTE_HLS_RETRY_ATTEMPTS:
                _start_remote_hls_cooldown(cooldown_key=cooldown_key, exc=exc)
                raise
            _REMOTE_HLS_RETRY_GOVERNANCE["retry_attempts"] += 1
            REMOTE_HLS_RECOVERY_EVENTS.labels(event="retry_attempt").inc()
            await asyncio.sleep(0)

    raise AssertionError("remote HLS retry loop exhausted unexpectedly")


def _validate_upstream_hls_playlist(playlist_text: str) -> None:
    """Validate one upstream HLS playlist before rewriting its child references."""

    lines = playlist_text.splitlines()
    if not lines:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream HLS playlist is empty",
        )

    first_non_empty = next((line.strip() for line in lines if line.strip()), "")
    if first_non_empty != "#EXTM3U":
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream HLS playlist is malformed",
        )

    has_reference = any(line.strip() and not line.strip().startswith("#") for line in lines)
    if not has_reference:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream HLS playlist has no child references",
        )


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

    if not controller.has_pending(item_identifier):
        return False

    governance["already_pending"] += 1
    last_result = controller.get_last_result(item_identifier)
    if last_result is not None and last_result.retry_after_seconds is not None:
        governance["backoff_pending"] += 1
    return True


async def _run_app_scoped_refresh_trigger(
    *,
    request: Request,
    item_identifier: str,
    governance: dict[str, int],
    trigger: Callable[[Any, str], Awaitable[Any]],
) -> None:
    """Trigger one app-scoped refresh controller and record shared route governance."""

    try:
        resources = get_resources(request)
    except RuntimeError:
        governance["controller_unavailable"] += 1
        return

    try:
        result = await trigger(resources, item_identifier)
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


async def _run_direct_playback_refresh_trigger(*, request: Request, item_identifier: str) -> None:
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
    )


def _is_selected_hls_failed_lease_error(exc: HTTPException) -> bool:
    """Return whether one HLS route exception represents a selected failed HLS lease."""

    detail = exc.detail if isinstance(exc.detail, str) else ""
    return exc.status_code == status.HTTP_503_SERVICE_UNAVAILABLE and detail.startswith(
        "Selected HLS playback lease refresh failed"
    )


async def _run_hls_failed_lease_refresh_trigger(*, request: Request, item_identifier: str) -> None:
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
    )


async def _run_hls_restricted_fallback_refresh_trigger(
    *, request: Request, item_identifier: str
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
        or resources.hls_failed_lease_refresh_controller
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
            _run_hls_failed_lease_refresh_trigger(request=request, item_identifier=item_identifier),
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
        or resources.hls_restricted_fallback_refresh_controller
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
        or resources.playback_refresh_controller
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
            _run_direct_playback_refresh_trigger(request=request, item_identifier=item_identifier),
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
    vfs_runtime_governance = _vfs_runtime_governance_snapshot(
        playback_gate_governance=playback_gate_governance,
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
    heavy_stage_exit_ready = int(
        resources.settings.arq_enabled
        and resources.settings.stream.refresh_dispatch_mode == "queued"
        and (
            resources.settings.stream.refresh_dispatch_mode != "queued"
            or (resources.arq_redis is not None and queued_refresh_controllers_attached)
        )
        and heavy_stage_policy.executor_mode == "process_pool_required"
        and heavy_stage_policy.max_tasks_per_child > 0
        and bool(heavy_stage_policy.proof_refs)
        and bool(resources.settings.orchestration.queued_refresh_proof_refs)
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
                "heavy_stage_process_isolation_required": int(
                    heavy_stage_policy.executor_mode == "process_pool_required"
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
