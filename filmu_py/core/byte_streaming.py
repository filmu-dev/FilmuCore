"""Shared byte-serving primitives for local files, generated HLS, and remote proxying."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import tempfile
from asyncio.subprocess import DEVNULL, Process
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import count
from pathlib import Path, PurePosixPath
from time import perf_counter
from typing import Final, Literal

import httpx
from fastapi import HTTPException, Request, status
from fastapi.responses import FileResponse, StreamingResponse
from prometheus_client import Counter, Gauge, Histogram

from .chunk_engine import (
    DEFAULT_CONFIG,
    ChunkCache,
    ChunkConfig,
    calculate_file_chunks,
    detect_read_type,
    fetch_and_stitch,
    iter_fetch_and_stitch,
    resolve_chunks_for_read,
)

HLS_GENERATION_EVENTS = Counter(
    "filmu_py_stream_hls_generation_events_total",
    "Count of local HLS generation lifecycle events by result.",
    labelnames=("result",),
)
STREAM_OPEN_OPERATIONS = Counter(
    "filmu_py_stream_open_operations_total",
    "Count of serving-session opens by owner and category.",
    labelnames=("owner", "category"),
)
STREAM_READ_OPERATIONS = Counter(
    "filmu_py_stream_read_operations_total",
    "Count of serving read operations by owner and category.",
    labelnames=("owner", "category"),
)
STREAM_BYTES_SERVED = Counter(
    "filmu_py_stream_bytes_served_total",
    "Total bytes served by owner and category.",
    labelnames=("owner", "category"),
)
HLS_GENERATION_DURATION_SECONDS = Histogram(
    "filmu_py_stream_hls_generation_duration_seconds",
    "Time spent generating local HLS playlists by result.",
    labelnames=("result",),
)
HLS_TRANSCODE_SPEED_MULTIPLIER = Histogram(
    "filmu_py_stream_hls_transcode_speed_multiplier",
    "Observed ffmpeg transcode speed multiplier for local HLS generation.",
    buckets=(0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 4.0, 8.0),
)
HLS_TRANSCODE_STDERR_EVENTS = Counter(
    "filmu_py_stream_hls_transcode_stderr_events_total",
    "Count of classified ffmpeg stderr outcomes for local HLS generation.",
    labelnames=("kind",),
)
HLS_FFMPEG_FAILURE_EVENTS = Counter(
    "filmu_py_stream_hls_ffmpeg_failure_events_total",
    "Count of local HLS ffmpeg generation failures by classified kind.",
    labelnames=("kind",),
)
HLS_ACTIVE_TRANSCODES = Gauge(
    "filmu_py_stream_hls_active_transcodes",
    "Number of active local HLS ffmpeg processes.",
)
REMOTE_PROXY_OPEN_DURATION_SECONDS = Histogram(
    "filmu_py_stream_remote_proxy_open_duration_seconds",
    "Time spent opening upstream remote proxy streams by HTTP status code.",
    labelnames=("status_code",),
)
STREAM_UPSTREAM_OPENS = Counter(
    "filmu_py_stream_upstream_open_total",
    "Count of upstream remote proxy open operations by owner and HTTP status code.",
    labelnames=("owner", "status_code"),
)
STREAM_ABORT_EVENTS = Counter(
    "filmu_py_stream_abort_events_total",
    "Count of stream abort/cancellation events by owner, category, and reason.",
    labelnames=("owner", "category", "reason"),
)
STREAM_REQUEST_SHAPES = Counter(
    "filmu_py_stream_request_shapes_total",
    "Count of stream request shapes by owner, category, and shape.",
    labelnames=("owner", "category", "shape"),
)
STREAM_RESPONSE_OUTCOMES = Counter(
    "filmu_py_stream_response_outcomes_total",
    "Count of stream response outcomes by owner, category, and outcome.",
    labelnames=("owner", "category", "outcome"),
)
STREAM_ACCESS_PATTERNS = Counter(
    "filmu_py_stream_access_patterns_total",
    "Count of pre-chunk request-side access patterns by owner, category, and pattern.",
    labelnames=("owner", "category", "pattern"),
)
STREAM_READ_SIZE_BYTES = Histogram(
    "filmu_py_stream_read_size_bytes",
    "Per-read byte sizes observed by owner and category.",
    labelnames=("owner", "category"),
    buckets=(1024, 4096, 16384, 65536, 262144, 1048576),
)
STREAM_READ_SIZE_BUCKETS = Counter(
    "filmu_py_stream_read_size_buckets_total",
    "Count of read sizes grouped into lightweight small/medium/large buckets.",
    labelnames=("owner", "category", "bucket"),
)
STREAM_READ_OPERATIONS_PER_SESSION = Histogram(
    "filmu_py_stream_read_operations_per_session",
    "Number of read operations observed for one serving session by owner and category.",
    labelnames=("owner", "category"),
    buckets=(1, 2, 4, 8, 16, 32, 64),
)
STREAM_BYTES_PER_READ_PROXY = Histogram(
    "filmu_py_stream_bytes_per_read_proxy",
    "Average bytes served per read operation for one serving session by owner and category. This is a pre-chunk proxy, not true read amplification.",
    labelnames=("owner", "category"),
    buckets=(1024, 4096, 16384, 65536, 262144, 1048576),
)

_LOCAL_STREAM_CHUNK_SIZE = 1024 * 64
_HLS_OUTPUT_ROOT = Path(tempfile.gettempdir()) / "filmu_py_hls"
_FORWARDED_HEADERS: Final[tuple[str, ...]] = (
    "content-type",
    "content-length",
    "content-range",
    "accept-ranges",
    "content-disposition",
)
_HLS_RETENTION_SECONDS: Final[int] = 60 * 60
_HLS_GENERATION_CONCURRENCY: Final[int] = 2
_HLS_GENERATION_TIMEOUT_SECONDS: Final[int] = 60
_HLS_GOVERNANCE_INTERVAL_SECONDS: Final[int] = 30
_HLS_DISK_HIGH_WATER_BYTES: Final[int] = 2 * 1024 * 1024 * 1024
_HLS_DISK_LOW_WATER_BYTES: Final[int] = 1024 * 1024 * 1024
_HLS_FFMPEG_MAX_TRANSPORT_RETRIES: Final[int] = 1
_SESSION_RETENTION_SECONDS: Final[int] = 15 * 60
_SCAN_PATTERN_MAX_BYTES: Final[int] = 1024 * 1024
_SESSION_RETENTION_BY_OWNER: Final[dict[str, int]] = {
    "http-direct": 15 * 60,
    "http-hls": 15 * 60,
    "future-vfs": 60 * 60,
}
_HLS_GENERATION_SEMAPHORE = asyncio.Semaphore(_HLS_GENERATION_CONCURRENCY)
_HLS_GENERATION_LOCKS: dict[str, asyncio.Lock] = {}


@dataclass(frozen=True, slots=True)
class LocalHlsTranscodeProfile:
    """Browser-safe local HLS transcode settings for generated playlists."""

    pix_fmt: str = "yuv420p"
    profile: str = "high"
    level: str = "4.1"


@dataclass(slots=True)
class _TrackedHlsGeneration:
    """Tracked local HLS generation process metadata."""

    item_id: str
    output_dir: Path
    source_path: str
    source_marker: str
    started_at: datetime
    process: Process | None = None
    monitor_task: asyncio.Task[None] | None = None

_ACTIVE_HLS_GENERATIONS: dict[str, _TrackedHlsGeneration] = {}
_GOVERNANCE_COUNTERS = {
    "hls_cleanup_runs": 0,
    "hls_cleanup_deleted_dirs": 0,
    "hls_cleanup_failed_dirs": 0,
    "hls_stale_segment_reap_runs": 0,
    "hls_stale_segment_reaped_files": 0,
    "hls_stale_segment_reap_failed_files": 0,
    "hls_quota_reap_runs": 0,
    "hls_quota_deleted_dirs": 0,
    "hls_quota_failed_dirs": 0,
    "hls_generation_started": 0,
    "hls_generation_completed": 0,
    "hls_generation_failed": 0,
    "hls_generation_timeouts": 0,
    "hls_generation_capacity_rejections": 0,
    "hls_generation_cancelled": 0,
    "hls_generation_terminated": 0,
    "hls_generation_killed": 0,
    "hls_manifest_invalid": 0,
    "hls_manifest_regenerated": 0,
    "hls_ffmpeg_failures_unavailable": 0,
    "hls_ffmpeg_failures_timeout": 0,
    "hls_ffmpeg_failures_manifest_invalid": 0,
    "hls_ffmpeg_failures_incomplete_output": 0,
    "hls_ffmpeg_failures_empty": 0,
    "hls_ffmpeg_failures_io": 0,
    "hls_ffmpeg_failures_input": 0,
    "hls_ffmpeg_failures_codec": 0,
    "hls_ffmpeg_failures_transport": 0,
    "hls_ffmpeg_failures_unknown": 0,
    "hls_ffmpeg_retry_attempts": 0,
    "hls_ffmpeg_retry_recovered": 0,
    "hls_ffmpeg_retry_suppressed": 0,
    "hls_ffmpeg_cleanup_failures": 0,
    "hls_ffmpeg_cleanup_suppressed_usable_output": 0,
    "stream_abort_events": 0,
    "local_stream_abort_events": 0,
    "remote_stream_abort_events": 0,
    "session_cleanup_runs": 0,
    "stale_sessions_removed": 0,
    "stale_handles_removed": 0,
}
_PATH_SEQUENCE = count(1)
_SESSION_SEQUENCE = count(1)

ServingSessionKind = Literal["local-file", "remote-proxy"]
ServingPathKind = Literal["local-file", "remote-proxy", "generated-hls"]
ServingHandleKind = Literal["local-file", "remote-proxy", "generated-hls"]
ServingOwnerKind = Literal["http-direct", "http-hls", "future-vfs"]
ServingPathNodeKind = Literal["file", "directory", "remote-resource"]


@dataclass(slots=True)
class ServingSession:
    """Runtime record for one active byte-serving session."""

    session_id: str
    category: ServingSessionKind
    owner: ServingOwnerKind
    resource: str
    started_at: datetime
    last_activity_at: datetime
    bytes_served: int = 0
    read_operations: int = 0


@dataclass(slots=True)
class ServingHandle:
    """Runtime record for one active file/path handle on the serving substrate."""

    handle_id: str
    session_id: str
    category: ServingHandleKind
    owner: ServingOwnerKind
    path: str
    path_id: str
    created_at: datetime
    last_activity_at: datetime
    bytes_served: int = 0
    read_offset: int = 0
    read_operations: int = 0


@dataclass(slots=True)
class ServingPath:
    """Runtime record for one path tracked by the serving substrate."""

    path_id: str
    category: ServingPathKind
    node_kind: ServingPathNodeKind
    path: str
    created_at: datetime
    last_activity_at: datetime
    size_bytes: int | None = None
    active_handle_count: int = 0


@dataclass(slots=True)
class ServingDirectoryEntry:
    """One child entry exposed by the serving-path registry."""

    name: str
    path_id: str
    node_kind: ServingPathNodeKind
    path: str


@dataclass(slots=True)
class ServingPathAttributes:
    """Minimal path-attribute view for future mount-oriented `getattr` style use."""

    path_id: str
    node_kind: ServingPathNodeKind
    path: str
    size_bytes: int | None
    active_handle_count: int
    created_at: datetime
    last_activity_at: datetime


_ACTIVE_SESSIONS: dict[str, ServingSession] = {}
_ACTIVE_HANDLES: dict[str, ServingHandle] = {}
_ACTIVE_PATHS: dict[str, ServingPath] = {}
_PATHS_BY_KEY: dict[tuple[str, str], str] = {}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _start_session(
    *, category: ServingSessionKind, owner: ServingOwnerKind, resource: str
) -> ServingSession:
    """Create and register one active serving session."""

    session_number = next(_SESSION_SEQUENCE)
    session = ServingSession(
        session_id=f"session-{session_number}",
        category=category,
        owner=owner,
        resource=resource,
        started_at=_utc_now(),
        last_activity_at=_utc_now(),
    )
    _ACTIVE_SESSIONS[session.session_id] = session
    STREAM_OPEN_OPERATIONS.labels(owner=owner, category=category).inc()
    return session


def _touch_session(session: ServingSession, *, chunk_size: int) -> None:
    """Advance counters for one active serving session."""

    session.bytes_served += chunk_size
    session.last_activity_at = _utc_now()
    STREAM_BYTES_SERVED.labels(owner=session.owner, category=session.category).inc(chunk_size)


def _get_or_create_path(
    *,
    category: ServingPathKind,
    node_kind: ServingPathNodeKind,
    path: str,
    size_bytes: int | None = None,
) -> ServingPath:
    """Return a stable serving-path record for one path/category pair."""

    key = (category, path)
    path_id = _PATHS_BY_KEY.get(key)
    if path_id is None:
        path_id = f"path-{next(_PATH_SEQUENCE)}"
        record = ServingPath(
            path_id=path_id,
            category=category,
            node_kind=node_kind,
            path=path,
            created_at=_utc_now(),
            last_activity_at=_utc_now(),
            size_bytes=size_bytes,
        )
        _PATHS_BY_KEY[key] = path_id
        _ACTIVE_PATHS[path_id] = record
        return record

    record = _ACTIVE_PATHS[path_id]
    record.last_activity_at = _utc_now()
    if size_bytes is not None:
        record.size_bytes = size_bytes
    return record


def _open_handle(
    *,
    session: ServingSession,
    category: ServingHandleKind,
    path: str,
    node_kind: ServingPathNodeKind,
) -> ServingHandle:
    """Create and register one active serving handle underneath one session."""

    path_record = _get_or_create_path(category=category, node_kind=node_kind, path=path)
    path_record.active_handle_count += 1
    path_record.last_activity_at = _utc_now()
    handle = ServingHandle(
        handle_id=f"handle-{session.session_id}",
        session_id=session.session_id,
        category=category,
        owner=session.owner,
        path=path,
        path_id=path_record.path_id,
        created_at=_utc_now(),
        last_activity_at=_utc_now(),
    )
    _ACTIVE_HANDLES[handle.handle_id] = handle
    return handle


def _touch_handle(handle: ServingHandle, *, chunk_size: int) -> None:
    """Advance counters for one active serving handle."""

    handle.bytes_served += chunk_size
    handle.read_offset += chunk_size
    handle.read_operations += 1
    handle.last_activity_at = _utc_now()
    STREAM_READ_OPERATIONS.labels(owner=handle.owner, category=handle.category).inc()
    STREAM_READ_SIZE_BYTES.labels(owner=handle.owner, category=handle.category).observe(chunk_size)
    STREAM_READ_SIZE_BUCKETS.labels(
        owner=handle.owner,
        category=handle.category,
        bucket=_classify_read_size_bucket(chunk_size),
    ).inc()
    path_record = _ACTIVE_PATHS.get(handle.path_id)
    if path_record is not None:
        path_record.last_activity_at = handle.last_activity_at
        if path_record.size_bytes is None:
            path_record.size_bytes = handle.read_offset
    session = _ACTIVE_SESSIONS.get(handle.session_id)
    if session is not None:
        session.read_operations += 1


def _close_handle(handle: ServingHandle) -> None:
    """Remove one serving handle from the active registry."""

    _ACTIVE_HANDLES.pop(handle.handle_id, None)
    path_record = _ACTIVE_PATHS.get(handle.path_id)
    if path_record is not None and path_record.active_handle_count > 0:
        path_record.active_handle_count -= 1
        path_record.last_activity_at = _utc_now()


def _finish_session(session: ServingSession) -> None:
    """Remove one serving session from the active registry."""

    active_session = _ACTIVE_SESSIONS.pop(session.session_id, None)
    if active_session is None:
        return

    if active_session.read_operations > 0:
        STREAM_READ_OPERATIONS_PER_SESSION.labels(
            owner=active_session.owner,
            category=active_session.category,
        ).observe(active_session.read_operations)
        STREAM_BYTES_PER_READ_PROXY.labels(
            owner=active_session.owner,
            category=active_session.category,
        ).observe(active_session.bytes_served / active_session.read_operations)


def _record_stream_abort(
    *,
    owner: ServingOwnerKind,
    category: ServingSessionKind,
    reason: str = "cancelled",
) -> None:
    """Record one stream abort/cancellation event in counters and governance state."""

    STREAM_ABORT_EVENTS.labels(owner=owner, category=category, reason=reason).inc()
    _GOVERNANCE_COUNTERS["stream_abort_events"] += 1
    if category == "local-file":
        _GOVERNANCE_COUNTERS["local_stream_abort_events"] += 1
    else:
        _GOVERNANCE_COUNTERS["remote_stream_abort_events"] += 1


def open_serving_session(
    *, category: ServingSessionKind, owner: ServingOwnerKind, resource: str
) -> ServingSession:
    """Public helper to open one serving session for future mount-oriented consumers."""

    return _start_session(category=category, owner=owner, resource=resource)


def release_serving_session(session: ServingSession) -> None:
    """Public helper to release one serving session from the registry."""

    _finish_session(session)


def get_active_session_snapshot() -> list[ServingSession]:
    """Return a snapshot of currently active serving sessions."""

    return list(_ACTIVE_SESSIONS.values())


def get_active_handle_snapshot() -> list[ServingHandle]:
    """Return a snapshot of currently active serving handles."""

    return list(_ACTIVE_HANDLES.values())


def get_handle_by_id(handle_id: str) -> ServingHandle | None:
    """Return one tracked handle by identifier when it exists."""

    return _ACTIVE_HANDLES.get(handle_id)


def get_active_path_snapshot() -> list[ServingPath]:
    """Return a snapshot of currently tracked serving paths."""

    return list(_ACTIVE_PATHS.values())


def get_path_by_id(path_id: str) -> ServingPath | None:
    """Return one tracked path by identifier when it exists."""

    return _ACTIVE_PATHS.get(path_id)


def open_mount_session(*, resource: str) -> ServingSession:
    """Open one future-VFS-oriented serving session detached from HTTP routes."""

    return open_serving_session(category="local-file", owner="future-vfs", resource=resource)


def get_path_by_key(*, category: ServingPathKind, path: str) -> ServingPath | None:
    """Return one tracked path by its stable `(category, path)` key when it exists."""

    path_id = _PATHS_BY_KEY.get((category, path))
    if path_id is None:
        return None
    return _ACTIVE_PATHS.get(path_id)


def classify_registered_path(path_record: ServingPath) -> ServingPathNodeKind:
    """Return the current node kind for one registered path."""

    return path_record.node_kind


def get_path_attributes(path_record: ServingPath) -> ServingPathAttributes:
    """Return minimal attribute data for one registered path.

    This is the first substrate-level analogue to a future mount-oriented `getattr`
    operation: it exposes stable metadata for a tracked path without binding to HTTP.
    """

    return ServingPathAttributes(
        path_id=path_record.path_id,
        node_kind=path_record.node_kind,
        path=path_record.path,
        size_bytes=path_record.size_bytes,
        active_handle_count=path_record.active_handle_count,
        created_at=path_record.created_at,
        last_activity_at=path_record.last_activity_at,
    )


def get_path_attributes_by_id(path_id: str) -> ServingPathAttributes:
    """Return attributes for one tracked path identifier."""

    path_record = get_path_by_id(path_id)
    if path_record is None:
        raise KeyError(f"unknown path_id={path_id}")
    return get_path_attributes(path_record)


def getattr_for_path_id(path_id: str) -> ServingPathAttributes:
    """Mount-facing alias for retrieving path attributes by identifier."""

    return get_path_attributes_by_id(path_id)


def list_directory_children(directory: ServingPath) -> list[ServingDirectoryEntry]:
    """Return direct child entries for one registered directory path."""

    if directory.node_kind != "directory":
        raise ValueError("only directory paths can list child entries")

    root = Path(directory.path)
    entries: list[ServingDirectoryEntry] = []
    for candidate in _ACTIVE_PATHS.values():
        if candidate.path_id == directory.path_id:
            continue
        candidate_path = Path(candidate.path)
        if candidate_path.parent != root:
            continue
        entries.append(
            ServingDirectoryEntry(
                name=candidate_path.name,
                path_id=candidate.path_id,
                node_kind=candidate.node_kind,
                path=candidate.path,
            )
        )
    return sorted(
        entries, key=lambda entry: (entry.node_kind != "directory", entry.name.casefold())
    )


def list_directory_children_by_id(path_id: str) -> list[ServingDirectoryEntry]:
    """Return direct child entries for one tracked directory identifier."""

    directory = get_path_by_id(path_id)
    if directory is None:
        raise KeyError(f"unknown path_id={path_id}")
    return list_directory_children(directory)


def readdir_for_path_id(path_id: str) -> list[ServingDirectoryEntry]:
    """Mount-facing alias for listing children of a tracked directory path."""

    return list_directory_children_by_id(path_id)


def get_serving_governance_snapshot() -> dict[str, int]:
    """Return counters and limits for the current serving substrate."""

    generated_hls_directories = 0
    if _HLS_OUTPUT_ROOT.exists():
        generated_hls_directories = sum(1 for child in _HLS_OUTPUT_ROOT.iterdir() if child.is_dir())

    hls_disk_usage_bytes = 0
    if _HLS_OUTPUT_ROOT.exists():
        hls_disk_usage_bytes = sum(
            path.stat().st_size for path in _HLS_OUTPUT_ROOT.rglob("*") if path.is_file()
        )

    return {
        "hls_retention_seconds": _HLS_RETENTION_SECONDS,
        "hls_generation_concurrency": _HLS_GENERATION_CONCURRENCY,
        "hls_generation_timeout_seconds": _HLS_GENERATION_TIMEOUT_SECONDS,
        "session_retention_seconds": _SESSION_RETENTION_SECONDS,
        "future_vfs_session_retention_seconds": _SESSION_RETENTION_BY_OWNER["future-vfs"],
        "active_sessions": len(_ACTIVE_SESSIONS),
        "active_handles": len(_ACTIVE_HANDLES),
        "tracked_paths": len(_ACTIVE_PATHS),
        "active_local_sessions": sum(
            1 for session in _ACTIVE_SESSIONS.values() if session.category == "local-file"
        ),
        "active_remote_sessions": sum(
            1 for session in _ACTIVE_SESSIONS.values() if session.category == "remote-proxy"
        ),
        "active_local_handles": sum(
            1 for handle in _ACTIVE_HANDLES.values() if handle.category == "local-file"
        ),
        "hls_cleanup_runs": _GOVERNANCE_COUNTERS["hls_cleanup_runs"],
        "hls_cleanup_deleted_dirs": _GOVERNANCE_COUNTERS["hls_cleanup_deleted_dirs"],
        "hls_cleanup_failed_dirs": _GOVERNANCE_COUNTERS["hls_cleanup_failed_dirs"],
        "hls_stale_segment_reap_runs": _GOVERNANCE_COUNTERS["hls_stale_segment_reap_runs"],
        "hls_stale_segment_reaped_files": _GOVERNANCE_COUNTERS["hls_stale_segment_reaped_files"],
        "hls_stale_segment_reap_failed_files": _GOVERNANCE_COUNTERS[
            "hls_stale_segment_reap_failed_files"
        ],
        "hls_quota_reap_runs": _GOVERNANCE_COUNTERS["hls_quota_reap_runs"],
        "hls_quota_deleted_dirs": _GOVERNANCE_COUNTERS["hls_quota_deleted_dirs"],
        "hls_quota_failed_dirs": _GOVERNANCE_COUNTERS["hls_quota_failed_dirs"],
        "hls_generation_started": _GOVERNANCE_COUNTERS["hls_generation_started"],
        "hls_generation_completed": _GOVERNANCE_COUNTERS["hls_generation_completed"],
        "hls_generation_failed": _GOVERNANCE_COUNTERS["hls_generation_failed"],
        "hls_generation_timeouts": _GOVERNANCE_COUNTERS["hls_generation_timeouts"],
        "hls_generation_capacity_rejections": _GOVERNANCE_COUNTERS[
            "hls_generation_capacity_rejections"
        ],
        "hls_generation_cancelled": _GOVERNANCE_COUNTERS["hls_generation_cancelled"],
        "hls_generation_terminated": _GOVERNANCE_COUNTERS["hls_generation_terminated"],
        "hls_generation_killed": _GOVERNANCE_COUNTERS["hls_generation_killed"],
        "active_hls_generation_processes": len(_ACTIVE_HLS_GENERATIONS),
        "hls_disk_usage_bytes": hls_disk_usage_bytes,
        "hls_manifest_invalid": _GOVERNANCE_COUNTERS["hls_manifest_invalid"],
        "hls_manifest_regenerated": _GOVERNANCE_COUNTERS["hls_manifest_regenerated"],
        "hls_ffmpeg_failures_unavailable": _GOVERNANCE_COUNTERS["hls_ffmpeg_failures_unavailable"],
        "hls_ffmpeg_failures_timeout": _GOVERNANCE_COUNTERS["hls_ffmpeg_failures_timeout"],
        "hls_ffmpeg_failures_manifest_invalid": _GOVERNANCE_COUNTERS[
            "hls_ffmpeg_failures_manifest_invalid"
        ],
        "hls_ffmpeg_failures_incomplete_output": _GOVERNANCE_COUNTERS[
            "hls_ffmpeg_failures_incomplete_output"
        ],
        "hls_ffmpeg_failures_empty": _GOVERNANCE_COUNTERS["hls_ffmpeg_failures_empty"],
        "hls_ffmpeg_failures_io": _GOVERNANCE_COUNTERS["hls_ffmpeg_failures_io"],
        "hls_ffmpeg_failures_input": _GOVERNANCE_COUNTERS["hls_ffmpeg_failures_input"],
        "hls_ffmpeg_failures_codec": _GOVERNANCE_COUNTERS["hls_ffmpeg_failures_codec"],
        "hls_ffmpeg_failures_transport": _GOVERNANCE_COUNTERS["hls_ffmpeg_failures_transport"],
        "hls_ffmpeg_failures_unknown": _GOVERNANCE_COUNTERS["hls_ffmpeg_failures_unknown"],
        "hls_ffmpeg_retry_attempts": _GOVERNANCE_COUNTERS["hls_ffmpeg_retry_attempts"],
        "hls_ffmpeg_retry_recovered": _GOVERNANCE_COUNTERS["hls_ffmpeg_retry_recovered"],
        "hls_ffmpeg_retry_suppressed": _GOVERNANCE_COUNTERS["hls_ffmpeg_retry_suppressed"],
        "hls_ffmpeg_cleanup_failures": _GOVERNANCE_COUNTERS["hls_ffmpeg_cleanup_failures"],
        "hls_ffmpeg_cleanup_suppressed_usable_output": _GOVERNANCE_COUNTERS[
            "hls_ffmpeg_cleanup_suppressed_usable_output"
        ],
        "stream_abort_events": _GOVERNANCE_COUNTERS["stream_abort_events"],
        "local_stream_abort_events": _GOVERNANCE_COUNTERS["local_stream_abort_events"],
        "remote_stream_abort_events": _GOVERNANCE_COUNTERS["remote_stream_abort_events"],
        "session_cleanup_runs": _GOVERNANCE_COUNTERS["session_cleanup_runs"],
        "stale_sessions_removed": _GOVERNANCE_COUNTERS["stale_sessions_removed"],
        "stale_handles_removed": _GOVERNANCE_COUNTERS["stale_handles_removed"],
        "generated_hls_directories": generated_hls_directories,
    }


def cleanup_expired_serving_runtime(*, now: datetime | None = None) -> None:
    """Remove stale serving sessions and handles from the in-memory registries."""

    _GOVERNANCE_COUNTERS["session_cleanup_runs"] += 1
    reference = now or _utc_now()

    def retention_for_owner(owner: ServingOwnerKind) -> int:
        return _SESSION_RETENTION_BY_OWNER[owner]

    stale_handles = [
        handle_id
        for handle_id, handle in _ACTIVE_HANDLES.items()
        if (reference - handle.last_activity_at).total_seconds() > retention_for_owner(handle.owner)
    ]
    for handle_id in stale_handles:
        handle = _ACTIVE_HANDLES.pop(handle_id, None)
        if handle is None:
            continue
        path_record = _ACTIVE_PATHS.get(handle.path_id)
        if path_record is not None and path_record.active_handle_count > 0:
            path_record.active_handle_count -= 1
            path_record.last_activity_at = reference
        _GOVERNANCE_COUNTERS["stale_handles_removed"] += 1

    stale_sessions = [
        session_id
        for session_id, session in _ACTIVE_SESSIONS.items()
        if (reference - session.last_activity_at).total_seconds()
        > retention_for_owner(session.owner)
    ]
    for session_id in stale_sessions:
        if _ACTIVE_SESSIONS.pop(session_id, None) is not None:
            _GOVERNANCE_COUNTERS["stale_sessions_removed"] += 1


def parse_byte_range(range_header: str, file_size: int) -> tuple[int, int]:
    """Parse one HTTP byte range header into an inclusive ``(start, end)`` tuple."""

    if not range_header.startswith("bytes="):
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail="Unsupported range unit",
        )

    start_raw, _, end_raw = range_header.removeprefix("bytes=").partition("-")
    if not start_raw and not end_raw:
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail="Invalid range header",
        )

    if start_raw:
        start = int(start_raw)
        end = int(end_raw) if end_raw else file_size - 1
    else:
        suffix_length = int(end_raw)
        if suffix_length <= 0:
            raise HTTPException(
                status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
                detail="Invalid suffix range",
            )
        start = max(file_size - suffix_length, 0)
        end = file_size - 1

    if start < 0 or start >= file_size or end < start:
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail="Requested range is not satisfiable",
        )

    end = min(end, file_size - 1)
    return start, end


def local_file_headers(
    *, file_size: int, start: int | None = None, end: int | None = None
) -> dict[str, str]:
    """Build direct-file response headers for full or partial content."""

    headers = {"accept-ranges": "bytes"}
    if start is None or end is None:
        headers["content-length"] = str(file_size)
        return headers

    assert start is not None and end is not None
    headers["content-length"] = str((end - start) + 1)
    headers["content-range"] = f"bytes {start}-{end}/{file_size}"
    return headers


def _classify_request_shape(range_header: str | None) -> str:
    """Classify one request as full, range, or suffix-range."""

    if not range_header:
        return "full"
    if range_header.strip().startswith("bytes=-"):
        return "suffix-range"
    return "range"


def _classify_read_size_bucket(chunk_size: int) -> str:
    """Group one read size into a lightweight small/medium/large proxy bucket."""

    if chunk_size <= 16 * 1024:
        return "small"
    if chunk_size <= 256 * 1024:
        return "medium"
    return "large"


def _classify_access_pattern(
    *,
    range_header: str | None,
    start: int | None,
    end: int | None,
    file_size: int,
) -> str:
    """Classify one request into a lightweight pre-chunk access-pattern bucket."""

    if not range_header or start is None or end is None:
        return "full-request"

    assert start is not None and end is not None
    resolved_start = start
    resolved_end = end
    window_size = (resolved_end - resolved_start) + 1
    if window_size > _SCAN_PATTERN_MAX_BYTES:
        return "stream-window"
    if resolved_start == 0:
        return "head-probe"
    if resolved_end == file_size - 1:
        return "tail-probe"
    return "seek-probe"


def forwarded_response_headers(headers: httpx.Headers) -> dict[str, str]:
    """Return the subset of upstream headers safe to forward to the client."""

    result: dict[str, str] = {}
    for header in _FORWARDED_HEADERS:
        value = headers.get(header)
        if value:
            result[header] = value
    return result


def iter_local_file(
    path: Path,
    *,
    start: int,
    end: int,
    owner: ServingOwnerKind = "http-direct",
) -> AsyncGenerator[bytes, None]:
    """Yield one local file range as streaming chunks."""

    async def iterator() -> AsyncGenerator[bytes, None]:
        session = open_serving_session(category="local-file", owner=owner, resource=str(path))
        handle = open_local_file_handle(session=session, path=path)
        with path.open("rb") as file_handle:
            try:
                file_handle.seek(start)
                remaining = (end - start) + 1
                while remaining > 0:
                    chunk = file_handle.read(min(_LOCAL_STREAM_CHUNK_SIZE, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    _touch_session(session, chunk_size=len(chunk))
                    read_from_handle(handle=handle, chunk_size=len(chunk))
                    yield chunk
            except asyncio.CancelledError:
                _record_stream_abort(owner=session.owner, category=session.category)
                raise
            finally:
                release_handle(handle)
                release_serving_session(session)

    return iterator()


def open_local_file_handle(*, session: ServingSession, path: Path) -> ServingHandle:
    """Open one explicit local-file handle for future mount-oriented consumers."""

    _get_or_create_path(
        category="local-file", node_kind="file", path=str(path), size_bytes=path.stat().st_size
    )
    return _open_handle(session=session, category="local-file", path=str(path), node_kind="file")


def register_directory_path(path: Path, *, category: ServingPathKind = "local-file") -> ServingPath:
    """Register one directory path in the serving registry for future mount traversal.

    Directory nodes should inherit the category of the serving surface they belong to so
    future mount code can distinguish local-file, generated-HLS, and remote-backed trees
    without relying on hard-coded assumptions.
    """

    return _get_or_create_path(category=category, node_kind="directory", path=str(path))


def ensure_directory_hierarchy(
    path: Path, *, category: ServingPathKind = "local-file"
) -> list[ServingPath]:
    """Ensure all parent directories for one path exist in the serving registry."""

    created: list[ServingPath] = []
    current = path.parent
    parents: list[Path] = []
    while current != current.parent:
        parents.append(current)
        current = current.parent
    for directory in reversed(parents):
        created.append(register_directory_path(directory, category=category))
    return created


def register_file_path(path: Path, *, category: ServingPathKind = "local-file") -> ServingPath:
    """Register one file path in the serving registry for future mount/file attachment use."""

    ensure_directory_hierarchy(path, category=category)
    return _get_or_create_path(
        category=category,
        node_kind="file",
        path=str(path),
        size_bytes=path.stat().st_size if path.exists() and path.is_file() else None,
    )


def register_remote_resource_path(url: str) -> ServingPath:
    """Register one remote resource path in the serving registry."""

    return _get_or_create_path(category="remote-proxy", node_kind="remote-resource", path=url)


def open_remote_proxy_handle(*, session: ServingSession, url: str) -> ServingHandle:
    """Open one explicit remote-proxy handle for future mount-oriented consumers."""

    _get_or_create_path(category="remote-proxy", node_kind="remote-resource", path=url)
    return _open_handle(
        session=session, category="remote-proxy", path=url, node_kind="remote-resource"
    )


def open_handle_for_path(*, session: ServingSession, path_record: ServingPath) -> ServingHandle:
    """Open one explicit handle from a registered serving path.

    This is the first generic handle-open API that future VFS code can use instead of
    binding directly to the HTTP-specific helpers.
    """

    if path_record.node_kind == "directory":
        raise ValueError("directory paths cannot be opened as byte handles")

    return _open_handle(
        session=session,
        category=path_record.category,
        path=path_record.path,
        node_kind=path_record.node_kind,
    )


def open_handle_for_path_id(*, session: ServingSession, path_id: str) -> ServingHandle:
    """Open one explicit handle from a tracked path identifier."""

    path_record = get_path_by_id(path_id)
    if path_record is None:
        raise KeyError(f"unknown path_id={path_id}")
    return open_handle_for_path(session=session, path_record=path_record)


def read_from_handle(*, handle: ServingHandle, chunk_size: int) -> None:
    """Advance one handle explicitly as read activity occurs."""

    _touch_handle(handle, chunk_size=chunk_size)


def release_handle(handle: ServingHandle) -> None:
    """Release one explicit handle from the shared serving registry."""

    _close_handle(handle)


def stream_local_file(
    path: Path,
    request: Request,
    *,
    owner: ServingOwnerKind = "http-direct",
) -> FileResponse | StreamingResponse:
    """Serve one local file with explicit byte-range behavior."""

    file_size = path.stat().st_size
    range_header = request.headers.get("range")
    request_shape = _classify_request_shape(range_header)
    STREAM_REQUEST_SHAPES.labels(owner=owner, category="local-file", shape=request_shape).inc()
    access_pattern = _classify_access_pattern(
        range_header=range_header,
        start=None,
        end=None,
        file_size=file_size,
    )
    if not range_header:
        STREAM_ACCESS_PATTERNS.labels(
            owner=owner,
            category="local-file",
            pattern=access_pattern,
        ).inc()
        STREAM_RESPONSE_OUTCOMES.labels(
            owner=owner,
            category="local-file",
            outcome="full",
        ).inc()
        return FileResponse(path, headers=local_file_headers(file_size=file_size))

    start, end = parse_byte_range(range_header, file_size)
    access_pattern = _classify_access_pattern(
        range_header=range_header,
        start=start,
        end=end,
        file_size=file_size,
    )
    STREAM_ACCESS_PATTERNS.labels(
        owner=owner,
        category="local-file",
        pattern=access_pattern,
    ).inc()
    STREAM_RESPONSE_OUTCOMES.labels(
        owner=owner,
        category="local-file",
        outcome="partial",
    ).inc()
    return StreamingResponse(
        iter_local_file(path, start=start, end=end, owner=owner),
        status_code=status.HTTP_206_PARTIAL_CONTENT,
        headers=local_file_headers(file_size=file_size, start=start, end=end),
        media_type="application/octet-stream",
    )


def resolve_safe_child_path(root: Path, child_path: str) -> Path:
    """Resolve one child path safely underneath a trusted root directory."""

    root = root.resolve()
    candidate = (root / child_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid HLS file path",
        ) from exc
    return candidate


def _remove_directory_tree(path: Path) -> bool:
    """Remove one directory tree best-effort and report whether it succeeded."""

    if not path.exists():
        return True

    try:
        for nested in sorted(path.rglob("*"), reverse=True):
            if nested.is_file():
                nested.unlink(missing_ok=True)
            elif nested.is_dir():
                nested.rmdir()
        path.rmdir()
    except OSError:
        return False
    return True


def _cleanup_failed_hls_generation_output(
    output_dir: Path,
    *,
    playlist_path: Path | None = None,
    suppress_if_usable: bool = False,
) -> bool:
    """Remove failed generated-HLS output unless an already-usable playlist should be preserved."""

    if suppress_if_usable and playlist_path is not None and is_usable_local_hls_playlist(playlist_path):
        _GOVERNANCE_COUNTERS["hls_ffmpeg_cleanup_suppressed_usable_output"] += 1
        return True
    cleaned = _remove_directory_tree(output_dir)
    if not cleaned:
        _GOVERNANCE_COUNTERS["hls_ffmpeg_cleanup_failures"] += 1
    return cleaned


def _hls_generation_lock(item_id: str) -> asyncio.Lock:
    """Return the per-item lock used to avoid duplicate HLS generation work."""

    return _HLS_GENERATION_LOCKS.setdefault(item_id, asyncio.Lock())


def local_hls_directory(item_id: str, *, source_marker: str | None = None) -> Path:
    """Return the cache directory used for generated HLS assets for one item or one item/profile variant."""

    item_root = _HLS_OUTPUT_ROOT / hashlib.sha256(item_id.encode("utf-8")).hexdigest()
    if source_marker is None:
        return item_root
    return item_root / hashlib.sha256(source_marker.encode("utf-8")).hexdigest()


def _extract_ffmpeg_speed(stderr_text: str) -> float | None:
    """Extract the final ffmpeg speed multiplier when present in stderr output."""

    matches = re.findall(r"speed=\s*([0-9]+(?:\.[0-9]+)?)x", stderr_text)
    if not matches:
        return None
    try:
        return float(matches[-1])
    except ValueError:
        return None


def _classify_ffmpeg_stderr_kind(stderr_text: str) -> str:
    """Return a coarse operator-facing classification for ffmpeg stderr output."""

    lowered = stderr_text.lower()
    if not lowered.strip():
        return "empty"
    if "no such file or directory" in lowered or "input/output error" in lowered:
        return "io"
    if "invalid data found" in lowered or "invalid argument" in lowered:
        return "input"
    if "conversion failed" in lowered or "error while decoding" in lowered:
        return "codec"
    if "connection reset" in lowered or "timed out" in lowered:
        return "transport"
    return "unknown"


def _classify_generated_playlist_failure_kind(playlist_path: Path) -> str:
    """Classify one generated playlist failure when ffmpeg exits without a complete output."""

    if not playlist_path.exists():
        return "incomplete_output"
    try:
        referenced = referenced_local_hls_files(playlist_path)
    except (HTTPException, OSError, UnicodeError):
        return "manifest_invalid"
    return "incomplete_output" if not all(path.is_file() for path in referenced) else "unknown"


def _record_hls_ffmpeg_failure_kind(kind: str) -> None:
    """Record one classified local-HLS ffmpeg failure for operator surfaces."""

    HLS_FFMPEG_FAILURE_EVENTS.labels(kind=kind).inc()
    counter_key = f"hls_ffmpeg_failures_{kind}"
    if counter_key in _GOVERNANCE_COUNTERS:
        _GOVERNANCE_COUNTERS[counter_key] += 1


def _should_retry_hls_ffmpeg_failure(*, kind: str, attempt: int) -> bool:
    """Return whether one classified HLS generation failure is worth retrying once."""

    return kind == "transport" and attempt < _HLS_FFMPEG_MAX_TRANSPORT_RETRIES


def _record_hls_ffmpeg_observability(stderr_text: str) -> None:
    """Publish speed and stderr classification metrics for one ffmpeg run."""

    speed = _extract_ffmpeg_speed(stderr_text)
    if speed is not None:
        HLS_TRANSCODE_SPEED_MULTIPLIER.observe(speed)
    HLS_TRANSCODE_STDERR_EVENTS.labels(kind=_classify_ffmpeg_stderr_kind(stderr_text)).inc()


def _register_hls_generation(
    item_id: str, *, output_dir: Path, source_path: str, source_marker: str
) -> None:
    """Reserve one HLS generation slot or reject immediately when saturated."""

    if (
        item_id not in _ACTIVE_HLS_GENERATIONS
        and len(_ACTIVE_HLS_GENERATIONS) >= _HLS_GENERATION_CONCURRENCY
    ):
        _GOVERNANCE_COUNTERS["hls_generation_capacity_rejections"] += 1
        HLS_GENERATION_EVENTS.labels(result="capacity_rejected").inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="HLS generation capacity exceeded",
            headers={"Retry-After": "5"},
        )

    _ACTIVE_HLS_GENERATIONS[item_id] = _TrackedHlsGeneration(
        item_id=item_id,
        output_dir=output_dir,
        source_path=source_path,
        source_marker=source_marker,
        started_at=datetime.now(UTC),
    )
    HLS_ACTIVE_TRANSCODES.set(len(_ACTIVE_HLS_GENERATIONS))


def _attach_hls_process(item_id: str, process: Process) -> None:
    """Attach the created ffmpeg process to one tracked generation slot."""

    tracked = _ACTIVE_HLS_GENERATIONS.get(item_id)
    if tracked is not None:
        tracked.process = process


def _release_hls_generation(item_id: str) -> None:
    """Release one tracked HLS generation slot."""

    _ACTIVE_HLS_GENERATIONS.pop(item_id, None)
    HLS_ACTIVE_TRANSCODES.set(len(_ACTIVE_HLS_GENERATIONS))


async def _wait_for_process_exit(process: Process) -> None:
    """Wait for one ffmpeg process to exit using the best available primitive."""

    await process.wait()


async def _terminate_hls_process(process: Process) -> None:
    """Terminate one ffmpeg process aggressively enough to avoid zombies."""

    if process.returncode is not None:
        return
    process.terminate()
    _GOVERNANCE_COUNTERS["hls_generation_terminated"] += 1
    try:
        await asyncio.wait_for(_wait_for_process_exit(process), timeout=2.0)
        return
    except TimeoutError:
        pass
    if process.returncode is None:
        process.kill()
        _GOVERNANCE_COUNTERS["hls_generation_killed"] += 1
        await _wait_for_process_exit(process)


def _iter_hls_directories() -> list[Path]:
    """Return generated HLS variant directories when the root exists."""

    if not _HLS_OUTPUT_ROOT.exists():
        return []
    directories: list[Path] = []
    for child in _HLS_OUTPUT_ROOT.iterdir():
        if not child.is_dir():
            continue
        variants = [grandchild for grandchild in child.iterdir() if grandchild.is_dir()]
        if variants:
            directories.extend(variants)
            continue
        directories.append(child)
    return directories


def _remove_empty_hls_item_root(directory: Path) -> None:
    """Remove one empty item-level HLS root after its final variant directory is deleted."""

    parent = directory.parent
    if parent == _HLS_OUTPUT_ROOT or not parent.exists():
        return
    try:
        next(parent.iterdir())
    except StopIteration:
        try:
            parent.rmdir()
        except OSError:
            return
    except OSError:
        return


def _hls_directory_size_bytes(path: Path) -> int:
    """Return the total on-disk byte size of one generated HLS directory."""

    return sum(file_path.stat().st_size for file_path in path.rglob("*") if file_path.is_file())


def _cleanup_stale_hls_segments(*, now: datetime) -> None:
    """Reap stalled generated segment directories whose segment files exceeded retention."""

    _GOVERNANCE_COUNTERS["hls_stale_segment_reap_runs"] += 1
    reference_seconds = now.timestamp()
    active_dirs = {tracked.output_dir.resolve() for tracked in _ACTIVE_HLS_GENERATIONS.values()}
    for directory in _iter_hls_directories():
        if directory.resolve() in active_dirs:
            continue
        segment_files = [child for child in directory.rglob("*.ts") if child.is_file()]
        stale_segments = [
            child
            for child in segment_files
            if reference_seconds - child.stat().st_mtime > _HLS_RETENTION_SECONDS
        ]
        if not stale_segments:
            continue
        stale_count = len(stale_segments)
        if _remove_directory_tree(directory):
            _GOVERNANCE_COUNTERS["hls_stale_segment_reaped_files"] += stale_count
            _remove_empty_hls_item_root(directory)
            continue
        _GOVERNANCE_COUNTERS["hls_stale_segment_reap_failed_files"] += stale_count


def _cleanup_hls_disk_quota() -> None:
    """Enforce a high-water/low-water disk quota for generated HLS directories."""

    _GOVERNANCE_COUNTERS["hls_quota_reap_runs"] += 1
    directories = _iter_hls_directories()
    total_bytes = sum(_hls_directory_size_bytes(directory) for directory in directories)
    if total_bytes <= _HLS_DISK_HIGH_WATER_BYTES:
        return
    active_dirs = {tracked.output_dir.resolve() for tracked in _ACTIVE_HLS_GENERATIONS.values()}
    for directory in sorted(directories, key=lambda candidate: candidate.stat().st_mtime):
        if total_bytes <= _HLS_DISK_LOW_WATER_BYTES:
            break
        if directory.resolve() in active_dirs:
            continue
        directory_size = _hls_directory_size_bytes(directory)
        if _remove_directory_tree(directory):
            total_bytes -= directory_size
            _GOVERNANCE_COUNTERS["hls_quota_deleted_dirs"] += 1
            _remove_empty_hls_item_root(directory)
            continue
        _GOVERNANCE_COUNTERS["hls_quota_failed_dirs"] += 1


def mark_local_hls_activity(path: Path) -> None:
    """Touch one generated HLS file and its parent directory to reflect active use."""

    if path.exists():
        path.touch(exist_ok=True)
    if path.parent.exists():
        path.parent.touch(exist_ok=True)


def build_local_hls_source_marker(
    source_path: str, *, transcode_profile: LocalHlsTranscodeProfile
) -> str:
    """Serialize the source identity and transcode contract for cache validation."""

    return json.dumps(
        {
            "source_path": source_path,
            "pix_fmt": transcode_profile.pix_fmt,
            "profile": transcode_profile.profile,
            "level": transcode_profile.level,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def local_hls_source_marker(output_dir: Path) -> Path:
    """Return the marker file that records which source produced one generated HLS directory."""

    return output_dir / ".source.txt"


def _read_local_hls_source_marker(output_dir: Path) -> str | None:
    """Return the recorded HLS generation source when present."""

    marker = local_hls_source_marker(output_dir)
    if not marker.is_file():
        return None
    try:
        content = marker.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return content or None


def _write_local_hls_source_marker(output_dir: Path, *, source_marker: str) -> None:
    """Persist the generation source contract for one HLS output directory."""

    local_hls_source_marker(output_dir).write_text(source_marker, encoding="utf-8")


def is_usable_local_hls_playlist(playlist_path: Path) -> bool:
    """Return whether one generated playlist is usable for incremental playback."""

    if not playlist_path.is_file():
        return False
    try:
        referenced = referenced_local_hls_files(playlist_path)
    except (HTTPException, OSError, UnicodeError):
        return False
    if not referenced:
        return False
    return all(path.is_file() for path in referenced)


def _is_usable_local_hls_playlist_for_source(playlist_path: Path, *, source_marker: str) -> bool:
    """Return whether one generated playlist is already usable and still belongs to the same source contract."""

    if not is_usable_local_hls_playlist(playlist_path):
        return False
    recorded_source = _read_local_hls_source_marker(playlist_path.parent)
    return recorded_source == source_marker


async def _wait_for_local_hls_playlist_ready(playlist_path: Path, *, process: Process) -> None:
    """Wait until one playlist has enough initial output for client playback or ffmpeg exits."""

    while True:
        if is_usable_local_hls_playlist(playlist_path):
            return
        if process.returncode is not None:
            return
        await asyncio.sleep(0.25)


async def _monitor_hls_generation_completion(
    item_id: str,
    *,
    process: Process,
    playlist_path: Path,
    output_dir: Path,
    generation_started: float,
) -> None:
    """Observe one background ffmpeg process after initial playlist readiness is reached."""

    stderr_text = ""
    try:
        _stdout, stderr = await process.communicate()
        stderr_text = (stderr or b"").decode("utf-8", errors="replace").strip()
        if process.returncode != 0 or not is_complete_local_hls_playlist(playlist_path):
            failure_kind = (
                _classify_ffmpeg_stderr_kind(stderr_text)
                if process.returncode != 0
                else _classify_generated_playlist_failure_kind(playlist_path)
            )
            _record_hls_ffmpeg_failure_kind(failure_kind)
            _GOVERNANCE_COUNTERS["hls_generation_failed"] += 1
            HLS_GENERATION_EVENTS.labels(result="failed").inc()
            HLS_GENERATION_DURATION_SECONDS.labels(result="failed").observe(
                perf_counter() - generation_started
            )
            _cleanup_failed_hls_generation_output(
                output_dir,
                playlist_path=playlist_path,
                suppress_if_usable=True,
            )
            return

        _GOVERNANCE_COUNTERS["hls_generation_completed"] += 1
        HLS_GENERATION_EVENTS.labels(result="completed").inc()
        HLS_GENERATION_DURATION_SECONDS.labels(result="completed").observe(
            perf_counter() - generation_started
        )
    except asyncio.CancelledError:
        await _terminate_hls_process(process)
        await process.communicate()
        _cleanup_failed_hls_generation_output(
            output_dir,
            playlist_path=playlist_path,
            suppress_if_usable=True,
        )
        raise
    finally:
        _record_hls_ffmpeg_observability(stderr_text)
        _release_hls_generation(item_id)

def _is_complete_local_hls_playlist_for_source(
    playlist_path: Path, *, source_marker: str
) -> bool:
    """Return whether one generated playlist is complete and still belongs to the same source contract."""

    if not is_complete_local_hls_playlist(playlist_path):
        return False
    recorded_source = _read_local_hls_source_marker(playlist_path.parent)
    return recorded_source == source_marker


def cleanup_expired_hls_dirs(*, now: datetime | None = None) -> None:
    """Remove expired generated-HLS directories opportunistically."""

    _GOVERNANCE_COUNTERS["hls_cleanup_runs"] += 1

    if not _HLS_OUTPUT_ROOT.exists():
        return

    reference = now or datetime.now(UTC)
    for child in _iter_hls_directories():
        age_seconds = reference.timestamp() - child.stat().st_mtime
        if age_seconds <= _HLS_RETENTION_SECONDS:
            continue
        if _remove_directory_tree(child):
            _GOVERNANCE_COUNTERS["hls_cleanup_deleted_dirs"] += 1
            _remove_empty_hls_item_root(child)
            continue
        _GOVERNANCE_COUNTERS["hls_cleanup_failed_dirs"] += 1

    _cleanup_stale_hls_segments(now=reference)
    _cleanup_hls_disk_quota()


def referenced_local_hls_files(playlist_path: Path) -> set[Path]:
    """Return the local child files referenced by one generated HLS playlist."""

    referenced: set[Path] = {playlist_path.resolve()}
    playlist_root = playlist_path.parent.resolve()
    lines = playlist_path.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Generated HLS playlist is empty",
        )
    first_non_empty = next((line.strip() for line in lines if line.strip()), "")
    if first_non_empty != "#EXTM3U":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Generated HLS playlist is malformed",
        )
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith(("http://", "https://")):
            continue
        candidate = resolve_safe_child_path(playlist_root, stripped)
        referenced.add(candidate.resolve())
    if len(referenced) <= 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Generated HLS playlist has no media segments",
        )
    return referenced


def resolve_referenced_local_hls_file(playlist_path: Path, child_path: str) -> Path:
    """Resolve one HLS child path by matching against the trusted playlist file set."""

    requested = PurePosixPath(child_path)
    if requested.is_absolute() or ".." in requested.parts:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid HLS file path",
        )

    normalized_child = requested.as_posix().lstrip("./")
    if not normalized_child or normalized_child.startswith("/"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid HLS file path",
        )

    playlist_root = playlist_path.parent.resolve()
    for referenced in referenced_local_hls_files(playlist_path):
        try:
            relative = referenced.relative_to(playlist_root).as_posix()
        except ValueError:
            continue
        if relative == normalized_child:
            return referenced

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Generated HLS file is missing",
    )


def is_complete_local_hls_playlist(playlist_path: Path) -> bool:
    """Return whether one generated local HLS playlist looks complete on disk."""

    if not playlist_path.is_file():
        return False
    try:
        referenced = referenced_local_hls_files(playlist_path)
    except (HTTPException, OSError, UnicodeError):
        return False
    return all(path.is_file() for path in referenced)


async def ensure_local_hls_playlist(
    source_path: str,
    item_id: str,
    *,
    transcode_profile: LocalHlsTranscodeProfile | None = None,
) -> Path:
    """Generate a local HLS playlist for one file-backed item when needed."""

    transcode_profile = transcode_profile or LocalHlsTranscodeProfile()
    source_marker = build_local_hls_source_marker(
        source_path,
        transcode_profile=transcode_profile,
    )
    cleanup_expired_hls_dirs()
    output_dir = local_hls_directory(item_id, source_marker=source_marker)
    playlist_path = output_dir / "index.m3u8"
    tracked_generation = _ACTIVE_HLS_GENERATIONS.get(item_id)
    if _is_complete_local_hls_playlist_for_source(playlist_path, source_marker=source_marker):
        return playlist_path
    if tracked_generation is not None and tracked_generation.source_marker == source_marker:
        if _is_usable_local_hls_playlist_for_source(playlist_path, source_marker=source_marker):
            return playlist_path
    elif playlist_path.exists() and not _is_complete_local_hls_playlist_for_source(
        playlist_path,
        source_marker=source_marker,
    ):
        _GOVERNANCE_COUNTERS["hls_manifest_invalid"] += 1
        _GOVERNANCE_COUNTERS["hls_manifest_regenerated"] += 1
        _remove_directory_tree(output_dir)

    lock = _hls_generation_lock(item_id)
    async with lock:
        tracked_generation = _ACTIVE_HLS_GENERATIONS.get(item_id)
        if _is_complete_local_hls_playlist_for_source(playlist_path, source_marker=source_marker):
            return playlist_path
        if tracked_generation is not None and tracked_generation.source_marker == source_marker:
            if _is_usable_local_hls_playlist_for_source(
                playlist_path,
                source_marker=source_marker,
            ):
                return playlist_path
        elif output_dir.exists() and any(output_dir.iterdir()):
            _remove_directory_tree(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)
        segment_pattern = output_dir / "segment_%05d.ts"
        _register_hls_generation(
            item_id,
            output_dir=output_dir,
            source_path=source_path,
            source_marker=source_marker,
        )
        _write_local_hls_source_marker(output_dir, source_marker=source_marker)

        async with _HLS_GENERATION_SEMAPHORE:
            attempt = 0
            while True:
                if attempt > 0:
                    output_dir.mkdir(parents=True, exist_ok=True)
                    _write_local_hls_source_marker(output_dir, source_marker=source_marker)

                _GOVERNANCE_COUNTERS["hls_generation_started"] += 1
                HLS_GENERATION_EVENTS.labels(result="started").inc()
                generation_started = perf_counter()
                stderr_text = ""
                try:
                    process = await asyncio.create_subprocess_exec(
                        "ffmpeg",
                        "-y",
                        "-i",
                        source_path,
                        "-map",
                        "0:v:0",
                        "-map",
                        "0:a:0?",
                        "-map_metadata",
                        "-1",
                        "-map_chapters",
                        "-1",
                        "-sn",
                        "-dn",
                        "-preset",
                        "ultrafast",
                        "-c:v",
                        "libx264",
                        "-pix_fmt",
                        transcode_profile.pix_fmt,
                        "-profile:v",
                        transcode_profile.profile,
                        "-level:v",
                        transcode_profile.level,
                        "-sc_threshold",
                        "0",
                        "-force_key_frames",
                        "expr:gte(t,n_forced*2)",
                        "-g",
                        "48",
                        "-keyint_min",
                        "48",
                        "-c:a",
                        "aac",
                        "-ac",
                        "2",
                        "-ar",
                        "48000",
                        "-b:a",
                        "192k",
                        "-f",
                        "hls",
                        "-hls_time",
                        "2",
                        "-hls_list_size",
                        "0",
                        "-hls_playlist_type",
                        "event",
                        "-hls_flags",
                        "independent_segments",
                        "-hls_segment_filename",
                        str(segment_pattern),
                        str(playlist_path),
                        stdout=DEVNULL,
                        stderr=DEVNULL,
                    )
                    _attach_hls_process(item_id, process)
                except FileNotFoundError as exc:
                    _record_hls_ffmpeg_failure_kind("unavailable")
                    HLS_GENERATION_EVENTS.labels(result="unavailable").inc()
                    HLS_GENERATION_DURATION_SECONDS.labels(result="unavailable").observe(
                        perf_counter() - generation_started
                    )
                    _release_hls_generation(item_id)
                    raise HTTPException(
                        status_code=status.HTTP_501_NOT_IMPLEMENTED,
                        detail="ffmpeg is not available for HLS generation",
                    ) from exc
                except Exception:
                    _release_hls_generation(item_id)
                    raise

                try:
                    await asyncio.wait_for(
                        _wait_for_local_hls_playlist_ready(playlist_path, process=process),
                        timeout=_HLS_GENERATION_TIMEOUT_SECONDS,
                    )
                except asyncio.CancelledError:
                    _GOVERNANCE_COUNTERS["hls_generation_cancelled"] += 1
                    HLS_GENERATION_EVENTS.labels(result="cancelled").inc()
                    await _terminate_hls_process(process)
                    await process.communicate()
                    _cleanup_failed_hls_generation_output(output_dir)
                    _release_hls_generation(item_id)
                    raise
                except TimeoutError as exc:
                    _record_hls_ffmpeg_failure_kind("timeout")
                    _GOVERNANCE_COUNTERS["hls_generation_failed"] += 1
                    _GOVERNANCE_COUNTERS["hls_generation_timeouts"] += 1
                    HLS_GENERATION_EVENTS.labels(result="timeout").inc()
                    HLS_GENERATION_DURATION_SECONDS.labels(result="timeout").observe(
                        perf_counter() - generation_started
                    )
                    await _terminate_hls_process(process)
                    await process.communicate()
                    _cleanup_failed_hls_generation_output(output_dir)
                    _release_hls_generation(item_id)
                    raise HTTPException(
                        status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                        detail="HLS generation timed out",
                    ) from exc

                if process.returncode is None and _is_usable_local_hls_playlist_for_source(
                    playlist_path,
                    source_marker=source_marker,
                ):
                    tracked_generation = _ACTIVE_HLS_GENERATIONS.get(item_id)
                    if tracked_generation is not None:
                        tracked_generation.monitor_task = asyncio.create_task(
                            _monitor_hls_generation_completion(
                                item_id,
                                process=process,
                                playlist_path=playlist_path,
                                output_dir=output_dir,
                                generation_started=generation_started,
                            )
                        )
                    if attempt > 0:
                        _GOVERNANCE_COUNTERS["hls_ffmpeg_retry_recovered"] += 1
                    return playlist_path

                _stdout, stderr = await process.communicate()
                stderr_text = (stderr or b"").decode("utf-8", errors="replace").strip()
                if process.returncode != 0 or not playlist_path.is_file():
                    failure_kind = _classify_ffmpeg_stderr_kind(stderr_text)
                    _record_hls_ffmpeg_failure_kind(failure_kind)
                    if _should_retry_hls_ffmpeg_failure(kind=failure_kind, attempt=attempt):
                        _GOVERNANCE_COUNTERS["hls_ffmpeg_retry_attempts"] += 1
                        if not _cleanup_failed_hls_generation_output(output_dir):
                            _GOVERNANCE_COUNTERS["hls_ffmpeg_retry_suppressed"] += 1
                        else:
                            attempt += 1
                            continue
                    _GOVERNANCE_COUNTERS["hls_generation_failed"] += 1
                    HLS_GENERATION_EVENTS.labels(result="failed").inc()
                    HLS_GENERATION_DURATION_SECONDS.labels(result="failed").observe(
                        perf_counter() - generation_started
                    )
                    _cleanup_failed_hls_generation_output(output_dir)
                    _record_hls_ffmpeg_observability(stderr_text)
                    _release_hls_generation(item_id)
                    detail = stderr_text or "HLS generation failed"
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=detail,
                    )
                if not is_complete_local_hls_playlist(playlist_path):
                    failure_kind = _classify_generated_playlist_failure_kind(playlist_path)
                    _record_hls_ffmpeg_failure_kind(failure_kind)
                    _GOVERNANCE_COUNTERS["hls_generation_failed"] += 1
                    if failure_kind == "manifest_invalid":
                        _GOVERNANCE_COUNTERS["hls_manifest_invalid"] += 1
                    HLS_GENERATION_EVENTS.labels(result="failed").inc()
                    HLS_GENERATION_DURATION_SECONDS.labels(result="failed").observe(
                        perf_counter() - generation_started
                    )
                    _cleanup_failed_hls_generation_output(output_dir)
                    _record_hls_ffmpeg_observability(stderr_text)
                    _release_hls_generation(item_id)
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=(
                            "Generated HLS playlist is malformed"
                            if failure_kind == "manifest_invalid"
                            else "HLS generation completed without a complete playlist"
                        ),
                    )
                _record_hls_ffmpeg_observability(stderr_text)
                _GOVERNANCE_COUNTERS["hls_generation_completed"] += 1
                HLS_GENERATION_EVENTS.labels(result="completed").inc()
                HLS_GENERATION_DURATION_SECONDS.labels(result="completed").observe(
                    perf_counter() - generation_started
                )
                if attempt > 0:
                    _GOVERNANCE_COUNTERS["hls_ffmpeg_retry_recovered"] += 1
                _release_hls_generation(item_id)
                break

    return playlist_path

async def run_hls_governance_loop() -> None:
    """Periodically enforce HLS cleanup and quota governance."""

    try:
        while True:
            cleanup_expired_hls_dirs()
            await asyncio.sleep(_HLS_GOVERNANCE_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        raise


def rewrite_local_hls_playlist(*, playlist_text: str, item_id: str, query_string: str = "") -> str:
    """Rewrite generated local HLS playlists to route segment fetches through the BFF path."""

    suffix = f"?{query_string}" if query_string else ""
    rewritten: list[str] = []
    for line in playlist_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            rewritten.append(line)
            continue
        rewritten.append(f"/api/stream/{item_id}/hls/{stripped.lstrip('/')}{suffix}")
    return "\n".join(rewritten) + "\n"


async def stream_remote(
    url: str,
    request: Request,
    *,
    owner: ServingOwnerKind = "http-direct",
) -> StreamingResponse:
    """Proxy one remote byte stream while forwarding range semantics when present."""

    forward_headers: dict[str, str] = {}
    range_header = request.headers.get("range")
    request_shape = _classify_request_shape(range_header)
    STREAM_REQUEST_SHAPES.labels(owner=owner, category="remote-proxy", shape=request_shape).inc()
    access_pattern = _classify_access_pattern(
        range_header=range_header,
        start=None,
        end=None,
        file_size=1,
    )
    if range_header:
        forward_headers["Range"] = range_header
        start, end = parse_byte_range(range_header, file_size=2**63 - 1)
        access_pattern = _classify_access_pattern(
            range_header=range_header,
            start=start,
            end=end,
            file_size=2**63 - 1,
        )

    STREAM_ACCESS_PATTERNS.labels(
        owner=owner,
        category="remote-proxy",
        pattern=access_pattern,
    ).inc()

    client = httpx.AsyncClient(follow_redirects=True, timeout=60.0)
    request_started = perf_counter()
    try:
        upstream = await client.send(
            client.build_request("GET", url, headers=forward_headers), stream=True
        )
    except httpx.TimeoutException as exc:
        STREAM_UPSTREAM_OPENS.labels(owner=owner, status_code="timeout").inc()
        REMOTE_PROXY_OPEN_DURATION_SECONDS.labels(status_code="timeout").observe(
            perf_counter() - request_started
        )
        await client.aclose()
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Upstream playback request timed out",
        ) from exc
    except httpx.HTTPError as exc:
        STREAM_UPSTREAM_OPENS.labels(owner=owner, status_code="error").inc()
        REMOTE_PROXY_OPEN_DURATION_SECONDS.labels(status_code="error").observe(
            perf_counter() - request_started
        )
        await client.aclose()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream playback request transport failed",
        ) from exc
    response_outcome = (
        "partial"
        if upstream.status_code == status.HTTP_206_PARTIAL_CONTENT
        else ("range_nonpartial" if range_header else "full")
    )
    STREAM_RESPONSE_OUTCOMES.labels(
        owner=owner,
        category="remote-proxy",
        outcome=response_outcome,
    ).inc()
    STREAM_UPSTREAM_OPENS.labels(owner=owner, status_code=str(upstream.status_code)).inc()
    REMOTE_PROXY_OPEN_DURATION_SECONDS.labels(status_code=str(upstream.status_code)).observe(
        perf_counter() - request_started
    )

    if upstream.status_code >= 400:
        await upstream.aclose()
        await client.aclose()
        raise HTTPException(
            status_code=upstream.status_code,
            detail=f"Upstream playback request failed with status {upstream.status_code}",
        )

    async def iterator() -> AsyncGenerator[bytes, None]:
        session = open_serving_session(category="remote-proxy", owner=owner, resource=url)
        handle = open_remote_proxy_handle(session=session, url=url)
        try:
            async for chunk in upstream.aiter_bytes():
                payload = bytes(chunk)
                _touch_session(session, chunk_size=len(payload))
                read_from_handle(handle=handle, chunk_size=len(payload))
                yield payload
        except asyncio.CancelledError:
            _record_stream_abort(owner=session.owner, category=session.category)
            raise
        finally:
            release_handle(handle)
            await upstream.aclose()
            await client.aclose()
            release_serving_session(session)

    return StreamingResponse(
        iterator(),
        status_code=upstream.status_code,
        headers=forwarded_response_headers(upstream.headers),
        media_type=upstream.headers.get("content-type"),
    )


def iter_remote_range_via_chunks(
    resource_id: str,
    url: str,
    file_size: int,
    offset: int,
    size: int,
    cache: ChunkCache,
    *,
    config: ChunkConfig = DEFAULT_CONFIG,
    owner: ServingOwnerKind = "http-direct",
) -> AsyncGenerator[bytes, None]:
    """Stream one remote byte range incrementally through the shared chunk engine."""

    async def _iterator() -> AsyncGenerator[bytes, None]:
        if file_size <= 0:
            raise ValueError("file_size must be positive for chunk-backed serving")
        if size <= 0:
            return

        file_chunks = calculate_file_chunks(resource_id, file_size, config)
        chunks = resolve_chunks_for_read(offset, size, file_chunks)
        if not chunks:
            return

        detect_read_type(
            offset,
            size,
            file_chunks,
            cache,
            previous_offset=None,
        )

        session = open_serving_session(category="remote-proxy", owner=owner, resource=url)
        handle = open_remote_proxy_handle(session=session, url=url)
        client = httpx.AsyncClient(follow_redirects=True, timeout=60.0)
        try:
            async for payload in iter_fetch_and_stitch(
                resource_id,
                url,
                offset,
                size,
                chunks,
                cache,
                client,
            ):
                _touch_session(session, chunk_size=len(payload))
                read_from_handle(handle=handle, chunk_size=len(payload))
                yield payload
        except asyncio.CancelledError:
            _record_stream_abort(owner=session.owner, category=session.category)
            raise
        finally:
            await client.aclose()
            release_handle(handle)
            release_serving_session(session)

    return _iterator()


async def fetch_remote_range_via_chunks(
    resource_id: str,
    url: str,
    file_size: int,
    offset: int,
    size: int,
    cache: ChunkCache,
    *,
    config: ChunkConfig = DEFAULT_CONFIG,
) -> bytes:
    """Resolve one remote byte range through the shared chunk engine before responding.

    This is the safe counterpart to ``iter_remote_range_via_chunks`` for callers that must not
    emit partial-response headers before the first upstream chunk has been validated and fetched.
    """

    if file_size <= 0:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Playback source temporarily unavailable",
        )
    if size <= 0:
        return b""

    try:
        file_chunks = calculate_file_chunks(resource_id, file_size, config)
        chunks = resolve_chunks_for_read(offset, size, file_chunks)
        if not chunks:
            return b""
        detect_read_type(
            offset,
            size,
            file_chunks,
            cache,
            previous_offset=None,
        )
        async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
            return await fetch_and_stitch(
                resource_id,
                url,
                offset,
                size,
                chunks,
                cache,
                client,
            )
    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Upstream playback request timed out",
        ) from exc
    except httpx.HTTPStatusError as exc:
        upstream_status = exc.response.status_code if exc.response is not None else 502
        raise HTTPException(
            status_code=upstream_status,
            detail=f"Upstream playback request failed with status {upstream_status}",
        ) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream playback request transport failed",
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream playback range validation failed",
        ) from exc


async def resolve_remote_content_length(url: str) -> int | None:
    """Return the upstream content length for one remote resource when available."""

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(2.0),
        ) as client:
            response = await client.head(url)
    except httpx.HTTPError:
        return None

    if not response.is_success:
        return None
    raw_value = response.headers.get("content-length")
    if raw_value is None:
        return None
    try:
        parsed = int(raw_value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None




