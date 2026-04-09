"""Async gRPC bridge that exposes the FilmuVFS catalog supplier to the Rust sidecar."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from time import monotonic
from uuid import uuid4

import grpc
import httpx
from google.protobuf.timestamp_pb2 import Timestamp
from prometheus_client import Counter, Gauge

from filmuvfs.catalog.v1 import catalog_pb2, catalog_pb2_grpc

from .playback import PlaybackAttachmentProviderClient, PlaybackAttachmentRefreshRequest
from .vfs_catalog import (
    FilmuVfsCatalogSupplier,
    VfsCatalogCorrelationKeys,
    VfsCatalogDelta,
    VfsCatalogEntry,
    VfsCatalogFileEntry,
    VfsCatalogSnapshot,
    VfsCatalogStats,
)

logger = logging.getLogger(__name__)
_FRESH_URL_SAFETY_MARGIN = timedelta(minutes=10)
_INLINE_REFRESH_URL_VALIDATION_ATTEMPTS = 3
_INLINE_REFRESH_URL_VALIDATION_DELAY = 0.75
_INLINE_REFRESH_URL_VALIDATION_TIMEOUT = 3.0
VFS_CATALOG_GOVERNANCE_EVENTS = Counter(
    "filmu_py_vfs_catalog_governance_total",
    "Count of FilmuVFS catalog gRPC governance events by event kind.",
    labelnames=("event",),
)
VFS_CATALOG_ACTIVE_WATCH_SESSIONS = Gauge(
    "filmu_py_vfs_catalog_watch_sessions_active",
    "Current number of active FilmuVFS WatchCatalog sessions.",
)
_VFS_CATALOG_GOVERNANCE_TEMPLATE = {
    "vfs_catalog_watch_sessions_started": 0,
    "vfs_catalog_watch_sessions_completed": 0,
    "vfs_catalog_watch_sessions_active": 0,
    "vfs_catalog_reconnect_requested": 0,
    "vfs_catalog_reconnect_delta_served": 0,
    "vfs_catalog_reconnect_snapshot_fallback": 0,
    "vfs_catalog_reconnect_failures": 0,
    "vfs_catalog_snapshots_served": 0,
    "vfs_catalog_deltas_served": 0,
    "vfs_catalog_heartbeats_served": 0,
    "vfs_catalog_problem_events": 0,
    "vfs_catalog_request_stream_failures": 0,
    "vfs_catalog_snapshot_build_failures": 0,
    "vfs_catalog_delta_build_failures": 0,
    "vfs_catalog_refresh_attempts": 0,
    "vfs_catalog_refresh_succeeded": 0,
    "vfs_catalog_refresh_provider_failures": 0,
    "vfs_catalog_refresh_empty_results": 0,
    "vfs_catalog_refresh_validation_failed": 0,
    "vfs_catalog_refresh_skipped_no_provider": 0,
    "vfs_catalog_refresh_skipped_no_restricted_url": 0,
    "vfs_catalog_refresh_skipped_no_client": 0,
    "vfs_catalog_refresh_skipped_fresh": 0,
    "vfs_catalog_inline_refresh_requests": 0,
    "vfs_catalog_inline_refresh_succeeded": 0,
    "vfs_catalog_inline_refresh_failed": 0,
    "vfs_catalog_inline_refresh_not_found": 0,
}

_ENTRY_KIND_VALUES = {
    "directory": catalog_pb2.CATALOG_ENTRY_KIND_DIRECTORY,
    "file": catalog_pb2.CATALOG_ENTRY_KIND_FILE,
}
_MEDIA_TYPE_VALUES = {
    "movie": catalog_pb2.CATALOG_MEDIA_TYPE_MOVIE,
    "show": catalog_pb2.CATALOG_MEDIA_TYPE_SHOW,
    "season": catalog_pb2.CATALOG_MEDIA_TYPE_SEASON,
    "episode": catalog_pb2.CATALOG_MEDIA_TYPE_EPISODE,
    "unknown": catalog_pb2.CATALOG_MEDIA_TYPE_UNKNOWN,
}
_FILE_TRANSPORT_VALUES = {
    "local-file": catalog_pb2.CATALOG_FILE_TRANSPORT_LOCAL_FILE,
    "remote-direct": catalog_pb2.CATALOG_FILE_TRANSPORT_REMOTE_DIRECT,
}
_LEASE_STATE_VALUES = {
    "ready": catalog_pb2.CATALOG_LEASE_STATE_READY,
    "stale": catalog_pb2.CATALOG_LEASE_STATE_STALE,
    "refreshing": catalog_pb2.CATALOG_LEASE_STATE_REFRESHING,
    "failed": catalog_pb2.CATALOG_LEASE_STATE_FAILED,
    "unknown": catalog_pb2.CATALOG_LEASE_STATE_UNKNOWN,
}
_PLAYBACK_ROLE_VALUES = {
    "direct": catalog_pb2.CATALOG_PLAYBACK_ROLE_DIRECT,
    "hls": catalog_pb2.CATALOG_PLAYBACK_ROLE_HLS,
}
_PROVIDER_FAMILY_VALUES = {
    "none": catalog_pb2.CATALOG_PROVIDER_FAMILY_NONE,
    "debrid": catalog_pb2.CATALOG_PROVIDER_FAMILY_DEBRID,
    "provider": catalog_pb2.CATALOG_PROVIDER_FAMILY_PROVIDER,
}
_LOCATOR_SOURCE_VALUES = {
    "local-path": catalog_pb2.CATALOG_LOCATOR_SOURCE_LOCAL_PATH,
    "unrestricted-url": catalog_pb2.CATALOG_LOCATOR_SOURCE_UNRESTRICTED_URL,
    "restricted-url": catalog_pb2.CATALOG_LOCATOR_SOURCE_RESTRICTED_URL,
    "locator": catalog_pb2.CATALOG_LOCATOR_SOURCE_LOCATOR,
}
_MATCH_BASIS_VALUES = {
    "source-attachment-id": catalog_pb2.CATALOG_MATCH_BASIS_SOURCE_ATTACHMENT_ID,
    "provider-file-id": catalog_pb2.CATALOG_MATCH_BASIS_PROVIDER_FILE_ID,
    "provider-file-path": catalog_pb2.CATALOG_MATCH_BASIS_PROVIDER_FILE_PATH,
    "local-path": catalog_pb2.CATALOG_MATCH_BASIS_LOCAL_PATH,
    "unrestricted-url": catalog_pb2.CATALOG_MATCH_BASIS_UNRESTRICTED_URL,
    "restricted-url": catalog_pb2.CATALOG_MATCH_BASIS_RESTRICTED_URL,
    "locator": catalog_pb2.CATALOG_MATCH_BASIS_LOCATOR,
    "filename+size": catalog_pb2.CATALOG_MATCH_BASIS_FILENAME_AND_SIZE,
    "provider-download-id+filename": (
        catalog_pb2.CATALOG_MATCH_BASIS_PROVIDER_DOWNLOAD_ID_AND_FILENAME
    ),
    "provider-download-id+provider-file-path": (
        catalog_pb2.CATALOG_MATCH_BASIS_PROVIDER_DOWNLOAD_ID_AND_PROVIDER_FILE_PATH
    ),
    "provider-download-id+file-size": (
        catalog_pb2.CATALOG_MATCH_BASIS_PROVIDER_DOWNLOAD_ID_AND_FILE_SIZE
    ),
}


async def _probe_remote_direct_url(url: str) -> bool:
    """Validate one refreshed remote-direct URL with a tiny ranged GET."""

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(_INLINE_REFRESH_URL_VALIDATION_TIMEOUT),
        ) as client:
            response = await client.get(url, headers={"Range": "bytes=0-0"})
    except httpx.HTTPError:
        return False
    return response.is_success


def _parse_generation_id(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if stripped == "":
        return None
    try:
        parsed = int(stripped)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _timestamp_from_datetime(value: datetime) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromDatetime(value.astimezone(UTC))
    return timestamp


def _correlation_to_proto(
    correlation: VfsCatalogCorrelationKeys,
) -> catalog_pb2.CatalogCorrelationKeys:
    message = catalog_pb2.CatalogCorrelationKeys()
    if correlation.item_id is not None:
        message.item_id = correlation.item_id
    if correlation.media_entry_id is not None:
        message.media_entry_id = correlation.media_entry_id
    if correlation.source_attachment_id is not None:
        message.source_attachment_id = correlation.source_attachment_id
    if correlation.provider is not None:
        message.provider = correlation.provider
    if correlation.provider_download_id is not None:
        message.provider_download_id = correlation.provider_download_id
    if correlation.provider_file_id is not None:
        message.provider_file_id = correlation.provider_file_id
    if correlation.provider_file_path is not None:
        message.provider_file_path = correlation.provider_file_path
    if correlation.session_id is not None:
        message.session_id = correlation.session_id
    if correlation.handle_key is not None:
        message.handle_key = correlation.handle_key
    return message


def _stats_to_proto(stats: VfsCatalogStats) -> catalog_pb2.CatalogStats:
    return catalog_pb2.CatalogStats(
        directory_count=stats.directory_count,
        file_count=stats.file_count,
        blocked_item_count=stats.blocked_item_count,
    )


def _file_entry_to_proto(
    entry: VfsCatalogFileEntry,
    *,
    resolved_urls: dict[str, str] | None = None,
) -> catalog_pb2.FileEntry:
    message = catalog_pb2.FileEntry(
        item_id=entry.item_id,
        item_title=entry.item_title,
        media_entry_id=entry.media_entry_id,
        media_type=_MEDIA_TYPE_VALUES[entry.media_type],
        transport=_FILE_TRANSPORT_VALUES[entry.transport],
        locator=entry.locator,
        lease_state=_LEASE_STATE_VALUES[entry.lease_state],
        active_roles=[_PLAYBACK_ROLE_VALUES[role] for role in entry.active_roles],
        provider_family=_PROVIDER_FAMILY_VALUES[entry.provider_family],
        locator_source=_LOCATOR_SOURCE_VALUES[entry.locator_source],
        restricted_fallback=entry.restricted_fallback,
    )
    if entry.item_external_ref:
        message.item_external_ref = entry.item_external_ref
    if entry.source_attachment_id is not None:
        message.source_attachment_id = entry.source_attachment_id
    if entry.local_path is not None:
        message.local_path = entry.local_path
    if entry.restricted_url is not None:
        message.restricted_url = entry.restricted_url
    url_to_use = (resolved_urls or {}).get(entry.media_entry_id) or entry.unrestricted_url
    if url_to_use is not None:
        message.unrestricted_url = url_to_use
    if entry.original_filename is not None:
        message.original_filename = entry.original_filename
    if entry.size_bytes is not None:
        message.size_bytes = entry.size_bytes
    if entry.expires_at is not None:
        message.expires_at.CopyFrom(_timestamp_from_datetime(entry.expires_at))
    if entry.last_refreshed_at is not None:
        message.last_refreshed_at.CopyFrom(_timestamp_from_datetime(entry.last_refreshed_at))
    if entry.last_refresh_error is not None:
        message.last_refresh_error = entry.last_refresh_error
    if entry.provider is not None:
        message.provider = entry.provider
    if entry.provider_download_id is not None:
        message.provider_download_id = entry.provider_download_id
    if entry.provider_file_id is not None:
        message.provider_file_id = entry.provider_file_id
    if entry.provider_file_path is not None:
        message.provider_file_path = entry.provider_file_path
    if entry.source_key is not None:
        message.source_key = entry.source_key
    if entry.query_strategy is not None:
        message.query_strategy = entry.query_strategy
    if entry.match_basis is not None:
        message.match_basis = _MATCH_BASIS_VALUES[entry.match_basis]
    return message


def _entry_to_proto(
    entry: VfsCatalogEntry,
    *,
    resolved_urls: dict[str, str] | None = None,
) -> catalog_pb2.CatalogEntry:
    message = catalog_pb2.CatalogEntry(
        entry_id=entry.entry_id,
        path=entry.path,
        name=entry.name,
        kind=_ENTRY_KIND_VALUES[entry.kind],
    )
    if entry.parent_entry_id is not None:
        message.parent_entry_id = entry.parent_entry_id
    message.correlation.CopyFrom(_correlation_to_proto(entry.correlation))
    if entry.directory is not None:
        message.directory.CopyFrom(catalog_pb2.DirectoryEntry())
    if entry.file is not None:
        message.file.CopyFrom(_file_entry_to_proto(entry.file, resolved_urls=resolved_urls))
    return message


def _snapshot_event_id(snapshot: VfsCatalogSnapshot) -> str:
    return f"catalog-snapshot:{snapshot.generation_id}"


def _delta_event_id(delta: VfsCatalogDelta) -> str:
    base_generation_id = delta.base_generation_id or "none"
    return f"catalog-delta:{base_generation_id}:{delta.generation_id}"


def _snapshot_to_proto(
    snapshot: VfsCatalogSnapshot,
    *,
    resolved_urls: dict[str, str] | None = None,
) -> catalog_pb2.CatalogSnapshot:
    return catalog_pb2.CatalogSnapshot(
        generation_id=snapshot.generation_id,
        entries=[_entry_to_proto(entry, resolved_urls=resolved_urls) for entry in snapshot.entries],
        stats=_stats_to_proto(snapshot.stats),
    )


def _delta_to_proto(
    delta: VfsCatalogDelta,
    *,
    resolved_urls: dict[str, str] | None = None,
) -> catalog_pb2.CatalogDelta:
    message = catalog_pb2.CatalogDelta(
        generation_id=delta.generation_id,
        upserts=[_entry_to_proto(entry, resolved_urls=resolved_urls) for entry in delta.upserts],
        removals=[
            catalog_pb2.CatalogRemoval(
                entry_id=removal.entry_id,
                path=removal.path,
                kind=_ENTRY_KIND_VALUES[removal.kind],
                correlation=_correlation_to_proto(removal.correlation),
            )
            for removal in delta.removals
        ],
        stats=_stats_to_proto(delta.stats),
    )
    if delta.base_generation_id is not None:
        message.base_generation_id = delta.base_generation_id
    return message


def _snapshot_event(
    snapshot: VfsCatalogSnapshot,
    *,
    resolved_urls: dict[str, str] | None = None,
) -> catalog_pb2.WatchCatalogEvent:
    message = catalog_pb2.WatchCatalogEvent(
        event_id=_snapshot_event_id(snapshot),
        published_at=_timestamp_from_datetime(snapshot.published_at),
    )
    message.snapshot.CopyFrom(_snapshot_to_proto(snapshot, resolved_urls=resolved_urls))
    return message


def _delta_event(
    delta: VfsCatalogDelta,
    *,
    resolved_urls: dict[str, str] | None = None,
) -> catalog_pb2.WatchCatalogEvent:
    message = catalog_pb2.WatchCatalogEvent(
        event_id=_delta_event_id(delta),
        published_at=_timestamp_from_datetime(delta.published_at),
    )
    message.delta.CopyFrom(_delta_to_proto(delta, resolved_urls=resolved_urls))
    return message


def _heartbeat_event() -> catalog_pb2.WatchCatalogEvent:
    message = catalog_pb2.WatchCatalogEvent(
        event_id=f"catalog-heartbeat:{uuid4().hex}",
        published_at=_timestamp_from_datetime(datetime.now(UTC)),
    )
    message.heartbeat.CopyFrom(catalog_pb2.CatalogHeartbeat())
    return message


def _problem_event(code: str, message_text: str) -> catalog_pb2.WatchCatalogEvent:
    message = catalog_pb2.WatchCatalogEvent(
        event_id=f"catalog-problem:{uuid4().hex}",
        published_at=_timestamp_from_datetime(datetime.now(UTC)),
    )
    message.problem.CopyFrom(catalog_pb2.CatalogProblem(code=code, message=message_text))
    return message


def _apply_delta(previous: VfsCatalogSnapshot, delta: VfsCatalogDelta) -> VfsCatalogSnapshot:
    entries_by_id = {entry.entry_id: entry for entry in previous.entries}
    for removal in delta.removals:
        entries_by_id.pop(removal.entry_id, None)
    for upsert in delta.upserts:
        entries_by_id[upsert.entry_id] = upsert
    entries = tuple(
        sorted(
            entries_by_id.values(),
            key=lambda entry: (
                entry.path.count("/"),
                entry.path,
                0 if entry.kind == "directory" else 1,
                entry.entry_id,
            ),
        )
    )
    blocked_items = (
        previous.blocked_items
        if previous.stats.blocked_item_count == delta.stats.blocked_item_count
        else ()
    )
    return VfsCatalogSnapshot(
        generation_id=delta.generation_id,
        published_at=delta.published_at,
        entries=entries,
        stats=delta.stats,
        blocked_items=blocked_items,
    )


def _resolve_bound_address(bind_address: str, port: int) -> str:
    host, separator, _ = bind_address.rpartition(":")
    if not separator:
        return f"{bind_address}:{port}"
    return f"{host}:{port}"


def build_empty_vfs_catalog_governance_snapshot() -> dict[str, int]:
    """Return the additive zero-value governance payload for FilmuVFS gRPC state."""

    return dict(_VFS_CATALOG_GOVERNANCE_TEMPLATE)


@dataclass(slots=True)
class _WatchCatalogRequestState:
    subscribe: catalog_pb2.CatalogSubscribe | None = None
    subscribe_received: asyncio.Event = field(default_factory=asyncio.Event)
    stream_closed: asyncio.Event = field(default_factory=asyncio.Event)
    last_ack_event_id: str | None = None
    last_ack_generation_id: str | None = None
    last_client_heartbeat_at: datetime | None = None


class FilmuVfsCatalogGrpcServicer:
    """Bind the Python catalog supplier to the generated FilmuVFS gRPC service."""

    def __init__(
        self,
        supplier: FilmuVfsCatalogSupplier,
        *,
        playback_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        poll_interval: timedelta = timedelta(seconds=1),
        heartbeat_interval: timedelta = timedelta(seconds=15),
    ) -> None:
        self._supplier = supplier
        self._playback_clients = playback_clients or {}
        self._poll_interval = poll_interval
        self._heartbeat_interval = heartbeat_interval
        self._governance = build_empty_vfs_catalog_governance_snapshot()

    def _record_governance_event(self, event: str, *, value: int = 1) -> None:
        self._governance[event] += value
        VFS_CATALOG_GOVERNANCE_EVENTS.labels(event=event).inc(value)

    def _change_active_watch_sessions(self, delta: int) -> None:
        current = max(self._governance["vfs_catalog_watch_sessions_active"] + delta, 0)
        self._governance["vfs_catalog_watch_sessions_active"] = current
        VFS_CATALOG_ACTIVE_WATCH_SESSIONS.set(float(current))

    def build_governance_snapshot(self) -> dict[str, int]:
        """Return a copy of the current FilmuVFS gRPC governance counters."""

        return dict(self._governance)

    async def _resolve_fresh_url(
        self,
        entry: VfsCatalogFileEntry,
        *,
        force: bool = False,
        allow_stale_fallback: bool = True,
    ) -> str | None:
        stored_url = entry.unrestricted_url

        if entry.transport != "remote-direct":
            return stored_url
        if not entry.provider:
            self._record_governance_event("vfs_catalog_refresh_skipped_no_provider")
            logger.debug(
                "vfs.catalog.grpc.entry.refresh.skipped.no_provider",
                extra={"media_entry_id": entry.media_entry_id},
            )
            return stored_url if allow_stale_fallback else None

        restricted_url = entry.restricted_url
        if not restricted_url:
            self._record_governance_event("vfs_catalog_refresh_skipped_no_restricted_url")
            logger.debug(
                "vfs.catalog.grpc.entry.refresh.skipped.no_restricted_url",
                extra={"media_entry_id": entry.media_entry_id, "provider": entry.provider},
            )
            return stored_url if allow_stale_fallback else None

        client = self._playback_clients.get(entry.provider)
        if client is None:
            self._record_governance_event("vfs_catalog_refresh_skipped_no_client")
            logger.debug(
                "vfs.catalog.grpc.entry.refresh.skipped.no_client",
                extra={"media_entry_id": entry.media_entry_id, "provider": entry.provider},
            )
            return stored_url if allow_stale_fallback else None

        if not force:
            now = datetime.now(UTC)
            needs_refresh = entry.expires_at is None or entry.expires_at <= (
                now + _FRESH_URL_SAFETY_MARGIN
            )
            if not needs_refresh:
                self._record_governance_event("vfs_catalog_refresh_skipped_fresh")
                logger.debug(
                    "vfs.catalog.grpc.entry.refresh.skipped.fresh",
                    extra={
                        "media_entry_id": entry.media_entry_id,
                        "provider": entry.provider,
                        "expires_at": entry.expires_at.isoformat() if entry.expires_at else None,
                    },
                )
                return stored_url

        logger.debug(
            "vfs.catalog.grpc.entry.refresh.attempting",
            extra={
                "media_entry_id": entry.media_entry_id,
                "provider": entry.provider,
                "expires_at": entry.expires_at.isoformat() if entry.expires_at else None,
                "lease_state": entry.lease_state,
                "force_refresh": force,
            },
        )

        request = PlaybackAttachmentRefreshRequest(
            attachment_id=entry.source_attachment_id or entry.media_entry_id,
            item_id=entry.item_id,
            kind="remote-direct",
            provider=entry.provider,
            provider_download_id=entry.provider_download_id,
            restricted_url=restricted_url,
            unrestricted_url=stored_url,
            local_path=entry.local_path,
            refresh_state=entry.lease_state,
            provider_file_id=entry.provider_file_id,
            provider_file_path=entry.provider_file_path,
            original_filename=entry.original_filename,
            file_size=entry.size_bytes,
        )

        max_attempts = _INLINE_REFRESH_URL_VALIDATION_ATTEMPTS if force else 1
        for attempt in range(1, max_attempts + 1):
            self._record_governance_event("vfs_catalog_refresh_attempts")
            try:
                result = await client.unrestrict_link(restricted_url, request=request)
            except Exception as exc:  # pragma: no cover - defensive provider failure fallback
                self._record_governance_event("vfs_catalog_refresh_provider_failures")
                logger.warning(
                    "vfs.catalog.grpc.entry.refresh.failed",
                    extra={
                        "media_entry_id": entry.media_entry_id,
                        "provider": entry.provider,
                        "error": str(exc),
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                    },
                )
                if attempt < max_attempts:
                    await asyncio.sleep(_INLINE_REFRESH_URL_VALIDATION_DELAY)
                    continue
                return stored_url if allow_stale_fallback else None

            if result is None:
                self._record_governance_event("vfs_catalog_refresh_empty_results")
                logger.warning(
                    "vfs.catalog.grpc.entry.refresh.empty_result",
                    extra={
                        "media_entry_id": entry.media_entry_id,
                        "provider": entry.provider,
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                    },
                )
                if attempt < max_attempts:
                    await asyncio.sleep(_INLINE_REFRESH_URL_VALIDATION_DELAY)
                    continue
                return stored_url if allow_stale_fallback else None

            download_url = result.download_url
            if download_url and (not force or await _probe_remote_direct_url(download_url)):
                self._record_governance_event("vfs_catalog_refresh_succeeded")
                logger.info(
                    "vfs.catalog.grpc.entry.refresh.succeeded",
                    extra={
                        "media_entry_id": entry.media_entry_id,
                        "provider": entry.provider,
                        "expires_at": entry.expires_at.isoformat() if entry.expires_at else None,
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                    },
                )
                return download_url

            self._record_governance_event("vfs_catalog_refresh_validation_failed")
            logger.warning(
                "vfs.catalog.grpc.entry.refresh.validation_failed",
                extra={
                    "media_entry_id": entry.media_entry_id,
                    "provider": entry.provider,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                },
            )
            if attempt < max_attempts:
                await asyncio.sleep(_INLINE_REFRESH_URL_VALIDATION_DELAY)
                continue

        return stored_url if allow_stale_fallback else None

    async def _resolve_all_file_urls(
        self,
        entries: Iterable[VfsCatalogEntry],
    ) -> dict[str, str]:
        file_entries = [entry for entry in entries if entry.kind == "file" and entry.file is not None]
        results = await asyncio.gather(
            *(self._resolve_fresh_url(entry.file) for entry in file_entries if entry.file is not None),
            return_exceptions=True,
        )

        resolved: dict[str, str] = {}
        for entry, result in zip(file_entries, results, strict=False):
            assert entry.file is not None
            if isinstance(result, Exception):
                logger.warning(
                    "vfs.catalog.grpc.entry.refresh.raised",
                    extra={"media_entry_id": entry.file.media_entry_id, "error": str(result)},
                )
                continue
            if result:
                assert isinstance(result, str)
                resolved[entry.file.media_entry_id] = result
        return resolved

    async def RefreshCatalogEntry(
        self,
        request: catalog_pb2.RefreshCatalogEntryRequest,
        context: grpc.aio.ServicerContext[
            catalog_pb2.RefreshCatalogEntryRequest,
            catalog_pb2.RefreshCatalogEntryResponse,
        ],
    ) -> catalog_pb2.RefreshCatalogEntryResponse:
        entry_id = request.entry_id.strip()
        provider_file_id = request.provider_file_id.strip()
        if entry_id == "" and provider_file_id == "":
            return catalog_pb2.RefreshCatalogEntryResponse(success=False, new_url="")
        self._record_governance_event("vfs_catalog_inline_refresh_requests")

        try:
            snapshot = await self._supplier.build_snapshot()
        except Exception as exc:  # pragma: no cover - defensive transport safety net
            self._record_governance_event("vfs_catalog_snapshot_build_failures")
            self._record_governance_event("vfs_catalog_problem_events")
            logger.exception("vfs.catalog.grpc.entry.inline_refresh.snapshot.failed")
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))
            raise RuntimeError("unreachable after context.abort") from exc

        matched_entry = None
        if entry_id != "":
            matched_entry = next(
                (
                    entry.file
                    for entry in snapshot.entries
                    if entry.kind == "file"
                    and entry.file is not None
                    and entry.entry_id == entry_id
                ),
                None,
            )

        if matched_entry is None and provider_file_id != "":
            matched_entry = next(
                (
                    entry.file
                    for entry in snapshot.entries
                    if entry.kind == "file"
                    and entry.file is not None
                    and entry.file.provider_file_id == provider_file_id
                ),
                None,
            )
        if matched_entry is None:
            logger.warning(
                "vfs.catalog.grpc.entry.inline_refresh.not_found",
                extra={
                    "entry_id": entry_id or None,
                    "provider_file_id": provider_file_id,
                    "handle_key": request.handle_key or None,
                },
            )
            self._record_governance_event("vfs_catalog_inline_refresh_not_found")
            return catalog_pb2.RefreshCatalogEntryResponse(success=False, new_url="")

        refreshed_url = await self._resolve_fresh_url(
            matched_entry,
            force=True,
            allow_stale_fallback=False,
        )
        if refreshed_url is None:
            logger.warning(
                "vfs.catalog.grpc.entry.inline_refresh.failed",
                extra={
                    "entry_id": entry_id or None,
                    "provider_file_id": provider_file_id,
                    "handle_key": request.handle_key or None,
                    "media_entry_id": matched_entry.media_entry_id,
                },
            )
            self._record_governance_event("vfs_catalog_inline_refresh_failed")
            return catalog_pb2.RefreshCatalogEntryResponse(success=False, new_url="")

        logger.info(
            "vfs.catalog.grpc.entry.inline_refresh.succeeded",
            extra={
                "entry_id": entry_id or None,
                "provider_file_id": provider_file_id,
                "handle_key": request.handle_key or None,
                "media_entry_id": matched_entry.media_entry_id,
            },
        )
        self._record_governance_event("vfs_catalog_inline_refresh_succeeded")
        return catalog_pb2.RefreshCatalogEntryResponse(success=True, new_url=refreshed_url)

    async def WatchCatalog(
        self,
        request_iterator: AsyncIterator[catalog_pb2.WatchCatalogRequest],
        context: grpc.aio.ServicerContext[
            catalog_pb2.WatchCatalogRequest,
            catalog_pb2.WatchCatalogEvent,
        ],
    ) -> AsyncIterator[catalog_pb2.WatchCatalogEvent]:
        self._record_governance_event("vfs_catalog_watch_sessions_started")
        self._change_active_watch_sessions(1)
        request_state = _WatchCatalogRequestState()
        request_task = asyncio.create_task(
            self._consume_requests(request_iterator, request_state, context)
        )
        last_server_event_at = monotonic()

        try:
            await self._wait_for_initial_subscribe(request_state, context)

            active_snapshot: VfsCatalogSnapshot | None = None
            reconnect_delta_served = False
            reconnect_generation_id = _parse_generation_id(
                request_state.subscribe.last_applied_generation_id if request_state.subscribe else None
            )
            if reconnect_generation_id is not None:
                self._record_governance_event("vfs_catalog_reconnect_requested")
                try:
                    reconnect_delta = await self._supplier.build_delta_since(reconnect_generation_id)
                except Exception as exc:  # pragma: no cover - defensive safety net
                    self._record_governance_event("vfs_catalog_reconnect_failures")
                    self._record_governance_event("vfs_catalog_problem_events")
                    logger.exception("vfs.catalog.grpc.reconnect_delta.failed")
                    yield _problem_event("reconnect_delta_failed", str(exc))
                    return

                if reconnect_delta is not None:
                    self._record_governance_event("vfs_catalog_reconnect_delta_served")
                    resolved_urls = await self._resolve_all_file_urls(reconnect_delta.upserts)
                    self._record_governance_event("vfs_catalog_deltas_served")
                    yield _delta_event(reconnect_delta, resolved_urls=resolved_urls)
                    last_server_event_at = monotonic()
                    reconnect_delta_served = True
                    current_generation_id = _parse_generation_id(reconnect_delta.generation_id)
                    if current_generation_id is not None:
                        active_snapshot = await self._supplier.snapshot_for_generation(
                            current_generation_id
                        )
                else:
                    self._record_governance_event("vfs_catalog_reconnect_snapshot_fallback")

            if active_snapshot is None:
                try:
                    active_snapshot = await self._supplier.build_snapshot()
                except Exception as exc:  # pragma: no cover - defensive safety net
                    self._record_governance_event("vfs_catalog_snapshot_build_failures")
                    self._record_governance_event("vfs_catalog_problem_events")
                    logger.exception("vfs.catalog.grpc.snapshot.failed")
                    yield _problem_event("snapshot_build_failed", str(exc))
                    return

                if not reconnect_delta_served:
                    resolved_urls = await self._resolve_all_file_urls(active_snapshot.entries)
                    self._record_governance_event("vfs_catalog_snapshots_served")
                    yield _snapshot_event(active_snapshot, resolved_urls=resolved_urls)
                    last_server_event_at = monotonic()

            assert active_snapshot is not None
            snapshot = active_snapshot

            while not context.done():
                if request_state.stream_closed.is_set():
                    return

                try:
                    delta = await self._supplier.build_delta(snapshot)
                except Exception as exc:  # pragma: no cover - defensive safety net
                    self._record_governance_event("vfs_catalog_delta_build_failures")
                    self._record_governance_event("vfs_catalog_problem_events")
                    logger.exception("vfs.catalog.grpc.delta.failed")
                    yield _problem_event("delta_build_failed", str(exc))
                    return

                if delta.generation_id != snapshot.generation_id or delta.upserts or delta.removals:
                    snapshot = _apply_delta(snapshot, delta)
                    resolved_urls = await self._resolve_all_file_urls(delta.upserts)
                    self._record_governance_event("vfs_catalog_deltas_served")
                    yield _delta_event(delta, resolved_urls=resolved_urls)
                    last_server_event_at = monotonic()
                elif monotonic() - last_server_event_at >= self._heartbeat_interval.total_seconds():
                    self._record_governance_event("vfs_catalog_heartbeats_served")
                    yield _heartbeat_event()
                    last_server_event_at = monotonic()

                await self._wait_for_next_poll_window(request_state)
        finally:
            request_task.cancel()
            with suppress(asyncio.CancelledError):
                await request_task
            self._change_active_watch_sessions(-1)
            self._record_governance_event("vfs_catalog_watch_sessions_completed")

    async def _consume_requests(
        self,
        request_iterator: AsyncIterator[catalog_pb2.WatchCatalogRequest],
        request_state: _WatchCatalogRequestState,
        context: grpc.aio.ServicerContext[
            catalog_pb2.WatchCatalogRequest,
            catalog_pb2.WatchCatalogEvent,
        ],
    ) -> None:
        try:
            async for request in request_iterator:
                command = request.WhichOneof("command")
                if command == "subscribe":
                    request_state.subscribe = request.subscribe
                    request_state.subscribe_received.set()
                elif command == "ack":
                    request_state.last_ack_event_id = request.ack.event_id
                    request_state.last_ack_generation_id = request.ack.generation_id or None
                elif command == "heartbeat":
                    request_state.last_client_heartbeat_at = datetime.now(UTC)
        except asyncio.CancelledError:
            raise
        except Exception:
            if not context.done():
                self._record_governance_event("vfs_catalog_request_stream_failures")
                logger.exception("vfs.catalog.grpc.request_stream.failed")
        finally:
            request_state.stream_closed.set()

    async def _wait_for_initial_subscribe(
        self,
        request_state: _WatchCatalogRequestState,
        context: grpc.aio.ServicerContext[
            catalog_pb2.WatchCatalogRequest,
            catalog_pb2.WatchCatalogEvent,
        ],
    ) -> None:
        while not request_state.subscribe_received.is_set():
            if request_state.stream_closed.is_set():
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "WatchCatalog requires an initial subscribe command",
                )
                raise RuntimeError("unreachable after context.abort")
            await asyncio.sleep(0.01)

    async def _wait_for_next_poll_window(self, request_state: _WatchCatalogRequestState) -> None:
        timeout_seconds = self._poll_interval.total_seconds()
        if timeout_seconds <= 0:
            await asyncio.sleep(0)
            return
        try:
            await asyncio.wait_for(request_state.stream_closed.wait(), timeout=timeout_seconds)
        except TimeoutError:
            return


class FilmuVfsCatalogGrpcServer:
    """Lifecycle wrapper for the app-scoped FilmuVFS catalog gRPC server."""

    def __init__(
        self,
        *,
        bind_address: str,
        supplier: FilmuVfsCatalogSupplier,
        playback_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        poll_interval: timedelta = timedelta(seconds=1),
        heartbeat_interval: timedelta = timedelta(seconds=15),
        shutdown_grace: timedelta = timedelta(seconds=5),
    ) -> None:
        self._bind_address = bind_address
        self._poll_interval = poll_interval
        self._heartbeat_interval = heartbeat_interval
        self._shutdown_grace = shutdown_grace
        self._servicer = FilmuVfsCatalogGrpcServicer(
            supplier,
            playback_clients=playback_clients,
            poll_interval=poll_interval,
            heartbeat_interval=heartbeat_interval,
        )
        self._server: grpc.aio.Server | None = None
        self.bound_address: str | None = None

    @property
    def target(self) -> str:
        return self.bound_address or self._bind_address

    def build_governance_snapshot(self) -> dict[str, int]:
        """Return the current FilmuVFS catalog gRPC governance snapshot."""

        return self._servicer.build_governance_snapshot()

    async def start(self) -> None:
        """Start serving the FilmuVFS catalog stream on the configured bind address."""

        if self._server is not None:
            return

        server = grpc.aio.server()
        catalog_pb2_grpc.add_FilmuVfsCatalogServiceServicer_to_server(  # type: ignore[no-untyped-call]
            self._servicer,
            server,
        )
        bound_port = server.add_insecure_port(self._bind_address)
        if bound_port == 0:
            raise RuntimeError(f"failed to bind FilmuVFS catalog gRPC server to {self._bind_address}")
        await server.start()
        self._server = server
        self.bound_address = _resolve_bound_address(self._bind_address, bound_port)
        logger.info(
            "vfs.catalog.grpc.started",
            extra={
                "bind_address": self._bind_address,
                "bound_address": self.bound_address,
                "poll_interval_seconds": self._poll_interval.total_seconds(),
                "heartbeat_interval_seconds": self._heartbeat_interval.total_seconds(),
            },
        )

    async def stop(self) -> None:
        """Stop serving the FilmuVFS catalog stream and wait for termination."""

        server = self._server
        if server is None:
            return

        self._server = None
        await server.stop(self._shutdown_grace.total_seconds())
        await server.wait_for_termination()
        logger.info(
            "vfs.catalog.grpc.stopped",
            extra={"bound_address": self.bound_address},
        )
