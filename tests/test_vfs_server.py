"""Unit tests for the FilmuVFS catalog gRPC bridge."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from filmu_py.services.playback import (
    PlaybackAttachmentProviderUnrestrictedLink,
    PlaybackAttachmentRefreshRequest,
)
from filmu_py.services.vfs_catalog import (
    VfsCatalogCorrelationKeys,
    VfsCatalogDelta,
    VfsCatalogDirectoryEntry,
    VfsCatalogEntry,
    VfsCatalogFileEntry,
    VfsCatalogSnapshot,
    VfsCatalogStats,
)
from filmu_py.services.vfs_server import FilmuVfsCatalogGrpcServicer
from filmuvfs.catalog.v1 import catalog_pb2


class _FakeContext:
    def __init__(self, metadata: list[tuple[str, str]] | None = None) -> None:
        self._metadata = metadata or []

    def done(self) -> bool:
        return False

    async def abort(self, code: object, details: str) -> None:
        raise RuntimeError(f"aborted: {code}: {details}")

    def invocation_metadata(self) -> list[tuple[str, str]]:
        return list(self._metadata)


class _StubSupplier:
    def __init__(
        self,
        *,
        current_snapshot: VfsCatalogSnapshot,
        reconnect_delta: VfsCatalogDelta | None = None,
        snapshots_by_generation: dict[int, VfsCatalogSnapshot] | None = None,
    ) -> None:
        self.current_snapshot = current_snapshot
        self.reconnect_delta = reconnect_delta
        self.snapshots_by_generation = snapshots_by_generation or {}
        self.snapshot_calls = 0
        self.delta_since_calls: list[int] = []

    async def build_snapshot(self) -> VfsCatalogSnapshot:
        self.snapshot_calls += 1
        return self.current_snapshot

    async def build_delta(self, previous: VfsCatalogSnapshot) -> VfsCatalogDelta:
        return VfsCatalogDelta(
            generation_id=previous.generation_id,
            base_generation_id=previous.generation_id,
            published_at=previous.published_at,
            upserts=(),
            removals=(),
            stats=previous.stats,
        )

    async def build_delta_since(self, generation_id: int) -> VfsCatalogDelta | None:
        self.delta_since_calls.append(generation_id)
        if generation_id in self.snapshots_by_generation:
            return self.reconnect_delta
        return None

    async def snapshot_for_generation(self, generation_id: int) -> VfsCatalogSnapshot | None:
        return self.snapshots_by_generation.get(generation_id)


class _FakeProviderClient:
    def __init__(self, refreshed_url: str | list[str]) -> None:
        if isinstance(refreshed_url, list):
            self.refreshed_urls = list(refreshed_url)
        else:
            self.refreshed_urls = [refreshed_url]
        self.calls: list[tuple[str, PlaybackAttachmentRefreshRequest]] = []

    async def unrestrict_link(
        self,
        link: str,
        *,
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
        self.calls.append((link, request))
        refreshed_url = self.refreshed_urls[min(len(self.calls) - 1, len(self.refreshed_urls) - 1)]
        return PlaybackAttachmentProviderUnrestrictedLink(
            download_url=refreshed_url,
            restricted_url=link,
        )


def _catalog_file_entry(*, url: str, restricted_url: str, provider_file_id: str) -> VfsCatalogEntry:
    published_at = datetime(2026, 3, 20, 0, 0, tzinfo=UTC)
    return VfsCatalogEntry(
        entry_id="file:movie-1",
        parent_entry_id="dir:/movies/Example Movie (2024)",
        path="/movies/Example Movie (2024)/Example Movie.mkv",
        name="Example Movie.mkv",
        kind="file",
        correlation=VfsCatalogCorrelationKeys(
            item_id="item-1",
            media_entry_id="media-entry-1",
            provider="realdebrid",
            provider_download_id="download-1",
            provider_file_id=provider_file_id,
        ),
        file=VfsCatalogFileEntry(
            item_id="item-1",
            item_title="Example Movie",
            item_external_ref="tmdb:1",
            media_entry_id="media-entry-1",
            source_attachment_id="attachment-1",
            media_type="movie",
            transport="remote-direct",
            locator=url,
            restricted_url=restricted_url,
            unrestricted_url=url,
            original_filename="Example Movie.mkv",
            size_bytes=1024,
            lease_state="ready",
            expires_at=published_at.replace(year=2099),
            last_refreshed_at=published_at,
            provider="realdebrid",
            provider_download_id="download-1",
            provider_file_id=provider_file_id,
            provider_file_path="Movies/Example Movie.mkv",
            active_roles=("direct",),
            source_key="media-entry:media-entry-1",
            query_strategy="by-provider-file-id",
            provider_family="debrid",
            locator_source="unrestricted-url",
            match_basis="provider-file-id",
        ),
    )


def _catalog_snapshot(*, generation_id: str, file_url: str) -> VfsCatalogSnapshot:
    published_at = datetime(2026, 3, 20, 0, 0, tzinfo=UTC)
    entries = (
        VfsCatalogEntry(
            entry_id="dir:/",
            parent_entry_id=None,
            path="/",
            name="/",
            kind="directory",
            directory=VfsCatalogDirectoryEntry(path="/"),
        ),
        VfsCatalogEntry(
            entry_id="dir:/movies",
            parent_entry_id="dir:/",
            path="/movies",
            name="movies",
            kind="directory",
            directory=VfsCatalogDirectoryEntry(path="/movies"),
        ),
        VfsCatalogEntry(
            entry_id="dir:/movies/Example Movie (2024)",
            parent_entry_id="dir:/movies",
            path="/movies/Example Movie (2024)",
            name="Example Movie (2024)",
            kind="directory",
            directory=VfsCatalogDirectoryEntry(path="/movies/Example Movie (2024)"),
        ),
        _catalog_file_entry(
            url=file_url,
            restricted_url="https://api.example.com/restricted/movie-1",
            provider_file_id="provider-file-movie-1",
        ),
    )
    return VfsCatalogSnapshot(
        generation_id=generation_id,
        published_at=published_at,
        entries=entries,
        stats=VfsCatalogStats(directory_count=3, file_count=1, blocked_item_count=0),
    )


async def _first_watch_event(
    servicer: FilmuVfsCatalogGrpcServicer,
    request: catalog_pb2.WatchCatalogRequest,
    *,
    context: _FakeContext | None = None,
) -> catalog_pb2.WatchCatalogEvent:
    async def request_iterator() -> AsyncIterator[catalog_pb2.WatchCatalogRequest]:
        yield request

    stream = servicer.WatchCatalog(request_iterator(), cast(Any, context or _FakeContext()))
    return await anext(stream)


async def _collect_watch_events(
    servicer: FilmuVfsCatalogGrpcServicer,
    request: catalog_pb2.WatchCatalogRequest,
) -> list[catalog_pb2.WatchCatalogEvent]:
    async def request_iterator() -> AsyncIterator[catalog_pb2.WatchCatalogRequest]:
        yield request

    stream = servicer.WatchCatalog(request_iterator(), cast(Any, _FakeContext()))
    return [event async for event in stream]


def test_watch_catalog_serves_delta_for_known_reconnect_generation() -> None:
    previous_snapshot = _catalog_snapshot(generation_id="1", file_url="https://cdn.example.com/movie-a")
    current_snapshot = _catalog_snapshot(generation_id="2", file_url="https://cdn.example.com/movie-b")
    reconnect_delta = VfsCatalogDelta(
        generation_id="2",
        base_generation_id="1",
        published_at=current_snapshot.published_at,
        upserts=(current_snapshot.entries[-1],),
        removals=(),
        stats=current_snapshot.stats,
    )
    supplier = _StubSupplier(
        current_snapshot=current_snapshot,
        reconnect_delta=reconnect_delta,
        snapshots_by_generation={1: previous_snapshot, 2: current_snapshot},
    )
    servicer = FilmuVfsCatalogGrpcServicer(cast(Any, supplier))

    event = asyncio.run(
        _first_watch_event(
            servicer,
            catalog_pb2.WatchCatalogRequest(
                subscribe=catalog_pb2.CatalogSubscribe(
                    daemon_id="pytest-daemon",
                    last_applied_generation_id="1",
                    want_full_snapshot=True,
                    correlation=catalog_pb2.CatalogCorrelationKeys(),
                )
            ),
        )
    )

    assert event.WhichOneof("payload") == "delta"
    assert event.delta.base_generation_id == "1"
    assert event.delta.generation_id == "2"
    assert supplier.snapshot_calls == 0
    assert supplier.delta_since_calls == [1]


def test_watch_catalog_governance_tracks_reconnect_delta_lifecycle() -> None:
    previous_snapshot = _catalog_snapshot(generation_id="1", file_url="https://cdn.example.com/movie-a")
    current_snapshot = _catalog_snapshot(generation_id="2", file_url="https://cdn.example.com/movie-b")
    reconnect_delta = VfsCatalogDelta(
        generation_id="2",
        base_generation_id="1",
        published_at=current_snapshot.published_at,
        upserts=(current_snapshot.entries[-1],),
        removals=(),
        stats=current_snapshot.stats,
    )
    supplier = _StubSupplier(
        current_snapshot=current_snapshot,
        reconnect_delta=reconnect_delta,
        snapshots_by_generation={1: previous_snapshot, 2: current_snapshot},
    )
    servicer = FilmuVfsCatalogGrpcServicer(cast(Any, supplier))

    events = asyncio.run(
        _collect_watch_events(
            servicer,
            catalog_pb2.WatchCatalogRequest(
                subscribe=catalog_pb2.CatalogSubscribe(
                    daemon_id="pytest-daemon",
                    last_applied_generation_id="1",
                    want_full_snapshot=True,
                    correlation=catalog_pb2.CatalogCorrelationKeys(),
                )
            ),
        )
    )

    assert len(events) == 1
    assert events[0].WhichOneof("payload") == "delta"
    snapshot = servicer.build_governance_snapshot()
    assert snapshot["vfs_catalog_watch_sessions_started"] == 1
    assert snapshot["vfs_catalog_watch_sessions_completed"] == 1
    assert snapshot["vfs_catalog_watch_sessions_active"] == 0
    assert snapshot["vfs_catalog_reconnect_requested"] == 1
    assert snapshot["vfs_catalog_reconnect_delta_served"] == 1
    assert snapshot["vfs_catalog_deltas_served"] == 1


def test_watch_catalog_reuses_current_generation_without_snapshot_fallback() -> None:
    current_snapshot = _catalog_snapshot(generation_id="2", file_url="https://cdn.example.com/movie-b")
    supplier = _StubSupplier(
        current_snapshot=current_snapshot,
        reconnect_delta=None,
        snapshots_by_generation={2: current_snapshot},
    )
    servicer = FilmuVfsCatalogGrpcServicer(cast(Any, supplier), heartbeat_interval=timedelta(seconds=1))

    event = asyncio.run(
        _first_watch_event(
            servicer,
            catalog_pb2.WatchCatalogRequest(
                subscribe=catalog_pb2.CatalogSubscribe(
                    daemon_id="pytest-daemon",
                    last_applied_generation_id="2",
                    want_full_snapshot=True,
                    correlation=catalog_pb2.CatalogCorrelationKeys(),
                )
            ),
        )
    )

    assert event.WhichOneof("payload") == "heartbeat"
    snapshot = servicer.build_governance_snapshot()
    assert snapshot["vfs_catalog_reconnect_requested"] == 1
    assert snapshot["vfs_catalog_reconnect_current_generation_reused"] == 1
    assert snapshot["vfs_catalog_reconnect_snapshot_fallback"] == 0


def test_watch_catalog_binds_cross_process_observability_metadata(monkeypatch: Any) -> None:
    supplier = _StubSupplier(
        current_snapshot=_catalog_snapshot(generation_id="1", file_url="https://cdn.example.com/movie-a")
    )
    servicer = FilmuVfsCatalogGrpcServicer(cast(Any, supplier))
    bound: dict[str, str] = {}

    def _capture_bindings(**kwargs: str) -> None:
        bound.update(kwargs)

    monkeypatch.setattr(
        "filmu_py.services.vfs_server.structlog.contextvars.bind_contextvars",
        _capture_bindings,
    )

    event = asyncio.run(
        _first_watch_event(
            servicer,
            catalog_pb2.WatchCatalogRequest(
                subscribe=catalog_pb2.CatalogSubscribe(
                    daemon_id="pytest-daemon",
                    want_full_snapshot=True,
                    correlation=catalog_pb2.CatalogCorrelationKeys(),
                )
            ),
            context=_FakeContext(
                [
                    ("x-request-id", "req-watch-1"),
                    ("x-tenant-id", "tenant-main"),
                    ("x-filmu-vfs-session-id", "session-1"),
                    ("x-filmu-vfs-daemon-id", "daemon-1"),
                ]
            ),
        )
    )

    assert event.WhichOneof("payload") == "snapshot"
    assert bound["request_id"] == "req-watch-1"
    assert bound["tenant_id"] == "tenant-main"
    assert bound["vfs_session_id"] == "session-1"
    assert bound["vfs_daemon_id"] == "daemon-1"


def test_refresh_catalog_entry_forces_provider_refresh_and_returns_new_url() -> None:
    snapshot = _catalog_snapshot(generation_id="1", file_url="https://cdn.example.com/stale")
    provider_client = _FakeProviderClient("https://cdn.example.com/fresh")
    servicer = FilmuVfsCatalogGrpcServicer(
        cast(Any, _StubSupplier(current_snapshot=snapshot)),
        playback_clients={"realdebrid": provider_client},
    )

    with patch(
        "filmu_py.services.vfs_server._probe_remote_direct_url",
        new=AsyncMock(return_value=True),
    ):
        response = asyncio.run(
            servicer.RefreshCatalogEntry(
                catalog_pb2.RefreshCatalogEntryRequest(
                    provider_file_id="provider-file-movie-1",
                    handle_key="handle-1",
                    entry_id="file:movie-file-1",
                ),
                cast(Any, _FakeContext()),
            )
        )

    assert response.success is True
    assert response.new_url == "https://cdn.example.com/fresh"
    assert len(provider_client.calls) == 1
    link, request = provider_client.calls[0]
    assert link == "https://api.example.com/restricted/movie-1"
    assert request.provider_file_id == "provider-file-movie-1"
    snapshot = servicer.build_governance_snapshot()
    assert snapshot["vfs_catalog_inline_refresh_requests"] == 1
    assert snapshot["vfs_catalog_inline_refresh_succeeded"] == 1
    assert snapshot["vfs_catalog_refresh_attempts"] == 1
    assert snapshot["vfs_catalog_refresh_succeeded"] == 1


def test_refresh_catalog_entry_deduplicates_concurrent_inline_refreshes() -> None:
    snapshot = _catalog_snapshot(generation_id="1", file_url="https://cdn.example.com/stale")
    provider_client = _FakeProviderClient("https://cdn.example.com/fresh")
    servicer = FilmuVfsCatalogGrpcServicer(
        cast(Any, _StubSupplier(current_snapshot=snapshot)),
        playback_clients={"realdebrid": provider_client},
    )

    async def invoke_refresh() -> catalog_pb2.RefreshCatalogEntryResponse:
        with patch(
            "filmu_py.services.vfs_server._probe_remote_direct_url",
            new=AsyncMock(return_value=True),
        ):
            return await servicer.RefreshCatalogEntry(
                catalog_pb2.RefreshCatalogEntryRequest(
                    provider_file_id="provider-file-movie-1",
                    handle_key="handle-1",
                    entry_id="file:movie-file-1",
                ),
                cast(Any, _FakeContext()),
            )

    async def run_concurrent() -> tuple[catalog_pb2.RefreshCatalogEntryResponse, ...]:
        return await asyncio.gather(invoke_refresh(), invoke_refresh())

    first, second = asyncio.run(run_concurrent())

    assert first.success is True
    assert second.success is True
    assert len(provider_client.calls) == 1
    snapshot = servicer.build_governance_snapshot()
    assert snapshot["vfs_catalog_inline_refresh_requests"] == 2
    assert snapshot["vfs_catalog_inline_refresh_deduplicated"] == 1


def test_refresh_catalog_entry_prefers_entry_id_over_shared_provider_file_id() -> None:
    first_snapshot = _catalog_snapshot(
        generation_id="1",
        file_url="https://cdn.example.com/stale-first",
    )
    second_snapshot = _catalog_snapshot(
        generation_id="1",
        file_url="https://cdn.example.com/stale-second",
    )
    assert second_snapshot.entries[3].file is not None
    second_file = replace(
        second_snapshot.entries[3].file,
        media_entry_id="movie-2",
        item_id="item-2",
        restricted_url="https://api.example.com/restricted/movie-2",
        unrestricted_url="https://cdn.example.com/stale-second",
        provider_file_id="provider-file-movie-1",
    )
    second_entry = replace(
        second_snapshot.entries[3],
        entry_id="file:movie-file-2",
        file=second_file,
    )

    merged_snapshot = VfsCatalogSnapshot(
        generation_id="1",
        published_at=first_snapshot.published_at,
        entries=(
            first_snapshot.entries[0],
            first_snapshot.entries[1],
            first_snapshot.entries[2],
            second_entry,
            first_snapshot.entries[3],
        ),
        stats=VfsCatalogStats(directory_count=3, file_count=2, blocked_item_count=0),
    )
    provider_client = _FakeProviderClient("https://cdn.example.com/fresh-second")
    servicer = FilmuVfsCatalogGrpcServicer(
        cast(Any, _StubSupplier(current_snapshot=merged_snapshot)),
        playback_clients={"realdebrid": provider_client},
    )

    with patch(
        "filmu_py.services.vfs_server._probe_remote_direct_url",
        new=AsyncMock(return_value=True),
    ):
        response = asyncio.run(
            servicer.RefreshCatalogEntry(
                catalog_pb2.RefreshCatalogEntryRequest(
                    provider_file_id="provider-file-movie-1",
                    handle_key="handle-2",
                    entry_id="file:movie-file-2",
                ),
                cast(Any, _FakeContext()),
            )
        )

    assert response.success is True
    assert response.new_url == "https://cdn.example.com/fresh-second"
    assert len(provider_client.calls) == 1
    link, request = provider_client.calls[0]
    assert link == "https://api.example.com/restricted/movie-2"
    assert request.item_id == "item-2"
    assert request.provider_file_id == "provider-file-movie-1"


def test_refresh_catalog_entry_retries_until_refreshed_url_validates() -> None:
    snapshot = _catalog_snapshot(generation_id="1", file_url="https://cdn.example.com/stale")
    provider_client = _FakeProviderClient(
        [
            "https://cdn.example.com/fresh-dead",
            "https://cdn.example.com/fresh-live",
        ]
    )
    servicer = FilmuVfsCatalogGrpcServicer(
        cast(Any, _StubSupplier(current_snapshot=snapshot)),
        playback_clients={"realdebrid": provider_client},
    )

    probe_mock = AsyncMock(side_effect=[False, True])
    with patch("filmu_py.services.vfs_server._probe_remote_direct_url", new=probe_mock):
        response = asyncio.run(
            servicer.RefreshCatalogEntry(
                catalog_pb2.RefreshCatalogEntryRequest(
                    provider_file_id="provider-file-movie-1",
                    handle_key="handle-3",
                    entry_id="file:movie-file-1",
                ),
                cast(Any, _FakeContext()),
            )
        )

    assert response.success is True
    assert response.new_url == "https://cdn.example.com/fresh-live"
    assert len(provider_client.calls) == 2
    assert probe_mock.await_count == 2
