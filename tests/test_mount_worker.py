"""Mount-worker boundary tests for future VFS query planning."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import cast

from filmu_py.api.playback_resolution import PlaybackAttachment
from filmu_py.db.models import ActiveStreamORM, MediaEntryORM, MediaItemORM
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.mount_worker import (
    MountPlaybackSnapshotSupplier,
    PersistedMountMediaEntryQueryExecutor,
    build_mount_media_entry_query_contract,
    build_mount_media_entry_query_contract_from_snapshot,
)
from filmu_py.services.playback import DirectFileLinkLifecycleSnapshot, PlaybackResolutionSnapshot


class FakeScalarResult:
    def __init__(self, items: list[MediaItemORM]) -> None:
        self._items = items

    def first(self) -> MediaItemORM | None:
        return self._items[0] if self._items else None


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
    def __init__(self, items: list[MediaItemORM]) -> None:
        self._items = items

    @asynccontextmanager
    async def session(self) -> AsyncIterator[FakeSession]:
        yield FakeSession(self._items)


def _build_item(item_id: str) -> MediaItemORM:
    return MediaItemORM(
        id=item_id,
        external_ref=f"ext-{item_id}",
        title=f"Title for {item_id}",
        state="completed",
        attributes={},
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )


def test_build_mount_media_entry_query_contract_from_snapshot_prefers_media_entry_identity() -> (
    None
):
    item = _build_item("item-mount-contract-direct")
    snapshot = PlaybackResolutionSnapshot(
        direct=PlaybackAttachment(
            kind="remote-direct",
            locator="https://api.example.com/restricted-mount-direct",
            source_key="media-entry:restricted-fallback",
            provider="realdebrid",
            provider_download_id="download-mount-direct",
            provider_file_id="file-mount-direct",
            provider_file_path="folder/Mount Direct.mkv",
            original_filename="Mount Direct.mkv",
            file_size=123456,
            restricted_url="https://api.example.com/restricted-mount-direct",
            unrestricted_url="https://cdn.example.com/mount-direct",
            refresh_state="stale",
        ),
        hls=None,
        direct_ready=True,
        hls_ready=False,
        direct_lifecycle=DirectFileLinkLifecycleSnapshot(
            owner_kind="media-entry",
            owner_id="media-entry-mount-direct",
            provider_family="debrid",
            locator_source="restricted-url",
            restricted_fallback=True,
            match_basis="source-attachment-id",
            source_attachment_id="attachment-mount-direct",
            refresh_state="stale",
        ),
        missing_local_file=False,
    )

    contract = build_mount_media_entry_query_contract_from_snapshot(item, snapshot, role="direct")

    assert contract.item_id == item.id
    assert contract.role == "direct"
    assert contract.status == "queryable"
    assert contract.provider_family == "debrid"
    assert contract.provider == "realdebrid"
    assert contract.source_key == "media-entry:restricted-fallback"
    assert contract.restricted_fallback is True
    assert contract.blocked_reason is None
    assert contract.resolved_locator == "https://api.example.com/restricted-mount-direct"
    assert [step.strategy for step in contract.steps] == [
        "by-media-entry-id",
        "by-source-attachment-id",
        "by-provider-file-id",
        "by-provider-file-path",
        "by-provider-download-id-and-filename",
        "by-provider-download-id-and-provider-file-path",
        "by-provider-download-id-and-file-size",
    ]
    assert contract.steps[0].media_entry_id == "media-entry-mount-direct"
    assert contract.steps[1].source_attachment_id == "attachment-mount-direct"


def test_build_mount_media_entry_query_contract_from_snapshot_blocks_metadata_only_sources() -> (
    None
):
    item = _build_item("item-mount-contract-metadata")
    snapshot = PlaybackResolutionSnapshot(
        direct=None,
        hls=PlaybackAttachment(
            kind="remote-hls",
            locator="https://cdn.example.com/mount-hls.m3u8",
            source_key="hls_url",
            provider="realdebrid",
            provider_download_id="download-mount-hls",
            original_filename="Mount HLS.m3u8",
        ),
        direct_ready=False,
        hls_ready=True,
        hls_lifecycle=DirectFileLinkLifecycleSnapshot(
            owner_kind="metadata",
            owner_id=None,
            provider_family="debrid",
            locator_source="locator",
            restricted_fallback=False,
        ),
        missing_local_file=False,
    )

    contract = build_mount_media_entry_query_contract_from_snapshot(item, snapshot, role="hls")

    assert contract.status == "blocked"
    assert contract.blocked_reason == "metadata_only"
    assert contract.provider_family == "debrid"
    assert contract.provider == "realdebrid"
    assert contract.source_key == "hls_url"
    assert contract.steps == ()


def test_build_mount_media_entry_query_contract_from_snapshot_blocks_when_lifecycle_is_missing() -> (
    None
):
    item = _build_item("item-mount-contract-missing-lifecycle")
    snapshot = PlaybackResolutionSnapshot(
        direct=PlaybackAttachment(
            kind="remote-direct",
            locator="https://cdn.example.com/mount-direct-no-lifecycle",
            source_key="persisted",
            provider="realdebrid",
        ),
        hls=None,
        direct_ready=True,
        hls_ready=False,
        missing_local_file=False,
    )

    contract = build_mount_media_entry_query_contract_from_snapshot(item, snapshot, role="direct")

    assert contract.status == "blocked"
    assert contract.blocked_reason == "missing_lifecycle"
    assert contract.provider == "realdebrid"
    assert contract.source_key == "persisted"
    assert contract.resolved_locator == "https://cdn.example.com/mount-direct-no-lifecycle"
    assert contract.steps == ()


def test_build_mount_media_entry_query_contract_uses_snapshot_supplier_protocol() -> None:
    item = _build_item("item-mount-contract-supplier")
    snapshot = PlaybackResolutionSnapshot(
        direct=PlaybackAttachment(
            kind="remote-direct",
            locator="https://cdn.example.com/mount-direct-supplier",
            source_key="persisted",
            provider="realdebrid",
            provider_file_id="file-mount-supplier",
        ),
        hls=None,
        direct_ready=True,
        hls_ready=False,
        direct_lifecycle=DirectFileLinkLifecycleSnapshot(
            owner_kind="attachment",
            owner_id="attachment-owner-supplier",
            provider_family="debrid",
            locator_source="locator",
            restricted_fallback=False,
            match_basis="provider-file-id",
        ),
        missing_local_file=False,
    )

    class StubSnapshotSupplier:
        def build_resolution_snapshot(self, item: MediaItemORM) -> PlaybackResolutionSnapshot:
            assert item.id == "item-mount-contract-supplier"
            return snapshot

    supplier: MountPlaybackSnapshotSupplier = StubSnapshotSupplier()

    contract = build_mount_media_entry_query_contract(
        item,
        role="direct",
        playback_snapshot_supplier=supplier,
    )

    assert contract.status == "queryable"
    assert contract.steps[0].strategy == "by-provider-file-id"
    assert contract.steps[0].provider_file_id == "file-mount-supplier"


def test_persisted_mount_media_entry_query_executor_uses_active_stream_to_break_provider_identity_tie() -> (
    None
):
    item = _build_item("item-mount-executor-active-stream-tie")
    first_entry = MediaEntryORM(
        id="media-entry-first",
        item_id=item.id,
        kind="remote-direct",
        provider="realdebrid",
        provider_file_id="shared-provider-file-id",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    second_entry = MediaEntryORM(
        id="media-entry-second",
        item_id=item.id,
        kind="remote-direct",
        provider="realdebrid",
        provider_file_id="shared-provider-file-id",
        created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
    )
    item.media_entries = [first_entry, second_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-direct",
            item_id=item.id,
            media_entry_id=second_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 2, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 2, tzinfo=UTC),
        )
    ]
    snapshot = PlaybackResolutionSnapshot(
        direct=PlaybackAttachment(
            kind="remote-direct",
            locator="https://cdn.example.com/mount-executor-direct",
            source_key="persisted",
            provider="realdebrid",
            provider_file_id="shared-provider-file-id",
        ),
        hls=None,
        direct_ready=True,
        hls_ready=False,
        direct_lifecycle=DirectFileLinkLifecycleSnapshot(
            owner_kind="attachment",
            owner_id="attachment-owner",
            provider_family="debrid",
            locator_source="locator",
            restricted_fallback=False,
            match_basis="provider-file-id",
        ),
        missing_local_file=False,
    )
    contract = build_mount_media_entry_query_contract_from_snapshot(item, snapshot, role="direct")

    result = asyncio.run(
        PersistedMountMediaEntryQueryExecutor(
            cast(DatabaseRuntime, DummyDatabaseRuntime([item]))
        ).resolve_media_entry(contract)
    )

    assert result.media_entry_id == second_entry.id
    assert result.matched_strategy == "by-provider-file-id"


def test_persisted_mount_media_entry_query_executor_returns_empty_for_blocked_contract() -> None:
    item = _build_item("item-mount-executor-blocked")
    snapshot = PlaybackResolutionSnapshot(
        direct=None,
        hls=PlaybackAttachment(
            kind="remote-hls",
            locator="https://cdn.example.com/mount-executor-blocked.m3u8",
            source_key="hls_url",
        ),
        direct_ready=False,
        hls_ready=True,
        hls_lifecycle=DirectFileLinkLifecycleSnapshot(
            owner_kind="metadata",
            owner_id=None,
            provider_family="none",
            locator_source="locator",
            restricted_fallback=False,
        ),
        missing_local_file=False,
    )
    contract = build_mount_media_entry_query_contract_from_snapshot(item, snapshot, role="hls")

    result = asyncio.run(
        PersistedMountMediaEntryQueryExecutor(
            cast(DatabaseRuntime, DummyDatabaseRuntime([item]))
        ).resolve_media_entry(contract)
    )

    assert result.contract == contract
    assert result.media_entry_id is None
    assert result.matched_strategy is None
