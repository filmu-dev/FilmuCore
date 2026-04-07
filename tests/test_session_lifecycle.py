from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

from sqlalchemy.ext.asyncio import async_sessionmaker

from filmu_py.db.models import ActiveStreamORM, MediaEntryORM
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.vfs_catalog import FilmuVfsCatalogSupplier
from tests.test_vfs_catalog import _build_item


def _build_catalog_item(item_id: str, *, title: str) -> object:
    item = _build_item(item_id, title=title)
    item.attributes = {"item_type": "movie", "year": 2024}
    media_entry = MediaEntryORM(
        id=f"media-entry-{item_id}",
        item_id=item.id,
        kind="remote-direct",
        original_filename=f"{title}.mkv",
        download_url=f"https://api.example.com/restricted/{item_id}",
        unrestricted_url=f"https://cdn.example.com/{item_id}",
        provider="realdebrid",
        provider_download_id=f"download-{item_id}",
        provider_file_id=f"provider-file-{item_id}",
        provider_file_path=f"Movies/{title}.mkv",
        size_bytes=7777,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id=f"active-stream-{item_id}",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]
    return item


class _MutableFakeSession:
    def __init__(self, items_ref: list[object]) -> None:
        self._items_ref = items_ref

    async def execute(self, stmt: object) -> object:
        _ = stmt

        class _ScalarResult:
            def __init__(self, items_ref: list[object]) -> None:
                self._items_ref = items_ref

            def all(self) -> list[object]:
                return list(self._items_ref)

            def first(self) -> object | None:
                return self._items_ref[0] if self._items_ref else None

        class _Result:
            def __init__(self, items_ref: list[object]) -> None:
                self._items_ref = items_ref

            def scalars(self) -> _ScalarResult:
                return _ScalarResult(self._items_ref)

        return _Result(self._items_ref)


class _MutableDummyDatabaseRuntime:
    def __init__(self, items_ref: list[object]) -> None:
        self._items_ref = items_ref

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_MutableFakeSession]:
        yield _MutableFakeSession(self._items_ref)


class _StubPlaybackSnapshotSupplier:
    def build_resolution_snapshot(self, item: object) -> object:
        media_entry = item.media_entries[0]
        direct = SimpleNamespace(
            locator=cast(str, media_entry.unrestricted_url),
            local_path=media_entry.local_path,
            restricted_url=media_entry.download_url,
            unrestricted_url=media_entry.unrestricted_url,
            original_filename=media_entry.original_filename,
            file_size=media_entry.size_bytes,
            provider=media_entry.provider,
            provider_download_id=media_entry.provider_download_id,
            provider_file_id=media_entry.provider_file_id,
            provider_file_path=media_entry.provider_file_path,
            source_key=f"media-entry:{media_entry.id}",
        )
        lifecycle = SimpleNamespace(
            owner_kind="media-entry",
            owner_id=media_entry.id,
            provider_family="debrid",
            locator_source="unrestricted-url",
            match_basis="provider-file-id",
            restricted_fallback=False,
            source_attachment_id=None,
            refresh_state=media_entry.refresh_state,
            expires_at=None,
            last_refreshed_at=None,
            last_refresh_error=None,
        )
        return SimpleNamespace(direct=direct, direct_lifecycle=lifecycle)


def test_database_runtime_enforces_hardened_session_factory() -> None:
    runtime = DatabaseRuntime("postgresql+asyncpg://postgres:postgres@localhost:5432/filmu")

    try:
        assert runtime.engine.pool._recycle <= 1800  # type: ignore[attr-defined]
        assert runtime.session_factory.kw["expire_on_commit"] is True
    finally:
        asyncio.run(runtime.dispose())


def test_session_factory_exposes_async_sessionmaker() -> None:
    runtime = DatabaseRuntime("postgresql+asyncpg://postgres:postgres@localhost:5432/filmu")

    try:
        assert isinstance(runtime.session_factory, async_sessionmaker)
        assert runtime.session_factory.kw["expire_on_commit"] is True
    finally:
        asyncio.run(runtime.dispose())


def test_session_not_reused_across_catalog_polls() -> None:
    first_item = _build_catalog_item("item-session-lifecycle", title="First Title")
    items: list[object] = [first_item]
    database = cast(DatabaseRuntime, _MutableDummyDatabaseRuntime(items))
    supplier = FilmuVfsCatalogSupplier(
        database,
        playback_snapshot_supplier=cast(object, _StubPlaybackSnapshotSupplier()),
    )

    first_snapshot = asyncio.run(supplier.build_snapshot())

    updated_item = _build_catalog_item("item-session-lifecycle", title="Second Title")
    items[0] = updated_item

    second_snapshot = asyncio.run(supplier.build_snapshot())

    first_paths = {entry.path for entry in first_snapshot.entries}
    second_paths = {entry.path for entry in second_snapshot.entries}
    assert first_paths != second_paths
