from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import cast

from filmu_py.core.event_bus import EventBus
from filmu_py.db.models import MediaEntryORM, MediaItemORM
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.debrid import TorrentFile, TorrentInfo
from filmu_py.services.media import MediaService


def _build_item(*, item_id: str = "item-media-entry") -> MediaItemORM:
    return MediaItemORM(
        id=item_id,
        external_ref=f"tmdb:{item_id}",
        title="Example Show",
        state="downloaded",
        attributes={"item_type": "show", "tmdb_id": "123"},
    )


def _build_torrent_info(
    *,
    provider_torrent_id: str,
    file_name: str,
    file_id: str = "1",
    download_url: str,
    file_size_bytes: int = 1024,
) -> TorrentInfo:
    return TorrentInfo(
        provider_torrent_id=provider_torrent_id,
        status="downloaded",
        files=[
            TorrentFile(
                file_id=file_id,
                file_name=file_name,
                file_size_bytes=file_size_bytes,
                selected=True,
                download_url=download_url,
                media_type="episode",
            )
        ],
        links=[download_url],
    )


class _ExecuteResult:
    def __init__(self, item: MediaItemORM | None) -> None:
        self._item = item

    def scalar_one_or_none(self) -> MediaItemORM | None:
        return self._item


class _Session:
    def __init__(self, item: MediaItemORM) -> None:
        self.item = item
        self.committed = False

    async def execute(self, _statement: object) -> _ExecuteResult:
        return _ExecuteResult(self.item)

    def add(self, obj: object) -> None:
        if isinstance(obj, MediaEntryORM) and obj not in self.item.media_entries:
            self.item.media_entries.append(obj)

    async def commit(self) -> None:
        self.committed = True


@dataclass
class _DummyDatabaseRuntime:
    item: MediaItemORM
    last_session: _Session | None = None

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_Session]:
        session = _Session(self.item)
        self.last_session = session
        yield session


def _build_media_service(runtime: _DummyDatabaseRuntime) -> MediaService:
    return MediaService(db=cast(DatabaseRuntime, runtime), event_bus=EventBus())


def test_persist_debrid_download_entries_reuses_existing_row_for_same_provider_path() -> None:
    item = _build_item()
    runtime = _DummyDatabaseRuntime(item=item)
    service = _build_media_service(runtime)

    first_url = "https://cdn.example.com/for-all-mankind-s01e08"
    first_torrent = _build_torrent_info(
        provider_torrent_id="old-download-id",
        file_name="For.All.Mankind.S01E08.mkv",
        download_url=first_url,
        file_size_bytes=800 * 1024 * 1024,
    )
    first_persisted = asyncio.run(
        service.persist_debrid_download_entries(
            media_item_id=item.id,
            provider="realdebrid",
            provider_download_id="old-download-id",
            torrent_info=first_torrent,
            download_urls=[first_url],
        )
    )

    assert len(first_persisted) == 1
    assert len(item.media_entries) == 1

    initial_entry = first_persisted[0]
    second_url = "https://cdn.example.com/for-all-mankind-s01e08-new"
    second_torrent = _build_torrent_info(
        provider_torrent_id="new-download-id",
        file_name="For.All.Mankind.S01E08.mkv",
        download_url=second_url,
        file_size_bytes=800 * 1024 * 1024,
    )
    second_persisted = asyncio.run(
        service.persist_debrid_download_entries(
            media_item_id=item.id,
            provider="realdebrid",
            provider_download_id="new-download-id",
            torrent_info=second_torrent,
            download_urls=[second_url],
        )
    )

    assert len(second_persisted) == 1
    assert len(item.media_entries) == 1
    assert second_persisted[0] is initial_entry
    assert initial_entry.provider_download_id == "new-download-id"
    assert initial_entry.provider_file_path == "For.All.Mankind.S01E08.mkv"
    assert initial_entry.download_url == second_url
    assert initial_entry.unrestricted_url is None
    assert initial_entry.refresh_state == "stale"


def test_persist_debrid_download_entries_keeps_distinct_rows_for_distinct_files() -> None:
    item = _build_item(item_id="item-two-files")
    runtime = _DummyDatabaseRuntime(item=item)
    service = _build_media_service(runtime)

    first_torrent = _build_torrent_info(
        provider_torrent_id="download-one",
        file_name="For.All.Mankind.S01E08.mkv",
        download_url="https://cdn.example.com/s01e08",
    )
    second_torrent = _build_torrent_info(
        provider_torrent_id="download-two",
        file_name="For.All.Mankind.S02E03.mkv",
        download_url="https://cdn.example.com/s02e03",
    )

    asyncio.run(
        service.persist_debrid_download_entries(
            media_item_id=item.id,
            provider="realdebrid",
            provider_download_id="download-one",
            torrent_info=first_torrent,
            download_urls=["https://cdn.example.com/s01e08"],
        )
    )
    asyncio.run(
        service.persist_debrid_download_entries(
            media_item_id=item.id,
            provider="realdebrid",
            provider_download_id="download-two",
            torrent_info=second_torrent,
            download_urls=["https://cdn.example.com/s02e03"],
        )
    )

    assert len(item.media_entries) == 2
    assert {entry.provider_file_path for entry in item.media_entries} == {
        "For.All.Mankind.S01E08.mkv",
        "For.All.Mankind.S02E03.mkv",
    }
    assert all(entry.unrestricted_url is None for entry in item.media_entries)
    assert all(entry.refresh_state == "stale" for entry in item.media_entries)

