"""Proto-first FilmuVFS catalog supplier tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import cast

from filmu_py.db.models import (
    ActiveStreamORM,
    EpisodeORM,
    MediaEntryORM,
    MediaItemORM,
    SeasonORM,
    ShowORM,
)
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.playback import PlaybackSourceService
from filmu_py.services.vfs_catalog import FilmuVfsCatalogSupplier


class FakeScalarResult:
    def __init__(self, items: list[MediaItemORM]) -> None:
        self._items = items

    def all(self) -> list[MediaItemORM]:
        return list(self._items)

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


def _build_item(item_id: str, *, title: str = "Catalog Title") -> MediaItemORM:
    return MediaItemORM(
        id=item_id,
        external_ref=f"ext-{item_id}",
        title=title,
        state="completed",
        attributes={},
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )


def _build_supplier(items: list[MediaItemORM]) -> FilmuVfsCatalogSupplier:
    database = cast(DatabaseRuntime, DummyDatabaseRuntime(items))
    return FilmuVfsCatalogSupplier(
        database,
        playback_snapshot_supplier=PlaybackSourceService(database),
    )


def test_build_snapshot_projects_movie_file_into_movies_directory() -> None:
    item = _build_item("item-catalog-movie", title="Mount Movie")
    item.attributes = {
        "item_type": "movie",
        "year": 2024,
        "tmdb_id": "12345",
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-movie",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Mount Movie.mkv",
        download_url="https://api.example.com/restricted/movie",
        unrestricted_url="https://cdn.example.com/movie",
        provider="realdebrid",
        provider_download_id="download-movie",
        provider_file_id="provider-file-movie",
        provider_file_path="Movies/Mount Movie.mkv",
        size_bytes=987654321,
        refresh_state="ready",
        expires_at=datetime(2099, 3, 15, 13, 0, tzinfo=UTC),
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-movie",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    snapshot = asyncio.run(_build_supplier([item]).build_snapshot())

    assert snapshot.stats.directory_count >= 3
    assert snapshot.stats.file_count == 1
    assert snapshot.stats.blocked_item_count == 0
    file_entries = [entry for entry in snapshot.entries if entry.kind == "file"]
    assert len(file_entries) == 1
    file_entry = file_entries[0]
    assert file_entry.entry_id == "file:media-entry-catalog-movie"
    assert file_entry.path == "/movies/Mount Movie (2024)/Mount Movie.mkv"
    assert file_entry.parent_entry_id == "dir:/movies/Mount Movie (2024)"
    assert file_entry.file is not None
    assert file_entry.file.media_type == "movie"
    assert file_entry.file.transport == "remote-direct"
    assert file_entry.file.locator == "https://cdn.example.com/movie"
    assert file_entry.file.provider_file_id == "provider-file-movie"
    assert file_entry.file.active_roles == ("direct",)
    assert file_entry.correlation.media_entry_id == media_entry.id


def test_build_snapshot_projects_episode_file_into_show_hierarchy() -> None:
    item = _build_item("item-catalog-episode", title="Episode Title")
    item.attributes = {
        "item_type": "episode",
        "show_title": "Show Title",
        "season_number": 2,
        "episode_number": 3,
        "tvdb_id": "tvdb-episode",
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-episode",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Show Title - S02E03 - Episode Title.mkv",
        download_url="https://api.example.com/restricted/episode",
        unrestricted_url="https://cdn.example.com/episode",
        provider="realdebrid",
        provider_download_id="download-episode",
        provider_file_id="provider-file-episode",
        provider_file_path="Shows/Show Title/Season 02/Episode.mkv",
        size_bytes=123456789,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-episode",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    snapshot = asyncio.run(_build_supplier([item]).build_snapshot())

    file_entries = [entry for entry in snapshot.entries if entry.kind == "file"]
    assert len(file_entries) == 1
    file_entry = file_entries[0]
    assert (
        file_entry.path
        == "/shows/Show Title/Season 02/Show Title - S02E03 - Episode Title.mkv"
    )
    assert file_entry.file is not None
    assert file_entry.file.media_type == "episode"


def test_build_snapshot_prefers_specialization_hierarchy_over_stale_episode_metadata() -> None:
    show_item = _build_item("item-catalog-specialization-show", title="Canonical Show")
    show_item.attributes = {"item_type": "show", "tvdb_id": "tvdb-canonical-show"}
    show_item.show = ShowORM(
        media_item_id=show_item.id,
        tmdb_id="tmdb-canonical-show",
        tvdb_id="tvdb-canonical-show",
    )
    show_item.show.media_item = show_item

    season_item = _build_item("item-catalog-specialization-season", title="Canonical Show Season")
    season_item.attributes = {"item_type": "season", "season_number": 9}
    season_item.season = SeasonORM(
        media_item_id=season_item.id,
        show_id=show_item.show.id,
        season_number=2,
        tmdb_id="tmdb-canonical-season",
        tvdb_id="tvdb-canonical-season",
    )
    season_item.season.media_item = season_item
    season_item.season.show = show_item.show
    show_item.show.seasons = [season_item.season]

    item = _build_item("item-catalog-specialization-episode", title="Episode Title")
    item.attributes = {
        "item_type": "movie",
        "show_title": "Wrong Metadata Show",
        "season_number": 9,
        "episode_number": 99,
        "year": 2024,
    }
    item.episode = EpisodeORM(
        media_item_id=item.id,
        season_id=season_item.season.id,
        episode_number=3,
        tmdb_id="tmdb-canonical-episode",
        tvdb_id="tvdb-canonical-episode",
        imdb_id="tt-canonical-episode",
    )
    item.episode.media_item = item
    item.episode.season = season_item.season
    season_item.season.episodes = [item.episode]

    media_entry = MediaEntryORM(
        id="media-entry-catalog-specialization-episode",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Canonical Show - S02E03 - Episode Title.mkv",
        download_url="https://api.example.com/restricted/specialization-episode",
        unrestricted_url="https://cdn.example.com/specialization-episode",
        provider="realdebrid",
        provider_download_id="download-specialization-episode",
        provider_file_id="provider-file-specialization-episode",
        provider_file_path="Wrong Metadata Show/S09E99.mkv",
        size_bytes=123456789,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-specialization-episode",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    snapshot = asyncio.run(_build_supplier([item]).build_snapshot())

    file_entries = [entry for entry in snapshot.entries if entry.kind == "file"]
    assert len(file_entries) == 1
    assert (
        file_entries[0].path
        == "/shows/Canonical Show (2024)/Season 02/Canonical Show - S02E03 - Episode Title.mkv"
    )
    assert file_entries[0].file is not None
    assert file_entries[0].file.media_type == "episode"


def test_build_snapshot_uses_restricted_locator_for_placeholder_debrid_unrestricted_url() -> None:
    item = _build_item("item-catalog-placeholder-debrid", title="Placeholder Debrid Movie")
    item.attributes = {
        "item_type": "movie",
        "year": 2025,
        "tmdb_id": "67890",
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-placeholder-debrid",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Placeholder Debrid Movie.mkv",
        download_url="https://real-debrid.com/d/placeholder-lease",
        unrestricted_url="https://real-debrid.com/d/placeholder-lease",
        provider="realdebrid",
        provider_download_id="placeholder-download-id",
        provider_file_id="provider-file-placeholder-debrid",
        provider_file_path="Movies/Placeholder Debrid Movie.mkv",
        size_bytes=123456789,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-placeholder-debrid",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    snapshot = asyncio.run(_build_supplier([item]).build_snapshot())

    file_entries = [entry for entry in snapshot.entries if entry.kind == "file"]
    assert len(file_entries) == 1
    file_entry = file_entries[0]
    assert file_entry.file is not None
    assert file_entry.file.locator == "https://real-debrid.com/d/placeholder-lease"
    assert file_entry.file.restricted_url == "https://real-debrid.com/d/placeholder-lease"
    assert file_entry.file.unrestricted_url is None
    assert file_entry.file.lease_state == "stale"
    assert file_entry.file.restricted_fallback is True


def test_build_snapshot_defaults_show_level_episode_pack_to_season_01_when_unlabeled() -> None:
    item = _build_item("item-catalog-show-pack", title="Frieren: Beyond Journey's End")
    item.attributes = {
        "item_type": "show",
        "tvdb_id": "424536",
        "tmdb_id": "209867",
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-show-pack",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Frieren - Episode 04.mkv",
        download_url="https://api.example.com/restricted/frieren-pack",
        unrestricted_url="https://cdn.example.com/frieren-pack",
        provider="realdebrid",
        provider_download_id="download-frieren-pack",
        provider_file_id="provider-file-frieren-pack",
        provider_file_path="Frieren- Beyond Journey's End E4 The Land Where Souls Rest.mkv",
        size_bytes=456789123,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-show-pack",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    snapshot = asyncio.run(_build_supplier([item]).build_snapshot())

    file_entries = [entry for entry in snapshot.entries if entry.kind == "file"]
    assert len(file_entries) == 1
    file_entry = file_entries[0]
    assert "/shows/Frieren_ Beyond Journey's End/Season 01/" in file_entry.path
    assert file_entry.path.endswith("/Frieren - Episode 04.mkv")
    assert file_entry.file is not None
    assert file_entry.file.media_type == "show"


def test_build_snapshot_normalizes_show_directory_with_year_and_preserves_original_filename() -> None:
    item = _build_item("item-catalog-episode-year", title="Stranger Things")
    item.attributes = {
        "item_type": "episode",
        "show_title": "Stranger Things",
        "year": 2016,
        "season_number": 5,
        "episode_number": 2,
        "tvdb_id": "tvdb-st",
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-episode-year",
        item_id=item.id,
        kind="remote-direct",
        original_filename="stranger things final file name.mkv",
        download_url="https://api.example.com/restricted/st",
        unrestricted_url="https://cdn.example.com/st",
        provider="realdebrid",
        provider_download_id="download-st",
        provider_file_id="provider-file-st",
        provider_file_path="Shows/Stranger Things/S05E02.mkv",
        size_bytes=111222333,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-episode-year",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    snapshot = asyncio.run(_build_supplier([item]).build_snapshot())

    file_entries = [entry for entry in snapshot.entries if entry.kind == "file"]
    assert len(file_entries) == 1
    assert (
        file_entries[0].path
        == "/shows/Stranger Things (2016)/Season 05/stranger things final file name.mkv"
    )


def test_build_snapshot_infers_season_and_episode_from_sxx_xxx_provider_pattern() -> None:
    item = _build_item("item-catalog-s05x08", title="Stranger Things")
    item.attributes = {
        "item_type": "show",
        "year": 2016,
        "tvdb_id": "305288",
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-s05x08",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Stranger Things S05x08 Il mondo reale AC3 5.1 ITA.ENG 1080p H265 sub ita.eng.mkv",
        download_url="https://api.example.com/restricted/st-s05x08",
        unrestricted_url="https://cdn.example.com/st-s05x08",
        provider="realdebrid",
        provider_download_id="download-st-s05x08",
        provider_file_id="provider-file-st-s05x08",
        provider_file_path="Stranger Things S05x08 Il mondo reale AC3 5.1 ITA.ENG 1080p H265 sub ita.eng.mkv",
        size_bytes=111111,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-s05x08",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    snapshot = asyncio.run(_build_supplier([item]).build_snapshot())

    file_entries = [entry for entry in snapshot.entries if entry.kind == "file"]
    assert len(file_entries) == 1
    assert (
        file_entries[0].path
        == "/shows/Stranger Things (2016)/Season 05/Stranger Things S05x08 Il mondo reale AC3 5.1 ITA.ENG 1080p H265 sub ita.eng.mkv"
    )


def test_build_snapshot_prefers_show_specialization_for_show_level_pack_directory() -> None:
    item = _build_item("item-catalog-specialization-show-pack", title="Pack Title")
    item.attributes = {
        "item_type": "movie",
        "show_title": "Wrong Metadata Show",
        "tvdb_id": "metadata-tvdb-show",
    }
    item.show = ShowORM(
        media_item_id=item.id,
        tmdb_id="tmdb-show-pack",
        tvdb_id="tvdb-show-pack",
        imdb_id="tt-show-pack",
    )
    item.show.media_item = item

    media_entry = MediaEntryORM(
        id="media-entry-catalog-specialization-show-pack",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Pack Title Episode 04.mkv",
        download_url="https://api.example.com/restricted/show-pack",
        unrestricted_url="https://cdn.example.com/show-pack",
        provider="realdebrid",
        provider_download_id="download-show-pack",
        provider_file_id="provider-file-show-pack",
        provider_file_path="Wrong Metadata Show Episode 04.mkv",
        size_bytes=456789123,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-specialization-show-pack",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    snapshot = asyncio.run(_build_supplier([item]).build_snapshot())

    file_entries = [entry for entry in snapshot.entries if entry.kind == "file"]
    assert len(file_entries) == 1
    assert "/shows/Pack Title/Season 01/" in file_entries[0].path
    assert "Wrong Metadata Show" not in file_entries[0].path
    assert file_entries[0].file is not None
    assert file_entries[0].file.media_type == "show"


def test_build_delta_emits_removal_when_existing_entry_path_changes() -> None:
    item = _build_item("item-catalog-path-move", title="Stranger Things")
    item.attributes = {
        "item_type": "episode",
        "show_title": "Stranger Things",
        "year": 2016,
        "season_number": 1,
        "episode_number": 1,
        "tvdb_id": "tvdb-path-move",
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-path-move",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Stranger Things - S01E01 - The Vanishing of Will Byers.mkv",
        download_url="https://api.example.com/restricted/path-move",
        unrestricted_url="https://cdn.example.com/path-move",
        provider="realdebrid",
        provider_download_id="download-path-move",
        provider_file_id="provider-file-path-move",
        provider_file_path="Shows/Stranger Things/Season 01/Stranger Things - S01E01.mkv",
        size_bytes=321321321,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-path-move",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    previous = asyncio.run(_build_supplier([item]).build_snapshot())

    updated_item = _build_item("item-catalog-path-move", title="Stranger Things")
    updated_item.attributes = {
        "item_type": "episode",
        "show_title": "Stranger Things",
        "year": 2016,
        "season_number": 1,
        "episode_number": 1,
        "tvdb_id": "tvdb-path-move",
    }
    updated_media_entry = MediaEntryORM(
        id="media-entry-catalog-path-move",
        item_id=updated_item.id,
        kind="remote-direct",
        original_filename="Completely Different Name.mp4",
        download_url="https://api.example.com/restricted/path-move",
        unrestricted_url="https://cdn.example.com/path-move",
        provider="realdebrid",
        provider_download_id="download-path-move",
        provider_file_id="provider-file-path-move",
        provider_file_path="Provider/Unreadable/File.mp4",
        size_bytes=321321321,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 5, tzinfo=UTC),
    )
    updated_item.media_entries = [updated_media_entry]
    updated_item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-path-move",
            item_id=updated_item.id,
            media_entry_id=updated_media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    delta = asyncio.run(_build_supplier([updated_item]).build_delta(previous))

    assert len(delta.removals) == 1
    assert delta.removals[0].entry_id == "file:media-entry-catalog-path-move"
    assert (
        delta.removals[0].path
        == "/shows/Stranger Things (2016)/Season 01/Stranger Things - S01E01 - The Vanishing of Will Byers.mkv"
    )
    assert any(
        entry.entry_id == "file:media-entry-catalog-path-move"
        and entry.path == "/shows/Stranger Things (2016)/Season 01/Completely Different Name.mp4"
        for entry in delta.upserts
    )


def test_build_snapshot_detects_season_ranges_from_pack_paths() -> None:
    item = _build_item("item-catalog-season-range", title="Range Show")
    item.attributes = {
        "item_type": "show",
        "tvdb_id": "900001",
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-season-range",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Range Show Pack.mkv",
        download_url="https://api.example.com/restricted/range-show",
        unrestricted_url="https://cdn.example.com/range-show",
        provider="realdebrid",
        provider_download_id="download-range-show",
        provider_file_id="provider-file-range-show",
        provider_file_path="Range.Show.S03-S05.2160p.WEB-DL.mkv",
        size_bytes=999999,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-season-range",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    snapshot = asyncio.run(_build_supplier([item]).build_snapshot())

    file_entries = [entry for entry in snapshot.entries if entry.kind == "file"]
    assert len(file_entries) == 1
    file_entry = file_entries[0]
    assert "/shows/Range Show/Season 03/" in file_entry.path


def test_build_snapshot_ignores_specials_for_season_inference() -> None:
    item = _build_item("item-catalog-specials", title="Specials Show")
    item.attributes = {
        "item_type": "show",
        "tvdb_id": "900002",
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-specials",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Specials Show Specials Collection.mkv",
        download_url="https://api.example.com/restricted/specials-show",
        unrestricted_url="https://cdn.example.com/specials-show",
        provider="realdebrid",
        provider_download_id="download-specials-show",
        provider_file_id="provider-file-specials-show",
        provider_file_path="Specials.Show.Complete.Series.Specials.Included.mkv",
        size_bytes=123123,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-specials",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    snapshot = asyncio.run(_build_supplier([item]).build_snapshot())

    file_entries = [entry for entry in snapshot.entries if entry.kind == "file"]
    assert len(file_entries) == 1
    file_entry = file_entries[0]
    assert "/shows/Specials Show/Season " not in file_entry.path
    assert file_entry.path.startswith("/shows/Specials Show/")


def test_build_snapshot_counts_items_blocked_before_mount_query_resolution() -> None:
    item = _build_item("item-catalog-blocked", title="Blocked Item")

    snapshot = asyncio.run(_build_supplier([item]).build_snapshot())

    assert snapshot.stats.file_count == 0
    assert snapshot.stats.blocked_item_count == 1
    assert snapshot.blocked_items[0].reason == "no_attachment"
    assert all(entry.kind == "directory" for entry in snapshot.entries)


def test_build_delta_reports_removed_catalog_file() -> None:
    item = _build_item("item-catalog-delta", title="Delta Movie")
    item.attributes = {
        "item_type": "movie",
        "year": 2022,
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-delta",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Delta Movie.mkv",
        download_url="https://api.example.com/restricted/delta",
        unrestricted_url="https://cdn.example.com/delta",
        provider="realdebrid",
        provider_download_id="download-delta",
        provider_file_id="provider-file-delta",
        provider_file_path="Movies/Delta Movie.mkv",
        size_bytes=4444,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-delta",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    previous_snapshot = asyncio.run(_build_supplier([item]).build_snapshot())
    delta = asyncio.run(_build_supplier([]).build_delta(previous_snapshot))

    assert delta.base_generation_id == previous_snapshot.generation_id
    assert delta.upserts == ()
    assert len(delta.removals) == 2
    removal_by_id = {removal.entry_id: removal for removal in delta.removals}
    assert (
        removal_by_id["file:media-entry-catalog-delta"].path
        == "/movies/Delta Movie (2022)/Delta Movie.mkv"
    )
    assert removal_by_id["dir:/movies/Delta Movie (2022)"].path == "/movies/Delta Movie (2022)"


def test_build_snapshot_reuses_generation_when_catalog_is_unchanged() -> None:
    item = _build_item("item-catalog-stable-generation", title="Stable Movie")
    item.attributes = {
        "item_type": "movie",
        "year": 2024,
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-stable-generation",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Stable Movie.mkv",
        download_url="https://api.example.com/restricted/stable",
        unrestricted_url="https://cdn.example.com/stable",
        provider="realdebrid",
        provider_download_id="download-stable",
        provider_file_id="provider-file-stable",
        provider_file_path="Movies/Stable Movie.mkv",
        size_bytes=7777,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-stable-generation",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    supplier = _build_supplier([item])
    first_snapshot = asyncio.run(supplier.build_snapshot())
    second_snapshot = asyncio.run(supplier.build_snapshot())

    assert first_snapshot.generation_id == second_snapshot.generation_id


def test_build_delta_since_returns_delta_for_known_generation() -> None:
    item = _build_item("item-catalog-known-generation", title="Known Generation Movie")
    item.attributes = {
        "item_type": "movie",
        "year": 2025,
    }
    media_entry = MediaEntryORM(
        id="media-entry-catalog-known-generation",
        item_id=item.id,
        kind="remote-direct",
        original_filename="Known Generation Movie.mkv",
        download_url="https://api.example.com/restricted/known-generation",
        unrestricted_url="https://cdn.example.com/known-generation",
        provider="realdebrid",
        provider_download_id="download-known-generation",
        provider_file_id="provider-file-known-generation",
        provider_file_path="Movies/Known Generation Movie.mkv",
        size_bytes=8888,
        refresh_state="ready",
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    item.media_entries = [media_entry]
    item.active_streams = [
        ActiveStreamORM(
            id="active-stream-catalog-known-generation",
            item_id=item.id,
            media_entry_id=media_entry.id,
            role="direct",
            created_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
            updated_at=datetime(2026, 3, 14, 12, 1, tzinfo=UTC),
        )
    ]

    items = [item]
    supplier = _build_supplier(items)
    previous_snapshot = asyncio.run(supplier.build_snapshot())
    items.clear()

    delta = asyncio.run(supplier.build_delta_since(int(previous_snapshot.generation_id)))

    assert delta is not None
    assert delta.base_generation_id == previous_snapshot.generation_id
    assert len(delta.removals) == 2
