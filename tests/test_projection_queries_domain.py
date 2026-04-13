from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from filmu_py.core.event_bus import EventBus
from filmu_py.db.models import EpisodeORM, MediaItemORM, MovieORM, SeasonORM, ShowORM
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.media import (
    CalendarProjectionRecord,
    MediaItemSpecializationRecord,
    MediaService,
    ParentIdsRecord,
    StatsProjection,
)
from filmu_py.state.item import ItemState


def _build_item(
    *,
    item_id: str,
    state: ItemState,
    item_type: str,
    title: str,
    aired_at: str | None = None,
    year: int | None = None,
) -> MediaItemORM:
    attributes: dict[str, object] = {"item_type": item_type}
    if aired_at is not None:
        attributes["aired_at"] = aired_at
    if year is not None:
        attributes["year"] = year
    return MediaItemORM(
        id=item_id,
        external_ref=f"ref:{item_id}",
        title=title,
        state=state.value,
        recovery_attempt_count=0,
        attributes=attributes,
        created_at=datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
    )


class _ScalarResult:
    def __init__(self, rows: list[MediaItemORM]) -> None:
        self._rows = rows

    def scalars(self) -> _ScalarResult:
        return self

    def all(self) -> list[MediaItemORM]:
        return self._rows


@dataclass
class _ProjectionSession:
    rows: list[MediaItemORM]

    async def execute(self, _statement: object) -> _ScalarResult:
        return _ScalarResult(self.rows)


@dataclass
class _ProjectionRuntime:
    rows: list[MediaItemORM]

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_ProjectionSession]:
        yield _ProjectionSession(self.rows)


def test_get_stats_counts_db_backed_projection_state_and_specializations() -> None:
    movie_item = _build_item(
        item_id="movie-1",
        state=ItemState.COMPLETED,
        item_type="movie",
        title="Movie One",
        year=2024,
    )
    movie_item.movie = MovieORM(media_item_id=movie_item.id)

    show_item = _build_item(
        item_id="show-1",
        state=ItemState.FAILED,
        item_type="show",
        title="Show One",
        year=2025,
    )
    show_item.show = ShowORM(media_item_id=show_item.id)

    episode_item = _build_item(
        item_id="episode-1",
        state=ItemState.SCRAPED,
        item_type="episode",
        title="Episode One",
        aired_at="2026-03-15T10:00:00+00:00",
    )
    season = SeasonORM(media_item_id="season-1", season_number=2)
    episode_item.episode = EpisodeORM(
        media_item_id=episode_item.id,
        season=season,
        episode_number=3,
    )

    service = MediaService(
        db=cast(DatabaseRuntime, _ProjectionRuntime([movie_item, show_item, episode_item])),
        event_bus=EventBus(),
    )

    projection = asyncio.run(service.get_stats())

    assert projection == StatsProjection(
        total_items=3,
        completed_items=1,
        failed_items=1,
        incomplete_items=1,
        movies=1,
        shows=1,
        episodes=1,
        seasons=0,
        states={
            "Requested": 0,
            "Indexed": 0,
            "Scraped": 1,
            "Downloaded": 0,
            "Partially Completed": 0,
            "Ongoing": 0,
            "Completed": 1,
            "Failed": 1,
            "Unreleased": 0,
        },
        activity={"2026-03-15": 3},
        media_year_releases=[
            # ordered ascending by year
            projection.media_year_releases[0],
            projection.media_year_releases[1],
        ],
    )
    assert [(item.year, item.count) for item in projection.media_year_releases] == [
        (2024, 1),
        (2025, 1),
    ]


def test_get_calendar_returns_episodes_sorted_by_air_date() -> None:
    later_item = _build_item(
        item_id="episode-later",
        state=ItemState.DOWNLOADED,
        item_type="episode",
        title="Later Episode",
        aired_at="2026-03-16T10:00:00+00:00",
    )
    later_item.episode = EpisodeORM(
        media_item_id=later_item.id,
        episode_number=4,
        season=SeasonORM(media_item_id="season-later", season_number=2),
    )

    earlier_item = _build_item(
        item_id="episode-earlier",
        state=ItemState.COMPLETED,
        item_type="episode",
        title="Earlier Episode",
        aired_at="2026-03-15T10:00:00+00:00",
    )
    earlier_item.episode = EpisodeORM(
        media_item_id=earlier_item.id,
        episode_number=2,
        season=SeasonORM(media_item_id="season-earlier", season_number=1),
    )

    service = MediaService(
        db=cast(DatabaseRuntime, _ProjectionRuntime([later_item, earlier_item])),
        event_bus=EventBus(),
    )

    projection = asyncio.run(service.get_calendar())

    assert projection == [
        CalendarProjectionRecord(
            item_id="episode-earlier",
            title="Earlier Episode",
            item_type="episode",
            tmdb_id=None,
            tvdb_id=None,
            episode_number=2,
            season_number=1,
            air_date="2026-03-15T10:00:00+00:00",
            last_state="Completed",
            release_data=None,
            specialization=MediaItemSpecializationRecord(
                item_type="episode",
                tmdb_id=None,
                tvdb_id=None,
                imdb_id=None,
                parent_ids=None,
                show_title="Earlier Episode",
                season_number=1,
                episode_number=2,
            ),
        ),
        CalendarProjectionRecord(
            item_id="episode-later",
            title="Later Episode",
            item_type="episode",
            tmdb_id=None,
            tvdb_id=None,
            episode_number=4,
            season_number=2,
            air_date="2026-03-16T10:00:00+00:00",
            last_state="Downloaded",
            release_data=None,
            specialization=MediaItemSpecializationRecord(
                item_type="episode",
                tmdb_id=None,
                tvdb_id=None,
                imdb_id=None,
                parent_ids=None,
                show_title="Later Episode",
                season_number=2,
                episode_number=4,
            ),
        ),
    ]


def test_get_calendar_snapshot_rebinds_episode_ids_to_parent_show_metadata() -> None:
    show_item = _build_item(
        item_id="show-1",
        state=ItemState.COMPLETED,
        item_type="show",
        title="Example Show",
    )
    show_item.show = ShowORM(media_item_id=show_item.id, tmdb_id="999", tvdb_id="555")

    episode_item = _build_item(
        item_id="episode-1",
        state=ItemState.COMPLETED,
        item_type="episode",
        title="Example Show - Episode",
        aired_at="2026-03-15T10:00:00+00:00",
    )
    episode_item.attributes["tmdb_id"] = "111"
    episode_item.attributes["tvdb_id"] = "222"
    season = SeasonORM(media_item_id="season-1", season_number=3, show=show_item.show)
    episode_item.episode = EpisodeORM(
        media_item_id=episode_item.id,
        episode_number=7,
        season=season,
    )

    service = MediaService(
        db=cast(DatabaseRuntime, _ProjectionRuntime([episode_item, show_item])),
        event_bus=EventBus(),
    )

    snapshot = asyncio.run(service.get_calendar_snapshot())

    assert snapshot["episode-1"].tmdb_id == "999"
    assert snapshot["episode-1"].tvdb_id == "555"
    assert snapshot["episode-1"].season == 3
    assert snapshot["episode-1"].episode == 7


def test_projection_empty_states_return_zero_counts_not_errors() -> None:
    service = MediaService(db=cast(DatabaseRuntime, _ProjectionRuntime([])), event_bus=EventBus())

    stats = asyncio.run(service.get_stats())
    calendar = asyncio.run(service.get_calendar())

    assert stats.total_items == 0
    assert stats.completed_items == 0
    assert stats.failed_items == 0
    assert stats.incomplete_items == 0
    assert stats.movies == 0
    assert stats.shows == 0
    assert stats.episodes == 0
    assert stats.states["Completed"] == 0
    assert stats.activity == {}
    assert calendar == []


def test_search_items_returns_show_type_for_show_rows() -> None:
    show_item = _build_item(
        item_id="show-1",
        state=ItemState.REQUESTED,
        item_type="show",
        title="Example Show",
    )
    service = MediaService(
        db=cast(DatabaseRuntime, _ProjectionRuntime([show_item])),
        event_bus=EventBus(),
    )

    page = asyncio.run(service.search_items(item_types=["show"]))

    assert len(page.items) == 1
    assert page.items[0].type == "show"


def test_search_items_prefers_specialization_identifiers_over_metadata() -> None:
    show_item = _build_item(
        item_id="show-1",
        state=ItemState.REQUESTED,
        item_type="movie",
        title="Example Show",
    )
    show_item.attributes["tmdb_id"] = "metadata-tmdb"
    show_item.attributes["tvdb_id"] = "metadata-tvdb"
    show_item.show = ShowORM(
        media_item_id=show_item.id,
        tmdb_id="specialized-tmdb",
        tvdb_id="specialized-tvdb",
        imdb_id="tt7654321",
    )
    service = MediaService(
        db=cast(DatabaseRuntime, _ProjectionRuntime([show_item])),
        event_bus=EventBus(),
    )

    page = asyncio.run(service.search_items(item_types=["show"]))

    assert len(page.items) == 1
    assert page.items[0].type == "show"
    assert page.items[0].tmdb_id == "specialized-tmdb"
    assert page.items[0].tvdb_id == "specialized-tvdb"
    assert page.items[0].specialization is not None
    assert page.items[0].specialization.imdb_id == "tt7654321"


def test_search_items_extended_metadata_prefers_specialization_hierarchy() -> None:
    show_item = _build_item(
        item_id="show-extended-1",
        state=ItemState.REQUESTED,
        item_type="movie",
        title="Canonical Show",
    )
    show_item.attributes["tmdb_id"] = "metadata-tmdb"
    show_item.attributes["tvdb_id"] = "metadata-tvdb"
    show_item.attributes["show_title"] = "Wrong Metadata Show"
    show_item.show = ShowORM(
        media_item_id=show_item.id,
        tmdb_id="specialized-tmdb",
        tvdb_id="specialized-tvdb",
        imdb_id="tt7654321",
    )
    service = MediaService(
        db=cast(DatabaseRuntime, _ProjectionRuntime([show_item])),
        event_bus=EventBus(),
    )

    page = asyncio.run(service.search_items(item_types=["show"], extended=True))

    assert len(page.items) == 1
    assert page.items[0].metadata is not None
    assert page.items[0].metadata["item_type"] == "show"
    assert page.items[0].metadata["tmdb_id"] == "specialized-tmdb"
    assert page.items[0].metadata["tvdb_id"] == "specialized-tvdb"
    assert page.items[0].metadata["imdb_id"] == "tt7654321"
    assert page.items[0].metadata["show_title"] == "Canonical Show"


def test_search_items_matches_show_and_tv_alias_filters() -> None:
    show_item = _build_item(
        item_id="show-1",
        state=ItemState.REQUESTED,
        item_type="show",
        title="Example Show",
    )
    service = MediaService(
        db=cast(DatabaseRuntime, _ProjectionRuntime([show_item])),
        event_bus=EventBus(),
    )

    show_page = asyncio.run(service.search_items(item_types=["show"]))
    tv_page = asyncio.run(service.search_items(item_types=["tv"]))

    assert len(show_page.items) == 1
    assert len(tv_page.items) == 1


def test_get_calendar_prefers_specialization_hierarchy_over_metadata() -> None:
    show_item = _build_item(
        item_id="show-1",
        state=ItemState.COMPLETED,
        item_type="show",
        title="Canonical Show",
    )
    show_item.show = ShowORM(media_item_id=show_item.id, tmdb_id="999", tvdb_id="555")

    episode_item = _build_item(
        item_id="episode-1",
        state=ItemState.COMPLETED,
        item_type="movie",
        title="Wrong Episode Title",
        aired_at="2026-03-15T10:00:00+00:00",
    )
    episode_item.attributes["show_title"] = "Metadata Show"
    episode_item.attributes["parent_ids"] = {"tmdb_id": "111", "tvdb_id": "222"}
    season = SeasonORM(media_item_id="season-1", season_number=3, show=show_item.show)
    episode_item.episode = EpisodeORM(
        media_item_id=episode_item.id,
        episode_number=7,
        tmdb_id="episode-tmdb",
        tvdb_id="episode-tvdb",
        season=season,
    )

    service = MediaService(
        db=cast(DatabaseRuntime, _ProjectionRuntime([episode_item, show_item])),
        event_bus=EventBus(),
    )

    projection = asyncio.run(service.get_calendar())

    assert projection[0].item_type == "episode"
    assert projection[0].title == "Canonical Show"
    assert projection[0].tmdb_id == "999"
    assert projection[0].tvdb_id == "555"
    assert projection[0].season_number == 3
    assert projection[0].episode_number == 7
    assert projection[0].specialization is not None
    assert projection[0].specialization.parent_ids == ParentIdsRecord(tmdb_id="999", tvdb_id="555")


def test_get_calendar_snapshot_exposes_specialization_identity_fields() -> None:
    show_item = _build_item(
        item_id="show-snapshot-1",
        state=ItemState.COMPLETED,
        item_type="show",
        title="Canonical Show",
    )
    show_item.show = ShowORM(
        media_item_id=show_item.id,
        tmdb_id="999",
        tvdb_id="555",
        imdb_id="tt-show",
    )

    episode_item = _build_item(
        item_id="episode-snapshot-1",
        state=ItemState.COMPLETED,
        item_type="movie",
        title="Wrong Episode Title",
        aired_at="2026-03-15T10:00:00+00:00",
    )
    season = SeasonORM(media_item_id="season-snapshot-1", season_number=3, show=show_item.show)
    episode_item.episode = EpisodeORM(
        media_item_id=episode_item.id,
        episode_number=7,
        tmdb_id="episode-tmdb",
        tvdb_id="episode-tvdb",
        imdb_id="tt-episode",
        season=season,
    )

    service = MediaService(
        db=cast(DatabaseRuntime, _ProjectionRuntime([episode_item, show_item])),
        event_bus=EventBus(),
    )

    snapshot = asyncio.run(service.get_calendar_snapshot())

    assert snapshot["episode-snapshot-1"].tmdb_id == "999"
    assert snapshot["episode-snapshot-1"].tvdb_id == "555"
    assert snapshot["episode-snapshot-1"].imdb_id == "tt-episode"
    assert snapshot["episode-snapshot-1"].parent_ids == ParentIdsRecord(
        tmdb_id="999", tvdb_id="555"
    )
