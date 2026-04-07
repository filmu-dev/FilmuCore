from __future__ import annotations

from filmu_py.db.models import EpisodeORM, MediaItemORM, MovieORM, SeasonORM, ShowORM
from filmu_py.services.media import (
    build_media_specialization_record,
    update_media_specialization_record,
)


def _build_item(
    *, external_ref: str, title: str = "Example", attributes: dict[str, object] | None = None
) -> MediaItemORM:
    return MediaItemORM(
        external_ref=external_ref,
        title=title,
        state="requested",
        attributes=attributes or {},
    )


def test_build_media_specialization_record_creates_movie_from_tmdb_request() -> None:
    item = _build_item(external_ref="tmdb:123", attributes={"item_type": "movie", "tmdb_id": "123"})

    record = build_media_specialization_record(item, media_type="movie", attributes=item.attributes)

    assert isinstance(record, MovieORM)
    assert record.media_item_id == item.id
    assert record.tmdb_id == "123"


def test_build_media_specialization_record_creates_show_from_tvdb_request() -> None:
    item = _build_item(external_ref="tvdb:456", attributes={"item_type": "show", "tvdb_id": "456"})

    record = build_media_specialization_record(item, media_type="show", attributes=item.attributes)

    assert isinstance(record, ShowORM)
    assert record.media_item_id == item.id
    assert record.tvdb_id == "456"


def test_update_media_specialization_record_refreshes_existing_movie_identity() -> None:
    item = _build_item(external_ref="tmdb:999", attributes={"item_type": "movie", "tmdb_id": "999"})
    record = MovieORM(media_item_id=item.id, tmdb_id="111", imdb_id=None)

    updated = update_media_specialization_record(
        record,
        item=item,
        media_type="movie",
        attributes=item.attributes,
    )

    assert updated is record
    assert record.tmdb_id == "999"


def test_episode_specialization_hierarchy_can_link_to_season_and_show() -> None:
    show_item = _build_item(
        external_ref="tvdb:show-1", attributes={"item_type": "show", "tvdb_id": "show-1"}
    )
    season_item = _build_item(
        external_ref="tvdb:season-1",
        attributes={"item_type": "season", "tvdb_id": "season-1", "season_number": 2},
    )
    episode_item = _build_item(
        external_ref="tvdb:episode-1",
        attributes={"item_type": "episode", "tvdb_id": "episode-1", "episode_number": 5},
    )

    show = ShowORM(media_item_id=show_item.id, tvdb_id="show-1")
    season = SeasonORM(media_item_id=season_item.id, tvdb_id="season-1", season_number=2, show=show)
    episode = EpisodeORM(
        media_item_id=episode_item.id,
        tvdb_id="episode-1",
        episode_number=5,
        season=season,
    )

    assert season.show is show
    assert episode.season is season
    assert season in show.seasons
    assert episode in season.episodes
