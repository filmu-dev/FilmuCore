from __future__ import annotations

import asyncio
from typing import cast

from filmu_py.db.models import MediaItemORM
from filmu_py.services.media import (
    EnrichmentResult,
    MediaItemSummaryRecord,
    MediaService,
    RequestMetadataResolution,
    RequestTimeMetadataRecord,
    _build_summary_record,
)
from filmu_py.services.tmdb import TmdbMetadataClient


def test_summary_record_normalizes_relative_tmdb_poster_path_to_absolute_url() -> None:
    item = MediaItemORM(
        id="item-1",
        external_ref="tmdb:550",
        title="Fight Club",
        state="requested",
        attributes={"item_type": "movie", "tmdb_id": "550", "poster_path": "/poster.jpg"},
    )

    summary = _build_summary_record(item, extended=False)

    assert summary.poster_path == "https://image.tmdb.org/t/p/original/poster.jpg"


def test_summary_record_preserves_absolute_poster_url() -> None:
    item = MediaItemORM(
        id="item-2",
        external_ref="tmdb:551",
        title="Absolute Poster",
        state="requested",
        attributes={
            "item_type": "movie",
            "tmdb_id": "551",
            "poster_path": "https://cdn.example.com/poster.jpg",
        },
    )

    summary = _build_summary_record(item, extended=False)

    assert summary.poster_path == "https://cdn.example.com/poster.jpg"


def test_summary_record_preserves_show_as_compatibility_type() -> None:
    item = MediaItemORM(
        id="item-show-1",
        external_ref="tvdb:555",
        title="Example Show",
        state="requested",
        attributes={"item_type": "show", "tvdb_id": "555"},
    )

    summary = _build_summary_record(item, extended=False)

    assert summary.type == "show"


def test_hydrate_summary_records_backfills_missing_poster_and_title_from_tmdb() -> None:
    service = MediaService(db=object(), event_bus=object())  # type: ignore[arg-type]
    summary = MediaItemSummaryRecord(
        id="item-3",
        type="movie",
        title="550",
        tmdb_id="550",
        external_ref="tmdb:550",
        poster_path=None,
    )

    service._resolve_tmdb_client = lambda: cast(TmdbMetadataClient, object())  # type: ignore[method-assign]

    async def fake_fetch(*, media_type: str, identifier: str) -> RequestMetadataResolution:
        assert media_type == "movie"
        assert identifier == "tmdb:550"
        return RequestMetadataResolution(
            metadata=RequestTimeMetadataRecord(
                title="Fight Club",
                attributes={"poster_path": "/poster.jpg"},
            ),
            enrichment=EnrichmentResult(
                source="tmdb",
                has_poster=True,
                has_imdb_id=False,
                has_tmdb_id=True,
                warnings=[],
            ),
        )

    service._fetch_request_metadata = fake_fetch  # type: ignore[method-assign]

    hydrated = asyncio.run(service._hydrate_summary_records([summary]))

    assert hydrated[0].title == "Fight Club"
    assert hydrated[0].poster_path == "https://image.tmdb.org/t/p/original/poster.jpg"


def test_hydrate_summary_records_leaves_existing_poster_untouched() -> None:
    service = MediaService(db=object(), event_bus=object())  # type: ignore[arg-type]
    summary = MediaItemSummaryRecord(
        id="item-4",
        type="movie",
        title="Fight Club",
        tmdb_id="550",
        external_ref="tmdb:550",
        poster_path="https://cdn.example.com/poster.jpg",
    )

    service._resolve_tmdb_client = lambda: cast(TmdbMetadataClient, object())  # type: ignore[method-assign]

    async def fake_fetch(*, media_type: str, identifier: str) -> RequestMetadataResolution:
        raise AssertionError("metadata fetch should not run when poster already exists")

    service._fetch_request_metadata = fake_fetch  # type: ignore[method-assign]

    hydrated = asyncio.run(service._hydrate_summary_records([summary]))

    assert hydrated == [summary]
