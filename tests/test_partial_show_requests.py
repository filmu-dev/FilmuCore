from __future__ import annotations

from datetime import UTC, datetime

from filmu_py.db.models import MediaItemORM
from filmu_py.services.media import (
    _build_detail_record,
    _candidate_matches_partial_scope,
    _candidate_parsed_seasons,
    build_item_request_record,
    parse_stream_candidate_title,
    update_item_request_record,
)


def test_partial_season_request_persists_partial_range_fields() -> None:
    record = build_item_request_record(
        external_ref="tvdb:456",
        media_item_id="item-1",
        requested_title="Example Show",
        media_type="show",
        requested_seasons=[1, 2],
        is_partial=True,
        requested_at=datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
    )

    assert record.is_partial is True
    assert record.requested_seasons == [1, 2]
    assert record.requested_episodes is None


def test_full_request_persists_non_partial_defaults() -> None:
    record = build_item_request_record(
        external_ref="tvdb:456",
        media_item_id="item-1",
        requested_title="Example Show",
        media_type="show",
        requested_at=datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
    )

    assert record.is_partial is False
    assert record.requested_seasons is None
    assert record.requested_episodes is None


def test_repeated_upsert_preserves_existing_partial_ranges_when_omitted() -> None:
    record = build_item_request_record(
        external_ref="tvdb:456",
        media_item_id="item-1",
        requested_title="Example Show",
        media_type="show",
        requested_seasons=[1, 2],
        requested_episodes={"1": [1, 2, 3]},
        is_partial=True,
        requested_at=datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
    )

    updated = update_item_request_record(
        record,
        media_item_id="item-1",
        requested_title="Example Show",
        media_type="show",
        requested_at=datetime(2026, 3, 20, 13, 0, tzinfo=UTC),
    )

    assert updated is record
    assert record.is_partial is True
    assert record.requested_seasons == [1, 2]
    assert record.requested_episodes == {"1": [1, 2, 3]}


def test_item_detail_record_includes_request_summary() -> None:
    item = MediaItemORM(
        id="item-1",
        external_ref="tvdb:456",
        title="Example Show",
        state="requested",
        attributes={"item_type": "show", "tvdb_id": "456"},
    )
    item.item_requests = [
        build_item_request_record(
            external_ref="tvdb:456",
            media_item_id=item.id,
            requested_title=item.title,
            media_type="show",
            requested_seasons=[1, 2],
            is_partial=True,
            request_source="webhook:overseerr",
            requested_at=datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
        )
    ]

    detail = _build_detail_record(item, extended=False)

    assert detail.request is not None
    assert detail.request.is_partial is True
    assert detail.request.requested_seasons == [1, 2]
    assert detail.request.requested_episodes is None
    assert detail.request.request_source == "webhook:overseerr"


def test_item_detail_record_has_null_request_without_request_row() -> None:
    item = MediaItemORM(
        id="item-1",
        external_ref="tvdb:456",
        title="Example Show",
        state="requested",
        attributes={"item_type": "show", "tvdb_id": "456"},
    )
    item.item_requests = []

    detail = _build_detail_record(item, extended=False)

    assert detail.request is None


def test_parse_stage_filters_candidate_with_wrong_season() -> None:
    parsed = parse_stream_candidate_title("Example.Show.S02E01.1080p.WEB-DL")

    assert _candidate_matches_partial_scope(
        _candidate_parsed_seasons(parsed.parsed_title),
        [1],
    ) is False


def test_parse_stage_keeps_candidate_with_matching_season() -> None:
    parsed = parse_stream_candidate_title("Example.Show.S01E01.1080p.WEB-DL")

    assert _candidate_matches_partial_scope(
        _candidate_parsed_seasons(parsed.parsed_title),
        [1],
    ) is True


def test_parse_stage_rejects_candidate_with_no_season_info_by_default() -> None:
    """Candidates without season metadata are rejected for partial-scope requests (Fix 2).

    Ambiguous packs like ``Example.Show.Complete.Series`` carry no parseable
    season info and must not be silently accepted during targeted scrapes.
    """
    parsed = parse_stream_candidate_title("Example.Show.Complete.1080p.WEB-DL")

    assert _candidate_matches_partial_scope(
        _candidate_parsed_seasons(parsed.parsed_title),
        [1],
    ) is False


def test_parse_stage_accepts_candidate_with_no_season_info_when_allow_unknown() -> None:
    """Callers that need to accept unknown-season packs can opt in via allow_unknown=True."""
    parsed = parse_stream_candidate_title("Example.Show.Complete.1080p.WEB-DL")

    assert _candidate_matches_partial_scope(
        _candidate_parsed_seasons(parsed.parsed_title),
        [1],
        allow_unknown=True,
    ) is True


def test_parse_stage_full_request_applies_no_filter() -> None:
    parsed = parse_stream_candidate_title("Example.Show.S02E01.1080p.WEB-DL")

    assert _candidate_parsed_seasons(parsed.parsed_title) == [2]


def test_parse_stage_multi_season_torrent_kept_if_any_season_matches() -> None:
    parsed = parse_stream_candidate_title("Example.Show.S01-S03.1080p.WEB-DL")

    assert _candidate_matches_partial_scope(
        _candidate_parsed_seasons(parsed.parsed_title),
        [2],
    ) is True
