from __future__ import annotations

from filmu_py.db.models import MediaItemORM
from filmu_py.services.media import (
    ParsedStreamCandidateValidation,
    attach_parse_validation,
    parse_stream_candidate_title,
    parse_stage_rejection_reason,
    validate_parsed_stream_candidate,
)


def _build_item(*, external_ref: str, attributes: dict[str, object]) -> MediaItemORM:
    return MediaItemORM(
        external_ref=external_ref,
        title="Example",
        state="requested",
        attributes=attributes,
    )


def test_parse_stream_candidate_title_extracts_resolution_and_payload() -> None:
    record = parse_stream_candidate_title("Example.Show.S02E05.1080p.WEB-DL.x265-GROUP.mkv")

    assert record.raw_title == "Example.Show.S02E05.1080p.WEB-DL.x265-GROUP.mkv"
    assert record.resolution == "1080p"
    assert record.parsed_title["title"] == "Example Show"
    assert record.parsed_title["season"] == 2
    assert record.parsed_title["episode"] == 5


def test_validate_parsed_stream_candidate_rejects_episode_for_movie_request() -> None:
    item = _build_item(external_ref="tmdb:123", attributes={"item_type": "movie", "tmdb_id": "123"})
    candidate = parse_stream_candidate_title("Example.Show.S02E05.1080p.WEB-DL.x265-GROUP.mkv")

    validation = validate_parsed_stream_candidate(item, candidate)

    assert validation == ParsedStreamCandidateValidation(
        ok=False,
        reason="movie_request_got_episode_candidate",
    )


def test_validate_parsed_stream_candidate_accepts_matching_episode_request() -> None:
    item = _build_item(
        external_ref="tvdb:456",
        attributes={
            "item_type": "episode",
            "tvdb_id": "456",
            "season_number": 2,
            "episode_number": 5,
        },
    )
    candidate = parse_stream_candidate_title("Example.Show.S02E05.1080p.WEB-DL.x265-GROUP.mkv")

    validation = validate_parsed_stream_candidate(item, candidate)

    assert validation.ok is True
    assert validation.reason is None


def test_validate_parsed_stream_candidate_rejects_season_mismatch() -> None:
    item = _build_item(
        external_ref="tvdb:456",
        attributes={"item_type": "season", "tvdb_id": "456", "season_number": 3},
    )
    candidate = parse_stream_candidate_title("Example.Show.S02E05.1080p.WEB-DL.x265-GROUP.mkv")

    validation = validate_parsed_stream_candidate(item, candidate)

    assert validation == ParsedStreamCandidateValidation(ok=False, reason="season_mismatch")


def test_validate_parsed_stream_candidate_rejects_season_range_that_misses_requested_season() -> (
    None
):
    item = _build_item(
        external_ref="tvdb:456",
        attributes={"item_type": "season", "tvdb_id": "456", "season_number": 4},
    )
    candidate = parse_stream_candidate_title("Example.Show.S01-S03.1080p.WEB-DL.x265-GROUP.mkv")

    validation = validate_parsed_stream_candidate(item, candidate)

    assert validation == ParsedStreamCandidateValidation(ok=False, reason="season_mismatch")


def test_validate_parsed_stream_candidate_rejects_incomplete_episode_candidate_for_season_request() -> (
    None
):
    item = _build_item(
        external_ref="tvdb:456",
        attributes={"item_type": "season", "tvdb_id": "456", "season_number": 1},
    )
    candidate = parse_stream_candidate_title("Example.Show.S01E02.1080p.WEB-DL.x265-GROUP.mkv")

    validation = validate_parsed_stream_candidate(item, candidate)

    assert validation == ParsedStreamCandidateValidation(
        ok=False,
        reason="season_request_incomplete_episode_candidate",
    )


def test_validate_parsed_stream_candidate_accepts_complete_season_candidate_for_season_request() -> (
    None
):
    item = _build_item(
        external_ref="tvdb:456",
        attributes={"item_type": "season", "tvdb_id": "456", "season_number": 1},
    )
    candidate = parse_stream_candidate_title("Example.Show.Season.01.Complete.1080p.WEB-DL")

    validation = validate_parsed_stream_candidate(item, candidate)

    assert validation.ok is True
    assert validation.reason is None


def test_validate_parsed_stream_candidate_accepts_matching_episode_range_for_episode_request() -> (
    None
):
    item = _build_item(
        external_ref="tvdb:456",
        attributes={
            "item_type": "episode",
            "tvdb_id": "456",
            "season_number": 2,
            "episode_number": 3,
        },
    )
    candidate = parse_stream_candidate_title("Example.Show.S02E01-E03.1080p.WEB-DL")

    validation = validate_parsed_stream_candidate(item, candidate)

    assert validation.ok is True
    assert validation.reason is None


def test_attach_parse_validation_persists_rejection_reason() -> None:
    candidate = parse_stream_candidate_title("Example.Show.S01E02.1080p.WEB-DL")
    payload = attach_parse_validation(
        candidate.parsed_title,
        ParsedStreamCandidateValidation(
            ok=False,
            reason="season_request_incomplete_episode_candidate",
        ),
    )

    assert parse_stage_rejection_reason(payload) == "season_request_incomplete_episode_candidate"
