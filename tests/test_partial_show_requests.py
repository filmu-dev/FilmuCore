from __future__ import annotations

from datetime import UTC, datetime

from filmu_py.db.models import MediaItemORM
from filmu_py.api.playback_resolution import PlaybackAttachment
from filmu_py.db.models import PlaybackAttachmentORM, MediaEntryORM
from filmu_py.services.media import (
    _build_detail_record,
    _candidate_matches_partial_scope,
    _candidate_parsed_seasons,
    build_item_request_record,
    parse_stream_candidate_title,
    update_item_request_record,
)
from filmu_py.services.playback import PlaybackResolutionSnapshot


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


def test_item_detail_record_includes_media_entry_lifecycle_projection() -> None:
    item = MediaItemORM(
        id="item-1",
        external_ref="tmdb:123",
        title="Example Movie",
        state="completed",
        attributes={"item_type": "movie", "tmdb_id": "123"},
    )
    attachment = PlaybackAttachmentORM(
        id="attachment-1",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/direct",
        source_key="persisted",
        provider="realdebrid",
        provider_download_id="download-1",
        provider_file_id="file-1",
        provider_file_path="/downloads/Example.Movie.mkv",
        original_filename="Example.Movie.mkv",
        unrestricted_url="https://cdn.example.com/direct",
        restricted_url="https://api.example.com/restricted",
        refresh_state="ready",
        created_at=datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 20, 12, 5, tzinfo=UTC),
    )
    entry = MediaEntryORM(
        id="media-entry-1",
        item_id=item.id,
        source_attachment_id=attachment.id,
        entry_type="media",
        kind="remote-direct",
        original_filename="Example.Movie.mkv",
        download_url="https://api.example.com/restricted",
        unrestricted_url="https://cdn.example.com/direct",
        provider="realdebrid",
        provider_download_id="download-1",
        provider_file_id="file-1",
        provider_file_path="/downloads/Example.Movie.mkv",
        refresh_state="ready",
        created_at=datetime(2026, 3, 20, 12, 10, tzinfo=UTC),
        updated_at=datetime(2026, 3, 20, 12, 15, tzinfo=UTC),
    )
    entry.source_attachment = attachment
    item.playback_attachments = [attachment]
    item.media_entries = [entry]

    class PlaybackStub:
        def build_resolution_snapshot(self, media_item: MediaItemORM) -> PlaybackResolutionSnapshot:
            assert media_item is item
            resolved = PlaybackAttachment(
                kind="remote-direct",
                locator="https://cdn.example.com/direct",
                source_key="persisted",
                provider="realdebrid",
                provider_download_id="download-1",
                provider_file_id="file-1",
                provider_file_path="/downloads/Example.Movie.mkv",
                original_filename="Example.Movie.mkv",
                restricted_url="https://api.example.com/restricted",
                unrestricted_url="https://cdn.example.com/direct",
                refresh_state="ready",
            )
            return PlaybackResolutionSnapshot(
                direct=resolved,
                hls=None,
                direct_ready=True,
                hls_ready=False,
                missing_local_file=False,
            )

    detail = _build_detail_record(item, extended=True, playback_service=PlaybackStub())

    assert detail.media_entries is not None
    lifecycle = detail.media_entries[0].lifecycle
    assert detail.media_entries[0].source_attachment_id == "attachment-1"
    assert lifecycle is not None
    assert lifecycle.owner_kind == "media-entry"
    assert lifecycle.owner_id == "media-entry-1"
    assert lifecycle.active_roles == ("direct",)
    assert lifecycle.source_key == "persisted"
    assert lifecycle.provider_family == "debrid"
    assert lifecycle.locator_source == "unrestricted-url"
    assert lifecycle.match_basis == "source-attachment-id"
    assert lifecycle.restricted_fallback is False
    assert lifecycle.effective_refresh_state == "ready"
    assert lifecycle.ready_for_direct is True
    assert lifecycle.ready_for_hls is True
    assert lifecycle.ready_for_playback is True


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
