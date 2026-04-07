from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

from filmu_py.core.event_bus import EventBus
from filmu_py.db.models import MediaItemORM, StreamORM
from filmu_py.services.media import (
    MediaService,
    RankedStreamCandidateRecord,
    RankingModel,
    RankingRule,
    SelectedStreamCandidateRecord,
    rank_persisted_streams_for_item,
    select_stream_candidate,
)


def _build_item(*, item_id: str = "item-1", title: str = "Example Movie") -> MediaItemORM:
    item = MediaItemORM(
        id=item_id,
        external_ref=f"tmdb:{item_id}",
        title=title,
        state="requested",
        attributes={"item_type": "movie", "tmdb_id": item_id},
    )
    return item


def _build_stream(
    *,
    stream_id: str,
    item: MediaItemORM,
    parsed_title: str,
    raw_title: str,
    resolution: str | None,
    parsed_payload: dict[str, object] | None = None,
    created_at: datetime | None = None,
    selected: bool = False,
) -> StreamORM:
    stream = StreamORM(
        id=stream_id,
        media_item=item,
        infohash=f"hash-{stream_id}",
        raw_title=raw_title,
        parsed_title={"title": parsed_title, **(parsed_payload or {})},
        rank=0,
        lev_ratio=None,
        resolution=resolution,
        selected=selected,
        created_at=created_at or datetime.now(UTC),
        updated_at=created_at or datetime.now(UTC),
    )
    return stream


def _build_ranked_record(
    *,
    item: MediaItemORM,
    stream: StreamORM,
    rank_score: int,
    lev_ratio: float,
    passed: bool,
    rejection_reason: str | None = None,
) -> RankedStreamCandidateRecord:
    return RankedStreamCandidateRecord(
        item_id=item.id,
        stream_id=stream.id,
        rank_score=rank_score,
        lev_ratio=lev_ratio,
        fetch=passed,
        passed=passed,
        rejection_reason=rejection_reason,
        stream=stream,
    )


def _assert_selected_stream(
    selected: SelectedStreamCandidateRecord | None,
    expected: StreamORM,
) -> None:
    assert selected is not None
    assert selected.id == expected.id
    assert selected.infohash == expected.infohash
    assert selected.raw_title == expected.raw_title
    assert selected.resolution == expected.resolution


def test_rank_persisted_streams_rejects_similarity_below_threshold() -> None:
    item = _build_item(title="Example Movie")
    stream = _build_stream(
        stream_id="stream-low-similarity",
        item=item,
        parsed_title="Completely Different Show",
        raw_title="Completely.Different.Show.S01E01.1080p.WEB-DL",
        resolution="1080p",
    )

    ranked = rank_persisted_streams_for_item(item, [stream], similarity_threshold=0.85)

    assert ranked == [
        RankedStreamCandidateRecord(
            item_id=item.id,
            stream_id="stream-low-similarity",
            rank_score=0,
            lev_ratio=stream.lev_ratio or 0.0,
            fetch=False,
            passed=False,
            rejection_reason="similarity_below_threshold",
        )
    ]
    assert stream.rank == 0
    assert stream.lev_ratio is not None
    assert stream.lev_ratio < 0.85


def test_rank_persisted_streams_orders_by_resolution_tier() -> None:
    item = _build_item(title="Example Movie")
    high = _build_stream(
        stream_id="stream-2160",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.2160p.WEB-DL",
        resolution="2160p",
    )
    medium = _build_stream(
        stream_id="stream-1080",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL",
        resolution="1080p",
    )
    lower = _build_stream(
        stream_id="stream-720",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.720p.WEB-DL",
        resolution="720p",
    )

    ranked = rank_persisted_streams_for_item(item, [lower, medium, high])

    assert [record.stream_id for record in ranked] == ["stream-2160", "stream-1080", "stream-720"]
    assert [record.rank_score for record in ranked] == [500, 300, 100]
    assert high.rank == 500
    assert medium.rank == 300
    assert lower.rank == 100


def test_rank_persisted_streams_uses_lev_ratio_as_tiebreaker_with_equal_resolution() -> None:
    item = _build_item(title="Example Movie")
    exact = _build_stream(
        stream_id="stream-exact",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL",
        resolution="1080p",
    )
    near = _build_stream(
        stream_id="stream-near",
        item=item,
        parsed_title="Example Movi",
        raw_title="Example.Movi.1080p.WEB-DL",
        resolution="1080p",
    )

    ranked = rank_persisted_streams_for_item(item, [near, exact])

    assert [record.stream_id for record in ranked] == ["stream-exact", "stream-near"]
    assert ranked[0].lev_ratio > ranked[1].lev_ratio
    assert exact.rank == near.rank == 300


def test_rank_persisted_streams_uses_alias_similarity_when_available() -> None:
    item = _build_item(title="Cidade de Deus")
    item.attributes["aliases"] = ["City of God"]
    stream = _build_stream(
        stream_id="stream-alias-match",
        item=item,
        parsed_title="City of God",
        raw_title="City.of.God.1080p.WEB-DL",
        resolution="1080p",
    )

    ranked = rank_persisted_streams_for_item(item, [stream], similarity_threshold=0.85)

    assert len(ranked) == 1
    assert ranked[0].passed is True
    assert ranked[0].fetch is True
    assert ranked[0].lev_ratio == 1.0
    assert stream.lev_ratio == 1.0


def test_rank_persisted_streams_applies_codec_penalty() -> None:
    item = _build_item(title="Example Movie")
    hevc = _build_stream(
        stream_id="stream-hevc",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL.H265",
        resolution="1080p",
        parsed_payload={"source": "Web", "video_codec": "H.265"},
    )
    xvid = _build_stream(
        stream_id="stream-xvid",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEBRip.XviD",
        resolution="1080p",
        parsed_payload={"source": "Web", "other": ["Rip"], "video_codec": "XviD"},
    )

    ranked = rank_persisted_streams_for_item(item, [xvid, hevc])

    assert [record.stream_id for record in ranked] == ["stream-hevc", "stream-xvid"]
    assert hevc.rank > xvid.rank
    assert ranked[1].passed is False
    assert ranked[1].fetch is False
    assert ranked[1].rejection_reason == "codec_fetch_disabled:xvid"


def test_rank_persisted_streams_applies_hdr_boost() -> None:
    item = _build_item(title="Example Movie")
    hdr = _build_stream(
        stream_id="stream-hdr",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL.HDR",
        resolution="1080p",
        parsed_payload={"source": "Web", "other": ["HDR"]},
    )
    sdr = _build_stream(
        stream_id="stream-sdr",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL.SDR",
        resolution="1080p",
        parsed_payload={"source": "Web", "other": ["SDR"]},
    )

    ranked = rank_persisted_streams_for_item(item, [sdr, hdr])

    assert [record.stream_id for record in ranked] == ["stream-hdr", "stream-sdr"]
    assert hdr.rank > sdr.rank


def test_ranking_model_override_can_change_the_winner() -> None:
    item = _build_item(title="Example Movie")
    bluray = _build_stream(
        stream_id="stream-bluray",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.BluRay.H265",
        resolution="1080p",
        parsed_payload={"source": "Blu-ray", "video_codec": "H.265"},
    )
    webdl = _build_stream(
        stream_id="stream-webdl",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL.H265",
        resolution="1080p",
        parsed_payload={"source": "Web", "video_codec": "H.265"},
    )

    default_ranked = rank_persisted_streams_for_item(item, [bluray, webdl])
    overridden_ranked = rank_persisted_streams_for_item(
        item,
        [bluray, webdl],
        ranking_model=RankingModel(
            quality_source_scores={
                # Highest quality physical sources
                "bluray_remux": RankingRule(rank=1200),
                "bluray": RankingRule(rank=1100),
                "bdrip": RankingRule(rank=1000),
                # High quality streaming sources
                "webdl": RankingRule(rank=900),
                "web-dl": RankingRule(rank=900),
                # Re-encoded web sources
                "webrip": RankingRule(rank=750),
                # Older physical releases
                "dvdrip": RankingRule(rank=500),
                "hdrip": RankingRule(rank=400),
                # Broadcast sources
                "hdtv": RankingRule(rank=200),
                # Low quality or temporary cinema captures (disabled)
                "cam": RankingRule(rank=-10000, fetch=False),
                "telesync": RankingRule(rank=-10000, fetch=False),
                "telecine": RankingRule(rank=-10000, fetch=False),
                "screener": RankingRule(rank=-10000, fetch=False),
                "ts": RankingRule(rank=-10000, fetch=False),
                "hdcam": RankingRule(rank=-10000, fetch=False),
            }
        ),
    )

    overridden_ranked = rank_persisted_streams_for_item(
        item,
        [bluray, webdl],
        ranking_model=RankingModel(
            quality_source_scores={
                "bluray_remux": RankingRule(rank=1200),
                "bluray": RankingRule(rank=0),
                "bdrip": RankingRule(rank=1000),
                "webdl": RankingRule(rank=1500),
                "web-dl": RankingRule(rank=1500),
                "webrip": RankingRule(rank=750),
                "dvdrip": RankingRule(rank=500),
                "hdrip": RankingRule(rank=400),
                "hdtv": RankingRule(rank=200),
                "cam": RankingRule(rank=-10000, fetch=False),
                "telesync": RankingRule(rank=-10000, fetch=False),
                "telecine": RankingRule(rank=-10000, fetch=False),
                "screener": RankingRule(rank=-10000, fetch=False),
                "ts": RankingRule(rank=-10000, fetch=False),
                "hdcam": RankingRule(rank=-10000, fetch=False),
            }
        ),
    )

    assert default_ranked[0].stream_id == "stream-bluray"
    assert overridden_ranked[0].stream_id == "stream-webdl"


def test_rank_persisted_streams_rejects_trash_candidate_after_scoring() -> None:
    item = _build_item(title="Example Movie")
    cam = _build_stream(
        stream_id="stream-cam",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.2026.CAM.XviD.MP3",
        resolution="1080p",
        parsed_payload={"other": ["CAM"], "video_codec": "XviD", "audio_codec": "MP3"},
    )

    ranked = rank_persisted_streams_for_item(item, [cam])

    assert ranked[0].passed is False
    assert ranked[0].fetch is False
    assert ranked[0].rejection_reason == "quality_source_fetch_disabled:cam"


def test_rank_persisted_streams_rejects_when_score_below_floor() -> None:
    item = _build_item(title="Example Movie")
    low_score = _build_stream(
        stream_id="stream-low-score",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.2026.WEBRip.H265.MP3",
        resolution="360p",
        parsed_payload={
            "source": "Web",
            "other": ["Rip"],
            "video_codec": "H.265",
            "audio_codec": "MP3",
        },
    )

    ranked = rank_persisted_streams_for_item(
        item,
        [low_score],
        ranking_model=RankingModel(remove_ranks_under=2000),
    )

    assert ranked[0].passed is False
    assert ranked[0].fetch is False
    assert ranked[0].rejection_reason == "rank_below_threshold"


def test_rank_persisted_streams_require_override_forces_fetch_true() -> None:
    item = _build_item(title="Example Movie")
    cam = _build_stream(
        stream_id="stream-require-cam",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.2026.CAM.XviD.MP3.REPACK",
        resolution="1080p",
        parsed_payload={"other": ["CAM", "REPACK"], "video_codec": "XviD", "audio_codec": "MP3"},
    )

    ranked = rank_persisted_streams_for_item(
        item,
        [cam],
        ranking_model=RankingModel(require=[r"REPACK"]),
    )

    assert ranked[0].passed is True
    assert ranked[0].fetch is True
    assert ranked[0].rejection_reason is None


def test_rank_persisted_streams_respects_fetch_false_attribute_rule() -> None:
    item = _build_item(title="Example Movie")
    stream = _build_stream(
        stream_id="stream-fetch-disabled-codec",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.2026.1080p.WEB-DL.H265",
        resolution="1080p",
        parsed_payload={"source": "Web", "video_codec": "H.265"},
    )

    ranked = rank_persisted_streams_for_item(
        item,
        [stream],
        ranking_model=RankingModel(
            codec_scores={
                "hevc": RankingRule(rank=150),
                "h265": RankingRule(rank=150, fetch=False),
                "avc": RankingRule(rank=50),
                "x264": RankingRule(rank=50),
                "av1": RankingRule(rank=500),
                "xvid": RankingRule(rank=-500, fetch=False),
            }
        ),
    )

    assert ranked[0].passed is False
    assert ranked[0].fetch is False
    assert ranked[0].rejection_reason == "codec_fetch_disabled:h265"


def test_select_stream_candidate_prefers_highest_rank_score() -> None:
    item = _build_item(title="Example Movie")
    lower = _build_stream(
        stream_id="stream-lower-score",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.720p.WEB-DL",
        resolution="720p",
    )
    higher = _build_stream(
        stream_id="stream-higher-score",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL",
        resolution="1080p",
    )

    selected = select_stream_candidate(
        [
            _build_ranked_record(
                item=item, stream=lower, rank_score=100, lev_ratio=1.0, passed=True
            ),
            _build_ranked_record(
                item=item, stream=higher, rank_score=300, lev_ratio=0.9, passed=True
            ),
        ]
    )

    _assert_selected_stream(selected, higher)


def test_select_stream_candidate_uses_lev_ratio_tiebreaker() -> None:
    item = _build_item(title="Example Movie")
    closer = _build_stream(
        stream_id="stream-closer",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL",
        resolution="1080p",
    )
    farther = _build_stream(
        stream_id="stream-farther",
        item=item,
        parsed_title="Example Movi",
        raw_title="Example.Movi.1080p.WEB-DL",
        resolution="1080p",
    )

    selected = select_stream_candidate(
        [
            _build_ranked_record(
                item=item, stream=farther, rank_score=300, lev_ratio=0.95, passed=True
            ),
            _build_ranked_record(
                item=item, stream=closer, rank_score=300, lev_ratio=1.0, passed=True
            ),
        ]
    )

    _assert_selected_stream(selected, closer)


def test_select_stream_candidate_uses_lower_stream_id_as_stable_tiebreaker() -> None:
    item = _build_item(title="Example Movie")
    lower_id = _build_stream(
        stream_id="stream-001",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL.A",
        resolution="1080p",
    )
    higher_id = _build_stream(
        stream_id="stream-002",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL.B",
        resolution="1080p",
    )

    selected = select_stream_candidate(
        [
            _build_ranked_record(
                item=item, stream=higher_id, rank_score=300, lev_ratio=1.0, passed=True
            ),
            _build_ranked_record(
                item=item, stream=lower_id, rank_score=300, lev_ratio=1.0, passed=True
            ),
        ]
    )

    _assert_selected_stream(selected, lower_id)


def test_select_stream_candidate_prefers_lower_part_when_rank_and_similarity_tie() -> None:
    item = _build_item(title="Frieren: Beyond Journey's End")
    part_two = _build_stream(
        stream_id="stream-part-2",
        item=item,
        parsed_title="Frieren: Beyond Journey's End",
        raw_title="[ToonsHub] Frieren S01 E15-E28 [Batch - Part 2]",
        resolution="2160p",
        parsed_payload={"part": 2, "season": 1},
    )
    part_one = _build_stream(
        stream_id="stream-part-1",
        item=item,
        parsed_title="Frieren: Beyond Journey's End",
        raw_title="[ToonsHub] Frieren S01 E01-E14 [Batch - Part 1]",
        resolution="2160p",
        parsed_payload={"part": 1, "season": 1},
    )

    selected = select_stream_candidate(
        [
            _build_ranked_record(
                item=item,
                stream=part_two,
                rank_score=11000,
                lev_ratio=1.0,
                passed=True,
            ),
            _build_ranked_record(
                item=item,
                stream=part_one,
                rank_score=11000,
                lev_ratio=1.0,
                passed=True,
            ),
        ]
    )

    _assert_selected_stream(selected, part_one)


def test_select_stream_candidate_returns_none_when_no_candidates_pass() -> None:
    item = _build_item(title="Example Movie")
    rejected = _build_stream(
        stream_id="stream-rejected",
        item=item,
        parsed_title="Wrong Title",
        raw_title="Wrong.Title.1080p.WEB-DL",
        resolution="1080p",
    )

    selected = select_stream_candidate(
        [
            _build_ranked_record(
                item=item,
                stream=rejected,
                rank_score=0,
                lev_ratio=0.2,
                passed=False,
                rejection_reason="similarity_below_threshold",
            )
        ]
    )

    assert selected is None


class _DummyExecuteResult:
    def __init__(self, item: MediaItemORM | None) -> None:
        self._item = item

    def scalar_one_or_none(self) -> MediaItemORM | None:
        return self._item


class _DummySession:
    def __init__(self, item: MediaItemORM | None) -> None:
        self.item = item
        self.committed = False
        streams = item.streams if item is not None else []
        self._streams_by_id = {stream.id: stream for stream in streams}

    async def execute(self, _statement: object) -> _DummyExecuteResult:
        return _DummyExecuteResult(self.item)

    async def get(self, model: type[StreamORM], identity: str) -> StreamORM | None:
        if model is not StreamORM:
            return None
        return self._streams_by_id.get(identity)

    async def commit(self) -> None:
        self.committed = True


@dataclass
class _DummyDatabaseRuntime:
    item: MediaItemORM | None
    last_session: _DummySession | None = None

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_DummySession]:
        session = _DummySession(self.item)
        self.last_session = session
        yield session


def _require_session(runtime: _DummyDatabaseRuntime) -> _DummySession:
    session = runtime.last_session
    assert session is not None
    return session


async def _load_stream_from_new_session(
    runtime: _DummyDatabaseRuntime, stream_id: str
) -> StreamORM | None:
    async with runtime.session() as session:
        return await session.get(StreamORM, stream_id)


def test_rank_stream_candidates_service_writes_rank_back_and_commits() -> None:
    item = _build_item(item_id="item-service-rank", title="Example Movie")
    stream = _build_stream(
        stream_id="stream-service",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL",
        resolution="1080p",
    )
    runtime = _DummyDatabaseRuntime(item=item)
    service = MediaService(db=runtime, event_bus=EventBus())

    ranked = asyncio.run(service.rank_stream_candidates(media_item_id=item.id))

    assert ranked[0].stream_id == stream.id
    assert ranked[0].rank_score == 300
    assert ranked[0].passed is True
    assert stream.rank == 300
    assert stream.lev_ratio is not None
    assert _require_session(runtime).committed is True


def test_select_stream_candidate_service_clears_previous_selection_when_new_selection_made() -> (
    None
):
    item = _build_item(item_id="item-select-clear", title="Example Movie")
    previous = _build_stream(
        stream_id="stream-previous",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.720p.WEB-DL",
        resolution="720p",
        selected=True,
    )
    winner = _build_stream(
        stream_id="stream-winner",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL",
        resolution="1080p",
    )
    item.streams = [previous, winner]
    runtime = _DummyDatabaseRuntime(item=item)
    service = MediaService(db=runtime, event_bus=EventBus())

    selected = asyncio.run(service.select_stream_candidate(media_item_id=item.id))

    _assert_selected_stream(selected, winner)
    assert previous.selected is False
    assert winner.selected is True
    assert _require_session(runtime).committed is True


def test_select_stream_candidate_service_persists_selected_flag_to_loaded_rows() -> None:
    item = _build_item(item_id="item-select-persist", title="Example Movie")
    losing = _build_stream(
        stream_id="stream-losing",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.720p.WEB-DL",
        resolution="720p",
        selected=True,
    )
    winning = _build_stream(
        stream_id="stream-winning",
        item=item,
        parsed_title="Example Movie",
        raw_title="Example.Movie.1080p.WEB-DL",
        resolution="1080p",
    )
    item.streams = [losing, winning]
    runtime = _DummyDatabaseRuntime(item=item)
    service = MediaService(db=runtime, event_bus=EventBus())

    selected = asyncio.run(service.select_stream_candidate(media_item_id=item.id))
    persisted_winner = asyncio.run(_load_stream_from_new_session(runtime, winning.id))
    persisted_loser = asyncio.run(_load_stream_from_new_session(runtime, losing.id))

    _assert_selected_stream(selected, winning)
    assert persisted_winner is not None
    assert persisted_winner.selected is True
    assert persisted_loser is not None
    assert persisted_loser.selected is False


def test_select_stream_candidate_service_clears_previous_selection_when_no_candidates_pass() -> (
    None
):
    item = _build_item(item_id="item-select-none", title="Example Movie")
    previously_selected = _build_stream(
        stream_id="stream-previously-selected",
        item=item,
        parsed_title="Completely Different Show",
        raw_title="Completely.Different.Show.S01E01.1080p.WEB-DL",
        resolution="1080p",
        selected=True,
    )
    item.streams = [previously_selected]
    runtime = _DummyDatabaseRuntime(item=item)
    service = MediaService(db=runtime, event_bus=EventBus())

    selected = asyncio.run(service.select_stream_candidate(media_item_id=item.id))
    selection_session = _require_session(runtime)
    persisted_previous = asyncio.run(_load_stream_from_new_session(runtime, previously_selected.id))

    assert selected is None
    assert persisted_previous is not None
    assert persisted_previous.selected is False
    assert selection_session.committed is True
