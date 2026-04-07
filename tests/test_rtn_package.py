from __future__ import annotations

import pytest

from filmu_py.rtn import RTN, DisabledRankingProfileError, default_ranking_model, parse_torrent_name
from filmu_py.rtn.ranker import check_fetch, resolve_rank
from filmu_py.rtn.schemas import RankedTorrent, RankingProfile


def _entry(*, fetch: bool) -> dict[str, object]:
    return {"fetch": fetch, "use_custom_rank": False, "rank": 0}


def _build_ranking_block() -> dict[str, object]:
    return {
        "name": "example",
        "enabled": True,
        "require": [],
        "exclude": ["rus", "ita", "pol"],
        "preferred": ["eng"],
        "resolutions": {
            "r2160p": True,
            "r1080p": True,
            "r720p": True,
            "r480p": False,
            "r360p": False,
            "unknown": True,
        },
        "options": {
            "title_similarity": 0.85,
            "remove_all_trash": True,
            "remove_ranks_under": -8500,
            "remove_unknown_languages": False,
            "allow_english_in_languages": True,
            "enable_fetch_speed_mode": True,
            "remove_adult_content": True,
        },
        "languages": {
            "required": [],
            "allowed": [],
            "exclude": ["rus", "pol", "hun", "ita", "es", "ukr"],
            "preferred": ["eng", "por"],
        },
        "custom_ranks": {
            "quality": {
                "av1": _entry(fetch=False),
                "avc": _entry(fetch=True),
                "bluray": _entry(fetch=True),
                "dvd": _entry(fetch=False),
                "hdtv": _entry(fetch=True),
                "hevc": _entry(fetch=True),
                "mpeg": _entry(fetch=False),
                "remux": _entry(fetch=False),
                "vhs": _entry(fetch=False),
                "web": _entry(fetch=True),
                "webdl": _entry(fetch=True),
                "webmux": _entry(fetch=False),
                "xvid": _entry(fetch=False),
            },
            "rips": {
                "bdrip": _entry(fetch=False),
                "brrip": _entry(fetch=True),
                "dvdrip": _entry(fetch=False),
                "hdrip": _entry(fetch=True),
                "ppvrip": _entry(fetch=False),
                "satrip": _entry(fetch=False),
                "tvrip": _entry(fetch=False),
                "uhdrip": _entry(fetch=False),
                "vhsrip": _entry(fetch=False),
                "webdlrip": _entry(fetch=False),
                "webrip": _entry(fetch=True),
            },
            "hdr": {
                "bit10": _entry(fetch=True),
                "dolby_vision": _entry(fetch=False),
                "hdr": _entry(fetch=True),
                "hdr10plus": _entry(fetch=True),
                "sdr": _entry(fetch=True),
            },
            "audio": {
                "aac": _entry(fetch=False),
                "atmos": _entry(fetch=False),
                "dolby_digital": _entry(fetch=False),
                "dolby_digital_plus": _entry(fetch=False),
                "dts_lossy": _entry(fetch=False),
                "dts_lossless": _entry(fetch=False),
                "flac": _entry(fetch=False),
                "mono": _entry(fetch=False),
                "mp3": _entry(fetch=False),
                "stereo": _entry(fetch=False),
                "surround": _entry(fetch=False),
                "truehd": _entry(fetch=False),
            },
            "extras": {
                "three_d": _entry(fetch=True),
                "converted": _entry(fetch=False),
                "documentary": _entry(fetch=False),
                "dubbed": _entry(fetch=False),
                "edition": _entry(fetch=False),
                "hardcoded": _entry(fetch=True),
                "network": _entry(fetch=True),
                "proper": _entry(fetch=True),
                "repack": _entry(fetch=True),
                "retail": _entry(fetch=True),
                "site": _entry(fetch=False),
                "subbed": _entry(fetch=True),
                "upscaled": _entry(fetch=False),
                "scene": _entry(fetch=True),
                "uncensored": _entry(fetch=True),
            },
            "trash": {
                "cam": _entry(fetch=False),
                "clean_audio": _entry(fetch=False),
                "pdtv": _entry(fetch=False),
                "r5": _entry(fetch=False),
                "screener": _entry(fetch=False),
                "size": _entry(fetch=False),
                "telecine": _entry(fetch=False),
                "telesync": _entry(fetch=False),
            },
        },
    }


def test_ranking_profile_round_trips_from_production_shape() -> None:
    block = _build_ranking_block()

    profile = RankingProfile.from_settings_dict(block)

    dumped = profile.model_dump(mode="python")
    assert dumped == block
    assert profile.custom_ranks.hdr.root["dolby_vision"].use_custom_rank is False
    assert profile.options.enable_fetch_speed_mode is True


def test_use_custom_rank_false_uses_default_model_value() -> None:
    block = _build_ranking_block()
    block["custom_ranks"]["hdr"]["dolby_vision"] = {
        "fetch": False,
        "use_custom_rank": False,
        "rank": 999,
    }
    profile = RankingProfile.from_settings_dict(block)

    default_model = default_ranking_model()
    entry = profile.custom_ranks.hdr.root["dolby_vision"]

    assert (
        resolve_rank(default_model.hdr["dolby_vision"], entry.use_custom_rank, entry.rank) == 3000
    )


def test_use_custom_rank_true_applies_override() -> None:
    block = _build_ranking_block()
    block["custom_ranks"]["hdr"]["dolby_vision"] = {
        "fetch": False,
        "use_custom_rank": True,
        "rank": 999,
    }
    profile = RankingProfile.from_settings_dict(block)
    entry = profile.custom_ranks.hdr.root["dolby_vision"]

    assert (
        resolve_rank(default_ranking_model().hdr["dolby_vision"], entry.use_custom_rank, entry.rank)
        == 999
    )


def test_disabled_profile_raises() -> None:
    block = _build_ranking_block()
    block["enabled"] = False
    profile = RankingProfile.from_settings_dict(block)
    rtn = RTN(profile)

    with pytest.raises(DisabledRankingProfileError):
        rtn.rank_torrent(
            "Example.Movie.2024.1080p.WEB-DL.x264-GROUP", correct_title="Example Movie"
        )


def test_full_fetch_check_pipeline_reports_expected_failures() -> None:
    block = _build_ranking_block()
    profile = RankingProfile.from_settings_dict(block)
    parsed = parse_torrent_name("Example.Movie.2024.480p.CAM.RUS.AAC.xvid-GROUP")
    parsed.parsed_title["language"] = ["rus"]

    fetch, failed_checks = check_fetch(parsed, profile)

    assert fetch is False
    assert "trash:cam" in failed_checks
    assert "excluded_language" in failed_checks
    assert "resolution:480p" in failed_checks
    assert "fetch_disabled:quality:xvid" in failed_checks


def test_title_similarity_accepts_acronym_titles_before_year_suffix() -> None:
    profile = RankingProfile.from_settings_dict(_build_ranking_block())
    rtn = RTN(profile)

    ranked = rtn.rank_torrent(
        "xXx.2002.1080p.BluRay.x264-GROUP",
        correct_title="xXx",
    )

    assert ranked.lev_ratio == 1.0
    assert ranked.fetch is True


def test_sort_with_bucket_limit() -> None:
    profile = RankingProfile.from_settings_dict(_build_ranking_block())
    rtn = RTN(profile)

    def ranked(raw_title: str, rank: int, resolution: str) -> RankedTorrent:
        parsed = parse_torrent_name(raw_title)
        parsed.resolution = resolution
        return RankedTorrent(
            data=parsed,
            rank=rank,
            lev_ratio=1.0,
            fetch=True,
            failed_checks=(),
            score_parts={},
        )

    results = [
        ranked("Example.Movie.2024.2160p.WEB-DL-G1", 4000, "2160p"),
        ranked("Example.Movie.2024.2160p.WEB-DL-G2", 3900, "2160p"),
        ranked("Example.Movie.2024.1080p.WEB-DL-G1", 3000, "1080p"),
        ranked("Example.Movie.2024.1080p.WEB-DL-G2", 2900, "1080p"),
    ]

    sorted_results = rtn.sort_torrents(results, bucket_limit=1)

    assert [item.data.raw_title for item in sorted_results] == [
        "Example.Movie.2024.2160p.WEB-DL-G1",
        "Example.Movie.2024.1080p.WEB-DL-G1",
    ]


def test_dolby_vision_snake_case_maps_correctly() -> None:
    profile = RankingProfile.from_settings_dict(_build_ranking_block())
    assert "dolby_vision" in profile.custom_ranks.hdr.root
