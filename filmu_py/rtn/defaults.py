"""Default RTN ranking model values derived from the audited TypeScript behavior."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RankingModel:
    """Default score maps used unless the user explicitly opts into a custom rank."""

    quality: dict[str, int] = field(default_factory=dict)
    rips: dict[str, int] = field(default_factory=dict)
    hdr: dict[str, int] = field(default_factory=dict)
    audio: dict[str, int] = field(default_factory=dict)
    extras: dict[str, int] = field(default_factory=dict)
    trash: dict[str, int] = field(default_factory=dict)


def default_ranking_model() -> RankingModel:
    """Return the first audited RTN score model used by the standalone compatibility package."""

    return RankingModel(
        quality={
            "av1": 500,
            "avc": 500,
            "bluray": 100,
            "dvd": -5000,
            "hdtv": -5000,
            "hevc": 500,
            "mpeg": -1000,
            "remux": 10000,
            "vhs": -10000,
            "web": 0,
            "webdl": 200,
            "webmux": -10000,
            "xvid": -10000,
        },
        rips={
            "bdrip": -5000,
            "brrip": 0,
            "dvdrip": -5000,
            "hdrip": 0,
            "ppvrip": 0,
            "satrip": 0,
            "tvrip": 0,
            "uhdrip": 0,
            "vhsrip": 0,
            "webdlrip": 0,
            "webrip": -1000,
        },
        hdr={
            "bit10": 0,
            "dolby_vision": 3000,
            "hdr": 2000,
            "hdr10plus": 2100,
            "sdr": 0,
        },
        audio={
            "aac": 0,
            "atmos": 1000,
            "dolby_digital": 0,
            "dolby_digital_plus": 0,
            "dts_lossless": 2000,
            "dts_lossy": 0,
            "flac": 0,
            "mono": 0,
            "mp3": 0,
            "stereo": 0,
            "surround": 0,
            "truehd": 2000,
        },
        extras={
            "three_d": 0,
            "converted": -1000,
            "documentary": 0,
            "dubbed": -1000,
            "edition": 0,
            "hardcoded": 0,
            "network": 0,
            "proper": 0,
            "repack": 0,
            "retail": 0,
            "scene": 0,
            "site": -10000,
            "subbed": 0,
            "uncensored": 0,
            "upscaled": 0,
        },
        trash={
            "cam": -10000,
            "clean_audio": -10000,
            "pdtv": -10000,
            "r5": -10000,
            "screener": -10000,
            "size": -10000,
            "telecine": -10000,
            "telesync": -10000,
        },
    )
