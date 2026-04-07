"""Pydantic schemas for RTN settings, parsed data, and ranked results."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator


class CustomRankEntry(BaseModel):
    """One user-configurable fetch/rank override entry from the settings surface."""

    model_config = ConfigDict(extra="forbid")

    fetch: bool
    use_custom_rank: bool
    rank: int


class _RankCategory(RootModel[dict[str, CustomRankEntry]]):
    """Base model for one category of custom rank entries with exact key enforcement."""

    expected_keys: ClassVar[frozenset[str]] = frozenset()

    @model_validator(mode="after")
    def validate_expected_keys(self) -> _RankCategory:
        keys = frozenset(self.root.keys())
        if keys != self.expected_keys:
            missing = sorted(self.expected_keys - keys)
            extra = sorted(keys - self.expected_keys)
            fragments: list[str] = []
            if missing:
                fragments.append(f"missing keys: {', '.join(missing)}")
            if extra:
                fragments.append(f"unexpected keys: {', '.join(extra)}")
            raise ValueError("; ".join(fragments))
        return self

    def get(self, key: str) -> CustomRankEntry:
        return self.root[key]


class QualityCustomRanks(_RankCategory):
    expected_keys = frozenset(
        {
            "av1",
            "avc",
            "bluray",
            "dvd",
            "hdtv",
            "hevc",
            "mpeg",
            "remux",
            "vhs",
            "web",
            "webdl",
            "webmux",
            "xvid",
        }
    )


class RipsCustomRanks(_RankCategory):
    expected_keys = frozenset(
        {
            "bdrip",
            "brrip",
            "dvdrip",
            "hdrip",
            "ppvrip",
            "satrip",
            "tvrip",
            "uhdrip",
            "vhsrip",
            "webdlrip",
            "webrip",
        }
    )


class HdrCustomRanks(_RankCategory):
    expected_keys = frozenset({"bit10", "dolby_vision", "hdr", "hdr10plus", "sdr"})


class AudioCustomRanks(_RankCategory):
    expected_keys = frozenset(
        {
            "aac",
            "atmos",
            "dolby_digital",
            "dolby_digital_plus",
            "dts_lossless",
            "dts_lossy",
            "flac",
            "mono",
            "mp3",
            "stereo",
            "surround",
            "truehd",
        }
    )


class ExtrasCustomRanks(_RankCategory):
    expected_keys = frozenset(
        {
            "three_d",
            "converted",
            "documentary",
            "dubbed",
            "edition",
            "hardcoded",
            "network",
            "proper",
            "repack",
            "retail",
            "scene",
            "site",
            "subbed",
            "uncensored",
            "upscaled",
        }
    )


class TrashCustomRanks(_RankCategory):
    expected_keys = frozenset(
        {"cam", "clean_audio", "pdtv", "r5", "screener", "size", "telecine", "telesync"}
    )


class CustomRanks(BaseModel):
    """Top-level custom-rank container matching the exact production settings keys."""

    model_config = ConfigDict(extra="forbid")

    quality: QualityCustomRanks
    rips: RipsCustomRanks
    hdr: HdrCustomRanks
    audio: AudioCustomRanks
    extras: ExtrasCustomRanks
    trash: TrashCustomRanks


class Languages(BaseModel):
    """Language inclusion/exclusion preferences from the RTN settings surface."""

    model_config = ConfigDict(extra="forbid")

    required: list[str] = Field(default_factory=list)
    allowed: list[str] = Field(default_factory=list)
    exclude: list[str] = Field(default_factory=list)
    preferred: list[str] = Field(default_factory=list)


class Options(BaseModel):
    """Ranking behavioral toggles from the production settings surface."""

    model_config = ConfigDict(extra="forbid")

    title_similarity: float = 0.85
    remove_all_trash: bool = True
    remove_ranks_under: int = -10000
    remove_unknown_languages: bool = False
    allow_english_in_languages: bool = True
    # TODO: `enable_fetch_speed_mode` exists in the production settings shape but its behavior is
    # not defined in the audited RTN report yet, so the standalone package stores it without using it.
    enable_fetch_speed_mode: bool = False
    remove_adult_content: bool = True


class ResolutionSettings(BaseModel):
    """Enabled resolution buckets from the RTN settings surface."""

    model_config = ConfigDict(extra="forbid")

    r2160p: bool
    r1080p: bool
    r720p: bool
    r480p: bool
    r360p: bool
    unknown: bool


class RankingProfile(BaseModel):
    """Top-level RTN ranking profile matching the exact `settings.json` ranking block."""

    model_config = ConfigDict(extra="forbid")

    name: str
    enabled: bool
    require: list[str] = Field(default_factory=list)
    exclude: list[str] = Field(default_factory=list)
    preferred: list[str] = Field(default_factory=list)
    resolutions: ResolutionSettings
    options: Options
    languages: Languages
    custom_ranks: CustomRanks

    @classmethod
    def from_settings_dict(cls, data: dict[str, object]) -> RankingProfile:
        """Build one validated ranking profile directly from the raw `settings.json` ranking block."""

        return cls.model_validate(data)


class ParsedData(BaseModel):
    """Parsed torrent data passed between the parser and ranking stages."""

    model_config = ConfigDict(extra="forbid")

    raw_title: str
    parsed_title: dict[str, object]
    resolution: str | None = None


@dataclass(frozen=True)
class RankedTorrent:
    """Final RTN ranking result for one parsed torrent candidate."""

    data: ParsedData
    rank: int
    lev_ratio: float
    fetch: bool
    failed_checks: tuple[str, ...]
    score_parts: dict[str, int]
