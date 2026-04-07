"""Standalone RTN compatibility package for ranking torrent names."""

from .defaults import RankingModel, default_ranking_model
from .exceptions import (
    DisabledRankingProfileError,
    FetchChecksFailedError,
    GarbageTorrentError,
    RankUnderThresholdError,
    TitleSimilarityError,
)
from .parser import parse_torrent_name
from .rtn import RTN
from .schemas import ParsedData, RankedTorrent, RankingProfile

__all__ = [
    "RTN",
    "DisabledRankingProfileError",
    "FetchChecksFailedError",
    "GarbageTorrentError",
    "ParsedData",
    "RankUnderThresholdError",
    "RankedTorrent",
    "RankingModel",
    "RankingProfile",
    "TitleSimilarityError",
    "default_ranking_model",
    "parse_torrent_name",
]
