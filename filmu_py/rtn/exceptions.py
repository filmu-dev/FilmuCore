"""RTN exception hierarchy mirroring the original riven-ts pipeline semantics."""

from __future__ import annotations


class GarbageTorrentError(Exception):
    """Base error for torrents rejected by RTN validation or ranking policy."""


class DisabledRankingProfileError(GarbageTorrentError):
    """Raised when RTN is asked to rank while the profile is disabled."""


class TitleSimilarityError(GarbageTorrentError):
    """Raised when a title falls below the configured similarity threshold."""


class FetchChecksFailedError(GarbageTorrentError):
    """Raised when one or more fetch checks reject a torrent."""


class RankUnderThresholdError(GarbageTorrentError):
    """Raised when the final score is below the configured minimum threshold."""
