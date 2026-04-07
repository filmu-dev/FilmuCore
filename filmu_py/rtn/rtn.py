"""Facade class for the standalone RTN compatibility package."""

from __future__ import annotations

from collections.abc import Sequence

from .defaults import RankingModel, default_ranking_model
from .exceptions import (
    DisabledRankingProfileError,
    FetchChecksFailedError,
    RankUnderThresholdError,
    TitleSimilarityError,
)
from .parser import parse_torrent_name
from .ranker import check_fetch, rank, sort_torrents, title_similarity
from .schemas import ParsedData, RankedTorrent, RankingProfile


class RTN:
    """Thin facade that mirrors the audited riven-ts RTN entry class behavior."""

    def __init__(self, profile: RankingProfile, ranking_model: RankingModel | None = None) -> None:
        self.profile = profile
        self.ranking_model = ranking_model or default_ranking_model()

    def rank_torrent(
        self,
        torrent: str | ParsedData,
        *,
        correct_title: str,
        aliases: Sequence[str] | None = None,
    ) -> RankedTorrent:
        if not self.profile.enabled:
            raise DisabledRankingProfileError("ranking profile is disabled")

        parsed = torrent if isinstance(torrent, ParsedData) else parse_torrent_name(torrent)
        lev_ratio = title_similarity(parsed, correct_title, aliases)
        if (
            self.profile.options.remove_all_trash
            and lev_ratio < self.profile.options.title_similarity
        ):
            raise TitleSimilarityError("title similarity below threshold")

        rank_score, score_parts = rank(parsed, self.profile, self.ranking_model)
        fetch, failed_checks = check_fetch(parsed, self.profile)
        if self.profile.options.remove_all_trash and not fetch:
            raise FetchChecksFailedError(", ".join(failed_checks) or "fetch checks failed")
        if (
            self.profile.options.remove_all_trash
            and rank_score < self.profile.options.remove_ranks_under
        ):
            raise RankUnderThresholdError("rank below configured threshold")

        return RankedTorrent(
            data=parsed,
            rank=rank_score,
            lev_ratio=lev_ratio,
            fetch=fetch,
            failed_checks=failed_checks,
            score_parts=score_parts,
        )

    def sort_torrents(
        self, results: Sequence[RankedTorrent], *, bucket_limit: int | None = None
    ) -> list[RankedTorrent]:
        if not self.profile.enabled:
            raise DisabledRankingProfileError("ranking profile is disabled")
        return sort_torrents(results, bucket_limit=bucket_limit)
