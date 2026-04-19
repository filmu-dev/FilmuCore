"""Search-context and partial-scope helpers extracted from the worker task module."""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from filmu_py.db.models import StreamORM
from filmu_py.plugins import ExternalIdentifiers, ScraperSearchInput
from filmu_py.plugins.interfaces import ScraperResult as PluginScraperResult
from filmu_py.rtn import RankedTorrent
from filmu_py.services.media import MediaItemRecord, ScrapeCandidateRecord
from filmu_py.state.item import ItemState

_PARTIAL_SCOPE_SEASON_COVERAGE_BONUS = 10_000
_PARTIAL_SCOPE_SEASON_PACK_BONUS = 20_000
_PARTIAL_SCOPE_MULTI_EPISODE_BONUS = 2_000


def extract_int_value(attributes: dict[str, object], *keys: str) -> int | None:
    for key in keys:
        value = attributes.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())
    return None


def resolve_item_type(item: MediaItemRecord) -> str:
    raw_type = item.attributes.get("item_type") or item.attributes.get("media_type")
    if isinstance(raw_type, str):
        normalized = raw_type.strip().casefold()
        item_type_aliases = {
            "movie": "movie",
            "show": "show",
            "season": "season",
            "episode": "episode",
            "tv": "show",
            "series": "show",
        }
        mapped = item_type_aliases.get(normalized)
        if mapped is not None:
            return mapped
    if item.external_ref.startswith("tmdb:"):
        return "movie"
    return "show"


def extract_string_attribute(attributes: dict[str, object], key: str) -> str | None:
    value = attributes.get(key)
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return None


def needs_failed_metadata_repair(item: MediaItemRecord) -> bool:
    """Return whether a failed item still has identifier repair work worth retrying."""

    if item.state is not ItemState.FAILED:
        return False

    attributes = item.attributes
    tmdb_id = extract_string_attribute(attributes, "tmdb_id")
    imdb_id = extract_string_attribute(attributes, "imdb_id")
    tvdb_id = extract_string_attribute(attributes, "tvdb_id")
    needs_tvdb_metadata_repair = tmdb_id is None and (
        tvdb_id is not None or item.external_ref.startswith("tvdb:")
    )
    return imdb_id is None or needs_tvdb_metadata_repair


def build_search_query(
    *,
    title: str,
    item_type: str,
    year: int | None,
    season_number: int | None,
    episode_number: int | None,
) -> str:
    """Build one scraper query that preserves season/episode scope when known."""

    if item_type in {"show", "season", "episode", "series"}:
        if season_number is not None and episode_number is not None:
            return f"{title} S{season_number:02d}E{episode_number:02d}"
        if season_number is not None:
            return f"{title} S{season_number:02d}"
        if year is not None:
            return f"{title} {year}"
    else:
        if year is not None:
            return f"{title} {year}"
    return title


def normalize_requested_seasons(requested_seasons: list[int] | None) -> list[int] | None:
    """Return positive, deduplicated requested-season values in deterministic order."""

    if requested_seasons is None:
        return None
    normalized = sorted({season for season in requested_seasons if isinstance(season, int) and season > 0})
    return normalized or None


def normalize_requested_episode_scope(
    requested_episodes: dict[str, list[int]] | dict[int, list[int]] | None,
) -> dict[str, list[int]] | None:
    """Return canonical episode scope keyed by season string with sorted unique episodes."""

    if requested_episodes is None:
        return None

    normalized: dict[str, list[int]] = {}
    for raw_season, raw_episodes in requested_episodes.items():
        try:
            season_number = int(raw_season)
        except (TypeError, ValueError):
            continue
        if season_number <= 0 or not isinstance(raw_episodes, list):
            continue
        episodes = sorted(
            {
                episode
                for episode in raw_episodes
                if isinstance(episode, int) and episode > 0
            }
        )
        if episodes:
            normalized[str(season_number)] = episodes
    return dict(sorted(normalized.items(), key=lambda item: int(item[0]))) or None


def requested_seasons_from_episode_scope(
    requested_episodes: dict[str, list[int]] | None,
) -> list[int] | None:
    """Return season numbers referenced by one episode scope mapping."""

    normalized = normalize_requested_episode_scope(requested_episodes)
    if normalized is None:
        return None
    return [int(season) for season in normalized]


def missing_episode_scope_from_pairs(
    missing_released: list[tuple[int, int]],
) -> dict[str, list[int]] | None:
    """Return episode follow-up scope for explicit missing episode tuples."""

    episode_scope: dict[str, list[int]] = {}
    for season_number, episode_number in missing_released:
        if season_number <= 0 or episode_number <= 0:
            continue
        episode_scope.setdefault(str(season_number), []).append(episode_number)
    return normalize_requested_episode_scope(episode_scope)


def parsed_episode_numbers_from_stream(stream: StreamORM) -> list[int] | None:
    """Return normalized parsed episode numbers for one persisted stream candidate."""

    parsed_title = stream.parsed_title if isinstance(stream.parsed_title, dict) else {}
    raw_value = parsed_title.get("episode")
    if raw_value is None:
        return None
    if isinstance(raw_value, int):
        return [raw_value] if raw_value > 0 else None
    if isinstance(raw_value, str) and raw_value.strip().isdigit():
        episode = int(raw_value.strip())
        return [episode] if episode > 0 else None
    if isinstance(raw_value, list):
        episodes: set[int] = set()
        for value in raw_value:
            if isinstance(value, int) and value > 0:
                episodes.add(value)
            elif isinstance(value, str) and value.strip().isdigit():
                episode = int(value.strip())
                if episode > 0:
                    episodes.add(episode)
        if episodes:
            return sorted(episodes)
    return None


def parsed_seasons_from_stream(stream: StreamORM) -> list[int] | None:
    """Return normalized parsed season numbers for one persisted stream candidate."""

    parsed_title = stream.parsed_title if isinstance(stream.parsed_title, dict) else {}
    raw_value = parsed_title.get("season")
    if raw_value is None:
        return None
    if isinstance(raw_value, int):
        return [raw_value] if raw_value > 0 else None
    if isinstance(raw_value, str) and raw_value.strip().isdigit():
        season = int(raw_value.strip())
        return [season] if season > 0 else None
    if isinstance(raw_value, list):
        seasons: set[int] = set()
        for value in raw_value:
            if isinstance(value, int) and value > 0:
                seasons.add(value)
            elif isinstance(value, str) and value.strip().isdigit():
                season = int(value.strip())
                if season > 0:
                    seasons.add(season)
        if seasons:
            return sorted(seasons)
    return None


def parsed_episode_count_from_stream(stream: StreamORM) -> int:
    """Return the parsed episode count for one persisted stream candidate when available."""

    parsed_episodes = parsed_episode_numbers_from_stream(stream)
    return len(parsed_episodes or [])


def partial_scope_rank_bonus(
    stream: StreamORM,
    requested_seasons: list[int],
    requested_episodes: dict[str, list[int]] | None = None,
) -> int:
    """Return an additive bonus favouring broader coverage for partial show requests."""

    parsed_seasons = parsed_seasons_from_stream(stream)
    if not parsed_seasons:
        return 0

    requested = set(requested_seasons)
    covered_season_count = len(requested.intersection(parsed_seasons))
    if covered_season_count <= 0:
        return 0

    bonus = covered_season_count * _PARTIAL_SCOPE_SEASON_COVERAGE_BONUS
    episode_count = parsed_episode_count_from_stream(stream)
    if episode_count == 0:
        return bonus + _PARTIAL_SCOPE_SEASON_PACK_BONUS
    if requested_episodes:
        parsed_episodes = set(parsed_episode_numbers_from_stream(stream) or [])
        matched_episodes = 0
        for season in parsed_seasons:
            season_key = str(season)
            if season_key not in requested_episodes:
                continue
            matched_episodes += len(parsed_episodes.intersection(requested_episodes[season_key]))
        if matched_episodes > 0:
            return bonus + (matched_episodes * _PARTIAL_SCOPE_MULTI_EPISODE_BONUS)
    if episode_count > 1:
        return bonus + _PARTIAL_SCOPE_MULTI_EPISODE_BONUS
    return bonus


def apply_partial_scope_rank_bonus(
    stream: StreamORM,
    ranked: RankedTorrent,
    requested_seasons: list[int] | None,
    requested_episodes: dict[str, list[int]] | None = None,
) -> RankedTorrent:
    """Return one adjusted RTN result while preserving existing fetch/failure semantics."""

    if not requested_seasons:
        return ranked

    bonus = partial_scope_rank_bonus(stream, requested_seasons, requested_episodes)
    if bonus <= 0:
        return ranked

    score_parts = dict(ranked.score_parts)
    score_parts["partial_scope_bonus"] = bonus
    return RankedTorrent(
        data=ranked.data,
        rank=ranked.rank + bonus,
        lev_ratio=ranked.lev_ratio,
        fetch=ranked.fetch,
        failed_checks=ranked.failed_checks,
        score_parts=score_parts,
    )


def partial_scope_rejection_reason(
    stream: StreamORM,
    requested_seasons: list[int],
    requested_episodes: dict[str, list[int]] | None = None,
) -> str | None:
    """Return one rejection reason when a stream falls outside a partial follow-up scope."""

    parsed_seasons = parsed_seasons_from_stream(stream)
    if not parsed_seasons:
        return "partial_scope_season_missing"
    requested = set(requested_seasons)
    if requested.isdisjoint(parsed_seasons):
        return "partial_scope_season_mismatch"
    if requested_episodes:
        parsed_episode_numbers = parsed_episode_numbers_from_stream(stream)
        if parsed_episode_numbers is None:
            return None
        for season in parsed_seasons:
            season_key = str(season)
            requested_episode_numbers = requested_episodes.get(season_key)
            if not requested_episode_numbers:
                continue
            if set(parsed_episode_numbers).intersection(requested_episode_numbers):
                return None
        return "partial_scope_episode_mismatch"
    return None


def post_rank_expected_scope_reason(item: MediaItemRecord, stream: StreamORM) -> str | None:
    """Return one rejection reason when parsed scope misses expected season/episode values."""

    expected_season = extract_int_value(
        item.attributes,
        "season_number",
        "season",
        "parent_season_number",
    )
    expected_episode = extract_int_value(item.attributes, "episode_number", "episode")
    parsed_seasons = parsed_seasons_from_stream(stream)
    parsed_episodes = parsed_episode_numbers_from_stream(stream)

    if expected_season is not None:
        if parsed_seasons is None:
            return "season_missing"
        if expected_season not in parsed_seasons:
            return "season_mismatch"
    if expected_episode is not None:
        if parsed_episodes is None:
            return "episode_missing"
        if expected_episode not in parsed_episodes:
            return "episode_mismatch"
    return None


def resolve_external_identifiers(item: MediaItemRecord) -> ExternalIdentifiers:
    tmdb_id = item.attributes.get("tmdb_id")
    if not isinstance(tmdb_id, str) and item.external_ref.startswith("tmdb:"):
        tmdb_id = item.external_ref.partition(":")[2] or None

    imdb_id = item.attributes.get("imdb_id")
    if not isinstance(imdb_id, str):
        if item.external_ref.startswith("tt"):
            imdb_id = item.external_ref
        elif item.external_ref.startswith("imdb:"):
            imdb_id = item.external_ref.partition(":")[2] or None
        else:
            imdb_id = None

    tvdb_id = item.attributes.get("tvdb_id")
    trakt_id = item.attributes.get("trakt_id")
    return ExternalIdentifiers(
        tmdb_id=tmdb_id if isinstance(tmdb_id, str) else None,
        tvdb_id=tvdb_id if isinstance(tvdb_id, str) else None,
        imdb_id=imdb_id if isinstance(imdb_id, str) else None,
        trakt_id=trakt_id if isinstance(trakt_id, str) else None,
    )


def build_scraper_search_input(
    item: MediaItemRecord,
    *,
    season_override: int | None = None,
    episode_override: int | None = None,
) -> ScraperSearchInput:
    """Build the search input for a scraper plugin call."""

    item_type = resolve_item_type(item)
    year = extract_int_value(item.attributes, "year", "release_year")
    season_number = season_override or extract_int_value(
        item.attributes,
        "season_number",
        "season",
        "parent_season_number",
    )
    episode_number = episode_override or extract_int_value(item.attributes, "episode_number", "episode")
    return ScraperSearchInput(
        item_id=item.id,
        item_type=item_type,
        title=item.title,
        year=year,
        season_number=season_number,
        episode_number=episode_number,
        query=build_search_query(
            title=item.title,
            item_type=item_type,
            year=year,
            season_number=season_number,
            episode_number=episode_number,
        ),
        external_ids=resolve_external_identifiers(item),
        metadata=dict(item.attributes),
    )


def scrape_candidate_from_plugin_result(
    *,
    item_id: str,
    result: PluginScraperResult,
) -> ScrapeCandidateRecord | None:
    info_hash = result.info_hash
    if info_hash is None and result.magnet_url:
        parsed = parse_qs(urlparse(result.magnet_url).query)
        for value in parsed.get("xt", []):
            if value.startswith("urn:btih:"):
                info_hash = value.partition("urn:btih:")[2]
                break
    if not info_hash:
        return None

    normalized_hash = info_hash.strip().lower()
    raw_title = result.title.strip()
    if not normalized_hash or not raw_title:
        return None

    return ScrapeCandidateRecord(
        item_id=item_id,
        info_hash=normalized_hash,
        raw_title=raw_title,
        provider=result.provider or "plugin",
        size_bytes=result.size_bytes,
    )
