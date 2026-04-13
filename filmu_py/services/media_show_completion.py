"""Show-completion inventory evaluation extracted from the media service."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, cast

from sqlalchemy import select
from sqlalchemy.orm import selectinload

import filmu_py.services.media_path_inference as _media_path_inference
from filmu_py.config import Settings
from filmu_py.db.models import ItemRequestORM, MediaEntryORM, MediaItemORM, SeasonORM, ShowORM
from filmu_py.db.runtime import DatabaseRuntime

if TYPE_CHECKING:
    from filmu_py.services.media import MediaItemRecord

logger = logging.getLogger(__name__)

_SATISFYING_MEDIA_ENTRY_REFRESH_STATES = ("ready", "stale", "refreshing")


@dataclass(frozen=True)
class ShowCompletionResult:
    """Coverage result for one requested show scope using cached metadata only."""

    all_satisfied: bool
    any_satisfied: bool
    has_future_episodes: bool
    missing_released: list[tuple[int, int]]


@dataclass
class SeasonEpisodeInventory:
    """Known, released, and future episode numbers for one season snapshot."""

    known_episodes: set[int] = field(default_factory=set)
    released_episodes: set[int] = field(default_factory=set)
    future_episodes: set[int] = field(default_factory=set)


def _latest_item_request(item_requests: list[ItemRequestORM]) -> ItemRequestORM | None:
    if not item_requests:
        return None
    return max(
        item_requests,
        key=lambda request_record: (
            request_record.last_requested_at,
            request_record.created_at,
        ),
    )


def _extract_int_value(attributes: dict[str, object], key: str, *aliases: str) -> int | None:
    for candidate_key in (key, *aliases):
        value = attributes.get(candidate_key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _parse_calendar_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.strptime(normalized.split("T")[0], "%Y-%m-%d")
        except ValueError:
            return None


def _coerce_object_dict(value: object) -> dict[str, object] | None:
    if isinstance(value, dict):
        return cast(dict[str, object], value)
    return None


def _iter_metadata_dicts(value: object, *, max_depth: int = 3) -> list[dict[str, object]]:
    if max_depth < 0:
        return []
    if isinstance(value, dict):
        dicts = [cast(dict[str, object], value)]
        if max_depth == 0:
            return dicts
        for nested in value.values():
            dicts.extend(_iter_metadata_dicts(nested, max_depth=max_depth - 1))
        return dicts
    if isinstance(value, list) and max_depth > 0:
        nested_dicts: list[dict[str, object]] = []
        for nested in value:
            nested_dicts.extend(_iter_metadata_dicts(nested, max_depth=max_depth - 1))
        return nested_dicts
    return []


def _parse_episode_air_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    parsed = _parse_calendar_datetime(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.date()
    return parsed.astimezone(UTC).date()


def _merge_future_episode_snapshot(
    inventory_by_season: dict[int, SeasonEpisodeInventory],
    payload: dict[str, object],
    *,
    today: date,
) -> None:
    season_number = _extract_int_value(payload, "season_number", "seasonNumber", "season")
    episode_number = _extract_int_value(
        payload,
        "episode_number",
        "episodeNumber",
        "episode",
        "number",
    )
    if season_number is None or episode_number is None:
        return

    inventory = inventory_by_season.setdefault(season_number, SeasonEpisodeInventory())
    inventory.known_episodes.add(episode_number)
    air_date = _parse_episode_air_date(
        payload.get("air_date")
        or payload.get("airDate")
        or payload.get("first_aired")
        or payload.get("release_date")
    )
    if air_date is None or air_date > today:
        inventory.future_episodes.add(episode_number)
    else:
        inventory.released_episodes.add(episode_number)


def extract_tmdb_episode_inventory(
    attributes: dict[str, object],
    *,
    today: date,
) -> dict[int, SeasonEpisodeInventory]:
    inventory_by_season: dict[int, SeasonEpisodeInventory] = {}
    next_episode_snapshots: list[dict[str, object]] = []
    raw_status = attributes.get("status")
    normalized_status = raw_status.strip().casefold() if isinstance(raw_status, str) else None

    for metadata_dict in _iter_metadata_dicts(attributes, max_depth=3):
        raw_seasons = metadata_dict.get("seasons")
        if not isinstance(raw_seasons, list):
            continue

        for raw_season in raw_seasons:
            season_dict = _coerce_object_dict(raw_season)
            if season_dict is None:
                continue

            season_number = _extract_int_value(
                season_dict,
                "season_number",
                "seasonNumber",
                "season",
            )
            if season_number is None:
                continue

            inventory = inventory_by_season.setdefault(season_number, SeasonEpisodeInventory())
            released_count = _extract_int_value(
                season_dict,
                "released_episode_count",
                "releasedEpisodeCount",
                "aired_episodes",
                "airedEpisodes",
                "episodes_released",
            )
            total_count = _extract_int_value(
                season_dict,
                "episode_count",
                "episodeCount",
                "number_of_episodes",
                "total_episodes",
            )

            raw_episodes = season_dict.get("episodes")
            if isinstance(raw_episodes, list):
                for raw_episode in raw_episodes:
                    episode_dict = _coerce_object_dict(raw_episode)
                    if episode_dict is None:
                        continue
                    episode_number = _extract_int_value(
                        episode_dict,
                        "episode_number",
                        "episodeNumber",
                        "episode",
                        "number",
                    )
                    if episode_number is None:
                        continue
                    inventory.known_episodes.add(episode_number)
                    air_date = _parse_episode_air_date(
                        episode_dict.get("air_date")
                        or episode_dict.get("airDate")
                        or episode_dict.get("first_aired")
                        or episode_dict.get("release_date")
                    )
                    if air_date is None:
                        continue
                    if air_date <= today:
                        inventory.released_episodes.add(episode_number)
                    else:
                        inventory.future_episodes.add(episode_number)

            if released_count is not None and released_count > 0:
                inventory.known_episodes.update(range(1, released_count + 1))
                inventory.released_episodes.update(range(1, released_count + 1))

            if total_count is not None and total_count > 0:
                inventory.known_episodes.update(range(1, total_count + 1))
                if released_count is not None and total_count > released_count:
                    inventory.future_episodes.update(range(released_count + 1, total_count + 1))

    for metadata_dict in _iter_metadata_dicts(attributes, max_depth=3):
        for key in ("next_episode_to_air", "nextEpisodeToAir"):
            next_episode = _coerce_object_dict(metadata_dict.get(key))
            if next_episode is not None:
                next_episode_snapshots.append(next_episode)
                _merge_future_episode_snapshot(inventory_by_season, next_episode, today=today)

    if normalized_status in {"ended", "cancelled", "canceled"}:
        for inventory in inventory_by_season.values():
            inventory.released_episodes.update(inventory.known_episodes)
            inventory.future_episodes.clear()
        return inventory_by_season

    next_season_number: int | None = None
    next_episode_number: int | None = None
    for snapshot in next_episode_snapshots:
        candidate_season = _extract_int_value(snapshot, "season_number", "seasonNumber", "season")
        candidate_episode = _extract_int_value(
            snapshot,
            "episode_number",
            "episodeNumber",
            "episode",
            "number",
        )
        if candidate_season is None or candidate_episode is None:
            continue
        next_season_number = candidate_season
        next_episode_number = candidate_episode
        break

    if next_season_number is None:
        return inventory_by_season

    for season_number, inventory in inventory_by_season.items():
        if not inventory.known_episodes:
            continue
        if season_number < next_season_number:
            inventory.released_episodes.update(inventory.known_episodes)
            inventory.future_episodes.difference_update(inventory.known_episodes)
            continue
        if season_number > next_season_number:
            continue
        assert next_episode_number is not None
        released_before_next = {ep for ep in inventory.known_episodes if ep < next_episode_number}
        future_from_next = {ep for ep in inventory.known_episodes if ep >= next_episode_number}
        inventory.released_episodes.update(released_before_next)
        inventory.future_episodes.update(future_from_next)

    return inventory_by_season


def _season_known_episode_numbers(
    season_number: int,
    inventory_by_season: dict[int, SeasonEpisodeInventory],
    fallback_scope: dict[int, set[int]],
) -> set[int]:
    inventory = inventory_by_season.get(season_number)
    if inventory is not None and inventory.known_episodes:
        return set(inventory.known_episodes)
    return set(fallback_scope.get(season_number, set()))


def _build_requested_episode_scope(
    request_record: ItemRequestORM | None,
    inventory_by_season: dict[int, SeasonEpisodeInventory],
    fallback_scope: dict[int, set[int]],
) -> set[tuple[int, int]]:
    available_seasons = set(inventory_by_season) | set(fallback_scope)

    if request_record is None or not request_record.is_partial:
        return {
            (season_number, episode_number)
            for season_number in available_seasons
            for episode_number in _season_known_episode_numbers(
                season_number,
                inventory_by_season,
                fallback_scope,
            )
        }

    requested_scope: set[tuple[int, int]] = set()
    explicitly_requested_seasons: set[int] = set()

    for raw_season, raw_episodes in (request_record.requested_episodes or {}).items():
        try:
            season_number = int(str(raw_season))
        except ValueError:
            continue
        explicitly_requested_seasons.add(season_number)
        requested_scope.update(
            (season_number, episode_number)
            for episode_number in raw_episodes
            if isinstance(episode_number, int) and episode_number > 0
        )

    for season_number in request_record.requested_seasons or []:
        if not isinstance(season_number, int) or season_number <= 0:
            continue
        if season_number in explicitly_requested_seasons:
            continue
        requested_scope.update(
            (season_number, episode_number)
            for episode_number in _season_known_episode_numbers(
                season_number,
                inventory_by_season,
                fallback_scope,
            )
        )

    return requested_scope


async def evaluate_show_completion(
    item: MediaItemRecord,
    db: DatabaseRuntime,
    settings: Settings,
) -> ShowCompletionResult:
    """Evaluate show completion from cached metadata, request scope, and active streams."""

    _ = settings
    today = datetime.now(UTC).date()
    show_attributes = dict(cast(dict[str, object], item.attributes or {}))

    async with db.session() as session:
        requests_result = await session.execute(
            select(ItemRequestORM).where(ItemRequestORM.media_item_id == item.id)
        )
        latest_request = _latest_item_request(list(requests_result.scalars().all()))

        show_result = await session.execute(
            select(ShowORM)
            .where(ShowORM.media_item_id == item.id)
            .options(selectinload(ShowORM.seasons).selectinload(SeasonORM.episodes))
        )
        show_orm = show_result.scalar_one_or_none()

        inventory_by_season = extract_tmdb_episode_inventory(show_attributes, today=today)
        fallback_scope: dict[int, set[int]] = {}
        episode_item_ids_by_scope: dict[tuple[int, int], str] = {}

        if show_orm is not None:
            for season in show_orm.seasons:
                season_number = season.season_number
                if season_number is None:
                    continue

                inventory = inventory_by_season.setdefault(season_number, SeasonEpisodeInventory())
                season_scope = fallback_scope.setdefault(season_number, set())
                for episode in season.episodes:
                    episode_number = episode.episode_number
                    if episode_number is None:
                        continue
                    season_scope.add(episode_number)
                    inventory.known_episodes.add(episode_number)
                    episode_item_id = str(episode.media_item_id)
                    episode_item_ids_by_scope[(season_number, episode_number)] = episode_item_id

                    episode_item = await session.get(MediaItemORM, episode_item_id)
                    if episode_item is None:
                        continue
                    episode_attributes = dict(cast(dict[str, object], episode_item.attributes or {}))
                    air_date = _parse_episode_air_date(
                        episode_attributes.get("aired_at")
                        or episode_attributes.get("air_date")
                        or episode_attributes.get("airDate")
                    )
                    if air_date is None:
                        continue
                    if air_date <= today:
                        inventory.released_episodes.add(episode_number)
                    else:
                        inventory.future_episodes.add(episode_number)

        requested_scope = _build_requested_episode_scope(
            latest_request,
            inventory_by_season,
            fallback_scope,
        )

        released_scope: set[tuple[int, int]] = set()
        future_scope: set[tuple[int, int]] = set()
        for season_number, episode_number in requested_scope:
            season_inventory = inventory_by_season.get(season_number)
            if season_inventory is None:
                continue
            if episode_number in season_inventory.released_episodes:
                released_scope.add((season_number, episode_number))
            elif episode_number in season_inventory.future_episodes:
                future_scope.add((season_number, episode_number))

        active_item_ids: set[str] = set()
        if episode_item_ids_by_scope:
            covered_result = await session.execute(
                select(MediaEntryORM.item_id)
                .where(
                    MediaEntryORM.item_id.in_(list(episode_item_ids_by_scope.values())),
                    MediaEntryORM.refresh_state.in_(_SATISFYING_MEDIA_ENTRY_REFRESH_STATES),
                    MediaEntryORM.entry_type == "media",
                )
                .distinct()
            )
            active_item_ids = {str(item_id) for item_id in covered_result.scalars().all()}
        else:
            show_entry_path_result = await session.execute(
                select(MediaEntryORM.provider_file_path, MediaEntryORM.original_filename)
                .where(
                    MediaEntryORM.item_id == item.id,
                    MediaEntryORM.refresh_state.in_(_SATISFYING_MEDIA_ENTRY_REFRESH_STATES),
                    MediaEntryORM.entry_type == "media",
                )
            )
            entry_paths = list(show_entry_path_result.all())
            if entry_paths:
                season_pack_seasons: set[int] = set()
                covered_scope_from_paths: set[tuple[int, int]] = set()
                for file_path, original_filename in entry_paths:
                    candidate_path = file_path or original_filename
                    inferred_seasons = _media_path_inference.infer_season_range_from_path(candidate_path)
                    inferred_episode = _media_path_inference.infer_episode_number_from_path(candidate_path)
                    if inferred_episode is not None and len(inferred_seasons) == 1:
                        covered_scope_from_paths.add((inferred_seasons[0], inferred_episode))
                    elif inferred_seasons:
                        season_pack_seasons.update(inferred_seasons)

                covered_seasons = set(season_pack_seasons) | {
                    season_number for season_number, _episode_number in covered_scope_from_paths
                }

                logger.info(
                    "evaluate_show_completion: no episode children but show-level media entries found"
                    " - pack-satisfied for seasons: %s",
                    sorted(covered_seasons),
                    extra={"item_id": item.id},
                )

                if released_scope:
                    for scope_key in released_scope:
                        season_num_key, _ = scope_key
                        if scope_key in covered_scope_from_paths or season_num_key in season_pack_seasons:
                            episode_item_ids_by_scope[scope_key] = str(item.id)
                    if episode_item_ids_by_scope:
                        active_item_ids = {str(item.id)}
                else:
                    if requested_scope:
                        total_requested: set[int] = {season for season, _episode in requested_scope}
                    elif (
                        latest_request is not None
                        and latest_request.is_partial
                        and latest_request.requested_seasons
                    ):
                        total_requested = set(latest_request.requested_seasons)
                    else:
                        total_requested = set(inventory_by_season.keys())
                        if not total_requested:
                            logger.warning(
                                "evaluate_show_completion: no inventory for full-show request; treating pack as incomplete",
                                extra={
                                    "item_id": item.id,
                                    "covered": sorted(covered_seasons),
                                },
                            )
                            return ShowCompletionResult(
                                all_satisfied=False,
                                any_satisfied=bool(covered_seasons),
                                has_future_episodes=bool(future_scope),
                                missing_released=[],
                            )

                    missing_seasons = sorted(total_requested - season_pack_seasons)
                    any_satisfied = bool(season_pack_seasons & total_requested) or bool(
                        covered_scope_from_paths or (season_pack_seasons and not total_requested)
                    )
                    all_satisfied = not missing_seasons and any_satisfied

                    logger.info(
                        "evaluate_show_completion: no TMDB inventory - direct pack result",
                        extra={
                            "item_id": item.id,
                            "covered": sorted(covered_seasons),
                            "requested": sorted(total_requested),
                            "missing": missing_seasons,
                            "all_satisfied": all_satisfied,
                            "any_satisfied": any_satisfied,
                        },
                    )
                    return ShowCompletionResult(
                        all_satisfied=all_satisfied,
                        any_satisfied=any_satisfied,
                        has_future_episodes=bool(future_scope),
                        missing_released=[(season, 0) for season in missing_seasons],
                    )

    satisfied_scope = {
        scope
        for scope, episode_item_id in episode_item_ids_by_scope.items()
        if scope in released_scope and episode_item_id in active_item_ids
    }

    unresolved_requested = requested_scope - released_scope - future_scope
    has_explicit_season_request = (
        latest_request is not None
        and latest_request.is_partial
        and bool(latest_request.requested_seasons)
    )
    inventory_is_empty = not episode_item_ids_by_scope and not released_scope
    scope_is_empty_despite_request = not requested_scope and has_explicit_season_request

    if inventory_is_empty or scope_is_empty_despite_request:
        logger.warning(
            "evaluate_show_completion: no episode coverage data, treating as unsatisfied",
            extra={
                "item_id": item.id,
                "inventory_is_empty": inventory_is_empty,
                "scope_is_empty_despite_request": scope_is_empty_despite_request,
                "episode_item_ids_count": len(episode_item_ids_by_scope),
                "released_scope_count": len(released_scope),
                "requested_scope_count": len(requested_scope),
            },
        )
        return ShowCompletionResult(
            all_satisfied=False,
            any_satisfied=False,
            has_future_episodes=bool(future_scope),
            missing_released=[],
        )

    return ShowCompletionResult(
        all_satisfied=not unresolved_requested and satisfied_scope == released_scope,
        any_satisfied=bool(satisfied_scope),
        has_future_episodes=bool(future_scope),
        missing_released=sorted(released_scope - satisfied_scope),
    )
