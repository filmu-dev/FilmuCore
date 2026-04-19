"""Media domain service for GraphQL/API orchestration and persistence."""

from __future__ import annotations

import asyncio
import enum
import logging
from collections import Counter
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from uuid import UUID

import structlog
from arq.connections import ArqRedis
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import attributes as orm_attributes
from sqlalchemy.orm import selectinload

import filmu_py.services.media_path_inference as _media_path_inference
import filmu_py.services.media_show_completion as _media_show_completion
import filmu_py.services.media_stream_candidates as _media_stream_candidates
from filmu_py.api.playback_resolution import PlaybackAttachment
from filmu_py.config import Settings, get_settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.db.models import (
    ActiveStreamORM,
    ConsumerPlaybackActivityEventORM,
    EpisodeORM,
    ItemRequestORM,
    ItemStateEventORM,
    ItemWorkflowCheckpointORM,
    MediaEntryORM,
    MediaItemORM,
    MovieORM,
    OutboxEventORM,
    PlaybackAttachmentORM,
    ScrapeCandidateORM,
    SeasonORM,
    ShowORM,
    StreamBlacklistRelationORM,
    StreamORM,
    SubtitleEntryORM,
)
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.debrid import TorrentInfo
from filmu_py.services.playback import (
    DirectFileLinkLifecycleSnapshot,
    PlaybackResolutionSnapshot,
    PlaybackSourceService,
)
from filmu_py.services.settings_service import load_settings
from filmu_py.services.tmdb import (
    MovieMetadata,
    ShowMetadata,
    TmdbDiscoveryProfile,
    TmdbMetadataClient,
    TmdbSearchPage,
    TmdbSearchResult,
    build_tmdb_metadata_client,
)
from filmu_py.services.tvdb import TvdbClient, TvdbSeriesMetadata
from filmu_py.state.item import ItemEvent, ItemState, ItemStateMachine

_NOTIFICATION_ITEM_TYPES = {"movie", "show", "season", "episode"}
_SATISFYING_MEDIA_ENTRY_REFRESH_STATES = ("ready", "stale", "refreshing")
logger = logging.getLogger(__name__)
structlogger = structlog.get_logger(__name__)
_STATE_CHANGED_EVENT_TOPIC = "item.state.changed"
_NOTIFICATIONS_EVENT_TOPIC = "notifications"
_TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/original"
_TITLE_ALIASES_ATTRIBUTE_KEY = "aliases"
_UNSET = object()
_SUPPORTED_EXTERNAL_REF_SYSTEMS = frozenset({"tmdb", "tvdb", "imdb"})
_GET_ITEM_UUID_ERROR = (
    "get_item() requires a UUID; use get_item_by_external_id() for external refs"
)
_REQUEST_SEARCH_MAX_REMOTE_PAGES = 6
_REQUEST_SEARCH_DEFAULT_SCAN_WINDOW = 60
_REQUEST_SEARCH_MAX_SCAN_WINDOW = 120
_REQUEST_DISCOVER_MAX_REMOTE_PAGES = 6
_REQUEST_DISCOVER_DEFAULT_SCAN_WINDOW = 80
_REQUEST_DISCOVER_MAX_SCAN_WINDOW = 180
_REQUEST_LOCAL_SIGNAL_PLAYBACK_WINDOW = timedelta(days=120)
_REQUEST_DISCOVERY_FACET_DETAIL_WINDOW = 48
_REQUEST_DISCOVERY_FACET_DETAIL_CONCURRENCY = 8
_REQUEST_DISCOVERY_PROJECTION_WINDOW = 20
_REQUEST_DISCOVERY_EDITORIAL_BLEND_PAGE_LIMIT = 2
_REQUEST_DISCOVERY_RELEASE_WINDOW_BLEND_PAGE_LIMIT = 2
_REQUEST_DISCOVERY_STUDIO_HINTS = (
    "studio",
    "studios",
    "pictures",
    "films",
    "television",
    "animation",
    "entertainment",
    "bros",
    "productions",
)
_REQUEST_DISCOVERY_RAILS: tuple[dict[str, str], ...] = (
    {
        "id": "new-sci-fi-films",
        "title": "New sci-fi films",
        "description": "Fresh science-fiction film candidates with immediate request coverage.",
        "query": "science fiction",
        "media_type": "movie",
    },
    {
        "id": "prestige-crime-films",
        "title": "Prestige crime films",
        "description": "High-signal crime films when you want tighter catalog depth.",
        "query": "crime thriller",
        "media_type": "movie",
    },
    {
        "id": "historical-epics",
        "title": "Historical epics",
        "description": "Large-scale period films ready for the same request flow.",
        "query": "historical epic",
        "media_type": "movie",
    },
    {
        "id": "animated-features",
        "title": "Animated features",
        "description": "Animated film candidates with broad catalog and family coverage.",
        "query": "animated adventure",
        "media_type": "movie",
    },
    {
        "id": "prestige-series",
        "title": "Prestige series",
        "description": "Returnable drama series that benefit from scoped intake.",
        "query": "prestige drama",
        "media_type": "show",
    },
    {
        "id": "mystery-series",
        "title": "Mystery series",
        "description": "Serialized mysteries suited for season-level request follow-through.",
        "query": "mystery series",
        "media_type": "show",
    },
    {
        "id": "space-operas",
        "title": "Space operas",
        "description": "Large-arc science-fiction series without dropping into a generic browse route.",
        "query": "space opera",
        "media_type": "show",
    },
    {
        "id": "limited-series",
        "title": "Limited series",
        "description": "Shorter-form series candidates for bounded intake and faster completion.",
        "query": "limited series",
        "media_type": "show",
    },
)
_REQUEST_EDITORIAL_DISCOVERY_FAMILIES: tuple[dict[str, str], ...] = (
    {
        "id": "trending-films",
        "family": "trending",
        "title": "Trending films",
        "description": "Fast-moving film picks pulled from the live TMDB trend window.",
        "media_type": "movie",
    },
    {
        "id": "popular-films",
        "family": "popular",
        "title": "Popular films",
        "description": "Broad-audience films ranked by current popularity signals.",
        "media_type": "movie",
    },
    {
        "id": "anticipated-films",
        "family": "anticipated",
        "title": "Anticipated films",
        "description": "Upcoming film releases with active audience demand.",
        "media_type": "movie",
    },
    {
        "id": "newly-released-films",
        "family": "newly-released",
        "title": "Newly released films",
        "description": "Recently released films that are already entering the current window.",
        "media_type": "movie",
    },
    {
        "id": "trending-series",
        "family": "trending",
        "title": "Trending series",
        "description": "Fast-moving series picks pulled from the live TMDB trend window.",
        "media_type": "show",
    },
    {
        "id": "popular-series",
        "family": "popular",
        "title": "Popular series",
        "description": "Broad-audience series ranked by current popularity signals.",
        "media_type": "show",
    },
    {
        "id": "returning-series",
        "family": "returning",
        "title": "Returning series",
        "description": "Series currently back on air and suited for scoped intake follow-through.",
        "media_type": "show",
    },
    {
        "id": "newly-released-series",
        "family": "newly-released",
        "title": "Newly released series",
        "description": "Series with fresh first-run episodes in the active release window.",
        "media_type": "show",
    },
)
_REQUEST_RELEASE_WINDOWS: tuple[dict[str, str], ...] = (
    {
        "id": "theatrical-films",
        "window": "theatrical",
        "title": "Theatrical window",
        "description": "Films playing in the near theatrical window for quick intake decisions.",
        "media_type": "movie",
    },
    {
        "id": "digital-film-premieres",
        "window": "digital",
        "title": "Digital window",
        "description": "Films crossing into digital availability without waiting on generic browse drift.",
        "media_type": "movie",
    },
    {
        "id": "returning-series-window",
        "window": "returning",
        "title": "Returning series",
        "description": "Series already back on air in the current active episode window.",
        "media_type": "show",
    },
    {
        "id": "limited-series-launches",
        "window": "limited-series",
        "title": "Limited-series launches",
        "description": "Bounded series launches that fit short-run intake and completion loops.",
        "media_type": "show",
    },
)
_CONSUMER_PLAYBACK_ACTIVE_WINDOW = timedelta(minutes=15)
_CONSUMER_PLAYBACK_SESSION_LIMIT = 6


class ItemNotFoundError(RuntimeError):
    """Raised when a media-item action targets an unknown item."""


class ArqNotEnabledError(RuntimeError):
    """Raised when an action requires ARQ but no queue client is configured."""


def _parse_external_ref_identifier(value: str) -> tuple[str, str] | None:
    """Return normalized `(system, reference)` parts for supported external refs."""

    normalized_value = value.strip()
    system, separator, reference = normalized_value.partition(":")
    normalized_system = system.casefold()
    if (
        separator == ""
        or reference == ""
        or normalized_system not in _SUPPORTED_EXTERNAL_REF_SYSTEMS
    ):
        return None
    return normalized_system, reference


def _normalize_internal_item_id(item_id: str) -> str:
    """Return one canonical internal UUID identifier or raise a clear caller error."""

    normalized_item_id = item_id.strip()
    if not normalized_item_id:
        raise ValueError(_GET_ITEM_UUID_ERROR)
    try:
        return str(UUID(normalized_item_id))
    except ValueError as exc:
        raise ValueError(_GET_ITEM_UUID_ERROR) from exc


def _attach_retry_reset_diagnostics(
    item: MediaItemORM,
    *,
    imdb_id_was_missing: bool,
    streams_blacklisted: int,
    active_stream_cleared: bool,
    scrape_job_enqueued: bool,
) -> MediaItemORM:
    item._imdb_id_was_missing = imdb_id_was_missing  # type: ignore[attr-defined]
    item._streams_blacklisted = streams_blacklisted  # type: ignore[attr-defined]
    item._active_stream_cleared = active_stream_cleared  # type: ignore[attr-defined]
    item._scrape_job_enqueued = scrape_job_enqueued  # type: ignore[attr-defined]
    return item


def _clone_media_item_snapshot(item: MediaItemORM) -> MediaItemORM:
    """Return a detached primitive snapshot safe to use after session commit."""

    return MediaItemORM(
        id=item.id,
        tenant_id=item.tenant_id,
        external_ref=item.external_ref,
        title=item.title,
        state=item.state,
        attributes=dict(cast(dict[str, object], item.attributes or {})),
        created_at=item.created_at,
        updated_at=item.updated_at,
    )


def _coerce_notification_item_type(attributes: dict[str, object]) -> str:
    """Return a frontend-compatible notification item type."""

    raw_type = attributes.get("item_type")
    if isinstance(raw_type, str) and raw_type in _NOTIFICATION_ITEM_TYPES:
        return raw_type
    return "movie"


_TYPE_ALIASES: dict[str, set[str]] = {
    "movie": {"movie"},
    "show": {"show", "tv"},
    "tv": {"show", "tv"},
    "season": {"season"},
    "episode": {"episode"},
}


def _canonical_item_type_name(item_type: str | None) -> str:
    """Return the compatibility wire type for one internal media item type."""

    if item_type is None:
        return "unknown"

    normalized = item_type.strip().casefold()
    if not normalized:
        return "unknown"
    if normalized in {"show", "tv"}:
        return "show"
    return normalized


def _coerce_notification_year(attributes: dict[str, object]) -> int | None:
    """Return a notification year when attributes contain a usable numeric value."""

    raw_year = attributes.get("year")
    if isinstance(raw_year, int):
        return raw_year
    if isinstance(raw_year, str) and raw_year.isdigit():
        return int(raw_year)
    return None


def _coerce_notification_imdb_id(item: MediaItemORM) -> str | None:
    """Return a likely imdb identifier for compatibility notifications when available."""

    raw_imdb_id = item.attributes.get("imdb_id")
    if isinstance(raw_imdb_id, str) and raw_imdb_id:
        return raw_imdb_id
    if item.external_ref.startswith("tt"):
        return item.external_ref
    return None


def build_completion_notification_payload(item: MediaItemORM) -> dict[str, Any]:
    """Build the notification payload expected by the frontend SSE consumer."""

    attributes = cast(dict[str, object], item.attributes or {})
    item_type = _coerce_notification_item_type(attributes)
    year = _coerce_notification_year(attributes)
    now = datetime.now(UTC)
    duration = max(0, int((now - item.created_at).total_seconds()))
    title = item.title or item.external_ref
    log_string = f"{title} ({year})" if year is not None and item_type == "movie" else title

    return {
        "title": title,
        "type": item_type,
        "year": year,
        "duration": duration,
        "timestamp": now.isoformat(),
        "log_string": log_string,
        "imdb_id": _coerce_notification_imdb_id(item),
    }


def _extract_string(attributes: dict[str, object], key: str) -> str | None:
    """Return a non-empty string value from item metadata when present."""

    value = attributes.get(key)
    if isinstance(value, str) and value.strip():
        return value
    return None


def _extract_first_string(attributes: dict[str, object], *keys: str) -> str | None:
    """Return the first non-empty string value from the provided metadata keys."""

    for key in keys:
        value = _extract_string(attributes, key)
        if value is not None:
            return value
    return None


def _coerce_int32_or_none(value: int | None) -> int | None:
    """Return persisted size values unchanged now that the storage columns are `BIGINT`."""

    return value


def _parse_calendar_datetime(value: str | None) -> datetime | None:
    """Parse one calendar/release timestamp leniently for compatibility filtering."""

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


def _build_calendar_release_data(attributes: dict[str, object]) -> CalendarReleaseDataRecord | None:
    """Return normalized release-data payload when metadata carries it."""

    nested = attributes.get("release_data")
    if isinstance(nested, dict):
        nested_dict = cast(dict[str, object], nested)
        release_data = CalendarReleaseDataRecord(
            next_aired=_extract_string(nested_dict, "next_aired"),
            nextAired=_extract_string(nested_dict, "nextAired"),
            last_aired=_extract_string(nested_dict, "last_aired"),
            lastAired=_extract_string(nested_dict, "lastAired"),
        )
    else:
        release_data = CalendarReleaseDataRecord(
            next_aired=_extract_string(attributes, "next_aired"),
            nextAired=_extract_string(attributes, "nextAired"),
            last_aired=_extract_string(attributes, "last_aired"),
            lastAired=_extract_string(attributes, "lastAired"),
        )

    if any(
        value is not None
        for value in (
            release_data.next_aired,
            release_data.nextAired,
            release_data.last_aired,
            release_data.lastAired,
        )
    ):
        return release_data
    return None


def _resolve_calendar_show_title(item: MediaItemORM, attributes: dict[str, object]) -> str:
    """Return the frontend-facing show title for one calendar row."""

    return (
        _extract_first_string(attributes, "show_title", "series_title", "parent_title")
        or item.title
    )


def _calendar_projection_type(specialization: MediaItemSpecializationRecord) -> str:
    """Return the compatibility calendar item type for one specialization."""

    if specialization.item_type == "show":
        return "tv"
    return specialization.item_type


def _calendar_projection_identifiers(
    specialization: MediaItemSpecializationRecord,
) -> tuple[str | None, str | None]:
    """Return calendar identifiers rebound to the parent show when available."""

    if specialization.item_type in {"season", "episode"} and specialization.parent_ids is not None:
        return specialization.parent_ids.tmdb_id, specialization.parent_ids.tvdb_id
    return specialization.tmdb_id, specialization.tvdb_id


def _canonical_state_name(state: str) -> str:
    """Return the frontend-facing display form for a lifecycle state value."""

    return state.replace("_", " ").strip().title()


def _coerce_parent_ids(attributes: dict[str, object]) -> ParentIdsRecord | None:
    """Return normalized parent identifier payload when metadata carries it."""

    raw_parent_ids = attributes.get("parent_ids")
    if not isinstance(raw_parent_ids, dict):
        return None

    tmdb_id = raw_parent_ids.get("tmdb_id")
    tvdb_id = raw_parent_ids.get("tvdb_id")
    return ParentIdsRecord(
        tmdb_id=tmdb_id if isinstance(tmdb_id, str) and tmdb_id else None,
        tvdb_id=tvdb_id if isinstance(tvdb_id, str) and tvdb_id else None,
    )


def _normalize_poster_path(value: str | None) -> str | None:
    """Return a library-safe poster URL for frontend image rendering."""

    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.startswith(("http://", "https://")):
        return normalized
    if normalized.startswith("/"):
        return f"{_TMDB_IMAGE_BASE_URL}{normalized}"
    return normalized


@dataclass(frozen=True)
class MediaItemSpecializationRecord:
    """Stable specialization-backed hierarchy and identifier projection."""

    item_type: str
    tmdb_id: str | None = None
    tvdb_id: str | None = None
    imdb_id: str | None = None
    parent_ids: ParentIdsRecord | None = None
    show_title: str | None = None
    season_number: int | None = None
    episode_number: int | None = None


def _loaded_relationship(instance: object | None, attribute_name: str) -> object | None:
    """Return one already-loaded ORM relationship without triggering lazy loads."""

    if instance is None:
        return None
    state = getattr(instance, "__dict__", None)
    if not isinstance(state, dict):
        return None
    value = state.get(attribute_name, NO_VALUE)
    return None if value is NO_VALUE else value


def _build_specialization_record(item: MediaItemORM) -> MediaItemSpecializationRecord:
    """Return specialization-backed item hierarchy above metadata fallbacks."""

    attributes = cast(dict[str, object], item.attributes or {})
    item_type = _canonical_item_type_name(_extract_string(attributes, "item_type"))
    tmdb_id = _extract_string(attributes, "tmdb_id")
    tvdb_id = _extract_string(attributes, "tvdb_id")
    imdb_id = _extract_string(attributes, "imdb_id")
    parent_ids = _coerce_parent_ids(attributes)
    show_title = _extract_first_string(attributes, "show_title", "series_title", "parent_title")
    season_number = _extract_int_value(attributes, "season_number", "season", "parent_season_number")
    episode_number = _extract_int_value(attributes, "episode_number", "episode")
    movie = cast(MovieORM | None, _loaded_relationship(item, "movie"))
    show = cast(ShowORM | None, _loaded_relationship(item, "show"))
    season = cast(SeasonORM | None, _loaded_relationship(item, "season"))
    episode = cast(EpisodeORM | None, _loaded_relationship(item, "episode"))

    if movie is not None:
        item_type = "movie"
        tmdb_id = movie.tmdb_id or tmdb_id
        imdb_id = movie.imdb_id or imdb_id
    elif show is not None:
        item_type = "show"
        tmdb_id = show.tmdb_id or tmdb_id
        tvdb_id = show.tvdb_id or tvdb_id
        imdb_id = show.imdb_id or imdb_id
        show_title = item.title
    elif season is not None:
        item_type = "season"
        tmdb_id = season.tmdb_id or tmdb_id
        tvdb_id = season.tvdb_id or tvdb_id
        season_number = season.season_number or season_number
        season_show = cast(ShowORM | None, _loaded_relationship(season, "show"))
        if season_show is not None:
            show_item = cast(MediaItemORM | None, _loaded_relationship(season_show, "media_item"))
            show_title = show_item.title if show_item is not None else show_title or item.title
            parent_ids = ParentIdsRecord(
                tmdb_id=season_show.tmdb_id
                or (parent_ids.tmdb_id if parent_ids is not None else None),
                tvdb_id=season_show.tvdb_id
                or (parent_ids.tvdb_id if parent_ids is not None else None),
            )
    elif episode is not None:
        item_type = "episode"
        tmdb_id = episode.tmdb_id or tmdb_id
        tvdb_id = episode.tvdb_id or tvdb_id
        imdb_id = episode.imdb_id or imdb_id
        episode_number = episode.episode_number or episode_number
        episode_season = cast(SeasonORM | None, _loaded_relationship(episode, "season"))
        if episode_season is not None:
            season_number = episode_season.season_number or season_number
            season_show = cast(ShowORM | None, _loaded_relationship(episode_season, "show"))
            if season_show is not None:
                show_item = cast(MediaItemORM | None, _loaded_relationship(season_show, "media_item"))
                show_title = show_item.title if show_item is not None else show_title or item.title
                parent_ids = ParentIdsRecord(
                    tmdb_id=season_show.tmdb_id
                    or (parent_ids.tmdb_id if parent_ids is not None else None),
                    tvdb_id=season_show.tvdb_id
                    or (parent_ids.tvdb_id if parent_ids is not None else None),
                )
        show_title = show_title or item.title
    elif item_type == "tv":
        item_type = "show"

    return MediaItemSpecializationRecord(
        item_type=item_type,
        tmdb_id=tmdb_id,
        tvdb_id=tvdb_id,
        imdb_id=imdb_id,
        parent_ids=parent_ids,
        show_title=show_title,
        season_number=season_number,
        episode_number=episode_number,
    )


def _projection_item_load_options() -> tuple[Any, ...]:
    """Return the eager-load policy for specialization-backed read models."""

    return (
        selectinload(MediaItemORM.movie),
        selectinload(MediaItemORM.show),
        selectinload(MediaItemORM.season)
        .selectinload(SeasonORM.show)
        .selectinload(ShowORM.media_item),
        selectinload(MediaItemORM.episode)
        .selectinload(EpisodeORM.season)
        .selectinload(SeasonORM.show)
        .selectinload(ShowORM.media_item),
    )


def _build_compatibility_metadata(
    attributes: dict[str, object],
    *,
    specialization: MediaItemSpecializationRecord,
) -> dict[str, object]:
    """Return compatibility metadata normalized to the persisted specialization seam."""

    metadata = dict(attributes)
    metadata["item_type"] = specialization.item_type
    if specialization.tmdb_id is not None:
        metadata["tmdb_id"] = specialization.tmdb_id
    if specialization.tvdb_id is not None:
        metadata["tvdb_id"] = specialization.tvdb_id
    if specialization.imdb_id is not None:
        metadata["imdb_id"] = specialization.imdb_id
    if specialization.parent_ids is not None:
        metadata["parent_ids"] = {
            "tmdb_id": specialization.parent_ids.tmdb_id,
            "tvdb_id": specialization.parent_ids.tvdb_id,
        }
    if specialization.show_title is not None:
        metadata["show_title"] = specialization.show_title
    if specialization.season_number is not None:
        metadata["season_number"] = specialization.season_number
    if specialization.episode_number is not None:
        metadata["episode_number"] = specialization.episode_number
    return metadata


def _build_summary_record(item: MediaItemORM, *, extended: bool) -> MediaItemSummaryRecord:
    """Map one ORM item into the current REST compatibility summary shape."""

    attributes = cast(dict[str, object], item.attributes or {})
    specialization = _build_specialization_record(item)
    metadata = (
        _build_compatibility_metadata(attributes, specialization=specialization)
        if extended
        else None
    )
    next_retry_at = _effective_next_retry_at(item.next_retry_at)
    return MediaItemSummaryRecord(
        id=item.id,
        type=specialization.item_type,
        title=item.title,
        state=_canonical_state_name(item.state),
        tmdb_id=specialization.tmdb_id,
        tvdb_id=specialization.tvdb_id,
        parent_ids=specialization.parent_ids,
        poster_path=_normalize_poster_path(_extract_string(attributes, "poster_path")),
        aired_at=_extract_string(attributes, "aired_at"),
        external_ref=item.external_ref,
        created_at=_serialize_datetime(item.created_at),
        updated_at=_serialize_datetime(item.updated_at),
        next_retry_at=_serialize_datetime(next_retry_at),
        recovery_attempt_count=int(item.recovery_attempt_count or 0),
        is_in_cooldown=_is_retry_cooldown_active(next_retry_at),
        metadata=metadata,
        specialization=specialization,
    )


def _build_consumer_activity_subtitle(summary: MediaItemSummaryRecord) -> str | None:
    """Return a compact subtitle for shared consumer activity surfaces."""

    metadata = summary.metadata or {}
    year_label: str | None = None
    if summary.aired_at:
        try:
            year_label = str(datetime.fromisoformat(summary.aired_at.replace("Z", "+00:00")).year)
        except ValueError:
            year_label = None

    if summary.type == "episode":
        show_title = _extract_first_string(
            cast(dict[str, object], metadata),
            "show_title",
            "series_title",
            "parent_title",
        )
        season_number = metadata.get("season_number")
        episode_number = metadata.get("episode_number")
        season_label = (
            f"S{int(season_number):02d}"
            if isinstance(season_number, int)
            else None
        )
        episode_label = (
            f"E{int(episode_number):02d}"
            if isinstance(episode_number, int)
            else None
        )
        return " • ".join(
            part
            for part in (
                show_title,
                " ".join(part for part in (season_label, episode_label) if part) or None,
                year_label,
            )
            if part
        ) or None

    if summary.type == "season":
        show_title = _extract_first_string(
            cast(dict[str, object], metadata),
            "show_title",
            "series_title",
            "parent_title",
        )
        return " • ".join(part for part in (show_title, year_label) if part) or None

    return year_label


def _build_consumer_activity_item_record(
    summary: MediaItemSummaryRecord,
    *,
    last_activity_at: datetime | None,
    last_viewed_at: datetime | None,
    last_launched_at: datetime | None,
    view_count: int,
    launch_count: int,
    session_count: int,
    active_session_count: int,
    last_session_key: str | None,
    resume_position_seconds: int | None,
    duration_seconds: int | None,
    progress_percent: float | None,
    completed: bool,
    last_target: str | None,
) -> ConsumerPlaybackActivityItemRecord:
    """Return one shared consumer activity row from one current item summary."""

    return ConsumerPlaybackActivityItemRecord(
        item_id=summary.id,
        title=summary.title,
        subtitle=_build_consumer_activity_subtitle(summary),
        poster_path=summary.poster_path,
        state=summary.state,
        request=summary.request,
        playback_ready=(
            (
                summary.resolved_playback is not None
                and (
                    summary.resolved_playback.direct_ready
                    or summary.resolved_playback.hls_ready
                )
            )
            or (
                summary.active_stream is not None
                and (
                    summary.active_stream.direct_ready
                    or summary.active_stream.hls_ready
                )
            )
            or any(
                bool(entry.lifecycle and entry.lifecycle.ready_for_playback)
                for entry in (summary.media_entries or [])
            )
        ),
        last_activity_at=_serialize_datetime(last_activity_at),
        last_viewed_at=_serialize_datetime(last_viewed_at),
        last_launched_at=_serialize_datetime(last_launched_at),
        view_count=view_count,
        launch_count=launch_count,
        session_count=session_count,
        active_session_count=active_session_count,
        last_session_key=last_session_key,
        resume_position_seconds=resume_position_seconds,
        duration_seconds=duration_seconds,
        progress_percent=progress_percent,
        completed=completed,
        last_target=last_target,
    )


def _serialize_datetime(value: datetime | None) -> str | None:
    """Return one optional datetime as an ISO-8601 string for compatibility responses."""

    if value is None:
        return None
    return value.isoformat()


def _coerce_activity_payload_str(
    payload: dict[str, object],
    key: str,
) -> str | None:
    """Return one normalized string field from an activity payload when present."""

    value = payload.get(key)
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return None


def _coerce_activity_payload_seconds(
    payload: dict[str, object],
    key: str,
) -> int | None:
    """Return one non-negative second count from an activity payload when present."""

    value = payload.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value.is_integer():
        integer_value = int(value)
        return integer_value if integer_value >= 0 else None
    return None


def _coerce_activity_payload_bool(
    payload: dict[str, object],
    key: str,
) -> bool | None:
    """Return one boolean field from an activity payload when present."""

    value = payload.get(key)
    return value if isinstance(value, bool) else None


def _build_progress_percent(
    position_seconds: int | None,
    duration_seconds: int | None,
    *,
    completed: bool,
) -> float | None:
    """Return one bounded progress percentage from retained playback counters."""

    if completed:
        return 100.0
    if position_seconds is None or duration_seconds is None or duration_seconds <= 0:
        return None
    return round(min(100.0, (position_seconds / duration_seconds) * 100.0), 1)


def _effective_next_retry_at(
    next_retry_at: datetime | None,
    *,
    reference_time: datetime | None = None,
) -> datetime | None:
    """Return one retry timestamp only while it still represents an active cooldown."""

    if next_retry_at is None:
        return None
    if next_retry_at <= (reference_time or datetime.now(UTC)):
        return None
    return next_retry_at


def _is_retry_cooldown_active(next_retry_at: datetime | None) -> bool:
    """Return whether one item currently has an active retry cooldown."""

    return next_retry_at is not None


def _failed_recovery_reason(*, has_scrape_candidates: bool, is_in_cooldown: bool) -> str:
    """Return the canonical recovery reason for one failed item."""

    if has_scrape_candidates:
        return "failed_retry_in_cooldown" if is_in_cooldown else "failed_cooldown_elapsed"
    return (
        "failed_retry_in_cooldown_no_scrape_candidates"
        if is_in_cooldown
        else "failed_cooldown_elapsed_no_scrape_candidates"
    )


def _build_recovery_plan_record(
    *,
    state: ItemState,
    next_retry_at: datetime | None = None,
    recovery_attempt_count: int = 0,
    has_scrape_candidates: bool | None = None,
    reference_time: datetime | None = None,
) -> RecoveryPlanRecord:
    """Build the intentional recovery plan for one persisted item state."""

    effective_next_retry_at = _effective_next_retry_at(
        next_retry_at, reference_time=reference_time
    )
    is_in_cooldown = _is_retry_cooldown_active(effective_next_retry_at)

    if state is ItemState.REQUESTED:
        return RecoveryPlanRecord(
            mechanism=RecoveryMechanism.ORPHAN_RECOVERY,
            target_stage=RecoveryTargetStage.INDEX,
            reason="orphaned_requested_item",
            next_retry_at=None,
            recovery_attempt_count=recovery_attempt_count,
            is_in_cooldown=False,
        )
    if state is ItemState.INDEXED:
        return RecoveryPlanRecord(
            mechanism=RecoveryMechanism.ORPHAN_RECOVERY,
            target_stage=RecoveryTargetStage.SCRAPE,
            reason="orphaned_indexed_item",
            next_retry_at=None,
            recovery_attempt_count=recovery_attempt_count,
            is_in_cooldown=False,
        )
    if state is ItemState.SCRAPED:
        return RecoveryPlanRecord(
            mechanism=RecoveryMechanism.ORPHAN_RECOVERY,
            target_stage=RecoveryTargetStage.PARSE,
            reason="orphaned_scraped_item",
            next_retry_at=None,
            recovery_attempt_count=recovery_attempt_count,
            is_in_cooldown=False,
        )
    if state is ItemState.DOWNLOADED:
        return RecoveryPlanRecord(
            mechanism=RecoveryMechanism.ORPHAN_RECOVERY,
            target_stage=RecoveryTargetStage.FINALIZE,
            reason="orphaned_downloaded_item",
            next_retry_at=None,
            recovery_attempt_count=recovery_attempt_count,
            is_in_cooldown=False,
        )
    if state is ItemState.FAILED:
        has_candidates = bool(has_scrape_candidates)
        return RecoveryPlanRecord(
            mechanism=RecoveryMechanism.COOLDOWN_RECOVERY,
            target_stage=(
                RecoveryTargetStage.PARSE if has_candidates else RecoveryTargetStage.SCRAPE
            ),
            reason=_failed_recovery_reason(
                has_scrape_candidates=has_candidates, is_in_cooldown=is_in_cooldown
            ),
            next_retry_at=_serialize_datetime(effective_next_retry_at),
            recovery_attempt_count=recovery_attempt_count,
            is_in_cooldown=is_in_cooldown,
        )
    return RecoveryPlanRecord(
        mechanism=RecoveryMechanism.NONE,
        target_stage=RecoveryTargetStage.NONE,
        reason="state_not_automatically_recoverable",
        next_retry_at=None,
        recovery_attempt_count=recovery_attempt_count,
        is_in_cooldown=False,
    )


def _clone_requested_seasons(value: list[int] | None) -> list[int] | None:
    """Return a detached copy of requested season numbers when present."""

    if value is None:
        return None
    return list(value)


def _clone_requested_episodes(
    value: dict[str, list[int]] | None,
) -> dict[str, list[int]] | None:
    """Return a detached copy of requested episode ranges when present."""

    if value is None:
        return None
    return {str(season): list(episodes) for season, episodes in value.items()}


def _normalize_request_source(value: str | None) -> str:
    """Keep request-source attribution non-empty and within the ORM column budget."""

    if value is None:
        return "api"
    normalized = value.strip()
    if not normalized:
        return "api"
    return normalized[:64]


def _build_item_request_summary_record(
    request_record: ItemRequestORM | None,
) -> ItemRequestSummaryRecord | None:
    """Map one persisted request-intent row into the item-detail summary shape."""

    if request_record is None:
        return None
    return ItemRequestSummaryRecord(
        is_partial=request_record.is_partial,
        requested_seasons=_clone_requested_seasons(request_record.requested_seasons),
        requested_episodes=_clone_requested_episodes(request_record.requested_episodes),
        request_source=_normalize_request_source(request_record.request_source),
    )


def _latest_item_request(item_requests: list[ItemRequestORM]) -> ItemRequestORM | None:
    """Return the latest request-intent row for one item when present."""

    if not item_requests:
        return None
    return max(
        item_requests,
        key=lambda request_record: (
            request_record.last_requested_at,
            request_record.created_at,
        ),
    )


def _basename_from_candidate(value: str | None) -> str | None:
    """Return one filename-like basename from a path/URL-ish string when possible."""

    if not value:
        return None
    candidate = value.rsplit("/", 1)[-1]
    candidate = candidate.rsplit("\\", 1)[-1]
    return candidate or None


def _build_playback_attachment_detail_record(
    attachment: PlaybackAttachmentORM,
) -> PlaybackAttachmentDetailRecord:
    """Map one persisted playback attachment into the current detail projection shape."""

    return PlaybackAttachmentDetailRecord(
        id=attachment.id,
        kind=attachment.kind,
        locator=attachment.locator,
        source_key=attachment.source_key,
        provider=attachment.provider,
        provider_download_id=attachment.provider_download_id,
        provider_file_id=attachment.provider_file_id,
        provider_file_path=attachment.provider_file_path,
        original_filename=attachment.original_filename,
        file_size=attachment.file_size,
        local_path=attachment.local_path,
        restricted_url=attachment.restricted_url,
        unrestricted_url=attachment.unrestricted_url,
        is_preferred=attachment.is_preferred,
        preference_rank=attachment.preference_rank,
        refresh_state=attachment.refresh_state,
        expires_at=_serialize_datetime(attachment.expires_at),
        last_refreshed_at=_serialize_datetime(attachment.last_refreshed_at),
        last_refresh_error=attachment.last_refresh_error,
    )


def _build_media_entry_detail_record(
    attachment: PlaybackAttachmentORM,
    *,
    active_for_direct: bool = False,
    active_for_hls: bool = False,
) -> MediaEntryDetailRecord:
    """Map one persisted playback attachment into a VFS-facing media-entry projection."""

    original_filename = attachment.original_filename or _basename_from_candidate(
        attachment.local_path or attachment.provider_file_path or attachment.locator
    )
    download_url = attachment.restricted_url
    unrestricted_url = attachment.unrestricted_url
    url = unrestricted_url or download_url
    lifecycle_snapshot = _build_attachment_direct_file_link_lifecycle_snapshot(attachment)
    lifecycle = _build_media_entry_lifecycle_record(
        owner_kind="attachment",
        owner_id=attachment.id,
        kind=attachment.kind,
        provider=attachment.provider,
        local_path=attachment.local_path,
        restricted_url=download_url,
        unrestricted_url=unrestricted_url,
        refresh_state=attachment.refresh_state,
        source_attachment_id=attachment.id,
        source_key=attachment.source_key,
        lifecycle_snapshot=lifecycle_snapshot,
        active_for_direct=active_for_direct,
        active_for_hls=active_for_hls,
    )
    return MediaEntryDetailRecord(
        entry_type="media",
        kind=attachment.kind,
        original_filename=original_filename,
        url=url,
        local_path=attachment.local_path,
        download_url=download_url,
        unrestricted_url=unrestricted_url,
        source_attachment_id=attachment.id,
        provider=attachment.provider,
        provider_download_id=attachment.provider_download_id,
        provider_file_id=attachment.provider_file_id,
        provider_file_path=attachment.provider_file_path,
        size=attachment.file_size,
        created=_serialize_datetime(attachment.created_at),
        modified=_serialize_datetime(attachment.updated_at),
        active_for_direct=active_for_direct,
        active_for_hls=active_for_hls,
        is_active_stream=active_for_direct or active_for_hls,
        lifecycle=lifecycle,
    )


def _build_persisted_media_entry_detail_record(
    entry: MediaEntryORM,
    *,
    active_for_direct: bool = False,
    active_for_hls: bool = False,
) -> MediaEntryDetailRecord:
    """Map one persisted media-entry record into the current VFS-facing detail shape."""

    original_filename = entry.original_filename or _basename_from_candidate(
        entry.local_path or entry.provider_file_path or entry.unrestricted_url or entry.download_url
    )
    url = entry.unrestricted_url or entry.download_url
    _, lifecycle_snapshot = PlaybackSourceService.build_media_entry_direct_file_link_snapshot(entry)
    lifecycle = _build_media_entry_lifecycle_record(
        owner_kind="media-entry",
        owner_id=entry.id,
        kind=entry.kind,
        provider=entry.provider or (entry.source_attachment.provider if entry.source_attachment else None),
        local_path=entry.local_path,
        restricted_url=entry.download_url,
        unrestricted_url=entry.unrestricted_url,
        refresh_state=entry.refresh_state,
        source_attachment_id=entry.source_attachment_id,
        source_key=entry.source_attachment.source_key if entry.source_attachment is not None else None,
        lifecycle_snapshot=lifecycle_snapshot,
        active_for_direct=active_for_direct,
        active_for_hls=active_for_hls,
    )
    return MediaEntryDetailRecord(
        entry_type=entry.entry_type,
        kind=entry.kind,
        original_filename=original_filename,
        url=url,
        local_path=entry.local_path,
        download_url=entry.download_url,
        unrestricted_url=entry.unrestricted_url,
        source_attachment_id=entry.source_attachment_id,
        provider=entry.provider,
        provider_download_id=entry.provider_download_id,
        provider_file_id=entry.provider_file_id,
        provider_file_path=entry.provider_file_path,
        size=entry.size_bytes,
        created=_serialize_datetime(entry.created_at),
        modified=_serialize_datetime(entry.updated_at),
        refresh_state=entry.refresh_state,
        expires_at=_serialize_datetime(entry.expires_at),
        last_refreshed_at=_serialize_datetime(entry.last_refreshed_at),
        last_refresh_error=entry.last_refresh_error,
        active_for_direct=active_for_direct,
        active_for_hls=active_for_hls,
        is_active_stream=active_for_direct or active_for_hls,
        lifecycle=lifecycle,
    )


def _build_subtitle_entry_detail_record(entry: SubtitleEntryORM) -> SubtitleEntryDetailRecord:
    """Map one persisted subtitle row into the current item-detail response shape."""

    return SubtitleEntryDetailRecord(
        id=entry.id,
        language=entry.language,
        format=entry.format,
        source=entry.source,
        url=entry.url,
        is_default=entry.is_default,
        is_forced=entry.is_forced,
    )


def _build_media_entry_active_roles(
    *,
    active_for_direct: bool,
    active_for_hls: bool,
) -> tuple[str, ...]:
    roles: list[str] = []
    if active_for_direct:
        roles.append("direct")
    if active_for_hls:
        roles.append("hls")
    return tuple(roles)


def _build_media_entry_lifecycle_record(
    *,
    owner_kind: str,
    owner_id: str | None,
    kind: str,
    provider: str | None,
    local_path: str | None,
    restricted_url: str | None,
    unrestricted_url: str | None,
    refresh_state: str,
    source_attachment_id: str | None,
    source_key: str | None,
    lifecycle_snapshot: DirectFileLinkLifecycleSnapshot | None,
    active_for_direct: bool,
    active_for_hls: bool,
) -> MediaEntryLifecycleRecord:
    locator = unrestricted_url or restricted_url or local_path or ""
    provider_family = (
        str(lifecycle_snapshot.provider_family)
        if lifecycle_snapshot is not None
        else PlaybackSourceService._classify_direct_file_provider_family(provider)
    )
    locator_source = (
        str(lifecycle_snapshot.locator_source)
        if lifecycle_snapshot is not None
        else PlaybackSourceService._classify_direct_file_locator_source(
            locator,
            local_path=local_path,
            unrestricted_url=unrestricted_url,
            restricted_url=restricted_url,
        )
    )
    normalized_refresh_state = (
        str(lifecycle_snapshot.refresh_state)
        if lifecycle_snapshot is not None and lifecycle_snapshot.refresh_state is not None
        else None
    )
    effective_refresh_state = PlaybackSourceService._effective_media_entry_refresh_state(
        normalized_refresh_state or refresh_state,
        provider=provider,
        restricted_url=restricted_url,
        unrestricted_url=unrestricted_url,
    )
    active_roles = _build_media_entry_active_roles(
        active_for_direct=active_for_direct,
        active_for_hls=active_for_hls,
    )
    ready_state = effective_refresh_state in _SATISFYING_MEDIA_ENTRY_REFRESH_STATES
    has_locator = bool(locator)
    ready_for_direct = has_locator and ready_state and kind in {"local-file", "remote-direct"}
    ready_for_hls = has_locator and ready_state and kind in {
        "local-file",
        "remote-direct",
        "remote-hls",
    }
    return MediaEntryLifecycleRecord(
        owner_kind=(
            str(lifecycle_snapshot.owner_kind) if lifecycle_snapshot is not None else owner_kind
        ),
        owner_id=lifecycle_snapshot.owner_id if lifecycle_snapshot is not None else owner_id,
        active_roles=active_roles,
        source_key=source_key,
        source_attachment_id=(
            lifecycle_snapshot.source_attachment_id
            if lifecycle_snapshot is not None
            else source_attachment_id
        ),
        provider_family=provider_family,
        locator_source=locator_source,
        match_basis=(
            lifecycle_snapshot.match_basis
            if lifecycle_snapshot is not None
            else ("source-attachment-id" if source_attachment_id is not None else None)
        ),
        restricted_fallback=(
            bool(lifecycle_snapshot.restricted_fallback)
            if lifecycle_snapshot is not None
            else bool(
                (source_key is not None and source_key.endswith(":restricted-fallback"))
                or (unrestricted_url is None and restricted_url is not None)
            )
        ),
        refresh_state=normalized_refresh_state,
        expires_at=(
            _serialize_datetime(lifecycle_snapshot.expires_at)
            if lifecycle_snapshot is not None
            else None
        ),
        last_refreshed_at=(
            _serialize_datetime(lifecycle_snapshot.last_refreshed_at)
            if lifecycle_snapshot is not None
            else None
        ),
        last_refresh_error=(
            lifecycle_snapshot.last_refresh_error if lifecycle_snapshot is not None else None
        ),
        effective_refresh_state=effective_refresh_state,
        ready_for_direct=ready_for_direct,
        ready_for_hls=ready_for_hls,
        ready_for_playback=ready_for_direct or ready_for_hls,
    )


def _build_attachment_direct_file_link_lifecycle_snapshot(
    attachment: PlaybackAttachmentORM,
) -> DirectFileLinkLifecycleSnapshot:
    """Return the owner-aligned lifecycle snapshot for one persisted attachment row."""

    resolved = PlaybackAttachment(
        kind=attachment.kind,
        locator=attachment.locator,
        source_key=attachment.source_key,
        provider=attachment.provider,
        provider_download_id=attachment.provider_download_id,
        provider_file_id=attachment.provider_file_id,
        provider_file_path=attachment.provider_file_path,
        original_filename=attachment.original_filename,
        file_size=attachment.file_size,
        local_path=attachment.local_path,
        restricted_url=attachment.restricted_url,
        unrestricted_url=attachment.unrestricted_url,
        expires_at=attachment.expires_at,
        refresh_state=PlaybackSourceService._normalize_refresh_state(attachment.refresh_state),
    )
    match_basis = PlaybackSourceService._match_basis_for_attachment_owner(attachment, resolved)
    return PlaybackSourceService._build_attachment_direct_file_link_lifecycle(
        attachment,
        attachment=resolved,
        match_basis=match_basis,
    )


def _providers_are_compatible(left: str | None, right: str | None) -> bool:
    """Return whether two optional provider identifiers can refer to the same source."""

    return left is None or right is None or left == right


def _matching_text(left: str | None, right: str | None) -> bool:
    """Return whether two optional strings match on a non-empty normalized value."""

    if left is None or right is None:
        return False
    assert left is not None
    assert right is not None
    left_text = left.strip()
    right_text = right.strip()
    return left_text != "" and left_text == right_text


def _matching_file_size(left: int | None, right: int | None) -> bool:
    """Return whether two optional file sizes match on a concrete numeric value."""

    return left is not None and right is not None and left == right


def _attachment_matches_resolved_attachment(
    attachment: PlaybackAttachmentORM,
    resolved: PlaybackAttachment,
) -> bool:
    """Return whether one persisted attachment owns the resolved playback candidate."""

    if not _providers_are_compatible(attachment.provider, resolved.provider):
        return False

    if _matching_text(attachment.provider_file_id, resolved.provider_file_id):
        return True
    if _matching_text(attachment.provider_file_path, resolved.provider_file_path):
        return True
    if _matching_text(attachment.local_path, resolved.local_path):
        return True
    if _matching_text(attachment.unrestricted_url, resolved.unrestricted_url):
        return True
    if _matching_text(attachment.restricted_url, resolved.restricted_url):
        return True
    if _matching_text(attachment.locator, resolved.locator):
        return True
    if _matching_text(
        attachment.original_filename, resolved.original_filename
    ) and _matching_file_size(attachment.file_size, resolved.file_size):
        return True
    return _matching_text(attachment.provider_download_id, resolved.provider_download_id) and (
        _matching_text(attachment.original_filename, resolved.original_filename)
        or _matching_text(attachment.provider_file_path, resolved.provider_file_path)
        or _matching_file_size(attachment.file_size, resolved.file_size)
    )


def _find_media_entry_owner_index(
    attachments: list[PlaybackAttachmentORM],
    resolved: PlaybackAttachment | None,
) -> int | None:
    """Return the media-entry index that owns one resolved playback attachment when available."""

    if resolved is None:
        return None
    for index, attachment in enumerate(attachments):
        if _attachment_matches_resolved_attachment(attachment, resolved):
            return index
    return None


def _find_attachment_owner(
    attachments: list[PlaybackAttachmentORM],
    resolved: PlaybackAttachment | None,
) -> PlaybackAttachmentORM | None:
    """Return the persisted playback attachment that owns one resolved candidate when available."""

    if resolved is None:
        return None
    for attachment in attachments:
        if _attachment_matches_resolved_attachment(attachment, resolved):
            return attachment
    return None


def _media_entry_matches_resolved_attachment(
    entry: MediaEntryORM,
    resolved: PlaybackAttachment,
    *,
    source_attachment_id: str | None,
) -> bool:
    """Return whether one persisted media entry owns the resolved playback candidate."""

    if source_attachment_id is not None and entry.source_attachment_id == source_attachment_id:
        return True
    if not _providers_are_compatible(entry.provider, resolved.provider):
        return False
    if _matching_text(entry.provider_file_id, resolved.provider_file_id):
        return True
    if _matching_text(entry.provider_file_path, resolved.provider_file_path):
        return True
    if _matching_text(entry.local_path, resolved.local_path):
        return True
    if _matching_text(entry.unrestricted_url, resolved.unrestricted_url):
        return True
    if _matching_text(entry.download_url, resolved.restricted_url):
        return True
    if _matching_text(entry.original_filename, resolved.original_filename) and _matching_file_size(
        entry.size_bytes, resolved.file_size
    ):
        return True
    return _matching_text(entry.provider_download_id, resolved.provider_download_id) and (
        _matching_text(entry.original_filename, resolved.original_filename)
        or _matching_text(entry.provider_file_path, resolved.provider_file_path)
        or _matching_file_size(entry.size_bytes, resolved.file_size)
    )


def _find_persisted_media_entry_owner_index(
    media_entries: list[MediaEntryORM],
    resolved: PlaybackAttachment | None,
    *,
    source_attachment_id: str | None,
) -> int | None:
    """Return the persisted media-entry index that owns one resolved playback attachment."""

    if resolved is None:
        return None
    for index, entry in enumerate(media_entries):
        if _media_entry_matches_resolved_attachment(
            entry,
            resolved,
            source_attachment_id=source_attachment_id,
        ):
            return index
    return None


def _find_persisted_active_stream_owner_index(
    media_entries: list[MediaEntryORM],
    active_streams: list[ActiveStreamORM],
    *,
    role: str,
) -> int | None:
    """Return the persisted media-entry index selected for one active-stream role."""

    if not media_entries:
        return None
    entry_indices = {entry.id: index for index, entry in enumerate(media_entries)}
    for active_stream in active_streams:
        if active_stream.role != role:
            continue
        return entry_indices.get(active_stream.media_entry_id)
    return None


def _build_active_stream_owner_record(
    attachment: PlaybackAttachmentORM,
    *,
    media_entry_index: int,
) -> ActiveStreamOwnerRecord:
    """Map one owning persisted attachment into an active-stream ownership record."""

    original_filename = attachment.original_filename or _basename_from_candidate(
        attachment.local_path or attachment.provider_file_path or attachment.locator
    )
    return ActiveStreamOwnerRecord(
        media_entry_index=media_entry_index,
        kind=attachment.kind,
        original_filename=original_filename,
        provider=attachment.provider,
        provider_download_id=attachment.provider_download_id,
        provider_file_id=attachment.provider_file_id,
        provider_file_path=attachment.provider_file_path,
    )


def _build_active_stream_detail_record(
    snapshot: PlaybackResolutionSnapshot,
    attachments: list[PlaybackAttachmentORM],
    *,
    direct_owner_index: int | None,
    hls_owner_index: int | None,
) -> ActiveStreamDetailRecord:
    """Map resolved playback plus media-entry ownership into an explicit active-stream view."""

    direct_owner = None
    if direct_owner_index is not None:
        direct_owner = _build_active_stream_owner_record(
            attachments[direct_owner_index],
            media_entry_index=direct_owner_index,
        )

    hls_owner = None
    if hls_owner_index is not None:
        hls_owner = _build_active_stream_owner_record(
            attachments[hls_owner_index],
            media_entry_index=hls_owner_index,
        )

    return ActiveStreamDetailRecord(
        direct_ready=snapshot.direct_ready,
        hls_ready=snapshot.hls_ready,
        missing_local_file=snapshot.missing_local_file,
        direct_owner=direct_owner,
        hls_owner=hls_owner,
    )


def _build_active_stream_owner_record_from_media_entry(
    entry: MediaEntryORM,
    *,
    media_entry_index: int,
) -> ActiveStreamOwnerRecord:
    """Map one persisted media entry into an active-stream ownership record."""

    original_filename = entry.original_filename or _basename_from_candidate(
        entry.local_path or entry.provider_file_path or entry.unrestricted_url or entry.download_url
    )
    return ActiveStreamOwnerRecord(
        media_entry_index=media_entry_index,
        kind=entry.kind,
        original_filename=original_filename,
        provider=entry.provider,
        provider_download_id=entry.provider_download_id,
        provider_file_id=entry.provider_file_id,
        provider_file_path=entry.provider_file_path,
    )


def _build_active_stream_detail_record_from_media_entries(
    snapshot: PlaybackResolutionSnapshot,
    media_entries: list[MediaEntryORM],
    *,
    direct_owner_index: int | None,
    hls_owner_index: int | None,
) -> ActiveStreamDetailRecord:
    """Map resolved playback plus persisted media-entry ownership into an active-stream view."""

    direct_owner = None
    if direct_owner_index is not None:
        direct_owner = _build_active_stream_owner_record_from_media_entry(
            media_entries[direct_owner_index],
            media_entry_index=direct_owner_index,
        )

    hls_owner = None
    if hls_owner_index is not None:
        hls_owner = _build_active_stream_owner_record_from_media_entry(
            media_entries[hls_owner_index],
            media_entry_index=hls_owner_index,
        )

    return ActiveStreamDetailRecord(
        direct_ready=snapshot.direct_ready,
        hls_ready=snapshot.hls_ready,
        missing_local_file=snapshot.missing_local_file,
        direct_owner=direct_owner,
        hls_owner=hls_owner,
    )


def _build_resolved_playback_attachment_record(
    attachment: PlaybackAttachment,
) -> ResolvedPlaybackAttachmentRecord:
    """Map one resolved playback attachment into the current detail snapshot shape."""

    return ResolvedPlaybackAttachmentRecord(
        kind=attachment.kind,
        locator=attachment.locator,
        source_key=attachment.source_key,
        provider=attachment.provider,
        provider_download_id=attachment.provider_download_id,
        provider_file_id=attachment.provider_file_id,
        provider_file_path=attachment.provider_file_path,
        original_filename=attachment.original_filename,
        file_size=attachment.file_size,
        local_path=attachment.local_path,
        restricted_url=attachment.restricted_url,
        unrestricted_url=attachment.unrestricted_url,
    )


def _build_resolved_playback_snapshot_record(
    snapshot: PlaybackResolutionSnapshot,
) -> ResolvedPlaybackSnapshotRecord:
    """Map one resolved playback snapshot into the current item-detail response shape."""

    direct = None
    if snapshot.direct is not None:
        direct = _build_resolved_playback_attachment_record(snapshot.direct)

    hls = None
    if snapshot.hls is not None:
        hls = _build_resolved_playback_attachment_record(snapshot.hls)

    return ResolvedPlaybackSnapshotRecord(
        direct=direct,
        hls=hls,
        direct_ready=snapshot.direct_ready,
        hls_ready=snapshot.hls_ready,
        missing_local_file=snapshot.missing_local_file,
    )


def _build_detail_record(
    item: MediaItemORM,
    *,
    extended: bool,
    playback_service: PlaybackSourceService | None = None,
) -> MediaItemSummaryRecord:
    """Map one ORM item into the detail response shape with playback attachment projections."""

    summary = _build_summary_record(item, extended=extended)
    request = _build_item_request_summary_record(_latest_item_request(item.item_requests))
    playback_attachments: list[PlaybackAttachmentDetailRecord] | None = None
    resolved_playback: ResolvedPlaybackSnapshotRecord | None = None
    active_stream: ActiveStreamDetailRecord | None = None
    media_entries: list[MediaEntryDetailRecord] | None = None
    if extended:
        ordered_attachments = sorted(
            item.playback_attachments,
            key=lambda attachment: (
                attachment.refresh_state == "failed",
                not attachment.is_preferred,
                attachment.preference_rank,
                attachment.created_at,
            ),
        )
        playback_attachments = [
            _build_playback_attachment_detail_record(attachment)
            for attachment in ordered_attachments
        ]
        snapshot: PlaybackResolutionSnapshot | None = None
        if playback_service is not None:
            snapshot = playback_service.build_resolution_snapshot(item)
            resolved_playback = _build_resolved_playback_snapshot_record(snapshot)

        direct_owner_index = None
        hls_owner_index = None
        persisted_media_entries = sorted(
            item.media_entries,
            key=lambda entry: (entry.created_at, entry.id),
        )
        persisted_active_streams = sorted(
            item.active_streams,
            key=lambda active_stream: (active_stream.created_at, active_stream.id),
        )
        if snapshot is not None:
            direct_owner_attachment = _find_attachment_owner(ordered_attachments, snapshot.direct)
            hls_owner_attachment = _find_attachment_owner(ordered_attachments, snapshot.hls)
            if persisted_media_entries:
                direct_owner_index = _find_persisted_active_stream_owner_index(
                    persisted_media_entries,
                    persisted_active_streams,
                    role="direct",
                )
                if direct_owner_index is None:
                    direct_owner_index = _find_persisted_media_entry_owner_index(
                        persisted_media_entries,
                        snapshot.direct,
                        source_attachment_id=(
                            direct_owner_attachment.id
                            if direct_owner_attachment is not None
                            else None
                        ),
                    )
                hls_owner_index = _find_persisted_active_stream_owner_index(
                    persisted_media_entries,
                    persisted_active_streams,
                    role="hls",
                )
                if hls_owner_index is None:
                    hls_owner_index = _find_persisted_media_entry_owner_index(
                        persisted_media_entries,
                        snapshot.hls,
                        source_attachment_id=(
                            hls_owner_attachment.id if hls_owner_attachment is not None else None
                        ),
                    )
                active_stream = _build_active_stream_detail_record_from_media_entries(
                    snapshot,
                    persisted_media_entries,
                    direct_owner_index=direct_owner_index,
                    hls_owner_index=hls_owner_index,
                )
            else:
                direct_owner_index = _find_media_entry_owner_index(
                    ordered_attachments,
                    snapshot.direct,
                )
                hls_owner_index = _find_media_entry_owner_index(
                    ordered_attachments,
                    snapshot.hls,
                )
                active_stream = _build_active_stream_detail_record(
                    snapshot,
                    ordered_attachments,
                    direct_owner_index=direct_owner_index,
                    hls_owner_index=hls_owner_index,
                )

        if persisted_media_entries:
            media_entries = [
                _build_persisted_media_entry_detail_record(
                    entry,
                    active_for_direct=index == direct_owner_index,
                    active_for_hls=index == hls_owner_index,
                )
                for index, entry in enumerate(persisted_media_entries)
            ]
        else:
            media_entries = [
                _build_media_entry_detail_record(
                    attachment,
                    active_for_direct=index == direct_owner_index,
                    active_for_hls=index == hls_owner_index,
                )
                for index, attachment in enumerate(ordered_attachments)
            ]

    subtitles = [
        _build_subtitle_entry_detail_record(entry)
        for entry in sorted(item.subtitle_entries, key=lambda entry: (entry.created_at, entry.id))
    ]

    covered_season_numbers = _resolve_covered_season_numbers(item, summary_type=summary.type)

    return MediaItemSummaryRecord(
        id=summary.id,
        type=summary.type,
        title=summary.title,
        state=summary.state,
        tmdb_id=summary.tmdb_id,
        tvdb_id=summary.tvdb_id,
        parent_ids=summary.parent_ids,
        poster_path=summary.poster_path,
        aired_at=summary.aired_at,
        external_ref=summary.external_ref,
        created_at=summary.created_at,
        updated_at=summary.updated_at,
        next_retry_at=summary.next_retry_at,
        recovery_attempt_count=summary.recovery_attempt_count,
        is_in_cooldown=summary.is_in_cooldown,
        metadata=summary.metadata,
        request=request,
        playback_attachments=playback_attachments,
        resolved_playback=resolved_playback,
        active_stream=active_stream,
        media_entries=media_entries,
        subtitles=subtitles,
        covered_season_numbers=covered_season_numbers,
    )


def _resolve_covered_season_numbers(
    item: MediaItemORM,
    *,
    summary_type: str,
) -> list[int] | None:
    """Return show season coverage, preferring persisted specialization hierarchy."""

    if summary_type not in {"show", "tv"}:
        return None

    if item.show is not None and item.show.seasons:
        persisted_seasons = sorted(
            {
                season.season_number
                for season in item.show.seasons
                if season.season_number is not None
            }
        )
        if persisted_seasons:
            return persisted_seasons

    inferred_seasons: set[int] = set()
    for entry in item.media_entries:
        if (
            getattr(entry, "refresh_state", None) not in _SATISFYING_MEDIA_ENTRY_REFRESH_STATES
            or getattr(entry, "entry_type", None) != "media"
        ):
            continue
        # Use range-aware inference so pack torrents named "S01-S04"
        # expand to [1,2,3,4] rather than just [1].
        for season_number in _infer_season_range_from_path(
            entry.provider_file_path or entry.original_filename
        ):
            inferred_seasons.add(season_number)
    if inferred_seasons:
        return sorted(inferred_seasons)
    return None


def _matches_item_type(item: MediaItemSummaryRecord, requested_types: list[str] | None) -> bool:
    """Return whether one item matches the requested media-type filter."""

    if not requested_types:
        return True

    item_aliases = _TYPE_ALIASES.get(item.type, {item.type})
    requested_aliases = {
        alias
        for requested_type in requested_types
        for alias in _TYPE_ALIASES.get(
            _canonical_item_type_name(requested_type),
            {_canonical_item_type_name(requested_type)},
        )
    }
    return not item_aliases.isdisjoint(requested_aliases)


def _matches_state(item: MediaItemSummaryRecord, requested_states: list[str] | None) -> bool:
    """Return whether one item matches the requested state filter."""

    if requested_states is None:
        return True
    if "All" in requested_states:
        return True
    if item.state is None:
        return "Unknown" in requested_states
    return item.state in requested_states


def _matches_search(item: MediaItemSummaryRecord, search: str | None) -> bool:
    """Return whether one item matches the current search term."""

    if not search:
        return True
    needle = search.casefold().strip()
    if not needle:
        return True
    specialization = item.specialization
    searchable = [
        item.title,
        item.external_ref,
        item.tmdb_id,
        item.tvdb_id,
        specialization.show_title if specialization is not None else None,
        specialization.imdb_id if specialization is not None else None,
    ]
    return any(value is not None and needle in value.casefold() for value in searchable)


def _sort_items(
    items: list[MediaItemSummaryRecord],
    sort: list[str] | None,
    *,
    search: str | None = None,
) -> list[MediaItemSummaryRecord]:
    """Return items sorted using the first compatible frontend sort directive."""

    if not sort:
        return list(items)

    directive = sort[0]
    if directive == "relevance":
        needle = (search or "").casefold().strip()

        def _relevance_score(item: MediaItemSummaryRecord) -> tuple[int, int, str]:
            title = item.title.casefold()
            subtitle = (item.specialization.show_title or "").casefold()
            if not needle:
                score = 0
            elif title == needle:
                score = 4
            elif title.startswith(needle):
                score = 3
            elif needle in title:
                score = 2
            elif needle in subtitle:
                score = 1
            else:
                score = 0
            return (score, int(bool(item.aired_at)), item.title.casefold())

        return sorted(
            items,
            key=lambda item: _relevance_score(item),
            reverse=True,
        )
    if directive == "title_asc":
        return sorted(items, key=lambda item: item.title.casefold())
    if directive == "title_desc":
        return sorted(items, key=lambda item: item.title.casefold(), reverse=True)
    if directive == "state_asc":
        return sorted(items, key=lambda item: (item.state.casefold(), item.title.casefold()))
    if directive == "date_asc":
        return sorted(items, key=lambda item: item.aired_at or "")
    if directive == "date_desc":
        return sorted(items, key=lambda item: item.aired_at or "", reverse=True)
    if directive == "year_desc":
        return sorted(
            items,
            key=lambda item: ((item.aired_at or "")[:4], item.title.casefold()),
            reverse=True,
        )
    return list(items)


def _normalized_playback_filter_value(value: str | None) -> str:
    """Return a trimmed lowercase filter value for playback recovery matching."""

    return value.casefold().strip() if isinstance(value, str) else ""


def _collect_playback_recovery_providers(item: MediaItemSummaryRecord) -> list[str]:
    """Return all provider labels visible on one playback recovery detail row."""

    providers = [
        *(attachment.provider for attachment in item.playback_attachments or []),
        *(entry.provider for entry in item.media_entries or []),
        (
            item.resolved_playback.direct.provider
            if item.resolved_playback is not None and item.resolved_playback.direct is not None
            else None
        ),
        (
            item.resolved_playback.hls.provider
            if item.resolved_playback is not None and item.resolved_playback.hls is not None
            else None
        ),
        (
            item.active_stream.direct_owner.provider
            if item.active_stream is not None and item.active_stream.direct_owner is not None
            else None
        ),
        (
            item.active_stream.hls_owner.provider
            if item.active_stream is not None and item.active_stream.hls_owner is not None
            else None
        ),
    ]
    return [
        provider.strip()
        for provider in providers
        if isinstance(provider, str) and provider.strip()
    ]


def _matches_playback_recovery_text_query(
    item: MediaItemSummaryRecord,
    query: str | None,
) -> bool:
    """Return whether one detail row matches the current playback text query."""

    needle = _normalized_playback_filter_value(query)
    if not needle:
        return True
    searchable = [
        item.title,
        item.external_ref,
        item.state,
        item.type,
        item.tmdb_id,
        item.tvdb_id,
        (
            item.specialization.show_title
            if item.specialization is not None
            else None
        ),
        (
            item.request.request_source
            if item.request is not None
            else None
        ),
        *(
            attachment.original_filename
            for attachment in item.playback_attachments or []
        ),
        *(attachment.provider for attachment in item.playback_attachments or []),
        *(attachment.locator for attachment in item.playback_attachments or []),
        *(attachment.refresh_state for attachment in item.playback_attachments or []),
        *(
            attachment.last_refresh_error
            for attachment in item.playback_attachments or []
        ),
        *(entry.original_filename for entry in item.media_entries or []),
        *(entry.provider for entry in item.media_entries or []),
        *(entry.local_path for entry in item.media_entries or []),
        *(entry.url for entry in item.media_entries or []),
        *(entry.download_url for entry in item.media_entries or []),
        *(entry.unrestricted_url for entry in item.media_entries or []),
        *(entry.refresh_state for entry in item.media_entries or []),
        *(entry.last_refresh_error for entry in item.media_entries or []),
        (
            item.recovery_plan.reason
            if item.recovery_plan is not None
            else None
        ),
        (
            item.selected_stream.parsed_title
            if item.selected_stream is not None
            else None
        ),
        (
            item.selected_stream.raw_title
            if item.selected_stream is not None
            else None
        ),
    ]
    haystack = " ".join(
        value for value in searchable if isinstance(value, str) and value.strip()
    ).casefold()
    return needle in haystack


def _matches_playback_recovery_provider(
    item: MediaItemSummaryRecord,
    provider: str | None,
) -> bool:
    """Return whether one detail row matches the current playback provider filter."""

    expected = _normalized_playback_filter_value(provider)
    if not expected:
        return True
    return expected in {
        _normalized_playback_filter_value(current)
        for current in _collect_playback_recovery_providers(item)
    }


def _matches_playback_recovery_attachment_state(
    item: MediaItemSummaryRecord,
    attachment_state: str | None,
) -> bool:
    """Return whether one detail row has a matching attachment refresh state."""

    expected = _normalized_playback_filter_value(attachment_state)
    if not expected:
        return True
    return expected in {
        _normalized_playback_filter_value(attachment.refresh_state)
        for attachment in item.playback_attachments or []
    }


def _matches_playback_recovery_stream(
    item: MediaItemSummaryRecord,
    stream: str | None,
) -> bool:
    """Return whether one detail row matches the current playback stream posture."""

    if stream is None:
        return True
    if stream == "direct_ready":
        return bool(
            (item.resolved_playback is not None and item.resolved_playback.direct_ready)
            or (item.active_stream is not None and item.active_stream.direct_ready)
        )
    if stream == "hls_ready":
        return bool(
            (item.resolved_playback is not None and item.resolved_playback.hls_ready)
            or (item.active_stream is not None and item.active_stream.hls_ready)
        )
    if stream == "missing_local_file":
        return bool(
            (item.resolved_playback is not None and item.resolved_playback.missing_local_file)
            or (item.active_stream is not None and item.active_stream.missing_local_file)
        )
    return True


def _item_has_playback_errors(item: MediaItemSummaryRecord) -> bool:
    """Return whether one detail row surfaces any playback-facing failure signal."""

    normalized_state = _normalized_playback_filter_value(item.state)
    return (
        "fail" in normalized_state
        or "error" in normalized_state
        or any(
            bool(attachment.last_refresh_error)
            for attachment in item.playback_attachments or []
        )
        or any(bool(entry.last_refresh_error) for entry in item.media_entries or [])
    )


def _playback_recovery_priority_rank(item: MediaItemSummaryRecord) -> int:
    """Return the Director-compatible recovery priority rank for one detail row."""

    normalized_state = _normalized_playback_filter_value(item.state)
    if _item_has_playback_errors(item):
        return 0
    if item.recovery_plan is not None and item.recovery_plan.is_in_cooldown:
        return 1
    if "queued" in normalized_state:
        return 2
    if "final" in normalized_state:
        return 4
    return 3


def _sort_playback_recovery_items(
    items: list[MediaItemSummaryRecord],
    sort: str | None,
) -> list[MediaItemSummaryRecord]:
    """Return playback recovery items sorted with Director-compatible ordering."""

    if sort == "updated_asc":
        return sorted(items, key=lambda item: item.updated_at or "")
    if sort == "updated_desc":
        return sorted(items, key=lambda item: item.updated_at or "", reverse=True)
    if sort == "title_asc":
        return sorted(items, key=lambda item: item.title.casefold())
    return sorted(
        items,
        key=lambda item: (
            _playback_recovery_priority_rank(item),
            -(
                item.recovery_plan.recovery_attempt_count
                if item.recovery_plan is not None
                else 0
            ),
            -(datetime.fromisoformat((item.updated_at or "1970-01-01T00:00:00+00:00").replace("Z", "+00:00")).timestamp()),
        ),
    )


def _extract_int(attributes: dict[str, object], key: str) -> int | None:
    """Return an integer metadata value when present and usable."""

    value = attributes.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _coerce_part_value(value: object) -> int | None:
    """Return one parsed multipart ``part`` number when present and numeric."""

    if isinstance(value, int):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if normalized.isdigit():
            return int(normalized)
    return None


def _part_tiebreaker(parsed_title: dict[str, object]) -> int:
    """Return deterministic multipart tie-break score (prefer lower part first)."""

    part = _coerce_part_value(parsed_title.get("part"))
    if part is None:
        return 0
    return part


@dataclass(frozen=True)
class MediaItemRecord:
    """Service-level media item representation."""

    id: str
    external_ref: str
    title: str
    state: ItemState
    tenant_id: str = "global"
    attributes: dict[str, object] = field(default_factory=dict)
    has_media_entries: bool = False


def _build_media_item_record_from_orm(item: MediaItemORM) -> MediaItemRecord:
    """Return a detached service record safe to use outside the ORM session."""

    media_entries_state = item.__dict__.get("media_entries", NO_VALUE)
    # Never trigger relationship lazy-loads here. This helper is used from async
    # service methods that may run outside SQLAlchemy's greenlet context, where
    # touching an unloaded relationship can raise MissingGreenlet.
    has_media_entries = bool(media_entries_state) if media_entries_state is not NO_VALUE else False

    return MediaItemRecord(
        id=item.id,
        external_ref=item.external_ref,
        title=item.title,
        state=ItemState(item.state),
        tenant_id=item.tenant_id,
        attributes=dict(cast(dict[str, object], item.attributes or {})),
        has_media_entries=has_media_entries,
    )


@dataclass(frozen=True)
class RequestTimeMetadataRecord:
    """Normalized metadata payload applied to a requested item after enrichment."""

    title: str
    attributes: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class EnrichmentResult:
    """Structured metadata-enrichment diagnostics for request and scrape callers."""

    source: str
    has_poster: bool
    has_imdb_id: bool
    has_tmdb_id: bool
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RequestMetadataResolution:
    """Metadata payload bundled with enrichment diagnostics."""

    metadata: RequestTimeMetadataRecord | None
    enrichment: EnrichmentResult


@dataclass(frozen=True)
class RequestItemServiceResult:
    """Created item plus enrichment diagnostics for additive GraphQL consumers."""

    item: MediaItemRecord
    enrichment: EnrichmentResult


@dataclass(frozen=True)
class RequestSearchLifecycleRecord:
    """Live intake lifecycle detail attached to one request-search hit."""

    stage_name: str | None = None
    stage_status: str | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    last_error: str | None = None
    updated_at: str | None = None
    recovery_reason: str | None = None
    retry_at: str | None = None
    recovery_attempt_count: int = 0
    in_cooldown: bool = False


@dataclass(frozen=True)
class RequestSearchCandidateRecord:
    """One TMDB-backed request search hit with current library/request state."""

    external_ref: str
    title: str
    media_type: str
    tmdb_id: str | None = None
    tvdb_id: str | None = None
    imdb_id: str | None = None
    poster_path: str | None = None
    overview: str = ""
    year: int | None = None
    is_requested: bool = False
    requested_item_id: str | None = None
    requested_state: str | None = None
    requested_seasons: list[int] | None = None
    requested_episodes: dict[str, list[int]] | None = None
    request_source: str | None = None
    request_count: int = 0
    first_requested_at: str | None = None
    last_requested_at: str | None = None
    lifecycle: RequestSearchLifecycleRecord | None = None
    ranking_signals: tuple[str, ...] = ()
    season_summary: RequestCandidateSeasonSummaryRecord | None = None
    season_preview: tuple[RequestCandidateSeasonRecord, ...] = ()


@dataclass(frozen=True)
class RequestCandidateSeasonRecord:
    """One show season preview row for the focused requester detail route."""

    season_number: int
    title: str | None = None
    episode_count: int | None = None
    air_date: str | None = None
    is_released: bool = True
    has_local_coverage: bool = False
    is_requested: bool = False
    requested_episode_count: int = 0
    requested_all_episodes: bool = False
    status: str = "available"


@dataclass(frozen=True)
class RequestCandidateSeasonSummaryRecord:
    """Aggregated show-season request posture for requester detail routes."""

    total_seasons: int = 0
    released_seasons: int = 0
    requested_seasons: int = 0
    partial_seasons: int = 0
    local_seasons: int = 0
    unreleased_seasons: int = 0
    next_air_date: str | None = None


@dataclass(frozen=True)
class RequestSearchLocalSignalRecord:
    """Detached local-demand snapshot used to rank and decorate discovery hits."""

    item: MediaItemRecord | None = None
    request_source: str | None = None
    request_count: int = 0
    first_requested_at: str | None = None
    last_requested_at: str | None = None
    requested_seasons: list[int] | None = None
    requested_episodes: dict[str, list[int]] | None = None
    launch_count: int = 0
    view_count: int = 0
    session_count: int = 0
    active_session_count: int = 0
    completed_session_count: int = 0
    resume_position_seconds: int | None = None
    ranking_boost: float = 0.0
    ranking_signals: tuple[str, ...] = ()


@dataclass(frozen=True)
class RequestDiscoveryRailRecord:
    """One backend-owned zero-query discovery rail for consumer search."""

    rail_id: str
    title: str
    description: str
    query: str
    media_type: str
    items: list[RequestSearchCandidateRecord]


@dataclass(frozen=True)
class RequestEditorialFamilyRecord:
    """One backend-owned editorial discovery family for consumer search."""

    family_id: str
    title: str
    description: str
    family: str
    media_type: str
    items: list[RequestSearchCandidateRecord]


@dataclass(frozen=True)
class RequestReleaseWindowRecord:
    """One backend-owned release-window family for consumer search."""

    window_id: str
    title: str
    description: str
    window: str
    media_type: str
    items: list[RequestSearchCandidateRecord]


@dataclass(frozen=True)
class RequestDiscoveryProjectionActionRecord:
    """One follow-up discovery action emitted from the current backend window."""

    kind: str
    value: str
    media_type: str | None = None


@dataclass(frozen=True)
class RequestDiscoveryProjectionItemRecord:
    """One grouped discovery pivot derived from the current backend window."""

    projection_id: str
    label: str
    projection_type: str
    match_count: int
    image_path: str | None
    action: RequestDiscoveryProjectionActionRecord
    sample_titles: tuple[str, ...] = ()
    local_match_count: int = 0
    requested_match_count: int = 0
    active_match_count: int = 0
    completed_match_count: int = 0
    preview_signals: tuple[str, ...] = ()


@dataclass(frozen=True)
class RequestDiscoveryProjectionGroupRecord:
    """One grouped discovery-projection section for people, companies, and franchises."""

    group_id: str
    title: str
    description: str
    projection_type: str
    items: list[RequestDiscoveryProjectionItemRecord]


@dataclass(frozen=True)
class RequestSearchPageRecord:
    """One paginated backend-ranked request-search window."""

    items: list[RequestSearchCandidateRecord]
    offset: int
    limit: int
    total_count: int
    has_previous_page: bool
    has_next_page: bool
    result_window_complete: bool


@dataclass(frozen=True)
class RequestDiscoveryFacetBucketRecord:
    """One selectable discovery facet bucket computed from the backend result window."""

    value: str
    label: str
    count: int
    selected: bool = False


@dataclass(frozen=True)
class RequestDiscoverySortOptionRecord:
    """One supported discovery sort choice exposed to GraphQL consumers."""

    value: str
    label: str
    selected: bool = False


@dataclass(frozen=True)
class RequestDiscoveryFacetSetRecord:
    """Facet metadata derived from the backend-owned discovery window."""

    genres: tuple[RequestDiscoveryFacetBucketRecord, ...] = ()
    release_years: tuple[RequestDiscoveryFacetBucketRecord, ...] = ()
    languages: tuple[RequestDiscoveryFacetBucketRecord, ...] = ()
    companies: tuple[RequestDiscoveryFacetBucketRecord, ...] = ()
    networks: tuple[RequestDiscoveryFacetBucketRecord, ...] = ()
    sorts: tuple[RequestDiscoverySortOptionRecord, ...] = ()


@dataclass(frozen=True)
class RequestDiscoveryPageRecord:
    """One paginated backend-owned discovery page with additive facet metadata."""

    items: list[RequestSearchCandidateRecord]
    offset: int
    limit: int
    total_count: int
    has_previous_page: bool
    has_next_page: bool
    result_window_complete: bool
    facets: RequestDiscoveryFacetSetRecord


@dataclass(frozen=True)
class ConsumerPlaybackActivityItemRecord:
    """One consumer activity row grouped by item for shared playback history surfaces."""

    item_id: str
    title: str
    subtitle: str | None = None
    poster_path: str | None = None
    state: str | None = None
    request: ItemRequestSummaryRecord | None = None
    playback_ready: bool = False
    last_activity_at: str | None = None
    last_viewed_at: str | None = None
    last_launched_at: str | None = None
    view_count: int = 0
    launch_count: int = 0
    session_count: int = 0
    active_session_count: int = 0
    last_session_key: str | None = None
    resume_position_seconds: int | None = None
    duration_seconds: int | None = None
    progress_percent: float | None = None
    completed: bool = False
    last_target: str | None = None


@dataclass(frozen=True)
class ConsumerPlaybackDeviceRecord:
    """One recent device bucket derived from retained consumer activity events."""

    device_key: str
    device_label: str
    last_seen_at: str
    last_activity_at: str | None = None
    last_viewed_at: str | None = None
    last_launched_at: str | None = None
    launch_count: int = 0
    view_count: int = 0
    session_count: int = 0
    active_session_count: int = 0
    last_session_key: str | None = None
    resume_position_seconds: int | None = None
    duration_seconds: int | None = None
    progress_percent: float | None = None
    completed_session_count: int = 0
    last_target: str | None = None


@dataclass(frozen=True)
class ConsumerPlaybackSessionRecord:
    """One retained consumer playback session rolled up across shared activity events."""

    session_key: str
    item_id: str
    device_key: str
    device_label: str
    started_at: str
    last_seen_at: str
    last_target: str | None = None
    active: bool = False
    resume_position_seconds: int | None = None
    duration_seconds: int | None = None
    progress_percent: float | None = None
    completed: bool = False


@dataclass(frozen=True)
class ConsumerPlaybackActivityRecord:
    """Shared consumer activity snapshot for continue-watching, watch, and account surfaces."""

    generated_at: str
    total_item_count: int = 0
    total_view_count: int = 0
    total_launch_count: int = 0
    total_session_count: int = 0
    active_session_count: int = 0
    items: tuple[ConsumerPlaybackActivityItemRecord, ...] = ()
    devices: tuple[ConsumerPlaybackDeviceRecord, ...] = ()
    recent_sessions: tuple[ConsumerPlaybackSessionRecord, ...] = ()


@dataclass(frozen=True)
class LibraryRecoveryRecord:
    """One recovered or permanently-skipped library item."""

    item_id: str
    previous_state: ItemState
    reason: str
    recovery_attempt_count: int
    re_enqueued: bool


@dataclass(frozen=True)
class LibraryRecoverySnapshot:
    """Summary of one retry-library recovery scan."""

    recovered: list[LibraryRecoveryRecord]
    permanently_failed: list[LibraryRecoveryRecord]


class RecoveryMechanism(enum.StrEnum):
    """Intentional recovery mechanism independent of cron implementation details."""

    NONE = "none"
    ORPHAN_RECOVERY = "orphan_recovery"
    COOLDOWN_RECOVERY = "cooldown_recovery"


class RecoveryTargetStage(enum.StrEnum):
    """Pipeline stage that should run next when automatic recovery applies."""

    NONE = "none"
    INDEX = "index"
    SCRAPE = "scrape"
    PARSE = "parse"
    FINALIZE = "finalize"


class WorkflowCheckpointStatus(enum.StrEnum):
    """Durable status for one persisted item-workflow checkpoint."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class WorkflowResumeStage(enum.StrEnum):
    """Next resumable worker stage named in one checkpoint record."""

    NONE = "none"
    DEBRID = "debrid_item"
    FINALIZE = "finalize_item"


@dataclass(frozen=True)
class RecoveryPlanRecord:
    """Intentional recovery plan exposed to GraphQL and shared worker logic."""

    mechanism: RecoveryMechanism
    target_stage: RecoveryTargetStage
    reason: str
    next_retry_at: str | None
    recovery_attempt_count: int
    is_in_cooldown: bool


@dataclass(frozen=True)
class WorkflowCheckpointRecord:
    """Detached durable workflow checkpoint representation for worker orchestration."""

    workflow_name: str
    stage_name: str
    resume_stage: WorkflowResumeStage
    status: WorkflowCheckpointStatus
    item_request_id: str | None = None
    selected_stream_id: str | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    checkpoint_payload: dict[str, object] = field(default_factory=dict)
    compensation_payload: dict[str, object] = field(default_factory=dict)
    last_error: str | None = None
    updated_at: str | None = None


@dataclass(frozen=True)
class WorkflowDrillCandidateRecord:
    """Detached workflow checkpoint plus current recovery posture for drill execution."""

    media_item_id: str
    tenant_id: str | None
    item_state: ItemState
    recovery_plan: RecoveryPlanRecord
    checkpoint: WorkflowCheckpointRecord


def _build_request_search_lifecycle_record(
    *,
    checkpoint: WorkflowCheckpointRecord | None,
    recovery_plan: RecoveryPlanRecord | None,
) -> RequestSearchLifecycleRecord | None:
    """Merge checkpoint and recovery posture into one request-search lifecycle summary."""

    if checkpoint is None and recovery_plan is None:
        return None

    return RequestSearchLifecycleRecord(
        stage_name=(checkpoint.stage_name if checkpoint is not None else None),
        stage_status=(
            checkpoint.status.value if checkpoint is not None else None
        ),
        provider=(checkpoint.provider if checkpoint is not None else None),
        provider_download_id=(
            checkpoint.provider_download_id if checkpoint is not None else None
        ),
        last_error=(checkpoint.last_error if checkpoint is not None else None),
        updated_at=(checkpoint.updated_at if checkpoint is not None else None),
        recovery_reason=(
            recovery_plan.reason if recovery_plan is not None else None
        ),
        retry_at=(
            recovery_plan.next_retry_at if recovery_plan is not None else None
        ),
        recovery_attempt_count=(
            recovery_plan.recovery_attempt_count if recovery_plan is not None else 0
        ),
        in_cooldown=(
            recovery_plan.is_in_cooldown if recovery_plan is not None else False
        ),
    )


@dataclass(frozen=True)
class OutboxPublishSnapshot:
    """Summary of one outbox publication pass."""

    published_count: int
    failed_count: int


def _build_state_changed_payload(
    *,
    item_id: str,
    state: ItemState,
    event: ItemEvent,
    message: str | None,
) -> dict[str, object]:
    """Build the canonical state-change payload for the outbox and event bus."""

    return {
        "item_id": item_id,
        "state": state.value,
        "event": event.value,
        "message": message or "",
    }


def _latest_failed_at(item: MediaItemORM) -> datetime:
    """Return the most recent failure timestamp for one item, falling back safely."""

    failed_events = [
        event.created_at for event in item.events if event.next_state == ItemState.FAILED.value
    ]
    if failed_events:
        return max(failed_events)
    return item.updated_at


@dataclass(frozen=True)
class StatsYearReleaseRecord:
    """Release-year aggregate used by dashboard/statistics views."""

    year: int | None
    count: int


@dataclass(frozen=True)
class MediaStatsSnapshot:
    """Aggregated dashboard statistics derived from persisted media items."""

    total_items: int
    total_movies: int
    total_shows: int
    total_seasons: int
    total_episodes: int
    total_symlinks: int
    incomplete_items: int
    states: dict[str, int]
    activity: dict[str, int]
    media_year_releases: list[StatsYearReleaseRecord]


@dataclass(frozen=True)
class StatsProjection:
    """First-class stats query projection backed by persisted domain rows."""

    total_items: int
    completed_items: int
    failed_items: int
    incomplete_items: int
    movies: int
    shows: int
    episodes: int
    seasons: int = 0
    states: dict[str, int] = field(default_factory=dict)
    activity: dict[str, int] = field(default_factory=dict)
    media_year_releases: list[StatsYearReleaseRecord] = field(default_factory=list)


@dataclass(frozen=True)
class ParentIdsRecord:
    """Parent identifier bundle for season/episode navigation compatibility."""

    tmdb_id: str | None = None
    tvdb_id: str | None = None


@dataclass(frozen=True)
class ItemRequestSummaryRecord:
    """Latest persisted request-intent summary attached to item-detail responses."""

    is_partial: bool
    requested_seasons: list[int] | None = None
    requested_episodes: dict[str, list[int]] | None = None
    request_source: str = "api"


@dataclass(frozen=True)
class MediaItemSummaryRecord:
    """List/detail summary record used by the current REST compatibility surface."""

    id: str
    type: str
    title: str
    state: str | None = None
    tmdb_id: str | None = None
    tvdb_id: str | None = None
    parent_ids: ParentIdsRecord | None = None
    poster_path: str | None = None
    aired_at: str | None = None
    external_ref: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    next_retry_at: str | None = None
    recovery_attempt_count: int = 0
    is_in_cooldown: bool = False
    specialization: MediaItemSpecializationRecord | None = None
    metadata: dict[str, object] | None = None
    request: ItemRequestSummaryRecord | None = None
    playback_attachments: list[PlaybackAttachmentDetailRecord] | None = None
    resolved_playback: ResolvedPlaybackSnapshotRecord | None = None
    active_stream: ActiveStreamDetailRecord | None = None
    media_entries: list[MediaEntryDetailRecord] | None = None
    subtitles: list[SubtitleEntryDetailRecord] = field(default_factory=list)
    # Season numbers inferred from media-entry file paths for show-type items;
    # populated regardless of `extended` so the frontend season-selector can
    # always determine which seasons are already installed.
    covered_season_numbers: list[int] | None = None


@dataclass(frozen=True)
class PlaybackAttachmentDetailRecord:
    """Persisted playback attachment projection for item-detail compatibility responses."""

    id: str
    kind: str
    locator: str
    source_key: str | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    original_filename: str | None = None
    file_size: int | None = None
    local_path: str | None = None
    restricted_url: str | None = None
    unrestricted_url: str | None = None
    is_preferred: bool = False
    preference_rank: int = 100
    refresh_state: str = "ready"
    expires_at: str | None = None
    last_refreshed_at: str | None = None
    last_refresh_error: str | None = None


@dataclass(frozen=True)
class ResolvedPlaybackAttachmentRecord:
    """Best-current resolved playback attachment snapshot for item-detail responses."""

    kind: str
    locator: str
    source_key: str
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    original_filename: str | None = None
    file_size: int | None = None
    local_path: str | None = None
    restricted_url: str | None = None
    unrestricted_url: str | None = None


@dataclass(frozen=True)
class ResolvedPlaybackSnapshotRecord:
    """Best-current direct/HLS playback availability snapshot for one item."""

    direct: ResolvedPlaybackAttachmentRecord | None = None
    hls: ResolvedPlaybackAttachmentRecord | None = None
    direct_ready: bool = False
    hls_ready: bool = False
    missing_local_file: bool = False


@dataclass(frozen=True)
class ActiveStreamOwnerRecord:
    """Ownership link from one resolved active stream to one projected media entry."""

    media_entry_index: int
    kind: str
    original_filename: str | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None


@dataclass(frozen=True)
class ActiveStreamDetailRecord:
    """Explicit active-stream readiness and ownership view layered above detail projections."""

    direct_ready: bool = False
    hls_ready: bool = False
    missing_local_file: bool = False
    direct_owner: ActiveStreamOwnerRecord | None = None
    hls_owner: ActiveStreamOwnerRecord | None = None


@dataclass(frozen=True)
class MediaEntryLifecycleRecord:
    """Expanded lifecycle view for one item-detail media-entry row."""

    owner_kind: str
    owner_id: str | None = None
    active_roles: tuple[str, ...] = ()
    source_key: str | None = None
    source_attachment_id: str | None = None
    provider_family: str = "none"
    locator_source: str = "locator"
    match_basis: str | None = None
    restricted_fallback: bool = False
    refresh_state: str | None = None
    expires_at: str | None = None
    last_refreshed_at: str | None = None
    last_refresh_error: str | None = None
    effective_refresh_state: str = "unknown"
    ready_for_direct: bool = False
    ready_for_hls: bool = False
    ready_for_playback: bool = False


@dataclass(frozen=True)
class MediaEntryDetailRecord:
    """VFS-facing media-entry projection derived from the current persisted playback attachment layer."""

    entry_type: str = "media"
    kind: str = "remote-direct"
    original_filename: str | None = None
    url: str | None = None
    local_path: str | None = None
    download_url: str | None = None
    unrestricted_url: str | None = None
    source_attachment_id: str | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    size: int | None = None
    created: str | None = None
    modified: str | None = None
    refresh_state: str = "ready"
    expires_at: str | None = None
    last_refreshed_at: str | None = None
    last_refresh_error: str | None = None
    active_for_direct: bool = False
    active_for_hls: bool = False
    is_active_stream: bool = False
    lifecycle: MediaEntryLifecycleRecord | None = None


@dataclass(frozen=True)
class SubtitleEntryDetailRecord:
    """Subtitle projection exposed on item-detail responses."""

    id: str
    language: str
    format: str
    source: str
    url: str | None = None
    is_default: bool = False
    is_forced: bool = False


@dataclass(frozen=True)
class MediaItemsPage:
    """Paginated item-list snapshot for the current library compatibility surface."""

    success: bool
    items: list[MediaItemSummaryRecord]
    page: int
    limit: int
    total_items: int
    total_pages: int


@dataclass(frozen=True)
class ItemActionResult:
    """Result wrapper for item reset/retry/remove compatibility actions."""

    message: str
    ids: list[str]


@dataclass(frozen=True)
class ScrapeCandidateRecord:
    """Raw scrape-stage candidate persisted before parse/rank stages."""

    item_id: str
    info_hash: str
    raw_title: str
    provider: str
    size_bytes: int | None = None


def _normalize_requested_media_type(media_type: str | None) -> str | None:
    """Normalize incoming request media types into service-layer item types."""

    if media_type is None:
        return None
    normalized = media_type.strip().casefold()
    if normalized == "tv":
        return "show"
    if normalized in {"movie", "show", "season", "episode"}:
        return normalized
    return None


_REQUEST_DISCOVERY_SORT_CHOICES: dict[str, tuple[str, str, str]] = {
    "popular": ("Popular", "popularity.desc", "popularity.desc"),
    "newest": ("Newest", "primary_release_date.desc", "first_air_date.desc"),
    "oldest": ("Oldest", "primary_release_date.asc", "first_air_date.asc"),
    "rating": ("Top rated", "vote_average.desc", "vote_average.desc"),
}


def _normalize_request_discovery_sort(sort: str | None) -> str:
    if sort is None:
        return "popular"
    normalized = sort.strip().casefold()
    if normalized in _REQUEST_DISCOVERY_SORT_CHOICES:
        return normalized
    return "popular"


def _tmdb_discovery_sort(media_type: str, *, sort: str) -> str:
    _label, movie_sort, show_sort = _REQUEST_DISCOVERY_SORT_CHOICES[sort]
    return movie_sort if media_type == "movie" else show_sort


def _normalize_discovery_filter_value(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _request_discovery_editorial_weight(family: str) -> float:
    if family == "trending":
        return 1.4
    if family == "anticipated":
        return 1.15
    if family in {"newly-released", "returning"}:
        return 1.1
    return 0.8


def _request_discovery_release_window_weight(window: str) -> float:
    if window == "theatrical":
        return 1.2
    if window == "digital":
        return 1.1
    if window == "returning":
        return 1.05
    if window == "limited-series":
        return 1.0
    return 0.0


def _request_discovery_sort_key(
    hit: TmdbSearchResult,
    *,
    sort: str,
    blend_boost: float = 0.0,
) -> tuple[float | int | str, ...]:
    if sort == "oldest":
        return (
            hit.year if hit.year is not None else 9999,
            -blend_boost,
            -hit.popularity,
            hit.title.casefold(),
            hit.tmdb_id,
        )
    if sort == "newest":
        return (
            -(hit.year or 0) - blend_boost,
            -hit.popularity,
            hit.title.casefold(),
            hit.tmdb_id,
        )
    if sort == "rating":
        return (
            -(hit.vote_average + (blend_boost * 0.35)),
            -(hit.vote_count + int(blend_boost * 100)),
            -(hit.popularity + (blend_boost * 8.0)),
            hit.title.casefold(),
            hit.tmdb_id,
        )

    blended_score = (
        hit.popularity
        + (hit.vote_average * 1.75)
        + min(hit.vote_count / 750.0, 12.0)
        + ((hit.year or 0) / 1000.0)
        + (blend_boost * 20.0)
    )
    return (
        -blended_score,
        -(hit.year or 0),
        hit.title.casefold(),
        hit.tmdb_id,
    )


def _sort_request_discovery_hits(
    hits: list[TmdbSearchResult],
    *,
    sort: str,
    blend_boosts: dict[str, float] | None = None,
    local_boosts: dict[str, float] | None = None,
) -> list[TmdbSearchResult]:
    boosts = blend_boosts or {}
    local = local_boosts or {}
    return sorted(
        hits,
        key=lambda hit: _request_discovery_sort_key(
            hit,
            sort=sort,
            blend_boost=boosts.get(f"{hit.media_type}:{hit.tmdb_id}", 0.0)
            + local.get(_request_search_local_signal_key(hit.media_type, hit.tmdb_id), 0.0),
        ),
    )


def _request_discovery_filter_selected(
    selected_value: str | None,
    *,
    value: str,
    label: str,
) -> bool:
    if selected_value is None:
        return False
    normalized_selected = selected_value.strip().casefold()
    return normalized_selected in {value.strip().casefold(), label.strip().casefold()}


def _is_studio_like_name(name: str) -> bool:
    lowered = name.casefold()
    return any(token in lowered for token in _REQUEST_DISCOVERY_STUDIO_HINTS)


def _append_sample_title(sample_titles: tuple[str, ...], title: str) -> tuple[str, ...]:
    if title in sample_titles:
        return sample_titles
    if len(sample_titles) >= 3:
        return sample_titles
    return (*sample_titles, title)


def _build_request_projection_preview_signals(entry: dict[str, object]) -> tuple[str, ...]:
    """Return concise grouped-pivot continuation signals for one projection item."""

    labels: list[str] = []
    requested_match_count = int(entry.get("requested_match_count", 0))
    active_match_count = int(entry.get("active_match_count", 0))
    completed_match_count = int(entry.get("completed_match_count", 0))
    local_match_count = int(entry.get("local_match_count", 0))

    if requested_match_count > 0:
        labels.append(
            "1 requested title"
            if requested_match_count == 1
            else f"{requested_match_count} requested titles"
        )
    if active_match_count > 0:
        labels.append(
            "1 resume path"
            if active_match_count == 1
            else f"{active_match_count} resume paths"
        )
    elif completed_match_count > 0:
        labels.append(
            "1 completed title"
            if completed_match_count == 1
            else f"{completed_match_count} completed titles"
        )
    if local_match_count > 0 and not labels:
        labels.append(
            "1 local title"
            if local_match_count == 1
            else f"{local_match_count} local titles"
        )

    return tuple(labels[:3])


def _dominant_projection_media_type(media_counts: Counter[str]) -> str | None:
    """Return the most common media type represented in one projection bucket."""

    if not media_counts:
        return None
    return media_counts.most_common(1)[0][0]


def _request_projection_continuation_score(entry: dict[str, object]) -> tuple[int, int, int, int, int]:
    """Return one deterministic continuation score for grouped-pivot ordering."""

    return (
        int(entry.get("active_match_count", 0)),
        int(entry.get("requested_match_count", 0)),
        int(entry.get("completed_match_count", 0)),
        int(entry.get("local_match_count", 0)),
        int(entry.get("match_count", 0)),
    )


async def _build_request_discovery_blend_boosts(
    service: MediaService,
    client: TmdbMetadataClient,
    *,
    selected_media_types: list[str],
) -> dict[str, float]:
    boosts: dict[str, float] = {}

    for family in _REQUEST_EDITORIAL_DISCOVERY_FAMILIES:
        if family["media_type"] not in selected_media_types:
            continue
        for page_number in range(1, _REQUEST_DISCOVERY_EDITORIAL_BLEND_PAGE_LIMIT + 1):
            page_result = await service._fetch_request_editorial_page(
                client,
                media_type=family["media_type"],
                family=family["family"],
                page=page_number,
            )
            if not page_result.results:
                break
            weight = _request_discovery_editorial_weight(family["family"])
            for index, hit in enumerate(page_result.results):
                key = f"{hit.media_type}:{hit.tmdb_id}"
                boost = max(0.15, weight - (index * 0.03))
                boosts[key] = max(boosts.get(key, 0.0), boost)
            if page_number >= page_result.total_pages:
                break

    for window in _REQUEST_RELEASE_WINDOWS:
        if window["media_type"] not in selected_media_types:
            continue
        for page_number in range(1, _REQUEST_DISCOVERY_RELEASE_WINDOW_BLEND_PAGE_LIMIT + 1):
            page_result = await service._fetch_request_release_window_page(
                client,
                media_type=window["media_type"],
                window=window["window"],
                page=page_number,
            )
            if not page_result.results:
                break
            weight = _request_discovery_release_window_weight(window["window"])
            for index, hit in enumerate(page_result.results):
                key = f"{hit.media_type}:{hit.tmdb_id}"
                boost = max(0.1, weight - (index * 0.025))
                boosts[key] = max(boosts.get(key, 0.0), boost)
            if page_number >= page_result.total_pages:
                break

    return boosts


async def _build_request_discovery_facets(
    hits: list[TmdbSearchResult],
    *,
    client: TmdbMetadataClient | None,
    selected_genre: str | None,
    selected_release_year: int | None,
    selected_language: str | None,
    selected_company: str | None,
    selected_network: str | None,
    selected_sort: str,
) -> RequestDiscoveryFacetSetRecord:
    genre_counts: Counter[str] = Counter()
    year_counts: Counter[int] = Counter()
    language_counts: Counter[str] = Counter()
    company_counts: Counter[tuple[str, str]] = Counter()
    network_counts: Counter[tuple[str, str]] = Counter()
    for hit in hits:
        for genre_name in hit.genre_names:
            if genre_name.strip():
                genre_counts[genre_name.strip()] += 1
        if hit.year is not None:
            year_counts[int(hit.year)] += 1
        if hit.original_language:
            language_counts[hit.original_language] += 1

    if client is not None and hits:
        semaphore = asyncio.Semaphore(_REQUEST_DISCOVERY_FACET_DETAIL_CONCURRENCY)
        detail_hits = hits[:_REQUEST_DISCOVERY_FACET_DETAIL_WINDOW]

        async def load_detail(
            hit: TmdbSearchResult,
        ) -> MovieMetadata | ShowMetadata | None:
            async with semaphore:
                if hit.media_type == "movie":
                    return await client.get_movie(hit.tmdb_id)
                return await client.get_show(hit.tmdb_id)

        details = await asyncio.gather(
            *(load_detail(hit) for hit in detail_hits),
            return_exceptions=True,
        )

        for detail in details:
            if isinstance(detail, Exception) or detail is None:
                continue

            for company in detail.companies:
                identifier = company.get("id", "").strip()
                label = company.get("name", "").strip()
                if identifier and label:
                    company_counts[(identifier, label)] += 1

            if isinstance(detail, ShowMetadata):
                for network in detail.networks:
                    identifier = network.get("id", "").strip()
                    label = network.get("name", "").strip()
                    if identifier and label:
                        network_counts[(identifier, label)] += 1

    genres = tuple(
        RequestDiscoveryFacetBucketRecord(
            value=genre_name,
            label=genre_name,
            count=count,
            selected=(selected_genre == genre_name),
        )
        for genre_name, count in sorted(
            genre_counts.items(),
            key=lambda item: (-item[1], item[0].casefold()),
        )[:12]
    )
    release_years = tuple(
        RequestDiscoveryFacetBucketRecord(
            value=str(year),
            label=str(year),
            count=count,
            selected=(selected_release_year == year),
        )
        for year, count in sorted(
            year_counts.items(),
            key=lambda item: (-item[1], -item[0]),
        )[:12]
    )
    languages = tuple(
        RequestDiscoveryFacetBucketRecord(
            value=language,
            label=language.upper(),
            count=count,
            selected=(selected_language == language),
        )
        for language, count in sorted(
            language_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:12]
    )
    companies = tuple(
        RequestDiscoveryFacetBucketRecord(
            value=identifier,
            label=label,
            count=count,
            selected=_request_discovery_filter_selected(
                selected_company,
                value=identifier,
                label=label,
            ),
        )
        for (identifier, label), count in sorted(
            company_counts.items(),
            key=lambda item: (-item[1], item[0][1].casefold(), item[0][0]),
        )[:12]
    )
    networks = tuple(
        RequestDiscoveryFacetBucketRecord(
            value=identifier,
            label=label,
            count=count,
            selected=_request_discovery_filter_selected(
                selected_network,
                value=identifier,
                label=label,
            ),
        )
        for (identifier, label), count in sorted(
            network_counts.items(),
            key=lambda item: (-item[1], item[0][1].casefold(), item[0][0]),
        )[:12]
    )
    sorts = tuple(
        RequestDiscoverySortOptionRecord(
            value=value,
            label=label,
            selected=(selected_sort == value),
        )
        for value, (label, _movie_sort, _show_sort) in _REQUEST_DISCOVERY_SORT_CHOICES.items()
    )
    return RequestDiscoveryFacetSetRecord(
        genres=genres,
        release_years=release_years,
        languages=languages,
        companies=companies,
        networks=networks,
        sorts=sorts,
    )


def _build_request_discovery_projection_groups(
    items: list[RequestSearchCandidateRecord],
    profiles: list[TmdbDiscoveryProfile],
    *,
    local_signals: dict[str, RequestSearchLocalSignalRecord] | None = None,
    limit_per_group: int,
) -> list[RequestDiscoveryProjectionGroupRecord]:
    signal_map = local_signals or {}
    group_buckets: dict[str, dict[str, dict[str, object]]] = {
        "people": {},
        "studios": {},
        "companies": {},
        "collections": {},
        "franchises": {},
    }

    def add_projection(
        group_id: str,
        *,
        projection_id: str,
        label: str,
        projection_type: str,
        action_kind: str,
        action_value: str,
        media_type: str,
        sample_title: str,
        image_path: str | None,
        is_local: bool,
        is_requested: bool,
        is_active: bool,
        is_completed: bool,
    ) -> None:
        bucket = group_buckets[group_id]
        entry = bucket.get(projection_id)
        if entry is None:
            entry = {
                "projection_id": projection_id,
                "label": label,
                "projection_type": projection_type,
                "action_kind": action_kind,
                "action_value": action_value,
                "image_path": image_path,
                "match_count": 0,
                "sample_titles": (),
                "media_counts": Counter(),
                "local_match_count": 0,
                "requested_match_count": 0,
                "active_match_count": 0,
                "completed_match_count": 0,
            }
            bucket[projection_id] = entry

        entry["match_count"] = int(entry["match_count"]) + 1
        if entry["image_path"] is None and image_path is not None:
            entry["image_path"] = image_path
        entry["sample_titles"] = _append_sample_title(
            cast(tuple[str, ...], entry["sample_titles"]),
            sample_title,
        )
        cast(Counter[str], entry["media_counts"])[media_type] += 1
        if is_local:
            entry["local_match_count"] = int(entry["local_match_count"]) + 1
        if is_requested:
            entry["requested_match_count"] = int(entry["requested_match_count"]) + 1
        if is_active:
            entry["active_match_count"] = int(entry["active_match_count"]) + 1
        if is_completed:
            entry["completed_match_count"] = int(entry["completed_match_count"]) + 1

    for item, profile in zip(items, profiles, strict=False):
        signal = (
            signal_map.get(_request_search_local_signal_key(item.media_type, item.tmdb_id))
            if item.tmdb_id
            else None
        )
        signal_is_local = signal is not None and (
            signal.item is not None
            or signal.request_count > 0
            or signal.launch_count > 0
            or signal.view_count > 0
            or signal.session_count > 0
            or signal.completed_session_count > 0
            or signal.resume_position_seconds is not None
        )
        signal_is_requested = signal is not None and signal.request_count > 0
        signal_is_active = signal is not None and (
            signal.active_session_count > 0 or signal.resume_position_seconds is not None
        )
        signal_is_completed = signal is not None and (
            signal.completed_session_count > 0 and not signal_is_active
        )
        if not item.tmdb_id:
            continue

        for person in profile.people[:4]:
            add_projection(
                "people",
                projection_id=f"person:{person.tmdb_id}",
                label=person.name,
                projection_type="person",
                action_kind="query",
                action_value=person.name,
                media_type=item.media_type,
                sample_title=item.title,
                image_path=person.image_path,
                is_local=signal_is_local,
                is_requested=signal_is_requested,
                is_active=signal_is_active,
                is_completed=signal_is_completed,
            )

        for company in profile.companies[:4]:
            target_group = "studios" if _is_studio_like_name(company.name) else "companies"
            projection_type = "studio" if target_group == "studios" else "company"
            add_projection(
                target_group,
                projection_id=f"company:{company.tmdb_id}",
                label=company.name,
                projection_type=projection_type,
                action_kind="company",
                action_value=company.tmdb_id,
                media_type=item.media_type,
                sample_title=item.title,
                image_path=company.image_path,
                is_local=signal_is_local,
                is_requested=signal_is_requested,
                is_active=signal_is_active,
                is_completed=signal_is_completed,
            )

        if profile.collection is not None:
            collection = profile.collection
            add_projection(
                "collections",
                projection_id=f"collection:{collection.tmdb_id}",
                label=collection.name,
                projection_type="collection",
                action_kind="query",
                action_value=collection.name,
                media_type=item.media_type,
                sample_title=item.title,
                image_path=collection.image_path,
                is_local=signal_is_local,
                is_requested=signal_is_requested,
                is_active=signal_is_active,
                is_completed=signal_is_completed,
            )
            franchise_label = collection.name
            if franchise_label.casefold().endswith(" collection"):
                franchise_label = franchise_label[: -len(" collection")].strip() or collection.name
            add_projection(
                "franchises",
                projection_id=f"franchise:{collection.tmdb_id}",
                label=franchise_label,
                projection_type="franchise",
                action_kind="query",
                action_value=franchise_label,
                media_type=item.media_type,
                sample_title=item.title,
                image_path=collection.image_path,
                is_local=signal_is_local,
                is_requested=signal_is_requested,
                is_active=signal_is_active,
                is_completed=signal_is_completed,
            )

    group_definitions = (
        (
            "people",
            "People around this window",
            "Pivot through cast, creators, and directors tied to the current discovery window.",
            "person",
        ),
        (
            "studios",
            "Studios behind these titles",
            "Narrow the current window with studio-backed production signatures.",
            "studio",
        ),
        (
            "companies",
            "Companies in this window",
            "Follow production-company clusters without dropping out of the current discover flow.",
            "company",
        ),
        (
            "collections",
            "Collections in this window",
            "Continue exact multi-title collections already present in the current window.",
            "collection",
        ),
        (
            "franchises",
            "Franchises to continue",
            "Jump from the current window into broader recurring worlds and sequel chains.",
            "franchise",
        ),
    )

    groups: list[RequestDiscoveryProjectionGroupRecord] = []
    for group_id, title, description, projection_type in group_definitions:
        bucket = group_buckets[group_id]
        if not bucket:
            continue
        ordered_items = sorted(
            bucket.values(),
            key=lambda entry: (
                -_request_projection_continuation_score(entry)[0],
                -_request_projection_continuation_score(entry)[1],
                -_request_projection_continuation_score(entry)[2],
                -_request_projection_continuation_score(entry)[3],
                -_request_projection_continuation_score(entry)[4],
                str(entry["label"]).casefold(),
                str(entry["projection_id"]),
            ),
        )[:limit_per_group]
        groups.append(
            RequestDiscoveryProjectionGroupRecord(
                group_id=group_id,
                title=title,
                description=description,
                projection_type=projection_type,
                items=[
                    RequestDiscoveryProjectionItemRecord(
                        projection_id=cast(str, entry["projection_id"]),
                        label=cast(str, entry["label"]),
                        projection_type=cast(str, entry["projection_type"]),
                        match_count=int(entry["match_count"]),
                        image_path=cast(str | None, entry["image_path"]),
                        action=RequestDiscoveryProjectionActionRecord(
                            kind=cast(str, entry["action_kind"]),
                            value=cast(str, entry["action_value"]),
                            media_type=_dominant_projection_media_type(
                                cast(Counter[str], entry["media_counts"]),
                            ),
                        ),
                        sample_titles=cast(tuple[str, ...], entry["sample_titles"]),
                        local_match_count=int(entry["local_match_count"]),
                        requested_match_count=int(entry["requested_match_count"]),
                        active_match_count=int(entry["active_match_count"]),
                        completed_match_count=int(entry["completed_match_count"]),
                        preview_signals=_build_request_projection_preview_signals(entry),
                    )
                    for entry in ordered_items
                ],
            )
        )

    return groups


def _request_search_local_signal_key(media_type: str, tmdb_id: str) -> str:
    """Return the normalized lookup key used for local search/discovery signals."""

    return f"{media_type}:{tmdb_id}"


def _build_request_search_ranking_signals(
    signal: RequestSearchLocalSignalRecord,
) -> tuple[str, ...]:
    """Return concise local-demand reasons for one surfaced request candidate."""

    labels: list[str] = []
    if signal.request_count > 0:
        labels.append(
            "Requested locally"
            if signal.request_count == 1
            else f"Requested {signal.request_count}x"
        )

    if signal.active_session_count > 0 or signal.resume_position_seconds is not None:
        labels.append("Resume activity")
    elif signal.completed_session_count > 0:
        labels.append("Completed locally")
    elif signal.session_count > 0 or signal.launch_count > 0 or signal.view_count > 0:
        labels.append("Watched locally")

    if signal.item is not None and not labels:
        labels.append("In library")

    return tuple(labels[:3])


def _build_request_search_local_boost(signal: RequestSearchLocalSignalRecord) -> float:
    """Return one small additive local-demand boost for ranking request candidates."""

    boost = 0.0
    if signal.item is not None:
        boost += 0.06
    boost += min(signal.request_count, 5) * 0.04
    boost += min(signal.launch_count, 4) * 0.02
    boost += min(signal.view_count, 6) * 0.01
    boost += min(signal.session_count, 4) * 0.015
    if signal.active_session_count > 0 or signal.resume_position_seconds is not None:
        boost += 0.09
    elif signal.completed_session_count > 0:
        boost += 0.04
    return round(boost, 3)


def _request_search_score(
    hit: TmdbSearchResult,
    *,
    query: str,
    local_boost: float = 0.0,
) -> tuple[int, float, float, int]:
    """Return a deterministic ranking tuple for one request-search hit."""

    lowered_query = query.casefold()
    title = hit.title.casefold()
    if title == lowered_query:
        query_score = 500
    elif title.startswith(lowered_query):
        query_score = 350
    elif lowered_query in title:
        query_score = 200
    else:
        query_score = 100
    return (query_score, local_boost, hit.popularity, hit.year or 0)


def _sort_request_search_hits(
    hits: list[TmdbSearchResult],
    *,
    query: str,
    local_boosts: dict[str, float] | None = None,
) -> list[TmdbSearchResult]:
    """Sort request-search hits with stable relevance ordering."""

    boosts = local_boosts or {}
    return sorted(
        hits,
        key=lambda hit: (
            -_request_search_score(
                hit,
                query=query,
                local_boost=boosts.get(_request_search_local_signal_key(hit.media_type, hit.tmdb_id), 0.0),
            )[0],
            -_request_search_score(
                hit,
                query=query,
                local_boost=boosts.get(_request_search_local_signal_key(hit.media_type, hit.tmdb_id), 0.0),
            )[1],
            -_request_search_score(
                hit,
                query=query,
                local_boost=boosts.get(_request_search_local_signal_key(hit.media_type, hit.tmdb_id), 0.0),
            )[2],
            -_request_search_score(
                hit,
                query=query,
                local_boost=boosts.get(_request_search_local_signal_key(hit.media_type, hit.tmdb_id), 0.0),
            )[3],
            hit.title.casefold(),
            hit.tmdb_id,
        ),
    )


def _sort_request_window_hits(
    hits: list[TmdbSearchResult],
    *,
    local_boosts: dict[str, float] | None = None,
) -> list[TmdbSearchResult]:
    """Sort release and editorial windows with local-demand-aware ordering."""

    boosts = local_boosts or {}
    return sorted(
        hits,
        key=lambda hit: (
            -boosts.get(_request_search_local_signal_key(hit.media_type, hit.tmdb_id), 0.0),
            -hit.popularity,
            -(hit.year or 0),
            hit.title.casefold(),
            hit.tmdb_id,
        ),
    )


def _merge_request_attributes_for_external_ref(
    *,
    media_type: str | None,
    external_ref: str,
    attributes: dict[str, object],
) -> dict[str, object]:
    """Seed request-time attributes from the external reference and explicit media type."""

    merged = dict(attributes)
    normalized_media_type = _normalize_requested_media_type(media_type)
    if normalized_media_type is not None:
        merged.setdefault("item_type", normalized_media_type)

    system, separator, reference = external_ref.partition(":")
    if separator == "" or reference == "":
        return merged
    if system == "tmdb":
        merged.setdefault("tmdb_id", reference)
    elif system == "tvdb":
        merged.setdefault("tvdb_id", reference)
    elif system == "imdb":
        merged.setdefault("imdb_id", reference)
    return merged


def _specialization_identifiers(
    *,
    external_ref: str,
    attributes: dict[str, object],
) -> tuple[str | None, str | None, str | None]:
    """Return `(tmdb_id, tvdb_id, imdb_id)` for specialization rows."""

    tmdb_id = _extract_string(attributes, "tmdb_id")
    tvdb_id = _extract_string(attributes, "tvdb_id")
    imdb_id = _extract_string(attributes, "imdb_id")
    if tmdb_id is None and external_ref.startswith("tmdb:"):
        tmdb_id = external_ref.partition(":")[2] or None
    if tvdb_id is None and external_ref.startswith("tvdb:"):
        tvdb_id = external_ref.partition(":")[2] or None
    return tmdb_id, tvdb_id, imdb_id


def build_media_specialization_record(
    item: MediaItemORM,
    *,
    media_type: str,
    attributes: dict[str, object],
) -> MovieORM | ShowORM | SeasonORM | EpisodeORM | None:
    """Build the additive specialization row for one lifecycle carrier item."""

    tmdb_id, tvdb_id, imdb_id = _specialization_identifiers(
        external_ref=item.external_ref,
        attributes=attributes,
    )
    if media_type == "movie":
        return MovieORM(media_item_id=item.id, tmdb_id=tmdb_id, imdb_id=imdb_id)
    if media_type == "show":
        return ShowORM(media_item_id=item.id, tmdb_id=tmdb_id, tvdb_id=tvdb_id, imdb_id=imdb_id)
    if media_type == "season":
        return SeasonORM(
            media_item_id=item.id,
            season_number=_extract_int_value(
                attributes, "season_number", "season", "parent_season_number"
            ),
            tmdb_id=tmdb_id,
            tvdb_id=tvdb_id,
        )
    if media_type == "episode":
        return EpisodeORM(
            media_item_id=item.id,
            episode_number=_extract_int_value(attributes, "episode_number", "episode"),
            tmdb_id=tmdb_id,
            tvdb_id=tvdb_id,
            imdb_id=imdb_id,
        )
    return None


def update_media_specialization_record(
    record: MovieORM | ShowORM | SeasonORM | EpisodeORM,
    *,
    item: MediaItemORM,
    media_type: str,
    attributes: dict[str, object],
) -> MovieORM | ShowORM | SeasonORM | EpisodeORM:
    """Refresh one specialization row from the latest request-time metadata."""

    tmdb_id, tvdb_id, imdb_id = _specialization_identifiers(
        external_ref=item.external_ref,
        attributes=attributes,
    )
    if isinstance(record, MovieORM):
        record.tmdb_id = tmdb_id
        record.imdb_id = imdb_id
        return record
    if isinstance(record, ShowORM):
        record.tmdb_id = tmdb_id
        record.tvdb_id = tvdb_id
        record.imdb_id = imdb_id
        return record
    if isinstance(record, SeasonORM):
        record.season_number = _extract_int_value(
            attributes, "season_number", "season", "parent_season_number"
        )
        record.tmdb_id = tmdb_id
        record.tvdb_id = tvdb_id
        return record
    record.episode_number = _extract_int_value(attributes, "episode_number", "episode")
    record.tmdb_id = tmdb_id
    record.tvdb_id = tvdb_id
    record.imdb_id = imdb_id
    return record


def build_item_request_record(
    *,
    tenant_id: str = "global",
    external_ref: str,
    media_item_id: str | None,
    requested_title: str,
    media_type: str,
    requested_seasons: list[int] | None = None,
    requested_episodes: dict[str, list[int]] | None = None,
    is_partial: bool = False,
    request_source: str = "api",
    requested_at: datetime | None = None,
) -> ItemRequestORM:
    """Build a first-class request-intent ORM record for one external reference."""

    now = requested_at or datetime.now(UTC)
    return ItemRequestORM(
        tenant_id=tenant_id,
        external_ref=external_ref,
        media_item_id=media_item_id,
        requested_title=requested_title,
        media_type=media_type,
        requested_seasons=_clone_requested_seasons(requested_seasons),
        requested_episodes=_clone_requested_episodes(requested_episodes),
        is_partial=is_partial,
        request_source=_normalize_request_source(request_source),
        request_count=1,
        first_requested_at=now,
        last_requested_at=now,
        created_at=now,
        updated_at=now,
    )


def update_item_request_record(
    record: ItemRequestORM,
    *,
    tenant_id: str | None = None,
    media_item_id: str | None,
    requested_title: str,
    media_type: str,
    requested_seasons: object = _UNSET,
    requested_episodes: object = _UNSET,
    is_partial: bool | None = None,
    request_source: str = "api",
    requested_at: datetime | None = None,
) -> ItemRequestORM:
    """Update one request-intent record when the same external reference is requested again."""

    now = requested_at or datetime.now(UTC)
    if tenant_id is not None:
        record.tenant_id = tenant_id
    record.media_item_id = media_item_id
    record.requested_title = requested_title
    record.media_type = media_type
    if requested_seasons is not _UNSET:
        record.requested_seasons = _clone_requested_seasons(
            cast(list[int] | None, requested_seasons)
        )
    if requested_episodes is not _UNSET:
        record.requested_episodes = _clone_requested_episodes(
            cast(dict[str, list[int]] | None, requested_episodes)
        )
    if is_partial is not None:
        record.is_partial = is_partial
    record.request_source = _normalize_request_source(request_source)
    record.request_count += 1
    record.last_requested_at = now
    record.updated_at = now
    return record


def _normalize_identifier_list(values: list[str] | None) -> list[str]:
    """Return a de-duplicated identifier list preserving request order."""

    if not values:
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        identifier = value.strip()
        if not identifier or identifier in seen:
            continue
        seen.add(identifier)
        normalized.append(identifier)
    return normalized


def _requested_item_type_for_media_type(media_type: str) -> str:
    """Return the stored frontend-facing item type for one request payload media type."""

    return "movie" if media_type == "movie" else "show"


def _external_ref_for_media_type(media_type: str, identifier: str) -> str:
    """Return a stable namespaced external reference for one request identifier."""

    prefix = "tmdb" if media_type == "movie" else "tvdb"
    return f"{prefix}:{identifier}"


def _request_attributes_for_identifier(media_type: str, identifier: str) -> dict[str, object]:
    """Return the minimal metadata slice needed for current request/detail compatibility."""

    attributes: dict[str, object] = {
        "item_type": _requested_item_type_for_media_type(media_type),
    }
    parsed_identifier = _parse_external_ref_identifier(identifier)
    if parsed_identifier is not None:
        system, reference = parsed_identifier
        if system == "tmdb":
            attributes["tmdb_id"] = reference
        elif system == "tvdb":
            attributes["tvdb_id"] = reference
        else:
            attributes["imdb_id"] = reference
        return attributes

    if media_type == "movie":
        attributes["tmdb_id"] = identifier
    else:
        attributes["tvdb_id"] = identifier
    return attributes


def _item_matches_identifier(
    detail: MediaItemSummaryRecord,
    *,
    media_type: str,
    item_identifier: str,
) -> bool:
    """Return whether one detail record matches the requested frontend identifier family."""

    candidates = {item_identifier}
    if media_type == "movie":
        candidates.add(f"tmdb:{item_identifier}")
        return any(
            candidate is not None and candidate in candidates
            for candidate in (detail.tmdb_id, detail.external_ref, detail.id)
        )
    if media_type == "tv":
        candidates.add(f"tvdb:{item_identifier}")
        return any(
            candidate is not None and candidate in candidates
            for candidate in (detail.tvdb_id, detail.external_ref, detail.id)
        )
    return detail.id == item_identifier or detail.external_ref == item_identifier


@dataclass(frozen=True)
class CalendarReleaseDataRecord:
    """Optional release-window fields used by the current calendar page."""

    next_aired: str | None = None
    nextAired: str | None = None
    last_aired: str | None = None
    lastAired: str | None = None


@dataclass(frozen=True)
class CalendarItemRecord:
    """Calendar item record for current frontend compatibility."""

    item_id: str
    show_title: str
    item_type: str
    aired_at: str
    tvdb_id: str | None = None
    tmdb_id: str | None = None
    imdb_id: str | None = None
    parent_ids: ParentIdsRecord | None = None
    season: int | None = None
    episode: int | None = None
    last_state: str | None = None
    release_data: CalendarReleaseDataRecord | None = None
    specialization: MediaItemSpecializationRecord | None = None


@dataclass(frozen=True)
class CalendarProjectionRecord:
    """First-class episode-air-date projection for calendar queries."""

    item_id: str
    title: str
    item_type: str
    tmdb_id: str | None
    tvdb_id: str | None
    episode_number: int | None
    season_number: int | None
    air_date: str
    last_state: str | None = None
    release_data: CalendarReleaseDataRecord | None = None
    specialization: MediaItemSpecializationRecord | None = None


ShowCompletionResult = _media_show_completion.ShowCompletionResult
ParsedStreamCandidateRecord = _media_stream_candidates.ParsedStreamCandidateRecord
ParsedStreamCandidateValidation = _media_stream_candidates.ParsedStreamCandidateValidation
RankedStreamCandidateRecord = _media_stream_candidates.RankedStreamCandidateRecord
SelectedStreamCandidateRecord = _media_stream_candidates.SelectedStreamCandidateRecord
RankingRule = _media_stream_candidates.RankingRule
RankingModel = _media_stream_candidates.RankingModel
_SIMILARITY_THRESHOLD_DEFAULT = _media_stream_candidates._SIMILARITY_THRESHOLD_DEFAULT
_infer_request_media_type = _media_stream_candidates.infer_request_media_type
_extract_int_value = _media_stream_candidates.extract_int_value
_candidate_matches_partial_scope = _media_stream_candidates.candidate_matches_partial_scope
_candidate_parsed_seasons = _media_stream_candidates.candidate_parsed_seasons
_dedupe_title_aliases = _media_stream_candidates._dedupe_title_aliases
_extract_title_aliases = _media_stream_candidates.extract_title_aliases
attach_parse_validation = _media_stream_candidates.attach_parse_validation
parse_stage_rejection_reason = _media_stream_candidates.parse_stage_rejection_reason
rank_persisted_streams_for_item = _media_stream_candidates.rank_persisted_streams_for_item
select_stream_candidate = _media_stream_candidates.select_stream_candidate
parse_stream_candidate_title = _media_stream_candidates.parse_stream_candidate_title
validate_parsed_stream_candidate = _media_stream_candidates.validate_parsed_stream_candidate


def _extract_tmdb_episode_inventory(
    attributes: dict[str, object],
    *,
    today: date,
) -> dict[int, _media_show_completion.SeasonEpisodeInventory]:
    return _media_show_completion.extract_tmdb_episode_inventory(attributes, today=today)


async def _evaluate_show_completion(
    item: MediaItemRecord,
    db: DatabaseRuntime,
    settings: Settings,
) -> ShowCompletionResult:
    return await _media_show_completion.evaluate_show_completion(item, db, settings)
_infer_season_range_from_path = _media_path_inference.infer_season_range_from_path
class CompletionStatus(enum.StrEnum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    ONGOING = "ongoing"


class MediaService:
    """Domain service encapsulating media-item lifecycle persistence logic."""

    def __init__(
        self,
        db: DatabaseRuntime,
        event_bus: EventBus,
        *,
        scraped_item_enqueuer: Callable[[str], Awaitable[None]] | None = None,
        settings: Settings | None = None,
        rate_limiter: DistributedRateLimiter | None = None,
        tmdb_client: TmdbMetadataClient | None = None,
        tvdb_client: TvdbClient | None = None,
    ) -> None:
        self._db = db
        self._event_bus = event_bus
        self._scraped_item_enqueuer = scraped_item_enqueuer
        self._settings = settings
        self._rate_limiter = rate_limiter
        self._tmdb_client = tmdb_client
        self._tvdb_client = tvdb_client
        self._tvdb_cache: CacheManager | None = None

    async def evaluate_show_completion_scope(
        self, item_id: str, session: AsyncSession
    ) -> CompletionStatus:
        """Evaluate completion against explicitly requested scope or unreleased airing dates."""
        item = await session.get(MediaItemORM, _normalize_internal_item_id(item_id))
        if not item:
            raise ItemNotFoundError()

        attributes = dict(cast(dict[str, object], item.attributes or {}))
        raw_item_type = attributes.get("item_type")
        item_type = _canonical_item_type_name(raw_item_type if isinstance(raw_item_type, str) else None)

        if item_type == "movie":
            media_entries_result = await session.execute(
                select(MediaEntryORM.id).where(
                    MediaEntryORM.item_id == item.id,
                    MediaEntryORM.refresh_state.in_(_SATISFYING_MEDIA_ENTRY_REFRESH_STATES),
                    MediaEntryORM.entry_type == "media",
                ).limit(1)
            )
            if media_entries_result.first():
                return CompletionStatus.COMPLETE
            return CompletionStatus.INCOMPLETE

        current_id = item.id
        current_type = item_type
        
        while current_type in ("episode", "season"):
            if current_type == "episode":
                # Use media_item_id — current_id is a lifecycle MediaItemORM.id,
                # not EpisodeORM.id (specialization PK).
                ep_result = await session.execute(
                    select(EpisodeORM).where(EpisodeORM.media_item_id == current_id)
                )
                episode = ep_result.scalar_one_or_none()
                if not episode or not episode.season_id:
                    break
                season = await session.get(SeasonORM, episode.season_id)
                if not season or not season.show_id:
                    break
                current_id = season.show_id
            elif current_type == "season":
                # Same: query by media_item_id, not SeasonORM.id.
                s_result = await session.execute(
                    select(SeasonORM).where(SeasonORM.media_item_id == current_id)
                )
                season = s_result.scalar_one_or_none()
                if not season or not season.show_id:
                    break
                current_id = season.show_id
            
            parent_item = await session.get(MediaItemORM, current_id)
            if not parent_item:
                break
            parent_attrs = dict(cast(dict[str, object], parent_item.attributes or {}))
            raw_parent_item_type = parent_attrs.get("item_type")
            current_type = _canonical_item_type_name(
                raw_parent_item_type if isinstance(raw_parent_item_type, str) else None
            )
        
        root_show_id = current_id

        show_result = await session.execute(
            select(ShowORM)
            .where(ShowORM.media_item_id == root_show_id)
            .options(selectinload(ShowORM.seasons).selectinload(SeasonORM.episodes))
        )
        show_orm = show_result.scalar_one_or_none()
        
        if not show_orm or not show_orm.seasons:
            logger.warning(
                "evaluate_show_completion_scope missing specialization hierarchy",
                extra={"item_id": root_show_id}
            )
            return CompletionStatus.INCOMPLETE

        requests_result = await session.execute(
            select(ItemRequestORM).where(ItemRequestORM.media_item_id == root_show_id)
        )
        item_requests = list(requests_result.scalars().all())
        latest_request = _latest_item_request(item_requests)

        is_partial = latest_request.is_partial if latest_request else False
        req_seasons = latest_request.requested_seasons if latest_request else []
        req_episodes = latest_request.requested_episodes if latest_request else {}
        
        expected_episodes: set[str] = set()
        unreleased_expected = False
        now_utc = datetime.now(UTC)

        for season in show_orm.seasons:
            s_num = season.season_number
            s_num_str = str(s_num)
            
            in_req_seasons = (req_seasons is not None) and (s_num in req_seasons)
            
            for episode in season.episodes:
                e_num = episode.episode_number
                
                is_expected = False
                if is_partial:
                    season_requested_episodes = (
                        req_episodes.get(s_num_str)
                        if isinstance(req_episodes, dict)
                        else None
                    )
                    if season_requested_episodes is not None:
                        if e_num in season_requested_episodes:
                            is_expected = True
                    elif in_req_seasons:
                        is_expected = True
                else:
                    is_expected = True

                if is_expected:
                    ep_item = await session.get(MediaItemORM, episode.media_item_id)
                    aired_at_dt = None
                    if ep_item:
                        ep_attrs = dict(cast(dict[str, object], ep_item.attributes or {}))
                        raw_aired_at = ep_attrs.get("aired_at")
                        aired_at_dt = _parse_calendar_datetime(
                            raw_aired_at if isinstance(raw_aired_at, str) else None
                        )
                    
                    if aired_at_dt is None or aired_at_dt > now_utc:
                        unreleased_expected = True
                    elif episode.media_item_id:
                        expected_episodes.add(str(episode.media_item_id))

        if not expected_episodes:
            if unreleased_expected:
                return CompletionStatus.ONGOING
            return CompletionStatus.COMPLETE

        finalized_entries_result = await session.execute(
            select(MediaEntryORM.item_id)
            .where(
                MediaEntryORM.item_id.in_(expected_episodes),
                MediaEntryORM.refresh_state.in_(_SATISFYING_MEDIA_ENTRY_REFRESH_STATES),
                MediaEntryORM.entry_type == "media",
            )
            .distinct()
        )
        finalized_item_ids = set(finalized_entries_result.scalars().all())

        if expected_episodes.issubset(finalized_item_ids):
            if unreleased_expected:
                return CompletionStatus.ONGOING
            return CompletionStatus.COMPLETE
            
        return CompletionStatus.INCOMPLETE
        


    def _resolve_settings(self) -> Settings | None:
        if self._settings is not None:
            return self._settings
        try:
            return get_settings()
        except Exception:
            return None

    def _resolve_tmdb_client(self) -> TmdbMetadataClient | None:
        if self._tmdb_client is not None:
            return self._tmdb_client
        settings = self._resolve_settings()
        if settings is None or self._rate_limiter is None:
            return None
        return build_tmdb_metadata_client(settings, self._rate_limiter)

    def _resolve_tvdb_cache(self) -> CacheManager | None:
        if self._tvdb_cache is not None:
            return self._tvdb_cache
        if self._rate_limiter is None:
            return None
        self._tvdb_cache = CacheManager(
            redis=self._rate_limiter.redis,
            namespace="filmu_py_tvdb",
            default_ttl_seconds=23 * 60 * 60,
        )
        return self._tvdb_cache

    def _resolve_tvdb_client(self) -> TvdbClient | None:
        if self._tvdb_client is not None:
            return self._tvdb_client
        if self._rate_limiter is None:
            return None
        cache = self._resolve_tvdb_cache()
        if cache is None:
            return None
        self._tvdb_client = TvdbClient(
            api_key="6be85335-5c4f-4d8d-b945-d3ed0eb8cdce",
            cache=cache,
            rate_limiter=self._rate_limiter,
        )
        return self._tvdb_client

    async def _resolve_retry_reset_tmdb_client(self) -> TmdbMetadataClient | None:
        """Resolve a TMDB client using persisted settings first for retry/reset flows."""

        if self._tmdb_client is not None:
            return self._tmdb_client
        if self._rate_limiter is None:
            return None

        settings = self._resolve_settings()
        try:
            persisted_settings = await load_settings(self._db)
        except Exception:
            persisted_settings = None

        if persisted_settings is not None:
            with suppress(Exception):
                settings = Settings.from_compatibility_dict(persisted_settings)

        if settings is None:
            return None
        return build_tmdb_metadata_client(settings, self._rate_limiter)

    @staticmethod
    def _build_request_time_metadata_record(
        metadata: MovieMetadata | ShowMetadata,
        *,
        media_type: str,
        existing_attributes: dict[str, object],
    ) -> RequestTimeMetadataRecord:
        merged_attributes = dict(existing_attributes)
        merged_aliases = _dedupe_title_aliases(
            [
                *_extract_title_aliases(existing_attributes, canonical_title=metadata.title),
                *metadata.aliases,
            ],
            canonical_title=metadata.title,
        )
        merged_attributes.update(
            {
                "item_type": _requested_item_type_for_media_type(media_type),
                "tmdb_id": metadata.tmdb_id,
                "year": metadata.year,
                "overview": metadata.overview,
                "poster_path": metadata.poster_path,
                "genres": metadata.genres,
                "status": metadata.status,
            }
        )
        if isinstance(metadata, ShowMetadata):
            merged_attributes["seasons"] = list(metadata.seasons)
            if metadata.next_episode_to_air is not None:
                merged_attributes["next_episode_to_air"] = dict(metadata.next_episode_to_air)
        if merged_aliases:
            merged_attributes[_TITLE_ALIASES_ATTRIBUTE_KEY] = merged_aliases
        return RequestTimeMetadataRecord(title=metadata.title, attributes=merged_attributes)

    async def _enrich_request_metadata_external_ids(
        self,
        *,
        client: TmdbMetadataClient,
        media_type: str,
        metadata: RequestTimeMetadataRecord,
    ) -> RequestTimeMetadataRecord:
        current_imdb_id = _extract_string(metadata.attributes, "imdb_id")
        current_tmdb_id = _extract_string(metadata.attributes, "tmdb_id")
        if current_imdb_id is not None or current_tmdb_id is None:
            return metadata

        external_ids = await client.get_external_ids(
            current_tmdb_id,
            "movie" if media_type == "movie" else "tv",
        )
        imdb_id = external_ids.get("imdb_id")
        tvdb_id = external_ids.get("tvdb_id")
        if imdb_id is None and tvdb_id is None:
            return metadata

        merged_attributes = dict(metadata.attributes)
        if imdb_id is not None:
            merged_attributes["imdb_id"] = imdb_id
        if tvdb_id is not None and _extract_string(merged_attributes, "tvdb_id") is None:
            merged_attributes["tvdb_id"] = tvdb_id
        return RequestTimeMetadataRecord(title=metadata.title, attributes=merged_attributes)

    @staticmethod
    def _metadata_resolution(
        *,
        source: str,
        metadata: RequestTimeMetadataRecord | None,
        warnings: list[str] | None = None,
    ) -> RequestMetadataResolution:
        attributes = metadata.attributes if metadata is not None else {}
        return RequestMetadataResolution(
            metadata=metadata,
            enrichment=EnrichmentResult(
                source=source,
                has_poster=_extract_string(attributes, "poster_path") is not None,
                has_imdb_id=_extract_string(attributes, "imdb_id") is not None,
                has_tmdb_id=_extract_string(attributes, "tmdb_id") is not None,
                warnings=list(warnings or []),
            ),
        )

    def _build_tvdb_request_metadata(
        self,
        *,
        media_type: str,
        identifier: str,
        metadata: TvdbSeriesMetadata,
    ) -> RequestTimeMetadataRecord:
        attributes = _request_attributes_for_identifier(media_type, identifier)
        attributes["tvdb_id"] = metadata.tvdb_id
        if metadata.imdb_id is not None:
            attributes["imdb_id"] = metadata.imdb_id
        if metadata.poster_url is not None:
            attributes["poster_path"] = metadata.poster_url
        if metadata.overview:
            attributes["overview"] = metadata.overview
        return RequestTimeMetadataRecord(title=metadata.title, attributes=attributes)

    async def _tmdb_search_show_metadata(
        self,
        *,
        client: TmdbMetadataClient,
        title: str,
    ) -> ShowMetadata | None:
        normalized_title = title.strip()
        if not normalized_title:
            return None
        payload = await client._request_json("/search/tv", params={"query": normalized_title})
        if payload is None:
            return None
        results = payload.get("results")
        if not isinstance(results, list) or not results:
            return None
        first = results[0]
        if not isinstance(first, dict):
            return None
        tmdb_id = first.get("id")
        if tmdb_id is None:
            return None
        return await client.get_show(str(tmdb_id))

    async def _resolve_tvdb_fallback_metadata(
        self,
        *,
        media_type: str,
        identifier: str,
        tvdb_id: str,
        tmdb_client: TmdbMetadataClient | None,
    ) -> RequestMetadataResolution:
        warnings: list[str] = []
        source = "none"
        tmdb_cross_lookup = False
        resolved_imdb_id: str | None = None
        metadata: RequestTimeMetadataRecord | None = None

        tvdb_client = self._resolve_tvdb_client()
        tvdb_metadata = (
            await tvdb_client.get_series_extended(tvdb_id) if tvdb_client is not None else None
        )
        if tvdb_metadata is None:
            warnings.append("tvdb_api_unavailable")
        else:
            metadata = self._build_tvdb_request_metadata(
                media_type=media_type,
                identifier=identifier,
                metadata=tvdb_metadata,
            )
            resolved_imdb_id = tvdb_metadata.imdb_id
            source = "tvdb"
            if resolved_imdb_id is None:
                warnings.append("imdb_not_resolved")

        if tmdb_client is not None and resolved_imdb_id is not None:
            tmdb_cross_lookup = True
            tmdb_metadata = await tmdb_client.find_by_external_id("imdb_id", resolved_imdb_id)
            if tmdb_metadata is not None:
                metadata = self._build_request_time_metadata_record(
                    tmdb_metadata,
                    media_type=media_type,
                    existing_attributes=(metadata.attributes if metadata is not None else {}),
                )
                metadata = await self._enrich_request_metadata_external_ids(
                    client=tmdb_client,
                    media_type=media_type,
                    metadata=metadata,
                )
                source = "tmdb_via_tvdb"
            else:
                warnings.append("tmdb_cross_lookup_failed")

        if (
            tmdb_client is not None
            and tvdb_metadata is not None
            and (metadata is None or _extract_string(metadata.attributes, "tmdb_id") is None)
        ):
            searched_metadata = await self._tmdb_search_show_metadata(
                client=tmdb_client,
                title=tvdb_metadata.title,
            )
            if searched_metadata is not None:
                metadata = self._build_request_time_metadata_record(
                    searched_metadata,
                    media_type=media_type,
                    existing_attributes=(metadata.attributes if metadata is not None else {}),
                )
                metadata = await self._enrich_request_metadata_external_ids(
                    client=tmdb_client,
                    media_type=media_type,
                    metadata=metadata,
                )
                source = "search_fallback"
            else:
                warnings.append("search_fallback_not_found")

        structlogger.info(
            "request_enrichment.tvdb_fallback_used",
            tvdb_id=tvdb_id,
            resolved_imdb_id=resolved_imdb_id,
            tmdb_cross_lookup=tmdb_cross_lookup,
            enrichment_source=source,
        )
        return self._metadata_resolution(source=source, metadata=metadata, warnings=warnings)

    async def _fetch_request_metadata(
        self,
        *,
        media_type: str,
        identifier: str,
    ) -> RequestMetadataResolution:
        client = self._resolve_tmdb_client()

        try:
            if ":" in identifier:
                system, reference = identifier.split(":", 1)
                if system == "tvdb":
                    metadata = (
                        await client.find_by_external_id("tvdb_id", reference)
                        if client is not None
                        else None
                    )
                    if metadata is None:
                        return await self._resolve_tvdb_fallback_metadata(
                            media_type=media_type,
                            identifier=identifier,
                            tvdb_id=reference,
                            tmdb_client=client,
                        )
                elif system == "imdb":
                    if client is None:
                        return self._metadata_resolution(source="none", metadata=None)
                    metadata = await client.find_by_external_id("imdb_id", reference)
                elif system == "tmdb":
                    if client is None:
                        return self._metadata_resolution(source="none", metadata=None)
                    if media_type == "movie":
                        metadata = await client.get_movie(reference)
                    else:
                        metadata = await client.get_show(reference)
                else:
                    return self._metadata_resolution(source="none", metadata=None)
            else:
                if client is None:
                    return self._metadata_resolution(source="none", metadata=None)
                if media_type == "movie":
                    metadata = await client.get_movie(identifier)
                else:
                    metadata = await client.get_show(identifier)
        except Exception as exc:
            logger.warning(
                "request metadata enrichment failed",
                extra={"media_type": media_type, "identifier": identifier, "error": str(exc)},
            )
            return self._metadata_resolution(
                source="none",
                metadata=None,
                warnings=["metadata_lookup_failed"],
            )

        if metadata is None:
            return self._metadata_resolution(source="none", metadata=None)

        request_metadata = self._build_request_time_metadata_record(
            metadata,
            media_type=media_type,
            existing_attributes=_request_attributes_for_identifier(media_type, identifier),
        )
        assert client is not None
        request_metadata = await self._enrich_request_metadata_external_ids(
            client=client,
            media_type=media_type,
            metadata=request_metadata,
        )
        return self._metadata_resolution(source="tmdb", metadata=request_metadata)

    async def enrich_item_metadata(self, *, item_id: str) -> RequestMetadataResolution:
        """Best-effort metadata enrichment refresh for an existing item."""

        async with self._db.session() as session:
            item = (
                await session.execute(select(MediaItemORM).where(MediaItemORM.id == item_id))
            ).scalar_one_or_none()
            if item is None:
                raise ValueError(f"Unknown item_id={item_id}")

            existing_attributes = cast(dict[str, object], item.attributes or {})
            media_type = (
                "movie"
                if _canonical_item_type_name(
                    _infer_request_media_type(external_ref=item.external_ref, attributes=existing_attributes)
                )
                == "movie"
                else "tv"
            )
            resolution = await self._fetch_request_metadata(
                media_type=media_type,
                identifier=item.external_ref,
            )
            metadata = resolution.metadata
            if metadata is None:
                return resolution

            merged_attributes = _merge_request_attributes_for_external_ref(
                media_type=_normalize_requested_media_type(media_type),
                external_ref=item.external_ref,
                attributes={**existing_attributes, **metadata.attributes},
            )
            item.attributes = merged_attributes
            if item.title == item.external_ref or not item.title.strip():
                item.title = metadata.title

            normalized_media_type = _normalize_requested_media_type(media_type)
            candidate_media_type = normalized_media_type or _infer_request_media_type(
                external_ref=item.external_ref,
                attributes=merged_attributes,
            )
            await self._upsert_media_specialization(
                session,
                item=item,
                media_type=candidate_media_type,
                attributes=merged_attributes,
            )
            await session.commit()
            return resolution

    @staticmethod
    def _log_missing_imdb_id_intake_warning(
        *, item_id: str, external_ref: str, attributes: dict[str, object]
    ) -> None:
        tmdb_id = _extract_string(attributes, "tmdb_id")
        if tmdb_id is None or _extract_string(attributes, "imdb_id") is not None:
            return
        structlogger.warning(
            "item.intake.imdb_id_missing",
            item_id=item_id,
            tmdb_id=tmdb_id,
            external_ref=external_ref,
        )

    async def backfill_missing_imdb_ids(self, db: AsyncSession) -> dict[str, int]:
        """Backfill missing IMDb identifiers for persisted items and requeue failed ones."""

        result = {
            "attempted": 0,
            "enriched": 0,
            "skipped_no_tmdb_id": 0,
            "failed": 0,
        }
        client = self._resolve_tmdb_client()
        rows = (
            (
                await db.execute(select(MediaItemORM).order_by(MediaItemORM.created_at.asc(), MediaItemORM.id.asc()))
            )
            .scalars()
            .all()
        )

        for item in rows:
            attributes = cast(dict[str, object], item.attributes or {})
            tmdb_id = _extract_string(attributes, "tmdb_id")
            needs_tvdb_metadata_repair = tmdb_id is None and (
                _extract_string(attributes, "tvdb_id") is not None or item.external_ref.startswith("tvdb:")
            )
            if _extract_string(attributes, "imdb_id") is not None and not needs_tvdb_metadata_repair:
                continue

            result["attempted"] += 1
            media_type = _infer_request_media_type(
                external_ref=item.external_ref,
                attributes=attributes,
            )
            lookup_media_type = "movie" if media_type == "movie" else "tv"

            if needs_tvdb_metadata_repair:
                try:
                    async with db.begin_nested():
                        resolution = await self._fetch_request_metadata(
                            media_type=lookup_media_type,
                            identifier=item.external_ref,
                        )
                        metadata = resolution.metadata
                        if metadata is None:
                            result["skipped_no_tmdb_id"] += 1
                            logger.info(
                                "item.backfill_imdb_id.skipped_no_tmdb_id",
                                extra={"item_id": item.id},
                            )
                            continue

                        updated_attributes = _merge_request_attributes_for_external_ref(
                            media_type=_normalize_requested_media_type(lookup_media_type),
                            external_ref=item.external_ref,
                            attributes={**attributes, **metadata.attributes},
                        )
                        item.attributes = updated_attributes
                        if item.title == item.external_ref or not item.title.strip():
                            item.title = metadata.title
                        await self._upsert_media_specialization(
                            db,
                            item=item,
                            media_type=media_type,
                            attributes=updated_attributes,
                        )

                        if item.state == ItemState.FAILED.value:
                            transition = ItemStateMachine(state=ItemState(item.state)).apply(ItemEvent.RETRY)
                            item.state = transition.current.value
                            db.add(
                                ItemStateEventORM(
                                    item_id=item.id,
                                    event=transition.event.value,
                                    previous_state=transition.previous.value,
                                    next_state=transition.current.value,
                                    message="backfill_imdb_id",
                                    payload={
                                        "reason": "backfill_imdb_id",
                                        "imdb_id": _extract_string(updated_attributes, "imdb_id"),
                                    },
                                )
                            )

                        logger.info(
                            "item.backfill_imdb_id.enriched",
                            extra={
                                "item_id": item.id,
                                "imdb_id": _extract_string(updated_attributes, "imdb_id"),
                                "tmdb_id": _extract_string(updated_attributes, "tmdb_id"),
                            },
                        )
                        result["enriched"] += 1
                except Exception as exc:
                    result["failed"] += 1
                    logger.warning(
                        "item.backfill_imdb_id.failed",
                        extra={"item_id": item.id, "tmdb_id": tmdb_id, "error": str(exc)},
                    )
                continue

            if tmdb_id is None:
                result["skipped_no_tmdb_id"] += 1
                logger.info(
                    "item.backfill_imdb_id.skipped_no_tmdb_id",
                    extra={"item_id": item.id},
                )
                continue
            if client is None:
                result["failed"] += 1
                logger.warning(
                    "item.backfill_imdb_id.failed.no_tmdb_client",
                    extra={"item_id": item.id, "tmdb_id": tmdb_id},
                )
                continue

            try:
                async with db.begin_nested():
                    external_ids = await client.get_external_ids(
                        tmdb_id,
                        lookup_media_type,
                    )
                    imdb_id = external_ids.get("imdb_id")
                    if imdb_id is None:
                        logger.warning(
                            "item.backfill_imdb_id.unresolved",
                            extra={"item_id": item.id, "tmdb_id": tmdb_id},
                        )
                        continue

                    updated_attributes = dict(attributes)
                    updated_attributes["imdb_id"] = imdb_id
                    tvdb_id = external_ids.get("tvdb_id")
                    if tvdb_id is not None and _extract_string(updated_attributes, "tvdb_id") is None:
                        updated_attributes["tvdb_id"] = tvdb_id
                    item.attributes = updated_attributes
                    await self._upsert_media_specialization(
                        db,
                        item=item,
                        media_type=media_type,
                        attributes=updated_attributes,
                    )

                    if item.state == ItemState.FAILED.value:
                        transition = ItemStateMachine(state=ItemState(item.state)).apply(ItemEvent.RETRY)
                        item.state = transition.current.value
                        db.add(
                            ItemStateEventORM(
                                item_id=item.id,
                                event=transition.event.value,
                                previous_state=transition.previous.value,
                                next_state=transition.current.value,
                                message="backfill_imdb_id",
                                payload={"reason": "backfill_imdb_id", "imdb_id": imdb_id},
                            )
                        )

                    logger.info(
                        "item.backfill_imdb_id.enriched",
                        extra={"item_id": item.id, "imdb_id": imdb_id, "tmdb_id": tmdb_id},
                    )
                    result["enriched"] += 1
            except Exception as exc:
                result["failed"] += 1
                logger.warning(
                    "item.backfill_imdb_id.failed",
                    extra={"item_id": item.id, "tmdb_id": tmdb_id, "error": str(exc)},
                )

        await db.commit()
        logger.info("item.backfill_imdb_id.summary", extra=result)
        return result

    async def _enrich_item_imdb_id(self, item: MediaItemORM, db: AsyncSession) -> bool:
        """Best-effort single-item IMDb enrichment used by retry/reset flows."""

        attributes = cast(dict[str, object], item.attributes or {})
        if _extract_string(attributes, "imdb_id") is not None:
            return False

        tmdb_id = _extract_string(attributes, "tmdb_id")
        client = await self._resolve_retry_reset_tmdb_client()
        if tmdb_id is None or client is None:
            return False

        media_type = _infer_request_media_type(external_ref=item.external_ref, attributes=attributes)
        external_ids = await client.get_external_ids(tmdb_id, "movie" if media_type == "movie" else "tv")
        imdb_id = external_ids.get("imdb_id")
        if imdb_id is None:
            return False

        updated_attributes = dict(attributes)
        updated_attributes["imdb_id"] = imdb_id
        tvdb_id = external_ids.get("tvdb_id")
        if tvdb_id is not None and _extract_string(updated_attributes, "tvdb_id") is None:
            updated_attributes["tvdb_id"] = tvdb_id
        item.attributes = updated_attributes
        await self._upsert_media_specialization(
            db,
            item=item,
            media_type=media_type,
            attributes=updated_attributes,
        )
        return True

    @staticmethod
    def _build_retry_reset_state_event(
        item: MediaItemORM,
        *,
        event_name: str,
        message: str,
    ) -> ItemStateEventORM:
        """Move one item back to requested, using retry transition when available."""

        previous_state = item.state
        if item.state == ItemState.FAILED.value:
            transition = ItemStateMachine(state=ItemState(item.state)).apply(ItemEvent.RETRY)
            item.state = transition.current.value
            next_state = transition.current.value
        else:
            item.state = ItemState.REQUESTED.value
            next_state = ItemState.REQUESTED.value

        return ItemStateEventORM(
            item_id=item.id,
            event=event_name,
            previous_state=previous_state,
            next_state=next_state,
            message=message,
        )

    async def retry_item(
        self,
        item_id: str,
        db: AsyncSession,
        arq_pool: ArqRedis | None,
        *,
        tenant_id: str | None = None,
    ) -> MediaItemORM:
        """Retry one item immediately, re-enqueueing scrape work."""

        if arq_pool is None:
            raise ArqNotEnabledError(
                "ARQ is not enabled; retry/reset requires the worker to be running"
            )

        statement = select(MediaItemORM).where(MediaItemORM.id == item_id)
        if tenant_id is not None:
            statement = statement.where(MediaItemORM.tenant_id == tenant_id)
        item = (await db.execute(statement)).scalar_one_or_none()
        if item is None:
            raise ItemNotFoundError(f"unknown item_id={item_id}")

        imdb_id_was_missing = _extract_string(cast(dict[str, object], item.attributes or {}), "imdb_id") is None
        await self._enrich_item_imdb_id(item, db)
        db.add(self._build_retry_reset_state_event(item, event_name="retry", message="Items retried."))
        job = await arq_pool.enqueue_job("scrape_item", item_id=str(item.id))
        item_snapshot = _clone_media_item_snapshot(item)
        await db.commit()
        logger.info(
            "item.retry",
            extra={
                "item_id": item_snapshot.id,
                "imdb_id_was_missing": imdb_id_was_missing,
                "streams_blacklisted": 0,
            },
        )
        return _attach_retry_reset_diagnostics(
            item_snapshot,
            imdb_id_was_missing=imdb_id_was_missing,
            streams_blacklisted=0,
            active_stream_cleared=False,
            scrape_job_enqueued=job is not None,
        )

    async def prepare_item_for_scrape_retry(
        self,
        item_id: str,
        *,
        message: str,
        blacklist_stream_ids: list[str] | None = None,
    ) -> MediaItemRecord:
        """Move one item back to requested so the worker can run a fresh scrape cycle."""

        async with self._db.session() as session:
            item = (
                await session.execute(select(MediaItemORM).where(MediaItemORM.id == item_id))
            ).scalar_one_or_none()
            if item is None:
                raise ItemNotFoundError(f"unknown item_id={item_id}")

            item.next_retry_at = None
            normalized_blacklist_ids = {
                stream_id.strip() for stream_id in (blacklist_stream_ids or []) if stream_id.strip()
            }
            if normalized_blacklist_ids:
                existing_blacklist_ids = set(
                    (
                        await session.execute(
                            select(StreamBlacklistRelationORM.stream_id).where(
                                StreamBlacklistRelationORM.media_item_id == item.id
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                for stream_id in sorted(normalized_blacklist_ids):
                    if stream_id in existing_blacklist_ids:
                        continue
                    session.add(StreamBlacklistRelationORM(media_item_id=item.id, stream_id=stream_id))
                    existing_blacklist_ids.add(stream_id)
            session.add(
                self._build_retry_reset_state_event(
                    item,
                    event_name="search_retry",
                    message=message,
                )
            )
            snapshot = MediaItemRecord(
                id=item.id,
                external_ref=item.external_ref,
                title=item.title,
                state=ItemState(item.state),
                attributes=dict(cast(dict[str, object], item.attributes or {})),
                has_media_entries=False,
            )
            await session.commit()
            return snapshot

    async def reset_item(
        self,
        item_id: str,
        db: AsyncSession,
        arq_pool: ArqRedis | None,
        *,
        tenant_id: str | None = None,
    ) -> MediaItemORM:
        """Reset one item by blacklisting current streams and re-enqueueing scrape work."""

        if arq_pool is None:
            raise ArqNotEnabledError(
                "ARQ is not enabled; retry/reset requires the worker to be running"
            )

        statement = select(MediaItemORM).where(MediaItemORM.id == item_id)
        if tenant_id is not None:
            statement = statement.where(MediaItemORM.tenant_id == tenant_id)
        item = (await db.execute(statement)).scalar_one_or_none()
        if item is None:
            raise ItemNotFoundError(f"unknown item_id={item_id}")

        imdb_id_was_missing = _extract_string(cast(dict[str, object], item.attributes or {}), "imdb_id") is None
        stream_ids = list(
            (
                await db.execute(select(StreamORM.id).where(StreamORM.media_item_id == item.id))
            )
            .scalars()
            .all()
        )
        existing_blacklist_ids = set(
            (
                await db.execute(
                    select(StreamBlacklistRelationORM.stream_id).where(
                        StreamBlacklistRelationORM.media_item_id == item.id
                    )
                )
            )
            .scalars()
            .all()
        )
        streams_blacklisted = 0
        for stream_id in stream_ids:
            if stream_id in existing_blacklist_ids:
                continue
            db.add(StreamBlacklistRelationORM(media_item_id=item.id, stream_id=stream_id))
            existing_blacklist_ids.add(stream_id)
            streams_blacklisted += 1

        active_stream_ids = list(
            (
                await db.execute(select(ActiveStreamORM.id).where(ActiveStreamORM.item_id == item.id))
            )
            .scalars()
            .all()
        )
        active_stream_cleared = bool(active_stream_ids)
        if active_stream_ids:
            await db.execute(delete(ActiveStreamORM).where(ActiveStreamORM.id.in_(active_stream_ids)))

        await self._enrich_item_imdb_id(item, db)
        db.add(self._build_retry_reset_state_event(item, event_name="reset", message="Items reset."))
        job = await arq_pool.enqueue_job("scrape_item", item_id=str(item.id))
        item_snapshot = _clone_media_item_snapshot(item)
        await db.commit()
        logger.info(
            "item.reset",
            extra={
                "item_id": item_snapshot.id,
                "imdb_id_was_missing": imdb_id_was_missing,
                "streams_blacklisted": streams_blacklisted,
            },
        )
        return _attach_retry_reset_diagnostics(
            item_snapshot,
            imdb_id_was_missing=imdb_id_was_missing,
            streams_blacklisted=streams_blacklisted,
            active_stream_cleared=active_stream_cleared,
            scrape_job_enqueued=job is not None,
        )

    async def _upsert_item_request(
        self,
        session: Any,
        *,
        tenant_id: str = "global",
        external_ref: str,
        media_item_id: str | None,
        requested_title: str,
        media_type: str,
        requested_seasons: object = _UNSET,
        requested_episodes: object = _UNSET,
        is_partial: bool | None = None,
        request_source: str = "api",
    ) -> ItemRequestORM:
        """Create or update the request-intent row for one external reference."""

        existing = (
            await session.execute(
                select(ItemRequestORM).where(
                    ItemRequestORM.tenant_id == tenant_id,
                    ItemRequestORM.external_ref == external_ref,
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            request_record = build_item_request_record(
                tenant_id=tenant_id,
                external_ref=external_ref,
                media_item_id=media_item_id,
                requested_title=requested_title,
                media_type=media_type,
                requested_seasons=(
                    None if requested_seasons is _UNSET else cast(list[int] | None, requested_seasons)
                ),
                requested_episodes=(
                    None
                    if requested_episodes is _UNSET
                    else cast(dict[str, list[int]] | None, requested_episodes)
                ),
                is_partial=is_partial or False,
                request_source=request_source,
            )
            session.add(request_record)
            return request_record

        return update_item_request_record(
            existing,
            media_item_id=media_item_id,
            requested_title=requested_title,
            media_type=media_type,
            requested_seasons=requested_seasons,
            requested_episodes=requested_episodes,
            is_partial=is_partial,
            request_source=request_source,
        )

    async def get_latest_item_request(
        self,
        *,
        media_item_id: str,
    ) -> ItemRequestSummaryRecord | None:
        """Return the newest persisted request-intent summary for one media item."""

        async with self._db.session() as session:
            request_record = (
                (
                    await session.execute(
                        select(ItemRequestORM)
                        .where(ItemRequestORM.media_item_id == media_item_id)
                        .order_by(
                            ItemRequestORM.last_requested_at.desc(),
                            ItemRequestORM.created_at.desc(),
                        )
                    )
                )
                .scalars()
                .first()
            )
        return _build_item_request_summary_record(request_record)

    async def add_subtitle_entry(
        self,
        item_id: str,
        language: str,
        format: str,
        source: str = "unknown",
        url: str | None = None,
        file_path: str | None = None,
        is_default: bool = False,
        is_forced: bool = False,
        provider_subtitle_id: str | None = None,
    ) -> SubtitleEntryORM:
        """Persist one subtitle row for a media item."""

        async with self._db.session() as session:
            entry = SubtitleEntryORM(
                item_id=item_id,
                language=language,
                format=format,
                source=source,
                url=url,
                file_path=file_path,
                is_default=is_default,
                is_forced=is_forced,
                provider_subtitle_id=provider_subtitle_id,
            )
            session.add(entry)
            await session.commit()
            return entry

    async def get_subtitle_entries(self, item_id: str) -> list[SubtitleEntryORM]:
        """Return subtitle rows for one item in deterministic order."""

        async with self._db.session() as session:
            rows = (
                (
                    await session.execute(
                        select(SubtitleEntryORM)
                        .where(SubtitleEntryORM.item_id == item_id)
                        .order_by(SubtitleEntryORM.created_at, SubtitleEntryORM.id)
                    )
                )
                .scalars()
                .all()
            )
        return list(rows)

    async def remove_subtitle_entry(self, subtitle_id: str) -> None:
        """Delete one persisted subtitle row when present."""

        async with self._db.session() as session:
            entry = await session.get(SubtitleEntryORM, subtitle_id)
            if entry is not None:
                await session.delete(entry)
                await session.commit()

    async def _upsert_media_specialization(
        self,
        session: Any,
        *,
        item: MediaItemORM,
        media_type: str,
        attributes: dict[str, object],
    ) -> MovieORM | ShowORM | SeasonORM | EpisodeORM | None:
        """Create or update the additive specialization row for one requested item."""

        specialization_model_by_type: dict[
            str, type[MovieORM] | type[ShowORM] | type[SeasonORM] | type[EpisodeORM]
        ] = {
            "movie": MovieORM,
            "show": ShowORM,
            "season": SeasonORM,
            "episode": EpisodeORM,
        }
        model = specialization_model_by_type.get(media_type)
        if model is None:
            return None

        existing = (
            await session.execute(select(model).where(model.media_item_id == item.id))
        ).scalar_one_or_none()
        if existing is None:
            record = build_media_specialization_record(
                item,
                media_type=media_type,
                attributes=attributes,
            )
            if record is None:
                return None
            session.add(record)
            return record

        return update_media_specialization_record(
            existing,
            item=item,
            media_type=media_type,
            attributes=attributes,
        )

    async def persist_parsed_stream_candidates(
        self,
        *,
        item_id: str,
        raw_titles: list[str],
        infohash: str | None = None,
        requested_seasons: list[int] | None = None,
    ) -> list[StreamORM]:
        """Parse and persist stream candidates once so later rank stages can reuse them."""

        normalized_titles = [title.strip() for title in raw_titles if title.strip()]
        if not normalized_titles:
            return []

        async with self._db.session() as session:
            item = (
                await session.execute(select(MediaItemORM).where(MediaItemORM.id == item_id))
            ).scalar_one_or_none()
            if item is None:
                raise ValueError(f"unknown item_id={item_id}")

            existing_rows = (
                (await session.execute(select(StreamORM).where(StreamORM.media_item_id == item_id)))
                .scalars()
                .all()
            )
            existing_by_key = {
                (row.infohash, row.raw_title.casefold()): row for row in existing_rows
            }

            persisted: list[StreamORM] = []
            for raw_title in normalized_titles:
                parsed_candidate = parse_stream_candidate_title(raw_title, infohash=infohash)
                validation = validate_parsed_stream_candidate(item, parsed_candidate)
                parsed_payload = attach_parse_validation(
                    parsed_candidate.parsed_title,
                    validation,
                )
                if requested_seasons is not None and not _candidate_matches_partial_scope(
                    _candidate_parsed_seasons(parsed_candidate.parsed_title),
                    requested_seasons,
                ):
                    continue

                stream_key = (parsed_candidate.infohash, parsed_candidate.raw_title.casefold())
                existing = existing_by_key.get(stream_key)
                if existing is None:
                    existing = StreamORM(
                        media_item_id=item_id,
                        infohash=parsed_candidate.infohash,
                        raw_title=parsed_candidate.raw_title,
                        parsed_title=parsed_payload,
                        rank=0,
                        lev_ratio=None,
                        resolution=parsed_candidate.resolution,
                    )
                    session.add(existing)
                    existing_by_key[stream_key] = existing
                else:
                    existing.parsed_title = parsed_payload
                    existing.resolution = parsed_candidate.resolution

                if not validation.ok:
                    continue
                persisted.append(existing)

            await session.commit()
            return persisted

    async def persist_scrape_candidates(
        self,
        *,
        item_id: str,
        candidates: list[ScrapeCandidateRecord],
    ) -> list[ScrapeCandidateORM]:
        """Replace persisted raw scrape candidates for one item with the latest scrape pass."""

        normalized: list[ScrapeCandidateRecord] = []
        seen_info_hashes: set[str] = set()
        for candidate in candidates:
            info_hash = candidate.info_hash.strip().lower()
            raw_title = candidate.raw_title.strip()
            provider = candidate.provider.strip()
            if not info_hash or not raw_title or not provider or info_hash in seen_info_hashes:
                continue
            seen_info_hashes.add(info_hash)
            normalized.append(
                ScrapeCandidateRecord(
                    item_id=item_id,
                    info_hash=info_hash,
                    raw_title=raw_title,
                    provider=provider,
                    size_bytes=candidate.size_bytes,
                )
            )

        async with self._db.session() as session:
            item = (
                await session.execute(select(MediaItemORM).where(MediaItemORM.id == item_id))
            ).scalar_one_or_none()
            if item is None:
                raise ValueError(f"unknown item_id={item_id}")

            await session.execute(
                delete(ScrapeCandidateORM).where(ScrapeCandidateORM.item_id == item_id)
            )

            persisted: list[ScrapeCandidateORM] = []
            for candidate in normalized:
                row = ScrapeCandidateORM(
                    item_id=item_id,
                    info_hash=candidate.info_hash,
                    raw_title=candidate.raw_title,
                    provider=candidate.provider,
                    size_bytes=_coerce_int32_or_none(candidate.size_bytes),
                )
                session.add(row)
                persisted.append(row)

            await session.commit()
            return persisted

    async def rank_stream_candidates(
        self,
        *,
        media_item_id: str,
        similarity_threshold: float = _SIMILARITY_THRESHOLD_DEFAULT,
        ranking_model: RankingModel | None = None,
    ) -> list[RankedStreamCandidateRecord]:
        """Read persisted parsed candidates, compute durable scores, and write them back."""

        async with self._db.session() as session:
            item = (
                await session.execute(
                    select(MediaItemORM)
                    .options(selectinload(MediaItemORM.streams))
                    .where(MediaItemORM.id == media_item_id)
                )
            ).scalar_one_or_none()
            if item is None:
                raise ValueError(f"unknown media_item_id={media_item_id}")

            streams = sorted(item.streams, key=lambda stream: (stream.created_at, stream.id))
            ranked = rank_persisted_streams_for_item(
                item,
                streams,
                similarity_threshold=similarity_threshold,
                ranking_model=ranking_model,
            )
            await session.commit()
            return ranked

    async def select_stream_candidate(
        self,
        *,
        media_item_id: str,
        ranked_results: list[RankedStreamCandidateRecord] | None = None,
        similarity_threshold: float = _SIMILARITY_THRESHOLD_DEFAULT,
        ranking_model: RankingModel | None = None,
    ) -> SelectedStreamCandidateRecord | None:
        """Persist exactly one selected stream candidate for the target media item."""

        async with self._db.session() as session:
            item = (
                await session.execute(
                    select(MediaItemORM)
                    .options(selectinload(MediaItemORM.streams))
                    .where(MediaItemORM.id == media_item_id)
                )
            ).scalar_one_or_none()
            if item is None:
                raise ValueError(f"unknown media_item_id={media_item_id}")

            streams = sorted(item.streams, key=lambda stream: (stream.created_at, stream.id))
            ranked = ranked_results
            if ranked is None:
                ranked = rank_persisted_streams_for_item(
                    item,
                    streams,
                    similarity_threshold=similarity_threshold,
                    ranking_model=ranking_model,
                )
            selected_candidate = select_stream_candidate(ranked)
            selected_stream_id = selected_candidate.id if selected_candidate is not None else None
            selected_stream_snapshot = selected_candidate

            if selected_stream_id is not None:
                selected_stream = next(
                    (stream for stream in streams if stream.id == selected_stream_id),
                    None,
                )
                if selected_stream is not None:
                    selected_stream_snapshot = SelectedStreamCandidateRecord(
                        id=selected_stream.id,
                        infohash=selected_stream.infohash,
                        raw_title=selected_stream.raw_title,
                        resolution=selected_stream.resolution,
                        provider=None,
                    )

            for stream in streams:
                stream.selected = stream.id == selected_stream_id

            await session.commit()
            return selected_stream_snapshot

    async def get_latest_item_request_id(self, *, media_item_id: str) -> str | None:
        """Return the newest persisted request-intent identifier for one media item."""

        async with self._db.session() as session:
            request_record = (
                (
                    await session.execute(
                        select(ItemRequestORM)
                        .where(ItemRequestORM.media_item_id == media_item_id)
                        .order_by(
                            ItemRequestORM.last_requested_at.desc(),
                            ItemRequestORM.created_at.desc(),
                        )
                    )
                )
                .scalars()
                .first()
            )
            return request_record.id if request_record is not None else None

    async def persist_debrid_download_entries(
        self,
        *,
        media_item_id: str,
        provider: str,
        provider_download_id: str,
        torrent_info: TorrentInfo,
        download_urls: list[str],
    ) -> list[MediaEntryORM]:
        """Upsert provider-backed media entries from one debrid download result set."""

        async with self._db.session() as session:
            item = (
                await session.execute(
                    select(MediaItemORM)
                    .options(selectinload(MediaItemORM.media_entries))
                    .where(MediaItemORM.id == media_item_id)
                )
            ).scalar_one_or_none()
            if item is None:
                raise ValueError(f"unknown media_item_id={media_item_id}")

            def _normalized_entry_text(value: str | None) -> str | None:
                if value is None:
                    return None
                normalized = value.strip()
                return normalized or None

            existing_by_strict_key: dict[tuple[str, str, str], MediaEntryORM] = {}
            existing_by_provider_path_key: dict[tuple[str, str], MediaEntryORM] = {}
            existing_by_name_size_key: dict[tuple[str, str, int], MediaEntryORM] = {}

            def _register_existing_entry(entry: MediaEntryORM) -> None:
                provider_key = _normalized_entry_text(entry.provider)
                download_id_key = _normalized_entry_text(entry.provider_download_id)
                file_id_key = _normalized_entry_text(entry.provider_file_id)
                original_name_key = _normalized_entry_text(entry.original_filename)
                path_name_key = _normalized_entry_text(entry.provider_file_path) or original_name_key

                if download_id_key and file_id_key and original_name_key:
                    existing_by_strict_key[(download_id_key, file_id_key, original_name_key)] = entry
                if provider_key and path_name_key:
                    existing_by_provider_path_key[(provider_key, path_name_key)] = entry
                if provider_key and original_name_key and entry.size_bytes is not None:
                    existing_by_name_size_key[
                        (provider_key, original_name_key, entry.size_bytes)
                    ] = entry

            for persisted_entry in item.media_entries:
                _register_existing_entry(persisted_entry)

            selected_files = [
                file
                for file in torrent_info.files
                if file.selected or file.download_url is not None
            ]
            persisted: list[MediaEntryORM] = []
            provider_key = _normalized_entry_text(provider)
            provider_download_key = _normalized_entry_text(provider_download_id)
            for index, file in enumerate(selected_files):
                resolved_url = file.download_url or (
                    download_urls[index] if index < len(download_urls) else None
                )
                if resolved_url is None:
                    continue

                file_id_key = _normalized_entry_text(file.file_id)
                file_name_key = _normalized_entry_text(file.file_name)
                file_path_key = _normalized_entry_text(file.file_path) or file_name_key

                existing: MediaEntryORM | None = None
                if provider_download_key and file_id_key and file_name_key:
                    strict_key = (provider_download_key, file_id_key, file_name_key)
                    existing = existing_by_strict_key.get(strict_key)
                if existing is None and provider_key and file_path_key:
                    existing = existing_by_provider_path_key.get((provider_key, file_path_key))
                if (
                    existing is None
                    and provider_key
                    and file_name_key
                    and file.file_size_bytes is not None
                ):
                    existing = existing_by_name_size_key.get(
                        (provider_key, file_name_key, file.file_size_bytes)
                    )

                if existing is None:
                    existing = MediaEntryORM(
                        item_id=media_item_id,
                        entry_type="media",
                        kind="remote-direct",
                        original_filename=file.file_name,
                        download_url=resolved_url,
                        unrestricted_url=None,
                        provider=provider,
                        provider_download_id=provider_download_id,
                        provider_file_id=file.file_id,
                        provider_file_path=file_path_key,
                        size_bytes=file.file_size_bytes,
                        refresh_state="stale",
                    )
                    session.add(existing)
                else:
                    existing.entry_type = "media"
                    existing.kind = "remote-direct"
                    existing.original_filename = file.file_name
                    existing.download_url = resolved_url
                    existing.unrestricted_url = None
                    existing.provider = provider
                    existing.provider_download_id = provider_download_id
                    existing.provider_file_id = file.file_id
                    existing.provider_file_path = file_path_key
                    existing.size_bytes = file.file_size_bytes
                    existing.refresh_state = "stale"
                    existing.last_refresh_error = None

                _register_existing_entry(existing)
                persisted.append(existing)

            await session.commit()
            return persisted

    async def persist_ranked_stream_results(
        self,
        *,
        media_item_id: str,
        ranked_results: list[RankedStreamCandidateRecord],
    ) -> list[StreamORM]:
        """Persist ranked worker-stage results back onto existing stream rows."""

        async with self._db.session() as session:
            item = (
                await session.execute(
                    select(MediaItemORM)
                    .options(selectinload(MediaItemORM.streams))
                    .where(MediaItemORM.id == media_item_id)
                )
            ).scalar_one_or_none()
            if item is None:
                raise ValueError(f"unknown media_item_id={media_item_id}")

            streams_by_id = {stream.id: stream for stream in item.streams}
            persisted: list[StreamORM] = []
            for ranked in ranked_results:
                stream = streams_by_id.get(ranked.stream_id)
                if stream is None:
                    continue
                stream.rank = ranked.rank_score
                stream.lev_ratio = ranked.lev_ratio
                persisted.append(stream)
            await session.commit()
            return persisted

    async def list_items_in_states(self, *, states: list[ItemState]) -> list[MediaItemRecord]:
        """Return media items currently in one of the supplied lifecycle states."""

        if not states:
            return []
        state_values = [state.value for state in states]
        async with self._db.session() as session:
            items = (
                (
                    await session.execute(
                        select(MediaItemORM)
                        .where(MediaItemORM.state.in_(state_values))
                        .order_by(MediaItemORM.updated_at.desc(), MediaItemORM.created_at.desc())
                    )
                )
                .scalars()
                .all()
            )
            return [
                MediaItemRecord(
                    id=item.id,
                    external_ref=item.external_ref,
                    title=item.title,
                    state=ItemState(item.state),
                    tenant_id=item.tenant_id,
                    attributes=dict(cast(dict[str, object], item.attributes or {})),
                )
                for item in items
            ]

    async def get_recovery_plan(self, *, media_item_id: str) -> RecoveryPlanRecord | None:
        """Return the intentional automatic-recovery plan for one media item."""

        async with self._db.session() as session:
            item = (
                await session.execute(
                    select(MediaItemORM)
                    .options(selectinload(MediaItemORM.scrape_candidates))
                    .where(MediaItemORM.id == media_item_id)
                )
            ).scalar_one_or_none()
            if item is None:
                return None
            return _build_recovery_plan_record(
                state=ItemState(item.state),
                next_retry_at=item.next_retry_at,
                recovery_attempt_count=item.recovery_attempt_count or 0,
                has_scrape_candidates=bool(item.scrape_candidates),
            )

    @staticmethod
    def _build_workflow_checkpoint_record(
        checkpoint: ItemWorkflowCheckpointORM,
    ) -> WorkflowCheckpointRecord:
        """Return one detached workflow checkpoint record safe for worker resumption."""

        return WorkflowCheckpointRecord(
            workflow_name=checkpoint.workflow_name,
            stage_name=checkpoint.stage_name,
            resume_stage=WorkflowResumeStage(checkpoint.resume_stage),
            status=WorkflowCheckpointStatus(checkpoint.status),
            item_request_id=checkpoint.item_request_id,
            selected_stream_id=checkpoint.selected_stream_id,
            provider=checkpoint.provider,
            provider_download_id=checkpoint.provider_download_id,
            checkpoint_payload=dict(cast(dict[str, object], checkpoint.checkpoint_payload or {})),
            compensation_payload=dict(
                cast(dict[str, object], checkpoint.compensation_payload or {})
            ),
            last_error=checkpoint.last_error,
            updated_at=_serialize_datetime(checkpoint.updated_at),
        )

    async def get_workflow_checkpoint(
        self,
        *,
        media_item_id: str,
        workflow_name: str = "item_pipeline",
    ) -> WorkflowCheckpointRecord | None:
        """Return the current persisted workflow checkpoint for one media item."""

        async with self._db.session() as session:
            checkpoint = (
                await session.execute(
                    select(ItemWorkflowCheckpointORM).where(
                        ItemWorkflowCheckpointORM.item_id == media_item_id,
                        ItemWorkflowCheckpointORM.workflow_name == workflow_name,
                    )
                )
            ).scalar_one_or_none()
            if checkpoint is None:
                return None
            return self._build_workflow_checkpoint_record(checkpoint)

    async def list_workflow_drill_candidates(
        self,
        *,
        workflow_name: str = "item_pipeline",
        limit: int = 100,
    ) -> list[WorkflowDrillCandidateRecord]:
        """Return recoverable workflow checkpoints with their current item recovery posture."""

        bounded_limit = max(1, min(limit, 500))
        async with self._db.session() as session:
            rows = (
                await session.execute(
                    select(ItemWorkflowCheckpointORM, MediaItemORM)
                    .join(MediaItemORM, MediaItemORM.id == ItemWorkflowCheckpointORM.item_id)
                    .options(selectinload(MediaItemORM.scrape_candidates))
                    .where(
                        ItemWorkflowCheckpointORM.workflow_name == workflow_name,
                        ItemWorkflowCheckpointORM.status.in_(
                            (
                                WorkflowCheckpointStatus.PENDING.value,
                                WorkflowCheckpointStatus.RUNNING.value,
                                WorkflowCheckpointStatus.FAILED.value,
                            )
                        ),
                    )
                    .order_by(ItemWorkflowCheckpointORM.updated_at.desc())
                    .limit(bounded_limit)
                )
            ).all()

        candidates: list[WorkflowDrillCandidateRecord] = []
        for checkpoint, item in rows:
            item_state = ItemState(item.state)
            candidates.append(
                WorkflowDrillCandidateRecord(
                    media_item_id=item.id,
                    tenant_id=item.tenant_id,
                    item_state=item_state,
                    recovery_plan=_build_recovery_plan_record(
                        state=item_state,
                        next_retry_at=item.next_retry_at,
                        recovery_attempt_count=item.recovery_attempt_count or 0,
                        has_scrape_candidates=bool(item.scrape_candidates),
                    ),
                    checkpoint=self._build_workflow_checkpoint_record(checkpoint),
                )
            )
        return candidates

    async def persist_workflow_checkpoint(
        self,
        *,
        media_item_id: str,
        stage_name: str,
        resume_stage: WorkflowResumeStage,
        status: WorkflowCheckpointStatus,
        workflow_name: str = "item_pipeline",
        item_request_id: str | None = None,
        selected_stream_id: str | None = None,
        provider: str | None = None,
        provider_download_id: str | None = None,
        checkpoint_payload: dict[str, object] | None = None,
        compensation_payload: dict[str, object] | None = None,
        last_error: str | None = None,
    ) -> WorkflowCheckpointRecord:
        """Upsert one durable item-workflow checkpoint for resumable stage execution."""

        async with self._db.session() as session:
            item = (
                await session.execute(select(MediaItemORM).where(MediaItemORM.id == media_item_id))
            ).scalar_one_or_none()
            if item is None:
                raise ValueError(f"unknown media_item_id={media_item_id}")

            checkpoint = (
                await session.execute(
                    select(ItemWorkflowCheckpointORM).where(
                        ItemWorkflowCheckpointORM.item_id == media_item_id,
                        ItemWorkflowCheckpointORM.workflow_name == workflow_name,
                    )
                )
            ).scalar_one_or_none()
            if checkpoint is None:
                checkpoint = ItemWorkflowCheckpointORM(
                    item_id=media_item_id,
                    workflow_name=workflow_name,
                    stage_name=stage_name,
                    resume_stage=resume_stage.value,
                    status=status.value,
                )
                session.add(checkpoint)

            checkpoint.stage_name = stage_name
            checkpoint.resume_stage = resume_stage.value
            checkpoint.status = status.value
            checkpoint.item_request_id = item_request_id
            checkpoint.selected_stream_id = selected_stream_id
            checkpoint.provider = provider
            checkpoint.provider_download_id = provider_download_id
            checkpoint.checkpoint_payload = dict(checkpoint_payload or {})
            checkpoint.compensation_payload = dict(compensation_payload or {})
            checkpoint.last_error = last_error

            await session.commit()
            return self._build_workflow_checkpoint_record(checkpoint)

    async def get_stats_snapshot(self) -> MediaStatsSnapshot:
        """Return aggregated statistics for dashboard compatibility routes."""

        projection = await self.get_stats()
        return MediaStatsSnapshot(
            total_items=projection.total_items,
            total_movies=projection.movies,
            total_shows=projection.shows,
            total_seasons=projection.seasons,
            total_episodes=projection.episodes,
            total_symlinks=0,
            incomplete_items=projection.incomplete_items,
            states=projection.states,
            activity=projection.activity,
            media_year_releases=projection.media_year_releases,
        )

    async def get_stats(self, *, tenant_id: str | None = None) -> StatsProjection:
        """Return a first-class domain-backed stats projection for dashboard reads."""

        async with self._db.session() as session:
            statement = (
                select(MediaItemORM)
                .options(
                    selectinload(MediaItemORM.movie),
                    selectinload(MediaItemORM.show),
                    selectinload(MediaItemORM.season),
                    selectinload(MediaItemORM.episode),
                )
                .order_by(MediaItemORM.created_at.desc())
            )
            if tenant_id is not None:
                statement = statement.where(MediaItemORM.tenant_id == tenant_id)
            rows = ((await session.execute(statement)).scalars().all())

        state_counts: Counter[str] = Counter()
        activity_counts: Counter[str] = Counter()
        year_counts: Counter[int] = Counter()
        movies = 0
        shows = 0
        seasons = 0
        episodes = 0

        for item in rows:
            normalized_state = _canonical_state_name(str(item.state))
            state_counts[normalized_state] += 1
            activity_counts[item.created_at.date().isoformat()] += 1

            if item.movie is not None:
                movies += 1
            if item.show is not None:
                shows += 1
            if item.season is not None:
                seasons += 1
            if item.episode is not None:
                episodes += 1

            metadata = cast(dict[str, object], item.attributes or {})

            raw_year = metadata.get("year")
            if isinstance(raw_year, int):
                year_counts[raw_year] += 1
            elif isinstance(raw_year, str) and raw_year.isdigit():
                year_counts[int(raw_year)] += 1

        total_items = len(rows)
        completed_items = state_counts.get(ItemState.COMPLETED.value.title(), 0)
        failed_items = state_counts.get(ItemState.FAILED.value.title(), 0)
        incomplete_items = max(0, total_items - completed_items - failed_items)

        canonical_states = {
            ItemState.REQUESTED.value.title(): state_counts.get(
                ItemState.REQUESTED.value.title(), 0
            ),
            ItemState.INDEXED.value.title(): state_counts.get(ItemState.INDEXED.value.title(), 0),
            ItemState.SCRAPED.value.title(): state_counts.get(ItemState.SCRAPED.value.title(), 0),
            ItemState.DOWNLOADED.value.title(): state_counts.get(
                ItemState.DOWNLOADED.value.title(), 0
            ),
            _canonical_state_name(ItemState.PARTIALLY_COMPLETED.value): state_counts.get(
                _canonical_state_name(ItemState.PARTIALLY_COMPLETED.value), 0
            ),
            _canonical_state_name(ItemState.ONGOING.value): state_counts.get(
                _canonical_state_name(ItemState.ONGOING.value), 0
            ),
            ItemState.COMPLETED.value.title(): completed_items,
            ItemState.FAILED.value.title(): failed_items,
            ItemState.UNRELEASED.value.title(): state_counts.get(ItemState.UNRELEASED.value.title(), 0),
        }

        return StatsProjection(
            total_items=total_items,
            completed_items=completed_items,
            failed_items=failed_items,
            incomplete_items=incomplete_items,
            movies=movies,
            shows=shows,
            episodes=episodes,
            seasons=seasons,
            states=canonical_states,
            activity=dict(sorted(activity_counts.items())),
            media_year_releases=[
                StatsYearReleaseRecord(year=year, count=count)
                for year, count in sorted(year_counts.items())
            ],
        )

    async def search_items(
        self,
        *,
        limit: int = 24,
        page: int = 1,
        item_types: list[str] | None = None,
        states: list[str] | None = None,
        sort: list[str] | None = None,
        search: str | None = None,
        extended: bool = False,
        tenant_id: str | None = None,
        allowed_item_ids: set[str] | None = None,
    ) -> MediaItemsPage:
        """Return paginated item summaries for current library compatibility routes."""

        bounded_limit = max(1, min(limit, 100))
        bounded_page = max(1, page)

        async with self._db.session() as session:
            statement = (
                select(MediaItemORM)
                .options(*_projection_item_load_options())
                .order_by(MediaItemORM.created_at.desc())
            )
            if tenant_id is not None:
                statement = statement.where(MediaItemORM.tenant_id == tenant_id)
            rows = ((await session.execute(statement)).scalars().all())

        items = [_build_summary_record(item, extended=extended) for item in rows]
        filtered = [
            item
            for item in items
            if (allowed_item_ids is None or item.id in allowed_item_ids)
            and _matches_item_type(item, item_types)
            and _matches_state(item, states)
            and _matches_search(item, search)
        ]
        ordered = _sort_items(filtered, sort, search=search)
        total_items = len(ordered)
        total_pages = max(1, (total_items + bounded_limit - 1) // bounded_limit)
        start = (bounded_page - 1) * bounded_limit
        end = start + bounded_limit
        paged_items = [item for index, item in enumerate(ordered) if start <= index < end]
        paged_items = await self._hydrate_summary_records(paged_items)

        return MediaItemsPage(
            success=True,
            items=paged_items,
            page=bounded_page,
            limit=bounded_limit,
            total_items=total_items,
            total_pages=total_pages,
        )

    async def search_item_details(
        self,
        *,
        limit: int = 24,
        offset: int = 0,
        states: list[str] | None = None,
        query: str | None = None,
        provider: str | None = None,
        attachment_state: str | None = None,
        stream: str | None = None,
        has_errors: bool = False,
        sort: str | None = None,
        tenant_id: str | None = None,
    ) -> MediaItemsPage:
        """Return paginated item-detail rows for the playback recovery control surface."""

        bounded_limit = max(1, min(limit, 100))
        bounded_offset = max(0, offset)
        playback_service = PlaybackSourceService(self._db)

        async with self._db.session() as session:
            statement = (
                select(MediaItemORM)
                .options(
                    *_projection_item_load_options(),
                    selectinload(MediaItemORM.show).selectinload(ShowORM.seasons),
                    selectinload(MediaItemORM.item_requests),
                    selectinload(MediaItemORM.playback_attachments),
                    selectinload(MediaItemORM.media_entries).selectinload(
                        MediaEntryORM.source_attachment
                    ),
                    selectinload(MediaItemORM.subtitle_entries),
                    selectinload(MediaItemORM.active_streams),
                )
                .order_by(MediaItemORM.created_at.desc())
            )
            if tenant_id is not None:
                statement = statement.where(MediaItemORM.tenant_id == tenant_id)
            rows = ((await session.execute(statement)).scalars().all())

        details = [
            _build_detail_record(
                item,
                extended=True,
                playback_service=playback_service,
            )
            for item in rows
        ]
        filtered = [
            item
            for item in details
            if _matches_state(item, states)
            and _matches_playback_recovery_text_query(item, query)
            and _matches_playback_recovery_provider(item, provider)
            and _matches_playback_recovery_attachment_state(item, attachment_state)
            and _matches_playback_recovery_stream(item, stream)
            and (not has_errors or _item_has_playback_errors(item))
        ]
        ordered = _sort_playback_recovery_items(filtered, sort)
        total_items = len(ordered)
        total_pages = max(1, (total_items + bounded_limit - 1) // bounded_limit)
        paged_items = ordered[bounded_offset : bounded_offset + bounded_limit]

        return MediaItemsPage(
            success=True,
            items=paged_items,
            page=(bounded_offset // bounded_limit) + 1,
            limit=bounded_limit,
            total_items=total_items,
            total_pages=total_pages,
        )


    async def _fetch_request_search_page(
        self,
        client: TmdbMetadataClient,
        *,
        query: str,
        media_type: str,
        page: int,
    ) -> TmdbSearchPage:
        if media_type == "movie":
            return await client.search_movie_page(query, page=page)
        return await client.search_show_page(query, page=page)

    async def _collect_ranked_request_search_hits(
        self,
        *,
        query: str,
        media_type: str | None,
        scan_target: int,
    ) -> tuple[list[TmdbSearchResult], bool]:
        normalized_query = query.strip()
        if not normalized_query:
            return ([], True)

        normalized_media_type = _normalize_requested_media_type(media_type)
        if normalized_media_type in {"season", "episode"}:
            normalized_media_type = "show"

        client = self._resolve_tmdb_client()
        if client is None:
            return ([], True)

        selected_media_types = (
            [normalized_media_type]
            if normalized_media_type in {"movie", "show"}
            else ["movie", "show"]
        )
        raw_hits: list[TmdbSearchResult] = []
        window_complete = True
        per_type_scan_target = max(20, scan_target // max(len(selected_media_types), 1))

        for selected_media_type in selected_media_types:
            page_number = 1
            total_pages: int | None = None
            media_hits: list[TmdbSearchResult] = []
            while page_number <= _REQUEST_SEARCH_MAX_REMOTE_PAGES:
                page_result = await self._fetch_request_search_page(
                    client,
                    query=normalized_query,
                    media_type=selected_media_type,
                    page=page_number,
                )
                total_pages = page_result.total_pages
                media_hits.extend(page_result.results)
                if page_number >= total_pages or not page_result.results:
                    break
                if len(media_hits) >= per_type_scan_target:
                    window_complete = False
                    break
                page_number += 1

            if total_pages is not None and page_number < total_pages:
                window_complete = False
            raw_hits.extend(media_hits)

        if not raw_hits:
            return ([], True)

        deduped: dict[str, TmdbSearchResult] = {}
        for hit in raw_hits:
            external_ref = f"tmdb:{hit.tmdb_id}"
            current = deduped.get(external_ref)
            if current is None or _request_search_score(hit, query=normalized_query) > _request_search_score(
                current, query=normalized_query
            ):
                deduped[external_ref] = hit

        return (_sort_request_search_hits(list(deduped.values()), query=normalized_query), window_complete)

    async def _build_request_search_local_signal_map(
        self,
        hits: list[TmdbSearchResult],
        *,
        tenant_id: str | None,
    ) -> dict[str, RequestSearchLocalSignalRecord]:
        """Return batched local-demand signals for the current TMDB hit window."""

        if not hits or not hasattr(self._db, "session"):
            return {}

        external_refs = [f"tmdb:{hit.tmdb_id}" for hit in hits if hit.tmdb_id]
        if not external_refs:
            return {}

        now = datetime.now(UTC)
        active_cutoff = now - _CONSUMER_PLAYBACK_ACTIVE_WINDOW
        playback_cutoff = now - _REQUEST_LOCAL_SIGNAL_PLAYBACK_WINDOW

        async with self._db.session() as session:
            item_query = select(MediaItemORM).where(MediaItemORM.external_ref.in_(external_refs))
            if tenant_id is not None:
                item_query = item_query.where(MediaItemORM.tenant_id == tenant_id)
            item_rows = (
                await session.execute(item_query.options(selectinload(MediaItemORM.media_entries)))
            ).scalars().all()

            latest_items_by_ref: dict[str, MediaItemORM] = {}
            for row in item_rows:
                current = latest_items_by_ref.get(row.external_ref)
                if current is None or row.created_at > current.created_at:
                    latest_items_by_ref[row.external_ref] = row

            if not latest_items_by_ref:
                return {}

            selected_items = list(latest_items_by_ref.values())
            item_ids = [row.id for row in selected_items]

            request_rows = (
                await session.execute(
                    select(ItemRequestORM)
                    .where(ItemRequestORM.media_item_id.in_(item_ids))
                    .order_by(ItemRequestORM.last_requested_at.desc(), ItemRequestORM.created_at.desc())
                )
            ).scalars().all()
            latest_requests: dict[str, ItemRequestORM] = {}
            for row in request_rows:
                latest_requests.setdefault(row.media_item_id, row)

            event_query = select(ConsumerPlaybackActivityEventORM).where(
                ConsumerPlaybackActivityEventORM.item_id.in_(item_ids),
                ConsumerPlaybackActivityEventORM.occurred_at >= playback_cutoff,
            )
            if tenant_id is not None:
                event_query = event_query.where(ConsumerPlaybackActivityEventORM.tenant_id == tenant_id)
            events = (
                await session.execute(
                    event_query.order_by(
                        ConsumerPlaybackActivityEventORM.occurred_at.desc(),
                        ConsumerPlaybackActivityEventORM.created_at.desc(),
                    )
                )
            ).scalars().all()

        playback_rollups: dict[str, dict[str, object]] = {}
        for event in events:
            payload = cast(dict[str, object], event.payload or {})
            session_key = _coerce_activity_payload_str(payload, "session_key")
            position_seconds = _coerce_activity_payload_seconds(payload, "position_seconds")
            completed = _coerce_activity_payload_bool(payload, "completed")
            is_completed = bool(completed or event.activity_kind == "complete")

            rollup = playback_rollups.setdefault(
                event.item_id,
                {
                    "launch_count": 0,
                    "view_count": 0,
                    "session_keys": set(),
                    "active_session_keys": set(),
                    "completed_session_keys": set(),
                    "resume_position_seconds": None,
                    "progress_recorded_at": None,
                },
            )
            if event.activity_kind == "launch":
                rollup["launch_count"] = int(rollup["launch_count"]) + 1
            elif event.activity_kind == "view":
                rollup["view_count"] = int(rollup["view_count"]) + 1

            if session_key is not None:
                cast(set[str], rollup["session_keys"]).add(session_key)
                if event.occurred_at >= active_cutoff and not is_completed:
                    cast(set[str], rollup["active_session_keys"]).add(session_key)
                if is_completed:
                    cast(set[str], rollup["completed_session_keys"]).add(session_key)

            progress_recorded_at = cast(datetime | None, rollup["progress_recorded_at"])
            if (position_seconds is not None or is_completed) and (
                progress_recorded_at is None
                or event.occurred_at >= progress_recorded_at
            ):
                rollup["resume_position_seconds"] = None if is_completed else position_seconds
                rollup["progress_recorded_at"] = event.occurred_at

        signal_map: dict[str, RequestSearchLocalSignalRecord] = {}
        for hit in hits:
            external_ref = f"tmdb:{hit.tmdb_id}"
            item_row = latest_items_by_ref.get(external_ref)
            if item_row is None:
                continue

            latest_request = latest_requests.get(item_row.id)
            playback = playback_rollups.get(item_row.id, {})
            signal = RequestSearchLocalSignalRecord(
                item=_build_media_item_record_from_orm(item_row),
                request_source=(
                    _normalize_request_source(latest_request.request_source)
                    if latest_request is not None
                    else None
                ),
                request_count=(int(latest_request.request_count) if latest_request is not None else 0),
                first_requested_at=(
                    _serialize_datetime(latest_request.first_requested_at)
                    if latest_request is not None
                    else None
                ),
                last_requested_at=(
                    _serialize_datetime(latest_request.last_requested_at)
                    if latest_request is not None
                    else None
                ),
                requested_seasons=(
                    _clone_requested_seasons(latest_request.requested_seasons)
                    if latest_request is not None and latest_request.requested_seasons is not None
                    else None
                ),
                requested_episodes=(
                    _clone_requested_episodes(latest_request.requested_episodes)
                    if latest_request is not None and latest_request.requested_episodes is not None
                    else None
                ),
                launch_count=int(playback.get("launch_count", 0)),
                view_count=int(playback.get("view_count", 0)),
                session_count=len(cast(set[str], playback.get("session_keys", set()))),
                active_session_count=len(cast(set[str], playback.get("active_session_keys", set()))),
                completed_session_count=len(cast(set[str], playback.get("completed_session_keys", set()))),
                resume_position_seconds=cast(int | None, playback.get("resume_position_seconds")),
            )
            signal = replace(
                signal,
                ranking_boost=_build_request_search_local_boost(signal),
            )
            signal = replace(
                signal,
                ranking_signals=_build_request_search_ranking_signals(signal),
            )
            signal_map[_request_search_local_signal_key(hit.media_type, hit.tmdb_id)] = signal

        return signal_map


    async def _build_request_search_candidate_record(
        self,
        hit: TmdbSearchResult,
        *,
        tenant_id: str | None,
        local_signal: RequestSearchLocalSignalRecord | None = None,
    ) -> RequestSearchCandidateRecord:
        resolved_media_type = "movie" if hit.media_type == "movie" else "show"
        external_ref = f"tmdb:{hit.tmdb_id}"
        existing_item = (
            local_signal.item
            if local_signal is not None and local_signal.item is not None
            else await self.get_item_by_external_id(
                external_ref,
                media_type=resolved_media_type,
                tenant_id=tenant_id,
            )
        )
        existing_attributes = existing_item.attributes if existing_item is not None else {}
        workflow_checkpoint = (
            await self.get_workflow_checkpoint(media_item_id=existing_item.id)
            if existing_item is not None
            else None
        )
        recovery_plan = (
            await self.get_recovery_plan(media_item_id=existing_item.id)
            if existing_item is not None
            else None
        )
        return RequestSearchCandidateRecord(
            external_ref=external_ref,
            title=existing_item.title if existing_item is not None else hit.title,
            media_type=resolved_media_type,
            tmdb_id=hit.tmdb_id,
            tvdb_id=(
                str(existing_attributes.get("tvdb_id"))
                if existing_attributes.get("tvdb_id") is not None
                else None
            ),
            imdb_id=(
                str(existing_attributes.get("imdb_id"))
                if existing_attributes.get("imdb_id") is not None
                else None
            ),
            poster_path=_normalize_poster_path(
                hit.poster_path
                if hit.poster_path is not None
                else _extract_string(existing_attributes, "poster_path")
            ),
            overview=hit.overview,
            year=hit.year,
            is_requested=existing_item is not None,
            requested_item_id=(existing_item.id if existing_item is not None else None),
            requested_state=(existing_item.state.value if existing_item is not None else None),
            requested_seasons=(
                list(local_signal.requested_seasons)
                if local_signal is not None and local_signal.requested_seasons is not None
                else None
            ),
            requested_episodes=(
                _clone_requested_episodes(local_signal.requested_episodes)
                if local_signal is not None and local_signal.requested_episodes is not None
                else None
            ),
            request_source=(local_signal.request_source if local_signal is not None else None),
            request_count=(local_signal.request_count if local_signal is not None else 0),
            first_requested_at=(local_signal.first_requested_at if local_signal is not None else None),
            last_requested_at=(local_signal.last_requested_at if local_signal is not None else None),
            lifecycle=_build_request_search_lifecycle_record(
                checkpoint=workflow_checkpoint,
                recovery_plan=recovery_plan,
            ),
            ranking_signals=(
                local_signal.ranking_signals
                if local_signal is not None
                else ()
            ),
        )

    async def _build_request_candidate_season_preview(
        self,
        *,
        metadata: ShowMetadata | None,
        requested_item_id: str | None,
        requested_seasons: list[int] | None,
        requested_episodes: dict[str, list[int]] | None,
        tenant_id: str | None,
    ) -> tuple[RequestCandidateSeasonSummaryRecord | None, tuple[RequestCandidateSeasonRecord, ...]]:
        """Build one focused show-season preview for the dedicated requester detail route."""

        if metadata is None or not metadata.seasons:
            return None, ()

        local_season_numbers: set[int] = set()
        if requested_item_id is not None:
            detail = await self.get_item_detail(
                requested_item_id,
                media_type="item",
                extended=False,
                tenant_id=tenant_id,
            )
            if detail is not None and detail.covered_season_numbers is not None:
                local_season_numbers = {
                    int(season_number)
                    for season_number in detail.covered_season_numbers
                    if isinstance(season_number, int) and season_number > 0
                }

        normalized_requested_seasons = {
            int(season_number)
            for season_number in (requested_seasons or [])
            if isinstance(season_number, int) and season_number > 0
        }
        normalized_requested_episodes = {
            str(season_key): sorted(
                {
                    int(episode_number)
                    for episode_number in episode_numbers
                    if int(episode_number) > 0
                }
            )
            for season_key, episode_numbers in (requested_episodes or {}).items()
        }

        now_utc = datetime.now(UTC)
        preview: list[RequestCandidateSeasonRecord] = []
        next_air_date: datetime | None = None

        for season_payload in metadata.seasons:
            if not isinstance(season_payload, dict):
                continue
            season_number = _extract_int(cast(dict[str, object], season_payload), "season_number")
            if season_number is None or season_number < 1:
                continue

            episode_count = _extract_int(cast(dict[str, object], season_payload), "episode_count")
            air_date = _extract_string(cast(dict[str, object], season_payload), "air_date")
            parsed_air_date = _parse_calendar_datetime(air_date)
            is_released = parsed_air_date is None or parsed_air_date <= now_utc
            if (
                parsed_air_date is not None
                and parsed_air_date > now_utc
                and (next_air_date is None or parsed_air_date < next_air_date)
            ):
                next_air_date = parsed_air_date

            requested_episode_numbers = normalized_requested_episodes.get(str(season_number))
            requested_all_episodes = season_number in normalized_requested_seasons
            requested_episode_count = (
                len(requested_episode_numbers)
                if requested_episode_numbers is not None
                else 0
            )
            if requested_all_episodes and episode_count is not None:
                requested_episode_count = episode_count
            elif (
                not requested_all_episodes
                and episode_count is not None
                and requested_episode_numbers is not None
                and len(requested_episode_numbers) >= episode_count
            ):
                requested_all_episodes = True
                requested_episode_count = episode_count

            has_local_coverage = season_number in local_season_numbers
            is_requested = requested_all_episodes or requested_episode_count > 0
            if has_local_coverage:
                status = "local"
            elif not is_released:
                status = "upcoming"
            elif requested_all_episodes:
                status = "requested"
            elif requested_episode_count > 0:
                status = "partial"
            else:
                status = "available"

            preview.append(
                RequestCandidateSeasonRecord(
                    season_number=season_number,
                    title=_extract_string(cast(dict[str, object], season_payload), "name"),
                    episode_count=episode_count,
                    air_date=air_date,
                    is_released=is_released,
                    has_local_coverage=has_local_coverage,
                    is_requested=is_requested,
                    requested_episode_count=requested_episode_count,
                    requested_all_episodes=requested_all_episodes,
                    status=status,
                )
            )

        if not preview:
            return None, ()

        return (
            RequestCandidateSeasonSummaryRecord(
                total_seasons=len(preview),
                released_seasons=sum(1 for season in preview if season.is_released),
                requested_seasons=sum(1 for season in preview if season.is_requested),
                partial_seasons=sum(
                    1
                    for season in preview
                    if season.status == "partial"
                ),
                local_seasons=sum(
                    1
                    for season in preview
                    if season.has_local_coverage
                ),
                unreleased_seasons=sum(
                    1
                    for season in preview
                    if not season.is_released
                ),
                next_air_date=_serialize_datetime(next_air_date),
            ),
            tuple(sorted(preview, key=lambda season: season.season_number)),
        )

    async def _build_request_history_candidate_record(
        self,
        *,
        item_row: MediaItemORM,
        request_row: ItemRequestORM,
    ) -> RequestSearchCandidateRecord | None:
        """Build one persisted requester-history candidate without a TMDB round-trip."""

        external_ref = item_row.external_ref.strip() if item_row.external_ref else ""
        if not external_ref:
            return None

        attributes = (
            dict(cast(dict[str, object], item_row.attributes))
            if isinstance(item_row.attributes, dict)
            else {}
        )
        normalized_media_type = _canonical_item_type_name(
            _extract_string(attributes, "item_type")
        )
        if normalized_media_type not in {"movie", "show"}:
            return None

        aired_at = _parse_calendar_datetime(_extract_string(attributes, "aired_at"))
        workflow_checkpoint = await self.get_workflow_checkpoint(media_item_id=item_row.id)
        recovery_plan = await self.get_recovery_plan(media_item_id=item_row.id)

        return RequestSearchCandidateRecord(
            external_ref=external_ref,
            title=item_row.title,
            media_type=normalized_media_type,
            tmdb_id=_extract_string(attributes, "tmdb_id"),
            tvdb_id=_extract_string(attributes, "tvdb_id"),
            imdb_id=_extract_string(attributes, "imdb_id"),
            poster_path=_extract_string(attributes, "poster_path"),
            overview=_extract_string(attributes, "overview") or "",
            year=(aired_at.year if aired_at is not None else _extract_int(attributes, "year")),
            is_requested=True,
            requested_item_id=item_row.id,
            requested_state=item_row.state.value,
            requested_seasons=_clone_requested_seasons(request_row.requested_seasons),
            requested_episodes=_clone_requested_episodes(request_row.requested_episodes),
            request_source=_normalize_request_source(request_row.request_source),
            request_count=int(request_row.request_count or 0),
            first_requested_at=_serialize_datetime(request_row.first_requested_at),
            last_requested_at=_serialize_datetime(request_row.last_requested_at),
            lifecycle=_build_request_search_lifecycle_record(
                checkpoint=workflow_checkpoint,
                recovery_plan=recovery_plan,
            ),
        )

    async def search_request_candidates(
        self,
        *,
        query: str,
        media_type: str | None = None,
        limit: int = 12,
        tenant_id: str | None = None,
    ) -> list[RequestSearchCandidateRecord]:
        """Return TMDB-backed request-search hits plus current local request state."""

        page = await self.search_request_candidates_page(
            query=query,
            media_type=media_type,
            limit=limit,
            offset=0,
            tenant_id=tenant_id,
        )
        return page.items

    async def get_request_candidate(
        self,
        *,
        external_ref: str,
        media_type: str,
        tenant_id: str | None = None,
    ) -> RequestSearchCandidateRecord | None:
        """Return one request candidate by external reference without a search fallback."""

        normalized_external_ref = external_ref.strip()
        normalized_media_type = _canonical_item_type_name(media_type)

        if not normalized_external_ref:
            return None
        if normalized_media_type not in {"movie", "show"}:
            raise ValueError("mediaType must be either movie or show")

        existing_item = await self.get_item_by_external_id(
            normalized_external_ref,
            media_type=normalized_media_type,
            tenant_id=tenant_id,
        )
        existing_attributes = (
            existing_item.attributes
            if existing_item is not None and isinstance(existing_item.attributes, dict)
            else {}
        )
        tmdb_id = (
            normalized_external_ref.partition(":")[2].strip()
            if normalized_external_ref.startswith("tmdb:")
            else _extract_string(existing_attributes, "tmdb_id")
        )
        if not tmdb_id:
            return None

        client = self._resolve_tmdb_client()
        metadata = None
        external_ids: dict[str, str | None] = {}
        if client is not None:
            if normalized_media_type == "movie":
                metadata = await client.get_movie(tmdb_id)
                external_ids = await client.get_external_ids(tmdb_id, "movie")
            else:
                metadata = await client.get_show(tmdb_id)
                external_ids = await client.get_external_ids(tmdb_id, "tv")

        if metadata is None and existing_item is None:
            return None

        hit = TmdbSearchResult.model_validate(
            {
                "id": tmdb_id,
                "media_type": normalized_media_type,
                "title": (
                    metadata.title
                    if metadata is not None
                    else existing_item.title
                ),
                "year": (metadata.year if metadata is not None else None),
                "overview": (
                    metadata.overview
                    if metadata is not None
                    else _extract_string(existing_attributes, "overview") or ""
                ),
                "poster_path": (
                    metadata.poster_path
                    if metadata is not None
                    else _extract_string(existing_attributes, "poster_path")
                ),
                "popularity": 0.0,
                "vote_average": 0.0,
                "vote_count": 0,
                "original_language": None,
                "genre_names": (
                    list(metadata.genres)
                    if metadata is not None and hasattr(metadata, "genres")
                    else []
                ),
            }
        )
        local_signal = (
            await self._build_request_search_local_signal_map(
                [hit],
                tenant_id=tenant_id,
            )
        ).get(_request_search_local_signal_key(hit.media_type, hit.tmdb_id))
        candidate = await self._build_request_search_candidate_record(
            hit,
            tenant_id=tenant_id,
            local_signal=local_signal,
        )

        if existing_item is None and external_ids:
            candidate = replace(
                candidate,
                tvdb_id=external_ids.get("tvdb_id"),
                imdb_id=external_ids.get("imdb_id"),
            )

        if normalized_media_type == "show":
            season_summary, season_preview = await self._build_request_candidate_season_preview(
                metadata=(metadata if isinstance(metadata, ShowMetadata) else None),
                requested_item_id=candidate.requested_item_id,
                requested_seasons=candidate.requested_seasons,
                requested_episodes=candidate.requested_episodes,
                tenant_id=tenant_id,
            )
            candidate = replace(
                candidate,
                season_summary=season_summary,
                season_preview=season_preview,
            )

        return candidate

    async def get_request_history_page(
        self,
        *,
        media_type: str | None = None,
        limit: int = 6,
        offset: int = 0,
        tenant_id: str | None = None,
    ) -> RequestSearchPageRecord:
        """Return one paged view of the most recent persisted requester history."""

        normalized_media_type = (
            _canonical_item_type_name(media_type)
            if media_type is not None
            else None
        )
        if normalized_media_type not in {None, "movie", "show"}:
            raise ValueError("mediaType must be either movie or show when provided")

        bounded_limit = max(1, min(limit, 24))
        bounded_offset = max(0, offset)

        async with self._db.session() as session:
            statement = (
                select(ItemRequestORM, MediaItemORM)
                .join(MediaItemORM, MediaItemORM.id == ItemRequestORM.media_item_id)
                .order_by(
                    ItemRequestORM.last_requested_at.desc(),
                    ItemRequestORM.created_at.desc(),
                )
            )
            if tenant_id is not None:
                statement = statement.where(MediaItemORM.tenant_id == tenant_id)

            rows = (await session.execute(statement)).all()

        latest_rows: list[tuple[ItemRequestORM, MediaItemORM]] = []
        seen_item_ids: set[str] = set()
        for request_row, item_row in rows:
            if item_row.id in seen_item_ids:
                continue
            seen_item_ids.add(item_row.id)

            attributes = (
                dict(cast(dict[str, object], item_row.attributes))
                if isinstance(item_row.attributes, dict)
                else {}
            )
            item_media_type = _canonical_item_type_name(
                _extract_string(attributes, "item_type")
            )
            if item_media_type not in {"movie", "show"}:
                continue
            if normalized_media_type is not None and item_media_type != normalized_media_type:
                continue
            latest_rows.append((request_row, item_row))

        total_count = len(latest_rows)
        paged_rows = latest_rows[bounded_offset : bounded_offset + bounded_limit]
        items = [
            candidate
            for candidate in await asyncio.gather(
                *[
                    self._build_request_history_candidate_record(
                        item_row=item_row,
                        request_row=request_row,
                    )
                    for request_row, item_row in paged_rows
                ]
            )
            if candidate is not None
        ]

        return RequestSearchPageRecord(
            items=items,
            offset=bounded_offset,
            limit=bounded_limit,
            total_count=total_count,
            has_previous_page=bounded_offset > 0,
            has_next_page=bounded_offset + bounded_limit < total_count,
            result_window_complete=True,
        )

    async def search_request_candidates_page(
        self,
        *,
        query: str,
        media_type: str | None = None,
        limit: int = 12,
        offset: int = 0,
        tenant_id: str | None = None,
    ) -> RequestSearchPageRecord:
        """Return one bounded backend-ranked request-search page."""

        normalized_query = query.strip()
        if not normalized_query:
            return RequestSearchPageRecord(
                items=[],
                offset=max(offset, 0),
                limit=max(1, min(limit, 40)),
                total_count=0,
                has_previous_page=max(offset, 0) > 0,
                has_next_page=False,
                result_window_complete=True,
            )

        bounded_limit = max(1, min(limit, 40))
        bounded_offset = max(0, offset)
        scan_target = min(
            max(
                bounded_offset + bounded_limit + 40,
                _REQUEST_SEARCH_DEFAULT_SCAN_WINDOW,
            ),
            _REQUEST_SEARCH_MAX_SCAN_WINDOW,
        )
        ordered_hits, window_complete = await self._collect_ranked_request_search_hits(
            query=normalized_query,
            media_type=media_type,
            scan_target=scan_target,
        )
        local_signals = await self._build_request_search_local_signal_map(
            ordered_hits,
            tenant_id=tenant_id,
        )
        local_boosts = {
            key: signal.ranking_boost
            for key, signal in local_signals.items()
            if signal.ranking_boost > 0
        }
        ordered_hits = _sort_request_search_hits(
            ordered_hits,
            query=normalized_query,
            local_boosts=local_boosts,
        )
        paged_hits = ordered_hits[bounded_offset : bounded_offset + bounded_limit]
        items = [
            await self._build_request_search_candidate_record(
                hit,
                tenant_id=tenant_id,
                local_signal=local_signals.get(
                    _request_search_local_signal_key(hit.media_type, hit.tmdb_id)
                ),
            )
            for hit in paged_hits
        ]
        total_count = len(ordered_hits)
        has_next_page = (bounded_offset + bounded_limit) < total_count or not window_complete

        return RequestSearchPageRecord(
            items=items,
            offset=bounded_offset,
            limit=bounded_limit,
            total_count=total_count,
            has_previous_page=bounded_offset > 0,
            has_next_page=has_next_page,
            result_window_complete=window_complete,
        )

    async def _fetch_request_discovery_page(
        self,
        client: TmdbMetadataClient,
        *,
        media_type: str,
        page: int,
        genre: str | None,
        release_year: int | None,
        original_language: str | None,
        company: str | None,
        network: str | None,
        sort: str,
    ) -> TmdbSearchPage:
        if media_type == "movie":
            return await client.discover_movie_page(
                page=page,
                genre=genre,
                release_year=release_year,
                original_language=original_language,
                company=company,
                sort_by=_tmdb_discovery_sort("movie", sort=sort),
            )
        return await client.discover_show_page(
            page=page,
            genre=genre,
            release_year=release_year,
            original_language=original_language,
            network=network,
            sort_by=_tmdb_discovery_sort("show", sort=sort),
        )

    async def _fetch_request_editorial_page(
        self,
        client: TmdbMetadataClient,
        *,
        media_type: str,
        family: str,
        page: int,
    ) -> TmdbSearchPage:
        fetch_page = (
            getattr(client, "editorial_movie_page", None)
            if media_type == "movie"
            else getattr(client, "editorial_show_page", None)
        )
        if fetch_page is None:
            return TmdbSearchPage(results=[], page=page, total_pages=1, total_results=0)
        return await fetch_page(family=family, page=page)

    async def _fetch_request_release_window_page(
        self,
        client: TmdbMetadataClient,
        *,
        media_type: str,
        window: str,
        page: int,
    ) -> TmdbSearchPage:
        fetch_page = (
            getattr(client, "release_window_movie_page", None)
            if media_type == "movie"
            else getattr(client, "release_window_show_page", None)
        )
        if fetch_page is None:
            return TmdbSearchPage(results=[], page=page, total_pages=1, total_results=0)
        return await fetch_page(window=window, page=page)

    async def discover_request_candidates_page(
        self,
        *,
        media_type: str | None = None,
        genre: str | None = None,
        release_year: int | None = None,
        original_language: str | None = None,
        company: str | None = None,
        network: str | None = None,
        sort: str | None = None,
        limit: int = 20,
        offset: int = 0,
        tenant_id: str | None = None,
    ) -> RequestDiscoveryPageRecord:
        """Return one backend-owned discover page with additive facet metadata."""

        bounded_limit = max(1, min(limit, 40))
        bounded_offset = max(0, offset)
        normalized_media_type = _normalize_requested_media_type(media_type)
        if normalized_media_type in {"season", "episode"}:
            normalized_media_type = "show"
        selected_media_types = (
            [normalized_media_type]
            if normalized_media_type in {"movie", "show"}
            else ["movie", "show"]
        )
        normalized_genre = _normalize_discovery_filter_value(genre)
        normalized_original_language = _normalize_discovery_filter_value(original_language)
        if normalized_original_language is not None:
            normalized_original_language = normalized_original_language.casefold()
        normalized_company = _normalize_discovery_filter_value(company)
        normalized_network = _normalize_discovery_filter_value(network)
        normalized_sort = _normalize_request_discovery_sort(sort)
        client = self._resolve_tmdb_client()
        facets = await _build_request_discovery_facets(
            [],
            client=client,
            selected_genre=normalized_genre,
            selected_release_year=release_year,
            selected_language=normalized_original_language,
            selected_company=normalized_company,
            selected_network=normalized_network,
            selected_sort=normalized_sort,
        )
        if client is None:
            return RequestDiscoveryPageRecord(
                items=[],
                offset=bounded_offset,
                limit=bounded_limit,
                total_count=0,
                has_previous_page=bounded_offset > 0,
                has_next_page=False,
                result_window_complete=True,
                facets=facets,
            )

        scan_target = min(
            max(
                bounded_offset + bounded_limit + 40,
                _REQUEST_DISCOVER_DEFAULT_SCAN_WINDOW,
            ),
            _REQUEST_DISCOVER_MAX_SCAN_WINDOW,
        )
        per_type_scan_target = max(20, scan_target // max(len(selected_media_types), 1))
        raw_hits: list[TmdbSearchResult] = []
        window_complete = True

        for selected_media_type in selected_media_types:
            page_number = 1
            total_pages: int | None = None
            media_hits: list[TmdbSearchResult] = []
            while page_number <= _REQUEST_DISCOVER_MAX_REMOTE_PAGES:
                page_result = await self._fetch_request_discovery_page(
                    client,
                    media_type=selected_media_type,
                    page=page_number,
                    genre=normalized_genre,
                    release_year=release_year,
                    original_language=normalized_original_language,
                    company=normalized_company,
                    network=normalized_network,
                    sort=normalized_sort,
                )
                total_pages = page_result.total_pages
                media_hits.extend(page_result.results)
                if page_number >= total_pages or not page_result.results:
                    break
                if len(media_hits) >= per_type_scan_target:
                    window_complete = False
                    break
                page_number += 1

            if total_pages is not None and page_number < total_pages:
                window_complete = False
            raw_hits.extend(media_hits)

        deduped: dict[str, TmdbSearchResult] = {}
        for hit in raw_hits:
            key = f"{hit.media_type}:{hit.tmdb_id}"
            current = deduped.get(key)
            if current is None or _request_discovery_sort_key(
                hit,
                sort=normalized_sort,
            ) < _request_discovery_sort_key(
                current,
                sort=normalized_sort,
            ):
                deduped[key] = hit

        local_signals = await self._build_request_search_local_signal_map(
            list(deduped.values()),
            tenant_id=tenant_id,
        )
        local_boosts = {
            key: signal.ranking_boost
            for key, signal in local_signals.items()
            if signal.ranking_boost > 0
        }
        blend_boosts = await _build_request_discovery_blend_boosts(
            self,
            client,
            selected_media_types=selected_media_types,
        )
        ordered_hits = _sort_request_discovery_hits(
            list(deduped.values()),
            sort=normalized_sort,
            blend_boosts=blend_boosts,
            local_boosts=local_boosts,
        )
        paged_hits = ordered_hits[bounded_offset : bounded_offset + bounded_limit]
        items = [
            await self._build_request_search_candidate_record(
                hit,
                tenant_id=tenant_id,
                local_signal=local_signals.get(
                    _request_search_local_signal_key(hit.media_type, hit.tmdb_id)
                ),
            )
            for hit in paged_hits
        ]
        facets = await _build_request_discovery_facets(
            ordered_hits,
            client=client,
            selected_genre=normalized_genre,
            selected_release_year=release_year,
            selected_language=normalized_original_language,
            selected_company=normalized_company,
            selected_network=normalized_network,
            selected_sort=normalized_sort,
        )
        total_count = len(ordered_hits)
        has_next_page = (bounded_offset + bounded_limit) < total_count or not window_complete

        return RequestDiscoveryPageRecord(
            items=items,
            offset=bounded_offset,
            limit=bounded_limit,
            total_count=total_count,
            has_previous_page=bounded_offset > 0,
            has_next_page=has_next_page,
            result_window_complete=window_complete,
            facets=facets,
        )

    async def discover_request_projection_groups(
        self,
        *,
        media_type: str | None = None,
        genre: str | None = None,
        release_year: int | None = None,
        original_language: str | None = None,
        company: str | None = None,
        network: str | None = None,
        sort: str | None = None,
        limit_per_group: int = 6,
        tenant_id: str | None = None,
    ) -> list[RequestDiscoveryProjectionGroupRecord]:
        """Return grouped discovery follow-ups derived from the current backend window."""

        bounded_limit = max(1, min(limit_per_group, 8))
        page = await self.discover_request_candidates_page(
            media_type=media_type,
            genre=genre,
            release_year=release_year,
            original_language=original_language,
            company=company,
            network=network,
            sort=sort,
            limit=_REQUEST_DISCOVERY_PROJECTION_WINDOW,
            offset=0,
            tenant_id=tenant_id,
        )
        if not page.items:
            return []

        client = self._resolve_tmdb_client()
        if client is None:
            return []

        semaphore = asyncio.Semaphore(_REQUEST_DISCOVERY_FACET_DETAIL_CONCURRENCY)

        async def load_profile(
            item: RequestSearchCandidateRecord,
        ) -> TmdbDiscoveryProfile | None:
            if not item.tmdb_id:
                return None
            async with semaphore:
                return await client.get_discovery_profile(item.tmdb_id, item.media_type)

        local_signal_map = await self._build_request_search_local_signal_map(
            page.items,
            tenant_id=tenant_id,
        )
        item_profiles = [
            (item, profile)
            for item, profile in zip(
                page.items,
                await asyncio.gather(
                    *(load_profile(item) for item in page.items),
                    return_exceptions=False,
                ),
                strict=False,
            )
            if profile is not None
        ]
        if not item_profiles:
            return []

        return _build_request_discovery_projection_groups(
            [item for item, _profile in item_profiles],
            [profile for _item, profile in item_profiles],
            local_signals={
                _request_search_local_signal_key(item.media_type, item.tmdb_id): signal
                for item, signal in ((entry, local_signal_map.get(_request_search_local_signal_key(entry.media_type, entry.tmdb_id))) for entry, _profile in item_profiles)
                if item.tmdb_id and signal is not None
            },
            limit_per_group=bounded_limit,
        )

    async def discover_request_editorial_families(
        self,
        *,
        limit_per_family: int = 8,
        family_ids: list[str] | None = None,
        tenant_id: str | None = None,
    ) -> list[RequestEditorialFamilyRecord]:
        """Return backend-owned editorial discovery families for consumer search."""

        bounded_limit = max(1, min(limit_per_family, 12))
        selected_family_ids = {
            family_id.strip() for family_id in family_ids or [] if family_id.strip()
        }
        family_definitions = [
            family
            for family in _REQUEST_EDITORIAL_DISCOVERY_FAMILIES
            if not selected_family_ids or family["id"] in selected_family_ids
        ]
        if not family_definitions:
            return []

        client = self._resolve_tmdb_client()
        if client is None:
            return []

        expanded_limit = min(40, max(bounded_limit * 2, bounded_limit))
        families: list[RequestEditorialFamilyRecord] = []
        for family in family_definitions:
            page_number = 1
            total_pages: int | None = None
            raw_hits: list[TmdbSearchResult] = []
            while page_number <= min(_REQUEST_DISCOVER_MAX_REMOTE_PAGES, 4):
                page_result = await self._fetch_request_editorial_page(
                    client,
                    media_type=cast(str, family["media_type"]),
                    family=cast(str, family["family"]),
                    page=page_number,
                )
                total_pages = page_result.total_pages
                raw_hits.extend(page_result.results)
                if page_number >= total_pages or not page_result.results:
                    break
                if len(raw_hits) >= expanded_limit:
                    break
                page_number += 1

            deduped: dict[str, TmdbSearchResult] = {}
            for hit in raw_hits:
                deduped.setdefault(f"{hit.media_type}:{hit.tmdb_id}", hit)

            ordered_hits = list(deduped.values())
            local_signals = await self._build_request_search_local_signal_map(
                ordered_hits,
                tenant_id=tenant_id,
            )
            local_boosts = {
                key: signal.ranking_boost
                for key, signal in local_signals.items()
                if signal.ranking_boost > 0
            }
            ordered_hits = _sort_request_window_hits(
                ordered_hits,
                local_boosts=local_boosts,
            )[:expanded_limit]
            items: list[RequestSearchCandidateRecord] = []
            for hit in ordered_hits:
                items.append(
                    await self._build_request_search_candidate_record(
                        hit,
                        tenant_id=tenant_id,
                        local_signal=local_signals.get(
                            _request_search_local_signal_key(hit.media_type, hit.tmdb_id)
                        ),
                    )
                )
                if len(items) >= bounded_limit:
                    break

            if not items and total_pages == 1:
                continue

            families.append(
                RequestEditorialFamilyRecord(
                    family_id=cast(str, family["id"]),
                    title=cast(str, family["title"]),
                    description=cast(str, family["description"]),
                    family=cast(str, family["family"]),
                    media_type=cast(str, family["media_type"]),
                    items=items,
                )
            )

        return families

    async def discover_request_release_windows(
        self,
        *,
        limit_per_window: int = 8,
        window_ids: list[str] | None = None,
        tenant_id: str | None = None,
    ) -> list[RequestReleaseWindowRecord]:
        """Return backend-owned release-window families for consumer search."""

        bounded_limit = max(1, min(limit_per_window, 12))
        selected_window_ids = {
            window_id.strip() for window_id in window_ids or [] if window_id.strip()
        }
        window_definitions = [
            window
            for window in _REQUEST_RELEASE_WINDOWS
            if not selected_window_ids or window["id"] in selected_window_ids
        ]
        if not window_definitions:
            return []

        client = self._resolve_tmdb_client()
        if client is None:
            return []

        expanded_limit = min(40, max(bounded_limit * 2, bounded_limit))
        windows: list[RequestReleaseWindowRecord] = []
        for window in window_definitions:
            page_number = 1
            total_pages: int | None = None
            raw_hits: list[TmdbSearchResult] = []
            while page_number <= min(_REQUEST_DISCOVER_MAX_REMOTE_PAGES, 4):
                page_result = await self._fetch_request_release_window_page(
                    client,
                    media_type=cast(str, window["media_type"]),
                    window=cast(str, window["window"]),
                    page=page_number,
                )
                total_pages = page_result.total_pages
                raw_hits.extend(page_result.results)
                if page_number >= total_pages or not page_result.results:
                    break
                if len(raw_hits) >= expanded_limit:
                    break
                page_number += 1

            deduped: dict[str, TmdbSearchResult] = {}
            for hit in raw_hits:
                deduped.setdefault(f"{hit.media_type}:{hit.tmdb_id}", hit)

            ordered_hits = list(deduped.values())
            local_signals = await self._build_request_search_local_signal_map(
                ordered_hits,
                tenant_id=tenant_id,
            )
            local_boosts = {
                key: signal.ranking_boost
                for key, signal in local_signals.items()
                if signal.ranking_boost > 0
            }
            ordered_hits = _sort_request_window_hits(
                ordered_hits,
                local_boosts=local_boosts,
            )[:expanded_limit]
            items: list[RequestSearchCandidateRecord] = []
            for hit in ordered_hits:
                items.append(
                    await self._build_request_search_candidate_record(
                        hit,
                        tenant_id=tenant_id,
                        local_signal=local_signals.get(
                            _request_search_local_signal_key(hit.media_type, hit.tmdb_id)
                        ),
                    )
                )
                if len(items) >= bounded_limit:
                    break

            if not items and total_pages == 1:
                continue

            windows.append(
                RequestReleaseWindowRecord(
                    window_id=cast(str, window["id"]),
                    title=cast(str, window["title"]),
                    description=cast(str, window["description"]),
                    window=cast(str, window["window"]),
                    media_type=cast(str, window["media_type"]),
                    items=items,
                )
            )

        return windows

    async def discover_request_candidates(
        self,
        *,
        limit_per_rail: int = 8,
        rail_ids: list[str] | None = None,
        tenant_id: str | None = None,
    ) -> list[RequestDiscoveryRailRecord]:
        """Return backend-owned zero-query discovery rails for consumer search."""

        bounded_limit = max(1, min(limit_per_rail, 12))
        selected_rail_ids = {rail_id.strip() for rail_id in rail_ids or [] if rail_id.strip()}
        rail_definitions = [
            rail
            for rail in _REQUEST_DISCOVERY_RAILS
            if not selected_rail_ids or rail["id"] in selected_rail_ids
        ]
        if not rail_definitions:
            return []

        expanded_limit = min(40, max(bounded_limit * 3, bounded_limit))
        discovery_results = await asyncio.gather(
            *[
                self.search_request_candidates(
                    query=cast(str, rail["query"]),
                    media_type=cast(str, rail["media_type"]),
                    limit=expanded_limit,
                    tenant_id=tenant_id,
                )
                for rail in rail_definitions
            ]
        )

        seen_external_refs: set[str] = set()
        rails: list[RequestDiscoveryRailRecord] = []
        for rail, candidates in zip(rail_definitions, discovery_results, strict=True):
            unique_candidates: list[RequestSearchCandidateRecord] = []
            for candidate in candidates:
                if candidate.external_ref in seen_external_refs:
                    continue
                seen_external_refs.add(candidate.external_ref)
                unique_candidates.append(candidate)
                if len(unique_candidates) >= bounded_limit:
                    break
            if not unique_candidates:
                continue
            rails.append(
                RequestDiscoveryRailRecord(
                    rail_id=cast(str, rail["id"]),
                    title=cast(str, rail["title"]),
                    description=cast(str, rail["description"]),
                    query=cast(str, rail["query"]),
                    media_type=cast(str, rail["media_type"]),
                    items=unique_candidates,
                )
            )

        return rails

    async def record_consumer_playback_activity(
        self,
        *,
        item_id: str,
        tenant_id: str,
        actor_id: str,
        actor_type: str,
        activity_kind: str,
        target: str | None = None,
        device_key: str,
        device_label: str,
        session_key: str | None = None,
        position_seconds: int | None = None,
        duration_seconds: int | None = None,
        completed: bool = False,
        occurred_at: datetime | None = None,
    ) -> None:
        """Persist one consumer playback activity event with optional session progress."""

        normalized_item_id = _normalize_internal_item_id(item_id)
        normalized_tenant_id = tenant_id.strip() or "global"
        normalized_actor_id = actor_id.strip()
        normalized_actor_type = actor_type.strip().casefold() or "unknown"
        normalized_activity_kind = activity_kind.strip().casefold()
        normalized_target = (target or "").strip().casefold() or None
        normalized_device_key = device_key.strip()[:128] or "unknown-device"
        normalized_device_label = device_label.strip()[:256] or "Current device"
        normalized_session_key = (session_key or "").strip()[:128] or None
        normalized_position_seconds = position_seconds
        normalized_duration_seconds = duration_seconds
        normalized_completed = bool(completed or normalized_activity_kind == "complete")

        if normalized_actor_type != "user":
            return
        if not normalized_actor_id:
            raise ValueError("actor_id must not be empty")
        if normalized_activity_kind not in {"view", "launch", "progress", "complete"}:
            raise ValueError("activity_kind must be one of view, launch, progress, or complete")
        if normalized_activity_kind == "launch" and normalized_target not in {"direct", "hls"}:
            raise ValueError("launch activity requires a direct or hls target")
        if normalized_activity_kind != "launch":
            normalized_target = None
        if normalized_activity_kind in {"progress", "complete"} and normalized_session_key is None:
            raise ValueError("progress and complete activity require a session_key")
        if (
            normalized_session_key is None
            and (
                normalized_position_seconds is not None
                or normalized_duration_seconds is not None
                or normalized_completed
            )
        ):
            raise ValueError("session_key is required when progress fields are recorded")
        if normalized_position_seconds is not None and normalized_position_seconds < 0:
            raise ValueError("position_seconds must be non-negative")
        if normalized_duration_seconds is not None and normalized_duration_seconds < 0:
            raise ValueError("duration_seconds must be non-negative")
        if (
            normalized_position_seconds is not None
            and normalized_duration_seconds is not None
            and normalized_position_seconds > normalized_duration_seconds
        ):
            raise ValueError("position_seconds must not exceed duration_seconds")
        payload: dict[str, object] = {}
        if normalized_session_key is not None:
            payload["session_key"] = normalized_session_key
        if normalized_position_seconds is not None:
            payload["position_seconds"] = normalized_position_seconds
        if normalized_duration_seconds is not None:
            payload["duration_seconds"] = normalized_duration_seconds
        if normalized_completed:
            payload["completed"] = True

        async with self._db.session() as session:
            item = (
                (
                    await session.execute(
                        select(MediaItemORM)
                        .options(*_projection_item_load_options())
                        .where(
                            MediaItemORM.id == normalized_item_id,
                            MediaItemORM.tenant_id == normalized_tenant_id,
                        )
                    )
                )
                .scalars()
                .one_or_none()
            )
            if item is None:
                raise ItemNotFoundError(f"item not found: {normalized_item_id}")

            session.add(
                ConsumerPlaybackActivityEventORM(
                    tenant_id=normalized_tenant_id,
                    actor_id=normalized_actor_id,
                    actor_type=normalized_actor_type,
                    item_id=item.id,
                    activity_kind=normalized_activity_kind,
                    target=normalized_target,
                    device_key=normalized_device_key,
                    device_label=normalized_device_label,
                    payload=payload,
                    occurred_at=occurred_at or datetime.now(UTC),
                    created_at=datetime.now(UTC),
                )
            )
            await session.commit()

    async def get_consumer_playback_activity(
        self,
        *,
        tenant_id: str,
        actor_id: str,
        actor_type: str,
        item_limit: int = 12,
        device_limit: int = 6,
        history_limit: int = 240,
        focus_item_id: str | None = None,
    ) -> ConsumerPlaybackActivityRecord:
        """Return shared consumer activity, session posture, and recent device rollups."""

        normalized_tenant_id = tenant_id.strip() or "global"
        normalized_actor_id = actor_id.strip()
        normalized_actor_type = actor_type.strip().casefold() or "unknown"
        bounded_item_limit = max(1, min(item_limit, 24))
        bounded_device_limit = max(1, min(device_limit, 12))
        bounded_history_limit = max(20, min(history_limit, 1000))
        normalized_focus_item_id = (
            _normalize_internal_item_id(focus_item_id) if focus_item_id is not None else None
        )
        now = datetime.now(UTC)
        active_cutoff = now - _CONSUMER_PLAYBACK_ACTIVE_WINDOW

        if normalized_actor_type != "user" or not normalized_actor_id:
            return ConsumerPlaybackActivityRecord(generated_at=now.isoformat())

        async with self._db.session() as session:
            event_query = select(ConsumerPlaybackActivityEventORM).where(
                ConsumerPlaybackActivityEventORM.tenant_id == normalized_tenant_id,
                ConsumerPlaybackActivityEventORM.actor_id == normalized_actor_id,
                ConsumerPlaybackActivityEventORM.actor_type == normalized_actor_type,
            )
            if normalized_focus_item_id is not None:
                event_query = event_query.where(
                    ConsumerPlaybackActivityEventORM.item_id == normalized_focus_item_id
                )
            events = (
                (
                    await session.execute(
                        event_query
                        .order_by(
                            ConsumerPlaybackActivityEventORM.occurred_at.desc(),
                            ConsumerPlaybackActivityEventORM.created_at.desc(),
                        )
                        .limit(bounded_history_limit)
                    )
                )
                .scalars()
                .all()
            )

            if not events:
                return ConsumerPlaybackActivityRecord(generated_at=now.isoformat())

            total_view_count = 0
            total_launch_count = 0
            item_rollups: dict[str, dict[str, Any]] = {}
            device_rollups: dict[str, dict[str, Any]] = {}
            session_rollups: dict[str, dict[str, Any]] = {}
            ordered_item_ids: list[str] = []
            ordered_device_keys: list[str] = []
            ordered_session_keys: list[str] = []

            for event in events:
                occurred_at = event.occurred_at
                payload = cast(dict[str, object], event.payload or {})
                session_key = _coerce_activity_payload_str(payload, "session_key")
                position_seconds = _coerce_activity_payload_seconds(payload, "position_seconds")
                duration_seconds = _coerce_activity_payload_seconds(payload, "duration_seconds")
                completed = _coerce_activity_payload_bool(payload, "completed")
                is_completed = bool(completed or event.activity_kind == "complete")

                if event.activity_kind == "launch":
                    total_launch_count += 1
                elif event.activity_kind == "view":
                    total_view_count += 1

                item_bucket = item_rollups.get(event.item_id)
                if item_bucket is None:
                    item_bucket = {
                        "last_activity_at": occurred_at,
                        "last_viewed_at": None,
                        "last_launched_at": None,
                        "view_count": 0,
                        "launch_count": 0,
                        "session_keys": set(),
                        "active_session_keys": set(),
                        "last_session_key": None,
                        "resume_position_seconds": None,
                        "duration_seconds": None,
                        "progress_percent": None,
                        "completed": False,
                        "progress_recorded_at": None,
                        "last_target": None,
                    }
                    item_rollups[event.item_id] = item_bucket
                    ordered_item_ids.append(event.item_id)
                if (
                    item_bucket["last_activity_at"] is None
                    or occurred_at > item_bucket["last_activity_at"]
                ):
                    item_bucket["last_activity_at"] = occurred_at
                    if session_key is not None:
                        item_bucket["last_session_key"] = session_key
                if event.activity_kind in {"view", "progress", "complete"} and (
                    item_bucket["last_viewed_at"] is None
                    or occurred_at > item_bucket["last_viewed_at"]
                ):
                    item_bucket["last_viewed_at"] = occurred_at
                if event.activity_kind == "view":
                    item_bucket["view_count"] = int(item_bucket["view_count"]) + 1
                if event.activity_kind == "launch":
                    item_bucket["launch_count"] = int(item_bucket["launch_count"]) + 1
                    if (
                        item_bucket["last_launched_at"] is None
                        or occurred_at > item_bucket["last_launched_at"]
                    ):
                        item_bucket["last_launched_at"] = occurred_at
                        item_bucket["last_target"] = event.target
                if session_key is not None:
                    cast(set[str], item_bucket["session_keys"]).add(session_key)
                    if occurred_at >= active_cutoff and not is_completed:
                        cast(set[str], item_bucket["active_session_keys"]).add(session_key)
                progress_recorded_at = cast(datetime | None, item_bucket["progress_recorded_at"])
                if (
                    position_seconds is not None
                    or duration_seconds is not None
                    or is_completed
                ) and (progress_recorded_at is None or occurred_at >= progress_recorded_at):
                    item_bucket["resume_position_seconds"] = (
                        None if is_completed else position_seconds
                    )
                    item_bucket["duration_seconds"] = duration_seconds
                    item_bucket["progress_percent"] = _build_progress_percent(
                        position_seconds,
                        duration_seconds,
                        completed=is_completed,
                    )
                    item_bucket["completed"] = is_completed
                    item_bucket["progress_recorded_at"] = occurred_at

                device_bucket = device_rollups.get(event.device_key)
                if device_bucket is None:
                    device_bucket = {
                        "device_label": event.device_label,
                        "last_seen_at": occurred_at,
                        "last_activity_at": occurred_at,
                        "last_viewed_at": None,
                        "last_launched_at": None,
                        "launch_count": 0,
                        "view_count": 0,
                        "session_keys": set(),
                        "active_session_keys": set(),
                        "last_session_key": None,
                        "resume_position_seconds": None,
                        "duration_seconds": None,
                        "progress_percent": None,
                        "completed_session_count": 0,
                        "completed_session_keys": set(),
                        "progress_recorded_at": None,
                        "last_target": None,
                    }
                    device_rollups[event.device_key] = device_bucket
                    ordered_device_keys.append(event.device_key)
                if (
                    device_bucket["last_seen_at"] is None
                    or occurred_at > device_bucket["last_seen_at"]
                ):
                    device_bucket["last_seen_at"] = occurred_at
                    device_bucket["device_label"] = event.device_label
                if (
                    device_bucket["last_activity_at"] is None
                    or occurred_at > device_bucket["last_activity_at"]
                ):
                    device_bucket["last_activity_at"] = occurred_at
                    if session_key is not None:
                        device_bucket["last_session_key"] = session_key
                if event.activity_kind == "launch":
                    device_bucket["launch_count"] = int(device_bucket["launch_count"]) + 1
                    if (
                        device_bucket["last_launched_at"] is None
                        or occurred_at > device_bucket["last_launched_at"]
                    ):
                        device_bucket["last_launched_at"] = occurred_at
                        device_bucket["last_target"] = event.target
                elif event.activity_kind == "view":
                    device_bucket["view_count"] = int(device_bucket["view_count"]) + 1
                if event.activity_kind in {"view", "progress", "complete"} and (
                    device_bucket["last_viewed_at"] is None
                    or occurred_at > device_bucket["last_viewed_at"]
                ):
                    device_bucket["last_viewed_at"] = occurred_at
                if session_key is not None:
                    cast(set[str], device_bucket["session_keys"]).add(session_key)
                    if occurred_at >= active_cutoff and not is_completed:
                        cast(set[str], device_bucket["active_session_keys"]).add(session_key)
                device_progress_recorded_at = cast(
                    datetime | None,
                    device_bucket["progress_recorded_at"],
                )
                if (
                    position_seconds is not None
                    or duration_seconds is not None
                    or is_completed
                ) and (
                    device_progress_recorded_at is None
                    or occurred_at >= device_progress_recorded_at
                ):
                    device_bucket["resume_position_seconds"] = (
                        None if is_completed else position_seconds
                    )
                    device_bucket["duration_seconds"] = duration_seconds
                    device_bucket["progress_percent"] = _build_progress_percent(
                        position_seconds,
                        duration_seconds,
                        completed=is_completed,
                    )
                    device_bucket["progress_recorded_at"] = occurred_at
                if is_completed and session_key is not None:
                    completed_session_keys = cast(
                        set[str],
                        device_bucket["completed_session_keys"],
                    )
                    if session_key not in completed_session_keys:
                        completed_session_keys.add(session_key)
                        device_bucket["completed_session_count"] = int(
                            device_bucket["completed_session_count"]
                        ) + 1

                if session_key is not None:
                    session_bucket = session_rollups.get(session_key)
                    if session_bucket is None:
                        session_bucket = {
                            "item_id": event.item_id,
                            "device_key": event.device_key,
                            "device_label": event.device_label,
                            "started_at": occurred_at,
                            "last_seen_at": occurred_at,
                            "last_target": event.target,
                            "resume_position_seconds": None,
                            "duration_seconds": None,
                            "progress_percent": None,
                            "completed": False,
                            "progress_recorded_at": None,
                        }
                        session_rollups[session_key] = session_bucket
                        ordered_session_keys.append(session_key)
                    else:
                        if occurred_at < cast(datetime, session_bucket["started_at"]):
                            session_bucket["started_at"] = occurred_at
                        if occurred_at > cast(datetime, session_bucket["last_seen_at"]):
                            session_bucket["last_seen_at"] = occurred_at
                    session_bucket["item_id"] = event.item_id
                    session_bucket["device_key"] = event.device_key
                    session_bucket["device_label"] = event.device_label
                    if event.target is not None:
                        session_bucket["last_target"] = event.target
                    session_progress_recorded_at = cast(
                        datetime | None,
                        session_bucket["progress_recorded_at"],
                    )
                    if (
                        position_seconds is not None
                        or duration_seconds is not None
                        or is_completed
                    ) and (
                        session_progress_recorded_at is None
                        or occurred_at >= session_progress_recorded_at
                    ):
                        session_bucket["resume_position_seconds"] = (
                            None if is_completed else position_seconds
                        )
                        session_bucket["duration_seconds"] = duration_seconds
                        session_bucket["progress_percent"] = _build_progress_percent(
                            position_seconds,
                            duration_seconds,
                            completed=is_completed,
                        )
                        session_bucket["progress_recorded_at"] = occurred_at
                    if is_completed:
                        session_bucket["completed"] = True

            if normalized_focus_item_id is not None and normalized_focus_item_id not in item_rollups:
                return ConsumerPlaybackActivityRecord(generated_at=now.isoformat())

            selected_item_ids = ordered_item_ids[:bounded_item_limit]
            item_rows = (
                (
                    await session.execute(
                        select(MediaItemORM)
                        .options(*_projection_item_load_options())
                        .where(
                            MediaItemORM.tenant_id == normalized_tenant_id,
                            MediaItemORM.id.in_(selected_item_ids),
                        )
                    )
                )
                .scalars()
                .all()
            )
            summaries_by_id = {
                row.id: _build_summary_record(row, extended=True)
                for row in item_rows
            }

        items = tuple(
            _build_consumer_activity_item_record(
                summaries_by_id[item_id],
                last_activity_at=cast(datetime | None, item_rollups[item_id]["last_activity_at"]),
                last_viewed_at=cast(datetime | None, item_rollups[item_id]["last_viewed_at"]),
                last_launched_at=cast(datetime | None, item_rollups[item_id]["last_launched_at"]),
                view_count=int(item_rollups[item_id]["view_count"]),
                launch_count=int(item_rollups[item_id]["launch_count"]),
                session_count=len(cast(set[str], item_rollups[item_id]["session_keys"])),
                active_session_count=len(
                    cast(set[str], item_rollups[item_id]["active_session_keys"])
                ),
                last_session_key=cast(str | None, item_rollups[item_id]["last_session_key"]),
                resume_position_seconds=cast(
                    int | None,
                    item_rollups[item_id]["resume_position_seconds"],
                ),
                duration_seconds=cast(int | None, item_rollups[item_id]["duration_seconds"]),
                progress_percent=cast(float | None, item_rollups[item_id]["progress_percent"]),
                completed=bool(item_rollups[item_id]["completed"]),
                last_target=cast(str | None, item_rollups[item_id]["last_target"]),
            )
            for item_id in selected_item_ids
            if item_id in summaries_by_id
        )

        devices = tuple(
            ConsumerPlaybackDeviceRecord(
                device_key=device_key,
                device_label=str(device_rollups[device_key]["device_label"]),
                last_seen_at=_serialize_datetime(
                    cast(datetime | None, device_rollups[device_key]["last_seen_at"])
                )
                or now.isoformat(),
                last_activity_at=_serialize_datetime(
                    cast(datetime | None, device_rollups[device_key]["last_activity_at"])
                ),
                last_viewed_at=_serialize_datetime(
                    cast(datetime | None, device_rollups[device_key]["last_viewed_at"])
                ),
                last_launched_at=_serialize_datetime(
                    cast(datetime | None, device_rollups[device_key]["last_launched_at"])
                ),
                launch_count=int(device_rollups[device_key]["launch_count"]),
                view_count=int(device_rollups[device_key]["view_count"]),
                session_count=len(cast(set[str], device_rollups[device_key]["session_keys"])),
                active_session_count=len(
                    cast(set[str], device_rollups[device_key]["active_session_keys"])
                ),
                last_session_key=cast(
                    str | None,
                    device_rollups[device_key]["last_session_key"],
                ),
                resume_position_seconds=cast(
                    int | None,
                    device_rollups[device_key]["resume_position_seconds"],
                ),
                duration_seconds=cast(int | None, device_rollups[device_key]["duration_seconds"]),
                progress_percent=cast(
                    float | None,
                    device_rollups[device_key]["progress_percent"],
                ),
                completed_session_count=int(
                    device_rollups[device_key]["completed_session_count"]
                ),
                last_target=cast(str | None, device_rollups[device_key]["last_target"]),
            )
            for device_key in ordered_device_keys[:bounded_device_limit]
        )

        recent_sessions = tuple(
            ConsumerPlaybackSessionRecord(
                session_key=session_key,
                item_id=str(session_rollups[session_key]["item_id"]),
                device_key=str(session_rollups[session_key]["device_key"]),
                device_label=str(session_rollups[session_key]["device_label"]),
                started_at=_serialize_datetime(
                    cast(datetime | None, session_rollups[session_key]["started_at"])
                )
                or now.isoformat(),
                last_seen_at=_serialize_datetime(
                    cast(datetime | None, session_rollups[session_key]["last_seen_at"])
                )
                or now.isoformat(),
                last_target=cast(str | None, session_rollups[session_key]["last_target"]),
                active=(
                    cast(datetime, session_rollups[session_key]["last_seen_at"]) >= active_cutoff
                    and not bool(session_rollups[session_key]["completed"])
                ),
                resume_position_seconds=cast(
                    int | None,
                    session_rollups[session_key]["resume_position_seconds"],
                ),
                duration_seconds=cast(
                    int | None,
                    session_rollups[session_key]["duration_seconds"],
                ),
                progress_percent=cast(
                    float | None,
                    session_rollups[session_key]["progress_percent"],
                ),
                completed=bool(session_rollups[session_key]["completed"]),
            )
            for session_key in ordered_session_keys[:_CONSUMER_PLAYBACK_SESSION_LIMIT]
        )

        return ConsumerPlaybackActivityRecord(
            generated_at=now.isoformat(),
            total_item_count=len(items),
            total_view_count=total_view_count,
            total_launch_count=total_launch_count,
            total_session_count=len(session_rollups),
            active_session_count=sum(
                1
                for session in session_rollups.values()
                if cast(datetime, session["last_seen_at"]) >= active_cutoff
                and not bool(session["completed"])
            ),
            items=items,
            devices=devices,
            recent_sessions=recent_sessions,
        )

    async def _hydrate_summary_records(
        self,
        items: list[MediaItemSummaryRecord],
    ) -> list[MediaItemSummaryRecord]:
        """Backfill missing poster/title metadata for library summaries when possible."""

        if not items:
            return items
        if self._resolve_tmdb_client() is None:
            return items

        hydrated: list[MediaItemSummaryRecord] = []
        for item in items:
            if item.poster_path is not None or item.tmdb_id is None:
                hydrated.append(item)
                continue

            media_type = "movie" if item.type == "movie" else "tv"
            metadata = await self._fetch_request_metadata(
                media_type=media_type,
                identifier=f"tmdb:{item.tmdb_id}",
            )
            resolved_metadata = metadata.metadata
            if resolved_metadata is None:
                hydrated.append(item)
                continue

            poster_path = _normalize_poster_path(
                _extract_string(resolved_metadata.attributes, "poster_path")
            )
            if poster_path is None:
                hydrated.append(item)
                continue

            title = item.title
            if (
                (item.external_ref is not None and title == item.external_ref and resolved_metadata.title)
                or (item.tmdb_id is not None and title == item.tmdb_id and resolved_metadata.title)
            ):
                title = resolved_metadata.title

            hydrated.append(replace(item, poster_path=poster_path, title=title))

        return hydrated

    async def get_item_detail(
        self,
        item_identifier: str,
        *,
        media_type: str,
        extended: bool = False,
        tenant_id: str | None = None,
    ) -> MediaItemSummaryRecord | None:
        """Return one item detail record for the current item-detail compatibility route."""

        playback_service = PlaybackSourceService(self._db)
        async with self._db.session() as session:
            statement = (
                select(MediaItemORM)
                .options(
                    *_projection_item_load_options(),
                    selectinload(MediaItemORM.show).selectinload(ShowORM.seasons),
                    selectinload(MediaItemORM.item_requests),
                    selectinload(MediaItemORM.playback_attachments),
                    selectinload(MediaItemORM.media_entries).selectinload(
                        MediaEntryORM.source_attachment
                    ),
                    selectinload(MediaItemORM.subtitle_entries),
                    selectinload(MediaItemORM.active_streams),
                )
                .order_by(MediaItemORM.created_at.desc())
            )
            if tenant_id is not None:
                statement = statement.where(MediaItemORM.tenant_id == tenant_id)
            rows = ((await session.execute(statement)).scalars().all())

        for item in rows:
            detail = _build_detail_record(
                item,
                extended=extended,
                playback_service=playback_service,
            )
            if media_type == "item" and _item_matches_identifier(
                detail,
                media_type=media_type,
                item_identifier=item_identifier,
            ):
                return detail
            if media_type in {"movie", "tv"} and _item_matches_identifier(
                detail,
                media_type=media_type,
                item_identifier=item_identifier,
            ):
                return detail
        return None

    async def request_items_by_identifiers(
        self,
        *,
        media_type: str,
        identifiers: list[str] | None = None,
        tmdb_ids: list[str] | None = None,
        tvdb_ids: list[str] | None = None,
        requested_seasons: list[int] | None = None,
        requested_episodes: dict[str, list[int]] | None = None,
        request_source: str = "api",
        tenant_id: str = "global",
    ) -> ItemActionResult:
        """Create request records for the current `/api/v1/items/add` compatibility route."""

        explicit_identifiers = identifiers is not None
        if explicit_identifiers:
            normalized_identifiers = _normalize_identifier_list(identifiers)
            request_identifiers = list(normalized_identifiers)
        else:
            preferred_identifiers = tmdb_ids if media_type == "movie" else tvdb_ids
            fallback_identifiers = tvdb_ids if media_type == "movie" else tmdb_ids
            normalized_identifiers = _normalize_identifier_list(preferred_identifiers)
            identifier_system = "tmdb" if media_type == "movie" else "tvdb"
            if not normalized_identifiers:
                normalized_identifiers = _normalize_identifier_list(fallback_identifiers)
                identifier_system = "tvdb" if media_type == "movie" else "tmdb"
            request_identifiers = [
                f"{identifier_system}:{identifier}" for identifier in normalized_identifiers
            ]
        if not normalized_identifiers:
            raise ValueError("no identifiers supplied for requested media type")

        requested_ids: list[str] = []
        for identifier in request_identifiers:
            enriched = await self._fetch_request_metadata(
                media_type=media_type, identifier=identifier
            )
            attributes = (
                enriched.metadata.attributes
                if enriched.metadata is not None
                else _request_attributes_for_identifier(media_type, identifier)
            )
            request_title = enriched.metadata.title if enriched.metadata is not None else identifier
            if enriched.metadata is None:
                logger.warning(
                    "request metadata enrichment unavailable; creating placeholder item",
                    extra={"media_type": media_type, "identifier": identifier},
                )
            if requested_seasons is not None or requested_episodes is not None:
                record = await self.request_item(
                    external_ref=identifier,
                    media_type=media_type,
                    title=request_title,
                    attributes=attributes,
                    requested_seasons=requested_seasons,
                    requested_episodes=requested_episodes,
                    request_source=request_source,
                    tenant_id=tenant_id,
                )
            else:
                record = await self.request_item(
                    external_ref=identifier,
                    media_type=media_type,
                    title=request_title,
                    attributes=attributes,
                    request_source=request_source,
                    tenant_id=tenant_id,
                )
            requested_ids.append(record.id)

        noun = "movie" if media_type == "movie" else "show"
        plural_suffix = "" if len(requested_ids) == 1 else "s"
        return ItemActionResult(
            message=f"Requested {len(requested_ids)} {noun}{plural_suffix}.",
            ids=requested_ids,
        )

    async def get_calendar_snapshot(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, CalendarItemRecord]:
        """Return a calendar payload keyed by stable item identifiers."""

        projection = await self.get_calendar(
            start_date=start_date,
            end_date=end_date,
            tenant_id=tenant_id,
        )
        return {
            item.item_id: CalendarItemRecord(
                item_id=item.item_id,
                tvdb_id=item.tvdb_id,
                tmdb_id=item.tmdb_id,
                imdb_id=(item.specialization.imdb_id if item.specialization is not None else None),
                parent_ids=(
                    item.specialization.parent_ids if item.specialization is not None else None
                ),
                show_title=item.title,
                item_type=item.item_type,
                aired_at=item.air_date,
                season=item.season_number,
                episode=item.episode_number,
                last_state=item.last_state,
                release_data=item.release_data,
                specialization=item.specialization,
            )
            for item in projection
        }

    async def get_calendar(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        tenant_id: str | None = None,
    ) -> list[CalendarProjectionRecord]:
        """Return calendar rows ordered by air date for both compatibility and GraphQL projections."""

        async with self._db.session() as session:
            statement = (
                select(MediaItemORM)
                .options(*_projection_item_load_options())
                .order_by(MediaItemORM.created_at.desc())
            )
            if tenant_id is not None:
                statement = statement.where(MediaItemORM.tenant_id == tenant_id)
            rows = ((await session.execute(statement)).scalars().all())

        result: list[CalendarProjectionRecord] = []
        child_show_item_ids: set[str] = set()
        for item in rows:
            attributes = cast(dict[str, object], item.attributes or {})
            specialization = _build_specialization_record(item)
            aired_at = _extract_string(attributes, "aired_at")
            aired_at_dt = _parse_calendar_datetime(aired_at)
            if aired_at_dt is None:
                continue

            aired_ts = aired_at_dt.timestamp()
            if start_date is not None:
                start_cutoff_dt = _parse_calendar_datetime(start_date)
                if start_cutoff_dt is None:
                    continue
                start_cutoff = start_cutoff_dt.timestamp()
                if aired_ts < start_cutoff:
                    continue
            if end_date is not None:
                end_cutoff_dt = _parse_calendar_datetime(end_date)
                if end_cutoff_dt is None:
                    continue
                end_cutoff = end_cutoff_dt.timestamp()
                if aired_ts > end_cutoff:
                    continue

            item_type = _calendar_projection_type(specialization)
            tmdb_id, tvdb_id = _calendar_projection_identifiers(specialization)
            season_number = specialization.season_number
            episode_number = specialization.episode_number
            release_data = _build_calendar_release_data(attributes)

            if item.season is not None:
                season_number = item.season.season_number
                if item.season.show is not None:
                    child_show_item_ids.add(item.season.show.media_item_id)
            elif item.episode is not None:
                episode_number = item.episode.episode_number
                if item.episode.season is not None:
                    season_number = item.episode.season.season_number
                    if item.episode.season.show is not None:
                        child_show_item_ids.add(item.episode.season.show.media_item_id)

            result.append(
                CalendarProjectionRecord(
                    item_id=item.id,
                    title=specialization.show_title or _resolve_calendar_show_title(item, attributes),
                    item_type=item_type,
                    tmdb_id=tmdb_id,
                    tvdb_id=tvdb_id,
                    episode_number=episode_number,
                    season_number=season_number,
                    air_date=aired_at or item.created_at.date().isoformat(),
                    last_state=_canonical_state_name(item.state),
                    release_data=release_data,
                    specialization=specialization,
                )
            )

        existing_item_ids = {item.item_id for item in result}
        for item in rows:
            if item.id in existing_item_ids or item.id in child_show_item_ids:
                continue

            attributes = cast(dict[str, object], item.attributes or {})
            specialization = _build_specialization_record(item)
            tmdb_id, tvdb_id = _calendar_projection_identifiers(specialization)
            release_data = _build_calendar_release_data(attributes)
            if release_data is None:
                continue

            next_aired = release_data.next_aired or release_data.last_aired
            fallback_dt = _parse_calendar_datetime(next_aired)
            if fallback_dt is None:
                continue

            fallback_ts = fallback_dt.timestamp()
            if start_date is not None:
                start_cutoff_dt = _parse_calendar_datetime(start_date)
                if start_cutoff_dt is None:
                    continue
                if fallback_ts < start_cutoff_dt.timestamp():
                    continue
            if end_date is not None:
                end_cutoff_dt = _parse_calendar_datetime(end_date)
                if end_cutoff_dt is None:
                    continue
                if fallback_ts > end_cutoff_dt.timestamp():
                    continue

            result.append(
                CalendarProjectionRecord(
                    item_id=item.id,
                    title=specialization.show_title or _resolve_calendar_show_title(item, attributes),
                    item_type=_calendar_projection_type(specialization),
                    tmdb_id=tmdb_id,
                    tvdb_id=tvdb_id,
                    episode_number=specialization.episode_number,
                    season_number=specialization.season_number,
                    air_date=next_aired or fallback_dt.date().isoformat(),
                    last_state=_canonical_state_name(item.state),
                    release_data=release_data,
                    specialization=specialization,
                )
            )

        return sorted(result, key=lambda item: (item.air_date, item.item_id))

    async def reset_items(self, ids: list[str]) -> ItemActionResult:
        """Reset compatible items back to the requested state for frontend actions."""

        return await self._set_items_state(ids, event_name="reset", message="Items reset.")

    async def retry_items(self, ids: list[str]) -> ItemActionResult:
        """Retry compatible items by moving them back to the requested state."""

        return await self._set_items_state(ids, event_name="retry", message="Items retried.")

    async def remove_items(self, ids: list[str], *, tenant_id: str | None = None) -> ItemActionResult:
        """Remove compatible items and their lifecycle events."""

        unique_ids = list(dict.fromkeys(ids))
        if not unique_ids:
            return ItemActionResult(message="Items removed.", ids=[])

        async with self._db.session() as session:
            statement = select(MediaItemORM.id).where(MediaItemORM.id.in_(unique_ids))
            if tenant_id is not None:
                statement = statement.where(MediaItemORM.tenant_id == tenant_id)
            matched_ids = list((await session.execute(statement)).scalars().all())
            if matched_ids:
                await session.execute(
                    delete(ItemStateEventORM).where(ItemStateEventORM.item_id.in_(matched_ids))
                )
                await session.execute(delete(MediaItemORM).where(MediaItemORM.id.in_(matched_ids)))
                await session.commit()

        return ItemActionResult(message="Items removed.", ids=matched_ids)

    async def _set_items_state(
        self,
        ids: list[str],
        *,
        event_name: str,
        message: str,
        tenant_id: str | None = None,
    ) -> ItemActionResult:
        """Move compatible items back to the requested state for reset/retry routes."""

        unique_ids = list(dict.fromkeys(ids))
        if not unique_ids:
            return ItemActionResult(message=message, ids=[])

        async with self._db.session() as session:
            statement = select(MediaItemORM).where(MediaItemORM.id.in_(unique_ids))
            if tenant_id is not None:
                statement = statement.where(MediaItemORM.tenant_id == tenant_id)
            rows = ((await session.execute(statement)).scalars().all())
            matched_ids: list[str] = []
            for item in rows:
                previous_state = item.state
                item.state = ItemState.REQUESTED.value
                session.add(
                    ItemStateEventORM(
                        item_id=item.id,
                        event=event_name,
                        previous_state=previous_state,
                        next_state=ItemState.REQUESTED.value,
                        message=message,
                    )
                )
                matched_ids.append(item.id)

            if matched_ids:
                await session.commit()

        return ItemActionResult(message=message, ids=matched_ids)

    async def get_item(self, item_id: str, *, tenant_id: str | None = None) -> MediaItemRecord | None:
        """Fetch one media item by internal UUID identifier."""

        normalized_item_id = _normalize_internal_item_id(item_id)

        async with self._db.session() as session:
            statement = (
                select(MediaItemORM)
                .options(selectinload(MediaItemORM.media_entries))
                .where(MediaItemORM.id == normalized_item_id)
            )
            if tenant_id is not None:
                statement = statement.where(MediaItemORM.tenant_id == tenant_id)
            row = (await session.execute(statement)).scalar_one_or_none()

        if row is None:
            return None

        return _build_media_item_record_from_orm(row)

    async def get_item_by_external_id(
        self,
        external_id: str,
        *,
        media_type: str | None = None,
        tenant_id: str | None = None,
    ) -> MediaItemRecord | None:
        """Fetch one media item by supported external identifier families."""

        normalized_external_id = external_id.strip()
        parsed_identifier = _parse_external_ref_identifier(normalized_external_id)
        if parsed_identifier is None:
            raise ValueError(
                "get_item_by_external_id() requires an external ref like 'tmdb:123', 'tvdb:456', or 'imdb:tt1234567'"
            )

        normalized_media_type = _normalize_requested_media_type(media_type)
        system, reference = parsed_identifier

        async with self._db.session() as session:
            async def _lookup_item(statement: Any) -> MediaItemORM | None:
                if tenant_id is not None:
                    statement = statement.where(MediaItemORM.tenant_id == tenant_id)
                return (
                    await session.execute(
                        statement.options(selectinload(MediaItemORM.media_entries))
                    )
                ).scalars().first()

            row = await _lookup_item(
                select(MediaItemORM).where(MediaItemORM.external_ref == normalized_external_id)
            )
            if row is not None:
                return _build_media_item_record_from_orm(row)

            if system == "tmdb":
                if normalized_media_type in {None, "movie"}:
                    row = await _lookup_item(
                        select(MediaItemORM)
                        .join(MovieORM, MovieORM.media_item_id == MediaItemORM.id)
                        .where(MovieORM.tmdb_id == reference)
                        .order_by(MediaItemORM.created_at.desc())
                    )
                    if row is not None:
                        return _build_media_item_record_from_orm(row)
                if normalized_media_type in {None, "show"}:
                    row = await _lookup_item(
                        select(MediaItemORM)
                        .join(ShowORM, ShowORM.media_item_id == MediaItemORM.id)
                        .where(ShowORM.tmdb_id == reference)
                        .order_by(MediaItemORM.created_at.desc())
                    )
                    if row is not None:
                        return _build_media_item_record_from_orm(row)
                if normalized_media_type in {None, "season"}:
                    row = await _lookup_item(
                        select(MediaItemORM)
                        .join(SeasonORM, SeasonORM.media_item_id == MediaItemORM.id)
                        .where(SeasonORM.tmdb_id == reference)
                        .order_by(MediaItemORM.created_at.desc())
                    )
                    if row is not None:
                        return _build_media_item_record_from_orm(row)
                if normalized_media_type in {None, "episode"}:
                    row = await _lookup_item(
                        select(MediaItemORM)
                        .join(EpisodeORM, EpisodeORM.media_item_id == MediaItemORM.id)
                        .where(EpisodeORM.tmdb_id == reference)
                        .order_by(MediaItemORM.created_at.desc())
                    )
                    if row is not None:
                        return _build_media_item_record_from_orm(row)
                return None

            if system == "tvdb":
                if normalized_media_type in {None, "show"}:
                    row = await _lookup_item(
                        select(MediaItemORM)
                        .join(ShowORM, ShowORM.media_item_id == MediaItemORM.id)
                        .where(ShowORM.tvdb_id == reference)
                        .order_by(MediaItemORM.created_at.desc())
                    )
                    if row is not None:
                        return _build_media_item_record_from_orm(row)
                if normalized_media_type in {None, "season"}:
                    row = await _lookup_item(
                        select(MediaItemORM)
                        .join(SeasonORM, SeasonORM.media_item_id == MediaItemORM.id)
                        .where(SeasonORM.tvdb_id == reference)
                        .order_by(MediaItemORM.created_at.desc())
                    )
                    if row is not None:
                        return _build_media_item_record_from_orm(row)
                if normalized_media_type in {None, "episode"}:
                    row = await _lookup_item(
                        select(MediaItemORM)
                        .join(EpisodeORM, EpisodeORM.media_item_id == MediaItemORM.id)
                        .where(EpisodeORM.tvdb_id == reference)
                        .order_by(MediaItemORM.created_at.desc())
                    )
                    if row is not None:
                        return _build_media_item_record_from_orm(row)
                return None

            if normalized_media_type in {None, "movie"}:
                row = await _lookup_item(
                    select(MediaItemORM)
                    .join(MovieORM, MovieORM.media_item_id == MediaItemORM.id)
                    .where(MovieORM.imdb_id == reference)
                    .order_by(MediaItemORM.created_at.desc())
                )
                if row is not None:
                    return _build_media_item_record_from_orm(row)
            if normalized_media_type in {None, "show"}:
                row = await _lookup_item(
                    select(MediaItemORM)
                    .join(ShowORM, ShowORM.media_item_id == MediaItemORM.id)
                    .where(ShowORM.imdb_id == reference)
                    .order_by(MediaItemORM.created_at.desc())
                )
                if row is not None:
                    return _build_media_item_record_from_orm(row)
            if normalized_media_type in {None, "episode"}:
                row = await _lookup_item(
                    select(MediaItemORM)
                    .join(EpisodeORM, EpisodeORM.media_item_id == MediaItemORM.id)
                    .where(EpisodeORM.imdb_id == reference)
                    .order_by(MediaItemORM.created_at.desc())
                )
                if row is not None:
                    return _build_media_item_record_from_orm(row)

        return None

    async def get_scrape_candidates(self, *, item_id: str) -> list[ScrapeCandidateRecord]:
        """Return persisted raw scrape candidates for one item in deterministic order."""

        async with self._db.session() as session:
            rows = (
                (
                    await session.execute(
                        select(ScrapeCandidateORM)
                        .where(ScrapeCandidateORM.item_id == item_id)
                        .order_by(ScrapeCandidateORM.created_at, ScrapeCandidateORM.id)
                    )
                )
                .scalars()
                .all()
            )

        return [
            ScrapeCandidateRecord(
                item_id=row.item_id,
                info_hash=row.info_hash,
                raw_title=row.raw_title,
                provider=row.provider,
                size_bytes=row.size_bytes,
            )
            for row in rows
        ]

    async def get_stream_candidates(
        self,
        *,
        media_item_id: str,
        exclude_blacklisted: bool = False,
    ) -> list[StreamORM]:
        """Return persisted stream candidates for one item in deterministic order."""

        async with self._db.session() as session:
            item = (
                await session.execute(
                    select(MediaItemORM)
                    .options(
                        selectinload(MediaItemORM.streams),
                        selectinload(MediaItemORM.blacklisted_stream_relations),
                    )
                    .where(MediaItemORM.id == media_item_id)
                )
            ).scalar_one_or_none()

        if item is None:
            raise ValueError(f"unknown media_item_id={media_item_id}")

        streams = list(item.streams)
        if exclude_blacklisted:
            blacklisted_ids = {
                relation.stream_id
                for relation in item.blacklisted_stream_relations
                if relation.stream_id is not None
            }
            streams = [stream for stream in streams if stream.id not in blacklisted_ids]

        return sorted(streams, key=lambda stream: (stream.created_at, stream.id))

    async def recover_incomplete_library(
        self,
        *,
        recovery_cooldown: timedelta = timedelta(minutes=30),
        max_recovery_attempts: int = 5,
        is_scrape_item_job_active: Callable[[str], Awaitable[bool]] | None = None,
        reenqueue_scrape_item: Callable[[str], Awaitable[bool]] | None = None,
        is_scraped_item_job_active: Callable[[str], Awaitable[bool]],
        reenqueue_scraped_item: Callable[[str], Awaitable[bool]],
        now: datetime | None = None,
    ) -> LibraryRecoverySnapshot:
        """Recover failed or orphaned scraped items back into the scraped-item worker path."""

        scan_time = now or datetime.now(UTC)
        recovered: list[LibraryRecoveryRecord] = []
        permanently_failed: list[LibraryRecoveryRecord] = []

        async with self._db.session() as session:
            items = (
                (
                    await session.execute(
                        select(MediaItemORM)
                        .options(
                            selectinload(MediaItemORM.events),
                            selectinload(MediaItemORM.scrape_candidates),
                        )
                        .where(
                            MediaItemORM.state.in_(
                                (
                                    ItemState.FAILED.value,
                                    ItemState.SCRAPED.value,
                                )
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )

            for item in items:
                item_state = ItemState(item.state)
                if item_state not in {ItemState.FAILED, ItemState.SCRAPED}:
                    continue
                attempt_count = item.recovery_attempt_count or 0
                if attempt_count >= max_recovery_attempts:
                    item.next_retry_at = None
                    if item_state is not ItemState.FAILED:
                        previous_state = item.state
                        item.state = ItemState.FAILED.value
                        session.add(
                            ItemStateEventORM(
                                item_id=item.id,
                                event="retry_library_exhausted",
                                previous_state=previous_state,
                                next_state=ItemState.FAILED.value,
                                message="retry-library recovery exhausted max attempts",
                            )
                        )
                    record = LibraryRecoveryRecord(
                        item_id=item.id,
                        previous_state=item_state,
                        reason="max_recovery_attempts_exceeded",
                        recovery_attempt_count=attempt_count,
                        re_enqueued=False,
                    )
                    permanently_failed.append(record)
                    logger.warning(
                        "library recovery permanently skipped item",
                        extra={
                            "item_id": item.id,
                            "previous_state": item_state.value,
                            "reason": record.reason,
                        },
                    )
                    continue

                if item_state is ItemState.FAILED:
                    failed_at = _latest_failed_at(item)
                    retry_at = failed_at + recovery_cooldown
                    recovery_plan = _build_recovery_plan_record(
                        state=item_state,
                        next_retry_at=retry_at,
                        recovery_attempt_count=attempt_count,
                        has_scrape_candidates=bool(item.scrape_candidates),
                        reference_time=scan_time,
                    )
                    if recovery_plan.is_in_cooldown:
                        item.next_retry_at = retry_at
                        continue

                    target_state = ItemState.SCRAPED.value
                    target_reason = "failed_cooldown_elapsed"
                    target_message = "retry-library recovery requeued failed item"
                    target_job_active = is_scraped_item_job_active
                    target_reenqueue = reenqueue_scraped_item

                    if (
                        recovery_plan.target_stage is RecoveryTargetStage.SCRAPE
                        and is_scrape_item_job_active is not None
                        and reenqueue_scrape_item is not None
                    ):
                        target_state = ItemState.REQUESTED.value
                        target_reason = recovery_plan.reason
                        target_message = "retry-library recovery requeued failed item for scrape"
                        target_job_active = is_scrape_item_job_active
                        target_reenqueue = reenqueue_scrape_item

                    job_active = await target_job_active(item.id)
                    if job_active:
                        item.next_retry_at = None
                        continue

                    item.next_retry_at = None
                    item.state = target_state
                    session.add(
                        ItemStateEventORM(
                            item_id=item.id,
                            event="retry_library_requeue",
                            previous_state=ItemState.FAILED.value,
                            next_state=target_state,
                            message=target_message,
                        )
                    )
                    enqueued = await target_reenqueue(item.id)
                    if enqueued:
                        item.recovery_attempt_count = attempt_count + 1
                        record = LibraryRecoveryRecord(
                            item_id=item.id,
                            previous_state=item_state,
                            reason=target_reason,
                            recovery_attempt_count=item.recovery_attempt_count,
                            re_enqueued=True,
                        )
                        recovered.append(record)
                        logger.info(
                            "library recovery re-enqueued item",
                            extra={
                                "item_id": item.id,
                                "previous_state": item_state.value,
                                "reason": record.reason,
                            },
                        )
                    continue

                job_active = await is_scraped_item_job_active(item.id)
                if job_active:
                    item.next_retry_at = None
                    continue

                enqueued = await reenqueue_scraped_item(item.id)
                if not enqueued:
                    continue

                item.next_retry_at = None
                item.recovery_attempt_count = attempt_count + 1
                record = LibraryRecoveryRecord(
                    item_id=item.id,
                    previous_state=item_state,
                    reason="scraped_without_inflight_worker",
                    recovery_attempt_count=item.recovery_attempt_count,
                    re_enqueued=True,
                )
                recovered.append(record)
                logger.info(
                    "library recovery re-enqueued item",
                    extra={
                        "item_id": item.id,
                        "previous_state": item_state.value,
                        "reason": record.reason,
                    },
                )

            await session.commit()

        return LibraryRecoverySnapshot(recovered=recovered, permanently_failed=permanently_failed)

    async def publish_outbox_events(self, *, max_outbox_attempts: int = 5) -> OutboxPublishSnapshot:
        """Publish pending outbox rows to the process-local event bus and persist results."""

        published_count: int = 0
        failed_count: int = 0
        processed_at = datetime.now(UTC)

        async with self._db.session() as session:
            rows = (
                (
                    await session.execute(
                        select(OutboxEventORM)
                        .where(
                            OutboxEventORM.published_at.is_(None),
                            OutboxEventORM.failed_at.is_(None),
                        )
                        .order_by(OutboxEventORM.created_at)
                    )
                )
                .scalars()
                .all()
            )

            for row in rows:
                row.attempt_count = row.attempt_count or 0
                try:
                    await self._event_bus.publish(row.event_type, cast(dict[str, Any], row.payload))
                    row.published_at = processed_at
                    published_count += 1
                except Exception:
                    row.attempt_count += 1
                    if row.attempt_count >= max_outbox_attempts:
                        row.failed_at = processed_at
                    failed_count += 1
                    logger.exception(
                        "failed to publish outbox event",
                        extra={"outbox_event_id": row.id, "event_type": row.event_type},
                    )

            await session.commit()

        return OutboxPublishSnapshot(published_count=published_count, failed_count=failed_count)

    async def list_items(self, limit: int = 100) -> list[MediaItemRecord]:
        """Return latest media items sorted by recency."""

        bounded_limit = max(1, min(limit, 500))
        stmt = select(MediaItemORM).order_by(MediaItemORM.created_at.desc()).limit(bounded_limit)
        async with self._db.session() as session:
            rows = (await session.execute(stmt)).scalars().all()

        return [
            MediaItemRecord(
                id=row.id,
                external_ref=row.external_ref,
                title=row.title,
                state=ItemState(row.state),
                tenant_id=row.tenant_id,
                attributes=cast(dict[str, object], row.attributes or {}),
            )
            for row in rows
        ]

    async def request_item_with_enrichment(
        self,
        external_ref: str,
        title: str | None = None,
        *,
        media_type: str | None = None,
        attributes: dict[str, object] | None = None,
        requested_seasons: list[int] | None = None,
        requested_episodes: dict[str, list[int]] | None = None,
        request_source: str = "api",
        tenant_id: str = "global",
    ) -> RequestItemServiceResult:
        """Create a requested media item when it does not already exist."""

        normalized_external_ref = external_ref.strip()
        if not normalized_external_ref:
            raise ValueError("external_ref must not be empty")

        candidate_title = title.strip() if title else normalized_external_ref
        if not candidate_title:
            candidate_title = normalized_external_ref
        normalized_requested_media_type = _normalize_requested_media_type(media_type)
        candidate_attributes = _merge_request_attributes_for_external_ref(
            media_type=normalized_requested_media_type,
            external_ref=normalized_external_ref,
            attributes=dict(attributes or {}),
        )
        enrichment = self._metadata_resolution(source="none", metadata=None).enrichment
        if normalized_requested_media_type is not None:
            lookup_media_type = (
                "movie" if normalized_requested_media_type == "movie" else "tv"
            )
            enriched = await self._fetch_request_metadata(
                media_type=lookup_media_type,
                identifier=normalized_external_ref,
            )
            enrichment = enriched.enrichment
            resolved_metadata = enriched.metadata
            if resolved_metadata is not None:
                if title is None or candidate_title == normalized_external_ref:
                    candidate_title = resolved_metadata.title
                candidate_attributes = _merge_request_attributes_for_external_ref(
                    media_type=normalized_requested_media_type,
                    external_ref=normalized_external_ref,
                    attributes={**candidate_attributes, **resolved_metadata.attributes},
                )
        candidate_media_type = normalized_requested_media_type or _infer_request_media_type(
            external_ref=normalized_external_ref,
            attributes=candidate_attributes,
        )
        client = self._resolve_tmdb_client()
        tmdb_id = _extract_string(candidate_attributes, "tmdb_id")
        if (
            client is not None
            and tmdb_id is not None
            and _extract_string(candidate_attributes, "imdb_id") is None
        ):
            external_ids = await client.get_external_ids(
                tmdb_id,
                "movie" if candidate_media_type == "movie" else "tv",
            )
            imdb_id = external_ids.get("imdb_id")
            if imdb_id is not None:
                candidate_attributes["imdb_id"] = imdb_id
            tvdb_id = external_ids.get("tvdb_id")
            if tvdb_id is not None and _extract_string(candidate_attributes, "tvdb_id") is None:
                candidate_attributes["tvdb_id"] = tvdb_id
        partial_request_fields_provided = (
            requested_seasons is not None or requested_episodes is not None
        )
        normalized_requested_seasons = _clone_requested_seasons(requested_seasons)
        normalized_requested_episodes = _clone_requested_episodes(requested_episodes)
        if partial_request_fields_provided:
            structlogger.info(
                "item.partial_request",
                external_ref=normalized_external_ref,
                requested_seasons=normalized_requested_seasons,
                requested_episodes=normalized_requested_episodes,
            )

        created_record: MediaItemRecord | None = None
        async with self._db.session() as session:
            existing = (
                await session.execute(
                    select(MediaItemORM)
                    .options(selectinload(MediaItemORM.media_entries))
                    .where(MediaItemORM.external_ref == normalized_external_ref)
                )
            ).scalar_one_or_none()
            if existing is not None:
                if candidate_attributes:
                    existing_attributes = cast(dict[str, object], existing.attributes or {})
                    merged_attributes = {**existing_attributes, **candidate_attributes}
                    if merged_attributes != existing_attributes:
                        existing.attributes = merged_attributes
                if existing.tenant_id == "global" and tenant_id != "global":
                    existing.tenant_id = tenant_id
                await self._upsert_media_specialization(
                    session,
                    item=existing,
                    media_type=candidate_media_type,
                    attributes=cast(dict[str, object], existing.attributes or {}),
                )
                if partial_request_fields_provided:
                    await self._upsert_item_request(
                        session,
                        tenant_id=tenant_id,
                        external_ref=normalized_external_ref,
                        media_item_id=existing.id,
                        requested_title=existing.title,
                        media_type=candidate_media_type,
                        requested_seasons=normalized_requested_seasons,
                        requested_episodes=normalized_requested_episodes,
                        is_partial=True,
                        request_source=request_source,
                    )
                else:
                    await self._upsert_item_request(
                        session,
                        tenant_id=tenant_id,
                        external_ref=normalized_external_ref,
                        media_item_id=existing.id,
                        requested_title=existing.title,
                        media_type=candidate_media_type,
                        request_source=request_source,
                    )
                # Flush before materialising the detached record to prevent
                # post-commit lazy-load (MissingGreenlet) on attribute access.
                _flush = getattr(session, "flush", None)
                if callable(_flush):
                    await _flush()
                existing_record = _build_media_item_record_from_orm(existing)
                await session.commit()
                self._log_missing_imdb_id_intake_warning(
                    item_id=existing_record.id,
                    external_ref=normalized_external_ref,
                    attributes=existing_record.attributes,
                )
                return RequestItemServiceResult(item=existing_record, enrichment=enrichment)

            item = MediaItemORM(
                tenant_id=tenant_id,
                external_ref=normalized_external_ref,
                title=candidate_title,
                state=ItemState.REQUESTED.value,
                attributes=candidate_attributes,
            )
            session.add(item)
            try:
                await session.flush()

                session.add(
                    ItemStateEventORM(
                        item_id=item.id,
                        event="create",
                        previous_state=ItemState.REQUESTED.value,
                        next_state=ItemState.REQUESTED.value,
                        message="item created",
                    )
                )
                await self._upsert_media_specialization(
                    session,
                    item=item,
                    media_type=candidate_media_type,
                    attributes=candidate_attributes,
                )
                if partial_request_fields_provided:
                    await self._upsert_item_request(
                        session,
                        tenant_id=tenant_id,
                        external_ref=normalized_external_ref,
                        media_item_id=item.id,
                        requested_title=item.title,
                        media_type=candidate_media_type,
                        requested_seasons=normalized_requested_seasons,
                        requested_episodes=normalized_requested_episodes,
                        is_partial=True,
                        request_source=request_source,
                    )
                else:
                    await self._upsert_item_request(
                        session,
                        tenant_id=tenant_id,
                        external_ref=normalized_external_ref,
                        media_item_id=item.id,
                        requested_title=item.title,
                        media_type=candidate_media_type,
                        request_source=request_source,
                    )
                # Flush pending writes before materialising the detached record so
                # that post-commit attribute access does not trigger a lazy load on
                # a closed session (MissingGreenlet guard, mirrors existing-item path).
                _new_flush = getattr(session, "flush", None)
                if callable(_new_flush):
                    await _new_flush()
                created_record = _build_media_item_record_from_orm(item)
                await session.commit()
            except IntegrityError:
                await session.rollback()
                existing = (
                    await session.execute(
                        select(MediaItemORM)
                        .options(selectinload(MediaItemORM.media_entries))
                        .where(MediaItemORM.external_ref == normalized_external_ref)
                    )
                ).scalar_one_or_none()
                if existing is None:
                    raise
                async with self._db.session() as retry_session:
                    retry_item = (
                        await retry_session.execute(
                            select(MediaItemORM)
                            .options(selectinload(MediaItemORM.media_entries))
                            .where(MediaItemORM.id == existing.id)
                        )
                    ).scalar_one_or_none()
                    if retry_item is not None:
                        if retry_item.tenant_id == "global" and tenant_id != "global":
                            retry_item.tenant_id = tenant_id
                        await self._upsert_media_specialization(
                            retry_session,
                            item=retry_item,
                            media_type=candidate_media_type,
                            attributes=cast(dict[str, object], retry_item.attributes or {}),
                        )
                    if partial_request_fields_provided:
                        await self._upsert_item_request(
                            retry_session,
                            tenant_id=tenant_id,
                            external_ref=normalized_external_ref,
                            media_item_id=existing.id,
                            requested_title=existing.title,
                            media_type=candidate_media_type,
                            requested_seasons=normalized_requested_seasons,
                            requested_episodes=normalized_requested_episodes,
                            is_partial=True,
                            request_source=request_source,
                        )
                    else:
                        await self._upsert_item_request(
                            retry_session,
                            tenant_id=tenant_id,
                            external_ref=normalized_external_ref,
                            media_item_id=existing.id,
                            requested_title=existing.title,
                            media_type=candidate_media_type,
                            request_source=request_source,
                        )
                    # Flush the retry session before materialising so that
                    # pending writes are visible and lazy-loads are not triggered
                    # on the detached record after commit.
                    _retry_flush = getattr(retry_session, "flush", None)
                    if callable(_retry_flush):
                        await _retry_flush()
                    created_record = _build_media_item_record_from_orm(
                        retry_item if retry_item is not None else existing
                    )
                    await retry_session.commit()
            else:
                assert created_record is not None

        assert created_record is not None
        self._log_missing_imdb_id_intake_warning(
            item_id=created_record.id,
            external_ref=normalized_external_ref,
            attributes=created_record.attributes,
        )

        await self._event_bus.publish(
            "item.state.changed",
            {
                "item_id": created_record.id,
                "state": created_record.state.value,
                "message": "item created",
            },
        )

        return RequestItemServiceResult(item=created_record, enrichment=enrichment)

    async def request_item(
        self,
        external_ref: str,
        title: str | None = None,
        *,
        media_type: str | None = None,
        attributes: dict[str, object] | None = None,
        requested_seasons: list[int] | None = None,
        requested_episodes: dict[str, list[int]] | None = None,
        request_source: str = "api",
        tenant_id: str = "global",
    ) -> MediaItemRecord:
        """Create a requested media item when it does not already exist."""

        return (
            await self.request_item_with_enrichment(
                external_ref,
                title,
                media_type=media_type,
                attributes=attributes,
                requested_seasons=requested_seasons,
                requested_episodes=requested_episodes,
                request_source=request_source,
                tenant_id=tenant_id,
            )
        ).item

    async def transition_item(
        self,
        item_id: str,
        event: ItemEvent,
        message: str | None = None,
    ) -> MediaItemRecord:
        """Apply lifecycle state transition and persist the event record."""

        async with self._db.session() as session:
            item = (
                await session.execute(select(MediaItemORM).where(MediaItemORM.id == item_id))
            ).scalar_one_or_none()
            if item is None:
                raise ValueError(f"Unknown item_id={item_id}")

            machine = ItemStateMachine(state=ItemState(item.state))
            transition = machine.apply(event)
            item.state = transition.current.value

            session.add(
                ItemStateEventORM(
                    item_id=item.id,
                    event=transition.event.value,
                    previous_state=transition.previous.value,
                    next_state=transition.current.value,
                    message=message,
                )
            )
            session.add(
                OutboxEventORM(
                    event_type=_STATE_CHANGED_EVENT_TOPIC,
                    payload=_build_state_changed_payload(
                        item_id=item.id,
                        state=transition.current,
                        event=transition.event,
                        message=message,
                    ),
                    item_id=item.id,
                )
            )
            if transition.current is ItemState.COMPLETED:
                session.add(
                    OutboxEventORM(
                        event_type=_NOTIFICATIONS_EVENT_TOPIC,
                        payload=build_completion_notification_payload(item),
                        item_id=item.id,
                    )
                )
            # Materialize the detached service record directly from local state before commit
            # to guarantee architectural immunity against async ORM lazy-load failures.
            media_entries_state = item.__dict__.get("media_entries", NO_VALUE)
            has_media_entries = bool(media_entries_state) if media_entries_state is not NO_VALUE else False
            
            result_record = MediaItemRecord(
                id=item.id,
                external_ref=item.external_ref,
                title=item.title,
                state=ItemState(item.state),
                attributes=dict(cast(dict[str, object], item.attributes or {})),
                has_media_entries=has_media_entries,
            )
            result_item_id = result_record.id
            await session.commit()

        scraped_item_enqueuer = self._scraped_item_enqueuer
        if transition.current is ItemState.SCRAPED and scraped_item_enqueuer is not None:
            try:
                await scraped_item_enqueuer(result_item_id)
            except Exception:
                logger.exception(
                    "failed to enqueue scraped-item processing job",
                    extra={"item_id": result_item_id},
                )

        return result_record


NO_VALUE = cast(Any, vars(orm_attributes)["NO_VALUE"])
