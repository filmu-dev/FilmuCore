"""Media domain service for GraphQL/API orchestration and persistence."""

from __future__ import annotations

import enum
import hashlib
import logging
import re
import unicodedata
from collections import Counter
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from uuid import UUID

import Levenshtein
import structlog
from arq.connections import ArqRedis
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import attributes as orm_attributes
from sqlalchemy.orm import selectinload

from filmu_py.api.playback_resolution import PlaybackAttachment
from filmu_py.config import Settings, get_settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.db.models import (
    ActiveStreamORM,
    EpisodeORM,
    ItemRequestORM,
    ItemStateEventORM,
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
from filmu_py.rtn import parse_torrent_name
from filmu_py.services.debrid import TorrentInfo
from filmu_py.services.playback import PlaybackResolutionSnapshot, PlaybackSourceService
from filmu_py.services.settings_service import load_settings
from filmu_py.services.tmdb import (
    MovieMetadata,
    ShowMetadata,
    TmdbMetadataClient,
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


def _resolve_calendar_parent_identifiers(
    attributes: dict[str, object],
    *,
    fallback_tmdb_id: str | None,
    fallback_tvdb_id: str | None,
) -> tuple[str | None, str | None]:
    """Return calendar ids rebound to parent-show identifiers when metadata exposes them."""

    parent_ids = _coerce_parent_ids(attributes)
    if parent_ids is not None:
        return parent_ids.tmdb_id, parent_ids.tvdb_id

    return (
        _extract_first_string(attributes, "parent_tmdb_id", "show_tmdb_id") or fallback_tmdb_id,
        _extract_first_string(attributes, "parent_tvdb_id", "show_tvdb_id") or fallback_tvdb_id,
    )


def _resolve_calendar_show_title(item: MediaItemORM, attributes: dict[str, object]) -> str:
    """Return the frontend-facing show title for one calendar row."""

    return (
        _extract_first_string(attributes, "show_title", "series_title", "parent_title")
        or item.title
    )


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


def _build_summary_record(item: MediaItemORM, *, extended: bool) -> MediaItemSummaryRecord:
    """Map one ORM item into the current REST compatibility summary shape."""

    attributes = cast(dict[str, object], item.attributes or {})
    item_type = _canonical_item_type_name(_extract_string(attributes, "item_type"))
    metadata = attributes if extended else None
    next_retry_at = _effective_next_retry_at(item.next_retry_at)
    return MediaItemSummaryRecord(
        id=item.id,
        type=item_type,
        title=item.title,
        state=_canonical_state_name(item.state),
        tmdb_id=_extract_string(attributes, "tmdb_id"),
        tvdb_id=_extract_string(attributes, "tvdb_id"),
        parent_ids=_coerce_parent_ids(attributes),
        poster_path=_normalize_poster_path(_extract_string(attributes, "poster_path")),
        aired_at=_extract_string(attributes, "aired_at"),
        external_ref=item.external_ref,
        created_at=_serialize_datetime(item.created_at),
        updated_at=_serialize_datetime(item.updated_at),
        next_retry_at=_serialize_datetime(next_retry_at),
        recovery_attempt_count=int(item.recovery_attempt_count or 0),
        is_in_cooldown=_is_retry_cooldown_active(next_retry_at),
        metadata=metadata,
    )


def _serialize_datetime(value: datetime | None) -> str | None:
    """Return one optional datetime as an ISO-8601 string for compatibility responses."""

    if value is None:
        return None
    return value.isoformat()


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
    return MediaEntryDetailRecord(
        entry_type="media",
        kind=attachment.kind,
        original_filename=original_filename,
        url=url,
        local_path=attachment.local_path,
        download_url=download_url,
        unrestricted_url=unrestricted_url,
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
    return MediaEntryDetailRecord(
        entry_type=entry.entry_type,
        kind=entry.kind,
        original_filename=original_filename,
        url=url,
        local_path=entry.local_path,
        download_url=entry.download_url,
        unrestricted_url=entry.unrestricted_url,
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

    # Always infer covered season numbers from media entries for show-type items,
    # regardless of the `extended` flag (media_entries are always selectinloaded).
    covered_season_numbers: list[int] | None = None
    if summary.type in {"show", "tv"}:
        season_set: set[int] = set()
        for entry in item.media_entries:
            if (
                getattr(entry, "refresh_state", None)
                not in _SATISFYING_MEDIA_ENTRY_REFRESH_STATES
                or getattr(entry, "entry_type", None) != "media"
            ):
                continue
            # Use range-aware inference so pack torrents named "S01-S04"
            # expand to [1,2,3,4] rather than just [1].
            for sn in _infer_season_range_from_path(
                entry.provider_file_path or entry.original_filename
            ):
                season_set.add(sn)
        if season_set:
            covered_season_numbers = sorted(season_set)

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
    searchable = [item.title, item.external_ref, item.tmdb_id, item.tvdb_id]
    return any(value is not None and needle in value.casefold() for value in searchable)


def _sort_items(
    items: list[MediaItemSummaryRecord], sort: list[str] | None
) -> list[MediaItemSummaryRecord]:
    """Return items sorted using the first compatible frontend sort directive."""

    if not sort:
        return list(items)

    directive = sort[0]
    if directive == "title_asc":
        return sorted(items, key=lambda item: item.title.casefold())
    if directive == "title_desc":
        return sorted(items, key=lambda item: item.title.casefold(), reverse=True)
    if directive == "date_asc":
        return sorted(items, key=lambda item: item.aired_at or "")
    if directive == "date_desc":
        return sorted(items, key=lambda item: item.aired_at or "", reverse=True)
    return list(items)


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
class MediaEntryDetailRecord:
    """VFS-facing media-entry projection derived from the current persisted playback attachment layer."""

    entry_type: str = "media"
    kind: str = "remote-direct"
    original_filename: str | None = None
    url: str | None = None
    local_path: str | None = None
    download_url: str | None = None
    unrestricted_url: str | None = None
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


@dataclass(frozen=True)
class ParsedStreamCandidateRecord:
    """Parsed stream-candidate payload persisted before later ranking stages."""

    raw_title: str
    infohash: str
    parsed_title: dict[str, object]
    resolution: str | None


@dataclass(frozen=True)
class ParsedStreamCandidateValidation:
    """Content-level validation result for one parsed stream candidate."""

    ok: bool
    reason: str | None = None


@dataclass(frozen=True)
class RankedStreamCandidateRecord:
    """Durable first-pass ranking result for one persisted stream candidate."""

    item_id: str
    stream_id: str
    rank_score: int
    lev_ratio: float
    fetch: bool
    passed: bool
    rejection_reason: str | None = None
    stream: StreamORM | None = field(default=None, compare=False, repr=False)


@dataclass(frozen=True)
class SelectedStreamCandidateRecord:
    """Stable selected-stream DTO safe to use outside the SQLAlchemy session scope."""

    id: str
    infohash: str
    raw_title: str
    resolution: str | None = None
    provider: str | None = None


@dataclass(frozen=True)
class RankingRule:
    """One additive ranking rule with an explicit fetch flag."""

    rank: int
    fetch: bool = True


def _default_quality_source_scores() -> dict[str, RankingRule]:
    return {
        "bluray_remux": RankingRule(rank=1200),
        "bluray": RankingRule(rank=1100),
        "bdrip": RankingRule(rank=1000),
        "webdl": RankingRule(rank=900),
        "web-dl": RankingRule(rank=900),
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


def _default_resolution_scores() -> dict[str, RankingRule]:
    return {
        "2160p": RankingRule(rank=500),
        "1080p": RankingRule(rank=300),
        "720p": RankingRule(rank=100),
        "480p": RankingRule(rank=0),
        "unknown": RankingRule(rank=0),
    }


def _default_codec_scores() -> dict[str, RankingRule]:
    return {
        "hevc": RankingRule(rank=150),
        "h265": RankingRule(rank=150),
        "avc": RankingRule(rank=50),
        "x264": RankingRule(rank=50),
        "xvid": RankingRule(rank=-500, fetch=False),
    }


def _default_hdr_scores() -> dict[str, RankingRule]:
    return {
        "dolby_vision": RankingRule(rank=120),
        "hdr10+": RankingRule(rank=100),
        "hdr10": RankingRule(rank=80),
    }


def _default_audio_scores() -> dict[str, RankingRule]:
    return {
        "truehd": RankingRule(rank=200),
        "dts-hd": RankingRule(rank=180),
        "atmos": RankingRule(rank=150),
        "dts": RankingRule(rank=80),
        "aac": RankingRule(rank=20),
        "mp3": RankingRule(rank=-50),
    }


@dataclass(frozen=True)
class RankingModel:
    """First-pass RTN-style additive scoring model with overridable defaults."""

    quality_source_scores: dict[str, RankingRule] = field(
        default_factory=_default_quality_source_scores
    )
    resolution_scores: dict[str, RankingRule] = field(default_factory=_default_resolution_scores)
    codec_scores: dict[str, RankingRule] = field(default_factory=_default_codec_scores)
    hdr_scores: dict[str, RankingRule] = field(default_factory=_default_hdr_scores)
    audio_scores: dict[str, RankingRule] = field(default_factory=_default_audio_scores)
    remove_ranks_under: int = -10000
    require: list[str] = field(default_factory=list)


_SIMILARITY_THRESHOLD_DEFAULT = 0.85
_RESOLUTION_RANKS: dict[str, int] = {
    "2160p": 7,
    "1080p": 6,
    "720p": 5,
    "480p": 4,
    "360p": 3,
    "unknown": 1,
}


def _infer_request_media_type(*, external_ref: str, attributes: dict[str, object]) -> str:
    """Return the best current media-type label for one request-intent record."""

    item_type = _extract_string(attributes, "item_type")
    if item_type is not None and item_type in {"movie", "show", "season", "episode"}:
        return item_type
    if external_ref.startswith("tmdb:"):
        return "movie"
    if external_ref.startswith("tvdb:"):
        return "show"
    return "unknown"


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


def _extract_int_value(attributes: dict[str, object], key: str, *aliases: str) -> int | None:
    """Return one integer-like metadata field when present."""

    for candidate_key in (key, *aliases):
        value = attributes.get(candidate_key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _candidate_matches_partial_scope(
    parsed_seasons: list[int] | None,
    requested_seasons: list[int],
    *,
    allow_unknown: bool = False,
) -> bool:
    """Return whether one parsed candidate should be kept for a partial-season request.

    When ``allow_unknown`` is False (the default), candidates whose titles carry
    no parseable season metadata are rejected.  This prevents ambiguous
    "Complete Series" packs from being silently accepted during targeted partial
    season requests and mapping to the wrong season at the finalisation stage.

    Pass ``allow_unknown=True`` only for paths where scope is intentionally
    absent (e.g. full-show requests or manual session flows).
    """

    if not parsed_seasons:
        return allow_unknown
    requested = set(requested_seasons)
    return any(season in requested for season in parsed_seasons)


def _candidate_parsed_seasons(parsed_title: dict[str, object]) -> list[int] | None:
    """Return normalized parsed season numbers from one torrent-title payload when present."""

    raw_value = parsed_title.get("season")
    if raw_value is None:
        return None
    if isinstance(raw_value, int):
        return [raw_value]
    if isinstance(raw_value, list):
        seasons = [value for value in raw_value if isinstance(value, int)]
        return seasons or None
    return None


def _json_safe_value(value: object) -> object:
    """Normalize parser output into JSON-safe structures for persistence."""

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    return str(value)


def _fallback_infohash_for_raw_title(raw_title: str) -> str:
    """Return a deterministic synthetic infohash when no provider hash exists yet."""

    return hashlib.sha1(raw_title.encode("utf-8")).hexdigest()


def _normalize_title_for_similarity(value: str) -> str:
    """Normalize one title string for deterministic Levenshtein comparison."""

    normalized = unicodedata.normalize("NFKC", value).casefold().replace("_", " ")
    normalized = re.sub(r"[^\w\s]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _dedupe_title_aliases(values: object, *, canonical_title: str | None = None) -> list[str]:
    aliases: list[str] = []
    seen: set[str] = set()
    if canonical_title is not None:
        normalized_canonical = _normalize_title_for_similarity(canonical_title)
        if normalized_canonical:
            seen.add(normalized_canonical)

    if not isinstance(values, list):
        return aliases

    for value in values:
        if not isinstance(value, str):
            continue
        alias = value.strip()
        if not alias:
            continue
        normalized_alias = _normalize_title_for_similarity(alias)
        if not normalized_alias or normalized_alias in seen:
            continue
        seen.add(normalized_alias)
        aliases.append(alias)
    return aliases


def _extract_title_aliases(
    attributes: dict[str, object], *, canonical_title: str | None = None
) -> list[str]:
    return _dedupe_title_aliases(
        attributes.get(_TITLE_ALIASES_ATTRIBUTE_KEY),
        canonical_title=canonical_title,
    )


def _candidate_title_for_similarity(stream: StreamORM) -> str:
    """Return the persisted candidate title used for similarity scoring without re-parsing."""

    parsed_title = stream.parsed_title.get("title")
    if isinstance(parsed_title, str) and parsed_title.strip():
        return parsed_title
    return stream.raw_title


def _levenshtein_ratio(left: str, right: str) -> float:
    """Return the TS-compatible Levenshtein ratio `(len(a)+len(b)-distance)/(len(a)+len(b))`."""

    normalized_left = _normalize_title_for_similarity(left)
    normalized_right = _normalize_title_for_similarity(right)
    total_length = len(normalized_left) + len(normalized_right)
    if total_length == 0:
        return 1.0
    distance = Levenshtein.distance(normalized_left, normalized_right)
    return (total_length - distance) / total_length


def _max_title_similarity(left: str, right: str, aliases: list[str] | None = None) -> float:
    ratios = [_levenshtein_ratio(left, right)]
    ratios.extend(_levenshtein_ratio(alias, right) for alias in aliases or [])
    return max(ratios, default=0.0)


def _normalize_resolution_for_ranking(resolution: str | None) -> str:
    """Normalize one persisted resolution value into the current first-pass ranking tiers."""

    if resolution is None:
        return "unknown"
    normalized = resolution.strip().casefold()
    if normalized == "4k":
        return "2160p"
    if normalized in _RESOLUTION_RANKS:
        return normalized
    return "unknown"


def _flatten_parsed_strings(value: object) -> list[str]:
    """Flatten selected parsed payload values into comparable lower-cased strings."""

    if isinstance(value, str):
        return [value.casefold()]
    if isinstance(value, list):
        flattened: list[str] = []
        for item in value:
            flattened.extend(_flatten_parsed_strings(item))
        return flattened
    if isinstance(value, dict):
        flattened = []
        for item in value.values():
            flattened.extend(_flatten_parsed_strings(item))
        return flattened
    return []


def _parsed_strings(parsed_title: dict[str, object], *keys: str) -> list[str]:
    """Collect lower-cased parsed payload strings from selected keys only."""

    strings: list[str] = []
    for key in keys:
        strings.extend(_flatten_parsed_strings(parsed_title.get(key)))
    return strings


def _quality_source_key(parsed_title: dict[str, object]) -> str | None:
    """Normalize parsed quality/source metadata into the first scoring bucket."""

    tokens = _parsed_strings(parsed_title, "source", "other")
    if any("hdcam" in token for token in tokens):
        return "hdcam"
    if any("cam" in token or "camera" in token for token in tokens):
        return "cam"
    if any("telesync" in token or token == "ts" for token in tokens):
        return "telesync"
    if any("telecine" in token or token == "tc" for token in tokens):
        return "telecine"
    if any("screener" in token for token in tokens):
        return "screener"
    if any("hdtv" in token for token in tokens):
        return "hdtv"
    if any(("blu-ray" in token or "bluray" in token) for token in tokens) and any(
        "remux" in token for token in tokens
    ):
        return "bluray_remux"
    if any(("blu-ray" in token or "bluray" in token) for token in tokens) and any(
        token == "rip" or token.endswith("rip") for token in tokens
    ):
        return "bdrip"
    if any("blu-ray" in token or "bluray" in token for token in tokens):
        return "bluray"
    if any("dvd" in token for token in tokens) and any(
        token == "rip" or token.endswith("rip") for token in tokens
    ):
        return "dvdrip"
    if any("hdrip" in token for token in tokens):
        return "hdrip"
    if any("webrip" in token for token in tokens):
        return "webrip"
    if any("web" in token for token in tokens):
        if any(token == "rip" or token.endswith("rip") for token in tokens):
            return "webrip"
        if any("dl" in token for token in tokens):
            return "web-dl"
        return "webdl"
    return None


def _codec_key(parsed_title: dict[str, object]) -> str | None:
    """Normalize parsed codec metadata into the first scoring bucket."""

    tokens = _parsed_strings(parsed_title, "video_codec", "other")
    if any("xvid" in token for token in tokens):
        return "xvid"
    if any(token in {"h.265", "h265", "hevc"} or "265" in token for token in tokens):
        return "h265"
    if any(token in {"h.264", "h264", "avc"} or "264" in token for token in tokens):
        return "x264"
    if any("av1" in token for token in tokens):
        return "av1"
    return None


def _hdr_key(parsed_title: dict[str, object]) -> str | None:
    """Normalize parsed HDR metadata into the first scoring bucket."""

    tokens = _parsed_strings(parsed_title, "hdr", "other")
    if any("dolby vision" in token or token == "dv" for token in tokens):
        return "dolby_vision"
    if any("hdr10+" in token or "hdr10plus" in token for token in tokens):
        return "hdr10+"
    if any(token == "hdr10" for token in tokens):
        return "hdr10"
    if any(token == "hdr" or token.startswith("hdr ") for token in tokens):
        return "hdr10"
    return None


def _audio_keys(parsed_title: dict[str, object]) -> set[str]:
    """Normalize parsed audio metadata into additive scoring buckets."""

    tokens = _parsed_strings(parsed_title, "audio_codec", "audio_profile", "other")
    keys: set[str] = set()
    if any("truehd" in token or "true hd" in token for token in tokens):
        keys.add("truehd")
    if any(
        "dts-hd" in token or "dts hd" in token or "dts ma" in token or "master audio" in token
        for token in tokens
    ):
        keys.add("dts-hd")
    if any("atmos" in token for token in tokens):
        keys.add("atmos")
    if any(token == "dts" for token in tokens):
        keys.add("dts")
    if any(token == "aac" for token in tokens):
        keys.add("aac")
    if any(token == "mp3" for token in tokens):
        keys.add("mp3")
    return keys


def _matches_require_override(raw_title: str, require_patterns: list[str]) -> bool:
    """Return whether one raw title matches a configured require override regex."""

    for pattern in require_patterns:
        try:
            if re.search(pattern, raw_title, flags=re.IGNORECASE):
                return True
        except re.error:
            continue
    return False


def rank_persisted_streams_for_item(
    item: MediaItemORM,
    streams: list[StreamORM],
    *,
    similarity_threshold: float = _SIMILARITY_THRESHOLD_DEFAULT,
    ranking_model: RankingModel | None = None,
) -> list[RankedStreamCandidateRecord]:
    """Rank persisted parsed stream candidates without reparsing raw titles."""

    canonical_title = item.title or item.external_ref
    title_aliases = _extract_title_aliases(
        cast(dict[str, object], item.attributes or {}),
        canonical_title=canonical_title,
    )
    model = ranking_model or RankingModel()
    ranked: list[RankedStreamCandidateRecord] = []
    for stream in streams:
        similarity_title = _candidate_title_for_similarity(stream)
        lev_ratio = _max_title_similarity(canonical_title, similarity_title, title_aliases)
        normalized_resolution = _normalize_resolution_for_ranking(stream.resolution)
        stream.lev_ratio = lev_ratio
        stream.resolution = normalized_resolution

        if lev_ratio < similarity_threshold:
            stream.rank = 0
            ranked.append(
                RankedStreamCandidateRecord(
                    item_id=item.id,
                    stream_id=stream.id,
                    rank_score=0,
                    lev_ratio=lev_ratio,
                    fetch=False,
                    passed=False,
                    rejection_reason="similarity_below_threshold",
                    stream=stream,
                )
            )
            continue

        resolution_rule = model.resolution_scores.get(normalized_resolution, RankingRule(rank=0))
        rank_score = int(resolution_rule.rank)
        fetch_allowed = True
        rejection_reason: str | None = None

        quality_source_key = _quality_source_key(stream.parsed_title)
        if quality_source_key is not None:
            rule = model.quality_source_scores.get(quality_source_key)
            if rule is not None:
                rank_score += rule.rank
                if not rule.fetch and rejection_reason is None:
                    fetch_allowed = False
                    rejection_reason = f"quality_source_fetch_disabled:{quality_source_key}"

        codec_key = _codec_key(stream.parsed_title)
        if codec_key is not None:
            rule = model.codec_scores.get(codec_key)
            if rule is not None:
                rank_score += rule.rank
                if not rule.fetch and rejection_reason is None:
                    fetch_allowed = False
                    rejection_reason = f"codec_fetch_disabled:{codec_key}"

        hdr_key = _hdr_key(stream.parsed_title)
        if hdr_key is not None:
            rule = model.hdr_scores.get(hdr_key)
            if rule is not None:
                rank_score += rule.rank
                if not rule.fetch and rejection_reason is None:
                    fetch_allowed = False
                    rejection_reason = f"hdr_fetch_disabled:{hdr_key}"

        for audio_key in sorted(_audio_keys(stream.parsed_title)):
            rule = model.audio_scores.get(audio_key)
            if rule is not None:
                rank_score += rule.rank
                if not rule.fetch and rejection_reason is None:
                    fetch_allowed = False
                    rejection_reason = f"audio_fetch_disabled:{audio_key}"

        if rank_score < model.remove_ranks_under and rejection_reason is None:
            fetch_allowed = False
            rejection_reason = "rank_below_threshold"

        if _matches_require_override(stream.raw_title, model.require):
            fetch_allowed = True
            rejection_reason = None

        stream.rank = rank_score
        ranked.append(
            RankedStreamCandidateRecord(
                item_id=item.id,
                stream_id=stream.id,
                rank_score=rank_score,
                lev_ratio=lev_ratio,
                fetch=fetch_allowed,
                passed=fetch_allowed,
                rejection_reason=rejection_reason,
                stream=stream,
            )
        )

    return sorted(
        ranked,
        key=lambda record: (
            not record.passed,
            -record.rank_score,
            -record.lev_ratio,
            _part_tiebreaker(record.stream.parsed_title) if record.stream is not None else 0,
            record.stream_id,
        ),
    )


def select_stream_candidate(
    ranked_results: list[RankedStreamCandidateRecord],
) -> SelectedStreamCandidateRecord | None:
    """Select the best passing candidate by score, then similarity, then stable id order."""

    passing_candidates = [record for record in ranked_results if record.passed]
    if not passing_candidates:
        return None

    selected_record = min(
        passing_candidates,
        key=lambda record: (
            -record.rank_score,
            -record.lev_ratio,
            _part_tiebreaker(record.stream.parsed_title) if record.stream is not None else 0,
            record.stream_id,
        ),
    )
    stream = selected_record.stream
    if stream is None:
        return SelectedStreamCandidateRecord(
            id=selected_record.stream_id,
            infohash="",
            raw_title="",
            resolution=None,
            provider=None,
        )
    return SelectedStreamCandidateRecord(
        id=stream.id,
        infohash=stream.infohash,
        raw_title=stream.raw_title,
        resolution=stream.resolution,
        provider=None,
    )


def parse_stream_candidate_title(
    raw_title: str,
    *,
    infohash: str | None = None,
) -> ParsedStreamCandidateRecord:
    """Parse one raw stream candidate title into a persisted pre-ranking record."""

    parsed = parse_torrent_name(raw_title)
    return ParsedStreamCandidateRecord(
        raw_title=parsed.raw_title,
        infohash=infohash or _fallback_infohash_for_raw_title(parsed.raw_title),
        parsed_title=parsed.parsed_title,
        resolution=parsed.resolution,
    )


def validate_parsed_stream_candidate(
    item: MediaItemORM,
    candidate: ParsedStreamCandidateRecord,
) -> ParsedStreamCandidateValidation:
    """Perform the first content-level validation pass on one parsed candidate."""

    attributes = cast(dict[str, object], item.attributes or {})
    expected_type = _infer_request_media_type(external_ref=item.external_ref, attributes=attributes)
    parsed_type = candidate.parsed_title.get("type")
    parsed_kind = parsed_type if isinstance(parsed_type, str) else None

    if expected_type == "movie" and parsed_kind == "episode":
        return ParsedStreamCandidateValidation(
            ok=False, reason="movie_request_got_episode_candidate"
        )
    # Only reject a show/season/episode request that matched a candidate whose RTN type
    # is "movie" when the candidate also contains episode/season markers — meaning it
    # genuinely is a movie and not a full-season or full-show pack (which RTN may type
    # as "movie" when there are no S/E markers in the title).
    if expected_type in {"show", "season", "episode"} and parsed_kind == "movie":
        has_season_marker = _extract_int(candidate.parsed_title, "season") is not None
        has_episode_marker = _extract_int(candidate.parsed_title, "episode") is not None
        if has_season_marker or has_episode_marker:
            return ParsedStreamCandidateValidation(ok=False, reason="show_request_got_movie_candidate")

    expected_season = _extract_int_value(
        attributes, "season_number", "season", "parent_season_number"
    )
    parsed_season = _extract_int(candidate.parsed_title, "season")
    if (
        expected_type in {"season", "episode"}
        and expected_season is not None
        and parsed_season is not None
        and expected_season != parsed_season
    ):
        return ParsedStreamCandidateValidation(ok=False, reason="season_mismatch")

    expected_episode = _extract_int_value(attributes, "episode_number", "episode")
    parsed_episode = _extract_int(candidate.parsed_title, "episode")
    if (
        expected_type == "episode"
        and expected_episode is not None
        and parsed_episode is not None
        and expected_episode != parsed_episode
    ):
        return ParsedStreamCandidateValidation(ok=False, reason="episode_mismatch")

    expected_year = _extract_int(attributes, "year")
    parsed_year = _extract_int(candidate.parsed_title, "year")
    if (
        expected_year is not None
        and parsed_year is not None
        and abs(expected_year - parsed_year) > 1
    ):
        return ParsedStreamCandidateValidation(ok=False, reason="year_mismatch")

    return ParsedStreamCandidateValidation(ok=True)


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
        request_source=request_source,
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
    record.request_source = request_source
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
    tvdb_id: str | None
    tmdb_id: str | None
    show_title: str
    item_type: str
    aired_at: str
    season: int | None = None
    episode: int | None = None
    last_state: str | None = None
    release_data: CalendarReleaseDataRecord | None = None


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
@dataclass(frozen=True)
class ShowCompletionResult:
    """Coverage result for one requested show scope using cached metadata only."""

    all_satisfied: bool
    any_satisfied: bool
    has_future_episodes: bool
    missing_released: list[tuple[int, int]]


@dataclass
class _SeasonEpisodeInventory:
    """Known, released, and future episode numbers for one season snapshot."""

    known_episodes: set[int] = field(default_factory=set)
    released_episodes: set[int] = field(default_factory=set)
    future_episodes: set[int] = field(default_factory=set)


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
    inventory_by_season: dict[int, _SeasonEpisodeInventory],
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

    inventory = inventory_by_season.setdefault(season_number, _SeasonEpisodeInventory())
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


def _extract_tmdb_episode_inventory(
    attributes: dict[str, object],
    *,
    today: date,
) -> dict[int, _SeasonEpisodeInventory]:
    inventory_by_season: dict[int, _SeasonEpisodeInventory] = {}
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

            inventory = inventory_by_season.setdefault(season_number, _SeasonEpisodeInventory())
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
    inventory_by_season: dict[int, _SeasonEpisodeInventory],
    fallback_scope: dict[int, set[int]],
) -> set[int]:
    inventory = inventory_by_season.get(season_number)
    if inventory is not None and inventory.known_episodes:
        return set(inventory.known_episodes)
    return set(fallback_scope.get(season_number, set()))


def _build_requested_episode_scope(
    request_record: ItemRequestORM | None,
    inventory_by_season: dict[int, _SeasonEpisodeInventory],
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



_SEASON_NUMBER_RE: tuple[re.Pattern[str], ...] = (
    re.compile(r"[Ss]eason\s*(\d+)", re.IGNORECASE),  # "Season 1", "season 01"
    re.compile(r"[Ss](\d{1,2})[Ee]\d{1,2}"),           # S01E02
    re.compile(r"(\d{1,2})x\d{1,2}"),                  # 1x02
)
_EPISODE_NUMBER_RE: tuple[re.Pattern[str], ...] = (
    re.compile(r"[Ss]\d{1,2}[Ee](\d{1,3})", re.IGNORECASE),
    re.compile(r"\b\d{1,2}x(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"\b[Ee]p?(?:isode)?\s*(\d{1,3})\b", re.IGNORECASE),
)

# Range patterns: "S01-S08", "S01-08", "Seasons 1-4", "Season 1-4"
_SEASON_RANGE_RE: tuple[re.Pattern[str], ...] = (
    re.compile(r"[Ss](?:eason)?s?\s*(\d{1,2})[-\u2013](\d{1,2})\b", re.IGNORECASE),  # S01-S08 / Seasons 1-4
    re.compile(r"[Ss](\d{1,2})[-\u2013][Ss](\d{1,2})\b"),                               # S01-S08 strict
)
_MAX_SEASON_RANGE = 20  # cap to avoid false positives on episode ranges


def _infer_season_number_from_path(path: str | None) -> int | None:
    """Return a season number inferred from common file-naming patterns, or None."""
    if not path:
        return None
    for pattern in _SEASON_NUMBER_RE:
        match = pattern.search(path)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                pass
    return None


def _infer_season_range_from_path(path: str | None) -> list[int]:
    """Return all season numbers inferred from a path, including pack ranges.

    Handles single-season patterns (returns ``[n]``) and range patterns such as
    ``S01-S08``, ``Seasons 1-4``, or ``S1-8`` (returns the full range list).
    Results are sorted ascending so earlier seasons are prioritised in callers.
    An empty list is returned when no season can be inferred.
    """
    if not path:
        return []
    # Try range patterns first (more specific)
    for pattern in _SEASON_RANGE_RE:
        match = pattern.search(path)
        if match:
            try:
                start, end = int(match.group(1)), int(match.group(2))
                if start <= end and (end - start) < _MAX_SEASON_RANGE:
                    return list(range(start, end + 1))
            except (ValueError, IndexError):
                pass
    # Fall back to single-season detection
    single = _infer_season_number_from_path(path)
    if single is not None:
        return [single]
    return []


def _infer_episode_number_from_path(path: str | None) -> int | None:
    """Return one episode number inferred from common file-naming patterns, or None."""

    if not path:
        return None
    for pattern in _EPISODE_NUMBER_RE:
        match = pattern.search(path)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                pass
    return None


async def _evaluate_show_completion(
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

        inventory_by_season = _extract_tmdb_episode_inventory(show_attributes, today=today)
        fallback_scope: dict[int, set[int]] = {}
        episode_item_ids_by_scope: dict[tuple[int, int], str] = {}

        if show_orm is not None:
            for season in show_orm.seasons:
                season_number = season.season_number
                if season_number is None:
                    continue

                inventory = inventory_by_season.setdefault(
                    season_number,
                    _SeasonEpisodeInventory(),
                )
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
            # Use MediaEntryORM (download URL exists from debrid) rather than ActiveStreamORM
            # (which only exists after an active playback session). Waiting for playback caused
            # an infinite finalize→scrape loop since no stream rows exist at initial library setup.
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
            # No SeasonORM/EpisodeORM children exist — this is a whole-show request where
            # debrid downloaded content as a pack keyed to the show-level media_item_id.
            # Query the actual media entries to determine which seasons are covered.
            show_entry_path_result = await session.execute(
                select(MediaEntryORM.provider_file_path, MediaEntryORM.original_filename)
                .where(
                    MediaEntryORM.item_id == item.id,
                    MediaEntryORM.refresh_state.in_(_SATISFYING_MEDIA_ENTRY_REFRESH_STATES),
                    MediaEntryORM.entry_type == "media",
                )
            )
            entry_paths = list(show_entry_path_result.all())
            show_has_entry = bool(entry_paths)
            if show_has_entry:
                # Infer covered scope from the actual persisted file paths.
                # Season packs (no explicit episode marker) satisfy the full season,
                # while single-episode files only satisfy their own episode.
                season_pack_seasons: set[int] = set()
                covered_scope_from_paths: set[tuple[int, int]] = set()
                for file_path, original_filename in entry_paths:
                    candidate_path = file_path or original_filename
                    inferred_seasons = _infer_season_range_from_path(candidate_path)
                    inferred_episode = _infer_episode_number_from_path(candidate_path)
                    if inferred_episode is not None and len(inferred_seasons) == 1:
                        covered_scope_from_paths.add((inferred_seasons[0], inferred_episode))
                    elif inferred_seasons:
                        season_pack_seasons.update(inferred_seasons)

                covered_seasons = set(season_pack_seasons) | {
                    season_number for season_number, _episode_number in covered_scope_from_paths
                }

                if not covered_seasons:
                    # No season info in paths.
                    # We cannot safely assume it covers newly requested seasons,
                    # as doing so instantly completes the new request and breaks
                    # the "Request More" flow. Leave covered_seasons empty so it
                    # remains partially_completed and triggers a directed scrape.
                    pass

                logger.info(
                    "evaluate_show_completion: no episode children but show-level media entries"
                    " found — pack-satisfied for seasons: %s",
                    sorted(covered_seasons),
                    extra={"item_id": item.id},
                )

                # Synthesise episode_item_ids_by_scope from released_scope when inventory
                # exists (TMDB-indexed shows).  For TVDB-indexed shows released_scope is
                # often empty because no TMDB episode inventory was fetched; in that case
                # we return a direct result derived purely from known request scope vs
                # covered seasons to avoid the inventory_is_empty guard below.
                if released_scope:
                    for scope_key in released_scope:
                        season_num_key, _ = scope_key
                        if scope_key in covered_scope_from_paths or season_num_key in season_pack_seasons:
                            episode_item_ids_by_scope[scope_key] = str(item.id)
                    if episode_item_ids_by_scope:
                        active_item_ids = {str(item.id)}
                else:
                    # No TMDB inventory available (TVDB show or inventory not yet fetched).
                    # Determine what was requested and compare against covered seasons.
                    if requested_scope:
                        total_requested: set[int] = {s for s, e in requested_scope}
                    elif (
                        latest_request is not None
                        and latest_request.is_partial
                        and latest_request.requested_seasons
                    ):
                        total_requested = set(latest_request.requested_seasons)
                    else:
                        # Full-show request with no explicit season scope.
                        #
                        # Do NOT infer "all requested" from ``covered_seasons`` because a
                        # single-episode path like "S01E08" would self-satisfy and incorrectly
                        # mark the whole show complete. Use only metadata-backed inventory keys.
                        total_requested = set(inventory_by_season.keys())
                        if not total_requested:
                            logger.warning(
                                "evaluate_show_completion: no inventory for full-show request;"
                                " treating pack as incomplete",
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
                        "evaluate_show_completion: no TMDB inventory — direct pack result",
                        extra={
                            "item_id": item.id,
                            "covered": sorted(covered_seasons),
                            "requested": sorted(total_requested),
                            "missing": missing_seasons,
                            "all_satisfied": all_satisfied,
                            "any_satisfied": any_satisfied,
                        },
                    )
                    # missing_released expects (season, episode) pairs; use sentinel ep=0.
                    return ShowCompletionResult(
                        all_satisfied=all_satisfied,
                        any_satisfied=any_satisfied,
                        has_future_episodes=bool(future_scope),
                        missing_released=[(s, 0) for s in missing_seasons],
                    )


    satisfied_scope = {
        scope
        for scope, episode_item_id in episode_item_ids_by_scope.items()
        if scope in released_scope and episode_item_id in active_item_ids
    }

    unresolved_requested = requested_scope - released_scope - future_scope

    # Guard: if no episode coverage data exists at all we cannot declare the show complete.
    # This covers two distinct scenarios:
    #   1. episode_item_ids_by_scope AND released_scope are both empty (ShowORM not yet
    #      populated, no air dates) — empty-set equality would make all_satisfied=True.
    #   2. requested_scope is empty even though the request explicitly names seasons
    #      (TMDB/TVDB episode inventory not yet fetched) — same empty-set trap.
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
    ) -> MediaItemRecord:
        """Move one item back to requested so the worker can run a fresh scrape cycle."""

        async with self._db.session() as session:
            item = (
                await session.execute(select(MediaItemORM).where(MediaItemORM.id == item_id))
            ).scalar_one_or_none()
            if item is None:
                raise ItemNotFoundError(f"unknown item_id={item_id}")

            item.next_retry_at = None
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
                if not validation.ok:
                    continue
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
                        parsed_title=parsed_candidate.parsed_title,
                        rank=0,
                        lev_ratio=None,
                        resolution=parsed_candidate.resolution,
                    )
                    session.add(existing)
                    existing_by_key[stream_key] = existing
                else:
                    existing.parsed_title = parsed_candidate.parsed_title
                    existing.resolution = parsed_candidate.resolution

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

                existing: MediaEntryORM | None = None
                if provider_download_key and file_id_key and file_name_key:
                    strict_key = (provider_download_key, file_id_key, file_name_key)
                    existing = existing_by_strict_key.get(strict_key)
                if existing is None and provider_key and file_name_key:
                    existing = existing_by_provider_path_key.get((provider_key, file_name_key))
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
                        provider_file_path=file.file_name,
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
                    existing.provider_file_path = file.file_name
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
    ) -> MediaItemsPage:
        """Return paginated item summaries for current library compatibility routes."""

        bounded_limit = max(1, min(limit, 100))
        bounded_page = max(1, page)

        async with self._db.session() as session:
            statement = select(MediaItemORM).order_by(MediaItemORM.created_at.desc())
            if tenant_id is not None:
                statement = statement.where(MediaItemORM.tenant_id == tenant_id)
            rows = ((await session.execute(statement)).scalars().all())

        items = [_build_summary_record(item, extended=extended) for item in rows]
        filtered = [
            item
            for item in items
            if _matches_item_type(item, item_types)
            and _matches_state(item, states)
            and _matches_search(item, search)
        ]
        ordered = _sort_items(filtered, sort)
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
                    tenant_id=tenant_id,
                )
            else:
                record = await self.request_item(
                    external_ref=identifier,
                    media_type=media_type,
                    title=request_title,
                    attributes=attributes,
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
                show_title=item.title,
                item_type=item.item_type,
                aired_at=item.air_date,
                season=item.season_number,
                episode=item.episode_number,
                last_state=item.last_state,
                release_data=item.release_data,
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
                .options(
                    selectinload(MediaItemORM.show),
                    selectinload(MediaItemORM.season).selectinload(SeasonORM.show),
                    selectinload(MediaItemORM.episode)
                    .selectinload(EpisodeORM.season)
                    .selectinload(SeasonORM.show),
                )
                .order_by(MediaItemORM.created_at.desc())
            )
            if tenant_id is not None:
                statement = statement.where(MediaItemORM.tenant_id == tenant_id)
            rows = ((await session.execute(statement)).scalars().all())

        result: list[CalendarProjectionRecord] = []
        child_show_item_ids: set[str] = set()
        for item in rows:
            attributes = cast(dict[str, object], item.attributes or {})
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

            item_type = _extract_first_string(attributes, "item_type") or (
                "episode"
                if item.episode is not None
                else "season"
                if item.season is not None
                else "tv"
                if item.show is not None
                else "movie"
            )
            if item_type == "show":
                item_type = "tv"
            tmdb_id = _extract_string(attributes, "tmdb_id")
            tvdb_id = _extract_string(attributes, "tvdb_id")
            season_number = _extract_int_value(
                attributes, "season_number", "season", "parent_season_number"
            )
            episode_number = _extract_int_value(attributes, "episode_number", "episode")
            release_data = _build_calendar_release_data(attributes)

            if item.season is not None:
                season_number = item.season.season_number
                if item.season.show is not None:
                    child_show_item_ids.add(item.season.show.media_item_id)
                    tmdb_id, tvdb_id = _resolve_calendar_parent_identifiers(
                        attributes,
                        fallback_tmdb_id=item.season.show.tmdb_id,
                        fallback_tvdb_id=item.season.show.tvdb_id,
                    )
            elif item.episode is not None:
                episode_number = item.episode.episode_number
                if item.episode.season is not None:
                    season_number = item.episode.season.season_number
                    if item.episode.season.show is not None:
                        child_show_item_ids.add(item.episode.season.show.media_item_id)
                        tmdb_id, tvdb_id = _resolve_calendar_parent_identifiers(
                            attributes,
                            fallback_tmdb_id=item.episode.season.show.tmdb_id,
                            fallback_tvdb_id=item.episode.season.show.tvdb_id,
                        )

            result.append(
                CalendarProjectionRecord(
                    item_id=item.id,
                    title=_resolve_calendar_show_title(item, attributes),
                    item_type=item_type,
                    tmdb_id=tmdb_id,
                    tvdb_id=tvdb_id,
                    episode_number=episode_number,
                    season_number=season_number,
                    air_date=aired_at or item.created_at.date().isoformat(),
                    last_state=_canonical_state_name(item.state),
                    release_data=release_data,
                )
            )

        existing_item_ids = {item.item_id for item in result}
        for item in rows:
            if item.id in existing_item_ids or item.id in child_show_item_ids:
                continue

            attributes = cast(dict[str, object], item.attributes or {})
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
                    title=_resolve_calendar_show_title(item, attributes),
                    item_type=_extract_first_string(attributes, "item_type") or "show",
                    tmdb_id=_extract_string(attributes, "tmdb_id"),
                    tvdb_id=_extract_string(attributes, "tvdb_id"),
                    episode_number=None,
                    season_number=None,
                    air_date=next_aired or fallback_dt.date().isoformat(),
                    last_state=_canonical_state_name(item.state),
                    release_data=release_data,
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

    async def get_stream_candidates(self, *, media_item_id: str) -> list[StreamORM]:
        """Return persisted stream candidates for one item in deterministic order."""

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

        return sorted(item.streams, key=lambda stream: (stream.created_at, stream.id))

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
                    )
                else:
                    await self._upsert_item_request(
                        session,
                        tenant_id=tenant_id,
                        external_ref=normalized_external_ref,
                        media_item_id=existing.id,
                        requested_title=existing.title,
                        media_type=candidate_media_type,
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
                    )
                else:
                    await self._upsert_item_request(
                        session,
                        tenant_id=tenant_id,
                        external_ref=normalized_external_ref,
                        media_item_id=item.id,
                        requested_title=item.title,
                        media_type=candidate_media_type,
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
                        )
                    else:
                        await self._upsert_item_request(
                            retry_session,
                            tenant_id=tenant_id,
                            external_ref=normalized_external_ref,
                            media_item_id=existing.id,
                            requested_title=existing.title,
                            media_type=candidate_media_type,
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
