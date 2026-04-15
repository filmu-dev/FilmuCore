"""Composable GraphQL resolver classes for plugin-dfilmu schema growth."""

# mypy: disable-error-code=untyped-decorator

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from importlib.metadata import PackageNotFoundError, version
from pathlib import PurePosixPath
from types import SimpleNamespace
from typing import Any, cast

import strawberry
from strawberry.scalars import JSON
from strawberry.types import Info

from filmu_py.core.metadata_reindex_status import MetadataReindexStatusStore
from filmu_py.core.queue_status import QueueStatusReader
from filmu_py.core.runtime_lifecycle import RuntimeLifecycleSnapshot
from filmu_py.db.models import StreamORM
from filmu_py.graphql.deps import GraphQLContext
from filmu_py.graphql.types import (
    GQLActiveStream,
    GQLActiveStreamOwner,
    GQLCalendarEntry,
    GQLFilmuSettings,
    GQLHealthCheck,
    GQLItemEvent,
    GQLLibraryStats,
    GQLMarkSelectedHlsMediaEntryStaleResult,
    GQLMediaEntry,
    GQLMediaItem,
    GQLMediaItemDetail,
    GQLMetadataReindexHistoryPoint,
    GQLMetadataReindexStatus,
    GQLPersistMediaEntryControlResult,
    GQLPersistPlaybackAttachmentControlResult,
    GQLPlaybackAttachment,
    GQLPlaybackRefreshTriggerResult,
    GQLQueueAlert,
    GQLRecoveryMechanism,
    GQLRecoveryPlan,
    GQLRecoveryTargetStage,
    GQLResolvedPlayback,
    GQLResolvedPlaybackAttachment,
    GQLRuntimeLifecycleSnapshot,
    GQLRuntimeLifecycleTransition,
    GQLStreamCandidate,
    GQLVfsBlockedItem,
    GQLVfsCatalogEntry,
    GQLVfsCatalogRollup,
    GQLVfsCatalogStats,
    GQLVfsCorrelationKeys,
    GQLVfsDirectoryDetail,
    GQLVfsDirectoryListing,
    GQLVfsFileDetail,
    GQLVfsRollupBucket,
    GQLVfsSnapshot,
    GQLWorkerQueueHistoryPoint,
    GQLWorkerQueueStatus,
    ItemActionInput,
    ItemStateChangedEvent,
    MediaKind,
    PersistMediaEntryControlInput,
    PersistPlaybackAttachmentControlInput,
    RequestItemInput,
    RequestItemResult,
    ResetItemResult,
    RetryItemResult,
    SettingsUpdateInput,
)
from filmu_py.services.media import (
    ArqNotEnabledError,
    CalendarProjectionRecord,
    ItemActionResult,
    ItemNotFoundError,
    MediaItemRecord,
    MediaItemSummaryRecord,
    RecoveryPlanRecord,
    StatsProjection,
    _canonical_item_type_name,
    _infer_request_media_type,
)
from filmu_py.services.playback import (
    AppScopedDirectPlaybackRefreshTriggerResult,
    AppScopedHlsFailedLeaseRefreshTriggerResult,
    AppScopedHlsRestrictedFallbackRefreshTriggerResult,
    PersistedMediaEntryControlMutationResult,
    PersistedPlaybackAttachmentControlMutationResult,
    trigger_direct_playback_refresh_from_resources,
    trigger_hls_failed_lease_refresh_from_resources,
    trigger_hls_restricted_fallback_refresh_from_resources,
)
from filmu_py.services.vfs_catalog import (
    VfsCatalogEntry,
    VfsCatalogRollup,
    VfsCatalogSnapshot,
    summarize_vfs_catalog_snapshot,
)


def _resolve_service_version() -> str:
    """Resolve package version for GraphQL settings parity output."""

    try:
        return version("filmu-python")
    except PackageNotFoundError:
        return "0.1.0"


def build_filmu_settings(info: Info[GraphQLContext, object]) -> GQLFilmuSettings:
    """Build the core `filmu` settings object for the GraphQL settings root."""

    current_settings = info.context.resources.settings
    return GQLFilmuSettings(
        version=_resolve_service_version(),
        api_key=current_settings.api_key.get_secret_value(),
        log_level=current_settings.log_level,
    )


def _to_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    return int(value) if value.isdigit() else None


def _serialize_release_data(record: CalendarProjectionRecord) -> str | None:
    if record.release_data is None:
        return None
    return json.dumps(
        {
            "next_aired": record.release_data.next_aired,
            "nextAired": record.release_data.nextAired,
            "last_aired": record.release_data.last_aired,
            "lastAired": record.release_data.lastAired,
        }
    )


def _record_media_type(record: MediaItemRecord) -> str:
    return _canonical_item_type_name(
        _infer_request_media_type(external_ref=record.external_ref, attributes=record.attributes)
    )


def _summary_media_type(record: MediaItemSummaryRecord) -> str:
    return _canonical_item_type_name(record.type)


def _media_kind(media_type: str) -> MediaKind:
    normalized = _canonical_item_type_name(media_type)
    if normalized == "movie":
        return MediaKind.MOVIE
    if normalized == "show":
        return MediaKind.SHOW
    if normalized == "season":
        return MediaKind.SEASON
    if normalized == "episode":
        return MediaKind.EPISODE
    raise ValueError(f"unsupported media type for MediaKind: {media_type}")


def _build_calendar_entry(record: CalendarProjectionRecord) -> GQLCalendarEntry:
    specialization = record.specialization
    return GQLCalendarEntry(
        item_id=strawberry.ID(record.item_id),
        show_title=record.title,
        item_type=record.item_type,
        aired_at=record.air_date,
        last_state=record.last_state or "Unknown",
        season=record.season_number,
        episode=record.episode_number,
        tmdb_id=_to_optional_int(record.tmdb_id),
        tvdb_id=_to_optional_int(record.tvdb_id),
        imdb_id=(specialization.imdb_id if specialization is not None else None),
        parent_tmdb_id=(
            _to_optional_int(specialization.parent_ids.tmdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        parent_tvdb_id=(
            _to_optional_int(specialization.parent_ids.tvdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        release_data=_serialize_release_data(record),
    )


def _format_optional_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _build_vfs_correlation_keys(record: VfsCatalogEntry) -> GQLVfsCorrelationKeys:
    correlation = record.correlation
    return GQLVfsCorrelationKeys(
        item_id=correlation.item_id,
        media_entry_id=correlation.media_entry_id,
        source_attachment_id=correlation.source_attachment_id,
        provider=correlation.provider,
        provider_download_id=correlation.provider_download_id,
        provider_file_id=correlation.provider_file_id,
        provider_file_path=correlation.provider_file_path,
        session_id=correlation.session_id,
        handle_key=correlation.handle_key,
    )


def _build_vfs_catalog_entry(record: VfsCatalogEntry) -> GQLVfsCatalogEntry:
    file_payload = record.file
    directory_payload = record.directory
    return GQLVfsCatalogEntry(
        entry_id=record.entry_id,
        parent_entry_id=record.parent_entry_id,
        path=record.path,
        name=record.name,
        kind=record.kind,
        correlation=_build_vfs_correlation_keys(record),
        directory=(
            GQLVfsDirectoryDetail(path=directory_payload.path)
            if directory_payload is not None
            else None
        ),
        file=(
            GQLVfsFileDetail(
                item_id=file_payload.item_id,
                item_title=file_payload.item_title,
                item_external_ref=file_payload.item_external_ref,
                media_entry_id=file_payload.media_entry_id,
                source_attachment_id=file_payload.source_attachment_id,
                media_type=file_payload.media_type,
                transport=file_payload.transport,
                locator=file_payload.locator,
                local_path=file_payload.local_path,
                restricted_url=file_payload.restricted_url,
                unrestricted_url=file_payload.unrestricted_url,
                original_filename=file_payload.original_filename,
                size_bytes=file_payload.size_bytes,
                lease_state=file_payload.lease_state,
                expires_at=_format_optional_datetime(file_payload.expires_at),
                last_refreshed_at=_format_optional_datetime(file_payload.last_refreshed_at),
                last_refresh_error=file_payload.last_refresh_error,
                provider=file_payload.provider,
                provider_download_id=file_payload.provider_download_id,
                provider_file_id=file_payload.provider_file_id,
                provider_file_path=file_payload.provider_file_path,
                active_roles=list(file_payload.active_roles),
                source_key=file_payload.source_key,
                query_strategy=file_payload.query_strategy,
                provider_family=file_payload.provider_family,
                locator_source=file_payload.locator_source,
                match_basis=file_payload.match_basis,
                restricted_fallback=file_payload.restricted_fallback,
            )
            if file_payload is not None
            else None
        ),
    )


def _build_vfs_catalog_stats(snapshot: VfsCatalogSnapshot) -> GQLVfsCatalogStats:
    return GQLVfsCatalogStats(
        directory_count=snapshot.stats.directory_count,
        file_count=snapshot.stats.file_count,
        blocked_item_count=snapshot.stats.blocked_item_count,
    )


def _build_vfs_blocked_item(item: object) -> GQLVfsBlockedItem:
    typed_item: Any = item
    return GQLVfsBlockedItem(
        item_id=str(typed_item.item_id),
        external_ref=str(typed_item.external_ref),
        title=str(typed_item.title),
        reason=str(typed_item.reason),
    )


def _build_vfs_rollup_buckets(values: dict[str, int]) -> list[GQLVfsRollupBucket]:
    return [GQLVfsRollupBucket(key=key, count=count) for key, count in values.items()]


def _build_vfs_catalog_rollup(rollup: VfsCatalogRollup) -> GQLVfsCatalogRollup:
    return GQLVfsCatalogRollup(
        blocked_reasons=_build_vfs_rollup_buckets(rollup.blocked_reason_counts),
        query_strategies=_build_vfs_rollup_buckets(rollup.query_strategy_counts),
        provider_families=_build_vfs_rollup_buckets(rollup.provider_family_counts),
        lease_states=_build_vfs_rollup_buckets(rollup.lease_state_counts),
        locator_sources=_build_vfs_rollup_buckets(rollup.locator_source_counts),
        restricted_fallback_file_count=rollup.restricted_fallback_file_count,
        provider_path_preserved_file_count=rollup.provider_path_preserved_file_count,
        multi_role_file_count=rollup.multi_role_file_count,
    )


def _normalize_vfs_path(path: str) -> str:
    stripped = path.strip()
    if not stripped or stripped == "/":
        return "/"
    normalized = PurePosixPath(f"/{stripped.lstrip('/')}").as_posix()
    return normalized if normalized.startswith("/") else f"/{normalized}"


async def _resolve_vfs_snapshot(
    info: Info[GraphQLContext, object],
    generation_id: str | None,
) -> VfsCatalogSnapshot | None:
    supplier = info.context.resources.vfs_catalog_supplier
    if supplier is None:
        return None
    if generation_id is None:
        return await supplier.build_snapshot()
    if not generation_id.isdigit():
        return None
    return await supplier.snapshot_for_generation(int(generation_id))


def _find_vfs_entry(snapshot: VfsCatalogSnapshot, path: str) -> VfsCatalogEntry | None:
    normalized_path = _normalize_vfs_path(path)
    return next((entry for entry in snapshot.entries if entry.path == normalized_path), None)


def _build_vfs_snapshot(snapshot: VfsCatalogSnapshot) -> GQLVfsSnapshot:
    rollup = summarize_vfs_catalog_snapshot(snapshot)
    return GQLVfsSnapshot(
        generation_id=snapshot.generation_id,
        published_at=snapshot.published_at.isoformat(),
        stats=_build_vfs_catalog_stats(snapshot),
        rollup=_build_vfs_catalog_rollup(rollup),
        blocked_items=[_build_vfs_blocked_item(item) for item in snapshot.blocked_items],
    )


def _build_runtime_lifecycle_snapshot(snapshot: RuntimeLifecycleSnapshot) -> GQLRuntimeLifecycleSnapshot:
    return GQLRuntimeLifecycleSnapshot(
        phase=snapshot.phase,
        health=snapshot.health,
        detail=snapshot.detail,
        updated_at=snapshot.updated_at.isoformat(),
        transitions=[
            GQLRuntimeLifecycleTransition(
                phase=transition.phase,
                health=transition.health,
                detail=transition.detail,
                at=transition.at.isoformat(),
            )
            for transition in snapshot.transitions
        ],
    )


def _queue_name(info: Info[GraphQLContext, object]) -> str:
    resources = info.context.resources
    return resources.arq_queue_name or resources.settings.arq_queue_name


def _queue_redis(info: Info[GraphQLContext, object]) -> object:
    resources = info.context.resources
    return resources.arq_redis or resources.redis


def _build_queue_alert(alert: object) -> GQLQueueAlert:
    typed_alert: Any = alert
    return GQLQueueAlert(
        code=str(typed_alert.code),
        severity=str(typed_alert.severity),
        message=str(typed_alert.message),
    )


def _build_worker_queue_status(info: Info[GraphQLContext, object], snapshot: object) -> GQLWorkerQueueStatus:
    typed_snapshot: Any = snapshot
    return GQLWorkerQueueStatus(
        queue_name=str(typed_snapshot.queue_name),
        arq_enabled=bool(info.context.resources.settings.arq_enabled),
        observed_at=str(typed_snapshot.observed_at),
        total_jobs=int(typed_snapshot.total_jobs),
        ready_jobs=int(typed_snapshot.ready_jobs),
        deferred_jobs=int(typed_snapshot.deferred_jobs),
        in_progress_jobs=int(typed_snapshot.in_progress_jobs),
        retry_jobs=int(typed_snapshot.retry_jobs),
        result_jobs=int(typed_snapshot.result_jobs),
        dead_letter_jobs=int(typed_snapshot.dead_letter_jobs),
        alert_level=str(typed_snapshot.alert_level),
        alerts=[_build_queue_alert(alert) for alert in typed_snapshot.alerts],
        oldest_ready_age_seconds=typed_snapshot.oldest_ready_age_seconds,
        next_scheduled_in_seconds=typed_snapshot.next_scheduled_in_seconds,
        dead_letter_oldest_age_seconds=typed_snapshot.dead_letter_oldest_age_seconds,
        dead_letter_reason_counts=cast(
            JSON, dict(typed_snapshot.dead_letter_reason_counts)
        ),
    )


def _build_worker_queue_history_point(point: object) -> GQLWorkerQueueHistoryPoint:
    typed_point: Any = point
    return GQLWorkerQueueHistoryPoint(
        observed_at=str(typed_point.observed_at),
        total_jobs=int(typed_point.total_jobs),
        ready_jobs=int(typed_point.ready_jobs),
        deferred_jobs=int(typed_point.deferred_jobs),
        in_progress_jobs=int(typed_point.in_progress_jobs),
        retry_jobs=int(typed_point.retry_jobs),
        dead_letter_jobs=int(typed_point.dead_letter_jobs),
        oldest_ready_age_seconds=typed_point.oldest_ready_age_seconds,
        next_scheduled_in_seconds=typed_point.next_scheduled_in_seconds,
        alert_level=str(typed_point.alert_level),
        dead_letter_oldest_age_seconds=typed_point.dead_letter_oldest_age_seconds,
        dead_letter_reason_counts=cast(JSON, dict(typed_point.dead_letter_reason_counts)),
    )


def _build_metadata_reindex_status(
    info: Info[GraphQLContext, object],
    point: object | None,
) -> GQLMetadataReindexStatus:
    typed_point: Any | None = point
    return GQLMetadataReindexStatus(
        queue_name=_queue_name(info),
        schedule_offset_minutes=info.context.resources.settings.indexer.schedule_offset_minutes,
        has_history=typed_point is not None,
        observed_at="" if typed_point is None else str(typed_point.observed_at),
        processed=0 if typed_point is None else int(typed_point.processed),
        queued=0 if typed_point is None else int(typed_point.queued),
        reconciled=0 if typed_point is None else int(typed_point.reconciled),
        skipped_active=0 if typed_point is None else int(typed_point.skipped_active),
        failed=0 if typed_point is None else int(typed_point.failed),
        repair_attempted=0 if typed_point is None else int(typed_point.repair_attempted),
        repair_enriched=0 if typed_point is None else int(typed_point.repair_enriched),
        repair_skipped_no_tmdb_id=(
            0 if typed_point is None else int(typed_point.repair_skipped_no_tmdb_id)
        ),
        repair_failed=0 if typed_point is None else int(typed_point.repair_failed),
        repair_requeued=0 if typed_point is None else int(typed_point.repair_requeued),
        repair_skipped_active=0 if typed_point is None else int(typed_point.repair_skipped_active),
        outcome="ok" if typed_point is None else str(typed_point.outcome),
        run_failed=False if typed_point is None else bool(typed_point.run_failed),
        last_error=None if typed_point is None else typed_point.last_error,
    )


def _build_metadata_reindex_history_point(point: object) -> GQLMetadataReindexHistoryPoint:
    typed_point: Any = point
    return GQLMetadataReindexHistoryPoint(
        observed_at=str(typed_point.observed_at),
        processed=int(typed_point.processed),
        queued=int(typed_point.queued),
        reconciled=int(typed_point.reconciled),
        skipped_active=int(typed_point.skipped_active),
        failed=int(typed_point.failed),
        repair_attempted=int(typed_point.repair_attempted),
        repair_enriched=int(typed_point.repair_enriched),
        repair_skipped_no_tmdb_id=int(typed_point.repair_skipped_no_tmdb_id),
        repair_failed=int(typed_point.repair_failed),
        repair_requeued=int(typed_point.repair_requeued),
        repair_skipped_active=int(typed_point.repair_skipped_active),
        outcome=str(typed_point.outcome),
        run_failed=bool(typed_point.run_failed),
        last_error=typed_point.last_error,
    )


def _build_playback_attachment(attachment: object) -> GQLPlaybackAttachment:
    typed_attachment: Any = attachment
    return GQLPlaybackAttachment(
        id=str(typed_attachment.id),
        kind=str(typed_attachment.kind),
        locator=str(typed_attachment.locator),
        source_key=typed_attachment.source_key,
        provider=typed_attachment.provider,
        provider_download_id=typed_attachment.provider_download_id,
        provider_file_id=typed_attachment.provider_file_id,
        provider_file_path=typed_attachment.provider_file_path,
        original_filename=typed_attachment.original_filename,
        file_size=typed_attachment.file_size,
        local_path=typed_attachment.local_path,
        restricted_url=typed_attachment.restricted_url,
        unrestricted_url=typed_attachment.unrestricted_url,
        is_preferred=bool(typed_attachment.is_preferred),
        preference_rank=int(typed_attachment.preference_rank),
        refresh_state=str(typed_attachment.refresh_state),
        expires_at=typed_attachment.expires_at,
        last_refreshed_at=typed_attachment.last_refreshed_at,
        last_refresh_error=typed_attachment.last_refresh_error,
    )


def _build_resolved_playback_attachment(attachment: object | None) -> GQLResolvedPlaybackAttachment | None:
    if attachment is None:
        return None
    typed_attachment: Any = attachment
    return GQLResolvedPlaybackAttachment(
        kind=str(typed_attachment.kind),
        locator=str(typed_attachment.locator),
        source_key=str(typed_attachment.source_key),
        provider=typed_attachment.provider,
        provider_download_id=typed_attachment.provider_download_id,
        provider_file_id=typed_attachment.provider_file_id,
        provider_file_path=typed_attachment.provider_file_path,
        original_filename=typed_attachment.original_filename,
        file_size=typed_attachment.file_size,
        local_path=typed_attachment.local_path,
        restricted_url=typed_attachment.restricted_url,
        unrestricted_url=typed_attachment.unrestricted_url,
    )


def _build_resolved_playback(snapshot: object | None) -> GQLResolvedPlayback | None:
    if snapshot is None:
        return None
    typed_snapshot: Any = snapshot
    return GQLResolvedPlayback(
        direct=_build_resolved_playback_attachment(typed_snapshot.direct),
        hls=_build_resolved_playback_attachment(typed_snapshot.hls),
        direct_ready=bool(typed_snapshot.direct_ready),
        hls_ready=bool(typed_snapshot.hls_ready),
        missing_local_file=bool(typed_snapshot.missing_local_file),
    )


def _build_active_stream_owner(owner: object | None) -> GQLActiveStreamOwner | None:
    if owner is None:
        return None
    typed_owner: Any = owner
    return GQLActiveStreamOwner(
        media_entry_index=int(typed_owner.media_entry_index),
        kind=str(typed_owner.kind),
        original_filename=typed_owner.original_filename,
        provider=typed_owner.provider,
        provider_download_id=typed_owner.provider_download_id,
        provider_file_id=typed_owner.provider_file_id,
        provider_file_path=typed_owner.provider_file_path,
    )


def _build_active_stream(active_stream: object | None) -> GQLActiveStream | None:
    if active_stream is None:
        return None
    typed_active_stream: Any = active_stream
    return GQLActiveStream(
        direct_ready=bool(typed_active_stream.direct_ready),
        hls_ready=bool(typed_active_stream.hls_ready),
        missing_local_file=bool(typed_active_stream.missing_local_file),
        direct_owner=_build_active_stream_owner(typed_active_stream.direct_owner),
        hls_owner=_build_active_stream_owner(typed_active_stream.hls_owner),
    )


def _build_media_entry(entry: object) -> GQLMediaEntry:
    typed_entry: Any = entry
    return GQLMediaEntry(
        entry_type=str(typed_entry.entry_type),
        kind=str(typed_entry.kind),
        original_filename=typed_entry.original_filename,
        url=typed_entry.url,
        local_path=typed_entry.local_path,
        download_url=typed_entry.download_url,
        unrestricted_url=typed_entry.unrestricted_url,
        provider=typed_entry.provider,
        provider_download_id=typed_entry.provider_download_id,
        provider_file_id=typed_entry.provider_file_id,
        provider_file_path=typed_entry.provider_file_path,
        size=typed_entry.size,
        created=typed_entry.created,
        modified=typed_entry.modified,
        refresh_state=str(typed_entry.refresh_state),
        expires_at=typed_entry.expires_at,
        last_refreshed_at=typed_entry.last_refreshed_at,
        last_refresh_error=typed_entry.last_refresh_error,
        active_for_direct=bool(typed_entry.active_for_direct),
        active_for_hls=bool(typed_entry.active_for_hls),
        is_active_stream=bool(typed_entry.is_active_stream),
    )


def _build_media_item_summary(record: MediaItemSummaryRecord) -> GQLMediaItem:
    specialization = record.specialization
    return GQLMediaItem(
        id=strawberry.ID(record.id),
        external_ref=record.external_ref or "",
        title=record.title,
        state=record.state or "Unknown",
        media_type=_summary_media_type(record),
        media_kind=_media_kind(record.type),
        tmdb_id=_to_optional_int(record.tmdb_id),
        tvdb_id=_to_optional_int(record.tvdb_id),
        imdb_id=(specialization.imdb_id if specialization is not None else None),
        parent_tmdb_id=(
            _to_optional_int(specialization.parent_ids.tmdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        parent_tvdb_id=(
            _to_optional_int(specialization.parent_ids.tvdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        show_title=(specialization.show_title if specialization is not None else None),
        season_number=(specialization.season_number if specialization is not None else None),
        episode_number=(specialization.episode_number if specialization is not None else None),
        poster_path=record.poster_path,
        aired_at=record.aired_at,
    )


def _build_library_stats(projection: StatsProjection) -> GQLLibraryStats:
    return GQLLibraryStats(
        total_items=projection.total_items,
        total_movies=projection.movies,
        total_shows=projection.shows,
        total_seasons=projection.seasons,
        total_episodes=projection.episodes,
        completed_items=projection.completed_items,
        incomplete_items=projection.incomplete_items,
        failed_items=projection.failed_items,
        state_breakdown=json.dumps(projection.states),
        activity=json.dumps(
            [{"date": date, "count": count} for date, count in projection.activity.items()]
        ),
    )


def _build_stream_candidate(stream: object) -> GQLStreamCandidate:
    stream_record = cast(StreamORM, stream)
    raw_title = stream_record.raw_title
    parsed_title = stream_record.parsed_title
    title_value = parsed_title.get("title") if isinstance(parsed_title, dict) else None
    return GQLStreamCandidate(
        id=strawberry.ID(stream_record.id),
        raw_title=raw_title,
        parsed_title=title_value if isinstance(title_value, str) else None,
        resolution=stream_record.resolution,
        rank_score=stream_record.rank,
        lev_ratio=stream_record.lev_ratio,
        selected=stream_record.selected,
        passed=(stream_record.rank > 0) if stream_record.lev_ratio is not None else None,
        rejection_reason=None,
    )


def _build_recovery_plan(plan: RecoveryPlanRecord) -> GQLRecoveryPlan:
    return GQLRecoveryPlan(
        mechanism=GQLRecoveryMechanism(plan.mechanism.value),
        target_stage=GQLRecoveryTargetStage(plan.target_stage.value),
        reason=plan.reason,
        next_retry_at=plan.next_retry_at,
        recovery_attempt_count=plan.recovery_attempt_count,
        is_in_cooldown=plan.is_in_cooldown,
    )


async def _build_media_item_detail(
    info: Info[GraphQLContext, object],
    record: MediaItemSummaryRecord,
) -> GQLMediaItemDetail:
    stream_candidates = [
        _build_stream_candidate(stream)
        for stream in await info.context.media_service.get_stream_candidates(
            media_item_id=record.id
        )
    ]
    recovery_plan = await info.context.media_service.get_recovery_plan(media_item_id=record.id)
    selected_stream = next(
        (candidate for candidate in stream_candidates if candidate.selected), None
    )
    specialization = record.specialization
    return GQLMediaItemDetail(
        id=strawberry.ID(record.id),
        title=record.title,
        state=record.state or "Unknown",
        item_type=record.type,
        media_type=_summary_media_type(record),
        media_kind=_media_kind(record.type),
        tmdb_id=_to_optional_int(record.tmdb_id),
        tvdb_id=_to_optional_int(record.tvdb_id),
        imdb_id=(specialization.imdb_id if specialization is not None else None),
        parent_tmdb_id=(
            _to_optional_int(specialization.parent_ids.tmdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        parent_tvdb_id=(
            _to_optional_int(specialization.parent_ids.tvdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        show_title=(specialization.show_title if specialization is not None else None),
        season_number=(specialization.season_number if specialization is not None else None),
        episode_number=(specialization.episode_number if specialization is not None else None),
        created_at=record.created_at or "",
        updated_at=record.updated_at or "",
        stream_candidates=stream_candidates,
        selected_stream=selected_stream,
        recovery_plan=(
            _build_recovery_plan(recovery_plan)
            if recovery_plan is not None
            else GQLRecoveryPlan(
                mechanism=GQLRecoveryMechanism.NONE,
                target_stage=GQLRecoveryTargetStage.NONE,
                reason="state_not_automatically_recoverable",
                next_retry_at=None,
                recovery_attempt_count=0,
                is_in_cooldown=False,
            )
        ),
        playback_attachments=[
            _build_playback_attachment(attachment) for attachment in record.playback_attachments or []
        ],
        resolved_playback=_build_resolved_playback(record.resolved_playback),
        active_stream=_build_active_stream(record.active_stream),
        media_entries=[_build_media_entry(entry) for entry in record.media_entries or []],
    )


def _serialize_trigger_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_graphql_datetime(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _build_direct_playback_refresh_trigger_result(
    result: AppScopedDirectPlaybackRefreshTriggerResult,
) -> GQLPlaybackRefreshTriggerResult:
    control_plane_result = result.control_plane_result
    scheduling_result = (
        control_plane_result.scheduling_result if control_plane_result is not None else None
    )
    scheduled_request = None
    if control_plane_result is not None and control_plane_result.scheduled_request is not None:
        scheduled_request = control_plane_result.scheduled_request
    elif scheduling_result is not None and scheduling_result.scheduled_request is not None:
        scheduled_request = scheduling_result.scheduled_request

    execution = scheduling_result.execution if scheduling_result is not None else None
    media_entry_execution = execution.media_entry_execution if execution is not None else None
    attachment_execution = execution.attachment_execution if execution is not None else None
    execution_ok = None
    execution_refresh_state = None
    execution_locator = None
    execution_error = None
    if media_entry_execution is not None:
        execution_ok = media_entry_execution.ok
        execution_refresh_state = media_entry_execution.refresh_state
        execution_locator = media_entry_execution.locator
        execution_error = media_entry_execution.error
    elif attachment_execution is not None:
        execution_ok = attachment_execution.ok
        execution_refresh_state = attachment_execution.refresh_state
        execution_locator = attachment_execution.locator
        execution_error = attachment_execution.error

    return GQLPlaybackRefreshTriggerResult(
        item_id=result.item_identifier,
        outcome=result.outcome,
        controller_attached=result.controller_attached,
        control_plane_outcome=(control_plane_result.outcome if control_plane_result is not None else None),
        refresh_outcome=(scheduling_result.outcome if scheduling_result is not None else None),
        execution_ok=execution_ok,
        execution_refresh_state=execution_refresh_state,
        execution_locator=execution_locator,
        execution_error=execution_error,
        retry_after_seconds=(
            scheduling_result.retry_after_seconds if scheduling_result is not None else None
        ),
        deferred_reason=(execution.deferred_reason if execution is not None else None),
        scheduled_requested_at=(
            _serialize_trigger_datetime(scheduled_request.requested_at)
            if scheduled_request is not None
            else None
        ),
        scheduled_not_before=(
            _serialize_trigger_datetime(scheduled_request.not_before)
            if scheduled_request is not None
            else None
        ),
    )


def _build_selected_hls_refresh_trigger_result(
    result: (
        AppScopedHlsFailedLeaseRefreshTriggerResult
        | AppScopedHlsRestrictedFallbackRefreshTriggerResult
    ),
) -> GQLPlaybackRefreshTriggerResult:
    control_plane_result = result.control_plane_result
    refresh_result = control_plane_result.refresh_result if control_plane_result is not None else None
    execution = refresh_result.execution if refresh_result is not None else None
    return GQLPlaybackRefreshTriggerResult(
        item_id=result.item_identifier,
        outcome=result.outcome,
        controller_attached=result.controller_attached,
        control_plane_outcome=(control_plane_result.outcome if control_plane_result is not None else None),
        refresh_outcome=(refresh_result.outcome if refresh_result is not None else None),
        execution_ok=(execution.ok if execution is not None else None),
        execution_refresh_state=(execution.refresh_state if execution is not None else None),
        execution_locator=(execution.locator if execution is not None else None),
        execution_error=(execution.error if execution is not None else None),
        retry_after_seconds=(refresh_result.retry_after_seconds if refresh_result is not None else None),
        deferred_reason=(refresh_result.deferred_reason if refresh_result is not None else None),
        scheduled_requested_at=None,
        scheduled_not_before=None,
    )


def _build_persisted_media_entry_control_result(
    result: PersistedMediaEntryControlMutationResult,
) -> GQLPersistMediaEntryControlResult:
    typed_item: Any = result.item
    roles = {
        str(active_stream.role)
        for active_stream in typed_item.active_streams
        if str(active_stream.media_entry_id) == result.media_entry.id
    }
    media_entry_projection = SimpleNamespace(
        entry_type=result.media_entry.entry_type,
        kind=result.media_entry.kind,
        original_filename=result.media_entry.original_filename,
        url=(
            result.media_entry.unrestricted_url
            or result.media_entry.download_url
            or result.media_entry.local_path
        ),
        local_path=result.media_entry.local_path,
        download_url=result.media_entry.download_url,
        unrestricted_url=result.media_entry.unrestricted_url,
        provider=result.media_entry.provider,
        provider_download_id=result.media_entry.provider_download_id,
        provider_file_id=result.media_entry.provider_file_id,
        provider_file_path=result.media_entry.provider_file_path,
        size=result.media_entry.size_bytes,
        created=result.media_entry.created_at.isoformat()
        if result.media_entry.created_at is not None
        else None,
        modified=result.media_entry.updated_at.isoformat()
        if result.media_entry.updated_at is not None
        else None,
        refresh_state=result.media_entry.refresh_state,
        expires_at=_serialize_trigger_datetime(result.media_entry.expires_at),
        last_refreshed_at=_serialize_trigger_datetime(result.media_entry.last_refreshed_at),
        last_refresh_error=result.media_entry.last_refresh_error,
        active_for_direct="direct" in roles,
        active_for_hls="hls" in roles,
        is_active_stream=bool(roles),
    )
    return GQLPersistMediaEntryControlResult(
        item_id=result.item.id,
        media_entry_id=result.media_entry.id,
        success=True,
        error=None,
        applied_role=result.applied_role,
        media_entry=_build_media_entry(media_entry_projection),
    )


def _build_persisted_playback_attachment_control_result(
    result: PersistedPlaybackAttachmentControlMutationResult,
) -> GQLPersistPlaybackAttachmentControlResult:
    attachment_projection = SimpleNamespace(
        id=result.attachment.id,
        kind=result.attachment.kind,
        locator=result.attachment.locator,
        source_key=result.attachment.source_key,
        provider=result.attachment.provider,
        provider_download_id=result.attachment.provider_download_id,
        provider_file_id=result.attachment.provider_file_id,
        provider_file_path=result.attachment.provider_file_path,
        original_filename=result.attachment.original_filename,
        file_size=result.attachment.file_size,
        local_path=result.attachment.local_path,
        restricted_url=result.attachment.restricted_url,
        unrestricted_url=result.attachment.unrestricted_url,
        is_preferred=result.attachment.is_preferred,
        preference_rank=result.attachment.preference_rank,
        refresh_state=result.attachment.refresh_state,
        expires_at=_serialize_trigger_datetime(result.attachment.expires_at),
        last_refreshed_at=_serialize_trigger_datetime(result.attachment.last_refreshed_at),
        last_refresh_error=result.attachment.last_refresh_error,
    )
    return GQLPersistPlaybackAttachmentControlResult(
        item_id=result.item.id,
        attachment_id=result.attachment.id,
        success=True,
        error=None,
        attachment=_build_playback_attachment(attachment_projection),
        linked_media_entries=[_build_media_entry(entry) for entry in result.linked_media_entries],
    )


@strawberry.type
class CoreQueryResolver:
    """Base query resolvers available without plugin registration."""

    @strawberry.field(description="Service health for GraphQL clients")
    async def health(self, info: Info[GraphQLContext, object]) -> GQLHealthCheck:
        return GQLHealthCheck(
            service=info.context.resources.settings.service_name,
            status="healthy",
        )

    @strawberry.field(description="List media items from persisted state")
    async def items(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 100,
    ) -> list[GQLMediaItem]:
        if limit < 1 or limit > 500:
            raise ValueError("limit must be within range [1, 500]")

        page = await info.context.media_service.search_items(limit=limit, page=1, extended=False)
        return [_build_media_item_summary(record) for record in page.items]

    @strawberry.field(description="Fetch one media item by internal identifier")
    async def item(
        self,
        info: Info[GraphQLContext, object],
        item_id: strawberry.ID,
    ) -> GQLMediaItem | None:
        record = await info.context.media_service.get_item(str(item_id))
        if record is None:
            return None

        return GQLMediaItem(
            id=strawberry.ID(record.id),
            external_ref=record.external_ref,
            title=record.title,
            state=record.state.value,
            media_type=_record_media_type(record),
            media_kind=_media_kind(_record_media_type(record)),
        )

    @strawberry.field(
        description="Intentional GraphQL calendar entries unconstrained by REST compatibility shape"
    )
    async def calendar_entries(
        self,
        info: Info[GraphQLContext, object],
        days_ahead: int = 30,
        days_behind: int = 7,
    ) -> list[GQLCalendarEntry]:
        now = datetime.now(UTC)
        start_date = (now - timedelta(days=days_behind)).isoformat()
        end_date = (now + timedelta(days=days_ahead)).isoformat()
        entries = await info.context.media_service.get_calendar(
            start_date=start_date,
            end_date=end_date,
        )
        return [_build_calendar_entry(entry) for entry in entries]

    @strawberry.field(description="Stat one mounted VFS catalog node by normalized catalog path")
    async def vfs_catalog_entry(
        self,
        info: Info[GraphQLContext, object],
        path: str,
        generation_id: str | None = None,
    ) -> GQLVfsCatalogEntry | None:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return None
        entry = _find_vfs_entry(snapshot, path)
        if entry is None:
            return None
        return _build_vfs_catalog_entry(entry)

    @strawberry.field(
        description="List one mounted VFS directory directly from the current or requested catalog generation"
    )
    async def vfs_directory(
        self,
        info: Info[GraphQLContext, object],
        path: str = "/",
        generation_id: str | None = None,
    ) -> GQLVfsDirectoryListing | None:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return None
        entry = _find_vfs_entry(snapshot, path)
        if entry is None or entry.kind != "directory":
            return None
        children = sorted(
            (candidate for candidate in snapshot.entries if candidate.parent_entry_id == entry.entry_id),
            key=lambda candidate: (candidate.kind != "directory", candidate.path),
        )
        directories = [
            _build_vfs_catalog_entry(candidate)
            for candidate in children
            if candidate.kind == "directory"
        ]
        files = [
            _build_vfs_catalog_entry(candidate) for candidate in children if candidate.kind == "file"
        ]
        return GQLVfsDirectoryListing(
            generation_id=snapshot.generation_id,
            path=entry.path,
            entry=_build_vfs_catalog_entry(entry),
            stats=_build_vfs_catalog_stats(snapshot),
            directories=directories,
            files=files,
        )

    @strawberry.field(
        description="Return one mounted VFS snapshot summary from the shared catalog supplier"
    )
    async def vfs_snapshot(
        self,
        info: Info[GraphQLContext, object],
        generation_id: str | None = None,
    ) -> GQLVfsSnapshot | None:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return None
        return _build_vfs_snapshot(snapshot)

    @strawberry.field(
        description="Return aggregate VFS blocking/query/provider posture for the current or requested snapshot"
    )
    async def vfs_catalog_rollup(
        self,
        info: Info[GraphQLContext, object],
        generation_id: str | None = None,
    ) -> GQLVfsCatalogRollup | None:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return None
        return _build_vfs_catalog_rollup(summarize_vfs_catalog_snapshot(snapshot))

    @strawberry.field(
        description="List blocked mounted catalog items from the current or requested snapshot"
    )
    async def vfs_blocked_items(
        self,
        info: Info[GraphQLContext, object],
        generation_id: str | None = None,
    ) -> list[GQLVfsBlockedItem]:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return []
        return [_build_vfs_blocked_item(item) for item in snapshot.blocked_items]

    @strawberry.field(description="Current runtime lifecycle graph and bounded transition history")
    async def runtime_lifecycle(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLRuntimeLifecycleSnapshot:
        return _build_runtime_lifecycle_snapshot(info.context.resources.runtime_lifecycle.snapshot())

    @strawberry.field(description="Current operator queue snapshot from the shared Redis-backed reader")
    async def worker_queue_status(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLWorkerQueueStatus:
        snapshot = await QueueStatusReader(
            _queue_redis(info),
            queue_name=_queue_name(info),
        ).snapshot()
        return _build_worker_queue_status(info, snapshot)

    @strawberry.field(description="Bounded operator queue history with optional replay-oriented filters")
    async def worker_queue_history(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
        alert_level: str | None = None,
        min_dead_letter_jobs: int = 0,
        reason_code: str | None = None,
    ) -> list[GQLWorkerQueueHistoryPoint]:
        bounded_limit = max(1, min(limit, 100))
        bounded_min_dead_letter_jobs = max(0, min(min_dead_letter_jobs, 10_000))
        if alert_level is not None and alert_level not in {"ok", "warning", "critical"}:
            raise ValueError("alert_level must be one of: ok, warning, critical")
        history = await QueueStatusReader(
            _queue_redis(info),
            queue_name=_queue_name(info),
        ).history(limit=bounded_limit)
        points = [_build_worker_queue_history_point(item) for item in history]
        if alert_level is not None:
            points = [item for item in points if item.alert_level == alert_level]
        if bounded_min_dead_letter_jobs > 0:
            points = [
                item
                for item in points
                if item.dead_letter_jobs >= bounded_min_dead_letter_jobs
            ]
        if reason_code is not None:
            points = [
                item
                for item in points
                if cast(dict[str, int], item.dead_letter_reason_counts).get(reason_code, 0) > 0
            ]
        return points

    @strawberry.field(description="Latest metadata reindex/reconciliation run summary")
    async def worker_metadata_reindex_status(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLMetadataReindexStatus:
        latest = await MetadataReindexStatusStore(
            _queue_redis(info),
            queue_name=_queue_name(info),
        ).latest()
        return _build_metadata_reindex_status(info, latest)

    @strawberry.field(description="Bounded metadata reindex/reconciliation history")
    async def worker_metadata_reindex_history(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
    ) -> list[GQLMetadataReindexHistoryPoint]:
        bounded_limit = max(1, min(limit, 100))
        history = await MetadataReindexStatusStore(
            _queue_redis(info),
            queue_name=_queue_name(info),
        ).history(limit=bounded_limit)
        return [_build_metadata_reindex_history_point(item) for item in history]

    @strawberry.field(description="Intentional GraphQL library stats projection")
    async def library_stats(self, info: Info[GraphQLContext, object]) -> GQLLibraryStats:
        projection = await info.context.media_service.get_stats()
        return _build_library_stats(projection)

    @strawberry.field(description="Fetch one rich media item detail by internal identifier")
    async def media_item(
        self,
        info: Info[GraphQLContext, object],
        id: strawberry.ID,
    ) -> GQLMediaItemDetail | None:
        record = await info.context.media_service.get_item_detail(
            str(id),
            media_type="item",
            extended=True,
        )
        if record is None:
            return None
        return await _build_media_item_detail(info, record)

    @strawberry.field(description="List rich media item details with optional state filtering")
    async def media_items(
        self,
        info: Info[GraphQLContext, object],
        state: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[GQLMediaItemDetail]:
        bounded_limit = max(1, min(limit, 100))
        bounded_offset = max(0, offset)
        page = await info.context.media_service.search_items(
            limit=min(100, bounded_limit + bounded_offset),
            page=1,
            states=[state] if state is not None else None,
            extended=True,
        )
        selected_records = page.items[bounded_offset : bounded_offset + bounded_limit]
        return [await _build_media_item_detail(info, record) for record in selected_records]


@strawberry.type
class CoreMutationResolver:
    """Base mutation resolvers available without plugin registration."""

    @strawberry.mutation(description="Create or get a media request by external reference")
    async def request_item(
        self,
        info: Info[GraphQLContext, object],
        input: RequestItemInput,
    ) -> RequestItemResult:
        result = await info.context.media_service.request_item_with_enrichment(
            input.external_ref,
            media_type=input.media_type,
            requested_seasons=input.requested_seasons,
        )
        return RequestItemResult(
            item_id=strawberry.ID(result.item.id),
            enrichment_source=result.enrichment.source,
            has_poster=result.enrichment.has_poster,
            has_imdb_id=result.enrichment.has_imdb_id,
            warnings=list(result.enrichment.warnings),
        )

    @strawberry.mutation(description="Apply a lifecycle transition to a media item")
    async def item_action(
        self,
        info: Info[GraphQLContext, object],
        input: ItemActionInput,
    ) -> ItemStateChangedEvent:
        action = input.action.lower()
        if action == "retry":
            result = await info.context.media_service.retry_items([input.item_id])
            to_state = "requested"
        elif action == "reset":
            result = await info.context.media_service.reset_items([input.item_id])
            to_state = "requested"
        elif action == "remove":
            result = await info.context.media_service.remove_items([input.item_id])
            to_state = "removed"
        else:
            raise ValueError(f"unknown action: {input.action}")

        _ensure_item_action_matched(result, input.item_id)
        return ItemStateChangedEvent(
            item_id=input.item_id,
            from_state=None,
            to_state=to_state,
            timestamp=datetime.now(UTC).isoformat(),
        )

    @strawberry.mutation(description="Retry one item immediately and enqueue scrape")
    async def retry_item(
        self,
        item_id: strawberry.ID,
        info: Info[GraphQLContext, object],
    ) -> RetryItemResult:
        try:
            async with info.context.resources.db.session() as session:
                item = await info.context.media_service.retry_item(
                    str(item_id),
                    session,
                    info.context.resources.arq_redis,
                )
        except (ArqNotEnabledError, ItemNotFoundError) as exc:
            return RetryItemResult(
                item_id=str(item_id),
                success=False,
                error=str(exc),
                new_state=None,
            )

        return RetryItemResult(
            item_id=item.id,
            success=True,
            error=None,
            new_state=item.state,
        )

    @strawberry.mutation(description="Reset one item, blacklist current streams, and enqueue scrape")
    async def reset_item(
        self,
        item_id: strawberry.ID,
        info: Info[GraphQLContext, object],
    ) -> ResetItemResult:
        try:
            async with info.context.resources.db.session() as session:
                item = await info.context.media_service.reset_item(
                    str(item_id),
                    session,
                    info.context.resources.arq_redis,
                )
        except (ArqNotEnabledError, ItemNotFoundError) as exc:
            return ResetItemResult(
                item_id=str(item_id),
                success=False,
                error=str(exc),
                new_state=None,
            )

        return ResetItemResult(
            item_id=item.id,
            success=True,
            error=None,
            new_state=item.state,
        )

    @strawberry.mutation(
        description="Trigger direct-play refresh through the shared playback control plane"
    )
    async def trigger_direct_playback_refresh(
        self,
        info: Info[GraphQLContext, object],
        item_id: strawberry.ID,
    ) -> GQLPlaybackRefreshTriggerResult:
        result = await trigger_direct_playback_refresh_from_resources(
            info.context.resources,
            str(item_id),
        )
        return _build_direct_playback_refresh_trigger_result(result)

    @strawberry.mutation(
        description="Trigger selected-HLS failed-lease refresh through the shared playback control plane"
    )
    async def trigger_hls_failed_lease_refresh(
        self,
        info: Info[GraphQLContext, object],
        item_id: strawberry.ID,
    ) -> GQLPlaybackRefreshTriggerResult:
        result = await trigger_hls_failed_lease_refresh_from_resources(
            info.context.resources,
            str(item_id),
        )
        return _build_selected_hls_refresh_trigger_result(result)

    @strawberry.mutation(
        description="Trigger selected-HLS restricted-fallback refresh through the shared playback control plane"
    )
    async def trigger_hls_restricted_fallback_refresh(
        self,
        info: Info[GraphQLContext, object],
        item_id: strawberry.ID,
    ) -> GQLPlaybackRefreshTriggerResult:
        result = await trigger_hls_restricted_fallback_refresh_from_resources(
            info.context.resources,
            str(item_id),
        )
        return _build_selected_hls_refresh_trigger_result(result)

    @strawberry.mutation(
        description="Mark the selected HLS media entry stale so the shared refresh loop can pick it up"
    )
    async def mark_selected_hls_media_entry_stale(
        self,
        info: Info[GraphQLContext, object],
        item_id: strawberry.ID,
    ) -> GQLMarkSelectedHlsMediaEntryStaleResult:
        playback_service = info.context.resources.playback_service
        if playback_service is None:
            return GQLMarkSelectedHlsMediaEntryStaleResult(
                item_id=str(item_id),
                success=False,
                error="playback_service_unavailable",
            )

        success = await playback_service.mark_selected_hls_media_entry_stale(str(item_id))
        return GQLMarkSelectedHlsMediaEntryStaleResult(
            item_id=str(item_id),
            success=success,
            error=None if success else "selected_hls_media_entry_not_marked",
        )

    @strawberry.mutation(
        description="Persist bounded media-entry URL/state changes and optional active-role rebinding through the shared playback service"
    )
    async def persist_media_entry_control_state(
        self,
        info: Info[GraphQLContext, object],
        input: PersistMediaEntryControlInput,
    ) -> GQLPersistMediaEntryControlResult:
        playback_service = info.context.resources.playback_service
        if playback_service is None:
            return GQLPersistMediaEntryControlResult(
                item_id=str(input.item_id),
                media_entry_id=str(input.media_entry_id),
                success=False,
                error="playback_service_unavailable",
                applied_role=None,
                media_entry=None,
            )

        if (
            input.active_role is None
            and input.local_path is None
            and input.download_url is None
            and input.unrestricted_url is None
            and input.refresh_state is None
            and input.last_refresh_error is None
            and input.expires_at is None
        ):
            return GQLPersistMediaEntryControlResult(
                item_id=str(input.item_id),
                media_entry_id=str(input.media_entry_id),
                success=False,
                error="no_changes_requested",
                applied_role=None,
                media_entry=None,
            )

        try:
            expires_at = (
                _parse_graphql_datetime(input.expires_at)
                if input.expires_at is not None
                else None
            )
        except ValueError:
            return GQLPersistMediaEntryControlResult(
                item_id=str(input.item_id),
                media_entry_id=str(input.media_entry_id),
                success=False,
                error="invalid_expires_at",
                applied_role=None,
                media_entry=None,
            )

        try:
            result = await playback_service.persist_media_entry_control_state(
                str(input.item_id),
                str(input.media_entry_id),
                active_role=(
                    input.active_role.value
                    if input.active_role is not None
                    else None
                ),
                local_path=input.local_path,
                download_url=input.download_url,
                unrestricted_url=input.unrestricted_url,
                refresh_state=input.refresh_state,
                last_refresh_error=input.last_refresh_error,
                expires_at=expires_at,
            )
        except ValueError as exc:
            return GQLPersistMediaEntryControlResult(
                item_id=str(input.item_id),
                media_entry_id=str(input.media_entry_id),
                success=False,
                error=str(exc),
                applied_role=None,
                media_entry=None,
            )

        if result is None:
            return GQLPersistMediaEntryControlResult(
                item_id=str(input.item_id),
                media_entry_id=str(input.media_entry_id),
                success=False,
                error="media_entry_not_found",
                applied_role=None,
                media_entry=None,
            )

        return _build_persisted_media_entry_control_result(result)

    @strawberry.mutation(
        description="Persist bounded playback-attachment URL/state changes through the shared playback service and sync linked media entries"
    )
    async def persist_playback_attachment_control_state(
        self,
        info: Info[GraphQLContext, object],
        input: PersistPlaybackAttachmentControlInput,
    ) -> GQLPersistPlaybackAttachmentControlResult:
        playback_service = info.context.resources.playback_service
        if playback_service is None:
            return GQLPersistPlaybackAttachmentControlResult(
                item_id=str(input.item_id),
                attachment_id=str(input.attachment_id),
                success=False,
                error="playback_service_unavailable",
                attachment=None,
                linked_media_entries=[],
            )

        if (
            input.locator is None
            and input.local_path is None
            and input.restricted_url is None
            and input.unrestricted_url is None
            and input.refresh_state is None
            and input.last_refresh_error is None
            and input.expires_at is None
        ):
            return GQLPersistPlaybackAttachmentControlResult(
                item_id=str(input.item_id),
                attachment_id=str(input.attachment_id),
                success=False,
                error="no_changes_requested",
                attachment=None,
                linked_media_entries=[],
            )

        try:
            expires_at = (
                _parse_graphql_datetime(input.expires_at)
                if input.expires_at is not None
                else None
            )
        except ValueError:
            return GQLPersistPlaybackAttachmentControlResult(
                item_id=str(input.item_id),
                attachment_id=str(input.attachment_id),
                success=False,
                error="invalid_expires_at",
                attachment=None,
                linked_media_entries=[],
            )

        try:
            result = await playback_service.persist_playback_attachment_control_state(
                str(input.item_id),
                str(input.attachment_id),
                locator=input.locator,
                local_path=input.local_path,
                restricted_url=input.restricted_url,
                unrestricted_url=input.unrestricted_url,
                refresh_state=input.refresh_state,
                last_refresh_error=input.last_refresh_error,
                expires_at=expires_at,
            )
        except ValueError as exc:
            return GQLPersistPlaybackAttachmentControlResult(
                item_id=str(input.item_id),
                attachment_id=str(input.attachment_id),
                success=False,
                error=str(exc),
                attachment=None,
                linked_media_entries=[],
            )

        if result is None:
            return GQLPersistPlaybackAttachmentControlResult(
                item_id=str(input.item_id),
                attachment_id=str(input.attachment_id),
                success=False,
                error="playback_attachment_not_found",
                attachment=None,
                linked_media_entries=[],
            )

        return _build_persisted_playback_attachment_control_result(result)

    @strawberry.mutation(description="Update one compatibility settings path")
    async def update_setting(
        self,
        info: Info[GraphQLContext, object],
        input: SettingsUpdateInput,
    ) -> bool:
        await info.context.settings_updater(input.path, input.value)
        return True


async def _resolve_first_item_record(
    info: Info[GraphQLContext, object], result: ItemActionResult
) -> MediaItemRecord:
    if not result.ids:
        raise ValueError("request did not return any item identifiers")

    record = await info.context.media_service.get_item(result.ids[0])
    if record is None:
        raise ValueError(f"requested item {result.ids[0]} could not be resolved")

    return record


def _ensure_item_action_matched(result: ItemActionResult, item_id: str) -> None:
    if item_id not in result.ids:
        raise ValueError(f"unknown item_id={item_id}")


@strawberry.type
class CoreSubscriptionResolver:
    """Base subscription resolvers available without plugin registration."""

    @strawberry.subscription(description="Scaffold item state stream")
    async def item_state_changed(
        self,
        info: Info[GraphQLContext, object],
    ) -> AsyncGenerator[GQLItemEvent, None]:
        yield GQLItemEvent(item_id=strawberry.ID("0"), state="idle", message="subscription-ready")
        async for envelope in info.context.resources.event_bus.subscribe("item.state.changed"):
            payload = envelope.payload
            yield GQLItemEvent(
                item_id=strawberry.ID(str(payload.get("item_id", "0"))),
                state=str(payload.get("state", "unknown")),
                message=str(payload.get("message", "")),
            )
