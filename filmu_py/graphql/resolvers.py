"""Composable GraphQL resolver classes for plugin-dfilmu schema growth."""

# mypy: disable-error-code=untyped-decorator

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from importlib.metadata import PackageNotFoundError, version
from pathlib import PurePosixPath
from typing import cast

import strawberry
from strawberry.types import Info

from filmu_py.db.models import StreamORM
from filmu_py.graphql.deps import GraphQLContext
from filmu_py.graphql.types import (
    GQLCalendarEntry,
    GQLFilmuSettings,
    GQLHealthCheck,
    GQLItemEvent,
    GQLLibraryStats,
    GQLMediaItem,
    GQLMediaItemDetail,
    GQLRecoveryMechanism,
    GQLRecoveryPlan,
    GQLRecoveryTargetStage,
    GQLStreamCandidate,
    GQLVfsCatalogEntry,
    GQLVfsCatalogStats,
    GQLVfsCorrelationKeys,
    GQLVfsDirectoryDetail,
    GQLVfsDirectoryListing,
    GQLVfsFileDetail,
    ItemActionInput,
    ItemStateChangedEvent,
    MediaKind,
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
from filmu_py.services.vfs_catalog import VfsCatalogEntry, VfsCatalogSnapshot


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
