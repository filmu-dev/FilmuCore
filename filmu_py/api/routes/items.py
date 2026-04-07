"""Item compatibility routes aligned with current library/detail frontend surfaces."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request, status

from filmu_py.api.deps import get_media_service, get_resources
from filmu_py.api.models import (
    ActiveStreamDetailResponse,
    ActiveStreamOwnerResponse,
    AddMediaItemPayload,
    IdListPayload,
    ItemActionResponse,
    ItemDetailResponse,
    ItemParentIdsResponse,
    ItemRequestSummaryResponse,
    ItemSeasonStateResponse,
    ItemsResponse,
    ItemSummaryResponse,
    MediaEntryDetailResponse,
    MessageResponse,
    PlaybackAttachmentDetailResponse,
    ResolvedPlaybackAttachmentResponse,
    ResolvedPlaybackSnapshotResponse,
    SubtitleEntryResponse,
)
from filmu_py.services.media import (
    ActiveStreamDetailRecord,
    ActiveStreamOwnerRecord,
    ArqNotEnabledError,
    ItemNotFoundError,
    ItemRequestSummaryRecord,
    MediaEntryDetailRecord,
    MediaItemSummaryRecord,
    MediaService,
    PlaybackAttachmentDetailRecord,
    ResolvedPlaybackAttachmentRecord,
    ResolvedPlaybackSnapshotRecord,
    SubtitleEntryDetailRecord,
)

router = APIRouter(prefix="/items", tags=["items"])
_EXTERNAL_REF_PREFIXES = ("tmdb:", "tvdb:", "imdb:")


def _is_external_ref_identifier(value: str) -> bool:
    return value.casefold().startswith(_EXTERNAL_REF_PREFIXES)


def _looks_like_uuid(value: str) -> bool:
    try:
        UUID(value)
    except ValueError:
        return False
    return True


def _resolve_detail_lookup(item_identifier: str, media_type: str) -> tuple[str, str]:
    """Resolve the safest lookup mode for item detail requests."""

    if _is_external_ref_identifier(item_identifier):
        return item_identifier, "item"
    if _looks_like_uuid(item_identifier):
        return item_identifier, "item"
    return item_identifier, media_type


def _to_item_summary(item: MediaItemSummaryRecord) -> ItemSummaryResponse:
    parent_ids = None
    if item.parent_ids is not None:
        parent_ids = ItemParentIdsResponse(
            tmdb_id=item.parent_ids.tmdb_id,
            tvdb_id=item.parent_ids.tvdb_id,
        )

    state = item.state.replace("_", " ").title() if item.state else None
    next_retry_at = datetime.fromisoformat(item.next_retry_at) if item.next_retry_at else None

    return ItemSummaryResponse(
        id=item.id,
        type=item.type,
        title=item.title,
        state=state,
        tmdb_id=item.tmdb_id,
        tvdb_id=item.tvdb_id,
        parent_ids=parent_ids,
        poster_path=item.poster_path,
        aired_at=item.aired_at,
        next_retry_at=next_retry_at,
        recovery_attempt_count=item.recovery_attempt_count,
        is_in_cooldown=item.is_in_cooldown,
    )


def _to_playback_attachment_detail(
    attachment: PlaybackAttachmentDetailRecord,
) -> PlaybackAttachmentDetailResponse:
    return PlaybackAttachmentDetailResponse(
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
        expires_at=attachment.expires_at,
        last_refreshed_at=attachment.last_refreshed_at,
        last_refresh_error=attachment.last_refresh_error,
    )


def _to_resolved_playback_attachment(
    attachment: ResolvedPlaybackAttachmentRecord,
) -> ResolvedPlaybackAttachmentResponse:
    return ResolvedPlaybackAttachmentResponse(
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


def _to_resolved_playback_snapshot(
    snapshot: ResolvedPlaybackSnapshotRecord,
) -> ResolvedPlaybackSnapshotResponse:
    direct = None
    if snapshot.direct is not None:
        direct = _to_resolved_playback_attachment(snapshot.direct)

    hls = None
    if snapshot.hls is not None:
        hls = _to_resolved_playback_attachment(snapshot.hls)

    return ResolvedPlaybackSnapshotResponse(
        direct=direct,
        hls=hls,
        direct_ready=snapshot.direct_ready,
        hls_ready=snapshot.hls_ready,
        missing_local_file=snapshot.missing_local_file,
    )


def _to_media_entry_detail(entry: MediaEntryDetailRecord) -> MediaEntryDetailResponse:
    return MediaEntryDetailResponse(
        entry_type=entry.entry_type,
        kind=entry.kind,
        original_filename=entry.original_filename,
        url=entry.url,
        local_path=entry.local_path,
        download_url=entry.download_url,
        unrestricted_url=entry.unrestricted_url,
        provider=entry.provider,
        provider_download_id=entry.provider_download_id,
        provider_file_id=entry.provider_file_id,
        provider_file_path=entry.provider_file_path,
        size=entry.size,
        created=entry.created,
        modified=entry.modified,
        refresh_state=entry.refresh_state,
        expires_at=entry.expires_at,
        last_refreshed_at=entry.last_refreshed_at,
        last_refresh_error=entry.last_refresh_error,
        active_for_direct=entry.active_for_direct,
        active_for_hls=entry.active_for_hls,
        is_active_stream=entry.is_active_stream,
    )


def _to_active_stream_owner(owner: ActiveStreamOwnerRecord) -> ActiveStreamOwnerResponse:
    return ActiveStreamOwnerResponse(
        media_entry_index=owner.media_entry_index,
        kind=owner.kind,
        original_filename=owner.original_filename,
        provider=owner.provider,
        provider_download_id=owner.provider_download_id,
        provider_file_id=owner.provider_file_id,
        provider_file_path=owner.provider_file_path,
    )


def _to_active_stream_detail(stream: ActiveStreamDetailRecord) -> ActiveStreamDetailResponse:
    direct_owner = None
    if stream.direct_owner is not None:
        direct_owner = _to_active_stream_owner(stream.direct_owner)

    hls_owner = None
    if stream.hls_owner is not None:
        hls_owner = _to_active_stream_owner(stream.hls_owner)

    return ActiveStreamDetailResponse(
        direct_ready=stream.direct_ready,
        hls_ready=stream.hls_ready,
        missing_local_file=stream.missing_local_file,
        direct_owner=direct_owner,
        hls_owner=hls_owner,
    )


def _to_item_request_summary(request: ItemRequestSummaryRecord) -> ItemRequestSummaryResponse:
    return ItemRequestSummaryResponse(
        is_partial=request.is_partial,
        requested_seasons=(
            None if request.requested_seasons is None else list(request.requested_seasons)
        ),
        requested_episodes=(
            None
            if request.requested_episodes is None
            else {
                str(season): list(episodes)
                for season, episodes in request.requested_episodes.items()
            }
        ),
    )


def _to_subtitle_entry(entry: SubtitleEntryDetailRecord) -> SubtitleEntryResponse:
    return SubtitleEntryResponse(
        id=entry.id,
        language=entry.language,
        format=entry.format,
        source=entry.source,
        url=entry.url,
        is_default=entry.is_default,
        is_forced=entry.is_forced,
    )


@router.get("", operation_id="items.get", response_model=ItemsResponse)
async def get_items(
    media_service: Annotated[MediaService, Depends(get_media_service)],
    limit: Annotated[int, Query(ge=1, le=100)] = 24,
    page: Annotated[int, Query(ge=1)] = 1,
    type: Annotated[list[str] | None, Query()] = None,
    states: Annotated[list[str] | None, Query()] = None,
    sort: Annotated[list[str] | None, Query()] = None,
    search: Annotated[str | None, Query()] = None,
    extended: Annotated[bool, Query()] = False,
) -> ItemsResponse:
    result = await media_service.search_items(
        limit=limit,
        page=page,
        item_types=type,
        states=states,
        sort=sort,
        search=search,
        extended=extended,
    )
    return ItemsResponse(
        success=result.success,
        items=[_to_item_summary(item) for item in result.items],
        page=result.page,
        limit=result.limit,
        total_items=result.total_items,
        total_pages=result.total_pages,
    )


@router.post("/add", operation_id="items.add_items", response_model=MessageResponse)
async def add_items(
    payload: Annotated[AddMediaItemPayload, Body(...)],
    media_service: Annotated[MediaService, Depends(get_media_service)],
    request: Request,
) -> MessageResponse:
    try:
        if payload.requested_seasons is not None or payload.requested_episodes is not None:
            result = await media_service.request_items_by_identifiers(
                media_type=payload.media_type,
                tmdb_ids=payload.tmdb_ids,
                tvdb_ids=payload.tvdb_ids,
                requested_seasons=payload.requested_seasons,
                requested_episodes=payload.requested_episodes,
            )
        else:
            result = await media_service.request_items_by_identifiers(
                media_type=payload.media_type,
                tmdb_ids=payload.tmdb_ids,
                tvdb_ids=payload.tvdb_ids,
            )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    # Immediately enqueue scrape jobs for newly created items (best-effort).
    resources = get_resources(request)
    arq_redis = resources.arq_redis
    if arq_redis is not None and result.ids:
        from filmu_py.workers.tasks import enqueue_scrape_item

        queue_name = resources.arq_queue_name
        for item_id in result.ids:
            try:
                await enqueue_scrape_item(arq_redis, item_id=item_id, queue_name=queue_name)
            except Exception:
                import logging

                logging.getLogger(__name__).warning(
                    "failed to enqueue scrape_item for %s", item_id, exc_info=True
                )

    return MessageResponse(message=result.message)


@router.get("/{id}", operation_id="items.get_item", response_model=ItemDetailResponse)
async def get_item(
    id: Annotated[str, Path(min_length=1)],
    media_service: Annotated[MediaService, Depends(get_media_service)],
    media_type: Annotated[str, Query(pattern="^(movie|tv|item)$")],
    extended: Annotated[bool, Query()] = False,
) -> ItemDetailResponse:
    lookup_identifier, lookup_media_type = _resolve_detail_lookup(id, media_type)
    result = await media_service.get_item_detail(
        lookup_identifier,
        media_type=lookup_media_type,
        extended=extended,
    )
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
    assert result is not None

    parent_ids = None
    if result.parent_ids is not None:
        parent_ids = ItemParentIdsResponse(
            tmdb_id=result.parent_ids.tmdb_id,
            tvdb_id=result.parent_ids.tvdb_id,
        )

    playback_attachments = None
    if result.playback_attachments is not None:
        playback_attachments = [
            _to_playback_attachment_detail(attachment) for attachment in result.playback_attachments
        ]

    resolved_playback = None
    if result.resolved_playback is not None:
        resolved_playback = _to_resolved_playback_snapshot(result.resolved_playback)

    active_stream = None
    if result.active_stream is not None:
        active_stream = _to_active_stream_detail(result.active_stream)

    media_entries = None
    if result.media_entries is not None:
        media_entries = [_to_media_entry_detail(entry) for entry in result.media_entries]

    subtitles = [_to_subtitle_entry(entry) for entry in result.subtitles]

    request_summary = None
    if result.request is not None:
        request_summary = _to_item_request_summary(result.request)

    state = result.state.replace("_", " ").title() if result.state else None
    next_retry_at = datetime.fromisoformat(result.next_retry_at) if result.next_retry_at else None

    seasons: list[ItemSeasonStateResponse] | None = None
    if result.type in {"show", "tv"} and result.covered_season_numbers:
        seasons = [
            ItemSeasonStateResponse(season_number=sn, state="Completed")
            for sn in result.covered_season_numbers
        ]

    return ItemDetailResponse(
        id=result.id,
        type=result.type,
        title=result.title,
        state=state,
        external_ref=result.external_ref,
        tmdb_id=result.tmdb_id,
        tvdb_id=result.tvdb_id,
        parent_ids=parent_ids,
        poster_path=result.poster_path,
        aired_at=result.aired_at,
        next_retry_at=next_retry_at,
        recovery_attempt_count=result.recovery_attempt_count,
        is_in_cooldown=result.is_in_cooldown,
        metadata=result.metadata,
        request=request_summary,
        playback_attachments=playback_attachments,
        resolved_playback=resolved_playback,
        active_stream=active_stream,
        media_entries=media_entries,
        subtitles=subtitles,
        seasons=seasons,
    )


@router.post("/reset", operation_id="items.reset", response_model=ItemActionResponse)
async def reset_items(
    payload: Annotated[IdListPayload, Body(...)],
    request: Request,
    media_service: Annotated[MediaService, Depends(get_media_service)],
) -> ItemActionResponse:
    resources = get_resources(request)
    matched_ids: list[str] = []
    try:
        async with resources.db.session() as session:
            for item_id in payload.ids:
                try:
                    item = await media_service.reset_item(item_id, session, resources.arq_redis)
                except ItemNotFoundError:
                    continue
                matched_ids.append(item.id)
    except ArqNotEnabledError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return ItemActionResponse(message="Items reset.", ids=matched_ids)


@router.post("/retry", operation_id="items.retry", response_model=ItemActionResponse)
async def retry_items(
    payload: Annotated[IdListPayload, Body(...)],
    request: Request,
    media_service: Annotated[MediaService, Depends(get_media_service)],
) -> ItemActionResponse:
    resources = get_resources(request)
    matched_ids: list[str] = []
    try:
        async with resources.db.session() as session:
            for item_id in payload.ids:
                try:
                    item = await media_service.retry_item(item_id, session, resources.arq_redis)
                except ItemNotFoundError:
                    continue
                matched_ids.append(item.id)
    except ArqNotEnabledError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return ItemActionResponse(message="Items retried.", ids=matched_ids)


@router.delete("/remove", operation_id="items.remove", response_model=ItemActionResponse)
async def remove_items(
    payload: Annotated[IdListPayload, Body(...)],
    media_service: Annotated[MediaService, Depends(get_media_service)],
) -> ItemActionResponse:
    result = await media_service.remove_items(payload.ids)
    return ItemActionResponse(message=result.message, ids=result.ids)
