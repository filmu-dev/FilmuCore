"""Scrape compatibility routes."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Annotated, Any, Literal, cast
from uuid import uuid4

from arq.connections import ArqRedis, RedisSettings, create_pool
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from filmu_py.api.deps import (
    get_auth_context,
    get_media_service,
    get_resources,
    require_permissions,
)
from filmu_py.api.models import MessageResponse, ScrapeAutoPayload, ScrapeSessionStateResponse
from filmu_py.services.media import ItemActionResult, MediaItemRecord, MediaService
from filmu_py.state.item import InvalidItemTransition, ItemEvent, ItemState

router = APIRouter(prefix="/scrape", tags=["scrape"])
logger = logging.getLogger(__name__)
_EXTERNAL_REF_PREFIXES = ("tmdb:", "tvdb:", "imdb:")


class AutoScrapeRequest(ScrapeAutoPayload):
    """Minimal auto-scrape request accepted by the current frontend.

    The upstream backend supports richer scrape-time behavior including ranking
    overrides, season targeting, and downloader/session orchestration. The current
    python compatibility slice accepts that request shape so the frontend can issue
    calls safely, while only activating the smallest currently supported behavior.
    """


class ScrapeItemQuery(BaseModel):
    """Compatibility query surface for the manual scrape endpoint."""

    item_id: str | int | None = None
    tmdb_id: str | None = None
    tvdb_id: str | None = None
    imdb_id: str | None = None
    media_type: Literal["movie", "tv"] | None = None
    custom_title: str | None = None
    custom_imdb_id: str | None = None
    ranking_overrides: str | None = None
    min_filesize_override: int | None = None
    max_filesize_override: int | None = None
    stream: bool = False


class ScrapeStreamPayload(BaseModel):
    """SSE event payload understood by the current manual scrape UI."""

    event: Literal["start", "progress", "streams", "complete", "error"]
    message: str
    service: str | None = None
    streams: dict[str, dict[str, Any]] | None = None
    total_streams: int | None = None
    services_completed: int | None = None
    total_services: int | None = None


class ScrapeItemResponse(BaseModel):
    """Compatibility JSON response for non-streaming scrape requests."""

    message: str
    streams: dict[str, dict[str, Any]]


class ParsedFileResponse(BaseModel):
    """Minimal parsed-file payload for manual scrape compatibility sessions."""

    file_id: int
    filename: str
    filesize: int
    download_url: str | None = None
    parsed_metadata: dict[str, Any]


class TorrentInfoResponse(BaseModel):
    """Minimal torrent info payload used by the frontend session flow."""

    id: str
    name: str
    infohash: str
    files: dict[str, dict[str, Any]] | None = None


class TorrentContainerResponse(BaseModel):
    """Minimal torrent container payload for session responses."""

    infohash: str
    files: list[dict[str, Any]] | None = None
    torrent_id: str | None = None
    torrent_info: TorrentInfoResponse | None = None


class StartSessionResponse(BaseModel):
    """Manual scrape session response consumed by the current frontend."""

    message: str
    session_id: str
    item_id: str
    media_type: Literal["movie", "tv"] | None = None
    tmdb_id: str | None = None
    tvdb_id: str | None = None
    imdb_id: str | None = None
    torrent_id: str
    torrent_info: TorrentInfoResponse
    containers: TorrentContainerResponse | None = None
    parsed_files: list[ParsedFileResponse] | None = None
    expires_at: str


class SelectFilesResponse(BaseModel):
    """Compatibility response for the `select_files` session action."""

    message: str
    download_type: Literal["cached", "uncached"]


class SessionActionRequest(BaseModel):
    """Unified scrape-session action body used by the frontend."""

    action: Literal["select_files", "update_attributes", "abort", "complete"]
    files: dict[str, dict[str, Any]] | None = None
    file_data: dict[str, Any] | None = None


@dataclass(slots=True)
class ScrapeSessionRecord:
    """Minimal scrape session record that now points to real item orchestration state."""

    session_id: str
    item_id: str
    title: str
    media_type: Literal["movie", "tv"] | None
    tmdb_id: str | None
    tvdb_id: str | None
    imdb_id: str | None
    torrent_id: str
    infohash: str
    parsed_files: list[ParsedFileResponse]
    expires_at: datetime
    selected_files: dict[str, dict[str, Any]] | None = None


def _scrape_item_identifier_from_request(request: AutoScrapeRequest) -> str | None:
    """Return the namespaced external identifier string encoded by the auto-scrape payload."""

    if request.item_id is not None:
        return str(request.item_id)
    if request.tmdb_id:
        return f"tmdb:{request.tmdb_id}"
    if request.tvdb_id:
        return f"tvdb:{request.tvdb_id}"
    if request.imdb_id:
        return f"imdb:{request.imdb_id}"
    return None


def _session_item_identifier(
    *,
    item_id: str | int | None,
    tmdb_id: str | None,
    tvdb_id: str | None,
    imdb_id: str | None,
) -> str | None:
    if item_id is not None:
        return str(item_id)
    if tmdb_id:
        return f"tmdb:{tmdb_id}"
    if tvdb_id:
        return f"tvdb:{tvdb_id}"
    if imdb_id:
        return f"imdb:{imdb_id}"
    return None


def _is_external_ref_identifier(value: str) -> bool:
    """Return whether one identifier uses a supported namespaced external-ref format."""

    return value.casefold().startswith(_EXTERNAL_REF_PREFIXES)


async def _resolve_existing_item(
    media_service: MediaService,
    item_identifier: str,
    *,
    media_type: str | None = None,
    tenant_id: str | None = None,
) -> MediaItemRecord | None:
    """Resolve one existing item by UUID or supported external identifier."""

    if _is_external_ref_identifier(item_identifier):
        get_by_external_id = getattr(media_service, "get_item_by_external_id", None)
        if callable(get_by_external_id):
            resolver = cast(Callable[..., Awaitable[Any]], get_by_external_id)
            item = cast(
                MediaItemRecord | None,
                await resolver(item_identifier, media_type=media_type, tenant_id=tenant_id),
            )
            if item is not None:
                return item

        detail = await media_service.get_item_detail(
            item_identifier,
            media_type="item",
            tenant_id=tenant_id,
        )
        if detail is not None:
            return await media_service.get_item(detail.id, tenant_id=tenant_id)
        return None

    return await media_service.get_item(item_identifier, tenant_id=tenant_id)


async def _request_missing_item_for_scrape(
    media_service: MediaService,
    *,
    item_identifier: str,
    scrape_request: AutoScrapeRequest,
    requested_seasons: list[int] | None,
    requested_episodes: dict[str, list[int]] | None,
    tenant_id: str,
) -> MediaItemRecord:
    """Create one missing item while reusing the add-items path when available."""

    request_items_by_identifiers = getattr(media_service, "request_items_by_identifiers", None)
    if callable(request_items_by_identifiers):
        requester = cast(Callable[..., Awaitable[Any]], request_items_by_identifiers)
        result = cast(
            ItemActionResult,
            await requester(
                media_type=scrape_request.media_type,
                identifiers=[item_identifier],
                requested_seasons=requested_seasons,
                requested_episodes=requested_episodes,
                tenant_id=tenant_id,
            ),
        )
        if result.ids:
            created_item = await media_service.get_item(result.ids[0], tenant_id=tenant_id)
            if created_item is not None:
                return created_item
        raise ValueError("Could not create item from provided identifier")

    return await media_service.request_item(
        external_ref=item_identifier,
        media_type=scrape_request.media_type,
        requested_seasons=requested_seasons,
        requested_episodes=requested_episodes,
        tenant_id=tenant_id,
    )


def _extract_requested_scope(
    scrape_request: AutoScrapeRequest,
) -> tuple[list[int] | None, dict[str, list[int]] | None]:
    """Return normalized optional partial-request scope from one scrape request."""

    requested_seasons = (
        list(scrape_request.requested_seasons)
        if scrape_request.requested_seasons is not None
        else list(scrape_request.season_numbers)
        if scrape_request.season_numbers is not None
        else None
    )
    requested_episodes = (
        {
            str(season): list(episodes)
            for season, episodes in scrape_request.requested_episodes.items()
        }
        if scrape_request.requested_episodes is not None
        else None
    )
    return requested_seasons, requested_episodes


def _ensure_scrape_eligible(item_state: ItemState) -> None:
    """Reject terminal or otherwise non-queueable states for scrape entrypoints."""

    if item_state in {ItemState.COMPLETED}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Item is not scrape-eligible from state={item_state.value}",
        )
    if item_state not in {
        ItemState.REQUESTED,
        ItemState.INDEXED,
        ItemState.PARTIALLY_COMPLETED,
        ItemState.ONGOING,
        # Allow DOWNLOADED so the frontend can re-trigger manually during the
        # window between debrid completing and finalize transitioning the item.
        ItemState.DOWNLOADED,
        # Allow FAILED so Request More can re-trigger a scrape for missing seasons
        # without the user needing to reset the item first.
        ItemState.FAILED,
    }:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Item is not scrape-eligible from state={item_state.value}",
        )


async def _resolve_scrape_queue_client(request: Request) -> ArqRedis:
    """Return an enqueue-capable ARQ client for scrape-route orchestration."""

    resources = get_resources(request)
    if resources.arq_redis is not None:
        return resources.arq_redis

    if hasattr(resources.redis, "enqueue_job"):
        return resources.redis  # type: ignore[return-value]

    resolved = await create_pool(
        RedisSettings.from_dsn(str(resources.settings.redis_url)),
        default_queue_name=resources.arq_queue_name,
    )
    resources.arq_redis = resolved
    return resolved


async def _queue_real_scrape(
    *,
    request: Request,
    media_service: MediaService,
    item_identifier: str,
    scrape_request: AutoScrapeRequest | None = None,
) -> tuple[str, str]:
    """Resolve an item, transition it into indexed state when needed, and enqueue scrape."""

    auth_context = get_auth_context(request)
    requested_seasons: list[int] | None = None
    requested_episodes: dict[str, list[int]] | None = None
    partial_scope_requested = False
    if scrape_request is not None:
        requested_seasons, requested_episodes = _extract_requested_scope(scrape_request)
        partial_scope_requested = (
            scrape_request.requested_seasons is not None
            or scrape_request.season_numbers is not None
            or scrape_request.requested_episodes is not None
        )

    try:
        item = await _resolve_existing_item(
            media_service,
            item_identifier,
            media_type=scrape_request.media_type if scrape_request is not None else None,
            tenant_id=auth_context.tenant_id,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    created_from_request = False
    if item is None and scrape_request is not None and _is_external_ref_identifier(item_identifier):
        try:
            item = await _request_missing_item_for_scrape(
                media_service,
                item_identifier=item_identifier,
                scrape_request=scrape_request,
                requested_seasons=requested_seasons,
                requested_episodes=requested_episodes,
                tenant_id=auth_context.tenant_id,
            )
            created_from_request = True
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Could not create item from provided identifier",
            ) from exc
        logger.info(
            "scrape_auto.item_created_from_new_request",
            extra={
                "item_id": item.id,
                "external_id": item.external_ref,
                "media_type": scrape_request.media_type,
                "season_count": len(requested_seasons or []),
            },
        )
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    if scrape_request is not None and partial_scope_requested and not created_from_request:
        item = await media_service.request_item(
            external_ref=item.external_ref,
            media_type=scrape_request.media_type,
            title=item.title,
            requested_seasons=requested_seasons,
            requested_episodes=requested_episodes,
            tenant_id=auth_context.tenant_id,
        )
        logger.info(
            "scrape_auto.partial_scope_upserted_for_existing_item",
            extra={
                "item_id": item.id,
                "external_id": item.external_ref,
                "media_type": scrape_request.media_type,
                "season_count": len(requested_seasons or []),
                "episode_scope_count": len(requested_episodes or {}),
            },
        )

    _ensure_scrape_eligible(item.state)

    if item.state == ItemState.FAILED:
        try:
            item = await media_service.transition_item(
                item.id,
                ItemEvent.RETRY,
                message="recovering from failed state for explicit scrape request",
            )
        except InvalidItemTransition as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from exc

    if item.state in {ItemState.REQUESTED, ItemState.PARTIALLY_COMPLETED, ItemState.ONGOING}:
        try:
            item = await media_service.transition_item(
                item.id,
                ItemEvent.INDEX,
                message="queued for scrape",
            )
        except InvalidItemTransition as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from exc

    resources = get_resources(request)
    queue_client = await _resolve_scrape_queue_client(request)
    enqueue_scrape_item = cast(
        Callable[..., Awaitable[bool]],
        import_module("filmu_py.workers.tasks").enqueue_scrape_item,
    )

    queued = await enqueue_scrape_item(
        queue_client,
        item_id=item.id,
        queue_name=resources.arq_queue_name,
    )
    if not queued:
        logger.info(
            "scrape_auto.job_already_queued — treating as acknowledged",
            extra={"item_id": item.id, "title": item.title},
        )

    return item.id, item.title


_SCRAPE_SESSIONS: dict[str, ScrapeSessionRecord] = {}


def _resolve_external_ref(request: AutoScrapeRequest) -> str:
    """Resolve a stable external reference from the supported request identifiers."""

    if request.tmdb_id:
        return f"tmdb:{request.tmdb_id}"
    if request.tvdb_id:
        return f"tvdb:{request.tvdb_id}"
    if request.imdb_id:
        return f"imdb:{request.imdb_id}"
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="One of item_id, tmdb_id, tvdb_id, or imdb_id is required",
    )


def _extract_infohash(magnet: str) -> str:
    """Extract a stable token from a magnet URI for compatibility session responses."""

    match = re.search(r"btih:([A-Za-z0-9]+)", magnet)
    if match is not None:
        return match.group(1)
    return uuid4().hex


def _cleanup_expired_sessions() -> None:
    """Drop expired in-memory scrape sessions."""

    now = datetime.now(UTC)
    expired_ids = [
        session_id for session_id, session in _SCRAPE_SESSIONS.items() if session.expires_at <= now
    ]
    for session_id in expired_ids:
        _SCRAPE_SESSIONS.pop(session_id, None)


def _require_session(session_id: str) -> ScrapeSessionRecord:
    """Return an active scrape session or raise a compatibility 404."""

    _cleanup_expired_sessions()
    session = _SCRAPE_SESSIONS.get(session_id)
    if session is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    return session


async def _resolve_scrape_target(
    query: ScrapeItemQuery,
    media_service: MediaService,
    *,
    tenant_id: str | None = None,
) -> tuple[str, str]:
    """Resolve a scrape request into a stable external reference and display title."""

    if query.item_id is not None:
        item_identifier = str(query.item_id)
        try:
            existing_item = await _resolve_existing_item(
                media_service,
                item_identifier,
                media_type=query.media_type,
                tenant_id=tenant_id,
            )
        except ValueError:
            existing_item = None
        if existing_item is not None:
            return existing_item.external_ref, query.custom_title or existing_item.title

    candidate = AutoScrapeRequest(
        media_type=query.media_type or "movie",
        item_id=query.item_id,
        tmdb_id=query.tmdb_id,
        tvdb_id=query.tvdb_id,
        imdb_id=query.custom_imdb_id or query.imdb_id,
        ranking_overrides=(
            json.loads(query.ranking_overrides) if query.ranking_overrides else None
        ),
        min_filesize_override=query.min_filesize_override,
        max_filesize_override=query.max_filesize_override,
    )
    external_ref = _resolve_external_ref(candidate)
    return external_ref, query.custom_title or external_ref


async def _resolve_session_target(
    query: ScrapeItemQuery,
    media_service: MediaService,
    *,
    tenant_id: str | None = None,
) -> tuple[str, str, str]:
    """Resolve a scrape-session target to `(item_id, external_ref, title)`.

    Existing items are reused; otherwise the target is created via the current media
    service compatibility path.
    """

    if query.item_id is not None:
        item_identifier = str(query.item_id)
        try:
            existing_item = await _resolve_existing_item(
                media_service,
                item_identifier,
                media_type=query.media_type,
                tenant_id=tenant_id,
            )
        except ValueError:
            existing_item = None
        if existing_item is not None:
            return (
                existing_item.id,
                existing_item.external_ref,
                query.custom_title or existing_item.title,
            )

    external_ref, title = await _resolve_scrape_target(
        query,
        media_service,
        tenant_id=tenant_id,
    )
    item = await media_service.request_item(
        external_ref=external_ref,
        title=title,
        tenant_id=tenant_id or "global",
    )
    return item.id, item.external_ref, title


def _encode_sse(payload: ScrapeStreamPayload) -> bytes:
    """Encode one scrape SSE payload frame."""

    return f"data: {payload.model_dump_json(exclude_none=True)}\n\n".encode()


async def _iter_scrape_events(title: str) -> AsyncIterator[bytes]:
    """Emit the smallest current SSE scrape lifecycle understood by the frontend."""

    yield _encode_sse(
        ScrapeStreamPayload(
            event="start",
            message=f"Starting scrape for {title}",
            total_services=0,
            services_completed=0,
            total_streams=0,
        )
    )
    yield _encode_sse(
        ScrapeStreamPayload(
            event="complete",
            message="Scraping complete. Found 0 total streams.",
            streams={},
            total_streams=0,
            services_completed=0,
            total_services=0,
        )
    )


@router.post(
    "/auto",
    operation_id="scrape.auto",
    response_model=MessageResponse,
    dependencies=[Depends(require_permissions("scrape:write"))],
)
async def auto_scrape(
    http_request: Request,
    request: AutoScrapeRequest,
    media_service: Annotated[MediaService, Depends(get_media_service)],
) -> MessageResponse:
    """Transition one real item into the scrape pipeline and enqueue the worker stage."""

    item_identifier = _scrape_item_identifier_from_request(request)
    if item_identifier is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="One of item_id, tmdb_id, tvdb_id, or imdb_id is required",
        )

    _, title = await _queue_real_scrape(
        request=http_request,
        media_service=media_service,
        item_identifier=item_identifier,
        scrape_request=request,
    )
    return MessageResponse(message=f"Scrape queued for {title}")


@router.post(
    "/start_session",
    operation_id="scrape.start_session",
    response_model=StartSessionResponse,
    dependencies=[Depends(require_permissions("scrape:write"))],
)
async def start_manual_session(
    request_context: Request,
    magnet: str | None = None,
    item_id: str | int | None = None,
    tmdb_id: str | None = None,
    tvdb_id: str | None = None,
    imdb_id: str | None = None,
    media_type: Literal["movie", "tv"] | None = None,
    min_filesize_override: int | None = None,
    max_filesize_override: int | None = None,
    *,
    media_service: Annotated[MediaService, Depends(get_media_service)],
) -> StartSessionResponse:
    """Queue a real scrape and return a pollable session identifier for the frontend."""

    _ = (magnet, min_filesize_override, max_filesize_override)
    item_identifier = _session_item_identifier(
        item_id=item_id,
        tmdb_id=tmdb_id,
        tvdb_id=tvdb_id,
        imdb_id=imdb_id,
    )
    if item_identifier is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="One of item_id, tmdb_id, tvdb_id, or imdb_id is required",
        )

    resolved_item_id, title = await _queue_real_scrape(
        request=request_context,
        media_service=media_service,
        item_identifier=item_identifier,
    )
    session_id = uuid4().hex
    expires_at = datetime.now(UTC) + timedelta(minutes=5)
    _SCRAPE_SESSIONS[session_id] = ScrapeSessionRecord(
        session_id=session_id,
        item_id=resolved_item_id,
        title=title,
        media_type=media_type,
        tmdb_id=tmdb_id,
        tvdb_id=tvdb_id,
        imdb_id=imdb_id,
        torrent_id=session_id,
        infohash=session_id,
        parsed_files=[],
        expires_at=expires_at,
    )
    return StartSessionResponse(
        message=f"Scrape queued for {title}",
        session_id=session_id,
        item_id=resolved_item_id,
        media_type=media_type,
        tmdb_id=tmdb_id,
        tvdb_id=tvdb_id,
        imdb_id=imdb_id,
        torrent_id=session_id,
        torrent_info=TorrentInfoResponse(id=session_id, name=title, infohash=session_id, files={}),
        containers=TorrentContainerResponse(infohash=session_id, files=[], torrent_id=session_id),
        parsed_files=[],
        expires_at=expires_at.isoformat(),
    )


@router.get(
    "/session/{session_id}",
    operation_id="scrape.session_state",
    response_model=ScrapeSessionStateResponse,
)
async def get_session_state(
    session_id: str,
    request: Request,
    media_service: Annotated[MediaService, Depends(get_media_service)],
) -> ScrapeSessionStateResponse:
    """Return the current persisted item state for one queued scrape session."""

    session = _require_session(session_id)
    auth_context = get_auth_context(request)
    item = await media_service.get_item(session.item_id, tenant_id=auth_context.tenant_id)
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")

    return ScrapeSessionStateResponse(
        session_id=session.session_id,
        item_id=session.item_id,
        title=session.title,
        state=item.state.value,
    )


@router.post(
    "/session/{session_id}",
    operation_id="scrape.session_action",
    response_model=MessageResponse | SelectFilesResponse,
    dependencies=[Depends(require_permissions("scrape:write"))],
)
async def session_action(
    session_id: str,
    request: SessionActionRequest,
    media_service: Annotated[MediaService, Depends(get_media_service)],
) -> MessageResponse | SelectFilesResponse:
    """Apply the current minimal scrape-session actions used by the frontend."""

    session = _require_session(session_id)

    if request.action == "select_files":
        if not request.files:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="files required for select_files action",
            )
        session.selected_files = request.files
        return SelectFilesResponse(
            message=f"Selected files for {session.item_id}",
            download_type="cached",
        )

    if request.action == "update_attributes":
        if request.file_data is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="file_data required for update_attributes action",
            )
        return MessageResponse(message=f"Updated given data to {session.title}")

    if request.action == "abort":
        _SCRAPE_SESSIONS.pop(session_id, None)
        return MessageResponse(message=f"Aborted session {session_id}")

    if request.action == "complete":
        if session.selected_files is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Session is incomplete"
            )
        selected_titles = [
            str(file_record.get("filename", "")).strip()
            for file_record in session.selected_files.values()
            if isinstance(file_record, dict)
        ]
        await media_service.persist_parsed_stream_candidates(
            item_id=session.item_id,
            raw_titles=selected_titles,
            infohash=session.infohash,
        )
        _SCRAPE_SESSIONS.pop(session_id, None)
        return MessageResponse(message=f"Completed session {session_id}")

    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown action")


@router.get("", operation_id="scrape.item", response_model=ScrapeItemResponse)
async def scrape_item(
    request: Request,
    item_id: str | int | None = None,
    tmdb_id: str | None = None,
    tvdb_id: str | None = None,
    imdb_id: str | None = None,
    media_type: Literal["movie", "tv"] | None = None,
    custom_title: str | None = None,
    custom_imdb_id: str | None = None,
    ranking_overrides: str | None = None,
    min_filesize_override: int | None = None,
    max_filesize_override: int | None = None,
    stream: bool = False,
    *,
    media_service: Annotated[MediaService, Depends(get_media_service)],
) -> ScrapeItemResponse | StreamingResponse:
    """Return the current minimal manual-scrape compatibility response.

    The present backend does not yet implement real scraper fan-out or stream ranking.
    This route therefore resolves the target item safely and returns an empty-result
    baseline in either JSON or SSE form so the manual scrape UI can operate without a
    hard backend failure.
    """

    auth_context = get_auth_context(request)
    query = ScrapeItemQuery(
        item_id=item_id,
        tmdb_id=tmdb_id,
        tvdb_id=tvdb_id,
        imdb_id=imdb_id,
        media_type=media_type,
        custom_title=custom_title,
        custom_imdb_id=custom_imdb_id,
        ranking_overrides=ranking_overrides,
        min_filesize_override=min_filesize_override,
        max_filesize_override=max_filesize_override,
        stream=stream,
    )
    _, title = await _resolve_scrape_target(
        query,
        media_service,
        tenant_id=auth_context.tenant_id,
    )

    if stream:
        return StreamingResponse(
            _iter_scrape_events(title),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )

    return ScrapeItemResponse(
        message=f"Manually scraped streams for item {title}",
        streams={},
    )
