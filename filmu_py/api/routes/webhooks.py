"""Webhook intake routes for external request automation providers."""

from __future__ import annotations

from typing import Annotated, Self

import structlog
from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from pydantic import BaseModel, model_validator

from filmu_py.api.deps import get_auth_context, get_media_service, require_permissions
from filmu_py.services.media import MediaService

router = APIRouter(prefix="/webhook", tags=["webhooks"])

ACTIONABLE_TYPES = {"REQUEST_ADDED", "MEDIA_APPROVED", "MEDIA_AUTO_APPROVED"}
logger = structlog.get_logger(__name__)


class OverseerrMediaBlock(BaseModel):
    media_type: str
    tmdbId: str | None = None
    tvdbId: str | None = None
    status: str | None = None


class OverseerrRequestBlock(BaseModel):
    request_id: str | None = None
    requestedBy_username: str | None = None
    seasons: list[int] | None = None


class OverseerrWebhookPayload(BaseModel):
    notification_type: str
    subject: str
    message: str | None = None
    media: OverseerrMediaBlock | None = None
    request: OverseerrRequestBlock | None = None

    @model_validator(mode="after")
    def validate_actionable_media_type(self) -> Self:
        if (
            self.notification_type in ACTIONABLE_TYPES
            and self.media is not None
            and self.media.media_type not in {"movie", "tv"}
        ):
            raise ValueError("media.media_type must be 'movie' or 'tv'")
        return self


@router.post(
    "/overseerr",
    dependencies=[Depends(require_permissions("webhook:intake"))],
)
async def overseerr_webhook(
    request: Request,
    payload: Annotated[OverseerrWebhookPayload, Body(...)],
    media_service: Annotated[MediaService, Depends(get_media_service)],
) -> dict[str, str | None]:
    if payload.notification_type not in ACTIONABLE_TYPES:
        return {"status": "ignored", "reason": payload.notification_type}
    if payload.media is None or payload.media.tmdbId is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="missing tmdbId in media block",
        )

    auth_context = get_auth_context(request)
    requested_seasons = payload.request.seasons if payload.request is not None else None
    result = await media_service.request_items_by_identifiers(
        identifiers=[f"tmdb:{payload.media.tmdbId}"],
        media_type=payload.media.media_type,
        requested_seasons=requested_seasons,
        request_source="webhook:overseerr",
        tenant_id=auth_context.tenant_id,
    )
    item_id = result.ids[0] if result.ids else None
    logger.info(
        "webhook.overseerr.intake",
        notification_type=payload.notification_type,
        tmdb_id=payload.media.tmdbId,
        media_type=payload.media.media_type,
        requested_seasons=requested_seasons,
        item_id=item_id,
    )
    return {"status": "accepted", "item_id": item_id}
