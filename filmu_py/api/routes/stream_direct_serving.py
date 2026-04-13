"""Direct-file descriptor helpers extracted from the stream route module."""

from __future__ import annotations

import httpx
from fastapi import HTTPException, Request, status
from fastapi.responses import Response

from filmu_py.api.deps import get_resources
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.playback import DirectFileServingDescriptor, PlaybackSourceService


def apply_serving_descriptor_headers(
    *,
    response: Response,
    descriptor: DirectFileServingDescriptor,
) -> None:
    """Attach one descriptor-owned response-header set without overriding upstream headers."""

    for header, value in descriptor.response_headers.items():
        response.headers.setdefault(header, value)



def descriptor_content_length(descriptor: DirectFileServingDescriptor) -> int | None:
    """Return the known content length from one serving descriptor when present."""

    raw_value = descriptor.response_headers.get("content-length")
    if raw_value is None:
        return None
    try:
        parsed = int(raw_value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


async def resolve_direct_file_serving_descriptor(
    db: DatabaseRuntime,
    item_identifier: str,
    *,
    request: Request,
    force_refresh: bool = False,
) -> DirectFileServingDescriptor:
    """Resolve one typed direct-file serving descriptor for the direct playback route."""

    playback_service = resolve_playback_service(request=request, db=db)
    return await playback_service.resolve_direct_file_serving_descriptor(
        item_identifier,
        force_refresh=force_refresh,
    )



def resolve_playback_service(
    *,
    request: Request | None,
    db: DatabaseRuntime,
) -> PlaybackSourceService:
    """Resolve the shared playback service for one route request when available."""

    if request is None:
        return PlaybackSourceService(db)

    try:
        resources = get_resources(request)
    except RuntimeError:
        return PlaybackSourceService(db)
    return resources.playback_service or PlaybackSourceService(
        db,
        settings=resources.settings,
        rate_limiter=resources.rate_limiter,
    )



def should_validate_remote_direct_descriptor(descriptor: DirectFileServingDescriptor) -> bool:
    """Return whether one remote direct descriptor should be probed before proxy serving."""

    if descriptor.transport != "remote-proxy" or descriptor.provenance is None:
        return False
    lifecycle = descriptor.provenance.lifecycle
    if lifecycle is None or lifecycle.owner_kind != "media-entry":
        return False
    return lifecycle.provider_family in {"debrid", "provider"}



def stable_direct_playback_refresh_detail(
    descriptor: DirectFileServingDescriptor | None = None,
) -> str:
    """Return the stable direct-play refresh failure detail expected by route callers."""

    detail = "Selected direct playback lease refresh failed"
    lifecycle = descriptor.provenance.lifecycle if descriptor and descriptor.provenance else None
    if (
        lifecycle is not None
        and lifecycle.owner_kind == "media-entry"
        and lifecycle.refresh_state == "failed"
        and lifecycle.last_refresh_error
    ):
        return f"{detail}: {lifecycle.last_refresh_error}"
    return detail


async def head_remote_direct_url(url: str) -> None:
    """Validate one remote direct URL with a short HEAD request."""

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(2.0),
        ) as client:
            response = await client.head(url)
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Playback source temporarily unavailable",
        ) from exc
    if not response.is_success:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Playback source temporarily unavailable",
        )
