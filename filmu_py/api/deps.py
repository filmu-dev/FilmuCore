"""FastAPI dependency helpers for typed access to shared app resources."""

from __future__ import annotations

from fastapi import HTTPException, Request, status

from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.log_stream import LogStreamBroker
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.resources import AppResources
from filmu_py.services.media import MediaService
from filmu_py.services.playback import InProcessDirectPlaybackRefreshController


def get_resources(request: Request) -> AppResources:
    """Return typed application resources from request state."""

    resources = getattr(request.app.state, "resources", None)
    if not isinstance(resources, AppResources):
        raise RuntimeError("Application resources are not initialized")
    return resources


def get_settings(request: Request) -> Settings:
    """Return runtime settings for request handlers and dependencies."""

    return get_resources(request).settings


def get_cache(request: Request) -> CacheManager:
    """Return shared two-layer cache manager."""

    return get_resources(request).cache


def get_rate_limiter(request: Request) -> DistributedRateLimiter:
    """Return shared distributed rate limiter."""

    return get_resources(request).rate_limiter


def get_event_bus(request: Request) -> EventBus:
    """Return process-local event bus implementation."""

    return get_resources(request).event_bus


def get_log_stream(request: Request) -> LogStreamBroker:
    """Return in-memory log history and live stream broker."""

    return get_resources(request).log_stream


def get_db(request: Request) -> DatabaseRuntime:
    """Return shared database runtime wrapper."""

    return get_resources(request).db


def get_media_service(request: Request) -> MediaService:
    """Return media service used by API and GraphQL resolvers."""

    return get_resources(request).media_service


def get_playback_refresh_controller(
    request: Request,
) -> InProcessDirectPlaybackRefreshController | None:
    """Return the app-scoped in-process direct-play refresh controller when configured."""

    return get_resources(request).playback_refresh_controller


def verify_api_key(request: Request) -> None:
    """Validate API key from standard Filmu-compatible request locations."""

    settings = get_settings(request)
    expected = settings.api_key.get_secret_value()

    provided = (
        request.headers.get("x-api-key")
        or request.headers.get("authorization", "").removeprefix("Bearer ").strip()
        or request.query_params.get("api_key")
    )

    if not provided or provided != expected:
        import logging
        logging.getLogger("filmu.auth").error("API Key mismatch! expected=[%s], provided=[%s]", expected, provided)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
