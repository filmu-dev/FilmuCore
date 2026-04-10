"""FastAPI dependency helpers for typed access to shared app resources."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

import structlog
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

logger = structlog.get_logger("filmu.auth")

_ACTOR_ID_HEADER = "x-actor-id"
_ACTOR_TYPE_HEADER = "x-actor-type"
_TENANT_ID_HEADER = "x-tenant-id"
_ACTOR_ROLES_HEADER = "x-actor-roles"
_ACTOR_SCOPES_HEADER = "x-actor-scopes"
_DEFAULT_ACTOR_TYPE = "service"
_DEFAULT_TENANT_ID = "global"
_DEFAULT_ROLES = ("platform:admin",)
_DEFAULT_SCOPES = ("backend:admin",)


@dataclass(frozen=True, slots=True)
class AuthContext:
    """Resolved request authentication and operator identity context."""

    authentication_mode: str
    api_key_id: str
    actor_id: str
    actor_type: str
    tenant_id: str
    roles: tuple[str, ...]
    scopes: tuple[str, ...]


def _fingerprint_secret(value: str | None) -> str | None:
    """Return a short non-reversible fingerprint for troubleshooting auth flows."""

    if not value:
        return None
    return sha256(value.encode("utf-8")).hexdigest()[:12]


def _split_header_values(raw: str | None, *, fallback: tuple[str, ...]) -> tuple[str, ...]:
    """Parse comma-separated identity headers into a normalized stable tuple."""

    if raw is None:
        return fallback
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    return values or fallback


def _build_auth_context(request: Request, *, api_key_id: str) -> AuthContext:
    """Resolve actor and tenant metadata from headers atop API-key authentication."""

    actor_id = request.headers.get(_ACTOR_ID_HEADER) or f"api-key:{api_key_id}"
    actor_type = request.headers.get(_ACTOR_TYPE_HEADER) or _DEFAULT_ACTOR_TYPE
    tenant_id = request.headers.get(_TENANT_ID_HEADER) or _DEFAULT_TENANT_ID
    roles = _split_header_values(
        request.headers.get(_ACTOR_ROLES_HEADER),
        fallback=_DEFAULT_ROLES,
    )
    scopes = _split_header_values(
        request.headers.get(_ACTOR_SCOPES_HEADER),
        fallback=_DEFAULT_SCOPES,
    )
    return AuthContext(
        authentication_mode="api_key",
        api_key_id=api_key_id,
        actor_id=actor_id,
        actor_type=actor_type,
        tenant_id=tenant_id,
        roles=roles,
        scopes=scopes,
    )


def get_auth_context(request: Request) -> AuthContext:
    """Return the resolved request auth context for downstream audit and routing."""

    auth_context = getattr(request.state, "auth_context", None)
    if isinstance(auth_context, AuthContext):
        return auth_context

    api_key = get_settings(request).api_key.get_secret_value()
    return _build_auth_context(request, api_key_id=_fingerprint_secret(api_key) or "unknown")


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
        logger.warning(
            "auth.api_key.rejected",
            expected_key_id=_fingerprint_secret(expected),
            provided_key_id=_fingerprint_secret(provided),
            provided=bool(provided),
            path=request.url.path,
            method=request.method,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )

    auth_context = _build_auth_context(
        request,
        api_key_id=_fingerprint_secret(expected) or "unknown",
    )
    request.state.auth_context = auth_context
    structlog.contextvars.bind_contextvars(
        api_key_id=auth_context.api_key_id,
        actor_id=auth_context.actor_id,
        actor_type=auth_context.actor_type,
        tenant_id=auth_context.tenant_id,
        actor_roles=",".join(auth_context.roles),
        actor_scopes=",".join(auth_context.scopes),
        authentication_mode=auth_context.authentication_mode,
    )
