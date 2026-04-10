"""FastAPI dependency helpers for typed access to shared app resources."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import structlog
from fastapi import HTTPException, Request, status

from filmu_py.authz import effective_permissions, has_permissions
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
_DEFAULT_ROLES: tuple[str, ...] = ()
_DEFAULT_SCOPES: tuple[str, ...] = ()


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
    effective_permissions: tuple[str, ...]


def _normalize_key_id(value: str | None) -> str | None:
    """Return a safe non-secret identifier for troubleshooting auth flows."""

    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


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
        effective_permissions=effective_permissions(roles=roles, scopes=scopes),
    )


def get_auth_context(request: Request) -> AuthContext:
    """Return the resolved request auth context for downstream audit and routing."""

    auth_context = getattr(request.state, "auth_context", None)
    if isinstance(auth_context, AuthContext):
        return auth_context

    api_key_id = _normalize_key_id(get_settings(request).api_key_id)
    return _build_auth_context(request, api_key_id=api_key_id or "primary")


def require_roles(*required_roles: str) -> Callable[[Request], Awaitable[AuthContext]]:
    """Return one dependency that rejects requests missing any required role."""

    normalized_roles = tuple(role.strip() for role in required_roles if role.strip())

    async def _dependency(request: Request) -> AuthContext:
        auth_context = get_auth_context(request)
        if not normalized_roles:
            return auth_context
        if not set(normalized_roles).issubset(set(auth_context.roles)):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing required roles: {', '.join(normalized_roles)}",
            )
        return auth_context

    return _dependency


def require_scopes(*required_scopes: str) -> Callable[[Request], Awaitable[AuthContext]]:
    """Return one dependency that rejects requests missing any required scope."""

    normalized_scopes = tuple(scope.strip() for scope in required_scopes if scope.strip())

    async def _dependency(request: Request) -> AuthContext:
        auth_context = get_auth_context(request)
        if not normalized_scopes:
            return auth_context
        if not set(normalized_scopes).issubset(set(auth_context.scopes)):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing required scopes: {', '.join(normalized_scopes)}",
            )
        return auth_context

    return _dependency


def require_permissions(
    *required_permissions: str,
) -> Callable[[Request], Awaitable[AuthContext]]:
    """Return one dependency that rejects requests missing effective permissions."""

    normalized_permissions = tuple(
        permission.strip().lower() for permission in required_permissions if permission.strip()
    )

    async def _dependency(request: Request) -> AuthContext:
        auth_context = get_auth_context(request)
        if not normalized_permissions:
            return auth_context
        if not has_permissions(auth_context.effective_permissions, normalized_permissions):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing required permissions: {', '.join(normalized_permissions)}",
            )
        return auth_context

    return _dependency


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


async def verify_api_key(request: Request) -> None:
    """Validate API key from standard Filmu-compatible request locations."""

    settings = get_settings(request)
    expected = settings.api_key.get_secret_value()
    key_id = _normalize_key_id(settings.api_key_id) or "primary"

    provided = (
        request.headers.get("x-api-key")
        or request.headers.get("authorization", "").removeprefix("Bearer ").strip()
        or request.query_params.get("api_key")
    )

    if not provided or provided != expected:
        logger.warning(
            "auth.api_key.rejected",
            expected_key_id=key_id,
            provided_key_present=bool(provided),
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
        api_key_id=key_id,
    )
    request.state.auth_context = auth_context
    identity_service = get_resources(request).security_identity_service
    if identity_service is not None:
        request.state.auth_identity = await identity_service.record_auth_context(auth_context)
    structlog.contextvars.bind_contextvars(
        api_key_id=auth_context.api_key_id,
        actor_id=auth_context.actor_id,
        actor_type=auth_context.actor_type,
        tenant_id=auth_context.tenant_id,
        actor_roles=",".join(auth_context.roles),
        actor_scopes=",".join(auth_context.scopes),
        effective_permissions=",".join(auth_context.effective_permissions),
        authentication_mode=auth_context.authentication_mode,
    )
