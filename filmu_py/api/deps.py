"""FastAPI dependency helpers for typed access to shared app resources."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog
from fastapi import HTTPException, Request, status

from filmu_py.authz import (
    describe_tenant_scope,
    effective_permissions,
    evaluate_permissions,
    permission_constraints_from_mapping,
)
from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.log_stream import LogStreamBroker
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.resources import AppResources
from filmu_py.security.oidc import (
    OidcValidationError,
    OidcValidationResult,
    validate_oidc_bearer_token,
)
from filmu_py.services.access_policy import AccessPolicySnapshot, snapshot_from_settings
from filmu_py.services.media import MediaService
from filmu_py.services.playback import InProcessDirectPlaybackRefreshController

logger = structlog.get_logger("filmu.auth")

_ACTOR_ID_HEADER = "x-actor-id"
_ACTOR_TYPE_HEADER = "x-actor-type"
_ACTOR_DISPLAY_NAME_HEADER = "x-actor-display-name"
_ACTOR_EMAIL_HEADER = "x-actor-email"
_TENANT_ID_HEADER = "x-tenant-id"
_TENANT_DISPLAY_NAME_HEADER = "x-tenant-display-name"
_TENANT_PLAN_HEADER = "x-tenant-plan"
_AUTHORIZED_TENANTS_HEADER = "x-actor-authorized-tenants"
_ACTOR_ROLES_HEADER = "x-actor-roles"
_ACTOR_SCOPES_HEADER = "x-actor-scopes"
_AUTH_SOURCE_HEADER = "x-auth-source"
_OIDC_ISSUER_HEADER = "x-auth-issuer"
_OIDC_SUBJECT_HEADER = "x-auth-subject"
_ACTOR_ID_QUERY = "actor_id"
_ACTOR_TYPE_QUERY = "actor_type"
_ACTOR_DISPLAY_NAME_QUERY = "actor_display_name"
_ACTOR_EMAIL_QUERY = "actor_email"
_TENANT_ID_QUERY = "tenant_id"
_TENANT_DISPLAY_NAME_QUERY = "tenant_display_name"
_TENANT_PLAN_QUERY = "tenant_plan"
_AUTHORIZED_TENANTS_QUERY = "actor_authorized_tenants"
_ACTOR_ROLES_QUERY = "actor_roles"
_ACTOR_SCOPES_QUERY = "actor_scopes"
_AUTH_SOURCE_QUERY = "auth_source"
_DEFAULT_ACTOR_TYPE = "service"
_DEFAULT_TENANT_ID = "global"
_DEFAULT_ROLES: tuple[str, ...] = ()
_DEFAULT_SCOPES: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AuthContext:
    """Resolved request authentication and operator identity context."""

    authentication_mode: str
    source_label: str
    api_key_id: str
    actor_id: str
    actor_type: str
    actor_display_name: str | None
    actor_email: str | None
    tenant_id: str
    tenant_display_name: str | None
    tenant_plan: str | None
    authorized_tenant_ids: tuple[str, ...]
    authorization_tenant_scope: str
    roles: tuple[str, ...]
    scopes: tuple[str, ...]
    effective_permissions: tuple[str, ...]
    oidc_issuer: str | None
    oidc_subject: str | None
    oidc_token_validated: bool
    access_policy_version: str
    access_policy_source: str
    quota_policy_version: str | None


def _normalize_key_id(value: str | None) -> str | None:
    """Return a safe non-secret identifier for troubleshooting auth flows."""

    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _normalize_optional_header(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _request_supports_query_auth_metadata(request: Request) -> bool:
    return bool(getattr(request, "scope", {}).get("type") == "websocket")


def _request_auth_metadata(
    request: Request,
    *,
    header_name: str,
    query_name: str,
) -> str | None:
    header_value = _normalize_optional_header(request.headers.get(header_name))
    if header_value is not None:
        return header_value
    if _request_supports_query_auth_metadata(request):
        return _normalize_optional_header(request.query_params.get(query_name))
    return None


def _split_header_values(raw: str | None, *, fallback: tuple[str, ...]) -> tuple[str, ...]:
    """Parse comma-separated identity headers into a normalized stable tuple."""

    if raw is None:
        return fallback
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    return values or fallback


def _claim_values(claims: dict[str, Any], claim_name: str) -> tuple[str, ...]:
    value = claims.get(claim_name)
    if isinstance(value, str):
        return tuple(part.strip() for part in value.replace(",", " ").split() if part.strip())
    if isinstance(value, list):
        return tuple(item.strip() for item in value if isinstance(item, str) and item.strip())
    return ()


def _merge_values(*groups: tuple[str, ...]) -> tuple[str, ...]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for value in group:
            normalized = value.strip()
            if not normalized or normalized in seen:
                continue
            merged.append(normalized)
            seen.add(normalized)
    return tuple(merged)


def _principal_policy_values(
    values: dict[str, list[str]],
    *,
    actor_id: str,
) -> tuple[str, ...]:
    return tuple(item for item in values.get(actor_id, ()) if isinstance(item, str) and item)


def _resolve_access_policy_snapshot(request: Request) -> AccessPolicySnapshot:
    resources = get_resources(request)
    snapshot = resources.access_policy_snapshot
    if snapshot is not None:
        return snapshot
    return snapshot_from_settings(resources.settings.access_policy)


def _resource_scope_for_path(path: str) -> str:
    """Return a stable policy/audit resource scope for one API path."""

    if path.startswith("/api/v1/auth/policy"):
        return "access_policy"
    if path.startswith("/api/v1/plugins/governance"):
        return "plugin_governance"
    if path.startswith("/api/v1/operations"):
        return "operations"
    if path.startswith("/api/v1/settings"):
        return "settings"
    if path.startswith("/api/v1/generateapikey"):
        return "settings"
    if path.startswith("/api/v1/items"):
        return "items"
    if path.startswith("/api/v1/stream"):
        return "stream"
    return "route"


async def _audit_authorization_decision(
    request: Request,
    *,
    auth_context: AuthContext,
    required_permissions: tuple[str, ...],
    target_tenant_id: str,
    decision: Any,
) -> None:
    policy = _resolve_access_policy_snapshot(request)
    if not policy.audit_decisions:
        return
    path = request.url.path
    resource_scope = _resource_scope_for_path(path)
    audit_service = get_resources(request).authorization_audit_service
    if audit_service is not None:
        try:
            await audit_service.record_decision(
                path=path,
                method=request.method,
                resource_scope=resource_scope,
                actor_id=auth_context.actor_id,
                actor_type=auth_context.actor_type,
                tenant_id=auth_context.tenant_id,
                target_tenant_id=target_tenant_id,
                required_permissions=required_permissions,
                matched_permissions=decision.matched_permissions,
                missing_permissions=decision.missing_permissions,
                constrained_permissions=getattr(decision, "constrained_permissions", ()),
                constraint_failures=getattr(decision, "constraint_failures", ()),
                allowed=decision.allowed,
                reason=decision.reason,
                tenant_scope=decision.tenant_scope,
                authentication_mode=auth_context.authentication_mode,
                access_policy_version=policy.version,
                access_policy_source=policy.source,
                oidc_issuer=auth_context.oidc_issuer,
                oidc_subject=auth_context.oidc_subject,
            )
        except Exception:
            logger.exception(
                "auth.permission_decision_audit_failed",
                path=path,
                method=request.method,
                actor_id=auth_context.actor_id,
                tenant_id=auth_context.tenant_id,
                target_tenant_id=target_tenant_id,
                required_permissions=list(required_permissions),
                allowed=decision.allowed,
                reason=decision.reason,
                access_policy_version=policy.version,
                access_policy_source=policy.source,
            )
    log_method = logger.info if decision.allowed else logger.warning
    log_method(
        "auth.permission_decision",
        path=path,
        method=request.method,
        resource_scope=resource_scope,
        actor_id=auth_context.actor_id,
        actor_type=auth_context.actor_type,
        tenant_id=auth_context.tenant_id,
        target_tenant_id=target_tenant_id,
        required_permissions=list(required_permissions),
        matched_permissions=list(decision.matched_permissions),
        missing_permissions=list(decision.missing_permissions),
        constrained_permissions=list(getattr(decision, "constrained_permissions", ())),
        constraint_failures=list(getattr(decision, "constraint_failures", ())),
        allowed=decision.allowed,
        reason=decision.reason,
        tenant_scope=decision.tenant_scope,
        authentication_mode=auth_context.authentication_mode,
        access_policy_version=policy.version,
        access_policy_source=policy.source,
    )


def _build_auth_context(
    request: Request,
    *,
    api_key_id: str,
    authentication_mode: str = "api_key",
    oidc_result: OidcValidationResult | None = None,
) -> AuthContext:
    """Resolve actor and tenant metadata from headers atop API-key authentication."""

    settings = get_settings(request)
    policy = _resolve_access_policy_snapshot(request)
    oidc_claims = oidc_result.claims if oidc_result is not None else {}
    actor_id_claim = oidc_claims.get(settings.oidc.actor_id_claim)
    actor_type_claim = oidc_claims.get(settings.oidc.actor_type_claim)
    tenant_id_claim = oidc_claims.get(settings.oidc.tenant_id_claim)
    actor_id = (
        _request_auth_metadata(
            request,
            header_name=_ACTOR_ID_HEADER,
            query_name=_ACTOR_ID_QUERY,
        )
        or (actor_id_claim if isinstance(actor_id_claim, str) and actor_id_claim else None)
        or f"api-key:{api_key_id}"
    )
    actor_type = (
        _request_auth_metadata(
            request,
            header_name=_ACTOR_TYPE_HEADER,
            query_name=_ACTOR_TYPE_QUERY,
        )
        or (actor_type_claim if isinstance(actor_type_claim, str) and actor_type_claim else None)
        or _DEFAULT_ACTOR_TYPE
    )
    tenant_id = (
        _request_auth_metadata(
            request,
            header_name=_TENANT_ID_HEADER,
            query_name=_TENANT_ID_QUERY,
        )
        or (tenant_id_claim if isinstance(tenant_id_claim, str) and tenant_id_claim else None)
        or _DEFAULT_TENANT_ID
    )
    actor_display_name = _request_auth_metadata(
        request,
        header_name=_ACTOR_DISPLAY_NAME_HEADER,
        query_name=_ACTOR_DISPLAY_NAME_QUERY,
    )
    actor_email = _request_auth_metadata(
        request,
        header_name=_ACTOR_EMAIL_HEADER,
        query_name=_ACTOR_EMAIL_QUERY,
    )
    tenant_display_name = _request_auth_metadata(
        request,
        header_name=_TENANT_DISPLAY_NAME_HEADER,
        query_name=_TENANT_DISPLAY_NAME_QUERY,
    )
    tenant_plan = _request_auth_metadata(
        request,
        header_name=_TENANT_PLAN_HEADER,
        query_name=_TENANT_PLAN_QUERY,
    )
    authorized_tenant_ids = _split_header_values(
        _request_auth_metadata(
            request,
            header_name=_AUTHORIZED_TENANTS_HEADER,
            query_name=_AUTHORIZED_TENANTS_QUERY,
        ),
        fallback=_claim_values(oidc_claims, settings.oidc.authorized_tenants_claim) or (tenant_id,),
    )
    roles = _merge_values(
        _split_header_values(
            _request_auth_metadata(
                request,
                header_name=_ACTOR_ROLES_HEADER,
                query_name=_ACTOR_ROLES_QUERY,
            ),
            fallback=_claim_values(oidc_claims, settings.oidc.roles_claim) or _DEFAULT_ROLES,
        ),
        _principal_policy_values(policy.principal_roles, actor_id=actor_id),
    )
    scopes = _merge_values(
        _split_header_values(
            _request_auth_metadata(
                request,
                header_name=_ACTOR_SCOPES_HEADER,
                query_name=_ACTOR_SCOPES_QUERY,
            ),
            fallback=_claim_values(oidc_claims, settings.oidc.scopes_claim) or _DEFAULT_SCOPES,
        ),
        _principal_policy_values(policy.principal_scopes, actor_id=actor_id),
    )
    authorized_tenant_ids = _merge_values(
        authorized_tenant_ids,
        _principal_policy_values(
            policy.principal_tenant_grants,
            actor_id=actor_id,
        ),
    )
    effective = effective_permissions(
        roles=roles,
        scopes=scopes,
        role_permission_grants=policy.role_grants,
    )
    return AuthContext(
        authentication_mode=authentication_mode,
        source_label=(
            _request_auth_metadata(
                request,
                header_name=_AUTH_SOURCE_HEADER,
                query_name=_AUTH_SOURCE_QUERY,
            )
            or authentication_mode
        ),
        api_key_id=api_key_id,
        actor_id=actor_id,
        actor_type=actor_type,
        actor_display_name=actor_display_name,
        actor_email=actor_email,
        tenant_id=tenant_id,
        tenant_display_name=tenant_display_name,
        tenant_plan=tenant_plan,
        authorized_tenant_ids=authorized_tenant_ids,
        authorization_tenant_scope=describe_tenant_scope(
            actor_tenant_id=tenant_id,
            authorized_tenant_ids=authorized_tenant_ids,
            granted_permissions=effective,
        ),
        roles=roles,
        scopes=scopes,
        effective_permissions=effective,
        oidc_issuer=(
            oidc_result.issuer
            if oidc_result is not None
            else _normalize_optional_header(request.headers.get(_OIDC_ISSUER_HEADER))
        ),
        oidc_subject=(
            oidc_result.subject
            if oidc_result is not None
            else _normalize_optional_header(request.headers.get(_OIDC_SUBJECT_HEADER))
        ),
        oidc_token_validated=oidc_result is not None,
        access_policy_version=policy.version,
        access_policy_source=policy.source,
        quota_policy_version=settings.tenant_quotas.version if settings.tenant_quotas.enabled else None,
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
        decision = evaluate_permissions(
            granted_permissions=auth_context.effective_permissions,
            required_permissions=normalized_permissions,
            actor_tenant_id=auth_context.tenant_id,
            target_tenant_id=auth_context.tenant_id,
            authorized_tenant_ids=auth_context.authorized_tenant_ids,
            actor_type=auth_context.actor_type,
            authentication_mode=auth_context.authentication_mode,
            request_path=request.url.path,
            resource_scope=_resource_scope_for_path(request.url.path),
            permission_constraints=permission_constraints_from_mapping(
                _resolve_access_policy_snapshot(request).permission_constraints
            ),
        )
        await _audit_authorization_decision(
            request,
            auth_context=auth_context,
            required_permissions=normalized_permissions,
            target_tenant_id=auth_context.tenant_id,
            decision=decision,
        )
        if not decision.allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Authorization denied ({decision.reason}) for tenant "
                    f"'{decision.target_tenant_id}'"
                ),
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
    authorization = request.headers.get("authorization", "")
    bearer_token = authorization[7:].strip() if authorization[:7].lower() == "bearer " else None

    if settings.oidc.enabled and bearer_token and bearer_token != expected:
        try:
            oidc_result = await validate_oidc_bearer_token(
                bearer_token,
                settings=settings.oidc,
                cache=get_cache(request),
            )
        except OidcValidationError as exc:
            logger.warning(
                "auth.oidc.rejected",
                issuer=settings.oidc.issuer,
                path=request.url.path,
                method=request.method,
                reason=str(exc),
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid OIDC bearer token",
            ) from exc

        auth_context = _build_auth_context(
            request,
            api_key_id=f"oidc:{oidc_result.issuer}",
            authentication_mode="oidc",
            oidc_result=oidc_result,
        )
        await _enforce_tenant_api_quota(request, auth_context)
        request.state.auth_context = auth_context
        identity_service = get_resources(request).security_identity_service
        if identity_service is not None:
            request.state.auth_identity = await identity_service.record_auth_context(auth_context)
        _bind_auth_context(auth_context)
        return

    provided = (
        request.headers.get("x-api-key")
        or bearer_token
        or request.query_params.get("api_key")
    )
    if settings.oidc.enabled and not settings.oidc.allow_api_key_fallback:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="OIDC bearer token required",
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
    await _enforce_tenant_api_quota(request, auth_context)
    request.state.auth_context = auth_context
    identity_service = get_resources(request).security_identity_service
    if identity_service is not None:
        request.state.auth_identity = await identity_service.record_auth_context(auth_context)
    _bind_auth_context(auth_context)


async def _enforce_tenant_api_quota(request: Request, auth_context: AuthContext) -> None:
    """Enforce request-intake quotas before route handlers run."""

    settings = get_settings(request)
    if not settings.tenant_quotas.enabled:
        return
    limit = settings.tenant_quotas.tenants.get(
        auth_context.tenant_id,
        settings.tenant_quotas.default,
    ).api_requests_per_minute
    if limit is None or limit <= 0:
        return

    minute = int(time.time() // 60)
    key = f"quota:tenant:{auth_context.tenant_id}:api:{minute}"
    redis = get_resources(request).redis
    current = await redis.incr(key)
    if current == 1:
        await redis.expire(key, 120)
    if current > limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                "Tenant request quota exceeded "
                f"(policy={settings.tenant_quotas.version}, limit={limit}/minute)"
            ),
        )


def _bind_auth_context(auth_context: AuthContext) -> None:
    """Bind sanitized identity attributes to structured logs and metrics."""

    structlog.contextvars.bind_contextvars(
        api_key_id=auth_context.api_key_id,
        actor_id=auth_context.actor_id,
        actor_type=auth_context.actor_type,
        tenant_id=auth_context.tenant_id,
        authorized_tenant_ids=",".join(auth_context.authorized_tenant_ids),
        authorization_tenant_scope=auth_context.authorization_tenant_scope,
        actor_roles=",".join(auth_context.roles),
        actor_scopes=",".join(auth_context.scopes),
        effective_permissions=",".join(auth_context.effective_permissions),
        authentication_mode=auth_context.authentication_mode,
        oidc_issuer=auth_context.oidc_issuer or "",
        oidc_subject=auth_context.oidc_subject or "",
        oidc_token_validated=auth_context.oidc_token_validated,
        access_policy_version=auth_context.access_policy_version,
        access_policy_source=auth_context.access_policy_source,
        quota_policy_version=auth_context.quota_policy_version or "",
    )
