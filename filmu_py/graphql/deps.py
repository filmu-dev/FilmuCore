"""GraphQL dependency adapters and context wiring helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import structlog
from fastapi import HTTPException, status
from fastapi import Request
from strawberry.fastapi import BaseContext
from strawberry.types import Info

from filmu_py.api.deps import get_auth_context, get_resources
from filmu_py.authz import evaluate_permissions, permission_constraints_from_mapping
from filmu_py.core.event_bus import EventBus
from filmu_py.core.log_stream import LogStreamBroker
from filmu_py.resources import AppResources
from filmu_py.services.media import MediaService
from filmu_py.services.access_policy import snapshot_from_settings
from filmu_py.services.settings_service import update_settings_path

logger = structlog.get_logger("filmu.graphql.auth")


@dataclass
class GraphQLContext(BaseContext):
    """Typed Strawberry context payload for resolver access."""

    request: Request
    resources: AppResources
    media_service: MediaService
    event_bus: EventBus
    log_stream: LogStreamBroker
    settings_updater: Callable[[str, Any], Any]


def get_graphql_context(request: Request) -> GraphQLContext:
    """Return typed Strawberry context with shared runtime resources."""

    resources: AppResources = get_resources(request)

    async def _settings_updater(path: str, value: Any) -> bool:
        await update_settings_path(request=request, db=resources.db, path=path, value=value)
        return True

    return GraphQLContext(
        request=request,
        resources=resources,
        media_service=resources.media_service,
        event_bus=resources.event_bus,
        log_stream=resources.log_stream,
        settings_updater=_settings_updater,
    )


async def require_graphql_permissions(
    info: Info[GraphQLContext, object],
    *required_permissions: str,
    resource_scope: str,
    target_tenant_id: str | None = None,
) -> None:
    """Reject GraphQL writes that do not satisfy the required control-plane permissions."""

    normalized_permissions = tuple(
        permission.strip().lower() for permission in required_permissions if permission.strip()
    )
    if not normalized_permissions:
        return

    request = info.context.request
    auth_context = get_auth_context(request)
    policy = info.context.resources.access_policy_snapshot or snapshot_from_settings(
        info.context.resources.settings.access_policy
    )
    resolved_target_tenant_id = target_tenant_id or auth_context.tenant_id
    decision = evaluate_permissions(
        granted_permissions=auth_context.effective_permissions,
        required_permissions=normalized_permissions,
        actor_tenant_id=auth_context.tenant_id,
        target_tenant_id=resolved_target_tenant_id,
        authorized_tenant_ids=auth_context.authorized_tenant_ids,
        actor_type=auth_context.actor_type,
        authentication_mode=auth_context.authentication_mode,
        request_path=request.url.path,
        resource_scope=resource_scope,
        permission_constraints=permission_constraints_from_mapping(policy.permission_constraints),
    )
    if policy.audit_decisions:
        audit_service = info.context.resources.authorization_audit_service
        if audit_service is not None:
            await audit_service.record_decision(
                path=request.url.path,
                method=request.method,
                resource_scope=resource_scope,
                actor_id=auth_context.actor_id,
                actor_type=auth_context.actor_type,
                tenant_id=auth_context.tenant_id,
                target_tenant_id=resolved_target_tenant_id,
                required_permissions=normalized_permissions,
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
    log_method = logger.info if decision.allowed else logger.warning
    log_method(
        "auth.permission_decision",
        path=request.url.path,
        method=request.method,
        resource_scope=resource_scope,
        actor_id=auth_context.actor_id,
        actor_type=auth_context.actor_type,
        tenant_id=auth_context.tenant_id,
        target_tenant_id=resolved_target_tenant_id,
        required_permissions=list(normalized_permissions),
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
    if not decision.allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Authorization denied ({decision.reason}) for tenant "
                f"'{decision.target_tenant_id}'"
            ),
        )
