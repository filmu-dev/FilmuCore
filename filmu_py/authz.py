"""Authorization helpers for effective request permissions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

_ROLE_PERMISSION_GRANTS: dict[str, frozenset[str]] = {
    "platform:admin": frozenset({"*"}),
    "settings:write": frozenset({"settings:write"}),
    "playback:operator": frozenset({"playback:read", "playback:operate"}),
}
_GLOBAL_TENANT_SCOPE_TOKEN = "*"


@dataclass(frozen=True, slots=True)
class AuthorizationDecision:
    """Result of evaluating one request against permission and tenant policy."""

    allowed: bool
    reason: str
    target_tenant_id: str
    tenant_scope: str
    matched_permissions: tuple[str, ...] = ()
    missing_permissions: tuple[str, ...] = ()
    constrained_permissions: tuple[str, ...] = ()
    constraint_failures: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PermissionConstraint:
    """Context-aware ABAC overlay for one effective permission."""

    actor_types: tuple[str, ...] = ()
    authentication_modes: tuple[str, ...] = ()
    route_prefixes: tuple[str, ...] = ()
    resource_scopes: tuple[str, ...] = ()
    tenant_scopes: tuple[str, ...] = ()


def _normalize_permission(value: str) -> str:
    return value.strip().lower()


def _normalize_tenant_id(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _normalize_tenant_scope_values(values: Iterable[str]) -> tuple[str, ...]:
    normalized = {
        value
        for raw in values
        if (value := _normalize_tenant_id(raw)) is not None
    }
    return tuple(sorted(normalized))


def permission_constraints_from_mapping(
    raw: Mapping[str, Mapping[str, Iterable[str]]] | None,
) -> dict[str, PermissionConstraint]:
    """Return normalized permission constraints from one compatibility payload."""

    if raw is None:
        return {}
    normalized: dict[str, PermissionConstraint] = {}
    for permission, payload in raw.items():
        normalized_permission = _normalize_permission(permission)
        if not normalized_permission:
            continue
        normalized[normalized_permission] = PermissionConstraint(
            actor_types=tuple(
                sorted(
                    {
                        value.strip().lower()
                        for value in payload.get("actor_types", ())
                        if isinstance(value, str) and value.strip()
                    }
                )
            ),
            authentication_modes=tuple(
                sorted(
                    {
                        value.strip().lower()
                        for value in payload.get("authentication_modes", ())
                        if isinstance(value, str) and value.strip()
                    }
                )
            ),
            route_prefixes=tuple(
                sorted(
                    {
                        value.strip()
                        for value in payload.get("route_prefixes", ())
                        if isinstance(value, str) and value.strip()
                    }
                )
            ),
            resource_scopes=tuple(
                sorted(
                    {
                        value.strip().lower()
                        for value in payload.get("resource_scopes", ())
                        if isinstance(value, str) and value.strip()
                    }
                )
            ),
            tenant_scopes=tuple(
                sorted(
                    {
                        value.strip().lower()
                        for value in payload.get("tenant_scopes", ())
                        if isinstance(value, str) and value.strip()
                    }
                )
            ),
        )
    return normalized


def _implied_permissions(permission: str) -> set[str]:
    if permission == "backend:admin":
        return {"*"}
    domain, separator, action = permission.partition(":")
    if not separator:
        return {permission}
    implied = {permission}
    if action == "admin":
        implied.add(f"{domain}:*")
    return implied


def effective_permissions(
    *,
    roles: Iterable[str],
    scopes: Iterable[str],
    role_permission_grants: Mapping[str, Iterable[str]] | None = None,
) -> tuple[str, ...]:
    """Return the normalized, deduplicated effective permission set."""

    grants = _ROLE_PERMISSION_GRANTS if role_permission_grants is None else role_permission_grants
    resolved: set[str] = set()
    for role in roles:
        resolved.update(
            _normalize_permission(permission)
            for permission in grants.get(_normalize_permission(role), ())
            if permission
        )
    for scope in scopes:
        resolved.update(_implied_permissions(_normalize_permission(scope)))
    return tuple(sorted(permission for permission in resolved if permission))


def describe_tenant_scope(
    *,
    actor_tenant_id: str,
    authorized_tenant_ids: Iterable[str],
    granted_permissions: Iterable[str] = (),
) -> str:
    """Return a stable operator-facing description of one actor tenant scope."""

    normalized_actor_tenant_id = _normalize_tenant_id(actor_tenant_id) or "global"
    normalized_tenant_ids = set(_normalize_tenant_scope_values(authorized_tenant_ids))
    normalized_permissions = {
        _normalize_permission(permission)
        for permission in granted_permissions
        if permission
    }
    if "*" in normalized_permissions or _GLOBAL_TENANT_SCOPE_TOKEN in normalized_tenant_ids:
        return "all"
    if normalized_tenant_ids == {normalized_actor_tenant_id}:
        return "self"
    if normalized_tenant_ids:
        return "delegated"
    return "self"


def has_permissions(
    granted_permissions: Iterable[str],
    required_permissions: Iterable[str],
) -> bool:
    """Return whether the granted set satisfies every required permission."""

    granted = {_normalize_permission(permission) for permission in granted_permissions if permission}
    if "*" in granted:
        return True

    for permission in (_normalize_permission(value) for value in required_permissions if value):
        if permission in granted:
            continue
        domain, separator, _action = permission.partition(":")
        if separator and f"{domain}:*" in granted:
            continue
        return False
    return True


def evaluate_permissions(
    *,
    granted_permissions: Iterable[str],
    required_permissions: Iterable[str],
    actor_tenant_id: str,
    target_tenant_id: str | None = None,
    authorized_tenant_ids: Iterable[str] = (),
    actor_type: str | None = None,
    authentication_mode: str | None = None,
    request_path: str | None = None,
    resource_scope: str | None = None,
    permission_constraints: Mapping[str, PermissionConstraint] | None = None,
) -> AuthorizationDecision:
    """Return a stable authz decision for one tenant-scoped request."""

    normalized_granted = {
        _normalize_permission(permission)
        for permission in granted_permissions
        if permission
    }
    normalized_required = tuple(
        sorted(
            {
                _normalize_permission(permission)
                for permission in required_permissions
                if permission
            }
        )
    )
    normalized_actor_tenant_id = _normalize_tenant_id(actor_tenant_id) or "global"
    resolved_target_tenant_id = (
        _normalize_tenant_id(target_tenant_id) or normalized_actor_tenant_id
    )
    normalized_tenant_scope = set(_normalize_tenant_scope_values(authorized_tenant_ids))
    normalized_tenant_scope.add(normalized_actor_tenant_id)
    tenant_scope = describe_tenant_scope(
        actor_tenant_id=normalized_actor_tenant_id,
        authorized_tenant_ids=normalized_tenant_scope,
        granted_permissions=normalized_granted,
    )

    missing_permissions = tuple(
        permission
        for permission in normalized_required
        if not has_permissions(normalized_granted, (permission,))
    )
    if missing_permissions:
        return AuthorizationDecision(
            allowed=False,
            reason="missing_permissions",
            target_tenant_id=resolved_target_tenant_id,
            tenant_scope=tenant_scope,
            matched_permissions=tuple(
                permission for permission in normalized_required if permission not in missing_permissions
            ),
            missing_permissions=missing_permissions,
        )

    if (
        resolved_target_tenant_id not in normalized_tenant_scope
        and _GLOBAL_TENANT_SCOPE_TOKEN not in normalized_tenant_scope
        and "*" not in normalized_granted
    ):
        return AuthorizationDecision(
            allowed=False,
            reason="tenant_forbidden",
            target_tenant_id=resolved_target_tenant_id,
            tenant_scope=tenant_scope,
            matched_permissions=normalized_required,
        )

    normalized_actor_type = actor_type.strip().lower() if actor_type is not None else ""
    normalized_authentication_mode = (
        authentication_mode.strip().lower() if authentication_mode is not None else ""
    )
    normalized_resource_scope = resource_scope.strip().lower() if resource_scope is not None else ""
    resolved_constraints = permission_constraints or {}
    constrained_permissions: list[str] = []
    constraint_failures: list[str] = []
    for permission in normalized_required:
        constraint = resolved_constraints.get(permission)
        if constraint is None:
            continue
        if constraint.actor_types and normalized_actor_type not in constraint.actor_types:
            constrained_permissions.append(permission)
            constraint_failures.append(f"{permission}:actor_type")
            continue
        if (
            constraint.authentication_modes
            and normalized_authentication_mode not in constraint.authentication_modes
        ):
            constrained_permissions.append(permission)
            constraint_failures.append(f"{permission}:authentication_mode")
            continue
        if constraint.route_prefixes and (
            request_path is None
            or not any(request_path.startswith(prefix) for prefix in constraint.route_prefixes)
        ):
            constrained_permissions.append(permission)
            constraint_failures.append(f"{permission}:route_prefix")
            continue
        if constraint.resource_scopes and normalized_resource_scope not in constraint.resource_scopes:
            constrained_permissions.append(permission)
            constraint_failures.append(f"{permission}:resource_scope")
            continue
        if constraint.tenant_scopes and tenant_scope.lower() not in constraint.tenant_scopes:
            constrained_permissions.append(permission)
            constraint_failures.append(f"{permission}:tenant_scope")
            continue
    if constrained_permissions:
        return AuthorizationDecision(
            allowed=False,
            reason="permission_constrained",
            target_tenant_id=resolved_target_tenant_id,
            tenant_scope=tenant_scope,
            matched_permissions=tuple(
                permission
                for permission in normalized_required
                if permission not in set(constrained_permissions)
            ),
            constrained_permissions=tuple(sorted(set(constrained_permissions))),
            constraint_failures=tuple(sorted(set(constraint_failures))),
        )

    return AuthorizationDecision(
        allowed=True,
        reason="allowed",
        target_tenant_id=resolved_target_tenant_id,
        tenant_scope=tenant_scope,
        matched_permissions=normalized_required,
    )
