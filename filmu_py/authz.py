"""Authorization helpers for effective request permissions."""

from __future__ import annotations

from collections.abc import Iterable
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


def effective_permissions(*, roles: Iterable[str], scopes: Iterable[str]) -> tuple[str, ...]:
    """Return the normalized, deduplicated effective permission set."""

    resolved: set[str] = set()
    for role in roles:
        resolved.update(_ROLE_PERMISSION_GRANTS.get(_normalize_permission(role), ()))
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

    return AuthorizationDecision(
        allowed=True,
        reason="allowed",
        target_tenant_id=resolved_target_tenant_id,
        tenant_scope=tenant_scope,
        matched_permissions=normalized_required,
    )
