"""Authorization helpers for effective request permissions."""

from __future__ import annotations

from collections.abc import Iterable

_ROLE_PERMISSION_GRANTS: dict[str, frozenset[str]] = {
    "platform:admin": frozenset({"*"}),
    "settings:write": frozenset({"settings:write"}),
    "playback:operator": frozenset({"playback:read", "playback:operate"}),
}


def _normalize_permission(value: str) -> str:
    return value.strip().lower()


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
