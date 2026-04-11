"""Persisted access-policy inventory and resolution helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select

from filmu_py.config import AccessPolicySettings, Settings
from filmu_py.db.models import AccessPolicyRevisionORM
from filmu_py.db.runtime import DatabaseRuntime


@dataclass(frozen=True, slots=True)
class AccessPolicySnapshot:
    """Resolved access-policy snapshot used by request auth and operator views."""

    version: str
    source: str
    role_grants: dict[str, list[str]]
    principal_roles: dict[str, list[str]]
    principal_scopes: dict[str, list[str]]
    principal_tenant_grants: dict[str, list[str]]
    audit_decisions: bool


class AccessPolicyService:
    """Persist and resolve operator-visible access-policy revisions."""

    def __init__(self, db: DatabaseRuntime) -> None:
        self._db = db

    async def bootstrap(self, settings: Settings) -> AccessPolicySnapshot:
        """Persist the configured access policy and return the active snapshot."""

        desired = _policy_payload(settings.access_policy)
        now = datetime.now(UTC)
        async with self._db.session() as session:
            active = (
                await session.execute(
                    select(AccessPolicyRevisionORM).where(
                        AccessPolicyRevisionORM.is_active.is_(True)
                    )
                )
            ).scalar_one_or_none()
            revision = (
                await session.execute(
                    select(AccessPolicyRevisionORM).where(
                        AccessPolicyRevisionORM.version == settings.access_policy.version
                    )
                )
            ).scalar_one_or_none()
            if active is None or active.version != settings.access_policy.version:
                if active is not None:
                    active.is_active = False
                if revision is None:
                    revision = AccessPolicyRevisionORM(
                        version=settings.access_policy.version,
                        source="settings_bootstrap",
                        policy_data=desired,
                        is_active=True,
                        activated_at=now,
                    )
                    session.add(revision)
                else:
                    revision.source = "settings_bootstrap"
                    revision.policy_data = desired
                    revision.is_active = True
                    revision.activated_at = now
            elif active.policy_data != desired:
                active.policy_data = desired
                active.source = "settings_bootstrap"
                active.activated_at = now
                revision = active
            else:
                revision = active
            await session.commit()
        return _snapshot_from_payload(
            version=revision.version,
            source=revision.source,
            payload=revision.policy_data,
        )

    async def load_active(self) -> AccessPolicySnapshot | None:
        """Return the active persisted access policy when available."""

        async with self._db.session() as session:
            active = (
                await session.execute(
                    select(AccessPolicyRevisionORM).where(
                        AccessPolicyRevisionORM.is_active.is_(True)
                    )
                )
            ).scalar_one_or_none()
        if active is None:
            return None
        return _snapshot_from_payload(
            version=active.version,
            source=active.source,
            payload=active.policy_data,
        )


def snapshot_from_settings(policy: AccessPolicySettings) -> AccessPolicySnapshot:
    """Return a transient snapshot from runtime settings when DB state is unavailable."""

    return _snapshot_from_payload(
        version=policy.version,
        source="settings",
        payload=_policy_payload(policy),
    )


def _policy_payload(policy: AccessPolicySettings) -> dict[str, object]:
    return {
        "role_grants": {
            role: list(permissions) for role, permissions in sorted(policy.role_grants.items())
        },
        "principal_roles": {
            principal: list(values)
            for principal, values in sorted(policy.principal_roles.items())
        },
        "principal_scopes": {
            principal: list(values)
            for principal, values in sorted(policy.principal_scopes.items())
        },
        "principal_tenant_grants": {
            principal: list(values)
            for principal, values in sorted(policy.principal_tenant_grants.items())
        },
        "audit_decisions": bool(policy.audit_decisions),
    }


def _snapshot_from_payload(
    *,
    version: str,
    source: str,
    payload: dict[str, object],
) -> AccessPolicySnapshot:
    return AccessPolicySnapshot(
        version=version,
        source=source,
        role_grants=_coerce_mapping(payload.get("role_grants")),
        principal_roles=_coerce_mapping(payload.get("principal_roles")),
        principal_scopes=_coerce_mapping(payload.get("principal_scopes")),
        principal_tenant_grants=_coerce_mapping(payload.get("principal_tenant_grants")),
        audit_decisions=bool(payload.get("audit_decisions", True)),
    )


def _coerce_mapping(raw: object) -> dict[str, list[str]]:
    if not isinstance(raw, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, list):
            normalized[key] = [item for item in value if isinstance(item, str) and item]
        else:
            normalized[key] = []
    return normalized
