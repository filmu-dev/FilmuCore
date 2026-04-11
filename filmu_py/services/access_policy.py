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


@dataclass(frozen=True, slots=True)
class AccessPolicyRevisionRecord:
    """Persisted operator-visible access-policy revision row."""

    version: str
    source: str
    is_active: bool
    activated_at: datetime
    created_at: datetime
    updated_at: datetime
    role_grants: dict[str, list[str]]
    principal_roles: dict[str, list[str]]
    principal_scopes: dict[str, list[str]]
    principal_tenant_grants: dict[str, list[str]]
    audit_decisions: bool

    def to_snapshot(self) -> AccessPolicySnapshot:
        """Return the request-time snapshot projection for this revision."""

        return AccessPolicySnapshot(
            version=self.version,
            source=self.source,
            role_grants=self.role_grants,
            principal_roles=self.principal_roles,
            principal_scopes=self.principal_scopes,
            principal_tenant_grants=self.principal_tenant_grants,
            audit_decisions=self.audit_decisions,
        )


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
            if active is not None and active.source != "settings_bootstrap":
                return _snapshot_from_payload(
                    version=active.version,
                    source=active.source,
                    payload=active.policy_data,
                )
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
            snapshot = _snapshot_from_payload(
                version=revision.version,
                source=revision.source,
                payload=revision.policy_data,
            )
            await session.commit()
        return snapshot

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

    async def list_revisions(self, *, limit: int = 20) -> list[AccessPolicyRevisionRecord]:
        """Return persisted access-policy revisions ordered for operator review."""

        async with self._db.session() as session:
            revisions = (
                await session.execute(
                    select(AccessPolicyRevisionORM)
                    .order_by(
                        AccessPolicyRevisionORM.is_active.desc(),
                        AccessPolicyRevisionORM.activated_at.desc(),
                        AccessPolicyRevisionORM.updated_at.desc(),
                    )
                    .limit(max(1, limit))
                )
            ).scalars()
            return [_record_from_orm(revision) for revision in revisions]

    async def write_revision(
        self,
        *,
        version: str,
        source: str,
        role_grants: dict[str, list[str]],
        principal_roles: dict[str, list[str]],
        principal_scopes: dict[str, list[str]],
        principal_tenant_grants: dict[str, list[str]],
        audit_decisions: bool,
        activate: bool = True,
    ) -> AccessPolicyRevisionRecord:
        """Create or update one persisted access-policy revision."""

        version_key = version.strip()
        if not version_key:
            raise ValueError("access policy revision version must not be empty")
        source_key = source.strip() or "operator_api"
        payload = {
            "role_grants": role_grants,
            "principal_roles": principal_roles,
            "principal_scopes": principal_scopes,
            "principal_tenant_grants": principal_tenant_grants,
            "audit_decisions": audit_decisions,
        }
        now = datetime.now(UTC)

        async with self._db.session() as session:
            revision = (
                await session.execute(
                    select(AccessPolicyRevisionORM).where(
                        AccessPolicyRevisionORM.version == version_key
                    )
                )
            ).scalar_one_or_none()
            if activate:
                active_revisions = (
                    await session.execute(
                        select(AccessPolicyRevisionORM).where(
                            AccessPolicyRevisionORM.is_active.is_(True)
                        )
                    )
                ).scalars()
                for active in active_revisions:
                    active.is_active = False

            if revision is None:
                revision = AccessPolicyRevisionORM(
                    version=version_key,
                    source=source_key,
                    policy_data=payload,
                    is_active=activate,
                    activated_at=now,
                )
                session.add(revision)
            else:
                revision.source = source_key
                revision.policy_data = payload
                revision.is_active = activate
                if activate:
                    revision.activated_at = now

            await session.flush()
            record = _record_from_orm(revision)
            await session.commit()
        return record

    async def activate_revision(self, version: str) -> AccessPolicyRevisionRecord:
        """Activate one persisted access-policy revision by version."""

        version_key = version.strip()
        if not version_key:
            raise ValueError("access policy revision version must not be empty")
        now = datetime.now(UTC)
        async with self._db.session() as session:
            revision = (
                await session.execute(
                    select(AccessPolicyRevisionORM).where(
                        AccessPolicyRevisionORM.version == version_key
                    )
                )
            ).scalar_one_or_none()
            if revision is None:
                raise LookupError(f"unknown access policy revision '{version_key}'")

            active_revisions = (
                await session.execute(
                    select(AccessPolicyRevisionORM).where(
                        AccessPolicyRevisionORM.is_active.is_(True)
                    )
                )
            ).scalars()
            for active in active_revisions:
                active.is_active = False

            revision.is_active = True
            revision.activated_at = now
            if revision.source == "settings_bootstrap":
                revision.source = "operator_activation"
            await session.flush()
            record = _record_from_orm(revision)
            await session.commit()
        return record


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


def _record_from_orm(revision: AccessPolicyRevisionORM) -> AccessPolicyRevisionRecord:
    payload = revision.policy_data if isinstance(revision.policy_data, dict) else {}
    return AccessPolicyRevisionRecord(
        version=revision.version,
        source=revision.source,
        is_active=revision.is_active,
        activated_at=revision.activated_at,
        created_at=revision.created_at,
        updated_at=revision.updated_at,
        role_grants=_coerce_mapping(payload.get("role_grants")),
        principal_roles=_coerce_mapping(payload.get("principal_roles")),
        principal_scopes=_coerce_mapping(payload.get("principal_scopes")),
        principal_tenant_grants=_coerce_mapping(payload.get("principal_tenant_grants")),
        audit_decisions=bool(payload.get("audit_decisions", True)),
    )
