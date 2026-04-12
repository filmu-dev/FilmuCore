"""Durable authorization-decision audit ledger and search helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import func, select

from filmu_py.db.models import AuthorizationDecisionAuditORM
from filmu_py.db.runtime import DatabaseRuntime


@dataclass(frozen=True, slots=True)
class AuthorizationDecisionAuditRecord:
    """Structured persisted authorization decision event."""

    occurred_at: datetime
    path: str
    method: str
    resource_scope: str
    actor_id: str
    actor_type: str
    tenant_id: str
    target_tenant_id: str
    required_permissions: tuple[str, ...]
    matched_permissions: tuple[str, ...]
    missing_permissions: tuple[str, ...]
    constrained_permissions: tuple[str, ...]
    constraint_failures: tuple[str, ...]
    allowed: bool
    reason: str
    tenant_scope: str
    authentication_mode: str
    access_policy_version: str
    access_policy_source: str
    oidc_issuer: str | None
    oidc_subject: str | None


@dataclass(frozen=True, slots=True)
class AuthorizationDecisionAuditSearchResult:
    """Bounded search result over persisted authorization decisions."""

    total_matches: int
    records: tuple[AuthorizationDecisionAuditRecord, ...]


class AuthorizationDecisionAuditService:
    """Persist and search authorization decisions for operator audit workflows."""

    def __init__(self, db: DatabaseRuntime) -> None:
        self._db = db

    async def record_decision(
        self,
        *,
        path: str,
        method: str,
        resource_scope: str,
        actor_id: str,
        actor_type: str,
        tenant_id: str,
        target_tenant_id: str,
        required_permissions: tuple[str, ...],
        matched_permissions: tuple[str, ...],
        missing_permissions: tuple[str, ...],
        constrained_permissions: tuple[str, ...],
        constraint_failures: tuple[str, ...],
        allowed: bool,
        reason: str,
        tenant_scope: str,
        authentication_mode: str,
        access_policy_version: str,
        access_policy_source: str,
        oidc_issuer: str | None = None,
        oidc_subject: str | None = None,
    ) -> None:
        """Persist one authorization decision row."""

        async with self._db.session() as session:
            session.add(
                AuthorizationDecisionAuditORM(
                    path=path,
                    method=method,
                    resource_scope=resource_scope,
                    actor_id=actor_id,
                    actor_type=actor_type,
                    tenant_id=tenant_id,
                    target_tenant_id=target_tenant_id,
                    required_permissions=list(required_permissions),
                    matched_permissions=list(matched_permissions),
                    missing_permissions=list(missing_permissions),
                    constrained_permissions=list(constrained_permissions),
                    constraint_failures=list(constraint_failures),
                    allowed=allowed,
                    reason=reason,
                    tenant_scope=tenant_scope,
                    authentication_mode=authentication_mode,
                    access_policy_version=access_policy_version,
                    access_policy_source=access_policy_source,
                    oidc_issuer=oidc_issuer,
                    oidc_subject=oidc_subject,
                    occurred_at=datetime.now(UTC),
                )
            )
            await session.commit()

    async def search(
        self,
        *,
        limit: int = 20,
        actor_id: str | None = None,
        tenant_id: str | None = None,
        target_tenant_id: str | None = None,
        permission: str | None = None,
        allowed: bool | None = None,
        reason: str | None = None,
        path_prefix: str | None = None,
    ) -> AuthorizationDecisionAuditSearchResult:
        """Return bounded operator search results over persisted decision history."""

        predicates = []
        if actor_id:
            predicates.append(AuthorizationDecisionAuditORM.actor_id == actor_id.strip())
        if tenant_id:
            predicates.append(AuthorizationDecisionAuditORM.tenant_id == tenant_id.strip())
        if target_tenant_id:
            predicates.append(
                AuthorizationDecisionAuditORM.target_tenant_id == target_tenant_id.strip()
            )
        if permission:
            normalized_permission = permission.strip().lower()
            predicates.append(
                AuthorizationDecisionAuditORM.required_permissions.contains(
                    [normalized_permission]
                )
            )
        if allowed is not None:
            predicates.append(AuthorizationDecisionAuditORM.allowed.is_(allowed))
        if reason:
            predicates.append(AuthorizationDecisionAuditORM.reason == reason.strip())
        if path_prefix:
            predicates.append(
                AuthorizationDecisionAuditORM.path.like(f"{path_prefix.strip()}%")
            )

        async with self._db.session() as session:
            count_query = select(func.count()).select_from(AuthorizationDecisionAuditORM)
            if predicates:
                count_query = count_query.where(*predicates)
            total_matches = int((await session.execute(count_query)).scalar_one())

            query = select(AuthorizationDecisionAuditORM).order_by(
                AuthorizationDecisionAuditORM.occurred_at.desc()
            )
            if predicates:
                query = query.where(*predicates)
            query = query.limit(max(1, limit))
            rows = (await session.execute(query)).scalars().all()

        return AuthorizationDecisionAuditSearchResult(
            total_matches=total_matches,
            records=tuple(_record_from_orm(row) for row in rows),
        )


def _record_from_orm(row: AuthorizationDecisionAuditORM) -> AuthorizationDecisionAuditRecord:
    return AuthorizationDecisionAuditRecord(
        occurred_at=row.occurred_at,
        path=row.path,
        method=row.method,
        resource_scope=row.resource_scope,
        actor_id=row.actor_id,
        actor_type=row.actor_type,
        tenant_id=row.tenant_id,
        target_tenant_id=row.target_tenant_id,
        required_permissions=tuple(row.required_permissions or ()),
        matched_permissions=tuple(row.matched_permissions or ()),
        missing_permissions=tuple(row.missing_permissions or ()),
        constrained_permissions=tuple(row.constrained_permissions or ()),
        constraint_failures=tuple(row.constraint_failures or ()),
        allowed=row.allowed,
        reason=row.reason,
        tenant_scope=row.tenant_scope,
        authentication_mode=row.authentication_mode,
        access_policy_version=row.access_policy_version,
        access_policy_source=row.access_policy_source,
        oidc_issuer=row.oidc_issuer,
        oidc_subject=row.oidc_subject,
    )
