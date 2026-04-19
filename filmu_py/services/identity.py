"""Persisted security identity and tenancy primitives for authenticated requests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import select

from filmu_py.config import Settings
from filmu_py.db.models import PrincipalORM, ServiceAccountORM, TenantORM
from filmu_py.db.runtime import DatabaseRuntime


class AuthContextLike(Protocol):
    """Minimal request-auth shape needed by the security identity service."""

    @property
    def authentication_mode(self) -> str:
        raise NotImplementedError

    @property
    def api_key_id(self) -> str:
        raise NotImplementedError

    @property
    def actor_id(self) -> str:
        raise NotImplementedError

    @property
    def actor_type(self) -> str:
        raise NotImplementedError

    @property
    def actor_display_name(self) -> str | None:
        raise NotImplementedError

    @property
    def tenant_id(self) -> str:
        raise NotImplementedError

    @property
    def tenant_display_name(self) -> str | None:
        raise NotImplementedError

    @property
    def roles(self) -> tuple[str, ...]:
        raise NotImplementedError

    @property
    def scopes(self) -> tuple[str, ...]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class IdentityResolution:
    """Persisted identity summary bound to one authenticated request."""

    tenant_id: str
    tenant_status: str
    principal_key: str
    principal_type: str
    service_account_api_key_id: str | None
    roles: tuple[str, ...]
    scopes: tuple[str, ...]


class SecurityIdentityService:
    """Persist and update tenant/principal/service-account control-plane records."""

    def __init__(self, db: DatabaseRuntime) -> None:
        self._db = db

    async def bootstrap(self, settings: Settings) -> IdentityResolution:
        """Ensure the default global tenant and primary service account exist."""

        return await self.record_auth_context(
            _BootstrapAuthContext(
                authentication_mode="api_key",
                api_key_id=settings.api_key_id,
                actor_id=f"api-key:{settings.api_key_id}",
                actor_type="service",
                tenant_id="global",
                roles=("platform:admin",),
                scopes=("backend:admin",),
            )
        )

    async def record_auth_context(self, auth_context: AuthContextLike) -> IdentityResolution:
        """Upsert tenant, principal, and service-account rows for one auth context."""

        now = datetime.now(UTC)
        async with self._db.session() as session:
            tenant = await session.get(TenantORM, auth_context.tenant_id)
            if tenant is None:
                tenant = TenantORM(
                    id=auth_context.tenant_id,
                    slug=auth_context.tenant_id.lower(),
                    display_name=auth_context.tenant_display_name
                    or _display_name_for(auth_context.tenant_id),
                    kind="system" if auth_context.tenant_id == "global" else "tenant",
                    status="active",
                )
                session.add(tenant)
            elif auth_context.tenant_display_name:
                tenant.display_name = auth_context.tenant_display_name
            tenant_status = tenant.status

            principal = (
                await session.execute(
                    select(PrincipalORM).where(PrincipalORM.principal_key == auth_context.actor_id)
                )
            ).scalar_one_or_none()
            if principal is None:
                principal = PrincipalORM(
                    tenant_id=tenant.id,
                    principal_key=auth_context.actor_id,
                    principal_type=auth_context.actor_type,
                    authentication_mode=auth_context.authentication_mode,
                    display_name=auth_context.actor_display_name
                    or _display_name_for(auth_context.actor_id),
                    roles=list(auth_context.roles),
                    scopes=list(auth_context.scopes),
                    status="active",
                    last_authenticated_at=now,
                )
                session.add(principal)
                await session.flush()
            else:
                principal.tenant_id = tenant.id
                principal.principal_type = auth_context.actor_type
                principal.authentication_mode = auth_context.authentication_mode
                principal.display_name = (
                    auth_context.actor_display_name
                    or principal.display_name
                    or _display_name_for(auth_context.actor_id)
                )
                principal.roles = list(auth_context.roles)
                principal.scopes = list(auth_context.scopes)
                principal.status = "active"
                principal.last_authenticated_at = now

            service_account_key: str | None = None
            if auth_context.actor_type == "service":
                service_account = (
                    await session.execute(
                        select(ServiceAccountORM).where(
                            ServiceAccountORM.api_key_id == auth_context.api_key_id
                        )
                    )
                ).scalar_one_or_none()
                if service_account is None:
                    service_account = ServiceAccountORM(
                        principal_id=principal.id,
                        api_key_id=auth_context.api_key_id,
                        status="active",
                        description=f"Service account for {auth_context.api_key_id}",
                        last_authenticated_at=now,
                    )
                    session.add(service_account)
                else:
                    service_account.principal_id = principal.id
                    service_account.status = "active"
                    service_account.last_authenticated_at = now
                service_account_key = auth_context.api_key_id

            await session.commit()

        return IdentityResolution(
            tenant_id=auth_context.tenant_id,
            tenant_status=tenant_status,
            principal_key=auth_context.actor_id,
            principal_type=auth_context.actor_type,
            service_account_api_key_id=service_account_key,
            roles=auth_context.roles,
            scopes=auth_context.scopes,
        )

    async def rotate_service_account_api_key_id(
        self,
        *,
        auth_context: AuthContextLike,
        new_api_key_id: str,
    ) -> IdentityResolution:
        """Persist a rotated API-key identifier for the authenticated service actor."""

        if auth_context.actor_type != "service":
            return await self.record_auth_context(auth_context)

        now = datetime.now(UTC)
        async with self._db.session() as session:
            tenant = await session.get(TenantORM, auth_context.tenant_id)
            if tenant is None:
                tenant = TenantORM(
                    id=auth_context.tenant_id,
                    slug=auth_context.tenant_id.lower(),
                    display_name=auth_context.tenant_display_name
                    or _display_name_for(auth_context.tenant_id),
                    kind="system" if auth_context.tenant_id == "global" else "tenant",
                    status="active",
                )
                session.add(tenant)
            elif auth_context.tenant_display_name:
                tenant.display_name = auth_context.tenant_display_name
            tenant_status = tenant.status

            principal = (
                await session.execute(
                    select(PrincipalORM).where(PrincipalORM.principal_key == auth_context.actor_id)
                )
            ).scalar_one_or_none()
            if principal is None:
                await session.commit()
                return await self.record_auth_context(
                    _BootstrapAuthContext(
                        authentication_mode=auth_context.authentication_mode,
                        api_key_id=new_api_key_id,
                        actor_id=auth_context.actor_id,
                        actor_type=auth_context.actor_type,
                        tenant_id=auth_context.tenant_id,
                        roles=auth_context.roles,
                        scopes=auth_context.scopes,
                    )
                )

            service_account = (
                await session.execute(
                    select(ServiceAccountORM).where(ServiceAccountORM.principal_id == principal.id)
                )
            ).scalar_one_or_none()
            if service_account is None:
                service_account = ServiceAccountORM(
                    principal_id=principal.id,
                    api_key_id=new_api_key_id,
                    status="active",
                    description=f"Service account for {new_api_key_id}",
                    last_authenticated_at=now,
                )
                session.add(service_account)
            else:
                service_account.api_key_id = new_api_key_id
                service_account.status = "active"
                service_account.last_authenticated_at = now

            principal.last_authenticated_at = now
            await session.commit()

        return IdentityResolution(
            tenant_id=auth_context.tenant_id,
            tenant_status=tenant_status,
            principal_key=auth_context.actor_id,
            principal_type=auth_context.actor_type,
            service_account_api_key_id=new_api_key_id,
            roles=auth_context.roles,
            scopes=auth_context.scopes,
        )


@dataclass(frozen=True, slots=True)
class _BootstrapAuthContext:
    """Internal auth-context record used for startup bootstrap."""

    authentication_mode: str
    api_key_id: str
    actor_id: str
    actor_type: str
    tenant_id: str
    roles: tuple[str, ...]
    scopes: tuple[str, ...]
    actor_display_name: str | None = None
    tenant_display_name: str | None = None


def _display_name_for(value: str) -> str:
    """Return a readable display name for persisted identity records."""

    normalized = value.replace(":", " ").replace("-", " ").replace("_", " ").strip()
    return normalized.title() if normalized else value
