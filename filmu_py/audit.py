"""Audit logging helpers for privileged API actions."""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import Request

from filmu_py.api.deps import get_auth_context

logger = structlog.get_logger("filmu.audit")


def audit_action(
    request: Request,
    *,
    action: str,
    target: str,
    outcome: str = "success",
    details: dict[str, Any] | None = None,
) -> None:
    """Emit one structured audit record using the current request auth context."""

    auth = get_auth_context(request)
    logger.info(
        "audit.action",
        action=action,
        target=target,
        outcome=outcome,
        authentication_mode=auth.authentication_mode,
        api_key_id=auth.api_key_id,
        actor_id=auth.actor_id,
        actor_type=auth.actor_type,
        tenant_id=auth.tenant_id,
        actor_roles=list(auth.roles),
        actor_scopes=list(auth.scopes),
        details=details or {},
    )
