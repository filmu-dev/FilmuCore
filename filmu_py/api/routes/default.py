"""Default compatibility routes."""

import asyncio
import json
import re
from datetime import UTC, datetime
from secrets import token_hex
from typing import Annotated, Any, Literal, cast
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import SecretStr

from filmu_py.api.deps import get_auth_context, require_permissions
from filmu_py.audit import audit_action
from filmu_py.authz import evaluate_permissions, permission_constraints_from_mapping
from filmu_py.config import set_runtime_settings
from filmu_py.core.metadata_reindex_status import MetadataReindexStatusStore
from filmu_py.core.queue_status import QueueStatusReader
from filmu_py.core.runtime_lifecycle import RuntimeLifecycleHealth, RuntimeLifecyclePhase
from filmu_py.services.debrid import DownloaderAccountService
from filmu_py.services.settings_service import save_settings

from ..models import (
    AccessPolicyAuditAlertResponse,
    AccessPolicyAuditEntryResponse,
    AccessPolicyAuditResponse,
    AccessPolicyRevisionApprovalRequest,
    AccessPolicyRevisionListResponse,
    AccessPolicyRevisionResponse,
    AccessPolicyRevisionWriteRequest,
    ApiKeyRotationResponse,
    AuthContextResponse,
    AuthPolicyDecisionResponse,
    AuthPolicyResponse,
    CalendarItemResponse,
    CalendarReleaseDataResponse,
    CalendarResponse,
    ControlPlaneSubscriberResponse,
    EnterpriseOperationsGovernanceResponse,
    EnterpriseOperationsSliceResponse,
    HealthResponse,
    LogsResponse,
    MessageResponse,
    MetadataReindexHistoryPointResponse,
    MetadataReindexHistoryResponse,
    MetadataReindexHistorySummaryResponse,
    MetadataReindexStatusResponse,
    PluginCapabilityStatusResponse,
    PluginEventStatusResponse,
    PluginGovernanceOverrideResponse,
    PluginGovernanceOverrideWriteRequest,
    PluginGovernanceResponse,
    PluginGovernanceSummaryResponse,
    QueueAlertResponse,
    QueueStatusHistoryPointResponse,
    QueueStatusHistoryResponse,
    QueueStatusHistorySummaryResponse,
    QueueStatusResponse,
    RuntimeLifecycleResponse,
    RuntimeLifecycleTransitionResponse,
    StatsMediaYearRelease,
    StatsResponse,
    TenantQuotaPolicyResponse,
)
from .stream import _playback_gate_governance_snapshot, _vfs_runtime_governance_snapshot

router = APIRouter(tags=["default"])
_MAX_API_KEY_ID_LENGTH = 128
_API_KEY_ID_SUFFIX_LENGTH = 12

API_KEY_ROTATION_WARNING = (
    "Update BACKEND_API_KEY in your frontend environment and restart the frontend "
    "server before your next request, or all API calls will fail."
)
_AUTH_POLICY_PROBES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("library_read", ("library:read",)),
    ("item_write", ("library:write",)),
    ("playback_operate", ("playback:operate",)),
    ("settings_write", ("settings:write",)),
    ("plugin_governance_write", ("settings:write",)),
    ("policy_write", ("settings:write",)),
    ("policy_approve", ("security:policy.approve",)),
    ("api_key_rotate", ("security:apikey.rotate",)),
)

_AUTH_POLICY_PROBE_PATHS: dict[str, str] = {
    "library_read": "/api/v1/items",
    "item_write": "/api/v1/items/reset",
    "playback_operate": "/api/v1/stream",
    "settings_write": "/api/v1/settings",
    "plugin_governance_write": "/api/v1/plugins/governance/example",
    "policy_write": "/api/v1/auth/policy/revisions",
    "policy_approve": "/api/v1/auth/policy/revisions/probe-version/approve",
    "api_key_rotate": "/api/v1/generateapikey",
}

_AUTH_POLICY_PROBE_RESOURCE_SCOPES: dict[str, str] = {
    "library_read": "items",
    "item_write": "items",
    "playback_operate": "stream",
    "settings_write": "settings",
    "plugin_governance_write": "plugin_governance",
    "policy_write": "access_policy",
    "policy_approve": "access_policy",
    "api_key_rotate": "settings",
}


def _plugin_load_report_maps(request: Request) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return plugin-load successes and failures keyed by plugin name when available."""

    report = getattr(request.app.state, "plugin_load_report", None)
    loaded: dict[str, Any] = {}
    failed: dict[str, Any] = {}
    if report is None:
        return loaded, failed

    for success in getattr(report, "loaded", []):
        plugin_name = getattr(success, "plugin_name", None)
        if isinstance(plugin_name, str) and plugin_name:
            loaded[plugin_name] = success
    for failure in getattr(report, "failed", []):
        plugin_name = getattr(failure, "plugin_name", None)
        if isinstance(plugin_name, str) and plugin_name:
            failed[plugin_name] = failure
    return loaded, failed


def _plugin_runtime_health(
    plugin_name: str,
    resources: Any,
) -> tuple[bool, bool | None, list[str]]:
    """Return operator-facing readiness for built-in/runtime-managed plugins."""

    warnings: list[str] = []
    configured: bool | None = None

    if plugin_name == "stremthru":
        stremthru = resources.settings.downloaders.stremthru
        configured_url = str(getattr(stremthru, "base_url", getattr(stremthru, "url", ""))).strip()
        configured = bool(
            stremthru.enabled
            and stremthru.token.strip()
            and configured_url
        )
        if stremthru.enabled and not stremthru.token.strip():
            warnings.append("enabled but token is missing")
        if stremthru.enabled and not configured_url:
            warnings.append("enabled but url is missing")
        if stremthru.token.strip() and not stremthru.enabled:
            warnings.append("token is configured but the plugin is disabled")
        return configured, configured, warnings

    return True, configured, warnings


def _plugin_signature_fields(*, manifest: Any, success: Any) -> dict[str, Any]:
    """Return operator-facing trust-store verification details when available."""

    return {
        "signature_present": bool(
            getattr(success, "signature_present", False) or (manifest and manifest.signature)
        ),
        "signature_verified": bool(getattr(success, "signature_verified", False)),
        "signature_verification_reason": getattr(success, "signature_verification_reason", None),
        "trust_policy_decision": getattr(success, "trust_policy_decision", None),
        "trust_store_source": getattr(success, "trust_store_source", None),
    }


def _plugin_recommended_actions(
    plugin: PluginCapabilityStatusResponse,
) -> tuple[str, ...]:
    """Return stable operator actions for one plugin governance row."""

    actions: set[str] = set()
    if plugin.status == "load_failed":
        actions.add("investigate_plugin_load_failure")
    if plugin.quarantined or plugin.quarantine_recommended:
        actions.add("review_plugin_quarantine")
    if plugin.publisher_policy_decision in {"rejected", "untrusted"}:
        actions.add("review_publisher_policy")
    if plugin.trust_policy_decision in {"rejected", "untrusted"}:
        actions.add("review_signature_trust")
    if (
        plugin.release_channel != "builtin"
        and plugin.source != "builtin"
        and not plugin.signature_present
    ):
        actions.add("require_external_plugin_signature")
    if not plugin.ready:
        actions.add("resolve_plugin_readiness")
    return tuple(sorted(actions))


def _plugin_governance_summary(
    plugins: list[PluginCapabilityStatusResponse],
    *,
    runtime_policy: Any,
) -> PluginGovernanceSummaryResponse:
    """Return a bounded plugin trust/isolation rollup for operators."""

    sandbox_profile_counts: dict[str, int] = {}
    tenancy_mode_counts: dict[str, int] = {}
    recommended_actions: set[str] = set()
    non_builtin_plugins = [
        plugin
        for plugin in plugins
        if plugin.release_channel != "builtin" and plugin.source != "builtin"
    ]
    for plugin in plugins:
        sandbox_profile = plugin.sandbox_profile or "unspecified"
        sandbox_profile_counts[sandbox_profile] = sandbox_profile_counts.get(sandbox_profile, 0) + 1
        tenancy_mode = plugin.tenancy_mode or "unspecified"
        tenancy_mode_counts[tenancy_mode] = tenancy_mode_counts.get(tenancy_mode, 0) + 1
        recommended_actions.update(_plugin_recommended_actions(plugin))

    runtime_isolation_ready = (
        runtime_policy.health_rollup_enabled
        and runtime_policy.enforcement_mode == "isolated_runtime_required"
        and runtime_policy.require_strict_signatures
        and runtime_policy.require_source_digest
        and bool(runtime_policy.proof_refs)
        and all(plugin.ready for plugin in plugins)
        and not any(plugin.quarantined or plugin.quarantine_recommended for plugin in plugins)
        and not any(plugin.status == "load_failed" for plugin in plugins)
        and not any(
            plugin.publisher_policy_decision in {"rejected", "untrusted"}
            or plugin.trust_policy_decision in {"rejected", "untrusted"}
            for plugin in plugins
        )
        and all(
            (plugin.sandbox_profile in runtime_policy.allowed_non_builtin_sandbox_profiles)
            and (plugin.tenancy_mode in runtime_policy.allowed_non_builtin_tenancy_modes)
            and (not runtime_policy.require_source_digest or bool(plugin.source_sha256))
            and (not runtime_policy.require_strict_signatures or plugin.signature_verified)
            for plugin in non_builtin_plugins
        )
    )

    return PluginGovernanceSummaryResponse(
        total_plugins=len(plugins),
        loaded_plugins=sum(1 for plugin in plugins if plugin.status == "loaded"),
        load_failed_plugins=sum(1 for plugin in plugins if plugin.status == "load_failed"),
        ready_plugins=sum(1 for plugin in plugins if plugin.ready),
        unready_plugins=sum(1 for plugin in plugins if not plugin.ready),
        healthy_plugins=sum(1 for plugin in plugins if plugin.ready and not plugin.warnings),
        degraded_plugins=sum(1 for plugin in plugins if (not plugin.ready) or bool(plugin.warnings)),
        non_builtin_plugins=len(non_builtin_plugins),
        isolated_non_builtin_plugins=sum(
            1 for plugin in non_builtin_plugins if plugin.sandbox_profile == "isolated"
        ),
        quarantined_plugins=sum(1 for plugin in plugins if plugin.quarantined),
        quarantine_recommended_plugins=sum(1 for plugin in plugins if plugin.quarantine_recommended),
        unsigned_external_plugins=sum(
            1
            for plugin in plugins
            if plugin.release_channel != "builtin"
            and plugin.source != "builtin"
            and not plugin.signature_present
        ),
        unverified_signature_plugins=sum(
            1 for plugin in plugins if plugin.signature_present and not plugin.signature_verified
        ),
        publisher_policy_rejections=sum(
            1
            for plugin in plugins
            if plugin.publisher_policy_decision in {"rejected", "untrusted"}
        ),
        trust_policy_rejections=sum(
            1 for plugin in plugins if plugin.trust_policy_decision in {"rejected", "untrusted"}
        ),
        sandbox_profile_counts=dict(sorted(sandbox_profile_counts.items())),
        tenancy_mode_counts=dict(sorted(tenancy_mode_counts.items())),
        runtime_policy_mode=runtime_policy.enforcement_mode,
        runtime_isolation_ready=runtime_isolation_ready,
        recommended_actions=sorted(recommended_actions),
        remaining_gaps=(
            []
            if runtime_isolation_ready
            else [
                "non-builtin plugin runtime isolation exit gates are not fully satisfied",
                "operator quarantine/revocation still depends on runtime policy enforcement",
                "external plugin artifact provenance or signature verification is still incomplete",
            ]
        ),
    )


def _next_api_key_id(auth_context: Any) -> str:
    """Return a non-secret service-account key identifier for rotations."""

    raw_actor_id = str(getattr(auth_context, "actor_id", "service"))
    normalized_prefix = re.sub(r"[^A-Za-z0-9._-]+", "-", raw_actor_id).strip("-._") or "service"
    max_prefix_length = _MAX_API_KEY_ID_LENGTH - _API_KEY_ID_SUFFIX_LENGTH - 1
    bounded_prefix = normalized_prefix[:max_prefix_length].rstrip("-._") or "service"
    return f"{bounded_prefix}-{uuid4().hex[:_API_KEY_ID_SUFFIX_LENGTH]}"


def _generate_api_key() -> str:
    """Return a strong API key candidate for compatibility-driven admin flows.

    The current python backend does not yet support persisted settings mutation, so
    this helper generates a replacement candidate without applying it to the live
    process configuration. That keeps the frontend settings UX unblocked without
    invalidating the currently configured BFF/backend trust relationship mid-session.
    """

    return token_hex(32)


def _summarize_queue_history(
    history: list[QueueStatusHistoryPointResponse],
) -> QueueStatusHistorySummaryResponse:
    """Return operator rollups for one bounded queue-history response."""

    latest = history[0].alert_level if history else "ok"
    return QueueStatusHistorySummaryResponse(
        points=len(history),
        latest_alert_level=latest,
        critical_points=sum(1 for item in history if item.alert_level == "critical"),
        warning_points=sum(1 for item in history if item.alert_level == "warning"),
        max_ready_jobs=max((item.ready_jobs for item in history), default=0),
        max_dead_letter_jobs=max((item.dead_letter_jobs for item in history), default=0),
        max_oldest_ready_age_seconds=max(
            (
                item.oldest_ready_age_seconds
                for item in history
                if item.oldest_ready_age_seconds is not None
            ),
            default=None,
        ),
        latest_dead_letter_reason_counts=(
            dict(history[0].dead_letter_reason_counts) if history else {}
        ),
    )


def _summarize_metadata_reindex_history(
    history: list[MetadataReindexHistoryPointResponse],
) -> MetadataReindexHistorySummaryResponse:
    """Return operator rollups for one bounded metadata reindex history response."""

    latest_outcome = history[0].outcome if history else "ok"
    return MetadataReindexHistorySummaryResponse(
        points=len(history),
        latest_outcome=latest_outcome,
        critical_points=sum(1 for item in history if item.outcome == "critical"),
        warning_points=sum(1 for item in history if item.outcome == "warning"),
        total_processed=sum(item.processed for item in history),
        total_queued=sum(item.queued for item in history),
        total_reconciled=sum(item.reconciled for item in history),
        total_skipped_active=sum(item.skipped_active for item in history),
        total_failed=sum(item.failed for item in history),
        max_processed=max((item.processed for item in history), default=0),
        max_failed=max((item.failed for item in history), default=0),
        latest_run_failed=history[0].run_failed if history else False,
        latest_error=history[0].last_error if history else None,
    )


def _runtime_lifecycle_response(request: Request) -> RuntimeLifecycleResponse:
    """Return the explicit runtime lifecycle state and bounded transition history."""

    resources = request.app.state.resources
    snapshot = resources.runtime_lifecycle.snapshot()
    return RuntimeLifecycleResponse(
        phase=snapshot.phase,
        health=snapshot.health,
        detail=snapshot.detail,
        updated_at=snapshot.updated_at.isoformat(),
        transitions=[
            RuntimeLifecycleTransitionResponse(
                phase=transition.phase,
                health=transition.health,
                detail=transition.detail,
                at=transition.at.isoformat(),
            )
            for transition in snapshot.transitions
        ],
    )


def _resolve_target_tenant_id(
    *,
    auth_context: Any,
    requested_tenant_id: str | None,
    required_permissions: tuple[str, ...] = (),
) -> str:
    """Return the allowed tenant scope for one operator request."""

    normalized_tenant_id = (requested_tenant_id or auth_context.tenant_id).strip()
    if normalized_tenant_id == auth_context.tenant_id:
        return normalized_tenant_id
    decision = evaluate_permissions(
        granted_permissions=auth_context.effective_permissions,
        required_permissions=required_permissions,
        actor_tenant_id=auth_context.tenant_id,
        target_tenant_id=normalized_tenant_id,
        authorized_tenant_ids=auth_context.authorized_tenant_ids,
    )
    if not decision.allowed:
        detail = (
            f"Authorization denied ({decision.reason}) for tenant "
            f"'{decision.target_tenant_id}'"
        )
        raise PermissionError(detail)
    return normalized_tenant_id


def _resolved_access_policy_snapshot(request: Request) -> Any:
    """Return the active access-policy snapshot or the runtime settings baseline."""

    resources = request.app.state.resources
    snapshot = resources.access_policy_snapshot
    if snapshot is not None:
        return snapshot
    return resources.settings.access_policy


def _wave2_constraint_coverage(
    permission_constraints: dict[str, dict[str, list[str]]],
) -> tuple[bool, list[str]]:
    """Return whether Wave 2 resource-scope ABAC coverage is present."""

    required = {
        "library:write": ("route_prefixes", "resource_scopes"),
        "playback:operate": ("route_prefixes", "resource_scopes"),
        "settings:write": ("route_prefixes", "resource_scopes"),
        "security:policy.approve": ("route_prefixes", "resource_scopes"),
    }
    gaps: list[str] = []
    for permission, required_fields in required.items():
        payload = permission_constraints.get(permission, {})
        for field_name in required_fields:
            if not payload.get(field_name):
                gaps.append(f"{permission}:{field_name}")
    return (not gaps), gaps


def _oidc_rollout_snapshot(settings: Any) -> tuple[str, bool, list[str], list[str]]:
    """Return operator-facing OIDC rollout posture derived from configuration."""

    evidence = [
        f"oidc_enabled={settings.oidc.enabled}",
        f"oidc_rollout_stage={settings.oidc.rollout_stage}",
        f"oidc_rollout_evidence_count={len(settings.oidc.rollout_evidence_refs)}",
        f"oidc_subject_mapping_ready={settings.oidc.subject_mapping_ready}",
        f"oidc_issuer_configured={bool(settings.oidc.issuer)}",
        f"oidc_audience_configured={bool(settings.oidc.audience)}",
        f"oidc_jwks_configured={bool(settings.oidc.jwks_url or settings.oidc.jwks_json)}",
        (
            "oidc_claim_mapping_configured="
            f"{all(bool(value) for value in (settings.oidc.actor_id_claim, settings.oidc.tenant_id_claim, settings.oidc.roles_claim))}"
        ),
        f"oidc_allow_api_key_fallback={settings.oidc.allow_api_key_fallback}",
    ]
    remaining_gaps: list[str] = []
    configuration_complete = bool(
        settings.oidc.enabled
        and settings.oidc.issuer
        and settings.oidc.audience
        and (settings.oidc.jwks_url or settings.oidc.jwks_json)
        and settings.oidc.actor_id_claim
        and settings.oidc.tenant_id_claim
        and settings.oidc.roles_claim
    )
    if not settings.oidc.enabled:
        remaining_gaps.append("OIDC/SSO validation is disabled for this environment")
        return "blocked", configuration_complete, evidence, remaining_gaps
    if not configuration_complete:
        remaining_gaps.append("OIDC is enabled but issuer/audience/JWKS/claim mapping is incomplete")
        return "partial", configuration_complete, evidence, remaining_gaps
    if settings.oidc.rollout_stage != "enforced":
        remaining_gaps.append("OIDC rollout is not yet in enforced mode for this environment")
        return "partial", configuration_complete, evidence, remaining_gaps
    if not settings.oidc.subject_mapping_ready:
        remaining_gaps.append("OIDC subject-to-application mapping is not yet marked ready")
        return "partial", configuration_complete, evidence, remaining_gaps
    if not settings.oidc.rollout_evidence_refs:
        remaining_gaps.append("OIDC rollout evidence references have not been recorded")
        return "partial", configuration_complete, evidence, remaining_gaps
    if settings.oidc.allow_api_key_fallback:
        remaining_gaps.append("API-key fallback remains enabled for OIDC traffic")
        return "partial", configuration_complete, evidence, remaining_gaps
    return "ready", configuration_complete, evidence, remaining_gaps


def _auth_policy_decisions(
    auth_context: Any,
    *,
    permission_constraints: dict[str, dict[str, list[str]]],
) -> list[AuthPolicyDecisionResponse]:
    """Return standard authorization probes for the current actor."""

    responses: list[AuthPolicyDecisionResponse] = []
    resolved_constraints = permission_constraints_from_mapping(permission_constraints)
    for name, required_permissions in _AUTH_POLICY_PROBES:
        decision = evaluate_permissions(
            granted_permissions=auth_context.effective_permissions,
            required_permissions=required_permissions,
            actor_tenant_id=auth_context.tenant_id,
            target_tenant_id=auth_context.tenant_id,
            authorized_tenant_ids=auth_context.authorized_tenant_ids,
            actor_type=auth_context.actor_type,
            authentication_mode=auth_context.authentication_mode,
            request_path=_AUTH_POLICY_PROBE_PATHS.get(name),
            resource_scope=_AUTH_POLICY_PROBE_RESOURCE_SCOPES.get(name),
            permission_constraints=resolved_constraints,
        )
        responses.append(
            AuthPolicyDecisionResponse(
                name=name,
                allowed=decision.allowed,
                reason=decision.reason,
                required_permissions=list(required_permissions),
                matched_permissions=list(decision.matched_permissions),
                missing_permissions=list(decision.missing_permissions),
                constrained_permissions=list(decision.constrained_permissions),
                constraint_failures=list(decision.constraint_failures),
                target_tenant_id=decision.target_tenant_id,
                tenant_scope=decision.tenant_scope,
            )
        )
    return responses


def _access_policy_revision_response(record: Any) -> AccessPolicyRevisionResponse:
    """Convert one service-layer revision record into an API response row."""

    return AccessPolicyRevisionResponse(
        version=record.version,
        source=record.source,
        approval_status=record.approval_status,
        proposed_by=record.proposed_by,
        approved_by=record.approved_by,
        approved_at=record.approved_at.isoformat() if record.approved_at is not None else None,
        approval_notes=record.approval_notes,
        is_active=record.is_active,
        activated_at=record.activated_at.isoformat(),
        created_at=record.created_at.isoformat(),
        updated_at=record.updated_at.isoformat(),
        role_grants={role: list(permissions) for role, permissions in sorted(record.role_grants.items())},
        principal_roles={
            principal: list(roles) for principal, roles in sorted(record.principal_roles.items())
        },
        principal_scopes={
            principal: list(scopes)
            for principal, scopes in sorted(record.principal_scopes.items())
        },
        principal_tenant_grants={
            principal: list(tenants)
            for principal, tenants in sorted(record.principal_tenant_grants.items())
        },
        permission_constraints={
            permission: {
                field: list(values)
                for field, values in sorted(constraints.items())
            }
            for permission, constraints in sorted(record.permission_constraints.items())
        },
        audit_decisions=record.audit_decisions,
        alerting_enabled=record.alerting_enabled,
        repeated_denial_warning_threshold=record.repeated_denial_warning_threshold,
        repeated_denial_critical_threshold=record.repeated_denial_critical_threshold,
    )


def _plugin_override_response(record: Any) -> PluginGovernanceOverrideResponse:
    """Convert one persisted plugin governance override into an API response row."""

    return PluginGovernanceOverrideResponse(
        plugin_name=record.plugin_name,
        state=record.state,
        reason=record.reason,
        notes=record.notes,
        updated_by=record.updated_by,
        created_at=record.created_at.isoformat(),
        updated_at=record.updated_at.isoformat(),
    )


def _actor_key(auth_context: Any) -> str:
    """Return a stable operator identity string for persisted governance actions."""

    principal_key = getattr(auth_context, "principal_key", None)
    if isinstance(principal_key, str) and principal_key.strip():
        return principal_key.strip()
    actor_id = str(getattr(auth_context, "actor_id", "operator")).strip() or "operator"
    tenant_id = str(getattr(auth_context, "tenant_id", "global")).strip() or "global"
    return f"{tenant_id}:{actor_id}"

def _authorization_audit_summary(record: Any) -> str:
    """Return a stable human-readable summary for one audit row."""

    outcome = "allowed" if record.allowed else "denied"
    permissions = ",".join(record.required_permissions) if record.required_permissions else "none"
    return (
        f"{record.occurred_at.isoformat()} {outcome} {record.method} {record.path} "
        f"actor={record.actor_id} tenant={record.tenant_id}->{record.target_tenant_id} "
        f"reason={record.reason} permissions={permissions}"
    )


def _authorization_audit_alerts(
    records: list[Any],
    *,
    alerting_enabled: bool,
    repeated_denial_warning_threshold: int,
    repeated_denial_critical_threshold: int,
) -> list[AccessPolicyAuditAlertResponse]:
    """Return bounded policy-alert candidates from audit search results."""

    if not alerting_enabled:
        return []

    warning_threshold = max(1, repeated_denial_warning_threshold)
    critical_threshold = max(warning_threshold, repeated_denial_critical_threshold)

    repeated_denials: dict[tuple[str, str, str], int] = {}
    for record in records:
        if record.allowed:
            continue
        key = (record.actor_id, record.reason, record.path)
        repeated_denials[key] = repeated_denials.get(key, 0) + 1

    alerts = [
        AccessPolicyAuditAlertResponse(
            code="repeated_denials",
            severity="warning" if count < critical_threshold else "critical",
            count=count,
            message=(
                f"actor '{actor_id}' saw {count} denied authorization decisions for "
                f"{path} ({reason}) in the current result set"
            ),
        )
        for (actor_id, reason, path), count in sorted(repeated_denials.items())
        if count >= warning_threshold
    ]

    privileged_api_key_usage = sum(
        1
        for record in records
        if record.allowed
        and record.authentication_mode == "api_key"
        and record.resource_scope in {"access_policy", "plugin_governance", "settings", "operations"}
    )
    if privileged_api_key_usage >= warning_threshold:
        alerts.append(
            AccessPolicyAuditAlertResponse(
                code="privileged_api_key_usage",
                severity=(
                    "warning"
                    if privileged_api_key_usage < critical_threshold
                    else "critical"
                ),
                count=privileged_api_key_usage,
                message=(
                    "privileged control-plane actions are still being exercised through "
                    f"API-key auth ({privileged_api_key_usage} matches in the current result set)"
                ),
            )
        )

    return alerts[:10]
def _vfs_data_plane_evidence(
    request: Request,
    *,
    runtime_governance: dict[str, int | float | str | list[str]] | None = None,
) -> list[str]:
    """Return bounded VFS runtime evidence for enterprise-governance posture."""

    resources = request.app.state.resources
    if runtime_governance is None:
        runtime_governance = _vfs_runtime_governance_snapshot()
    evidence = [
        f"vfs_catalog_server_enabled={resources.vfs_catalog_server is not None}",
        f"chunk_cache_enabled={resources.chunk_cache is not None}",
        (
            "vfs_runtime_snapshot_available="
            f"{runtime_governance['vfs_runtime_snapshot_available']}"
        ),
    ]
    if resources.chunk_cache is not None:
        evidence.append(f"chunk_cache_max_bytes={resources.chunk_cache.max_bytes()}")
    if resources.vfs_catalog_server is not None:
        snapshot = resources.vfs_catalog_server.build_governance_snapshot()
        evidence.extend(
            [
                f"catalog_watch_sessions_started={snapshot['vfs_catalog_watch_sessions_started']}",
                f"catalog_reconnect_delta_served={snapshot['vfs_catalog_reconnect_delta_served']}",
                f"catalog_refresh_attempts={snapshot['vfs_catalog_refresh_attempts']}",
                f"catalog_inline_refresh_succeeded={snapshot['vfs_catalog_inline_refresh_succeeded']}",
            ]
        )
    if cast(int, runtime_governance["vfs_runtime_snapshot_available"]) > 0:
        rollout_reasons = cast(list[str], runtime_governance["vfs_runtime_rollout_reasons"])
        evidence.extend(
            [
                (
                    "vfs_runtime_rollout_readiness="
                    f"{runtime_governance['vfs_runtime_rollout_readiness']}"
                ),
                (
                    "vfs_runtime_rollout_reasons="
                    + (",".join(rollout_reasons) if rollout_reasons else "none")
                ),
                (
                    "vfs_runtime_rollout_canary_decision="
                    f"{runtime_governance['vfs_runtime_rollout_canary_decision']}"
                ),
                (
                    "vfs_runtime_rollout_merge_gate="
                    f"{runtime_governance['vfs_runtime_rollout_merge_gate']}"
                ),
                (
                    "vfs_runtime_rollout_environment_class="
                    f"{runtime_governance['vfs_runtime_rollout_environment_class']}"
                ),
                (
                    "vfs_runtime_cache_hit_ratio="
                    f"{cast(float, runtime_governance['vfs_runtime_cache_hit_ratio']):.3f}"
                ),
                (
                    "vfs_runtime_fallback_success_ratio="
                    f"{cast(float, runtime_governance['vfs_runtime_fallback_success_ratio']):.3f}"
                ),
                (
                    "vfs_runtime_prefetch_pressure_ratio="
                    f"{cast(float, runtime_governance['vfs_runtime_prefetch_pressure_ratio']):.3f}"
                ),
                (
                    "vfs_runtime_provider_pressure_incidents="
                    f"{runtime_governance['vfs_runtime_provider_pressure_incidents']}"
                ),
                (
                    "vfs_runtime_fairness_pressure_incidents="
                    f"{runtime_governance['vfs_runtime_fairness_pressure_incidents']}"
                ),
                (
                    "vfs_runtime_mounted_reads_total="
                    f"{runtime_governance['vfs_runtime_mounted_reads_total']}"
                ),
                (
                    "vfs_runtime_mounted_reads_error="
                    f"{runtime_governance['vfs_runtime_mounted_reads_error']}"
                ),
                (
                    "vfs_runtime_handle_startup_average_duration_ms="
                    f"{runtime_governance['vfs_runtime_handle_startup_average_duration_ms']}"
                ),
                (
                    "vfs_runtime_prefetch_active_background_tasks="
                    f"{runtime_governance['vfs_runtime_prefetch_active_background_tasks']}"
                ),
            ]
        )
    return evidence


async def _enterprise_operations_governance(
    *,
    request: Request,
    plugins: list[PluginCapabilityStatusResponse],
) -> EnterpriseOperationsGovernanceResponse:
    """Return machine-readable posture for the current enterprise roadmap slices."""

    resources = request.app.state.resources
    settings = resources.settings
    auth_context = get_auth_context(request)
    policy_snapshot = _resolved_access_policy_snapshot(request)
    policy_constraints = policy_snapshot.permission_constraints
    alerting_enabled = policy_snapshot.alerting_enabled
    repeated_denial_warning_threshold = policy_snapshot.repeated_denial_warning_threshold
    repeated_denial_critical_threshold = policy_snapshot.repeated_denial_critical_threshold
    wave2_constraint_coverage_ready, wave2_constraint_gaps = _wave2_constraint_coverage(
        policy_constraints
    )
    policy_decisions = _auth_policy_decisions(
        auth_context,
        permission_constraints=policy_constraints,
    )
    playback_gate_governance = _playback_gate_governance_snapshot()
    vfs_runtime_governance = _vfs_runtime_governance_snapshot(
        playback_gate_governance=playback_gate_governance,
    )
    runtime_snapshot = resources.runtime_lifecycle.snapshot()
    queued_refresh_ready = (
        settings.stream.refresh_dispatch_mode != "queued"
        or (
            resources.arq_redis is not None
            and resources.queued_direct_playback_refresh_controller is not None
            and resources.queued_hls_failed_lease_refresh_controller is not None
            and resources.queued_hls_restricted_fallback_refresh_controller is not None
        )
    )
    heavy_stage_policy = settings.orchestration.heavy_stage_isolation
    heavy_stage_exit_ready = (
        settings.arq_enabled
        and settings.stream.refresh_dispatch_mode == "queued"
        and queued_refresh_ready
        and heavy_stage_policy.executor_mode == "process_pool_required"
        and heavy_stage_policy.max_tasks_per_child > 0
        and bool(heavy_stage_policy.proof_refs)
        and bool(settings.orchestration.queued_refresh_proof_refs)
    )
    oidc_rollout_status, oidc_configuration_complete, oidc_evidence, oidc_remaining_gaps = (
        _oidc_rollout_snapshot(settings)
    )

    identity_required_actions = [
        "configure_real_oidc_issuer_and_audience"
        if not settings.oidc.enabled
        else "monitor_oidc_validation_failures",
        "promote_operator_managed_access_policy_revisions"
        if auth_context.access_policy_source == "settings"
        else "review_access_policy_revision_history",
        "persist_authorization_decision_audit_history"
        if resources.authorization_audit_service is None
        else "monitor_repeated_authorization_denials",
    ]
    if settings.oidc.rollout_stage != "enforced":
        identity_required_actions.append("promote_oidc_rollout_to_enforced")
    if not settings.oidc.subject_mapping_ready:
        identity_required_actions.append("complete_oidc_subject_mapping_rollout")
    if not settings.oidc.rollout_evidence_refs:
        identity_required_actions.append("record_oidc_rollout_evidence")
    if not alerting_enabled:
        identity_required_actions.append("enable_access_policy_alerting")
    if wave2_constraint_gaps:
        identity_required_actions.append("expand_resource_scope_abac_constraints")
    if any(not decision.allowed for decision in policy_decisions):
        identity_required_actions.append("grant_or_document_missing_control_plane_permissions")

    plugin_override_count = 0
    if resources.plugin_governance_service is not None:
        plugin_override_count = len(await resources.plugin_governance_service.list_overrides())

    control_plane_subscriber_count = 0
    if resources.control_plane_service is not None:
        control_plane_subscriber_count = len(await resources.control_plane_service.list_subscribers())

    tenant_required_actions = [
        "define_tenant_quota_policy"
        if not settings.tenant_quotas.enabled
        else "review_tenant_quota_pressure",
        "attribute_plugin_execution_vfs_governance_and_metrics_to_tenant_id",
    ]
    if auth_context.authorization_tenant_scope == "all":
        tenant_required_actions.append("review_global_tenant_scope_for_actor")

    normalized_log_dir = settings.logging.directory.rstrip("/\\") or "logs"
    structured_log_path = f"{normalized_log_dir}/{settings.logging.structured_filename}"
    observability_policy = settings.observability
    operator_log_pipeline_ready = (
        settings.logging.enabled
        and settings.log_shipper.enabled
        and bool(settings.log_shipper.target)
        and bool(settings.log_shipper.healthcheck_url)
        and settings.otel_enabled
        and bool(settings.otel_exporter_otlp_endpoint)
        and observability_policy.environment_shipping_enabled
        and observability_policy.alerting_enabled
        and observability_policy.rust_trace_correlation_enabled
        and observability_policy.search_backend != "none"
        and bool(observability_policy.required_correlation_fields)
        and bool(observability_policy.proof_refs)
    )
    plugin_governance_summary = _plugin_governance_summary(
        plugins,
        runtime_policy=settings.plugin_runtime,
    )

    vfs_data_plane_status: Literal["ready", "partial", "blocked", "not_ready"] = "partial"
    vfs_required_actions = [
        "repeat_multi_environment_soak_and_backpressure_runs",
        "promote_rollout_readiness_thresholds_into_merge_policy",
        "expand_tenant_attribution_for_mounted_runtime_metrics",
    ]
    vfs_remaining_gaps = [
        "mounted rollout confidence still depends on repeated proof execution",
        "cache correctness and fairness are observable but not yet policy-enforced in CI",
        "tenant attribution is not yet complete across all mounted runtime counters",
    ]
    if cast(int, vfs_runtime_governance["vfs_runtime_snapshot_available"]) > 0:
        rollout_readiness = cast(str, vfs_runtime_governance["vfs_runtime_rollout_readiness"])
        rollout_next_action = cast(str, vfs_runtime_governance["vfs_runtime_rollout_next_action"])
        rollout_reasons = cast(list[str], vfs_runtime_governance["vfs_runtime_rollout_reasons"])
        canary_decision = cast(str, vfs_runtime_governance["vfs_runtime_rollout_canary_decision"])
        merge_gate = cast(str, vfs_runtime_governance["vfs_runtime_rollout_merge_gate"])
        if rollout_readiness == "blocked":
            vfs_data_plane_status = "blocked"
        elif rollout_readiness == "ready" and merge_gate == "ready":
            vfs_data_plane_status = "ready"
        if rollout_next_action and rollout_next_action not in vfs_required_actions:
            vfs_required_actions.insert(0, rollout_next_action)
        if canary_decision and canary_decision not in vfs_required_actions:
            vfs_required_actions.insert(0, canary_decision)
        if rollout_reasons:
            vfs_remaining_gaps.insert(
                0,
                "live runtime rollout reasons are present: " + ", ".join(rollout_reasons),
            )
        if merge_gate != "ready":
            vfs_remaining_gaps.insert(
                0,
                "VFS canary promotion is not yet merge-gate ready: "
                f"merge_gate={merge_gate} canary_decision={canary_decision}",
            )
    else:
        vfs_required_actions.insert(0, "configure_vfs_runtime_status_export")
        vfs_remaining_gaps.insert(
            0,
            "operations/governance cannot yet summarize live mounted rollout readiness without a runtime snapshot",
        )

    return EnterpriseOperationsGovernanceResponse(
        generated_at=datetime.now(UTC).isoformat(),
        playback_gate=EnterpriseOperationsSliceResponse(
            name="Playback Gate Promotion / Merge Policy Proof",
            status=(
                "blocked"
                if cast(str, playback_gate_governance["playback_gate_rollout_readiness"]) == "blocked"
                else (
                    "ready"
                    if cast(str, playback_gate_governance["playback_gate_rollout_readiness"])
                    == "ready"
                    else "partial"
                )
            ),
            evidence=[
                "proof:playback:gate:enterprise package entrypoint exists",
                "playback gate workflow writes github-main-policy-expected.json",
                "check_github_main_policy.ps1 can validate and now persist live policy artifacts with gh admin auth",
                (
                    "playback_gate_snapshot_available="
                    f"{playback_gate_governance['playback_gate_snapshot_available']}"
                ),
                (
                    "playback_gate_gate_mode="
                    f"{playback_gate_governance['playback_gate_gate_mode']}"
                ),
                (
                    "playback_gate_environment_class="
                    f"{playback_gate_governance['playback_gate_environment_class']}"
                ),
                (
                    "playback_gate_provider_gate_required="
                    f"{playback_gate_governance['playback_gate_provider_gate_required']}"
                ),
                (
                    "playback_gate_provider_gate_ran="
                    f"{playback_gate_governance['playback_gate_provider_gate_ran']}"
                ),
                (
                    "playback_gate_provider_parity_ready="
                    f"{playback_gate_governance['playback_gate_provider_parity_ready']}"
                ),
                (
                    "playback_gate_windows_provider_ready="
                    f"{playback_gate_governance['playback_gate_windows_provider_ready']}"
                ),
                (
                    "playback_gate_windows_soak_ready="
                    f"{playback_gate_governance['playback_gate_windows_soak_ready']}"
                ),
                (
                    "playback_gate_policy_validation_status="
                    f"{playback_gate_governance['playback_gate_policy_validation_status']}"
                ),
                (
                    "playback_gate_rollout_readiness="
                    f"{playback_gate_governance['playback_gate_rollout_readiness']}"
                ),
                (
                    "playback_gate_rollout_reasons="
                    + ",".join(cast(list[str], playback_gate_governance["playback_gate_rollout_reasons"]))
                ),
            ],
            required_actions=[
                cast(str, playback_gate_governance["playback_gate_rollout_next_action"]),
                "ensure Playback Gate / Playback Gate is a required protected-branch check",
                "retain playback/provider/windows proof artifacts as merge evidence",
            ],
            remaining_gaps=[
                "this API host still depends on a recorded admin-authenticated policy artifact to prove live branch protection",
                "playback/provider/windows proof promotion remains evidence-backed rather than assumption-backed",
            ],
        ),
        identity_authz=EnterpriseOperationsSliceResponse(
            name="Enterprise Identity / OIDC / ABAC",
            status=(
                "ready"
                if (
                    oidc_rollout_status == "ready"
                    and resources.authorization_audit_service is not None
                    and bool(policy_constraints)
                    and alerting_enabled
                    and wave2_constraint_coverage_ready
                )
                else ("blocked" if oidc_rollout_status == "blocked" else "partial")
            ),
            evidence=[
                f"authentication_mode={auth_context.authentication_mode}",
                f"authorization_tenant_scope={auth_context.authorization_tenant_scope}",
                f"oidc_validation_enabled={settings.oidc.enabled}",
                f"oidc_token_validated={auth_context.oidc_token_validated}",
                f"access_policy_version={auth_context.access_policy_version}",
                f"access_policy_source={auth_context.access_policy_source}",
                f"permission_constraint_count={len(policy_constraints)}",
                (
                    "authorization_decision_audit_persistence="
                    f"{resources.authorization_audit_service is not None}"
                ),
                f"policy_alerting_enabled={alerting_enabled}",
                (
                    "policy_alert_thresholds="
                    f"{repeated_denial_warning_threshold}/{repeated_denial_critical_threshold}"
                ),
                f"resource_scope_constraint_coverage={wave2_constraint_coverage_ready}",
                (
                    "oidc_claims_present="
                    f"{auth_context.oidc_issuer is not None and auth_context.oidc_subject is not None}"
                ),
                f"oidc_rollout_status={oidc_rollout_status}",
                f"oidc_configuration_complete={oidc_configuration_complete}",
                f"oidc_rollout_stage={settings.oidc.rollout_stage}",
                f"oidc_subject_mapping_ready={settings.oidc.subject_mapping_ready}",
                f"oidc_rollout_evidence_count={len(settings.oidc.rollout_evidence_refs)}",
                "GET /api/v1/auth/policy exposes standard authorization probes",
                "GET /api/v1/auth/policy/revisions exposes persisted policy revision inventory",
                "POST /api/v1/auth/policy/revisions/{version}/approve|reject adds approval workflow state",
                "GET /api/v1/auth/policy/audit exposes bounded audit-search history",
                *oidc_evidence,
            ],
            required_actions=identity_required_actions,
            remaining_gaps=[
                *oidc_remaining_gaps,
                *(
                    []
                    if wave2_constraint_coverage_ready
                    else [
                        "ABAC route/resource-scope coverage is still incomplete for some Wave 2 control-plane permissions: "
                        + ", ".join(wave2_constraint_gaps)
                    ]
                ),
                *(
                    []
                    if alerting_enabled
                    else ["policy alerting is disabled for the active access-policy revision"]
                ),
            ],
        ),
        tenant_boundary=EnterpriseOperationsSliceResponse(
            name="Tenant Boundary / Quotas / Attribution",
            status="partial",
            evidence=[
                f"request_tenant_id={auth_context.tenant_id}",
                f"authorized_tenant_ids={','.join(auth_context.authorized_tenant_ids)}",
                f"tenant_quota_enabled={settings.tenant_quotas.enabled}",
                f"tenant_quota_policy_version={settings.tenant_quotas.version}",
                "tenant-scoped stats and calendar authorization are enforced",
                "worker context and plugin governance expose tenant posture",
            ],
            required_actions=tenant_required_actions,
            remaining_gaps=[
                "worker enqueue and provider/playback pressure quotas still need deep route coverage",
                "plugin execution metrics are not fully tenant-attributed",
                "VFS runtime metrics are not fully tenant-attributed",
            ],
        ),
        vfs_data_plane=EnterpriseOperationsSliceResponse(
            name="FilmuVFS Enterprise Data Plane",
            status=vfs_data_plane_status,
            evidence=_vfs_data_plane_evidence(
                request,
                runtime_governance=vfs_runtime_governance,
            ),
            required_actions=vfs_required_actions,
            remaining_gaps=vfs_remaining_gaps,
        ),
        distributed_control_plane=EnterpriseOperationsSliceResponse(
            name="Distributed Control Plane",
            status=(
                "partial"
                if settings.control_plane.event_backplane == "redis_stream"
                else "not_ready"
            ),
            evidence=[
                f"EventBus backend={settings.control_plane.event_backplane}",
                f"event_stream_name={settings.control_plane.event_stream_name}",
                f"event_replay_maxlen={settings.control_plane.event_replay_maxlen}",
                f"consumer_group={settings.control_plane.consumer_group}",
                f"subscriber_ledger_rows={control_plane_subscriber_count}",
                "LogStreamBroker backend=process_local",
                f"arq_enabled={settings.arq_enabled}",
                f"queue_name={resources.arq_queue_name or settings.arq_queue_name}",
                "GET /api/v1/operations/control-plane/subscribers exposes durable replay ownership",
            ],
            required_actions=[
                "promote_redis_stream_consumer_groups_into_active_subscribers",
                "expand_failover_automation_from_resume_offsets_to node fencing",
                "document_and_exercise_node_coordination_failover_semantics",
            ],
            remaining_gaps=[
                "event replay now persists subscriber resume/heartbeat state but is not yet the only bus",
                "log streaming history is bounded per process",
                "node coordination and failover promotion are not fully automated",
            ],
        ),
        runtime_lifecycle=EnterpriseOperationsSliceResponse(
            name="Formal Runtime Lifecycle Graph",
            status=(
                "ready"
                if runtime_snapshot.phase is RuntimeLifecyclePhase.STEADY_STATE
                and runtime_snapshot.health is RuntimeLifecycleHealth.HEALTHY
                else (
                    "partial"
                    if runtime_snapshot.phase is RuntimeLifecyclePhase.STEADY_STATE
                    else "blocked"
                )
            ),
            evidence=[
                f"runtime_phase={runtime_snapshot.phase}",
                f"runtime_health={runtime_snapshot.health}",
                f"runtime_detail={runtime_snapshot.detail}",
                f"runtime_transition_count={len(runtime_snapshot.transitions)}",
                "GET /api/v1/operations/runtime exposes bounded lifecycle transition history",
            ],
            required_actions=(
                ["resolve_runtime_degraded_state"]
                if runtime_snapshot.health is RuntimeLifecycleHealth.DEGRADED
                else ["keep_runtime_transition_history_visible"]
            ),
            remaining_gaps=(
                []
                if runtime_snapshot.phase is RuntimeLifecyclePhase.STEADY_STATE
                and runtime_snapshot.health is RuntimeLifecycleHealth.HEALTHY
                else [
                    "runtime has not yet reached a healthy steady-state phase"
                    if runtime_snapshot.phase is not RuntimeLifecyclePhase.STEADY_STATE
                    else f"runtime steady state is degraded: {runtime_snapshot.detail}"
                ]
            ),
        ),
        sre_program=EnterpriseOperationsSliceResponse(
            name="SRE / Production Operations Program",
            status="partial",
            evidence=[
                "docs/OPERATIONS_PROGRAM.md defines SLOs, DR, rollback, incident, rollout, and capacity policy",
                "scripts/run_backup_restore_proof.ps1 produces restore-proof artifacts",
                "playback and VFS proof scripts provide operational evidence inputs",
                "queue and VFS status APIs expose operator readiness signals",
            ],
            required_actions=[
                "exercise_backup_restore_runbook_on_real_environment",
                "record_error_budget_review_cadence",
                "wire_canary_rollout_checks_into_release_promotion",
            ],
            remaining_gaps=[
                "SLO/error-budget enforcement is not automated",
                "DR restore proof is scriptable but not yet required in CI",
                "capacity review is policy-defined but not scheduled by automation",
            ],
        ),
        operator_log_pipeline=EnterpriseOperationsSliceResponse(
            name="Durable Operator Log Pipeline",
            status=(
                "ready"
                if operator_log_pipeline_ready
                else ("partial" if settings.logging.enabled else "blocked")
            ),
            evidence=[
                f"structured_logging_enabled={settings.logging.enabled}",
                f"structured_log_path={structured_log_path}",
                f"retention_files={settings.logging.retention_files}",
                f"otel_enabled={settings.otel_enabled}",
                f"otel_endpoint_configured={bool(settings.otel_exporter_otlp_endpoint)}",
                f"log_shipper_enabled={settings.log_shipper.enabled}",
                f"log_shipper_type={settings.log_shipper.type}",
                f"log_shipper_target_configured={bool(settings.log_shipper.target)}",
                f"log_shipper_healthcheck_configured={bool(settings.log_shipper.healthcheck_url)}",
                f"log_field_mapping_version={settings.log_shipper.field_mapping_version}",
                f"log_search_backend={observability_policy.search_backend}",
                f"observability_environment_shipping_enabled={observability_policy.environment_shipping_enabled}",
                f"observability_alerting_enabled={observability_policy.alerting_enabled}",
                f"rust_trace_correlation_enabled={observability_policy.rust_trace_correlation_enabled}",
                (
                    "correlation_fields="
                    + ",".join(observability_policy.required_correlation_fields)
                ),
                f"observability_proof_ref_count={len(observability_policy.proof_refs)}",
                "docs/OPERATOR_LOG_PIPELINE.md defines shipping/search/replay taxonomy",
            ],
            required_actions=(
                []
                if operator_log_pipeline_ready
                else [
                    "configure_log_shipper_for_structured_ndjson"
                    if not settings.log_shipper.enabled
                    else "monitor_log_shipper_health",
                    "define_search_index_mapping_and_retention_policy"
                    if not settings.log_shipper.target
                    or observability_policy.search_backend == "none"
                    else "validate_search_index_contract",
                    "configure_otlp_trace_export"
                    if not (settings.otel_enabled and settings.otel_exporter_otlp_endpoint)
                    else "verify_trace_export_in_collector",
                    "enable_environment_log_shipping"
                    if not observability_policy.environment_shipping_enabled
                    else "review_environment_log_shipping_rollout",
                    "enable_alerting_for_log_search_and_trace_pipeline"
                    if not observability_policy.alerting_enabled
                    else "review_log_pipeline_alert_thresholds",
                    "wire_rust_trace_correlation_fields"
                    if not observability_policy.rust_trace_correlation_enabled
                    else "review_cross_process_trace_correlation",
                    "record_log_pipeline_rollout_evidence"
                    if not observability_policy.proof_refs
                    else "review_log_pipeline_rollout_evidence",
                ]
            ),
            remaining_gaps=(
                []
                if operator_log_pipeline_ready
                else [
                    "shipper/search backend still requires environment provisioning even though repo config is present",
                    "cross-process trace correlation is not fully enforced",
                    "replay taxonomy now has Redis Streams baseline but needs end-to-end operations rollout",
                ]
            ),
        ),
        plugin_runtime_isolation=EnterpriseOperationsSliceResponse(
            name="Plugin Trust / Runtime Isolation",
            status="ready" if plugin_governance_summary.runtime_isolation_ready else "partial",
            evidence=[
                f"plugin_total={len(plugins)}",
                f"plugin_non_builtin={plugin_governance_summary.non_builtin_plugins}",
                f"plugin_healthy={plugin_governance_summary.healthy_plugins}",
                f"plugin_degraded={plugin_governance_summary.degraded_plugins}",
                (
                    "plugin_sandbox_profiles="
                    + ",".join(sorted({plugin.sandbox_profile or 'unspecified' for plugin in plugins}))
                ),
                (
                    "plugin_allowed_non_builtin_sandbox_profiles="
                    + ",".join(settings.plugin_runtime.allowed_non_builtin_sandbox_profiles)
                ),
                (
                    "plugin_allowed_non_builtin_tenancy_modes="
                    + ",".join(settings.plugin_runtime.allowed_non_builtin_tenancy_modes)
                ),
                f"plugin_quarantined={sum(1 for plugin in plugins if plugin.quarantined)}",
                (
                    "plugin_quarantine_recommended="
                    f"{sum(1 for plugin in plugins if plugin.quarantine_recommended)}"
                ),
                f"plugin_operator_overrides={plugin_override_count}",
                f"plugin_runtime_enforcement_mode={settings.plugin_runtime.enforcement_mode}",
                f"plugin_runtime_health_rollup_enabled={settings.plugin_runtime.health_rollup_enabled}",
                f"plugin_runtime_require_strict_signatures={settings.plugin_runtime.require_strict_signatures}",
                f"plugin_runtime_require_source_digest={settings.plugin_runtime.require_source_digest}",
                f"plugin_runtime_proof_ref_count={len(settings.plugin_runtime.proof_refs)}",
                f"plugin_runtime_exit_ready={int(plugin_governance_summary.runtime_isolation_ready)}",
                "GET /api/v1/plugins/governance summarizes signature, publisher-policy, and tenancy posture",
            ],
            required_actions=(
                []
                if plugin_governance_summary.runtime_isolation_ready
                else [
                    "enable_non_builtin_plugin_runtime_enforcement"
                    if settings.plugin_runtime.enforcement_mode == "report_only"
                    else "review_non_builtin_plugin_runtime_enforcement",
                    "require_isolated_non_builtin_plugin_sandbox_profiles"
                    if settings.plugin_runtime.enforcement_mode != "isolated_runtime_required"
                    else "review_non_builtin_plugin_health_rollups",
                    "record_plugin_runtime_isolation_evidence"
                    if not settings.plugin_runtime.proof_refs
                    else "review_plugin_runtime_isolation_evidence",
                ]
            ),
            remaining_gaps=plugin_governance_summary.remaining_gaps,
        ),
        heavy_stage_workload_isolation=EnterpriseOperationsSliceResponse(
            name="Heavy-Stage Workload Isolation",
            status=(
                "ready"
                if heavy_stage_exit_ready
                else (
                    "partial"
                    if settings.arq_enabled and queued_refresh_ready
                    else (
                        "blocked"
                        if settings.stream.refresh_dispatch_mode == "queued"
                        else "not_ready"
                    )
                )
            ),
            evidence=[
                f"arq_enabled={settings.arq_enabled}",
                f"queue_name={resources.arq_queue_name or settings.arq_queue_name}",
                (
                    "worker stages include index_item, scrape_item, parse_scrape_results, "
                    "rank_streams, debrid_item, finalize_item, and queued playback refresh jobs"
                ),
                "tenant worker enqueue quotas can deny downstream stage fan-out",
                "index_item, parse_scrape_results, and rank_streams execute inside bounded isolated stage budgets",
                f"stream_refresh_dispatch_mode={settings.stream.refresh_dispatch_mode}",
                f"stream_refresh_queue_ready={int(queued_refresh_ready)}",
                f"heavy_stage_executor_mode={heavy_stage_policy.executor_mode}",
                f"heavy_stage_max_workers={heavy_stage_policy.max_workers}",
                f"heavy_stage_max_tasks_per_child={heavy_stage_policy.max_tasks_per_child}",
                f"heavy_stage_process_isolation_required={int(heavy_stage_policy.executor_mode == 'process_pool_required')}",
                (
                    "heavy_stage_timeouts="
                    "index_item:"
                    f"{heavy_stage_policy.index_timeout_seconds},"
                    "parse_scrape_results:"
                    f"{heavy_stage_policy.parse_timeout_seconds},"
                    "rank_streams:"
                    f"{heavy_stage_policy.rank_timeout_seconds}"
                ),
                f"heavy_stage_proof_ref_count={len(heavy_stage_policy.proof_refs)}",
                f"queued_refresh_proof_ref_count={len(settings.orchestration.queued_refresh_proof_refs)}",
                f"heavy_stage_exit_ready={int(heavy_stage_exit_ready)}",
                "GET /api/v1/workers/queue and /api/v1/workers/queue/history expose queue pressure",
            ],
            required_actions=(
                []
                if heavy_stage_exit_ready
                else [
                    "enable_queued_stream_link_refresh_dispatch"
                    if settings.stream.refresh_dispatch_mode != "queued"
                    else "promote_per_stage_cpu_memory_and_timeout_budgets_into_environment policy",
                    "attach_queued_refresh_runtime_controllers"
                    if settings.stream.refresh_dispatch_mode == "queued" and not queued_refresh_ready
                    else (
                        "record_queued_refresh_soak_evidence"
                        if not settings.orchestration.queued_refresh_proof_refs
                        else "review_queued_refresh_soak_evidence"
                    ),
                    "require_process_backed_heavy_stage_isolation"
                    if heavy_stage_policy.executor_mode != "process_pool_required"
                    else (
                        "set_heavy_stage_process_recycle_budget"
                        if heavy_stage_policy.max_tasks_per_child <= 0
                        else (
                            "record_heavy_stage_failure_injection_or_soak_evidence"
                            if not heavy_stage_policy.proof_refs
                            else "review_heavy_stage_isolation_evidence"
                        )
                    ),
                ]
            ),
            remaining_gaps=(
                []
                if heavy_stage_exit_ready
                else [
                    "queued stream-link refresh dispatch is not fully configured"
                    if settings.stream.refresh_dispatch_mode != "queued" or not queued_refresh_ready
                    else "queued stream-link refresh soak evidence has not been recorded",
                    "heavy stages are not yet forced into process-backed isolation"
                    if heavy_stage_policy.executor_mode != "process_pool_required"
                    else "heavy-stage worker recycle limits are not configured"
                    if heavy_stage_policy.max_tasks_per_child <= 0
                    else "heavy-stage isolation evidence has not been recorded",
                ]
            ),
        ),
        release_metadata_performance=EnterpriseOperationsSliceResponse(
            name="Release Engineering / Metadata Governance / Performance Discipline",
            status="partial",
            evidence=[
                "release workflow requires PAT-authenticated release-please updates",
                "package.json exposes security:audit, security:bandit, and perf:bench",
                "STATUS.md plus the active TODO matrix set track release, metadata, and chaos gaps explicitly",
                "scripts/run_backup_restore_proof.ps1 and playback/VFS proof gates already produce promotion evidence inputs",
                "scheduled metadata reindex/reconciliation now runs as a first-class worker cron program",
                "GET /api/v1/workers/metadata-reindex and /api/v1/workers/metadata-reindex/history expose bounded operator rollups",
            ],
            required_actions=[
                "add_sbom_signing_and_artifact_promotion_policy",
                "define_benchmark_baselines_and_chaos_regression_thresholds",
            ],
            remaining_gaps=[
                "artifact provenance and SBOM policy are not yet first-class release gates",
                "benchmark and chaos discipline are not yet enforced by regression thresholds",
            ],
        ),
    )


@router.get("/", operation_id="default.root", response_model=MessageResponse)
async def root() -> MessageResponse:
    """Compatibility root endpoint."""

    return MessageResponse(message="Filmu Python compatibility backend is running")


@router.get("/health", operation_id="default.health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    """Health endpoint compatible with frontend expectations."""

    resources = request.app.state.resources
    checks: dict[str, str] = {"redis": "unknown"}

    try:
        await asyncio.wait_for(resources.redis.ping(), timeout=1.0)
        checks["redis"] = "ok"
    except Exception:
        checks["redis"] = "unreachable"

    status: Literal["healthy", "degraded", "unhealthy"] = (
        "healthy" if all(value == "ok" for value in checks.values()) else "degraded"
    )
    return HealthResponse(
        message="OK" if status == "healthy" else "DEGRADED",
        service=resources.settings.service_name,
        status=status,
        checks=checks,
    )


@router.get(
    "/operations/runtime",
    operation_id="default.operations_runtime",
    response_model=RuntimeLifecycleResponse,
)
async def get_runtime_lifecycle(request: Request) -> RuntimeLifecycleResponse:
    """Return the explicit runtime lifecycle graph and bounded transition history."""

    return _runtime_lifecycle_response(request)


@router.get(
    "/auth/context",
    operation_id="default.auth_context",
    response_model=AuthContextResponse,
)
async def get_auth_identity_context(request: Request) -> AuthContextResponse:
    """Return the current authenticated actor plus persisted identity-plane mapping."""

    auth_context = get_auth_context(request)
    identity = getattr(request.state, "auth_identity", None)
    return AuthContextResponse(
        authentication_mode=auth_context.authentication_mode,
        api_key_id=auth_context.api_key_id,
        actor_id=auth_context.actor_id,
        actor_type=auth_context.actor_type,
        tenant_id=auth_context.tenant_id,
        authorized_tenant_ids=list(auth_context.authorized_tenant_ids),
        authorization_tenant_scope=auth_context.authorization_tenant_scope,
        roles=list(auth_context.roles),
        scopes=list(auth_context.scopes),
        effective_permissions=list(auth_context.effective_permissions),
        oidc_issuer=auth_context.oidc_issuer,
        oidc_subject=auth_context.oidc_subject,
        oidc_token_validated=auth_context.oidc_token_validated,
        access_policy_version=auth_context.access_policy_version,
        access_policy_source=auth_context.access_policy_source,
        quota_policy_version=auth_context.quota_policy_version,
        principal_key=getattr(identity, "principal_key", None),
        principal_type=getattr(identity, "principal_type", None),
        service_account_api_key_id=getattr(identity, "service_account_api_key_id", None),
    )


@router.get(
    "/auth/policy",
    operation_id="default.auth_policy",
    response_model=AuthPolicyResponse,
)
async def get_auth_policy_context(request: Request) -> AuthPolicyResponse:
    """Return tenant-aware authorization posture for the current actor."""

    auth_context = get_auth_context(request)
    resources = request.app.state.resources
    policy_snapshot = _resolved_access_policy_snapshot(request)
    policy_constraints = policy_snapshot.permission_constraints
    wave2_constraint_coverage_ready, wave2_constraint_gaps = _wave2_constraint_coverage(
        policy_constraints
    )
    oidc_rollout_status, oidc_configuration_complete, _oidc_evidence, oidc_remaining_gaps = (
        _oidc_rollout_snapshot(resources.settings)
    )
    warnings: list[str] = []
    if auth_context.authentication_mode == "api_key":
        warnings.append("authentication is still API-key anchored")
    if auth_context.oidc_issuer is None or auth_context.oidc_subject is None:
        warnings.append("oidc claims are not present on this request")
    if auth_context.oidc_issuer is not None and not auth_context.oidc_token_validated:
        warnings.append("oidc claims were supplied by headers and were not token-validated")
    if auth_context.authorization_tenant_scope == "all":
        warnings.append("actor has global tenant scope")
    return AuthPolicyResponse(
        authentication_mode=auth_context.authentication_mode,
        actor_id=auth_context.actor_id,
        actor_type=auth_context.actor_type,
        tenant_id=auth_context.tenant_id,
        authorization_tenant_scope=auth_context.authorization_tenant_scope,
        authorized_tenant_ids=list(auth_context.authorized_tenant_ids),
        oidc_claims_present=(
            auth_context.oidc_issuer is not None and auth_context.oidc_subject is not None
        ),
        oidc_token_validated=auth_context.oidc_token_validated,
        oidc_allow_api_key_fallback=resources.settings.oidc.allow_api_key_fallback,
        oidc_rollout_stage=resources.settings.oidc.rollout_stage,
        oidc_rollout_evidence_refs=list(resources.settings.oidc.rollout_evidence_refs),
        oidc_subject_mapping_ready=resources.settings.oidc.subject_mapping_ready,
        oidc_rollout_status=cast(Literal["ready", "partial", "blocked"], oidc_rollout_status),
        oidc_configuration_complete=oidc_configuration_complete,
        access_policy_version=auth_context.access_policy_version,
        quota_policy_version=auth_context.quota_policy_version,
        permissions_model=(
            "role_scope_effective_permissions_with_route_and_resource_scope_constraints_and_tenant_scope"
        ),
        policy_source=auth_context.access_policy_source,
        role_grants={
            role: list(permissions)
            for role, permissions in sorted(policy_snapshot.role_grants.items())
        },
        permission_constraints={
            permission: {
                field: list(values)
                for field, values in sorted(constraints.items())
            }
            for permission, constraints in sorted(policy_constraints.items())
        },
        audit_mode=(
            "persisted_decision_ledger"
            if resources.authorization_audit_service is not None
            else "structured_log_history_only"
        ),
        policy_alerting_enabled=policy_snapshot.alerting_enabled,
        repeated_denial_warning_threshold=policy_snapshot.repeated_denial_warning_threshold,
        repeated_denial_critical_threshold=policy_snapshot.repeated_denial_critical_threshold,
        decisions=_auth_policy_decisions(
            auth_context,
            permission_constraints=policy_constraints,
        ),
        warnings=warnings,
        remaining_gaps=[
            *oidc_remaining_gaps,
            *(
                []
                if wave2_constraint_coverage_ready
                else [
                    "ABAC route/resource-scope coverage is still incomplete for some Wave 2 permissions: "
                    + ", ".join(wave2_constraint_gaps)
                ]
            ),
            *(
                []
                if policy_snapshot.alerting_enabled
                else ["policy alerting is disabled for the active access-policy revision"]
            ),
        ],
    )


@router.get(
    "/auth/policy/revisions",
    operation_id="default.auth_policy_revisions",
    response_model=AccessPolicyRevisionListResponse,
    dependencies=[Depends(require_permissions("settings:write"))],
)
async def list_auth_policy_revisions(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
) -> AccessPolicyRevisionListResponse:
    """Return persisted access-policy revisions for operator review."""

    resources = request.app.state.resources
    service = resources.access_policy_service
    if service is None:
        snapshot = resources.access_policy_snapshot
        if snapshot is None:
            return AccessPolicyRevisionListResponse(active_version=None, revisions=[])
        now = datetime.now(UTC).isoformat()
        return AccessPolicyRevisionListResponse(
            active_version=snapshot.version,
            revisions=[
                AccessPolicyRevisionResponse(
                    version=snapshot.version,
                    source=snapshot.source,
                    approval_status="bootstrap",
                    proposed_by=None,
                    approved_by="settings_bootstrap",
                    approved_at=now,
                    approval_notes="bootstrapped from runtime settings",
                    is_active=True,
                    activated_at=now,
                    created_at=now,
                    updated_at=now,
                    role_grants={
                        role: list(permissions)
                        for role, permissions in sorted(snapshot.role_grants.items())
                    },
                    principal_roles={
                        principal: list(roles)
                        for principal, roles in sorted(snapshot.principal_roles.items())
                    },
                    principal_scopes={
                        principal: list(scopes)
                        for principal, scopes in sorted(snapshot.principal_scopes.items())
                    },
                    principal_tenant_grants={
                        principal: list(tenants)
                        for principal, tenants in sorted(snapshot.principal_tenant_grants.items())
                    },
                    permission_constraints={
                        permission: {
                            field: list(values)
                            for field, values in sorted(constraints.items())
                        }
                        for permission, constraints in sorted(snapshot.permission_constraints.items())
                    },
                    audit_decisions=snapshot.audit_decisions,
                    alerting_enabled=snapshot.alerting_enabled,
                    repeated_denial_warning_threshold=snapshot.repeated_denial_warning_threshold,
                    repeated_denial_critical_threshold=snapshot.repeated_denial_critical_threshold,
                )
            ],
        )

    revisions = await service.list_revisions(limit=limit)
    active_version = next((revision.version for revision in revisions if revision.is_active), None)
    return AccessPolicyRevisionListResponse(
        active_version=active_version,
        revisions=[_access_policy_revision_response(revision) for revision in revisions],
    )


@router.post(
    "/auth/policy/revisions",
    operation_id="default.write_auth_policy_revision",
    response_model=AccessPolicyRevisionResponse,
    dependencies=[Depends(require_permissions("settings:write"))],
)
async def write_auth_policy_revision(
    request: Request,
    payload: AccessPolicyRevisionWriteRequest,
) -> AccessPolicyRevisionResponse:
    """Persist one operator-managed access-policy revision and optionally activate it."""

    auth_context = get_auth_context(request)
    resources = request.app.state.resources
    service = resources.access_policy_service
    if service is None:
        raise HTTPException(status_code=503, detail="Access policy service unavailable")

    can_approve = "*" in auth_context.effective_permissions or "security:policy.approve" in set(
        auth_context.effective_permissions
    )
    record = await service.write_revision(
        version=payload.version,
        source=payload.source,
        role_grants=payload.role_grants,
        principal_roles=payload.principal_roles,
        principal_scopes=payload.principal_scopes,
        principal_tenant_grants=payload.principal_tenant_grants,
        permission_constraints=payload.permission_constraints,
        audit_decisions=payload.audit_decisions,
        alerting_enabled=payload.alerting_enabled,
        repeated_denial_warning_threshold=payload.repeated_denial_warning_threshold,
        repeated_denial_critical_threshold=payload.repeated_denial_critical_threshold,
        proposed_by=_actor_key(auth_context),
        approval_notes=payload.approval_notes,
        auto_approve=can_approve,
        activate=payload.activate,
    )
    if record.is_active:
        resources.access_policy_snapshot = record.to_snapshot()
    audit_action(
        request,
        action="security.access_policy.write_revision",
        target=f"access_policy.{payload.version}",
        details={
            "activate": payload.activate,
            "source": payload.source,
            "approval_status": record.approval_status,
        },
    )
    return _access_policy_revision_response(record)


@router.post(
    "/auth/policy/revisions/{version}/activate",
    operation_id="default.activate_auth_policy_revision",
    response_model=AccessPolicyRevisionResponse,
    dependencies=[Depends(require_permissions("settings:write"))],
)
async def activate_auth_policy_revision(
    request: Request,
    version: str,
) -> AccessPolicyRevisionResponse:
    """Activate one persisted access-policy revision."""

    resources = request.app.state.resources
    service = resources.access_policy_service
    if service is None:
        raise HTTPException(status_code=503, detail="Access policy service unavailable")
    try:
        record = await service.activate_revision(version)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    resources.access_policy_snapshot = record.to_snapshot()
    audit_action(
        request,
        action="security.access_policy.activate_revision",
        target=f"access_policy.{record.version}",
    )
    return _access_policy_revision_response(record)


@router.post(
    "/auth/policy/revisions/{version}/approve",
    operation_id="default.approve_auth_policy_revision",
    response_model=AccessPolicyRevisionResponse,
    dependencies=[Depends(require_permissions("security:policy.approve"))],
)
async def approve_auth_policy_revision(
    request: Request,
    version: str,
    payload: AccessPolicyRevisionApprovalRequest,
) -> AccessPolicyRevisionResponse:
    """Approve one persisted access-policy revision and optionally activate it."""

    auth_context = get_auth_context(request)
    resources = request.app.state.resources
    service = resources.access_policy_service
    if service is None:
        raise HTTPException(status_code=503, detail="Access policy service unavailable")
    try:
        record = await service.approve_revision(
            version,
            approved_by=_actor_key(auth_context),
            approval_notes=payload.approval_notes,
            activate=payload.activate,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if record.is_active:
        resources.access_policy_snapshot = record.to_snapshot()
    audit_action(
        request,
        action="security.access_policy.approve_revision",
        target=f"access_policy.{record.version}",
        details={"activate": payload.activate},
    )
    return _access_policy_revision_response(record)


@router.post(
    "/auth/policy/revisions/{version}/reject",
    operation_id="default.reject_auth_policy_revision",
    response_model=AccessPolicyRevisionResponse,
    dependencies=[Depends(require_permissions("security:policy.approve"))],
)
async def reject_auth_policy_revision(
    request: Request,
    version: str,
    payload: AccessPolicyRevisionApprovalRequest,
) -> AccessPolicyRevisionResponse:
    """Reject one persisted access-policy revision while retaining its history."""

    auth_context = get_auth_context(request)
    resources = request.app.state.resources
    service = resources.access_policy_service
    if service is None:
        raise HTTPException(status_code=503, detail="Access policy service unavailable")
    try:
        record = await service.reject_revision(
            version,
            rejected_by=_actor_key(auth_context),
            approval_notes=payload.approval_notes,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    audit_action(
        request,
        action="security.access_policy.reject_revision",
        target=f"access_policy.{record.version}",
    )
    return _access_policy_revision_response(record)


@router.get(
    "/auth/policy/audit",
    operation_id="default.auth_policy_audit",
    response_model=AccessPolicyAuditResponse,
    dependencies=[Depends(require_permissions("settings:write"))],
)
async def get_auth_policy_audit(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    actor_id: Annotated[str | None, Query()] = None,
    tenant_id: Annotated[str | None, Query()] = None,
    target_tenant_id: Annotated[str | None, Query()] = None,
    permission: Annotated[str | None, Query()] = None,
    allowed: Annotated[bool | None, Query()] = None,
    reason: Annotated[str | None, Query()] = None,
    path_prefix: Annotated[str | None, Query()] = None,
) -> AccessPolicyAuditResponse:
    """Return bounded operator audit-search results for access-policy governance actions."""

    resources = request.app.state.resources
    audit_service = resources.authorization_audit_service
    policy_snapshot = _resolved_access_policy_snapshot(request)
    if audit_service is None:
        history = resources.log_stream.history()
        matches = [
            line
            for line in history
            if "security.access_policy." in line or "auth.permission_decision" in line
        ]
        bounded = matches[-limit:]
        return AccessPolicyAuditResponse(total_matches=len(matches), entries=bounded)

    search = await audit_service.search(
        limit=limit,
        actor_id=actor_id,
        tenant_id=tenant_id,
        target_tenant_id=target_tenant_id,
        permission=permission,
        allowed=allowed,
        reason=reason,
        path_prefix=path_prefix,
    )
    records = [
        AccessPolicyAuditEntryResponse(
            occurred_at=record.occurred_at.isoformat(),
            path=record.path,
            method=record.method,
            resource_scope=record.resource_scope,
            actor_id=record.actor_id,
            actor_type=record.actor_type,
            tenant_id=record.tenant_id,
            target_tenant_id=record.target_tenant_id,
            required_permissions=list(record.required_permissions),
            matched_permissions=list(record.matched_permissions),
            missing_permissions=list(record.missing_permissions),
            constrained_permissions=list(record.constrained_permissions),
            constraint_failures=list(record.constraint_failures),
            allowed=record.allowed,
            reason=record.reason,
            tenant_scope=record.tenant_scope,
            authentication_mode=record.authentication_mode,
            access_policy_version=record.access_policy_version,
            access_policy_source=record.access_policy_source,
            oidc_issuer=record.oidc_issuer,
            oidc_subject=record.oidc_subject,
            summary=_authorization_audit_summary(record),
        )
        for record in search.records
    ]
    return AccessPolicyAuditResponse(
        total_matches=search.total_matches,
        entries=[record.summary for record in records],
        records=records,
        alerts=_authorization_audit_alerts(
            list(search.records),
            alerting_enabled=policy_snapshot.alerting_enabled,
            repeated_denial_warning_threshold=policy_snapshot.repeated_denial_warning_threshold,
            repeated_denial_critical_threshold=policy_snapshot.repeated_denial_critical_threshold,
        ),
    )


@router.get("/logs", operation_id="default.logs", response_model=LogsResponse)
async def get_logs(request: Request) -> LogsResponse:
    """Return bounded in-memory historical logs for frontend historical log views."""

    resources = request.app.state.resources
    return LogsResponse(logs=resources.log_stream.history())


@router.get(
    "/tenants/quota",
    operation_id="default.tenant_quota_policy",
    response_model=TenantQuotaPolicyResponse,
)
async def get_tenant_quota_policy(
    request: Request,
    tenant_id: str | None = Query(default=None),
) -> TenantQuotaPolicyResponse:
    """Return current quota boundaries for one authorized tenant."""

    auth_context = get_auth_context(request)
    try:
        resolved_tenant_id = _resolve_target_tenant_id(
            auth_context=auth_context,
            requested_tenant_id=tenant_id,
            required_permissions=("tenant:quota.read",),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    settings = request.app.state.resources.settings
    limits = settings.tenant_quotas.tenants.get(
        resolved_tenant_id,
        settings.tenant_quotas.default,
    )
    return TenantQuotaPolicyResponse(
        tenant_id=resolved_tenant_id,
        enabled=settings.tenant_quotas.enabled,
        policy_version=settings.tenant_quotas.version,
        api_requests_per_minute=limits.api_requests_per_minute,
        worker_enqueues_per_minute=limits.worker_enqueues_per_minute,
        playback_refreshes_per_minute=limits.playback_refreshes_per_minute,
        provider_refreshes_per_minute=limits.provider_refreshes_per_minute,
        enforcement_points=[
            "api_request_intake" if settings.tenant_quotas.enabled else "api_request_visibility",
            "worker_enqueue_policy",
            "provider_refresh_policy",
            "playback_refresh_policy",
        ],
        remaining_gaps=[
            "worker/provider/playback quota ceilings are visible but not yet enforced everywhere",
            "quota counters are Redis minute buckets, not long-horizon billing records",
        ],
    )


@router.get(
    "/workers/queue",
    operation_id="default.worker_queue",
    response_model=QueueStatusResponse,
)
async def get_worker_queue_status(request: Request) -> QueueStatusResponse:
    """Return ARQ queue depth, lag, retry, and dead-letter visibility."""

    resources = request.app.state.resources
    queue_name = resources.arq_queue_name or resources.settings.arq_queue_name
    redis = resources.arq_redis or resources.redis
    snapshot = await QueueStatusReader(redis, queue_name=queue_name).snapshot()
    return QueueStatusResponse(
        queue_name=snapshot.queue_name,
        arq_enabled=resources.settings.arq_enabled,
        observed_at=snapshot.observed_at,
        total_jobs=snapshot.total_jobs,
        ready_jobs=snapshot.ready_jobs,
        deferred_jobs=snapshot.deferred_jobs,
        in_progress_jobs=snapshot.in_progress_jobs,
        retry_jobs=snapshot.retry_jobs,
        result_jobs=snapshot.result_jobs,
        dead_letter_jobs=snapshot.dead_letter_jobs,
        alert_level=snapshot.alert_level,
        alerts=[
            QueueAlertResponse(
                code=alert.code,
                severity=alert.severity,
                message=alert.message,
            )
            for alert in snapshot.alerts
        ],
        oldest_ready_age_seconds=snapshot.oldest_ready_age_seconds,
        next_scheduled_in_seconds=snapshot.next_scheduled_in_seconds,
        dead_letter_reason_counts=dict(snapshot.dead_letter_reason_counts),
    )


@router.get(
    "/workers/queue/history",
    operation_id="default.worker_queue_history",
    response_model=QueueStatusHistoryResponse,
)
async def get_worker_queue_history(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> QueueStatusHistoryResponse:
    """Return bounded queue trend history captured during queue observations."""

    resources = request.app.state.resources
    queue_name = resources.arq_queue_name or resources.settings.arq_queue_name
    redis = resources.arq_redis or resources.redis
    history = await QueueStatusReader(redis, queue_name=queue_name).history(limit=limit)
    history_points = [
        QueueStatusHistoryPointResponse(
            observed_at=item.observed_at,
            total_jobs=item.total_jobs,
            ready_jobs=item.ready_jobs,
            deferred_jobs=item.deferred_jobs,
            in_progress_jobs=item.in_progress_jobs,
            retry_jobs=item.retry_jobs,
            dead_letter_jobs=item.dead_letter_jobs,
            oldest_ready_age_seconds=item.oldest_ready_age_seconds,
            next_scheduled_in_seconds=item.next_scheduled_in_seconds,
            alert_level=item.alert_level,
            dead_letter_reason_counts=dict(item.dead_letter_reason_counts),
        )
        for item in history
    ]
    return QueueStatusHistoryResponse(
        queue_name=queue_name,
        summary=_summarize_queue_history(history_points),
        history=history_points,
    )


@router.get(
    "/workers/metadata-reindex",
    operation_id="default.worker_metadata_reindex",
    response_model=MetadataReindexStatusResponse,
)
async def get_worker_metadata_reindex_status(request: Request) -> MetadataReindexStatusResponse:
    """Return the latest metadata reindex/reconciliation run summary."""

    resources = request.app.state.resources
    queue_name = resources.arq_queue_name or resources.settings.arq_queue_name
    redis = resources.arq_redis or resources.redis
    latest = await MetadataReindexStatusStore(redis, queue_name=queue_name).latest()
    if latest is None:
        return MetadataReindexStatusResponse(
            queue_name=queue_name,
            schedule_offset_minutes=resources.settings.indexer.schedule_offset_minutes,
            has_history=False,
            observed_at="",
            processed=0,
            queued=0,
            reconciled=0,
            skipped_active=0,
            failed=0,
            outcome="ok",
            run_failed=False,
            last_error=None,
        )
    return MetadataReindexStatusResponse(
        queue_name=queue_name,
        schedule_offset_minutes=resources.settings.indexer.schedule_offset_minutes,
        has_history=True,
        observed_at=latest.observed_at,
        processed=latest.processed,
        queued=latest.queued,
        reconciled=latest.reconciled,
        skipped_active=latest.skipped_active,
        failed=latest.failed,
        outcome=latest.outcome,
        run_failed=latest.run_failed,
        last_error=latest.last_error,
    )


@router.get(
    "/workers/metadata-reindex/history",
    operation_id="default.worker_metadata_reindex_history",
    response_model=MetadataReindexHistoryResponse,
)
async def get_worker_metadata_reindex_history(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> MetadataReindexHistoryResponse:
    """Return bounded metadata reindex/reconciliation trend history."""

    resources = request.app.state.resources
    queue_name = resources.arq_queue_name or resources.settings.arq_queue_name
    redis = resources.arq_redis or resources.redis
    history = await MetadataReindexStatusStore(redis, queue_name=queue_name).history(limit=limit)
    history_points = [
        MetadataReindexHistoryPointResponse(
            observed_at=item.observed_at,
            processed=item.processed,
            queued=item.queued,
            reconciled=item.reconciled,
            skipped_active=item.skipped_active,
            failed=item.failed,
            outcome=item.outcome,
            run_failed=item.run_failed,
            last_error=item.last_error,
        )
        for item in history
    ]
    return MetadataReindexHistoryResponse(
        queue_name=queue_name,
        schedule_offset_minutes=resources.settings.indexer.schedule_offset_minutes,
        summary=_summarize_metadata_reindex_history(history_points),
        history=history_points,
    )


@router.get("/services", operation_id="default.services", response_model=dict[str, dict[str, bool]])
async def get_services(request: Request) -> dict[str, dict[str, bool]]:
    """Return a real provider enablement map for dashboard compatibility."""

    resources = request.app.state.resources
    return {
        "real_debrid": {"enabled": bool(resources.settings.downloaders.real_debrid.api_key)},
        "all_debrid": {"enabled": bool(resources.settings.downloaders.all_debrid.api_key)},
        "debrid_link": {"enabled": bool(resources.settings.downloaders.debrid_link.api_key)},
        "mdblist": {"enabled": bool(resources.settings.content.mdblist.api_key)},
    }


@router.get(
    "/plugins",
    operation_id="default.plugins",
    response_model=list[PluginCapabilityStatusResponse],
)
async def get_plugins(request: Request) -> list[PluginCapabilityStatusResponse]:
    """Return loaded non-GraphQL capability plugins for runtime visibility."""

    resources = request.app.state.resources
    plugin_registry = resources.plugin_registry
    loaded_report, failed_report = _plugin_load_report_maps(request)
    override_service = resources.plugin_governance_service
    overrides = await override_service.list_overrides() if override_service is not None else {}
    if plugin_registry is None:
        return [
            PluginCapabilityStatusResponse(
                name=plugin_name,
                capabilities=[],
                status="load_failed",
                ready=False,
                source=getattr(failure, "source", None),
                error=getattr(failure, "reason", None),
            )
            for plugin_name, failure in sorted(failed_report.items())
        ]

    responses: list[PluginCapabilityStatusResponse] = []
    registrations_by_plugin = plugin_registry.by_plugin()
    all_plugin_names = plugin_registry.all_plugin_names() | set(failed_report)
    for plugin_name in sorted(all_plugin_names):
        manifest = plugin_registry.manifest(plugin_name)
        registrations = registrations_by_plugin.get(plugin_name, [])
        success = loaded_report.get(plugin_name)
        failure = failed_report.get(plugin_name)
        ready, configured, warnings = _plugin_runtime_health(plugin_name, resources)
        signature_fields = _plugin_signature_fields(manifest=manifest, success=success)
        override = overrides.get(plugin_name)
        if success is not None:
            warnings.extend(list(getattr(success, "skipped", ())))
        if failure is not None and not registrations:
            responses.append(
                PluginCapabilityStatusResponse(
                    name=plugin_name,
                    capabilities=[],
                    status="load_failed",
                    ready=False,
                    version=manifest.version if manifest is not None else None,
                    api_version=manifest.api_version if manifest is not None else None,
                    min_host_version=manifest.min_host_version if manifest is not None else None,
                    max_host_version=manifest.max_host_version if manifest is not None else None,
                    publisher=manifest.publisher if manifest is not None else None,
                    release_channel=manifest.release_channel if manifest is not None else None,
                    trust_level=manifest.trust_level if manifest is not None else None,
                    permission_scopes=(
                        sorted(manifest.effective_permission_scopes())
                        if manifest is not None
                        else []
                    ),
                    source_sha256=manifest.source_sha256 if manifest is not None else None,
                    signing_key_id=manifest.signing_key_id if manifest is not None else None,
                    sandbox_profile=manifest.sandbox_profile if manifest is not None else None,
                    tenancy_mode=manifest.tenancy_mode if manifest is not None else None,
                    quarantined=(
                        (override.state == "quarantined")
                        if override is not None
                        else (manifest.quarantined if manifest is not None else False)
                    ),
                    quarantine_reason=(
                        override.reason
                        if override is not None and override.state == "quarantined"
                        else (manifest.quarantine_reason if manifest is not None else None)
                    ),
                    publisher_policy_decision=(
                        getattr(success, "publisher_policy_decision", None)
                        if success is not None
                        else None
                    ),
                    publisher_policy_status=(
                        getattr(success, "publisher_policy_status", None)
                        if success is not None
                        else None
                    ),
                    quarantine_recommended=(
                        bool(getattr(success, "quarantine_recommended", False))
                        if success is not None
                        else False
                    ),
                    source=getattr(failure, "source", None),
                    warnings=warnings,
                    error=getattr(failure, "reason", None),
                    **signature_fields,
                )
            )
            continue

        is_revoked = override is not None and override.state == "revoked"
        is_quarantined = override is not None and override.state == "quarantined"
        if is_revoked:
            warnings.append("operator override revoked this plugin")
        elif is_quarantined:
            warnings.append("operator override quarantined this plugin")
        responses.append(
            PluginCapabilityStatusResponse(
                name=plugin_name,
                capabilities=sorted({registration.kind.value for registration in registrations}),
                status="loaded",
                ready=ready and not is_revoked and not is_quarantined,
                configured=configured,
                version=manifest.version if manifest is not None else None,
                api_version=manifest.api_version if manifest is not None else None,
                min_host_version=manifest.min_host_version if manifest is not None else None,
                max_host_version=manifest.max_host_version if manifest is not None else None,
                publisher=manifest.publisher if manifest is not None else None,
                release_channel=manifest.release_channel if manifest is not None else None,
                trust_level=manifest.trust_level if manifest is not None else None,
                permission_scopes=(
                    sorted(manifest.effective_permission_scopes())
                    if manifest is not None
                    else []
                ),
                source_sha256=manifest.source_sha256 if manifest is not None else None,
                signing_key_id=manifest.signing_key_id if manifest is not None else None,
                sandbox_profile=manifest.sandbox_profile if manifest is not None else None,
                tenancy_mode=manifest.tenancy_mode if manifest is not None else None,
                quarantined=(
                    is_quarantined if override is not None else (manifest.quarantined if manifest is not None else False)
                ),
                quarantine_reason=(
                    override.reason
                    if is_quarantined and override is not None
                    else (manifest.quarantine_reason if manifest is not None else None)
                ),
                publisher_policy_decision=getattr(success, "publisher_policy_decision", None),
                publisher_policy_status=getattr(success, "publisher_policy_status", None),
                quarantine_recommended=bool(
                    getattr(success, "quarantine_recommended", False)
                ),
                source=(
                    manifest.distribution
                    if manifest is not None
                    else getattr(success, "source", None)
                ),
                warnings=warnings,
                **signature_fields,
            )
        )
    return responses


@router.get(
    "/plugins/governance",
    operation_id="default.plugin_governance",
    response_model=PluginGovernanceResponse,
)
async def get_plugin_governance(request: Request) -> PluginGovernanceResponse:
    """Return plugin trust, quarantine, and isolation posture for operators."""

    plugins = await get_plugins(request)
    resources = request.app.state.resources
    return PluginGovernanceResponse(
        summary=_plugin_governance_summary(
            plugins,
            runtime_policy=resources.settings.plugin_runtime,
        ),
        plugins=plugins,
    )


@router.get(
    "/plugins/governance/overrides",
    operation_id="default.plugin_governance_overrides",
    response_model=list[PluginGovernanceOverrideResponse],
    dependencies=[Depends(require_permissions("settings:write"))],
)
async def list_plugin_governance_overrides(
    request: Request,
) -> list[PluginGovernanceOverrideResponse]:
    """Return persisted operator-managed plugin governance overrides."""

    service = request.app.state.resources.plugin_governance_service
    if service is None:
        return []
    return [
        _plugin_override_response(record)
        for record in (await service.list_overrides()).values()
    ]


@router.post(
    "/plugins/governance/{plugin_name}",
    operation_id="default.write_plugin_governance_override",
    response_model=PluginGovernanceOverrideResponse,
    dependencies=[Depends(require_permissions("settings:write"))],
)
async def write_plugin_governance_override(
    request: Request,
    plugin_name: str,
    payload: PluginGovernanceOverrideWriteRequest,
) -> PluginGovernanceOverrideResponse:
    """Persist one plugin governance override for quarantine, revocation, or approval."""

    auth_context = get_auth_context(request)
    service = request.app.state.resources.plugin_governance_service
    if service is None:
        raise HTTPException(status_code=503, detail="Plugin governance service unavailable")
    record = await service.write_override(
        plugin_name=plugin_name,
        state=payload.state,
        reason=payload.reason,
        notes=payload.notes,
        updated_by=_actor_key(auth_context),
    )
    audit_action(
        request,
        action="security.plugin_governance.write_override",
        target=f"plugin.{record.plugin_name}",
        details={"state": record.state},
    )
    return _plugin_override_response(record)


@router.get(
    "/operations/governance",
    operation_id="default.enterprise_operations_governance",
    response_model=EnterpriseOperationsGovernanceResponse,
)
async def get_enterprise_operations_governance(
    request: Request,
) -> EnterpriseOperationsGovernanceResponse:
    """Return enterprise operations posture across the active roadmap slices."""

    plugins = await get_plugins(request)
    return await _enterprise_operations_governance(request=request, plugins=plugins)


@router.get(
    "/operations/control-plane/subscribers",
    operation_id="default.control_plane_subscribers",
    response_model=list[ControlPlaneSubscriberResponse],
    dependencies=[Depends(require_permissions("settings:write"))],
)
async def get_control_plane_subscribers(
    request: Request,
    active_within_seconds: Annotated[int, Query(ge=1, le=3600)] = 120,
) -> list[ControlPlaneSubscriberResponse]:
    """Return durable replay/control-plane subscriber ownership and resume state."""

    service = request.app.state.resources.control_plane_service
    if service is None:
        return []
    records = await service.list_subscribers(active_within_seconds=active_within_seconds)
    return [
        ControlPlaneSubscriberResponse(
            stream_name=record.stream_name,
            group_name=record.group_name,
            consumer_name=record.consumer_name,
            node_id=record.node_id,
            tenant_id=record.tenant_id,
            status=record.status,
            last_read_offset=record.last_read_offset,
            last_delivered_event_id=record.last_delivered_event_id,
            last_acked_event_id=record.last_acked_event_id,
            last_error=record.last_error,
            claimed_at=record.claimed_at.isoformat(),
            last_heartbeat_at=record.last_heartbeat_at.isoformat(),
            created_at=record.created_at.isoformat(),
            updated_at=record.updated_at.isoformat(),
        )
        for record in records
    ]


@router.get(
    "/plugins/events",
    operation_id="default.plugin_events",
    response_model=list[PluginEventStatusResponse],
)
async def get_plugin_events(request: Request) -> list[PluginEventStatusResponse]:
    """Return declared publishable events and hook subscriptions per plugin."""

    resources = request.app.state.resources
    plugin_registry = resources.plugin_registry
    if plugin_registry is None:
        return []

    publishable_by_plugin = plugin_registry.publishable_events_by_plugin()
    subscriptions_by_plugin = plugin_registry.hook_subscriptions_by_plugin()
    return [
        PluginEventStatusResponse(
            name=plugin_name,
            publisher=(
                plugin_registry.manifest(plugin_name).publisher
                if plugin_registry.manifest(plugin_name) is not None
                else None
            ),
            publishable_events=list(publishable_by_plugin.get(plugin_name, ())),
            hook_subscriptions=list(subscriptions_by_plugin.get(plugin_name, ())),
        )
        for plugin_name in sorted(plugin_registry.all_plugin_names())
    ]


@router.get(
    "/downloader_user_info",
    operation_id="default.download_user_info",
    response_model=dict[str, Any],
)
async def get_downloader_user_info(request: Request) -> dict[str, Any]:
    """Return normalized downloader-account info for dashboard compatibility."""

    resources = request.app.state.resources
    cached = await resources.cache.get("downloader:user_info")
    if isinstance(cached, bytes):
        return cast(dict[str, Any], json.loads(cached.decode("utf-8")))

    service = DownloaderAccountService(resources.settings.downloaders)
    result = await service.get_active_provider_info()
    await resources.cache.set(
        "downloader:user_info",
        json.dumps(result).encode("utf-8"),
        ttl_seconds=300,
    )
    return result


@router.post(
    "/generateapikey",
    operation_id="default.generate_apikey",
    response_model=ApiKeyRotationResponse,
    dependencies=[Depends(require_permissions("security:apikey.rotate"))],
)
async def generate_apikey(request: Request) -> ApiKeyRotationResponse:
    """Rotate the live backend API key and persist the new compatibility payload.

    Operators must still update the frontend/BFF environment before making further
    protected requests, because the current request is authenticated with the old key.
    """

    resources = request.app.state.resources
    auth_context = get_auth_context(request)
    new_key = _generate_api_key()
    new_key_id = _next_api_key_id(auth_context)
    resources.settings.api_key = SecretStr(new_key)
    resources.settings.api_key_id = new_key_id
    set_runtime_settings(resources.settings)
    await save_settings(resources.db, resources.settings.to_compatibility_dict())
    identity_service = resources.security_identity_service
    if identity_service is not None:
        await identity_service.rotate_service_account_api_key_id(
            auth_context=auth_context,
            new_api_key_id=new_key_id,
        )
    audit_action(
        request,
        action="security.generate_apikey",
        target="runtime.api_key",
        details={"rotated": True, "new_api_key_id": new_key_id},
    )
    return ApiKeyRotationResponse(
        key=new_key,
        api_key_id=new_key_id,
        warning=API_KEY_ROTATION_WARNING,
    )


@router.get("/stats", operation_id="default.stats", response_model=StatsResponse)
async def get_stats(
    request: Request,
    tenant_id: Annotated[str | None, Query()] = None,
) -> StatsResponse:
    """Return aggregated statistics for the current dashboard compatibility surface."""

    resources = request.app.state.resources
    auth_context = get_auth_context(request)
    try:
        target_tenant_id = _resolve_target_tenant_id(
            auth_context=auth_context,
            requested_tenant_id=tenant_id,
            required_permissions=("library:read",),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    snapshot = await resources.media_service.get_stats(tenant_id=target_tenant_id)
    return StatsResponse(
        total_items=snapshot.total_items,
        total_movies=snapshot.movies,
        total_shows=snapshot.shows,
        total_seasons=snapshot.seasons,
        total_episodes=snapshot.episodes,
        total_symlinks=0,
        incomplete_items=snapshot.incomplete_items,
        states=snapshot.states,
        activity=snapshot.activity,
        media_year_releases=[
            StatsMediaYearRelease(year=item.year, count=item.count)
            for item in snapshot.media_year_releases
        ],
    )


@router.get("/calendar", operation_id="default.calendar", response_model=CalendarResponse)
async def get_calendar(
    request: Request,
    start_date: Annotated[str | None, Query()] = None,
    end_date: Annotated[str | None, Query()] = None,
    tenant_id: Annotated[str | None, Query()] = None,
) -> CalendarResponse:
    """Return calendar items for the current frontend calendar compatibility surface."""

    resources = request.app.state.resources
    auth_context = get_auth_context(request)
    try:
        target_tenant_id = _resolve_target_tenant_id(
            auth_context=auth_context,
            requested_tenant_id=tenant_id,
            required_permissions=("library:read",),
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    snapshot = await resources.media_service.get_calendar_snapshot(
        start_date=start_date,
        end_date=end_date,
        tenant_id=target_tenant_id,
    )
    return CalendarResponse(
        data={
            key: CalendarItemResponse(
                item_id=item.item_id,
                tvdb_id=item.tvdb_id,
                tmdb_id=item.tmdb_id,
                show_title=item.show_title,
                item_type=item.item_type,
                aired_at=item.aired_at,
                season=item.season,
                episode=item.episode,
                last_state=item.last_state,
                release_data=(
                    CalendarReleaseDataResponse(
                        next_aired=item.release_data.next_aired,
                        nextAired=item.release_data.nextAired,
                        last_aired=item.release_data.last_aired,
                        lastAired=item.release_data.lastAired,
                    )
                    if item.release_data is not None
                    else None
                ),
            )
            for key, item in snapshot.items()
        }
    )
