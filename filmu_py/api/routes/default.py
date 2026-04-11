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
from filmu_py.authz import evaluate_permissions
from filmu_py.config import set_runtime_settings
from filmu_py.core.queue_status import QueueStatusReader
from filmu_py.services.debrid import DownloaderAccountService
from filmu_py.services.settings_service import save_settings

from ..models import (
    ApiKeyRotationResponse,
    AuthContextResponse,
    AuthPolicyDecisionResponse,
    AuthPolicyResponse,
    CalendarItemResponse,
    CalendarReleaseDataResponse,
    CalendarResponse,
    EnterpriseOperationsGovernanceResponse,
    EnterpriseOperationsSliceResponse,
    HealthResponse,
    LogsResponse,
    MessageResponse,
    PluginCapabilityStatusResponse,
    PluginEventStatusResponse,
    PluginGovernanceResponse,
    PluginGovernanceSummaryResponse,
    QueueAlertResponse,
    QueueStatusHistoryPointResponse,
    QueueStatusHistoryResponse,
    QueueStatusHistorySummaryResponse,
    QueueStatusResponse,
    StatsMediaYearRelease,
    StatsResponse,
    TenantQuotaPolicyResponse,
)

router = APIRouter(tags=["default"])
_MAX_API_KEY_ID_LENGTH = 128
_API_KEY_ID_SUFFIX_LENGTH = 12

API_KEY_ROTATION_WARNING = (
    "Update BACKEND_API_KEY in your frontend environment and restart the frontend "
    "server before your next request, or all API calls will fail."
)
_AUTH_POLICY_PROBES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("library_read", ("library:read",)),
    ("playback_operate", ("playback:operate",)),
    ("settings_write", ("settings:write",)),
    ("api_key_rotate", ("security:apikey.rotate",)),
)


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
        configured = bool(stremthru.enabled and stremthru.token.strip())
        if stremthru.enabled and not stremthru.token.strip():
            warnings.append("enabled but token is missing")
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
) -> PluginGovernanceSummaryResponse:
    """Return a bounded plugin trust/isolation rollup for operators."""

    sandbox_profile_counts: dict[str, int] = {}
    tenancy_mode_counts: dict[str, int] = {}
    recommended_actions: set[str] = set()
    for plugin in plugins:
        sandbox_profile = plugin.sandbox_profile or "unspecified"
        sandbox_profile_counts[sandbox_profile] = sandbox_profile_counts.get(sandbox_profile, 0) + 1
        tenancy_mode = plugin.tenancy_mode or "unspecified"
        tenancy_mode_counts[tenancy_mode] = tenancy_mode_counts.get(tenancy_mode, 0) + 1
        recommended_actions.update(_plugin_recommended_actions(plugin))

    return PluginGovernanceSummaryResponse(
        total_plugins=len(plugins),
        loaded_plugins=sum(1 for plugin in plugins if plugin.status == "loaded"),
        load_failed_plugins=sum(1 for plugin in plugins if plugin.status == "load_failed"),
        ready_plugins=sum(1 for plugin in plugins if plugin.ready),
        unready_plugins=sum(1 for plugin in plugins if not plugin.ready),
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
        recommended_actions=sorted(recommended_actions),
        remaining_gaps=[
            "runtime sandbox isolation is still in-process",
            "operator quarantine/revocation persistence is still trust-store driven",
            "external plugin artifact provenance is not yet SBOM/signing-policy complete",
        ],
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


def _auth_policy_decisions(auth_context: Any) -> list[AuthPolicyDecisionResponse]:
    """Return standard authorization probes for the current actor."""

    responses: list[AuthPolicyDecisionResponse] = []
    for name, required_permissions in _AUTH_POLICY_PROBES:
        decision = evaluate_permissions(
            granted_permissions=auth_context.effective_permissions,
            required_permissions=required_permissions,
            actor_tenant_id=auth_context.tenant_id,
            target_tenant_id=auth_context.tenant_id,
            authorized_tenant_ids=auth_context.authorized_tenant_ids,
        )
        responses.append(
            AuthPolicyDecisionResponse(
                name=name,
                allowed=decision.allowed,
                reason=decision.reason,
                required_permissions=list(required_permissions),
                matched_permissions=list(decision.matched_permissions),
                missing_permissions=list(decision.missing_permissions),
                target_tenant_id=decision.target_tenant_id,
                tenant_scope=decision.tenant_scope,
            )
        )
    return responses


def _enterprise_operations_governance(
    *,
    request: Request,
    plugins: list[PluginCapabilityStatusResponse],
) -> EnterpriseOperationsGovernanceResponse:
    """Return machine-readable posture for the current enterprise roadmap slices."""

    resources = request.app.state.resources
    settings = resources.settings
    auth_context = get_auth_context(request)
    policy_decisions = _auth_policy_decisions(auth_context)

    identity_required_actions = [
        "configure_real_oidc_issuer_and_audience"
        if not settings.oidc.enabled
        else "monitor_oidc_validation_failures",
        "persist_first_class_access_policy_configuration"
        if settings.access_policy.version == "default-v1"
        else "review_access_policy_audit_trail",
    ]
    if any(not decision.allowed for decision in policy_decisions):
        identity_required_actions.append("grant_or_document_missing_control_plane_permissions")

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
    log_required_actions = [
        "configure_log_shipper_for_structured_ndjson"
        if not settings.log_shipper.enabled
        else "monitor_log_shipper_health",
        "define_search_index_mapping_and_retention_policy"
        if not settings.log_shipper.target
        else "validate_search_index_contract",
    ]
    if settings.otel_enabled and settings.otel_exporter_otlp_endpoint:
        log_required_actions.append("verify_trace_export_in_collector")
    else:
        log_required_actions.append("configure_otlp_trace_export")

    return EnterpriseOperationsGovernanceResponse(
        generated_at=datetime.now(UTC).isoformat(),
        playback_gate=EnterpriseOperationsSliceResponse(
            name="Playback Gate Promotion / Merge Policy Proof",
            status="partial",
            evidence=[
                "proof:playback:gate:enterprise package entrypoint exists",
                "playback gate workflow writes github-main-policy-expected.json",
                "check_github_main_policy.ps1 can validate live policy with gh admin auth",
            ],
            required_actions=[
                "run proof:playback:policy:enterprise:validate from an admin-authenticated host",
                "ensure Playback Gate / Playback Gate is a required protected-branch check",
                "retain playback/provider/windows proof artifacts as merge evidence",
            ],
            remaining_gaps=[
                "this API host cannot prove GitHub branch-protection state without gh admin auth",
                "provider and Windows proof promotion still depends on repeated green evidence",
            ],
        ),
        identity_authz=EnterpriseOperationsSliceResponse(
            name="Enterprise Identity / OIDC / ABAC",
            status="partial",
            evidence=[
                f"authentication_mode={auth_context.authentication_mode}",
                f"authorization_tenant_scope={auth_context.authorization_tenant_scope}",
                f"oidc_validation_enabled={settings.oidc.enabled}",
                f"oidc_token_validated={auth_context.oidc_token_validated}",
                f"access_policy_version={settings.access_policy.version}",
                (
                    "oidc_claims_present="
                    f"{auth_context.oidc_issuer is not None and auth_context.oidc_subject is not None}"
                ),
                "GET /api/v1/auth/policy exposes standard authorization probes",
            ],
            required_actions=identity_required_actions,
            remaining_gaps=[
                "OIDC is setting-gated and must be enabled per environment",
                "ABAC is currently permission plus tenant-scope based",
                "access policies are settings-managed, not database-versioned records",
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
                "LogStreamBroker backend=process_local",
                f"arq_enabled={settings.arq_enabled}",
                f"queue_name={resources.arq_queue_name or settings.arq_queue_name}",
            ],
            required_actions=[
                "introduce_replayable_event_stream_backend",
                "add_subscription_resume_offsets",
                "document_node_coordination_and_failover_semantics",
            ],
            remaining_gaps=[
                "event replay is available as Redis Streams baseline but not yet the only bus",
                "log streaming history is bounded per process",
                "node coordination and failover promotion are not fully implemented",
            ],
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
            status="partial" if settings.logging.enabled else "blocked",
            evidence=[
                f"structured_logging_enabled={settings.logging.enabled}",
                f"structured_log_path={structured_log_path}",
                f"retention_files={settings.logging.retention_files}",
                f"otel_enabled={settings.otel_enabled}",
                f"otel_endpoint_configured={bool(settings.otel_exporter_otlp_endpoint)}",
                f"log_shipper_enabled={settings.log_shipper.enabled}",
                f"log_shipper_type={settings.log_shipper.type}",
                f"log_shipper_target_configured={bool(settings.log_shipper.target)}",
                f"log_field_mapping_version={settings.log_shipper.field_mapping_version}",
                "docs/OPERATOR_LOG_PIPELINE.md defines shipping/search/replay taxonomy",
            ],
            required_actions=log_required_actions,
            remaining_gaps=[
                "shipper/search backend is configured externally and verified by contract checks",
                "trace/span coverage is still partial",
                "replay taxonomy now has Redis Streams baseline but needs end-to-end operations rollout",
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
    warnings: list[str] = []
    if auth_context.authentication_mode == "api_key":
        warnings.append("authentication is still API-key anchored")
    if auth_context.oidc_issuer is None or auth_context.oidc_subject is None:
        warnings.append("oidc claims are not present on this request")
    if auth_context.oidc_issuer is not None and not auth_context.oidc_token_validated:
        warnings.append("oidc claims were supplied by headers and were not token-validated")
    if auth_context.authorization_tenant_scope == "all":
        warnings.append("actor has global tenant scope")

    resources = request.app.state.resources
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
        access_policy_version=auth_context.access_policy_version,
        quota_policy_version=auth_context.quota_policy_version,
        permissions_model="role_scope_effective_permissions_with_tenant_scope",
        policy_source="settings",
        role_grants={
            role: list(permissions)
            for role, permissions in sorted(resources.settings.access_policy.role_grants.items())
        },
        decisions=_auth_policy_decisions(auth_context),
        warnings=warnings,
        remaining_gaps=[
            "OIDC/SSO validation is active only when FILMU_PY_OIDC enables it",
            "ABAC policy is limited to permission and tenant-scope checks",
            "policy inventory is settings-managed and not yet stored as audited database records",
        ],
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
    resolved_tenant_id = _resolve_target_tenant_id(
        auth_context=auth_context,
        requested_tenant_id=tenant_id,
        required_permissions=("tenant:quota.read",),
    )
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
        )
        for item in history
    ]
    return QueueStatusHistoryResponse(
        queue_name=queue_name,
        summary=_summarize_queue_history(history_points),
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
                    quarantined=manifest.quarantined if manifest is not None else False,
                    quarantine_reason=manifest.quarantine_reason if manifest is not None else None,
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

        responses.append(
            PluginCapabilityStatusResponse(
                name=plugin_name,
                capabilities=sorted({registration.kind.value for registration in registrations}),
                status="loaded",
                ready=ready,
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
                quarantined=manifest.quarantined if manifest is not None else False,
                quarantine_reason=manifest.quarantine_reason if manifest is not None else None,
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
    return PluginGovernanceResponse(
        summary=_plugin_governance_summary(plugins),
        plugins=plugins,
    )


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
    return _enterprise_operations_governance(request=request, plugins=plugins)


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
