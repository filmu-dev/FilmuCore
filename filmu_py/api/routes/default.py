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
    EnterpriseOperationsGovernanceResponse,
    EnterpriseOperationsSliceResponse,
    HealthResponse,
    LogsResponse,
    MessageResponse,
    PluginCapabilityStatusResponse,
    PluginEventStatusResponse,
    PluginGovernanceResponse,
    PluginGovernanceOverrideResponse,
    PluginGovernanceOverrideWriteRequest,
    PluginGovernanceSummaryResponse,
    QueueAlertResponse,
    QueueStatusHistoryPointResponse,
    QueueStatusHistoryResponse,
    QueueStatusHistorySummaryResponse,
    QueueStatusResponse,
    StatsMediaYearRelease,
    StatsResponse,
    ControlPlaneSubscriberResponse,
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
    ("policy_write", ("settings:write",)),
    ("policy_approve", ("security:policy.approve",)),
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
            "operator quarantine/revocation still needs sandbox-enforced execution boundaries",
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
        audit_decisions=record.audit_decisions,
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


def _vfs_data_plane_evidence(request: Request) -> list[str]:
    """Return bounded VFS runtime evidence for enterprise-governance posture."""

    resources = request.app.state.resources
    evidence = [
        f"vfs_catalog_server_enabled={resources.vfs_catalog_server is not None}",
        f"chunk_cache_enabled={resources.chunk_cache is not None}",
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
    policy_decisions = _auth_policy_decisions(auth_context)

    identity_required_actions = [
        "configure_real_oidc_issuer_and_audience"
        if not settings.oidc.enabled
        else "monitor_oidc_validation_failures",
        "promote_operator_managed_access_policy_revisions"
        if auth_context.access_policy_source == "settings"
        else "review_access_policy_revision_history",
    ]
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
                f"access_policy_version={auth_context.access_policy_version}",
                f"access_policy_source={auth_context.access_policy_source}",
                (
                    "oidc_claims_present="
                    f"{auth_context.oidc_issuer is not None and auth_context.oidc_subject is not None}"
                ),
                "GET /api/v1/auth/policy exposes standard authorization probes",
                "GET /api/v1/auth/policy/revisions exposes persisted policy revision inventory",
                "POST /api/v1/auth/policy/revisions/{version}/approve|reject adds approval workflow state",
                "GET /api/v1/auth/policy/audit exposes bounded audit-search history",
            ],
            required_actions=identity_required_actions,
            remaining_gaps=[
                "OIDC is setting-gated and must be enabled per environment",
                "ABAC is currently permission plus tenant-scope based",
                "policy CRUD/version workflows exist but broader resource-level ABAC rollout is still incomplete",
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
            status="partial",
            evidence=_vfs_data_plane_evidence(request),
            required_actions=[
                "repeat_multi_environment_soak_and_backpressure_runs",
                "promote_rollout_readiness_thresholds_into_merge_policy",
                "expand_tenant_attribution_for_mounted_runtime_metrics",
            ],
            remaining_gaps=[
                "mounted rollout confidence still depends on repeated proof execution",
                "cache correctness and fairness are observable but not yet policy-enforced in CI",
                "tenant attribution is not yet complete across all mounted runtime counters",
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
                "shipper/search backend still requires environment provisioning even though repo config is present",
                "trace/span coverage is still partial",
                "replay taxonomy now has Redis Streams baseline but needs end-to-end operations rollout",
            ],
        ),
        plugin_runtime_isolation=EnterpriseOperationsSliceResponse(
            name="Plugin Trust / Runtime Isolation",
            status="partial",
            evidence=[
                f"plugin_total={len(plugins)}",
                (
                    "plugin_sandbox_profiles="
                    + ",".join(sorted({plugin.sandbox_profile or 'unspecified' for plugin in plugins}))
                ),
                f"plugin_quarantined={sum(1 for plugin in plugins if plugin.quarantined)}",
                (
                    "plugin_quarantine_recommended="
                    f"{sum(1 for plugin in plugins if plugin.quarantine_recommended)}"
                ),
                f"plugin_operator_overrides={plugin_override_count}",
                "GET /api/v1/plugins/governance summarizes signature, publisher-policy, and tenancy posture",
            ],
            required_actions=[
                "promote_plugin_quarantine_and_revocation_into_persisted_operator_workflows",
                "move_runtime_plugin_execution_into_real_sandbox_boundaries",
                "require_external_plugin_artifact_provenance_and_signing_policy",
            ],
            remaining_gaps=[
                "plugin runtime isolation remains process-local rather than sandboxed execution",
                "quarantine and revocation are persisted but still enforced inside the host process",
                "external plugin artifact provenance is not yet SBOM/signing-policy complete",
            ],
        ),
        heavy_stage_workload_isolation=EnterpriseOperationsSliceResponse(
            name="Heavy-Stage Workload Isolation",
            status="partial" if settings.arq_enabled else "not_ready",
            evidence=[
                f"arq_enabled={settings.arq_enabled}",
                f"queue_name={resources.arq_queue_name or settings.arq_queue_name}",
                "worker stages include scrape_item, parse_scrape_results, rank_streams, debrid_item, and finalize_item",
                "tenant worker enqueue quotas can deny downstream stage fan-out",
                "rank_streams now executes RTN ranking/sorting in a bounded isolated executor",
                "GET /api/v1/workers/queue and /api/v1/workers/queue/history expose queue pressure",
            ],
            required_actions=[
                "split_parse_map_validate_index_stages_into_isolated_workers_or_sandboxes",
                "define_per_stage_cpu_memory_and_timeout_budgets",
                "add_crash_containment_and_retry_policy_for_heavy_jobs",
            ],
            remaining_gaps=[
                "parse/map/validate stages still need the same isolation treatment as rank_streams",
                "no per-stage memory ceiling is enforced yet",
                "crash containment exists for ranking but not every heavy stage",
            ],
        ),
        release_metadata_performance=EnterpriseOperationsSliceResponse(
            name="Release Engineering / Metadata Governance / Performance Discipline",
            status="partial",
            evidence=[
                "release workflow requires PAT-authenticated release-please updates",
                "package.json exposes security:audit, security:bandit, and perf:bench",
                "STATUS.md plus the active TODO matrix set track release, metadata, and chaos gaps explicitly",
                "scripts/run_backup_restore_proof.ps1 and playback/VFS proof gates already produce promotion evidence inputs",
            ],
            required_actions=[
                "add_sbom_signing_and_artifact_promotion_policy",
                "promote_metadata_reindex_and_reconciliation_into_a_first_class_program",
                "define_benchmark_baselines_and_chaos_regression_thresholds",
            ],
            remaining_gaps=[
                "artifact provenance and SBOM policy are not yet first-class release gates",
                "metadata and reindex governance are not yet exposed as a dedicated operator surface",
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
        policy_source=auth_context.access_policy_source,
        role_grants={
            role: list(permissions)
            for role, permissions in sorted(
                (
                    resources.access_policy_snapshot.role_grants
                    if resources.access_policy_snapshot is not None
                    else resources.settings.access_policy.role_grants
                ).items()
            )
        },
        decisions=_auth_policy_decisions(auth_context),
        warnings=warnings,
        remaining_gaps=[
            "OIDC/SSO validation is active only when FILMU_PY_OIDC enables it",
            "ABAC policy is limited to permission and tenant-scope checks",
            "policy approval/version workflows now exist, but broader ABAC resource policies still need rollout",
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
                    audit_decisions=snapshot.audit_decisions,
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
        audit_decisions=payload.audit_decisions,
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
) -> AccessPolicyAuditResponse:
    """Return bounded operator audit-search results for access-policy governance actions."""

    history = request.app.state.resources.log_stream.history()
    matches = [
        line
        for line in history
        if "security.access_policy." in line or "access_policy." in line
    ]
    bounded = matches[-limit:]
    return AccessPolicyAuditResponse(total_matches=len(matches), entries=bounded)


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
                    if is_quarantined
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
    return PluginGovernanceResponse(
        summary=_plugin_governance_summary(plugins),
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
