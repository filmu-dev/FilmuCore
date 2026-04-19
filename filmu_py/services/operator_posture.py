"""Shared operator posture builders reused by GraphQL and compatibility routes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast

from filmu_py.core.plugin_hook_queue_status import (
    PluginHookQueueHistoryPoint,
    PluginHookQueueStatusStore,
)
from filmu_py.core.queue_status import QueueStatusReader
from filmu_py.plugins.builtin.listrr import resolve_listrr_settings
from filmu_py.plugins.builtin.plex import resolve_plex_settings
from filmu_py.plugins.builtin.seerr import resolve_seerr_settings
from filmu_py.plugins.registry import PluginRegistry
from filmu_py.resources import AppResources


@dataclass(slots=True)
class ProofArtifactSnapshot:
    ref: str
    category: str
    label: str
    recorded: bool


@dataclass(slots=True)
class ControlPlaneSummarySnapshot:
    total_subscribers: int
    active_subscribers: int
    stale_subscribers: int
    error_subscribers: int
    fenced_subscribers: int
    ack_pending_subscribers: int
    stream_count: int
    group_count: int
    node_count: int
    tenant_count: int
    oldest_heartbeat_age_seconds: float | None
    status_counts: dict[str, int]
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class PluginIntegrationReadinessPluginSnapshot:
    name: str
    capability_kind: str
    status: Literal["ready", "partial", "blocked"]
    registered: bool
    enabled: bool
    configured: bool
    ready: bool
    endpoint: str | None
    endpoint_configured: bool
    config_source: str | None
    required_settings: list[str]
    missing_settings: list[str]
    contract_proof_refs: list[str]
    soak_proof_refs: list[str]
    contract_proofs: list[ProofArtifactSnapshot]
    soak_proofs: list[ProofArtifactSnapshot]
    contract_validated: bool
    soak_validated: bool
    proof_gap_count: int
    verification_status: Literal["verified", "partial", "missing"]
    verification_check_count: int
    verified_check_count: int
    missing_verification_checks: list[str]
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class PluginIntegrationReadinessSnapshot:
    generated_at: str
    status: Literal["ready", "partial", "blocked"]
    plugins: list[PluginIntegrationReadinessPluginSnapshot]
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class DownloaderProviderCandidateSnapshot:
    name: str
    source: str
    enabled: bool
    configured: bool
    selected: bool
    priority: int | None
    capabilities: list[str]


@dataclass(slots=True)
class DownloaderOrchestrationSnapshot:
    generated_at: str
    selection_mode: str
    selected_provider: str | None
    selected_provider_source: str | None
    enabled_provider_count: int
    configured_provider_count: int
    builtin_enabled_provider_count: int
    plugin_enabled_provider_count: int
    multi_provider_enabled: bool
    plugin_downloaders_registered: int
    worker_plugin_dispatch_ready: bool
    ordered_failover_ready: bool
    fanout_ready: bool
    multi_container_ready: bool
    provider_priority_order: list[str]
    providers: list[DownloaderProviderCandidateSnapshot]
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class DownloaderExecutionDeadLetterSnapshot:
    """One downloader/debrid dead-letter sample with normalized metadata."""

    stage: str
    item_id: str
    reason: str
    reason_code: str
    idempotency_key: str
    attempt: int
    queued_at: str
    provider: str | None
    failure_kind: str | None
    selected_stream_id: str | None
    item_request_id: str | None
    status_code: int | None
    retry_after_seconds: int | None


@dataclass(slots=True)
class QueueHistorySummarySnapshot:
    """Aggregate queue-history rollup for GraphQL-first operator screens."""

    point_count: int
    warning_point_count: int
    critical_point_count: int
    max_total_jobs: int
    max_ready_jobs: int
    max_retry_jobs: int
    max_dead_letter_jobs: int
    latest_alert_level: str
    dead_letter_reason_counts: dict[str, int]


@dataclass(slots=True)
class DownloaderExecutionEvidenceSnapshot:
    """GraphQL-facing downloader execution and failover evidence posture."""

    generated_at: str
    queue_name: str
    status: Literal["ready", "partial", "blocked"]
    selection_mode: str
    ordered_failover_ready: bool
    fanout_ready: bool
    provider_counts: dict[str, int]
    failure_kind_counts: dict[str, int]
    dead_letter_reason_counts: dict[str, int]
    history_summary: QueueHistorySummarySnapshot
    recent_dead_letters: list[DownloaderExecutionDeadLetterSnapshot]
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class PluginEventStatusSnapshot:
    name: str
    publisher: str | None
    publishable_events: list[str]
    hook_subscriptions: list[str]
    queued_hook_subscriptions: list[str]
    publishable_event_count: int
    hook_subscription_count: int
    queued_hook_subscription_count: int
    wiring_status: str
    hook_dispatch_mode: str
    queued_dispatch_enabled: bool
    queue_health_status: str
    queue_delivery_observed: bool
    queue_observation_count: int
    latest_queue_lag_seconds: float | None
    max_queue_lag_seconds: float | None
    successful_deliveries: int
    timeout_deliveries: int
    failed_deliveries: int
    retried_deliveries: int
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class PluginCapabilityStatusSnapshot:
    name: str
    capabilities: list[str]
    status: str
    ready: bool
    configured: bool | None
    version: str | None
    api_version: str | None
    min_host_version: str | None
    max_host_version: str | None
    publisher: str | None
    release_channel: str | None
    trust_level: str | None
    permission_scopes: list[str]
    source_sha256: str | None
    signing_key_id: str | None
    signature_present: bool
    signature_verified: bool
    signature_verification_reason: str | None
    trust_policy_decision: str | None
    trust_store_source: str | None
    sandbox_profile: str | None
    tenancy_mode: str | None
    quarantined: bool
    quarantine_reason: str | None
    publisher_policy_decision: str | None
    publisher_policy_status: str | None
    quarantine_recommended: bool
    override_state: str | None
    override_reason: str | None
    override_updated_at: str | None
    source: str | None
    warnings: list[str]
    error: str | None


@dataclass(slots=True)
class PluginGovernanceSummarySnapshot:
    total_plugins: int
    loaded_plugins: int
    load_failed_plugins: int
    ready_plugins: int
    unready_plugins: int
    healthy_plugins: int
    degraded_plugins: int
    non_builtin_plugins: int
    isolated_non_builtin_plugins: int
    quarantined_plugins: int
    quarantine_recommended_plugins: int
    unsigned_external_plugins: int
    unverified_signature_plugins: int
    publisher_policy_rejections: int
    trust_policy_rejections: int
    scraper_plugins: int
    downloader_plugins: int
    content_service_plugins: int
    event_hook_plugins: int
    override_count: int
    approved_overrides: int
    quarantined_overrides: int
    revoked_overrides: int
    sandbox_profile_counts: dict[str, int]
    tenancy_mode_counts: dict[str, int]
    runtime_policy_mode: str
    runtime_isolation_ready: bool
    recommended_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class PluginGovernanceSnapshot:
    summary: PluginGovernanceSummarySnapshot
    plugins: list[PluginCapabilityStatusSnapshot]


@dataclass(slots=True)
class ControlPlaneAutomationSnapshot:
    generated_at: str
    enabled: bool
    runner_status: Literal["disabled", "running", "degraded", "stopped"]
    interval_seconds: int
    active_within_seconds: int
    pending_min_idle_ms: int
    claim_limit: int
    max_claim_passes: int
    consumer_group: str
    consumer_name: str
    service_attached: bool
    backplane_attached: bool
    last_run_at: str | None
    last_success_at: str | None
    last_failure_at: str | None
    consecutive_failures: int
    last_error: str | None
    remediation_updated_subscribers: int
    rewound_subscribers: int
    claimed_pending_events: int
    claim_passes: int
    pending_count_after: int | None
    summary: ControlPlaneSummarySnapshot
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class ControlPlaneSubscriberSnapshot:
    stream_name: str
    group_name: str
    consumer_name: str
    node_id: str
    tenant_id: str | None
    status: str
    last_read_offset: str | None
    last_delivered_event_id: str | None
    last_acked_event_id: str | None
    ack_pending: bool
    fenced: bool
    last_error: str | None
    claimed_at: str
    last_heartbeat_at: str
    created_at: str
    updated_at: str


@dataclass(slots=True)
class ControlPlaneReplayBackplaneSnapshot:
    generated_at: str
    status: Literal["ready", "partial", "blocked"]
    event_backplane: str
    stream_name: str
    consumer_group: str
    replay_maxlen: int
    claim_limit: int
    max_claim_passes: int
    attached: bool
    pending_count: int
    oldest_event_id: str | None
    latest_event_id: str | None
    consumer_counts: dict[str, int]
    consumer_count: int
    has_pending_backlog: bool
    proof_refs: list[str]
    proof_artifacts: list[ProofArtifactSnapshot]
    proof_ready: bool
    pending_recovery_ready: bool
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class ControlPlaneRecoveryReadinessSnapshot:
    generated_at: str
    status: Literal["ready", "partial", "blocked"]
    active_within_seconds: int
    stale_subscribers: int
    ack_pending_subscribers: int
    pending_count: int
    consumer_count: int
    automation_enabled: bool
    automation_healthy: bool
    replay_attached: bool
    proof_refs: list[str]
    proof_artifacts: list[ProofArtifactSnapshot]
    proof_ready: bool
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class VfsCatalogGovernanceSummarySnapshot:
    active_watch_sessions: int
    reconnect_requests: int
    reconnect_delta_served: int
    reconnect_snapshot_fallbacks: int
    reconnect_failures: int
    snapshots_served: int
    deltas_served: int
    heartbeats_served: int
    problem_events: int
    request_stream_failures: int
    refresh_attempts: int
    refresh_succeeded: int
    refresh_provider_failures: int
    refresh_validation_failures: int
    inline_refresh_requests: int
    inline_refresh_succeeded: int
    inline_refresh_failed: int


@dataclass(slots=True)
class VfsCatalogGovernanceSnapshot:
    generated_at: str
    status: Literal["ready", "partial", "blocked"]
    counters: dict[str, int]
    summary: VfsCatalogGovernanceSummarySnapshot
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class VfsMountDiagnosticsSnapshot:
    generated_at: str
    status: Literal["ready", "partial", "blocked"]
    supplier_attached: bool
    server_attached: bool
    current_generation_id: str | None
    current_published_at: str | None
    history_generation_ids: list[str]
    history_generation_count: int
    delta_history_ready: bool
    active_watch_sessions: int
    snapshots_served: int
    deltas_served: int
    reconnect_delta_served: int
    reconnect_snapshot_fallbacks: int
    reconnect_failures: int
    request_stream_failures: int
    problem_events: int
    refresh_provider_failures: int
    refresh_validation_failures: int
    required_actions: list[str]
    remaining_gaps: list[str]


def _build_proof_artifacts(
    refs: list[str],
    *,
    category: str,
    label: str,
) -> list[ProofArtifactSnapshot]:
    return [
        ProofArtifactSnapshot(
            ref=ref,
            category=category,
            label=label,
            recorded=bool(str(ref).strip()),
        )
        for ref in refs
        if str(ref).strip()
    ]


def _plugin_load_report_maps(app_state: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return plugin-load successes and failures keyed by plugin name when available."""

    report = getattr(app_state, "plugin_load_report", None)
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
    resources: AppResources,
) -> tuple[bool, bool | None, list[str]]:
    """Return operator-facing readiness for built-in/runtime-managed plugins."""

    warnings: list[str] = []
    configured: bool | None = None

    if plugin_name == "stremthru":
        stremthru = resources.settings.downloaders.stremthru
        configured_url = str(getattr(stremthru, "base_url", getattr(stremthru, "url", ""))).strip()
        configured = bool(stremthru.enabled and stremthru.token.strip() and configured_url)
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
    plugin: PluginCapabilityStatusSnapshot,
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
    plugins: list[PluginCapabilityStatusSnapshot],
    *,
    runtime_policy: Any,
) -> PluginGovernanceSummarySnapshot:
    """Return a bounded plugin trust/isolation rollup for operators and GraphQL."""

    sandbox_profile_counts: dict[str, int] = {}
    tenancy_mode_counts: dict[str, int] = {}
    recommended_actions: set[str] = set()
    capability_counts = {
        "scraper": 0,
        "downloader": 0,
        "content_service": 0,
        "event_hook": 0,
    }
    override_counts = {
        "approved": 0,
        "quarantined": 0,
        "revoked": 0,
    }
    non_builtin_plugins = [
        plugin
        for plugin in plugins
        if plugin.release_channel != "builtin" and plugin.source != "builtin"
    ]
    governed_plugins = non_builtin_plugins
    for plugin in plugins:
        sandbox_profile = plugin.sandbox_profile or "unspecified"
        sandbox_profile_counts[sandbox_profile] = sandbox_profile_counts.get(sandbox_profile, 0) + 1
        tenancy_mode = plugin.tenancy_mode or "unspecified"
        tenancy_mode_counts[tenancy_mode] = tenancy_mode_counts.get(tenancy_mode, 0) + 1
        recommended_actions.update(_plugin_recommended_actions(plugin))
        for capability in plugin.capabilities:
            if capability in capability_counts:
                capability_counts[capability] += 1
        if plugin.override_state in override_counts:
            override_counts[plugin.override_state] += 1

    runtime_isolation_ready = (
        runtime_policy.health_rollup_enabled
        and runtime_policy.enforcement_mode == "isolated_runtime_required"
        and runtime_policy.require_strict_signatures
        and runtime_policy.require_source_digest
        and bool(runtime_policy.proof_refs)
        and all(plugin.ready for plugin in governed_plugins)
        and not any(
            plugin.quarantined or plugin.quarantine_recommended for plugin in governed_plugins
        )
        and not any(plugin.status == "load_failed" for plugin in governed_plugins)
        and not any(
            plugin.publisher_policy_decision in {"rejected", "untrusted"}
            or plugin.trust_policy_decision in {"rejected", "untrusted"}
            for plugin in governed_plugins
        )
        and all(
            (plugin.sandbox_profile in runtime_policy.allowed_non_builtin_sandbox_profiles)
            and (plugin.tenancy_mode in runtime_policy.allowed_non_builtin_tenancy_modes)
            and (not runtime_policy.require_source_digest or bool(plugin.source_sha256))
            and (not runtime_policy.require_strict_signatures or plugin.signature_verified)
            for plugin in non_builtin_plugins
        )
    )

    return PluginGovernanceSummarySnapshot(
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
        scraper_plugins=capability_counts["scraper"],
        downloader_plugins=capability_counts["downloader"],
        content_service_plugins=capability_counts["content_service"],
        event_hook_plugins=capability_counts["event_hook"],
        override_count=sum(override_counts.values()),
        approved_overrides=override_counts["approved"],
        quarantined_overrides=override_counts["quarantined"],
        revoked_overrides=override_counts["revoked"],
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


def plugin_settings_payload(resources: AppResources) -> Mapping[str, Any]:
    """Return the plugin initialization settings payload used by the runtime."""

    payload = resources.plugin_settings_payload
    if isinstance(payload, Mapping):
        return payload
    return cast(Mapping[str, Any], resources.settings.to_compatibility_dict())


def _mapping_at(payload: Mapping[str, Any], *path: str) -> Mapping[str, Any] | None:
    current: object = payload
    for segment in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(segment)
    return cast(Mapping[str, Any], current) if isinstance(current, Mapping) else None


def _proof_ref_list(payload: Mapping[str, Any] | None, key: str) -> list[str]:
    if payload is None:
        return []
    value = payload.get(key)
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item).strip()]


def empty_control_plane_summary() -> ControlPlaneSummarySnapshot:
    """Return a normalized empty control-plane summary when no service is attached."""

    return ControlPlaneSummarySnapshot(
        total_subscribers=0,
        active_subscribers=0,
        stale_subscribers=0,
        error_subscribers=0,
        fenced_subscribers=0,
        ack_pending_subscribers=0,
        stream_count=0,
        group_count=0,
        node_count=0,
        tenant_count=0,
        oldest_heartbeat_age_seconds=None,
        status_counts={},
        required_actions=["attach_control_plane_service"],
        remaining_gaps=["durable replay/control-plane ownership is not configured"],
    )


async def build_downloader_execution_evidence_posture(
    resources: AppResources,
    *,
    history_limit: int = 20,
    dead_letter_limit: int = 20,
) -> DownloaderExecutionEvidenceSnapshot:
    """Return downloader/debrid execution evidence from queue history and DLQ samples."""

    def _coerce_metadata_int(metadata: Mapping[str, Any], key: str) -> int | None:
        value = metadata.get(key)
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return value

    queue_name = resources.arq_queue_name or resources.settings.arq_queue_name
    redis = resources.arq_redis or resources.redis
    reader = QueueStatusReader(redis, queue_name=queue_name)
    history_summary = await reader.history_summary(limit=history_limit)
    dead_letter_samples = await reader.dead_letter_samples(limit=dead_letter_limit, stage="debrid_item")

    provider_counts: dict[str, int] = {}
    failure_kind_counts: dict[str, int] = {}
    normalized_dead_letters: list[DownloaderExecutionDeadLetterSnapshot] = []
    for sample in dead_letter_samples:
        provider_raw = sample.metadata.get("provider")
        provider = str(provider_raw).strip() if isinstance(provider_raw, str) else ""
        if provider:
            provider_counts[provider] = provider_counts.get(provider, 0) + 1
        failure_kind_raw = sample.metadata.get("failure_kind")
        failure_kind = str(failure_kind_raw).strip() if isinstance(failure_kind_raw, str) else ""
        if failure_kind:
            failure_kind_counts[failure_kind] = failure_kind_counts.get(failure_kind, 0) + 1
        status_code = _coerce_metadata_int(sample.metadata, "status_code")
        retry_after_seconds = _coerce_metadata_int(sample.metadata, "retry_after_seconds")
        normalized_dead_letters.append(
            DownloaderExecutionDeadLetterSnapshot(
                stage=sample.stage,
                item_id=sample.item_id,
                reason=sample.reason,
                reason_code=sample.reason_code,
                idempotency_key=sample.idempotency_key,
                attempt=int(sample.attempt),
                queued_at=sample.queued_at,
                provider=provider or None,
                failure_kind=failure_kind or None,
                selected_stream_id=(
                    str(sample.metadata["selected_stream_id"]).strip() or None
                    if isinstance(sample.metadata.get("selected_stream_id"), str)
                    else None
                ),
                item_request_id=(
                    str(sample.metadata["item_request_id"]).strip() or None
                    if isinstance(sample.metadata.get("item_request_id"), str)
                    else None
                ),
                status_code=status_code,
                retry_after_seconds=retry_after_seconds,
            )
        )

    orchestration = build_downloader_orchestration_posture(resources)
    required_actions = list(orchestration.required_actions)
    remaining_gaps = list(orchestration.remaining_gaps)
    if not normalized_dead_letters:
        required_actions.append("record_downloader_failover_dead_letter_evidence")
        remaining_gaps.append("no retained downloader dead-letter evidence is available yet")

    status: Literal["ready", "partial", "blocked"] = (
        "ready"
        if orchestration.ordered_failover_ready and not remaining_gaps
        else "partial"
        if orchestration.configured_provider_count > 0 or normalized_dead_letters
        else "blocked"
    )

    return DownloaderExecutionEvidenceSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        queue_name=queue_name,
        status=status,
        selection_mode=orchestration.selection_mode,
        ordered_failover_ready=orchestration.ordered_failover_ready,
        fanout_ready=orchestration.fanout_ready,
        provider_counts=dict(sorted(provider_counts.items())),
        failure_kind_counts=dict(sorted(failure_kind_counts.items())),
        dead_letter_reason_counts=dict(sorted(history_summary.dead_letter_reason_counts.items())),
        history_summary=QueueHistorySummarySnapshot(
            point_count=history_summary.point_count,
            warning_point_count=history_summary.warning_point_count,
            critical_point_count=history_summary.critical_point_count,
            max_total_jobs=history_summary.max_total_jobs,
            max_ready_jobs=history_summary.max_ready_jobs,
            max_retry_jobs=history_summary.max_retry_jobs,
            max_dead_letter_jobs=history_summary.max_dead_letter_jobs,
            latest_alert_level=history_summary.latest_alert_level,
            dead_letter_reason_counts=dict(sorted(history_summary.dead_letter_reason_counts.items())),
        ),
        recent_dead_letters=normalized_dead_letters,
        required_actions=list(dict.fromkeys(required_actions)),
        remaining_gaps=list(dict.fromkeys(remaining_gaps)),
    )


def normalize_control_plane_summary(summary: object | None) -> ControlPlaneSummarySnapshot:
    """Normalize service DTOs into one shared control-plane summary read model."""

    if summary is None:
        return empty_control_plane_summary()
    typed_summary: Any = summary
    return ControlPlaneSummarySnapshot(
        total_subscribers=int(typed_summary.total_subscribers),
        active_subscribers=int(typed_summary.active_subscribers),
        stale_subscribers=int(typed_summary.stale_subscribers),
        error_subscribers=int(typed_summary.error_subscribers),
        fenced_subscribers=int(typed_summary.fenced_subscribers),
        ack_pending_subscribers=int(typed_summary.ack_pending_subscribers),
        stream_count=int(typed_summary.stream_count),
        group_count=int(typed_summary.group_count),
        node_count=int(typed_summary.node_count),
        tenant_count=int(typed_summary.tenant_count),
        oldest_heartbeat_age_seconds=typed_summary.oldest_heartbeat_age_seconds,
        status_counts=dict(typed_summary.status_counts),
        required_actions=list(typed_summary.required_actions),
        remaining_gaps=list(typed_summary.remaining_gaps),
    )


async def build_control_plane_summary_posture(
    resources: AppResources,
    *,
    active_within_seconds: int = 120,
) -> ControlPlaneSummarySnapshot:
    """Build the shared control-plane summary posture."""

    service = resources.control_plane_service
    if service is None:
        return empty_control_plane_summary()
    return normalize_control_plane_summary(
        await service.summarize_subscribers(active_within_seconds=active_within_seconds)
    )


def _plugin_integration_row(
    *,
    name: str,
    capability_kind: str,
    registered: bool,
    enabled: bool,
    endpoint: str | None,
    config_source: str | None,
    required_settings: list[str],
    configured_values: Mapping[str, Any],
    contract_proof_refs: list[str],
    soak_proof_refs: list[str],
    event_row: PluginEventStatusSnapshot | None = None,
) -> PluginIntegrationReadinessPluginSnapshot:
    missing_settings = [
        setting_name
        for setting_name in required_settings
        if not str(configured_values.get(setting_name) or "").strip()
        and not (
            setting_name == "list_ids"
            and isinstance(configured_values.get(setting_name), list)
            and bool(configured_values.get(setting_name))
        )
        and not (
            setting_name == "section_ids"
            and isinstance(configured_values.get(setting_name), list)
            and bool(configured_values.get(setting_name))
        )
    ]
    endpoint_text = str(endpoint or "").strip()
    endpoint_configured = bool(endpoint_text)
    configured = enabled and not missing_settings
    contract_proofs = _build_proof_artifacts(
        list(contract_proof_refs),
        category="plugin_contract",
        label=f"{name} contract proof",
    )
    soak_proofs = _build_proof_artifacts(
        list(soak_proof_refs),
        category="plugin_soak",
        label=f"{name} soak proof",
    )
    sanitized_contract_proof_refs = [proof.ref for proof in contract_proofs]
    sanitized_soak_proof_refs = [proof.ref for proof in soak_proofs]
    contract_validated = bool(contract_proofs)
    soak_validated = bool(soak_proofs)
    ready = registered and configured and contract_validated and soak_validated
    verification_checks = {
        "registered": registered,
        "configured": configured,
        "contract_proof": contract_validated,
        "soak_proof": soak_validated,
    }
    if (
        capability_kind == "event_hook"
        and event_row is not None
        and event_row.queued_dispatch_enabled
        and event_row.queued_hook_subscription_count > 0
    ):
        verification_checks["queued_delivery"] = event_row.queue_delivery_observed
    status: Literal["ready", "partial", "blocked"] = (
        "ready" if ready else "partial" if enabled or registered else "blocked"
    )
    verified_check_count = sum(1 for verified in verification_checks.values() if verified)
    verification_check_count = len(verification_checks)
    missing_verification_checks = [
        check_name for check_name, verified in verification_checks.items() if not verified
    ]
    verification_status: Literal["verified", "partial", "missing"] = (
        "verified"
        if verified_check_count == verification_check_count
        else "partial"
        if verified_check_count > 0
        else "missing"
    )
    required_actions: list[str] = []
    remaining_gaps: list[str] = []
    if not registered:
        required_actions.append(f"register_{name}_builtin_plugin")
        remaining_gaps.append(f"{name} is not registered in the active plugin runtime")
    if enabled and missing_settings:
        required_actions.append(f"configure_{name}_plugin_contract")
        remaining_gaps.append(
            f"{name} is enabled but missing required settings: {', '.join(missing_settings)}"
        )
    if enabled and not contract_validated:
        required_actions.append(f"validate_{name}_plugin_endpoint_contract")
        remaining_gaps.append(
            f"{name} has no retained endpoint contract validation evidence"
        )
    if enabled and not soak_validated:
        required_actions.append(f"record_{name}_plugin_soak_evidence")
        remaining_gaps.append(f"{name} has no retained soak evidence")
    if (
        enabled
        and capability_kind == "event_hook"
        and event_row is not None
        and event_row.queued_dispatch_enabled
        and event_row.queued_hook_subscription_count > 0
        and not event_row.queue_delivery_observed
    ):
        required_actions.append(f"record_{name}_queued_hook_delivery")
        remaining_gaps.append(
            f"{name} has queued hook dispatch enabled but no retained queued delivery history"
        )
    if not enabled:
        required_actions.append(f"enable_{name}_integration")
        remaining_gaps.append(f"{name} integration is not enabled in runtime settings")
    return PluginIntegrationReadinessPluginSnapshot(
        name=name,
        capability_kind=capability_kind,
        status=status,
        registered=registered,
        enabled=enabled,
        configured=configured,
        ready=ready,
        endpoint=endpoint_text or None,
        endpoint_configured=endpoint_configured,
        config_source=config_source,
        required_settings=list(required_settings),
        missing_settings=missing_settings,
        contract_proof_refs=sanitized_contract_proof_refs,
        soak_proof_refs=sanitized_soak_proof_refs,
        contract_proofs=contract_proofs,
        soak_proofs=soak_proofs,
        contract_validated=contract_validated,
        soak_validated=soak_validated,
        proof_gap_count=int(not contract_validated) + int(not soak_validated),
        verification_status=verification_status,
        verification_check_count=verification_check_count,
        verified_check_count=verified_check_count,
        missing_verification_checks=missing_verification_checks,
        required_actions=required_actions,
        remaining_gaps=remaining_gaps,
    )


async def build_plugin_integration_readiness_posture(
    resources: AppResources,
) -> PluginIntegrationReadinessSnapshot:
    """Return readiness and proof posture for the supported runtime integrations."""

    plugin_registry = resources.plugin_registry
    payload = plugin_settings_payload(resources)
    scraper_names = (
        {
            str(getattr(plugin, "plugin_name", type(plugin).__name__)).strip()
            for plugin in plugin_registry.get_scrapers()
        }
        if plugin_registry is not None
        else set()
    )
    content_service_names = (
        {
            str(getattr(plugin, "plugin_name", type(plugin).__name__)).strip()
            for plugin in plugin_registry.get_content_services()
        }
        if plugin_registry is not None
        else set()
    )
    notification_names = (
        {
            str(getattr(plugin, "plugin_name", type(plugin).__name__)).strip()
            for plugin in plugin_registry.get_notifications()
        }
        if plugin_registry is not None
        else set()
    )
    downloader_names = (
        {
            str(getattr(plugin, "plugin_name", type(plugin).__name__)).strip()
            for plugin in plugin_registry.get_downloaders()
        }
        if plugin_registry is not None
        else set()
    )
    event_hook_names = (
        {
            str(getattr(plugin, "plugin_name", type(plugin).__name__)).strip()
            for plugin in plugin_registry.get_event_hooks()
        }
        if plugin_registry is not None
        else set()
    )
    downloaders_settings = _mapping_at(payload, "downloaders")
    notifications_settings = _mapping_at(payload, "notifications")
    metadata_settings = _mapping_at(payload, "metadata")
    tmdb_settings = _mapping_at(payload, "metadata", "tmdb")
    tvdb_settings = _mapping_at(payload, "metadata", "tvdb")
    realdebrid_settings = _mapping_at(payload, "downloaders", "real_debrid")
    alldebrid_settings = _mapping_at(payload, "downloaders", "all_debrid")
    debridlink_settings = _mapping_at(payload, "downloaders", "debrid_link")
    stremthru_settings = _mapping_at(payload, "downloaders", "stremthru")

    seerr_source = (
        "content.seerr"
        if isinstance(payload.get("content"), Mapping)
        and isinstance(cast(Mapping[str, Any], payload.get("content")).get("seerr"), Mapping)
        else "content.overseerr"
        if isinstance(payload.get("content"), Mapping)
        and isinstance(cast(Mapping[str, Any], payload.get("content")).get("overseerr"), Mapping)
        else None
    )
    seerr_settings = resolve_seerr_settings(payload)
    listrr_source = (
        "content.listrr"
        if isinstance(payload.get("content"), Mapping)
        and isinstance(cast(Mapping[str, Any], payload.get("content")).get("listrr"), Mapping)
        else None
    )
    listrr_settings = resolve_listrr_settings(payload)
    plex_source = (
        "plex"
        if isinstance(payload.get("plex"), dict)
        else "notifications.plex"
        if isinstance(payload.get("notifications"), dict)
        and isinstance(cast(dict[str, Any], payload.get("notifications")).get("plex"), dict)
        else "updaters.plex"
        if isinstance(payload.get("updaters"), dict)
        and isinstance(cast(dict[str, Any], payload.get("updaters")).get("plex"), dict)
        else None
    )
    plex_settings = resolve_plex_settings(dict(payload))

    event_rows = await build_plugin_event_status_posture(resources)
    event_by_name = {row.name: row for row in event_rows}

    rows = [
        _plugin_integration_row(
            name="comet",
            capability_kind="scraper",
            registered="comet" in scraper_names,
            enabled=bool(resources.settings.scraping.comet.enabled),
            endpoint=resources.settings.scraping.comet.url,
            config_source="scraping.comet",
            required_settings=["url"],
            configured_values={
                "url": resources.settings.scraping.comet.url,
            },
            contract_proof_refs=list(resources.settings.scraping.comet.contract_proof_refs),
            soak_proof_refs=list(resources.settings.scraping.comet.soak_proof_refs),
            event_row=event_by_name.get("comet"),
        ),
        _plugin_integration_row(
            name="seerr",
            capability_kind="content_service",
            registered="seerr" in content_service_names,
            enabled=bool(seerr_settings.get("enabled", False)),
            endpoint=cast(str | None, seerr_settings.get("url") or seerr_settings.get("base_url")),
            config_source=seerr_source,
            required_settings=["url", "api_key"],
            configured_values={
                "url": seerr_settings.get("url") or seerr_settings.get("base_url"),
                "api_key": seerr_settings.get("api_key"),
            },
            contract_proof_refs=list(cast(list[str], seerr_settings.get("contract_proof_refs", []))),
            soak_proof_refs=list(cast(list[str], seerr_settings.get("soak_proof_refs", []))),
            event_row=event_by_name.get("seerr"),
        ),
        _plugin_integration_row(
            name="listrr",
            capability_kind="content_service",
            registered="listrr" in content_service_names,
            enabled=bool(listrr_settings.get("enabled", False)),
            endpoint=cast(str | None, listrr_settings.get("url") or listrr_settings.get("base_url")),
            config_source=listrr_source,
            required_settings=["url", "list_ids"],
            configured_values={
                "url": listrr_settings.get("url") or listrr_settings.get("base_url"),
                "list_ids": listrr_settings.get("list_ids"),
            },
            contract_proof_refs=list(cast(list[str], listrr_settings.get("contract_proof_refs", []))),
            soak_proof_refs=list(cast(list[str], listrr_settings.get("soak_proof_refs", []))),
            event_row=event_by_name.get("listrr"),
        ),
        _plugin_integration_row(
            name="plex",
            capability_kind="event_hook",
            registered="plex" in event_hook_names,
            enabled=bool(plex_settings.get("enabled", False)),
            endpoint=cast(str | None, plex_settings.get("url") or plex_settings.get("base_url")),
            config_source=plex_source,
            required_settings=["url", "token"],
            configured_values={
                "url": plex_settings.get("url") or plex_settings.get("base_url"),
                "token": plex_settings.get("token"),
            },
            contract_proof_refs=list(cast(list[str], plex_settings.get("contract_proof_refs", []))),
            soak_proof_refs=list(cast(list[str], plex_settings.get("soak_proof_refs", []))),
            event_row=event_by_name.get("plex"),
        ),
        _plugin_integration_row(
            name="notifications",
            capability_kind="notification",
            registered="notifications" in notification_names,
            enabled=bool(
                resources.settings.notifications.enabled
                and (
                    resources.settings.notifications.discord_webhook_url
                    or resources.settings.notifications.webhook_url
                    or next(
                        (
                            candidate
                            for candidate in resources.settings.notifications.service_urls
                            if str(candidate).strip()
                        ),
                        None,
                    )
                )
            ),
            endpoint=(
                resources.settings.notifications.discord_webhook_url
                or resources.settings.notifications.webhook_url
                or next(
                    (
                        candidate
                        for candidate in resources.settings.notifications.service_urls
                        if str(candidate).strip()
                    ),
                    None,
                )
            ),
            config_source="notifications" if notifications_settings is not None else None,
            required_settings=["delivery_target"],
            configured_values={
                "delivery_target": (
                    resources.settings.notifications.discord_webhook_url
                    or resources.settings.notifications.webhook_url
                    or next(
                        (
                            candidate
                            for candidate in resources.settings.notifications.service_urls
                            if str(candidate).strip()
                        ),
                        "",
                    )
                ),
            },
            contract_proof_refs=_proof_ref_list(notifications_settings, "contract_proof_refs"),
            soak_proof_refs=_proof_ref_list(notifications_settings, "soak_proof_refs"),
            event_row=event_by_name.get("notifications"),
        ),
        _plugin_integration_row(
            name="tmdb",
            capability_kind="metadata_provider",
            registered=True,
            enabled=bool(resources.settings.tmdb_api_key.strip()),
            endpoint="https://api.themoviedb.org/3",
            config_source="tmdb_api_key",
            required_settings=["api_key"],
            configured_values={
                "api_key": resources.settings.tmdb_api_key,
            },
            contract_proof_refs=(
                _proof_ref_list(tmdb_settings, "contract_proof_refs")
                or _proof_ref_list(metadata_settings, "tmdb_contract_proof_refs")
            ),
            soak_proof_refs=(
                _proof_ref_list(tmdb_settings, "soak_proof_refs")
                or _proof_ref_list(metadata_settings, "tmdb_soak_proof_refs")
            ),
        ),
        _plugin_integration_row(
            name="tvdb",
            capability_kind="metadata_provider",
            registered=resources.media_service is not None,
            enabled=bool(tvdb_settings and tvdb_settings.get("enabled", False)),
            endpoint="https://api4.thetvdb.com/v4",
            config_source="metadata.tvdb",
            required_settings=[],
            configured_values={},
            contract_proof_refs=(
                _proof_ref_list(tvdb_settings, "contract_proof_refs")
                or _proof_ref_list(metadata_settings, "tvdb_contract_proof_refs")
            ),
            soak_proof_refs=(
                _proof_ref_list(tvdb_settings, "soak_proof_refs")
                or _proof_ref_list(metadata_settings, "tvdb_soak_proof_refs")
            ),
        ),
        _plugin_integration_row(
            name="realdebrid",
            capability_kind="downloader",
            registered=True,
            enabled=bool(
                resources.settings.downloaders.real_debrid.enabled
                and resources.settings.downloaders.real_debrid.api_key.strip()
            ),
            endpoint="https://api.real-debrid.com/rest/1.0",
            config_source="downloaders.real_debrid",
            required_settings=["api_key"],
            configured_values={
                "api_key": resources.settings.downloaders.real_debrid.api_key,
            },
            contract_proof_refs=_proof_ref_list(realdebrid_settings, "contract_proof_refs"),
            soak_proof_refs=_proof_ref_list(realdebrid_settings, "soak_proof_refs"),
        ),
        _plugin_integration_row(
            name="alldebrid",
            capability_kind="downloader",
            registered=True,
            enabled=bool(
                resources.settings.downloaders.all_debrid.enabled
                and resources.settings.downloaders.all_debrid.api_key.strip()
            ),
            endpoint="https://api.alldebrid.com",
            config_source="downloaders.all_debrid",
            required_settings=["api_key"],
            configured_values={
                "api_key": resources.settings.downloaders.all_debrid.api_key,
            },
            contract_proof_refs=_proof_ref_list(alldebrid_settings, "contract_proof_refs"),
            soak_proof_refs=_proof_ref_list(alldebrid_settings, "soak_proof_refs"),
        ),
        _plugin_integration_row(
            name="debridlink",
            capability_kind="downloader",
            registered=True,
            enabled=bool(
                resources.settings.downloaders.debrid_link.enabled
                and resources.settings.downloaders.debrid_link.api_key.strip()
            ),
            endpoint="https://debrid-link.com/api/v2",
            config_source="downloaders.debrid_link",
            required_settings=["api_key"],
            configured_values={
                "api_key": resources.settings.downloaders.debrid_link.api_key,
            },
            contract_proof_refs=_proof_ref_list(debridlink_settings, "contract_proof_refs"),
            soak_proof_refs=_proof_ref_list(debridlink_settings, "soak_proof_refs"),
        ),
        _plugin_integration_row(
            name="stremthru",
            capability_kind="downloader",
            registered="stremthru" in downloader_names,
            enabled=bool(
                resources.settings.downloaders.stremthru.enabled
                and resources.settings.downloaders.stremthru.url.strip()
                and resources.settings.downloaders.stremthru.token.strip()
            ),
            endpoint=resources.settings.downloaders.stremthru.url,
            config_source=(
                "downloaders.stremthru" if downloaders_settings is not None else None
            ),
            required_settings=["url", "token"],
            configured_values={
                "url": resources.settings.downloaders.stremthru.url,
                "token": resources.settings.downloaders.stremthru.token,
            },
            contract_proof_refs=_proof_ref_list(stremthru_settings, "contract_proof_refs"),
            soak_proof_refs=_proof_ref_list(stremthru_settings, "soak_proof_refs"),
            event_row=event_by_name.get("stremthru"),
        ),
    ]
    required_actions = sorted({action for row in rows for action in row.required_actions})
    remaining_gaps = list(dict.fromkeys(gap for row in rows for gap in row.remaining_gaps))
    ready_rows = [row for row in rows if row.ready]
    status: Literal["ready", "partial", "blocked"] = (
        "ready"
        if len(ready_rows) == len(rows)
        else "partial"
        if ready_rows or any(row.enabled for row in rows)
        else "blocked"
    )
    return PluginIntegrationReadinessSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        status=status,
        plugins=rows,
        required_actions=required_actions,
        remaining_gaps=remaining_gaps,
    )


def build_downloader_orchestration_posture(
    resources: AppResources,
) -> DownloaderOrchestrationSnapshot:
    """Return the current downloader orchestration posture and breadth gaps."""

    settings = resources.settings
    provider_priority = {
        name: index
        for index, name in enumerate(settings.orchestration.downloader_provider_priority, start=1)
    }
    providers: list[DownloaderProviderCandidateSnapshot] = []
    builtin_candidates: list[DownloaderProviderCandidateSnapshot] = []
    provider_entries = (
        ("realdebrid", settings.downloaders.real_debrid),
        ("alldebrid", settings.downloaders.all_debrid),
        ("debridlink", settings.downloaders.debrid_link),
    )
    for priority, (name, config) in enumerate(provider_entries, start=1):
        configured = bool(config.api_key.strip())
        enabled = bool(config.enabled and configured)
        candidate = DownloaderProviderCandidateSnapshot(
            name=name,
            source="builtin",
            enabled=enabled,
            configured=configured,
            selected=False,
            priority=provider_priority.get(name, priority),
            capabilities=["magnet_add", "file_select", "status_poll", "download_links"],
        )
        builtin_candidates.append(candidate)
        providers.append(candidate)

    plugin_registry = resources.plugin_registry
    plugin_downloaders_registered = 0
    if plugin_registry is not None:
        for plugin in plugin_registry.get_downloaders():
            plugin_downloaders_registered += 1
            plugin_name = str(getattr(plugin, "plugin_name", type(plugin).__name__))
            configured = True
            enabled = True
            if plugin_name == "stremthru":
                stremthru = settings.downloaders.stremthru
                configured = bool(
                    stremthru.enabled and stremthru.token.strip() and stremthru.url.strip()
                )
                enabled = configured
            providers.append(
                DownloaderProviderCandidateSnapshot(
                    name=plugin_name,
                    source="plugin",
                    enabled=enabled,
                    configured=configured,
                    selected=False,
                    priority=provider_priority.get(plugin_name),
                    capabilities=["magnet_add", "status_poll", "download_links"],
                )
            )

    enabled_candidates = sorted(
        [candidate for candidate in providers if candidate.enabled],
        key=lambda candidate: (
            candidate.priority if candidate.priority is not None else 10_000,
            candidate.name,
        ),
    )
    selected_provider = enabled_candidates[0].name if enabled_candidates else None
    selected_provider_source = enabled_candidates[0].source if enabled_candidates else None
    if selected_provider is not None:
        providers = [
            DownloaderProviderCandidateSnapshot(
                name=candidate.name,
                source=candidate.source,
                enabled=candidate.enabled,
                configured=candidate.configured,
                selected=candidate.name == selected_provider,
                priority=candidate.priority,
                capabilities=list(candidate.capabilities),
            )
            for candidate in providers
        ]

    plugin_policy_ready = any(
        candidate.source == "plugin" and candidate.enabled for candidate in providers
    )
    enabled_provider_count = sum(1 for candidate in providers if candidate.enabled)
    configured_provider_count = sum(1 for candidate in providers if candidate.configured)
    builtin_enabled_provider_count = sum(
        1 for candidate in providers if candidate.source == "builtin" and candidate.enabled
    )
    plugin_enabled_provider_count = sum(
        1 for candidate in providers if candidate.source == "plugin" and candidate.enabled
    )
    multi_provider_enabled = sum(1 for candidate in builtin_candidates if candidate.enabled) > 1
    worker_plugin_dispatch_ready = plugin_policy_ready
    ordered_failover_ready = settings.orchestration.downloader_selection_mode == "ordered_failover"
    fanout_ready = bool(
        ordered_failover_ready
        and len(enabled_candidates) > 1
        and (plugin_downloaders_registered == 0 or plugin_policy_ready)
    )
    multi_container_ready = True
    provider_priority_order = [
        name
        for name, _priority in sorted(provider_priority.items(), key=lambda item: (item[1], item[0]))
    ]

    required_actions: list[str] = []
    remaining_gaps: list[str] = []
    if selected_provider is None:
        required_actions.append("configure_at_least_one_builtin_downloader_provider")
        remaining_gaps.append(
            "debrid worker execution has no configured builtin downloader provider"
        )
    if multi_provider_enabled:
        if ordered_failover_ready and not fanout_ready:
            required_actions.append("promote_ordered_failover_into_policy_driven_fanout")
            remaining_gaps.append(
                "multiple builtin downloaders now support ordered failover, but not policy-driven fan-out"
            )
        elif not ordered_failover_ready:
            required_actions.append(
                "replace_fixed_priority_builtin_selection_with_policy_driven_fanout"
            )
            remaining_gaps.append(
                "multiple builtin downloaders are enabled but debrid_item still selects by fixed priority"
            )
    if plugin_downloaders_registered > 0 and not worker_plugin_dispatch_ready:
        required_actions.append("wire_registered_downloader_plugins_into_debrid_worker")
        remaining_gaps.append(
            "registered downloader plugins are visible in the registry but not yet dispatched by debrid_item"
        )

    return DownloaderOrchestrationSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        selection_mode=(
            "ordered_failover_policy_fanout"
            if ordered_failover_ready and worker_plugin_dispatch_ready and fanout_ready
            else "ordered_failover_with_plugin_policy"
            if ordered_failover_ready and worker_plugin_dispatch_ready
            else "ordered_failover"
            if ordered_failover_ready
            else "fixed_priority_builtin_then_plugin_policy"
            if worker_plugin_dispatch_ready
            else "fixed_priority_builtin_only"
        ),
        selected_provider=selected_provider,
        selected_provider_source=selected_provider_source,
        enabled_provider_count=enabled_provider_count,
        configured_provider_count=configured_provider_count,
        builtin_enabled_provider_count=builtin_enabled_provider_count,
        plugin_enabled_provider_count=plugin_enabled_provider_count,
        multi_provider_enabled=multi_provider_enabled,
        plugin_downloaders_registered=plugin_downloaders_registered,
        worker_plugin_dispatch_ready=worker_plugin_dispatch_ready,
        ordered_failover_ready=ordered_failover_ready,
        fanout_ready=fanout_ready,
        multi_container_ready=multi_container_ready,
        provider_priority_order=provider_priority_order,
        providers=providers,
        required_actions=required_actions,
        remaining_gaps=remaining_gaps,
    )


async def _plugin_hook_history_by_name(
    resources: AppResources,
    *,
    limit: int = 200,
) -> dict[str, list[PluginHookQueueHistoryPoint]]:
    """Return recent queued plugin-hook history grouped by plugin name."""

    redis = resources.arq_redis
    if redis is None:
        return {}
    history = await PluginHookQueueStatusStore(
        redis,
        queue_name=resources.arq_queue_name,
    ).history(limit=limit)
    grouped: dict[str, list[PluginHookQueueHistoryPoint]] = {}
    for point in history:
        grouped.setdefault(point.plugin_name, []).append(point)
    return grouped


async def build_plugin_event_status_posture(
    resources: AppResources,
) -> list[PluginEventStatusSnapshot]:
    """Return declared publishable events, queued delivery health, and subscriptions."""

    plugin_registry = resources.plugin_registry
    if plugin_registry is None:
        return []

    publishable_by_plugin = plugin_registry.publishable_events_by_plugin()
    subscriptions_by_plugin = plugin_registry.hook_subscriptions_by_plugin()
    plugin_runtime = resources.settings.plugin_runtime
    queued_events = frozenset(plugin_runtime.queued_hook_events)
    hook_history_by_name = await _plugin_hook_history_by_name(resources)
    rows: list[PluginEventStatusSnapshot] = []
    for plugin_name in sorted(plugin_registry.all_plugin_names()):
        manifest = plugin_registry.manifest(plugin_name)
        publishable_events = list(publishable_by_plugin.get(plugin_name, ()))
        hook_subscriptions = list(subscriptions_by_plugin.get(plugin_name, ()))
        queued_hook_subscriptions = [
            event_name for event_name in hook_subscriptions if event_name in queued_events
        ]
        history = hook_history_by_name.get(plugin_name, [])
        latest = history[0] if history else None
        wiring_status = (
            "bidirectional"
            if publishable_events and hook_subscriptions
            else "publisher_only"
            if publishable_events
            else "subscriber_only"
            if hook_subscriptions
            else "idle"
        )
        queued_dispatch_enabled = bool(
            plugin_runtime.hook_dispatch_mode == "queued" and queued_hook_subscriptions
        )
        queue_delivery_observed = bool(history)
        successful_deliveries = sum(
            1
            for point in history
            if point.successful_hooks > 0 and point.timeout_hooks == 0 and point.failed_hooks == 0
        )
        timeout_deliveries = sum(1 for point in history if point.timeout_hooks > 0)
        failed_deliveries = sum(
            1 for point in history if point.failed_hooks > 0 or point.matched_hooks <= 0
        )
        retried_deliveries = sum(1 for point in history if point.attempt > 1)
        required_actions: list[str] = []
        remaining_gaps: list[str] = []
        queue_health_status = "inactive"
        if plugin_runtime.hook_dispatch_mode == "queued" and hook_subscriptions:
            if not queued_hook_subscriptions:
                queue_health_status = "not_configured"
                required_actions.append(f"configure_{plugin_name}_queued_hook_events")
                remaining_gaps.append(
                    f"{plugin_name} subscribes to hook events but none are configured for queued dispatch"
                )
            elif not queue_delivery_observed:
                queue_health_status = "pending_proof"
                required_actions.append(f"record_{plugin_name}_queued_hook_delivery")
                remaining_gaps.append(
                    f"{plugin_name} has queued hook dispatch configured but no retained delivery history"
                )
            elif latest is not None and (latest.failed_hooks > 0 or latest.matched_hooks <= 0):
                queue_health_status = "blocked"
                required_actions.append(f"stabilize_{plugin_name}_queued_hook_delivery")
                remaining_gaps.append(
                    f"{plugin_name} has failed queued hook deliveries in retained history"
                )
            elif latest is not None and (latest.timeout_hooks > 0 or latest.attempt > 1):
                queue_health_status = "degraded"
                required_actions.append(f"stabilize_{plugin_name}_queued_hook_delivery")
                remaining_gaps.append(
                    f"{plugin_name} queued hook deliveries require retries or time out"
                )
            else:
                queue_health_status = "ready"

        rows.append(
            PluginEventStatusSnapshot(
                name=plugin_name,
                publisher=manifest.publisher if manifest is not None else None,
                publishable_events=publishable_events,
                hook_subscriptions=hook_subscriptions,
                queued_hook_subscriptions=queued_hook_subscriptions,
                publishable_event_count=len(publishable_events),
                hook_subscription_count=len(hook_subscriptions),
                queued_hook_subscription_count=len(queued_hook_subscriptions),
                wiring_status=wiring_status,
                hook_dispatch_mode=plugin_runtime.hook_dispatch_mode,
                queued_dispatch_enabled=queued_dispatch_enabled,
                queue_health_status=queue_health_status,
                queue_delivery_observed=queue_delivery_observed,
                queue_observation_count=len(history),
                latest_queue_lag_seconds=latest.queue_lag_seconds if latest is not None else None,
                max_queue_lag_seconds=(
                    max(point.queue_lag_seconds for point in history) if history else None
                ),
                successful_deliveries=successful_deliveries,
                timeout_deliveries=timeout_deliveries,
                failed_deliveries=failed_deliveries,
                retried_deliveries=retried_deliveries,
                required_actions=required_actions,
                remaining_gaps=remaining_gaps,
            )
        )
    return rows


async def build_plugin_governance_posture(
    resources: AppResources,
    *,
    app_state: Any,
) -> PluginGovernanceSnapshot:
    """Return plugin trust, quarantine, and isolation posture for GraphQL/REST."""

    plugin_registry: PluginRegistry | None = resources.plugin_registry
    loaded_report, failed_report = _plugin_load_report_maps(app_state)
    override_service = resources.plugin_governance_service
    overrides = await override_service.list_overrides() if override_service is not None else {}

    if plugin_registry is None:
        failed_plugins = [
            PluginCapabilityStatusSnapshot(
                name=plugin_name,
                capabilities=[],
                status="load_failed",
                ready=False,
                configured=None,
                version=None,
                api_version=None,
                min_host_version=None,
                max_host_version=None,
                publisher=None,
                release_channel=None,
                trust_level=None,
                permission_scopes=[],
                source_sha256=None,
                signing_key_id=None,
                signature_present=False,
                signature_verified=False,
                signature_verification_reason=None,
                trust_policy_decision=None,
                trust_store_source=None,
                sandbox_profile=None,
                tenancy_mode=None,
                quarantined=False,
                quarantine_reason=None,
                publisher_policy_decision=None,
                publisher_policy_status=None,
                quarantine_recommended=False,
                override_state=None,
                override_reason=None,
                override_updated_at=None,
                source=getattr(failure, "source", None),
                warnings=[],
                error=getattr(failure, "reason", None),
            )
            for plugin_name, failure in sorted(failed_report.items())
        ]
        return PluginGovernanceSnapshot(
            summary=_plugin_governance_summary(
                failed_plugins,
                runtime_policy=resources.settings.plugin_runtime,
            ),
            plugins=failed_plugins,
        )

    plugins: list[PluginCapabilityStatusSnapshot] = []
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
            plugins.append(
                PluginCapabilityStatusSnapshot(
                    name=plugin_name,
                    capabilities=[],
                    status="load_failed",
                    ready=False,
                    configured=None,
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
                    override_state=override.state if override is not None else None,
                    override_reason=override.reason if override is not None else None,
                    override_updated_at=(
                        override.updated_at.isoformat() if override is not None else None
                    ),
                    source=getattr(failure, "source", None),
                    warnings=warnings,
                    error=getattr(failure, "reason", None),
                    signature_present=bool(signature_fields["signature_present"]),
                    signature_verified=bool(signature_fields["signature_verified"]),
                    signature_verification_reason=signature_fields["signature_verification_reason"],
                    trust_policy_decision=signature_fields["trust_policy_decision"],
                    trust_store_source=signature_fields["trust_store_source"],
                )
            )
            continue

        is_revoked = override is not None and override.state == "revoked"
        is_quarantined = override is not None and override.state == "quarantined"
        if is_revoked:
            warnings.append("operator override revoked this plugin")
        elif is_quarantined:
            warnings.append("operator override quarantined this plugin")
        plugins.append(
            PluginCapabilityStatusSnapshot(
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
                signature_present=bool(signature_fields["signature_present"]),
                signature_verified=bool(signature_fields["signature_verified"]),
                signature_verification_reason=signature_fields["signature_verification_reason"],
                trust_policy_decision=signature_fields["trust_policy_decision"],
                trust_store_source=signature_fields["trust_store_source"],
                sandbox_profile=manifest.sandbox_profile if manifest is not None else None,
                tenancy_mode=manifest.tenancy_mode if manifest is not None else None,
                quarantined=(
                    is_quarantined
                    if override is not None
                    else (manifest.quarantined if manifest is not None else False)
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
                override_state=override.state if override is not None else None,
                override_reason=override.reason if override is not None else None,
                override_updated_at=(
                    override.updated_at.isoformat() if override is not None else None
                ),
                source=(
                    manifest.distribution
                    if manifest is not None
                    else getattr(success, "source", None)
                ),
                warnings=warnings,
                error=None,
            )
        )

    return PluginGovernanceSnapshot(
        summary=_plugin_governance_summary(
            plugins,
            runtime_policy=resources.settings.plugin_runtime,
        ),
        plugins=plugins,
    )


async def build_control_plane_automation_posture(
    resources: AppResources,
) -> ControlPlaneAutomationSnapshot:
    """Return current background replay/control-plane automation posture."""

    controller = resources.control_plane_automation
    automation = resources.settings.control_plane.automation
    if controller is not None:
        controller_snapshot = controller.snapshot()
        summary = (
            normalize_control_plane_summary(controller_snapshot.summary)
            if controller_snapshot.summary is not None
            else await build_control_plane_summary_posture(
                resources,
                active_within_seconds=controller_snapshot.active_within_seconds,
            )
        )
    else:
        controller_snapshot = None
        summary = await build_control_plane_summary_posture(
            resources,
            active_within_seconds=automation.active_within_seconds,
        )

    required_actions = list(summary.required_actions)
    remaining_gaps = list(summary.remaining_gaps)
    runner_status: Literal["disabled", "running", "degraded", "stopped"] = (
        controller_snapshot.runner_status
        if controller_snapshot is not None
        else "disabled"
        if not automation.enabled
        else "stopped"
    )
    if not automation.enabled:
        required_actions.append("enable_control_plane_automation")
        remaining_gaps.append("background replay/control-plane automation is disabled")
    if resources.control_plane_service is None:
        required_actions.append("attach_control_plane_service")
        remaining_gaps.append("durable replay/control-plane ownership is not configured")
    if resources.replay_backplane is None:
        required_actions.append("attach_redis_replay_backplane")
        remaining_gaps.append("durable replay pending-entry recovery is not attached")
    return ControlPlaneAutomationSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        enabled=bool(controller_snapshot.enabled) if controller_snapshot is not None else automation.enabled,
        runner_status=runner_status,
        interval_seconds=(
            controller_snapshot.interval_seconds
            if controller_snapshot is not None
            else automation.interval_seconds
        ),
        active_within_seconds=(
            controller_snapshot.active_within_seconds
            if controller_snapshot is not None
            else automation.active_within_seconds
        ),
        pending_min_idle_ms=(
            controller_snapshot.pending_min_idle_ms
            if controller_snapshot is not None
            else automation.pending_min_idle_ms
        ),
        claim_limit=(
            controller_snapshot.claim_limit if controller_snapshot is not None else automation.claim_limit
        ),
        max_claim_passes=(
            controller_snapshot.max_claim_passes
            if controller_snapshot is not None
            else automation.max_claim_passes
        ),
        consumer_group=(
            controller_snapshot.consumer_group
            if controller_snapshot is not None
            else resources.settings.control_plane.consumer_group
        ),
        consumer_name=(
            controller_snapshot.consumer_name
            if controller_snapshot is not None
            else automation.consumer_name
        ),
        service_attached=(
            controller_snapshot.service_attached
            if controller_snapshot is not None
            else resources.control_plane_service is not None
        ),
        backplane_attached=(
            controller_snapshot.backplane_attached
            if controller_snapshot is not None
            else resources.replay_backplane is not None
        ),
        last_run_at=(
            controller_snapshot.last_run_at.isoformat()
            if controller_snapshot is not None and controller_snapshot.last_run_at
            else None
        ),
        last_success_at=(
            controller_snapshot.last_success_at.isoformat()
            if controller_snapshot is not None and controller_snapshot.last_success_at
            else None
        ),
        last_failure_at=(
            controller_snapshot.last_failure_at.isoformat()
            if controller_snapshot is not None and controller_snapshot.last_failure_at
            else None
        ),
        consecutive_failures=(
            controller_snapshot.consecutive_failures if controller_snapshot is not None else 0
        ),
        last_error=controller_snapshot.last_error if controller_snapshot is not None else None,
        remediation_updated_subscribers=(
            controller_snapshot.remediation_updated_subscribers if controller_snapshot is not None else 0
        ),
        rewound_subscribers=(
            controller_snapshot.rewound_subscribers if controller_snapshot is not None else 0
        ),
        claimed_pending_events=(
            controller_snapshot.claimed_pending_events if controller_snapshot is not None else 0
        ),
        claim_passes=controller_snapshot.claim_passes if controller_snapshot is not None else 0,
        pending_count_after=(
            controller_snapshot.pending_count_after if controller_snapshot is not None else None
        ),
        summary=summary,
        required_actions=sorted(set(required_actions)),
        remaining_gaps=list(dict.fromkeys(remaining_gaps)),
    )


def _control_plane_record_is_fenced(record: Any) -> bool:
    return bool(record.status == "fenced" or "consumer_fenced" in str(record.last_error or ""))


async def build_control_plane_subscribers_posture(
    resources: AppResources,
    *,
    active_within_seconds: int = 120,
) -> list[ControlPlaneSubscriberSnapshot]:
    """Return typed durable subscriber rows for GraphQL-first operator consoles."""

    service = resources.control_plane_service
    if service is None:
        return []
    records = await service.list_subscribers(active_within_seconds=active_within_seconds)
    return [
        ControlPlaneSubscriberSnapshot(
            stream_name=record.stream_name,
            group_name=record.group_name,
            consumer_name=record.consumer_name,
            node_id=record.node_id,
            tenant_id=record.tenant_id,
            status=record.status,
            last_read_offset=record.last_read_offset,
            last_delivered_event_id=record.last_delivered_event_id,
            last_acked_event_id=record.last_acked_event_id,
            ack_pending=bool(
                record.last_delivered_event_id
                and record.last_delivered_event_id != record.last_acked_event_id
            ),
            fenced=_control_plane_record_is_fenced(record),
            last_error=record.last_error,
            claimed_at=record.claimed_at.isoformat(),
            last_heartbeat_at=record.last_heartbeat_at.isoformat(),
            created_at=record.created_at.isoformat(),
            updated_at=record.updated_at.isoformat(),
        )
        for record in records
    ]


async def build_control_plane_replay_backplane_posture(
    resources: AppResources,
) -> ControlPlaneReplayBackplaneSnapshot:
    """Return replay-backplane readiness and pending-delivery posture."""

    settings = resources.settings.control_plane
    backplane = resources.replay_backplane
    proof_artifacts = _build_proof_artifacts(
        list(settings.proof_refs),
        category="control_plane_rollout",
        label="control-plane replay backplane proof",
    )
    proof_refs = [artifact.ref for artifact in proof_artifacts]
    required_actions: list[str] = []
    remaining_gaps: list[str] = []
    pending_count = 0
    oldest_event_id: str | None = None
    latest_event_id: str | None = None
    consumer_counts: dict[str, int] = {}
    attached = backplane is not None and hasattr(backplane, "pending_summary")

    if settings.event_backplane != "redis_stream":
        required_actions.append("promote_control_plane_event_backplane_to_redis_stream")
        remaining_gaps.append("durable control-plane replay still uses a non-redis backplane")
    if not attached:
        required_actions.append("attach_redis_replay_backplane")
        remaining_gaps.append("redis replay backplane is not attached to the runtime")
    else:
        try:
            summary = await cast(Any, backplane).pending_summary(
                group_name=settings.consumer_group
            )
        except Exception as exc:
            required_actions.append("repair_replay_backplane_pending_summary")
            remaining_gaps.append(f"replay pending-summary probe failed: {exc}")
        else:
            pending_count = int(summary.pending_count)
            oldest_event_id = summary.oldest_event_id
            latest_event_id = summary.latest_event_id
            consumer_counts = dict(summary.consumer_counts)
    if not proof_artifacts:
        required_actions.append("record_control_plane_redis_consumer_group_evidence")
        remaining_gaps.append("redis consumer-group rollout has no retained production evidence")

    ready = settings.event_backplane == "redis_stream" and attached and bool(proof_artifacts)
    status: Literal["ready", "partial", "blocked"] = (
        "ready"
        if ready and not remaining_gaps
        else "partial"
        if settings.event_backplane == "redis_stream" or attached
        else "blocked"
    )
    return ControlPlaneReplayBackplaneSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        status=status,
        event_backplane=settings.event_backplane,
        stream_name=settings.event_stream_name,
        consumer_group=settings.consumer_group,
        replay_maxlen=settings.event_replay_maxlen,
        claim_limit=settings.automation.claim_limit,
        max_claim_passes=settings.automation.max_claim_passes,
        attached=attached,
        pending_count=pending_count,
        oldest_event_id=oldest_event_id,
        latest_event_id=latest_event_id,
        consumer_counts=dict(sorted(consumer_counts.items())),
        consumer_count=len(consumer_counts),
        has_pending_backlog=pending_count > 0,
        proof_refs=proof_refs,
        proof_artifacts=proof_artifacts,
        proof_ready=bool(proof_artifacts),
        pending_recovery_ready=bool(
            settings.event_backplane == "redis_stream"
            and attached
            and settings.automation.claim_limit > 0
            and settings.automation.max_claim_passes > 0
        ),
        required_actions=list(dict.fromkeys(required_actions)),
        remaining_gaps=list(dict.fromkeys(remaining_gaps)),
    )


async def build_control_plane_recovery_readiness_posture(
    resources: AppResources,
    *,
    active_within_seconds: int = 120,
) -> ControlPlaneRecoveryReadinessSnapshot:
    """Return one graph-friendly recovery readiness rollup for Director consoles."""

    summary = await build_control_plane_summary_posture(
        resources,
        active_within_seconds=active_within_seconds,
    )
    automation = await build_control_plane_automation_posture(resources)
    replay = await build_control_plane_replay_backplane_posture(resources)
    required_actions = list(
        dict.fromkeys(
            list(summary.required_actions)
            + list(automation.required_actions)
            + list(replay.required_actions)
        )
    )
    remaining_gaps = list(
        dict.fromkeys(
            list(summary.remaining_gaps)
            + list(automation.remaining_gaps)
            + list(replay.remaining_gaps)
        )
    )
    proof_refs = list(replay.proof_refs)
    status: Literal["ready", "partial", "blocked"] = (
        "ready"
        if (
            automation.enabled
            and automation.runner_status == "running"
            and automation.service_attached
            and automation.backplane_attached
            and replay.attached
            and replay.proof_ready
            and summary.stale_subscribers == 0
            and summary.ack_pending_subscribers == 0
        )
        else "partial"
        if (
            automation.enabled
            or replay.attached
            or summary.total_subscribers > 0
            or replay.pending_count > 0
        )
        else "blocked"
    )
    return ControlPlaneRecoveryReadinessSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        status=status,
        active_within_seconds=active_within_seconds,
        stale_subscribers=summary.stale_subscribers,
        ack_pending_subscribers=summary.ack_pending_subscribers,
        pending_count=replay.pending_count,
        consumer_count=replay.consumer_count,
        automation_enabled=automation.enabled,
        automation_healthy=automation.runner_status == "running" and automation.backplane_attached,
        replay_attached=replay.attached,
        proof_refs=proof_refs,
        proof_artifacts=list(replay.proof_artifacts),
        proof_ready=replay.proof_ready,
        required_actions=required_actions,
        remaining_gaps=remaining_gaps,
    )


def build_vfs_catalog_governance_posture(resources: AppResources) -> VfsCatalogGovernanceSnapshot:
    """Return graph-friendly governance counters for the live VFS gRPC catalog server."""

    counter_keys = (
        "vfs_catalog_watch_sessions_active",
        "vfs_catalog_reconnect_requested",
        "vfs_catalog_reconnect_delta_served",
        "vfs_catalog_reconnect_snapshot_fallback",
        "vfs_catalog_reconnect_failures",
        "vfs_catalog_snapshots_served",
        "vfs_catalog_deltas_served",
        "vfs_catalog_heartbeats_served",
        "vfs_catalog_problem_events",
        "vfs_catalog_request_stream_failures",
        "vfs_catalog_refresh_attempts",
        "vfs_catalog_refresh_succeeded",
        "vfs_catalog_refresh_provider_failures",
        "vfs_catalog_refresh_validation_failed",
        "vfs_catalog_inline_refresh_requests",
        "vfs_catalog_inline_refresh_succeeded",
        "vfs_catalog_inline_refresh_failed",
    )
    server = resources.vfs_catalog_server
    counters = (
        server.build_governance_snapshot()
        if server is not None and hasattr(server, "build_governance_snapshot")
        else dict.fromkeys(counter_keys, 0)
    )
    required_actions: list[str] = []
    remaining_gaps: list[str] = []
    if server is None:
        required_actions.append("attach_vfs_catalog_grpc_server")
        remaining_gaps.append("live FilmuVFS gRPC governance counters are not attached")
    if counters.get("vfs_catalog_reconnect_failures", 0) > 0:
        required_actions.append("investigate_vfs_catalog_reconnect_failures")
        remaining_gaps.append("vfs catalog reconnect failures were observed in the current runtime")
    if counters.get("vfs_catalog_problem_events", 0) > 0:
        required_actions.append("investigate_vfs_catalog_problem_events")
        remaining_gaps.append("vfs catalog problem events were emitted by the current runtime")
    if counters.get("vfs_catalog_request_stream_failures", 0) > 0:
        required_actions.append("repair_vfs_catalog_request_stream_failures")
        remaining_gaps.append("vfs catalog request-stream failures were observed in the current runtime")
    if counters.get("vfs_catalog_refresh_provider_failures", 0) > 0:
        required_actions.append("reduce_vfs_catalog_refresh_provider_failures")
        remaining_gaps.append("vfs catalog refresh provider failures were observed in the current runtime")
    if counters.get("vfs_catalog_refresh_validation_failed", 0) > 0:
        required_actions.append("repair_vfs_catalog_refresh_validation_failures")
        remaining_gaps.append("vfs catalog refresh validation failures were observed in the current runtime")

    status: Literal["ready", "partial", "blocked"] = (
        "ready"
        if server is not None and not remaining_gaps
        else "partial"
        if server is not None
        else "blocked"
    )
    return VfsCatalogGovernanceSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        status=status,
        counters=dict(sorted(counters.items())),
        summary=VfsCatalogGovernanceSummarySnapshot(
            active_watch_sessions=int(counters.get("vfs_catalog_watch_sessions_active", 0)),
            reconnect_requests=int(counters.get("vfs_catalog_reconnect_requested", 0)),
            reconnect_delta_served=int(counters.get("vfs_catalog_reconnect_delta_served", 0)),
            reconnect_snapshot_fallbacks=int(
                counters.get("vfs_catalog_reconnect_snapshot_fallback", 0)
            ),
            reconnect_failures=int(counters.get("vfs_catalog_reconnect_failures", 0)),
            snapshots_served=int(counters.get("vfs_catalog_snapshots_served", 0)),
            deltas_served=int(counters.get("vfs_catalog_deltas_served", 0)),
            heartbeats_served=int(counters.get("vfs_catalog_heartbeats_served", 0)),
            problem_events=int(counters.get("vfs_catalog_problem_events", 0)),
            request_stream_failures=int(counters.get("vfs_catalog_request_stream_failures", 0)),
            refresh_attempts=int(counters.get("vfs_catalog_refresh_attempts", 0)),
            refresh_succeeded=int(counters.get("vfs_catalog_refresh_succeeded", 0)),
            refresh_provider_failures=int(
                counters.get("vfs_catalog_refresh_provider_failures", 0)
            ),
            refresh_validation_failures=int(
                counters.get("vfs_catalog_refresh_validation_failed", 0)
            ),
            inline_refresh_requests=int(counters.get("vfs_catalog_inline_refresh_requests", 0)),
            inline_refresh_succeeded=int(counters.get("vfs_catalog_inline_refresh_succeeded", 0)),
            inline_refresh_failed=int(counters.get("vfs_catalog_inline_refresh_failed", 0)),
        ),
        required_actions=list(dict.fromkeys(required_actions)),
        remaining_gaps=list(dict.fromkeys(remaining_gaps)),
    )


async def build_vfs_mount_diagnostics_posture(
    resources: AppResources,
) -> VfsMountDiagnosticsSnapshot:
    """Return shared VFS mount diagnostics for GraphQL-first Director/operator views."""

    supplier = resources.vfs_catalog_supplier
    governance = build_vfs_catalog_governance_posture(resources)
    snapshot = None
    history_generation_ids: list[str] = []
    required_actions = list(governance.required_actions)
    remaining_gaps = list(governance.remaining_gaps)

    if supplier is None:
        required_actions.append("attach_vfs_catalog_supplier")
        remaining_gaps.append("mounted VFS catalog supplier is not attached")
    else:
        try:
            snapshot = await supplier.build_snapshot()
        except Exception as exc:
            required_actions.append("repair_vfs_catalog_supplier_snapshot_build")
            remaining_gaps.append(f"vfs catalog supplier snapshot build failed: {exc}")
        else:
            if hasattr(supplier, "history_generation_ids"):
                try:
                    history_generation_ids = list(await supplier.history_generation_ids())
                except Exception as exc:
                    required_actions.append("repair_vfs_catalog_supplier_history")
                    remaining_gaps.append(f"vfs catalog supplier history probe failed: {exc}")
            if len(history_generation_ids) < 2:
                required_actions.append("retain_vfs_catalog_delta_history")
                remaining_gaps.append(
                    "vfs catalog supplier has not retained enough published generations for delta inspection"
                )

    status: Literal["ready", "partial", "blocked"] = (
        "ready"
        if supplier is not None and resources.vfs_catalog_server is not None and not remaining_gaps
        else "partial"
        if supplier is not None or resources.vfs_catalog_server is not None
        else "blocked"
    )
    return VfsMountDiagnosticsSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        status=status,
        supplier_attached=supplier is not None,
        server_attached=resources.vfs_catalog_server is not None,
        current_generation_id=(snapshot.generation_id if snapshot is not None else None),
        current_published_at=(
            snapshot.published_at.isoformat() if snapshot is not None else None
        ),
        history_generation_ids=history_generation_ids,
        history_generation_count=len(history_generation_ids),
        delta_history_ready=len(history_generation_ids) >= 2,
        active_watch_sessions=governance.summary.active_watch_sessions,
        snapshots_served=governance.summary.snapshots_served,
        deltas_served=governance.summary.deltas_served,
        reconnect_delta_served=governance.summary.reconnect_delta_served,
        reconnect_snapshot_fallbacks=governance.summary.reconnect_snapshot_fallbacks,
        reconnect_failures=governance.summary.reconnect_failures,
        request_stream_failures=governance.summary.request_stream_failures,
        problem_events=governance.summary.problem_events,
        refresh_provider_failures=governance.summary.refresh_provider_failures,
        refresh_validation_failures=governance.summary.refresh_validation_failures,
        required_actions=list(dict.fromkeys(required_actions)),
        remaining_gaps=list(dict.fromkeys(remaining_gaps)),
    )
