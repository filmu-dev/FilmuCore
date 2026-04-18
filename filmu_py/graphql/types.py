"""GraphQL types for the filmu-python compatibility surface."""

from __future__ import annotations

from enum import Enum, StrEnum

import strawberry
from strawberry.scalars import JSON


@strawberry.enum
class MediaKind(Enum):
    """Intentional GraphQL media-kind enum decoupled from REST alias history."""

    MOVIE = "movie"
    SHOW = "show"
    SEASON = "season"
    EPISODE = "episode"


@strawberry.enum
class GQLRecoveryMechanism(StrEnum):
    """Intentional automatic-recovery mechanism for future GraphQL consumers."""

    NONE = "none"
    ORPHAN_RECOVERY = "orphan_recovery"
    COOLDOWN_RECOVERY = "cooldown_recovery"


@strawberry.enum
class GQLRecoveryTargetStage(StrEnum):
    """Pipeline stage targeted by automatic recovery."""

    NONE = "none"
    INDEX = "index"
    SCRAPE = "scrape"
    PARSE = "parse"
    FINALIZE = "finalize"


@strawberry.enum
class GQLActiveStreamRole(StrEnum):
    """Selectable persisted active-stream roles for graph control-plane mutations."""

    DIRECT = "direct"
    HLS = "hls"


@strawberry.type
class GQLHealthCheck:
    """Structured health status for GraphQL clients."""

    service: str
    status: str


@strawberry.type
class GQLMediaItem:
    """Minimal media item type scaffold for compatibility evolution."""

    id: strawberry.ID
    external_ref: str
    title: str
    state: str
    media_type: str = strawberry.field(name="mediaType")
    media_kind: MediaKind = strawberry.field(name="mediaKind")
    tmdb_id: int | None = strawberry.field(name="tmdbId", default=None)
    tvdb_id: int | None = strawberry.field(name="tvdbId", default=None)
    imdb_id: str | None = strawberry.field(name="imdbId", default=None)
    parent_tmdb_id: int | None = strawberry.field(name="parentTmdbId", default=None)
    parent_tvdb_id: int | None = strawberry.field(name="parentTvdbId", default=None)
    show_title: str | None = strawberry.field(name="showTitle", default=None)
    season_number: int | None = strawberry.field(name="seasonNumber", default=None)
    episode_number: int | None = strawberry.field(name="episodeNumber", default=None)
    poster_path: str | None = strawberry.field(name="posterPath", default=None)
    aired_at: str | None = strawberry.field(name="airedAt", default=None)


@strawberry.type
class GQLCalendarReleaseWindow:
    """Typed release-window projection for GraphQL-first calendar consumers."""

    next_aired: str | None = strawberry.field(name="nextAired", default=None)
    last_aired: str | None = strawberry.field(name="lastAired", default=None)


@strawberry.type
class GQLCalendarEntry:
    """Intentional GraphQL calendar entry unconstrained by REST compatibility shape."""

    item_id: strawberry.ID = strawberry.field(name="itemId")
    show_title: str = strawberry.field(name="showTitle")
    item_type: str = strawberry.field(name="itemType")
    aired_at: str | None = strawberry.field(name="airedAt")
    last_state: str = strawberry.field(name="lastState")
    season: int | None = None
    episode: int | None = None
    tmdb_id: int | None = strawberry.field(name="tmdbId", default=None)
    tvdb_id: int | None = strawberry.field(name="tvdbId", default=None)
    imdb_id: str | None = strawberry.field(name="imdbId", default=None)
    parent_tmdb_id: int | None = strawberry.field(name="parentTmdbId", default=None)
    parent_tvdb_id: int | None = strawberry.field(name="parentTvdbId", default=None)
    release_data: str | None = strawberry.field(name="releaseData", default=None)
    release_window: GQLCalendarReleaseWindow | None = strawberry.field(
        name="releaseWindow",
        default=None,
    )


@strawberry.type
class GQLProofArtifact:
    """Typed retained evidence reference exposed to Director consoles."""

    ref: str
    category: str
    label: str
    recorded: bool


@strawberry.type
class GQLObservabilityConvergenceSummary:
    """Rollup counters for cross-process observability readiness."""

    pipeline_stage_count: int = strawberry.field(name="pipelineStageCount")
    ready_stage_count: int = strawberry.field(name="readyStageCount")
    production_evidence_ready: bool = strawberry.field(name="productionEvidenceReady")
    grpc_rust_trace_ready: bool = strawberry.field(name="grpcRustTraceReady")
    otlp_export_ready: bool = strawberry.field(name="otlpExportReady")
    search_index_ready: bool = strawberry.field(name="searchIndexReady")
    alert_rollout_ready: bool = strawberry.field(name="alertRolloutReady")


@strawberry.type
class GQLObservabilityConvergence:
    """Typed GraphQL view over cross-process log/search/trace convergence."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    structured_logging_enabled: bool = strawberry.field(name="structuredLoggingEnabled")
    structured_log_path: str = strawberry.field(name="structuredLogPath")
    otel_enabled: bool = strawberry.field(name="otelEnabled")
    otel_endpoint_configured: bool = strawberry.field(name="otelEndpointConfigured")
    log_shipper_enabled: bool = strawberry.field(name="logShipperEnabled")
    log_shipper_type: str = strawberry.field(name="logShipperType")
    log_shipper_target_configured: bool = strawberry.field(name="logShipperTargetConfigured")
    log_shipper_healthcheck_configured: bool = strawberry.field(
        name="logShipperHealthcheckConfigured"
    )
    search_backend: str = strawberry.field(name="searchBackend")
    environment_shipping_enabled: bool = strawberry.field(name="environmentShippingEnabled")
    alerting_enabled: bool = strawberry.field(name="alertingEnabled")
    rust_trace_correlation_enabled: bool = strawberry.field(name="rustTraceCorrelationEnabled")
    correlation_contract_complete: bool = strawberry.field(name="correlationContractComplete")
    proof_refs: list[str] = strawberry.field(name="proofRefs")
    required_correlation_fields: list[str] = strawberry.field(name="requiredCorrelationFields")
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")
    trace_context_headers: list[str] = strawberry.field(name="traceContextHeaders")
    correlation_headers: list[str] = strawberry.field(name="correlationHeaders")
    shared_cross_process_headers: list[str] = strawberry.field(name="sharedCrossProcessHeaders")
    expected_correlation_fields: list[str] = strawberry.field(name="expectedCorrelationFields")
    expected_correlation_fields_ready: bool = strawberry.field(
        name="expectedCorrelationFieldsReady"
    )
    summary: GQLObservabilityConvergenceSummary
    missing_expected_correlation_fields: list[str] = strawberry.field(
        name="missingExpectedCorrelationFields"
    )
    grpc_bind_address: str = strawberry.field(name="grpcBindAddress")
    grpc_service_name: str = strawberry.field(name="grpcServiceName")
    otlp_endpoint: str | None = strawberry.field(name="otlpEndpoint", default=None)
    log_shipper_target: str | None = strawberry.field(name="logShipperTarget", default=None)
    proof_artifacts: list[GQLProofArtifact] = strawberry.field(name="proofArtifacts")
    pipeline_stages: list[GQLObservabilityPipelineStage] = strawberry.field(
        name="pipelineStages"
    )


@strawberry.type
class GQLObservabilityPipelineStage:
    """One typed observability pipeline stage in the GraphQL convergence view."""

    name: str
    status: str
    configured: bool
    ready: bool
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLObservabilityRolloutSummary:
    """Compact GraphQL rollout summary for cross-process observability closure."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    pipeline_stage_count: int = strawberry.field(name="pipelineStageCount")
    ready_stage_count: int = strawberry.field(name="readyStageCount")
    production_evidence_count: int = strawberry.field(name="productionEvidenceCount")
    production_evidence_ready: bool = strawberry.field(name="productionEvidenceReady")
    grpc_rust_trace_ready: bool = strawberry.field(name="grpcRustTraceReady")
    otlp_export_ready: bool = strawberry.field(name="otlpExportReady")
    search_index_ready: bool = strawberry.field(name="searchIndexReady")
    alert_rollout_ready: bool = strawberry.field(name="alertRolloutReady")
    ready_stage_names: list[str] = strawberry.field(name="readyStageNames")
    blocked_stage_names: list[str] = strawberry.field(name="blockedStageNames")
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLObservabilityFieldContractSummary:
    """Compact summary of cross-process field/header contract coverage."""

    total_required_correlation_fields: int = strawberry.field(
        name="totalRequiredCorrelationFields"
    )
    expected_field_count: int = strawberry.field(name="expectedFieldCount")
    configured_expected_field_count: int = strawberry.field(name="configuredExpectedFieldCount")
    missing_expected_field_count: int = strawberry.field(name="missingExpectedFieldCount")
    trace_context_header_count: int = strawberry.field(name="traceContextHeaderCount")
    correlation_header_count: int = strawberry.field(name="correlationHeaderCount")
    shared_header_count: int = strawberry.field(name="sharedHeaderCount")


@strawberry.type
class GQLGovernanceEvidenceCheck:
    """One retained rollout-evidence check for Director/operator governance views."""

    key: str
    label: str
    status: str
    recorded: bool
    ready: bool
    evidence_refs: list[str] = strawberry.field(name="evidenceRefs")
    proof_artifacts: list[GQLProofArtifact] = strawberry.field(name="proofArtifacts")
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLEnterpriseRolloutEvidence:
    """Aggregated retained rollout evidence across enterprise governance domains."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    total_check_count: int = strawberry.field(name="totalCheckCount")
    ready_check_count: int = strawberry.field(name="readyCheckCount")
    checks: list[GQLGovernanceEvidenceCheck]
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLGovernanceStatusCount:
    """One rollout-governance status bucket."""

    status: str
    count: int


@strawberry.type
class GQLGovernanceArtifactInventoryItem:
    """One retained rollout artifact entry exposed intentionally through GraphQL."""

    check_key: str = strawberry.field(name="checkKey")
    check_label: str = strawberry.field(name="checkLabel")
    ref: str
    category: str
    label: str
    recorded: bool


@strawberry.type
class GQLOperatorActionItem:
    """One flattened actionable operator/governance action row."""

    domain: str
    subject: str
    severity: str
    status: str
    action: str
    capability_kind: str | None = strawberry.field(name="capabilityKind", default=None)


@strawberry.type
class GQLOperatorGapItem:
    """One flattened actionable operator/governance gap row."""

    domain: str
    subject: str
    severity: str
    status: str
    message: str
    capability_kind: str | None = strawberry.field(name="capabilityKind", default=None)


@strawberry.type
class GQLPlaybackGateGovernance:
    """Typed playback-gate rollout posture for GraphQL-first operator consoles."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    rollout_readiness: str = strawberry.field(name="rolloutReadiness")
    next_action: str = strawberry.field(name="nextAction")
    reasons: list[str]
    environment_class: str = strawberry.field(name="environmentClass")
    gate_mode: str = strawberry.field(name="gateMode")
    runner_status: str = strawberry.field(name="runnerStatus")
    runner_ready: bool = strawberry.field(name="runnerReady")
    runner_required_failures: int = strawberry.field(name="runnerRequiredFailures")
    provider_gate_required: bool = strawberry.field(name="providerGateRequired")
    provider_gate_ran: bool = strawberry.field(name="providerGateRan")
    provider_parity_ready: bool = strawberry.field(name="providerParityReady")
    windows_provider_ready: bool = strawberry.field(name="windowsProviderReady")
    windows_provider_movie_ready: bool = strawberry.field(name="windowsProviderMovieReady")
    windows_provider_tv_ready: bool = strawberry.field(name="windowsProviderTvReady")
    windows_provider_coverage: list[str] = strawberry.field(name="windowsProviderCoverage")
    windows_soak_ready: bool = strawberry.field(name="windowsSoakReady")
    windows_soak_repeat_count: int = strawberry.field(name="windowsSoakRepeatCount")
    windows_soak_profile_coverage_complete: bool = strawberry.field(
        name="windowsSoakProfileCoverageComplete"
    )
    windows_soak_profile_coverage: list[str] = strawberry.field(
        name="windowsSoakProfileCoverage"
    )
    policy_validation_status: str = strawberry.field(name="policyValidationStatus")
    policy_ready: bool = strawberry.field(name="policyReady")
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLVfsRuntimeRollout:
    """Typed VFS runtime rollout/canary posture for GraphQL-first operator consoles."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    rollout_readiness: str = strawberry.field(name="rolloutReadiness")
    next_action: str = strawberry.field(name="nextAction")
    canary_decision: str = strawberry.field(name="canaryDecision")
    merge_gate: str = strawberry.field(name="mergeGate")
    environment_class: str = strawberry.field(name="environmentClass")
    snapshot_available: bool = strawberry.field(name="snapshotAvailable")
    open_handles: int = strawberry.field(name="openHandles")
    active_reads: int = strawberry.field(name="activeReads")
    cache_pressure_class: str = strawberry.field(name="cachePressureClass")
    refresh_pressure_class: str = strawberry.field(name="refreshPressureClass")
    provider_pressure_incidents: int = strawberry.field(name="providerPressureIncidents")
    fairness_pressure_incidents: int = strawberry.field(name="fairnessPressureIncidents")
    reasons: list[str]
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLVfsRuntimePercentiles:
    """One bounded percentile set for runtime handle ages."""

    p50_ms: float = strawberry.field(name="p50Ms")
    p95_ms: float = strawberry.field(name="p95Ms")
    p99_ms: float = strawberry.field(name="p99Ms")
    max_ms: float = strawberry.field(name="maxMs")


@strawberry.type
class GQLVfsRuntimeRustHandleRollup:
    """One Rust-mounted handle-depth rollup grouped by tenant and session."""

    tenant_id: str = strawberry.field(name="tenantId")
    session_id: str = strawberry.field(name="sessionId")
    open_handles: int = strawberry.field(name="openHandles")
    invalidated_handles: int = strawberry.field(name="invalidatedHandles")
    average_depth: float = strawberry.field(name="averageDepth")
    max_depth: int = strawberry.field(name="maxDepth")
    average_age_ms: float = strawberry.field(name="averageAgeMs")
    max_age_ms: float = strawberry.field(name="maxAgeMs")


@strawberry.type
class GQLVfsRuntimePythonSessionRollup:
    """One Python serving-session rollup with depth and age summaries."""

    owner: str
    session_id: str = strawberry.field(name="sessionId")
    resource: str
    open_handles: int = strawberry.field(name="openHandles")
    read_operations: int = strawberry.field(name="readOperations")
    bytes_served: int = strawberry.field(name="bytesServed")
    average_age_ms: float = strawberry.field(name="averageAgeMs")
    p95_age_ms: float = strawberry.field(name="p95AgeMs")
    average_depth: float = strawberry.field(name="averageDepth")
    max_depth: int = strawberry.field(name="maxDepth")
    bytes_per_read: float = strawberry.field(name="bytesPerRead")


@strawberry.type
class GQLVfsRuntimeReadAmplification:
    """One view-specific bytes-per-read summary."""

    view: str
    total_operations: int = strawberry.field(name="totalOperations")
    total_bytes: int = strawberry.field(name="totalBytes")
    bytes_per_read: float = strawberry.field(name="bytesPerRead")


@strawberry.type
class GQLVfsRuntimeTelemetry:
    """Detailed VFS runtime telemetry across Rust-mounted and Python-serving views."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    rust_snapshot_available: bool = strawberry.field(name="rustSnapshotAvailable")
    python_active_session_count: int = strawberry.field(name="pythonActiveSessionCount")
    python_active_handle_count: int = strawberry.field(name="pythonActiveHandleCount")
    rust_handle_age_ms: GQLVfsRuntimePercentiles = strawberry.field(name="rustHandleAgeMs")
    python_handle_age_ms: GQLVfsRuntimePercentiles = strawberry.field(name="pythonHandleAgeMs")
    mounted_read_duration_buckets: list[GQLNamedCountBucket] = strawberry.field(
        name="mountedReadDurationBuckets"
    )
    rust_handle_depth_rollups: list[GQLVfsRuntimeRustHandleRollup] = strawberry.field(
        name="rustHandleDepthRollups"
    )
    python_session_rollups: list[GQLVfsRuntimePythonSessionRollup] = strawberry.field(
        name="pythonSessionRollups"
    )
    read_amplification: list[GQLVfsRuntimeReadAmplification] = strawberry.field(
        name="readAmplification"
    )
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLVfsRolloutLedgerEntry:
    """One retained operator history row for VFS rollout control."""

    entry_id: str = strawberry.field(name="entryId")
    recorded_at: str = strawberry.field(name="recordedAt")
    actor_id: str | None = strawberry.field(name="actorId", default=None)
    action: str
    summary: str
    environment_class: str = strawberry.field(name="environmentClass")
    runtime_status_path: str | None = strawberry.field(name="runtimeStatusPath", default=None)
    promotion_paused: bool = strawberry.field(name="promotionPaused")
    promotion_pause_reason: str | None = strawberry.field(name="promotionPauseReason", default=None)
    promotion_pause_expires_at: str | None = strawberry.field(
        name="promotionPauseExpiresAt",
        default=None,
    )
    promotion_pause_active: bool = strawberry.field(name="promotionPauseActive")
    rollback_requested: bool = strawberry.field(name="rollbackRequested")
    rollback_reason: str | None = strawberry.field(name="rollbackReason", default=None)
    rollback_expires_at: str | None = strawberry.field(name="rollbackExpiresAt", default=None)
    rollback_active: bool = strawberry.field(name="rollbackActive")
    notes: str | None = None


@strawberry.type
class GQLVfsRolloutControl:
    """Persisted VFS rollout-control state plus the derived current canary posture."""

    generated_at: str = strawberry.field(name="generatedAt")
    environment_class: str = strawberry.field(name="environmentClass")
    runtime_status_path: str | None = strawberry.field(name="runtimeStatusPath", default=None)
    promotion_paused: bool = strawberry.field(name="promotionPaused")
    promotion_pause_reason: str | None = strawberry.field(name="promotionPauseReason", default=None)
    promotion_pause_expires_at: str | None = strawberry.field(
        name="promotionPauseExpiresAt",
        default=None,
    )
    promotion_pause_active: bool = strawberry.field(name="promotionPauseActive")
    rollback_requested: bool = strawberry.field(name="rollbackRequested")
    rollback_reason: str | None = strawberry.field(name="rollbackReason", default=None)
    rollback_expires_at: str | None = strawberry.field(name="rollbackExpiresAt", default=None)
    rollback_active: bool = strawberry.field(name="rollbackActive")
    notes: str | None = None
    updated_at: str | None = strawberry.field(name="updatedAt", default=None)
    updated_by: str | None = strawberry.field(name="updatedBy", default=None)
    rollout_readiness: str = strawberry.field(name="rolloutReadiness")
    next_action: str = strawberry.field(name="nextAction")
    canary_decision: str = strawberry.field(name="canaryDecision")
    merge_gate: str = strawberry.field(name="mergeGate")
    reasons: list[str]
    history: list[GQLVfsRolloutLedgerEntry]


@strawberry.type
class GQLAccessPolicyRevision:
    """One persisted access-policy revision exposed through GraphQL."""

    version: str
    source: str
    approval_status: str = strawberry.field(name="approvalStatus")
    proposed_by: str | None = strawberry.field(name="proposedBy", default=None)
    approved_by: str | None = strawberry.field(name="approvedBy", default=None)
    approved_at: str | None = strawberry.field(name="approvedAt", default=None)
    approval_notes: str | None = strawberry.field(name="approvalNotes", default=None)
    is_active: bool = strawberry.field(name="isActive")
    activated_at: str = strawberry.field(name="activatedAt")
    created_at: str = strawberry.field(name="createdAt")
    updated_at: str = strawberry.field(name="updatedAt")
    role_grants: JSON = strawberry.field(name="roleGrants", default_factory=dict)
    principal_roles: JSON = strawberry.field(name="principalRoles", default_factory=dict)
    principal_scopes: JSON = strawberry.field(name="principalScopes", default_factory=dict)
    principal_tenant_grants: JSON = strawberry.field(
        name="principalTenantGrants",
        default_factory=dict,
    )
    permission_constraints: JSON = strawberry.field(
        name="permissionConstraints",
        default_factory=dict,
    )
    audit_decisions: bool = strawberry.field(name="auditDecisions")
    alerting_enabled: bool = strawberry.field(name="alertingEnabled")
    repeated_denial_warning_threshold: int = strawberry.field(
        name="repeatedDenialWarningThreshold"
    )
    repeated_denial_critical_threshold: int = strawberry.field(
        name="repeatedDenialCriticalThreshold"
    )


@strawberry.type
class GQLAccessPolicyRevisionList:
    """Bounded access-policy revision inventory for GraphQL operator clients."""

    active_version: str | None = strawberry.field(name="activeVersion", default=None)
    revisions: list[GQLAccessPolicyRevision]


@strawberry.input
class AccessPolicyRevisionWriteInput:
    """GraphQL input for one persisted access-policy revision write."""

    version: str
    source: str = "operator_api"
    activate: bool = False
    approval_notes: str | None = strawberry.field(name="approvalNotes", default=None)
    role_grants: JSON = strawberry.field(name="roleGrants", default_factory=dict)
    principal_roles: JSON = strawberry.field(name="principalRoles", default_factory=dict)
    principal_scopes: JSON = strawberry.field(name="principalScopes", default_factory=dict)
    principal_tenant_grants: JSON = strawberry.field(
        name="principalTenantGrants",
        default_factory=dict,
    )
    permission_constraints: JSON = strawberry.field(
        name="permissionConstraints",
        default_factory=dict,
    )
    audit_decisions: bool = strawberry.field(name="auditDecisions", default=True)
    alerting_enabled: bool = strawberry.field(name="alertingEnabled", default=True)
    repeated_denial_warning_threshold: int = strawberry.field(
        name="repeatedDenialWarningThreshold",
        default=3,
    )
    repeated_denial_critical_threshold: int = strawberry.field(
        name="repeatedDenialCriticalThreshold",
        default=5,
    )


@strawberry.input
class AccessPolicyRevisionApprovalInput:
    """GraphQL input for approval or rejection of one access-policy revision."""

    approval_notes: str | None = strawberry.field(name="approvalNotes", default=None)
    activate: bool = False


@strawberry.type
class GQLPluginGovernanceOverride:
    """One persisted plugin-governance override exposed through GraphQL."""

    plugin_name: str = strawberry.field(name="pluginName")
    state: str
    reason: str | None = None
    notes: str | None = None
    updated_by: str | None = strawberry.field(name="updatedBy", default=None)
    created_at: str = strawberry.field(name="createdAt")
    updated_at: str = strawberry.field(name="updatedAt")


@strawberry.input
class PluginGovernanceOverrideWriteInput:
    """GraphQL input for one plugin-governance override write."""

    state: str
    reason: str | None = None
    notes: str | None = None


@strawberry.type
class GQLControlPlaneStatusCount:
    """One typed control-plane subscriber-status count bucket."""

    status: str
    count: int


@strawberry.type
class GQLControlPlaneSummary:
    """Bounded control-plane health rollup for GraphQL-first operator views."""

    total_subscribers: int = strawberry.field(name="totalSubscribers")
    active_subscribers: int = strawberry.field(name="activeSubscribers")
    stale_subscribers: int = strawberry.field(name="staleSubscribers")
    error_subscribers: int = strawberry.field(name="errorSubscribers")
    fenced_subscribers: int = strawberry.field(name="fencedSubscribers")
    ack_pending_subscribers: int = strawberry.field(name="ackPendingSubscribers")
    stream_count: int = strawberry.field(name="streamCount")
    group_count: int = strawberry.field(name="groupCount")
    node_count: int = strawberry.field(name="nodeCount")
    tenant_count: int = strawberry.field(name="tenantCount")
    oldest_heartbeat_age_seconds: float | None = strawberry.field(
        name="oldestHeartbeatAgeSeconds",
        default=None,
    )
    status_counts: list[GQLControlPlaneStatusCount] = strawberry.field(name="statusCounts")
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLControlPlaneAutomation:
    """Background replay/control-plane automation posture for GraphQL clients."""

    generated_at: str = strawberry.field(name="generatedAt")
    enabled: bool
    runner_status: str = strawberry.field(name="runnerStatus")
    interval_seconds: int = strawberry.field(name="intervalSeconds")
    active_within_seconds: int = strawberry.field(name="activeWithinSeconds")
    pending_min_idle_ms: int = strawberry.field(name="pendingMinIdleMs")
    claim_limit: int = strawberry.field(name="claimLimit")
    max_claim_passes: int = strawberry.field(name="maxClaimPasses")
    consumer_group: str = strawberry.field(name="consumerGroup")
    consumer_name: str = strawberry.field(name="consumerName")
    service_attached: bool = strawberry.field(name="serviceAttached")
    backplane_attached: bool = strawberry.field(name="backplaneAttached")
    last_run_at: str | None = strawberry.field(name="lastRunAt", default=None)
    last_success_at: str | None = strawberry.field(name="lastSuccessAt", default=None)
    last_failure_at: str | None = strawberry.field(name="lastFailureAt", default=None)
    consecutive_failures: int = strawberry.field(name="consecutiveFailures")
    last_error: str | None = strawberry.field(name="lastError", default=None)
    remediation_updated_subscribers: int = strawberry.field(name="remediationUpdatedSubscribers")
    rewound_subscribers: int = strawberry.field(name="rewoundSubscribers")
    claimed_pending_events: int = strawberry.field(name="claimedPendingEvents")
    claim_passes: int = strawberry.field(name="claimPasses")
    pending_count_after: int | None = strawberry.field(name="pendingCountAfter", default=None)
    summary: GQLControlPlaneSummary
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLControlPlaneSubscriber:
    """One durable replay/control-plane subscriber row for GraphQL consoles."""

    stream_name: str = strawberry.field(name="streamName")
    group_name: str = strawberry.field(name="groupName")
    consumer_name: str = strawberry.field(name="consumerName")
    node_id: str = strawberry.field(name="nodeId")
    tenant_id: str | None = strawberry.field(name="tenantId", default=None)
    status: str
    last_read_offset: str | None = strawberry.field(name="lastReadOffset", default=None)
    last_delivered_event_id: str | None = strawberry.field(name="lastDeliveredEventId", default=None)
    last_acked_event_id: str | None = strawberry.field(name="lastAckedEventId", default=None)
    ack_pending: bool = strawberry.field(name="ackPending")
    fenced: bool
    last_error: str | None = strawberry.field(name="lastError", default=None)
    claimed_at: str = strawberry.field(name="claimedAt")
    last_heartbeat_at: str = strawberry.field(name="lastHeartbeatAt")
    created_at: str = strawberry.field(name="createdAt")
    updated_at: str = strawberry.field(name="updatedAt")


@strawberry.type
class GQLControlPlaneReplayBackplane:
    """Replay-backplane readiness and pending-delivery posture for GraphQL."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    event_backplane: str = strawberry.field(name="eventBackplane")
    stream_name: str = strawberry.field(name="streamName")
    consumer_group: str = strawberry.field(name="consumerGroup")
    replay_maxlen: int = strawberry.field(name="replayMaxlen")
    claim_limit: int = strawberry.field(name="claimLimit")
    max_claim_passes: int = strawberry.field(name="maxClaimPasses")
    attached: bool
    pending_count: int = strawberry.field(name="pendingCount")
    oldest_event_id: str | None = strawberry.field(name="oldestEventId", default=None)
    latest_event_id: str | None = strawberry.field(name="latestEventId", default=None)
    consumer_counts: list[GQLNamedCountBucket] = strawberry.field(name="consumerCounts")
    consumer_count: int = strawberry.field(name="consumerCount")
    has_pending_backlog: bool = strawberry.field(name="hasPendingBacklog")
    proof_refs: list[str] = strawberry.field(name="proofRefs")
    proof_artifacts: list[GQLProofArtifact] = strawberry.field(name="proofArtifacts")
    proof_ready: bool = strawberry.field(name="proofReady")
    pending_recovery_ready: bool = strawberry.field(name="pendingRecoveryReady")
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLControlPlaneRecoveryReadiness:
    """One GraphQL-first recovery readiness rollup for control-plane evidence and automation."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    active_within_seconds: int = strawberry.field(name="activeWithinSeconds")
    stale_subscribers: int = strawberry.field(name="staleSubscribers")
    ack_pending_subscribers: int = strawberry.field(name="ackPendingSubscribers")
    pending_count: int = strawberry.field(name="pendingCount")
    consumer_count: int = strawberry.field(name="consumerCount")
    automation_enabled: bool = strawberry.field(name="automationEnabled")
    automation_healthy: bool = strawberry.field(name="automationHealthy")
    replay_attached: bool = strawberry.field(name="replayAttached")
    proof_refs: list[str] = strawberry.field(name="proofRefs")
    proof_artifacts: list[GQLProofArtifact] = strawberry.field(name="proofArtifacts")
    proof_ready: bool = strawberry.field(name="proofReady")
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLControlPlaneRemediation:
    """One operator-triggered stale/fence/error remediation result."""

    generated_at: str = strawberry.field(name="generatedAt")
    active_within_seconds: int = strawberry.field(name="activeWithinSeconds")
    stale_marked_subscribers: int = strawberry.field(name="staleMarkedSubscribers")
    fence_resolved_subscribers: int = strawberry.field(name="fenceResolvedSubscribers")
    error_recovered_subscribers: int = strawberry.field(name="errorRecoveredSubscribers")
    total_updated_subscribers: int = strawberry.field(name="totalUpdatedSubscribers")
    summary: GQLControlPlaneSummary


@strawberry.type
class GQLControlPlaneAckRecovery:
    """One operator-triggered ack-backlog recovery result."""

    generated_at: str = strawberry.field(name="generatedAt")
    active_within_seconds: int = strawberry.field(name="activeWithinSeconds")
    rewound_subscribers: int = strawberry.field(name="rewoundSubscribers")
    stale_marked_subscribers: int = strawberry.field(name="staleMarkedSubscribers")
    pending_without_ack_subscribers: int = strawberry.field(
        name="pendingWithoutAckSubscribers"
    )
    total_updated_subscribers: int = strawberry.field(name="totalUpdatedSubscribers")
    summary: GQLControlPlaneSummary


@strawberry.type
class GQLControlPlanePendingRecovery:
    """One operator-triggered replay pending-entry recovery result."""

    generated_at: str = strawberry.field(name="generatedAt")
    group_name: str = strawberry.field(name="groupName")
    consumer_name: str = strawberry.field(name="consumerName")
    min_idle_ms: int = strawberry.field(name="minIdleMs")
    claim_limit: int = strawberry.field(name="claimLimit")
    claimed_count: int = strawberry.field(name="claimedCount")
    claimed_event_ids: list[str] = strawberry.field(name="claimedEventIds")
    next_start_id: str = strawberry.field(name="nextStartId")
    pending_count_before: int = strawberry.field(name="pendingCountBefore")
    pending_count_after: int = strawberry.field(name="pendingCountAfter")
    oldest_pending_event_id: str | None = strawberry.field(name="oldestPendingEventId", default=None)
    latest_pending_event_id: str | None = strawberry.field(name="latestPendingEventId", default=None)
    pending_consumer_counts: list[GQLNamedCountBucket] = strawberry.field(
        name="pendingConsumerCounts"
    )
    summary: GQLControlPlaneSummary
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.input
class ControlPlanePendingRecoveryInput:
    """GraphQL input for one replay pending-entry recovery run."""

    group_name: str | None = strawberry.field(name="groupName", default=None)
    consumer_name: str = strawberry.field(name="consumerName", default="recovery-ops")
    min_idle_ms: int = strawberry.field(name="minIdleMs", default=60_000)
    claim_limit: int = strawberry.field(name="claimLimit", default=100)
    active_within_seconds: int = strawberry.field(name="activeWithinSeconds", default=120)


@strawberry.type
class GQLControlPlaneConsumerSummary:
    """Grouped control-plane ownership summary per consumer."""

    consumer_name: str = strawberry.field(name="consumerName")
    subscriber_count: int = strawberry.field(name="subscriberCount")
    active_subscribers: int = strawberry.field(name="activeSubscribers")
    ack_pending_subscribers: int = strawberry.field(name="ackPendingSubscribers")
    fenced_subscribers: int = strawberry.field(name="fencedSubscribers")
    error_subscribers: int = strawberry.field(name="errorSubscribers")
    latest_heartbeat_at: str | None = strawberry.field(name="latestHeartbeatAt", default=None)


@strawberry.type
class GQLControlPlaneOwnershipSummary:
    """Aggregated subscriber ownership and backlog summary."""

    total_subscribers: int = strawberry.field(name="totalSubscribers")
    active_subscribers: int = strawberry.field(name="activeSubscribers")
    stale_subscribers: int = strawberry.field(name="staleSubscribers")
    error_subscribers: int = strawberry.field(name="errorSubscribers")
    fenced_subscribers: int = strawberry.field(name="fencedSubscribers")
    ack_pending_subscribers: int = strawberry.field(name="ackPendingSubscribers")
    unique_consumers: int = strawberry.field(name="uniqueConsumers")
    unique_nodes: int = strawberry.field(name="uniqueNodes")
    unique_tenants: int = strawberry.field(name="uniqueTenants")


@strawberry.type
class GQLPluginIntegrationReadinessPlugin:
    """One builtin plugin readiness row for GraphQL-first Director consoles."""

    name: str
    capability_kind: str = strawberry.field(name="capabilityKind")
    status: str
    registered: bool
    enabled: bool
    configured: bool
    ready: bool
    endpoint: str | None = None
    endpoint_configured: bool = strawberry.field(name="endpointConfigured")
    config_source: str | None = strawberry.field(name="configSource", default=None)
    required_settings: list[str] = strawberry.field(name="requiredSettings")
    missing_settings: list[str] = strawberry.field(name="missingSettings")
    contract_proof_refs: list[str] = strawberry.field(name="contractProofRefs")
    soak_proof_refs: list[str] = strawberry.field(name="soakProofRefs")
    contract_proofs: list[GQLProofArtifact] = strawberry.field(name="contractProofs")
    soak_proofs: list[GQLProofArtifact] = strawberry.field(name="soakProofs")
    contract_validated: bool = strawberry.field(name="contractValidated")
    soak_validated: bool = strawberry.field(name="soakValidated")
    proof_gap_count: int = strawberry.field(name="proofGapCount")
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLPluginIntegrationReadinessSummary:
    """Rollup counters for Director plugin readiness consoles."""

    total_plugins: int = strawberry.field(name="totalPlugins")
    enabled_plugins: int = strawberry.field(name="enabledPlugins")
    configured_plugins: int = strawberry.field(name="configuredPlugins")
    contract_validated_plugins: int = strawberry.field(name="contractValidatedPlugins")
    soak_validated_plugins: int = strawberry.field(name="soakValidatedPlugins")
    ready_plugins: int = strawberry.field(name="readyPlugins")
    missing_contract_proof_plugins: int = strawberry.field(name="missingContractProofPlugins")
    missing_soak_proof_plugins: int = strawberry.field(name="missingSoakProofPlugins")


@strawberry.type
class GQLPluginIntegrationReadiness:
    """Builtin plugin registration and config-validation posture for GraphQL."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    summary: GQLPluginIntegrationReadinessSummary
    plugins: list[GQLPluginIntegrationReadinessPlugin]
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLNamedCountBucket:
    """Generic named counter bucket for typed governance rollups."""

    key: str
    count: int


@strawberry.type
class GQLPluginEventStatus:
    """Declared publishable plugin events and subscriptions."""

    name: str
    publisher: str | None = None
    publishable_events: list[str] = strawberry.field(name="publishableEvents")
    hook_subscriptions: list[str] = strawberry.field(name="hookSubscriptions")
    publishable_event_count: int = strawberry.field(name="publishableEventCount")
    hook_subscription_count: int = strawberry.field(name="hookSubscriptionCount")
    wiring_status: str = strawberry.field(name="wiringStatus")


@strawberry.type
class GQLPluginRuntimeOverview:
    """Aggregated plugin runtime health/readiness posture for Director operator screens."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    total_plugins: int = strawberry.field(name="totalPlugins")
    ready_plugins: int = strawberry.field(name="readyPlugins")
    load_failed_plugins: int = strawberry.field(name="loadFailedPlugins")
    wiring_ready_plugins: int = strawberry.field(name="wiringReadyPlugins")
    contract_validated_plugins: int = strawberry.field(name="contractValidatedPlugins")
    soak_validated_plugins: int = strawberry.field(name="soakValidatedPlugins")
    quarantined_plugins: int = strawberry.field(name="quarantinedPlugins")
    publishable_event_count: int = strawberry.field(name="publishableEventCount")
    hook_subscription_count: int = strawberry.field(name="hookSubscriptionCount")
    warning_count: int = strawberry.field(name="warningCount")
    recommended_actions: list[str] = strawberry.field(name="recommendedActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLPluginRuntimeWarning:
    """One actionable plugin runtime warning row for GraphQL-first consoles."""

    plugin_name: str = strawberry.field(name="pluginName")
    source: str
    severity: str
    status: str
    message: str
    capability_kind: str | None = strawberry.field(name="capabilityKind", default=None)


@strawberry.type
class GQLPluginRuntimeRow:
    """One combined plugin runtime row with wiring and proof posture."""

    name: str
    status: str
    ready: bool
    capability_kinds: list[str] = strawberry.field(name="capabilityKinds")
    wiring_status: str = strawberry.field(name="wiringStatus")
    publishable_event_count: int = strawberry.field(name="publishableEventCount")
    hook_subscription_count: int = strawberry.field(name="hookSubscriptionCount")
    contract_validated: bool = strawberry.field(name="contractValidated")
    soak_validated: bool = strawberry.field(name="soakValidated")
    proof_gap_count: int = strawberry.field(name="proofGapCount")
    warning_count: int = strawberry.field(name="warningCount")
    quarantined: bool
    recommended_actions: list[str] = strawberry.field(name="recommendedActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLPluginRuntimeCapabilitySummary:
    """Capability-grouped plugin runtime rollup."""

    capability_kind: str = strawberry.field(name="capabilityKind")
    total_plugins: int = strawberry.field(name="totalPlugins")
    ready_plugins: int = strawberry.field(name="readyPlugins")
    blocked_plugins: int = strawberry.field(name="blockedPlugins")
    warning_count: int = strawberry.field(name="warningCount")
    contract_validated_plugins: int = strawberry.field(name="contractValidatedPlugins")
    soak_validated_plugins: int = strawberry.field(name="soakValidatedPlugins")


@strawberry.type
class GQLPluginProofCoverageSummary:
    """Capability-grouped retained plugin proof coverage summary."""

    capability_kind: str = strawberry.field(name="capabilityKind")
    total_plugins: int = strawberry.field(name="totalPlugins")
    contract_validated_plugins: int = strawberry.field(name="contractValidatedPlugins")
    soak_validated_plugins: int = strawberry.field(name="soakValidatedPlugins")
    missing_contract_plugins: int = strawberry.field(name="missingContractPlugins")
    missing_soak_plugins: int = strawberry.field(name="missingSoakPlugins")


@strawberry.type
class GQLDownloaderProviderCandidate:
    """One downloader candidate in the orchestration posture graph."""

    name: str
    source: str
    enabled: bool
    configured: bool
    selected: bool
    priority: int | None = None
    capabilities: list[str]


@strawberry.type
class GQLDownloaderOrchestration:
    """Downloader orchestration posture exposed to GraphQL-first clients."""

    generated_at: str = strawberry.field(name="generatedAt")
    selection_mode: str = strawberry.field(name="selectionMode")
    selected_provider: str | None = strawberry.field(name="selectedProvider", default=None)
    selected_provider_source: str | None = strawberry.field(
        name="selectedProviderSource",
        default=None,
    )
    enabled_provider_count: int = strawberry.field(name="enabledProviderCount")
    configured_provider_count: int = strawberry.field(name="configuredProviderCount")
    builtin_enabled_provider_count: int = strawberry.field(name="builtinEnabledProviderCount")
    plugin_enabled_provider_count: int = strawberry.field(name="pluginEnabledProviderCount")
    multi_provider_enabled: bool = strawberry.field(name="multiProviderEnabled")
    plugin_downloaders_registered: int = strawberry.field(name="pluginDownloadersRegistered")
    worker_plugin_dispatch_ready: bool = strawberry.field(name="workerPluginDispatchReady")
    ordered_failover_ready: bool = strawberry.field(name="orderedFailoverReady")
    fanout_ready: bool = strawberry.field(name="fanoutReady")
    multi_container_ready: bool = strawberry.field(name="multiContainerReady")
    provider_priority_order: list[str] = strawberry.field(name="providerPriorityOrder")
    providers: list[GQLDownloaderProviderCandidate]
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLWorkerQueueHistorySummary:
    """Aggregate rollup across bounded worker queue history."""

    point_count: int = strawberry.field(name="pointCount")
    warning_point_count: int = strawberry.field(name="warningPointCount")
    critical_point_count: int = strawberry.field(name="criticalPointCount")
    max_total_jobs: int = strawberry.field(name="maxTotalJobs")
    max_ready_jobs: int = strawberry.field(name="maxReadyJobs")
    max_retry_jobs: int = strawberry.field(name="maxRetryJobs")
    max_dead_letter_jobs: int = strawberry.field(name="maxDeadLetterJobs")
    latest_alert_level: str = strawberry.field(name="latestAlertLevel")
    dead_letter_reason_counts: list[GQLNamedCountBucket] = strawberry.field(
        name="deadLetterReasonCounts"
    )


@strawberry.type
class GQLDownloaderExecutionDeadLetter:
    """One recent downloader/debrid dead-letter sample with structured evidence fields."""

    stage: str
    item_id: str = strawberry.field(name="itemId")
    reason: str
    reason_code: str = strawberry.field(name="reasonCode")
    idempotency_key: str = strawberry.field(name="idempotencyKey")
    attempt: int
    queued_at: str = strawberry.field(name="queuedAt")
    provider: str | None = None
    failure_kind: str | None = strawberry.field(name="failureKind", default=None)
    selected_stream_id: str | None = strawberry.field(name="selectedStreamId", default=None)
    item_request_id: str | None = strawberry.field(name="itemRequestId", default=None)
    status_code: int | None = strawberry.field(name="statusCode", default=None)
    retry_after_seconds: int | None = strawberry.field(name="retryAfterSeconds", default=None)


@strawberry.type
class GQLDownloaderExecutionEvidence:
    """Retained downloader execution and failover evidence for Director/operator screens."""

    generated_at: str = strawberry.field(name="generatedAt")
    queue_name: str = strawberry.field(name="queueName")
    status: str
    selection_mode: str = strawberry.field(name="selectionMode")
    ordered_failover_ready: bool = strawberry.field(name="orderedFailoverReady")
    fanout_ready: bool = strawberry.field(name="fanoutReady")
    provider_counts: list[GQLNamedCountBucket] = strawberry.field(name="providerCounts")
    failure_kind_counts: list[GQLNamedCountBucket] = strawberry.field(name="failureKindCounts")
    dead_letter_reason_counts: list[GQLNamedCountBucket] = strawberry.field(
        name="deadLetterReasonCounts"
    )
    history_summary: GQLWorkerQueueHistorySummary = strawberry.field(name="historySummary")
    recent_dead_letters: list[GQLDownloaderExecutionDeadLetter] = strawberry.field(
        name="recentDeadLetters"
    )
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLDownloaderExecutionTrendSummary:
    """Bounded downloader queue-history trend summary for graph operator views."""

    point_count: int = strawberry.field(name="pointCount")
    ok_point_count: int = strawberry.field(name="okPointCount")
    warning_point_count: int = strawberry.field(name="warningPointCount")
    critical_point_count: int = strawberry.field(name="criticalPointCount")
    average_ready_jobs: float = strawberry.field(name="averageReadyJobs")
    average_retry_jobs: float = strawberry.field(name="averageRetryJobs")
    average_dead_letter_jobs: float = strawberry.field(name="averageDeadLetterJobs")
    latest_alert_level: str = strawberry.field(name="latestAlertLevel")


@strawberry.type
class GQLDownloaderProviderSummary:
    """Provider-grouped downloader dead-letter evidence summary."""

    provider: str
    sample_count: int = strawberry.field(name="sampleCount")
    failure_kind_counts: list[GQLNamedCountBucket] = strawberry.field(name="failureKindCounts")
    reason_code_counts: list[GQLNamedCountBucket] = strawberry.field(name="reasonCodeCounts")
    status_code_counts: list[GQLNamedCountBucket] = strawberry.field(name="statusCodeCounts")
    retry_after_hint_count: int = strawberry.field(name="retryAfterHintCount")


@strawberry.type
class GQLDownloaderReasonSummary:
    """Reason-code grouped downloader dead-letter evidence summary."""

    reason_code: str = strawberry.field(name="reasonCode")
    sample_count: int = strawberry.field(name="sampleCount")
    provider_counts: list[GQLNamedCountBucket] = strawberry.field(name="providerCounts")
    failure_kind_counts: list[GQLNamedCountBucket] = strawberry.field(name="failureKindCounts")


@strawberry.type
class GQLDownloaderDeadLetterTimelinePoint:
    """Time-bucketed downloader dead-letter posture."""

    bucket_at: str = strawberry.field(name="bucketAt")
    sample_count: int = strawberry.field(name="sampleCount")
    provider_counts: list[GQLNamedCountBucket] = strawberry.field(name="providerCounts")
    reason_code_counts: list[GQLNamedCountBucket] = strawberry.field(name="reasonCodeCounts")
    failure_kind_counts: list[GQLNamedCountBucket] = strawberry.field(name="failureKindCounts")


@strawberry.type
class GQLDownloaderFailureKindSummary:
    """Failure-kind grouped downloader summary."""

    failure_kind: str = strawberry.field(name="failureKind")
    sample_count: int = strawberry.field(name="sampleCount")
    provider_counts: list[GQLNamedCountBucket] = strawberry.field(name="providerCounts")
    reason_code_counts: list[GQLNamedCountBucket] = strawberry.field(name="reasonCodeCounts")


@strawberry.type
class GQLDownloaderStatusCodeSummary:
    """Status-code grouped downloader summary."""

    status_code: int = strawberry.field(name="statusCode")
    sample_count: int = strawberry.field(name="sampleCount")
    provider_counts: list[GQLNamedCountBucket] = strawberry.field(name="providerCounts")
    reason_code_counts: list[GQLNamedCountBucket] = strawberry.field(name="reasonCodeCounts")


@strawberry.type
class GQLPluginCapabilityStatus:
    """Loaded plugin runtime row with trust and readiness posture."""

    name: str
    capabilities: list[str]
    status: str
    ready: bool
    configured: bool | None = None
    version: str | None = None
    api_version: str | None = strawberry.field(name="apiVersion", default=None)
    min_host_version: str | None = strawberry.field(name="minHostVersion", default=None)
    max_host_version: str | None = strawberry.field(name="maxHostVersion", default=None)
    publisher: str | None = None
    release_channel: str | None = strawberry.field(name="releaseChannel", default=None)
    trust_level: str | None = strawberry.field(name="trustLevel", default=None)
    permission_scopes: list[str] = strawberry.field(name="permissionScopes")
    source_sha256: str | None = strawberry.field(name="sourceSha256", default=None)
    signing_key_id: str | None = strawberry.field(name="signingKeyId", default=None)
    signature_present: bool = strawberry.field(name="signaturePresent")
    signature_verified: bool = strawberry.field(name="signatureVerified")
    signature_verification_reason: str | None = strawberry.field(
        name="signatureVerificationReason",
        default=None,
    )
    trust_policy_decision: str | None = strawberry.field(name="trustPolicyDecision", default=None)
    trust_store_source: str | None = strawberry.field(name="trustStoreSource", default=None)
    sandbox_profile: str | None = strawberry.field(name="sandboxProfile", default=None)
    tenancy_mode: str | None = strawberry.field(name="tenancyMode", default=None)
    quarantined: bool
    quarantine_reason: str | None = strawberry.field(name="quarantineReason", default=None)
    publisher_policy_decision: str | None = strawberry.field(
        name="publisherPolicyDecision",
        default=None,
    )
    publisher_policy_status: str | None = strawberry.field(
        name="publisherPolicyStatus",
        default=None,
    )
    quarantine_recommended: bool = strawberry.field(name="quarantineRecommended")
    override_state: str | None = strawberry.field(name="overrideState", default=None)
    override_reason: str | None = strawberry.field(name="overrideReason", default=None)
    override_updated_at: str | None = strawberry.field(name="overrideUpdatedAt", default=None)
    source: str | None = None
    warnings: list[str]
    error: str | None = None


@strawberry.type
class GQLPluginGovernanceSummary:
    """Plugin trust/isolation rollup for operator and Director consoles."""

    total_plugins: int = strawberry.field(name="totalPlugins")
    loaded_plugins: int = strawberry.field(name="loadedPlugins")
    load_failed_plugins: int = strawberry.field(name="loadFailedPlugins")
    ready_plugins: int = strawberry.field(name="readyPlugins")
    unready_plugins: int = strawberry.field(name="unreadyPlugins")
    healthy_plugins: int = strawberry.field(name="healthyPlugins")
    degraded_plugins: int = strawberry.field(name="degradedPlugins")
    non_builtin_plugins: int = strawberry.field(name="nonBuiltinPlugins")
    isolated_non_builtin_plugins: int = strawberry.field(name="isolatedNonBuiltinPlugins")
    quarantined_plugins: int = strawberry.field(name="quarantinedPlugins")
    quarantine_recommended_plugins: int = strawberry.field(name="quarantineRecommendedPlugins")
    unsigned_external_plugins: int = strawberry.field(name="unsignedExternalPlugins")
    unverified_signature_plugins: int = strawberry.field(name="unverifiedSignaturePlugins")
    publisher_policy_rejections: int = strawberry.field(name="publisherPolicyRejections")
    trust_policy_rejections: int = strawberry.field(name="trustPolicyRejections")
    scraper_plugins: int = strawberry.field(name="scraperPlugins")
    downloader_plugins: int = strawberry.field(name="downloaderPlugins")
    content_service_plugins: int = strawberry.field(name="contentServicePlugins")
    event_hook_plugins: int = strawberry.field(name="eventHookPlugins")
    override_count: int = strawberry.field(name="overrideCount")
    approved_overrides: int = strawberry.field(name="approvedOverrides")
    quarantined_overrides: int = strawberry.field(name="quarantinedOverrides")
    revoked_overrides: int = strawberry.field(name="revokedOverrides")
    sandbox_profile_counts: list[GQLNamedCountBucket] = strawberry.field(name="sandboxProfileCounts")
    tenancy_mode_counts: list[GQLNamedCountBucket] = strawberry.field(name="tenancyModeCounts")
    runtime_policy_mode: str = strawberry.field(name="runtimePolicyMode")
    runtime_isolation_ready: bool = strawberry.field(name="runtimeIsolationReady")
    recommended_actions: list[str] = strawberry.field(name="recommendedActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLPluginGovernance:
    """Plugin trust/isolation summary plus plugin runtime rows."""

    summary: GQLPluginGovernanceSummary
    plugins: list[GQLPluginCapabilityStatus]


@strawberry.type
class GQLPluginRuntimePublisherSummary:
    """Publisher-grouped runtime and proof posture summary."""

    publisher: str
    plugin_count: int = strawberry.field(name="pluginCount")
    ready_plugins: int = strawberry.field(name="readyPlugins")
    quarantined_plugins: int = strawberry.field(name="quarantinedPlugins")
    warning_count: int = strawberry.field(name="warningCount")
    capability_counts: list[GQLNamedCountBucket] = strawberry.field(name="capabilityCounts")


@strawberry.type
class GQLEnterpriseOperationsSlice:
    """One enterprise-operations roadmap slice posture row."""

    name: str
    status: str
    evidence: list[str]
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLEnterpriseOperationsGovernance:
    """Machine-readable enterprise operations posture for Director consoles."""

    generated_at: str = strawberry.field(name="generatedAt")
    playback_gate: GQLEnterpriseOperationsSlice = strawberry.field(name="playbackGate")
    operational_evidence: GQLEnterpriseOperationsSlice = strawberry.field(name="operationalEvidence")
    identity_authz: GQLEnterpriseOperationsSlice = strawberry.field(name="identityAuthz")
    tenant_boundary: GQLEnterpriseOperationsSlice = strawberry.field(name="tenantBoundary")
    vfs_data_plane: GQLEnterpriseOperationsSlice = strawberry.field(name="vfsDataPlane")
    distributed_control_plane: GQLEnterpriseOperationsSlice = strawberry.field(
        name="distributedControlPlane"
    )
    runtime_lifecycle: GQLEnterpriseOperationsSlice = strawberry.field(name="runtimeLifecycle")
    sre_program: GQLEnterpriseOperationsSlice = strawberry.field(name="sreProgram")
    operator_log_pipeline: GQLEnterpriseOperationsSlice = strawberry.field(name="operatorLogPipeline")
    plugin_runtime_isolation: GQLEnterpriseOperationsSlice = strawberry.field(
        name="pluginRuntimeIsolation"
    )
    heavy_stage_workload_isolation: GQLEnterpriseOperationsSlice = strawberry.field(
        name="heavyStageWorkloadIsolation"
    )
    release_metadata_performance: GQLEnterpriseOperationsSlice = strawberry.field(
        name="releaseMetadataPerformance"
    )


@strawberry.type
class GQLVfsCorrelationKeys:
    """Correlation identifiers for one VFS catalog node."""

    item_id: str | None = strawberry.field(name="itemId", default=None)
    media_entry_id: str | None = strawberry.field(name="mediaEntryId", default=None)
    source_attachment_id: str | None = strawberry.field(name="sourceAttachmentId", default=None)
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)
    session_id: str | None = strawberry.field(name="sessionId", default=None)
    handle_key: str | None = strawberry.field(name="handleKey", default=None)


@strawberry.type
class GQLVfsDirectoryDetail:
    """Directory metadata for one VFS catalog node."""

    path: str


@strawberry.type
class GQLVfsFileDetail:
    """File metadata for one VFS catalog node."""

    item_id: str = strawberry.field(name="itemId")
    item_title: str = strawberry.field(name="itemTitle")
    item_external_ref: str = strawberry.field(name="itemExternalRef")
    media_entry_id: str = strawberry.field(name="mediaEntryId")
    source_attachment_id: str | None = strawberry.field(name="sourceAttachmentId", default=None)
    media_type: str = strawberry.field(name="mediaType")
    transport: str
    locator: str
    local_path: str | None = strawberry.field(name="localPath", default=None)
    restricted_url: str | None = strawberry.field(name="restrictedUrl", default=None)
    unrestricted_url: str | None = strawberry.field(name="unrestrictedUrl", default=None)
    original_filename: str | None = strawberry.field(name="originalFilename", default=None)
    size_bytes: int | None = strawberry.field(name="sizeBytes", default=None)
    lease_state: str = strawberry.field(name="leaseState")
    expires_at: str | None = strawberry.field(name="expiresAt", default=None)
    last_refreshed_at: str | None = strawberry.field(name="lastRefreshedAt", default=None)
    last_refresh_error: str | None = strawberry.field(name="lastRefreshError", default=None)
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)
    active_roles: list[str] = strawberry.field(name="activeRoles")
    source_key: str | None = strawberry.field(name="sourceKey", default=None)
    query_strategy: str | None = strawberry.field(name="queryStrategy", default=None)
    provider_family: str = strawberry.field(name="providerFamily")
    locator_source: str = strawberry.field(name="locatorSource")
    match_basis: str | None = strawberry.field(name="matchBasis", default=None)
    restricted_fallback: bool = strawberry.field(name="restrictedFallback")


@strawberry.type
class GQLVfsCatalogEntry:
    """One mounted catalog node exposed intentionally through GraphQL."""

    entry_id: str = strawberry.field(name="entryId")
    parent_entry_id: str | None = strawberry.field(name="parentEntryId", default=None)
    path: str
    name: str
    kind: str
    correlation: GQLVfsCorrelationKeys
    directory: GQLVfsDirectoryDetail | None = None
    file: GQLVfsFileDetail | None = None


@strawberry.type
class GQLVfsBreadcrumb:
    """One breadcrumb node for Director browse/detail navigation."""

    entry_id: str = strawberry.field(name="entryId")
    path: str
    name: str
    kind: str


@strawberry.type
class GQLVfsCatalogStats:
    """Aggregate counts for one mounted catalog snapshot."""

    directory_count: int = strawberry.field(name="directoryCount")
    file_count: int = strawberry.field(name="fileCount")
    blocked_item_count: int = strawberry.field(name="blockedItemCount")


@strawberry.type
class GQLVfsRollupBucket:
    """One named VFS rollup counter."""

    key: str
    count: int


@strawberry.type
class GQLVfsCatalogRollup:
    """Aggregate VFS posture counts derived from one published snapshot."""

    blocked_reasons: list[GQLVfsRollupBucket] = strawberry.field(name="blockedReasons")
    query_strategies: list[GQLVfsRollupBucket] = strawberry.field(name="queryStrategies")
    provider_families: list[GQLVfsRollupBucket] = strawberry.field(name="providerFamilies")
    lease_states: list[GQLVfsRollupBucket] = strawberry.field(name="leaseStates")
    locator_sources: list[GQLVfsRollupBucket] = strawberry.field(name="locatorSources")
    restricted_fallback_file_count: int = strawberry.field(name="restrictedFallbackFileCount")
    provider_path_preserved_file_count: int = strawberry.field(
        name="providerPathPreservedFileCount"
    )
    multi_role_file_count: int = strawberry.field(name="multiRoleFileCount")


@strawberry.type
class GQLVfsBlockedItem:
    """Blocked mounted item retained in one VFS snapshot."""

    item_id: str = strawberry.field(name="itemId")
    external_ref: str = strawberry.field(name="externalRef")
    title: str
    reason: str


@strawberry.type
class GQLVfsSnapshot:
    """Snapshot-level mounted VFS control-plane view."""

    generation_id: str = strawberry.field(name="generationId")
    published_at: str = strawberry.field(name="publishedAt")
    stats: GQLVfsCatalogStats
    rollup: GQLVfsCatalogRollup
    blocked_items: list[GQLVfsBlockedItem] = strawberry.field(name="blockedItems")


@strawberry.type
class GQLVfsDirectoryListing:
    """Immediate directory listing backed by the mounted VFS catalog snapshot."""

    generation_id: str = strawberry.field(name="generationId")
    path: str
    search_query: str | None = strawberry.field(name="searchQuery", default=None)
    entry: GQLVfsCatalogEntry
    focused_entry: GQLVfsCatalogEntry = strawberry.field(name="focusedEntry")
    parent: GQLVfsCatalogEntry | None = None
    breadcrumbs: list[GQLVfsBreadcrumb]
    directory_count: int = strawberry.field(name="directoryCount")
    file_count: int = strawberry.field(name="fileCount")
    total_directory_count: int = strawberry.field(name="totalDirectoryCount")
    total_file_count: int = strawberry.field(name="totalFileCount")
    sibling_index: int = strawberry.field(name="siblingIndex")
    sibling_count: int = strawberry.field(name="siblingCount")
    previous_entry: GQLVfsCatalogEntry | None = strawberry.field(name="previousEntry", default=None)
    next_entry: GQLVfsCatalogEntry | None = strawberry.field(name="nextEntry", default=None)
    stats: GQLVfsCatalogStats
    directories: list[GQLVfsCatalogEntry]
    files: list[GQLVfsCatalogEntry]


@strawberry.type
class GQLVfsSearchResult:
    """GraphQL-native VFS search result for Director browse surfaces."""

    generation_id: str = strawberry.field(name="generationId")
    query: str
    path_prefix: str = strawberry.field(name="pathPrefix")
    total_matches: int = strawberry.field(name="totalMatches")
    exact_match_count: int = strawberry.field(name="exactMatchCount")
    directory_matches: int = strawberry.field(name="directoryMatches")
    file_matches: int = strawberry.field(name="fileMatches")
    media_type_counts: list[GQLNamedCountBucket] = strawberry.field(name="mediaTypeCounts")
    provider_family_counts: list[GQLNamedCountBucket] = strawberry.field(name="providerFamilyCounts")
    lease_state_counts: list[GQLNamedCountBucket] = strawberry.field(name="leaseStateCounts")
    entries: list[GQLVfsCatalogEntry]


@strawberry.type
class GQLVfsFileContext:
    """File-focused VFS context for Director detail screens."""

    generation_id: str = strawberry.field(name="generationId")
    file: GQLVfsCatalogEntry
    directory: GQLVfsDirectoryListing
    sibling_file_index: int = strawberry.field(name="siblingFileIndex")
    sibling_file_count: int = strawberry.field(name="siblingFileCount")
    previous_file: GQLVfsCatalogEntry | None = strawberry.field(name="previousFile", default=None)
    next_file: GQLVfsCatalogEntry | None = strawberry.field(name="nextFile", default=None)


@strawberry.type
class GQLVfsOverview:
    """Screen-oriented VFS overview with snapshot and one directory listing."""

    snapshot: GQLVfsSnapshot
    directory: GQLVfsDirectoryListing


@strawberry.type
class GQLVfsCatalogGovernanceSummary:
    """Rollup of live VFS gRPC governance counters for Director operator screens."""

    active_watch_sessions: int = strawberry.field(name="activeWatchSessions")
    reconnect_requests: int = strawberry.field(name="reconnectRequests")
    reconnect_delta_served: int = strawberry.field(name="reconnectDeltaServed")
    reconnect_snapshot_fallbacks: int = strawberry.field(name="reconnectSnapshotFallbacks")
    reconnect_failures: int = strawberry.field(name="reconnectFailures")
    snapshots_served: int = strawberry.field(name="snapshotsServed")
    deltas_served: int = strawberry.field(name="deltasServed")
    heartbeats_served: int = strawberry.field(name="heartbeatsServed")
    problem_events: int = strawberry.field(name="problemEvents")
    request_stream_failures: int = strawberry.field(name="requestStreamFailures")
    refresh_attempts: int = strawberry.field(name="refreshAttempts")
    refresh_succeeded: int = strawberry.field(name="refreshSucceeded")
    refresh_provider_failures: int = strawberry.field(name="refreshProviderFailures")
    refresh_validation_failures: int = strawberry.field(name="refreshValidationFailures")
    inline_refresh_requests: int = strawberry.field(name="inlineRefreshRequests")
    inline_refresh_succeeded: int = strawberry.field(name="inlineRefreshSucceeded")
    inline_refresh_failed: int = strawberry.field(name="inlineRefreshFailed")


@strawberry.type
class GQLVfsCatalogGovernance:
    """GraphQL-native live FilmuVFS gRPC governance snapshot."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    counters: list[GQLNamedCountBucket]
    summary: GQLVfsCatalogGovernanceSummary
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLVfsCatalogDelta:
    """One typed VFS catalog delta rollup for Director/operator inspection."""

    generation_id: str = strawberry.field(name="generationId")
    base_generation_id: str | None = strawberry.field(name="baseGenerationId", default=None)
    published_at: str = strawberry.field(name="publishedAt")
    upsert_directory_count: int = strawberry.field(name="upsertDirectoryCount")
    upsert_file_count: int = strawberry.field(name="upsertFileCount")
    removal_directory_count: int = strawberry.field(name="removalDirectoryCount")
    removal_file_count: int = strawberry.field(name="removalFileCount")
    provider_family_counts: list[GQLNamedCountBucket] = strawberry.field(
        name="providerFamilyCounts"
    )
    lease_state_counts: list[GQLNamedCountBucket] = strawberry.field(name="leaseStateCounts")


@strawberry.type
class GQLVfsMountDiagnostics:
    """Shared mount diagnostics and delta-retention posture for Director/operator screens."""

    generated_at: str = strawberry.field(name="generatedAt")
    status: str
    supplier_attached: bool = strawberry.field(name="supplierAttached")
    server_attached: bool = strawberry.field(name="serverAttached")
    current_generation_id: str | None = strawberry.field(name="currentGenerationId", default=None)
    current_published_at: str | None = strawberry.field(name="currentPublishedAt", default=None)
    history_generation_ids: list[str] = strawberry.field(name="historyGenerationIds")
    history_generation_count: int = strawberry.field(name="historyGenerationCount")
    delta_history_ready: bool = strawberry.field(name="deltaHistoryReady")
    active_watch_sessions: int = strawberry.field(name="activeWatchSessions")
    snapshots_served: int = strawberry.field(name="snapshotsServed")
    deltas_served: int = strawberry.field(name="deltasServed")
    reconnect_delta_served: int = strawberry.field(name="reconnectDeltaServed")
    reconnect_snapshot_fallbacks: int = strawberry.field(name="reconnectSnapshotFallbacks")
    reconnect_failures: int = strawberry.field(name="reconnectFailures")
    request_stream_failures: int = strawberry.field(name="requestStreamFailures")
    problem_events: int = strawberry.field(name="problemEvents")
    refresh_provider_failures: int = strawberry.field(name="refreshProviderFailures")
    refresh_validation_failures: int = strawberry.field(name="refreshValidationFailures")
    required_actions: list[str] = strawberry.field(name="requiredActions")
    remaining_gaps: list[str] = strawberry.field(name="remainingGaps")


@strawberry.type
class GQLVfsGenerationHistoryPoint:
    """One retained VFS generation with snapshot and delta rollups for Director screens."""

    generation_id: str = strawberry.field(name="generationId")
    published_at: str = strawberry.field(name="publishedAt")
    entry_count: int = strawberry.field(name="entryCount")
    directory_count: int = strawberry.field(name="directoryCount")
    file_count: int = strawberry.field(name="fileCount")
    blocked_item_count: int = strawberry.field(name="blockedItemCount")
    blocked_reason_counts: list[GQLNamedCountBucket] = strawberry.field(name="blockedReasonCounts")
    query_strategy_counts: list[GQLNamedCountBucket] = strawberry.field(name="queryStrategyCounts")
    provider_family_counts: list[GQLNamedCountBucket] = strawberry.field(name="providerFamilyCounts")
    lease_state_counts: list[GQLNamedCountBucket] = strawberry.field(name="leaseStateCounts")
    delta_from_previous_available: bool = strawberry.field(name="deltaFromPreviousAvailable")
    delta_upsert_count: int = strawberry.field(name="deltaUpsertCount")
    delta_removal_count: int = strawberry.field(name="deltaRemovalCount")
    delta_upsert_file_count: int = strawberry.field(name="deltaUpsertFileCount")
    delta_removal_file_count: int = strawberry.field(name="deltaRemovalFileCount")


@strawberry.type
class GQLVfsGenerationHistorySummary:
    """Aggregate rollup over retained VFS generation history."""

    generation_count: int = strawberry.field(name="generationCount")
    newest_generation_id: str | None = strawberry.field(name="newestGenerationId", default=None)
    oldest_generation_id: str | None = strawberry.field(name="oldestGenerationId", default=None)
    max_entry_count: int = strawberry.field(name="maxEntryCount")
    max_file_count: int = strawberry.field(name="maxFileCount")
    blocked_generation_count: int = strawberry.field(name="blockedGenerationCount")
    total_delta_upsert_count: int = strawberry.field(name="totalDeltaUpsertCount")
    total_delta_removal_count: int = strawberry.field(name="totalDeltaRemovalCount")
    provider_family_counts: list[GQLNamedCountBucket] = strawberry.field(name="providerFamilyCounts")
    lease_state_counts: list[GQLNamedCountBucket] = strawberry.field(name="leaseStateCounts")


@strawberry.type
class GQLVfsCatalogDeltaHistorySummary:
    """Aggregate rollup over sequential retained VFS deltas."""

    delta_count: int = strawberry.field(name="deltaCount")
    max_upsert_count: int = strawberry.field(name="maxUpsertCount")
    max_removal_count: int = strawberry.field(name="maxRemovalCount")
    total_upsert_count: int = strawberry.field(name="totalUpsertCount")
    total_removal_count: int = strawberry.field(name="totalRemovalCount")
    total_upsert_file_count: int = strawberry.field(name="totalUpsertFileCount")
    total_removal_file_count: int = strawberry.field(name="totalRemovalFileCount")
    provider_family_counts: list[GQLNamedCountBucket] = strawberry.field(name="providerFamilyCounts")
    lease_state_counts: list[GQLNamedCountBucket] = strawberry.field(name="leaseStateCounts")


@strawberry.type
class GQLRuntimeLifecycleTransition:
    """One runtime lifecycle transition entry."""

    phase: str
    health: str
    detail: str
    at: str


@strawberry.type
class GQLRuntimeLifecycleSnapshot:
    """Current runtime lifecycle snapshot plus bounded transition history."""

    phase: str
    health: str
    detail: str
    updated_at: str = strawberry.field(name="updatedAt")
    transitions: list[GQLRuntimeLifecycleTransition]


@strawberry.type
class GQLQueueAlert:
    """One queue alert surfaced through the operator graph."""

    code: str
    severity: str
    message: str


@strawberry.type
class GQLWorkerQueueStatus:
    """Current worker queue status snapshot."""

    queue_name: str = strawberry.field(name="queueName")
    arq_enabled: bool = strawberry.field(name="arqEnabled")
    observed_at: str = strawberry.field(name="observedAt")
    total_jobs: int = strawberry.field(name="totalJobs")
    ready_jobs: int = strawberry.field(name="readyJobs")
    deferred_jobs: int = strawberry.field(name="deferredJobs")
    in_progress_jobs: int = strawberry.field(name="inProgressJobs")
    retry_jobs: int = strawberry.field(name="retryJobs")
    result_jobs: int = strawberry.field(name="resultJobs")
    dead_letter_jobs: int = strawberry.field(name="deadLetterJobs")
    alert_level: str = strawberry.field(name="alertLevel")
    alerts: list[GQLQueueAlert]
    oldest_ready_age_seconds: float | None = strawberry.field(
        name="oldestReadyAgeSeconds", default=None
    )
    next_scheduled_in_seconds: float | None = strawberry.field(
        name="nextScheduledInSeconds", default=None
    )
    dead_letter_oldest_age_seconds: float | None = strawberry.field(
        name="deadLetterOldestAgeSeconds", default=None
    )
    dead_letter_reason_counts: JSON = strawberry.field(
        name="deadLetterReasonCounts", default_factory=dict
    )


@strawberry.type
class GQLWorkerQueueHistoryPoint:
    """One persisted queue history point."""

    observed_at: str = strawberry.field(name="observedAt")
    total_jobs: int = strawberry.field(name="totalJobs")
    ready_jobs: int = strawberry.field(name="readyJobs")
    deferred_jobs: int = strawberry.field(name="deferredJobs")
    in_progress_jobs: int = strawberry.field(name="inProgressJobs")
    retry_jobs: int = strawberry.field(name="retryJobs")
    dead_letter_jobs: int = strawberry.field(name="deadLetterJobs")
    oldest_ready_age_seconds: float | None = strawberry.field(
        name="oldestReadyAgeSeconds", default=None
    )
    next_scheduled_in_seconds: float | None = strawberry.field(
        name="nextScheduledInSeconds", default=None
    )
    alert_level: str = strawberry.field(name="alertLevel")
    dead_letter_oldest_age_seconds: float | None = strawberry.field(
        name="deadLetterOldestAgeSeconds", default=None
    )
    dead_letter_reason_counts: JSON = strawberry.field(
        name="deadLetterReasonCounts", default_factory=dict
    )


@strawberry.type
class GQLMetadataReindexStatus:
    """Latest metadata reindex/reconciliation run summary."""

    queue_name: str = strawberry.field(name="queueName")
    schedule_offset_minutes: int = strawberry.field(name="scheduleOffsetMinutes")
    has_history: bool = strawberry.field(name="hasHistory")
    observed_at: str = strawberry.field(name="observedAt")
    processed: int
    queued: int
    reconciled: int
    skipped_active: int = strawberry.field(name="skippedActive")
    failed: int
    repair_attempted: int = strawberry.field(name="repairAttempted")
    repair_enriched: int = strawberry.field(name="repairEnriched")
    repair_skipped_no_tmdb_id: int = strawberry.field(name="repairSkippedNoTmdbId")
    repair_failed: int = strawberry.field(name="repairFailed")
    repair_requeued: int = strawberry.field(name="repairRequeued")
    repair_skipped_active: int = strawberry.field(name="repairSkippedActive")
    outcome: str
    run_failed: bool = strawberry.field(name="runFailed")
    last_error: str | None = strawberry.field(name="lastError", default=None)


@strawberry.type
class GQLMetadataReindexHistoryPoint:
    """One persisted metadata reindex/reconciliation history point."""

    observed_at: str = strawberry.field(name="observedAt")
    processed: int
    queued: int
    reconciled: int
    skipped_active: int = strawberry.field(name="skippedActive")
    failed: int
    repair_attempted: int = strawberry.field(name="repairAttempted")
    repair_enriched: int = strawberry.field(name="repairEnriched")
    repair_skipped_no_tmdb_id: int = strawberry.field(name="repairSkippedNoTmdbId")
    repair_failed: int = strawberry.field(name="repairFailed")
    repair_requeued: int = strawberry.field(name="repairRequeued")
    repair_skipped_active: int = strawberry.field(name="repairSkippedActive")
    outcome: str
    run_failed: bool = strawberry.field(name="runFailed")
    last_error: str | None = strawberry.field(name="lastError", default=None)


@strawberry.type
class GQLLibraryStats:
    """Intentional GraphQL stats type above the compatibility REST contract."""

    total_items: int = strawberry.field(name="totalItems")
    total_movies: int = strawberry.field(name="totalMovies")
    total_shows: int = strawberry.field(name="totalShows")
    total_seasons: int = strawberry.field(name="totalSeasons")
    total_episodes: int = strawberry.field(name="totalEpisodes")
    completed_items: int = strawberry.field(name="completedItems")
    incomplete_items: int = strawberry.field(name="incompleteItems")
    failed_items: int = strawberry.field(name="failedItems")
    # Placeholder until Director or another product client defines the exact richer JSON contract it wants.
    state_breakdown: str | None = strawberry.field(name="stateBreakdown", default=None)
    # Placeholder until Director or another product client defines the exact richer JSON contract it wants.
    activity: str | None = None


@strawberry.type
class GQLStreamCandidate:
    """GraphQL stream-candidate projection for intentional media detail queries."""

    id: strawberry.ID
    raw_title: str = strawberry.field(name="rawTitle")
    parsed_title: str | None = strawberry.field(name="parsedTitle", default=None)
    resolution: str | None = None
    rank_score: int = strawberry.field(name="rankScore")
    lev_ratio: float | None = strawberry.field(name="levRatio", default=None)
    selected: bool
    passed: bool | None = None
    rejection_reason: str | None = strawberry.field(name="rejectionReason", default=None)


@strawberry.type
class GQLRecoveryPlan:
    """Intentional recovery projection above the REST compatibility surface."""

    mechanism: GQLRecoveryMechanism
    target_stage: GQLRecoveryTargetStage = strawberry.field(name="targetStage")
    reason: str
    next_retry_at: str | None = strawberry.field(name="nextRetryAt", default=None)
    recovery_attempt_count: int = strawberry.field(name="recoveryAttemptCount")
    is_in_cooldown: bool = strawberry.field(name="isInCooldown")


@strawberry.type
class GQLPlaybackAttachment:
    """Persisted playback attachment projection for graph item detail."""

    id: str
    kind: str
    locator: str
    source_key: str | None = strawberry.field(name="sourceKey", default=None)
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)
    original_filename: str | None = strawberry.field(name="originalFilename", default=None)
    file_size: int | None = strawberry.field(name="fileSize", default=None)
    local_path: str | None = strawberry.field(name="localPath", default=None)
    restricted_url: str | None = strawberry.field(name="restrictedUrl", default=None)
    unrestricted_url: str | None = strawberry.field(name="unrestrictedUrl", default=None)
    is_preferred: bool = strawberry.field(name="isPreferred")
    preference_rank: int = strawberry.field(name="preferenceRank")
    refresh_state: str = strawberry.field(name="refreshState")
    expires_at: str | None = strawberry.field(name="expiresAt", default=None)
    last_refreshed_at: str | None = strawberry.field(name="lastRefreshedAt", default=None)
    last_refresh_error: str | None = strawberry.field(name="lastRefreshError", default=None)


@strawberry.type
class GQLResolvedPlaybackAttachment:
    """Current resolved playback attachment for direct or HLS access."""

    kind: str
    locator: str
    source_key: str = strawberry.field(name="sourceKey")
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)
    original_filename: str | None = strawberry.field(name="originalFilename", default=None)
    file_size: int | None = strawberry.field(name="fileSize", default=None)
    local_path: str | None = strawberry.field(name="localPath", default=None)
    restricted_url: str | None = strawberry.field(name="restrictedUrl", default=None)
    unrestricted_url: str | None = strawberry.field(name="unrestrictedUrl", default=None)


@strawberry.type
class GQLResolvedPlayback:
    """Resolved playback readiness snapshot for one item."""

    direct: GQLResolvedPlaybackAttachment | None = None
    hls: GQLResolvedPlaybackAttachment | None = None
    direct_ready: bool = strawberry.field(name="directReady")
    hls_ready: bool = strawberry.field(name="hlsReady")
    missing_local_file: bool = strawberry.field(name="missingLocalFile")


@strawberry.type
class GQLActiveStreamOwner:
    """Ownership link from an active playback role to one media entry."""

    media_entry_index: int = strawberry.field(name="mediaEntryIndex")
    kind: str
    original_filename: str | None = strawberry.field(name="originalFilename", default=None)
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)


@strawberry.type
class GQLActiveStream:
    """Current active-stream readiness and owner mapping."""

    direct_ready: bool = strawberry.field(name="directReady")
    hls_ready: bool = strawberry.field(name="hlsReady")
    missing_local_file: bool = strawberry.field(name="missingLocalFile")
    direct_owner: GQLActiveStreamOwner | None = strawberry.field(name="directOwner", default=None)
    hls_owner: GQLActiveStreamOwner | None = strawberry.field(name="hlsOwner", default=None)


@strawberry.type
class GQLMediaEntry:
    """Mounted/playback-facing media-entry projection for graph item detail."""

    entry_type: str = strawberry.field(name="entryType")
    kind: str
    original_filename: str | None = strawberry.field(name="originalFilename", default=None)
    url: str | None = None
    local_path: str | None = strawberry.field(name="localPath", default=None)
    download_url: str | None = strawberry.field(name="downloadUrl", default=None)
    unrestricted_url: str | None = strawberry.field(name="unrestrictedUrl", default=None)
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)
    size: int | None = None
    created: str | None = None
    modified: str | None = None
    refresh_state: str = strawberry.field(name="refreshState")
    expires_at: str | None = strawberry.field(name="expiresAt", default=None)
    last_refreshed_at: str | None = strawberry.field(name="lastRefreshedAt", default=None)
    last_refresh_error: str | None = strawberry.field(name="lastRefreshError", default=None)
    active_for_direct: bool = strawberry.field(name="activeForDirect")
    active_for_hls: bool = strawberry.field(name="activeForHls")
    is_active_stream: bool = strawberry.field(name="isActiveStream")


@strawberry.type
class GQLMediaItemDetail:
    """Intentional GraphQL item detail type with stream-candidate visibility."""

    id: strawberry.ID
    title: str
    state: str
    item_type: str | None = strawberry.field(name="itemType", default=None)
    media_type: str = strawberry.field(name="mediaType")
    media_kind: MediaKind = strawberry.field(name="mediaKind")
    tmdb_id: int | None = strawberry.field(name="tmdbId", default=None)
    tvdb_id: int | None = strawberry.field(name="tvdbId", default=None)
    imdb_id: str | None = strawberry.field(name="imdbId", default=None)
    parent_tmdb_id: int | None = strawberry.field(name="parentTmdbId", default=None)
    parent_tvdb_id: int | None = strawberry.field(name="parentTvdbId", default=None)
    show_title: str | None = strawberry.field(name="showTitle", default=None)
    season_number: int | None = strawberry.field(name="seasonNumber", default=None)
    episode_number: int | None = strawberry.field(name="episodeNumber", default=None)
    created_at: str = strawberry.field(name="createdAt")
    updated_at: str = strawberry.field(name="updatedAt")
    stream_candidates: list[GQLStreamCandidate] = strawberry.field(name="streamCandidates")
    selected_stream: GQLStreamCandidate | None = strawberry.field(
        name="selectedStream", default=None
    )
    recovery_plan: GQLRecoveryPlan = strawberry.field(name="recoveryPlan")
    playback_attachments: list[GQLPlaybackAttachment] = strawberry.field(
        name="playbackAttachments", default_factory=list
    )
    resolved_playback: GQLResolvedPlayback | None = strawberry.field(
        name="resolvedPlayback", default=None
    )
    active_stream: GQLActiveStream | None = strawberry.field(name="activeStream", default=None)
    media_entries: list[GQLMediaEntry] = strawberry.field(name="mediaEntries", default_factory=list)


@strawberry.type
class GQLFilmuSettings:
    """Core filmu settings exposed through GraphQL compatibility schema."""

    version: str
    api_key: str = strawberry.field(name="apiKey")
    log_level: str = strawberry.field(name="logLevel")


@strawberry.type
class GQLSettings:
    """Settings root object for parity with upstream GraphQL settings query."""

    filmu: GQLFilmuSettings


@strawberry.type
class GQLItemEvent:
    """Subscription event representing media item state transitions."""

    item_id: strawberry.ID
    state: str
    message: str


@strawberry.type
class ItemStateChangedEvent:
    """Mirrors the existing SSE `item.state.changed` payload as a compat GraphQL type."""

    # COMPAT: keep field names aligned with the current SSE contract until the new frontend expands them.
    item_id: str = strawberry.field(name="item_id")
    from_state: str | None = strawberry.field(name="from_state", default=None)
    to_state: str = strawberry.field(name="to_state")
    timestamp: str


@strawberry.type
class RetryItemResult:
    item_id: str = strawberry.field(name="itemId")
    success: bool
    error: str | None = None
    new_state: str | None = strawberry.field(name="newState", default=None)


@strawberry.type
class ResetItemResult:
    item_id: str = strawberry.field(name="itemId")
    success: bool
    error: str | None = None
    new_state: str | None = strawberry.field(name="newState", default=None)


@strawberry.type
class GQLPlaybackRefreshTriggerResult:
    """GraphQL control-plane trigger result for direct-play and selected-HLS refresh paths."""

    item_id: str = strawberry.field(name="itemId")
    outcome: str
    controller_attached: bool = strawberry.field(name="controllerAttached")
    control_plane_outcome: str | None = strawberry.field(name="controlPlaneOutcome", default=None)
    refresh_outcome: str | None = strawberry.field(name="refreshOutcome", default=None)
    execution_ok: bool | None = strawberry.field(name="executionOk", default=None)
    execution_refresh_state: str | None = strawberry.field(
        name="executionRefreshState", default=None
    )
    execution_locator: str | None = strawberry.field(name="executionLocator", default=None)
    execution_error: str | None = strawberry.field(name="executionError", default=None)
    retry_after_seconds: float | None = strawberry.field(
        name="retryAfterSeconds", default=None
    )
    deferred_reason: str | None = strawberry.field(name="deferredReason", default=None)
    scheduled_requested_at: str | None = strawberry.field(
        name="scheduledRequestedAt", default=None
    )
    scheduled_not_before: str | None = strawberry.field(
        name="scheduledNotBefore", default=None
    )


@strawberry.type
class GQLMarkSelectedHlsMediaEntryStaleResult:
    """GraphQL mutation result for marking the selected HLS media entry stale."""

    item_id: str = strawberry.field(name="itemId")
    success: bool
    error: str | None = None


@strawberry.input
class PersistMediaEntryControlInput:
    """Bounded persisted media-entry URL/state mutation input for graph control-plane writes."""

    item_id: strawberry.ID = strawberry.field(name="itemId")
    media_entry_id: strawberry.ID = strawberry.field(name="mediaEntryId")
    active_role: GQLActiveStreamRole | None = strawberry.field(name="activeRole", default=None)
    local_path: str | None = strawberry.field(name="localPath", default=None)
    download_url: str | None = strawberry.field(name="downloadUrl", default=None)
    unrestricted_url: str | None = strawberry.field(name="unrestrictedUrl", default=None)
    refresh_state: str | None = strawberry.field(name="refreshState", default=None)
    last_refresh_error: str | None = strawberry.field(name="lastRefreshError", default=None)
    expires_at: str | None = strawberry.field(name="expiresAt", default=None)


@strawberry.input
class PersistVfsRolloutControlInput:
    """Bounded GraphQL mutation input for VFS rollout-control state."""

    environment_class: str | None = strawberry.field(name="environmentClass", default=None)
    runtime_status_path: str | None = strawberry.field(name="runtimeStatusPath", default=None)
    promotion_paused: bool | None = strawberry.field(name="promotionPaused", default=None)
    promotion_pause_reason: str | None = strawberry.field(
        name="promotionPauseReason",
        default=None,
    )
    promotion_pause_expires_at: str | None = strawberry.field(
        name="promotionPauseExpiresAt",
        default=None,
    )
    rollback_requested: bool | None = strawberry.field(name="rollbackRequested", default=None)
    rollback_reason: str | None = strawberry.field(name="rollbackReason", default=None)
    rollback_expires_at: str | None = strawberry.field(
        name="rollbackExpiresAt",
        default=None,
    )
    notes: str | None = None


@strawberry.type
class GQLPersistMediaEntryControlResult:
    """GraphQL result for one persisted media-entry control-plane mutation."""

    item_id: str = strawberry.field(name="itemId")
    media_entry_id: str = strawberry.field(name="mediaEntryId")
    success: bool
    error: str | None = None
    applied_role: str | None = strawberry.field(name="appliedRole", default=None)
    media_entry: GQLMediaEntry | None = strawberry.field(name="mediaEntry", default=None)


@strawberry.input
class PersistPlaybackAttachmentControlInput:
    """Bounded persisted playback-attachment URL/state mutation input for graph control-plane writes."""

    item_id: strawberry.ID = strawberry.field(name="itemId")
    attachment_id: strawberry.ID = strawberry.field(name="attachmentId")
    locator: str | None = None
    local_path: str | None = strawberry.field(name="localPath", default=None)
    restricted_url: str | None = strawberry.field(name="restrictedUrl", default=None)
    unrestricted_url: str | None = strawberry.field(name="unrestrictedUrl", default=None)
    refresh_state: str | None = strawberry.field(name="refreshState", default=None)
    last_refresh_error: str | None = strawberry.field(name="lastRefreshError", default=None)
    expires_at: str | None = strawberry.field(name="expiresAt", default=None)


@strawberry.type
class GQLPersistPlaybackAttachmentControlResult:
    """GraphQL result for one persisted playback-attachment control-plane mutation."""

    item_id: str = strawberry.field(name="itemId")
    attachment_id: str = strawberry.field(name="attachmentId")
    success: bool
    error: str | None = None
    attachment: GQLPlaybackAttachment | None = None
    linked_media_entries: list[GQLMediaEntry] = strawberry.field(
        name="linkedMediaEntries",
        default_factory=list,
    )


@strawberry.type
class LogEntry:
    """Intentional structured log-stream entry for future GraphQL consumers."""

    timestamp: str
    level: str
    event: str
    worker_id: str | None = strawberry.field(name="worker_id", default=None)
    item_id: str | None = strawberry.field(name="item_id", default=None)
    stage: str | None = None
    extra: JSON = strawberry.field(default_factory=dict)


@strawberry.type
class NotificationEvent:
    """Mirrors the existing SSE notification payload as a compat GraphQL type."""

    # COMPAT: keep field names aligned with the current SSE contract until the new frontend expands them.
    event_type: str = strawberry.field(name="event_type")
    title: str | None = None
    message: str | None = None
    timestamp: str


@strawberry.type
class RequestItemResult:
    """Additive request-intake result for future GraphQL consumers."""

    item_id: strawberry.ID = strawberry.field(name="itemId")
    enrichment_source: str = strawberry.field(name="enrichmentSource")
    has_poster: bool = strawberry.field(name="hasPoster")
    has_imdb_id: bool = strawberry.field(name="hasImdbId")
    warnings: list[str]


@strawberry.input
class RequestItemInput:
    """Request a media item by external identifier."""

    external_ref: str = strawberry.field(name="externalRef")
    media_type: str = strawberry.field(name="mediaType")
    requested_seasons: list[int] | None = strawberry.field(name="requestedSeasons", default=None)


@strawberry.input
class ItemActionInput:
    """Trigger a state action on an existing item."""

    item_id: str = strawberry.field(name="itemId")
    action: str


@strawberry.input
class SettingsUpdateInput:
    """Update one settings path with a JSON-serializable value."""

    path: str
    value: strawberry.scalars.JSON


@strawberry.enum
class GQLItemTransitionEvent(StrEnum):
    """Allowed item transition events for mutation operations."""

    INDEX = "index"
    SCRAPE = "scrape"
    DOWNLOAD = "download"
    COMPLETE = "complete"
    FAIL = "fail"
    RETRY = "retry"
    PARTIAL_COMPLETE = "partial_complete"
    MARK_ONGOING = "mark_ongoing"
    MARK_UNRELEASED = "mark_unreleased"
