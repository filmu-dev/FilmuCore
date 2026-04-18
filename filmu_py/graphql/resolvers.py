"""Composable GraphQL resolver classes for plugin-dfilmu schema growth."""

# mypy: disable-error-code=untyped-decorator

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from importlib.metadata import PackageNotFoundError, version
from pathlib import PurePosixPath
from types import SimpleNamespace
from typing import Any, cast

import strawberry
from fastapi import HTTPException
from strawberry.scalars import JSON
from strawberry.types import Info

from filmu_py.audit import audit_action
from filmu_py.core.metadata_reindex_status import MetadataReindexStatusStore
from filmu_py.core.queue_status import QueueStatusReader
from filmu_py.core.runtime_lifecycle import RuntimeLifecycleSnapshot
from filmu_py.db.models import StreamORM
from filmu_py.graphql.deps import GraphQLContext, require_graphql_permissions
from filmu_py.graphql.types import (
    AccessPolicyRevisionApprovalInput,
    AccessPolicyRevisionWriteInput,
    ControlPlanePendingRecoveryInput,
    GQLAccessPolicyRevision,
    GQLAccessPolicyRevisionList,
    GQLActiveStream,
    GQLActiveStreamOwner,
    GQLCalendarEntry,
    GQLCalendarReleaseWindow,
    GQLControlPlaneAckRecovery,
    GQLControlPlaneAutomation,
    GQLControlPlaneConsumerSummary,
    GQLControlPlaneOwnershipSummary,
    GQLControlPlanePendingRecovery,
    GQLControlPlaneRecoveryReadiness,
    GQLControlPlaneRemediation,
    GQLControlPlaneReplayBackplane,
    GQLControlPlaneStatusCount,
    GQLControlPlaneSubscriber,
    GQLControlPlaneSummary,
    GQLDownloaderDeadLetterTimelinePoint,
    GQLDownloaderExecutionDeadLetter,
    GQLDownloaderExecutionEvidence,
    GQLDownloaderExecutionTrendSummary,
    GQLDownloaderFailureKindSummary,
    GQLDownloaderOrchestration,
    GQLDownloaderProviderCandidate,
    GQLDownloaderProviderSummary,
    GQLDownloaderReasonSummary,
    GQLDownloaderStatusCodeSummary,
    GQLEnterpriseOperationsGovernance,
    GQLEnterpriseOperationsSlice,
    GQLEnterpriseRolloutEvidence,
    GQLFilmuSettings,
    GQLGovernanceArtifactInventoryItem,
    GQLGovernanceEvidenceCheck,
    GQLGovernanceStatusCount,
    GQLHealthCheck,
    GQLItemEvent,
    GQLLibraryStats,
    GQLMarkSelectedHlsMediaEntryStaleResult,
    GQLMediaEntry,
    GQLMediaItem,
    GQLMediaItemDetail,
    GQLMetadataReindexHistoryPoint,
    GQLMetadataReindexStatus,
    GQLNamedCountBucket,
    GQLObservabilityConvergence,
    GQLObservabilityConvergenceSummary,
    GQLObservabilityFieldContractSummary,
    GQLObservabilityPipelineStage,
    GQLObservabilityRolloutSummary,
    GQLOperatorActionItem,
    GQLOperatorGapItem,
    GQLPersistMediaEntryControlResult,
    GQLPersistPlaybackAttachmentControlResult,
    GQLPlaybackAttachment,
    GQLPlaybackGateGovernance,
    GQLPlaybackRefreshTriggerResult,
    GQLPluginCapabilityStatus,
    GQLPluginEventStatus,
    GQLPluginGovernance,
    GQLPluginGovernanceOverride,
    GQLPluginGovernanceSummary,
    GQLPluginIntegrationReadiness,
    GQLPluginIntegrationReadinessPlugin,
    GQLPluginIntegrationReadinessSummary,
    GQLPluginProofCoverageSummary,
    GQLPluginRuntimeCapabilitySummary,
    GQLPluginRuntimeOverview,
    GQLPluginRuntimePublisherSummary,
    GQLPluginRuntimeRow,
    GQLPluginRuntimeWarning,
    GQLProofArtifact,
    GQLQueueAlert,
    GQLRecoveryMechanism,
    GQLRecoveryPlan,
    GQLRecoveryTargetStage,
    GQLResolvedPlayback,
    GQLResolvedPlaybackAttachment,
    GQLRuntimeLifecycleSnapshot,
    GQLRuntimeLifecycleTransition,
    GQLStreamCandidate,
    GQLVfsBlockedItem,
    GQLVfsBreadcrumb,
    GQLVfsCatalogDelta,
    GQLVfsCatalogDeltaHistorySummary,
    GQLVfsCatalogEntry,
    GQLVfsCatalogGovernance,
    GQLVfsCatalogGovernanceSummary,
    GQLVfsCatalogRollup,
    GQLVfsCatalogStats,
    GQLVfsCorrelationKeys,
    GQLVfsDirectoryDetail,
    GQLVfsDirectoryListing,
    GQLVfsFileContext,
    GQLVfsFileDetail,
    GQLVfsGenerationHistoryPoint,
    GQLVfsGenerationHistorySummary,
    GQLVfsMountDiagnostics,
    GQLVfsOverview,
    GQLVfsRolloutControl,
    GQLVfsRolloutLedgerEntry,
    GQLVfsRollupBucket,
    GQLVfsRuntimePercentiles,
    GQLVfsRuntimePythonSessionRollup,
    GQLVfsRuntimeReadAmplification,
    GQLVfsRuntimeRollout,
    GQLVfsRuntimeRustHandleRollup,
    GQLVfsRuntimeTelemetry,
    GQLVfsSearchResult,
    GQLVfsSnapshot,
    GQLWorkerQueueHistoryPoint,
    GQLWorkerQueueHistorySummary,
    GQLWorkerQueueStatus,
    ItemActionInput,
    ItemStateChangedEvent,
    MediaKind,
    PersistMediaEntryControlInput,
    PersistPlaybackAttachmentControlInput,
    PersistVfsRolloutControlInput,
    PluginGovernanceOverrideWriteInput,
    RequestItemInput,
    RequestItemResult,
    ResetItemResult,
    RetryItemResult,
    SettingsUpdateInput,
)
from filmu_py.observability_convergence import (
    build_observability_convergence_snapshot,
    build_observability_rollout_summary,
)
from filmu_py.services.governance_posture import (
    build_downloader_execution_trend_summary,
    build_downloader_provider_summaries,
    build_downloader_reason_summaries,
    build_enterprise_rollout_action_items,
    build_enterprise_rollout_artifact_inventory,
    build_enterprise_rollout_evidence_posture,
    build_enterprise_rollout_gap_items,
    build_enterprise_rollout_status_counts,
    build_playback_gate_governance_posture,
    build_plugin_proof_coverage_summaries,
    build_plugin_runtime_action_items,
    build_plugin_runtime_capability_summaries,
    build_plugin_runtime_gap_items,
    build_plugin_runtime_overview_posture,
    build_plugin_runtime_rows,
    build_vfs_generation_history_posture,
    build_vfs_generation_history_summary,
    build_vfs_runtime_rollout_posture,
    build_vfs_runtime_telemetry_posture,
)
from filmu_py.services.graphql_support_posture import (
    build_control_plane_action_items,
    build_control_plane_consumer_summaries,
    build_control_plane_gap_items,
    build_control_plane_node_counts,
    build_control_plane_ownership_summary,
    build_control_plane_replay_consumer_counts,
    build_control_plane_status_counts,
    build_control_plane_tenant_counts,
    build_downloader_action_items,
    build_downloader_alert_level_counts,
    build_downloader_dead_letter_timeline,
    build_downloader_failure_kind_summaries,
    build_downloader_gap_items,
    build_downloader_status_code_summaries,
    build_observability_action_items,
    build_observability_field_contract_summary,
    build_observability_gap_items,
    build_observability_proof_inventory,
    build_observability_stage_counts,
    build_plugin_runtime_capability_action_counts,
    build_plugin_runtime_capability_gap_counts,
    build_plugin_runtime_publisher_summaries,
    build_plugin_runtime_status_counts,
    build_plugin_runtime_wiring_status_counts,
    build_vfs_blocked_reason_summaries,
    build_vfs_catalog_delta_history,
    build_vfs_catalog_delta_history_summary,
    build_vfs_mount_action_items,
    build_vfs_mount_gap_items,
)
from filmu_py.services.media import (
    ArqNotEnabledError,
    CalendarProjectionRecord,
    ItemActionResult,
    ItemNotFoundError,
    MediaItemRecord,
    MediaItemSummaryRecord,
    RecoveryPlanRecord,
    StatsProjection,
    _canonical_item_type_name,
    _infer_request_media_type,
)
from filmu_py.services.operator_posture import (
    build_control_plane_automation_posture,
    build_control_plane_recovery_readiness_posture,
    build_control_plane_replay_backplane_posture,
    build_control_plane_subscribers_posture,
    build_control_plane_summary_posture,
    build_downloader_execution_evidence_posture,
    build_downloader_orchestration_posture,
    build_plugin_event_status_posture,
    build_plugin_governance_posture,
    build_plugin_integration_readiness_posture,
    build_vfs_catalog_governance_posture,
    build_vfs_mount_diagnostics_posture,
)
from filmu_py.services.playback import (
    AppScopedDirectPlaybackRefreshTriggerResult,
    AppScopedHlsFailedLeaseRefreshTriggerResult,
    AppScopedHlsRestrictedFallbackRefreshTriggerResult,
    PersistedMediaEntryControlMutationResult,
    PersistedPlaybackAttachmentControlMutationResult,
    trigger_direct_playback_refresh_from_resources,
    trigger_hls_failed_lease_refresh_from_resources,
    trigger_hls_restricted_fallback_refresh_from_resources,
)
from filmu_py.services.vfs_catalog import (
    VfsCatalogDelta,
    VfsCatalogEntry,
    VfsCatalogRollup,
    VfsCatalogSnapshot,
    summarize_vfs_catalog_delta,
    summarize_vfs_catalog_snapshot,
)


def _compat_route_module() -> Any:
    """Resolve compatibility route helpers lazily to avoid import-time cycles."""

    from filmu_py.api.routes import default as default_routes

    return default_routes


_GRAPHQL_CONTROL_PLANE_CACHE_TTL_SECONDS = 30
_GRAPHQL_ACCESS_POLICY_REVISIONS_CACHE_KEY = (
    "graphql:control_plane:access_policy_revisions"
)
_GRAPHQL_PLUGIN_GOVERNANCE_CACHE_KEY = "graphql:control_plane:plugin_governance"
_GRAPHQL_PLUGIN_GOVERNANCE_OVERRIDES_CACHE_KEY = (
    "graphql:control_plane:plugin_governance_overrides"
)


async def _read_cached_graphql_payload(
    info: Info[GraphQLContext, object],
    *,
    key: str,
) -> object | None:
    cached = await info.context.resources.cache.get(key)
    if not isinstance(cached, bytes):
        return None
    try:
        return cast(object, json.loads(cached.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError):
        await info.context.resources.cache.invalidate(key, reason="decode_error")
        return None


async def _write_cached_graphql_payload(
    info: Info[GraphQLContext, object],
    *,
    key: str,
    payload: object,
    ttl_seconds: int = _GRAPHQL_CONTROL_PLANE_CACHE_TTL_SECONDS,
) -> None:
    await info.context.resources.cache.set(
        key,
        json.dumps(payload, default=str, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        ttl_seconds=ttl_seconds,
    )


async def _invalidate_graphql_control_plane_cache(
    info: Info[GraphQLContext, object],
    *keys: str,
    reason: str,
) -> None:
    for key in keys:
        await info.context.resources.cache.invalidate(key, reason=reason)


def _raise_graphql_compat_error(exc: Exception) -> None:
    """Normalize compatibility-route exceptions into GraphQL-safe errors."""

    if isinstance(exc, HTTPException):
        raise ValueError(str(exc.detail)) from exc
    raise ValueError(str(exc)) from exc


def _resolve_service_version() -> str:
    """Resolve package version for GraphQL settings parity output."""

    try:
        return version("filmu-python")
    except PackageNotFoundError:
        return "0.1.0"


def build_filmu_settings(info: Info[GraphQLContext, object]) -> GQLFilmuSettings:
    """Build the core `filmu` settings object for the GraphQL settings root."""

    current_settings = info.context.resources.settings
    return GQLFilmuSettings(
        version=_resolve_service_version(),
        api_key=current_settings.api_key.get_secret_value(),
        log_level=current_settings.log_level,
    )


def _to_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    return int(value) if value.isdigit() else None


def _serialize_release_data(record: CalendarProjectionRecord) -> str | None:
    if record.release_data is None:
        return None
    return json.dumps(
        {
            "next_aired": record.release_data.next_aired,
            "nextAired": record.release_data.nextAired,
            "last_aired": record.release_data.last_aired,
            "lastAired": record.release_data.lastAired,
        }
    )


def _record_media_type(record: MediaItemRecord) -> str:
    return _canonical_item_type_name(
        _infer_request_media_type(external_ref=record.external_ref, attributes=record.attributes)
    )


def _summary_media_type(record: MediaItemSummaryRecord) -> str:
    return _canonical_item_type_name(record.type)


def _media_kind(media_type: str) -> MediaKind:
    normalized = _canonical_item_type_name(media_type)
    if normalized == "movie":
        return MediaKind.MOVIE
    if normalized == "show":
        return MediaKind.SHOW
    if normalized == "season":
        return MediaKind.SEASON
    if normalized == "episode":
        return MediaKind.EPISODE
    raise ValueError(f"unsupported media type for MediaKind: {media_type}")


def _build_calendar_entry(record: CalendarProjectionRecord) -> GQLCalendarEntry:
    specialization = record.specialization
    release_data = record.release_data
    return GQLCalendarEntry(
        item_id=strawberry.ID(record.item_id),
        show_title=record.title,
        item_type=record.item_type,
        aired_at=record.air_date,
        last_state=record.last_state or "Unknown",
        season=record.season_number,
        episode=record.episode_number,
        tmdb_id=_to_optional_int(record.tmdb_id),
        tvdb_id=_to_optional_int(record.tvdb_id),
        imdb_id=(specialization.imdb_id if specialization is not None else None),
        parent_tmdb_id=(
            _to_optional_int(specialization.parent_ids.tmdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        parent_tvdb_id=(
            _to_optional_int(specialization.parent_ids.tvdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        release_data=_serialize_release_data(record),
        release_window=(
            GQLCalendarReleaseWindow(
                next_aired=(release_data.next_aired or release_data.nextAired),
                last_aired=(release_data.last_aired or release_data.lastAired),
            )
            if release_data is not None
            else None
        ),
    )


def _build_observability_convergence(info: Info[GraphQLContext, object]) -> GQLObservabilityConvergence:
    snapshot = build_observability_convergence_snapshot(info.context.resources.settings)
    ready_stage_count = sum(1 for stage in snapshot.pipeline_stages if stage.ready)
    proof_artifacts = [
        GQLProofArtifact(
            ref=ref,
            category="observability_rollout",
            label="observability rollout proof",
            recorded=True,
        )
        for ref in snapshot.proof_refs
        if str(ref).strip()
    ]
    proof_refs = [artifact.ref for artifact in proof_artifacts]
    return GQLObservabilityConvergence(
        generated_at=snapshot.generated_at,
        status=snapshot.status,
        structured_logging_enabled=snapshot.structured_logging_enabled,
        structured_log_path=snapshot.structured_log_path,
        otel_enabled=snapshot.otel_enabled,
        otel_endpoint_configured=snapshot.otel_endpoint_configured,
        log_shipper_enabled=snapshot.log_shipper_enabled,
        log_shipper_type=snapshot.log_shipper_type,
        log_shipper_target_configured=snapshot.log_shipper_target_configured,
        log_shipper_healthcheck_configured=snapshot.log_shipper_healthcheck_configured,
        search_backend=snapshot.search_backend,
        environment_shipping_enabled=snapshot.environment_shipping_enabled,
        alerting_enabled=snapshot.alerting_enabled,
        rust_trace_correlation_enabled=snapshot.rust_trace_correlation_enabled,
        correlation_contract_complete=snapshot.correlation_contract_complete,
        proof_refs=proof_refs,
        required_correlation_fields=list(snapshot.required_correlation_fields),
        required_actions=list(snapshot.required_actions),
        remaining_gaps=list(snapshot.remaining_gaps),
        trace_context_headers=list(snapshot.trace_context_headers),
        correlation_headers=list(snapshot.correlation_headers),
        shared_cross_process_headers=list(snapshot.shared_cross_process_headers),
        expected_correlation_fields=list(snapshot.expected_correlation_fields),
        expected_correlation_fields_ready=snapshot.expected_correlation_fields_ready,
        summary=GQLObservabilityConvergenceSummary(
            pipeline_stage_count=len(snapshot.pipeline_stages),
            ready_stage_count=ready_stage_count,
            production_evidence_ready=bool(proof_artifacts),
            grpc_rust_trace_ready=bool(
                snapshot.rust_trace_correlation_enabled
                and snapshot.expected_correlation_fields_ready
            ),
            otlp_export_ready=bool(snapshot.otel_enabled and snapshot.otel_endpoint_configured),
            search_index_ready=bool(
                snapshot.log_shipper_enabled
                and snapshot.log_shipper_target_configured
                and snapshot.search_backend != "none"
            ),
            alert_rollout_ready=bool(snapshot.alerting_enabled and proof_artifacts),
        ),
        missing_expected_correlation_fields=list(snapshot.missing_expected_correlation_fields),
        grpc_bind_address=snapshot.grpc_bind_address,
        grpc_service_name=snapshot.grpc_service_name,
        otlp_endpoint=snapshot.otlp_endpoint,
        log_shipper_target=snapshot.log_shipper_target,
        proof_artifacts=proof_artifacts,
        pipeline_stages=[
            GQLObservabilityPipelineStage(
                name=stage.name,
                status=stage.status,
                configured=stage.configured,
                ready=stage.ready,
                required_actions=list(stage.required_actions),
                remaining_gaps=list(stage.remaining_gaps),
            )
            for stage in snapshot.pipeline_stages
        ],
    )


def _build_observability_rollout(info: Info[GraphQLContext, object]) -> GQLObservabilityRolloutSummary:
    summary = build_observability_rollout_summary(info.context.resources.settings)
    return GQLObservabilityRolloutSummary(
        generated_at=summary.generated_at,
        status=summary.status,
        pipeline_stage_count=summary.pipeline_stage_count,
        ready_stage_count=summary.ready_stage_count,
        production_evidence_count=summary.production_evidence_count,
        production_evidence_ready=summary.production_evidence_ready,
        grpc_rust_trace_ready=summary.grpc_rust_trace_ready,
        otlp_export_ready=summary.otlp_export_ready,
        search_index_ready=summary.search_index_ready,
        alert_rollout_ready=summary.alert_rollout_ready,
        ready_stage_names=list(summary.ready_stage_names),
        blocked_stage_names=list(summary.blocked_stage_names),
        required_actions=list(summary.required_actions),
        remaining_gaps=list(summary.remaining_gaps),
    )


def _build_control_plane_summary(snapshot: object) -> GQLControlPlaneSummary:
    typed_snapshot: Any = snapshot
    return GQLControlPlaneSummary(
        total_subscribers=int(typed_snapshot.total_subscribers),
        active_subscribers=int(typed_snapshot.active_subscribers),
        stale_subscribers=int(typed_snapshot.stale_subscribers),
        error_subscribers=int(typed_snapshot.error_subscribers),
        fenced_subscribers=int(typed_snapshot.fenced_subscribers),
        ack_pending_subscribers=int(typed_snapshot.ack_pending_subscribers),
        stream_count=int(typed_snapshot.stream_count),
        group_count=int(typed_snapshot.group_count),
        node_count=int(typed_snapshot.node_count),
        tenant_count=int(typed_snapshot.tenant_count),
        oldest_heartbeat_age_seconds=typed_snapshot.oldest_heartbeat_age_seconds,
        status_counts=[
            GQLControlPlaneStatusCount(status=status, count=count)
            for status, count in dict(typed_snapshot.status_counts).items()
        ],
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_plugin_integration_readiness(snapshot: object) -> GQLPluginIntegrationReadiness:
    typed_snapshot: Any = snapshot
    plugins = list(typed_snapshot.plugins)
    return GQLPluginIntegrationReadiness(
        generated_at=str(typed_snapshot.generated_at),
        status=str(typed_snapshot.status),
        summary=GQLPluginIntegrationReadinessSummary(
            total_plugins=len(plugins),
            enabled_plugins=sum(1 for row in plugins if bool(row.enabled)),
            configured_plugins=sum(1 for row in plugins if bool(row.configured)),
            contract_validated_plugins=sum(
                1 for row in plugins if bool(row.contract_validated)
            ),
            soak_validated_plugins=sum(1 for row in plugins if bool(row.soak_validated)),
            ready_plugins=sum(1 for row in plugins if bool(row.ready)),
            missing_contract_proof_plugins=sum(
                1 for row in plugins if not bool(row.contract_validated)
            ),
            missing_soak_proof_plugins=sum(
                1 for row in plugins if not bool(row.soak_validated)
            ),
        ),
        plugins=[
            GQLPluginIntegrationReadinessPlugin(
                name=str(row.name),
                capability_kind=str(row.capability_kind),
                status=str(row.status),
                registered=bool(row.registered),
                enabled=bool(row.enabled),
                configured=bool(row.configured),
                ready=bool(row.ready),
                endpoint=row.endpoint,
                endpoint_configured=bool(row.endpoint_configured),
                config_source=row.config_source,
                required_settings=list(row.required_settings),
                missing_settings=list(row.missing_settings),
                contract_proof_refs=list(row.contract_proof_refs),
                soak_proof_refs=list(row.soak_proof_refs),
                contract_proofs=[
                    GQLProofArtifact(
                        ref=proof.ref,
                        category=proof.category,
                        label=proof.label,
                        recorded=bool(proof.recorded),
                    )
                    for proof in row.contract_proofs
                ],
                soak_proofs=[
                    GQLProofArtifact(
                        ref=proof.ref,
                        category=proof.category,
                        label=proof.label,
                        recorded=bool(proof.recorded),
                    )
                    for proof in row.soak_proofs
                ],
                contract_validated=bool(row.contract_validated),
                soak_validated=bool(row.soak_validated),
                proof_gap_count=int(row.proof_gap_count),
                required_actions=list(row.required_actions),
                remaining_gaps=list(row.remaining_gaps),
            )
            for row in plugins
        ],
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_control_plane_automation(snapshot: object) -> GQLControlPlaneAutomation:
    typed_snapshot: Any = snapshot
    return GQLControlPlaneAutomation(
        generated_at=str(typed_snapshot.generated_at),
        enabled=bool(typed_snapshot.enabled),
        runner_status=str(typed_snapshot.runner_status),
        interval_seconds=int(typed_snapshot.interval_seconds),
        active_within_seconds=int(typed_snapshot.active_within_seconds),
        pending_min_idle_ms=int(typed_snapshot.pending_min_idle_ms),
        claim_limit=int(typed_snapshot.claim_limit),
        max_claim_passes=int(typed_snapshot.max_claim_passes),
        consumer_group=str(typed_snapshot.consumer_group),
        consumer_name=str(typed_snapshot.consumer_name),
        service_attached=bool(typed_snapshot.service_attached),
        backplane_attached=bool(typed_snapshot.backplane_attached),
        last_run_at=typed_snapshot.last_run_at,
        last_success_at=typed_snapshot.last_success_at,
        last_failure_at=typed_snapshot.last_failure_at,
        consecutive_failures=int(typed_snapshot.consecutive_failures),
        last_error=typed_snapshot.last_error,
        remediation_updated_subscribers=int(typed_snapshot.remediation_updated_subscribers),
        rewound_subscribers=int(typed_snapshot.rewound_subscribers),
        claimed_pending_events=int(typed_snapshot.claimed_pending_events),
        claim_passes=int(typed_snapshot.claim_passes),
        pending_count_after=typed_snapshot.pending_count_after,
        summary=_build_control_plane_summary(typed_snapshot.summary),
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_control_plane_subscriber(snapshot: object) -> GQLControlPlaneSubscriber:
    typed_snapshot: Any = snapshot
    return GQLControlPlaneSubscriber(
        stream_name=str(typed_snapshot.stream_name),
        group_name=str(typed_snapshot.group_name),
        consumer_name=str(typed_snapshot.consumer_name),
        node_id=str(typed_snapshot.node_id),
        tenant_id=typed_snapshot.tenant_id,
        status=str(typed_snapshot.status),
        last_read_offset=typed_snapshot.last_read_offset,
        last_delivered_event_id=typed_snapshot.last_delivered_event_id,
        last_acked_event_id=typed_snapshot.last_acked_event_id,
        ack_pending=bool(typed_snapshot.ack_pending),
        fenced=bool(typed_snapshot.fenced),
        last_error=typed_snapshot.last_error,
        claimed_at=str(typed_snapshot.claimed_at),
        last_heartbeat_at=str(typed_snapshot.last_heartbeat_at),
        created_at=str(typed_snapshot.created_at),
        updated_at=str(typed_snapshot.updated_at),
    )


def _build_control_plane_replay_backplane(snapshot: object) -> GQLControlPlaneReplayBackplane:
    typed_snapshot: Any = snapshot
    return GQLControlPlaneReplayBackplane(
        generated_at=str(typed_snapshot.generated_at),
        status=str(typed_snapshot.status),
        event_backplane=str(typed_snapshot.event_backplane),
        stream_name=str(typed_snapshot.stream_name),
        consumer_group=str(typed_snapshot.consumer_group),
        replay_maxlen=int(typed_snapshot.replay_maxlen),
        claim_limit=int(typed_snapshot.claim_limit),
        max_claim_passes=int(typed_snapshot.max_claim_passes),
        attached=bool(typed_snapshot.attached),
        pending_count=int(typed_snapshot.pending_count),
        oldest_event_id=typed_snapshot.oldest_event_id,
        latest_event_id=typed_snapshot.latest_event_id,
        consumer_counts=_build_named_count_buckets(dict(typed_snapshot.consumer_counts)),
        consumer_count=int(typed_snapshot.consumer_count),
        has_pending_backlog=bool(typed_snapshot.has_pending_backlog),
        proof_refs=list(typed_snapshot.proof_refs),
        proof_artifacts=[
            GQLProofArtifact(
                ref=proof.ref,
                category=proof.category,
                label=proof.label,
                recorded=bool(proof.recorded),
            )
            for proof in typed_snapshot.proof_artifacts
        ],
        proof_ready=bool(typed_snapshot.proof_ready),
        pending_recovery_ready=bool(typed_snapshot.pending_recovery_ready),
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_control_plane_recovery_readiness(
    snapshot: object,
) -> GQLControlPlaneRecoveryReadiness:
    typed_snapshot: Any = snapshot
    return GQLControlPlaneRecoveryReadiness(
        generated_at=str(typed_snapshot.generated_at),
        status=str(typed_snapshot.status),
        active_within_seconds=int(typed_snapshot.active_within_seconds),
        stale_subscribers=int(typed_snapshot.stale_subscribers),
        ack_pending_subscribers=int(typed_snapshot.ack_pending_subscribers),
        pending_count=int(typed_snapshot.pending_count),
        consumer_count=int(typed_snapshot.consumer_count),
        automation_enabled=bool(typed_snapshot.automation_enabled),
        automation_healthy=bool(typed_snapshot.automation_healthy),
        replay_attached=bool(typed_snapshot.replay_attached),
        proof_refs=list(typed_snapshot.proof_refs),
        proof_artifacts=[
            GQLProofArtifact(
                ref=proof.ref,
                category=proof.category,
                label=proof.label,
                recorded=bool(proof.recorded),
            )
            for proof in typed_snapshot.proof_artifacts
        ],
        proof_ready=bool(typed_snapshot.proof_ready),
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_control_plane_remediation(snapshot: object) -> GQLControlPlaneRemediation:
    typed_snapshot: Any = snapshot
    return GQLControlPlaneRemediation(
        generated_at=str(typed_snapshot.generated_at),
        active_within_seconds=int(typed_snapshot.active_within_seconds),
        stale_marked_subscribers=int(typed_snapshot.stale_marked_subscribers),
        fence_resolved_subscribers=int(typed_snapshot.fence_resolved_subscribers),
        error_recovered_subscribers=int(typed_snapshot.error_recovered_subscribers),
        total_updated_subscribers=int(typed_snapshot.total_updated_subscribers),
        summary=_build_control_plane_summary(typed_snapshot.summary),
    )


def _build_control_plane_ack_recovery(snapshot: object) -> GQLControlPlaneAckRecovery:
    typed_snapshot: Any = snapshot
    return GQLControlPlaneAckRecovery(
        generated_at=str(typed_snapshot.generated_at),
        active_within_seconds=int(typed_snapshot.active_within_seconds),
        rewound_subscribers=int(typed_snapshot.rewound_subscribers),
        stale_marked_subscribers=int(typed_snapshot.stale_marked_subscribers),
        pending_without_ack_subscribers=int(typed_snapshot.pending_without_ack_subscribers),
        total_updated_subscribers=int(typed_snapshot.total_updated_subscribers),
        summary=_build_control_plane_summary(typed_snapshot.summary),
    )


def _build_control_plane_pending_recovery(snapshot: object) -> GQLControlPlanePendingRecovery:
    typed_snapshot: Any = snapshot
    return GQLControlPlanePendingRecovery(
        generated_at=str(typed_snapshot.generated_at),
        group_name=str(typed_snapshot.group_name),
        consumer_name=str(typed_snapshot.consumer_name),
        min_idle_ms=int(typed_snapshot.min_idle_ms),
        claim_limit=int(typed_snapshot.claim_limit),
        claimed_count=int(typed_snapshot.claimed_count),
        claimed_event_ids=list(typed_snapshot.claimed_event_ids),
        next_start_id=str(typed_snapshot.next_start_id),
        pending_count_before=int(typed_snapshot.pending_count_before),
        pending_count_after=int(typed_snapshot.pending_count_after),
        oldest_pending_event_id=typed_snapshot.oldest_pending_event_id,
        latest_pending_event_id=typed_snapshot.latest_pending_event_id,
        pending_consumer_counts=_build_named_count_buckets(
            dict(typed_snapshot.pending_consumer_counts)
        ),
        summary=_build_control_plane_summary(typed_snapshot.summary),
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_named_count_buckets(counts: dict[str, int]) -> list[GQLNamedCountBucket]:
    return [
        GQLNamedCountBucket(key=key, count=count)
        for key, count in sorted(counts.items())
    ]


def _build_observability_field_contract_summary(
    snapshot: object,
) -> GQLObservabilityFieldContractSummary:
    typed_snapshot: Any = snapshot
    return GQLObservabilityFieldContractSummary(
        total_required_correlation_fields=int(typed_snapshot.total_required_correlation_fields),
        expected_field_count=int(typed_snapshot.expected_field_count),
        configured_expected_field_count=int(typed_snapshot.configured_expected_field_count),
        missing_expected_field_count=int(typed_snapshot.missing_expected_field_count),
        trace_context_header_count=int(typed_snapshot.trace_context_header_count),
        correlation_header_count=int(typed_snapshot.correlation_header_count),
        shared_header_count=int(typed_snapshot.shared_header_count),
    )


def _build_proof_artifact_rows(rows: list[object]) -> list[GQLProofArtifact]:
    return [
        GQLProofArtifact(
            ref=str(cast(Any, row).ref),
            category=str(cast(Any, row).category),
            label=str(cast(Any, row).label),
            recorded=bool(cast(Any, row).recorded),
        )
        for row in rows
    ]


def _build_governance_evidence_check(snapshot: object) -> GQLGovernanceEvidenceCheck:
    typed_snapshot: Any = snapshot
    return GQLGovernanceEvidenceCheck(
        key=str(typed_snapshot.key),
        label=str(typed_snapshot.label),
        status=str(typed_snapshot.status),
        recorded=bool(typed_snapshot.recorded),
        ready=bool(typed_snapshot.ready),
        evidence_refs=list(typed_snapshot.evidence_refs),
        proof_artifacts=_build_proof_artifact_rows(list(typed_snapshot.proof_artifacts)),
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_enterprise_rollout_evidence(snapshot: object) -> GQLEnterpriseRolloutEvidence:
    typed_snapshot: Any = snapshot
    return GQLEnterpriseRolloutEvidence(
        generated_at=str(typed_snapshot.generated_at),
        status=str(typed_snapshot.status),
        total_check_count=int(typed_snapshot.total_check_count),
        ready_check_count=int(typed_snapshot.ready_check_count),
        checks=[
            _build_governance_evidence_check(row)
            for row in typed_snapshot.checks
        ],
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_playback_gate_governance(snapshot: object) -> GQLPlaybackGateGovernance:
    typed_snapshot: Any = snapshot
    return GQLPlaybackGateGovernance(
        generated_at=str(typed_snapshot.generated_at),
        status=str(typed_snapshot.status),
        rollout_readiness=str(typed_snapshot.rollout_readiness),
        next_action=str(typed_snapshot.next_action),
        reasons=list(typed_snapshot.reasons),
        environment_class=str(typed_snapshot.environment_class),
        gate_mode=str(typed_snapshot.gate_mode),
        runner_status=str(typed_snapshot.runner_status),
        runner_ready=bool(typed_snapshot.runner_ready),
        runner_required_failures=int(typed_snapshot.runner_required_failures),
        provider_gate_required=bool(typed_snapshot.provider_gate_required),
        provider_gate_ran=bool(typed_snapshot.provider_gate_ran),
        provider_parity_ready=bool(typed_snapshot.provider_parity_ready),
        windows_provider_ready=bool(typed_snapshot.windows_provider_ready),
        windows_provider_movie_ready=bool(typed_snapshot.windows_provider_movie_ready),
        windows_provider_tv_ready=bool(typed_snapshot.windows_provider_tv_ready),
        windows_provider_coverage=list(typed_snapshot.windows_provider_coverage),
        windows_soak_ready=bool(typed_snapshot.windows_soak_ready),
        windows_soak_repeat_count=int(typed_snapshot.windows_soak_repeat_count),
        windows_soak_profile_coverage_complete=bool(
            typed_snapshot.windows_soak_profile_coverage_complete
        ),
        windows_soak_profile_coverage=list(typed_snapshot.windows_soak_profile_coverage),
        policy_validation_status=str(typed_snapshot.policy_validation_status),
        policy_ready=bool(typed_snapshot.policy_ready),
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_governance_status_count(snapshot: object) -> GQLGovernanceStatusCount:
    typed_snapshot: Any = snapshot
    return GQLGovernanceStatusCount(
        status=str(typed_snapshot.status),
        count=int(typed_snapshot.count),
    )


def _build_governance_artifact_inventory_item(
    snapshot: object,
) -> GQLGovernanceArtifactInventoryItem:
    typed_snapshot: Any = snapshot
    return GQLGovernanceArtifactInventoryItem(
        check_key=str(typed_snapshot.check_key),
        check_label=str(typed_snapshot.check_label),
        ref=str(typed_snapshot.ref),
        category=str(typed_snapshot.category),
        label=str(typed_snapshot.label),
        recorded=bool(typed_snapshot.recorded),
    )


def _build_operator_action_item(snapshot: object) -> GQLOperatorActionItem:
    typed_snapshot: Any = snapshot
    return GQLOperatorActionItem(
        domain=str(typed_snapshot.domain),
        subject=str(typed_snapshot.subject),
        severity=str(typed_snapshot.severity),
        status=str(typed_snapshot.status),
        action=str(typed_snapshot.action),
        capability_kind=typed_snapshot.capability_kind,
    )


def _build_operator_gap_item(snapshot: object) -> GQLOperatorGapItem:
    typed_snapshot: Any = snapshot
    return GQLOperatorGapItem(
        domain=str(typed_snapshot.domain),
        subject=str(typed_snapshot.subject),
        severity=str(typed_snapshot.severity),
        status=str(typed_snapshot.status),
        message=str(typed_snapshot.message),
        capability_kind=typed_snapshot.capability_kind,
    )


def _build_control_plane_consumer_summary(snapshot: object) -> GQLControlPlaneConsumerSummary:
    typed_snapshot: Any = snapshot
    return GQLControlPlaneConsumerSummary(
        consumer_name=str(typed_snapshot.consumer_name),
        subscriber_count=int(typed_snapshot.subscriber_count),
        active_subscribers=int(typed_snapshot.active_subscribers),
        ack_pending_subscribers=int(typed_snapshot.ack_pending_subscribers),
        fenced_subscribers=int(typed_snapshot.fenced_subscribers),
        error_subscribers=int(typed_snapshot.error_subscribers),
        latest_heartbeat_at=typed_snapshot.latest_heartbeat_at,
    )


def _build_control_plane_ownership_summary(snapshot: object) -> GQLControlPlaneOwnershipSummary:
    typed_snapshot: Any = snapshot
    return GQLControlPlaneOwnershipSummary(
        total_subscribers=int(typed_snapshot.total_subscribers),
        active_subscribers=int(typed_snapshot.active_subscribers),
        stale_subscribers=int(typed_snapshot.stale_subscribers),
        error_subscribers=int(typed_snapshot.error_subscribers),
        fenced_subscribers=int(typed_snapshot.fenced_subscribers),
        ack_pending_subscribers=int(typed_snapshot.ack_pending_subscribers),
        unique_consumers=int(typed_snapshot.unique_consumers),
        unique_nodes=int(typed_snapshot.unique_nodes),
        unique_tenants=int(typed_snapshot.unique_tenants),
    )


def _build_vfs_runtime_rollout(snapshot: object) -> GQLVfsRuntimeRollout:
    typed_snapshot: Any = snapshot
    return GQLVfsRuntimeRollout(
        generated_at=str(typed_snapshot.generated_at),
        status=str(typed_snapshot.status),
        rollout_readiness=str(typed_snapshot.rollout_readiness),
        next_action=str(typed_snapshot.next_action),
        canary_decision=str(typed_snapshot.canary_decision),
        merge_gate=str(typed_snapshot.merge_gate),
        environment_class=str(typed_snapshot.environment_class),
        snapshot_available=bool(typed_snapshot.snapshot_available),
        open_handles=int(typed_snapshot.open_handles),
        active_reads=int(typed_snapshot.active_reads),
        cache_pressure_class=str(typed_snapshot.cache_pressure_class),
        refresh_pressure_class=str(typed_snapshot.refresh_pressure_class),
        provider_pressure_incidents=int(typed_snapshot.provider_pressure_incidents),
        fairness_pressure_incidents=int(typed_snapshot.fairness_pressure_incidents),
        reasons=list(typed_snapshot.reasons),
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_vfs_runtime_percentiles(snapshot: object) -> GQLVfsRuntimePercentiles:
    typed_snapshot: Any = snapshot
    return GQLVfsRuntimePercentiles(
        p50_ms=float(typed_snapshot.p50_ms),
        p95_ms=float(typed_snapshot.p95_ms),
        p99_ms=float(typed_snapshot.p99_ms),
        max_ms=float(typed_snapshot.max_ms),
    )


def _build_vfs_runtime_rust_handle_rollup(snapshot: object) -> GQLVfsRuntimeRustHandleRollup:
    typed_snapshot: Any = snapshot
    return GQLVfsRuntimeRustHandleRollup(
        tenant_id=str(typed_snapshot.tenant_id),
        session_id=str(typed_snapshot.session_id),
        open_handles=int(typed_snapshot.open_handles),
        invalidated_handles=int(typed_snapshot.invalidated_handles),
        average_depth=float(typed_snapshot.average_depth),
        max_depth=int(typed_snapshot.max_depth),
        average_age_ms=float(typed_snapshot.average_age_ms),
        max_age_ms=float(typed_snapshot.max_age_ms),
    )


def _build_vfs_runtime_python_session_rollup(
    snapshot: object,
) -> GQLVfsRuntimePythonSessionRollup:
    typed_snapshot: Any = snapshot
    return GQLVfsRuntimePythonSessionRollup(
        owner=str(typed_snapshot.owner),
        session_id=str(typed_snapshot.session_id),
        resource=str(typed_snapshot.resource),
        open_handles=int(typed_snapshot.open_handles),
        read_operations=int(typed_snapshot.read_operations),
        bytes_served=int(typed_snapshot.bytes_served),
        average_age_ms=float(typed_snapshot.average_age_ms),
        p95_age_ms=float(typed_snapshot.p95_age_ms),
        average_depth=float(typed_snapshot.average_depth),
        max_depth=int(typed_snapshot.max_depth),
        bytes_per_read=float(typed_snapshot.bytes_per_read),
    )


def _build_vfs_runtime_read_amplification(
    snapshot: object,
) -> GQLVfsRuntimeReadAmplification:
    typed_snapshot: Any = snapshot
    return GQLVfsRuntimeReadAmplification(
        view=str(typed_snapshot.view),
        total_operations=int(typed_snapshot.total_operations),
        total_bytes=int(typed_snapshot.total_bytes),
        bytes_per_read=float(typed_snapshot.bytes_per_read),
    )


def _build_vfs_runtime_telemetry(snapshot: object) -> GQLVfsRuntimeTelemetry:
    typed_snapshot: Any = snapshot
    bucket_rows = [
        GQLNamedCountBucket(key=key, count=int(count))
        for key, count in sorted(dict(typed_snapshot.mounted_read_duration_buckets).items())
    ]
    return GQLVfsRuntimeTelemetry(
        generated_at=str(typed_snapshot.generated_at),
        status=str(typed_snapshot.status),
        rust_snapshot_available=bool(typed_snapshot.rust_snapshot_available),
        python_active_session_count=int(typed_snapshot.python_active_session_count),
        python_active_handle_count=int(typed_snapshot.python_active_handle_count),
        rust_handle_age_ms=_build_vfs_runtime_percentiles(typed_snapshot.rust_handle_age_ms),
        python_handle_age_ms=_build_vfs_runtime_percentiles(typed_snapshot.python_handle_age_ms),
        mounted_read_duration_buckets=bucket_rows,
        rust_handle_depth_rollups=[
            _build_vfs_runtime_rust_handle_rollup(row)
            for row in list(typed_snapshot.rust_handle_depth_rollups)
        ],
        python_session_rollups=[
            _build_vfs_runtime_python_session_rollup(row)
            for row in list(typed_snapshot.python_session_rollups)
        ],
        read_amplification=[
            _build_vfs_runtime_read_amplification(row)
            for row in list(typed_snapshot.read_amplification)
        ],
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_vfs_rollout_ledger_entry(snapshot: object) -> GQLVfsRolloutLedgerEntry:
    typed_snapshot: Any = snapshot
    return GQLVfsRolloutLedgerEntry(
        entry_id=str(typed_snapshot.entry_id),
        recorded_at=str(typed_snapshot.recorded_at),
        actor_id=typed_snapshot.actor_id,
        action=str(typed_snapshot.action),
        summary=str(typed_snapshot.summary),
        environment_class=str(typed_snapshot.environment_class),
        runtime_status_path=typed_snapshot.runtime_status_path,
        promotion_paused=bool(typed_snapshot.promotion_paused),
        promotion_pause_reason=typed_snapshot.promotion_pause_reason,
        promotion_pause_expires_at=typed_snapshot.promotion_pause_expires_at,
        promotion_pause_active=bool(typed_snapshot.promotion_pause_active),
        rollback_requested=bool(typed_snapshot.rollback_requested),
        rollback_reason=typed_snapshot.rollback_reason,
        rollback_expires_at=typed_snapshot.rollback_expires_at,
        rollback_active=bool(typed_snapshot.rollback_active),
        notes=typed_snapshot.notes,
    )


def _build_vfs_rollout_control(snapshot: object) -> GQLVfsRolloutControl:
    typed_snapshot: Any = snapshot
    return GQLVfsRolloutControl(
        generated_at=str(typed_snapshot.generated_at),
        environment_class=str(typed_snapshot.environment_class),
        runtime_status_path=typed_snapshot.runtime_status_path,
        promotion_paused=bool(typed_snapshot.promotion_paused),
        promotion_pause_reason=typed_snapshot.promotion_pause_reason,
        promotion_pause_expires_at=typed_snapshot.promotion_pause_expires_at,
        promotion_pause_active=bool(typed_snapshot.promotion_pause_active),
        rollback_requested=bool(typed_snapshot.rollback_requested),
        rollback_reason=typed_snapshot.rollback_reason,
        rollback_expires_at=typed_snapshot.rollback_expires_at,
        rollback_active=bool(typed_snapshot.rollback_active),
        notes=typed_snapshot.notes,
        updated_at=typed_snapshot.updated_at,
        updated_by=typed_snapshot.updated_by,
        rollout_readiness=str(typed_snapshot.rollout_readiness),
        next_action=str(typed_snapshot.next_action),
        canary_decision=str(typed_snapshot.canary_decision),
        merge_gate=str(typed_snapshot.merge_gate),
        reasons=list(typed_snapshot.reasons),
        history=[
            _build_vfs_rollout_ledger_entry(entry)
            for entry in list(getattr(typed_snapshot, "history", []) or [])
        ],
    )


def _build_plugin_runtime_overview(snapshot: object) -> GQLPluginRuntimeOverview:
    typed_snapshot: Any = snapshot
    return GQLPluginRuntimeOverview(
        generated_at=str(typed_snapshot.generated_at),
        status=str(typed_snapshot.status),
        total_plugins=int(typed_snapshot.total_plugins),
        ready_plugins=int(typed_snapshot.ready_plugins),
        load_failed_plugins=int(typed_snapshot.load_failed_plugins),
        wiring_ready_plugins=int(typed_snapshot.wiring_ready_plugins),
        contract_validated_plugins=int(typed_snapshot.contract_validated_plugins),
        soak_validated_plugins=int(typed_snapshot.soak_validated_plugins),
        quarantined_plugins=int(typed_snapshot.quarantined_plugins),
        publishable_event_count=int(typed_snapshot.publishable_event_count),
        hook_subscription_count=int(typed_snapshot.hook_subscription_count),
        warning_count=int(typed_snapshot.warning_count),
        recommended_actions=list(typed_snapshot.recommended_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_plugin_runtime_warning(snapshot: object) -> GQLPluginRuntimeWarning:
    typed_snapshot: Any = snapshot
    return GQLPluginRuntimeWarning(
        plugin_name=str(typed_snapshot.plugin_name),
        source=str(typed_snapshot.source),
        severity=str(typed_snapshot.severity),
        status=str(typed_snapshot.status),
        message=str(typed_snapshot.message),
        capability_kind=typed_snapshot.capability_kind,
    )


def _build_plugin_runtime_row(snapshot: object) -> GQLPluginRuntimeRow:
    typed_snapshot: Any = snapshot
    return GQLPluginRuntimeRow(
        name=str(typed_snapshot.name),
        status=str(typed_snapshot.status),
        ready=bool(typed_snapshot.ready),
        capability_kinds=list(typed_snapshot.capability_kinds),
        wiring_status=str(typed_snapshot.wiring_status),
        publishable_event_count=int(typed_snapshot.publishable_event_count),
        hook_subscription_count=int(typed_snapshot.hook_subscription_count),
        contract_validated=bool(typed_snapshot.contract_validated),
        soak_validated=bool(typed_snapshot.soak_validated),
        proof_gap_count=int(typed_snapshot.proof_gap_count),
        warning_count=int(typed_snapshot.warning_count),
        quarantined=bool(typed_snapshot.quarantined),
        recommended_actions=list(typed_snapshot.recommended_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_plugin_runtime_capability_summary(
    snapshot: object,
) -> GQLPluginRuntimeCapabilitySummary:
    typed_snapshot: Any = snapshot
    return GQLPluginRuntimeCapabilitySummary(
        capability_kind=str(typed_snapshot.capability_kind),
        total_plugins=int(typed_snapshot.total_plugins),
        ready_plugins=int(typed_snapshot.ready_plugins),
        blocked_plugins=int(typed_snapshot.blocked_plugins),
        warning_count=int(typed_snapshot.warning_count),
        contract_validated_plugins=int(typed_snapshot.contract_validated_plugins),
        soak_validated_plugins=int(typed_snapshot.soak_validated_plugins),
    )


def _build_plugin_proof_coverage_summary(snapshot: object) -> GQLPluginProofCoverageSummary:
    typed_snapshot: Any = snapshot
    return GQLPluginProofCoverageSummary(
        capability_kind=str(typed_snapshot.capability_kind),
        total_plugins=int(typed_snapshot.total_plugins),
        contract_validated_plugins=int(typed_snapshot.contract_validated_plugins),
        soak_validated_plugins=int(typed_snapshot.soak_validated_plugins),
        missing_contract_plugins=int(typed_snapshot.missing_contract_plugins),
        missing_soak_plugins=int(typed_snapshot.missing_soak_plugins),
    )


def _build_plugin_runtime_publisher_summary(snapshot: object) -> GQLPluginRuntimePublisherSummary:
    typed_snapshot: Any = snapshot
    return GQLPluginRuntimePublisherSummary(
        publisher=str(typed_snapshot.publisher),
        plugin_count=int(typed_snapshot.plugin_count),
        ready_plugins=int(typed_snapshot.ready_plugins),
        quarantined_plugins=int(typed_snapshot.quarantined_plugins),
        warning_count=int(typed_snapshot.warning_count),
        capability_counts=_build_named_count_buckets(dict(typed_snapshot.capability_counts)),
    )


def _build_vfs_generation_history_point(snapshot: object) -> GQLVfsGenerationHistoryPoint:
    typed_snapshot: Any = snapshot
    return GQLVfsGenerationHistoryPoint(
        generation_id=str(typed_snapshot.generation_id),
        published_at=str(typed_snapshot.published_at),
        entry_count=int(typed_snapshot.entry_count),
        directory_count=int(typed_snapshot.directory_count),
        file_count=int(typed_snapshot.file_count),
        blocked_item_count=int(typed_snapshot.blocked_item_count),
        blocked_reason_counts=_build_named_count_buckets(
            dict(typed_snapshot.blocked_reason_counts)
        ),
        query_strategy_counts=_build_named_count_buckets(
            dict(typed_snapshot.query_strategy_counts)
        ),
        provider_family_counts=_build_named_count_buckets(
            dict(typed_snapshot.provider_family_counts)
        ),
        lease_state_counts=_build_named_count_buckets(dict(typed_snapshot.lease_state_counts)),
        delta_from_previous_available=bool(typed_snapshot.delta_from_previous_available),
        delta_upsert_count=int(typed_snapshot.delta_upsert_count),
        delta_removal_count=int(typed_snapshot.delta_removal_count),
        delta_upsert_file_count=int(typed_snapshot.delta_upsert_file_count),
        delta_removal_file_count=int(typed_snapshot.delta_removal_file_count),
    )


def _build_vfs_generation_history_summary(snapshot: object) -> GQLVfsGenerationHistorySummary:
    typed_snapshot: Any = snapshot
    return GQLVfsGenerationHistorySummary(
        generation_count=int(typed_snapshot.generation_count),
        newest_generation_id=typed_snapshot.newest_generation_id,
        oldest_generation_id=typed_snapshot.oldest_generation_id,
        max_entry_count=int(typed_snapshot.max_entry_count),
        max_file_count=int(typed_snapshot.max_file_count),
        blocked_generation_count=int(typed_snapshot.blocked_generation_count),
        total_delta_upsert_count=int(typed_snapshot.total_delta_upsert_count),
        total_delta_removal_count=int(typed_snapshot.total_delta_removal_count),
        provider_family_counts=_build_named_count_buckets(
            dict(typed_snapshot.provider_family_counts)
        ),
        lease_state_counts=_build_named_count_buckets(dict(typed_snapshot.lease_state_counts)),
    )


def _build_vfs_catalog_delta(delta: VfsCatalogDelta) -> GQLVfsCatalogDelta:
    summary = summarize_vfs_catalog_delta(delta)
    return GQLVfsCatalogDelta(
        generation_id=str(delta.generation_id),
        base_generation_id=(
            str(delta.base_generation_id) if delta.base_generation_id is not None else None
        ),
        published_at=delta.published_at.isoformat(),
        upsert_directory_count=summary.upsert_directory_count,
        upsert_file_count=summary.upsert_file_count,
        removal_directory_count=summary.removal_directory_count,
        removal_file_count=summary.removal_file_count,
        provider_family_counts=_build_named_count_buckets(dict(summary.provider_family_counts)),
        lease_state_counts=_build_named_count_buckets(dict(summary.lease_state_counts)),
    )


def _build_vfs_catalog_delta_history_summary(snapshot: object) -> GQLVfsCatalogDeltaHistorySummary:
    typed_snapshot: Any = snapshot
    return GQLVfsCatalogDeltaHistorySummary(
        delta_count=int(typed_snapshot.delta_count),
        max_upsert_count=int(typed_snapshot.max_upsert_count),
        max_removal_count=int(typed_snapshot.max_removal_count),
        total_upsert_count=int(typed_snapshot.total_upsert_count),
        total_removal_count=int(typed_snapshot.total_removal_count),
        total_upsert_file_count=int(typed_snapshot.total_upsert_file_count),
        total_removal_file_count=int(typed_snapshot.total_removal_file_count),
        provider_family_counts=_build_named_count_buckets(dict(typed_snapshot.provider_family_counts)),
        lease_state_counts=_build_named_count_buckets(dict(typed_snapshot.lease_state_counts)),
    )


def _build_downloader_orchestration(snapshot: object) -> GQLDownloaderOrchestration:
    typed_snapshot: Any = snapshot
    return GQLDownloaderOrchestration(
        generated_at=str(typed_snapshot.generated_at),
        selection_mode=str(typed_snapshot.selection_mode),
        selected_provider=typed_snapshot.selected_provider,
        selected_provider_source=typed_snapshot.selected_provider_source,
        enabled_provider_count=int(typed_snapshot.enabled_provider_count),
        configured_provider_count=int(typed_snapshot.configured_provider_count),
        builtin_enabled_provider_count=int(typed_snapshot.builtin_enabled_provider_count),
        plugin_enabled_provider_count=int(typed_snapshot.plugin_enabled_provider_count),
        multi_provider_enabled=bool(typed_snapshot.multi_provider_enabled),
        plugin_downloaders_registered=int(typed_snapshot.plugin_downloaders_registered),
        worker_plugin_dispatch_ready=bool(typed_snapshot.worker_plugin_dispatch_ready),
        ordered_failover_ready=bool(typed_snapshot.ordered_failover_ready),
        fanout_ready=bool(typed_snapshot.fanout_ready),
        multi_container_ready=bool(typed_snapshot.multi_container_ready),
        provider_priority_order=list(typed_snapshot.provider_priority_order),
        providers=[
            GQLDownloaderProviderCandidate(
                name=str(row.name),
                source=str(row.source),
                enabled=bool(row.enabled),
                configured=bool(row.configured),
                selected=bool(row.selected),
                priority=row.priority,
                capabilities=list(row.capabilities),
            )
            for row in typed_snapshot.providers
        ],
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_downloader_execution_evidence(snapshot: object) -> GQLDownloaderExecutionEvidence:
    typed_snapshot: Any = snapshot
    return GQLDownloaderExecutionEvidence(
        generated_at=str(typed_snapshot.generated_at),
        queue_name=str(typed_snapshot.queue_name),
        status=str(typed_snapshot.status),
        selection_mode=str(typed_snapshot.selection_mode),
        ordered_failover_ready=bool(typed_snapshot.ordered_failover_ready),
        fanout_ready=bool(typed_snapshot.fanout_ready),
        provider_counts=_build_named_count_buckets(dict(typed_snapshot.provider_counts)),
        failure_kind_counts=_build_named_count_buckets(
            dict(typed_snapshot.failure_kind_counts)
        ),
        dead_letter_reason_counts=_build_named_count_buckets(
            dict(typed_snapshot.dead_letter_reason_counts)
        ),
        history_summary=GQLWorkerQueueHistorySummary(
            point_count=int(typed_snapshot.history_summary.point_count),
            warning_point_count=int(typed_snapshot.history_summary.warning_point_count),
            critical_point_count=int(typed_snapshot.history_summary.critical_point_count),
            max_total_jobs=int(typed_snapshot.history_summary.max_total_jobs),
            max_ready_jobs=int(typed_snapshot.history_summary.max_ready_jobs),
            max_retry_jobs=int(typed_snapshot.history_summary.max_retry_jobs),
            max_dead_letter_jobs=int(typed_snapshot.history_summary.max_dead_letter_jobs),
            latest_alert_level=str(typed_snapshot.history_summary.latest_alert_level),
            dead_letter_reason_counts=_build_named_count_buckets(
                dict(typed_snapshot.history_summary.dead_letter_reason_counts)
            ),
        ),
        recent_dead_letters=[
            _build_downloader_dead_letter(row) for row in typed_snapshot.recent_dead_letters
        ],
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_downloader_dead_letter(snapshot: object) -> GQLDownloaderExecutionDeadLetter:
    typed_snapshot: Any = snapshot
    return GQLDownloaderExecutionDeadLetter(
        stage=str(typed_snapshot.stage),
        item_id=str(typed_snapshot.item_id),
        reason=str(typed_snapshot.reason),
        reason_code=str(typed_snapshot.reason_code),
        idempotency_key=str(typed_snapshot.idempotency_key),
        attempt=int(typed_snapshot.attempt),
        queued_at=str(typed_snapshot.queued_at),
        provider=typed_snapshot.provider,
        failure_kind=typed_snapshot.failure_kind,
        selected_stream_id=typed_snapshot.selected_stream_id,
        item_request_id=typed_snapshot.item_request_id,
        status_code=typed_snapshot.status_code,
        retry_after_seconds=typed_snapshot.retry_after_seconds,
    )

def _build_plugin_event_status(row: object) -> GQLPluginEventStatus:
    typed_row: Any = row
    return GQLPluginEventStatus(
        name=str(typed_row.name),
        publisher=typed_row.publisher,
        publishable_events=list(typed_row.publishable_events),
        hook_subscriptions=list(typed_row.hook_subscriptions),
        publishable_event_count=int(typed_row.publishable_event_count),
        hook_subscription_count=int(typed_row.hook_subscription_count),
        wiring_status=str(typed_row.wiring_status),
    )


def _build_plugin_capability_status(row: object) -> GQLPluginCapabilityStatus:
    typed_row: Any = row
    return GQLPluginCapabilityStatus(
        name=str(typed_row.name),
        capabilities=list(typed_row.capabilities),
        status=str(typed_row.status),
        ready=bool(typed_row.ready),
        configured=typed_row.configured,
        version=typed_row.version,
        api_version=typed_row.api_version,
        min_host_version=typed_row.min_host_version,
        max_host_version=typed_row.max_host_version,
        publisher=typed_row.publisher,
        release_channel=typed_row.release_channel,
        trust_level=typed_row.trust_level,
        permission_scopes=list(typed_row.permission_scopes),
        source_sha256=typed_row.source_sha256,
        signing_key_id=typed_row.signing_key_id,
        signature_present=bool(typed_row.signature_present),
        signature_verified=bool(typed_row.signature_verified),
        signature_verification_reason=typed_row.signature_verification_reason,
        trust_policy_decision=typed_row.trust_policy_decision,
        trust_store_source=typed_row.trust_store_source,
        sandbox_profile=typed_row.sandbox_profile,
        tenancy_mode=typed_row.tenancy_mode,
        quarantined=bool(typed_row.quarantined),
        quarantine_reason=typed_row.quarantine_reason,
        publisher_policy_decision=typed_row.publisher_policy_decision,
        publisher_policy_status=typed_row.publisher_policy_status,
        quarantine_recommended=bool(typed_row.quarantine_recommended),
        override_state=typed_row.override_state,
        override_reason=typed_row.override_reason,
        override_updated_at=typed_row.override_updated_at,
        source=typed_row.source,
        warnings=list(typed_row.warnings),
        error=typed_row.error,
    )


def _build_plugin_governance_summary(snapshot: object) -> GQLPluginGovernanceSummary:
    typed_snapshot: Any = snapshot
    return GQLPluginGovernanceSummary(
        total_plugins=int(typed_snapshot.total_plugins),
        loaded_plugins=int(typed_snapshot.loaded_plugins),
        load_failed_plugins=int(typed_snapshot.load_failed_plugins),
        ready_plugins=int(typed_snapshot.ready_plugins),
        unready_plugins=int(typed_snapshot.unready_plugins),
        healthy_plugins=int(typed_snapshot.healthy_plugins),
        degraded_plugins=int(typed_snapshot.degraded_plugins),
        non_builtin_plugins=int(typed_snapshot.non_builtin_plugins),
        isolated_non_builtin_plugins=int(typed_snapshot.isolated_non_builtin_plugins),
        quarantined_plugins=int(typed_snapshot.quarantined_plugins),
        quarantine_recommended_plugins=int(typed_snapshot.quarantine_recommended_plugins),
        unsigned_external_plugins=int(typed_snapshot.unsigned_external_plugins),
        unverified_signature_plugins=int(typed_snapshot.unverified_signature_plugins),
        publisher_policy_rejections=int(typed_snapshot.publisher_policy_rejections),
        trust_policy_rejections=int(typed_snapshot.trust_policy_rejections),
        scraper_plugins=int(typed_snapshot.scraper_plugins),
        downloader_plugins=int(typed_snapshot.downloader_plugins),
        content_service_plugins=int(typed_snapshot.content_service_plugins),
        event_hook_plugins=int(typed_snapshot.event_hook_plugins),
        override_count=int(typed_snapshot.override_count),
        approved_overrides=int(typed_snapshot.approved_overrides),
        quarantined_overrides=int(typed_snapshot.quarantined_overrides),
        revoked_overrides=int(typed_snapshot.revoked_overrides),
        sandbox_profile_counts=_build_named_count_buckets(
            dict(typed_snapshot.sandbox_profile_counts)
        ),
        tenancy_mode_counts=_build_named_count_buckets(dict(typed_snapshot.tenancy_mode_counts)),
        runtime_policy_mode=str(typed_snapshot.runtime_policy_mode),
        runtime_isolation_ready=bool(typed_snapshot.runtime_isolation_ready),
        recommended_actions=list(typed_snapshot.recommended_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_access_policy_revision(snapshot: object) -> GQLAccessPolicyRevision:
    typed_snapshot: Any = snapshot
    return GQLAccessPolicyRevision(
        version=str(typed_snapshot.version),
        source=str(typed_snapshot.source),
        approval_status=str(typed_snapshot.approval_status),
        proposed_by=typed_snapshot.proposed_by,
        approved_by=typed_snapshot.approved_by,
        approved_at=typed_snapshot.approved_at,
        approval_notes=typed_snapshot.approval_notes,
        is_active=bool(typed_snapshot.is_active),
        activated_at=str(typed_snapshot.activated_at),
        created_at=str(typed_snapshot.created_at),
        updated_at=str(typed_snapshot.updated_at),
        role_grants=cast(JSON, dict(typed_snapshot.role_grants)),
        principal_roles=cast(JSON, dict(typed_snapshot.principal_roles)),
        principal_scopes=cast(JSON, dict(typed_snapshot.principal_scopes)),
        principal_tenant_grants=cast(JSON, dict(typed_snapshot.principal_tenant_grants)),
        permission_constraints=cast(JSON, dict(typed_snapshot.permission_constraints)),
        audit_decisions=bool(typed_snapshot.audit_decisions),
        alerting_enabled=bool(typed_snapshot.alerting_enabled),
        repeated_denial_warning_threshold=int(
            typed_snapshot.repeated_denial_warning_threshold
        ),
        repeated_denial_critical_threshold=int(
            typed_snapshot.repeated_denial_critical_threshold
        ),
    )


def _build_access_policy_revision_list(snapshot: object) -> GQLAccessPolicyRevisionList:
    typed_snapshot: Any = snapshot
    return GQLAccessPolicyRevisionList(
        active_version=typed_snapshot.active_version,
        revisions=[
            _build_access_policy_revision(row) for row in cast(list[object], typed_snapshot.revisions)
        ],
    )


def _build_downloader_execution_trend_summary(
    snapshot: object,
) -> GQLDownloaderExecutionTrendSummary:
    typed_snapshot: Any = snapshot
    return GQLDownloaderExecutionTrendSummary(
        point_count=int(typed_snapshot.point_count),
        ok_point_count=int(typed_snapshot.ok_point_count),
        warning_point_count=int(typed_snapshot.warning_point_count),
        critical_point_count=int(typed_snapshot.critical_point_count),
        average_ready_jobs=float(typed_snapshot.average_ready_jobs),
        average_retry_jobs=float(typed_snapshot.average_retry_jobs),
        average_dead_letter_jobs=float(typed_snapshot.average_dead_letter_jobs),
        latest_alert_level=str(typed_snapshot.latest_alert_level),
    )


def _build_downloader_provider_summary(snapshot: object) -> GQLDownloaderProviderSummary:
    typed_snapshot: Any = snapshot
    return GQLDownloaderProviderSummary(
        provider=str(typed_snapshot.provider),
        sample_count=int(typed_snapshot.sample_count),
        failure_kind_counts=_build_named_count_buckets(dict(typed_snapshot.failure_kind_counts)),
        reason_code_counts=_build_named_count_buckets(dict(typed_snapshot.reason_code_counts)),
        status_code_counts=_build_named_count_buckets(dict(typed_snapshot.status_code_counts)),
        retry_after_hint_count=int(typed_snapshot.retry_after_hint_count),
    )


def _build_downloader_dead_letter_timeline_point(
    snapshot: object,
) -> GQLDownloaderDeadLetterTimelinePoint:
    typed_snapshot: Any = snapshot
    return GQLDownloaderDeadLetterTimelinePoint(
        bucket_at=str(typed_snapshot.bucket_at),
        sample_count=int(typed_snapshot.sample_count),
        provider_counts=_build_named_count_buckets(dict(typed_snapshot.provider_counts)),
        reason_code_counts=_build_named_count_buckets(dict(typed_snapshot.reason_code_counts)),
        failure_kind_counts=_build_named_count_buckets(dict(typed_snapshot.failure_kind_counts)),
    )


def _build_downloader_failure_kind_summary(snapshot: object) -> GQLDownloaderFailureKindSummary:
    typed_snapshot: Any = snapshot
    return GQLDownloaderFailureKindSummary(
        failure_kind=str(typed_snapshot.failure_kind),
        sample_count=int(typed_snapshot.sample_count),
        provider_counts=_build_named_count_buckets(dict(typed_snapshot.provider_counts)),
        reason_code_counts=_build_named_count_buckets(dict(typed_snapshot.reason_code_counts)),
    )


def _build_downloader_status_code_summary(snapshot: object) -> GQLDownloaderStatusCodeSummary:
    typed_snapshot: Any = snapshot
    return GQLDownloaderStatusCodeSummary(
        status_code=int(typed_snapshot.status_code),
        sample_count=int(typed_snapshot.sample_count),
        provider_counts=_build_named_count_buckets(dict(typed_snapshot.provider_counts)),
        reason_code_counts=_build_named_count_buckets(dict(typed_snapshot.reason_code_counts)),
    )


def _build_downloader_reason_summary(snapshot: object) -> GQLDownloaderReasonSummary:
    typed_snapshot: Any = snapshot
    return GQLDownloaderReasonSummary(
        reason_code=str(typed_snapshot.reason_code),
        sample_count=int(typed_snapshot.sample_count),
        provider_counts=_build_named_count_buckets(dict(typed_snapshot.provider_counts)),
        failure_kind_counts=_build_named_count_buckets(dict(typed_snapshot.failure_kind_counts)),
    )


def _build_plugin_governance(
    *,
    summary: object,
    plugins: list[object],
) -> GQLPluginGovernance:
    return GQLPluginGovernance(
        summary=_build_plugin_governance_summary(summary),
        plugins=[_build_plugin_capability_status(row) for row in plugins],
    )


def _build_plugin_governance_override(snapshot: object) -> GQLPluginGovernanceOverride:
    typed_snapshot: Any = snapshot
    return GQLPluginGovernanceOverride(
        plugin_name=str(typed_snapshot.plugin_name),
        state=str(typed_snapshot.state),
        reason=typed_snapshot.reason,
        notes=typed_snapshot.notes,
        updated_by=typed_snapshot.updated_by,
        created_at=str(typed_snapshot.created_at),
        updated_at=str(typed_snapshot.updated_at),
    )


def _hydrate_named_count_bucket(snapshot: object) -> GQLNamedCountBucket:
    return GQLNamedCountBucket(**cast(dict[str, Any], snapshot))


def _hydrate_access_policy_revision(snapshot: object) -> GQLAccessPolicyRevision:
    return GQLAccessPolicyRevision(**cast(dict[str, Any], snapshot))


def _hydrate_access_policy_revision_list(
    snapshot: object,
    *,
    limit: int,
) -> GQLAccessPolicyRevisionList:
    typed_snapshot = cast(dict[str, Any], snapshot)
    return GQLAccessPolicyRevisionList(
        active_version=cast(str | None, typed_snapshot.get("active_version")),
        revisions=[
            _hydrate_access_policy_revision(row)
            for row in cast(list[object], typed_snapshot.get("revisions", ()))[:limit]
        ],
    )


def _hydrate_plugin_governance_summary(snapshot: object) -> GQLPluginGovernanceSummary:
    typed_snapshot = dict(cast(dict[str, Any], snapshot))
    typed_snapshot["sandbox_profile_counts"] = [
        _hydrate_named_count_bucket(row)
        for row in cast(list[object], typed_snapshot.get("sandbox_profile_counts", ()))
    ]
    typed_snapshot["tenancy_mode_counts"] = [
        _hydrate_named_count_bucket(row)
        for row in cast(list[object], typed_snapshot.get("tenancy_mode_counts", ()))
    ]
    return GQLPluginGovernanceSummary(**typed_snapshot)


def _hydrate_plugin_governance_override(snapshot: object) -> GQLPluginGovernanceOverride:
    return GQLPluginGovernanceOverride(**cast(dict[str, Any], snapshot))


def _hydrate_plugin_capability_status(snapshot: object) -> GQLPluginCapabilityStatus:
    return GQLPluginCapabilityStatus(**cast(dict[str, Any], snapshot))


def _hydrate_plugin_governance(snapshot: object) -> GQLPluginGovernance:
    typed_snapshot = cast(dict[str, Any], snapshot)
    return GQLPluginGovernance(
        summary=_hydrate_plugin_governance_summary(typed_snapshot["summary"]),
        plugins=[
            _hydrate_plugin_capability_status(row)
            for row in cast(list[object], typed_snapshot.get("plugins", ()))
        ],
    )


def _build_enterprise_operations_slice(snapshot: object) -> GQLEnterpriseOperationsSlice:
    typed_snapshot: Any = snapshot
    return GQLEnterpriseOperationsSlice(
        name=str(typed_snapshot.name),
        status=str(typed_snapshot.status),
        evidence=list(typed_snapshot.evidence),
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_enterprise_operations_governance(
    snapshot: object,
) -> GQLEnterpriseOperationsGovernance:
    typed_snapshot: Any = snapshot
    return GQLEnterpriseOperationsGovernance(
        generated_at=str(typed_snapshot.generated_at),
        playback_gate=_build_enterprise_operations_slice(typed_snapshot.playback_gate),
        operational_evidence=_build_enterprise_operations_slice(
            typed_snapshot.operational_evidence
        ),
        identity_authz=_build_enterprise_operations_slice(typed_snapshot.identity_authz),
        tenant_boundary=_build_enterprise_operations_slice(typed_snapshot.tenant_boundary),
        vfs_data_plane=_build_enterprise_operations_slice(typed_snapshot.vfs_data_plane),
        distributed_control_plane=_build_enterprise_operations_slice(
            typed_snapshot.distributed_control_plane
        ),
        runtime_lifecycle=_build_enterprise_operations_slice(typed_snapshot.runtime_lifecycle),
        sre_program=_build_enterprise_operations_slice(typed_snapshot.sre_program),
        operator_log_pipeline=_build_enterprise_operations_slice(
            typed_snapshot.operator_log_pipeline
        ),
        plugin_runtime_isolation=_build_enterprise_operations_slice(
            typed_snapshot.plugin_runtime_isolation
        ),
        heavy_stage_workload_isolation=_build_enterprise_operations_slice(
            typed_snapshot.heavy_stage_workload_isolation
        ),
        release_metadata_performance=_build_enterprise_operations_slice(
            typed_snapshot.release_metadata_performance
        ),
    )


def _build_vfs_catalog_governance(snapshot: object) -> GQLVfsCatalogGovernance:
    typed_snapshot: Any = snapshot
    return GQLVfsCatalogGovernance(
        generated_at=str(typed_snapshot.generated_at),
        status=str(typed_snapshot.status),
        counters=_build_named_count_buckets(dict(typed_snapshot.counters)),
        summary=GQLVfsCatalogGovernanceSummary(
            active_watch_sessions=int(typed_snapshot.summary.active_watch_sessions),
            reconnect_requests=int(typed_snapshot.summary.reconnect_requests),
            reconnect_delta_served=int(typed_snapshot.summary.reconnect_delta_served),
            reconnect_snapshot_fallbacks=int(
                typed_snapshot.summary.reconnect_snapshot_fallbacks
            ),
            reconnect_failures=int(typed_snapshot.summary.reconnect_failures),
            snapshots_served=int(typed_snapshot.summary.snapshots_served),
            deltas_served=int(typed_snapshot.summary.deltas_served),
            heartbeats_served=int(typed_snapshot.summary.heartbeats_served),
            problem_events=int(typed_snapshot.summary.problem_events),
            request_stream_failures=int(typed_snapshot.summary.request_stream_failures),
            refresh_attempts=int(typed_snapshot.summary.refresh_attempts),
            refresh_succeeded=int(typed_snapshot.summary.refresh_succeeded),
            refresh_provider_failures=int(typed_snapshot.summary.refresh_provider_failures),
            refresh_validation_failures=int(
                typed_snapshot.summary.refresh_validation_failures
            ),
            inline_refresh_requests=int(typed_snapshot.summary.inline_refresh_requests),
            inline_refresh_succeeded=int(typed_snapshot.summary.inline_refresh_succeeded),
            inline_refresh_failed=int(typed_snapshot.summary.inline_refresh_failed),
        ),
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _build_vfs_mount_diagnostics(snapshot: object) -> GQLVfsMountDiagnostics:
    typed_snapshot: Any = snapshot
    return GQLVfsMountDiagnostics(
        generated_at=str(typed_snapshot.generated_at),
        status=str(typed_snapshot.status),
        supplier_attached=bool(typed_snapshot.supplier_attached),
        server_attached=bool(typed_snapshot.server_attached),
        current_generation_id=typed_snapshot.current_generation_id,
        current_published_at=typed_snapshot.current_published_at,
        history_generation_ids=list(typed_snapshot.history_generation_ids),
        history_generation_count=int(typed_snapshot.history_generation_count),
        delta_history_ready=bool(typed_snapshot.delta_history_ready),
        active_watch_sessions=int(typed_snapshot.active_watch_sessions),
        snapshots_served=int(typed_snapshot.snapshots_served),
        deltas_served=int(typed_snapshot.deltas_served),
        reconnect_delta_served=int(typed_snapshot.reconnect_delta_served),
        reconnect_snapshot_fallbacks=int(typed_snapshot.reconnect_snapshot_fallbacks),
        reconnect_failures=int(typed_snapshot.reconnect_failures),
        request_stream_failures=int(typed_snapshot.request_stream_failures),
        problem_events=int(typed_snapshot.problem_events),
        refresh_provider_failures=int(typed_snapshot.refresh_provider_failures),
        refresh_validation_failures=int(typed_snapshot.refresh_validation_failures),
        required_actions=list(typed_snapshot.required_actions),
        remaining_gaps=list(typed_snapshot.remaining_gaps),
    )


def _format_optional_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _build_vfs_correlation_keys(record: VfsCatalogEntry) -> GQLVfsCorrelationKeys:
    correlation = record.correlation
    return GQLVfsCorrelationKeys(
        item_id=correlation.item_id,
        media_entry_id=correlation.media_entry_id,
        source_attachment_id=correlation.source_attachment_id,
        provider=correlation.provider,
        provider_download_id=correlation.provider_download_id,
        provider_file_id=correlation.provider_file_id,
        provider_file_path=correlation.provider_file_path,
        session_id=correlation.session_id,
        handle_key=correlation.handle_key,
    )


def _build_vfs_catalog_entry(record: VfsCatalogEntry) -> GQLVfsCatalogEntry:
    file_payload = record.file
    directory_payload = record.directory
    return GQLVfsCatalogEntry(
        entry_id=record.entry_id,
        parent_entry_id=record.parent_entry_id,
        path=record.path,
        name=record.name,
        kind=record.kind,
        correlation=_build_vfs_correlation_keys(record),
        directory=(
            GQLVfsDirectoryDetail(path=directory_payload.path)
            if directory_payload is not None
            else None
        ),
        file=(
            GQLVfsFileDetail(
                item_id=file_payload.item_id,
                item_title=file_payload.item_title,
                item_external_ref=file_payload.item_external_ref,
                media_entry_id=file_payload.media_entry_id,
                source_attachment_id=file_payload.source_attachment_id,
                media_type=file_payload.media_type,
                transport=file_payload.transport,
                locator=file_payload.locator,
                local_path=file_payload.local_path,
                restricted_url=file_payload.restricted_url,
                unrestricted_url=file_payload.unrestricted_url,
                original_filename=file_payload.original_filename,
                size_bytes=file_payload.size_bytes,
                lease_state=file_payload.lease_state,
                expires_at=_format_optional_datetime(file_payload.expires_at),
                last_refreshed_at=_format_optional_datetime(file_payload.last_refreshed_at),
                last_refresh_error=file_payload.last_refresh_error,
                provider=file_payload.provider,
                provider_download_id=file_payload.provider_download_id,
                provider_file_id=file_payload.provider_file_id,
                provider_file_path=file_payload.provider_file_path,
                active_roles=list(file_payload.active_roles),
                source_key=file_payload.source_key,
                query_strategy=file_payload.query_strategy,
                provider_family=file_payload.provider_family,
                locator_source=file_payload.locator_source,
                match_basis=file_payload.match_basis,
                restricted_fallback=file_payload.restricted_fallback,
            )
            if file_payload is not None
            else None
        ),
    )


def _build_vfs_catalog_stats(snapshot: VfsCatalogSnapshot) -> GQLVfsCatalogStats:
    return GQLVfsCatalogStats(
        directory_count=snapshot.stats.directory_count,
        file_count=snapshot.stats.file_count,
        blocked_item_count=snapshot.stats.blocked_item_count,
    )


def _build_vfs_blocked_item(item: object) -> GQLVfsBlockedItem:
    typed_item: Any = item
    return GQLVfsBlockedItem(
        item_id=str(typed_item.item_id),
        external_ref=str(typed_item.external_ref),
        title=str(typed_item.title),
        reason=str(typed_item.reason),
    )


def _build_vfs_rollup_buckets(values: dict[str, int]) -> list[GQLVfsRollupBucket]:
    return [GQLVfsRollupBucket(key=key, count=count) for key, count in values.items()]


def _build_vfs_catalog_rollup(rollup: VfsCatalogRollup) -> GQLVfsCatalogRollup:
    return GQLVfsCatalogRollup(
        blocked_reasons=_build_vfs_rollup_buckets(rollup.blocked_reason_counts),
        query_strategies=_build_vfs_rollup_buckets(rollup.query_strategy_counts),
        provider_families=_build_vfs_rollup_buckets(rollup.provider_family_counts),
        lease_states=_build_vfs_rollup_buckets(rollup.lease_state_counts),
        locator_sources=_build_vfs_rollup_buckets(rollup.locator_source_counts),
        restricted_fallback_file_count=rollup.restricted_fallback_file_count,
        provider_path_preserved_file_count=rollup.provider_path_preserved_file_count,
        multi_role_file_count=rollup.multi_role_file_count,
    )


def _normalize_vfs_path(path: str) -> str:
    stripped = path.strip()
    if not stripped or stripped == "/":
        return "/"
    normalized = PurePosixPath(f"/{stripped.lstrip('/')}").as_posix()
    return normalized if normalized.startswith("/") else f"/{normalized}"


async def _resolve_vfs_snapshot(
    info: Info[GraphQLContext, object],
    generation_id: str | None,
) -> VfsCatalogSnapshot | None:
    supplier = info.context.resources.vfs_catalog_supplier
    if supplier is None:
        return None
    if generation_id is None:
        return await supplier.build_snapshot()
    if not generation_id.isdigit():
        return None
    return await supplier.snapshot_for_generation(int(generation_id))


async def _resolve_vfs_delta(
    info: Info[GraphQLContext, object],
    *,
    base_generation_id: str | None,
) -> VfsCatalogDelta | None:
    supplier = info.context.resources.vfs_catalog_supplier
    if supplier is None or not hasattr(supplier, "build_delta_since"):
        return None

    requested_generation = (base_generation_id or "").strip()
    if requested_generation:
        if not requested_generation.isdigit():
            raise ValueError("base_generation_id must be numeric when provided")
        return cast(
            VfsCatalogDelta | None,
            await cast(Any, supplier).build_delta_since(int(requested_generation)),
        )

    history_ids: list[str] = []
    if hasattr(supplier, "history_generation_ids"):
        history_ids = list(await cast(Any, supplier).history_generation_ids())
    if len(history_ids) < 2:
        return None
    return cast(
        VfsCatalogDelta | None,
        await cast(Any, supplier).build_delta_since(int(history_ids[-2])),
    )


def _find_vfs_entry(snapshot: VfsCatalogSnapshot, path: str) -> VfsCatalogEntry | None:
    normalized_path = _normalize_vfs_path(path)
    return next((entry for entry in snapshot.entries if entry.path == normalized_path), None)


def _build_vfs_breadcrumbs(
    snapshot: VfsCatalogSnapshot,
    entry: VfsCatalogEntry,
) -> list[GQLVfsBreadcrumb]:
    entries_by_id = {candidate.entry_id: candidate for candidate in snapshot.entries}
    lineage: list[VfsCatalogEntry] = []
    cursor: VfsCatalogEntry | None = entry
    while cursor is not None:
        lineage.append(cursor)
        cursor = entries_by_id.get(cursor.parent_entry_id or "")
    lineage.reverse()
    return [
        GQLVfsBreadcrumb(
            entry_id=node.entry_id,
            path=node.path,
            name=node.name,
            kind=node.kind,
        )
        for node in lineage
    ]


def _build_vfs_directory_listing(
    snapshot: VfsCatalogSnapshot,
    *,
    path: str,
    search: str | None = None,
    directories_limit: int = 200,
    files_limit: int = 200,
) -> GQLVfsDirectoryListing | None:
    focused_entry = _find_vfs_entry(snapshot, path)
    if focused_entry is None:
        return None
    entry: VfsCatalogEntry | None = focused_entry
    if focused_entry.kind != "directory":
        if focused_entry.parent_entry_id is None:
            return None
        entry = next(
            (
                candidate
                for candidate in snapshot.entries
                if candidate.entry_id == focused_entry.parent_entry_id and candidate.kind == "directory"
            ),
            None,
        )
        if entry is None:
            return None
    assert entry is not None
    children = sorted(
        (candidate for candidate in snapshot.entries if candidate.parent_entry_id == entry.entry_id),
        key=lambda candidate: (candidate.kind != "directory", candidate.path),
    )
    search_query = (search or "").strip().casefold()
    matched_children = [
        candidate
        for candidate in children
        if not search_query
        or search_query in candidate.name.casefold()
        or search_query in candidate.path.casefold()
    ]
    directories = [
        _build_vfs_catalog_entry(candidate)
        for candidate in matched_children[: max(0, directories_limit + files_limit)]
        if candidate.kind == "directory"
    ][: max(0, directories_limit)]
    files = [
        _build_vfs_catalog_entry(candidate)
        for candidate in matched_children
        if candidate.kind == "file"
    ][: max(0, files_limit)]
    siblings = sorted(
        (
            candidate
            for candidate in snapshot.entries
            if candidate.parent_entry_id == focused_entry.parent_entry_id
        ),
        key=lambda candidate: (candidate.kind != "directory", candidate.path),
    )
    sibling_index = next(
        (index for index, candidate in enumerate(siblings) if candidate.entry_id == focused_entry.entry_id),
        0,
    )
    previous_entry = siblings[sibling_index - 1] if sibling_index > 0 else None
    next_entry = siblings[sibling_index + 1] if sibling_index + 1 < len(siblings) else None
    parent = (
        next(
            (
                candidate
                for candidate in snapshot.entries
                if candidate.entry_id == entry.parent_entry_id
            ),
            None,
        )
        if entry.parent_entry_id is not None
        else None
    )
    return GQLVfsDirectoryListing(
        generation_id=snapshot.generation_id,
        path=entry.path,
        search_query=(search or None) if search_query else None,
        entry=_build_vfs_catalog_entry(entry),
        focused_entry=_build_vfs_catalog_entry(focused_entry),
        parent=_build_vfs_catalog_entry(parent) if parent is not None else None,
        breadcrumbs=_build_vfs_breadcrumbs(snapshot, focused_entry),
        directory_count=len(directories),
        file_count=len(files),
        total_directory_count=sum(1 for candidate in children if candidate.kind == "directory"),
        total_file_count=sum(1 for candidate in children if candidate.kind == "file"),
        sibling_index=sibling_index,
        sibling_count=len(siblings),
        previous_entry=(
            _build_vfs_catalog_entry(previous_entry) if previous_entry is not None else None
        ),
        next_entry=_build_vfs_catalog_entry(next_entry) if next_entry is not None else None,
        stats=_build_vfs_catalog_stats(snapshot),
        directories=directories,
        files=files,
    )


def _build_vfs_snapshot(snapshot: VfsCatalogSnapshot) -> GQLVfsSnapshot:
    rollup = summarize_vfs_catalog_snapshot(snapshot)
    return GQLVfsSnapshot(
        generation_id=snapshot.generation_id,
        published_at=snapshot.published_at.isoformat(),
        stats=_build_vfs_catalog_stats(snapshot),
        rollup=_build_vfs_catalog_rollup(rollup),
        blocked_items=[_build_vfs_blocked_item(item) for item in snapshot.blocked_items],
    )


def _build_vfs_search_result(
    snapshot: VfsCatalogSnapshot,
    *,
    query: str,
    path_prefix: str,
    limit: int,
    kind: str = "any",
    media_type: str | None = None,
    provider_family: str | None = None,
) -> GQLVfsSearchResult:
    normalized_prefix = _normalize_vfs_path(path_prefix)
    search_query = query.strip().casefold()
    matches = [
        entry
        for entry in snapshot.entries
        if entry.path.startswith(normalized_prefix)
        and search_query
        and (kind == "any" or entry.kind == kind)
        and (
            media_type is None
            or (entry.file is not None and str(entry.file.media_type).casefold() == media_type.casefold())
        )
        and (
            provider_family is None
            or (
                entry.file is not None
                and str(entry.file.provider_family).casefold() == provider_family.casefold()
            )
        )
        and (search_query in entry.name.casefold() or search_query in entry.path.casefold())
    ]
    exact_match_count = sum(
        1
        for entry in matches
        if search_query == entry.name.casefold() or search_query == entry.path.casefold()
    )
    directory_matches = sum(1 for entry in matches if entry.kind == "directory")
    file_matches = sum(1 for entry in matches if entry.kind == "file")
    media_type_counts: dict[str, int] = {}
    provider_family_counts: dict[str, int] = {}
    lease_state_counts: dict[str, int] = {}
    for entry in matches:
        if entry.file is None:
            continue
        media_type_key = str(entry.file.media_type) if entry.file.media_type else "unknown"
        provider_family_key = (
            str(entry.file.provider_family) if entry.file.provider_family else "unknown"
        )
        lease_state_key = str(entry.file.lease_state) if entry.file.lease_state else "unknown"
        media_type_counts[media_type_key] = media_type_counts.get(media_type_key, 0) + 1
        provider_family_counts[provider_family_key] = (
            provider_family_counts.get(provider_family_key, 0) + 1
        )
        lease_state_counts[lease_state_key] = lease_state_counts.get(lease_state_key, 0) + 1
    return GQLVfsSearchResult(
        generation_id=snapshot.generation_id,
        query=query,
        path_prefix=normalized_prefix,
        total_matches=len(matches),
        exact_match_count=exact_match_count,
        directory_matches=directory_matches,
        file_matches=file_matches,
        media_type_counts=_build_named_count_buckets(media_type_counts),
        provider_family_counts=_build_named_count_buckets(provider_family_counts),
        lease_state_counts=_build_named_count_buckets(lease_state_counts),
        entries=[_build_vfs_catalog_entry(entry) for entry in matches[:limit]],
    )


def _build_vfs_file_context(
    snapshot: VfsCatalogSnapshot,
    *,
    path: str,
    search: str | None = None,
    directories_limit: int = 200,
    files_limit: int = 200,
) -> GQLVfsFileContext | None:
    focused_entry = _find_vfs_entry(snapshot, path)
    if focused_entry is None or focused_entry.kind != "file":
        return None
    directory = _build_vfs_directory_listing(
        snapshot,
        path=path,
        search=search,
        directories_limit=directories_limit,
        files_limit=files_limit,
    )
    if directory is None:
        return None
    sibling_files = sorted(
        (
            candidate
            for candidate in snapshot.entries
            if candidate.parent_entry_id == focused_entry.parent_entry_id and candidate.kind == "file"
        ),
        key=lambda candidate: candidate.path,
    )
    sibling_index = next(
        (index for index, candidate in enumerate(sibling_files) if candidate.entry_id == focused_entry.entry_id),
        0,
    )
    previous_file = sibling_files[sibling_index - 1] if sibling_index > 0 else None
    next_file = (
        sibling_files[sibling_index + 1]
        if sibling_index + 1 < len(sibling_files)
        else None
    )
    return GQLVfsFileContext(
        generation_id=snapshot.generation_id,
        file=_build_vfs_catalog_entry(focused_entry),
        directory=directory,
        sibling_file_index=sibling_index,
        sibling_file_count=len(sibling_files),
        previous_file=(
            _build_vfs_catalog_entry(previous_file) if previous_file is not None else None
        ),
        next_file=_build_vfs_catalog_entry(next_file) if next_file is not None else None,
    )


def _build_runtime_lifecycle_snapshot(snapshot: RuntimeLifecycleSnapshot) -> GQLRuntimeLifecycleSnapshot:
    return GQLRuntimeLifecycleSnapshot(
        phase=snapshot.phase,
        health=snapshot.health,
        detail=snapshot.detail,
        updated_at=snapshot.updated_at.isoformat(),
        transitions=[
            GQLRuntimeLifecycleTransition(
                phase=transition.phase,
                health=transition.health,
                detail=transition.detail,
                at=transition.at.isoformat(),
            )
            for transition in snapshot.transitions
        ],
    )


def _queue_name(info: Info[GraphQLContext, object]) -> str:
    resources = info.context.resources
    return resources.arq_queue_name or resources.settings.arq_queue_name


def _queue_redis(info: Info[GraphQLContext, object]) -> object:
    resources = info.context.resources
    return resources.arq_redis or resources.redis


def _build_queue_alert(alert: object) -> GQLQueueAlert:
    typed_alert: Any = alert
    return GQLQueueAlert(
        code=str(typed_alert.code),
        severity=str(typed_alert.severity),
        message=str(typed_alert.message),
    )


def _build_worker_queue_status(info: Info[GraphQLContext, object], snapshot: object) -> GQLWorkerQueueStatus:
    typed_snapshot: Any = snapshot
    return GQLWorkerQueueStatus(
        queue_name=str(typed_snapshot.queue_name),
        arq_enabled=bool(info.context.resources.settings.arq_enabled),
        observed_at=str(typed_snapshot.observed_at),
        total_jobs=int(typed_snapshot.total_jobs),
        ready_jobs=int(typed_snapshot.ready_jobs),
        deferred_jobs=int(typed_snapshot.deferred_jobs),
        in_progress_jobs=int(typed_snapshot.in_progress_jobs),
        retry_jobs=int(typed_snapshot.retry_jobs),
        result_jobs=int(typed_snapshot.result_jobs),
        dead_letter_jobs=int(typed_snapshot.dead_letter_jobs),
        alert_level=str(typed_snapshot.alert_level),
        alerts=[_build_queue_alert(alert) for alert in typed_snapshot.alerts],
        oldest_ready_age_seconds=typed_snapshot.oldest_ready_age_seconds,
        next_scheduled_in_seconds=typed_snapshot.next_scheduled_in_seconds,
        dead_letter_oldest_age_seconds=typed_snapshot.dead_letter_oldest_age_seconds,
        dead_letter_reason_counts=cast(
            JSON, dict(typed_snapshot.dead_letter_reason_counts)
        ),
    )


def _build_worker_queue_history_point(point: object) -> GQLWorkerQueueHistoryPoint:
    typed_point: Any = point
    return GQLWorkerQueueHistoryPoint(
        observed_at=str(typed_point.observed_at),
        total_jobs=int(typed_point.total_jobs),
        ready_jobs=int(typed_point.ready_jobs),
        deferred_jobs=int(typed_point.deferred_jobs),
        in_progress_jobs=int(typed_point.in_progress_jobs),
        retry_jobs=int(typed_point.retry_jobs),
        dead_letter_jobs=int(typed_point.dead_letter_jobs),
        oldest_ready_age_seconds=typed_point.oldest_ready_age_seconds,
        next_scheduled_in_seconds=typed_point.next_scheduled_in_seconds,
        alert_level=str(typed_point.alert_level),
        dead_letter_oldest_age_seconds=typed_point.dead_letter_oldest_age_seconds,
        dead_letter_reason_counts=cast(JSON, dict(typed_point.dead_letter_reason_counts)),
    )


def _build_metadata_reindex_status(
    info: Info[GraphQLContext, object],
    point: object | None,
) -> GQLMetadataReindexStatus:
    typed_point: Any | None = point
    return GQLMetadataReindexStatus(
        queue_name=_queue_name(info),
        schedule_offset_minutes=info.context.resources.settings.indexer.schedule_offset_minutes,
        has_history=typed_point is not None,
        observed_at="" if typed_point is None else str(typed_point.observed_at),
        processed=0 if typed_point is None else int(typed_point.processed),
        queued=0 if typed_point is None else int(typed_point.queued),
        reconciled=0 if typed_point is None else int(typed_point.reconciled),
        skipped_active=0 if typed_point is None else int(typed_point.skipped_active),
        failed=0 if typed_point is None else int(typed_point.failed),
        repair_attempted=0 if typed_point is None else int(typed_point.repair_attempted),
        repair_enriched=0 if typed_point is None else int(typed_point.repair_enriched),
        repair_skipped_no_tmdb_id=(
            0 if typed_point is None else int(typed_point.repair_skipped_no_tmdb_id)
        ),
        repair_failed=0 if typed_point is None else int(typed_point.repair_failed),
        repair_requeued=0 if typed_point is None else int(typed_point.repair_requeued),
        repair_skipped_active=0 if typed_point is None else int(typed_point.repair_skipped_active),
        outcome="ok" if typed_point is None else str(typed_point.outcome),
        run_failed=False if typed_point is None else bool(typed_point.run_failed),
        last_error=None if typed_point is None else typed_point.last_error,
    )


def _build_metadata_reindex_history_point(point: object) -> GQLMetadataReindexHistoryPoint:
    typed_point: Any = point
    return GQLMetadataReindexHistoryPoint(
        observed_at=str(typed_point.observed_at),
        processed=int(typed_point.processed),
        queued=int(typed_point.queued),
        reconciled=int(typed_point.reconciled),
        skipped_active=int(typed_point.skipped_active),
        failed=int(typed_point.failed),
        repair_attempted=int(typed_point.repair_attempted),
        repair_enriched=int(typed_point.repair_enriched),
        repair_skipped_no_tmdb_id=int(typed_point.repair_skipped_no_tmdb_id),
        repair_failed=int(typed_point.repair_failed),
        repair_requeued=int(typed_point.repair_requeued),
        repair_skipped_active=int(typed_point.repair_skipped_active),
        outcome=str(typed_point.outcome),
        run_failed=bool(typed_point.run_failed),
        last_error=typed_point.last_error,
    )


def _build_playback_attachment(attachment: object) -> GQLPlaybackAttachment:
    typed_attachment: Any = attachment
    return GQLPlaybackAttachment(
        id=str(typed_attachment.id),
        kind=str(typed_attachment.kind),
        locator=str(typed_attachment.locator),
        source_key=typed_attachment.source_key,
        provider=typed_attachment.provider,
        provider_download_id=typed_attachment.provider_download_id,
        provider_file_id=typed_attachment.provider_file_id,
        provider_file_path=typed_attachment.provider_file_path,
        original_filename=typed_attachment.original_filename,
        file_size=typed_attachment.file_size,
        local_path=typed_attachment.local_path,
        restricted_url=typed_attachment.restricted_url,
        unrestricted_url=typed_attachment.unrestricted_url,
        is_preferred=bool(typed_attachment.is_preferred),
        preference_rank=int(typed_attachment.preference_rank),
        refresh_state=str(typed_attachment.refresh_state),
        expires_at=typed_attachment.expires_at,
        last_refreshed_at=typed_attachment.last_refreshed_at,
        last_refresh_error=typed_attachment.last_refresh_error,
    )


def _build_resolved_playback_attachment(attachment: object | None) -> GQLResolvedPlaybackAttachment | None:
    if attachment is None:
        return None
    typed_attachment: Any = attachment
    return GQLResolvedPlaybackAttachment(
        kind=str(typed_attachment.kind),
        locator=str(typed_attachment.locator),
        source_key=str(typed_attachment.source_key),
        provider=typed_attachment.provider,
        provider_download_id=typed_attachment.provider_download_id,
        provider_file_id=typed_attachment.provider_file_id,
        provider_file_path=typed_attachment.provider_file_path,
        original_filename=typed_attachment.original_filename,
        file_size=typed_attachment.file_size,
        local_path=typed_attachment.local_path,
        restricted_url=typed_attachment.restricted_url,
        unrestricted_url=typed_attachment.unrestricted_url,
    )


def _build_resolved_playback(snapshot: object | None) -> GQLResolvedPlayback | None:
    if snapshot is None:
        return None
    typed_snapshot: Any = snapshot
    return GQLResolvedPlayback(
        direct=_build_resolved_playback_attachment(typed_snapshot.direct),
        hls=_build_resolved_playback_attachment(typed_snapshot.hls),
        direct_ready=bool(typed_snapshot.direct_ready),
        hls_ready=bool(typed_snapshot.hls_ready),
        missing_local_file=bool(typed_snapshot.missing_local_file),
    )


def _build_active_stream_owner(owner: object | None) -> GQLActiveStreamOwner | None:
    if owner is None:
        return None
    typed_owner: Any = owner
    return GQLActiveStreamOwner(
        media_entry_index=int(typed_owner.media_entry_index),
        kind=str(typed_owner.kind),
        original_filename=typed_owner.original_filename,
        provider=typed_owner.provider,
        provider_download_id=typed_owner.provider_download_id,
        provider_file_id=typed_owner.provider_file_id,
        provider_file_path=typed_owner.provider_file_path,
    )


def _build_active_stream(active_stream: object | None) -> GQLActiveStream | None:
    if active_stream is None:
        return None
    typed_active_stream: Any = active_stream
    return GQLActiveStream(
        direct_ready=bool(typed_active_stream.direct_ready),
        hls_ready=bool(typed_active_stream.hls_ready),
        missing_local_file=bool(typed_active_stream.missing_local_file),
        direct_owner=_build_active_stream_owner(typed_active_stream.direct_owner),
        hls_owner=_build_active_stream_owner(typed_active_stream.hls_owner),
    )


def _build_media_entry(entry: object) -> GQLMediaEntry:
    typed_entry: Any = entry
    return GQLMediaEntry(
        entry_type=str(typed_entry.entry_type),
        kind=str(typed_entry.kind),
        original_filename=typed_entry.original_filename,
        url=typed_entry.url,
        local_path=typed_entry.local_path,
        download_url=typed_entry.download_url,
        unrestricted_url=typed_entry.unrestricted_url,
        provider=typed_entry.provider,
        provider_download_id=typed_entry.provider_download_id,
        provider_file_id=typed_entry.provider_file_id,
        provider_file_path=typed_entry.provider_file_path,
        size=typed_entry.size,
        created=typed_entry.created,
        modified=typed_entry.modified,
        refresh_state=str(typed_entry.refresh_state),
        expires_at=typed_entry.expires_at,
        last_refreshed_at=typed_entry.last_refreshed_at,
        last_refresh_error=typed_entry.last_refresh_error,
        active_for_direct=bool(typed_entry.active_for_direct),
        active_for_hls=bool(typed_entry.active_for_hls),
        is_active_stream=bool(typed_entry.is_active_stream),
    )


def _build_media_item_summary(record: MediaItemSummaryRecord) -> GQLMediaItem:
    specialization = record.specialization
    return GQLMediaItem(
        id=strawberry.ID(record.id),
        external_ref=record.external_ref or "",
        title=record.title,
        state=record.state or "Unknown",
        media_type=_summary_media_type(record),
        media_kind=_media_kind(record.type),
        tmdb_id=_to_optional_int(record.tmdb_id),
        tvdb_id=_to_optional_int(record.tvdb_id),
        imdb_id=(specialization.imdb_id if specialization is not None else None),
        parent_tmdb_id=(
            _to_optional_int(specialization.parent_ids.tmdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        parent_tvdb_id=(
            _to_optional_int(specialization.parent_ids.tvdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        show_title=(specialization.show_title if specialization is not None else None),
        season_number=(specialization.season_number if specialization is not None else None),
        episode_number=(specialization.episode_number if specialization is not None else None),
        poster_path=record.poster_path,
        aired_at=record.aired_at,
    )


def _build_library_stats(projection: StatsProjection) -> GQLLibraryStats:
    return GQLLibraryStats(
        total_items=projection.total_items,
        total_movies=projection.movies,
        total_shows=projection.shows,
        total_seasons=projection.seasons,
        total_episodes=projection.episodes,
        completed_items=projection.completed_items,
        incomplete_items=projection.incomplete_items,
        failed_items=projection.failed_items,
        state_breakdown=json.dumps(projection.states),
        activity=json.dumps(
            [{"date": date, "count": count} for date, count in projection.activity.items()]
        ),
    )


def _build_stream_candidate(stream: object) -> GQLStreamCandidate:
    stream_record = cast(StreamORM, stream)
    raw_title = stream_record.raw_title
    parsed_title = stream_record.parsed_title
    title_value = parsed_title.get("title") if isinstance(parsed_title, dict) else None
    return GQLStreamCandidate(
        id=strawberry.ID(stream_record.id),
        raw_title=raw_title,
        parsed_title=title_value if isinstance(title_value, str) else None,
        resolution=stream_record.resolution,
        rank_score=stream_record.rank,
        lev_ratio=stream_record.lev_ratio,
        selected=stream_record.selected,
        passed=(stream_record.rank > 0) if stream_record.lev_ratio is not None else None,
        rejection_reason=None,
    )


def _build_recovery_plan(plan: RecoveryPlanRecord) -> GQLRecoveryPlan:
    return GQLRecoveryPlan(
        mechanism=GQLRecoveryMechanism(plan.mechanism.value),
        target_stage=GQLRecoveryTargetStage(plan.target_stage.value),
        reason=plan.reason,
        next_retry_at=plan.next_retry_at,
        recovery_attempt_count=plan.recovery_attempt_count,
        is_in_cooldown=plan.is_in_cooldown,
    )


async def _build_media_item_detail(
    info: Info[GraphQLContext, object],
    record: MediaItemSummaryRecord,
) -> GQLMediaItemDetail:
    stream_candidates = [
        _build_stream_candidate(stream)
        for stream in await info.context.media_service.get_stream_candidates(
            media_item_id=record.id
        )
    ]
    recovery_plan = await info.context.media_service.get_recovery_plan(media_item_id=record.id)
    selected_stream = next(
        (candidate for candidate in stream_candidates if candidate.selected), None
    )
    specialization = record.specialization
    return GQLMediaItemDetail(
        id=strawberry.ID(record.id),
        title=record.title,
        state=record.state or "Unknown",
        item_type=record.type,
        media_type=_summary_media_type(record),
        media_kind=_media_kind(record.type),
        tmdb_id=_to_optional_int(record.tmdb_id),
        tvdb_id=_to_optional_int(record.tvdb_id),
        imdb_id=(specialization.imdb_id if specialization is not None else None),
        parent_tmdb_id=(
            _to_optional_int(specialization.parent_ids.tmdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        parent_tvdb_id=(
            _to_optional_int(specialization.parent_ids.tvdb_id)
            if specialization is not None and specialization.parent_ids is not None
            else None
        ),
        show_title=(specialization.show_title if specialization is not None else None),
        season_number=(specialization.season_number if specialization is not None else None),
        episode_number=(specialization.episode_number if specialization is not None else None),
        created_at=record.created_at or "",
        updated_at=record.updated_at or "",
        stream_candidates=stream_candidates,
        selected_stream=selected_stream,
        recovery_plan=(
            _build_recovery_plan(recovery_plan)
            if recovery_plan is not None
            else GQLRecoveryPlan(
                mechanism=GQLRecoveryMechanism.NONE,
                target_stage=GQLRecoveryTargetStage.NONE,
                reason="state_not_automatically_recoverable",
                next_retry_at=None,
                recovery_attempt_count=0,
                is_in_cooldown=False,
            )
        ),
        playback_attachments=[
            _build_playback_attachment(attachment) for attachment in record.playback_attachments or []
        ],
        resolved_playback=_build_resolved_playback(record.resolved_playback),
        active_stream=_build_active_stream(record.active_stream),
        media_entries=[_build_media_entry(entry) for entry in record.media_entries or []],
    )


def _serialize_trigger_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_graphql_datetime(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _build_direct_playback_refresh_trigger_result(
    result: AppScopedDirectPlaybackRefreshTriggerResult,
) -> GQLPlaybackRefreshTriggerResult:
    control_plane_result = result.control_plane_result
    scheduling_result = (
        control_plane_result.scheduling_result if control_plane_result is not None else None
    )
    scheduled_request = None
    if control_plane_result is not None and control_plane_result.scheduled_request is not None:
        scheduled_request = control_plane_result.scheduled_request
    elif scheduling_result is not None and scheduling_result.scheduled_request is not None:
        scheduled_request = scheduling_result.scheduled_request

    execution = scheduling_result.execution if scheduling_result is not None else None
    media_entry_execution = execution.media_entry_execution if execution is not None else None
    attachment_execution = execution.attachment_execution if execution is not None else None
    execution_ok = None
    execution_refresh_state = None
    execution_locator = None
    execution_error = None
    if media_entry_execution is not None:
        execution_ok = media_entry_execution.ok
        execution_refresh_state = media_entry_execution.refresh_state
        execution_locator = media_entry_execution.locator
        execution_error = media_entry_execution.error
    elif attachment_execution is not None:
        execution_ok = attachment_execution.ok
        execution_refresh_state = attachment_execution.refresh_state
        execution_locator = attachment_execution.locator
        execution_error = attachment_execution.error

    return GQLPlaybackRefreshTriggerResult(
        item_id=result.item_identifier,
        outcome=result.outcome,
        controller_attached=result.controller_attached,
        control_plane_outcome=(control_plane_result.outcome if control_plane_result is not None else None),
        refresh_outcome=(scheduling_result.outcome if scheduling_result is not None else None),
        execution_ok=execution_ok,
        execution_refresh_state=execution_refresh_state,
        execution_locator=execution_locator,
        execution_error=execution_error,
        retry_after_seconds=(
            scheduling_result.retry_after_seconds if scheduling_result is not None else None
        ),
        deferred_reason=(execution.deferred_reason if execution is not None else None),
        scheduled_requested_at=(
            _serialize_trigger_datetime(scheduled_request.requested_at)
            if scheduled_request is not None
            else None
        ),
        scheduled_not_before=(
            _serialize_trigger_datetime(scheduled_request.not_before)
            if scheduled_request is not None
            else None
        ),
    )


def _build_selected_hls_refresh_trigger_result(
    result: (
        AppScopedHlsFailedLeaseRefreshTriggerResult
        | AppScopedHlsRestrictedFallbackRefreshTriggerResult
    ),
) -> GQLPlaybackRefreshTriggerResult:
    control_plane_result = result.control_plane_result
    refresh_result = control_plane_result.refresh_result if control_plane_result is not None else None
    execution = refresh_result.execution if refresh_result is not None else None
    return GQLPlaybackRefreshTriggerResult(
        item_id=result.item_identifier,
        outcome=result.outcome,
        controller_attached=result.controller_attached,
        control_plane_outcome=(control_plane_result.outcome if control_plane_result is not None else None),
        refresh_outcome=(refresh_result.outcome if refresh_result is not None else None),
        execution_ok=(execution.ok if execution is not None else None),
        execution_refresh_state=(execution.refresh_state if execution is not None else None),
        execution_locator=(execution.locator if execution is not None else None),
        execution_error=(execution.error if execution is not None else None),
        retry_after_seconds=(refresh_result.retry_after_seconds if refresh_result is not None else None),
        deferred_reason=(refresh_result.deferred_reason if refresh_result is not None else None),
        scheduled_requested_at=None,
        scheduled_not_before=None,
    )


def _build_persisted_media_entry_control_result(
    result: PersistedMediaEntryControlMutationResult,
) -> GQLPersistMediaEntryControlResult:
    typed_item: Any = result.item
    roles = {
        str(active_stream.role)
        for active_stream in typed_item.active_streams
        if str(active_stream.media_entry_id) == result.media_entry.id
    }
    media_entry_projection = SimpleNamespace(
        entry_type=result.media_entry.entry_type,
        kind=result.media_entry.kind,
        original_filename=result.media_entry.original_filename,
        url=(
            result.media_entry.unrestricted_url
            or result.media_entry.download_url
            or result.media_entry.local_path
        ),
        local_path=result.media_entry.local_path,
        download_url=result.media_entry.download_url,
        unrestricted_url=result.media_entry.unrestricted_url,
        provider=result.media_entry.provider,
        provider_download_id=result.media_entry.provider_download_id,
        provider_file_id=result.media_entry.provider_file_id,
        provider_file_path=result.media_entry.provider_file_path,
        size=result.media_entry.size_bytes,
        created=result.media_entry.created_at.isoformat()
        if result.media_entry.created_at is not None
        else None,
        modified=result.media_entry.updated_at.isoformat()
        if result.media_entry.updated_at is not None
        else None,
        refresh_state=result.media_entry.refresh_state,
        expires_at=_serialize_trigger_datetime(result.media_entry.expires_at),
        last_refreshed_at=_serialize_trigger_datetime(result.media_entry.last_refreshed_at),
        last_refresh_error=result.media_entry.last_refresh_error,
        active_for_direct="direct" in roles,
        active_for_hls="hls" in roles,
        is_active_stream=bool(roles),
    )
    return GQLPersistMediaEntryControlResult(
        item_id=result.item.id,
        media_entry_id=result.media_entry.id,
        success=True,
        error=None,
        applied_role=result.applied_role,
        media_entry=_build_media_entry(media_entry_projection),
    )


def _build_persisted_playback_attachment_control_result(
    result: PersistedPlaybackAttachmentControlMutationResult,
) -> GQLPersistPlaybackAttachmentControlResult:
    attachment_projection = SimpleNamespace(
        id=result.attachment.id,
        kind=result.attachment.kind,
        locator=result.attachment.locator,
        source_key=result.attachment.source_key,
        provider=result.attachment.provider,
        provider_download_id=result.attachment.provider_download_id,
        provider_file_id=result.attachment.provider_file_id,
        provider_file_path=result.attachment.provider_file_path,
        original_filename=result.attachment.original_filename,
        file_size=result.attachment.file_size,
        local_path=result.attachment.local_path,
        restricted_url=result.attachment.restricted_url,
        unrestricted_url=result.attachment.unrestricted_url,
        is_preferred=result.attachment.is_preferred,
        preference_rank=result.attachment.preference_rank,
        refresh_state=result.attachment.refresh_state,
        expires_at=_serialize_trigger_datetime(result.attachment.expires_at),
        last_refreshed_at=_serialize_trigger_datetime(result.attachment.last_refreshed_at),
        last_refresh_error=result.attachment.last_refresh_error,
    )
    return GQLPersistPlaybackAttachmentControlResult(
        item_id=result.item.id,
        attachment_id=result.attachment.id,
        success=True,
        error=None,
        attachment=_build_playback_attachment(attachment_projection),
        linked_media_entries=[_build_media_entry(entry) for entry in result.linked_media_entries],
    )


@strawberry.type
class CoreQueryResolver:
    """Base query resolvers available without plugin registration."""

    @strawberry.field(description="Service health for GraphQL clients")
    async def health(self, info: Info[GraphQLContext, object]) -> GQLHealthCheck:
        return GQLHealthCheck(
            service=info.context.resources.settings.service_name,
            status="healthy",
        )

    @strawberry.field(description="List media items from persisted state")
    async def items(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 100,
    ) -> list[GQLMediaItem]:
        if limit < 1 or limit > 500:
            raise ValueError("limit must be within range [1, 500]")

        page = await info.context.media_service.search_items(limit=limit, page=1, extended=False)
        return [_build_media_item_summary(record) for record in page.items]

    @strawberry.field(description="Fetch one media item by internal identifier")
    async def item(
        self,
        info: Info[GraphQLContext, object],
        item_id: strawberry.ID,
    ) -> GQLMediaItem | None:
        record = await info.context.media_service.get_item(str(item_id))
        if record is None:
            return None

        return GQLMediaItem(
            id=strawberry.ID(record.id),
            external_ref=record.external_ref,
            title=record.title,
            state=record.state.value,
            media_type=_record_media_type(record),
            media_kind=_media_kind(_record_media_type(record)),
        )

    @strawberry.field(
        description="Intentional GraphQL calendar entries unconstrained by REST compatibility shape"
    )
    async def calendar_entries(
        self,
        info: Info[GraphQLContext, object],
        days_ahead: int = 30,
        days_behind: int = 7,
    ) -> list[GQLCalendarEntry]:
        now = datetime.now(UTC)
        start_date = (now - timedelta(days=days_behind)).isoformat()
        end_date = (now + timedelta(days=days_ahead)).isoformat()
        entries = await info.context.media_service.get_calendar(
            start_date=start_date,
            end_date=end_date,
        )
        return [_build_calendar_entry(entry) for entry in entries]

    @strawberry.field(description="Stat one mounted VFS catalog node by normalized catalog path")
    async def vfs_catalog_entry(
        self,
        info: Info[GraphQLContext, object],
        path: str,
        generation_id: str | None = None,
    ) -> GQLVfsCatalogEntry | None:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return None
        entry = _find_vfs_entry(snapshot, path)
        if entry is None:
            return None
        return _build_vfs_catalog_entry(entry)

    @strawberry.field(
        description="List one mounted VFS directory directly from the current or requested catalog generation"
    )
    async def vfs_directory(
        self,
        info: Info[GraphQLContext, object],
        path: str = "/",
        generation_id: str | None = None,
        search: str | None = None,
        directories_limit: int = 200,
        files_limit: int = 200,
    ) -> GQLVfsDirectoryListing | None:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return None
        return _build_vfs_directory_listing(
            snapshot,
            path=path,
            search=search,
            directories_limit=max(0, min(directories_limit, 500)),
            files_limit=max(0, min(files_limit, 500)),
        )

    @strawberry.field(
        description="Screen-oriented VFS overview for Director browse and detail surfaces"
    )
    async def vfs_overview(
        self,
        info: Info[GraphQLContext, object],
        path: str = "/",
        generation_id: str | None = None,
        search: str | None = None,
        directories_limit: int = 200,
        files_limit: int = 200,
    ) -> GQLVfsOverview | None:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return None
        directory = _build_vfs_directory_listing(
            snapshot,
            path=path,
            search=search,
            directories_limit=max(0, min(directories_limit, 500)),
            files_limit=max(0, min(files_limit, 500)),
        )
        if directory is None:
            return None
        return GQLVfsOverview(
            snapshot=_build_vfs_snapshot(snapshot),
            directory=directory,
        )

    @strawberry.field(
        description="Return one mounted VFS snapshot summary from the shared catalog supplier"
    )
    async def vfs_snapshot(
        self,
        info: Info[GraphQLContext, object],
        generation_id: str | None = None,
    ) -> GQLVfsSnapshot | None:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return None
        return _build_vfs_snapshot(snapshot)

    @strawberry.field(
        description="Return aggregate VFS blocking/query/provider posture for the current or requested snapshot"
    )
    async def vfs_catalog_rollup(
        self,
        info: Info[GraphQLContext, object],
        generation_id: str | None = None,
    ) -> GQLVfsCatalogRollup | None:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return None
        return _build_vfs_catalog_rollup(summarize_vfs_catalog_snapshot(snapshot))

    @strawberry.field(
        description="Live FilmuVFS gRPC governance counters and refresh health for Director operator screens"
    )
    async def vfs_catalog_governance(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLVfsCatalogGovernance:
        return _build_vfs_catalog_governance(
            build_vfs_catalog_governance_posture(info.context.resources)
        )

    @strawberry.field(
        description="GraphQL-native VFS catalog delta rollup using retained mounted generations"
    )
    async def vfs_catalog_delta(
        self,
        info: Info[GraphQLContext, object],
        base_generation_id: str | None = None,
    ) -> GQLVfsCatalogDelta | None:
        delta = await _resolve_vfs_delta(info, base_generation_id=base_generation_id)
        if delta is None:
            return None
        rollup = summarize_vfs_catalog_delta(delta)
        return GQLVfsCatalogDelta(
            generation_id=str(delta.generation_id),
            base_generation_id=delta.base_generation_id,
            published_at=delta.published_at.isoformat(),
            upsert_directory_count=rollup.upsert_directory_count,
            upsert_file_count=rollup.upsert_file_count,
            removal_directory_count=rollup.removal_directory_count,
            removal_file_count=rollup.removal_file_count,
            provider_family_counts=_build_named_count_buckets(
                dict(rollup.provider_family_counts)
            ),
            lease_state_counts=_build_named_count_buckets(dict(rollup.lease_state_counts)),
        )

    @strawberry.field(
        description="GraphQL-native VFS mount diagnostics covering retained generations and live gRPC mount counters"
    )
    async def vfs_mount_diagnostics(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLVfsMountDiagnostics:
        return _build_vfs_mount_diagnostics(
            await build_vfs_mount_diagnostics_posture(info.context.resources)
        )

    @strawberry.field(description="Flattened VFS mount action feed")
    async def vfs_mount_actions(
        self,
        info: Info[GraphQLContext, object],
        severity: str | None = None,
        status: str | None = None,
    ) -> list[GQLOperatorActionItem]:
        rows = await build_vfs_mount_action_items(info.context.resources)
        filtered = [
            row
            for row in rows
            if (severity is None or row.severity == severity)
            and (status is None or row.status == status)
        ]
        return [_build_operator_action_item(row) for row in filtered]

    @strawberry.field(description="Flattened VFS mount gap feed")
    async def vfs_mount_gaps(
        self,
        info: Info[GraphQLContext, object],
        severity: str | None = None,
        status: str | None = None,
    ) -> list[GQLOperatorGapItem]:
        rows = await build_vfs_mount_gap_items(info.context.resources)
        filtered = [
            row
            for row in rows
            if (severity is None or row.severity == severity)
            and (status is None or row.status == status)
        ]
        return [_build_operator_gap_item(row) for row in filtered]

    @strawberry.field(
        description="Retained VFS generation history with snapshot and delta rollups for Director browse/operator screens"
    )
    async def vfs_generation_history(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
    ) -> list[GQLVfsGenerationHistoryPoint]:
        rows = await build_vfs_generation_history_posture(
            info.context.resources,
            limit=limit,
        )
        return [_build_vfs_generation_history_point(row) for row in rows]

    @strawberry.field(
        description="Aggregate VFS generation-history rollup for Director/operator screens"
    )
    async def vfs_generation_history_summary(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
    ) -> GQLVfsGenerationHistorySummary:
        return _build_vfs_generation_history_summary(
            await build_vfs_generation_history_summary(
                info.context.resources,
                limit=max(1, min(limit, 100)),
            )
        )

    @strawberry.field(description="Sequential retained VFS catalog delta history")
    async def vfs_catalog_delta_history(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
    ) -> list[GQLVfsCatalogDelta]:
        rows = await build_vfs_catalog_delta_history(
            info.context.resources,
            limit=max(1, min(limit, 100)),
        )
        return [_build_vfs_catalog_delta(row) for row in rows]

    @strawberry.field(description="Aggregate rollup over sequential retained VFS deltas")
    async def vfs_catalog_delta_history_summary(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
    ) -> GQLVfsCatalogDeltaHistorySummary:
        return _build_vfs_catalog_delta_history_summary(
            await build_vfs_catalog_delta_history_summary(
                info.context.resources,
                limit=max(1, min(limit, 100)),
            )
        )

    @strawberry.field(
        description="List blocked mounted catalog items from the current or requested snapshot"
    )
    async def vfs_blocked_items(
        self,
        info: Info[GraphQLContext, object],
        generation_id: str | None = None,
        reason: str | None = None,
        external_ref: str | None = None,
        title_query: str | None = None,
        limit: int = 100,
    ) -> list[GQLVfsBlockedItem]:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return []
        reason_filter = (reason or "").strip()
        external_ref_filter = (external_ref or "").strip()
        title_filter = (title_query or "").strip().casefold()
        matches = [
            item
            for item in snapshot.blocked_items
            if not reason_filter or str(getattr(item, "reason", "")) == reason_filter
            if not external_ref_filter or str(getattr(item, "external_ref", "")) == external_ref_filter
            if not title_filter or title_filter in str(getattr(item, "title", "")).casefold()
        ]
        return [_build_vfs_blocked_item(item) for item in matches[: max(1, min(limit, 500))]]

    @strawberry.field(description="Blocked-item counts grouped by reason")
    async def vfs_blocked_reason_summaries(
        self,
        info: Info[GraphQLContext, object],
        generation_id: str | None = None,
    ) -> list[GQLNamedCountBucket]:
        rows = await build_vfs_blocked_reason_summaries(
            info.context.resources,
            generation_id=generation_id,
        )
        return _build_named_count_buckets({row.key: row.count for row in rows})

    @strawberry.field(
        description="GraphQL-native VFS search scoped to one path prefix for Director browse surfaces"
    )
    async def vfs_search(
        self,
        info: Info[GraphQLContext, object],
        query: str,
        path_prefix: str = "/",
        generation_id: str | None = None,
        kind: str = "any",
        media_type: str | None = None,
        provider_family: str | None = None,
        limit: int = 50,
    ) -> GQLVfsSearchResult | None:
        bounded_limit = max(1, min(limit, 500))
        normalized_kind = kind.strip().lower() or "any"
        if normalized_kind not in {"any", "directory", "file"}:
            raise ValueError("kind must be one of: any, directory, file")
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return None
        if not query.strip():
            return _build_vfs_search_result(
                snapshot,
                query=query,
                path_prefix=path_prefix,
                limit=bounded_limit,
                kind=normalized_kind,
                media_type=media_type,
                provider_family=provider_family,
            )
        return _build_vfs_search_result(
            snapshot,
            query=query,
            path_prefix=path_prefix,
            limit=bounded_limit,
            kind=normalized_kind,
            media_type=media_type,
            provider_family=provider_family,
        )

    @strawberry.field(
        description="File-focused VFS context for Director detail screens"
    )
    async def vfs_file_context(
        self,
        info: Info[GraphQLContext, object],
        path: str,
        generation_id: str | None = None,
        search: str | None = None,
        directories_limit: int = 200,
        files_limit: int = 200,
    ) -> GQLVfsFileContext | None:
        snapshot = await _resolve_vfs_snapshot(info, generation_id)
        if snapshot is None:
            return None
        return _build_vfs_file_context(
            snapshot,
            path=path,
            search=search,
            directories_limit=max(0, min(directories_limit, 500)),
            files_limit=max(0, min(files_limit, 500)),
        )

    @strawberry.field(
        description="Typed cross-process observability convergence posture for GraphQL-first clients"
    )
    async def observability_convergence(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLObservabilityConvergence:
        return _build_observability_convergence(info)

    @strawberry.field(
        description="Compact observability rollout closure summary for Director/operator screens"
    )
    async def observability_rollout_summary(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLObservabilityRolloutSummary:
        return _build_observability_rollout(info)

    @strawberry.field(
        description="Typed observability pipeline stages with optional GraphQL-native filters"
    )
    async def observability_pipeline_stages(
        self,
        info: Info[GraphQLContext, object],
        status: str | None = None,
        ready: bool | None = None,
    ) -> list[GQLObservabilityPipelineStage]:
        snapshot = build_observability_convergence_snapshot(info.context.resources.settings)
        rows = [
            stage
            for stage in snapshot.pipeline_stages
            if (status is None or stage.status == status)
            and (ready is None or stage.ready == ready)
        ]
        return [
            GQLObservabilityPipelineStage(
                name=str(stage.name),
                status=str(stage.status),
                configured=bool(stage.configured),
                ready=bool(stage.ready),
                required_actions=list(stage.required_actions),
                remaining_gaps=list(stage.remaining_gaps),
            )
            for stage in rows
        ]

    @strawberry.field(
        description="Compact field/header contract summary for Python and Rust observability correlation"
    )
    async def observability_field_contract_summary(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLObservabilityFieldContractSummary:
        return _build_observability_field_contract_summary(
            build_observability_field_contract_summary(info.context.resources)
        )

    @strawberry.field(description="Status buckets across observability pipeline stages")
    async def observability_stage_counts(
        self,
        info: Info[GraphQLContext, object],
    ) -> list[GQLGovernanceStatusCount]:
        return [
            _build_governance_status_count(row)
            for row in build_observability_stage_counts(info.context.resources)
        ]

    @strawberry.field(description="Retained observability proof inventory")
    async def observability_proof_inventory(
        self,
        info: Info[GraphQLContext, object],
        recorded: bool | None = None,
    ) -> list[GQLProofArtifact]:
        rows = build_observability_proof_inventory(info.context.resources)
        if recorded is not None:
            rows = [row for row in rows if row.recorded == recorded]
        return _build_proof_artifact_rows(list(rows))

    @strawberry.field(description="Flattened observability action feed")
    async def observability_actions(
        self,
        info: Info[GraphQLContext, object],
        severity: str | None = None,
        status: str | None = None,
    ) -> list[GQLOperatorActionItem]:
        rows = build_observability_action_items(info.context.resources)
        filtered = [
            row
            for row in rows
            if (severity is None or row.severity == severity)
            and (status is None or row.status == status)
        ]
        return [_build_operator_action_item(row) for row in filtered]

    @strawberry.field(description="Flattened observability gap feed")
    async def observability_gaps(
        self,
        info: Info[GraphQLContext, object],
        severity: str | None = None,
        status: str | None = None,
    ) -> list[GQLOperatorGapItem]:
        rows = build_observability_gap_items(info.context.resources)
        filtered = [
            row
            for row in rows
            if (severity is None or row.severity == severity)
            and (status is None or row.status == status)
        ]
        return [_build_operator_gap_item(row) for row in filtered]

    @strawberry.field(
        description="Retained rollout evidence checks across playback, VFS, identity, plugin runtime, observability, and control-plane domains"
    )
    async def enterprise_rollout_evidence(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLEnterpriseRolloutEvidence:
        return _build_enterprise_rollout_evidence(
            build_enterprise_rollout_evidence_posture(info.context.resources)
        )

    @strawberry.field(
        description="Status buckets across retained rollout-evidence checks"
    )
    async def enterprise_rollout_status_counts(
        self,
        info: Info[GraphQLContext, object],
    ) -> list[GQLGovernanceStatusCount]:
        return [
            _build_governance_status_count(row)
            for row in build_enterprise_rollout_status_counts(info.context.resources)
        ]

    @strawberry.field(
        description="Retained rollout artifact inventory for Director/operator evidence views"
    )
    async def enterprise_rollout_artifact_inventory(
        self,
        info: Info[GraphQLContext, object],
        check_key: str | None = None,
        recorded: bool | None = None,
    ) -> list[GQLGovernanceArtifactInventoryItem]:
        rows = build_enterprise_rollout_artifact_inventory(info.context.resources)
        filtered = [
            row
            for row in rows
            if (check_key is None or row.check_key == check_key)
            and (recorded is None or row.recorded == recorded)
        ]
        return [_build_governance_artifact_inventory_item(row) for row in filtered]

    @strawberry.field(
        description="Flattened rollout-governance action feed for Director/operator consoles"
    )
    async def enterprise_rollout_actions(
        self,
        info: Info[GraphQLContext, object],
        domain: str | None = None,
        severity: str | None = None,
        status: str | None = None,
    ) -> list[GQLOperatorActionItem]:
        rows = build_enterprise_rollout_action_items(info.context.resources)
        filtered = [
            row
            for row in rows
            if (domain is None or row.domain == domain)
            and (severity is None or row.severity == severity)
            and (status is None or row.status == status)
        ]
        return [_build_operator_action_item(row) for row in filtered]

    @strawberry.field(
        description="Flattened rollout-governance gap feed for Director/operator consoles"
    )
    async def enterprise_rollout_gaps(
        self,
        info: Info[GraphQLContext, object],
        domain: str | None = None,
        severity: str | None = None,
        status: str | None = None,
    ) -> list[GQLOperatorGapItem]:
        rows = build_enterprise_rollout_gap_items(info.context.resources)
        filtered = [
            row
            for row in rows
            if (domain is None or row.domain == domain)
            and (severity is None or row.severity == severity)
            and (status is None or row.status == status)
        ]
        return [_build_operator_gap_item(row) for row in filtered]

    @strawberry.field(
        description="Typed playback-gate rollout posture for GraphQL-first operator consoles"
    )
    async def playback_gate_governance(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLPlaybackGateGovernance:
        _ = info
        return _build_playback_gate_governance(build_playback_gate_governance_posture())

    @strawberry.field(
        description="Typed VFS runtime rollout and canary posture for GraphQL-first operator consoles"
    )
    async def vfs_runtime_rollout(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLVfsRuntimeRollout:
        return _build_vfs_runtime_rollout(
            build_vfs_runtime_rollout_posture(info.context.resources)
        )

    @strawberry.field(
        description="Detailed VFS runtime telemetry with Rust-mounted and Python-serving rollups"
    )
    async def vfs_runtime_telemetry(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLVfsRuntimeTelemetry:
        return _build_vfs_runtime_telemetry(
            build_vfs_runtime_telemetry_posture(info.context.resources)
        )

    @strawberry.field(
        description="Persisted VFS rollout-control state and bounded operator history for canary promotion"
    )
    async def vfs_rollout_control(
        self,
        info: Info[GraphQLContext, object],
        history_limit: int = 20,
    ) -> GQLVfsRolloutControl:
        compat_routes = _compat_route_module()
        snapshot = compat_routes._vfs_rollout_control_response()
        bounded_limit = max(1, min(history_limit, 100))
        payload = snapshot.model_dump()
        payload["history"] = list(snapshot.history[:bounded_limit])
        filtered_snapshot = SimpleNamespace(**payload)
        return _build_vfs_rollout_control(filtered_snapshot)

    @strawberry.field(
        description="Builtin plugin registration and config-validation posture for GraphQL-first clients"
    )
    async def plugin_integration_readiness(
        self,
        info: Info[GraphQLContext, object],
        status: str | None = None,
        capability_kind: str | None = None,
        include_disabled: bool = True,
    ) -> GQLPluginIntegrationReadiness:
        snapshot = build_plugin_integration_readiness_posture(info.context.resources)
        filtered_plugins = [
            plugin
            for plugin in snapshot.plugins
            if (status is None or plugin.status == status)
            and (capability_kind is None or plugin.capability_kind == capability_kind)
            and (include_disabled or plugin.enabled)
        ]
        filtered_snapshot = SimpleNamespace(
            generated_at=snapshot.generated_at,
            status=(
                "ready"
                if filtered_plugins and all(plugin.ready for plugin in filtered_plugins)
                else "partial"
                if filtered_plugins
                else "blocked"
            ),
            plugins=filtered_plugins,
            required_actions=sorted(
                {action for plugin in filtered_plugins for action in plugin.required_actions}
            ),
            remaining_gaps=list(
                dict.fromkeys(
                    gap for plugin in filtered_plugins for gap in plugin.remaining_gaps
                )
            ),
        )
        return _build_plugin_integration_readiness(filtered_snapshot)

    @strawberry.field(
        description="Downloader orchestration posture for GraphQL-first operator and Director clients"
    )
    async def downloader_orchestration(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLDownloaderOrchestration:
        return _build_downloader_orchestration(
            build_downloader_orchestration_posture(info.context.resources)
        )

    @strawberry.field(
        description="Retained downloader execution evidence and recent dead-letter samples for failover/operator inspection"
    )
    async def downloader_execution_evidence(
        self,
        info: Info[GraphQLContext, object],
        provider: str | None = None,
        failure_kind: str | None = None,
        limit: int = 20,
    ) -> GQLDownloaderExecutionEvidence:
        bounded_limit = max(1, min(limit, 100))
        snapshot = await build_downloader_execution_evidence_posture(
            info.context.resources,
            dead_letter_limit=bounded_limit,
        )
        filtered_dead_letters = [
            row
            for row in snapshot.recent_dead_letters
            if (provider is None or row.provider == provider)
            and (failure_kind is None or row.failure_kind == failure_kind)
        ]
        filtered_snapshot = SimpleNamespace(
            generated_at=snapshot.generated_at,
            queue_name=snapshot.queue_name,
            status=snapshot.status,
            selection_mode=snapshot.selection_mode,
            ordered_failover_ready=snapshot.ordered_failover_ready,
            fanout_ready=snapshot.fanout_ready,
            provider_counts=snapshot.provider_counts,
            failure_kind_counts=snapshot.failure_kind_counts,
            dead_letter_reason_counts=snapshot.dead_letter_reason_counts,
            history_summary=snapshot.history_summary,
            recent_dead_letters=filtered_dead_letters[:bounded_limit],
            required_actions=snapshot.required_actions,
            remaining_gaps=snapshot.remaining_gaps,
        )
        return _build_downloader_execution_evidence(filtered_snapshot)

    @strawberry.field(
        description="Bounded downloader queue-history trend summary for Director/operator consoles"
    )
    async def downloader_execution_trend_summary(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
    ) -> GQLDownloaderExecutionTrendSummary:
        return _build_downloader_execution_trend_summary(
            await build_downloader_execution_trend_summary(
                info.context.resources,
                limit=max(1, min(limit, 100)),
            )
        )

    @strawberry.field(
        description="Provider-grouped downloader dead-letter evidence summary"
    )
    async def downloader_provider_summaries(
        self,
        info: Info[GraphQLContext, object],
        provider: str | None = None,
        failure_kind: str | None = None,
        reason_code: str | None = None,
        limit: int = 50,
    ) -> list[GQLDownloaderProviderSummary]:
        rows = await build_downloader_provider_summaries(
            info.context.resources,
            limit=max(1, min(limit, 100)),
        )
        filtered = [
            row
            for row in rows
            if (provider is None or row.provider == provider)
            and (failure_kind is None or row.failure_kind_counts.get(failure_kind, 0) > 0)
            and (reason_code is None or row.reason_code_counts.get(reason_code, 0) > 0)
        ]
        return [_build_downloader_provider_summary(row) for row in filtered]

    @strawberry.field(
        description="Reason-code grouped downloader dead-letter evidence summary"
    )
    async def downloader_reason_summaries(
        self,
        info: Info[GraphQLContext, object],
        reason_code: str | None = None,
        provider: str | None = None,
        failure_kind: str | None = None,
        limit: int = 50,
    ) -> list[GQLDownloaderReasonSummary]:
        rows = await build_downloader_reason_summaries(
            info.context.resources,
            limit=max(1, min(limit, 100)),
        )
        filtered = [
            row
            for row in rows
            if (reason_code is None or row.reason_code == reason_code)
            and (provider is None or row.provider_counts.get(provider, 0) > 0)
            and (failure_kind is None or row.failure_kind_counts.get(failure_kind, 0) > 0)
        ]
        return [_build_downloader_reason_summary(row) for row in filtered]

    @strawberry.field(description="Alert-level counts across bounded downloader queue history")
    async def downloader_alert_level_counts(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
    ) -> list[GQLNamedCountBucket]:
        rows = await build_downloader_alert_level_counts(
            info.context.resources,
            limit=max(1, min(limit, 100)),
        )
        return _build_named_count_buckets({row.key: row.count for row in rows})

    @strawberry.field(description="Time-bucketed downloader dead-letter evidence")
    async def downloader_dead_letter_timeline(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 50,
        bucket_minutes: int = 60,
        provider: str | None = None,
        reason_code: str | None = None,
        failure_kind: str | None = None,
    ) -> list[GQLDownloaderDeadLetterTimelinePoint]:
        rows = await build_downloader_dead_letter_timeline(
            info.context.resources,
            limit=max(1, min(limit, 200)),
            bucket_minutes=bucket_minutes,
            provider=provider,
            reason_code=reason_code,
            failure_kind=failure_kind,
        )
        return [_build_downloader_dead_letter_timeline_point(row) for row in rows]

    @strawberry.field(description="Failure-kind grouped downloader dead-letter evidence")
    async def downloader_failure_kind_summaries(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 50,
        provider: str | None = None,
        reason_code: str | None = None,
    ) -> list[GQLDownloaderFailureKindSummary]:
        rows = await build_downloader_failure_kind_summaries(
            info.context.resources,
            limit=max(1, min(limit, 200)),
            provider=provider,
            reason_code=reason_code,
        )
        return [_build_downloader_failure_kind_summary(row) for row in rows]

    @strawberry.field(description="Status-code grouped downloader dead-letter evidence")
    async def downloader_status_code_summaries(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 50,
        provider: str | None = None,
        reason_code: str | None = None,
    ) -> list[GQLDownloaderStatusCodeSummary]:
        rows = await build_downloader_status_code_summaries(
            info.context.resources,
            limit=max(1, min(limit, 200)),
            provider=provider,
            reason_code=reason_code,
        )
        return [_build_downloader_status_code_summary(row) for row in rows]

    @strawberry.field(description="Flattened downloader action feed")
    async def downloader_actions(
        self,
        info: Info[GraphQLContext, object],
        severity: str | None = None,
        status: str | None = None,
    ) -> list[GQLOperatorActionItem]:
        rows = await build_downloader_action_items(info.context.resources)
        filtered = [
            row
            for row in rows
            if (severity is None or row.severity == severity)
            and (status is None or row.status == status)
        ]
        return [_build_operator_action_item(row) for row in filtered]

    @strawberry.field(description="Flattened downloader gap feed")
    async def downloader_gaps(
        self,
        info: Info[GraphQLContext, object],
        severity: str | None = None,
        status: str | None = None,
    ) -> list[GQLOperatorGapItem]:
        rows = await build_downloader_gap_items(info.context.resources)
        filtered = [
            row
            for row in rows
            if (severity is None or row.severity == severity)
            and (status is None or row.status == status)
        ]
        return [_build_operator_gap_item(row) for row in filtered]

    @strawberry.field(
        description="Bounded downloader-focused queue history so Director does not need to repurpose the generic worker queue graph"
    )
    async def downloader_execution_history(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
        alert_level: str | None = None,
        min_dead_letter_jobs: int = 0,
        reason_code: str | None = None,
    ) -> list[GQLWorkerQueueHistoryPoint]:
        bounded_limit = max(1, min(limit, 100))
        bounded_min_dead_letter_jobs = max(0, min(min_dead_letter_jobs, 10_000))
        if alert_level is not None and alert_level not in {"ok", "warning", "critical"}:
            raise ValueError("alert_level must be one of: ok, warning, critical")
        history = await QueueStatusReader(
            _queue_redis(info),
            queue_name=_queue_name(info),
        ).history(limit=bounded_limit)
        points = [_build_worker_queue_history_point(item) for item in history]
        if alert_level is not None:
            points = [item for item in points if item.alert_level == alert_level]
        if bounded_min_dead_letter_jobs > 0:
            points = [item for item in points if item.dead_letter_jobs >= bounded_min_dead_letter_jobs]
        if reason_code is not None:
            points = [
                item
                for item in points
                if cast(dict[str, int], item.dead_letter_reason_counts).get(reason_code, 0) > 0
            ]
        return points

    @strawberry.field(
        description="Bounded downloader/debrid dead-letter samples with GraphQL-native filters"
    )
    async def downloader_execution_dead_letters(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
        reason_code: str | None = None,
        provider: str | None = None,
        failure_kind: str | None = None,
    ) -> list[GQLDownloaderExecutionDeadLetter]:
        bounded_limit = max(1, min(limit, 100))
        samples = await QueueStatusReader(
            _queue_redis(info),
            queue_name=_queue_name(info),
        ).dead_letter_samples(limit=bounded_limit, stage="debrid_item", reason_code=reason_code)
        rows: list[GQLDownloaderExecutionDeadLetter] = []
        for sample in samples:
            metadata = sample.metadata
            row = SimpleNamespace(
                stage=sample.stage,
                item_id=sample.item_id,
                reason=sample.reason,
                reason_code=sample.reason_code,
                idempotency_key=sample.idempotency_key,
                attempt=sample.attempt,
                queued_at=sample.queued_at,
                provider=(
                    str(metadata.get("provider")).strip()
                    if isinstance(metadata.get("provider"), str)
                    else None
                ),
                failure_kind=(
                    str(metadata.get("failure_kind")).strip()
                    if isinstance(metadata.get("failure_kind"), str)
                    else None
                ),
                selected_stream_id=(
                    str(metadata.get("selected_stream_id")).strip()
                    if isinstance(metadata.get("selected_stream_id"), str)
                    else None
                ),
                item_request_id=(
                    str(metadata.get("item_request_id")).strip()
                    if isinstance(metadata.get("item_request_id"), str)
                    else None
                ),
                status_code=(
                    int(metadata["status_code"])
                    if isinstance(metadata.get("status_code"), int)
                    and not isinstance(metadata.get("status_code"), bool)
                    else None
                ),
                retry_after_seconds=(
                    int(metadata["retry_after_seconds"])
                    if isinstance(metadata.get("retry_after_seconds"), int)
                    and not isinstance(metadata.get("retry_after_seconds"), bool)
                    else None
                ),
            )
            if provider is not None and row.provider != provider:
                continue
            if failure_kind is not None and row.failure_kind != failure_kind:
                continue
            rows.append(_build_downloader_dead_letter(row))
        return rows[:bounded_limit]

    @strawberry.field(
        description="Declared publishable plugin events and hook subscriptions"
    )
    async def plugin_events(
        self,
        info: Info[GraphQLContext, object],
        publisher: str | None = None,
        wiring_status: str | None = None,
    ) -> list[GQLPluginEventStatus]:
        rows = build_plugin_event_status_posture(info.context.resources)
        if publisher is not None:
            rows = [row for row in rows if row.publisher == publisher]
        if wiring_status is not None:
            rows = [row for row in rows if row.wiring_status == wiring_status]
        return [_build_plugin_event_status(row) for row in rows]

    @strawberry.field(
        description="Aggregated plugin runtime health, wiring, and retained proof posture for Director operator screens"
    )
    async def plugin_runtime_overview(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLPluginRuntimeOverview:
        overview, _warnings = await build_plugin_runtime_overview_posture(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        return _build_plugin_runtime_overview(overview)

    @strawberry.field(
        description="Actionable plugin runtime warning rows derived from governance and integration posture"
    )
    async def plugin_runtime_warnings(
        self,
        info: Info[GraphQLContext, object],
        severity: str | None = None,
        source: str | None = None,
    ) -> list[GQLPluginRuntimeWarning]:
        _overview, warnings = await build_plugin_runtime_overview_posture(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        filtered = [
            warning
            for warning in warnings
            if (severity is None or warning.severity == severity)
            and (source is None or warning.source == source)
        ]
        return [_build_plugin_runtime_warning(row) for row in filtered]

    @strawberry.field(
        description="Combined plugin runtime rows with wiring and retained proof posture"
    )
    async def plugin_runtime_rows(
        self,
        info: Info[GraphQLContext, object],
        status: str | None = None,
        capability_kind: str | None = None,
        ready: bool | None = None,
        wiring_status: str | None = None,
        quarantined: bool | None = None,
    ) -> list[GQLPluginRuntimeRow]:
        rows = await build_plugin_runtime_rows(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        filtered = [
            row
            for row in rows
            if (status is None or row.status == status)
            and (capability_kind is None or capability_kind in row.capability_kinds)
            and (ready is None or row.ready == ready)
            and (wiring_status is None or row.wiring_status == wiring_status)
            and (quarantined is None or row.quarantined == quarantined)
        ]
        return [_build_plugin_runtime_row(row) for row in filtered]

    @strawberry.field(
        description="Capability-grouped plugin runtime summary for Director/operator screens"
    )
    async def plugin_runtime_capability_summaries(
        self,
        info: Info[GraphQLContext, object],
        capability_kind: str | None = None,
    ) -> list[GQLPluginRuntimeCapabilitySummary]:
        rows = await build_plugin_runtime_rows(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        summaries = build_plugin_runtime_capability_summaries(rows)
        if capability_kind is not None:
            summaries = [row for row in summaries if row.capability_kind == capability_kind]
        return [_build_plugin_runtime_capability_summary(row) for row in summaries]

    @strawberry.field(
        description="Capability-grouped retained plugin proof coverage summary"
    )
    async def plugin_proof_coverage_summaries(
        self,
        info: Info[GraphQLContext, object],
        capability_kind: str | None = None,
    ) -> list[GQLPluginProofCoverageSummary]:
        rows = await build_plugin_runtime_rows(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        summaries = build_plugin_proof_coverage_summaries(rows)
        if capability_kind is not None:
            summaries = [row for row in summaries if row.capability_kind == capability_kind]
        return [_build_plugin_proof_coverage_summary(row) for row in summaries]

    @strawberry.field(description="Status buckets across plugin runtime rows")
    async def plugin_runtime_status_counts(
        self,
        info: Info[GraphQLContext, object],
    ) -> list[GQLGovernanceStatusCount]:
        rows = await build_plugin_runtime_status_counts(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        return [_build_governance_status_count(row) for row in rows]

    @strawberry.field(description="Wiring-status buckets across plugin runtime rows")
    async def plugin_runtime_wiring_status_counts(
        self,
        info: Info[GraphQLContext, object],
    ) -> list[GQLNamedCountBucket]:
        rows = await build_plugin_runtime_wiring_status_counts(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        return _build_named_count_buckets({row.key: row.count for row in rows})

    @strawberry.field(description="Publisher-grouped plugin runtime summary")
    async def plugin_runtime_publisher_summaries(
        self,
        info: Info[GraphQLContext, object],
        publisher: str | None = None,
    ) -> list[GQLPluginRuntimePublisherSummary]:
        rows = await build_plugin_runtime_publisher_summaries(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        if publisher is not None:
            rows = [row for row in rows if row.publisher == publisher]
        return [_build_plugin_runtime_publisher_summary(row) for row in rows]

    @strawberry.field(description="Capability-grouped action counts across plugin runtime posture")
    async def plugin_runtime_capability_action_counts(
        self,
        info: Info[GraphQLContext, object],
    ) -> list[GQLNamedCountBucket]:
        rows = await build_plugin_runtime_capability_action_counts(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        return _build_named_count_buckets({row.key: row.count for row in rows})

    @strawberry.field(description="Capability-grouped gap counts across plugin runtime posture")
    async def plugin_runtime_capability_gap_counts(
        self,
        info: Info[GraphQLContext, object],
    ) -> list[GQLNamedCountBucket]:
        rows = await build_plugin_runtime_capability_gap_counts(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        return _build_named_count_buckets({row.key: row.count for row in rows})

    @strawberry.field(
        description="Flattened plugin runtime action feed for Director/operator consoles"
    )
    async def plugin_runtime_actions(
        self,
        info: Info[GraphQLContext, object],
        severity: str | None = None,
        status: str | None = None,
        capability_kind: str | None = None,
        plugin_name: str | None = None,
    ) -> list[GQLOperatorActionItem]:
        rows = await build_plugin_runtime_rows(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        actions = build_plugin_runtime_action_items(rows)
        filtered = [
            row
            for row in actions
            if (severity is None or row.severity == severity)
            and (status is None or row.status == status)
            and (capability_kind is None or row.capability_kind == capability_kind)
            and (plugin_name is None or row.subject == plugin_name)
        ]
        return [_build_operator_action_item(row) for row in filtered]

    @strawberry.field(
        description="Flattened plugin runtime gap feed for Director/operator consoles"
    )
    async def plugin_runtime_gaps(
        self,
        info: Info[GraphQLContext, object],
        severity: str | None = None,
        status: str | None = None,
        capability_kind: str | None = None,
        plugin_name: str | None = None,
    ) -> list[GQLOperatorGapItem]:
        rows = await build_plugin_runtime_rows(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        gaps = build_plugin_runtime_gap_items(rows)
        filtered = [
            row
            for row in gaps
            if (severity is None or row.severity == severity)
            and (status is None or row.status == status)
            and (capability_kind is None or row.capability_kind == capability_kind)
            and (plugin_name is None or row.subject == plugin_name)
        ]
        return [_build_operator_gap_item(row) for row in filtered]

    @strawberry.field(
        description="Plugin trust, readiness, and runtime isolation governance posture"
    )
    async def plugin_governance(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLPluginGovernance:
        cached = await _read_cached_graphql_payload(
            info,
            key=_GRAPHQL_PLUGIN_GOVERNANCE_CACHE_KEY,
        )
        if isinstance(cached, dict):
            return _hydrate_plugin_governance(cached)

        snapshot = await build_plugin_governance_posture(
            info.context.resources,
            app_state=info.context.request.app.state,
        )
        result = _build_plugin_governance(summary=snapshot.summary, plugins=list(snapshot.plugins))
        await _write_cached_graphql_payload(
            info,
            key=_GRAPHQL_PLUGIN_GOVERNANCE_CACHE_KEY,
            payload=asdict(result),
        )
        return result

    @strawberry.field(
        description="Persisted access-policy revisions for graph operator workflows"
    )
    async def access_policy_revisions(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
    ) -> GQLAccessPolicyRevisionList:
        await require_graphql_permissions(
            info,
            "settings:write",
            resource_scope="access_policy",
        )
        normalized_limit = max(1, min(limit, 50))
        cached = await _read_cached_graphql_payload(
            info,
            key=_GRAPHQL_ACCESS_POLICY_REVISIONS_CACHE_KEY,
        )
        if isinstance(cached, dict):
            return _hydrate_access_policy_revision_list(cached, limit=normalized_limit)

        compat_routes = _compat_route_module()
        response = await compat_routes.list_auth_policy_revisions(
            info.context.request,
            limit=50,
        )
        result = _build_access_policy_revision_list(response)
        await _write_cached_graphql_payload(
            info,
            key=_GRAPHQL_ACCESS_POLICY_REVISIONS_CACHE_KEY,
            payload=asdict(result),
        )
        return GQLAccessPolicyRevisionList(
            active_version=result.active_version,
            revisions=list(result.revisions[:normalized_limit]),
        )

    @strawberry.field(
        description="Persisted plugin-governance overrides for graph operator workflows"
    )
    async def plugin_governance_overrides(
        self,
        info: Info[GraphQLContext, object],
    ) -> list[GQLPluginGovernanceOverride]:
        await require_graphql_permissions(
            info,
            "settings:write",
            resource_scope="plugin_governance",
        )
        cached = await _read_cached_graphql_payload(
            info,
            key=_GRAPHQL_PLUGIN_GOVERNANCE_OVERRIDES_CACHE_KEY,
        )
        if isinstance(cached, list):
            return [_hydrate_plugin_governance_override(row) for row in cached]

        compat_routes = _compat_route_module()
        response = await compat_routes.list_plugin_governance_overrides(info.context.request)
        result = [_build_plugin_governance_override(row) for row in response]
        await _write_cached_graphql_payload(
            info,
            key=_GRAPHQL_PLUGIN_GOVERNANCE_OVERRIDES_CACHE_KEY,
            payload=[asdict(row) for row in result],
        )
        return result

    @strawberry.field(description="Bounded control-plane subscriber health rollup")
    async def control_plane_summary(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
    ) -> GQLControlPlaneSummary:
        return _build_control_plane_summary(
            await build_control_plane_summary_posture(
                info.context.resources,
                active_within_seconds=active_within_seconds,
            )
        )

    @strawberry.field(description="Status buckets across control-plane subscriber states")
    async def control_plane_status_counts(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
    ) -> list[GQLNamedCountBucket]:
        rows = await build_control_plane_status_counts(
            info.context.resources,
            active_within_seconds=active_within_seconds,
        )
        return _build_named_count_buckets({row.key: row.count for row in rows})

    @strawberry.field(
        description="Durable replay/control-plane subscriber ledger rows for GraphQL-first operator consoles"
    )
    async def control_plane_subscribers(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
        status: str | None = None,
        tenant_id: str | None = None,
        consumer_group: str | None = None,
        consumer_name: str | None = None,
        node_id: str | None = None,
        ack_pending: bool | None = None,
        fenced: bool | None = None,
        limit: int = 100,
    ) -> list[GQLControlPlaneSubscriber]:
        rows = [
            _build_control_plane_subscriber(row)
            for row in await build_control_plane_subscribers_posture(
                info.context.resources,
                active_within_seconds=active_within_seconds,
            )
        ]
        filtered = [
            row
            for row in rows
            if (status is None or row.status == status)
            and (tenant_id is None or row.tenant_id == tenant_id)
            and (consumer_group is None or row.group_name == consumer_group)
            and (consumer_name is None or row.consumer_name == consumer_name)
            and (node_id is None or row.node_id == node_id)
            and (ack_pending is None or row.ack_pending == ack_pending)
            and (fenced is None or row.fenced == fenced)
        ]
        return filtered[: max(1, min(limit, 500))]

    @strawberry.field(description="Grouped control-plane summary per consumer")
    async def control_plane_consumer_summaries(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
        consumer_name: str | None = None,
    ) -> list[GQLControlPlaneConsumerSummary]:
        rows = await build_control_plane_consumer_summaries(
            info.context.resources,
            active_within_seconds=active_within_seconds,
        )
        if consumer_name is not None:
            rows = [row for row in rows if row.consumer_name == consumer_name]
        return [_build_control_plane_consumer_summary(row) for row in rows]

    @strawberry.field(description="Subscriber counts grouped by owning node")
    async def control_plane_node_counts(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
    ) -> list[GQLNamedCountBucket]:
        rows = await build_control_plane_node_counts(
            info.context.resources,
            active_within_seconds=active_within_seconds,
        )
        return _build_named_count_buckets({row.key: row.count for row in rows})

    @strawberry.field(description="Subscriber counts grouped by tenant")
    async def control_plane_tenant_counts(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
    ) -> list[GQLNamedCountBucket]:
        rows = await build_control_plane_tenant_counts(
            info.context.resources,
            active_within_seconds=active_within_seconds,
        )
        return _build_named_count_buckets({row.key: row.count for row in rows})

    @strawberry.field(description="Aggregated ownership summary across control-plane subscribers")
    async def control_plane_ownership_summary(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
    ) -> GQLControlPlaneOwnershipSummary:
        return _build_control_plane_ownership_summary(
            await build_control_plane_ownership_summary(
                info.context.resources,
                active_within_seconds=active_within_seconds,
            )
        )

    @strawberry.field(
        description="GraphQL-first recovery readiness rollup for control-plane evidence and automation"
    )
    async def control_plane_recovery_readiness(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
    ) -> GQLControlPlaneRecoveryReadiness:
        return _build_control_plane_recovery_readiness(
            await build_control_plane_recovery_readiness_posture(
                info.context.resources,
                active_within_seconds=active_within_seconds,
            )
        )

    @strawberry.field(
        description="Background replay/control-plane recovery automation posture"
    )
    async def control_plane_automation(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLControlPlaneAutomation:
        return _build_control_plane_automation(
            await build_control_plane_automation_posture(info.context.resources)
        )

    @strawberry.field(
        description="Replay-backplane readiness and pending-delivery posture for live Redis consumer-group proof"
    )
    async def control_plane_replay_backplane(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLControlPlaneReplayBackplane:
        return _build_control_plane_replay_backplane(
            await build_control_plane_replay_backplane_posture(info.context.resources)
        )

    @strawberry.field(description="Replay consumer counts grouped by consumer name")
    async def control_plane_replay_consumer_counts(
        self,
        info: Info[GraphQLContext, object],
    ) -> list[GQLNamedCountBucket]:
        rows = await build_control_plane_replay_consumer_counts(info.context.resources)
        return _build_named_count_buckets({row.key: row.count for row in rows})

    @strawberry.field(description="Flattened control-plane action feed")
    async def control_plane_actions(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
        severity: str | None = None,
        status: str | None = None,
    ) -> list[GQLOperatorActionItem]:
        rows = await build_control_plane_action_items(
            info.context.resources,
            active_within_seconds=active_within_seconds,
        )
        filtered = [
            row
            for row in rows
            if (severity is None or row.severity == severity)
            and (status is None or row.status == status)
        ]
        return [_build_operator_action_item(row) for row in filtered]

    @strawberry.field(description="Flattened control-plane gap feed")
    async def control_plane_gaps(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
        severity: str | None = None,
        status: str | None = None,
    ) -> list[GQLOperatorGapItem]:
        rows = await build_control_plane_gap_items(
            info.context.resources,
            active_within_seconds=active_within_seconds,
        )
        filtered = [
            row
            for row in rows
            if (severity is None or row.severity == severity)
            and (status is None or row.status == status)
        ]
        return [_build_operator_gap_item(row) for row in filtered]

    @strawberry.field(
        description="Machine-readable enterprise operations posture across current governance slices"
    )
    async def enterprise_operations_governance(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLEnterpriseOperationsGovernance:
        compat_routes = _compat_route_module()
        plugins = await compat_routes.get_plugins(info.context.request)
        return _build_enterprise_operations_governance(
            await compat_routes._enterprise_operations_governance(
                request=info.context.request,
                plugins=plugins,
            )
        )

    @strawberry.field(description="Current runtime lifecycle graph and bounded transition history")
    async def runtime_lifecycle(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLRuntimeLifecycleSnapshot:
        return _build_runtime_lifecycle_snapshot(info.context.resources.runtime_lifecycle.snapshot())

    @strawberry.field(description="Current operator queue snapshot from the shared Redis-backed reader")
    async def worker_queue_status(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLWorkerQueueStatus:
        snapshot = await QueueStatusReader(
            _queue_redis(info),
            queue_name=_queue_name(info),
        ).snapshot()
        return _build_worker_queue_status(info, snapshot)

    @strawberry.field(description="Bounded operator queue history with optional replay-oriented filters")
    async def worker_queue_history(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
        alert_level: str | None = None,
        min_dead_letter_jobs: int = 0,
        reason_code: str | None = None,
    ) -> list[GQLWorkerQueueHistoryPoint]:
        bounded_limit = max(1, min(limit, 100))
        bounded_min_dead_letter_jobs = max(0, min(min_dead_letter_jobs, 10_000))
        if alert_level is not None and alert_level not in {"ok", "warning", "critical"}:
            raise ValueError("alert_level must be one of: ok, warning, critical")
        history = await QueueStatusReader(
            _queue_redis(info),
            queue_name=_queue_name(info),
        ).history(limit=bounded_limit)
        points = [_build_worker_queue_history_point(item) for item in history]
        if alert_level is not None:
            points = [item for item in points if item.alert_level == alert_level]
        if bounded_min_dead_letter_jobs > 0:
            points = [
                item
                for item in points
                if item.dead_letter_jobs >= bounded_min_dead_letter_jobs
            ]
        if reason_code is not None:
            points = [
                item
                for item in points
                if cast(dict[str, int], item.dead_letter_reason_counts).get(reason_code, 0) > 0
            ]
        return points

    @strawberry.field(description="Latest metadata reindex/reconciliation run summary")
    async def worker_metadata_reindex_status(
        self,
        info: Info[GraphQLContext, object],
    ) -> GQLMetadataReindexStatus:
        latest = await MetadataReindexStatusStore(
            _queue_redis(info),
            queue_name=_queue_name(info),
        ).latest()
        return _build_metadata_reindex_status(info, latest)

    @strawberry.field(description="Bounded metadata reindex/reconciliation history")
    async def worker_metadata_reindex_history(
        self,
        info: Info[GraphQLContext, object],
        limit: int = 20,
    ) -> list[GQLMetadataReindexHistoryPoint]:
        bounded_limit = max(1, min(limit, 100))
        history = await MetadataReindexStatusStore(
            _queue_redis(info),
            queue_name=_queue_name(info),
        ).history(limit=bounded_limit)
        return [_build_metadata_reindex_history_point(item) for item in history]

    @strawberry.field(description="Intentional GraphQL library stats projection")
    async def library_stats(self, info: Info[GraphQLContext, object]) -> GQLLibraryStats:
        projection = await info.context.media_service.get_stats()
        return _build_library_stats(projection)

    @strawberry.field(description="Fetch one rich media item detail by internal identifier")
    async def media_item(
        self,
        info: Info[GraphQLContext, object],
        id: strawberry.ID,
    ) -> GQLMediaItemDetail | None:
        record = await info.context.media_service.get_item_detail(
            str(id),
            media_type="item",
            extended=True,
        )
        if record is None:
            return None
        return await _build_media_item_detail(info, record)

    @strawberry.field(description="List rich media item details with optional state filtering")
    async def media_items(
        self,
        info: Info[GraphQLContext, object],
        state: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[GQLMediaItemDetail]:
        bounded_limit = max(1, min(limit, 100))
        bounded_offset = max(0, offset)
        page = await info.context.media_service.search_items(
            limit=min(100, bounded_limit + bounded_offset),
            page=1,
            states=[state] if state is not None else None,
            extended=True,
        )
        selected_records = page.items[bounded_offset : bounded_offset + bounded_limit]
        return [await _build_media_item_detail(info, record) for record in selected_records]


@strawberry.type
class CoreMutationResolver:
    """Base mutation resolvers available without plugin registration."""

    @strawberry.mutation(description="Create or get a media request by external reference")
    async def request_item(
        self,
        info: Info[GraphQLContext, object],
        input: RequestItemInput,
    ) -> RequestItemResult:
        result = await info.context.media_service.request_item_with_enrichment(
            input.external_ref,
            media_type=input.media_type,
            requested_seasons=input.requested_seasons,
        )
        return RequestItemResult(
            item_id=strawberry.ID(result.item.id),
            enrichment_source=result.enrichment.source,
            has_poster=result.enrichment.has_poster,
            has_imdb_id=result.enrichment.has_imdb_id,
            warnings=list(result.enrichment.warnings),
        )

    @strawberry.mutation(description="Apply a lifecycle transition to a media item")
    async def item_action(
        self,
        info: Info[GraphQLContext, object],
        input: ItemActionInput,
    ) -> ItemStateChangedEvent:
        action = input.action.lower()
        if action == "retry":
            result = await info.context.media_service.retry_items([input.item_id])
            to_state = "requested"
        elif action == "reset":
            result = await info.context.media_service.reset_items([input.item_id])
            to_state = "requested"
        elif action == "remove":
            result = await info.context.media_service.remove_items([input.item_id])
            to_state = "removed"
        else:
            raise ValueError(f"unknown action: {input.action}")

        _ensure_item_action_matched(result, input.item_id)
        return ItemStateChangedEvent(
            item_id=input.item_id,
            from_state=None,
            to_state=to_state,
            timestamp=datetime.now(UTC).isoformat(),
        )

    @strawberry.mutation(description="Retry one item immediately and enqueue scrape")
    async def retry_item(
        self,
        item_id: strawberry.ID,
        info: Info[GraphQLContext, object],
    ) -> RetryItemResult:
        try:
            async with info.context.resources.db.session() as session:
                item = await info.context.media_service.retry_item(
                    str(item_id),
                    session,
                    info.context.resources.arq_redis,
                )
        except (ArqNotEnabledError, ItemNotFoundError) as exc:
            return RetryItemResult(
                item_id=str(item_id),
                success=False,
                error=str(exc),
                new_state=None,
            )

        return RetryItemResult(
            item_id=item.id,
            success=True,
            error=None,
            new_state=item.state,
        )

    @strawberry.mutation(description="Reset one item, blacklist current streams, and enqueue scrape")
    async def reset_item(
        self,
        item_id: strawberry.ID,
        info: Info[GraphQLContext, object],
    ) -> ResetItemResult:
        try:
            async with info.context.resources.db.session() as session:
                item = await info.context.media_service.reset_item(
                    str(item_id),
                    session,
                    info.context.resources.arq_redis,
                )
        except (ArqNotEnabledError, ItemNotFoundError) as exc:
            return ResetItemResult(
                item_id=str(item_id),
                success=False,
                error=str(exc),
                new_state=None,
            )

        return ResetItemResult(
            item_id=item.id,
            success=True,
            error=None,
            new_state=item.state,
        )

    @strawberry.mutation(
        description="Trigger direct-play refresh through the shared playback control plane"
    )
    async def trigger_direct_playback_refresh(
        self,
        info: Info[GraphQLContext, object],
        item_id: strawberry.ID,
    ) -> GQLPlaybackRefreshTriggerResult:
        result = await trigger_direct_playback_refresh_from_resources(
            info.context.resources,
            str(item_id),
        )
        return _build_direct_playback_refresh_trigger_result(result)

    @strawberry.mutation(
        description="Trigger selected-HLS failed-lease refresh through the shared playback control plane"
    )
    async def trigger_hls_failed_lease_refresh(
        self,
        info: Info[GraphQLContext, object],
        item_id: strawberry.ID,
    ) -> GQLPlaybackRefreshTriggerResult:
        result = await trigger_hls_failed_lease_refresh_from_resources(
            info.context.resources,
            str(item_id),
        )
        return _build_selected_hls_refresh_trigger_result(result)

    @strawberry.mutation(
        description="Trigger selected-HLS restricted-fallback refresh through the shared playback control plane"
    )
    async def trigger_hls_restricted_fallback_refresh(
        self,
        info: Info[GraphQLContext, object],
        item_id: strawberry.ID,
    ) -> GQLPlaybackRefreshTriggerResult:
        result = await trigger_hls_restricted_fallback_refresh_from_resources(
            info.context.resources,
            str(item_id),
        )
        return _build_selected_hls_refresh_trigger_result(result)

    @strawberry.mutation(
        description="Mark the selected HLS media entry stale so the shared refresh loop can pick it up"
    )
    async def mark_selected_hls_media_entry_stale(
        self,
        info: Info[GraphQLContext, object],
        item_id: strawberry.ID,
    ) -> GQLMarkSelectedHlsMediaEntryStaleResult:
        playback_service = info.context.resources.playback_service
        if playback_service is None:
            return GQLMarkSelectedHlsMediaEntryStaleResult(
                item_id=str(item_id),
                success=False,
                error="playback_service_unavailable",
            )

        success = await playback_service.mark_selected_hls_media_entry_stale(str(item_id))
        return GQLMarkSelectedHlsMediaEntryStaleResult(
            item_id=str(item_id),
            success=success,
            error=None if success else "selected_hls_media_entry_not_marked",
        )

    @strawberry.mutation(
        description="Persist bounded media-entry URL/state changes and optional active-role rebinding through the shared playback service"
    )
    async def persist_media_entry_control_state(
        self,
        info: Info[GraphQLContext, object],
        input: PersistMediaEntryControlInput,
    ) -> GQLPersistMediaEntryControlResult:
        playback_service = info.context.resources.playback_service
        if playback_service is None:
            return GQLPersistMediaEntryControlResult(
                item_id=str(input.item_id),
                media_entry_id=str(input.media_entry_id),
                success=False,
                error="playback_service_unavailable",
                applied_role=None,
                media_entry=None,
            )

        if (
            input.active_role is None
            and input.local_path is None
            and input.download_url is None
            and input.unrestricted_url is None
            and input.refresh_state is None
            and input.last_refresh_error is None
            and input.expires_at is None
        ):
            return GQLPersistMediaEntryControlResult(
                item_id=str(input.item_id),
                media_entry_id=str(input.media_entry_id),
                success=False,
                error="no_changes_requested",
                applied_role=None,
                media_entry=None,
            )

        try:
            expires_at = (
                _parse_graphql_datetime(input.expires_at)
                if input.expires_at is not None
                else None
            )
        except ValueError:
            return GQLPersistMediaEntryControlResult(
                item_id=str(input.item_id),
                media_entry_id=str(input.media_entry_id),
                success=False,
                error="invalid_expires_at",
                applied_role=None,
                media_entry=None,
            )

        try:
            result = await playback_service.persist_media_entry_control_state(
                str(input.item_id),
                str(input.media_entry_id),
                active_role=(
                    input.active_role.value
                    if input.active_role is not None
                    else None
                ),
                local_path=input.local_path,
                download_url=input.download_url,
                unrestricted_url=input.unrestricted_url,
                refresh_state=input.refresh_state,
                last_refresh_error=input.last_refresh_error,
                expires_at=expires_at,
            )
        except ValueError as exc:
            return GQLPersistMediaEntryControlResult(
                item_id=str(input.item_id),
                media_entry_id=str(input.media_entry_id),
                success=False,
                error=str(exc),
                applied_role=None,
                media_entry=None,
            )

        if result is None:
            return GQLPersistMediaEntryControlResult(
                item_id=str(input.item_id),
                media_entry_id=str(input.media_entry_id),
                success=False,
                error="media_entry_not_found",
                applied_role=None,
                media_entry=None,
            )

        return _build_persisted_media_entry_control_result(result)

    @strawberry.mutation(
        description="Persist bounded playback-attachment URL/state changes through the shared playback service and sync linked media entries"
    )
    async def persist_playback_attachment_control_state(
        self,
        info: Info[GraphQLContext, object],
        input: PersistPlaybackAttachmentControlInput,
    ) -> GQLPersistPlaybackAttachmentControlResult:
        playback_service = info.context.resources.playback_service
        if playback_service is None:
            return GQLPersistPlaybackAttachmentControlResult(
                item_id=str(input.item_id),
                attachment_id=str(input.attachment_id),
                success=False,
                error="playback_service_unavailable",
                attachment=None,
                linked_media_entries=[],
            )

        if (
            input.locator is None
            and input.local_path is None
            and input.restricted_url is None
            and input.unrestricted_url is None
            and input.refresh_state is None
            and input.last_refresh_error is None
            and input.expires_at is None
        ):
            return GQLPersistPlaybackAttachmentControlResult(
                item_id=str(input.item_id),
                attachment_id=str(input.attachment_id),
                success=False,
                error="no_changes_requested",
                attachment=None,
                linked_media_entries=[],
            )

        try:
            expires_at = (
                _parse_graphql_datetime(input.expires_at)
                if input.expires_at is not None
                else None
            )
        except ValueError:
            return GQLPersistPlaybackAttachmentControlResult(
                item_id=str(input.item_id),
                attachment_id=str(input.attachment_id),
                success=False,
                error="invalid_expires_at",
                attachment=None,
                linked_media_entries=[],
            )

        try:
            result = await playback_service.persist_playback_attachment_control_state(
                str(input.item_id),
                str(input.attachment_id),
                locator=input.locator,
                local_path=input.local_path,
                restricted_url=input.restricted_url,
                unrestricted_url=input.unrestricted_url,
                refresh_state=input.refresh_state,
                last_refresh_error=input.last_refresh_error,
                expires_at=expires_at,
            )
        except ValueError as exc:
            return GQLPersistPlaybackAttachmentControlResult(
                item_id=str(input.item_id),
                attachment_id=str(input.attachment_id),
                success=False,
                error=str(exc),
                attachment=None,
                linked_media_entries=[],
            )

        if result is None:
            return GQLPersistPlaybackAttachmentControlResult(
                item_id=str(input.item_id),
                attachment_id=str(input.attachment_id),
                success=False,
                error="playback_attachment_not_found",
                attachment=None,
                linked_media_entries=[],
            )

        return _build_persisted_playback_attachment_control_result(result)

    @strawberry.mutation(
        description="Persist bounded VFS rollout-control overrides with audit and permission parity"
    )
    async def persist_vfs_rollout_control(
        self,
        info: Info[GraphQLContext, object],
        input: PersistVfsRolloutControlInput,
    ) -> GQLVfsRolloutControl:
        await require_graphql_permissions(
            info,
            "backend:admin",
            resource_scope="operations",
        )
        compat_routes = _compat_route_module()
        auth_context = compat_routes.get_auth_context(info.context.request)
        updates = {
            "environment_class": input.environment_class,
            "runtime_status_path": input.runtime_status_path,
            "promotion_paused": input.promotion_paused,
            "promotion_pause_reason": input.promotion_pause_reason,
            "promotion_pause_expires_at": input.promotion_pause_expires_at,
            "rollback_requested": input.rollback_requested,
            "rollback_reason": input.rollback_reason,
            "rollback_expires_at": input.rollback_expires_at,
            "notes": input.notes,
        }
        updates = {key: value for key, value in updates.items() if value is not None}
        try:
            compat_routes.persist_managed_windows_vfs_state(
                updates,
                actor_id=compat_routes._actor_key(auth_context),
            )
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        audit_action(
            info.context.request,
            action="operations.vfs_rollout.write_control",
            target="operations.vfs_rollout",
            details={
                "promotion_paused": input.promotion_paused,
                "promotion_pause_reason": input.promotion_pause_reason,
                "rollback_requested": input.rollback_requested,
                "rollback_reason": input.rollback_reason,
                "environment_class": input.environment_class,
            },
        )
        return _build_vfs_rollout_control(compat_routes._vfs_rollout_control_response())

    @strawberry.mutation(
        description="Remediate stale, fenced, or errored control-plane subscribers through the shared control plane"
    )
    async def remediate_control_plane_subscribers(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
    ) -> GQLControlPlaneRemediation:
        await require_graphql_permissions(
            info,
            "backend:admin",
            resource_scope="operations",
        )
        compat_routes = _compat_route_module()
        try:
            response = await compat_routes.remediate_control_plane_subscribers(
                info.context.request,
                active_within_seconds=max(1, min(active_within_seconds, 3600)),
            )
        except Exception as exc:
            _raise_graphql_compat_error(exc)
        return _build_control_plane_remediation(response)

    @strawberry.mutation(
        description="Recover stale control-plane delivery cursors back to their last acknowledged event"
    )
    async def recover_control_plane_ack_backlog(
        self,
        info: Info[GraphQLContext, object],
        active_within_seconds: int = 120,
    ) -> GQLControlPlaneAckRecovery:
        await require_graphql_permissions(
            info,
            "backend:admin",
            resource_scope="operations",
        )
        compat_routes = _compat_route_module()
        try:
            response = await compat_routes.recover_control_plane_ack_backlog(
                info.context.request,
                active_within_seconds=max(1, min(active_within_seconds, 3600)),
            )
        except Exception as exc:
            _raise_graphql_compat_error(exc)
        return _build_control_plane_ack_recovery(response)

    @strawberry.mutation(
        description="Claim stale replay pending entries into one operator recovery consumer"
    )
    async def recover_control_plane_pending_entries(
        self,
        info: Info[GraphQLContext, object],
        input: ControlPlanePendingRecoveryInput | None = None,
    ) -> GQLControlPlanePendingRecovery:
        await require_graphql_permissions(
            info,
            "backend:admin",
            resource_scope="operations",
        )
        compat_routes = _compat_route_module()
        recovery = input or ControlPlanePendingRecoveryInput()
        try:
            response = await compat_routes.recover_control_plane_pending_entries(
                info.context.request,
                group_name=recovery.group_name,
                consumer_name=recovery.consumer_name,
                min_idle_ms=max(1, min(recovery.min_idle_ms, 86_400_000)),
                claim_limit=max(1, min(recovery.claim_limit, 500)),
                active_within_seconds=max(1, min(recovery.active_within_seconds, 3600)),
            )
        except Exception as exc:
            _raise_graphql_compat_error(exc)
        return _build_control_plane_pending_recovery(response)

    @strawberry.mutation(
        description="Persist one access-policy revision through the shared control-plane service"
    )
    async def write_access_policy_revision(
        self,
        info: Info[GraphQLContext, object],
        input: AccessPolicyRevisionWriteInput,
    ) -> GQLAccessPolicyRevision:
        await require_graphql_permissions(
            info,
            "settings:write",
            resource_scope="access_policy",
        )
        compat_routes = _compat_route_module()
        try:
            payload = compat_routes.AccessPolicyRevisionWriteRequest(
                version=input.version,
                source=input.source,
                activate=input.activate,
                approval_notes=input.approval_notes,
                role_grants=input.role_grants,
                principal_roles=input.principal_roles,
                principal_scopes=input.principal_scopes,
                principal_tenant_grants=input.principal_tenant_grants,
                permission_constraints=input.permission_constraints,
                audit_decisions=input.audit_decisions,
                alerting_enabled=input.alerting_enabled,
                repeated_denial_warning_threshold=input.repeated_denial_warning_threshold,
                repeated_denial_critical_threshold=input.repeated_denial_critical_threshold,
            )
            response = await compat_routes.write_auth_policy_revision(
                info.context.request,
                payload,
            )
        except Exception as exc:
            _raise_graphql_compat_error(exc)
        await _invalidate_graphql_control_plane_cache(
            info,
            _GRAPHQL_ACCESS_POLICY_REVISIONS_CACHE_KEY,
            reason="access_policy_mutation",
        )
        return _build_access_policy_revision(response)

    @strawberry.mutation(
        description="Approve one persisted access-policy revision through the shared control plane"
    )
    async def approve_access_policy_revision(
        self,
        info: Info[GraphQLContext, object],
        version: str,
        input: AccessPolicyRevisionApprovalInput | None = None,
    ) -> GQLAccessPolicyRevision:
        await require_graphql_permissions(
            info,
            "security:policy.approve",
            resource_scope="access_policy",
        )
        compat_routes = _compat_route_module()
        approval = input or AccessPolicyRevisionApprovalInput()
        try:
            payload = compat_routes.AccessPolicyRevisionApprovalRequest(
                approval_notes=approval.approval_notes,
                activate=approval.activate,
            )
            response = await compat_routes.approve_auth_policy_revision(
                info.context.request,
                version,
                payload,
            )
        except Exception as exc:
            _raise_graphql_compat_error(exc)
        await _invalidate_graphql_control_plane_cache(
            info,
            _GRAPHQL_ACCESS_POLICY_REVISIONS_CACHE_KEY,
            reason="access_policy_mutation",
        )
        return _build_access_policy_revision(response)

    @strawberry.mutation(
        description="Reject one persisted access-policy revision through the shared control plane"
    )
    async def reject_access_policy_revision(
        self,
        info: Info[GraphQLContext, object],
        version: str,
        input: AccessPolicyRevisionApprovalInput | None = None,
    ) -> GQLAccessPolicyRevision:
        await require_graphql_permissions(
            info,
            "security:policy.approve",
            resource_scope="access_policy",
        )
        compat_routes = _compat_route_module()
        approval = input or AccessPolicyRevisionApprovalInput()
        try:
            payload = compat_routes.AccessPolicyRevisionApprovalRequest(
                approval_notes=approval.approval_notes,
                activate=approval.activate,
            )
            response = await compat_routes.reject_auth_policy_revision(
                info.context.request,
                version,
                payload,
            )
        except Exception as exc:
            _raise_graphql_compat_error(exc)
        await _invalidate_graphql_control_plane_cache(
            info,
            _GRAPHQL_ACCESS_POLICY_REVISIONS_CACHE_KEY,
            reason="access_policy_mutation",
        )
        return _build_access_policy_revision(response)

    @strawberry.mutation(
        description="Activate one persisted access-policy revision through the shared control plane"
    )
    async def activate_access_policy_revision(
        self,
        info: Info[GraphQLContext, object],
        version: str,
    ) -> GQLAccessPolicyRevision:
        await require_graphql_permissions(
            info,
            "settings:write",
            resource_scope="access_policy",
        )
        compat_routes = _compat_route_module()
        try:
            response = await compat_routes.activate_auth_policy_revision(
                info.context.request,
                version,
            )
        except Exception as exc:
            _raise_graphql_compat_error(exc)
        await _invalidate_graphql_control_plane_cache(
            info,
            _GRAPHQL_ACCESS_POLICY_REVISIONS_CACHE_KEY,
            reason="access_policy_mutation",
        )
        return _build_access_policy_revision(response)

    @strawberry.mutation(
        description="Persist one plugin-governance override through the shared control plane"
    )
    async def write_plugin_governance_override(
        self,
        info: Info[GraphQLContext, object],
        plugin_name: str,
        input: PluginGovernanceOverrideWriteInput,
    ) -> GQLPluginGovernanceOverride:
        await require_graphql_permissions(
            info,
            "settings:write",
            resource_scope="plugin_governance",
        )
        compat_routes = _compat_route_module()
        try:
            payload = compat_routes.PluginGovernanceOverrideWriteRequest(
                state=input.state,
                reason=input.reason,
                notes=input.notes,
            )
            response = await compat_routes.write_plugin_governance_override(
                info.context.request,
                plugin_name,
                payload,
            )
        except Exception as exc:
            _raise_graphql_compat_error(exc)
        await _invalidate_graphql_control_plane_cache(
            info,
            _GRAPHQL_PLUGIN_GOVERNANCE_CACHE_KEY,
            _GRAPHQL_PLUGIN_GOVERNANCE_OVERRIDES_CACHE_KEY,
            reason="plugin_governance_mutation",
        )
        return _build_plugin_governance_override(response)

    @strawberry.mutation(description="Update one compatibility settings path")
    async def update_setting(
        self,
        info: Info[GraphQLContext, object],
        input: SettingsUpdateInput,
    ) -> bool:
        await info.context.settings_updater(input.path, input.value)
        return True


async def _resolve_first_item_record(
    info: Info[GraphQLContext, object], result: ItemActionResult
) -> MediaItemRecord:
    if not result.ids:
        raise ValueError("request did not return any item identifiers")

    record = await info.context.media_service.get_item(result.ids[0])
    if record is None:
        raise ValueError(f"requested item {result.ids[0]} could not be resolved")

    return record


def _ensure_item_action_matched(result: ItemActionResult, item_id: str) -> None:
    if item_id not in result.ids:
        raise ValueError(f"unknown item_id={item_id}")


@strawberry.type
class CoreSubscriptionResolver:
    """Base subscription resolvers available without plugin registration."""

    @strawberry.subscription(description="Scaffold item state stream")
    async def item_state_changed(
        self,
        info: Info[GraphQLContext, object],
    ) -> AsyncGenerator[GQLItemEvent, None]:
        yield GQLItemEvent(item_id=strawberry.ID("0"), state="idle", message="subscription-ready")
        async for envelope in info.context.resources.event_bus.subscribe("item.state.changed"):
            payload = envelope.payload
            yield GQLItemEvent(
                item_id=strawberry.ID(str(payload.get("item_id", "0"))),
                state=str(payload.get("state", "unknown")),
                message=str(payload.get("message", "")),
            )
