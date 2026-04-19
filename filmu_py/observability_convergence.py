"""Shared observability convergence builder for operator and GraphQL surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from filmu_py.config import Settings
from filmu_py.observability_contract import (
    CORRELATION_HEADERS,
    REQUIRED_CROSS_PROCESS_HEADERS,
    TRACE_CONTEXT_HEADERS,
)

EXPECTED_CORRELATION_FIELDS = (
    "request.id",
    "trace.id",
    "tenant.id",
    "vfs.session_id",
    "vfs.daemon_id",
    "catalog.entry_id",
    "provider.file_id",
    "vfs.handle_key",
)
GRPC_SERVICE_NAME = "filmu.vfs.catalog.v1.FilmuVfsCatalogService"


@dataclass(frozen=True)
class ObservabilityPipelineStageSnapshot:
    """One typed observability pipeline stage for GraphQL/operator surfaces."""

    name: str
    status: str
    configured: bool
    ready: bool
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(frozen=True)
class ObservabilityConvergenceSnapshot:
    """Normalized cross-process log/search/trace convergence posture."""

    generated_at: str
    status: str
    structured_logging_enabled: bool
    structured_log_path: str
    otel_enabled: bool
    otel_endpoint_configured: bool
    log_shipper_enabled: bool
    log_shipper_type: str
    log_shipper_target_configured: bool
    log_shipper_healthcheck_configured: bool
    log_field_mapping_version: str
    search_backend: str
    environment_shipping_enabled: bool
    alerting_enabled: bool
    rust_trace_correlation_enabled: bool
    correlation_contract_complete: bool
    proof_refs: list[str]
    proof_ref_count: int
    required_correlation_fields: list[str]
    required_actions: list[str]
    remaining_gaps: list[str]
    trace_context_headers: list[str]
    correlation_headers: list[str]
    shared_cross_process_headers: list[str]
    expected_correlation_fields: list[str]
    expected_correlation_fields_ready: bool
    missing_expected_correlation_fields: list[str]
    grpc_bind_address: str
    grpc_service_name: str
    otlp_endpoint: str | None
    log_shipper_target: str | None
    environment_rollout_ready: bool
    alert_rollout_ready: bool
    pipeline_stages: list[ObservabilityPipelineStageSnapshot]


@dataclass(frozen=True)
class ObservabilityRolloutSummarySnapshot:
    """Compact rollout/evidence summary for Director operator cards."""

    generated_at: str
    status: str
    pipeline_stage_count: int
    ready_stage_count: int
    production_evidence_count: int
    production_evidence_ready: bool
    grpc_rust_trace_ready: bool
    otlp_export_ready: bool
    search_index_ready: bool
    alert_rollout_ready: bool
    ready_stage_names: list[str]
    blocked_stage_names: list[str]
    required_actions: list[str]
    remaining_gaps: list[str]


def structured_log_path(settings: Settings) -> str:
    """Return the normalized structured log path for operator and graph surfaces."""

    normalized_log_dir = settings.logging.directory.rstrip("/\\") or "logs"
    return f"{normalized_log_dir}/{settings.logging.structured_filename}"


def operator_log_pipeline_ready(settings: Settings) -> bool:
    """Return whether the repo-side operator log pipeline exit gates are satisfied."""

    observability_policy = settings.observability
    return bool(
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


def build_observability_convergence_snapshot(settings: Settings) -> ObservabilityConvergenceSnapshot:
    """Build one shared observability convergence snapshot from settings."""

    observability_policy = settings.observability
    ready = operator_log_pipeline_ready(settings)
    configured_fields = set(observability_policy.required_correlation_fields)
    missing_expected_correlation_fields = [
        field for field in EXPECTED_CORRELATION_FIELDS if field not in configured_fields
    ]
    expected_correlation_fields_ready = all(
        field in configured_fields for field in EXPECTED_CORRELATION_FIELDS
    )
    correlation_contract_complete = bool(observability_policy.required_correlation_fields)

    required_actions: list[str] = []
    remaining_gaps: list[str] = []
    if not settings.log_shipper.enabled:
        required_actions.append("configure_log_shipper_for_structured_ndjson")
        remaining_gaps.append("structured logs are not yet shipped out of process")
    elif not settings.log_shipper.healthcheck_url:
        required_actions.append("monitor_log_shipper_health")
        remaining_gaps.append("log shipper health is not externally checked")
    if not settings.log_shipper.target or observability_policy.search_backend == "none":
        required_actions.append("define_search_index_mapping_and_retention_policy")
        remaining_gaps.append("structured logs are not yet wired into a searchable backend")
    if not (settings.otel_enabled and settings.otel_exporter_otlp_endpoint):
        required_actions.append("configure_otlp_trace_export")
        remaining_gaps.append("cross-process traces are not exported through OTLP")
    if not observability_policy.environment_shipping_enabled:
        required_actions.append("enable_environment_log_shipping")
        remaining_gaps.append("environment-managed log shipping is not enabled")
    if not observability_policy.alerting_enabled:
        required_actions.append("enable_alerting_for_log_search_and_trace_pipeline")
        remaining_gaps.append("search/trace alerting is not configured")
    if not observability_policy.rust_trace_correlation_enabled:
        required_actions.append("wire_rust_trace_correlation_fields")
        remaining_gaps.append("Python and Rust traces are not yet forced onto one correlation contract")
    if not correlation_contract_complete:
        required_actions.append("define_required_cross_process_correlation_fields")
        remaining_gaps.append("required correlation fields are not configured")
    if not expected_correlation_fields_ready:
        required_actions.append("expand_cross_process_correlation_field_contract")
        remaining_gaps.append("shared Python/Rust correlation fields are not fully represented in policy")
    if not observability_policy.proof_refs:
        required_actions.append("record_log_pipeline_rollout_evidence")
        remaining_gaps.append("observability convergence has no retained rollout evidence references")

    status = (
        "ready"
        if ready
        else (
            "partial"
            if settings.logging.enabled or settings.otel_enabled or settings.log_shipper.enabled
            else "blocked"
        )
    )
    pipeline_stages = [
        ObservabilityPipelineStageSnapshot(
            name="python_structured_logging",
            status="ready" if settings.logging.enabled else "blocked",
            configured=settings.logging.enabled,
            ready=settings.logging.enabled,
            required_actions=([] if settings.logging.enabled else ["enable_structured_logging"]),
            remaining_gaps=(
                []
                if settings.logging.enabled
                else ["structured logging is not enabled for the Python API runtime"]
            ),
        ),
        ObservabilityPipelineStageSnapshot(
            name="grpc_rust_correlation",
            status=(
                "ready"
                if observability_policy.rust_trace_correlation_enabled
                and expected_correlation_fields_ready
                else "partial"
                if bool(observability_policy.required_correlation_fields)
                else "blocked"
            ),
            configured=bool(observability_policy.required_correlation_fields),
            ready=bool(
                observability_policy.rust_trace_correlation_enabled
                and expected_correlation_fields_ready
            ),
            required_actions=(
                []
                if observability_policy.rust_trace_correlation_enabled
                and expected_correlation_fields_ready
                else ["wire_rust_trace_correlation_fields"]
            ),
            remaining_gaps=(
                []
                if observability_policy.rust_trace_correlation_enabled
                and expected_correlation_fields_ready
                else [
                    "Python request scope and Rust gRPC handlers are not yet proven on one full correlation contract"
                ]
            ),
        ),
        ObservabilityPipelineStageSnapshot(
            name="otlp_export",
            status=(
                "ready"
                if settings.otel_enabled and bool(settings.otel_exporter_otlp_endpoint)
                else "blocked"
            ),
            configured=settings.otel_enabled or bool(settings.otel_exporter_otlp_endpoint),
            ready=settings.otel_enabled and bool(settings.otel_exporter_otlp_endpoint),
            required_actions=(
                []
                if settings.otel_enabled and bool(settings.otel_exporter_otlp_endpoint)
                else ["configure_otlp_trace_export"]
            ),
            remaining_gaps=(
                []
                if settings.otel_enabled and bool(settings.otel_exporter_otlp_endpoint)
                else ["OTLP export is not configured for cross-process traces"]
            ),
        ),
        ObservabilityPipelineStageSnapshot(
            name="log_shipping_and_search",
            status=(
                "ready"
                if settings.log_shipper.enabled
                and bool(settings.log_shipper.target)
                and observability_policy.search_backend != "none"
                else "partial"
                if settings.log_shipper.enabled or observability_policy.search_backend != "none"
                else "blocked"
            ),
            configured=bool(settings.log_shipper.enabled or observability_policy.search_backend != "none"),
            ready=bool(
                settings.log_shipper.enabled
                and bool(settings.log_shipper.target)
                and observability_policy.search_backend != "none"
            ),
            required_actions=(
                []
                if settings.log_shipper.enabled
                and bool(settings.log_shipper.target)
                and observability_policy.search_backend != "none"
                else ["define_search_index_mapping_and_retention_policy"]
            ),
            remaining_gaps=(
                []
                if settings.log_shipper.enabled
                and bool(settings.log_shipper.target)
                and observability_policy.search_backend != "none"
                else ["structured logs are not yet shipped into a searchable backend"]
            ),
        ),
        ObservabilityPipelineStageSnapshot(
            name="alerting_and_rollout_evidence",
            status=(
                "ready"
                if observability_policy.alerting_enabled and bool(observability_policy.proof_refs)
                else "partial"
                if observability_policy.alerting_enabled or bool(observability_policy.proof_refs)
                else "blocked"
            ),
            configured=bool(observability_policy.alerting_enabled or observability_policy.proof_refs),
            ready=bool(observability_policy.alerting_enabled and observability_policy.proof_refs),
            required_actions=(
                []
                if observability_policy.alerting_enabled and bool(observability_policy.proof_refs)
                else ["enable_alerting_for_log_search_and_trace_pipeline", "record_log_pipeline_rollout_evidence"]
            ),
            remaining_gaps=(
                []
                if observability_policy.alerting_enabled and bool(observability_policy.proof_refs)
                else ["alerting or retained rollout evidence is still incomplete for observability convergence"]
            ),
        ),
    ]
    return ObservabilityConvergenceSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        status=status,
        structured_logging_enabled=settings.logging.enabled,
        structured_log_path=structured_log_path(settings),
        otel_enabled=settings.otel_enabled,
        otel_endpoint_configured=bool(settings.otel_exporter_otlp_endpoint),
        log_shipper_enabled=settings.log_shipper.enabled,
        log_shipper_type=settings.log_shipper.type,
        log_shipper_target_configured=bool(settings.log_shipper.target),
        log_shipper_healthcheck_configured=bool(settings.log_shipper.healthcheck_url),
        log_field_mapping_version=settings.log_shipper.field_mapping_version,
        search_backend=observability_policy.search_backend,
        environment_shipping_enabled=observability_policy.environment_shipping_enabled,
        alerting_enabled=observability_policy.alerting_enabled,
        rust_trace_correlation_enabled=observability_policy.rust_trace_correlation_enabled,
        correlation_contract_complete=correlation_contract_complete,
        proof_refs=list(observability_policy.proof_refs),
        proof_ref_count=len([ref for ref in observability_policy.proof_refs if str(ref).strip()]),
        required_correlation_fields=list(observability_policy.required_correlation_fields),
        required_actions=required_actions,
        remaining_gaps=remaining_gaps,
        trace_context_headers=list(TRACE_CONTEXT_HEADERS),
        correlation_headers=list(CORRELATION_HEADERS),
        shared_cross_process_headers=list(REQUIRED_CROSS_PROCESS_HEADERS),
        expected_correlation_fields=list(EXPECTED_CORRELATION_FIELDS),
        expected_correlation_fields_ready=expected_correlation_fields_ready,
        missing_expected_correlation_fields=missing_expected_correlation_fields,
        grpc_bind_address=settings.grpc_bind_address,
        grpc_service_name=GRPC_SERVICE_NAME,
        otlp_endpoint=settings.otel_exporter_otlp_endpoint,
        log_shipper_target=settings.log_shipper.target,
        environment_rollout_ready=bool(
            settings.log_shipper.enabled
            and bool(settings.log_shipper.target)
            and observability_policy.environment_shipping_enabled
            and observability_policy.search_backend != "none"
        ),
        alert_rollout_ready=bool(
            observability_policy.alerting_enabled and observability_policy.proof_refs
        ),
        pipeline_stages=pipeline_stages,
    )


def build_observability_rollout_summary(
    settings: Settings,
) -> ObservabilityRolloutSummarySnapshot:
    """Return one compact summary over the full observability convergence snapshot."""

    snapshot = build_observability_convergence_snapshot(settings)
    ready_stage_names = [stage.name for stage in snapshot.pipeline_stages if stage.ready]
    blocked_stage_names = [
        stage.name for stage in snapshot.pipeline_stages if not stage.ready and stage.status == "blocked"
    ]
    production_evidence_refs = [ref for ref in snapshot.proof_refs if str(ref).strip()]
    return ObservabilityRolloutSummarySnapshot(
        generated_at=snapshot.generated_at,
        status=snapshot.status,
        pipeline_stage_count=len(snapshot.pipeline_stages),
        ready_stage_count=len(ready_stage_names),
        production_evidence_count=len(production_evidence_refs),
        production_evidence_ready=bool(production_evidence_refs),
        grpc_rust_trace_ready=bool(
            snapshot.rust_trace_correlation_enabled and snapshot.expected_correlation_fields_ready
        ),
        otlp_export_ready=bool(snapshot.otel_enabled and snapshot.otel_endpoint_configured),
        search_index_ready=bool(
            snapshot.log_shipper_enabled
            and snapshot.log_shipper_target_configured
            and snapshot.search_backend != "none"
        ),
        alert_rollout_ready=bool(snapshot.alerting_enabled and production_evidence_refs),
        ready_stage_names=ready_stage_names,
        blocked_stage_names=blocked_stage_names,
        required_actions=list(snapshot.required_actions),
        remaining_gaps=list(snapshot.remaining_gaps),
    )
