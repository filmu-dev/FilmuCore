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
    search_backend: str
    environment_shipping_enabled: bool
    alerting_enabled: bool
    rust_trace_correlation_enabled: bool
    correlation_contract_complete: bool
    proof_refs: list[str]
    required_correlation_fields: list[str]
    required_actions: list[str]
    remaining_gaps: list[str]
    trace_context_headers: list[str]
    correlation_headers: list[str]
    shared_cross_process_headers: list[str]
    expected_correlation_fields: list[str]
    expected_correlation_fields_ready: bool


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
        search_backend=observability_policy.search_backend,
        environment_shipping_enabled=observability_policy.environment_shipping_enabled,
        alerting_enabled=observability_policy.alerting_enabled,
        rust_trace_correlation_enabled=observability_policy.rust_trace_correlation_enabled,
        correlation_contract_complete=correlation_contract_complete,
        proof_refs=list(observability_policy.proof_refs),
        required_correlation_fields=list(observability_policy.required_correlation_fields),
        required_actions=required_actions,
        remaining_gaps=remaining_gaps,
        trace_context_headers=list(TRACE_CONTEXT_HEADERS),
        correlation_headers=list(CORRELATION_HEADERS),
        shared_cross_process_headers=list(REQUIRED_CROSS_PROCESS_HEADERS),
        expected_correlation_fields=list(EXPECTED_CORRELATION_FIELDS),
        expected_correlation_fields_ready=expected_correlation_fields_ready,
    )
