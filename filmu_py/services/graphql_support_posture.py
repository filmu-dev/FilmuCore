"""Shared GraphQL support-summary builders for runtime and migration surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise
from typing import Literal

from filmu_py.core.queue_status import DeadLetterSample, QueueStatusReader
from filmu_py.observability_convergence import (
    EXPECTED_CORRELATION_FIELDS,
    build_observability_convergence_snapshot,
)
from filmu_py.resources import AppResources
from filmu_py.services.governance_posture import (
    GovernanceStatusCountSnapshot,
    OperatorActionItemSnapshot,
    OperatorGapItemSnapshot,
    build_plugin_runtime_action_items,
    build_plugin_runtime_gap_items,
    build_plugin_runtime_rows,
)
from filmu_py.services.operator_posture import (
    ProofArtifactSnapshot,
    build_control_plane_automation_posture,
    build_control_plane_recovery_readiness_posture,
    build_control_plane_replay_backplane_posture,
    build_control_plane_subscribers_posture,
    build_control_plane_summary_posture,
    build_downloader_execution_evidence_posture,
    build_downloader_orchestration_posture,
    build_plugin_governance_posture,
    build_vfs_mount_diagnostics_posture,
)
from filmu_py.services.vfs_catalog import (
    VfsCatalogDelta,
    VfsCatalogRemoval,
    VfsCatalogSnapshot,
    summarize_vfs_catalog_delta,
)


@dataclass(slots=True)
class NamedCountSnapshot:
    key: str
    count: int


@dataclass(slots=True)
class ObservabilityFieldContractSummarySnapshot:
    total_required_correlation_fields: int
    expected_field_count: int
    configured_expected_field_count: int
    missing_expected_field_count: int
    trace_context_header_count: int
    correlation_header_count: int
    shared_header_count: int


@dataclass(slots=True)
class ControlPlaneConsumerSummarySnapshot:
    consumer_name: str
    subscriber_count: int
    active_subscribers: int
    ack_pending_subscribers: int
    fenced_subscribers: int
    error_subscribers: int
    latest_heartbeat_at: str | None


@dataclass(slots=True)
class ControlPlaneOwnershipSummarySnapshot:
    total_subscribers: int
    active_subscribers: int
    stale_subscribers: int
    error_subscribers: int
    fenced_subscribers: int
    ack_pending_subscribers: int
    unique_consumers: int
    unique_nodes: int
    unique_tenants: int


@dataclass(slots=True)
class DownloaderDeadLetterTimelinePointSnapshot:
    bucket_at: str
    sample_count: int
    provider_counts: dict[str, int]
    reason_code_counts: dict[str, int]
    failure_kind_counts: dict[str, int]


@dataclass(slots=True)
class DownloaderFailureKindSummarySnapshot:
    failure_kind: str
    sample_count: int
    provider_counts: dict[str, int]
    reason_code_counts: dict[str, int]


@dataclass(slots=True)
class DownloaderStatusCodeSummarySnapshot:
    status_code: int
    sample_count: int
    provider_counts: dict[str, int]
    reason_code_counts: dict[str, int]


@dataclass(slots=True)
class PluginRuntimePublisherSummarySnapshot:
    publisher: str
    plugin_count: int
    ready_plugins: int
    quarantined_plugins: int
    warning_count: int
    capability_counts: dict[str, int]


@dataclass(slots=True)
class VfsCatalogDeltaHistorySummarySnapshot:
    delta_count: int
    max_upsert_count: int
    max_removal_count: int
    total_upsert_count: int
    total_removal_count: int
    total_upsert_file_count: int
    total_removal_file_count: int
    provider_family_counts: dict[str, int]
    lease_state_counts: dict[str, int]


Severity = Literal["warning", "critical"]


def _queue_name(resources: AppResources) -> str:
    return resources.arq_queue_name or resources.settings.arq_queue_name


def _queue_redis(resources: AppResources) -> object:
    return resources.arq_redis or resources.redis


def _status_counts_from_values(values: list[str]) -> list[GovernanceStatusCountSnapshot]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return [
        GovernanceStatusCountSnapshot(status=status, count=count)
        for status, count in sorted(counts.items())
    ]


def _named_counts(counts: dict[str, int]) -> list[NamedCountSnapshot]:
    return [NamedCountSnapshot(key=key, count=count) for key, count in sorted(counts.items())]


def _severity_for_status(status: str) -> Severity:
    return "critical" if status in {"blocked", "degraded"} else "warning"


def _bucket_time(value: str, *, bucket_minutes: int) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    floored_minute = (parsed.minute // bucket_minutes) * bucket_minutes
    bucket = parsed.replace(minute=floored_minute, second=0, microsecond=0)
    return bucket.isoformat()


def _normalize_datetime_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        normalized_dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return normalized_dt.isoformat()
    normalized_text = str(value).strip()
    return normalized_text or None


def _provider_name(sample: DeadLetterSample) -> str:
    provider = sample.metadata.get("provider")
    return str(provider).strip() if isinstance(provider, str) and str(provider).strip() else "unknown"


def _failure_kind_name(sample: DeadLetterSample) -> str:
    failure_kind = sample.metadata.get("failure_kind")
    return (
        str(failure_kind).strip()
        if isinstance(failure_kind, str) and str(failure_kind).strip()
        else "unknown"
    )


async def _downloader_dead_letter_samples(
    resources: AppResources,
    *,
    limit: int,
) -> list[DeadLetterSample]:
    reader = QueueStatusReader(_queue_redis(resources), queue_name=_queue_name(resources))
    return await reader.dead_letter_samples(limit=max(1, min(limit, 200)), stage="debrid_item")


def build_observability_field_contract_summary(
    resources: AppResources,
) -> ObservabilityFieldContractSummarySnapshot:
    snapshot = build_observability_convergence_snapshot(resources.settings)
    configured_fields = set(snapshot.required_correlation_fields)
    configured_expected_field_count = sum(
        1 for field in EXPECTED_CORRELATION_FIELDS if field in configured_fields
    )
    return ObservabilityFieldContractSummarySnapshot(
        total_required_correlation_fields=len(snapshot.required_correlation_fields),
        expected_field_count=len(snapshot.expected_correlation_fields),
        configured_expected_field_count=configured_expected_field_count,
        missing_expected_field_count=len(snapshot.missing_expected_correlation_fields),
        trace_context_header_count=len(snapshot.trace_context_headers),
        correlation_header_count=len(snapshot.correlation_headers),
        shared_header_count=len(snapshot.shared_cross_process_headers),
    )


def build_observability_stage_counts(
    resources: AppResources,
) -> list[GovernanceStatusCountSnapshot]:
    snapshot = build_observability_convergence_snapshot(resources.settings)
    return _status_counts_from_values([stage.status for stage in snapshot.pipeline_stages])


def build_observability_proof_inventory(resources: AppResources) -> list[ProofArtifactSnapshot]:
    snapshot = build_observability_convergence_snapshot(resources.settings)
    return [
        ProofArtifactSnapshot(
            ref=ref,
            category="observability_rollout",
            label="observability rollout proof",
            recorded=True,
        )
        for ref in snapshot.proof_refs
        if str(ref).strip()
    ]


def build_observability_action_items(
    resources: AppResources,
) -> list[OperatorActionItemSnapshot]:
    snapshot = build_observability_convergence_snapshot(resources.settings)
    actions: dict[tuple[str, str, str], OperatorActionItemSnapshot] = {}
    for stage in snapshot.pipeline_stages:
        severity = _severity_for_status(stage.status)
        for action in stage.required_actions:
            actions[("observability", stage.name, action)] = OperatorActionItemSnapshot(
                domain="observability",
                subject=stage.name,
                severity=severity,
                status=stage.status,
                action=action,
            )
    for action in snapshot.required_actions:
        actions[("observability", "observability_convergence", action)] = (
            OperatorActionItemSnapshot(
                domain="observability",
                subject="observability_convergence",
                severity=_severity_for_status(snapshot.status),
                status=snapshot.status,
                action=action,
            )
        )
    return list(actions.values())


def build_observability_gap_items(
    resources: AppResources,
) -> list[OperatorGapItemSnapshot]:
    snapshot = build_observability_convergence_snapshot(resources.settings)
    gaps: dict[tuple[str, str, str], OperatorGapItemSnapshot] = {}
    for stage in snapshot.pipeline_stages:
        severity = _severity_for_status(stage.status)
        for message in stage.remaining_gaps:
            gaps[("observability", stage.name, message)] = OperatorGapItemSnapshot(
                domain="observability",
                subject=stage.name,
                severity=severity,
                status=stage.status,
                message=message,
            )
    for message in snapshot.remaining_gaps:
        gaps[("observability", "observability_convergence", message)] = (
            OperatorGapItemSnapshot(
                domain="observability",
                subject="observability_convergence",
                severity=_severity_for_status(snapshot.status),
                status=snapshot.status,
                message=message,
            )
        )
    return list(gaps.values())


async def build_control_plane_status_counts(
    resources: AppResources,
    *,
    active_within_seconds: int = 120,
) -> list[NamedCountSnapshot]:
    summary = await build_control_plane_summary_posture(
        resources,
        active_within_seconds=active_within_seconds,
    )
    return _named_counts(summary.status_counts)


async def build_control_plane_action_items(
    resources: AppResources,
    *,
    active_within_seconds: int = 120,
) -> list[OperatorActionItemSnapshot]:
    summary = await build_control_plane_summary_posture(
        resources,
        active_within_seconds=active_within_seconds,
    )
    automation = await build_control_plane_automation_posture(resources)
    replay = await build_control_plane_replay_backplane_posture(resources)
    recovery = await build_control_plane_recovery_readiness_posture(
        resources,
        active_within_seconds=active_within_seconds,
    )
    rows: dict[tuple[str, str, str], OperatorActionItemSnapshot] = {}
    snapshots = (
        ("control_plane_summary", summary.required_actions, "partial" if summary.total_subscribers else "blocked"),
        ("control_plane_automation", automation.required_actions, automation.runner_status),
        ("control_plane_replay", replay.required_actions, replay.status),
        ("control_plane_recovery", recovery.required_actions, recovery.status),
    )
    for subject, actions, status in snapshots:
        severity = _severity_for_status(status)
        normalized_status = "blocked" if status == "degraded" else status
        for action in actions:
            rows[("control_plane", subject, action)] = OperatorActionItemSnapshot(
                domain="control_plane",
                subject=subject,
                severity=severity,
                status=normalized_status,
                action=action,
            )
    return list(rows.values())


async def build_control_plane_gap_items(
    resources: AppResources,
    *,
    active_within_seconds: int = 120,
) -> list[OperatorGapItemSnapshot]:
    summary = await build_control_plane_summary_posture(
        resources,
        active_within_seconds=active_within_seconds,
    )
    automation = await build_control_plane_automation_posture(resources)
    replay = await build_control_plane_replay_backplane_posture(resources)
    recovery = await build_control_plane_recovery_readiness_posture(
        resources,
        active_within_seconds=active_within_seconds,
    )
    rows: dict[tuple[str, str, str], OperatorGapItemSnapshot] = {}
    snapshots = (
        ("control_plane_summary", summary.remaining_gaps, "partial" if summary.total_subscribers else "blocked"),
        ("control_plane_automation", automation.remaining_gaps, automation.runner_status),
        ("control_plane_replay", replay.remaining_gaps, replay.status),
        ("control_plane_recovery", recovery.remaining_gaps, recovery.status),
    )
    for subject, gaps, status in snapshots:
        severity = _severity_for_status(status)
        normalized_status = "blocked" if status == "degraded" else status
        for message in gaps:
            rows[("control_plane", subject, message)] = OperatorGapItemSnapshot(
                domain="control_plane",
                subject=subject,
                severity=severity,
                status=normalized_status,
                message=message,
            )
    return list(rows.values())


async def build_control_plane_consumer_summaries(
    resources: AppResources,
    *,
    active_within_seconds: int = 120,
) -> list[ControlPlaneConsumerSummarySnapshot]:
    subscribers = await build_control_plane_subscribers_posture(
        resources,
        active_within_seconds=active_within_seconds,
    )
    grouped: dict[str, ControlPlaneConsumerSummarySnapshot] = {}
    for row in subscribers:
        summary = grouped.get(row.consumer_name)
        if summary is None:
            summary = ControlPlaneConsumerSummarySnapshot(
                consumer_name=row.consumer_name,
                subscriber_count=0,
                active_subscribers=0,
                ack_pending_subscribers=0,
                fenced_subscribers=0,
                error_subscribers=0,
                latest_heartbeat_at=None,
            )
            grouped[row.consumer_name] = summary
        summary.subscriber_count += 1
        summary.active_subscribers += int(row.status == "active")
        summary.ack_pending_subscribers += int(row.ack_pending)
        summary.fenced_subscribers += int(row.fenced)
        summary.error_subscribers += int(row.status == "error")
        heartbeat = _normalize_datetime_value(row.last_heartbeat_at)
        if heartbeat is not None and (
            summary.latest_heartbeat_at is None or heartbeat > summary.latest_heartbeat_at
        ):
            summary.latest_heartbeat_at = heartbeat
    return [grouped[key] for key in sorted(grouped)]


async def build_control_plane_node_counts(
    resources: AppResources,
    *,
    active_within_seconds: int = 120,
) -> list[NamedCountSnapshot]:
    subscribers = await build_control_plane_subscribers_posture(
        resources,
        active_within_seconds=active_within_seconds,
    )
    counts: dict[str, int] = {}
    for row in subscribers:
        counts[row.node_id] = counts.get(row.node_id, 0) + 1
    return _named_counts(counts)


async def build_control_plane_tenant_counts(
    resources: AppResources,
    *,
    active_within_seconds: int = 120,
) -> list[NamedCountSnapshot]:
    subscribers = await build_control_plane_subscribers_posture(
        resources,
        active_within_seconds=active_within_seconds,
    )
    counts: dict[str, int] = {}
    for row in subscribers:
        tenant = row.tenant_id or "global"
        counts[tenant] = counts.get(tenant, 0) + 1
    return _named_counts(counts)


async def build_control_plane_ownership_summary(
    resources: AppResources,
    *,
    active_within_seconds: int = 120,
) -> ControlPlaneOwnershipSummarySnapshot:
    subscribers = await build_control_plane_subscribers_posture(
        resources,
        active_within_seconds=active_within_seconds,
    )
    return ControlPlaneOwnershipSummarySnapshot(
        total_subscribers=len(subscribers),
        active_subscribers=sum(1 for row in subscribers if row.status == "active"),
        stale_subscribers=sum(1 for row in subscribers if row.status == "stale"),
        error_subscribers=sum(1 for row in subscribers if row.status == "error"),
        fenced_subscribers=sum(1 for row in subscribers if row.fenced),
        ack_pending_subscribers=sum(1 for row in subscribers if row.ack_pending),
        unique_consumers=len({row.consumer_name for row in subscribers}),
        unique_nodes=len({row.node_id for row in subscribers}),
        unique_tenants=len({row.tenant_id or "global" for row in subscribers}),
    )


async def build_control_plane_replay_consumer_counts(
    resources: AppResources,
) -> list[NamedCountSnapshot]:
    replay = await build_control_plane_replay_backplane_posture(resources)
    return _named_counts(replay.consumer_counts)


async def build_downloader_alert_level_counts(
    resources: AppResources,
    *,
    limit: int = 20,
) -> list[NamedCountSnapshot]:
    history = await QueueStatusReader(
        _queue_redis(resources),
        queue_name=_queue_name(resources),
    ).history(limit=max(1, min(limit, 100)))
    counts: dict[str, int] = {}
    for point in history:
        counts[point.alert_level] = counts.get(point.alert_level, 0) + 1
    return _named_counts(counts)


async def build_downloader_dead_letter_timeline(
    resources: AppResources,
    *,
    limit: int = 50,
    bucket_minutes: int = 60,
    provider: str | None = None,
    reason_code: str | None = None,
    failure_kind: str | None = None,
) -> list[DownloaderDeadLetterTimelinePointSnapshot]:
    bounded_bucket_minutes = max(1, min(bucket_minutes, 60 * 24))
    samples = await _downloader_dead_letter_samples(resources, limit=limit)
    grouped: dict[str, DownloaderDeadLetterTimelinePointSnapshot] = {}
    for sample in samples:
        provider_name = _provider_name(sample)
        failure_kind_name = _failure_kind_name(sample)
        if provider is not None and provider_name != provider:
            continue
        if reason_code is not None and sample.reason_code != reason_code:
            continue
        if failure_kind is not None and failure_kind_name != failure_kind:
            continue
        bucket_at = _bucket_time(sample.queued_at, bucket_minutes=bounded_bucket_minutes)
        row = grouped.get(bucket_at)
        if row is None:
            row = DownloaderDeadLetterTimelinePointSnapshot(
                bucket_at=bucket_at,
                sample_count=0,
                provider_counts={},
                reason_code_counts={},
                failure_kind_counts={},
            )
            grouped[bucket_at] = row
        row.sample_count += 1
        row.provider_counts[provider_name] = row.provider_counts.get(provider_name, 0) + 1
        row.reason_code_counts[sample.reason_code] = row.reason_code_counts.get(sample.reason_code, 0) + 1
        row.failure_kind_counts[failure_kind_name] = row.failure_kind_counts.get(failure_kind_name, 0) + 1
    return [grouped[key] for key in sorted(grouped, reverse=True)]


async def build_downloader_failure_kind_summaries(
    resources: AppResources,
    *,
    limit: int = 50,
    provider: str | None = None,
    reason_code: str | None = None,
) -> list[DownloaderFailureKindSummarySnapshot]:
    samples = await _downloader_dead_letter_samples(resources, limit=limit)
    grouped: dict[str, DownloaderFailureKindSummarySnapshot] = {}
    for sample in samples:
        provider_name = _provider_name(sample)
        if provider is not None and provider_name != provider:
            continue
        if reason_code is not None and sample.reason_code != reason_code:
            continue
        failure_kind = _failure_kind_name(sample)
        row = grouped.get(failure_kind)
        if row is None:
            row = DownloaderFailureKindSummarySnapshot(
                failure_kind=failure_kind,
                sample_count=0,
                provider_counts={},
                reason_code_counts={},
            )
            grouped[failure_kind] = row
        row.sample_count += 1
        row.provider_counts[provider_name] = row.provider_counts.get(provider_name, 0) + 1
        row.reason_code_counts[sample.reason_code] = row.reason_code_counts.get(sample.reason_code, 0) + 1
    return [grouped[key] for key in sorted(grouped)]


async def build_downloader_status_code_summaries(
    resources: AppResources,
    *,
    limit: int = 50,
    provider: str | None = None,
    reason_code: str | None = None,
) -> list[DownloaderStatusCodeSummarySnapshot]:
    samples = await _downloader_dead_letter_samples(resources, limit=limit)
    grouped: dict[int, DownloaderStatusCodeSummarySnapshot] = {}
    for sample in samples:
        provider_name = _provider_name(sample)
        if provider is not None and provider_name != provider:
            continue
        if reason_code is not None and sample.reason_code != reason_code:
            continue
        status_code = sample.metadata.get("status_code")
        if isinstance(status_code, bool) or not isinstance(status_code, int):
            continue
        row = grouped.get(status_code)
        if row is None:
            row = DownloaderStatusCodeSummarySnapshot(
                status_code=status_code,
                sample_count=0,
                provider_counts={},
                reason_code_counts={},
            )
            grouped[status_code] = row
        row.sample_count += 1
        row.provider_counts[provider_name] = row.provider_counts.get(provider_name, 0) + 1
        row.reason_code_counts[sample.reason_code] = row.reason_code_counts.get(sample.reason_code, 0) + 1
    return [grouped[key] for key in sorted(grouped)]


async def build_downloader_action_items(resources: AppResources) -> list[OperatorActionItemSnapshot]:
    orchestration = build_downloader_orchestration_posture(resources)
    evidence = await build_downloader_execution_evidence_posture(resources)
    rows: dict[tuple[str, str, str], OperatorActionItemSnapshot] = {}
    for subject, status, actions in (
        (
            "downloader_orchestration",
            "partial" if orchestration.configured_provider_count else "blocked",
            orchestration.required_actions,
        ),
        ("downloader_execution", evidence.status, evidence.required_actions),
    ):
        severity = _severity_for_status(status)
        for action in actions:
            rows[("downloader", subject, action)] = OperatorActionItemSnapshot(
                domain="downloader",
                subject=subject,
                severity=severity,
                status=status,
                action=action,
            )
    return list(rows.values())


async def build_downloader_gap_items(resources: AppResources) -> list[OperatorGapItemSnapshot]:
    orchestration = build_downloader_orchestration_posture(resources)
    evidence = await build_downloader_execution_evidence_posture(resources)
    rows: dict[tuple[str, str, str], OperatorGapItemSnapshot] = {}
    for subject, status, gaps in (
        (
            "downloader_orchestration",
            "partial" if orchestration.configured_provider_count else "blocked",
            orchestration.remaining_gaps,
        ),
        ("downloader_execution", evidence.status, evidence.remaining_gaps),
    ):
        severity = _severity_for_status(status)
        for message in gaps:
            rows[("downloader", subject, message)] = OperatorGapItemSnapshot(
                domain="downloader",
                subject=subject,
                severity=severity,
                status=status,
                message=message,
            )
    return list(rows.values())


async def build_plugin_runtime_status_counts(
    resources: AppResources,
    *,
    app_state: object,
) -> list[GovernanceStatusCountSnapshot]:
    rows = await build_plugin_runtime_rows(resources, app_state=app_state)
    return _status_counts_from_values([row.status for row in rows])


async def build_plugin_runtime_wiring_status_counts(
    resources: AppResources,
    *,
    app_state: object,
) -> list[NamedCountSnapshot]:
    rows = await build_plugin_runtime_rows(resources, app_state=app_state)
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.wiring_status] = counts.get(row.wiring_status, 0) + 1
    return _named_counts(counts)


async def build_plugin_runtime_publisher_summaries(
    resources: AppResources,
    *,
    app_state: object,
) -> list[PluginRuntimePublisherSummarySnapshot]:
    rows = await build_plugin_runtime_rows(resources, app_state=app_state)
    governance = await build_plugin_governance_posture(resources, app_state=app_state)
    row_by_name = {row.name: row for row in rows}
    grouped: dict[str, PluginRuntimePublisherSummarySnapshot] = {}
    for plugin in governance.plugins:
        publisher = plugin.publisher or "unknown"
        row = row_by_name.get(plugin.name)
        summary = grouped.get(publisher)
        if summary is None:
            summary = PluginRuntimePublisherSummarySnapshot(
                publisher=publisher,
                plugin_count=0,
                ready_plugins=0,
                quarantined_plugins=0,
                warning_count=0,
                capability_counts={},
            )
            grouped[publisher] = summary
        summary.plugin_count += 1
        summary.ready_plugins += int(bool(row.ready) if row is not None else plugin.ready)
        summary.quarantined_plugins += int(plugin.quarantined)
        summary.warning_count += row.warning_count if row is not None else len(plugin.warnings)
        for capability in plugin.capabilities:
            summary.capability_counts[capability] = summary.capability_counts.get(capability, 0) + 1
    return [grouped[key] for key in sorted(grouped)]


def _capability_count_rows(
    rows: list[OperatorActionItemSnapshot] | list[OperatorGapItemSnapshot],
) -> list[NamedCountSnapshot]:
    counts: dict[str, int] = {}
    for row in rows:
        capability = row.capability_kind or "unknown"
        counts[capability] = counts.get(capability, 0) + 1
    return _named_counts(counts)


async def build_plugin_runtime_capability_action_counts(
    resources: AppResources,
    *,
    app_state: object,
) -> list[NamedCountSnapshot]:
    rows = await build_plugin_runtime_rows(resources, app_state=app_state)
    return _capability_count_rows(build_plugin_runtime_action_items(rows))


async def build_plugin_runtime_capability_gap_counts(
    resources: AppResources,
    *,
    app_state: object,
) -> list[NamedCountSnapshot]:
    rows = await build_plugin_runtime_rows(resources, app_state=app_state)
    return _capability_count_rows(build_plugin_runtime_gap_items(rows))


def _build_delta_between_snapshots(
    previous: VfsCatalogSnapshot,
    current: VfsCatalogSnapshot,
) -> VfsCatalogDelta:
    previous_by_id = {entry.entry_id: entry for entry in previous.entries}
    current_by_id = {entry.entry_id: entry for entry in current.entries}
    upserts = tuple(
        entry
        for entry_id, entry in current_by_id.items()
        if previous_by_id.get(entry_id) != entry
    )
    removals = tuple(
        VfsCatalogRemoval(
            entry_id=entry.entry_id,
            path=entry.path,
            kind=entry.kind,
            correlation=entry.correlation,
        )
        for entry_id, entry in previous_by_id.items()
        if entry_id not in current_by_id
    )
    return VfsCatalogDelta(
        generation_id=current.generation_id,
        base_generation_id=previous.generation_id,
        published_at=current.published_at,
        upserts=upserts,
        removals=removals,
        stats=current.stats,
    )


async def build_vfs_blocked_reason_summaries(
    resources: AppResources,
    *,
    generation_id: str | None = None,
) -> list[NamedCountSnapshot]:
    supplier = resources.vfs_catalog_supplier
    if supplier is None:
        return []
    snapshot: VfsCatalogSnapshot | None
    if generation_id is None:
        snapshot = await supplier.build_snapshot()
    else:
        snapshot = await supplier.snapshot_for_generation(int(generation_id))
    if snapshot is None:
        return []
    counts: dict[str, int] = {}
    for item in snapshot.blocked_items:
        counts[item.reason] = counts.get(item.reason, 0) + 1
    return _named_counts(counts)


async def build_vfs_catalog_delta_history(
    resources: AppResources,
    *,
    limit: int = 20,
) -> list[VfsCatalogDelta]:
    supplier = resources.vfs_catalog_supplier
    if supplier is None or not hasattr(supplier, "history_generation_ids"):
        return []
    generation_ids = list(await supplier.history_generation_ids())
    if len(generation_ids) < 2:
        return []
    bounded_ids = generation_ids[-max(2, min(limit + 1, 100)) :]
    snapshots: list[VfsCatalogSnapshot] = []
    for generation_id in bounded_ids:
        snapshot = await supplier.snapshot_for_generation(int(generation_id))
        if snapshot is not None:
            snapshots.append(snapshot)
    deltas = [
        _build_delta_between_snapshots(previous, current)
        for previous, current in pairwise(snapshots)
    ]
    return list(reversed(deltas[-max(1, min(limit, 100)) :]))


async def build_vfs_catalog_delta_history_summary(
    resources: AppResources,
    *,
    limit: int = 20,
) -> VfsCatalogDeltaHistorySummarySnapshot:
    deltas = await build_vfs_catalog_delta_history(resources, limit=limit)
    provider_family_counts: dict[str, int] = {}
    lease_state_counts: dict[str, int] = {}
    upsert_counts: list[int] = []
    removal_counts: list[int] = []
    upsert_file_counts: list[int] = []
    removal_file_counts: list[int] = []
    for delta in deltas:
        rollup = summarize_vfs_catalog_delta(delta)
        upsert_counts.append(len(delta.upserts))
        removal_counts.append(len(delta.removals))
        upsert_file_counts.append(rollup.upsert_file_count)
        removal_file_counts.append(rollup.removal_file_count)
        for key, count in rollup.provider_family_counts.items():
            provider_family_counts[key] = provider_family_counts.get(key, 0) + int(count)
        for key, count in rollup.lease_state_counts.items():
            lease_state_counts[key] = lease_state_counts.get(key, 0) + int(count)
    return VfsCatalogDeltaHistorySummarySnapshot(
        delta_count=len(deltas),
        max_upsert_count=max(upsert_counts, default=0),
        max_removal_count=max(removal_counts, default=0),
        total_upsert_count=sum(upsert_counts),
        total_removal_count=sum(removal_counts),
        total_upsert_file_count=sum(upsert_file_counts),
        total_removal_file_count=sum(removal_file_counts),
        provider_family_counts=dict(sorted(provider_family_counts.items())),
        lease_state_counts=dict(sorted(lease_state_counts.items())),
    )


async def build_vfs_mount_action_items(
    resources: AppResources,
) -> list[OperatorActionItemSnapshot]:
    diagnostics = await build_vfs_mount_diagnostics_posture(resources)
    severity = _severity_for_status(diagnostics.status)
    return [
        OperatorActionItemSnapshot(
            domain="vfs_mount",
            subject="vfs_mount_diagnostics",
            severity=severity,
            status=diagnostics.status,
            action=action,
        )
        for action in diagnostics.required_actions
    ]


async def build_vfs_mount_gap_items(
    resources: AppResources,
) -> list[OperatorGapItemSnapshot]:
    diagnostics = await build_vfs_mount_diagnostics_posture(resources)
    severity = _severity_for_status(diagnostics.status)
    return [
        OperatorGapItemSnapshot(
            domain="vfs_mount",
            subject="vfs_mount_diagnostics",
            severity=severity,
            status=diagnostics.status,
            message=message,
        )
        for message in diagnostics.remaining_gaps
    ]
