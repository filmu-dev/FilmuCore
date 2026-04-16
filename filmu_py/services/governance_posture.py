"""GraphQL-first governance and runtime posture builders for Director screens."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast

from filmu_py.api.routes import runtime_governance
from filmu_py.core.queue_status import QueueStatusReader
from filmu_py.resources import AppResources
from filmu_py.services.operator_posture import (
    PluginCapabilityStatusSnapshot,
    PluginIntegrationReadinessPluginSnapshot,
    ProofArtifactSnapshot,
    _plugin_recommended_actions,
    build_plugin_event_status_posture,
    build_plugin_governance_posture,
    build_plugin_integration_readiness_posture,
)
from filmu_py.services.vfs_catalog import VfsCatalogSnapshot, summarize_vfs_catalog_snapshot


@dataclass(slots=True)
class GovernanceEvidenceCheckSnapshot:
    key: str
    label: str
    status: Literal["ready", "partial", "blocked", "not_applicable"]
    recorded: bool
    ready: bool
    evidence_refs: list[str]
    proof_artifacts: list[ProofArtifactSnapshot]
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class EnterpriseRolloutEvidenceSnapshot:
    generated_at: str
    status: Literal["ready", "partial", "blocked"]
    total_check_count: int
    ready_check_count: int
    checks: list[GovernanceEvidenceCheckSnapshot]
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class PlaybackGateGovernanceSnapshot:
    generated_at: str
    status: Literal["ready", "partial", "blocked"]
    rollout_readiness: str
    next_action: str
    reasons: list[str]
    environment_class: str
    gate_mode: str
    runner_status: str
    runner_ready: bool
    runner_required_failures: int
    provider_gate_required: bool
    provider_gate_ran: bool
    provider_parity_ready: bool
    windows_provider_ready: bool
    windows_provider_movie_ready: bool
    windows_provider_tv_ready: bool
    windows_provider_coverage: list[str]
    windows_soak_ready: bool
    windows_soak_repeat_count: int
    windows_soak_profile_coverage_complete: bool
    windows_soak_profile_coverage: list[str]
    policy_validation_status: str
    policy_ready: bool
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class VfsRuntimeRolloutSnapshot:
    generated_at: str
    status: Literal["ready", "partial", "blocked"]
    rollout_readiness: str
    next_action: str
    canary_decision: str
    merge_gate: str
    environment_class: str
    snapshot_available: bool
    open_handles: int
    active_reads: int
    cache_pressure_class: str
    refresh_pressure_class: str
    provider_pressure_incidents: int
    fairness_pressure_incidents: int
    reasons: list[str]
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class PluginRuntimeOverviewSnapshot:
    generated_at: str
    status: Literal["ready", "partial", "blocked"]
    total_plugins: int
    ready_plugins: int
    load_failed_plugins: int
    wiring_ready_plugins: int
    contract_validated_plugins: int
    soak_validated_plugins: int
    quarantined_plugins: int
    publishable_event_count: int
    hook_subscription_count: int
    warning_count: int
    recommended_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class PluginRuntimeWarningSnapshot:
    plugin_name: str
    source: Literal["governance", "integration"]
    severity: Literal["warning", "critical"]
    status: str
    message: str
    capability_kind: str | None = None


@dataclass(slots=True)
class VfsGenerationHistoryPointSnapshot:
    generation_id: str
    published_at: str
    entry_count: int
    directory_count: int
    file_count: int
    blocked_item_count: int
    blocked_reason_counts: dict[str, int]
    query_strategy_counts: dict[str, int]
    provider_family_counts: dict[str, int]
    lease_state_counts: dict[str, int]
    delta_from_previous_available: bool
    delta_upsert_count: int
    delta_removal_count: int
    delta_upsert_file_count: int
    delta_removal_file_count: int


@dataclass(slots=True)
class GovernanceStatusCountSnapshot:
    status: str
    count: int


@dataclass(slots=True)
class GovernanceArtifactInventorySnapshot:
    check_key: str
    check_label: str
    ref: str
    category: str
    label: str
    recorded: bool


@dataclass(slots=True)
class OperatorActionItemSnapshot:
    domain: str
    subject: str
    severity: Literal["warning", "critical"]
    status: str
    action: str
    capability_kind: str | None = None


@dataclass(slots=True)
class OperatorGapItemSnapshot:
    domain: str
    subject: str
    severity: Literal["warning", "critical"]
    status: str
    message: str
    capability_kind: str | None = None


@dataclass(slots=True)
class DownloaderExecutionTrendSummarySnapshot:
    point_count: int
    ok_point_count: int
    warning_point_count: int
    critical_point_count: int
    average_ready_jobs: float
    average_retry_jobs: float
    average_dead_letter_jobs: float
    latest_alert_level: str


@dataclass(slots=True)
class DownloaderProviderSummarySnapshot:
    provider: str
    sample_count: int
    failure_kind_counts: dict[str, int]
    reason_code_counts: dict[str, int]
    status_code_counts: dict[str, int]
    retry_after_hint_count: int


@dataclass(slots=True)
class DownloaderReasonSummarySnapshot:
    reason_code: str
    sample_count: int
    provider_counts: dict[str, int]
    failure_kind_counts: dict[str, int]


@dataclass(slots=True)
class PluginRuntimeRowSnapshot:
    name: str
    status: str
    ready: bool
    capability_kinds: list[str]
    wiring_status: str
    publishable_event_count: int
    hook_subscription_count: int
    contract_validated: bool
    soak_validated: bool
    proof_gap_count: int
    warning_count: int
    quarantined: bool
    recommended_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class PluginRuntimeCapabilitySummarySnapshot:
    capability_kind: str
    total_plugins: int
    ready_plugins: int
    blocked_plugins: int
    warning_count: int
    contract_validated_plugins: int
    soak_validated_plugins: int


@dataclass(slots=True)
class PluginProofCoverageSummarySnapshot:
    capability_kind: str
    total_plugins: int
    contract_validated_plugins: int
    soak_validated_plugins: int
    missing_contract_plugins: int
    missing_soak_plugins: int


@dataclass(slots=True)
class VfsGenerationHistorySummarySnapshot:
    generation_count: int
    newest_generation_id: str | None
    oldest_generation_id: str | None
    max_entry_count: int
    max_file_count: int
    blocked_generation_count: int
    total_delta_upsert_count: int
    total_delta_removal_count: int
    provider_family_counts: dict[str, int]
    lease_state_counts: dict[str, int]


def _queue_name(resources: AppResources) -> str:
    return resources.arq_queue_name or resources.settings.arq_queue_name


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _as_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        if isinstance(item, str) and item:
            result.append(item)
    return result


def _rollout_status(readiness: str) -> Literal["ready", "partial", "blocked"]:
    if readiness == "ready":
        return "ready"
    if readiness == "blocked":
        return "blocked"
    return "partial"


def _artifact_snapshot(
    refs: list[str],
    *,
    category: str,
    label: str,
) -> list[ProofArtifactSnapshot]:
    return [
        ProofArtifactSnapshot(ref=ref, category=category, label=label, recorded=True)
        for ref in refs
        if ref.strip()
    ]


def build_playback_gate_governance_posture() -> PlaybackGateGovernanceSnapshot:
    """Return typed playback-gate rollout posture for GraphQL-first operator screens."""

    snapshot = runtime_governance._playback_gate_governance_snapshot()
    reasons = _as_str_list(snapshot.get("playback_gate_rollout_reasons"))
    next_action = str(snapshot.get("playback_gate_rollout_next_action", ""))
    remaining_gaps = [
        f"playback gate rollout reason: {reason}"
        for reason in reasons
        if reason != "enterprise_playback_gate_green"
    ]
    required_actions = [next_action] if next_action else []
    if _as_int(snapshot.get("playback_gate_runner_required_failures")) > 0:
        required_actions.append("repair_required_playback_gate_runner_failures")
    if not _as_int(snapshot.get("playback_gate_policy_ready")):
        required_actions.append("validate_github_main_branch_protection")
    required_actions = list(dict.fromkeys(required_actions))
    return PlaybackGateGovernanceSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        status=_rollout_status(str(snapshot.get("playback_gate_rollout_readiness", "not_ready"))),
        rollout_readiness=str(snapshot.get("playback_gate_rollout_readiness", "not_ready")),
        next_action=next_action,
        reasons=reasons,
        environment_class=str(snapshot.get("playback_gate_environment_class", "")),
        gate_mode=str(snapshot.get("playback_gate_gate_mode", "unknown")),
        runner_status=str(snapshot.get("playback_gate_runner_status", "unknown")),
        runner_ready=bool(_as_int(snapshot.get("playback_gate_runner_ready"))),
        runner_required_failures=_as_int(snapshot.get("playback_gate_runner_required_failures")),
        provider_gate_required=bool(_as_int(snapshot.get("playback_gate_provider_gate_required"))),
        provider_gate_ran=bool(_as_int(snapshot.get("playback_gate_provider_gate_ran"))),
        provider_parity_ready=bool(_as_int(snapshot.get("playback_gate_provider_parity_ready"))),
        windows_provider_ready=bool(_as_int(snapshot.get("playback_gate_windows_provider_ready"))),
        windows_provider_movie_ready=bool(
            _as_int(snapshot.get("playback_gate_windows_provider_movie_ready"))
        ),
        windows_provider_tv_ready=bool(
            _as_int(snapshot.get("playback_gate_windows_provider_tv_ready"))
        ),
        windows_provider_coverage=_as_str_list(
            snapshot.get("playback_gate_windows_provider_coverage")
        ),
        windows_soak_ready=bool(_as_int(snapshot.get("playback_gate_windows_soak_ready"))),
        windows_soak_repeat_count=_as_int(snapshot.get("playback_gate_windows_soak_repeat_count")),
        windows_soak_profile_coverage_complete=bool(
            _as_int(snapshot.get("playback_gate_windows_soak_profile_coverage_complete"))
        ),
        windows_soak_profile_coverage=_as_str_list(
            snapshot.get("playback_gate_windows_soak_profile_coverage")
        ),
        policy_validation_status=str(
            snapshot.get("playback_gate_policy_validation_status", "unverified")
        ),
        policy_ready=bool(_as_int(snapshot.get("playback_gate_policy_ready"))),
        required_actions=required_actions,
        remaining_gaps=remaining_gaps,
    )


def build_vfs_runtime_rollout_posture(resources: AppResources) -> VfsRuntimeRolloutSnapshot:
    """Return typed VFS rollout/canary posture for GraphQL-first operator screens."""

    playback_gate = runtime_governance._playback_gate_governance_snapshot()
    snapshot = runtime_governance._vfs_runtime_governance_snapshot(
        playback_gate_governance=playback_gate,
    )
    reasons = _as_str_list(snapshot.get("vfs_runtime_rollout_reasons"))
    next_action = str(snapshot.get("vfs_runtime_rollout_next_action", ""))
    canary_decision = str(snapshot.get("vfs_runtime_rollout_canary_decision", ""))
    merge_gate = str(snapshot.get("vfs_runtime_rollout_merge_gate", "blocked"))
    required_actions = [action for action in (next_action, canary_decision) if action]
    remaining_gaps = [f"vfs rollout reason: {reason}" for reason in reasons]
    if merge_gate != "ready":
        remaining_gaps.insert(
            0,
            f"VFS rollout merge gate is not yet ready: merge_gate={merge_gate}",
        )
    return VfsRuntimeRolloutSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        status=_rollout_status(str(snapshot.get("vfs_runtime_rollout_readiness", "unknown"))),
        rollout_readiness=str(snapshot.get("vfs_runtime_rollout_readiness", "unknown")),
        next_action=next_action,
        canary_decision=canary_decision,
        merge_gate=merge_gate,
        environment_class=str(snapshot.get("vfs_runtime_rollout_environment_class", "")),
        snapshot_available=bool(_as_int(snapshot.get("vfs_runtime_snapshot_available"))),
        open_handles=_as_int(snapshot.get("vfs_runtime_open_handles")),
        active_reads=_as_int(snapshot.get("vfs_runtime_active_reads")),
        cache_pressure_class=str(snapshot.get("vfs_runtime_cache_pressure_class", "unknown")),
        refresh_pressure_class=str(
            snapshot.get("vfs_runtime_refresh_pressure_class", "unknown")
        ),
        provider_pressure_incidents=_as_int(
            snapshot.get("vfs_runtime_provider_pressure_incidents")
        ),
        fairness_pressure_incidents=_as_int(
            snapshot.get("vfs_runtime_fairness_pressure_incidents")
        ),
        reasons=reasons,
        required_actions=list(dict.fromkeys(required_actions)),
        remaining_gaps=remaining_gaps,
    )


def build_enterprise_rollout_evidence_posture(
    resources: AppResources,
) -> EnterpriseRolloutEvidenceSnapshot:
    """Return retained rollout-evidence posture across GraphQL-first operator domains."""

    playback_gate = runtime_governance._playback_gate_governance_snapshot()
    checks: list[GovernanceEvidenceCheckSnapshot] = []

    def _append_check(
        key: str,
        label: str,
        *,
        status: Literal["ready", "partial", "blocked", "not_applicable"],
        ready: bool,
        refs: list[str],
        required_actions: list[str],
        remaining_gaps: list[str],
        category: str,
    ) -> None:
        artifacts = _artifact_snapshot(refs, category=category, label=label)
        checks.append(
            GovernanceEvidenceCheckSnapshot(
                key=key,
                label=label,
                status=status,
                recorded=bool(artifacts),
                ready=ready,
                evidence_refs=[artifact.ref for artifact in artifacts],
                proof_artifacts=artifacts,
                required_actions=list(dict.fromkeys(required_actions)),
                remaining_gaps=list(dict.fromkeys(remaining_gaps)),
            )
        )

    oidc_enabled = resources.settings.oidc.enabled
    oidc_refs = list(resources.settings.oidc.rollout_evidence_refs)
    _append_check(
        "oidc_rollout",
        "OIDC rollout evidence",
        status=(
            "not_applicable"
            if not oidc_enabled
            else ("ready" if bool(oidc_refs) else "partial")
        ),
        ready=(not oidc_enabled or bool(oidc_refs)),
        refs=oidc_refs,
        required_actions=([] if not oidc_enabled or oidc_refs else ["record_oidc_rollout_evidence"]),
        remaining_gaps=(
            [] if not oidc_enabled or oidc_refs else ["OIDC is enabled without retained rollout evidence"]
        ),
        category="identity_rollout",
    )

    plugin_runtime_refs = list(resources.settings.plugin_runtime.proof_refs)
    _append_check(
        "plugin_runtime_isolation",
        "Plugin runtime isolation evidence",
        status="ready" if bool(plugin_runtime_refs) else "partial",
        ready=bool(plugin_runtime_refs),
        refs=plugin_runtime_refs,
        required_actions=(
            [] if plugin_runtime_refs else ["record_plugin_runtime_rollout_evidence"]
        ),
        remaining_gaps=(
            []
            if plugin_runtime_refs
            else ["plugin runtime isolation has no retained rollout evidence"]
        ),
        category="plugin_runtime_rollout",
    )

    observability_refs = list(resources.settings.observability.proof_refs)
    _append_check(
        "observability_rollout",
        "Observability rollout evidence",
        status="ready" if bool(observability_refs) else "partial",
        ready=bool(observability_refs),
        refs=observability_refs,
        required_actions=(
            [] if observability_refs else ["record_observability_rollout_evidence"]
        ),
        remaining_gaps=(
            []
            if observability_refs
            else ["observability rollout has no retained evidence references"]
        ),
        category="observability_rollout",
    )

    control_plane_refs = list(resources.settings.control_plane.proof_refs)
    _append_check(
        "control_plane_replay",
        "Control-plane replay evidence",
        status=(
            "ready"
            if resources.settings.control_plane.event_backplane == "redis_stream" and bool(control_plane_refs)
            else "partial"
            if resources.settings.control_plane.event_backplane == "redis_stream"
            else "not_applicable"
        ),
        ready=(
            resources.settings.control_plane.event_backplane != "redis_stream"
            or bool(control_plane_refs)
        ),
        refs=control_plane_refs,
        required_actions=(
            []
            if resources.settings.control_plane.event_backplane != "redis_stream" or control_plane_refs
            else ["record_control_plane_redis_consumer_group_evidence"]
        ),
        remaining_gaps=(
            []
            if resources.settings.control_plane.event_backplane != "redis_stream" or control_plane_refs
            else ["redis consumer-group rollout has no retained production evidence"]
        ),
        category="control_plane_rollout",
    )

    windows_soak_ready = bool(_as_int(playback_gate.get("playback_gate_windows_soak_ready")))
    windows_soak_recorded = (
        _as_int(playback_gate.get("playback_gate_windows_soak_repeat_count")) > 0
        or bool(_as_str_list(playback_gate.get("playback_gate_windows_soak_profile_coverage")))
    )
    checks.append(
        GovernanceEvidenceCheckSnapshot(
            key="windows_vfs_soak",
            label="Windows VFS soak evidence",
            status="ready" if windows_soak_ready else "partial",
            recorded=windows_soak_recorded,
            ready=windows_soak_ready,
            evidence_refs=[],
            proof_artifacts=[],
            required_actions=([] if windows_soak_ready else ["run_windows_vfs_soak_enterprise_profiles"]),
            remaining_gaps=(
                []
                if windows_soak_ready
                else ["Windows VFS soak evidence is incomplete or not green"]
            ),
        )
    )

    runner_ready = bool(_as_int(playback_gate.get("playback_gate_runner_ready")))
    checks.append(
        GovernanceEvidenceCheckSnapshot(
            key="playback_gate_runner",
            label="Playback-gate runner readiness",
            status="ready" if runner_ready else "partial",
            recorded=str(playback_gate.get("playback_gate_runner_status", "unknown")) != "unknown",
            ready=runner_ready,
            evidence_refs=[],
            proof_artifacts=[],
            required_actions=([] if runner_ready else ["record_playback_gate_runner_readiness"]),
            remaining_gaps=(
                []
                if runner_ready
                else ["playback-gate runner readiness is not yet recorded as ready"]
            ),
        )
    )

    policy_ready = bool(_as_int(playback_gate.get("playback_gate_policy_ready")))
    policy_status = str(playback_gate.get("playback_gate_policy_validation_status", "unverified"))
    checks.append(
        GovernanceEvidenceCheckSnapshot(
            key="github_main_policy",
            label="GitHub protected-branch policy validation",
            status="ready" if policy_ready else "partial",
            recorded=policy_status != "unverified",
            ready=policy_ready,
            evidence_refs=[],
            proof_artifacts=[],
            required_actions=([] if policy_ready else ["record_github_main_policy_validation"]),
            remaining_gaps=(
                []
                if policy_ready
                else ["live GitHub protected-branch policy has not been validated as ready"]
            ),
        )
    )

    ready_check_count = sum(1 for check in checks if check.ready or check.status == "not_applicable")
    blocking_checks = [check for check in checks if check.status == "blocked"]
    partial_checks = [check for check in checks if check.status == "partial"]
    status: Literal["ready", "partial", "blocked"]
    if blocking_checks:
        status = "blocked"
    elif partial_checks:
        status = "partial"
    else:
        status = "ready"

    return EnterpriseRolloutEvidenceSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        status=status,
        total_check_count=len(checks),
        ready_check_count=ready_check_count,
        checks=checks,
        required_actions=list(
            dict.fromkeys(
                action for check in checks for action in check.required_actions
            )
        ),
        remaining_gaps=list(
            dict.fromkeys(
                gap for check in checks for gap in check.remaining_gaps
            )
        ),
    )


async def build_plugin_runtime_overview_posture(
    resources: AppResources,
    *,
    app_state: object,
) -> tuple[PluginRuntimeOverviewSnapshot, list[PluginRuntimeWarningSnapshot]]:
    """Return plugin runtime overview plus actionable warning rows."""

    integration = build_plugin_integration_readiness_posture(resources)
    event_rows = build_plugin_event_status_posture(resources)
    governance = await build_plugin_governance_posture(resources, app_state=app_state)
    governance_plugins = list(governance.plugins)

    load_failed_plugins = sum(
        1 for plugin in governance_plugins if str(getattr(plugin, "status", "")) == "load_failed"
    )
    ready_plugins = sum(1 for plugin in governance_plugins if bool(getattr(plugin, "ready", False)))
    wiring_ready_plugins = sum(
        1 for row in event_rows if str(row.wiring_status) == "wired"
    )
    contract_validated_plugins = sum(1 for row in integration.plugins if row.contract_validated)
    soak_validated_plugins = sum(1 for row in integration.plugins if row.soak_validated)
    quarantined_plugins = sum(
        1 for plugin in governance_plugins if bool(getattr(plugin, "quarantined", False))
    )
    publishable_event_count = sum(row.publishable_event_count for row in event_rows)
    hook_subscription_count = sum(row.hook_subscription_count for row in event_rows)

    warnings: list[PluginRuntimeWarningSnapshot] = []
    for plugin in governance_plugins:
        plugin_name = str(getattr(plugin, "name", ""))
        plugin_status = str(getattr(plugin, "status", "unknown"))
        capabilities = cast(list[str], getattr(plugin, "capabilities", []))
        capability_kind = capabilities[0] if capabilities else None
        for warning in cast(list[str], getattr(plugin, "warnings", [])):
            warnings.append(
                PluginRuntimeWarningSnapshot(
                    plugin_name=plugin_name,
                    source="governance",
                    severity="warning",
                    status=plugin_status,
                    message=warning,
                    capability_kind=capability_kind,
                )
            )
        error = getattr(plugin, "error", None)
        if isinstance(error, str) and error:
            warnings.append(
                PluginRuntimeWarningSnapshot(
                    plugin_name=plugin_name,
                    source="governance",
                    severity="critical",
                    status=plugin_status,
                    message=error,
                    capability_kind=capability_kind,
                )
            )

    for readiness_plugin in integration.plugins:
        if readiness_plugin.ready:
            continue
        message = (
            "; ".join(readiness_plugin.remaining_gaps) or "plugin integration is not ready"
        )
        warnings.append(
            PluginRuntimeWarningSnapshot(
                plugin_name=readiness_plugin.name,
                source="integration",
                severity="critical" if readiness_plugin.status == "blocked" else "warning",
                status=readiness_plugin.status,
                message=message,
                capability_kind=readiness_plugin.capability_kind,
            )
        )

    status: Literal["ready", "partial", "blocked"]
    if load_failed_plugins or quarantined_plugins:
        status = "blocked"
    elif warnings:
        status = "partial"
    else:
        status = "ready"

    overview = PluginRuntimeOverviewSnapshot(
        generated_at=datetime.now(UTC).isoformat(),
        status=status,
        total_plugins=len(governance_plugins),
        ready_plugins=ready_plugins,
        load_failed_plugins=load_failed_plugins,
        wiring_ready_plugins=wiring_ready_plugins,
        contract_validated_plugins=contract_validated_plugins,
        soak_validated_plugins=soak_validated_plugins,
        quarantined_plugins=quarantined_plugins,
        publishable_event_count=publishable_event_count,
        hook_subscription_count=hook_subscription_count,
        warning_count=len(warnings),
        recommended_actions=list(governance.summary.recommended_actions),
        remaining_gaps=list(dict.fromkeys(governance.summary.remaining_gaps)),
    )
    return overview, warnings


def _delta_counts(
    previous_snapshot: VfsCatalogSnapshot | None,
    current_snapshot: VfsCatalogSnapshot,
) -> tuple[bool, int, int, int, int]:
    if previous_snapshot is None:
        return False, 0, 0, 0, 0
    previous_by_id = {entry.entry_id: entry for entry in previous_snapshot.entries}
    current_by_id = {entry.entry_id: entry for entry in current_snapshot.entries}
    upserts = [
        entry
        for entry_id, entry in current_by_id.items()
        if previous_by_id.get(entry_id) != entry
    ]
    removals = [
        entry
        for entry_id, entry in previous_by_id.items()
        if entry_id not in current_by_id
    ]
    return (
        True,
        len(upserts),
        len(removals),
        sum(1 for entry in upserts if entry.kind == "file"),
        sum(1 for entry in removals if entry.kind == "file"),
    )


async def build_vfs_generation_history_posture(
    resources: AppResources,
    *,
    limit: int = 20,
) -> list[VfsGenerationHistoryPointSnapshot]:
    """Return retained VFS generation history with snapshot-level rollups for GraphQL."""

    supplier = resources.vfs_catalog_supplier
    if supplier is None:
        return []

    history_generation_ids = list(await supplier.history_generation_ids())
    current_snapshot = await supplier.build_snapshot()
    if current_snapshot.generation_id and current_snapshot.generation_id not in history_generation_ids:
        history_generation_ids.append(current_snapshot.generation_id)
    bounded_ids = history_generation_ids[-max(1, min(limit, 100)) :]

    snapshots: list[VfsCatalogSnapshot] = []
    current_generation_id = current_snapshot.generation_id
    current_generation_int = int(current_generation_id) if current_generation_id.isdigit() else None
    for generation_id in bounded_ids:
        snapshot: VfsCatalogSnapshot | None = None
        if generation_id == current_generation_id:
            snapshot = current_snapshot
        elif generation_id.isdigit():
            snapshot = await supplier.snapshot_for_generation(int(generation_id))
        if (
            snapshot is None
            and current_generation_int is not None
            and generation_id.isdigit()
            and int(generation_id) == current_generation_int
        ):
            snapshot = current_snapshot
        if snapshot is not None:
            snapshots.append(snapshot)

    snapshots.sort(key=lambda item: int(item.generation_id) if item.generation_id.isdigit() else 0)
    results: list[VfsGenerationHistoryPointSnapshot] = []
    previous_snapshot: VfsCatalogSnapshot | None = None
    for snapshot in snapshots:
        rollup = summarize_vfs_catalog_snapshot(snapshot)
        (
            delta_available,
            delta_upsert_count,
            delta_removal_count,
            delta_upsert_file_count,
            delta_removal_file_count,
        ) = _delta_counts(previous_snapshot, snapshot)
        results.append(
            VfsGenerationHistoryPointSnapshot(
                generation_id=snapshot.generation_id,
                published_at=snapshot.published_at.isoformat(),
                entry_count=len(snapshot.entries),
                directory_count=snapshot.stats.directory_count,
                file_count=snapshot.stats.file_count,
                blocked_item_count=snapshot.stats.blocked_item_count,
                blocked_reason_counts=dict(rollup.blocked_reason_counts),
                query_strategy_counts=dict(rollup.query_strategy_counts),
                provider_family_counts=dict(rollup.provider_family_counts),
                lease_state_counts=dict(rollup.lease_state_counts),
                delta_from_previous_available=delta_available,
                delta_upsert_count=delta_upsert_count,
                delta_removal_count=delta_removal_count,
                delta_upsert_file_count=delta_upsert_file_count,
                delta_removal_file_count=delta_removal_file_count,
            )
        )
        previous_snapshot = snapshot
    return list(reversed(results))


def build_enterprise_rollout_status_counts(
    resources: AppResources,
) -> list[GovernanceStatusCountSnapshot]:
    """Return status buckets for retained rollout evidence checks."""

    evidence = build_enterprise_rollout_evidence_posture(resources)
    counts: dict[str, int] = {}
    for check in evidence.checks:
        counts[check.status] = counts.get(check.status, 0) + 1
    return [
        GovernanceStatusCountSnapshot(status=status, count=count)
        for status, count in sorted(counts.items())
    ]


def build_enterprise_rollout_artifact_inventory(
    resources: AppResources,
) -> list[GovernanceArtifactInventorySnapshot]:
    """Return the retained rollout-evidence artifact inventory for GraphQL."""

    evidence = build_enterprise_rollout_evidence_posture(resources)
    rows: list[GovernanceArtifactInventorySnapshot] = []
    for check in evidence.checks:
        for artifact in check.proof_artifacts:
            rows.append(
                GovernanceArtifactInventorySnapshot(
                    check_key=check.key,
                    check_label=check.label,
                    ref=artifact.ref,
                    category=artifact.category,
                    label=artifact.label,
                    recorded=bool(artifact.recorded),
                )
            )
    return rows


def build_enterprise_rollout_action_items(
    resources: AppResources,
) -> list[OperatorActionItemSnapshot]:
    """Return one flattened governance-action feed for Director/operator screens."""

    evidence = build_enterprise_rollout_evidence_posture(resources)
    playback_gate = build_playback_gate_governance_posture()
    vfs_runtime = build_vfs_runtime_rollout_posture(resources)
    actions: list[OperatorActionItemSnapshot] = []
    for check in evidence.checks:
        severity: Literal["warning", "critical"] = (
            "critical" if check.status == "blocked" else "warning"
        )
        for action in check.required_actions:
            actions.append(
                OperatorActionItemSnapshot(
                    domain="enterprise_rollout",
                    subject=check.key,
                    severity=severity,
                    status=check.status,
                    action=action,
                )
            )
    for action in playback_gate.required_actions:
        actions.append(
            OperatorActionItemSnapshot(
                domain="playback_gate",
                subject="playback_gate_governance",
                severity="critical" if playback_gate.status == "blocked" else "warning",
                status=playback_gate.status,
                action=action,
            )
        )
    for action in vfs_runtime.required_actions:
        actions.append(
            OperatorActionItemSnapshot(
                domain="vfs_runtime_rollout",
                subject="vfs_runtime_rollout",
                severity="critical" if vfs_runtime.status == "blocked" else "warning",
                status=vfs_runtime.status,
                action=action,
            )
        )
    deduped: dict[tuple[str, str, str], OperatorActionItemSnapshot] = {}
    for row in actions:
        deduped[(row.domain, row.subject, row.action)] = row
    return list(deduped.values())


def build_enterprise_rollout_gap_items(
    resources: AppResources,
) -> list[OperatorGapItemSnapshot]:
    """Return flattened retained rollout gaps for GraphQL-first consoles."""

    evidence = build_enterprise_rollout_evidence_posture(resources)
    playback_gate = build_playback_gate_governance_posture()
    vfs_runtime = build_vfs_runtime_rollout_posture(resources)
    gaps: list[OperatorGapItemSnapshot] = []
    for check in evidence.checks:
        severity: Literal["warning", "critical"] = (
            "critical" if check.status == "blocked" else "warning"
        )
        for message in check.remaining_gaps:
            gaps.append(
                OperatorGapItemSnapshot(
                    domain="enterprise_rollout",
                    subject=check.key,
                    severity=severity,
                    status=check.status,
                    message=message,
                )
            )
    for message in playback_gate.remaining_gaps:
        gaps.append(
            OperatorGapItemSnapshot(
                domain="playback_gate",
                subject="playback_gate_governance",
                severity="critical" if playback_gate.status == "blocked" else "warning",
                status=playback_gate.status,
                message=message,
            )
        )
    for message in vfs_runtime.remaining_gaps:
        gaps.append(
            OperatorGapItemSnapshot(
                domain="vfs_runtime_rollout",
                subject="vfs_runtime_rollout",
                severity="critical" if vfs_runtime.status == "blocked" else "warning",
                status=vfs_runtime.status,
                message=message,
            )
        )
    deduped: dict[tuple[str, str, str], OperatorGapItemSnapshot] = {}
    for row in gaps:
        deduped[(row.domain, row.subject, row.message)] = row
    return list(deduped.values())


async def build_downloader_execution_trend_summary(
    resources: AppResources,
    *,
    limit: int = 20,
) -> DownloaderExecutionTrendSummarySnapshot:
    """Return bounded queue-history trend rollups for downloader execution."""

    reader = QueueStatusReader(resources.redis, queue_name=_queue_name(resources))
    points = await reader.history(limit=max(1, min(limit, 100)))
    point_count = len(points)
    average_ready_jobs = (
        sum(point.ready_jobs for point in points) / point_count if point_count else 0.0
    )
    average_retry_jobs = (
        sum(point.retry_jobs for point in points) / point_count if point_count else 0.0
    )
    average_dead_letter_jobs = (
        sum(point.dead_letter_jobs for point in points) / point_count if point_count else 0.0
    )
    return DownloaderExecutionTrendSummarySnapshot(
        point_count=point_count,
        ok_point_count=sum(1 for point in points if point.alert_level == "ok"),
        warning_point_count=sum(1 for point in points if point.alert_level == "warning"),
        critical_point_count=sum(1 for point in points if point.alert_level == "critical"),
        average_ready_jobs=average_ready_jobs,
        average_retry_jobs=average_retry_jobs,
        average_dead_letter_jobs=average_dead_letter_jobs,
        latest_alert_level=(points[0].alert_level if points else "ok"),
    )


async def build_downloader_provider_summaries(
    resources: AppResources,
    *,
    limit: int = 50,
) -> list[DownloaderProviderSummarySnapshot]:
    """Return provider-grouped dead-letter/failover evidence for GraphQL."""

    reader = QueueStatusReader(resources.redis, queue_name=_queue_name(resources))
    samples = await reader.dead_letter_samples(limit=max(1, min(limit, 100)), stage="debrid_item")
    grouped: dict[str, DownloaderProviderSummarySnapshot] = {}
    for sample in samples:
        metadata = sample.metadata
        provider = (
            str(metadata.get("provider")).strip()
            if isinstance(metadata.get("provider"), str) and str(metadata.get("provider")).strip()
            else "unknown"
        )
        failure_kind = (
            str(metadata.get("failure_kind")).strip()
            if isinstance(metadata.get("failure_kind"), str) and str(metadata.get("failure_kind")).strip()
            else "unknown"
        )
        status_code = metadata.get("status_code")
        status_code_key = (
            str(int(status_code))
            if isinstance(status_code, int) and not isinstance(status_code, bool)
            else "unknown"
        )
        row = grouped.get(provider)
        if row is None:
            row = DownloaderProviderSummarySnapshot(
                provider=provider,
                sample_count=0,
                failure_kind_counts={},
                reason_code_counts={},
                status_code_counts={},
                retry_after_hint_count=0,
            )
            grouped[provider] = row
        row.sample_count += 1
        row.failure_kind_counts[failure_kind] = row.failure_kind_counts.get(failure_kind, 0) + 1
        row.reason_code_counts[sample.reason_code] = row.reason_code_counts.get(sample.reason_code, 0) + 1
        row.status_code_counts[status_code_key] = row.status_code_counts.get(status_code_key, 0) + 1
        if isinstance(metadata.get("retry_after_seconds"), int) and not isinstance(
            metadata.get("retry_after_seconds"), bool
        ):
            row.retry_after_hint_count += 1
    return [grouped[key] for key in sorted(grouped)]


async def build_downloader_reason_summaries(
    resources: AppResources,
    *,
    limit: int = 50,
) -> list[DownloaderReasonSummarySnapshot]:
    """Return reason-code grouped downloader dead-letter evidence."""

    reader = QueueStatusReader(resources.redis, queue_name=_queue_name(resources))
    samples = await reader.dead_letter_samples(limit=max(1, min(limit, 100)), stage="debrid_item")
    grouped: dict[str, DownloaderReasonSummarySnapshot] = {}
    for sample in samples:
        metadata = sample.metadata
        provider = (
            str(metadata.get("provider")).strip()
            if isinstance(metadata.get("provider"), str) and str(metadata.get("provider")).strip()
            else "unknown"
        )
        failure_kind = (
            str(metadata.get("failure_kind")).strip()
            if isinstance(metadata.get("failure_kind"), str) and str(metadata.get("failure_kind")).strip()
            else "unknown"
        )
        row = grouped.get(sample.reason_code)
        if row is None:
            row = DownloaderReasonSummarySnapshot(
                reason_code=sample.reason_code,
                sample_count=0,
                provider_counts={},
                failure_kind_counts={},
            )
            grouped[sample.reason_code] = row
        row.sample_count += 1
        row.provider_counts[provider] = row.provider_counts.get(provider, 0) + 1
        row.failure_kind_counts[failure_kind] = row.failure_kind_counts.get(failure_kind, 0) + 1
    return [grouped[key] for key in sorted(grouped)]


def _integration_by_name(
    rows: list[PluginIntegrationReadinessPluginSnapshot],
) -> dict[str, PluginIntegrationReadinessPluginSnapshot]:
    return {row.name: row for row in rows}


def _event_by_name(rows: list[object]) -> dict[str, object]:
    return {str(getattr(row, "name", "")): row for row in rows}


def _row_capability_kinds(plugin: PluginCapabilityStatusSnapshot) -> list[str]:
    return [str(capability) for capability in plugin.capabilities]


async def build_plugin_runtime_rows(
    resources: AppResources,
    *,
    app_state: object,
) -> list[PluginRuntimeRowSnapshot]:
    """Return per-plugin runtime rows combining governance, events, and proof posture."""

    integration = build_plugin_integration_readiness_posture(resources)
    event_rows = build_plugin_event_status_posture(resources)
    governance = await build_plugin_governance_posture(resources, app_state=app_state)
    integration_by_name = _integration_by_name(list(integration.plugins))
    event_by_name = _event_by_name(list(event_rows))
    rows: list[PluginRuntimeRowSnapshot] = []
    for plugin in governance.plugins:
        event_row = event_by_name.get(plugin.name)
        integration_row = integration_by_name.get(plugin.name)
        capability_kinds = _row_capability_kinds(plugin)
        status = "blocked" if plugin.status == "load_failed" else ("partial" if not plugin.ready else "ready")
        if integration_row is not None and integration_row.status == "blocked":
            status = "blocked"
        elif integration_row is not None and integration_row.status == "partial" and status == "ready":
            status = "partial"
        row_remaining_gaps = list(plugin.warnings)
        recommended_actions = set(_plugin_recommended_actions(plugin))
        contract_validated = bool(integration_row.contract_validated) if integration_row is not None else False
        soak_validated = bool(integration_row.soak_validated) if integration_row is not None else False
        proof_gap_count = int(integration_row.proof_gap_count) if integration_row is not None else 0
        if integration_row is not None:
            row_remaining_gaps.extend(integration_row.remaining_gaps)
            recommended_actions.update(integration_row.required_actions)
        rows.append(
            PluginRuntimeRowSnapshot(
                name=plugin.name,
                status=status,
                ready=bool(plugin.ready and (integration_row.ready if integration_row is not None else True)),
                capability_kinds=capability_kinds,
                wiring_status=(
                    str(getattr(event_row, "wiring_status", "not_wired"))
                    if event_row is not None
                    else "not_wired"
                ),
                publishable_event_count=int(getattr(event_row, "publishable_event_count", 0)),
                hook_subscription_count=int(getattr(event_row, "hook_subscription_count", 0)),
                contract_validated=contract_validated,
                soak_validated=soak_validated,
                proof_gap_count=proof_gap_count,
                warning_count=len(row_remaining_gaps),
                quarantined=bool(plugin.quarantined),
                recommended_actions=sorted(recommended_actions),
                remaining_gaps=list(dict.fromkeys(row_remaining_gaps)),
            )
        )
    return rows


def build_plugin_runtime_capability_summaries(
    rows: list[PluginRuntimeRowSnapshot],
) -> list[PluginRuntimeCapabilitySummarySnapshot]:
    """Return capability-grouped plugin runtime rollups."""

    grouped: dict[str, PluginRuntimeCapabilitySummarySnapshot] = {}
    for row in rows:
        for capability_kind in row.capability_kinds or ["unknown"]:
            summary = grouped.get(capability_kind)
            if summary is None:
                summary = PluginRuntimeCapabilitySummarySnapshot(
                    capability_kind=capability_kind,
                    total_plugins=0,
                    ready_plugins=0,
                    blocked_plugins=0,
                    warning_count=0,
                    contract_validated_plugins=0,
                    soak_validated_plugins=0,
                )
                grouped[capability_kind] = summary
            summary.total_plugins += 1
            if row.ready:
                summary.ready_plugins += 1
            if row.status == "blocked":
                summary.blocked_plugins += 1
            summary.warning_count += row.warning_count
            if row.contract_validated:
                summary.contract_validated_plugins += 1
            if row.soak_validated:
                summary.soak_validated_plugins += 1
    return [grouped[key] for key in sorted(grouped)]


def build_plugin_proof_coverage_summaries(
    rows: list[PluginRuntimeRowSnapshot],
) -> list[PluginProofCoverageSummarySnapshot]:
    """Return capability-grouped retained proof coverage summaries."""

    grouped: dict[str, PluginProofCoverageSummarySnapshot] = {}
    for row in rows:
        for capability_kind in row.capability_kinds or ["unknown"]:
            summary = grouped.get(capability_kind)
            if summary is None:
                summary = PluginProofCoverageSummarySnapshot(
                    capability_kind=capability_kind,
                    total_plugins=0,
                    contract_validated_plugins=0,
                    soak_validated_plugins=0,
                    missing_contract_plugins=0,
                    missing_soak_plugins=0,
                )
                grouped[capability_kind] = summary
            summary.total_plugins += 1
            if row.contract_validated:
                summary.contract_validated_plugins += 1
            else:
                summary.missing_contract_plugins += 1
            if row.soak_validated:
                summary.soak_validated_plugins += 1
            else:
                summary.missing_soak_plugins += 1
    return [grouped[key] for key in sorted(grouped)]


def build_plugin_runtime_action_items(
    rows: list[PluginRuntimeRowSnapshot],
) -> list[OperatorActionItemSnapshot]:
    """Return flattened plugin runtime action items for GraphQL consoles."""

    actions: dict[tuple[str, str, str], OperatorActionItemSnapshot] = {}
    for row in rows:
        severity: Literal["warning", "critical"] = "critical" if row.status == "blocked" else "warning"
        primary_capability = row.capability_kinds[0] if row.capability_kinds else None
        for action in row.recommended_actions:
            actions[("plugin_runtime", row.name, action)] = OperatorActionItemSnapshot(
                domain="plugin_runtime",
                subject=row.name,
                severity=severity,
                status=row.status,
                action=action,
                capability_kind=primary_capability,
            )
    return list(actions.values())


def build_plugin_runtime_gap_items(
    rows: list[PluginRuntimeRowSnapshot],
) -> list[OperatorGapItemSnapshot]:
    """Return flattened plugin runtime gaps for GraphQL consoles."""

    gaps: dict[tuple[str, str, str], OperatorGapItemSnapshot] = {}
    for row in rows:
        severity: Literal["warning", "critical"] = "critical" if row.status == "blocked" else "warning"
        primary_capability = row.capability_kinds[0] if row.capability_kinds else None
        for message in row.remaining_gaps:
            gaps[("plugin_runtime", row.name, message)] = OperatorGapItemSnapshot(
                domain="plugin_runtime",
                subject=row.name,
                severity=severity,
                status=row.status,
                message=message,
                capability_kind=primary_capability,
            )
    return list(gaps.values())


async def build_vfs_generation_history_summary(
    resources: AppResources,
    *,
    limit: int = 20,
) -> VfsGenerationHistorySummarySnapshot:
    """Return aggregate rollups over retained VFS generation history."""

    rows = await build_vfs_generation_history_posture(resources, limit=limit)
    provider_family_counts: dict[str, int] = {}
    lease_state_counts: dict[str, int] = {}
    for row in rows:
        for key, count in row.provider_family_counts.items():
            provider_family_counts[key] = provider_family_counts.get(key, 0) + int(count)
        for key, count in row.lease_state_counts.items():
            lease_state_counts[key] = lease_state_counts.get(key, 0) + int(count)
    return VfsGenerationHistorySummarySnapshot(
        generation_count=len(rows),
        newest_generation_id=(rows[0].generation_id if rows else None),
        oldest_generation_id=(rows[-1].generation_id if rows else None),
        max_entry_count=max((row.entry_count for row in rows), default=0),
        max_file_count=max((row.file_count for row in rows), default=0),
        blocked_generation_count=sum(1 for row in rows if row.blocked_item_count > 0),
        total_delta_upsert_count=sum(row.delta_upsert_count for row in rows),
        total_delta_removal_count=sum(row.delta_removal_count for row in rows),
        provider_family_counts=dict(sorted(provider_family_counts.items())),
        lease_state_counts=dict(sorted(lease_state_counts.items())),
    )
