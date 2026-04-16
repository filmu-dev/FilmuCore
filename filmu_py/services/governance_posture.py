"""GraphQL-first governance and runtime posture builders for Director screens."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast

from filmu_py.api.routes import runtime_governance
from filmu_py.resources import AppResources
from filmu_py.services.operator_posture import (
    ProofArtifactSnapshot,
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
