"""Shared operator posture builders reused by GraphQL and compatibility routes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast

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
    capability_kind: Literal["scraper", "content_service", "event_hook"]
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
    multi_provider_enabled: bool
    plugin_downloaders_registered: int
    worker_plugin_dispatch_ready: bool
    fanout_ready: bool
    multi_container_ready: bool
    providers: list[DownloaderProviderCandidateSnapshot]
    required_actions: list[str]
    remaining_gaps: list[str]


@dataclass(slots=True)
class PluginEventStatusSnapshot:
    name: str
    publisher: str | None
    publishable_events: list[str]
    hook_subscriptions: list[str]


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
    capability_kind: Literal["scraper", "content_service", "event_hook"],
    registered: bool,
    enabled: bool,
    endpoint: str | None,
    config_source: str | None,
    required_settings: list[str],
    configured_values: Mapping[str, Any],
    contract_proof_refs: list[str],
    soak_proof_refs: list[str],
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
    contract_validated = bool(contract_proof_refs)
    soak_validated = bool(soak_proof_refs)
    ready = registered and configured and contract_validated and soak_validated
    status: Literal["ready", "partial", "blocked"] = (
        "ready" if ready else "partial" if enabled or registered else "blocked"
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
        contract_proof_refs=list(contract_proof_refs),
        soak_proof_refs=list(soak_proof_refs),
        contract_proofs=contract_proofs,
        soak_proofs=soak_proofs,
        contract_validated=contract_validated,
        soak_validated=soak_validated,
        proof_gap_count=int(not contract_validated) + int(not soak_validated),
        required_actions=required_actions,
        remaining_gaps=remaining_gaps,
    )


def build_plugin_integration_readiness_posture(
    resources: AppResources,
) -> PluginIntegrationReadinessSnapshot:
    """Return readiness and config-validation posture for builtin enterprise plugins."""

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
    event_hook_names = (
        {
            str(getattr(plugin, "plugin_name", type(plugin).__name__)).strip()
            for plugin in plugin_registry.get_event_hooks()
        }
        if plugin_registry is not None
        else set()
    )

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
    multi_provider_enabled = sum(1 for candidate in builtin_candidates if candidate.enabled) > 1
    worker_plugin_dispatch_ready = plugin_policy_ready
    fanout_ready = bool(
        settings.orchestration.downloader_selection_mode == "ordered_failover"
        and len(enabled_candidates) > 1
        and (plugin_downloaders_registered == 0 or plugin_policy_ready)
    )
    multi_container_ready = True
    ordered_failover_ready = settings.orchestration.downloader_selection_mode == "ordered_failover"

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
        multi_provider_enabled=multi_provider_enabled,
        plugin_downloaders_registered=plugin_downloaders_registered,
        worker_plugin_dispatch_ready=worker_plugin_dispatch_ready,
        fanout_ready=fanout_ready,
        multi_container_ready=multi_container_ready,
        providers=providers,
        required_actions=required_actions,
        remaining_gaps=remaining_gaps,
    )


def build_plugin_event_status_posture(
    resources: AppResources,
) -> list[PluginEventStatusSnapshot]:
    """Return declared publishable events and hook subscriptions per plugin."""

    plugin_registry = resources.plugin_registry
    if plugin_registry is None:
        return []

    publishable_by_plugin = plugin_registry.publishable_events_by_plugin()
    subscriptions_by_plugin = plugin_registry.hook_subscriptions_by_plugin()
    rows: list[PluginEventStatusSnapshot] = []
    for plugin_name in sorted(plugin_registry.all_plugin_names()):
        manifest = plugin_registry.manifest(plugin_name)
        rows.append(
            PluginEventStatusSnapshot(
                name=plugin_name,
                publisher=manifest.publisher if manifest is not None else None,
                publishable_events=list(publishable_by_plugin.get(plugin_name, ())),
                hook_subscriptions=list(subscriptions_by_plugin.get(plugin_name, ())),
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
    proof_refs = list(settings.proof_refs)
    proof_artifacts = _build_proof_artifacts(
        proof_refs,
        category="control_plane_rollout",
        label="control-plane replay backplane proof",
    )
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
    if not proof_refs:
        required_actions.append("record_control_plane_redis_consumer_group_evidence")
        remaining_gaps.append("redis consumer-group rollout has no retained production evidence")

    ready = settings.event_backplane == "redis_stream" and attached and bool(proof_refs)
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
        attached=attached,
        pending_count=pending_count,
        oldest_event_id=oldest_event_id,
        latest_event_id=latest_event_id,
        consumer_counts=dict(sorted(consumer_counts.items())),
        consumer_count=len(consumer_counts),
        has_pending_backlog=pending_count > 0,
        proof_refs=proof_refs,
        proof_artifacts=proof_artifacts,
        proof_ready=bool(proof_refs),
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
