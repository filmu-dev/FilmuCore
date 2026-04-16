"""Shared operator posture builders reused by GraphQL and compatibility routes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast

from filmu_py.plugins.builtin.listrr import resolve_listrr_settings
from filmu_py.plugins.builtin.plex import resolve_plex_settings
from filmu_py.plugins.builtin.seerr import resolve_seerr_settings
from filmu_py.resources import AppResources


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
    contract_validated: bool
    soak_validated: bool
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
    proof_refs: list[str]
    proof_ready: bool
    required_actions: list[str]
    remaining_gaps: list[str]


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
        contract_validated=contract_validated,
        soak_validated=soak_validated,
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
        proof_refs=proof_refs,
        proof_ready=bool(proof_refs),
        required_actions=list(dict.fromkeys(required_actions)),
        remaining_gaps=list(dict.fromkeys(remaining_gaps)),
    )
