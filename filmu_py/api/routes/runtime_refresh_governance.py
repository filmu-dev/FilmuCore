"""Shared route-level refresh-trigger and dispatch-policy governance helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol


class _RouteRefreshPendingResult(Protocol):
    retry_after_seconds: float | int | None


class _RouteRefreshController(Protocol):
    def has_pending(self, item_identifier: str) -> bool:
        pass

    def get_last_result(self, item_identifier: str) -> _RouteRefreshPendingResult | None:
        pass


DIRECT_PLAYBACK_TRIGGER_GOVERNANCE = {
    "starts": 0,
    "no_action": 0,
    "controller_unavailable": 0,
    "already_pending": 0,
    "backoff_pending": 0,
    "failures": 0,
}
HLS_FAILED_LEASE_TRIGGER_GOVERNANCE = {
    "starts": 0,
    "no_action": 0,
    "controller_unavailable": 0,
    "already_pending": 0,
    "backoff_pending": 0,
    "failures": 0,
}
HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE = {
    "starts": 0,
    "no_action": 0,
    "controller_unavailable": 0,
    "already_pending": 0,
    "backoff_pending": 0,
    "failures": 0,
}
STREAM_REFRESH_POLICY_GOVERNANCE = {
    "forced_queued": 0,
    "forced_in_process": 0,
    "fallback_in_process": 0,
    "latency_slo_breaches": 0,
}
RuntimeGovernanceSnapshot = dict[str, int | float | str | list[str]]
RuntimePressureEvaluator = Callable[[RuntimeGovernanceSnapshot], tuple[bool, bool]]
RuntimeGovernanceProvider = Callable[[], RuntimeGovernanceSnapshot]


def direct_playback_trigger_governance_snapshot(*, active_tasks: int) -> dict[str, int]:
    """Return additive governance counters for route-adjacent direct-play refresh triggering."""

    return {
        "direct_playback_refresh_trigger_starts": DIRECT_PLAYBACK_TRIGGER_GOVERNANCE["starts"],
        "direct_playback_refresh_trigger_no_action": DIRECT_PLAYBACK_TRIGGER_GOVERNANCE[
            "no_action"
        ],
        "direct_playback_refresh_trigger_controller_unavailable": DIRECT_PLAYBACK_TRIGGER_GOVERNANCE[
            "controller_unavailable"
        ],
        "direct_playback_refresh_trigger_already_pending": DIRECT_PLAYBACK_TRIGGER_GOVERNANCE[
            "already_pending"
        ],
        "direct_playback_refresh_trigger_backoff_pending": DIRECT_PLAYBACK_TRIGGER_GOVERNANCE[
            "backoff_pending"
        ],
        "direct_playback_refresh_trigger_failures": DIRECT_PLAYBACK_TRIGGER_GOVERNANCE["failures"],
        "direct_playback_refresh_trigger_tasks_active": active_tasks,
    }


def hls_failed_lease_trigger_governance_snapshot(*, active_tasks: int) -> dict[str, int]:
    """Return additive governance counters for route-adjacent HLS failed-lease refresh triggering."""

    return {
        "hls_failed_lease_refresh_trigger_starts": HLS_FAILED_LEASE_TRIGGER_GOVERNANCE["starts"],
        "hls_failed_lease_refresh_trigger_no_action": HLS_FAILED_LEASE_TRIGGER_GOVERNANCE[
            "no_action"
        ],
        "hls_failed_lease_refresh_trigger_controller_unavailable": HLS_FAILED_LEASE_TRIGGER_GOVERNANCE[
            "controller_unavailable"
        ],
        "hls_failed_lease_refresh_trigger_already_pending": HLS_FAILED_LEASE_TRIGGER_GOVERNANCE[
            "already_pending"
        ],
        "hls_failed_lease_refresh_trigger_backoff_pending": HLS_FAILED_LEASE_TRIGGER_GOVERNANCE[
            "backoff_pending"
        ],
        "hls_failed_lease_refresh_trigger_failures": HLS_FAILED_LEASE_TRIGGER_GOVERNANCE[
            "failures"
        ],
        "hls_failed_lease_refresh_trigger_tasks_active": active_tasks,
    }


def hls_restricted_fallback_trigger_governance_snapshot(*, active_tasks: int) -> dict[str, int]:
    """Return additive governance counters for route-adjacent HLS restricted-fallback refresh triggering."""

    return {
        "hls_restricted_fallback_refresh_trigger_starts": HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "starts"
        ],
        "hls_restricted_fallback_refresh_trigger_no_action": HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "no_action"
        ],
        "hls_restricted_fallback_refresh_trigger_controller_unavailable": HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "controller_unavailable"
        ],
        "hls_restricted_fallback_refresh_trigger_already_pending": HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "already_pending"
        ],
        "hls_restricted_fallback_refresh_trigger_backoff_pending": HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "backoff_pending"
        ],
        "hls_restricted_fallback_refresh_trigger_failures": HLS_RESTRICTED_FALLBACK_TRIGGER_GOVERNANCE[
            "failures"
        ],
        "hls_restricted_fallback_refresh_trigger_tasks_active": active_tasks,
    }


def stream_refresh_policy_governance_snapshot(
    *,
    stream_refresh_latency_slo_ms: int,
) -> dict[str, int]:
    """Return route-adjacent stream refresh dispatch policy counters."""

    return {
        "stream_refresh_latency_slo_ms": stream_refresh_latency_slo_ms,
        "stream_refresh_policy_forced_queued": STREAM_REFRESH_POLICY_GOVERNANCE["forced_queued"],
        "stream_refresh_policy_forced_in_process": STREAM_REFRESH_POLICY_GOVERNANCE[
            "forced_in_process"
        ],
        "stream_refresh_policy_fallback_in_process": STREAM_REFRESH_POLICY_GOVERNANCE[
            "fallback_in_process"
        ],
        "stream_refresh_policy_latency_slo_breaches": STREAM_REFRESH_POLICY_GOVERNANCE[
            "latency_slo_breaches"
        ],
    }


def select_refresh_dispatch_preference(
    *,
    refresh_dispatch_mode: str,
    queued_controller_available: bool,
    runtime_governance_provider: RuntimeGovernanceProvider,
    runtime_pressure_evaluator: RuntimePressureEvaluator,
) -> bool:
    """Return whether route-adjacent refresh work should prefer queued dispatch."""

    if refresh_dispatch_mode == "queued":
        if queued_controller_available:
            return True
        STREAM_REFRESH_POLICY_GOVERNANCE["fallback_in_process"] += 1
        return False

    runtime_governance = runtime_governance_provider()
    requires_queue, latency_slo_breached = runtime_pressure_evaluator(runtime_governance)
    if latency_slo_breached:
        STREAM_REFRESH_POLICY_GOVERNANCE["latency_slo_breaches"] += 1
    if requires_queue and queued_controller_available:
        STREAM_REFRESH_POLICY_GOVERNANCE["forced_queued"] += 1
        return True
    STREAM_REFRESH_POLICY_GOVERNANCE["forced_in_process"] += 1
    if requires_queue and not queued_controller_available:
        STREAM_REFRESH_POLICY_GOVERNANCE["fallback_in_process"] += 1
    return False


def record_route_refresh_trigger_pending(
    *,
    governance: dict[str, int],
    item_identifier: str,
    controller: _RouteRefreshController,
) -> bool:
    """Record duplicate-trigger/backoff governance when route-adjacent work is already pending."""

    if not controller.has_pending(item_identifier):
        return False

    governance["already_pending"] += 1
    last_result = controller.get_last_result(item_identifier)
    if last_result is not None and getattr(last_result, "retry_after_seconds", None) is not None:
        governance["backoff_pending"] += 1
    return True
