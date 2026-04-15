from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from filmu_py.config import ControlPlaneAutomationSettings
from filmu_py.services.control_plane_automation import ControlPlaneAutomationController


@dataclass
class _FakeSummary:
    total_subscribers: int = 1
    active_subscribers: int = 1
    stale_subscribers: int = 0
    error_subscribers: int = 0
    fenced_subscribers: int = 0
    ack_pending_subscribers: int = 0
    stream_count: int = 1
    group_count: int = 1
    node_count: int = 1
    tenant_count: int = 1
    oldest_heartbeat_age_seconds: float | None = 5.0
    status_counts: dict[str, int] = None  # type: ignore[assignment]
    required_actions: tuple[str, ...] = ()
    remaining_gaps: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.status_counts is None:
            self.status_counts = {"active": 1}


class _FakeService:
    def __init__(self) -> None:
        self.remediation_calls = 0
        self.ack_calls = 0
        self.summary = _FakeSummary()

    async def remediate_subscribers(self, *, active_within_seconds: int) -> Any:
        _ = active_within_seconds
        self.remediation_calls += 1
        result = type("RemediationResult", (), {})()
        result.total_updated_subscribers = 2
        return result

    async def recover_ack_backlog(self, *, active_within_seconds: int) -> Any:
        _ = active_within_seconds
        self.ack_calls += 1
        result = type("AckRecoveryResult", (), {})()
        result.rewound_subscribers = 1
        return result

    async def summarize_subscribers(self, *, active_within_seconds: int) -> _FakeSummary:
        _ = active_within_seconds
        return self.summary


class _FakeBackplane:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def claim_pending(
        self,
        *,
        group_name: str,
        consumer_name: str,
        min_idle_ms: int,
        count: int,
        start_id: str,
        heartbeat_expiry_seconds: int,
    ) -> Any:
        self.calls.append(
            {
                "group_name": group_name,
                "consumer_name": consumer_name,
                "min_idle_ms": min_idle_ms,
                "count": count,
                "start_id": start_id,
                "heartbeat_expiry_seconds": heartbeat_expiry_seconds,
            }
        )
        result = type("ReplayPendingClaimResult", (), {})()
        if len(self.calls) == 1:
            result.claimed_events = [type("ReplayEvent", (), {"event_id": "21-0"})()]
            result.next_start_id = "22-0"
            result.pending_after = type("PendingSummary", (), {"pending_count": 1})()
        else:
            result.claimed_events = [type("ReplayEvent", (), {"event_id": "22-0"})()]
            result.next_start_id = "23-0"
            result.pending_after = type("PendingSummary", (), {"pending_count": 0})()
        return result


def test_control_plane_automation_controller_runs_recovery_pass() -> None:
    controller = ControlPlaneAutomationController(
        service=_FakeService(),  # type: ignore[arg-type]
        backplane=_FakeBackplane(),
        consumer_group="filmu-api",
        automation=ControlPlaneAutomationSettings(
            enabled=True,
            interval_seconds=300,
            active_within_seconds=120,
            pending_min_idle_ms=60_000,
            claim_limit=25,
            max_claim_passes=3,
            consumer_name="recovery-automation",
        ),
    )

    snapshot = asyncio.run(controller.run_once())

    assert snapshot.runner_status == "running"
    assert snapshot.remediation_updated_subscribers == 2
    assert snapshot.rewound_subscribers == 1
    assert snapshot.claimed_pending_events == 2
    assert snapshot.claim_passes == 2
    assert snapshot.pending_count_after == 0
    assert snapshot.last_success_at is not None


def test_control_plane_automation_controller_marks_failures_degraded() -> None:
    class _FailingService(_FakeService):
        async def remediate_subscribers(self, *, active_within_seconds: int) -> Any:
            _ = active_within_seconds
            raise RuntimeError("boom")

    controller = ControlPlaneAutomationController(
        service=_FailingService(),  # type: ignore[arg-type]
        backplane=None,
        consumer_group="filmu-api",
        automation=ControlPlaneAutomationSettings(enabled=True),
    )

    snapshot = asyncio.run(controller.run_once())

    assert snapshot.runner_status == "degraded"
    assert snapshot.consecutive_failures == 1
    assert snapshot.last_failure_at is not None
    assert snapshot.last_error == "boom"


def test_control_plane_automation_controller_clears_stale_success_metrics_on_failure() -> None:
    class _ToggleService(_FakeService):
        def __init__(self) -> None:
            super().__init__()
            self.fail = False

        async def remediate_subscribers(self, *, active_within_seconds: int) -> Any:
            if self.fail:
                raise RuntimeError("boom")
            return await super().remediate_subscribers(
                active_within_seconds=active_within_seconds
            )

    service = _ToggleService()
    controller = ControlPlaneAutomationController(
        service=service,  # type: ignore[arg-type]
        backplane=_FakeBackplane(),
        consumer_group="filmu-api",
        automation=ControlPlaneAutomationSettings(enabled=True),
    )

    success_snapshot = asyncio.run(controller.run_once())
    service.fail = True
    failure_snapshot = asyncio.run(controller.run_once())

    assert success_snapshot.runner_status == "running"
    assert success_snapshot.remediation_updated_subscribers == 2
    assert success_snapshot.rewound_subscribers == 1
    assert success_snapshot.claimed_pending_events == 2
    assert success_snapshot.claim_passes == 2
    assert success_snapshot.pending_count_after == 0
    assert success_snapshot.summary is not None
    assert failure_snapshot.runner_status == "degraded"
    assert failure_snapshot.last_success_at is not None
    assert failure_snapshot.last_failure_at is not None
    assert failure_snapshot.remediation_updated_subscribers == 0
    assert failure_snapshot.rewound_subscribers == 0
    assert failure_snapshot.claimed_pending_events == 0
    assert failure_snapshot.claim_passes == 0
    assert failure_snapshot.pending_count_after is None
    assert failure_snapshot.summary is None
