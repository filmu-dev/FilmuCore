"""Automated replay/control-plane recovery orchestration."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Literal, cast

from filmu_py.config import ControlPlaneAutomationSettings
from filmu_py.services.control_plane import ControlPlaneService, ControlPlaneSummary

_UNSET = object()


@dataclass(frozen=True, slots=True)
class ControlPlaneAutomationSnapshot:
    """Current background recovery posture and last-run outcome."""

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
    last_run_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    consecutive_failures: int = 0
    last_error: str | None = None
    remediation_updated_subscribers: int = 0
    rewound_subscribers: int = 0
    claimed_pending_events: int = 0
    claim_passes: int = 0
    pending_count_after: int | None = None
    summary: ControlPlaneSummary | None = None


class ControlPlaneAutomationController:
    """Run replay/control-plane recovery passes on a bounded background interval."""

    def __init__(
        self,
        *,
        service: ControlPlaneService | None,
        backplane: Any | None,
        consumer_group: str,
        automation: ControlPlaneAutomationSettings,
    ) -> None:
        self._service = service
        self._backplane = backplane
        self._consumer_group = consumer_group
        self._automation = automation
        self._task: asyncio.Task[None] | None = None
        self._snapshot = ControlPlaneAutomationSnapshot(
            enabled=automation.enabled,
            runner_status="disabled" if not automation.enabled else "stopped",
            interval_seconds=automation.interval_seconds,
            active_within_seconds=automation.active_within_seconds,
            pending_min_idle_ms=automation.pending_min_idle_ms,
            claim_limit=automation.claim_limit,
            max_claim_passes=automation.max_claim_passes,
            consumer_group=consumer_group,
            consumer_name=automation.consumer_name,
            service_attached=service is not None,
            backplane_attached=bool(backplane and hasattr(backplane, "claim_pending")),
        )

    def snapshot(self) -> ControlPlaneAutomationSnapshot:
        """Return the latest automation snapshot."""

        return self._snapshot

    def start(self) -> None:
        """Start the background automation loop when enabled."""

        if not self._automation.enabled or self._task is not None:
            return
        self._task = asyncio.create_task(self._run_loop())

    async def shutdown(self) -> None:
        """Stop the background automation loop."""

        task = self._task
        self._task = None
        if task is None:
            if self._automation.enabled:
                self._snapshot = self._replace_snapshot(runner_status="stopped")
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._snapshot = self._replace_snapshot(runner_status="stopped")

    async def run_once(self) -> ControlPlaneAutomationSnapshot:
        """Execute one automation pass immediately."""

        if not self._automation.enabled:
            self._snapshot = self._replace_snapshot(
                runner_status="disabled",
                remediation_updated_subscribers=0,
                rewound_subscribers=0,
                claimed_pending_events=0,
                claim_passes=0,
                pending_count_after=None,
                summary=None,
            )
            return self._snapshot

        started_at = datetime.now(UTC)
        if self._service is None:
            self._snapshot = self._replace_snapshot(
                runner_status="degraded",
                last_run_at=started_at,
                last_failure_at=started_at,
                consecutive_failures=self._snapshot.consecutive_failures + 1,
                last_error="control_plane_service_unavailable",
                remediation_updated_subscribers=0,
                rewound_subscribers=0,
                claimed_pending_events=0,
                claim_passes=0,
                pending_count_after=None,
                summary=None,
            )
            return self._snapshot

        try:
            remediation = await self._service.remediate_subscribers(
                active_within_seconds=self._automation.active_within_seconds
            )
            ack_recovery = await self._service.recover_ack_backlog(
                active_within_seconds=self._automation.active_within_seconds
            )

            claimed_pending_events = 0
            claim_passes = 0
            pending_count_after: int | None = None
            if self._backplane is not None and hasattr(self._backplane, "claim_pending"):
                start_id = "0-0"
                while claim_passes < self._automation.max_claim_passes:
                    result = await self._backplane.claim_pending(
                        group_name=self._consumer_group,
                        consumer_name=self._automation.consumer_name,
                        min_idle_ms=self._automation.pending_min_idle_ms,
                        count=self._automation.claim_limit,
                        start_id=start_id,
                        heartbeat_expiry_seconds=self._automation.active_within_seconds,
                    )
                    claim_passes += 1
                    claimed_pending_events += len(result.claimed_events)
                    pending_count_after = result.pending_after.pending_count
                    start_id = result.next_start_id
                    if pending_count_after <= 0 or not result.claimed_events:
                        break

            summary = await self._service.summarize_subscribers(
                active_within_seconds=self._automation.active_within_seconds
            )
            self._snapshot = self._replace_snapshot(
                runner_status="running",
                last_run_at=started_at,
                last_success_at=started_at,
                consecutive_failures=0,
                last_error=None,
                remediation_updated_subscribers=remediation.total_updated_subscribers,
                rewound_subscribers=ack_recovery.rewound_subscribers,
                claimed_pending_events=claimed_pending_events,
                claim_passes=claim_passes,
                pending_count_after=pending_count_after,
                summary=summary,
            )
            return self._snapshot
        except Exception as exc:
            self._snapshot = self._replace_snapshot(
                runner_status="degraded",
                last_run_at=started_at,
                last_failure_at=started_at,
                consecutive_failures=self._snapshot.consecutive_failures + 1,
                last_error=str(exc),
                remediation_updated_subscribers=0,
                rewound_subscribers=0,
                claimed_pending_events=0,
                claim_passes=0,
                pending_count_after=None,
                summary=None,
            )
            return self._snapshot

    async def _run_loop(self) -> None:
        """Continuously run automation passes until shutdown."""

        while True:
            await self.run_once()
            await asyncio.sleep(self._automation.interval_seconds)

    def _replace_snapshot(
        self,
        *,
        enabled: bool | None = None,
        runner_status: Literal["disabled", "running", "degraded", "stopped"] | None = None,
        interval_seconds: int | None = None,
        active_within_seconds: int | None = None,
        pending_min_idle_ms: int | None = None,
        claim_limit: int | None = None,
        max_claim_passes: int | None = None,
        consumer_group: str | None = None,
        consumer_name: str | None = None,
        service_attached: bool | None = None,
        backplane_attached: bool | None = None,
        last_run_at: datetime | None | object = _UNSET,
        last_success_at: datetime | None | object = _UNSET,
        last_failure_at: datetime | None | object = _UNSET,
        consecutive_failures: int | None = None,
        last_error: str | None | object = _UNSET,
        remediation_updated_subscribers: int | None = None,
        rewound_subscribers: int | None = None,
        claimed_pending_events: int | None = None,
        claim_passes: int | None = None,
        pending_count_after: int | None | object = _UNSET,
        summary: ControlPlaneSummary | None | object = _UNSET,
    ) -> ControlPlaneAutomationSnapshot:
        current = self._snapshot
        return replace(
            current,
            enabled=current.enabled if enabled is None else enabled,
            runner_status=current.runner_status if runner_status is None else runner_status,
            interval_seconds=(
                current.interval_seconds if interval_seconds is None else interval_seconds
            ),
            active_within_seconds=(
                current.active_within_seconds
                if active_within_seconds is None
                else active_within_seconds
            ),
            pending_min_idle_ms=(
                current.pending_min_idle_ms if pending_min_idle_ms is None else pending_min_idle_ms
            ),
            claim_limit=current.claim_limit if claim_limit is None else claim_limit,
            max_claim_passes=(
                current.max_claim_passes if max_claim_passes is None else max_claim_passes
            ),
            consumer_group=current.consumer_group if consumer_group is None else consumer_group,
            consumer_name=current.consumer_name if consumer_name is None else consumer_name,
            service_attached=(
                current.service_attached if service_attached is None else service_attached
            ),
            backplane_attached=(
                current.backplane_attached if backplane_attached is None else backplane_attached
            ),
            last_run_at=(
                current.last_run_at
                if last_run_at is _UNSET
                else cast(datetime | None, last_run_at)
            ),
            last_success_at=(
                current.last_success_at
                if last_success_at is _UNSET
                else cast(datetime | None, last_success_at)
            ),
            last_failure_at=(
                current.last_failure_at
                if last_failure_at is _UNSET
                else cast(datetime | None, last_failure_at)
            ),
            consecutive_failures=(
                current.consecutive_failures
                if consecutive_failures is None
                else consecutive_failures
            ),
            last_error=(
                current.last_error if last_error is _UNSET else cast(str | None, last_error)
            ),
            remediation_updated_subscribers=(
                current.remediation_updated_subscribers
                if remediation_updated_subscribers is None
                else remediation_updated_subscribers
            ),
            rewound_subscribers=(
                current.rewound_subscribers if rewound_subscribers is None else rewound_subscribers
            ),
            claimed_pending_events=(
                current.claimed_pending_events
                if claimed_pending_events is None
                else claimed_pending_events
            ),
            claim_passes=current.claim_passes if claim_passes is None else claim_passes,
            pending_count_after=(
                current.pending_count_after
                if pending_count_after is _UNSET
                else cast(int | None, pending_count_after)
            ),
            summary=(
                current.summary
                if summary is _UNSET
                else cast(ControlPlaneSummary | None, summary)
            ),
        )
