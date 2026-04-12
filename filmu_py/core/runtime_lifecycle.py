"""Explicit runtime lifecycle graph and observable transition history."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum


class RuntimeLifecyclePhase(StrEnum):
    """Intentional runtime phases for bootstrap, steady state, and teardown."""

    BOOTSTRAP = "bootstrap"
    PLUGIN_REGISTRATION = "plugin_registration"
    STEADY_STATE = "steady_state"
    DEGRADED = "degraded"
    SHUTTING_DOWN = "shutting_down"


class RuntimeLifecycleHealth(StrEnum):
    """Current runtime health classification."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"


@dataclass(frozen=True, slots=True)
class RuntimeLifecycleTransition:
    """One immutable runtime lifecycle transition record."""

    phase: RuntimeLifecyclePhase
    health: RuntimeLifecycleHealth
    detail: str
    at: datetime


@dataclass(frozen=True, slots=True)
class RuntimeLifecycleSnapshot:
    """Point-in-time runtime lifecycle snapshot plus bounded history."""

    phase: RuntimeLifecyclePhase
    health: RuntimeLifecycleHealth
    detail: str
    updated_at: datetime
    transitions: tuple[RuntimeLifecycleTransition, ...]


class RuntimeLifecycleState:
    """Bounded in-memory lifecycle graph for app bootstrap and shutdown."""

    def __init__(self, *, history_limit: int = 32) -> None:
        self._history: deque[RuntimeLifecycleTransition] = deque(maxlen=max(1, history_limit))
        now = datetime.now(UTC)
        initial = RuntimeLifecycleTransition(
            phase=RuntimeLifecyclePhase.BOOTSTRAP,
            health=RuntimeLifecycleHealth.HEALTHY,
            detail="runtime_bootstrap_pending",
            at=now,
        )
        self._phase = initial.phase
        self._health = initial.health
        self._detail = initial.detail
        self._updated_at = initial.at
        self._history.append(initial)

    def transition(
        self,
        phase: RuntimeLifecyclePhase,
        *,
        detail: str,
        health: RuntimeLifecycleHealth = RuntimeLifecycleHealth.HEALTHY,
    ) -> RuntimeLifecycleSnapshot:
        """Record one lifecycle transition and return the new snapshot."""

        record = RuntimeLifecycleTransition(
            phase=phase,
            health=health,
            detail=detail,
            at=datetime.now(UTC),
        )
        self._phase = record.phase
        self._health = record.health
        self._detail = record.detail
        self._updated_at = record.at
        self._history.append(record)
        return self.snapshot()

    def snapshot(self) -> RuntimeLifecycleSnapshot:
        """Return the current runtime lifecycle snapshot."""

        return RuntimeLifecycleSnapshot(
            phase=self._phase,
            health=self._health,
            detail=self._detail,
            updated_at=self._updated_at,
            transitions=tuple(self._history),
        )
