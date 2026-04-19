"""Queued plugin-hook delivery history helpers for worker lag and health rollups."""

from __future__ import annotations

import json
import time
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Literal, cast

from prometheus_client import Counter, Gauge, Histogram

PLUGIN_HOOK_QUEUE_DELIVERIES_TOTAL = Counter(
    "filmu_py_plugin_hook_queue_deliveries_total",
    "Observed queued plugin-hook deliveries by queue, plugin, event, and outcome",
    ["queue_name", "plugin_name", "event_type", "outcome"],
)
PLUGIN_HOOK_QUEUE_LAG_SECONDS = Histogram(
    "filmu_py_plugin_hook_queue_lag_seconds",
    "Lag between queue enqueue and queued plugin-hook execution",
    ["queue_name", "plugin_name", "event_type"],
    buckets=[0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 15.0, 30.0, 60.0],
)
PLUGIN_HOOK_QUEUE_EXECUTION_SECONDS = Histogram(
    "filmu_py_plugin_hook_queue_execution_seconds",
    "Queued plugin-hook execution duration observed by plugin and event",
    ["queue_name", "plugin_name", "event_type"],
    buckets=[0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 15.0],
)
PLUGIN_HOOK_QUEUE_LAST_LAG_SECONDS = Gauge(
    "filmu_py_plugin_hook_queue_last_lag_seconds",
    "Latest queued plugin-hook lag by queue and plugin",
    ["queue_name", "plugin_name"],
)
PLUGIN_HOOK_QUEUE_LAST_ATTEMPT = Gauge(
    "filmu_py_plugin_hook_queue_last_attempt",
    "Latest queued plugin-hook worker attempt by queue and plugin",
    ["queue_name", "plugin_name"],
)
PLUGIN_HOOK_QUEUE_LAST_OUTCOME = Gauge(
    "filmu_py_plugin_hook_queue_last_outcome",
    "Latest queued plugin-hook outcome where ok=0, warning=1, critical=2",
    ["queue_name", "plugin_name"],
)

_HISTORY_KEY_PREFIX = "arq:plugin-hook-history:"
_OUTCOME_SCORES = {"ok": 0.0, "warning": 1.0, "critical": 2.0}
type PluginHookQueueOutcome = Literal["ok", "warning", "critical"]


@dataclass(frozen=True, slots=True)
class PluginHookQueueHistoryPoint:
    """Persisted queued plugin-hook delivery result."""

    observed_at: str
    plugin_name: str
    event_type: str
    queue_lag_seconds: float
    execution_duration_seconds: float
    matched_hooks: int
    successful_hooks: int
    timeout_hooks: int
    failed_hooks: int
    attempt: int
    outcome: PluginHookQueueOutcome
    last_error: str | None = None


class PluginHookQueueStatusStore:
    """Persist bounded queued plugin-hook history to Redis list primitives."""

    def __init__(
        self,
        redis: object,
        *,
        queue_name: str,
        history_limit: int = 200,
    ) -> None:
        self.redis = redis
        self.queue_name = queue_name
        self.history_limit = history_limit

    async def _await_maybe(self, value: object) -> object:
        if isinstance(value, Awaitable):
            return await value
        return value

    @staticmethod
    def classify_outcome(
        *,
        matched_hooks: int,
        timeout_hooks: int,
        failed_hooks: int,
        attempt: int,
    ) -> PluginHookQueueOutcome:
        """Return the operator-facing severity for one queued delivery."""

        if matched_hooks <= 0 or failed_hooks > 0:
            return "critical"
        if timeout_hooks > 0 or attempt > 1:
            return "warning"
        return "ok"

    async def record_delivery(
        self,
        *,
        plugin_name: str,
        event_type: str,
        queued_at_seconds: float | None,
        execution_duration_seconds: float,
        matched_hooks: int,
        successful_hooks: int,
        timeout_hooks: int,
        failed_hooks: int,
        attempt: int,
        last_error: str | None = None,
        now_seconds: float | None = None,
    ) -> PluginHookQueueHistoryPoint:
        """Persist one bounded queued plugin-hook delivery record."""

        current_time_seconds = time.time() if now_seconds is None else now_seconds
        queue_lag_seconds = max(
            0.0,
            current_time_seconds - queued_at_seconds,
        ) if queued_at_seconds is not None else 0.0
        point = PluginHookQueueHistoryPoint(
            observed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(current_time_seconds)),
            plugin_name=plugin_name,
            event_type=event_type,
            queue_lag_seconds=queue_lag_seconds,
            execution_duration_seconds=max(0.0, execution_duration_seconds),
            matched_hooks=max(0, matched_hooks),
            successful_hooks=max(0, successful_hooks),
            timeout_hooks=max(0, timeout_hooks),
            failed_hooks=max(0, failed_hooks),
            attempt=max(1, attempt),
            outcome=self.classify_outcome(
                matched_hooks=matched_hooks,
                timeout_hooks=timeout_hooks,
                failed_hooks=failed_hooks,
                attempt=attempt,
            ),
            last_error=self._coerce_optional_str(last_error),
        )
        lpush = getattr(self.redis, "lpush", None)
        ltrim = getattr(self.redis, "ltrim", None)
        if lpush is not None and ltrim is not None:
            payload = json.dumps(
                {
                    "observed_at": point.observed_at,
                    "plugin_name": point.plugin_name,
                    "event_type": point.event_type,
                    "queue_lag_seconds": point.queue_lag_seconds,
                    "execution_duration_seconds": point.execution_duration_seconds,
                    "matched_hooks": point.matched_hooks,
                    "successful_hooks": point.successful_hooks,
                    "timeout_hooks": point.timeout_hooks,
                    "failed_hooks": point.failed_hooks,
                    "attempt": point.attempt,
                    "outcome": point.outcome,
                    "last_error": point.last_error,
                },
                separators=(",", ":"),
            )
            history_key = f"{_HISTORY_KEY_PREFIX}{self.queue_name}"
            await self._await_maybe(lpush(history_key, payload))
            await self._await_maybe(ltrim(history_key, 0, max(0, self.history_limit - 1)))

        PLUGIN_HOOK_QUEUE_DELIVERIES_TOTAL.labels(
            queue_name=self.queue_name,
            plugin_name=plugin_name,
            event_type=event_type,
            outcome=point.outcome,
        ).inc()
        PLUGIN_HOOK_QUEUE_LAG_SECONDS.labels(
            queue_name=self.queue_name,
            plugin_name=plugin_name,
            event_type=event_type,
        ).observe(point.queue_lag_seconds)
        PLUGIN_HOOK_QUEUE_EXECUTION_SECONDS.labels(
            queue_name=self.queue_name,
            plugin_name=plugin_name,
            event_type=event_type,
        ).observe(point.execution_duration_seconds)
        PLUGIN_HOOK_QUEUE_LAST_LAG_SECONDS.labels(
            queue_name=self.queue_name,
            plugin_name=plugin_name,
        ).set(point.queue_lag_seconds)
        PLUGIN_HOOK_QUEUE_LAST_ATTEMPT.labels(
            queue_name=self.queue_name,
            plugin_name=plugin_name,
        ).set(point.attempt)
        PLUGIN_HOOK_QUEUE_LAST_OUTCOME.labels(
            queue_name=self.queue_name,
            plugin_name=plugin_name,
        ).set(_OUTCOME_SCORES.get(point.outcome, 0.0))
        return point

    @staticmethod
    def _coerce_int(value: object, *, default: int = 0) -> int:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return default
        return default

    @staticmethod
    def _coerce_float(value: object, *, default: float = 0.0) -> float:
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return default
        return default

    @staticmethod
    def _coerce_optional_str(value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _coerce_outcome(value: object) -> PluginHookQueueOutcome:
        if isinstance(value, str) and value in _OUTCOME_SCORES:
            return cast(PluginHookQueueOutcome, value)
        return "ok"

    async def history(
        self,
        *,
        limit: int = 50,
        plugin_name: str | None = None,
    ) -> list[PluginHookQueueHistoryPoint]:
        """Return bounded queued plugin-hook history newest-first."""

        lrange = getattr(self.redis, "lrange", None)
        if lrange is None:
            return []

        rows = await self._await_maybe(
            lrange(f"{_HISTORY_KEY_PREFIX}{self.queue_name}", 0, max(0, limit - 1))
        )
        history: list[PluginHookQueueHistoryPoint] = []
        for row in cast(list[object], rows or []):
            raw = row.decode("utf-8") if isinstance(row, bytes) else str(row)
            try:
                payload = cast(dict[str, Any], json.loads(raw))
            except Exception:
                continue
            point = PluginHookQueueHistoryPoint(
                observed_at=str(payload.get("observed_at", "")),
                plugin_name=str(payload.get("plugin_name", "")),
                event_type=str(payload.get("event_type", "")),
                queue_lag_seconds=self._coerce_float(payload.get("queue_lag_seconds", 0.0)),
                execution_duration_seconds=self._coerce_float(
                    payload.get("execution_duration_seconds", 0.0)
                ),
                matched_hooks=self._coerce_int(payload.get("matched_hooks", 0)),
                successful_hooks=self._coerce_int(payload.get("successful_hooks", 0)),
                timeout_hooks=self._coerce_int(payload.get("timeout_hooks", 0)),
                failed_hooks=self._coerce_int(payload.get("failed_hooks", 0)),
                attempt=self._coerce_int(payload.get("attempt", 1), default=1),
                outcome=self._coerce_outcome(payload.get("outcome", "ok")),
                last_error=self._coerce_optional_str(payload.get("last_error")),
            )
            if plugin_name is not None and point.plugin_name != plugin_name:
                continue
            history.append(point)
        return history

    async def latest(self, *, plugin_name: str | None = None) -> PluginHookQueueHistoryPoint | None:
        """Return the latest queued plugin-hook history point when available."""

        history = await self.history(limit=self.history_limit, plugin_name=plugin_name)
        return history[0] if history else None
