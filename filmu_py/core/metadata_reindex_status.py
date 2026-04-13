"""Metadata reindex/reconciliation run visibility helpers for operator routes and metrics."""

from __future__ import annotations

import json
import time
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Literal, cast

from prometheus_client import Counter, Gauge

METADATA_REINDEX_RUNS_TOTAL = Counter(
    "filmu_py_metadata_reindex_runs_total",
    "Observed metadata reindex/reconciliation runs by queue and outcome",
    ["queue_name", "outcome"],
)
METADATA_REINDEX_LAST_COUNTS = Gauge(
    "filmu_py_metadata_reindex_last_counts",
    "Latest metadata reindex/reconciliation run counters by queue and kind",
    ["queue_name", "kind"],
)
METADATA_REINDEX_LAST_OUTCOME = Gauge(
    "filmu_py_metadata_reindex_last_outcome",
    "Latest metadata reindex/reconciliation outcome where ok=0, warning=1, critical=2",
    ["queue_name"],
)
METADATA_REINDEX_LAST_RUN_FAILED = Gauge(
    "filmu_py_metadata_reindex_last_run_failed",
    "Whether the latest metadata reindex/reconciliation run failed before completing",
    ["queue_name"],
)
METADATA_REINDEX_LAST_RUN_TIMESTAMP = Gauge(
    "filmu_py_metadata_reindex_last_run_timestamp_seconds",
    "Unix timestamp of the latest metadata reindex/reconciliation run",
    ["queue_name"],
)

_HISTORY_KEY_PREFIX = "arq:metadata-reindex-history:"
_OUTCOME_SCORES = {"ok": 0.0, "warning": 1.0, "critical": 2.0}
type MetadataReindexOutcome = Literal["ok", "warning", "critical"]


@dataclass(frozen=True, slots=True)
class MetadataReindexHistoryPoint:
    """Persisted metadata reindex/reconciliation run record."""

    observed_at: str
    processed: int
    queued: int
    reconciled: int
    skipped_active: int
    failed: int
    outcome: MetadataReindexOutcome
    run_failed: bool = False
    last_error: str | None = None


class MetadataReindexStatusStore:
    """Persist bounded metadata reindex/reconciliation history to Redis primitives."""

    def __init__(
        self,
        redis: object,
        *,
        queue_name: str,
        history_limit: int = 48,
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
        failed: int,
        run_failed: bool,
    ) -> MetadataReindexOutcome:
        """Return the operator-facing severity for one run."""

        if run_failed:
            return "critical"
        if failed > 0:
            return "warning"
        return "ok"

    def _publish_metrics(self, point: MetadataReindexHistoryPoint, *, now_seconds: float) -> None:
        METADATA_REINDEX_RUNS_TOTAL.labels(
            queue_name=self.queue_name,
            outcome=point.outcome,
        ).inc()
        for kind, value in {
            "processed": point.processed,
            "queued": point.queued,
            "reconciled": point.reconciled,
            "skipped_active": point.skipped_active,
            "failed": point.failed,
        }.items():
            METADATA_REINDEX_LAST_COUNTS.labels(
                queue_name=self.queue_name,
                kind=kind,
            ).set(value)
        METADATA_REINDEX_LAST_OUTCOME.labels(queue_name=self.queue_name).set(
            _OUTCOME_SCORES.get(point.outcome, 0.0)
        )
        METADATA_REINDEX_LAST_RUN_FAILED.labels(queue_name=self.queue_name).set(
            1.0 if point.run_failed else 0.0
        )
        METADATA_REINDEX_LAST_RUN_TIMESTAMP.labels(queue_name=self.queue_name).set(now_seconds)

    async def record_run(
        self,
        *,
        processed: int,
        queued: int,
        reconciled: int,
        skipped_active: int,
        failed: int,
        run_failed: bool = False,
        last_error: str | None = None,
        now_seconds: float | None = None,
    ) -> MetadataReindexHistoryPoint:
        """Persist one bounded metadata reindex/reconciliation run record."""

        current_time_seconds = time.time() if now_seconds is None else now_seconds
        outcome = self.classify_outcome(failed=failed, run_failed=run_failed)
        point = MetadataReindexHistoryPoint(
            observed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(current_time_seconds)),
            processed=processed,
            queued=queued,
            reconciled=reconciled,
            skipped_active=skipped_active,
            failed=failed,
            outcome=outcome,
            run_failed=run_failed,
            last_error=last_error,
        )
        lpush = getattr(self.redis, "lpush", None)
        ltrim = getattr(self.redis, "ltrim", None)
        if lpush is not None and ltrim is not None:
            payload = json.dumps(
                {
                    "observed_at": point.observed_at,
                    "processed": point.processed,
                    "queued": point.queued,
                    "reconciled": point.reconciled,
                    "skipped_active": point.skipped_active,
                    "failed": point.failed,
                    "outcome": point.outcome,
                    "run_failed": point.run_failed,
                    "last_error": point.last_error,
                },
                separators=(",", ":"),
            )
            history_key = f"{_HISTORY_KEY_PREFIX}{self.queue_name}"
            await self._await_maybe(lpush(history_key, payload))
            await self._await_maybe(ltrim(history_key, 0, max(0, self.history_limit - 1)))
        self._publish_metrics(point, now_seconds=current_time_seconds)
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
    def _coerce_bool(value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @staticmethod
    def _coerce_optional_str(value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _coerce_outcome(value: object) -> MetadataReindexOutcome:
        if isinstance(value, str) and value in _OUTCOME_SCORES:
            return cast(MetadataReindexOutcome, value)
        return "ok"

    async def history(self, *, limit: int = 20) -> list[MetadataReindexHistoryPoint]:
        """Return bounded persisted metadata reindex/reconciliation history newest-first."""

        lrange = getattr(self.redis, "lrange", None)
        if lrange is None:
            return []

        rows = await self._await_maybe(
            lrange(f"{_HISTORY_KEY_PREFIX}{self.queue_name}", 0, max(0, limit - 1))
        )
        history: list[MetadataReindexHistoryPoint] = []
        for row in cast(list[object], rows):
            raw = row.decode("utf-8") if isinstance(row, bytes) else str(row)
            try:
                payload = cast(dict[str, Any], json.loads(raw))
            except Exception:
                continue
            history.append(
                MetadataReindexHistoryPoint(
                    observed_at=str(payload.get("observed_at", "")),
                    processed=self._coerce_int(payload.get("processed", 0)),
                    queued=self._coerce_int(payload.get("queued", 0)),
                    reconciled=self._coerce_int(payload.get("reconciled", 0)),
                    skipped_active=self._coerce_int(payload.get("skipped_active", 0)),
                    failed=self._coerce_int(payload.get("failed", 0)),
                    outcome=self._coerce_outcome(payload.get("outcome")),
                    run_failed=self._coerce_bool(payload.get("run_failed", False)),
                    last_error=self._coerce_optional_str(payload.get("last_error")),
                )
            )
        return history

    async def latest(self) -> MetadataReindexHistoryPoint | None:
        """Return the latest metadata reindex/reconciliation run record when available."""

        history = await self.history(limit=1)
        return history[0] if history else None
