"""Durable item-workflow drill history helpers for worker replay and compensation runs."""

from __future__ import annotations

import json
import time
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Literal, cast

from prometheus_client import Counter, Gauge

ITEM_WORKFLOW_DRILL_RUNS_TOTAL = Counter(
    "filmu_py_item_workflow_drill_runs_total",
    "Observed item-workflow drill runs by queue and outcome",
    ["queue_name", "outcome"],
)
ITEM_WORKFLOW_DRILL_LAST_COUNTS = Gauge(
    "filmu_py_item_workflow_drill_last_counts",
    "Latest item-workflow drill counters by queue and kind",
    ["queue_name", "kind"],
)
ITEM_WORKFLOW_DRILL_LAST_OUTCOME = Gauge(
    "filmu_py_item_workflow_drill_last_outcome",
    "Latest item-workflow drill outcome where ok=0, warning=1, critical=2",
    ["queue_name"],
)
ITEM_WORKFLOW_DRILL_LAST_RUN_FAILED = Gauge(
    "filmu_py_item_workflow_drill_last_run_failed",
    "Whether the latest item-workflow drill failed before completing",
    ["queue_name"],
)
ITEM_WORKFLOW_DRILL_LAST_RUN_TIMESTAMP = Gauge(
    "filmu_py_item_workflow_drill_last_run_timestamp_seconds",
    "Unix timestamp of the latest item-workflow drill run",
    ["queue_name"],
)

_HISTORY_KEY_PREFIX = "arq:item-workflow-drill-history:"
_OUTCOME_SCORES = {"ok": 0.0, "warning": 1.0, "critical": 2.0}
type ItemWorkflowDrillOutcome = Literal["ok", "warning", "critical"]


@dataclass(frozen=True, slots=True)
class ItemWorkflowDrillHistoryPoint:
    """Persisted item-workflow drill run record."""

    observed_at: str
    examined_checkpoints: int
    replayed_checkpoints: int
    compensated_checkpoints: int
    finalize_requeues: int
    parse_requeues: int
    scrape_requeues: int
    index_requeues: int
    skipped_active: int
    unrecoverable: int
    failed: int
    candidate_status_counts: dict[str, int]
    compensation_stage_counts: dict[str, int]
    outcome: ItemWorkflowDrillOutcome = "ok"
    run_failed: bool = False
    last_error: str | None = None


class ItemWorkflowDrillStatusStore:
    """Persist bounded item-workflow drill history to Redis primitives."""

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
        unrecoverable: int,
        failed: int,
        run_failed: bool,
    ) -> ItemWorkflowDrillOutcome:
        """Return the operator-facing severity for one drill run."""

        if run_failed:
            return "critical"
        if failed > 0 or unrecoverable > 0:
            return "warning"
        return "ok"

    def _publish_metrics(self, point: ItemWorkflowDrillHistoryPoint, *, now_seconds: float) -> None:
        ITEM_WORKFLOW_DRILL_RUNS_TOTAL.labels(
            queue_name=self.queue_name,
            outcome=point.outcome,
        ).inc()
        for kind, value in {
            "examined_checkpoints": point.examined_checkpoints,
            "replayed_checkpoints": point.replayed_checkpoints,
            "compensated_checkpoints": point.compensated_checkpoints,
            "finalize_requeues": point.finalize_requeues,
            "parse_requeues": point.parse_requeues,
            "scrape_requeues": point.scrape_requeues,
            "index_requeues": point.index_requeues,
            "skipped_active": point.skipped_active,
            "unrecoverable": point.unrecoverable,
            "failed": point.failed,
        }.items():
            ITEM_WORKFLOW_DRILL_LAST_COUNTS.labels(
                queue_name=self.queue_name,
                kind=kind,
            ).set(value)
        ITEM_WORKFLOW_DRILL_LAST_OUTCOME.labels(queue_name=self.queue_name).set(
            _OUTCOME_SCORES.get(point.outcome, 0.0)
        )
        ITEM_WORKFLOW_DRILL_LAST_RUN_FAILED.labels(queue_name=self.queue_name).set(
            1.0 if point.run_failed else 0.0
        )
        ITEM_WORKFLOW_DRILL_LAST_RUN_TIMESTAMP.labels(queue_name=self.queue_name).set(now_seconds)

    async def record_run(
        self,
        *,
        examined_checkpoints: int,
        replayed_checkpoints: int,
        compensated_checkpoints: int,
        finalize_requeues: int,
        parse_requeues: int,
        scrape_requeues: int,
        index_requeues: int,
        skipped_active: int,
        unrecoverable: int,
        failed: int,
        candidate_status_counts: dict[str, int] | None = None,
        compensation_stage_counts: dict[str, int] | None = None,
        run_failed: bool = False,
        last_error: str | None = None,
        now_seconds: float | None = None,
    ) -> ItemWorkflowDrillHistoryPoint:
        """Persist one bounded item-workflow drill run record."""

        current_time_seconds = time.time() if now_seconds is None else now_seconds
        point = ItemWorkflowDrillHistoryPoint(
            observed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(current_time_seconds)),
            examined_checkpoints=examined_checkpoints,
            replayed_checkpoints=replayed_checkpoints,
            compensated_checkpoints=compensated_checkpoints,
            finalize_requeues=finalize_requeues,
            parse_requeues=parse_requeues,
            scrape_requeues=scrape_requeues,
            index_requeues=index_requeues,
            skipped_active=skipped_active,
            unrecoverable=unrecoverable,
            failed=failed,
            candidate_status_counts=dict(sorted((candidate_status_counts or {}).items())),
            compensation_stage_counts=dict(sorted((compensation_stage_counts or {}).items())),
            outcome=self.classify_outcome(
                unrecoverable=unrecoverable,
                failed=failed,
                run_failed=run_failed,
            ),
            run_failed=run_failed,
            last_error=last_error,
        )
        lpush = getattr(self.redis, "lpush", None)
        ltrim = getattr(self.redis, "ltrim", None)
        if lpush is not None and ltrim is not None:
            payload = json.dumps(
                {
                    "observed_at": point.observed_at,
                    "examined_checkpoints": point.examined_checkpoints,
                    "replayed_checkpoints": point.replayed_checkpoints,
                    "compensated_checkpoints": point.compensated_checkpoints,
                    "finalize_requeues": point.finalize_requeues,
                    "parse_requeues": point.parse_requeues,
                    "scrape_requeues": point.scrape_requeues,
                    "index_requeues": point.index_requeues,
                    "skipped_active": point.skipped_active,
                    "unrecoverable": point.unrecoverable,
                    "failed": point.failed,
                    "candidate_status_counts": point.candidate_status_counts,
                    "compensation_stage_counts": point.compensation_stage_counts,
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
    def _coerce_counts(value: object) -> dict[str, int]:
        if not isinstance(value, dict):
            return {}
        counts: dict[str, int] = {}
        for key, raw_count in value.items():
            counts[str(key)] = ItemWorkflowDrillStatusStore._coerce_int(raw_count)
        return dict(sorted(counts.items()))

    @staticmethod
    def _coerce_outcome(value: object) -> ItemWorkflowDrillOutcome:
        if isinstance(value, str) and value in _OUTCOME_SCORES:
            return cast(ItemWorkflowDrillOutcome, value)
        return "ok"

    async def history(self, *, limit: int = 20) -> list[ItemWorkflowDrillHistoryPoint]:
        """Return bounded persisted item-workflow drill history newest-first."""

        lrange = getattr(self.redis, "lrange", None)
        if lrange is None:
            return []

        rows = await self._await_maybe(
            lrange(f"{_HISTORY_KEY_PREFIX}{self.queue_name}", 0, max(0, limit - 1))
        )
        history: list[ItemWorkflowDrillHistoryPoint] = []
        for row in cast(list[object], rows or []):
            raw = row.decode("utf-8") if isinstance(row, bytes) else str(row)
            try:
                payload = cast(dict[str, Any], json.loads(raw))
            except Exception:
                continue
            history.append(
                ItemWorkflowDrillHistoryPoint(
                    observed_at=str(payload.get("observed_at", "")),
                    examined_checkpoints=self._coerce_int(payload.get("examined_checkpoints", 0)),
                    replayed_checkpoints=self._coerce_int(payload.get("replayed_checkpoints", 0)),
                    compensated_checkpoints=self._coerce_int(
                        payload.get("compensated_checkpoints", 0)
                    ),
                    finalize_requeues=self._coerce_int(payload.get("finalize_requeues", 0)),
                    parse_requeues=self._coerce_int(payload.get("parse_requeues", 0)),
                    scrape_requeues=self._coerce_int(payload.get("scrape_requeues", 0)),
                    index_requeues=self._coerce_int(payload.get("index_requeues", 0)),
                    skipped_active=self._coerce_int(payload.get("skipped_active", 0)),
                    unrecoverable=self._coerce_int(payload.get("unrecoverable", 0)),
                    failed=self._coerce_int(payload.get("failed", 0)),
                    candidate_status_counts=self._coerce_counts(
                        payload.get("candidate_status_counts", {})
                    ),
                    compensation_stage_counts=self._coerce_counts(
                        payload.get("compensation_stage_counts", {})
                    ),
                    outcome=self._coerce_outcome(payload.get("outcome")),
                    run_failed=self._coerce_bool(payload.get("run_failed", False)),
                    last_error=self._coerce_optional_str(payload.get("last_error")),
                )
            )
        return history

    async def latest(self) -> ItemWorkflowDrillHistoryPoint | None:
        """Return the latest item-workflow drill run record when available."""

        history = await self.history(limit=1)
        return history[0] if history else None
