"""ARQ queue visibility helpers for operator routes and metrics."""

from __future__ import annotations

import json
import time
from collections.abc import AsyncIterable, Awaitable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, cast

from arq.constants import in_progress_key_prefix, result_key_prefix, retry_key_prefix
from prometheus_client import Gauge

QUEUE_JOBS = Gauge(
    "filmu_py_queue_jobs",
    "Observed ARQ queue job counts by queue and state",
    ["queue_name", "state"],
)
QUEUE_OLDEST_READY_AGE_SECONDS = Gauge(
    "filmu_py_queue_oldest_ready_age_seconds",
    "Age in seconds of the oldest ready ARQ job",
    ["queue_name"],
)
QUEUE_NEXT_SCHEDULED_IN_SECONDS = Gauge(
    "filmu_py_queue_next_scheduled_in_seconds",
    "Time in seconds until the next deferred ARQ job is due",
    ["queue_name"],
)
QUEUE_ALERT_LEVEL = Gauge(
    "filmu_py_queue_alert_level",
    "Current queue alert level where ok=0, warning=1, critical=2",
    ["queue_name"],
)

_DEAD_LETTER_KEY_PREFIX = "arq:dead-letter:"
_HISTORY_KEY_PREFIX = "arq:queue-status-history:"
_ALERT_SCORES = {"ok": 0.0, "warning": 1.0, "critical": 2.0}
type AlertLevel = Literal["ok", "warning", "critical"]
type AlertSeverity = Literal["warning", "critical"]


@dataclass(frozen=True, slots=True)
class QueueAlert:
    """One classified queue-health alert."""

    code: str
    severity: AlertSeverity
    message: str


@dataclass(frozen=True, slots=True)
class QueueStatusHistoryPoint:
    """Persisted queue snapshot for operator trend inspection."""

    observed_at: str
    total_jobs: int
    ready_jobs: int
    deferred_jobs: int
    in_progress_jobs: int
    retry_jobs: int
    dead_letter_jobs: int
    oldest_ready_age_seconds: float | None
    next_scheduled_in_seconds: float | None
    alert_level: AlertLevel
    dead_letter_oldest_age_seconds: float | None = None
    dead_letter_reason_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class QueueStatusSnapshot:
    """Current operator-facing ARQ queue status snapshot."""

    observed_at: str
    queue_name: str
    total_jobs: int
    ready_jobs: int
    deferred_jobs: int
    in_progress_jobs: int
    retry_jobs: int
    result_jobs: int
    dead_letter_jobs: int
    oldest_ready_age_seconds: float | None
    next_scheduled_in_seconds: float | None
    alert_level: AlertLevel
    alerts: tuple[QueueAlert, ...]
    dead_letter_oldest_age_seconds: float | None = None
    dead_letter_reason_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DeadLetterSample:
    """One persisted dead-letter payload normalized for graph/operator consumers."""

    stage: str
    task: str
    item_id: str
    reason: str
    reason_code: str
    idempotency_key: str
    attempt: int
    queued_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class QueueHistorySummary:
    """Aggregate rollup derived from bounded queue-history points."""

    point_count: int
    warning_point_count: int
    critical_point_count: int
    max_total_jobs: int
    max_ready_jobs: int
    max_retry_jobs: int
    max_dead_letter_jobs: int
    latest_alert_level: AlertLevel
    dead_letter_reason_counts: dict[str, int] = field(default_factory=dict)


class QueueStatusReader:
    """Read one bounded ARQ queue snapshot from Redis primitives."""

    def __init__(
        self,
        redis: object,
        *,
        queue_name: str,
        history_limit: int = 48,
        backlog_warning_threshold: int = 25,
        backlog_critical_threshold: int = 100,
        ready_age_warning_seconds: float = 60.0,
        ready_age_critical_seconds: float = 300.0,
        retry_warning_threshold: int = 10,
        dead_letter_warning_threshold: int = 1,
    ) -> None:
        self.redis = redis
        self.queue_name = queue_name
        self.history_limit = history_limit
        self.backlog_warning_threshold = backlog_warning_threshold
        self.backlog_critical_threshold = backlog_critical_threshold
        self.ready_age_warning_seconds = ready_age_warning_seconds
        self.ready_age_critical_seconds = ready_age_critical_seconds
        self.retry_warning_threshold = retry_warning_threshold
        self.dead_letter_warning_threshold = dead_letter_warning_threshold

    async def _await_maybe(self, value: object) -> object:
        if isinstance(value, Awaitable):
            return await value
        return value

    async def _count_matching_keys(self, pattern: str) -> int:
        scan_iter = getattr(self.redis, "scan_iter", None)
        if scan_iter is None:
            return 0

        iterator = scan_iter(match=pattern)
        count = 0
        if isinstance(iterator, AsyncIterable):
            async for _item in cast(AsyncIterable[object], iterator):
                count += 1
            return count

        for _item in cast(Iterable[object], iterator):
            count += 1
        return count

    async def _first_scheduled_score(
        self,
        *,
        minimum: str,
        maximum: str | int,
    ) -> float | None:
        zrangebyscore = getattr(self.redis, "zrangebyscore", None)
        if zrangebyscore is None:
            return None

        rows = await self._await_maybe(
            zrangebyscore(
                self.queue_name,
                minimum,
                maximum,
                start=0,
                num=1,
                withscores=True,
            )
        )
        values = cast(list[tuple[object, float]], rows)
        if not values:
            return None
        return float(values[0][1])

    async def _persist_history(self, snapshot: QueueStatusSnapshot) -> None:
        history_key = f"{_HISTORY_KEY_PREFIX}{snapshot.queue_name}"
        lpush = getattr(self.redis, "lpush", None)
        ltrim = getattr(self.redis, "ltrim", None)
        if lpush is None or ltrim is None:
            return

        payload = json.dumps(
            {
                "observed_at": snapshot.observed_at,
                "total_jobs": snapshot.total_jobs,
                "ready_jobs": snapshot.ready_jobs,
                "deferred_jobs": snapshot.deferred_jobs,
                "in_progress_jobs": snapshot.in_progress_jobs,
                "retry_jobs": snapshot.retry_jobs,
                "dead_letter_jobs": snapshot.dead_letter_jobs,
                "oldest_ready_age_seconds": snapshot.oldest_ready_age_seconds,
                "next_scheduled_in_seconds": snapshot.next_scheduled_in_seconds,
                "alert_level": snapshot.alert_level,
                "dead_letter_oldest_age_seconds": snapshot.dead_letter_oldest_age_seconds,
                "dead_letter_reason_counts": snapshot.dead_letter_reason_counts,
            },
            separators=(",", ":"),
        )
        await self._await_maybe(lpush(history_key, payload))
        await self._await_maybe(ltrim(history_key, 0, max(0, self.history_limit - 1)))

    async def _dead_letter_payloads(
        self,
        *,
        start: int = 0,
        stop: int = -1,
    ) -> list[dict[str, Any]]:
        lrange = getattr(self.redis, "lrange", None)
        if lrange is None:
            return []

        rows = await self._await_maybe(lrange(f"{_DEAD_LETTER_KEY_PREFIX}{self.queue_name}", start, stop))
        payloads: list[dict[str, Any]] = []
        for row in cast(list[object], rows):
            raw = row.decode("utf-8") if isinstance(row, bytes) else str(row)
            try:
                payload = cast(dict[str, Any], json.loads(raw))
            except Exception:
                continue
            payloads.append(payload)
        return payloads

    @staticmethod
    def _dead_letter_sample_from_payload(payload: dict[str, Any]) -> DeadLetterSample | None:
        stage = payload.get("stage")
        task = payload.get("task")
        item_id = payload.get("item_id")
        reason = payload.get("reason")
        reason_code = payload.get("reason_code")
        idempotency_key = payload.get("idempotency_key")
        queued_at = payload.get("queued_at")
        metadata = payload.get("metadata", {})
        if not (
            isinstance(stage, str)
            and stage.strip()
            and isinstance(task, str)
            and task.strip()
            and isinstance(item_id, str)
            and item_id.strip()
            and isinstance(reason, str)
            and reason.strip()
            and isinstance(reason_code, str)
            and reason_code.strip()
            and isinstance(idempotency_key, str)
            and idempotency_key.strip()
            and isinstance(queued_at, str)
            and queued_at.strip()
        ):
            return None
        attempt = QueueStatusReader._coerce_int(payload.get("attempt", 0))
        normalized_metadata = (
            cast(dict[str, Any], metadata)
            if isinstance(metadata, dict)
            else {}
        )
        return DeadLetterSample(
            stage=stage,
            task=task,
            item_id=item_id,
            reason=reason,
            reason_code=reason_code,
            idempotency_key=idempotency_key,
            attempt=attempt,
            queued_at=queued_at,
            metadata=normalized_metadata,
        )

    @staticmethod
    def _payload_queued_age_seconds(
        payload: dict[str, Any],
        *,
        now_seconds: float,
    ) -> float | None:
        queued_at = payload.get("queued_at")
        if not isinstance(queued_at, str) or not queued_at.strip():
            return None
        try:
            parsed = datetime.fromisoformat(queued_at.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return max(0.0, now_seconds - parsed.timestamp())

    @staticmethod
    def _dead_letter_reason_counts_from_payloads(
        payloads: Iterable[dict[str, Any]],
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for payload in payloads:
            reason_code = payload.get("reason_code")
            if not isinstance(reason_code, str) or not reason_code:
                continue
            counts[reason_code] = counts.get(reason_code, 0) + 1
        return counts

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
    def _coerce_optional_float(value: object) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return None
        return None

    @staticmethod
    def _coerce_alert_level(value: object) -> AlertLevel:
        if value in {"ok", "warning", "critical"}:
            return cast(AlertLevel, value)
        return "ok"

    def _classify_alerts(
        self,
        *,
        ready_jobs: int,
        retry_jobs: int,
        dead_letter_jobs: int,
        oldest_ready_age_seconds: float | None,
    ) -> tuple[AlertLevel, tuple[QueueAlert, ...]]:
        alerts: list[QueueAlert] = []

        if dead_letter_jobs >= self.dead_letter_warning_threshold:
            alerts.append(
                QueueAlert(
                    code="dead_letter_backlog",
                    severity="critical",
                    message=f"Dead-letter queue contains {dead_letter_jobs} job(s).",
                )
            )

        if ready_jobs >= self.backlog_critical_threshold:
            alerts.append(
                QueueAlert(
                    code="ready_backlog_critical",
                    severity="critical",
                    message=f"Ready queue backlog reached {ready_jobs} job(s).",
                )
            )
        elif ready_jobs >= self.backlog_warning_threshold:
            alerts.append(
                QueueAlert(
                    code="ready_backlog_warning",
                    severity="warning",
                    message=f"Ready queue backlog reached {ready_jobs} job(s).",
                )
            )

        if oldest_ready_age_seconds is not None:
            if oldest_ready_age_seconds >= self.ready_age_critical_seconds:
                alerts.append(
                    QueueAlert(
                        code="ready_age_critical",
                        severity="critical",
                        message=(
                            "Oldest ready job age reached "
                            f"{oldest_ready_age_seconds:.1f} seconds."
                        ),
                    )
                )
            elif oldest_ready_age_seconds >= self.ready_age_warning_seconds:
                alerts.append(
                    QueueAlert(
                        code="ready_age_warning",
                        severity="warning",
                        message=(
                            "Oldest ready job age reached "
                            f"{oldest_ready_age_seconds:.1f} seconds."
                        ),
                    )
                )

        if retry_jobs >= self.retry_warning_threshold:
            alerts.append(
                QueueAlert(
                    code="retry_pressure",
                    severity="warning",
                    message=f"Retry queue contains {retry_jobs} job(s).",
                )
            )

        if any(alert.severity == "critical" for alert in alerts):
            return "critical", tuple(alerts)
        if alerts:
            return "warning", tuple(alerts)
        return "ok", ()

    def _publish_metrics(self, snapshot: QueueStatusSnapshot) -> None:
        queue_name = snapshot.queue_name
        for state, value in {
            "total": snapshot.total_jobs,
            "ready": snapshot.ready_jobs,
            "deferred": snapshot.deferred_jobs,
            "in_progress": snapshot.in_progress_jobs,
            "retry": snapshot.retry_jobs,
            "result": snapshot.result_jobs,
            "dead_letter": snapshot.dead_letter_jobs,
        }.items():
            QUEUE_JOBS.labels(queue_name=queue_name, state=state).set(value)

        QUEUE_OLDEST_READY_AGE_SECONDS.labels(queue_name=queue_name).set(
            snapshot.oldest_ready_age_seconds or 0.0
        )
        QUEUE_NEXT_SCHEDULED_IN_SECONDS.labels(queue_name=queue_name).set(
            snapshot.next_scheduled_in_seconds or 0.0
        )
        QUEUE_ALERT_LEVEL.labels(queue_name=queue_name).set(
            _ALERT_SCORES.get(snapshot.alert_level, 0.0)
        )

    async def history(self, *, limit: int = 20) -> list[QueueStatusHistoryPoint]:
        """Return bounded persisted queue history in newest-first order."""

        lrange = getattr(self.redis, "lrange", None)
        if lrange is None:
            return []

        rows = await self._await_maybe(
            lrange(f"{_HISTORY_KEY_PREFIX}{self.queue_name}", 0, max(0, limit - 1))
        )
        history: list[QueueStatusHistoryPoint] = []
        for row in cast(list[object], rows):
            raw = row.decode("utf-8") if isinstance(row, bytes) else str(row)
            try:
                payload = cast(dict[str, Any], json.loads(raw))
            except Exception:
                continue
            history.append(
                QueueStatusHistoryPoint(
                    observed_at=str(payload.get("observed_at", "")),
                    total_jobs=self._coerce_int(payload.get("total_jobs", 0)),
                    ready_jobs=self._coerce_int(payload.get("ready_jobs", 0)),
                    deferred_jobs=self._coerce_int(payload.get("deferred_jobs", 0)),
                    in_progress_jobs=self._coerce_int(payload.get("in_progress_jobs", 0)),
                    retry_jobs=self._coerce_int(payload.get("retry_jobs", 0)),
                    dead_letter_jobs=self._coerce_int(payload.get("dead_letter_jobs", 0)),
                    oldest_ready_age_seconds=self._coerce_optional_float(
                        payload.get("oldest_ready_age_seconds")
                    ),
                    next_scheduled_in_seconds=self._coerce_optional_float(
                        payload.get("next_scheduled_in_seconds")
                    ),
                    alert_level=self._coerce_alert_level(payload.get("alert_level")),
                    dead_letter_oldest_age_seconds=self._coerce_optional_float(
                        payload.get("dead_letter_oldest_age_seconds")
                    ),
                    dead_letter_reason_counts=cast(
                        dict[str, int],
                        payload.get("dead_letter_reason_counts", {}) or {},
                    ),
                )
            )
        return history

    async def history_summary(self, *, limit: int = 20) -> QueueHistorySummary:
        """Return an aggregate rollup over bounded persisted queue history."""

        points = await self.history(limit=limit)
        dead_letter_reason_counts: dict[str, int] = {}
        for point in points:
            for reason_code, count in point.dead_letter_reason_counts.items():
                dead_letter_reason_counts[reason_code] = (
                    dead_letter_reason_counts.get(reason_code, 0) + int(count)
                )

        return QueueHistorySummary(
            point_count=len(points),
            warning_point_count=sum(1 for point in points if point.alert_level == "warning"),
            critical_point_count=sum(1 for point in points if point.alert_level == "critical"),
            max_total_jobs=max((point.total_jobs for point in points), default=0),
            max_ready_jobs=max((point.ready_jobs for point in points), default=0),
            max_retry_jobs=max((point.retry_jobs for point in points), default=0),
            max_dead_letter_jobs=max((point.dead_letter_jobs for point in points), default=0),
            latest_alert_level=(points[0].alert_level if points else "ok"),
            dead_letter_reason_counts=dict(sorted(dead_letter_reason_counts.items())),
        )

    async def dead_letter_samples(
        self,
        *,
        limit: int = 20,
        stage: str | None = None,
        reason_code: str | None = None,
    ) -> list[DeadLetterSample]:
        """Return normalized dead-letter payload samples with optional bounded filters."""

        bounded_limit = max(1, min(limit, 100))
        payloads = await self._dead_letter_payloads(start=0, stop=max(0, bounded_limit - 1))
        samples = [
            sample
            for payload in payloads
            if (sample := self._dead_letter_sample_from_payload(payload)) is not None
        ]
        if stage is not None:
            samples = [sample for sample in samples if sample.stage == stage]
        if reason_code is not None:
            samples = [sample for sample in samples if sample.reason_code == reason_code]
        return samples[:bounded_limit]

    async def snapshot(self, *, now_seconds: float | None = None) -> QueueStatusSnapshot:
        """Return one live queue snapshot and update exported Prometheus gauges."""

        current_time_seconds = time.time() if now_seconds is None else now_seconds
        now_milliseconds = int(current_time_seconds * 1000)

        total_jobs = self._coerce_int(
            await self._await_maybe(cast(Any, self.redis).zcard(self.queue_name))
        )
        ready_jobs = self._coerce_int(
            await self._await_maybe(
                cast(Any, self.redis).zcount(self.queue_name, "-inf", now_milliseconds)
            )
        )
        deferred_jobs = self._coerce_int(
            await self._await_maybe(
                cast(Any, self.redis).zcount(self.queue_name, f"({now_milliseconds}", "+inf")
            )
        )

        oldest_ready_score = await self._first_scheduled_score(
            minimum="-inf",
            maximum=now_milliseconds,
        )
        next_scheduled_score = await self._first_scheduled_score(
            minimum=f"({now_milliseconds}",
            maximum="+inf",
        )

        oldest_ready_age_seconds = (
            max(0.0, (now_milliseconds - oldest_ready_score) / 1000.0)
            if oldest_ready_score is not None
            else None
        )
        next_scheduled_in_seconds = (
            max(0.0, (next_scheduled_score - now_milliseconds) / 1000.0)
            if next_scheduled_score is not None
            else None
        )
        retry_jobs = await self._count_matching_keys(f"{retry_key_prefix}*")
        dead_letter_jobs = self._coerce_int(
            await self._await_maybe(
                cast(Any, self.redis).llen(f"{_DEAD_LETTER_KEY_PREFIX}{self.queue_name}")
            )
        )
        sampled_dead_letter_payloads = await self._dead_letter_payloads(start=0, stop=49)
        dead_letter_reason_counts = self._dead_letter_reason_counts_from_payloads(
            sampled_dead_letter_payloads
        )
        dead_letter_oldest_age_seconds: float | None = None
        if dead_letter_jobs > 0:
            oldest_dead_letter_payloads = await self._dead_letter_payloads(
                start=max(0, dead_letter_jobs - 1),
                stop=max(0, dead_letter_jobs - 1),
            )
            if oldest_dead_letter_payloads:
                dead_letter_oldest_age_seconds = self._payload_queued_age_seconds(
                    oldest_dead_letter_payloads[0],
                    now_seconds=current_time_seconds,
                )
        alert_level, alerts = self._classify_alerts(
            ready_jobs=ready_jobs,
            retry_jobs=retry_jobs,
            dead_letter_jobs=dead_letter_jobs,
            oldest_ready_age_seconds=oldest_ready_age_seconds,
        )
        snapshot = QueueStatusSnapshot(
            observed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(current_time_seconds)),
            queue_name=self.queue_name,
            total_jobs=total_jobs,
            ready_jobs=ready_jobs,
            deferred_jobs=deferred_jobs,
            in_progress_jobs=await self._count_matching_keys(f"{in_progress_key_prefix}*"),
            retry_jobs=retry_jobs,
            result_jobs=await self._count_matching_keys(f"{result_key_prefix}*"),
            dead_letter_jobs=dead_letter_jobs,
            oldest_ready_age_seconds=oldest_ready_age_seconds,
            next_scheduled_in_seconds=next_scheduled_in_seconds,
            alert_level=alert_level,
            alerts=alerts,
            dead_letter_oldest_age_seconds=dead_letter_oldest_age_seconds,
            dead_letter_reason_counts=dead_letter_reason_counts,
        )
        self._publish_metrics(snapshot)
        await self._persist_history(snapshot)
        return snapshot
