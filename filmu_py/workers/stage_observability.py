"""Worker-stage observability counters and helper recorders."""

from __future__ import annotations

from threading import Lock

from arq.jobs import JobStatus
from prometheus_client import Counter, Histogram

WORKER_ENQUEUE_DECISIONS_TOTAL = Counter(
    "filmu_py_worker_enqueue_decisions_total",
    "Downstream worker enqueue decisions by stage",
    ["stage", "decision"],
)
WORKER_JOB_STATUS_TOTAL = Counter(
    "filmu_py_worker_job_status_total",
    "Observed ARQ job statuses while coordinating worker stages",
    ["stage", "status"],
)
WORKER_CLEANUP_TOTAL = Counter(
    "filmu_py_worker_cleanup_total",
    "Cleanup actions taken before replaying or deduplicating worker stages",
    ["stage", "action"],
)
WORKER_STAGE_IDEMPOTENCY_TOTAL = Counter(
    "filmu_py_worker_stage_idempotency_total",
    "Observed stage idempotency and replay outcomes by stage",
    ["stage", "outcome"],
)
WORKER_ENQUEUE_DEFER_SECONDS = Histogram(
    "filmu_py_worker_enqueue_defer_seconds",
    "Deferred worker enqueue delays in seconds",
    ["stage"],
    buckets=[1.0, 5.0, 15.0, 30.0, 60.0, 300.0, 900.0, 3600.0],
)
WORKER_BLOCKER_EVENTS_TOTAL = Counter(
    "filmu_py_worker_blocker_events_total",
    "Observed worker-side operational blocker events by stage and bounded reason",
    ["stage", "blocker", "reason"],
)

_WORKER_BLOCKER_SNAPSHOT_LOCK = Lock()
_WORKER_BLOCKER_SNAPSHOT: dict[str, object] = {
    "rank_streams_no_winner_total": 0,
    "rank_streams_no_winner_reason_counts": {},
    "rank_streams_no_winner_last_reason": "",
    "debrid_rate_limited_total": 0,
    "debrid_rate_limited_provider_counts": {},
    "debrid_rate_limited_last_provider": "",
    "debrid_rate_limited_last_retry_after_seconds": 0.0,
}


def job_status_name(status: JobStatus) -> str:
    """Normalize ARQ job status enum values for bounded metrics/log labels."""

    return status.name if isinstance(status, JobStatus) else str(status)


def record_enqueue_decision(stage_name: str, decision: str) -> None:
    """Record one enqueue decision outcome for a downstream stage."""

    WORKER_ENQUEUE_DECISIONS_TOTAL.labels(stage=stage_name, decision=decision).inc()


def record_job_status(stage_name: str, status: JobStatus) -> None:
    """Record one observed ARQ job status during stage coordination."""

    WORKER_JOB_STATUS_TOTAL.labels(stage=stage_name, status=job_status_name(status)).inc()


def record_cleanup_action(stage_name: str, action: str) -> None:
    """Record one stale-job/result cleanup action for a stage."""

    WORKER_CLEANUP_TOTAL.labels(stage=stage_name, action=action).inc()


def record_stage_idempotency(stage_name: str, outcome: str) -> None:
    """Record one stage idempotency/replay outcome."""

    WORKER_STAGE_IDEMPOTENCY_TOTAL.labels(stage=stage_name, outcome=outcome).inc()


def record_enqueue_defer(stage_name: str, defer_seconds: float) -> None:
    """Record one deferred enqueue delay for a stage."""

    WORKER_ENQUEUE_DEFER_SECONDS.labels(stage=stage_name).observe(float(defer_seconds))


def _increment_snapshot_counter(counter_name: str, key: str) -> None:
    """Increment one bounded reason/provider counter inside the blocker snapshot."""

    with _WORKER_BLOCKER_SNAPSHOT_LOCK:
        raw_counts = _WORKER_BLOCKER_SNAPSHOT.setdefault(counter_name, {})
        if not isinstance(raw_counts, dict):
            raw_counts = {}
            _WORKER_BLOCKER_SNAPSHOT[counter_name] = raw_counts
        current_value = raw_counts.get(key, 0)
        raw_counts[key] = int(current_value) + 1


def record_rank_no_winner(*, failure_reason: str) -> None:
    """Record one `rank_streams.no_winner` blocker event."""

    normalized_reason = failure_reason.strip() or "unknown"
    WORKER_BLOCKER_EVENTS_TOTAL.labels(
        stage="rank_streams",
        blocker="no_winner",
        reason=normalized_reason,
    ).inc()
    with _WORKER_BLOCKER_SNAPSHOT_LOCK:
        _WORKER_BLOCKER_SNAPSHOT["rank_streams_no_winner_total"] = (
            int(_WORKER_BLOCKER_SNAPSHOT["rank_streams_no_winner_total"]) + 1
        )
        _WORKER_BLOCKER_SNAPSHOT["rank_streams_no_winner_last_reason"] = normalized_reason
    _increment_snapshot_counter("rank_streams_no_winner_reason_counts", normalized_reason)


def record_debrid_rate_limited(*, provider: str, retry_after_seconds: float | None) -> None:
    """Record one `debrid_item.rate_limited` blocker event."""

    normalized_provider = provider.strip() or "unknown"
    reason = "retry_after_present" if retry_after_seconds is not None else "retry_after_missing"
    WORKER_BLOCKER_EVENTS_TOTAL.labels(
        stage="debrid_item",
        blocker="rate_limited",
        reason=reason,
    ).inc()
    with _WORKER_BLOCKER_SNAPSHOT_LOCK:
        _WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_total"] = (
            int(_WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_total"]) + 1
        )
        _WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_last_provider"] = normalized_provider
        _WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_last_retry_after_seconds"] = float(
            retry_after_seconds or 0.0
        )
    _increment_snapshot_counter("debrid_rate_limited_provider_counts", normalized_provider)


def worker_blocker_snapshot() -> dict[str, object]:
    """Return a bounded copy of the current worker blocker posture."""

    with _WORKER_BLOCKER_SNAPSHOT_LOCK:
        return {
            "rank_streams_no_winner_total": int(
                _WORKER_BLOCKER_SNAPSHOT["rank_streams_no_winner_total"]
            ),
            "rank_streams_no_winner_reason_counts": dict(
                _WORKER_BLOCKER_SNAPSHOT["rank_streams_no_winner_reason_counts"]
            ),
            "rank_streams_no_winner_last_reason": str(
                _WORKER_BLOCKER_SNAPSHOT["rank_streams_no_winner_last_reason"]
            ),
            "debrid_rate_limited_total": int(
                _WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_total"]
            ),
            "debrid_rate_limited_provider_counts": dict(
                _WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_provider_counts"]
            ),
            "debrid_rate_limited_last_provider": str(
                _WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_last_provider"]
            ),
            "debrid_rate_limited_last_retry_after_seconds": float(
                _WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_last_retry_after_seconds"]
            ),
        }


def reset_worker_blocker_snapshot() -> None:
    """Reset the in-memory blocker snapshot for deterministic tests."""

    with _WORKER_BLOCKER_SNAPSHOT_LOCK:
        _WORKER_BLOCKER_SNAPSHOT["rank_streams_no_winner_total"] = 0
        _WORKER_BLOCKER_SNAPSHOT["rank_streams_no_winner_reason_counts"] = {}
        _WORKER_BLOCKER_SNAPSHOT["rank_streams_no_winner_last_reason"] = ""
        _WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_total"] = 0
        _WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_provider_counts"] = {}
        _WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_last_provider"] = ""
        _WORKER_BLOCKER_SNAPSHOT["debrid_rate_limited_last_retry_after_seconds"] = 0.0
