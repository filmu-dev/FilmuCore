"""Worker-stage observability counters and helper recorders."""

from __future__ import annotations

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
