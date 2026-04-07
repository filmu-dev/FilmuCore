"""Retry and dead-letter policy helpers for ARQ worker tasks."""

from __future__ import annotations

import json
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import wraps
from time import perf_counter
from typing import Any, ParamSpec, Protocol, TypeVar, cast, runtime_checkable

import structlog
from arq import Retry
from prometheus_client import Counter, Histogram

WORKER_STAGE_DURATION = Histogram(
    "filmu_py_worker_stage_duration_seconds",
    "Worker stage execution duration",
    ["stage", "outcome"],
    buckets=[0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0],
)
WORKER_RETRY_TOTAL = Counter(
    "filmu_py_worker_retry_total",
    "Worker stage retry count",
    ["stage"],
)
WORKER_DLQ_TOTAL = Counter(
    "filmu_py_worker_dlq_total",
    "Worker stage dead-letter routing count",
    ["stage", "reason"],
)

P = ParamSpec("P")
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded exponential backoff policy for worker task retries."""

    max_attempts: int
    base_delay_seconds: int
    max_delay_seconds: int

    def next_delay_seconds(self, attempt: int) -> int:
        """Compute backoff delay for attempt number (1-based)."""

        safe_attempt = max(1, attempt)
        exponent = safe_attempt - 1
        delay = int(self.base_delay_seconds * (2**exponent))
        if delay > self.max_delay_seconds:
            return int(self.max_delay_seconds)
        return int(delay)

    def should_dead_letter(self, attempt: int) -> bool:
        """Return whether a failed attempt should route task to dead letter."""

        return attempt >= self.max_attempts


@runtime_checkable
class DeadLetterRedisClient(Protocol):
    """Redis protocol subset required for dead-letter routing."""

    async def lpush(self, name: str, *values: str) -> int:
        """Prepend one or more values to a Redis list key."""

        ...


def task_try_count(ctx: dict[str, Any]) -> int:
    """Extract current ARQ task attempt count from worker context.

    Uses ``job_try`` (ARQ convention) and defaults to first attempt.
    """

    value = ctx.get("job_try", 1)
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 1
    return max(1, parsed)


def bind_worker_contextvars(
    *,
    ctx: dict[str, Any],
    stage: str,
    item_id: str | None,
    item_request_id: str | None = None,
) -> None:
    """Bind worker correlation keys into structlog contextvars for one stage."""

    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        item_id=str(item_id) if item_id is not None else None,
        item_request_id=str(item_request_id) if item_request_id is not None else None,
        worker_stage=stage,
        job_id=str(ctx.get("job_id")) if ctx.get("job_id") is not None else None,
    )


def record_worker_retry(stage: str) -> None:
    """Increment retry counter for one worker stage."""

    WORKER_RETRY_TOTAL.labels(stage=stage).inc()


def timed_stage(
    stage: str,
) -> Callable[[Callable[P, Coroutine[Any, Any, T]]], Callable[P, Coroutine[Any, Any, T]]]:
    """Record worker stage duration with success/retry/DLQ outcomes."""

    def decorator(
        func: Callable[P, Coroutine[Any, Any, T]],
    ) -> Callable[P, Coroutine[Any, Any, T]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            started_at = perf_counter()
            outcome = "success"
            ctx_candidate = args[0] if args else kwargs.get("ctx")
            ctx = cast(dict[str, Any] | None, ctx_candidate if isinstance(ctx_candidate, dict) else None)

            try:
                return await func(*args, **kwargs)
            except Retry:
                outcome = "retry"
                record_worker_retry(stage)
                raise
            except Exception:
                if ctx is not None:
                    dlq = ctx.pop("_worker_last_dead_letter", None)
                    if isinstance(dlq, dict) and dlq.get("stage") == stage:
                        outcome = "dlq"
                raise
            finally:
                WORKER_STAGE_DURATION.labels(stage=stage, outcome=outcome).observe(
                    perf_counter() - started_at
                )

        return wrapper

    return decorator


async def route_dead_letter(
    *,
    ctx: dict[str, Any],
    task_name: str,
    item_id: str,
    reason: str,
) -> None:
    """Publish dead-letter metadata to Redis list for operator inspection."""

    redis = ctx.get("redis")
    if not isinstance(redis, DeadLetterRedisClient):
        return

    queue_name = str(ctx.get("queue_name", "filmu-py"))
    key = f"arq:dead-letter:{queue_name}"

    payload = json.dumps(
        {
            "task": task_name,
            "item_id": item_id,
            "reason": reason,
            "attempt": task_try_count(ctx),
            "queued_at": datetime.now(UTC).isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )

    ctx["_worker_last_dead_letter"] = {"stage": task_name, "reason": reason}
    WORKER_DLQ_TOTAL.labels(stage=task_name, reason=reason).inc()
    await redis.lpush(key, payload)
