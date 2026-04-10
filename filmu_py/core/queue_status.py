"""ARQ queue visibility helpers for operator routes and metrics."""

from __future__ import annotations

import time
from collections.abc import AsyncIterable, Awaitable, Iterable
from dataclasses import dataclass
from typing import Any, cast

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

_DEAD_LETTER_KEY_PREFIX = "arq:dead-letter:"


@dataclass(frozen=True, slots=True)
class QueueStatusSnapshot:
    """Current operator-facing ARQ queue status snapshot."""

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


class QueueStatusReader:
    """Read one bounded ARQ queue snapshot from Redis primitives."""

    def __init__(self, redis: object, *, queue_name: str) -> None:
        self.redis = redis
        self.queue_name = queue_name

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

    async def snapshot(self, *, now_seconds: float | None = None) -> QueueStatusSnapshot:
        """Return one live queue snapshot and update exported Prometheus gauges."""

        current_time_seconds = time.time() if now_seconds is None else now_seconds
        now_milliseconds = int(current_time_seconds * 1000)

        total_jobs = int(await self._await_maybe(cast(Any, self.redis).zcard(self.queue_name)))
        ready_jobs = int(
            await self._await_maybe(
                cast(Any, self.redis).zcount(self.queue_name, "-inf", now_milliseconds)
            )
        )
        deferred_jobs = int(
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

        snapshot = QueueStatusSnapshot(
            queue_name=self.queue_name,
            total_jobs=total_jobs,
            ready_jobs=ready_jobs,
            deferred_jobs=deferred_jobs,
            in_progress_jobs=await self._count_matching_keys(f"{in_progress_key_prefix}*"),
            retry_jobs=await self._count_matching_keys(f"{retry_key_prefix}*"),
            result_jobs=await self._count_matching_keys(f"{result_key_prefix}*"),
            dead_letter_jobs=int(
                await self._await_maybe(
                    cast(Any, self.redis).llen(f"{_DEAD_LETTER_KEY_PREFIX}{self.queue_name}")
                )
            ),
            oldest_ready_age_seconds=(
                max(0.0, (now_milliseconds - oldest_ready_score) / 1000.0)
                if oldest_ready_score is not None
                else None
            ),
            next_scheduled_in_seconds=(
                max(0.0, (next_scheduled_score - now_milliseconds) / 1000.0)
                if next_scheduled_score is not None
                else None
            ),
        )
        self._publish_metrics(snapshot)
        return snapshot
