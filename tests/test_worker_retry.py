"""Worker retry/dead-letter behavior tests for Phase D1 hardening."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from arq import Retry

from filmu_py.state.item import ItemState
from filmu_py.workers import tasks
from filmu_py.workers.retry import RetryPolicy, route_dead_letter, task_try_count


class FakeRedis:
    """In-memory Redis list stub for dead-letter assertions."""

    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    async def lpush(self, name: str, *values: str) -> int:
        for value in values:
            self.entries.insert(0, (name, value))
        return len(self.entries)


class FailingMediaService:
    """Media service stub that always fails transition calls."""

    async def get_item(self, item_id: str) -> Any:
        from types import SimpleNamespace
        return SimpleNamespace(id=item_id, external_ref="tmdb:123", attributes={"tmdb_id": "123", "item_type": "movie"}, state=ItemState.INDEXED)

    async def transition_item(self, **kwargs: Any) -> None:
        _ = kwargs
        raise RuntimeError("transition failed")


def test_retry_policy_backoff_is_bounded() -> None:
    """Backoff should grow exponentially and clamp to max delay."""

    policy = RetryPolicy(max_attempts=4, base_delay_seconds=2, max_delay_seconds=10)
    assert policy.next_delay_seconds(1) == 2
    assert policy.next_delay_seconds(2) == 4
    assert policy.next_delay_seconds(3) == 8
    assert policy.next_delay_seconds(4) == 10
    assert policy.next_delay_seconds(9) == 10


def test_task_try_count_parses_and_clamps() -> None:
    """Attempt extraction should handle invalid/negative values safely."""

    assert task_try_count({}) == 1
    assert task_try_count({"job_try": "3"}) == 3
    assert task_try_count({"job_try": 0}) == 1
    assert task_try_count({"job_try": -4}) == 1
    assert task_try_count({"job_try": "invalid"}) == 1


def test_route_dead_letter_writes_json_payload() -> None:
    """Dead-letter routing should push a structured JSON payload to Redis."""

    redis = FakeRedis()
    ctx: dict[str, Any] = {"redis": redis, "queue_name": "compat", "job_try": 4}

    asyncio.run(
        route_dead_letter(
            ctx=ctx,
            task_name="scrape_item",
            item_id="item-123",
            reason="boom",
        )
    )

    assert len(redis.entries) == 1
    key, payload = redis.entries[0]
    assert key == "arq:dead-letter:compat"

    parsed = json.loads(payload)
    assert parsed["task"] == "scrape_item"
    assert parsed["item_id"] == "item-123"
    assert parsed["reason"] == "boom"
    assert parsed["attempt"] == 4
    assert "queued_at" in parsed


def test_scrape_item_raises_retry_before_dead_letter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-terminal failures should return ARQ Retry with policy-based defer."""

    async def no_rate_limit(
        *, limiter: object, bucket: str, capacity: float, refill_per_second: float
    ) -> None:
        _ = (limiter, bucket, capacity, refill_per_second)

    async def fake_settings(*args: Any, **kwargs: Any) -> Any:
        return object()

    monkeypatch.setattr(tasks, "_resolve_runtime_settings", fake_settings)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: object())
    monkeypatch.setattr(tasks, "_acquire_worker_rate_limit", no_rate_limit)
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: FailingMediaService())

    redis = FakeRedis()
    ctx: dict[str, object] = {"job_try": 1, "redis": redis, "queue_name": "q"}

    with pytest.raises(Retry) as exc:
        asyncio.run(tasks.scrape_item(ctx, "item-1"))

    assert exc.value.defer_score == 2000
    assert redis.entries == []


def test_scrape_item_routes_dead_letter_on_terminal_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Terminal attempt failures should route to dead-letter and re-raise root error."""

    async def no_rate_limit(
        *, limiter: object, bucket: str, capacity: float, refill_per_second: float
    ) -> None:
        _ = (limiter, bucket, capacity, refill_per_second)

    async def fake_settings(*args: Any, **kwargs: Any) -> Any:
        return object()

    monkeypatch.setattr(tasks, "_resolve_runtime_settings", fake_settings)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: object())
    monkeypatch.setattr(tasks, "_acquire_worker_rate_limit", no_rate_limit)
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: FailingMediaService())

    redis = FakeRedis()
    ctx: dict[str, object] = {"job_try": 4, "redis": redis, "queue_name": "q"}

    with pytest.raises(RuntimeError, match="transition failed"):
        asyncio.run(tasks.scrape_item(ctx, "item-2"))

    assert len(redis.entries) == 1
    key, payload = redis.entries[0]
    assert key == "arq:dead-letter:q"
    parsed = json.loads(payload)
    assert parsed["attempt"] == 4
