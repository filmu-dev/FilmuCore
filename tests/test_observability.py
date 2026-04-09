"""Observability coverage for route, worker, cache, and plugin metrics."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.api.router import (
    ROUTE_ERRORS_TOTAL,
    ROUTE_LATENCY_SECONDS,
    ROUTE_REQUESTS_TOTAL,
    create_api_router,
)
from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql.plugin_registry import GraphQLPluginRegistry
from filmu_py.plugins.hooks import (
    PLUGIN_HOOK_DURATION_SECONDS,
    PLUGIN_HOOK_INVOCATIONS_TOTAL,
    PluginHookWorkerExecutor,
)
from filmu_py.plugins.loader import PLUGIN_LOAD_TOTAL
from filmu_py.plugins.registry import PluginRegistry
from filmu_py.resources import AppResources
from filmu_py.services.media import StatsProjection, StatsYearReleaseRecord
from filmu_py.workers import retry as retry_helpers
from filmu_py.workers import tasks as worker_tasks


class DummyRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.deleted: list[str] = []

    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool

    async def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
        _ = ex
        self.values[key] = value

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        return 1 if self.values.pop(key, None) is not None else 0


class DummyDatabaseRuntime:
    async def dispose(self) -> None:
        return None


@dataclass
class DummyMediaService:
    snapshot: StatsProjection

    async def get_stats(self) -> StatsProjection:
        return self.snapshot

    async def get_item_detail(
        self,
        item_identifier: str,
        *,
        media_type: str,
        extended: bool,
    ) -> None:
        _ = (item_identifier, media_type, extended)
        return None

    async def search_items(self, **kwargs: Any) -> Any:
        _ = kwargs
        return type(
            "_Result",
            (),
            {
                "success": True,
                "items": [],
                "page": 1,
                "limit": 24,
                "total_items": 0,
                "total_pages": 0,
            },
        )()


def _counter_value(counter: Any, **labels: str) -> float:
    metric = counter.labels(**labels) if labels else counter
    return float(metric._value.get())


def _histogram_count(histogram: Any, **labels: str) -> float:
    sample_name = f"{histogram._name}_count"
    for metric in histogram.collect():
        for sample in metric.samples:
            if sample.name == sample_name and sample.labels == labels:
                return float(sample.value)
    return 0.0


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_PROMETHEUS_ENABLED=True,
    )


def _build_snapshot() -> StatsProjection:
    return StatsProjection(
        total_items=0,
        completed_items=0,
        failed_items=0,
        incomplete_items=0,
        movies=0,
        shows=0,
        episodes=0,
        seasons=0,
        states={},
        activity={},
        media_year_releases=[StatsYearReleaseRecord(year=2024, count=1)],
    )


def _build_client(*, plugin_registry: PluginRegistry | None = None) -> TestClient:
    settings = _build_settings()
    redis = DummyRedis()
    app = FastAPI()
    app.state.resources = AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=DummyMediaService(snapshot=_build_snapshot()),  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
        plugin_registry=plugin_registry,
    )
    app.include_router(create_api_router())
    from filmu_py.api.router import RouteMetricsMiddleware
    from filmu_py.middleware import RequestIdMiddleware
    from filmu_py.observability import setup_observability

    app.add_middleware(RequestIdMiddleware)
    app.add_middleware(RouteMetricsMiddleware)
    setup_observability(app, settings)
    return TestClient(app)


def _headers() -> dict[str, str]:
    return {"x-api-key": "a" * 32}


def test_route_metrics_use_template_labels() -> None:
    item_request_before = _counter_value(
        ROUTE_REQUESTS_TOTAL,
        route="/api/v1/items/{id}",
        method="GET",
        status_code="404",
    )
    item_latency_before = _histogram_count(
        ROUTE_LATENCY_SECONDS,
        route="/api/v1/items/{id}",
        method="GET",
    )
    item_client_error_before = _counter_value(
        ROUTE_ERRORS_TOTAL,
        route="/api/v1/items/{id}",
        method="GET",
        error_class="client_error",
    )
    stats_request_before = _counter_value(
        ROUTE_REQUESTS_TOTAL,
        route="/api/v1/stats",
        method="GET",
        status_code="200",
    )

    client = _build_client()
    first = client.get("/api/v1/items/123", params={"media_type": "movie"}, headers=_headers())
    second = client.get("/api/v1/items/456", params={"media_type": "movie"}, headers=_headers())
    third = client.get("/api/v1/stats", headers=_headers())
    metrics_response = client.get("/metrics")

    assert first.status_code == 404
    assert second.status_code == 404
    assert third.status_code == 200
    assert metrics_response.status_code == 200
    assert "filmu_py_route_requests_total" in metrics_response.text

    assert _counter_value(
        ROUTE_REQUESTS_TOTAL,
        route="/api/v1/items/{id}",
        method="GET",
        status_code="404",
    ) == item_request_before + 2.0
    assert _histogram_count(
        ROUTE_LATENCY_SECONDS,
        route="/api/v1/items/{id}",
        method="GET",
    ) == item_latency_before + 2.0
    assert _counter_value(
        ROUTE_ERRORS_TOTAL,
        route="/api/v1/items/{id}",
        method="GET",
        error_class="client_error",
    ) == item_client_error_before + 2.0
    assert _counter_value(
        ROUTE_REQUESTS_TOTAL,
        route="/api/v1/stats",
        method="GET",
        status_code="200",
    ) == stats_request_before + 1.0


def test_cache_metrics_track_hits_misses_and_invalidations() -> None:
    from filmu_py.core.cache import (
        CACHE_HITS_TOTAL,
        CACHE_INVALIDATIONS_TOTAL,
        CACHE_MISSES_TOTAL,
    )

    redis = DummyRedis()
    cache = CacheManager(redis=redis, namespace="metrics")  # type: ignore[arg-type]

    local_miss_before = _counter_value(CACHE_MISSES_TOTAL, layer="local", namespace="metrics")
    redis_miss_before = _counter_value(CACHE_MISSES_TOTAL, layer="redis", namespace="metrics")
    redis_hit_before = _counter_value(CACHE_HITS_TOTAL, layer="redis", namespace="metrics")
    local_hit_before = _counter_value(CACHE_HITS_TOTAL, layer="local", namespace="metrics")
    invalidation_before = _counter_value(
        CACHE_INVALIDATIONS_TOTAL,
        namespace="metrics",
        reason="explicit",
    )

    assert asyncio.run(cache.get("movie")) is None
    asyncio.run(cache.set("movie", b"bytes"))
    cache.local.clear()
    assert asyncio.run(cache.get("movie")) == b"bytes"
    assert asyncio.run(cache.get("movie")) == b"bytes"
    asyncio.run(cache.delete("movie", reason="explicit"))

    assert _counter_value(CACHE_MISSES_TOTAL, layer="local", namespace="metrics") == (
        local_miss_before + 2.0
    )
    assert _counter_value(CACHE_MISSES_TOTAL, layer="redis", namespace="metrics") == (
        redis_miss_before + 1.0
    )
    assert _counter_value(CACHE_HITS_TOTAL, layer="redis", namespace="metrics") == (
        redis_hit_before + 1.0
    )
    assert _counter_value(CACHE_HITS_TOTAL, layer="local", namespace="metrics") == (
        local_hit_before + 1.0
    )
    assert _counter_value(
        CACHE_INVALIDATIONS_TOTAL,
        namespace="metrics",
        reason="explicit",
    ) == invalidation_before + 1.0


def test_plugin_load_metrics_track_success_failure_and_version_skip() -> None:
    success_before = _counter_value(PLUGIN_LOAD_TOTAL, plugin_name="good-plugin", result="success")
    failed_before = _counter_value(PLUGIN_LOAD_TOTAL, plugin_name="broken-plugin", result="failed")
    version_skip_before = _counter_value(
        PLUGIN_LOAD_TOTAL,
        plugin_name="future-plugin",
        result="skipped_version",
    )
    api_skip_before = _counter_value(
        PLUGIN_LOAD_TOTAL,
        plugin_name="future-api-plugin",
        result="skipped_api_version",
    )

    import json
    from pathlib import Path
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as temp_dir:
        plugins_dir = Path(temp_dir)
        good_dir = plugins_dir / "good-plugin"
        good_dir.mkdir()
        (good_dir / "plugin.json").write_text(
            json.dumps(
                {
                    "name": "good-plugin",
                    "version": "1.0.0",
                    "api_version": "1",
                    "entry_module": "plugin.py",
                    "graphql": {"query_resolvers": []},
                }
            ),
            encoding="utf-8",
        )
        (good_dir / "plugin.py").write_text("", encoding="utf-8")

        broken_dir = plugins_dir / "broken-plugin"
        broken_dir.mkdir()
        (broken_dir / "plugin.json").write_text(
            json.dumps(
                {
                    "name": "broken-plugin",
                    "version": "1.0.0",
                    "api_version": "1",
                    "entry_module": "plugin.py",
                }
            ),
            encoding="utf-8",
        )

        future_dir = plugins_dir / "future-plugin"
        future_dir.mkdir()
        (future_dir / "plugin.json").write_text(
            json.dumps(
                {
                    "name": "future-plugin",
                    "version": "1.0.0",
                    "api_version": "1",
                    "min_host_version": "99.0.0",
                    "entry_module": "plugin.py",
                }
            ),
            encoding="utf-8",
        )
        (future_dir / "plugin.py").write_text("", encoding="utf-8")

        future_api_dir = plugins_dir / "future-api-plugin"
        future_api_dir.mkdir()
        (future_api_dir / "plugin.json").write_text(
            json.dumps(
                {
                    "name": "future-api-plugin",
                    "version": "1.0.0",
                    "api_version": "2",
                    "entry_module": "plugin.py",
                }
            ),
            encoding="utf-8",
        )
        (future_api_dir / "plugin.py").write_text("", encoding="utf-8")

        from filmu_py.plugins.loader import load_plugins

        load_plugins(plugins_dir, PluginRegistry(), host_version="0.1.0")

    assert _counter_value(PLUGIN_LOAD_TOTAL, plugin_name="good-plugin", result="success") == (
        success_before + 1.0
    )
    assert _counter_value(PLUGIN_LOAD_TOTAL, plugin_name="broken-plugin", result="failed") == (
        failed_before + 1.0
    )
    assert _counter_value(
        PLUGIN_LOAD_TOTAL,
        plugin_name="future-plugin",
        result="skipped_version",
    ) == version_skip_before + 1.0
    assert _counter_value(
        PLUGIN_LOAD_TOTAL,
        plugin_name="future-api-plugin",
        result="skipped_api_version",
    ) == api_skip_before + 1.0


def test_plugin_hook_metrics_track_success_and_timeout_outcomes() -> None:
    success_before = _counter_value(
        PLUGIN_HOOK_INVOCATIONS_TOTAL,
        plugin_name="hook-plugin",
        event_type="item.completed",
        outcome="success",
    )
    timeout_before = _counter_value(
        PLUGIN_HOOK_INVOCATIONS_TOTAL,
        plugin_name="hook-plugin",
        event_type="item.completed",
        outcome="timeout",
    )
    duration_before = _histogram_count(
        PLUGIN_HOOK_DURATION_SECONDS,
        plugin_name="hook-plugin",
        event_type="item.completed",
    )

    @dataclass
    class Hook:
        plugin_name: str = "hook-plugin"
        subscribed_events: frozenset[str] = frozenset({"item.completed"})
        delay_seconds: float = 0.0

        async def initialize(self, ctx: object) -> None:
            _ = ctx

        async def handle(self, event_type: str, payload: dict[str, Any]) -> None:
            _ = (event_type, payload)
            if self.delay_seconds:
                await asyncio.sleep(self.delay_seconds)

    executor = PluginHookWorkerExecutor(timeout_seconds=0.01)
    asyncio.run(executor._safe_invoke(Hook(delay_seconds=0.0), "item.completed", {"item_id": "1"}))
    asyncio.run(executor._safe_invoke(Hook(delay_seconds=0.1), "item.completed", {"item_id": "2"}))

    assert _counter_value(
        PLUGIN_HOOK_INVOCATIONS_TOTAL,
        plugin_name="hook-plugin",
        event_type="item.completed",
        outcome="success",
    ) == success_before + 1.0
    assert _counter_value(
        PLUGIN_HOOK_INVOCATIONS_TOTAL,
        plugin_name="hook-plugin",
        event_type="item.completed",
        outcome="timeout",
    ) == timeout_before + 1.0
    assert _histogram_count(
        PLUGIN_HOOK_DURATION_SECONDS,
        plugin_name="hook-plugin",
        event_type="item.completed",
    ) == duration_before + 2.0


def test_worker_retry_metrics_track_retry_dead_letter_and_stage_duration() -> None:
    retry_before = _counter_value(retry_helpers.WORKER_RETRY_TOTAL, stage="scrape_item")
    dlq_before = _counter_value(
        retry_helpers.WORKER_DLQ_TOTAL,
        stage="scrape_item",
        reason="boom",
    )
    duration_before = _histogram_count(
        retry_helpers.WORKER_STAGE_DURATION,
        stage="scrape_item",
        outcome="success",
    )

    retry_helpers.record_worker_retry("scrape_item")

    class FakeRedis:
        async def lpush(self, name: str, *values: str) -> int:
            _ = (name, values)
            return 1

    asyncio.run(
        retry_helpers.route_dead_letter(
            ctx={"redis": FakeRedis(), "queue_name": "filmu-py", "job_try": 3},
            task_name="scrape_item",
            item_id="item-1",
            reason="boom",
        )
    )

    @retry_helpers.timed_stage("scrape_item")
    async def run_stage() -> str:
        return "ok"

    asyncio.run(run_stage())

    assert _counter_value(retry_helpers.WORKER_RETRY_TOTAL, stage="scrape_item") == (
        retry_before + 1.0
    )
    assert _counter_value(
        retry_helpers.WORKER_DLQ_TOTAL,
        stage="scrape_item",
        reason="boom",
    ) == dlq_before + 1.0
    assert _histogram_count(
        retry_helpers.WORKER_STAGE_DURATION,
        stage="scrape_item",
        outcome="success",
    ) == duration_before + 1.0


def test_worker_queue_metrics_track_status_cleanup_and_enqueue_decisions(monkeypatch: Any) -> None:
    status_before = _counter_value(
        worker_tasks.WORKER_JOB_STATUS_TOTAL,
        stage="scrape_item",
        status="queued",
    )
    cleanup_before = _counter_value(
        worker_tasks.WORKER_CLEANUP_TOTAL,
        stage="scrape_item",
        action="stale_result_deleted",
    )
    decision_before = _counter_value(
        worker_tasks.WORKER_ENQUEUE_DECISIONS_TOTAL,
        stage="scrape_item",
        decision="enqueued",
    )
    defer_before = _histogram_count(
        worker_tasks.WORKER_ENQUEUE_DEFER_SECONDS,
        stage="scrape_item",
    )

    class FakeJob:
        def __init__(self, _job_id: str, redis: object) -> None:
            _ = redis

        async def status(self) -> Any:
            from arq.jobs import JobStatus

            return JobStatus.queued

    monkeypatch.setattr(worker_tasks, "Job", FakeJob)
    redis = DummyRedis()
    class FakeArqRedis:
        async def delete(self, key: str) -> int:
            return await redis.delete(key)

        async def enqueue_job(self, *args: Any, **kwargs: Any) -> object:
            _ = (args, kwargs)
            return object()

    fake_arq = FakeArqRedis()
    redis.values["arq:result:scrape-item:item-1"] = b"stale"

    asyncio.run(worker_tasks.is_scrape_item_job_active(fake_arq, item_id="item-1"))
    asyncio.run(
        worker_tasks._clear_stale_downstream_job(
            fake_arq,
            item_id="item-1",
            stage_name="scrape_item",
            job_id="scrape-item:item-1",
        )
    )
    asyncio.run(
        worker_tasks.enqueue_scrape_item(
            fake_arq,
            item_id="item-1",
            queue_name="filmu-py",
            defer_by_seconds=30,
        )
    )
    worker_tasks._log_downstream_enqueue_result(
        item_id="item-1",
        stage_name="scrape_item",
        job_id="scrape-item:item-1",
        enqueued=True,
    )

    assert _counter_value(
        worker_tasks.WORKER_JOB_STATUS_TOTAL,
        stage="scrape_item",
        status="queued",
    ) == status_before + 1.0
    assert _counter_value(
        worker_tasks.WORKER_CLEANUP_TOTAL,
        stage="scrape_item",
        action="stale_result_deleted",
    ) == cleanup_before + 1.0
    assert _counter_value(
        worker_tasks.WORKER_ENQUEUE_DECISIONS_TOTAL,
        stage="scrape_item",
        decision="enqueued",
    ) == decision_before + 1.0
    assert _histogram_count(
        worker_tasks.WORKER_ENQUEUE_DEFER_SECONDS,
        stage="scrape_item",
    ) == defer_before + 1.0
