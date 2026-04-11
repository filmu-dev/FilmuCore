from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from filmu_py.db.models import MediaItemORM
from filmu_py.services.media import MediaItemRecord
from filmu_py.state.item import ItemEvent, ItemState
from filmu_py.workers import tasks


class _AllowedLimiter:
    async def acquire(self, *a: Any, **kw: Any) -> Any:
        class _Result:
            allowed = True
            retry_after_seconds = 0.0

        return _Result()


class FakeMediaService:
    def __init__(self, item: MediaItemRecord | None = None) -> None:
        self.item = item
        self.transition_calls: list[ItemEvent] = []

    async def get_item(self, item_id: str) -> MediaItemRecord | None:
        return self.item

    async def get_latest_item_request_id(self, *, media_item_id: str) -> str | None:
        return "req-1"

    async def retry_item(self, item_id: str, session: Any, redis: Any) -> MediaItemRecord:
        if self.item:
            self.item.state = ItemState.REQUESTED
            return self.item
        raise ValueError("Item not found")


async def fake_try_transition(
    media_service: FakeMediaService,
    item_id: str,
    event: ItemEvent,
    message: str | None = None,
) -> bool:
    media_service.transition_calls.append(event)
    return True


async def fake_scrape_with_plugins(*a: Any, **kw: Any) -> tuple[list[Any], list[Any]]:
    return [], []


def _build_worker_settings() -> Any:
    from filmu_py.config import Settings
    return Settings(
        FILMU_PY_API_KEY="a" * 32,
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL="redis://localhost:6379/0",
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
    )


def test_scrape_item_defers_unreleased_movie(monkeypatch: Any) -> None:
    future_date = (datetime.now(UTC) + timedelta(days=10)).isoformat()
    svc = FakeMediaService(
        item=MediaItemRecord(
            id="item-1",
            external_ref="tmdb:1",
            title="Unreleased Movie",
            state=ItemState.REQUESTED,
            attributes={"aired_at": future_date},
        )
    )

    monkeypatch.setattr(tasks, "_try_transition", fake_try_transition)
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: svc)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())
    monkeypatch.setattr(tasks, "get_settings", _build_worker_settings)
    
    async def _async_mock(*a: Any, **kw: Any) -> Any:
        return _build_worker_settings()
        
    monkeypatch.setattr(tasks, "_resolve_runtime_settings", _async_mock)

    result = asyncio.run(tasks.scrape_item({"settings": _build_worker_settings()}, "item-1"))

    assert result == "item-1"
    assert len(svc.transition_calls) == 1
    assert svc.transition_calls[0] == ItemEvent.MARK_UNRELEASED


def test_scrape_item_proceeds_normal_scraping_if_past_released(monkeypatch: Any) -> None:
    past_date = (datetime.now(UTC) - timedelta(days=10)).isoformat()
    svc = FakeMediaService(
        item=MediaItemRecord(
            id="item-2",
            external_ref="tmdb:2",
            title="Released Movie",
            state=ItemState.REQUESTED,
            attributes={"aired_at": past_date},
        )
    )

    monkeypatch.setattr(tasks, "_try_transition", fake_try_transition)
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: svc)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())
    monkeypatch.setattr(tasks, "_scrape_with_plugins", fake_scrape_with_plugins)
    
    async def _async_mock(*a: Any, **kw: Any) -> Any:
        return _build_worker_settings()

    monkeypatch.setattr(tasks, "_resolve_runtime_settings", _async_mock)
    monkeypatch.setattr(tasks, "get_settings", _build_worker_settings)

    result = asyncio.run(tasks.scrape_item({"settings": _build_worker_settings()}, "item-2"))

    assert result == "item-2"
    assert ItemEvent.MARK_UNRELEASED not in svc.transition_calls


def test_scrape_item_proceeds_normal_scraping_if_no_aired_at(monkeypatch: Any) -> None:
    svc = FakeMediaService(
        item=MediaItemRecord(
            id="item-3",
            external_ref="tmdb:3",
            title="No Date Movie",
            state=ItemState.REQUESTED,
            attributes={},
        )
    )

    monkeypatch.setattr(tasks, "_try_transition", fake_try_transition)
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: svc)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())
    monkeypatch.setattr(tasks, "_scrape_with_plugins", fake_scrape_with_plugins)
    
    async def _async_mock(*a: Any, **kw: Any) -> Any:
        return _build_worker_settings()

    monkeypatch.setattr(tasks, "_resolve_runtime_settings", _async_mock)
    monkeypatch.setattr(tasks, "get_settings", _build_worker_settings)

    result = asyncio.run(tasks.scrape_item({"settings": _build_worker_settings()}, "item-3"))

    assert result == "item-3"
    assert ItemEvent.MARK_UNRELEASED not in svc.transition_calls


def test_poll_unreleased_items_requeues_when_release_date_passed(monkeypatch: Any) -> None:
    past_date = (datetime.now(UTC) - timedelta(days=1)).isoformat()

    fake_item = MediaItemORM(
        id="item-4",
        external_ref="tmdb:4",
        title="Now Released Movie",
        state=ItemState.UNRELEASED.value,
        recovery_attempt_count=0,
        attributes={"aired_at": past_date},
    )

    class _MockScalarResult:
        def scalars(self) -> Any:
            return self

        def all(self) -> list[MediaItemORM]:
            return [fake_item]

    class _MockSession:
        async def execute(self, *a: Any, **kw: Any) -> _MockScalarResult:
            return _MockScalarResult()

        async def __aenter__(self) -> Any:
            return self

        async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

    class _MockDB:
        def session(self) -> _MockSession:
            return _MockSession()

    svc = FakeMediaService()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: svc)

    enqueue_calls: list[str] = []

    async def fake_enqueue(
        redis: Any,
        item_id: str,
        queue_name: str,
        tenant_id: str | None = None,
    ) -> None:
        _ = (redis, queue_name, tenant_id)
        enqueue_calls.append(item_id)

    monkeypatch.setattr(tasks, "enqueue_scrape_item", fake_enqueue)
    monkeypatch.setattr(tasks, "_try_transition", fake_try_transition)

    ctx: dict[str, object] = {"db": _MockDB(), "arq_redis": {"mock": True}, "queue_name": "q"}

    async def _async_mock(*a: Any, **kw: Any) -> Any:
        return _build_worker_settings()

    async def _resolve_arq(_ctx: Any) -> Any:
        return ctx["arq_redis"]

    monkeypatch.setattr(tasks, "_resolve_runtime_settings", _async_mock)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", _resolve_arq)

    result = asyncio.run(tasks.poll_unreleased_items(ctx))
    assert result["processed"] == 1
    assert result["transitioned"] == 1
    assert len(svc.transition_calls) == 1
    assert svc.transition_calls[0] == ItemEvent.INDEX
    assert enqueue_calls == ["item-4"]
