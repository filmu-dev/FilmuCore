from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from filmu_py.core.event_bus import EventBus
from filmu_py.db.models import ItemStateEventORM, MediaItemORM, OutboxEventORM
from filmu_py.services.media import MediaService, OutboxPublishSnapshot
from filmu_py.state.item import ItemEvent, ItemState
from filmu_py.workers import tasks


def _build_item(*, item_id: str = "item-1", state: ItemState = ItemState.INDEXED) -> MediaItemORM:
    return MediaItemORM(
        id=item_id,
        external_ref=f"tmdb:{item_id}",
        title="Example Movie",
        state=state.value,
        recovery_attempt_count=0,
        attributes={"item_type": "movie", "tmdb_id": item_id},
    )


class _TransitionExecuteResult:
    def __init__(self, item: MediaItemORM | None) -> None:
        self._item = item

    def scalar_one_or_none(self) -> MediaItemORM | None:
        return self._item


@dataclass
class _OutboxStorage:
    item: MediaItemORM | None
    committed_events: list[ItemStateEventORM] = field(default_factory=list)
    committed_outbox: list[OutboxEventORM] = field(default_factory=list)


class _TransitionSession:
    def __init__(self, storage: _OutboxStorage, *, fail_commit: bool = False) -> None:
        self.storage = storage
        self.fail_commit = fail_commit
        self.pending_events: list[ItemStateEventORM] = []
        self.pending_outbox: list[OutboxEventORM] = []
        self.committed = False

    async def execute(self, _statement: object) -> _TransitionExecuteResult:
        return _TransitionExecuteResult(self.storage.item)

    def add(self, value: object) -> None:
        if isinstance(value, ItemStateEventORM):
            self.pending_events.append(value)
        elif isinstance(value, OutboxEventORM):
            self.pending_outbox.append(value)

    async def commit(self) -> None:
        if self.fail_commit:
            raise RuntimeError("commit failed")
        self.storage.committed_events.extend(self.pending_events)
        self.storage.committed_outbox.extend(self.pending_outbox)
        self.committed = True


@dataclass
class _TransitionRuntime:
    storage: _OutboxStorage
    fail_commit: bool = False
    last_session: _TransitionSession | None = None

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_TransitionSession]:
        session = _TransitionSession(self.storage, fail_commit=self.fail_commit)
        self.last_session = session
        yield session


class _OutboxScalarResult:
    def __init__(self, rows: list[OutboxEventORM]) -> None:
        self._rows = rows

    def scalars(self) -> _OutboxScalarResult:
        return self

    def all(self) -> list[OutboxEventORM]:
        return self._rows


@dataclass
class _OutboxPublishStorage:
    rows: list[OutboxEventORM]


class _OutboxPublishSession:
    def __init__(self, storage: _OutboxPublishStorage) -> None:
        self.storage = storage
        self.committed = False

    async def execute(self, _statement: object) -> _OutboxScalarResult:
        pending = [
            row for row in self.storage.rows if row.published_at is None and row.failed_at is None
        ]
        pending.sort(key=lambda row: row.created_at)
        return _OutboxScalarResult(pending)

    async def commit(self) -> None:
        self.committed = True


@dataclass
class _OutboxPublishRuntime:
    storage: _OutboxPublishStorage
    last_session: _OutboxPublishSession | None = None

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_OutboxPublishSession]:
        session = _OutboxPublishSession(self.storage)
        self.last_session = session
        yield session


class FailingEventBus(EventBus):
    async def publish(self, topic: str, payload: dict[str, Any]) -> None:
        _ = (topic, payload)
        raise RuntimeError("publish failed")


@dataclass
class FakeOutboxMediaService:
    snapshot: OutboxPublishSnapshot

    async def publish_outbox_events(self, *, max_outbox_attempts: int = 5) -> OutboxPublishSnapshot:
        _ = max_outbox_attempts
        return self.snapshot


def test_transition_item_creates_outbox_row_in_same_transaction() -> None:
    item = _build_item(state=ItemState.INDEXED)
    storage = _OutboxStorage(item=item)
    runtime = _TransitionRuntime(storage=storage)
    service = MediaService(db=runtime, event_bus=EventBus())  # type: ignore[arg-type]

    result = asyncio.run(
        service.transition_item(item_id=item.id, event=ItemEvent.SCRAPE, message="scrape done")
    )

    assert result.state is ItemState.SCRAPED
    assert len(storage.committed_events) == 1
    assert len(storage.committed_outbox) == 1
    assert storage.committed_outbox[0].event_type == "item.state.changed"
    assert storage.committed_outbox[0].payload["item_id"] == item.id


def test_transition_item_rollback_removes_both_transition_and_outbox() -> None:
    item = _build_item(state=ItemState.INDEXED)
    storage = _OutboxStorage(item=item)
    runtime = _TransitionRuntime(storage=storage, fail_commit=True)
    service = MediaService(db=runtime, event_bus=EventBus())  # type: ignore[arg-type]

    try:
        asyncio.run(
            service.transition_item(item_id=item.id, event=ItemEvent.SCRAPE, message="scrape done")
        )
    except RuntimeError as exc:
        assert str(exc) == "commit failed"
    else:
        raise AssertionError("expected commit failure")

    assert storage.committed_events == []
    assert storage.committed_outbox == []


def test_publish_outbox_events_publishes_unpublished_rows() -> None:
    row = OutboxEventORM(
        event_type="item.state.changed",
        payload={
            "item_id": "item-1",
            "state": "scraped",
            "event": "scrape",
            "message": "scrape done",
        },
        item_id="item-1",
        created_at=datetime(2026, 3, 15, 2, 0, tzinfo=UTC),
        attempt_count=0,
    )
    runtime = _OutboxPublishRuntime(storage=_OutboxPublishStorage(rows=[row]))
    event_bus = EventBus()
    service = MediaService(db=runtime, event_bus=event_bus)  # type: ignore[arg-type]

    snapshot = asyncio.run(service.publish_outbox_events(max_outbox_attempts=5))

    assert snapshot == OutboxPublishSnapshot(published_count=1, failed_count=0)
    assert row.published_at is not None
    assert row.failed_at is None
    assert row.attempt_count == 0


def test_publish_outbox_events_failed_publish_increments_attempt_count() -> None:
    row = OutboxEventORM(
        event_type="item.state.changed",
        payload={
            "item_id": "item-1",
            "state": "scraped",
            "event": "scrape",
            "message": "scrape done",
        },
        item_id="item-1",
        created_at=datetime(2026, 3, 15, 2, 0, tzinfo=UTC),
        attempt_count=0,
    )
    runtime = _OutboxPublishRuntime(storage=_OutboxPublishStorage(rows=[row]))
    service = MediaService(db=runtime, event_bus=FailingEventBus())  # type: ignore[arg-type]

    snapshot = asyncio.run(service.publish_outbox_events(max_outbox_attempts=5))

    assert snapshot == OutboxPublishSnapshot(published_count=0, failed_count=1)
    assert row.published_at is None
    assert row.failed_at is None
    assert row.attempt_count == 1


def test_publish_outbox_events_marks_row_failed_after_threshold() -> None:
    row = OutboxEventORM(
        event_type="item.state.changed",
        payload={
            "item_id": "item-1",
            "state": "scraped",
            "event": "scrape",
            "message": "scrape done",
        },
        item_id="item-1",
        created_at=datetime(2026, 3, 15, 2, 0, tzinfo=UTC),
        attempt_count=4,
    )
    runtime = _OutboxPublishRuntime(storage=_OutboxPublishStorage(rows=[row]))
    service = MediaService(db=runtime, event_bus=FailingEventBus())  # type: ignore[arg-type]

    snapshot = asyncio.run(service.publish_outbox_events(max_outbox_attempts=5))

    assert snapshot == OutboxPublishSnapshot(published_count=0, failed_count=1)
    assert row.attempt_count == 5
    assert row.failed_at is not None


def test_publish_outbox_events_task_uses_service_snapshot(monkeypatch: Any) -> None:
    media_service = FakeOutboxMediaService(
        snapshot=OutboxPublishSnapshot(published_count=2, failed_count=0)
    )
    async def fake_settings(*args: Any, **kwargs: Any) -> Any:
        return SimpleNamespace(max_outbox_attempts=5)

    monkeypatch.setattr(tasks, "_resolve_runtime_settings", fake_settings)
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    ctx: dict[str, Any] = {}

    published = asyncio.run(tasks.publish_outbox_events(ctx))

    assert published == 2
