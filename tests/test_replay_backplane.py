"""Replayable event backplane tests."""

from __future__ import annotations

import asyncio

import pytest

from filmu_py.core.event_bus import EventBus
from filmu_py.core.replay import RedisReplayEventBackplane


class FakeRedisStream:
    def __init__(self) -> None:
        self.rows: list[tuple[str, dict[str, str]]] = []

    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
        *,
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        _ = (name, id, approximate)
        event_id = f"{len(self.rows) + 1}-0"
        self.rows.append((event_id, fields))
        if maxlen is not None and len(self.rows) > maxlen:
            del self.rows[: len(self.rows) - maxlen]
        return event_id

    async def xread(
        self,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        _ = block
        offset = next(iter(streams.values()))
        selected = [row for row in self.rows if _stream_id_gt(row[0], offset)]
        if count is not None:
            selected = selected[:count]
        return [("filmu:events", selected)]


def _stream_id_gt(left: str, right: str) -> bool:
    left_ms, left_seq = (int(part) for part in left.split("-", 1))
    right_ms, right_seq = (int(part) for part in right.split("-", 1))
    return (left_ms, left_seq) > (right_ms, right_seq)


@pytest.mark.asyncio
async def test_redis_replay_backplane_round_trips_tenant_event() -> None:
    redis = FakeRedisStream()
    backplane = RedisReplayEventBackplane(redis, stream_name="filmu:events", maxlen=10)

    event_id = await backplane.publish(
        "catalog.item.updated",
        {"tenant_id": "tenant-a", "item_id": "item-1"},
        tenant_id="tenant-a",
    )
    events = await backplane.read_after("0-0")

    assert event_id == "1-0"
    assert len(events) == 1
    assert events[0].event_id == "1-0"
    assert events[0].topic == "catalog.item.updated"
    assert events[0].tenant_id == "tenant-a"
    assert events[0].payload == {"tenant_id": "tenant-a", "item_id": "item-1"}


@pytest.mark.asyncio
async def test_event_bus_publishes_to_replay_backplane_and_local_subscribers() -> None:
    redis = FakeRedisStream()
    bus = EventBus()
    bus.attach_replay_backplane(RedisReplayEventBackplane(redis, stream_name="filmu:events"))
    subscriber = bus.subscribe("plugins.scan.finished")
    next_event = asyncio.create_task(subscriber.__anext__())
    await asyncio.sleep(0)

    await bus.publish("plugins.scan.finished", {"tenant_id": "tenant-b", "ok": True})
    envelope = await next_event
    await subscriber.aclose()

    assert envelope.topic == "plugins.scan.finished"
    assert envelope.payload == {"tenant_id": "tenant-b", "ok": True}
    assert redis.rows
    replay_payload = redis.rows[0][1]
    assert replay_payload["tenant_id"] == "tenant-b"
    assert "plugins.scan.finished" in replay_payload["topic"]


def test_fake_redis_stream_signature_is_compatible() -> None:
    assert hasattr(FakeRedisStream(), "xadd")
    assert hasattr(FakeRedisStream(), "xread")
