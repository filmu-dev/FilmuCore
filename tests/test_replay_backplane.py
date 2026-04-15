"""Replayable event backplane tests."""

from __future__ import annotations

import asyncio

import pytest
from redis.exceptions import ResponseError

from filmu_py.core.event_bus import EventBus
from filmu_py.core.replay import RedisReplayEventBackplane, ReplayConsumerFencedError


class FakeRedisStream:
    def __init__(self) -> None:
        self.rows: list[tuple[str, dict[str, str]]] = []
        self.groups: dict[str, dict[str, dict[str, set[str]]]] = {}
        self.acked: list[tuple[str, str, tuple[str, ...]]] = []

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

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        *,
        mkstream: bool = False,
    ) -> bool:
        _ = (id, mkstream)
        groups = self.groups.setdefault(name, {})
        if groupname in groups:
            raise ResponseError("BUSYGROUP Consumer Group name already exists")
        groups[groupname] = {}
        return True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        _ = (block, consumername)
        stream_name, offset = next(iter(streams.items()))
        selected = [row for row in self.rows if _stream_id_gt(row[0], "0-0")]
        if offset != ">":
            selected = [row for row in selected if _stream_id_gt(row[0], offset)]
        if count is not None:
            selected = selected[:count]
        group = self.groups.setdefault(stream_name, {}).setdefault(groupname, {})
        group.setdefault(consumername, set()).update(row[0] for row in selected)
        return [(stream_name, selected)]

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        self.acked.append((name, groupname, ids))
        group = self.groups.setdefault(name, {}).setdefault(groupname, {})
        acked = 0
        for event_id in ids:
            for consumer_ids in group.values():
                if event_id in consumer_ids:
                    consumer_ids.remove(event_id)
                    acked += 1
                    break
        return acked

    async def xpending(self, name: str, groupname: str) -> dict[str, object]:
        group = self.groups.setdefault(name, {}).setdefault(groupname, {})
        all_ids = sorted({event_id for consumer_ids in group.values() for event_id in consumer_ids}, key=_stream_id_sort_key)
        return {
            "pending": len(all_ids),
            "min": all_ids[0] if all_ids else None,
            "max": all_ids[-1] if all_ids else None,
            "consumers": [
                {"name": consumer, "pending": len(ids)}
                for consumer, ids in sorted(group.items())
            ],
        }

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> list[object]:
        _ = (min_idle_time, justid)
        group = self.groups.setdefault(name, {}).setdefault(groupname, {})
        claimable_ids = sorted(
            {
                event_id
                for ids in group.values()
                for event_id in ids
                if _stream_id_gt(event_id, start_id) or event_id == start_id
            },
            key=_stream_id_sort_key,
        )
        if count is not None:
            claimable_ids = claimable_ids[:count]
        for ids in group.values():
            ids.difference_update(claimable_ids)
        group.setdefault(consumername, set()).update(claimable_ids)
        claimed_rows = [row for row in self.rows if row[0] in claimable_ids]
        next_start = claimable_ids[-1] if claimable_ids else start_id
        return [next_start, claimed_rows, []]


class FailingReplayBackplane:
    async def publish(
        self,
        topic: str,
        payload: dict[str, object],
        *,
        tenant_id: str | None = None,
    ) -> str:
        _ = (topic, payload, tenant_id)
        raise TimeoutError("redis unavailable")


class RecordingReplaySink:
    def __init__(self) -> None:
        self.deliveries: list[dict[str, object]] = []
        self.acks: list[dict[str, object]] = []
        self.errors: list[dict[str, object]] = []

    async def observe_delivery(self, **payload: object) -> object:
        self.deliveries.append(dict(payload))
        return None

    async def observe_ack(self, **payload: object) -> object:
        self.acks.append(dict(payload))
        return None

    async def observe_error(self, **payload: object) -> object:
        self.errors.append(dict(payload))
        return None


class ClaimingReplaySink(RecordingReplaySink):
    def __init__(self, *, outcome: str) -> None:
        super().__init__()
        self._outcome = outcome
        self.claims: list[dict[str, object]] = []

    async def claim_consumer(self, **payload: object) -> object:
        self.claims.append(dict(payload))
        return {
            "outcome": self._outcome,
            "owner_node_id": "node-owner",
            "fence_reason": "active_owner_not_expired",
        }


def _stream_id_gt(left: str, right: str) -> bool:
    left_ms, left_seq = (int(part) for part in left.split("-", 1))
    right_ms, right_seq = (int(part) for part in right.split("-", 1))
    return (left_ms, left_seq) > (right_ms, right_seq)


def _stream_id_sort_key(value: str) -> tuple[int, int]:
    left_ms, left_seq = (int(part) for part in value.split("-", 1))
    return left_ms, left_seq


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


@pytest.mark.asyncio
async def test_event_bus_replay_failure_still_delivers_local_subscriber() -> None:
    bus = EventBus()
    bus.attach_replay_backplane(FailingReplayBackplane())
    subscriber = bus.subscribe("plugins.scan.finished")
    next_event = asyncio.create_task(subscriber.__anext__())
    await asyncio.sleep(0)

    await bus.publish("plugins.scan.finished", {"tenant_id": "tenant-b", "ok": True})
    envelope = await next_event
    await subscriber.aclose()

    assert envelope.topic == "plugins.scan.finished"
    assert envelope.payload == {"tenant_id": "tenant-b", "ok": True}


def test_fake_redis_stream_signature_is_compatible() -> None:
    assert hasattr(FakeRedisStream(), "xadd")
    assert hasattr(FakeRedisStream(), "xread")
    assert hasattr(FakeRedisStream(), "xgroup_create")
    assert hasattr(FakeRedisStream(), "xreadgroup")
    assert hasattr(FakeRedisStream(), "xack")
    assert hasattr(FakeRedisStream(), "xpending")
    assert hasattr(FakeRedisStream(), "xautoclaim")


@pytest.mark.asyncio
async def test_redis_replay_backplane_supports_consumer_group_reads_and_ack() -> None:
    redis = FakeRedisStream()
    backplane = RedisReplayEventBackplane(redis, stream_name="filmu:events", maxlen=10)
    await backplane.publish("tenant.updated", {"ok": True}, tenant_id="tenant-a")
    await backplane.publish("tenant.updated", {"ok": False}, tenant_id="tenant-b")

    await backplane.ensure_consumer_group("filmu-api", start_id="0-0")
    events = await backplane.read_group(
        group_name="filmu-api",
        consumer_name="consumer-1",
        count=2,
    )
    acked = await backplane.ack(
        group_name="filmu-api",
        event_ids=[event.event_id for event in events],
    )

    assert [event.event_id for event in events] == ["1-0", "2-0"]
    assert acked == 2


@pytest.mark.asyncio
async def test_redis_replay_backplane_ignores_existing_consumer_group() -> None:
    redis = FakeRedisStream()
    backplane = RedisReplayEventBackplane(redis, stream_name="filmu:events", maxlen=10)

    await backplane.ensure_consumer_group("filmu-api", start_id="0-0")
    await backplane.ensure_consumer_group("filmu-api", start_id="0-0")

    assert "filmu-api" in redis.groups["filmu:events"]


@pytest.mark.asyncio
async def test_redis_replay_backplane_reports_delivery_and_ack_state() -> None:
    redis = FakeRedisStream()
    sink = RecordingReplaySink()
    backplane = RedisReplayEventBackplane(
        redis,
        stream_name="filmu:events",
        maxlen=10,
        subscription_state_sink=sink,
    )
    await backplane.publish("tenant.updated", {"ok": True}, tenant_id="tenant-a")
    await backplane.ensure_consumer_group("filmu-api", start_id="0-0")

    events = await backplane.read_group(
        group_name="filmu-api",
        consumer_name="consumer-1",
        node_id="node-a",
        tenant_id="tenant-a",
    )
    await backplane.ack(
        group_name="filmu-api",
        consumer_name="consumer-1",
        node_id="node-a",
        tenant_id="tenant-a",
        event_ids=[event.event_id for event in events],
    )

    assert sink.deliveries == [
        {
            "stream_name": "filmu:events",
            "group_name": "filmu-api",
            "consumer_name": "consumer-1",
            "node_id": "node-a",
            "tenant_id": "tenant-a",
            "offset": ">",
            "event_id": "1-0",
        }
    ]
    assert sink.acks == [
        {
            "stream_name": "filmu:events",
            "group_name": "filmu-api",
            "consumer_name": "consumer-1",
            "node_id": "node-a",
            "tenant_id": "tenant-a",
            "event_id": "1-0",
        }
    ]


@pytest.mark.asyncio
async def test_redis_replay_backplane_fences_consumer_when_claim_is_denied() -> None:
    redis = FakeRedisStream()
    sink = ClaimingReplaySink(outcome="fenced")
    backplane = RedisReplayEventBackplane(
        redis,
        stream_name="filmu:events",
        maxlen=10,
        subscription_state_sink=sink,
    )
    await backplane.publish("tenant.updated", {"ok": True}, tenant_id="tenant-a")
    await backplane.ensure_consumer_group("filmu-api", start_id="0-0")

    with pytest.raises(ReplayConsumerFencedError):
        await backplane.read_group(
            group_name="filmu-api",
            consumer_name="consumer-1",
            node_id="node-contender",
            tenant_id="tenant-a",
            heartbeat_expiry_seconds=45,
        )

    assert sink.claims == [
        {
            "stream_name": "filmu:events",
            "group_name": "filmu-api",
            "consumer_name": "consumer-1",
            "node_id": "node-contender",
            "tenant_id": "tenant-a",
            "heartbeat_expiry_seconds": 45,
        }
    ]
    assert sink.errors == []


@pytest.mark.asyncio
async def test_redis_replay_backplane_reports_pending_summary() -> None:
    redis = FakeRedisStream()
    backplane = RedisReplayEventBackplane(redis, stream_name="filmu:events", maxlen=10)
    await backplane.publish("tenant.updated", {"ok": True}, tenant_id="tenant-a")
    await backplane.publish("tenant.updated", {"ok": False}, tenant_id="tenant-b")
    await backplane.ensure_consumer_group("filmu-api", start_id="0-0")
    await backplane.read_group(
        group_name="filmu-api",
        consumer_name="consumer-1",
        count=2,
    )

    summary = await backplane.pending_summary(group_name="filmu-api")

    assert summary.pending_count == 2
    assert summary.oldest_event_id == "1-0"
    assert summary.latest_event_id == "2-0"
    assert summary.consumer_counts == {"consumer-1": 2}


@pytest.mark.asyncio
async def test_redis_replay_backplane_claims_pending_entries_into_recovery_consumer() -> None:
    redis = FakeRedisStream()
    sink = RecordingReplaySink()
    backplane = RedisReplayEventBackplane(
        redis,
        stream_name="filmu:events",
        maxlen=10,
        subscription_state_sink=sink,
    )
    await backplane.publish("tenant.updated", {"ok": True}, tenant_id="tenant-a")
    await backplane.publish("tenant.updated", {"ok": False}, tenant_id="tenant-a")
    await backplane.ensure_consumer_group("filmu-api", start_id="0-0")
    await backplane.read_group(
        group_name="filmu-api",
        consumer_name="consumer-1",
        node_id="node-a",
        tenant_id="tenant-a",
        count=2,
    )

    result = await backplane.claim_pending(
        group_name="filmu-api",
        consumer_name="recovery-ops",
        node_id="node-recovery",
        tenant_id="tenant-a",
        min_idle_ms=5_000,
        count=10,
    )

    assert [event.event_id for event in result.claimed_events] == ["1-0", "2-0"]
    assert result.pending_before.pending_count == 2
    assert result.pending_after.pending_count == 2
    assert result.pending_after.consumer_counts == {"consumer-1": 0, "recovery-ops": 2}
    assert sink.deliveries[-1] == {
        "stream_name": "filmu:events",
        "group_name": "filmu-api",
        "consumer_name": "recovery-ops",
        "node_id": "node-recovery",
        "tenant_id": "tenant-a",
        "offset": "0-0",
        "event_id": "2-0",
    }
