"""Replayable event backplane primitives."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol, cast

from redis.exceptions import ResponseError


class RedisStreamClient(Protocol):
    """Subset of Redis stream commands used by the replay backplane."""

    async def xadd(
        self,
        name: str,
        fields: dict[str, str],
        *,
        id: str = "*",
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> bytes | str:
        pass

    async def xread(
        self,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[bytes | str, list[tuple[bytes | str, dict[bytes | str, bytes | str]]]]]:
        pass

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        *,
        mkstream: bool = False,
    ) -> object:
        pass

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[bytes | str, list[tuple[bytes | str, dict[bytes | str, bytes | str]]]]]:
        pass

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        pass


@dataclass(frozen=True, slots=True)
class ReplayEvent:
    """One event read from the durable replay stream."""

    event_id: str
    topic: str
    tenant_id: str | None
    payload: dict[str, Any]


class RedisReplayEventBackplane:
    """Redis Streams-backed durable event journal with replay offsets."""

    def __init__(
        self,
        redis: RedisStreamClient,
        *,
        stream_name: str = "filmu:events",
        maxlen: int = 10_000,
    ) -> None:
        self._redis = redis
        self.stream_name = stream_name
        self.maxlen = max(1, maxlen)

    async def publish(
        self,
        topic: str,
        payload: dict[str, Any],
        *,
        tenant_id: str | None = None,
    ) -> str:
        """Append one event to the replay stream and return its stream id."""

        fields = {
            "topic": topic,
            "tenant_id": tenant_id or "",
            "payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        }
        event_id = await self._redis.xadd(
            self.stream_name,
            fields,
            maxlen=self.maxlen,
            approximate=True,
        )
        return event_id.decode("utf-8") if isinstance(event_id, bytes) else event_id

    async def ensure_consumer_group(
        self,
        group_name: str,
        *,
        start_id: str = "0-0",
    ) -> None:
        """Ensure one durable consumer group exists for the replay stream."""

        try:
            await self._redis.xgroup_create(
                self.stream_name,
                group_name,
                id=start_id,
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read_after(self, offset: str = "0-0", *, count: int = 100) -> list[ReplayEvent]:
        """Read replay events after one Redis Streams offset."""

        streams = await self._redis.xread(
            {self.stream_name: offset},
            count=max(1, count),
            block=None,
        )
        return _decode_replay_events(streams)

    async def read_group(
        self,
        *,
        group_name: str,
        consumer_name: str,
        count: int = 100,
        block_ms: int | None = None,
        offset: str = ">",
    ) -> list[ReplayEvent]:
        """Read events through one consumer group for durable subscription resume."""

        streams = await self._redis.xreadgroup(
            group_name,
            consumer_name,
            {self.stream_name: offset},
            count=max(1, count),
            block=block_ms,
        )
        return _decode_replay_events(streams)

    async def ack(
        self,
        *,
        group_name: str,
        event_ids: list[str] | tuple[str, ...],
    ) -> int:
        """Acknowledge processed events for one durable consumer group."""

        if not event_ids:
            return 0
        return await self._redis.xack(self.stream_name, group_name, *event_ids)


def _decode_replay_events(
    streams: list[tuple[bytes | str, list[tuple[bytes | str, dict[bytes | str, bytes | str]]]]],
) -> list[ReplayEvent]:
    events: list[ReplayEvent] = []
    for _stream_name, rows in streams:
        for raw_event_id, raw_fields in rows:
            event_id = raw_event_id.decode("utf-8") if isinstance(raw_event_id, bytes) else raw_event_id
            fields = {
                (key.decode("utf-8") if isinstance(key, bytes) else key): (
                    value.decode("utf-8") if isinstance(value, bytes) else value
                )
                for key, value in raw_fields.items()
            }
            payload_raw = fields.get("payload", "{}")
            try:
                payload = json.loads(payload_raw)
            except ValueError:
                payload = {}
            events.append(
                ReplayEvent(
                    event_id=event_id,
                    topic=fields.get("topic", ""),
                    tenant_id=fields.get("tenant_id") or None,
                    payload=cast(dict[str, Any], payload if isinstance(payload, dict) else {}),
                )
            )
    return events
