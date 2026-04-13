"""Replayable event backplane primitives."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol, cast

from redis.exceptions import ResponseError


class ReplayConsumerFencedError(RuntimeError):
    """Raised when one consumer-group read is fenced by active ownership policy."""


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


class ReplaySubscriptionStateSink(Protocol):
    """Observer sink used to persist durable replay consumer ownership/offset state."""

    async def observe_delivery(
        self,
        *,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        node_id: str,
        tenant_id: str | None = None,
        offset: str | None = None,
        event_id: str | None = None,
    ) -> object:
        pass

    async def observe_ack(
        self,
        *,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        node_id: str,
        tenant_id: str | None = None,
        event_id: str | None = None,
    ) -> object:
        pass

    async def observe_error(
        self,
        *,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        node_id: str,
        tenant_id: str | None = None,
        error: str,
    ) -> object:
        pass

    async def claim_consumer(
        self,
        *,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        node_id: str,
        tenant_id: str | None = None,
        heartbeat_expiry_seconds: int = 120,
    ) -> object:
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
        subscription_state_sink: ReplaySubscriptionStateSink | None = None,
    ) -> None:
        self._redis = redis
        self.stream_name = stream_name
        self.maxlen = max(1, maxlen)
        self._subscription_state_sink = subscription_state_sink

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
        node_id: str | None = None,
        tenant_id: str | None = None,
        count: int = 100,
        block_ms: int | None = None,
        offset: str = ">",
        heartbeat_expiry_seconds: int = 120,
    ) -> list[ReplayEvent]:
        """Read events through one consumer group for durable subscription resume."""

        claim_result: object | None = None
        if self._subscription_state_sink is not None:
            claim_consumer = getattr(self._subscription_state_sink, "claim_consumer", None)
            if callable(claim_consumer):
                claim_result = await claim_consumer(
                    stream_name=self.stream_name,
                    group_name=group_name,
                    consumer_name=consumer_name,
                    node_id=node_id or "unknown",
                    tenant_id=tenant_id,
                    heartbeat_expiry_seconds=max(1, heartbeat_expiry_seconds),
                )
                if isinstance(claim_result, dict):
                    claim_outcome = claim_result.get("outcome")
                    claim_owner = claim_result.get("owner_node_id", node_id or "unknown")
                    claim_reason = claim_result.get("fence_reason") or "consumer_fenced"
                else:
                    claim_outcome = getattr(claim_result, "outcome", None)
                    claim_owner = getattr(claim_result, "owner_node_id", node_id or "unknown")
                    claim_reason = getattr(claim_result, "fence_reason", None) or "consumer_fenced"
                if claim_outcome == "fenced":
                    await self._subscription_state_sink.observe_error(
                        stream_name=self.stream_name,
                        group_name=group_name,
                        consumer_name=consumer_name,
                        node_id=node_id or "unknown",
                        tenant_id=tenant_id,
                        error=(
                            f"consumer_fenced owner={claim_owner} "
                            f"contender={node_id or 'unknown'} reason={claim_reason}"
                        ),
                    )
                    raise ReplayConsumerFencedError(
                        f"consumer {consumer_name} fenced by active owner {claim_owner}"
                    )

        try:
            streams = await self._redis.xreadgroup(
                group_name,
                consumer_name,
                {self.stream_name: offset},
                count=max(1, count),
                block=block_ms,
            )
        except Exception as exc:
            if self._subscription_state_sink is not None:
                await self._subscription_state_sink.observe_error(
                    stream_name=self.stream_name,
                    group_name=group_name,
                    consumer_name=consumer_name,
                    node_id=node_id or "unknown",
                    tenant_id=tenant_id,
                    error=str(exc),
                )
            raise
        events = _decode_replay_events(streams)
        if self._subscription_state_sink is not None:
            await self._subscription_state_sink.observe_delivery(
                stream_name=self.stream_name,
                group_name=group_name,
                consumer_name=consumer_name,
                node_id=node_id or "unknown",
                tenant_id=tenant_id,
                offset=offset,
                event_id=(events[-1].event_id if events else None),
            )
        return events

    async def ack(
        self,
        *,
        group_name: str,
        consumer_name: str | None = None,
        node_id: str | None = None,
        tenant_id: str | None = None,
        event_ids: list[str] | tuple[str, ...],
    ) -> int:
        """Acknowledge processed events for one durable consumer group."""

        if not event_ids:
            return 0
        acked = await self._redis.xack(self.stream_name, group_name, *event_ids)
        if (
            acked > 0
            and self._subscription_state_sink is not None
            and consumer_name is not None
        ):
            await self._subscription_state_sink.observe_ack(
                stream_name=self.stream_name,
                group_name=group_name,
                consumer_name=consumer_name,
                node_id=node_id or "unknown",
                tenant_id=tenant_id,
                event_id=event_ids[-1],
            )
        return acked


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
