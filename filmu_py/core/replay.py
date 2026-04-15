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

    async def xpending(self, name: str, groupname: str) -> object:
        pass

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ) -> object:
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


@dataclass(frozen=True, slots=True)
class ReplayPendingSummary:
    """Summary of pending consumer-group deliveries for one replay stream."""

    pending_count: int
    oldest_event_id: str | None
    latest_event_id: str | None
    consumer_counts: dict[str, int]


@dataclass(frozen=True, slots=True)
class ReplayPendingClaimResult:
    """Outcome of one stale pending-delivery claim operation."""

    group_name: str
    consumer_name: str
    min_idle_ms: int
    claimed_events: list[ReplayEvent]
    next_start_id: str
    pending_before: ReplayPendingSummary
    pending_after: ReplayPendingSummary


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
                else:
                    claim_outcome = getattr(claim_result, "outcome", None)
                    claim_owner = getattr(claim_result, "owner_node_id", node_id or "unknown")
                if claim_outcome == "fenced":
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

    async def pending_summary(
        self,
        *,
        group_name: str,
    ) -> ReplayPendingSummary:
        """Return one bounded summary of pending consumer-group deliveries."""

        raw_summary = await self._redis.xpending(self.stream_name, group_name)
        return _decode_pending_summary(raw_summary)

    async def claim_pending(
        self,
        *,
        group_name: str,
        consumer_name: str,
        node_id: str | None = None,
        tenant_id: str | None = None,
        min_idle_ms: int = 60_000,
        count: int = 100,
        start_id: str = "0-0",
        heartbeat_expiry_seconds: int = 120,
    ) -> ReplayPendingClaimResult:
        """Claim stale pending entries into one recovery consumer."""

        pending_before = await self.pending_summary(group_name=group_name)

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
                else:
                    claim_outcome = getattr(claim_result, "outcome", None)
                    claim_owner = getattr(claim_result, "owner_node_id", node_id or "unknown")
                if claim_outcome == "fenced":
                    raise ReplayConsumerFencedError(
                        f"consumer {consumer_name} fenced by active owner {claim_owner}"
                    )

        raw_claim = await self._redis.xautoclaim(
            self.stream_name,
            group_name,
            consumer_name,
            max(1, min_idle_ms),
            start_id=start_id,
            count=max(1, count),
            justid=False,
        )
        next_start_id, claimed_events = _decode_autoclaim_result(self.stream_name, raw_claim)
        if self._subscription_state_sink is not None and claimed_events:
            await self._subscription_state_sink.observe_delivery(
                stream_name=self.stream_name,
                group_name=group_name,
                consumer_name=consumer_name,
                node_id=node_id or "unknown",
                tenant_id=tenant_id,
                offset=start_id,
                event_id=claimed_events[-1].event_id,
            )
        pending_after = await self.pending_summary(group_name=group_name)
        return ReplayPendingClaimResult(
            group_name=group_name,
            consumer_name=consumer_name,
            min_idle_ms=max(1, min_idle_ms),
            claimed_events=claimed_events,
            next_start_id=next_start_id,
            pending_before=pending_before,
            pending_after=pending_after,
        )


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


def _decode_pending_summary(raw_summary: object) -> ReplayPendingSummary:
    pending_count = 0
    oldest_event_id: str | None = None
    latest_event_id: str | None = None
    consumer_counts: dict[str, int] = {}

    if isinstance(raw_summary, dict):
        pending_count = int(raw_summary.get("pending", 0) or 0)
        oldest = raw_summary.get("min")
        latest = raw_summary.get("max")
        oldest_event_id = str(oldest) if oldest else None
        latest_event_id = str(latest) if latest else None
        consumers = raw_summary.get("consumers", ())
        if isinstance(consumers, list):
            for consumer in consumers:
                if isinstance(consumer, dict):
                    name = consumer.get("name")
                    pending = consumer.get("pending", 0)
                    if name:
                        consumer_counts[str(name)] = int(pending or 0)
    elif isinstance(raw_summary, (tuple, list)) and len(raw_summary) >= 4:
        pending_count = int(raw_summary[0] or 0)
        oldest_event_id = str(raw_summary[1]) if raw_summary[1] else None
        latest_event_id = str(raw_summary[2]) if raw_summary[2] else None
        consumers = raw_summary[3]
        if isinstance(consumers, list):
            for consumer in consumers:
                if isinstance(consumer, dict):
                    name = consumer.get("name")
                    pending = consumer.get("pending", 0)
                    if name:
                        consumer_counts[str(name)] = int(pending or 0)
                elif isinstance(consumer, (tuple, list)) and len(consumer) >= 2:
                    consumer_counts[str(consumer[0])] = int(consumer[1] or 0)

    return ReplayPendingSummary(
        pending_count=pending_count,
        oldest_event_id=oldest_event_id,
        latest_event_id=latest_event_id,
        consumer_counts=consumer_counts,
    )


def _decode_autoclaim_result(
    stream_name: str,
    raw_claim: object,
) -> tuple[str, list[ReplayEvent]]:
    next_start_id = "0-0"
    rows: list[tuple[bytes | str, dict[bytes | str, bytes | str]]] = []

    if isinstance(raw_claim, (tuple, list)) and len(raw_claim) >= 2:
        raw_next_start = raw_claim[0]
        next_start_id = (
            raw_next_start.decode("utf-8")
            if isinstance(raw_next_start, bytes)
            else str(raw_next_start)
        )
        raw_rows = raw_claim[1]
        if isinstance(raw_rows, list):
            for row in raw_rows:
                if isinstance(row, (tuple, list)) and len(row) >= 2 and isinstance(row[1], dict):
                    rows.append(
                        (
                            cast(bytes | str, row[0]),
                            cast(dict[bytes | str, bytes | str], row[1]),
                        )
                    )

    events = _decode_replay_events([(stream_name, rows)])
    return next_start_id, events
