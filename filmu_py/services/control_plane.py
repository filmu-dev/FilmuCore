"""Durable control-plane subscriber ownership and resume-offset state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from filmu_py.db.models import ControlPlaneSubscriberORM
from filmu_py.db.runtime import DatabaseRuntime


@dataclass(frozen=True, slots=True)
class ControlPlaneSubscriberRecord:
    """One durable replay/control-plane subscriber ledger entry."""

    stream_name: str
    group_name: str
    consumer_name: str
    node_id: str
    tenant_id: str | None
    status: str
    last_read_offset: str | None
    last_delivered_event_id: str | None
    last_acked_event_id: str | None
    last_error: str | None
    claimed_at: datetime
    last_heartbeat_at: datetime
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ControlPlaneConsumerClaimResult:
    """Outcome for one durable consumer ownership claim/fencing decision."""

    stream_name: str
    group_name: str
    consumer_name: str
    node_id: str
    tenant_id: str | None
    outcome: Literal["claimed", "transferred", "fenced"]
    owner_node_id: str
    heartbeat_expired: bool
    fence_reason: str | None = None


@dataclass(frozen=True, slots=True)
class ControlPlaneSummary:
    """Derived replay/control-plane health rollup for operator posture routes."""

    total_subscribers: int
    active_subscribers: int
    stale_subscribers: int
    error_subscribers: int
    fenced_subscribers: int
    ack_pending_subscribers: int
    stream_count: int
    group_count: int
    node_count: int
    tenant_count: int
    oldest_heartbeat_age_seconds: float | None
    status_counts: dict[str, int]
    required_actions: tuple[str, ...]
    remaining_gaps: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ControlPlaneRemediationResult:
    """Outcome of one control-plane stale/fence/error sweep."""

    active_within_seconds: int
    stale_marked_subscribers: int
    fence_resolved_subscribers: int
    error_recovered_subscribers: int
    total_updated_subscribers: int
    summary: ControlPlaneSummary


@dataclass(frozen=True, slots=True)
class ControlPlaneAckRecoveryResult:
    """Outcome of one operator-triggered ack-backlog recovery sweep."""

    active_within_seconds: int
    rewound_subscribers: int
    stale_marked_subscribers: int
    pending_without_ack_subscribers: int
    total_updated_subscribers: int
    summary: ControlPlaneSummary


class ControlPlaneService:
    """Persist and summarize active replay/control-plane subscriber ownership."""

    def __init__(self, db: DatabaseRuntime) -> None:
        self._db = db

    async def list_subscribers(
        self,
        *,
        active_within_seconds: int = 120,
    ) -> list[ControlPlaneSubscriberRecord]:
        """Return subscriber rows with derived stale/active status."""

        stale_before = datetime.now(UTC) - timedelta(seconds=max(1, active_within_seconds))
        async with self._db.session() as session:
            rows = (
                await session.execute(
                    select(ControlPlaneSubscriberORM).order_by(
                        ControlPlaneSubscriberORM.stream_name.asc(),
                        ControlPlaneSubscriberORM.group_name.asc(),
                        ControlPlaneSubscriberORM.consumer_name.asc(),
                    )
                )
            ).scalars()
            records = []
            for row in rows:
                status = row.status
                if row.last_heartbeat_at < stale_before and status == "active":
                    status = "stale"
                records.append(_record_from_orm(row, status=status))
            return records

    async def summarize_subscribers(
        self,
        *,
        active_within_seconds: int = 120,
    ) -> ControlPlaneSummary:
        """Return a bounded summary of durable replay/control-plane ownership health."""

        records = await self.list_subscribers(active_within_seconds=active_within_seconds)
        now = datetime.now(UTC)
        status_counts: dict[str, int] = {}
        active = 0
        stale = 0
        error = 0
        fenced = 0
        ack_pending = 0
        stream_names: set[str] = set()
        group_keys: set[tuple[str, str]] = set()
        node_ids: set[str] = set()
        tenant_ids: set[str] = set()
        oldest_heartbeat_age_seconds: float | None = None

        for record in records:
            status_counts[record.status] = status_counts.get(record.status, 0) + 1
            stream_names.add(record.stream_name)
            group_keys.add((record.stream_name, record.group_name))
            node_ids.add(record.node_id)
            if record.tenant_id:
                tenant_ids.add(record.tenant_id)

            heartbeat_age_seconds = max(
                0.0,
                (now - record.last_heartbeat_at).total_seconds(),
            )
            if oldest_heartbeat_age_seconds is None:
                oldest_heartbeat_age_seconds = heartbeat_age_seconds
            else:
                oldest_heartbeat_age_seconds = max(
                    oldest_heartbeat_age_seconds,
                    heartbeat_age_seconds,
                )

            if record.status == "active":
                active += 1
            elif record.status == "stale":
                stale += 1
            elif record.status == "error":
                error += 1

            if _has_unresolved_fence(record):
                fenced += 1
            if record.last_delivered_event_id and record.last_delivered_event_id != record.last_acked_event_id:
                ack_pending += 1

        required_actions: list[str] = []
        remaining_gaps: list[str] = []
        if stale > 0:
            required_actions.append("recover_stale_control_plane_subscribers")
            remaining_gaps.append(
                "at least one control-plane subscriber heartbeat is stale"
            )
        if fenced > 0:
            required_actions.append("resolve_fenced_control_plane_consumers")
            remaining_gaps.append(
                "one or more replay consumers remain fenced by an active owner"
            )
        if error > 0:
            required_actions.append("investigate_control_plane_subscriber_errors")
            remaining_gaps.append(
                "one or more control-plane subscribers are in error status"
            )
        if ack_pending > 0:
            required_actions.append("drain_control_plane_ack_backlog")
            remaining_gaps.append(
                "one or more subscribers have unacknowledged delivered events"
            )

        return ControlPlaneSummary(
            total_subscribers=len(records),
            active_subscribers=active,
            stale_subscribers=stale,
            error_subscribers=error,
            fenced_subscribers=fenced,
            ack_pending_subscribers=ack_pending,
            stream_count=len(stream_names),
            group_count=len(group_keys),
            node_count=len(node_ids),
            tenant_count=len(tenant_ids),
            oldest_heartbeat_age_seconds=oldest_heartbeat_age_seconds,
            status_counts=dict(sorted(status_counts.items())),
            required_actions=tuple(required_actions),
            remaining_gaps=tuple(remaining_gaps),
        )

    async def remediate_subscribers(
        self,
        *,
        active_within_seconds: int = 120,
    ) -> ControlPlaneRemediationResult:
        """Persistently mark expired/fenced/error rows stale so ownership can recover cleanly."""

        now = datetime.now(UTC)
        stale_before = now - timedelta(seconds=max(1, active_within_seconds))
        stale_marked = 0
        fence_resolved = 0
        error_recovered = 0

        async with self._db.session() as session:
            rows = (
                await session.execute(
                    select(ControlPlaneSubscriberORM).order_by(
                        ControlPlaneSubscriberORM.stream_name.asc(),
                        ControlPlaneSubscriberORM.group_name.asc(),
                        ControlPlaneSubscriberORM.consumer_name.asc(),
                    )
                )
            ).scalars()
            for row in rows:
                heartbeat_expired = row.last_heartbeat_at < stale_before
                unresolved_fence = _has_unresolved_fence(_record_from_orm(row))
                should_mark_stale = heartbeat_expired and row.status in {
                    "active",
                    "error",
                    "fenced",
                }
                if not should_mark_stale and not (heartbeat_expired and unresolved_fence):
                    continue

                if unresolved_fence or row.status == "fenced":
                    fence_resolved += 1
                elif row.status == "error":
                    error_recovered += 1
                elif row.status == "active":
                    stale_marked += 1

                row.status = "stale"
                row.updated_at = now
                if unresolved_fence and "consumer_fenced" in str(row.last_error or ""):
                    row.last_error = (
                        f"{row.last_error}; stale_fence_swept at={now.isoformat()}"
                    )
            await session.flush()
            await session.commit()

        summary = await self.summarize_subscribers(active_within_seconds=active_within_seconds)
        return ControlPlaneRemediationResult(
            active_within_seconds=active_within_seconds,
            stale_marked_subscribers=stale_marked,
            fence_resolved_subscribers=fence_resolved,
            error_recovered_subscribers=error_recovered,
            total_updated_subscribers=stale_marked + fence_resolved + error_recovered,
            summary=summary,
        )

    async def recover_ack_backlog(
        self,
        *,
        active_within_seconds: int = 120,
    ) -> ControlPlaneAckRecoveryResult:
        """Rewind abandoned subscriber delivery cursors back to the last acknowledged event."""

        now = datetime.now(UTC)
        stale_before = now - timedelta(seconds=max(1, active_within_seconds))
        rewound = 0
        stale_marked = 0
        pending_without_ack = 0
        updated_rows = 0

        async with self._db.session() as session:
            rows = (
                await session.execute(
                    select(ControlPlaneSubscriberORM).order_by(
                        ControlPlaneSubscriberORM.stream_name.asc(),
                        ControlPlaneSubscriberORM.group_name.asc(),
                        ControlPlaneSubscriberORM.consumer_name.asc(),
                    )
                )
            ).scalars()
            for row in rows:
                ack_pending = bool(
                    row.last_delivered_event_id
                    and row.last_delivered_event_id != row.last_acked_event_id
                )
                if not ack_pending:
                    continue

                heartbeat_expired = row.last_heartbeat_at < stale_before
                recoverable_status = row.status in {"stale", "fenced", "error"}
                if not heartbeat_expired and not recoverable_status:
                    continue

                if row.last_acked_event_id is None:
                    pending_without_ack += 1
                    if row.status != "stale":
                        row.status = "stale"
                        stale_marked += 1
                    if row.last_error:
                        row.last_error = (
                            f"{row.last_error}; ack_recovery_blocked no_last_acked_event at={now.isoformat()}"
                        )
                    else:
                        row.last_error = (
                            f"ack_recovery_blocked no_last_acked_event at={now.isoformat()}"
                        )
                    row.updated_at = now
                    updated_rows += 1
                    continue

                if row.status != "stale":
                    row.status = "stale"
                    stale_marked += 1

                row.last_read_offset = row.last_acked_event_id
                row.last_delivered_event_id = row.last_acked_event_id
                row.updated_at = now
                if row.last_error:
                    row.last_error = (
                        f"{row.last_error}; ack_recovery_rewound to={row.last_acked_event_id} at={now.isoformat()}"
                    )
                else:
                    row.last_error = (
                        f"ack_recovery_rewound to={row.last_acked_event_id} at={now.isoformat()}"
                    )
                rewound += 1
                updated_rows += 1
            await session.flush()
            await session.commit()

        summary = await self.summarize_subscribers(active_within_seconds=active_within_seconds)
        return ControlPlaneAckRecoveryResult(
            active_within_seconds=active_within_seconds,
            rewound_subscribers=rewound,
            stale_marked_subscribers=stale_marked,
            pending_without_ack_subscribers=pending_without_ack,
            total_updated_subscribers=updated_rows,
            summary=summary,
        )

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
    ) -> ControlPlaneSubscriberRecord:
        """Upsert one subscriber row after a replay/control-plane delivery heartbeat."""

        return await self._upsert(
            stream_name=stream_name,
            group_name=group_name,
            consumer_name=consumer_name,
            node_id=node_id,
            tenant_id=tenant_id,
            status="active",
            last_read_offset=offset,
            last_delivered_event_id=event_id,
        )

    async def observe_ack(
        self,
        *,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        node_id: str,
        tenant_id: str | None = None,
        event_id: str | None = None,
    ) -> ControlPlaneSubscriberRecord:
        """Update one subscriber row after a consumer-group acknowledgement."""

        return await self._upsert(
            stream_name=stream_name,
            group_name=group_name,
            consumer_name=consumer_name,
            node_id=node_id,
            tenant_id=tenant_id,
            status="active",
            last_acked_event_id=event_id,
        )

    async def observe_error(
        self,
        *,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        node_id: str,
        tenant_id: str | None = None,
        error: str,
    ) -> ControlPlaneSubscriberRecord:
        """Persist one subscriber error without discarding ownership metadata."""

        return await self._upsert(
            stream_name=stream_name,
            group_name=group_name,
            consumer_name=consumer_name,
            node_id=node_id,
            tenant_id=tenant_id,
            status="error",
            last_error=error,
        )

    async def claim_consumer(
        self,
        *,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        node_id: str,
        tenant_id: str | None = None,
        heartbeat_expiry_seconds: int = 120,
    ) -> ControlPlaneConsumerClaimResult:
        """Claim one durable consumer row with stale-heartbeat transfer and active-owner fencing."""

        max_claim_attempts = 3
        for _attempt in range(max_claim_attempts):
            now = datetime.now(UTC)
            stale_before = now - timedelta(seconds=max(1, heartbeat_expiry_seconds))
            async with self._db.session() as session:
                row = (
                    await session.execute(
                        select(ControlPlaneSubscriberORM)
                        .where(
                            ControlPlaneSubscriberORM.stream_name == stream_name,
                            ControlPlaneSubscriberORM.group_name == group_name,
                            ControlPlaneSubscriberORM.consumer_name == consumer_name,
                        )
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if row is None:
                    row = ControlPlaneSubscriberORM(
                        stream_name=stream_name,
                        group_name=group_name,
                        consumer_name=consumer_name,
                        node_id=node_id,
                        tenant_id=tenant_id,
                        status="active",
                        claimed_at=now,
                        last_heartbeat_at=now,
                        created_at=now,
                        updated_at=now,
                    )
                    session.add(row)
                    try:
                        await session.flush()
                        await session.commit()
                    except IntegrityError:
                        await session.rollback()
                        continue
                    return ControlPlaneConsumerClaimResult(
                        stream_name=stream_name,
                        group_name=group_name,
                        consumer_name=consumer_name,
                        node_id=node_id,
                        tenant_id=tenant_id,
                        outcome="claimed",
                        owner_node_id=node_id,
                        heartbeat_expired=False,
                    )

                heartbeat_expired = row.last_heartbeat_at < stale_before
                ownership_transfer_allowed = heartbeat_expired or row.status in {"stale", "error", "fenced"}
                if row.node_id != node_id and row.status == "active" and not ownership_transfer_allowed:
                    row.last_error = (
                        "consumer_fenced owner="
                        f"{row.node_id} contender={node_id}"
                    )
                    row.updated_at = now
                    await session.flush()
                    await session.commit()
                    return ControlPlaneConsumerClaimResult(
                        stream_name=stream_name,
                        group_name=group_name,
                        consumer_name=consumer_name,
                        node_id=node_id,
                        tenant_id=tenant_id,
                        outcome="fenced",
                        owner_node_id=row.node_id,
                        heartbeat_expired=False,
                        fence_reason="active_owner_not_expired",
                    )

                outcome: Literal["claimed", "transferred"] = "claimed"
                previous_owner = row.node_id
                if row.node_id != node_id:
                    outcome = "transferred"
                    row.claimed_at = now
                    row.last_error = f"ownership_transferred from={previous_owner} to={node_id}"
                row.node_id = node_id
                row.tenant_id = tenant_id
                row.status = "active"
                row.last_heartbeat_at = now
                row.updated_at = now
                await session.flush()
                await session.commit()
                return ControlPlaneConsumerClaimResult(
                    stream_name=stream_name,
                    group_name=group_name,
                    consumer_name=consumer_name,
                    node_id=node_id,
                    tenant_id=tenant_id,
                    outcome=outcome,
                    owner_node_id=node_id,
                    heartbeat_expired=heartbeat_expired,
                )

        raise RuntimeError("Unable to claim control-plane consumer after repeated contention")

    async def _upsert(
        self,
        *,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        node_id: str,
        tenant_id: str | None = None,
        status: str,
        last_read_offset: str | None = None,
        last_delivered_event_id: str | None = None,
        last_acked_event_id: str | None = None,
        last_error: str | None = None,
    ) -> ControlPlaneSubscriberRecord:
        now = datetime.now(UTC)
        async with self._db.session() as session:
            row = (
                await session.execute(
                    select(ControlPlaneSubscriberORM).where(
                        ControlPlaneSubscriberORM.stream_name == stream_name,
                        ControlPlaneSubscriberORM.group_name == group_name,
                        ControlPlaneSubscriberORM.consumer_name == consumer_name,
                    )
                )
            ).scalar_one_or_none()
            if row is None:
                row = ControlPlaneSubscriberORM(
                    stream_name=stream_name,
                    group_name=group_name,
                    consumer_name=consumer_name,
                    node_id=node_id,
                    tenant_id=tenant_id,
                    status=status,
                    last_read_offset=last_read_offset,
                    last_delivered_event_id=last_delivered_event_id,
                    last_acked_event_id=last_acked_event_id,
                    last_error=last_error,
                    claimed_at=now,
                    last_heartbeat_at=now,
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)
            else:
                row.node_id = node_id
                row.tenant_id = tenant_id
                row.status = status
                row.last_heartbeat_at = now
                row.updated_at = now
                if last_read_offset is not None:
                    row.last_read_offset = last_read_offset
                if last_delivered_event_id is not None:
                    row.last_delivered_event_id = last_delivered_event_id
                if last_acked_event_id is not None:
                    row.last_acked_event_id = last_acked_event_id
                if last_error is not None:
                    row.last_error = last_error
            await session.flush()
            record = _record_from_orm(row)
            await session.commit()
        return record


def _record_from_orm(
    row: ControlPlaneSubscriberORM,
    *,
    status: str | None = None,
) -> ControlPlaneSubscriberRecord:
    return ControlPlaneSubscriberRecord(
        stream_name=row.stream_name,
        group_name=row.group_name,
        consumer_name=row.consumer_name,
        node_id=row.node_id,
        tenant_id=row.tenant_id,
        status=status or row.status,
        last_read_offset=row.last_read_offset,
        last_delivered_event_id=row.last_delivered_event_id,
        last_acked_event_id=row.last_acked_event_id,
        last_error=row.last_error,
        claimed_at=row.claimed_at,
        last_heartbeat_at=row.last_heartbeat_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _has_unresolved_fence(record: ControlPlaneSubscriberRecord) -> bool:
    """Return whether one subscriber row still reflects an unresolved fence state."""

    if record.status == "fenced":
        return True
    if "consumer_fenced" not in str(record.last_error or ""):
        return False
    return bool(record.last_heartbeat_at <= record.updated_at)
