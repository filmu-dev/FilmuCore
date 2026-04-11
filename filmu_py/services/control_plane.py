"""Durable control-plane subscriber ownership and resume-offset state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

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
