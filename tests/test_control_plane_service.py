"""Control-plane durable ownership/fencing tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from filmu_py.db.base import Base
from filmu_py.db.models import ControlPlaneSubscriberORM
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.control_plane import ControlPlaneService

pytest.importorskip("aiosqlite")


async def _build_runtime(tmp_path: Path) -> DatabaseRuntime:
    runtime = DatabaseRuntime(f"sqlite+aiosqlite:///{tmp_path / 'control-plane.db'}")
    async with runtime.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return runtime


@pytest.mark.asyncio
async def test_claim_consumer_fences_active_owner(tmp_path: Path) -> None:
    runtime = await _build_runtime(tmp_path)
    service = ControlPlaneService(runtime)
    try:
        first = await service.claim_consumer(
            stream_name="filmu:events",
            group_name="filmu-api",
            consumer_name="consumer-1",
            node_id="node-a",
            tenant_id="tenant-main",
            heartbeat_expiry_seconds=120,
        )
        second = await service.claim_consumer(
            stream_name="filmu:events",
            group_name="filmu-api",
            consumer_name="consumer-1",
            node_id="node-b",
            tenant_id="tenant-main",
            heartbeat_expiry_seconds=120,
        )

        assert first.outcome == "claimed"
        assert second.outcome == "fenced"
        assert second.owner_node_id == "node-a"

        subscribers = await service.list_subscribers(active_within_seconds=120)
        assert len(subscribers) == 1
        assert subscribers[0].node_id == "node-a"
        assert subscribers[0].status == "active"
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_claim_consumer_transfers_ownership_after_heartbeat_expiry(tmp_path: Path) -> None:
    runtime = await _build_runtime(tmp_path)
    service = ControlPlaneService(runtime)
    try:
        await service.claim_consumer(
            stream_name="filmu:events",
            group_name="filmu-api",
            consumer_name="consumer-1",
            node_id="node-a",
            tenant_id="tenant-main",
            heartbeat_expiry_seconds=120,
        )

        stale_time = datetime.now(UTC) - timedelta(minutes=10)
        async with runtime.session() as session:
            row = (
                await session.execute(
                    select(ControlPlaneSubscriberORM).where(
                        ControlPlaneSubscriberORM.stream_name == "filmu:events",
                        ControlPlaneSubscriberORM.group_name == "filmu-api",
                        ControlPlaneSubscriberORM.consumer_name == "consumer-1",
                    )
                )
            ).scalar_one()
            row.last_heartbeat_at = stale_time
            row.updated_at = stale_time
            await session.flush()
            await session.commit()

        claim = await service.claim_consumer(
            stream_name="filmu:events",
            group_name="filmu-api",
            consumer_name="consumer-1",
            node_id="node-b",
            tenant_id="tenant-main",
            heartbeat_expiry_seconds=30,
        )

        assert claim.outcome == "transferred"
        assert claim.owner_node_id == "node-b"
        assert claim.heartbeat_expired is True

        subscribers = await service.list_subscribers(active_within_seconds=120)
        assert len(subscribers) == 1
        assert subscribers[0].node_id == "node-b"
        assert subscribers[0].status == "active"
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_control_plane_summary_reports_ack_backlog_and_stale_subscribers(
    tmp_path: Path,
) -> None:
    runtime = await _build_runtime(tmp_path)
    service = ControlPlaneService(runtime)
    try:
        await service.observe_delivery(
            stream_name="filmu:events",
            group_name="filmu-api",
            consumer_name="consumer-1",
            node_id="node-a",
            tenant_id="tenant-main",
            offset=">",
            event_id="11-0",
        )
        stale_time = datetime.now(UTC) - timedelta(minutes=15)
        async with runtime.session() as session:
            row = (
                await session.execute(
                    select(ControlPlaneSubscriberORM).where(
                        ControlPlaneSubscriberORM.stream_name == "filmu:events",
                        ControlPlaneSubscriberORM.group_name == "filmu-api",
                        ControlPlaneSubscriberORM.consumer_name == "consumer-1",
                    )
                )
            ).scalar_one()
            row.last_heartbeat_at = stale_time
            row.updated_at = stale_time
            await session.flush()
            await session.commit()

        summary = await service.summarize_subscribers(active_within_seconds=30)

        assert summary.total_subscribers == 1
        assert summary.active_subscribers == 0
        assert summary.stale_subscribers == 1
        assert summary.ack_pending_subscribers == 1
        assert summary.required_actions == (
            "recover_stale_control_plane_subscribers",
            "drain_control_plane_ack_backlog",
        )
        assert summary.oldest_heartbeat_age_seconds is not None
        assert summary.oldest_heartbeat_age_seconds >= 30
    finally:
        await runtime.dispose()
