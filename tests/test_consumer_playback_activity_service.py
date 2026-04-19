from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from filmu_py.core.event_bus import EventBus
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.media import MediaService
from tests.db_seed import DbModelFactory, build_test_database_runtime, seed_models

pytest.importorskip("aiosqlite")


async def _build_runtime(tmp_path: Path) -> DatabaseRuntime:
    return await build_test_database_runtime(tmp_path, filename="consumer-playback.db")


@pytest.mark.asyncio
async def test_get_consumer_playback_activity_applies_focus_item_before_history_limit(
    tmp_path: Path,
) -> None:
    runtime = await _build_runtime(tmp_path)
    service = MediaService(runtime, EventBus())
    try:
        factory = DbModelFactory()
        tenant = factory.tenant()
        focus_item = factory.media_item(item_id="item-focus", title="Focus Item")
        other_item = factory.media_item(item_id="item-other", title="Other Item")
        await seed_models(runtime, tenant, focus_item, other_item)

        base_time = datetime(2026, 4, 19, 12, 0, tzinfo=UTC)
        await service.record_consumer_playback_activity(
            item_id=focus_item.id,
            tenant_id=tenant.id,
            actor_id="user-focus",
            actor_type="user",
            activity_kind="view",
            device_key="device-focus",
            device_label="Living Room",
            occurred_at=base_time,
        )

        for index in range(241):
            await service.record_consumer_playback_activity(
                item_id=other_item.id,
                tenant_id=tenant.id,
                actor_id="user-focus",
                actor_type="user",
                activity_kind="view",
                device_key=f"device-other-{index}",
                device_label=f"Other Device {index}",
                occurred_at=base_time + timedelta(minutes=index + 1),
            )

        snapshot = await service.get_consumer_playback_activity(
            tenant_id=tenant.id,
            actor_id="user-focus",
            actor_type="user",
            item_limit=4,
            device_limit=4,
            history_limit=240,
            focus_item_id=focus_item.id,
        )

        assert snapshot.total_item_count == 1
        assert snapshot.total_view_count == 1
        assert len(snapshot.items) == 1
        assert snapshot.items[0].item_id == focus_item.id
        assert snapshot.items[0].view_count == 1
    finally:
        await runtime.dispose()
