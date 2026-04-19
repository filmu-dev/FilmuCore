from __future__ import annotations

from pathlib import Path

import pytest

from filmu_py.core.event_bus import EventBus
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.media import (
    MediaService,
    WorkflowCheckpointStatus,
    WorkflowResumeStage,
)
from tests.db_seed import DbModelFactory, build_test_database_runtime, seed_models

pytest.importorskip("aiosqlite")


async def _build_runtime(tmp_path: Path) -> DatabaseRuntime:
    return await build_test_database_runtime(tmp_path, filename="workflow-checkpoint.db")


@pytest.mark.asyncio
async def test_media_service_persists_and_updates_workflow_checkpoint(tmp_path: Path) -> None:
    runtime = await _build_runtime(tmp_path)
    service = MediaService(runtime, EventBus())
    try:
        factory = DbModelFactory()
        bundle = factory.media_item_bundle(
            item_id="item-workflow",
            title="Workflow Item",
        )
        await seed_models(runtime, *bundle.models())

        first = await service.persist_workflow_checkpoint(
            media_item_id="item-workflow",
            stage_name="debrid_item",
            resume_stage=WorkflowResumeStage.FINALIZE,
            status=WorkflowCheckpointStatus.PENDING,
            item_request_id="request-item-workflow",
            selected_stream_id="stream-1",
            provider="realdebrid",
            provider_download_id="provider-torrent-1",
            checkpoint_payload={"persisted_media_entry_count": 1},
            compensation_payload={"selected_stream_id": "stream-1"},
        )
        second = await service.persist_workflow_checkpoint(
            media_item_id="item-workflow",
            stage_name="finalize_item",
            resume_stage=WorkflowResumeStage.NONE,
            status=WorkflowCheckpointStatus.COMPLETED,
            item_request_id="request-item-workflow",
            selected_stream_id="stream-1",
            provider="realdebrid",
            provider_download_id="provider-torrent-1",
            checkpoint_payload={"resulting_state": "completed"},
            compensation_payload={},
        )
        loaded = await service.get_workflow_checkpoint(media_item_id="item-workflow")

        assert first.resume_stage is WorkflowResumeStage.FINALIZE
        assert first.status is WorkflowCheckpointStatus.PENDING
        assert second.stage_name == "finalize_item"
        assert second.resume_stage is WorkflowResumeStage.NONE
        assert second.status is WorkflowCheckpointStatus.COMPLETED
        assert loaded == second
    finally:
        await runtime.dispose()
