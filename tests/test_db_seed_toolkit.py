from __future__ import annotations

from pathlib import Path

import pytest

from filmu_py.db.models import (
    ActiveStreamORM,
    ControlPlaneSubscriberORM,
    ItemRequestORM,
    ItemWorkflowCheckpointORM,
    MediaEntryORM,
    MediaItemORM,
    PlaybackAttachmentORM,
    PrincipalORM,
    ServiceAccountORM,
    TenantORM,
)
from tests.db_seed import DbModelFactory, build_test_database_runtime, seed_models

pytest.importorskip("aiosqlite")


@pytest.mark.asyncio
async def test_db_seed_toolkit_persists_media_item_bundle(
    tmp_path: Path,
    db_model_factory: DbModelFactory,
) -> None:
    runtime = await build_test_database_runtime(tmp_path, filename="seed-toolkit.db")
    try:
        bundle = db_model_factory.media_item_bundle(
            item_id="item-seeded",
            title="Seeded Item",
            include_request=True,
            include_attachment=True,
            include_media_entry=True,
            include_active_stream=True,
            include_workflow_checkpoint=True,
        )
        await seed_models(runtime, *bundle.models())

        async with runtime.session() as session:
            tenant = await session.get(TenantORM, bundle.tenant.id)
            item = await session.get(MediaItemORM, bundle.item.id)
            item_request = await session.get(ItemRequestORM, bundle.item_request.id)
            attachment = await session.get(
                PlaybackAttachmentORM, bundle.playback_attachment.id
            )
            media_entry = await session.get(MediaEntryORM, bundle.media_entry.id)
            active_stream = await session.get(ActiveStreamORM, bundle.active_stream.id)
            checkpoint = await session.get(
                ItemWorkflowCheckpointORM, bundle.workflow_checkpoint.id
            )

        assert tenant is not None
        assert item is not None
        assert item_request is not None
        assert attachment is not None
        assert media_entry is not None
        assert active_stream is not None
        assert checkpoint is not None
        assert item.tenant_id == bundle.tenant.id
        assert item_request.media_item_id == bundle.item.id
        assert media_entry.source_attachment_id == bundle.playback_attachment.id
        assert active_stream.media_entry_id == bundle.media_entry.id
        assert checkpoint.item_id == bundle.item.id
    finally:
        await runtime.dispose()


@pytest.mark.asyncio
async def test_db_seed_toolkit_builds_identity_and_control_plane_rows(
    tmp_path: Path,
    db_model_factory: DbModelFactory,
) -> None:
    runtime = await build_test_database_runtime(tmp_path, filename="seed-toolkit-identity.db")
    try:
        tenant = db_model_factory.tenant(id="tenant-ops")
        principal = db_model_factory.principal(
            tenant_id=tenant.id,
            principal_key="tenant-ops:svc-control",
            scopes=["backend:admin"],
        )
        service_account = db_model_factory.service_account(
            principal_id=principal.id,
            api_key_id="svc-control-primary",
        )
        subscriber = db_model_factory.control_plane_subscriber(
            group_name="ops-api",
            consumer_name="ops-worker",
            node_id="node-ops-a",
            tenant_id=tenant.id,
            last_read_offset="10-0",
            last_delivered_event_id="12-0",
            last_acked_event_id="11-0",
        )
        await seed_models(runtime, tenant, principal, service_account, subscriber)

        async with runtime.session() as session:
            loaded_principal = await session.get(PrincipalORM, principal.id)
            loaded_service_account = await session.get(ServiceAccountORM, service_account.id)
            loaded_subscriber = await session.get(ControlPlaneSubscriberORM, subscriber.id)

        assert loaded_principal is not None
        assert loaded_service_account is not None
        assert loaded_subscriber is not None
        assert loaded_principal.tenant_id == tenant.id
        assert loaded_service_account.principal_id == principal.id
        assert loaded_subscriber.tenant_id == tenant.id
        assert loaded_subscriber.last_delivered_event_id == "12-0"
        assert loaded_subscriber.last_acked_event_id == "11-0"
    finally:
        await runtime.dispose()
