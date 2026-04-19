from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from filmu_py.db.base import Base
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
from filmu_py.db.runtime import DatabaseRuntime


async def build_test_database_runtime(
    tmp_path: Path,
    *,
    filename: str = "test.db",
) -> DatabaseRuntime:
    """Create one sqlite-backed runtime with all ORM tables ready for tests."""

    runtime = DatabaseRuntime(f"sqlite+aiosqlite:///{tmp_path / filename}")
    async with runtime.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return runtime


async def seed_models(runtime: DatabaseRuntime, *models: object) -> tuple[object, ...]:
    """Persist one batch of ORM rows and return them unchanged for fluent tests."""

    persisted = tuple(model for model in models if model is not None)
    async with runtime.session() as session:
        session.add_all(list(persisted))
        await session.commit()
    return persisted


@dataclass(frozen=True, slots=True)
class MediaItemSeedBundle:
    """One commonly seeded media-item graph for service and route tests."""

    tenant: TenantORM
    item: MediaItemORM
    item_request: ItemRequestORM | None = None
    playback_attachment: PlaybackAttachmentORM | None = None
    media_entry: MediaEntryORM | None = None
    active_stream: ActiveStreamORM | None = None
    workflow_checkpoint: ItemWorkflowCheckpointORM | None = None

    def models(self) -> tuple[object, ...]:
        return tuple(
            model
            for model in (
                self.tenant,
                self.item,
                self.item_request,
                self.playback_attachment,
                self.media_entry,
                self.active_stream,
                self.workflow_checkpoint,
            )
            if model is not None
        )


@dataclass(slots=True)
class DbModelFactory:
    """Build consistent ORM rows for shared database tests."""

    default_tenant_id: str = "tenant-main"
    _timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def _stamp(self, *, seconds: int = 0) -> datetime:
        return self._timestamp

    @staticmethod
    def _id(prefix: str, value: str | None = None) -> str:
        suffix = value or str(uuid4())
        return f"{prefix}-{suffix}"

    def tenant(
        self,
        *,
        id: str | None = None,
        slug: str | None = None,
        display_name: str | None = None,
        kind: str = "tenant",
        status: str = "active",
    ) -> TenantORM:
        tenant_id = id or self.default_tenant_id
        return TenantORM(
            id=tenant_id,
            slug=slug or tenant_id,
            display_name=display_name or tenant_id.replace("-", " ").title(),
            kind=kind,
            status=status,
            created_at=self._stamp(),
            updated_at=self._stamp(),
        )

    def principal(
        self,
        *,
        tenant_id: str | None = None,
        id: str | None = None,
        principal_key: str | None = None,
        principal_type: str = "service",
        authentication_mode: str = "api_key",
        display_name: str | None = None,
        roles: list[str] | None = None,
        scopes: list[str] | None = None,
        status: str = "active",
    ) -> PrincipalORM:
        resolved_tenant_id = tenant_id or self.default_tenant_id
        resolved_key = principal_key or f"{resolved_tenant_id}:operator"
        return PrincipalORM(
            id=id or self._id("principal"),
            tenant_id=resolved_tenant_id,
            principal_key=resolved_key,
            principal_type=principal_type,
            authentication_mode=authentication_mode,
            display_name=display_name,
            roles=list(roles or []),
            scopes=list(scopes or []),
            status=status,
            created_at=self._stamp(),
            updated_at=self._stamp(),
            last_authenticated_at=None,
        )

    def service_account(
        self,
        *,
        principal_id: str,
        id: str | None = None,
        api_key_id: str = "primary-test",
        status: str = "active",
        description: str | None = None,
    ) -> ServiceAccountORM:
        return ServiceAccountORM(
            id=id or self._id("service-account"),
            principal_id=principal_id,
            api_key_id=api_key_id,
            status=status,
            description=description,
            last_authenticated_at=None,
            created_at=self._stamp(),
            updated_at=self._stamp(),
        )

    def media_item(
        self,
        *,
        item_id: str,
        tenant_id: str | None = None,
        external_ref: str | None = None,
        title: str = "Example Item",
        state: str = "downloaded",
        attributes: dict[str, object] | None = None,
        recovery_attempt_count: int = 0,
        next_retry_at: datetime | None = None,
    ) -> MediaItemORM:
        return MediaItemORM(
            id=item_id,
            tenant_id=tenant_id or self.default_tenant_id,
            external_ref=external_ref or f"tmdb:{item_id}",
            title=title,
            state=state,
            recovery_attempt_count=recovery_attempt_count,
            next_retry_at=next_retry_at,
            attributes=dict(attributes or {"item_type": "movie"}),
            created_at=self._stamp(),
            updated_at=self._stamp(),
        )

    def item_request(
        self,
        *,
        tenant_id: str | None = None,
        id: str | None = None,
        external_ref: str,
        media_item_id: str | None = None,
        media_type: str = "movie",
        requested_title: str = "Example Item",
        requested_seasons: list[int] | None = None,
        requested_episodes: dict[str, list[int]] | None = None,
        is_partial: bool = False,
        request_source: str = "api",
        request_count: int = 1,
    ) -> ItemRequestORM:
        return ItemRequestORM(
            id=id or self._id("item-request"),
            tenant_id=tenant_id or self.default_tenant_id,
            external_ref=external_ref,
            media_item_id=media_item_id,
            media_type=media_type,
            requested_title=requested_title,
            requested_seasons=requested_seasons,
            requested_episodes=requested_episodes,
            is_partial=is_partial,
            request_source=request_source,
            request_count=request_count,
            first_requested_at=self._stamp(),
            last_requested_at=self._stamp(),
            created_at=self._stamp(),
            updated_at=self._stamp(),
        )

    def playback_attachment(
        self,
        *,
        item_id: str,
        id: str | None = None,
        kind: str = "remote-direct",
        locator: str | None = None,
        source_key: str | None = None,
        provider: str | None = "realdebrid",
        provider_download_id: str | None = None,
        provider_file_id: str | None = None,
        provider_file_path: str | None = None,
        original_filename: str | None = None,
        file_size: int | None = 1024,
        local_path: str | None = None,
        restricted_url: str | None = None,
        unrestricted_url: str | None = None,
        refresh_state: str = "ready",
    ) -> PlaybackAttachmentORM:
        resolved_id = id or self._id("playback-attachment", item_id)
        resolved_locator = locator or unrestricted_url or restricted_url or f"attachment:{item_id}"
        return PlaybackAttachmentORM(
            id=resolved_id,
            item_id=item_id,
            kind=kind,
            locator=resolved_locator,
            source_key=source_key or f"playback-attachment:{resolved_id}",
            provider=provider,
            provider_download_id=provider_download_id,
            provider_file_id=provider_file_id,
            provider_file_path=provider_file_path,
            original_filename=original_filename,
            file_size=file_size,
            local_path=local_path,
            restricted_url=restricted_url,
            unrestricted_url=unrestricted_url,
            is_preferred=True,
            preference_rank=0,
            refresh_state=refresh_state,
            expires_at=None,
            last_refreshed_at=None,
            last_refresh_error=None,
            created_at=self._stamp(),
            updated_at=self._stamp(),
        )

    def media_entry(
        self,
        *,
        item_id: str,
        id: str | None = None,
        source_attachment_id: str | None = None,
        entry_type: str = "media",
        kind: str = "remote-direct",
        original_filename: str | None = None,
        local_path: str | None = None,
        download_url: str | None = None,
        unrestricted_url: str | None = None,
        provider: str | None = "realdebrid",
        provider_download_id: str | None = None,
        provider_file_id: str | None = None,
        provider_file_path: str | None = None,
        size_bytes: int | None = 1024,
        refresh_state: str = "ready",
    ) -> MediaEntryORM:
        return MediaEntryORM(
            id=id or self._id("media-entry", item_id),
            item_id=item_id,
            source_attachment_id=source_attachment_id,
            entry_type=entry_type,
            kind=kind,
            original_filename=original_filename,
            local_path=local_path,
            download_url=download_url,
            unrestricted_url=unrestricted_url,
            provider=provider,
            provider_download_id=provider_download_id,
            provider_file_id=provider_file_id,
            provider_file_path=provider_file_path,
            size_bytes=size_bytes,
            refresh_state=refresh_state,
            expires_at=None,
            last_refreshed_at=None,
            last_refresh_error=None,
            created_at=self._stamp(),
            updated_at=self._stamp(),
        )

    def active_stream(
        self,
        *,
        item_id: str,
        media_entry_id: str,
        id: str | None = None,
        role: str = "direct",
    ) -> ActiveStreamORM:
        return ActiveStreamORM(
            id=id or self._id("active-stream", f"{item_id}-{role}"),
            item_id=item_id,
            media_entry_id=media_entry_id,
            role=role,
            created_at=self._stamp(),
            updated_at=self._stamp(),
        )

    def workflow_checkpoint(
        self,
        *,
        item_id: str,
        id: str | None = None,
        workflow_name: str = "item_pipeline",
        stage_name: str = "debrid_item",
        resume_stage: str = "finalize",
        status: str = "pending",
        item_request_id: str | None = None,
        selected_stream_id: str | None = None,
        provider: str | None = None,
        provider_download_id: str | None = None,
        checkpoint_payload: dict[str, object] | None = None,
        compensation_payload: dict[str, object] | None = None,
        last_error: str | None = None,
    ) -> ItemWorkflowCheckpointORM:
        return ItemWorkflowCheckpointORM(
            id=id or self._id("workflow-checkpoint", item_id),
            item_id=item_id,
            workflow_name=workflow_name,
            stage_name=stage_name,
            resume_stage=resume_stage,
            status=status,
            item_request_id=item_request_id,
            selected_stream_id=selected_stream_id,
            provider=provider,
            provider_download_id=provider_download_id,
            checkpoint_payload=dict(checkpoint_payload or {}),
            compensation_payload=dict(compensation_payload or {}),
            last_error=last_error,
            created_at=self._stamp(),
            updated_at=self._stamp(),
        )

    def control_plane_subscriber(
        self,
        *,
        stream_name: str = "filmu:events",
        group_name: str = "filmu-api",
        consumer_name: str = "consumer-1",
        node_id: str = "node-a",
        tenant_id: str | None = None,
        status: str = "active",
        last_read_offset: str | None = None,
        last_delivered_event_id: str | None = None,
        last_acked_event_id: str | None = None,
        last_error: str | None = None,
        claimed_at: datetime | None = None,
        last_heartbeat_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> ControlPlaneSubscriberORM:
        stamp = self._stamp()
        return ControlPlaneSubscriberORM(
            id=self._id("control-plane-subscriber", f"{group_name}-{consumer_name}"),
            stream_name=stream_name,
            group_name=group_name,
            consumer_name=consumer_name,
            node_id=node_id,
            tenant_id=tenant_id or self.default_tenant_id,
            status=status,
            last_read_offset=last_read_offset,
            last_delivered_event_id=last_delivered_event_id,
            last_acked_event_id=last_acked_event_id,
            last_error=last_error,
            claimed_at=claimed_at or stamp,
            last_heartbeat_at=last_heartbeat_at or stamp,
            created_at=stamp,
            updated_at=updated_at or stamp,
        )

    def media_item_bundle(
        self,
        *,
        item_id: str,
        tenant_id: str | None = None,
        title: str = "Example Item",
        state: str = "downloaded",
        item_type: str = "movie",
        include_request: bool = False,
        include_attachment: bool = False,
        include_media_entry: bool = False,
        include_active_stream: bool = False,
        include_workflow_checkpoint: bool = False,
    ) -> MediaItemSeedBundle:
        resolved_tenant_id = tenant_id or self.default_tenant_id
        tenant = self.tenant(id=resolved_tenant_id)
        item = self.media_item(
            item_id=item_id,
            tenant_id=resolved_tenant_id,
            title=title,
            state=state,
            attributes={"item_type": item_type},
        )
        item_request = (
            self.item_request(
                tenant_id=resolved_tenant_id,
                external_ref=item.external_ref,
                media_item_id=item.id,
                media_type=item_type,
                requested_title=title,
            )
            if include_request
            else None
        )
        playback_attachment = None
        if include_attachment or include_media_entry:
            playback_attachment = self.playback_attachment(
                item_id=item.id,
                locator=f"https://cdn.example.com/{item.id}",
                restricted_url=f"https://api.example.com/restricted/{item.id}",
                unrestricted_url=f"https://cdn.example.com/{item.id}",
                provider_download_id=f"download-{item.id}",
                provider_file_id=f"provider-file-{item.id}",
                provider_file_path=f"Library/{title}.mkv",
                original_filename=f"{title}.mkv",
                file_size=7777,
            )
        media_entry = None
        if include_media_entry:
            media_entry = self.media_entry(
                item_id=item.id,
                source_attachment_id=(
                    playback_attachment.id if playback_attachment is not None else None
                ),
                original_filename=f"{title}.mkv",
                download_url=f"https://api.example.com/restricted/{item.id}",
                unrestricted_url=f"https://cdn.example.com/{item.id}",
                provider_download_id=f"download-{item.id}",
                provider_file_id=f"provider-file-{item.id}",
                provider_file_path=f"Library/{title}.mkv",
                size_bytes=7777,
            )
        active_stream = None
        if include_active_stream and media_entry is not None:
            active_stream = self.active_stream(
                item_id=item.id,
                media_entry_id=media_entry.id,
            )
        workflow_checkpoint = (
            self.workflow_checkpoint(
                item_id=item.id,
                item_request_id=item_request.id if item_request is not None else None,
                selected_stream_id=active_stream.id if active_stream is not None else None,
                provider=media_entry.provider if media_entry is not None else "realdebrid",
                provider_download_id=(
                    media_entry.provider_download_id if media_entry is not None else None
                ),
                checkpoint_payload={"seeded": True},
                compensation_payload={},
            )
            if include_workflow_checkpoint
            else None
        )
        return MediaItemSeedBundle(
            tenant=tenant,
            item=item,
            item_request=item_request,
            playback_attachment=playback_attachment,
            media_entry=media_entry,
            active_stream=active_stream,
            workflow_checkpoint=workflow_checkpoint,
        )
