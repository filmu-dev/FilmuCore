"""Mount-worker boundary contracts for future VFS query planning.

This module intentionally stops at lifecycle-supplier and persistence-query-contract design.
It does not implement FUSE operations, mount loops, or database query execution.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from filmu_py.api.playback_resolution import PlaybackAttachment
from filmu_py.db.models import MediaEntryORM, MediaItemORM
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.playback import (
    DirectFileLinkLifecycleSnapshot,
    DirectFileLinkProviderFamily,
    PlaybackResolutionSnapshot,
)

MountPlaybackRole = Literal["direct", "hls"]
MountMediaEntryQueryStatus = Literal["queryable", "blocked"]
MountMediaEntryQueryBlockedReason = Literal[
    "no_attachment",
    "missing_lifecycle",
    "metadata_only",
    "no_persisted_identity",
]
MountMediaEntryQueryStrategy = Literal[
    "by-media-entry-id",
    "by-source-attachment-id",
    "by-provider-file-id",
    "by-provider-file-path",
    "by-provider-download-id-and-filename",
    "by-provider-download-id-and-provider-file-path",
    "by-provider-download-id-and-file-size",
]


@runtime_checkable
class MountPlaybackSnapshotSupplier(Protocol):
    """Supplier that can provide the internal resolved playback snapshot for one persisted item."""

    def build_resolution_snapshot(self, item: MediaItemORM) -> PlaybackResolutionSnapshot: ...


@dataclass(frozen=True)
class MountMediaEntryQueryStep:
    """One explicit persistence-query strategy the future mount worker may execute."""

    strategy: MountMediaEntryQueryStrategy
    provider: str | None = None
    media_entry_id: str | None = None
    source_attachment_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    provider_download_id: str | None = None
    original_filename: str | None = None
    size_bytes: int | None = None


@dataclass(frozen=True)
class MountMediaEntryQueryContract:
    """Explicit query contract for resolving one mount-facing media entry from persisted identity."""

    item_id: str
    role: MountPlaybackRole
    status: MountMediaEntryQueryStatus
    provider_family: DirectFileLinkProviderFamily = "none"
    provider: str | None = None
    source_key: str | None = None
    restricted_fallback: bool = False
    blocked_reason: MountMediaEntryQueryBlockedReason | None = None
    resolved_locator: str | None = None
    steps: tuple[MountMediaEntryQueryStep, ...] = ()


@dataclass(frozen=True)
class MountMediaEntryQueryResult:
    """Result shape for a future mount-worker media-entry query execution."""

    contract: MountMediaEntryQueryContract
    media_entry_id: str | None = None
    matched_strategy: MountMediaEntryQueryStrategy | None = None


@runtime_checkable
class MountMediaEntryQueryExecutor(Protocol):
    """Executor contract for future persisted media-entry lookup from a mount-worker query plan."""

    async def resolve_media_entry(
        self,
        contract: MountMediaEntryQueryContract,
    ) -> MountMediaEntryQueryResult: ...


@dataclass(slots=True)
class PersistedMountMediaEntryQueryExecutor:
    """Concrete persisted media-entry query executor for future mount-worker consumers."""

    db: DatabaseRuntime

    async def resolve_media_entry(
        self,
        contract: MountMediaEntryQueryContract,
    ) -> MountMediaEntryQueryResult:
        """Resolve one query contract against the existing persisted media-entry + active-stream model."""

        if contract.status != "queryable":
            return MountMediaEntryQueryResult(contract=contract)

        item = await self._load_item(contract.item_id)
        if item is None:
            return MountMediaEntryQueryResult(contract=contract)
        assert item is not None

        active_media_entry_id = self._get_active_media_entry_id(item, role=contract.role)
        for step in contract.steps:
            matches = self._matching_entries(item.media_entries, step)
            if len(matches) == 1:
                return MountMediaEntryQueryResult(
                    contract=contract,
                    media_entry_id=matches[0].id,
                    matched_strategy=step.strategy,
                )
            if len(matches) > 1 and active_media_entry_id is not None:
                for entry in matches:
                    if entry.id == active_media_entry_id:
                        return MountMediaEntryQueryResult(
                            contract=contract,
                            media_entry_id=entry.id,
                            matched_strategy=step.strategy,
                        )

        return MountMediaEntryQueryResult(contract=contract)

    async def _load_item(self, item_id: str) -> MediaItemORM | None:
        async with self.db.session() as session:
            result = await session.execute(
                select(MediaItemORM)
                .options(
                    selectinload(MediaItemORM.media_entries),
                    selectinload(MediaItemORM.active_streams),
                )
                .where(MediaItemORM.id == item_id)
            )
            return result.scalars().first()

    @staticmethod
    def _get_active_media_entry_id(item: MediaItemORM, *, role: MountPlaybackRole) -> str | None:
        for active_stream in item.active_streams:
            if active_stream.role == role:
                return active_stream.media_entry_id
        return None

    @staticmethod
    def _providers_are_compatible(left: str | None, right: str | None) -> bool:
        return left is None or right is None or left == right

    @staticmethod
    def _matching_text(left: str | None, right: str | None) -> bool:
        if left is None or right is None:
            return False
        assert left is not None
        assert right is not None
        left_text = left.strip()
        right_text = right.strip()
        return left_text != "" and left_text == right_text

    @staticmethod
    def _matching_size(left: int | None, right: int | None) -> bool:
        return left is not None and right is not None and left == right

    @classmethod
    def _matching_entries(
        cls,
        entries: Sequence[MediaEntryORM],
        step: MountMediaEntryQueryStep,
    ) -> list[MediaEntryORM]:
        matches: list[MediaEntryORM] = []
        for entry in entries:
            if not cls._providers_are_compatible(entry.provider, step.provider):
                continue
            if step.strategy == "by-media-entry-id":
                if entry.id == step.media_entry_id:
                    matches.append(entry)
                continue
            if step.strategy == "by-source-attachment-id":
                if entry.source_attachment_id == step.source_attachment_id:
                    matches.append(entry)
                continue
            if step.strategy == "by-provider-file-id":
                if cls._matching_text(entry.provider_file_id, step.provider_file_id):
                    matches.append(entry)
                continue
            if step.strategy == "by-provider-file-path":
                if cls._matching_text(entry.provider_file_path, step.provider_file_path):
                    matches.append(entry)
                continue
            if step.strategy == "by-provider-download-id-and-filename":
                if cls._matching_text(
                    entry.provider_download_id, step.provider_download_id
                ) and cls._matching_text(
                    entry.original_filename,
                    step.original_filename,
                ):
                    matches.append(entry)
                continue
            if step.strategy == "by-provider-download-id-and-provider-file-path":
                if cls._matching_text(
                    entry.provider_download_id, step.provider_download_id
                ) and cls._matching_text(
                    entry.provider_file_path,
                    step.provider_file_path,
                ):
                    matches.append(entry)
                continue
            if step.strategy == "by-provider-download-id-and-file-size":
                if cls._matching_text(
                    entry.provider_download_id, step.provider_download_id
                ) and cls._matching_size(
                    entry.size_bytes,
                    step.size_bytes,
                ):
                    matches.append(entry)
                continue
        return matches


def _select_snapshot_attachment_lifecycle(
    snapshot: PlaybackResolutionSnapshot,
    *,
    role: MountPlaybackRole,
) -> tuple[PlaybackAttachment | None, DirectFileLinkLifecycleSnapshot | None]:
    if role == "direct":
        return snapshot.direct, snapshot.direct_lifecycle
    return snapshot.hls, snapshot.hls_lifecycle


def _build_mount_media_entry_query_steps(
    attachment: PlaybackAttachment,
    lifecycle: DirectFileLinkLifecycleSnapshot,
) -> tuple[MountMediaEntryQueryStep, ...]:
    steps: list[MountMediaEntryQueryStep] = []

    if lifecycle.owner_kind == "media-entry" and lifecycle.owner_id is not None:
        steps.append(
            MountMediaEntryQueryStep(
                strategy="by-media-entry-id",
                provider=attachment.provider,
                media_entry_id=lifecycle.owner_id,
            )
        )

    if lifecycle.source_attachment_id is not None:
        steps.append(
            MountMediaEntryQueryStep(
                strategy="by-source-attachment-id",
                provider=attachment.provider,
                source_attachment_id=lifecycle.source_attachment_id,
            )
        )

    if attachment.provider_file_id is not None:
        steps.append(
            MountMediaEntryQueryStep(
                strategy="by-provider-file-id",
                provider=attachment.provider,
                provider_file_id=attachment.provider_file_id,
            )
        )

    if attachment.provider_file_path is not None:
        steps.append(
            MountMediaEntryQueryStep(
                strategy="by-provider-file-path",
                provider=attachment.provider,
                provider_file_path=attachment.provider_file_path,
            )
        )

    if attachment.provider_download_id is not None and attachment.original_filename is not None:
        steps.append(
            MountMediaEntryQueryStep(
                strategy="by-provider-download-id-and-filename",
                provider=attachment.provider,
                provider_download_id=attachment.provider_download_id,
                original_filename=attachment.original_filename,
            )
        )

    if attachment.provider_download_id is not None and attachment.provider_file_path is not None:
        steps.append(
            MountMediaEntryQueryStep(
                strategy="by-provider-download-id-and-provider-file-path",
                provider=attachment.provider,
                provider_download_id=attachment.provider_download_id,
                provider_file_path=attachment.provider_file_path,
            )
        )

    if attachment.provider_download_id is not None and attachment.file_size is not None:
        steps.append(
            MountMediaEntryQueryStep(
                strategy="by-provider-download-id-and-file-size",
                provider=attachment.provider,
                provider_download_id=attachment.provider_download_id,
                size_bytes=attachment.file_size,
            )
        )

    return tuple(steps)


def build_mount_media_entry_query_contract_from_snapshot(
    item: MediaItemORM,
    snapshot: PlaybackResolutionSnapshot,
    *,
    role: MountPlaybackRole,
) -> MountMediaEntryQueryContract:
    """Build the explicit persisted media-entry query contract for one future mount-worker open path."""

    attachment, lifecycle = _select_snapshot_attachment_lifecycle(snapshot, role=role)
    if attachment is None:
        return MountMediaEntryQueryContract(
            item_id=item.id,
            role=role,
            status="blocked",
            blocked_reason="no_attachment",
        )

    if lifecycle is None:
        return MountMediaEntryQueryContract(
            item_id=item.id,
            role=role,
            status="blocked",
            provider=attachment.provider,
            source_key=attachment.source_key,
            resolved_locator=attachment.locator,
            blocked_reason="missing_lifecycle",
        )

    if lifecycle.owner_kind == "metadata":
        return MountMediaEntryQueryContract(
            item_id=item.id,
            role=role,
            status="blocked",
            provider_family=lifecycle.provider_family,
            provider=attachment.provider,
            source_key=attachment.source_key,
            restricted_fallback=lifecycle.restricted_fallback,
            blocked_reason="metadata_only",
            resolved_locator=attachment.locator,
        )

    steps = _build_mount_media_entry_query_steps(attachment, lifecycle)
    if not steps:
        return MountMediaEntryQueryContract(
            item_id=item.id,
            role=role,
            status="blocked",
            provider_family=lifecycle.provider_family,
            provider=attachment.provider,
            source_key=attachment.source_key,
            restricted_fallback=lifecycle.restricted_fallback,
            blocked_reason="no_persisted_identity",
            resolved_locator=attachment.locator,
        )

    return MountMediaEntryQueryContract(
        item_id=item.id,
        role=role,
        status="queryable",
        provider_family=lifecycle.provider_family,
        provider=attachment.provider,
        source_key=attachment.source_key,
        restricted_fallback=lifecycle.restricted_fallback,
        resolved_locator=attachment.locator,
        steps=steps,
    )


def build_mount_media_entry_query_contract(
    item: MediaItemORM,
    *,
    role: MountPlaybackRole,
    playback_snapshot_supplier: MountPlaybackSnapshotSupplier,
) -> MountMediaEntryQueryContract:
    """Build the explicit mount-worker query contract using the supplied playback lifecycle snapshot."""

    snapshot = playback_snapshot_supplier.build_resolution_snapshot(item)
    return build_mount_media_entry_query_contract_from_snapshot(item, snapshot, role=role)
