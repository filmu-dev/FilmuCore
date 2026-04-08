"""Proto-first FilmuVFS catalog supplier for the future Rust sidecar.

This module defines the Python-side projection boundary for the future
[`WatchCatalog`](../../proto/filmuvfs/catalog/v1/catalog.proto) gRPC channel.
It intentionally stops at catalog snapshot/delta generation and does not start a
gRPC server or any FUSE/runtime behavior.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Literal, cast

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from filmu_py.db.models import MediaEntryORM, MediaItemORM
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.services.mount_worker import (
    MountMediaEntryQueryBlockedReason,
    MountMediaEntryQueryContract,
    MountMediaEntryQueryExecutor,
    MountMediaEntryQueryStrategy,
    MountPlaybackSnapshotSupplier,
    PersistedMountMediaEntryQueryExecutor,
    build_mount_media_entry_query_contract,
    build_mount_media_entry_query_contract_from_snapshot,
)
from filmu_py.services.playback import (
    DirectFileLinkLifecycleSnapshot,
    DirectFileLinkLocatorSource,
    DirectFileLinkMatchBasis,
    DirectFileLinkProviderFamily,
    PlaybackResolutionSnapshot,
    PlaybackSourceService,
)

VfsCatalogEntryKind = Literal["directory", "file"]
VfsCatalogMediaType = Literal["movie", "show", "season", "episode", "unknown"]
VfsCatalogFileTransport = Literal["local-file", "remote-direct"]
VfsCatalogLeaseState = Literal["ready", "stale", "refreshing", "failed", "unknown"]
VfsCatalogPlaybackRole = Literal["direct", "hls"]
VfsCatalogBlockedReason = (
    MountMediaEntryQueryBlockedReason
    | Literal[
        "missing_media_entry",
        "non_file_attachment",
        "unresolved_query",
    ]
)

_INVALID_PATH_SEGMENT = re.compile(r'[<>:"/\\|?*\x00-\x1f]+')
_COLLAPSE_WHITESPACE = re.compile(r"\s+")
# Season-detection patterns applied to media-entry file paths.
# Ordered from most to least specific to minimise false positives.
_SEASON_RANGE_PATTERNS = (
    re.compile(r"\bS(\d{1,2})\s*[-\u2013]\s*S(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"\bS(\d{1,2})\s*[-\u2013]\s*(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"\bSeasons?\s*[\._ -]*(\d{1,2})\s*[-\u2013]\s*(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"\bSeries\s*[\._ -]*(\d{1,2})\s*[-\u2013]\s*(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"\bSeasons?\s*[\._ -]*(\d{1,2})\s*(?:to|through|thru)\s*(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"\bSeries\s*[\._ -]*(\d{1,2})\s*(?:to|through|thru)\s*(\d{1,2})\b", re.IGNORECASE),
)
_SEASON_SINGLE_PATTERNS = (
    re.compile(r"\bSeason\s*[\._ -]*(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"\bSeasons\s*[\._ -]*(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"\bSeries\s*[\._ -]*(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"\bS(\d{1,2})E\d{1,3}\b", re.IGNORECASE),
    re.compile(r"\bS(\d{1,2})x\d{1,3}\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,2})x\d{1,3}\b", re.IGNORECASE),
    re.compile(r"\bSeason\s*[\._ -]*(\d{1,2})\s*(?:Complete|Pack)\b", re.IGNORECASE),
    re.compile(r"\bS(\d{1,2})\s*(?:Complete|Pack)\b", re.IGNORECASE),
    re.compile(r"\bS(\d{1,2})\b", re.IGNORECASE),
)
_COMPLETE_SERIES_PATTERNS = (
    re.compile(r"\bComplete\s+Series\b", re.IGNORECASE),
    re.compile(r"\bFull\s+Series\b", re.IGNORECASE),
    re.compile(r"\bComplete\s+Seasons\b", re.IGNORECASE),
    re.compile(r"\bAll\s+Seasons\b", re.IGNORECASE),
    re.compile(r"\bSeries\s+Collection\b", re.IGNORECASE),
    re.compile(r"\bSeason\s+Collection\b", re.IGNORECASE),
)
_EPISODE_HINT_PATTERNS = (
    re.compile(r"\bS\d{1,2}E\d{1,3}\b", re.IGNORECASE),
    re.compile(r"\bS\d{1,2}x\d{1,3}\b", re.IGNORECASE),
    re.compile(r"\b\d{1,2}x\d{1,3}\b", re.IGNORECASE),
    re.compile(r"\b(?:E|EP|Episode)\s*[\._ -]*\d{1,3}\b", re.IGNORECASE),
)
_EPISODE_NUMBER_PATTERNS = (
    re.compile(r"\bS\d{1,2}E(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"\bS\d{1,2}x(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"\b\d{1,2}x(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"\b(?:E|EP|Episode)\s*[\._ -]*(\d{1,3})\b", re.IGNORECASE),
)
_SPECIALS_PATTERN = re.compile(r"\bSpecials?\b", re.IGNORECASE)
_MAX_SEASON_RANGE = 50
_WINDOWS_RESERVED_SEGMENTS = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    }
)


@dataclass(frozen=True, slots=True)
class VfsCatalogCorrelationKeys:
    """Stable correlation keys shared across the Python supplier and future Rust runtime."""

    item_id: str | None = None
    media_entry_id: str | None = None
    source_attachment_id: str | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    session_id: str | None = None
    handle_key: str | None = None


@dataclass(frozen=True, slots=True)
class VfsCatalogDirectoryEntry:
    """Directory payload aligned with the proto contract's directory detail."""

    path: str


@dataclass(frozen=True, slots=True)
class VfsCatalogFileEntry:
    """File payload aligned with the proto contract's file detail."""

    item_id: str
    item_title: str
    item_external_ref: str
    media_entry_id: str
    source_attachment_id: str | None
    media_type: VfsCatalogMediaType
    transport: VfsCatalogFileTransport
    locator: str
    local_path: str | None = None
    restricted_url: str | None = None
    unrestricted_url: str | None = None
    original_filename: str | None = None
    size_bytes: int | None = None
    lease_state: VfsCatalogLeaseState = "unknown"
    expires_at: datetime | None = None
    last_refreshed_at: datetime | None = None
    last_refresh_error: str | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    active_roles: tuple[VfsCatalogPlaybackRole, ...] = ()
    source_key: str | None = None
    query_strategy: MountMediaEntryQueryStrategy | None = None
    provider_family: DirectFileLinkProviderFamily = "none"
    locator_source: DirectFileLinkLocatorSource = "locator"
    match_basis: DirectFileLinkMatchBasis | None = None
    restricted_fallback: bool = False


@dataclass(frozen=True, slots=True)
class VfsCatalogEntry:
    """One proto-aligned catalog entry for a directory or file node."""

    entry_id: str
    parent_entry_id: str | None
    path: str
    name: str
    kind: VfsCatalogEntryKind
    correlation: VfsCatalogCorrelationKeys = field(default_factory=VfsCatalogCorrelationKeys)
    directory: VfsCatalogDirectoryEntry | None = None
    file: VfsCatalogFileEntry | None = None


@dataclass(frozen=True, slots=True)
class VfsCatalogBlockedItem:
    """Internal observability record for items that cannot be projected into the catalog."""

    item_id: str
    external_ref: str
    title: str
    reason: VfsCatalogBlockedReason


@dataclass(frozen=True, slots=True)
class VfsCatalogStats:
    """Aggregate snapshot counts mirrored by the proto contract."""

    directory_count: int
    file_count: int
    blocked_item_count: int


@dataclass(frozen=True, slots=True)
class VfsCatalogSnapshot:
    """Full proto-aligned catalog snapshot published by the Python supplier."""

    generation_id: str
    published_at: datetime
    entries: tuple[VfsCatalogEntry, ...]
    stats: VfsCatalogStats
    blocked_items: tuple[VfsCatalogBlockedItem, ...] = ()


@dataclass(frozen=True, slots=True)
class VfsCatalogRemoval:
    """One removed catalog entry emitted by a computed delta."""

    entry_id: str
    path: str
    kind: VfsCatalogEntryKind
    correlation: VfsCatalogCorrelationKeys


@dataclass(frozen=True, slots=True)
class VfsCatalogDelta:
    """Computed change-set between two full catalog snapshots."""

    generation_id: str
    base_generation_id: str | None
    published_at: datetime
    upserts: tuple[VfsCatalogEntry, ...]
    removals: tuple[VfsCatalogRemoval, ...]
    stats: VfsCatalogStats


@dataclass(frozen=True, slots=True)
class VfsCatalogWatchEvent:
    """Future gRPC-stream event payload produced by the supplier before transport binding."""

    event_id: str
    published_at: datetime
    snapshot: VfsCatalogSnapshot | None = None
    delta: VfsCatalogDelta | None = None


@dataclass(frozen=True, slots=True)
class _PreparedCatalogFile:
    """Internal prepared file projection before path de-duplication and entry wrapping."""

    media_entry_id: str
    candidate_path: str
    dedupe_suffix: str
    correlation: VfsCatalogCorrelationKeys
    payload: VfsCatalogFileEntry


class FilmuVfsCatalogSupplier:
    """Project persisted Python playback state into the Rust-sidecar catalog contract."""

    def __init__(
        self,
        db: DatabaseRuntime,
        *,
        playback_snapshot_supplier: MountPlaybackSnapshotSupplier,
        query_executor: MountMediaEntryQueryExecutor | None = None,
    ) -> None:
        self._db = db
        self._playback_snapshot_supplier = playback_snapshot_supplier
        self._query_executor = query_executor or PersistedMountMediaEntryQueryExecutor(db)
        self._state_lock = asyncio.Lock()
        self._generation_counter = 0
        self._current_snapshot: VfsCatalogSnapshot | None = None
        self._current_fingerprint: str | None = None
        self._snapshot_history: OrderedDict[int, VfsCatalogSnapshot] = OrderedDict()
        self._snapshot_history_limit = 64

    async def build_snapshot(self) -> VfsCatalogSnapshot:
        """Build a full current catalog snapshot for the future Rust sidecar."""

        async with self._state_lock:
            return await self._build_snapshot_locked()

    async def snapshot_for_generation(self, generation_id: int) -> VfsCatalogSnapshot | None:
        """Return one cached snapshot for a known numeric generation identifier."""

        if generation_id <= 0:
            return None

        async with self._state_lock:
            await self._build_snapshot_locked()
            return self._snapshot_history.get(generation_id)

    async def build_delta_since(self, generation_id: int) -> VfsCatalogDelta | None:
        """Build a delta from one previously published generation to the current catalog state."""

        if generation_id <= 0:
            return None

        async with self._state_lock:
            current = await self._build_snapshot_locked()
            previous = self._snapshot_history.get(generation_id)

        if previous is None:
            return None
        return self._build_delta_from_snapshots(previous, current)

    async def _build_snapshot_locked(self) -> VfsCatalogSnapshot:
        """Build or reuse the latest snapshot while holding the supplier state lock."""

        published_at = datetime.now(UTC)
        used_file_paths: set[str] = set()
        prepared_files: list[_PreparedCatalogFile] = []
        blocked_items: list[VfsCatalogBlockedItem] = []

        for item in await self._load_items():
            item_files, item_blocked = await self._prepare_item_files(item)
            blocked_items.extend(item_blocked)
            for prepared in item_files:
                unique_path = self._dedupe_file_path(
                    prepared.candidate_path,
                    dedupe_suffix=prepared.dedupe_suffix,
                    used_paths=used_file_paths,
                )
                used_file_paths.add(unique_path)
                prepared_files.append(
                    _PreparedCatalogFile(
                        media_entry_id=prepared.media_entry_id,
                        candidate_path=unique_path,
                        dedupe_suffix=prepared.dedupe_suffix,
                        correlation=prepared.correlation,
                        payload=prepared.payload,
                    )
                )

        directory_paths = self._build_directory_paths(prepared_files)
        directory_entries = self._build_directory_entries(directory_paths)
        file_entries = self._build_file_entries(prepared_files)
        entries = tuple(directory_entries + file_entries)
        stats = VfsCatalogStats(
            directory_count=len(directory_entries),
            file_count=len(file_entries),
            blocked_item_count=len(blocked_items),
        )
        blocked_items_tuple = tuple(blocked_items)
        fingerprint = self._build_snapshot_fingerprint(
            entries,
            stats=stats,
            blocked_items=blocked_items_tuple,
        )

        if self._current_snapshot is not None and fingerprint == self._current_fingerprint:
            current_snapshot = self._current_snapshot
            assert current_snapshot is not None
            return current_snapshot

        self._generation_counter += 1
        snapshot = VfsCatalogSnapshot(
            generation_id=str(self._generation_counter),
            published_at=published_at,
            entries=entries,
            stats=stats,
            blocked_items=blocked_items_tuple,
        )
        self._current_snapshot = snapshot
        self._current_fingerprint = fingerprint
        self._snapshot_history[self._generation_counter] = snapshot
        self._snapshot_history.move_to_end(self._generation_counter)
        while len(self._snapshot_history) > self._snapshot_history_limit:
            self._snapshot_history.popitem(last=False)
        return snapshot

    async def build_snapshot_event(self) -> VfsCatalogWatchEvent:
        """Build the first full-watch event for the future `WatchCatalog` stream."""

        snapshot = await self.build_snapshot()
        return VfsCatalogWatchEvent(
            event_id=f"catalog-snapshot:{snapshot.generation_id}",
            published_at=snapshot.published_at,
            snapshot=snapshot,
        )

    async def build_delta(self, previous: VfsCatalogSnapshot) -> VfsCatalogDelta:
        """Build a delta from a previously published snapshot to the current catalog state."""

        async with self._state_lock:
            current = await self._build_snapshot_locked()
        return self._build_delta_from_snapshots(previous, current)

    @staticmethod
    def _build_delta_from_snapshots(
        previous: VfsCatalogSnapshot,
        current: VfsCatalogSnapshot,
    ) -> VfsCatalogDelta:
        previous_entries = {entry.entry_id: entry for entry in previous.entries}
        current_entries = {entry.entry_id: entry for entry in current.entries}
        path_changed_entry_ids = {
            entry_id
            for entry_id, current_entry in current_entries.items()
            if (previous_entry := previous_entries.get(entry_id)) is not None
            and previous_entry.path != current_entry.path
        }

        upserts = tuple(
            entry
            for entry_id, entry in sorted(current_entries.items(), key=lambda pair: pair[1].path)
            if previous_entries.get(entry_id) != entry
        )
        removals = tuple(
            VfsCatalogRemoval(
                entry_id=entry.entry_id,
                path=entry.path,
                kind=entry.kind,
                correlation=entry.correlation,
            )
            for entry_id, entry in sorted(previous_entries.items(), key=lambda pair: pair[1].path)
            if entry_id not in current_entries or entry_id in path_changed_entry_ids
        )
        return VfsCatalogDelta(
            generation_id=current.generation_id,
            base_generation_id=previous.generation_id,
            published_at=current.published_at,
            upserts=upserts,
            removals=removals,
            stats=current.stats,
        )

    async def build_delta_event(self, previous: VfsCatalogSnapshot) -> VfsCatalogWatchEvent:
        """Build a delta watch event for the future `WatchCatalog` stream."""

        delta = await self.build_delta(previous)
        base_generation = delta.base_generation_id or "none"
        return VfsCatalogWatchEvent(
            event_id=f"catalog-delta:{base_generation}:{delta.generation_id}",
            published_at=delta.published_at,
            delta=delta,
        )

    async def _load_items(self) -> list[MediaItemORM]:
        async with self._db.session() as session:
            result = await session.execute(
                select(MediaItemORM)
                .options(
                    selectinload(MediaItemORM.playback_attachments),
                    selectinload(MediaItemORM.media_entries).selectinload(
                        MediaEntryORM.source_attachment
                    ),
                    selectinload(MediaItemORM.active_streams),
                )
                .order_by(MediaItemORM.created_at.asc(), MediaItemORM.id.asc())
            )
            return list(result.scalars().all())

    async def _prepare_item_files(
        self,
        item: MediaItemORM,
    ) -> tuple[list[_PreparedCatalogFile], list[VfsCatalogBlockedItem]]:
        """Prepare one catalog file entry per valid MediaEntryORM for this item.

        Show-level items backed by multiple media entries (e.g. a full-season
        pack split into per-episode files) each produce a separate VFS file
        rather than collapsing to the single 'active' entry the singular query
        executor would pick.  Season directories are inferred from
        ``provider_file_path`` when the item itself carries no season attribute.
        """
        file_entries = [
            me for me in item.media_entries
            if me.kind in {"local-file", "remote-direct"}
        ]
        if not file_entries:
            # Nothing persisted yet — fall back to the singular path so that
            # the contract machinery can classify the correct blocked reason.
            prepared, blocked = await self._prepare_item_file(item)
            return ([prepared] if prepared is not None else []), (
                [blocked] if blocked is not None else []
            )

        snapshot = self._playback_snapshot_supplier.build_resolution_snapshot(item)
        contract = build_mount_media_entry_query_contract_from_snapshot(
            item, snapshot, role="direct"
        )
        media_type = self._normalize_media_type(item)
        prepared_list: list[_PreparedCatalogFile] = []
        for media_entry in file_entries:
            filename = self._select_original_filename(item, media_entry, snapshot)
            candidate_path = self._build_candidate_path_for_entry(
                item,
                media_entry=media_entry,
                media_type=media_type,
                filename=filename,
            )
            correlation = self._build_correlation_keys(item, media_entry)
            payload = self._build_entry_payload(item, media_entry, snapshot, contract=contract)
            # Combine external_ref + entry-ID suffix to guarantee uniqueness per file.
            dedupe_suffix = self._sanitize_path_segment(
                f"{item.external_ref or media_entry.id[:8]}-{media_entry.id[:8]}"
            )
            prepared_list.append(
                _PreparedCatalogFile(
                    media_entry_id=media_entry.id,
                    candidate_path=candidate_path,
                    dedupe_suffix=dedupe_suffix,
                    correlation=correlation,
                    payload=payload,
                )
            )
        return prepared_list, []

    async def _prepare_item_file(
        self,
        item: MediaItemORM,
    ) -> tuple[_PreparedCatalogFile | None, VfsCatalogBlockedItem | None]:
        snapshot = self._playback_snapshot_supplier.build_resolution_snapshot(item)
        contract = build_mount_media_entry_query_contract(
            item,
            role="direct",
            playback_snapshot_supplier=self._playback_snapshot_supplier,
        )
        if contract.status != "queryable":
            assert contract.blocked_reason is not None
            return None, self._build_blocked_item(item, contract.blocked_reason)

        query_result = await self._query_executor.resolve_media_entry(contract)
        if query_result.media_entry_id is None:
            return None, self._build_blocked_item(item, "unresolved_query")

        media_entry = self._find_media_entry(item, query_result.media_entry_id)
        if media_entry is None:
            return None, self._build_blocked_item(item, "missing_media_entry")
        if media_entry.kind not in {"local-file", "remote-direct"}:
            return None, self._build_blocked_item(item, "non_file_attachment")

        direct_attachment = snapshot.direct
        direct_lifecycle = snapshot.direct_lifecycle
        if direct_attachment is None or direct_lifecycle is None:
            return None, self._build_blocked_item(item, "missing_lifecycle")

        media_type = self._normalize_media_type(item)
        original_filename = self._select_original_filename(item, media_entry, snapshot)
        candidate_path = self._build_candidate_path(
            item, media_type=media_type, filename=original_filename
        )
        correlation = self._build_correlation_keys(item, media_entry)
        payload = self._build_file_payload(
            item,
            media_entry,
            snapshot,
            lifecycle=direct_lifecycle,
            query_strategy=query_result.matched_strategy,
            media_type=media_type,
        )
        dedupe_suffix = self._sanitize_path_segment(item.external_ref or media_entry.id[:8])
        return (
            _PreparedCatalogFile(
                media_entry_id=media_entry.id,
                candidate_path=candidate_path,
                dedupe_suffix=dedupe_suffix,
                correlation=correlation,
                payload=payload,
            ),
            None,
        )

    @staticmethod
    def _find_media_entry(item: MediaItemORM, media_entry_id: str) -> MediaEntryORM | None:
        for entry in item.media_entries:
            if entry.id == media_entry_id:
                return entry
        return None

    @staticmethod
    def _infer_season_from_path(path: str | None) -> int | None:
        """Return a season number inferred from common file-naming patterns, or None."""
        if not path:
            return None
        if _SPECIALS_PATTERN.search(path):
            return None

        for pattern in _SEASON_RANGE_PATTERNS:
            match = pattern.search(path)
            if match:
                try:
                    start, end = int(match.group(1)), int(match.group(2))
                    if start <= end and (end - start) < _MAX_SEASON_RANGE:
                        return start
                except (ValueError, IndexError):
                    pass

        for pattern in _SEASON_SINGLE_PATTERNS:
            match = pattern.search(path)
            if match:
                try:
                    return int(match.group(1))
                except (ValueError, IndexError):
                    pass

        return None

    @staticmethod
    def _should_default_to_season_one(path: str | None) -> bool:
        """Return whether one path should fall back to Season 01 when season cannot be inferred."""

        if not path:
            return False
        if _SPECIALS_PATTERN.search(path):
            return False
        if any(pattern.search(path) for pattern in _COMPLETE_SERIES_PATTERNS):
            return True
        return any(pattern.search(path) for pattern in _EPISODE_HINT_PATTERNS)

    @staticmethod
    def _build_candidate_path_for_entry(
        item: MediaItemORM,
        *,
        media_entry: MediaEntryORM,
        media_type: VfsCatalogMediaType,
        filename: str,
    ) -> str:
        """Build a VFS path for one specific media entry, inferring season from the entry path."""
        attributes = cast(dict[str, object], item.attributes or {})

        if media_type == "movie":
            extension = FilmuVfsCatalogSupplier._suffix_from_filename(filename)
            movie_dir = FilmuVfsCatalogSupplier._normalized_movie_directory(item)
            safe_filename = FilmuVfsCatalogSupplier._normalized_movie_filename(item, extension=extension)
            return str(PurePosixPath("/") / "movies" / movie_dir / safe_filename)

        show_dir = FilmuVfsCatalogSupplier._normalized_show_directory(item)
        season_number = FilmuVfsCatalogSupplier._extract_int(
            attributes, "season_number", "season", "parent_season_number"
        )
        # Show-level records don't always carry season_number in attributes.
        # When absent, infer from persisted provider paths. If still unavailable,
        # default to Season 01 for known episodic rows so files are grouped under
        # a season folder instead of the show root.
        # For show-level items the season isn't in the item attributes; infer
        # it from the media entry's own file path (e.g. "S01E03.mkv").
        if season_number is None:
            entry_path = media_entry.provider_file_path or media_entry.original_filename
            season_number = FilmuVfsCatalogSupplier._infer_season_from_path(entry_path)

        if (
            season_number is None
            and media_type in {"show", "episode"}
            and FilmuVfsCatalogSupplier._should_default_to_season_one(entry_path)
        ):
            season_number = 1

        episode_number = FilmuVfsCatalogSupplier._extract_int(attributes, "episode_number", "episode")
        if episode_number is None:
            entry_path = media_entry.provider_file_path or media_entry.original_filename
            episode_number = FilmuVfsCatalogSupplier._infer_episode_from_path(entry_path)

        safe_filename = FilmuVfsCatalogSupplier._sanitize_path_segment(filename)

        if season_number is not None:
            season_dir = f"Season {season_number:02d}"
            return str(PurePosixPath("/") / "shows" / show_dir / season_dir / safe_filename)
        return str(PurePosixPath("/") / "shows" / show_dir / safe_filename)

    @staticmethod
    def _build_entry_payload(
        item: MediaItemORM,
        media_entry: MediaEntryORM,
        snapshot: PlaybackResolutionSnapshot,
        *,
        contract: MountMediaEntryQueryContract,
    ) -> VfsCatalogFileEntry:
        """Build a file-entry payload directly from a specific MediaEntryORM.

        Unlike ``_build_file_payload`` which always reads from the active
        snapshot attachment (singular), this helper projects the given entry's
        own fields so that every entry in a multi-file item gets an
        accurate, distinct payload.
        """
        active_attachment = snapshot.direct
        restricted_url, unrestricted_url = PlaybackSourceService._normalize_media_entry_urls(
            provider=media_entry.provider,
            restricted_url=media_entry.download_url,
            unrestricted_url=media_entry.unrestricted_url,
        )
        locator = (
            unrestricted_url
            or restricted_url
            or media_entry.local_path
            or (active_attachment.locator if active_attachment else "")
        )
        active_roles = tuple(
            role
            for role in FilmuVfsCatalogSupplier._active_roles_for_media_entry(
                item, media_entry.id
            )
            if role in {"direct", "hls"}
        )
        effective_refresh_state = PlaybackSourceService._effective_media_entry_refresh_state(
            media_entry.refresh_state,
            provider=media_entry.provider,
            restricted_url=restricted_url,
            unrestricted_url=media_entry.unrestricted_url,
        )
        lease_state = FilmuVfsCatalogSupplier._normalize_lease_state(effective_refresh_state)
        direct_lifecycle = snapshot.direct_lifecycle
        media_type = FilmuVfsCatalogSupplier._normalize_media_type(item)
        return VfsCatalogFileEntry(
            item_id=item.id,
            item_title=item.title,
            item_external_ref=item.external_ref,
            media_entry_id=media_entry.id,
            source_attachment_id=media_entry.source_attachment_id,
            media_type=media_type,
            transport=("local-file" if media_entry.kind == "local-file" else "remote-direct"),
            locator=locator,
            local_path=media_entry.local_path,
            restricted_url=restricted_url,
            unrestricted_url=unrestricted_url,
            original_filename=media_entry.original_filename,
            size_bytes=media_entry.size_bytes,
            lease_state=lease_state,
            expires_at=media_entry.expires_at,
            last_refreshed_at=media_entry.last_refreshed_at,
            last_refresh_error=media_entry.last_refresh_error,
            provider=media_entry.provider,
            provider_download_id=media_entry.provider_download_id,
            provider_file_id=media_entry.provider_file_id,
            provider_file_path=media_entry.provider_file_path,
            active_roles=active_roles,
            source_key=contract.source_key,
            query_strategy="by-media-entry-id",
            provider_family=direct_lifecycle.provider_family if direct_lifecycle else "none",
            locator_source=direct_lifecycle.locator_source if direct_lifecycle else "locator",
            match_basis=direct_lifecycle.match_basis if direct_lifecycle else None,
            restricted_fallback=(
                direct_lifecycle.restricted_fallback
                if direct_lifecycle
                else unrestricted_url is None and restricted_url is not None
            ),
        )

    @staticmethod
    def _build_blocked_item(
        item: MediaItemORM, reason: VfsCatalogBlockedReason
    ) -> VfsCatalogBlockedItem:
        return VfsCatalogBlockedItem(
            item_id=item.id,
            external_ref=item.external_ref,
            title=item.title,
            reason=reason,
        )

    @staticmethod
    def _build_correlation_keys(
        item: MediaItemORM,
        media_entry: MediaEntryORM,
    ) -> VfsCatalogCorrelationKeys:
        return VfsCatalogCorrelationKeys(
            item_id=item.id,
            media_entry_id=media_entry.id,
            source_attachment_id=media_entry.source_attachment_id,
            provider=media_entry.provider,
            provider_download_id=media_entry.provider_download_id,
            provider_file_id=media_entry.provider_file_id,
            provider_file_path=media_entry.provider_file_path,
        )

    @staticmethod
    def _build_file_payload(
        item: MediaItemORM,
        media_entry: MediaEntryORM,
        snapshot: PlaybackResolutionSnapshot,
        *,
        lifecycle: DirectFileLinkLifecycleSnapshot,
        query_strategy: MountMediaEntryQueryStrategy | None,
        media_type: VfsCatalogMediaType,
    ) -> VfsCatalogFileEntry:
        direct_attachment = snapshot.direct
        assert direct_attachment is not None

        active_roles = tuple(
            role
            for role in FilmuVfsCatalogSupplier._active_roles_for_media_entry(item, media_entry.id)
            if role in {"direct", "hls"}
        )
        lease_state = FilmuVfsCatalogSupplier._normalize_lease_state(media_entry.refresh_state)
        return VfsCatalogFileEntry(
            item_id=item.id,
            item_title=item.title,
            item_external_ref=item.external_ref,
            media_entry_id=media_entry.id,
            source_attachment_id=media_entry.source_attachment_id,
            media_type=media_type,
            transport=("local-file" if media_entry.kind == "local-file" else "remote-direct"),
            locator=direct_attachment.locator,
            local_path=media_entry.local_path or direct_attachment.local_path,
            restricted_url=media_entry.download_url or direct_attachment.restricted_url,
            unrestricted_url=media_entry.unrestricted_url or direct_attachment.unrestricted_url,
            original_filename=media_entry.original_filename or direct_attachment.original_filename,
            size_bytes=media_entry.size_bytes
            if media_entry.size_bytes is not None
            else direct_attachment.file_size,
            lease_state=lease_state,
            expires_at=media_entry.expires_at,
            last_refreshed_at=media_entry.last_refreshed_at,
            last_refresh_error=media_entry.last_refresh_error,
            provider=media_entry.provider or direct_attachment.provider,
            provider_download_id=media_entry.provider_download_id
            or direct_attachment.provider_download_id,
            provider_file_id=media_entry.provider_file_id or direct_attachment.provider_file_id,
            provider_file_path=media_entry.provider_file_path
            or direct_attachment.provider_file_path,
            active_roles=active_roles,
            source_key=direct_attachment.source_key,
            query_strategy=query_strategy,
            provider_family=lifecycle.provider_family,
            locator_source=lifecycle.locator_source,
            match_basis=lifecycle.match_basis,
            restricted_fallback=lifecycle.restricted_fallback,
        )

    @staticmethod
    def _active_roles_for_media_entry(
        item: MediaItemORM,
        media_entry_id: str,
    ) -> tuple[VfsCatalogPlaybackRole, ...]:
        ordered_roles = sorted(
            item.active_streams,
            key=lambda active_stream: (
                0 if active_stream.role == "direct" else 1,
                active_stream.created_at,
                active_stream.id,
            ),
        )
        roles: list[VfsCatalogPlaybackRole] = []
        for active_stream in ordered_roles:
            if active_stream.media_entry_id != media_entry_id:
                continue
            if active_stream.role in {"direct", "hls"} and active_stream.role not in roles:
                roles.append(cast(VfsCatalogPlaybackRole, active_stream.role))
        return cast(tuple[VfsCatalogPlaybackRole, ...], tuple(roles))

    @staticmethod
    def _normalize_lease_state(value: str | None) -> VfsCatalogLeaseState:
        if value in {"ready", "stale", "refreshing", "failed"}:
            return cast(VfsCatalogLeaseState, value)
        return "unknown"

    @staticmethod
    def _normalize_media_type(item: MediaItemORM) -> VfsCatalogMediaType:
        attributes = cast(dict[str, object], item.attributes or {})
        raw_type = FilmuVfsCatalogSupplier._extract_string(attributes, "item_type", "media_type")
        if raw_type is not None:
            normalized = raw_type.strip().casefold()
            mapping = {
                "movie": "movie",
                "show": "show",
                "season": "season",
                "episode": "episode",
                "tv": "show",
                "series": "show",
            }
            mapped = mapping.get(normalized)
            if mapped is not None:
                return cast(VfsCatalogMediaType, mapped)

        if (
            FilmuVfsCatalogSupplier._extract_int(attributes, "episode_number", "episode")
            is not None
        ):
            return "episode"
        if FilmuVfsCatalogSupplier._extract_int(attributes, "season_number", "season") is not None:
            return "season"
        if FilmuVfsCatalogSupplier._extract_string(attributes, "tvdb_id") is not None:
            return "show"
        if FilmuVfsCatalogSupplier._extract_string(attributes, "tmdb_id") is not None:
            return "movie"
        return "unknown"

    @staticmethod
    def _extract_string(attributes: dict[str, object], *keys: str) -> str | None:
        for key in keys:
            value = attributes.get(key)
            if isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    return stripped
        return None

    @staticmethod
    def _extract_int(attributes: dict[str, object], *keys: str) -> int | None:
        for key in keys:
            value = attributes.get(key)
            if isinstance(value, bool):
                continue
            if isinstance(value, int):
                return value
            if isinstance(value, str):
                stripped = value.strip()
                if stripped and stripped.lstrip("-").isdigit():
                    return int(stripped)
        return None

    @staticmethod
    def _select_original_filename(
        item: MediaItemORM,
        media_entry: MediaEntryORM,
        snapshot: PlaybackResolutionSnapshot,
    ) -> str:
        direct_attachment = snapshot.direct
        assert direct_attachment is not None

        candidates = (
            media_entry.original_filename,
            direct_attachment.original_filename,
            media_entry.local_path,
            direct_attachment.local_path,
            media_entry.provider_file_path,
            direct_attachment.provider_file_path,
            media_entry.unrestricted_url,
            direct_attachment.unrestricted_url,
            media_entry.download_url,
            direct_attachment.restricted_url,
            direct_attachment.locator,
        )
        for candidate in candidates:
            basename = FilmuVfsCatalogSupplier._basename_from_candidate(candidate)
            if basename is not None:
                return FilmuVfsCatalogSupplier._sanitize_path_segment(basename)
        return FilmuVfsCatalogSupplier._fallback_filename(item)

    @staticmethod
    def _basename_from_candidate(value: str | None) -> str | None:
        if value is None:
            return None
        candidate = value.rsplit("/", 1)[-1]
        candidate = candidate.rsplit("\\", 1)[-1]
        stripped = candidate.strip()
        return stripped or None

    @staticmethod
    def _fallback_filename(item: MediaItemORM) -> str:
        safe_name = FilmuVfsCatalogSupplier._sanitize_path_segment(item.title or item.external_ref)
        return f"{safe_name}.bin"

    @staticmethod
    def _build_candidate_path(
        item: MediaItemORM,
        *,
        media_type: VfsCatalogMediaType,
        filename: str,
    ) -> str:
        attributes = cast(dict[str, object], item.attributes or {})
        if media_type == "movie":
            extension = FilmuVfsCatalogSupplier._suffix_from_filename(filename)
            movie_dir = FilmuVfsCatalogSupplier._normalized_movie_directory(item)
            safe_filename = FilmuVfsCatalogSupplier._normalized_movie_filename(item, extension=extension)
            return str(PurePosixPath("/") / "movies" / movie_dir / safe_filename)

        show_dir = FilmuVfsCatalogSupplier._normalized_show_directory(item)
        season_number = FilmuVfsCatalogSupplier._extract_int(
            attributes,
            "season_number",
            "season",
            "parent_season_number",
        )
        safe_filename = FilmuVfsCatalogSupplier._sanitize_path_segment(filename)
        if media_type in {"season", "episode"} and season_number is not None:
            season_dir = f"Season {season_number:02d}"
            return str(PurePosixPath("/") / "shows" / show_dir / season_dir / safe_filename)
        return str(PurePosixPath("/") / "shows" / show_dir / safe_filename)

    @staticmethod
    def _infer_episode_from_path(path: str | None) -> int | None:
        if not path:
            return None
        for pattern in _EPISODE_NUMBER_PATTERNS:
            match = pattern.search(path)
            if match:
                try:
                    return int(match.group(1))
                except (ValueError, IndexError):
                    pass
        return None

    @staticmethod
    def _suffix_from_filename(filename: str) -> str:
        pure_path = PurePosixPath(filename)
        return "".join(pure_path.suffixes)

    @staticmethod
    def _normalized_movie_directory(item: MediaItemORM) -> str:
        attributes = cast(dict[str, object], item.attributes or {})
        title = FilmuVfsCatalogSupplier._sanitize_path_segment(item.title or item.external_ref)
        year = FilmuVfsCatalogSupplier._extract_int(attributes, "year", "release_year")
        return f"{title} ({year})" if year is not None else title

    @staticmethod
    def _normalized_movie_filename(item: MediaItemORM, *, extension: str) -> str:
        title = FilmuVfsCatalogSupplier._sanitize_path_segment(item.title or item.external_ref)
        return f"{title}{extension}"

    @staticmethod
    def _normalized_show_directory(item: MediaItemORM) -> str:
        attributes = cast(dict[str, object], item.attributes or {})
        show_title = FilmuVfsCatalogSupplier._sanitize_path_segment(
            FilmuVfsCatalogSupplier._extract_string(
                attributes, "show_title", "series_title", "parent_title"
            )
            or item.title
            or item.external_ref
        )
        year = FilmuVfsCatalogSupplier._extract_int(attributes, "year", "release_year")
        return f"{show_title} ({year})" if year is not None else show_title

    @staticmethod
    def _normalized_show_filename(
        item: MediaItemORM,
        *,
        extension: str,
        season_number: int | None,
        episode_number: int | None,
    ) -> str:
        attributes = cast(dict[str, object], item.attributes or {})
        show_title = FilmuVfsCatalogSupplier._sanitize_path_segment(
            FilmuVfsCatalogSupplier._extract_string(
                attributes, "show_title", "series_title", "parent_title"
            )
            or item.title
            or item.external_ref
        )
        if season_number is not None and episode_number is not None:
            return f"{show_title} - s{season_number:02d}e{episode_number:02d}{extension}"
        if season_number is not None:
            return f"{show_title} - s{season_number:02d}{extension}"
        return f"{show_title}{extension}"

    @staticmethod
    def _sanitize_path_segment(value: str) -> str:
        cleaned = _INVALID_PATH_SEGMENT.sub("_", value)
        cleaned = _COLLAPSE_WHITESPACE.sub(" ", cleaned).strip().rstrip(". ")
        if not cleaned:
            cleaned = "untitled"
        if cleaned.upper() in _WINDOWS_RESERVED_SEGMENTS:
            cleaned = f"{cleaned}_"
        return cleaned

    @staticmethod
    def _dedupe_file_path(candidate_path: str, *, dedupe_suffix: str, used_paths: set[str]) -> str:
        if candidate_path not in used_paths:
            return candidate_path

        pure_path = PurePosixPath(candidate_path)
        parent = pure_path.parent
        suffixes = pure_path.suffixes
        combined_suffix = "".join(suffixes)
        stem = pure_path.name.removesuffix(combined_suffix) if combined_suffix else pure_path.name
        normalized_suffix = FilmuVfsCatalogSupplier._sanitize_path_segment(dedupe_suffix)

        counter = 1
        while True:
            numbered_suffix = (
                normalized_suffix if counter == 1 else f"{normalized_suffix}-{counter}"
            )
            candidate_name = f"{stem} [{numbered_suffix}]{combined_suffix}"
            resolved = str(parent / candidate_name)
            if resolved not in used_paths:
                return resolved
            counter += 1

        raise AssertionError("unreachable path de-duplication loop")

    @staticmethod
    def _build_directory_paths(prepared_files: Sequence[_PreparedCatalogFile]) -> set[str]:
        directories = {"/", "/movies", "/shows"}
        for prepared in prepared_files:
            pure_path = PurePosixPath(prepared.candidate_path)
            current = PurePosixPath("/")
            directories.add(current.as_posix())
            path_parts = pure_path.parts
            part_count = len(path_parts)
            for index, part in enumerate(path_parts):
                if index == 0 or index == part_count - 1:
                    continue
                current = current / part
                directories.add(current.as_posix())
        return directories

    @staticmethod
    def _build_directory_entries(directory_paths: set[str]) -> list[VfsCatalogEntry]:
        ordered_paths = sorted(
            directory_paths,
            key=lambda path: (PurePosixPath(path).as_posix().count("/"), path),
        )
        entries: list[VfsCatalogEntry] = []
        for path in ordered_paths:
            if path == "/":
                parent_entry_id = None
                name = "/"
            else:
                parent_path = PurePosixPath(path).parent.as_posix()
                parent_entry_id = FilmuVfsCatalogSupplier._directory_entry_id(parent_path)
                name = PurePosixPath(path).name
            entries.append(
                VfsCatalogEntry(
                    entry_id=FilmuVfsCatalogSupplier._directory_entry_id(path),
                    parent_entry_id=parent_entry_id,
                    path=path,
                    name=name,
                    kind="directory",
                    correlation=VfsCatalogCorrelationKeys(),
                    directory=VfsCatalogDirectoryEntry(path=path),
                )
            )
        return entries

    @staticmethod
    def _build_file_entries(
        prepared_files: Sequence[_PreparedCatalogFile],
    ) -> list[VfsCatalogEntry]:
        entries: list[VfsCatalogEntry] = []
        for prepared in sorted(
            prepared_files, key=lambda item: (item.candidate_path, item.media_entry_id)
        ):
            parent_path = PurePosixPath(prepared.candidate_path).parent.as_posix()
            entries.append(
                VfsCatalogEntry(
                    entry_id=FilmuVfsCatalogSupplier._file_entry_id(prepared.media_entry_id),
                    parent_entry_id=FilmuVfsCatalogSupplier._directory_entry_id(parent_path),
                    path=prepared.candidate_path,
                    name=PurePosixPath(prepared.candidate_path).name,
                    kind="file",
                    correlation=prepared.correlation,
                    file=prepared.payload,
                )
            )
        return entries

    @staticmethod
    def _build_snapshot_fingerprint(
        entries: Sequence[VfsCatalogEntry],
        *,
        stats: VfsCatalogStats,
        blocked_items: Sequence[VfsCatalogBlockedItem],
    ) -> str:
        hasher = hashlib.sha256()
        hasher.update(f"{stats.directory_count}:{stats.file_count}:{stats.blocked_item_count}".encode())
        hasher.update(b"\0")
        for blocked_item in blocked_items:
            hasher.update(repr(blocked_item).encode("utf-8"))
            hasher.update(b"\0")
        for entry in entries:
            hasher.update(repr(entry).encode("utf-8"))
            hasher.update(b"\0")
        return hasher.hexdigest()

    @staticmethod
    def _directory_entry_id(path: str) -> str:
        return f"dir:{path}"

    @staticmethod
    def _file_entry_id(media_entry_id: str) -> str:
        return f"file:{media_entry_id}"
