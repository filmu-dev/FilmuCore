"""Typed plugin capability interfaces intended for future SDK packaging."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from filmu_py.plugins.context import PluginContext


@dataclass(frozen=True, slots=True)
class ExternalIdentifiers:
    """Well-known external identifiers shared across plugin capability DTOs."""

    tmdb_id: str | None = None
    tvdb_id: str | None = None
    imdb_id: str | None = None
    trakt_id: str | None = None


@dataclass(frozen=True, slots=True)
class ScraperSearchInput:
    """Typed search input passed into one scraper plugin."""

    item_id: str | None = None
    item_type: str | None = None
    title: str | None = None
    year: int | None = None
    season_number: int | None = None
    episode_number: int | None = None
    query: str | None = None
    external_ids: ExternalIdentifiers = field(default_factory=ExternalIdentifiers)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ScraperResult:
    """One normalized scraper result returned by a scraper plugin."""

    title: str
    provider: str | None = None
    magnet_url: str | None = None
    download_url: str | None = None
    info_hash: str | None = None
    quality: str | None = None
    size_bytes: int | None = None
    seeders: int | None = None
    leechers: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MagnetAddInput:
    """Request payload for a downloader plugin magnet-add call."""

    magnet_url: str
    display_name: str | None = None
    external_ids: ExternalIdentifiers = field(default_factory=ExternalIdentifiers)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MagnetAddResult:
    """Normalized result of a downloader plugin magnet-add request."""

    download_id: str
    accepted: bool = True
    queued_at: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DownloadStatusInput:
    """Typed status query for one downloader plugin download."""

    download_id: str


@dataclass(frozen=True, slots=True)
class DownloadFileRecord:
    """One file surfaced by a downloader plugin status response."""

    file_id: str
    path: str
    size_bytes: int | None = None
    selected: bool = False
    download_url: str | None = None


@dataclass(frozen=True, slots=True)
class DownloadStatusResult:
    """Normalized downloader status snapshot."""

    download_id: str
    status: str
    progress: float | None = None
    error: str | None = None
    files: tuple[DownloadFileRecord, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)


class DownloadStatus(StrEnum):
    """Normalized downloader lifecycle states shared by downloader plugins."""

    PENDING = "pending"
    DOWNLOADING = "downloading"
    PROCESSING = "processing"
    READY = "ready"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class DownloadLinksInput:
    """Typed request for downloader-provided resolved download links."""

    download_id: str
    file_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DownloadLinkResult:
    """One normalized download link returned by a downloader plugin."""

    url: str
    file_id: str | None = None
    filename: str | None = None
    size_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class IndexerInput:
    """Typed enrichment input for an indexer plugin."""

    item_id: str
    item_type: str
    title: str
    year: int | None = None
    external_ids: ExternalIdentifiers = field(default_factory=ExternalIdentifiers)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class IndexerResult:
    """Typed enrichment output returned by an indexer plugin."""

    title: str | None = None
    overview: str | None = None
    year: int | None = None
    external_ids: ExternalIdentifiers = field(default_factory=ExternalIdentifiers)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ContentRequest:
    """One content request emitted by a content-service plugin poll."""

    external_ref: str
    media_type: str
    title: str | None = None
    source: str = "unknown"
    source_list_id: str | None = None


@dataclass(frozen=True, slots=True)
class NotificationEvent:
    """Typed notification payload delivered to notification plugins."""

    event_type: str
    title: str
    message: str | None = None
    item_id: str | None = None
    severity: str = "info"
    payload: Mapping[str, Any] | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class StreamControlAction(StrEnum):
    """Supported stream-control actions on top of the serving/status substrate."""

    SERVING_STATUS_SNAPSHOT = "serving_status_snapshot"
    TRIGGER_DIRECT_PLAYBACK_REFRESH = "trigger_direct_playback_refresh"
    TRIGGER_HLS_FAILED_LEASE_REFRESH = "trigger_hls_failed_lease_refresh"
    TRIGGER_HLS_RESTRICTED_FALLBACK_REFRESH = "trigger_hls_restricted_fallback_refresh"
    MARK_SELECTED_HLS_MEDIA_ENTRY_STALE = "mark_selected_hls_media_entry_stale"


@dataclass(frozen=True, slots=True)
class StreamControlInput:
    """Typed stream-control request passed into one stream-control plugin."""

    action: StreamControlAction
    item_identifier: str | None = None
    prefer_queued: bool | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StreamControlResult:
    """Normalized stream-control outcome returned by one stream-control plugin."""

    action: StreamControlAction
    item_identifier: str | None
    accepted: bool
    outcome: str
    detail: str | None = None
    controller_attached: bool | None = None
    retry_after_seconds: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class PluginInitializer(Protocol):
    """Shared initialization contract for all plugin capability implementations."""

    async def initialize(self, ctx: PluginContext) -> None: ...


@runtime_checkable
class ScraperPlugin(PluginInitializer, Protocol):
    """Capability interface for scraper plugins."""

    async def search(self, metadata: ScraperSearchInput) -> list[ScraperResult]: ...


@runtime_checkable
class DownloaderPlugin(PluginInitializer, Protocol):
    """Capability interface for downloader plugins."""

    async def add_magnet(self, request: MagnetAddInput) -> MagnetAddResult: ...

    async def get_status(self, request: DownloadStatusInput) -> DownloadStatusResult: ...

    async def get_download_links(self, request: DownloadLinksInput) -> list[DownloadLinkResult]: ...


@runtime_checkable
class IndexerPlugin(PluginInitializer, Protocol):
    """Capability interface for metadata/indexer plugins."""

    async def enrich(self, item: IndexerInput) -> IndexerResult: ...


@runtime_checkable
class ContentServicePlugin(PluginInitializer, Protocol):
    """Capability interface for content-service intake plugins."""

    async def poll(self) -> list[ContentRequest]: ...


@runtime_checkable
class NotificationPlugin(PluginInitializer, Protocol):
    """Capability interface for notification plugins."""

    async def send(self, event: NotificationEvent) -> None: ...


@runtime_checkable
class StreamControlPlugin(PluginInitializer, Protocol):
    """Capability interface for controlled stream/status operations."""

    async def control(self, request: StreamControlInput) -> StreamControlResult:
        pass


@runtime_checkable
class PluginDatasource(Protocol):
    """Limited host datasource exposed to plugins through the context provider."""

    async def initialize(self, ctx: PluginContext) -> None: ...

    async def teardown(self) -> None: ...


@runtime_checkable
class PluginEventHookWorker(Protocol):
    """Asynchronous hook worker invoked for declared host or plugin events."""

    plugin_name: str
    subscribed_events: frozenset[str]

    async def initialize(self, ctx: PluginContext) -> None: ...

    async def handle(self, event_type: str, payload: dict[str, Any]) -> None: ...
