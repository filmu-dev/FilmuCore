import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CatalogEntryKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_ENTRY_KIND_UNSPECIFIED: _ClassVar[CatalogEntryKind]
    CATALOG_ENTRY_KIND_DIRECTORY: _ClassVar[CatalogEntryKind]
    CATALOG_ENTRY_KIND_FILE: _ClassVar[CatalogEntryKind]

class CatalogMediaType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_MEDIA_TYPE_UNSPECIFIED: _ClassVar[CatalogMediaType]
    CATALOG_MEDIA_TYPE_MOVIE: _ClassVar[CatalogMediaType]
    CATALOG_MEDIA_TYPE_SHOW: _ClassVar[CatalogMediaType]
    CATALOG_MEDIA_TYPE_SEASON: _ClassVar[CatalogMediaType]
    CATALOG_MEDIA_TYPE_EPISODE: _ClassVar[CatalogMediaType]
    CATALOG_MEDIA_TYPE_UNKNOWN: _ClassVar[CatalogMediaType]

class CatalogFileTransport(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_FILE_TRANSPORT_UNSPECIFIED: _ClassVar[CatalogFileTransport]
    CATALOG_FILE_TRANSPORT_LOCAL_FILE: _ClassVar[CatalogFileTransport]
    CATALOG_FILE_TRANSPORT_REMOTE_DIRECT: _ClassVar[CatalogFileTransport]

class CatalogLeaseState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_LEASE_STATE_UNSPECIFIED: _ClassVar[CatalogLeaseState]
    CATALOG_LEASE_STATE_READY: _ClassVar[CatalogLeaseState]
    CATALOG_LEASE_STATE_STALE: _ClassVar[CatalogLeaseState]
    CATALOG_LEASE_STATE_REFRESHING: _ClassVar[CatalogLeaseState]
    CATALOG_LEASE_STATE_FAILED: _ClassVar[CatalogLeaseState]
    CATALOG_LEASE_STATE_UNKNOWN: _ClassVar[CatalogLeaseState]

class CatalogPlaybackRole(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_PLAYBACK_ROLE_UNSPECIFIED: _ClassVar[CatalogPlaybackRole]
    CATALOG_PLAYBACK_ROLE_DIRECT: _ClassVar[CatalogPlaybackRole]
    CATALOG_PLAYBACK_ROLE_HLS: _ClassVar[CatalogPlaybackRole]

class CatalogProviderFamily(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_PROVIDER_FAMILY_UNSPECIFIED: _ClassVar[CatalogProviderFamily]
    CATALOG_PROVIDER_FAMILY_NONE: _ClassVar[CatalogProviderFamily]
    CATALOG_PROVIDER_FAMILY_DEBRID: _ClassVar[CatalogProviderFamily]
    CATALOG_PROVIDER_FAMILY_PROVIDER: _ClassVar[CatalogProviderFamily]

class CatalogLocatorSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_LOCATOR_SOURCE_UNSPECIFIED: _ClassVar[CatalogLocatorSource]
    CATALOG_LOCATOR_SOURCE_LOCAL_PATH: _ClassVar[CatalogLocatorSource]
    CATALOG_LOCATOR_SOURCE_UNRESTRICTED_URL: _ClassVar[CatalogLocatorSource]
    CATALOG_LOCATOR_SOURCE_RESTRICTED_URL: _ClassVar[CatalogLocatorSource]
    CATALOG_LOCATOR_SOURCE_LOCATOR: _ClassVar[CatalogLocatorSource]

class CatalogMatchBasis(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CATALOG_MATCH_BASIS_UNSPECIFIED: _ClassVar[CatalogMatchBasis]
    CATALOG_MATCH_BASIS_SOURCE_ATTACHMENT_ID: _ClassVar[CatalogMatchBasis]
    CATALOG_MATCH_BASIS_PROVIDER_FILE_ID: _ClassVar[CatalogMatchBasis]
    CATALOG_MATCH_BASIS_PROVIDER_FILE_PATH: _ClassVar[CatalogMatchBasis]
    CATALOG_MATCH_BASIS_LOCAL_PATH: _ClassVar[CatalogMatchBasis]
    CATALOG_MATCH_BASIS_UNRESTRICTED_URL: _ClassVar[CatalogMatchBasis]
    CATALOG_MATCH_BASIS_RESTRICTED_URL: _ClassVar[CatalogMatchBasis]
    CATALOG_MATCH_BASIS_LOCATOR: _ClassVar[CatalogMatchBasis]
    CATALOG_MATCH_BASIS_FILENAME_AND_SIZE: _ClassVar[CatalogMatchBasis]
    CATALOG_MATCH_BASIS_PROVIDER_DOWNLOAD_ID_AND_FILENAME: _ClassVar[CatalogMatchBasis]
    CATALOG_MATCH_BASIS_PROVIDER_DOWNLOAD_ID_AND_PROVIDER_FILE_PATH: _ClassVar[CatalogMatchBasis]
    CATALOG_MATCH_BASIS_PROVIDER_DOWNLOAD_ID_AND_FILE_SIZE: _ClassVar[CatalogMatchBasis]
CATALOG_ENTRY_KIND_UNSPECIFIED: CatalogEntryKind
CATALOG_ENTRY_KIND_DIRECTORY: CatalogEntryKind
CATALOG_ENTRY_KIND_FILE: CatalogEntryKind
CATALOG_MEDIA_TYPE_UNSPECIFIED: CatalogMediaType
CATALOG_MEDIA_TYPE_MOVIE: CatalogMediaType
CATALOG_MEDIA_TYPE_SHOW: CatalogMediaType
CATALOG_MEDIA_TYPE_SEASON: CatalogMediaType
CATALOG_MEDIA_TYPE_EPISODE: CatalogMediaType
CATALOG_MEDIA_TYPE_UNKNOWN: CatalogMediaType
CATALOG_FILE_TRANSPORT_UNSPECIFIED: CatalogFileTransport
CATALOG_FILE_TRANSPORT_LOCAL_FILE: CatalogFileTransport
CATALOG_FILE_TRANSPORT_REMOTE_DIRECT: CatalogFileTransport
CATALOG_LEASE_STATE_UNSPECIFIED: CatalogLeaseState
CATALOG_LEASE_STATE_READY: CatalogLeaseState
CATALOG_LEASE_STATE_STALE: CatalogLeaseState
CATALOG_LEASE_STATE_REFRESHING: CatalogLeaseState
CATALOG_LEASE_STATE_FAILED: CatalogLeaseState
CATALOG_LEASE_STATE_UNKNOWN: CatalogLeaseState
CATALOG_PLAYBACK_ROLE_UNSPECIFIED: CatalogPlaybackRole
CATALOG_PLAYBACK_ROLE_DIRECT: CatalogPlaybackRole
CATALOG_PLAYBACK_ROLE_HLS: CatalogPlaybackRole
CATALOG_PROVIDER_FAMILY_UNSPECIFIED: CatalogProviderFamily
CATALOG_PROVIDER_FAMILY_NONE: CatalogProviderFamily
CATALOG_PROVIDER_FAMILY_DEBRID: CatalogProviderFamily
CATALOG_PROVIDER_FAMILY_PROVIDER: CatalogProviderFamily
CATALOG_LOCATOR_SOURCE_UNSPECIFIED: CatalogLocatorSource
CATALOG_LOCATOR_SOURCE_LOCAL_PATH: CatalogLocatorSource
CATALOG_LOCATOR_SOURCE_UNRESTRICTED_URL: CatalogLocatorSource
CATALOG_LOCATOR_SOURCE_RESTRICTED_URL: CatalogLocatorSource
CATALOG_LOCATOR_SOURCE_LOCATOR: CatalogLocatorSource
CATALOG_MATCH_BASIS_UNSPECIFIED: CatalogMatchBasis
CATALOG_MATCH_BASIS_SOURCE_ATTACHMENT_ID: CatalogMatchBasis
CATALOG_MATCH_BASIS_PROVIDER_FILE_ID: CatalogMatchBasis
CATALOG_MATCH_BASIS_PROVIDER_FILE_PATH: CatalogMatchBasis
CATALOG_MATCH_BASIS_LOCAL_PATH: CatalogMatchBasis
CATALOG_MATCH_BASIS_UNRESTRICTED_URL: CatalogMatchBasis
CATALOG_MATCH_BASIS_RESTRICTED_URL: CatalogMatchBasis
CATALOG_MATCH_BASIS_LOCATOR: CatalogMatchBasis
CATALOG_MATCH_BASIS_FILENAME_AND_SIZE: CatalogMatchBasis
CATALOG_MATCH_BASIS_PROVIDER_DOWNLOAD_ID_AND_FILENAME: CatalogMatchBasis
CATALOG_MATCH_BASIS_PROVIDER_DOWNLOAD_ID_AND_PROVIDER_FILE_PATH: CatalogMatchBasis
CATALOG_MATCH_BASIS_PROVIDER_DOWNLOAD_ID_AND_FILE_SIZE: CatalogMatchBasis

class WatchCatalogRequest(_message.Message):
    __slots__ = ("subscribe", "ack", "heartbeat")
    SUBSCRIBE_FIELD_NUMBER: _ClassVar[int]
    ACK_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    subscribe: CatalogSubscribe
    ack: CatalogAck
    heartbeat: CatalogHeartbeat
    def __init__(self, subscribe: _Optional[_Union[CatalogSubscribe, _Mapping]] = ..., ack: _Optional[_Union[CatalogAck, _Mapping]] = ..., heartbeat: _Optional[_Union[CatalogHeartbeat, _Mapping]] = ...) -> None: ...

class CatalogSubscribe(_message.Message):
    __slots__ = ("daemon_id", "daemon_version", "last_applied_generation_id", "want_full_snapshot", "correlation")
    DAEMON_ID_FIELD_NUMBER: _ClassVar[int]
    DAEMON_VERSION_FIELD_NUMBER: _ClassVar[int]
    LAST_APPLIED_GENERATION_ID_FIELD_NUMBER: _ClassVar[int]
    WANT_FULL_SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_FIELD_NUMBER: _ClassVar[int]
    daemon_id: str
    daemon_version: str
    last_applied_generation_id: str
    want_full_snapshot: bool
    correlation: CatalogCorrelationKeys
    def __init__(self, daemon_id: _Optional[str] = ..., daemon_version: _Optional[str] = ..., last_applied_generation_id: _Optional[str] = ..., want_full_snapshot: bool = ..., correlation: _Optional[_Union[CatalogCorrelationKeys, _Mapping]] = ...) -> None: ...

class CatalogAck(_message.Message):
    __slots__ = ("event_id", "generation_id", "correlation")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    GENERATION_ID_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    generation_id: str
    correlation: CatalogCorrelationKeys
    def __init__(self, event_id: _Optional[str] = ..., generation_id: _Optional[str] = ..., correlation: _Optional[_Union[CatalogCorrelationKeys, _Mapping]] = ...) -> None: ...

class CatalogHeartbeat(_message.Message):
    __slots__ = ("correlation",)
    CORRELATION_FIELD_NUMBER: _ClassVar[int]
    correlation: CatalogCorrelationKeys
    def __init__(self, correlation: _Optional[_Union[CatalogCorrelationKeys, _Mapping]] = ...) -> None: ...

class RefreshCatalogEntryRequest(_message.Message):
    __slots__ = ("provider_file_id", "handle_key", "entry_id")
    PROVIDER_FILE_ID_FIELD_NUMBER: _ClassVar[int]
    HANDLE_KEY_FIELD_NUMBER: _ClassVar[int]
    ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    provider_file_id: str
    handle_key: str
    entry_id: str
    def __init__(self, provider_file_id: _Optional[str] = ..., handle_key: _Optional[str] = ..., entry_id: _Optional[str] = ...) -> None: ...

class RefreshCatalogEntryResponse(_message.Message):
    __slots__ = ("success", "new_url")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    NEW_URL_FIELD_NUMBER: _ClassVar[int]
    success: bool
    new_url: str
    def __init__(self, success: bool = ..., new_url: _Optional[str] = ...) -> None: ...

class WatchCatalogEvent(_message.Message):
    __slots__ = ("event_id", "published_at", "snapshot", "delta", "heartbeat", "problem")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    PUBLISHED_AT_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    DELTA_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    PROBLEM_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    published_at: _timestamp_pb2.Timestamp
    snapshot: CatalogSnapshot
    delta: CatalogDelta
    heartbeat: CatalogHeartbeat
    problem: CatalogProblem
    def __init__(self, event_id: _Optional[str] = ..., published_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., snapshot: _Optional[_Union[CatalogSnapshot, _Mapping]] = ..., delta: _Optional[_Union[CatalogDelta, _Mapping]] = ..., heartbeat: _Optional[_Union[CatalogHeartbeat, _Mapping]] = ..., problem: _Optional[_Union[CatalogProblem, _Mapping]] = ...) -> None: ...

class CatalogSnapshot(_message.Message):
    __slots__ = ("generation_id", "entries", "stats")
    GENERATION_ID_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    generation_id: str
    entries: _containers.RepeatedCompositeFieldContainer[CatalogEntry]
    stats: CatalogStats
    def __init__(self, generation_id: _Optional[str] = ..., entries: _Optional[_Iterable[_Union[CatalogEntry, _Mapping]]] = ..., stats: _Optional[_Union[CatalogStats, _Mapping]] = ...) -> None: ...

class CatalogDelta(_message.Message):
    __slots__ = ("generation_id", "base_generation_id", "upserts", "removals", "stats")
    GENERATION_ID_FIELD_NUMBER: _ClassVar[int]
    BASE_GENERATION_ID_FIELD_NUMBER: _ClassVar[int]
    UPSERTS_FIELD_NUMBER: _ClassVar[int]
    REMOVALS_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    generation_id: str
    base_generation_id: str
    upserts: _containers.RepeatedCompositeFieldContainer[CatalogEntry]
    removals: _containers.RepeatedCompositeFieldContainer[CatalogRemoval]
    stats: CatalogStats
    def __init__(self, generation_id: _Optional[str] = ..., base_generation_id: _Optional[str] = ..., upserts: _Optional[_Iterable[_Union[CatalogEntry, _Mapping]]] = ..., removals: _Optional[_Iterable[_Union[CatalogRemoval, _Mapping]]] = ..., stats: _Optional[_Union[CatalogStats, _Mapping]] = ...) -> None: ...

class CatalogStats(_message.Message):
    __slots__ = ("directory_count", "file_count", "blocked_item_count")
    DIRECTORY_COUNT_FIELD_NUMBER: _ClassVar[int]
    FILE_COUNT_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_ITEM_COUNT_FIELD_NUMBER: _ClassVar[int]
    directory_count: int
    file_count: int
    blocked_item_count: int
    def __init__(self, directory_count: _Optional[int] = ..., file_count: _Optional[int] = ..., blocked_item_count: _Optional[int] = ...) -> None: ...

class CatalogRemoval(_message.Message):
    __slots__ = ("entry_id", "path", "kind", "correlation")
    ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_FIELD_NUMBER: _ClassVar[int]
    entry_id: str
    path: str
    kind: CatalogEntryKind
    correlation: CatalogCorrelationKeys
    def __init__(self, entry_id: _Optional[str] = ..., path: _Optional[str] = ..., kind: _Optional[_Union[CatalogEntryKind, str]] = ..., correlation: _Optional[_Union[CatalogCorrelationKeys, _Mapping]] = ...) -> None: ...

class CatalogEntry(_message.Message):
    __slots__ = ("entry_id", "parent_entry_id", "path", "name", "kind", "correlation", "directory", "file")
    ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_FIELD_NUMBER: _ClassVar[int]
    DIRECTORY_FIELD_NUMBER: _ClassVar[int]
    FILE_FIELD_NUMBER: _ClassVar[int]
    entry_id: str
    parent_entry_id: str
    path: str
    name: str
    kind: CatalogEntryKind
    correlation: CatalogCorrelationKeys
    directory: DirectoryEntry
    file: FileEntry
    def __init__(self, entry_id: _Optional[str] = ..., parent_entry_id: _Optional[str] = ..., path: _Optional[str] = ..., name: _Optional[str] = ..., kind: _Optional[_Union[CatalogEntryKind, str]] = ..., correlation: _Optional[_Union[CatalogCorrelationKeys, _Mapping]] = ..., directory: _Optional[_Union[DirectoryEntry, _Mapping]] = ..., file: _Optional[_Union[FileEntry, _Mapping]] = ...) -> None: ...

class DirectoryEntry(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FileEntry(_message.Message):
    __slots__ = ("item_id", "item_title", "item_external_ref", "media_entry_id", "source_attachment_id", "media_type", "transport", "locator", "local_path", "restricted_url", "unrestricted_url", "original_filename", "size_bytes", "lease_state", "expires_at", "last_refreshed_at", "last_refresh_error", "provider", "provider_download_id", "provider_file_id", "provider_file_path", "active_roles", "source_key", "query_strategy", "provider_family", "locator_source", "match_basis", "restricted_fallback")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_TITLE_FIELD_NUMBER: _ClassVar[int]
    ITEM_EXTERNAL_REF_FIELD_NUMBER: _ClassVar[int]
    MEDIA_ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ATTACHMENT_ID_FIELD_NUMBER: _ClassVar[int]
    MEDIA_TYPE_FIELD_NUMBER: _ClassVar[int]
    TRANSPORT_FIELD_NUMBER: _ClassVar[int]
    LOCATOR_FIELD_NUMBER: _ClassVar[int]
    LOCAL_PATH_FIELD_NUMBER: _ClassVar[int]
    RESTRICTED_URL_FIELD_NUMBER: _ClassVar[int]
    UNRESTRICTED_URL_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_FILENAME_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    LEASE_STATE_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_REFRESHED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_REFRESH_ERROR_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_DOWNLOAD_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FILE_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ROLES_FIELD_NUMBER: _ClassVar[int]
    SOURCE_KEY_FIELD_NUMBER: _ClassVar[int]
    QUERY_STRATEGY_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FAMILY_FIELD_NUMBER: _ClassVar[int]
    LOCATOR_SOURCE_FIELD_NUMBER: _ClassVar[int]
    MATCH_BASIS_FIELD_NUMBER: _ClassVar[int]
    RESTRICTED_FALLBACK_FIELD_NUMBER: _ClassVar[int]
    item_id: str
    item_title: str
    item_external_ref: str
    media_entry_id: str
    source_attachment_id: str
    media_type: CatalogMediaType
    transport: CatalogFileTransport
    locator: str
    local_path: str
    restricted_url: str
    unrestricted_url: str
    original_filename: str
    size_bytes: int
    lease_state: CatalogLeaseState
    expires_at: _timestamp_pb2.Timestamp
    last_refreshed_at: _timestamp_pb2.Timestamp
    last_refresh_error: str
    provider: str
    provider_download_id: str
    provider_file_id: str
    provider_file_path: str
    active_roles: _containers.RepeatedScalarFieldContainer[CatalogPlaybackRole]
    source_key: str
    query_strategy: str
    provider_family: CatalogProviderFamily
    locator_source: CatalogLocatorSource
    match_basis: CatalogMatchBasis
    restricted_fallback: bool
    def __init__(self, item_id: _Optional[str] = ..., item_title: _Optional[str] = ..., item_external_ref: _Optional[str] = ..., media_entry_id: _Optional[str] = ..., source_attachment_id: _Optional[str] = ..., media_type: _Optional[_Union[CatalogMediaType, str]] = ..., transport: _Optional[_Union[CatalogFileTransport, str]] = ..., locator: _Optional[str] = ..., local_path: _Optional[str] = ..., restricted_url: _Optional[str] = ..., unrestricted_url: _Optional[str] = ..., original_filename: _Optional[str] = ..., size_bytes: _Optional[int] = ..., lease_state: _Optional[_Union[CatalogLeaseState, str]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_refreshed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_refresh_error: _Optional[str] = ..., provider: _Optional[str] = ..., provider_download_id: _Optional[str] = ..., provider_file_id: _Optional[str] = ..., provider_file_path: _Optional[str] = ..., active_roles: _Optional[_Iterable[_Union[CatalogPlaybackRole, str]]] = ..., source_key: _Optional[str] = ..., query_strategy: _Optional[str] = ..., provider_family: _Optional[_Union[CatalogProviderFamily, str]] = ..., locator_source: _Optional[_Union[CatalogLocatorSource, str]] = ..., match_basis: _Optional[_Union[CatalogMatchBasis, str]] = ..., restricted_fallback: bool = ...) -> None: ...

class CatalogCorrelationKeys(_message.Message):
    __slots__ = ("item_id", "media_entry_id", "source_attachment_id", "provider", "provider_download_id", "provider_file_id", "provider_file_path", "session_id", "handle_key")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    MEDIA_ENTRY_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ATTACHMENT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_DOWNLOAD_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FILE_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FILE_PATH_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    HANDLE_KEY_FIELD_NUMBER: _ClassVar[int]
    item_id: str
    media_entry_id: str
    source_attachment_id: str
    provider: str
    provider_download_id: str
    provider_file_id: str
    provider_file_path: str
    session_id: str
    handle_key: str
    def __init__(self, item_id: _Optional[str] = ..., media_entry_id: _Optional[str] = ..., source_attachment_id: _Optional[str] = ..., provider: _Optional[str] = ..., provider_download_id: _Optional[str] = ..., provider_file_id: _Optional[str] = ..., provider_file_path: _Optional[str] = ..., session_id: _Optional[str] = ..., handle_key: _Optional[str] = ...) -> None: ...

class CatalogProblem(_message.Message):
    __slots__ = ("code", "message", "retry_after_seconds")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_SECONDS_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    retry_after_seconds: float
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ..., retry_after_seconds: _Optional[float] = ...) -> None: ...
