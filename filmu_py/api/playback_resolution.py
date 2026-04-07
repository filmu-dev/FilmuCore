"""Reusable playback attachment/source resolution helpers for HTTP and future VFS consumers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast
from urllib.parse import urlparse

_SOURCE_URL_KEYS = (
    "stream_url",
    "download_url",
    "source_url",
    "url",
    "streamUrl",
    "downloadUrl",
    "sourceUrl",
)
_SOURCE_PATH_KEYS = ("file_path", "local_path", "path", "filePath", "localPath")
_HLS_URL_KEYS = ("hls_url", "hlsUrl", "playlist_url", "playlistUrl")
_PREFERRED_SOURCE_CONTAINER_KEYS = (
    "active_stream",
    "activeStream",
    "selected_stream",
    "selectedStream",
    "current_stream",
    "currentStream",
    "primary_stream",
    "primaryStream",
)
_SOURCE_COLLECTION_CONTAINER_KEYS = (
    "streams",
    "streamsByProvider",
    "stream_list",
    "streamList",
    "sources",
    "source_list",
    "sourceList",
    "media",
    "playback",
)
_PREFERRED_SOURCE_FLAG_KEYS = (
    "selected",
    "is_selected",
    "isSelected",
    "active",
    "is_active",
    "isActive",
    "default",
    "is_default",
    "isDefault",
    "primary",
    "is_primary",
    "isPrimary",
    "current",
    "is_current",
    "isCurrent",
)
_PREFERRED_SOURCE_CONTAINER_BONUS = 200
_SOURCE_COLLECTION_CONTAINER_BONUS = 25
_PREFERRED_SOURCE_FLAG_BONUS = 100
_PATH_SOURCE_BONUS = 20
_URL_SOURCE_BONUS = 10
_ATTACHMENT_PROVIDER_KEYS = (
    "provider",
    "provider_key",
    "providerKey",
    "service",
    "debrid_service",
    "debridService",
)
_ATTACHMENT_PROVIDER_DOWNLOAD_ID_KEYS = (
    "provider_download_id",
    "providerDownloadId",
    "download_id",
    "downloadId",
    "torrent_id",
    "torrentId",
)
_ATTACHMENT_PROVIDER_FILE_ID_KEYS = (
    "provider_file_id",
    "providerFileId",
    "file_id",
    "fileId",
)
_ATTACHMENT_PROVIDER_FILE_PATH_KEYS = (
    "provider_file_path",
    "providerFilePath",
    "torrent_path",
    "torrentPath",
    "stream_path",
    "streamPath",
)
_ATTACHMENT_FILENAME_KEYS = (
    "original_filename",
    "originalFilename",
    "filename",
    "file_name",
    "fileName",
    "name",
)
_ATTACHMENT_FILE_SIZE_KEYS = (
    "file_size",
    "fileSize",
    "filesize",
    "size_bytes",
    "sizeBytes",
    "size",
)
_ATTACHMENT_UNRESTRICTED_URL_KEYS = (
    "unrestricted_url",
    "unrestrictedUrl",
    "stream_url",
    "streamUrl",
    "unrestricted_link",
    "unrestrictedLink",
)
_ATTACHMENT_REFRESH_STATE_KEYS = ("refresh_state", "refreshState")
_ATTACHMENT_RESTRICTED_URL_KEYS = (
    "download_url",
    "downloadUrl",
    "source_url",
    "sourceUrl",
    "url",
)

PlaybackAttachmentKind = Literal["local-file", "remote-direct", "remote-hls"]
PlaybackAttachmentRefreshState = Literal["ready", "stale", "refreshing", "failed"]
DirectPlaybackSourceClass = Literal[
    "selected-local-file",
    "selected-provider-direct-ready",
    "selected-provider-direct-stale",
    "selected-provider-direct-refreshing",
    "selected-provider-direct-failed",
    "selected-provider-direct-degraded",
    "selected-generic-direct",
    "fallback-local-file",
    "fallback-provider-direct-ready",
    "fallback-provider-direct-stale",
    "fallback-provider-direct-refreshing",
    "fallback-provider-direct-failed",
    "fallback-provider-direct-degraded",
    "fallback-generic-direct",
    "selected-degraded-direct",
    "fallback-degraded-direct",
]


@dataclass(frozen=True, slots=True)
class SourceCandidate:
    """One ranked playback/HLS source candidate extracted from flexible item metadata."""

    kind: str
    value: str
    priority: int
    authoritative: bool
    ordinal: int
    source_key: str
    context: dict[str, object]


@dataclass(frozen=True, slots=True)
class PlaybackAttachment:
    """One playback attachment/link resolved from flexible item metadata."""

    kind: PlaybackAttachmentKind
    locator: str
    source_key: str
    resolver_priority: int = 0
    resolver_authoritative: bool = False
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    original_filename: str | None = None
    file_size: int | None = None
    local_path: str | None = None
    restricted_url: str | None = None
    unrestricted_url: str | None = None
    expires_at: datetime | None = None
    refresh_state: PlaybackAttachmentRefreshState | None = None


def _coerce_truthy_flag(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "selected", "active"}
    return False


def _extract_first_string(payload: dict[str, object], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_first_identifier(payload: dict[str, object], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return str(value)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _extract_first_int(payload: dict[str, object], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            text = value.strip()
            if text.isdigit():
                return int(text)
    return None


def _is_preferred_source_node(node: dict[str, object]) -> bool:
    return any(_coerce_truthy_flag(node.get(key)) for key in _PREFERRED_SOURCE_FLAG_KEYS)


def extract_sources(
    payload: object,
    *,
    path_keys: tuple[str, ...] = _SOURCE_PATH_KEYS,
    url_keys: tuple[str, ...] = _SOURCE_URL_KEYS,
) -> list[SourceCandidate]:
    """Return ranked playback source candidates from one flexible metadata payload."""

    candidates: list[SourceCandidate] = []
    ordinal = [0]

    def register_candidate(
        *,
        kind: str,
        value: str,
        priority: int,
        authoritative: bool,
        source_key: str,
        context: dict[str, object],
    ) -> None:
        ordinal[0] += 1
        candidates.append(
            SourceCandidate(
                kind=kind,
                value=value.strip(),
                priority=priority,
                authoritative=authoritative,
                ordinal=ordinal[0],
                source_key=source_key,
                context=dict(context),
            )
        )

    def visit(
        node: object,
        *,
        inherited_priority: int = 0,
        inherited_authority: bool = False,
        via_key: str | None = None,
    ) -> None:
        container_priority = inherited_priority
        authoritative = inherited_authority
        if via_key in _PREFERRED_SOURCE_CONTAINER_KEYS:
            container_priority += _PREFERRED_SOURCE_CONTAINER_BONUS
            authoritative = True
        elif via_key in _SOURCE_COLLECTION_CONTAINER_KEYS:
            container_priority += _SOURCE_COLLECTION_CONTAINER_BONUS

        if isinstance(node, dict):
            node_priority = container_priority
            if _is_preferred_source_node(node):
                node_priority += _PREFERRED_SOURCE_FLAG_BONUS
                authoritative = True

            for key in path_keys:
                value = node.get(key)
                if isinstance(value, str) and value.strip():
                    register_candidate(
                        kind="path",
                        value=value,
                        priority=node_priority + _PATH_SOURCE_BONUS,
                        authoritative=authoritative,
                        source_key=key,
                        context=node,
                    )

            for key in url_keys:
                value = node.get(key)
                if isinstance(value, str) and value.strip():
                    register_candidate(
                        kind="url",
                        value=value,
                        priority=node_priority + _URL_SOURCE_BONUS,
                        authoritative=authoritative,
                        source_key=key,
                        context=node,
                    )

            for child_key, value in node.items():
                if isinstance(value, (dict, list)):
                    visit(
                        value,
                        inherited_priority=node_priority,
                        inherited_authority=authoritative,
                        via_key=child_key,
                    )

        elif isinstance(node, list):
            for value in node:
                if isinstance(value, (dict, list)):
                    visit(
                        value,
                        inherited_priority=container_priority,
                        inherited_authority=authoritative,
                        via_key=via_key,
                    )

    visit(payload)

    ordered = sorted(
        candidates,
        key=lambda candidate: (
            -candidate.priority,
            0 if candidate.kind == "path" else 1,
            candidate.ordinal,
        ),
    )

    deduplicated: list[SourceCandidate] = []
    seen: set[tuple[str, str]] = set()
    for candidate in ordered:
        key = (candidate.kind, candidate.value)
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(candidate)
    return deduplicated


def is_hls_playlist_url(url: str) -> bool:
    """Return whether one URL appears to point at an HLS playlist."""

    return urlparse(url).path.lower().endswith(".m3u8")


def build_playback_attachment(
    candidate: SourceCandidate,
    *,
    fallback_context: dict[str, object] | None = None,
) -> PlaybackAttachment:
    """Convert one ranked candidate into a richer playback attachment/link record."""

    def prefer_string(keys: tuple[str, ...]) -> str | None:
        if fallback_context is not None:
            nested = _extract_first_string(candidate.context, keys)
            if nested is not None:
                return nested
            return _extract_first_string(fallback_context, keys)
        return _extract_first_string(candidate.context, keys)

    def prefer_identifier(keys: tuple[str, ...]) -> str | None:
        if fallback_context is not None:
            nested = _extract_first_identifier(candidate.context, keys)
            if nested is not None:
                return nested
            return _extract_first_identifier(fallback_context, keys)
        return _extract_first_identifier(candidate.context, keys)

    def prefer_int(keys: tuple[str, ...]) -> int | None:
        if fallback_context is not None:
            nested = _extract_first_int(candidate.context, keys)
            if nested is not None:
                return nested
            return _extract_first_int(fallback_context, keys)
        return _extract_first_int(candidate.context, keys)

    provider = prefer_string(_ATTACHMENT_PROVIDER_KEYS)
    provider_download_id = prefer_identifier(_ATTACHMENT_PROVIDER_DOWNLOAD_ID_KEYS)
    provider_file_id = prefer_identifier(_ATTACHMENT_PROVIDER_FILE_ID_KEYS)
    provider_file_path = prefer_string(_ATTACHMENT_PROVIDER_FILE_PATH_KEYS)
    original_filename = prefer_string(_ATTACHMENT_FILENAME_KEYS)
    file_size = prefer_int(_ATTACHMENT_FILE_SIZE_KEYS)
    restricted_url = prefer_string(_ATTACHMENT_RESTRICTED_URL_KEYS)
    unrestricted_url = prefer_string(_ATTACHMENT_UNRESTRICTED_URL_KEYS)
    refresh_state_value = prefer_string(_ATTACHMENT_REFRESH_STATE_KEYS)
    refresh_state: PlaybackAttachmentRefreshState | None = None
    if refresh_state_value is not None:
        normalized_refresh_state = refresh_state_value.strip().casefold()
        if normalized_refresh_state in {"ready", "stale", "refreshing", "failed"}:
            refresh_state = cast(PlaybackAttachmentRefreshState, normalized_refresh_state)

    if candidate.kind == "path":
        path = Path(candidate.value)
        if not path.is_file():
            raise FileNotFoundError(candidate.value)
        return PlaybackAttachment(
            kind="local-file",
            locator=str(path),
            source_key=candidate.source_key,
            resolver_priority=candidate.priority,
            resolver_authoritative=candidate.authoritative,
            provider=provider,
            provider_download_id=provider_download_id,
            provider_file_id=provider_file_id,
            provider_file_path=provider_file_path,
            original_filename=original_filename,
            file_size=file_size,
            local_path=str(path),
            restricted_url=restricted_url,
            unrestricted_url=unrestricted_url,
            expires_at=None,
            refresh_state=refresh_state,
        )

    if is_hls_playlist_url(candidate.value):
        return PlaybackAttachment(
            kind="remote-hls",
            locator=candidate.value,
            source_key=candidate.source_key,
            resolver_priority=candidate.priority,
            resolver_authoritative=candidate.authoritative,
            provider=provider,
            provider_download_id=provider_download_id,
            provider_file_id=provider_file_id,
            provider_file_path=provider_file_path,
            original_filename=original_filename,
            file_size=file_size,
            restricted_url=restricted_url,
            unrestricted_url=unrestricted_url,
            expires_at=None,
            refresh_state=refresh_state,
        )

    return PlaybackAttachment(
        kind="remote-direct",
        locator=candidate.value,
        source_key=candidate.source_key,
        resolver_priority=candidate.priority,
        resolver_authoritative=candidate.authoritative,
        provider=provider,
        provider_download_id=provider_download_id,
        provider_file_id=provider_file_id,
        provider_file_path=provider_file_path,
        original_filename=original_filename,
        file_size=file_size,
        restricted_url=restricted_url,
        unrestricted_url=unrestricted_url,
        expires_at=None,
        refresh_state=refresh_state,
    )


def resolve_attachments_from_attributes(
    attributes: dict[str, object],
) -> tuple[list[PlaybackAttachment], bool]:
    """Return playback attachments plus whether metadata referenced a missing local file."""

    resolved: list[PlaybackAttachment] = []
    saw_missing_path = False
    seen: set[tuple[PlaybackAttachmentKind, str]] = set()

    def add_source(source: PlaybackAttachment) -> None:
        key = (source.kind, source.locator)
        if key in seen:
            return
        seen.add(key)
        resolved.append(source)

    for candidate in extract_sources(attributes, path_keys=(), url_keys=_HLS_URL_KEYS):
        add_source(build_playback_attachment(candidate, fallback_context=attributes))

    for candidate in extract_sources(attributes):
        try:
            add_source(build_playback_attachment(candidate, fallback_context=attributes))
        except FileNotFoundError:
            saw_missing_path = True
    return resolved, saw_missing_path


def select_direct_playback_attachment(
    attachments: list[PlaybackAttachment],
) -> PlaybackAttachment | None:
    """Return the best attachment for the direct-byte playback route."""

    ranked: list[tuple[tuple[int, int, int, float, int], int, PlaybackAttachment]] = []
    for index, attachment in enumerate(attachments):
        if attachment.kind == "remote-hls":
            continue
        ranked.append((_direct_attachment_priority(attachment), index, attachment))
    if not ranked:
        return None
    ranked.sort(key=lambda candidate: (candidate[0], candidate[1]))
    return ranked[0][2]


def classify_direct_playback_source_class(
    attachment: PlaybackAttachment,
) -> DirectPlaybackSourceClass:
    """Return the named direct-play source class for one attachment candidate."""

    authority_prefix = "selected" if attachment.resolver_authoritative else "fallback"
    if attachment.kind == "local-file":
        return cast(DirectPlaybackSourceClass, f"{authority_prefix}-local-file")
    if _is_provider_direct_attachment(attachment):
        return cast(
            DirectPlaybackSourceClass,
            f"{authority_prefix}-provider-direct-{_classify_provider_direct_health_suffix(attachment)}",
        )
    if _is_degraded_direct_attachment(attachment):
        return cast(DirectPlaybackSourceClass, f"{authority_prefix}-degraded-direct")
    return cast(DirectPlaybackSourceClass, f"{authority_prefix}-generic-direct")


def _classify_provider_direct_health_suffix(
    attachment: PlaybackAttachment,
) -> Literal["ready", "stale", "refreshing", "failed", "degraded"]:
    """Return the named health suffix for one provider-backed direct attachment."""

    if attachment.refresh_state == "failed":
        return "failed"
    if attachment.refresh_state == "refreshing":
        return "refreshing"
    if attachment.refresh_state == "stale":
        return "stale"
    if _is_degraded_direct_attachment(attachment):
        return "degraded"
    return "ready"


def _direct_attachment_priority(attachment: PlaybackAttachment) -> tuple[int, int, int, float, int]:
    """Return the relative preference for one direct-play attachment candidate."""

    source_class = classify_direct_playback_source_class(attachment)
    source_class_rank = {
        "selected-local-file": 0,
        "selected-provider-direct-ready": 1,
        "selected-generic-direct": 2,
        "fallback-local-file": 3,
        "fallback-provider-direct-ready": 4,
        "fallback-generic-direct": 5,
        "selected-provider-direct-stale": 6,
        "selected-provider-direct-refreshing": 7,
        "selected-provider-direct-failed": 8,
        "selected-provider-direct-degraded": 9,
        "fallback-provider-direct-stale": 10,
        "fallback-provider-direct-refreshing": 11,
        "fallback-provider-direct-failed": 12,
        "fallback-provider-direct-degraded": 13,
        "selected-degraded-direct": 14,
        "fallback-degraded-direct": 15,
    }[source_class]

    if source_class.endswith("ready"):
        return (
            source_class_rank,
            0,
            0 if attachment.expires_at is not None else 1,
            -_expires_timestamp(attachment.expires_at),
            -_provider_identity_score(attachment),
        )
    return (source_class_rank, 0, 0, 0.0, 0)


def _is_provider_direct_attachment(attachment: PlaybackAttachment) -> bool:
    """Return whether one direct-play attachment preserves provider-side identity."""

    if attachment.kind != "remote-direct":
        return False
    return any(
        value is not None and value != ""
        for value in (
            attachment.provider,
            attachment.provider_download_id,
            attachment.provider_file_id,
            attachment.provider_file_path,
        )
    )


def _provider_identity_score(attachment: PlaybackAttachment) -> int:
    """Return how much provider-native identity one attachment preserves."""

    return sum(
        1
        for value in (
            attachment.provider,
            attachment.provider_download_id,
            attachment.provider_file_id,
            attachment.provider_file_path,
        )
        if value is not None and value != ""
    )


def _expires_timestamp(expires_at: datetime | None) -> float:
    """Return a comparable UTC timestamp for direct-source freshness ranking."""

    if expires_at is None:
        return 0.0
    return expires_at.astimezone(UTC).timestamp()


def _is_provider_backed_unrestricted_direct_attachment(attachment: PlaybackAttachment) -> bool:
    """Return whether one direct-play attachment is a provider-backed unrestricted direct URL."""

    if attachment.kind != "remote-direct":
        return False
    if attachment.unrestricted_url is None or attachment.locator != attachment.unrestricted_url:
        return False
    return any(
        value is not None and value != ""
        for value in (
            attachment.provider,
            attachment.provider_download_id,
            attachment.provider_file_id,
            attachment.provider_file_path,
        )
    )


def _is_degraded_direct_attachment(attachment: PlaybackAttachment) -> bool:
    """Return whether one direct-play attachment is only a restricted-link fallback."""

    if attachment.kind != "remote-direct":
        return False
    if attachment.source_key.endswith(":restricted-fallback"):
        return True
    if attachment.restricted_url is None:
        return False
    if attachment.locator != attachment.restricted_url:
        return False
    if attachment.unrestricted_url is None:
        return False
    return attachment.unrestricted_url != attachment.restricted_url


def select_hls_playback_attachment(
    attachments: list[PlaybackAttachment],
) -> PlaybackAttachment | None:
    """Return the best attachment for the HLS route family."""

    for preferred_kind in ("remote-hls", "local-file"):
        for attachment in attachments:
            if attachment.kind == preferred_kind:
                return attachment
    direct_attachment = select_direct_playback_attachment(attachments)
    if direct_attachment is not None and direct_attachment.kind == "remote-direct":
        return direct_attachment
    return None
