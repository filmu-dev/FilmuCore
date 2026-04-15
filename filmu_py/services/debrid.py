"""Built-in debrid-service clients for persisted playback refresh execution."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, Protocol, cast, runtime_checkable

import httpx

from filmu_py.config import DownloadersSettings, Settings
from filmu_py.services.playback import (
    PlaybackAttachmentProviderClient,
    PlaybackAttachmentProviderFileProjection,
    PlaybackAttachmentProviderUnrestrictedLink,
    PlaybackAttachmentRefreshRequest,
    PlaybackRefreshRateLimiter,
    PlaybackSourceService,
)

logger = logging.getLogger(__name__)

_ALLDEBRID_BASE_URL = "https://api.alldebrid.com"
_DEBRIDLINK_BASE_URL = "https://debrid-link.com/api/v2"
_REALDEBRID_BASE_URL = "https://api.real-debrid.com/rest/1.0"
_DEFAULT_HTTPX_LIMITS = httpx.Limits(max_connections=200, max_keepalive_connections=50)


class DebridRateLimitError(RuntimeError):
    """Transient debrid-provider rate-limit signal for worker retry handling."""

    def __init__(self, *, provider: str, retry_after_seconds: float | None = None) -> None:
        self.provider = provider
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"download_rate_limited:{provider}")


@dataclass(frozen=True)
class TorrentFile:
    """Normalized provider-side torrent file record for the download pipeline."""

    file_id: str
    file_name: str
    file_path: str | None = None
    file_size_bytes: int | None = None
    selected: bool = False
    download_url: str | None = None
    media_type: str | None = None


@dataclass(frozen=True)
class TorrentInfo:
    """Normalized provider-side torrent/container status for the download pipeline."""

    provider_torrent_id: str
    status: str
    name: str | None = None
    info_hash: str | None = None
    files: list[TorrentFile] = field(default_factory=list)
    links: list[str] = field(default_factory=list)


@runtime_checkable
class DebridDownloadClient(Protocol):
    """Structural interface for debrid provider clients used in the download pipeline."""

    async def add_magnet(self, magnet_url: str) -> str: ...
    async def get_torrent_info(self, provider_torrent_id: str) -> TorrentInfo: ...
    async def select_files(self, provider_torrent_id: str, file_ids: list[str]) -> None: ...
    async def get_download_links(self, provider_torrent_id: str) -> list[str]: ...


@dataclass(slots=True)
class PluginDownloaderClientAdapter:
    """Adapt one registered downloader plugin into the debrid worker client contract."""

    provider: str
    plugin: Any

    async def add_magnet(self, magnet_url: str) -> str:
        result = await self.plugin.add_magnet(SimpleNamespace(magnet_url=magnet_url))
        return result.download_id

    async def get_torrent_info(self, provider_torrent_id: str) -> TorrentInfo:
        status = await self.plugin.get_status(SimpleNamespace(download_id=provider_torrent_id))
        files = [
            TorrentFile(
                file_id=file.file_id,
                file_name=file.path.rsplit("/", 1)[-1],
                file_path=file.path,
                file_size_bytes=file.size_bytes,
                selected=file.selected,
                download_url=file.download_url,
                media_type=_infer_media_type(file.path),
            )
            for file in status.files
        ]
        links = [file.download_url for file in status.files if file.download_url]
        return TorrentInfo(
            provider_torrent_id=status.download_id,
            status=status.status,
            files=files,
            links=links,
        )

    async def select_files(self, provider_torrent_id: str, file_ids: list[str]) -> None:
        selection_method = getattr(self.plugin, "select_files", None)
        if callable(selection_method):
            await cast(Any, selection_method)(provider_torrent_id, file_ids)
        return None

    async def get_download_links(self, provider_torrent_id: str) -> list[str]:
        results = await self.plugin.get_download_links(
            SimpleNamespace(download_id=provider_torrent_id, file_ids=())
        )
        return [result.url for result in results if result.url]


def _build_download_rate_limit_bucket_key(provider: str) -> str:
    return f"ratelimit:{provider}:download"


async def _acquire_download_rate_limit(
    *,
    provider: str,
    limiter: PlaybackRefreshRateLimiter | None,
) -> None:
    if limiter is None:
        return
    decision = await limiter.acquire(
        bucket_key=_build_download_rate_limit_bucket_key(provider),
        capacity=10.0,
        refill_rate_per_second=1.0,
        expiry_seconds=60,
    )
    if not decision.allowed:
        retry_after_seconds = decision.retry_after_seconds
        raise DebridRateLimitError(
            provider=provider,
            retry_after_seconds=retry_after_seconds if retry_after_seconds > 0 else None,
        )


def _infer_media_type(file_name: str) -> str:
    return (
        "episode" if re.search(r"\bs\d{1,2}e\d{1,2}\b", file_name, flags=re.IGNORECASE) else "movie"
    )


def filter_torrent_files(
    files: list[TorrentFile], settings: DownloadersSettings
) -> list[TorrentFile]:
    """Filter torrent files by extension and configured movie/episode filesize rules."""

    allowed_extensions = {
        extension.lower().removeprefix(".") for extension in settings.video_extensions
    }
    filtered: list[TorrentFile] = []
    for file in files:
        identity_path = (
            getattr(file, "file_path", None)
            or getattr(file, "file_name", None)
            or ""
        )
        extension = identity_path.rsplit(".", 1)[-1].lower() if "." in identity_path else ""
        if extension not in allowed_extensions:
            continue

        media_type = getattr(file, "media_type", None) or _infer_media_type(identity_path)
        size_mb = None
        size_bytes = getattr(file, "file_size_bytes", None)
        if size_bytes is not None:
            size_mb = float(size_bytes) / (1024 * 1024)
        if media_type == "episode":
            min_mb = settings.episode_filesize_mb_min
            max_mb = settings.episode_filesize_mb_max
        else:
            min_mb = settings.movie_filesize_mb_min
            max_mb = settings.movie_filesize_mb_max
        if size_mb is not None and size_mb < min_mb:
            continue
        if size_mb is not None and max_mb != -1 and size_mb > max_mb:
            continue
        filtered.append(file)
    return filtered


def _normalize_torrent_file(
    file_payload: dict[str, Any],
    *,
    download_url: str | None = None,
) -> TorrentFile | None:
    file_id_value = file_payload.get("id") or file_payload.get("fileId") or file_payload.get("uuid")
    path_value = (
        file_payload.get("path")
        or file_payload.get("name")
        or file_payload.get("filename")
        or file_payload.get("fileName")
        or file_payload.get("n")  # AllDebrid uses 'n' for filename
    )
    if not isinstance(path_value, str) or not path_value:
        return None
    normalized_path = path_value.strip()
    if not normalized_path:
        return None
    file_size_value = (
        file_payload.get("bytes")
        or file_payload.get("size")
        or file_payload.get("downloadSize")
        or file_payload.get("s")  # AllDebrid uses 's' for size in bytes
    )
    size_bytes = file_size_value if isinstance(file_size_value, int) else None
    selected_value = file_payload.get("selected")
    selected = selected_value in {True, "1", "true", "True"}
    file_id = str(file_id_value) if file_id_value is not None else path_value
    return TorrentFile(
        file_id=file_id,
        file_name=normalized_path.rsplit("/", 1)[-1],
        file_path=normalized_path,
        file_size_bytes=size_bytes,
        selected=selected,
        download_url=download_url,
        media_type=_infer_media_type(normalized_path),
    )


def _flatten_alldebrid_files(
    file_payloads: list[Any],
    _parent_path: str = "",
) -> list[tuple[dict[str, Any], str | None]]:
    """Recursively flatten AllDebrid nested file structures into (file_dict, link) pairs.

    AllDebrid returns season packs as nested directory objects:
      [{"n": "Season 1", "e": [{"n": "ep01.mkv", "l": "https://...", "s": 1234}, ...]}, ...]

    Leaf files carry their own link ('l') field. Top-level 'links' array is unreliable
    for nested packs and must not be used for index-based pairing.

    The full nested path is reconstructed and injected as 'path' in each leaf dict so
    that _normalize_torrent_file uses it as the file_id fallback, preventing collisions
    when multiple episodes share the same basename across different season directories.
    """
    result: list[tuple[dict[str, Any], str | None]] = []
    for entry in file_payloads:
        if not isinstance(entry, dict):
            continue
        entry_name = entry.get("n") or ""
        full_path = f"{_parent_path}/{entry_name}" if _parent_path else entry_name
        sub_entries = entry.get("e")
        if isinstance(sub_entries, list):
            # Directory node — recurse, threading the accumulated path
            result.extend(_flatten_alldebrid_files(sub_entries, _parent_path=full_path))
        else:
            # Leaf file — inject the full nested path so file_id fallback is unique
            leaf = dict(entry)
            leaf["path"] = full_path  # used by _normalize_torrent_file as file_id fallback
            link = entry.get("l")
            result.append((leaf, link if isinstance(link, str) and link else None))
    return result



def _build_async_client_kwargs(
    *,
    base_url: str,
    headers: dict[str, str],
    timeout_seconds: float,
    transport: httpx.AsyncBaseTransport | None,
    limits: httpx.Limits | None,
) -> dict[str, Any]:
    """Build a consistent `httpx.AsyncClient` configuration for provider clients."""

    return {
        "base_url": base_url,
        "headers": headers,
        "follow_redirects": True,
        "timeout": timeout_seconds,
        "transport": transport,
        "limits": limits or _DEFAULT_HTTPX_LIMITS,
    }


@dataclass(slots=True)
class RealDebridPlaybackClient:
    """Minimal built-in Real-Debrid client for playback-link unrestriction."""

    api_token: str
    base_url: str = _REALDEBRID_BASE_URL
    timeout_seconds: float = 10.0
    transport: httpx.AsyncBaseTransport | None = None
    limits: httpx.Limits | None = None
    limiter: PlaybackRefreshRateLimiter | None = None

    async def unrestrict_link(
        self,
        link: str,
        *,
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
        """Resolve one restricted Real-Debrid link into a direct playback URL."""

        headers = {"Authorization": f"Bearer {self.api_token}"}
        try:
            async with httpx.AsyncClient(
                **_build_async_client_kwargs(
                    base_url=self.base_url,
                    headers=headers,
                    timeout_seconds=self.timeout_seconds,
                    transport=self.transport,
                    limits=self.limits,
                )
            ) as client:
                response = await client.post("/unrestrict/link", data={"link": link})
        except httpx.HTTPError as exc:
            logger.warning(
                "Real-Debrid unrestrict request failed for attachment %s: %s",
                request.attachment_id,
                exc,
            )
            return None

        if not response.is_success:
            logger.warning(
                "Real-Debrid unrestrict failed for attachment %s with status %s",
                request.attachment_id,
                response.status_code,
            )
            return None

        try:
            payload = response.json()
        except ValueError:
            logger.warning(
                "Real-Debrid unrestrict returned non-JSON data for attachment %s",
                request.attachment_id,
            )
            return None

        download_url = payload.get("download")
        if not isinstance(download_url, str) or not download_url:
            logger.warning(
                "Real-Debrid unrestrict returned no download URL for attachment %s",
                request.attachment_id,
            )
            return None

        return PlaybackAttachmentProviderUnrestrictedLink(
            download_url=download_url,
            restricted_url=link,
        )

    async def add_magnet(self, magnet_url: str) -> str:
        await _acquire_download_rate_limit(provider="realdebrid", limiter=self.limiter)
        headers = {"Authorization": f"Bearer {self.api_token}"}
        async with httpx.AsyncClient(
            **_build_async_client_kwargs(
                base_url=self.base_url,
                headers=headers,
                timeout_seconds=self.timeout_seconds,
                transport=self.transport,
                limits=self.limits,
            )
        ) as client:
            response = await client.post("/torrents/addMagnet", data={"magnet": magnet_url})
        response.raise_for_status()
        payload = response.json()
        torrent_id = payload.get("id") if isinstance(payload, dict) else None
        if not isinstance(torrent_id, str) or not torrent_id:
            raise RuntimeError("realdebrid_add_magnet_failed")
        return torrent_id

    async def get_torrent_info(self, provider_torrent_id: str) -> TorrentInfo:
        await _acquire_download_rate_limit(provider="realdebrid", limiter=self.limiter)
        headers = {"Authorization": f"Bearer {self.api_token}"}
        async with httpx.AsyncClient(
            **_build_async_client_kwargs(
                base_url=self.base_url,
                headers=headers,
                timeout_seconds=self.timeout_seconds,
                transport=self.transport,
                limits=self.limits,
            )
        ) as client:
            response = await client.get(f"/torrents/info/{provider_torrent_id}")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("realdebrid_torrent_info_invalid")
        files_payload = payload.get("files")
        links_payload = payload.get("links")
        files: list[TorrentFile] = []
        file_payload_list = (
            [file_payload for file_payload in files_payload if isinstance(file_payload, dict)]
            if isinstance(files_payload, list)
            else []
        )
        links = (
            [link for link in links_payload if isinstance(link, str) and link]
            if isinstance(links_payload, list)
            else []
        )
        selected_index: int = 0
        for file_payload in file_payload_list:
            download_url = None
            if file_payload.get("selected") == 1 and selected_index < len(links):
                download_url = links[selected_index]
                selected_index += 1
            normalized = _normalize_torrent_file(
                file_payload,
                download_url=download_url,
            )
            if normalized is not None:
                files.append(normalized)
        return TorrentInfo(
            provider_torrent_id=str(payload.get("id") or provider_torrent_id),
            status=str(payload.get("status") or "unknown"),
            name=payload.get("filename") if isinstance(payload.get("filename"), str) else None,
            info_hash=payload.get("hash") if isinstance(payload.get("hash"), str) else None,
            files=files,
            links=links,
        )

    async def select_files(self, provider_torrent_id: str, file_ids: list[str]) -> None:
        await _acquire_download_rate_limit(provider="realdebrid", limiter=self.limiter)
        headers = {"Authorization": f"Bearer {self.api_token}"}
        async with httpx.AsyncClient(
            **_build_async_client_kwargs(
                base_url=self.base_url,
                headers=headers,
                timeout_seconds=self.timeout_seconds,
                transport=self.transport,
                limits=self.limits,
            )
        ) as client:
            response = await client.post(
                f"/torrents/selectFiles/{provider_torrent_id}",
                data={"files": ",".join(file_ids) if file_ids else "all"},
            )
        response.raise_for_status()

    async def get_download_links(self, provider_torrent_id: str) -> list[str]:
        info = await self.get_torrent_info(provider_torrent_id)
        return [link for link in info.links if link]

    async def get_user_info(self) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.api_token}"}
        async with httpx.AsyncClient(timeout=8.0, headers=headers) as client:
            response = await client.get(f"{self.base_url}/user")
        response.raise_for_status()
        return cast(dict[str, Any], response.json())

    @staticmethod
    def _build_file_projections(
        request: PlaybackAttachmentRefreshRequest,
        *,
        files: list[dict[str, Any]],
        links: list[str],
    ) -> list[PlaybackAttachmentProviderFileProjection]:
        selected_files = [
            file for file in files if isinstance(file, dict) and file.get("selected") == 1
        ]
        projections: list[PlaybackAttachmentProviderFileProjection] = []

        for index in range(min(len(selected_files), len(links))):
            link = links[index]
            if not isinstance(link, str) or not link:
                continue
            file = selected_files[index]
            path = file.get("path")
            bytes_value = file.get("bytes")
            candidate_path = path if isinstance(path, str) else None
            candidate_size = bytes_value if isinstance(bytes_value, int) else None
            file_id = file.get("id")
            provider_file_id = (
                str(file_id)
                if isinstance(file_id, (int, str)) and not isinstance(file_id, bool)
                else None
            )
            original_filename = None
            if candidate_path:
                original_filename = candidate_path.rsplit("/", 1)[-1]
            projections.append(
                PlaybackAttachmentProviderFileProjection(
                    provider=request.provider,
                    provider_download_id=request.provider_download_id,
                    provider_file_id=provider_file_id,
                    provider_file_path=candidate_path,
                    original_filename=original_filename,
                    file_size=candidate_size,
                    restricted_url=link,
                )
            )

        return projections

    async def project_download_attachments(
        self,
        *,
        request: PlaybackAttachmentRefreshRequest,
    ) -> list[PlaybackAttachmentProviderFileProjection]:
        """Project provider-side file records for one Real-Debrid download identifier."""

        torrent_id = request.provider_download_id
        if not torrent_id:
            return []

        headers = {"Authorization": f"Bearer {self.api_token}"}
        try:
            async with httpx.AsyncClient(
                **_build_async_client_kwargs(
                    base_url=self.base_url,
                    headers=headers,
                    timeout_seconds=self.timeout_seconds,
                    transport=self.transport,
                    limits=self.limits,
                )
            ) as client:
                response = await client.get(f"/torrents/info/{torrent_id}")
        except httpx.HTTPError as exc:
            logger.warning(
                "Real-Debrid torrent info request failed for attachment %s: %s",
                request.attachment_id,
                exc,
            )
            return []

        if not response.is_success:
            logger.warning(
                "Real-Debrid torrent info failed for attachment %s with status %s",
                request.attachment_id,
                response.status_code,
            )
            return []

        try:
            payload = response.json()
        except ValueError:
            logger.warning(
                "Real-Debrid torrent info returned non-JSON data for attachment %s",
                request.attachment_id,
            )
            return []

        if not isinstance(payload, dict):
            logger.warning(
                "Real-Debrid torrent info returned an unexpected payload for attachment %s",
                request.attachment_id,
            )
            return []

        raw_files = payload.get("files")
        raw_links = payload.get("links")
        if not isinstance(raw_files, list) or not all(isinstance(file, dict) for file in raw_files):
            logger.warning(
                "Real-Debrid torrent info returned no file/link data for attachment %s",
                request.attachment_id,
            )
            return []
        if not isinstance(raw_links, list) or not all(isinstance(link, str) for link in raw_links):
            logger.warning(
                "Real-Debrid torrent info returned no file/link data for attachment %s",
                request.attachment_id,
            )
            return []

        files = cast(list[dict[str, Any]], raw_files)
        links = cast(list[str], raw_links)
        return self._build_file_projections(request, files=files, links=links)

    async def refresh_download(
        self,
        *,
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
        """Resolve one Real-Debrid provider download into a fresh unrestricted playback URL."""

        projections = await self.project_download_attachments(request=request)
        projection = PlaybackSourceService.select_provider_file_projection(
            request,
            projections,
        )
        if projection is None:
            logger.warning(
                "Real-Debrid torrent info could not match a file link for attachment %s",
                request.attachment_id,
            )
            return None

        return await self.unrestrict_link(projection.restricted_url, request=request)


@dataclass(slots=True)
class AllDebridPlaybackClient:
    """Minimal built-in AllDebrid client for playback-link unlocking."""

    api_token: str
    base_url: str = _ALLDEBRID_BASE_URL
    timeout_seconds: float = 10.0
    transport: httpx.AsyncBaseTransport | None = None
    limits: httpx.Limits | None = None
    limiter: PlaybackRefreshRateLimiter | None = None

    async def unrestrict_link(
        self,
        link: str,
        *,
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
        """Resolve one restricted AllDebrid link into a direct playback URL."""

        headers = {"Authorization": f"Bearer {self.api_token}"}
        try:
            async with httpx.AsyncClient(
                **_build_async_client_kwargs(
                    base_url=self.base_url,
                    headers=headers,
                    timeout_seconds=self.timeout_seconds,
                    transport=self.transport,
                    limits=self.limits,
                )
            ) as client:
                response = await client.get("/v4/link/unlock", params={"link": link})
        except httpx.HTTPError as exc:
            logger.warning(
                "AllDebrid unlock request failed for attachment %s: %s",
                request.attachment_id,
                exc,
            )
            return None

        if not response.is_success:
            logger.warning(
                "AllDebrid unlock failed for attachment %s with status %s",
                request.attachment_id,
                response.status_code,
            )
            return None

        try:
            payload = response.json()
        except ValueError:
            logger.warning(
                "AllDebrid unlock returned non-JSON data for attachment %s",
                request.attachment_id,
            )
            return None

        if not isinstance(payload, dict) or payload.get("status") != "success":
            logger.warning(
                "AllDebrid unlock returned an unexpected payload for attachment %s",
                request.attachment_id,
            )
            return None

        data = payload.get("data")
        if not isinstance(data, dict):
            logger.warning(
                "AllDebrid unlock returned no data object for attachment %s",
                request.attachment_id,
            )
            return None

        download_url = data.get("link")
        if not isinstance(download_url, str) or not download_url:
            logger.warning(
                "AllDebrid unlock returned no download URL for attachment %s",
                request.attachment_id,
            )
            return None

        return PlaybackAttachmentProviderUnrestrictedLink(download_url=download_url)

    async def add_magnet(self, magnet_url: str) -> str:
        await _acquire_download_rate_limit(provider="alldebrid", limiter=self.limiter)
        headers = {"Authorization": f"Bearer {self.api_token}"}
        async with httpx.AsyncClient(
            **_build_async_client_kwargs(
                base_url=self.base_url,
                headers=headers,
                timeout_seconds=self.timeout_seconds,
                transport=self.transport,
                limits=self.limits,
            )
        ) as client:
            response = await client.post("/v4/magnet/upload", data={"magnets[]": magnet_url})
        response.raise_for_status()
        payload = response.json()
        magnets = payload.get("data", {}).get("magnets") if isinstance(payload, dict) else None
        if not isinstance(magnets, list) or not magnets or not isinstance(magnets[0], dict):
            raise RuntimeError("alldebrid_add_magnet_failed")
        first_magnet = cast(dict[str, Any], magnets[0])
        torrent_id = first_magnet.get("id")
        if not isinstance(torrent_id, (int, str)):
            raise RuntimeError("alldebrid_add_magnet_failed")
        return str(torrent_id)

    async def get_torrent_info(self, provider_torrent_id: str) -> TorrentInfo:
        await _acquire_download_rate_limit(provider="alldebrid", limiter=self.limiter)
        headers = {"Authorization": f"Bearer {self.api_token}"}
        async with httpx.AsyncClient(
            **_build_async_client_kwargs(
                base_url=self.base_url,
                headers=headers,
                timeout_seconds=self.timeout_seconds,
                transport=self.transport,
                limits=self.limits,
            )
        ) as client:
            response = await client.post("/v4.1/magnet/status", data={"id": provider_torrent_id})
        response.raise_for_status()
        payload = response.json()
        magnet_payload: dict[str, Any] | None = None
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict):
                magnets = data.get("magnets")
                if isinstance(magnets, list) and magnets and isinstance(magnets[0], dict):
                    magnet_payload = cast(dict[str, Any], magnets[0])
                elif isinstance(magnets, dict):
                    magnet_payload = cast(dict[str, Any], magnets)
        if magnet_payload is None:
            raise RuntimeError("alldebrid_torrent_info_invalid")
        files: list[TorrentFile] = []
        file_payloads = magnet_payload.get("files")
        link_payloads = magnet_payload.get("links")
        # Top-level links array: only reliable for flat (non-nested) torrents
        top_links = (
            [link for link in link_payloads if isinstance(link, str) and link]
            if isinstance(link_payloads, list)
            else []
        )
        if isinstance(file_payloads, list):
            # Use recursive flattener: reads per-file 'l' field and handles nested
            # season-pack directory structures that break flat index-based mapping.
            flat_pairs = _flatten_alldebrid_files(file_payloads)
            for index, (file_payload, per_file_link) in enumerate(flat_pairs):
                # Prefer per-file embedded link ('l') over sequential top-level links[]
                resolved_link = per_file_link or (
                    top_links[index] if index < len(top_links) else None
                )
                normalized = _normalize_torrent_file(
                    file_payload,
                    download_url=resolved_link,
                )
                if normalized is not None:
                    files.append(normalized)
        # Collect all resolved links for the TorrentInfo.links attribute (cast: guarded by truthiness)
        resolved_links: list[str] = cast(
            list[str], [f.download_url for f in files if f.download_url]
        )
        return TorrentInfo(
            provider_torrent_id=str(magnet_payload.get("id") or provider_torrent_id),
            status=str(magnet_payload.get("status") or "unknown"),
            name=magnet_payload.get("filename")
            if isinstance(magnet_payload.get("filename"), str)
            else None,
            info_hash=magnet_payload.get("hash")
            if isinstance(magnet_payload.get("hash"), str)
            else None,
            files=files,
            links=resolved_links,
        )

    async def select_files(self, provider_torrent_id: str, file_ids: list[str]) -> None:
        await _acquire_download_rate_limit(provider="alldebrid", limiter=self.limiter)
        headers = {"Authorization": f"Bearer {self.api_token}"}
        payload: dict[str, Any] = {"id[]": provider_torrent_id}
        if file_ids:
            payload["files[]"] = file_ids
        async with httpx.AsyncClient(
            **_build_async_client_kwargs(
                base_url=self.base_url,
                headers=headers,
                timeout_seconds=self.timeout_seconds,
                transport=self.transport,
                limits=self.limits,
            )
        ) as client:
            response = await client.post("/v4/magnet/files", data=payload)
        response.raise_for_status()

    async def get_download_links(self, provider_torrent_id: str) -> list[str]:
        info = await self.get_torrent_info(provider_torrent_id)
        return [link for link in info.links if link]

    async def get_user_info(self) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=8.0) as client:
            response = await client.get(
                f"{self.base_url}/v4/user",
                params={"agent": "filmucore", "apikey": self.api_token},
            )
        response.raise_for_status()
        return cast(dict[str, Any], response.json())


@dataclass(slots=True)
class DebridLinkPlaybackClient:
    """Minimal built-in Debrid-Link client for playback refresh resolution."""

    api_token: str
    base_url: str = _DEBRIDLINK_BASE_URL
    timeout_seconds: float = 10.0
    transport: httpx.AsyncBaseTransport | None = None
    limits: httpx.Limits | None = None
    limiter: PlaybackRefreshRateLimiter | None = None

    async def unrestrict_link(
        self,
        link: str,
        *,
        request: PlaybackAttachmentRefreshRequest,
    ) -> PlaybackAttachmentProviderUnrestrictedLink | None:
        """Return the provided link, because Debrid-Link already exposes direct download URLs."""

        _ = request
        if not link:
            return None
        return PlaybackAttachmentProviderUnrestrictedLink(download_url=link)

    async def add_magnet(self, magnet_url: str) -> str:
        await _acquire_download_rate_limit(provider="debridlink", limiter=self.limiter)
        headers = {"Authorization": f"Bearer {self.api_token}"}
        async with httpx.AsyncClient(
            **_build_async_client_kwargs(
                base_url=self.base_url,
                headers=headers,
                timeout_seconds=self.timeout_seconds,
                transport=self.transport,
                limits=self.limits,
            )
        ) as client:
            response = await client.post("/seedbox/add", json={"url": magnet_url})
        response.raise_for_status()
        payload = response.json()
        value = payload.get("value") if isinstance(payload, dict) else None
        torrent_id = value.get("id") if isinstance(value, dict) else None
        if not isinstance(torrent_id, (str, int)):
            raise RuntimeError("debridlink_add_magnet_failed")
        return str(torrent_id)

    async def get_torrent_info(self, provider_torrent_id: str) -> TorrentInfo:
        await _acquire_download_rate_limit(provider="debridlink", limiter=self.limiter)
        headers = {"Authorization": f"Bearer {self.api_token}"}
        async with httpx.AsyncClient(
            **_build_async_client_kwargs(
                base_url=self.base_url,
                headers=headers,
                timeout_seconds=self.timeout_seconds,
                transport=self.transport,
                limits=self.limits,
            )
        ) as client:
            response = await client.get("/seedbox/list", params={"ids": provider_torrent_id})
        response.raise_for_status()
        payload = response.json()
        value = payload.get("value") if isinstance(payload, dict) else None
        torrent_payload = (
            value[0]
            if isinstance(value, list) and value and isinstance(value[0], dict)
            else value
            if isinstance(value, dict)
            else None
        )
        if not isinstance(torrent_payload, dict):
            raise RuntimeError("debridlink_torrent_info_invalid")
        files: list[TorrentFile] = []
        file_payloads = torrent_payload.get("files")
        if isinstance(file_payloads, list):
            for file_payload in file_payloads:
                if not isinstance(file_payload, dict):
                    continue
                download_url = (
                    file_payload.get("downloadUrl")
                    if isinstance(file_payload.get("downloadUrl"), str)
                    else None
                )
                normalized = _normalize_torrent_file(file_payload, download_url=download_url)
                if normalized is not None:
                    files.append(normalized)
        links = [
            link for link in (file.download_url for file in files) if isinstance(link, str) and link
        ]
        return TorrentInfo(
            provider_torrent_id=str(torrent_payload.get("id") or provider_torrent_id),
            status=str(torrent_payload.get("status") or "unknown"),
            name=torrent_payload.get("name")
            if isinstance(torrent_payload.get("name"), str)
            else None,
            info_hash=torrent_payload.get("hashString")
            if isinstance(torrent_payload.get("hashString"), str)
            else None,
            files=files,
            links=links,
        )

    async def select_files(self, provider_torrent_id: str, file_ids: list[str]) -> None:
        await _acquire_download_rate_limit(provider="debridlink", limiter=self.limiter)
        headers = {"Authorization": f"Bearer {self.api_token}"}
        async with httpx.AsyncClient(
            **_build_async_client_kwargs(
                base_url=self.base_url,
                headers=headers,
                timeout_seconds=self.timeout_seconds,
                transport=self.transport,
                limits=self.limits,
            )
        ) as client:
            response = await client.post(
                f"/seedbox/{provider_torrent_id}/select_files",
                json={"ids": file_ids},
            )
        response.raise_for_status()

    async def get_download_links(self, provider_torrent_id: str) -> list[str]:
        info = await self.get_torrent_info(provider_torrent_id)
        return [link for link in info.links if link]

    async def get_user_info(self) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.api_token}"}
        async with httpx.AsyncClient(timeout=8.0, headers=headers) as client:
            response = await client.get(f"{self.base_url}/account/infos")
        response.raise_for_status()
        return cast(dict[str, Any], response.json())


class DownloaderAccountService:
    """Normalize active downloader-account information for dashboard compatibility."""

    def __init__(self, settings: DownloadersSettings) -> None:
        self._settings = settings

    async def get_active_provider_info(self) -> dict[str, Any]:
        providers: list[tuple[str, str, object]] = []
        if self._settings.real_debrid.api_key:
            providers.append(
                ("real_debrid", self._settings.real_debrid.api_key, RealDebridPlaybackClient(api_token=self._settings.real_debrid.api_key))
            )
        if self._settings.all_debrid.api_key:
            providers.append(
                ("all_debrid", self._settings.all_debrid.api_key, AllDebridPlaybackClient(api_token=self._settings.all_debrid.api_key))
            )
        if self._settings.debrid_link.api_key:
            providers.append(
                ("debrid_link", self._settings.debrid_link.api_key, DebridLinkPlaybackClient(api_token=self._settings.debrid_link.api_key))
            )
        if not providers:
            return {"provider": None, "error": "no provider configured"}

        provider_name, _api_key, client = providers[0]
        try:
            payload = await cast(Any, client).get_user_info()
        except Exception:
            logger.warning(
                "downloader account info request failed",
                extra={"provider": provider_name},
                exc_info=True,
            )
            return {"provider": provider_name, "error": "provider_request_failed"}

        if provider_name == "real_debrid":
            return {
                "provider": provider_name,
                "username": payload.get("username"),
                "email": payload.get("email"),
                "premium_days_remaining": self._days_remaining_from_iso(cast(str | None, payload.get("expiration"))),
                "plan": "premium" if payload.get("premium") else "free",
            }

        if provider_name == "all_debrid":
            user = payload.get("data", {}).get("user", {}) if isinstance(payload, dict) else {}
            return {
                "provider": provider_name,
                "username": user.get("username"),
                "email": None,
                "premium_days_remaining": self._days_remaining_from_unix(cast(int | None, user.get("premiumUntil"))),
                "plan": "premium" if user.get("isPremium") else "free",
            }

        value = payload.get("value", {}) if isinstance(payload, dict) else {}
        premium_left = value.get("premiumLeft")
        expiry = None
        if isinstance(premium_left, int):
            expiry = int(datetime.now(tz=UTC).timestamp()) + premium_left
        return {
            "provider": provider_name,
            "username": value.get("login"),
            "email": None,
            "premium_days_remaining": self._days_remaining_from_unix(expiry),
            "plan": "premium" if isinstance(premium_left, int) and premium_left > 0 else "free",
        }

    @staticmethod
    def _days_remaining_from_iso(expiration_str: str | None) -> int | None:
        if not expiration_str:
            return None
        try:
            expiry = datetime.fromisoformat(expiration_str.replace("Z", "+00:00"))
        except ValueError:
            return None
        return max(0, (expiry - datetime.now(tz=UTC)).days)

    @staticmethod
    def _days_remaining_from_unix(unix_ts: int | None) -> int | None:
        if unix_ts is None:
            return None
        expiry = datetime.fromtimestamp(unix_ts, tz=UTC)
        return max(0, (expiry - datetime.now(tz=UTC)).days)


def build_builtin_playback_provider_clients(
    settings: Settings,
) -> dict[str, PlaybackAttachmentProviderClient]:
    """Build built-in playback refresh provider clients from configured runtime settings."""

    clients: dict[str, PlaybackAttachmentProviderClient] = {}

    if settings.downloaders.real_debrid.api_key:
        clients["realdebrid"] = RealDebridPlaybackClient(
            api_token=settings.downloaders.real_debrid.api_key
        )

    if settings.downloaders.all_debrid.api_key:
        clients["alldebrid"] = AllDebridPlaybackClient(
            api_token=settings.downloaders.all_debrid.api_key
        )

    if settings.downloaders.debrid_link.api_key:
        clients["debridlink"] = DebridLinkPlaybackClient(
            api_token=settings.downloaders.debrid_link.api_key
        )

    return clients
