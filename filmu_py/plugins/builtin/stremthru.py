"""Built-in StremThru downloader plugin stub."""

from __future__ import annotations

import httpx

from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import (
    DownloadFileRecord,
    DownloadLinkResult,
    DownloadLinksInput,
    DownloadStatus,
    DownloadStatusInput,
    DownloadStatusResult,
    MagnetAddInput,
    MagnetAddResult,
)

STREMTHRU_PLUGIN_NAME = "stremthru"


class StremThruDownloader:
    """StremThru downloader plugin backed by the StremThru v0 store API."""

    plugin_name: str = STREMTHRU_PLUGIN_NAME

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.ctx: PluginContext | None = None
        self.base_url = "https://stremthru.com"
        self.token = ""
        self.enabled = False
        self._transport = transport

    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx
        self.enabled = bool(ctx.settings.get("enabled", False))
        configured_url = ctx.settings.get("url")
        if isinstance(configured_url, str) and configured_url.strip():
            self.base_url = configured_url.rstrip("/")
        token = ctx.settings.get("token", "")
        self.token = token.strip() if isinstance(token, str) else ""
        if not self.enabled or not self.token:
            ctx.logger.debug(
                "plugin.stub_not_configured",
                plugin=self.plugin_name,
                reason="stremthru not enabled or token missing",
            )
            return
        ctx.logger.info("plugin.stremthru.initialized", base_url=self.base_url)

    def _headers(self) -> dict[str, str]:
        return {"X-StremThru-Store-Authorization": f"Basic {self.token}"}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: object | None = None,
    ) -> httpx.Response:
        if self.ctx is None:
            raise RuntimeError("StremThruDownloader must be initialized before use")
        async with httpx.AsyncClient(timeout=30.0, transport=self._transport) as client:
            response = await client.request(
                method,
                f"{self.base_url}{path}",
                headers=self._headers(),
                json=json_body,
            )
            response.raise_for_status()
            return response

    @staticmethod
    def _map_status(raw_status: object) -> str:
        status = raw_status if isinstance(raw_status, str) else ""
        return {
            "queued": DownloadStatus.PENDING.value,
            "downloading": DownloadStatus.DOWNLOADING.value,
            "processing": DownloadStatus.PROCESSING.value,
            "ready": DownloadStatus.READY.value,
            "error": DownloadStatus.FAILED.value,
        }.get(status, DownloadStatus.UNKNOWN.value)

    async def add_magnet(self, request: MagnetAddInput) -> MagnetAddResult:
        if self.ctx is None:
            raise RuntimeError("StremThruDownloader must be initialized before add_magnet")
        await self.ctx.rate_limiter.acquire("ratelimit:stremthru:download", 1.0, 1.0)
        response = await self._request(
            "POST",
            "/v0/store/magnets",
            json_body={"magnet": request.magnet_url},
        )
        payload = response.json()
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        download_id = data.get("id") if isinstance(data, dict) else None
        if not isinstance(download_id, str) or not download_id:
            raise ValueError("stremthru response did not include a download id")
        return MagnetAddResult(download_id=download_id)

    async def get_status(self, request: DownloadStatusInput) -> DownloadStatusResult:
        if not self.enabled or not self.token:
            return DownloadStatusResult(
                download_id=request.download_id,
                status=DownloadStatus.UNKNOWN.value,
            )
        response = await self._request("GET", f"/v0/store/magnets/{request.download_id}")
        payload = response.json()
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        files_payload = data.get("files", []) if isinstance(data, dict) else []
        files: list[DownloadFileRecord] = []
        if isinstance(files_payload, list):
            for file_payload in files_payload:
                if not isinstance(file_payload, dict):
                    continue
                file_id = file_payload.get("id")
                path = file_payload.get("name") or file_payload.get("path") or ""
                if not isinstance(file_id, str) or not isinstance(path, str) or not path:
                    continue
                size_bytes = file_payload.get("size")
                files.append(
                    DownloadFileRecord(
                        file_id=file_id,
                        path=path,
                        size_bytes=size_bytes if isinstance(size_bytes, int) else None,
                        selected=bool(file_payload.get("selected", False)),
                        download_url=file_payload.get("link")
                        if isinstance(file_payload.get("link"), str)
                        else None,
                    )
                )
        return DownloadStatusResult(
            download_id=request.download_id,
            status=self._map_status(data.get("status") if isinstance(data, dict) else None),
            files=tuple(files),
        )

    async def get_download_links(self, request: DownloadLinksInput) -> list[DownloadLinkResult]:
        if not self.enabled or not self.token:
            return []
        response = await self._request("GET", f"/v0/store/magnets/{request.download_id}")
        payload = response.json()
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        files_payload = data.get("files", []) if isinstance(data, dict) else []
        requested_file_ids = set(request.file_ids)
        links: list[DownloadLinkResult] = []
        if not isinstance(files_payload, list):
            return links
        for file_payload in files_payload:
            if not isinstance(file_payload, dict):
                continue
            file_id = file_payload.get("id")
            link = file_payload.get("link")
            if not isinstance(link, str) or not link:
                continue
            if requested_file_ids and (not isinstance(file_id, str) or file_id not in requested_file_ids):
                continue
            links.append(
                DownloadLinkResult(
                    url=link,
                    file_id=file_id if isinstance(file_id, str) else None,
                    filename=file_payload.get("name") if isinstance(file_payload.get("name"), str) else None,
                    size_bytes=file_payload.get("size") if isinstance(file_payload.get("size"), int) else None,
                )
            )
        return links
