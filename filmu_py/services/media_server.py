"""Best-effort media-server library scan triggers for completed items."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar
from xml.etree import ElementTree

import httpx

from filmu_py.config import (
    EmbyUpdaterConfig,
    JellyfinUpdaterConfig,
    PlexUpdaterConfig,
    UpdatersSettings,
)

logger = logging.getLogger(__name__)
_REQUEST_TIMEOUT_SECONDS = 10.0
T = TypeVar("T")


class MediaServerNotifier:
    def __init__(self, settings: UpdatersSettings) -> None:
        self._settings = settings

    async def notify_all(self, item_id: str) -> dict[str, str]:
        operations: tuple[tuple[str, Callable[[str], Awaitable[None]]], ...] = (
            ("plex", self.notify_plex),
            ("jellyfin", self.notify_jellyfin),
            ("emby", self.notify_emby),
        )
        summary: dict[str, str] = {}
        results = await asyncio.gather(
            *(operation(item_id) for _, operation in operations),
            return_exceptions=True,
        )
        for (provider, _), result in zip(operations, results, strict=True):
            if isinstance(result, Exception):
                logger.warning(
                    "media_server.scan_failed",
                    extra={
                        "event": "media_server.scan_failed",
                        "provider": provider,
                        "item_id": item_id,
                        "url": self._provider_url(provider),
                        "error": str(result),
                    },
                )
                summary[provider] = "failed"
                continue

            summary[provider] = str(result)

        return summary

    async def notify_plex(self, item_id: str) -> str:
        config = self._settings.plex
        if not self._plex_ready(config):
            return "skipped"

        url = self._normalized_url(config.url)
        headers = {"X-Plex-Token": config.token}
        try:
            async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
                response = await client.get(f"{url}/library/sections", headers=headers)
                response.raise_for_status()
                section_keys = self._plex_section_keys(response.text)
                for key in section_keys:
                    refresh_response = await client.get(
                        f"{url}/library/sections/{key}/refresh",
                        headers=headers,
                    )
                    refresh_response.raise_for_status()
        except Exception as exc:
            self._log_failed(provider="plex", item_id=item_id, url=url, error=str(exc))
            return "failed"

        self._log_triggered(provider="plex", item_id=item_id, url=url)
        return "triggered"

    async def notify_jellyfin(self, item_id: str) -> str:
        return await self._notify_emby_family(
            provider="jellyfin",
            item_id=item_id,
            config=self._settings.jellyfin,
        )

    async def notify_emby(self, item_id: str) -> str:
        return await self._notify_emby_family(
            provider="emby",
            item_id=item_id,
            config=self._settings.emby,
        )

    @staticmethod
    def _normalized_url(url: str) -> str:
        return url.rstrip("/")

    @staticmethod
    def _plex_ready(config: PlexUpdaterConfig) -> bool:
        return config.enabled and bool(config.url.strip()) and bool(config.token.strip())

    @staticmethod
    def _emby_family_ready(config: JellyfinUpdaterConfig | EmbyUpdaterConfig) -> bool:
        return config.enabled and bool(config.url.strip()) and bool(config.api_key.strip())

    @staticmethod
    def _plex_section_keys(payload: str) -> list[str]:
        root = ElementTree.fromstring(payload)
        keys: list[str] = []
        for section in root.iter("Directory"):
            key = section.attrib.get("key")
            if key:
                keys.append(key)
        return keys

    async def _notify_emby_family(
        self,
        *,
        provider: str,
        item_id: str,
        config: JellyfinUpdaterConfig | EmbyUpdaterConfig,
    ) -> str:
        if not self._emby_family_ready(config):
            return "skipped"

        url = self._normalized_url(config.url)
        headers = {"X-Emby-Token": config.api_key}
        try:
            async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
                response = await client.post(f"{url}/Library/Refresh", headers=headers)
                response.raise_for_status()
        except Exception as exc:
            self._log_failed(provider=provider, item_id=item_id, url=url, error=str(exc))
            return "failed"

        self._log_triggered(provider=provider, item_id=item_id, url=url)
        return "triggered"

    def _provider_url(self, provider: str) -> str:
        if provider == "plex":
            return self._normalized_url(self._settings.plex.url) if self._settings.plex.url else ""
        if provider == "jellyfin":
            return (
                self._normalized_url(self._settings.jellyfin.url)
                if self._settings.jellyfin.url
                else ""
            )
        return self._normalized_url(self._settings.emby.url) if self._settings.emby.url else ""

    @staticmethod
    def _log_triggered(*, provider: str, item_id: str, url: str) -> None:
        logger.info(
            "media_server.scan_triggered",
            extra={
                "event": "media_server.scan_triggered",
                "provider": provider,
                "item_id": item_id,
                "url": url,
            },
        )

    @staticmethod
    def _log_failed(*, provider: str, item_id: str, url: str, error: str) -> None:
        logger.warning(
            "media_server.scan_failed",
            extra={
                "event": "media_server.scan_failed",
                "provider": provider,
                "item_id": item_id,
                "url": url,
                "error": error,
            },
        )
