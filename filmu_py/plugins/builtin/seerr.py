"""Built-in Seerr/Overseerr content-service plugin implementation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx

from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import ContentRequest

SEERR_PLUGIN_NAME = "seerr"
_SEERR_POLL_BUCKET = "ratelimit:seerr:poll"


def resolve_seerr_settings(settings: Mapping[str, Any]) -> Mapping[str, Any]:
    if any(key in settings for key in {"enabled", "url", "api_key", "take"}):
        return settings
    content = settings.get("content")
    if not isinstance(content, Mapping):
        return {}
    seerr = content.get(SEERR_PLUGIN_NAME)
    if not isinstance(seerr, Mapping):
        seerr = content.get("overseerr")
        if not isinstance(seerr, Mapping):
            return {}
    return seerr


def _media_type(value: object) -> str:
    if not isinstance(value, str):
        return "movie"
    normalized = value.strip().lower()
    if normalized in {"tv", "show", "series"}:
        return "tv"
    return "movie"


def _external_ref(media: Mapping[str, Any]) -> str | None:
    tmdb_id = media.get("tmdbId") or media.get("tmdb_id")
    if isinstance(tmdb_id, int | str) and not isinstance(tmdb_id, bool):
        text = str(tmdb_id).strip()
        if text:
            return f"tmdb:{text}"
    tvdb_id = media.get("tvdbId") or media.get("tvdb_id")
    if isinstance(tvdb_id, int | str) and not isinstance(tvdb_id, bool):
        text = str(tvdb_id).strip()
        if text:
            return f"tvdb:{text}"
    return None


class SeerrContentService:
    """Poll Seerr/Overseerr request inventory into normalized content requests."""

    plugin_name: str = SEERR_PLUGIN_NAME
    subscribed_events: frozenset[str] = frozenset()

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.ctx: PluginContext | None = None
        self.enabled = False
        self.base_url = ""
        self.api_key = ""
        self.take = 50
        self.allowed_statuses: set[str] = {"pending", "approved"}
        self._transport = transport

    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx
        block = resolve_seerr_settings(ctx.settings)
        self.enabled = bool(block.get("enabled", False))
        self.base_url = str(block.get("url") or block.get("base_url") or "").rstrip("/")
        self.api_key = str(block.get("api_key") or "").strip()
        take_value = block.get("take")
        if isinstance(take_value, int) and take_value > 0:
            self.take = take_value
        raw_statuses = block.get("statuses")
        if isinstance(raw_statuses, list):
            normalized = {
                str(value).strip().lower()
                for value in raw_statuses
                if isinstance(value, str) and value.strip()
            }
            if normalized:
                self.allowed_statuses = normalized
        if not self.enabled or not self.base_url or not self.api_key:
            ctx.logger.warning(
                "plugin.stub_not_configured",
                plugin=self.plugin_name,
                reason="seerr not enabled or missing url/api_key",
            )

    async def poll(self) -> list[ContentRequest]:
        if self.ctx is None:
            raise RuntimeError("SeerrContentService must be initialized before poll")
        if not self.enabled or not self.base_url or not self.api_key:
            return []

        try:
            await self.ctx.rate_limiter.acquire(
                _SEERR_POLL_BUCKET,
                1.0,
                1.0 / 60.0,
            )
            async with httpx.AsyncClient(timeout=10.0, transport=self._transport) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/request",
                    params={"take": self.take, "sort": "added", "sortDirection": "desc"},
                    headers={"X-Api-Key": self.api_key},
                )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            self.ctx.logger.warning(
                "plugin.seerr.poll_failed",
                plugin=self.plugin_name,
                exc=str(exc),
            )
            return []

        results = payload.get("results") if isinstance(payload, Mapping) else None
        if not isinstance(results, list):
            return []

        requests: list[ContentRequest] = []
        seen: set[tuple[str, str]] = set()
        for item in results:
            if not isinstance(item, Mapping):
                continue
            status = str(item.get("status") or "").strip().lower()
            if self.allowed_statuses and status not in self.allowed_statuses:
                continue
            media = item.get("media")
            if not isinstance(media, Mapping):
                continue
            external_ref = _external_ref(media)
            if external_ref is None:
                continue
            media_type = _media_type(media.get("mediaType") or media.get("type"))
            title = media.get("title")
            request_key = (external_ref, media_type)
            if request_key in seen:
                continue
            seen.add(request_key)
            requests.append(
                ContentRequest(
                    external_ref=external_ref,
                    media_type=media_type,
                    title=title if isinstance(title, str) else None,
                    source=SEERR_PLUGIN_NAME,
                    source_list_id=status or None,
                )
            )
        return requests
