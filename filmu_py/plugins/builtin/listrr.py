"""Built-in Listrr content-service plugin implementation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx

from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import ContentRequest

LISTRR_PLUGIN_NAME = "listrr"
_LISTRR_POLL_BUCKET = "ratelimit:listrr:poll"


def _listrr_settings(settings: Mapping[str, Any]) -> Mapping[str, Any]:
    if any(key in settings for key in {"enabled", "url", "list_ids", "api_key"}):
        return settings
    content = settings.get("content")
    if not isinstance(content, Mapping):
        return {}
    listrr = content.get(LISTRR_PLUGIN_NAME)
    if not isinstance(listrr, Mapping):
        return {}
    return listrr


def _normalize_media_type(value: object) -> str:
    if not isinstance(value, str):
        return "movie"
    normalized = value.strip().lower()
    if normalized in {"tv", "show", "series"}:
        return "tv"
    return "movie"


def _normalize_request(item: Mapping[str, Any], *, list_id: str) -> ContentRequest | None:
    media = item.get("media") if isinstance(item.get("media"), Mapping) else item
    if not isinstance(media, Mapping):
        return None
    tmdb_id = media.get("tmdbId") or media.get("tmdb_id")
    tvdb_id = media.get("tvdbId") or media.get("tvdb_id")
    external_ref: str | None = None
    if isinstance(tmdb_id, int | str) and not isinstance(tmdb_id, bool):
        text = str(tmdb_id).strip()
        if text:
            external_ref = f"tmdb:{text}"
    elif isinstance(tvdb_id, int | str) and not isinstance(tvdb_id, bool):
        text = str(tvdb_id).strip()
        if text:
            external_ref = f"tvdb:{text}"
    if external_ref is None:
        return None
    title = media.get("title") or media.get("name")
    return ContentRequest(
        external_ref=external_ref,
        media_type=_normalize_media_type(media.get("mediaType") or media.get("type")),
        title=title if isinstance(title, str) else None,
        source=LISTRR_PLUGIN_NAME,
        source_list_id=list_id,
    )


class ListrrContentService:
    """Poll Listrr-compatible list endpoints into normalized content requests."""

    plugin_name: str = LISTRR_PLUGIN_NAME
    subscribed_events: frozenset[str] = frozenset()

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.ctx: PluginContext | None = None
        self.enabled = False
        self.base_url = ""
        self.api_key = ""
        self.list_ids: list[str] = []
        self._transport = transport

    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx
        block = _listrr_settings(ctx.settings)
        self.enabled = bool(block.get("enabled", False))
        self.base_url = str(block.get("url") or block.get("base_url") or "").rstrip("/")
        self.api_key = str(block.get("api_key") or "").strip()
        raw_list_ids = block.get("list_ids")
        self.list_ids = (
            [value.strip() for value in raw_list_ids if isinstance(value, str) and value.strip()]
            if isinstance(raw_list_ids, list)
            else []
        )
        if not self.enabled or not self.base_url or not self.list_ids:
            ctx.logger.warning(
                "plugin.stub_not_configured",
                plugin=self.plugin_name,
                reason="listrr not enabled or missing url/list_ids",
            )

    async def poll(self) -> list[ContentRequest]:
        if self.ctx is None:
            raise RuntimeError("ListrrContentService must be initialized before poll")
        if not self.enabled or not self.base_url or not self.list_ids:
            return []

        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        requests: list[ContentRequest] = []
        seen: set[tuple[str, str]] = set()
        try:
            async with httpx.AsyncClient(timeout=10.0, transport=self._transport) as client:
                for list_id in self.list_ids:
                    await self.ctx.rate_limiter.acquire(
                        _LISTRR_POLL_BUCKET,
                        1.0,
                        1.0 / 60.0,
                    )
                    response = await client.get(
                        f"{self.base_url}/api/lists/{list_id}/items",
                        headers=headers,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    items = (
                        payload.get("items")
                        if isinstance(payload, Mapping)
                        else payload
                    )
                    if not isinstance(items, list):
                        continue
                    for item in items:
                        if not isinstance(item, Mapping):
                            continue
                        request = _normalize_request(item, list_id=list_id)
                        if request is None:
                            continue
                        key = (request.external_ref, request.media_type)
                        if key in seen:
                            continue
                        seen.add(key)
                        requests.append(request)
        except Exception as exc:
            self.ctx.logger.warning(
                "plugin.listrr.poll_failed",
                plugin=self.plugin_name,
                exc=str(exc),
            )
            return []

        return requests
