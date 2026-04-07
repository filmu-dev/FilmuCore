"""Built-in MDBList content-service plugin implementation."""

from __future__ import annotations

import httpx

from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import ContentRequest

MDBLIST_PLUGIN_NAME = "mdblist"
_MDBLIST_POLL_BUCKET = "ratelimit:mdblist:poll"


class MDBListContentService:
    """MDBList content-service plugin that polls configured remote lists over HTTP."""

    plugin_name: str = MDBLIST_PLUGIN_NAME

    subscribed_events: frozenset[str] = frozenset()

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.ctx: PluginContext | None = None
        self.enabled = False
        self.api_key = ""
        self.list_ids: list[str] = []
        self.poll_interval_minutes = 60
        self._transport = transport

    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx
        self.enabled = bool(ctx.settings.get("enabled", False))
        api_key = ctx.settings.get("api_key", "")
        self.api_key = api_key.strip() if isinstance(api_key, str) else ""
        raw_list_ids = ctx.settings.get("list_ids", [])
        self.list_ids = [value.strip() for value in raw_list_ids if isinstance(value, str) and value.strip()] if isinstance(raw_list_ids, list) else []
        poll_interval_minutes = ctx.settings.get("poll_interval_minutes", 60)
        self.poll_interval_minutes = poll_interval_minutes if isinstance(poll_interval_minutes, int) and poll_interval_minutes > 0 else 60
        if not self.enabled or not self.api_key:
            ctx.logger.warning(
                "plugin.stub_not_configured",
                plugin=self.plugin_name,
                reason="mdblist not enabled or api_key missing",
            )

    async def poll(self) -> list[ContentRequest]:
        if self.ctx is None:
            raise RuntimeError("MDBListContentService must be initialized before poll")
        if not self.enabled or not self.api_key or not self.list_ids:
            return []

        requests: list[ContentRequest] = []
        refill_per_second = 1.0 / max(float(self.poll_interval_minutes * 60), 1.0)
        async with httpx.AsyncClient(timeout=10.0, transport=self._transport) as client:
            for list_id in self.list_ids:
                try:
                    await self.ctx.rate_limiter.acquire(
                        _MDBLIST_POLL_BUCKET,
                        1.0,
                        refill_per_second,
                    )
                    response = await client.get(
                        f"https://mdblist.com/api/lists/{list_id}/items/",
                        params={"apikey": self.api_key},
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, list):
                        continue
                    for item in payload:
                        if not isinstance(item, dict):
                            continue
                        tmdb_id = item.get("tmdb_id")
                        if tmdb_id is None:
                            continue
                        media_type = item.get("mediatype", "movie")
                        if not isinstance(media_type, str):
                            media_type = "movie"
                        title = item.get("title")
                        requests.append(
                            ContentRequest(
                                external_ref=f"tmdb:{tmdb_id}",
                                media_type=media_type,
                                title=title if isinstance(title, str) else None,
                                source=MDBLIST_PLUGIN_NAME,
                                source_list_id=list_id,
                            )
                        )
                except Exception as exc:
                    self.ctx.logger.warning(
                        "plugin.mdblist.poll_failed",
                        list_id=list_id,
                        exc=str(exc),
                    )
        return requests
