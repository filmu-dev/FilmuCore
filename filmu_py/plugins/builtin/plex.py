"""Built-in Plex post-download event hook plugin implementation."""

from __future__ import annotations

from typing import Any

import httpx

from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import NotificationEvent, PluginEventHookWorker

PLEX_PLUGIN_NAME = "plex"
_PLEX_REFRESH_BUCKET = "plex:library_refresh"


def resolve_plex_settings(settings: dict[str, Any]) -> dict[str, Any]:
    """Return the most specific Plex hook settings block available."""

    if any(key in settings for key in {"enabled", "url", "token", "section_ids"}):
        return settings
    for candidate in (
        settings.get(PLEX_PLUGIN_NAME),
        settings.get("notifications", {}).get(PLEX_PLUGIN_NAME)
        if isinstance(settings.get("notifications"), dict)
        else None,
        settings.get("updaters", {}).get(PLEX_PLUGIN_NAME)
        if isinstance(settings.get("updaters"), dict)
        else None,
    ):
        if isinstance(candidate, dict):
            return candidate
    return {}


class PlexLibraryRefreshPlugin(PluginEventHookWorker):
    """Trigger Plex library refresh operations from completion-oriented host events."""

    plugin_name: str = PLEX_PLUGIN_NAME
    subscribed_events: frozenset[str] = frozenset({"item.completed", "item.state.changed"})

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.ctx: PluginContext | None = None
        self.enabled = False
        self.base_url = ""
        self.token = ""
        self.section_ids: tuple[str, ...] = ()
        self.notify_on: frozenset[str] = frozenset({"item.completed"})
        self._transport = transport

    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx
        settings = resolve_plex_settings(dict(ctx.settings))
        self.enabled = bool(settings.get("enabled", False))
        self.base_url = str(settings.get("url") or settings.get("base_url") or "").rstrip("/")
        self.token = str(settings.get("token") or "").strip()
        raw_section_ids = settings.get("section_ids")
        if isinstance(raw_section_ids, list):
            self.section_ids = tuple(
                str(value).strip()
                for value in raw_section_ids
                if str(value).strip()
            )
        raw_notify_on = settings.get("notify_on")
        if isinstance(raw_notify_on, list):
            normalized = tuple(
                value.strip()
                for value in raw_notify_on
                if isinstance(value, str) and value.strip()
            )
            if normalized:
                self.notify_on = frozenset(normalized)
        if not self.enabled or not self.base_url or not self.token:
            ctx.logger.warning(
                "plugin.stub_not_configured",
                plugin=self.plugin_name,
                reason="plex not enabled or missing url/token",
            )

    def _section_ids_from_payload(self, payload: dict[str, Any]) -> tuple[str, ...]:
        explicit = payload.get("plex_section_ids")
        if isinstance(explicit, list):
            section_ids = tuple(
                str(value).strip()
                for value in explicit
                if str(value).strip()
            )
            if section_ids:
                return section_ids
        fallback = payload.get("plex_section_id")
        if fallback is not None and str(fallback).strip():
            return (str(fallback).strip(),)
        return self.section_ids

    async def _refresh_section(self, client: httpx.AsyncClient, section_id: str) -> None:
        if self.ctx is None:
            return
        await self.ctx.rate_limiter.acquire(_PLEX_REFRESH_BUCKET, 5.0, 1.0)
        response = await client.get(
            f"{self.base_url}/library/sections/{section_id}/refresh",
            params={"X-Plex-Token": self.token},
        )
        response.raise_for_status()

    async def send(self, event: NotificationEvent) -> None:
        if self.ctx is None:
            raise RuntimeError("PlexLibraryRefreshPlugin must be initialized before send")
        if not self.enabled or not self.base_url or not self.token:
            return
        if event.event_type not in self.notify_on:
            return

        payload = dict(event.payload or {})
        section_ids = self._section_ids_from_payload(payload)
        if not section_ids:
            self.ctx.logger.warning(
                "plugin.plex.refresh_skipped",
                plugin=self.plugin_name,
                reason="section_ids_missing",
            )
            return

        async with httpx.AsyncClient(timeout=8.0, transport=self._transport) as client:
            for section_id in section_ids:
                try:
                    await self._refresh_section(client, section_id)
                    self.ctx.logger.info(
                        "plugin.plex.section_refresh.sent",
                        plugin=self.plugin_name,
                        section_id=section_id,
                        event_type=event.event_type,
                    )
                except Exception as exc:
                    self.ctx.logger.warning(
                        "plugin.plex.section_refresh.failed",
                        plugin=self.plugin_name,
                        section_id=section_id,
                        event_type=event.event_type,
                        exc=str(exc),
                    )

    async def handle(self, event_type: str, payload: dict[str, Any]) -> None:
        if (
            event_type == "item.state.changed"
            and str(payload.get("to_state") or "").strip().lower() == "completed"
            and "item.completed" in self.notify_on
        ):
            event_type = "item.completed"
        await self.send(
            NotificationEvent(
                event_type=event_type,
                title=str(payload.get("title") or event_type),
                message=(payload.get("message") if isinstance(payload.get("message"), str) else None),
                payload=payload,
            )
        )
