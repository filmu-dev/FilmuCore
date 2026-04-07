"""Built-in webhook notification plugin implementation."""

from __future__ import annotations

from typing import Any

import httpx

from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import NotificationEvent, PluginEventHookWorker

NOTIFICATIONS_PLUGIN_NAME = "notifications"
_DISCORD_BUCKET = "notifications:discord"
_GENERIC_BUCKET = "notifications:webhook"


class WebhookNotificationPlugin(PluginEventHookWorker):
    """Notification plugin delivering Discord embeds and generic JSON webhooks."""

    plugin_name: str = NOTIFICATIONS_PLUGIN_NAME
    subscribed_events: frozenset[str] = frozenset({"item.state.changed", "item.completed"})

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.ctx: PluginContext | None = None
        self.enabled = False
        self.discord_url = ""
        self.webhook_url = ""
        self.notify_on: set[str] = {"completed", "failed"}
        self._transport = transport

    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx
        enabled = ctx.settings.get("enabled")
        self.enabled = bool(enabled) if enabled is not None else True
        discord_webhook_url = ctx.settings.get("discord_webhook_url")
        webhook_url = ctx.settings.get("webhook_url")
        service_urls = ctx.settings.get("service_urls")
        self.discord_url = discord_webhook_url.strip() if isinstance(discord_webhook_url, str) else ""
        self.webhook_url = webhook_url.strip() if isinstance(webhook_url, str) else ""
        if not self.webhook_url and isinstance(service_urls, list):
            for candidate in service_urls:
                if isinstance(candidate, str) and candidate.strip():
                    self.webhook_url = candidate.strip()
                    break
        raw_notify_on = ctx.settings.get("notify_on")
        if isinstance(raw_notify_on, list):
            normalized = {
                value.strip().lower()
                for value in raw_notify_on
                if isinstance(value, str) and value.strip()
            }
            self.notify_on = normalized or {"completed", "failed"}
        if not self.enabled or not (self.discord_url or self.webhook_url):
            ctx.logger.warning("plugin.stub_not_configured", plugin=ctx.plugin_name)

    async def _send_discord(self, client: httpx.AsyncClient, event: NotificationEvent) -> None:
        if self.ctx is None or not self.discord_url:
            return
        payload = {
            "embeds": [
                {
                    "title": event.title,
                    "description": event.message or "",
                    "color": 0x00B0F4,
                }
            ]
        }
        try:
            await self.ctx.rate_limiter.acquire(_DISCORD_BUCKET, 1.0, 1.0)
            response = await client.post(self.discord_url, json=payload)
            response.raise_for_status()
            self.ctx.logger.info(
                "plugin.notification.discord.sent",
                plugin=self.ctx.plugin_name,
                event_type=event.event_type,
            )
        except Exception as exc:
            self.ctx.logger.warning(
                "plugin.notification.discord.failed",
                plugin=self.ctx.plugin_name,
                event_type=event.event_type,
                exc=str(exc),
            )

    async def _send_generic(self, client: httpx.AsyncClient, payload: dict[str, object]) -> None:
        if self.ctx is None or not self.webhook_url:
            return
        try:
            await self.ctx.rate_limiter.acquire(_GENERIC_BUCKET, 1.0, 1.0)
            response = await client.post(self.webhook_url, json=payload)
            response.raise_for_status()
            self.ctx.logger.info(
                "plugin.notification.webhook.sent",
                plugin=self.ctx.plugin_name,
                event_type=payload.get("event_type"),
            )
        except Exception as exc:
            self.ctx.logger.warning(
                "plugin.notification.webhook.failed",
                plugin=self.ctx.plugin_name,
                event_type=payload.get("event_type"),
                exc=str(exc),
            )

    async def send(self, event: NotificationEvent) -> None:
        if self.ctx is None:
            raise RuntimeError("WebhookNotificationPlugin must be initialized before send")
        if not self.enabled:
            return
        state = ""
        if event.payload is not None:
            to_state = event.payload.get("to_state")
            state = to_state.lower() if isinstance(to_state, str) else ""
        if state and state not in self.notify_on:
            return

        async with httpx.AsyncClient(timeout=8.0, transport=self._transport) as client:
            if self.discord_url:
                await self._send_discord(client, event)
            if self.webhook_url:
                await self._send_generic(
                    client,
                    {
                        "event_type": event.event_type,
                        "title": event.title,
                        "message": event.message,
                        "payload": dict(event.payload or {}),
                    },
                )

    async def handle(self, event_type: str, payload: dict[str, Any]) -> None:
        await self.send(
            NotificationEvent(
                event_type=event_type,
                title=str(payload.get("title") or event_type),
                message=(payload.get("message") if isinstance(payload.get("message"), str) else None),
                payload=payload,
            )
        )
