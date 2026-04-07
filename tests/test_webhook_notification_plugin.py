from __future__ import annotations

import asyncio

import httpx

from filmu_py.plugins import NotificationEvent, PluginRegistry, TestPluginContext
from filmu_py.plugins.builtin.notifications import WebhookNotificationPlugin
from filmu_py.plugins.builtins import register_builtin_plugins


def test_webhook_notifications_send_disabled_makes_no_http_calls() -> None:
    calls: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"ok": True})

    plugin = WebhookNotificationPlugin(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "notifications": {
                "enabled": False,
                "webhook_url": "https://hooks.example/webhook",
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("notifications")))

    asyncio.run(plugin.send(NotificationEvent(event_type="item.completed", title="Done")))

    assert calls == []


def test_webhook_notifications_send_filtered_state_makes_no_http_calls() -> None:
    calls: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"ok": True})

    plugin = WebhookNotificationPlugin(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "notifications": {
                "enabled": True,
                "webhook_url": "https://hooks.example/webhook",
                "notify_on": ["completed"],
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("notifications")))

    asyncio.run(
        plugin.send(
            NotificationEvent(
                event_type="item.state.changed",
                title="State changed",
                payload={"to_state": "requested"},
            )
        )
    )

    assert calls == []


def test_webhook_notifications_send_posts_discord_embed() -> None:
    calls: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"ok": True})

    plugin = WebhookNotificationPlugin(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "notifications": {
                "enabled": True,
                "discord_webhook_url": "https://discord.example/hook",
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("notifications")))

    asyncio.run(
        plugin.send(
            NotificationEvent(
                event_type="item.completed",
                title="Completed",
                message="Movie done",
                payload={"to_state": "completed"},
            )
        )
    )

    assert len(calls) == 1
    assert str(calls[0].url) == "https://discord.example/hook"
    assert calls[0].read().decode("utf-8") == (
        '{"embeds":[{"title":"Completed","description":"Movie done","color":45300}]}'
    )


def test_webhook_notifications_send_posts_generic_json() -> None:
    calls: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"ok": True})

    plugin = WebhookNotificationPlugin(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "notifications": {
                "enabled": True,
                "webhook_url": "https://hooks.example/webhook",
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("notifications")))

    asyncio.run(
        plugin.send(
            NotificationEvent(
                event_type="item.completed",
                title="Completed",
                message="Movie done",
                payload={"to_state": "completed"},
            )
        )
    )

    assert len(calls) == 1
    assert str(calls[0].url) == "https://hooks.example/webhook"
    assert calls[0].read().decode("utf-8") == (
        '{"event_type":"item.completed","title":"Completed","message":"Movie done","payload":{"to_state":"completed"}}'
    )


def test_webhook_notifications_http_failure_is_logged_without_raising() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"detail": "boom"})

    plugin = WebhookNotificationPlugin(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "notifications": {
                "enabled": True,
                "webhook_url": "https://hooks.example/webhook",
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("notifications")))

    asyncio.run(
        plugin.send(
            NotificationEvent(
                event_type="item.completed",
                title="Completed",
                payload={"to_state": "completed"},
            )
        )
    )

    assert any(entry[1] == "plugin.notification.webhook.failed" for entry in harness.logger.entries)


def test_webhook_notifications_handle_bridges_to_send() -> None:
    plugin = WebhookNotificationPlugin()
    captured: list[NotificationEvent] = []

    async def fake_send(event: NotificationEvent) -> None:
        captured.append(event)

    plugin.send = fake_send  # type: ignore[method-assign]

    asyncio.run(
        plugin.handle(
            "item.state.changed",
            {"title": "Changed", "message": "done", "to_state": "completed"},
        )
    )

    assert len(captured) == 1
    assert captured[0].event_type == "item.state.changed"
    assert captured[0].title == "Changed"
    assert captured[0].payload == {"title": "Changed", "message": "done", "to_state": "completed"}


def test_webhook_notifications_are_registered_as_notification_and_event_hook() -> None:
    registry = PluginRegistry()
    harness = TestPluginContext(settings={"notifications": {"enabled": True}})

    register_builtin_plugins(registry, context_provider=harness.provider())

    assert any(plugin.__class__.__name__ == "WebhookNotificationPlugin" for plugin in registry.get_notifications())
    assert any(plugin.__class__.__name__ == "WebhookNotificationPlugin" for plugin in registry.get_event_hooks())
