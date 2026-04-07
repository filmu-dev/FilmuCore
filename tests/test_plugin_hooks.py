"""Plugin hook worker execution and event-governance tests."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from filmu_py.core.event_bus import EventBus
from filmu_py.plugins.hooks import PluginHookWorkerExecutor
from filmu_py.plugins.manifest import PluginManifest
from filmu_py.plugins.registry import PluginCapabilityKind, PluginRegistry


async def _next_envelope(iterator: Any) -> Any:
    return await iterator.__anext__()


@dataclass
class _RecordingHook:
    subscribed_events: frozenset[str]
    delay_seconds: float = 0.0
    should_fail: bool = False
    handled: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    async def initialize(self, ctx: object) -> None:
        _ = ctx

    async def handle(self, event_type: str, payload: dict[str, Any]) -> None:
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.should_fail:
            raise RuntimeError("boom")
        self.handled.append((event_type, payload))


@pytest.mark.asyncio
async def test_event_bus_publish_dispatches_hooks_without_blocking_publisher() -> None:
    event_bus = EventBus()
    registry = PluginRegistry()
    registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "hook-plugin",
                "version": "1.0.0",
                "api_version": "1",
                "entry_module": "plugin.py",
                "event_hook": "ExampleHook",
                "publishable_events": ["hook-plugin.ready"],
            }
        )
    )
    hook = _RecordingHook(subscribed_events=frozenset({"item.completed"}), delay_seconds=0.1)
    registry.register_capability(
        plugin_name="hook-plugin",
        kind=PluginCapabilityKind.EVENT_HOOK,
        implementation=hook,
    )
    event_bus.attach_plugin_runtime(registry, hook_executor=PluginHookWorkerExecutor())

    started_at = time.perf_counter()
    await event_bus.publish("item.completed", {"item_id": "1"})
    elapsed = time.perf_counter() - started_at

    assert elapsed < 0.05
    await asyncio.sleep(0.15)
    assert hook.handled == [("item.completed", {"item_id": "1"})]


@pytest.mark.asyncio
async def test_event_bus_hook_failures_are_isolated_and_logged(caplog: pytest.LogCaptureFixture) -> None:
    event_bus = EventBus()
    registry = PluginRegistry()
    registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "hook-plugin",
                "version": "1.0.0",
                "api_version": "1",
                "entry_module": "plugin.py",
                "event_hook": "ExampleHook",
            }
        )
    )
    good_hook = _RecordingHook(subscribed_events=frozenset({"item.completed"}))
    bad_hook = _RecordingHook(subscribed_events=frozenset({"item.completed"}), should_fail=True)
    registry.register_capability(
        plugin_name="hook-plugin",
        kind=PluginCapabilityKind.EVENT_HOOK,
        implementation=good_hook,
    )
    registry.register_capability(
        plugin_name="hook-plugin",
        kind=PluginCapabilityKind.EVENT_HOOK,
        implementation=bad_hook,
    )
    event_bus.attach_plugin_runtime(registry, hook_executor=PluginHookWorkerExecutor())

    with caplog.at_level(logging.WARNING):
        await event_bus.publish("item.completed", {"item_id": "1"})
        await asyncio.sleep(0.05)

    assert good_hook.handled == [("item.completed", {"item_id": "1"})]
    assert any(record.message == "plugin_hook_failed" for record in caplog.records)


@pytest.mark.asyncio
async def test_plugin_hook_executor_times_out_hanging_hooks(caplog: pytest.LogCaptureFixture) -> None:
    event_bus = EventBus()
    registry = PluginRegistry()
    registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "hook-plugin",
                "version": "1.0.0",
                "api_version": "1",
                "entry_module": "plugin.py",
                "event_hook": "ExampleHook",
            }
        )
    )
    hanging_hook = _RecordingHook(subscribed_events=frozenset({"item.completed"}), delay_seconds=0.2)
    registry.register_capability(
        plugin_name="hook-plugin",
        kind=PluginCapabilityKind.EVENT_HOOK,
        implementation=hanging_hook,
    )
    event_bus.attach_plugin_runtime(
        registry,
        hook_executor=PluginHookWorkerExecutor(timeout_seconds=0.01),
    )

    with caplog.at_level(logging.WARNING):
        await event_bus.publish("item.completed", {"item_id": "2"})
        await asyncio.sleep(0.05)

    assert hanging_hook.handled == []
    assert any(record.message == "plugin_hook_failed" for record in caplog.records)


@pytest.mark.asyncio
async def test_event_bus_drops_undeclared_plugin_events(caplog: pytest.LogCaptureFixture) -> None:
    event_bus = EventBus()
    registry = PluginRegistry()
    registry.register_manifest(
        PluginManifest.model_validate(
            {
                "name": "alpha",
                "version": "1.0.0",
                "api_version": "1",
                "entry_module": "plugin.py",
                "publishable_events": ["alpha.allowed"],
            }
        )
    )
    event_bus.attach_plugin_runtime(registry, hook_executor=PluginHookWorkerExecutor())

    subscription = cast(Any, event_bus.subscribe("alpha.blocked"))
    iterator = subscription.__aiter__()
    next_event: asyncio.Task[Any] = asyncio.create_task(_next_envelope(iterator))

    with caplog.at_level(logging.WARNING):
        await event_bus.publish("alpha.blocked", {"status": "nope"})
        await asyncio.sleep(0.05)

    assert next_event.done() is False
    next_event.cancel()
    with suppress(asyncio.CancelledError):
        await next_event
    with suppress(AttributeError, TypeError):
        await subscription.aclose()
    assert any(record.message == "plugin_event_dropped" for record in caplog.records)
