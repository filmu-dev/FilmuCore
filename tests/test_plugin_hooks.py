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
from filmu_py.core.plugin_hook_queue_status import PluginHookQueueStatusStore
from filmu_py.plugins.hooks import (
    PluginHookWorkerExecutor,
    QueuedPluginHookDispatcher,
    queued_plugin_hook_job_id,
)
from filmu_py.plugins.manifest import PluginManifest
from filmu_py.plugins.registry import PluginCapabilityKind, PluginRegistry
from filmu_py.workers import tasks


async def _next_envelope(iterator: Any) -> Any:
    return await iterator.__anext__()


@dataclass
class _QueueStub:
    first_result: object | None = field(default_factory=object)
    exception: Exception | None = None
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = field(default_factory=list)

    async def enqueue_job(self, function: str, *args: Any, **kwargs: Any) -> object | None:
        self.calls.append((function, args, kwargs))
        if self.exception is not None:
            raise self.exception
        return self.first_result


@dataclass
class _HistoryRedis:
    lists: dict[str, list[bytes]] = field(default_factory=dict)

    async def lpush(self, key: str, *values: Any) -> int:
        bucket = self.lists.setdefault(key, [])
        for value in values:
            bucket.insert(0, value if isinstance(value, bytes) else str(value).encode("utf-8"))
        return len(bucket)

    async def ltrim(self, key: str, start: int, stop: int) -> bool:
        bucket = self.lists.get(key, [])
        self.lists[key] = bucket[start : stop + 1]
        return True

    async def lrange(self, key: str, start: int, stop: int) -> list[bytes]:
        bucket = self.lists.get(key, [])
        end = None if stop == -1 else stop + 1
        return bucket[start:end]


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


def test_queued_plugin_hook_job_id_is_stable_for_same_delivery_identity() -> None:
    payload = {
        "tenant_id": "tenant-main",
        "item_id": "item-1",
        "item_request_id": "request-1",
        "event_id": "evt-1",
        "status": "ready",
    }

    first = queued_plugin_hook_job_id(
        plugin_name="hook-plugin",
        event_type="hook-plugin.ready",
        payload=payload,
    )
    second = queued_plugin_hook_job_id(
        plugin_name="hook-plugin",
        event_type="hook-plugin.ready",
        payload=dict(payload),
    )

    assert first == second
    assert first.startswith("plugin-hook:hook-plugin:hook-plugin.ready:")


@pytest.mark.asyncio
async def test_event_bus_publish_queues_selected_plugin_hook_events() -> None:
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
    hook = _RecordingHook(subscribed_events=frozenset({"hook-plugin.ready"}))
    registry.register_capability(
        plugin_name="hook-plugin",
        kind=PluginCapabilityKind.EVENT_HOOK,
        implementation=hook,
    )
    queue = _QueueStub()
    dispatcher = QueuedPluginHookDispatcher(
        arq_redis=queue,
        queue_name="filmu-py",
        queued_events=frozenset({"hook-plugin.ready"}),
        fallback_executor=PluginHookWorkerExecutor(),
    )
    event_bus.attach_plugin_runtime(registry, hook_executor=dispatcher)

    await event_bus.publish(
        "hook-plugin.ready",
        {"tenant_id": "tenant-main", "item_id": "item-1", "status": "ready"},
    )
    await asyncio.sleep(0.01)

    assert hook.handled == []
    assert len(queue.calls) == 1
    function, args, kwargs = queue.calls[0]
    assert function == "dispatch_plugin_hook_event"
    assert args[:3] == (
        "hook-plugin",
        "hook-plugin.ready",
        {"tenant_id": "tenant-main", "item_id": "item-1", "status": "ready"},
    )
    assert isinstance(args[3], float)
    assert kwargs == {
        "_job_id": queued_plugin_hook_job_id(
            plugin_name="hook-plugin",
            event_type="hook-plugin.ready",
            payload={
                "tenant_id": "tenant-main",
                "item_id": "item-1",
                "status": "ready",
            },
        ),
        "_queue_name": "filmu-py",
    }


@pytest.mark.asyncio
async def test_event_bus_queued_plugin_hook_dispatch_dedupe_does_not_fall_back_in_process() -> None:
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
    hook = _RecordingHook(subscribed_events=frozenset({"hook-plugin.ready"}))
    registry.register_capability(
        plugin_name="hook-plugin",
        kind=PluginCapabilityKind.EVENT_HOOK,
        implementation=hook,
    )
    dispatcher = QueuedPluginHookDispatcher(
        arq_redis=_QueueStub(first_result=None),
        queue_name="filmu-py",
        queued_events=frozenset({"hook-plugin.ready"}),
        fallback_executor=PluginHookWorkerExecutor(),
    )
    event_bus.attach_plugin_runtime(registry, hook_executor=dispatcher)

    await event_bus.publish("hook-plugin.ready", {"item_id": "item-2"})
    await asyncio.sleep(0.05)

    assert hook.handled == []


@pytest.mark.asyncio
async def test_event_bus_queued_plugin_hook_dispatch_falls_back_in_process_when_enqueue_fails() -> None:
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
    hook = _RecordingHook(subscribed_events=frozenset({"hook-plugin.ready"}))
    registry.register_capability(
        plugin_name="hook-plugin",
        kind=PluginCapabilityKind.EVENT_HOOK,
        implementation=hook,
    )
    dispatcher = QueuedPluginHookDispatcher(
        arq_redis=_QueueStub(exception=RuntimeError("queue down")),
        queue_name="filmu-py",
        queued_events=frozenset({"hook-plugin.ready"}),
        fallback_executor=PluginHookWorkerExecutor(),
    )
    event_bus.attach_plugin_runtime(registry, hook_executor=dispatcher)

    await event_bus.publish("hook-plugin.ready", {"item_id": "item-2"})
    await asyncio.sleep(0.05)

    assert hook.handled == [("hook-plugin.ready", {"item_id": "item-2"})]


@pytest.mark.asyncio
async def test_dispatch_plugin_hook_event_worker_executes_matching_registered_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = PluginRegistry()
    hook = _RecordingHook(subscribed_events=frozenset({"hook-plugin.ready"}))
    history_redis = _HistoryRedis()
    registry.register_capability(
        plugin_name="hook-plugin",
        kind=PluginCapabilityKind.EVENT_HOOK,
        implementation=hook,
    )

    async def resolve_plugin_registry(_: dict[str, object]) -> PluginRegistry:
        return registry

    async def resolve_runtime_settings(_: dict[str, object]) -> Any:
        plugin_runtime = type("PluginRuntime", (), {"hook_timeout_seconds": 0.1})()
        return type(
            "Settings",
            (),
            {"plugin_runtime": plugin_runtime, "arq_queue_name": "filmu-py"},
        )()

    async def resolve_arq_redis(_: dict[str, object]) -> _HistoryRedis:
        return history_redis

    monkeypatch.setattr(tasks, "_resolve_plugin_registry", resolve_plugin_registry)
    monkeypatch.setattr(tasks, "_resolve_runtime_settings", resolve_runtime_settings)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", resolve_arq_redis)

    result = await tasks.dispatch_plugin_hook_event(
        {},
        "hook-plugin",
        "hook-plugin.ready",
        {"item_id": "item-3", "status": "queued"},
        10.0,
    )

    assert result == {
        "plugin_name": "hook-plugin",
        "event_type": "hook-plugin.ready",
        "handled": True,
        "matched_hooks": 1,
        "successful_hooks": 1,
        "timeout_hooks": 0,
        "failed_hooks": 0,
        "attempt": 1,
    }
    assert hook.handled == [("hook-plugin.ready", {"item_id": "item-3", "status": "queued"})]
    history = await PluginHookQueueStatusStore(history_redis, queue_name="filmu-py").history(limit=5)
    assert history[0].plugin_name == "hook-plugin"
    assert history[0].event_type == "hook-plugin.ready"
    assert history[0].successful_hooks == 1
    assert history[0].failed_hooks == 0
    assert history[0].timeout_hooks == 0
