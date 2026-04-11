"""Internal async pub/sub event bus for plugin hooks."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from prometheus_client import Counter

if TYPE_CHECKING:
    from filmu_py.plugins.hooks import PluginHookWorkerExecutor
    from filmu_py.plugins.registry import PluginRegistry

EVENTBUS_DROPPED_EVENTS = Counter(
    "filmu_py_eventbus_dropped_events_total",
    "Number of dropped events by topic and reason.",
    labelnames=("topic", "reason"),
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EventEnvelope:
    """Canonical event payload delivered through the internal bus."""

    topic: str
    payload: dict[str, Any]


class EventBus:
    """Simple in-memory async event bus.

    Note: this is process-local. It can be replaced by Redis Streams/Kafka later
    without changing plugin handler contracts.
    """

    def __init__(self, max_queue_size: int = 1_000):
        self._subscribers: dict[str, list[asyncio.Queue[EventEnvelope]]] = defaultdict(list)
        self._max_queue_size = max_queue_size
        self._known_topics: set[str] = set()
        self._replay_backplane: Any | None = None
        self._plugin_registry: PluginRegistry | None = None
        self._hook_executor: PluginHookWorkerExecutor | None = None
        self._background_tasks: set[asyncio.Task[None]] = set()

    def attach_replay_backplane(self, replay_backplane: Any) -> None:
        """Attach a durable replay journal without changing subscriber contracts."""

        self._replay_backplane = replay_backplane

    def attach_plugin_runtime(
        self,
        plugin_registry: PluginRegistry,
        *,
        hook_executor: PluginHookWorkerExecutor | None = None,
    ) -> None:
        """Attach plugin governance and hook dispatch collaborators."""

        if hook_executor is None:
            from filmu_py.plugins.hooks import PluginHookWorkerExecutor

            hook_executor = PluginHookWorkerExecutor()
        self._plugin_registry = plugin_registry
        self._hook_executor = hook_executor

    def _is_publish_allowed(self, topic: str) -> bool:
        plugin_registry = self._plugin_registry
        if plugin_registry is None:
            return True

        plugin_name = plugin_registry.plugin_for_namespaced_event(topic)
        if plugin_name is None:
            return True
        if plugin_registry.is_declared_publishable_event(plugin_name, topic):
            return True

        EVENTBUS_DROPPED_EVENTS.labels(topic=topic, reason="undeclared_plugin_event").inc()
        logger.warning(
            "plugin_event_dropped",
            extra={"plugin": plugin_name, "topic": topic, "reason": "undeclared_plugin_event"},
        )
        return False

    def _schedule_hook_dispatch(self, topic: str, payload: dict[str, Any]) -> None:
        plugin_registry = self._plugin_registry
        hook_executor = self._hook_executor
        if plugin_registry is None or hook_executor is None:
            return

        hooks = plugin_registry.get_event_hooks()
        if not hooks:
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(hook_executor.dispatch(topic, dict(payload), hooks))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _publish_envelope(self, envelope: EventEnvelope) -> None:
        """Synchronously broadcast an envelope to all active topic subscribers."""

        if envelope.topic not in self._subscribers:
            return

        dead_queues: list[asyncio.Queue[EventEnvelope]] = []
        for queue in self._subscribers[envelope.topic]:
            try:
                queue.put_nowait(envelope)
            except asyncio.QueueFull:
                EVENTBUS_DROPPED_EVENTS.labels(topic=envelope.topic, reason="queue_full").inc()
                dead_queues.append(queue)

        for queue in dead_queues:
            if queue in self._subscribers[envelope.topic]:
                self._subscribers[envelope.topic].remove(queue)

    async def publish(self, topic: str, payload: dict[str, Any]) -> None:
        """Publish an event to all subscribers of ``topic``."""

        if not self._is_publish_allowed(topic):
            return
        self._known_topics.add(topic)
        if self._replay_backplane is not None:
            try:
                await self._replay_backplane.publish(
                    topic,
                    payload,
                    tenant_id=payload.get("tenant_id")
                    if isinstance(payload.get("tenant_id"), str)
                    else None,
                )
            except Exception:
                logger.warning(
                    "replay_backplane_publish_failed",
                    extra={"topic": topic},
                    exc_info=True,
                )
        self._publish_envelope(EventEnvelope(topic=topic, payload=payload))
        self._schedule_hook_dispatch(topic, payload)

    def publish_nowait(self, topic: str, payload: dict[str, Any]) -> None:
        """Synchronously publish an event to all subscribers of ``topic``."""

        if not self._is_publish_allowed(topic):
            return
        self._known_topics.add(topic)
        if self._replay_backplane is not None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is not None:
                task = loop.create_task(
                    self._replay_backplane.publish(
                        topic,
                        payload,
                        tenant_id=payload.get("tenant_id")
                        if isinstance(payload.get("tenant_id"), str)
                        else None,
                    )
                )
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
        self._publish_envelope(EventEnvelope(topic=topic, payload=payload))
        self._schedule_hook_dispatch(topic, payload)

    async def subscribe(self, topic: str) -> AsyncIterator[EventEnvelope]:
        """Subscribe to a topic and yield envelopes as they arrive."""

        self._known_topics.add(topic)
        queue: asyncio.Queue[EventEnvelope] = asyncio.Queue(maxsize=self._max_queue_size)
        self._subscribers[topic].append(queue)

        try:
            while True:
                yield await queue.get()
        finally:
            if queue in self._subscribers[topic]:
                self._subscribers[topic].remove(queue)

    def known_topics(self) -> set[str]:
        """Return all topics seen through publish/subscribe operations."""

        return set(self._known_topics)
