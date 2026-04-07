"""Standalone testing helpers for plugin authors and host-side plugin tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar

from filmu_py.plugins.context import (
    HostPluginDatasource,
    PluginContext,
    PluginContextProvider,
    PluginRateLimitDecision,
)
from filmu_py.plugins.settings import PluginSettingsRegistry


@dataclass(slots=True)
class TestRateLimitDecision:
    """Deterministic rate-limit decision used by the plugin test harness."""

    allowed: bool
    remaining_tokens: float
    retry_after_seconds: float


@dataclass(slots=True)
class TestRateLimiter:
    """In-memory rate limiter test double with queued decisions."""

    decisions: list[TestRateLimitDecision] = field(default_factory=list)
    requests: list[tuple[str, float, float, float, float | None, int | None]] = field(
        default_factory=list
    )

    async def acquire(
        self,
        bucket_key: str,
        capacity: float,
        refill_rate_per_second: float,
        requested_tokens: float = 1.0,
        now_seconds: float | None = None,
        expiry_seconds: int | None = None,
    ) -> PluginRateLimitDecision:
        self.requests.append(
            (
                bucket_key,
                capacity,
                refill_rate_per_second,
                requested_tokens,
                now_seconds,
                expiry_seconds,
            )
        )
        if self.decisions:
            return self.decisions.pop(0)
        return TestRateLimitDecision(
            allowed=True,
            remaining_tokens=capacity,
            retry_after_seconds=0.0,
        )


@dataclass(slots=True)
class TestCache:
    """Simple in-memory cache used by the plugin test harness."""

    values: dict[str, bytes] = field(default_factory=dict)

    async def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
        _ = ttl_seconds
        self.values[key] = value

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)


@dataclass(slots=True)
class TestEventBus:
    """In-memory event bus used by the plugin test harness."""

    max_queue_size: int = 100
    _subscribers: dict[str, list[asyncio.Queue[dict[str, Any]]]] = field(default_factory=dict)
    _known_topics: set[str] = field(default_factory=set)

    async def publish(self, topic: str, payload: dict[str, Any]) -> None:
        self.publish_nowait(topic, payload)

    def publish_nowait(self, topic: str, payload: dict[str, Any]) -> None:
        self._known_topics.add(topic)
        for queue in list(self._subscribers.get(topic, [])):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                continue

    def subscribe(self, topic: str) -> AsyncIterator[object]:
        self._known_topics.add(topic)
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self.max_queue_size)
        self._subscribers.setdefault(topic, []).append(queue)
        return TestEventBus._Subscription(topic=topic, bus=self, queue=queue)

    @dataclass(slots=True)
    class _Subscription:
        topic: str
        bus: TestEventBus
        queue: asyncio.Queue[dict[str, Any]]

        def __aiter__(self) -> TestEventBus._Subscription:
            return self

        async def __anext__(self) -> dict[str, Any]:
            return await self.queue.get()

        async def aclose(self) -> None:
            subscribers = self.bus._subscribers.get(self.topic, [])
            if self.queue in subscribers:
                subscribers.remove(self.queue)

    def known_topics(self) -> set[str]:
        return set(self._known_topics)


@dataclass(slots=True)
class TestLogger:
    """Tiny logger implementation that records emitted entries."""

    entries: list[tuple[str, object, dict[str, Any]]] = field(default_factory=list)

    def debug(self, message: object, *args: object, **kwargs: Any) -> None:
        _ = args
        self.entries.append(("debug", message, dict(kwargs)))

    def info(self, message: object, *args: object, **kwargs: Any) -> None:
        _ = args
        self.entries.append(("info", message, dict(kwargs)))

    def warning(self, message: object, *args: object, **kwargs: Any) -> None:
        _ = args
        self.entries.append(("warning", message, dict(kwargs)))

    def error(self, message: object, *args: object, **kwargs: Any) -> None:
        _ = args
        self.entries.append(("error", message, dict(kwargs)))

    def exception(self, message: object, *args: object, **kwargs: Any) -> None:
        _ = args
        self.entries.append(("exception", message, dict(kwargs)))


@dataclass(slots=True)
class TestPluginContext:
    """Standalone plugin test harness without a full application boot."""

    __test__: ClassVar[bool] = False

    settings: Mapping[str, Any] = field(default_factory=dict)
    event_bus: TestEventBus = field(default_factory=TestEventBus)
    rate_limiter: TestRateLimiter = field(default_factory=TestRateLimiter)
    cache: TestCache = field(default_factory=TestCache)
    logger: TestLogger = field(default_factory=TestLogger)
    settings_registry: PluginSettingsRegistry = field(default_factory=PluginSettingsRegistry)
    datasource_factory: Any = None

    def provider(self) -> PluginContextProvider:
        """Return a provider that builds plugin contexts from the harness doubles."""

        return PluginContextProvider(
            settings=self.settings,
            event_bus=self.event_bus,
            rate_limiter=self.rate_limiter,
            cache=self.cache,
            logger_factory=lambda _plugin_name: self.logger,
            settings_registry=self.settings_registry,
            datasource_factory=(
                self.datasource_factory
                if self.datasource_factory is not None
                else lambda _plugin_name, datasource_name: (
                    HostPluginDatasource(
                        session_factory=lambda: object(),
                        http_client_factory=lambda: object(),
                    )
                    if datasource_name == "host"
                    else None
                )
            ),
        )

    def build(self, plugin_name: str) -> PluginContext:
        """Build one plugin context directly for convenience in tests."""

        return self.provider().build(plugin_name)
