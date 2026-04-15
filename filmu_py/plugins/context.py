"""Plugin context contracts and provider helpers."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol, cast

from filmu_py.plugins.interfaces import PluginDatasource
from filmu_py.plugins.settings import PluginSettingsRegistry

if TYPE_CHECKING:
    from filmu_py.plugins.interfaces import StreamControlInput, StreamControlResult


class PluginRateLimitDecision(Protocol):
    """Minimal rate-limit decision shape exposed to plugins."""

    allowed: bool
    remaining_tokens: float
    retry_after_seconds: float


class PluginRateLimiter(Protocol):
    """Limiter contract exposed to plugins."""

    async def acquire(
        self,
        bucket_key: str,
        capacity: float,
        refill_rate_per_second: float,
        requested_tokens: float = 1.0,
        now_seconds: float | None = None,
        expiry_seconds: int | None = None,
    ) -> PluginRateLimitDecision: ...


class PluginCache(Protocol):
    """Cache contract exposed to plugins."""

    async def get(self, key: str) -> bytes | None: ...

    async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None: ...

    async def delete(self, key: str) -> None: ...


class PluginEventBus(Protocol):
    """Event-bus contract exposed to plugins."""

    async def publish(self, topic: str, payload: dict[str, Any]) -> None: ...

    def publish_nowait(self, topic: str, payload: dict[str, Any]) -> None: ...

    def subscribe(self, topic: str) -> AsyncIterator[object]: ...

    def known_topics(self) -> set[str]: ...


class PluginLogger(Protocol):
    """Minimal logger surface exposed to plugins."""

    def debug(self, message: object, *args: object, **kwargs: Any) -> None: ...

    def info(self, message: object, *args: object, **kwargs: Any) -> None: ...

    def warning(self, message: object, *args: object, **kwargs: Any) -> None: ...

    def error(self, message: object, *args: object, **kwargs: Any) -> None: ...

    def exception(self, message: object, *args: object, **kwargs: Any) -> None: ...


class PluginStreamControlGateway(Protocol):
    """Host-controlled stream/status gateway exposed to authorized plugins."""

    async def control(self, request: StreamControlInput) -> StreamControlResult:
        pass


@dataclass(frozen=True, slots=True)
class PluginContext:
    """Scoped runtime context injected into one plugin implementation."""

    plugin_name: str
    tenant_id: str
    settings: Mapping[str, Any]
    event_bus: PluginEventBus
    rate_limiter: PluginRateLimiter
    cache: PluginCache
    logger: PluginLogger
    datasource: PluginDatasource | None = None


@dataclass(slots=True)
class HostPluginDatasource:
    """Default limited datasource containing only host-approved plugin dependencies."""

    session_factory: Callable[[], Any] | None = None
    http_client_factory: Callable[[], Any] | None = None
    stream_control: PluginStreamControlGateway | None = None

    async def initialize(self, ctx: PluginContext) -> None:
        _ = ctx

    async def teardown(self) -> None:
        return None


@dataclass(slots=True)
class PluginContextProvider:
    """Build read-only plugin contexts from approved host dependencies."""

    settings: Mapping[str, Any]
    event_bus: PluginEventBus
    rate_limiter: PluginRateLimiter
    cache: PluginCache
    tenant_id: str = "control-plane"
    logger_factory: Callable[[str], PluginLogger] | None = None
    settings_registry: PluginSettingsRegistry | None = None
    datasource_factory: Callable[[str, str | None], PluginDatasource | None] | None = None

    def lock(self) -> None:
        """Freeze the underlying settings registry after plugin bootstrap."""

        self._ensure_settings_registry().lock()

    def register_plugin_settings(
        self,
        plugin_name: str,
        *,
        schema: dict[str, Any] | None = None,
    ) -> None:
        """Populate scoped settings for one plugin when not yet registered."""

        registry = self._ensure_settings_registry()
        if registry.has(plugin_name):
            return
        registry.register(
            plugin_name,
            schema=schema or {},
            values=self._extract_plugin_settings(plugin_name),
        )

    def build(
        self,
        plugin_name: str,
        *,
        settings_schema: dict[str, Any] | None = None,
        datasource_name: str | None = None,
    ) -> PluginContext:
        """Build one scoped plugin context for the requested plugin name."""

        self.register_plugin_settings(plugin_name, schema=settings_schema)
        logger_factory = self.logger_factory or self._default_logger_factory
        settings_view: Mapping[str, Any] = MappingProxyType(
            self._ensure_settings_registry().get(plugin_name)
        )
        return PluginContext(
            plugin_name=plugin_name,
            tenant_id=self.tenant_id,
            settings=settings_view,
            event_bus=self.event_bus,
            rate_limiter=self.rate_limiter,
            cache=self.cache,
            logger=logger_factory(plugin_name),
            datasource=(
                self.datasource_factory(plugin_name, datasource_name)
                if self.datasource_factory is not None and datasource_name is not None
                else None
            ),
        )

    def _ensure_settings_registry(self) -> PluginSettingsRegistry:
        if self.settings_registry is None:
            self.settings_registry = PluginSettingsRegistry()
        return self.settings_registry

    def _extract_plugin_settings(self, plugin_name: str) -> dict[str, Any]:
        candidate_keys = {
            plugin_name,
            plugin_name.replace("-", "_"),
            plugin_name.replace("_", "-"),
        }

        for container_name in ("plugins", "scraping", "downloaders", "notifications", "content"):
            container = self.settings.get(container_name)
            if not isinstance(container, Mapping):
                continue
            for key in candidate_keys:
                scoped = container.get(key)
                if isinstance(scoped, Mapping):
                    return deepcopy(dict(scoped))

        for key in candidate_keys:
            scoped = self.settings.get(key)
            if isinstance(scoped, Mapping):
                return deepcopy(dict(scoped))

        return {}

    @staticmethod
    def _default_logger_factory(plugin_name: str) -> PluginLogger:
        import logging

        return cast(PluginLogger, logging.getLogger(f"filmu_py.plugins.{plugin_name}"))
