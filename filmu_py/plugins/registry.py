"""Plugin capability registry above the GraphQL-only baseline."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Coroutine
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from inspect import isawaitable
from typing import Any, cast

import structlog

from filmu_py.graphql.plugin_registry import (
    GraphQLPluginRegistry,
    GraphQLResolverKind,
    GraphQLResolverRegistration,
)
from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import (
    ContentServicePlugin,
    DownloaderPlugin,
    IndexerPlugin,
    NotificationPlugin,
    PluginEventHookWorker,
    ScraperPlugin,
)
from filmu_py.plugins.manifest import PluginManifest

logger = structlog.get_logger(__name__)


class PluginCapabilityKind(StrEnum):
    """Supported non-GraphQL plugin capability kinds."""

    SCRAPER = "scraper"
    DOWNLOADER = "downloader"
    INDEXER = "indexer"
    CONTENT_SERVICE = "content_service"
    NOTIFICATION = "notification"
    EVENT_HOOK = "event_hook"


@dataclass(frozen=True, slots=True)
class PluginCapabilityRegistration:
    """One registered plugin capability implementation."""

    kind: PluginCapabilityKind
    plugin_name: str
    implementation: object


_PROTOCOLS_BY_KIND: dict[PluginCapabilityKind, type[object]] = {
    PluginCapabilityKind.SCRAPER: ScraperPlugin,
    PluginCapabilityKind.DOWNLOADER: DownloaderPlugin,
    PluginCapabilityKind.INDEXER: IndexerPlugin,
    PluginCapabilityKind.CONTENT_SERVICE: ContentServicePlugin,
    PluginCapabilityKind.NOTIFICATION: NotificationPlugin,
    PluginCapabilityKind.EVENT_HOOK: PluginEventHookWorker,
}


class PluginRegistry:
    """Registry that tracks GraphQL and non-GraphQL plugin capabilities."""

    def __init__(self, *, graphql_registry: GraphQLPluginRegistry | None = None) -> None:
        self.graphql = graphql_registry or GraphQLPluginRegistry()
        self._capabilities_by_kind: dict[PluginCapabilityKind, list[object]] = defaultdict(list)
        self._capabilities_by_plugin: dict[str, list[PluginCapabilityRegistration]] = defaultdict(
            list
        )
        self._manifests_by_plugin: dict[str, PluginManifest] = {}

    def register(self, registration: GraphQLResolverRegistration) -> None:
        """Delegate GraphQL resolver registration to the embedded GraphQL registry."""

        self.graphql.register(registration)

    def register_many(
        self,
        plugin_name: str,
        kind: GraphQLResolverKind,
        resolvers: list[type[Any]],
    ) -> None:
        """Delegate bulk GraphQL resolver registration to the embedded GraphQL registry."""

        self.graphql.register_many(plugin_name, kind, resolvers)

    def safe_register_many(
        self,
        plugin_name: str,
        kind: GraphQLResolverKind,
        resolvers: list[object],
    ) -> tuple[int, list[str]]:
        """Delegate defensive GraphQL resolver registration to the embedded registry."""

        return self.graphql.safe_register_many(plugin_name, kind, resolvers)

    def resolvers_for(self, kind: GraphQLResolverKind) -> list[type[Any]]:
        """Return GraphQL resolvers for one root kind."""

        return self.graphql.resolvers_for(kind)

    def all_resolvers(self) -> list[type[Any]]:
        """Return all GraphQL resolvers preserving registration order."""

        return self.graphql.all_resolvers()

    def register_capability(
        self,
        *,
        plugin_name: str,
        kind: PluginCapabilityKind,
        implementation: object,
    ) -> None:
        """Register one non-GraphQL capability implementation."""

        typed_implementation = cast(Any, implementation)
        with suppress(AttributeError, TypeError):
            typed_implementation.plugin_name = plugin_name
        self._capabilities_by_kind[kind].append(implementation)
        self._capabilities_by_plugin[plugin_name].append(
            PluginCapabilityRegistration(
                kind=kind,
                plugin_name=plugin_name,
                implementation=implementation,
            )
        )

    def register_manifest(self, manifest: PluginManifest) -> None:
        """Register manifest metadata for governance and observability queries."""

        self._manifests_by_plugin[manifest.name] = manifest

    def manifest(self, plugin_name: str) -> PluginManifest | None:
        """Return one registered manifest by plugin name."""

        return self._manifests_by_plugin.get(plugin_name)

    def safe_register_capability(
        self,
        *,
        plugin_name: str,
        kind: PluginCapabilityKind,
        candidate: object,
        context: PluginContext,
    ) -> tuple[int, list[str]]:
        """Register one capability defensively, including context initialization."""

        if not plugin_name.strip():
            message = "plugin_name is empty"
            logger.warning(
                "plugin.capability.registration.rejected",
                reason=message,
                capability=kind.value,
            )
            return 0, [message]

        skipped: list[str] = []
        implementation = self._instantiate_candidate(candidate)
        if kind is PluginCapabilityKind.EVENT_HOOK:
            typed_implementation = cast(Any, implementation)
            with suppress(AttributeError, TypeError):
                typed_implementation.plugin_name = plugin_name
        protocol_type = _PROTOCOLS_BY_KIND[kind]
        if not isinstance(implementation, protocol_type):
            message = f"capability '{kind.value}' export {candidate!r} does not implement the required protocol"
            skipped.append(message)
            logger.warning(
                "plugin.capability.registration.skipped",
                plugin=plugin_name,
                capability=kind.value,
                reason=message,
            )
            return 0, skipped

        try:
            self._initialize_candidate(implementation, context)
        except Exception as exc:
            message = f"capability '{kind.value}' initialization failed: {exc}"
            skipped.append(message)
            logger.warning(
                "plugin.capability.registration.skipped",
                plugin=plugin_name,
                capability=kind.value,
                reason=message,
            )
            return 0, skipped

        self.register_capability(
            plugin_name=plugin_name,
            kind=kind,
            implementation=implementation,
        )
        return 1, skipped

    def get_scrapers(self) -> list[ScraperPlugin]:
        """Return registered scraper plugins in registration order."""

        return [
            plugin
            for plugin in self._capabilities_by_kind[PluginCapabilityKind.SCRAPER]
            if isinstance(plugin, ScraperPlugin)
        ]

    def get_downloaders(self) -> list[DownloaderPlugin]:
        """Return registered downloader plugins in registration order."""

        return [
            plugin
            for plugin in self._capabilities_by_kind[PluginCapabilityKind.DOWNLOADER]
            if isinstance(plugin, DownloaderPlugin)
        ]

    def get_indexers(self) -> list[IndexerPlugin]:
        """Return registered indexer plugins in registration order."""

        return [
            plugin
            for plugin in self._capabilities_by_kind[PluginCapabilityKind.INDEXER]
            if isinstance(plugin, IndexerPlugin)
        ]

    def get_content_services(self) -> list[ContentServicePlugin]:
        """Return registered content-service plugins in registration order."""

        return [
            plugin
            for plugin in self._capabilities_by_kind[PluginCapabilityKind.CONTENT_SERVICE]
            if isinstance(plugin, ContentServicePlugin)
        ]

    def get_notifications(self) -> list[NotificationPlugin]:
        """Return registered notification plugins in registration order."""

        return [
            plugin
            for plugin in self._capabilities_by_kind[PluginCapabilityKind.NOTIFICATION]
            if isinstance(plugin, NotificationPlugin)
        ]

    def get_event_hooks(self) -> list[PluginEventHookWorker]:
        """Return registered event-hook workers in registration order."""

        return [
            plugin
            for plugin in self._capabilities_by_kind[PluginCapabilityKind.EVENT_HOOK]
            if isinstance(plugin, PluginEventHookWorker)
        ]

    def all_plugin_names(self) -> set[str]:
        """Return every plugin name known through manifests or capability registrations."""

        return set(self._manifests_by_plugin) | set(self._capabilities_by_plugin)

    def publishable_events_by_plugin(self) -> dict[str, tuple[str, ...]]:
        """Return declared publishable events grouped by plugin name."""

        return {
            plugin_name: manifest.publishable_events
            for plugin_name, manifest in self._manifests_by_plugin.items()
        }

    def hook_subscriptions_by_plugin(self) -> dict[str, tuple[str, ...]]:
        """Return registered hook subscriptions grouped by plugin name."""

        grouped: dict[str, tuple[str, ...]] = {}
        for plugin_name, registrations in self._capabilities_by_plugin.items():
            subscriptions = sorted(
                {
                    event_type
                    for registration in registrations
                    if registration.kind is PluginCapabilityKind.EVENT_HOOK
                    for event_type in getattr(registration.implementation, "subscribed_events", ())
                    if isinstance(event_type, str)
                }
            )
            grouped[plugin_name] = tuple(subscriptions)
        return grouped

    def plugin_for_namespaced_event(self, event_type: str) -> str | None:
        """Return the owning plugin when an event uses a known plugin namespace."""

        if "." not in event_type:
            return None
        candidate = event_type.split(".", 1)[0]
        if candidate in self.all_plugin_names():
            return candidate
        return None

    def is_declared_publishable_event(self, plugin_name: str, event_type: str) -> bool:
        """Return whether a plugin declared a namespaced event for publishing."""

        manifest = self._manifests_by_plugin.get(plugin_name)
        if manifest is None:
            return False
        return manifest.declares_publishable_event(event_type)

    def by_plugin(self) -> dict[str, list[PluginCapabilityRegistration]]:
        """Return a read-copy of capability registrations grouped by plugin."""

        return {
            plugin_name: list(registrations)
            for plugin_name, registrations in self._capabilities_by_plugin.items()
        }

    @staticmethod
    def _instantiate_candidate(candidate: object) -> object:
        if isinstance(candidate, type):
            return candidate()
        return candidate

    @staticmethod
    def _initialize_candidate(candidate: object, context: PluginContext) -> None:
        typed_candidate = cast(Any, candidate)
        typed_candidate.plugin_name = context.plugin_name

        def _run_initialization(awaitable: object, *, phase: str) -> None:
            if not isawaitable(awaitable):
                raise TypeError(f"{phase} must return an awaitable")

            try:
                asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(cast(Coroutine[Any, Any, Any], awaitable))
                return

            raise RuntimeError("plugin initialization requires a synchronous load phase")

        datasource = context.datasource
        if datasource is not None and not getattr(datasource, "_initialized", False):
            _run_initialization(datasource.initialize(context), phase="datasource.initialize(ctx)")

        initialize = getattr(candidate, "initialize", None)
        if not callable(initialize):
            raise TypeError("missing initialize(ctx) coroutine")

        _run_initialization(initialize(context), phase="initialize(ctx)")
