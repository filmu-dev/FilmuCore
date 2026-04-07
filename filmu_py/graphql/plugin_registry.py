"""Plugin resolver registration primitives for GraphQL schema composition."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class GraphQLResolverKind(StrEnum):
    """Supported GraphQL resolver contribution kinds."""

    QUERY = "query"
    SETTINGS = "settings"
    MUTATION = "mutation"
    SUBSCRIPTION = "subscription"


@dataclass(frozen=True)
class GraphQLResolverRegistration:
    """Single resolver registration unit contributed by a plugin."""

    kind: GraphQLResolverKind
    plugin_name: str
    resolver: type[Any]


class GraphQLPluginRegistry:
    """In-memory plugin registry for GraphQL resolver composition."""

    def __init__(self) -> None:
        self._resolvers_by_kind: dict[GraphQLResolverKind, list[type[Any]]] = defaultdict(list)
        self._resolvers_by_plugin: dict[str, list[GraphQLResolverRegistration]] = defaultdict(list)

    def register(self, registration: GraphQLResolverRegistration) -> None:
        """Register a single plugin resolver class."""

        self._resolvers_by_kind[registration.kind].append(registration.resolver)
        self._resolvers_by_plugin[registration.plugin_name].append(registration)

    def register_many(
        self,
        plugin_name: str,
        kind: GraphQLResolverKind,
        resolvers: list[type[Any]],
    ) -> None:
        """Register multiple resolver classes for one plugin and resolver kind."""

        for resolver in resolvers:
            self.register(
                GraphQLResolverRegistration(
                    kind=kind,
                    plugin_name=plugin_name,
                    resolver=resolver,
                )
            )

    def safe_register_many(
        self,
        plugin_name: str,
        kind: GraphQLResolverKind,
        resolvers: list[object],
    ) -> tuple[int, list[str]]:
        """Register plugin resolvers defensively, skipping invalid entries.

        Returns a tuple of `(registered_count, skipped_reason_messages)`.
        """

        registered_count = 0
        skipped: list[str] = []

        if not plugin_name.strip():
            message = "plugin_name is empty"
            logger.warning("graphql.plugin.registration.rejected", reason=message, kind=kind.value)
            return 0, [message]

        for resolver in resolvers:
            if not isinstance(resolver, type):
                message = f"resolver {resolver!r} is not a class"
                skipped.append(message)
                logger.warning(
                    "graphql.plugin.registration.skipped",
                    plugin=plugin_name,
                    reason=message,
                    kind=kind.value,
                )
                continue

            self.register(
                GraphQLResolverRegistration(
                    kind=kind,
                    plugin_name=plugin_name,
                    resolver=resolver,
                )
            )
            registered_count += 1

        return registered_count, skipped

    def resolvers_for(self, kind: GraphQLResolverKind) -> list[type[Any]]:
        """Return resolver classes for a given root kind in registration order."""

        return list(self._resolvers_by_kind.get(kind, []))

    def all_resolvers(self) -> list[type[Any]]:
        """Return all registered resolver classes preserving registration order."""

        result: list[type[Any]] = []
        for resolvers in self._resolvers_by_kind.values():
            result.extend(resolvers)
        return result

    def by_plugin(self) -> dict[str, list[GraphQLResolverRegistration]]:
        """Return a read-copy of current plugin resolver registrations."""

        return {
            plugin: list(registrations)
            for plugin, registrations in self._resolvers_by_plugin.items()
        }
