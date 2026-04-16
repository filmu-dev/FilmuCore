"""GraphQL package exports for router/schema wiring.

Keep schema/router imports lazy so plugin-manifest and worker bootstrap paths can
import GraphQL registration primitives without pulling the full app graph.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .plugin_registry import (
    GraphQLPluginRegistry,
    GraphQLResolverKind,
    GraphQLResolverRegistration,
)

if TYPE_CHECKING:
    import strawberry
    from strawberry.fastapi import GraphQLRouter

__all__ = [
    "GraphQLPluginRegistry",
    "GraphQLResolverKind",
    "GraphQLResolverRegistration",
    "build_schema",
    "create_graphql_router",
]


def build_schema(plugin_registry: GraphQLPluginRegistry) -> strawberry.Schema:
    """Build the GraphQL schema lazily to avoid bootstrap-time import cycles."""

    from .schema import build_schema as _build_schema

    return _build_schema(plugin_registry)


def create_graphql_router(plugin_registry: GraphQLPluginRegistry) -> GraphQLRouter:
    """Create the GraphQL router lazily to avoid bootstrap-time import cycles."""

    from .schema import create_graphql_router as _create_graphql_router

    return _create_graphql_router(plugin_registry)


def __getattr__(name: str) -> Any:
    """Expose lazy schema helpers for attribute-based imports."""

    if name == "build_schema":
        return build_schema
    if name == "create_graphql_router":
        return create_graphql_router
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
