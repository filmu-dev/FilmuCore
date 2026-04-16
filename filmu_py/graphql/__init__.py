"""GraphQL package exports for router/schema wiring."""

from __future__ import annotations

from .plugin_registry import (
    GraphQLPluginRegistry,
    GraphQLResolverKind,
    GraphQLResolverRegistration,
)

__all__ = [
    "GraphQLPluginRegistry",
    "GraphQLResolverKind",
    "GraphQLResolverRegistration",
    "build_schema",
    "create_graphql_router",
]


def build_schema(plugin_registry: GraphQLPluginRegistry):
    """Build the GraphQL schema without importing schema wiring eagerly."""

    from .schema import build_schema as _build_schema

    return _build_schema(plugin_registry)


def create_graphql_router(plugin_registry: GraphQLPluginRegistry):
    """Build the GraphQL router without importing schema wiring eagerly."""

    from .schema import create_graphql_router as _create_graphql_router

    return _create_graphql_router(plugin_registry)
