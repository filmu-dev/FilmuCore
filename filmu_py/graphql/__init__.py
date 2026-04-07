"""GraphQL package exports for router/schema wiring."""

from .plugin_registry import (
    GraphQLPluginRegistry,
    GraphQLResolverKind,
    GraphQLResolverRegistration,
)
from .schema import build_schema, create_graphql_router

__all__ = [
    "GraphQLPluginRegistry",
    "GraphQLResolverKind",
    "GraphQLResolverRegistration",
    "build_schema",
    "create_graphql_router",
]
