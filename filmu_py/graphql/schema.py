"""GraphQL schema root for filmu-python compatibility and future parity."""

# mypy: disable-error-code=untyped-decorator

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from typing import Any, cast

import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.scalars import JSON
from strawberry.subscriptions import GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL
from strawberry.types import Info

from filmu_py.graphql.deps import GraphQLContext, get_graphql_context
from filmu_py.graphql.observability import GraphQLOperationMetricsExtension
from filmu_py.graphql.plugin_registry import GraphQLPluginRegistry, GraphQLResolverKind
from filmu_py.graphql.resolvers import (
    CoreMutationResolver,
    CoreQueryResolver,
    build_filmu_settings,
)
from filmu_py.graphql.types import (
    GQLSettings,
    ItemStateChangedEvent,
    LogEntry,
    NotificationEvent,
)


def _stringify(value: object, *, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _optional_stringify(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


@strawberry.type
class CompatSubscriptionResolver:
    """GraphQL compat subscription surface mirroring the current SSE streams."""

    @strawberry.subscription(description="Mirrors the `item.state.changed` SSE payload.")
    async def item_state_changed(
        self,
        info: Info[GraphQLContext, object],
    ) -> AsyncGenerator[ItemStateChangedEvent, None]:
        async for envelope in info.context.event_bus.subscribe("item.state.changed"):
            payload = envelope.payload
            yield ItemStateChangedEvent(
                item_id=_stringify(payload.get("item_id")),
                from_state=_optional_stringify(payload.get("from_state")),
                to_state=_stringify(payload.get("to_state") or payload.get("state")),
                timestamp=_stringify(payload.get("timestamp")),
            )

    @strawberry.subscription(description="Structured log stream for future GraphQL consumers.")
    async def log_stream(
        self,
        info: Info[GraphQLContext, object],
        level: str | None = "INFO",
        item_id: str | None = None,
    ) -> AsyncGenerator[LogEntry, None]:
        async for payload in info.context.log_stream.subscribe_events(level=level, item_id=item_id):
            yield LogEntry(
                timestamp=payload.timestamp,
                level=payload.level,
                event=payload.event,
                worker_id=payload.worker_id,
                item_id=payload.item_id,
                stage=payload.stage,
                extra=cast(JSON, payload.extra or {}),
            )

    @strawberry.subscription(description="Mirrors the notifications SSE payload.")
    async def notifications(
        self,
        info: Info[GraphQLContext, object],
    ) -> AsyncGenerator[NotificationEvent, None]:
        async for envelope in info.context.event_bus.subscribe("notifications"):
            payload = envelope.payload
            yield NotificationEvent(
                event_type=_stringify(
                    payload.get("event_type") or envelope.topic,
                    default="notifications",
                ),
                title=_optional_stringify(payload.get("title")),
                message=_optional_stringify(payload.get("message") or payload.get("log_string")),
                timestamp=_stringify(payload.get("timestamp")),
            )


def _merge_resolvers(
    base: type[Any],
    extension_resolvers: Sequence[type[Any]],
    class_name: str,
) -> type[Any]:
    """Create merged resolver types for plugin-dfilmu schema composition."""

    merged_type = type(class_name, (*extension_resolvers, base), {})
    return strawberry.type(merged_type)


def _build_query_type(plugin_registry: GraphQLPluginRegistry) -> type[Any]:
    """Build the GraphQL query root, including plugin-contributed settings fields."""

    query_resolvers = plugin_registry.resolvers_for(GraphQLResolverKind.QUERY)
    settings_resolvers = plugin_registry.resolvers_for(GraphQLResolverKind.SETTINGS)
    settings_type = _merge_resolvers(GQLSettings, settings_resolvers, "Settings")

    def settings_resolver(info: Info[GraphQLContext, object]) -> Any:
        return settings_type(filmu=build_filmu_settings(info))

    query_type = type(
        "Query",
        (*query_resolvers, CoreQueryResolver),
        {
            "settings": strawberry.field(
                resolver=settings_resolver,
                graphql_type=settings_type,
                description="Compatibility settings root for GraphQL clients",
            )
        },
    )
    return strawberry.type(query_type)


def _build_root_types(
    plugin_registry: GraphQLPluginRegistry,
) -> tuple[type[Any], type[Any], type[Any]]:
    """Build query/mutation/subscription root types from core and plugin resolvers."""

    mutation_resolvers = plugin_registry.resolvers_for(GraphQLResolverKind.MUTATION)
    subscription_resolvers = plugin_registry.resolvers_for(GraphQLResolverKind.SUBSCRIPTION)

    query = _build_query_type(plugin_registry)
    mutation = _merge_resolvers(CoreMutationResolver, mutation_resolvers, "Mutation")
    subscription = _merge_resolvers(
        CompatSubscriptionResolver,
        subscription_resolvers,
        "Subscription",
    )
    return query, mutation, subscription


def build_schema(plugin_registry: GraphQLPluginRegistry) -> strawberry.Schema:
    """Build Strawberry schema from the current plugin registration snapshot."""

    query, mutation, subscription = _build_root_types(plugin_registry)
    return strawberry.Schema(
        query=query,
        mutation=mutation,
        subscription=subscription,
        extensions=[GraphQLOperationMetricsExtension],
    )


def create_graphql_router(plugin_registry: GraphQLPluginRegistry) -> GraphQLRouter:
    """Create GraphQL router wired to plugin-aware schema and typed context."""

    schema = build_schema(plugin_registry)
    context_getter: Any = get_graphql_context
    return cast(
        GraphQLRouter,
        GraphQLRouter(
            schema,
            context_getter=context_getter,
            subscription_protocols=[GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL],
        ),
    )
