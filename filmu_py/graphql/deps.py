"""GraphQL dependency adapters and context wiring helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fastapi import Request
from strawberry.fastapi import BaseContext

from filmu_py.api.deps import get_resources
from filmu_py.core.event_bus import EventBus
from filmu_py.core.log_stream import LogStreamBroker
from filmu_py.resources import AppResources
from filmu_py.services.media import MediaService
from filmu_py.services.settings_service import update_settings_path


@dataclass
class GraphQLContext(BaseContext):
    """Typed Strawberry context payload for resolver access."""

    request: Request
    resources: AppResources
    media_service: MediaService
    event_bus: EventBus
    log_stream: LogStreamBroker
    settings_updater: Callable[[str, Any], Any]


def get_graphql_context(request: Request) -> GraphQLContext:
    """Return typed Strawberry context with shared runtime resources."""

    resources: AppResources = get_resources(request)

    async def _settings_updater(path: str, value: Any) -> bool:
        await update_settings_path(request=request, db=resources.db, path=path, value=value)
        return True

    return GraphQLContext(
        request=request,
        resources=resources,
        media_service=resources.media_service,
        event_bus=resources.event_bus,
        log_stream=resources.log_stream,
        settings_updater=_settings_updater,
    )
