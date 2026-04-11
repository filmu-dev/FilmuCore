"""Typed runtime resources attached to FastAPI application state."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from redis.asyncio import Redis

from .config import Settings
from .core.cache import CacheManager
from .core.chunk_engine import ChunkCache
from .core.event_bus import EventBus
from .core.log_stream import LogStreamBroker
from .core.rate_limiter import DistributedRateLimiter
from .db.runtime import DatabaseRuntime
from .services.media import MediaService

if TYPE_CHECKING:
    from arq.connections import ArqRedis

    from .graphql.plugin_registry import GraphQLPluginRegistry
    from .plugins.registry import PluginRegistry
    from .services.access_policy import AccessPolicyService, AccessPolicySnapshot
    from .services.identity import SecurityIdentityService
    from .services.playback import (
        InProcessDirectPlaybackRefreshController,
        InProcessHlsFailedLeaseRefreshController,
        InProcessHlsRestrictedFallbackRefreshController,
        PlaybackSourceService,
    )
    from .services.vfs_catalog import FilmuVfsCatalogSupplier
    from .services.vfs_server import FilmuVfsCatalogGrpcServer


@dataclass(slots=True)
class AppResources:
    """Application-scoped runtime resources initialized at startup."""

    settings: Settings
    redis: Redis
    cache: CacheManager
    rate_limiter: DistributedRateLimiter
    event_bus: EventBus
    db: DatabaseRuntime
    media_service: MediaService
    graphql_plugin_registry: GraphQLPluginRegistry
    chunk_cache: ChunkCache | None = None
    plugin_registry: PluginRegistry | None = None
    security_identity_service: SecurityIdentityService | None = None
    access_policy_service: AccessPolicyService | None = None
    access_policy_snapshot: AccessPolicySnapshot | None = None
    plugin_settings_payload: Mapping[str, Any] | None = None
    playback_service: PlaybackSourceService | None = None
    playback_refresh_controller: InProcessDirectPlaybackRefreshController | None = None
    hls_failed_lease_refresh_controller: InProcessHlsFailedLeaseRefreshController | None = None
    hls_restricted_fallback_refresh_controller: (
        InProcessHlsRestrictedFallbackRefreshController | None
    ) = None
    vfs_catalog_supplier: FilmuVfsCatalogSupplier | None = None
    vfs_catalog_server: FilmuVfsCatalogGrpcServer | None = None
    log_stream: LogStreamBroker = field(default_factory=LogStreamBroker)
    arq_redis: ArqRedis | None = None
    arq_queue_name: str = "filmu-py"
