"""Application factory and lifespan wiring."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any, cast

import httpx
import structlog
from arq.connections import ArqRedis, RedisSettings, create_pool
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis

from .api.router import RouteMetricsMiddleware, create_api_router
from .config import Settings, get_settings, reset_runtime_settings, set_runtime_settings
from .core import byte_streaming
from .core.cache import CacheManager
from .core.chunk_engine import ChunkCache
from .core.event_bus import EventBus
from .core.rate_limiter import DistributedRateLimiter
from .core.replay import RedisReplayEventBackplane
from .core.runtime_lifecycle import (
    RuntimeLifecycleHealth,
    RuntimeLifecyclePhase,
    RuntimeLifecycleState,
)
from .db import DatabaseRuntime, run_migrations
from .graphql import GraphQLPluginRegistry, create_graphql_router
from .logging import attach_log_stream, configure_logging, detach_log_stream
from .middleware import RequestIdMiddleware
from .observability import setup_observability
from .plugins import load_plugins, register_builtin_plugins
from .plugins.context import HostPluginDatasource, PluginContextProvider
from .plugins.registry import PluginRegistry
from .resources import AppResources
from .services.access_policy import AccessPolicyService
from .services.authorization_audit import AuthorizationDecisionAuditService
from .services.control_plane import ControlPlaneService
from .services.identity import SecurityIdentityService
from .services.media import MediaService
from .services.playback import (
    InProcessDirectPlaybackRefreshController,
    InProcessHlsFailedLeaseRefreshController,
    InProcessHlsRestrictedFallbackRefreshController,
    PlaybackSourceService,
    QueuedDirectPlaybackRefreshController,
    QueuedHlsFailedLeaseRefreshController,
    QueuedHlsRestrictedFallbackRefreshController,
)
from .services.plugin_governance import PluginGovernanceService
from .services.settings_service import load_settings
from .workers.tasks import enqueue_process_scraped_item

if TYPE_CHECKING:
    from .services.vfs_catalog import FilmuVfsCatalogSupplier
    from .services.vfs_server import FilmuVfsCatalogGrpcServer
else:
    class FilmuVfsCatalogSupplier:  # pragma: no cover - typing fallback only
        pass

    class FilmuVfsCatalogGrpcServer:  # pragma: no cover - typing fallback only
        async def start(self) -> None:
            raise RuntimeError("vfs unavailable")

RuntimeFilmuVfsCatalogSupplier: Any | None = None
RuntimeFilmuVfsCatalogGrpcServer: Any | None = None

try:
    from .services.vfs_catalog import FilmuVfsCatalogSupplier as RuntimeFilmuVfsCatalogSupplier
    from .services.vfs_server import FilmuVfsCatalogGrpcServer as RuntimeFilmuVfsCatalogGrpcServer

    _HAS_VFS = True
except ImportError:
    _HAS_VFS = False
    RuntimeFilmuVfsCatalogSupplier = None
    RuntimeFilmuVfsCatalogGrpcServer = None

logger = logging.getLogger(__name__)

_BACKFILL_IMDB_IDS_SENTINEL_KEY = "backfill:metadata_repair:v2:enqueued"


def _redis_from_settings(settings: Settings) -> Redis:
    """Build Redis client from settings."""

    return cast(Redis, Redis.from_url(str(settings.redis_url), decode_responses=False))


async def _ping_redis(redis: Redis) -> None:
    """Ping redis while handling sync/async typing differences across redis clients."""

    ping_result = redis.ping()
    if isinstance(ping_result, Awaitable):
        await ping_result


def _arq_queue_name(settings: Settings) -> str:
    """Normalize the ARQ queue name used by app-side enqueuers."""

    return settings.arq_queue_name.strip() or "filmu-py"


def build_playback_service(resources: AppResources) -> PlaybackSourceService:
    """Build the shared playback-resolution service for HTTP and future VFS consumers."""

    return PlaybackSourceService(
        resources.db,
        settings=resources.settings,
        rate_limiter=resources.rate_limiter,
    )


def build_security_identity_service(resources: AppResources) -> SecurityIdentityService:
    """Build the persisted identity-plane service for auth and tenant bootstrap."""

    return SecurityIdentityService(resources.db)


def build_access_policy_service(resources: AppResources) -> AccessPolicyService:
    """Build the persisted access-policy inventory service."""

    return AccessPolicyService(resources.db)


def build_control_plane_service(resources: AppResources) -> ControlPlaneService:
    """Build the persisted control-plane subscriber state service."""

    return ControlPlaneService(resources.db)


def build_authorization_audit_service(resources: AppResources) -> AuthorizationDecisionAuditService:
    """Build the persisted authorization-decision audit service."""

    return AuthorizationDecisionAuditService(resources.db)


def build_plugin_governance_service(resources: AppResources) -> PluginGovernanceService:
    """Build the persisted plugin-governance override service."""

    return PluginGovernanceService(resources.db)


def build_plugin_registry(
    *,
    graphql_plugin_registry: GraphQLPluginRegistry | None = None,
) -> PluginRegistry:
    """Build the shared plugin registry above the GraphQL-only baseline."""

    return PluginRegistry(graphql_registry=graphql_plugin_registry)


def build_plugin_context_provider(resources: AppResources) -> PluginContextProvider:
    """Build the approved plugin context provider from app-scoped resources."""

    return PluginContextProvider(
        settings=resources.plugin_settings_payload or resources.settings.to_compatibility_dict(),
        tenant_id="control-plane",
        event_bus=resources.event_bus,
        rate_limiter=cast("Any", resources.rate_limiter),
        cache=resources.cache,
        logger_factory=lambda plugin_name: cast(
            "Any", structlog.get_logger(f"filmu_py.plugins.{plugin_name}")
        ),
        datasource_factory=lambda _plugin_name, datasource_name: (
            HostPluginDatasource(
                session_factory=resources.db.session,
                http_client_factory=httpx.AsyncClient,
            )
            if datasource_name == "host"
            else None
        ),
    )


def build_playback_refresh_controller(
    resources: AppResources,
) -> InProcessDirectPlaybackRefreshController:
    """Build the app-scoped in-process direct-play refresh controller."""

    playback_service = resources.playback_service or build_playback_service(resources)
    return InProcessDirectPlaybackRefreshController(
        playback_service,
        rate_limiter=resources.rate_limiter,
    )


def build_hls_failed_lease_refresh_controller(
    resources: AppResources,
) -> InProcessHlsFailedLeaseRefreshController:
    """Build the app-scoped in-process selected-HLS failed-lease refresh controller."""

    playback_service = resources.playback_service or build_playback_service(resources)
    return InProcessHlsFailedLeaseRefreshController(
        playback_service,
        rate_limiter=resources.rate_limiter,
    )


def build_hls_restricted_fallback_refresh_controller(
    resources: AppResources,
) -> InProcessHlsRestrictedFallbackRefreshController:
    """Build the app-scoped in-process selected-HLS restricted-fallback refresh controller."""

    playback_service = resources.playback_service or build_playback_service(resources)
    return InProcessHlsRestrictedFallbackRefreshController(
        playback_service,
        rate_limiter=resources.rate_limiter,
    )


def build_vfs_catalog_supplier(resources: AppResources) -> FilmuVfsCatalogSupplier | None:
    """Build the proto-first catalog supplier for the future Rust sidecar."""

    if not _HAS_VFS:
        return None
    playback_service = resources.playback_service or build_playback_service(resources)
    runtime_supplier = RuntimeFilmuVfsCatalogSupplier
    if runtime_supplier is None:
        return None
    return cast(FilmuVfsCatalogSupplier, runtime_supplier(
        resources.db,
        playback_snapshot_supplier=playback_service,
    ))


def build_vfs_catalog_server(resources: AppResources) -> FilmuVfsCatalogGrpcServer | None:
    """Build the app-scoped gRPC bridge that exposes the FilmuVFS catalog supplier."""

    if not _HAS_VFS or resources.vfs_catalog_supplier is None:
        return None
    from .services.debrid import build_builtin_playback_provider_clients

    runtime_server = RuntimeFilmuVfsCatalogGrpcServer
    if runtime_server is None:
        return None
    return cast(FilmuVfsCatalogGrpcServer, runtime_server(
        bind_address=resources.settings.grpc_bind_address,
        supplier=resources.vfs_catalog_supplier,
        playback_clients=build_builtin_playback_provider_clients(resources.settings),
    ))


def _build_lifespan(
    settings: Settings,
    plugin_registry: PluginRegistry,
) -> Callable[[FastAPI], AbstractAsyncContextManager[None]]:
    """Create lifespan handler bound to resolved settings."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Initialize and teardown shared runtime resources."""

        runtime_lifecycle = RuntimeLifecycleState()
        app.state.runtime_lifecycle = runtime_lifecycle
        runtime_lifecycle.transition(
            RuntimeLifecyclePhase.BOOTSTRAP,
            detail="startup_initializing_core_dependencies",
        )
        redis = _redis_from_settings(settings)
        resources: AppResources | None = None
        db: DatabaseRuntime | None = None
        arq_redis: ArqRedis | None = None
        hls_governance_task: asyncio.Task[None] | None = None
        try:
            await _ping_redis(redis)
            if settings.run_migrations_on_startup:
                await asyncio.to_thread(run_migrations, settings.postgres_dsn)

            db = DatabaseRuntime(settings.postgres_dsn, echo=settings.env == "development")
            persisted_settings = await load_settings(db)
            runtime_settings = settings
            settings_source = "environment"
            if persisted_settings is not None:
                runtime_settings = Settings.from_compatibility_dict(persisted_settings)
                settings_source = "database"
            set_runtime_settings(runtime_settings)
            configure_logging(runtime_settings)
            logger.info(
                "startup.settings.loaded",
                extra={"source": settings_source, "persisted": persisted_settings is not None},
            )

            event_bus = EventBus()
            queue_name = _arq_queue_name(runtime_settings)
            if runtime_settings.arq_enabled:
                arq_redis = await create_pool(
                    RedisSettings.from_dsn(str(runtime_settings.redis_url)),
                    default_queue_name=queue_name,
                )
                sentinel_exists = (
                    await redis.exists(_BACKFILL_IMDB_IDS_SENTINEL_KEY)
                    if hasattr(redis, "exists")
                    else 0
                )
                if not sentinel_exists:
                    await arq_redis.enqueue_job("backfill_imdb_ids")
                    if hasattr(redis, "set"):
                        await redis.set(_BACKFILL_IMDB_IDS_SENTINEL_KEY, "1")
                    logger.info("backfill.imdb_ids.enqueued", extra={"one_shot": True})
                await arq_redis.enqueue_job(
                    "recover_incomplete_library",
                    _job_id="startup-recover-incomplete-library",
                )
                logger.info("recover_incomplete_library.enqueued", extra={"one_shot": True})
                await arq_redis.enqueue_job(
                    "retry_library",
                    _job_id="startup-retry-library",
                )
                logger.info("retry_library.enqueued", extra={"one_shot": True})
            else:
                logger.warning("backfill.imdb_ids.skipped", extra={"reason": "arq_not_enabled"})

            async def enqueue_scraped_item(item_id: str) -> None:
                if arq_redis is None:
                    return
                await enqueue_process_scraped_item(
                    arq_redis,
                    item_id=item_id,
                    queue_name=queue_name,
                )

            rate_limiter = DistributedRateLimiter(redis=redis)
            media_service = MediaService(
                db=db,
                event_bus=event_bus,
                scraped_item_enqueuer=(enqueue_scraped_item if arq_redis is not None else None),
                settings=runtime_settings,
                rate_limiter=rate_limiter,
            )

            resources = AppResources(
                settings=runtime_settings,
                redis=redis,
                cache=CacheManager(redis=redis, namespace="filmu_py"),
                chunk_cache=ChunkCache(max_bytes=256 * 1024 * 1024),
                rate_limiter=rate_limiter,
                event_bus=event_bus,
                db=db,
                media_service=media_service,
                graphql_plugin_registry=plugin_registry.graphql,
                runtime_lifecycle=runtime_lifecycle,
                plugin_registry=plugin_registry,
                plugin_settings_payload=(persisted_settings or runtime_settings.to_compatibility_dict()),
                arq_redis=arq_redis,
                arq_queue_name=queue_name,
            )
            resources.security_identity_service = build_security_identity_service(resources)
            await resources.security_identity_service.bootstrap(runtime_settings)
            resources.access_policy_service = build_access_policy_service(resources)
            resources.access_policy_snapshot = await resources.access_policy_service.bootstrap(
                runtime_settings
            )
            resources.authorization_audit_service = build_authorization_audit_service(resources)
            resources.control_plane_service = build_control_plane_service(resources)
            resources.plugin_governance_service = build_plugin_governance_service(resources)
            if runtime_settings.control_plane.event_backplane == "redis_stream":
                event_bus.attach_replay_backplane(
                    RedisReplayEventBackplane(
                        cast("Any", redis),
                        stream_name=runtime_settings.control_plane.event_stream_name,
                        maxlen=runtime_settings.control_plane.event_replay_maxlen,
                        subscription_state_sink=resources.control_plane_service,
                    )
                )
            runtime_lifecycle.transition(
                RuntimeLifecyclePhase.PLUGIN_REGISTRATION,
                detail="startup_registering_runtime_plugins",
            )
            plugin_context_provider = build_plugin_context_provider(resources)
            app.state.plugin_capability_load_report = await asyncio.to_thread(
                load_plugins,
                runtime_settings.plugins_dir,
                plugin_registry,
                context_provider=plugin_context_provider,
                host_version=runtime_settings.version,
                trust_store_path=runtime_settings.plugin_trust_store_path,
                strict_signatures=runtime_settings.plugin_strict_signatures,
                register_graphql=False,
                register_capabilities=True,
            )
            app.state.builtin_plugin_registrations = await asyncio.to_thread(
                register_builtin_plugins,
                plugin_registry,
                context_provider=plugin_context_provider,
            )
            plugin_context_provider.lock()
            resources.event_bus.attach_plugin_runtime(plugin_registry)
            resources.playback_service = build_playback_service(resources)
            resources.playback_refresh_controller = build_playback_refresh_controller(resources)
            resources.hls_failed_lease_refresh_controller = (
                build_hls_failed_lease_refresh_controller(resources)
            )
            resources.hls_restricted_fallback_refresh_controller = (
                build_hls_restricted_fallback_refresh_controller(resources)
            )
            if (
                runtime_settings.stream.refresh_dispatch_mode == "queued"
                and arq_redis is not None
            ):
                resources.queued_direct_playback_refresh_controller = (
                    QueuedDirectPlaybackRefreshController(
                        arq_redis,
                        queue_name=queue_name,
                    )
                )
                resources.queued_hls_failed_lease_refresh_controller = (
                    QueuedHlsFailedLeaseRefreshController(
                        arq_redis,
                        queue_name=queue_name,
                    )
                )
                resources.queued_hls_restricted_fallback_refresh_controller = (
                    QueuedHlsRestrictedFallbackRefreshController(
                        arq_redis,
                        queue_name=queue_name,
                    )
                )
            resources.vfs_catalog_supplier = build_vfs_catalog_supplier(resources)
            if resources.vfs_catalog_supplier is not None:
                resources.vfs_catalog_server = build_vfs_catalog_server(resources)
                if resources.vfs_catalog_server is not None:
                    await resources.vfs_catalog_server.start()
            app.state.resources = resources
            attach_log_stream(resources.log_stream)
            hls_governance_task = asyncio.create_task(byte_streaming.run_hls_governance_loop())
            plugin_failure_count = len(getattr(app.state.plugin_capability_load_report, "failed", []))
            runtime_lifecycle.transition(
                RuntimeLifecyclePhase.STEADY_STATE,
                health=(
                    RuntimeLifecycleHealth.DEGRADED
                    if plugin_failure_count
                    else RuntimeLifecycleHealth.HEALTHY
                ),
                detail=(
                    f"runtime_ready plugin_failures={plugin_failure_count}"
                    if plugin_failure_count
                    else "runtime_ready"
                ),
            )

            yield
        except Exception as exc:
            runtime_lifecycle.transition(
                RuntimeLifecyclePhase.DEGRADED,
                health=RuntimeLifecycleHealth.DEGRADED,
                detail=f"startup_failed:{type(exc).__name__}",
            )
            raise
        finally:
            runtime_lifecycle.transition(
                RuntimeLifecyclePhase.SHUTTING_DOWN,
                health=runtime_lifecycle.snapshot().health,
                detail="runtime_shutdown_started",
            )
            if hls_governance_task is not None:
                hls_governance_task.cancel()
                with suppress(asyncio.CancelledError):
                    await hls_governance_task
            if resources is not None and resources.vfs_catalog_server is not None:
                await resources.vfs_catalog_server.stop()
            detach_log_stream()
            if resources is not None and resources.hls_restricted_fallback_refresh_controller is not None:
                await resources.hls_restricted_fallback_refresh_controller.shutdown()
            if resources is not None and resources.hls_failed_lease_refresh_controller is not None:
                await resources.hls_failed_lease_refresh_controller.shutdown()
            if resources is not None and resources.playback_refresh_controller is not None:
                await resources.playback_refresh_controller.shutdown()
            if resources is not None and resources.arq_redis is not None:
                await resources.arq_redis.aclose()
            elif arq_redis is not None:
                await arq_redis.aclose()
            if db is not None:
                await db.dispose()
            await redis.aclose()
            reset_runtime_settings()

    return lifespan


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""

    current_settings = set_runtime_settings(settings or get_settings())
    configure_logging(current_settings)
    graphql_plugin_registry = GraphQLPluginRegistry()
    plugin_registry = build_plugin_registry(graphql_plugin_registry=graphql_plugin_registry)
    plugin_load_report = load_plugins(
        current_settings.plugins_dir,
        plugin_registry,
        host_version=current_settings.version,
        trust_store_path=current_settings.plugin_trust_store_path,
        strict_signatures=current_settings.plugin_strict_signatures,
        register_graphql=True,
        register_capabilities=False,
    )

    app = FastAPI(
        title=current_settings.service_name,
        version=current_settings.version,
        default_response_class=ORJSONResponse,
        lifespan=_build_lifespan(current_settings, plugin_registry),
    )

    app.add_middleware(RequestIdMiddleware)
    app.add_middleware(RouteMetricsMiddleware)
    app.state.plugin_load_report = plugin_load_report
    app.state.plugin_registry = plugin_registry
    app.include_router(create_api_router())
    app.include_router(create_graphql_router(plugin_registry.graphql), prefix="/graphql")
    setup_observability(app, current_settings)

    return app
