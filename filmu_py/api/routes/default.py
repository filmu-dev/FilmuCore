"""Default compatibility routes."""

import asyncio
import json
import re
from secrets import token_hex
from typing import Annotated, Any, Literal, cast
from uuid import uuid4

from fastapi import APIRouter, Depends, Query, Request
from pydantic import SecretStr

from filmu_py.api.deps import get_auth_context, require_permissions
from filmu_py.audit import audit_action
from filmu_py.config import set_runtime_settings
from filmu_py.core.queue_status import QueueStatusReader
from filmu_py.services.debrid import DownloaderAccountService
from filmu_py.services.settings_service import save_settings

from ..models import (
    ApiKeyRotationResponse,
    AuthContextResponse,
    CalendarItemResponse,
    CalendarReleaseDataResponse,
    CalendarResponse,
    HealthResponse,
    LogsResponse,
    MessageResponse,
    PluginCapabilityStatusResponse,
    PluginEventStatusResponse,
    QueueAlertResponse,
    QueueStatusHistoryPointResponse,
    QueueStatusHistoryResponse,
    QueueStatusResponse,
    StatsMediaYearRelease,
    StatsResponse,
)

router = APIRouter(tags=["default"])
_MAX_API_KEY_ID_LENGTH = 128
_API_KEY_ID_SUFFIX_LENGTH = 12

API_KEY_ROTATION_WARNING = (
    "Update BACKEND_API_KEY in your frontend environment and restart the frontend "
    "server before your next request, or all API calls will fail."
)


def _plugin_load_report_maps(request: Request) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return plugin-load successes and failures keyed by plugin name when available."""

    report = getattr(request.app.state, "plugin_load_report", None)
    loaded: dict[str, Any] = {}
    failed: dict[str, Any] = {}
    if report is None:
        return loaded, failed

    for success in getattr(report, "loaded", []):
        plugin_name = getattr(success, "plugin_name", None)
        if isinstance(plugin_name, str) and plugin_name:
            loaded[plugin_name] = success
    for failure in getattr(report, "failed", []):
        plugin_name = getattr(failure, "plugin_name", None)
        if isinstance(plugin_name, str) and plugin_name:
            failed[plugin_name] = failure
    return loaded, failed


def _plugin_runtime_health(
    plugin_name: str,
    resources: Any,
) -> tuple[bool, bool | None, list[str]]:
    """Return operator-facing readiness for built-in/runtime-managed plugins."""

    warnings: list[str] = []
    configured: bool | None = None

    if plugin_name == "stremthru":
        stremthru = resources.settings.downloaders.stremthru
        configured = bool(stremthru.enabled and stremthru.token.strip())
        if stremthru.enabled and not stremthru.token.strip():
            warnings.append("enabled but token is missing")
        if stremthru.token.strip() and not stremthru.enabled:
            warnings.append("token is configured but the plugin is disabled")
        return configured, configured, warnings

    return True, configured, warnings


def _plugin_signature_fields(*, manifest: Any, success: Any) -> dict[str, Any]:
    """Return operator-facing trust-store verification details when available."""

    return {
        "signature_present": bool(
            getattr(success, "signature_present", False) or (manifest and manifest.signature)
        ),
        "signature_verified": bool(getattr(success, "signature_verified", False)),
        "signature_verification_reason": getattr(success, "signature_verification_reason", None),
        "trust_policy_decision": getattr(success, "trust_policy_decision", None),
        "trust_store_source": getattr(success, "trust_store_source", None),
    }


def _next_api_key_id(auth_context: Any) -> str:
    """Return a non-secret service-account key identifier for rotations."""

    raw_actor_id = str(getattr(auth_context, "actor_id", "service"))
    normalized_prefix = re.sub(r"[^A-Za-z0-9._-]+", "-", raw_actor_id).strip("-._") or "service"
    max_prefix_length = _MAX_API_KEY_ID_LENGTH - _API_KEY_ID_SUFFIX_LENGTH - 1
    bounded_prefix = normalized_prefix[:max_prefix_length].rstrip("-._") or "service"
    return f"{bounded_prefix}-{uuid4().hex[:_API_KEY_ID_SUFFIX_LENGTH]}"


def _generate_api_key() -> str:
    """Return a strong API key candidate for compatibility-driven admin flows.

    The current python backend does not yet support persisted settings mutation, so
    this helper generates a replacement candidate without applying it to the live
    process configuration. That keeps the frontend settings UX unblocked without
    invalidating the currently configured BFF/backend trust relationship mid-session.
    """

    return token_hex(32)


@router.get("/", operation_id="default.root", response_model=MessageResponse)
async def root() -> MessageResponse:
    """Compatibility root endpoint."""

    return MessageResponse(message="Filmu Python compatibility backend is running")


@router.get("/health", operation_id="default.health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    """Health endpoint compatible with frontend expectations."""

    resources = request.app.state.resources
    checks: dict[str, str] = {"redis": "unknown"}

    try:
        await asyncio.wait_for(resources.redis.ping(), timeout=1.0)
        checks["redis"] = "ok"
    except Exception:
        checks["redis"] = "unreachable"

    status: Literal["healthy", "degraded", "unhealthy"] = (
        "healthy" if all(value == "ok" for value in checks.values()) else "degraded"
    )
    return HealthResponse(
        message="OK" if status == "healthy" else "DEGRADED",
        service=resources.settings.service_name,
        status=status,
        checks=checks,
    )


@router.get(
    "/auth/context",
    operation_id="default.auth_context",
    response_model=AuthContextResponse,
)
async def get_auth_identity_context(request: Request) -> AuthContextResponse:
    """Return the current authenticated actor plus persisted identity-plane mapping."""

    auth_context = get_auth_context(request)
    identity = getattr(request.state, "auth_identity", None)
    return AuthContextResponse(
        authentication_mode=auth_context.authentication_mode,
        api_key_id=auth_context.api_key_id,
        actor_id=auth_context.actor_id,
        actor_type=auth_context.actor_type,
        tenant_id=auth_context.tenant_id,
        roles=list(auth_context.roles),
        scopes=list(auth_context.scopes),
        effective_permissions=list(auth_context.effective_permissions),
        principal_key=getattr(identity, "principal_key", None),
        principal_type=getattr(identity, "principal_type", None),
        service_account_api_key_id=getattr(identity, "service_account_api_key_id", None),
    )


@router.get("/logs", operation_id="default.logs", response_model=LogsResponse)
async def get_logs(request: Request) -> LogsResponse:
    """Return bounded in-memory historical logs for frontend historical log views."""

    resources = request.app.state.resources
    return LogsResponse(logs=resources.log_stream.history())


@router.get(
    "/workers/queue",
    operation_id="default.worker_queue",
    response_model=QueueStatusResponse,
)
async def get_worker_queue_status(request: Request) -> QueueStatusResponse:
    """Return ARQ queue depth, lag, retry, and dead-letter visibility."""

    resources = request.app.state.resources
    queue_name = resources.arq_queue_name or resources.settings.arq_queue_name
    redis = resources.arq_redis or resources.redis
    snapshot = await QueueStatusReader(redis, queue_name=queue_name).snapshot()
    return QueueStatusResponse(
        queue_name=snapshot.queue_name,
        arq_enabled=resources.settings.arq_enabled,
        observed_at=snapshot.observed_at,
        total_jobs=snapshot.total_jobs,
        ready_jobs=snapshot.ready_jobs,
        deferred_jobs=snapshot.deferred_jobs,
        in_progress_jobs=snapshot.in_progress_jobs,
        retry_jobs=snapshot.retry_jobs,
        result_jobs=snapshot.result_jobs,
        dead_letter_jobs=snapshot.dead_letter_jobs,
        alert_level=snapshot.alert_level,
        alerts=[
            QueueAlertResponse(
                code=alert.code,
                severity=alert.severity,
                message=alert.message,
            )
            for alert in snapshot.alerts
        ],
        oldest_ready_age_seconds=snapshot.oldest_ready_age_seconds,
        next_scheduled_in_seconds=snapshot.next_scheduled_in_seconds,
    )


@router.get(
    "/workers/queue/history",
    operation_id="default.worker_queue_history",
    response_model=QueueStatusHistoryResponse,
)
async def get_worker_queue_history(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> QueueStatusHistoryResponse:
    """Return bounded queue trend history captured during queue observations."""

    resources = request.app.state.resources
    queue_name = resources.arq_queue_name or resources.settings.arq_queue_name
    redis = resources.arq_redis or resources.redis
    history = await QueueStatusReader(redis, queue_name=queue_name).history(limit=limit)
    return QueueStatusHistoryResponse(
        queue_name=queue_name,
        history=[
            QueueStatusHistoryPointResponse(
                observed_at=item.observed_at,
                total_jobs=item.total_jobs,
                ready_jobs=item.ready_jobs,
                deferred_jobs=item.deferred_jobs,
                in_progress_jobs=item.in_progress_jobs,
                retry_jobs=item.retry_jobs,
                dead_letter_jobs=item.dead_letter_jobs,
                oldest_ready_age_seconds=item.oldest_ready_age_seconds,
                next_scheduled_in_seconds=item.next_scheduled_in_seconds,
                alert_level=item.alert_level,
            )
            for item in history
        ],
    )


@router.get("/services", operation_id="default.services", response_model=dict[str, dict[str, bool]])
async def get_services(request: Request) -> dict[str, dict[str, bool]]:
    """Return a real provider enablement map for dashboard compatibility."""

    resources = request.app.state.resources
    return {
        "real_debrid": {"enabled": bool(resources.settings.downloaders.real_debrid.api_key)},
        "all_debrid": {"enabled": bool(resources.settings.downloaders.all_debrid.api_key)},
        "debrid_link": {"enabled": bool(resources.settings.downloaders.debrid_link.api_key)},
        "mdblist": {"enabled": bool(resources.settings.content.mdblist.api_key)},
    }


@router.get(
    "/plugins",
    operation_id="default.plugins",
    response_model=list[PluginCapabilityStatusResponse],
)
async def get_plugins(request: Request) -> list[PluginCapabilityStatusResponse]:
    """Return loaded non-GraphQL capability plugins for runtime visibility."""

    resources = request.app.state.resources
    plugin_registry = resources.plugin_registry
    loaded_report, failed_report = _plugin_load_report_maps(request)
    if plugin_registry is None:
        return [
            PluginCapabilityStatusResponse(
                name=plugin_name,
                capabilities=[],
                status="load_failed",
                ready=False,
                source=getattr(failure, "source", None),
                error=getattr(failure, "reason", None),
            )
            for plugin_name, failure in sorted(failed_report.items())
        ]

    responses: list[PluginCapabilityStatusResponse] = []
    registrations_by_plugin = plugin_registry.by_plugin()
    all_plugin_names = plugin_registry.all_plugin_names() | set(failed_report)
    for plugin_name in sorted(all_plugin_names):
        manifest = plugin_registry.manifest(plugin_name)
        registrations = registrations_by_plugin.get(plugin_name, [])
        success = loaded_report.get(plugin_name)
        failure = failed_report.get(plugin_name)
        ready, configured, warnings = _plugin_runtime_health(plugin_name, resources)
        signature_fields = _plugin_signature_fields(manifest=manifest, success=success)
        if success is not None:
            warnings.extend(list(getattr(success, "skipped", ())))
        if failure is not None and not registrations:
            responses.append(
                PluginCapabilityStatusResponse(
                    name=plugin_name,
                    capabilities=[],
                    status="load_failed",
                    ready=False,
                    version=manifest.version if manifest is not None else None,
                    api_version=manifest.api_version if manifest is not None else None,
                    min_host_version=manifest.min_host_version if manifest is not None else None,
                    max_host_version=manifest.max_host_version if manifest is not None else None,
                    publisher=manifest.publisher if manifest is not None else None,
                    release_channel=manifest.release_channel if manifest is not None else None,
                    trust_level=manifest.trust_level if manifest is not None else None,
                    permission_scopes=(
                        sorted(manifest.effective_permission_scopes())
                        if manifest is not None
                        else []
                    ),
                    source_sha256=manifest.source_sha256 if manifest is not None else None,
                    signing_key_id=manifest.signing_key_id if manifest is not None else None,
                    sandbox_profile=manifest.sandbox_profile if manifest is not None else None,
                    tenancy_mode=manifest.tenancy_mode if manifest is not None else None,
                    quarantined=manifest.quarantined if manifest is not None else False,
                    quarantine_reason=manifest.quarantine_reason if manifest is not None else None,
                    publisher_policy_decision=(
                        getattr(success, "publisher_policy_decision", None)
                        if success is not None
                        else None
                    ),
                    source=getattr(failure, "source", None),
                    warnings=warnings,
                    error=getattr(failure, "reason", None),
                    **signature_fields,
                )
            )
            continue

        responses.append(
            PluginCapabilityStatusResponse(
                name=plugin_name,
                capabilities=sorted({registration.kind.value for registration in registrations}),
                status="loaded",
                ready=ready,
                configured=configured,
                version=manifest.version if manifest is not None else None,
                api_version=manifest.api_version if manifest is not None else None,
                min_host_version=manifest.min_host_version if manifest is not None else None,
                max_host_version=manifest.max_host_version if manifest is not None else None,
                publisher=manifest.publisher if manifest is not None else None,
                release_channel=manifest.release_channel if manifest is not None else None,
                trust_level=manifest.trust_level if manifest is not None else None,
                permission_scopes=(
                    sorted(manifest.effective_permission_scopes())
                    if manifest is not None
                    else []
                ),
                source_sha256=manifest.source_sha256 if manifest is not None else None,
                signing_key_id=manifest.signing_key_id if manifest is not None else None,
                sandbox_profile=manifest.sandbox_profile if manifest is not None else None,
                tenancy_mode=manifest.tenancy_mode if manifest is not None else None,
                quarantined=manifest.quarantined if manifest is not None else False,
                quarantine_reason=manifest.quarantine_reason if manifest is not None else None,
                publisher_policy_decision=getattr(success, "publisher_policy_decision", None),
                source=(
                    manifest.distribution
                    if manifest is not None
                    else getattr(success, "source", None)
                ),
                warnings=warnings,
                **signature_fields,
            )
        )
    return responses


@router.get(
    "/plugins/events",
    operation_id="default.plugin_events",
    response_model=list[PluginEventStatusResponse],
)
async def get_plugin_events(request: Request) -> list[PluginEventStatusResponse]:
    """Return declared publishable events and hook subscriptions per plugin."""

    resources = request.app.state.resources
    plugin_registry = resources.plugin_registry
    if plugin_registry is None:
        return []

    publishable_by_plugin = plugin_registry.publishable_events_by_plugin()
    subscriptions_by_plugin = plugin_registry.hook_subscriptions_by_plugin()
    return [
        PluginEventStatusResponse(
            name=plugin_name,
            publisher=(
                plugin_registry.manifest(plugin_name).publisher
                if plugin_registry.manifest(plugin_name) is not None
                else None
            ),
            publishable_events=list(publishable_by_plugin.get(plugin_name, ())),
            hook_subscriptions=list(subscriptions_by_plugin.get(plugin_name, ())),
        )
        for plugin_name in sorted(plugin_registry.all_plugin_names())
    ]


@router.get(
    "/downloader_user_info",
    operation_id="default.download_user_info",
    response_model=dict[str, Any],
)
async def get_downloader_user_info(request: Request) -> dict[str, Any]:
    """Return normalized downloader-account info for dashboard compatibility."""

    resources = request.app.state.resources
    cached = await resources.cache.get("downloader:user_info")
    if isinstance(cached, bytes):
        return cast(dict[str, Any], json.loads(cached.decode("utf-8")))

    service = DownloaderAccountService(resources.settings.downloaders)
    result = await service.get_active_provider_info()
    await resources.cache.set(
        "downloader:user_info",
        json.dumps(result).encode("utf-8"),
        ttl_seconds=300,
    )
    return result


@router.post(
    "/generateapikey",
    operation_id="default.generate_apikey",
    response_model=ApiKeyRotationResponse,
    dependencies=[Depends(require_permissions("security:apikey.rotate"))],
)
async def generate_apikey(request: Request) -> ApiKeyRotationResponse:
    """Rotate the live backend API key and persist the new compatibility payload.

    Operators must still update the frontend/BFF environment before making further
    protected requests, because the current request is authenticated with the old key.
    """

    resources = request.app.state.resources
    auth_context = get_auth_context(request)
    new_key = _generate_api_key()
    new_key_id = _next_api_key_id(auth_context)
    resources.settings.api_key = SecretStr(new_key)
    resources.settings.api_key_id = new_key_id
    set_runtime_settings(resources.settings)
    await save_settings(resources.db, resources.settings.to_compatibility_dict())
    identity_service = resources.security_identity_service
    if identity_service is not None:
        await identity_service.rotate_service_account_api_key_id(
            auth_context=auth_context,
            new_api_key_id=new_key_id,
        )
    audit_action(
        request,
        action="security.generate_apikey",
        target="runtime.api_key",
        details={"rotated": True, "new_api_key_id": new_key_id},
    )
    return ApiKeyRotationResponse(
        key=new_key,
        api_key_id=new_key_id,
        warning=API_KEY_ROTATION_WARNING,
    )


@router.get("/stats", operation_id="default.stats", response_model=StatsResponse)
async def get_stats(request: Request) -> StatsResponse:
    """Return aggregated statistics for the current dashboard compatibility surface."""

    resources = request.app.state.resources
    auth_context = get_auth_context(request)
    snapshot = await resources.media_service.get_stats(tenant_id=auth_context.tenant_id)
    return StatsResponse(
        total_items=snapshot.total_items,
        total_movies=snapshot.movies,
        total_shows=snapshot.shows,
        total_seasons=snapshot.seasons,
        total_episodes=snapshot.episodes,
        total_symlinks=0,
        incomplete_items=snapshot.incomplete_items,
        states=snapshot.states,
        activity=snapshot.activity,
        media_year_releases=[
            StatsMediaYearRelease(year=item.year, count=item.count)
            for item in snapshot.media_year_releases
        ],
    )


@router.get("/calendar", operation_id="default.calendar", response_model=CalendarResponse)
async def get_calendar(
    request: Request,
    start_date: Annotated[str | None, Query()] = None,
    end_date: Annotated[str | None, Query()] = None,
) -> CalendarResponse:
    """Return calendar items for the current frontend calendar compatibility surface."""

    resources = request.app.state.resources
    auth_context = get_auth_context(request)
    snapshot = await resources.media_service.get_calendar_snapshot(
        start_date=start_date,
        end_date=end_date,
        tenant_id=auth_context.tenant_id,
    )
    return CalendarResponse(
        data={
            key: CalendarItemResponse(
                item_id=item.item_id,
                tvdb_id=item.tvdb_id,
                tmdb_id=item.tmdb_id,
                show_title=item.show_title,
                item_type=item.item_type,
                aired_at=item.aired_at,
                season=item.season,
                episode=item.episode,
                last_state=item.last_state,
                release_data=(
                    CalendarReleaseDataResponse(
                        next_aired=item.release_data.next_aired,
                        nextAired=item.release_data.nextAired,
                        last_aired=item.release_data.last_aired,
                        lastAired=item.release_data.lastAired,
                    )
                    if item.release_data is not None
                    else None
                ),
            )
            for key, item in snapshot.items()
        }
    )
