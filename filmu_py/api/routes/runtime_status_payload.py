"""Shared `/stream/status` payload assembly for route decomposition."""

from __future__ import annotations

from fastapi import Request

from filmu_py.api.deps import get_auth_context
from filmu_py.api.models import (
    ServingGovernanceResponse,
    ServingHandleResponse,
    ServingPathResponse,
    ServingSessionResponse,
    ServingStatusResponse,
)
from filmu_py.core import byte_streaming
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.resources import AppResources
from filmu_py.services.playback import PlaybackSourceService
from filmu_py.services.vfs_server import build_empty_vfs_catalog_governance_snapshot

from .runtime_governance import (
    playback_gate_governance_snapshot,
    vfs_runtime_governance_snapshot,
)
from .runtime_hls_governance import (
    hls_route_failure_governance_snapshot,
    remote_hls_recovery_governance_snapshot,
)
from .runtime_refresh_governance import (
    direct_playback_trigger_governance_snapshot,
    hls_failed_lease_trigger_governance_snapshot,
    hls_restricted_fallback_trigger_governance_snapshot,
    stream_refresh_policy_governance_snapshot,
)


async def build_serving_status_response(
    *,
    request: Request,
    db: DatabaseRuntime,
    resources: AppResources,
    direct_playback_active_tasks: int,
    hls_failed_lease_active_tasks: int,
    hls_restricted_fallback_active_tasks: int,
    stream_refresh_latency_slo_ms: int,
) -> ServingStatusResponse:
    """Build the full `ServingStatusResponse` payload from runtime surfaces."""

    byte_streaming.cleanup_expired_serving_runtime()
    playback_governance = await PlaybackSourceService(db).build_playback_governance_snapshot()
    vfs_governance = (
        resources.vfs_catalog_server.build_governance_snapshot()
        if resources.vfs_catalog_server is not None
        else build_empty_vfs_catalog_governance_snapshot()
    )
    playback_gate_governance = playback_gate_governance_snapshot()
    auth_context = get_auth_context(request)
    vfs_runtime_governance = vfs_runtime_governance_snapshot(
        playback_gate_governance=playback_gate_governance,
        request_tenant_id=auth_context.tenant_id,
        authorized_tenant_ids=set(auth_context.authorized_tenant_ids),
    )
    sessions = [
        ServingSessionResponse(
            session_id=session.session_id,
            category=session.category,
            resource=session.resource,
            started_at=session.started_at.isoformat(),
            last_activity_at=session.last_activity_at.isoformat(),
            bytes_served=session.bytes_served,
        )
        for session in byte_streaming.get_active_session_snapshot()
    ]
    handles = [
        ServingHandleResponse(
            handle_id=handle.handle_id,
            session_id=handle.session_id,
            category=handle.category,
            path=handle.path,
            path_id=handle.path_id,
            created_at=handle.created_at.isoformat(),
            last_activity_at=handle.last_activity_at.isoformat(),
            bytes_served=handle.bytes_served,
            read_offset=handle.read_offset,
        )
        for handle in byte_streaming.get_active_handle_snapshot()
    ]
    paths = [
        ServingPathResponse(
            path_id=path.path_id,
            category=path.category,
            path=path.path,
            created_at=path.created_at.isoformat(),
            last_activity_at=path.last_activity_at.isoformat(),
            size_bytes=path.size_bytes,
            active_handle_count=path.active_handle_count,
        )
        for path in byte_streaming.get_active_path_snapshot()
    ]
    queued_refresh_controllers_attached = int(
        resources.queued_direct_playback_refresh_controller is not None
        and resources.queued_hls_failed_lease_refresh_controller is not None
        and resources.queued_hls_restricted_fallback_refresh_controller is not None
    )
    heavy_stage_policy = resources.settings.orchestration.heavy_stage_isolation
    heavy_stage_policy_violations = heavy_stage_policy.policy_violations()
    heavy_stage_exit_ready = int(
        heavy_stage_policy.exit_ready(
            arq_enabled=resources.settings.arq_enabled,
            refresh_dispatch_mode=resources.settings.stream.refresh_dispatch_mode,
            queued_refresh_ready=bool(
                resources.settings.stream.refresh_dispatch_mode != "queued"
                or (resources.arq_redis is not None and queued_refresh_controllers_attached)
            ),
            queued_refresh_proof_refs=resources.settings.orchestration.queued_refresh_proof_refs,
        )
    )
    return ServingStatusResponse(
        sessions=sessions,
        handles=handles,
        paths=paths,
        governance=ServingGovernanceResponse.model_validate(
            {
                **byte_streaming.get_serving_governance_snapshot(),
                **hls_route_failure_governance_snapshot(),
                **remote_hls_recovery_governance_snapshot(),
                **direct_playback_trigger_governance_snapshot(
                    active_tasks=direct_playback_active_tasks
                ),
                **hls_failed_lease_trigger_governance_snapshot(
                    active_tasks=hls_failed_lease_active_tasks
                ),
                **hls_restricted_fallback_trigger_governance_snapshot(
                    active_tasks=hls_restricted_fallback_active_tasks
                ),
                **stream_refresh_policy_governance_snapshot(
                    stream_refresh_latency_slo_ms=stream_refresh_latency_slo_ms
                ),
                **playback_governance,
                **vfs_governance,
                **vfs_runtime_governance,
                **playback_gate_governance,
                "stream_refresh_dispatch_mode": resources.settings.stream.refresh_dispatch_mode,
                "stream_refresh_queue_enabled": int(
                    resources.settings.stream.refresh_dispatch_mode == "queued"
                ),
                "stream_refresh_queue_ready": int(
                    resources.settings.stream.refresh_dispatch_mode != "queued"
                    or (resources.arq_redis is not None and queued_refresh_controllers_attached)
                ),
                "stream_refresh_proof_ref_count": len(
                    resources.settings.orchestration.queued_refresh_proof_refs
                ),
                "heavy_stage_executor_mode": heavy_stage_policy.executor_mode,
                "heavy_stage_max_workers": heavy_stage_policy.max_workers,
                "heavy_stage_max_tasks_per_child": heavy_stage_policy.max_tasks_per_child,
                "heavy_stage_spawn_context_required": int(heavy_stage_policy.require_spawn_context),
                "heavy_stage_max_worker_ceiling": heavy_stage_policy.max_worker_ceiling,
                "heavy_stage_policy_violation_count": len(heavy_stage_policy_violations),
                "heavy_stage_policy_violations": list(heavy_stage_policy_violations),
                "heavy_stage_process_isolation_required": int(
                    heavy_stage_policy.process_isolation_required()
                ),
                "heavy_stage_exit_ready": heavy_stage_exit_ready,
                "heavy_stage_index_timeout_seconds": heavy_stage_policy.index_timeout_seconds,
                "heavy_stage_parse_timeout_seconds": heavy_stage_policy.parse_timeout_seconds,
                "heavy_stage_rank_timeout_seconds": heavy_stage_policy.rank_timeout_seconds,
                "heavy_stage_proof_ref_count": len(heavy_stage_policy.proof_refs),
                "serving_active_session_summaries": [
                    f"{session.session_id}:{session.category}:{session.resource}"
                    for session in sessions[:10]
                ],
                "vfs_runtime_active_handle_summaries": vfs_runtime_governance.get(
                    "vfs_runtime_active_handle_summaries", []
                ),
            }
        ),
    )
