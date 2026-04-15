"""Host stream-control gateway for controlled plugin operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from filmu_py.core.byte_streaming import (
    get_active_handle_snapshot,
    get_active_session_snapshot,
    get_serving_governance_snapshot,
)
from filmu_py.plugins.interfaces import StreamControlAction, StreamControlInput, StreamControlResult
from filmu_py.resources import AppResources
from filmu_py.services.playback import (
    trigger_direct_playback_refresh_from_resources,
    trigger_hls_failed_lease_refresh_from_resources,
    trigger_hls_restricted_fallback_refresh_from_resources,
)


def _normalize_control_plane_outcome(
    app_result: object,
) -> tuple[str, float | None, dict[str, str | bool]]:
    """Return `(outcome, retry_after, metadata)` from one trigger result payload."""

    app_outcome = str(getattr(app_result, "outcome", "unknown"))
    control_plane_result = getattr(app_result, "control_plane_result", None)
    control_plane_outcome = (
        str(getattr(control_plane_result, "outcome", "unknown"))
        if control_plane_result is not None
        else "unavailable"
    )
    refresh_result = (
        getattr(control_plane_result, "scheduling_result", None)
        if control_plane_result is not None
        else None
    )
    if refresh_result is None and control_plane_result is not None:
        refresh_result = getattr(control_plane_result, "refresh_result", None)
    refresh_outcome = (
        str(getattr(refresh_result, "outcome", "unknown"))
        if refresh_result is not None
        else "unavailable"
    )
    retry_after_seconds = (
        cast(Any, refresh_result).retry_after_seconds
        if refresh_result is not None and hasattr(refresh_result, "retry_after_seconds")
        else None
    )
    retry_after = float(retry_after_seconds) if retry_after_seconds is not None else None
    metadata: dict[str, str | bool] = {
        "app_outcome": app_outcome,
        "control_plane_outcome": control_plane_outcome,
        "refresh_outcome": refresh_outcome,
        "controller_attached": bool(getattr(app_result, "controller_attached", False)),
    }
    return refresh_outcome, retry_after, metadata


@dataclass(slots=True)
class HostStreamControlGateway:
    """Controlled gateway that maps plugin actions to serving/playback controls."""

    resources: AppResources

    async def control(self, request: StreamControlInput) -> StreamControlResult:
        """Execute one stream-control action against host-managed controls."""

        if request.action is StreamControlAction.SERVING_STATUS_SNAPSHOT:
            serving_governance = get_serving_governance_snapshot()
            return StreamControlResult(
                action=request.action,
                item_identifier=request.item_identifier,
                accepted=True,
                outcome="snapshot",
                metadata={
                    "active_sessions": len(get_active_session_snapshot()),
                    "active_handles": len(get_active_handle_snapshot()),
                    "serving_governance_keys": ",".join(sorted(serving_governance)),
                },
            )

        item_identifier = request.item_identifier
        if item_identifier is None or not item_identifier.strip():
            return StreamControlResult(
                action=request.action,
                item_identifier=item_identifier,
                accepted=False,
                outcome="invalid_request",
                detail="item_identifier is required for this action",
            )

        if request.action is StreamControlAction.MARK_SELECTED_HLS_MEDIA_ENTRY_STALE:
            playback_service = self.resources.playback_service
            if playback_service is None:
                return StreamControlResult(
                    action=request.action,
                    item_identifier=item_identifier,
                    accepted=False,
                    outcome="controller_unavailable",
                    detail="playback_service_unavailable",
                    controller_attached=False,
                )
            marked = await playback_service.mark_selected_hls_media_entry_stale(item_identifier)
            return StreamControlResult(
                action=request.action,
                item_identifier=item_identifier,
                accepted=marked,
                outcome="marked" if marked else "no_action",
                controller_attached=True,
            )

        app_result: object
        if request.action is StreamControlAction.TRIGGER_DIRECT_PLAYBACK_REFRESH:
            app_result = await trigger_direct_playback_refresh_from_resources(
                self.resources,
                item_identifier,
                prefer_queued=request.prefer_queued,
            )
        elif request.action is StreamControlAction.TRIGGER_HLS_FAILED_LEASE_REFRESH:
            app_result = await trigger_hls_failed_lease_refresh_from_resources(
                self.resources,
                item_identifier,
                prefer_queued=request.prefer_queued,
            )
        else:
            app_result = await trigger_hls_restricted_fallback_refresh_from_resources(
                self.resources,
                item_identifier,
                prefer_queued=request.prefer_queued,
            )

        refresh_outcome, retry_after, metadata = _normalize_control_plane_outcome(app_result)
        controller_attached = bool(getattr(app_result, "controller_attached", False))
        return StreamControlResult(
            action=request.action,
            item_identifier=item_identifier,
            accepted=bool(getattr(app_result, "outcome", "") == "triggered"),
            outcome=refresh_outcome,
            controller_attached=controller_attached,
            retry_after_seconds=retry_after,
            metadata=metadata,
        )
