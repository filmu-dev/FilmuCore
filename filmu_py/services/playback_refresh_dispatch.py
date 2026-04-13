"""Refresh-dispatch policy helpers shared by playback trigger wrappers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from filmu_py.resources import AppResources


def resolve_refresh_controller(
    resources: AppResources,
    *,
    prefer_queued: bool | None,
    in_process_attr: str,
    queued_attr: str,
) -> Any | None:
    """Resolve the refresh controller using route preference + policy fallback."""

    default_use_queued = resources.settings.stream.refresh_dispatch_mode == "queued"
    use_queued = default_use_queued if prefer_queued is None else prefer_queued
    primary_controller = getattr(
        resources,
        queued_attr if use_queued else in_process_attr,
        None,
    )
    if primary_controller is not None:
        return primary_controller
    in_process_controller = getattr(resources, in_process_attr, None)
    if in_process_controller is not None:
        return in_process_controller
    return getattr(resources, queued_attr, None)
