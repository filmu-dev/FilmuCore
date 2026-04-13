"""Shared remote-HLS retry/cooldown and failure-governance helpers for stream routes."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from threading import Lock
from time import monotonic
from typing import Any

from fastapi import HTTPException, status
from prometheus_client import Counter

HLS_ROUTE_FAILURE_EVENTS = Counter(
    "filmu_py_stream_hls_route_failures_total",
    "Count of normalized HLS route failures by classified reason.",
    labelnames=("reason",),
)
REMOTE_HLS_RECOVERY_EVENTS = Counter(
    "filmu_py_stream_remote_hls_recovery_total",
    "Count of remote-HLS retry/cooldown recovery events by kind.",
    labelnames=("event",),
)
INLINE_REMOTE_HLS_REFRESH_EVENTS = Counter(
    "filmu_py_stream_inline_remote_hls_refresh_total",
    "Count of inline remote-HLS media-entry repair attempts by outcome.",
    labelnames=("event",),
)

_HLS_ROUTE_FAILURE_GOVERNANCE = {
    "generation_failed": 0,
    "generation_timeout": 0,
    "generation_capacity_exceeded": 0,
    "generator_unavailable": 0,
    "lease_failed": 0,
    "transcode_source_unavailable": 0,
    "manifest_invalid": 0,
    "generated_missing": 0,
    "upstream_failed": 0,
    "upstream_manifest_invalid": 0,
}
_REMOTE_HLS_RETRY_GOVERNANCE = {
    "retry_attempts": 0,
    "cooldown_starts": 0,
    "cooldown_hits": 0,
}
_INLINE_REMOTE_HLS_REFRESH_GOVERNANCE = {
    "attempts": 0,
    "recovered": 0,
    "no_action": 0,
    "failures": 0,
}

_REMOTE_HLS_COOLDOWNS: dict[str, tuple[float, int, str]] = {}
_REMOTE_HLS_COOLDOWN_LOCK = Lock()
_REMOTE_HLS_RETRY_ATTEMPTS = 2
_REMOTE_HLS_COOLDOWN_SECONDS = 15.0


def _cleanup_remote_hls_cooldowns(*, now: float | None = None) -> None:
    """Drop expired remote-HLS cooldown entries."""

    current_time = monotonic() if now is None else now
    expired_keys = [
        key
        for key, (expires_at, _, _) in _REMOTE_HLS_COOLDOWNS.items()
        if expires_at <= current_time
    ]
    for key in expired_keys:
        _REMOTE_HLS_COOLDOWNS.pop(key, None)


def record_hls_route_failure(*, reason: str) -> None:
    """Record one normalized HLS route failure by classified reason."""

    HLS_ROUTE_FAILURE_EVENTS.labels(reason=reason).inc()
    _HLS_ROUTE_FAILURE_GOVERNANCE[reason] += 1


def classify_hls_route_failure_reason(exc: HTTPException) -> str:
    """Classify one normalized HLS route failure into a small reason taxonomy."""

    detail = exc.detail if isinstance(exc.detail, str) else ""
    if detail.startswith("HLS transcode source is unavailable"):
        return "transcode_source_unavailable"
    if detail.startswith("HLS generation capacity exceeded"):
        return "generation_capacity_exceeded"
    if detail.startswith("Generated HLS playlist is"):
        return "manifest_invalid"
    if detail.startswith("Upstream HLS playlist is"):
        return "upstream_manifest_invalid"
    if exc.status_code == status.HTTP_504_GATEWAY_TIMEOUT:
        return "generation_timeout"
    if exc.status_code == status.HTTP_501_NOT_IMPLEMENTED:
        return "generator_unavailable"
    if exc.status_code == status.HTTP_503_SERVICE_UNAVAILABLE:
        return "lease_failed"
    return "generation_failed"


def hls_route_failure_governance_snapshot() -> dict[str, int]:
    """Return additive governance counters for normalized HLS route failures."""

    return {
        "hls_route_failures_total": sum(_HLS_ROUTE_FAILURE_GOVERNANCE.values()),
        "hls_route_failures_generation_failed": _HLS_ROUTE_FAILURE_GOVERNANCE["generation_failed"],
        "hls_route_failures_generation_timeout": _HLS_ROUTE_FAILURE_GOVERNANCE[
            "generation_timeout"
        ],
        "hls_route_failures_generation_capacity_exceeded": _HLS_ROUTE_FAILURE_GOVERNANCE[
            "generation_capacity_exceeded"
        ],
        "hls_route_failures_generator_unavailable": _HLS_ROUTE_FAILURE_GOVERNANCE[
            "generator_unavailable"
        ],
        "hls_route_failures_lease_failed": _HLS_ROUTE_FAILURE_GOVERNANCE["lease_failed"],
        "hls_route_failures_transcode_source_unavailable": _HLS_ROUTE_FAILURE_GOVERNANCE[
            "transcode_source_unavailable"
        ],
        "hls_route_failures_manifest_invalid": _HLS_ROUTE_FAILURE_GOVERNANCE["manifest_invalid"],
        "hls_route_failures_generated_missing": _HLS_ROUTE_FAILURE_GOVERNANCE["generated_missing"],
        "hls_route_failures_upstream_failed": _HLS_ROUTE_FAILURE_GOVERNANCE["upstream_failed"],
        "hls_route_failures_upstream_manifest_invalid": _HLS_ROUTE_FAILURE_GOVERNANCE[
            "upstream_manifest_invalid"
        ],
    }


def remote_hls_recovery_governance_snapshot() -> dict[str, int]:
    """Return additive governance counters for remote-HLS retry/cooldown behavior."""

    with _REMOTE_HLS_COOLDOWN_LOCK:
        _cleanup_remote_hls_cooldowns()
        active_cooldowns = len(_REMOTE_HLS_COOLDOWNS)
    return {
        "remote_hls_retry_attempts": _REMOTE_HLS_RETRY_GOVERNANCE["retry_attempts"],
        "remote_hls_cooldown_starts": _REMOTE_HLS_RETRY_GOVERNANCE["cooldown_starts"],
        "remote_hls_cooldown_hits": _REMOTE_HLS_RETRY_GOVERNANCE["cooldown_hits"],
        "remote_hls_cooldowns_active": active_cooldowns,
        "inline_remote_hls_refresh_attempts": _INLINE_REMOTE_HLS_REFRESH_GOVERNANCE["attempts"],
        "inline_remote_hls_refresh_recovered": _INLINE_REMOTE_HLS_REFRESH_GOVERNANCE[
            "recovered"
        ],
        "inline_remote_hls_refresh_no_action": _INLINE_REMOTE_HLS_REFRESH_GOVERNANCE[
            "no_action"
        ],
        "inline_remote_hls_refresh_failures": _INLINE_REMOTE_HLS_REFRESH_GOVERNANCE[
            "failures"
        ],
    }


def record_inline_remote_hls_refresh(*, event: str) -> None:
    """Record one inline remote-HLS media-entry repair event."""

    INLINE_REMOTE_HLS_REFRESH_EVENTS.labels(event=event).inc()
    _INLINE_REMOTE_HLS_REFRESH_GOVERNANCE[event] += 1


def is_retryable_remote_hls_error(exc: HTTPException) -> bool:
    """Return whether one remote-HLS HTTP exception is safe to retry briefly."""

    if exc.status_code not in {status.HTTP_502_BAD_GATEWAY, status.HTTP_504_GATEWAY_TIMEOUT}:
        return False
    detail = exc.detail if isinstance(exc.detail, str) else ""
    return detail in {
        "Upstream HLS request timed out",
        "Upstream HLS request transport failed",
        "Upstream playback request timed out",
        "Upstream playback request transport failed",
    }


def raise_remote_hls_cooldown_if_active(*, cooldown_key: str) -> None:
    """Fail fast when one remote-HLS upstream is in a short cooldown window."""

    current_time = monotonic()
    with _REMOTE_HLS_COOLDOWN_LOCK:
        _cleanup_remote_hls_cooldowns(now=current_time)
        cooldown = _REMOTE_HLS_COOLDOWNS.get(cooldown_key)
        if cooldown is None:
            return
        expires_at, status_code, detail = cooldown
        _REMOTE_HLS_RETRY_GOVERNANCE["cooldown_hits"] += 1
        REMOTE_HLS_RECOVERY_EVENTS.labels(event="cooldown_hit").inc()
        retry_after = max(1, int(expires_at - current_time + 0.999))
    raise HTTPException(
        status_code=status_code,
        detail=detail,
        headers={"Retry-After": str(retry_after)},
    )


def start_remote_hls_cooldown(*, cooldown_key: str, exc: HTTPException) -> None:
    """Start a short cooldown for one remote-HLS upstream after repeated transient failure."""

    detail = exc.detail if isinstance(exc.detail, str) else "Upstream HLS request transport failed"
    with _REMOTE_HLS_COOLDOWN_LOCK:
        _cleanup_remote_hls_cooldowns()
        _REMOTE_HLS_COOLDOWNS[cooldown_key] = (
            monotonic() + _REMOTE_HLS_COOLDOWN_SECONDS,
            exc.status_code,
            detail,
        )
        _REMOTE_HLS_RETRY_GOVERNANCE["cooldown_starts"] += 1
    REMOTE_HLS_RECOVERY_EVENTS.labels(event="cooldown_start").inc()


async def run_remote_hls_with_retry(
    *,
    cooldown_key: str,
    operation: Callable[[], Awaitable[Any]],
) -> Any:
    """Run one remote-HLS operation with a single transient retry and short cooldown."""

    raise_remote_hls_cooldown_if_active(cooldown_key=cooldown_key)
    for attempt in range(_REMOTE_HLS_RETRY_ATTEMPTS):
        try:
            return await operation()
        except HTTPException as exc:
            if not is_retryable_remote_hls_error(exc):
                raise
            if attempt + 1 >= _REMOTE_HLS_RETRY_ATTEMPTS:
                start_remote_hls_cooldown(cooldown_key=cooldown_key, exc=exc)
                raise
            _REMOTE_HLS_RETRY_GOVERNANCE["retry_attempts"] += 1
            REMOTE_HLS_RECOVERY_EVENTS.labels(event="retry_attempt").inc()
            await asyncio.sleep(0)

    raise AssertionError("remote HLS retry loop exhausted unexpectedly")


def validate_upstream_hls_playlist(playlist_text: str) -> None:
    """Validate one upstream HLS playlist before rewriting its child references."""

    lines = playlist_text.splitlines()
    if not lines:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream HLS playlist is empty",
        )

    first_non_empty = next((line.strip() for line in lines if line.strip()), "")
    if first_non_empty != "#EXTM3U":
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream HLS playlist is malformed",
        )

    has_reference = any(line.strip() and not line.strip().startswith("#") for line in lines)
    if not has_reference:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Upstream HLS playlist has no child references",
        )
