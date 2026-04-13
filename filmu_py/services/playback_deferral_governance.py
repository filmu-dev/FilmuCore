"""Shared deferral-governance counters for playback background refresh paths."""

from __future__ import annotations

from typing import Literal

PlaybackRefreshDeferralTrigger = Literal["failed_lease", "restricted_fallback"]
PlaybackRefreshDeferredReason = Literal["refresh_rate_limited", "provider_circuit_open"]

_SELECTED_HLS_REFRESH_DEFERRAL_GOVERNANCE: dict[str, int] = {
    "hls_failed_lease_refresh_rate_limited": 0,
    "hls_failed_lease_refresh_provider_circuit_open": 0,
    "hls_restricted_fallback_refresh_rate_limited": 0,
    "hls_restricted_fallback_refresh_provider_circuit_open": 0,
}
_DIRECT_PLAYBACK_REFRESH_DEFERRAL_GOVERNANCE: dict[str, int] = {
    "direct_playback_refresh_rate_limited": 0,
    "direct_playback_refresh_provider_circuit_open": 0,
}


def playback_refresh_deferral_governance_snapshot() -> dict[str, int]:
    """Return additive playback deferral-governance counters."""

    return {
        "direct_playback_refresh_rate_limited": _DIRECT_PLAYBACK_REFRESH_DEFERRAL_GOVERNANCE[
            "direct_playback_refresh_rate_limited"
        ],
        "direct_playback_refresh_provider_circuit_open": _DIRECT_PLAYBACK_REFRESH_DEFERRAL_GOVERNANCE[
            "direct_playback_refresh_provider_circuit_open"
        ],
        "hls_failed_lease_refresh_rate_limited": _SELECTED_HLS_REFRESH_DEFERRAL_GOVERNANCE[
            "hls_failed_lease_refresh_rate_limited"
        ],
        "hls_failed_lease_refresh_provider_circuit_open": _SELECTED_HLS_REFRESH_DEFERRAL_GOVERNANCE[
            "hls_failed_lease_refresh_provider_circuit_open"
        ],
        "hls_restricted_fallback_refresh_rate_limited": _SELECTED_HLS_REFRESH_DEFERRAL_GOVERNANCE[
            "hls_restricted_fallback_refresh_rate_limited"
        ],
        "hls_restricted_fallback_refresh_provider_circuit_open": _SELECTED_HLS_REFRESH_DEFERRAL_GOVERNANCE[
            "hls_restricted_fallback_refresh_provider_circuit_open"
        ],
    }


def record_selected_hls_refresh_deferral(
    *,
    trigger: PlaybackRefreshDeferralTrigger,
    reason: PlaybackRefreshDeferredReason,
) -> None:
    """Record one selected-HLS background refresh deferral for status visibility."""

    key_reason = "rate_limited" if reason == "refresh_rate_limited" else reason
    key = f"hls_{trigger}_refresh_{key_reason}"
    _SELECTED_HLS_REFRESH_DEFERRAL_GOVERNANCE[key] += 1


def record_direct_playback_refresh_deferral(
    *,
    reason: PlaybackRefreshDeferredReason,
) -> None:
    """Record one direct-play background refresh deferral for status visibility."""

    key_reason = "rate_limited" if reason == "refresh_rate_limited" else reason
    key = f"direct_playback_refresh_{key_reason}"
    _DIRECT_PLAYBACK_REFRESH_DEFERRAL_GOVERNANCE[key] += 1
