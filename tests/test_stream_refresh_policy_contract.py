"""Stream refresh dispatch policy contract tests."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from filmu_py.api.routes import stream as stream_routes


def _reset_policy_counters() -> None:
    for key in stream_routes._STREAM_REFRESH_POLICY_GOVERNANCE:
        stream_routes._STREAM_REFRESH_POLICY_GOVERNANCE[key] = 0


def test_runtime_pressure_requires_queued_dispatch_when_latency_slo_breaches() -> None:
    requires_queue, slo_breached = stream_routes._runtime_pressure_requires_queued_dispatch(
        {
            "vfs_runtime_refresh_pressure_class": "healthy",
            "vfs_runtime_upstream_wait_class": "healthy",
            "vfs_runtime_chunk_coalescing_pressure_class": "healthy",
            "vfs_runtime_provider_pressure_incidents": 0,
            "vfs_runtime_upstream_fetch_average_duration_ms": 301,
            "vfs_runtime_upstream_fetch_max_duration_ms": 510,
        }
    )
    assert requires_queue is True
    assert slo_breached is True


def test_select_refresh_dispatch_preference_forces_queued_under_pressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_policy_counters()
    resources = SimpleNamespace(
        settings=SimpleNamespace(stream=SimpleNamespace(refresh_dispatch_mode="in_process"))
    )
    monkeypatch.setattr(
        stream_routes,
        "_vfs_runtime_governance_snapshot",
        lambda: {
            "vfs_runtime_refresh_pressure_class": "critical",
            "vfs_runtime_upstream_wait_class": "warning",
            "vfs_runtime_chunk_coalescing_pressure_class": "healthy",
            "vfs_runtime_provider_pressure_incidents": 3,
            "vfs_runtime_upstream_fetch_average_duration_ms": 120,
            "vfs_runtime_upstream_fetch_max_duration_ms": 280,
        },
    )

    decision = stream_routes._select_refresh_dispatch_preference(
        resources=cast(Any, resources),
        queued_controller_available=True,
    )

    assert decision is True
    snapshot = stream_routes._stream_refresh_policy_governance_snapshot()
    assert snapshot["stream_refresh_policy_forced_queued"] == 1
    assert snapshot["stream_refresh_policy_forced_in_process"] == 0
    assert snapshot["stream_refresh_policy_fallback_in_process"] == 0


def test_select_refresh_dispatch_preference_falls_back_to_in_process_when_queued_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_policy_counters()
    resources = SimpleNamespace(
        settings=SimpleNamespace(stream=SimpleNamespace(refresh_dispatch_mode="queued"))
    )
    monkeypatch.setattr(
        stream_routes,
        "_vfs_runtime_governance_snapshot",
        lambda: {
            "vfs_runtime_refresh_pressure_class": "critical",
            "vfs_runtime_upstream_wait_class": "critical",
            "vfs_runtime_chunk_coalescing_pressure_class": "warning",
            "vfs_runtime_provider_pressure_incidents": 12,
            "vfs_runtime_upstream_fetch_average_duration_ms": 200,
            "vfs_runtime_upstream_fetch_max_duration_ms": 600,
        },
    )

    decision = stream_routes._select_refresh_dispatch_preference(
        resources=cast(Any, resources),
        queued_controller_available=False,
    )

    assert decision is False
    snapshot = stream_routes._stream_refresh_policy_governance_snapshot()
    assert snapshot["stream_refresh_policy_fallback_in_process"] == 1
