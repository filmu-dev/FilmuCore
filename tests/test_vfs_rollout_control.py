from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from filmu_py.services.vfs_rollout_control import (
    apply_vfs_rollout_control_updates,
    build_vfs_rollout_control_state,
)


def test_vfs_rollout_control_defaults_pause_window_and_records_history() -> None:
    now = datetime(2026, 4, 17, 18, 0, tzinfo=UTC)

    payload = apply_vfs_rollout_control_updates(
        {},
        {
            "environment_class": "windows-native:managed",
            "promotion_paused": True,
            "promotion_pause_reason": "repeat soak before next step",
        },
        actor_id="tenant-main:operator-1",
        now=now,
    )
    state = build_vfs_rollout_control_state(payload, now=now)

    assert state.environment_class == "windows-native:managed"
    assert state.promotion_paused is True
    assert state.promotion_pause_reason == "repeat soak before next step"
    assert state.promotion_pause_expires_at == now + timedelta(hours=4)
    assert state.promotion_pause_active is True
    assert state.updated_by == "tenant-main:operator-1"
    assert len(state.history) == 1
    assert state.history[0].summary.startswith("promotion pause enabled")


def test_vfs_rollout_control_requires_explicit_rollback_reason() -> None:
    now = datetime(2026, 4, 17, 18, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="rollback_reason_required"):
        apply_vfs_rollout_control_updates(
            {},
            {"rollback_requested": True},
            actor_id="tenant-main:operator-1",
            now=now,
        )


def test_vfs_rollout_control_marks_expired_overrides_inactive_and_bounds_history() -> None:
    now = datetime(2026, 4, 17, 18, 0, tzinfo=UTC)
    payload = {
        "environment_class": "windows-native:managed",
        "promotion_paused": True,
        "promotion_pause_reason": "stale pause",
        "promotion_pause_expires_at": "2026-04-17T17:00:00Z",
        "history": [
            {
                "entry_id": f"entry-{index}",
                "recorded_at": "2026-04-17T16:00:00Z",
                "action": "write_control",
                "summary": f"entry {index}",
                "environment_class": "windows-native:managed",
                "promotion_paused": False,
                "rollback_requested": False,
                "promotion_pause_active": False,
                "rollback_active": False,
            }
            for index in range(25)
        ],
    }

    state = build_vfs_rollout_control_state(payload, now=now)

    assert state.promotion_pause_active is False
    assert len(state.history) == 20
    assert state.history[0].entry_id == "entry-0"
    assert state.history[-1].entry_id == "entry-19"
