from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from filmu_py.services.vfs_rollout_control import (
    apply_vfs_rollout_control_updates,
    build_vfs_rollout_control_state,
    derive_vfs_rollout_allowed_actions,
    execute_vfs_rollout_action,
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


def test_vfs_rollout_action_promote_requires_gate_and_changes_environment() -> None:
    now = datetime(2026, 4, 18, 9, 0, tzinfo=UTC)
    raw_state = {
        "environment_class": "windows-native:managed",
        "promotion_paused": True,
        "promotion_pause_reason": "manual hold",
        "promotion_pause_expires_at": "2026-04-18T12:00:00Z",
    }

    allowed_actions = derive_vfs_rollout_allowed_actions(
        raw_state,
        canary_decision="promote_to_next_environment_class",
        merge_gate="ready",
        now=now,
    )

    assert allowed_actions == ("clear_hold", "promote", "hold", "rollback")

    payload = execute_vfs_rollout_action(
        raw_state,
        action="promote",
        actor_id="tenant-main:operator-1",
        canary_decision="promote_to_next_environment_class",
        merge_gate="ready",
        target_environment_class="windows-native:expanded",
        expected_canary_decision="promote_to_next_environment_class",
        expected_merge_gate="ready",
        now=now,
    )

    state = build_vfs_rollout_control_state(payload, now=now)
    assert state.environment_class == "windows-native:expanded"
    assert state.promotion_paused is False
    assert state.rollback_requested is False
    assert state.history[0].action == "execute_promote"
    assert state.history[0].summary == "promotion executed (windows-native:expanded)"


def test_vfs_rollout_action_rejects_non_rollback_when_runtime_is_blocked() -> None:
    now = datetime(2026, 4, 18, 9, 0, tzinfo=UTC)

    allowed_actions = derive_vfs_rollout_allowed_actions(
        {"environment_class": "windows-native:managed"},
        canary_decision="rollback_current_environment",
        merge_gate="blocked",
        now=now,
    )

    assert allowed_actions == ("rollback",)
    with pytest.raises(ValueError, match="rollout_action_not_allowed:hold"):
        execute_vfs_rollout_action(
            {"environment_class": "windows-native:managed"},
            action="hold",
            actor_id="tenant-main:operator-1",
            canary_decision="rollback_current_environment",
            merge_gate="blocked",
            reason="not allowed",
            now=now,
        )
