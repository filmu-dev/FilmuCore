"""Shared persisted VFS rollout-control state and bounded operator history."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

_SCHEMA_VERSION = 2
_DEFAULT_OVERRIDE_WINDOW_HOURS = 4
_MAX_HISTORY_ENTRIES = 20


@dataclass(frozen=True, slots=True)
class VfsRolloutLedgerEntry:
    """One retained operator change recorded for VFS promotion control."""

    entry_id: str
    recorded_at: datetime
    actor_id: str | None
    action: str
    summary: str
    environment_class: str
    runtime_status_path: str | None
    promotion_paused: bool
    promotion_pause_reason: str | None
    promotion_pause_expires_at: datetime | None
    promotion_pause_active: bool
    rollback_requested: bool
    rollback_reason: str | None
    rollback_expires_at: datetime | None
    rollback_active: bool
    notes: str | None


@dataclass(frozen=True, slots=True)
class VfsRolloutControlState:
    """Normalized persisted VFS promotion-control state."""

    schema_version: int
    environment_class: str
    runtime_status_path: str | None
    promotion_paused: bool
    promotion_pause_reason: str | None
    promotion_pause_expires_at: datetime | None
    promotion_pause_active: bool
    rollback_requested: bool
    rollback_reason: str | None
    rollback_expires_at: datetime | None
    rollback_active: bool
    notes: str | None
    updated_at: datetime | None
    updated_by: str | None
    history: tuple[VfsRolloutLedgerEntry, ...]


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def format_rollout_timestamp(value: datetime | None) -> str | None:
    """Return a stable UTC timestamp for persisted rollout-control payloads."""

    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _clean_text(value: object, *, allow_empty: bool = False) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if cleaned or allow_empty:
        return cleaned
    return None


def _clean_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if isinstance(value, int):
        return value != 0
    return False


def _override_active(requested: bool, expires_at: datetime | None, *, now: datetime) -> bool:
    if not requested:
        return False
    if expires_at is None:
        return True
    return expires_at > now


def build_vfs_rollout_control_state(
    raw_state: Mapping[str, object] | None,
    *,
    now: datetime | None = None,
) -> VfsRolloutControlState:
    """Return normalized persisted VFS rollout-control state."""

    current_time = datetime.now(UTC) if now is None else now.astimezone(UTC)
    payload = raw_state or {}
    promotion_pause_expires_at = _parse_timestamp(payload.get("promotion_pause_expires_at"))
    rollback_expires_at = _parse_timestamp(payload.get("rollback_expires_at"))
    raw_history = payload.get("history")
    history_entries = raw_history if isinstance(raw_history, list) else []
    raw_schema_version = payload.get("schema_version", _SCHEMA_VERSION)
    if isinstance(raw_schema_version, int):
        schema_version = raw_schema_version
    elif isinstance(raw_schema_version, str):
        try:
            schema_version = int(raw_schema_version)
        except ValueError:
            schema_version = _SCHEMA_VERSION
    else:
        schema_version = _SCHEMA_VERSION
    history = tuple(
        _build_history_entry(entry, now=current_time)
        for entry in history_entries
        if isinstance(entry, Mapping)
    )[:_MAX_HISTORY_ENTRIES]
    promotion_paused = _clean_bool(payload.get("promotion_paused"))
    rollback_requested = _clean_bool(payload.get("rollback_requested"))
    return VfsRolloutControlState(
        schema_version=max(1, schema_version),
        environment_class=_clean_text(payload.get("environment_class"), allow_empty=True) or "",
        runtime_status_path=_clean_text(payload.get("runtime_status_path")),
        promotion_paused=promotion_paused,
        promotion_pause_reason=_clean_text(payload.get("promotion_pause_reason")),
        promotion_pause_expires_at=promotion_pause_expires_at,
        promotion_pause_active=_override_active(
            promotion_paused,
            promotion_pause_expires_at,
            now=current_time,
        ),
        rollback_requested=rollback_requested,
        rollback_reason=_clean_text(payload.get("rollback_reason")),
        rollback_expires_at=rollback_expires_at,
        rollback_active=_override_active(
            rollback_requested,
            rollback_expires_at,
            now=current_time,
        ),
        notes=_clean_text(payload.get("notes")),
        updated_at=_parse_timestamp(payload.get("updated_at")),
        updated_by=_clean_text(payload.get("updated_by")),
        history=history,
    )


def apply_vfs_rollout_control_updates(
    raw_state: Mapping[str, object] | None,
    updates: Mapping[str, object | None],
    *,
    actor_id: str | None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Apply one operator update to the persisted VFS rollout-control state."""

    current_time = datetime.now(UTC) if now is None else now.astimezone(UTC)
    current = build_vfs_rollout_control_state(raw_state, now=current_time)
    provided = set(updates)
    next_environment_class = _pick_text(
        current.environment_class,
        updates.get("environment_class"),
        allow_empty=True,
    )
    next_runtime_status_path = _pick_text(current.runtime_status_path, updates.get("runtime_status_path"))
    next_notes = _pick_text(current.notes, updates.get("notes"))
    next_promotion_paused = _pick_bool(
        current.promotion_paused,
        updates.get("promotion_paused"),
        field_name="promotion_paused",
        provided=provided,
    )
    next_rollback_requested = _pick_bool(
        current.rollback_requested,
        updates.get("rollback_requested"),
        field_name="rollback_requested",
        provided=provided,
    )

    next_promotion_pause_reason, next_promotion_pause_expires_at = _resolve_override_payload(
        requested=next_promotion_paused,
        current_reason=current.promotion_pause_reason,
        current_expires_at=current.promotion_pause_expires_at,
        explicit_reason=updates.get("promotion_pause_reason"),
        explicit_expires_at=updates.get("promotion_pause_expires_at"),
        reason_required=False,
        default_reason="repeat_windows_soak_before_next_promotion_step",
        now=current_time,
    )
    next_rollback_reason, next_rollback_expires_at = _resolve_override_payload(
        requested=next_rollback_requested,
        current_reason=current.rollback_reason,
        current_expires_at=current.rollback_expires_at,
        explicit_reason=updates.get("rollback_reason"),
        explicit_expires_at=updates.get("rollback_expires_at"),
        reason_required=True,
        default_reason="operator_requested_runtime_rollback",
        now=current_time,
    )

    next_state: dict[str, object] = {
        "schema_version": _SCHEMA_VERSION,
        "environment_class": next_environment_class,
        "runtime_status_path": next_runtime_status_path,
        "promotion_paused": next_promotion_paused,
        "promotion_pause_reason": next_promotion_pause_reason,
        "promotion_pause_expires_at": format_rollout_timestamp(next_promotion_pause_expires_at),
        "rollback_requested": next_rollback_requested,
        "rollback_reason": next_rollback_reason,
        "rollback_expires_at": format_rollout_timestamp(next_rollback_expires_at),
        "notes": next_notes,
        "updated_at": format_rollout_timestamp(current_time),
        "updated_by": _clean_text(actor_id) or current.updated_by,
    }

    previous_state = serialize_vfs_rollout_control_state(current)
    changed_fields = sorted(
        key
        for key, value in next_state.items()
        if previous_state.get(key) != value
    )
    raw_previous_history = previous_state.get("history")
    history = list(raw_previous_history) if isinstance(raw_previous_history, list) else []
    if changed_fields:
        history.insert(
            0,
            _new_history_entry(
                current_time=current_time,
                actor_id=_clean_text(actor_id) or current.updated_by,
                changed_fields=changed_fields,
                next_state=next_state,
            ),
        )
    next_state["history"] = history[:_MAX_HISTORY_ENTRIES]
    cleaned_next_state = _strip_none_values(next_state)
    preserved_state: dict[str, object] = (
        dict(raw_state)
        if raw_state is not None
        else {}
    )
    for key in next_state:
        if key in cleaned_next_state:
            preserved_state[key] = cleaned_next_state[key]
        else:
            preserved_state.pop(key, None)
    return preserved_state


def serialize_vfs_rollout_control_state(state: VfsRolloutControlState) -> dict[str, object]:
    """Return one JSON-serializable VFS rollout-control payload."""

    return _strip_none_values(
        {
            "schema_version": state.schema_version,
            "environment_class": state.environment_class,
            "runtime_status_path": state.runtime_status_path,
            "promotion_paused": state.promotion_paused,
            "promotion_pause_reason": state.promotion_pause_reason,
            "promotion_pause_expires_at": format_rollout_timestamp(
                state.promotion_pause_expires_at
            ),
            "rollback_requested": state.rollback_requested,
            "rollback_reason": state.rollback_reason,
            "rollback_expires_at": format_rollout_timestamp(state.rollback_expires_at),
            "notes": state.notes,
            "updated_at": format_rollout_timestamp(state.updated_at),
            "updated_by": state.updated_by,
            "history": [_serialize_history_entry(entry) for entry in state.history],
        }
    )


def has_active_promotion_pause(
    raw_state: Mapping[str, object] | None,
    *,
    now: datetime | None = None,
) -> bool:
    """Return whether the persisted promotion pause override is still active."""

    return build_vfs_rollout_control_state(raw_state, now=now).promotion_pause_active


def has_active_rollback(
    raw_state: Mapping[str, object] | None,
    *,
    now: datetime | None = None,
) -> bool:
    """Return whether the persisted rollback override is still active."""

    return build_vfs_rollout_control_state(raw_state, now=now).rollback_active


def _pick_text(current: str | None, incoming: object | None, *, allow_empty: bool = False) -> str | None:
    if incoming is None:
        return current
    return _clean_text(incoming, allow_empty=allow_empty)


def _pick_bool(
    current: bool,
    incoming: object | None,
    *,
    field_name: str,
    provided: set[str],
) -> bool:
    if field_name not in provided:
        return current
    return _clean_bool(incoming)


def _resolve_override_payload(
    *,
    requested: bool,
    current_reason: str | None,
    current_expires_at: datetime | None,
    explicit_reason: object | None,
    explicit_expires_at: object | None,
    reason_required: bool,
    default_reason: str,
    now: datetime,
) -> tuple[str | None, datetime | None]:
    if not requested:
        return None, None

    reason = _clean_text(explicit_reason) if explicit_reason is not None else current_reason
    if not reason:
        if reason_required:
            raise ValueError("rollback_reason_required")
        reason = default_reason

    if explicit_expires_at is not None:
        expires_at = _parse_override_expiry(explicit_expires_at, now=now)
    elif current_expires_at is not None and current_expires_at > now:
        expires_at = current_expires_at
    else:
        expires_at = now + timedelta(hours=_DEFAULT_OVERRIDE_WINDOW_HOURS)
    return reason, expires_at


def _parse_override_expiry(value: object, *, now: datetime) -> datetime:
    parsed = _parse_timestamp(value)
    if parsed is None:
        raise ValueError("override_expiry_invalid")
    if parsed <= now:
        raise ValueError("override_expiry_must_be_future")
    return parsed


def _new_history_entry(
    *,
    current_time: datetime,
    actor_id: str | None,
    changed_fields: list[str],
    next_state: Mapping[str, object],
) -> dict[str, object]:
    promotion_pause_expires_at = _parse_timestamp(next_state.get("promotion_pause_expires_at"))
    rollback_expires_at = _parse_timestamp(next_state.get("rollback_expires_at"))
    promotion_paused = _clean_bool(next_state.get("promotion_paused"))
    rollback_requested = _clean_bool(next_state.get("rollback_requested"))
    return _strip_none_values(
        {
            "entry_id": uuid4().hex,
            "recorded_at": format_rollout_timestamp(current_time),
            "actor_id": actor_id,
            "action": "write_control",
            "summary": _summarize_history_change(changed_fields, next_state),
            "environment_class": _clean_text(
                next_state.get("environment_class"),
                allow_empty=True,
            )
            or "",
            "runtime_status_path": _clean_text(next_state.get("runtime_status_path")),
            "promotion_paused": promotion_paused,
            "promotion_pause_reason": _clean_text(next_state.get("promotion_pause_reason")),
            "promotion_pause_expires_at": format_rollout_timestamp(promotion_pause_expires_at),
            "promotion_pause_active": _override_active(
                promotion_paused,
                promotion_pause_expires_at,
                now=current_time,
            ),
            "rollback_requested": rollback_requested,
            "rollback_reason": _clean_text(next_state.get("rollback_reason")),
            "rollback_expires_at": format_rollout_timestamp(rollback_expires_at),
            "rollback_active": _override_active(
                rollback_requested,
                rollback_expires_at,
                now=current_time,
            ),
            "notes": _clean_text(next_state.get("notes")),
        }
    )


def _summarize_history_change(
    changed_fields: list[str],
    next_state: Mapping[str, object],
) -> str:
    labels: list[str] = []
    if any(field.startswith("promotion_pause") or field == "promotion_paused" for field in changed_fields):
        labels.append(
            "promotion pause enabled"
            if _clean_bool(next_state.get("promotion_paused"))
            else "promotion pause cleared"
        )
    if any(field.startswith("rollback_") for field in changed_fields):
        labels.append(
            "rollback enabled"
            if _clean_bool(next_state.get("rollback_requested"))
            else "rollback cleared"
        )
    if "environment_class" in changed_fields:
        labels.append("environment updated")
    if "runtime_status_path" in changed_fields:
        labels.append("runtime status path updated")
    if "notes" in changed_fields:
        labels.append("notes updated")
    if not labels:
        labels.append("control state refreshed")
    environment_class = _clean_text(next_state.get("environment_class"), allow_empty=True) or "unset"
    return f"{'; '.join(labels)} ({environment_class})"


def _build_history_entry(entry: Mapping[str, object], *, now: datetime) -> VfsRolloutLedgerEntry:
    promotion_pause_expires_at = _parse_timestamp(entry.get("promotion_pause_expires_at"))
    rollback_expires_at = _parse_timestamp(entry.get("rollback_expires_at"))
    promotion_paused = _clean_bool(entry.get("promotion_paused"))
    rollback_requested = _clean_bool(entry.get("rollback_requested"))
    return VfsRolloutLedgerEntry(
        entry_id=_clean_text(entry.get("entry_id")) or uuid4().hex,
        recorded_at=_parse_timestamp(entry.get("recorded_at")) or now,
        actor_id=_clean_text(entry.get("actor_id")),
        action=_clean_text(entry.get("action")) or "write_control",
        summary=_clean_text(entry.get("summary")) or "control state refreshed",
        environment_class=_clean_text(entry.get("environment_class"), allow_empty=True) or "",
        runtime_status_path=_clean_text(entry.get("runtime_status_path")),
        promotion_paused=promotion_paused,
        promotion_pause_reason=_clean_text(entry.get("promotion_pause_reason")),
        promotion_pause_expires_at=promotion_pause_expires_at,
        promotion_pause_active=_override_active(
            promotion_paused,
            promotion_pause_expires_at,
            now=now,
        ),
        rollback_requested=rollback_requested,
        rollback_reason=_clean_text(entry.get("rollback_reason")),
        rollback_expires_at=rollback_expires_at,
        rollback_active=_override_active(
            rollback_requested,
            rollback_expires_at,
            now=now,
        ),
        notes=_clean_text(entry.get("notes")),
    )


def _serialize_history_entry(entry: VfsRolloutLedgerEntry) -> dict[str, object]:
    return _strip_none_values(
        {
            "entry_id": entry.entry_id,
            "recorded_at": format_rollout_timestamp(entry.recorded_at),
            "actor_id": entry.actor_id,
            "action": entry.action,
            "summary": entry.summary,
            "environment_class": entry.environment_class,
            "runtime_status_path": entry.runtime_status_path,
            "promotion_paused": entry.promotion_paused,
            "promotion_pause_reason": entry.promotion_pause_reason,
            "promotion_pause_expires_at": format_rollout_timestamp(
                entry.promotion_pause_expires_at
            ),
            "promotion_pause_active": entry.promotion_pause_active,
            "rollback_requested": entry.rollback_requested,
            "rollback_reason": entry.rollback_reason,
            "rollback_expires_at": format_rollout_timestamp(entry.rollback_expires_at),
            "rollback_active": entry.rollback_active,
            "notes": entry.notes,
        }
    )


def _strip_none_values(payload: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in payload.items() if value is not None}
