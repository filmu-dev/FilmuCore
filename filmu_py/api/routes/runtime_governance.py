"""Shared playback/VFS runtime governance snapshots for stream and operations routes."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Literal, cast

_PLAYBACK_PROOF_ARTIFACTS_ROOT = Path(__file__).resolve().parents[3] / "playback-proof-artifacts"
_MANAGED_WINDOWS_VFS_STATE_PATH = (
    _PLAYBACK_PROOF_ARTIFACTS_ROOT / "windows-native-stack" / "filmuvfs-windows-state.json"
)
_STREAM_REFRESH_LATENCY_SLO_MS = 250

def _empty_vfs_runtime_governance_snapshot() -> dict[str, int | float | str | list[str]]:
    """Return the default Rust runtime governance payload for /stream/status."""

    return {
        "vfs_runtime_snapshot_available": 0,
        "vfs_runtime_open_handles": 0,
        "vfs_runtime_peak_open_handles": 0,
        "vfs_runtime_active_reads": 0,
        "vfs_runtime_peak_active_reads": 0,
        "vfs_runtime_chunk_cache_weighted_bytes": 0,
        "vfs_runtime_chunk_cache_backend": "unknown",
        "vfs_runtime_chunk_cache_memory_bytes": 0,
        "vfs_runtime_chunk_cache_memory_max_bytes": 0,
        "vfs_runtime_chunk_cache_memory_hits": 0,
        "vfs_runtime_chunk_cache_memory_misses": 0,
        "vfs_runtime_chunk_cache_disk_bytes": 0,
        "vfs_runtime_chunk_cache_disk_max_bytes": 0,
        "vfs_runtime_chunk_cache_disk_hits": 0,
        "vfs_runtime_chunk_cache_disk_misses": 0,
        "vfs_runtime_chunk_cache_disk_writes": 0,
        "vfs_runtime_chunk_cache_disk_write_errors": 0,
        "vfs_runtime_chunk_cache_disk_evictions": 0,
        "vfs_runtime_handle_startup_total": 0,
        "vfs_runtime_handle_startup_ok": 0,
        "vfs_runtime_handle_startup_error": 0,
        "vfs_runtime_handle_startup_estale": 0,
        "vfs_runtime_handle_startup_cancelled": 0,
        "vfs_runtime_handle_startup_average_duration_ms": 0,
        "vfs_runtime_handle_startup_max_duration_ms": 0,
        "vfs_runtime_mounted_reads_total": 0,
        "vfs_runtime_mounted_reads_ok": 0,
        "vfs_runtime_mounted_reads_error": 0,
        "vfs_runtime_mounted_reads_estale": 0,
        "vfs_runtime_mounted_reads_cancelled": 0,
        "vfs_runtime_mounted_reads_average_duration_ms": 0,
        "vfs_runtime_mounted_reads_max_duration_ms": 0,
        "vfs_runtime_upstream_fetch_operations": 0,
        "vfs_runtime_upstream_fetch_bytes_total": 0,
        "vfs_runtime_upstream_fetch_average_duration_ms": 0,
        "vfs_runtime_upstream_fetch_max_duration_ms": 0,
        "vfs_runtime_upstream_fail_invalid_url": 0,
        "vfs_runtime_upstream_fail_build_request": 0,
        "vfs_runtime_upstream_fail_network": 0,
        "vfs_runtime_upstream_fail_stale_status": 0,
        "vfs_runtime_upstream_fail_unexpected_status": 0,
        "vfs_runtime_upstream_fail_unexpected_status_too_many_requests": 0,
        "vfs_runtime_upstream_fail_unexpected_status_server_error": 0,
        "vfs_runtime_upstream_fail_read_body": 0,
        "vfs_runtime_upstream_retryable_network": 0,
        "vfs_runtime_upstream_retryable_read_body": 0,
        "vfs_runtime_upstream_retryable_status_too_many_requests": 0,
        "vfs_runtime_upstream_retryable_status_server_error": 0,
        "vfs_runtime_backend_fallback_attempts": 0,
        "vfs_runtime_backend_fallback_success": 0,
        "vfs_runtime_backend_fallback_failure": 0,
        "vfs_runtime_backend_fallback_attempts_direct_read_failure": 0,
        "vfs_runtime_backend_fallback_attempts_inline_refresh_unavailable": 0,
        "vfs_runtime_backend_fallback_attempts_post_inline_refresh_failure": 0,
        "vfs_runtime_backend_fallback_success_direct_read_failure": 0,
        "vfs_runtime_backend_fallback_success_inline_refresh_unavailable": 0,
        "vfs_runtime_backend_fallback_success_post_inline_refresh_failure": 0,
        "vfs_runtime_backend_fallback_failure_direct_read_failure": 0,
        "vfs_runtime_backend_fallback_failure_inline_refresh_unavailable": 0,
        "vfs_runtime_backend_fallback_failure_post_inline_refresh_failure": 0,
        "vfs_runtime_chunk_cache_hits": 0,
        "vfs_runtime_chunk_cache_misses": 0,
        "vfs_runtime_chunk_cache_inserts": 0,
        "vfs_runtime_chunk_cache_prefetch_hits": 0,
        "vfs_runtime_prefetch_concurrency_limit": 0,
        "vfs_runtime_prefetch_available_permits": 0,
        "vfs_runtime_prefetch_active_permits": 0,
        "vfs_runtime_prefetch_active_background_tasks": 0,
        "vfs_runtime_prefetch_peak_active_background_tasks": 0,
        "vfs_runtime_prefetch_background_spawned": 0,
        "vfs_runtime_prefetch_background_backpressure": 0,
        "vfs_runtime_prefetch_fairness_denied": 0,
        "vfs_runtime_prefetch_global_backpressure_denied": 0,
        "vfs_runtime_prefetch_background_error": 0,
        "vfs_runtime_chunk_coalescing_in_flight_chunks": 0,
        "vfs_runtime_chunk_coalescing_peak_in_flight_chunks": 0,
        "vfs_runtime_chunk_coalescing_waits_total": 0,
        "vfs_runtime_chunk_coalescing_waits_hit": 0,
        "vfs_runtime_chunk_coalescing_waits_miss": 0,
        "vfs_runtime_chunk_coalescing_wait_average_duration_ms": 0.0,
        "vfs_runtime_chunk_coalescing_wait_max_duration_ms": 0.0,
        "vfs_runtime_inline_refresh_success": 0,
        "vfs_runtime_inline_refresh_no_url": 0,
        "vfs_runtime_inline_refresh_error": 0,
        "vfs_runtime_inline_refresh_timeout": 0,
        "vfs_runtime_windows_callbacks_cancelled": 0,
        "vfs_runtime_windows_callbacks_error": 0,
        "vfs_runtime_windows_callbacks_estale": 0,
        "vfs_runtime_cache_hit_ratio": 0.0,
        "vfs_runtime_fallback_success_ratio": 0.0,
        "vfs_runtime_prefetch_pressure_ratio": 0.0,
        "vfs_runtime_provider_pressure_incidents": 0,
        "vfs_runtime_fairness_pressure_incidents": 0,
        "vfs_runtime_cache_pressure_class": "healthy",
        "vfs_runtime_cache_pressure_reasons": [],
        "vfs_runtime_chunk_coalescing_pressure_class": "healthy",
        "vfs_runtime_chunk_coalescing_pressure_reasons": [],
        "vfs_runtime_upstream_wait_class": "healthy",
        "vfs_runtime_upstream_wait_reasons": [],
        "vfs_runtime_refresh_pressure_class": "healthy",
        "vfs_runtime_refresh_pressure_reasons": [],
        "vfs_runtime_rollout_readiness": "unknown",
        "vfs_runtime_rollout_reasons": ["runtime_snapshot_unavailable"],
        "vfs_runtime_rollout_next_action": "capture_runtime_status",
        "vfs_runtime_rollout_canary_decision": "capture_runtime_status",
        "vfs_runtime_rollout_merge_gate": "blocked",
        "vfs_runtime_rollout_environment_class": "",
        "vfs_runtime_active_handles_visible": 0,
        "vfs_runtime_active_handles_hidden": 0,
        "vfs_runtime_active_handle_tenant_count": 0,
        "vfs_runtime_active_handle_tenants": [],
        "vfs_runtime_active_handle_summaries": [],
    }


def _as_int(value: object) -> int:
    """Normalize Rust runtime JSON numbers into additive integer counters."""

    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return 0
        try:
            return int(stripped)
        except ValueError:
            try:
                return round(float(stripped))
            except ValueError:
                return 0
    return 0


def _as_float(value: object) -> float:
    """Normalize Rust runtime JSON numbers into additive float durations."""

    if isinstance(value, bool):
        return float(value)
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return 0.0
        try:
            return float(stripped)
        except ValueError:
            return 0.0
    return 0.0


def _as_str(value: object, *, default: str = "") -> str:
    """Normalize Rust runtime JSON string values into safe status payloads."""

    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return stripped
    return default


def _as_str_list(value: object) -> list[str]:
    """Normalize list-like runtime snapshot strings into bounded operator summaries."""

    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        stripped = item.strip()
        if stripped:
            normalized.append(stripped)
    return normalized[:10]


def _safe_ratio(numerator: int, denominator: int) -> float:
    """Return a bounded operator-facing ratio for additive governance counters."""

    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _pressure_class(
    *, critical: bool, warning: bool
) -> Literal["healthy", "warning", "critical"]:
    """Collapse additive runtime signals into a bounded operator pressure class."""

    if critical:
        return "critical"
    if warning:
        return "warning"
    return "healthy"


def _nested_mapping_value(payload: object, *keys: str) -> object | None:
    """Safely walk nested JSON objects loaded from the Rust runtime status file."""

    current = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _normalize_active_handle_summary(summary: str) -> tuple[str, str]:
    """Return tenant id plus a tenant-safe active-handle summary line."""

    parts = summary.split("|")
    if len(parts) >= 5 and parts[-1].startswith("invalidated="):
        tenant_id = (parts[0].strip() or "unknown").lower()
        session_id = parts[1].strip()
        handle_key = parts[2].strip()
        invalidated = parts[-1].strip()
    elif len(parts) >= 4 and parts[-1].startswith("invalidated="):
        tenant_id = "unknown"
        session_id = parts[0].strip()
        handle_key = parts[1].strip()
        invalidated = parts[-1].strip()
    else:
        tenant_id = "unknown"
        session_id = ""
        handle_key = hashlib.sha256(summary.encode("utf-8")).hexdigest()[:16]
        invalidated = "invalidated=unknown"
    safe_line = f"{tenant_id}|{session_id}|{handle_key}|{invalidated}"
    return tenant_id, safe_line


def _tenant_safe_runtime_handle_summaries(
    raw_summaries: object,
    *,
    request_tenant_id: str | None = None,
    authorized_tenant_ids: set[str] | None = None,
) -> tuple[list[str], int, int, list[str]]:
    """Filter active-handle summaries to tenant-safe, request-scoped telemetry."""

    normalized = _as_str_list(raw_summaries)
    allowed_tenants = {tenant.strip().lower() for tenant in (authorized_tenant_ids or set()) if tenant}
    if request_tenant_id is not None and request_tenant_id.strip():
        allowed_tenants.add(request_tenant_id.strip().lower())
    allow_all = not allowed_tenants

    visible: list[str] = []
    hidden = 0
    visible_tenants: set[str] = set()
    for summary in normalized:
        tenant_id, safe_line = _normalize_active_handle_summary(summary)
        if allow_all or tenant_id in allowed_tenants or tenant_id == "unknown":
            visible.append(safe_line)
            visible_tenants.add(tenant_id)
        else:
            hidden += 1

    return visible[:10], len(visible), hidden, sorted(visible_tenants)


def _runtime_pressure_requires_queued_dispatch(
    governance: dict[str, int | float | str | list[str]],
) -> tuple[bool, bool]:
    """Return queued-dispatch recommendation and latency-SLO breach flag."""

    avg_latency_ms = _as_int(governance.get("vfs_runtime_upstream_fetch_average_duration_ms"))
    max_latency_ms = _as_int(governance.get("vfs_runtime_upstream_fetch_max_duration_ms"))
    latency_slo_breached = avg_latency_ms > _STREAM_REFRESH_LATENCY_SLO_MS or max_latency_ms > (
        _STREAM_REFRESH_LATENCY_SLO_MS * 2
    )
    pressure_requires_queue = (
        _as_str(governance.get("vfs_runtime_refresh_pressure_class"), default="healthy")
        in {"warning", "critical"}
        or _as_str(governance.get("vfs_runtime_upstream_wait_class"), default="healthy")
        in {"warning", "critical"}
        or _as_str(governance.get("vfs_runtime_chunk_coalescing_pressure_class"), default="healthy")
        in {"warning", "critical"}
        or _as_int(governance.get("vfs_runtime_provider_pressure_incidents")) > 0
        or latency_slo_breached
    )
    return pressure_requires_queue, latency_slo_breached


def _candidate_vfs_runtime_status_paths() -> list[Path]:
    """Return the preferred Rust runtime snapshot locations in precedence order."""

    paths: list[Path] = []
    env_path = os.getenv("FILMU_PY_VFS_RUNTIME_STATUS_PATH")
    if env_path and env_path.strip():
        paths.append(Path(env_path.strip()))
    try:
        state_payload = json.loads(_MANAGED_WINDOWS_VFS_STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        state_payload = None
    if isinstance(state_payload, dict):
        runtime_status_path = state_payload.get("runtime_status_path")
        if isinstance(runtime_status_path, str) and runtime_status_path.strip():
            paths.append(Path(runtime_status_path.strip()))
    paths.append(_MANAGED_WINDOWS_VFS_STATE_PATH.parent / "filmuvfs-runtime-status.json")
    unique_paths: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        normalized = path.expanduser()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_paths.append(normalized)
    return unique_paths


def _load_vfs_runtime_status_payload() -> dict[str, object] | None:
    """Load the first readable Rust runtime status JSON payload, if any."""

    for path in _candidate_vfs_runtime_status_paths():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            return cast(dict[str, object], payload)
    return None


def _candidate_playback_artifacts_roots() -> list[Path]:
    """Return playback-proof artifact roots in precedence order."""

    roots: list[Path] = []
    env_root = os.getenv("FILMU_PY_PLAYBACK_PROOF_ARTIFACTS_ROOT")
    if env_root and env_root.strip():
        roots.append(Path(env_root.strip()))
    roots.append(_PLAYBACK_PROOF_ARTIFACTS_ROOT)

    unique_roots: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        normalized = root.expanduser()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_roots.append(normalized)
    return unique_roots


def _candidate_github_main_policy_paths() -> list[Path]:
    """Return candidate current-policy artifact paths in precedence order."""

    paths: list[Path] = []
    env_path = os.getenv("FILMU_PY_GITHUB_MAIN_POLICY_PATH")
    if env_path and env_path.strip():
        paths.append(Path(env_path.strip()))
    for root in _candidate_playback_artifacts_roots():
        paths.append(root / "github-main-policy-current.json")

    unique_paths: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        normalized = path.expanduser()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_paths.append(normalized)
    return unique_paths


def _load_json_file(path: Path) -> dict[str, object] | None:
    """Load one JSON file if it exists and contains an object payload."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if isinstance(payload, dict):
        return cast(dict[str, object], payload)
    return None


def _load_latest_json_artifact(*, prefix: str, subdir: str | None = None) -> dict[str, object] | None:
    """Load the newest matching JSON artifact from the playback-proof artifact tree."""

    for root in _candidate_playback_artifacts_roots():
        candidate_root = root / subdir if subdir is not None else root
        try:
            matches = list(candidate_root.glob(f"{prefix}*.json"))
        except OSError:
            continue
        newest_path: Path | None = None
        newest_mtime = -1.0
        for path in matches:
            try:
                stat = path.stat()
            except OSError:
                continue
            if stat.st_mtime > newest_mtime:
                newest_mtime = stat.st_mtime
                newest_path = path
        if newest_path is None:
            continue
        payload = _load_json_file(newest_path)
        if payload is not None:
            return payload
    return None


def _load_current_github_main_policy_artifact() -> dict[str, object] | None:
    """Load the newest available GitHub main-policy validation artifact, if present."""

    for path in _candidate_github_main_policy_paths():
        payload = _load_json_file(path)
        if payload is not None:
            return payload
    return None


def _load_playback_artifact_at_relative_path(relative_path: str) -> dict[str, object] | None:
    """Load one playback-proof artifact by relative path across candidate roots."""

    for root in _candidate_playback_artifacts_roots():
        payload = _load_json_file(root / relative_path)
        if payload is not None:
            return payload
    return None


def _empty_playback_gate_governance_snapshot() -> dict[str, int | str | list[str]]:
    """Return the default playback-gate promotion snapshot."""

    return {
        "playback_gate_snapshot_available": 0,
        "playback_gate_artifact_generated_at": "",
        "playback_gate_environment_class": "",
        "playback_gate_repeat_count": 0,
        "playback_gate_gate_mode": "unknown",
        "playback_gate_provider_gate_required": 0,
        "playback_gate_provider_gate_ran": 0,
        "playback_gate_stability_ready": 0,
        "playback_gate_provider_parity_ready": 0,
        "playback_gate_windows_provider_ready": 0,
        "playback_gate_windows_soak_ready": 0,
        "playback_gate_policy_validation_status": "unverified",
        "playback_gate_policy_ready": 0,
        "playback_gate_rollout_readiness": "not_ready",
        "playback_gate_rollout_reasons": ["missing_playback_gate_artifacts"],
        "playback_gate_rollout_next_action": "run_proof_playback_gate_enterprise",
    }


def _playback_gate_governance_snapshot() -> dict[str, int | str | list[str]]:
    """Return machine-shaped playback-gate promotion posture from local artifacts."""

    governance = _empty_playback_gate_governance_snapshot()
    stability_summary = _load_latest_json_artifact(prefix="stability-summary-")
    ci_summary = _load_playback_artifact_at_relative_path("ci-execution-summary.json")
    provider_summary = _load_latest_json_artifact(prefix="media-server-gate-")
    windows_provider_summary = _load_latest_json_artifact(prefix="windows-media-server-gate-")
    windows_soak_summary = _load_latest_json_artifact(
        prefix="soak-stability-",
        subdir="windows-native-stack",
    )
    policy_summary = _load_current_github_main_policy_artifact()

    if stability_summary is not None:
        governance["playback_gate_snapshot_available"] = 1
        governance["playback_gate_artifact_generated_at"] = _as_str(
            stability_summary.get("timestamp"),
        )
        governance["playback_gate_environment_class"] = _as_str(
            stability_summary.get("environment_class"),
        )
        governance["playback_gate_repeat_count"] = _as_int(stability_summary.get("repeat_count"))
        if bool(stability_summary.get("all_green")) and not bool(stability_summary.get("dry_run")):
            governance["playback_gate_stability_ready"] = 1

    if ci_summary is not None:
        governance["playback_gate_gate_mode"] = _as_str(
            ci_summary.get("gate_mode"),
            default="unknown",
        )
        governance["playback_gate_provider_gate_required"] = _as_int(
            ci_summary.get("provider_gate_required"),
        )
        governance["playback_gate_provider_gate_ran"] = _as_int(
            ci_summary.get("provider_gate_ran"),
        )

    provider_summary_available = provider_summary is not None
    if provider_summary is not None and bool(provider_summary.get("all_green")):
        governance["playback_gate_provider_parity_ready"] = 1

    windows_provider_summary_available = windows_provider_summary is not None
    if windows_provider_summary is not None:
        results = windows_provider_summary.get("results")
        if (
            isinstance(results, list)
            and any(
                isinstance(result, dict) and result.get("status") == "passed" for result in results
            )
            and all(
                not isinstance(result, dict) or result.get("status") in {"passed", "skipped"}
                for result in results
            )
        ):
            governance["playback_gate_windows_provider_ready"] = 1

    windows_soak_summary_available = windows_soak_summary is not None
    if windows_soak_summary is not None and bool(windows_soak_summary.get("all_green")):
        governance["playback_gate_windows_soak_ready"] = 1

    if policy_summary is not None:
        validation = policy_summary.get("validation")
        if isinstance(validation, dict):
            validation_status = _as_str(validation.get("status"), default="unverified")
            governance["playback_gate_policy_validation_status"] = validation_status
            if validation_status == "ready":
                governance["playback_gate_policy_ready"] = 1

    rollout_reasons: list[str] = []
    if governance["playback_gate_snapshot_available"] == 0:
        rollout_reasons.append("missing_playback_gate_artifacts")
    elif governance["playback_gate_gate_mode"] == "dry_run":
        rollout_reasons.append("playback_gate_dry_run_mode")
    elif governance["playback_gate_stability_ready"] == 0:
        rollout_reasons.append("playback_gate_failed_or_incomplete")

    provider_gate_required = _as_int(governance["playback_gate_provider_gate_required"]) > 0
    provider_gate_ran = _as_int(governance["playback_gate_provider_gate_ran"]) > 0
    if provider_gate_required and not provider_gate_ran:
        rollout_reasons.append("provider_gate_not_run")
    elif provider_gate_ran:
        if not provider_summary_available:
            rollout_reasons.append("provider_gate_artifact_missing")
        elif _as_int(governance["playback_gate_provider_parity_ready"]) == 0:
            rollout_reasons.append("provider_gate_not_green")

    if not windows_provider_summary_available:
        rollout_reasons.append("windows_provider_gate_artifact_missing")
    elif _as_int(governance["playback_gate_windows_provider_ready"]) == 0:
        rollout_reasons.append("windows_provider_gate_not_green")

    if not windows_soak_summary_available:
        rollout_reasons.append("windows_vfs_soak_artifact_missing")
    elif _as_int(governance["playback_gate_windows_soak_ready"]) == 0:
        rollout_reasons.append("windows_vfs_soak_not_green")

    policy_status = _as_str(governance["playback_gate_policy_validation_status"], default="unverified")
    if policy_status == "not_ready":
        rollout_reasons.append("github_main_policy_not_ready")
    elif policy_status == "unverified":
        rollout_reasons.append("github_main_policy_unverified")

    blocked_reasons = {
        "playback_gate_failed_or_incomplete",
        "provider_gate_not_green",
        "windows_provider_gate_not_green",
        "windows_vfs_soak_not_green",
        "github_main_policy_not_ready",
    }
    warning_reasons = {
        "missing_playback_gate_artifacts",
        "playback_gate_dry_run_mode",
        "provider_gate_not_run",
        "provider_gate_artifact_missing",
        "windows_provider_gate_artifact_missing",
        "windows_vfs_soak_artifact_missing",
        "github_main_policy_unverified",
    }

    if any(reason in blocked_reasons for reason in rollout_reasons):
        governance["playback_gate_rollout_readiness"] = "blocked"
        governance["playback_gate_rollout_next_action"] = "resolve_failed_playback_gate_proofs"
    elif any(reason in warning_reasons for reason in rollout_reasons):
        governance["playback_gate_rollout_readiness"] = "warning"
        governance["playback_gate_rollout_next_action"] = "record_enterprise_playback_gate_evidence"
    else:
        governance["playback_gate_rollout_readiness"] = "ready"
        governance["playback_gate_rollout_next_action"] = "keep_required_checks_enforced"
        rollout_reasons.append("enterprise_playback_gate_green")

    governance["playback_gate_rollout_reasons"] = rollout_reasons
    return governance


def _apply_vfs_rollout_policy(
    governance: dict[str, int | float | str | list[str]],
    *,
    playback_gate_governance: dict[str, int | str | list[str]] | None = None,
) -> dict[str, int | float | str | list[str]]:
    """Apply canary and rollback policy to the runtime-derived VFS rollout posture."""

    canary_environment = ""
    if playback_gate_governance is not None:
        canary_environment = _as_str(
            playback_gate_governance.get("playback_gate_environment_class"),
        )

    governance["vfs_runtime_rollout_environment_class"] = canary_environment
    governance["vfs_runtime_rollout_canary_decision"] = "capture_runtime_status"
    governance["vfs_runtime_rollout_merge_gate"] = "blocked"

    if _as_int(governance["vfs_runtime_snapshot_available"]) <= 0:
        return governance

    rollout_readiness = _as_str(
        governance["vfs_runtime_rollout_readiness"],
        default="unknown",
    )
    windows_soak_ready = (
        playback_gate_governance is not None
        and _as_int(playback_gate_governance.get("playback_gate_windows_soak_ready")) > 0
    )

    if rollout_readiness == "blocked":
        governance["vfs_runtime_rollout_canary_decision"] = "rollback_current_environment"
        governance["vfs_runtime_rollout_merge_gate"] = "blocked"
    elif not windows_soak_ready:
        governance["vfs_runtime_rollout_canary_decision"] = "hold_until_windows_soak_is_green"
        governance["vfs_runtime_rollout_merge_gate"] = "hold"
        rollout_reasons = cast(list[str], governance["vfs_runtime_rollout_reasons"])
        if "windows_vfs_soak_not_green" not in rollout_reasons:
            rollout_reasons.append("windows_vfs_soak_not_green")
    elif rollout_readiness == "warning":
        governance["vfs_runtime_rollout_canary_decision"] = "hold_canary_and_repeat_soak"
        governance["vfs_runtime_rollout_merge_gate"] = "hold"
    else:
        governance["vfs_runtime_rollout_canary_decision"] = "promote_to_next_environment_class"
        governance["vfs_runtime_rollout_merge_gate"] = "ready"

    return governance


def _vfs_runtime_governance_snapshot(
    playback_gate_governance: dict[str, int | str | list[str]] | None = None,
    *,
    request_tenant_id: str | None = None,
    authorized_tenant_ids: set[str] | None = None,
) -> dict[str, int | float | str | list[str]]:
    """Return additive governance counters extracted from the Rust runtime snapshot."""

    payload = _load_vfs_runtime_status_payload()
    governance = _empty_vfs_runtime_governance_snapshot()
    if payload is None:
        return _apply_vfs_rollout_policy(
            governance,
            playback_gate_governance=playback_gate_governance,
        )
    governance["vfs_runtime_snapshot_available"] = 1
    governance["vfs_runtime_open_handles"] = _as_int(_nested_mapping_value(payload, "runtime", "open_handles"))
    governance["vfs_runtime_peak_open_handles"] = _as_int(
        _nested_mapping_value(payload, "runtime", "peak_open_handles")
    )
    governance["vfs_runtime_active_reads"] = _as_int(_nested_mapping_value(payload, "runtime", "active_reads"))
    governance["vfs_runtime_peak_active_reads"] = _as_int(
        _nested_mapping_value(payload, "runtime", "peak_active_reads")
    )
    governance["vfs_runtime_chunk_cache_weighted_bytes"] = _as_int(
        _nested_mapping_value(payload, "runtime", "chunk_cache_weighted_bytes")
    )
    governance["vfs_runtime_chunk_cache_backend"] = _as_str(
        _nested_mapping_value(payload, "chunk_cache", "backend"),
        default="unknown",
    )
    governance["vfs_runtime_chunk_cache_memory_bytes"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "memory_bytes")
    )
    governance["vfs_runtime_chunk_cache_memory_max_bytes"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "memory_max_bytes")
    )
    governance["vfs_runtime_chunk_cache_memory_hits"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "memory_hits")
    )
    governance["vfs_runtime_chunk_cache_memory_misses"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "memory_misses")
    )
    governance["vfs_runtime_chunk_cache_disk_bytes"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_bytes")
    )
    governance["vfs_runtime_chunk_cache_disk_max_bytes"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_max_bytes")
    )
    governance["vfs_runtime_chunk_cache_disk_hits"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_hits")
    )
    governance["vfs_runtime_chunk_cache_disk_misses"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_misses")
    )
    governance["vfs_runtime_chunk_cache_disk_writes"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_writes")
    )
    governance["vfs_runtime_chunk_cache_disk_write_errors"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_write_errors")
    )
    governance["vfs_runtime_chunk_cache_disk_evictions"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "disk_evictions")
    )
    governance["vfs_runtime_handle_startup_total"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "total")
    )
    governance["vfs_runtime_handle_startup_ok"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "ok")
    )
    governance["vfs_runtime_handle_startup_error"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "error")
    )
    governance["vfs_runtime_handle_startup_estale"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "estale")
    )
    governance["vfs_runtime_handle_startup_cancelled"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "cancelled")
    )
    governance["vfs_runtime_handle_startup_average_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "average_duration_ms")
    )
    governance["vfs_runtime_handle_startup_max_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "handle_startup", "max_duration_ms")
    )
    governance["vfs_runtime_mounted_reads_total"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "total")
    )
    governance["vfs_runtime_mounted_reads_ok"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "ok")
    )
    governance["vfs_runtime_mounted_reads_error"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "error")
    )
    governance["vfs_runtime_mounted_reads_estale"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "estale")
    )
    governance["vfs_runtime_mounted_reads_cancelled"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "cancelled")
    )
    governance["vfs_runtime_mounted_reads_average_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "average_duration_ms")
    )
    governance["vfs_runtime_mounted_reads_max_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "mounted_reads", "max_duration_ms")
    )
    governance["vfs_runtime_upstream_fetch_operations"] = _as_int(
        _nested_mapping_value(payload, "upstream_fetch", "operations")
    )
    governance["vfs_runtime_upstream_fetch_bytes_total"] = _as_int(
        _nested_mapping_value(payload, "upstream_fetch", "bytes_total")
    )
    governance["vfs_runtime_upstream_fetch_average_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "upstream_fetch", "average_duration_ms")
    )
    governance["vfs_runtime_upstream_fetch_max_duration_ms"] = _as_int(
        _nested_mapping_value(payload, "upstream_fetch", "max_duration_ms")
    )
    governance["vfs_runtime_upstream_fail_invalid_url"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "invalid_url")
    )
    governance["vfs_runtime_upstream_fail_build_request"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "build_request")
    )
    governance["vfs_runtime_upstream_fail_network"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "network")
    )
    governance["vfs_runtime_upstream_fail_stale_status"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "stale_status")
    )
    governance["vfs_runtime_upstream_fail_unexpected_status"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "unexpected_status")
    )
    governance["vfs_runtime_upstream_fail_unexpected_status_too_many_requests"] = _as_int(
        _nested_mapping_value(
            payload,
            "upstream_failures",
            "unexpected_status_too_many_requests",
        )
    )
    governance["vfs_runtime_upstream_fail_unexpected_status_server_error"] = _as_int(
        _nested_mapping_value(
            payload,
            "upstream_failures",
            "unexpected_status_server_error",
        )
    )
    governance["vfs_runtime_upstream_fail_read_body"] = _as_int(
        _nested_mapping_value(payload, "upstream_failures", "read_body")
    )
    governance["vfs_runtime_upstream_retryable_network"] = _as_int(
        _nested_mapping_value(payload, "upstream_retryable_events", "network")
    )
    governance["vfs_runtime_upstream_retryable_read_body"] = _as_int(
        _nested_mapping_value(payload, "upstream_retryable_events", "read_body")
    )
    governance["vfs_runtime_upstream_retryable_status_too_many_requests"] = _as_int(
        _nested_mapping_value(
            payload,
            "upstream_retryable_events",
            "status_too_many_requests",
        )
    )
    governance["vfs_runtime_upstream_retryable_status_server_error"] = _as_int(
        _nested_mapping_value(payload, "upstream_retryable_events", "status_server_error")
    )
    governance["vfs_runtime_backend_fallback_attempts"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "attempts")
    )
    governance["vfs_runtime_backend_fallback_success"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "success")
    )
    governance["vfs_runtime_backend_fallback_failure"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "failure")
    )
    governance["vfs_runtime_backend_fallback_attempts_direct_read_failure"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "attempts_direct_read_failure")
    )
    governance["vfs_runtime_backend_fallback_attempts_inline_refresh_unavailable"] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "attempts_inline_refresh_unavailable",
        )
    )
    governance[
        "vfs_runtime_backend_fallback_attempts_post_inline_refresh_failure"
    ] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "attempts_post_inline_refresh_failure",
        )
    )
    governance["vfs_runtime_backend_fallback_success_direct_read_failure"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "success_direct_read_failure")
    )
    governance["vfs_runtime_backend_fallback_success_inline_refresh_unavailable"] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "success_inline_refresh_unavailable",
        )
    )
    governance[
        "vfs_runtime_backend_fallback_success_post_inline_refresh_failure"
    ] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "success_post_inline_refresh_failure",
        )
    )
    governance["vfs_runtime_backend_fallback_failure_direct_read_failure"] = _as_int(
        _nested_mapping_value(payload, "backend_fallback", "failure_direct_read_failure")
    )
    governance["vfs_runtime_backend_fallback_failure_inline_refresh_unavailable"] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "failure_inline_refresh_unavailable",
        )
    )
    governance[
        "vfs_runtime_backend_fallback_failure_post_inline_refresh_failure"
    ] = _as_int(
        _nested_mapping_value(
            payload,
            "backend_fallback",
            "failure_post_inline_refresh_failure",
        )
    )
    governance["vfs_runtime_chunk_cache_hits"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "hits")
    )
    governance["vfs_runtime_chunk_cache_misses"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "misses")
    )
    governance["vfs_runtime_chunk_cache_inserts"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "inserts")
    )
    governance["vfs_runtime_chunk_cache_prefetch_hits"] = _as_int(
        _nested_mapping_value(payload, "chunk_cache", "prefetch_hits")
    )
    governance["vfs_runtime_prefetch_concurrency_limit"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "concurrency_limit")
    )
    governance["vfs_runtime_prefetch_available_permits"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "available_permits")
    )
    governance["vfs_runtime_prefetch_active_permits"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "active_permits")
    )
    governance["vfs_runtime_prefetch_active_background_tasks"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "active_background_tasks")
    )
    governance["vfs_runtime_prefetch_peak_active_background_tasks"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "peak_active_background_tasks")
    )
    governance["vfs_runtime_prefetch_background_spawned"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "background_spawned")
    )
    governance["vfs_runtime_prefetch_background_backpressure"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "background_backpressure")
    )
    governance["vfs_runtime_prefetch_fairness_denied"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "fairness_denied")
    )
    governance["vfs_runtime_prefetch_global_backpressure_denied"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "global_backpressure_denied")
    )
    governance["vfs_runtime_prefetch_background_error"] = _as_int(
        _nested_mapping_value(payload, "prefetch", "background_error")
    )
    governance["vfs_runtime_chunk_coalescing_in_flight_chunks"] = _as_int(
        _nested_mapping_value(payload, "chunk_coalescing", "in_flight_chunks")
    )
    governance["vfs_runtime_chunk_coalescing_peak_in_flight_chunks"] = _as_int(
        _nested_mapping_value(payload, "chunk_coalescing", "peak_in_flight_chunks")
    )
    governance["vfs_runtime_chunk_coalescing_waits_total"] = _as_int(
        _nested_mapping_value(payload, "chunk_coalescing", "waits_total")
    )
    governance["vfs_runtime_chunk_coalescing_waits_hit"] = _as_int(
        _nested_mapping_value(payload, "chunk_coalescing", "waits_hit")
    )
    governance["vfs_runtime_chunk_coalescing_waits_miss"] = _as_int(
        _nested_mapping_value(payload, "chunk_coalescing", "waits_miss")
    )
    governance["vfs_runtime_chunk_coalescing_wait_average_duration_ms"] = _as_float(
        _nested_mapping_value(payload, "chunk_coalescing", "wait_average_duration_ms")
    )
    governance["vfs_runtime_chunk_coalescing_wait_max_duration_ms"] = _as_float(
        _nested_mapping_value(payload, "chunk_coalescing", "wait_max_duration_ms")
    )
    governance["vfs_runtime_inline_refresh_success"] = _as_int(
        _nested_mapping_value(payload, "inline_refresh", "success")
    )
    governance["vfs_runtime_inline_refresh_no_url"] = _as_int(
        _nested_mapping_value(payload, "inline_refresh", "no_url")
    )
    governance["vfs_runtime_inline_refresh_error"] = _as_int(
        _nested_mapping_value(payload, "inline_refresh", "error")
    )
    governance["vfs_runtime_inline_refresh_timeout"] = _as_int(
        _nested_mapping_value(payload, "inline_refresh", "timeout")
    )
    governance["vfs_runtime_windows_callbacks_cancelled"] = _as_int(
        _nested_mapping_value(payload, "windows_projfs", "callbacks_cancelled")
    )
    governance["vfs_runtime_windows_callbacks_error"] = _as_int(
        _nested_mapping_value(payload, "windows_projfs", "callbacks_error")
    )
    governance["vfs_runtime_windows_callbacks_estale"] = _as_int(
        _nested_mapping_value(payload, "windows_projfs", "callbacks_estale")
    )
    total_cache_lookups = (
        _as_int(governance["vfs_runtime_chunk_cache_hits"])
        + _as_int(governance["vfs_runtime_chunk_cache_misses"])
    )
    governance["vfs_runtime_cache_hit_ratio"] = _safe_ratio(
        _as_int(governance["vfs_runtime_chunk_cache_hits"]),
        total_cache_lookups,
    )
    governance["vfs_runtime_fallback_success_ratio"] = _safe_ratio(
        _as_int(governance["vfs_runtime_backend_fallback_success"]),
        _as_int(governance["vfs_runtime_backend_fallback_attempts"]),
    )
    governance["vfs_runtime_prefetch_pressure_ratio"] = _safe_ratio(
        _as_int(governance["vfs_runtime_prefetch_active_permits"]),
        _as_int(governance["vfs_runtime_prefetch_active_permits"])
        + _as_int(governance["vfs_runtime_prefetch_available_permits"]),
    )
    governance["vfs_runtime_provider_pressure_incidents"] = (
        _as_int(governance["vfs_runtime_upstream_fail_unexpected_status_too_many_requests"])
        + _as_int(governance["vfs_runtime_upstream_fail_unexpected_status_server_error"])
        + _as_int(governance["vfs_runtime_upstream_retryable_status_too_many_requests"])
        + _as_int(governance["vfs_runtime_upstream_retryable_status_server_error"])
        + _as_int(governance["vfs_runtime_prefetch_background_backpressure"])
    )
    governance["vfs_runtime_fairness_pressure_incidents"] = (
        _as_int(governance["vfs_runtime_prefetch_fairness_denied"])
        + _as_int(governance["vfs_runtime_prefetch_global_backpressure_denied"])
    )
    cache_pressure_reasons: list[str] = []
    cache_memory_pressure_ratio = _safe_ratio(
        _as_int(governance["vfs_runtime_chunk_cache_memory_bytes"]),
        _as_int(governance["vfs_runtime_chunk_cache_memory_max_bytes"]),
    )
    cache_disk_pressure_ratio = _safe_ratio(
        _as_int(governance["vfs_runtime_chunk_cache_disk_bytes"]),
        _as_int(governance["vfs_runtime_chunk_cache_disk_max_bytes"]),
    )
    if _as_int(governance["vfs_runtime_chunk_cache_disk_write_errors"]) > 0:
        cache_pressure_reasons.append("disk_write_errors")
    if max(cache_memory_pressure_ratio, cache_disk_pressure_ratio) >= 0.85:
        cache_pressure_reasons.append("cache_capacity_high")
    if _as_int(governance["vfs_runtime_chunk_cache_disk_evictions"]) > 0:
        cache_pressure_reasons.append("disk_evictions_observed")
    governance["vfs_runtime_cache_pressure_class"] = _pressure_class(
        critical=(
            _as_int(governance["vfs_runtime_chunk_cache_disk_write_errors"]) > 0
            or max(cache_memory_pressure_ratio, cache_disk_pressure_ratio) >= 0.95
        ),
        warning=bool(cache_pressure_reasons),
    )
    governance["vfs_runtime_cache_pressure_reasons"] = cache_pressure_reasons

    chunk_pressure_reasons: list[str] = []
    if _as_int(governance["vfs_runtime_chunk_coalescing_waits_miss"]) > 0:
        chunk_pressure_reasons.append("coalescing_wait_misses")
    if _as_float(governance["vfs_runtime_chunk_coalescing_wait_average_duration_ms"]) >= 10.0:
        chunk_pressure_reasons.append("coalescing_wait_latency_high")
    if _as_float(governance["vfs_runtime_chunk_coalescing_wait_max_duration_ms"]) >= 75.0:
        chunk_pressure_reasons.append("coalescing_wait_spike")
    governance["vfs_runtime_chunk_coalescing_pressure_class"] = _pressure_class(
        critical=(
            _as_int(governance["vfs_runtime_chunk_coalescing_waits_miss"]) >= 5
            or _as_float(governance["vfs_runtime_chunk_coalescing_wait_max_duration_ms"]) >= 250.0
        ),
        warning=bool(chunk_pressure_reasons),
    )
    governance["vfs_runtime_chunk_coalescing_pressure_reasons"] = chunk_pressure_reasons

    upstream_wait_reasons: list[str] = []
    if _as_int(governance["vfs_runtime_provider_pressure_incidents"]) > 0:
        upstream_wait_reasons.append("provider_pressure_incidents")
    if _as_int(governance["vfs_runtime_upstream_retryable_network"]) > 0:
        upstream_wait_reasons.append("retryable_network_wait")
    if _as_int(governance["vfs_runtime_upstream_retryable_read_body"]) > 0:
        upstream_wait_reasons.append("retryable_read_body_wait")
    if _as_int(governance["vfs_runtime_upstream_fetch_average_duration_ms"]) >= 50:
        upstream_wait_reasons.append("average_fetch_latency_high")
    if _as_int(governance["vfs_runtime_upstream_fetch_max_duration_ms"]) >= 250:
        upstream_wait_reasons.append("max_fetch_latency_high")
    governance["vfs_runtime_upstream_wait_class"] = _pressure_class(
        critical=(
            _as_int(governance["vfs_runtime_provider_pressure_incidents"]) >= 10
            or _as_int(governance["vfs_runtime_upstream_fetch_average_duration_ms"]) >= 100
            or _as_int(governance["vfs_runtime_upstream_fetch_max_duration_ms"]) >= 500
        ),
        warning=bool(upstream_wait_reasons),
    )
    governance["vfs_runtime_upstream_wait_reasons"] = upstream_wait_reasons

    refresh_pressure_reasons: list[str] = []
    if _as_int(governance["vfs_runtime_backend_fallback_failure"]) > 0:
        refresh_pressure_reasons.append("backend_fallback_failures")
    if _as_int(governance["vfs_runtime_inline_refresh_error"]) > 0:
        refresh_pressure_reasons.append("inline_refresh_errors")
    if _as_int(governance["vfs_runtime_inline_refresh_timeout"]) > 0:
        refresh_pressure_reasons.append("inline_refresh_timeouts")
    if _as_int(governance["vfs_runtime_backend_fallback_attempts"]) > 0:
        refresh_pressure_reasons.append("backend_fallback_activity")
    governance["vfs_runtime_refresh_pressure_class"] = _pressure_class(
        critical=(
            _as_int(governance["vfs_runtime_backend_fallback_failure"]) > 0
            or _as_int(governance["vfs_runtime_inline_refresh_timeout"]) >= 3
        ),
        warning=bool(refresh_pressure_reasons),
    )
    governance["vfs_runtime_refresh_pressure_reasons"] = refresh_pressure_reasons
    rollout_reasons: list[str] = []
    if _as_int(governance["vfs_runtime_backend_fallback_failure"]) > 0:
        rollout_reasons.append("backend_fallback_failures")
    if _as_int(governance["vfs_runtime_mounted_reads_error"]) > 0:
        rollout_reasons.append("mounted_read_errors")
    if _as_int(governance["vfs_runtime_prefetch_background_error"]) > 0:
        rollout_reasons.append("prefetch_background_errors")
    if _as_int(governance["vfs_runtime_chunk_cache_disk_write_errors"]) > 0:
        rollout_reasons.append("disk_cache_write_errors")
    if rollout_reasons:
        governance["vfs_runtime_rollout_readiness"] = "blocked"
        governance["vfs_runtime_rollout_next_action"] = "resolve_blocking_runtime_failures"
    else:
        if _as_int(governance["vfs_runtime_provider_pressure_incidents"]) > 0:
            rollout_reasons.append("provider_pressure_incidents")
        if _as_int(governance["vfs_runtime_fairness_pressure_incidents"]) > 0:
            rollout_reasons.append("fairness_pressure_incidents")
        if _as_int(governance["vfs_runtime_inline_refresh_error"]) > 0:
            rollout_reasons.append("inline_refresh_errors")
        if _as_int(governance["vfs_runtime_chunk_coalescing_waits_miss"]) > 0:
            rollout_reasons.append("chunk_coalescing_misses")
    if governance["vfs_runtime_rollout_readiness"] != "blocked" and rollout_reasons:
        governance["vfs_runtime_rollout_readiness"] = "warning"
        governance["vfs_runtime_rollout_next_action"] = "repeat_soak_and_tune_thresholds"
    elif governance["vfs_runtime_rollout_readiness"] != "blocked":
        governance["vfs_runtime_rollout_readiness"] = "ready"
        governance["vfs_runtime_rollout_next_action"] = "promote_to_next_environment_class"
        rollout_reasons.append("no_blocking_runtime_signals")
    governance["vfs_runtime_rollout_reasons"] = rollout_reasons
    (
        tenant_safe_summaries,
        visible_handles,
        hidden_handles,
        visible_tenants,
    ) = _tenant_safe_runtime_handle_summaries(
        _nested_mapping_value(payload, "runtime", "active_handle_summaries"),
        request_tenant_id=request_tenant_id,
        authorized_tenant_ids=authorized_tenant_ids,
    )
    governance["vfs_runtime_active_handle_summaries"] = tenant_safe_summaries
    governance["vfs_runtime_active_handles_visible"] = visible_handles
    governance["vfs_runtime_active_handles_hidden"] = hidden_handles
    governance["vfs_runtime_active_handle_tenant_count"] = len(visible_tenants)
    governance["vfs_runtime_active_handle_tenants"] = visible_tenants
    return _apply_vfs_rollout_policy(
        governance,
        playback_gate_governance=playback_gate_governance,
    )

def playback_gate_governance_snapshot() -> dict[str, int | str | list[str]]:
    """Return machine-shaped playback-gate promotion posture from local artifacts."""

    return _playback_gate_governance_snapshot()


def empty_playback_gate_governance_snapshot() -> dict[str, int | str | list[str]]:
    """Return the default playback-gate promotion snapshot."""

    return _empty_playback_gate_governance_snapshot()


def vfs_runtime_governance_snapshot(
    playback_gate_governance: dict[str, int | str | list[str]] | None = None,
    *,
    request_tenant_id: str | None = None,
    authorized_tenant_ids: set[str] | None = None,
) -> dict[str, int | float | str | list[str]]:
    """Return additive runtime governance counters from the Rust status payload."""

    return _vfs_runtime_governance_snapshot(
        playback_gate_governance=playback_gate_governance,
        request_tenant_id=request_tenant_id,
        authorized_tenant_ids=authorized_tenant_ids,
    )


def runtime_pressure_requires_queued_dispatch(
    governance: dict[str, int | float | str | list[str]],
) -> tuple[bool, bool]:
    """Return queued-dispatch recommendation and latency-SLO breach flag."""

    return _runtime_pressure_requires_queued_dispatch(governance)


def as_int(value: object) -> int:
    """Public integer coercion helper for stream-route policy evaluation."""

    return _as_int(value)


def as_str(value: object, *, default: str = "") -> str:
    """Public string coercion helper for stream-route policy evaluation."""

    return _as_str(value, default=default)

