from __future__ import annotations

import ast
from pathlib import Path

from filmu_py.api.routes import (
    runtime_governance,
    runtime_hls_governance,
    runtime_refresh_governance,
    runtime_status_payload,
    stream_direct_serving,
)


def _project_file(*parts: str) -> Path:
    return Path(__file__).resolve().parents[1].joinpath(*parts)


def test_default_route_does_not_import_stream_internals() -> None:
    source = _project_file("filmu_py", "api", "routes", "default.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level != 1:
            continue
        if node.module != "stream":
            continue
        imported = {alias.name for alias in node.names}
        assert "_playback_gate_governance_snapshot" not in imported
        assert "_vfs_runtime_governance_snapshot" not in imported


def test_runtime_governance_module_exports_snapshots() -> None:
    assert callable(runtime_governance.playback_gate_governance_snapshot)
    assert callable(runtime_governance.vfs_runtime_governance_snapshot)
    assert callable(runtime_governance.runtime_pressure_requires_queued_dispatch)


def test_stream_route_imports_hls_runtime_governance_module() -> None:
    source = _project_file("filmu_py", "api", "routes", "stream.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level != 1:
            continue
        if node.module != "runtime_hls_governance":
            continue
        imported.update(alias.name for alias in node.names)

    assert "record_hls_route_failure" in imported
    assert "remote_hls_recovery_governance_snapshot" in imported
    assert "run_remote_hls_with_retry" in imported
    assert "validate_upstream_hls_playlist" in imported


def test_runtime_hls_governance_module_exports_contract() -> None:
    assert callable(runtime_hls_governance.record_hls_route_failure)
    assert callable(runtime_hls_governance.classify_hls_route_failure_reason)
    assert callable(runtime_hls_governance.hls_route_failure_governance_snapshot)
    assert callable(runtime_hls_governance.remote_hls_recovery_governance_snapshot)
    assert callable(runtime_hls_governance.record_inline_remote_hls_refresh)
    assert callable(runtime_hls_governance.run_remote_hls_with_retry)
    assert callable(runtime_hls_governance.validate_upstream_hls_playlist)


def test_stream_route_imports_refresh_runtime_governance_module() -> None:
    source = _project_file("filmu_py", "api", "routes", "stream.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported: set[str] = set()
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
            continue
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level == 1 and node.module is None:
            imported_modules.update(alias.name for alias in node.names)
            continue
        if node.level != 1:
            continue
        if node.module != "runtime_refresh_governance":
            continue
        imported.update(alias.name for alias in node.names)

    assert "runtime_refresh_governance" in imported_modules or (
        "DIRECT_PLAYBACK_TRIGGER_GOVERNANCE" in imported
    )
    assert "runtime_refresh_governance" in imported_modules or (
        "STREAM_REFRESH_POLICY_GOVERNANCE" in imported
    )
    assert "record_route_refresh_trigger_pending" in imported
    assert "select_refresh_dispatch_preference" in imported


def test_runtime_refresh_governance_module_exports_contract() -> None:
    assert callable(runtime_refresh_governance.direct_playback_trigger_governance_snapshot)
    assert callable(runtime_refresh_governance.hls_failed_lease_trigger_governance_snapshot)
    assert callable(runtime_refresh_governance.hls_restricted_fallback_trigger_governance_snapshot)
    assert callable(runtime_refresh_governance.stream_refresh_policy_governance_snapshot)
    assert callable(runtime_refresh_governance.record_route_refresh_trigger_pending)
    assert callable(runtime_refresh_governance.select_refresh_dispatch_preference)


def test_stream_route_imports_runtime_status_payload_module() -> None:
    source = _project_file("filmu_py", "api", "routes", "stream.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported: set[str] = set()
    imported_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
            continue
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level == 1 and node.module is None:
            imported_modules.update(alias.name for alias in node.names)
            continue
        if node.level != 1:
            continue
        if node.module != "runtime_status_payload":
            continue
        imported.update(alias.name for alias in node.names)

    assert "runtime_status_payload" in imported_modules or (
        "build_serving_status_response" in imported
    )


def test_stream_route_imports_direct_serving_boundary_module() -> None:
    source = _project_file("filmu_py", "api", "routes", "stream.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level != 1 or node.module != "stream_direct_serving":
            continue
        imported.update(alias.name for alias in node.names)

    assert "resolve_direct_file_serving_descriptor" in imported
    assert "resolve_playback_service" in imported
    assert "head_remote_direct_url" in imported


def test_runtime_status_payload_module_exports_contract() -> None:
    assert callable(runtime_status_payload.build_serving_status_response)


def test_stream_direct_serving_module_exports_contract() -> None:
    assert callable(stream_direct_serving.resolve_direct_file_serving_descriptor)
    assert callable(stream_direct_serving.resolve_playback_service)
    assert callable(stream_direct_serving.head_remote_direct_url)
