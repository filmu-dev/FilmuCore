from __future__ import annotations

import ast
from pathlib import Path

from filmu_py.api.routes import runtime_governance, runtime_hls_governance


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
