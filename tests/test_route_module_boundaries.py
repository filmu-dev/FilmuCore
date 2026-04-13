from __future__ import annotations

import ast
from pathlib import Path

from filmu_py.api.routes import runtime_governance


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
