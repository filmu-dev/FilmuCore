from __future__ import annotations

import ast
from pathlib import Path

from filmu_py.services import playback_deferral_governance


def _project_file(*parts: str) -> Path:
    return Path(__file__).resolve().parents[1].joinpath(*parts)


def test_playback_service_imports_deferral_governance_module() -> None:
    source = _project_file("filmu_py", "services", "playback.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.module != "filmu_py.services.playback_deferral_governance":
            continue
        imported.update(alias.name for alias in node.names)

    assert "playback_refresh_deferral_governance_snapshot" in imported
    assert "record_direct_playback_refresh_deferral" in imported
    assert "record_selected_hls_refresh_deferral" in imported


def test_playback_deferral_governance_module_exports_snapshot_contract() -> None:
    snapshot = playback_deferral_governance.playback_refresh_deferral_governance_snapshot()

    assert callable(playback_deferral_governance.playback_refresh_deferral_governance_snapshot)
    assert callable(playback_deferral_governance.record_selected_hls_refresh_deferral)
    assert callable(playback_deferral_governance.record_direct_playback_refresh_deferral)
    assert set(snapshot) == {
        "direct_playback_refresh_rate_limited",
        "direct_playback_refresh_provider_circuit_open",
        "hls_failed_lease_refresh_rate_limited",
        "hls_failed_lease_refresh_provider_circuit_open",
        "hls_restricted_fallback_refresh_rate_limited",
        "hls_restricted_fallback_refresh_provider_circuit_open",
    }
