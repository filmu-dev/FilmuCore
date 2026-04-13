from __future__ import annotations

import ast
from pathlib import Path

from filmu_py.services import media_path_inference, playback_refresh_dispatch


def _project_file(*parts: str) -> Path:
    return Path(__file__).resolve().parents[1].joinpath(*parts)


def _assert_file_imports_module(path: Path, module_name: str, symbol_hint: str) -> None:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules: set[str] = set()
    imported_symbols: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
            continue
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.module is None:
            continue
        imported_modules.add(node.module)
        imported_symbols.update(alias.name for alias in node.names)

    assert module_name in imported_modules or symbol_hint in imported_symbols


def test_media_service_imports_path_inference_boundary_module() -> None:
    media_path = _project_file("filmu_py", "services", "media.py")
    source = media_path.read_text(encoding="utf-8")
    _assert_file_imports_module(
        media_path,
        "filmu_py.services.media_path_inference",
        "_media_path_inference",
    )
    assert "_infer_season_number_from_path = _media_path_inference.infer_season_number_from_path" in source
    assert "_infer_season_range_from_path = _media_path_inference.infer_season_range_from_path" in source
    assert "_infer_episode_number_from_path = _media_path_inference.infer_episode_number_from_path" in source


def test_playback_service_imports_refresh_dispatch_boundary_module() -> None:
    playback_path = _project_file("filmu_py", "services", "playback.py")
    source = playback_path.read_text(encoding="utf-8")
    _assert_file_imports_module(
        playback_path,
        "filmu_py.services.playback_refresh_dispatch",
        "resolve_refresh_controller",
    )
    assert source.count("resolve_refresh_controller(") >= 3


def test_media_path_inference_module_exports_contract() -> None:
    assert media_path_inference.infer_season_number_from_path("Season 03") == 3
    assert media_path_inference.infer_season_range_from_path("Show S01-S03 Pack") == [1, 2, 3]
    assert media_path_inference.infer_episode_number_from_path("Show S02E11") == 11


def test_playback_refresh_dispatch_module_exports_contract() -> None:
    class _Settings:
        class stream:
            refresh_dispatch_mode = "queued"

    class _Resources:
        settings = _Settings()
        playback_refresh_controller = "in-process"
        queued_direct_playback_refresh_controller = "queued"
        hls_failed_lease_refresh_controller = None
        queued_hls_failed_lease_refresh_controller = "queued-hls-failed"

    resources = _Resources()
    assert (
        playback_refresh_dispatch.resolve_refresh_controller(
            resources,
            prefer_queued=None,
            in_process_attr="playback_refresh_controller",
            queued_attr="queued_direct_playback_refresh_controller",
        )
        == "queued"
    )
    assert (
        playback_refresh_dispatch.resolve_refresh_controller(
            resources,
            prefer_queued=False,
            in_process_attr="hls_failed_lease_refresh_controller",
            queued_attr="queued_hls_failed_lease_refresh_controller",
        )
        == "queued-hls-failed"
    )


def test_large_file_decomposition_size_budget_contract() -> None:
    budgets = {
        _project_file("filmu_py", "services", "media.py"): 6280,
        _project_file("filmu_py", "services", "playback.py"): 5405,
        _project_file("filmu_py", "workers", "tasks.py"): 4145,
        _project_file("filmu_py", "api", "routes", "stream.py"): 1685,
    }
    for file_path, max_lines in budgets.items():
        line_count = sum(1 for _ in file_path.open(encoding="utf-8"))
        assert line_count <= max_lines, f"{file_path.name} grew to {line_count} lines > {max_lines}"
