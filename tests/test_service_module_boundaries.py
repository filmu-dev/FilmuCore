from __future__ import annotations

import ast
import importlib
import sys
from datetime import date
from pathlib import Path
from typing import Any, cast

from filmu_py.services import (
    media_path_inference,
    media_show_completion,
    media_stream_candidates,
    playback_refresh_controllers,
    playback_refresh_dispatch,
)


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
    assert (
        "_infer_season_number_from_path = _media_path_inference.infer_season_number_from_path"
        not in source
    )
    assert "_infer_season_range_from_path = _media_path_inference.infer_season_range_from_path" in source


def test_media_service_imports_show_completion_boundary_module() -> None:
    media_path = _project_file("filmu_py", "services", "media.py")
    source = media_path.read_text(encoding="utf-8")
    _assert_file_imports_module(
        media_path,
        "filmu_py.services.media_show_completion",
        "_media_show_completion",
    )
    assert "ShowCompletionResult = _media_show_completion.ShowCompletionResult" in source
    assert "def _extract_tmdb_episode_inventory(" in source
    assert "async def _evaluate_show_completion(" in source


def test_media_service_imports_stream_candidate_boundary_module() -> None:
    media_path = _project_file("filmu_py", "services", "media.py")
    source = media_path.read_text(encoding="utf-8")
    _assert_file_imports_module(
        media_path,
        "filmu_py.services.media_stream_candidates",
        "_media_stream_candidates",
    )
    assert "ParsedStreamCandidateRecord = _media_stream_candidates.ParsedStreamCandidateRecord" in source
    assert "rank_persisted_streams_for_item = _media_stream_candidates.rank_persisted_streams_for_item" in source


def test_playback_service_imports_refresh_dispatch_boundary_module() -> None:
    playback_path = _project_file("filmu_py", "services", "playback.py")
    source = playback_path.read_text(encoding="utf-8")
    _assert_file_imports_module(
        playback_path,
        "filmu_py.services.playback_refresh_dispatch",
        "resolve_refresh_controller",
    )
    assert source.count("resolve_refresh_controller(") >= 3


def test_playback_service_imports_refresh_controller_boundary_module() -> None:
    playback_path = _project_file("filmu_py", "services", "playback.py")
    source = playback_path.read_text(encoding="utf-8")
    assert "filmu_py.services.playback_refresh_controllers" in source
    assert "QueuedDirectPlaybackRefreshController =" in source
    assert "InProcessDirectPlaybackRefreshController =" in source


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
            cast(Any, resources),
            prefer_queued=None,
            in_process_attr="playback_refresh_controller",
            queued_attr="queued_direct_playback_refresh_controller",
        )
        == "queued"
    )
    assert (
        playback_refresh_dispatch.resolve_refresh_controller(
            cast(Any, resources),
            prefer_queued=False,
            in_process_attr="hls_failed_lease_refresh_controller",
            queued_attr="queued_hls_failed_lease_refresh_controller",
        )
        == "queued-hls-failed"
    )


def test_media_show_completion_module_exports_contract() -> None:
    assert media_show_completion.extract_tmdb_episode_inventory({}, today=date.today()) == {}
    assert callable(media_show_completion.evaluate_show_completion)
    assert media_show_completion.ShowCompletionResult(
        all_satisfied=False,
        any_satisfied=False,
        has_future_episodes=False,
        missing_released=[],
    ).all_satisfied is False


def test_media_stream_candidates_module_exports_contract() -> None:
    parsed = media_stream_candidates.parse_stream_candidate_title(
        "Example.Show.S02E05.1080p.WEB-DL.x265-GROUP.mkv"
    )
    assert parsed.resolution == "1080p"
    assert callable(media_stream_candidates.validate_parsed_stream_candidate)
    assert media_stream_candidates.RankingModel().remove_ranks_under == -10000


def test_playback_refresh_controllers_module_exports_contract() -> None:
    assert hasattr(playback_refresh_controllers, "QueuedDirectPlaybackRefreshController")
    assert hasattr(playback_refresh_controllers, "InProcessDirectPlaybackRefreshController")


def test_plugin_manifest_import_does_not_eagerly_load_graphql_schema() -> None:
    modules_to_clear = [
        "filmu_py.graphql",
        "filmu_py.graphql.schema",
        "filmu_py.graphql.resolvers",
        "filmu_py.plugins.manifest",
    ]
    for module_name in modules_to_clear:
        sys.modules.pop(module_name, None)

    manifest_module = importlib.import_module("filmu_py.plugins.manifest")

    assert manifest_module.PluginManifest.__name__ == "PluginManifest"
    assert "filmu_py.graphql.schema" not in sys.modules
    assert "filmu_py.graphql.resolvers" not in sys.modules


def test_worker_tasks_import_contract() -> None:
    sys.modules.pop("filmu_py.workers.tasks", None)
    tasks_module = importlib.import_module("filmu_py.workers.tasks")
    assert hasattr(tasks_module, "run_worker_entrypoint")


def test_large_file_decomposition_size_budget_contract() -> None:
    budgets = {
        _project_file("filmu_py", "services", "media.py"): 5300,
        _project_file("filmu_py", "services", "playback.py"): 4900,
        _project_file("filmu_py", "workers", "tasks.py"): 3850,
        _project_file("filmu_py", "api", "routes", "stream.py"): 1600,
    }
    for file_path, max_lines in budgets.items():
        line_count = sum(1 for _ in file_path.open(encoding="utf-8"))
        assert line_count <= max_lines, f"{file_path.name} grew to {line_count} lines > {max_lines}"
