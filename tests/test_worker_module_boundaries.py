from __future__ import annotations

import ast
from pathlib import Path

from filmu_py.workers import stage_isolation, stage_job_ids, stage_observability, stage_scope


def _project_file(*parts: str) -> Path:
    return Path(__file__).resolve().parents[1].joinpath(*parts)


def test_worker_tasks_imports_stage_modules() -> None:
    source = _project_file("filmu_py", "workers", "tasks.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    import_modules: set[str] = set()
    from_import_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            import_modules.update(alias.name for alias in node.names)
            continue
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            from_import_modules.add(node.module)

    assert "filmu_py.workers.stage_observability" in import_modules or (
        "filmu_py.workers" in from_import_modules and "stage_observability" in source
    )
    assert "filmu_py.workers.stage_job_ids" in import_modules or (
        "filmu_py.workers" in from_import_modules and "stage_job_ids" in source
    )
    assert "filmu_py.workers.stage_isolation" in import_modules or (
        "filmu_py.workers" in from_import_modules and "stage_isolation" in source
    )
    assert "filmu_py.workers.stage_scope" in import_modules or (
        "filmu_py.workers" in from_import_modules and "stage_scope" in source
    )

    # Keep compatibility: worker tasks should re-export these symbols from the shared module.
    assert "WORKER_ENQUEUE_DECISIONS_TOTAL =" in source
    assert "WORKER_JOB_STATUS_TOTAL =" in source
    assert "WORKER_CLEANUP_TOTAL =" in source
    assert "WORKER_STAGE_IDEMPOTENCY_TOTAL =" in source
    assert "WORKER_ENQUEUE_DEFER_SECONDS =" in source
    assert "_record_enqueue_decision =" in source
    assert "_record_job_status =" in source
    assert "_record_cleanup_action =" in source
    assert "_record_stage_idempotency =" in source
    assert "_record_enqueue_defer =" in source
    assert "worker_stage_idempotency_key =" in source
    assert "index_item_job_id =" in source
    assert "parse_scrape_results_job_id =" in source
    assert "process_scraped_item_job_id =" in source
    assert "rank_streams_job_id =" in source
    assert "scrape_item_job_id =" in source
    assert "debrid_item_job_id =" in source
    assert "finalize_item_job_id =" in source
    assert "refresh_direct_playback_link_job_id =" in source
    assert "refresh_selected_hls_failed_lease_job_id =" in source
    assert "refresh_selected_hls_restricted_fallback_job_id =" in source
    assert "_post_rank_expected_scope_reason = _stage_scope.post_rank_expected_scope_reason" in source
    assert "_build_scraper_search_input = _stage_scope.build_scraper_search_input" in source


def test_worker_stage_observability_module_exports_contract() -> None:
    assert stage_observability.WORKER_ENQUEUE_DECISIONS_TOTAL is not None
    assert stage_observability.WORKER_JOB_STATUS_TOTAL is not None
    assert stage_observability.WORKER_CLEANUP_TOTAL is not None
    assert stage_observability.WORKER_STAGE_IDEMPOTENCY_TOTAL is not None
    assert stage_observability.WORKER_ENQUEUE_DEFER_SECONDS is not None
    assert callable(stage_observability.job_status_name)
    assert callable(stage_observability.record_enqueue_decision)
    assert callable(stage_observability.record_job_status)
    assert callable(stage_observability.record_cleanup_action)
    assert callable(stage_observability.record_stage_idempotency)
    assert callable(stage_observability.record_enqueue_defer)


def test_worker_stage_job_ids_module_exports_contract() -> None:
    assert stage_job_ids.worker_stage_idempotency_key("rank", "item-1") == "rank:item-1"
    assert (
        stage_job_ids.worker_stage_idempotency_key("rank", "item-1", discriminator="a")
        == "rank:item-1:a"
    )
    assert stage_job_ids.index_item_job_id("item-1") == "index-item:item-1"
    assert stage_job_ids.parse_scrape_results_job_id("item-1") == "parse-scrape-results:item-1"
    assert stage_job_ids.process_scraped_item_job_id("item-1") == "parse-scrape-results:item-1"
    assert stage_job_ids.rank_streams_job_id("item-1") == "rank-streams:item-1"
    assert stage_job_ids.scrape_item_job_id("item-1") == "scrape-item:item-1"
    assert stage_job_ids.debrid_item_job_id("item-1") == "debrid-item:item-1"
    assert stage_job_ids.finalize_item_job_id("item-1") == "finalize-item:item-1"
    assert (
        stage_job_ids.refresh_direct_playback_link_job_id("item-1")
        == "refresh-direct-playback:item-1"
    )
    assert (
        stage_job_ids.refresh_selected_hls_failed_lease_job_id("item-1")
        == "refresh-selected-hls-failed-lease:item-1"
    )
    assert (
        stage_job_ids.refresh_selected_hls_restricted_fallback_job_id("item-1")
        == "refresh-selected-hls-restricted-fallback:item-1"
    )


def test_worker_stage_isolation_module_exports_contract() -> None:
    assert callable(stage_isolation.heavy_stage_executor)
    assert callable(stage_isolation.heavy_stage_timeout_seconds)
    assert callable(stage_isolation.rank_stream_batch)
    assert callable(stage_isolation.coerce_rank_batch_parsed_title)
    assert callable(stage_isolation.shutdown_heavy_stage_executors)
    assert stage_isolation.coerce_rank_batch_parsed_title("{'a': 1}") == {"a": 1}
    assert stage_isolation.coerce_rank_batch_parsed_title("[]") == {}


def test_worker_stage_scope_module_exports_contract() -> None:
    assert stage_scope.normalize_requested_seasons([3, 1, 3, -1]) == [1, 3]
    assert stage_scope.normalize_requested_episode_scope({"2": [4, 2, 4]}) == {"2": [2, 4]}
    assert callable(stage_scope.post_rank_expected_scope_reason)
    assert callable(stage_scope.build_scraper_search_input)
