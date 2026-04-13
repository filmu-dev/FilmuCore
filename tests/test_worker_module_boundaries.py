from __future__ import annotations

import ast
from pathlib import Path

from filmu_py.workers import stage_observability


def _project_file(*parts: str) -> Path:
    return Path(__file__).resolve().parents[1].joinpath(*parts)


def test_worker_tasks_imports_stage_observability_module() -> None:
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
