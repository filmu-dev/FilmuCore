"""Heavy-stage isolation helpers for ARQ worker task execution."""

from __future__ import annotations

import ast
import multiprocessing
import os
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from typing import TypedDict

from filmu_py.config import get_settings
from filmu_py.rtn import RTN, ParsedData, RankedTorrent, RankingProfile

_HEAVY_STAGE_EXECUTORS: dict[tuple[str, str, int, int, int, int], Executor] = {}


class RankBatchInput(TypedDict):
    """Serializable input payload for isolated ranking work."""

    stream_id: str
    raw_title: str
    parsed_title: dict[str, object] | str
    resolution: str | None
    partial_scope_bonus: int


class RankBatchRecord(TypedDict):
    """Serializable output payload from isolated ranking work."""

    stream_id: str
    rank_score: int
    lev_ratio: float
    fetch: bool
    passed: bool
    rejection_reason: str | None


def heavy_stage_executor(stage_name: str) -> Executor:
    """Return the bounded executor used for one CPU-heavy worker stage."""

    settings = get_settings()
    policy = settings.orchestration.heavy_stage_isolation
    violations = policy.policy_violations()
    if violations:
        raise RuntimeError(
            "invalid heavy-stage isolation policy for "
            f"{stage_name}: {','.join(sorted(violations))}"
        )
    executor_key = (
        stage_name,
        policy.executor_mode,
        policy.max_workers,
        policy.max_tasks_per_child,
        int(policy.require_spawn_context),
        policy.max_worker_ceiling,
    )
    stale_keys = [key for key in _HEAVY_STAGE_EXECUTORS if key[1:] != executor_key[1:]]
    for stale_key in stale_keys:
        stale_executor = _HEAVY_STAGE_EXECUTORS.pop(stale_key)
        stale_executor.shutdown(wait=False, cancel_futures=True)
    executor = _HEAVY_STAGE_EXECUTORS.get(executor_key)
    if executor is not None:
        return executor
    if policy.executor_mode == "thread_pool_only" or (
        policy.executor_mode != "process_pool_required" and "PYTEST_CURRENT_TEST" in os.environ
    ):
        executor = ThreadPoolExecutor(max_workers=policy.max_workers)
    else:
        max_tasks_per_child = policy.max_tasks_per_child if policy.max_tasks_per_child > 0 else None
        try:
            if policy.require_spawn_context:
                executor = ProcessPoolExecutor(
                    max_workers=policy.max_workers,
                    mp_context=multiprocessing.get_context("spawn"),
                    max_tasks_per_child=max_tasks_per_child,
                )
            elif multiprocessing.get_start_method(allow_none=True) == "fork":
                executor = ProcessPoolExecutor(
                    max_workers=policy.max_workers,
                    max_tasks_per_child=max_tasks_per_child,
                )
            else:
                executor = ProcessPoolExecutor(
                    max_workers=policy.max_workers,
                    mp_context=multiprocessing.get_context("spawn"),
                    max_tasks_per_child=max_tasks_per_child,
                )
        except (ValueError, RuntimeError):
            if policy.executor_mode == "process_pool_required":
                raise RuntimeError(
                    f"process-backed heavy-stage isolation is required for {stage_name}"
                ) from None
            executor = ThreadPoolExecutor(max_workers=policy.max_workers)
    _HEAVY_STAGE_EXECUTORS[executor_key] = executor
    return executor


def heavy_stage_timeout_seconds(stage_name: str) -> float:
    """Return the configured timeout budget for one isolated heavy stage."""

    policy = get_settings().orchestration.heavy_stage_isolation
    if stage_name == "index_item":
        return policy.index_timeout_seconds
    if stage_name == "parse_scrape_results":
        return policy.parse_timeout_seconds
    if stage_name == "rank_streams":
        return policy.rank_timeout_seconds
    return max(policy.parse_timeout_seconds, 30.0)


def rank_stream_batch(
    *,
    item_title: str,
    item_aliases: list[str],
    profile: RankingProfile,
    bucket_limit: int | None,
    stream_inputs: list[RankBatchInput],
) -> list[RankBatchRecord]:
    """Run the expensive RTN ranking/sorting batch in an isolated worker."""

    rtn = RTN(profile)
    successful: list[tuple[str, RankedTorrent]] = []
    failures: list[RankBatchRecord] = []

    for stream_input in stream_inputs:
        stream_id = stream_input["stream_id"]
        parsed = ParsedData(
            raw_title=stream_input["raw_title"],
            parsed_title=coerce_rank_batch_parsed_title(stream_input["parsed_title"]),
            resolution=stream_input["resolution"],
        )
        try:
            ranked = rtn.rank_torrent(
                parsed,
                correct_title=item_title,
                aliases=item_aliases or None,
            )
            partial_scope_bonus = stream_input["partial_scope_bonus"]
            if partial_scope_bonus > 0:
                score_parts = dict(ranked.score_parts)
                score_parts["partial_scope_bonus"] = partial_scope_bonus
                ranked = RankedTorrent(
                    data=ranked.data,
                    rank=ranked.rank + partial_scope_bonus,
                    lev_ratio=ranked.lev_ratio,
                    fetch=ranked.fetch,
                    failed_checks=ranked.failed_checks,
                    score_parts=score_parts,
                )
        except Exception as exc:  # pragma: no cover - subprocess defensive path
            failures.append(
                {
                    "stream_id": stream_id,
                    "rank_score": 0,
                    "lev_ratio": 0.0,
                    "fetch": False,
                    "passed": False,
                    "rejection_reason": str(exc),
                }
            )
            continue
        successful.append((stream_id, ranked))

    sorted_ranked = rtn.sort_torrents(
        [ranked for _, ranked in successful],
        bucket_limit=bucket_limit,
    )
    kept_ids = {id(ranked.data) for ranked in sorted_ranked}
    results = list(failures)
    for stream_id, ranked in successful:
        if id(ranked.data) not in kept_ids:
            results.append(
                {
                    "stream_id": stream_id,
                    "rank_score": 0,
                    "lev_ratio": ranked.lev_ratio,
                    "fetch": False,
                    "passed": False,
                    "rejection_reason": "bucket_limit_exceeded",
                }
            )
            continue
        results.append(
            {
                "stream_id": stream_id,
                "rank_score": ranked.rank,
                "lev_ratio": ranked.lev_ratio,
                "fetch": ranked.fetch,
                "passed": ranked.fetch,
                "rejection_reason": None if ranked.fetch else ",".join(ranked.failed_checks) or "fetch_failed",
            }
        )
    return results


def coerce_rank_batch_parsed_title(raw: dict[str, object] | str) -> dict[str, object]:
    """Normalize serialized parsed-title payloads for the isolated rank worker."""

    if isinstance(raw, dict):
        return {key: value for key, value in raw.items() if isinstance(key, str)}
    try:
        parsed = ast.literal_eval(raw)
    except (SyntaxError, ValueError):
        return {}
    if isinstance(parsed, dict):
        return {key: value for key, value in parsed.items() if isinstance(key, str)}
    return {}


def shutdown_heavy_stage_executors() -> None:
    """Drain and clear all stage-isolation executors."""

    for executor in _HEAVY_STAGE_EXECUTORS.values():
        executor.shutdown(wait=False, cancel_futures=True)
    _HEAVY_STAGE_EXECUTORS.clear()
