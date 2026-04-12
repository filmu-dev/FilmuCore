"""ARQ worker tasks for scrape -> debrid -> finalize compatibility flow."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import functools
import json
import logging
import multiprocessing
import os
from collections.abc import Awaitable, Callable
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict, cast
from urllib.parse import parse_qs, urlparse
from uuid import UUID

import httpx
import structlog
from arq import Retry
from arq.connections import ArqRedis, RedisSettings, create_pool
from arq.cron import cron
from arq.jobs import Job, JobStatus
from arq.worker import Worker
from prometheus_client import Counter, Histogram
from redis.asyncio import Redis
from sqlalchemy import select

from filmu_py.config import Settings, TenantQuotaLimitSettings, get_settings, set_runtime_settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.db.models import MediaItemORM, StreamORM
from filmu_py.db.runtime import DatabaseRuntime
from filmu_py.plugins import ExternalIdentifiers, ScraperSearchInput
from filmu_py.plugins.builtins import register_builtin_plugins
from filmu_py.plugins.context import HostPluginDatasource, PluginContextProvider
from filmu_py.plugins.interfaces import ScraperResult as PluginScraperResult
from filmu_py.plugins.loader import PluginRuntimePolicy, load_plugins
from filmu_py.plugins.registry import PluginRegistry
from filmu_py.rtn import RTN, ParsedData, RankedTorrent, RankingProfile
from filmu_py.services.debrid import (
    AllDebridPlaybackClient,
    DebridDownloadClient,
    DebridLinkPlaybackClient,
    DebridRateLimitError,
    RealDebridPlaybackClient,
    filter_torrent_files,
)
from filmu_py.services.media import (
    MediaItemRecord,
    MediaService,
    RankedStreamCandidateRecord,
    RecoveryTargetStage,
    ScrapeCandidateRecord,
    SelectedStreamCandidateRecord,
    _build_recovery_plan_record,
    _evaluate_show_completion,
    _parse_calendar_datetime,
)
from filmu_py.services.media_server import MediaServerNotifier
from filmu_py.services.playback import (
    PlaybackSourceService,
)
from filmu_py.services.settings_service import load_settings
from filmu_py.state.item import InvalidItemTransition, ItemEvent, ItemState
from filmu_py.workers.retry import (
    RetryPolicy,
    bind_worker_contextvars,
    route_dead_letter,
    task_try_count,
    timed_stage,
)

logger = logging.getLogger(__name__)
INDEX_RETRY_POLICY = RetryPolicy(max_attempts=4, base_delay_seconds=2, max_delay_seconds=30)
SCRAPE_RETRY_POLICY = RetryPolicy(max_attempts=4, base_delay_seconds=2, max_delay_seconds=30)
PARSE_RESULTS_RETRY_POLICY = RetryPolicy(max_attempts=4, base_delay_seconds=2, max_delay_seconds=30)
RANK_STREAMS_RETRY_POLICY = RetryPolicy(max_attempts=4, base_delay_seconds=2, max_delay_seconds=30)
DEBRID_RETRY_POLICY = RetryPolicy(max_attempts=5, base_delay_seconds=3, max_delay_seconds=60)
FINALIZE_RETRY_POLICY = RetryPolicy(max_attempts=3, base_delay_seconds=2, max_delay_seconds=20)
RECOVERY_RETRY_POLICY = RetryPolicy(max_attempts=3, base_delay_seconds=30, max_delay_seconds=300)
OUTBOX_RETRY_POLICY = RetryPolicy(max_attempts=3, base_delay_seconds=5, max_delay_seconds=60)
WORKER_ENQUEUE_DECISIONS_TOTAL = Counter(
    "filmu_py_worker_enqueue_decisions_total",
    "Downstream worker enqueue decisions by stage",
    ["stage", "decision"],
)
WORKER_JOB_STATUS_TOTAL = Counter(
    "filmu_py_worker_job_status_total",
    "Observed ARQ job statuses while coordinating worker stages",
    ["stage", "status"],
)
WORKER_CLEANUP_TOTAL = Counter(
    "filmu_py_worker_cleanup_total",
    "Cleanup actions taken before replaying or deduplicating worker stages",
    ["stage", "action"],
)
WORKER_STAGE_IDEMPOTENCY_TOTAL = Counter(
    "filmu_py_worker_stage_idempotency_total",
    "Observed stage idempotency and replay outcomes by stage",
    ["stage", "outcome"],
)
WORKER_ENQUEUE_DEFER_SECONDS = Histogram(
    "filmu_py_worker_enqueue_defer_seconds",
    "Deferred worker enqueue delays in seconds",
    ["stage"],
    buckets=[1.0, 5.0, 15.0, 30.0, 60.0, 300.0, 900.0, 3600.0],
)
_HEAVY_STAGE_EXECUTORS: dict[tuple[str, str, int, int], Executor] = {}


class _RankBatchInput(TypedDict):
    """Serializable input payload for isolated ranking work."""

    stream_id: str
    raw_title: str
    parsed_title: dict[str, object] | str
    resolution: str | None
    partial_scope_bonus: int


class _RankBatchRecord(TypedDict):
    """Serializable output payload from isolated ranking work."""

    stream_id: str
    rank_score: int
    lev_ratio: float
    fetch: bool
    passed: bool
    rejection_reason: str | None


def _heavy_stage_executor(stage_name: str) -> Executor:
    """Return the bounded executor used for one CPU-heavy worker stage."""

    settings = get_settings()
    policy = settings.orchestration.heavy_stage_isolation
    executor_key = (
        stage_name,
        policy.executor_mode,
        policy.max_workers,
        policy.max_tasks_per_child,
    )
    stale_keys = [key for key in _HEAVY_STAGE_EXECUTORS if key[1:] != executor_key[1:]]
    for stale_key in stale_keys:
        stale_executor = _HEAVY_STAGE_EXECUTORS.pop(stale_key)
        stale_executor.shutdown(wait=False, cancel_futures=True)
    executor = _HEAVY_STAGE_EXECUTORS.get(executor_key)
    if executor is not None:
        return executor
    if policy.executor_mode == "thread_pool_only" or (
        policy.executor_mode != "process_pool_required"
        and "PYTEST_CURRENT_TEST" in os.environ
    ):
        executor = ThreadPoolExecutor(max_workers=policy.max_workers)
    else:
        max_tasks_per_child = (
            policy.max_tasks_per_child if policy.max_tasks_per_child > 0 else None
        )
        try:
            if multiprocessing.get_start_method(allow_none=True) == "fork":
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


def _heavy_stage_timeout_seconds(stage_name: str) -> float:
    """Return the configured timeout budget for one isolated heavy stage."""

    policy = get_settings().orchestration.heavy_stage_isolation
    if stage_name == "index_item":
        return policy.index_timeout_seconds
    if stage_name == "parse_scrape_results":
        return policy.parse_timeout_seconds
    if stage_name == "rank_streams":
        return policy.rank_timeout_seconds
    return max(policy.parse_timeout_seconds, 30.0)


def _rank_stream_batch(
    *,
    item_title: str,
    item_aliases: list[str],
    profile: RankingProfile,
    bucket_limit: int | None,
    stream_inputs: list[_RankBatchInput],
) -> list[_RankBatchRecord]:
    """Run the expensive RTN ranking/sorting batch in an isolated worker."""

    rtn = RTN(profile)
    successful: list[tuple[str, RankedTorrent]] = []
    failures: list[_RankBatchRecord] = []

    for stream_input in stream_inputs:
        stream_id = stream_input["stream_id"]
        parsed = ParsedData(
            raw_title=stream_input["raw_title"],
            parsed_title=_coerce_rank_batch_parsed_title(stream_input["parsed_title"]),
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
                "rejection_reason": None
                if ranked.fetch
                else ",".join(ranked.failed_checks) or "fetch_failed",
            }
        )
    return results


def _coerce_rank_batch_parsed_title(raw: dict[str, object] | str) -> dict[str, object]:
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


def _redis_from_settings(settings: Settings) -> Redis:
    """Build Redis client for worker-side limiter usage."""

    return cast(Redis, Redis.from_url(str(settings.redis_url), decode_responses=False))


async def _enqueue_arq_job(
    redis: ArqRedis,
    function: str,
    *args: object,
    **kwargs: object,
) -> object | None:
    """Preserve ARQ runtime keyword payloads while containing the local Any cast."""

    enqueued_job = await cast(Any, redis).enqueue_job(function, *args, **kwargs)
    return cast(object | None, enqueued_job)


async def _enforce_tenant_worker_enqueue_quota(
    redis: object,
    *,
    settings: Settings,
    tenant_id: str | None,
    stage_name: str,
) -> bool:
    """Enforce tenant-scoped worker enqueue pressure when quota policy enables it."""

    if tenant_id is None or not settings.tenant_quotas.enabled or not hasattr(redis, "incr"):
        return True

    limits: object = settings.tenant_quotas.tenants.get(
        tenant_id,
        settings.tenant_quotas.default,
    )
    if isinstance(limits, dict):
        raw_limit = limits.get("worker_enqueues_per_minute")
        try:
            if isinstance(raw_limit, (int, float)):
                limit = int(raw_limit)
            elif isinstance(raw_limit, str) and raw_limit.strip():
                limit = int(float(raw_limit))
            else:
                limit = None
        except (TypeError, ValueError, OverflowError):
            limit = None
    else:
        limit = cast(TenantQuotaLimitSettings, limits).worker_enqueues_per_minute
    if limit is None or limit <= 0:
        return True

    minute = int(datetime.now(UTC).timestamp() // 60)
    key = f"quota:tenant:{tenant_id}:worker_enqueue:{minute}"
    current = await cast(Any, redis).incr(key)
    if current == 1 and hasattr(redis, "expire"):
        await cast(Any, redis).expire(key, 120)
    if current > limit:
        _record_enqueue_decision(stage_name, "tenant_quota_denied")
        logger.warning(
            "tenant worker enqueue quota exceeded",
            extra={
                "tenant_id": tenant_id,
                "stage": stage_name,
                "policy_version": settings.tenant_quotas.version,
                "limit_per_minute": limit,
                "observed_count": current,
            },
        )
        return False
    return True


async def _acquire_worker_rate_limit(
    *,
    limiter: DistributedRateLimiter,
    bucket: str,
    capacity: float,
    refill_per_second: float,
) -> bool:
    """Acquire distributed budget or trigger ARQ retry with bounded backoff."""

    decision = await limiter.acquire(
        bucket_key=bucket,
        capacity=capacity,
        refill_rate_per_second=refill_per_second,
    )
    if decision.allowed:
        return False

    retry_seconds = max(1, int(decision.retry_after_seconds) + 1)
    raise Retry(defer=retry_seconds)


def _redis_settings(settings: Settings) -> RedisSettings:
    """Return ARQ Redis settings derived from app configuration."""

    return RedisSettings.from_dsn(str(settings.redis_url))


def _settings_from_worker_context(ctx: dict[str, Any]) -> Settings:
    """Resolve settings from worker context before falling back to process globals."""

    explicit = ctx.get("settings")
    if isinstance(explicit, Settings):
        return explicit
    return get_settings()


def _resolve_limiter(ctx: dict[str, Any]) -> DistributedRateLimiter:
    """Resolve a shared distributed limiter from worker context."""

    limiter = ctx.get("rate_limiter")
    if isinstance(limiter, DistributedRateLimiter):
        return limiter

    redis = ctx.get("redis")
    if not isinstance(redis, Redis):
        settings = _settings_from_worker_context(ctx)
        redis = _redis_from_settings(settings)
        ctx["redis"] = redis

    limiter = DistributedRateLimiter(redis=redis)
    ctx["rate_limiter"] = limiter
    return limiter


async def _resolve_arq_redis(ctx: dict[str, Any]) -> ArqRedis:
    """Resolve an ARQ Redis client from context or create one lazily."""

    redis = ctx.get("arq_redis")
    if isinstance(redis, ArqRedis):
        return redis

    settings = await _resolve_runtime_settings(ctx)
    queue_name = str(ctx.get("queue_name", _queue_name(settings)))
    resolved = await create_pool(_redis_settings(settings), default_queue_name=queue_name)
    ctx["arq_redis"] = resolved
    return resolved


async def _resolve_runtime_settings(ctx: dict[str, Any]) -> Settings:
    """Resolve the latest runtime settings, preferring persisted settings for worker jobs."""

    explicit = ctx.get("settings")
    if isinstance(explicit, Settings):
        payload = ctx.get("plugin_settings_payload")
        if not isinstance(payload, dict):
            ctx["plugin_settings_payload"] = explicit.to_compatibility_dict()
        return explicit

    current = get_settings()
    db = ctx.get("db")
    if not isinstance(db, DatabaseRuntime):
        db = DatabaseRuntime(current.postgres_dsn, echo=False)
        ctx["db"] = db

    persisted = await load_settings(db)
    if persisted is None:
        ctx["plugin_settings_payload"] = current.to_compatibility_dict()
        return current

    ctx["plugin_settings_payload"] = persisted
    resolved = Settings.from_compatibility_dict(persisted)
    set_runtime_settings(resolved)
    return resolved


async def _try_transition(
    *,
    media_service: MediaService,
    item_id: str,
    event: ItemEvent,
    message: str,
) -> None:
    """Apply transition while treating already-applied transitions as idempotent."""

    try:
        await media_service.transition_item(item_id=item_id, event=event, message=message)
    except InvalidItemTransition:
        return None


def worker_stage_idempotency_key(
    stage_name: str,
    item_id: str,
    *,
    discriminator: str | None = None,
) -> str:
    """Return a stable idempotency key for one stage/item combination."""

    if discriminator is None or discriminator == "":
        return f"{stage_name}:{item_id}"
    return f"{stage_name}:{item_id}:{discriminator}"


def index_item_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for index-stage processing."""

    return f"index-item:{item_id}"


def index_item_followup_job_id(
    item_id: str,
    *,
    discriminator: str | None = None,
    missing_seasons: list[int] | None = None,
) -> str:
    """Return a stable follow-up index job id for delayed polling or inventory rechecks."""

    suffix_parts: list[str] = ["followup"]
    if discriminator:
        suffix_parts.append(discriminator)
    if missing_seasons:
        normalized = "-".join(str(season) for season in sorted(set(missing_seasons)))
        suffix_parts.append(f"missing:{normalized}")
    return ":".join([index_item_job_id(item_id), *suffix_parts])


def parse_scrape_results_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for parse-scrape-results processing."""

    return f"parse-scrape-results:{item_id}"


def process_scraped_item_job_id(item_id: str) -> str:
    """Backward-compatible alias for the parse-scrape-results stage identifier."""

    return parse_scrape_results_job_id(item_id)


def rank_streams_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for rank-streams processing."""

    return f"rank-streams:{item_id}"


def scrape_item_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for scrape-stage processing."""

    return f"scrape-item:{item_id}"


def scrape_item_followup_job_id(item_id: str, *, missing_seasons: list[int] | None = None) -> str:
    """Return a stable follow-up scrape job id for partial/ongoing requeues."""

    if not missing_seasons:
        return scrape_item_job_id(item_id)
    normalized = "-".join(str(season) for season in sorted(set(missing_seasons)))
    return f"{scrape_item_job_id(item_id)}:missing:{normalized}"


def debrid_item_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for debrid-stage processing."""

    return f"debrid-item:{item_id}"


def finalize_item_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for finalize-stage processing."""

    return f"finalize-item:{item_id}"


def refresh_direct_playback_link_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for queued direct-play refresh work."""

    return f"refresh-direct-playback:{item_id}"


def refresh_selected_hls_failed_lease_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for queued failed-HLS refresh work."""

    return f"refresh-selected-hls-failed-lease:{item_id}"


def refresh_selected_hls_restricted_fallback_job_id(item_id: str) -> str:
    """Return a stable ARQ job identifier for queued restricted-fallback refresh work."""

    return f"refresh-selected-hls-restricted-fallback:{item_id}"


def _job_status_name(status: JobStatus) -> str:
    return getattr(status, "value", str(status))


def _worker_stage_logger() -> Any:
    return structlog.get_logger(__name__)


def _record_enqueue_decision(stage_name: str, decision: str) -> None:
    WORKER_ENQUEUE_DECISIONS_TOTAL.labels(stage=stage_name, decision=decision).inc()


def _record_job_status(stage_name: str, status: JobStatus) -> None:
    WORKER_JOB_STATUS_TOTAL.labels(stage=stage_name, status=_job_status_name(status)).inc()


def _record_cleanup_action(stage_name: str, action: str) -> None:
    WORKER_CLEANUP_TOTAL.labels(stage=stage_name, action=action).inc()


def _record_stage_idempotency(stage_name: str, outcome: str) -> None:
    WORKER_STAGE_IDEMPOTENCY_TOTAL.labels(stage=stage_name, outcome=outcome).inc()


async def _clear_stale_downstream_job(
    redis: object,
    *,
    item_id: str,
    stage_name: str,
    job_id: str,
) -> None:
    if not hasattr(redis, "delete"):
        return None

    result_key = f"arq:result:{job_id}"
    try:
        deleted = await cast(Any, redis).delete(result_key)
    except Exception as exc:
        _record_cleanup_action(stage_name, "stale_result_delete_failed")
        _worker_stage_logger().warning(
            "downstream stage stale result cleanup failed",
            item_id=item_id,
            next_stage=stage_name,
            job_id=job_id,
            result_key=result_key,
            error=str(exc),
        )
    else:
        if deleted:
            _record_cleanup_action(stage_name, "stale_result_deleted")
            _worker_stage_logger().warning(
                "downstream stage stale result cleared",
                item_id=item_id,
                next_stage=stage_name,
                job_id=job_id,
                result_key=result_key,
            )

    if not isinstance(redis, ArqRedis):
        return

    job = Job(job_id, redis=redis)
    try:
        status = await job.status()
    except Exception as exc:
        _record_cleanup_action(stage_name, "stale_job_status_failed")
        _worker_stage_logger().warning(
            "downstream stage stale job inspection failed",
            item_id=item_id,
            next_stage=stage_name,
            job_id=job_id,
            error=str(exc),
        )
        return
    _record_job_status(stage_name, status)

    if status not in {JobStatus.deferred, JobStatus.queued, JobStatus.in_progress}:
        return

    try:
        aborted = await job.abort(timeout=0)
    except Exception as exc:
        _record_cleanup_action(stage_name, "stale_job_abort_failed")
        _worker_stage_logger().warning(
            "downstream stage stale job abort failed",
            item_id=item_id,
            next_stage=stage_name,
            job_id=job_id,
            job_status=_job_status_name(status),
            error=str(exc),
        )
        return
    _record_cleanup_action(stage_name, "stale_job_aborted")

    _worker_stage_logger().warning(
        "downstream stage stale job cleared",
        item_id=item_id,
        next_stage=stage_name,
        job_id=job_id,
        job_status=_job_status_name(status),
        aborted=aborted,
    )


def _log_downstream_enqueue_result(
    *, item_id: str, stage_name: str, job_id: str, enqueued: bool
) -> None:
    worker_logger = _worker_stage_logger()
    if enqueued:
        _record_stage_idempotency(stage_name, "scheduled")
        _record_enqueue_decision(stage_name, "enqueued")
        worker_logger.info(
            "downstream stage enqueued",
            item_id=item_id,
            next_stage=stage_name,
            job_id=job_id,
        )
    else:
        _record_stage_idempotency(stage_name, "suppressed")
        _record_enqueue_decision(stage_name, "suppressed")
        worker_logger.warning(
            "downstream stage enqueue suppressed",
            item_id=item_id,
            next_stage=stage_name,
            job_id=job_id,
        )


async def enqueue_parse_scrape_results(
    redis: ArqRedis,
    *,
    item_id: str,
    queue_name: str,
    partial_seasons: list[int] | None = None,
    tenant_id: str | None = None,
) -> bool:
    """Enqueue the parse-scrape-results stage with a unique job id for idempotency."""

    settings = get_settings()
    if not await _enforce_tenant_worker_enqueue_quota(
        redis,
        settings=settings,
        tenant_id=tenant_id,
        stage_name="parse_scrape_results",
    ):
        return False
    await _clear_stale_downstream_job(
        redis,
        item_id=item_id,
        stage_name="parse_scrape_results",
        job_id=parse_scrape_results_job_id(item_id),
    )
    if partial_seasons is None:
        job = await redis.enqueue_job(
            "parse_scrape_results",
            item_id,
            _job_id=parse_scrape_results_job_id(item_id),
            _queue_name=queue_name,
        )
    else:
        job = await redis.enqueue_job(
            "parse_scrape_results",
            item_id,
            _job_id=parse_scrape_results_job_id(item_id),
            _queue_name=queue_name,
            partial_seasons=partial_seasons,
        )
    return job is not None


async def enqueue_process_scraped_item(
    redis: ArqRedis,
    *,
    item_id: str,
    queue_name: str,
    tenant_id: str | None = None,
) -> bool:
    """Backward-compatible alias that now enqueues parse-scrape-results."""

    return await enqueue_parse_scrape_results(
        redis,
        item_id=item_id,
        queue_name=queue_name,
        tenant_id=tenant_id,
    )


async def enqueue_index_item(
    redis: ArqRedis,
    *,
    item_id: str,
    queue_name: str,
    tenant_id: str | None = None,
    defer_by_seconds: int | None = None,
    job_id: str | None = None,
    missing_seasons: list[int] | None = None,
) -> bool:
    """Enqueue the index stage with a unique job id for idempotency."""

    settings = get_settings()
    if not await _enforce_tenant_worker_enqueue_quota(
        redis,
        settings=settings,
        tenant_id=tenant_id,
        stage_name="index_item",
    ):
        return False
    resolved_job_id = job_id or index_item_job_id(item_id)
    await _clear_stale_downstream_job(
        redis,
        item_id=item_id,
        stage_name="index_item",
        job_id=resolved_job_id,
    )
    kwargs: dict[str, object] = {
        "_job_id": resolved_job_id,
        "_queue_name": queue_name,
    }
    if defer_by_seconds is not None and defer_by_seconds > 0:
        WORKER_ENQUEUE_DEFER_SECONDS.labels(stage="index_item").observe(float(defer_by_seconds))
        kwargs["_defer_by"] = timedelta(seconds=defer_by_seconds)
    if missing_seasons:
        kwargs["missing_seasons"] = missing_seasons
    job = await _enqueue_arq_job(redis, "index_item", item_id, **kwargs)
    return job is not None


async def enqueue_scrape_item(
    redis: ArqRedis,
    *,
    item_id: str,
    queue_name: str,
    defer_by_seconds: int | None = None,
    job_id: str | None = None,
    missing_seasons: list[int] | None = None,
    tenant_id: str | None = None,
) -> bool:
    """Enqueue the scrape stage with a unique job id for idempotency."""

    settings = get_settings()
    if not await _enforce_tenant_worker_enqueue_quota(
        redis,
        settings=settings,
        tenant_id=tenant_id,
        stage_name="scrape_item",
    ):
        return False
    resolved_job_id = job_id or scrape_item_job_id(item_id)
    await _clear_stale_downstream_job(
        redis,
        item_id=item_id,
        stage_name="scrape_item",
        job_id=resolved_job_id,
    )
    if defer_by_seconds is not None and defer_by_seconds > 0:
        WORKER_ENQUEUE_DEFER_SECONDS.labels(stage="scrape_item").observe(float(defer_by_seconds))
        if missing_seasons:
            job = await _enqueue_arq_job(
                redis,
                "scrape_item",
                item_id,
                _job_id=resolved_job_id,
                _queue_name=queue_name,
                _defer_by=timedelta(seconds=defer_by_seconds),
                missing_seasons=missing_seasons,
            )
        else:
            job = await _enqueue_arq_job(
                redis,
                "scrape_item",
                item_id,
                _job_id=resolved_job_id,
                _queue_name=queue_name,
                _defer_by=timedelta(seconds=defer_by_seconds),
            )
    else:
        if missing_seasons:
            job = await _enqueue_arq_job(
                redis,
                "scrape_item",
                item_id,
                _job_id=resolved_job_id,
                _queue_name=queue_name,
                missing_seasons=missing_seasons,
            )
        else:
            job = await _enqueue_arq_job(
                redis,
                "scrape_item",
                item_id,
                _job_id=resolved_job_id,
                _queue_name=queue_name,
            )
    return job is not None


async def enqueue_refresh_direct_playback_link(
    redis: ArqRedis,
    *,
    item_id: str,
    queue_name: str,
) -> bool:
    """Enqueue queued direct-play refresh work."""

    job = await _enqueue_arq_job(
        redis,
        "refresh_direct_playback_link",
        item_id,
        _job_id=refresh_direct_playback_link_job_id(item_id),
        _queue_name=queue_name,
    )
    return job is not None


async def enqueue_refresh_selected_hls_failed_lease(
    redis: ArqRedis,
    *,
    item_id: str,
    queue_name: str,
) -> bool:
    """Enqueue queued selected-HLS failed-lease refresh work."""

    job = await _enqueue_arq_job(
        redis,
        "refresh_selected_hls_failed_lease",
        item_id,
        _job_id=refresh_selected_hls_failed_lease_job_id(item_id),
        _queue_name=queue_name,
    )
    return job is not None


async def enqueue_refresh_selected_hls_restricted_fallback(
    redis: ArqRedis,
    *,
    item_id: str,
    queue_name: str,
) -> bool:
    """Enqueue queued selected-HLS restricted-fallback refresh work."""

    job = await _enqueue_arq_job(
        redis,
        "refresh_selected_hls_restricted_fallback",
        item_id,
        _job_id=refresh_selected_hls_restricted_fallback_job_id(item_id),
        _queue_name=queue_name,
    )
    return job is not None


async def enqueue_rank_streams(
    redis: ArqRedis,
    *,
    item_id: str,
    queue_name: str,
    partial_seasons: list[int] | None = None,
    tenant_id: str | None = None,
) -> bool:
    """Enqueue the rank-streams stage with a unique job id for idempotency."""

    settings = get_settings()
    if not await _enforce_tenant_worker_enqueue_quota(
        redis,
        settings=settings,
        tenant_id=tenant_id,
        stage_name="rank_streams",
    ):
        return False
    await _clear_stale_downstream_job(
        redis,
        item_id=item_id,
        stage_name="rank_streams",
        job_id=rank_streams_job_id(item_id),
    )
    if partial_seasons is not None:
        job = await _enqueue_arq_job(
            redis,
            "rank_streams",
            item_id,
            _job_id=rank_streams_job_id(item_id),
            _queue_name=queue_name,
            partial_seasons=partial_seasons,
        )
    else:
        job = await _enqueue_arq_job(
            redis,
            "rank_streams",
            item_id,
            _job_id=rank_streams_job_id(item_id),
            _queue_name=queue_name,
        )
    return job is not None


def _ongoing_show_poll_interval_hours(settings: Settings) -> int:
    """Return the configured poll cadence for ongoing show rechecks."""

    configured = getattr(settings.scraping, "ongoing_show_poll_interval_hours", 24)
    if isinstance(configured, int):
        return max(1, configured)
    return max(1, int(configured))


def _show_completion_retry_delay_seconds(settings: Settings, *, event: ItemEvent) -> int:
    """Return the delayed retry window for non-terminal show completion states.

    PARTIAL_COMPLETE (missing seasons) now requeues immediately so one explicit
    Request More / partial-show run can keep walking the still-missing seasons in
    the same logical fulfillment flow instead of waiting another 15 minutes
    between every successfully downloaded season pack.
    """

    if event is ItemEvent.PARTIAL_COMPLETE:
        return 0
    return _ongoing_show_poll_interval_hours(settings) * 3600


def _show_inventory_retry_delay_seconds(settings: Settings) -> int:
    """Return a bounded retry delay when released show inventory is still empty."""

    return min(_ongoing_show_poll_interval_hours(settings) * 3600, 300)


_PARTIAL_SCOPE_SEASON_COVERAGE_BONUS = 10_000
_PARTIAL_SCOPE_SEASON_PACK_BONUS = 20_000
_PARTIAL_SCOPE_MULTI_EPISODE_BONUS = 2_000


def _ongoing_show_poll_hours(settings: Settings) -> set[int]:
    """Return cron-compatible hour slots for ongoing-show polling."""

    interval_hours = _ongoing_show_poll_interval_hours(settings)
    if interval_hours >= 24:
        return {0}
    return set(range(0, 24, interval_hours))


async def enqueue_debrid_item(
    redis: ArqRedis,
    *,
    item_id: str,
    queue_name: str,
    tenant_id: str | None = None,
) -> bool:
    """Enqueue the debrid stage with a unique job id for idempotency."""

    settings = get_settings()
    if not await _enforce_tenant_worker_enqueue_quota(
        redis,
        settings=settings,
        tenant_id=tenant_id,
        stage_name="debrid_item",
    ):
        return False
    await _clear_stale_downstream_job(
        redis,
        item_id=item_id,
        stage_name="debrid_item",
        job_id=debrid_item_job_id(item_id),
    )
    job = await redis.enqueue_job(
        "debrid_item",
        item_id,
        _job_id=debrid_item_job_id(item_id),
        _queue_name=queue_name,
    )
    return job is not None


async def enqueue_finalize_item(
    redis: ArqRedis,
    *,
    item_id: str,
    queue_name: str,
    tenant_id: str | None = None,
) -> bool:
    """Enqueue the finalize stage with a unique job id for idempotency."""

    settings = get_settings()
    if not await _enforce_tenant_worker_enqueue_quota(
        redis,
        settings=settings,
        tenant_id=tenant_id,
        stage_name="finalize_item",
    ):
        return False
    await _clear_stale_downstream_job(
        redis,
        item_id=item_id,
        stage_name="finalize_item",
        job_id=finalize_item_job_id(item_id),
    )
    job = await redis.enqueue_job(
        "finalize_item",
        item_id,
        _job_id=finalize_item_job_id(item_id),
        _queue_name=queue_name,
    )
    return job is not None


async def is_process_scraped_item_job_active(redis: ArqRedis, *, item_id: str) -> bool:
    """Backward-compatible alias for parse-scrape-results queue activity."""

    status = await Job(parse_scrape_results_job_id(item_id), redis=redis).status()
    _record_job_status("parse_scrape_results", status)
    return status in {JobStatus.deferred, JobStatus.queued, JobStatus.in_progress}


async def is_rank_streams_job_active(redis: ArqRedis, *, item_id: str) -> bool:
    """Return whether the rank-streams stage job is already queued or running."""

    status = await Job(rank_streams_job_id(item_id), redis=redis).status()
    _record_job_status("rank_streams", status)
    return status in {JobStatus.deferred, JobStatus.queued, JobStatus.in_progress}


async def is_index_item_job_active(redis: ArqRedis, *, item_id: str) -> bool:
    """Return whether the index-item stage job is already queued or running."""

    status = await Job(index_item_job_id(item_id), redis=redis).status()
    _record_job_status("index_item", status)
    return status in {JobStatus.deferred, JobStatus.queued, JobStatus.in_progress}


async def is_scrape_item_job_active(redis: ArqRedis, *, item_id: str) -> bool:
    """Return whether the scrape-item stage job is already queued or running."""

    status = await Job(scrape_item_job_id(item_id), redis=redis).status()
    _record_job_status("scrape_item", status)
    return status in {JobStatus.deferred, JobStatus.queued, JobStatus.in_progress}


def _selected_stream(streams: list[StreamORM]) -> StreamORM | None:
    for stream in streams:
        if stream.selected:
            return stream
    return None


def _is_anime_item(attributes: dict[str, object]) -> bool:
    anime_flag = attributes.get("is_anime")
    if isinstance(anime_flag, bool):
        return anime_flag
    genres = attributes.get("genres")
    if isinstance(genres, list):
        return any(isinstance(genre, str) and genre.casefold() == "anime" for genre in genres)
    return False


def _title_aliases(attributes: dict[str, object]) -> list[str]:
    raw_aliases = attributes.get("aliases")
    if not isinstance(raw_aliases, list):
        return []

    aliases: list[str] = []
    seen: set[str] = set()
    for value in raw_aliases:
        if not isinstance(value, str):
            continue
        alias = value.strip()
        if not alias:
            continue
        normalized_alias = alias.casefold()
        if normalized_alias in seen:
            continue
        seen.add(normalized_alias)
        aliases.append(alias)
    return aliases


def _is_dubbed_candidate(stream: StreamORM) -> bool:
    raw = stream.raw_title.casefold()
    if "dubbed" in raw or "dual audio" in raw:
        return True
    payload = str(stream.parsed_title).casefold()
    return "dubbed" in payload or "dual audio" in payload


async def _worker_log_context(media_service: MediaService, *, item_id: str) -> dict[str, object]:
    get_latest_item_request_id = getattr(media_service, "get_latest_item_request_id", None)
    item_request_id: object = None
    if callable(get_latest_item_request_id):
        item_request_id = await get_latest_item_request_id(media_item_id=item_id)
    return {
        "item_id": item_id,
        "item_request_id": item_request_id,
    }


def _build_default_ranking_settings() -> dict[str, object]:
    category_keys = {
        "quality": [
            "av1",
            "avc",
            "bluray",
            "dvd",
            "hdtv",
            "hevc",
            "mpeg",
            "remux",
            "vhs",
            "web",
            "webdl",
            "webmux",
            "xvid",
        ],
        "rips": [
            "bdrip",
            "brrip",
            "dvdrip",
            "hdrip",
            "ppvrip",
            "satrip",
            "tvrip",
            "uhdrip",
            "vhsrip",
            "webdlrip",
            "webrip",
        ],
        "hdr": ["bit10", "dolby_vision", "hdr", "hdr10plus", "sdr"],
        "audio": [
            "aac",
            "atmos",
            "dolby_digital",
            "dolby_digital_plus",
            "dts_lossless",
            "dts_lossy",
            "flac",
            "mono",
            "mp3",
            "stereo",
            "surround",
            "truehd",
        ],
        "extras": [
            "three_d",
            "converted",
            "documentary",
            "dubbed",
            "edition",
            "hardcoded",
            "network",
            "proper",
            "repack",
            "retail",
            "scene",
            "site",
            "subbed",
            "uncensored",
            "upscaled",
        ],
        "trash": ["cam", "clean_audio", "pdtv", "r5", "screener", "size", "telecine", "telesync"],
    }
    return {
        "name": "default",
        "enabled": True,
        "require": [],
        "exclude": [],
        "preferred": [],
        "resolutions": {
            "r2160p": True,
            "r1080p": True,
            "r720p": True,
            "r480p": True,
            "r360p": True,
            "unknown": True,
        },
        "options": {
            "title_similarity": 0.85,
            "remove_all_trash": True,
            "remove_ranks_under": -10000,
            "remove_unknown_languages": False,
            "allow_english_in_languages": True,
            "enable_fetch_speed_mode": False,
            "remove_adult_content": True,
        },
        "languages": {"required": [], "allowed": [], "exclude": [], "preferred": []},
        "custom_ranks": {
            category: {key: {"fetch": True, "use_custom_rank": False, "rank": 0} for key in keys}
            for category, keys in category_keys.items()
        },
    }


def _resolve_ranking_profile(settings: Settings) -> RankingProfile:
    return settings.ranking


def _bucket_limit(settings: Settings) -> int | None:
    raw = settings.scraping.bucket_limit
    return raw if isinstance(raw, int) and raw > 0 else None


def _dubbed_anime_only(settings: Settings) -> bool:
    raw = settings.scraping.dubbed_anime_only
    return bool(raw)


def _build_provider_client(
    *, provider: str, api_key: str, limiter: DistributedRateLimiter
) -> DebridDownloadClient:
    if provider == "realdebrid":
        return RealDebridPlaybackClient(api_token=api_key, limiter=limiter)
    if provider == "alldebrid":
        return AllDebridPlaybackClient(api_token=api_key, limiter=limiter)
    if provider == "debridlink":
        return DebridLinkPlaybackClient(api_token=api_key, limiter=limiter)
    raise ValueError(f"unsupported downloader provider={provider}")


def _resolve_enabled_downloader(
    settings: Settings,
    *,
    item_id: str | None = None,
    item_request_id: str | None = None,
) -> tuple[str, str]:
    # Explicit compatibility-first provider priority: Real-Debrid, then AllDebrid, then Debrid-Link.
    provider_entries = (
        ("realdebrid", settings.downloaders.real_debrid),
        ("alldebrid", settings.downloaders.all_debrid),
        ("debridlink", settings.downloaders.debrid_link),
    )
    enabled = [
        (provider, config.api_key.strip())
        for provider, config in provider_entries
        if config.enabled and config.api_key.strip()
    ]
    if len(enabled) > 1:
        logger.warning(
            "multiple downloaders enabled; selecting by fixed provider priority",
            extra={
                "item_id": item_id,
                "item_request_id": item_request_id,
                "enabled_providers": [provider for provider, _ in enabled],
            },
        )
    if enabled:
        return enabled[0]
    raise ValueError("no_enabled_downloader")


async def _maybe_enqueue_next_stage(
    ctx: dict[str, Any],
    *,
    enqueuer: Callable[[ArqRedis, str, str, str | None], Awaitable[bool]],
    item_id: str,
    stage_name: str,
    job_id: str,
    cleanup_stage_job_ids: tuple[tuple[str, str], ...] = (),
) -> bool:
    arq_redis = ctx.get("arq_redis")
    if arq_redis is None or not hasattr(arq_redis, "enqueue_job"):
        _record_enqueue_decision(stage_name, "arq_unavailable")
        logger.warning(
            "downstream stage enqueue skipped",
            extra={
                "item_id": item_id,
                "next_stage": stage_name,
                "job_id": job_id,
                "reason": "arq_unavailable",
            },
        )
        return False
    redis_client = cast(ArqRedis, arq_redis)
    for cleanup_stage_name, cleanup_job_id in cleanup_stage_job_ids:
        await _clear_stale_downstream_job(
            redis_client,
            item_id=item_id,
            stage_name=cleanup_stage_name,
            job_id=cleanup_job_id,
        )
    queue_name_value = ctx.get("queue_name")
    queue_name = _queue_name(get_settings()) if queue_name_value is None else str(queue_name_value)
    tenant_id = await _resolve_item_tenant_id(ctx, item_id=item_id)
    enqueued = await enqueuer(redis_client, item_id, queue_name, tenant_id)
    _log_downstream_enqueue_result(
        item_id=item_id,
        stage_name=stage_name,
        job_id=job_id,
        enqueued=enqueued,
    )
    return enqueued


async def _maybe_enqueue_parse_stage(
    ctx: dict[str, Any],
    *,
    item_id: str,
    partial_seasons: list[int] | None,
) -> None:
    arq_redis = ctx.get("arq_redis")
    if arq_redis is None or not hasattr(arq_redis, "enqueue_job"):
        _record_enqueue_decision("parse_scrape_results", "arq_unavailable")
        logger.warning(
            "downstream stage enqueue skipped",
            extra={
                "item_id": item_id,
                "next_stage": "parse_scrape_results",
                "job_id": parse_scrape_results_job_id(item_id),
                "reason": "arq_unavailable",
            },
        )
        return
    redis_client = cast(ArqRedis, arq_redis)
    for cleanup_stage_name, cleanup_job_id in (
        ("parse_scrape_results", parse_scrape_results_job_id(item_id)),
        ("rank_streams", rank_streams_job_id(item_id)),
        ("debrid_item", debrid_item_job_id(item_id)),
    ):
        await _clear_stale_downstream_job(
            redis_client,
            item_id=item_id,
            stage_name=cleanup_stage_name,
            job_id=cleanup_job_id,
        )
    queue_name_value = ctx.get("queue_name")
    queue_name = _queue_name(get_settings()) if queue_name_value is None else str(queue_name_value)
    tenant_id = await _resolve_item_tenant_id(ctx, item_id=item_id)
    enqueued = await enqueue_parse_scrape_results(
        redis_client,
        item_id=item_id,
        queue_name=queue_name,
        partial_seasons=partial_seasons,
        tenant_id=tenant_id,
    )
    _log_downstream_enqueue_result(
        item_id=item_id,
        stage_name="parse_scrape_results",
        job_id=parse_scrape_results_job_id(item_id),
        enqueued=enqueued,
    )


def _build_unparsed_candidate_batches(
    raw_candidates: list[tuple[str, str]],
    existing_streams: list[tuple[str, str, bool]],
) -> dict[str, list[str]]:
    """Group raw scrape candidates that still need parse/validate work."""

    existing_by_key = {
        (infohash.strip().lower(), raw_title.casefold()): parsed
        for infohash, raw_title, parsed in existing_streams
    }
    batches: dict[str, list[str]] = {}
    for infohash, raw_title in raw_candidates:
        stream_key = (infohash.strip().lower(), raw_title.casefold())
        if existing_by_key.get(stream_key):
            continue
        batches.setdefault(infohash, []).append(raw_title)
    return batches


async def _persist_unparsed_stream_candidates(
    *,
    media_service: MediaService,
    item_id: str,
    existing_streams: list[StreamORM] | None = None,
    requested_seasons: list[int] | None = None,
) -> int:
    """Parse persisted raw scrape candidates into durable stream rows."""

    raw_candidates = await media_service.get_scrape_candidates(item_id=item_id)
    streams = (
        existing_streams
        if existing_streams is not None
        else await media_service.get_stream_candidates(media_item_id=item_id)
    )
    loop = asyncio.get_running_loop()
    unparsed_by_infohash = await asyncio.wait_for(
        loop.run_in_executor(
            _heavy_stage_executor("parse_scrape_results"),
            functools.partial(
                _build_unparsed_candidate_batches,
                [(candidate.info_hash, candidate.raw_title) for candidate in raw_candidates],
                [
                    (stream.infohash, stream.raw_title, bool(stream.parsed_title))
                    for stream in streams
                ],
            ),
        ),
        timeout=_heavy_stage_timeout_seconds("parse_scrape_results"),
    )

    parsed_count = 0
    for infohash, raw_titles in unparsed_by_infohash.items():
        parsed = await media_service.persist_parsed_stream_candidates(
            item_id=item_id,
            raw_titles=raw_titles,
            infohash=infohash,
            requested_seasons=requested_seasons,
        )
        parsed_count += len(parsed)
    return parsed_count


def _selection_failure_reason(
    ranked_streams: list[RankedStreamCandidateRecord],
    selected_stream_id: str | None,
) -> str:
    """Return a stable failure reason for scraped-item selection failures."""

    if selected_stream_id is not None:
        return "selected_stream_unavailable"
    if not ranked_streams:
        return "no_stream_candidates"
    return "no_passing_stream_candidates"


async def _set_item_recovery_attempt_count(
    ctx: dict[str, Any],
    *,
    item_id: str,
    value: int,
) -> int:
    attempt_counts = cast(dict[str, int], ctx.setdefault("_rank_failure_attempt_counts", {}))
    attempt_counts[item_id] = max(0, value)
    try:
        UUID(str(item_id))
    except (TypeError, ValueError):
        return attempt_counts[item_id]

    db = cast(
        DatabaseRuntime, ctx.get("db") or DatabaseRuntime(get_settings().postgres_dsn, echo=False)
    )
    ctx["db"] = db
    async with db.session() as session:
        item = await session.get(MediaItemORM, item_id)
        if item is None:
            return attempt_counts[item_id]
        next_count = max(0, value)
        item.recovery_attempt_count = next_count
        await session.commit()
        return next_count


async def _set_item_next_retry_at(
    ctx: dict[str, Any],
    *,
    item_id: str,
    value: datetime | None,
) -> datetime | None:
    try:
        UUID(str(item_id))
    except (TypeError, ValueError):
        return value

    db = cast(
        DatabaseRuntime, ctx.get("db") or DatabaseRuntime(get_settings().postgres_dsn, echo=False)
    )
    ctx["db"] = db
    async with db.session() as session:
        item = await session.get(MediaItemORM, item_id)
        if item is None:
            return value
        item.next_retry_at = value
        await session.commit()
        return value


def _plugin_settings_payload_snapshot(payload: dict[str, Any]) -> str:
    """Return a stable snapshot used to detect plugin-setting changes."""

    return json.dumps(payload, sort_keys=True, default=str)


async def _increment_item_recovery_attempt_count(ctx: dict[str, Any], *, item_id: str) -> int:
    attempt_counts = cast(dict[str, int], ctx.setdefault("_rank_failure_attempt_counts", {}))
    attempt_counts[item_id] = int(attempt_counts.get(item_id, 0)) + 1
    try:
        UUID(str(item_id))
    except (TypeError, ValueError):
        return attempt_counts[item_id]

    db = cast(
        DatabaseRuntime, ctx.get("db") or DatabaseRuntime(get_settings().postgres_dsn, echo=False)
    )
    ctx["db"] = db
    async with db.session() as session:
        item = await session.get(MediaItemORM, item_id)
        if item is None:
            return attempt_counts[item_id]
        next_count = int(item.recovery_attempt_count or 0) + 1
        item.recovery_attempt_count = next_count
        await session.commit()
        return next_count


def _rank_failure_cooldown_seconds(settings: Settings, *, attempt_count: int) -> int:
    if attempt_count < 2:
        return 300
    if attempt_count < 5:
        return max(0, int(settings.scraping.after_2 * 3600))
    if attempt_count < 10:
        return max(0, int(settings.scraping.after_5 * 3600))
    return max(0, int(settings.scraping.after_10 * 3600))


def _build_rank_no_winner_diagnostics(
    *,
    scraped_candidate_count: int | None,
    parsed_stream_count: int,
    ranked_results: list[RankedStreamCandidateRecord],
    rank_threshold: int,
) -> dict[str, object]:
    from collections import Counter
    
    passing_fetch_count = sum(1 for record in ranked_results if record.fetch)
    above_threshold_count = sum(
        1 for record in ranked_results if record.rank_score >= rank_threshold
    )
    
    rejection_reasons = Counter(
        record.rejection_reason for record in ranked_results if not record.fetch and record.rejection_reason
    )

    if scraped_candidate_count == 0:
        failure_reason = "no_candidates_scraped"
    elif parsed_stream_count == 0:
        failure_reason = "no_candidates_parsed"
    elif passing_fetch_count == 0:
        failure_reason = "no_candidates_passing_fetch"
    elif above_threshold_count == 0:
        failure_reason = "no_candidates_above_threshold"
    else:
        failure_reason = "unknown"

    return {
        "scraped_candidate_count": scraped_candidate_count,
        "parsed_stream_count": parsed_stream_count,
        "passing_fetch_count": passing_fetch_count,
        "above_threshold_count": above_threshold_count,
        "failure_reason": failure_reason,
        "rejection_reasons": dict(rejection_reasons.most_common()),
    }


def _scraper_provider_name(scraper: object) -> str:
    ctx = getattr(scraper, "ctx", None)
    plugin_name = getattr(ctx, "plugin_name", None)
    if isinstance(plugin_name, str) and plugin_name:
        return plugin_name
    direct_name = getattr(scraper, "plugin_name", None)
    if isinstance(direct_name, str) and direct_name:
        return direct_name
    return scraper.__class__.__name__.casefold()


def _retry_after_seconds_from_http_status_error(exc: httpx.HTTPStatusError) -> float | None:
    raw_retry_after = exc.response.headers.get("Retry-After")
    if raw_retry_after is None:
        return None
    try:
        retry_after = float(raw_retry_after)
    except ValueError:
        return None
    return retry_after if retry_after >= 0 else None


@timed_stage("index_item")
async def index_item(
    ctx: dict[str, object],
    item_id: str,
    *,
    missing_seasons: list[int] | None = None,
) -> str:
    """Enrich metadata in its own stage and enqueue scrape once the item is indexed."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(ctx=mutable_ctx, stage="index_item", item_id=item_id)
    media_service = _resolve_media_service(mutable_ctx)

    try:
        item = await media_service.get_item(item_id)
        if item is None:
            raise ValueError(f"Unknown item_id={item_id}")
        bind_worker_contextvars(
            ctx=mutable_ctx,
            stage="index_item",
            item_id=item_id,
            tenant_id=getattr(item, "tenant_id", None),
        )
        partial_seasons = _normalize_requested_seasons(missing_seasons)
        if item.state in {ItemState.DOWNLOADED, ItemState.COMPLETED, ItemState.SCRAPED}:
            if item.state is ItemState.SCRAPED:
                await _maybe_enqueue_parse_stage(
                    mutable_ctx,
                    item_id=item_id,
                    partial_seasons=partial_seasons,
                )
            return item_id

        if item.state in {
            ItemState.REQUESTED,
            ItemState.PARTIALLY_COMPLETED,
            ItemState.ONGOING,
            ItemState.UNRELEASED,
        }:
            await _try_transition(
                media_service=media_service,
                item_id=item_id,
                event=ItemEvent.INDEX,
                message="index stage started",
            )

        refreshed_item = await media_service.get_item(item_id)
        if refreshed_item is None or refreshed_item.state is not ItemState.INDEXED:
            return item_id

        enrichment = await asyncio.wait_for(
            media_service.enrich_item_metadata(item_id=item_id),
            timeout=_heavy_stage_timeout_seconds("index_item"),
        )
        _worker_stage_logger().info(
            "index_item completed",
            item_id=item_id,
            enrichment_source=enrichment.enrichment.source,
            has_tmdb_id=enrichment.enrichment.has_tmdb_id,
            has_imdb_id=enrichment.enrichment.has_imdb_id,
            warnings=enrichment.enrichment.warnings,
        )
        await _maybe_enqueue_next_stage(
            mutable_ctx,
            enqueuer=lambda redis, item_id, queue_name, tenant_id: enqueue_scrape_item(
                redis,
                item_id=item_id,
                queue_name=queue_name,
                missing_seasons=partial_seasons,
                tenant_id=tenant_id,
            ),
            item_id=item_id,
            stage_name="scrape_item",
            job_id=scrape_item_job_id(item_id),
            cleanup_stage_job_ids=(("scrape_item", scrape_item_job_id(item_id)),),
        )
        return item_id
    except Exception as exc:
        attempt = task_try_count(mutable_ctx)
        if INDEX_RETRY_POLICY.should_dead_letter(attempt):
            await route_dead_letter(
                ctx=mutable_ctx,
                task_name="index_item",
                item_id=item_id,
                reason=str(exc),
            )
            raise
        raise Retry(defer=INDEX_RETRY_POLICY.next_delay_seconds(attempt)) from exc


async def process_scraped_item(ctx: dict[str, object], item_id: str) -> str:
    """Backward-compatible wrapper that runs the new parse and rank stages in sequence."""

    await parse_scrape_results(ctx, item_id)
    return await rank_streams(ctx, item_id)


@timed_stage("parse_scrape_results")
async def parse_scrape_results(
    ctx: dict[str, object],
    item_id: str,
    *,
    partial_seasons: list[int] | None = None,
) -> str:
    """Parse persisted raw candidates, validate them, and enqueue the rank stage."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(ctx=mutable_ctx, stage="parse_scrape_results", item_id=item_id)
    media_service = _resolve_media_service(mutable_ctx)

    try:
        item = await media_service.get_item(item_id)
        if item is None:
            raise ValueError(f"Unknown item_id={item_id}")
        if item.state in {ItemState.DOWNLOADED, ItemState.COMPLETED, ItemState.FAILED}:
            return item_id
        if item.state is not ItemState.SCRAPED:
            return item_id

        log_context = await _worker_log_context(media_service, item_id=item_id)
        logger.info("parse_scrape_results starting", extra=log_context)
        raw_candidates = await media_service.get_scrape_candidates(item_id=item_id)
        if not raw_candidates:
            logger.warning(
                "parse_scrape_results found no persisted scrape candidates",
                extra=log_context,
            )
            await _try_transition(
                media_service=media_service,
                item_id=item_id,
                event=ItemEvent.FAIL,
                message="parse_scrape_results failed: no_scrape_candidates",
            )
            return item_id
        streams = await media_service.get_stream_candidates(media_item_id=item_id)
        has_parsed_candidates = any(stream.parsed_title for stream in streams)
        parsed_count = await _persist_unparsed_stream_candidates(
            media_service=media_service,
            item_id=item_id,
            existing_streams=streams,
            requested_seasons=partial_seasons,
        )
        if parsed_count == 0 and not has_parsed_candidates:
            logger.warning(
                "parse_scrape_results produced no valid parsed candidates",
                extra={**log_context, "parsed_count": parsed_count},
            )
            await _try_transition(
                media_service=media_service,
                item_id=item_id,
                event=ItemEvent.FAIL,
                message="parse_scrape_results failed: no_valid_parsed_candidates",
            )
            return item_id
        logger.info(
            "parse_scrape_results completed",
            extra={**log_context, "parsed_count": parsed_count},
        )
        await _maybe_enqueue_next_stage(
            mutable_ctx,
            enqueuer=lambda redis, item_id, queue_name, tenant_id: enqueue_rank_streams(
                redis,
                item_id=item_id,
                queue_name=queue_name,
                partial_seasons=partial_seasons,
                tenant_id=tenant_id,
            ),
            item_id=item_id,
            stage_name="rank_streams",
            job_id=rank_streams_job_id(item_id),
            cleanup_stage_job_ids=(("rank_streams", rank_streams_job_id(item_id)),),
        )
        return item_id
    except Exception as exc:
        attempt = task_try_count(mutable_ctx)
        if PARSE_RESULTS_RETRY_POLICY.should_dead_letter(attempt):
            await route_dead_letter(
                ctx=mutable_ctx,
                task_name="parse_scrape_results",
                item_id=item_id,
                reason=str(exc),
            )
            raise
        raise Retry(defer=PARSE_RESULTS_RETRY_POLICY.next_delay_seconds(attempt)) from exc


@timed_stage("rank_streams")
async def rank_streams(
    ctx: dict[str, object],
    item_id: str,
    *,
    partial_seasons: list[int] | None = None,
) -> str:
    """Load RTN settings, rank persisted parsed streams, select a winner, and enqueue debrid."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(ctx=mutable_ctx, stage="rank_streams", item_id=item_id)
    media_service = _resolve_media_service(mutable_ctx)
    settings = await _resolve_runtime_settings(mutable_ctx)

    try:
        item = await media_service.get_item(item_id)
        if item is None:
            raise ValueError(f"Unknown item_id={item_id}")
        await _set_item_next_retry_at(mutable_ctx, item_id=item_id, value=None)
        if item.state in {ItemState.COMPLETED, ItemState.FAILED}:
            return item_id
        if item.state is ItemState.DOWNLOADED:
            await _maybe_enqueue_next_stage(
                mutable_ctx,
                enqueuer=lambda redis, item_id, queue_name, tenant_id: enqueue_debrid_item(
                    redis,
                    item_id=item_id,
                    queue_name=queue_name,
                    tenant_id=tenant_id,
                ),
                item_id=item_id,
                stage_name="debrid_item",
                job_id=debrid_item_job_id(item_id),
                cleanup_stage_job_ids=(("debrid_item", debrid_item_job_id(item_id)),),
            )
            _worker_stage_logger().warning(
                "rank_streams resumed downstream debrid enqueue for already-downloaded item",
                item_id=item_id,
            )
            return item_id
        if item.state is not ItemState.SCRAPED:
            return item_id

        log_context = await _worker_log_context(media_service, item_id=item_id)
        logger.info("rank_streams starting", extra=log_context)
        streams = await media_service.get_stream_candidates(media_item_id=item_id)
        profile = _resolve_ranking_profile(settings)
        anime_only = _dubbed_anime_only(settings) and _is_anime_item(item.attributes)
        item_aliases = (
            _title_aliases(item.attributes)
            if getattr(settings.scraping, "enable_aliases", True)
            else []
        )
        # ``partial_seasons`` from the queued job (parse stage) is authoritative
        # for this ranking pass. Fall back to latest request scope only when no
        # explicit partial scope was provided.
        partial_requested_seasons: list[int] | None = _normalize_requested_seasons(partial_seasons)
        get_latest_item_request = getattr(media_service, "get_latest_item_request", None)
        if callable(get_latest_item_request):
            item_request = await get_latest_item_request(media_item_id=item_id)
            if (
                partial_requested_seasons is None
                and item_request is not None
                and item_request.is_partial
            ):
                partial_requested_seasons = _normalize_requested_seasons(
                    item_request.requested_seasons
                )
        if partial_requested_seasons is not None:
            structlog.contextvars.bind_contextvars(
                partial_request=True,
                requested_seasons=partial_requested_seasons,
            )
        ranked_results: list[RankedStreamCandidateRecord] = []
        rankable_streams: list[StreamORM] = []
        rank_batch_inputs: list[_RankBatchInput] = []

        for stream in streams:
            if not stream.parsed_title:
                continue
            if anime_only and not _is_dubbed_candidate(stream):
                ranked_results.append(
                    RankedStreamCandidateRecord(
                        item_id=item_id,
                        stream_id=stream.id,
                        rank_score=0,
                        lev_ratio=0.0,
                        fetch=False,
                        passed=False,
                        rejection_reason="dubbed_anime_only_filtered",
                        stream=stream,
                    )
                )
                continue

            if partial_requested_seasons is not None:
                partial_scope_reason = _partial_scope_rejection_reason(
                    stream,
                    partial_requested_seasons,
                )
                if partial_scope_reason is not None:
                    ranked_results.append(
                        RankedStreamCandidateRecord(
                            item_id=item_id,
                            stream_id=stream.id,
                            rank_score=0,
                            lev_ratio=0.0,
                            fetch=False,
                            passed=False,
                            rejection_reason=partial_scope_reason,
                            stream=stream,
                        )
                    )
                    continue

            expected_scope_reason = _post_rank_expected_scope_reason(item, stream)
            if expected_scope_reason is not None:
                ranked_results.append(
                    RankedStreamCandidateRecord(
                        item_id=item_id,
                        stream_id=stream.id,
                        rank_score=0,
                        lev_ratio=0.0,
                        fetch=False,
                        passed=False,
                        rejection_reason=expected_scope_reason,
                        stream=stream,
                    )
                )
                continue

            rankable_streams.append(stream)
            rank_batch_inputs.append(
                {
                    "stream_id": str(stream.id),
                    "raw_title": stream.raw_title,
                    "parsed_title": (
                        stream.parsed_title if isinstance(stream.parsed_title, dict) else {}
                    ),
                    "resolution": stream.resolution,
                    "partial_scope_bonus": (
                        _partial_scope_rank_bonus(stream, partial_requested_seasons)
                        if partial_requested_seasons is not None
                        else 0
                    ),
                }
            )

        ranked_batch_records: list[_RankBatchRecord]
        if rank_batch_inputs:
            loop = asyncio.get_running_loop()
            ranked_batch_records = await asyncio.wait_for(
                loop.run_in_executor(
                    _heavy_stage_executor("rank_streams"),
                    functools.partial(
                        _rank_stream_batch,
                        item_title=item.title,
                        item_aliases=item_aliases,
                        profile=profile,
                        bucket_limit=_bucket_limit(settings),
                        stream_inputs=rank_batch_inputs,
                    ),
                ),
                timeout=_heavy_stage_timeout_seconds("rank_streams"),
            )
        else:
            ranked_batch_records = []

        stream_map = {str(stream.id): stream for stream in rankable_streams}
        for ranked_record in ranked_batch_records:
            ranked_stream = stream_map.get(ranked_record["stream_id"])
            if ranked_stream is None:
                continue
            ranked_results.append(
                RankedStreamCandidateRecord(
                    item_id=item_id,
                    stream_id=ranked_stream.id,
                    rank_score=ranked_record["rank_score"],
                    lev_ratio=ranked_record["lev_ratio"],
                    fetch=ranked_record["fetch"],
                    passed=ranked_record["passed"],
                    rejection_reason=ranked_record["rejection_reason"],
                    stream=ranked_stream,
                )
            )

        await media_service.persist_ranked_stream_results(
            media_item_id=item_id,
            ranked_results=ranked_results,
        )
        selected_stream: (
            SelectedStreamCandidateRecord | None
        ) = await media_service.select_stream_candidate(
            media_item_id=item_id,
            ranked_results=ranked_results,
        )
        selected_stream_id = selected_stream.id if selected_stream is not None else None
        if selected_stream is None:
            selection_reason = _selection_failure_reason(ranked_results, selected_stream_id)
            diagnostics = _build_rank_no_winner_diagnostics(
                scraped_candidate_count=None,
                parsed_stream_count=len(streams),
                ranked_results=ranked_results,
                rank_threshold=profile.options.remove_ranks_under,
            )
            failure_reason = cast(str, diagnostics["failure_reason"])
            _worker_stage_logger().warning(
                "rank_streams.no_winner",
                item_id=item_id,
                item_request_id=log_context.get("item_request_id"),
                scraped_candidate_count=diagnostics["scraped_candidate_count"],
                parsed_stream_count=diagnostics["parsed_stream_count"],
                passing_fetch_count=diagnostics["passing_fetch_count"],
                above_threshold_count=diagnostics["above_threshold_count"],
                failure_reason=failure_reason,
                rejection_reasons=diagnostics["rejection_reasons"],
            )

            arq_redis = mutable_ctx.get("arq_redis")
            if arq_redis is None or not hasattr(arq_redis, "enqueue_job"):
                await _try_transition(
                    media_service=media_service,
                    item_id=item_id,
                    event=ItemEvent.FAIL,
                    message=f"rank_streams failed: {selection_reason}",
                )
                return item_id

            attempt_count = await _increment_item_recovery_attempt_count(
                mutable_ctx, item_id=item_id
            )
            max_failed_attempts = settings.scraping.max_failed_attempts
            if max_failed_attempts > 0 and attempt_count >= max_failed_attempts:
                await _set_item_next_retry_at(mutable_ctx, item_id=item_id, value=None)
                _worker_stage_logger().warning(
                    "rank_streams.max_attempts_reached",
                    item_id=item_id,
                    attempt_count=attempt_count,
                    max_failed_attempts=max_failed_attempts,
                )
                await _try_transition(
                    media_service=media_service,
                    item_id=item_id,
                    event=ItemEvent.FAIL,
                    message=f"rank_streams failed: {selection_reason}",
                )
                return item_id

            cooldown_seconds = _rank_failure_cooldown_seconds(settings, attempt_count=attempt_count)
            next_retry_at = (
                datetime.now(UTC) + timedelta(seconds=cooldown_seconds)
                if cooldown_seconds > 0
                else None
            )
            requeue_job_id = f"{scrape_item_job_id(item_id)}:retry:{attempt_count}"
            enqueued = await _maybe_enqueue_next_stage(
                mutable_ctx,
                enqueuer=lambda redis, item_id, queue_name, tenant_id: enqueue_scrape_item(
                    redis,
                    item_id=item_id,
                    queue_name=queue_name,
                    defer_by_seconds=cooldown_seconds if cooldown_seconds > 0 else None,
                    job_id=requeue_job_id,
                    tenant_id=tenant_id,
                ),
                item_id=item_id,
                stage_name="scrape_item",
                job_id=requeue_job_id,
                cleanup_stage_job_ids=(("scrape_item", scrape_item_job_id(item_id)),),
            )
            if enqueued:
                await _set_item_next_retry_at(
                    mutable_ctx,
                    item_id=item_id,
                    value=next_retry_at,
                )
            _worker_stage_logger().info(
                "rank_streams.requeue_scrape",
                item_id=item_id,
                attempt_count=attempt_count,
                cooldown_seconds=cooldown_seconds,
                next_retry_at=next_retry_at.isoformat() if next_retry_at is not None else None,
                next_stage="scrape_item",
                failure_reason=failure_reason,
            )
            return item_id

        assert selected_stream_id is not None
        await _set_item_recovery_attempt_count(mutable_ctx, item_id=item_id, value=0)
        await _set_item_next_retry_at(mutable_ctx, item_id=item_id, value=None)
        logger.info(
            "rank_streams selected stream",
            extra={**log_context, "selected_stream_id": selected_stream_id},
        )
        await _try_transition(
            media_service=media_service,
            item_id=item_id,
            event=ItemEvent.DOWNLOAD,
            message=f"rank_streams selected stream {selected_stream_id}",
        )
        await _maybe_enqueue_next_stage(
            mutable_ctx,
            enqueuer=lambda redis, item_id, queue_name, tenant_id: enqueue_debrid_item(
                redis,
                item_id=item_id,
                queue_name=queue_name,
                tenant_id=tenant_id,
            ),
            item_id=item_id,
            stage_name="debrid_item",
            job_id=debrid_item_job_id(item_id),
            cleanup_stage_job_ids=(("debrid_item", debrid_item_job_id(item_id)),),
        )
        return item_id
    except Exception as exc:
        log_context = await _worker_log_context(media_service, item_id=item_id)
        logger.warning("rank_streams failed", extra={**log_context, "error": str(exc)})
        attempt = task_try_count(mutable_ctx)
        if RANK_STREAMS_RETRY_POLICY.should_dead_letter(attempt):
            await route_dead_letter(
                ctx=mutable_ctx,
                task_name="rank_streams",
                item_id=item_id,
                reason=str(exc),
            )
            raise
        raise Retry(defer=RANK_STREAMS_RETRY_POLICY.next_delay_seconds(attempt)) from exc


@timed_stage("recover_incomplete_library")
async def recover_incomplete_library(ctx: dict[str, object]) -> int:
    """Recover failed or orphaned scraped items back into the scraped-item worker path."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(
        ctx=mutable_ctx,
        stage="recover_incomplete_library",
        item_id="library-recovery",
    )
    media_service = _resolve_media_service(mutable_ctx)
    settings = await _resolve_runtime_settings(mutable_ctx)
    queue_name = str(mutable_ctx.get("queue_name", _queue_name(settings)))

    try:
        arq_redis = await _resolve_arq_redis(mutable_ctx)
        snapshot = await media_service.recover_incomplete_library(
            recovery_cooldown=timedelta(minutes=settings.recovery_cooldown_minutes),
            max_recovery_attempts=settings.max_recovery_attempts,
            is_scrape_item_job_active=lambda item_id: is_scrape_item_job_active(
                arq_redis,
                item_id=item_id,
            ),
            reenqueue_scrape_item=lambda item_id: enqueue_scrape_item(
                arq_redis,
                item_id=item_id,
                queue_name=queue_name,
            ),
            is_scraped_item_job_active=lambda item_id: is_process_scraped_item_job_active(
                arq_redis,
                item_id=item_id,
            ),
            reenqueue_scraped_item=lambda item_id: enqueue_process_scraped_item(
                arq_redis,
                item_id=item_id,
                queue_name=queue_name,
            ),
        )
        return len(snapshot.recovered)
    except Exception as exc:
        attempt = task_try_count(mutable_ctx)
        if RECOVERY_RETRY_POLICY.should_dead_letter(attempt):
            await route_dead_letter(
                ctx=mutable_ctx,
                task_name="recover_incomplete_library",
                item_id="library-recovery",
                reason=str(exc),
            )
            raise
        raise Retry(defer=RECOVERY_RETRY_POLICY.next_delay_seconds(attempt)) from exc


@timed_stage("retry_library")
async def retry_library(ctx: dict[str, object]) -> int:
    """Re-enqueue incomplete items at the correct stage based on current lifecycle state."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(ctx=mutable_ctx, stage="retry_library", item_id="retry-library")
    media_service = _resolve_media_service(mutable_ctx)
    settings = await _resolve_runtime_settings(mutable_ctx)

    try:
        arq_redis = await _resolve_arq_redis(mutable_ctx)
        queue_name = str(mutable_ctx.get("queue_name", _queue_name(settings)))
        items = await media_service.list_items_in_states(
            states=[ItemState.REQUESTED, ItemState.INDEXED, ItemState.SCRAPED, ItemState.DOWNLOADED]
        )
        re_enqueued: int = 0
        for item in items:
            log_context = {"item_id": item.id}
            recovery_plan = _build_recovery_plan_record(state=item.state)
            if recovery_plan.target_stage is RecoveryTargetStage.INDEX:
                if await is_index_item_job_active(arq_redis, item_id=item.id):
                    logger.info(
                        "retry_library skipped already-queued item",
                        extra={**log_context, "stage": "index_item"},
                    )
                    continue
                if await enqueue_index_item(
                    arq_redis,
                    item_id=item.id,
                    queue_name=queue_name,
                    tenant_id=item.tenant_id,
                ):
                    logger.info(
                        "retry_library re-enqueued item",
                        extra={**log_context, "stage": "index_item"},
                    )
                    re_enqueued += 1
            elif recovery_plan.target_stage is RecoveryTargetStage.SCRAPE:
                if await is_scrape_item_job_active(arq_redis, item_id=item.id):
                    logger.info(
                        "retry_library skipped already-queued item",
                        extra={**log_context, "stage": "scrape_item"},
                    )
                    continue
                if await enqueue_scrape_item(
                    arq_redis,
                    item_id=item.id,
                    queue_name=queue_name,
                    tenant_id=item.tenant_id,
                ):
                    logger.info(
                        "retry_library re-enqueued item",
                        extra={**log_context, "stage": "scrape_item"},
                    )
                    re_enqueued += 1
            elif recovery_plan.target_stage is RecoveryTargetStage.PARSE:
                if await is_process_scraped_item_job_active(arq_redis, item_id=item.id):
                    logger.info(
                        "retry_library skipped already-queued item",
                        extra={**log_context, "stage": "parse_scrape_results"},
                    )
                    continue
                if await enqueue_parse_scrape_results(
                    arq_redis,
                    item_id=item.id,
                    queue_name=queue_name,
                    tenant_id=item.tenant_id,
                ):
                    logger.info(
                        "retry_library re-enqueued item",
                        extra={**log_context, "stage": "parse_scrape_results"},
                    )
                    re_enqueued += 1
            elif recovery_plan.target_stage is RecoveryTargetStage.FINALIZE:
                # Recover orphaned DOWNLOADED items: worker crashed after debrid completed
                # but before finalize_item was enqueued. Re-enqueue finalize to complete them.
                finalize_status = await Job(
                    finalize_item_job_id(item.id), redis=arq_redis
                ).status()
                _record_job_status("finalize_item", finalize_status)
                if finalize_status in {JobStatus.deferred, JobStatus.queued, JobStatus.in_progress}:
                    logger.info(
                        "retry_library skipped already-queued item",
                        extra={**log_context, "stage": "finalize_item"},
                    )
                    continue
                if await enqueue_finalize_item(
                    arq_redis,
                    item_id=item.id,
                    queue_name=queue_name,
                    tenant_id=item.tenant_id,
                ):
                    logger.info(
                        "retry_library re-enqueued item",
                        extra={**log_context, "stage": "finalize_item"},
                    )
                    re_enqueued += 1
        return re_enqueued
    except Exception as exc:
        attempt = task_try_count(mutable_ctx)
        if RECOVERY_RETRY_POLICY.should_dead_letter(attempt):
            await route_dead_letter(
                ctx=mutable_ctx,
                task_name="retry_library",
                item_id="retry-library",
                reason=str(exc),
            )
            raise
        raise Retry(defer=RECOVERY_RETRY_POLICY.next_delay_seconds(attempt)) from exc


@timed_stage("publish_outbox_events")
async def publish_outbox_events(ctx: dict[str, object]) -> int:
    """Publish pending transactional outbox rows through the process-local event bus."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(
        ctx=mutable_ctx, stage="publish_outbox_events", item_id="outbox-publisher"
    )
    media_service = _resolve_media_service(mutable_ctx)
    settings = await _resolve_runtime_settings(mutable_ctx)

    try:
        snapshot = await media_service.publish_outbox_events(
            max_outbox_attempts=settings.max_outbox_attempts,
        )
        return snapshot.published_count
    except Exception as exc:
        attempt = task_try_count(mutable_ctx)
        if OUTBOX_RETRY_POLICY.should_dead_letter(attempt):
            await route_dead_letter(
                ctx=mutable_ctx,
                task_name="publish_outbox_events",
                item_id="outbox-publisher",
                reason=str(exc),
            )
            raise
        raise Retry(defer=OUTBOX_RETRY_POLICY.next_delay_seconds(attempt)) from exc


async def poll_content_services(ctx: dict[str, object]) -> None:
    """Poll registered content-service plugins and fan out their requests into media intake."""

    mutable_ctx = cast(dict[str, Any], ctx)
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(worker_stage="poll_content_services")
    plugin_registry = cast(Any, mutable_ctx.get("plugin_registry"))
    if not hasattr(plugin_registry, "get_content_services"):
        plugin_registry = await _resolve_plugin_registry(mutable_ctx)
    media_service = cast(Any, mutable_ctx.get("media_service"))
    if not hasattr(media_service, "request_items_by_identifiers"):
        media_service = _resolve_media_service(mutable_ctx)

    for plugin in plugin_registry.get_content_services():
        try:
            requests = await plugin.poll()
            for request in requests:
                await media_service.request_items_by_identifiers(
                    identifiers=[request.external_ref],
                    media_type=request.media_type,
                )
            structlog.get_logger().info(
                "worker.content_poll.complete",
                plugin=getattr(plugin, "plugin_name", "unknown"),
                request_count=len(requests),
            )
        except Exception as exc:
            structlog.get_logger().warning(
                "worker.content_poll.failed",
                plugin=getattr(plugin, "plugin_name", "unknown"),
                exc=str(exc),
            )


async def backfill_imdb_ids(ctx: dict[str, object]) -> dict[str, int]:
    """Backfill missing IMDb IDs for persisted items and requeue failed ones."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(ctx=mutable_ctx, stage="backfill_imdb_ids", item_id="imdb-backfill")
    media_service = _resolve_media_service(mutable_ctx)
    db = cast(
        DatabaseRuntime, mutable_ctx.get("db") or DatabaseRuntime(get_settings().postgres_dsn)
    )
    mutable_ctx["db"] = db

    async with db.session() as session:
        summary = await media_service.backfill_missing_imdb_ids(session)

    logger.info("worker.backfill_imdb_ids.complete", extra=summary)
    return summary


@timed_stage("scrape_item")
async def scrape_item(ctx: dict[str, object], item_id: str, *, missing_seasons: list[int] | None = None) -> str:
    """Run configured scrape providers, persist raw candidates, and enqueue parse."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(ctx=mutable_ctx, stage="scrape_item", item_id=item_id)
    limiter = _resolve_limiter(mutable_ctx)
    await _acquire_worker_rate_limit(
        limiter=limiter,
        bucket="worker:scrape",
        capacity=20,
        refill_per_second=5,
    )

    media_service = _resolve_media_service(mutable_ctx)
    await _resolve_runtime_settings(mutable_ctx)

    try:
        item = await media_service.get_item(item_id)
        if item is None:
            raise ValueError(f"Unknown item_id={item_id}")
        bind_worker_contextvars(
            ctx=mutable_ctx,
            stage="scrape_item",
            item_id=item_id,
            tenant_id=getattr(item, "tenant_id", None),
        )

        aired_at = item.attributes.get("aired_at")
        if aired_at is not None:
            from datetime import date, datetime

            try:
                release_date = datetime.fromisoformat(str(aired_at).replace("Z", "+00:00")).date()
                if release_date > date.today():
                    await _try_transition(
                        media_service=media_service,
                        item_id=item_id,
                        event=ItemEvent.MARK_UNRELEASED,
                        message="item is unreleased",
                    )
                    _worker_stage_logger().info(
                        "scrape_item deferred unreleased item to UNRELEASED holding state",
                        item_id=item_id,
                        aired_at=aired_at,
                    )
                    return item_id
            except (ValueError, TypeError):
                pass

        # Resolve the scrape scope for partial show retries.
        #
        # For finalize-driven retries, ``missing_seasons`` is authoritative and
        # intentionally overrides the original request scope. This prevents the
        # worker from repeatedly re-selecting already-satisfied seasons.
        partial_seasons: list[int] | None = None
        get_latest_item_request = getattr(media_service, "get_latest_item_request", None)
        if callable(get_latest_item_request):
            item_request = await get_latest_item_request(media_item_id=item_id)
            if item_request is not None and item_request.is_partial:
                partial_seasons = _normalize_requested_seasons(item_request.requested_seasons)
        if missing_seasons:
            normalized_missing_seasons = _normalize_requested_seasons(missing_seasons)
            partial_seasons = normalized_missing_seasons
            structlog.contextvars.bind_contextvars(missing_seasons=normalized_missing_seasons)
        if partial_seasons is not None:
            structlog.contextvars.bind_contextvars(
                partial_request=True,
                requested_seasons=partial_seasons,
            )
        if item.state in {
            ItemState.DOWNLOADED,
            ItemState.COMPLETED,
            ItemState.UNRELEASED,
        }:
            return item_id
        if item.state in {ItemState.REQUESTED, ItemState.PARTIALLY_COMPLETED, ItemState.ONGOING}:
            await _maybe_enqueue_next_stage(
                mutable_ctx,
                enqueuer=lambda redis, item_id, queue_name, tenant_id: enqueue_index_item(
                    redis,
                    item_id=item_id,
                    queue_name=queue_name,
                    tenant_id=tenant_id,
                ),
                item_id=item_id,
                stage_name="index_item",
                job_id=index_item_job_id(item_id),
                cleanup_stage_job_ids=(("index_item", index_item_job_id(item_id)),),
            )
            _worker_stage_logger().warning(
                "scrape_item requeued upstream index stage for pre-index item",
                item_id=item_id,
                state=item.state.value,
            )
            return item_id
        if item.state is ItemState.SCRAPED:
            await _maybe_enqueue_parse_stage(
                mutable_ctx,
                item_id=item_id,
                partial_seasons=partial_seasons,
            )
            _worker_stage_logger().warning(
                "scrape_item resumed downstream parse enqueue for already-scraped item",
                item_id=item_id,
                partial_seasons=partial_seasons,
            )
            return item_id
        if item.state is not ItemState.INDEXED:
            return item_id
        log_context = await _worker_log_context(media_service, item_id=item_id)
        plugin_registry = await _resolve_plugin_registry(mutable_ctx)
        scrape_candidates, provider_summaries = await _scrape_with_plugins(
            plugin_registry=plugin_registry,
            item=item,
            partial_seasons=partial_seasons,
        )
        for summary in provider_summaries:
            _worker_stage_logger().debug(
                "scrape_item.provider_summary",
                item_id=item_id,
                provider=summary["provider"],
                candidate_count=summary["candidate_count"],
                status=summary["status"],
            )
        if (
            not scrape_candidates
            and item.attributes.get("tvdb_id") is not None
            and item.attributes.get("imdb_id") is None
        ):
            enrichment = await media_service.enrich_item_metadata(item_id=item_id)
            if enrichment.enrichment.has_imdb_id:
                refreshed_item = await media_service.get_item(item_id)
                if refreshed_item is not None:
                    item = refreshed_item
                    bind_worker_contextvars(
                        ctx=mutable_ctx,
                        stage="scrape_item",
                        item_id=item_id,
                        tenant_id=getattr(item, "tenant_id", None),
                    )
                    scrape_candidates, provider_summaries = await _scrape_with_plugins(
                        plugin_registry=plugin_registry,
                        item=item,
                        partial_seasons=partial_seasons,
                    )
                    for summary in provider_summaries:
                        _worker_stage_logger().debug(
                            "scrape_item.provider_summary",
                            item_id=item_id,
                            provider=summary["provider"],
                            candidate_count=summary["candidate_count"],
                            status=summary["status"],
                        )
            else:
                logger.warning(
                    "scrape_item.enrichment_failed",
                    extra={
                        **log_context,
                        "enrichment_source": enrichment.enrichment.source,
                        "warnings": enrichment.enrichment.warnings,
                    },
                )
        if not scrape_candidates:
            logger.warning("scrape_item produced no candidates", extra=log_context)
            
            has_existing_media = item.has_media_entries
            if has_existing_media:
                await _try_transition(
                    media_service=media_service,
                    item_id=item_id,
                    event=ItemEvent.PARTIAL_COMPLETE,
                    message="scrape_item no_candidates (but has existing media)",
                )
            else:
                await _try_transition(
                    media_service=media_service,
                    item_id=item_id,
                    event=ItemEvent.FAIL,
                    message="scrape_item failed: no_candidates",
                )
            return item_id

        await media_service.persist_scrape_candidates(
            item_id=item_id,
            candidates=scrape_candidates,
        )
        await _try_transition(
            media_service=media_service,
            item_id=item_id,
            event=ItemEvent.SCRAPE,
            message=f"scrape done: {len(scrape_candidates)} candidates",
        )
        await _maybe_enqueue_parse_stage(
            mutable_ctx,
            item_id=item_id,
            partial_seasons=partial_seasons,
        )
        return item_id
    except Exception as exc:
        attempt = task_try_count(mutable_ctx)
        if SCRAPE_RETRY_POLICY.should_dead_letter(attempt):
            await route_dead_letter(
                ctx=mutable_ctx,
                task_name="scrape_item",
                item_id=item_id,
                reason=str(exc),
            )
            raise
        raise Retry(defer=SCRAPE_RETRY_POLICY.next_delay_seconds(attempt)) from exc


@timed_stage("debrid_item")
async def debrid_item(ctx: dict[str, object], item_id: str) -> str:
    """Resolve the selected torrent through the configured debrid provider and persist media entries."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(ctx=mutable_ctx, stage="debrid_item", item_id=item_id)
    limiter = _resolve_limiter(mutable_ctx)
    await _acquire_worker_rate_limit(
        limiter=limiter,
        bucket="worker:debrid",
        capacity=10,
        refill_per_second=2,
    )

    media_service = _resolve_media_service(mutable_ctx)
    settings = await _resolve_runtime_settings(mutable_ctx)
    item_request_id: str | None = None
    provider: str | None = None

    try:
        item = await media_service.get_item(item_id)
        if item is None:
            raise ValueError(f"Unknown item_id={item_id}")
        bind_worker_contextvars(
            ctx=mutable_ctx,
            stage="debrid_item",
            item_id=item_id,
            tenant_id=getattr(item, "tenant_id", None),
        )
        if item.state in {ItemState.COMPLETED, ItemState.FAILED}:
            return item_id
        if item.state is not ItemState.DOWNLOADED:
            return item_id

        item_request_id = await media_service.get_latest_item_request_id(media_item_id=item_id)
        streams = await media_service.get_stream_candidates(media_item_id=item_id)
        selected_stream = _selected_stream(streams)
        if selected_stream is None:
            raise ValueError("selected_stream_missing")

        provider, api_key = _resolve_enabled_downloader(
            settings,
            item_id=item_id,
            item_request_id=item_request_id,
        )
        client = _build_provider_client(provider=provider, api_key=api_key, limiter=limiter)
        magnet_url = f"magnet:?xt=urn:btih:{selected_stream.infohash}".lower()
        logger.info(
            "debrid stage starting",
            extra={"item_id": item_id, "item_request_id": item_request_id, "provider": provider},
        )

        provider_torrent_id = await client.add_magnet(magnet_url)

        # Eagerly select all downloadable files BEFORE the polling loop so that
        # providers like Real-Debrid register the full file selection and return
        # links for ALL selected files (not just the first one) when done.
        initial_info = await client.get_torrent_info(provider_torrent_id)
        initial_selected = filter_torrent_files(initial_info.files, settings.downloaders)
        if initial_selected:
            try:
                await client.select_files(
                    provider_torrent_id, [file.file_id for file in initial_selected]
                )
            except Exception as _sel_exc:
                # Non-fatal: provider may not yet accept selection before magnet resolution
                _worker_stage_logger().debug(
                    "debrid_item.pre_poll_select_skipped",
                    item_id=item_id,
                    reason=str(_sel_exc),
                )

        timeout_at = asyncio.get_running_loop().time() + 300.0  # 5-min cap for large packs
        torrent_info = await client.get_torrent_info(provider_torrent_id)
        while torrent_info.status not in {"downloaded", "ready", "download_ready"}:
            if asyncio.get_running_loop().time() >= timeout_at:
                raise TimeoutError("debrid_poll_timeout")
            mid_selected = filter_torrent_files(torrent_info.files, settings.downloaders)
            if mid_selected:
                with contextlib.suppress(Exception):
                    await client.select_files(
                        provider_torrent_id, [file.file_id for file in mid_selected]
                    )
            await asyncio.sleep(2.0)  # poll every 2s — sleep(0) was busy-spinning
            torrent_info = await client.get_torrent_info(provider_torrent_id)

        selected_files = filter_torrent_files(torrent_info.files, settings.downloaders)
        if not selected_files:
            raise ValueError("no_downloadable_files")
        await client.select_files(provider_torrent_id, [file.file_id for file in selected_files])
        refreshed_info = await client.get_torrent_info(provider_torrent_id)
        download_urls = await client.get_download_links(provider_torrent_id)
        await media_service.persist_debrid_download_entries(
            media_item_id=item_id,
            provider=provider,
            provider_download_id=provider_torrent_id,
            torrent_info=refreshed_info,
            download_urls=download_urls,
        )
        logger.info(
            "debrid stage completed",
            extra={
                "item_id": item_id,
                "item_request_id": item_request_id,
                "provider": provider,
                "provider_torrent_id": provider_torrent_id,
                "download_url_count": len(download_urls),
            },
        )
        await _maybe_enqueue_next_stage(
            mutable_ctx,
            enqueuer=lambda redis, item_id, queue_name, tenant_id: enqueue_finalize_item(
                redis,
                item_id=item_id,
                queue_name=queue_name,
                tenant_id=tenant_id,
            ),
            item_id=item_id,
            stage_name="finalize_item",
            job_id=finalize_item_job_id(item_id),
        )
        return item_id
    except DebridRateLimitError as exc:
        _worker_stage_logger().warning(
            "debrid_item.rate_limited",
            item_id=item_id,
            item_request_id=item_request_id,
            provider=provider or exc.provider,
            retry_after=exc.retry_after_seconds,
        )
        attempt = task_try_count(mutable_ctx)
        if DEBRID_RETRY_POLICY.should_dead_letter(attempt):
            await route_dead_letter(
                ctx=mutable_ctx,
                task_name="debrid_item",
                item_id=item_id,
                reason=str(exc),
            )
            raise
        raise Retry(defer=DEBRID_RETRY_POLICY.next_delay_seconds(attempt)) from exc
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 429:
            _worker_stage_logger().warning(
                "debrid_item.rate_limited",
                item_id=item_id,
                item_request_id=item_request_id,
                provider=provider or "unknown",
                retry_after=_retry_after_seconds_from_http_status_error(exc),
            )
            attempt = task_try_count(mutable_ctx)
            if DEBRID_RETRY_POLICY.should_dead_letter(attempt):
                await route_dead_letter(
                    ctx=mutable_ctx,
                    task_name="debrid_item",
                    item_id=item_id,
                    reason=str(exc),
                )
                raise
            raise Retry(defer=DEBRID_RETRY_POLICY.next_delay_seconds(attempt)) from exc

        logger.warning(
            "debrid stage failed",
            extra={
                "item_id": item_id,
                "item_request_id": item_request_id,
                "error": str(exc),
            },
        )
        await _try_transition(
            media_service=media_service,
            item_id=item_id,
            event=ItemEvent.FAIL,
            message=f"debrid failed: {exc}",
        )
        attempt = task_try_count(mutable_ctx)
        if DEBRID_RETRY_POLICY.should_dead_letter(attempt):
            await route_dead_letter(
                ctx=mutable_ctx,
                task_name="debrid_item",
                item_id=item_id,
                reason=str(exc),
            )
            raise
        raise Retry(defer=DEBRID_RETRY_POLICY.next_delay_seconds(attempt)) from exc
    except Exception as exc:
        logger.warning(
            "debrid stage failed",
            extra={
                "item_id": item_id,
                "item_request_id": item_request_id,
                "error": str(exc),
            },
        )
        await _try_transition(
            media_service=media_service,
            item_id=item_id,
            event=ItemEvent.FAIL,
            message=f"debrid failed: {exc}",
        )
        attempt = task_try_count(mutable_ctx)
        if DEBRID_RETRY_POLICY.should_dead_letter(attempt):
            await route_dead_letter(
                ctx=mutable_ctx,
                task_name="debrid_item",
                item_id=item_id,
                reason=str(exc),
            )
            raise
        raise Retry(defer=DEBRID_RETRY_POLICY.next_delay_seconds(attempt)) from exc


@timed_stage("finalize_item")
async def finalize_item(ctx: dict[str, object], item_id: str) -> str:
    """Finalize phase scaffold to mark item as completed."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(ctx=mutable_ctx, stage="finalize_item", item_id=item_id)
    media_service = _resolve_media_service(mutable_ctx)
    settings = await _resolve_runtime_settings(mutable_ctx)
    item_request_id: str | None = None

    try:
        item = await media_service.get_item(item_id)
        if item is None:
            raise ValueError(f"Unknown item_id={item_id}")
        bind_worker_contextvars(
            ctx=mutable_ctx,
            stage="finalize_item",
            item_id=item_id,
            tenant_id=getattr(item, "tenant_id", None),
        )
        if item.state is ItemState.COMPLETED:
            return item_id
        if item.state not in (
            ItemState.DOWNLOADED,
            ItemState.PARTIALLY_COMPLETED,
            ItemState.ONGOING,
        ):
            return item_id

        if item.state in (ItemState.PARTIALLY_COMPLETED, ItemState.ONGOING):
            logger.debug(
                "finalize_item re-entered from holding state",
                extra={"item_id": item_id, "state": item.state.value},
            )

        item_request_id = await media_service.get_latest_item_request_id(media_item_id=item_id)
        logger.info(
            "finalize stage starting",
            extra={"item_id": item_id, "item_request_id": item_request_id},
        )

        completion_result: dict[str, object] | None = None
        event = ItemEvent.COMPLETE
        message = "finalize done"
        missing_season_numbers: list[int] = []

        if _resolve_item_type(item) == "show":
            result = await _evaluate_show_completion(item, media_service._db, settings)
            completion_result = {
                "all_satisfied": result.all_satisfied,
                "any_satisfied": result.any_satisfied,
                "has_future_episodes": result.has_future_episodes,
                "missing_released": result.missing_released,
            }
            if result.all_satisfied and not result.has_future_episodes:
                event = ItemEvent.COMPLETE
                message = "finalize done"
            elif result.all_satisfied and result.has_future_episodes:
                event = ItemEvent.MARK_ONGOING
                message = "waiting_on_unreleased_episodes"
            elif result.any_satisfied:
                event = ItemEvent.PARTIAL_COMPLETE
                message = "missing_episodes"
                missing_season_numbers = sorted({s for s, _e in result.missing_released})
            else:
                arq_redis = await _resolve_arq_redis(mutable_ctx)
                # Add a minimum defer so the pipeline doesn't tight-loop when
                # show inventory/metadata hasn't stabilised yet.
                no_inventory_defer = _show_inventory_retry_delay_seconds(settings)
                await enqueue_index_item(
                    arq_redis,
                    item_id=item.id,
                    queue_name=_queue_name(settings),
                    tenant_id=item.tenant_id,
                    defer_by_seconds=no_inventory_defer,
                    job_id=index_item_followup_job_id(
                        item.id,
                        discriminator="inventory-recheck",
                        missing_seasons=sorted({s for s, _e in result.missing_released}),
                    ),
                    missing_seasons=sorted({s for s, _e in result.missing_released}),
                )
                logger.info(
                    "finalize stage re-queued show without satisfied released episodes",
                    extra={
                        "item_id": item_id,
                        "item_request_id": item_request_id,
                        "completion_status": completion_result,
                        "defer_seconds": no_inventory_defer,
                    },
                )
                return item_id

        await _try_transition(
            media_service=media_service,
            item_id=item_id,
            event=event,
            message=message,
        )

        if event is ItemEvent.COMPLETE:
            notifier = MediaServerNotifier(settings.updaters)
            try:
                notification_summary = await notifier.notify_all(str(item_id)) or {}
            except Exception as exc:
                logger.warning(
                    "finalize stage media server notification failed",
                    extra={
                        "item_id": item_id,
                        "item_request_id": item_request_id,
                        "error": str(exc),
                    },
                )
            else:
                triggered = ",".join(
                    sorted(
                        provider
                        for provider, status in notification_summary.items()
                        if status == "triggered"
                    )
                ) or "none"
                failed = ",".join(
                    sorted(
                        provider
                        for provider, status in notification_summary.items()
                        if status == "failed"
                    )
                ) or "none"
                skipped = ",".join(
                    sorted(
                        provider
                        for provider, status in notification_summary.items()
                        if status == "skipped"
                    )
                ) or "none"
                logger.info(
                    "media_server.notification_summary item_id=%s triggered=%s failed=%s skipped=%s",
                    item_id,
                    triggered,
                    failed,
                    skipped,
                )
        elif event in {ItemEvent.PARTIAL_COMPLETE, ItemEvent.MARK_ONGOING}:
            arq_redis = await _resolve_arq_redis(mutable_ctx)
            followup_defer = _show_completion_retry_delay_seconds(settings, event=event)
            await enqueue_index_item(
                arq_redis,
                item_id=item.id,
                queue_name=_queue_name(settings),
                tenant_id=item.tenant_id,
                defer_by_seconds=followup_defer if followup_defer > 0 else None,
                job_id=index_item_followup_job_id(
                    item.id,
                    discriminator=(
                        "ongoing-poll" if event is ItemEvent.MARK_ONGOING else "partial-followup"
                    ),
                    missing_seasons=missing_season_numbers or None,
                ),
                missing_seasons=missing_season_numbers or None,
            )

        logger.info(
            "finalize stage completed",
            extra={
                "item_id": item_id,
                "item_request_id": item_request_id,
                "completion_status": completion_result or event.value,
            },
        )
        return item_id
    except Exception as exc:
        logger.warning(
            "finalize stage failed",
            extra={"item_id": item_id, "item_request_id": item_request_id, "error": str(exc)},
        )
        attempt = task_try_count(mutable_ctx)
        if FINALIZE_RETRY_POLICY.should_dead_letter(attempt):
            await route_dead_letter(
                ctx=mutable_ctx,
                task_name="finalize_item",
                item_id=item_id,
                reason=str(exc),
            )
            raise
        raise Retry(defer=FINALIZE_RETRY_POLICY.next_delay_seconds(attempt)) from exc


@timed_stage("refresh_direct_playback_link")
async def refresh_direct_playback_link(ctx: dict[str, object], item_id: str) -> str:
    """Run queued direct-play refresh work outside the route request path."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(ctx=mutable_ctx, stage="refresh_direct_playback_link", item_id=item_id)
    playback_service = await _resolve_playback_service(mutable_ctx)
    request = await playback_service.prepare_direct_playback_refresh_schedule_request(
        item_id,
        at=datetime.now(UTC),
    )
    if request is None:
        return item_id
    await playback_service.execute_scheduled_direct_playback_refresh_with_providers(
        request,
        scheduler=None,
        at=datetime.now(UTC),
    )
    return item_id


@timed_stage("refresh_selected_hls_failed_lease")
async def refresh_selected_hls_failed_lease(ctx: dict[str, object], item_id: str) -> str:
    """Run queued selected-HLS failed-lease refresh work."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(
        ctx=mutable_ctx, stage="refresh_selected_hls_failed_lease", item_id=item_id
    )
    playback_service = await _resolve_playback_service(mutable_ctx)
    await playback_service.execute_selected_hls_failed_lease_refresh_with_providers(
        item_id,
        at=datetime.now(UTC),
    )
    return item_id


@timed_stage("refresh_selected_hls_restricted_fallback")
async def refresh_selected_hls_restricted_fallback(
    ctx: dict[str, object], item_id: str
) -> str:
    """Run queued selected-HLS restricted-fallback refresh work."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(
        ctx=mutable_ctx,
        stage="refresh_selected_hls_restricted_fallback",
        item_id=item_id,
    )
    playback_service = await _resolve_playback_service(mutable_ctx)
    await playback_service.execute_selected_hls_restricted_fallback_refresh_with_providers(
        item_id,
        at=datetime.now(UTC),
    )
    return item_id


def _queue_name(settings: Settings) -> str:
    """Normalize worker queue name from environment configuration."""

    return settings.arq_queue_name.strip() or "filmu-py"


async def _resolve_item_tenant_id(ctx: dict[str, Any], *, item_id: str) -> str | None:
    """Resolve and cache tenant ownership for one media item during worker orchestration."""

    cache = cast(dict[str, str | None], ctx.setdefault("_tenant_ids_by_item_id", {}))
    if item_id in cache:
        return cache[item_id]

    try:
        UUID(str(item_id))
    except (TypeError, ValueError):
        item = await _resolve_media_service(ctx).get_item(item_id)
        tenant_id = getattr(item, "tenant_id", None) if item is not None else None
        cache[item_id] = tenant_id
        return tenant_id

    db = ctx.get("db")
    if not isinstance(db, DatabaseRuntime):
        settings = await _resolve_runtime_settings(ctx)
        db = DatabaseRuntime(settings.postgres_dsn, echo=False)
        ctx["db"] = db

    async with db.session() as session:
        result = await session.execute(select(MediaItemORM.tenant_id).where(MediaItemORM.id == item_id))
        tenant_id = result.scalar_one_or_none()
    cache[item_id] = tenant_id
    return tenant_id


def _resolve_media_service(ctx: dict[str, Any]) -> MediaService:
    """Resolve MediaService from ARQ context or construct a fallback runtime."""

    media_service = ctx.get("media_service")
    if isinstance(media_service, MediaService):
        return media_service

    db = ctx.get("db")
    if not isinstance(db, DatabaseRuntime):
        settings = _settings_from_worker_context(ctx)
        db = DatabaseRuntime(settings.postgres_dsn, echo=False)
        ctx["db"] = db

    event_bus = ctx.get("event_bus")
    if not isinstance(event_bus, EventBus):
        event_bus = EventBus()
        ctx["event_bus"] = event_bus

    resolved = MediaService(db=db, event_bus=event_bus, rate_limiter=_resolve_limiter(ctx))
    ctx["media_service"] = resolved
    return resolved


async def _resolve_playback_service(ctx: dict[str, Any]) -> PlaybackSourceService:
    """Resolve playback refresh service from worker context or construct a fallback runtime."""

    service = ctx.get("playback_service")
    if isinstance(service, PlaybackSourceService):
        return service

    settings = await _resolve_runtime_settings(ctx)
    db = ctx.get("db")
    if not isinstance(db, DatabaseRuntime):
        db = DatabaseRuntime(settings.postgres_dsn, echo=False)
        ctx["db"] = db

    service = PlaybackSourceService(
        db,
        settings=settings,
        rate_limiter=_resolve_limiter(ctx),
    )
    ctx["playback_service"] = service
    return service


def _resolve_worker_cache(ctx: dict[str, Any]) -> CacheManager:
    """Resolve cache manager from worker context or construct a fallback runtime."""

    cache = ctx.get("cache")
    if isinstance(cache, CacheManager):
        return cache

    redis = ctx.get("redis")
    if not isinstance(redis, Redis):
        settings = _settings_from_worker_context(ctx)
        redis = _redis_from_settings(settings)
        ctx["redis"] = redis

    resolved = CacheManager(redis=redis, namespace="filmu_py_worker_plugins")
    ctx["cache"] = resolved
    return resolved


def _build_worker_plugin_context_provider(
    ctx: dict[str, Any],
    *,
    settings: Settings,
) -> PluginContextProvider:
    """Build the worker-side plugin context provider from existing runtime objects."""

    ctx.setdefault("settings", settings)

    event_bus = ctx.get("event_bus")
    if not isinstance(event_bus, EventBus):
        event_bus = EventBus()
        ctx["event_bus"] = event_bus

    db = ctx.get("db")
    if not isinstance(db, DatabaseRuntime):
        db = DatabaseRuntime(settings.postgres_dsn, echo=False)
        ctx["db"] = db

    settings_source = ctx.get("plugin_settings_payload")
    if not isinstance(settings_source, dict):
        settings_source = settings.to_compatibility_dict()

    return PluginContextProvider(
        settings=settings_source,
        event_bus=event_bus,
        rate_limiter=cast("Any", _resolve_limiter(ctx)),
        cache=_resolve_worker_cache(ctx),
        logger_factory=lambda plugin_name: cast(
            "Any", structlog.get_logger(f"filmu_py.plugins.{plugin_name}")
        ),
        datasource_factory=lambda _plugin_name, datasource_name: (
            HostPluginDatasource(
                session_factory=db.session,
                http_client_factory=httpx.AsyncClient,
            )
            if datasource_name == "host"
            else None
        ),
    )


async def _resolve_plugin_registry(ctx: dict[str, Any]) -> PluginRegistry:
    """Resolve plugin registry from worker context or construct a worker-local registry."""

    settings = await _resolve_runtime_settings(ctx)
    settings_source = ctx.get("plugin_settings_payload")
    if not isinstance(settings_source, dict):
        settings_source = settings.to_compatibility_dict()
        ctx["plugin_settings_payload"] = settings_source
    snapshot = _plugin_settings_payload_snapshot(settings_source)

    plugin_registry = ctx.get("plugin_registry")
    if (
        isinstance(plugin_registry, PluginRegistry)
        and ctx.get("plugin_settings_payload_snapshot") == snapshot
    ):
        return plugin_registry

    resolved = PluginRegistry()
    context_provider = _build_worker_plugin_context_provider(ctx, settings=settings)
    await asyncio.to_thread(
        load_plugins,
        settings.plugins_dir,
        resolved,
        context_provider=context_provider,
        host_version=settings.version,
        trust_store_path=settings.plugin_trust_store_path,
        strict_signatures=(
            settings.plugin_strict_signatures
            or settings.plugin_runtime.require_strict_signatures
        ),
        runtime_policy=PluginRuntimePolicy(
            enforcement_mode=settings.plugin_runtime.enforcement_mode,
            require_strict_signatures=settings.plugin_runtime.require_strict_signatures,
            require_source_digest=settings.plugin_runtime.require_source_digest,
            allowed_non_builtin_sandbox_profiles=tuple(
                settings.plugin_runtime.allowed_non_builtin_sandbox_profiles
            ),
            allowed_non_builtin_tenancy_modes=tuple(
                settings.plugin_runtime.allowed_non_builtin_tenancy_modes
            ),
        ),
        register_graphql=False,
        register_capabilities=True,
    )
    await asyncio.to_thread(
        register_builtin_plugins,
        resolved,
        context_provider=context_provider,
    )
    context_provider.lock()
    cast(EventBus, ctx["event_bus"]).attach_plugin_runtime(resolved)
    ctx["plugin_registry"] = resolved
    ctx["plugin_settings_payload_snapshot"] = snapshot
    return resolved


def _extract_int_value(attributes: dict[str, object], *keys: str) -> int | None:
    for key in keys:
        value = attributes.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())
    return None


def _resolve_item_type(item: MediaItemRecord) -> str:
    raw_type = item.attributes.get("item_type") or item.attributes.get("media_type")
    if isinstance(raw_type, str):
        normalized = raw_type.strip().casefold()
        _ITEM_TYPE_ALIASES = {
            "movie": "movie",
            "show": "show",
            "season": "season",
            "episode": "episode",
            "tv": "show",
            "series": "show",
        }
        mapped = _ITEM_TYPE_ALIASES.get(normalized)
        if mapped is not None:
            return mapped
    if item.external_ref.startswith("tmdb:"):
        return "movie"
    return "show"


def _build_search_query(
    *,
    title: str,
    item_type: str,
    year: int | None,
    season_number: int | None,
    episode_number: int | None,
) -> str:
    """Build one scraper query that preserves season/episode scope when known."""

    if item_type in {"show", "season", "episode", "series"}:
        if season_number is not None and episode_number is not None:
            return f"{title} S{season_number:02d}E{episode_number:02d}"
        if season_number is not None:
            return f"{title} S{season_number:02d}"
        if year is not None:
            return f"{title} {year}"
    else:
        if year is not None:
            return f"{title} {year}"
    return title


def _normalize_requested_seasons(requested_seasons: list[int] | None) -> list[int] | None:
    """Return positive, deduplicated requested-season values in deterministic order."""

    if requested_seasons is None:
        return None
    normalized = sorted({season for season in requested_seasons if isinstance(season, int) and season > 0})
    return normalized or None


def _parsed_seasons_from_stream(stream: StreamORM) -> list[int] | None:
    """Return normalized parsed season numbers for one persisted stream candidate."""

    parsed_title = stream.parsed_title if isinstance(stream.parsed_title, dict) else {}
    raw_value = parsed_title.get("season")
    if raw_value is None:
        return None
    if isinstance(raw_value, int):
        return [raw_value] if raw_value > 0 else None
    if isinstance(raw_value, str) and raw_value.strip().isdigit():
        season = int(raw_value.strip())
        return [season] if season > 0 else None
    if isinstance(raw_value, list):
        seasons: set[int] = set()
        for value in raw_value:
            if isinstance(value, int) and value > 0:
                seasons.add(value)
            elif isinstance(value, str) and value.strip().isdigit():
                season = int(value.strip())
                if season > 0:
                    seasons.add(season)
        if seasons:
            return sorted(seasons)
    return None


def _parsed_episode_count_from_stream(stream: StreamORM) -> int:
    """Return the parsed episode count for one persisted stream candidate when available."""

    parsed_title = stream.parsed_title if isinstance(stream.parsed_title, dict) else {}
    raw_value = parsed_title.get("episode")
    if raw_value is None:
        return 0
    if isinstance(raw_value, int):
        return 1 if raw_value > 0 else 0
    if isinstance(raw_value, str):
        normalized = raw_value.strip()
        return 1 if normalized.isdigit() and int(normalized) > 0 else 0
    if isinstance(raw_value, list):
        episodes: set[int] = set()
        for value in raw_value:
            if isinstance(value, int) and value > 0:
                episodes.add(value)
            elif isinstance(value, str) and value.strip().isdigit():
                episode = int(value.strip())
                if episode > 0:
                    episodes.add(episode)
        return len(episodes)
    return 0


def _partial_scope_rank_bonus(stream: StreamORM, requested_seasons: list[int]) -> int:
    """Return an additive bonus favouring broader coverage for partial show requests."""

    parsed_seasons = _parsed_seasons_from_stream(stream)
    if not parsed_seasons:
        return 0

    requested = set(requested_seasons)
    covered_season_count = len(requested.intersection(parsed_seasons))
    if covered_season_count <= 0:
        return 0

    bonus = covered_season_count * _PARTIAL_SCOPE_SEASON_COVERAGE_BONUS
    episode_count = _parsed_episode_count_from_stream(stream)
    if episode_count == 0:
        return bonus + _PARTIAL_SCOPE_SEASON_PACK_BONUS
    if episode_count > 1:
        return bonus + _PARTIAL_SCOPE_MULTI_EPISODE_BONUS
    return bonus


def _apply_partial_scope_rank_bonus(
    stream: StreamORM,
    ranked: RankedTorrent,
    requested_seasons: list[int] | None,
) -> RankedTorrent:
    """Return one adjusted RTN result while preserving existing fetch/failure semantics."""

    if not requested_seasons:
        return ranked

    bonus = _partial_scope_rank_bonus(stream, requested_seasons)
    if bonus <= 0:
        return ranked

    score_parts = dict(ranked.score_parts)
    score_parts["partial_scope_bonus"] = bonus
    return RankedTorrent(
        data=ranked.data,
        rank=ranked.rank + bonus,
        lev_ratio=ranked.lev_ratio,
        fetch=ranked.fetch,
        failed_checks=ranked.failed_checks,
        score_parts=score_parts,
    )


def _partial_scope_rejection_reason(stream: StreamORM, requested_seasons: list[int]) -> str | None:
    """Return one rejection reason when a stream falls outside a partial-season request scope."""

    parsed_seasons = _parsed_seasons_from_stream(stream)
    if not parsed_seasons:
        return "partial_scope_season_missing"
    requested = set(requested_seasons)
    if requested.isdisjoint(parsed_seasons):
        return "partial_scope_season_mismatch"
    return None


def _post_rank_expected_scope_reason(item: MediaItemRecord, stream: StreamORM) -> str | None:
    """Return one rejection reason when parsed scope misses expected season/episode values."""

    expected_season = _extract_int_value(
        item.attributes,
        "season_number",
        "season",
        "parent_season_number",
    )
    expected_episode = _extract_int_value(item.attributes, "episode_number", "episode")
    parsed_title = stream.parsed_title if isinstance(stream.parsed_title, dict) else {}
    parsed = parsed_title
    parsed_season = _extract_int_value(parsed, "season")
    parsed_episode = _extract_int_value(parsed, "episode")

    if expected_season is not None and parsed_season != expected_season:
        return "season_mismatch" if parsed_season is not None else "season_missing"
    if expected_episode is not None and parsed_episode != expected_episode:
        return "episode_mismatch" if parsed_episode is not None else "episode_missing"
    return None


def _resolve_external_identifiers(item: MediaItemRecord) -> ExternalIdentifiers:
    tmdb_id = item.attributes.get("tmdb_id")
    if not isinstance(tmdb_id, str) and item.external_ref.startswith("tmdb:"):
        tmdb_id = item.external_ref.partition(":")[2] or None

    imdb_id = item.attributes.get("imdb_id")
    if not isinstance(imdb_id, str):
        if item.external_ref.startswith("tt"):
            imdb_id = item.external_ref
        elif item.external_ref.startswith("imdb:"):
            imdb_id = item.external_ref.partition(":")[2] or None
        else:
            imdb_id = None

    tvdb_id = item.attributes.get("tvdb_id")
    trakt_id = item.attributes.get("trakt_id")
    return ExternalIdentifiers(
        tmdb_id=tmdb_id if isinstance(tmdb_id, str) else None,
        tvdb_id=tvdb_id if isinstance(tvdb_id, str) else None,
        imdb_id=imdb_id if isinstance(imdb_id, str) else None,
        trakt_id=trakt_id if isinstance(trakt_id, str) else None,
    )


def _build_scraper_search_input(
    item: MediaItemRecord,
    *,
    season_override: int | None = None,
) -> ScraperSearchInput:
    """Build the search input for a scraper plugin call.

    When ``season_override`` is provided it is used in place of the
    season_number stored in item.attributes.  This allows show-level items
    (which carry no season_number attribute) to produce season-qualified
    queries (e.g. "Show Title S01") when the partial-scope context is known.
    """
    item_type = _resolve_item_type(item)
    year = _extract_int_value(item.attributes, "year", "release_year")
    season_number = season_override or _extract_int_value(
        item.attributes,
        "season_number",
        "season",
        "parent_season_number",
    )
    episode_number = _extract_int_value(item.attributes, "episode_number", "episode")
    return ScraperSearchInput(
        item_id=item.id,
        item_type=item_type,
        title=item.title,
        year=year,
        season_number=season_number,
        episode_number=episode_number,
        query=_build_search_query(
            title=item.title,
            item_type=item_type,
            year=year,
            season_number=season_number,
            episode_number=episode_number,
        ),
        external_ids=_resolve_external_identifiers(item),
        metadata=dict(item.attributes),
    )


def _scrape_candidate_from_plugin_result(
    *,
    item_id: str,
    result: PluginScraperResult,
) -> ScrapeCandidateRecord | None:
    info_hash = result.info_hash
    if info_hash is None and result.magnet_url:
        parsed = parse_qs(urlparse(result.magnet_url).query)
        for value in parsed.get("xt", []):
            if value.startswith("urn:btih:"):
                info_hash = value.partition("urn:btih:")[2]
                break
    if not info_hash:
        return None

    normalized_hash = info_hash.strip().lower()
    raw_title = result.title.strip()
    if not normalized_hash or not raw_title:
        return None

    return ScrapeCandidateRecord(
        item_id=item_id,
        info_hash=normalized_hash,
        raw_title=raw_title,
        provider=result.provider or "plugin",
        size_bytes=result.size_bytes,
    )


async def _scrape_with_plugins(
    *,
    plugin_registry: PluginRegistry,
    item: MediaItemRecord,
    partial_seasons: list[int] | None = None,
) -> tuple[list[ScrapeCandidateRecord], list[dict[str, object]]]:
    """Execute registered scraper plugins and normalize their outputs for persistence.

    For partial show requests, this fans out season-qualified search inputs so
    each requested season gets an explicit query (for example, ``"Show S01"``
    and ``"Show S03"``). Candidate filtering remains enforced downstream during
    parse/rank stages, and duplicate torrents are deduped globally by info hash.
    """

    scrapers = plugin_registry.get_scrapers()
    if not scrapers:
        return [], []

    # Build one search input per requested season. For full-scope requests,
    # run exactly one broad query with no season override.
    season_overrides: tuple[int | None, ...]
    season_overrides = (
        tuple(sorted(set(partial_seasons)))
        if partial_seasons
        else (None,)
    )

    search_inputs = [
        _build_scraper_search_input(item, season_override=season_override)
        for season_override in season_overrides
    ]

    request_specs = [(scraper, search_input) for scraper in scrapers for search_input in search_inputs]
    responses = await asyncio.gather(
        *(scraper.search(search_input) for scraper, search_input in request_specs),
        return_exceptions=True,
    )

    normalized: list[ScrapeCandidateRecord] = []
    provider_summaries: list[dict[str, object]] = []
    seen_info_hashes: set[str] = set()
    provider_candidate_counts: dict[str, int] = {}
    providers_with_errors: set[str] = set()
    provider_order: list[str] = []

    for scraper in scrapers:
        provider = _scraper_provider_name(scraper)
        if provider not in provider_order:
            provider_order.append(provider)

    for request_spec, response in zip(request_specs, responses, strict=False):
        scraper, _search_input = request_spec
        provider = _scraper_provider_name(scraper)
        if isinstance(response, Exception):
            logger.warning(
                "scrape plugin failed", extra={"item_id": item.id, "reason": str(response)}
            )
            providers_with_errors.add(provider)
            continue

        provider_candidate_counts.setdefault(provider, 0)
        for result in cast(list[PluginScraperResult], response):
            candidate = _scrape_candidate_from_plugin_result(item_id=item.id, result=result)
            if candidate is None or candidate.info_hash in seen_info_hashes:
                continue
            seen_info_hashes.add(candidate.info_hash)
            normalized.append(candidate)

            provider_candidate_counts[provider] += 1

    for provider in provider_order:
        provider_count = provider_candidate_counts.get(provider, 0)
        status = "ok" if provider_count > 0 else "error" if provider in providers_with_errors else "empty"
        provider_summaries.append(
            {
                "provider": provider,
                "candidate_count": provider_count,
                "status": status,
            }
        )

    return normalized, provider_summaries


async def poll_ongoing_shows(ctx: dict[str, object]) -> dict[str, int]:
    """Queue new scrape passes for partially completed or ongoing shows with unmet released episodes."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(ctx=mutable_ctx, stage="poll_ongoing_shows", item_id="cron")
    media_service = _resolve_media_service(mutable_ctx)
    settings = await _resolve_runtime_settings(mutable_ctx)
    arq_redis = await _resolve_arq_redis(mutable_ctx)
    queue_name = str(mutable_ctx.get("queue_name", _queue_name(settings)))

    processed_count: int = 0
    queued_count: int = 0
    items = await media_service.list_items_in_states(
        states=[ItemState.PARTIALLY_COMPLETED, ItemState.ONGOING]
    )
    for listed_item in items:
        item = listed_item if listed_item.attributes else await media_service.get_item(listed_item.id)
        if item is None or _resolve_item_type(item) != "show":
            continue

        processed_count += 1
        result = await _evaluate_show_completion(item, media_service._db, settings)
        if not result.missing_released:
            continue

        # Guard: don't double-enqueue if a scrape job is already active for this show
        if await is_scrape_item_job_active(arq_redis, item_id=item.id):
            continue
        await enqueue_scrape_item(
            arq_redis,
            item_id=item.id,
            queue_name=queue_name,
            tenant_id=item.tenant_id,
        )
        queued_count += 1

    return {"processed": processed_count, "queued": queued_count}


async def poll_unreleased_items(ctx: dict[str, object]) -> dict[str, int]:
    """Requeue unreleased items once their release date passes to preserve existing flow."""

    mutable_ctx = cast(dict[str, Any], ctx)
    bind_worker_contextvars(ctx=mutable_ctx, stage="poll_unreleased_items", item_id="cron")
    media_service = _resolve_media_service(mutable_ctx)
    settings = await _resolve_runtime_settings(mutable_ctx)
    arq_redis = await _resolve_arq_redis(mutable_ctx)
    queue_name = str(mutable_ctx.get("queue_name", _queue_name(settings)))
    db = cast(DatabaseRuntime, mutable_ctx.get("db"))

    processed_count: int = 0
    transitioned_count: int = 0
    async with db.session() as session:
        result = await session.execute(
            select(MediaItemORM).where(MediaItemORM.state == ItemState.UNRELEASED.value)
        )
        items = result.scalars().all()

        for item in items:
            processed_count += 1
            aired_at = cast(dict[str, object], item.attributes or {}).get("aired_at")
            release_dt = _parse_calendar_datetime(aired_at) if isinstance(aired_at, str) else None
            if release_dt is None or release_dt > datetime.now(UTC):
                continue

            await _try_transition(
                media_service=media_service,
                item_id=str(item.id),
                event=ItemEvent.INDEX,
                message="unreleased item now available",
            )
            await enqueue_index_item(
                arq_redis,
                item_id=str(item.id),
                queue_name=queue_name,
                tenant_id=item.tenant_id,
                job_id=index_item_followup_job_id(
                    str(item.id),
                    discriminator="release-poll",
                ),
            )
            transitioned_count += 1

    return {"processed": processed_count, "transitioned": transitioned_count}


async def on_startup(ctx: dict[str, Any]) -> None:
    """Initialize shared worker context objects once per worker process."""

    settings = get_settings()
    queue_name = _queue_name(settings)
    ctx["queue_name"] = queue_name
    ctx["redis"] = _redis_from_settings(settings)
    ctx["rate_limiter"] = DistributedRateLimiter(redis=cast(Redis, ctx["redis"]))
    ctx["arq_redis"] = await create_pool(_redis_settings(settings), default_queue_name=queue_name)
    ctx["db"] = DatabaseRuntime(settings.postgres_dsn, echo=False)
    ctx["event_bus"] = EventBus()
    ctx["cache"] = CacheManager(
        redis=cast(Redis, ctx["redis"]), namespace="filmu_py_worker_plugins"
    )
    ctx["media_service"] = MediaService(
        db=cast(DatabaseRuntime, ctx["db"]),
        event_bus=cast(EventBus, ctx["event_bus"]),
        rate_limiter=cast(DistributedRateLimiter, ctx["rate_limiter"]),
    )
    ctx["plugin_registry"] = await _resolve_plugin_registry(ctx)


async def on_shutdown(ctx: dict[str, Any]) -> None:
    """Release worker-owned runtime resources."""

    db = ctx.get("db")
    if isinstance(db, DatabaseRuntime):
        database_runtime = db
        await database_runtime.dispose()

    redis = ctx.get("redis")
    if isinstance(redis, Redis):
        redis_client = redis
        await redis_client.aclose()

    arq_redis = ctx.get("arq_redis")
    if isinstance(arq_redis, ArqRedis):
        await arq_redis.aclose()

    for executor in _HEAVY_STAGE_EXECUTORS.values():
        executor.shutdown(wait=False, cancel_futures=True)
    _HEAVY_STAGE_EXECUTORS.clear()


def build_worker_settings(settings: Settings | None = None) -> dict[str, Any]:
    """Create ARQ worker settings object consumable by ``arq.worker.run_worker``."""

    current = settings or get_settings()
    redis_conn_settings = RedisSettings.from_dsn(str(current.redis_url))

    queue_name = _queue_name(current)

    return {
        "functions": [
            index_item,
            scrape_item,
            parse_scrape_results,
            rank_streams,
            debrid_item,
            finalize_item,
            refresh_direct_playback_link,
            refresh_selected_hls_failed_lease,
            refresh_selected_hls_restricted_fallback,
            backfill_imdb_ids,
            poll_content_services,
            recover_incomplete_library,
            retry_library,
            poll_unreleased_items,
            poll_ongoing_shows,
            publish_outbox_events,
        ],
        "cron_jobs": [
            cron(
                poll_unreleased_items,
                name="poll_unreleased_items",
                hour=_ongoing_show_poll_hours(current),
                minute={30},
                second=0,
                unique=True,
                job_id="poll-unreleased-items",
            ),
            cron(
                poll_ongoing_shows,
                name="poll_ongoing_shows",
                hour=_ongoing_show_poll_hours(current),
                minute={0},
                second=0,
                unique=True,
                job_id="poll-ongoing-shows",
            ),
            cron(
                publish_outbox_events,
                name="publish_outbox_events",
                second={0, 30},
                unique=True,
                job_id="publish-outbox-events",
            ),
            cron(
                recover_incomplete_library,
                name="recover_incomplete_library",
                minute=set(range(0, 60, 5)),
                second=15,
                unique=True,
                job_id="recover-incomplete-library",
            ),
            cron(
                retry_library,
                name="retry_library",
                hour={0} if current.retry_interval == 86400 else None,
                minute={0} if current.retry_interval == 86400 else None,
                second=0,
                unique=True,
                job_id="retry-library",
            ),
            cron(
                poll_content_services,
                name="poll_content_services",
                minute={0, 30},
                second=0,
                unique=True,
                job_id="poll-content-services",
            ),
        ],
        "queue_name": queue_name,
        "max_jobs": current.arq_max_jobs,
        "redis_settings": redis_conn_settings,
        "on_startup": on_startup,
        "on_shutdown": on_shutdown,
        "job_timeout": current.arq_job_timeout_seconds,
        "ctx": {"queue_name": queue_name},
    }


async def run_worker(settings: Settings | None = None) -> Worker:
    """Start an ARQ worker instance for the configured scrape/debrid/finalize pipeline."""

    worker_settings = build_worker_settings(settings)
    worker = Worker(**worker_settings)
    await worker.main()
    return worker


def run_worker_entrypoint() -> None:
    """Synchronous entrypoint used by script runners to launch ARQ worker loop."""

    import asyncio

    asyncio.run(run_worker())


if __name__ == "__main__":
    run_worker_entrypoint()
