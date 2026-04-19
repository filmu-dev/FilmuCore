"""Async dispatch helpers for plugin event hook workers."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from time import perf_counter, time
from typing import Any

from prometheus_client import Counter, Histogram

from filmu_py.plugins.interfaces import PluginEventHookWorker

logger = logging.getLogger(__name__)

PLUGIN_HOOK_INVOCATIONS_TOTAL = Counter(
    "filmu_py_plugin_hook_invocations_total",
    "Plugin hook worker invocations",
    ["plugin_name", "event_type", "outcome"],
)
PLUGIN_HOOK_DURATION_SECONDS = Histogram(
    "filmu_py_plugin_hook_duration_seconds",
    "Plugin hook execution duration",
    ["plugin_name", "event_type"],
    buckets=[0.01, 0.1, 0.5, 1.0, 2.5, 5.0],
)


def queued_plugin_hook_job_id(
    *,
    plugin_name: str,
    event_type: str,
    payload: dict[str, Any],
) -> str:
    """Return a stable queued plugin-hook job identifier for one plugin/event delivery."""

    identity_parts: list[str] = [plugin_name, event_type]
    for field_name in ("tenant_id", "item_id", "item_request_id", "event_id"):
        raw_value = payload.get(field_name)
        if isinstance(raw_value, str) and raw_value.strip():
            identity_parts.append(f"{field_name}={raw_value.strip()}")
    payload_digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()[:16]
    identity_parts.append(payload_digest)
    return "plugin-hook:" + ":".join(identity_parts)


@dataclass(slots=True)
class PluginHookWorkerExecutor:
    """Fan out published events to registered plugin hook workers safely."""

    timeout_seconds: float = 5.0
    _background_tasks: set[asyncio.Task[PluginHookInvocationResult]] = field(default_factory=set)

    def _matching_hooks(
        self,
        event_type: str,
        hooks: Sequence[PluginEventHookWorker],
    ) -> list[PluginEventHookWorker]:
        return [hook for hook in hooks if event_type in hook.subscribed_events]

    def _create_invoke_tasks(
        self,
        event_type: str,
        payload: dict[str, Any],
        hooks: Sequence[PluginEventHookWorker],
    ) -> list[asyncio.Task[PluginHookInvocationResult]]:
        tasks: list[asyncio.Task[PluginHookInvocationResult]] = []
        for hook in self._matching_hooks(event_type, hooks):
            task = asyncio.create_task(self._safe_invoke(hook, event_type, deepcopy(payload)))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
            tasks.append(task)
        return tasks

    async def dispatch(
        self,
        event_type: str,
        payload: dict[str, Any],
        hooks: Sequence[PluginEventHookWorker],
    ) -> None:
        self._create_invoke_tasks(event_type, payload, hooks)

    async def dispatch_and_wait(
        self,
        event_type: str,
        payload: dict[str, Any],
        hooks: Sequence[PluginEventHookWorker],
    ) -> list[PluginHookInvocationResult]:
        """Dispatch one event to matching hooks and await completion."""

        tasks = self._create_invoke_tasks(event_type, payload, hooks)
        if tasks:
            return list(await asyncio.gather(*tasks))
        return []

    async def _safe_invoke(
        self,
        hook: PluginEventHookWorker,
        event_type: str,
        payload: dict[str, Any],
    ) -> PluginHookInvocationResult:
        plugin_name = hook.plugin_name
        started_at = perf_counter()
        try:
            await asyncio.wait_for(hook.handle(event_type, payload), timeout=self.timeout_seconds)
        except TimeoutError as exc:
            PLUGIN_HOOK_INVOCATIONS_TOTAL.labels(
                plugin_name=plugin_name,
                event_type=event_type,
                outcome="timeout",
            ).inc()
            logger.warning(
                "plugin_hook_failed",
                extra={
                    "hook": type(hook).__name__,
                    "exc": str(exc),
                    "event_type": event_type,
                    "plugin_name": plugin_name,
                },
            )
            return PluginHookInvocationResult(
                plugin_name=plugin_name,
                event_type=event_type,
                outcome="timeout",
                duration_seconds=perf_counter() - started_at,
                error=str(exc),
            )
        except Exception as exc:
            PLUGIN_HOOK_INVOCATIONS_TOTAL.labels(
                plugin_name=plugin_name,
                event_type=event_type,
                outcome="error",
            ).inc()
            logger.warning(
                "plugin_hook_failed",
                extra={
                    "hook": type(hook).__name__,
                    "exc": str(exc),
                    "event_type": event_type,
                    "plugin_name": plugin_name,
                },
            )
            return PluginHookInvocationResult(
                plugin_name=plugin_name,
                event_type=event_type,
                outcome="error",
                duration_seconds=perf_counter() - started_at,
                error=str(exc),
            )
        else:
            PLUGIN_HOOK_INVOCATIONS_TOTAL.labels(
                plugin_name=plugin_name,
                event_type=event_type,
                outcome="success",
            ).inc()
            return PluginHookInvocationResult(
                plugin_name=plugin_name,
                event_type=event_type,
                outcome="success",
                duration_seconds=perf_counter() - started_at,
            )
        finally:
            PLUGIN_HOOK_DURATION_SECONDS.labels(
                plugin_name=plugin_name,
                event_type=event_type,
            ).observe(perf_counter() - started_at)


@dataclass(frozen=True, slots=True)
class PluginHookInvocationResult:
    """Observed outcome of one plugin-hook invocation."""

    plugin_name: str
    event_type: str
    outcome: str
    duration_seconds: float
    error: str | None = None


@dataclass(slots=True)
class QueuedPluginHookDispatcher:
    """Queue-backed plugin-hook dispatcher with bounded fallback to in-process execution."""

    arq_redis: Any
    queue_name: str
    queued_events: frozenset[str]
    fallback_executor: PluginHookWorkerExecutor
    fallback_enabled: bool = True

    async def dispatch(
        self,
        event_type: str,
        payload: dict[str, Any],
        hooks: Sequence[PluginEventHookWorker],
    ) -> None:
        matching_hooks = self.fallback_executor._matching_hooks(event_type, hooks)
        if not matching_hooks:
            return
        if event_type not in self.queued_events:
            await self.fallback_executor.dispatch(event_type, payload, matching_hooks)
            return

        fallback_hooks: list[PluginEventHookWorker] = []
        for hook in matching_hooks:
            plugin_name = hook.plugin_name
            try:
                enqueued = await self.arq_redis.enqueue_job(
                    "dispatch_plugin_hook_event",
                    plugin_name,
                    event_type,
                    deepcopy(payload),
                    time(),
                    _job_id=queued_plugin_hook_job_id(
                        plugin_name=plugin_name,
                        event_type=event_type,
                        payload=payload,
                    ),
                    _queue_name=self.queue_name,
                )
            except Exception:
                logger.warning(
                    "plugin_hook_queue_dispatch_failed",
                    extra={
                        "plugin_name": plugin_name,
                        "event_type": event_type,
                    },
                    exc_info=True,
                )
                enqueued = None
            if enqueued is None and self.fallback_enabled:
                fallback_hooks.append(hook)
        if fallback_hooks:
            await self.fallback_executor.dispatch(event_type, payload, fallback_hooks)
