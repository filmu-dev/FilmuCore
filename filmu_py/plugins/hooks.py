"""Async dispatch helpers for plugin event hook workers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from time import perf_counter
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


@dataclass(slots=True)
class PluginHookWorkerExecutor:
    """Fan out published events to registered plugin hook workers safely."""

    timeout_seconds: float = 5.0
    _background_tasks: set[asyncio.Task[None]] = field(default_factory=set)

    async def dispatch(
        self,
        event_type: str,
        payload: dict[str, Any],
        hooks: Sequence[PluginEventHookWorker],
    ) -> None:
        for hook in hooks:
            if event_type in hook.subscribed_events:
                task = asyncio.create_task(self._safe_invoke(hook, event_type, deepcopy(payload)))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)

    async def _safe_invoke(
        self,
        hook: PluginEventHookWorker,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
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
        else:
            PLUGIN_HOOK_INVOCATIONS_TOTAL.labels(
                plugin_name=plugin_name,
                event_type=event_type,
                outcome="success",
            ).inc()
        finally:
            PLUGIN_HOOK_DURATION_SECONDS.labels(
                plugin_name=plugin_name,
                event_type=event_type,
            ).observe(perf_counter() - started_at)
