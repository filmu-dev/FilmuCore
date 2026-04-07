"""In-memory log history and live fan-out for compatibility streaming endpoints."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from prometheus_client import Counter

LOG_STREAM_DROPPED_EVENTS = Counter(
    "filmu_py_log_stream_dropped_events_total",
    "Number of dropped log stream events by reason.",
    labelnames=("reason",),
)


@dataclass(frozen=True, slots=True)
class LogEvent:
    """Structured log event retained for history and SSE delivery."""

    timestamp: str
    level: str
    message: str
    event: str
    logger: str | None = None
    worker_id: str | None = None
    item_id: str | None = None
    stage: str | None = None
    extra: dict[str, Any] | None = None

    def as_history_line(self) -> str:
        """Return a human-readable line for historical log responses."""

        return f"{self.timestamp} [{self.level}] {self.message}"

    def as_payload(self) -> dict[str, str]:
        """Return the structured payload sent over live SSE streams."""

        return {
            "timestamp": self.timestamp,
            "level": self.level,
            "message": self.message,
        }


def _severity_value(level: str) -> int:
    return {
        "CRITICAL": 50,
        "ERROR": 40,
        "WARNING": 30,
        "INFO": 20,
        "DEBUG": 10,
        "NOTSET": 0,
    }.get(level.upper(), 20)


class LogStreamBroker:
    """Maintain bounded log history and broadcast live log events to subscribers."""

    def __init__(self, *, max_history: int = 500, max_queue_size: int = 1_000) -> None:
        self._history: deque[LogEvent] = deque(maxlen=max_history)
        self._subscribers: list[asyncio.Queue[LogEvent]] = []
        self._max_queue_size = max_queue_size

    def record(
        self,
        *,
        level: str,
        message: str,
        timestamp: str | None = None,
        event: str | None = None,
        logger: str | None = None,
        worker_id: str | None = None,
        item_id: str | None = None,
        stage: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Append one log event to history and broadcast it to active subscribers."""

        log_event = LogEvent(
            timestamp=timestamp or datetime.now(UTC).isoformat(),
            level=level,
            message=message,
            event=event or message,
            logger=logger,
            worker_id=worker_id,
            item_id=item_id,
            stage=stage,
            extra=dict(extra or {}),
        )
        self._history.append(log_event)

        dead_queues: list[asyncio.Queue[LogEvent]] = []
        for queue in self._subscribers:
            try:
                queue.put_nowait(log_event)
            except asyncio.QueueFull:
                LOG_STREAM_DROPPED_EVENTS.labels(reason="queue_full").inc()
                dead_queues.append(queue)

        for queue in dead_queues:
            if queue in self._subscribers:
                self._subscribers.remove(queue)

    def history(self) -> list[str]:
        """Return bounded historical log lines in chronological order."""

        return [event.as_history_line() for event in self._history]

    async def subscribe(self) -> AsyncIterator[dict[str, str]]:
        """Yield live structured log payloads for SSE streaming."""

        queue: asyncio.Queue[LogEvent] = asyncio.Queue(maxsize=self._max_queue_size)
        self._subscribers.append(queue)
        try:
            while True:
                yield (await queue.get()).as_payload()
        finally:
            if queue in self._subscribers:
                self._subscribers.remove(queue)

    async def subscribe_events(
        self,
        *,
        level: str | None = None,
        item_id: str | None = None,
    ) -> AsyncIterator[LogEvent]:
        """Yield live structured log events with optional level and item filters."""

        threshold = _severity_value(level or "INFO")
        queue: asyncio.Queue[LogEvent] = asyncio.Queue(maxsize=self._max_queue_size)
        self._subscribers.append(queue)
        try:
            while True:
                event = await queue.get()
                if _severity_value(event.level) < threshold:
                    continue
                if item_id is not None and event.item_id != item_id:
                    continue
                yield event
        finally:
            if queue in self._subscribers:
                self._subscribers.remove(queue)
