"""Queued and in-process playback refresh controller implementations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any, cast

from filmu_py.services.playback import (
    DirectPlaybackRefreshControlPlaneTriggerResult,
    DirectPlaybackRefreshScheduleRequest,
    DirectPlaybackRefreshSchedulingResult,
    HlsFailedLeaseRefreshControlPlaneTriggerResult,
    HlsFailedLeaseRefreshResult,
    HlsRestrictedFallbackRefreshControlPlaneTriggerResult,
    HlsRestrictedFallbackRefreshResult,
    PlaybackAttachmentProviderClient,
)


class _QueuedRefreshControllerBase:
    """Shared bounded duplicate-suppression for queued route-trigger refresh work."""

    def __init__(self, arq_redis: object, *, queue_name: str) -> None:
        self._arq_redis = arq_redis
        self._queue_name = queue_name
        self._pending_deadline_by_item_identifier: dict[str, float] = {}

    @staticmethod
    def _pending_deadline_seconds() -> float:
        return 300.0

    def has_pending(self, item_identifier: str) -> bool:
        deadline = self._pending_deadline_by_item_identifier.get(item_identifier)
        if deadline is None:
            return False
        if monotonic() >= deadline:
            self._pending_deadline_by_item_identifier.pop(item_identifier, None)
            return False
        return True

    async def shutdown(self) -> None:
        self._pending_deadline_by_item_identifier.clear()
        return None

    def _mark_enqueued(self, item_identifier: str, *, enqueued: bool) -> None:
        if enqueued:
            self._pending_deadline_by_item_identifier[item_identifier] = (
                monotonic() + self._pending_deadline_seconds()
            )


class QueuedDirectPlaybackRefreshController(_QueuedRefreshControllerBase):
    """Queue-backed dispatcher for direct-play refresh work."""

    def __init__(self, arq_redis: object, *, queue_name: str) -> None:
        super().__init__(arq_redis, queue_name=queue_name)
        self._last_results_by_item_identifier: dict[str, DirectPlaybackRefreshSchedulingResult] = {}

    def get_last_result(self, item_identifier: str) -> DirectPlaybackRefreshSchedulingResult | None:
        return self._last_results_by_item_identifier.get(item_identifier)

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> DirectPlaybackRefreshControlPlaneTriggerResult:
        _ = at
        from filmu_py.workers.tasks import enqueue_refresh_direct_playback_link

        enqueued = await enqueue_refresh_direct_playback_link(
            cast(Any, self._arq_redis),
            item_id=item_identifier,
            queue_name=self._queue_name,
        )
        existing_result = self._last_results_by_item_identifier.get(item_identifier)
        still_pending = self.has_pending(item_identifier) and existing_result is not None
        if enqueued:
            scheduling_result = DirectPlaybackRefreshSchedulingResult(outcome="scheduled")
        elif still_pending:
            assert existing_result is not None
            scheduling_result = existing_result
        else:
            scheduling_result = DirectPlaybackRefreshSchedulingResult(outcome="no_action")
        self._last_results_by_item_identifier[item_identifier] = scheduling_result
        self._mark_enqueued(item_identifier, enqueued=enqueued)
        return DirectPlaybackRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled" if enqueued else ("already_pending" if still_pending else "no_action"),
            scheduling_result=scheduling_result,
        )


class QueuedHlsFailedLeaseRefreshController(_QueuedRefreshControllerBase):
    """Queue-backed dispatcher for selected-HLS failed-lease refresh work."""

    def __init__(self, arq_redis: object, *, queue_name: str) -> None:
        super().__init__(arq_redis, queue_name=queue_name)
        self._last_results_by_item_identifier: dict[str, HlsFailedLeaseRefreshResult] = {}

    def get_last_result(self, item_identifier: str) -> HlsFailedLeaseRefreshResult | None:
        return self._last_results_by_item_identifier.get(item_identifier)

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> HlsFailedLeaseRefreshControlPlaneTriggerResult:
        _ = at
        from filmu_py.workers.tasks import enqueue_refresh_selected_hls_failed_lease

        enqueued = await enqueue_refresh_selected_hls_failed_lease(
            cast(Any, self._arq_redis),
            item_id=item_identifier,
            queue_name=self._queue_name,
        )
        existing_result = self._last_results_by_item_identifier.get(item_identifier)
        still_pending = self.has_pending(item_identifier) and existing_result is not None
        if enqueued:
            refresh_result = HlsFailedLeaseRefreshResult(
                item_identifier=item_identifier,
                outcome="scheduled",
            )
        elif still_pending:
            assert existing_result is not None
            refresh_result = existing_result
        else:
            refresh_result = HlsFailedLeaseRefreshResult(
                item_identifier=item_identifier,
                outcome="no_action",
            )
        self._last_results_by_item_identifier[item_identifier] = refresh_result
        self._mark_enqueued(item_identifier, enqueued=enqueued)
        return HlsFailedLeaseRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled" if enqueued else ("already_pending" if still_pending else "no_action"),
            refresh_result=refresh_result,
        )


class QueuedHlsRestrictedFallbackRefreshController(_QueuedRefreshControllerBase):
    """Queue-backed dispatcher for selected-HLS restricted-fallback refresh work."""

    def __init__(self, arq_redis: object, *, queue_name: str) -> None:
        super().__init__(arq_redis, queue_name=queue_name)
        self._last_results_by_item_identifier: dict[str, HlsRestrictedFallbackRefreshResult] = {}

    def get_last_result(
        self,
        item_identifier: str,
    ) -> HlsRestrictedFallbackRefreshResult | None:
        return self._last_results_by_item_identifier.get(item_identifier)

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> HlsRestrictedFallbackRefreshControlPlaneTriggerResult:
        _ = at
        from filmu_py.workers.tasks import enqueue_refresh_selected_hls_restricted_fallback

        enqueued = await enqueue_refresh_selected_hls_restricted_fallback(
            cast(Any, self._arq_redis),
            item_id=item_identifier,
            queue_name=self._queue_name,
        )
        existing_result = self._last_results_by_item_identifier.get(item_identifier)
        still_pending = self.has_pending(item_identifier) and existing_result is not None
        if enqueued:
            refresh_result = HlsRestrictedFallbackRefreshResult(
                item_identifier=item_identifier,
                outcome="scheduled",
            )
        elif still_pending:
            assert existing_result is not None
            refresh_result = existing_result
        else:
            refresh_result = HlsRestrictedFallbackRefreshResult(
                item_identifier=item_identifier,
                outcome="no_action",
            )
        self._last_results_by_item_identifier[item_identifier] = refresh_result
        self._mark_enqueued(item_identifier, enqueued=enqueued)
        return HlsRestrictedFallbackRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled" if enqueued else ("already_pending" if still_pending else "no_action"),
            refresh_result=refresh_result,
        )


class _InProcessControllerBase:
    """Shared task lifecycle handling for fire-and-forget in-process refresh controllers."""

    def __init__(self, *, sleep: Callable[[float], Awaitable[None]] | None = None) -> None:
        self._sleep = sleep or asyncio.sleep
        self._tasks_by_item_identifier: dict[str, asyncio.Task[None]] = {}

    def _register_task(self, item_identifier: str, task: asyncio.Task[None]) -> None:
        self._tasks_by_item_identifier[item_identifier] = task

        def cleanup(done_task: asyncio.Task[None], *, key: str = item_identifier) -> None:
            current = self._tasks_by_item_identifier.get(key)
            if current is done_task:
                self._tasks_by_item_identifier.pop(key, None)

        task.add_done_callback(cleanup)

    def has_pending(self, item_identifier: str) -> bool:
        task = self._tasks_by_item_identifier.get(item_identifier)
        return task is not None and not task.done()

    async def wait_for_item(self, item_identifier: str) -> None:
        while True:
            task = self._tasks_by_item_identifier.get(item_identifier)
            if task is None:
                return
            done, _pending = await asyncio.wait({task}, return_when=asyncio.ALL_COMPLETED)
            next(iter(done)).result()
            current = self._tasks_by_item_identifier.get(item_identifier)
            if current is task and task.done():
                self._tasks_by_item_identifier.pop(item_identifier, None)

    async def shutdown(self) -> None:
        tasks = [task for task in self._tasks_by_item_identifier.values() if not task.done()]
        if not tasks:
            self._tasks_by_item_identifier.clear()
            return

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks_by_item_identifier.clear()


class InProcessDirectPlaybackRefreshController(_InProcessControllerBase):
    """Small in-process caller that triggers scheduled direct-play refresh work in the background."""

    def __init__(
        self,
        playback_service: Any,
        *,
        executors: dict[str, Any] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: Any = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        super().__init__(sleep=sleep)
        self._playback_service = playback_service
        self._executors = executors
        self._provider_clients = provider_clients
        self._rate_limiter = rate_limiter
        self._last_results_by_item_identifier: dict[str, DirectPlaybackRefreshSchedulingResult] = {}

    def get_last_result(self, item_identifier: str) -> DirectPlaybackRefreshSchedulingResult | None:
        return self._last_results_by_item_identifier.get(item_identifier)

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> DirectPlaybackRefreshControlPlaneTriggerResult:
        if self.has_pending(item_identifier):
            return DirectPlaybackRefreshControlPlaneTriggerResult(
                item_identifier=item_identifier,
                outcome="already_pending",
                scheduling_result=self.get_last_result(item_identifier),
            )

        scheduling_result = await self._playback_service.schedule_direct_playback_refresh(
            item_identifier,
            scheduler=self,
            at=at,
        )
        self._last_results_by_item_identifier[item_identifier] = scheduling_result

        if scheduling_result.outcome != "scheduled" or scheduling_result.scheduled_request is None:
            return DirectPlaybackRefreshControlPlaneTriggerResult(
                item_identifier=item_identifier,
                outcome="no_action",
                scheduling_result=scheduling_result,
            )

        return DirectPlaybackRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled",
            scheduling_result=scheduling_result,
            scheduled_request=scheduling_result.scheduled_request,
        )

    async def schedule(self, request: DirectPlaybackRefreshScheduleRequest) -> None:
        if self.has_pending(request.item_identifier):
            return

        task = asyncio.create_task(
            self._run_request(request),
            name=f"direct-play-refresh:{request.item_identifier}",
        )
        self._register_task(request.item_identifier, task)

    async def _run_request(self, request: DirectPlaybackRefreshScheduleRequest) -> None:
        now = datetime.now(UTC)
        effective_run_at = request.not_before if request.not_before > now else now
        delay_seconds = max(0.0, (effective_run_at - now).total_seconds())
        if delay_seconds > 0.0:
            await self._sleep(delay_seconds)

        result = (
            await self._playback_service.execute_scheduled_direct_playback_refresh_with_providers(
                request,
                scheduler=None,
                executors=self._executors,
                provider_clients=self._provider_clients,
                rate_limiter=self._rate_limiter,
                at=effective_run_at,
            )
        )
        self._last_results_by_item_identifier[request.item_identifier] = result

        follow_up_request = result.scheduled_request
        if follow_up_request is None:
            return

        current_task = asyncio.current_task()
        if current_task is not None:
            current = self._tasks_by_item_identifier.get(request.item_identifier)
            if current is current_task:
                self._tasks_by_item_identifier.pop(request.item_identifier, None)

        await self.schedule(follow_up_request)


class InProcessHlsFailedLeaseRefreshController(_InProcessControllerBase):
    """Small in-process caller that refreshes selected failed HLS leases in the background."""

    def __init__(
        self,
        playback_service: Any,
        *,
        executors: dict[str, Any] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: Any = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        super().__init__(sleep=sleep)
        self._playback_service = playback_service
        self._executors = executors
        self._provider_clients = provider_clients
        self._rate_limiter = rate_limiter
        self._last_results_by_item_identifier: dict[str, HlsFailedLeaseRefreshResult] = {}

    def get_last_result(self, item_identifier: str) -> HlsFailedLeaseRefreshResult | None:
        return self._last_results_by_item_identifier.get(item_identifier)

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> HlsFailedLeaseRefreshControlPlaneTriggerResult:
        if self.has_pending(item_identifier):
            return HlsFailedLeaseRefreshControlPlaneTriggerResult(
                item_identifier=item_identifier,
                outcome="already_pending",
                refresh_result=self.get_last_result(item_identifier),
            )

        task = asyncio.create_task(
            self._run_item(item_identifier, at=at),
            name=f"hls-failed-lease-refresh:{item_identifier}",
        )
        self._register_task(item_identifier, task)
        return HlsFailedLeaseRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled",
        )

    async def _run_item(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        requested_at = at or datetime.now(UTC)
        normalized_retry_after = max(0.0, retry_after_seconds or 0.0)
        if normalized_retry_after > 0.0:
            await self._sleep(normalized_retry_after)
            requested_at = requested_at + timedelta(seconds=normalized_retry_after)

        result = (
            await self._playback_service.execute_selected_hls_failed_lease_refresh_with_providers(
                item_identifier,
                executors=self._executors,
                provider_clients=self._provider_clients,
                rate_limiter=self._rate_limiter,
                at=requested_at,
            )
        )
        self._last_results_by_item_identifier[item_identifier] = result

        if result.outcome != "run_later" or result.retry_after_seconds is None:
            return

        current_task = asyncio.current_task()
        if current_task is not None:
            current = self._tasks_by_item_identifier.get(item_identifier)
            if current is current_task:
                self._tasks_by_item_identifier.pop(item_identifier, None)

        follow_up_task = asyncio.create_task(
            self._run_item(
                item_identifier,
                at=requested_at,
                retry_after_seconds=result.retry_after_seconds,
            ),
            name=f"hls-failed-lease-refresh:{item_identifier}",
        )
        self._register_task(item_identifier, follow_up_task)


class InProcessHlsRestrictedFallbackRefreshController(_InProcessControllerBase):
    """Small in-process caller that refreshes selected HLS restricted-fallback winners in the background."""

    def __init__(
        self,
        playback_service: Any,
        *,
        executors: dict[str, Any] | None = None,
        provider_clients: dict[str, PlaybackAttachmentProviderClient] | None = None,
        rate_limiter: Any = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        super().__init__(sleep=sleep)
        self._playback_service = playback_service
        self._executors = executors
        self._provider_clients = provider_clients
        self._rate_limiter = rate_limiter
        self._last_results_by_item_identifier: dict[str, HlsRestrictedFallbackRefreshResult] = {}

    def get_last_result(self, item_identifier: str) -> HlsRestrictedFallbackRefreshResult | None:
        return self._last_results_by_item_identifier.get(item_identifier)

    async def trigger(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
    ) -> HlsRestrictedFallbackRefreshControlPlaneTriggerResult:
        if self.has_pending(item_identifier):
            return HlsRestrictedFallbackRefreshControlPlaneTriggerResult(
                item_identifier=item_identifier,
                outcome="already_pending",
                refresh_result=self.get_last_result(item_identifier),
            )

        task = asyncio.create_task(
            self._run_item(item_identifier, at=at),
            name=f"hls-restricted-fallback-refresh:{item_identifier}",
        )
        self._register_task(item_identifier, task)
        return HlsRestrictedFallbackRefreshControlPlaneTriggerResult(
            item_identifier=item_identifier,
            outcome="scheduled",
        )

    async def _run_item(
        self,
        item_identifier: str,
        *,
        at: datetime | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        requested_at = at or datetime.now(UTC)
        normalized_retry_after = max(0.0, retry_after_seconds or 0.0)
        if normalized_retry_after > 0.0:
            await self._sleep(normalized_retry_after)
            requested_at = requested_at + timedelta(seconds=normalized_retry_after)

        result = await self._playback_service.execute_selected_hls_restricted_fallback_refresh_with_providers(
            item_identifier,
            executors=self._executors,
            provider_clients=self._provider_clients,
            rate_limiter=self._rate_limiter,
            at=requested_at,
        )
        self._last_results_by_item_identifier[item_identifier] = result

        if result.outcome != "run_later" or result.retry_after_seconds is None:
            return

        current_task = asyncio.current_task()
        if current_task is not None:
            current = self._tasks_by_item_identifier.get(item_identifier)
            if current is current_task:
                self._tasks_by_item_identifier.pop(item_identifier, None)

        follow_up_task = asyncio.create_task(
            self._run_item(
                item_identifier,
                at=requested_at,
                retry_after_seconds=result.retry_after_seconds,
            ),
            name=f"hls-restricted-fallback-refresh:{item_identifier}",
        )
        self._register_task(item_identifier, follow_up_task)
