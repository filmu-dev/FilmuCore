from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from filmu_py.config import Settings
from filmu_py.db.models import ItemRequestORM, MediaItemORM
from filmu_py.services.media import MediaItemRecord, ShowCompletionResult, _evaluate_show_completion
from filmu_py.state.item import ItemEvent, ItemState
from filmu_py.workers import tasks


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY="a" * 32,
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL="redis://localhost:6379/0",
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
    )


@pytest.fixture(autouse=True)
def _show_completion_runtime_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tasks, "get_settings", _build_settings)
    async def _no_persisted_settings(_db: Any) -> None:
        return None

    monkeypatch.setattr(tasks, "load_settings", _no_persisted_settings)


@dataclass
class _FakeShowSession:
    latest_request: ItemRequestORM | None
    active_item_ids: set[str]
    episode_items_by_id: dict[str, MediaItemORM]
    show_orm: object | None = None

    async def __aenter__(self) -> _FakeShowSession:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    async def execute(self, stmt: object) -> Any:
        stmt_text = str(stmt).lower()

        class _ScalarResult:
            def __init__(self, values: list[object]) -> None:
                self._values = values

            def scalars(self) -> _ScalarResult:
                return self

            def all(self) -> list[object]:
                return list(self._values)

            def scalar_one_or_none(self) -> object | None:
                return self._values[0] if self._values else None

        if "from item_requests" in stmt_text:
            values = [] if self.latest_request is None else [self.latest_request]
            return _ScalarResult(values)
        if "from shows" in stmt_text:
            values = [] if self.show_orm is None else [self.show_orm]
            return _ScalarResult(values)
        # Satisfaction check now queries media_entries (download URLs) not active_streams
        if "from media_entries" in stmt_text:
            return _ScalarResult(list(self.active_item_ids))
        raise AssertionError(f"unexpected statement: {stmt_text}")

    async def get(self, model: object, item_id: str) -> MediaItemORM | None:
        _ = model
        return self.episode_items_by_id.get(str(item_id))


@dataclass
class _FakeShowDb:
    session_obj: _FakeShowSession

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_FakeShowSession]:
        yield self.session_obj


def _build_episode_item(item_id: str, *, aired_at: str | None) -> MediaItemORM:
    attributes: dict[str, object] = {"item_type": "episode"}
    if aired_at is not None:
        attributes["aired_at"] = aired_at
    return MediaItemORM(
        id=item_id,
        external_ref=f"tvdb:{item_id}",
        title=f"Episode {item_id}",
        state=ItemState.DOWNLOADED.value,
        attributes=attributes,
    )


def test_evaluate_show_completion_excludes_unreleased_episodes_from_missing_count() -> None:
    now = datetime.now(UTC)
    released_episode_id = "episode-1"
    future_episode_id = "episode-2"
    item = MediaItemRecord(
        id="show-1",
        external_ref="tvdb:show-1",
        title="Example Show",
        state=ItemState.DOWNLOADED,
        attributes={
            "item_type": "show",
            "seasons": [
                {
                    "season_number": 1,
                    "episodes": [
                        {
                            "episode_number": 1,
                            "air_date": (now - timedelta(days=7)).date().isoformat(),
                        },
                        {
                            "episode_number": 2,
                            "air_date": (now + timedelta(days=7)).date().isoformat(),
                        },
                    ],
                }
            ],
        },
    )
    latest_request = ItemRequestORM(
        external_ref=item.external_ref,
        media_item_id=item.id,
        media_type="show",
        requested_title=item.title,
        requested_seasons=[1],
        requested_episodes=None,
        is_partial=True,
    )
    session = _FakeShowSession(
        latest_request=latest_request,
        active_item_ids={released_episode_id},
        episode_items_by_id={
            released_episode_id: _build_episode_item(
                released_episode_id,
                aired_at=(now - timedelta(days=7)).isoformat(),
            ),
            future_episode_id: _build_episode_item(
                future_episode_id,
                aired_at=(now + timedelta(days=7)).isoformat(),
            ),
        },
        show_orm=type(
            "ShowStub",
            (),
            {
                "seasons": [
                    type(
                        "SeasonStub",
                        (),
                        {
                            "season_number": 1,
                            "episodes": [
                                type(
                                    "EpisodeStub",
                                    (),
                                    {"episode_number": 1, "media_item_id": released_episode_id},
                                )(),
                                type(
                                    "EpisodeStub",
                                    (),
                                    {"episode_number": 2, "media_item_id": future_episode_id},
                                )(),
                            ],
                        },
                    )(),
                ]
            },
        )(),
    )

    result = asyncio.run(_evaluate_show_completion(item, _FakeShowDb(session), _build_settings()))

    assert result.all_satisfied is True
    assert result.any_satisfied is True
    assert result.has_future_episodes is True
    assert result.missing_released == []


def test_finalize_movie_item_still_completes_unconditionally(monkeypatch: Any) -> None:
    item = MediaItemRecord(
        id="movie-1",
        external_ref="tmdb:1",
        title="Movie",
        state=ItemState.DOWNLOADED,
        attributes={"item_type": "movie"},
    )
    transitions: list[tuple[ItemEvent, str | None]] = []

    class _MediaService:
        _db: object = object()

        async def get_item(self, item_id: str) -> MediaItemRecord | None:
            assert item_id == item.id
            return item

        async def get_latest_item_request_id(self, *, media_item_id: str) -> str | None:
            return "req-1"

        async def transition_item(
            self,
            *,
            item_id: str,
            event: ItemEvent,
            message: str | None = None,
        ) -> MediaItemRecord:
            assert item_id == item.id
            transitions.append((event, message))
            return item

    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: _MediaService())
    monkeypatch.setattr(tasks, "MediaServerNotifier", lambda *_args, **_kwargs: type("Notifier", (), {"notify_all": staticmethod(lambda *_a, **_k: asyncio.sleep(0))})())

    result = asyncio.run(tasks.finalize_item({"settings": _build_settings()}, item.id))

    assert result == item.id
    assert transitions == [(ItemEvent.COMPLETE, "finalize done")]


def test_finalize_show_item_marks_complete_when_all_released_satisfied(monkeypatch: Any) -> None:
    item = MediaItemRecord(
        id="show-complete",
        external_ref="tvdb:complete",
        title="Completed Show",
        state=ItemState.DOWNLOADED,
        attributes={"item_type": "show"},
    )
    transitions: list[tuple[ItemEvent, str | None]] = []

    class _MediaService:
        _db: object = object()

        async def get_item(self, item_id: str) -> MediaItemRecord | None:
            assert item_id == item.id
            return item

        async def get_latest_item_request_id(self, *, media_item_id: str) -> str | None:
            return "req-complete"

        async def transition_item(
            self,
            *,
            item_id: str,
            event: ItemEvent,
            message: str | None = None,
        ) -> MediaItemRecord:
            assert item_id == item.id
            transitions.append((event, message))
            return item

    class _Notifier:
        async def notify_all(self, item_id: str) -> None:
            assert item_id == item.id

    async def fake_evaluate(*_args: Any, **_kwargs: Any) -> ShowCompletionResult:
        return ShowCompletionResult(
            all_satisfied=True,
            any_satisfied=True,
            has_future_episodes=False,
            missing_released=[],
        )

    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: _MediaService())
    monkeypatch.setattr(tasks, "_evaluate_show_completion", fake_evaluate)
    monkeypatch.setattr(tasks, "MediaServerNotifier", lambda *_args, **_kwargs: _Notifier())

    result = asyncio.run(tasks.finalize_item({"settings": _build_settings()}, item.id))

    assert result == item.id
    assert transitions == [(ItemEvent.COMPLETE, "finalize done")]


def test_finalize_show_item_requeues_when_no_episodes_satisfied(monkeypatch: Any) -> None:
    item = MediaItemRecord(
        id="show-empty",
        external_ref="tvdb:empty",
        title="Unsatisfied Show",
        state=ItemState.DOWNLOADED,
        attributes={"item_type": "show"},
    )
    transitions: list[tuple[ItemEvent, str | None]] = []

    class _MediaService:
        _db: object = object()

        async def get_item(self, item_id: str) -> MediaItemRecord | None:
            assert item_id == item.id
            return item

        async def get_latest_item_request_id(self, *, media_item_id: str) -> str | None:
            return "req-empty"

        async def transition_item(
            self,
            *,
            item_id: str,
            event: ItemEvent,
            message: str | None = None,
        ) -> MediaItemRecord:
            transitions.append((event, message))
            return item

    class _FakeRedis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

        async def enqueue_job(self, function: str, *args: Any, **kwargs: Any) -> object:
            self.calls.append((function, args, kwargs))
            return object()

        async def delete(self, _key: str) -> int:
            return 1

    redis = _FakeRedis()

    async def fake_evaluate(*_args: Any, **_kwargs: Any) -> ShowCompletionResult:
        return ShowCompletionResult(
            all_satisfied=False,
            any_satisfied=False,
            has_future_episodes=False,
            missing_released=[(1, 1)],
        )

    async def fake_resolve_arq(_ctx: Any) -> _FakeRedis:
        return redis

    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: _MediaService())
    monkeypatch.setattr(tasks, "_evaluate_show_completion", fake_evaluate)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", fake_resolve_arq)

    result = asyncio.run(
        tasks.finalize_item({"settings": _build_settings(), "queue_name": "filmu-py"}, item.id)
    )

    assert result == item.id
    assert transitions == []


def test_infer_season_range_from_path_handles_various_formats() -> None:
    from filmu_py.services.media import _infer_season_range_from_path

    # Single season or episode
    assert _infer_season_range_from_path("Arcane S02E01.mkv") == [2]
    assert _infer_season_range_from_path("Shows/Arcane/Season 1/Episode 1.mkv") == [1]
    assert _infer_season_range_from_path("Prison Break 3x02.mp4") == [3]

    # Season packs with ranges
    assert _infer_season_range_from_path("Dragon Ball Z S01-S04 PACK") == [1, 2, 3, 4]
    assert _infer_season_range_from_path("The.Office.US S01-S09 1080p") == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    
    # Ambiguous or non-matching paths
    assert _infer_season_range_from_path("Some Random Movie 2024.mkv") == []
    assert _infer_season_range_from_path(None) == []


def test_poll_ongoing_shows_enqueues_scrape_for_newly_released_unsatisfied_episodes(
    monkeypatch: Any,
) -> None:
    item = MediaItemRecord(
        id="show-poll",
        external_ref="tvdb:poll",
        title="Ongoing Show",
        state=ItemState.ONGOING,
        attributes={"item_type": "show"},
    )

    class _MediaService:
        _db: object = object()

        async def list_items_in_states(self, *, states: list[ItemState]) -> list[MediaItemRecord]:
            assert states == [ItemState.PARTIALLY_COMPLETED, ItemState.ONGOING]
            return [item]

    class _FakeRedis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

        async def enqueue_job(self, function: str, *args: Any, **kwargs: Any) -> object:
            self.calls.append((function, args, kwargs))
            return object()

        async def delete(self, _key: str) -> int:
            return 1

        def pipeline(self, transaction: bool = True) -> Any:
            """Stub pipeline for Job.status() used by is_scrape_item_job_active."""

            class _FakePipeline:
                """Minimal stub satisfying arq Job.status():
                tr.exists(result_key)      → is_complete
                tr.exists(in_progress_key) → is_in_progress
                tr.zscore(queue, job_id)   → score
                All return None → JobStatus.not_found → not active.
                """

                async def __aenter__(self) -> Any:
                    return self

                async def __aexit__(self, *_: Any) -> None:
                    return None

                def exists(self, *_a: Any) -> Any:
                    return self

                def zscore(self, *_a: Any) -> Any:
                    return self

                def lpos(self, *_a: Any) -> Any:
                    return self

                async def execute(self) -> list[Any]:
                    # None, None, None → not_found → job not active
                    return [None, None, None]

            return _FakePipeline()

    redis = _FakeRedis()

    async def fake_evaluate(*_args: Any, **_kwargs: Any) -> ShowCompletionResult:
        return ShowCompletionResult(
            all_satisfied=False,
            any_satisfied=False,
            has_future_episodes=False,
            missing_released=[(1, 3)],
        )

    async def fake_resolve_arq(_ctx: Any) -> _FakeRedis:
        return redis

    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: _MediaService())
    monkeypatch.setattr(tasks, "_evaluate_show_completion", fake_evaluate)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", fake_resolve_arq)

    result = asyncio.run(
        tasks.poll_ongoing_shows({"settings": _build_settings(), "queue_name": "filmu-py"})
    )

    assert result == {"processed": 1, "queued": 1}
    assert redis.calls[-1][0] == "scrape_item"

def test_finalize_show_item_requeues_all_missing_seasons_not_just_first(
    monkeypatch: Any,
) -> None:
    """Fix 1: finalize_item must re-enqueue index_item with ALL still-missing seasons.

    Before the fix, only ``all_missing[0]`` was passed, causing a stall loop
    when the first missing season was unavailable.  Wave 3 now re-enters through
    ``index_item`` instead of jumping directly to scrape, but the full sorted
    season set still needs to be preserved.
    """
    item = MediaItemRecord(
        id="show-partial",
        external_ref="tvdb:partial",
        title="Multi-Season Show",
        state=ItemState.DOWNLOADED,
        attributes={"item_type": "show"},
    )
    transitions: list[tuple[ItemEvent, str | None]] = []

    class _MediaService:
        _db: object = object()

        async def get_item(self, item_id: str) -> MediaItemRecord | None:
            assert item_id == item.id
            return item

        async def get_latest_item_request_id(self, *, media_item_id: str) -> str | None:
            return "req-partial"

        async def transition_item(
            self,
            *,
            item_id: str,
            event: ItemEvent,
            message: str | None = None,
        ) -> MediaItemRecord:
            assert item_id == item.id
            transitions.append((event, message))
            return item

    class _FakeRedis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

        async def enqueue_job(self, function: str, *args: Any, **kwargs: Any) -> object:
            self.calls.append((function, args, kwargs))
            return object()

        async def delete(self, _key: str) -> int:
            return 1

    redis = _FakeRedis()

    # Two released seasons missing (S02 and S03), with S01 already satisfied.
    async def fake_evaluate(*_args: Any, **_kwargs: Any) -> ShowCompletionResult:
        return ShowCompletionResult(
            all_satisfied=False,
            any_satisfied=True,  # S01 satisfied
            has_future_episodes=False,
            missing_released=[(2, 1), (3, 1)],  # (season, episode) tuples
        )

    async def fake_resolve_arq(_ctx: Any) -> _FakeRedis:
        return redis

    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: _MediaService())
    monkeypatch.setattr(tasks, "_evaluate_show_completion", fake_evaluate)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", fake_resolve_arq)

    result = asyncio.run(
        tasks.finalize_item({"settings": _build_settings(), "queue_name": "filmu-py"}, item.id)
    )

    assert result == item.id
    assert transitions == [(ItemEvent.PARTIAL_COMPLETE, "missing_episodes")]

    # Verify that index_item was enqueued with missing_seasons=[2, 3] (full set).
    index_calls = [call for call in redis.calls if call[0] == "index_item"]
    assert len(index_calls) == 1, f"expected 1 index_item enqueue call, got {index_calls}"
    enqueued_kwargs = index_calls[0][2]
    assert enqueued_kwargs.get("missing_seasons") == [2, 3], (
        f"expected missing_seasons=[2, 3], got {enqueued_kwargs.get('missing_seasons')!r} — "
        "only the first missing season should NOT be passed (Fix 1 regression)"
    )


def test_finalize_show_item_requeues_single_missing_season(
    monkeypatch: Any,
) -> None:
    """Fix 1 (single-season case): missing_seasons=[N] survives re-entry through index_item."""
    item = MediaItemRecord(
        id="show-one-missing",
        external_ref="tvdb:one-missing",
        title="One Season Missing Show",
        state=ItemState.DOWNLOADED,
        attributes={"item_type": "show"},
    )

    class _MediaService:
        _db: object = object()

        async def get_item(self, item_id: str) -> MediaItemRecord | None:
            return item

        async def get_latest_item_request_id(self, *, media_item_id: str) -> str | None:
            return "req-one-missing"

        async def transition_item(
            self, *, item_id: str, event: ItemEvent, message: str | None = None
        ) -> MediaItemRecord:
            return item

    class _FakeRedis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

        async def enqueue_job(self, function: str, *args: Any, **kwargs: Any) -> object:
            self.calls.append((function, args, kwargs))
            return object()

        async def delete(self, _key: str) -> int:
            return 1

    redis = _FakeRedis()

    async def fake_evaluate(*_args: Any, **_kwargs: Any) -> ShowCompletionResult:
        return ShowCompletionResult(
            all_satisfied=False,
            any_satisfied=True,
            has_future_episodes=False,
            missing_released=[(5, 3)],
        )

    async def fake_resolve_arq(_ctx: Any) -> _FakeRedis:
        return redis

    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: _MediaService())
    monkeypatch.setattr(tasks, "_evaluate_show_completion", fake_evaluate)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", fake_resolve_arq)

    asyncio.run(
        tasks.finalize_item({"settings": _build_settings(), "queue_name": "filmu-py"}, item.id)
    )

    index_calls = [call for call in redis.calls if call[0] == "index_item"]
    assert len(index_calls) == 1
    assert index_calls[0][2].get("missing_seasons") == [5]


def test_finalize_show_item_requeues_missing_episodes_alongside_seasons(
    monkeypatch: Any,
) -> None:
    item = MediaItemRecord(
        id="show-episode-followup",
        external_ref="tvdb:episode-followup",
        title="Episode Followup Show",
        state=ItemState.DOWNLOADED,
        attributes={"item_type": "show"},
    )

    class _MediaService:
        _db: object = object()

        async def get_item(self, item_id: str) -> MediaItemRecord | None:
            return item

        async def get_latest_item_request_id(self, *, media_item_id: str) -> str | None:
            return "req-episode-followup"

        async def transition_item(
            self, *, item_id: str, event: ItemEvent, message: str | None = None
        ) -> MediaItemRecord:
            return item

    class _FakeRedis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

        async def enqueue_job(self, function: str, *args: Any, **kwargs: Any) -> object:
            self.calls.append((function, args, kwargs))
            return object()

        async def delete(self, _key: str) -> int:
            return 1

    redis = _FakeRedis()

    async def fake_evaluate(*_args: Any, **_kwargs: Any) -> ShowCompletionResult:
        return ShowCompletionResult(
            all_satisfied=False,
            any_satisfied=True,
            has_future_episodes=False,
            missing_released=[(4, 3), (4, 5), (6, 0)],
        )

    async def fake_resolve_arq(_ctx: Any) -> _FakeRedis:
        return redis

    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: _MediaService())
    monkeypatch.setattr(tasks, "_evaluate_show_completion", fake_evaluate)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", fake_resolve_arq)

    asyncio.run(
        tasks.finalize_item({"settings": _build_settings(), "queue_name": "filmu-py"}, item.id)
    )

    index_calls = [call for call in redis.calls if call[0] == "index_item"]
    assert len(index_calls) == 1
    assert index_calls[0][2].get("missing_seasons") == [4, 6]
    assert index_calls[0][2].get("missing_episodes") == {"4": [3, 5]}


def test_evaluate_show_completion_does_not_self_satisfy_full_show_from_single_episode_pack() -> None:
    """Full-show + no inventory must not be marked complete from one SxxEyy path alone."""

    item = MediaItemRecord(
        id="show-pack-single-episode",
        external_ref="tvdb:show-pack-single-episode",
        title="Pack Ambiguity Show",
        state=ItemState.DOWNLOADED,
        attributes={"item_type": "show"},
    )

    # Full-show request (non-partial), no explicit season scope.
    latest_request = ItemRequestORM(
        external_ref=item.external_ref,
        media_item_id=item.id,
        media_type="show",
        requested_title=item.title,
        requested_seasons=None,
        requested_episodes=None,
        is_partial=False,
    )

    # A single media-entry path containing season/episode markers is present.
    # Without the regression fix this could incorrectly imply "all requested"
    # and mark the show complete.
    session = _FakeShowSession(
        latest_request=latest_request,
        active_item_ids=set(),
        episode_items_by_id={},
        show_orm=None,
    )

    # Monkeypatch _FakeShowSession.execute for this test case to return one
    # show-level media entry path for the media_entries query branch.
    original_execute = session.execute

    async def execute_with_entry_path(stmt: object) -> Any:
        stmt_text = str(stmt).lower()

        class _ScalarResult:
            def __init__(self, values: list[object]) -> None:
                self._values = values

            def scalars(self) -> _ScalarResult:
                return self

            def all(self) -> list[object]:
                return list(self._values)

            def scalar_one_or_none(self) -> object | None:
                return self._values[0] if self._values else None

        if "from media_entries" in stmt_text and "provider_file_path" in stmt_text:
            class _TupleResult:
                def __init__(self, values: list[tuple[object, object]]) -> None:
                    self._values = values

                def all(self) -> list[tuple[object, object]]:
                    return list(self._values)

            return _TupleResult(
                [
                    (
                        "Pack.Ambiguity.Show.S01E08.1080p.WEB-DL.mkv",
                        "Pack.Ambiguity.Show.S01E08.1080p.WEB-DL.mkv",
                    )
                ]
            )

        return await original_execute(stmt)

    session.execute = execute_with_entry_path  # type: ignore[assignment]

    result = asyncio.run(_evaluate_show_completion(item, _FakeShowDb(session), _build_settings()))

    assert result.all_satisfied is False
    assert result.any_satisfied is True
    assert result.has_future_episodes is False
    assert result.missing_released == []


def test_evaluate_show_completion_does_not_mark_partial_season_episode_as_full_season_cover() -> None:
    item = MediaItemRecord(
        id="show-partial-season-episode",
        external_ref="tvdb:show-partial-season-episode",
        title="Ongoing Pack Ambiguity Show",
        state=ItemState.DOWNLOADED,
        attributes={"item_type": "show"},
    )

    latest_request = ItemRequestORM(
        external_ref=item.external_ref,
        media_item_id=item.id,
        media_type="show",
        requested_title=item.title,
        requested_seasons=[4],
        requested_episodes=None,
        is_partial=True,
    )

    session = _FakeShowSession(
        latest_request=latest_request,
        active_item_ids=set(),
        episode_items_by_id={},
        show_orm=None,
    )
    original_execute = session.execute

    async def execute_with_entry_path(stmt: object) -> Any:
        stmt_text = str(stmt).lower()

        class _ScalarResult:
            def __init__(self, values: list[object]) -> None:
                self._values = values

            def scalars(self) -> _ScalarResult:
                return self

            def all(self) -> list[object]:
                return list(self._values)

            def scalar_one_or_none(self) -> object | None:
                return self._values[0] if self._values else None

        if "from media_entries" in stmt_text and "provider_file_path" in stmt_text:
            class _TupleResult:
                def __init__(self, values: list[tuple[object, object]]) -> None:
                    self._values = values

                def all(self) -> list[tuple[object, object]]:
                    return list(self._values)

            return _TupleResult(
                [
                    (
                        "Invincible.2021.S04E01.1080p.WEB-DL.mkv",
                        "Invincible.2021.S04E01.1080p.WEB-DL.mkv",
                    )
                ]
            )

        return await original_execute(stmt)

    session.execute = execute_with_entry_path  # type: ignore[assignment]

    result = asyncio.run(_evaluate_show_completion(item, _FakeShowDb(session), _build_settings()))

    assert result.all_satisfied is False
    assert result.any_satisfied is True
    assert result.missing_released == [(4, 0)]


def test_infer_episode_number_from_path_extracts_episode_component() -> None:
    from filmu_py.services.media import _infer_episode_number_from_path

    assert _infer_episode_number_from_path("Invincible.S04E03.1080p.WEB-DL.mkv") == 3
    assert _infer_episode_number_from_path("Invincible 4x04 WEB-DL.mkv") == 4
    assert _infer_episode_number_from_path("Invincible Episode 05 WEB-DL.mkv") == 5
    assert _infer_episode_number_from_path("Invincible Season 4 Pack.mkv") is None


def test_extract_tmdb_episode_inventory_uses_next_episode_to_air_for_returning_series() -> None:
    from filmu_py.services.media import _extract_tmdb_episode_inventory

    inventory = _extract_tmdb_episode_inventory(
        {
            "status": "Returning Series",
            "seasons": [
                {"season_number": 4, "episode_count": 8},
            ],
            "next_episode_to_air": {
                "season_number": 4,
                "episode_number": 5,
                "air_date": "2026-04-01",
            },
        },
        today=datetime(2026, 3, 28, tzinfo=UTC).date(),
    )

    season_inventory = inventory[4]
    assert season_inventory.released_episodes == {1, 2, 3, 4}
    assert season_inventory.future_episodes == {5, 6, 7, 8}


def test_extract_tmdb_episode_inventory_marks_all_known_episodes_released_for_ended_show() -> None:
    from filmu_py.services.media import _extract_tmdb_episode_inventory

    inventory = _extract_tmdb_episode_inventory(
        {
            "status": "Ended",
            "seasons": [
                {"season_number": 2, "episode_count": 8},
            ],
            "next_episode_to_air": {
                "season_number": 2,
                "episode_number": 5,
                "air_date": "2026-04-01",
            },
        },
        today=datetime(2026, 3, 28, tzinfo=UTC).date(),
    )

    season_inventory = inventory[2]
    assert season_inventory.released_episodes == {1, 2, 3, 4, 5, 6, 7, 8}
    assert season_inventory.future_episodes == set()
