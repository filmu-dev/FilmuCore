"""GraphQL projection query tests for the dual-surface API strategy."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.db.models import StreamORM
from filmu_py.graphql import GraphQLPluginRegistry, create_graphql_router
from filmu_py.resources import AppResources
from filmu_py.services.media import (
    CalendarProjectionRecord,
    CalendarReleaseDataRecord,
    MediaItemRecord,
    MediaItemSummaryRecord,
    RecoveryMechanism,
    RecoveryPlanRecord,
    RecoveryTargetStage,
    StatsProjection,
)
from filmu_py.state.item import ItemState


class DummyRedis:
    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None


class DummyDatabaseRuntime:
    async def dispose(self) -> None:
        return None


@dataclass
class FakeMediaService:
    calendar_entries: list[CalendarProjectionRecord] = field(default_factory=list)
    stats: StatsProjection = field(
        default_factory=lambda: StatsProjection(
            total_items=0,
            completed_items=0,
            failed_items=0,
            incomplete_items=0,
            movies=0,
            shows=0,
            episodes=0,
        )
    )
    detail: MediaItemSummaryRecord | None = None
    stream_candidates: list[StreamORM] = field(default_factory=list)
    item_records: list[MediaItemRecord] = field(default_factory=list)
    recovery_plan: RecoveryPlanRecord | None = None

    async def get_calendar(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[CalendarProjectionRecord]:
        _ = (start_date, end_date)
        return list(self.calendar_entries)

    async def get_stats(self) -> StatsProjection:
        return self.stats

    async def get_item_detail(
        self,
        item_identifier: str,
        *,
        media_type: str,
        extended: bool = False,
    ) -> MediaItemSummaryRecord | None:
        _ = (item_identifier, media_type, extended)
        return self.detail

    async def search_items(
        self,
        *,
        limit: int = 24,
        page: int = 1,
        item_types: list[str] | None = None,
        states: list[str] | None = None,
        sort: list[str] | None = None,
        search: str | None = None,
        extended: bool = False,
    ) -> object:
        _ = (limit, page, item_types, states, sort, search, extended)

        @dataclass
        class _Page:
            items: list[MediaItemSummaryRecord]

        return _Page(items=[self.detail] if self.detail is not None else [])

    async def get_stream_candidates(self, *, media_item_id: str) -> list[StreamORM]:
        _ = media_item_id
        return list(self.stream_candidates)

    async def get_recovery_plan(self, *, media_item_id: str) -> RecoveryPlanRecord | None:
        _ = media_item_id
        return self.recovery_plan

    async def list_items(self, limit: int = 100) -> list[MediaItemRecord]:
        return list(self.item_records[:limit])

    async def get_item(self, item_id: str) -> MediaItemRecord | None:
        return next((record for record in self.item_records if record.id == item_id), None)


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
    )


def _build_client(media_service: FakeMediaService) -> TestClient:
    settings = _build_settings()
    redis = DummyRedis()
    app = FastAPI()
    resources = AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=media_service,  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
    )
    app.state.resources = resources
    app.include_router(create_graphql_router(resources.graphql_plugin_registry), prefix="/graphql")
    return TestClient(app)


def test_graphql_calendar_entries_returns_list_shape() -> None:
    client = _build_client(
        FakeMediaService(
            calendar_entries=[
                CalendarProjectionRecord(
                    item_id="item-1",
                    title="Example Show",
                    item_type="episode",
                    tmdb_id="123",
                    tvdb_id="456",
                    episode_number=2,
                    season_number=1,
                    air_date="2026-03-15T10:00:00+00:00",
                    last_state="Completed",
                    release_data=CalendarReleaseDataRecord(next_aired="2026-03-16T10:00:00+00:00"),
                )
            ]
        )
    )

    response = client.post(
        "/graphql",
        json={
            "query": "query { calendarEntries { itemId showTitle itemType airedAt lastState season episode tmdbId tvdbId releaseData } }"
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["calendarEntries"] == [
        {
            "itemId": "item-1",
            "showTitle": "Example Show",
            "itemType": "episode",
            "airedAt": "2026-03-15T10:00:00+00:00",
            "lastState": "Completed",
            "season": 1,
            "episode": 2,
            "tmdbId": 123,
            "tvdbId": 456,
            "releaseData": '{"next_aired": "2026-03-16T10:00:00+00:00", "nextAired": null, "last_aired": null, "lastAired": null}',
        }
    ]


def test_graphql_library_stats_returns_typed_breakdown() -> None:
    client = _build_client(
        FakeMediaService(
            stats=StatsProjection(
                total_items=10,
                completed_items=4,
                failed_items=2,
                incomplete_items=4,
                movies=3,
                shows=2,
                episodes=5,
                seasons=2,
                states={"Completed": 4, "Failed": 2, "Unreleased": 0},
                activity={"2026-03-15": 10},
            )
        )
    )

    response = client.post(
        "/graphql",
        json={
            "query": "query { libraryStats { totalItems totalMovies totalShows totalSeasons totalEpisodes completedItems incompleteItems failedItems stateBreakdown activity } }"
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["libraryStats"]
    assert payload["totalItems"] == 10
    assert payload["failedItems"] == 2
    assert payload["stateBreakdown"] == '{"Completed": 4, "Failed": 2, "Unreleased": 0}'
    assert payload["activity"] == '[{"date": "2026-03-15", "count": 10}]'


def test_graphql_media_item_returns_stream_candidates() -> None:
    detail = MediaItemSummaryRecord(
        id="item-1",
        type="movie",
        title="Example Movie",
        state="Completed",
        tmdb_id="123",
        created_at="2026-03-15T10:00:00+00:00",
        updated_at="2026-03-15T11:00:00+00:00",
    )
    selected_stream = StreamORM(
        id="stream-1",
        media_item_id="item-1",
        infohash="hash-1",
        raw_title="Example.Movie.1080p.WEB-DL",
        parsed_title={"title": "Example Movie"},
        rank=300,
        lev_ratio=1.0,
        resolution="1080p",
        selected=True,
    )
    alternate_stream = StreamORM(
        id="stream-2",
        media_item_id="item-1",
        infohash="hash-2",
        raw_title="Example.Movie.720p.WEB-DL",
        parsed_title={"title": "Example Movie"},
        rank=100,
        lev_ratio=0.9,
        resolution="720p",
        selected=False,
    )
    client = _build_client(
        FakeMediaService(
            detail=detail,
            stream_candidates=[selected_stream, alternate_stream],
            recovery_plan=RecoveryPlanRecord(
                mechanism=RecoveryMechanism.ORPHAN_RECOVERY,
                target_stage=RecoveryTargetStage.FINALIZE,
                reason="orphaned_downloaded_item",
                next_retry_at=None,
                recovery_attempt_count=2,
                is_in_cooldown=False,
            ),
        )
    )

    response = client.post(
        "/graphql",
        json={
            "query": 'query { mediaItem(id: "item-1") { id title state itemType tmdbId createdAt updatedAt recoveryPlan { mechanism targetStage reason nextRetryAt recoveryAttemptCount isInCooldown } streamCandidates { id rawTitle parsedTitle resolution rankScore levRatio selected passed rejectionReason } selectedStream { id rawTitle selected } } }'
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]["mediaItem"]
    assert payload["id"] == "item-1"
    assert payload["recoveryPlan"] == {
        "mechanism": "ORPHAN_RECOVERY",
        "targetStage": "FINALIZE",
        "reason": "orphaned_downloaded_item",
        "nextRetryAt": None,
        "recoveryAttemptCount": 2,
        "isInCooldown": False,
    }
    assert len(payload["streamCandidates"]) == 2
    assert payload["selectedStream"]["id"] == "stream-1"
    assert payload["streamCandidates"][0]["rawTitle"] == "Example.Movie.1080p.WEB-DL"


def test_graphql_items_exposes_media_type_and_media_kind() -> None:
    client = _build_client(
        FakeMediaService(
            item_records=[
                MediaItemRecord(
                    id="show-1",
                    external_ref="tvdb:555",
                    title="Example Show",
                    state=ItemState.REQUESTED,
                    attributes={"item_type": "show", "tvdb_id": "555"},
                )
            ]
        )
    )

    response = client.post(
        "/graphql",
        json={"query": "query { items(limit: 1) { id mediaType mediaKind } }"},
    )

    assert response.status_code == 200
    assert response.json()["data"]["items"] == [
        {"id": "show-1", "mediaType": "show", "mediaKind": "SHOW"}
    ]
