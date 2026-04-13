"""GraphQL projection query tests for the dual-surface API strategy."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import fnmatch
import json
from typing import Any, cast

from arq.constants import in_progress_key_prefix, result_key_prefix, retry_key_prefix
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.metadata_reindex_status import MetadataReindexStatusStore
from filmu_py.core.queue_status import QueueStatusReader
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.core.runtime_lifecycle import (
    RuntimeLifecycleHealth,
    RuntimeLifecyclePhase,
    RuntimeLifecycleState,
)
from filmu_py.db.models import StreamORM
from filmu_py.graphql import GraphQLPluginRegistry, create_graphql_router
from filmu_py.resources import AppResources
from filmu_py.services.media import (
    CalendarProjectionRecord,
    CalendarReleaseDataRecord,
    MediaItemRecord,
    MediaItemSpecializationRecord,
    MediaItemSummaryRecord,
    ParentIdsRecord,
    RecoveryMechanism,
    RecoveryPlanRecord,
    RecoveryTargetStage,
    StatsProjection,
)
from filmu_py.services.vfs_catalog import (
    VfsCatalogCorrelationKeys,
    VfsCatalogDirectoryEntry,
    VfsCatalogEntry,
    VfsCatalogFileEntry,
    VfsCatalogSnapshot,
    VfsCatalogStats,
)
from filmu_py.state.item import ItemState


class DummyRedis:
    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None


@dataclass
class FakeOperatorRedis(DummyRedis):
    zsets: dict[str, list[tuple[str, float]]] = field(default_factory=dict)
    lists: dict[str, list[str]] = field(default_factory=dict)
    keys: set[str] = field(default_factory=set)

    def zcard(self, name: str) -> int:
        return len(self.zsets.get(name, []))

    def zcount(self, name: str, minimum: str | int, maximum: str | int) -> int:
        return len(self._filter_scores(self.zsets.get(name, []), minimum, maximum))

    def zrangebyscore(
        self,
        name: str,
        minimum: str | int,
        maximum: str | int,
        *,
        start: int = 0,
        num: int = 1,
        withscores: bool = True,
    ) -> list[tuple[str, float]]:
        _ = withscores
        rows = self._filter_scores(self.zsets.get(name, []), minimum, maximum)
        ordered = sorted(rows, key=lambda item: item[1])
        return ordered[start : start + num]

    def scan_iter(self, *, match: str) -> list[str]:
        return [key for key in sorted(self.keys) if fnmatch.fnmatch(key, match)]

    def llen(self, name: str) -> int:
        return len(self.lists.get(name, []))

    def lrange(self, name: str, start: int, stop: int) -> list[str]:
        values = list(self.lists.get(name, []))
        if not values:
            return []
        end = None if stop == -1 else stop + 1
        return values[start:end]

    def lpush(self, name: str, value: str) -> int:
        values = self.lists.setdefault(name, [])
        values.insert(0, value)
        return len(values)

    def ltrim(self, name: str, start: int, stop: int) -> bool:
        values = list(self.lists.get(name, []))
        end = None if stop == -1 else stop + 1
        self.lists[name] = values[start:end]
        return True

    @staticmethod
    def _filter_scores(
        rows: list[tuple[str, float]],
        minimum: str | int,
        maximum: str | int,
    ) -> list[tuple[str, float]]:
        def _matches_lower(score: float, boundary: str | int) -> bool:
            if boundary == "-inf":
                return True
            if isinstance(boundary, str) and boundary.startswith("("):
                return score > float(boundary[1:])
            return score >= float(boundary)

        def _matches_upper(score: float, boundary: str | int) -> bool:
            if boundary == "+inf":
                return True
            if isinstance(boundary, str) and boundary.startswith("("):
                return score < float(boundary[1:])
            return score <= float(boundary)

        return [
            row for row in rows if _matches_lower(row[1], minimum) and _matches_upper(row[1], maximum)
        ]


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

        if self.detail is not None:
            return _Page(items=[self.detail])
        return _Page(
            items=[
                MediaItemSummaryRecord(
                    id=record.id,
                    type=str(record.attributes.get("item_type", "unknown")),
                    title=record.title,
                    state=record.state.value,
                    tmdb_id=(
                        str(record.attributes["tmdb_id"])
                        if record.attributes.get("tmdb_id") is not None
                        else None
                    ),
                    tvdb_id=(
                        str(record.attributes["tvdb_id"])
                        if record.attributes.get("tvdb_id") is not None
                        else None
                    ),
                    external_ref=record.external_ref,
                    aired_at=(
                        str(record.attributes["aired_at"])
                        if record.attributes.get("aired_at") is not None
                        else None
                    ),
                    poster_path=(
                        str(record.attributes["poster_path"])
                        if record.attributes.get("poster_path") is not None
                        else None
                    ),
                    specialization=MediaItemSpecializationRecord(
                        item_type=str(record.attributes.get("item_type", "unknown")),
                        tmdb_id=(
                            str(record.attributes["tmdb_id"])
                            if record.attributes.get("tmdb_id") is not None
                            else None
                        ),
                        tvdb_id=(
                            str(record.attributes["tvdb_id"])
                            if record.attributes.get("tvdb_id") is not None
                            else None
                        ),
                        imdb_id=(
                            str(record.attributes["imdb_id"])
                            if record.attributes.get("imdb_id") is not None
                            else None
                        ),
                        parent_ids=(
                            ParentIdsRecord(
                                tmdb_id=(
                                    str(cast(dict[str, object], record.attributes["parent_ids"]).get("tmdb_id"))
                                    if cast(dict[str, object], record.attributes["parent_ids"]).get("tmdb_id")
                                    is not None
                                    else None
                                ),
                                tvdb_id=(
                                    str(cast(dict[str, object], record.attributes["parent_ids"]).get("tvdb_id"))
                                    if cast(dict[str, object], record.attributes["parent_ids"]).get("tvdb_id")
                                    is not None
                                    else None
                                ),
                            )
                            if isinstance(record.attributes.get("parent_ids"), dict)
                            else None
                        ),
                        show_title=(
                            str(record.attributes["show_title"])
                            if record.attributes.get("show_title") is not None
                            else None
                        ),
                        season_number=(
                            int(record.attributes["season_number"])
                            if record.attributes.get("season_number") is not None
                            else None
                        ),
                        episode_number=(
                            int(record.attributes["episode_number"])
                            if record.attributes.get("episode_number") is not None
                            else None
                        ),
                    ),
                )
                for record in self.item_records[:limit]
            ]
        )

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


@dataclass
class FakeVfsCatalogSupplier:
    snapshot: VfsCatalogSnapshot | None = None
    snapshots_by_generation: dict[int, VfsCatalogSnapshot] = field(default_factory=dict)

    async def build_snapshot(self) -> VfsCatalogSnapshot:
        if self.snapshot is None:
            raise AssertionError("test did not configure a VFS snapshot")
        return self.snapshot

    async def snapshot_for_generation(self, generation_id: int) -> VfsCatalogSnapshot | None:
        return self.snapshots_by_generation.get(generation_id)


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
    )


def _build_client(
    media_service: FakeMediaService,
    *,
    vfs_catalog_supplier: FakeVfsCatalogSupplier | None = None,
    redis: DummyRedis | None = None,
    runtime_lifecycle: RuntimeLifecycleState | None = None,
) -> TestClient:
    settings = _build_settings()
    runtime_redis = redis or DummyRedis()
    app = FastAPI()
    resources = AppResources(
        settings=settings,
        redis=runtime_redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=runtime_redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=runtime_redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=media_service,  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
        runtime_lifecycle=runtime_lifecycle or RuntimeLifecycleState(),
        vfs_catalog_supplier=vfs_catalog_supplier,  # type: ignore[arg-type]
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
                    specialization=MediaItemSpecializationRecord(
                        item_type="episode",
                        tmdb_id="789",
                        tvdb_id="654",
                        imdb_id="tt1234567",
                        parent_ids=ParentIdsRecord(tmdb_id="999", tvdb_id="555"),
                        show_title="Example Show",
                        season_number=1,
                        episode_number=2,
                    ),
                )
            ]
        )
    )

    response = client.post(
        "/graphql",
        json={
            "query": "query { calendarEntries { itemId showTitle itemType airedAt lastState season episode tmdbId tvdbId imdbId parentTmdbId parentTvdbId releaseData } }"
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
            "imdbId": "tt1234567",
            "parentTmdbId": 999,
            "parentTvdbId": 555,
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
        tvdb_id="456",
        created_at="2026-03-15T10:00:00+00:00",
        updated_at="2026-03-15T11:00:00+00:00",
        specialization=MediaItemSpecializationRecord(
            item_type="episode",
            tmdb_id="123",
            tvdb_id="456",
            imdb_id="tt1234567",
            parent_ids=ParentIdsRecord(tmdb_id="999", tvdb_id="555"),
            show_title="Example Show",
            season_number=2,
            episode_number=7,
        ),
        playback_attachments=[
            cast(
                Any,
                type(
                    "Attachment",
                    (),
                    {
                        "id": "attachment-1",
                        "kind": "direct",
                        "locator": "https://cdn.example.com/direct",
                        "source_key": "persisted",
                        "provider": "realdebrid",
                        "provider_download_id": "torrent-123",
                        "provider_file_id": "file-123",
                        "provider_file_path": "/downloads/Example.Movie.mkv",
                        "original_filename": "Example.Movie.mkv",
                        "file_size": 123456789,
                        "local_path": None,
                        "restricted_url": "https://api.real-debrid.com/restricted",
                        "unrestricted_url": "https://cdn.example.com/direct",
                        "is_preferred": True,
                        "preference_rank": 1,
                        "refresh_state": "ready",
                        "expires_at": "2026-03-15T12:00:00+00:00",
                        "last_refreshed_at": "2026-03-15T11:00:00+00:00",
                        "last_refresh_error": None,
                    },
                )(),
            )
        ],
        resolved_playback=cast(
            Any,
            type(
                "ResolvedPlayback",
                (),
                {
                    "direct": type(
                        "ResolvedAttachment",
                        (),
                        {
                            "kind": "direct",
                            "locator": "https://cdn.example.com/direct",
                            "source_key": "persisted",
                            "provider": "realdebrid",
                            "provider_download_id": "torrent-123",
                            "provider_file_id": "file-123",
                            "provider_file_path": "/downloads/Example.Movie.mkv",
                            "original_filename": "Example.Movie.mkv",
                            "file_size": 123456789,
                            "local_path": None,
                            "restricted_url": None,
                            "unrestricted_url": "https://cdn.example.com/direct",
                        },
                    )(),
                    "hls": None,
                    "direct_ready": True,
                    "hls_ready": False,
                    "missing_local_file": False,
                },
            )(),
        ),
        active_stream=cast(
            Any,
            type(
                "ActiveStream",
                (),
                {
                    "direct_ready": True,
                    "hls_ready": False,
                    "missing_local_file": False,
                    "direct_owner": type(
                        "ActiveOwner",
                        (),
                        {
                            "media_entry_index": 0,
                            "kind": "remote-direct",
                            "original_filename": "Example.Movie.mkv",
                            "provider": "realdebrid",
                            "provider_download_id": "torrent-123",
                            "provider_file_id": "file-123",
                            "provider_file_path": "/downloads/Example.Movie.mkv",
                        },
                    )(),
                    "hls_owner": None,
                },
            )(),
        ),
        media_entries=[
            cast(
                Any,
                type(
                    "MediaEntry",
                    (),
                    {
                        "entry_type": "media",
                        "kind": "remote-direct",
                        "original_filename": "Example.Movie.mkv",
                        "url": "https://cdn.example.com/direct",
                        "local_path": None,
                        "download_url": "https://api.real-debrid.com/restricted",
                        "unrestricted_url": "https://cdn.example.com/direct",
                        "provider": "realdebrid",
                        "provider_download_id": "torrent-123",
                        "provider_file_id": "file-123",
                        "provider_file_path": "/downloads/Example.Movie.mkv",
                        "size": 123456789,
                        "created": "2026-03-15T10:30:00+00:00",
                        "modified": "2026-03-15T11:00:00+00:00",
                        "refresh_state": "ready",
                        "expires_at": "2026-03-15T12:00:00+00:00",
                        "last_refreshed_at": "2026-03-15T11:00:00+00:00",
                        "last_refresh_error": None,
                        "active_for_direct": True,
                        "active_for_hls": False,
                        "is_active_stream": True,
                    },
                )(),
            )
        ],
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
            "query": 'query { mediaItem(id: "item-1") { id title state itemType tmdbId tvdbId imdbId parentTmdbId parentTvdbId showTitle seasonNumber episodeNumber createdAt updatedAt recoveryPlan { mechanism targetStage reason nextRetryAt recoveryAttemptCount isInCooldown } streamCandidates { id rawTitle parsedTitle resolution rankScore levRatio selected passed rejectionReason } selectedStream { id rawTitle selected } playbackAttachments { id kind sourceKey provider providerDownloadId originalFilename fileSize refreshState } resolvedPlayback { directReady hlsReady missingLocalFile direct { kind locator sourceKey providerDownloadId originalFilename } } activeStream { directReady hlsReady missingLocalFile directOwner { mediaEntryIndex kind providerDownloadId originalFilename } } mediaEntries { entryType kind originalFilename providerDownloadId size refreshState activeForDirect activeForHls isActiveStream } } }'
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
    assert payload["tvdbId"] == 456
    assert payload["imdbId"] == "tt1234567"
    assert payload["parentTmdbId"] == 999
    assert payload["parentTvdbId"] == 555
    assert payload["showTitle"] == "Example Show"
    assert payload["seasonNumber"] == 2
    assert payload["episodeNumber"] == 7
    assert len(payload["streamCandidates"]) == 2
    assert payload["selectedStream"]["id"] == "stream-1"
    assert payload["streamCandidates"][0]["rawTitle"] == "Example.Movie.1080p.WEB-DL"
    assert payload["playbackAttachments"] == [
        {
            "id": "attachment-1",
            "kind": "direct",
            "sourceKey": "persisted",
            "provider": "realdebrid",
            "providerDownloadId": "torrent-123",
            "originalFilename": "Example.Movie.mkv",
            "fileSize": 123456789,
            "refreshState": "ready",
        }
    ]
    assert payload["resolvedPlayback"] == {
        "directReady": True,
        "hlsReady": False,
        "missingLocalFile": False,
        "direct": {
            "kind": "direct",
            "locator": "https://cdn.example.com/direct",
            "sourceKey": "persisted",
            "providerDownloadId": "torrent-123",
            "originalFilename": "Example.Movie.mkv",
        },
    }
    assert payload["activeStream"] == {
        "directReady": True,
        "hlsReady": False,
        "missingLocalFile": False,
        "directOwner": {
            "mediaEntryIndex": 0,
            "kind": "remote-direct",
            "providerDownloadId": "torrent-123",
            "originalFilename": "Example.Movie.mkv",
        },
    }
    assert payload["mediaEntries"] == [
        {
            "entryType": "media",
            "kind": "remote-direct",
            "originalFilename": "Example.Movie.mkv",
            "providerDownloadId": "torrent-123",
            "size": 123456789,
            "refreshState": "ready",
            "activeForDirect": True,
            "activeForHls": False,
            "isActiveStream": True,
        }
    ]


def test_graphql_items_exposes_media_type_and_media_kind() -> None:
    client = _build_client(
        FakeMediaService(
            item_records=[
                MediaItemRecord(
                    id="show-1",
                    external_ref="tvdb:555",
                    title="Example Show",
                    state=ItemState.REQUESTED,
                    attributes={
                        "item_type": "show",
                        "tmdb_id": "999",
                        "tvdb_id": "555",
                        "imdb_id": "tt5550001",
                        "show_title": "Example Show",
                        "poster_path": "/poster.jpg",
                    },
                )
            ]
        )
    )

    response = client.post(
        "/graphql",
        json={
            "query": "query { items(limit: 1) { id mediaType mediaKind tmdbId tvdbId imdbId showTitle posterPath } }"
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["items"] == [
        {
            "id": "show-1",
            "mediaType": "show",
            "mediaKind": "SHOW",
            "tmdbId": 999,
            "tvdbId": 555,
            "imdbId": "tt5550001",
            "showTitle": "Example Show",
            "posterPath": "/poster.jpg",
        }
    ]


def test_graphql_vfs_directory_and_entry_queries_use_catalog_snapshot() -> None:
    snapshot = VfsCatalogSnapshot(
        generation_id="7",
        published_at=datetime(2026, 4, 13, 12, 0, tzinfo=UTC),
        entries=(
            VfsCatalogEntry(
                entry_id="dir:/",
                parent_entry_id=None,
                path="/",
                name="/",
                kind="directory",
                directory=VfsCatalogDirectoryEntry(path="/"),
            ),
            VfsCatalogEntry(
                entry_id="dir:/Shows",
                parent_entry_id="dir:/",
                path="/Shows",
                name="Shows",
                kind="directory",
                directory=VfsCatalogDirectoryEntry(path="/Shows"),
            ),
            VfsCatalogEntry(
                entry_id="dir:/Shows/Example Show (2024)",
                parent_entry_id="dir:/Shows",
                path="/Shows/Example Show (2024)",
                name="Example Show (2024)",
                kind="directory",
                directory=VfsCatalogDirectoryEntry(path="/Shows/Example Show (2024)"),
            ),
            VfsCatalogEntry(
                entry_id="dir:/Shows/Example Show (2024)/Season 01",
                parent_entry_id="dir:/Shows/Example Show (2024)",
                path="/Shows/Example Show (2024)/Season 01",
                name="Season 01",
                kind="directory",
                directory=VfsCatalogDirectoryEntry(path="/Shows/Example Show (2024)/Season 01"),
            ),
            VfsCatalogEntry(
                entry_id="file:entry-1",
                parent_entry_id="dir:/Shows/Example Show (2024)/Season 01",
                path="/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
                name="Example Show S01E01.mkv",
                kind="file",
                correlation=VfsCatalogCorrelationKeys(
                    item_id="item-1",
                    media_entry_id="entry-1",
                    provider="realdebrid",
                    provider_download_id="torrent-123",
                    provider_file_path="/downloads/Example.Show.S01E01.mkv",
                ),
                file=VfsCatalogFileEntry(
                    item_id="item-1",
                    item_title="Example Show",
                    item_external_ref="tvdb:555",
                    media_entry_id="entry-1",
                    source_attachment_id="attachment-1",
                    media_type="episode",
                    transport="remote-direct",
                    locator="https://cdn.example.com/stream/entry-1",
                    unrestricted_url="https://cdn.example.com/stream/entry-1",
                    original_filename="Example Show S01E01.mkv",
                    size_bytes=987654321,
                    lease_state="ready",
                    last_refreshed_at=datetime(2026, 4, 13, 11, 55, tzinfo=UTC),
                    provider="realdebrid",
                    provider_download_id="torrent-123",
                    provider_file_path="/downloads/Example.Show.S01E01.mkv",
                    active_roles=("direct",),
                    source_key="persisted",
                    query_strategy="persisted_media_entries",
                    provider_family="debrid",
                    locator_source="unrestricted_url",
                    match_basis="provider_identity",
                ),
            ),
        ),
        stats=VfsCatalogStats(directory_count=4, file_count=1, blocked_item_count=0),
    )
    client = _build_client(
        FakeMediaService(),
        vfs_catalog_supplier=FakeVfsCatalogSupplier(
            snapshot=snapshot,
            snapshots_by_generation={7: snapshot},
        ),
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  vfsDirectory(path: "/Shows/Example Show (2024)/Season 01", generationId: "7") {
                    generationId
                    path
                    entry { entryId kind path }
                    stats { directoryCount fileCount blockedItemCount }
                    directories { path name kind }
                    files {
                      path
                      name
                      kind
                      correlation { itemId mediaEntryId provider providerDownloadId providerFilePath }
                      file {
                        itemId
                        itemTitle
                        itemExternalRef
                        mediaEntryId
                        mediaType
                        transport
                        locator
                        unrestrictedUrl
                        originalFilename
                        sizeBytes
                        leaseState
                        lastRefreshedAt
                        activeRoles
                        sourceKey
                        queryStrategy
                        providerFamily
                        locatorSource
                        matchBasis
                      }
                    }
                  }
                  vfsCatalogEntry(path: "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv", generationId: "7") {
                    path
                    kind
                    correlation { itemId mediaEntryId providerDownloadId }
                    file { mediaEntryId providerDownloadId restrictedFallback }
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["vfsDirectory"] == {
        "generationId": "7",
        "path": "/Shows/Example Show (2024)/Season 01",
        "entry": {
            "entryId": "dir:/Shows/Example Show (2024)/Season 01",
            "kind": "directory",
            "path": "/Shows/Example Show (2024)/Season 01",
        },
        "stats": {
            "directoryCount": 4,
            "fileCount": 1,
            "blockedItemCount": 0,
        },
        "directories": [],
        "files": [
            {
                "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
                "name": "Example Show S01E01.mkv",
                "kind": "file",
                "correlation": {
                    "itemId": "item-1",
                    "mediaEntryId": "entry-1",
                    "provider": "realdebrid",
                    "providerDownloadId": "torrent-123",
                    "providerFilePath": "/downloads/Example.Show.S01E01.mkv",
                },
                "file": {
                    "itemId": "item-1",
                    "itemTitle": "Example Show",
                    "itemExternalRef": "tvdb:555",
                    "mediaEntryId": "entry-1",
                    "mediaType": "episode",
                    "transport": "remote-direct",
                    "locator": "https://cdn.example.com/stream/entry-1",
                    "unrestrictedUrl": "https://cdn.example.com/stream/entry-1",
                    "originalFilename": "Example Show S01E01.mkv",
                    "sizeBytes": 987654321,
                    "leaseState": "ready",
                    "lastRefreshedAt": "2026-04-13T11:55:00+00:00",
                    "activeRoles": ["direct"],
                    "sourceKey": "persisted",
                    "queryStrategy": "persisted_media_entries",
                    "providerFamily": "debrid",
                    "locatorSource": "unrestricted_url",
                    "matchBasis": "provider_identity",
                },
            }
        ],
    }
    assert payload["vfsCatalogEntry"] == {
        "path": "/Shows/Example Show (2024)/Season 01/Example Show S01E01.mkv",
        "kind": "file",
        "correlation": {
            "itemId": "item-1",
            "mediaEntryId": "entry-1",
            "providerDownloadId": "torrent-123",
        },
        "file": {
            "mediaEntryId": "entry-1",
            "providerDownloadId": "torrent-123",
            "restrictedFallback": False,
        },
    }


def test_graphql_vfs_snapshot_and_blocked_items_queries_use_catalog_snapshot() -> None:
    snapshot = VfsCatalogSnapshot(
        generation_id="12",
        published_at=datetime(2026, 4, 13, 12, 30, tzinfo=UTC),
        entries=(),
        stats=VfsCatalogStats(directory_count=3, file_count=5, blocked_item_count=1),
        blocked_items=(
            cast(
                Any,
                type(
                    "BlockedItem",
                    (),
                    {
                        "item_id": "item-blocked",
                        "external_ref": "tmdb:42",
                        "title": "Blocked Example",
                        "reason": "missing_media_entry",
                    },
                )(),
            ),
        ),
    )
    client = _build_client(
        FakeMediaService(),
        vfs_catalog_supplier=FakeVfsCatalogSupplier(snapshot=snapshot, snapshots_by_generation={12: snapshot}),
    )

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  vfsSnapshot(generationId: "12") {
                    generationId
                    publishedAt
                    stats { directoryCount fileCount blockedItemCount }
                    blockedItems { itemId externalRef title reason }
                  }
                  vfsBlockedItems(generationId: "12") {
                    itemId
                    externalRef
                    title
                    reason
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["vfsSnapshot"] == {
        "generationId": "12",
        "publishedAt": "2026-04-13T12:30:00+00:00",
        "stats": {
            "directoryCount": 3,
            "fileCount": 5,
            "blockedItemCount": 1,
        },
        "blockedItems": [
            {
                "itemId": "item-blocked",
                "externalRef": "tmdb:42",
                "title": "Blocked Example",
                "reason": "missing_media_entry",
            }
        ],
    }
    assert payload["vfsBlockedItems"] == [
        {
            "itemId": "item-blocked",
            "externalRef": "tmdb:42",
            "title": "Blocked Example",
            "reason": "missing_media_entry",
        }
    ]


def test_graphql_operator_queries_expose_runtime_queue_and_metadata_history() -> None:
    current_ms = datetime.now(UTC).timestamp() * 1000
    redis = FakeOperatorRedis(
        zsets={
            "filmu-py": [
                ("job-ready", current_ms - 30_000),
                ("job-deferred", current_ms + 45_000),
            ],
        },
        lists={
            "arq:queue-status-history:filmu-py": [
                json.dumps(
                    {
                        "observed_at": "2026-04-13T12:01:00Z",
                        "total_jobs": 5,
                        "ready_jobs": 2,
                        "deferred_jobs": 1,
                        "in_progress_jobs": 1,
                        "retry_jobs": 1,
                        "dead_letter_jobs": 2,
                        "oldest_ready_age_seconds": 12.5,
                        "next_scheduled_in_seconds": 42.0,
                        "alert_level": "critical",
                        "dead_letter_oldest_age_seconds": 420.0,
                        "dead_letter_reason_counts": {"provider_timeout": 2},
                    }
                )
            ],
            "arq:metadata-reindex-history:filmu-py": [
                json.dumps(
                    {
                        "observed_at": "2026-04-13T12:02:00Z",
                        "processed": 10,
                        "queued": 3,
                        "reconciled": 6,
                        "skipped_active": 1,
                        "failed": 1,
                        "repair_attempted": 2,
                        "repair_enriched": 1,
                        "repair_skipped_no_tmdb_id": 0,
                        "repair_failed": 1,
                        "repair_requeued": 1,
                        "repair_skipped_active": 0,
                        "outcome": "warning",
                        "run_failed": False,
                        "last_error": "provider_timeout",
                    }
                )
            ],
            "arq:dead-letter:filmu-py": [
                json.dumps({"queued_at": "2026-04-13T11:50:00Z", "reason_code": "provider_timeout"})
            ],
        },
        keys={
            f"{retry_key_prefix}filmu-py:1",
            f"{in_progress_key_prefix}filmu-py:1",
            f"{result_key_prefix}filmu-py:1",
        },
    )
    lifecycle = RuntimeLifecycleState()
    lifecycle.transition(
        RuntimeLifecyclePhase.PLUGIN_REGISTRATION,
        detail="plugins_registered",
        health=RuntimeLifecycleHealth.HEALTHY,
    )
    lifecycle.transition(
        RuntimeLifecyclePhase.STEADY_STATE,
        detail="runtime_steady",
        health=RuntimeLifecycleHealth.HEALTHY,
    )
    client = _build_client(FakeMediaService(), redis=redis, runtime_lifecycle=lifecycle)

    response = client.post(
        "/graphql",
        json={
            "query": """
                query {
                  runtimeLifecycle {
                    phase
                    health
                    detail
                    transitions { phase health detail }
                  }
                  workerQueueStatus {
                    queueName
                    arqEnabled
                    totalJobs
                    readyJobs
                    deferredJobs
                    inProgressJobs
                    retryJobs
                    resultJobs
                    deadLetterJobs
                    alertLevel
                    deadLetterReasonCounts
                  }
                  workerQueueHistory(limit: 5, alertLevel: "critical", minDeadLetterJobs: 2, reasonCode: "provider_timeout") {
                    observedAt
                    alertLevel
                    deadLetterJobs
                    deadLetterReasonCounts
                  }
                  workerMetadataReindexStatus {
                    queueName
                    hasHistory
                    processed
                    queued
                    reconciled
                    failed
                    outcome
                    lastError
                  }
                  workerMetadataReindexHistory(limit: 5) {
                    observedAt
                    processed
                    queued
                    reconciled
                    failed
                    repairAttempted
                    repairFailed
                    outcome
                    lastError
                  }
                }
            """
        },
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["runtimeLifecycle"]["phase"] == "steady_state"
    assert payload["runtimeLifecycle"]["detail"] == "runtime_steady"
    assert payload["runtimeLifecycle"]["transitions"][-1] == {
        "phase": "steady_state",
        "health": "healthy",
        "detail": "runtime_steady",
    }
    assert payload["workerQueueStatus"] == {
        "queueName": "filmu-py",
        "arqEnabled": True,
        "totalJobs": 2,
        "readyJobs": 1,
        "deferredJobs": 1,
        "inProgressJobs": 1,
        "retryJobs": 1,
        "resultJobs": 1,
        "deadLetterJobs": 1,
        "alertLevel": "critical",
        "deadLetterReasonCounts": {"provider_timeout": 1},
    }
    assert payload["workerQueueHistory"] == [
        {
            "observedAt": "2026-04-13T12:01:00Z",
            "alertLevel": "critical",
            "deadLetterJobs": 2,
            "deadLetterReasonCounts": {"provider_timeout": 2},
        }
    ]
    assert payload["workerMetadataReindexStatus"] == {
        "queueName": "filmu-py",
        "hasHistory": True,
        "processed": 10,
        "queued": 3,
        "reconciled": 6,
        "failed": 1,
        "outcome": "warning",
        "lastError": "provider_timeout",
    }
    assert payload["workerMetadataReindexHistory"] == [
        {
            "observedAt": "2026-04-13T12:02:00Z",
            "processed": 10,
            "queued": 3,
            "reconciled": 6,
            "failed": 1,
            "repairAttempted": 2,
            "repairFailed": 1,
            "outcome": "warning",
            "lastError": "provider_timeout",
        }
    ]
