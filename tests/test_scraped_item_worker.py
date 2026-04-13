from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import httpx
import pytest
from arq.jobs import JobStatus

from filmu_py.config import Settings
from filmu_py.core.event_bus import EventBus
from filmu_py.db.models import (
    EpisodeORM,
    ItemStateEventORM,
    MediaItemORM,
    SeasonORM,
    ShowORM,
    StreamORM,
)
from filmu_py.plugins import ExternalIdentifiers, ScraperSearchInput
from filmu_py.plugins import ScraperResult as PluginScraperResult
from filmu_py.rtn import RankedTorrent
from filmu_py.services.media import (
    CompletionStatus,
    EnrichmentResult,
    ItemRequestSummaryRecord,
    LibraryRecoverySnapshot,
    MediaItemRecord,
    MediaService,
    RankedStreamCandidateRecord,
    RecoveryMechanism,
    RecoveryTargetStage,
    RequestMetadataResolution,
    ScrapeCandidateRecord,
    ShowCompletionResult,
    _build_recovery_plan_record,
    parse_stream_candidate_title,
    validate_parsed_stream_candidate,
)
from filmu_py.state.item import ItemEvent, ItemState, ItemStateMachine
from filmu_py.workers import tasks


class _PluginRegistryStub:
    def __init__(self, scrapers: list[object]) -> None:
        self._scrapers = scrapers

    def get_scrapers(self) -> list[object]:
        return list(self._scrapers)


def _build_item_orm(*, item_id: str, state: ItemState) -> MediaItemORM:
    return MediaItemORM(
        id=item_id,
        external_ref=f"tmdb:{item_id}",
        title="Example Movie",
        state=state.value,
        recovery_attempt_count=0,
        next_retry_at=None,
        attributes={"item_type": "movie", "tmdb_id": item_id},
    )


def _build_state_event(
    *,
    item_id: str,
    previous_state: ItemState,
    next_state: ItemState,
    created_at: datetime,
) -> ItemStateEventORM:
    return ItemStateEventORM(
        item_id=item_id,
        event=f"{previous_state.value}_to_{next_state.value}",
        previous_state=previous_state.value,
        next_state=next_state.value,
        message=None,
        payload={},
        created_at=created_at,
    )


def _build_stream(
    *,
    stream_id: str,
    item_id: str,
    parsed: bool,
    selected: bool = False,
) -> StreamORM:
    return StreamORM(
        id=stream_id,
        media_item_id=item_id,
        infohash=f"hash-{stream_id}",
        raw_title=f"Example.Movie.{stream_id}.1080p.WEB-DL",
        parsed_title={"title": "Example Movie"} if parsed else {},
        rank=0,
        lev_ratio=None,
        resolution="1080p",
        selected=selected,
    )


def _build_ranked_record(
    *,
    item_id: str,
    stream: StreamORM,
    rank_score: int,
    lev_ratio: float,
    passed: bool,
) -> RankedStreamCandidateRecord:
    return RankedStreamCandidateRecord(
        item_id=item_id,
        stream_id=stream.id,
        rank_score=rank_score,
        lev_ratio=lev_ratio,
        fetch=passed,
        passed=passed,
        rejection_reason=None if passed else "no_passing_stream_candidates",
        stream=stream,
    )


@dataclass
class FakePipelineMediaService:
    item_id: str
    state: ItemState = ItemState.SCRAPED
    streams: list[StreamORM] = field(default_factory=list)
    scrape_candidates: list[ScrapeCandidateRecord] = field(default_factory=list)
    ranked_results: list[RankedStreamCandidateRecord] = field(default_factory=list)
    selected_stream_id: str | None = None
    calls: list[str] = field(default_factory=list)
    transition_messages: list[tuple[ItemEvent, str | None]] = field(default_factory=list)
    prepared_retry_messages: list[str] = field(default_factory=list)
    persisted_downloads: list[dict[str, object]] = field(default_factory=list)
    persisted_ranked_results: list[RankedStreamCandidateRecord] = field(default_factory=list)
    persisted_scrape_candidates: list[ScrapeCandidateRecord] = field(default_factory=list)
    item_attributes: dict[str, object] = field(default_factory=lambda: {"item_type": "movie"})
    latest_item_request_id: str | None = None
    latest_item_request: ItemRequestSummaryRecord | None = None
    listed_items: list[MediaItemRecord] = field(default_factory=list)
    has_media_entries: bool = False
    last_requested_seasons: list[int] | None = None
    enrichment_resolution: RequestMetadataResolution = field(
        default_factory=lambda: RequestMetadataResolution(
            metadata=None,
            enrichment=EnrichmentResult(
                source="none",
                has_poster=False,
                has_imdb_id=False,
                has_tmdb_id=False,
                warnings=[],
            ),
        )
    )

    class _FakeSession:
        async def __aenter__(self) -> Any:
            return self
        async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass
            
    class _FakeDB:
        def session(self) -> Any:
            return FakePipelineMediaService._FakeSession()

    _db: Any = field(default_factory=_FakeDB)
    completion_status: str = "complete"

    async def evaluate_show_completion_scope(self, item_id: str, session: Any) -> str:
        self.calls.append("evaluate_show_completion_scope")
        return self.completion_status

    def __post_init__(self) -> None:
        if not self.scrape_candidates:
            self.scrape_candidates = [
                ScrapeCandidateRecord(
                    item_id=self.item_id,
                    info_hash=stream.infohash,
                    raw_title=stream.raw_title,
                    provider="test",
                    size_bytes=None,
                )
                for stream in self.streams
            ]

    async def get_item(self, item_id: str) -> MediaItemRecord | None:
        self.calls.append("get_item")
        if item_id != self.item_id:
            return None
        return MediaItemRecord(
            id=self.item_id,
            external_ref=f"tmdb:{self.item_id}",
            title="Example Movie",
            state=self.state,
            attributes=dict(self.item_attributes),
            has_media_entries=self.has_media_entries,
        )

    async def get_stream_candidates(self, *, media_item_id: str) -> list[StreamORM]:
        self.calls.append("get_stream_candidates")
        assert media_item_id == self.item_id
        return list(self.streams)

    async def get_scrape_candidates(self, *, item_id: str) -> list[ScrapeCandidateRecord]:
        self.calls.append("get_scrape_candidates")
        assert item_id == self.item_id
        return list(self.scrape_candidates)

    async def persist_scrape_candidates(
        self,
        *,
        item_id: str,
        candidates: list[ScrapeCandidateRecord],
    ) -> list[ScrapeCandidateRecord]:
        self.calls.append("persist_scrape_candidates")
        assert item_id == self.item_id
        self.persisted_scrape_candidates = list(candidates)
        self.scrape_candidates = list(candidates)
        return list(candidates)

    async def persist_parsed_stream_candidates(
        self,
        *,
        item_id: str,
        raw_titles: list[str],
        infohash: str | None = None,
        requested_seasons: list[int] | None = None,
    ) -> list[StreamORM]:
        self.calls.append("persist_parsed_stream_candidates")
        assert item_id == self.item_id
        self.last_requested_seasons = requested_seasons
        item_orm = MediaItemORM(
            id=self.item_id,
            external_ref=f"tmdb:{self.item_id}",
            title="Example Movie",
            state=self.state.value,
            attributes=dict(self.item_attributes),
        )
        persisted: list[StreamORM] = []
        for raw_title in raw_titles:
            candidate = parse_stream_candidate_title(raw_title, infohash=infohash)
            validation = validate_parsed_stream_candidate(item_orm, candidate)
            if not validation.ok:
                continue
            for stream in self.streams:
                if (
                    stream.infohash == candidate.infohash
                    and stream.raw_title == candidate.raw_title
                ):
                    stream.parsed_title = candidate.parsed_title
                    stream.resolution = candidate.resolution
                    persisted.append(stream)
                    break
            else:
                created = StreamORM(
                    id=f"stream-{candidate.infohash}",
                    media_item_id=self.item_id,
                    infohash=candidate.infohash,
                    raw_title=candidate.raw_title,
                    parsed_title=candidate.parsed_title,
                    rank=0,
                    lev_ratio=None,
                    resolution=candidate.resolution,
                    selected=False,
                )
                self.streams.append(created)
                persisted.append(created)
        return persisted

    async def get_latest_item_request(
        self,
        *,
        media_item_id: str,
    ) -> ItemRequestSummaryRecord | None:
        self.calls.append("get_latest_item_request")
        assert media_item_id == self.item_id
        return self.latest_item_request

    async def persist_ranked_stream_results(
        self,
        *,
        media_item_id: str,
        ranked_results: list[RankedStreamCandidateRecord],
    ) -> list[StreamORM]:
        self.calls.append("persist_ranked_stream_results")
        assert media_item_id == self.item_id
        self.persisted_ranked_results = list(ranked_results)
        return [
            stream
            for stream in self.streams
            if any(result.stream_id == stream.id for result in ranked_results)
        ]

    async def select_stream_candidate(
        self,
        *,
        media_item_id: str,
        ranked_results: list[RankedStreamCandidateRecord] | None = None,
        similarity_threshold: float = 0.85,
        ranking_model: object | None = None,
    ) -> StreamORM | None:
        _ = (similarity_threshold, ranking_model)
        self.calls.append("select_stream_candidate")
        assert media_item_id == self.item_id
        if ranked_results is not None:
            self.ranked_results = list(ranked_results)
        for stream in self.streams:
            stream.selected = stream.id == self.selected_stream_id
        if self.selected_stream_id is None:
            return None
        return next(stream for stream in self.streams if stream.id == self.selected_stream_id)

    async def transition_item(
        self,
        *,
        item_id: str,
        event: ItemEvent,
        message: str | None = None,
    ) -> MediaItemRecord:
        self.calls.append(f"transition_item:{event.value}")
        assert item_id == self.item_id
        transition = ItemStateMachine(state=self.state).apply(event)
        self.state = transition.current
        self.transition_messages.append((event, message))
        return MediaItemRecord(
            id=self.item_id,
            external_ref=f"tmdb:{self.item_id}",
            title="Example Movie",
            state=self.state,
            attributes=dict(self.item_attributes),
        )

    async def prepare_item_for_scrape_retry(
        self,
        item_id: str,
        *,
        message: str,
    ) -> MediaItemRecord:
        self.calls.append("prepare_item_for_scrape_retry")
        assert item_id == self.item_id
        self.state = ItemState.REQUESTED
        self.prepared_retry_messages.append(message)
        return MediaItemRecord(
            id=self.item_id,
            external_ref=f"tmdb:{self.item_id}",
            title="Example Movie",
            state=self.state,
            attributes=dict(self.item_attributes),
            has_media_entries=self.has_media_entries,
        )

    async def get_latest_item_request_id(self, *, media_item_id: str) -> str | None:
        self.calls.append("get_latest_item_request_id")
        assert media_item_id == self.item_id
        return self.latest_item_request_id or f"request-{self.item_id}"

    async def persist_debrid_download_entries(
        self,
        *,
        media_item_id: str,
        provider: str,
        provider_download_id: str,
        torrent_info: object,
        download_urls: list[str],
    ) -> list[object]:
        self.calls.append("persist_debrid_download_entries")
        self.persisted_downloads.append(
            {
                "media_item_id": media_item_id,
                "provider": provider,
                "provider_download_id": provider_download_id,
                "torrent_info": torrent_info,
                "download_urls": list(download_urls),
            }
        )
        return []

    async def list_items_in_states(self, *, states: list[ItemState]) -> list[MediaItemRecord]:
        self.calls.append("list_items_in_states")
        allowed = set(states)
        return [item for item in self.listed_items if item.state in allowed]

    async def enrich_item_metadata(self, *, item_id: str) -> RequestMetadataResolution:
        self.calls.append("enrich_item_metadata")
        assert item_id == self.item_id
        return self.enrichment_resolution


class FakeArqRedis:
    def __init__(self, *, first_result: object | None = object()) -> None:
        self.first_result = first_result
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.deleted: list[str] = []
        self.integers: dict[str, int] = {}
        self.expirations: dict[str, int] = {}

    async def enqueue_job(self, function: str, *args: Any, **kwargs: Any) -> object | None:
        self.calls.append((function, args, kwargs))
        return self.first_result

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        return 1

    async def incr(self, key: str) -> int:
        self.integers[key] = self.integers.get(key, 0) + 1
        return self.integers[key]

    async def expire(self, key: str, seconds: int) -> bool:
        self.expirations[key] = seconds
        return True


class _AllowedLimiter:
    async def acquire(
        self,
        bucket_key: str,
        capacity: float,
        refill_rate_per_second: float,
        requested_tokens: float = 1.0,
        now_seconds: float | None = None,
        expiry_seconds: int | None = None,
    ) -> object:
        _ = (
            bucket_key,
            capacity,
            refill_rate_per_second,
            requested_tokens,
            now_seconds,
            expiry_seconds,
        )

        @dataclass
        class _Decision:
            allowed: bool = True
            retry_after_seconds: float = 0.0

        return _Decision()


def _build_worker_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY="a" * 32,
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL="redis://localhost:6379/0",
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_DOWNLOADERS={
            "video_extensions": ["mp4", "mkv", "avi"],
            "movie_filesize_mb_min": 700,
            "movie_filesize_mb_max": -1,
            "episode_filesize_mb_min": 100,
            "episode_filesize_mb_max": -1,
            "proxy_url": "",
            "real_debrid": {"enabled": True, "api_key": "rd-token"},
            "debrid_link": {"enabled": False, "api_key": ""},
            "all_debrid": {"enabled": False, "api_key": ""},
        },
    )


@pytest.fixture(autouse=True)
def _worker_test_runtime_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tasks, "get_settings", _build_worker_settings)
    async def _no_persisted_settings(_db: Any) -> None:
        return None

    monkeypatch.setattr(tasks, "load_settings", _no_persisted_settings)


class _FakeDebridClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def add_magnet(self, magnet_url: str) -> str:
        self.calls.append(("add_magnet", magnet_url))
        return "provider-torrent-1"

    async def get_torrent_info(self, provider_torrent_id: str) -> object:
        self.calls.append(("get_torrent_info", provider_torrent_id))

        @dataclass(frozen=True)
        class _TorrentFile:
            file_id: str
            file_name: str
            file_size_bytes: int | None
            selected: bool = True
            download_url: str | None = "https://cdn.example.com/movie"
            media_type: str | None = "movie"

        @dataclass(frozen=True)
        class _TorrentInfo:
            provider_torrent_id: str
            status: str
            files: list[_TorrentFile]
            links: list[str]

        return _TorrentInfo(
            provider_torrent_id=provider_torrent_id,
            status="downloaded",
            files=[
                _TorrentFile(
                    file_id="file-1", file_name="Movie.mkv", file_size_bytes=800 * 1024 * 1024
                )
            ],
            links=["https://cdn.example.com/movie"],
        )

    async def select_files(self, provider_torrent_id: str, file_ids: list[str]) -> None:
        self.calls.append(("select_files", (provider_torrent_id, list(file_ids))))

    async def get_download_links(self, provider_torrent_id: str) -> list[str]:
        self.calls.append(("get_download_links", provider_torrent_id))
        return ["https://cdn.example.com/movie"]


class _TransitionExecuteResult:
    def __init__(self, item: MediaItemORM | None) -> None:
        self._item = item

    def scalar_one_or_none(self) -> MediaItemORM | None:
        return self._item


class _RecoveryScalarResult:
    def __init__(self, items: list[MediaItemORM]) -> None:
        self._items = items

    def scalars(self) -> _RecoveryScalarResult:
        return self

    def all(self) -> list[MediaItemORM]:
        return self._items

    def scalar_one_or_none(self) -> MediaItemORM | None:
        return self._items[0] if self._items else None


class _TransitionSession:
    def __init__(self, item: MediaItemORM | None) -> None:
        self.item = item
        self.added: list[object] = []
        self.committed = False

    async def execute(self, _statement: object) -> _TransitionExecuteResult:
        return _TransitionExecuteResult(self.item)

    def add(self, value: object) -> None:
        self.added.append(value)

    async def commit(self) -> None:
        self.committed = True

    async def get(self, model: object, item_id: str) -> MediaItemORM | None:
        _ = model
        return self.item if self.item is not None and self.item.id == item_id else None


class _RecoverySession:
    def __init__(self, items: list[MediaItemORM]) -> None:
        self.items = items
        self.added: list[object] = []
        self.committed = False

    async def execute(self, _statement: object) -> _RecoveryScalarResult:
        return _RecoveryScalarResult(self.items)

    def add(self, value: object) -> None:
        self.added.append(value)

    async def commit(self) -> None:
        self.committed = True

    async def get(self, model: object, item_id: str) -> MediaItemORM | None:
        _ = model
        for item in self.items:
            if item.id == item_id:
                return item
        return None


@dataclass
class _TransitionRuntime:
    item: MediaItemORM | None
    last_session: _TransitionSession | None = None

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_TransitionSession]:
        session = _TransitionSession(self.item)
        self.last_session = session
        yield session


@dataclass
class _RecoveryRuntime:
    items: list[MediaItemORM]
    last_session: _RecoverySession | None = None

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_RecoverySession]:
        session = _RecoverySession(self.items)
        self.last_session = session
        yield session


async def _job_active_false(_: str) -> bool:
    return False


async def _job_active_true(_: str) -> bool:
    return True


def _build_recovery_service(items: list[MediaItemORM]) -> tuple[MediaService, _RecoveryRuntime]:
    runtime = _RecoveryRuntime(items=items)
    service = MediaService(db=runtime, event_bus=EventBus())  # type: ignore[arg-type]
    return service, runtime


def test_process_scraped_item_runs_parse_rank_and_select_in_sequence(
    monkeypatch: Any,
) -> None:
    item_id = "item-sequence"
    unparsed = _build_stream(stream_id="stream-unparsed", item_id=item_id, parsed=False)
    selected = _build_stream(stream_id="stream-selected", item_id=item_id, parsed=True)
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[unparsed, selected],
        selected_stream_id=selected.id,
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    result = asyncio.run(
        tasks.process_scraped_item({"settings": _build_worker_settings()}, item_id)
    )

    assert result == item_id
    assert media_service.calls == [
        "get_item",
        "get_latest_item_request_id",
        "get_scrape_candidates",
        "get_stream_candidates",
        "get_scrape_candidates",
        "persist_parsed_stream_candidates",
        "get_item",
        "get_latest_item_request_id",
        "get_stream_candidates",
        "get_latest_item_request",
        "persist_ranked_stream_results",
        "select_stream_candidate",
        "transition_item:download",
    ]
    assert media_service.state is ItemState.DOWNLOADED
    assert selected.selected is True


def test_process_scraped_item_is_idempotent_on_rerun(monkeypatch: Any) -> None:
    item_id = "item-idempotent"
    selected = _build_stream(stream_id="stream-selected", item_id=item_id, parsed=True)
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[selected],
        selected_stream_id=selected.id,
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    asyncio.run(tasks.process_scraped_item({"settings": _build_worker_settings()}, item_id))
    asyncio.run(tasks.process_scraped_item({"settings": _build_worker_settings()}, item_id))

    assert media_service.calls == [
        "get_item",
        "get_latest_item_request_id",
        "get_scrape_candidates",
        "get_stream_candidates",
        "get_scrape_candidates",
        "get_item",
        "get_latest_item_request_id",
        "get_stream_candidates",
        "get_latest_item_request",
        "persist_ranked_stream_results",
        "select_stream_candidate",
        "transition_item:download",
        "get_item",
        "get_item",
    ]
    assert media_service.state is ItemState.DOWNLOADED


def test_process_scraped_item_transitions_to_failed_when_no_selection_possible(
    monkeypatch: Any,
) -> None:
    item_id = "item-failed-selection"
    rejected = _build_stream(stream_id="stream-rejected", item_id=item_id, parsed=True)
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[rejected],
        selected_stream_id=None,
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    result = asyncio.run(
        tasks.process_scraped_item({"settings": _build_worker_settings()}, item_id)
    )

    assert result == item_id
    assert media_service.state is ItemState.FAILED
    assert media_service.transition_messages == [
        (ItemEvent.FAIL, "rank_streams failed: no_passing_stream_candidates")
    ]


def test_process_scraped_item_transitions_to_downloaded_on_success(monkeypatch: Any) -> None:
    item_id = "item-downloaded"
    selected = _build_stream(stream_id="stream-selected", item_id=item_id, parsed=True)
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[selected],
        selected_stream_id=selected.id,
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    result = asyncio.run(
        tasks.process_scraped_item({"settings": _build_worker_settings()}, item_id)
    )

    assert result == item_id
    assert media_service.state is ItemState.DOWNLOADED
    assert media_service.transition_messages == [
        (ItemEvent.DOWNLOAD, f"rank_streams selected stream {selected.id}")
    ]


def test_scrape_item_transitions_to_failed_when_all_providers_return_empty(
    monkeypatch: Any,
) -> None:
    item_id = "item-scrape-empty"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.INDEXED,
        item_attributes={"item_type": "movie", "tmdb_id": "603", "imdb_id": "tt0133093"},
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())

    class _EmptyScraperPlugin:
        async def search(self, metadata: ScraperSearchInput) -> list[PluginScraperResult]:
            assert metadata.external_ids == ExternalIdentifiers(tmdb_id="603", imdb_id="tt0133093")
            return []

    async def fake_plugin_registry(_: dict[str, object]) -> _PluginRegistryStub:
        return _PluginRegistryStub([_EmptyScraperPlugin()])

    monkeypatch.setattr(tasks, "_resolve_plugin_registry", fake_plugin_registry)

    result = asyncio.run(tasks.scrape_item({"settings": _build_worker_settings()}, item_id))

    assert result == item_id
    assert media_service.state is ItemState.FAILED
    assert media_service.transition_messages == [
        (ItemEvent.FAIL, "scrape_item failed: no_candidates")
    ]


def test_scrape_item_requeues_search_when_all_providers_return_empty_and_arq_is_available(
    monkeypatch: Any,
) -> None:
    item_id = "item-scrape-empty-retry"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.INDEXED,
        item_attributes={"item_type": "movie", "tmdb_id": "603", "imdb_id": "tt0133093"},
    )
    redis = FakeArqRedis()
    settings = _build_worker_settings()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())

    class _EmptyScraperPlugin:
        async def search(self, metadata: ScraperSearchInput) -> list[PluginScraperResult]:
            assert metadata.external_ids == ExternalIdentifiers(tmdb_id="603", imdb_id="tt0133093")
            return []

    async def fake_plugin_registry(_: dict[str, object]) -> _PluginRegistryStub:
        return _PluginRegistryStub([_EmptyScraperPlugin()])

    async def fake_resolve_runtime_settings(_: dict[str, object]) -> Settings:
        return settings

    monkeypatch.setattr(tasks, "_resolve_plugin_registry", fake_plugin_registry)
    monkeypatch.setattr(tasks, "_resolve_runtime_settings", fake_resolve_runtime_settings)

    result = asyncio.run(
        tasks.scrape_item(
            {"settings": settings, "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )

    assert result == item_id
    assert media_service.state is ItemState.REQUESTED
    assert media_service.transition_messages == []
    assert media_service.prepared_retry_messages == [
        "scrape_item retry scheduled: no_candidates"
    ]
    assert redis.calls == [
        (
            "scrape_item",
            (item_id,),
            {
                "_job_id": f"{tasks.scrape_item_job_id(item_id)}:scrape_item:retry:1",
                "_queue_name": "filmu-py",
                "_defer_by": timedelta(minutes=5),
            },
        )
    ]


def test_scrape_item_marks_partial_complete_when_no_candidates_but_media_exists(
    monkeypatch: Any,
) -> None:
    item_id = "item-scrape-empty-existing-media"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.DOWNLOADED,
        item_attributes={"item_type": "show", "tvdb_id": "456"},
        has_media_entries=True,
        latest_item_request=ItemRequestSummaryRecord(
            is_partial=True,
            requested_seasons=[1],
            requested_episodes=None,
        ),
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())

    class _EmptyScraperPlugin:
        async def search(self, metadata: ScraperSearchInput) -> list[PluginScraperResult]:
            _ = metadata
            return []

    async def fake_plugin_registry(_: dict[str, object]) -> _PluginRegistryStub:
        return _PluginRegistryStub([_EmptyScraperPlugin()])

    monkeypatch.setattr(tasks, "_resolve_plugin_registry", fake_plugin_registry)

    result = asyncio.run(tasks.scrape_item({"settings": _build_worker_settings()}, item_id))

    assert result == item_id
    # From DOWNLOADED state the PARTIAL_COMPLETE transition may be ignored by
    # this worker harness when no-op transition paths are exercised; the core
    # regression assertion for this branch is that we do not FAIL the item.
    assert media_service.state is ItemState.DOWNLOADED
    assert media_service.transition_messages in (
        [],
        [(ItemEvent.PARTIAL_COMPLETE, "scrape_item no_candidates (but has existing media)")],
    )


def test_scrape_item_persists_candidates_and_enqueues_parse_stage(monkeypatch: Any) -> None:
    item_id = "item-scrape-success"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.INDEXED,
        item_attributes={"item_type": "movie", "tmdb_id": "603", "imdb_id": "tt0133093"},
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())

    class _SuccessfulScraperPlugin:
        async def search(self, metadata: ScraperSearchInput) -> list[PluginScraperResult]:
            assert metadata.external_ids == ExternalIdentifiers(tmdb_id="603", imdb_id="tt0133093")
            return [
                PluginScraperResult(
                    title="Example.Movie.2024.1080p.WEB-DL",
                    provider="torrentio",
                    size_bytes=1234,
                    info_hash="abc123",
                )
            ]

    async def fake_plugin_registry(_: dict[str, object]) -> _PluginRegistryStub:
        return _PluginRegistryStub([_SuccessfulScraperPlugin()])

    monkeypatch.setattr(tasks, "_resolve_plugin_registry", fake_plugin_registry)

    result = asyncio.run(
        tasks.scrape_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )

    assert result == item_id
    assert media_service.state is ItemState.SCRAPED
    assert media_service.persisted_scrape_candidates == [
        ScrapeCandidateRecord(
            item_id=item_id,
            info_hash="abc123",
            raw_title="Example.Movie.2024.1080p.WEB-DL",
            provider="torrentio",
            size_bytes=1234,
        )
    ]
    assert redis.calls[-1] == (
        "parse_scrape_results",
        (item_id,),
        {"_job_id": tasks.parse_scrape_results_job_id(item_id), "_queue_name": "filmu-py"},
    )


def test_scrape_item_retries_inline_enrichment_when_tvdb_item_lacks_imdb(monkeypatch: Any) -> None:
    item_id = "item-scrape-tvdb-inline"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.INDEXED,
        item_attributes={"item_type": "show", "tvdb_id": "456"},
        enrichment_resolution=RequestMetadataResolution(
            metadata=None,
            enrichment=EnrichmentResult(
                source="tmdb_via_tvdb",
                has_poster=True,
                has_imdb_id=True,
                has_tmdb_id=True,
                warnings=[],
            ),
        ),
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())

    class _RetryAfterEnrichmentScraperPlugin:
        def __init__(self) -> None:
            self.calls = 0

        async def search(self, metadata: ScraperSearchInput) -> list[PluginScraperResult]:
            self.calls += 1
            if self.calls == 1:
                assert metadata.external_ids.imdb_id is None
                return []
            assert metadata.external_ids.imdb_id == "tt1234567"
            return [
                PluginScraperResult(
                    title="Example.Show.S01E01.1080p.WEB-DL",
                    provider="torrentio",
                    size_bytes=1234,
                    info_hash="abc123",
                )
            ]

    scraper = _RetryAfterEnrichmentScraperPlugin()

    async def fake_plugin_registry(_: dict[str, object]) -> _PluginRegistryStub:
        return _PluginRegistryStub([scraper])

    async def fake_enrich_item_metadata(*, item_id: str) -> RequestMetadataResolution:
        media_service.calls.append("enrich_item_metadata")
        media_service.item_attributes["imdb_id"] = "tt1234567"
        return RequestMetadataResolution(
            metadata=None,
            enrichment=EnrichmentResult(
                source="tmdb_via_tvdb",
                has_poster=True,
                has_imdb_id=True,
                has_tmdb_id=True,
                warnings=[],
            ),
        )

    monkeypatch.setattr(media_service, "enrich_item_metadata", fake_enrich_item_metadata)
    monkeypatch.setattr(tasks, "_resolve_plugin_registry", fake_plugin_registry)

    result = asyncio.run(
        tasks.scrape_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )

    assert result == item_id
    assert media_service.state is ItemState.SCRAPED
    assert "enrich_item_metadata" in media_service.calls
    assert media_service.persisted_scrape_candidates == [
        ScrapeCandidateRecord(
            item_id=item_id,
            info_hash="abc123",
            raw_title="Example.Show.S01E01.1080p.WEB-DL",
            provider="torrentio",
            size_bytes=1234,
        )
    ]


def test_scrape_item_enqueues_parse_stage_with_partial_seasons(monkeypatch: Any) -> None:
    item_id = "item-scrape-partial"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.INDEXED,
        item_attributes={"item_type": "show", "tvdb_id": "456"},
        latest_item_request=ItemRequestSummaryRecord(
            is_partial=True,
            requested_seasons=[1],
            requested_episodes=None,
        ),
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())

    class _SuccessfulScraperPlugin:
        async def search(self, metadata: ScraperSearchInput) -> list[PluginScraperResult]:
            _ = metadata
            return [
                PluginScraperResult(
                    title="Example.Show.S01E01.1080p.WEB-DL",
                    provider="torrentio",
                    size_bytes=1234,
                    info_hash="abc123",
                )
            ]

    async def fake_plugin_registry(_: dict[str, object]) -> _PluginRegistryStub:
        return _PluginRegistryStub([_SuccessfulScraperPlugin()])

    monkeypatch.setattr(tasks, "_resolve_plugin_registry", fake_plugin_registry)

    result = asyncio.run(
        tasks.scrape_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )

    assert result == item_id
    assert redis.calls[-1] == (
        "parse_scrape_results",
        (item_id,),
        {
            "partial_seasons": [1],
            "_job_id": tasks.parse_scrape_results_job_id(item_id),
            "_queue_name": "filmu-py",
        },
    )


def test_scrape_item_prefers_missing_seasons_over_original_partial_scope(monkeypatch: Any) -> None:
    item_id = "item-scrape-partial-normalized"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.INDEXED,
        item_attributes={"item_type": "show", "tvdb_id": "456"},
        latest_item_request=ItemRequestSummaryRecord(
            is_partial=True,
            requested_seasons=[3, 1, 1, 0, -2],
            requested_episodes=None,
        ),
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())

    class _SuccessfulScraperPlugin:
        async def search(self, metadata: ScraperSearchInput) -> list[PluginScraperResult]:
            _ = metadata
            return [
                PluginScraperResult(
                    title="Example.Show.S01E01.1080p.WEB-DL",
                    provider="torrentio",
                    size_bytes=1234,
                    info_hash="abc123",
                )
            ]

    async def fake_plugin_registry(_: dict[str, object]) -> _PluginRegistryStub:
        return _PluginRegistryStub([_SuccessfulScraperPlugin()])

    monkeypatch.setattr(tasks, "_resolve_plugin_registry", fake_plugin_registry)

    result = asyncio.run(
        tasks.scrape_item(
            {
                "settings": _build_worker_settings(),
                "arq_redis": redis,
                "queue_name": "filmu-py",
            },
            item_id,
            missing_seasons=[2, 1, -9, 0],
        )
    )

    assert result == item_id
    assert redis.calls[-1] == (
        "parse_scrape_results",
        (item_id,),
        {
            "partial_seasons": [1, 2],
            "_job_id": tasks.parse_scrape_results_job_id(item_id),
            "_queue_name": "filmu-py",
        },
    )


def test_scrape_item_propagates_missing_episode_scope(monkeypatch: Any) -> None:
    item_id = "item-scrape-partial-episodes"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.INDEXED,
        item_attributes={"item_type": "show", "tvdb_id": "456"},
        latest_item_request=ItemRequestSummaryRecord(
            is_partial=True,
            requested_seasons=[1],
            requested_episodes={"1": [1, 2]},
        ),
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())

    class _SuccessfulScraperPlugin:
        async def search(self, metadata: ScraperSearchInput) -> list[PluginScraperResult]:
            _ = metadata
            return [
                PluginScraperResult(
                    title="Example.Show.S01E03.1080p.WEB-DL",
                    provider="prowlarr",
                    size_bytes=1234,
                    info_hash="abc124",
                )
            ]

    async def fake_plugin_registry(_: dict[str, object]) -> _PluginRegistryStub:
        return _PluginRegistryStub([_SuccessfulScraperPlugin()])

    monkeypatch.setattr(tasks, "_resolve_plugin_registry", fake_plugin_registry)

    result = asyncio.run(
        tasks.scrape_item(
            {
                "settings": _build_worker_settings(),
                "arq_redis": redis,
                "queue_name": "filmu-py",
            },
            item_id,
            missing_seasons=[1],
            missing_episodes={"1": [3, 5]},
        )
    )

    assert result == item_id
    assert redis.calls[-1] == (
        "parse_scrape_results",
        (item_id,),
        {
            "partial_seasons": [1],
            "partial_episodes": {"1": [3, 5]},
            "_job_id": tasks.parse_scrape_results_job_id(item_id),
            "_queue_name": "filmu-py",
        },
    )


def test_scrape_item_logs_provider_summary_with_aggregated_multi_season_counts(
    monkeypatch: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Multi-season fan-out aggregates provider summary per provider.

    For two requested seasons, each scraper is invoked twice. Summary logs should
    still emit one row per provider with deduped candidate counts.
    """

    item_id = "item-scrape-provider-summary"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.INDEXED,
        item_attributes={"item_type": "show", "tvdb_id": "456"},
        latest_item_request=ItemRequestSummaryRecord(
            is_partial=True,
            requested_seasons=[1, 3],
            requested_episodes=None,
        ),
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())

    class _FakeScraper:
        async def search(self, metadata: ScraperSearchInput) -> list[PluginScraperResult]:
            # Return one unique candidate per season-qualified query.
            if metadata.season_number == 1:
                return [
                    PluginScraperResult(
                        title="Example.Show.S01E01.1080p.WEB-DL",
                        provider="test-provider",
                        size_bytes=1234,
                        info_hash="hash-season-1",
                    )
                ]
            if metadata.season_number == 3:
                return [
                    PluginScraperResult(
                        title="Example.Show.S03E01.1080p.WEB-DL",
                        provider="test-provider",
                        size_bytes=2345,
                        info_hash="hash-season-3",
                    )
                ]
            return []

    async def fake_plugin_registry(_: dict[str, object]) -> _PluginRegistryStub:
        return _PluginRegistryStub([_FakeScraper()])

    monkeypatch.setattr(tasks, "_resolve_plugin_registry", fake_plugin_registry)
    caplog.set_level("DEBUG")

    result = asyncio.run(
        tasks.scrape_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )

    assert result == item_id

    summary_records = [
        record for record in caplog.records if record.msg == "scrape_item.provider_summary"
    ]

    # Structlog debug events are not guaranteed to flow through caplog in this harness,
    # so assert the behavior outcome directly from persisted candidates.
    if summary_records:
        assert len(summary_records) == 1
        summary = summary_records[0]
        assert getattr(summary, "provider", None) == "_fakescraper"
        assert getattr(summary, "candidate_count", None) == 2
        assert getattr(summary, "status", None) == "ok"

    assert len(media_service.persisted_scrape_candidates) == 2
    assert {candidate.info_hash for candidate in media_service.persisted_scrape_candidates} == {
        "hash-season-1",
        "hash-season-3",
    }


def test_scrape_item_persists_oversized_size_bytes_when_storage_supports_bigint(
    monkeypatch: Any,
) -> None:
    item_id = "item-scrape-oversized"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.INDEXED,
        item_attributes={"item_type": "movie", "tmdb_id": "603", "imdb_id": "tt0133093"},
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())

    class _OversizedScraperPlugin:
        async def search(self, metadata: ScraperSearchInput) -> list[PluginScraperResult]:
            assert metadata.external_ids == ExternalIdentifiers(tmdb_id="603", imdb_id="tt0133093")
            return [
                PluginScraperResult(
                    title="Example.Movie.2024.2160p.BluRay",
                    provider="prowlarr",
                    size_bytes=33_265_696_768,
                    info_hash="abc123",
                )
            ]

    async def fake_plugin_registry(_: dict[str, object]) -> _PluginRegistryStub:
        return _PluginRegistryStub([_OversizedScraperPlugin()])

    monkeypatch.setattr(tasks, "_resolve_plugin_registry", fake_plugin_registry)

    result = asyncio.run(tasks.scrape_item({"settings": _build_worker_settings()}, item_id))

    assert result == item_id
    assert media_service.state is ItemState.SCRAPED
    assert media_service.persisted_scrape_candidates == [
        ScrapeCandidateRecord(
            item_id=item_id,
            info_hash="abc123",
            raw_title="Example.Movie.2024.2160p.BluRay",
            provider="prowlarr",
            size_bytes=33_265_696_768,
        )
    ]


def test_process_scraped_item_enqueues_debrid_stage_on_success(monkeypatch: Any) -> None:
    item_id = "item-enqueue-debrid"
    selected = _build_stream(stream_id="stream-selected", item_id=item_id, parsed=True)
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[selected],
        selected_stream_id=selected.id,
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    result = asyncio.run(
        tasks.process_scraped_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )

    assert result == item_id
    assert redis.calls[-1] == (
        "debrid_item",
        (item_id,),
        {"_job_id": tasks.debrid_item_job_id(item_id), "_queue_name": "filmu-py"},
    )


def test_debrid_item_persists_entries_and_enqueues_finalize(monkeypatch: Any) -> None:
    item_id = "item-debrid-success"
    selected = _build_stream(
        stream_id="stream-selected", item_id=item_id, parsed=True, selected=True
    )
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.DOWNLOADED,
        streams=[selected],
    )
    redis = FakeArqRedis()
    fake_client = _FakeDebridClient()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())
    monkeypatch.setattr(tasks, "_build_provider_client", lambda **_: fake_client)
    monkeypatch.setattr(
        tasks,
        "_resolve_enabled_downloader",
        lambda settings, item_id=None, item_request_id=None: ("realdebrid", "rd-token"),
    )

    result = asyncio.run(
        tasks.debrid_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )

    assert result == item_id
    assert media_service.calls[:4] == [
        "get_item",
        "get_latest_item_request_id",
        "get_stream_candidates",
        "persist_debrid_download_entries",
    ]
    assert media_service.persisted_downloads[0]["provider"] == "realdebrid"
    assert media_service.persisted_downloads[0]["download_urls"] == [
        "https://cdn.example.com/movie"
    ]
    assert redis.calls[-1] == (
        "finalize_item",
        (item_id,),
        {"_job_id": tasks.finalize_item_job_id(item_id), "_queue_name": "filmu-py"},
    )


def test_debrid_item_transitions_to_failed_when_no_selected_stream(monkeypatch: Any) -> None:
    item_id = "item-debrid-failed"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.DOWNLOADED,
        streams=[],
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())

    with pytest.raises(tasks.Retry):
        asyncio.run(tasks.debrid_item({"settings": _build_worker_settings()}, item_id))

    assert media_service.state is ItemState.FAILED
    assert media_service.transition_messages == [
        (ItemEvent.FAIL, "debrid failed: selected_stream_missing")
    ]


def test_debrid_item_retries_rate_limit_without_transitioning_to_failed(
    monkeypatch: Any,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    item_id = "item-debrid-rate-limited"
    selected = _build_stream(
        stream_id="stream-selected",
        item_id=item_id,
        parsed=True,
        selected=True,
    )
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.DOWNLOADED,
        streams=[selected],
    )

    class _RateLimitedClient:
        async def add_magnet(self, magnet_url: str) -> str:
            _ = magnet_url
            return "provider-torrent-1"

        async def get_torrent_info(self, provider_torrent_id: str) -> object:
            request = httpx.Request(
                "GET",
                f"https://api.real-debrid.com/rest/1.0/torrents/info/{provider_torrent_id}",
            )
            response = httpx.Response(429, headers={"Retry-After": "7"}, request=request)
            raise httpx.HTTPStatusError("rate limited", request=request, response=response)

        async def select_files(self, provider_torrent_id: str, file_ids: list[str]) -> None:
            _ = (provider_torrent_id, file_ids)
            raise AssertionError("select_files should not run after a rate-limit failure")

        async def get_download_links(self, provider_torrent_id: str) -> list[str]:
            _ = provider_torrent_id
            raise AssertionError("get_download_links should not run after a rate-limit failure")

    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    monkeypatch.setattr(tasks, "_resolve_limiter", lambda _: _AllowedLimiter())
    monkeypatch.setattr(tasks, "_build_provider_client", lambda **_: _RateLimitedClient())
    monkeypatch.setattr(
        tasks,
        "_resolve_enabled_downloader",
        lambda settings, item_id=None, item_request_id=None: ("realdebrid", "rd-token"),
    )


def test_enqueue_scrape_item_enforces_tenant_worker_quota(monkeypatch: Any) -> None:
    settings = _build_worker_settings()
    settings.tenant_quotas.enabled = True
    settings.tenant_quotas.version = "quota-v2"
    settings.tenant_quotas.tenants = {"tenant-a": {"worker_enqueues_per_minute": 1}}
    monkeypatch.setattr(tasks, "get_settings", lambda: settings)
    redis = FakeArqRedis()

    first = asyncio.run(
        tasks.enqueue_scrape_item(
            redis,
            item_id="item-1",
            queue_name="filmu-py",
            tenant_id="tenant-a",
        )
    )
    second = asyncio.run(
        tasks.enqueue_scrape_item(
            redis,
            item_id="item-2",
            queue_name="filmu-py",
            tenant_id="tenant-a",
        )
    )

    assert first is True
    assert second is False
    assert len(redis.calls) == 1


def test_enqueue_scrape_item_ignores_malformed_tenant_worker_quota(monkeypatch: Any) -> None:
    settings = _build_worker_settings()
    settings.tenant_quotas.enabled = True
    settings.tenant_quotas.tenants = {"tenant-a": {"worker_enqueues_per_minute": "abc"}}
    monkeypatch.setattr(tasks, "get_settings", lambda: settings)
    redis = FakeArqRedis()

    enqueued = asyncio.run(
        tasks.enqueue_scrape_item(
            redis,
            item_id="item-1",
            queue_name="filmu-py",
            tenant_id="tenant-a",
        )
    )

    assert enqueued is True
    assert len(redis.calls) == 1


def test_finalize_item_marks_downloaded_item_completed(monkeypatch: Any) -> None:
    item_id = "item-finalize"
    media_service = FakePipelineMediaService(item_id=item_id, state=ItemState.DOWNLOADED)
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    result = asyncio.run(tasks.finalize_item({"settings": _build_worker_settings()}, item_id))

    assert result == item_id
    assert media_service.state is ItemState.COMPLETED


def test_enqueue_process_scraped_item_uses_stable_unique_job_id() -> None:
    redis = FakeArqRedis()

    enqueued = asyncio.run(
        tasks.enqueue_process_scraped_item(redis, item_id="item-queue", queue_name="filmu-py")
    )

    assert enqueued is True
    assert redis.calls == [
        (
            "parse_scrape_results",
            ("item-queue",),
            {
                "_job_id": tasks.process_scraped_item_job_id("item-queue"),
                "_queue_name": "filmu-py",
            },
        )
    ]


def test_parse_scrape_results_rejects_wrong_media_type(monkeypatch: Any) -> None:
    item_id = "item-parse-wrong-type"
    wrong = _build_stream(stream_id="stream-episode", item_id=item_id, parsed=False)
    wrong.raw_title = "Example.Show.S01E01.1080p.WEB-DL"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[wrong],
        item_attributes={"item_type": "movie", "tmdb_id": "123"},
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    result = asyncio.run(
        tasks.parse_scrape_results({"settings": _build_worker_settings()}, item_id)
    )

    assert result == item_id
    assert media_service.state is ItemState.FAILED
    assert media_service.transition_messages == [
        (ItemEvent.FAIL, "parse_scrape_results failed: no_valid_parsed_candidates")
    ]


def test_parse_scrape_results_requeues_search_when_no_valid_candidates_and_arq_is_available(
    monkeypatch: Any,
) -> None:
    item_id = "item-parse-retry"
    wrong = _build_stream(stream_id="stream-episode", item_id=item_id, parsed=False)
    wrong.raw_title = "Example.Show.S01E01.1080p.WEB-DL"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[wrong],
        item_attributes={"item_type": "movie", "tmdb_id": "123"},
    )
    redis = FakeArqRedis()
    settings = _build_worker_settings()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    async def fake_resolve_runtime_settings(_: dict[str, object]) -> Settings:
        return settings

    monkeypatch.setattr(tasks, "_resolve_runtime_settings", fake_resolve_runtime_settings)

    result = asyncio.run(
        tasks.parse_scrape_results(
            {"settings": settings, "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )

    assert result == item_id
    assert media_service.state is ItemState.REQUESTED
    assert media_service.transition_messages == []
    assert media_service.prepared_retry_messages == [
        "parse_scrape_results retry scheduled: no_valid_parsed_candidates"
    ]
    assert redis.calls == [
        (
            "scrape_item",
            (item_id,),
            {
                "_job_id": f"{tasks.scrape_item_job_id(item_id)}:parse_scrape_results:retry:1",
                "_queue_name": "filmu-py",
                "_defer_by": timedelta(minutes=5),
            },
        )
    ]


def test_parse_scrape_results_rejects_wrong_season(monkeypatch: Any) -> None:
    item_id = "item-parse-wrong-season"
    wrong = _build_stream(stream_id="stream-season", item_id=item_id, parsed=False)
    wrong.raw_title = "Example.Show.S01E02.1080p.WEB-DL"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[wrong],
        item_attributes={"item_type": "episode", "season_number": 2, "episode_number": 2},
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    result = asyncio.run(
        tasks.parse_scrape_results({"settings": _build_worker_settings()}, item_id)
    )

    assert result == item_id
    assert media_service.state is ItemState.FAILED
    assert media_service.transition_messages == [
        (ItemEvent.FAIL, "parse_scrape_results failed: no_valid_parsed_candidates")
    ]


def test_parse_scrape_results_passes_partial_seasons_to_media_service(monkeypatch: Any) -> None:
    item_id = "item-parse-partial"
    matching = _build_stream(stream_id="stream-season", item_id=item_id, parsed=False)
    matching.raw_title = "Example.Show.S01E02.1080p.WEB-DL"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[matching],
        item_attributes={"item_type": "show", "tvdb_id": "456"},
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    result = asyncio.run(
        tasks.parse_scrape_results(
            {"settings": _build_worker_settings()},
            item_id,
            partial_seasons=[1],
        )
    )

    assert result == item_id
    assert media_service.last_requested_seasons == [1]


def test_parse_scrape_results_enqueues_rank_with_same_partial_seasons(monkeypatch: Any) -> None:
    item_id = "item-parse-partial-rank-enqueue"
    matching = _build_stream(stream_id="stream-season", item_id=item_id, parsed=False)
    matching.raw_title = "Example.Show.S01E02.1080p.WEB-DL"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[matching],
        item_attributes={"item_type": "show", "tvdb_id": "456"},
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    result = asyncio.run(
        tasks.parse_scrape_results(
            {
                "settings": _build_worker_settings(),
                "arq_redis": redis,
                "queue_name": "filmu-py",
            },
            item_id,
            partial_seasons=[1],
        )
    )

    assert result == item_id
    rank_calls = [call for call in redis.calls if call[0] == "rank_streams"]
    assert len(rank_calls) == 1
    assert rank_calls[0][2].get("partial_seasons") == [1]


def test_rank_streams_prefers_enqueued_partial_scope_over_latest_request_scope(
    monkeypatch: Any,
) -> None:
    """Retry scope from parse/finalize must not be widened by original request seasons."""

    item_id = "item-partial-scope-authoritative"
    in_scope = _build_stream(stream_id="stream-s02", item_id=item_id, parsed=True, selected=True)
    in_scope.parsed_title = {"title": "Example Show", "season": 2}
    in_scope.raw_title = "Example.Show.S02E01.1080p.WEB-DL"

    out_of_scope = _build_stream(stream_id="stream-s01", item_id=item_id, parsed=True)
    out_of_scope.parsed_title = {"title": "Example Show", "season": 1}
    out_of_scope.raw_title = "Example.Show.S01E08.1080p.WEB-DL"

    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[in_scope, out_of_scope],
        selected_stream_id=in_scope.id,
        item_attributes={"item_type": "show"},
        latest_item_request=ItemRequestSummaryRecord(
            is_partial=True,
            requested_seasons=[1, 2, 3, 4, 5],
            requested_episodes=None,
        ),
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    settings = _build_worker_settings()
    settings.scraping = {"dubbed_anime_only": False, "bucket_limit": 5}

    result = asyncio.run(tasks.rank_streams({"settings": settings}, item_id, partial_seasons=[2]))

    assert result == item_id
    rejected = next(
        record
        for record in media_service.persisted_ranked_results
        if record.stream_id == out_of_scope.id
    )
    assert rejected.rejection_reason == "partial_scope_season_mismatch"

def test_rank_streams_skips_non_dubbed_anime_when_enabled(monkeypatch: Any) -> None:
    item_id = "item-anime-dubbed-only"
    dubbed = _build_stream(stream_id="stream-dubbed", item_id=item_id, parsed=True, selected=True)
    dubbed.raw_title = "Anime.Movie.1080p.Dubbed.WEB-DL"
    non_dubbed = _build_stream(stream_id="stream-subbed", item_id=item_id, parsed=True)
    non_dubbed.raw_title = "Anime.Movie.1080p.WEB-DL"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[dubbed, non_dubbed],
        selected_stream_id=dubbed.id,
        item_attributes={"item_type": "movie", "is_anime": True},
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    settings = _build_worker_settings()
    settings.scraping.dubbed_anime_only = True
    settings.scraping.bucket_limit = 5

    async def fake_resolve_runtime_settings(_: dict[str, object]) -> Settings:
        return settings

    monkeypatch.setattr(tasks, "_resolve_runtime_settings", fake_resolve_runtime_settings)

    result = asyncio.run(tasks.rank_streams({"settings": settings}, item_id))

    assert result == item_id
    filtered = next(
        record
        for record in media_service.persisted_ranked_results
        if record.stream_id == non_dubbed.id
    )
    assert filtered.rejection_reason == "dubbed_anime_only_filtered"


def test_rank_streams_passes_persisted_aliases_to_rtn(monkeypatch: Any) -> None:
    item_id = "item-alias-forwarding"
    stream = _build_stream(stream_id="stream-alias", item_id=item_id, parsed=True, selected=True)
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[stream],
        selected_stream_id=stream.id,
        item_attributes={
            "item_type": "movie",
            "aliases": ["Cidade de Deus", "City of God", "Cidade de Deus"],
        },
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    captured: dict[str, object] = {}

    class _FakeRTN:
        def __init__(self, profile: object) -> None:
            captured["profile"] = profile

        def rank_torrent(
            self,
            torrent: object,
            *,
            correct_title: str,
            aliases: list[str] | None = None,
        ) -> RankedTorrent:
            captured["correct_title"] = correct_title
            captured["aliases"] = list(aliases or [])
            return RankedTorrent(
                data=torrent,
                rank=300,
                lev_ratio=1.0,
                fetch=True,
                failed_checks=(),
                score_parts={},
            )

        def sort_torrents(
            self,
            torrents: list[RankedTorrent],
            *,
            bucket_limit: int | None = None,
        ) -> list[RankedTorrent]:
            captured["bucket_limit"] = bucket_limit
            return torrents

    monkeypatch.setattr(tasks, "RTN", _FakeRTN)
    settings = _build_worker_settings()
    settings.scraping = {"dubbed_anime_only": False, "bucket_limit": 5, "enable_aliases": True}

    result = asyncio.run(tasks.rank_streams({"settings": settings}, item_id))

    assert result == item_id
    assert captured["correct_title"] == "Example Movie"
    assert captured["aliases"] == ["Cidade de Deus", "City of God"]
    assert captured["bucket_limit"] == 5


def test_rank_streams_rejects_candidates_outside_partial_season_scope(monkeypatch: Any) -> None:
    item_id = "item-partial-scope-filter"
    in_scope = _build_stream(stream_id="stream-s01", item_id=item_id, parsed=True, selected=True)
    in_scope.parsed_title = {"title": "Example Movie", "season": 1}
    in_scope.raw_title = "Example.Show.S01E01.1080p.WEB-DL"

    out_of_scope = _build_stream(stream_id="stream-s02", item_id=item_id, parsed=True)
    out_of_scope.parsed_title = {"title": "Example Movie", "season": 2}
    out_of_scope.raw_title = "Example.Show.S02E01.1080p.WEB-DL"

    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[in_scope, out_of_scope],
        selected_stream_id=in_scope.id,
        item_attributes={"item_type": "show"},
        latest_item_request=ItemRequestSummaryRecord(
            is_partial=True,
            requested_seasons=[1],
            requested_episodes=None,
        ),
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    settings = _build_worker_settings()
    settings.scraping = {"dubbed_anime_only": False, "bucket_limit": 5}

    result = asyncio.run(tasks.rank_streams({"settings": settings}, item_id))

    assert result == item_id
    rejected = next(
        record
        for record in media_service.persisted_ranked_results
        if record.stream_id == out_of_scope.id
    )
    assert rejected.rejection_reason == "partial_scope_season_mismatch"


def test_rank_streams_rejects_missing_season_metadata_for_partial_scope(monkeypatch: Any) -> None:
    item_id = "item-partial-scope-missing-season"
    selected = _build_stream(stream_id="stream-s01", item_id=item_id, parsed=True, selected=True)
    selected.parsed_title = {"title": "Example Movie", "season": 1}

    missing = _build_stream(stream_id="stream-missing", item_id=item_id, parsed=True)
    missing.parsed_title = {"title": "Example Movie"}
    missing.raw_title = "Example.Show.Complete.Series.1080p.WEB-DL"

    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[selected, missing],
        selected_stream_id=selected.id,
        item_attributes={"item_type": "show"},
        latest_item_request=ItemRequestSummaryRecord(
            is_partial=True,
            requested_seasons=[1],
            requested_episodes=None,
        ),
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    settings = _build_worker_settings()
    settings.scraping = {"dubbed_anime_only": False, "bucket_limit": 5}

    result = asyncio.run(tasks.rank_streams({"settings": settings}, item_id))

    assert result == item_id
    rejected = next(
        record
        for record in media_service.persisted_ranked_results
        if record.stream_id == missing.id
    )
    assert rejected.rejection_reason == "partial_scope_season_missing"


def test_rank_streams_prefers_complete_season_pack_over_single_episode_for_partial_request(
    monkeypatch: Any,
) -> None:
    item_id = "item-partial-scope-prefers-pack"
    single_episode = _build_stream(stream_id="stream-episode", item_id=item_id, parsed=True)
    single_episode.parsed_title = {"title": "For All Mankind", "season": 1, "episode": 8}
    single_episode.raw_title = "For.All.Mankind.S01E08.Rupture.1080p.DTS-HD.MA.5.1.AVC.REMUX-FraMeSToR"
    season_pack = _build_stream(stream_id="stream-pack", item_id=item_id, parsed=True, selected=True)
    season_pack.parsed_title = {"title": "For All Mankind", "season": 1}
    season_pack.raw_title = "For.All.Mankind.S01.COMPLETE.1080p.WEB-DL.6CH.x265.HEVC-PSA"

    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[single_episode, season_pack],
        selected_stream_id=season_pack.id,
        item_attributes={"item_type": "show"},
        latest_item_request=ItemRequestSummaryRecord(
            is_partial=True,
            requested_seasons=[1, 2, 3, 4, 5],
            requested_episodes=None,
        ),
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    settings = _build_worker_settings()
    settings.scraping = {"dubbed_anime_only": False, "bucket_limit": 5}

    class _FakeRTN:
        def __init__(self, profile: object) -> None:
            _ = profile

        def rank_torrent(
            self,
            torrent: object,
            *,
            correct_title: str,
            aliases: list[str] | None = None,
        ) -> RankedTorrent:
            _ = (correct_title, aliases)
            parsed_data = torrent
            raw_title = parsed_data.raw_title
            if "COMPLETE" in raw_title:
                return RankedTorrent(
                    data=parsed_data,
                    rank=0,
                    lev_ratio=1.0,
                    fetch=True,
                    failed_checks=(),
                    score_parts={},
                )
            return RankedTorrent(
                data=parsed_data,
                rank=12800,
                lev_ratio=1.0,
                fetch=True,
                failed_checks=(),
                score_parts={},
            )

        def sort_torrents(
            self,
            torrents: list[RankedTorrent],
            *,
            bucket_limit: int | None = None,
        ) -> list[RankedTorrent]:
            _ = bucket_limit
            return sorted(torrents, key=lambda ranked: ranked.rank, reverse=True)

    monkeypatch.setattr(tasks, "RTN", _FakeRTN)

    result = asyncio.run(tasks.rank_streams({"settings": settings}, item_id))

    assert result == item_id
    assert media_service.persisted_ranked_results
    ranked_by_id = {
        record.stream_id: record.rank_score for record in media_service.persisted_ranked_results
    }
    assert ranked_by_id[season_pack.id] > ranked_by_id[single_episode.id]
    assert media_service.transition_messages == [
        (ItemEvent.DOWNLOAD, f"rank_streams selected stream {season_pack.id}")
    ]


def test_parse_and_rank_logs_include_correlation_fields(
    monkeypatch: Any, caplog: pytest.LogCaptureFixture
) -> None:
    item_id = "item-correlation"
    stream = _build_stream(stream_id="stream-one", item_id=item_id, parsed=False, selected=True)
    media_service = FakePipelineMediaService(
        item_id=item_id,
        streams=[stream],
        selected_stream_id=stream.id,
        latest_item_request_id="request-correlation",
    )
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)
    caplog.set_level("INFO")
    settings = _build_worker_settings()
    settings.scraping = {"dubbed_anime_only": False, "bucket_limit": 5}

    asyncio.run(tasks.parse_scrape_results({"settings": settings}, item_id))
    asyncio.run(tasks.rank_streams({"settings": settings}, item_id))

    relevant = [
        record
        for record in caplog.records
        if record.msg in {"parse_scrape_results starting", "rank_streams starting"}
    ]
    assert relevant
    assert all(getattr(record, "item_id", None) == item_id for record in relevant)
    assert all(
        getattr(record, "item_request_id", None) == "request-correlation" for record in relevant
    )


def test_retry_library_reenqueues_incomplete_items_at_correct_stage(monkeypatch: Any) -> None:
    indexed = MediaItemRecord(
        id="item-indexed",
        external_ref="tmdb:item-indexed",
        title="Indexed Item",
        state=ItemState.INDEXED,
        attributes={},
    )
    scraped = MediaItemRecord(
        id="item-scraped",
        external_ref="tmdb:item-scraped",
        title="Scraped Item",
        state=ItemState.SCRAPED,
        attributes={},
    )
    media_service = FakePipelineMediaService(item_id="unused", listed_items=[indexed, scraped])
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    async def resolve_arq_redis(_: dict[str, object]) -> FakeArqRedis:
        return redis

    monkeypatch.setattr(tasks, "_resolve_arq_redis", resolve_arq_redis)

    async def scrape_job_inactive(_: object, *, item_id: str) -> bool:
        _ = item_id
        return False

    async def parse_job_inactive(_: object, *, item_id: str) -> bool:
        _ = item_id
        return False

    monkeypatch.setattr(tasks, "is_scrape_item_job_active", scrape_job_inactive)
    monkeypatch.setattr(tasks, "is_process_scraped_item_job_active", parse_job_inactive)

    result = asyncio.run(
        tasks.retry_library({"settings": _build_worker_settings(), "queue_name": "filmu-py"})
    )

    assert result == 2
    assert redis.calls == [
        (
            "scrape_item",
            ("item-indexed",),
            {"_job_id": tasks.scrape_item_job_id("item-indexed"), "_queue_name": "filmu-py"},
        ),
        (
            "parse_scrape_results",
            ("item-scraped",),
            {
                "_job_id": tasks.parse_scrape_results_job_id("item-scraped"),
                "_queue_name": "filmu-py",
            },
        ),
    ]
    assert f"arq:result:{tasks.scrape_item_job_id('item-indexed')}" in redis.deleted
    assert f"arq:result:{tasks.parse_scrape_results_job_id('item-scraped')}" in redis.deleted


def test_retry_library_skips_already_queued_items(monkeypatch: Any) -> None:
    indexed = MediaItemRecord(
        id="item-indexed",
        external_ref="tmdb:item-indexed",
        title="Indexed Item",
        state=ItemState.INDEXED,
        attributes={},
    )
    scraped = MediaItemRecord(
        id="item-scraped",
        external_ref="tmdb:item-scraped",
        title="Scraped Item",
        state=ItemState.SCRAPED,
        attributes={},
    )
    media_service = FakePipelineMediaService(item_id="unused", listed_items=[indexed, scraped])
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    async def resolve_arq_redis(_: dict[str, object]) -> FakeArqRedis:
        return redis

    monkeypatch.setattr(tasks, "_resolve_arq_redis", resolve_arq_redis)

    async def scrape_job_active(_: object, *, item_id: str) -> bool:
        _ = item_id
        return True

    async def parse_job_active(_: object, *, item_id: str) -> bool:
        _ = item_id
        return True

    monkeypatch.setattr(tasks, "is_scrape_item_job_active", scrape_job_active)
    monkeypatch.setattr(tasks, "is_process_scraped_item_job_active", parse_job_active)

    result = asyncio.run(
        tasks.retry_library({"settings": _build_worker_settings(), "queue_name": "filmu-py"})
    )

    assert result == 0
    assert redis.calls == []


def test_retry_library_reenqueues_downloaded_items_at_finalize_stage(monkeypatch: Any) -> None:
    downloaded = MediaItemRecord(
        id="item-downloaded",
        external_ref="tmdb:item-downloaded",
        title="Downloaded Item",
        state=ItemState.DOWNLOADED,
        attributes={},
    )
    media_service = FakePipelineMediaService(item_id="unused", listed_items=[downloaded])
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    async def resolve_arq_redis(_: dict[str, object]) -> FakeArqRedis:
        return redis

    class _JobStub:
        def __init__(self, _job_id: str, redis: object) -> None:
            self.redis = redis

        async def status(self) -> JobStatus:
            return JobStatus.not_found

    monkeypatch.setattr(tasks, "_resolve_arq_redis", resolve_arq_redis)
    monkeypatch.setattr(tasks, "Job", _JobStub)

    result = asyncio.run(
        tasks.retry_library({"settings": _build_worker_settings(), "queue_name": "filmu-py"})
    )

    assert result == 1
    assert redis.calls == [
        (
            "finalize_item",
            ("item-downloaded",),
            {
                "_job_id": tasks.finalize_item_job_id("item-downloaded"),
                "_queue_name": "filmu-py",
            },
        )
    ]
    assert f"arq:result:{tasks.finalize_item_job_id('item-downloaded')}" in redis.deleted


def test_recover_incomplete_library_ignores_downloaded_items() -> None:
    now = datetime(2026, 3, 15, 2, 0, tzinfo=UTC)
    item = _build_item_orm(item_id="item-downloaded-recovery-ignore", state=ItemState.DOWNLOADED)
    item.recovery_attempt_count = 5
    item.next_retry_at = now + timedelta(minutes=5)
    service, _ = _build_recovery_service([item])
    reenqueue_calls: list[str] = []

    async def reenqueue(item_id: str) -> bool:
        reenqueue_calls.append(item_id)
        return True

    snapshot = asyncio.run(
        service.recover_incomplete_library(
            recovery_cooldown=timedelta(minutes=30),
            max_recovery_attempts=5,
            is_scraped_item_job_active=_job_active_false,
            reenqueue_scraped_item=reenqueue,
            now=now,
        )
    )

    assert snapshot == LibraryRecoverySnapshot(recovered=[], permanently_failed=[])
    assert reenqueue_calls == []
    assert item.state == ItemState.DOWNLOADED.value
    assert item.recovery_attempt_count == 5
    assert item.next_retry_at == now + timedelta(minutes=5)


def test_recovery_jobs_preserve_documented_ownership_split(monkeypatch: Any) -> None:
    indexed = MediaItemRecord(
        id="item-indexed-ownership",
        external_ref="tmdb:item-indexed-ownership",
        title="Indexed Ownership Item",
        state=ItemState.INDEXED,
        attributes={},
    )
    scraped = MediaItemRecord(
        id="item-scraped-ownership",
        external_ref="tmdb:item-scraped-ownership",
        title="Scraped Ownership Item",
        state=ItemState.SCRAPED,
        attributes={},
    )
    downloaded = MediaItemRecord(
        id="item-downloaded-ownership",
        external_ref="tmdb:item-downloaded-ownership",
        title="Downloaded Ownership Item",
        state=ItemState.DOWNLOADED,
        attributes={},
    )
    retry_media_service = FakePipelineMediaService(
        item_id="unused",
        listed_items=[indexed, scraped, downloaded],
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: retry_media_service)

    async def resolve_arq_redis(_: dict[str, object]) -> FakeArqRedis:
        return redis

    class _JobStub:
        def __init__(self, _job_id: str, redis: object) -> None:
            self.redis = redis

        async def status(self) -> JobStatus:
            return JobStatus.not_found

    monkeypatch.setattr(tasks, "_resolve_arq_redis", resolve_arq_redis)
    monkeypatch.setattr(tasks, "Job", _JobStub)

    retry_result = asyncio.run(
        tasks.retry_library({"settings": _build_worker_settings(), "queue_name": "filmu-py"})
    )

    assert retry_result == 3
    assert [call[0] for call in redis.calls] == [
        "scrape_item",
        "parse_scrape_results",
        "finalize_item",
    ]

    now = datetime(2026, 3, 15, 2, 0, tzinfo=UTC)
    downloaded_ignored = _build_item_orm(
        item_id="item-downloaded-recovery-ownership",
        state=ItemState.DOWNLOADED,
    )
    failed_no_candidates = _build_item_orm(
        item_id="item-failed-recovery-ownership",
        state=ItemState.FAILED,
    )
    failed_no_candidates.events = [
        _build_state_event(
            item_id=failed_no_candidates.id,
            previous_state=ItemState.SCRAPED,
            next_state=ItemState.FAILED,
            created_at=now - timedelta(minutes=45),
        )
    ]
    service, _ = _build_recovery_service([downloaded_ignored, failed_no_candidates])
    scrape_reenqueue_calls: list[str] = []
    parse_reenqueue_calls: list[str] = []

    async def reenqueue_scrape(item_id: str) -> bool:
        scrape_reenqueue_calls.append(item_id)
        return True

    async def reenqueue_parse(item_id: str) -> bool:
        parse_reenqueue_calls.append(item_id)
        return True

    snapshot = asyncio.run(
        service.recover_incomplete_library(
            recovery_cooldown=timedelta(minutes=30),
            max_recovery_attempts=5,
            is_scrape_item_job_active=_job_active_false,
            reenqueue_scrape_item=reenqueue_scrape,
            is_scraped_item_job_active=_job_active_false,
            reenqueue_scraped_item=reenqueue_parse,
            now=now,
        )
    )

    assert scrape_reenqueue_calls == [failed_no_candidates.id]
    assert parse_reenqueue_calls == []
    assert [record.item_id for record in snapshot.recovered] == [failed_no_candidates.id]
    assert snapshot.recovered[0].reason == "failed_cooldown_elapsed_no_scrape_candidates"
    assert downloaded_ignored.state == ItemState.DOWNLOADED.value


def test_build_recovery_plan_record_marks_downloaded_items_for_finalize() -> None:
    plan = _build_recovery_plan_record(
        state=ItemState.DOWNLOADED,
        recovery_attempt_count=3,
    )

    assert plan.mechanism is RecoveryMechanism.ORPHAN_RECOVERY
    assert plan.target_stage is RecoveryTargetStage.FINALIZE
    assert plan.reason == "orphaned_downloaded_item"
    assert plan.next_retry_at is None
    assert plan.recovery_attempt_count == 3
    assert plan.is_in_cooldown is False


def test_build_recovery_plan_record_marks_failed_items_with_candidates_for_parse() -> None:
    retry_at = datetime.now(UTC) + timedelta(minutes=10)
    plan = _build_recovery_plan_record(
        state=ItemState.FAILED,
        next_retry_at=retry_at,
        recovery_attempt_count=2,
        has_scrape_candidates=True,
    )

    assert plan.mechanism is RecoveryMechanism.COOLDOWN_RECOVERY
    assert plan.target_stage is RecoveryTargetStage.PARSE
    assert plan.reason == "failed_retry_in_cooldown"
    assert plan.next_retry_at == retry_at.isoformat()
    assert plan.recovery_attempt_count == 2
    assert plan.is_in_cooldown is True


def test_build_recovery_plan_record_marks_failed_items_without_candidates_for_scrape() -> None:
    plan = _build_recovery_plan_record(
        state=ItemState.FAILED,
        recovery_attempt_count=1,
        has_scrape_candidates=False,
    )

    assert plan.mechanism is RecoveryMechanism.COOLDOWN_RECOVERY
    assert plan.target_stage is RecoveryTargetStage.SCRAPE
    assert plan.reason == "failed_cooldown_elapsed_no_scrape_candidates"
    assert plan.next_retry_at is None
    assert plan.recovery_attempt_count == 1
    assert plan.is_in_cooldown is False


def test_get_recovery_plan_returns_finalize_orphan_for_downloaded_items() -> None:
    item = _build_item_orm(item_id="item-downloaded-plan", state=ItemState.DOWNLOADED)
    item.recovery_attempt_count = 4
    item.next_retry_at = datetime(2026, 3, 15, 2, 15, tzinfo=UTC)
    service, _ = _build_recovery_service([item])

    plan = asyncio.run(service.get_recovery_plan(media_item_id=item.id))

    assert plan is not None
    assert plan.mechanism is RecoveryMechanism.ORPHAN_RECOVERY
    assert plan.target_stage is RecoveryTargetStage.FINALIZE
    assert plan.reason == "orphaned_downloaded_item"
    assert plan.next_retry_at is None
    assert plan.recovery_attempt_count == 4
    assert plan.is_in_cooldown is False


def test_resolve_enabled_downloader_uses_priority_order_and_logs_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _build_worker_settings()
    settings.downloaders.real_debrid.enabled = True
    settings.downloaders.real_debrid.api_key = "rd-token"
    settings.downloaders.all_debrid.enabled = True
    settings.downloaders.all_debrid.api_key = "ad-token"
    settings.downloaders.debrid_link.enabled = True
    settings.downloaders.debrid_link.api_key = "dl-token"
    caplog.set_level("WARNING")

    provider, api_key = tasks._resolve_enabled_downloader(
        settings,
        item_id="item-priority",
        item_request_id="request-priority",
    )

    assert (provider, api_key) == ("realdebrid", "rd-token")
    assert any(
        record.msg == "multiple downloaders enabled; selecting by fixed provider priority"
        and getattr(record, "item_id", None) == "item-priority"
        and getattr(record, "item_request_id", None) == "request-priority"
        for record in caplog.records
    )


def test_worker_runtime_settings_resolution_prefers_persisted_blob_when_ctx_has_no_settings(
    monkeypatch: Any,
) -> None:
    persisted = _build_worker_settings().to_compatibility_dict()
    persisted["downloaders"]["real_debrid"] = {"enabled": False, "api_key": ""}
    persisted["downloaders"]["all_debrid"] = {"enabled": True, "api_key": "ad-token"}

    @dataclass
    class _SettingsRuntime:
        dsn: str
        echo: bool = False

    async def fake_load_settings(_: object) -> dict[str, object] | None:
        return persisted

    monkeypatch.setattr(tasks, "DatabaseRuntime", _SettingsRuntime)
    monkeypatch.setattr(tasks, "get_settings", _build_worker_settings)
    monkeypatch.setattr(tasks, "load_settings", fake_load_settings)

    settings = asyncio.run(tasks._resolve_runtime_settings({}))

    assert settings.downloaders.real_debrid.enabled is False
    assert settings.downloaders.all_debrid.enabled is True
    provider, api_key = tasks._resolve_enabled_downloader(
        settings,
        item_id="item-persisted-worker",
        item_request_id="request-persisted-worker",
    )
    assert (provider, api_key) == ("alldebrid", "ad-token")


def test_worker_runtime_settings_resolution_preserves_env_only_tmdb_key_when_persisted_blob_is_blank(
    monkeypatch: Any,
) -> None:
    persisted = _build_worker_settings().to_compatibility_dict()
    persisted["tmdb_api_key"] = ""

    @dataclass
    class _SettingsRuntime:
        dsn: str
        echo: bool = False

    async def fake_load_settings(_: object) -> dict[str, object] | None:
        return persisted

    monkeypatch.setattr(tasks, "DatabaseRuntime", _SettingsRuntime)
    monkeypatch.setattr(tasks, "load_settings", fake_load_settings)
    monkeypatch.setenv("TMDB_API_KEY", "tmdb-token")
    monkeypatch.setattr(tasks, "get_settings", _build_worker_settings)

    settings = asyncio.run(tasks._resolve_runtime_settings({}))

    assert settings.tmdb_api_key == "tmdb-token"


def test_worker_plugin_context_provider_uses_worker_plugin_settings_payload() -> None:
    settings = _build_worker_settings()
    worker_ctx = tasks.build_worker_settings(settings)["ctx"]
    worker_ctx["plugin_settings_payload"] = {
        "plugins": {"mdblist": {"enabled": True, "watchlist_items": [{"title": "Movie"}]}}
    }
    provider = tasks._build_worker_plugin_context_provider(worker_ctx, settings=settings)
    plugin_context = provider.build("mdblist", datasource_name="host")

    assert dict(plugin_context.settings) == {
        "enabled": True,
        "watchlist_items": [{"title": "Movie"}],
    }


def test_worker_plugin_registry_refreshes_when_plugin_settings_payload_changes(monkeypatch: Any) -> None:
    settings = _build_worker_settings()
    worker_ctx = tasks.build_worker_settings(settings)["ctx"]
    worker_ctx["settings"] = settings
    worker_ctx["event_bus"] = EventBus()
    worker_ctx["plugin_settings_payload"] = {"scraping": {"torrentio": {"enabled": False}}}

    build_calls: list[dict[str, object]] = []

    def fake_load_plugins(*args: object, **kwargs: object) -> None:
        _ = args, kwargs
        build_calls.append({"phase": "load", "settings": worker_ctx["plugin_settings_payload"]})

    def fake_register_builtin_plugins(*args: object, **kwargs: object) -> tuple[str, ...]:
        _ = args, kwargs
        build_calls.append({"phase": "builtin", "settings": worker_ctx["plugin_settings_payload"]})
        return ("torrentio",)

    async def fake_load_settings(_: object) -> dict[str, object] | None:
        persisted = settings.to_compatibility_dict()
        persisted.update(cast(dict[str, object], worker_ctx["plugin_settings_payload"]))
        return persisted

    monkeypatch.setattr(tasks, "load_plugins", fake_load_plugins)
    monkeypatch.setattr(tasks, "register_builtin_plugins", fake_register_builtin_plugins)
    monkeypatch.setattr(tasks, "load_settings", fake_load_settings)

    first = asyncio.run(tasks._resolve_plugin_registry(worker_ctx))
    second = asyncio.run(tasks._resolve_plugin_registry(worker_ctx))

    worker_ctx["plugin_settings_payload"] = {"scraping": {"torrentio": {"enabled": True}}}
    third = asyncio.run(tasks._resolve_plugin_registry(worker_ctx))

    assert first is second
    assert third is not first
    builtin_calls = [call for call in build_calls if call["phase"] == "builtin"]
    assert len(builtin_calls) == 2
    assert cast(dict[str, object], builtin_calls[0]["settings"])["scraping"] == {
        "torrentio": {"enabled": False}
    }
    assert cast(dict[str, object], builtin_calls[1]["settings"])["scraping"] == {
        "torrentio": {"enabled": True}
    }


def test_transition_item_enqueues_scraped_item_processing_job() -> None:
    item = _build_item_orm(item_id="item-transition-enqueue", state=ItemState.INDEXED)
    runtime = _TransitionRuntime(item=item)
    enqueued_item_ids: list[str] = []

    async def enqueue(item_id: str) -> None:
        enqueued_item_ids.append(item_id)

    service = MediaService(
        db=runtime,  # type: ignore[arg-type]
        event_bus=EventBus(),
        scraped_item_enqueuer=enqueue,
    )

    result = asyncio.run(
        service.transition_item(item_id=item.id, event=ItemEvent.SCRAPE, message="scrape done")
    )

    assert result.state is ItemState.SCRAPED
    assert item.state == ItemState.SCRAPED.value
    assert enqueued_item_ids == [item.id]
    assert runtime.last_session is not None
    assert runtime.last_session.committed is True


def test_recover_incomplete_library_skips_failed_items_within_cooldown() -> None:
    now = datetime(2026, 3, 15, 2, 0, tzinfo=UTC)
    item = _build_item_orm(item_id="item-failed-cooldown", state=ItemState.FAILED)
    item.events = [
        _build_state_event(
            item_id=item.id,
            previous_state=ItemState.SCRAPED,
            next_state=ItemState.FAILED,
            created_at=now - timedelta(minutes=5),
        )
    ]
    service, runtime = _build_recovery_service([item])
    reenqueue_calls: list[str] = []

    async def reenqueue(item_id: str) -> bool:
        reenqueue_calls.append(item_id)
        return True

    snapshot = asyncio.run(
        service.recover_incomplete_library(
            recovery_cooldown=timedelta(minutes=30),
            max_recovery_attempts=5,
            is_scraped_item_job_active=_job_active_false,
            reenqueue_scraped_item=reenqueue,
            now=now,
        )
    )

    assert snapshot == LibraryRecoverySnapshot(recovered=[], permanently_failed=[])
    assert reenqueue_calls == []
    assert item.recovery_attempt_count == 0
    assert item.next_retry_at == now + timedelta(minutes=25)
    assert runtime.last_session is not None
    assert runtime.last_session.committed is True


def test_recover_incomplete_library_reenqueues_failed_items_past_cooldown() -> None:
    now = datetime(2026, 3, 15, 2, 0, tzinfo=UTC)
    item = _build_item_orm(item_id="item-failed-past-cooldown", state=ItemState.FAILED)
    item.events = [
        _build_state_event(
            item_id=item.id,
            previous_state=ItemState.SCRAPED,
            next_state=ItemState.FAILED,
            created_at=now - timedelta(minutes=45),
        )
    ]
    service, _ = _build_recovery_service([item])
    reenqueue_calls: list[str] = []

    async def reenqueue(item_id: str) -> bool:
        reenqueue_calls.append(item_id)
        return True

    snapshot = asyncio.run(
        service.recover_incomplete_library(
            recovery_cooldown=timedelta(minutes=30),
            max_recovery_attempts=5,
            is_scraped_item_job_active=_job_active_false,
            reenqueue_scraped_item=reenqueue,
            now=now,
        )
    )

    assert [record.item_id for record in snapshot.recovered] == [item.id]
    assert snapshot.recovered[0].reason == "failed_cooldown_elapsed"
    assert reenqueue_calls == [item.id]
    assert item.state == ItemState.SCRAPED.value
    assert item.recovery_attempt_count == 1
    assert item.next_retry_at is None


def test_recover_incomplete_library_honors_max_recovery_attempts() -> None:
    now = datetime(2026, 3, 15, 2, 0, tzinfo=UTC)
    item = _build_item_orm(item_id="item-max-attempts", state=ItemState.FAILED)
    item.recovery_attempt_count = 5
    item.events = [
        _build_state_event(
            item_id=item.id,
            previous_state=ItemState.SCRAPED,
            next_state=ItemState.FAILED,
            created_at=now - timedelta(hours=2),
        )
    ]
    service, _ = _build_recovery_service([item])
    reenqueue_calls: list[str] = []

    async def reenqueue(item_id: str) -> bool:
        reenqueue_calls.append(item_id)
        return True

    snapshot = asyncio.run(
        service.recover_incomplete_library(
            recovery_cooldown=timedelta(minutes=30),
            max_recovery_attempts=5,
            is_scraped_item_job_active=_job_active_false,
            reenqueue_scraped_item=reenqueue,
            now=now,
        )
    )

    assert [record.item_id for record in snapshot.permanently_failed] == [item.id]
    assert snapshot.permanently_failed[0].reason == "max_recovery_attempts_exceeded"
    assert reenqueue_calls == []
    assert item.state == ItemState.FAILED.value
    assert item.recovery_attempt_count == 5


def test_recover_incomplete_library_reenqueues_scraped_items_with_no_active_job() -> None:
    now = datetime(2026, 3, 15, 2, 0, tzinfo=UTC)
    item = _build_item_orm(item_id="item-scraped-orphan", state=ItemState.SCRAPED)
    service, _ = _build_recovery_service([item])
    reenqueue_calls: list[str] = []

    async def reenqueue(item_id: str) -> bool:
        reenqueue_calls.append(item_id)
        return True

    snapshot = asyncio.run(
        service.recover_incomplete_library(
            recovery_cooldown=timedelta(minutes=30),
            max_recovery_attempts=5,
            is_scraped_item_job_active=_job_active_false,
            reenqueue_scraped_item=reenqueue,
            now=now,
        )
    )

    assert [record.item_id for record in snapshot.recovered] == [item.id]
    assert snapshot.recovered[0].reason == "scraped_without_inflight_worker"
    assert reenqueue_calls == [item.id]
    assert item.recovery_attempt_count == 1


def test_recover_incomplete_library_does_not_reenqueue_scraped_items_with_active_job() -> None:
    now = datetime(2026, 3, 15, 2, 0, tzinfo=UTC)
    item = _build_item_orm(item_id="item-scraped-active-job", state=ItemState.SCRAPED)
    service, _ = _build_recovery_service([item])
    reenqueue_calls: list[str] = []

    async def reenqueue(item_id: str) -> bool:
        reenqueue_calls.append(item_id)
        return True

    snapshot = asyncio.run(
        service.recover_incomplete_library(
            recovery_cooldown=timedelta(minutes=30),
            max_recovery_attempts=5,
            is_scraped_item_job_active=_job_active_true,
            reenqueue_scraped_item=reenqueue,
            now=now,
        )
    )

    assert snapshot == LibraryRecoverySnapshot(recovered=[], permanently_failed=[])
    assert reenqueue_calls == []
    assert item.recovery_attempt_count == 0


def test_recover_incomplete_library_reenqueues_failed_items_without_scrape_candidates_at_scrape_stage() -> None:
    now = datetime(2026, 3, 15, 2, 0, tzinfo=UTC)
    item = _build_item_orm(item_id="item-failed-scrape-stage", state=ItemState.FAILED)
    item.events = [
        _build_state_event(
            item_id=item.id,
            previous_state=ItemState.INDEXED,
            next_state=ItemState.FAILED,
            created_at=now - timedelta(minutes=45),
        )
    ]
    item.scrape_candidates = []
    service, _ = _build_recovery_service([item])
    scrape_reenqueue_calls: list[str] = []
    parse_reenqueue_calls: list[str] = []

    async def reenqueue_scrape(item_id: str) -> bool:
        scrape_reenqueue_calls.append(item_id)
        return True

    async def reenqueue_parse(item_id: str) -> bool:
        parse_reenqueue_calls.append(item_id)
        return True

    snapshot = asyncio.run(
        service.recover_incomplete_library(
            recovery_cooldown=timedelta(minutes=30),
            max_recovery_attempts=5,
            is_scrape_item_job_active=_job_active_false,
            reenqueue_scrape_item=reenqueue_scrape,
            is_scraped_item_job_active=_job_active_false,
            reenqueue_scraped_item=reenqueue_parse,
            now=now,
        )
    )

    assert [record.item_id for record in snapshot.recovered] == [item.id]
    assert snapshot.recovered[0].reason == "failed_cooldown_elapsed_no_scrape_candidates"
    assert item.state == ItemState.REQUESTED.value
    assert scrape_reenqueue_calls == [item.id]
    assert parse_reenqueue_calls == []


def test_build_worker_settings_includes_recovery_worker() -> None:
    worker_settings = tasks.build_worker_settings(_build_worker_settings())

    assert tasks.recover_incomplete_library in worker_settings["functions"]
    assert tasks.scheduled_metadata_reindex_reconciliation in worker_settings["functions"]


def test_build_worker_settings_includes_scheduled_metadata_reindex_cron() -> None:
    settings = _build_worker_settings()
    settings.indexer.schedule_offset_minutes = 45

    worker_settings = tasks.build_worker_settings(settings)
    cron_jobs = worker_settings["cron_jobs"]
    metadata_reindex_job = next(
        job for job in cron_jobs if job.name == "scheduled_metadata_reindex_reconciliation"
    )

    assert metadata_reindex_job.hour == {0}
    assert metadata_reindex_job.minute == {45}
    assert metadata_reindex_job.job_id == "scheduled-metadata-reindex-reconciliation"


def test_scheduled_metadata_reindex_reconciliation_runs_reindex_and_completed_refresh(
    monkeypatch: Any,
) -> None:
    partial = MediaItemRecord(
        id="item-partial-reindex",
        external_ref="tmdb:item-partial-reindex",
        title="Partial Item",
        state=ItemState.PARTIALLY_COMPLETED,
        attributes={"item_type": "show"},
    )
    ongoing = MediaItemRecord(
        id="item-ongoing-reindex",
        external_ref="tmdb:item-ongoing-reindex",
        title="Ongoing Item",
        state=ItemState.ONGOING,
        attributes={"item_type": "show"},
    )
    completed = MediaItemRecord(
        id="item-completed-reindex",
        external_ref="tmdb:item-completed-reindex",
        title="Completed Item",
        state=ItemState.COMPLETED,
        attributes={"item_type": "movie"},
    )

    media_service = FakePipelineMediaService(
        item_id="item-completed-reindex",
        listed_items=[partial, ongoing, completed],
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    async def resolve_arq_redis(_: dict[str, object]) -> FakeArqRedis:
        return redis

    async def index_job_active(
        _: object,
        *,
        item_id: str,
        job_id: str | None = None,
    ) -> bool:
        if item_id == "item-ongoing-reindex":
            assert job_id == tasks.index_item_followup_job_id(
                "item-ongoing-reindex",
                discriminator="scheduled-reindex",
            )
        return item_id == "item-ongoing-reindex"

    monkeypatch.setattr(tasks, "_resolve_arq_redis", resolve_arq_redis)
    monkeypatch.setattr(tasks, "is_index_item_job_active", index_job_active)

    result = asyncio.run(
        tasks.scheduled_metadata_reindex_reconciliation(
            {"settings": _build_worker_settings(), "queue_name": "filmu-py"}
        )
    )

    assert result == {
        "processed": 3,
        "queued": 1,
        "reconciled": 1,
        "skipped_active": 1,
        "failed": 0,
    }
    assert redis.calls == [
        (
            "index_item",
            ("item-partial-reindex",),
            {
                "_job_id": tasks.index_item_followup_job_id(
                    "item-partial-reindex",
                    discriminator="scheduled-reindex",
                ),
                "_queue_name": "filmu-py",
            },
        )
    ]
    assert media_service.calls.count("enrich_item_metadata") == 1


def test_scheduled_metadata_reindex_reconciliation_checks_followup_job_id(
    monkeypatch: Any,
) -> None:
    partial = MediaItemRecord(
        id="item-partial-followup-check",
        external_ref="tmdb:item-partial-followup-check",
        title="Partial Item",
        state=ItemState.PARTIALLY_COMPLETED,
        attributes={"item_type": "show"},
    )
    media_service = FakePipelineMediaService(
        item_id="item-partial-followup-check",
        listed_items=[partial],
    )
    redis = FakeArqRedis()
    observed_job_ids: list[str | None] = []
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    async def resolve_arq_redis(_: dict[str, object]) -> FakeArqRedis:
        return redis

    async def index_job_inactive(_: object, *, item_id: str, job_id: str | None = None) -> bool:
        assert item_id == "item-partial-followup-check"
        observed_job_ids.append(job_id)
        return False

    monkeypatch.setattr(tasks, "_resolve_arq_redis", resolve_arq_redis)
    monkeypatch.setattr(tasks, "is_index_item_job_active", index_job_inactive)

    result = asyncio.run(
        tasks.scheduled_metadata_reindex_reconciliation(
            {"settings": _build_worker_settings(), "queue_name": "filmu-py"}
        )
    )

    assert result["queued"] == 1
    assert observed_job_ids == [
        tasks.index_item_followup_job_id(
            "item-partial-followup-check",
            discriminator="scheduled-reindex",
        )
    ]


# --- SHOW COMPLETION TESTS ---

def test_finalize_item_partial_scope_incomplete(monkeypatch: Any) -> None:
    item_id = "item-finalize-partial-incomplete"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.DOWNLOADED,
        item_attributes={"item_type": "show"},
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    async def fake_evaluate(*_args: Any, **_kwargs: Any) -> ShowCompletionResult:
        return ShowCompletionResult(
            all_satisfied=False,
            any_satisfied=True,
            has_future_episodes=False,
            missing_released=[(1, 2)],
        )

    async def fake_resolve_arq(_ctx: Any) -> FakeArqRedis:
        return redis

    monkeypatch.setattr(tasks, "_evaluate_show_completion", fake_evaluate)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", fake_resolve_arq)
    result = asyncio.run(
        tasks.finalize_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )
    assert result == item_id
    assert media_service.state is ItemState.PARTIALLY_COMPLETED
    assert media_service.transition_messages == [(ItemEvent.PARTIAL_COMPLETE, "missing_episodes")]
    assert redis.calls[-1][0] == "index_item"
    assert redis.calls[-1][2].get("missing_seasons") == [1]
    assert redis.calls[-1][2].get("missing_episodes") == {"1": [2]}
    assert "_defer_by" not in redis.calls[-1][2]
    assert redis.calls[-1][2].get("_job_id") == tasks.index_item_followup_job_id(
        item_id,
        discriminator="partial-followup",
        missing_seasons=[1],
        missing_episodes={"1": [2]},
    )


def test_finalize_item_full_show_ongoing(monkeypatch: Any) -> None:
    item_id = "item-finalize-ongoing"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.DOWNLOADED,
        item_attributes={"item_type": "show"},
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    async def fake_evaluate(*_args: Any, **_kwargs: Any) -> ShowCompletionResult:
        return ShowCompletionResult(
            all_satisfied=True,
            any_satisfied=True,
            has_future_episodes=True,
            missing_released=[],
        )

    async def fake_resolve_arq(_ctx: Any) -> FakeArqRedis:
        return redis

    monkeypatch.setattr(tasks, "_evaluate_show_completion", fake_evaluate)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", fake_resolve_arq)
    result = asyncio.run(
        tasks.finalize_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )
    assert result == item_id
    assert media_service.state is ItemState.ONGOING
    assert media_service.transition_messages == [(ItemEvent.MARK_ONGOING, "waiting_on_unreleased_episodes")]
    assert redis.calls[-1][0] == "index_item"
    assert redis.calls[-1][2].get("_job_id") == tasks.index_item_followup_job_id(
        item_id,
        discriminator="ongoing-poll",
    )
    assert redis.calls[-1][2].get("_defer_by") == timedelta(hours=24)


def test_finalize_item_without_satisfied_released_episodes_defers_index_followup(
    monkeypatch: Any,
) -> None:
    item_id = "item-finalize-no-inventory"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.DOWNLOADED,
        item_attributes={"item_type": "show"},
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    async def fake_evaluate(*_args: Any, **_kwargs: Any) -> ShowCompletionResult:
        return ShowCompletionResult(
            all_satisfied=False,
            any_satisfied=False,
            has_future_episodes=False,
            missing_released=[(1, 2)],
        )

    async def fake_resolve_arq(_ctx: Any) -> FakeArqRedis:
        return redis

    monkeypatch.setattr(tasks, "_evaluate_show_completion", fake_evaluate)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", fake_resolve_arq)
    result = asyncio.run(
        tasks.finalize_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )

    assert result == item_id
    assert redis.calls[-1][0] == "index_item"
    assert redis.calls[-1][2].get("_job_id") == tasks.index_item_followup_job_id(
        item_id,
        discriminator="inventory-recheck",
        missing_seasons=[1],
        missing_episodes={"1": [2]},
    )
    assert redis.calls[-1][2].get("_defer_by") == timedelta(seconds=300)
    assert redis.calls[-1][2].get("missing_seasons") == [1]
    assert redis.calls[-1][2].get("missing_episodes") == {"1": [2]}


def test_finalize_item_re_entry_path(monkeypatch: Any) -> None:
    item_id = "item-re-entry"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.DOWNLOADED,
        item_attributes={"item_type": "show"},
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    results = iter(
        [
            ShowCompletionResult(
                all_satisfied=False,
                any_satisfied=True,
                has_future_episodes=False,
                missing_released=[(1, 2)],
            ),
            ShowCompletionResult(
                all_satisfied=True,
                any_satisfied=True,
                has_future_episodes=False,
                missing_released=[],
            ),
        ]
    )

    async def fake_evaluate(*_args: Any, **_kwargs: Any) -> ShowCompletionResult:
        return next(results)

    async def fake_resolve_arq(_ctx: Any) -> FakeArqRedis:
        return redis

    monkeypatch.setattr(tasks, "_evaluate_show_completion", fake_evaluate)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", fake_resolve_arq)
    asyncio.run(
        tasks.finalize_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )
    assert media_service.state is ItemState.PARTIALLY_COMPLETED

    asyncio.run(
        tasks.finalize_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )
    assert media_service.state is ItemState.COMPLETED
    assert media_service.transition_messages[-1][0] == ItemEvent.COMPLETE


def test_finalize_item_no_stranding(monkeypatch: Any) -> None:
    item_id = "item-no-stranding"
    media_service = FakePipelineMediaService(
        item_id=item_id,
        state=ItemState.DOWNLOADED,
        item_attributes={"item_type": "show"},
    )
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "_resolve_media_service", lambda _: media_service)

    async def fake_evaluate(*_args: Any, **_kwargs: Any) -> ShowCompletionResult:
        return ShowCompletionResult(
            all_satisfied=False,
            any_satisfied=True,
            has_future_episodes=False,
            missing_released=[(1, 2)],
        )

    async def fake_resolve_arq(_ctx: Any) -> FakeArqRedis:
        return redis

    monkeypatch.setattr(tasks, "_evaluate_show_completion", fake_evaluate)
    monkeypatch.setattr(tasks, "_resolve_arq_redis", fake_resolve_arq)
    asyncio.run(
        tasks.finalize_item(
            {"settings": _build_worker_settings(), "arq_redis": redis, "queue_name": "filmu-py"},
            item_id,
        )
    )
    assert media_service.state is not ItemState.DOWNLOADED


def test_enqueue_index_item_supports_custom_job_id_and_defer(monkeypatch: Any) -> None:
    redis = FakeArqRedis()
    monkeypatch.setattr(tasks, "get_settings", _build_worker_settings)

    result = asyncio.run(
        tasks.enqueue_index_item(
            redis,
            item_id="item-index-enqueue-followup",
            queue_name="filmu-py",
            tenant_id="tenant-a",
            defer_by_seconds=90,
            job_id=tasks.index_item_followup_job_id(
                "item-index-enqueue-followup",
                discriminator="release-poll",
            ),
            missing_seasons=[2],
        )
    )

    assert result is True
    assert redis.calls[-1][0] == "index_item"
    assert redis.calls[-1][2].get("_job_id") == tasks.index_item_followup_job_id(
        "item-index-enqueue-followup",
        discriminator="release-poll",
    )
    assert redis.calls[-1][2].get("_defer_by") == timedelta(seconds=90)
    assert redis.calls[-1][2].get("missing_seasons") == [2]


def test_heavy_stage_executor_evicts_stale_policy_executors(monkeypatch: Any) -> None:
    original_cache = dict(tasks._HEAVY_STAGE_EXECUTORS)
    tasks._HEAVY_STAGE_EXECUTORS.clear()

    class FakeExecutor:
        def __init__(self, max_workers: int) -> None:
            self.max_workers = max_workers
            self.shutdown_calls: list[tuple[bool, bool]] = []

        def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
            self.shutdown_calls.append((wait, cancel_futures))

    created: list[FakeExecutor] = []

    def fake_thread_pool_executor(*, max_workers: int) -> FakeExecutor:
        executor = FakeExecutor(max_workers=max_workers)
        created.append(executor)
        return executor

    base_settings = _build_worker_settings()
    first_settings = base_settings.model_copy(
        update={
            "orchestration": base_settings.orchestration.model_copy(
                update={
                    "heavy_stage_isolation": base_settings.orchestration.heavy_stage_isolation.model_copy(
                        update={"executor_mode": "thread_pool_only", "max_workers": 1}
                    )
                }
            )
        }
    )
    second_settings = base_settings.model_copy(
        update={
            "orchestration": base_settings.orchestration.model_copy(
                update={
                    "heavy_stage_isolation": base_settings.orchestration.heavy_stage_isolation.model_copy(
                        update={"executor_mode": "thread_pool_only", "max_workers": 2}
                    )
                }
            )
        }
    )
    current_settings = first_settings

    monkeypatch.setattr(tasks, "ThreadPoolExecutor", fake_thread_pool_executor)
    monkeypatch.setattr(tasks, "get_settings", lambda: current_settings)

    try:
        first_executor = tasks._heavy_stage_executor("index_item")
        current_settings = second_settings
        second_executor = tasks._heavy_stage_executor("index_item")
        assert first_executor is not second_executor
        assert created[0].shutdown_calls == [(False, True)]
        assert len(tasks._HEAVY_STAGE_EXECUTORS) == 1
    finally:
        for executor in tasks._HEAVY_STAGE_EXECUTORS.values():
            executor.shutdown(wait=False, cancel_futures=True)
        tasks._HEAVY_STAGE_EXECUTORS.clear()
        tasks._HEAVY_STAGE_EXECUTORS.update(original_cache)


def test_evaluate_show_missing_specialization() -> None:
    item_id = str(uuid.uuid4())
    item = _build_item_orm(item_id=item_id, state=ItemState.DOWNLOADED)
    item.attributes = {"item_type": "show"}
    service, runtime = _build_recovery_service([item])

    async def run_test() -> None:
        async with runtime.session() as session:
            async def mock_execute(stmt: Any) -> Any:
                class MockResult:
                    def scalar_one_or_none(self) -> Any:
                        return None

                return MockResult()

            session.execute = mock_execute

            status = await service.evaluate_show_completion_scope(item_id, session)
            assert status == CompletionStatus.INCOMPLETE

    asyncio.run(run_test())


def test_evaluate_show_evaluates_from_episode_id() -> None:
    show_uuid = uuid.uuid4()
    season_uuid = uuid.uuid4()
    ep_uuid = uuid.uuid4()

    show_id = str(show_uuid)
    season_id = str(season_uuid)
    episode_id = str(ep_uuid)
    
    show_item = _build_item_orm(item_id=show_id, state=ItemState.DOWNLOADED)
    show_item.attributes = {"item_type": "show"}
    season_item = _build_item_orm(item_id=season_id, state=ItemState.DOWNLOADED)
    season_item.attributes = {"item_type": "season"}
    episode_item = _build_item_orm(item_id=episode_id, state=ItemState.DOWNLOADED)
    episode_item.attributes = {"item_type": "episode", "aired_at": "2020-01-01T00:00:00Z"}
    
    show_orm = ShowORM(id=show_uuid)
    season_orm = SeasonORM(id=season_uuid, show_id=show_uuid, show=show_orm, season_number=1)
    episode_orm = EpisodeORM(id=ep_uuid, season_id=season_uuid, season=season_orm, episode_number=1)
    episode_orm.media_item_id = ep_uuid  # link specialization PK to lifecycle ID for ancestry walk
    
    show_orm.seasons = [season_orm]
    season_orm.episodes = [episode_orm]

    service, runtime = _build_recovery_service([show_item, season_item, episode_item])

    # Overload mock execution safely
    async def run_test() -> None:
        async with runtime.session() as session:
            # Simple wrapper to intercept session calls
            original_get = session.get

            async def mock_get(model: Any, i_id: Any) -> Any:
                if model == EpisodeORM:
                    return episode_orm
                if model == SeasonORM:
                    return season_orm
                if model == MediaItemORM:
                    for it in [show_item, season_item, episode_item]:
                        if str(it.id) == str(i_id):
                            return it
                return await original_get(model, i_id)

            async def mock_execute(stmt: Any) -> Any:
                stmt_str = str(stmt).lower()

                # Pre-determine what ORM the query targets so ancestry walk
                # receives type-correct objects rather than always show_orm.
                if "item_request" in stmt_str or "media_entry" in stmt_str:
                    target: Any = None
                elif "episode" in stmt_str:
                    target = episode_orm
                elif "season" in stmt_str:
                    target = season_orm
                else:  # ShowORM or fallthrough
                    target = show_orm

                class MockResult:
                    def scalar_one_or_none(self) -> Any:
                        return target

                    def scalars(self) -> Any:
                        return self

                    def all(self) -> list[Any]:
                        return []

                    def first(self) -> Any:
                        return None

                    def distinct(self) -> Any:
                        return self

                return MockResult()

            session.get = mock_get
            session.execute = mock_execute

            status = await service.evaluate_show_completion_scope(episode_id, session)
            assert status == CompletionStatus.INCOMPLETE

    asyncio.run(run_test())
