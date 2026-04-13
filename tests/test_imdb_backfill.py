from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, cast

import httpx
import pytest

from filmu_py.config import Settings
from filmu_py.core.event_bus import EventBus
from filmu_py.db.models import ItemStateEventORM, MediaItemORM, MovieORM, ShowORM
from filmu_py.services.media import (
    EnrichmentResult,
    MediaService,
    RequestMetadataResolution,
    RequestTimeMetadataRecord,
)
from filmu_py.services.tmdb import TmdbMetadataClient
from filmu_py.state.item import ItemState


class _FakeLimiter:
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
        return object()


class _DummyDb:
    pass


class _RequestItemExecuteResult:
    def scalar_one_or_none(self) -> None:
        return None


class _RequestItemSession:
    def __init__(self) -> None:
        self.added: list[object] = []

    async def execute(self, stmt: object) -> _RequestItemExecuteResult:
        _ = stmt
        return _RequestItemExecuteResult()

    def add(self, obj: object) -> None:
        self.added.append(obj)

    async def flush(self) -> None:
        for obj in self.added:
            if isinstance(obj, MediaItemORM) and not obj.id:
                obj.id = "item-created"

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None


class _RequestItemDb:
    @asynccontextmanager
    async def session(self) -> AsyncIterator[_RequestItemSession]:
        yield _RequestItemSession()


def _build_settings(*, tmdb_api_key: str = "tmdb-token") -> Settings:
    return Settings(
        FILMU_PY_API_KEY="a" * 32,
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL="redis://localhost:6379/0",
        TMDB_API_KEY=tmdb_api_key,
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
    )


def _build_media_service(
    *, tmdb_client: TmdbMetadataClient | None = None, db: object | None = None
) -> MediaService:
    return MediaService(
        db=(db or _DummyDb()),  # type: ignore[arg-type]
        event_bus=EventBus(),
        settings=_build_settings(),
        rate_limiter=cast(Any, _FakeLimiter()),
        tmdb_client=tmdb_client,
    )


def _build_item(item_id: str, *, state: str = ItemState.FAILED.value, tmdb_id: str | None = None) -> MediaItemORM:
    attributes: dict[str, object] = {"item_type": "movie", "year": 2024}
    if tmdb_id is not None:
        attributes["tmdb_id"] = tmdb_id
    return MediaItemORM(
        id=item_id,
        external_ref=f"tmdb:{tmdb_id or item_id}",
        title=f"Title {item_id}",
        state=state,
        attributes=attributes,
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )


class _FakeSession:
    def __init__(self, items: list[MediaItemORM]) -> None:
        self.items = items
        self.added: list[object] = []
        self.committed = False

    async def execute(self, stmt: object) -> object:
        _ = stmt

        class _ScalarResult:
            def __init__(self, items: list[MediaItemORM]) -> None:
                self._items = items

            def all(self) -> list[MediaItemORM]:
                return list(self._items)

        class _Result:
            def __init__(self, items: list[MediaItemORM]) -> None:
                self._items = items

            def scalars(self) -> _ScalarResult:
                return _ScalarResult(self._items)

        return _Result(self.items)

    def add(self, obj: object) -> None:
        self.added.append(obj)

    async def commit(self) -> None:
        self.committed = True

    @asynccontextmanager
    async def begin_nested(self) -> AsyncIterator[None]:
        yield


class _FakeTmdbClient:
    def __init__(self, responses: dict[str, dict[str, str | None]], failures: set[str] | None = None) -> None:
        self.responses = responses
        self.failures = failures or set()
        self.calls: list[tuple[str, str]] = []

    async def get_external_ids(self, tmdb_id: str, media_type: str) -> dict[str, str | None]:
        self.calls.append((tmdb_id, media_type))
        if tmdb_id in self.failures:
            raise RuntimeError("boom")
        return self.responses.get(tmdb_id, {"imdb_id": None, "tvdb_id": None})


@pytest.mark.asyncio
async def test_get_external_ids_movie() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/3/movie/550/external_ids"
        return httpx.Response(200, json={"imdb_id": "tt0137523", "tvdb_id": 123})

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=cast(Any, _FakeLimiter()),
        transport=httpx.MockTransport(handler),
    )

    assert await client.get_external_ids("550", "movie") == {
        "imdb_id": "tt0137523",
        "tvdb_id": "123",
    }


@pytest.mark.asyncio
async def test_get_external_ids_tv() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/3/tv/1399/external_ids"
        return httpx.Response(200, json={"imdb_id": "tt0944947", "tvdb_id": 121361})

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=cast(Any, _FakeLimiter()),
        transport=httpx.MockTransport(handler),
    )

    assert await client.get_external_ids("1399", "tv") == {
        "imdb_id": "tt0944947",
        "tvdb_id": "121361",
    }


@pytest.mark.asyncio
async def test_get_external_ids_404() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"status_message": "missing"})

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=cast(Any, _FakeLimiter()),
        transport=httpx.MockTransport(handler),
    )

    assert await client.get_external_ids("999", "movie") == {"imdb_id": None, "tvdb_id": None}


@pytest.mark.asyncio
async def test_request_item_secondary_imdb_lookup() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/3/movie/550":
            return httpx.Response(
                200,
                json={
                    "id": 550,
                    "title": "Fight Club",
                    "release_date": "1999-10-15",
                    "overview": "Overview",
                    "poster_path": "/poster.jpg",
                    "genres": [],
                    "status": "Released",
                },
            )
        if request.url.path == "/3/movie/550/external_ids":
            return httpx.Response(200, json={"imdb_id": "tt0137523", "tvdb_id": None})
        raise AssertionError(request.url.path)

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=cast(Any, _FakeLimiter()),
        transport=httpx.MockTransport(handler),
    )
    service = _build_media_service(tmdb_client=client)

    enriched = await service._fetch_request_metadata(media_type="movie", identifier="550")

    assert enriched.metadata is not None
    assert enriched.metadata.attributes["imdb_id"] == "tt0137523"


@pytest.mark.asyncio
async def test_request_item_warns_on_missing_imdb_id(capsys: pytest.CaptureFixture[str]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/3/movie/550":
            return httpx.Response(
                200,
                json={
                    "id": 550,
                    "title": "Fight Club",
                    "release_date": "1999-10-15",
                    "overview": "Overview",
                    "poster_path": "/poster.jpg",
                    "genres": [],
                    "status": "Released",
                },
            )
        if request.url.path == "/3/movie/550/external_ids":
            return httpx.Response(200, json={"imdb_id": None, "tvdb_id": None})
        raise AssertionError(request.url.path)

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=cast(Any, _FakeLimiter()),
        transport=httpx.MockTransport(handler),
    )
    service = _build_media_service(tmdb_client=client, db=_RequestItemDb())

    async def _stub_upsert_media_specialization(*args: object, **kwargs: object) -> None:
        _ = (args, kwargs)

    service._upsert_media_specialization = cast(Any, _stub_upsert_media_specialization)

    async def _stub_upsert_item_request(*args: object, **kwargs: object) -> None:
        _ = (args, kwargs)

    service._upsert_item_request = cast(Any, _stub_upsert_item_request)

    await service.request_item(
        external_ref="tmdb:550",
        title="Fight Club",
        attributes={"item_type": "movie", "tmdb_id": "550"},
    )

    assert "item.intake.imdb_id_missing" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_backfill_enriches_item_with_tmdb_id() -> None:
    item = _build_item("item-1", tmdb_id="550")
    session = _FakeSession([item])
    tmdb_client = _FakeTmdbClient({"550": {"imdb_id": "tt0137523", "tvdb_id": None}})
    service = _build_media_service(tmdb_client=cast(Any, tmdb_client))
    specialization_updates: list[tuple[str, dict[str, object]]] = []

    async def _stub_upsert(
        _session: object, *, item: MediaItemORM, media_type: str, attributes: dict[str, object]
    ) -> MovieORM | ShowORM | None:
        specialization_updates.append((media_type, dict(attributes)))
        return None

    service._upsert_media_specialization = _stub_upsert  # type: ignore[method-assign]

    summary = await service.backfill_missing_imdb_ids(session)  # type: ignore[arg-type]

    assert summary == {"attempted": 1, "enriched": 1, "skipped_no_tmdb_id": 0, "failed": 0}
    assert item.attributes["imdb_id"] == "tt0137523"
    assert item.state == ItemState.REQUESTED.value
    assert isinstance(session.added[0], ItemStateEventORM)
    assert cast(ItemStateEventORM, session.added[0]).payload["reason"] == "backfill_imdb_id"


@pytest.mark.asyncio
async def test_backfill_skips_item_with_no_tmdb_id() -> None:
    item = _build_item("item-1", tmdb_id=None)
    session = _FakeSession([item])
    service = _build_media_service(tmdb_client=cast(Any, _FakeTmdbClient({})))

    summary = await service.backfill_missing_imdb_ids(session)  # type: ignore[arg-type]

    assert summary == {"attempted": 1, "enriched": 0, "skipped_no_tmdb_id": 1, "failed": 0}
    assert item.state == ItemState.FAILED.value


@pytest.mark.asyncio
async def test_backfill_continues_on_per_item_failure() -> None:
    failing = _build_item("item-1", tmdb_id="550")
    working = _build_item("item-2", tmdb_id="551")
    session = _FakeSession([failing, working])
    tmdb_client = _FakeTmdbClient(
        {"551": {"imdb_id": "tt0000551", "tvdb_id": None}},
        failures={"550"},
    )
    service = _build_media_service(tmdb_client=cast(Any, tmdb_client))

    async def _stub_upsert(
        _session: object, *, item: MediaItemORM, media_type: str, attributes: dict[str, object]
    ) -> MovieORM | ShowORM | None:
        _ = (item, media_type, attributes)
        return None

    service._upsert_media_specialization = _stub_upsert  # type: ignore[method-assign]

    summary = await service.backfill_missing_imdb_ids(session)  # type: ignore[arg-type]

    assert summary == {"attempted": 2, "enriched": 1, "skipped_no_tmdb_id": 0, "failed": 1}
    assert cast(dict[str, object], working.attributes)["imdb_id"] == "tt0000551"


@pytest.mark.asyncio
async def test_backfill_patches_specialization_row() -> None:
    item = _build_item("item-1", tmdb_id="550")
    session = _FakeSession([item])
    tmdb_client = _FakeTmdbClient({"550": {"imdb_id": "tt0137523", "tvdb_id": None}})
    service = _build_media_service(tmdb_client=cast(Any, tmdb_client))
    specialization_updates: list[dict[str, object]] = []

    async def _stub_upsert(
        _session: object, *, item: MediaItemORM, media_type: str, attributes: dict[str, object]
    ) -> MovieORM | ShowORM | None:
        _ = (item, media_type)
        specialization_updates.append(dict(attributes))
        return None

    service._upsert_media_specialization = _stub_upsert  # type: ignore[method-assign]

    await service.backfill_missing_imdb_ids(session)  # type: ignore[arg-type]

    assert specialization_updates[-1]["imdb_id"] == "tt0137523"


@pytest.mark.asyncio
async def test_backfill_repairs_tvdb_placeholder_item() -> None:
    item = MediaItemORM(
        id="item-tvdb-1",
        external_ref="tvdb:456",
        title="tvdb:456",
        state=ItemState.FAILED.value,
        attributes={"item_type": "show", "tvdb_id": "456"},
        created_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
    )
    session = _FakeSession([item])
    service = _build_media_service(tmdb_client=cast(Any, _FakeTmdbClient({})))
    specialization_updates: list[dict[str, object]] = []

    async def _stub_upsert(
        _session: object, *, item: MediaItemORM, media_type: str, attributes: dict[str, object]
    ) -> MovieORM | ShowORM | None:
        _ = (item, media_type)
        specialization_updates.append(dict(attributes))
        return None

    async def _stub_fetch(*, media_type: str, identifier: str) -> RequestMetadataResolution:
        assert media_type == "tv"
        assert identifier == "tvdb:456"
        return RequestMetadataResolution(
            metadata=RequestTimeMetadataRecord(
                title="Example Show",
                attributes={
                    "item_type": "show",
                    "tvdb_id": "456",
                    "tmdb_id": "999",
                    "imdb_id": "tt9999999",
                    "poster_path": "/poster.jpg",
                },
            ),
            enrichment=EnrichmentResult(
                source="tmdb_via_tvdb",
                has_poster=True,
                has_imdb_id=True,
                has_tmdb_id=True,
                warnings=[],
            ),
        )

    service._upsert_media_specialization = _stub_upsert  # type: ignore[method-assign]
    service._fetch_request_metadata = _stub_fetch  # type: ignore[method-assign]

    summary = await service.backfill_missing_imdb_ids(session)  # type: ignore[arg-type]

    assert summary == {"attempted": 1, "enriched": 1, "skipped_no_tmdb_id": 0, "failed": 0}
    assert item.title == "Example Show"
    assert cast(dict[str, object], item.attributes)["tmdb_id"] == "999"
    assert cast(dict[str, object], item.attributes)["poster_path"] == "/poster.jpg"
    assert item.state == ItemState.REQUESTED.value
    assert specialization_updates[-1]["imdb_id"] == "tt9999999"
