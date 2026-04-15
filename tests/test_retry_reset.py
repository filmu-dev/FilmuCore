from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr
from starlette.requests import Request

from filmu_py.api.router import create_api_router
from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.db.models import MediaItemORM, StreamBlacklistRelationORM
from filmu_py.graphql import GraphQLPluginRegistry, build_schema
from filmu_py.graphql.deps import GraphQLContext
from filmu_py.resources import AppResources
from filmu_py.services.media import (
    ArqNotEnabledError,
    ItemActionResult,
    MediaService,
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
    @asynccontextmanager
    async def session(self) -> AsyncGenerator[None, None]:
        yield None


class FakeArqPool:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def enqueue_job(self, name: str, *args: Any, **kwargs: Any) -> object:
        self.calls.append((name, args, kwargs))
        return object()


class FakeTmdbClient:
    def __init__(self, *, imdb_id: str | None = "tt0137523") -> None:
        self.imdb_id = imdb_id
        self.calls: list[tuple[str, str]] = []

    async def get_external_ids(self, tmdb_id: str, media_type: str) -> dict[str, str | None]:
        self.calls.append((tmdb_id, media_type))
        return {"imdb_id": self.imdb_id, "tvdb_id": None}


class FakeAsyncSession:
    def __init__(
        self,
        *,
        item: MediaItemORM | None,
        stream_ids: list[str] | None = None,
        active_stream_ids: list[str] | None = None,
        blacklist_ids: set[str] | None = None,
    ) -> None:
        self.item = item
        self.stream_ids = list(stream_ids or [])
        self.active_stream_ids = list(active_stream_ids or [])
        self.blacklist_ids = set(blacklist_ids or set())
        self.added: list[object] = []
        self.committed = False

    async def execute(self, stmt: object) -> object:
        sql = str(stmt)

        class ScalarResult:
            def __init__(self, values: list[object], single: object | None = None) -> None:
                self._values = values
                self._single = single

            def all(self) -> list[object]:
                return list(self._values)

            def scalar_one_or_none(self) -> object | None:
                return self._single

        class Result:
            def __init__(self, values: list[object], single: object | None = None) -> None:
                self._values = values
                self._single = single

            def scalars(self) -> ScalarResult:
                return ScalarResult(self._values, self._single)

            def scalar_one_or_none(self) -> object | None:
                return self._single

        if "FROM media_items" in sql and "WHERE media_items.id" in sql:
            return Result([], self.item)
        if "FROM streams" in sql and "streams.media_item_id" in sql:
            return Result(list(self.stream_ids))
        if "FROM stream_blacklist_relations" in sql:
            return Result(list(self.blacklist_ids))
        if "FROM active_streams" in sql and "active_streams.item_id" in sql:
            return Result(list(self.active_stream_ids))
        if "DELETE FROM active_streams" in sql:
            self.active_stream_ids.clear()
            return Result([])
        raise AssertionError(sql)

    def add(self, obj: object) -> None:
        self.added.append(obj)
        if isinstance(obj, StreamBlacklistRelationORM):
            self.blacklist_ids.add(obj.stream_id)

    async def commit(self) -> None:
        self.committed = True


def _build_item(*, imdb_id: str | None = None, state: str = ItemState.FAILED.value) -> MediaItemORM:
    attributes: dict[str, object] = {"item_type": "movie", "tmdb_id": "550"}
    if imdb_id is not None:
        attributes["imdb_id"] = imdb_id
    return MediaItemORM(
        id="item-1",
        external_ref="tmdb:550",
        title="Fight Club",
        state=state,
        attributes=attributes,
        created_at=datetime(2026, 3, 21, 12, 0, tzinfo=UTC),
        updated_at=datetime(2026, 3, 21, 12, 0, tzinfo=UTC),
    )


def _build_media_service(*, tmdb_client: FakeTmdbClient | None = None) -> MediaService:
    return MediaService(
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        event_bus=EventBus(),
        settings=_build_settings(),
        rate_limiter=DistributedRateLimiter(redis=DummyRedis()),  # type: ignore[arg-type]
        tmdb_client=tmdb_client,  # type: ignore[arg-type]
    )


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
    )


@dataclass
class FakeRouteMediaService:
    arq_required: bool = False

    async def retry_item(
        self,
        item_id: str,
        db: object,
        arq_pool: object | None,
        *,
        tenant_id: str | None = None,
    ) -> MediaItemORM:
        _ = (db, tenant_id)
        if self.arq_required and arq_pool is None:
            raise ArqNotEnabledError("ARQ is not enabled; retry/reset requires the worker to be running")
        return _build_item(imdb_id="tt0137523", state=ItemState.REQUESTED.value)

    async def reset_item(
        self,
        item_id: str,
        db: object,
        arq_pool: object | None,
        *,
        tenant_id: str | None = None,
    ) -> MediaItemORM:
        _ = (item_id, db, tenant_id)
        if self.arq_required and arq_pool is None:
            raise ArqNotEnabledError("ARQ is not enabled; retry/reset requires the worker to be running")
        item = _build_item(imdb_id="tt0137523", state=ItemState.REQUESTED.value)
        item._streams_blacklisted = 2  # type: ignore[attr-defined]
        item._active_stream_cleared = True  # type: ignore[attr-defined]
        return item

    async def search_items(self, **kwargs: Any) -> Any:  # pragma: no cover - route scaffolding only
        raise AssertionError(kwargs)

    async def get_item_detail(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
        raise AssertionError((args, kwargs))

    async def request_items_by_identifiers(self, **kwargs: Any) -> ItemActionResult:  # pragma: no cover
        raise AssertionError(kwargs)

    async def retry_items(self, ids: list[str], *, tenant_id: str | None = None) -> ItemActionResult:
        _ = tenant_id
        return ItemActionResult(message="Items retried.", ids=list(ids))

    async def reset_items(self, ids: list[str], *, tenant_id: str | None = None) -> ItemActionResult:
        _ = tenant_id
        return ItemActionResult(message="Items reset.", ids=list(ids))

    async def remove_items(self, ids: list[str], *, tenant_id: str | None = None) -> ItemActionResult:
        _ = tenant_id
        return ItemActionResult(message="Items removed.", ids=list(ids))


def _build_route_client(*, arq_enabled: bool) -> TestClient:
    settings = _build_settings()
    redis = DummyRedis()
    app = FastAPI()
    app.state.resources = AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=FakeRouteMediaService(arq_required=True),  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
        arq_redis=(FakeArqPool() if arq_enabled else None),
    )
    app.include_router(create_api_router())
    return TestClient(app)


def _route_headers() -> dict[str, str]:
    return {"x-api-key": "a" * 32, "x-actor-roles": "platform:admin"}


@dataclass
class FakeGraphqlMediaService:
    arq_enabled: bool = True

    async def request_items_by_identifiers(self, **kwargs: Any) -> ItemActionResult:
        _ = kwargs
        return ItemActionResult(message="Requested 1 item.", ids=["item-1"])

    async def get_item(self, item_id: str, *, tenant_id: str | None = None) -> Any:
        _ = tenant_id
        if item_id != "item-1":
            return None
        return type("_Record", (), {"id": "item-1", "state": type("_State", (), {"value": "requested"})()})()

    async def retry_items(self, ids: list[str], *, tenant_id: str | None = None) -> ItemActionResult:
        _ = tenant_id
        return ItemActionResult(message="Items retried.", ids=list(ids))

    async def reset_items(self, ids: list[str], *, tenant_id: str | None = None) -> ItemActionResult:
        _ = tenant_id
        return ItemActionResult(message="Items reset.", ids=list(ids))

    async def remove_items(self, ids: list[str], *, tenant_id: str | None = None) -> ItemActionResult:
        _ = tenant_id
        return ItemActionResult(message="Items removed.", ids=list(ids))

    async def retry_item(
        self,
        item_id: str,
        db: object,
        arq_pool: object | None,
        *,
        tenant_id: str | None = None,
    ) -> MediaItemORM:
        _ = (db, tenant_id)
        if not self.arq_enabled or arq_pool is None:
            raise ArqNotEnabledError("ARQ is not enabled; retry/reset requires the worker to be running")
        item = _build_item(imdb_id="tt0137523", state=ItemState.REQUESTED.value)
        item._imdb_id_was_missing = True  # type: ignore[attr-defined]
        item._scrape_job_enqueued = True  # type: ignore[attr-defined]
        return item

    async def reset_item(
        self,
        item_id: str,
        db: object,
        arq_pool: object | None,
        *,
        tenant_id: str | None = None,
    ) -> MediaItemORM:
        _ = (item_id, db, tenant_id)
        if not self.arq_enabled or arq_pool is None:
            raise ArqNotEnabledError("ARQ is not enabled; retry/reset requires the worker to be running")
        item = _build_item(imdb_id="tt0137523", state=ItemState.REQUESTED.value)
        item._imdb_id_was_missing = True  # type: ignore[attr-defined]
        item._scrape_job_enqueued = True  # type: ignore[attr-defined]
        item._streams_blacklisted = 2  # type: ignore[attr-defined]
        item._active_stream_cleared = True  # type: ignore[attr-defined]
        return item


def _build_graphql_context(media_service: FakeGraphqlMediaService) -> GraphQLContext:
    request = Request({"type": "http", "method": "POST", "path": "/graphql", "headers": [], "query_string": b""})
    resources = AppResources(
        settings=_build_settings(),
        redis=DummyRedis(),  # type: ignore[arg-type]
        cache=CacheManager(redis=DummyRedis(), namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=DummyRedis()),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=media_service,  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
        arq_redis=(FakeArqPool() if media_service.arq_enabled else None),
    )
    return GraphQLContext(
        request=request,
        resources=resources,
        media_service=resources.media_service,
        event_bus=resources.event_bus,
        log_stream=resources.log_stream,
        settings_updater=lambda *_args, **_kwargs: True,
    )


def _execute_graphql(query: str, media_service: FakeGraphqlMediaService) -> Any:
    return asyncio.run(build_schema(GraphQLPluginRegistry()).execute(query, context_value=_build_graphql_context(media_service)))


@pytest.mark.asyncio
async def test_retry_item_enqueues_scrape_immediately() -> None:
    service = _build_media_service(tmdb_client=FakeTmdbClient(imdb_id="tt0137523"))
    service._upsert_media_specialization = lambda *args, **kwargs: asyncio.sleep(0)  # type: ignore[method-assign]
    session = FakeAsyncSession(item=_build_item())
    arq_pool = FakeArqPool()

    item = await service.retry_item("item-1", session, arq_pool)  # type: ignore[arg-type]

    assert item.state == ItemState.REQUESTED.value
    assert arq_pool.calls == [("scrape_item", (), {"item_id": "item-1"})]


@pytest.mark.asyncio
async def test_retry_item_enriches_imdb_when_missing() -> None:
    tmdb_client = FakeTmdbClient(imdb_id="tt0137523")
    service = _build_media_service(tmdb_client=tmdb_client)
    service._upsert_media_specialization = lambda *args, **kwargs: asyncio.sleep(0)  # type: ignore[method-assign]
    session = FakeAsyncSession(item=_build_item())

    item = await service.retry_item("item-1", session, FakeArqPool())  # type: ignore[arg-type]

    assert item.attributes["imdb_id"] == "tt0137523"
    assert tmdb_client.calls == [("550", "movie")]


@pytest.mark.asyncio
async def test_retry_item_raises_when_arq_disabled() -> None:
    service = _build_media_service(tmdb_client=FakeTmdbClient())
    session = FakeAsyncSession(item=_build_item())

    try:
        await service.retry_item("item-1", session, None)  # type: ignore[arg-type]
    except ArqNotEnabledError:
        return
    raise AssertionError("expected ArqNotEnabledError")


@pytest.mark.asyncio
async def test_reset_item_blacklists_all_streams() -> None:
    service = _build_media_service(tmdb_client=FakeTmdbClient())
    service._upsert_media_specialization = lambda *args, **kwargs: asyncio.sleep(0)  # type: ignore[method-assign]
    session = FakeAsyncSession(item=_build_item(), stream_ids=["stream-1", "stream-2"])

    await service.reset_item("item-1", session, FakeArqPool())  # type: ignore[arg-type]

    blacklisted = [obj for obj in session.added if isinstance(obj, StreamBlacklistRelationORM)]
    assert len(blacklisted) == 2


@pytest.mark.asyncio
async def test_reset_item_skips_duplicate_blacklist_entries() -> None:
    service = _build_media_service(tmdb_client=FakeTmdbClient())
    service._upsert_media_specialization = lambda *args, **kwargs: asyncio.sleep(0)  # type: ignore[method-assign]
    session = FakeAsyncSession(item=_build_item(), stream_ids=["stream-1", "stream-2"], blacklist_ids={"stream-1"})

    await service.reset_item("item-1", session, FakeArqPool())  # type: ignore[arg-type]

    blacklisted = [obj for obj in session.added if isinstance(obj, StreamBlacklistRelationORM)]
    assert len(blacklisted) == 1
    assert blacklisted[0].stream_id == "stream-2"


@pytest.mark.asyncio
async def test_reset_item_clears_active_stream() -> None:
    service = _build_media_service(tmdb_client=FakeTmdbClient())
    service._upsert_media_specialization = lambda *args, **kwargs: asyncio.sleep(0)  # type: ignore[method-assign]
    session = FakeAsyncSession(item=_build_item(), active_stream_ids=["active-1"])

    item = await service.reset_item("item-1", session, FakeArqPool())  # type: ignore[arg-type]

    assert session.active_stream_ids == []
    assert bool(getattr(item, "_active_stream_cleared", False)) is True


@pytest.mark.asyncio
async def test_reset_item_enqueues_scrape_immediately() -> None:
    service = _build_media_service(tmdb_client=FakeTmdbClient())
    service._upsert_media_specialization = lambda *args, **kwargs: asyncio.sleep(0)  # type: ignore[method-assign]
    session = FakeAsyncSession(item=_build_item())
    arq_pool = FakeArqPool()

    await service.reset_item("item-1", session, arq_pool)  # type: ignore[arg-type]

    assert arq_pool.calls == [("scrape_item", (), {"item_id": "item-1"})]


@pytest.mark.asyncio
async def test_prepare_item_for_scrape_retry_blacklists_selected_stream() -> None:
    service = _build_media_service(tmdb_client=FakeTmdbClient())
    session = FakeAsyncSession(item=_build_item(state=ItemState.DOWNLOADED.value))

    @asynccontextmanager
    async def fake_session() -> AsyncGenerator[FakeAsyncSession, None]:
        yield session

    service._db.session = fake_session  # type: ignore[method-assign]

    item = await service.prepare_item_for_scrape_retry(
        "item-1",
        message="debrid_item retry scheduled: debrid_poll_timeout",
        blacklist_stream_ids=["stream-1", "stream-1"],
    )

    blacklisted = [obj for obj in session.added if isinstance(obj, StreamBlacklistRelationORM)]
    assert item.state is ItemState.REQUESTED
    assert len(blacklisted) == 1
    assert blacklisted[0].stream_id == "stream-1"


@pytest.mark.asyncio
async def test_reset_item_enriches_imdb_when_missing() -> None:
    tmdb_client = FakeTmdbClient(imdb_id="tt0137523")
    service = _build_media_service(tmdb_client=tmdb_client)
    service._upsert_media_specialization = lambda *args, **kwargs: asyncio.sleep(0)  # type: ignore[method-assign]
    session = FakeAsyncSession(item=_build_item())

    item = await service.reset_item("item-1", session, FakeArqPool())  # type: ignore[arg-type]

    assert item.attributes["imdb_id"] == "tt0137523"


@pytest.mark.asyncio
async def test_reset_item_raises_when_arq_disabled() -> None:
    service = _build_media_service(tmdb_client=FakeTmdbClient())
    session = FakeAsyncSession(item=_build_item())

    try:
        await service.reset_item("item-1", session, None)  # type: ignore[arg-type]
    except ArqNotEnabledError:
        return
    raise AssertionError("expected ArqNotEnabledError")


def test_retry_route_returns_item_action_response_shape() -> None:
    client = _build_route_client(arq_enabled=True)
    response = client.post("/api/v1/items/retry", json={"ids": ["item-1"]}, headers=_route_headers())
    assert response.status_code == 200
    assert response.json() == {"message": "Items retried.", "ids": ["item-1"]}


def test_reset_route_returns_item_action_response_shape() -> None:
    client = _build_route_client(arq_enabled=True)
    response = client.post("/api/v1/items/reset", json={"ids": ["item-1"]}, headers=_route_headers())
    assert response.status_code == 200
    assert response.json() == {"message": "Items reset.", "ids": ["item-1"]}


def test_retry_route_returns_400_when_arq_disabled() -> None:
    client = _build_route_client(arq_enabled=False)
    response = client.post("/api/v1/items/retry", json={"ids": ["item-1"]}, headers=_route_headers())
    assert response.status_code == 400
    assert response.json() == {"detail": "ARQ is not enabled; retry/reset requires the worker to be running"}


def test_reset_route_returns_400_when_arq_disabled() -> None:
    client = _build_route_client(arq_enabled=False)
    response = client.post("/api/v1/items/reset", json={"ids": ["item-1"]}, headers=_route_headers())
    assert response.status_code == 400
    assert response.json() == {"detail": "ARQ is not enabled; retry/reset requires the worker to be running"}


def test_retry_item_mutation_returns_rich_result() -> None:
    result = _execute_graphql(
        "mutation { retryItem(itemId: \"item-1\") { itemId success error newState } }",
        FakeGraphqlMediaService(),
    )
    assert result.errors is None
    assert result.data["retryItem"] == {
        "itemId": "item-1",
        "success": True,
        "error": None,
        "newState": "requested",
    }


def test_reset_item_mutation_returns_rich_result_with_blacklist_count() -> None:
    result = _execute_graphql(
        "mutation { resetItem(itemId: \"item-1\") { itemId success error newState } }",
        FakeGraphqlMediaService(),
    )
    assert result.errors is None
    assert result.data["resetItem"] == {
        "itemId": "item-1",
        "success": True,
        "error": None,
        "newState": "requested",
    }


def test_retry_item_mutation_graphql_error_when_arq_disabled() -> None:
    result = _execute_graphql(
        "mutation { retryItem(itemId: \"item-1\") { itemId success error newState } }",
        FakeGraphqlMediaService(arq_enabled=False),
    )
    assert result.errors is None
    assert result.data["retryItem"] == {
        "itemId": "item-1",
        "success": False,
        "error": "ARQ is not enabled; retry/reset requires the worker to be running",
        "newState": None,
    }


def test_reset_item_mutation_graphql_error_when_arq_disabled() -> None:
    result = _execute_graphql(
        "mutation { resetItem(itemId: \"item-1\") { itemId success error newState } }",
        FakeGraphqlMediaService(arq_enabled=False),
    )
    assert result.errors is None
    assert result.data["resetItem"] == {
        "itemId": "item-1",
        "success": False,
        "error": "ARQ is not enabled; retry/reset requires the worker to be running",
        "newState": None,
    }


def test_existing_item_action_mutation_still_works() -> None:
    result = _execute_graphql(
        'mutation { itemAction(input: { itemId: "item-1", action: "retry" }) { item_id to_state } }',
        FakeGraphqlMediaService(),
    )
    assert result.errors is None
    assert result.data["itemAction"]["to_state"] == "requested"
