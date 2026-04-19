from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql import GraphQLPluginRegistry, create_graphql_router
from filmu_py.resources import AppResources
from filmu_py.services.media import MediaService, RequestTimeMetadataRecord
from filmu_py.state.item import ItemState
from tests.db_seed import DbModelFactory, build_test_database_runtime

pytest.importorskip("aiosqlite")


@dataclass
class DummyRedis:
    values: dict[str, bytes] = field(default_factory=dict)

    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def get(self, name: str) -> bytes | None:
        return self.values.get(name)

    async def set(self, name: str, value: bytes | str, ex: int | None = None) -> bool:
        _ = ex
        self.values[name] = value.encode("utf-8") if isinstance(value, str) else value
        return True

    async def delete(self, *names: str) -> int:
        removed = 0
        for name in names:
            if name in self.values:
                removed += 1
            self.values.pop(name, None)
        return removed

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_ARQ_ENABLED=False,
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
        FILMU_PY_SERVICE_NAME="filmu-python-test",
    )


def _graphql_headers() -> dict[str, str]:
    return {
        "x-api-key": "a" * 32,
        "x-tenant-id": "global",
        "x-actor-id": "director-1",
        "x-actor-roles": "playback:operator",
    }


def _build_db_client(tmp_path: Path) -> tuple[Any, MediaService, TestClient]:
    runtime = asyncio.run(build_test_database_runtime(tmp_path, filename="graphql-e2e.db"))
    redis = DummyRedis()
    settings = _build_settings()
    rate_limiter = DistributedRateLimiter(redis=redis)  # type: ignore[arg-type]
    event_bus = EventBus()
    media_service = MediaService(
        runtime,
        event_bus,
        settings=settings,
        rate_limiter=rate_limiter,
    )
    app = FastAPI()
    resources = AppResources(
        settings=settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=rate_limiter,
        event_bus=event_bus,
        db=runtime,
        media_service=media_service,
        graphql_plugin_registry=GraphQLPluginRegistry(),
    )
    app.state.resources = resources
    app.include_router(create_graphql_router(resources.graphql_plugin_registry), prefix="/graphql")
    return runtime, media_service, TestClient(app)


def _patch_request_metadata(
    media_service: MediaService,
    *,
    entries: dict[tuple[str, str], tuple[str, dict[str, object]]],
) -> None:
    async def _fake_fetch_request_metadata(
        *,
        media_type: str,
        identifier: str,
    ) -> Any:
        title, attributes = entries[(media_type, identifier)]
        return media_service._metadata_resolution(
            source="tmdb",
            metadata=RequestTimeMetadataRecord(
                title=title,
                attributes=dict(attributes),
            ),
        )

    media_service._fetch_request_metadata = _fake_fetch_request_metadata  # type: ignore[method-assign]


async def _attach_playback_ready_detail(
    runtime: Any,
    *,
    item_id: str,
    title: str,
    state: str,
) -> None:
    from filmu_py.db.models import MediaItemORM

    factory = DbModelFactory(default_tenant_id="global")
    async with runtime.session() as session:
        item = await session.get(MediaItemORM, item_id)
        assert item is not None
        item.state = state
        attachment = factory.playback_attachment(
            item_id=item_id,
            locator=f"https://edge.example.com/{item_id}",
            restricted_url=f"https://api.example.com/restricted/{item_id}",
            unrestricted_url=f"https://cdn.example.com/{item_id}",
            provider_download_id=f"download-{item_id}",
            provider_file_id=f"provider-file-{item_id}",
            provider_file_path=f"Library/{title}.mkv",
            original_filename=f"{title}.mkv",
            refresh_state="ready",
        )
        media_entry = factory.media_entry(
            item_id=item_id,
            source_attachment_id=attachment.id,
            original_filename=f"{title}.mkv",
            download_url=f"https://api.example.com/restricted/{item_id}",
            unrestricted_url=f"https://cdn.example.com/{item_id}",
            provider_download_id=f"download-{item_id}",
            provider_file_id=f"provider-file-{item_id}",
            provider_file_path=f"Library/{title}.mkv",
            refresh_state="ready",
        )
        active_stream = factory.active_stream(
            item_id=item_id,
            media_entry_id=media_entry.id,
            role="direct",
        )
        session.add_all([attachment, media_entry, active_stream])
        await session.commit()


def test_graphql_db_backed_request_flow_persists_detail_and_reports_playback_ready(
    tmp_path: Path,
) -> None:
    runtime, media_service, client = _build_db_client(tmp_path)
    _patch_request_metadata(
        media_service,
        entries={
            (
                "movie",
                "tmdb:603",
            ): (
                "The Matrix",
                {
                    "item_type": "movie",
                    "tmdb_id": "603",
                    "imdb_id": "tt0133093",
                    "poster_path": "/matrix.jpg",
                },
            )
        },
    )

    try:
        with client:
            requested = client.post(
                "/graphql",
                headers=_graphql_headers(),
                json={
                    "query": """
                        mutation RequestItem($externalRef: String!, $mediaType: String!) {
                          requestItem(input: { externalRef: $externalRef, mediaType: $mediaType }) {
                            itemId
                            enrichmentSource
                            hasPoster
                            hasImdbId
                          }
                        }
                    """,
                    "variables": {"externalRef": "tmdb:603", "mediaType": "movie"},
                },
            )
            assert requested.status_code == 200
            requested_payload = requested.json()
            assert "errors" not in requested_payload, requested_payload
            payload = requested_payload["data"]["requestItem"]
            assert payload["enrichmentSource"] == "tmdb"
            assert payload["hasPoster"] is True
            assert payload["hasImdbId"] is True

            item_id = payload["itemId"]
            asyncio.run(
                _attach_playback_ready_detail(
                    runtime,
                    item_id=item_id,
                    title="The Matrix",
                    state=ItemState.DOWNLOADED.value,
                )
            )

            detail = client.post(
                "/graphql",
                headers=_graphql_headers(),
                json={
                    "query": """
                        query RequestedItem($id: ID!) {
                          mediaItem(id: $id) {
                            title
                            request {
                              isPartial
                              requestSource
                            }
                            requestLifecycle {
                              state
                              playbackReady
                              cta
                            }
                            resolvedPlayback {
                              directReady
                            }
                          }
                        }
                    """,
                    "variables": {"id": item_id},
                },
            )
            assert detail.status_code == 200
            detail_payload = detail.json()
            assert "errors" not in detail_payload, detail_payload
            assert detail_payload["data"]["mediaItem"] == {
                "title": "The Matrix",
                "request": {
                    "isPartial": False,
                    "requestSource": "api",
                },
                "requestLifecycle": {
                    "state": "ready",
                    "playbackReady": True,
                    "cta": "watch",
                },
                "resolvedPlayback": {"directReady": True},
            }
    finally:
        asyncio.run(runtime.dispose())


def test_graphql_db_backed_partial_show_flow_exposes_scope_and_partial_ready(
    tmp_path: Path,
) -> None:
    runtime, media_service, client = _build_db_client(tmp_path)
    _patch_request_metadata(
        media_service,
        entries={
            (
                "tv",
                "tmdb:1396",
            ): (
                "Breaking Bad",
                {
                    "item_type": "show",
                    "tmdb_id": "1396",
                    "tvdb_id": "81189",
                    "imdb_id": "tt0903747",
                    "poster_path": "/breaking-bad.jpg",
                },
            )
        },
    )

    try:
        with client:
            requested = client.post(
                "/graphql",
                headers=_graphql_headers(),
                json={
                    "query": """
                        mutation RequestItem(
                          $externalRef: String!
                          $mediaType: String!
                          $requestedSeasons: [Int!]
                          $requestedEpisodes: [RequestedEpisodeScopeInput!]
                        ) {
                          requestItem(
                            input: {
                              externalRef: $externalRef
                              mediaType: $mediaType
                              requestedSeasons: $requestedSeasons
                              requestedEpisodes: $requestedEpisodes
                            }
                          ) {
                            itemId
                            enrichmentSource
                          }
                        }
                    """,
                    "variables": {
                        "externalRef": "tmdb:1396",
                        "mediaType": "tv",
                        "requestedSeasons": [1, 2],
                        "requestedEpisodes": [
                            {"seasonNumber": 1, "episodeNumbers": [1, 2]},
                            {"seasonNumber": 2, "episodeNumbers": [3]},
                        ],
                    },
                },
            )
            assert requested.status_code == 200
            requested_payload = requested.json()
            assert "errors" not in requested_payload, requested_payload
            payload = requested_payload["data"]["requestItem"]
            assert payload["enrichmentSource"] == "tmdb"

            item_id = payload["itemId"]
            asyncio.run(
                _attach_playback_ready_detail(
                    runtime,
                    item_id=item_id,
                    title="Breaking Bad S01E01",
                    state=ItemState.PARTIALLY_COMPLETED.value,
                )
            )

            detail = client.post(
                "/graphql",
                headers=_graphql_headers(),
                json={
                    "query": """
                        query RequestedItem($id: ID!) {
                          mediaItem(id: $id) {
                            title
                            request {
                              isPartial
                              requestedSeasons
                              requestedEpisodes {
                                seasonNumber
                                episodeNumbers
                              }
                              requestSource
                            }
                            requestLifecycle {
                              state
                              playbackReady
                              cta
                            }
                            resolvedPlayback {
                              directReady
                            }
                          }
                        }
                    """,
                    "variables": {"id": item_id},
                },
            )
            assert detail.status_code == 200
            detail_payload = detail.json()
            assert "errors" not in detail_payload, detail_payload
            assert detail_payload["data"]["mediaItem"] == {
                "title": "Breaking Bad",
                "request": {
                    "isPartial": True,
                    "requestedSeasons": [1, 2],
                    "requestedEpisodes": [
                        {"seasonNumber": 1, "episodeNumbers": [1, 2]},
                        {"seasonNumber": 2, "episodeNumbers": [3]},
                    ],
                    "requestSource": "api",
                },
                "requestLifecycle": {
                    "state": "partial_ready",
                    "playbackReady": True,
                    "cta": "watch",
                },
                "resolvedPlayback": {"directReady": True},
            }
    finally:
        asyncio.run(runtime.dispose())
