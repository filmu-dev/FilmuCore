"""Item list/detail/action compatibility route tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.api.playback_resolution import PlaybackAttachment
from filmu_py.api.router import create_api_router
from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.db.models import (
    ActiveStreamORM,
    EpisodeORM,
    MediaEntryORM,
    MediaItemORM,
    PlaybackAttachmentORM,
    SeasonORM,
    ShowORM,
)
from filmu_py.graphql.plugin_registry import GraphQLPluginRegistry
from filmu_py.resources import AppResources
from filmu_py.services.media import (
    ActiveStreamDetailRecord,
    ActiveStreamOwnerRecord,
    ItemActionResult,
    ItemRequestSummaryRecord,
    MediaEntryDetailRecord,
    MediaItemsPage,
    MediaItemSpecializationRecord,
    MediaItemSummaryRecord,
    ParentIdsRecord,
    PlaybackAttachmentDetailRecord,
    ResolvedPlaybackAttachmentRecord,
    ResolvedPlaybackSnapshotRecord,
    _build_detail_record,
    _item_matches_identifier,
)
from filmu_py.services.playback import PlaybackResolutionSnapshot, PlaybackSourceService


class DummyRedis:
    """Minimal Redis stub used by route-level tests without network dependencies."""

    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None


class DummyDatabaseRuntime:
    """No-op DB runtime placeholder for application resources in tests."""

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[None, None]:
        yield None

    async def dispose(self) -> None:
        return None


@dataclass
class DummyMediaService:
    """Deterministic media-service test double for item compatibility routes."""

    last_detail_request: tuple[str, str] | None = None
    last_search_tenant_id: str | None = None
    last_detail_tenant_id: str | None = None
    last_reset_tenant_id: str | None = None
    last_retry_tenant_id: str | None = None
    last_remove_tenant_id: str | None = None

    async def search_items(
        self,
        *,
        limit: int,
        page: int,
        item_types: list[str] | None,
        states: list[str] | None,
        sort: list[str] | None,
        search: str | None,
        extended: bool,
        tenant_id: str | None,
    ) -> MediaItemsPage:
        _ = (item_types, states, sort, search, extended)
        self.last_search_tenant_id = tenant_id
        return MediaItemsPage(
            success=True,
            items=[
                MediaItemSummaryRecord(
                    id="item-1",
                    type="movie",
                    title="Example Movie",
                    state="completed",
                    tmdb_id="123",
                    poster_path="/poster.jpg",
                    aired_at="2024-01-01T00:00:00+00:00",
                    next_retry_at="2026-03-22T22:00:00+00:00",
                    recovery_attempt_count=2,
                    is_in_cooldown=True,
                    specialization=MediaItemSpecializationRecord(
                        item_type="movie",
                        tmdb_id="123",
                        imdb_id="tt1234567",
                    ),
                )
            ],
            page=page,
            limit=limit,
            total_items=1,
            total_pages=1,
        )

    async def get_item_detail(
        self,
        item_identifier: str,
        *,
        media_type: str,
        extended: bool,
        tenant_id: str | None = None,
    ) -> MediaItemSummaryRecord | None:
        self.last_detail_request = (item_identifier, media_type)
        self.last_detail_tenant_id = tenant_id
        if item_identifier == "missing":
            return None
        active_stream = None
        media_entries = None
        playback_attachments = None
        resolved_playback = None
        if extended:
            active_stream = ActiveStreamDetailRecord(
                direct_ready=True,
                hls_ready=True,
                missing_local_file=False,
                direct_owner=ActiveStreamOwnerRecord(
                    media_entry_index=0,
                    kind="remote-direct",
                    original_filename="Movie.mkv",
                    provider="realdebrid",
                    provider_download_id="torrent-123",
                    provider_file_id="file-7",
                    provider_file_path="folder/Movie.mkv",
                ),
                hls_owner=None,
            )
            media_entries = [
                MediaEntryDetailRecord(
                    entry_type="media",
                    kind="remote-direct",
                    original_filename="Movie.mkv",
                    url="https://cdn.example.com/direct",
                    local_path=None,
                    download_url="https://api.real-debrid.com/restricted-link",
                    unrestricted_url="https://cdn.example.com/direct",
                    provider="realdebrid",
                    provider_download_id="torrent-123",
                    provider_file_id="file-7",
                    provider_file_path="folder/Movie.mkv",
                    size=123,
                    created="2026-03-12T10:00:00+00:00",
                    modified="2026-03-12T12:00:00+00:00",
                    active_for_direct=True,
                    active_for_hls=False,
                    is_active_stream=True,
                )
            ]
            playback_attachments = [
                PlaybackAttachmentDetailRecord(
                    id="attachment-1",
                    kind="remote-direct",
                    locator="https://cdn.example.com/direct",
                    source_key="persisted",
                    provider="realdebrid",
                    provider_download_id="torrent-123",
                    provider_file_id="file-7",
                    provider_file_path="folder/Movie.mkv",
                    original_filename="Movie.mkv",
                    file_size=123,
                    restricted_url="https://api.real-debrid.com/restricted-link",
                    unrestricted_url="https://cdn.example.com/direct",
                    is_preferred=True,
                    preference_rank=1,
                    refresh_state="ready",
                    expires_at="2026-03-19T12:00:00+00:00",
                    last_refreshed_at="2026-03-12T12:00:00+00:00",
                )
            ]
            resolved_playback = ResolvedPlaybackSnapshotRecord(
                direct=ResolvedPlaybackAttachmentRecord(
                    kind="remote-direct",
                    locator="https://cdn.example.com/direct",
                    source_key="persisted",
                    provider="realdebrid",
                    provider_download_id="torrent-123",
                    provider_file_id="file-7",
                    provider_file_path="folder/Movie.mkv",
                    original_filename="Movie.mkv",
                    file_size=123,
                    restricted_url="https://api.real-debrid.com/restricted-link",
                    unrestricted_url="https://cdn.example.com/direct",
                ),
                hls=ResolvedPlaybackAttachmentRecord(
                    kind="local-file",
                    locator="E:/media/Movie.mkv",
                    source_key="persisted",
                    local_path="E:/media/Movie.mkv",
                    original_filename="Movie.mkv",
                    file_size=123,
                ),
                direct_ready=True,
                hls_ready=True,
                missing_local_file=False,
            )
        return MediaItemSummaryRecord(
            id="item-1",
            type="movie" if media_type != "tv" else "show",
            title="Example Movie",
            state="completed",
            tmdb_id="123",
            tvdb_id="456" if media_type == "tv" else None,
            parent_ids=ParentIdsRecord(tvdb_id="456") if media_type == "tv" else None,
            poster_path="/poster.jpg",
            aired_at="2024-01-01T00:00:00+00:00",
            external_ref=item_identifier,
            next_retry_at="2026-03-22T22:00:00+00:00",
            recovery_attempt_count=2,
            is_in_cooldown=True,
            specialization=MediaItemSpecializationRecord(
                item_type="show" if media_type == "tv" else "movie",
                tmdb_id="123",
                tvdb_id="456" if media_type == "tv" else None,
                imdb_id="tt1234567",
                parent_ids=ParentIdsRecord(tvdb_id="456") if media_type == "tv" else None,
                show_title="Example Show" if media_type == "tv" else None,
                season_number=3 if media_type == "tv" else None,
                episode_number=7 if media_type == "tv" else None,
            ),
            metadata={"item_type": "movie", "year": 2024},
            request=ItemRequestSummaryRecord(
                is_partial=True,
                requested_seasons=[1, 2],
                requested_episodes={"1": [1, 2]},
                request_source="mdblist:list-a",
            ),
            active_stream=active_stream,
            media_entries=media_entries,
            playback_attachments=playback_attachments,
            resolved_playback=resolved_playback,
        )

    async def reset_items(self, ids: list[str]) -> ItemActionResult:
        return ItemActionResult(message="Items reset.", ids=ids)

    async def retry_items(self, ids: list[str]) -> ItemActionResult:
        return ItemActionResult(message="Items retried.", ids=ids)

    async def reset_item(
        self,
        item_id: str,
        db: object,
        arq_pool: object,
        *,
        tenant_id: str | None = None,
    ) -> Any:
        _ = (db, arq_pool)
        self.last_reset_tenant_id = tenant_id
        return type("_ResetItem", (), {"id": item_id})()

    async def retry_item(
        self,
        item_id: str,
        db: object,
        arq_pool: object,
        *,
        tenant_id: str | None = None,
    ) -> Any:
        _ = (db, arq_pool)
        self.last_retry_tenant_id = tenant_id
        return type("_RetryItem", (), {"id": item_id})()

    async def remove_items(
        self,
        ids: list[str],
        *,
        tenant_id: str | None = None,
    ) -> ItemActionResult:
        self.last_remove_tenant_id = tenant_id
        return ItemActionResult(message="Items removed.", ids=ids)

    async def request_items_by_identifiers(
        self,
        *,
        media_type: str,
        tmdb_ids: list[str] | None = None,
        tvdb_ids: list[str] | None = None,
        requested_seasons: list[int] | None = None,
        requested_episodes: dict[str, list[int]] | None = None,
        tenant_id: str = "global",
    ) -> ItemActionResult:
        _ = (requested_seasons, requested_episodes)
        identifiers = tmdb_ids if media_type == "movie" else tvdb_ids
        resolved = [identifier for identifier in identifiers or [] if identifier]
        if not resolved:
            raise ValueError("no identifiers supplied for requested media type")

        noun = "movie" if media_type == "movie" else "show"
        plural_suffix = "" if len(resolved) == 1 else "s"
        return ItemActionResult(
            message=f"Requested {len(resolved)} {noun}{plural_suffix}.",
            ids=resolved,
        )


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
    )


def _build_client(*, media_service: DummyMediaService | None = None) -> TestClient:
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
        media_service=media_service or DummyMediaService(),  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
    )
    app.include_router(create_api_router())
    return TestClient(app)


def _headers() -> dict[str, str]:
    return {
        "x-api-key": "a" * 32,
        "x-actor-roles": "platform:admin",
        "x-tenant-id": "tenant-main",
    }


def test_items_route_returns_paginated_payload() -> None:
    media_service = DummyMediaService()
    client = _build_client(media_service=media_service)
    response = client.get(
        "/api/v1/items",
        params={"limit": 24, "page": 1, "type": ["movie"], "states": ["Completed"]},
        headers=_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["page"] == 1
    assert body["total_items"] == 1
    assert body["items"][0]["tmdb_id"] == "123"
    assert body["items"][0]["next_retry_at"] == "2026-03-22T22:00:00Z"
    assert body["items"][0]["recovery_attempt_count"] == 2
    assert body["items"][0]["is_in_cooldown"] is True
    assert media_service.last_search_tenant_id == "tenant-main"


def test_get_item_route_returns_detail_payload() -> None:
    media_service = DummyMediaService()
    client = _build_client(media_service=media_service)
    response = client.get(
        "/api/v1/items/123",
        params={"media_type": "movie", "extended": True},
        headers=_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "item-1"
    assert body["metadata"]["year"] == 2024
    assert body["request"]["is_partial"] is True
    assert body["request"]["requested_seasons"] == [1, 2]
    assert body["request"]["requested_episodes"] == {"1": [1, 2]}
    assert body["request"]["request_source"] == "mdblist:list-a"
    assert body["playback_attachments"][0]["provider"] == "realdebrid"
    assert body["playback_attachments"][0]["provider_file_id"] == "file-7"
    assert body["playback_attachments"][0]["refresh_state"] == "ready"
    assert body["resolved_playback"]["direct_ready"] is True
    assert body["resolved_playback"]["hls_ready"] is True
    assert body["active_stream"]["direct_ready"] is True
    assert body["active_stream"]["hls_ready"] is True
    assert body["active_stream"]["direct_owner"]["media_entry_index"] == 0
    assert body["active_stream"]["hls_owner"] is None
    assert body["resolved_playback"]["direct"]["provider"] == "realdebrid"
    assert body["resolved_playback"]["hls"]["kind"] == "local-file"
    assert body["media_entries"][0]["entry_type"] == "media"
    assert media_service.last_detail_tenant_id == "tenant-main"
    assert body["media_entries"][0]["provider_download_id"] == "torrent-123"
    assert body["media_entries"][0]["original_filename"] == "Movie.mkv"
    assert body["media_entries"][0]["active_for_direct"] is True
    assert body["media_entries"][0]["active_for_hls"] is False
    assert body["media_entries"][0]["is_active_stream"] is True
    assert body["next_retry_at"] == "2026-03-22T22:00:00Z"
    assert body["recovery_attempt_count"] == 2
    assert body["is_in_cooldown"] is True


def test_get_item_route_uses_item_lookup_for_external_refs() -> None:
    media_service = DummyMediaService()
    client = _build_client(media_service=media_service)

    response = client.get(
        "/api/v1/items/tvdb:368207",
        params={"media_type": "tv", "extended": True},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert media_service.last_detail_request == ("tvdb:368207", "item")


def test_get_item_route_keeps_typed_lookup_for_plain_identifiers() -> None:
    media_service = DummyMediaService()
    client = _build_client(media_service=media_service)

    response = client.get(
        "/api/v1/items/123",
        params={"media_type": "movie", "extended": True},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert media_service.last_detail_request == ("123", "movie")


def test_add_items_route_accepts_movie_identifier_payload() -> None:
    client = _build_client()
    response = client.post(
        "/api/v1/items/add",
        json={"media_type": "movie", "tmdb_ids": ["123", "456"], "tvdb_ids": []},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {"message": "Requested 2 movies."}


def test_add_items_route_returns_404_when_media_type_has_no_identifiers() -> None:
    client = _build_client()
    response = client.post(
        "/api/v1/items/add",
        json={"media_type": "tv", "tmdb_ids": [], "tvdb_ids": []},
        headers=_headers(),
    )

    assert response.status_code == 404


def test_add_items_route_requires_admin_role() -> None:
    client = _build_client()
    response = client.post(
        "/api/v1/items/add",
        json={"media_type": "movie", "tmdb_ids": ["123"], "tvdb_ids": []},
        headers={"x-api-key": "a" * 32, "x-actor-roles": "library:viewer"},
    )

    assert response.status_code == 403


def test_item_identifier_matching_accepts_namespaced_external_refs() -> None:
    detail = MediaItemSummaryRecord(
        id="item-1",
        type="movie",
        title="Example Movie",
        tmdb_id="123",
        external_ref="tmdb:123",
    )

    assert _item_matches_identifier(detail, media_type="movie", item_identifier="123")
    assert _item_matches_identifier(detail, media_type="item", item_identifier="tmdb:123")


def test_build_detail_record_links_active_stream_to_matching_media_entry() -> None:
    item = MediaItemORM(
        id="item-active-detail",
        external_ref="tt1234567",
        title="Active Detail Movie",
        state="completed",
        attributes={"item_type": "movie"},
    )
    attachment = PlaybackAttachmentORM(
        id="attachment-active-detail",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/direct",
        provider="realdebrid",
        provider_download_id="torrent-123",
        provider_file_id="file-7",
        provider_file_path="folder/Movie.mkv",
        original_filename="Movie.mkv",
        file_size=123,
        restricted_url="https://api.real-debrid.com/restricted-link",
        unrestricted_url="https://cdn.example.com/direct",
        is_preferred=True,
        preference_rank=1,
        refresh_state="ready",
        created_at=datetime(2026, 3, 12, 10, tzinfo=UTC),
        updated_at=datetime(2026, 3, 12, 12, tzinfo=UTC),
    )
    item.playback_attachments = [attachment]

    class StubPlaybackService:
        def build_resolution_snapshot(self, item: MediaItemORM) -> PlaybackResolutionSnapshot:
            _ = item
            return PlaybackResolutionSnapshot(
                direct=PlaybackAttachment(
                    kind="remote-direct",
                    locator="https://cdn.example.com/direct",
                    source_key="persisted",
                    provider="realdebrid",
                    provider_download_id="torrent-123",
                    provider_file_id="file-7",
                    provider_file_path="folder/Movie.mkv",
                    original_filename="Movie.mkv",
                    file_size=123,
                    restricted_url="https://api.real-debrid.com/restricted-link",
                    unrestricted_url="https://cdn.example.com/direct",
                ),
                hls=None,
                direct_ready=True,
                hls_ready=False,
                missing_local_file=False,
            )

    detail = _build_detail_record(
        item,
        extended=True,
        playback_service=cast(PlaybackSourceService, StubPlaybackService()),
    )

    assert detail.active_stream is not None
    assert detail.active_stream.direct_ready is True
    assert detail.active_stream.hls_ready is False
    assert detail.active_stream.direct_owner is not None
    assert detail.active_stream.direct_owner.media_entry_index == 0
    assert detail.active_stream.hls_owner is None
    assert detail.media_entries is not None
    assert detail.media_entries[0].active_for_direct is True
    assert detail.media_entries[0].active_for_hls is False
    assert detail.media_entries[0].is_active_stream is True


def test_build_detail_record_allows_same_media_entry_to_own_direct_and_hls() -> None:
    created_at = datetime(2026, 3, 12, 10, tzinfo=UTC)
    updated_at = datetime(2026, 3, 12, 12, tzinfo=UTC)
    item = MediaItemORM(
        id="item-shared-owner",
        external_ref="tt7654321",
        title="Shared Owner Movie",
        state="completed",
        attributes={"item_type": "movie"},
    )
    attachment = PlaybackAttachmentORM(
        id="attachment-shared-owner",
        item_id=item.id,
        kind="local-file",
        locator="E:/media/Movie.mkv",
        local_path="E:/media/Movie.mkv",
        original_filename="Movie.mkv",
        file_size=456,
        is_preferred=True,
        preference_rank=1,
        refresh_state="ready",
        created_at=created_at,
        updated_at=updated_at,
    )
    item.playback_attachments = [attachment]

    shared_attachment = PlaybackAttachment(
        kind="local-file",
        locator="E:/media/Movie.mkv",
        source_key="persisted",
        original_filename="Movie.mkv",
        file_size=456,
        local_path="E:/media/Movie.mkv",
    )

    class StubPlaybackService:
        def build_resolution_snapshot(self, item: MediaItemORM) -> PlaybackResolutionSnapshot:
            _ = item
            return PlaybackResolutionSnapshot(
                direct=shared_attachment,
                hls=shared_attachment,
                direct_ready=True,
                hls_ready=True,
                missing_local_file=False,
            )

    detail = _build_detail_record(
        item,
        extended=True,
        playback_service=cast(PlaybackSourceService, StubPlaybackService()),
    )

    assert detail.active_stream is not None
    assert detail.active_stream.direct_owner is not None
    assert detail.active_stream.hls_owner is not None
    assert detail.active_stream.direct_owner.media_entry_index == 0
    assert detail.active_stream.hls_owner.media_entry_index == 0
    assert detail.media_entries is not None
    assert detail.media_entries[0].active_for_direct is True
    assert detail.media_entries[0].active_for_hls is True
    assert detail.media_entries[0].is_active_stream is True


def test_build_detail_record_prefers_persisted_media_entries_over_attachment_projection() -> None:
    created_at = datetime(2026, 3, 12, 10, tzinfo=UTC)
    updated_at = datetime(2026, 3, 12, 12, tzinfo=UTC)
    item = MediaItemORM(
        id="item-persisted-media-entry",
        external_ref="tt1122334",
        title="Persisted Media Entry Movie",
        state="completed",
        attributes={"item_type": "movie"},
    )
    attachment = PlaybackAttachmentORM(
        id="attachment-persisted-media-entry",
        item_id=item.id,
        kind="remote-direct",
        locator="https://cdn.example.com/attachment-direct",
        provider="realdebrid",
        provider_download_id="torrent-456",
        provider_file_id="file-9",
        provider_file_path="folder/Attachment Movie.mkv",
        original_filename="Attachment Movie.mkv",
        file_size=789,
        restricted_url="https://api.real-debrid.com/restricted-attachment",
        unrestricted_url="https://cdn.example.com/attachment-direct",
        is_preferred=True,
        preference_rank=1,
        refresh_state="ready",
        created_at=created_at,
        updated_at=updated_at,
    )
    media_entry = MediaEntryORM(
        id="media-entry-1",
        item_id=item.id,
        source_attachment_id=attachment.id,
        entry_type="media",
        kind="remote-direct",
        original_filename="Persisted Media Entry.mkv",
        download_url="https://api.real-debrid.com/restricted-media-entry",
        unrestricted_url="https://cdn.example.com/persisted-media-entry",
        provider="realdebrid",
        provider_download_id="torrent-456",
        provider_file_id="file-9",
        provider_file_path="folder/Persisted Media Entry.mkv",
        size_bytes=789,
        created_at=created_at,
        updated_at=updated_at,
    )
    item.playback_attachments = [attachment]
    item.media_entries = [media_entry]

    class StubPlaybackService:
        def build_resolution_snapshot(self, item: MediaItemORM) -> PlaybackResolutionSnapshot:
            _ = item
            return PlaybackResolutionSnapshot(
                direct=PlaybackAttachment(
                    kind="remote-direct",
                    locator="https://cdn.example.com/attachment-direct",
                    source_key="persisted",
                    provider="realdebrid",
                    provider_download_id="torrent-456",
                    provider_file_id="file-9",
                    provider_file_path="folder/Attachment Movie.mkv",
                    original_filename="Attachment Movie.mkv",
                    file_size=789,
                    restricted_url="https://api.real-debrid.com/restricted-attachment",
                    unrestricted_url="https://cdn.example.com/attachment-direct",
                ),
                hls=None,
                direct_ready=True,
                hls_ready=False,
                missing_local_file=False,
            )

    detail = _build_detail_record(
        item,
        extended=True,
        playback_service=cast(PlaybackSourceService, StubPlaybackService()),
    )

    assert detail.media_entries is not None
    assert detail.media_entries[0].original_filename == "Persisted Media Entry.mkv"
    assert detail.media_entries[0].url == "https://cdn.example.com/persisted-media-entry"
    assert (
        detail.media_entries[0].download_url == "https://api.real-debrid.com/restricted-media-entry"
    )
    assert detail.active_stream is not None
    assert detail.active_stream.direct_owner is not None
    assert detail.active_stream.direct_owner.media_entry_index == 0


def test_build_detail_record_prefers_persisted_active_stream_relation() -> None:
    created_at = datetime(2026, 3, 12, 10, tzinfo=UTC)
    updated_at = datetime(2026, 3, 12, 12, tzinfo=UTC)
    item = MediaItemORM(
        id="item-persisted-active-stream",
        external_ref="tt5566778",
        title="Persisted Active Stream Movie",
        state="completed",
        attributes={"item_type": "movie"},
    )
    first_entry = MediaEntryORM(
        id="media-entry-first",
        item_id=item.id,
        entry_type="media",
        kind="remote-direct",
        original_filename="First Entry.mkv",
        unrestricted_url="https://cdn.example.com/first-entry",
        created_at=created_at,
        updated_at=updated_at,
    )
    second_entry = MediaEntryORM(
        id="media-entry-second",
        item_id=item.id,
        entry_type="media",
        kind="remote-direct",
        original_filename="Second Entry.mkv",
        unrestricted_url="https://cdn.example.com/second-entry",
        created_at=created_at,
        updated_at=updated_at,
    )
    active_stream = ActiveStreamORM(
        id="active-stream-direct",
        item_id=item.id,
        media_entry_id=second_entry.id,
        role="direct",
        created_at=created_at,
        updated_at=updated_at,
    )
    item.media_entries = [first_entry, second_entry]
    item.active_streams = [active_stream]

    class StubPlaybackService:
        def build_resolution_snapshot(self, item: MediaItemORM) -> PlaybackResolutionSnapshot:
            _ = item
            return PlaybackResolutionSnapshot(
                direct=None,
                hls=None,
                direct_ready=False,
                hls_ready=False,
                missing_local_file=False,
            )

    detail = _build_detail_record(
        item,
        extended=True,
        playback_service=cast(PlaybackSourceService, StubPlaybackService()),
    )

    assert detail.active_stream is not None
    assert detail.active_stream.direct_ready is False
    assert detail.active_stream.direct_owner is not None
    assert detail.active_stream.direct_owner.media_entry_index == 1
    assert detail.media_entries is not None
    assert detail.media_entries[0].is_active_stream is False
    assert detail.media_entries[1].active_for_direct is True
    assert detail.media_entries[1].is_active_stream is True


def test_build_detail_record_prefers_specialization_season_coverage_over_path_inference() -> None:
    item = MediaItemORM(
        id="item-specialization-season-coverage",
        external_ref="tvdb:12345",
        title="Canonical Show",
        state="completed",
        attributes={
            "item_type": "show",
            "show_title": "Wrong Metadata Show",
        },
    )
    show = ShowORM(
        media_item_id=item.id,
        tmdb_id="12345",
        tvdb_id="54321",
        imdb_id="tt9988776",
    )
    show.media_item = item
    show.seasons = [
        SeasonORM(media_item_id="season-item-2", show_id=show.id, season_number=2),
        SeasonORM(media_item_id="season-item-4", show_id=show.id, season_number=4),
    ]
    item.show = show
    item.media_entries = [
        MediaEntryORM(
            id="media-entry-specialization-season-coverage",
            item_id=item.id,
            entry_type="media",
            kind="remote-direct",
            original_filename="Wrong Metadata Show S09-S10 Pack.mkv",
            provider_file_path="Wrong Metadata Show S09-S10 Pack.mkv",
            refresh_state="ready",
            created_at=datetime(2026, 3, 12, 10, tzinfo=UTC),
            updated_at=datetime(2026, 3, 12, 12, tzinfo=UTC),
        )
    ]

    detail = _build_detail_record(item, extended=False)

    assert detail.type == "show"
    assert detail.covered_season_numbers == [2, 4]


def test_build_detail_record_normalizes_extended_metadata_from_specialization() -> None:
    show_item = MediaItemORM(
        id="item-specialization-metadata-show",
        external_ref="tvdb:777",
        title="Canonical Show",
        state="completed",
        attributes={
            "item_type": "movie",
            "tmdb_id": "metadata-tmdb",
            "tvdb_id": "metadata-tvdb",
            "show_title": "Wrong Metadata Show",
            "season_number": 9,
            "episode_number": 99,
            "parent_ids": {"tmdb_id": "111", "tvdb_id": "222"},
        },
    )
    show = ShowORM(media_item_id=show_item.id, tmdb_id="special-show", tvdb_id="special-tv")
    show.media_item = show_item

    season = SeasonORM(media_item_id="season-specialization-metadata", show_id=show.id, season_number=2)
    season.show = show
    show.seasons = [season]

    episode_item = MediaItemORM(
        id="item-specialization-metadata-episode",
        external_ref="tvdb:episode-777",
        title="Episode Title",
        state="completed",
        attributes={
            "item_type": "movie",
            "tmdb_id": "metadata-episode-tmdb",
            "tvdb_id": "metadata-episode-tvdb",
            "show_title": "Wrong Metadata Show",
            "season_number": 9,
            "episode_number": 99,
            "parent_ids": {"tmdb_id": "111", "tvdb_id": "222"},
        },
    )
    episode_item.episode = EpisodeORM(
        media_item_id=episode_item.id,
        season_id=season.id,
        episode_number=3,
        tmdb_id="special-episode-tmdb",
        tvdb_id="special-episode-tvdb",
        imdb_id="tt-special-episode",
    )
    episode_item.episode.media_item = episode_item
    episode_item.episode.season = season
    season.episodes = [episode_item.episode]

    detail = _build_detail_record(episode_item, extended=True)

    assert detail.metadata is not None
    assert detail.metadata["item_type"] == "episode"
    assert detail.metadata["tmdb_id"] == "special-episode-tmdb"
    assert detail.metadata["tvdb_id"] == "special-episode-tvdb"
    assert detail.metadata["imdb_id"] == "tt-special-episode"
    assert detail.metadata["show_title"] == "Canonical Show"
    assert detail.metadata["season_number"] == 2
    assert detail.metadata["episode_number"] == 3
    assert detail.metadata["parent_ids"] == {"tmdb_id": "special-show", "tvdb_id": "special-tv"}


def test_get_item_route_returns_404_for_missing_item() -> None:
    client = _build_client()
    response = client.get(
        "/api/v1/items/missing",
        params={"media_type": "movie"},
        headers=_headers(),
    )

    assert response.status_code == 404


def test_item_action_routes_return_message_and_ids() -> None:
    client = _build_client()
    payload = {"ids": ["item-1", "item-2"]}

    reset_response = client.post("/api/v1/items/reset", json=payload, headers=_headers())
    retry_response = client.post("/api/v1/items/retry", json=payload, headers=_headers())
    remove_response = client.request(
        "DELETE", "/api/v1/items/remove", json=payload, headers=_headers()
    )

    assert reset_response.status_code == 200
    assert reset_response.json() == {"message": "Items reset.", "ids": ["item-1", "item-2"]}
    assert retry_response.status_code == 200
    assert retry_response.json() == {
        "message": "Items retried.",
        "ids": ["item-1", "item-2"],
    }
    assert remove_response.status_code == 200
    assert remove_response.json() == {
        "message": "Items removed.",
        "ids": ["item-1", "item-2"],
    }


def test_item_routes_require_api_key() -> None:
    client = _build_client()
    cases: list[tuple[str, str, dict[str, Any]]] = [
        ("GET", "/api/v1/items", {}),
        ("GET", "/api/v1/items/123", {"params": {"media_type": "movie"}}),
        ("POST", "/api/v1/items/reset", {"json": {"ids": ["item-1"]}}),
        ("POST", "/api/v1/items/retry", {"json": {"ids": ["item-1"]}}),
        ("DELETE", "/api/v1/items/remove", {"json": {"ids": ["item-1"]}}),
    ]
    for method, path, kwargs in cases:
        response = client.request(method, path, **kwargs)
        assert response.status_code == 401
