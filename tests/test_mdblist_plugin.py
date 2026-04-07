from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import httpx

from filmu_py.plugins import ContentRequest, TestPluginContext
from filmu_py.plugins.builtin.mdblist import MDBListContentService
from filmu_py.workers import tasks


def test_mdblist_initialize_with_disabled_config_warns_and_poll_returns_empty() -> None:
    plugin = MDBListContentService()
    harness = TestPluginContext(settings={"content": {"mdblist": {"enabled": False}}})

    asyncio.run(plugin.initialize(harness.build("mdblist")))
    result = asyncio.run(plugin.poll())

    assert result == []
    assert any(entry[1] == "plugin.stub_not_configured" for entry in harness.logger.entries)


def test_mdblist_initialize_with_valid_config_does_not_warn() -> None:
    plugin = MDBListContentService()
    harness = TestPluginContext(
        settings={
            "content": {
                "mdblist": {
                    "enabled": True,
                    "api_key": "mdblist-key",
                    "list_ids": ["list-a"],
                    "poll_interval_minutes": 30,
                }
            }
        }
    )

    asyncio.run(plugin.initialize(harness.build("mdblist")))

    assert not any(entry[1] == "plugin.stub_not_configured" for entry in harness.logger.entries)


def test_mdblist_poll_returns_normalized_content_requests() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://mdblist.com/api/lists/list-a/items/?apikey=mdblist-key"
        return httpx.Response(
            200,
            json=[
                {"tmdb_id": 123, "mediatype": "movie", "title": "Movie One"},
                {"tmdb_id": 456, "mediatype": "tv", "title": "Show One"},
            ],
        )

    plugin = MDBListContentService(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "content": {
                "mdblist": {
                    "enabled": True,
                    "api_key": "mdblist-key",
                    "list_ids": ["list-a"],
                    "poll_interval_minutes": 30,
                }
            }
        }
    )

    asyncio.run(plugin.initialize(harness.build("mdblist")))
    results = asyncio.run(plugin.poll())

    assert results == [
        ContentRequest(
            external_ref="tmdb:123",
            media_type="movie",
            title="Movie One",
            source="mdblist",
            source_list_id="list-a",
        ),
        ContentRequest(
            external_ref="tmdb:456",
            media_type="tv",
            title="Show One",
            source="mdblist",
            source_list_id="list-a",
        ),
    ]
    assert harness.rate_limiter.requests[0][0] == "ratelimit:mdblist:poll"


def test_mdblist_poll_failed_http_call_logs_warning_and_returns_empty() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"detail": "boom"})

    plugin = MDBListContentService(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "content": {
                "mdblist": {
                    "enabled": True,
                    "api_key": "mdblist-key",
                    "list_ids": ["list-a"],
                }
            }
        }
    )

    asyncio.run(plugin.initialize(harness.build("mdblist")))
    results = asyncio.run(plugin.poll())

    assert results == []
    assert any(entry[1] == "plugin.mdblist.poll_failed" for entry in harness.logger.entries)


@dataclass
class _RegistryStub:
    plugins: list[object]

    def get_content_services(self) -> list[object]:
        return list(self.plugins)


@dataclass
class _PluginStub:
    requests: list[ContentRequest]
    plugin_name: str = "mdblist"

    async def poll(self) -> list[ContentRequest]:
        return list(self.requests)


@dataclass
class _MediaServiceStub:
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def request_items_by_identifiers(
        self,
        *,
        media_type: str,
        identifiers: list[str] | None = None,
        tmdb_ids: list[str] | None = None,
        tvdb_ids: list[str] | None = None,
        requested_seasons: list[int] | None = None,
        requested_episodes: dict[str, list[int]] | None = None,
    ) -> object:
        self.calls.append(
            {
                "media_type": media_type,
                "identifiers": identifiers,
                "tmdb_ids": tmdb_ids,
                "tvdb_ids": tvdb_ids,
                "requested_seasons": requested_seasons,
                "requested_episodes": requested_episodes,
            }
        )
        return object()


def test_poll_content_services_worker_fans_out_requests() -> None:
    plugin = _PluginStub(
        requests=[
            ContentRequest(external_ref="tmdb:123", media_type="movie", source="mdblist"),
            ContentRequest(external_ref="tmdb:456", media_type="tv", source="mdblist"),
        ]
    )
    media_service = _MediaServiceStub()

    asyncio.run(
        tasks.poll_content_services(
            {"plugin_registry": _RegistryStub([plugin]), "media_service": media_service}
        )
    )

    assert media_service.calls == [
        {
            "media_type": "movie",
            "identifiers": ["tmdb:123"],
            "tmdb_ids": None,
            "tvdb_ids": None,
            "requested_seasons": None,
            "requested_episodes": None,
        },
        {
            "media_type": "tv",
            "identifiers": ["tmdb:456"],
            "tmdb_ids": None,
            "tvdb_ids": None,
            "requested_seasons": None,
            "requested_episodes": None,
        },
    ]
