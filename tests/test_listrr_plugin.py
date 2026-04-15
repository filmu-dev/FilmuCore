from __future__ import annotations

import asyncio

import httpx

from filmu_py.plugins import ContentRequest, TestPluginContext
from filmu_py.plugins.builtin.listrr import ListrrContentService


def test_listrr_poll_returns_normalized_content_requests() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://listrr.example/api/lists/list-a/items"
        assert request.headers["Authorization"] == "Bearer listrr-key"
        return httpx.Response(
            200,
            json={
                "items": [
                    {"media": {"mediaType": "movie", "tmdbId": 550, "title": "Fight Club"}},
                    {"media": {"mediaType": "tv", "tvdbId": 121361, "title": "Silo"}},
                ]
            },
        )

    plugin = ListrrContentService(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "content": {
                "listrr": {
                    "enabled": True,
                    "url": "https://listrr.example",
                    "api_key": "listrr-key",
                    "list_ids": ["list-a"],
                }
            }
        }
    )

    asyncio.run(plugin.initialize(harness.build("listrr")))
    results = asyncio.run(plugin.poll())

    assert results == [
        ContentRequest(
            external_ref="tmdb:550",
            media_type="movie",
            title="Fight Club",
            source="listrr",
            source_list_id="list-a",
        ),
        ContentRequest(
            external_ref="tvdb:121361",
            media_type="tv",
            title="Silo",
            source="listrr",
            source_list_id="list-a",
        ),
    ]
    assert harness.rate_limiter.requests[0][0] == "ratelimit:listrr:poll"


def test_listrr_plugin_derives_list_ids_from_compatibility_lists() -> None:
    plugin = ListrrContentService()
    harness = TestPluginContext(
        settings={
            "content": {
                "listrr": {
                    "enabled": True,
                    "url": "https://listrr.example",
                    "movie_lists": ["movies-a"],
                    "show_lists": ["shows-b"],
                }
            }
        }
    )

    asyncio.run(plugin.initialize(harness.build("listrr")))

    assert plugin.enabled is True
    assert plugin.base_url == "https://listrr.example"
    assert plugin.list_ids == ["movies-a", "shows-b"]
