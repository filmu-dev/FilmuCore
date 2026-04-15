from __future__ import annotations

import asyncio

import httpx

from filmu_py.plugins import ContentRequest, TestPluginContext
from filmu_py.plugins.builtin.seerr import SeerrContentService


def test_seerr_poll_returns_normalized_content_requests() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == (
            "https://seerr.example/api/v1/request?take=25&sort=added&sortDirection=desc"
        )
        assert request.headers["X-Api-Key"] == "seerr-key"
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "status": "pending",
                        "media": {
                            "mediaType": "movie",
                            "tmdbId": 603,
                            "title": "The Matrix",
                        },
                    },
                    {
                        "status": "approved",
                        "media": {
                            "mediaType": "tv",
                            "tmdbId": 1399,
                            "title": "Game of Thrones",
                        },
                    },
                ]
            },
        )

    plugin = SeerrContentService(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "content": {
                "seerr": {
                    "enabled": True,
                    "url": "https://seerr.example",
                    "api_key": "seerr-key",
                    "take": 25,
                    "statuses": ["pending", "approved"],
                }
            }
        }
    )

    asyncio.run(plugin.initialize(harness.build("seerr")))
    results = asyncio.run(plugin.poll())

    assert results == [
        ContentRequest(
            external_ref="tmdb:603",
            media_type="movie",
            title="The Matrix",
            source="seerr",
            source_list_id="pending",
        ),
        ContentRequest(
            external_ref="tmdb:1399",
            media_type="tv",
            title="Game of Thrones",
            source="seerr",
            source_list_id="approved",
        ),
    ]
    assert harness.rate_limiter.requests[0][0] == "ratelimit:seerr:poll"
