from __future__ import annotations

import asyncio

import httpx
import pytest

from filmu_py.plugins import (
    DownloadLinksInput,
    DownloadStatusInput,
    MagnetAddInput,
    TestPluginContext,
)
from filmu_py.plugins.builtin.stremthru import StremThruDownloader
from filmu_py.plugins.interfaces import DownloadStatus


def test_initialize_no_token_emits_debug_not_warning() -> None:
    plugin = StremThruDownloader()
    harness = TestPluginContext(settings={"downloaders": {"stremthru": {"enabled": True}}})

    asyncio.run(plugin.initialize(harness.build("stremthru")))

    assert any(entry[0] == "debug" and entry[1] == "plugin.stub_not_configured" for entry in harness.logger.entries)
    assert not any(entry[0] == "warning" and entry[1] == "plugin.stub_not_configured" for entry in harness.logger.entries)


def test_add_magnet_returns_id() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert str(request.url) == "https://stremthru.example/v0/store/magnets"
        assert request.headers["X-StremThru-Store-Authorization"] == "Basic token-123"
        assert request.read().decode("utf-8") == '{"magnet":"magnet:?xt=urn:btih:abc"}'
        return httpx.Response(200, json={"data": {"id": "abc123"}})

    plugin = StremThruDownloader(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "downloaders": {
                "stremthru": {"enabled": True, "url": "https://stremthru.example", "token": "token-123"}
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("stremthru")))

    result = asyncio.run(plugin.add_magnet(MagnetAddInput(magnet_url="magnet:?xt=urn:btih:abc")))

    assert result.download_id == "abc123"
    assert harness.rate_limiter.requests[0][0] == "ratelimit:stremthru:download"


def test_get_status_maps_ready() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "status": "ready",
                    "files": [{"id": "file-1", "name": "movie.mkv", "size": 123, "link": "https://cdn.example/movie"}],
                }
            },
        )

    plugin = StremThruDownloader(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "downloaders": {
                "stremthru": {"enabled": True, "url": "https://stremthru.example", "token": "token-123"}
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("stremthru")))

    result = asyncio.run(plugin.get_status(DownloadStatusInput(download_id="abc123")))

    assert result.status == DownloadStatus.READY.value
    assert result.files[0].file_id == "file-1"
    assert result.files[0].download_url == "https://cdn.example/movie"


def test_get_status_maps_unknown() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"status": "weird", "files": []}})

    plugin = StremThruDownloader(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "downloaders": {
                "stremthru": {"enabled": True, "url": "https://stremthru.example", "token": "token-123"}
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("stremthru")))

    result = asyncio.run(plugin.get_status(DownloadStatusInput(download_id="abc123")))

    assert result.status == DownloadStatus.UNKNOWN.value


def test_get_download_links_returns_links() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "files": [
                        {"id": "file-1", "name": "movie.mkv", "size": 123, "link": "https://cdn.example/movie"},
                        {"id": "file-2", "name": "extra.srt", "size": 10, "link": "https://cdn.example/subtitle"},
                    ]
                }
            },
        )

    plugin = StremThruDownloader(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "downloaders": {
                "stremthru": {"enabled": True, "url": "https://stremthru.example", "token": "token-123"}
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("stremthru")))

    result = asyncio.run(plugin.get_download_links(DownloadLinksInput(download_id="abc123")))

    assert [link.url for link in result] == [
        "https://cdn.example/movie",
        "https://cdn.example/subtitle",
    ]


def test_get_download_links_filters_requested_file_ids() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "files": [
                        {"id": "file-1", "name": "movie.mkv", "size": 123, "link": "https://cdn.example/movie"},
                        {"id": "file-2", "name": "extra.srt", "size": 10, "link": "https://cdn.example/subtitle"},
                    ]
                }
            },
        )

    plugin = StremThruDownloader(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "downloaders": {
                "stremthru": {
                    "enabled": True,
                    "url": "https://stremthru.example",
                    "token": "token-123",
                }
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("stremthru")))

    result = asyncio.run(
        plugin.get_download_links(
            DownloadLinksInput(download_id="abc123", file_ids=("file-2",))
        )
    )

    assert [link.url for link in result] == ["https://cdn.example/subtitle"]


def test_get_download_links_skips_missing_links() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"files": [{"id": "file-1", "name": "movie.mkv"}]}})

    plugin = StremThruDownloader(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "downloaders": {
                "stremthru": {"enabled": True, "url": "https://stremthru.example", "token": "token-123"}
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("stremthru")))

    result = asyncio.run(plugin.get_download_links(DownloadLinksInput(download_id="abc123")))

    assert result == []


def test_api_failure_raises() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"detail": "boom"})

    plugin = StremThruDownloader(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "downloaders": {
                "stremthru": {"enabled": True, "url": "https://stremthru.example", "token": "token-123"}
            }
        }
    )
    asyncio.run(plugin.initialize(harness.build("stremthru")))

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(plugin.get_status(DownloadStatusInput(download_id="abc123")))
