from __future__ import annotations

import asyncio
from urllib.parse import parse_qs, urlparse

import httpx

from filmu_py.plugins import (
    COMET_PLUGIN_NAME,
    LISTRR_PLUGIN_NAME,
    MDBLIST_PLUGIN_NAME,
    NOTIFICATIONS_PLUGIN_NAME,
    PLEX_PLUGIN_NAME,
    SEERR_PLUGIN_NAME,
    STREAM_CONTROL_PLUGIN_NAME,
    STREMTHRU_PLUGIN_NAME,
    DownloadStatusInput,
    ExternalIdentifiers,
    NotificationEvent,
    PluginRegistry,
    ScraperSearchInput,
    TestPluginContext,
)
from filmu_py.plugins.builtin.comet import CometScraper
from filmu_py.plugins.builtin.prowlarr import PROWLARR_PLUGIN_NAME, ProwlarrScraper
from filmu_py.plugins.builtin.rarbg import RARBG_PLUGIN_NAME, RarbgScraper
from filmu_py.plugins.builtin.torrentio import TORRENTIO_PLUGIN_NAME
from filmu_py.plugins.builtins import register_builtin_plugins


def test_register_builtin_plugins_registers_all_builtin_scrapers() -> None:
    registry = PluginRegistry()
    harness = TestPluginContext(
        settings={
            "scraping": {
                "torrentio": {"enabled": True},
                "prowlarr": {"enabled": True},
                "comet": {"enabled": True},
                "rarbg": {"enabled": True},
            }
        }
    )

    registered = register_builtin_plugins(registry, context_provider=harness.provider())

    assert registered == (
        TORRENTIO_PLUGIN_NAME,
        PROWLARR_PLUGIN_NAME,
        COMET_PLUGIN_NAME,
        RARBG_PLUGIN_NAME,
        MDBLIST_PLUGIN_NAME,
        SEERR_PLUGIN_NAME,
        LISTRR_PLUGIN_NAME,
        STREMTHRU_PLUGIN_NAME,
        STREAM_CONTROL_PLUGIN_NAME,
        NOTIFICATIONS_PLUGIN_NAME,
        PLEX_PLUGIN_NAME,
    )
    assert [plugin.__class__.__name__ for plugin in registry.get_scrapers()] == [
        "TorrentioScraper",
        "ProwlarrScraper",
        "CometScraper",
        "RarbgScraper",
    ]
    assert [plugin.__class__.__name__ for plugin in registry.get_content_services()] == [
        "MDBListContentService",
        "SeerrContentService",
        "ListrrContentService",
    ]
    assert [plugin.__class__.__name__ for plugin in registry.get_downloaders()] == [
        "StremThruDownloader"
    ]
    assert [plugin.__class__.__name__ for plugin in registry.get_notifications()] == [
        "WebhookNotificationPlugin"
    ]
    assert [plugin.__class__.__name__ for plugin in registry.get_stream_controls()] == [
        "HostStreamControlPlugin"
    ]
    assert any(plugin.__class__.__name__ == "PlexLibraryRefreshPlugin" for plugin in registry.get_event_hooks())


def test_builtin_stub_plugins_warn_when_not_configured() -> None:
    registry = PluginRegistry()
    harness = TestPluginContext(settings={"plugins": {}})

    register_builtin_plugins(registry, context_provider=harness.provider())

    mdblist = registry.get_content_services()[0]
    stremthru = registry.get_downloaders()[0]
    notifications = registry.get_notifications()[0]

    asyncio.run(mdblist.poll())
    asyncio.run(stremthru.get_status(DownloadStatusInput(download_id="download-1")))
    asyncio.run(
        notifications.send(
            NotificationEvent(event_type="item.completed", title="Done", message="done")
        )
    )

    assert any(entry[1] == "plugin.stub_not_configured" for entry in harness.logger.entries)


def test_prowlarr_scraper_parses_search_results() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        parsed = urlparse(str(request.url))
        params = parse_qs(parsed.query)
        assert parsed.scheme == "https"
        assert parsed.netloc == "prowlarr.example"
        assert parsed.path == "/api/v1/search"
        assert params == {
            "query": ["The Matrix"],
            "limit": ["50"],
            "categories": ["2000"],
            "type": ["movie"],
            "imdbId": ["tt0133093"],
        }
        assert request.headers["X-Api-Key"] == "prowlarr-key"
        return httpx.Response(
            200,
            json=[
                {
                    "title": "The.Matrix.1999.2160p.WEB-DL",
                    "infoHash": "ABCDEF0123456789ABCDEF0123456789ABCDEF01",
                    "magnetUrl": "magnet:?xt=urn:btih:ABCDEF0123456789ABCDEF0123456789ABCDEF01",
                    "downloadUrl": "https://prowlarr.example/download/1",
                    "size": 2147483648,
                    "seeders": 150,
                    "leechers": 12,
                    "indexer": "BitSearch",
                    "infoUrl": "https://bitsearch.to/torrent/1",
                }
            ],
        )

    scraper = ProwlarrScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "scraping": {
                "prowlarr": {
                    "enabled": True,
                    "url": "https://prowlarr.example",
                    "api_key": "prowlarr-key",
                }
            }
        }
    )
    asyncio.run(scraper.initialize(harness.build("prowlarr")))

    results = asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="The Matrix",
                query="The Matrix",
                item_type="movie",
                external_ids=ExternalIdentifiers(imdb_id="tt0133093"),
            )
        )
    )

    assert harness.rate_limiter.requests[0][0] == "prowlarr:search"
    assert len(results) == 1
    assert results[0].provider == "prowlarr"
    assert results[0].info_hash == "abcdef0123456789abcdef0123456789abcdef01"
    assert results[0].size_bytes == 2147483648
    assert results[0].seeders == 150
    assert results[0].metadata == {"indexer": "BitSearch", "info_url": "https://bitsearch.to/torrent/1"}


def test_rarbg_scraper_parses_search_and_detail_pages() -> None:
    search_html = '<a href="/post-detail/abc/the-matrix-1999/">The Matrix</a>'
    detail_html = """
        <title>Download The Matrix 1999 1080p Free Torrent from The RarBg</title>
        <a href="magnet:?xt=urn:btih:ABCDEF0123456789ABCDEF0123456789ABCDEF01&amp;dn=The+Matrix+1999+1080p"></a>
        <a href="https://itorrents.org/torrent/ABCDEF0123456789ABCDEF0123456789ABCDEF01.torrent"></a>
        Seeders: 27, Leechers: 29
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == "https://therarbg.to/search?search=tt0133093":
            return httpx.Response(200, text=search_html)
        if str(request.url) == "https://therarbg.to/post-detail/abc/the-matrix-1999/":
            return httpx.Response(200, text=detail_html)
        raise AssertionError(f"unexpected url {request.url}")

    scraper = RarbgScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(settings={"scraping": {"rarbg": {"enabled": True}}})
    asyncio.run(scraper.initialize(harness.build("rarbg")))

    results = asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="The Matrix",
                item_type="movie",
                external_ids=ExternalIdentifiers(imdb_id="tt0133093"),
            )
        )
    )

    assert harness.rate_limiter.requests[0][0] == "rarbg:search"
    assert len(results) == 1
    assert results[0].provider == "rarbg"
    assert results[0].title == "The Matrix 1999 1080p"
    assert results[0].info_hash == "abcdef0123456789abcdef0123456789abcdef01"
    assert results[0].seeders == 27
    assert results[0].leechers == 29


def test_comet_scraper_parses_stremio_stream_results() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://comet.example/stream/movie/tt0133093.json"
        return httpx.Response(
            200,
            json={
                "streams": [
                    {
                        "title": "The Matrix 1999 2160p\nSource: Comet",
                        "magnetUrl": "magnet:?xt=urn:btih:ABCDEF0123456789ABCDEF0123456789ABCDEF01",
                    }
                ]
            },
        )

    scraper = CometScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={"scraping": {"comet": {"enabled": True, "url": "https://comet.example"}}}
    )
    asyncio.run(scraper.initialize(harness.build("comet")))

    results = asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="The Matrix",
                item_type="movie",
                external_ids=ExternalIdentifiers(imdb_id="tt0133093"),
            )
        )
    )

    assert harness.rate_limiter.requests[0][0] == "comet:search"
    assert results == [
        results[0]
    ]
    assert results[0].provider == "comet"
    assert results[0].info_hash == "abcdef0123456789abcdef0123456789abcdef01"
    assert results[0].title == "The Matrix 1999 2160p"
