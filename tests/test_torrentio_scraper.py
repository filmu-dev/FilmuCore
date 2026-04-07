from __future__ import annotations

import asyncio

import httpx

from filmu_py.plugins import (
    MDBLIST_PLUGIN_NAME,
    NOTIFICATIONS_PLUGIN_NAME,
    STREMTHRU_PLUGIN_NAME,
    ExternalIdentifiers,
    PluginRegistry,
    ScraperSearchInput,
    TestPluginContext,
)
from filmu_py.plugins.builtin.prowlarr import PROWLARR_PLUGIN_NAME
from filmu_py.plugins.builtin.rarbg import RARBG_PLUGIN_NAME
from filmu_py.plugins.builtin.torrentio import TORRENTIO_PLUGIN_NAME, TorrentioScraper
from filmu_py.plugins.builtins import register_builtin_plugins


def test_register_builtin_plugins_registers_torrentio_scraper() -> None:
    registry = PluginRegistry()
    harness = TestPluginContext(settings={"scraping": {"torrentio": {"enabled": True}}})

    registered = register_builtin_plugins(registry, context_provider=harness.provider())

    assert registered == (
        TORRENTIO_PLUGIN_NAME,
        PROWLARR_PLUGIN_NAME,
        RARBG_PLUGIN_NAME,
        MDBLIST_PLUGIN_NAME,
        STREMTHRU_PLUGIN_NAME,
        NOTIFICATIONS_PLUGIN_NAME,
    )
    assert [plugin.__class__.__name__ for plugin in registry.get_scrapers()] == [
        "TorrentioScraper",
        "ProwlarrScraper",
        "RarbgScraper",
    ]


def test_torrentio_scraper_parses_streams_and_rate_limits_before_request() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert (
            str(request.url)
            == "https://torrentio.strem.fun/stream/movie/tt1234567.json?sort=seeders"
        )
        return httpx.Response(
            200,
            json={
                "streams": [
                    {
                        "title": "Movie 1080p\n⚙️ tracker info",
                        "infoHash": "ABCDEF0123456789ABCDEF0123456789ABCDEF01",
                    }
                ]
            },
        )

    transport = httpx.MockTransport(handler)
    scraper = TorrentioScraper(transport=transport)
    harness = TestPluginContext(
        settings={
            "scraping": {
                "torrentio": {
                    "enabled": True,
                    "url": "https://torrentio.strem.fun",
                    "filter": "sort=seeders",
                    "timeout": 5,
                }
            }
        }
    )
    asyncio.run(scraper.initialize(harness.build("torrentio")))

    results = asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="Movie",
                item_type="movie",
                external_ids=ExternalIdentifiers(imdb_id="tt1234567"),
            )
        )
    )

    assert harness.rate_limiter.requests[0][0] == "torrentio:search"
    assert len(results) == 1
    assert results[0].title == "Movie 1080p"
    assert results[0].info_hash == "abcdef0123456789abcdef0123456789abcdef01"
    assert results[0].magnet_url == "magnet:?xt=urn:btih:abcdef0123456789abcdef0123456789abcdef01"
    assert results[0].provider == "torrentio"


def test_torrentio_scraper_returns_empty_results_cleanly() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"streams": []})

    scraper = TorrentioScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(settings={"scraping": {"torrentio": {"enabled": True}}})
    asyncio.run(scraper.initialize(harness.build("torrentio")))

    results = asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="Movie",
                item_type="movie",
                external_ids=ExternalIdentifiers(imdb_id="tt1234567"),
            )
        )
    )

    assert results == []
    assert harness.rate_limiter.requests[0][0] == "torrentio:search"


def test_torrentio_scraper_handles_transport_errors_gracefully() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    scraper = TorrentioScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(settings={"scraping": {"torrentio": {"enabled": True}}})
    asyncio.run(scraper.initialize(harness.build("torrentio")))

    results = asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="Movie",
                item_type="movie",
                external_ids=ExternalIdentifiers(imdb_id="tt1234567"),
            )
        )
    )

    assert results == []
    assert harness.rate_limiter.requests[0][0] == "torrentio:search"
    assert harness.logger.entries[-1][0] == "warning"


def test_torrentio_scraper_is_disabled_when_unconfigured() -> None:
    scraper = TorrentioScraper(transport=httpx.MockTransport(lambda request: httpx.Response(500)))
    harness = TestPluginContext(settings={})
    asyncio.run(scraper.initialize(harness.build("torrentio")))

    results = asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="Movie",
                item_type="movie",
                external_ids=ExternalIdentifiers(imdb_id="tt1234567"),
            )
        )
    )

    assert scraper.enabled is False
    assert results == []
    assert harness.rate_limiter.requests == []


def test_torrentio_scraper_upgrades_official_http_url_to_https() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url).startswith("https://torrentio.strem.fun/")
        return httpx.Response(200, json={"streams": []})

    scraper = TorrentioScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={
            "scraping": {
                "torrentio": {
                    "enabled": True,
                    "url": "http://torrentio.strem.fun",
                }
            }
        }
    )
    asyncio.run(scraper.initialize(harness.build("torrentio")))

    results = asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="Movie",
                item_type="movie",
                external_ids=ExternalIdentifiers(imdb_id="tt1234567"),
            )
        )
    )

    assert scraper.base_url == "https://torrentio.strem.fun"
    assert results == []
