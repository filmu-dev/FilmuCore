import asyncio

import httpx

from filmu_py.plugins import ExternalIdentifiers, ScraperSearchInput, TestPluginContext
from filmu_py.plugins.builtin.prowlarr import ProwlarrScraper


def test_prowlarr_searches_with_movie_category() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert "categories=2000" in str(request.url)
        return httpx.Response(200, json=[])

    scraper = ProwlarrScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={"scraping": {"prowlarr": {"enabled": True, "url": "http://p", "api_key": "k"}}}
    )
    asyncio.run(scraper.initialize(harness.build("prowlarr")))

    asyncio.run(
        scraper.search(
            ScraperSearchInput(title="A", item_type="movie", external_ids=ExternalIdentifiers())
        )
    )


def test_prowlarr_searches_with_show_category() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert "categories=5000" in str(request.url)
        return httpx.Response(200, json=[])

    scraper = ProwlarrScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={"scraping": {"prowlarr": {"enabled": True, "url": "http://p", "api_key": "k"}}}
    )
    asyncio.run(scraper.initialize(harness.build("prowlarr")))

    asyncio.run(
        scraper.search(
            ScraperSearchInput(title="A", item_type="show", external_ids=ExternalIdentifiers())
        )
    )


def test_prowlarr_handles_category_rejection_gracefully() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "Invalid category"})

    scraper = ProwlarrScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={"scraping": {"prowlarr": {"enabled": True, "url": "http://p", "api_key": "k"}}}
    )
    asyncio.run(scraper.initialize(harness.build("prowlarr")))

    results = asyncio.run(
        scraper.search(
            ScraperSearchInput(title="A", item_type="movie", external_ids=ExternalIdentifiers())
        )
    )
    assert results == []

    debug_logs = [entry for entry in harness.logger.entries if entry[0] == "debug"]
    assert len(debug_logs) == 1
    assert debug_logs[0][1] == "plugin.scraper.prowlarr.category_rejected"
