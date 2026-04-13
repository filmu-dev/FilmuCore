from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx

from filmu_py.config import Settings
from filmu_py.plugins import ExternalIdentifiers, ScraperSearchInput, TestPluginContext
from filmu_py.plugins.builtin.prowlarr import ProwlarrScraper
from filmu_py.rtn import ParsedData, RankedTorrent
from filmu_py.services.media import MediaItemRecord
from filmu_py.workers import tasks


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY="a" * 32,
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL="redis://localhost:6379/0",
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
    )


def test_prowlarr_show_episode_uses_tvsearch_with_season_and_episode() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        parsed = urlparse(str(request.url))
        params = parse_qs(parsed.query)
        assert parsed.path == "/api/v1/search"
        assert params["type"] == ["tvsearch"]
        assert params["categories"] == ["5000"]
        assert params["season"] == ["1"]
        assert params["episode"] == ["2"]
        assert params["query"] == ["Show Title"]
        return httpx.Response(200, json=[])

    scraper = ProwlarrScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={"scraping": {"prowlarr": {"enabled": True, "url": "https://prowlarr.example", "api_key": "token"}}}
    )
    asyncio.run(scraper.initialize(harness.build("prowlarr")))
    asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="Show Title",
                query="Show Title S01E02",
                item_type="episode",
                season_number=1,
                episode_number=2,
                external_ids=ExternalIdentifiers(tvdb_id="123"),
            )
        )
    )


def test_prowlarr_show_season_only_uses_tvsearch_with_season_only() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        parsed = urlparse(str(request.url))
        params = parse_qs(parsed.query)
        assert parsed.path == "/api/v1/search"
        assert params["type"] == ["tvsearch"]
        assert params["categories"] == ["5000"]
        assert params["season"] == ["3"]
        assert "episode" not in params
        assert params["query"] == ["Show Title"]
        return httpx.Response(200, json=[])

    scraper = ProwlarrScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={"scraping": {"prowlarr": {"enabled": True, "url": "https://prowlarr.example", "api_key": "token"}}}
    )
    asyncio.run(scraper.initialize(harness.build("prowlarr")))
    asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="Show Title",
                query="Show Title S03",
                item_type="show",
                season_number=3,
                episode_number=None,
                external_ids=ExternalIdentifiers(tvdb_id="123"),
            )
        )
    )


def test_prowlarr_movie_uses_general_search_without_season_episode() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        parsed = urlparse(str(request.url))
        params = parse_qs(parsed.query)
        assert parsed.path == "/api/v1/search"
        assert params["type"] == ["movie"]
        assert params["categories"] == ["2000"]
        assert "season" not in params
        assert "episode" not in params
        assert params["query"] == ["Movie Title"]
        assert params["imdbId"] == ["tt1234567"]
        return httpx.Response(200, json=[])

    scraper = ProwlarrScraper(transport=httpx.MockTransport(handler))
    harness = TestPluginContext(
        settings={"scraping": {"prowlarr": {"enabled": True, "url": "https://prowlarr.example", "api_key": "token"}}}
    )
    asyncio.run(scraper.initialize(harness.build("prowlarr")))
    asyncio.run(
        scraper.search(
            ScraperSearchInput(
                title="Movie Title",
                query="Movie Title",
                item_type="movie",
                external_ids=ExternalIdentifiers(imdb_id="tt1234567"),
            )
        )
    )


def test_build_scraper_search_input_includes_episode_context() -> None:
    item = MediaItemRecord(
        id="episode-1",
        external_ref="tvdb:1",
        title="Show Title",
        state=tasks.ItemState.REQUESTED,
        attributes={
            "item_type": "episode",
            "season_number": 1,
            "episode_number": 2,
            "tvdb_id": "123",
        },
    )

    result = tasks._build_scraper_search_input(item)

    assert result.season_number == 1
    assert result.episode_number == 2
    assert result.query == "Show Title S01E02"


def test_build_scraper_search_input_includes_season_context() -> None:
    item = MediaItemRecord(
        id="season-1",
        external_ref="tvdb:1",
        title="Show Title",
        state=tasks.ItemState.REQUESTED,
        attributes={
            "item_type": "season",
            "season_number": 3,
            "tvdb_id": "123",
        },
    )

    result = tasks._build_scraper_search_input(item)

    assert result.season_number == 3
    assert result.episode_number is None
    assert result.query == "Show Title S03"


@dataclass
class _FakeStream:
    id: str
    raw_title: str
    parsed_title: dict[str, object]
    resolution: str | None = "1080p"


def _ranked(parsed_title: dict[str, object]) -> RankedTorrent:
    return RankedTorrent(
        data=ParsedData(raw_title="Example", parsed_title=parsed_title, resolution="1080p"),
        rank=100,
        lev_ratio=1.0,
        fetch=True,
        failed_checks=[],
        score_parts={},
    )


def test_rank_streams_post_filter_rejects_wrong_season() -> None:
    item = MediaItemRecord(
        id="episode-1",
        external_ref="tvdb:1",
        title="Show Title",
        state=tasks.ItemState.SCRAPED,
        attributes={"item_type": "episode", "season_number": 2, "episode_number": 1},
    )
    wrong = _FakeStream(id="stream-1", raw_title="Show.Title.S01E01", parsed_title={"season": 1, "episode": 1})

    validation = tasks._post_rank_expected_scope_reason(item, wrong)
    assert validation == "season_mismatch"


def test_rank_streams_post_filter_rejects_wrong_episode(monkeypatch: Any) -> None:
    item = MediaItemRecord(
        id="episode-1",
        external_ref="tvdb:1",
        title="Show Title",
        state=tasks.ItemState.SCRAPED,
        attributes={"item_type": "episode", "season_number": 2, "episode_number": 4},
    )
    wrong = _FakeStream(id="stream-1", raw_title="Show.Title.S02E03", parsed_title={"season": 2, "episode": 3})

    validation = tasks._post_rank_expected_scope_reason(item, wrong)
    assert validation == "episode_mismatch"


def test_rank_streams_post_filter_allows_correct_episode() -> None:
    item = MediaItemRecord(
        id="episode-1",
        external_ref="tvdb:1",
        title="Show Title",
        state=tasks.ItemState.SCRAPED,
        attributes={"item_type": "episode", "season_number": 2, "episode_number": 4},
    )
    correct = _FakeStream(id="stream-1", raw_title="Show.Title.S02E04", parsed_title={"season": 2, "episode": 4})

    validation = tasks._post_rank_expected_scope_reason(item, correct)
    assert validation is None


# ---------------------------------------------------------------------------
# Fix 5 — season_override threads partial_seasons into the scrape query
# ---------------------------------------------------------------------------


def test_build_scraper_search_input_with_season_override() -> None:
    """Fix 5: _build_scraper_search_input with season_override produces a season-qualified query.

    Show-level items carry no season_number attribute, so without season_override
    the query degenerates to a bare title.  Passing season_override=1 must
    produce ``'Title S01'`` regardless of the item's attribute dict.
    """
    item = MediaItemRecord(
        id="show-1",
        external_ref="tvdb:1",
        title="Frieren Beyond Journeys End",
        state=tasks.ItemState.REQUESTED,
        attributes={"item_type": "show", "tvdb_id": "123"},
        # Note: deliberately NO season_number attribute — show-level items.
    )

    result = tasks._build_scraper_search_input(item, season_override=1)

    assert result.season_number == 1
    assert result.episode_number is None
    assert result.query == "Frieren Beyond Journeys End S01"


def test_build_scraper_search_input_no_season_override_produces_bare_query() -> None:
    """Without season_override a show-level item produces a bare title query."""
    item = MediaItemRecord(
        id="show-2",
        external_ref="tvdb:2",
        title="One Piece",
        state=tasks.ItemState.REQUESTED,
        attributes={"item_type": "show", "tvdb_id": "456"},
    )

    result = tasks._build_scraper_search_input(item)

    assert result.season_number is None
    assert result.episode_number is None
    assert result.query == "One Piece"


def test_scrape_with_plugins_single_partial_season_uses_season_qualified_query() -> None:
    """Fix 5: _scrape_with_plugins narrows the query when exactly one partial season is requested.

    When partial_seasons=[N], the search query should be ``'Title SNN'`` so that
    targeted season scrapes on show-level items return relevant results instead
    of the entire show catalogue.
    """
    recorded_inputs: list[ScraperSearchInput] = []

    class _FakeScraper:
        async def search(self, search_input: ScraperSearchInput) -> list[object]:
            recorded_inputs.append(search_input)
            return []

    class _FakePluginRegistry:
        def get_scrapers(self) -> list[_FakeScraper]:
            return [_FakeScraper()]

    item = MediaItemRecord(
        id="show-3",
        external_ref="tvdb:3",
        title="Attack on Titan",
        state=tasks.ItemState.REQUESTED,
        attributes={"item_type": "show", "tvdb_id": "789"},
        # No season_number — show-level item.
    )

    asyncio.run(
        tasks._scrape_with_plugins(
            plugin_registry=_FakePluginRegistry(),  # type: ignore[arg-type]
            item=item,
            partial_seasons=[2],
        )
    )

    assert len(recorded_inputs) == 1
    assert recorded_inputs[0].season_number == 2
    assert recorded_inputs[0].query == "Attack on Titan S02"


def test_scrape_with_plugins_multi_partial_season_uses_broad_query() -> None:
    """Fix 6 (multi-season path): fan out one season-qualified query per season.

    For partial_seasons=[1, 3], _scrape_with_plugins should issue two requests
    with ``S01`` and ``S03`` scope rather than one broad title-only query.
    """
    recorded_inputs: list[ScraperSearchInput] = []

    class _FakeScraper:
        async def search(self, search_input: ScraperSearchInput) -> list[object]:
            recorded_inputs.append(search_input)
            return []

    class _FakePluginRegistry:
        def get_scrapers(self) -> list[_FakeScraper]:
            return [_FakeScraper()]

    item = MediaItemRecord(
        id="show-4",
        external_ref="tvdb:4",
        title="Demon Slayer",
        state=tasks.ItemState.REQUESTED,
        attributes={"item_type": "show", "tvdb_id": "321"},
    )

    asyncio.run(
        tasks._scrape_with_plugins(
            plugin_registry=_FakePluginRegistry(),  # type: ignore[arg-type]
            item=item,
            partial_seasons=[1, 3],
        )
    )

    assert len(recorded_inputs) == 2
    seen = {(entry.season_number, entry.query) for entry in recorded_inputs}
    assert seen == {(1, "Demon Slayer S01"), (3, "Demon Slayer S03")}
