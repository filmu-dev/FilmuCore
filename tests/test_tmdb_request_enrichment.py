from __future__ import annotations

from dataclasses import dataclass

import httpx

from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.services.media import MediaItemRecord, MediaService, RequestSearchLocalSignalRecord
from filmu_py.services.tmdb import (
    MovieMetadata,
    ShowMetadata,
    TmdbMetadataClient,
    TmdbSearchPage,
    TmdbSearchResult,
)
from filmu_py.services.tvdb import TvdbClient
from filmu_py.state.item import ItemState


class _FakeLimiter:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def acquire(
        self,
        bucket_key: str,
        capacity: float,
        refill_rate_per_second: float,
        requested_tokens: float = 1.0,
        now_seconds: float | None = None,
        expiry_seconds: int | None = None,
    ) -> object:
        self.calls.append(
            {
                "bucket_key": bucket_key,
                "capacity": capacity,
                "refill_rate_per_second": refill_rate_per_second,
                "requested_tokens": requested_tokens,
                "now_seconds": now_seconds,
                "expiry_seconds": expiry_seconds,
            }
        )

        @dataclass(frozen=True)
        class _Decision:
            allowed: bool = True
            remaining_tokens: float = 1.0
            retry_after_seconds: float = 0.0

        return _Decision()


class _DummyDb:
    pass


class _DummyRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}

    async def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
        _ = ex
        self.values[key] = value

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)


def _build_settings(*, tmdb_api_key: str = "tmdb-token") -> Settings:
    return Settings(
        FILMU_PY_API_KEY="a" * 32,
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL="redis://localhost:6379/0",
        TMDB_API_KEY=tmdb_api_key,
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
    )


def _build_media_service(
    *,
    tmdb_client: TmdbMetadataClient | None = None,
    tvdb_client: TvdbClient | None = None,
    limiter: _FakeLimiter | None = None,
    settings: Settings | None = None,
) -> MediaService:
    return MediaService(
        db=_DummyDb(),  # type: ignore[arg-type]
        event_bus=EventBus(),
        settings=settings,
        rate_limiter=limiter,  # type: ignore[arg-type]
        tmdb_client=tmdb_client,
        tvdb_client=tvdb_client,
    )


def test_request_search_candidates_page_paginates_ranked_results() -> None:
    limiter = _FakeLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/3/search/movie"
        page = int(request.url.params["page"])
        payloads = {
            1: {
                "page": 1,
                "total_pages": 2,
                "total_results": 4,
                "results": [
                    {
                        "id": 603,
                        "title": "The Matrix",
                        "release_date": "1999-03-30",
                        "overview": "Wake up, Neo.",
                        "poster_path": "/matrix.jpg",
                        "popularity": 90.0,
                    },
                    {
                        "id": 604,
                        "title": "The Matrix Reloaded",
                        "release_date": "2003-05-15",
                        "overview": "Reloaded.",
                        "poster_path": "/matrix-reloaded.jpg",
                        "popularity": 82.0,
                    },
                ],
            },
            2: {
                "page": 2,
                "total_pages": 2,
                "total_results": 4,
                "results": [
                    {
                        "id": 605,
                        "title": "The Matrix Revolutions",
                        "release_date": "2003-11-05",
                        "overview": "Revolutions.",
                        "poster_path": "/matrix-revolutions.jpg",
                        "popularity": 70.0,
                    },
                    {
                        "id": 624860,
                        "title": "The Matrix Resurrections",
                        "release_date": "2021-12-16",
                        "overview": "Resurrections.",
                        "poster_path": "/matrix-resurrections.jpg",
                        "popularity": 76.0,
                    },
                ],
            },
        }
        return httpx.Response(200, json=payloads[page])

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=limiter,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )
    service = _build_media_service(tmdb_client=client, limiter=limiter, settings=_build_settings())
    service.get_item_by_external_id = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_latest_item_request = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_workflow_checkpoint = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_recovery_plan = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]

    page = __import__("asyncio").run(
        service.search_request_candidates_page(
            query="matrix",
            media_type="movie",
            limit=2,
            offset=1,
        )
    )

    assert page.offset == 1
    assert page.limit == 2
    assert page.total_count == 4
    assert page.has_previous_page is True
    assert page.has_next_page is True
    assert page.result_window_complete is True
    assert [item.title for item in page.items] == [
        "The Matrix Reloaded",
        "The Matrix Resurrections",
    ]


def test_tmdb_discover_movie_page_normalizes_genres_and_filters() -> None:
    limiter = _FakeLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/3/genre/movie/list":
            return httpx.Response(
                200,
                json={
                    "genres": [
                        {"id": 878, "name": "Science Fiction"},
                        {"id": 28, "name": "Action"},
                    ]
                },
            )
        assert request.url.path == "/3/discover/movie"
        assert request.url.params["page"] == "2"
        assert request.url.params["with_genres"] == "878"
        assert request.url.params["primary_release_year"] == "1999"
        assert request.url.params["with_original_language"] == "en"
        assert request.url.params["with_companies"] == "4"
        assert request.url.params["sort_by"] == "vote_average.desc"
        return httpx.Response(
            200,
            json={
                "page": 2,
                "total_pages": 3,
                "total_results": 45,
                "results": [
                    {
                        "id": 603,
                        "title": "The Matrix",
                        "release_date": "1999-03-30",
                        "overview": "Wake up, Neo.",
                        "poster_path": "/matrix.jpg",
                        "popularity": 88.2,
                        "vote_average": 8.2,
                        "vote_count": 25000,
                        "original_language": "en",
                        "genre_ids": [878, 28],
                    }
                ],
            },
        )

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=limiter,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )

    page = __import__("asyncio").run(
        client.discover_movie_page(
            page=2,
            genre="878",
            release_year=1999,
            original_language="en",
            company="4",
            sort_by="vote_average.desc",
        )
    )

    assert page.page == 2
    assert page.total_pages == 3
    assert page.total_results == 45
    assert page.results[0].genre_names == ["Science Fiction", "Action"]
    assert page.results[0].original_language == "en"
    assert page.results[0].vote_average == 8.2


def test_discover_request_candidates_page_returns_facets_and_page_metadata() -> None:
    limiter = _FakeLimiter()

    class _FakeTmdbClient:
        async def discover_movie_page(  # type: ignore[no-untyped-def]
            self,
            *,
            page: int = 1,
            genre: str | None = None,
            release_year: int | None = None,
            original_language: str | None = None,
            company: str | None = None,
            sort_by: str | None = None,
        ) -> TmdbSearchPage:
            assert genre == "Science Fiction"
            assert release_year == 1999
            assert original_language == "en"
            assert company == "4"
            assert sort_by == "vote_average.desc"
            payloads = {
                1: [
                    TmdbSearchResult.model_validate(
                        {
                            "id": "603",
                            "media_type": "movie",
                            "title": "The Matrix",
                            "year": 1999,
                            "overview": "Wake up, Neo.",
                            "poster_path": "/matrix.jpg",
                            "popularity": 88.2,
                            "vote_average": 8.2,
                            "vote_count": 25000,
                            "original_language": "en",
                            "genre_names": ["Science Fiction", "Action"],
                        }
                    ),
                    TmdbSearchResult.model_validate(
                        {
                            "id": "604",
                            "media_type": "movie",
                            "title": "The Matrix Reloaded",
                            "year": 2003,
                            "overview": "Reloaded.",
                            "poster_path": "/matrix-reloaded.jpg",
                            "popularity": 82.0,
                            "vote_average": 7.0,
                            "vote_count": 15000,
                            "original_language": "en",
                            "genre_names": ["Science Fiction"],
                        }
                    ),
                ],
                2: [
                    TmdbSearchResult.model_validate(
                        {
                            "id": "605",
                            "media_type": "movie",
                            "title": "The Matrix Revolutions",
                            "year": 2003,
                            "overview": "Revolutions.",
                            "poster_path": "/matrix-revolutions.jpg",
                            "popularity": 70.0,
                            "vote_average": 6.7,
                            "vote_count": 12000,
                            "original_language": "en",
                            "genre_names": ["Science Fiction"],
                        }
                    )
                ],
            }
            return TmdbSearchPage(
                results=payloads.get(page, []),
                page=page,
                total_pages=2,
                total_results=3,
            )

        async def discover_show_page(self, **kwargs: object) -> TmdbSearchPage:
            _ = kwargs
            return TmdbSearchPage(results=[], page=1, total_pages=1, total_results=0)

        async def get_movie(self, tmdb_id: str) -> MovieMetadata | None:
            payloads = {
                "603": MovieMetadata.model_validate(
                    {
                        "id": "603",
                        "title": "The Matrix",
                        "year": 1999,
                        "overview": "Wake up, Neo.",
                        "poster_path": "/matrix.jpg",
                        "genres": ["Science Fiction", "Action"],
                        "companies": [
                            {"id": "4", "name": "Paramount Pictures"},
                            {"id": "174", "name": "Warner Bros."},
                        ],
                    }
                ),
                "604": MovieMetadata.model_validate(
                    {
                        "id": "604",
                        "title": "The Matrix Reloaded",
                        "year": 2003,
                        "overview": "Reloaded.",
                        "poster_path": "/matrix-reloaded.jpg",
                        "genres": ["Science Fiction"],
                        "companies": [
                            {"id": "4", "name": "Paramount Pictures"},
                        ],
                    }
                ),
                "605": MovieMetadata.model_validate(
                    {
                        "id": "605",
                        "title": "The Matrix Revolutions",
                        "year": 2003,
                        "overview": "Revolutions.",
                        "poster_path": "/matrix-revolutions.jpg",
                        "genres": ["Science Fiction"],
                        "companies": [
                            {"id": "174", "name": "Warner Bros."},
                        ],
                    }
                ),
            }
            return payloads.get(tmdb_id)

        async def get_show(self, tmdb_id: str) -> ShowMetadata | None:
            _ = tmdb_id
            return None

    service = _build_media_service(
        tmdb_client=_FakeTmdbClient(),  # type: ignore[arg-type]
        limiter=limiter,
        settings=_build_settings(),
    )
    service.get_item_by_external_id = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_latest_item_request = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_workflow_checkpoint = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_recovery_plan = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]

    page = __import__("asyncio").run(
        service.discover_request_candidates_page(
            media_type="movie",
            genre="Science Fiction",
            release_year=1999,
            original_language="en",
            company="4",
            sort="rating",
            limit=2,
            offset=1,
        )
    )

    assert page.offset == 1
    assert page.limit == 2
    assert page.total_count == 3
    assert page.has_previous_page is True
    assert page.has_next_page is False
    assert page.result_window_complete is True
    assert [item.title for item in page.items] == [
        "The Matrix Reloaded",
        "The Matrix Revolutions",
    ]
    assert [(facet.value, facet.count, facet.selected) for facet in page.facets.genres] == [
        ("Science Fiction", 3, True),
        ("Action", 1, False),
    ]
    assert [(facet.value, facet.count, facet.selected) for facet in page.facets.release_years] == [
        ("2003", 2, False),
        ("1999", 1, True),
    ]
    assert [(facet.value, facet.count, facet.selected) for facet in page.facets.languages] == [
        ("en", 3, True)
    ]
    assert [(facet.value, facet.label, facet.count, facet.selected) for facet in page.facets.companies] == [
        ("4", "Paramount Pictures", 2, True),
        ("174", "Warner Bros.", 2, False),
    ]
    assert page.facets.networks == ()
    assert [(facet.value, facet.selected) for facet in page.facets.sorts] == [
        ("popular", False),
        ("newest", False),
        ("oldest", False),
        ("rating", True),
    ]


def test_tmdb_editorial_movie_page_normalizes_trending_payload() -> None:
    limiter = _FakeLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/3/genre/movie/list":
            return httpx.Response(
                200,
                json={"genres": [{"id": 878, "name": "Science Fiction"}]},
            )
        assert request.url.path == "/3/trending/movie/day"
        assert request.url.params["page"] == "2"
        return httpx.Response(
            200,
            json={
                "page": 2,
                "total_pages": 4,
                "total_results": 80,
                "results": [
                    {
                        "id": 603,
                        "title": "The Matrix",
                        "release_date": "1999-03-30",
                        "overview": "Wake up, Neo.",
                        "poster_path": "/matrix.jpg",
                        "popularity": 88.2,
                        "vote_average": 8.2,
                        "vote_count": 25000,
                        "original_language": "en",
                        "genre_ids": [878],
                    }
                ],
            },
        )

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=limiter,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )

    page = __import__("asyncio").run(client.editorial_movie_page(family="trending", page=2))

    assert page.page == 2
    assert page.total_pages == 4
    assert page.total_results == 80
    assert [result.title for result in page.results] == ["The Matrix"]
    assert page.results[0].genre_names == ["Science Fiction"]


def test_discover_request_editorial_families_returns_ranked_windows() -> None:
    limiter = _FakeLimiter()

    class _FakeTmdbClient:
        async def editorial_movie_page(  # type: ignore[no-untyped-def]
            self,
            *,
            family: str,
            page: int = 1,
        ) -> TmdbSearchPage:
            if family != "trending":
                return TmdbSearchPage(results=[], page=page, total_pages=1, total_results=0)
            payloads = {
                1: [
                    TmdbSearchResult.model_validate(
                        {
                            "id": "603",
                            "media_type": "movie",
                            "title": "The Matrix",
                            "year": 1999,
                            "overview": "Wake up, Neo.",
                            "poster_path": "/matrix.jpg",
                            "popularity": 88.2,
                        }
                    ),
                    TmdbSearchResult.model_validate(
                        {
                            "id": "604",
                            "media_type": "movie",
                            "title": "The Matrix Reloaded",
                            "year": 2003,
                            "overview": "Reloaded.",
                            "poster_path": "/matrix-reloaded.jpg",
                            "popularity": 82.0,
                        }
                    ),
                ],
                2: [
                    TmdbSearchResult.model_validate(
                        {
                            "id": "624860",
                            "media_type": "movie",
                            "title": "The Matrix Resurrections",
                            "year": 2021,
                            "overview": "Resurrections.",
                            "poster_path": "/matrix-resurrections.jpg",
                            "popularity": 76.0,
                        }
                    )
                ],
            }
            return TmdbSearchPage(
                results=payloads.get(page, []),
                page=page,
                total_pages=2,
                total_results=3,
            )

        async def editorial_show_page(  # type: ignore[no-untyped-def]
            self,
            *,
            family: str,
            page: int = 1,
        ) -> TmdbSearchPage:
            if family != "returning":
                return TmdbSearchPage(results=[], page=page, total_pages=1, total_results=0)
            payloads = {
                1: [
                    TmdbSearchResult.model_validate(
                        {
                            "id": "1399",
                            "media_type": "show",
                            "title": "Game of Thrones",
                            "year": 2011,
                            "overview": "Winter is coming.",
                            "poster_path": "/got.jpg",
                            "popularity": 90.0,
                        }
                    )
                ]
            }
            return TmdbSearchPage(
                results=payloads.get(page, []),
                page=page,
                total_pages=1,
                total_results=1,
            )

    service = _build_media_service(
        tmdb_client=_FakeTmdbClient(),  # type: ignore[arg-type]
        limiter=limiter,
        settings=_build_settings(),
    )
    service.get_item_by_external_id = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_latest_item_request = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_workflow_checkpoint = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_recovery_plan = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]

    families = __import__("asyncio").run(
        service.discover_request_editorial_families(
            family_ids=["trending-films", "returning-series"],
            limit_per_family=2,
        )
    )

    assert [family.family_id for family in families] == [
        "trending-films",
        "returning-series",
    ]
    assert [item.title for item in families[0].items] == [
        "The Matrix",
        "The Matrix Reloaded",
    ]
    assert [item.title for item in families[1].items] == ["Game of Thrones"]


def test_tmdb_release_window_movie_page_normalizes_temporal_filters() -> None:
    limiter = _FakeLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/3/genre/movie/list":
            return httpx.Response(
                200,
                json={"genres": [{"id": 28, "name": "Action"}]},
            )
        assert request.url.path == "/3/discover/movie"
        assert request.url.params["page"] == "2"
        assert request.url.params["sort_by"] == "primary_release_date.asc"
        assert request.url.params["with_release_type"] == "2|3"
        assert request.url.params["primary_release_date.gte"] == "2026-04-05"
        assert request.url.params["primary_release_date.lte"] == "2026-06-03"
        return httpx.Response(
            200,
            json={
                "page": 2,
                "total_pages": 3,
                "total_results": 35,
                "results": [
                    {
                        "id": 603,
                        "title": "The Matrix",
                        "release_date": "2026-05-01",
                        "overview": "Wake up, Neo.",
                        "poster_path": "/matrix.jpg",
                        "popularity": 88.2,
                        "vote_average": 8.2,
                        "vote_count": 25000,
                        "original_language": "en",
                        "genre_ids": [28],
                    }
                ],
            },
        )

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=limiter,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )

    page = __import__("asyncio").run(
        client.release_window_movie_page(
            window="theatrical",
            page=2,
            reference_date=__import__("datetime").date(2026, 4, 19),
        )
    )

    assert page.page == 2
    assert page.total_pages == 3
    assert page.total_results == 35
    assert [result.title for result in page.results] == ["The Matrix"]
    assert page.results[0].genre_names == ["Action"]


def test_discover_request_release_windows_returns_temporal_windows() -> None:
    limiter = _FakeLimiter()

    class _FakeTmdbClient:
        async def release_window_movie_page(  # type: ignore[no-untyped-def]
            self,
            *,
            window: str,
            page: int = 1,
            reference_date=None,
        ) -> TmdbSearchPage:
            _ = reference_date
            if window == "theatrical":
                payloads = {
                    1: [
                        TmdbSearchResult.model_validate(
                            {
                                "id": "603",
                                "media_type": "movie",
                                "title": "The Matrix",
                                "year": 1999,
                                "overview": "Wake up, Neo.",
                                "poster_path": "/matrix.jpg",
                                "popularity": 88.2,
                            }
                        ),
                        TmdbSearchResult.model_validate(
                            {
                                "id": "604",
                                "media_type": "movie",
                                "title": "The Matrix Reloaded",
                                "year": 2003,
                                "overview": "Reloaded.",
                                "poster_path": "/matrix-reloaded.jpg",
                                "popularity": 82.0,
                            }
                        ),
                    ]
                }
                return TmdbSearchPage(
                    results=payloads.get(page, []),
                    page=page,
                    total_pages=1,
                    total_results=2,
                )
            if window == "digital":
                return TmdbSearchPage(results=[], page=page, total_pages=1, total_results=0)
            return TmdbSearchPage(results=[], page=page, total_pages=1, total_results=0)

        async def release_window_show_page(  # type: ignore[no-untyped-def]
            self,
            *,
            window: str,
            page: int = 1,
            reference_date=None,
        ) -> TmdbSearchPage:
            _ = reference_date
            if window != "limited-series":
                return TmdbSearchPage(results=[], page=page, total_pages=1, total_results=0)
            payloads = {
                1: [
                    TmdbSearchResult.model_validate(
                        {
                            "id": "1399",
                            "media_type": "show",
                            "title": "Game of Thrones",
                            "year": 2011,
                            "overview": "Winter is coming.",
                            "poster_path": "/got.jpg",
                            "popularity": 90.0,
                        }
                    )
                ]
            }
            return TmdbSearchPage(
                results=payloads.get(page, []),
                page=page,
                total_pages=1,
                total_results=1,
            )

    service = _build_media_service(
        tmdb_client=_FakeTmdbClient(),  # type: ignore[arg-type]
        limiter=limiter,
        settings=_build_settings(),
    )
    service.get_item_by_external_id = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_latest_item_request = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_workflow_checkpoint = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_recovery_plan = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]

    windows = __import__("asyncio").run(
        service.discover_request_release_windows(
            window_ids=["theatrical-films", "limited-series-launches"],
            limit_per_window=2,
        )
    )

    assert [window.window_id for window in windows] == [
        "theatrical-films",
        "limited-series-launches",
    ]
    assert [item.title for item in windows[0].items] == [
        "The Matrix",
        "The Matrix Reloaded",
    ]
    assert [item.title for item in windows[1].items] == ["Game of Thrones"]


def test_request_time_metadata_is_applied_when_tmdb_returns_successfully() -> None:
    limiter = _FakeLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/3/movie/550":
            return httpx.Response(
                200,
                json={
                    "id": 550,
                    "title": "Fight Club",
                    "release_date": "1999-10-15",
                    "overview": "An insomniac office worker crosses paths with a devil-may-care soap maker.",
                    "poster_path": "/poster.jpg",
                    "genres": [{"id": 18, "name": "Drama"}],
                    "alternative_titles": {
                        "titles": [
                            {"title": "Clube da Luta"},
                            {"title": "Fight Club"},
                            {"title": "Clube da Luta"},
                        ]
                    },
                    "status": "Released",
                },
            )
        assert request.url.path == "/3/movie/550/external_ids"
        return httpx.Response(200, json={"imdb_id": "tt0137523", "tvdb_id": None})

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=limiter,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )
    service = _build_media_service(tmdb_client=client, limiter=limiter, settings=_build_settings())

    enriched = __import__("asyncio").run(
        service._fetch_request_metadata(media_type="movie", identifier="550")
    )

    assert enriched.metadata is not None
    assert enriched.metadata.title == "Fight Club"
    assert enriched.metadata.attributes["poster_path"] == "/poster.jpg"
    assert enriched.metadata.attributes["year"] == 1999
    assert enriched.metadata.attributes["genres"] == ["Drama"]
    assert enriched.metadata.attributes["aliases"] == ["Clube da Luta"]
    assert enriched.metadata.attributes["imdb_id"] == "tt0137523"
    assert enriched.enrichment.source == "tmdb"


def test_request_time_metadata_returns_none_when_tmdb_key_missing() -> None:
    limiter = _FakeLimiter()
    service = _build_media_service(limiter=limiter, settings=_build_settings(tmdb_api_key=""))

    enriched = __import__("asyncio").run(
        service._fetch_request_metadata(media_type="movie", identifier="550")
    )

    assert enriched.metadata is None
    assert limiter.calls == []


def test_request_time_metadata_returns_none_when_tmdb_returns_non_200() -> None:
    limiter = _FakeLimiter()

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"status_message": "boom"})

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=limiter,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )
    service = _build_media_service(tmdb_client=client, limiter=limiter, settings=_build_settings())

    enriched = __import__("asyncio").run(
        service._fetch_request_metadata(media_type="movie", identifier="550")
    )

    assert enriched.metadata is None


def test_tmdb_metadata_fetch_invokes_rate_limiter_on_each_request() -> None:
    limiter = _FakeLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/3/movie/550":
            return httpx.Response(
                200,
                json={
                    "id": 550,
                    "title": "Fight Club",
                    "release_date": "1999-10-15",
                    "overview": "Movie overview",
                    "poster_path": "/movie.jpg",
                    "genres": [],
                    "status": "Released",
                },
            )
        return httpx.Response(
            200,
            json={
                "id": 1399,
                "name": "Game of Thrones",
                "first_air_date": "2011-04-17",
                "overview": "Show overview",
                "poster_path": "/show.jpg",
                "genres": [],
                "status": "Ended",
                "seasons": [
                    {"season_number": 1, "episode_count": 10},
                    {"season_number": 2, "episode_count": 10},
                ],
                "next_episode_to_air": {"season_number": 2, "episode_number": 5, "air_date": "2011-04-20"},
            },
        )

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=limiter,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )

    __import__("asyncio").run(client.get_movie("550"))
    show = __import__("asyncio").run(client.get_show("1399"))

    assert len(limiter.calls) == 2
    assert all(call["bucket_key"] == "ratelimit:tmdb:metadata" for call in limiter.calls)
    assert show is not None
    assert show.seasons == [
        {"season_number": 1, "episode_count": 10},
        {"season_number": 2, "episode_count": 10},
    ]
    assert show.next_episode_to_air == {
        "season_number": 2,
        "episode_number": 5,
        "air_date": "2011-04-20",
    }


def test_tmdb_search_page_returns_page_metadata() -> None:
    limiter = _FakeLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/3/search/movie"
        assert request.url.params["query"] == "matrix"
        assert request.url.params["page"] == "2"
        return httpx.Response(
            200,
            json={
                "page": 2,
                "total_pages": 4,
                "total_results": 61,
                "results": [
                    {
                        "id": 603,
                        "title": "The Matrix",
                        "release_date": "1999-03-30",
                        "overview": "Wake up, Neo.",
                        "poster_path": "/matrix.jpg",
                        "popularity": 88.2,
                    }
                ],
            },
        )

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=limiter,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )

    page = __import__("asyncio").run(client.search_movie_page("matrix", page=2))

    assert page.page == 2
    assert page.total_pages == 4
    assert page.total_results == 61
    assert [result.title for result in page.results] == ["The Matrix"]


def test_tvdb_request_metadata_falls_back_to_tvdb_then_tmdb_imdb_lookup() -> None:
    limiter = _FakeLimiter()

    def tmdb_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/3/find/424536":
            return httpx.Response(404, json={"status_message": "not found"})
        if request.url.path == "/3/find/tt0903747":
            return httpx.Response(200, json={"tv_results": [{"id": 1396}]})
        if request.url.path == "/3/tv/1396":
            return httpx.Response(
                200,
                json={
                    "id": 1396,
                    "name": "Breaking Bad",
                    "first_air_date": "2008-01-20",
                    "overview": "TMDB overview",
                    "poster_path": "/breaking-bad.jpg",
                    "genres": [],
                    "status": "Ended",
                },
            )
        assert request.url.path == "/3/tv/1396/external_ids"
        return httpx.Response(200, json={"imdb_id": "tt0903747", "tvdb_id": 81189})

    tmdb_client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=limiter,  # type: ignore[arg-type]
        transport=httpx.MockTransport(tmdb_handler),
    )

    def tvdb_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v4/login":
            return httpx.Response(200, json={"data": {"token": "tvdb-token"}})
        assert request.url.path == "/v4/series/424536/extended"
        return httpx.Response(
            200,
            json={
                "data": {
                    "id": 424536,
                    "name": "Sousou no Frieren",
                    "overview": "TVDB overview",
                    "image": "/banners/posters/424536.jpg",
                    "remoteIds": [{"id": "tt0903747", "sourceName": "IMDb"}],
                }
            },
        )

    tvdb_cache = CacheManager(redis=_DummyRedis(), namespace="test-tvdb")  # type: ignore[arg-type]
    tvdb_client = TvdbClient(
        api_key="tvdb-key",
        cache=tvdb_cache,
        rate_limiter=limiter,  # type: ignore[arg-type]
        transport=httpx.MockTransport(tvdb_handler),
    )
    service = _build_media_service(
        tmdb_client=tmdb_client,
        tvdb_client=tvdb_client,
        limiter=limiter,
        settings=_build_settings(),
    )

    enriched = __import__("asyncio").run(
        service._fetch_request_metadata(media_type="tv", identifier="tvdb:424536")
    )

    assert enriched.metadata is not None
    assert enriched.metadata.title == "Breaking Bad"
    assert enriched.metadata.attributes["tmdb_id"] == "1396"
    assert enriched.metadata.attributes["imdb_id"] == "tt0903747"
    assert enriched.metadata.attributes["poster_path"] == "/breaking-bad.jpg"
    assert enriched.enrichment.source == "tmdb_via_tvdb"


def test_request_items_by_identifiers_prefixes_tvdb_and_tmdb_inputs() -> None:
    service = _build_media_service(settings=_build_settings())
    seen_identifiers: list[str] = []
    seen_external_refs: list[str] = []

    async def fake_fetch(*, media_type: str, identifier: str):  # type: ignore[no-untyped-def]
        assert media_type == "tv"
        seen_identifiers.append(identifier)

        class _Metadata:
            def __init__(self) -> None:
                self.title = "Stranger Things"
                self.attributes = {"item_type": "show", "tvdb_id": "305288", "tmdb_id": "66732"}

        class _Resolution:
            def __init__(self) -> None:
                self.metadata = _Metadata()
                self.enrichment = type("Enrichment", (), {"source": "test"})()

        return _Resolution()

    async def fake_request_item(
        external_ref: str,
        title: str | None = None,
        *,
        media_type: str | None = None,
        attributes: dict[str, object] | None = None,
        requested_seasons: list[int] | None = None,
        requested_episodes: dict[str, list[int]] | None = None,
        request_source: str = "api",
        tenant_id: str = "global",
    ) -> object:
        _ = (
            title,
            media_type,
            attributes,
            requested_seasons,
            requested_episodes,
            request_source,
            tenant_id,
        )
        seen_external_refs.append(external_ref)
        return type("ItemRecord", (), {"id": "item-1"})()

    service._fetch_request_metadata = fake_fetch  # type: ignore[method-assign]
    service.request_item = fake_request_item  # type: ignore[method-assign]

    __import__("asyncio").run(
        service.request_items_by_identifiers(media_type="tv", tvdb_ids=["305288"])
    )
    __import__("asyncio").run(
        service.request_items_by_identifiers(media_type="tv", tmdb_ids=["66732"])
    )

    assert seen_identifiers == ["tvdb:305288", "tmdb:66732"]
    assert seen_external_refs == ["tvdb:305288", "tmdb:66732"]


def test_request_search_candidates_page_applies_local_signal_boosts_and_labels() -> None:
    limiter = _FakeLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/3/search/movie"
        page = int(request.url.params["page"])
        payloads = {
            1: {
                "page": 1,
                "total_pages": 2,
                "total_results": 4,
                "results": [
                    {
                        "id": 603,
                        "title": "The Matrix",
                        "release_date": "1999-03-30",
                        "overview": "Wake up, Neo.",
                        "poster_path": "/matrix.jpg",
                        "popularity": 90.0,
                    },
                    {
                        "id": 604,
                        "title": "The Matrix Reloaded",
                        "release_date": "2003-05-15",
                        "overview": "Reloaded.",
                        "poster_path": "/matrix-reloaded.jpg",
                        "popularity": 80.0,
                    },
                ],
            },
            2: {
                "page": 2,
                "total_pages": 2,
                "total_results": 4,
                "results": [
                    {
                        "id": 605,
                        "title": "The Matrix Revolutions",
                        "release_date": "2003-11-05",
                        "overview": "Revolutions.",
                        "poster_path": "/matrix-revolutions.jpg",
                        "popularity": 70.0,
                    },
                    {
                        "id": 624860,
                        "title": "The Matrix Resurrections",
                        "release_date": "2021-12-16",
                        "overview": "Resurrections.",
                        "poster_path": "/matrix-resurrections.jpg",
                        "popularity": 76.0,
                    },
                ],
            },
        }
        return httpx.Response(200, json=payloads[page])

    client = TmdbMetadataClient(
        api_key="tmdb-token",
        rate_limiter=limiter,  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )
    service = _build_media_service(tmdb_client=client, limiter=limiter, settings=_build_settings())
    service.get_item_by_external_id = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_workflow_checkpoint = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]
    service.get_recovery_plan = lambda *args, **kwargs: __import__("asyncio").sleep(0, result=None)  # type: ignore[method-assign]

    async def _fake_local_signals(*args: object, **kwargs: object) -> dict[str, RequestSearchLocalSignalRecord]:
        _ = args, kwargs
        return {
            "movie:624860": RequestSearchLocalSignalRecord(
                item=MediaItemRecord(
                    id="item-624860",
                    external_ref="tmdb:624860",
                    title="The Matrix Resurrections",
                    state=ItemState.REQUESTED,
                ),
                request_count=4,
                session_count=1,
                active_session_count=1,
                ranking_boost=0.31,
                ranking_signals=("Requested 4x", "Resume activity"),
            )
        }

    service._build_request_search_local_signal_map = _fake_local_signals  # type: ignore[method-assign]

    page = __import__("asyncio").run(
        service.search_request_candidates_page(
            query="matrix",
            media_type="movie",
            limit=4,
            offset=0,
        )
    )

    surfaced = {item.title: item for item in page.items}
    assert "The Matrix Resurrections" in surfaced
    assert surfaced["The Matrix Resurrections"].ranking_signals == ("Requested 4x", "Resume activity")
