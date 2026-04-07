from __future__ import annotations

from dataclasses import dataclass

import httpx

from filmu_py.config import Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.services.media import MediaService
from filmu_py.services.tmdb import TmdbMetadataClient
from filmu_py.services.tvdb import TvdbClient


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
