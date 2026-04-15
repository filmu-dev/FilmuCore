from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from filmu_py.config import DownloadersSettings, Settings
from filmu_py.core.rate_limiter import RateLimitDecision
from filmu_py.services.debrid import (
    AllDebridPlaybackClient,
    DebridLinkPlaybackClient,
    DebridRateLimitError,
    RealDebridPlaybackClient,
    TorrentFile,
    filter_torrent_files,
)


class FakeLimiter:
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
    ) -> RateLimitDecision:
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
        return RateLimitDecision(allowed=True, remaining_tokens=9.0, retry_after_seconds=0.0)


class RateLimitedLimiter:
    async def acquire(
        self,
        bucket_key: str,
        capacity: float,
        refill_rate_per_second: float,
        requested_tokens: float = 1.0,
        now_seconds: float | None = None,
        expiry_seconds: int | None = None,
    ) -> RateLimitDecision:
        _ = (
            bucket_key,
            capacity,
            refill_rate_per_second,
            requested_tokens,
            now_seconds,
            expiry_seconds,
        )
        return RateLimitDecision(allowed=False, remaining_tokens=0.0, retry_after_seconds=7.5)


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY="a" * 32,
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL="redis://localhost:6379/0",
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_DOWNLOADERS={
            "video_extensions": ["mp4", "mkv", "avi"],
            "movie_filesize_mb_min": 700,
            "movie_filesize_mb_max": -1,
            "episode_filesize_mb_min": 100,
            "episode_filesize_mb_max": -1,
            "proxy_url": "",
            "real_debrid": {"enabled": True, "api_key": "rd-key"},
            "debrid_link": {"enabled": False, "api_key": ""},
            "all_debrid": {"enabled": False, "api_key": ""},
        },
    )


def test_downloaders_settings_round_trip_from_production_block() -> None:
    settings = DownloadersSettings.model_validate(
        {
            "video_extensions": ["mp4", "mkv", "avi"],
            "movie_filesize_mb_min": 700,
            "movie_filesize_mb_max": -1,
            "episode_filesize_mb_min": 100,
            "episode_filesize_mb_max": -1,
            "proxy_url": "",
            "real_debrid": {"enabled": True, "api_key": "rd-key"},
            "debrid_link": {"enabled": False, "api_key": "dl-key"},
            "all_debrid": {"enabled": False, "api_key": "ad-key"},
        }
    )

    assert settings.model_dump(mode="python") == {
        "video_extensions": ["mp4", "mkv", "avi"],
        "movie_filesize_mb_min": 700,
        "movie_filesize_mb_max": -1,
        "episode_filesize_mb_min": 100,
        "episode_filesize_mb_max": -1,
        "proxy_url": "",
        "real_debrid": {"enabled": True, "api_key": "rd-key"},
        "debrid_link": {"enabled": False, "api_key": "dl-key"},
        "all_debrid": {"enabled": False, "api_key": "ad-key"},
        "stremthru": {"enabled": False, "url": "https://stremthru.com", "token": ""},
    }


def test_filter_torrent_files_rejects_wrong_extensions_and_small_sizes() -> None:
    settings = DownloadersSettings(
        movie_filesize_mb_min=700,
        movie_filesize_mb_max=-1,
        episode_filesize_mb_min=100,
        episode_filesize_mb_max=-1,
        video_extensions=["mp4", "mkv", "avi"],
    )
    files = [
        TorrentFile(
            file_id="1",
            file_name="Movie.mkv",
            file_size_bytes=800 * 1024 * 1024,
            media_type="movie",
        ),
        TorrentFile(
            file_id="2",
            file_name="Movie.txt",
            file_size_bytes=900 * 1024 * 1024,
            media_type="movie",
        ),
        TorrentFile(
            file_id="3",
            file_name="Episode.S01E01.mkv",
            file_size_bytes=50 * 1024 * 1024,
            media_type="episode",
        ),
    ]

    filtered = filter_torrent_files(files, settings)

    assert [file.file_id for file in filtered] == ["1"]


def test_filter_torrent_files_minus_one_max_means_no_upper_limit() -> None:
    settings = DownloadersSettings(
        movie_filesize_mb_min=700,
        movie_filesize_mb_max=-1,
        episode_filesize_mb_min=100,
        episode_filesize_mb_max=-1,
        video_extensions=["mkv"],
    )
    files = [
        TorrentFile(
            file_id="1",
            file_name="Huge.Movie.mkv",
            file_size_bytes=20_000 * 1024 * 1024,
            media_type="movie",
        )
    ]

    filtered = filter_torrent_files(files, settings)

    assert [file.file_id for file in filtered] == ["1"]


def test_realdebrid_add_magnet_returns_provider_torrent_id_and_rate_limits(
    monkeypatch: Any,
) -> None:
    limiter = FakeLimiter()

    class FakeAsyncClient:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
            _ = (exc_type, exc, tb)
            return False

        async def post(self, url: str, data: dict[str, str]) -> httpx.Response:
            request = httpx.Request("POST", f"https://api.real-debrid.com/rest/1.0{url}")
            assert data["magnet"] == "magnet:?xt=urn:btih:abc"
            return httpx.Response(200, json={"id": "rd-torrent-1"}, request=request)

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

    client = RealDebridPlaybackClient(api_token="rd-token", limiter=limiter)
    torrent_id = asyncio.run(client.add_magnet("magnet:?xt=urn:btih:abc"))

    assert torrent_id == "rd-torrent-1"
    assert limiter.calls[0]["bucket_key"] == "ratelimit:realdebrid:download"


def test_realdebrid_get_torrent_info_maps_provider_response(monkeypatch: Any) -> None:
    limiter = FakeLimiter()

    class FakeAsyncClient:
        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
            _ = (exc_type, exc, tb)
            return False

        def __init__(self, **kwargs: object) -> None:
            _ = kwargs

        async def get(self, url: str) -> httpx.Response:
            request = httpx.Request("GET", f"https://api.real-debrid.com/rest/1.0{url}")
            return httpx.Response(
                200,
                json={
                    "id": "rd-torrent-1",
                    "status": "downloaded",
                    "filename": "Movie Pack",
                    "hash": "abc123",
                    "files": [
                        {"id": 1, "path": "Movie.mkv", "bytes": 800 * 1024 * 1024, "selected": 1},
                        {"id": 2, "path": "Sample.txt", "bytes": 1_024, "selected": 0},
                    ],
                    "links": ["https://cdn.example.com/movie"],
                },
                request=request,
            )

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

    client = RealDebridPlaybackClient(api_token="rd-token", limiter=limiter)
    info = asyncio.run(client.get_torrent_info("rd-torrent-1"))

    assert info.provider_torrent_id == "rd-torrent-1"
    assert info.status == "downloaded"
    assert info.info_hash == "abc123"
    assert info.links == ["https://cdn.example.com/movie"]
    assert info.files[0].file_name == "Movie.mkv"
    assert info.files[0].file_path == "Movie.mkv"
    assert info.files[0].download_url == "https://cdn.example.com/movie"
    assert limiter.calls[0]["bucket_key"] == "ratelimit:realdebrid:download"


def test_realdebrid_add_magnet_raises_distinct_rate_limit_error_when_limiter_blocks() -> None:
    client = RealDebridPlaybackClient(api_token="rd-token", limiter=RateLimitedLimiter())

    with pytest.raises(DebridRateLimitError) as excinfo:
        asyncio.run(client.add_magnet("magnet:?xt=urn:btih:abc"))

    assert excinfo.value.provider == "realdebrid"
    assert excinfo.value.retry_after_seconds == 7.5


def test_alldebrid_download_pipeline_methods_rate_limit(monkeypatch: Any) -> None:
    limiter = FakeLimiter()
    calls: list[tuple[str, str]] = []

    class FakeAsyncClient:
        def __init__(self, **kwargs: object) -> None:
            _ = kwargs

        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
            _ = (exc_type, exc, tb)
            return False

        async def post(self, url: str, data: dict[str, object]) -> httpx.Response:
            calls.append(("POST", url))
            request = httpx.Request("POST", f"https://api.alldebrid.com{url}")
            if url == "/v4/magnet/upload":
                return httpx.Response(
                    200, json={"data": {"magnets": [{"id": 123}]}}, request=request
                )
            if url == "/v4.1/magnet/status":
                return httpx.Response(
                    200,
                    json={
                        "data": {
                            "magnets": [
                                {
                                    "id": 123,
                                    "status": "ready",
                                    "filename": "Movie",
                                    "files": [],
                                    "links": [],
                                }
                            ]
                        }
                    },
                    request=request,
                )
            return httpx.Response(200, json={"status": "success"}, request=request)

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

    client = AllDebridPlaybackClient(api_token="ad-token", limiter=limiter)
    assert asyncio.run(client.add_magnet("magnet:?xt=urn:btih:def")) == "123"
    asyncio.run(client.get_torrent_info("123"))
    asyncio.run(client.select_files("123", ["1", "2"]))

    assert [call["bucket_key"] for call in limiter.calls] == [
        "ratelimit:alldebrid:download",
        "ratelimit:alldebrid:download",
        "ratelimit:alldebrid:download",
    ]


def test_debridlink_download_pipeline_methods_rate_limit(monkeypatch: Any) -> None:
    limiter = FakeLimiter()

    class FakeAsyncClient:
        def __init__(self, **kwargs: object) -> None:
            _ = kwargs

        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
            _ = (exc_type, exc, tb)
            return False

        async def post(self, url: str, json: dict[str, object]) -> httpx.Response:
            request = httpx.Request("POST", f"https://debrid-link.com/api/v2{url}")
            if url == "/seedbox/add":
                return httpx.Response(200, json={"value": {"id": "dl-1"}}, request=request)
            return httpx.Response(200, json={"success": True}, request=request)

        async def get(self, url: str, params: dict[str, str]) -> httpx.Response:
            request = httpx.Request("GET", f"https://debrid-link.com/api/v2{url}")
            return httpx.Response(
                200,
                json={
                    "value": [
                        {
                            "id": "dl-1",
                            "status": "downloaded",
                            "name": "Movie",
                            "hashString": "hash-1",
                            "files": [
                                {
                                    "id": "f1",
                                    "name": "Movie.mkv",
                                    "downloadSize": 800 * 1024 * 1024,
                                    "selected": True,
                                    "downloadUrl": "https://cdn.example.com/dl-movie",
                                }
                            ],
                        }
                    ]
                },
                request=request,
            )

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

    client = DebridLinkPlaybackClient(api_token="dl-token", limiter=limiter)
    assert asyncio.run(client.add_magnet("magnet:?xt=urn:btih:ghi")) == "dl-1"
    info = asyncio.run(client.get_torrent_info("dl-1"))
    asyncio.run(client.select_files("dl-1", ["f1"]))
    links = asyncio.run(client.get_download_links("dl-1"))

    assert info.links == ["https://cdn.example.com/dl-movie"]
    assert links == ["https://cdn.example.com/dl-movie"]
    assert [call["bucket_key"] for call in limiter.calls] == [
        "ratelimit:debridlink:download",
        "ratelimit:debridlink:download",
        "ratelimit:debridlink:download",
        "ratelimit:debridlink:download",
    ]


def test_filter_torrent_files_preserves_nested_provider_path_for_pack_files() -> None:
    settings = DownloadersSettings(
        movie_filesize_mb_min=700,
        movie_filesize_mb_max=-1,
        episode_filesize_mb_min=100,
        episode_filesize_mb_max=-1,
        video_extensions=["mkv"],
    )
    files = [
        TorrentFile(
            file_id="1",
            file_name="Episode 01.mkv",
            file_path="Show/Season 01/Episode 01.mkv",
            file_size_bytes=800 * 1024 * 1024,
            media_type="episode",
        )
    ]

    filtered = filter_torrent_files(files, settings)

    assert filtered[0].file_path == "Show/Season 01/Episode 01.mkv"
