from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import AnyUrl, SecretStr

from filmu_py.api.router import create_api_router
from filmu_py.config import DownloadersSettings, Settings
from filmu_py.core.cache import CacheManager
from filmu_py.core.event_bus import EventBus
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.graphql.plugin_registry import GraphQLPluginRegistry
from filmu_py.resources import AppResources
from filmu_py.services.debrid import DownloaderAccountService


class DummyRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}

    def ping(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
        _ = ex
        self.values[key] = value

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)

    async def aclose(self, close_connection_pool: bool | None = None) -> None:
        _ = close_connection_pool
        return None


class DummyDatabaseRuntime:
    async def dispose(self) -> None:
        return None


@dataclass
class DummyMediaService:
    async def get_stats(self) -> Any:
        raise AssertionError("unused in downloader account tests")


def _build_settings() -> Settings:
    return Settings(
        FILMU_PY_API_KEY=SecretStr("a" * 32),
        FILMU_PY_POSTGRES_DSN="postgresql+asyncpg://postgres:postgres@localhost:5432/filmu",
        FILMU_PY_REDIS_URL=AnyUrl("redis://localhost:6379/0"),
        FILMU_PY_RUN_MIGRATIONS_ON_STARTUP=False,
        FILMU_PY_LOG_LEVEL="INFO",
    )


def _build_client(*, settings: Settings | None = None) -> TestClient:
    resolved_settings = settings or _build_settings()
    redis = DummyRedis()
    app = FastAPI()
    app.state.resources = AppResources(
        settings=resolved_settings,
        redis=redis,  # type: ignore[arg-type]
        cache=CacheManager(redis=redis, namespace="test"),  # type: ignore[arg-type]
        rate_limiter=DistributedRateLimiter(redis=redis),  # type: ignore[arg-type]
        event_bus=EventBus(),
        db=DummyDatabaseRuntime(),  # type: ignore[arg-type]
        media_service=DummyMediaService(),  # type: ignore[arg-type]
        graphql_plugin_registry=GraphQLPluginRegistry(),
    )
    app.include_router(create_api_router())
    return TestClient(app)


def _headers() -> dict[str, str]:
    return {"x-api-key": "a" * 32}


def test_get_active_provider_info_real_debrid(monkeypatch: Any) -> None:
    settings = DownloadersSettings(real_debrid={"enabled": True, "api_key": "rd-key"})

    async def fake_user_info(self: Any) -> dict[str, Any]:
        _ = self
        return {
            "username": "rd-user",
            "email": "rd@example.com",
            "premium": 1234,
            "expiration": (datetime.now(tz=UTC) + timedelta(days=30)).isoformat(),
        }

    monkeypatch.setattr("filmu_py.services.debrid.RealDebridPlaybackClient.get_user_info", fake_user_info)

    result = asyncio.run(DownloaderAccountService(settings).get_active_provider_info())

    assert result["provider"] == "real_debrid"
    assert result["username"] == "rd-user"
    assert result["plan"] == "premium"


def test_get_active_provider_info_api_failure(monkeypatch: Any) -> None:
    settings = DownloadersSettings(real_debrid={"enabled": True, "api_key": "rd-key"})

    async def fake_user_info(self: Any) -> dict[str, Any]:
        _ = self
        raise RuntimeError("boom")

    monkeypatch.setattr("filmu_py.services.debrid.RealDebridPlaybackClient.get_user_info", fake_user_info)

    result = asyncio.run(DownloaderAccountService(settings).get_active_provider_info())

    assert result == {"provider": "real_debrid", "error": "boom"}


def test_get_active_provider_info_no_provider() -> None:
    result = asyncio.run(DownloaderAccountService(DownloadersSettings()).get_active_provider_info())

    assert result == {"provider": None, "error": "no provider configured"}


def test_days_remaining_from_iso_future() -> None:
    future = (datetime.now(tz=UTC) + timedelta(days=365)).isoformat()
    assert DownloaderAccountService._days_remaining_from_iso(future) is not None
    assert DownloaderAccountService._days_remaining_from_iso(future) >= 364


def test_days_remaining_from_iso_past() -> None:
    past = (datetime.now(tz=UTC) - timedelta(days=1)).isoformat()
    assert DownloaderAccountService._days_remaining_from_iso(past) == 0


def test_days_remaining_from_iso_none() -> None:
    assert DownloaderAccountService._days_remaining_from_iso(None) is None


def test_days_remaining_from_unix() -> None:
    future = int((datetime.now(tz=UTC) + timedelta(days=30)).timestamp())
    assert DownloaderAccountService._days_remaining_from_unix(future) is not None


def test_downloader_user_info_route(monkeypatch: Any) -> None:
    client = _build_client()

    async def fake_get_active(self: Any) -> dict[str, Any]:
        _ = self
        return {"provider": "real_debrid", "username": "rd-user", "premium_days_remaining": 10, "plan": "premium"}

    monkeypatch.setattr(DownloaderAccountService, "get_active_provider_info", fake_get_active)

    response = client.get("/api/v1/downloader_user_info", headers=_headers())

    assert response.status_code == 200
    assert response.json()["provider"] == "real_debrid"


def test_services_route() -> None:
    settings = _build_settings()
    settings.downloaders.real_debrid.api_key = "rd-key"
    settings.downloaders.all_debrid.api_key = ""
    settings.downloaders.debrid_link.api_key = "dl-key"
    settings.content.mdblist.api_key = "mdblist-key"
    client = _build_client(settings=settings)

    response = client.get("/api/v1/services", headers=_headers())

    assert response.status_code == 200
    assert response.json() == {
        "real_debrid": {"enabled": True},
        "all_debrid": {"enabled": False},
        "debrid_link": {"enabled": True},
        "mdblist": {"enabled": True},
    }
