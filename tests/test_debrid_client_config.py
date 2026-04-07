from __future__ import annotations

import asyncio
from typing import Any

import httpx

from filmu_py.services.debrid import (
    _DEFAULT_HTTPX_LIMITS,
    AllDebridPlaybackClient,
    RealDebridPlaybackClient,
)
from filmu_py.services.playback import PlaybackAttachmentRefreshRequest


def _build_request(*, provider: str) -> PlaybackAttachmentRefreshRequest:
    return PlaybackAttachmentRefreshRequest(
        attachment_id="attachment-1",
        item_id="item-1",
        kind="remote-direct",
        provider=provider,
        provider_download_id="download-1",
        restricted_url="https://api.example.com/restricted",
        unrestricted_url=None,
        local_path=None,
        refresh_state="stale",
    )


def test_realdebrid_client_uses_default_httpx_limits_when_none_are_provided(
    monkeypatch: Any,
) -> None:
    captured: dict[str, object] = {}

    class FakeAsyncClient:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
            _ = (exc_type, exc, tb)
            return False

        async def post(self, _url: str, data: dict[str, str]) -> httpx.Response:
            request = httpx.Request("POST", "https://api.real-debrid.com/rest/1.0/unrestrict/link")
            return httpx.Response(200, json={"download": data["link"]}, request=request)

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

    client = RealDebridPlaybackClient(api_token="token")
    result = asyncio.run(
        client.unrestrict_link(
            "https://api.example.com/restricted",
            request=_build_request(provider="realdebrid"),
        )
    )

    assert result is not None
    assert captured["limits"] is _DEFAULT_HTTPX_LIMITS


def test_alldebrid_client_uses_custom_httpx_limits_when_provided(monkeypatch: Any) -> None:
    captured: dict[str, object] = {}
    custom_limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)

    class FakeAsyncClient:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
            _ = (exc_type, exc, tb)
            return False

        async def get(self, _url: str, params: dict[str, str]) -> httpx.Response:
            request = httpx.Request("GET", "https://api.alldebrid.com/v4/link/unlock")
            return httpx.Response(
                200, json={"status": "success", "data": {"link": params["link"]}}, request=request
            )

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

    client = AllDebridPlaybackClient(api_token="token", limits=custom_limits)
    result = asyncio.run(
        client.unrestrict_link(
            "https://api.example.com/restricted",
            request=_build_request(provider="alldebrid"),
        )
    )

    assert result is not None
    assert captured["limits"] is custom_limits
