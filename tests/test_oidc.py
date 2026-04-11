"""OIDC validation helper regression tests."""

from __future__ import annotations

import pytest

from filmu_py.config import OidcSettings
from filmu_py.security import oidc


class FakeCache:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}

    async def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
        _ = ttl_seconds
        self.values[key] = value


class FakeResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return {"keys": []}


class FakeAsyncClient:
    def __init__(self, *args: object, **kwargs: object) -> None:
        _ = (args, kwargs)

    async def __aenter__(self) -> FakeAsyncClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        _ = args

    async def get(self, url: str) -> FakeResponse:
        _ = url
        return FakeResponse()


@pytest.mark.asyncio
async def test_load_jwks_refetches_after_malformed_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = FakeCache()
    cache.values["oidc:jwks:https://issuer.example/.well-known/jwks.json"] = b"{bad-json"
    monkeypatch.setattr(oidc.httpx, "AsyncClient", FakeAsyncClient)

    jwks = await oidc._load_jwks(
        OidcSettings(
            enabled=True,
            issuer="https://issuer.example",
            audience="filmu",
        ),
        cache,  # type: ignore[arg-type]
    )

    assert jwks == {"keys": []}
    assert cache.values["oidc:jwks:https://issuer.example/.well-known/jwks.json"] == b'{"keys":[]}'
