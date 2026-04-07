"""TVDB metadata client used for fallback request enrichment."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx

from filmu_py.core.cache import CacheManager
from filmu_py.core.rate_limiter import DistributedRateLimiter

logger = logging.getLogger(__name__)

_TVDB_BASE_URL = "https://api4.thetvdb.com/v4"
_TVDB_AUTH_CACHE_KEY = "tvdb:auth_token"
_TVDB_TOKEN_TTL_SECONDS = 23 * 60 * 60
_TVDB_RATE_BUCKET = "ratelimit:tvdb:metadata"
_TVDB_RATE_CAPACITY = 40.0
_TVDB_RATE_REFILL_PER_SECOND = 4.0


def _normalize_tvdb_image_url(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.startswith(("http://", "https://")):
        return normalized
    if normalized.startswith("//"):
        return f"https:{normalized}"
    if normalized.startswith("/"):
        return f"https://artworks.thetvdb.com{normalized}"
    return f"https://artworks.thetvdb.com/{normalized.lstrip('/')}"


def _extract_imdb_id(remote_ids: object) -> str | None:
    if not isinstance(remote_ids, list):
        return None
    for entry in remote_ids:
        if not isinstance(entry, dict):
            continue
        raw_id = entry.get("id")
        if not isinstance(raw_id, str) or not raw_id.strip():
            continue
        source_name = entry.get("sourceName")
        if isinstance(source_name, str) and source_name.strip().casefold() in {
            "imdb",
            "imdb.com",
        }:
            return raw_id.strip()
    return None


def _extract_poster_url(payload: dict[str, Any]) -> str | None:
    artworks = payload.get("artworks")
    if isinstance(artworks, list):
        for entry in artworks:
            if not isinstance(entry, dict):
                continue
            if entry.get("type") not in {2, 14}:
                continue
            raw_image = entry.get("image")
            if isinstance(raw_image, str) and raw_image.strip():
                return _normalize_tvdb_image_url(raw_image)
    raw_image = payload.get("image")
    return _normalize_tvdb_image_url(raw_image if isinstance(raw_image, str) else None)


@dataclass(frozen=True)
class TvdbSeriesMetadata:
    """Normalized TVDB series metadata used by fallback enrichment."""

    title: str
    poster_url: str | None
    imdb_id: str | None
    tvdb_id: str
    overview: str


@dataclass(slots=True)
class TvdbClient:
    """Minimal TVDB v4 client with cached bearer-token handling."""

    api_key: str
    cache: CacheManager
    rate_limiter: DistributedRateLimiter | None = None
    transport: httpx.AsyncBaseTransport | None = None

    async def _acquire_rate_limit(self) -> None:
        if self.rate_limiter is None:
            return
        await self.rate_limiter.acquire(
            bucket_key=_TVDB_RATE_BUCKET,
            capacity=_TVDB_RATE_CAPACITY,
            refill_rate_per_second=_TVDB_RATE_REFILL_PER_SECOND,
        )

    async def _cached_token(self) -> str | None:
        cached = await self.cache.get(_TVDB_AUTH_CACHE_KEY)
        if not isinstance(cached, bytes):
            return None
        try:
            payload = json.loads(cached.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return None
        token = payload.get("token")
        return token if isinstance(token, str) and token.strip() else None

    async def _store_token(self, token: str) -> None:
        await self.cache.set(
            _TVDB_AUTH_CACHE_KEY,
            json.dumps({"token": token}).encode("utf-8"),
            ttl_seconds=_TVDB_TOKEN_TTL_SECONDS,
        )

    async def _login(self) -> str | None:
        normalized_api_key = self.api_key.strip()
        if not normalized_api_key:
            return None

        await self._acquire_rate_limit()
        try:
            async with httpx.AsyncClient(timeout=10.0, transport=self.transport) as client:
                response = await client.post(
                    f"{_TVDB_BASE_URL}/login",
                    json={"apikey": normalized_api_key},
                )
        except httpx.HTTPError as exc:
            logger.warning("tvdb login failed", extra={"error": str(exc)})
            return None

        if not response.is_success:
            logger.warning(
                "tvdb login returned non-success status",
                extra={"status_code": response.status_code},
            )
            return None

        try:
            payload = response.json()
        except ValueError:
            logger.warning("tvdb login returned invalid JSON")
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        token = data.get("token") if isinstance(data, dict) else None
        if not isinstance(token, str) or not token.strip():
            return None

        normalized_token = token.strip()
        await self._store_token(normalized_token)
        return normalized_token

    async def _bearer_token(self) -> str | None:
        cached_token = await self._cached_token()
        if cached_token is not None:
            return cached_token
        return await self._login()

    async def get_series_extended(self, tvdb_id: str) -> TvdbSeriesMetadata | None:
        token = await self._bearer_token()
        if token is None:
            return None

        await self._acquire_rate_limit()
        try:
            async with httpx.AsyncClient(timeout=10.0, transport=self.transport) as client:
                response = await client.get(
                    f"{_TVDB_BASE_URL}/series/{tvdb_id}/extended",
                    headers={"Authorization": f"Bearer {token}"},
                )
        except httpx.HTTPError as exc:
            logger.warning(
                "tvdb series lookup failed",
                extra={"tvdb_id": tvdb_id, "error": str(exc)},
            )
            return None

        if not response.is_success:
            logger.warning(
                "tvdb series lookup returned non-success status",
                extra={"tvdb_id": tvdb_id, "status_code": response.status_code},
            )
            return None

        try:
            payload = response.json()
        except ValueError:
            logger.warning("tvdb series lookup returned invalid JSON", extra={"tvdb_id": tvdb_id})
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return None

        raw_title = data.get("name")
        title = raw_title.strip() if isinstance(raw_title, str) and raw_title.strip() else tvdb_id
        raw_overview = data.get("overview")
        overview = raw_overview.strip() if isinstance(raw_overview, str) else ""
        return TvdbSeriesMetadata(
            title=title,
            poster_url=_extract_poster_url(data),
            imdb_id=_extract_imdb_id(data.get("remoteIds")),
            tvdb_id=str(data.get("id") or tvdb_id),
            overview=overview,
        )
