"""TMDB metadata client used for request-time enrichment."""

from __future__ import annotations

import logging
import re
import unicodedata
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

import httpx
from pydantic import BaseModel, ConfigDict, Field

from filmu_py.config import Settings
from filmu_py.core.rate_limiter import DistributedRateLimiter

logger = logging.getLogger(__name__)

_TMDB_BASE_URL = "https://api.themoviedb.org/3"
_TMDB_RATE_BUCKET = "ratelimit:tmdb:metadata"
_TMDB_RATE_CAPACITY = 40.0
_TMDB_RATE_REFILL_PER_SECOND = 4.0
_TMDB_ALIAS_KEYS: tuple[str, ...] = ("title", "name", "translated_title")


class TmdbGenre(BaseModel):
    """One TMDB genre entry."""

    model_config = ConfigDict(extra="ignore")

    id: int
    name: str


class MovieMetadata(BaseModel):
    """Normalized TMDB movie metadata used by request-time enrichment."""

    model_config = ConfigDict(extra="ignore")

    tmdb_id: str = Field(alias="id")
    title: str
    year: int | None = None
    overview: str = ""
    poster_path: str | None = None
    genres: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    status: str = ""


class ShowMetadata(BaseModel):
    """Normalized TMDB show metadata used by request-time enrichment."""

    model_config = ConfigDict(extra="ignore")

    tmdb_id: str = Field(alias="id")
    title: str = Field(alias="name")
    year: int | None = None
    overview: str = ""
    poster_path: str | None = None
    genres: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    status: str = ""
    seasons: list[dict[str, object]] = Field(default_factory=list)
    next_episode_to_air: dict[str, object] | None = None


def _extract_year(value: Any) -> int | None:
    if not isinstance(value, str) or len(value) < 4:
        return None
    year_prefix = value[:4]
    return int(year_prefix) if year_prefix.isdigit() else None


def _normalize_genres(payload: dict[str, Any]) -> list[str]:
    raw = payload.get("genres")
    if not isinstance(raw, list):
        return []
    normalized: list[str] = []
    for entry in raw:
        if isinstance(entry, dict):
            name = entry.get("name")
            if isinstance(name, str) and name:
                normalized.append(name)
    return normalized


def _normalize_alias_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _alias_entries(payload: object) -> list[object]:
    if isinstance(payload, dict):
        for key in ("titles", "results"):
            entries = payload.get(key)
            if isinstance(entries, list):
                return entries
    if isinstance(payload, list):
        return payload
    return []


def _normalize_aliases(payload: dict[str, Any], *, canonical_title: str | None = None) -> list[str]:
    aliases: list[str] = []
    seen: set[str] = set()
    if canonical_title is not None:
        normalized_canonical = _normalize_alias_text(canonical_title)
        if normalized_canonical:
            seen.add(normalized_canonical)

    alternative_titles = payload.get("alternative_titles")
    for entry in _alias_entries(alternative_titles):
        alias: str | None = None
        if isinstance(entry, str):
            alias = entry
        elif isinstance(entry, dict):
            for key in _TMDB_ALIAS_KEYS:
                value = entry.get(key)
                if isinstance(value, str) and value.strip():
                    alias = value.strip()
                    break
        if alias is None:
            continue

        normalized_alias = _normalize_alias_text(alias)
        if not normalized_alias or normalized_alias in seen:
            continue
        seen.add(normalized_alias)
        aliases.append(alias)

    return aliases


@dataclass(slots=True)
class TmdbMetadataClient:
    """Minimal TMDB API client for request-time title/poster enrichment."""

    api_key: str
    rate_limiter: DistributedRateLimiter
    transport: httpx.AsyncBaseTransport | None = None

    async def _request_json(
        self,
        path: str,
        *,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any] | None:
        api_key = self.api_key.strip()
        if not api_key:
            return None

        await self.rate_limiter.acquire(
            bucket_key=_TMDB_RATE_BUCKET,
            capacity=_TMDB_RATE_CAPACITY,
            refill_rate_per_second=_TMDB_RATE_REFILL_PER_SECOND,
        )

        url = f"{_TMDB_BASE_URL}{path}"
        request_params = {"api_key": api_key, **(params or {})}
        try:
            async with httpx.AsyncClient(
                timeout=10.0,
                transport=self.transport,
                params=request_params,
            ) as client:
                response = await client.get(url)
        except httpx.HTTPError as exc:
            logger.warning("tmdb metadata request failed", extra={"url": url, "error": str(exc)})
            return None

        if not response.is_success:
            logger.warning(
                "tmdb metadata request returned non-success status",
                extra={"url": url, "status_code": response.status_code},
            )
            return None

        try:
            payload = response.json()
        except ValueError:
            logger.warning("tmdb metadata request returned invalid JSON", extra={"url": url})
            return None

        return payload if isinstance(payload, dict) else None

    async def get_movie(self, tmdb_id: str) -> MovieMetadata | None:
        payload = await self._request_json(
            f"/movie/{tmdb_id}",
            params={"append_to_response": "alternative_titles"},
        )
        if payload is None:
            return None

        normalized = {
            **payload,
            "id": str(payload.get("id", tmdb_id)),
            "year": _extract_year(payload.get("release_date")),
            "genres": _normalize_genres(payload),
            "aliases": _normalize_aliases(payload, canonical_title=cast(str | None, payload.get("title"))),
        }
        return MovieMetadata.model_validate(normalized)

    async def get_show(self, tmdb_id: str) -> ShowMetadata | None:
        payload = await self._request_json(
            f"/tv/{tmdb_id}",
            params={"append_to_response": "alternative_titles"},
        )
        if payload is None:
            return None

        normalized = {
            **payload,
            "id": str(payload.get("id", tmdb_id)),
            "year": _extract_year(payload.get("first_air_date")),
            "genres": _normalize_genres(payload),
            "aliases": _normalize_aliases(payload, canonical_title=cast(str | None, payload.get("name"))),
        }
        return ShowMetadata.model_validate(normalized)

    async def get_external_ids(self, tmdb_id: str, media_type: str) -> dict[str, str | None]:
        """Resolve TMDB external IDs for one movie or TV record."""

        normalized_media_type = "movie" if media_type == "movie" else "tv"
        payload = await self._request_json(f"/{normalized_media_type}/{tmdb_id}/external_ids")
        if payload is None:
            return {"imdb_id": None, "tvdb_id": None}

        raw_imdb_id = payload.get("imdb_id")
        imdb_id = raw_imdb_id.strip() if isinstance(raw_imdb_id, str) and raw_imdb_id.strip() else None

        raw_tvdb_id = payload.get("tvdb_id")
        if isinstance(raw_tvdb_id, str):
            tvdb_id = raw_tvdb_id.strip() or None
        elif isinstance(raw_tvdb_id, int):
            tvdb_id = str(raw_tvdb_id)
        else:
            tvdb_id = None

        return {"imdb_id": imdb_id, "tvdb_id": tvdb_id}

    async def find_by_external_id(
        self, external_source: str, external_id: str
    ) -> MovieMetadata | ShowMetadata | None:
        """Resolve an external ID directly to normalized TMDB metadata."""

        payload = await self._request_json(f"/find/{external_id}?external_source={external_source}")
        if not payload:
            return None

        movie_results = payload.get("movie_results", [])
        if movie_results and isinstance(movie_results, list):
            first = movie_results[0]
            if not isinstance(first, dict):
                return None
            # /find doesn't return full details, but we can construct partial metadata
            # or hit the actual endpoint for the full details. Let's hit the full endpoint for simplicity:
            get_movie = cast(Callable[[str], Awaitable[MovieMetadata | None]], self.get_movie)
            movie = await get_movie(str(first.get("id")))
            return movie

        tv_results = payload.get("tv_results", [])
        if tv_results and isinstance(tv_results, list):
            first = tv_results[0]
            if not isinstance(first, dict):
                return None
            get_show = cast(Callable[[str], Awaitable[ShowMetadata | None]], self.get_show)
            show = await get_show(str(first.get("id")))
            return show

        return None


def build_tmdb_metadata_client(
    settings: Settings,
    rate_limiter: DistributedRateLimiter,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> TmdbMetadataClient | None:
    """Return a TMDB client when the runtime settings include a configured API key."""

    if not settings.tmdb_api_key.strip():
        return None
    return TmdbMetadataClient(
        api_key=settings.tmdb_api_key,
        rate_limiter=rate_limiter,
        transport=transport,
    )
