"""TMDB metadata client used for request-time enrichment."""

from __future__ import annotations

import logging
import re
import unicodedata
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
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
_TMDB_EDITORIAL_MOVIE_PATHS: dict[str, str] = {
    "trending": "/trending/movie/day",
    "popular": "/movie/popular",
    "anticipated": "/movie/upcoming",
    "newly-released": "/movie/now_playing",
}
_TMDB_EDITORIAL_SHOW_PATHS: dict[str, str] = {
    "trending": "/trending/tv/day",
    "popular": "/tv/popular",
    "returning": "/tv/on_the_air",
    "newly-released": "/tv/airing_today",
}


def _iso_day(value: date) -> str:
    return value.isoformat()


def _movie_release_window_params(
    window: str,
    *,
    page: int,
    reference_date: date,
) -> dict[str, str] | None:
    if window == "theatrical":
        start_day = reference_date - timedelta(days=14)
        end_day = reference_date + timedelta(days=45)
        return {
            "page": str(max(page, 1)),
            "sort_by": "primary_release_date.asc",
            "with_release_type": "2|3",
            "primary_release_date.gte": _iso_day(start_day),
            "primary_release_date.lte": _iso_day(end_day),
        }
    if window == "digital":
        start_day = reference_date - timedelta(days=21)
        end_day = reference_date + timedelta(days=14)
        return {
            "page": str(max(page, 1)),
            "sort_by": "primary_release_date.desc",
            "with_release_type": "4|5",
            "primary_release_date.gte": _iso_day(start_day),
            "primary_release_date.lte": _iso_day(end_day),
        }
    return None


def _show_release_window_params(
    window: str,
    *,
    page: int,
    reference_date: date,
) -> tuple[str, dict[str, str]] | None:
    if window == "returning":
        return ("/tv/on_the_air", {"page": str(max(page, 1))})
    if window == "limited-series":
        start_day = reference_date - timedelta(days=21)
        end_day = reference_date + timedelta(days=75)
        return (
            "/discover/tv",
            {
                "page": str(max(page, 1)),
                "sort_by": "first_air_date.asc",
                "with_type": "2",
                "first_air_date.gte": _iso_day(start_day),
                "first_air_date.lte": _iso_day(end_day),
            },
        )
    return None


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
    companies: list[dict[str, str]] = Field(default_factory=list)
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
    companies: list[dict[str, str]] = Field(default_factory=list)
    networks: list[dict[str, str]] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    status: str = ""
    seasons: list[dict[str, object]] = Field(default_factory=list)
    next_episode_to_air: dict[str, object] | None = None


class TmdbSearchResult(BaseModel):
    """Normalized TMDB search hit used by request discovery flows."""

    model_config = ConfigDict(extra="ignore")

    tmdb_id: str = Field(alias="id")
    media_type: str
    title: str
    year: int | None = None
    overview: str = ""
    poster_path: str | None = None
    popularity: float = 0.0
    vote_average: float = 0.0
    vote_count: int = 0
    original_language: str | None = None
    genre_names: list[str] = Field(default_factory=list)


@dataclass(frozen=True, slots=True)
class TmdbSearchPage:
    """One normalized TMDB search page with page metadata."""

    results: list[TmdbSearchResult]
    page: int
    total_pages: int
    total_results: int


@dataclass(frozen=True, slots=True)
class TmdbNamedReference:
    """One normalized named TMDB entity reference used by discovery follow-ups."""

    tmdb_id: str
    name: str
    image_path: str | None = None


@dataclass(frozen=True, slots=True)
class TmdbDiscoveryProfile:
    """One detail-backed TMDB discovery profile for follow-up projections and facets."""

    tmdb_id: str
    media_type: str
    title: str
    people: tuple[TmdbNamedReference, ...] = ()
    companies: tuple[TmdbNamedReference, ...] = ()
    networks: tuple[TmdbNamedReference, ...] = ()
    collection: TmdbNamedReference | None = None


def _extract_year(value: Any) -> int | None:
    if not isinstance(value, str) or len(value) < 4:
        return None
    year_prefix = value[:4]
    return int(year_prefix) if year_prefix.isdigit() else None


def _extract_float(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _extract_int(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


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


def _normalize_named_entities(payload: dict[str, Any], key: str) -> list[dict[str, str]]:
    raw = payload.get(key)
    if not isinstance(raw, list):
        return []

    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        raw_id = entry.get("id")
        raw_name = entry.get("name")
        if raw_id is None or raw_name is None:
            continue
        identifier = str(raw_id).strip()
        name = str(raw_name).strip()
        if not identifier or not name:
            continue
        dedupe_key = f"{identifier}:{name.casefold()}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized.append({"id": identifier, "name": name})
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


def _normalize_search_results(
    payload: dict[str, Any],
    *,
    media_type: str,
    genre_lookup: dict[int, str] | None = None,
) -> list[TmdbSearchResult]:
    raw_results = payload.get("results")
    if not isinstance(raw_results, list):
        return []

    normalized_results: list[TmdbSearchResult] = []
    for entry in raw_results:
        if not isinstance(entry, dict):
            continue

        title_value = entry.get("title") if media_type == "movie" else entry.get("name")
        if not isinstance(title_value, str) or not title_value.strip():
            continue

        raw_genre_ids = entry.get("genre_ids")
        genre_names: list[str] = []
        if isinstance(raw_genre_ids, list) and genre_lookup:
            for genre_id in raw_genre_ids:
                label = genre_lookup.get(_extract_int(genre_id))
                if label and label not in genre_names:
                    genre_names.append(label)

        normalized_results.append(
            TmdbSearchResult.model_validate(
                {
                    "id": str(entry.get("id", "")).strip(),
                    "media_type": media_type,
                    "title": title_value.strip(),
                    "year": _extract_year(
                        entry.get("release_date")
                        if media_type == "movie"
                        else entry.get("first_air_date")
                    ),
                    "overview": str(entry.get("overview") or ""),
                    "poster_path": entry.get("poster_path"),
                    "popularity": _extract_float(entry.get("popularity")),
                    "vote_average": _extract_float(entry.get("vote_average")),
                    "vote_count": _extract_int(entry.get("vote_count")),
                    "original_language": (
                        str(entry.get("original_language")).strip().casefold() or None
                        if entry.get("original_language") is not None
                        else None
                    ),
                    "genre_names": genre_names,
                }
            )
        )

    return [result for result in normalized_results if result.tmdb_id]


def _normalize_search_page(
    payload: dict[str, Any],
    *,
    media_type: str,
    page: int,
    genre_lookup: dict[int, str] | None = None,
) -> TmdbSearchPage:
    total_pages_raw = payload.get("total_pages")
    total_results_raw = payload.get("total_results")
    if isinstance(total_pages_raw, int):
        total_pages = max(total_pages_raw, 1)
    elif isinstance(total_pages_raw, str) and total_pages_raw.isdigit():
        total_pages = max(int(total_pages_raw), 1)
    else:
        total_pages = max(page, 1)

    if isinstance(total_results_raw, int):
        total_results = max(total_results_raw, 0)
    elif isinstance(total_results_raw, str) and total_results_raw.isdigit():
        total_results = max(int(total_results_raw), 0)
    else:
        total_results = len(payload.get("results", [])) if isinstance(payload.get("results"), list) else 0

    return TmdbSearchPage(
        results=_normalize_search_results(
            payload,
            media_type=media_type,
            genre_lookup=genre_lookup,
        ),
        page=max(page, 1),
        total_pages=total_pages,
        total_results=total_results,
    )


def _normalize_named_references(
    payload: object,
    *,
    image_keys: tuple[str, ...],
) -> tuple[TmdbNamedReference, ...]:
    if not isinstance(payload, list):
        return ()

    references: list[TmdbNamedReference] = []
    seen: set[str] = set()
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        raw_id = str(entry.get("id") or "").strip()
        name = str(entry.get("name") or "").strip()
        if not raw_id or not name or raw_id in seen:
            continue
        seen.add(raw_id)
        image_path = None
        for key in image_keys:
            value = entry.get(key)
            if isinstance(value, str) and value.strip():
                image_path = value.strip()
                break
        references.append(
            TmdbNamedReference(
                tmdb_id=raw_id,
                name=name,
                image_path=image_path,
            )
        )

    return tuple(references)


def _normalize_collection_reference(payload: object) -> TmdbNamedReference | None:
    if not isinstance(payload, dict):
        return None

    raw_id = str(payload.get("id") or "").strip()
    name = str(payload.get("name") or "").strip()
    if not raw_id or not name:
        return None

    poster_path = payload.get("poster_path")
    image_path = poster_path.strip() if isinstance(poster_path, str) and poster_path.strip() else None
    return TmdbNamedReference(tmdb_id=raw_id, name=name, image_path=image_path)


def _normalize_credit_people(payload: object) -> tuple[TmdbNamedReference, ...]:
    if not isinstance(payload, dict):
        return ()

    selected: list[TmdbNamedReference] = []
    seen: set[str] = set()

    crew_entries = payload.get("crew")
    if isinstance(crew_entries, list):
        prioritized_jobs = (
            "Director",
            "Creator",
            "Screenplay",
            "Writer",
            "Original Story",
        )
        for job in prioritized_jobs:
            for entry in crew_entries:
                if not isinstance(entry, dict):
                    continue
                if str(entry.get("job") or "").strip() != job:
                    continue
                raw_id = str(entry.get("id") or "").strip()
                name = str(entry.get("name") or "").strip()
                if not raw_id or not name or raw_id in seen:
                    continue
                seen.add(raw_id)
                profile_path = entry.get("profile_path")
                selected.append(
                    TmdbNamedReference(
                        tmdb_id=raw_id,
                        name=name,
                        image_path=(
                            profile_path.strip()
                            if isinstance(profile_path, str) and profile_path.strip()
                            else None
                        ),
                    )
                )
                break
            if len(selected) >= 3:
                break

    cast_entries = payload.get("cast")
    if isinstance(cast_entries, list):
        for entry in cast_entries:
            if len(selected) >= 6:
                break
            if not isinstance(entry, dict):
                continue
            raw_id = str(entry.get("id") or "").strip()
            name = str(entry.get("name") or "").strip()
            if not raw_id or not name or raw_id in seen:
                continue
            seen.add(raw_id)
            profile_path = entry.get("profile_path")
            selected.append(
                TmdbNamedReference(
                    tmdb_id=raw_id,
                    name=name,
                    image_path=(
                        profile_path.strip()
                        if isinstance(profile_path, str) and profile_path.strip()
                        else None
                    ),
                )
            )

    return tuple(selected)


@dataclass(slots=True)
class TmdbMetadataClient:
    """Minimal TMDB API client for request-time title/poster enrichment."""

    api_key: str
    rate_limiter: DistributedRateLimiter
    transport: httpx.AsyncBaseTransport | None = None
    _genre_cache: dict[str, dict[int, str]] = field(default_factory=dict, init=False, repr=False)
    _profile_cache: dict[str, TmdbDiscoveryProfile] = field(default_factory=dict, init=False, repr=False)

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

    async def _get_genre_lookup(self, media_type: str) -> dict[int, str]:
        normalized_media_type = "movie" if media_type == "movie" else "tv"
        cached = self._genre_cache.get(normalized_media_type)
        if cached is not None:
            return cached

        payload = await self._request_json(f"/genre/{normalized_media_type}/list")
        raw_genres = payload.get("genres") if payload is not None else None
        if not isinstance(raw_genres, list):
            return {}

        lookup: dict[int, str] = {}
        for entry in raw_genres:
            if not isinstance(entry, dict):
                continue
            genre_id = _extract_int(entry.get("id"))
            genre_name = str(entry.get("name") or "").strip()
            if genre_id > 0 and genre_name:
                lookup[genre_id] = genre_name
        self._genre_cache[normalized_media_type] = lookup
        return lookup

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
            "companies": _normalize_named_entities(payload, "production_companies"),
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
            "companies": _normalize_named_entities(payload, "production_companies"),
            "networks": _normalize_named_entities(payload, "networks"),
            "aliases": _normalize_aliases(payload, canonical_title=cast(str | None, payload.get("name"))),
        }
        return ShowMetadata.model_validate(normalized)

    async def search_movies(self, query: str, *, page: int = 1) -> list[TmdbSearchResult]:
        """Return normalized TMDB movie search hits for one query."""

        return (await self.search_movie_page(query, page=page)).results

    async def search_movie_page(self, query: str, *, page: int = 1) -> TmdbSearchPage:
        """Return one normalized TMDB movie search page."""

        normalized_query = query.strip()
        if not normalized_query:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        payload = await self._request_json(
            "/search/movie",
            params={"query": normalized_query, "page": str(max(page, 1))},
        )
        if payload is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        return _normalize_search_page(payload, media_type="movie", page=max(page, 1))

    async def search_shows(self, query: str, *, page: int = 1) -> list[TmdbSearchResult]:
        """Return normalized TMDB show search hits for one query."""

        return (await self.search_show_page(query, page=page)).results

    async def search_show_page(self, query: str, *, page: int = 1) -> TmdbSearchPage:
        """Return one normalized TMDB show search page."""

        normalized_query = query.strip()
        if not normalized_query:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        payload = await self._request_json(
            "/search/tv",
            params={"query": normalized_query, "page": str(max(page, 1))},
        )
        if payload is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        return _normalize_search_page(payload, media_type="show", page=max(page, 1))

    async def discover_movie_page(
        self,
        *,
        page: int = 1,
        genre: str | None = None,
        release_year: int | None = None,
        original_language: str | None = None,
        company: str | None = None,
        sort_by: str | None = None,
    ) -> TmdbSearchPage:
        """Return one normalized TMDB movie discover page."""

        params = {"page": str(max(page, 1))}
        if genre:
            params["with_genres"] = genre
        if release_year is not None:
            params["primary_release_year"] = str(release_year)
        if original_language:
            params["with_original_language"] = original_language
        if company:
            params["with_companies"] = company
        if sort_by:
            params["sort_by"] = sort_by

        payload = await self._request_json("/discover/movie", params=params)
        if payload is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        return _normalize_search_page(
            payload,
            media_type="movie",
            page=max(page, 1),
            genre_lookup=await self._get_genre_lookup("movie"),
        )

    async def discover_show_page(
        self,
        *,
        page: int = 1,
        genre: str | None = None,
        release_year: int | None = None,
        original_language: str | None = None,
        network: str | None = None,
        sort_by: str | None = None,
    ) -> TmdbSearchPage:
        """Return one normalized TMDB show discover page."""

        params = {"page": str(max(page, 1))}
        if genre:
            params["with_genres"] = genre
        if release_year is not None:
            params["first_air_date_year"] = str(release_year)
        if original_language:
            params["with_original_language"] = original_language
        if network:
            params["with_networks"] = network
        if sort_by:
            params["sort_by"] = sort_by

        payload = await self._request_json("/discover/tv", params=params)
        if payload is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        return _normalize_search_page(
            payload,
            media_type="show",
            page=max(page, 1),
            genre_lookup=await self._get_genre_lookup("show"),
        )

    async def editorial_movie_page(
        self,
        *,
        family: str,
        page: int = 1,
    ) -> TmdbSearchPage:
        """Return one normalized TMDB movie editorial page."""

        normalized_family = family.strip().casefold()
        path = _TMDB_EDITORIAL_MOVIE_PATHS.get(normalized_family)
        if path is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        payload = await self._request_json(path, params={"page": str(max(page, 1))})
        if payload is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        return _normalize_search_page(
            payload,
            media_type="movie",
            page=max(page, 1),
            genre_lookup=await self._get_genre_lookup("movie"),
        )

    async def editorial_show_page(
        self,
        *,
        family: str,
        page: int = 1,
    ) -> TmdbSearchPage:
        """Return one normalized TMDB show editorial page."""

        normalized_family = family.strip().casefold()
        path = _TMDB_EDITORIAL_SHOW_PATHS.get(normalized_family)
        if path is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        payload = await self._request_json(path, params={"page": str(max(page, 1))})
        if payload is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        return _normalize_search_page(
            payload,
            media_type="show",
            page=max(page, 1),
            genre_lookup=await self._get_genre_lookup("show"),
        )

    async def release_window_movie_page(
        self,
        *,
        window: str,
        page: int = 1,
        reference_date: date | None = None,
    ) -> TmdbSearchPage:
        """Return one normalized TMDB movie release-window page."""

        normalized_window = window.strip().casefold()
        reference_day = reference_date or datetime.now(UTC).date()
        params = _movie_release_window_params(
            normalized_window,
            page=max(page, 1),
            reference_date=reference_day,
        )
        if params is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        payload = await self._request_json("/discover/movie", params=params)
        if payload is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        return _normalize_search_page(
            payload,
            media_type="movie",
            page=max(page, 1),
            genre_lookup=await self._get_genre_lookup("movie"),
        )

    async def release_window_show_page(
        self,
        *,
        window: str,
        page: int = 1,
        reference_date: date | None = None,
    ) -> TmdbSearchPage:
        """Return one normalized TMDB show release-window page."""

        normalized_window = window.strip().casefold()
        reference_day = reference_date or datetime.now(UTC).date()
        request_definition = _show_release_window_params(
            normalized_window,
            page=max(page, 1),
            reference_date=reference_day,
        )
        if request_definition is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        path, params = request_definition
        payload = await self._request_json(path, params=params)
        if payload is None:
            return TmdbSearchPage(results=[], page=max(page, 1), total_pages=1, total_results=0)

        return _normalize_search_page(
            payload,
            media_type="show",
            page=max(page, 1),
            genre_lookup=await self._get_genre_lookup("show"),
        )

    async def get_discovery_profile(
        self,
        tmdb_id: str,
        media_type: str,
    ) -> TmdbDiscoveryProfile | None:
        """Return one detail-backed discovery profile for follow-up grouping and facets."""

        normalized_media_type = "movie" if media_type == "movie" else "tv"
        cache_key = f"{normalized_media_type}:{tmdb_id}"
        cached = self._profile_cache.get(cache_key)
        if cached is not None:
            return cached

        payload = await self._request_json(
            f"/{normalized_media_type}/{tmdb_id}",
            params={"append_to_response": "credits"},
        )
        if payload is None:
            return None

        title_value = payload.get("title") if media_type == "movie" else payload.get("name")
        title = str(title_value or "").strip()
        if not title:
            return None

        profile = TmdbDiscoveryProfile(
            tmdb_id=str(payload.get("id") or tmdb_id).strip(),
            media_type=("movie" if media_type == "movie" else "show"),
            title=title,
            people=_normalize_credit_people(payload.get("credits")),
            companies=_normalize_named_references(
                payload.get("production_companies"),
                image_keys=("logo_path",),
            ),
            networks=_normalize_named_references(
                payload.get("networks"),
                image_keys=("logo_path",),
            ),
            collection=_normalize_collection_reference(payload.get("belongs_to_collection")),
        )
        self._profile_cache[cache_key] = profile
        return profile

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
