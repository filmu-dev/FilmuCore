"""Built-in Prowlarr scraper plugin implementation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse

import httpx

from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import ScraperResult, ScraperSearchInput

PROWLARR_PLUGIN_NAME = "prowlarr"
_DEFAULT_TIMEOUT_SECONDS = 30.0
_PROWLARR_SEARCH_BUCKET = "prowlarr:search"
_PROWLARR_RATE_LIMIT_CAPACITY = 60.0
_PROWLARR_RATE_LIMIT_REFILL_PER_SECOND = 1.0


def _prowlarr_settings(settings: Mapping[str, Any]) -> Mapping[str, Any]:
    if any(key in settings for key in {"enabled", "url", "api_key", "timeout"}):
        return settings
    scraping = settings.get("scraping")
    if not isinstance(scraping, Mapping):
        return {}
    prowlarr = scraping.get("prowlarr")
    if not isinstance(prowlarr, Mapping):
        return {}
    return prowlarr


def _docker_service_fallback_url(base_url: str) -> str | None:
    parsed = urlparse(base_url)
    if parsed.hostname not in {"localhost", "127.0.0.1"}:
        return None
    return f"{parsed.scheme or 'http'}://prowlarr:{parsed.port or 9696}{parsed.path}"


class ProwlarrScraper:
    """Query Prowlarr's search API and normalize torrent candidates."""

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.ctx: PluginContext | None = None
        self._transport = transport
        self.base_url = ""
        self.api_key = ""
        self.timeout_seconds = _DEFAULT_TIMEOUT_SECONDS
        self.enabled = True

    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx
        block = _prowlarr_settings(ctx.settings)
        self.base_url = str(block.get("url") or "").rstrip("/")
        self.api_key = str(block.get("api_key") or "").strip()
        timeout_value = block.get("timeout")
        if isinstance(timeout_value, (int, float)) and timeout_value > 0:
            self.timeout_seconds = float(timeout_value)
        enabled_value = block.get("enabled")
        self.enabled = bool(enabled_value) if enabled_value is not None else True

    async def search(self, metadata: ScraperSearchInput) -> list[ScraperResult]:
        if self.ctx is None:
            raise RuntimeError("ProwlarrScraper must be initialized before search")
        if not self.enabled or not self.base_url or not self.api_key:
            return []

        query = (metadata.query or metadata.title or metadata.external_ids.imdb_id or "").strip()
        if not query:
            self.ctx.logger.warning(
                "plugin.scraper.prowlarr.skipped",
                reason="query_missing",
                plugin=self.ctx.plugin_name,
            )
            return []

        await self.ctx.rate_limiter.acquire(
            _PROWLARR_SEARCH_BUCKET,
            _PROWLARR_RATE_LIMIT_CAPACITY,
            _PROWLARR_RATE_LIMIT_REFILL_PER_SECOND,
        )

        item_type = (metadata.item_type or "movie").casefold()
        category = 5000 if item_type in {"episode", "show", "series"} else 2000

        use_tvsearch = item_type in {"episode", "show", "series"} and (
            metadata.season_number is not None or metadata.episode_number is not None
        )
        params: dict[str, str | int] = {
            "query": query,
            "type": "tvsearch" if use_tvsearch else "search",
            "limit": 50,
            "categories": category,
        }
        if use_tvsearch:
            if metadata.season_number is not None:
                params["season"] = metadata.season_number
            if metadata.episode_number is not None:
                params["episode"] = metadata.episode_number

        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=self.timeout_seconds,
                transport=self._transport,
                headers={"X-Api-Key": self.api_key},
            ) as client:
                try:
                    response = await client.get(f"{self.base_url}/api/v1/search", params=params)
                    response.raise_for_status()
                except httpx.HTTPError:
                    fallback_url = _docker_service_fallback_url(self.base_url)
                    if fallback_url is None:
                        raise
                    response = await client.get(f"{fallback_url}/api/v1/search", params=params)
                    response.raise_for_status()
            payload = response.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 400:
                self.ctx.logger.debug(
                    "plugin.scraper.prowlarr.category_rejected",
                    plugin=self.ctx.plugin_name,
                    error=repr(exc),
                    query=query,
                )
                return []
            self.ctx.logger.warning(
                "plugin.scraper.prowlarr.request_failed",
                plugin=self.ctx.plugin_name,
                error=repr(exc),
                query=query,
            )
            return []
        except (httpx.HTTPError, ValueError) as exc:
            self.ctx.logger.warning(
                "plugin.scraper.prowlarr.request_failed",
                plugin=self.ctx.plugin_name,
                error=repr(exc),
                query=query,
            )
            return []

        if not isinstance(payload, list):
            return []

        results: list[ScraperResult] = []
        for item in payload:
            if not isinstance(item, Mapping):
                continue
            raw_title = item.get("title")
            raw_hash = item.get("infoHash") or item.get("infohash")
            if not isinstance(raw_title, str) or not raw_title.strip():
                continue
            if not isinstance(raw_hash, str) or not raw_hash.strip():
                continue

            results.append(
                ScraperResult(
                    title=str(raw_title).strip(),
                    provider=PROWLARR_PLUGIN_NAME,
                    magnet_url=item.get("magnetUrl")
                    if isinstance(item.get("magnetUrl"), str)
                    else None,
                    download_url=item.get("downloadUrl")
                    if isinstance(item.get("downloadUrl"), str)
                    else None,
                    info_hash=raw_hash.strip().lower(),
                    size_bytes=item.get("size") if isinstance(item.get("size"), int) else None,
                    seeders=item.get("seeders") if isinstance(item.get("seeders"), int) else None,
                    leechers=item.get("leechers")
                    if isinstance(item.get("leechers"), int)
                    else None,
                    metadata={"indexer": item.get("indexer"), "info_url": item.get("infoUrl")},
                )
            )
        return results
