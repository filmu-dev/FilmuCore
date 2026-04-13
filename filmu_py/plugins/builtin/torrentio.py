"""Built-in Torrentio scraper plugin implementation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import httpx

from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import ScraperResult, ScraperSearchInput

TORRENTIO_PLUGIN_NAME = "torrentio"
_DEFAULT_TORRENTIO_BASE_URL = "https://torrentio.strem.fun"
_DEFAULT_TORRENTIO_FILTER = "sort=qualitysize%7Cqualityfilter=480p,scr,cam"
_TORRENTIO_SEARCH_BUCKET = "torrentio:search"
_TORRENTIO_RATE_LIMIT_CAPACITY = 150.0
_TORRENTIO_RATE_LIMIT_REFILL_PER_SECOND = 150.0 / 60.0


def _normalize_base_url(value: str) -> str:
    normalized = value.strip().rstrip("/")
    parsed = urlsplit(normalized)
    if (
        parsed.scheme.casefold() == "http"
        and (parsed.hostname or "").casefold() == "torrentio.strem.fun"
    ):
        return parsed._replace(scheme="https").geturl().rstrip("/")
    return normalized


def _scraping_settings(settings: Mapping[str, Any]) -> Mapping[str, Any]:
    if any(key in settings for key in {"enabled", "url", "filter", "timeout", "torrentio_url"}):
        return settings
    scraping = settings.get("scraping")
    if not isinstance(scraping, Mapping):
        return {}
    torrentio = scraping.get("torrentio")
    if not isinstance(torrentio, Mapping):
        return {}
    return torrentio


def _stream_endpoint(metadata: ScraperSearchInput, *, imdb_id: str) -> str:
    item_type = (metadata.item_type or "movie").casefold()
    if item_type in {"episode", "show", "series"}:
        season_number = metadata.season_number or 1
        episode_number = metadata.episode_number or 1
        return f"stream/series/{imdb_id}:{season_number}:{episode_number}.json"
    return f"stream/movie/{imdb_id}.json"


def _normalize_stream_title(raw_title: str) -> str:
    title_block = raw_title.split("\n⚙️", 1)[0]
    first_line = title_block.splitlines()[0] if title_block.splitlines() else title_block
    normalized = first_line.strip() or raw_title.strip()
    return normalized


class TorrentioScraper:
    """First built-in scraper plugin that queries Torrentio's Stremio API."""

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.ctx: PluginContext | None = None
        self._transport = transport
        self.base_url = _DEFAULT_TORRENTIO_BASE_URL
        self.filter_query = _DEFAULT_TORRENTIO_FILTER
        self.timeout_seconds = 30.0
        self.enabled = True

    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx
        block = _scraping_settings(ctx.settings)
        configured_url = block.get("url") or ctx.settings.get("torrentio_url")
        self.base_url = _normalize_base_url(str(configured_url or _DEFAULT_TORRENTIO_BASE_URL))
        configured_filter = block.get("filter") or ctx.settings.get("torrentio_filter")
        self.filter_query = str(configured_filter or _DEFAULT_TORRENTIO_FILTER).strip()
        timeout_value = block.get("timeout") or ctx.settings.get("torrentio_timeout")
        if isinstance(timeout_value, (int, float)) and timeout_value > 0:
            self.timeout_seconds = float(timeout_value)
        enabled_value = block.get("enabled")
        self.enabled = bool(enabled_value) if enabled_value is not None else False

    async def search(self, metadata: ScraperSearchInput) -> list[ScraperResult]:
        if self.ctx is None:
            raise RuntimeError("TorrentioScraper must be initialized before search")
        if not self.enabled:
            return []

        imdb_id = metadata.external_ids.imdb_id
        if not imdb_id:
            self.ctx.logger.warning(
                "plugin.scraper.torrentio.skipped",
                reason="imdb_id_missing",
                plugin=self.ctx.plugin_name,
            )
            return []

        await self.ctx.rate_limiter.acquire(
            _TORRENTIO_SEARCH_BUCKET,
            _TORRENTIO_RATE_LIMIT_CAPACITY,
            _TORRENTIO_RATE_LIMIT_REFILL_PER_SECOND,
        )

        endpoint = _stream_endpoint(metadata, imdb_id=imdb_id)
        url = f"{self.base_url}/{endpoint}"
        if self.filter_query:
            url = f"{url}?{self.filter_query.lstrip('?')}"

        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=self.timeout_seconds,
                transport=self._transport,
            ) as client:
                response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
            self.ctx.logger.warning(
                "plugin.scraper.torrentio.request_failed",
                provider=TORRENTIO_PLUGIN_NAME,
                plugin=self.ctx.plugin_name,
                status=status,
                error=str(exc),
                url=url,
            )
            return []

        streams = payload.get("streams") if isinstance(payload, Mapping) else None
        if not isinstance(streams, list):
            return []

        results: list[ScraperResult] = []
        for stream in streams:
            if not isinstance(stream, Mapping):
                continue
            raw_hash = stream.get("infoHash") or stream.get("infohash")
            raw_title = stream.get("title") or stream.get("name")
            if not isinstance(raw_hash, str) or not raw_hash.strip():
                continue
            if not isinstance(raw_title, str) or not raw_title.strip():
                continue
            info_hash = raw_hash.strip().lower()
            title = _normalize_stream_title(raw_title)
            results.append(
                ScraperResult(
                    title=title,
                    provider=TORRENTIO_PLUGIN_NAME,
                    info_hash=info_hash,
                    magnet_url=f"magnet:?xt=urn:btih:{info_hash}",
                    metadata={"raw_title": raw_title, "imdb_id": imdb_id},
                )
            )
        return results


def build_example_manifest() -> dict[str, Any]:
    """Return a filesystem-plugin example manifest for the built-in scraper."""

    return {
        "name": TORRENTIO_PLUGIN_NAME,
        "version": "1.0.0",
        "api_version": "1",
        "capabilities": ["scraper"],
        "entry_module": "plugin.py",
        "scraper": "TorrentioScraper",
    }
