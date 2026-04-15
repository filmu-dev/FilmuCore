"""Built-in Comet scraper plugin implementation."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import httpx

from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import ScraperResult, ScraperSearchInput

COMET_PLUGIN_NAME = "comet"
_COMET_SEARCH_BUCKET = "comet:search"
_COMET_RATE_LIMIT_CAPACITY = 60.0
_COMET_RATE_LIMIT_REFILL_PER_SECOND = 1.0

_BTIH_PATTERN = re.compile(r"btih:([A-Fa-f0-9]{32,40})")


def _comet_settings(settings: Mapping[str, Any]) -> Mapping[str, Any]:
    if any(key in settings for key in {"enabled", "url", "timeout"}):
        return settings
    scraping = settings.get("scraping")
    if not isinstance(scraping, Mapping):
        return {}
    comet = scraping.get(COMET_PLUGIN_NAME)
    if not isinstance(comet, Mapping):
        return {}
    return comet


def _stream_endpoint(metadata: ScraperSearchInput, *, imdb_id: str) -> str:
    item_type = (metadata.item_type or "movie").casefold()
    if item_type in {"episode", "show", "series"}:
        season_number = metadata.season_number or 1
        episode_number = metadata.episode_number or 1
        return f"stream/series/{imdb_id}:{season_number}:{episode_number}.json"
    return f"stream/movie/{imdb_id}.json"


def _extract_info_hash(candidate: Mapping[str, Any]) -> str | None:
    for key in ("infoHash", "infohash"):
        value = candidate.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    for key in ("magnetUrl", "magnet_url", "magnet"):
        value = candidate.get(key)
        if not isinstance(value, str):
            continue
        match = _BTIH_PATTERN.search(value)
        if match is not None:
            return match.group(1).lower()
    return None


class CometScraper:
    """Query a Comet-compatible Stremio endpoint and normalize torrent candidates."""

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.ctx: PluginContext | None = None
        self._transport = transport
        self.base_url = ""
        self.timeout_seconds = 20.0
        self.enabled = False

    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx
        block = _comet_settings(ctx.settings)
        self.base_url = str(block.get("url") or block.get("base_url") or "").rstrip("/")
        timeout_value = block.get("timeout")
        if isinstance(timeout_value, (int, float)) and timeout_value > 0:
            self.timeout_seconds = float(timeout_value)
        enabled_value = block.get("enabled")
        self.enabled = bool(enabled_value) if enabled_value is not None else False
        if self.enabled and not self.base_url:
            ctx.logger.warning(
                "plugin.stub_not_configured",
                plugin=self.ctx.plugin_name,
                reason="comet enabled but url missing",
            )

    async def search(self, metadata: ScraperSearchInput) -> list[ScraperResult]:
        if self.ctx is None:
            raise RuntimeError("CometScraper must be initialized before search")
        if not self.enabled or not self.base_url:
            return []

        imdb_id = metadata.external_ids.imdb_id
        if not imdb_id:
            self.ctx.logger.warning(
                "plugin.scraper.comet.skipped",
                plugin=self.ctx.plugin_name,
                reason="imdb_id_missing",
            )
            return []

        await self.ctx.rate_limiter.acquire(
            _COMET_SEARCH_BUCKET,
            _COMET_RATE_LIMIT_CAPACITY,
            _COMET_RATE_LIMIT_REFILL_PER_SECOND,
        )

        url = f"{self.base_url}/{_stream_endpoint(metadata, imdb_id=imdb_id)}"
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
            self.ctx.logger.warning(
                "plugin.scraper.comet.request_failed",
                plugin=self.ctx.plugin_name,
                error=repr(exc),
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
            title = stream.get("title") or stream.get("name")
            if not isinstance(title, str) or not title.strip():
                continue
            info_hash = _extract_info_hash(stream)
            if not info_hash:
                continue

            results.append(
                ScraperResult(
                    title=title.strip().splitlines()[0],
                    provider=COMET_PLUGIN_NAME,
                    magnet_url=(
                        stream.get("magnetUrl")
                        if isinstance(stream.get("magnetUrl"), str)
                        else f"magnet:?xt=urn:btih:{info_hash}"
                    ),
                    download_url=(
                        stream.get("downloadUrl")
                        if isinstance(stream.get("downloadUrl"), str)
                        else None
                    ),
                    info_hash=info_hash,
                    metadata={"raw_title": title, "imdb_id": imdb_id},
                )
            )
        return results
