"""Built-in RARBG-compatible scraper plugin implementation."""

from __future__ import annotations

import re
from collections.abc import Mapping
from html import unescape
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote_plus, urlparse

import httpx

from filmu_py.plugins.context import PluginContext
from filmu_py.plugins.interfaces import ScraperResult, ScraperSearchInput

RARBG_PLUGIN_NAME = "rarbg"
_DEFAULT_RARBG_BASE_URL = "https://therarbg.to"
_DEFAULT_TIMEOUT_SECONDS = 30.0
_RARBG_SEARCH_BUCKET = "rarbg:search"
_RARBG_RATE_LIMIT_CAPACITY = 20.0
_RARBG_RATE_LIMIT_REFILL_PER_SECOND = 20.0 / 60.0
_DETAIL_LINK_RE = re.compile(r'href="(/post-detail/[^"]+)"', re.IGNORECASE)
_MAGNET_RE = re.compile(r'href="(magnet:\?xt=urn:btih:[^"]+)"', re.IGNORECASE)
_SEED_LEECH_RE = re.compile(r"Seeders:\s*(\d+),\s*Leechers:\s*(\d+)", re.IGNORECASE)


def _rarbg_settings(settings: Mapping[str, Any]) -> Mapping[str, Any]:
    if any(key in settings for key in {"enabled", "url", "timeout"}):
        return settings
    scraping = settings.get("scraping")
    if not isinstance(scraping, Mapping):
        return {}
    rarbg = scraping.get("rarbg")
    if not isinstance(rarbg, Mapping):
        return {}
    return rarbg


def _extract_post_detail_links(html: str, *, base_url: str) -> list[str]:
    links: list[str] = []
    for match in _DETAIL_LINK_RE.finditer(html):
        link = match.group(1)
        normalized = f"{base_url}{link}" if link.startswith("/") else link
        if normalized not in links:
            links.append(normalized)
    return links


def _extract_result_from_detail(detail_html: str) -> ScraperResult | None:
    magnet_match = _MAGNET_RE.search(detail_html)
    if magnet_match is None:
        return None

    magnet_url = unescape(magnet_match.group(1))
    parsed = urlparse(magnet_url)
    query = parse_qs(parsed.query)
    xt_values = query.get("xt", [])
    dn_values = query.get("dn", [])
    if not xt_values:
        return None

    xt_value = xt_values[0]
    if not xt_value.startswith("urn:btih:"):
        return None
    info_hash = xt_value.partition("urn:btih:")[2].strip().lower()
    if not info_hash:
        return None

    raw_title = unquote_plus(dn_values[0]).strip() if dn_values else ""
    if not raw_title:
        title_match = re.search(r"<title>Download\s+(.+?)\s+Free Torrent", detail_html, re.IGNORECASE)
        raw_title = unescape(title_match.group(1)).strip() if title_match else ""
    if not raw_title:
        return None

    seeders: int | None = None
    leechers: int | None = None
    seed_leech_match = _SEED_LEECH_RE.search(detail_html)
    if seed_leech_match is not None:
        seeders = int(seed_leech_match.group(1))
        leechers = int(seed_leech_match.group(2))

    torrent_match = re.search(r'href="([^"]+\.torrent[^"]*)"', detail_html, re.IGNORECASE)
    download_url = unescape(torrent_match.group(1)) if torrent_match is not None else None

    return ScraperResult(
        title=raw_title,
        provider=RARBG_PLUGIN_NAME,
        magnet_url=magnet_url,
        download_url=download_url,
        info_hash=info_hash,
        seeders=seeders,
        leechers=leechers,
    )


class RarbgScraper:
    """Scrape RARBG-compatible HTML results and normalize magnet payloads."""

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.ctx: PluginContext | None = None
        self._transport = transport
        self.base_url = _DEFAULT_RARBG_BASE_URL
        self.timeout_seconds = _DEFAULT_TIMEOUT_SECONDS
        self.enabled = True

    async def initialize(self, ctx: PluginContext) -> None:
        self.ctx = ctx
        block = _rarbg_settings(ctx.settings)
        self.base_url = str(block.get("url") or _DEFAULT_RARBG_BASE_URL).rstrip("/")
        timeout_value = block.get("timeout")
        if isinstance(timeout_value, (int, float)) and timeout_value > 0:
            self.timeout_seconds = float(timeout_value)
        enabled_value = block.get("enabled")
        self.enabled = bool(enabled_value) if enabled_value is not None else True

    async def search(self, metadata: ScraperSearchInput) -> list[ScraperResult]:
        if self.ctx is None:
            raise RuntimeError("RarbgScraper must be initialized before search")
        if not self.enabled:
            return []

        query = (metadata.external_ids.imdb_id or metadata.query or metadata.title or "").strip()
        if not query:
            self.ctx.logger.warning(
                "plugin.scraper.rarbg.skipped",
                reason="query_missing",
                plugin=self.ctx.plugin_name,
            )
            return []

        await self.ctx.rate_limiter.acquire(
            _RARBG_SEARCH_BUCKET,
            _RARBG_RATE_LIMIT_CAPACITY,
            _RARBG_RATE_LIMIT_REFILL_PER_SECOND,
        )

        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=self.timeout_seconds,
                transport=self._transport,
            ) as client:
                response = await client.get(f"{self.base_url}/search?search={quote_plus(query)}")
            response.raise_for_status()
            search_html = response.text
        except httpx.HTTPError as exc:
            self.ctx.logger.warning(
                "plugin.scraper.rarbg.request_failed",
                plugin=self.ctx.plugin_name,
                error=str(exc),
                query=query,
            )
            return []

        detail_links = _extract_post_detail_links(search_html, base_url=self.base_url)[:5]
        if not detail_links:
            return []

        results: list[ScraperResult] = []
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=self.timeout_seconds,
            transport=self._transport,
        ) as client:
            for detail_url in detail_links:
                try:
                    detail_response = await client.get(detail_url)
                    detail_response.raise_for_status()
                except httpx.HTTPError:
                    continue
                parsed = _extract_result_from_detail(detail_response.text)
                if parsed is not None:
                    results.append(parsed)
        return results
