"""Shared downloader-orchestration helpers for the ARQ worker pipeline."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import Any

from filmu_py.config import Settings
from filmu_py.core.rate_limiter import DistributedRateLimiter
from filmu_py.plugins.registry import PluginRegistry
from filmu_py.services.debrid import (
    AllDebridPlaybackClient,
    DebridDownloadClient,
    DebridLinkPlaybackClient,
    PluginDownloaderClientAdapter,
    RealDebridPlaybackClient,
    TorrentInfo,
    build_download_manifest,
    filter_torrent_files,
)
from filmu_py.services.media import RankedStreamCandidateRecord

logger = logging.getLogger(__name__)


def build_provider_client(
    *, provider: str, api_key: str, limiter: DistributedRateLimiter
) -> DebridDownloadClient:
    if provider == "realdebrid":
        return RealDebridPlaybackClient(api_token=api_key, limiter=limiter)
    if provider == "alldebrid":
        return AllDebridPlaybackClient(api_token=api_key, limiter=limiter)
    if provider == "debridlink":
        return DebridLinkPlaybackClient(api_token=api_key, limiter=limiter)
    raise ValueError(f"unsupported_downloader_provider:{provider}")


def resolve_enabled_downloader(
    settings: Settings,
    *,
    item_id: str | None = None,
    item_request_id: str | None = None,
) -> str:
    """Return the highest-priority enabled builtin downloader."""

    provider_entries = (
        ("realdebrid", settings.downloaders.real_debrid),
        ("alldebrid", settings.downloaders.all_debrid),
        ("debridlink", settings.downloaders.debrid_link),
    )
    enabled_providers: list[str] = []
    for provider, config in provider_entries:
        api_key = config.api_key.strip()
        if config.enabled and api_key:
            enabled_providers.append(provider)

    if len(enabled_providers) > 1:
        logger.warning(
            "multiple downloaders enabled; selecting by fixed provider priority",
            extra={
                "item_id": item_id,
                "item_request_id": item_request_id,
                "enabled_providers": enabled_providers,
            },
        )
    if enabled_providers:
        return enabled_providers[0]
    raise ValueError("no_enabled_downloader")


def configured_builtin_downloader_providers(settings: Settings) -> list[str]:
    """Return enabled builtin providers ordered by orchestration priority."""

    provider_entries = {
        "realdebrid": settings.downloaders.real_debrid,
        "alldebrid": settings.downloaders.all_debrid,
        "debridlink": settings.downloaders.debrid_link,
    }
    configured: list[str] = []
    for provider in settings.orchestration.downloader_provider_priority:
        config = provider_entries.get(provider)
        if config is None:
            continue
        if config.enabled and config.api_key.strip():
            configured.append(provider)
    return configured


def configured_plugin_downloader_providers(
    settings: Settings,
    *,
    plugin_registry: PluginRegistry,
) -> list[tuple[str, DebridDownloadClient]]:
    """Return enabled plugin downloader adapters ordered by orchestration policy."""

    plugin_by_name = {
        str(getattr(plugin, "plugin_name", type(plugin).__name__)): plugin
        for plugin in plugin_registry.get_downloaders()
    }
    ordered_names: list[str] = []
    for provider in settings.orchestration.downloader_provider_priority:
        if provider in plugin_by_name and provider not in ordered_names:
            ordered_names.append(provider)
    for provider in plugin_by_name:
        if provider not in ordered_names:
            ordered_names.append(provider)

    configured: list[tuple[str, DebridDownloadClient]] = []
    for provider in ordered_names:
        plugin = plugin_by_name[provider]
        if provider == "stremthru":
            stremthru_settings = settings.downloaders.stremthru
            if not (
                stremthru_settings.enabled
                and bool(stremthru_settings.token.strip())
                and bool(stremthru_settings.url.strip())
            ):
                continue
        configured.append(
            (
                provider,
                PluginDownloaderClientAdapter(provider=provider, plugin=plugin),
            )
        )
    return configured


def resolve_downloader_api_key(settings: Settings, *, provider: str) -> str:
    provider_entries = {
        "realdebrid": settings.downloaders.real_debrid,
        "alldebrid": settings.downloaders.all_debrid,
        "debridlink": settings.downloaders.debrid_link,
    }
    try:
        config = provider_entries[provider]
    except KeyError as exc:
        raise ValueError(f"unsupported_downloader_provider:{provider}") from exc

    api_key = config.api_key.strip()
    if not api_key:
        raise ValueError(f"missing_downloader_api_key:{provider}")
    return api_key


def build_dead_letter_metadata(
    *,
    provider: str | None,
    item_request_id: str | None,
    selected_stream_id: str | None,
    failure_kind: str,
    status_code: int | None = None,
    retry_after_seconds: int | None = None,
) -> dict[str, object]:
    """Build normalized downloader/debrid dead-letter metadata for retained evidence."""

    metadata: dict[str, object] = {
        "provider": provider or "",
        "item_request_id": item_request_id or "",
        "selected_stream_id": selected_stream_id or "",
        "failure_kind": failure_kind,
    }
    if status_code is not None:
        metadata["status_code"] = int(status_code)
    if retry_after_seconds is not None:
        metadata["retry_after_seconds"] = int(retry_after_seconds)
    return metadata


def resolve_download_clients(
    *,
    settings: Settings,
    limiter: DistributedRateLimiter,
    plugin_registry: PluginRegistry,
    provider_client_builder: Callable[..., DebridDownloadClient] | None = None,
    item_id: str | None = None,
    item_request_id: str | None = None,
) -> list[tuple[str, DebridDownloadClient]]:
    """Resolve ordered downloader candidates from builtin and plugin-backed providers."""

    candidate_by_provider: dict[str, DebridDownloadClient] = {}
    discovery_order: list[str] = []

    builder = provider_client_builder or build_provider_client
    for provider in configured_builtin_downloader_providers(settings):
        api_key = resolve_downloader_api_key(settings, provider=provider)
        candidate_by_provider[provider] = builder(
            provider=provider,
            api_key=api_key,
            limiter=limiter,
        )
        discovery_order.append(provider)

    for provider, client in configured_plugin_downloader_providers(
        settings,
        plugin_registry=plugin_registry,
    ):
        if provider in candidate_by_provider:
            continue
        candidate_by_provider[provider] = client
        discovery_order.append(provider)

    ordered_names: list[str] = []
    for provider in settings.orchestration.downloader_provider_priority:
        if provider in candidate_by_provider and provider not in ordered_names:
            ordered_names.append(provider)
    for provider in discovery_order:
        if provider not in ordered_names:
            ordered_names.append(provider)
    candidates = [(provider, candidate_by_provider[provider]) for provider in ordered_names]

    if len(candidates) > 1 and settings.orchestration.downloader_selection_mode == "fixed_priority":
        logger.warning(
            "multiple downloaders enabled; selecting by fixed provider priority",
            extra={
                "item_id": item_id,
                "item_request_id": item_request_id,
                "enabled_providers": [provider for provider, _client in candidates],
            },
        )
        return [candidates[0]]

    attempt_limit = settings.orchestration.downloader_provider_attempt_limit
    bounded = candidates[:attempt_limit]
    if len(bounded) > 1:
        logger.info(
            "resolved downloader candidates from orchestration policy",
            extra={
                "item_id": item_id,
                "item_request_id": item_request_id,
                "providers": [provider for provider, _client in bounded],
                "selection_mode": settings.orchestration.downloader_selection_mode,
            },
    )
    return bounded


def resolve_download_client(
    *,
    settings: Settings,
    limiter: DistributedRateLimiter,
    plugin_registry: PluginRegistry,
    provider_client_builder: Callable[..., DebridDownloadClient] | None = None,
    item_id: str | None = None,
    item_request_id: str | None = None,
) -> tuple[str, DebridDownloadClient]:
    """Resolve the active debrid client from the ordered orchestration candidates."""

    candidates = resolve_download_clients(
        settings=settings,
        limiter=limiter,
        plugin_registry=plugin_registry,
        provider_client_builder=provider_client_builder,
        item_id=item_id,
        item_request_id=item_request_id,
    )
    if candidates:
        return candidates[0]
    raise ValueError("no_enabled_downloader")


def rank_failure_cooldown_seconds(
    settings: Settings,
    *,
    attempt_count: int,
    use_short_first_retry: bool = True,
) -> int:
    if use_short_first_retry and attempt_count < 2:
        return 300
    if attempt_count < 5:
        return max(0, int(settings.scraping.after_2 * 3600))
    if attempt_count < 10:
        return max(0, int(settings.scraping.after_5 * 3600))
    return max(0, int(settings.scraping.after_10 * 3600))


async def execute_debrid_download(
    *,
    client: DebridDownloadClient,
    provider: str,
    infohash: str,
    settings: Settings,
    item_id: str,
    item_request_id: str | None,
    stage_logger: Any,
) -> tuple[str, TorrentInfo, list[str]]:
    """Run one downloader candidate through add/select/poll/link resolution."""

    magnet_url = f"magnet:?xt=urn:btih:{infohash}".lower()
    logger.info(
        "debrid stage starting",
        extra={"item_id": item_id, "item_request_id": item_request_id, "provider": provider},
    )

    provider_torrent_id = await client.add_magnet(magnet_url)
    initial_info = await client.get_torrent_info(provider_torrent_id)
    initial_selected = filter_torrent_files(initial_info.files, settings.downloaders)
    if initial_selected:
        try:
            await client.select_files(provider_torrent_id, [file.file_id for file in initial_selected])
        except Exception as exc:
            stage_logger.debug(
                "debrid_item.pre_poll_select_skipped",
                item_id=item_id,
                provider=provider,
                reason=str(exc),
            )

    timeout_at = asyncio.get_running_loop().time() + 300.0
    torrent_info = await client.get_torrent_info(provider_torrent_id)
    while torrent_info.status not in {"downloaded", "ready", "download_ready"}:
        if asyncio.get_running_loop().time() >= timeout_at:
            raise TimeoutError("debrid_poll_timeout")
        mid_selected = filter_torrent_files(torrent_info.files, settings.downloaders)
        if mid_selected:
            with contextlib.suppress(Exception):
                await client.select_files(provider_torrent_id, [file.file_id for file in mid_selected])
        await asyncio.sleep(2.0)
        torrent_info = await client.get_torrent_info(provider_torrent_id)

    selected_files = filter_torrent_files(torrent_info.files, settings.downloaders)
    if not selected_files:
        raise ValueError("no_downloadable_files")
    await client.select_files(provider_torrent_id, [file.file_id for file in selected_files])
    refreshed_info = await client.get_torrent_info(provider_torrent_id)
    download_urls = await client.get_download_links(provider_torrent_id)
    manifest = build_download_manifest(
        refreshed_info.files,
        download_urls=download_urls,
        settings=settings.downloaders,
    )
    if not manifest.files:
        raise ValueError("no_downloadable_files")
    if manifest.unresolved_file_count > 0:
        raise ValueError("download_manifest_incomplete")
    if manifest.duplicate_path_count > 0:
        raise ValueError("download_manifest_duplicate_paths")
    validated_info = TorrentInfo(
        provider_torrent_id=refreshed_info.provider_torrent_id,
        status=refreshed_info.status,
        name=getattr(refreshed_info, "name", None),
        info_hash=getattr(refreshed_info, "info_hash", None),
        files=manifest.files,
        links=manifest.download_urls,
    )
    stage_logger.info(
        "debrid_item.manifest_validated",
        item_id=item_id,
        item_request_id=item_request_id,
        provider=provider,
        file_count=len(manifest.files),
        multi_container=manifest.multi_container,
        container_roots=list(manifest.container_roots),
    )
    return provider_torrent_id, validated_info, manifest.download_urls


def should_failover_downloader(
    settings: Settings,
    *,
    remaining_candidates: int,
    error_kind: str,
) -> bool:
    """Return whether downloader orchestration should continue to the next candidate."""

    if remaining_candidates <= 0:
        return False
    if settings.orchestration.downloader_selection_mode != "ordered_failover":
        return False
    if error_kind == "rate_limit":
        return settings.orchestration.downloader_failover_on_rate_limit
    return settings.orchestration.downloader_failover_on_provider_error


def selection_failure_reason(
    ranked_streams: list[RankedStreamCandidateRecord],
    selected_stream_id: str | None,
) -> str:
    """Return a stable failure reason for scraped-item selection failures."""

    if selected_stream_id is not None:
        return "selected_stream_unavailable"
    if not ranked_streams:
        return "no_stream_candidates"
    return "no_passing_stream_candidates"


def build_rank_no_winner_diagnostics(
    *,
    scraped_candidate_count: int | None,
    parsed_stream_count: int,
    ranked_results: list[RankedStreamCandidateRecord],
    rank_threshold: int,
) -> dict[str, object]:
    from collections import Counter

    passing_fetch_count = sum(1 for record in ranked_results if record.fetch)
    above_threshold_count = sum(1 for record in ranked_results if record.rank_score >= rank_threshold)
    rejection_reasons = Counter(
        record.rejection_reason for record in ranked_results if not record.fetch and record.rejection_reason
    )

    if scraped_candidate_count == 0:
        failure_reason = "no_candidates_scraped"
    elif parsed_stream_count == 0:
        failure_reason = "no_candidates_parsed"
    elif passing_fetch_count == 0:
        failure_reason = "no_candidates_passing_fetch"
    elif above_threshold_count == 0:
        failure_reason = "no_candidates_above_threshold"
    else:
        failure_reason = "unknown"

    return {
        "scraped_candidate_count": scraped_candidate_count,
        "parsed_stream_count": parsed_stream_count,
        "passing_fetch_count": passing_fetch_count,
        "above_threshold_count": above_threshold_count,
        "failure_reason": failure_reason,
        "rejection_reasons": dict(rejection_reasons.most_common()),
    }
