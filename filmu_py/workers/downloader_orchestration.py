"""Shared downloader-orchestration helpers for the ARQ worker pipeline."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
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
    TorrentDownloadContainerCandidate,
    TorrentDownloadValidationError,
    TorrentInfo,
    build_download_container_candidates,
    filter_torrent_files,
)
from filmu_py.services.media import RankedStreamCandidateRecord

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DebridDownloadExecutionSuccess:
    """One validated provider/container execution chosen for persistence."""

    provider: str
    provider_priority: int
    provider_torrent_id: str
    torrent_info: TorrentInfo
    download_urls: tuple[str, ...]
    container_root: str | None = None
    matched_file_ids: tuple[str, ...] = ()
    selected_file_ids: tuple[str, ...] = ()
    fanout_parallelism: int = 1
    attempted_providers: tuple[str, ...] = ()
    successful_providers: tuple[str, ...] = ()
    failed_providers: tuple[str, ...] = ()
    provider_failure_types: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class DebridDownloadExecutionFailure:
    """One attributed provider execution failure used for deterministic reconciliation."""

    provider: str
    provider_priority: int
    error: Exception


def _container_candidate_sort_key(
    candidate: TorrentDownloadContainerCandidate,
) -> tuple[int, int, int, str]:
    return (
        -len(candidate.validation.matched_file_ids),
        -len(candidate.validation.manifest.files),
        candidate.candidate_rank,
        candidate.container_root or "",
    )


def _download_success_sort_key(
    success: DebridDownloadExecutionSuccess,
) -> tuple[int, int, int, str, str]:
    return (
        -len(success.matched_file_ids),
        success.provider_priority,
        -len(success.selected_file_ids),
        success.container_root or "",
        success.provider,
    )


def _download_failure_sort_key(
    failure: DebridDownloadExecutionFailure,
) -> tuple[int, str]:
    return (failure.provider_priority, failure.provider)


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
    retry_after_seconds: float | int | None = None,
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
    provider_priority: int,
    infohash: str,
    settings: Settings,
    item_id: str,
    item_request_id: str | None,
    selected_stream_parsed_title: dict[str, object] | None,
    stage_logger: Any,
) -> DebridDownloadExecutionSuccess:
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
    container_candidates = build_download_container_candidates(
        refreshed_info.files,
        download_urls=download_urls,
        settings=settings.downloaders,
        expected_parsed_title=selected_stream_parsed_title,
    )
    valid_candidates = [
        candidate for candidate in container_candidates if candidate.validation.ok
    ]
    if not valid_candidates:
        rejected_candidate = sorted(container_candidates, key=_container_candidate_sort_key)[0]
        manifest = rejected_candidate.validation.manifest
        stage_logger.warning(
            "debrid_item.manifest_rejected",
            item_id=item_id,
            item_request_id=item_request_id,
            provider=provider,
            rejection_reason=rejected_candidate.validation.rejection_reason,
            selected_file_ids=list(rejected_candidate.validation.selected_file_ids),
            matched_file_ids=list(rejected_candidate.validation.matched_file_ids),
            container_roots=list(manifest.container_roots),
            container_variant_count=len(container_candidates),
            container_variants=[
                {
                    "container_root": candidate.container_root,
                    "matched_file_ids": list(candidate.validation.matched_file_ids),
                    "selected_file_ids": list(candidate.validation.selected_file_ids),
                    "rejection_reason": candidate.validation.rejection_reason,
                }
                for candidate in container_candidates
            ],
            selected_file_evidence=[
                {
                    "file_id": evidence.file_id,
                    "file_path": evidence.file_path or evidence.file_name,
                    "media_type": evidence.media_type,
                    "container_root": evidence.container_root,
                    "scope_season": evidence.scope_season,
                    "scope_episodes": list(evidence.scope_episodes),
                    "matched_expected_content": evidence.matched_expected_content,
                    "match_reason": evidence.match_reason,
                    "download_url_present": evidence.download_url is not None,
                }
                for evidence in rejected_candidate.validation.selected_file_evidence
            ],
        )
        raise TorrentDownloadValidationError(rejected_candidate.validation)
    selected_candidate = sorted(valid_candidates, key=_container_candidate_sort_key)[0]
    validation = selected_candidate.validation
    manifest = validation.manifest
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
        container_variant_count=len(container_candidates),
        selected_container_root=selected_candidate.container_root,
        matched_file_ids=list(validation.matched_file_ids),
        candidate_provider_priority=provider_priority,
        selected_file_evidence=[
            {
                "file_id": evidence.file_id,
                "file_path": evidence.file_path or evidence.file_name,
                "media_type": evidence.media_type,
                "container_root": evidence.container_root,
                "scope_season": evidence.scope_season,
                "scope_episodes": list(evidence.scope_episodes),
                "matched_expected_content": evidence.matched_expected_content,
                "match_reason": evidence.match_reason,
            }
            for evidence in validation.selected_file_evidence
        ],
    )
    return DebridDownloadExecutionSuccess(
        provider=provider,
        provider_priority=provider_priority,
        provider_torrent_id=provider_torrent_id,
        torrent_info=validated_info,
        download_urls=tuple(manifest.download_urls),
        container_root=selected_candidate.container_root,
        matched_file_ids=validation.matched_file_ids,
        selected_file_ids=validation.selected_file_ids,
    )


async def execute_debrid_download_fanout(
    *,
    provider_candidates: list[tuple[str, DebridDownloadClient]],
    infohash: str,
    settings: Settings,
    item_id: str,
    item_request_id: str | None,
    selected_stream_parsed_title: dict[str, object] | None,
    stage_logger: Any,
) -> DebridDownloadExecutionSuccess:
    """Run bounded provider fan-out and deterministically reconcile to one winner."""

    if not provider_candidates:
        raise ValueError("no_enabled_downloader")

    parallelism = min(
        settings.orchestration.downloader_provider_parallelism,
        len(provider_candidates),
    )
    if settings.orchestration.downloader_selection_mode == "fixed_priority":
        parallelism = 1

    semaphore = asyncio.Semaphore(parallelism)

    async def _run_candidate(
        provider_priority: int,
        provider: str,
        client: DebridDownloadClient,
    ) -> DebridDownloadExecutionSuccess | DebridDownloadExecutionFailure:
        async with semaphore:
            try:
                return await execute_debrid_download(
                    client=client,
                    provider=provider,
                    provider_priority=provider_priority,
                    infohash=infohash,
                    settings=settings,
                    item_id=item_id,
                    item_request_id=item_request_id,
                    selected_stream_parsed_title=selected_stream_parsed_title,
                    stage_logger=stage_logger,
                )
            except Exception as exc:
                return DebridDownloadExecutionFailure(
                    provider=provider,
                    provider_priority=provider_priority,
                    error=exc,
                )

    results = await asyncio.gather(
        *[
            _run_candidate(index, provider, client)
            for index, (provider, client) in enumerate(provider_candidates)
        ]
    )
    successes = [
        result for result in results if isinstance(result, DebridDownloadExecutionSuccess)
    ]
    failures = [
        result for result in results if isinstance(result, DebridDownloadExecutionFailure)
    ]

    if successes:
        selected = sorted(successes, key=_download_success_sort_key)[0]
        attempted_providers = tuple(provider for provider, _client in provider_candidates)
        successful_providers = tuple(result.provider for result in successes)
        failed_providers = tuple(failure.provider for failure in failures)
        provider_failure_types = tuple(
            (failure.provider, type(failure.error).__name__)
            for failure in sorted(failures, key=_download_failure_sort_key)
        )
        stage_logger.info(
            "debrid_item.fanout_reconciled",
            item_id=item_id,
            item_request_id=item_request_id,
            parallelism=parallelism,
            attempted_providers=list(attempted_providers),
            successful_providers=list(successful_providers),
            failed_providers=list(failed_providers),
            selected_provider=selected.provider,
            selected_provider_priority=selected.provider_priority,
            selected_container_root=selected.container_root,
            matched_file_ids=list(selected.matched_file_ids),
            provider_failure_types=[
                {"provider": provider, "error_type": error_type}
                for provider, error_type in provider_failure_types
            ],
        )
        return DebridDownloadExecutionSuccess(
            provider=selected.provider,
            provider_priority=selected.provider_priority,
            provider_torrent_id=selected.provider_torrent_id,
            torrent_info=selected.torrent_info,
            download_urls=selected.download_urls,
            container_root=selected.container_root,
            matched_file_ids=selected.matched_file_ids,
            selected_file_ids=selected.selected_file_ids,
            fanout_parallelism=parallelism,
            attempted_providers=attempted_providers,
            successful_providers=successful_providers,
            failed_providers=failed_providers,
            provider_failure_types=provider_failure_types,
        )

    selected_failure = sorted(failures, key=_download_failure_sort_key)[0]
    stage_logger.warning(
        "debrid_item.fanout_exhausted",
        item_id=item_id,
        item_request_id=item_request_id,
        parallelism=parallelism,
        attempted_providers=[provider for provider, _client in provider_candidates],
        failed_providers=[failure.provider for failure in failures],
        selected_failure_provider=selected_failure.provider,
        selected_failure_type=type(selected_failure.error).__name__,
    )
    raise selected_failure.error


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
