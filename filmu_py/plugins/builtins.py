"""Programmatic registration helpers for built-in plugins."""

from __future__ import annotations

from collections.abc import Sequence

from filmu_py.plugins.builtin import (
    MDBLIST_PLUGIN_NAME,
    NOTIFICATIONS_PLUGIN_NAME,
    PROWLARR_PLUGIN_NAME,
    RARBG_PLUGIN_NAME,
    STREMTHRU_PLUGIN_NAME,
    TORRENTIO_PLUGIN_NAME,
    MDBListContentService,
    ProwlarrScraper,
    RarbgScraper,
    StremThruDownloader,
    TorrentioScraper,
    WebhookNotificationPlugin,
)
from filmu_py.plugins.context import PluginContextProvider
from filmu_py.plugins.manifest import PluginManifest
from filmu_py.plugins.registry import PluginCapabilityKind, PluginRegistry


def register_builtin_plugins(
    registry: PluginRegistry,
    *,
    context_provider: PluginContextProvider,
) -> tuple[str, ...]:
    """Register built-in capability plugins without filesystem manifest discovery."""

    registered: list[str] = []
    seen_plugin_names: set[str] = set()
    for manifest, capability_kind, candidate in _builtin_capability_definitions():
        registry.register_manifest(manifest)
        context = context_provider.build(manifest.name, datasource_name=manifest.datasource)
        added, _skipped = registry.safe_register_capability(
            plugin_name=manifest.name,
            kind=capability_kind,
            candidate=candidate,
            context=context,
        )
        if added and manifest.name not in seen_plugin_names:
            seen_plugin_names.add(manifest.name)
            registered.append(manifest.name)
    return tuple(registered)


def _manifest(**payload: object) -> PluginManifest:
    return PluginManifest.model_validate(payload)


def _builtin_capability_definitions() -> Sequence[tuple[PluginManifest, PluginCapabilityKind, object]]:
    return (
        (
            _manifest(
                name=TORRENTIO_PLUGIN_NAME,
                version="1.0.0",
                api_version="1",
                capabilities=["scraper"],
                entry_module="plugin.py",
                scraper="TorrentioScraper",
            ),
            PluginCapabilityKind.SCRAPER,
            TorrentioScraper,
        ),
        (
            _manifest(
                name=PROWLARR_PLUGIN_NAME,
                version="1.0.0",
                api_version="1",
                capabilities=["scraper"],
                entry_module="plugin.py",
                scraper="ProwlarrScraper",
            ),
            PluginCapabilityKind.SCRAPER,
            ProwlarrScraper,
        ),
        (
            _manifest(
                name=RARBG_PLUGIN_NAME,
                version="1.0.0",
                api_version="1",
                capabilities=["scraper"],
                entry_module="plugin.py",
                scraper="RarbgScraper",
            ),
            PluginCapabilityKind.SCRAPER,
            RarbgScraper,
        ),
        (
            _manifest(
                name=MDBLIST_PLUGIN_NAME,
                version="1.0.0",
                api_version="1",
                capabilities=["content_service"],
                entry_module="plugin.py",
                content_service="MDBListContentService",
                datasource="host",
                publishable_events=["mdblist.scan.completed", "mdblist.error"],
            ),
            PluginCapabilityKind.CONTENT_SERVICE,
            MDBListContentService,
        ),
        (
            _manifest(
                name=STREMTHRU_PLUGIN_NAME,
                version="1.0.0",
                api_version="1",
                capabilities=["downloader"],
                entry_module="plugin.py",
                downloader="StremThruDownloader",
                datasource="host",
                publishable_events=["stremthru.download.queued", "stremthru.error"],
            ),
            PluginCapabilityKind.DOWNLOADER,
            StremThruDownloader,
        ),
        (
            _manifest(
                name=NOTIFICATIONS_PLUGIN_NAME,
                version="1.0.0",
                api_version="1",
                capabilities=["notification"],
                entry_module="plugin.py",
                notification="WebhookNotificationPlugin",
                datasource="host",
                publishable_events=["notifications.sent", "notifications.error"],
            ),
            PluginCapabilityKind.NOTIFICATION,
            WebhookNotificationPlugin,
        ),
        (
            _manifest(
                name=NOTIFICATIONS_PLUGIN_NAME,
                version="1.0.0",
                api_version="1",
                capabilities=["notification", "event_hook"],
                entry_module="plugin.py",
                notification="WebhookNotificationPlugin",
                event_hook="WebhookNotificationPlugin",
                datasource="host",
                publishable_events=["notifications.sent", "notifications.error"],
            ),
            PluginCapabilityKind.EVENT_HOOK,
            WebhookNotificationPlugin,
        ),
    )
