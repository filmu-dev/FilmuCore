"""Built-in plugin implementations that ship with FilmuCore."""

from .mdblist import MDBLIST_PLUGIN_NAME, MDBListContentService
from .notifications import NOTIFICATIONS_PLUGIN_NAME, WebhookNotificationPlugin
from .prowlarr import PROWLARR_PLUGIN_NAME, ProwlarrScraper
from .rarbg import RARBG_PLUGIN_NAME, RarbgScraper
from .stream_control import STREAM_CONTROL_PLUGIN_NAME, HostStreamControlPlugin
from .stremthru import STREMTHRU_PLUGIN_NAME, StremThruDownloader
from .torrentio import TORRENTIO_PLUGIN_NAME, TorrentioScraper, build_example_manifest

__all__ = [
    "MDBLIST_PLUGIN_NAME",
    "NOTIFICATIONS_PLUGIN_NAME",
    "PROWLARR_PLUGIN_NAME",
    "RARBG_PLUGIN_NAME",
    "STREAM_CONTROL_PLUGIN_NAME",
    "STREMTHRU_PLUGIN_NAME",
    "TORRENTIO_PLUGIN_NAME",
    "HostStreamControlPlugin",
    "MDBListContentService",
    "ProwlarrScraper",
    "RarbgScraper",
    "StremThruDownloader",
    "TorrentioScraper",
    "WebhookNotificationPlugin",
    "build_example_manifest",
]
