"""Built-in plugin implementations that ship with FilmuCore."""

from .comet import COMET_PLUGIN_NAME, CometScraper
from .listrr import LISTRR_PLUGIN_NAME, ListrrContentService
from .mdblist import MDBLIST_PLUGIN_NAME, MDBListContentService
from .notifications import NOTIFICATIONS_PLUGIN_NAME, WebhookNotificationPlugin
from .plex import PLEX_PLUGIN_NAME, PlexLibraryRefreshPlugin
from .prowlarr import PROWLARR_PLUGIN_NAME, ProwlarrScraper
from .rarbg import RARBG_PLUGIN_NAME, RarbgScraper
from .seerr import SEERR_PLUGIN_NAME, SeerrContentService
from .stream_control import STREAM_CONTROL_PLUGIN_NAME, HostStreamControlPlugin
from .stremthru import STREMTHRU_PLUGIN_NAME, StremThruDownloader
from .torrentio import TORRENTIO_PLUGIN_NAME, TorrentioScraper, build_example_manifest

__all__ = [
    "COMET_PLUGIN_NAME",
    "LISTRR_PLUGIN_NAME",
    "MDBLIST_PLUGIN_NAME",
    "NOTIFICATIONS_PLUGIN_NAME",
    "PLEX_PLUGIN_NAME",
    "PROWLARR_PLUGIN_NAME",
    "RARBG_PLUGIN_NAME",
    "SEERR_PLUGIN_NAME",
    "STREAM_CONTROL_PLUGIN_NAME",
    "STREMTHRU_PLUGIN_NAME",
    "TORRENTIO_PLUGIN_NAME",
    "CometScraper",
    "HostStreamControlPlugin",
    "ListrrContentService",
    "MDBListContentService",
    "PlexLibraryRefreshPlugin",
    "ProwlarrScraper",
    "RarbgScraper",
    "SeerrContentService",
    "StremThruDownloader",
    "TorrentioScraper",
    "WebhookNotificationPlugin",
    "build_example_manifest",
]
