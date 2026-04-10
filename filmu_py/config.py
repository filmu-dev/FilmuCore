"""Application settings and compatibility-schema translation for filmu-python."""

from __future__ import annotations

import os
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

from dotenv import dotenv_values
from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from filmu_py.rtn.schemas import RankingProfile

DEFAULT_POSTGRES_DSN = "postgresql+asyncpg://postgres:postgres@localhost:5432/filmu"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"

TModel = TypeVar("TModel", bound=BaseModel)
_DEBRID_ENV_FALLBACKS: tuple[tuple[str, str, tuple[str, ...], str], ...] = (
    (
        "realdebrid_api_token",
        "FILMU_PY_REALDEBRID_API_TOKEN",
        ("REAL_DEBRID_API_KEY", "REALDEBRID_API_KEY"),
        "real_debrid",
    ),
    (
        "alldebrid_api_token",
        "FILMU_PY_ALLDEBRID_API_TOKEN",
        ("ALL_DEBRID_API_KEY", "ALLDEBRID_API_KEY"),
        "all_debrid",
    ),
    (
        "debridlink_api_token",
        "FILMU_PY_DEBRIDLINK_API_TOKEN",
        ("DEBRID_LINK_API_KEY", "DEBRIDLINK_API_KEY"),
        "debrid_link",
    ),
)


def _coerce_model[TModel: BaseModel](
    value: TModel | dict[str, Any], model_type: type[TModel]
) -> TModel:
    """Return one validated model instance from either a model or mapping."""

    if isinstance(value, model_type):
        return value
    return model_type.model_validate(value)


def _compat_dump(model: BaseModel) -> dict[str, Any]:
    """Return one compatibility-safe nested dict with omitted `None` fields."""

    return model.model_dump(mode="python", exclude_none=True)


def _build_default_ranking_settings() -> dict[str, object]:
    """Return the compatibility-default ranking block used when no explicit ranking exists."""

    category_keys = {
        "quality": [
            "av1",
            "avc",
            "bluray",
            "dvd",
            "hdtv",
            "hevc",
            "mpeg",
            "remux",
            "vhs",
            "web",
            "webdl",
            "webmux",
            "xvid",
        ],
        "rips": [
            "bdrip",
            "brrip",
            "dvdrip",
            "hdrip",
            "ppvrip",
            "satrip",
            "tvrip",
            "uhdrip",
            "vhsrip",
            "webdlrip",
            "webrip",
        ],
        "hdr": ["bit10", "dolby_vision", "hdr", "hdr10plus", "sdr"],
        "audio": [
            "aac",
            "atmos",
            "dolby_digital",
            "dolby_digital_plus",
            "dts_lossless",
            "dts_lossy",
            "flac",
            "mono",
            "mp3",
            "stereo",
            "surround",
            "truehd",
        ],
        "extras": [
            "three_d",
            "converted",
            "documentary",
            "dubbed",
            "edition",
            "hardcoded",
            "network",
            "proper",
            "repack",
            "retail",
            "scene",
            "site",
            "subbed",
            "uncensored",
            "upscaled",
        ],
        "trash": [
            "cam",
            "clean_audio",
            "pdtv",
            "r5",
            "screener",
            "size",
            "telecine",
            "telesync",
        ],
    }
    return {
        "name": "default",
        "enabled": True,
        "require": [],
        "exclude": [],
        "preferred": [],
        "resolutions": {
            "r2160p": True,
            "r1080p": True,
            "r720p": True,
            "r480p": True,
            "r360p": True,
            "unknown": True,
        },
        "options": {
            "title_similarity": 0.85,
            "remove_all_trash": True,
            "remove_ranks_under": -10000,
            "remove_unknown_languages": False,
            "allow_english_in_languages": True,
            "enable_fetch_speed_mode": False,
            "remove_adult_content": True,
        },
        "languages": {"required": [], "allowed": [], "exclude": [], "preferred": []},
        "custom_ranks": {
            category: {key: {"fetch": True, "use_custom_rank": False, "rank": 0} for key in keys}
            for category, keys in category_keys.items()
        },
    }


def build_default_ranking_profile() -> RankingProfile:
    """Return the internal typed ranking profile default."""

    return RankingProfile.from_settings_dict(_build_default_ranking_settings())


class CompatibilityModel(BaseModel):
    """Base model for compatibility-backed nested settings blocks."""

    model_config = ConfigDict(extra="ignore")


class LibraryFilterRules(CompatibilityModel):
    """Optional rule set used by one filesystem library profile."""

    is_anime: bool | None = None
    content_types: list[str] | None = None
    genres: list[str] | None = None
    max_rating: float | None = None
    content_ratings: list[str] | None = None


class LibraryProfile(CompatibilityModel):
    """One compatibility library profile definition."""

    name: str
    library_path: str
    enabled: bool = True
    filter_rules: LibraryFilterRules = Field(default_factory=LibraryFilterRules)


class FilesystemSettings(CompatibilityModel):
    """Filesystem compatibility block matching the original settings surface."""

    mount_path: str = "/mnt/rivenfs"
    library_profiles: dict[str, LibraryProfile] = Field(default_factory=dict)
    cache_dir: str = "/dev/shm/riven-cache"
    cache_max_size_mb: int = 12600
    cache_ttl_seconds: int = 3600
    cache_eviction: str = "LRU"
    cache_metrics: bool = True
    movie_dir_template: str = "{title} ({year}) {{tmdb-{tmdb_id}}}"
    movie_file_template: str = "{title} ({year})"
    show_dir_template: str = "{title} ({year}) {{tvdb-{tvdb_id}}}"
    season_dir_template: str = "Season {season:02d}"
    episode_file_template: str = "{show[title]} - s{season:02d}e{episode:02d}"


class PlexUpdaterConfig(CompatibilityModel):
    """Plex library updater settings."""

    enabled: bool = False
    token: str = ""
    url: str = "http://localhost:32400"


class JellyfinUpdaterConfig(CompatibilityModel):
    """Jellyfin library updater settings."""

    enabled: bool = False
    api_key: str = ""
    url: str = "http://localhost:8096"


class EmbyUpdaterConfig(CompatibilityModel):
    """Emby library updater settings."""

    enabled: bool = False
    api_key: str = ""
    url: str = "http://localhost:8096"


class UpdatersSettings(CompatibilityModel):
    """Compatibility updater settings surface."""

    updater_interval: int = 90
    library_path: str = "/mnt/filmuvfs"
    plex: PlexUpdaterConfig = Field(default_factory=PlexUpdaterConfig)
    jellyfin: JellyfinUpdaterConfig = Field(default_factory=JellyfinUpdaterConfig)
    emby: EmbyUpdaterConfig = Field(default_factory=EmbyUpdaterConfig)


class ProviderConfig(CompatibilityModel):
    """One provider block from the original frontend `downloaders` settings surface."""

    enabled: bool = False
    api_key: str = ""


class StremThruSettings(CompatibilityModel):
    """StremThru downloader settings supporting hosted and self-hosted store endpoints."""

    enabled: bool = False
    url: str = "https://stremthru.com"
    token: str = ""


class DownloadersSettings(CompatibilityModel):
    """Runtime downloader settings matching the original `settings.json` downloaders block."""

    video_extensions: list[str] = Field(default_factory=lambda: ["mp4", "mkv", "avi"])
    movie_filesize_mb_min: int = 700
    movie_filesize_mb_max: int = -1
    episode_filesize_mb_min: int = 100
    episode_filesize_mb_max: int = -1
    proxy_url: str = ""
    real_debrid: ProviderConfig = Field(default_factory=ProviderConfig)
    debrid_link: ProviderConfig = Field(default_factory=ProviderConfig)
    all_debrid: ProviderConfig = Field(default_factory=ProviderConfig)
    stremthru: StremThruSettings = Field(default_factory=StremThruSettings)


class OverseerrConfig(CompatibilityModel):
    """Overseerr request-source settings."""

    update_interval: int = 60
    enabled: bool = False
    url: str = ""
    api_key: str = ""
    use_webhook: bool = False


class PlexWatchlistConfig(CompatibilityModel):
    """Plex watchlist content-source settings."""

    update_interval: int = 60
    enabled: bool = False
    rss: list[str] = Field(default_factory=list)


class MdblistConfig(CompatibilityModel):
    """MDBList content-source settings."""

    enabled: bool = False
    api_key: str = ""
    list_ids: list[str] = Field(default_factory=list)
    poll_interval_minutes: int = 60

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_keys(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        payload = dict(data)
        if "list_ids" not in payload and isinstance(payload.get("lists"), list):
            payload["list_ids"] = payload["lists"]
        if (
            "poll_interval_minutes" not in payload
            and isinstance(payload.get("update_interval"), int)
            and payload["update_interval"] >= 0
        ):
            payload["poll_interval_minutes"] = max(1, payload["update_interval"] // 60)
        return payload


class ListrrConfig(CompatibilityModel):
    """Listrr content-source settings."""

    update_interval: int = 86400
    enabled: bool = False
    movie_lists: list[str] = Field(default_factory=list)
    show_lists: list[str] = Field(default_factory=list)
    api_key: str = ""


class TraktOAuth(CompatibilityModel):
    """Nested trakt OAuth token/configuration block."""

    oauth_client_id: str = ""
    oauth_client_secret: str = ""
    oauth_redirect_uri: str = ""
    access_token: str = ""
    refresh_token: str = ""


class TraktConfig(CompatibilityModel):
    """Trakt content-source settings."""

    update_interval: int = 86400
    enabled: bool = False
    api_key: str = ""
    watchlist: list[str] = Field(default_factory=list)
    user_lists: list[str] = Field(default_factory=list)
    collection: list[str] = Field(default_factory=list)
    fetch_trending: bool = False
    trending_count: int = 10
    fetch_popular: bool = False
    popular_count: int = 10
    fetch_most_watched: bool = False
    most_watched_period: str = "weekly"
    most_watched_count: int = 10
    oauth: TraktOAuth = Field(default_factory=TraktOAuth)
    proxy_url: str = ""


class ContentSettings(CompatibilityModel):
    """Compatibility content-request/source settings surface."""

    overseerr: OverseerrConfig = Field(default_factory=OverseerrConfig)
    plex_watchlist: PlexWatchlistConfig = Field(default_factory=PlexWatchlistConfig)
    mdblist: MdblistConfig = Field(default_factory=MdblistConfig)
    listrr: ListrrConfig = Field(default_factory=ListrrConfig)
    trakt: TraktConfig = Field(default_factory=TraktConfig)


class ScraperBaseConfig(CompatibilityModel):
    """Shared fields present on most scraper compatibility blocks."""

    enabled: bool = False
    url: str = ""
    timeout: int = 30
    retries: int = 1
    ratelimit: bool = True


class TorrentioConfig(ScraperBaseConfig):
    """Torrentio scraper settings."""

    filter: str = "sort=qualitysize%7Cqualityfilter=480p,scr,cam"
    proxy_url: str = ""


class JackettConfig(ScraperBaseConfig):
    """Jackett scraper settings."""

    api_key: str = ""
    infohash_fetch_timeout: int = 30


class ProwlarrConfig(ScraperBaseConfig):
    """Prowlarr scraper settings."""

    api_key: str = ""
    infohash_fetch_timeout: int = 60
    limiter_seconds: int = 60


class OrionoidParameters(CompatibilityModel):
    """Nested Orionoid request parameters block."""

    video3d: bool = False
    videoquality: str = "sd_hd8k"
    limitcount: int = 5


class OrionoidConfig(CompatibilityModel):
    """Orionoid scraper settings preserving the original compatibility shape."""

    enabled: bool = False
    api_key: str = ""
    cached_results_only: bool = False
    parameters: OrionoidParameters = Field(default_factory=OrionoidParameters)
    timeout: int = 30
    retries: int = 1
    ratelimit: bool = True


class MediafusionConfig(ScraperBaseConfig):
    """MediaFusion scraper settings."""


class ZileanConfig(ScraperBaseConfig):
    """Zilean scraper settings."""


class CometConfig(ScraperBaseConfig):
    """Comet scraper settings."""


class RarbgConfig(ScraperBaseConfig):
    """RARBG scraper settings."""


class AioStreamsConfig(ScraperBaseConfig):
    """AioStreams scraper settings."""

    proxy_url: str = ""
    uuid: str = ""
    password: str = ""


class ScrapingSettings(CompatibilityModel):
    """Compatibility scraping settings surface."""

    after_2: float = 2.0
    after_5: float = 6.0
    after_10: float = 24.0
    ongoing_show_poll_interval_hours: int = 24
    enable_aliases: bool = True
    bucket_limit: int = 5
    max_failed_attempts: int = 0
    dubbed_anime_only: bool = False
    torrentio: TorrentioConfig = Field(default_factory=TorrentioConfig)
    jackett: JackettConfig = Field(default_factory=JackettConfig)
    prowlarr: ProwlarrConfig = Field(default_factory=ProwlarrConfig)
    orionoid: OrionoidConfig = Field(default_factory=OrionoidConfig)
    mediafusion: MediafusionConfig = Field(default_factory=MediafusionConfig)
    zilean: ZileanConfig = Field(default_factory=ZileanConfig)
    comet: CometConfig = Field(default_factory=CometConfig)
    rarbg: RarbgConfig = Field(default_factory=RarbgConfig)
    aiostreams: AioStreamsConfig = Field(default_factory=AioStreamsConfig)


class IndexerSettings(CompatibilityModel):
    """Indexer compatibility block."""

    schedule_offset_minutes: int = 30


class DatabaseSettings(CompatibilityModel):
    """Database compatibility block."""

    host: str = DEFAULT_POSTGRES_DSN


class NotificationsSettings(CompatibilityModel):
    """Notification compatibility block."""

    enabled: bool = True
    on_item_type: list[str] = Field(default_factory=lambda: ["movie", "show", "season", "episode"])
    service_urls: list[str] = Field(default_factory=list)
    discord_webhook_url: str | None = None
    webhook_url: str | None = None
    notify_on: list[str] | None = None


class OpensubtitlesConfig(CompatibilityModel):
    """OpenSubtitles provider settings."""

    enabled: bool = True


class SubtitleProviders(CompatibilityModel):
    """Subtitle providers compatibility block."""

    opensubtitles: OpensubtitlesConfig = Field(default_factory=OpensubtitlesConfig)


class SubtitleSettings(CompatibilityModel):
    """Subtitle post-processing settings."""

    enabled: bool = True
    languages: list[str] = Field(default_factory=lambda: ["eng", "por"])
    providers: SubtitleProviders = Field(default_factory=SubtitleProviders)


class PostProcessingSettings(CompatibilityModel):
    """Post-processing compatibility block."""

    subtitle: SubtitleSettings = Field(default_factory=SubtitleSettings)


class LoggingSettings(CompatibilityModel):
    """File/log-cleanup compatibility block."""

    enabled: bool = True
    clean_interval: int = 3600
    retention_hours: int = 24
    rotation_mb: int = 10
    retention_files: int = 7
    compression: str = "disabled"
    directory: str = "logs"
    structured_filename: str = "ecs.json"


class StreamSettings(CompatibilityModel):
    """Streaming compatibility block."""

    chunk_size_mb: int = 80
    connect_timeout_seconds: int = 30
    chunk_wait_timeout_seconds: int = 300
    activity_timeout_seconds: int = 360


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables and compatibility payloads."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        validate_assignment=True,
    )

    env: Literal["development", "staging", "production"] = Field(
        default="development",
        alias="FILMU_PY_ENV",
    )
    version: str = Field(default="0.1.0", alias="FILMU_PY_VERSION")
    service_name: str = Field(default="filmu-python", alias="FILMU_PY_SERVICE_NAME")
    host: str = Field(default="0.0.0.0", alias="FILMU_PY_HOST")
    port: int = Field(default=8080, alias="FILMU_PY_PORT")
    api_key: SecretStr = Field(alias="FILMU_PY_API_KEY")
    api_key_id: str = Field(default="primary", alias="FILMU_PY_API_KEY_ID")
    tmdb_api_key: str = Field(default="", alias="TMDB_API_KEY")
    log_level: str = Field(default="INFO", alias="FILMU_PY_LOG_LEVEL")
    enable_network_tracing: bool = Field(default=False, alias="FILMU_PY_ENABLE_NETWORK_TRACING")
    enable_stream_tracing: bool = Field(default=False, alias="FILMU_PY_ENABLE_STREAM_TRACING")
    retry_interval: int = Field(default=86400, alias="FILMU_PY_RETRY_INTERVAL")
    tracemalloc: bool = Field(default=False, alias="FILMU_PY_TRACEMALLOC")

    filesystem: FilesystemSettings = Field(
        default_factory=FilesystemSettings,
        alias="FILMU_PY_FILESYSTEM",
    )
    updaters: UpdatersSettings = Field(
        default_factory=UpdatersSettings,
        alias="FILMU_PY_UPDATERS",
    )
    downloaders: DownloadersSettings = Field(
        default_factory=DownloadersSettings,
        alias="FILMU_PY_DOWNLOADERS",
    )
    content: ContentSettings = Field(
        default_factory=ContentSettings,
        alias="FILMU_PY_CONTENT",
    )
    scraping: ScrapingSettings = Field(
        default_factory=ScrapingSettings,
        alias="FILMU_PY_SCRAPING",
    )
    ranking: RankingProfile = Field(
        default_factory=build_default_ranking_profile,
        alias="FILMU_PY_RANKING",
    )
    indexer: IndexerSettings = Field(
        default_factory=IndexerSettings,
        alias="FILMU_PY_INDEXER",
    )
    database: DatabaseSettings = Field(
        default_factory=DatabaseSettings,
        alias="FILMU_PY_DATABASE",
    )
    notifications: NotificationsSettings = Field(
        default_factory=NotificationsSettings,
        alias="FILMU_PY_NOTIFICATIONS",
    )
    post_processing: PostProcessingSettings = Field(
        default_factory=PostProcessingSettings,
        alias="FILMU_PY_POST_PROCESSING",
    )
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        alias="FILMU_PY_LOGGING",
    )
    stream: StreamSettings = Field(
        default_factory=StreamSettings,
        alias="FILMU_PY_STREAM",
    )
    grpc_bind_address: str = Field(
        default="127.0.0.1:50051",
        alias="FILMU_PY_GRPC_BIND_ADDRESS",
    )

    postgres_dsn: str = Field(default=DEFAULT_POSTGRES_DSN, alias="FILMU_PY_POSTGRES_DSN")
    redis_url: AnyUrl = Field(default=cast(AnyUrl, DEFAULT_REDIS_URL), alias="FILMU_PY_REDIS_URL")
    realdebrid_api_token: SecretStr | None = Field(
        default=None,
        alias="FILMU_PY_REALDEBRID_API_TOKEN",
    )
    alldebrid_api_token: SecretStr | None = Field(
        default=None,
        alias="FILMU_PY_ALLDEBRID_API_TOKEN",
    )
    debridlink_api_token: SecretStr | None = Field(
        default=None,
        alias="FILMU_PY_DEBRIDLINK_API_TOKEN",
    )

    temporal_enabled: bool = Field(default=False, alias="FILMU_PY_TEMPORAL_ENABLED")
    temporal_target: str = Field(default="localhost:7233", alias="FILMU_PY_TEMPORAL_TARGET")
    temporal_namespace: str = Field(default="default", alias="FILMU_PY_TEMPORAL_NAMESPACE")
    run_migrations_on_startup: bool = Field(
        default=True,
        alias="FILMU_PY_RUN_MIGRATIONS_ON_STARTUP",
    )

    arq_enabled: bool = Field(default=False, alias="FILMU_PY_ARQ_ENABLED")
    arq_queue_name: str = Field(default="filmu-py", alias="FILMU_PY_ARQ_QUEUE_NAME")
    arq_max_jobs: int = Field(default=32, alias="FILMU_PY_ARQ_MAX_JOBS")
    arq_job_timeout_seconds: int = Field(default=900, alias="FILMU_PY_ARQ_JOB_TIMEOUT_SECONDS")
    max_outbox_attempts: int = Field(default=5, alias="FILMU_PY_MAX_OUTBOX_ATTEMPTS")
    recovery_cooldown_minutes: int = Field(
        default=30,
        alias="FILMU_PY_RECOVERY_COOLDOWN_MINUTES",
    )
    max_recovery_attempts: int = Field(default=5, alias="FILMU_PY_MAX_RECOVERY_ATTEMPTS")

    unsafe_clear_queues_on_startup: bool = Field(
        default=False,
        alias="FILMU_PY_UNSAFE_CLEAR_QUEUES_ON_STARTUP",
    )
    unsafe_refresh_database_on_startup: bool = Field(
        default=False,
        alias="FILMU_PY_UNSAFE_REFRESH_DATABASE_ON_STARTUP",
    )

    otel_enabled: bool = Field(default=False, alias="FILMU_PY_OTEL_ENABLED")
    otel_exporter_otlp_endpoint: str | None = Field(
        default=None,
        alias="FILMU_PY_OTEL_EXPORTER_OTLP_ENDPOINT",
    )
    sentry_dsn: SecretStr | None = Field(default=None, alias="FILMU_PY_SENTRY_DSN")

    prometheus_enabled: bool = Field(default=True, alias="FILMU_PY_PROMETHEUS_ENABLED")
    plugins_dir: Path = Field(default=Path("plugins"), alias="FILMU_PY_PLUGINS_DIR")

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, value: SecretStr) -> SecretStr:
        """Ensure API key is not a weak placeholder value."""

        raw = value.get_secret_value().strip()
        if raw.lower() == "change-me" or len(raw) < 32:
            raise ValueError("FILMU_PY_API_KEY must be at least 32 characters")
        return SecretStr(raw)

    @field_validator("api_key_id")
    @classmethod
    def validate_api_key_id(cls, value: str) -> str:
        """Ensure API key identifiers are stable log-safe labels, not secrets."""

        normalized = value.strip()
        if not normalized:
            raise ValueError("FILMU_PY_API_KEY_ID must not be empty")
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
        if any(char not in allowed for char in normalized):
            raise ValueError(
                "FILMU_PY_API_KEY_ID may only contain letters, numbers, '.', '_' or '-'"
            )
        return normalized

    @classmethod
    def _load_env_file_values(cls) -> dict[str, str]:
        env_file = cls.model_config.get("env_file")
        if env_file is None:
            return {}

        env_files = env_file if isinstance(env_file, (list, tuple)) else (env_file,)
        merged: dict[str, str] = {}
        for entry in env_files:
            path = Path(entry)
            if not path.is_absolute():
                path = Path.cwd() / path
            if not path.exists():
                continue
            for key, value in dotenv_values(path).items():
                if isinstance(value, str) and value:
                    merged[key] = value
        return merged

    @model_validator(mode="before")
    @classmethod
    def apply_legacy_debrid_env_fallbacks(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data

        payload = dict(data)
        env_file_values = cls._load_env_file_values()
        downloaders_payload = payload.get("FILMU_PY_DOWNLOADERS") or payload.get("downloaders")
        downloaders = downloaders_payload if isinstance(downloaders_payload, dict) else {}

        for field_name, alias_name, legacy_names, provider_key in _DEBRID_ENV_FALLBACKS:
            if field_name in payload or alias_name in payload:
                continue

            provider_payload = downloaders.get(provider_key)
            if isinstance(provider_payload, dict) and "api_key" in provider_payload:
                continue

            legacy_value = next(
                (
                    value
                    for name in legacy_names
                    for value in (os.environ.get(name), env_file_values.get(name))
                    if isinstance(value, str) and value.strip()
                ),
                None,
            )
            if legacy_value is not None:
                payload[alias_name] = legacy_value

        return payload

    @model_validator(mode="after")
    def sync_runtime_and_compatibility_views(self) -> Settings:
        """Synchronize compatibility-backed nested blocks with existing runtime fields."""

        provider_secret_pairs = (
            ("real_debrid", "realdebrid_api_token"),
            ("all_debrid", "alldebrid_api_token"),
            ("debrid_link", "debridlink_api_token"),
        )
        for provider_key, secret_attr in provider_secret_pairs:
            provider_settings = self.downloaders.__getattribute__(provider_key)
            secret = cast(SecretStr | None, getattr(self, secret_attr))
            raw_secret = secret.get_secret_value().strip() if secret is not None else ""
            if not provider_settings.api_key and raw_secret:
                provider_settings.api_key = raw_secret
                provider_settings.enabled = True
            elif provider_settings.api_key and not raw_secret:
                object.__setattr__(self, secret_attr, SecretStr(provider_settings.api_key))

        if self.database.host and self.postgres_dsn == DEFAULT_POSTGRES_DSN:
            object.__setattr__(self, "postgres_dsn", self.database.host)
        if self.postgres_dsn and (
            not self.database.host or self.database.host == DEFAULT_POSTGRES_DSN
        ):
            self.database.host = self.postgres_dsn
        return self

    def _filesystem_model(self) -> FilesystemSettings:
        return _coerce_model(self.filesystem, FilesystemSettings)

    def _updaters_model(self) -> UpdatersSettings:
        return _coerce_model(self.updaters, UpdatersSettings)

    def _downloaders_model(self) -> DownloadersSettings:
        return _coerce_model(self.downloaders, DownloadersSettings)

    def _compat_downloaders_payload(self) -> dict[str, Any]:
        payload = _compat_dump(self._downloaders_model())
        stremthru_payload = cast(dict[str, Any], payload.get("stremthru", {}))
        if stremthru_payload == _compat_dump(StremThruSettings()):
            payload.pop("stremthru", None)
        return payload

    def _content_model(self) -> ContentSettings:
        return _coerce_model(self.content, ContentSettings)

    def _scraping_model(self) -> ScrapingSettings:
        return _coerce_model(self.scraping, ScrapingSettings)

    def _compat_scraping_payload(self) -> dict[str, Any]:
        payload = _compat_dump(self._scraping_model())
        payload.pop("ongoing_show_poll_interval_hours", None)
        return payload

    def _ranking_model(self) -> RankingProfile:
        return _coerce_model(self.ranking, RankingProfile)

    def _indexer_model(self) -> IndexerSettings:
        return _coerce_model(self.indexer, IndexerSettings)

    def _database_model(self) -> DatabaseSettings:
        return _coerce_model(self.database, DatabaseSettings)

    def _notifications_model(self) -> NotificationsSettings:
        return _coerce_model(self.notifications, NotificationsSettings)

    def _post_processing_model(self) -> PostProcessingSettings:
        return _coerce_model(self.post_processing, PostProcessingSettings)

    def _logging_model(self) -> LoggingSettings:
        return _coerce_model(self.logging, LoggingSettings)

    def _compat_logging_payload(self) -> dict[str, Any]:
        payload = _compat_dump(self._logging_model())
        payload.pop("retention_files", None)
        payload.pop("directory", None)
        payload.pop("structured_filename", None)
        return payload

    def _stream_model(self) -> StreamSettings:
        return _coerce_model(self.stream, StreamSettings)

    def to_compatibility_dict(self) -> dict[str, Any]:
        """Return the exact legacy `settings.json` compatibility shape."""

        content = _compat_dump(self._content_model())
        mdblist = cast(dict[str, Any], content.get("mdblist", {}))
        if mdblist:
            if "list_ids" in mdblist:
                mdblist["lists"] = list(cast(list[str], mdblist.pop("list_ids")))
            if "poll_interval_minutes" in mdblist:
                mdblist["update_interval"] = int(mdblist.pop("poll_interval_minutes")) * 60

        return {
            "version": self.version,
            "api_key": self.api_key.get_secret_value(),
            "tmdb_api_key": self.tmdb_api_key,
            "log_level": self.log_level,
            "enable_network_tracing": self.enable_network_tracing,
            "enable_stream_tracing": self.enable_stream_tracing,
            "retry_interval": self.retry_interval,
            "tracemalloc": self.tracemalloc,
            "filesystem": _compat_dump(self._filesystem_model()),
            "updaters": _compat_dump(self._updaters_model()),
            "downloaders": self._compat_downloaders_payload(),
            "content": content,
            "scraping": self._compat_scraping_payload(),
            "ranking": _compat_dump(self._ranking_model()),
            "indexer": _compat_dump(self._indexer_model()),
            "database": _compat_dump(self._database_model()),
            "notifications": _compat_dump(self._notifications_model()),
            "post_processing": _compat_dump(self._post_processing_model()),
            "logging": self._compat_logging_payload(),
            "stream": _compat_dump(self._stream_model()),
        }

    @classmethod
    def from_compatibility_dict(cls, data: dict[str, Any]) -> Settings:
        """Hydrate one runtime settings object from the original riven JSON shape."""

        payload = deepcopy(data)
        env_tmdb_api_key = os.getenv("TMDB_API_KEY", "")
        downloaders = cast(dict[str, Any], payload.get("downloaders", {}))
        database = cast(dict[str, Any], payload.get("database", {}))
        database_host = database.get("host", DEFAULT_POSTGRES_DSN)

        return cls(
            FILMU_PY_VERSION=payload.get("version", "0.1.0"),
            FILMU_PY_API_KEY=SecretStr(
                str(payload.get("api_key") or os.getenv("FILMU_PY_API_KEY", ""))
            ),
            FILMU_PY_API_KEY_ID=os.getenv("FILMU_PY_API_KEY_ID", "primary"),
            TMDB_API_KEY=payload.get("tmdb_api_key") or env_tmdb_api_key,
            FILMU_PY_LOG_LEVEL=payload.get("log_level", "INFO"),
            FILMU_PY_ENABLE_NETWORK_TRACING=payload.get("enable_network_tracing", False),
            FILMU_PY_ENABLE_STREAM_TRACING=payload.get("enable_stream_tracing", False),
            FILMU_PY_RETRY_INTERVAL=payload.get("retry_interval", 86400),
            FILMU_PY_TRACEMALLOC=payload.get("tracemalloc", False),
            FILMU_PY_FILESYSTEM=payload.get("filesystem", {}),
            FILMU_PY_UPDATERS=payload.get("updaters", {}),
            FILMU_PY_DOWNLOADERS=_coerce_model(downloaders, DownloadersSettings),
            FILMU_PY_CONTENT=payload.get("content", {}),
            FILMU_PY_SCRAPING=payload.get("scraping", {}),
            FILMU_PY_RANKING=payload.get("ranking", _build_default_ranking_settings()),
            FILMU_PY_INDEXER=payload.get("indexer", {}),
            FILMU_PY_DATABASE=_coerce_model(database, DatabaseSettings),
            FILMU_PY_NOTIFICATIONS=payload.get("notifications", {}),
            FILMU_PY_POST_PROCESSING=payload.get("post_processing", {}),
            FILMU_PY_LOGGING=payload.get("logging", {}),
            FILMU_PY_STREAM=payload.get("stream", {}),
            FILMU_PY_POSTGRES_DSN=database_host,
            FILMU_PY_REALDEBRID_API_TOKEN=(
                cast(dict[str, Any], downloaders.get("real_debrid", {})).get("api_key") or None
            ),
            FILMU_PY_ALLDEBRID_API_TOKEN=(
                cast(dict[str, Any], downloaders.get("all_debrid", {})).get("api_key") or None
            ),
            FILMU_PY_DEBRIDLINK_API_TOKEN=(
                cast(dict[str, Any], downloaders.get("debrid_link", {})).get("api_key") or None
            ),
        )


_runtime_settings: Settings | None = None


@lru_cache(maxsize=1)
def _load_env_settings() -> Settings:
    """Load the environment-backed settings snapshot once per process."""

    return Settings()  # type: ignore[call-arg]


def get_settings() -> Settings:
    """Return the active runtime settings object for the current process."""

    override = _runtime_settings
    if override is not None:
        return override
    return _load_env_settings()


def set_runtime_settings(settings: Settings) -> Settings:
    """Replace the active runtime settings object for the current process."""

    global _runtime_settings
    _runtime_settings = settings
    return settings


def reset_runtime_settings() -> None:
    """Clear any runtime override and reset the cached environment snapshot."""

    global _runtime_settings
    _runtime_settings = None
    _load_env_settings.cache_clear()
