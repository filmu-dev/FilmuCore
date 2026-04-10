"""Shared API response models."""

# mypy: disable-error-code=misc

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel


class MessageResponse(BaseModel):
    """Simple message response model."""

    message: str


class PluginCapabilityStatusResponse(BaseModel):
    """One loaded capability-plugin summary."""

    name: str
    capabilities: list[str]
    status: Literal["loaded", "load_failed"] = "loaded"
    ready: bool = True
    configured: bool | None = None
    version: str | None = None
    api_version: str | None = None
    min_host_version: str | None = None
    max_host_version: str | None = None
    publisher: str | None = None
    release_channel: str | None = None
    trust_level: str | None = None
    permission_scopes: list[str] = []
    source_sha256: str | None = None
    signing_key_id: str | None = None
    signature_present: bool = False
    signature_verified: bool = False
    signature_verification_reason: str | None = None
    trust_policy_decision: str | None = None
    trust_store_source: str | None = None
    sandbox_profile: str | None = None
    tenancy_mode: str | None = None
    quarantined: bool = False
    quarantine_reason: str | None = None
    publisher_policy_decision: str | None = None
    publisher_policy_status: str | None = None
    quarantine_recommended: bool = False
    source: str | None = None
    warnings: list[str] = []
    error: str | None = None


class AuthContextResponse(BaseModel):
    """Operator-visible authenticated identity context for the current request."""

    authentication_mode: str
    api_key_id: str
    actor_id: str
    actor_type: str
    tenant_id: str
    authorized_tenant_ids: list[str]
    authorization_tenant_scope: str
    roles: list[str]
    scopes: list[str]
    effective_permissions: list[str]
    oidc_issuer: str | None = None
    oidc_subject: str | None = None
    principal_key: str | None = None
    principal_type: str | None = None
    service_account_api_key_id: str | None = None


class PluginEventStatusResponse(BaseModel):
    """One plugin event-governance and hook-subscription summary."""

    name: str
    publisher: str | None = None
    publishable_events: list[str]
    hook_subscriptions: list[str]


class QueueStatusResponse(BaseModel):
    """Current ARQ queue control-plane snapshot."""

    queue_name: str
    arq_enabled: bool
    observed_at: str
    total_jobs: int
    ready_jobs: int
    deferred_jobs: int
    in_progress_jobs: int
    retry_jobs: int
    result_jobs: int
    dead_letter_jobs: int
    alert_level: Literal["ok", "warning", "critical"] = "ok"
    alerts: list["QueueAlertResponse"] = []
    oldest_ready_age_seconds: float | None = None
    next_scheduled_in_seconds: float | None = None


class QueueAlertResponse(BaseModel):
    """One operator-facing queue alert classification."""

    code: str
    severity: Literal["warning", "critical"]
    message: str


class QueueStatusHistoryPointResponse(BaseModel):
    """One persisted queue snapshot for trend inspection."""

    observed_at: str
    total_jobs: int
    ready_jobs: int
    deferred_jobs: int
    in_progress_jobs: int
    retry_jobs: int
    dead_letter_jobs: int
    oldest_ready_age_seconds: float | None = None
    next_scheduled_in_seconds: float | None = None
    alert_level: Literal["ok", "warning", "critical"] = "ok"


class QueueStatusHistorySummaryResponse(BaseModel):
    """Derived operator rollup for one bounded queue-history response."""

    points: int
    latest_alert_level: Literal["ok", "warning", "critical"] = "ok"
    critical_points: int
    warning_points: int
    max_ready_jobs: int
    max_dead_letter_jobs: int
    max_oldest_ready_age_seconds: float | None = None


class QueueStatusHistoryResponse(BaseModel):
    """Bounded queue-history timeline for operator views."""

    queue_name: str
    summary: QueueStatusHistorySummaryResponse
    history: list[QueueStatusHistoryPointResponse]


class ApiKeyRotationResponse(BaseModel):
    """Response payload returned after a real backend API-key rotation."""

    key: str
    api_key_id: str
    warning: str


class LogsResponse(BaseModel):
    """Historical log response model."""

    logs: list[str]


class EventTypesResponse(BaseModel):
    """Available stream event types response model."""

    event_types: list[str]


class ServingSessionResponse(BaseModel):
    """One active serving-session snapshot for internal observability endpoints."""

    session_id: str
    category: str
    resource: str
    started_at: str
    last_activity_at: str
    bytes_served: int


class ServingHandleResponse(BaseModel):
    """One active serving-handle snapshot for internal observability endpoints."""

    handle_id: str
    session_id: str
    category: str
    path: str
    path_id: str
    created_at: str
    last_activity_at: str
    bytes_served: int
    read_offset: int


class ServingPathResponse(BaseModel):
    """One tracked serving-path snapshot for internal observability endpoints."""

    path_id: str
    category: str
    path: str
    created_at: str
    last_activity_at: str
    size_bytes: int | None = None
    active_handle_count: int


class ServingGovernanceResponse(BaseModel):
    """Shared serving-core governance counters and limits."""

    hls_retention_seconds: int
    hls_generation_concurrency: int
    hls_generation_timeout_seconds: int
    active_sessions: int
    active_handles: int
    tracked_paths: int
    active_local_sessions: int
    active_remote_sessions: int
    active_local_handles: int
    hls_cleanup_runs: int
    hls_cleanup_deleted_dirs: int
    hls_cleanup_failed_dirs: int
    hls_stale_segment_reap_runs: int
    hls_stale_segment_reaped_files: int
    hls_stale_segment_reap_failed_files: int
    hls_quota_reap_runs: int
    hls_quota_deleted_dirs: int
    hls_quota_failed_dirs: int
    hls_generation_started: int
    hls_generation_completed: int
    hls_generation_failed: int
    hls_generation_timeouts: int
    hls_generation_capacity_rejections: int
    hls_generation_cancelled: int
    hls_generation_terminated: int
    hls_generation_killed: int
    active_hls_generation_processes: int
    hls_disk_usage_bytes: int
    hls_manifest_invalid: int
    hls_manifest_regenerated: int
    hls_route_failures_total: int
    hls_route_failures_generation_failed: int
    hls_route_failures_generation_timeout: int
    hls_route_failures_generation_capacity_exceeded: int
    hls_route_failures_generator_unavailable: int
    hls_route_failures_lease_failed: int
    hls_route_failures_transcode_source_unavailable: int
    hls_route_failures_manifest_invalid: int
    hls_route_failures_generated_missing: int
    hls_route_failures_upstream_failed: int
    hls_route_failures_upstream_manifest_invalid: int
    remote_hls_retry_attempts: int
    remote_hls_cooldown_starts: int
    remote_hls_cooldown_hits: int
    remote_hls_cooldowns_active: int
    inline_remote_hls_refresh_attempts: int
    inline_remote_hls_refresh_recovered: int
    inline_remote_hls_refresh_no_action: int
    inline_remote_hls_refresh_failures: int
    direct_playback_refresh_trigger_starts: int
    direct_playback_refresh_trigger_no_action: int
    direct_playback_refresh_trigger_controller_unavailable: int
    direct_playback_refresh_trigger_already_pending: int
    direct_playback_refresh_trigger_backoff_pending: int
    direct_playback_refresh_trigger_failures: int
    direct_playback_refresh_trigger_tasks_active: int
    hls_failed_lease_refresh_trigger_starts: int
    hls_failed_lease_refresh_trigger_no_action: int
    hls_failed_lease_refresh_trigger_controller_unavailable: int
    hls_failed_lease_refresh_trigger_already_pending: int
    hls_failed_lease_refresh_trigger_backoff_pending: int
    hls_failed_lease_refresh_trigger_failures: int
    hls_failed_lease_refresh_trigger_tasks_active: int
    hls_restricted_fallback_refresh_trigger_starts: int
    hls_restricted_fallback_refresh_trigger_no_action: int
    hls_restricted_fallback_refresh_trigger_controller_unavailable: int
    hls_restricted_fallback_refresh_trigger_already_pending: int
    hls_restricted_fallback_refresh_trigger_backoff_pending: int
    hls_restricted_fallback_refresh_trigger_failures: int
    hls_restricted_fallback_refresh_trigger_tasks_active: int
    stream_abort_events: int
    local_stream_abort_events: int
    remote_stream_abort_events: int
    generated_hls_directories: int
    tracked_media_entries: int
    tracked_active_streams: int
    media_entries_refreshing: int
    media_entries_failed: int
    media_entries_needing_refresh: int
    selected_direct_streams: int
    selected_hls_streams: int
    selected_direct_streams_needing_refresh: int
    selected_hls_streams_needing_refresh: int
    selected_direct_streams_failed: int
    selected_hls_streams_failed: int
    direct_playback_refresh_rate_limited: int
    direct_playback_refresh_provider_circuit_open: int
    hls_failed_lease_refresh_rate_limited: int
    hls_failed_lease_refresh_provider_circuit_open: int
    hls_restricted_fallback_refresh_rate_limited: int
    hls_restricted_fallback_refresh_provider_circuit_open: int
    vfs_catalog_watch_sessions_started: int
    vfs_catalog_watch_sessions_completed: int
    vfs_catalog_watch_sessions_active: int
    vfs_catalog_reconnect_requested: int
    vfs_catalog_reconnect_delta_served: int
    vfs_catalog_reconnect_snapshot_fallback: int
    vfs_catalog_reconnect_failures: int
    vfs_catalog_snapshots_served: int
    vfs_catalog_deltas_served: int
    vfs_catalog_heartbeats_served: int
    vfs_catalog_problem_events: int
    vfs_catalog_request_stream_failures: int
    vfs_catalog_snapshot_build_failures: int
    vfs_catalog_delta_build_failures: int
    vfs_catalog_refresh_attempts: int
    vfs_catalog_refresh_succeeded: int
    vfs_catalog_refresh_provider_failures: int
    vfs_catalog_refresh_empty_results: int
    vfs_catalog_refresh_validation_failed: int
    vfs_catalog_refresh_skipped_no_provider: int
    vfs_catalog_refresh_skipped_no_restricted_url: int
    vfs_catalog_refresh_skipped_no_client: int
    vfs_catalog_refresh_skipped_fresh: int
    vfs_catalog_inline_refresh_requests: int
    vfs_catalog_inline_refresh_succeeded: int
    vfs_catalog_inline_refresh_failed: int
    vfs_catalog_inline_refresh_not_found: int
    vfs_runtime_snapshot_available: int
    vfs_runtime_open_handles: int
    vfs_runtime_peak_open_handles: int
    vfs_runtime_active_reads: int
    vfs_runtime_peak_active_reads: int
    vfs_runtime_chunk_cache_weighted_bytes: int
    vfs_runtime_chunk_cache_backend: str
    vfs_runtime_chunk_cache_memory_bytes: int
    vfs_runtime_chunk_cache_memory_max_bytes: int
    vfs_runtime_chunk_cache_memory_hits: int
    vfs_runtime_chunk_cache_memory_misses: int
    vfs_runtime_chunk_cache_disk_bytes: int
    vfs_runtime_chunk_cache_disk_max_bytes: int
    vfs_runtime_chunk_cache_disk_hits: int
    vfs_runtime_chunk_cache_disk_misses: int
    vfs_runtime_chunk_cache_disk_writes: int
    vfs_runtime_chunk_cache_disk_write_errors: int
    vfs_runtime_chunk_cache_disk_evictions: int
    vfs_runtime_handle_startup_total: int
    vfs_runtime_handle_startup_ok: int
    vfs_runtime_handle_startup_error: int
    vfs_runtime_handle_startup_estale: int
    vfs_runtime_handle_startup_average_duration_ms: int
    vfs_runtime_handle_startup_max_duration_ms: int
    vfs_runtime_mounted_reads_total: int
    vfs_runtime_mounted_reads_ok: int
    vfs_runtime_mounted_reads_error: int
    vfs_runtime_mounted_reads_estale: int
    vfs_runtime_mounted_reads_average_duration_ms: int
    vfs_runtime_mounted_reads_max_duration_ms: int
    vfs_runtime_upstream_fetch_operations: int
    vfs_runtime_upstream_fetch_bytes_total: int
    vfs_runtime_upstream_fetch_average_duration_ms: int
    vfs_runtime_upstream_fetch_max_duration_ms: int
    vfs_runtime_upstream_fail_invalid_url: int
    vfs_runtime_upstream_fail_build_request: int
    vfs_runtime_upstream_fail_network: int
    vfs_runtime_upstream_fail_stale_status: int
    vfs_runtime_upstream_fail_unexpected_status: int
    vfs_runtime_upstream_fail_unexpected_status_too_many_requests: int
    vfs_runtime_upstream_fail_unexpected_status_server_error: int
    vfs_runtime_upstream_fail_read_body: int
    vfs_runtime_upstream_retryable_network: int
    vfs_runtime_upstream_retryable_read_body: int
    vfs_runtime_upstream_retryable_status_too_many_requests: int
    vfs_runtime_upstream_retryable_status_server_error: int
    vfs_runtime_backend_fallback_attempts: int
    vfs_runtime_backend_fallback_success: int
    vfs_runtime_backend_fallback_failure: int
    vfs_runtime_backend_fallback_attempts_direct_read_failure: int
    vfs_runtime_backend_fallback_attempts_inline_refresh_unavailable: int
    vfs_runtime_backend_fallback_attempts_post_inline_refresh_failure: int
    vfs_runtime_backend_fallback_success_direct_read_failure: int
    vfs_runtime_backend_fallback_success_inline_refresh_unavailable: int
    vfs_runtime_backend_fallback_success_post_inline_refresh_failure: int
    vfs_runtime_backend_fallback_failure_direct_read_failure: int
    vfs_runtime_backend_fallback_failure_inline_refresh_unavailable: int
    vfs_runtime_backend_fallback_failure_post_inline_refresh_failure: int
    vfs_runtime_chunk_cache_hits: int
    vfs_runtime_chunk_cache_misses: int
    vfs_runtime_chunk_cache_inserts: int
    vfs_runtime_chunk_cache_prefetch_hits: int
    vfs_runtime_prefetch_concurrency_limit: int
    vfs_runtime_prefetch_available_permits: int
    vfs_runtime_prefetch_active_permits: int
    vfs_runtime_prefetch_active_background_tasks: int
    vfs_runtime_prefetch_peak_active_background_tasks: int
    vfs_runtime_prefetch_background_spawned: int
    vfs_runtime_prefetch_background_backpressure: int
    vfs_runtime_prefetch_fairness_denied: int
    vfs_runtime_prefetch_global_backpressure_denied: int
    vfs_runtime_prefetch_background_error: int
    vfs_runtime_chunk_coalescing_in_flight_chunks: int
    vfs_runtime_chunk_coalescing_peak_in_flight_chunks: int
    vfs_runtime_chunk_coalescing_waits_total: int
    vfs_runtime_chunk_coalescing_waits_hit: int
    vfs_runtime_chunk_coalescing_waits_miss: int
    vfs_runtime_chunk_coalescing_wait_average_duration_ms: float
    vfs_runtime_chunk_coalescing_wait_max_duration_ms: float
    vfs_runtime_inline_refresh_success: int
    vfs_runtime_inline_refresh_no_url: int
    vfs_runtime_inline_refresh_error: int
    vfs_runtime_inline_refresh_timeout: int
    vfs_runtime_windows_callbacks_error: int
    vfs_runtime_windows_callbacks_estale: int
    vfs_runtime_cache_hit_ratio: float
    vfs_runtime_fallback_success_ratio: float
    vfs_runtime_prefetch_pressure_ratio: float
    vfs_runtime_provider_pressure_incidents: int
    vfs_runtime_fairness_pressure_incidents: int
    vfs_runtime_rollout_readiness: str


class ServingStatusResponse(BaseModel):
    """Internal stream-status surface for serving-session/accounting visibility."""

    sessions: list[ServingSessionResponse]
    handles: list[ServingHandleResponse]
    paths: list[ServingPathResponse]
    governance: ServingGovernanceResponse


class HealthResponse(BaseModel):
    """Health response model with service metadata."""

    message: str
    service: str
    status: Literal["healthy", "degraded", "unhealthy"]
    checks: dict[str, str]


class DownloaderUserInfo(BaseModel):
    """Normalized downloader-account information for dashboard compatibility."""

    service: str
    username: str | None = None
    email: str | None = None
    user_id: str | None = None
    premium_status: Literal["free", "premium"]
    premium_expires_at: str | None = None
    premium_days_left: int | None = None
    points: int | None = None
    total_downloaded_bytes: int | None = None
    cooldown_until: str | None = None


class DownloaderUserInfoResponse(BaseModel):
    """Collection of normalized downloader-account records."""

    services: list[DownloaderUserInfo]


class StatsMediaYearRelease(BaseModel):
    """Release-year aggregate used by the dashboard line chart."""

    year: int | None
    count: int


class StatsResponse(BaseModel):
    """Aggregated dashboard statistics for current frontend compatibility."""

    total_items: int
    total_movies: int
    total_shows: int
    total_seasons: int
    total_episodes: int
    total_symlinks: int
    incomplete_items: int
    states: dict[str, int]
    activity: dict[str, int]
    media_year_releases: list[StatsMediaYearRelease]


class IdListPayload(BaseModel):
    """Compatibility payload carrying a list of item identifiers."""

    ids: list[str]


class AddMediaItemPayload(BaseModel):
    """Compatibility payload for `/api/v1/items/add`."""

    tmdb_ids: list[str] | None = None
    tvdb_ids: list[str] | None = None
    media_type: Literal["movie", "tv"]
    requested_seasons: list[int] | None = None
    requested_episodes: dict[str, list[int]] | None = None


class ScrapeAutoPayload(BaseModel):
    """Compatibility payload for `POST /api/v1/scrape/auto`."""

    media_type: Literal["movie", "tv"]
    item_id: str | int | None = None
    tmdb_id: str | None = None
    tvdb_id: str | None = None
    imdb_id: str | None = None
    ranking_overrides: dict[str, list[str]] | None = None
    season_numbers: list[int] | None = None
    requested_seasons: list[int] | None = None
    requested_episodes: dict[str, list[int]] | None = None
    min_filesize_override: int | None = None
    max_filesize_override: int | None = None


class ItemParentIdsResponse(BaseModel):
    """Parent identifier bundle for season/episode navigation compatibility."""

    tmdb_id: str | None = None
    tvdb_id: str | None = None


class ItemSummaryResponse(BaseModel):
    """Minimal item summary used by library and list views."""

    id: str
    type: str
    title: str
    state: str | None = None
    tmdb_id: str | None = None
    tvdb_id: str | None = None
    parent_ids: ItemParentIdsResponse | None = None
    poster_path: str | None = None
    aired_at: str | None = None
    next_retry_at: datetime | None = None
    recovery_attempt_count: int = 0
    is_in_cooldown: bool = False


class ItemsResponse(BaseModel):
    """Paginated item-list response for current library compatibility."""

    success: bool
    items: list[ItemSummaryResponse]
    page: int
    limit: int
    total_items: int
    total_pages: int


class ItemActionResponse(BaseModel):
    """Shared response wrapper for item reset/retry/remove actions."""

    message: str
    ids: list[str]


class ScrapeSessionStateResponse(BaseModel):
    """Polling response for one scrape session backed by a real item state."""

    session_id: str
    item_id: str
    title: str
    state: str


class PlaybackAttachmentDetailResponse(BaseModel):
    """Persisted playback attachment projection for item-detail compatibility responses."""

    id: str
    kind: str
    locator: str
    source_key: str | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    original_filename: str | None = None
    file_size: int | None = None
    local_path: str | None = None
    restricted_url: str | None = None
    unrestricted_url: str | None = None
    is_preferred: bool = False
    preference_rank: int = 100
    refresh_state: str
    expires_at: str | None = None
    last_refreshed_at: str | None = None
    last_refresh_error: str | None = None


class ResolvedPlaybackAttachmentResponse(BaseModel):
    """Best-current resolved playback attachment snapshot for item-detail responses."""

    kind: str
    locator: str
    source_key: str
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    original_filename: str | None = None
    file_size: int | None = None
    local_path: str | None = None
    restricted_url: str | None = None
    unrestricted_url: str | None = None


class ResolvedPlaybackSnapshotResponse(BaseModel):
    """Best-current direct/HLS playback availability snapshot for one detail response."""

    direct: ResolvedPlaybackAttachmentResponse | None = None
    hls: ResolvedPlaybackAttachmentResponse | None = None
    direct_ready: bool = False
    hls_ready: bool = False
    missing_local_file: bool = False


class ActiveStreamOwnerResponse(BaseModel):
    """Ownership link from one resolved active stream to one projected media entry."""

    media_entry_index: int
    kind: str
    original_filename: str | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None


class ActiveStreamDetailResponse(BaseModel):
    """Explicit active-stream readiness and ownership view for item-detail responses."""

    direct_ready: bool = False
    hls_ready: bool = False
    missing_local_file: bool = False
    direct_owner: ActiveStreamOwnerResponse | None = None
    hls_owner: ActiveStreamOwnerResponse | None = None


class MediaEntryDetailResponse(BaseModel):
    """VFS-facing media-entry projection for item-detail compatibility responses."""

    entry_type: str = "media"
    kind: str
    original_filename: str | None = None
    url: str | None = None
    local_path: str | None = None
    download_url: str | None = None
    unrestricted_url: str | None = None
    provider: str | None = None
    provider_download_id: str | None = None
    provider_file_id: str | None = None
    provider_file_path: str | None = None
    size: int | None = None
    created: str | None = None
    modified: str | None = None
    refresh_state: str = "ready"
    expires_at: str | None = None
    last_refreshed_at: str | None = None
    last_refresh_error: str | None = None
    active_for_direct: bool = False
    active_for_hls: bool = False
    is_active_stream: bool = False


class ItemRequestSummaryResponse(BaseModel):
    """Latest persisted request-intent summary for one item detail response."""

    is_partial: bool
    requested_seasons: list[int] | None = None
    requested_episodes: dict[str, list[int]] | None = None


class SubtitleEntryResponse(BaseModel):
    """Subtitle projection surfaced on item detail responses."""

    id: str
    language: str
    format: str
    source: str
    url: str | None = None
    is_default: bool = False
    is_forced: bool = False


class ItemSeasonStateResponse(BaseModel):
    """Per-season availability state for Riven-frontend compatibility.

    Consumed by ``data.riven?.seasons`` in the detail page to determine
    which seasons are already installed (and should be greyed-out in the
    season-selector popup).
    """

    season_number: int
    state: str


class ItemDetailResponse(BaseModel):
    """Flexible item-detail payload for the current details-page compatibility layer."""

    id: str
    type: str
    title: str
    state: str | None = None
    external_ref: str | None = None
    tmdb_id: str | None = None
    tvdb_id: str | None = None
    parent_ids: ItemParentIdsResponse | None = None
    poster_path: str | None = None
    aired_at: str | None = None
    next_retry_at: datetime | None = None
    recovery_attempt_count: int = 0
    is_in_cooldown: bool = False
    metadata: dict[str, Any] | None = None
    request: ItemRequestSummaryResponse | None = None
    playback_attachments: list[PlaybackAttachmentDetailResponse] | None = None
    resolved_playback: ResolvedPlaybackSnapshotResponse | None = None
    active_stream: ActiveStreamDetailResponse | None = None
    media_entries: list[MediaEntryDetailResponse] | None = None
    subtitles: list[SubtitleEntryResponse] = []
    # Per-season availability for Riven-frontend season-selector compatibility.
    # Populated from media_entries for TV show items; None for movies.
    seasons: list[ItemSeasonStateResponse] | None = None


class CalendarReleaseDataResponse(BaseModel):
    """Optional release-window fields used by the current calendar page."""

    next_aired: str | None = None
    nextAired: str | None = None
    last_aired: str | None = None
    lastAired: str | None = None


class CalendarItemResponse(BaseModel):
    """Calendar item payload for current frontend compatibility."""

    item_id: str
    tvdb_id: str | None = None
    tmdb_id: str | None = None
    show_title: str
    item_type: str
    aired_at: str
    season: int | None = None
    episode: int | None = None
    last_state: str | None = None
    release_data: CalendarReleaseDataResponse | None = None


class CalendarResponse(BaseModel):
    """Calendar response keyed by stable item identifiers."""

    data: dict[str, CalendarItemResponse]
