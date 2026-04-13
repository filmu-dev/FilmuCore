"""GraphQL types for the filmu-python compatibility surface."""

from __future__ import annotations

from enum import Enum, StrEnum

import strawberry
from strawberry.scalars import JSON


@strawberry.enum
class MediaKind(Enum):
    """Intentional GraphQL media-kind enum decoupled from REST alias history."""

    MOVIE = "movie"
    SHOW = "show"
    SEASON = "season"
    EPISODE = "episode"


@strawberry.enum
class GQLRecoveryMechanism(StrEnum):
    """Intentional automatic-recovery mechanism for future GraphQL consumers."""

    NONE = "none"
    ORPHAN_RECOVERY = "orphan_recovery"
    COOLDOWN_RECOVERY = "cooldown_recovery"


@strawberry.enum
class GQLRecoveryTargetStage(StrEnum):
    """Pipeline stage targeted by automatic recovery."""

    NONE = "none"
    INDEX = "index"
    SCRAPE = "scrape"
    PARSE = "parse"
    FINALIZE = "finalize"


@strawberry.type
class GQLHealthCheck:
    """Structured health status for GraphQL clients."""

    service: str
    status: str


@strawberry.type
class GQLMediaItem:
    """Minimal media item type scaffold for compatibility evolution."""

    id: strawberry.ID
    external_ref: str
    title: str
    state: str
    media_type: str = strawberry.field(name="mediaType")
    media_kind: MediaKind = strawberry.field(name="mediaKind")
    tmdb_id: int | None = strawberry.field(name="tmdbId", default=None)
    tvdb_id: int | None = strawberry.field(name="tvdbId", default=None)
    imdb_id: str | None = strawberry.field(name="imdbId", default=None)
    parent_tmdb_id: int | None = strawberry.field(name="parentTmdbId", default=None)
    parent_tvdb_id: int | None = strawberry.field(name="parentTvdbId", default=None)
    show_title: str | None = strawberry.field(name="showTitle", default=None)
    season_number: int | None = strawberry.field(name="seasonNumber", default=None)
    episode_number: int | None = strawberry.field(name="episodeNumber", default=None)
    poster_path: str | None = strawberry.field(name="posterPath", default=None)
    aired_at: str | None = strawberry.field(name="airedAt", default=None)


@strawberry.type
class GQLCalendarEntry:
    """Intentional GraphQL calendar entry unconstrained by REST compatibility shape."""

    item_id: strawberry.ID = strawberry.field(name="itemId")
    show_title: str = strawberry.field(name="showTitle")
    item_type: str = strawberry.field(name="itemType")
    aired_at: str | None = strawberry.field(name="airedAt")
    last_state: str = strawberry.field(name="lastState")
    season: int | None = None
    episode: int | None = None
    tmdb_id: int | None = strawberry.field(name="tmdbId", default=None)
    tvdb_id: int | None = strawberry.field(name="tvdbId", default=None)
    imdb_id: str | None = strawberry.field(name="imdbId", default=None)
    parent_tmdb_id: int | None = strawberry.field(name="parentTmdbId", default=None)
    parent_tvdb_id: int | None = strawberry.field(name="parentTvdbId", default=None)
    release_data: str | None = strawberry.field(name="releaseData", default=None)


@strawberry.type
class GQLVfsCorrelationKeys:
    """Correlation identifiers for one VFS catalog node."""

    item_id: str | None = strawberry.field(name="itemId", default=None)
    media_entry_id: str | None = strawberry.field(name="mediaEntryId", default=None)
    source_attachment_id: str | None = strawberry.field(name="sourceAttachmentId", default=None)
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)
    session_id: str | None = strawberry.field(name="sessionId", default=None)
    handle_key: str | None = strawberry.field(name="handleKey", default=None)


@strawberry.type
class GQLVfsDirectoryDetail:
    """Directory metadata for one VFS catalog node."""

    path: str


@strawberry.type
class GQLVfsFileDetail:
    """File metadata for one VFS catalog node."""

    item_id: str = strawberry.field(name="itemId")
    item_title: str = strawberry.field(name="itemTitle")
    item_external_ref: str = strawberry.field(name="itemExternalRef")
    media_entry_id: str = strawberry.field(name="mediaEntryId")
    source_attachment_id: str | None = strawberry.field(name="sourceAttachmentId", default=None)
    media_type: str = strawberry.field(name="mediaType")
    transport: str
    locator: str
    local_path: str | None = strawberry.field(name="localPath", default=None)
    restricted_url: str | None = strawberry.field(name="restrictedUrl", default=None)
    unrestricted_url: str | None = strawberry.field(name="unrestrictedUrl", default=None)
    original_filename: str | None = strawberry.field(name="originalFilename", default=None)
    size_bytes: int | None = strawberry.field(name="sizeBytes", default=None)
    lease_state: str = strawberry.field(name="leaseState")
    expires_at: str | None = strawberry.field(name="expiresAt", default=None)
    last_refreshed_at: str | None = strawberry.field(name="lastRefreshedAt", default=None)
    last_refresh_error: str | None = strawberry.field(name="lastRefreshError", default=None)
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)
    active_roles: list[str] = strawberry.field(name="activeRoles")
    source_key: str | None = strawberry.field(name="sourceKey", default=None)
    query_strategy: str | None = strawberry.field(name="queryStrategy", default=None)
    provider_family: str = strawberry.field(name="providerFamily")
    locator_source: str = strawberry.field(name="locatorSource")
    match_basis: str | None = strawberry.field(name="matchBasis", default=None)
    restricted_fallback: bool = strawberry.field(name="restrictedFallback")


@strawberry.type
class GQLVfsCatalogEntry:
    """One mounted catalog node exposed intentionally through GraphQL."""

    entry_id: str = strawberry.field(name="entryId")
    parent_entry_id: str | None = strawberry.field(name="parentEntryId", default=None)
    path: str
    name: str
    kind: str
    correlation: GQLVfsCorrelationKeys
    directory: GQLVfsDirectoryDetail | None = None
    file: GQLVfsFileDetail | None = None


@strawberry.type
class GQLVfsCatalogStats:
    """Aggregate counts for one mounted catalog snapshot."""

    directory_count: int = strawberry.field(name="directoryCount")
    file_count: int = strawberry.field(name="fileCount")
    blocked_item_count: int = strawberry.field(name="blockedItemCount")


@strawberry.type
class GQLVfsBlockedItem:
    """Blocked mounted item retained in one VFS snapshot."""

    item_id: str = strawberry.field(name="itemId")
    external_ref: str = strawberry.field(name="externalRef")
    title: str
    reason: str


@strawberry.type
class GQLVfsSnapshot:
    """Snapshot-level mounted VFS control-plane view."""

    generation_id: str = strawberry.field(name="generationId")
    published_at: str = strawberry.field(name="publishedAt")
    stats: GQLVfsCatalogStats
    blocked_items: list[GQLVfsBlockedItem] = strawberry.field(name="blockedItems")


@strawberry.type
class GQLVfsDirectoryListing:
    """Immediate directory listing backed by the mounted VFS catalog snapshot."""

    generation_id: str = strawberry.field(name="generationId")
    path: str
    entry: GQLVfsCatalogEntry
    stats: GQLVfsCatalogStats
    directories: list[GQLVfsCatalogEntry]
    files: list[GQLVfsCatalogEntry]


@strawberry.type
class GQLRuntimeLifecycleTransition:
    """One runtime lifecycle transition entry."""

    phase: str
    health: str
    detail: str
    at: str


@strawberry.type
class GQLRuntimeLifecycleSnapshot:
    """Current runtime lifecycle snapshot plus bounded transition history."""

    phase: str
    health: str
    detail: str
    updated_at: str = strawberry.field(name="updatedAt")
    transitions: list[GQLRuntimeLifecycleTransition]


@strawberry.type
class GQLQueueAlert:
    """One queue alert surfaced through the operator graph."""

    code: str
    severity: str
    message: str


@strawberry.type
class GQLWorkerQueueStatus:
    """Current worker queue status snapshot."""

    queue_name: str = strawberry.field(name="queueName")
    arq_enabled: bool = strawberry.field(name="arqEnabled")
    observed_at: str = strawberry.field(name="observedAt")
    total_jobs: int = strawberry.field(name="totalJobs")
    ready_jobs: int = strawberry.field(name="readyJobs")
    deferred_jobs: int = strawberry.field(name="deferredJobs")
    in_progress_jobs: int = strawberry.field(name="inProgressJobs")
    retry_jobs: int = strawberry.field(name="retryJobs")
    result_jobs: int = strawberry.field(name="resultJobs")
    dead_letter_jobs: int = strawberry.field(name="deadLetterJobs")
    alert_level: str = strawberry.field(name="alertLevel")
    alerts: list[GQLQueueAlert]
    oldest_ready_age_seconds: float | None = strawberry.field(
        name="oldestReadyAgeSeconds", default=None
    )
    next_scheduled_in_seconds: float | None = strawberry.field(
        name="nextScheduledInSeconds", default=None
    )
    dead_letter_oldest_age_seconds: float | None = strawberry.field(
        name="deadLetterOldestAgeSeconds", default=None
    )
    dead_letter_reason_counts: JSON = strawberry.field(
        name="deadLetterReasonCounts", default_factory=dict
    )


@strawberry.type
class GQLWorkerQueueHistoryPoint:
    """One persisted queue history point."""

    observed_at: str = strawberry.field(name="observedAt")
    total_jobs: int = strawberry.field(name="totalJobs")
    ready_jobs: int = strawberry.field(name="readyJobs")
    deferred_jobs: int = strawberry.field(name="deferredJobs")
    in_progress_jobs: int = strawberry.field(name="inProgressJobs")
    retry_jobs: int = strawberry.field(name="retryJobs")
    dead_letter_jobs: int = strawberry.field(name="deadLetterJobs")
    oldest_ready_age_seconds: float | None = strawberry.field(
        name="oldestReadyAgeSeconds", default=None
    )
    next_scheduled_in_seconds: float | None = strawberry.field(
        name="nextScheduledInSeconds", default=None
    )
    alert_level: str = strawberry.field(name="alertLevel")
    dead_letter_oldest_age_seconds: float | None = strawberry.field(
        name="deadLetterOldestAgeSeconds", default=None
    )
    dead_letter_reason_counts: JSON = strawberry.field(
        name="deadLetterReasonCounts", default_factory=dict
    )


@strawberry.type
class GQLMetadataReindexStatus:
    """Latest metadata reindex/reconciliation run summary."""

    queue_name: str = strawberry.field(name="queueName")
    schedule_offset_minutes: int = strawberry.field(name="scheduleOffsetMinutes")
    has_history: bool = strawberry.field(name="hasHistory")
    observed_at: str = strawberry.field(name="observedAt")
    processed: int
    queued: int
    reconciled: int
    skipped_active: int = strawberry.field(name="skippedActive")
    failed: int
    repair_attempted: int = strawberry.field(name="repairAttempted")
    repair_enriched: int = strawberry.field(name="repairEnriched")
    repair_skipped_no_tmdb_id: int = strawberry.field(name="repairSkippedNoTmdbId")
    repair_failed: int = strawberry.field(name="repairFailed")
    repair_requeued: int = strawberry.field(name="repairRequeued")
    repair_skipped_active: int = strawberry.field(name="repairSkippedActive")
    outcome: str
    run_failed: bool = strawberry.field(name="runFailed")
    last_error: str | None = strawberry.field(name="lastError", default=None)


@strawberry.type
class GQLMetadataReindexHistoryPoint:
    """One persisted metadata reindex/reconciliation history point."""

    observed_at: str = strawberry.field(name="observedAt")
    processed: int
    queued: int
    reconciled: int
    skipped_active: int = strawberry.field(name="skippedActive")
    failed: int
    repair_attempted: int = strawberry.field(name="repairAttempted")
    repair_enriched: int = strawberry.field(name="repairEnriched")
    repair_skipped_no_tmdb_id: int = strawberry.field(name="repairSkippedNoTmdbId")
    repair_failed: int = strawberry.field(name="repairFailed")
    repair_requeued: int = strawberry.field(name="repairRequeued")
    repair_skipped_active: int = strawberry.field(name="repairSkippedActive")
    outcome: str
    run_failed: bool = strawberry.field(name="runFailed")
    last_error: str | None = strawberry.field(name="lastError", default=None)


@strawberry.type
class GQLLibraryStats:
    """Intentional GraphQL stats type above the compatibility REST contract."""

    total_items: int = strawberry.field(name="totalItems")
    total_movies: int = strawberry.field(name="totalMovies")
    total_shows: int = strawberry.field(name="totalShows")
    total_seasons: int = strawberry.field(name="totalSeasons")
    total_episodes: int = strawberry.field(name="totalEpisodes")
    completed_items: int = strawberry.field(name="completedItems")
    incomplete_items: int = strawberry.field(name="incompleteItems")
    failed_items: int = strawberry.field(name="failedItems")
    # Placeholder until the future frontend defines the exact richer JSON contract it wants.
    state_breakdown: str | None = strawberry.field(name="stateBreakdown", default=None)
    # Placeholder until the future frontend defines the exact richer JSON contract it wants.
    activity: str | None = None


@strawberry.type
class GQLStreamCandidate:
    """GraphQL stream-candidate projection for intentional media detail queries."""

    id: strawberry.ID
    raw_title: str = strawberry.field(name="rawTitle")
    parsed_title: str | None = strawberry.field(name="parsedTitle", default=None)
    resolution: str | None = None
    rank_score: int = strawberry.field(name="rankScore")
    lev_ratio: float | None = strawberry.field(name="levRatio", default=None)
    selected: bool
    passed: bool | None = None
    rejection_reason: str | None = strawberry.field(name="rejectionReason", default=None)


@strawberry.type
class GQLRecoveryPlan:
    """Intentional recovery projection above the REST compatibility surface."""

    mechanism: GQLRecoveryMechanism
    target_stage: GQLRecoveryTargetStage = strawberry.field(name="targetStage")
    reason: str
    next_retry_at: str | None = strawberry.field(name="nextRetryAt", default=None)
    recovery_attempt_count: int = strawberry.field(name="recoveryAttemptCount")
    is_in_cooldown: bool = strawberry.field(name="isInCooldown")


@strawberry.type
class GQLPlaybackAttachment:
    """Persisted playback attachment projection for graph item detail."""

    id: str
    kind: str
    locator: str
    source_key: str | None = strawberry.field(name="sourceKey", default=None)
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)
    original_filename: str | None = strawberry.field(name="originalFilename", default=None)
    file_size: int | None = strawberry.field(name="fileSize", default=None)
    local_path: str | None = strawberry.field(name="localPath", default=None)
    restricted_url: str | None = strawberry.field(name="restrictedUrl", default=None)
    unrestricted_url: str | None = strawberry.field(name="unrestrictedUrl", default=None)
    is_preferred: bool = strawberry.field(name="isPreferred")
    preference_rank: int = strawberry.field(name="preferenceRank")
    refresh_state: str = strawberry.field(name="refreshState")
    expires_at: str | None = strawberry.field(name="expiresAt", default=None)
    last_refreshed_at: str | None = strawberry.field(name="lastRefreshedAt", default=None)
    last_refresh_error: str | None = strawberry.field(name="lastRefreshError", default=None)


@strawberry.type
class GQLResolvedPlaybackAttachment:
    """Current resolved playback attachment for direct or HLS access."""

    kind: str
    locator: str
    source_key: str = strawberry.field(name="sourceKey")
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)
    original_filename: str | None = strawberry.field(name="originalFilename", default=None)
    file_size: int | None = strawberry.field(name="fileSize", default=None)
    local_path: str | None = strawberry.field(name="localPath", default=None)
    restricted_url: str | None = strawberry.field(name="restrictedUrl", default=None)
    unrestricted_url: str | None = strawberry.field(name="unrestrictedUrl", default=None)


@strawberry.type
class GQLResolvedPlayback:
    """Resolved playback readiness snapshot for one item."""

    direct: GQLResolvedPlaybackAttachment | None = None
    hls: GQLResolvedPlaybackAttachment | None = None
    direct_ready: bool = strawberry.field(name="directReady")
    hls_ready: bool = strawberry.field(name="hlsReady")
    missing_local_file: bool = strawberry.field(name="missingLocalFile")


@strawberry.type
class GQLActiveStreamOwner:
    """Ownership link from an active playback role to one media entry."""

    media_entry_index: int = strawberry.field(name="mediaEntryIndex")
    kind: str
    original_filename: str | None = strawberry.field(name="originalFilename", default=None)
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)


@strawberry.type
class GQLActiveStream:
    """Current active-stream readiness and owner mapping."""

    direct_ready: bool = strawberry.field(name="directReady")
    hls_ready: bool = strawberry.field(name="hlsReady")
    missing_local_file: bool = strawberry.field(name="missingLocalFile")
    direct_owner: GQLActiveStreamOwner | None = strawberry.field(name="directOwner", default=None)
    hls_owner: GQLActiveStreamOwner | None = strawberry.field(name="hlsOwner", default=None)


@strawberry.type
class GQLMediaEntry:
    """Mounted/playback-facing media-entry projection for graph item detail."""

    entry_type: str = strawberry.field(name="entryType")
    kind: str
    original_filename: str | None = strawberry.field(name="originalFilename", default=None)
    url: str | None = None
    local_path: str | None = strawberry.field(name="localPath", default=None)
    download_url: str | None = strawberry.field(name="downloadUrl", default=None)
    unrestricted_url: str | None = strawberry.field(name="unrestrictedUrl", default=None)
    provider: str | None = None
    provider_download_id: str | None = strawberry.field(name="providerDownloadId", default=None)
    provider_file_id: str | None = strawberry.field(name="providerFileId", default=None)
    provider_file_path: str | None = strawberry.field(name="providerFilePath", default=None)
    size: int | None = None
    created: str | None = None
    modified: str | None = None
    refresh_state: str = strawberry.field(name="refreshState")
    expires_at: str | None = strawberry.field(name="expiresAt", default=None)
    last_refreshed_at: str | None = strawberry.field(name="lastRefreshedAt", default=None)
    last_refresh_error: str | None = strawberry.field(name="lastRefreshError", default=None)
    active_for_direct: bool = strawberry.field(name="activeForDirect")
    active_for_hls: bool = strawberry.field(name="activeForHls")
    is_active_stream: bool = strawberry.field(name="isActiveStream")


@strawberry.type
class GQLMediaItemDetail:
    """Intentional GraphQL item detail type with stream-candidate visibility."""

    id: strawberry.ID
    title: str
    state: str
    item_type: str | None = strawberry.field(name="itemType", default=None)
    media_type: str = strawberry.field(name="mediaType")
    media_kind: MediaKind = strawberry.field(name="mediaKind")
    tmdb_id: int | None = strawberry.field(name="tmdbId", default=None)
    tvdb_id: int | None = strawberry.field(name="tvdbId", default=None)
    imdb_id: str | None = strawberry.field(name="imdbId", default=None)
    parent_tmdb_id: int | None = strawberry.field(name="parentTmdbId", default=None)
    parent_tvdb_id: int | None = strawberry.field(name="parentTvdbId", default=None)
    show_title: str | None = strawberry.field(name="showTitle", default=None)
    season_number: int | None = strawberry.field(name="seasonNumber", default=None)
    episode_number: int | None = strawberry.field(name="episodeNumber", default=None)
    created_at: str = strawberry.field(name="createdAt")
    updated_at: str = strawberry.field(name="updatedAt")
    stream_candidates: list[GQLStreamCandidate] = strawberry.field(name="streamCandidates")
    selected_stream: GQLStreamCandidate | None = strawberry.field(
        name="selectedStream", default=None
    )
    recovery_plan: GQLRecoveryPlan = strawberry.field(name="recoveryPlan")
    playback_attachments: list[GQLPlaybackAttachment] = strawberry.field(
        name="playbackAttachments", default_factory=list
    )
    resolved_playback: GQLResolvedPlayback | None = strawberry.field(
        name="resolvedPlayback", default=None
    )
    active_stream: GQLActiveStream | None = strawberry.field(name="activeStream", default=None)
    media_entries: list[GQLMediaEntry] = strawberry.field(name="mediaEntries", default_factory=list)


@strawberry.type
class GQLFilmuSettings:
    """Core filmu settings exposed through GraphQL compatibility schema."""

    version: str
    api_key: str = strawberry.field(name="apiKey")
    log_level: str = strawberry.field(name="logLevel")


@strawberry.type
class GQLSettings:
    """Settings root object for parity with upstream GraphQL settings query."""

    filmu: GQLFilmuSettings


@strawberry.type
class GQLItemEvent:
    """Subscription event representing media item state transitions."""

    item_id: strawberry.ID
    state: str
    message: str


@strawberry.type
class ItemStateChangedEvent:
    """Mirrors the existing SSE `item.state.changed` payload as a compat GraphQL type."""

    # COMPAT: keep field names aligned with the current SSE contract until the new frontend expands them.
    item_id: str = strawberry.field(name="item_id")
    from_state: str | None = strawberry.field(name="from_state", default=None)
    to_state: str = strawberry.field(name="to_state")
    timestamp: str


@strawberry.type
class RetryItemResult:
    item_id: str = strawberry.field(name="itemId")
    success: bool
    error: str | None = None
    new_state: str | None = strawberry.field(name="newState", default=None)


@strawberry.type
class ResetItemResult:
    item_id: str = strawberry.field(name="itemId")
    success: bool
    error: str | None = None
    new_state: str | None = strawberry.field(name="newState", default=None)


@strawberry.type
class LogEntry:
    """Intentional structured log-stream entry for future GraphQL consumers."""

    timestamp: str
    level: str
    event: str
    worker_id: str | None = strawberry.field(name="worker_id", default=None)
    item_id: str | None = strawberry.field(name="item_id", default=None)
    stage: str | None = None
    extra: JSON = strawberry.field(default_factory=dict)


@strawberry.type
class NotificationEvent:
    """Mirrors the existing SSE notification payload as a compat GraphQL type."""

    # COMPAT: keep field names aligned with the current SSE contract until the new frontend expands them.
    event_type: str = strawberry.field(name="event_type")
    title: str | None = None
    message: str | None = None
    timestamp: str


@strawberry.type
class RequestItemResult:
    """Additive request-intake result for future GraphQL consumers."""

    item_id: strawberry.ID = strawberry.field(name="itemId")
    enrichment_source: str = strawberry.field(name="enrichmentSource")
    has_poster: bool = strawberry.field(name="hasPoster")
    has_imdb_id: bool = strawberry.field(name="hasImdbId")
    warnings: list[str]


@strawberry.input
class RequestItemInput:
    """Request a media item by external identifier."""

    external_ref: str = strawberry.field(name="externalRef")
    media_type: str = strawberry.field(name="mediaType")
    requested_seasons: list[int] | None = strawberry.field(name="requestedSeasons", default=None)


@strawberry.input
class ItemActionInput:
    """Trigger a state action on an existing item."""

    item_id: str = strawberry.field(name="itemId")
    action: str


@strawberry.input
class SettingsUpdateInput:
    """Update one settings path with a JSON-serializable value."""

    path: str
    value: strawberry.scalars.JSON


@strawberry.enum
class GQLItemTransitionEvent(StrEnum):
    """Allowed item transition events for mutation operations."""

    INDEX = "index"
    SCRAPE = "scrape"
    DOWNLOAD = "download"
    COMPLETE = "complete"
    FAIL = "fail"
    RETRY = "retry"
    PARTIAL_COMPLETE = "partial_complete"
    MARK_ONGOING = "mark_ongoing"
    MARK_UNRELEASED = "mark_unreleased"
