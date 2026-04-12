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
    release_data: str | None = strawberry.field(name="releaseData", default=None)


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
    created_at: str = strawberry.field(name="createdAt")
    updated_at: str = strawberry.field(name="updatedAt")
    stream_candidates: list[GQLStreamCandidate] = strawberry.field(name="streamCandidates")
    selected_stream: GQLStreamCandidate | None = strawberry.field(
        name="selectedStream", default=None
    )
    recovery_plan: GQLRecoveryPlan = strawberry.field(name="recoveryPlan")


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
