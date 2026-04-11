"""ORM persistence models for media items, playback attachments, and lifecycle events."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from filmu_py.db.base import Base


class SettingsORM(Base):
    """Single-row persisted compatibility settings blob."""

    __tablename__ = "settings"
    __table_args__ = (CheckConstraint("id = 1", name="ck_settings_single_row"),)

    id: Mapped[int] = mapped_column(
        Integer(), primary_key=True, nullable=False, server_default=text("1")
    )
    data: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )


class TenantORM(Base):
    """First-class tenant/org boundary for resource ownership and authz evolution."""

    __tablename__ = "tenants"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    slug: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(256), nullable=False)
    kind: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="tenant",
        server_default=text("'tenant'"),
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="active",
        server_default=text("'active'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    principals: Mapped[list[PrincipalORM]] = relationship(back_populates="tenant")
    media_items: Mapped[list[MediaItemORM]] = relationship(back_populates="tenant")
    item_requests: Mapped[list[ItemRequestORM]] = relationship(back_populates="tenant")


class PrincipalORM(Base):
    """Persisted operator or service identity resolved from authenticated requests."""

    __tablename__ = "principals"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    tenant_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    principal_key: Mapped[str] = mapped_column(String(256), nullable=False, unique=True, index=True)
    principal_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    authentication_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(256), default=None)
    roles: Mapped[list[str]] = mapped_column(JSONB, default=list)
    scopes: Mapped[list[str]] = mapped_column(JSONB, default=list)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="active",
        server_default=text("'active'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )
    last_authenticated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
    )

    tenant: Mapped[TenantORM] = relationship(back_populates="principals")
    service_account: Mapped[ServiceAccountORM | None] = relationship(back_populates="principal")


class ServiceAccountORM(Base):
    """Persisted machine credential anchored to one principal and API key identifier."""

    __tablename__ = "service_accounts"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    principal_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("principals.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    api_key_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="active",
        server_default=text("'active'"),
    )
    description: Mapped[str | None] = mapped_column(String(512), default=None)
    last_authenticated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    principal: Mapped[PrincipalORM] = relationship(back_populates="service_account")


class AccessPolicyRevisionORM(Base):
    """Persisted access-policy inventory for operator-visible authz governance."""

    __tablename__ = "access_policy_revisions"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    version: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    source: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default="settings_bootstrap",
        server_default=text("'settings_bootstrap'"),
    )
    approval_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="bootstrap",
        server_default=text("'bootstrap'"),
        index=True,
    )
    proposed_by: Mapped[str | None] = mapped_column(String(256), default=None)
    approved_by: Mapped[str | None] = mapped_column(String(256), default=None)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    approval_notes: Mapped[str | None] = mapped_column(Text(), default=None)
    policy_data: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, default=dict)
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
        index=True,
    )
    activated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )


class PluginGovernanceOverrideORM(Base):
    """Persisted operator-managed quarantine/revocation/approval state per plugin."""

    __tablename__ = "plugin_governance_overrides"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    plugin_name: Mapped[str] = mapped_column(String(256), nullable=False, unique=True, index=True)
    state: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="approved",
        server_default=text("'approved'"),
        index=True,
    )
    reason: Mapped[str | None] = mapped_column(String(512), default=None)
    notes: Mapped[str | None] = mapped_column(Text(), default=None)
    updated_by: Mapped[str | None] = mapped_column(String(256), default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )


class ControlPlaneSubscriberORM(Base):
    """Durable resume and ownership ledger for distributed replay/control-plane consumers."""

    __tablename__ = "control_plane_subscribers"
    __table_args__ = (
        UniqueConstraint(
            "stream_name",
            "group_name",
            "consumer_name",
            name="uq_control_plane_subscriber_identity",
        ),
        Index(
            "ix_control_plane_subscribers_status_heartbeat",
            "status",
            "last_heartbeat_at",
        ),
    )

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    stream_name: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    group_name: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    consumer_name: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    node_id: Mapped[str] = mapped_column(String(256), nullable=False, default="unknown")
    tenant_id: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="active",
        server_default=text("'active'"),
        index=True,
    )
    last_read_offset: Mapped[str | None] = mapped_column(String(128), default=None)
    last_delivered_event_id: Mapped[str | None] = mapped_column(String(128), default=None)
    last_acked_event_id: Mapped[str | None] = mapped_column(String(128), default=None)
    last_error: Mapped[str | None] = mapped_column(Text(), default=None)
    claimed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    last_heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )


class MediaItemORM(Base):
    """Persistent media item tracked through pipeline lifecycle states."""

    __tablename__ = "media_items"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    tenant_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        default="global",
        server_default=text("'global'"),
        index=True,
    )
    external_ref: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(512))
    state: Mapped[str] = mapped_column(String(64), index=True)
    recovery_attempt_count: Mapped[int] = mapped_column(Integer(), default=0, nullable=False)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    attributes: Mapped[dict[str, object]] = mapped_column("metadata", JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    tenant: Mapped[TenantORM] = relationship(back_populates="media_items")
    events: Mapped[list[ItemStateEventORM]] = relationship(
        back_populates="item",
        cascade="all, delete-orphan",
        order_by="ItemStateEventORM.created_at",
    )
    item_requests: Mapped[list[ItemRequestORM]] = relationship(
        back_populates="media_item",
        order_by="ItemRequestORM.last_requested_at",
    )
    movie: Mapped[MovieORM | None] = relationship(back_populates="media_item")
    show: Mapped[ShowORM | None] = relationship(back_populates="media_item")
    season: Mapped[SeasonORM | None] = relationship(back_populates="media_item")
    episode: Mapped[EpisodeORM | None] = relationship(back_populates="media_item")
    scrape_candidates: Mapped[list[ScrapeCandidateORM]] = relationship(
        back_populates="item",
        cascade="all, delete-orphan",
        order_by="ScrapeCandidateORM.created_at",
    )
    streams: Mapped[list[StreamORM]] = relationship(
        back_populates="media_item",
        cascade="all, delete-orphan",
        order_by="StreamORM.created_at",
    )
    blacklisted_stream_relations: Mapped[list[StreamBlacklistRelationORM]] = relationship(
        back_populates="media_item",
        cascade="all, delete-orphan",
        order_by="StreamBlacklistRelationORM.created_at",
    )
    playback_attachments: Mapped[list[PlaybackAttachmentORM]] = relationship(
        back_populates="item",
        cascade="all, delete-orphan",
        order_by="PlaybackAttachmentORM.created_at",
    )
    media_entries: Mapped[list[MediaEntryORM]] = relationship(
        back_populates="item",
        cascade="all, delete-orphan",
        order_by="MediaEntryORM.created_at",
    )
    subtitle_entries: Mapped[list[SubtitleEntryORM]] = relationship(
        back_populates="item",
        cascade="all, delete-orphan",
        order_by="SubtitleEntryORM.created_at",
    )
    active_streams: Mapped[list[ActiveStreamORM]] = relationship(
        back_populates="item",
        cascade="all, delete-orphan",
        order_by="ActiveStreamORM.created_at",
    )
    outbox_events: Mapped[list[OutboxEventORM]] = relationship(
        back_populates="item",
        cascade="all, delete-orphan",
        order_by="OutboxEventORM.created_at",
    )


class PlaybackAttachmentORM(Base):
    """Persisted playback attachment/link record for one media item."""

    __tablename__ = "playback_attachments"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        index=True,
    )
    kind: Mapped[str] = mapped_column(String(32), index=True)
    locator: Mapped[str] = mapped_column(String(2048))
    source_key: Mapped[str | None] = mapped_column(String(128), default=None)
    provider: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    provider_download_id: Mapped[str | None] = mapped_column(String(128), default=None)
    provider_file_id: Mapped[str | None] = mapped_column(String(128), default=None)
    provider_file_path: Mapped[str | None] = mapped_column(String(2048), default=None)
    original_filename: Mapped[str | None] = mapped_column(String(1024), default=None)
    file_size: Mapped[int | None] = mapped_column(BigInteger(), default=None)
    local_path: Mapped[str | None] = mapped_column(String(2048), default=None)
    restricted_url: Mapped[str | None] = mapped_column(String(2048), default=None)
    unrestricted_url: Mapped[str | None] = mapped_column(String(2048), default=None)
    is_preferred: Mapped[bool] = mapped_column(default=False, nullable=False)
    preference_rank: Mapped[int] = mapped_column(default=100, nullable=False)
    refresh_state: Mapped[str] = mapped_column(String(32), default="ready", nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    last_refresh_error: Mapped[str | None] = mapped_column(Text(), default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    item: Mapped[MediaItemORM] = relationship(back_populates="playback_attachments")
    media_entries: Mapped[list[MediaEntryORM]] = relationship(back_populates="source_attachment")


class ItemRequestORM(Base):
    """Persistent request-intent record kept separate from media lifecycle state."""

    __tablename__ = "item_requests"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "external_ref",
            name="uq_item_requests_tenant_external_ref",
        ),
    )

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    tenant_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        default="global",
        server_default=text("'global'"),
        index=True,
    )
    external_ref: Mapped[str] = mapped_column(String(128), index=True)
    media_item_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="SET NULL"),
        index=True,
        default=None,
    )
    media_type: Mapped[str] = mapped_column(String(32), nullable=False, default="unknown")
    requested_title: Mapped[str] = mapped_column(String(512), nullable=False)
    requested_seasons: Mapped[list[int] | None] = mapped_column(JSONB, nullable=True)
    requested_episodes: Mapped[dict[str, list[int]] | None] = mapped_column(JSONB, nullable=True)
    is_partial: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    request_source: Mapped[str] = mapped_column(String(64), nullable=False, default="api")
    request_count: Mapped[int] = mapped_column(nullable=False, default=1)
    first_requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    last_requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    tenant: Mapped[TenantORM] = relationship(back_populates="item_requests")
    media_item: Mapped[MediaItemORM | None] = relationship(back_populates="item_requests")


class SubtitleEntryORM(Base):
    """Persisted subtitle record linked to one media item."""

    __tablename__ = "subtitle_entries"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        index=True,
    )
    language: Mapped[str] = mapped_column(String(10), nullable=False)
    format: Mapped[str] = mapped_column(String(20), nullable=False)
    source: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="unknown",
        server_default=text("'unknown'"),
    )
    url: Mapped[str | None] = mapped_column(Text(), default=None)
    file_path: Mapped[str | None] = mapped_column(Text(), default=None)
    is_default: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    is_forced: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    provider_subtitle_id: Mapped[str | None] = mapped_column(String(200), default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    item: Mapped[MediaItemORM] = relationship(back_populates="subtitle_entries")


class MovieORM(Base):
    """Movie-specialization row linked one-to-one with the lifecycle carrier item."""

    __tablename__ = "movies"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    media_item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    tmdb_id: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    imdb_id: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    media_item: Mapped[MediaItemORM] = relationship(back_populates="movie")


class ShowORM(Base):
    """Show-specialization row linked one-to-one with the lifecycle carrier item."""

    __tablename__ = "shows"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    media_item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    tmdb_id: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    tvdb_id: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    imdb_id: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    media_item: Mapped[MediaItemORM] = relationship(back_populates="show")
    seasons: Mapped[list[SeasonORM]] = relationship(back_populates="show")


class SeasonORM(Base):
    """Season-specialization row linked one-to-one with the lifecycle carrier item."""

    __tablename__ = "seasons"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    media_item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    show_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=False), ForeignKey("shows.id", ondelete="SET NULL"), default=None, index=True
    )
    season_number: Mapped[int | None] = mapped_column(Integer(), default=None)
    tmdb_id: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    tvdb_id: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    media_item: Mapped[MediaItemORM] = relationship(back_populates="season")
    show: Mapped[ShowORM | None] = relationship(back_populates="seasons")
    episodes: Mapped[list[EpisodeORM]] = relationship(back_populates="season")


class EpisodeORM(Base):
    """Episode-specialization row linked one-to-one with the lifecycle carrier item."""

    __tablename__ = "episodes"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    media_item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    season_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=False), ForeignKey("seasons.id", ondelete="SET NULL"), default=None, index=True
    )
    episode_number: Mapped[int | None] = mapped_column(Integer(), default=None)
    tmdb_id: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    tvdb_id: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    imdb_id: Mapped[str | None] = mapped_column(String(64), default=None, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    media_item: Mapped[MediaItemORM] = relationship(back_populates="episode")
    season: Mapped[SeasonORM | None] = relationship(back_populates="episodes")


class StreamORM(Base):
    """Persisted scraped/ranked stream candidate attached to one media item."""

    __tablename__ = "streams"
    __table_args__ = (Index("ix_streams_media_item_selected", "media_item_id", "selected"),)

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    media_item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    infohash: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    raw_title: Mapped[str] = mapped_column(String(2048), nullable=False)
    parsed_title: Mapped[dict[str, object]] = mapped_column(JSONB, default=dict)
    rank: Mapped[int] = mapped_column(nullable=False, default=0)
    lev_ratio: Mapped[float | None] = mapped_column(Float(), default=None)
    resolution: Mapped[str | None] = mapped_column(String(32), default=None, index=True)
    selected: Mapped[bool] = mapped_column(Boolean(), default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    media_item: Mapped[MediaItemORM] = relationship(back_populates="streams")
    blacklist_relations: Mapped[list[StreamBlacklistRelationORM]] = relationship(
        back_populates="stream",
        cascade="all, delete-orphan",
        order_by="StreamBlacklistRelationORM.created_at",
    )
    parent_relations: Mapped[list[StreamRelationORM]] = relationship(
        back_populates="parent_stream",
        cascade="all, delete-orphan",
        foreign_keys="StreamRelationORM.parent_stream_id",
        order_by="StreamRelationORM.created_at",
    )
    child_relations: Mapped[list[StreamRelationORM]] = relationship(
        back_populates="child_stream",
        cascade="all, delete-orphan",
        foreign_keys="StreamRelationORM.child_stream_id",
        order_by="StreamRelationORM.created_at",
    )


class ScrapeCandidateORM(Base):
    """Raw scrape-stage candidate persisted before RTN parse/validation."""

    __tablename__ = "scrape_candidates"
    __table_args__ = (
        UniqueConstraint("item_id", "info_hash", name="uq_scrape_candidates_item_info_hash"),
    )

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    info_hash: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    raw_title: Mapped[str] = mapped_column(String(2048), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger(), default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )

    item: Mapped[MediaItemORM] = relationship(back_populates="scrape_candidates")


class StreamBlacklistRelationORM(Base):
    """Relation marking one stream candidate as blacklisted for one media item."""

    __tablename__ = "stream_blacklist_relations"
    __table_args__ = (
        UniqueConstraint("media_item_id", "stream_id", name="uq_stream_blacklist_media_stream"),
    )

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    media_item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    stream_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("streams.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )

    media_item: Mapped[MediaItemORM] = relationship(back_populates="blacklisted_stream_relations")
    stream: Mapped[StreamORM] = relationship(back_populates="blacklist_relations")


class StreamRelationORM(Base):
    """Parent-child relation between two persisted stream candidates."""

    __tablename__ = "stream_relations"
    __table_args__ = (
        UniqueConstraint(
            "parent_stream_id", "child_stream_id", name="uq_stream_relations_parent_child"
        ),
    )

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    parent_stream_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("streams.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    child_stream_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("streams.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )

    parent_stream: Mapped[StreamORM] = relationship(
        back_populates="parent_relations",
        foreign_keys=[parent_stream_id],
    )
    child_stream: Mapped[StreamORM] = relationship(
        back_populates="child_relations",
        foreign_keys=[child_stream_id],
    )


class MediaEntryORM(Base):
    """Persisted VFS/media-entry record for one media item."""

    __tablename__ = "media_entries"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        index=True,
    )
    source_attachment_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("playback_attachments.id", ondelete="SET NULL"),
        default=None,
        index=True,
    )
    entry_type: Mapped[str] = mapped_column(String(32), default="media", nullable=False)
    kind: Mapped[str] = mapped_column(String(32), index=True)
    original_filename: Mapped[str | None] = mapped_column(String(1024), default=None)
    local_path: Mapped[str | None] = mapped_column(String(2048), default=None)
    download_url: Mapped[str | None] = mapped_column(String(2048), default=None)
    unrestricted_url: Mapped[str | None] = mapped_column(String(2048), default=None)
    provider: Mapped[str | None] = mapped_column(String(64), index=True, default=None)
    provider_download_id: Mapped[str | None] = mapped_column(String(128), default=None)
    provider_file_id: Mapped[str | None] = mapped_column(String(128), default=None)
    provider_file_path: Mapped[str | None] = mapped_column(String(2048), default=None)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger(), default=None)
    refresh_state: Mapped[str] = mapped_column(String(32), default="ready", nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    last_refresh_error: Mapped[str | None] = mapped_column(Text(), default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    item: Mapped[MediaItemORM] = relationship(back_populates="media_entries")
    source_attachment: Mapped[PlaybackAttachmentORM | None] = relationship(
        back_populates="media_entries"
    )
    active_streams: Mapped[list[ActiveStreamORM]] = relationship(back_populates="media_entry")


class ActiveStreamORM(Base):
    """Persisted active-stream selection keyed to one media-entry row."""

    __tablename__ = "active_streams"
    __table_args__ = (UniqueConstraint("item_id", "role", name="uq_active_streams_item_role"),)

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        index=True,
    )
    media_entry_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_entries.id", ondelete="CASCADE"),
        index=True,
    )
    role: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
        onupdate=func.now(),
    )

    item: Mapped[MediaItemORM] = relationship(back_populates="active_streams")
    media_entry: Mapped[MediaEntryORM] = relationship(back_populates="active_streams")


class ItemStateEventORM(Base):
    """Immutable state transition record for a media item."""

    __tablename__ = "item_state_events"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        index=True,
    )
    event: Mapped[str] = mapped_column(String(64), index=True)
    previous_state: Mapped[str] = mapped_column(String(64))
    next_state: Mapped[str] = mapped_column(String(64), index=True)
    message: Mapped[str | None] = mapped_column(Text(), default=None)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )

    item: Mapped[MediaItemORM] = relationship(back_populates="events")


class OutboxEventORM(Base):
    """Transactional outbox row for deferred event-bus publication."""

    __tablename__ = "outbox_events"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    event_type: Mapped[str] = mapped_column(String(128), index=True)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, default=dict)
    item_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("media_items.id", ondelete="CASCADE"),
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=func.now(),
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None, index=True
    )
    failed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None, index=True
    )
    attempt_count: Mapped[int] = mapped_column(Integer(), default=0, nullable=False)

    item: Mapped[MediaItemORM] = relationship(back_populates="outbox_events")


Index(
    "ix_playback_attachments_item_preferred",
    PlaybackAttachmentORM.item_id,
    PlaybackAttachmentORM.is_preferred,
)
Index(
    "ix_playback_attachments_item_rank",
    PlaybackAttachmentORM.item_id,
    PlaybackAttachmentORM.preference_rank,
)
Index("ix_media_entries_item_created", MediaEntryORM.item_id, MediaEntryORM.created_at)
Index("ix_media_entries_item_refresh_state", MediaEntryORM.item_id, MediaEntryORM.refresh_state)
Index("ix_active_streams_item_created", ActiveStreamORM.item_id, ActiveStreamORM.created_at)
Index("ix_item_state_events_item_created", ItemStateEventORM.item_id, ItemStateEventORM.created_at)
Index("ix_outbox_events_item_created", OutboxEventORM.item_id, OutboxEventORM.created_at)
Index("ix_outbox_events_pending_created", OutboxEventORM.published_at, OutboxEventORM.created_at)
Index(
    "ix_scrape_candidates_item_created", ScrapeCandidateORM.item_id, ScrapeCandidateORM.created_at
)
Index("ix_streams_media_item_created", StreamORM.media_item_id, StreamORM.created_at)
Index(
    "ix_stream_blacklist_media_item_created",
    StreamBlacklistRelationORM.media_item_id,
    StreamBlacklistRelationORM.created_at,
)
Index(
    "ix_stream_relations_parent_created",
    StreamRelationORM.parent_stream_id,
    StreamRelationORM.created_at,
)
