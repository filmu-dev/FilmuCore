"""Add additive media specialization tables.

Revision ID: 20260314_0010
Revises: 20260314_0009
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260314_0010"
down_revision = "20260314_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "movies",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("media_item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("tmdb_id", sa.String(length=64), nullable=True),
        sa.Column("imdb_id", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["media_item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("media_item_id", name="uq_movies_media_item_id"),
    )
    op.create_index("ix_movies_media_item_id", "movies", ["media_item_id"], unique=False)
    op.create_index("ix_movies_tmdb_id", "movies", ["tmdb_id"], unique=False)
    op.create_index("ix_movies_imdb_id", "movies", ["imdb_id"], unique=False)

    op.create_table(
        "shows",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("media_item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("tmdb_id", sa.String(length=64), nullable=True),
        sa.Column("tvdb_id", sa.String(length=64), nullable=True),
        sa.Column("imdb_id", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["media_item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("media_item_id", name="uq_shows_media_item_id"),
    )
    op.create_index("ix_shows_media_item_id", "shows", ["media_item_id"], unique=False)
    op.create_index("ix_shows_tmdb_id", "shows", ["tmdb_id"], unique=False)
    op.create_index("ix_shows_tvdb_id", "shows", ["tvdb_id"], unique=False)
    op.create_index("ix_shows_imdb_id", "shows", ["imdb_id"], unique=False)

    op.create_table(
        "seasons",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("media_item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("show_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column("season_number", sa.Integer(), nullable=True),
        sa.Column("tmdb_id", sa.String(length=64), nullable=True),
        sa.Column("tvdb_id", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["media_item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["show_id"], ["shows.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("media_item_id", name="uq_seasons_media_item_id"),
    )
    op.create_index("ix_seasons_media_item_id", "seasons", ["media_item_id"], unique=False)
    op.create_index("ix_seasons_show_id", "seasons", ["show_id"], unique=False)
    op.create_index("ix_seasons_tmdb_id", "seasons", ["tmdb_id"], unique=False)
    op.create_index("ix_seasons_tvdb_id", "seasons", ["tvdb_id"], unique=False)

    op.create_table(
        "episodes",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("media_item_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("season_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column("episode_number", sa.Integer(), nullable=True),
        sa.Column("tmdb_id", sa.String(length=64), nullable=True),
        sa.Column("tvdb_id", sa.String(length=64), nullable=True),
        sa.Column("imdb_id", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["media_item_id"], ["media_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["season_id"], ["seasons.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("media_item_id", name="uq_episodes_media_item_id"),
    )
    op.create_index("ix_episodes_media_item_id", "episodes", ["media_item_id"], unique=False)
    op.create_index("ix_episodes_season_id", "episodes", ["season_id"], unique=False)
    op.create_index("ix_episodes_tmdb_id", "episodes", ["tmdb_id"], unique=False)
    op.create_index("ix_episodes_tvdb_id", "episodes", ["tvdb_id"], unique=False)
    op.create_index("ix_episodes_imdb_id", "episodes", ["imdb_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_episodes_imdb_id", table_name="episodes")
    op.drop_index("ix_episodes_tvdb_id", table_name="episodes")
    op.drop_index("ix_episodes_tmdb_id", table_name="episodes")
    op.drop_index("ix_episodes_season_id", table_name="episodes")
    op.drop_index("ix_episodes_media_item_id", table_name="episodes")
    op.drop_table("episodes")

    op.drop_index("ix_seasons_tvdb_id", table_name="seasons")
    op.drop_index("ix_seasons_tmdb_id", table_name="seasons")
    op.drop_index("ix_seasons_show_id", table_name="seasons")
    op.drop_index("ix_seasons_media_item_id", table_name="seasons")
    op.drop_table("seasons")

    op.drop_index("ix_shows_imdb_id", table_name="shows")
    op.drop_index("ix_shows_tvdb_id", table_name="shows")
    op.drop_index("ix_shows_tmdb_id", table_name="shows")
    op.drop_index("ix_shows_media_item_id", table_name="shows")
    op.drop_table("shows")

    op.drop_index("ix_movies_imdb_id", table_name="movies")
    op.drop_index("ix_movies_tmdb_id", table_name="movies")
    op.drop_index("ix_movies_media_item_id", table_name="movies")
    op.drop_table("movies")
