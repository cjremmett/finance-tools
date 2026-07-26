"""Create Kalshi market monitoring tables.

Revision ID: 0002
Revises: 0001
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "kalshi_markets"


def upgrade() -> None:
    op.execute(sa.schema.CreateSchema(SCHEMA, if_not_exists=True))
    op.create_table(
        "monitor_state",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "last_successful_cutoff", sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column("workflow_id", sa.String(length=255), nullable=False),
        sa.Column("workflow_run_id", sa.String(length=255), nullable=False),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("window_ended_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("notification_batches", sa.JSON(), nullable=False),
        sa.Column("next_batch_index", sa.Integer(), nullable=False),
        sa.Column("notification_status", sa.String(length=32), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA,
    )
    op.create_table(
        "latest_markets",
        sa.Column("ticker", sa.String(length=255), nullable=False),
        sa.Column("event_ticker", sa.String(length=255), nullable=False),
        sa.Column("series_ticker", sa.String(length=255), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("subtitle", sa.Text(), nullable=True),
        sa.Column("category", sa.String(length=255), nullable=False),
        sa.Column("created_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("close_time", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("ticker"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_latest_markets_category",
        "latest_markets",
        ["category"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_latest_markets_category",
        table_name="latest_markets",
        schema=SCHEMA,
    )
    op.drop_table("latest_markets", schema=SCHEMA)
    op.drop_table("monitor_state", schema=SCHEMA)
    op.execute(sa.schema.DropSchema(SCHEMA))

