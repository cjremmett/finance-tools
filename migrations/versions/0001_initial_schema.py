"""Create interest-rate monitoring tables.

Revision ID: 0001
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "interest_rates"


def upgrade() -> None:
    op.execute(sa.schema.CreateSchema(SCHEMA, if_not_exists=True))
    op.create_table(
        "scrape_runs",
        sa.Column("workflow_run_id", sa.String(length=255), nullable=False),
        sa.Column("workflow_id", sa.String(length=255), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("baseline", sa.Boolean(), nullable=False),
        sa.Column("change_payload", sa.JSON(), nullable=False),
        sa.Column("notification_status", sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint("workflow_run_id"),
        schema=SCHEMA,
    )
    op.create_table(
        "products",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("product_key", sa.String(length=255), nullable=False),
        sa.Column("product_name", sa.String(length=255), nullable=False),
        sa.Column("product_type", sa.String(length=128), nullable=False),
        sa.Column("term_months", sa.Integer(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source", "product_key", name="uq_product_source_key"),
        schema=SCHEMA,
    )
    op.create_table(
        "source_runs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("workflow_run_id", sa.String(length=255), nullable=False),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("source_effective_date", sa.Date(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["workflow_run_id"],
            [f"{SCHEMA}.scrape_runs.workflow_run_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("workflow_run_id", "source", name="uq_source_run"),
        schema=SCHEMA,
    )
    op.create_table(
        "rate_observations",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("workflow_run_id", sa.String(length=255), nullable=False),
        sa.Column("product_id", sa.BigInteger(), nullable=False),
        sa.Column("rate_percent", sa.Numeric(precision=9, scale=5), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_effective_date", sa.Date(), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ["product_id"], [f"{SCHEMA}.products.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["workflow_run_id"],
            [f"{SCHEMA}.scrape_runs.workflow_run_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "workflow_run_id", "product_id", name="uq_observation_run_product"
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_source_runs_latest",
        "source_runs",
        ["source", "observed_at"],
        schema=SCHEMA,
    )
    op.create_index(
        "ix_rate_observations_product_time",
        "rate_observations",
        ["product_id", "observed_at"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_rate_observations_product_time",
        table_name="rate_observations",
        schema=SCHEMA,
    )
    op.drop_index("ix_source_runs_latest", table_name="source_runs", schema=SCHEMA)
    op.drop_table("rate_observations", schema=SCHEMA)
    op.drop_table("source_runs", schema=SCHEMA)
    op.drop_table("products", schema=SCHEMA)
    op.drop_table("scrape_runs", schema=SCHEMA)
    op.execute(sa.schema.DropSchema(SCHEMA))
