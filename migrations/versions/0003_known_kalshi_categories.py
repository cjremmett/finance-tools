"""Seed the Kalshi category registry.

Revision ID: 0003
Revises: 0002
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | Sequence[str] | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "kalshi_markets"
KNOWN_CATEGORIES = (
    "Climate and Weather",
    "Commodities",
    "Companies",
    "Crypto",
    "Economics",
    "Elections",
    "Entertainment",
    "Financials",
    "Mentions",
    "Politics",
    "Science and Technology",
    "Social",
    "Sports",
)


def upgrade() -> None:
    table = op.create_table(
        "known_categories",
        sa.Column("category", sa.String(length=255), nullable=False),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("category"),
        schema=SCHEMA,
    )
    op.bulk_insert(
        table,
        [{"category": category} for category in KNOWN_CATEGORIES],
    )


def downgrade() -> None:
    op.drop_table("known_categories", schema=SCHEMA)

