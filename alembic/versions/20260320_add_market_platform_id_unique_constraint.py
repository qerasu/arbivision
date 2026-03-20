"""Add unique constraint to market identifier per platform

Revision ID: 9f7e2c4d8a11
Revises: 6aa3a18e4724
Create Date: 2026-03-20 00:00:00.000000
"""

from alembic import op


revision = "9f7e2c4d8a11"
down_revision = "6aa3a18e4724"
branch_labels = None
depends_on = None


def upgrade():
    op.create_unique_constraint(
        "uq_markets_platform_market_id",
        "markets",
        ["platform", "platform_market_id"],
    )


def downgrade():
    op.drop_constraint("uq_markets_platform_market_id", "markets", type_="unique")
