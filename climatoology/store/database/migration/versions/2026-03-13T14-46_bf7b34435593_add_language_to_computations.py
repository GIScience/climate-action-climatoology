"""add_language_to_computations

Revision ID: bf7b34435593
Revises: 7d48fb34ccbe
Create Date: 2026-03-13 14:46:36.990587

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'bf7b34435593'
down_revision: Union[str, None] = '7d48fb34ccbe'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('computation', sa.Column('language', sa.String(length=2), nullable=True), schema='ca_base')
    op.execute(
        sa.text(
            "update ca_base.computation set language=CASE WHEN params->>'language' IS NOT NULL THEN params->>'language' ELSE 'en' END"
        )
    )

    op.alter_column('computation', 'language', nullable=False, schema='ca_base')

    op.drop_column('computation', 'deduplication_key', schema='ca_base')
    op.add_column(
        'computation',
        sa.Column(
            'deduplication_key',
            sa.UUID(),
            sa.Computed('md5(requested_params::text||st_astext(aoi_geom)||language::text)::uuid'),
            nullable=False,
        ),
        schema='ca_base',
    )
    op.create_unique_constraint(
        'computation_deduplication_constraint',
        'computation',
        ['plugin_key', 'deduplication_key', 'cache_epoch'],
        schema='ca_base',
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('computation', 'deduplication_key', schema='ca_base')
    op.add_column(
        'computation',
        sa.Column(
            'deduplication_key',
            sa.UUID(),
            sa.Computed('md5(requested_params::text||st_astext(aoi_geom))::uuid'),
            nullable=False,
        ),
        schema='ca_base',
    )
    op.create_unique_constraint(
        'computation_deduplication_constraint',
        'computation',
        ['plugin_key', 'deduplication_key', 'cache_epoch'],
        schema='ca_base',
    )

    op.execute(
        sa.text(
            """
            UPDATE
                ca_base.computation
            SET
                params = (LEFT(params::text, -1) || CASE
                    WHEN params::text = '{}' THEN ''
                    ELSE ','
                END || '\"language\":\"' || LANGUAGE || '\"}')::json
            WHERE
                params->>'language' IS NULL;
            """
        )
    )

    op.drop_column('computation', 'language', schema='ca_base')
