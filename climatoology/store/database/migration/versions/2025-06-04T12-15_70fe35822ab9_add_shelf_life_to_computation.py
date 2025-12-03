"""add shelf life to computation

Revision ID: 70fe35822ab9
Revises: 3ac1827b309d
Create Date: 2025-06-04 12:15:32.972570

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '70fe35822ab9'
down_revision: Union[str, None] = '3ac1827b309d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('computation', sa.Column('cache_epoch', sa.Integer(), nullable=True), schema='ca-base')

    op.add_column('computation', sa.Column('valid_until', sa.DateTime(), nullable=True), schema='ca-base')
    op.execute(sa.text('update "ca-base".computation set valid_until=\'1970-01-01\'::timestamp'))
    op.alter_column('computation', 'valid_until', nullable=False, schema='ca-base')

    op.add_column(
        'computation',
        sa.Column('requested_params', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        schema='ca-base',
    )
    op.execute(sa.text('update "ca-base".computation set requested_params=\'{}\'::jsonb'))
    op.alter_column('computation', 'requested_params', nullable=False, schema='ca-base')

    op.alter_column(
        'computation', 'params', existing_type=postgresql.JSONB(astext_type=sa.Text()), nullable=True, schema='ca-base'
    )
    op.drop_constraint(op.f('computation_deduplication_constraint'), 'computation', schema='ca-base', type_='unique')
    op.create_unique_constraint(
        'computation_deduplication_constraint',
        'computation',
        ['cache_epoch', 'requested_params', 'aoi_geom', 'plugin_id', 'plugin_version'],
        schema='ca-base',
    )
    op.add_column('info', sa.Column('computation_shelf_life', sa.Interval(), nullable=True), schema='ca-base')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('info', 'computation_shelf_life', schema='ca-base')
    op.drop_constraint('computation_deduplication_constraint', 'computation', schema='ca-base', type_='unique')
    op.create_unique_constraint(
        op.f('computation_deduplication_constraint'),
        'computation',
        ['params', 'aoi_geom', 'plugin_id', 'plugin_version'],
        schema='ca-base',
    )
    op.alter_column(
        'computation', 'params', existing_type=postgresql.JSONB(astext_type=sa.Text()), nullable=False, schema='ca-base'
    )
    op.drop_column('computation', 'requested_params', schema='ca-base')
    op.drop_column('computation', 'valid_until', schema='ca-base')
    op.drop_column('computation', 'cache_epoch', schema='ca-base')
