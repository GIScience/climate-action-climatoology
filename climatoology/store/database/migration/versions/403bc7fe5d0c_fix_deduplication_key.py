"""fix deduplication key

Revision ID: 403bc7fe5d0c
Revises: 45b227b8bee7
Create Date: 2025-06-04 12:20:41.987564

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '403bc7fe5d0c'
down_revision: Union[str, None] = '45b227b8bee7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'computation',
        sa.Column(
            'deduplication_key',
            sa.Uuid(),
            sa.Computed(
                'md5(requested_params::text||st_astext(aoi_geom)||plugin_id||plugin_version)::uuid',
            ),
            nullable=False,
        ),
        schema='ca_base',
    )
    op.drop_constraint(op.f('computation_deduplication_constraint'), 'computation', schema='ca_base', type_='unique')
    op.create_unique_constraint(
        'computation_deduplication_constraint', 'computation', ['deduplication_key', 'cache_epoch'], schema='ca_base'
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('computation_deduplication_constraint', 'computation', schema='ca_base', type_='unique')
    op.create_unique_constraint(
        op.f('computation_deduplication_constraint'),
        'computation',
        ['cache_epoch', 'requested_params', 'aoi_geom', 'plugin_id', 'plugin_version'],
        schema='ca_base',
    )
    op.drop_column('computation', 'deduplication_key', schema='ca_base')
