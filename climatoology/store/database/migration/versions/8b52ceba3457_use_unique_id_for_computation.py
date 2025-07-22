"""use unique id for computation

Revision ID: 8b52ceba3457
Revises: f5dbc6d77574
Create Date: 2025-06-03 23:08:29.171846

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '8b52ceba3457'
down_revision: Union[str, None] = 'f5dbc6d77574'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'computation_lookup',
        sa.Column('user_correlation_uuid', sa.Uuid(), nullable=False),
        sa.Column('request_ts', sa.DateTime(), nullable=False),
        sa.Column('computation_id', sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(
            ['computation_id'],
            ['ca-base.computation.correlation_uuid'],
        ),
        sa.PrimaryKeyConstraint('user_correlation_uuid'),
        schema='ca-base',
    )
    op.create_unique_constraint(
        'computation_deduplication_constraint',
        'computation',
        ['params', 'aoi_geom', 'plugin_id', 'plugin_version'],
        schema='ca-base',
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('computation_deduplication_constraint', 'computation', schema='ca-base', type_='unique')
    op.drop_table('computation_lookup', schema='ca-base')
