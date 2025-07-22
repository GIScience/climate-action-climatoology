"""move aoi params to lookup

Revision ID: b19b6b5c427f
Revises: 403bc7fe5d0c
Create Date: 2025-06-04 12:32:01.103610

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'b19b6b5c427f'
down_revision: Union[str, None] = '403bc7fe5d0c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('computation_lookup', sa.Column('aoi_name', sa.String(), nullable=True), schema='ca_base')
    op.add_column('computation_lookup', sa.Column('aoi_id', sa.String(), nullable=True), schema='ca_base')
    op.execute(
        sa.text(
            'update ca_base.computation_lookup cl set aoi_name=c.aoi_name, aoi_id=c.aoi_id from ca_base.computation c where c.correlation_uuid=cl.computation_id'
        )
    )
    op.alter_column('computation_lookup', 'aoi_name', nullable=False, schema='ca_base')
    op.alter_column('computation_lookup', 'aoi_id', nullable=False, schema='ca_base')
    op.alter_column('computation', 'aoi_name', server_default='dummy', schema='ca_base')
    op.alter_column('computation', 'aoi_id', server_default='dummy', schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(
        sa.text(
            'update ca_base.computation c set aoi_name=cl.aoi_name, aoi_id=cl.aoi_id from ca_base.computation_lookup cl where c.correlation_uuid=cl.computation_id;'
        )
    )
    op.drop_column('computation_lookup', 'aoi_id', schema='ca_base')
    op.drop_column('computation_lookup', 'aoi_name', schema='ca_base')
    op.alter_column('computation', 'aoi_name', server_default=None, schema='ca_base')
    op.alter_column('computation', 'aoi_id', server_default=None, schema='ca_base')
