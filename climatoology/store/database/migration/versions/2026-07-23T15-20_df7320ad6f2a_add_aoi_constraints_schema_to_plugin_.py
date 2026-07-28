"""add aoi constraints schema to plugin info

Revision ID: df7320ad6f2a
Revises: 8945b7d5fac8
Create Date: 2026-07-23 15:20:24.864563

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'df7320ad6f2a'
down_revision: Union[str, None] = '8945b7d5fac8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('plugin_info', sa.Column('aoi_constraints', sa.JSON(), nullable=True), schema='ca_base')
    op.execute(sa.text("update ca_base.plugin_info set aoi_constraints='[]'"))
    op.alter_column('plugin_info', 'aoi_constraints', nullable=False, schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('plugin_info', 'aoi_constraints', schema='ca_base')
