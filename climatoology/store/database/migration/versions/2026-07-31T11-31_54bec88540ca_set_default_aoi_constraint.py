"""set-default-aoi-constraint

Revision ID: 54bec88540ca
Revises: df7320ad6f2a
Create Date: 2026-07-31 11:31:57.917094

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '54bec88540ca'
down_revision: Union[str, None] = 'df7320ad6f2a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column('plugin_info', 'aoi_constraints', nullable=True, schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column('plugin_info', 'aoi_constraints', nullable=False, schema='ca_base')
