"""add teaser to info

Revision ID: f5dbc6d77574
Revises: 49cccfd144a8
Create Date: 2025-06-03 22:42:24.174192

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'f5dbc6d77574'
down_revision: Union[str, None] = '49cccfd144a8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('info', sa.Column('teaser', sa.String(), nullable=True), schema='ca-base')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('info', 'teaser', schema='ca-base')
