"""add_artifact_sources

Revision ID: 6c56a2ce5490
Revises: 018584ddd53c
Create Date: 2025-09-26 15:03:54.531664

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '6c56a2ce5490'
down_revision: Union[str, None] = '018584ddd53c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('artifact', sa.Column('sources', sa.JSON(), nullable=True), schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('artifact', 'sources', schema='ca_base')
