"""add author and artifact rank

Revision ID: 85fc74361055
Revises: d77cdd5e2fb2
Create Date: 2025-07-25 16:36:58.396202

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '85fc74361055'
down_revision: Union[str, None] = 'd77cdd5e2fb2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'author_info_link_table',
        sa.Column('author_seat', sa.Integer(), nullable=True),
        schema='ca_base',
    )
    op.add_column('artifact', sa.Column('rank', sa.Integer(), nullable=True), schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('author_info_link_table', 'author_seat', schema='ca_base')
    op.drop_column('artifact', 'rank', schema='ca_base')
