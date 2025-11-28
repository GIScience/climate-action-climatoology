"""add plugin state info

Revision ID: 3ac1827b309d
Revises: 8b52ceba3457
Create Date: 2025-06-03 23:11:14.780605

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '3ac1827b309d'
down_revision: Union[str, None] = '8b52ceba3457'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    plugin_state = sa.Enum('EXPERIMENTAL', 'ACTIVE', 'HIBERNATE', 'ARCHIVE', name='pluginstate')
    plugin_state.create(op.get_bind())
    op.add_column(
        'info',
        sa.Column('state', plugin_state, nullable=True),
        schema='ca-base',
    )
    op.execute(sa.text('update "ca-base".info set state=\'ACTIVE\' where state is null'))
    op.alter_column('info', 'state', nullable=False, schema='ca-base')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('info', 'state', schema='ca-base')
    sa.Enum(name='pluginstate').drop(op.get_bind())
