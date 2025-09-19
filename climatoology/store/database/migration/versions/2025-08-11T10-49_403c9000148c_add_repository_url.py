"""add_repository_url

Revision ID: 403c9000148c
Revises: 85fc74361055
Create Date: 2025-08-11 10:49:17.970951

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '403c9000148c'
down_revision: Union[str, None] = '85fc74361055'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('info', sa.Column('repository', sa.String(), nullable=True), schema='ca_base')
    op.execute(
        sa.text(
            "update ca_base.info set repository='https://gitlab.heigit.org/climate-action' where repository is null"
        )
    )
    op.alter_column('info', 'repository', nullable=False, schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('info', 'repository', schema='ca_base')
