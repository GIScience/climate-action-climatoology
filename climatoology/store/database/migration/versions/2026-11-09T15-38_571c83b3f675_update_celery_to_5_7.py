"""update celery to 5.7

Revision ID: 571c83b3f675
Revises: 73de3acb3ef9
Create Date: 2026-11-09 15:38:41.841436

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '571c83b3f675'
down_revision: Union[str, None] = '73de3acb3ef9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('celery_taskmeta', sa.Column('children', sa.LargeBinary(), nullable=True))
    op.add_column('celery_taskmeta', sa.Column('stamps', sa.LargeBinary(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('celery_taskmeta', 'stamps')
    op.drop_column('celery_taskmeta', 'children')
