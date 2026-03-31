"""empty message

Revision ID: f398f777a563
Revises: bf7b34435593
Create Date: 2026-03-31 20:29:26.104332

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'f398f777a563'
down_revision: Union[str, None] = 'bf7b34435593'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(op.f('ix_celery_taskmeta_date_done'), 'celery_taskmeta', ['date_done'])
    op.create_index(op.f('ix_celery_tasksetmeta_date_done'), 'celery_tasksetmeta', ['date_done'])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_celery_tasksetmeta_date_done'), table_name='celery_tasksetmeta')
    op.drop_index(op.f('ix_celery_taskmeta_date_done'), table_name='celery_taskmeta')
