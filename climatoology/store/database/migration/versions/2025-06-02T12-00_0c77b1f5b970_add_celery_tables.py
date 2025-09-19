"""add_celery_tables

Revision ID: 0c77b1f5b970
Revises:
Create Date: 2025-09-16 22:01:05.415730

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '0c77b1f5b970'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'celery_taskmeta',
        sa.Column('id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('task_id', sa.String(), nullable=True),
        sa.Column('status', sa.String(), nullable=True),
        sa.Column('result', sa.LargeBinary(), nullable=True),
        sa.Column('date_done', sa.DateTime(), nullable=True),
        sa.Column('traceback', sa.Text(), nullable=True),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('args', sa.LargeBinary(), nullable=True),
        sa.Column('kwargs', sa.LargeBinary(), nullable=True),
        sa.Column('worker', sa.String(), nullable=True),
        sa.Column('retries', sa.Integer(), nullable=True),
        sa.Column('queue', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('task_id'),
        schema='public',
    )
    op.create_table(
        'celery_tasksetmeta',
        sa.Column('id', sa.Integer(), autoincrement=False, nullable=False),
        sa.Column('taskset_id', sa.String(), nullable=True),
        sa.Column('result', sa.LargeBinary(), nullable=True),
        sa.Column('date_done', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('taskset_id'),
        schema='public',
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('celery_tasksetmeta', schema='public')
    op.drop_table('celery_taskmeta', schema='public')
