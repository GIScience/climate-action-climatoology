"""add_celery_tables

Revision ID: 0c77b1f5b970
Revises:
Create Date: 2025-09-16 22:01:05.415730

"""

from datetime import datetime, timezone
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import PickleType
from sqlalchemy.schema import CreateSequence, DropSequence

# revision identifiers, used by Alembic.
revision: str = '0c77b1f5b970'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    task_id_sequence = sa.Sequence('task_id_sequence')
    op.create_table(
        'celery_taskmeta',
        sa.Column('id', sa.Integer(), task_id_sequence, autoincrement=True, nullable=False, primary_key=True),
        sa.Column('task_id', sa.String(155), nullable=True, unique=True),
        sa.Column('status', sa.String(50), default='PENDING', nullable=True),
        sa.Column('result', PickleType, nullable=True),
        sa.Column(
            'date_done',
            sa.DateTime(),
            default=datetime.now(timezone.utc),
            onupdate=datetime.now(timezone.utc),
            nullable=True,
        ),
        sa.Column('traceback', sa.Text(), nullable=True),
        sa.Column('name', sa.String(155), nullable=True),
        sa.Column('args', sa.LargeBinary(), nullable=True),
        sa.Column('kwargs', sa.LargeBinary(), nullable=True),
        sa.Column('worker', sa.String(155), nullable=True),
        sa.Column('retries', sa.Integer(), nullable=True),
        sa.Column('queue', sa.String(155), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('task_id'),
        schema='public',
    )
    op.execute(CreateSequence(task_id_sequence))
    taskset_id_sequence = sa.Sequence('taskset_id_sequence')
    op.create_table(
        'celery_tasksetmeta',
        sa.Column('id', sa.Integer(), taskset_id_sequence, autoincrement=True, nullable=False, primary_key=True),
        sa.Column('taskset_id', sa.String(155), unique=True, nullable=True),
        sa.Column('result', PickleType, nullable=True),
        sa.Column('date_done', sa.DateTime(), nullable=True, default=datetime.now(timezone.utc)),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('taskset_id'),
        schema='public',
    )
    op.execute(CreateSequence(taskset_id_sequence))


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(DropSequence(sa.Sequence('task_id_sequence')))
    op.drop_table('celery_tasksetmeta', schema='public')
    op.execute(DropSequence(sa.Sequence('taskset_id_sequence')))
    op.drop_table('celery_taskmeta', schema='public')
