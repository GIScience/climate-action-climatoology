"""computation-lookup-timestamps

Revision ID: 540bd0182fc3
Revises: 0364d1c8dd4e
Create Date: 2025-11-12 08:49:11.533401

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '540bd0182fc3'
down_revision: Union[str, None] = '0364d1c8dd4e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_column('computation', 'timestamp', schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column('computation', sa.Column('timestamp', sa.DateTime(), nullable=True), schema='ca_base')
    op.execute(
        sa.text(
            'update ca_base.computation c set timestamp=ct.date_done from public.celery_taskmeta ct where c.correlation_uuid::text = ct.task_id'
        )
    )
    # revoked computations will have NULL for the celery `date_done`
    op.execute(
        sa.text(
            'update ca_base.computation c set timestamp=cl.request_ts from ca_base.computation_lookup cl where cl.computation_id = c.correlation_uuid and c.timestamp is null'
        )
    )
    op.alter_column('computation', 'timestamp', nullable=False, schema='ca_base')
