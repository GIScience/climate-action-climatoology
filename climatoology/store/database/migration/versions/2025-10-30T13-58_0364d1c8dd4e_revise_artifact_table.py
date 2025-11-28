"""revise-artifact-table

Revision ID: 0364d1c8dd4e
Revises: 6c56a2ce5490
Create Date: 2025-10-30 13:58:27.616746

Note: this revision previously revised the `add views` change, but due to major rehaul of the db structure, that
revision was moved to be later.

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0364d1c8dd4e'
down_revision: Union[str, None] = '6c56a2ce5490'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(sa.text('alter table ca_base.artifact rename column store_id to filename;'))
    op.drop_column('artifact', 'file_path', schema='ca_base')
    op.execute(sa.text('update ca_base.artifact set tags=ARRAY[]::text[] where tags is NULL'))
    op.alter_column('artifact', 'tags', existing_type=postgresql.ARRAY(sa.VARCHAR()), nullable=False, schema='ca_base')
    op.execute(sa.text('update ca_base.artifact set rank=-1 where rank is NULL'))
    op.alter_column('artifact', 'rank', existing_type=sa.Integer(), nullable=False, schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column('artifact', 'rank', existing_type=sa.Integer(), nullable=True, schema='ca_base')
    op.alter_column('artifact', 'tags', existing_type=postgresql.ARRAY(sa.VARCHAR()), nullable=True, schema='ca_base')
    op.add_column(
        'artifact', sa.Column('file_path', sa.String(), nullable=False, default='/tmp/dummy_filename'), schema='ca_base'
    )
    op.execute(sa.text('alter table ca_base.artifact rename column filename to store_id;'))
