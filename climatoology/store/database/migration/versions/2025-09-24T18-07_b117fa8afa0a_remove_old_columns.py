"""remove_old_columns

Revision ID: b117fa8afa0a
Revises: 403c9000148c
Create Date: 2025-09-24 18:07:58.966998

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b117fa8afa0a'
down_revision: Union[str, None] = '403c9000148c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_column('computation', 'aoi_id', schema='ca_base')
    op.drop_column('computation', 'status', schema='ca_base')
    op.drop_column('computation', 'aoi_name', schema='ca_base')
    op.add_column(
        'computation_lookup', sa.Column('aoi_properties', postgresql.JSONB(astext_type=sa.Text())), schema='ca_base'
    )

    op.execute(sa.text("update ca_base.info set teaser='This plugin does not provide a teaser.' where teaser is null"))
    op.alter_column('info', 'teaser', existing_type=sa.VARCHAR(), nullable=False, schema='ca_base')
    op.execute(sa.text("update ca_base.info set demo_config='{}' where demo_config is null"))
    op.alter_column(
        'info', 'demo_config', existing_type=postgresql.JSON(astext_type=sa.Text()), nullable=False, schema='ca_base'
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column(
        'computation',
        sa.Column(
            'aoi_name',
            sa.VARCHAR(),
            server_default=sa.text("'dummy'::character varying"),
            autoincrement=False,
            nullable=False,
        ),
        schema='ca_base',
    )
    op.add_column(
        'computation',
        sa.Column(
            'status',
            postgresql.ENUM('PENDING', 'STARTED', 'SUCCESS', 'FAILURE', 'RETRY', 'REVOKED', name='computationstate'),
            autoincrement=False,
            nullable=True,
        ),
        schema='ca_base',
    )
    op.add_column(
        'computation',
        sa.Column(
            'aoi_id',
            sa.VARCHAR(),
            server_default=sa.text("'dummy'::character varying"),
            autoincrement=False,
            nullable=False,
        ),
        schema='ca_base',
    )
    op.drop_column('computation_lookup', 'aoi_properties', schema='ca_base')

    op.alter_column(
        'info', 'demo_config', existing_type=postgresql.JSON(astext_type=sa.Text()), nullable=True, schema='ca_base'
    )
    op.alter_column('info', 'teaser', existing_type=sa.VARCHAR(), nullable=True, schema='ca_base')
