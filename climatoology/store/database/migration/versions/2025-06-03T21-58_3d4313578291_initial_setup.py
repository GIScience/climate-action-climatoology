"""initial_setup

Revision ID: 3d4313578291
Revises:
Create Date: 2025-06-03 21:58:23.527934

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '3d4313578291'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'info',
        sa.Column('plugin_id', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('version', sa.String(), nullable=False),
        sa.Column(
            'concerns',
            postgresql.ARRAY(
                sa.Enum(
                    'CLIMATE_ACTION__GHG_EMISSION',
                    'CLIMATE_ACTION__MITIGATION',
                    'CLIMATE_ACTION__ADAPTION',
                    'MOBILITY_PEDESTRIAN',
                    'MOBILITY_CYCLING',
                    'SUSTAINABILITY__WASTE',
                    name='concern',
                )
            ),
            nullable=False,
        ),
        sa.Column('purpose', sa.String(), nullable=False),
        sa.Column('methodology', sa.String(), nullable=False),
        sa.Column('sources', sa.JSON(), nullable=True),
        sa.Column('demo_config', sa.JSON(), nullable=True),
        sa.Column('assets', sa.JSON(), nullable=False),
        sa.Column('operator_schema', sa.JSON(), nullable=False),
        sa.Column('library_version', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('plugin_id'),
    )
    op.create_table(
        'pluginauthor',
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('affiliation', sa.String(), nullable=True),
        sa.Column('website', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('name'),
    )
    op.create_table(
        'author_info_link_table',
        sa.Column('info_id', sa.String(), nullable=True),
        sa.Column('author_id', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ['author_id'],
            ['pluginauthor.name'],
        ),
        sa.ForeignKeyConstraint(
            ['info_id'],
            ['info.plugin_id'],
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('author_info_link_table')
    op.drop_table('pluginauthor')
    op.drop_table('info')
    sa.Enum(name='concern').drop(op.get_bind())
