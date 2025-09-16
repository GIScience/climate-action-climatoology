"""add computation metadata

Revision ID: 49cccfd144a8
Revises: 3d4313578291
Create Date: 2025-06-03 22:05:33.233408

"""

from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.ddl import CreateSchema, DropSchema

# revision identifiers, used by Alembic.
revision: str = '49cccfd144a8'
down_revision: Union[str, None] = '3d4313578291'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

new_schema = 'ca-base'


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(CreateSchema(new_schema, if_not_exists=True))
    op.execute(sa.text('alter table info set schema "ca-base"'))
    op.execute(sa.text('alter table pluginauthor set schema "ca-base"'))
    op.execute(sa.text('alter table author_info_link_table set schema "ca-base"'))

    op.create_table(
        'computation',
        sa.Column('correlation_uuid', sa.Uuid(), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('params', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            'aoi_geom',
            geoalchemy2.types.Geometry(
                geometry_type='MULTIPOLYGON', srid=4326, from_text='ST_GeomFromEWKT', name='geometry', nullable=False
            ),
            nullable=False,
        ),
        sa.Column('aoi_name', sa.String(), nullable=False),
        sa.Column('aoi_id', sa.String(), nullable=False),
        sa.Column('plugin_id', sa.String(), nullable=False),
        sa.Column('plugin_version', sa.String(), nullable=False),
        sa.Column(
            'status',
            sa.Enum('PENDING', 'STARTED', 'SUCCESS', 'FAILURE', 'RETRY', 'REVOKED', name='computationstate'),
            nullable=False,
        ),
        sa.Column('message', sa.String(), nullable=True),
        sa.Column('artifact_errors', sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ['plugin_id'],
            ['ca-base.info.plugin_id'],
        ),
        sa.PrimaryKeyConstraint('correlation_uuid'),
        schema='ca-base',
    )

    op.create_table(
        'artifact',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('correlation_uuid', sa.Uuid(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column(
            'modality',
            sa.Enum(
                'MARKDOWN',
                'TABLE',
                'IMAGE',
                'CHART',
                'CHART_PLOTLY',
                'MAP_LAYER_GEOJSON',
                'MAP_LAYER_GEOTIFF',
                'COMPUTATION_INFO',
                name='artifactmodality',
            ),
            nullable=False,
        ),
        sa.Column('primary', sa.Boolean(), nullable=False),
        sa.Column('tags', postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column('summary', sa.String(), nullable=False),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('attachments', sa.JSON(), nullable=True),
        sa.Column('store_id', sa.String(), nullable=False),
        sa.Column('file_path', sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ['correlation_uuid'],
            ['ca-base.computation.correlation_uuid'],
        ),
        sa.PrimaryKeyConstraint('id'),
        schema='ca-base',
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(sa.text('alter table "ca-base".info set schema "public"'))
    op.execute(sa.text('alter table "ca-base".pluginauthor set schema "public"'))
    op.execute(sa.text('alter table "ca-base".author_info_link_table set schema "public"'))
    op.drop_table('artifact', schema='ca-base')
    op.drop_table('computation', schema='ca-base')
    op.execute(DropSchema(new_schema))
    sa.Enum(name='computationstate').drop(op.get_bind())
    sa.Enum(name='artifactmodality').drop(op.get_bind())
