"""add centroid

Revision ID: 44579dd466ad
Revises: 0d747bafb75c
Create Date: 2026-07-14 19:46:25.344171

"""

from typing import Sequence, Union

from alembic import op
from alembic_utils.pg_view import PGView

# revision identifiers, used by Alembic.
revision: str = '44579dd466ad'
down_revision: Union[str, None] = '0d747bafb75c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # op.add_column(
    #     'computation',
    #     sa.Column(
    #         'aoi_centroid',
    #         geoalchemy2.types.Geometry(
    #             geometry_type='point',
    #             srid=4326,
    #             dimension=2,
    #             from_text='ST_GeomFromEWKT',
    #             name='geometry',
    #             nullable=False,
    #         ),
    #         sa.Computed('st_pointonsurface(aoi_geom)', persisted=True),
    #         nullable=False,
    #     ),
    #     schema='ca_base',
    # )
    op.execute(
        'ALTER TABLE ca_base.computation ADD COLUMN aoi_centroid geometry(POINT,4326) GENERATED ALWAYS AS (st_pointonsurface(aoi_geom)) STORED NOT NULL;'
    )
    op.create_index(
        'idx_computation_aoi_centroid',
        'computation',
        ['aoi_centroid'],
        unique=False,
        schema='ca_base',
        postgresql_using='gist',
    )

    ca_base_valid_computations = PGView(
        schema='ca_base',
        signature='valid_computations',
        definition="SELECT ca_base.computation.correlation_uuid, ca_base.plugin_info.name AS plugin_name, ca_base.computation.aoi_geom AS aoi_geom, ca_base.computation.aoi_centroid AS aoi_centroid, ca_base.computation.params \nFROM ca_base.computation JOIN ca_base.plugin_info ON ca_base.computation.plugin_key = ca_base.plugin_info.key JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id \nWHERE ca_base.plugin_info.latest AND ca_base.computation.valid_until > now() AND celery_taskmeta.status = 'SUCCESS'",
    )
    op.replace_entity(ca_base_valid_computations)


def downgrade() -> None:
    """Downgrade schema."""
    ca_base_valid_computations = PGView(
        schema='ca_base',
        signature='valid_computations',
        definition="SELECT ca_base.computation.correlation_uuid, ca_base.plugin_info.name AS plugin_name, ca_base.computation.aoi_geom AS aoi, ca_base.computation.params \nFROM ca_base.computation JOIN ca_base.plugin_info ON ca_base.computation.plugin_key = ca_base.plugin_info.key JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id \nWHERE ca_base.plugin_info.latest AND ca_base.computation.valid_until > now() AND celery_taskmeta.status = 'SUCCESS'",
    )
    op.replace_entity(ca_base_valid_computations)
    op.drop_column(
        'computation',
        'aoi_centroid',
        schema='ca_base',
    )
