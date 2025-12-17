"""create_demo_flag

Revision ID: 3128973eabf4
Revises: afbe6fd67545
Create Date: 2025-12-16 13:46:32.296690

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from alembic_utils.pg_view import PGView

# revision identifiers, used by Alembic.
revision: str = '3128973eabf4'
down_revision: Union[str, None] = 'afbe6fd67545'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('computation_lookup', sa.Column('is_demo', sa.Boolean(), nullable=True), schema='ca_base')
    op.execute(sa.text("update ca_base.computation_lookup set is_demo = aoi_id like 'demo-%'"))
    op.alter_column('computation_lookup', 'is_demo', existing_type=sa.BOOLEAN(), nullable=False, schema='ca_base')
    op.drop_index(op.f('ix_ca_base_computation_lookup_aoi_id'), table_name='computation_lookup', schema='ca_base')
    op.create_index(
        op.f('ix_ca_base_computation_lookup_is_demo'), 'computation_lookup', ['is_demo'], unique=False, schema='ca_base'
    )
    ca_base_usage_summary = PGView(
        schema='ca_base',
        signature='usage_summary',
        definition="""
SELECT
    ca_base.plugin_info.id AS plugin_id,
    count(*) AS no_of_requested_computations,
    CAST(round(count(*) / CAST(((CAST(now() AS DATE) - CAST(min(ca_base.computation_lookup.request_ts) AS DATE)) + 1) AS NUMERIC), 2) AS FLOAT) AS avg_computations_per_day,
    CAST(min(ca_base.computation_lookup.request_ts) AS DATE) AS since
FROM
    ca_base.plugin_info
JOIN ca_base.computation ON
    ca_base.computation.plugin_key = ca_base.plugin_info.key
JOIN ca_base.computation_lookup ON
    ca_base.computation.correlation_uuid = ca_base.computation_lookup.computation_id
WHERE
    NOT ca_base.computation_lookup.is_demo
GROUP BY
    ca_base.plugin_info.id
ORDER BY
    count(*) DESC,
    ca_base.plugin_info.id
                       """,
    )
    op.replace_entity(ca_base_usage_summary)


def downgrade() -> None:
    """Downgrade schema."""
    ca_base_usage_summary = PGView(
        schema='ca_base',
        signature='usage_summary',
        definition="""
                   SELECT ca_base.plugin_info.id                                   as plugin_id,
                          count(*)                                                 AS no_of_requested_computations,
                          CAST(round(count(*) / CAST((
                              (CAST(now() AS DATE) - CAST(min(ca_base.computation_lookup.request_ts) AS DATE)) +
                              1) AS NUMERIC), 2) AS FLOAT)                         AS avg_computations_per_day,
                          CAST(min(ca_base.computation_lookup.request_ts) AS DATE) AS since
                   FROM ca_base.plugin_info
                            JOIN ca_base.computation ON
                       ca_base.computation.plugin_key = ca_base.plugin_info.key
                            JOIN ca_base.computation_lookup ON
                       ca_base.computation.correlation_uuid = ca_base.computation_lookup.computation_id
                   WHERE (
                             ca_base.computation_lookup.aoi_id NOT LIKE 'demo-' || '%%'
                             )
                   GROUP BY ca_base.plugin_info.id
                   ORDER BY count(*) DESC,
                            ca_base.plugin_info.id
                   """,
    )
    op.replace_entity(ca_base_usage_summary)

    op.drop_index(op.f('ix_ca_base_computation_lookup_is_demo'), 'computation_lookup', schema='ca_base')
    op.create_index(
        op.f('ix_ca_base_computation_lookup_aoi_id'),
        table_name='computation_lookup',
        columns=['aoi_id'],
        unique=False,
        schema='ca_base',
    )

    op.drop_column('computation_lookup', 'is_demo', schema='ca_base')
