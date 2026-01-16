"""revert from jsonb to json

Revision ID: eec8b7cd6825
Revises: 3128973eabf4
Create Date: 2026-01-14 14:10:26.854272

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from alembic_utils.pg_view import PGView
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'eec8b7cd6825'
down_revision: Union[str, None] = '3128973eabf4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Drop views and indices that depend on the columns being altered
    ca_base_valid_computations = PGView(schema='ca_base', signature='valid_computations', definition='')
    op.drop_entity(ca_base_valid_computations)

    ca_base_usage_summary = PGView(schema='ca_base', signature='artifact_errors', definition='')
    op.drop_entity(ca_base_usage_summary)

    op.drop_constraint('computation_deduplication_constraint', 'computation', schema='ca_base')
    op.drop_column('computation', 'deduplication_key', schema='ca_base')

    # Alter columns from JSONB to JSON
    op.alter_column(
        'plugin_info',
        'operator_schema',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'assets',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'demo_config',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'sources',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'computation',
        'artifact_errors',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'artifact',
        'attachments',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'artifact',
        'sources',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'computation',
        'params',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=sa.JSON(),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'computation',
        'requested_params',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=sa.JSON(),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'computation_lookup',
        'aoi_properties',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=sa.JSON(),
        existing_nullable=True,
        schema='ca_base',
    )

    # Reinstate views and indices that depend on the columns being altered
    op.add_column(
        'computation',
        sa.Column(
            'deduplication_key',
            sa.Uuid(),
            sa.Computed('md5(requested_params::text||st_astext(aoi_geom))::uuid'),
            nullable=False,
        ),
        schema='ca_base',
    )
    op.create_unique_constraint(
        'computation_deduplication_constraint',
        'computation',
        ['plugin_key', 'deduplication_key', 'cache_epoch'],
        schema='ca_base',
    )

    ca_base_valid_computations = PGView(
        schema='ca_base',
        signature='valid_computations',
        definition="""
SELECT
    ca_base.computation.correlation_uuid,
    ca_base.plugin_info.name AS plugin_name,
    ca_base.computation.aoi_geom AS aoi,
    ca_base.computation.params
FROM
    ca_base.computation
JOIN ca_base.plugin_info ON
    ca_base.computation.plugin_key = ca_base.plugin_info.key
JOIN public.celery_taskmeta ON
    ca_base.computation.correlation_uuid = public.celery_taskmeta.task_id
WHERE
    ca_base.plugin_info.latest
    AND ca_base.computation.valid_until > now()
    AND public.celery_taskmeta.status = 'SUCCESS'
""",
    )
    op.create_entity(ca_base_valid_computations)

    ca_base_artifact_errors = PGView(
        schema='ca_base',
        signature='artifact_errors',
        definition="""
SELECT
	ca_base.plugin_info.id AS plugin_id,
	artifact_errors.key AS artifact,
	count(*) AS no_of_computations_with_errors_in_last_30_days,
	array_agg(DISTINCT CAST(celery_taskmeta.date_done AS DATE)) AS on_days,
	array_agg(DISTINCT ca_base.plugin_info.version) AS in_versions,
	array_agg(DISTINCT artifact_errors.value) AS with_messages
FROM
	ca_base.plugin_info
JOIN ca_base.computation ON
	ca_base.computation.plugin_key = ca_base.plugin_info.key
LEFT OUTER JOIN celery_taskmeta ON
	ca_base.computation.correlation_uuid = celery_taskmeta.task_id,
	json_each_text(ca_base.computation.artifact_errors) AS artifact_errors
WHERE
	celery_taskmeta.date_done > now() - make_interval(secs =>2592000.0)
GROUP BY
	ca_base.plugin_info.id,
	artifact_errors.key
ORDER BY
	ca_base.plugin_info.id,
	artifact_errors.key,
	count(*) DESC
""",
    )
    op.create_entity(ca_base_artifact_errors)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop views and indices that depend on the columns being altered
    ca_base_valid_computations = PGView(schema='ca_base', signature='valid_computations', definition='')
    op.drop_entity(ca_base_valid_computations)

    ca_base_usage_summary = PGView(schema='ca_base', signature='artifact_errors', definition='')
    op.drop_entity(ca_base_usage_summary)

    op.drop_constraint('computation_deduplication_constraint', 'computation', schema='ca_base')
    op.drop_column('computation', 'deduplication_key', schema='ca_base')

    # Alter columns from JSON to JSONB
    op.alter_column(
        'plugin_info',
        'operator_schema',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'assets',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'demo_config',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'sources',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'computation',
        'artifact_errors',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'artifact',
        'attachments',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'artifact',
        'sources',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'computation',
        'params',
        existing_type=postgresql.JSON(),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'computation',
        'requested_params',
        existing_type=postgresql.JSON(),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'computation_lookup',
        'aoi_properties',
        existing_type=postgresql.JSON(),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )

    # Reinstate views and indices that depend on the columns being altered
    op.add_column(
        'computation',
        sa.Column(
            'deduplication_key',
            sa.Uuid(),
            sa.Computed('md5(requested_params::text||st_astext(aoi_geom))::uuid'),
            nullable=False,
        ),
        schema='ca_base',
    )
    op.create_unique_constraint(
        'computation_deduplication_constraint',
        'computation',
        ['plugin_key', 'deduplication_key', 'cache_epoch'],
        schema='ca_base',
    )

    ca_base_valid_computations = PGView(
        schema='ca_base',
        signature='valid_computations',
        definition="""
SELECT
    ca_base.computation.correlation_uuid,
    ca_base.plugin_info.name AS plugin_name,
    ca_base.computation.aoi_geom AS aoi,
    ca_base.computation.params
FROM
    ca_base.computation
JOIN ca_base.plugin_info ON
    ca_base.computation.plugin_key = ca_base.plugin_info.key
JOIN public.celery_taskmeta ON
    ca_base.computation.correlation_uuid = public.celery_taskmeta.task_id
WHERE
    ca_base.plugin_info.latest
    AND ca_base.computation.valid_until > now()
    AND public.celery_taskmeta.status = 'SUCCESS'
""",
    )
    op.create_entity(ca_base_valid_computations)

    ca_base_artifact_errors = PGView(
        schema='ca_base',
        signature='artifact_errors',
        definition="""
SELECT
    ca_base.plugin_info.id AS plugin_id,
    artifact_errors.key as artifact,
    count(*) AS no_of_computations_with_errors_in_last_30_days,
    array_agg(DISTINCT CAST(public.celery_taskmeta.date_done AS DATE)) AS on_days,
    array_agg(DISTINCT ca_base.plugin_info.version) AS in_versions,
    array_agg(DISTINCT artifact_errors.value) AS with_messages
FROM
    ca_base.plugin_info
JOIN ca_base.computation ON
    ca_base.computation.plugin_key = ca_base.plugin_info.key
LEFT OUTER JOIN public.celery_taskmeta ON
    ca_base.computation.correlation_uuid = public.celery_taskmeta.task_id,
    jsonb_each_text(ca_base.computation.artifact_errors) AS artifact_errors
WHERE
    public.celery_taskmeta.date_done > now() - make_interval(secs =>2592000.0)
GROUP BY
    ca_base.plugin_info.id,
    artifact_errors.key
ORDER BY
    ca_base.plugin_info.id,
    artifact_errors.key,
    count(*) DESC
""",
    )
    op.create_entity(ca_base_artifact_errors)
