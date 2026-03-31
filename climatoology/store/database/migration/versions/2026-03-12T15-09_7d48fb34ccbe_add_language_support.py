"""add_language_support

Revision ID: 7d48fb34ccbe
Revises: f4d8bfa97356
Create Date: 2026-03-12 15:09:48.813552

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from alembic_utils.pg_view import PGView

# revision identifiers, used by Alembic.
revision: str = '7d48fb34ccbe'
down_revision: Union[str, None] = 'f4d8bfa97356'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'plugin_info', sa.Column('language', sa.String(length=2), nullable=True, server_default='en'), schema='ca_base'
    )
    op.execute(sa.text("update ca_base.plugin_info set language='en'"))
    op.alter_column('plugin_info', 'language', nullable=False, schema='ca_base')

    op.alter_column('plugin_info', 'key', new_column_name='old_key', schema='ca_base', primary_key=False)
    op.add_column(
        'plugin_info',
        sa.Column(
            'key',
            sa.String(),
            sa.Computed("id::text || '-'::text || version::text || '-'::text || language::text"),
            nullable=False,
            primary_key=True,
        ),
        schema='ca_base',
    )

    op.drop_constraint(op.f('computation_plugin_key_fkey'), 'computation', schema='ca_base', type_='foreignkey')
    op.drop_constraint(
        op.f('author_info_link_table_info_key_fkey'), 'plugin_info_author_link', schema='ca_base', type_='foreignkey'
    )
    op.drop_constraint('info_pkey', 'plugin_info', schema='ca_base', type_='primary')
    op.create_primary_key('info_pkey', 'plugin_info', ['key'], schema='ca_base')

    op.execute(sa.text("update ca_base.computation set plugin_key=replace(plugin_key,';','-')||'-en';"))
    op.create_foreign_key(
        None, 'computation', 'plugin_info', ['plugin_key'], ['key'], source_schema='ca_base', referent_schema='ca_base'
    )
    op.execute(sa.text("update ca_base.plugin_info_author_link set info_key=replace(info_key,';','-')||'-en';"))
    op.create_foreign_key(
        None,
        'plugin_info_author_link',
        'plugin_info',
        ['info_key'],
        ['key'],
        source_schema='ca_base',
        referent_schema='ca_base',
    )

    ca_base_valid_computations = PGView(
        schema='ca_base',
        signature='valid_computations',
        definition="SELECT ca_base.computation.correlation_uuid, ca_base.plugin_info.name AS plugin_name, ca_base.computation.aoi_geom AS aoi, ca_base.computation.params \nFROM ca_base.computation JOIN ca_base.plugin_info ON ca_base.computation.plugin_key = ca_base.plugin_info.key JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id \nWHERE ca_base.plugin_info.latest AND ca_base.computation.valid_until > now() AND celery_taskmeta.status = 'SUCCESS'",
    )
    op.replace_entity(ca_base_valid_computations)
    ca_base_computations_summary = PGView(
        schema='ca_base',
        signature='computations_summary',
        definition="SELECT ca_base.plugin_info.id AS plugin_id, ca_base.plugin_info.version AS plugin_version, count(*) AS no_of_computations, count(*) FILTER (WHERE celery_taskmeta.status = 'SUCCESS') AS no_of_successes, count(*) FILTER (WHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%')) AS no_of_failures, CAST(round(((count(*) FILTER (WHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%'))) / CAST((count(*) FILTER (WHERE celery_taskmeta.status = 'SUCCESS' OR celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%'))) AS NUMERIC)) * 100.0, 2) AS FLOAT) AS percent_failed, min(celery_taskmeta.date_done) AS since, count(*) FILTER (WHERE celery_taskmeta.traceback LIKE '%%' || 'InputValidationError' || '%%') AS no_of_input_validation_fails, count(*) FILTER (WHERE (celery_taskmeta.status NOT IN ('SUCCESS', 'FAILURE'))) AS no_of_other_states \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key LEFT OUTER JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id GROUP BY ca_base.plugin_info.id, ca_base.plugin_info.version ORDER BY ca_base.plugin_info.id, ca_base.plugin_info.version DESC",
    )
    op.replace_entity(ca_base_computations_summary)
    ca_base_usage_summary = PGView(
        schema='ca_base',
        signature='usage_summary',
        definition='SELECT ca_base.plugin_info.id AS plugin_id, count(*) AS no_of_requested_computations, CAST(round(count(*) / CAST(((CAST(now() AS DATE) - CAST(min(ca_base.computation_lookup.request_ts) AS DATE)) + 1) AS NUMERIC), 2) AS FLOAT) AS avg_computations_per_day, CAST(min(ca_base.computation_lookup.request_ts) AS DATE) AS since \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key JOIN ca_base.computation_lookup ON ca_base.computation.correlation_uuid = ca_base.computation_lookup.computation_id \nWHERE NOT ca_base.computation_lookup.is_demo GROUP BY ca_base.plugin_info.id ORDER BY count(*) DESC, ca_base.plugin_info.id',
    )
    op.replace_entity(ca_base_usage_summary)
    ca_base_failed_computations = PGView(
        schema='ca_base',
        signature='failed_computations',
        definition="SELECT ca_base.plugin_info.id AS plugin_id, count(*) AS no_of_failures_in_last_30_days, left(coalesce(ca_base.computation.message, celery_taskmeta.traceback), 10) AS cause, array_agg(DISTINCT CAST(celery_taskmeta.date_done AS DATE)) AS on_days, array_agg(DISTINCT ca_base.plugin_info.version) AS in_versions, array_agg(DISTINCT ca_base.computation.message) AS with_messages, array_agg(DISTINCT celery_taskmeta.traceback) AS with_tracebacks \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key LEFT OUTER JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id \nWHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%') AND celery_taskmeta.date_done > now() - make_interval(secs=>2592000.0) GROUP BY ca_base.plugin_info.id, left(coalesce(ca_base.computation.message, celery_taskmeta.traceback), 10) ORDER BY ca_base.plugin_info.id, count(*) DESC",
    )
    op.replace_entity(ca_base_failed_computations)
    ca_base_artifact_errors = PGView(
        schema='ca_base',
        signature='artifact_errors',
        definition='SELECT ca_base.plugin_info.id AS plugin_id, artifact_errors.key AS artifact, count(*) AS no_of_computations_with_errors_in_last_30_days, array_agg(DISTINCT CAST(celery_taskmeta.date_done AS DATE)) AS on_days, array_agg(DISTINCT ca_base.plugin_info.version) AS in_versions, array_agg(DISTINCT artifact_errors.value) AS with_messages \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key LEFT OUTER JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id, json_each_text(ca_base.computation.artifact_errors) AS artifact_errors \nWHERE celery_taskmeta.date_done > now() - make_interval(secs=>2592000.0) GROUP BY ca_base.plugin_info.id, artifact_errors.key ORDER BY ca_base.plugin_info.id, artifact_errors.key, count(*) DESC',
    )
    op.replace_entity(ca_base_artifact_errors)

    op.drop_column('plugin_info', 'old_key', schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column('plugin_info', 'key', new_column_name='new_key', schema='ca_base', primary_key=False)
    op.add_column(
        'plugin_info',
        sa.Column(
            'key',
            sa.String(),
            sa.Computed("id::text || ';'::text || version::text"),
            nullable=False,
            primary_key=True,
        ),
        schema='ca_base',
    )

    ca_base_valid_computations = PGView(
        schema='ca_base',
        signature='valid_computations',
        definition="SELECT ca_base.computation.correlation_uuid, ca_base.plugin_info.name AS plugin_name, ca_base.computation.aoi_geom AS aoi, ca_base.computation.params \nFROM ca_base.computation JOIN ca_base.plugin_info ON ca_base.computation.plugin_key = ca_base.plugin_info.key JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id \nWHERE ca_base.plugin_info.latest AND ca_base.computation.valid_until > now() AND celery_taskmeta.status = 'SUCCESS'",
    )
    op.replace_entity(ca_base_valid_computations)
    ca_base_computations_summary = PGView(
        schema='ca_base',
        signature='computations_summary',
        definition="SELECT ca_base.plugin_info.id AS plugin_id, ca_base.plugin_info.version AS plugin_version, count(*) AS no_of_computations, count(*) FILTER (WHERE celery_taskmeta.status = 'SUCCESS') AS no_of_successes, count(*) FILTER (WHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%')) AS no_of_failures, CAST(round(((count(*) FILTER (WHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%'))) / CAST((count(*) FILTER (WHERE celery_taskmeta.status = 'SUCCESS' OR celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%'))) AS NUMERIC)) * 100.0, 2) AS FLOAT) AS percent_failed, min(celery_taskmeta.date_done) AS since, count(*) FILTER (WHERE celery_taskmeta.traceback LIKE '%%' || 'InputValidationError' || '%%') AS no_of_input_validation_fails, count(*) FILTER (WHERE (celery_taskmeta.status NOT IN ('SUCCESS', 'FAILURE'))) AS no_of_other_states \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key LEFT OUTER JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id GROUP BY ca_base.plugin_info.id, ca_base.plugin_info.version ORDER BY ca_base.plugin_info.id, ca_base.plugin_info.version DESC",
    )
    op.replace_entity(ca_base_computations_summary)
    ca_base_usage_summary = PGView(
        schema='ca_base',
        signature='usage_summary',
        definition='SELECT ca_base.plugin_info.id AS plugin_id, count(*) AS no_of_requested_computations, CAST(round(count(*) / CAST(((CAST(now() AS DATE) - CAST(min(ca_base.computation_lookup.request_ts) AS DATE)) + 1) AS NUMERIC), 2) AS FLOAT) AS avg_computations_per_day, CAST(min(ca_base.computation_lookup.request_ts) AS DATE) AS since \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key JOIN ca_base.computation_lookup ON ca_base.computation.correlation_uuid = ca_base.computation_lookup.computation_id \nWHERE NOT ca_base.computation_lookup.is_demo GROUP BY ca_base.plugin_info.id ORDER BY count(*) DESC, ca_base.plugin_info.id',
    )
    op.replace_entity(ca_base_usage_summary)
    ca_base_failed_computations = PGView(
        schema='ca_base',
        signature='failed_computations',
        definition="SELECT ca_base.plugin_info.id AS plugin_id, count(*) AS no_of_failures_in_last_30_days, left(coalesce(ca_base.computation.message, celery_taskmeta.traceback), 10) AS cause, array_agg(DISTINCT CAST(celery_taskmeta.date_done AS DATE)) AS on_days, array_agg(DISTINCT ca_base.plugin_info.version) AS in_versions, array_agg(DISTINCT ca_base.computation.message) AS with_messages, array_agg(DISTINCT celery_taskmeta.traceback) AS with_tracebacks \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key LEFT OUTER JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id \nWHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%') AND celery_taskmeta.date_done > now() - make_interval(secs=>2592000.0) GROUP BY ca_base.plugin_info.id, left(coalesce(ca_base.computation.message, celery_taskmeta.traceback), 10) ORDER BY ca_base.plugin_info.id, count(*) DESC",
    )
    op.replace_entity(ca_base_failed_computations)
    ca_base_artifact_errors = PGView(
        schema='ca_base',
        signature='artifact_errors',
        definition='SELECT ca_base.plugin_info.id AS plugin_id, artifact_errors.key AS artifact, count(*) AS no_of_computations_with_errors_in_last_30_days, array_agg(DISTINCT CAST(celery_taskmeta.date_done AS DATE)) AS on_days, array_agg(DISTINCT ca_base.plugin_info.version) AS in_versions, array_agg(DISTINCT artifact_errors.value) AS with_messages \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key LEFT OUTER JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id, json_each_text(ca_base.computation.artifact_errors) AS artifact_errors \nWHERE celery_taskmeta.date_done > now() - make_interval(secs=>2592000.0) GROUP BY ca_base.plugin_info.id, artifact_errors.key ORDER BY ca_base.plugin_info.id, artifact_errors.key, count(*) DESC',
    )
    op.replace_entity(ca_base_artifact_errors)

    op.drop_constraint(op.f('computation_plugin_key_fkey'), 'computation', schema='ca_base', type_='foreignkey')
    op.drop_constraint(
        op.f('plugin_info_author_link_info_key_fkey'), 'plugin_info_author_link', schema='ca_base', type_='foreignkey'
    )
    op.drop_constraint('info_pkey', 'plugin_info', schema='ca_base', type_='primary')

    op.execute(
        sa.text('update ca_base.computation set plugin_key=key from ca_base.plugin_info where plugin_key=new_key')
    )
    op.execute(
        sa.text(
            'update ca_base.plugin_info_author_link set info_key=key from ca_base.plugin_info where info_key=new_key'
        )
    )
    op.execute(
        sa.text(
            "DELETE FROM ca_base.plugin_info_author_link pial USING ca_base.plugin_info pi WHERE pi.new_key=pial.info_key AND NOT pi.language = 'en'"
        )
    )
    op.execute(sa.text("DELETE FROM ca_base.plugin_info WHERE NOT ca_base.plugin_info.language = 'en';"))

    op.create_primary_key('info_pkey', 'plugin_info', ['key'], schema='ca_base')

    op.create_foreign_key(
        'computation_plugin_key_fkey',
        'computation',
        'plugin_info',
        ['plugin_key'],
        ['key'],
        source_schema='ca_base',
        referent_schema='ca_base',
    )

    op.create_foreign_key(
        'author_info_link_table_info_key_fkey',
        'plugin_info_author_link',
        'plugin_info',
        ['info_key'],
        ['key'],
        source_schema='ca_base',
        referent_schema='ca_base',
    )

    op.drop_column('plugin_info', 'new_key', schema='ca_base')
    op.drop_column('plugin_info', 'language', schema='ca_base')
