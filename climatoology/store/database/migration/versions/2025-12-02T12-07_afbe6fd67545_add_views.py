"""add_views

Revision ID: afbe6fd67545
Revises: 4e81ecfc7a7a
Create Date: 2025-09-27 23:07:27.435189

Note: this revision was previously earlier but due to major rehaul of the db structure was moved to be last.

"""

from typing import Sequence, Union

from alembic import op
from alembic_utils.pg_view import PGView

# revision identifiers, used by Alembic.
revision: str = 'afbe6fd67545'
down_revision: Union[str, None] = '4e81ecfc7a7a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
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

    ca_base_computations_summary = PGView(
        schema='ca_base',
        signature='computations_summary',
        definition="""
SELECT
    ca_base.plugin_info.id AS plugin_id,
    ca_base.plugin_info.version AS plugin_version,
    count(*) AS no_of_computations,
    count(*) FILTER (
    WHERE
        public.celery_taskmeta.status = 'SUCCESS'
    ) AS no_of_successes,
    count(*) FILTER (
    WHERE
        public.celery_taskmeta.status = 'FAILURE'
        AND (
            COALESCE(public.celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%'
        )
    ) AS no_of_failures,
    CAST(round(((count(*) FILTER (WHERE public.celery_taskmeta.status = 'FAILURE' AND (COALESCE(public.celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%'))) / CAST((count(*) FILTER (WHERE public.celery_taskmeta.status = 'SUCCESS' OR public.celery_taskmeta.status = 'FAILURE' AND (COALESCE(public.celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%'))) AS NUMERIC)) * 100.0, 2) AS FLOAT) AS percent_failed,
    min(public.celery_taskmeta.date_done) AS since,
    count(*) FILTER (
    WHERE
        public.celery_taskmeta.traceback LIKE '%%' || 'InputValidationError' || '%%'
    ) AS no_of_input_validation_fails,
    count(*) FILTER (
    WHERE
        (
            public.celery_taskmeta.status NOT IN (
                'SUCCESS', 'FAILURE'
            )
        )
    ) AS no_of_other_states
FROM
    ca_base.plugin_info
JOIN ca_base.computation ON
    ca_base.computation.plugin_key = ca_base.plugin_info.key
LEFT OUTER JOIN public.celery_taskmeta ON
    ca_base.computation.correlation_uuid = public.celery_taskmeta.task_id
GROUP BY
    ca_base.plugin_info.id,
    ca_base.plugin_info.version
ORDER BY
    ca_base.plugin_info.id,
    ca_base.plugin_info.version DESC
""",
    )
    op.create_entity(ca_base_computations_summary)

    ca_base_usage_summary = PGView(
        schema='ca_base',
        signature='usage_summary',
        definition="""
SELECT
    ca_base.plugin_info.id as plugin_id,
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
    (
        ca_base.computation_lookup.aoi_id NOT LIKE 'demo-' || '%%'
    )
GROUP BY
    ca_base.plugin_info.id
ORDER BY
    count(*) DESC,
    ca_base.plugin_info.id
""",
    )
    op.create_entity(ca_base_usage_summary)

    ca_base_failed_computations = PGView(
        schema='ca_base',
        signature='failed_computations',
        definition="""
SELECT
    ca_base.plugin_info.id AS plugin_id,
    count(*) AS no_of_failures_in_last_30_days,
    LEFT(COALESCE(ca_base.computation.message, public.celery_taskmeta.traceback), 10) AS cause,
    array_agg(DISTINCT CAST(public.celery_taskmeta.date_done AS DATE)) AS on_days,
    array_agg(DISTINCT ca_base.plugin_info.version) AS in_versions,
    array_agg(DISTINCT ca_base.computation.message) AS with_messages,
    array_agg(DISTINCT public.celery_taskmeta.traceback) AS with_tracebacks
FROM
    ca_base.plugin_info
JOIN ca_base.computation ON
    ca_base.computation.plugin_key = ca_base.plugin_info.key
LEFT OUTER JOIN public.celery_taskmeta ON
    ca_base.computation.correlation_uuid = public.celery_taskmeta.task_id
WHERE
    public.celery_taskmeta.status = 'FAILURE'
    AND (
        COALESCE(public.celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%'
    )
    AND public.celery_taskmeta.date_done > now() - make_interval(secs =>2592000.0)
GROUP BY
    ca_base.plugin_info.id,
    LEFT(COALESCE(ca_base.computation.message, public.celery_taskmeta.traceback), 10)
ORDER BY
    ca_base.plugin_info.id,
    count(*) DESC
""",
    )
    op.create_entity(ca_base_failed_computations)

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


def downgrade() -> None:
    """Downgrade schema."""
    ca_base_artifact_errors = PGView(schema='ca_base', signature='artifact_errors', definition='')
    op.drop_entity(ca_base_artifact_errors)

    ca_base_failed_computations = PGView(schema='ca_base', signature='failed_computations', definition='')
    op.drop_entity(ca_base_failed_computations)

    ca_base_usage_summary = PGView(schema='ca_base', signature='usage_summary', definition='')
    op.drop_entity(ca_base_usage_summary)

    ca_base_computations_summary = PGView(schema='ca_base', signature='computations_summary', definition='')
    op.drop_entity(ca_base_computations_summary)

    ca_base_valid_computations = PGView(schema='ca_base', signature='valid_computations', definition='')
    op.drop_entity(ca_base_valid_computations)
