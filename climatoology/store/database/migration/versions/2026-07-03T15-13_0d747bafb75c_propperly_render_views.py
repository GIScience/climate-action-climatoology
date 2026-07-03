"""propperly-render-views

Revision ID: 0d747bafb75c
Revises: 8f6ee7328f1e
Create Date: 2026-07-03 15:13:42.589123

"""

from typing import Sequence, Union

from alembic import op
from alembic_utils.pg_view import PGView

# revision identifiers, used by Alembic.
revision: str = '0d747bafb75c'
down_revision: Union[str, None] = '8f6ee7328f1e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    ca_base_computations_summary = PGView(
        schema='ca_base',
        signature='computations_summary',
        definition="SELECT ca_base.plugin_info.id AS plugin_id, ca_base.plugin_info.version AS plugin_version, count(*) AS no_of_computations, count(*) FILTER (WHERE celery_taskmeta.status = 'SUCCESS') AS no_of_successes, count(*) FILTER (WHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%' || 'InputValidationError' || '%')) AS no_of_failures, CAST(round(((count(*) FILTER (WHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%' || 'InputValidationError' || '%'))) / CAST((count(*) FILTER (WHERE celery_taskmeta.status = 'SUCCESS' OR celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%' || 'InputValidationError' || '%'))) AS NUMERIC)) * 100.0, 2) AS FLOAT) AS percent_failed, min(celery_taskmeta.date_done) AS since, count(*) FILTER (WHERE celery_taskmeta.traceback LIKE '%' || 'InputValidationError' || '%') AS no_of_input_validation_fails, count(*) FILTER (WHERE (celery_taskmeta.status NOT IN ('SUCCESS', 'FAILURE'))) AS no_of_other_states \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key LEFT OUTER JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id GROUP BY ca_base.plugin_info.id, ca_base.plugin_info.version ORDER BY ca_base.plugin_info.id, ca_base.plugin_info.version DESC",
    )
    op.replace_entity(ca_base_computations_summary)

    ca_base_failed_computations = PGView(
        schema='ca_base',
        signature='failed_computations',
        definition="SELECT ca_base.plugin_info.id AS plugin_id, count(*) AS no_of_failures_in_last_30_days, left(coalesce(ca_base.computation.message, celery_taskmeta.traceback), 10) AS cause, array_agg(DISTINCT CAST(celery_taskmeta.date_done AS DATE)) AS on_days, array_agg(DISTINCT ca_base.plugin_info.version) AS in_versions, array_agg(DISTINCT ca_base.computation.message) AS with_messages, array_agg(DISTINCT celery_taskmeta.traceback) AS with_tracebacks \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key LEFT OUTER JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id \nWHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%' || 'InputValidationError' || '%') AND celery_taskmeta.date_done > now() - make_interval(secs=>2592000.0) GROUP BY ca_base.plugin_info.id, left(coalesce(ca_base.computation.message, celery_taskmeta.traceback), 10) ORDER BY ca_base.plugin_info.id, count(*) DESC",
    )
    op.replace_entity(ca_base_failed_computations)


def downgrade() -> None:
    """Downgrade schema."""
    ca_base_failed_computations = PGView(
        schema='ca_base',
        signature='failed_computations',
        definition="SELECT ca_base.plugin_info.id AS plugin_id, count(*) AS no_of_failures_in_last_30_days, left(coalesce(ca_base.computation.message, celery_taskmeta.traceback), 10) AS cause, array_agg(DISTINCT CAST(celery_taskmeta.date_done AS DATE)) AS on_days, array_agg(DISTINCT ca_base.plugin_info.version) AS in_versions, array_agg(DISTINCT ca_base.computation.message) AS with_messages, array_agg(DISTINCT celery_taskmeta.traceback) AS with_tracebacks \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key LEFT OUTER JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id \nWHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%') AND celery_taskmeta.date_done > now() - make_interval(secs=>2592000.0) GROUP BY ca_base.plugin_info.id, left(coalesce(ca_base.computation.message, celery_taskmeta.traceback), 10) ORDER BY ca_base.plugin_info.id, count(*) DESC",
    )
    op.replace_entity(ca_base_failed_computations)
    ca_base_computations_summary = PGView(
        schema='ca_base',
        signature='computations_summary',
        definition="SELECT ca_base.plugin_info.id AS plugin_id, ca_base.plugin_info.version AS plugin_version, count(*) AS no_of_computations, count(*) FILTER (WHERE celery_taskmeta.status = 'SUCCESS') AS no_of_successes, count(*) FILTER (WHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%')) AS no_of_failures, CAST(round(((count(*) FILTER (WHERE celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%'))) / CAST((count(*) FILTER (WHERE celery_taskmeta.status = 'SUCCESS' OR celery_taskmeta.status = 'FAILURE' AND (coalesce(celery_taskmeta.traceback, '') NOT LIKE '%%' || 'InputValidationError' || '%%'))) AS NUMERIC)) * 100.0, 2) AS FLOAT) AS percent_failed, min(celery_taskmeta.date_done) AS since, count(*) FILTER (WHERE celery_taskmeta.traceback LIKE '%%' || 'InputValidationError' || '%%') AS no_of_input_validation_fails, count(*) FILTER (WHERE (celery_taskmeta.status NOT IN ('SUCCESS', 'FAILURE'))) AS no_of_other_states \nFROM ca_base.plugin_info JOIN ca_base.computation ON ca_base.computation.plugin_key = ca_base.plugin_info.key LEFT OUTER JOIN celery_taskmeta ON ca_base.computation.correlation_uuid = celery_taskmeta.task_id GROUP BY ca_base.plugin_info.id, ca_base.plugin_info.version ORDER BY ca_base.plugin_info.id, ca_base.plugin_info.version DESC",
    )
    op.replace_entity(ca_base_computations_summary)
