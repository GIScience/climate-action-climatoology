from datetime import timedelta

import sqlalchemy
from celery.backends.database import TaskExtended
from geoalchemy2 import Geometry
from sqlalchemy import Date, and_, cast, distinct, not_, or_, select, type_coerce
from sqlalchemy.dialects.postgresql import ARRAY, array_agg
from sqlalchemy.sql.functions import coalesce, count, func
from sqlalchemy.sql.functions import now as db_now
from sqlalchemy_utils import create_view

from climatoology.base.event import ComputationState
from climatoology.store.database.database import DEMO_SUFFIX
from climatoology.store.database.models import DbSemver
from climatoology.store.database.models.base import CLIMATOOLOGY_SCHEMA_NAME, ClimatoologyTableBase
from climatoology.store.database.models.computation import ComputationLookupTable, ComputationTable
from climatoology.store.database.models.info import PluginInfoTable


class RawGeometry(Geometry):
    """
    This class is used to remove the 'ST_AsEWKB()' function from select queries as seen in
    https://geoalchemy-2.readthedocs.io/en/latest/gallery/test_disable_wrapping.html#sphx-glr-gallery-test-disable-wrapping-py
    """

    def column_expression(self, col):
        return col


class ValidComputationsView(ClimatoologyTableBase):
    """A potentially public-facing collection of currently valid computations.

    The view is deliberately not ordered because it should be filtered by plugin on the client side.
    """

    select_statement = (
        select(
            ComputationTable.correlation_uuid,
            PluginInfoTable.name.label('plugin_name'),
            type_coerce(ComputationTable.aoi_geom, type_=RawGeometry).label('aoi'),
            ComputationTable.params,
        )
        .join(PluginInfoTable, ComputationTable.plugin_key == PluginInfoTable.key)
        .join(TaskExtended, cast(ComputationTable.correlation_uuid, sqlalchemy.String) == TaskExtended.task_id)
        .where(PluginInfoTable.latest)
        .where(ComputationTable.valid_until > db_now())
        .where(TaskExtended.status == ComputationState.SUCCESS)
    )

    __table__ = create_view(
        name='valid_computations', selectable=select_statement, metadata=ClimatoologyTableBase.metadata
    )
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


real_failure_filter = and_(
    TaskExtended.status == ComputationState.FAILURE,
    not_(coalesce(TaskExtended.traceback, '').contains('InputValidationError')),
)


class ComputationsSummaryView(ClimatoologyTableBase):
    """Internal reliability report"""

    select_statement = (
        select(
            PluginInfoTable.id.label('plugin_id'),
            PluginInfoTable.version.label('plugin_version'),
            count().label('no_of_computations'),
            (count().filter(TaskExtended.status == ComputationState.SUCCESS)).label('no_of_successes'),
            (count().filter(real_failure_filter)).label('no_of_failures'),
            (
                cast(
                    func.round(
                        count().filter(real_failure_filter)
                        / (count().filter(or_(TaskExtended.status == ComputationState.SUCCESS, real_failure_filter)))
                        * 100.0,
                        2,
                    ),
                    sqlalchemy.Float,
                )
            ).label('percent_failed'),
            func.min(TaskExtended.date_done).label('since'),
            (count().filter(TaskExtended.traceback.contains('InputValidationError'))).label(
                'no_of_input_validation_fails'
            ),
            (count().filter(not_(TaskExtended.status.in_([ComputationState.SUCCESS, ComputationState.FAILURE])))).label(
                'no_of_other_states'
            ),
        )
        .join(ComputationTable, ComputationTable.plugin_key == PluginInfoTable.key)
        .join(
            TaskExtended,
            cast(ComputationTable.correlation_uuid, sqlalchemy.String) == TaskExtended.task_id,
            isouter=True,
        )
        .group_by(PluginInfoTable.id)
        .group_by(PluginInfoTable.version)
        .order_by(PluginInfoTable.id, PluginInfoTable.version.desc())
    )

    __table__ = create_view(
        name='computations_summary', selectable=select_statement, metadata=ClimatoologyTableBase.metadata
    )
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


class UsageView(ClimatoologyTableBase):
    """Internal usage report (ignoring demo-requests)"""

    select_statement = (
        select(
            PluginInfoTable.id.label('plugin_id'),
            count().label('no_of_requested_computations'),
            cast(
                func.round(
                    count() / (cast(db_now(), Date) - cast(func.min(ComputationLookupTable.request_ts), Date)), 2
                ),
                sqlalchemy.Float,
            ).label('avg_computations_per_day'),
            func.min(ComputationLookupTable.request_ts).label('since'),
        )
        .join(ComputationTable, ComputationTable.plugin_key == PluginInfoTable.key)
        .join(ComputationLookupTable, ComputationTable.correlation_uuid == ComputationLookupTable.computation_id)
        .group_by(PluginInfoTable.id)
        .where(not_(ComputationLookupTable.aoi_id.contains(DEMO_SUFFIX)))
        .order_by(count().desc(), PluginInfoTable.id)
    )

    __table__ = create_view(name='usage_summary', selectable=select_statement, metadata=ClimatoologyTableBase.metadata)
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


FAILURE_REPORTING_DAYS = 30

cause_extraction = func.left(coalesce(ComputationTable.message, TaskExtended.traceback), 10)


class FailedComputationsView(ClimatoologyTableBase):
    """A quick overview of failure per plugin to assess the need for investigation"""

    select_statement = (
        select(
            PluginInfoTable.id.label('plugin_id'),
            count().label(f'no_of_failures_in_last_{FAILURE_REPORTING_DAYS}_days'),
            cause_extraction.label('cause'),
            array_agg(distinct(cast(TaskExtended.date_done, Date)), type_=ARRAY(Date, as_tuple=True)).label('on_days'),
            array_agg(distinct(PluginInfoTable.version), type_=ARRAY(DbSemver, as_tuple=True)).label('in_versions'),
            array_agg(distinct(ComputationTable.message), type_=ARRAY(sqlalchemy.String, as_tuple=True)).label(
                'with_messages'
            ),
            array_agg(distinct(TaskExtended.traceback), type_=ARRAY(sqlalchemy.String, as_tuple=True)).label(
                'with_tracebacks'
            ),
        )
        .where(real_failure_filter)
        .join(ComputationTable, ComputationTable.plugin_key == PluginInfoTable.key)
        .join(
            TaskExtended,
            cast(ComputationTable.correlation_uuid, sqlalchemy.String) == TaskExtended.task_id,
            isouter=True,
        )
        .group_by(PluginInfoTable.id)
        .group_by(cause_extraction)
        .where(TaskExtended.date_done > (db_now()) - timedelta(days=FAILURE_REPORTING_DAYS))
        .order_by(PluginInfoTable.id, count().desc())
    )

    __table__ = create_view(
        name='failed_computations', selectable=select_statement, metadata=ClimatoologyTableBase.metadata
    )
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


class ArtifactErrorsView(ClimatoologyTableBase):
    """A quick overview of common artifact errors per plugin to assess the need for investigation"""

    aliased_json_values = func.json_each_text(ComputationTable.artifact_errors).table_valued(
        'key', 'value', name='artifact_errors'
    )
    select_statement = (
        select(
            PluginInfoTable.id.label('plugin_id'),
            aliased_json_values.c.key.label('artifact'),
            count().label(f'no_of_computations_with_errors_in_last_{FAILURE_REPORTING_DAYS}_days'),
            array_agg(distinct(cast(TaskExtended.date_done, Date)), type_=ARRAY(Date, as_tuple=True)).label('on_days'),
            array_agg(distinct(PluginInfoTable.version), type_=ARRAY(DbSemver, as_tuple=True)).label('in_versions'),
            array_agg(distinct(aliased_json_values.c.value), type_=ARRAY(sqlalchemy.String, as_tuple=True)).label(
                'with_messages'
            ),
        )
        .join(ComputationTable, ComputationTable.plugin_key == PluginInfoTable.key)
        .join(
            TaskExtended,
            cast(ComputationTable.correlation_uuid, sqlalchemy.String) == TaskExtended.task_id,
            isouter=True,
        )
        .group_by(PluginInfoTable.id)
        .group_by(aliased_json_values.c.key)
        .where(TaskExtended.date_done > (db_now()) - timedelta(days=FAILURE_REPORTING_DAYS))
        .order_by(PluginInfoTable.id, aliased_json_values.c.key, count().desc())
    )

    __table__ = create_view(
        name='artifact_errors', selectable=select_statement, metadata=ClimatoologyTableBase.metadata
    )
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME
