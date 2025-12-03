from datetime import timedelta
from typing import Type

import sqlalchemy
from alembic_utils.pg_view import PGView
from celery.backends.database import TaskExtended
from geoalchemy2 import Geometry
from sqlalchemy import Date, and_, cast, distinct, not_, or_, select, type_coerce
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import ARRAY, array_agg
from sqlalchemy.sql.functions import coalesce, count, func
from sqlalchemy.sql.functions import now as db_now
from sqlalchemy_utils import create_view
from sqlalchemy_utils.view import CreateView

from climatoology.base.event import ComputationState
from climatoology.store.database.database import DEMO_PREFIX
from climatoology.store.database.models import DbSemver
from climatoology.store.database.models.base import CLIMATOOLOGY_SCHEMA_NAME, ClimatoologyViewBase
from climatoology.store.database.models.computation import ComputationLookupTable, ComputationTable
from climatoology.store.database.models.info import PluginInfoTable


class RawGeometry(Geometry):
    """
    This class is used to remove the 'ST_AsEWKB()' function from select queries as seen in
    https://geoalchemy-2.readthedocs.io/en/latest/gallery/test_disable_wrapping.html#sphx-glr-gallery-test-disable-wrapping-py
    """

    def column_expression(self, col):
        return col


class ValidComputationsView(ClimatoologyViewBase):
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
        .join(TaskExtended, ComputationTable.correlation_uuid == TaskExtended.task_id)
        .where(PluginInfoTable.latest)
        .where(ComputationTable.valid_until > db_now())
        .where(TaskExtended.status == ComputationState.SUCCESS)
    )

    __table__ = create_view(
        name='valid_computations', selectable=select_statement, metadata=ClimatoologyViewBase.metadata
    )
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


REAL_FAILURE_FILTER = and_(
    TaskExtended.status == ComputationState.FAILURE,
    not_(coalesce(TaskExtended.traceback, '').contains('InputValidationError')),
)


class ComputationsSummaryView(ClimatoologyViewBase):
    """Internal reliability report"""

    select_statement = (
        select(
            PluginInfoTable.id.label('plugin_id'),
            PluginInfoTable.version.label('plugin_version'),
            count().label('no_of_computations'),
            (count().filter(TaskExtended.status == ComputationState.SUCCESS)).label('no_of_successes'),
            (count().filter(REAL_FAILURE_FILTER)).label('no_of_failures'),
            (
                cast(
                    func.round(
                        count().filter(REAL_FAILURE_FILTER)
                        / (count().filter(or_(TaskExtended.status == ComputationState.SUCCESS, REAL_FAILURE_FILTER)))
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
            ComputationTable.correlation_uuid == TaskExtended.task_id,
            isouter=True,
        )
        .group_by(PluginInfoTable.id)
        .group_by(PluginInfoTable.version)
        .order_by(PluginInfoTable.id, PluginInfoTable.version.desc())
    )

    __table__ = create_view(
        name='computations_summary', selectable=select_statement, metadata=ClimatoologyViewBase.metadata
    )
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


class UsageView(ClimatoologyViewBase):
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
        .where(not_(ComputationLookupTable.aoi_id.startswith(DEMO_PREFIX)))
        .order_by(count().desc(), PluginInfoTable.id)
    )

    __table__ = create_view(name='usage_summary', selectable=select_statement, metadata=ClimatoologyViewBase.metadata)
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


FAILURE_REPORTING_DAYS = 30

CAUSE_EXTRACTION = func.left(coalesce(ComputationTable.message, TaskExtended.traceback), 10)


class FailedComputationsView(ClimatoologyViewBase):
    """A quick overview of failure per plugin to assess the need for investigation"""

    select_statement = (
        select(
            PluginInfoTable.id.label('plugin_id'),
            count().label(f'no_of_failures_in_last_{FAILURE_REPORTING_DAYS}_days'),
            CAUSE_EXTRACTION.label('cause'),
            array_agg(distinct(cast(TaskExtended.date_done, Date)), type_=ARRAY(Date, as_tuple=True)).label('on_days'),
            array_agg(distinct(PluginInfoTable.version), type_=ARRAY(DbSemver, as_tuple=True)).label('in_versions'),
            array_agg(distinct(ComputationTable.message), type_=ARRAY(sqlalchemy.String, as_tuple=True)).label(
                'with_messages'
            ),
            array_agg(distinct(TaskExtended.traceback), type_=ARRAY(sqlalchemy.String, as_tuple=True)).label(
                'with_tracebacks'
            ),
        )
        .where(REAL_FAILURE_FILTER)
        .join(ComputationTable, ComputationTable.plugin_key == PluginInfoTable.key)
        .join(
            TaskExtended,
            ComputationTable.correlation_uuid == TaskExtended.task_id,
            isouter=True,
        )
        .group_by(PluginInfoTable.id)
        .group_by(CAUSE_EXTRACTION)
        .where(TaskExtended.date_done > (db_now()) - timedelta(days=FAILURE_REPORTING_DAYS))
        .order_by(PluginInfoTable.id, count().desc())
    )

    __table__ = create_view(
        name='failed_computations', selectable=select_statement, metadata=ClimatoologyViewBase.metadata
    )
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


class ArtifactErrorsView(ClimatoologyViewBase):
    """A quick overview of common artifact errors per plugin to assess the need for investigation"""

    aliased_json_values = func.jsonb_each_text(ComputationTable.artifact_errors).table_valued(
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
            ComputationTable.correlation_uuid == TaskExtended.task_id,
            isouter=True,
        )
        .group_by(PluginInfoTable.id)
        .group_by(aliased_json_values.c.key)
        .where(TaskExtended.date_done > (db_now()) - timedelta(days=FAILURE_REPORTING_DAYS))
        .order_by(PluginInfoTable.id, aliased_json_values.c.key, count().desc())
    )

    __table__ = create_view(name='artifact_errors', selectable=select_statement, metadata=ClimatoologyViewBase.metadata)
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


def create_view_tracking_object(view_cls: Type[ClimatoologyViewBase]) -> PGView:
    """
    Create an alembic tracking object for a view, so changes to the view are recorded.

    The workaround is required because PGView does not support creating view from `select()` statements and the two
    libraries (sqlalchemy-utils, used for view creation, and alembic-utils, used for view tracking) are not compatible.

    :param view_cls: the view class to create a tracking object for
    :return: the tracking object
    """
    select_stmt = CreateView(view_cls.__table__.fullname, view_cls.select_statement)
    select_stmt = select_stmt.compile(dialect=postgresql.dialect())
    select_stmt = str(select_stmt).replace(f'CREATE VIEW {view_cls.__table__.fullname} AS ', '')
    tracking_object = PGView(
        schema=view_cls.__table__.schema,
        signature=view_cls.__table__.name,
        definition=select_stmt,
    )
    return tracking_object
