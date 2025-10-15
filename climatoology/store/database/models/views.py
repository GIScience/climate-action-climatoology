from datetime import timedelta

import sqlalchemy
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
from climatoology.store.database.models.celery import CeleryTaskMeta
from climatoology.store.database.models.computation import ComputationLookup, ComputationTable
from climatoology.store.database.models.info import InfoTable


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
            InfoTable.name.label('plugin_name'),
            type_coerce(ComputationTable.aoi_geom, type_=RawGeometry).label('aoi'),
            ComputationTable.params,
        )
        .join(InfoTable, ComputationTable.plugin_id == InfoTable.id)
        .join(CeleryTaskMeta, cast(ComputationTable.correlation_uuid, sqlalchemy.String) == CeleryTaskMeta.task_id)
        .where(ComputationTable.plugin_version == InfoTable.version)
        .where(ComputationTable.valid_until > db_now())
        .where(CeleryTaskMeta.status == ComputationState.SUCCESS.value)
    )

    __table__ = create_view(
        name='valid_computations', selectable=select_statement, metadata=ClimatoologyTableBase.metadata
    )
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


real_failure_filter = and_(
    CeleryTaskMeta.status == ComputationState.FAILURE.value,
    not_(coalesce(CeleryTaskMeta.traceback, '').contains('InputValidationError')),
)


class ComputationsSummaryView(ClimatoologyTableBase):
    """Internal reliability report"""

    select_statement = (
        select(
            ComputationTable.plugin_id,
            ComputationTable.plugin_version,
            count().label('no_of_computations'),
            (count().filter(CeleryTaskMeta.status == ComputationState.SUCCESS.value)).label('no_of_successes'),
            (count().filter(real_failure_filter)).label('no_of_failures'),
            (
                cast(
                    func.round(
                        count().filter(real_failure_filter)
                        / (
                            count().filter(
                                or_(CeleryTaskMeta.status == ComputationState.SUCCESS.value, real_failure_filter)
                            )
                        )
                        * 100.0,
                        2,
                    ),
                    sqlalchemy.Float,
                )
            ).label('percent_failed'),
            func.min(ComputationTable.timestamp).label('since'),
            (count().filter(CeleryTaskMeta.traceback.contains('InputValidationError'))).label(
                'no_of_input_validation_fails'
            ),
            (
                count().filter(
                    not_(CeleryTaskMeta.status.in_([ComputationState.SUCCESS.value, ComputationState.FAILURE.value]))
                )
            ).label('no_of_other_states'),
        )
        .join(
            CeleryTaskMeta,
            cast(ComputationTable.correlation_uuid, sqlalchemy.String) == CeleryTaskMeta.task_id,
            isouter=True,
        )
        .group_by(ComputationTable.plugin_id)
        .group_by(ComputationTable.plugin_version)
        .order_by(ComputationTable.plugin_id, ComputationTable.plugin_version.desc())
    )

    __table__ = create_view(
        name='computations_summary', selectable=select_statement, metadata=ClimatoologyTableBase.metadata
    )
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


class UsageView(ClimatoologyTableBase):
    """Internal usage report (ignoring demo-requests)"""

    select_statement = (
        select(
            ComputationTable.plugin_id,
            count().label('no_of_requested_computations'),
            cast(
                func.round(count() / (cast(db_now(), Date) - cast(func.min(ComputationTable.timestamp), Date)), 2),
                sqlalchemy.Float,
            ).label('avg_computations_per_day'),
            func.min(ComputationTable.timestamp).label('since'),
        )
        .join(ComputationLookup, ComputationTable.correlation_uuid == ComputationLookup.computation_id)
        .group_by(ComputationTable.plugin_id)
        .where(not_(ComputationLookup.aoi_id.contains(DEMO_SUFFIX)))
        .order_by(count().desc(), ComputationTable.plugin_id)
    )

    __table__ = create_view(name='usage_summary', selectable=select_statement, metadata=ClimatoologyTableBase.metadata)
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME


FAILURE_REPORTING_DAYS = 30

cause_extraction = func.left(coalesce(ComputationTable.message, CeleryTaskMeta.traceback), 10)


class FailedComputationsView(ClimatoologyTableBase):
    """A quick overview of failure per plugin to assess the need for investigation"""

    select_statement = (
        select(
            ComputationTable.plugin_id,
            count().label(f'no_of_failures_in_last_{FAILURE_REPORTING_DAYS}_days'),
            cause_extraction.label('cause'),
            array_agg(distinct(cast(ComputationTable.timestamp, Date)), type_=ARRAY(Date, as_tuple=True)).label(
                'on_days'
            ),
            array_agg(distinct(ComputationTable.plugin_version), type_=ARRAY(DbSemver, as_tuple=True)).label(
                'in_versions'
            ),
            array_agg(distinct(ComputationTable.message), type_=ARRAY(sqlalchemy.String, as_tuple=True)).label(
                'with_messages'
            ),
            array_agg(distinct(CeleryTaskMeta.traceback), type_=ARRAY(sqlalchemy.String, as_tuple=True)).label(
                'with_tracebacks'
            ),
        )
        .where(real_failure_filter)
        .join(
            CeleryTaskMeta,
            cast(ComputationTable.correlation_uuid, sqlalchemy.String) == CeleryTaskMeta.task_id,
            isouter=True,
        )
        .group_by(ComputationTable.plugin_id)
        .group_by(cause_extraction)
        .where(ComputationTable.timestamp > (db_now()) - timedelta(days=FAILURE_REPORTING_DAYS))
        .order_by(ComputationTable.plugin_id, count().desc())
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
            ComputationTable.plugin_id,
            aliased_json_values.c.key.label('artifact'),
            count().label(f'no_of_computations_with_errors_in_last_{FAILURE_REPORTING_DAYS}_days'),
            array_agg(distinct(cast(ComputationTable.timestamp, Date)), type_=ARRAY(Date, as_tuple=True)).label(
                'on_days'
            ),
            array_agg(distinct(ComputationTable.plugin_version), type_=ARRAY(DbSemver, as_tuple=True)).label(
                'in_versions'
            ),
            array_agg(distinct(aliased_json_values.c.value), type_=ARRAY(sqlalchemy.String, as_tuple=True)).label(
                'with_messages'
            ),
        )
        .group_by(ComputationTable.plugin_id)
        .group_by(aliased_json_values.c.key)
        .where(ComputationTable.timestamp > (db_now()) - timedelta(days=FAILURE_REPORTING_DAYS))
        .order_by(ComputationTable.plugin_id, aliased_json_values.c.key, count().desc())
    )

    __table__ = create_view(
        name='artifact_errors', selectable=select_statement, metadata=ClimatoologyTableBase.metadata
    )
    __table__.schema = CLIMATOOLOGY_SCHEMA_NAME
