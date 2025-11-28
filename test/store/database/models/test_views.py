import uuid
from datetime import date, datetime, timedelta

from celery.backends.database import TaskExtended
from geoalchemy2.shape import to_shape
from semver import Version
from sqlalchemy import String, cast, insert, select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import now as db_now

from climatoology.base.event import ComputationState
from climatoology.store.database.database import DEMO_SUFFIX, row_to_dict
from climatoology.store.database.models.computation import ComputationTable
from climatoology.store.database.models.views import (
    ArtifactErrorsView,
    ComputationsSummaryView,
    FailedComputationsView,
    UsageView,
    ValidComputationsView,
)

REAL_DATE = date.today()


def test_valid_computations_view(backend_with_computation_successful, general_uuid, default_aoi_geom_shapely):
    expected_view = {
        'correlation_uuid': general_uuid,
        'plugin_name': 'Test Plugin',
        'params': {'id': 1, 'name': 'John Doe', 'execution_time': 0.0},
        'aoi': default_aoi_geom_shapely,
    }

    with Session(backend_with_computation_successful.engine) as session:
        session.execute(update(ComputationTable).values(valid_until=db_now() + timedelta(hours=1)))

        valid_computation_select = select(ValidComputationsView)
        result_scalars = session.scalars(valid_computation_select)
        results = result_scalars.fetchall()

        assert len(results) == 1

        result = results[0]

    result_dict = row_to_dict(result)
    result_dict['aoi'] = to_shape(result.aoi)
    assert result_dict == expected_view


def test_valid_computations_view_multiple(
    backend_with_computation_successful, default_computation_info, default_info_final, default_plugin_key
):
    correlation_uuid = uuid.uuid4()
    backend_with_computation_successful.register_computation(
        correlation_uuid=correlation_uuid,
        requested_params={},
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_info_final.computation_shelf_life,
    )

    with Session(backend_with_computation_successful.engine) as session:
        session.execute(insert(TaskExtended).values(id='2', task_id=correlation_uuid, status=ComputationState.SUCCESS))
        session.execute(update(ComputationTable).values(valid_until=db_now() + timedelta(hours=1)))

        valid_computation_select = select(ValidComputationsView)
        result_scalars = session.scalars(valid_computation_select)
        results = result_scalars.fetchall()

        assert len(results) == 2


def test_valid_computations_view_plugin_version(
    backend_with_computation_successful, default_computation_info, default_info_final
):
    """A new computation with a different plugin version should not be listed"""
    older_plugin_info = default_info_final.model_copy(deep=True)
    older_plugin_info.version = Version(0, 1, 0)
    backend_with_computation_successful.write_info(info=older_plugin_info)

    correlation_uuid = uuid.uuid4()
    backend_with_computation_successful.register_computation(
        correlation_uuid=correlation_uuid,
        requested_params=default_computation_info.requested_params,
        aoi=default_computation_info.aoi,
        plugin_key=f'{older_plugin_info.id};{older_plugin_info.version}',
        computation_shelf_life=older_plugin_info.computation_shelf_life,
    )

    with Session(backend_with_computation_successful.engine) as session:
        session.execute(insert(TaskExtended).values(id='2', task_id=correlation_uuid, status=ComputationState.SUCCESS))
        session.execute(update(ComputationTable).values(valid_until=db_now() + timedelta(hours=1)))

        valid_computation_select = select(ValidComputationsView)
        result_scalars = session.scalars(valid_computation_select)
        results = result_scalars.fetchall()

        assert len(results) == 1


def test_valid_computations_view_only_valid(
    default_backend_db, default_info_final, default_computation_info, default_plugin_key
):
    default_backend_db.write_info(info=default_info_final)
    default_backend_db.register_computation(
        correlation_uuid=uuid.uuid4(),
        requested_params=default_computation_info.requested_params,
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=None,
    )
    with Session(default_backend_db.engine) as session:
        valid_computation_select = select(ValidComputationsView)
        result_scalars = session.scalars(valid_computation_select)
        results = result_scalars.fetchall()

        assert len(results) == 0


def test_valid_computations_view_only_successful(backend_with_computation_registered, default_computation_info):
    with Session(backend_with_computation_registered.engine) as session:
        update_stmt = (
            update(TaskExtended)
            .values(status=ComputationState.FAILURE)
            .where(TaskExtended.task_id == cast(default_computation_info.correlation_uuid, String))
        )
        session.execute(update_stmt)
        valid_computation_select = select(ValidComputationsView)
        result_scalars = session.scalars(valid_computation_select)
        results = result_scalars.fetchall()

        assert len(results) == 0


def test_computation_summary_view(
    backend_with_computation_successful, default_computation_info, default_info_final, default_plugin_key
):
    expected_view = {
        'plugin_id': 'test_plugin',
        'plugin_version': Version(3, 1, 0),
        'no_of_computations': 4,
        'no_of_successes': 1,
        'no_of_failures': 1,
        'percent_failed': 50.0,
        'since': datetime(2018, 1, 1, 12, 0),
        'no_of_input_validation_fails': 1,
        'no_of_other_states': 1,
    }

    correlation_uuid_failure = uuid.uuid4()
    backend_with_computation_successful.register_computation(
        correlation_uuid=correlation_uuid_failure,
        requested_params={'dont': 'deduplicate failure!'},
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_info_final.computation_shelf_life,
    )

    correlation_uuid_validation_failure = uuid.uuid4()
    backend_with_computation_successful.register_computation(
        correlation_uuid=correlation_uuid_validation_failure,
        requested_params={'dont': 'deduplicate invalid input!'},
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_info_final.computation_shelf_life,
    )

    correlation_uuid_pending = uuid.uuid4()
    backend_with_computation_successful.register_computation(
        correlation_uuid=correlation_uuid_pending,
        requested_params={'dont': 'deduplicate pending!'},
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_info_final.computation_shelf_life,
    )

    with Session(backend_with_computation_successful.engine) as session:
        session.execute(
            insert(TaskExtended).values(id='2', task_id=correlation_uuid_failure, status=ComputationState.FAILURE)
        )
        session.execute(
            insert(TaskExtended).values(
                id='3',
                task_id=correlation_uuid_validation_failure,
                status=ComputationState.FAILURE,
                traceback='climatoology.utility.exception.InputValidationError: Start: Field required. You provided: {}. End: Field required. You provided: {}.',
            )
        )
        session.execute(
            insert(TaskExtended).values(id='4', task_id=correlation_uuid_pending, status=ComputationState.PENDING)
        )

        computations_summary = select(ComputationsSummaryView)
        result_scalars = session.scalars(computations_summary)
        results = result_scalars.fetchall()

        assert len(results) == 1

        result = results[0]

    result_dict = row_to_dict(result)
    assert result_dict == expected_view


def test_usage_view(
    backend_with_computation_registered, default_computation_info, default_info_final, default_plugin_key
):
    expected_view = {
        'plugin_id': 'test_plugin',
        'no_of_requested_computations': 2,
        'avg_computations_per_day': 0.0,
        'since': datetime(2018, 1, 1, 12, 0),
    }

    deduplicated_correlation_uuid = uuid.uuid4()
    backend_with_computation_registered.register_computation(
        correlation_uuid=deduplicated_correlation_uuid,
        requested_params=default_computation_info.requested_params,
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_info_final.computation_shelf_life,
    )

    with Session(backend_with_computation_registered.engine) as session:
        usage_select = select(UsageView)
        result_scalars = session.scalars(usage_select)
        results = result_scalars.fetchall()

        assert len(results) == 1

        result = results[0]

    result_dict = row_to_dict(result)
    assert result_dict == expected_view


def test_usage_view_excludes_demo(
    backend_with_computation_registered, default_computation_info, default_info_final, default_plugin_key
):
    loc_computation_info = default_computation_info.model_copy(deep=True)
    expected_view = {
        'plugin_id': 'test_plugin',
        'no_of_requested_computations': 1,
        'avg_computations_per_day': 0.0,
        'since': datetime(2018, 1, 1, 12, 0),
    }

    loc_computation_info.correlation_uuid = uuid.uuid4()
    loc_computation_info.aoi.properties.id = f'test_plugin{DEMO_SUFFIX}'
    backend_with_computation_registered.register_computation(
        correlation_uuid=loc_computation_info.correlation_uuid,
        requested_params=loc_computation_info.requested_params,
        aoi=loc_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_info_final.computation_shelf_life,
    )

    with Session(backend_with_computation_registered.engine) as session:
        usage_select = select(UsageView)
        result_scalars = session.scalars(usage_select)
        results = result_scalars.fetchall()

        assert len(results) == 1

        result = results[0]

    result_dict = row_to_dict(result)
    assert result_dict == expected_view


def test_failed_computations_view(backend_with_computation_registered, default_computation_info, default_info_final):
    expected_view = {
        'plugin_id': 'test_plugin',
        'no_of_failures_in_last_30_days': 1,
        'cause': 'Failure me',
        'on_days': (REAL_DATE,),
        'in_versions': (Version(3, 1, 0),),
        'with_messages': ('Failure message',),
        'with_tracebacks': ('Very long traceback',),
    }

    backend_with_computation_registered.update_failed_computation(
        correlation_uuid=default_computation_info.correlation_uuid, failure_message='Failure message', cache=False
    )
    with Session(backend_with_computation_registered.engine) as session:
        update_stmt = (
            update(TaskExtended)
            .values(
                date_done=db_now(),
                status=ComputationState.FAILURE,
                traceback='Very long traceback',
            )
            .where(TaskExtended.task_id == cast(default_computation_info.correlation_uuid, String))
        )
        session.execute(update_stmt)

        usage_select = select(FailedComputationsView)
        result_scalars = session.scalars(usage_select)
        results = result_scalars.fetchall()

        assert len(results) == 1

        result = results[0]

    result_dict = row_to_dict(result)
    assert result_dict == expected_view


def test_failed_computations_view_multiple_and_traceback(
    backend_with_computation_registered, default_computation_info, default_info_final, default_plugin_key
):
    expected_view = [
        {
            'plugin_id': 'test_plugin',
            'no_of_failures_in_last_30_days': 1,
            'cause': 'Failure me',
            'on_days': (REAL_DATE,),
            'in_versions': (Version(3, 1, 0),),
            'with_messages': ('Failure message',),
            'with_tracebacks': ('Very long traceback',),
        },
        {
            'plugin_id': 'test_plugin',
            'no_of_failures_in_last_30_days': 1,
            'cause': 'Other trac',
            'on_days': (REAL_DATE,),
            'in_versions': (Version(3, 1, 0),),
            'with_messages': (None,),
            'with_tracebacks': ('Other traceback',),
        },
    ]

    backend_with_computation_registered.update_failed_computation(
        correlation_uuid=default_computation_info.correlation_uuid, failure_message='Failure message', cache=False
    )

    other_correlation_uuid = uuid.uuid4()
    backend_with_computation_registered.register_computation(
        correlation_uuid=other_correlation_uuid,
        requested_params={},
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_info_final.computation_shelf_life,
    )
    backend_with_computation_registered.update_failed_computation(
        correlation_uuid=other_correlation_uuid, failure_message=None, cache=False
    )
    with Session(backend_with_computation_registered.engine) as session:
        update_stmt = update(TaskExtended).values(
            date_done=db_now(),
            status=ComputationState.FAILURE,
            traceback='Very long traceback',
        )
        session.execute(update_stmt)

        session.execute(
            insert(TaskExtended).values(
                id='2',
                task_id=other_correlation_uuid,
                date_done=db_now(),
                status=ComputationState.FAILURE,
                traceback='Other traceback',
            )
        )

        usage_select = select(FailedComputationsView)
        result_scalars = session.scalars(usage_select)
        results = result_scalars.fetchall()

    result_dict = [row_to_dict(result) for result in results]
    assert result_dict == expected_view


def test_artifact_errors_view(backend_with_computation_registered, default_computation_info, default_info_final):
    expected_view = {
        'plugin_id': 'test_plugin',
        'no_of_computations_with_errors_in_last_30_days': 1,
        'artifact': 'artifact one',
        'on_days': (REAL_DATE,),
        'in_versions': (Version(3, 1, 0),),
        'with_messages': ('Artifact could not be computed',),
    }

    with Session(backend_with_computation_registered.engine) as session:
        session.execute(update(TaskExtended).values(date_done=db_now()))
        session.execute(
            update(ComputationTable).values(artifact_errors={'artifact one': 'Artifact could not be computed'})
        )

        usage_select = select(ArtifactErrorsView)
        result_scalars = session.scalars(usage_select)
        results = result_scalars.fetchall()

        assert len(results) == 1

        result = results[0]

    result_dict = row_to_dict(result)
    assert result_dict == expected_view


def test_artifact_errors_view_empty_on_none(
    backend_with_computation_registered, default_computation_info, default_info_final
):
    with Session(backend_with_computation_registered.engine) as session:
        session.execute(update(TaskExtended).values(date_done=db_now()))

        usage_select = select(ArtifactErrorsView)
        result_scalars = session.scalars(usage_select)
        results = result_scalars.fetchall()

        assert len(results) == 0


def test_artifact_errors_view_multiple_errors(
    backend_with_computation_registered, default_computation_info, default_info_final
):
    expected_view = [
        {
            'artifact': 'artifact one',
            'in_versions': (Version(3, 1, 0),),
            'no_of_computations_with_errors_in_last_30_days': 1,
            'on_days': (REAL_DATE,),
            'plugin_id': 'test_plugin',
            'with_messages': ('Artifact could not be computed',),
        },
        {
            'artifact': 'artifact two',
            'in_versions': (Version(3, 1, 0),),
            'no_of_computations_with_errors_in_last_30_days': 1,
            'on_days': (REAL_DATE,),
            'plugin_id': 'test_plugin',
            'with_messages': ('Artifact could not be computed',),
        },
    ]

    with Session(backend_with_computation_registered.engine) as session:
        session.execute(update(TaskExtended).values(date_done=db_now()))
        session.execute(
            update(ComputationTable).values(
                artifact_errors={
                    'artifact one': 'Artifact could not be computed',
                    'artifact two': 'Artifact could not be computed',
                },
            )
        )

        usage_select = select(ArtifactErrorsView)
        result_scalars = session.scalars(usage_select)
        results = result_scalars.fetchall()

    result_dict = [row_to_dict(result) for result in results]
    assert result_dict == expected_view


def test_failed_computations_view_multiple_computations(
    backend_with_computation_registered, default_computation_info, default_info_final, default_plugin_key
):
    expected_view = {
        'artifact': 'artifact one',
        'in_versions': (Version(3, 1, 0),),
        'no_of_computations_with_errors_in_last_30_days': 2,
        'on_days': (REAL_DATE,),
        'plugin_id': 'test_plugin',
        'with_messages': ('Artifact could not be computed',),
    }

    other_correlation_uuid = uuid.uuid4()
    backend_with_computation_registered.register_computation(
        correlation_uuid=other_correlation_uuid,
        requested_params={},
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_info_final.computation_shelf_life,
    )
    with Session(backend_with_computation_registered.engine) as session:
        session.execute(insert(TaskExtended).values(id='2', task_id=other_correlation_uuid))
        session.execute(update(TaskExtended).values(date_done=db_now()))
        session.execute(
            update(ComputationTable).values(
                artifact_errors={
                    'artifact one': 'Artifact could not be computed',
                },
            )
        )

        usage_select = select(ArtifactErrorsView)
        result_scalars = session.scalars(usage_select)
        results = result_scalars.fetchall()
        assert len(results) == 1

        result = results[0]

    result_dict = row_to_dict(result)
    assert result_dict == expected_view


def test_failed_computations_view_multiple_computations_different(
    backend_with_computation_registered, default_computation_info, default_info_final, default_plugin_key
):
    expected_view = [
        {
            'plugin_id': 'test_plugin',
            'no_of_computations_with_errors_in_last_30_days': 1,
            'artifact': 'artifact one',
            'on_days': (REAL_DATE,),
            'in_versions': (Version(3, 1, 0),),
            'with_messages': ('Artifact could not be computed',),
        },
        {
            'plugin_id': 'test_plugin',
            'no_of_computations_with_errors_in_last_30_days': 1,
            'artifact': 'artifact two',
            'on_days': (REAL_DATE,),
            'in_versions': (Version(3, 1, 0),),
            'with_messages': ('Artifact could not be computed',),
        },
    ]

    other_correlation_uuid = uuid.uuid4()
    backend_with_computation_registered.register_computation(
        correlation_uuid=other_correlation_uuid,
        requested_params={},
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_info_final.computation_shelf_life,
    )
    with Session(backend_with_computation_registered.engine) as session:
        session.execute(insert(TaskExtended).values(id='2', task_id=other_correlation_uuid))
        session.execute(
            update(ComputationTable)
            .values(artifact_errors={'artifact one': 'Artifact could not be computed'})
            .where(ComputationTable.correlation_uuid == default_computation_info.correlation_uuid)
        )
        session.execute(
            update(ComputationTable)
            .values(artifact_errors={'artifact two': 'Artifact could not be computed'})
            .where(ComputationTable.correlation_uuid == other_correlation_uuid)
        )

        usage_select = select(ArtifactErrorsView)
        result_scalars = session.scalars(usage_select)
        results = result_scalars.fetchall()

    result_dict = [row_to_dict(result) for result in results]
    assert result_dict == expected_view
