import uuid
from datetime import datetime, timedelta

import alembic
import pytest

from climatoology.base.info import PluginAuthor
from climatoology.store.database.database import BackendDatabase


def test_info_to_db_and_back(default_backend_db, default_info_final):
    plugin_id = default_backend_db.write_info(info=default_info_final)
    assert plugin_id == default_info_final.plugin_id

    read_info = default_backend_db.read_info(plugin_id=default_info_final.plugin_id)
    assert read_info == default_info_final


def test_author_order_is_preserved(default_backend_db, default_info_final):
    info = default_info_final.model_copy(deep=True)
    info.authors = [
        PluginAuthor(
            name='Adam',
        ),
        PluginAuthor(
            name='Bdam',
        ),
        PluginAuthor(
            name='Cdam',
        ),
    ]
    _ = default_backend_db.write_info(info=info)

    info.authors = [
        PluginAuthor(
            name='Adam',
        ),
        PluginAuthor(
            name='Ddam',
        ),
        PluginAuthor(
            name='Bdam',
        ),
    ]
    _ = default_backend_db.write_info(info=info)

    read_info = default_backend_db.read_info(plugin_id=info.plugin_id)
    author_names = ['Adam', 'Ddam', 'Bdam']
    for i in range(0, len(author_names)):
        assert author_names[i] == read_info.authors[i].name


def test_overwrite_info(default_backend_db, default_info_final):
    _ = default_backend_db.write_info(info=default_info_final)

    newer_plugin_info = default_info_final
    newer_plugin_info.version = '3.2.0'
    _ = default_backend_db.write_info(info=default_info_final)

    read_info = default_backend_db.read_info(plugin_id=default_info_final.plugin_id)
    assert read_info == newer_plugin_info


def test_add_authors(default_backend_db, default_info_final):
    _ = default_backend_db.write_info(info=default_info_final)

    changed_plugin_info = default_info_final
    changed_plugin_info.authors.append(PluginAuthor(name='anotherone'))
    _ = default_backend_db.write_info(info=default_info_final)

    read_info = default_backend_db.read_info(plugin_id=default_info_final.plugin_id)
    assert read_info == changed_plugin_info


def test_remove_authors(default_backend_db, default_info_final):
    _ = default_backend_db.write_info(info=default_info_final)

    changed_plugin_info = default_info_final
    changed_plugin_info.authors = [PluginAuthor(name='anotherone')]
    _ = default_backend_db.write_info(info=default_info_final)

    read_info = default_backend_db.read_info(plugin_id=default_info_final.plugin_id)
    assert read_info == changed_plugin_info


@pytest.mark.parametrize(
    'shelf_life,params,expected_deduplication',
    [
        (None, [{'same': True}, {'same': True}], True),
        (None, [{'different': True}, {'different': False}], False),
        (timedelta(0), [{'same': True}, {'same': True}], False),
        (timedelta(0), [{'different': True}, {'different': False}], False),
        (timedelta(days=7), [{'same': True}, {'same': True}], True),
        (timedelta(days=7), [{'different': True}, {'different': False}], False),
        (timedelta(seconds=1), [{'same': True}, {'same': True}], False),
        (timedelta(seconds=1), [{'different': True}, {'different': False}], False),
    ],
)
def test_register_computations(
    default_plugin,
    default_backend_db,
    default_computation_info,
    default_info,
    shelf_life,
    params,
    expected_deduplication,
    stop_time,
    time_machine,
):
    first_correlation_uuid = default_computation_info.correlation_uuid
    second_correlation_uuid = uuid.uuid4()

    db_correlation_uuid_original = default_backend_db.register_computation(
        correlation_uuid=first_correlation_uuid,
        requested_params=params[0],
        aoi=default_computation_info.aoi,
        plugin_id=default_info.plugin_id,
        plugin_version=default_info.version,
        computation_shelf_life=shelf_life,
    )
    time_machine.shift(timedelta(days=1))
    db_correlation_uuid_duplicate = default_backend_db.register_computation(
        correlation_uuid=second_correlation_uuid,
        requested_params=params[1],
        aoi=default_computation_info.aoi,
        plugin_id=default_info.plugin_id,
        plugin_version=default_info.version,
        computation_shelf_life=shelf_life,
    )
    deduplicated = db_correlation_uuid_original == db_correlation_uuid_duplicate
    assert deduplicated == expected_deduplication


def test_read_computation_with_request_ts(backend_with_computation, default_computation_info):
    computation_info = default_computation_info.model_copy()
    computation_info.timestamp = datetime(2025, 1, 1, 12)
    backend_with_computation.update_successful_computation(computation_info=computation_info)
    db_computation_info = backend_with_computation.read_computation(
        correlation_uuid=computation_info.correlation_uuid, state_actual_computation_time=True
    )
    assert db_computation_info.timestamp == datetime(2018, 1, 1, 12)
    assert db_computation_info.message == 'The results were computed on the 2025-01-01 12:00:00'


def test_read_duplicate_computation(
    default_plugin, default_backend_db, default_aoi_feature_geojson_pydantic, default_info, stop_time
):
    first_computation_id = uuid.uuid4()
    second_computation_id = uuid.uuid4()
    params = {'id': 1}
    _ = default_backend_db.register_computation(
        correlation_uuid=second_computation_id,
        requested_params=params,
        aoi=default_aoi_feature_geojson_pydantic,
        plugin_id=default_info.plugin_id,
        plugin_version=default_info.version,
        computation_shelf_life=None,
    )
    _ = default_backend_db.register_computation(
        correlation_uuid=first_computation_id,
        requested_params=params,
        aoi=default_aoi_feature_geojson_pydantic,
        plugin_id=default_info.plugin_id,
        plugin_version=default_info.version,
        computation_shelf_life=None,
    )

    first_computation = default_backend_db.read_computation(correlation_uuid=second_computation_id)
    second_computation = default_backend_db.read_computation(correlation_uuid=first_computation_id)

    assert first_computation == second_computation


def test_resolve_deduplicated_computation_id(
    default_plugin, default_backend_db, default_computation_info, default_info
):
    duplicate_computation_id = uuid.uuid4()
    _ = default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        requested_params=default_computation_info.params,
        aoi=default_computation_info.aoi,
        plugin_id=default_info.plugin_id,
        plugin_version=default_info.version,
        computation_shelf_life=None,
    )
    _ = default_backend_db.register_computation(
        correlation_uuid=duplicate_computation_id,
        requested_params=default_computation_info.params,
        aoi=default_computation_info.aoi,
        plugin_id=default_info.plugin_id,
        plugin_version=default_info.version,
        computation_shelf_life=None,
    )

    db_computation_id = default_backend_db.resolve_computation_id(user_correlation_uuid=duplicate_computation_id)

    assert db_computation_id == default_computation_info.correlation_uuid


def test_update_successful_computation_with_validated_params(
    default_plugin, default_backend_db, default_computation_info, default_info, stop_time
):
    _ = default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        requested_params=default_computation_info.requested_params,
        aoi=default_computation_info.aoi,
        plugin_id=default_info.plugin_id,
        plugin_version=default_info.version,
        computation_shelf_life=default_info.computation_shelf_life,
    )

    default_backend_db.add_validated_params(
        correlation_uuid=default_computation_info.correlation_uuid, params=default_computation_info.params
    )
    default_backend_db.update_successful_computation(computation_info=default_computation_info)

    db_computation = default_backend_db.read_computation(correlation_uuid=default_computation_info.correlation_uuid)
    assert db_computation.artifacts[0].rank == 0
    db_computation.artifacts[0].rank = None
    assert db_computation == default_computation_info


def test_computation_artifact_order_is_preserved(backend_with_computation, default_computation_info, default_artifact):
    info = default_computation_info.model_copy(deep=True)
    second_artifact = default_artifact.model_copy(deep=True)
    second_artifact.name = 'second'

    info.artifacts.append(second_artifact)
    backend_with_computation.update_successful_computation(computation_info=info)

    db_computation = backend_with_computation.read_computation(
        correlation_uuid=default_computation_info.correlation_uuid
    )
    artifact_names = ['test_name', 'second']
    for i in range(0, len(artifact_names)):
        assert artifact_names[i] == db_computation.artifacts[i].name
        assert db_computation.artifacts[i].rank == i


def test_update_failed_computation(default_plugin, default_backend_db, default_computation_info, default_info):
    _ = default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        requested_params=default_computation_info.params,
        aoi=default_computation_info.aoi,
        plugin_id=default_info.plugin_id,
        plugin_version=default_info.version,
        computation_shelf_life=None,
    )

    default_backend_db.update_failed_computation(
        correlation_uuid=str(default_computation_info.correlation_uuid),
        failure_message='Custom failure message',
        cache=False,
    )

    db_computation = default_backend_db.read_computation(correlation_uuid=default_computation_info.correlation_uuid)
    assert db_computation.cache_epoch is None
    assert db_computation.message == 'Custom failure message'


def test_outdated_db_refuses_startup(db_with_postgis, alembic_runner):
    alembic_runner.migrate_up_to('45b227b8bee7')
    with pytest.raises(alembic.util.exc.CommandError, match=r'Target database is not up to date.'):
        BackendDatabase(connection_string=db_with_postgis, user_agent='Test Climatoology Backend')
