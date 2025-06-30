import datetime
import uuid

import pytest

from climatoology.base.info import PluginAuthor


def test_info_to_db_and_back(default_backend_db, default_info_final):
    plugin_id = default_backend_db.write_info(info=default_info_final)
    assert plugin_id == default_info_final.plugin_id

    read_info = default_backend_db.read_info(plugin_id=default_info_final.plugin_id)
    assert read_info == default_info_final


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
        (datetime.timedelta(0), [{'same': True}, {'same': True}], False),
        (datetime.timedelta(0), [{'different': True}, {'different': False}], False),
        (datetime.timedelta(days=7), [{'same': True}, {'same': True}], True),
        (datetime.timedelta(days=7), [{'different': True}, {'different': False}], False),
        (datetime.timedelta(seconds=1), [{'same': True}, {'same': True}], False),
        (datetime.timedelta(seconds=1), [{'different': True}, {'different': False}], False),
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
    time_machine.shift(datetime.timedelta(days=1))
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
    computation_info.timestamp = datetime.datetime(2025, 1, 1, 12)
    backend_with_computation.update_successful_computation(computation_info=computation_info)
    db_computation_info = backend_with_computation.read_computation(
        correlation_uuid=computation_info.correlation_uuid, state_actual_computation_time=True
    )
    assert db_computation_info.timestamp == datetime.datetime(2018, 1, 1, 12)
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
    assert db_computation == default_computation_info


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
