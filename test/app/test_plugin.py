import re
from unittest.mock import patch

import pytest
from celery import Celery

from climatoology.app.plugin import _create_plugin, _version_is_compatible, extract_plugin_id
from climatoology.utility.exception import VersionMismatchError


def test_plugin_creation(default_operator, default_settings, mocked_object_store, default_backend_db):
    with patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)
        assert isinstance(plugin, Celery)


def test_plugin_register_task(default_operator, default_settings, mocked_object_store, default_backend_db):
    with patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)

        assert plugin.tasks.unregister('compute') is None


def test_worker_send_compute_task(
    default_plugin,
    default_computation_info,
    general_uuid,
    default_aoi_feature_pure_dict,
    backend_with_computation,
    stop_time,
):
    expected_computation_info = default_computation_info.model_copy(deep=True).model_dump(mode='json')

    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {'id': 1},
    }
    with patch('uuid.uuid4', return_value=general_uuid):
        computed_compute_result = default_plugin.send_task(
            'compute',
            kwargs=kwargs,
            task_id=str(general_uuid),
        ).get(timeout=5)

    assert computed_compute_result == expected_computation_info


def test_successful_compute_saves_metadata_to_storage(
    default_plugin,
    general_uuid,
    default_aoi_feature_pure_dict,
    backend_with_computation,
    default_computation_info,
    mocker,
    stop_time,
):
    expected_computation_info = default_computation_info.model_copy(deep=True)
    save_info_spy = mocker.spy(default_plugin.tasks['compute'], '_save_computation_info')

    with patch('uuid.uuid4', return_value=general_uuid):
        kwargs = {
            'aoi': default_aoi_feature_pure_dict,
            'params': {'id': 1},
        }
        _ = default_plugin.send_task(
            'compute',
            kwargs=kwargs,
            task_id=str(general_uuid),
        ).get(timeout=5)

    save_info_spy.assert_called_once_with(computation_info=expected_computation_info)


def test_successful_compute_saves_metadata_to_backend(
    default_plugin,
    general_uuid,
    default_aoi_feature_pure_dict,
    backend_with_computation,
    default_computation_info,
    stop_time,
):
    expected_computation_info = default_computation_info.model_copy(deep=True)

    with patch('uuid.uuid4', return_value=general_uuid):
        kwargs = {
            'aoi': default_aoi_feature_pure_dict,
            'params': {'id': 1},
        }
        _ = default_plugin.send_task(
            'compute',
            kwargs=kwargs,
            task_id=str(general_uuid),
        ).get(timeout=5)

    saved_computation = backend_with_computation.read_computation(correlation_uuid=general_uuid)
    assert saved_computation == expected_computation_info


def test_failing_compute_updates_backend(
    default_plugin, general_uuid, default_aoi_feature_pure_dict, backend_with_computation
):
    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {},
    }
    with patch('uuid.uuid4', return_value=general_uuid), pytest.raises(Exception):
        _ = default_plugin.send_task(
            'compute',
            kwargs=kwargs,
            task_id=str(general_uuid),
        ).get(timeout=5)

    updated_computation = backend_with_computation.read_computation(correlation_uuid=general_uuid)
    assert updated_computation.message == 'ID: Field required. You provided: {}.'


def test_version_matches_raises_on_lower(default_backend_db, default_info_final, celery_app):
    _ = default_backend_db.write_info(info=default_info_final)

    older_plugin_info = default_info_final
    older_plugin_info.version = '2.1.0'
    with pytest.raises(VersionMismatchError, match=r'Refusing to register plugin*'):
        _version_is_compatible(info=older_plugin_info, db=default_backend_db, celery=celery_app)


def test_version_matches_equal(default_backend_db, default_info_final, celery_app):
    _ = default_backend_db.write_info(info=default_info_final)
    assert _version_is_compatible(info=default_info_final, db=default_backend_db, celery=celery_app)


def test_version_matches_no_plugin_registered(default_backend_db, default_info_final, celery_app):
    assert _version_is_compatible(info=default_info_final, db=default_backend_db, celery=celery_app)


def test_version_matches_higher(default_backend_db, default_info_final, celery_app):
    _ = default_backend_db.write_info(info=default_info_final)

    newer_plugin_info = default_info_final
    newer_plugin_info.version = '3.1.1'
    assert _version_is_compatible(info=newer_plugin_info, db=default_backend_db, celery=celery_app)


def test_version_matches_higher_not_alone(default_plugin, default_backend_db, default_info_final, celery_app):
    newer_plugin_info = default_info_final.model_copy(deep=True)
    newer_plugin_info.version = '3.1.1'
    with pytest.raises(
        AssertionError,
        match=re.escape(
            'Refusing to register plugin Test Plugin version 3.1.1 because a plugin with a lower version (3.1.0) is already running. Make sure to stop it before upgrading.',
        ),
    ):
        _version_is_compatible(info=newer_plugin_info, db=default_backend_db, celery=celery_app)


def test_extract_plugin_id():
    computed_plugin_id = extract_plugin_id('a@b')
    assert computed_plugin_id == 'a'
