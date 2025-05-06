import datetime
from unittest.mock import patch

import pytest
from celery import Celery

from climatoology.app.plugin import _create_plugin, generate_plugin_name
from climatoology.base.event import ComputationState


def test_plugin_creation(default_operator, default_settings, mocked_object_store, default_backend_db):
    with patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)
        assert isinstance(plugin, Celery)


def test_plugin_register_task(default_operator, default_settings, mocked_object_store, default_backend_db):
    with patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)

        assert plugin.tasks.unregister('compute') is None


def test_worker_send_compute_task(
    default_plugin, default_computation_info, general_uuid, default_aoi_feature_pure_dict, backend_with_computation
):
    expected_computation_info = default_computation_info.model_copy(deep=True).model_dump(mode='json')

    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {'id': 1, 'name': 'John Doe'},
    }
    with (
        patch('uuid.uuid4', return_value=general_uuid),
        patch('climatoology.app.tasks.datetime', wraps=datetime.datetime) as dt_mock,
    ):
        dt_mock.now.return_value = datetime.datetime(day=1, month=1, year=2025)
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
):
    save_info_spy = mocker.spy(default_plugin.tasks['compute'], '_save_computation_info')

    with (
        patch('uuid.uuid4', return_value=general_uuid),
        patch('climatoology.app.tasks.datetime', wraps=datetime.datetime) as dt_mock,
    ):
        dt_mock.now.return_value = datetime.datetime(day=1, month=1, year=2025)
        kwargs = {
            'aoi': default_aoi_feature_pure_dict,
            'params': {'id': 1, 'name': 'John Doe'},
        }
        _ = default_plugin.send_task(
            'compute',
            kwargs=kwargs,
            task_id=str(general_uuid),
        ).get(timeout=5)

    save_info_spy.assert_called_once_with(computation_info=default_computation_info)


def test_successful_compute_saves_metadata_to_backend(
    default_plugin, general_uuid, default_aoi_feature_pure_dict, backend_with_computation, default_computation_info
):
    with (
        patch('uuid.uuid4', return_value=general_uuid),
        patch('climatoology.app.tasks.datetime', wraps=datetime.datetime) as dt_mock,
    ):
        dt_mock.now.return_value = datetime.datetime(day=1, month=1, year=2025)
        kwargs = {
            'aoi': default_aoi_feature_pure_dict,
            'params': {'id': 1, 'name': 'John Doe'},
        }
        _ = default_plugin.send_task(
            'compute',
            kwargs=kwargs,
            task_id=str(general_uuid),
        ).get(timeout=5)

    saved_computation = backend_with_computation.read_computation(correlation_uuid=general_uuid)
    assert saved_computation == default_computation_info


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
    assert updated_computation.status == ComputationState.FAILURE
    assert updated_computation.message == 'ID: Field required. You provided: {}.'


def test_generate_plugin_name():
    computed_name = generate_plugin_name('plugin_id')
    assert computed_name == 'plugin_id@_'
