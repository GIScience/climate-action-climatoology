from unittest.mock import patch

from celery import Celery

from climatoology.app.plugin import _create_plugin, generate_plugin_name


def test_plugin_creation(default_operator, default_settings, mocked_object_store, default_backend_db):
    with patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)
        assert isinstance(plugin, Celery)


def test_plugin_register_task(default_operator, default_settings, mocked_object_store, default_backend_db):
    with patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)

        assert plugin.tasks.unregister('compute') is None


def test_worker_send_compute_task(default_plugin, default_artifact, general_uuid, default_aoi_feature_pure_dict):
    expected_compute_result = [default_artifact.model_dump(mode='json')]
    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {'id': 1, 'name': 'John Dow'},
    }
    with patch('uuid.uuid4', return_value=general_uuid):
        computed_compute_result = default_plugin.send_task(
            'compute',
            kwargs=kwargs,
            task_id=str(general_uuid),
        ).get(timeout=5)

    assert computed_compute_result == expected_compute_result


def test_generate_plugin_name():
    computed_name = generate_plugin_name('plugin_id')
    assert computed_name == 'plugin_id@_'
