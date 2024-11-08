from unittest.mock import patch

from celery import Celery

from climatoology.app.plugin import _create_plugin, generate_plugin_name


def test_plugin_creation(default_operator, default_settings, mocked_object_store):
    plugin = _create_plugin(operator=default_operator, settings=default_settings)
    assert isinstance(plugin, Celery)


def test_plugin_register_task(default_operator, default_settings, mocked_object_store):
    plugin = _create_plugin(operator=default_operator, settings=default_settings)

    assert plugin.tasks.unregister('compute') is None
    assert plugin.tasks.unregister('info') is None


def test_worker_send_info_task(
    default_operator, celery_app, celery_worker, default_info, mocked_object_store, default_settings
):
    with patch('climatoology.app.plugin.Celery', return_value=celery_app):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)

        celery_worker.reload()

        expected_info_result = default_info.model_dump(mode='json')
        computed_info_result = plugin.send_task('info').get(timeout=5)

        assert computed_info_result == expected_info_result


def test_worker_send_compute_task(default_plugin, default_artifact, general_uuid):
    expected_compute_result = [default_artifact.model_dump(mode='json')]
    with patch('uuid.uuid4', return_value=general_uuid):
        computed_compute_result = default_plugin.send_task(
            'compute',
            kwargs={'params': {'id': 1, 'name': 'John Dow'}},
            task_id=str(general_uuid),
        ).get(timeout=5)

    assert computed_compute_result == expected_compute_result


def test_generate_plugin_name():
    computed_name = generate_plugin_name('plugin_id')
    assert computed_name == 'plugin_id@_'
