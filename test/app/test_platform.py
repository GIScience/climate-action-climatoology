from unittest.mock import patch, ANY, Mock

import pytest
from celery.result import AsyncResult
from geojson_pydantic import Feature
from semver import Version

from climatoology.app.platform import CeleryPlatform
from climatoology.base.artifact import _Artifact
from climatoology.utility.exception import ClimatoologyVersionMismatchException


def test_list_default_active_plugins(default_platform_connection, celery_worker):
    computed_plugins = default_platform_connection.list_active_plugins()
    assert computed_plugins == set()


def test_list_no_active_plugins(default_platform_connection):
    computed_plugins = default_platform_connection.list_active_plugins()
    assert computed_plugins == set()


def test_list_active_plugins(default_platform_connection, celery_worker, default_plugin):
    computed_plugins = default_platform_connection.list_active_plugins()

    expected_plugins = {celery_worker.hostname.split('@')[0]}

    assert computed_plugins == expected_plugins


def test_request_info(default_platform_connection, default_info_final, default_plugin, celery_worker):
    computed_info = default_platform_connection.request_info(plugin_id='test_plugin')

    assert celery_worker.stats()['total'].get('info') == 1
    assert computed_info == default_info_final


@patch('climatoology.__version__', Version(1, 0, 0))
def test_request_info_plugin_version_assert(default_platform_connection, default_info, default_plugin, celery_worker):
    with pytest.raises(ClimatoologyVersionMismatchException, match='Refusing to register plugin.*'):
        default_platform_connection.request_info(plugin_id='test_plugin')


def test_send_compute(
    default_platform_connection, default_plugin, celery_worker, general_uuid, default_artifact, celery_app
):
    mocked_app = Mock(side_effect=celery_app)
    default_platform_connection.celery_app = mocked_app

    feature = Feature(
        **{
            'type': 'Feature',
            'properties': {'name': 'Heidelberg', 'id': 'Q12345'},
            'geometry': {
                'type': 'MultiPolygon',
                'coordinates': [
                    [
                        [
                            [12.3, 48.22],
                            [12.3, 48.34],
                            [12.48, 48.34],
                            [12.48, 48.22],
                            [12.3, 48.22],
                        ]
                    ]
                ],
            },
        }
    )
    _ = default_platform_connection.send_compute_request(
        plugin_id='test_plugin',
        aoi=feature,
        params={'id': 1, 'name': 'John Doe'},
        correlation_uuid=general_uuid,
    )

    mocked_app.send_task.assert_called_once_with(
        name='compute',
        kwargs={
            'aoi': {
                'type': 'MultiPolygon',
                'coordinates': [[[[12.3, 48.22], [12.3, 48.34], [12.48, 48.34], [12.48, 48.22], [12.3, 48.22]]]],
            },
            'aoi_properties': {'name': 'Heidelberg', 'id': 'Q12345'},
            'params': {'id': 1, 'name': 'John Doe'},
        },
        task_id=str(general_uuid),
        routing_key='test_plugin@_',
        exchange='C.dq2',
    )


def test_send_compute_produces_result(
    default_platform_connection, default_plugin, celery_worker, general_uuid, default_artifact, celery_app
):
    feature = Feature(
        **{
            'type': 'Feature',
            'properties': {'name': 'Heidelberg', 'id': 'Q12345'},
            'geometry': {
                'type': 'MultiPolygon',
                'coordinates': [
                    [
                        [
                            [12.3, 48.22],
                            [12.3, 48.34],
                            [12.48, 48.34],
                            [12.48, 48.22],
                            [12.3, 48.22],
                        ]
                    ]
                ],
            },
        }
    )
    result = default_platform_connection.send_compute_request(
        plugin_id='test_plugin',
        aoi=feature,
        params={'id': 1, 'name': 'John Doe'},
        correlation_uuid=general_uuid,
    )

    assert isinstance(result, AsyncResult)
    artifacts = result.get(timeout=5)

    default_artifact.store_id = ANY
    artifacts = [_Artifact.model_validate(artifact) for artifact in artifacts]
    assert artifacts == [default_artifact]


def test_send_compute_reaches_worker(
    default_platform_connection, default_plugin, celery_worker, general_uuid, default_artifact, celery_app
):
    previous_computations = celery_worker.stats()['total'].get('compute')
    feature = Feature(
        **{
            'type': 'Feature',
            'properties': {'name': 'Heidelberg', 'id': 'Q12345'},
            'geometry': {
                'type': 'MultiPolygon',
                'coordinates': [
                    [
                        [
                            [12.3, 48.22],
                            [12.3, 48.34],
                            [12.48, 48.34],
                            [12.48, 48.22],
                            [12.3, 48.22],
                        ]
                    ]
                ],
            },
        }
    )
    result = default_platform_connection.send_compute_request(
        plugin_id='test_plugin',
        aoi=feature,
        params={'id': 1, 'name': 'John Doe'},
        correlation_uuid=general_uuid,
    )

    _ = result.get(timeout=5)
    assert celery_worker.stats()['total'].get('compute') == (previous_computations + 1)


def test_extract_plugin_id():
    computed_plugin_id = CeleryPlatform._extract_plugin_id('a@b')
    assert computed_plugin_id == 'a'
