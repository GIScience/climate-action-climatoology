from unittest.mock import patch, Mock, ANY

from shapely import get_srid


def test_computation_task_init(default_computation_task):
    assert default_computation_task


def test_computation_task_run(
    default_computation_task, default_artifact, general_uuid, default_aoi_geojson, default_aoi_properties
):
    with patch('uuid.uuid4', return_value=general_uuid):
        computed_result = default_computation_task.run(
            aoi=default_aoi_geojson,
            aoi_properties=default_aoi_properties.model_dump(mode='json'),
            params={'id': 1, 'name': 'test'},
        )
    expected_result = [default_artifact.model_dump(mode='json')]

    assert computed_result == expected_result


def test_computation_task_run_forward_input(
    default_computation_task, default_artifact, general_uuid, default_aoi_geojson, default_aoi_properties, default_aoi
):
    compute_unsafe_mock = Mock(side_effect=default_computation_task.operator.compute_unsafe)
    default_computation_task.operator.compute_unsafe = compute_unsafe_mock

    method_input_params = {'id': 1, 'name': 'test'}

    with patch('uuid.uuid4', return_value=general_uuid):
        computed_result = default_computation_task.run(
            aoi=default_aoi_geojson,
            aoi_properties=default_aoi_properties.model_dump(mode='json'),
            params=method_input_params,
        )

    expected_result = [default_artifact.model_dump(mode='json')]

    compute_unsafe_mock.assert_called_once_with(
        resources=ANY, aoi=default_aoi, aoi_properties=default_aoi_properties, params=method_input_params
    )
    assert get_srid(compute_unsafe_mock.mock_calls[0].kwargs.get('aoi')) == 4326
    assert computed_result == expected_result


def test_info_task_init(default_info_task):
    assert default_info_task


def test_info_task_run(default_info_task, default_info, general_uuid):
    computed_result = default_info_task.run()
    expected_result = default_info.model_dump(mode='json')
    assert computed_result == expected_result
