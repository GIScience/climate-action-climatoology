import datetime
import tempfile
from pathlib import Path
from typing import List
from unittest.mock import ANY, Mock, patch

from pydantic import BaseModel
import pytest
from shapely import get_srid
import shapely

from climatoology.app.tasks import CAPlatformComputeTask, CAPlatformInfoTask, ComputationInfo, PluginBaseInfo
from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.base.baseoperator import AoiProperties, BaseOperator
from climatoology.base.computation import ComputationResources
from climatoology.base.event import ComputationState
from climatoology.base.info import _Info


def test_computation_task_init(default_computation_task):
    assert default_computation_task


def test_computation_task_run(
    default_computation_task,
    default_artifact,
    general_uuid,
    default_aoi_feature_pure_dict,
):
    with patch('uuid.uuid4', return_value=general_uuid):
        computed_result = default_computation_task.run(
            aoi=default_aoi_feature_pure_dict,
            params={'id': 1, 'name': 'test'},
        )
    expected_result = [default_artifact.model_dump(mode='json')]

    assert computed_result == expected_result
    default_computation_task.update_state.assert_called_once_with(task_id=general_uuid, state='STARTED')


def test_computation_task_run_must_return_results(
    default_info, default_aoi_geom_shapely, default_aoi_properties, default_computation_resources
):
    class TestModel(BaseModel):
        pass

    class TestOperator(BaseOperator[TestModel]):
        def info(self) -> _Info:
            return default_info.model_copy()

        def compute(
            self,
            resources: ComputationResources,
            aoi: shapely.MultiPolygon,
            aoi_properties: AoiProperties,
            params: TestModel,
        ) -> List[_Artifact]:
            return []

    with pytest.raises(AssertionError, match='The computation returned no results'):
        operator = TestOperator()
        operator.compute_unsafe(
            aoi=default_aoi_geom_shapely,
            aoi_properties=default_aoi_properties,
            params=dict(),
            resources=default_computation_resources,
        )


def test_computation_task_run_forward_input(
    default_computation_task,
    default_artifact,
    general_uuid,
    default_aoi_feature_pure_dict,
    default_aoi_properties,
    default_aoi_geom_shapely,
):
    compute_unsafe_mock = Mock(side_effect=default_computation_task.operator.compute_unsafe)
    default_computation_task.operator.compute_unsafe = compute_unsafe_mock

    method_input_params = {'id': 1, 'name': 'test'}
    method_input_params_obj = default_computation_task.operator._model(**method_input_params)

    with patch('uuid.uuid4', return_value=general_uuid):
        computed_result = default_computation_task.run(
            aoi=default_aoi_feature_pure_dict,
            params=method_input_params,
        )

    expected_result = [default_artifact.model_dump(mode='json')]

    compute_unsafe_mock.assert_called_once_with(
        resources=ANY,
        aoi=default_aoi_geom_shapely,
        aoi_properties=default_aoi_properties,
        params=method_input_params_obj,
    )
    assert get_srid(compute_unsafe_mock.mock_calls[0].kwargs.get('aoi')) == 4326
    assert computed_result == expected_result


def test_computation_task_run_input_validated(
    default_computation_task, default_aoi_feature_pure_dict, default_aoi_geom_shapely, default_aoi_properties
):
    compute_unsafe_mock = Mock(side_effect=default_computation_task.operator.compute_unsafe)
    default_computation_task.operator.compute_unsafe = compute_unsafe_mock

    method_input_params = {'id': 99}

    default_computation_task.run(
        aoi=default_aoi_feature_pure_dict,
        params=method_input_params,
    )

    compute_unsafe_mock.assert_called_once_with(
        resources=ANY,
        aoi=default_aoi_geom_shapely,
        aoi_properties=default_aoi_properties,
        params=default_computation_task.operator._model(id=99, name='John Doe'),
    )


def test_computation_task_run_saves_metadata_with_full_params(
    default_computation_task,
    general_uuid,
    default_aoi_feature_geojson_pydantic,
    default_aoi_feature_pure_dict,
    default_artifact,
):
    expected_computation_info = ComputationInfo(
        correlation_uuid=general_uuid,
        timestamp=datetime.datetime(day=1, month=1, year=2021),
        params={'id': 1, 'name': 'John Doe'},
        aoi=default_aoi_feature_geojson_pydantic,
        artifacts=[default_artifact],
        plugin_info=PluginBaseInfo(plugin_id='test_plugin', plugin_version='3.1.0'),
        status=ComputationState.SUCCESS,
    )

    save_metadata_mock = Mock(side_effect=default_computation_task._save_computation_info)
    default_computation_task._save_computation_info = save_metadata_mock

    method_input_params = {'id': 1}

    with (
        patch('uuid.uuid4', return_value=general_uuid),
        patch('climatoology.app.tasks.datetime.datetime', wraps=datetime.datetime) as dt_mock,
    ):
        dt_mock.now.return_value = datetime.datetime(day=1, month=1, year=2021)

        _ = default_computation_task.run(
            aoi=default_aoi_feature_pure_dict,
            params=method_input_params,
        )

        save_metadata_mock.assert_called_once_with(computation_info=expected_computation_info)


def test_info_task_init(default_info_task):
    assert default_info_task


def test_info_task_run(default_info_task, default_info_final, general_uuid):
    computed_result = default_info_task.run()
    expected_result = default_info_final.model_dump(mode='json')
    assert computed_result == expected_result


def test_info_task_uploads_assets(default_operator, mocked_object_store):
    storage = mocked_object_store['minio_storage']
    synch_assets_mock = Mock(side_effect=storage.synch_assets)
    storage.synch_assets = synch_assets_mock

    _ = CAPlatformInfoTask(operator=default_operator, storage=storage, overwrite_assets=False)

    synch_assets_mock.assert_called_once_with(
        plugin_id='test_plugin',
        plugin_version='3.1.0',
        assets=ANY,
        overwrite=False,
    )


def test_save_computation_info(
    default_operator, mocked_object_store, general_uuid, default_aoi_feature_geojson_pydantic
):
    task = CAPlatformComputeTask(operator=default_operator, storage=mocked_object_store['minio_storage'])
    info = ComputationInfo(
        correlation_uuid=general_uuid,
        timestamp=datetime.datetime(day=1, month=1, year=2021),
        params={},
        aoi=default_aoi_feature_geojson_pydantic,
        artifacts=[],
        plugin_info=PluginBaseInfo(plugin_id='test_plugin', plugin_version='0.0.1'),
        status=ComputationState.SUCCESS,
    )

    save_mock = Mock()
    mocked_object_store['minio_storage'].save = save_mock

    with tempfile.TemporaryDirectory() as temp_dir:
        expected_save_artifact = _Artifact(
            name='Computation Info',
            modality=ArtifactModality.COMPUTATION_INFO,
            file_path=Path(f'{temp_dir}/metadata.json'),
            summary=f'Computation information of correlation_uuid {general_uuid}',
            correlation_uuid=general_uuid,
        )
        with patch('climatoology.app.tasks.tempfile.TemporaryDirectory.__enter__', return_value=temp_dir):
            task._save_computation_info(computation_info=info)

            save_mock.assert_called_once_with(expected_save_artifact)

            with open(expected_save_artifact.file_path, 'r') as metadata_file:
                assert metadata_file.read() == info.model_dump_json()
