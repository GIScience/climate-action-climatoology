import sys
import tempfile
from pathlib import Path
from typing import List
from unittest.mock import ANY, Mock, patch

import pytest
import shapely
from pydantic import BaseModel
from shapely import get_srid

from climatoology.app.tasks import CAPlatformComputeTask
from climatoology.base.artifact import Artifact, ArtifactEnriched, ArtifactModality
from climatoology.base.baseoperator import BaseOperator
from climatoology.base.computation import AoiProperties, ComputationResources
from climatoology.base.plugin_info import PluginInfo


def test_computation_task_init(default_computation_task):
    assert default_computation_task


def test_computation_task_run(
    default_computation_task,
    default_computation_info,
    general_uuid,
    default_aoi_feature_pure_dict,
    backend_with_computation_registered,
    frozen_time,
):
    expected_computation_info = default_computation_info.model_copy(deep=True).model_dump(mode='json')

    computed_result = default_computation_task.run(
        aoi=default_aoi_feature_pure_dict,
        params={'id': 1},
    )

    assert computed_result == expected_computation_info
    default_computation_task.update_state.assert_called_once_with(task_id=str(general_uuid), state='STARTED')


def test_computation_task_run_must_return_results(
    default_plugin_info, default_aoi_geom_shapely, default_aoi_properties, default_computation_resources
):
    class TestModel(BaseModel):
        pass

    class TestOperator(BaseOperator[TestModel]):
        def info(self) -> PluginInfo:
            return default_plugin_info.model_copy()

        def compute(
            self,
            resources: ComputationResources,
            aoi: shapely.MultiPolygon,
            aoi_properties: AoiProperties,
            params: TestModel,
        ) -> List[Artifact]:
            return []

    with pytest.raises(AssertionError, match='The computation returned no results'):
        operator = TestOperator()
        operator.compute_unsafe(
            aoi=default_aoi_geom_shapely,
            aoi_properties=default_aoi_properties,
            params=TestModel(),
            resources=default_computation_resources,
        )


def test_computation_task_run_forward_input(
    default_computation_task,
    default_computation_info,
    general_uuid,
    default_aoi_feature_pure_dict,
    default_aoi_properties,
    default_aoi_geom_shapely,
    backend_with_computation_registered,
    frozen_time,
):
    expected_computation_info = default_computation_info.model_copy(deep=True).model_dump(mode='json')

    compute_unsafe_mock = Mock(side_effect=default_computation_task.operator.compute_unsafe)
    default_computation_task.operator.compute_unsafe = compute_unsafe_mock

    method_input_params = {'id': 1}
    method_input_params_obj = default_computation_task.operator._model(**method_input_params)

    computed_result = default_computation_task.run(
        aoi=default_aoi_feature_pure_dict,
        params=method_input_params,
    )

    compute_unsafe_mock.assert_called_once_with(
        resources=ANY,
        aoi=default_aoi_geom_shapely,
        aoi_properties=default_aoi_properties,
        params=method_input_params_obj,
    )
    assert get_srid(compute_unsafe_mock.mock_calls[0].kwargs.get('aoi')) == 4326
    assert computed_result == expected_computation_info


def test_computation_task_run_with_extra_kwarg(
    default_computation_task,
    default_computation_info,
    default_aoi_feature_pure_dict,
    backend_with_computation_registered,
    frozen_time,
):
    """This test asserts that our plugins are ready to accept future optional arguments.
    This will reduce the number of breaking changes we will have to make in climatoology.
    """
    expected_computation_info = default_computation_info.model_copy(deep=True).model_dump(mode='json')

    computed_result = default_computation_task.run(
        aoi=default_aoi_feature_pure_dict, params={'id': 1}, future_new_param=True
    )

    assert computed_result == expected_computation_info


def test_computation_task_run_input_validated(
    default_computation_task,
    default_aoi_feature_pure_dict,
    default_aoi_geom_shapely,
    default_aoi_properties,
    backend_with_computation_registered,
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


def test_save_computation_info(
    default_operator, mocked_object_store, general_uuid, default_backend_db, default_computation_info, mocker
):
    task = CAPlatformComputeTask(operator=default_operator, storage=mocked_object_store, backend_db=default_backend_db)

    save_spy = mocker.spy(mocked_object_store, 'save')

    with tempfile.TemporaryDirectory() as temp_dir:
        expected_save_artifact = ArtifactEnriched(
            name='Computation Info',
            rank=sys.maxsize,
            modality=ArtifactModality.COMPUTATION_INFO,
            filename='metadata.json',
            summary=f'Computation information of correlation_uuid {general_uuid}',
            correlation_uuid=general_uuid,
        )
        with patch('climatoology.app.tasks.tempfile.TemporaryDirectory.__enter__', return_value=temp_dir):
            task._save_computation_info(computation_info=default_computation_info)

            save_spy.assert_called_once_with(expected_save_artifact, file_dir=Path(temp_dir))

            with open(Path(temp_dir) / expected_save_artifact.filename, 'r') as metadata_file:
                assert metadata_file.read() == default_computation_info.model_dump_json()
