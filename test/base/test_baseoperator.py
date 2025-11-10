import logging
import re
import uuid
from datetime import date
from pathlib import Path
from typing import List
from unittest.mock import Mock, patch

import pytest
import shapely
from pydantic import BaseModel, Field, model_validator
from semver import Version
from shapely import get_srid

from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.base.baseoperator import AoiProperties, BaseOperator
from climatoology.base.computation import ComputationResources, ComputationScope
from climatoology.base.info import _Info
from climatoology.utility.exception import ClimatoologyUserError, InputValidationError
from test.conftest import TestModel


def test_default_aoi_init(default_aoi_geom_shapely):
    assert get_srid(default_aoi_geom_shapely) == 4326


def test_default_aoi_properties_init(default_aoi_properties):
    assert isinstance(default_aoi_properties, AoiProperties)


def test_operator_scope():
    correlation_uuid = uuid.uuid4()
    with ComputationScope(correlation_uuid) as resources:
        assert resources.correlation_uuid == correlation_uuid
        assert resources.computation_dir.exists()

    assert not resources.computation_dir.exists()


def test_operator_info_enrichment_does_not_change_given_input(default_info):
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

    operator = TestOperator()

    expected_info = default_info.model_dump(exclude={'library_version', 'operator_schema'})
    computed_info = operator.info_enriched.model_dump(exclude={'library_version', 'operator_schema'})

    assert computed_info == expected_info


@patch('climatoology.__version__', Version(1, 0, 0))
def test_operator_info_enrichment_does_overwrite_additional_parts(default_info):
    class MinimalTestModel(BaseModel):
        test: str

    class TestOperator(BaseOperator[MinimalTestModel]):
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

    operator = TestOperator()

    computed_info = operator.info_enriched

    assert computed_info.library_version == Version(1, 0, 0)
    assert computed_info.operator_schema == {
        'properties': {'test': {'title': 'Test', 'type': 'string'}},
        'required': ['test'],
        'title': 'MinimalTestModel',
        'type': 'object',
    }


def test_operator_startup_checks_for_aoi_fields(default_info):
    class TestModelAOI(BaseModel):
        aoi: str
        aoi_properties: str

    class TestOperator(BaseOperator[TestModelAOI]):
        def info(self) -> _Info:
            return default_info.model_copy()

        def compute(
            self,
            resources: ComputationResources,
            aoi: shapely.MultiPolygon,
            aoi_properties: AoiProperties,
            params: TestModelAOI,
        ) -> List[_Artifact]:
            return []

    with pytest.raises(
        AssertionError,
        match=re.escape("The plugin input parameters cannot contain fields named any of ('aoi', 'aoi_properties')"),
    ):
        TestOperator()


def test_operator_validate_params_valid(default_operator):
    expected_params = default_operator._model(id=1234, name='test')
    computed_params = default_operator.validate_params(params={'id': 1234, 'name': 'test'})
    assert computed_params == expected_params


def test_operator_validate_params_invalid_exception(default_operator):
    expected_error = re.escape(
        'ID: Input should be a valid integer, unable to parse string as an integer. You provided: s1234.'
    )

    with pytest.raises(InputValidationError, match=expected_error):
        default_operator.validate_params(params={'id': 's1234', 'name': 'test'})


def test_operator_validate_params_invalid_exception_multiple(default_operator):
    expected_error = re.escape(
        'ID: Input should be a valid integer, unable to parse string as an integer. You provided: s1234.\n'
        'Name: Input should be a valid string. You provided: 1.0.'
    )

    with pytest.raises(InputValidationError, match=expected_error):
        default_operator.validate_params(params={'id': 's1234', 'name': 1.0})


def test_operator_validate_params_missing_exception(default_operator):
    expected_error = re.escape('ID: Field required. You provided: {}.')

    with pytest.raises(InputValidationError, match=expected_error):
        default_operator.validate_params(params={})


def test_operator_validate_params_invalid_custom_validation_exception(default_info, default_artifact):
    class TestModelValidateParams(BaseModel):
        first_date: date = Field(title='Period Start')
        last_date: date = Field(title='Period End')

        @model_validator(mode='after')
        def check_order(self):
            if not self.last_date > self.first_date:
                raise ValueError('Period start must be before period end')
            return self

    class TestOperatorValidateParams(BaseOperator[TestModelValidateParams]):
        def info(self) -> _Info:
            return default_info.model_copy()

        def compute(
            self,
            resources: ComputationResources,
            aoi: shapely.MultiPolygon,
            aoi_properties: AoiProperties,
            params: TestModel,
        ) -> List[_Artifact]:
            return [default_artifact]

    operator = TestOperatorValidateParams()
    expected_error = re.escape(
        "Value error, Period start must be before period end. You provided: {'Period Start': "
        "datetime.date(2017, 2, 15), 'Period End': datetime.date(2017, 1, 15)}."
    )

    with pytest.raises(InputValidationError, match=expected_error):
        operator.validate_params(params={'first_date': date(2017, 2, 15), 'last_date': date(2017, 1, 15)})


def test_operator_compute_unsafe_must_return_results(
    default_operator,
    default_input_model,
    default_aoi_geom_shapely,
    default_aoi_properties,
    default_computation_resources,
):
    compute_mock = Mock(return_value=[])
    default_operator.compute = compute_mock

    with pytest.raises(AssertionError, match='The computation returned no results'):
        default_operator.compute_unsafe(
            aoi=default_aoi_geom_shapely,
            aoi_properties=default_aoi_properties,
            params=default_input_model,
            resources=default_computation_resources,
        )


def test_operator_compute_unsafe_results_no_computation_info(
    general_uuid,
    default_operator,
    default_input_model,
    default_aoi_geom_shapely,
    default_aoi_properties,
    default_computation_resources,
):
    computation_info_artifact = _Artifact(
        name='Computation Info',
        modality=ArtifactModality.COMPUTATION_INFO,
        filename='metadata.json',
        summary='Computation information of correlation_uuid {general_uuid}',
    )

    compute_mock = Mock(return_value=[computation_info_artifact])
    default_operator.compute = compute_mock

    with pytest.raises(AssertionError, match='Computation-info files are not allowed as plugin result'):
        default_operator.compute_unsafe(
            aoi=default_aoi_geom_shapely,
            aoi_properties=default_aoi_properties,
            params=default_input_model,
            resources=default_computation_resources,
        )


def test_operator_create_artifact_safely_with_only_good_artifact(
    default_info,
    default_artifact,
    default_artifact_enriched,
    default_aoi_geom_shapely,
    default_aoi_properties,
    general_uuid,
):
    def good_fn():
        return default_artifact

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
            artifacts = []
            with self.catch_exceptions(indicator_name='test_indicator', resources=resources):
                artifacts.append(good_fn())
            return artifacts

    operator = TestOperator()
    input_resources = ComputationResources(correlation_uuid=general_uuid, computation_dir=Path())
    computed_artifacts = operator.compute_unsafe(
        resources=input_resources,
        aoi=default_aoi_geom_shapely,
        aoi_properties=default_aoi_properties,
        params=TestModel(id=1),
    )
    assert computed_artifacts == [default_artifact_enriched]
    assert input_resources.artifact_errors == {}


def test_operator_create_artifact_safely_with_only_bad_artifact(
    default_info,
    default_aoi_geom_shapely,
    default_aoi_properties,
):
    def bad_fn():
        raise ValueError('Artifact computation failed')

    def bad_fn_with_reason():
        raise ClimatoologyUserError('Error message to store for the user')

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
            artifacts = []
            with self.catch_exceptions(indicator_name='First Indicator', resources=resources):
                artifacts.append(bad_fn())

            with self.catch_exceptions(indicator_name='Second Indicator', resources=resources):
                artifacts.append(bad_fn_with_reason())

            return artifacts

    operator = TestOperator()
    input_resources = ComputationResources(correlation_uuid=uuid.uuid4(), computation_dir=Path())

    with pytest.raises(
        ClimatoologyUserError,
        match='Failed to create any indicators due to the following errors: '
        '{"First Indicator": "", "Second Indicator": "Error message to store for the user"}',
    ):
        operator.compute_unsafe(
            resources=input_resources,
            aoi=default_aoi_geom_shapely,
            aoi_properties=default_aoi_properties,
            params=TestModel(id=1),
        )


def test_operator_create_artifact_safely_with_good_and_bad_artifacts(
    default_info,
    default_artifact,
    default_artifact_enriched,
    default_aoi_geom_shapely,
    default_aoi_properties,
    caplog,
    general_uuid,
):
    def bad_fn():
        raise ValueError('Test artifact computation failed')

    def bad_fn_with_reason():
        raise ClimatoologyUserError('Error message to store for the user')

    def good_fn():
        return default_artifact

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
            artifacts = []

            with self.catch_exceptions(indicator_name='First Indicator', resources=resources):
                artifacts.append(bad_fn())

            with self.catch_exceptions(indicator_name='Second Indicator', resources=resources):
                artifacts.append(bad_fn_with_reason())

            with self.catch_exceptions(indicator_name='Good Indicator', resources=resources):
                artifacts.append(good_fn())

            return artifacts

    expected_log_msgs = [
        f'First Indicator computation failed for correlation id {general_uuid}',
        f'Second Indicator computation failed for correlation_id {general_uuid}',
    ]

    operator = TestOperator()
    input_resources = ComputationResources(correlation_uuid=general_uuid, computation_dir=Path())
    with caplog.at_level(logging.ERROR):
        computed_artifacts = operator.compute_unsafe(
            resources=input_resources,
            aoi=default_aoi_geom_shapely,
            aoi_properties=default_aoi_properties,
            params=TestModel(id=1),
        )

    assert computed_artifacts == [default_artifact_enriched]
    assert input_resources.artifact_errors == {
        'First Indicator': '',
        'Second Indicator': 'Error message to store for the user',
    }
    assert [m in caplog.messages for m in expected_log_msgs]
