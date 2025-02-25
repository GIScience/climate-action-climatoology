import re
import uuid
from typing import List
from unittest.mock import Mock, patch

import pytest
import shapely
from pydantic import BaseModel
from semver import Version
from shapely import get_srid

from climatoology.base.artifact import _Artifact, ArtifactModality
from climatoology.base.baseoperator import BaseOperator, AoiProperties
from climatoology.base.computation import ComputationScope, ComputationResources
from climatoology.base.info import _Info
from climatoology.utility.exception import InputValidationError


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

    operator = TestOperator()

    expected_info = default_info.model_dump(exclude={'library_version', 'operator_schema'})
    computed_info = operator.info_enriched.model_dump(exclude={'library_version', 'operator_schema'})

    assert computed_info == expected_info


@patch('climatoology.__version__', Version(1, 0, 0))
def test_operator_info_enrichment_does_overwrite_additional_parts(default_info):
    class TestModel(BaseModel):
        test: str

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

    computed_info = operator.info_enriched

    assert computed_info.library_version == Version(1, 0, 0)
    assert computed_info.operator_schema == {
        'properties': {'test': {'title': 'Test', 'type': 'string'}},
        'required': ['test'],
        'title': 'TestModel',
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


def test_operator_validate_params(default_operator):
    # Valid
    default_operator.validate_params(params={'id': 1234, 'name': 'test'})

    # Invalid
    with pytest.raises(InputValidationError, match='The given user input is invalid'):
        default_operator.validate_params(params={'id': 'ID:1234', 'name': 'test'})

    # Missing
    with pytest.raises(InputValidationError, match='The given user input is invalid'):
        default_operator.validate_params(params={})


def test_operator_compute_unsafe_must_return_results(
    default_operator, default_aoi_geom_shapely, default_aoi_properties, default_computation_resources
):
    compute_mock = Mock(return_value=[])
    default_operator.compute = compute_mock

    with pytest.raises(AssertionError, match='The computation returned no results.'):
        default_operator.compute_unsafe(
            aoi=default_aoi_geom_shapely,
            aoi_properties=default_aoi_properties,
            params=dict(),
            resources=default_computation_resources,
        )


def test_operator_compute_unsafe_results_no_computation_info(
    general_uuid,
    default_operator,
    default_aoi_geom_shapely,
    default_aoi_properties,
    default_computation_resources,
    default_info,
):
    computation_info_artifact = _Artifact(
        name='Computation Info',
        modality=ArtifactModality.COMPUTATION_INFO,
        file_path='metadata.json',
        summary='Computation information of correlation_uuid {general_uuid}',
        correlation_uuid=general_uuid,
    )

    compute_mock = Mock(return_value=[computation_info_artifact])
    default_operator.compute = compute_mock

    with pytest.raises(AssertionError, match='Computation-info files are not allowed as plugin result'):
        default_operator.compute_unsafe(
            aoi=default_aoi_geom_shapely,
            aoi_properties=default_aoi_properties,
            params={'id': 1},
            resources=default_computation_resources,
        )
