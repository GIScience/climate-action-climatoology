import uuid
from typing import List
from unittest.mock import patch

import pytest
from pydantic import BaseModel
from semver import Version

from climatoology.base.artifact import _Artifact
from climatoology.base.computation import ComputationScope, ComputationResources
from climatoology.base.info import _Info
from climatoology.base.operator import Operator
from climatoology.utility.exception import InputValidationError


def test_operator_typing(default_operator, default_computation_resources):
    default_operator.compute_unsafe(default_computation_resources, {'id': 1234, 'name': 'test'})

    with pytest.raises(InputValidationError):
        default_operator.compute_unsafe(default_computation_resources, {'id': 'ID:1234', 'name': 'test'})


def test_operator_scope():
    correlation_uuid = uuid.uuid4()
    with ComputationScope(correlation_uuid) as resources:
        assert resources.correlation_uuid == correlation_uuid
        assert resources.computation_dir.exists()

    assert not resources.computation_dir.exists()


def test_operator_info_enrichment_does_not_change_given_input(default_info):
    class TestModel(BaseModel):
        pass

    class TestOperator(Operator[TestModel]):
        def info(self) -> _Info:
            return default_info.model_copy()

        def compute(self, resources: ComputationResources, params: TestModel) -> List[_Artifact]:
            return []

    operator = TestOperator()

    expected_info = default_info.model_dump(exclude={'library_version', 'operator_schema'})
    computed_info = operator.info_enriched.model_dump(exclude={'library_version', 'operator_schema'})

    assert computed_info == expected_info


@patch('climatoology.__version__', Version(1, 0, 0))
def test_operator_info_enrichment_does_overwrite_additional_parts(default_info):
    class TestModel(BaseModel):
        test: str

    class TestOperator(Operator[TestModel]):
        def info(self) -> _Info:
            return default_info.model_copy()

        def compute(self, resources: ComputationResources, params: TestModel) -> List[_Artifact]:
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


def test_operator_compute_missing_input_validation(default_operator, default_computation_resources):
    with pytest.raises(InputValidationError, match='The given user input is invalid'):
        default_operator.compute_unsafe(resources=default_computation_resources, params={})
