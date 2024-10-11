import uuid
from pathlib import Path
from typing import List

import pytest
import responses
from pydantic import BaseModel
from semver import Version

from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.base.computation import ComputationResources, ComputationScope
from climatoology.base.info import Concern, PluginAuthor, _Info, generate_plugin_info
from climatoology.base.operator import Operator
from climatoology.utility.api import HealthCheck


@pytest.fixture
def general_uuid():
    return uuid.uuid4()


@pytest.fixture
def default_info() -> _Info:
    info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website='https://heigit.org/heigit-team/',
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent / 'resources/test_methodology.md',
        sources=Path(__file__).parent / 'resources/test.bib',
    )
    info.library_version = '1.0.0'
    info.operator_schema = {
        'properties': {
            'bool': {
                'description': 'A required boolean parameter.',
                'examples': [True],
                'title': 'Boolean Input',
                'type': 'boolean',
            },
            'required': [
                'bool',
            ],
            'title': 'ComputeInput',
            'type': 'object',
        }
    }
    return info


@pytest.fixture
def default_artifact(general_uuid):
    return _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOJSON,
        file_path=Path(__file__).parent / 'test_file.tiff',
        summary='Test summary',
        description='Test description',
        correlation_uuid=general_uuid,
        store_id=f'{general_uuid}_test_file.tiff',
    )


@pytest.fixture
def default_operator(default_info):
    class TestModel(BaseModel):
        id: int
        name: str

    class TestOperator(Operator[TestModel]):
        def info(self) -> _Info:
            return default_info.model_copy()

        def compute(self, resources: ComputationResources, params: TestModel) -> List[_Artifact]:
            pass

    yield TestOperator()


@pytest.fixture
def default_computation_resources(general_uuid) -> ComputationResources:
    with ComputationScope(general_uuid) as resources:
        yield resources


@pytest.fixture
def mocked_client():
    with responses.RequestsMock() as rsps:
        rsps.get('http://localhost:80/health', json=HealthCheck().model_dump())
        yield rsps
