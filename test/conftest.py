import uuid
from pathlib import Path
from typing import List

import pytest
from pydantic import BaseModel
from semver import Version

from climatoology.base.artifact import ArtifactModality, Artifact
from climatoology.base.computation import ComputationResources, ComputationScope
from climatoology.base.operator import Info, Concern, Operator


@pytest.fixture
def general_uuid():
    return uuid.uuid4()


@pytest.fixture
def default_info() -> Info:
    return Info(
        name='Test Plugin',
        icon=Path(__file__).parent / 'resources/test_icon.jpeg',
        version=Version.parse('3.1.0'),
        concerns=[Concern.CLIMATE_ACTION__GHG_EMISSION],
        purpose='The purpose of this base is to '
                'present basic library properties in '
                'terms of enforcing similar capabilities '
                'between Climate Action event components',
        methodology='This is a test base',
        sources=Path(__file__).parent / 'resources/test.bib'
    )


@pytest.fixture
def default_artifact(general_uuid):
    return Artifact(name='test_name',
                    modality=ArtifactModality.MAP_LAYER_GEOJSON,
                    file_path=Path(__file__).parent / 'test_file.tiff',
                    summary='Test summary',
                    description='Test description',
                    correlation_uuid=general_uuid,
                    store_id=f'{general_uuid}_test_file.tiff')


@pytest.fixture
def default_operator():
    class TestModel(BaseModel):
        id: int
        name: str

    class TestOperator(Operator[TestModel]):

        def info(self) -> Info:
            pass

        def compute(self, resources: ComputationResources, params: TestModel) -> List[Artifact]:
            pass

    yield TestOperator()


@pytest.fixture
def default_computation_resources(general_uuid) -> ComputationResources:
    with ComputationScope(general_uuid) as resources:
        yield resources
