import uuid
from pathlib import Path

import pytest
from semver import Version

from climatoology.base.operator import Info, Concern, Artifact, ArtifactModality


@pytest.fixture
def general_uuid():
    return uuid.uuid4()


@pytest.fixture
def default_info() -> Info:
    return Info(
        name='test_plugin',
        icon=Path('resources/test_icon.jpeg'),
        version=Version.parse('3.1.0'),
        concerns=[Concern.GHG_EMISSION],
        purpose='The purpose of this base is to '
                'present basic library properties in '
                'terms of enforcing similar capabilities '
                'between Climate Action event components',
        methodology='This is a test base',
        sources=Path('resources/test.bib')
    )


@pytest.fixture
def default_artifact(general_uuid):
    return Artifact(name='test_name',
                    modality=ArtifactModality.MAP_LAYER,
                    file_path=Path('test_file.tiff'),
                    summary='Test summary',
                    description='Test description',
                    correlation_uuid=general_uuid,
                    params={'test param key': 'test param val'},
                    store_uuid=general_uuid)
