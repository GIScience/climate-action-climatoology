from enum import StrEnum

import pytest

from climatoology.base.artifact import (
    ARTIFACT_OVERWRITE_FIELDS,
    Artifact,
    ArtifactEnriched,
    ArtifactMetadata,
    ArtifactModality,
)
from climatoology.base.plugin_info import MiscSource


@pytest.fixture
def default_artifact_metadata() -> ArtifactMetadata:
    """Note: this should only provide required fields (except filename, which would make testing very cumbersome).
    This way it automatically is a test that optional fields are in fact optional.
    """
    return ArtifactMetadata(name='test_name', filename='test_artifact_file', summary='Test summary')


@pytest.fixture
def extensive_artifact_metadata() -> ArtifactMetadata:
    class ArtifactAssociation(StrEnum):
        TAG_A = 'Tag A'
        TAG_B = 'Tag B'

    association_tags = {ArtifactAssociation.TAG_A, ArtifactAssociation.TAG_B}

    return ArtifactMetadata(
        name='test_name',
        primary=False,
        tags=association_tags,
        filename='test_artifact_file',
        summary='Test summary',
        description='Test description',
        sources={'key1', 'key2'},
    )


@pytest.fixture
def default_artifact(default_artifact_metadata) -> Artifact:
    """Note: this should only provide required fields (except filename, which would make testing very cumbersome).
    This way it automatically is a test that optional fields are in fact optional.
    """
    return Artifact(
        **default_artifact_metadata.model_dump(exclude={'filename'}),
        modality=ArtifactModality.MARKDOWN,
        filename='test_artifact_file.md',
    )


@pytest.fixture
def extensive_artifact(extensive_artifact_metadata) -> Artifact:
    """Note: this should alter ALL fields (including optional ones,
    except rank and attachments, which would make testing very cumbersome).
    """
    return Artifact(
        **extensive_artifact_metadata.model_dump(exclude=ARTIFACT_OVERWRITE_FIELDS),
        modality=ArtifactModality.MARKDOWN,
        filename='test_artifact_file.md',
    )


@pytest.fixture
def default_artifact_enriched(default_artifact, general_uuid) -> ArtifactEnriched:
    return ArtifactEnriched(**default_artifact.model_dump(exclude={'sources'}), rank=0, correlation_uuid=general_uuid)


@pytest.fixture
def default_artifact_enriched_de(default_artifact, general_uuid_de) -> ArtifactEnriched:
    return ArtifactEnriched(
        **default_artifact.model_dump(exclude={'sources'}), rank=0, correlation_uuid=general_uuid_de
    )


@pytest.fixture
def extensive_artifact_enriched(extensive_artifact, general_uuid) -> ArtifactEnriched:
    return ArtifactEnriched(
        **extensive_artifact.model_dump(exclude={'sources'}),
        rank=0,
        correlation_uuid=general_uuid,
        sources=[MiscSource(ID='id', title='title', author='author', year='2025', ENTRYTYPE='misc', url='https://a.b')],
    )
