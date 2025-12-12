import pytest
from pydantic import ValidationError

from climatoology.base.artifact import (
    Artifact,
    ArtifactEnriched,
    ArtifactMetadata,
    ArtifactModality,
)


def test_artifact_enriched_init_from_json(extensive_artifact_enriched):
    transformed_artifact = extensive_artifact_enriched.model_dump(mode='json')
    computed_artifact = ArtifactEnriched(**transformed_artifact)
    assert computed_artifact == extensive_artifact_enriched


def test_artifact_filename_ascii_compliance_check():
    with pytest.raises(ValidationError, match="Value error, 'ascii' codec can't encode character"):
        _ = Artifact(
            name='test_name',
            modality=ArtifactModality.MARKDOWN,
            filename='test_artifact_file_$p€ciöl.md',
            summary='Test summary',
        )


def test_artifact_filename_is_unique():
    metadata_a = ArtifactMetadata(name='name', summary='summary')
    metadata_b = ArtifactMetadata(name='name', summary='summary')

    assert metadata_a.filename != metadata_b.filename
