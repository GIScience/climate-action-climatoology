import pytest
from pydantic import ValidationError
from pydantic_extra_types.color import Color

from climatoology.base.artifact import (
    ArtifactEnriched,
    ArtifactModality,
    _Artifact,
    legend_data_from_colormap,
)


def test_artifact_enriched_init_from_json(extensive_artifact_enriched):
    transformed_artifact = extensive_artifact_enriched.model_dump(mode='json')
    computed_artifact = ArtifactEnriched(**transformed_artifact)
    assert computed_artifact == extensive_artifact_enriched


def test_artifact_filename_ascii_compliance_check():
    with pytest.raises(ValidationError, match="Value error, 'ascii' codec can't encode character"):
        _ = _Artifact(
            name='test_name',
            modality=ArtifactModality.MARKDOWN,
            filename='test_artifact_file_$p€ciöl.md',
            summary='Test summary',
        )


def test_legend_data_from_colormap():
    expected_legend_data = {'1': Color((255, 255, 255))}

    colormap = {1: (255, 255, 255)}
    computed_legend_data = legend_data_from_colormap(colormap=colormap)

    assert computed_legend_data == expected_legend_data


def test_legend_data_from_colormap_with_alpha():
    expected_legend_data = {'1': Color((255, 255, 255, 0.2))}

    colormap = {1: (255, 255, 255, 51)}
    computed_legend_data = legend_data_from_colormap(colormap=colormap)

    assert computed_legend_data == expected_legend_data
