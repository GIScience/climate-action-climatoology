import uuid
from pathlib import Path

from pydantic_extra_types.color import Color

from climatoology.base.artifact import (
    ArtifactModality,
    Attachments,
    Legend,
    _Artifact,
)
from climatoology.base.info import MiscSource


def test_artifact_init_from_json():
    target_artifact = _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOJSON,
        file_path=Path(__file__).parent / 'test_file.tiff',
        summary='Test summary',
        description='Test description',
        correlation_uuid=uuid.uuid4(),
        store_id='test_file.tiff',
        tags={'A', 'B'},
    )
    transformed_artifact = target_artifact.model_dump(mode='json')
    computed_artifact = _Artifact(**transformed_artifact)
    assert computed_artifact == target_artifact


def test_artifact_optional_fields():
    """The optionality of fields is checked in the default_artifact fixture.
    The same should happen with the individual artifact tests where there should be a main test with minimum and a
    second test with maximum input.
    """
    assert _Artifact(
        rank=0,
        name='test_name',
        modality=ArtifactModality.MARKDOWN,
        primary=True,
        tags={'A', 'B'},
        file_path=Path(__file__).parent / 'resources/test_artifact_file.md',
        summary='Test summary',
        description='Test description',
        sources=[MiscSource(ID='id', title='title', author='author', year='2025', ENTRYTYPE='misc', url='https://a.b')],
        correlation_uuid=uuid.uuid4(),
        store_id='stor_id',
        attachments=Attachments(legend=Legend(legend_data={'a': Color('blue')})),
    )
