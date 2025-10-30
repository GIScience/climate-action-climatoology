from pathlib import Path

from PIL import Image

from climatoology.base.artifact import ArtifactModality, _Artifact, create_image_artifact


def test_create_concise_image_artifact(default_computation_resources, general_uuid, default_association_tags):
    expected_artifact = _Artifact(
        name='Test Image',
        modality=ArtifactModality.IMAGE,
        filename='test_file.png',
        summary='Image caption',
        correlation_uuid=general_uuid,
    )
    expected_content = Image.new(mode='RGB', size=(2, 2), color=(153, 153, 255))

    generated_artifact = create_image_artifact(
        image=expected_content,
        title='Test Image',
        caption='Image caption',
        resources=default_computation_resources,
        filename='test_file',
    )
    generated_content = Image.open(
        default_computation_resources.computation_dir / generated_artifact.filename, mode='r', formats=['PNG']
    )

    assert generated_artifact == expected_artifact
    assert generated_content.convert('RGB') == expected_content


def test_create_extensive_image_artifact(
    default_computation_resources, general_uuid, default_association_tags, default_sources
):
    expected_artifact = _Artifact(
        name='Test Image',
        modality=ArtifactModality.IMAGE,
        filename='test_file.png',
        summary='Image caption',
        description='Nice graphic',
        sources=default_sources,
        tags=default_association_tags,
        primary=False,
        correlation_uuid=general_uuid,
    )
    expected_content = Image.new(mode='RGB', size=(2, 2), color=(153, 153, 255))

    expected_content_copy = expected_content.copy()

    generated_artifact = create_image_artifact(
        image=expected_content,
        title='Test Image',
        caption='Image caption',
        resources=default_computation_resources,
        primary=False,
        description='Nice graphic',
        sources=Path(__file__).parent.parent.parent / 'resources/minimal.bib',
        tags=default_association_tags,
        filename='test_file',
    )
    generated_content = Image.open(
        default_computation_resources.computation_dir / generated_artifact.filename, mode='r', formats=['PNG']
    )

    assert expected_content == expected_content_copy, 'Method input should not be mutated during artifact creation'
    assert generated_artifact == expected_artifact
    assert generated_content.convert('RGB') == expected_content
