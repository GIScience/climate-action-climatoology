from pathlib import Path

from PIL import Image

from climatoology.base.artifact import ArtifactModality, _Artifact, create_image_artifact


def test_create_concise_image_artifact(default_computation_resources, general_uuid, default_association_tags):
    expected_artifact = _Artifact(
        name='Test Image',
        modality=ArtifactModality.IMAGE,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.png'),
        summary='Image caption',
    )
    expected_content = Image.new(mode='RGB', size=(2, 2), color=(153, 153, 255))

    generated_artifact = create_image_artifact(
        image=expected_content,
        title='Test Image',
        caption='Image caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
    )
    generated_content = Image.open(generated_artifact.file_path, mode='r', formats=['PNG'])

    assert generated_artifact == expected_artifact
    assert generated_content.convert('RGB') == expected_content


def test_create_extensive_image_artifact(
    default_computation_resources, general_uuid, default_association_tags, default_sources
):
    expected_artifact = _Artifact(
        name='Test Image',
        modality=ArtifactModality.IMAGE,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.png'),
        summary='Image caption',
        description='Nice graphic',
        sources=default_sources,
        tags=default_association_tags,
        primary=False,
    )
    expected_content = Image.new(mode='RGB', size=(2, 2), color=(153, 153, 255))

    generated_artifact = create_image_artifact(
        image=expected_content,
        title='Test Image',
        caption='Image caption',
        resources=default_computation_resources,
        primary=False,
        description='Nice graphic',
        sources=Path(__file__).parent.parent.parent / 'resources/minimal.bib',
        tags=default_association_tags,
        filename=str(general_uuid),
    )
    generated_content = Image.open(generated_artifact.file_path, mode='r', formats=['PNG'])

    assert generated_artifact == expected_artifact
    assert generated_content.convert('RGB') == expected_content
