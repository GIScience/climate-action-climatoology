from PIL import Image

from climatoology.base.artifact import ArtifactModality, create_image_artifact


def test_create_concise_image_artifact(default_computation_resources, default_artifact, default_artifact_metadata):
    method_input = Image.new(mode='RGB', size=(2, 2), color=(153, 153, 255))

    default_artifact_copy = default_artifact.model_copy(deep=True)
    default_artifact_copy.modality = ArtifactModality.IMAGE
    default_artifact_copy.filename = f'{default_artifact_metadata.filename}.png'

    generated_artifact = create_image_artifact(
        image=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )
    generated_content = Image.open(
        default_computation_resources.computation_dir / generated_artifact.filename, mode='r', formats=['PNG']
    )

    assert generated_artifact == default_artifact_copy
    assert generated_content.convert('RGB') == method_input


def test_create_extensive_image_artifact(
    default_computation_resources, extensive_artifact, extensive_artifact_metadata
):
    method_input = Image.new(mode='RGB', size=(2, 2), color=(153, 153, 255))

    expected_content_copy = method_input.copy()

    extensive_artifact_copy = extensive_artifact.model_copy(deep=True)
    extensive_artifact_copy.modality = ArtifactModality.IMAGE
    extensive_artifact_copy.filename = f'{extensive_artifact_metadata.filename}.png'

    generated_artifact = create_image_artifact(
        image=method_input,
        metadata=extensive_artifact_metadata,
        resources=default_computation_resources,
    )

    assert method_input == expected_content_copy, 'Method input should not be mutated during artifact creation'
    assert generated_artifact == extensive_artifact_copy
