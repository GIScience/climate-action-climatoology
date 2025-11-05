from climatoology.base.artifact import create_markdown_artifact


def test_create_concise_markdown_artifact(default_computation_resources, default_artifact, default_artifact_metadata):
    method_input = """# Header

    Content
    """

    generated_artifact = create_markdown_artifact(
        text=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )
    with open(default_computation_resources.computation_dir / generated_artifact.filename, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == default_artifact
    assert generated_content == method_input


def test_create_extensive_markdown_artifact(
    default_computation_resources, general_uuid, extensive_artifact, default_sources, extensive_artifact_metadata
):
    method_input = """# Header

    Content
    """
    method_input_copy = method_input

    extensive_artifact_copy = extensive_artifact.model_copy(deep=True)
    extensive_artifact_copy.description = None
    extensive_artifact_metadata_copy = extensive_artifact_metadata.model_copy(deep=True)
    extensive_artifact_metadata_copy.description = None

    generated_artifact = create_markdown_artifact(
        text=method_input,
        metadata=extensive_artifact_metadata_copy,
        resources=default_computation_resources,
    )

    assert method_input_copy == method_input, 'Method input should not be mutated during artifact creation'
    assert generated_artifact == extensive_artifact_copy
