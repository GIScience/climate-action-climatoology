from pathlib import Path

from climatoology.base.artifact import ArtifactModality, _Artifact, create_markdown_artifact


def test_create_concise_markdown_artifact(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='-',
        modality=ArtifactModality.MARKDOWN,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.md'),
        summary='-',
    )
    expected_content = """# Header

    Content
    """

    generated_artifact = create_markdown_artifact(
        text=expected_content,
        resources=default_computation_resources,
        name='-',
        tl_dr='-',
        filename=str(general_uuid),
    )
    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == expected_artifact
    assert generated_content == expected_content


def test_create_extensive_markdown_artifact(
    default_computation_resources, general_uuid, default_association_tags, default_sources
):
    expected_artifact = _Artifact(
        name='-',
        modality=ArtifactModality.MARKDOWN,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.md'),
        summary='-',
        sources=default_sources,
        primary=False,
        tags=default_association_tags,
    )
    expected_content = """# Header

    Content
    """

    generated_artifact = create_markdown_artifact(
        text=expected_content,
        resources=default_computation_resources,
        name='-',
        tl_dr='-',
        sources=Path(__file__).parent.parent.parent / 'resources/minimal.bib',
        primary=False,
        tags=default_association_tags,
        filename=str(general_uuid),
    )
    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == expected_artifact
    assert generated_content == expected_content
