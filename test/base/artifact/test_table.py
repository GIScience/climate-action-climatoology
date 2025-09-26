from pathlib import Path

import numpy as np
from pandas import DataFrame

from climatoology.base.artifact import ArtifactModality, _Artifact, create_table_artifact


def test_create_concise_table_artifact(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Table',
        modality=ArtifactModality.TABLE,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.csv'),
        summary='Table caption',
    )
    method_input = DataFrame(
        data=[('Data1', 2.5), ('Data2', np.nan)],
        index=['Row1', 'Row2'],
        columns=['Column1', 'Column2'],
    )
    expected_content = """index,Column1,Column2
Row1,Data1,2.5
Row2,Data2,
"""

    generated_artifact = create_table_artifact(
        data=method_input,
        title='Test Table',
        caption='Table caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
    )
    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == expected_artifact
    assert generated_content == expected_content


def test_create_extensive_table_artifact(
    default_computation_resources, general_uuid, default_association_tags, default_sources
):
    expected_artifact = _Artifact(
        name='Test Table',
        modality=ArtifactModality.TABLE,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.csv'),
        primary=False,
        summary='Table caption',
        description='Table description',
        sources=default_sources,
        tags=default_association_tags,
    )
    method_input = DataFrame(
        data=[('Data1', 2.5), ('Data2', np.nan)],
        index=['Row1', 'Row2'],
        columns=['Column1', 'Column2'],
    )
    expected_content = """index,Column1,Column2
Row1,Data1,2.5
Row2,Data2,
"""

    generated_artifact = create_table_artifact(
        data=method_input,
        title='Test Table',
        caption='Table caption',
        resources=default_computation_resources,
        primary=False,
        description='Table description',
        sources=Path(__file__).parent.parent.parent / 'resources/minimal.bib',
        tags=default_association_tags,
        filename=str(general_uuid),
    )
    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == expected_artifact
    assert generated_content == expected_content
