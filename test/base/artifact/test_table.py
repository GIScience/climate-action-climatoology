import numpy as np
from pandas import DataFrame

from climatoology.base.artifact import ArtifactModality, create_table_artifact


def test_create_concise_table_artifact(default_computation_resources, default_artifact, default_artifact_metadata):
    method_input = DataFrame(
        data=[('Data1', 2.5), ('Data2', np.nan)],
        index=['Row1', 'Row2'],
        columns=['Column1', 'Column2'],
    )
    expected_content = """index,Column1,Column2
Row1,Data1,2.5
Row2,Data2,
"""

    default_artifact_copy = default_artifact.model_copy(deep=True)
    default_artifact_copy.modality = ArtifactModality.TABLE
    default_artifact_copy.filename = f'{default_artifact_metadata.filename}.csv'

    generated_artifact = create_table_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )
    with open(default_computation_resources.computation_dir / generated_artifact.filename, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == default_artifact_copy
    assert generated_content == expected_content


def test_create_extensive_table_artifact(
    default_computation_resources, extensive_artifact, extensive_artifact_metadata
):
    method_input = DataFrame(
        data=[('Data1', 2.5), ('Data2', np.nan)],
        index=['Row1', 'Row2'],
        columns=['Column1', 'Column2'],
    )
    method_input_copy = method_input.copy(deep=True)

    extensive_artifact_copy = extensive_artifact.model_copy(deep=True)
    extensive_artifact_copy.modality = ArtifactModality.TABLE
    extensive_artifact_copy.filename = f'{extensive_artifact_metadata.filename}.csv'

    generated_artifact = create_table_artifact(
        data=method_input,
        metadata=extensive_artifact_metadata,
        resources=default_computation_resources,
    )

    assert method_input.equals(method_input_copy), 'Method input should not be mutated during artifact creation'
    assert generated_artifact == extensive_artifact_copy
