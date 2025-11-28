from copy import deepcopy

import plotly
import pytest
from plotly import express as px
from pydantic import ValidationError
from pydantic_extra_types.color import Color

from climatoology.base.artifact import (
    ArtifactModality,
    Chart2dData,
    ChartType,
)
from climatoology.base.artifact_creators import create_chart_artifact, create_plotly_chart_artifact


def test_chart_check_length():
    with pytest.raises(ValidationError, match='X and Y data must be the same length.'):
        Chart2dData(
            x=[1, 2],
            y=[3, 2, 1],
            chart_type=ChartType.SCATTER,
        )
    with pytest.raises(ValidationError, match='Data and color lists must be the same length.'):
        Chart2dData(
            x=[1, 2, 3],
            y=[3, 2, 1],
            color=[Color('green'), Color('yellow')],
            chart_type=ChartType.SCATTER,
        )


def test_chart_check_type():
    try:
        Chart2dData(
            x=['1', '2'],
            y=[1, 2],
            chart_type=ChartType.SCATTER,
        )
    except ValidationError:
        pytest.fail('String and Number should be allowed')

    with pytest.raises(ValidationError, match=r'Only one dimension can be nominal \(a str\).'):
        Chart2dData(
            x=['1', '2'],
            y=['1', '2'],
            chart_type=ChartType.SCATTER,
        )


def test_chart_check_data():
    with pytest.raises(ValidationError, match='Pie-chart Y-Axis must be numeric.'):
        Chart2dData(
            x=[1, 2],
            y=['1', '2'],
            chart_type=ChartType.PIE,
        )
    with pytest.raises(ValidationError, match='Pie-chart Y-Data must be all positive.'):
        Chart2dData(
            x=[1, 2],
            y=[1, -2],
            chart_type=ChartType.PIE,
        )

    data = Chart2dData(
        x=[1, 2],
        y=[1, 2],
        chart_type=ChartType.PIE,
    )
    assert data.x == [1, 2]
    assert data.y == [1, 2]


def test_chart_explode_color():
    chart_data = Chart2dData(
        x=[1, 2],
        y=['1', '2'],
        color=Color('green'),
        chart_type=ChartType.BAR,
    )
    assert chart_data.color == [Color('green'), Color('green')]


def test_chart_explode_color_for_line():
    chart_data = Chart2dData(
        x=[1, 2],
        y=['1', '2'],
        color=Color('green'),
        chart_type=ChartType.LINE,
    )
    assert chart_data.color == Color('green')


def test_three_instance_chart2ddata():
    """Assert that Chart2dDate can be created with exactly three elements.

    This threw a cryptic TypeError if pydantic checks for Color before checking for List[Color].
    """
    assert Chart2dData(
        x=[1, 2, 3], y=[1, 2, 3], chart_type=ChartType.BAR, color=[Color('red'), Color('green'), Color('blue')]
    )


@pytest.mark.parametrize('chart_type', [ChartType.SCATTER, ChartType.LINE, ChartType.BAR, ChartType.PIE])
def test_create_concise_chart_artifact(
    chart_type, default_computation_resources, default_artifact, default_artifact_metadata
):
    method_input = Chart2dData(
        x=[1, 2, 3],
        y=[3, 2, 1],
        x_label='x title',
        y_label='y title',
        color=Color('green'),
        chart_type=chart_type,
    )

    default_artifact_copy = default_artifact.model_copy(deep=True)
    default_artifact_copy.modality = ArtifactModality.CHART_PLOTLY
    default_artifact_copy.filename = f'{default_artifact_metadata.filename}.json'

    generated_artifact = create_chart_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    assert generated_artifact == default_artifact_copy
    assert plotly.io.read_json(default_computation_resources.computation_dir / generated_artifact.filename)


@pytest.mark.parametrize('chart_type', [ChartType.SCATTER, ChartType.LINE, ChartType.BAR, ChartType.PIE])
def test_create_extensive_chart_artifact(
    chart_type, default_computation_resources, extensive_artifact, extensive_artifact_metadata
):
    method_input = Chart2dData(
        x=[1, 2, 3],
        y=[3, 2, 1],
        x_label='x title',
        y_label='y title',
        color=Color('green'),
        chart_type=chart_type,
    )
    method_input_copy = method_input.model_copy(deep=True)

    extensive_artifact_copy = extensive_artifact.model_copy(deep=True)
    extensive_artifact_copy.modality = ArtifactModality.CHART_PLOTLY
    extensive_artifact_copy.filename = f'{extensive_artifact_metadata.filename}.json'

    generated_artifact = create_chart_artifact(
        data=method_input,
        metadata=extensive_artifact_metadata,
        resources=default_computation_resources,
    )

    assert generated_artifact == extensive_artifact_copy
    assert method_input == method_input_copy, 'Method input should not be mutated during artifact creation'


def test_create_concise_plotly_chart_artifact(
    default_computation_resources, default_artifact, default_artifact_metadata
):
    method_input = px.scatter(x=[0, 1, 2, 3, 4], y=[0, 1, 4, 9, 16])

    default_artifact_copy = default_artifact.model_copy(deep=True)
    default_artifact_copy.modality = ArtifactModality.CHART_PLOTLY
    default_artifact_copy.filename = f'{default_artifact_metadata.filename}.json'

    generated_artifact = create_plotly_chart_artifact(
        figure=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )
    generated_content = plotly.io.read_json(default_computation_resources.computation_dir / generated_artifact.filename)

    assert generated_artifact == default_artifact_copy
    assert generated_content == method_input


def test_create_extensive_plotly_chart_artifact(
    default_computation_resources, extensive_artifact, extensive_artifact_metadata
):
    method_input = px.scatter(x=[0, 1, 2, 3, 4], y=[0, 1, 4, 9, 16])
    method_input_copy = deepcopy(method_input)

    extensive_artifact_copy = extensive_artifact.model_copy(deep=True)
    extensive_artifact_copy.modality = ArtifactModality.CHART_PLOTLY
    extensive_artifact_copy.filename = f'{extensive_artifact_metadata.filename}.json'

    generated_artifact = create_plotly_chart_artifact(
        figure=method_input,
        metadata=extensive_artifact_metadata,
        resources=default_computation_resources,
    )

    assert method_input == method_input_copy, 'Method input should not be mutated during artifact creation'
    assert generated_artifact == extensive_artifact_copy
