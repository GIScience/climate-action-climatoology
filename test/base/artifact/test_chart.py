from pathlib import Path

import plotly
import pytest
from plotly import express as px
from pydantic import ValidationError
from pydantic_extra_types.color import Color

from climatoology.base.artifact import (
    ArtifactModality,
    Chart2dData,
    ChartType,
    _Artifact,
    create_chart_artifact,
    create_plotly_chart_artifact,
)


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
def test_create_concise_chart_artifact(chart_type, default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Chart',
        modality=ArtifactModality.CHART_PLOTLY,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.json'),
        summary='Chart caption',
    )
    method_input = Chart2dData(
        x=[1, 2, 3],
        y=[3, 2, 1],
        x_label='x title',
        y_label='y title',
        color=Color('green'),
        chart_type=chart_type,
    )

    generated_artifact = create_chart_artifact(
        data=method_input,
        title='Test Chart',
        caption='Chart caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
    )

    assert generated_artifact == expected_artifact


@pytest.mark.parametrize('chart_type', [ChartType.SCATTER, ChartType.LINE, ChartType.BAR, ChartType.PIE])
def test_create_extensive_chart_artifact(
    chart_type, default_computation_resources, general_uuid, default_association_tags, default_sources
):
    expected_artifact = _Artifact(
        name='Test Chart',
        modality=ArtifactModality.CHART_PLOTLY,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.json'),
        summary='Chart caption',
        description='Chart description',
        sources=default_sources,
        tags=default_association_tags,
        primary=False,
    )
    method_input = Chart2dData(
        x=[1, 2, 3],
        y=[3, 2, 1],
        x_label='x title',
        y_label='y title',
        color=Color('green'),
        chart_type=chart_type,
    )

    generated_artifact = create_chart_artifact(
        data=method_input,
        title='Test Chart',
        caption='Chart caption',
        resources=default_computation_resources,
        primary=False,
        description='Chart description',
        sources=Path(__file__).parent.parent.parent / 'resources/minimal.bib',
        tags=default_association_tags,
        filename=str(general_uuid),
    )

    assert generated_artifact == expected_artifact


def test_create_concise_plotly_chart_artifact(
    default_computation_resources,
    general_uuid,
):
    expected_artifact = _Artifact(
        name='Test Plotly Chart',
        modality=ArtifactModality.CHART_PLOTLY,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.json'),
        summary='Chart caption',
    )
    method_input = px.scatter(x=[0, 1, 2, 3, 4], y=[0, 1, 4, 9, 16])

    generated_artifact = create_plotly_chart_artifact(
        figure=method_input,
        title='Test Plotly Chart',
        caption='Chart caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
    )

    assert generated_artifact == expected_artifact

    generated_chart = plotly.io.read_json(generated_artifact.file_path)
    assert generated_chart == method_input


def test_create_plotly_chart_artifact(
    default_computation_resources, general_uuid, default_association_tags, default_sources
):
    expected_artifact = _Artifact(
        name='Test Plotly Chart',
        modality=ArtifactModality.CHART_PLOTLY,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.json'),
        summary='Chart caption',
        description='Chart description',
        sources=default_sources,
        tags=default_association_tags,
        primary=False,
    )
    method_input = px.scatter(x=[0, 1, 2, 3, 4], y=[0, 1, 4, 9, 16])

    generated_artifact = create_plotly_chart_artifact(
        figure=method_input,
        title='Test Plotly Chart',
        caption='Chart caption',
        resources=default_computation_resources,
        primary=False,
        description='Chart description',
        sources=Path(__file__).parent.parent.parent / 'resources/minimal.bib',
        tags=default_association_tags,
        filename=str(general_uuid),
    )

    assert generated_artifact == expected_artifact

    generated_chart = plotly.io.read_json(generated_artifact.file_path)
    assert generated_chart == method_input
