import geopandas as gpd
import pandas as pd
import pytest
from geopandas import GeoDataFrame
from geopandas.geoseries import GeoSeries
from geopandas.testing import assert_geodataframe_equal, assert_geoseries_equal
from pandas import Series
from pandas._testing import assert_series_equal
from pydantic_extra_types.color import Color
from shapely import MultiPoint, Point

from climatoology.base.artifact import (
    ArtifactModality,
    Attachments,
    ContinuousLegendData,
    Legend,
)
from climatoology.base.artifact_creators import create_vector_artifact


def test_create_concise_vector_artifact(default_computation_resources, default_artifact, default_artifact_metadata):
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
    )
    expected_content = GeoDataFrame(
        data={
            'index': [0, 1, 2],
            'color': ['#fff', '#000', '#0f0'],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
    )

    default_artifact_copy = default_artifact.model_copy(deep=True)
    default_artifact_copy.modality = ArtifactModality.VECTOR_MAP_LAYER
    default_artifact_copy.filename = f'{default_artifact_metadata.filename}.gpkg'
    default_artifact_copy.attachments = Attachments(
        legend=Legend(legend_data={'Black b': Color('#000'), 'Green c': Color('#0f0'), 'White a': Color('#fff')}),
        display_filename=f'{default_artifact_metadata.filename}-display.pmtiles',
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )
    generated_content = gpd.read_file(default_computation_resources.computation_dir / generated_artifact.filename)

    assert generated_artifact == default_artifact_copy
    gpd.testing.assert_geodataframe_equal(generated_content, expected_content)


def test_create_extensive_vector_artifact(
    default_computation_resources, extensive_artifact, extensive_artifact_metadata
):
    method_input = GeoDataFrame(
        data={
            'my_color': [Color((254, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'my_label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
    )
    legend = Legend(
        legend_data={'Black b': Color('#000'), 'Green c': Color('#0f0'), 'White a': Color('#fff')},
        title='Custom Legend Title',
    )
    method_input_copy = method_input.copy(deep=True)

    extensive_artifact_copy = extensive_artifact.model_copy(deep=True)
    extensive_artifact_copy.modality = ArtifactModality.VECTOR_MAP_LAYER
    extensive_artifact_copy.filename = f'{extensive_artifact_metadata.filename}.gpkg'
    extensive_artifact_copy.attachments = Attachments(
        legend=legend.model_copy(deep=True), display_filename=f'{extensive_artifact_metadata.filename}-display.pmtiles'
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=extensive_artifact_metadata,
        resources=default_computation_resources,
        color='my_color',
        label='my_label',
        legend=legend,
    )

    # Method input should not be mutated during artifact creation
    assert_geodataframe_equal(method_input, method_input_copy)
    assert generated_artifact == extensive_artifact_copy


def test_create_vector_artifact_generates_pmtiles(default_computation_resources, default_artifact_metadata):
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 254)), Color((0, 0, 1)), Color((0, 255, 1))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
    )
    expected_display_data = GeoDataFrame(
        data={
            'index': ['0', '1', '2'],
            'color': [Color((255, 255, 254)).as_hex(), Color((0, 0, 1)).as_hex(), Color((0, 255, 1)).as_hex()],
            'label': ['White a', 'Black b', 'Green c'],
            'mvt_id': pd.Series([None, None, None], dtype=float),
            'geometry': [MultiPoint([(1, 1)]), MultiPoint([(2, 2)]), MultiPoint([(3, 3)])],
        },
        crs='EPSG:4326',
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    assert generated_artifact.attachments.display_filename == 'test_artifact_file-display.pmtiles'

    written_data = gpd.read_file(
        default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
    )
    assert written_data.crs == 3857

    written_data = written_data.to_crs(4326)
    written_data['geometry'] = written_data['geometry'].set_precision(grid_size=1e-5)

    assert_geodataframe_equal(written_data, expected_display_data, check_like=True)


def test_create_vector_artifact_can_overwrite_pmtile_config(default_computation_resources, default_artifact_metadata):
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 254)), Color((0, 0, 1)), Color((0, 255, 1))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
        pmtiles_lco={'NAME': 'another-layer-name'},
    )

    written_data = gpd.read_file(
        default_computation_resources.computation_dir / generated_artifact.attachments.display_filename,
        layer='another-layer-name',
    )
    assert not written_data.empty


def test_create_vector_artifact_extra_column_removed_for_display_but_retained_for_download(
    default_computation_resources, default_artifact_metadata
):
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 254))],
            'label': ['inf'],
            'extra_column_1': ['x'],
            'geometry': [Point(1, 1)],
        },
        crs='EPSG:4326',
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    written_data = gpd.read_file(default_computation_resources.computation_dir / generated_artifact.filename)
    assert written_data.columns.to_list() == ['index', 'color', 'label', 'extra_column_1', 'geometry']

    written_display_data = gpd.read_file(
        default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
    )
    assert written_display_data.columns.to_list() == ['mvt_id', 'index', 'color', 'label', 'geometry']


def test_create_vector_artifact_retain_custom_legend(
    default_computation_resources, general_uuid, default_artifact_metadata
):
    input_legend = Legend(
        legend_data=ContinuousLegendData(cmap_name='plasma', ticks={'Black b': 0, 'Green c': 0.5, 'White a': 1})
    )
    expect_output = input_legend.model_copy(deep=True)

    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
        legend=input_legend,
    )

    assert generated_artifact.attachments.legend == expect_output


def test_create_vector_artifact_creates_fitting_index_for_display_file(
    default_computation_resources, default_artifact_metadata
):
    """The requirements are: must be unique of type str and called 'index'"""
    # Provided to the function are: a GeoDataFrame with non-unique, int-type index called 'wrong_name'
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        index=[0, 0, 1],
        crs='EPSG:4326',
    )
    method_input.index = method_input.index.rename('wrong_name')

    expected_output_index_column = Series(['0', '1', '2'], name='index')

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    written_display_data = gpd.read_file(
        default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
    )
    assert_series_equal(written_display_data['index'], expected_output_index_column, check_index=False)


def test_create_vector_artifact_fail_on_wrong_color_type(default_computation_resources, default_artifact_metadata):
    method_input = GeoDataFrame(
        data={
            'color': ['#ffffff', '#b00b1e', '#000000'],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        index=['hello', 'again', 'world'],
        crs='EPSG:4326',
    )

    with pytest.raises(AssertionError, match=r'Not all values in column color are of type'):
        create_vector_artifact(
            data=method_input,
            metadata=default_artifact_metadata,
            resources=default_computation_resources,
        )


def test_create_vector_artifact_fail_on_missing_legend_labels(default_artifact_metadata, default_computation_resources):
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0))],
            'label': ['White a', 'Black b'],
            'geometry': [Point(1, 1), Point(2, 2)],
        },
        crs='EPSG:4326',
    )

    legend = Legend(
        legend_data={'White a': Color('#fff'), 'Green c': Color('#0f0')},
        title='Custom Legend Title',
    )

    with pytest.raises(
        AssertionError, match=r"The following labels are included in the data, but not in the legend: {'Black b'}"
    ):
        create_vector_artifact(
            data=method_input,
            metadata=default_artifact_metadata,
            resources=default_computation_resources,
            legend=legend,
        )


def test_write_vector_file_max_precision(default_computation_resources, default_artifact_metadata):
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['Do Not Augment', 'Do Not Round', 'Round Me'],
            'geometry': [Point(1.0, 0.9), Point(2.0000001, 1.9999999), Point(3.00000001, 2.99999999)],
        },
        crs='EPSG:4326',
    )
    expected_output = GeoSeries(
        data=[Point(1.0, 0.9), Point(2.0000001, 1.9999999), Point(3.0, 3.0)],
        crs='EPSG:4326',
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    written_data = gpd.read_file(default_computation_resources.computation_dir / generated_artifact.filename)
    written_data = written_data.set_index('index')

    assert_geoseries_equal(written_data.geometry, expected_output)
