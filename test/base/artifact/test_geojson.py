import geopandas as gpd
import pandas as pd
import pytest
from geopandas import GeoDataFrame
from geopandas.testing import assert_geodataframe_equal
from pandas._testing import assert_series_equal
from pydantic_extra_types.color import Color
from shapely import MultiPoint, Point

from climatoology.base.artifact import (
    Artifact,
    ArtifactModality,
    Attachments,
    ContinuousLegendData,
    Legend,
    create_vector_artifact,
)


def test_create_concise_vector_artifact(default_computation_resources, default_artifact, default_artifact_metadata):
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
    )
    expected_content = """{
"type": "FeatureCollection",
"features": [
{ "type": "Feature", "properties": { "index": "0", "color": "#fff", "label": "White a" }, "geometry": { "type": "Point", "coordinates": [ 1.0, 1.0 ] } },
{ "type": "Feature", "properties": { "index": "1", "color": "#000", "label": "Black b" }, "geometry": { "type": "Point", "coordinates": [ 2.0, 2.0 ] } },
{ "type": "Feature", "properties": { "index": "2", "color": "#0f0", "label": "Green c" }, "geometry": { "type": "Point", "coordinates": [ 3.0, 3.0 ] } }
]
}
"""

    default_artifact_copy = default_artifact.model_copy(deep=True)
    default_artifact_copy.modality = ArtifactModality.VECTOR_MAP_LAYER
    default_artifact_copy.filename = f'{default_artifact_metadata.filename}.geojson'
    default_artifact_copy.attachments = Attachments(
        legend=Legend(legend_data={'Black b': Color('#000'), 'Green c': Color('#0f0'), 'White a': Color('#fff')}),
        display_filename=f'{default_artifact_metadata.filename}-display.pmtiles',
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )
    with open(default_computation_resources.computation_dir / generated_artifact.filename, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == default_artifact_copy
    assert generated_content == expected_content


def test_create_extensive_vector_artifact(
    default_computation_resources, extensive_artifact, extensive_artifact_metadata
):
    method_input = GeoDataFrame(
        data={
            'my_color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
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
    extensive_artifact_copy.filename = f'{extensive_artifact_metadata.filename}.geojson'
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


def test_create_vector_artifact_continuous_legend(
    default_computation_resources, general_uuid, default_artifact_metadata
):
    expected_artifact = Artifact(
        name='test_name',
        modality=ArtifactModality.VECTOR_MAP_LAYER,
        filename='test_artifact_file.geojson',
        summary='Test summary',
        attachments=Attachments(
            legend=Legend(
                legend_data=ContinuousLegendData(
                    cmap_name='plasma', ticks={'Black b': 0.0, 'Green c': 0.5, 'White a': 1.0}
                )
            ),
            display_filename=f'{default_artifact_metadata.filename}-display.pmtiles',
        ),
    )

    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
    )

    legend = Legend(
        legend_data=ContinuousLegendData(cmap_name='plasma', ticks={'Black b': 0, 'Green c': 0.5, 'White a': 1})
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
        legend=legend,
    )

    assert generated_artifact == expected_artifact


def test_create_vector_artifact_index_str(default_computation_resources, general_uuid, default_artifact_metadata):
    expected_geojson = """{
"type": "FeatureCollection",
"features": [
{ "type": "Feature", "properties": { "index": "hello", "color": "#fff", "label": "White a" }, "geometry": { "type": "Point", "coordinates": [ 1.0, 1.0 ] } },
{ "type": "Feature", "properties": { "index": "again", "color": "#000", "label": "Black b" }, "geometry": { "type": "Point", "coordinates": [ 2.0, 2.0 ] } },
{ "type": "Feature", "properties": { "index": "world", "color": "#0f0", "label": "Green c" }, "geometry": { "type": "Point", "coordinates": [ 3.0, 3.0 ] } }
]
}
"""

    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        index=['hello', 'again', 'world'],
        crs='EPSG:4326',
    )

    generated_artifact = create_vector_artifact(
        data=method_input, metadata=default_artifact_metadata, resources=default_computation_resources
    )

    with open(default_computation_resources.computation_dir / generated_artifact.filename, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == expected_geojson

    written_display_data = gpd.read_file(
        default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
    )
    assert_series_equal(
        written_display_data['index'], method_input.index.to_series(), check_names=False, check_index=False
    )


def test_create_vector_artifact_index_non_unique(
    default_computation_resources, general_uuid, default_artifact_metadata
):
    expected_geojson = """{
"type": "FeatureCollection",
"features": [
{ "type": "Feature", "properties": { "index": "hello", "color": "#fff", "label": "White a" }, "geometry": { "type": "Point", "coordinates": [ 1.0, 1.0 ] } },
{ "type": "Feature", "properties": { "index": "hello", "color": "#000", "label": "Black b" }, "geometry": { "type": "Point", "coordinates": [ 2.0, 2.0 ] } },
{ "type": "Feature", "properties": { "index": "world", "color": "#0f0", "label": "Green c" }, "geometry": { "type": "Point", "coordinates": [ 3.0, 3.0 ] } }
]
}
"""

    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        index=['hello', 'hello', 'world'],
        crs='EPSG:4326',
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    with open(default_computation_resources.computation_dir / generated_artifact.filename, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == expected_geojson

    written_display_data = gpd.read_file(
        default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
    )
    assert_series_equal(
        written_display_data['index'], method_input.index.to_series(), check_names=False, check_index=False
    )


EXPECTED_MULTIINDEX_GEOJSON = """{
"type": "FeatureCollection",
"features": [
{ "type": "Feature", "properties": { "index": "('bar', 'one')", "color": "#fff", "label": "White a" }, "geometry": { "type": "Point", "coordinates": [ 1.0, 1.0 ] } },
{ "type": "Feature", "properties": { "index": "('bar', 'two')", "color": "#000", "label": "Black b" }, "geometry": { "type": "Point", "coordinates": [ 2.0, 2.0 ] } },
{ "type": "Feature", "properties": { "index": "('baz', 'one')", "color": "#0f0", "label": "Green c" }, "geometry": { "type": "Point", "coordinates": [ 3.0, 3.0 ] } }
]
}
"""


def test_create_vector_artifact_multiindex(default_computation_resources, general_uuid, default_artifact_metadata):
    expected_artifact = Artifact(
        name='test_name',
        modality=ArtifactModality.VECTOR_MAP_LAYER,
        filename='test_artifact_file.geojson',
        summary='Test summary',
        attachments=Attachments(
            legend=Legend(legend_data={'Black b': Color('#000'), 'Green c': Color('#0f0'), 'White a': Color('#fff')}),
            display_filename=f'{default_artifact_metadata.filename}-display.pmtiles',
        ),
    )

    index = pd.MultiIndex.from_tuples(
        [('bar', 'one'), ('bar', 'two'), ('baz', 'one')],
        names=['first', 'second'],
    )
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
        index=index,
    )
    expected_pmtiles_index = pd.Series(["('bar', 'one')", "('bar', 'two')", "('baz', 'one')"])

    generated_artifact = create_vector_artifact(
        data=method_input, metadata=default_artifact_metadata, resources=default_computation_resources
    )

    assert generated_artifact == expected_artifact

    with open(default_computation_resources.computation_dir / generated_artifact.filename, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == EXPECTED_MULTIINDEX_GEOJSON

    written_display_data = gpd.read_file(
        default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
    )
    assert_series_equal(written_display_data['index'], expected_pmtiles_index, check_names=False, check_index=False)


def test_create_vector_artifact_tuple_index(default_computation_resources, default_artifact_metadata):
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
        index=[('bar', 'one'), ('bar', 'two'), ('baz', 'one')],
    )
    expected_pmtiles_index = pd.Series(["('bar', 'one')", "('bar', 'two')", "('baz', 'one')"])

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    with open(default_computation_resources.computation_dir / generated_artifact.filename, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == EXPECTED_MULTIINDEX_GEOJSON

    written_display_data = gpd.read_file(
        default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
    )
    assert_series_equal(written_display_data['index'], expected_pmtiles_index, check_names=False, check_index=False)


def test_create_vector_artifact_extra_column_retained(default_computation_resources, default_artifact_metadata):
    expected_geojson = """{
"type": "FeatureCollection",
"features": [
{ "type": "Feature", "properties": { "index": "hello", "color": "#fff", "label": "White a", "extra_column_1": "lorem" }, "geometry": { "type": "Point", "coordinates": [ 1.0, 1.0 ] } },
{ "type": "Feature", "properties": { "index": "again", "color": "#000", "label": "Black b", "extra_column_1": "ipsum" }, "geometry": { "type": "Point", "coordinates": [ 2.0, 2.0 ] } },
{ "type": "Feature", "properties": { "index": "world", "color": "#0f0", "label": "Green c", "extra_column_1": "dolor" }, "geometry": { "type": "Point", "coordinates": [ 3.0, 3.0 ] } }
]
}
"""

    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'extra_column_1': ['lorem', 'ipsum', 'dolor'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        index=['hello', 'again', 'world'],
        crs='EPSG:4326',
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    with open(default_computation_resources.computation_dir / generated_artifact.filename, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == expected_geojson
    written_display_data = gpd.read_file(
        default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
    )
    assert 'extra_column_1' in written_display_data.columns


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


EXPECTED_ROUNDING_GEOJSON = """{
"type": "FeatureCollection",
"features": [
{ "type": "Feature", "properties": { "index": "0", "color": "#fff", "label": "Do Not Augment" }, "geometry": { "type": "Point", "coordinates": [ 1.0, 0.9 ] } },
{ "type": "Feature", "properties": { "index": "1", "color": "#000", "label": "Do Not Round" }, "geometry": { "type": "Point", "coordinates": [ 2.0000001, 1.9999999 ] } },
{ "type": "Feature", "properties": { "index": "2", "color": "#0f0", "label": "Round Me" }, "geometry": { "type": "Point", "coordinates": [ 3.0, 3.0 ] } }
]
}
"""


def test_write_vector_file_max_precision(default_computation_resources, default_artifact_metadata):
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['Do Not Augment', 'Do Not Round', 'Round Me'],
            'geometry': [Point(1.0, 0.9), Point(2.0000001, 1.9999999), Point(3.00000001, 2.99999999)],
        },
        crs='EPSG:4326',
    )

    generated_artifact = create_vector_artifact(
        data=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    with open(default_computation_resources.computation_dir / generated_artifact.filename, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == EXPECTED_ROUNDING_GEOJSON
