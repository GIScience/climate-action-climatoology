import logging
import os
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import rasterio
from PIL import Image
from affine import Affine
from geopandas import GeoDataFrame
from numpy.testing import assert_array_equal
from pandas import DataFrame
from pydantic import ValidationError
from pydantic_extra_types.color import Color
from rasterio import CRS
from shapely import Point

from climatoology.base.artifact import (
    create_markdown_artifact,
    _Artifact,
    ArtifactModality,
    create_table_artifact,
    create_image_artifact,
    create_chart_artifact,
    Chart2dData,
    ChartType,
    create_geojson_artifact,
    create_geotiff_artifact,
    RasterInfo,
    ContinuousLegendData,
    AttachmentType,
    Legend,
    Colormap,
)


def test_artifact_init_from_json():
    target_artifact = _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOJSON,
        file_path=Path(__file__).parent / 'test_file.tiff',
        summary='Test summary',
        description='Test description',
        correlation_uuid=uuid.uuid4(),
        store_id='test_file.tiff',
        tags={'A', 'B'},
    )
    transformed_artifact = target_artifact.model_dump(mode='json')
    computed_artifact = _Artifact(**transformed_artifact)
    assert computed_artifact == target_artifact


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


def test_create_markdown_artifact(default_computation_resources, general_uuid, default_association_tags):
    expected_artifact = _Artifact(
        name='-',
        modality=ArtifactModality.MARKDOWN,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.md'),
        summary='-',
        tags=default_association_tags,
    )
    expected_content = """# Header

    Content
    """

    generated_artifact = create_markdown_artifact(
        text=expected_content,
        name='-',
        tl_dr='-',
        resources=default_computation_resources,
        filename=str(general_uuid),
        tags=default_association_tags,
    )
    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == expected_artifact
    assert generated_content == expected_content


def test_create_table_artifact(default_computation_resources, general_uuid, default_association_tags):
    expected_artifact = _Artifact(
        name='Test Table',
        modality=ArtifactModality.TABLE,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.csv'),
        summary='Table caption',
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
        filename=str(general_uuid),
        tags=default_association_tags,
    )
    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == expected_artifact
    assert generated_content == expected_content


def test_create_image_artifact(default_computation_resources, general_uuid, default_association_tags):
    expected_artifact = _Artifact(
        name='Test Image',
        modality=ArtifactModality.IMAGE,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.png'),
        summary='Image caption',
        description='Nice graphic',
        tags=default_association_tags,
    )
    expected_content = Image.new(mode='RGB', size=(2, 2), color=(153, 153, 255))

    generated_artifact = create_image_artifact(
        image=expected_content,
        title='Test Image',
        caption='Image caption',
        description='Nice graphic',
        resources=default_computation_resources,
        filename=str(general_uuid),
        tags=default_association_tags,
    )
    generated_content = Image.open(generated_artifact.file_path, mode='r', formats=['PNG'])

    assert generated_artifact == expected_artifact
    assert generated_content.convert('RGB') == expected_content


def test_create_chart_artifact(
    default_computation_resources,
    general_uuid,
    default_association_tags,
):
    expected_artifact = _Artifact(
        name='Test Chart',
        modality=ArtifactModality.CHART,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.json'),
        summary='Chart caption',
        tags=default_association_tags,
    )
    method_input = Chart2dData(
        x=[1, 2, 3],
        y=[3, 2, 1],
        chart_type=ChartType.SCATTER,
    )
    expected_content = """{"x": [1.0, 2.0, 3.0], "y": [3.0, 2.0, 1.0], "chart_type": "SCATTER", "color": "#590d08"}"""

    generated_artifact = create_chart_artifact(
        data=method_input,
        title='Test Chart',
        caption='Chart caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
        tags=default_association_tags,
    )
    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == expected_artifact
    assert generated_content == expected_content


EXPECTED_GEOJSON = """{
"type": "FeatureCollection",
"features": [
{ "type": "Feature", "properties": { "index": 0, "color": "#fff", "label": "White a" }, "geometry": { "type": "Point", "coordinates": [ 1.0, 1.0 ] } },
{ "type": "Feature", "properties": { "index": 1, "color": "#000", "label": "Black b" }, "geometry": { "type": "Point", "coordinates": [ 2.0, 2.0 ] } },
{ "type": "Feature", "properties": { "index": 2, "color": "#0f0", "label": "Green c" }, "geometry": { "type": "Point", "coordinates": [ 3.0, 3.0 ] } }
]
}
"""


def test_create_geojson_artifact(default_computation_resources, general_uuid, default_association_tags):
    expected_artifact = _Artifact(
        name='Test Vector',
        modality=ArtifactModality.MAP_LAYER_GEOJSON,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.geojson'),
        summary='Vector caption',
        attachments={
            AttachmentType.LEGEND: Legend(
                legend_data={'Black b': Color('#000'), 'Green c': Color('#0f0'), 'White a': Color('#fff')}
            )
        },
        tags=default_association_tags,
    )

    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
    )

    generated_artifact = create_geojson_artifact(
        features=method_input.geometry,
        color=method_input.color.tolist(),
        label=method_input.label.tolist(),
        layer_name='Test Vector',
        caption='Vector caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
        tags=default_association_tags,
    )

    assert generated_artifact == expected_artifact

    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == EXPECTED_GEOJSON


def test_create_geojson_artifact_continuous_legend(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Vector',
        modality=ArtifactModality.MAP_LAYER_GEOJSON,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.geojson'),
        summary='Vector caption',
        attachments={
            AttachmentType.LEGEND: Legend(
                legend_data=ContinuousLegendData(
                    cmap_name='plasma', ticks={'Black b': 0.0, 'Green c': 0.5, 'White a': 1.0}
                )
            )
        },
    )

    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
    )

    legend = ContinuousLegendData(cmap_name='plasma', ticks={'Black b': 0, 'Green c': 0.5, 'White a': 1})

    generated_artifact = create_geojson_artifact(
        features=method_input.geometry,
        color=method_input.color.tolist(),
        label=method_input.label.tolist(),
        layer_name='Test Vector',
        caption='Vector caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
        legend_data=legend,
    )

    assert generated_artifact == expected_artifact


def test_create_geojson_artifact_index_str(default_computation_resources, general_uuid, default_association_tags):
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

    generated_artifact = create_geojson_artifact(
        features=method_input.geometry,
        color=method_input.color.tolist(),
        label=method_input.label.tolist(),
        layer_name='Test Vector',
        caption='Vector caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
        tags=default_association_tags,
    )

    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == expected_geojson


def test_create_geojson_artifact_index_non_unique(
    default_computation_resources, general_uuid, default_association_tags
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

    generated_artifact = create_geojson_artifact(
        features=method_input.geometry,
        color=method_input.color.tolist(),
        label=method_input.label.tolist(),
        layer_name='Test Vector',
        caption='Vector caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
        tags=default_association_tags,
    )

    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == expected_geojson


EXPECTED_MULTIINDEX_GEOJSON = """{
"type": "FeatureCollection",
"features": [
{ "type": "Feature", "properties": { "index": "('bar', 'one')", "color": "#fff", "label": "White a" }, "geometry": { "type": "Point", "coordinates": [ 1.0, 1.0 ] } },
{ "type": "Feature", "properties": { "index": "('bar', 'two')", "color": "#000", "label": "Black b" }, "geometry": { "type": "Point", "coordinates": [ 2.0, 2.0 ] } },
{ "type": "Feature", "properties": { "index": "('baz', 'one')", "color": "#0f0", "label": "Green c" }, "geometry": { "type": "Point", "coordinates": [ 3.0, 3.0 ] } }
]
}
"""


def test_create_geojson_artifact_multiindex(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Vector',
        modality=ArtifactModality.MAP_LAYER_GEOJSON,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.geojson'),
        summary='Vector caption',
        attachments={
            AttachmentType.LEGEND: Legend(
                legend_data={'Black b': Color('#000'), 'Green c': Color('#0f0'), 'White a': Color('#fff')}
            )
        },
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

    generated_artifact = create_geojson_artifact(
        features=method_input.geometry,
        layer_name='Test Vector',
        caption='Vector caption',
        color=method_input.color.tolist(),
        label=method_input.label.tolist(),
        resources=default_computation_resources,
        filename=str(general_uuid),
    )

    assert generated_artifact == expected_artifact

    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == EXPECTED_MULTIINDEX_GEOJSON


def test_create_geojson_artifact_tuple_index(default_computation_resources, general_uuid):
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['White a', 'Black b', 'Green c'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)],
        },
        crs='EPSG:4326',
        index=[('bar', 'one'), ('bar', 'two'), ('baz', 'one')],
    )

    generated_artifact = create_geojson_artifact(
        features=method_input.geometry,
        layer_name='Test Vector',
        caption='Vector caption',
        color=method_input.color.tolist(),
        label=method_input.label.tolist(),
        resources=default_computation_resources,
        filename=str(general_uuid),
    )

    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == EXPECTED_MULTIINDEX_GEOJSON


EXPECTED_ROUNDING_GEOJSON = """{
"type": "FeatureCollection",
"features": [
{ "type": "Feature", "properties": { "index": 0, "color": "#fff", "label": "Do Not Augment" }, "geometry": { "type": "Point", "coordinates": [ 1.0, 0.9 ] } },
{ "type": "Feature", "properties": { "index": 1, "color": "#000", "label": "Do Not Round" }, "geometry": { "type": "Point", "coordinates": [ 2.0000001, 1.9999999 ] } },
{ "type": "Feature", "properties": { "index": 2, "color": "#0f0", "label": "Round Me" }, "geometry": { "type": "Point", "coordinates": [ 3.0, 3.0 ] } }
]
}
"""


def test_write_geojson_file_max_precision(default_computation_resources, general_uuid):
    method_input = GeoDataFrame(
        data={
            'color': [Color((255, 255, 255)), Color((0, 0, 0)), Color((0, 255, 0))],
            'label': ['Do Not Augment', 'Do Not Round', 'Round Me'],
            'geometry': [Point(1.0, 0.9), Point(2.0000001, 1.9999999), Point(3.00000001, 2.99999999)],
        },
        crs='EPSG:4326',
    )

    generated_artifact = create_geojson_artifact(
        features=method_input.geometry,
        color=method_input.color.tolist(),
        label=method_input.label.tolist(),
        layer_name='Test Vector',
        caption='Vector caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
    )

    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

        assert generated_content == EXPECTED_ROUNDING_GEOJSON


def test_create_geotiff_artifact_2d(default_computation_resources, general_uuid, default_association_tags):
    expected_artifact = _Artifact(
        name='Test Raster',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.tiff'),
        summary='Raster caption',
        attachments={AttachmentType.LEGEND: Legend(legend_data={'1': Color('#0f0')})},
        tags=default_association_tags,
    )

    method_input = RasterInfo(
        data=np.ones(shape=(4, 5), dtype=np.uint8),
        crs=CRS({'init': 'epsg:4326'}),
        transformation=Affine.from_gdal(
            c=8.7,
            a=0.1,
            b=0.0,
            f=49.4,
            d=0.0,
            e=0.1,
        ),
        colormap=Colormap({1: (0, 255, 0)}),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
        tags=default_association_tags,
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(generated_artifact.file_path) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_geotiff_artifact_2d_rgba(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Raster',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.tiff'),
        summary='Raster caption',
        attachments={AttachmentType.LEGEND: Legend(legend_data={'1': Color('#00ff00fe')})},
    )

    method_input = RasterInfo(
        data=np.ones(shape=(4, 5), dtype=np.uint8),
        crs=CRS({'init': 'epsg:4326'}),
        transformation=Affine.from_gdal(
            c=8.7,
            a=0.1,
            b=0.0,
            f=49.4,
            d=0.0,
            e=0.1,
        ),
        colormap=Colormap({1: (0, 255, 0, 254)}),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(generated_artifact.file_path) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_geotiff_artifact_masked_array(default_computation_resources, general_uuid):
    method_input = RasterInfo(
        data=np.ma.masked_array(np.ones(shape=(2, 2), dtype=np.uint8), mask=[[0, 0], [0, 1]]),
        crs=CRS({'init': 'epsg:4326'}),
        transformation=Affine.from_gdal(
            c=8.7,
            a=0.1,
            b=0.0,
            f=49.4,
            d=0.0,
            e=0.1,
        ),
        colormap=Colormap({1: (0, 255, 0)}),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
    )

    with rasterio.open(generated_artifact.file_path) as generated_content:
        assert_array_equal(generated_content.read(1, masked=True), method_input.data)


def test_create_geotiff_with_legend_data(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Raster',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.tiff'),
        summary='Raster caption',
        attachments={AttachmentType.LEGEND: Legend(legend_data={'A': Color('#f00')})},
    )

    method_input = RasterInfo(
        data=np.ones(shape=(4, 5), dtype=np.uint8),
        crs=CRS({'init': 'epsg:4326'}),
        transformation=Affine.from_gdal(
            c=8.7,
            a=0.1,
            b=0.0,
            f=49.4,
            d=0.0,
            e=0.1,
        ),
        colormap=Colormap({1: (0, 255, 0)}),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
        legend_data={'A': Color('red')},
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(generated_artifact.file_path) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_geotiff_without_legend(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Raster',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.tiff'),
        summary='Raster caption',
        attachments={},
    )

    method_input = RasterInfo(
        data=np.ones(shape=(4, 5), dtype=np.uint8),
        crs=CRS({'init': 'epsg:4326'}),
        transformation=Affine.from_gdal(
            c=8.7,
            a=0.1,
            b=0.0,
            f=49.4,
            d=0.0,
            e=0.1,
        ),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(generated_artifact.file_path) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_geotiff_artifact_3d(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Raster',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.tiff'),
        summary='Raster caption',
        attachments={AttachmentType.LEGEND: Legend(legend_data={'1': Color('#0f0')})},
    )

    method_input = RasterInfo(
        data=np.ones(shape=(3, 4, 5), dtype=np.uint8),
        crs=CRS({'init': 'epsg:4326'}),
        transformation=Affine.from_gdal(
            c=8.7,
            a=0.1,
            b=0.0,
            f=49.4,
            d=0.0,
            e=0.1,
        ),
        colormap=Colormap({1: (0, 255, 0)}),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename=str(general_uuid),
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(generated_artifact.file_path) as generated_content:
        assert_array_equal(generated_content.read(), method_input.data)


def test_geotiff_creation_option_warnings(default_computation_resources, general_uuid, caplog):
    method_input = RasterInfo(
        data=np.ones(shape=(4, 5), dtype=np.uint8),
        crs=CRS({'init': 'epsg:4326'}),
        transformation=Affine.from_gdal(
            c=8.7,
            a=0.1,
            b=0.0,
            f=49.4,
            d=0.0,
            e=0.1,
        ),
        colormap=Colormap({1: (0, 255, 0)}),
    )

    with caplog.at_level(logging.INFO):
        create_geotiff_artifact(
            method_input,
            layer_name='Test Raster',
            caption='Raster caption',
            resources=default_computation_resources,
            filename=str(general_uuid),
        )

    assert caplog.messages == []


def test_disallow_colormap_for_incompatible_dtype(default_computation_resources):
    method_input = RasterInfo(
        data=np.ones(shape=(4, 5), dtype=float),
        crs=CRS({'init': 'epsg:4326'}),
        transformation=Affine.from_gdal(
            c=8.7,
            a=0.1,
            b=0.0,
            f=49.4,
            d=0.0,
            e=0.1,
        ),
        colormap=Colormap({1: (0, 255, 0)}),
    )

    with pytest.raises(AssertionError, match='Colormaps are not allowed for dtype float.'):
        create_geotiff_artifact(
            method_input,
            layer_name='Test Raster',
            caption='Raster caption',
            resources=default_computation_resources,
        )


def test_rasterinfo_from_rasterio(default_computation_resources, general_uuid):
    with rasterio.open(f'{os.path.dirname(__file__)}/../resources/test_raster_a.tiff') as raster:
        # the Colormap type is the correct type for rasterio colormaps
        # noinspection PyTypeChecker
        colormap: Colormap = raster.colormap(1)
        generated_info = RasterInfo(
            data=raster.read(), crs=raster.crs, transformation=raster.transform, colormap=colormap
        )

    assert generated_info


def test_three_instance_chart2ddata():
    """Assert that Chart2dDate can be created with exactly three elements.

    This threw a cryptic TypeError if pydantic checks for Color before checking for List[Color].
    """
    assert Chart2dData(
        x=[1, 2, 3], y=[1, 2, 3], chart_type=ChartType.BAR, color=[Color('red'), Color('green'), Color('blue')]
    )
