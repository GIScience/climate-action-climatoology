from pathlib import Path

import numpy as np
import pytest
import rasterio
from PIL import Image
from affine import Affine
from geopandas import GeoSeries
from pandas import DataFrame
from pydantic import ValidationError
from pydantic.color import Color
from rasterio import CRS
from shapely import Point

from climatoology.base.artifact import create_markdown_artifact, Artifact, ArtifactModality, create_table_artifact, \
    create_image_artifact, create_chart_artifact, Chart2dData, ChartType, create_geojson_artifact, \
    create_geotiff_artifact


def test_chart_check_length():
    with pytest.raises(ValidationError, match='X and Y data must be the same length.'):
        Chart2dData(x=[1, 2],
                    y=[3, 2, 1],
                    chart_type=ChartType.SCATTER)
    with pytest.raises(ValidationError, match='Data and color lists must be the same length.'):
        Chart2dData(x=[1, 2, 3],
                    y=[3, 2, 1],
                    color=['green', 'yellow'],
                    chart_type=ChartType.SCATTER)


def test_chart_check_type():
    try:
        Chart2dData(x=['1', '2'],
                    y=[1, 2],
                    chart_type=ChartType.SCATTER)
    except ValidationError:
        pytest.fail('String and Number should be allowed')

    with pytest.raises(ValidationError, match='Only one dimension can be nominal \(a str\).'):
        Chart2dData(x=['1', '2'],
                    y=['1', '2'],
                    chart_type=ChartType.SCATTER)


def test_chart_check_data():
    with pytest.raises(ValidationError, match='Pie-chart Y-Axis must be numeric.'):
        Chart2dData(x=[1, 2],
                    y=['1', '2'],
                    chart_type=ChartType.PIE)
    with pytest.raises(ValidationError, match='Pie-chart Y-Data must be all positive.'):
        Chart2dData(x=[1, 2],
                    y=[1, -2],
                    chart_type=ChartType.PIE)


def test_create_markdown_artifact(default_computation_resources, general_uuid):
    expected_artifact = Artifact(name='-',
                                 modality=ArtifactModality.MARKDOWN,
                                 file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.md'),
                                 summary='-')
    expected_content = """# Header

    Content
    """

    generated_artifact = create_markdown_artifact(text=expected_content,
                                                  name='-',
                                                  tl_dr='-',
                                                  resources=default_computation_resources,
                                                  filename=general_uuid)
    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == expected_artifact
    assert generated_content == expected_content


def test_create_table_artifact(default_computation_resources, general_uuid):
    expected_artifact = Artifact(name='Test Table',
                                 modality=ArtifactModality.TABLE,
                                 file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.csv'),
                                 summary='Table caption')
    method_input = DataFrame(data=[('Data1', 2.5),
                                   ('Data2', np.nan)],
                             index=['Row1', 'Row2'],
                             columns=['Column1', 'Column2'])
    expected_content = """index,Column1,Column2
Row1,Data1,2.5
Row2,Data2,
"""

    generated_artifact = create_table_artifact(data=method_input,
                                               title='Test Table',
                                               caption='Table caption',
                                               resources=default_computation_resources,
                                               filename=general_uuid)
    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == expected_artifact
    assert generated_content == expected_content


def test_create_image_artifact(default_computation_resources, general_uuid):
    expected_artifact = Artifact(name='Test Image',
                                 modality=ArtifactModality.IMAGE,
                                 file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.png'),
                                 summary='Image caption',
                                 description='Nice graphic')
    expected_content = Image.new(mode='RGB',
                                 size=(2, 2),
                                 color=(153, 153, 255))

    generated_artifact = create_image_artifact(image=expected_content,
                                               title='Test Image',
                                               caption='Image caption',
                                               description='Nice graphic',
                                               resources=default_computation_resources,
                                               filename=general_uuid)
    generated_content = Image.open(generated_artifact.file_path,
                                   mode='r',
                                   formats=['PNG'])

    assert generated_artifact == expected_artifact
    assert generated_content.convert('RGB') == expected_content


def test_create_chart_artifact(default_computation_resources, general_uuid):
    expected_artifact = Artifact(name='Test Chart',
                                 modality=ArtifactModality.CHART,
                                 file_path=Path(default_computation_resources.computation_dir / f'{general_uuid}.json'),
                                 summary='Chart caption')
    method_input = Chart2dData(x=[1, 2, 3],
                               y=[3, 2, 1],
                               chart_type=ChartType.SCATTER)
    expected_content = """{
    "x": [
        1.0,
        2.0,
        3.0
    ],
    "y": [
        3.0,
        2.0,
        1.0
    ],
    "chart_type": "SCATTER",
    "color": "#590d08"
}"""

    generated_artifact = create_chart_artifact(data=method_input,
                                               title='Test Chart',
                                               caption='Chart caption',
                                               resources=default_computation_resources,
                                               filename=general_uuid)
    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == expected_artifact
    assert generated_content == expected_content


def test_create_geojson_artifact(default_computation_resources, general_uuid):
    expected_artifact = Artifact(name='Test Vector',
                                 modality=ArtifactModality.MAP_LAYER_GEOJSON,
                                 file_path=Path(default_computation_resources.computation_dir /
                                                f'{general_uuid}.geojson'),
                                 summary='Vector caption')
    method_input = GeoSeries(data=[Point(1, 1),
                                   Point(2, 2),
                                   Point(3, 3)],
                             crs='EPSG:4326')
    expected_content = """{
    "type": "FeatureCollection",
    "features": [
        {
            "id": "0",
            "type": "Feature",
            "properties": {
                "color": "#fff"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [
                    1.0,
                    1.0
                ]
            },
            "bbox": [
                1.0,
                1.0,
                1.0,
                1.0
            ]
        },
        {
            "id": "1",
            "type": "Feature",
            "properties": {
                "color": "#fff"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [
                    2.0,
                    2.0
                ]
            },
            "bbox": [
                2.0,
                2.0,
                2.0,
                2.0
            ]
        },
        {
            "id": "2",
            "type": "Feature",
            "properties": {
                "color": "#fff"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [
                    3.0,
                    3.0
                ]
            },
            "bbox": [
                3.0,
                3.0,
                3.0,
                3.0
            ]
        }
    ],
    "bbox": [
        1.0,
        1.0,
        3.0,
        3.0
    ]
}"""

    generated_artifact = create_geojson_artifact(method_input,
                                                 layer_name='Test Vector',
                                                 caption='Vector caption',
                                                 color=Color((255, 255, 255)),
                                                 resources=default_computation_resources,
                                                 filename=general_uuid)
    with open(generated_artifact.file_path, 'r') as test_file:
        generated_content = test_file.read()

    assert generated_artifact == expected_artifact
    assert generated_content == expected_content


def test_create_geotiff_artifact(default_computation_resources, general_uuid):
    expected_artifact = Artifact(name='Test Raster',
                                 modality=ArtifactModality.MAP_LAYER_GEOTIFF,
                                 file_path=Path(default_computation_resources.computation_dir /
                                                f'{general_uuid}.tiff'),
                                 summary='Raster caption')
    method_input = np.ones(shape=(2, 3, 4), dtype=float)

    generated_artifact = create_geotiff_artifact(method_input,
                                                 crs=CRS({'init': 'epsg:4326'}),
                                                 transformation=Affine.from_gdal(c=8.7,
                                                                                 a=0.1,
                                                                                 b=0.0,
                                                                                 f=49.4,
                                                                                 d=0.0,
                                                                                 e=0.1),
                                                 colormap={1: (0, 255, 0)},
                                                 layer_name='Test Raster',
                                                 caption='Raster caption',
                                                 resources=default_computation_resources,
                                                 filename=general_uuid)

    generated_content = rasterio.open(generated_artifact.file_path)

    assert generated_artifact == expected_artifact
    assert (generated_content.read() == method_input).all()
