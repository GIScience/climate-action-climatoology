import logging
import os
from pathlib import Path

import numpy as np
import pytest
import rasterio
from affine import Affine
from numpy.testing import assert_array_equal
from pydantic_extra_types.color import Color
from rasterio import CRS

from climatoology.base.artifact import (
    ArtifactModality,
    Attachments,
    Legend,
    RasterInfo,
    _Artifact,
    colormap_type,
    create_geotiff_artifact,
)


def test_create_concise_geotiff_artifact_2d(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Raster',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_file.tiff',
        summary='Raster caption',
        attachments=Attachments(legend=Legend(legend_data={'1': Color('#0f0')})),
        correlation_uuid=general_uuid,
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
        colormap=colormap_type({1: (0, 255, 0)}),
    )

    generated_artifact = create_geotiff_artifact(
        raster_info=method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename='test_file',
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_extensive_geotiff_artifact_2d(
    default_computation_resources, general_uuid, default_association_tags, default_sources
):
    expected_artifact = _Artifact(
        name='Test Raster',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_file.tiff',
        summary='Raster caption',
        description='Raster description',
        sources=default_sources,
        tags=default_association_tags,
        primary=False,
        attachments=Attachments(legend=Legend(legend_data={'1': Color('#0f0')}, title='Custom Legend Title')),
        correlation_uuid=general_uuid,
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
        colormap=colormap_type({1: (0, 255, 0)}),
    )
    method_input_copy = method_input.model_copy(deep=True)

    legend = Legend(legend_data={'1': Color('#0f0')}, title='Custom Legend Title')
    generated_artifact = create_geotiff_artifact(
        raster_info=method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        primary=False,
        legend=legend,
        description='Raster description',
        sources=Path(__file__).parent.parent.parent / 'resources/minimal.bib',
        tags=default_association_tags,
        filename='test_file',
    )

    assert generated_artifact == expected_artifact

    # Method input should not be mutated during artifact creation
    assert (method_input.data == method_input_copy.data).all()
    assert method_input.crs == method_input_copy.crs
    assert method_input.transformation == method_input_copy.transformation
    assert method_input.colormap == method_input_copy.colormap

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_geotiff_artifact_2d_rgba(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Raster',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_file.tiff',
        summary='Raster caption',
        attachments=Attachments(legend=Legend(legend_data={'1': Color('#00ff00fe')})),
        correlation_uuid=general_uuid,
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
        colormap=colormap_type({1: (0, 255, 0, 254)}),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename='test_file',
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_geotiff_artifact_masked_array(default_computation_resources):
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
        colormap=colormap_type({1: (0, 255, 0)}),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename='test_file',
    )

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(1, masked=True), method_input.data)


def test_create_geotiff_with_legend_data(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Raster',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_file.tiff',
        summary='Raster caption',
        attachments=Attachments(legend=Legend(legend_data={'A': Color('#f00')})),
        correlation_uuid=general_uuid,
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
        colormap=colormap_type({1: (0, 255, 0)}),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename='test_file',
        legend=Legend(legend_data={'A': Color('red')}),
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_geotiff_without_legend(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Raster',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_file.tiff',
        summary='Raster caption',
        correlation_uuid=general_uuid,
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
        filename='test_file',
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_geotiff_artifact_3d(default_computation_resources, general_uuid):
    expected_artifact = _Artifact(
        name='Test Raster',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_file.tiff',
        summary='Raster caption',
        attachments=Attachments(legend=Legend(legend_data={'1': Color('#0f0')})),
        correlation_uuid=general_uuid,
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
        colormap=colormap_type({1: (0, 255, 0)}),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename='test_file',
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(), method_input.data)


def test_geotiff_creation_option_warnings(default_computation_resources, caplog):
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
        colormap=colormap_type({1: (0, 255, 0)}),
    )

    with caplog.at_level(logging.INFO):
        create_geotiff_artifact(
            method_input,
            layer_name='Test Raster',
            caption='Raster caption',
            resources=default_computation_resources,
            filename='test_file',
        )

    assert caplog.messages == []


def test_small_geotiff_created_without_overviews(default_computation_resources):
    method_input = RasterInfo(
        data=np.ones(shape=(10, 10), dtype=np.uint8),
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
        filename='test_file',
    )

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert generated_content.overviews(1) == []


def test_geotiff_created_with_overviews(default_computation_resources):
    method_input = RasterInfo(
        data=np.ones(shape=(1000, 1000), dtype=np.uint8),
        crs=CRS({'init': 'epsg:4326'}),
        transformation=Affine.from_gdal(
            c=8.7,
            a=0.1,
            b=0.0,
            f=49.4,
            d=0.0,
            e=0.1,
        ),
        colormap=colormap_type({1: (0, 255, 0)}),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename='test_file',
    )

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert generated_content.overviews(1) == [2, 4]


def test_geotiff_tiled(default_computation_resources):
    method_input = RasterInfo(
        data=np.ones(shape=(1000, 1000), dtype=np.uint8),
        crs=CRS({'init': 'epsg:4326'}),
        transformation=Affine.from_gdal(
            c=8.7,
            a=0.1,
            b=0.0,
            f=49.4,
            d=0.0,
            e=0.1,
        ),
        colormap=colormap_type({1: (0, 255, 0)}),
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        layer_name='Test Raster',
        caption='Raster caption',
        resources=default_computation_resources,
        filename='test_file',
    )

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert generated_content.profile['tiled']
        assert generated_content.profile['blockxsize'] == 512
        assert generated_content.profile['blockysize'] == 512


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
        colormap=colormap_type({1: (0, 255, 0)}),
    )

    with pytest.raises(AssertionError, match='Colormaps are not allowed for dtype float.'):
        create_geotiff_artifact(
            method_input,
            layer_name='Test Raster',
            caption='Raster caption',
            resources=default_computation_resources,
        )


def test_rasterinfo_from_rasterio(default_computation_resources):
    with rasterio.open(f'{os.path.dirname(__file__)}/../../resources/test_raster_a.tiff') as raster:
        # the Colormap type is the correct type for rasterio colormaps
        # noinspection PyTypeChecker
        colormap: colormap_type = raster.colormap(1)
        generated_info = RasterInfo(
            data=raster.read(), crs=raster.crs, transformation=raster.transform, colormap=colormap
        )

    assert generated_info
