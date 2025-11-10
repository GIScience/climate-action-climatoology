import logging
import os

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
    create_geotiff_artifact,
)


def test_create_concise_geotiff_artifact_2d(default_computation_resources, default_artifact, default_artifact_metadata):
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

    default_artifact_copy = default_artifact.model_copy(deep=True)
    default_artifact_copy.modality = ArtifactModality.MAP_LAYER_GEOTIFF
    default_artifact_copy.filename = f'{default_artifact_metadata.filename}.tiff'

    generated_artifact = create_geotiff_artifact(
        raster_info=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )
    with rasterio.open(default_computation_resources.computation_dir / generated_artifact.filename) as generated_file:
        generated_content = generated_file.read()

    assert generated_artifact == default_artifact_copy
    assert_array_equal(generated_content, [method_input.data])


def test_create_extensive_geotiff_artifact_2d(
    default_computation_resources, extensive_artifact, extensive_artifact_metadata
):
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
        colormap={1: (0, 255, 0)},
    )
    legend = Legend(legend_data={'1': Color('#0f0')}, title='Custom Legend Title')
    method_input_copy = method_input.model_copy(deep=True)

    extensive_artifact_copy = extensive_artifact.model_copy(deep=True)
    extensive_artifact_copy.modality = ArtifactModality.MAP_LAYER_GEOTIFF
    extensive_artifact_copy.filename = f'{extensive_artifact_metadata.filename}.tiff'
    extensive_artifact_copy.attachments = Attachments(legend=legend)

    generated_artifact = create_geotiff_artifact(
        raster_info=method_input,
        metadata=extensive_artifact_metadata,
        resources=default_computation_resources,
        legend=legend,
    )

    # Method input should not be mutated during artifact creation
    assert (method_input.data == method_input_copy.data).all()
    assert method_input.crs == method_input_copy.crs
    assert method_input.transformation == method_input_copy.transformation
    assert method_input.colormap == method_input_copy.colormap

    assert generated_artifact == extensive_artifact_copy


def test_create_geotiff_artifact_2d_rgba(default_computation_resources, general_uuid, default_artifact_metadata):
    expected_artifact = _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_artifact_file.tiff',
        summary='Test summary',
        attachments=Attachments(legend=Legend(legend_data={'1': Color('#00ff00fe')})),
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
        colormap={1: (0, 255, 0, 254)},
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_geotiff_artifact_masked_array(default_computation_resources, default_artifact_metadata):
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
        colormap={1: (0, 255, 0)},
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(1, masked=True), method_input.data)


def test_create_geotiff_with_legend_data(default_computation_resources, general_uuid, default_artifact_metadata):
    expected_artifact = _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_artifact_file.tiff',
        summary='Test summary',
        attachments=Attachments(legend=Legend(legend_data={'A': Color('#f00')})),
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
        colormap={1: (0, 255, 0)},
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
        legend=Legend(legend_data={'A': Color('red')}),
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_geotiff_without_legend(default_computation_resources, general_uuid, default_artifact_metadata):
    expected_artifact = _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_artifact_file.tiff',
        summary='Test summary',
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
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(), [method_input.data])


def test_create_geotiff_artifact_3d(default_computation_resources, general_uuid, default_artifact_metadata):
    expected_artifact = _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_artifact_file.tiff',
        summary='Test summary',
        attachments=Attachments(legend=Legend(legend_data={'1': Color('#0f0')})),
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
        colormap={1: (0, 255, 0)},
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    assert generated_artifact == expected_artifact

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert_array_equal(generated_content.read(), method_input.data)


def test_geotiff_creation_option_warnings(default_computation_resources, caplog, default_artifact_metadata):
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
        colormap={1: (0, 255, 0)},
    )

    with caplog.at_level(logging.INFO):
        create_geotiff_artifact(
            method_input,
            metadata=default_artifact_metadata,
            resources=default_computation_resources,
        )

    assert caplog.messages == []


def test_small_geotiff_created_without_overviews(default_computation_resources, default_artifact_metadata):
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
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert generated_content.overviews(1) == []


def test_geotiff_created_with_overviews(default_computation_resources, default_artifact_metadata):
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
        colormap={1: (0, 255, 0)},
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert generated_content.overviews(1) == [2, 4]


def test_geotiff_tiled(default_computation_resources, default_artifact_metadata):
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
        colormap={1: (0, 255, 0)},
    )

    generated_artifact = create_geotiff_artifact(
        method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
    )

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.filename
    ) as generated_content:
        assert generated_content.profile['tiled']
        assert generated_content.profile['blockxsize'] == 512
        assert generated_content.profile['blockysize'] == 512


def test_disallow_colormap_for_incompatible_dtype(default_computation_resources, default_artifact_metadata):
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
        colormap={1: (0, 255, 0)},
    )

    with pytest.raises(AssertionError, match='Colormaps are not allowed for dtype float.'):
        create_geotiff_artifact(
            method_input,
            metadata=default_artifact_metadata,
            resources=default_computation_resources,
        )


def test_rasterinfo_from_rasterio(default_computation_resources):
    with rasterio.open(f'{os.path.dirname(__file__)}/../../resources/test_raster_a.tiff') as raster:
        colormap = raster.colormap(1)
        generated_info = RasterInfo(
            data=raster.read(), crs=raster.crs, transformation=raster.transform, colormap=colormap
        )

    assert generated_info
