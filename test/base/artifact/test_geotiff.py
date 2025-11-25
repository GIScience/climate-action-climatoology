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
    default_artifact_copy.attachments = Attachments(display_filename='test_artifact_file-display.tiff')

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
    extensive_artifact_copy.attachments = Attachments(legend=legend, display_filename='test_artifact_file-display.tiff')

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


def test_create_geotiff_artifact_generates_display_raster_square_pixels(
    default_artifact_metadata, default_computation_resources
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

    generated_artifact = create_geotiff_artifact(
        raster_info=method_input,
        metadata=default_artifact_metadata,
        resources=default_computation_resources,
        display_square_pixels=True,
    )

    with rasterio.open(
        default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
    ) as generated_content:
        assert_array_equal(generated_content.read(), [np.ones(shape=(5, 4))])


def test_create_geotiff_artifact_2d_colormap_rgba(
    default_computation_resources, general_uuid, default_artifact_metadata
):
    expected_artifact = _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_artifact_file.tiff',
        summary='Test summary',
        attachments=Attachments(
            legend=Legend(legend_data={'1': Color('#00ff00fe')}), display_filename='test_artifact_file-display.tiff'
        ),
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


def test_create_geotiff_artifact_masked_array_and_nodata_value(
    default_computation_resources, default_artifact_metadata
):
    method_input = RasterInfo(
        data=np.ma.masked_array([[1, 2], [3, 4]], dtype=np.uint8, mask=[[0, 0], [0, 1]], fill_value=5),
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

    with (
        rasterio.open(default_computation_resources.computation_dir / generated_artifact.filename) as generated_content,
        rasterio.open(
            default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
        ) as display_content,
    ):
        generated_data = generated_content.read(1, masked=True)
        assert_array_equal(generated_data.data, np.array([[1, 2], [3, 5]]))
        assert_array_equal(generated_data.mask, method_input.data.mask)
        assert generated_data.fill_value == method_input.data.fill_value

        display_data = display_content.read(1, masked=True)
        # The pseudo mercator projection stores the data in a different order to WGS84, so the expected array is flipped
        assert_array_equal(display_data.data, np.array([[3, 5], [1, 2]]))
        assert_array_equal(display_data.mask, np.array([[False, True], [False, False]]))
        assert display_data.fill_value == method_input.data.fill_value


def test_create_geotiff_artifact_3d(default_computation_resources, general_uuid, default_artifact_metadata):
    expected_artifact = _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOTIFF,
        filename='test_artifact_file.tiff',
        summary='Test summary',
        attachments=Attachments(
            legend=Legend(legend_data={'1': Color('#0f0')}), display_filename='test_artifact_file-display.tiff'
        ),
    )

    method_input = RasterInfo(
        data=np.array([[[1, 2], [3, 4]], [[10, 20], [30, 40]]], dtype=np.uint8),
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

    with (
        rasterio.open(default_computation_resources.computation_dir / generated_artifact.filename) as generated_content,
        rasterio.open(
            default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
        ) as display_content,
    ):
        assert_array_equal(generated_content.read(), method_input.data)
        assert_array_equal(display_content.read(), np.flip(method_input.data, axis=1))


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

    with (
        rasterio.open(default_computation_resources.computation_dir / generated_artifact.filename) as generated_content,
        rasterio.open(
            default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
        ) as display_content,
    ):
        assert generated_content.overviews(1) == []
        assert display_content.overviews(1) == []


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

    with (
        rasterio.open(default_computation_resources.computation_dir / generated_artifact.filename) as generated_content,
        rasterio.open(
            default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
        ) as display_content,
    ):
        assert generated_content.overviews(1) == [2, 4]
        assert display_content.overviews(1) == [2, 4]


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

    with (
        rasterio.open(default_computation_resources.computation_dir / generated_artifact.filename) as generated_content,
        rasterio.open(
            default_computation_resources.computation_dir / generated_artifact.attachments.display_filename
        ) as display_content,
    ):
        assert generated_content.profile['tiled']
        assert generated_content.profile['blockxsize'] == 512
        assert generated_content.profile['blockysize'] == 512
        assert display_content.profile['tiled']
        assert display_content.profile['blockxsize'] == 512
        assert display_content.profile['blockysize'] == 512


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
