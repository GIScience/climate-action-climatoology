from datetime import date

import geopandas as gpd
import pandas as pd
import pytest
import rasterio
import shapely
from geopandas import testing
from numpy.testing import assert_allclose
from pandas.testing import assert_index_equal
from pyproj import CRS
from requests import Response
from responses import matchers
from shapely import LineString, geometry
from shapely.geometry.polygon import Polygon

from climatoology.utility.api import TimeRange
from climatoology.utility.exception import PlatformUtilityError
from climatoology.utility.naturalness import NaturalnessIndex, NaturalnessWorkUnit
from test.conftest import TEST_RESOURCES_DIR

NATURALNESS_UNIT_A = NaturalnessWorkUnit(
    aoi=geometry.box(8.67, 49.41, 8.69, 49.43),
    time_range=TimeRange(end_date=date(2023, 6, 1)),
)

NATURALNESS_UNIT_B = NaturalnessWorkUnit(
    aoi=geometry.box(8.69, 49.41, 8.71, 49.43),
    time_range=TimeRange(end_date=date(2023, 6, 1)),
)


def test_naturalness_connection_issues(mocked_utility_response, default_naturalness_utility):
    response = Response()
    response.status_code = 409

    mocked_utility_response.return_value = response

    with pytest.raises(PlatformUtilityError):
        with default_naturalness_utility.compute_raster(index=NaturalnessIndex.NDVI, units=[NATURALNESS_UNIT_A]):
            pass


def test_compute_raster_with_invalid_inputs(mocked_utility_response, default_naturalness_utility):
    with pytest.raises(AssertionError):
        with default_naturalness_utility.compute_raster(index=NaturalnessIndex.NDVI, units=[]):
            pass


def test_compute_raster_single_unit(mock_naturalness_raster_a, default_naturalness_utility):
    with default_naturalness_utility.compute_raster(index=NaturalnessIndex.NDVI, units=[NATURALNESS_UNIT_A]) as raster:
        assert raster.shape == (25, 16)
        assert raster.count == 1
        assert raster.dtypes[0] == rasterio.float32
        assert raster.bounds == NATURALNESS_UNIT_A.aoi.bounds


def test_compute_raster_multi_unit(mock_naturalness_raster_a, mock_naturalness_raster_b, default_naturalness_utility):
    units = [NATURALNESS_UNIT_A, NATURALNESS_UNIT_B]

    expected_bounds = (8.67, 49.41, 8.71, 49.43)

    with default_naturalness_utility.compute_raster(index=NaturalnessIndex.NDVI, units=units) as raster:
        assert raster.shape == (25, 32)
        assert_allclose(raster.bounds, expected_bounds)


def test_compute_raster_exceeds_max_raster_size(
    mock_naturalness_raster_a, mock_naturalness_raster_b, default_naturalness_utility
):
    """Assert that a requested area that exceeds the max_unit_size is separated into multiple chunks to query from the
    naturalness utility.
    """
    naturalness_unit = NaturalnessWorkUnit(
        aoi=geometry.box(8.67, 49.41, 8.71, 49.43),
        time_range=TimeRange(end_date=date(2023, 6, 1)),
    )

    with default_naturalness_utility.compute_raster(
        index=NaturalnessIndex.NDVI, units=[naturalness_unit], max_unit_size=30
    ) as raster:
        assert_allclose(raster.bounds, naturalness_unit.aoi.bounds)


def test_compute_raster_smaller_than_resolution(mock_naturalness_raster_small, default_naturalness_utility):
    """If the requested area has a dimension smaller than 1 pixel, the requested area should first be buffered,
    to ensure the requested dimensions >= 1.
    """
    invalid_small_bounds = [8.6790252, 49.4198277, 8.6797126, 49.4200740]  # dimensions: 50m x 27m

    unit = NaturalnessWorkUnit(
        aoi=shapely.geometry.box(*invalid_small_bounds),
        time_range=TimeRange(end_date=date(2023, 6, 1)),
    )

    with default_naturalness_utility.compute_raster(index=NaturalnessIndex.NDVI, units=[unit]) as raster:
        assert raster.shape == (1, 1)


def test_compute_vector_single_unit(mock_naturalness_raster_a, default_naturalness_utility):
    vectors = [
        Polygon([[8.67, 49.41], [8.69, 49.41], [8.69, 49.42], [8.67, 49.42], [8.67, 49.41]]),
        Polygon([[8.67, 49.42], [8.69, 49.42], [8.69, 49.43], [8.67, 49.43], [8.67, 49.42]]),
    ]
    request_gdf = gpd.GeoSeries(index=['first', 'second'], data=vectors, crs=CRS.from_epsg(4326))

    agg_stats = ['mean']
    time_range = TimeRange(end_date=date(2023, 6, 1))

    response_gdf = default_naturalness_utility.compute_vector(
        index=NaturalnessIndex.NDVI, aggregation_stats=agg_stats, vectors=[request_gdf], time_range=time_range
    )

    expected_output = gpd.GeoDataFrame(
        index=['first', 'second'],
        data={'mean': [0.5, 0.5]},
        geometry=vectors,
        crs=CRS.from_epsg(4326),
    )
    testing.assert_geodataframe_equal(response_gdf, expected_output, check_like=True)


@pytest.mark.parametrize('index_dtype', ['str', 'int', 'float'])
def test_compute_vector_retain_index_dtype(index_dtype, mock_naturalness_raster_a, default_naturalness_utility):
    request_gdf = gpd.GeoSeries(
        index=[1, 2],
        data=[
            Polygon([[8.67, 49.41], [8.69, 49.41], [8.69, 49.42], [8.67, 49.42], [8.67, 49.41]]),
            Polygon([[8.67, 49.42], [8.69, 49.42], [8.69, 49.43], [8.67, 49.43], [8.67, 49.42]]),
        ],
        crs=CRS.from_epsg(4326),
    )
    request_gdf.index = request_gdf.index.astype(index_dtype)

    agg_stats = ['mean']
    time_range = TimeRange(end_date=date(2023, 6, 1))

    response_gdf = default_naturalness_utility.compute_vector(
        index=NaturalnessIndex.NDVI, aggregation_stats=agg_stats, vectors=[request_gdf], time_range=time_range
    )

    match index_dtype:
        case 'str':
            assert_index_equal(response_gdf.index, pd.Index(['1', '2']))
        case 'int':
            assert_index_equal(response_gdf.index, pd.Index([1, 2]))
        case 'float':
            assert_index_equal(response_gdf.index, pd.Index([1.0, 2.0]))


def test_compute_vector_exceeds_max_raster_size(
    mock_naturalness_raster_a, mock_naturalness_raster_b, default_naturalness_utility
):
    """If the total vector bounds exceed the max_raster_size, the raster requests should be split into bounds that don't
    exceed this limit.
    """
    vectors = [shapely.geometry.box(8.67, 49.41, 8.71, 49.43)]
    request_gdf = [gpd.GeoSeries(index=['1'], data=vectors, crs=CRS.from_epsg(4326))]

    expected_output = gpd.GeoDataFrame(index=['1'], data={'mean': [0.5]}, geometry=vectors, crs=CRS.from_epsg(4326))

    time_range = TimeRange(end_date=date(2023, 6, 1))
    response_gdf = default_naturalness_utility.compute_vector(
        index=NaturalnessIndex.NDVI,
        aggregation_stats=['mean'],
        vectors=request_gdf,
        time_range=time_range,
        max_raster_size=30,
        resolution=90,
    )

    testing.assert_geodataframe_equal(response_gdf, expected_output, check_like=True)


def test_compute_vector_exceeds_max_raster_size_only_intersecting_bounds(
    mocked_utility_response, mock_naturalness_raster_a, default_naturalness_utility
):
    """If vector's total bounds exceeds the max_raster_size, the extent should be split into smaller bounds and bounds
    that don't intersect with the required vectors should be dropped.
    """
    vectors = [
        LineString([[8.67, 49.41], [8.68, 49.42]]),
        LineString([[8.68, 49.46], [8.69, 49.47]]),
    ]
    vectors = gpd.GeoSeries(data=vectors, crs=CRS.from_epsg(4326))

    expected_output = gpd.GeoDataFrame(data={'mean': [1.0, 0.0]}, geometry=vectors, crs=CRS.from_epsg(4326))

    request_body_c = {
        'time_range': {'start_date': '2022-06-01', 'end_date': '2023-06-01'},
        'resolution': 90.0,
        'bbox': [8.67, 49.45, 8.69, 49.47],
    }
    with (
        open(TEST_RESOURCES_DIR / 'rasters/test_raster_0-1_c_res90.tiff', 'rb') as raster_c,
    ):
        mocked_utility_response.post(
            'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_body_c)], body=raster_c.read()
        )

    response_gdf = default_naturalness_utility.compute_vector(
        index=NaturalnessIndex.NDVI,
        aggregation_stats=['mean'],
        vectors=[vectors],
        time_range=TimeRange(end_date=date(2023, 6, 1)),
        max_raster_size=26,
        resolution=90,
    )

    testing.assert_geodataframe_equal(response_gdf, expected_output, check_like=True)


def test_compute_vector_smaller_than_resolution(mock_naturalness_raster_small, default_naturalness_utility):
    """If the vector bounds are less than the raster resolution in either dimension, valid bounds (>0 dimension) should
    be created.
    """
    vectors = [LineString([[8.6790252, 49.4198277], [8.6797126, 49.4200740]])]
    request_gdf = gpd.GeoSeries(data=vectors, crs=CRS.from_epsg(4326))

    expected_output = gpd.GeoDataFrame(data={'mean': [0.0]}, geometry=vectors, crs=CRS.from_epsg(4326))

    response_gdf = default_naturalness_utility.compute_vector(
        index=NaturalnessIndex.NDVI,
        aggregation_stats=['mean'],
        vectors=[request_gdf],
        time_range=TimeRange(end_date=date(2023, 6, 1)),
    )

    testing.assert_geodataframe_equal(response_gdf, expected_output, check_like=True)


def test_compute_vector_geometries_input_not_edited(mock_naturalness_raster_a, default_naturalness_utility):
    """The input vector geometries shouldn't be edited."""
    vectors = [
        Polygon([[8.67, 49.41], [8.69, 49.41], [8.69, 49.42], [8.67, 49.42], [8.67, 49.41]]),
        Polygon([[8.67, 49.42], [8.69, 49.42], [8.69, 49.43], [8.67, 49.43], [8.67, 49.42]]),
    ]
    request_gdf = gpd.GeoSeries(index=['first', 'second'], data=vectors, crs=CRS.from_epsg(4326))

    response_gdf = default_naturalness_utility.compute_vector(
        index=NaturalnessIndex.NDVI,
        aggregation_stats=['mean'],
        vectors=[request_gdf],
        time_range=TimeRange(end_date=date(2023, 6, 1)),
    )

    testing.assert_geoseries_equal(response_gdf.geometry, request_gdf)


def test_compute_vector_multiple_aggregation_stats(mock_naturalness_raster_a, default_naturalness_utility):
    vectors = [shapely.geometry.box(8.67, 49.41, 8.69, 49.43)]
    request_gdf = gpd.GeoSeries(data=vectors, crs=CRS.from_epsg(4326))

    expected_output = gpd.GeoDataFrame(
        data={'mean': [0.5], 'min': [0.0], 'max': [1.0]}, geometry=vectors, crs=CRS.from_epsg(4326)
    )

    response_gdf = default_naturalness_utility.compute_vector(
        index=NaturalnessIndex.NDVI,
        aggregation_stats=['mean', 'min', 'max'],
        vectors=[request_gdf],
        time_range=TimeRange(end_date=date(2023, 6, 1)),
    )

    testing.assert_geodataframe_equal(response_gdf, expected_output, check_like=True)


def test_adjust_work_units_model_validate(mocked_utility_response, default_naturalness_utility):
    adjusted_units = default_naturalness_utility.adjust_work_units(units=[NATURALNESS_UNIT_A])

    assert [NaturalnessWorkUnit.model_validate(dict(u), extra='forbid') for u in adjusted_units]
