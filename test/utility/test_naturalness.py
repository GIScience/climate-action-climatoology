import os
from datetime import date

import geopandas as gpd
import pandas as pd
import pytest
import rasterio
from geopandas import testing
from pandas.testing import assert_index_equal
from pyproj import CRS
from rasterio.coords import BoundingBox
from requests import Response
from responses import matchers
from shapely import LineString, Point, geometry
from shapely.geometry.polygon import Polygon

from climatoology.utility.api import TimeRange
from climatoology.utility.exception import PlatformUtilityError
from climatoology.utility.naturalness import NaturalnessIndex, NaturalnessUtility, NaturalnessWorkUnit

NATURALNESS_UNIT = NaturalnessWorkUnit(
    aoi=geometry.box(7.38, 47.5, 7.385, 47.52),
    time_range=TimeRange(end_date=date(2023, 6, 1)),
)


def test_naturalness_connection_issues(mocked_utility_response):
    response = Response()
    response.status_code = 409

    mocked_utility_response.return_value = response

    operator = NaturalnessUtility(base_url='http://localhost')
    with pytest.raises(PlatformUtilityError):
        with operator.compute_raster(index='NDVI', units=[NATURALNESS_UNIT]):
            pass


def test_compute_raster_with_invalid_inputs(mocked_utility_response):
    operator = NaturalnessUtility(base_url='http://localhost')
    with pytest.raises(AssertionError):
        with operator.compute_raster(index=NaturalnessIndex.NDVI, units=[]):
            pass


def test_compute_raster_single_unit(mocked_utility_response):
    request_body = {
        'time_range': {'start_date': '2022-06-01', 'end_date': '2023-06-01'},
        'resolution': 90.0,
        'bbox': [7.38, 47.5, 7.385, 47.52],
    }
    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_c.tiff', 'rb') as raster:
        mocked_utility_response.post(
            'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_body)], body=raster.read()
        )

    operator = NaturalnessUtility(base_url='http://localhost')
    with operator.compute_raster(index=NaturalnessIndex.NDVI, units=[NATURALNESS_UNIT]) as raster:
        assert raster.shape == (221, 42)
        assert raster.count == 1
        assert raster.dtypes[0] == rasterio.float32
        assert raster.bounds == BoundingBox(7.38, 47.5, 7.385, 47.52)


def test_compute_raster_multi_unit(mocked_utility_response):
    units = [
        NaturalnessWorkUnit(
            aoi=geometry.box(7.38, 47.5, 7.381, 47.51),
            time_range=TimeRange(end_date=date(2023, 6, 1)),
        ),
        NaturalnessWorkUnit(
            aoi=geometry.box(7.382, 47.51, 7.383, 47.52),
            time_range=TimeRange(end_date=date(2023, 6, 1)),
        ),
    ]

    request_body_a = units[0].model_dump(mode='json', exclude={'aoi'})
    request_body_a['bbox'] = list(units[0].aoi.bounds)
    request_body_b = units[1].model_dump(mode='json', exclude={'aoi'})
    request_body_b['bbox'] = list(units[1].aoi.bounds)

    with (
        open(f'{os.path.dirname(__file__)}/../resources/test_raster_a.tiff', 'rb') as raster_a,
        open(f'{os.path.dirname(__file__)}/../resources/test_raster_b.tiff', 'rb') as raster_b,
    ):
        mocked_utility_response.post(
            'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_body_a)], body=raster_a.read()
        )
        mocked_utility_response.post(
            'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_body_b)], body=raster_b.read()
        )

    operator = NaturalnessUtility(base_url='http://localhost')

    with operator.compute_raster(index=NaturalnessIndex.NDVI, units=units):
        mocked_utility_response.assert_call_count(url='http://localhost/NDVI/raster', count=2)


def test_compute_raster_exceeds_max_raster_size(mocked_utility_response):
    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_c.tiff', 'rb') as raster:
        response_raster = raster.read()

    request_a = {
        'time_range': {'start_date': '2022-06-01', 'end_date': '2023-06-01'},
        'resolution': 90.0,
        'bbox': [7.38, 47.5, 7.385, 47.510000000000005],
    }
    request_b = {
        'time_range': {'start_date': '2022-06-01', 'end_date': '2023-06-01'},
        'resolution': 90.0,
        'bbox': [7.38, 47.510000000000005, 7.385, 47.52],
    }
    mocked_utility_response.post(
        'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_a)], body=response_raster
    )
    mocked_utility_response.post(
        'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_b)], body=response_raster
    )

    operator = NaturalnessUtility(base_url='http://localhost')

    # default unit dimensions (25, 5)
    with operator.compute_raster(index=NaturalnessIndex.NDVI, units=[NATURALNESS_UNIT], max_unit_size=15) as raster:
        mocked_utility_response.assert_call_count(url='http://localhost/NDVI/raster', count=2)


def test_compute_raster_smaller_than_resolution(mocked_utility_response):
    request_body = {
        'time_range': {'start_date': '2022-06-01', 'end_date': '2023-06-01'},
        'resolution': 1000.0,
        'bbox': [7.380000000000001, 47.5, 7.392664329866816, 47.520107702971806],
    }
    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_c.tiff', 'rb') as raster:
        mocked_utility_response.post(
            'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_body)], body=raster.read()
        )

    operator = NaturalnessUtility(base_url='http://localhost')
    unit = NaturalnessWorkUnit(
        aoi=geometry.box(7.38, 47.5, 7.385, 47.52),
        time_range=TimeRange(end_date=date(2023, 6, 1)),
        resolution=1000,
    )

    with operator.compute_raster(index=NaturalnessIndex.NDVI, units=[unit]) as raster:
        mocked_utility_response.assert_call_count(url='http://localhost/NDVI/raster', count=1)


def test_compute_vector_single_unit(mocked_utility_response):
    vectors = gpd.GeoSeries(
        index=['first', 'second'],
        data=[
            Polygon([[7.380516, 47.508434], [7.380516, 47.507434], [7.381516, 47.507434]]),
            Polygon([[7.382516, 47.508434], [7.382516, 47.507434], [7.383516, 47.507434]]),
        ],
        crs=CRS.from_epsg(4326),
    )

    request_body = {
        'time_range': {
            'start_date': '2022-06-01',
            'end_date': '2023-06-01',
        },
        'bbox': [7.380516, 47.507434, 7.383516, 47.508434],
        'resolution': 90,
    }

    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_c.tiff', 'rb') as raster:
        mocked_utility_response.post(
            'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_body)], body=raster.read()
        )

    operator = NaturalnessUtility(base_url='http://localhost')
    agg_stats = ['mean']

    time_range = TimeRange(end_date=date(2023, 6, 1))

    response_gdf = operator.compute_vector(
        index=NaturalnessIndex.NDVI, aggregation_stats=agg_stats, vectors=[vectors], time_range=time_range
    )

    expected_output = gpd.GeoDataFrame(
        index=['first', 'second'],
        data={'mean': [0.149369, 0.045897]},
        geometry=[
            Polygon([[7.380516, 47.508434], [7.380516, 47.507434], [7.381516, 47.507434]]),
            Polygon([[7.382516, 47.508434], [7.382516, 47.507434], [7.383516, 47.507434]]),
        ],
        crs=CRS.from_epsg(4326),
    )
    testing.assert_geodataframe_equal(response_gdf, expected_output, check_like=True)


@pytest.mark.parametrize('index_dtype', ['str', 'int', 'float'])
def test_compute_vector_retain_index_dtype(index_dtype, mocked_utility_response):
    vectors = gpd.GeoSeries(
        index=[1, 2],
        data=[
            Polygon([[7.380516, 47.508434], [7.380516, 47.507434], [7.381516, 47.507434]]),
            Polygon([[7.382516, 47.508434], [7.382516, 47.507434], [7.383516, 47.507434]]),
        ],
        crs=CRS.from_epsg(4326),
    )
    vectors.index = vectors.index.astype(index_dtype)

    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_c.tiff', 'rb') as raster:
        mocked_utility_response.post('http://localhost/NDVI/raster', body=raster.read())

    operator = NaturalnessUtility(base_url='http://localhost')
    agg_stats = ['mean']

    time_range = TimeRange(end_date=date(2023, 6, 1))

    response_gdf = operator.compute_vector(
        index=NaturalnessIndex.NDVI, aggregation_stats=agg_stats, vectors=[vectors], time_range=time_range
    )

    match index_dtype:
        case 'str':
            assert_index_equal(response_gdf.index, pd.Index(['1', '2']))
        case 'int':
            assert_index_equal(response_gdf.index, pd.Index([1, 2]))
        case 'float':
            assert_index_equal(response_gdf.index, pd.Index([1.0, 2.0]))


def test_compute_vector_exceeds_max_raster_size(mocked_utility_response):
    vectors = [
        gpd.GeoSeries(
            index=['1'],
            data=[LineString([[7.380516, 47.508434], [7.384516, 47.518434]])],
            crs=CRS.from_epsg(4326),
        )
    ]

    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_c.tiff', 'rb') as raster:
        response_raster = raster.read()

    request_a = {
        'time_range': {'start_date': '2022-06-01', 'end_date': '2023-06-01'},
        'resolution': 10.0,
        'bbox': [7.380516, 47.508434, 7.384516, 47.513434000000004],
    }
    mocked_utility_response.post(
        'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_a)], body=response_raster
    )
    request_b = {
        'time_range': {'start_date': '2022-06-01', 'end_date': '2023-06-01'},
        'resolution': 10.0,
        'bbox': [7.380516, 47.513434000000004, 7.384516, 47.518434],
    }
    mocked_utility_response.post(
        'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_b)], body=response_raster
    )

    operator = NaturalnessUtility(base_url='http://localhost')
    agg_stats = ['mean']

    time_range = TimeRange(end_date=date(2023, 6, 1))

    # dimensions of example vectors here: h, w = (111, 32)
    response_gdf = operator.compute_vector(
        index=NaturalnessIndex.NDVI,
        aggregation_stats=agg_stats,
        vectors=vectors,
        time_range=time_range,
        max_raster_size=100,
        resolution=10,
    )

    mocked_utility_response.assert_call_count(url='http://localhost/NDVI/raster', count=2)

    expected_output = gpd.GeoDataFrame(
        index=['1'],
        data={'mean': [0.15999547640482584]},
        geometry=[LineString([[7.380516, 47.508434], [7.384516, 47.518434]])],
        crs=CRS.from_epsg(4326),
    )
    testing.assert_geodataframe_equal(response_gdf, expected_output, check_like=True)


@pytest.mark.parametrize(['max_unit_size', 'expected_groups'], [[200, 1], [20, 2]])
def test_compute_vector_exceeds_max_raster_size_only_intersecting_bounds(
    max_unit_size, expected_groups, mocked_utility_response, caplog
):
    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_c.tiff', 'rb') as raster:
        mocked_utility_response.post('http://localhost/NDVI/raster', body=raster.read())

    # dimensions of example vectors here: h, w = (111, 32)
    operator = NaturalnessUtility(base_url='http://localhost')
    vectors = gpd.GeoSeries(
        data=[
            Point([7.380516, 47.508434]),
            Point([7.384516, 47.518434]),
        ],
        crs=CRS.from_epsg(4326),
    )

    _ = operator.compute_vector(
        index=NaturalnessIndex.NDVI,
        aggregation_stats=['median'],
        vectors=[vectors],
        time_range=TimeRange(end_date=date(2023, 6, 1)),
        max_raster_size=max_unit_size,
        resolution=10,
    )

    mocked_utility_response.assert_call_count(url='http://localhost/NDVI/raster', count=expected_groups)


def test_compute_vector_smaller_than_resolution(mocked_utility_response):
    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_c.tiff', 'rb') as raster:
        request_body = {
            'time_range': {'start_date': '2022-06-01', 'end_date': '2023-06-01'},
            'resolution': 90.0,
            'bbox': [7.380516, 47.50843399999999, 7.381685830456872, 47.50926036293937],
        }
        mocked_utility_response.post(
            'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_body)], body=raster.read()
        )

    operator = NaturalnessUtility(base_url='http://localhost')
    vectors = gpd.GeoSeries(
        data=[Point([7.380516, 47.508434])],
        crs=CRS.from_epsg(4326),
    )

    response_gdf = operator.compute_vector(
        index=NaturalnessIndex.NDVI,
        aggregation_stats=['mean'],
        vectors=[vectors],
        time_range=TimeRange(end_date=date(2023, 6, 1)),
    )

    expected_output = gpd.GeoDataFrame(
        data={'mean': [0.262496]},
        geometry=[Point([7.380516, 47.508434])],
        crs=CRS.from_epsg(4326),
    )
    testing.assert_geodataframe_equal(response_gdf, expected_output, check_like=True)


def test_compute_vector_multiple_aggregation_stats(mocked_utility_response):
    vectors = gpd.GeoSeries(
        index=['1'],
        data=[
            Polygon([[7.380516, 47.508434], [7.380516, 47.507434], [7.381516, 47.507434]]),
        ],
        crs=CRS.from_epsg(4326),
    )

    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_c.tiff', 'rb') as raster:
        mocked_utility_response.post('http://localhost/NDVI/raster', body=raster.read())

    operator = NaturalnessUtility(base_url='http://localhost')
    agg_stats = ['mean', 'max']

    time_range = TimeRange(end_date=date(2023, 6, 1))

    response_gdf = operator.compute_vector(
        index=NaturalnessIndex.NDVI, aggregation_stats=agg_stats, vectors=[vectors], time_range=time_range
    )

    expected_output = gpd.GeoDataFrame(
        index=['1'],
        data={'mean': [0.149369], 'max': [0.3697051703929901]},
        geometry=[
            Polygon([[7.380516, 47.508434], [7.380516, 47.507434], [7.381516, 47.507434]]),
        ],
        crs=CRS.from_epsg(4326),
    )
    testing.assert_geodataframe_equal(response_gdf, expected_output, check_like=True)


def test_adjust_work_units_model_validate(mocked_utility_response):
    operator = NaturalnessUtility(base_url='http://localhost')

    adjusted_units = operator.adjust_work_units(units=[NATURALNESS_UNIT])

    assert [NaturalnessWorkUnit.model_validate(dict(u), extra='forbid') for u in adjusted_units]
