import logging
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
from shapely import Point, geometry
from shapely.geometry.polygon import Polygon

from climatoology.utility.api import TimeRange
from climatoology.utility.exception import PlatformUtilityError
from climatoology.utility.naturalness import NaturalnessIndex, NaturalnessUtility, NaturalnessWorkUnit

NATURALNESS_UNIT = NaturalnessWorkUnit(
    aoi=geometry.box(7.38, 47.5, 7.385, 47.52),
    time_range=TimeRange(end_date=date(2023, 6, 1)),
)


@pytest.fixture
def default_zonal_vector_response():
    return {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'id': 'first',
                'properties': {'mean': 0.5},
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[0, 0], [0.01, 0], [0.01, 0.01], [0, 0.01], [0, 0]]],
                },
            },
            {
                'type': 'Feature',
                'id': 'second',
                'properties': {'mean': 0.6},
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[0, 0], [-0.01, 0], [-0.01, -0.01], [0, -0.01], [0, 0]]],
                },
            },
        ],
    }


def test_naturalness_connection_issues(mocked_utility_response):
    response = Response()
    response.status_code = 409

    mocked_utility_response.return_value = response

    operator = NaturalnessUtility(host='localhost', port=80, path='/')
    with pytest.raises(PlatformUtilityError):
        with operator.compute_raster(index='NDVI', units=[NATURALNESS_UNIT]):
            pass


def test_compute_raster_with_invalid_inputs(mocked_utility_response):
    operator = NaturalnessUtility(host='localhost', port=80, path='/')
    with pytest.raises(AssertionError):
        with operator.compute_raster(index=NaturalnessIndex.NDVI, units=[]):
            pass


def test_compute_raster_single_unit(mocked_utility_response):
    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_c.tiff', 'rb') as raster:
        mocked_utility_response.post('http://localhost:80/NDVI/raster', body=raster.read())

    operator = NaturalnessUtility(host='localhost', port=80, path='/')

    with operator.compute_raster(index=NaturalnessIndex.NDVI, units=[NATURALNESS_UNIT]) as raster:
        assert raster.shape == (221, 42)
        assert raster.count == 1
        assert raster.dtypes[0] == rasterio.float32
        assert raster.bounds == BoundingBox(7.38, 47.5, 7.385, 47.52)


def test_compute_vector_single_unit(mocked_utility_response, default_zonal_vector_response):
    vectors = gpd.GeoSeries(
        index=['first', 'second'],
        data=[
            Polygon([[0, 0], [0.01, 0], [0.01, 0.01], [0, 0.01], [0, 0]]),
            Polygon([[0, 0], [-0.01, 0], [-0.01, -0.01], [0, -0.01], [0, 0]]),
        ],
        crs=CRS.from_epsg(4326),
    )
    mocked_utility_response.post(
        'http://localhost:80/NDVI/vector',
        json=default_zonal_vector_response,
        match=[
            matchers.json_params_matcher(
                {
                    'time_range': {
                        'start_date': '2022-06-01',
                        'end_date': '2023-06-01',
                    },
                    'vectors': {
                        'type': 'FeatureCollection',
                        'features': [
                            {
                                'id': 'second',
                                'type': 'Feature',
                                'properties': {},
                                'geometry': {
                                    'type': 'Polygon',
                                    'coordinates': [
                                        [[0.0, 0.0], [0.0, -0.01], [-0.01, -0.01], [-0.01, 0.0], [0.0, 0.0]]
                                    ],
                                },
                                'bbox': [-0.01, -0.01, 0.0, 0.0],
                            },
                            {
                                'id': 'first',
                                'type': 'Feature',
                                'properties': {},
                                'geometry': {
                                    'type': 'Polygon',
                                    'coordinates': [[[0.0, 0.0], [0.0, 0.01], [0.01, 0.01], [0.01, 0.0], [0.0, 0.0]]],
                                },
                                'bbox': [0.0, 0.0, 0.01, 0.01],
                            },
                        ],
                        'bbox': [-0.01, -0.01, 0.01, 0.01],
                    },
                    'aggregation_stats': ['mean'],
                    'resolution': 90,
                }
            )
        ],
    )

    operator = NaturalnessUtility(host='localhost', port=80, path='/')
    agg_stats = ['mean']

    time_range = TimeRange(end_date=date(2023, 6, 1))

    response_gdf = operator.compute_vector(
        index=NaturalnessIndex.NDVI, aggregation_stats=agg_stats, vectors=[vectors], time_range=time_range
    )

    expected_output = gpd.GeoDataFrame(
        index=['first', 'second'],
        data={'mean': [0.5, 0.6]},
        geometry=[
            Polygon([[0, 0], [0.01, 0], [0.01, 0.01], [0, 0.01], [0, 0]]),
            Polygon([[0, 0], [-0.01, 0], [-0.01, -0.01], [0, -0.01], [0, 0]]),
        ],
        crs=CRS.from_epsg(4326),
    )
    testing.assert_geodataframe_equal(response_gdf, expected_output)


@pytest.mark.parametrize('index_dtype', ['str', 'int', 'float'])
def test_compute_vector_retain_index_dtype(index_dtype, mocked_utility_response):
    vectors = gpd.GeoSeries(
        index=[1, 2],
        data=[
            Polygon([[0, 0], [0.01, 0], [0.01, 0.01], [0, 0.01], [0, 0]]),
            Polygon([[0, 0], [-0.01, 0], [-0.01, -0.01], [0, -0.01], [0, 0]]),
        ],
        crs=CRS.from_epsg(4326),
    )
    vectors.index = vectors.index.astype(index_dtype)

    vector_response = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'id': '1',
                'properties': {'mean': 0.5},
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[0, 0], [0.01, 0], [0.01, 0.01], [0, 0.01], [0, 0]]],
                },
            },
            {
                'type': 'Feature',
                'id': '2',
                'properties': {'mean': 0.6},
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[0, 0], [-0.01, 0], [-0.01, -0.01], [0, -0.01], [0, 0]]],
                },
            },
        ],
    }
    mocked_utility_response.post('http://localhost:80/NDVI/vector', json=vector_response)

    operator = NaturalnessUtility(host='localhost', port=80, path='/')
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


def test_compute_vector_multi_unit(mocked_utility_response):
    vectors = [
        gpd.GeoSeries(
            index=['first'],
            data=[Polygon([[0, 0], [0.01, 0], [0.01, 0.01], [0, 0.01], [0, 0]])],
            crs=CRS.from_epsg(4326),
        ),
        gpd.GeoSeries(
            index=['second'],
            data=[Polygon([[0, 0], [-0.01, 0], [-0.01, -0.01], [0, -0.01], [0, 0]])],
            crs='EPSG:4326',
        ),
    ]
    vector_response = [
        {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'id': 'first',
                    'properties': {'mean': 0.5},
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [[[0, 0], [0.01, 0], [0.01, 0.01], [0, 0.01], [0, 0]]],
                    },
                },
            ],
        },
        {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'id': 'second',
                    'properties': {'mean': 0.6},
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [[[0, 0], [-0.01, 0], [-0.01, -0.01], [0, -0.01], [0, 0]]],
                    },
                },
            ],
        },
    ]
    mocked_utility_response.add(method='POST', url='http://localhost:80/NDVI/vector', json=vector_response[0])
    mocked_utility_response.add(method='POST', url='http://localhost:80/NDVI/vector', json=vector_response[1])

    operator = NaturalnessUtility(host='localhost', port=80, path='/')
    agg_stats = ['mean']

    time_range = TimeRange(end_date=date(2023, 6, 1))

    response_gdf = operator.compute_vector(
        index=NaturalnessIndex.NDVI, aggregation_stats=agg_stats, vectors=vectors, time_range=time_range
    )

    expected_output = gpd.GeoDataFrame(
        index=['first', 'second'],
        data={'mean': [0.5, 0.6]},
        geometry=[
            Polygon([[0, 0], [0.01, 0], [0.01, 0.01], [0, 0.01], [0, 0]]),
            Polygon([[0, 0], [-0.01, 0], [-0.01, -0.01], [0, -0.01], [0, 0]]),
        ],
        crs=CRS.from_epsg(4326),
    )
    testing.assert_geodataframe_equal(response_gdf, expected_output)


def test_compute_vector_exceeds_max_raster_size(mocked_utility_response):
    vectors = [
        gpd.GeoSeries(
            index=['first', 'second'],
            data=[
                Point([-0.01, -0.01]),
                Point([0.01, 0.01]),
            ],
            crs=CRS.from_epsg(4326),
        )
    ]
    vector_response = [
        {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'id': 'first',
                    'properties': {'mean': 0.5},
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [-0.01, -0.01],
                    },
                },
            ],
        },
        {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'id': 'second',
                    'properties': {'mean': 0.6},
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [0.01, 0.01],
                    },
                },
            ],
        },
    ]
    mocked_utility_response.add(method='POST', url='http://localhost:80/NDVI/vector', json=vector_response[0])
    mocked_utility_response.add(method='POST', url='http://localhost:80/NDVI/vector', json=vector_response[1])

    operator = NaturalnessUtility(host='localhost', port=80, path='/')
    agg_stats = ['mean']

    time_range = TimeRange(end_date=date(2023, 6, 1))

    # dimensions of example vectors here: h, w = (223, 221)
    response_gdf = operator.compute_vector(
        index=NaturalnessIndex.NDVI,
        aggregation_stats=agg_stats,
        vectors=vectors,
        time_range=time_range,
        max_raster_size=150,
        resolution=10,
    )

    mocked_utility_response.assert_call_count(url='http://localhost:80/NDVI/vector', count=2)

    expected_output = gpd.GeoDataFrame(
        index=['first', 'second'],
        data={'mean': [0.5, 0.6]},
        geometry=[
            Point([-0.01, -0.01]),
            Point([0.01, 0.01]),
        ],
        crs=CRS.from_epsg(4326),
    )
    testing.assert_geodataframe_equal(response_gdf, expected_output)


@pytest.mark.parametrize(['max_unit_size', 'expected_groups'], [[200, 4], [100, 9]])
def test_split_vectors_exceeding_max_unit_size(max_unit_size, expected_groups, mocked_utility_response, caplog):
    # (h, w) of default vectors = (221, 223)
    operator = NaturalnessUtility(host='localhost', port=80, path='/')
    vectors = gpd.GeoSeries(
        data=[
            Polygon([[0, 0], [0.01, 0], [0.01, 0.01], [0, 0.01], [0, 0]]),
            Polygon([[0, 0], [-0.01, 0], [-0.01, -0.01], [0, -0.01], [0, 0]]),
            Polygon([[-0.01, -0.01], [0.01, -0.01], [0.01, 0.01], [-0.01, 0.01], [-0.01, -0.01]]),
        ],
        crs=CRS.from_epsg(4326),
    )

    expected_warning = (
        'The dimensions of at least one of the provided vector GeoSeries exceeds the max_unit_size. '
        'The returned geometries will be segmented into smaller parts to meet computation limits.'
    )

    with caplog.at_level(logging.WARNING):
        vector_groups = operator.split_vectors(vectors=[vectors], resolution=10, max_unit_size=max_unit_size)

    assert expected_warning in caplog.messages
    assert len(vector_groups) == expected_groups
