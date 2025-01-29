import datetime
import os

import geopandas as gpd
import pytest
import rasterio
from pyproj import CRS
from rasterio.coords import BoundingBox
from requests import Response
from responses import matchers
from shapely.geometry.polygon import Polygon

from climatoology.utility.Naturalness import NaturalnessWorkUnit, NaturalnessUtility, Index
from climatoology.utility.api import TimeRange
from climatoology.utility.exception import PlatformUtilityException

NATURALNESS_UNIT_A = NaturalnessWorkUnit(
    bbox=(7.38, 47.5, 7.385, 47.52),
    time_range=TimeRange(end_date=datetime.date(2023, 6, 1)),
)


@pytest.fixture
def default_zonal_vector_response():
    return {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'properties': {'mean': 0.5},
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[0, 0], [0.1, 0], [0.1, 0.1], [0, 0.1], [0, 0]]],
                },
            },
            {
                'type': 'Feature',
                'properties': {'mean': 0.6},
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[0, 0], [-0.1, 0], [-0.1, -0.1], [0, -0.1], [0, 0]]],
                },
            },
        ],
    }


def test_naturalness_connection_issues(mocked_utility_response):
    response = Response()
    response.status_code = 409

    mocked_utility_response.return_value = response

    operator = NaturalnessUtility(host='localhost', port=80, path='/')
    with pytest.raises(PlatformUtilityException):
        with operator.compute_raster(index='NDVI', units=[NATURALNESS_UNIT_A]):
            pass


def test_naturalness_raster_with_invalid_inputs(mocked_utility_response):
    operator = NaturalnessUtility(host='localhost', port=80, path='/')
    with pytest.raises(AssertionError):
        with operator.compute_raster(index=Index.NDVI, units=[]):
            pass


def test_naturalness_raster_single_unit(mocked_utility_response):
    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_c.tiff', 'rb') as raster:
        mocked_utility_response.post('http://localhost:80/NDVI/raster/', body=raster.read())

    operator = NaturalnessUtility(host='localhost', port=80, path='/')

    with operator.compute_raster(index=Index.NDVI, units=[NATURALNESS_UNIT_A]) as raster:
        assert raster.shape == (221, 42)
        assert raster.count == 1
        assert raster.dtypes[0] == rasterio.float32
        assert raster.bounds == BoundingBox(7.38, 47.5, 7.385, 47.52)


def test_naturalness_vector_single_unit(mocked_utility_response, default_zonal_vector_response):
    vectors = gpd.GeoSeries(
        data=[
            Polygon([[0, 0], [0.1, 0], [0.1, 0.1], [0, 0.1], [0, 0]]),
            Polygon([[0, 0], [-0.1, 0], [-0.1, -0.1], [0, -0.1], [0, 0]]),
        ],
        crs=CRS.from_epsg(4326),
    )
    mocked_utility_response.post(
        'http://localhost:80/NDVI/vector/',
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
                                'id': '1',
                                'type': 'Feature',
                                'properties': {},
                                'geometry': {
                                    'type': 'Polygon',
                                    'coordinates': [[[0.0, 0.0], [0.0, -0.1], [-0.1, -0.1], [-0.1, 0.0], [0.0, 0.0]]],
                                },
                                'bbox': [-0.1, -0.1, 0.0, 0.0],
                            },
                            {
                                'id': '0',
                                'type': 'Feature',
                                'properties': {},
                                'geometry': {
                                    'type': 'Polygon',
                                    'coordinates': [[[0.0, 0.0], [0.0, 0.1], [0.1, 0.1], [0.1, 0.0], [0.0, 0.0]]],
                                },
                                'bbox': [0.0, 0.0, 0.1, 0.1],
                            },
                        ],
                        'bbox': [-0.1, -0.1, 0.1, 0.1],
                    },
                    'aggregation_stats': ['mean'],
                }
            )
        ],
    )

    operator = NaturalnessUtility(host='localhost', port=80, path='/')
    agg_stats = ['mean']

    time_range = TimeRange(end_date=datetime.date(2023, 6, 1))

    response_gdf = operator.compute_vector(
        index=Index.NDVI, aggregation_stats=agg_stats, vectors=[vectors], time_range=time_range
    )
    assert isinstance(response_gdf, gpd.GeoDataFrame)


@pytest.mark.parametrize(['max_unit_size', 'expected_groups'], [[3000, 1], [2000, 4], [1000, 16]])
def test_adjust_vector_groups_above_limit(
    max_unit_size, expected_groups, default_zonal_vector_response, mocked_utility_response
):
    # (h, w) of default vectors = (2229, 2214)
    operator = NaturalnessUtility(host='localhost', port=80, path='/')
    vectors = gpd.GeoSeries(
        data=[
            Polygon([[0, 0], [0.1, 0], [0.1, 0.1], [0, 0.1], [0, 0]]),
            Polygon([[0, 0], [-0.1, 0], [-0.1, -0.1], [0, -0.1], [0, 0]]),
        ],
        crs=CRS.from_epsg(4326),
    )

    vector_groups = operator.adjust_vectors(vectors=[vectors], max_unit_size=max_unit_size)
    assert len(vector_groups) == expected_groups
