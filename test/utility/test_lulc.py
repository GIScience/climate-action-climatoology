import os
from datetime import date

import pytest
import rasterio
from rasterio.coords import BoundingBox
from requests import Response
from responses import matchers

from climatoology.utility.exception import PlatformUtilityError
from climatoology.utility.lulc import LabelDescriptor, LabelResponse, LulcUtility, LulcWorkUnit

lulc_unit_a = LulcWorkUnit(
    area_coords=(8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485),
    start_date=date(2023, 5, 1),
    end_date=date(2023, 6, 1),
)
lulc_unit_b = LulcWorkUnit(
    area_coords=(8.0859375, 47.63578359086485, 8.26171875, 47.754097979680026),
    start_date=date(2023, 5, 1),
    end_date=date(2023, 6, 1),
)


def test_lulc_when_passing_zero_units(mocked_utility_response):
    operator = LulcUtility(host='localhost', port=80, path='/')
    with pytest.raises(AssertionError):
        with operator.compute_raster([]):
            pass


def test_lulc_when_passing_single_unit(mocked_utility_response):
    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_a.tiff', 'rb') as raster:
        mocked_utility_response.post('http://localhost:80/segment/', body=raster.read())

    operator = LulcUtility(host='localhost', port=80, path='/')

    with operator.compute_raster([lulc_unit_a]) as raster:
        assert raster.shape == (1304, 1336)
        assert raster.count == 1
        assert raster.dtypes[0] == rasterio.uint8
        assert raster.bounds == BoundingBox(8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485)


def test_lulc_when_passing_multiple_units(mocked_utility_response):
    with (
        open(f'{os.path.dirname(__file__)}/../resources/test_raster_a.tiff', 'rb') as raster_a,
        open(f'{os.path.dirname(__file__)}/../resources/test_raster_b.tiff', 'rb') as raster_b,
    ):
        mocked_utility_response.post(
            'http://localhost:80/segment/',
            match=[matchers.json_params_matcher(lulc_unit_a.model_dump(mode='json'))],
            body=raster_a.read(),
        )
        mocked_utility_response.post(
            'http://localhost:80/segment/',
            match=[matchers.json_params_matcher(lulc_unit_b.model_dump(mode='json'))],
            body=raster_b.read(),
        )

    operator = LulcUtility(host='localhost', port=80, path='/')

    with operator.compute_raster([lulc_unit_a, lulc_unit_b]) as raster:
        assert raster.shape == (2605, 1336)
        assert raster.count == 1
        assert raster.dtypes[0] == rasterio.uint8
        assert raster.bounds == BoundingBox(8.0859375, 47.51720500703333, 8.26171875, 47.754097979680026)


def test_lulc_when_encountering_connection_issues(mocked_utility_response):
    response = Response()
    response.status_code = 409

    mocked_utility_response.return_value = response

    operator = LulcUtility(host='localhost', port=80, path='/')
    with pytest.raises(PlatformUtilityError):
        with operator.compute_raster([lulc_unit_a]):
            pass


def test_legend_retrieval(mocked_utility_response):
    mocked_utility_response.get(
        'http://localhost:80/segment/describe',
        json={
            'osm': {
                'class1': {
                    'name': 'class1',
                    'description': 'description',
                    'osm_ref': None,
                    'osm_filter': 'landuse=nothing',
                    'raster_value': 1,
                    'color': [255, 255, 255],
                }
            },
            'corine': {
                'class1': {
                    'name': 'class1',
                    'description': 'description',
                    'osm_ref': 'class1',
                    'osm_filter': 'landuse=nothing',
                    'raster_value': 1,
                    'color': [255, 255, 255],
                }
            },
        },
    )
    operator = LulcUtility(host='localhost', port=80, path='/')

    expected_result = LabelResponse(
        osm={
            'class1': LabelDescriptor(
                name='class1',
                description='description',
                osm_filter='landuse=nothing',
                raster_value=1,
                color=(255, 255, 255),
            )
        },
        corine={
            'class1': LabelDescriptor(
                name='class1',
                osm_ref='class1',
                description='description',
                osm_filter='landuse=nothing',
                raster_value=1,
                color=(255, 255, 255),
            )
        },
    )
    computed_result = operator.get_class_legend()

    assert expected_result == computed_result
