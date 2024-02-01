import os

import pytest
import rasterio
import responses
from rasterio.coords import BoundingBox
from requests import Response
from responses import matchers

from climatoology.utility.api import LulcUtility, LulcWorkUnit
from climatoology.utility.exception import PlatformUtilityException


@pytest.fixture()
def mocked_client():
    with responses.RequestsMock() as rsps:
        rsps.get('http://localhost:80/health',
                 json={'status': 'ok'})
        yield rsps


unit_a = LulcWorkUnit(area_coords=(8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485),
                      start_date='2023-05-01',
                      end_date='2023-06-01')

unit_b = LulcWorkUnit(area_coords=(8.0859375, 47.63578359086485, 8.26171875, 47.754097979680026),
                      start_date='2023-05-01',
                      end_date='2023-06-01')


def test_lulc_when_passing_zero_units(mocked_client):
    operator = LulcUtility(host='localhost', port=80, path='/')
    with pytest.raises(AssertionError):
        with operator.compute_raster([]):
            pass


def test_lulc_when_passing_single_unit(mocked_client):
    with open(f'{os.path.dirname(__file__)}/../resources/test_raster_a.tiff', 'rb') as raster:
        mocked_client.post('http://localhost:80/segment/',
                           body=raster.read())

    operator = LulcUtility(host='localhost', port=80, path='/')

    with operator.compute_raster([unit_a]) as raster:
        assert raster.shape == (1304, 1336)
        assert raster.count == 1
        assert raster.dtypes[0] == rasterio.uint8
        assert raster.bounds == BoundingBox(8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485)


def test_lulc_when_passing_multiple_units(mocked_client):
    with (open(f'{os.path.dirname(__file__)}/../resources/test_raster_a.tiff', 'rb') as raster_a,
          open(f'{os.path.dirname(__file__)}/../resources/test_raster_b.tiff', 'rb') as raster_b):
        mocked_client.post('http://localhost:80/segment/',
                           match=[matchers.json_params_matcher(unit_a.model_dump(mode='json'))],
                           body=raster_a.read())
        mocked_client.post('http://localhost:80/segment/',
                           match=[matchers.json_params_matcher(unit_b.model_dump(mode='json'))],
                           body=raster_b.read())

    operator = LulcUtility(host='localhost', port=80, path='/')

    with operator.compute_raster([unit_a, unit_b]) as raster:
        assert raster.shape == (2605, 1336)
        assert raster.count == 1
        assert raster.dtypes[0] == rasterio.uint8
        assert raster.bounds == BoundingBox(8.0859375, 47.51720500703333, 8.26171875, 47.754097979680026)


def test_lulc_when_encountering_connection_issues(mocked_client):
    response = Response()
    response.status_code = 409

    mocked_client.return_value = response

    operator = LulcUtility(host='localhost', port=80, path='/')
    with pytest.raises(PlatformUtilityException):
        with operator.compute_raster([unit_a]):
            pass
