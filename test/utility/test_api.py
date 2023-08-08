import os
from unittest.mock import patch, Mock

import pytest
import rasterio
from rasterio.coords import BoundingBox
from requests import Response

from climatoology.utility.api import LulcUtilityUtility, LULCWorkUnit
from climatoology.utility.exception import PlatformUtilityException


@pytest.fixture()
def mocked_client():
    with patch('requests.Session.post') as platform_session:
        yield platform_session


unit_a = LULCWorkUnit(area_coords=(8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485),
                      start_date='2023-05-01',
                      end_date='2023-06-01')

unit_b = LULCWorkUnit(area_coords=(8.0859375, 47.63578359086485, 8.26171875, 47.754097979680026),
                      start_date='2023-05-01',
                      end_date='2023-06-01')


def test_lulc_when_passing_zero_units():
    operator = LulcUtilityUtility()
    with pytest.raises(AssertionError):
        with operator.compute_raster([]):
            pass


def test_lulc_when_passing_single_unit(mocked_client):
    with open(f'{os.path.dirname(__file__)}/test_raster_a.tiff', 'rb') as raster:
        response = Mock()
        response.status_code = 200
        response.content = raster.read()

        mocked_client.return_value = response
        operator = LulcUtilityUtility()

    with operator.compute_raster([unit_a]) as raster:
        assert raster.shape == (1304, 1336)
        assert raster.count == 1
        assert raster.dtypes[0] == rasterio.uint8
        assert raster.bounds == BoundingBox(8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485)


def test_lulc_when_passing_multiple_units(mocked_client):
    with open(f'{os.path.dirname(__file__)}/test_raster_a.tiff', 'rb') as raster_a, open(f'{os.path.dirname(__file__)}/test_raster_b.tiff', 'rb') as raster_b:
        response_a = Mock()
        response_a.status_code = 200
        response_a.content = raster_a.read()

        response_b = Mock()
        response_b.status_code = 200
        response_b.content = raster_b.read()

        mocked_client.side_effect = [response_a, response_b]
        operator = LulcUtilityUtility()

    with operator.compute_raster([unit_a, unit_b]) as raster:
        assert raster.shape == (2605, 1336)
        assert raster.count == 1
        assert raster.dtypes[0] == rasterio.uint8
        assert raster.bounds == BoundingBox(8.0859375, 47.51720500703333, 8.26171875, 47.754097979680026)


def test_lulc_when_encountering_connection_issues(mocked_client):
    response = Response()
    response.status_code = 409

    mocked_client.return_value = response

    operator = LulcUtilityUtility()
    with pytest.raises(PlatformUtilityException):
        with operator.compute_raster([unit_a]):
            pass
