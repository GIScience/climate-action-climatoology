import pytest
from responses import matchers

from climatoology.utility.naturalness import NaturalnessUtility
from test.conftest import TEST_RESOURCES_DIR


@pytest.fixture
def default_naturalness_utility():
    yield NaturalnessUtility(base_url='http://localhost')


@pytest.fixture
def mock_naturalness_raster_a(mocked_utility_response):
    request_body = {
        'time_range': {'start_date': '2022-06-01', 'end_date': '2023-06-01'},
        'resolution': 90.0,
        'bbox': [8.67, 49.41, 8.69, 49.43],
    }

    with open(TEST_RESOURCES_DIR / 'rasters/test_raster_0-1_a_res90.tiff', 'rb') as raster:
        mocked_utility_response.post(
            'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_body)], body=raster.read()
        )
        yield mocked_utility_response


@pytest.fixture
def mock_naturalness_raster_b(mocked_utility_response):
    request_body = {
        'time_range': {'start_date': '2022-06-01', 'end_date': '2023-06-01'},
        'resolution': 90.0,
        'bbox': [8.69, 49.41, 8.71, 49.43],
    }

    with open(TEST_RESOURCES_DIR / 'rasters/test_raster_0-1_b_res90.tiff', 'rb') as raster:
        mocked_utility_response.post(
            'http://localhost/NDVI/raster', match=[matchers.json_params_matcher(request_body)], body=raster.read()
        )
        yield mocked_utility_response


@pytest.fixture
def mock_naturalness_raster_small(mocked_utility_response):
    expected_bounds = [8.6790199, 49.4198258, 8.6802661, 49.4206388]
    request_body_small = {
        'time_range': {'start_date': '2022-06-01', 'end_date': '2023-06-01'},
        'resolution': 90.0,
        'bbox': expected_bounds,
    }
    with open(TEST_RESOURCES_DIR / 'rasters/test_raster_small_res90.tiff', 'rb') as raster_small:
        mocked_utility_response.post(
            'http://localhost/NDVI/raster',
            match=[matchers.json_params_matcher(request_body_small)],
            body=raster_small.read(),
        )
        yield mocked_utility_response
