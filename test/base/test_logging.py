import logging

import rasterio
from rasterio.merge import merge

from climatoology.base.logging import filter_trace_level_logs
from test.conftest import TEST_RESOURCES_DIR


def test_filter_trace_type_logging(caplog):
    caplog.set_level(logging.DEBUG)
    with rasterio.open(TEST_RESOURCES_DIR / 'rasters/test_raster_lulc_a.tiff') as raster:
        merge([raster], masked=True)
    assert 'Color: (' in caplog.text

    caplog.clear()

    filter_trace_level_logs()
    with rasterio.open(TEST_RESOURCES_DIR / 'rasters/test_raster_lulc_a.tiff') as raster:
        merge([raster], masked=True)
    assert 'Color: (' not in caplog.text
