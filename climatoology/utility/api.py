import math
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Any, Callable, Generator, List, Optional

import pyproj
import rasterio
import requests
from geopandas import GeoSeries
from pydantic import BaseModel, Field, model_validator
from rasterio.merge import merge
from requests.adapters import HTTPAdapter
from sentinelhub import (
    CRS,
    BBox,
    BBoxSplitter,
    bbox_to_dimensions,
    get_utm_crs,
)
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from urllib3 import Retry

from climatoology.base.logging import get_climatoology_logger

log = get_climatoology_logger(__name__)


class TimeRange(BaseModel):
    start_date: Optional[date] = Field(
        title='Start Date',
        description='Lower bound (inclusive) of remote sensing imagery acquisition date (UTC). '
        'If not set it will be automatically set to one year before `end_date`',
        examples=['2024-01-01'],
        default=None,
    )
    end_date: date = Field(
        title='End Date',
        description='Upper bound (inclusive) of remote sensing imagery acquisition date (UTC). '
        'Defaults to the 31st December of last year.',
        examples=['2024-12-31'],
        default=date(date.today().year - 1, 12, 31),
    )

    @model_validator(mode='after')
    def check_order(self) -> 'TimeRange':
        if self.start_date is not None:
            assert self.start_date < self.end_date, 'Start date must be before end date'
        return self

    @model_validator(mode='after')
    def minus_year(self) -> 'TimeRange':
        if not self.start_date:
            self.start_date = self.end_date - timedelta(days=365)
        return self


class HealthCheck(BaseModel):
    status: str = 'ok'


class PlatformHttpUtility(ABC):
    def __init__(
        self,
        base_url: str,
        max_retries: int = 5,
    ):
        assert base_url[-1] != '/', 'The base_url must not end with a /'
        self.base_url = base_url

        retries = Retry(total=max_retries, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])

        self.session = requests.Session()

        # We have to mount http:// to overwrite the default adapters
        # noinspection HttpUrlsUsage
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        assert self.health(), 'Utility startup failed: the API is not reachable.'

    def health(self) -> bool:
        try:
            url = f'{self.base_url}/health'
            response = self.session.get(url=url)
            response.raise_for_status()
            assert response.json().get('status') == HealthCheck().status
        except Exception as e:
            log.error(f'{self.__class__.__name__} API not reachable', exc_info=e)
            return False
        return True


@contextmanager
def compute_raster(
    units: List[Any], max_workers: int, fetch_data: Callable, has_color_map: bool
) -> Generator[rasterio.DatasetReader]:
    """Generate a remote sensing-based LULC classification.

    :param has_color_map:
    :param fetch_data:
    :param max_workers:
    :param units: Areas of interest
    :return: An opened geo-tiff file within a generator. Use it as `with compute_raster(units) as lulc:`
    """
    assert len(units) > 0

    with logging_redirect_tqdm():
        with tqdm(total=len(units)) as pbar:
            slices = []

            with ThreadPoolExecutor(max_workers) as pool:
                for dataset in pool.map(fetch_data, units):
                    pbar.update()
                    pbar.set_description(f'Collecting rasters: {dataset.shape}')
                    slices.append(dataset)

                pbar.set_description(f'Merging {len(slices)} slices')
                mosaic, transform = merge(slices)

                if has_color_map:
                    first_colormap = slices[0].colormap(1)

            with rasterio.MemoryFile() as memfile:
                log.debug('Creating raster file')

                write_profile = slices[0].profile
                write_profile['transform'] = transform
                write_profile['height'] = mosaic.shape[1]
                write_profile['width'] = mosaic.shape[2]

                with memfile.open(**write_profile) as m:
                    pbar.set_description(f'Writing mosaic {mosaic.shape}')

                    if has_color_map:
                        m.write_colormap(1, first_colormap)
                    m.write(mosaic)

                    del mosaic
                    del slices

                with memfile.open() as dataset:
                    log.debug('Serving raster file')
                    yield dataset


def _make_bound_dimensions_valid(bounds: BBox, resolution: float) -> BBox:
    """Buffer `bounds` as required to ensure that both height and width of the returned bounds are greater than or equal
    to `resolution`.

    :param bounds: the input bounds to be adjusted (in WGS84)
    :param resolution: the resolution of the raster data (metres)
    :return: a `BBox` containing bounds with both dimensions >= 1
    """
    utm_crs = get_utm_crs(lng=bounds.min_x, lat=bounds.min_y)
    transformer = pyproj.Transformer.from_crs(pyproj.CRS('EPSG:4326'), pyproj.CRS(utm_crs.epsg), always_xy=True)

    # Transform all corners of the bounding box to avoid reprojection issues
    xmin, ymin = bounds.lower_left
    xmax, ymax = bounds.upper_right
    x_t, y_t = transformer.transform((xmin, xmax, xmax, xmin), (ymin, ymin, ymax, ymax))
    xmin, ymin, xmax, ymax = min(x_t), min(y_t), max(x_t), max(y_t)
    xmax = max(xmin + resolution, xmax)
    ymax = max(ymin + resolution, ymax)

    x, y = transformer.transform(
        (xmin, xmax, xmax, xmin), (ymin, ymin, ymax, ymax), direction=pyproj.enums.TransformDirection.INVERSE
    )
    return BBox((min(x), min(y), max(x), max(y)), crs=CRS.WGS84)


def generate_bounds(
    target_geometries: GeoSeries,
    resolution: float,
    max_unit_size: int = 2300,
    max_unit_area: int = None,
) -> list[BBox]:
    """Generate a list of bounding boxes for the area covered by the provided geometry, where each bounding box is
    smaller than the maximum size. The union of the returned boxes may be up to one 'pixel' larger than the input
    geometry space to avoid empty (invalid) boxes.

    :param target_geometries: The input geometries to be adjusted
    :param resolution: the resolution of the raster data (metres)
    :param max_unit_size: The maximum edge length per bounding box (pixels)
    :param max_unit_area: The maximum area per bounding box (pixels squared), defaults to max_unit_size^2
    """
    if max_unit_area:
        max_unit_size = min(max_unit_size, int(math.sqrt(max_unit_area)))
        log.debug(f'Using max_unit_size of {max_unit_size} to adjust bounds')

    # the total bounds are a np.array of length 4 and the tuple command correctly turns it into a tuple
    # noinspection PyTypeChecker
    bbox: tuple[float, float, float, float] = tuple(target_geometries.total_bounds)
    bounds = BBox(bbox, crs=CRS.WGS84)
    w, h = bbox_to_dimensions(bounds, resolution=resolution)

    if min(h, w) < 1:
        w, h = max(w, 1), max(h, 1)
        bounds = _make_bound_dimensions_valid(bounds=bounds, resolution=resolution)
        log.debug(f'The bounds were adjusted from {bbox} to {bounds}')

    h_splits = math.ceil(h / max_unit_size)
    w_splits = math.ceil(w / max_unit_size)

    split_bounds = BBoxSplitter(shape_list=[bounds], crs=CRS.WGS84, split_shape=(w_splits, h_splits)).bbox_list

    intersecting_bounds = [b for b in split_bounds if any(target_geometries.intersects(b.geometry))]
    log.debug(f'Removed {len(split_bounds) - len(intersecting_bounds)} non-overlapping bounding boxes')
    log.info(f'The geometry space was split into {len(intersecting_bounds)} bounding boxes')

    return [b.apply(lambda x, y: (round(x, 7), round(y, 7))) for b in intersecting_bounds]
