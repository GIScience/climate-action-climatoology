import logging
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Any, ContextManager, Iterable, List, Optional, Tuple

import rasterio
import requests
from pydantic import BaseModel, Field, model_validator
from rasterio.merge import merge
from requests.adapters import HTTPAdapter
from sentinelhub import CRS, BBox, BBoxSplitter, bbox_to_dimensions
from tqdm import tqdm
from urllib3 import Retry

log = logging.getLogger(__name__)


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
        host: str,
        port: int,
        path: str,
        max_retries: int = 5,
    ):
        assert path[0] == path[-1] == '/', 'The path must start and end with a /'
        self.base_url = f'http://{host}:{port}{path}'

        retries = Retry(total=max_retries, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])

        self.session = requests.Session()
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        assert self.health(), 'Utility startup failed: the API is not reachable.'

    def health(self) -> bool:
        try:
            url = f'{self.base_url}health'
            response = self.session.get(url=url)
            response.raise_for_status()
            assert response.json().get('status') == HealthCheck().status
        except Exception as e:
            log.error(f'{self.__class__.__name__} API not reachable', exc_info=e)
            return False
        return True


@contextmanager
def compute_raster(
    units: List[Any], max_workers: int, fetch_data: callable, has_color_map: bool
) -> ContextManager[rasterio.DatasetReader]:
    """Generate a remote sensing-based LULC classification.

    :param has_color_map:
    :param fetch_data:
    :param max_workers:
    :param units: Areas of interest
    :return: An opened geo-tiff file within a context manager. Use it as `with compute_raster(units) as lulc:`
    """
    assert len(units) > 0

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
            log.debug('Creating LULC file.')

            write_profile = slices[0].profile
            write_profile['transform'] = transform
            write_profile['height'] = mosaic.shape[1]
            write_profile['width'] = mosaic.shape[2]

            with memfile.open(**write_profile) as m:
                pbar.set_description(f'Writing mosaic {mosaic.shape}')

                m.write(mosaic)
                if has_color_map:
                    m.write_colormap(1, first_colormap)

                del mosaic
                del slices

            with memfile.open() as dataset:
                log.debug('Serving LULC classification')
                yield dataset


def adjust_bounds(
    bbox: Tuple[float, float, float, float],
    max_unit_size: int = 2300,
    max_unit_area: int = None,
    resolution: int = 10,
) -> list[BBox]:
    """Adjust the given bbox to be a list of bounds within a maximum size.

    :param bbox: The input bounding box to be adjusted
    :param max_unit_size: The maximum edge length per bounding box (pixels)
    :param max_unit_area: The maximum area per bounding box (pixels squared), defaults to max_unit_size^2
    :param resolution: the resolution of the raster data (metres)
    """
    if not max_unit_area:
        max_unit_area = max_unit_size * max_unit_size

    def flatten(iterable: Iterable[Any]) -> List[Any]:
        """Convert nested iterables to single level list."""
        for outer_i in iterable:
            if isinstance(outer_i, list):
                for inner_i in flatten(outer_i):
                    yield inner_i
            else:
                yield outer_i

    def split(
        bounds: Tuple[float, float, float, float], max_edge_length: int, max_area: int, pixel_edge_length: int
    ) -> list[BBox | Any]:
        """Split bounds into Bboxes with a given maximum unit size."""
        bounds = BBox(bounds, crs=CRS.WGS84)
        h, w = bbox_to_dimensions(bounds, resolution=pixel_edge_length)
        if h > max_edge_length or w > max_edge_length or h * w > max_area:
            return [
                split(b, max_edge_length=max_edge_length, max_area=max_area, pixel_edge_length=pixel_edge_length)
                for b in BBoxSplitter([bounds], CRS.WGS84, (2, 2)).bbox_list
            ]
        else:
            return [bounds]

    return list(
        flatten(split(bounds=bbox, max_edge_length=max_unit_size, max_area=max_unit_area, pixel_edge_length=resolution))
    )
