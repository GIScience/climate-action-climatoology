import logging
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pydantic.dataclasses import dataclass
from io import BytesIO
from typing import Tuple, List, ContextManager

import rasterio as rio
import requests
from rasterio.merge import merge
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3 import Retry

from climatoology.utility.exception import PlatformUtilityException

log = logging.getLogger(__name__)


@dataclass
class LULCWorkUnit:
    area_coords: Tuple[float, float, float, float]
    start_date: str
    end_date: str


class PlatformHttpUtility(ABC):

    def __init__(self, host: str, port: int, max_retries=5):
        self.base_url = f'http://{host}:{port}'

        retries = Retry(total=max_retries,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])

        self.session = requests.Session()
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))


class LulcUtilityUtility(PlatformHttpUtility):

    def __init__(self, host='localhost', port=4000, max_workers=2, max_retries=5, root_url='/'):
        super().__init__(host, port, max_retries)
        self.max_workers = max_workers
        self.root_url = root_url

    def __fetch_data(self, unit: LULCWorkUnit) -> rio.DatasetReader:
        try:
            response = self.session.post(f'{self.base_url}{self.root_url}segment/', json=unit.__dict__)
            response.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            raise PlatformUtilityException('Connection to utility cannot be established') from e
        except requests.exceptions.HTTPError as e:
            raise PlatformUtilityException('Query failed due to an error') from e
        else:
            return rio.open(BytesIO(response.content), mode='r')

    @contextmanager
    def compute_raster(self, units: List[LULCWorkUnit]) -> ContextManager[rio.DatasetReader]:
        """Generate a remote sensing-based LULC classification.

        :param units: Areas of interest
        :return: An opened geo-tiff file within a context manager. Use it as `with compute_raster(units) as lulc:`
        """
        assert len(units) > 0

        with tqdm(total=len(units)) as pbar:
            slices = []

            with ThreadPoolExecutor(self.max_workers) as pool:
                for dataset in pool.map(self.__fetch_data, units):
                    pbar.update()
                    pbar.set_description(f'Collecting rasters: {dataset.shape}')
                    slices.append(dataset)

                pbar.set_description(f'Merging {len(slices)} slices')
                mosaic, transform = merge(slices)
                first_colormap = slices[0].colormap(1)

            with rio.MemoryFile() as memfile:
                with memfile.open(
                        driver='GTiff',
                        height=mosaic.shape[1], width=mosaic.shape[2],
                        transform=transform,
                        count=1,
                        dtype=rio.dtypes.uint8,
                        crs=rio.CRS.from_string('EPSG:4326'),
                        nodata=None
                ) as m:
                    pbar.set_description(f'Writing mosaic {mosaic.shape}')

                    m.write(mosaic)
                    m.write_colormap(1, first_colormap)

                    del mosaic
                    del slices

                with memfile.open() as dataset:
                    yield dataset
