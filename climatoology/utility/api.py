import logging
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from io import BytesIO
from typing import Tuple, List, ContextManager, Optional

import rasterio as rio
import requests
from pydantic import Field, BaseModel
from rasterio.merge import merge
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3 import Retry

from climatoology.utility.exception import PlatformUtilityException

log = logging.getLogger(__name__)


class FusionMode(Enum):
    """Available LULC Utility fusion modes."""
    ONLY_MODEL = 'only_model'
    ONLY_OSM = 'only_osm'
    FAVOUR_OSM = 'favour_osm'
    FAVOUR_MODEL = 'favour_model'
    MEAN_MIXIN = 'mean_mixin'


class LULCWorkUnit(BaseModel):
    """LULC area of interest."""
    area_coords: Tuple[float, float, float, float] = Field(
        description='Bounding box coordinates in WGS 84 (left, bottom, right, top)',
        examples=[[12.304687500000002,
                   48.2246726495652,
                   12.480468750000002,
                   48.3416461723746]])
    start_date: Optional[str] = Field(
        description='Lower bound (inclusive) of remote sensing imagery acquisition date (UTC). '
                    'The model uses an image stack of multiple acquisition times for predictions. '
                    'Larger time intervals will improve the prediction accuracy'
                    'If not set it will be automatically set to the week before `end_date`',
        examples=['2023-05-01'],
        default=None)
    end_date: str = Field(description='Upper bound (inclusive) of remote sensing imagery acquisition date (UTC).'
                                      "Defaults to today's date"
                                      'In case `fusion_mode` has been declared to value different than `only_model`'
                                      'the `end_date` will also be used to acquire OSM data',
                          examples=['2023-06-01'],
                          default=str(datetime.now().strftime('%Y-%m-%d')))
    threshold: float = Field(
        description='Not exceeding this value by the class prediction score results in the recognition of the result '
                    'as "unknown"',
        default=0,
        examples=[0.75],
        ge=0.0,
        le=1.0)
    fusion_mode: FusionMode = Field(description='Enables merging model results with OSM data: '
                                                '`only_model` - no fusion with OSM will take place, '
                                                '`only_osm` - displays OSM output only, '
                                                '`favour_model` - OSM will be used to fill in regions considered as '
                                                '"unknown" for the model, '
                                                '`favour_osm` - model results will be used to fill in empty OSM data, '
                                                '`mean_mixin` - model and OSM will simultaneously contribute to '
                                                'overall classification',
                                    default=FusionMode.ONLY_MODEL,
                                    examples=[FusionMode.ONLY_MODEL])


class PlatformHttpUtility(ABC):

    def __init__(self, host: str, port: int, path: str, max_retries: int = 5):
        assert path[0] == path[-1] == '/', 'The path must start and end with a /'
        self.base_url = f'http://{host}:{port}{path}'

        retries = Retry(total=max_retries,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])

        self.session = requests.Session()
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        assert self.health(), 'Utility startup failed: the API is not reachable.'

    def health(self) -> bool:
        try:
            url = f'{self.base_url}health/'
            response = self.session.get(url=url)
            response.raise_for_status()
            assert response.json().get('status') == 'ok'
        except Exception as e:
            logging.error('LULC utility not reachable', exc_info=e)
            return False
        return True


class LulcUtility(PlatformHttpUtility):

    def __init__(self, host: str, port: int, path: str, max_workers: int = 2, max_retries: int = 5):
        """A wrapper class around the LULC Utility API.

        :param host: api host
        :param port: api port
        :param path: api path
        :param max_workers: maximum number of threads to spawn for parallel requests
        :param max_retries: number of retires before giving up during connection startup
        """
        super().__init__(host, port, path, max_retries)

        self.max_workers = max_workers

    def __fetch_data(self, unit: LULCWorkUnit) -> rio.DatasetReader:
        try:
            url = f'{self.base_url}segment/'
            log.debug(f'Requesting classification from LULC Utility at {url} for region {unit.model_dump()}')
            response = self.session.post(url=url, json=unit.model_dump(mode='json'))
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
                log.debug('Creating LULC file.')

                write_profile = slices[0].profile
                write_profile['transform'] = transform
                write_profile['height'] = mosaic.shape[1]
                write_profile['width'] = mosaic.shape[2]

                with memfile.open(**write_profile) as m:
                    pbar.set_description(f'Writing mosaic {mosaic.shape}')

                    m.write(mosaic)
                    m.write_colormap(1, first_colormap)

                    del mosaic
                    del slices

                with memfile.open() as dataset:
                    log.debug('Serving LULC classification')
                    yield dataset
