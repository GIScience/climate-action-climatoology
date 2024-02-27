import logging
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, date
from enum import Enum
from io import BytesIO
from typing import Optional, Tuple, List, ContextManager, Dict

import rasterio as rio
import requests
from pydantic import BaseModel, Field, model_validator, confloat
from rasterio.merge import merge
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3 import Retry

from climatoology.utility.exception import PlatformUtilityException

log = logging.getLogger(__name__)


class HealthCheck(BaseModel):
    status: str = 'ok'


class FusionMode(Enum):
    """Available LULC Utility fusion modes."""

    ONLY_MODEL = 'only_model'
    ONLY_OSM = 'only_osm'
    FAVOUR_OSM = 'favour_osm'
    FAVOUR_MODEL = 'favour_model'
    MEAN_MIXIN = 'mean_mixin'


class LabelDescriptor(BaseModel):
    """Segmentation label definition."""

    name: str = Field(title='Name', description='Name of the segmentation label.', examples=['Forest'])
    description: Optional[str] = Field(
        title='Description',
        description='A concise label description or caption.',
        examples=['Areas with a tree cover of more than 80% and a continuous area of more than 25ha'],
        default=None,
    )
    osm_filter: Optional[str] = Field(
        title='OSM Filter',
        description='The OSM filter statement that will extract all elements that fit '
        'the description of this label.',
        examples=['landuse=forest or natural=wood'],
        default=None,
    )
    raster_value: int = Field(
        title='Raster Value', description='The numeric value in the raster that represents this label.', examples=[1]
    )
    color: Tuple[int, int, int] = Field(
        title='Color Hex-Code', description='The RGB-color values of the label', examples=[(255, 0, 0)]
    )


class LulcWorkUnit(BaseModel):
    """LULC area of interest."""

    area_coords: Tuple[
        confloat(ge=-180, le=180), confloat(ge=-90, le=90), confloat(ge=-180, le=180), confloat(ge=-90, le=90)
    ] = Field(
        title='Area Coordinates',
        description='Bounding box coordinates in WGS 84 (west, south, east, north)',
        examples=[
            [
                12.304687500000002,
                48.2246726495652,
                12.480468750000002,
                48.3416461723746,
            ]
        ],
    )
    start_date: Optional[date] = Field(
        title='Start Date',
        description='Lower bound (inclusive) of remote sensing imagery acquisition date (UTC). '
        'The model uses an image stack of multiple acquisition times for predictions. '
        'Larger time intervals will improve the prediction accuracy'
        'If not set it will be automatically set to the week before `end_date`',
        examples=['2023-05-01'],
        default=None,
    )
    end_date: date = Field(
        title='End Date',
        description='Upper bound (inclusive) of remote sensing imagery acquisition date (UTC).'
        "Defaults to today's date"
        'In case `fusion_mode` has been declared to value different than `only_model`'
        'the `end_date` will also be used to acquire OSM data',
        examples=['2023-06-01'],
        default=datetime.now().date(),
    )
    threshold: confloat(ge=0.0, le=1.0) = Field(
        title='Threshold',
        description='Not exceeding this value by the class prediction score results in the recognition of the result '
        'as "unknown"',
        default=0,
        examples=[0.75],
    )
    fusion_mode: FusionMode = Field(
        title='Fusion Mode',
        description='Enables merging model results with OSM data: '
        '`only_model` - no fusion with OSM will take place, '
        '`only_osm` - displays OSM output only, '
        '`favour_model` - OSM will be used to fill in regions considered as '
        '"unknown" for the model, '
        '`favour_osm` - model results will be used to fill in empty OSM data, '
        '`mean_mixin` - model and OSM will simultaneously contribute to '
        'overall classification',
        default=FusionMode.ONLY_MODEL,
        examples=[FusionMode.ONLY_MODEL],
    )

    @model_validator(mode='after')
    def minus_week(self) -> 'LulcWorkUnit':
        if not self.start_date:
            self.start_date = self.end_date - timedelta(days=7)
        return self


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


class LulcUtility(PlatformHttpUtility):
    def __init__(
        self,
        host: str,
        port: int,
        path: str,
        max_workers: int = 2,
        max_retries: int = 5,
    ):
        """A wrapper class around the LULC Utility API.

        :param host: api host
        :param port: api port
        :param path: api path
        :param max_workers: maximum number of threads to spawn for parallel requests
        :param max_retries: number of retires before giving up during connection startup
        """
        super().__init__(host, port, path, max_retries)

        self.max_workers = max_workers

    def __fetch_data(self, unit: LulcWorkUnit) -> rio.DatasetReader:
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
    def compute_raster(self, units: List[LulcWorkUnit]) -> ContextManager[rio.DatasetReader]:
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

    def get_class_legend(self) -> Dict[str, LabelDescriptor]:
        url = f'{self.base_url}segment/describe'
        response = self.session.get(url=url)
        response.raise_for_status()

        return {name: LabelDescriptor(**label) for name, label in response.json().items()}
