import typing
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from io import BytesIO
from typing import Dict, Generator, List, Optional, Tuple, Union

import rasterio
import rasterio as rio
import requests
import shapely
from geopandas import GeoSeries
from pydantic import BaseModel, Field, model_validator
from pydantic_shapely import GeometryField
from shapely import MultiPolygon, Polygon, geometry

from climatoology.base.logging import get_climatoology_logger
from climatoology.utility.api import PlatformHttpUtility, compute_raster, generate_bounds
from climatoology.utility.exception import PlatformUtilityError

log = get_climatoology_logger(__name__)


class FusionMode(Enum):
    """Available LULC Utility fusion modes."""

    ONLY_MODEL = 'only_model'
    ONLY_OSM = 'only_osm'
    FAVOUR_OSM = 'favour_osm'
    FAVOUR_MODEL = 'favour_model'
    MEAN_MIXIN = 'mean_mixin'
    ONLY_CORINE = 'only_corine'
    HARMONIZED_CORINE = 'harmonized_corine'


class LabelDescriptor(BaseModel):
    """Segmentation label definition."""

    name: str = Field(
        title='Name',
        description='Name of the segmentation label.',
        examples=['Forest'],
    )
    osm_ref: Optional[str] = Field(
        title='OSM Reference',
        description='Name of matching OSM label used during harmonization process',
        examples=['Forest'],
        default=None,
    )
    description: Optional[str] = Field(
        title='Description',
        description='A concise label description or caption.',
        examples=['Areas with a tree cover of more than 80% and a continuous area of more than 25ha'],
        default=None,
    )
    osm_filter: Optional[str] = Field(
        title='OSM Filter',
        description='The OSM filter statement that will extract all elements that fit the description of this label.',
        examples=['landuse=forest or natural=wood'],
        default=None,
    )
    raster_value: int = Field(
        title='Raster Value',
        description='The numeric value in the raster that represents this label.',
        examples=[1],
    )
    color: Tuple[int, int, int] = Field(
        title='Color',
        description='The RGB-color values of the label',
        examples=[(255, 0, 0)],
    )


class LabelResponse(BaseModel):
    osm: Dict[str, LabelDescriptor] = Field(
        title='OSM Labels',
        description='Labels of classes present in the osm derived data.',
        examples=[
            {
                'corine': LabelDescriptor(
                    name='unknown',
                    osm_filter=None,
                    color=(0, 0, 0),
                    description='Class Unknown',
                    raster_value=0,
                )
            }
        ],
    )
    corine: Dict[str, LabelDescriptor] = Field(
        title='CORINE Labels',
        description='Labels of classes present in the corine derived data.',
        examples=[
            {
                'corine': LabelDescriptor(
                    name='unknown',
                    osm_filter=None,
                    color=(0, 0, 0),
                    description='Class Unknown',
                    raster_value=0,
                )
            }
        ],
    )


class LulcWorkUnit(BaseModel):
    """LULC area of interest."""

    aoi: typing.Annotated[
        Union[Polygon, MultiPolygon],
        GeometryField(),
        Field(
            title='Area of interest',
            description='The area of interest in WGS84 to request LULC data from. Note that the request will be roughly '
            'limited to the geometry but filled with no-data to fit the bounds.',
            examples=[
                shapely.to_geojson(
                    geometry.box(
                        12.304687500000002,
                        48.2246726495652,
                        12.480468750000002,
                        48.3416461723746,
                    )
                )
            ],
        ),
    ]
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
        default=datetime.now(timezone.utc).date(),
    )
    threshold: float = Field(
        title='Threshold',
        description='Not exceeding this value by the class prediction score results in the recognition of the result '
        'as "unknown"',
        default=0,
        examples=[0.75],
        ge=0.0,
        le=1.0,
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
        'overall classification, '
        '`only_corine` - return raw CLC data, '
        '`harmonized_corine` - return CLC data reclassified to match model output',
        default=FusionMode.ONLY_MODEL,
        examples=[FusionMode.ONLY_MODEL],
    )

    @model_validator(mode='after')
    def minus_week(self) -> 'LulcWorkUnit':
        if not self.start_date:
            self.start_date = self.end_date - timedelta(days=7)
        return self


class LulcUtility(PlatformHttpUtility):
    def __init__(
        self,
        base_url: str,
        max_workers: int = 2,
        max_retries: int = 5,
    ):
        """A wrapper class around the LULC Utility API.

        :param base_url: The base URL of the LULC API, including any path it may be deployed under
            (e.g. https://staging.climate-action.org/api/v1/lulc).
        :param max_workers: maximum number of threads to spawn for parallel requests
        :param max_retries: number of retires before giving up during connection startup
        """
        super().__init__(base_url=base_url, max_retries=max_retries)

        self.max_workers = max_workers

    def __fetch_data(self, unit: LulcWorkUnit) -> rio.DatasetReader:
        try:
            url = f'{self.base_url}/segment/'
            request_body = unit.model_dump(mode='json', exclude={'aoi'})
            request_body['area_coords'] = unit.aoi.bounds

            log.debug(f'Requesting classification from LULC Utility at {url} for region {unit.model_dump()}')
            response = self.session.post(url=url, json=request_body)
            response.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            raise PlatformUtilityError('Connection to utility cannot be established') from e
        except requests.exceptions.HTTPError as e:
            raise PlatformUtilityError('Query failed due to an error') from e
        else:
            return rio.open(BytesIO(response.content), mode='r')

    @contextmanager
    def compute_raster(
        self, units: List[LulcWorkUnit], max_unit_size: int = 2300, max_unit_area: int = 4e6
    ) -> Generator[rasterio.DatasetReader]:
        """Generate a remote sensing-based LULC classification.

        :param units: Areas of interest
        :param max_unit_size: Size in pixels used to determine whether the unit has to be split to meet external service
        processing requirements. The value applies to both height and width. The default of 2300 is slightly under the
        SentinelHub limit of 2500.
        :param max_unit_area: Area in pixels-squared used to determine whether the unit has to be split to meet external
        service processing requirements. The default of 4,000,000 is within the capabilities of a device with 16GB RAM.
        :return: An opened geo-tiff file within a context manager. Use it as `with compute_raster(units) as lulc:`
        """
        units = LulcUtility.adjust_work_units(units, max_unit_size=max_unit_size, max_unit_area=max_unit_area)
        with compute_raster(
            units, max_workers=self.max_workers, fetch_data=self.__fetch_data, has_color_map=True
        ) as lulc:
            yield lulc

    def get_class_legend(self) -> LabelResponse:
        url = f'{self.base_url}/segment/describe'
        response = self.session.get(url=url)
        response.raise_for_status()
        return LabelResponse(**response.json())

    @staticmethod
    def adjust_work_units(
        units: List[LulcWorkUnit], max_unit_size: int = 2300, max_unit_area: int = None
    ) -> List[LulcWorkUnit]:
        max_area = max_unit_area if max_unit_area else max_unit_size * max_unit_size
        log.debug(
            f'Binning areas based on {max_unit_size=:g} and {max_area=:g}. '
            '8GB of RAM should be able to support at least max_unit_area=3e+6. '
            '16GB of RAM should be able to support at least max_unit_area=4e+6'
        )

        adjusted_units = []
        for unit in units:
            request_shapes = GeoSeries([unit.aoi])
            bounds = generate_bounds(
                request_shapes, max_unit_size=max_unit_size, max_unit_area=max_unit_area, resolution=10.0
            )
            adjusted_units.extend([unit.model_copy(update={'area_coords': tuple(b)}, deep=True) for b in bounds])
        return adjusted_units
