import json
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from enum import StrEnum
from functools import partial
from io import BytesIO
from itertools import repeat
from typing import ContextManager, List, Tuple

import geopandas as gpd
import pandas as pd
import rasterio
import requests
from pydantic import BaseModel, Field, confloat, conint
from shapely.geometry import shape
from tqdm import tqdm

from climatoology.utility.api import PlatformHttpUtility, TimeRange, adjust_bounds, compute_raster
from climatoology.utility.exception import PlatformUtilityException

log = logging.getLogger(__name__)


class NaturalnessIndex(StrEnum):
    NDVI = 'NDVI'
    WATER = 'WATER'
    NATURALNESS = 'NATURALNESS'


class NaturalnessWorkUnit(BaseModel):
    """Area of interest for naturalness index"""

    time_range: TimeRange = Field(
        title='Time Range',
        description='The time range of satellite observations to base the index on.',
        examples=[TimeRange()],
    )
    resolution: conint(ge=10) = Field(
        title='Resolution',
        description='Resolution of the resulting raster image. Will be down sampled, if necessary.',
        default=90,
        examples=[90],
    )
    bbox: Tuple[
        confloat(ge=-180, le=180), confloat(ge=-90, le=90), confloat(ge=-180, le=180), confloat(ge=-90, le=90)
    ] = Field(
        title='Area Coordinates',
        description='Bounding box coordinates in WGS 84 (west, south, east, north)',
        examples=[[8.70, 49.41, 8.71, 49.42]],
    )


class NaturalnessUtility(PlatformHttpUtility):
    def __init__(
        self,
        host: str,
        port: int,
        path: str,
        max_workers: int = 1,
        max_retries: int = 5,
    ):
        """A wrapper class around the Naturalness Utility API.

        :param host: api host
        :param port: api port
        :param path: api path
        :param max_workers: maximum number of threads to spawn for parallel requests
        :param max_retries: number of retires before giving up during connection startup
        """
        super().__init__(host, port, path, max_retries)

        self.max_workers = max_workers

    @contextmanager
    def compute_raster(
        self, index: NaturalnessIndex, units: List[NaturalnessWorkUnit], max_unit_size: int = 1000
    ) -> ContextManager[rasterio.DatasetReader]:
        """Generate a remote sensing-based Naturalness raster.

        :param index: The index to be requested from the utility
        :param units: Areas of interest
        :param max_unit_size: Size in pixels used to determine whether the unit has to be split to meet external service
        processing requirements. The value applies to both height and width.
        :return: An opened geo-tiff file within a context manager. Use it as `with compute_raster(units) as naturalness:`
        """

        units = self.adjust_work_units(units, max_unit_size)

        fetch_raster = partial(self.__fetch_raster_data, index)

        with compute_raster(
            units=units, max_workers=self.max_workers, fetch_data=fetch_raster, has_color_map=False
        ) as naturalness:
            yield naturalness

    def compute_vector(
        self,
        index: NaturalnessIndex,
        aggregation_stats: list[str],
        vectors: List[gpd.GeoSeries],
        time_range: TimeRange,
        resolution: int = 90,
        max_raster_size: int = 1000,
    ) -> gpd.GeoDataFrame:
        """Generate vector features with aggregated raster statistics.

        If the height or width of the `total_bounds` of any of the GeoSeries in `vectors` is greater than
        `max_raster_size`, the geometries will be segmented and clipped into smaller areas for computation.
        In this case, the returned geometries will also be clipped at the smaller boundaries and will not match the
        provided `vectors` geometries.

        :param index: raster index parameter to use
        :param aggregation_stats: list of statistics methods for aggregating raster values
        :param vectors: list of GeoSeries of SimpleFeatures to calculate statistics for.
        :param time_range: time range for the analysis
        :param resolution: raster pixel resolution in meters
        :param max_raster_size: Size in pixels used to determine whether the vectors have to be split to meet external
        service processing requirements for the raster input. The value applies to both height and width.
        :return: A GeoDataFrame with a column for each stat in aggregation_stats.
        Note that the geometries may not match the provided `vectors` geometries (due to clipping for the max_raster_size).
        """
        log.debug('Extracting aggregated raster statistics..')

        vectors = [v.to_crs(4326) for v in vectors]
        vectors = self.split_vectors(vectors=vectors, resolution=resolution, max_unit_size=max_raster_size)

        n = len(vectors)
        with tqdm(total=n) as pbar:
            slices = []

            with ThreadPoolExecutor(self.max_workers) as pool:
                for dataset in pool.map(
                    self.__fetch_zonal_statistics,
                    repeat(index, n),
                    repeat(aggregation_stats, n),
                    vectors,
                    repeat(time_range, n),
                    repeat(resolution, n),
                ):
                    pbar.update()
                    slices.append(dataset)

        # pd.concat correctly returns a GeoDataFrame but the type checker cannot know
        # noinspection PyTypeChecker
        result: gpd.GeoDataFrame = pd.concat(slices)
        return result

    def __fetch_raster_data(self, index: NaturalnessIndex, unit: NaturalnessWorkUnit) -> rasterio.DatasetReader:
        try:
            url = f'{self.base_url}{index}/raster'

            log.debug(f'Requesting raster from Naturalness Utility at {url} for region {unit.model_dump()}')
            response = self.session.post(url=url, json=unit.model_dump(mode='json'))
            response.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            raise PlatformUtilityException('Connection to utility cannot be established') from e
        except requests.exceptions.HTTPError as e:
            raise PlatformUtilityException('Query failed due to an error') from e
        else:
            return rasterio.open(BytesIO(response.content), mode='r')

    def __fetch_zonal_statistics(
        self,
        index: NaturalnessIndex,
        aggregation_stats: list[str],
        vectors: gpd.GeoSeries,
        time_range: TimeRange,
        resolution: int,
    ) -> gpd.GeoDataFrame:
        try:
            url = f'{self.base_url}{index}/vector'
            log.debug(f'Requesting zonal statistics from Naturalness Utility at {url}')

            request_json = {
                'time_range': time_range.model_dump(mode='json'),
                'vectors': json.loads(vectors.to_json(to_wgs84=True)),
                'aggregation_stats': aggregation_stats,
                'resolution': resolution,
            }

            response = self.session.post(url=url, json=request_json)
            response.raise_for_status()
            geojson = response.json()

            geoms = [shape(i['geometry']) for i in geojson['features']]
            indices = [i['id'] for i in geojson['features']]
            data = [i['properties'] for i in geojson['features']]
            vectors_with_stats = gpd.GeoDataFrame(index=indices, data=data, geometry=geoms, crs=vectors.crs)
            vectors_with_stats.index = vectors_with_stats.index.astype(vectors.index.dtype)
            return vectors_with_stats

        except requests.exceptions.ConnectionError as e:
            raise PlatformUtilityException('Connection to utility cannot be established') from e
        except requests.exceptions.HTTPError as e:
            raise PlatformUtilityException('Query failed due to an error') from e

    @staticmethod
    def adjust_work_units(units: List[NaturalnessWorkUnit], max_unit_size: int = 1000) -> List[NaturalnessWorkUnit]:
        adjusted_units = []
        for unit in units:
            bounds = adjust_bounds(unit.bbox, max_unit_size=max_unit_size, resolution=unit.resolution)
            adjusted_units.extend([unit.model_copy(update={'bbox': tuple(b)}, deep=True) for b in bounds])
        return adjusted_units

    @staticmethod
    def split_vectors(vectors: List[gpd.GeoSeries], resolution: int, max_unit_size: int = 1000) -> List[gpd.GeoSeries]:
        """Split vectors into separate GeoSeries, such that the total bounds of each GeoSeries is less than max_unit_size"""
        adjusted_vectors = []
        for vector in vectors:
            # the total bounds are a np.array of length 4 and the tuple command correctly turns it into a tuple
            # noinspection PyTypeChecker
            bbox: Tuple[float, float, float, float] = tuple(vector.total_bounds)
            bounds = adjust_bounds(bbox=bbox, max_unit_size=max_unit_size, resolution=resolution)
            split_features = [gpd.clip(gdf=vector, mask=b.geometry, keep_geom_type=True) for b in bounds]
            adjusted_vectors.extend([f for f in split_features if not f.empty])

        if len(adjusted_vectors) > len(vectors):
            log.warning(
                'The dimensions of at least one of the provided vector GeoSeries exceeds the max_unit_size. '
                'The returned geometries will be segmented into smaller parts to meet computation limits.'
            )
        return adjusted_vectors
