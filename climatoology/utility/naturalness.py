from contextlib import contextmanager
from enum import StrEnum
from functools import partial
from io import BytesIO
from typing import Generator, List, Optional

import geopandas as gpd
import pandas as pd
import rasterio
import shapely
from geopandas import GeoSeries
from pydantic import Field
from rasterstats import zonal_stats

from climatoology.base.logging import get_climatoology_logger
from climatoology.utility.api import PlatformHttpUtility, TimeRange
from climatoology.utility.exception import PlatformUtilityError
from climatoology.utility.raster import RasterWorkUnit, compute_raster, generate_bounds

log = get_climatoology_logger(__name__)


class NaturalnessIndex(StrEnum):
    NDVI = 'NDVI'
    WATER = 'WATER'
    NATURALNESS = 'NATURALNESS'


class NaturalnessWorkUnit(RasterWorkUnit):
    """Area of interest for naturalness index"""

    time_range: TimeRange = Field(
        title='Time Range',
        description='The time range of satellite observations to base the index on.',
        examples=[TimeRange()],
    )
    resolution: float = Field(
        title='Resolution',
        description='Resolution of the resulting raster image. Will be down sampled, if necessary.',
        default=90.0,
        examples=[90.0],
        ge=10.0,
    )


class NaturalnessUtility(PlatformHttpUtility):
    def __init__(
        self,
        base_url: str,
        max_workers: int = 1,
        max_retries: int = 5,
    ):
        """A wrapper class around the Naturalness Utility API.

        :param base_url: The base URL of the Naturalness API, including any path it may be deployed under
            (e.g. https://staging.climate-action.org/api/v1/naturalness).
        :param max_workers: maximum number of threads to spawn for parallel requests, currently unused
        :param max_retries: number of retires before giving up during connection startup
        """
        super().__init__(base_url=base_url, max_retries=max_retries)

        self.max_workers = max_workers

    @contextmanager
    def compute_raster(
        self, index: NaturalnessIndex, units: List[NaturalnessWorkUnit], max_unit_size: int = 1000
    ) -> Generator[rasterio.DatasetReader]:
        """Generate a remote sensing-based Naturalness raster.

        :param index: The index to be requested from the utility
        :param units: Areas of interest
        :param max_unit_size: Size in pixels used to determine whether the unit has to be split to meet external service
        processing requirements. The value applies to both height and width.
        :return: An opened geo-tiff file within a context manager.
          Use it as `with compute_raster(units) as naturalness:`.
          The file provides a mask outside the area of interest.
        """
        assert len(units) > 0

        units = self.adjust_work_units(index=index, units=units, max_unit_size=max_unit_size)

        fetch_raster = partial(self.__fetch_raster_data, index)

        with compute_raster(units=units, fetch_data=fetch_raster, has_color_map=False) as naturalness:
            yield naturalness

    def compute_vector(
        self,
        index: NaturalnessIndex,
        aggregation_stats: list[str],
        vectors: List[gpd.GeoSeries],
        time_range: TimeRange,
        resolution: float = 90.0,
        max_raster_size: int = 1000,
    ) -> gpd.GeoDataFrame:
        """Generate vector features with aggregated raster statistics.

        The `vectors` input is a list of GeoSeries for backwards compatibility only. If multiple GeoSeries are provided,
        they are merged into a single GeoSeries which makes up the geometry column of the returned GeoDataFrame. To
        ensure completely separate queries for each GeoSeries (e.g. for two sets of vectors that are very far apart),
        make separate calls to this function.

        :param index: raster index parameter to use
        :param aggregation_stats: list of statistics methods for aggregating raster values
        :param vectors: list of GeoSeries of SimpleFeatures to calculate statistics for.
        :param time_range: time range for the analysis
        :param resolution: raster pixel resolution in meters
        :param max_raster_size: Size in pixels used to determine whether the raster queries have to be split to meet
        external service processing requirements. The value applies to both height and width.
        :return: A GeoDataFrame with a column for each stat in aggregation_stats.
        """
        log.debug('Extracting aggregated raster statistics.')

        vectors = [v.to_crs(4326) for v in vectors]
        projected_vectors = gpd.GeoSeries(pd.concat(vectors), crs=4326)

        init_unit = NaturalnessWorkUnit(
            time_range=time_range, resolution=resolution, aoi=shapely.geometry.box(*projected_vectors.total_bounds)
        )
        units = self.adjust_work_units(
            index=index,
            units=[init_unit],
            max_unit_size=max_raster_size,
            intersecting_features=projected_vectors,
        )

        with self.compute_raster(index=index, units=units, max_unit_size=max_raster_size) as raster:
            vectors_with_stats = zonal_stats(
                vectors=projected_vectors,
                raster=raster.read(1),
                stats=aggregation_stats,
                affine=rasterio.transform.from_bounds(*raster.bounds, width=raster.width, height=raster.height),
                geojson_out=True,
                nodata=raster.nodata,
                all_touched=True,
            )

        vectors_processed = gpd.GeoDataFrame.from_features(vectors_with_stats, crs=projected_vectors.crs)
        vectors_processed.index = [v['id'] for v in vectors_with_stats]
        vectors_processed.index = vectors_processed.index.astype(projected_vectors.index.dtype)
        return vectors_processed

    def __fetch_raster_data(self, index: NaturalnessIndex, unit: NaturalnessWorkUnit) -> rasterio.DatasetReader:
        url = f'{self.base_url}/{index}/raster'
        request_body = unit.model_dump(mode='json', exclude={'aoi'})
        request_body['bbox'] = unit.aoi.bounds

        response = self.post_request(url=url, request_body=request_body)

        return rasterio.open(BytesIO(response.content), mode='r')

    def adjust_work_units(
        self,
        index: NaturalnessIndex,
        units: list[NaturalnessWorkUnit],
        max_unit_size: int = 1000,
        intersecting_features: Optional[gpd.GeoSeries] = None,
    ) -> list[NaturalnessWorkUnit]:
        """
        Adjust work units to:
        - meet the max_unit_size restrictions
        - meet the PU quota limit (informed by the utility response)
        - optionally filter to only the units intersecting with the provided features

        :param index: The index to be requested from the utility
        :param units: List of work units to adjust
        :param max_unit_size: Maximum size in pixels for height and width of the work units
        :param intersecting_features: Optional GeoSeries of features that the returned work units should intersect with
        :return: List of adjusted work units
        """
        split_units = self.split_work_units(
            units=units, max_unit_size=max_unit_size, intersecting_features=intersecting_features
        )
        if len(split_units) > len(units):
            log.debug(f'Split {len(units)} units into {len(split_units)} units to meet the requested {max_unit_size=}')

        while not self._pus_within_quota(index=index, units=split_units):
            if max_unit_size < 100:
                raise PlatformUtilityError(
                    f'Cancelled request because it is too large: for {index=} with {units=}. '
                    'Consider reducing your resolution or time range.'
                )

            max_unit_size //= 2
            log.debug(f'Estimated processing unit consumption is too high, splitting units with {max_unit_size=}')
            split_units = self.split_work_units(
                units=split_units, max_unit_size=max_unit_size, intersecting_features=intersecting_features
            )

        return split_units

    @staticmethod
    def split_work_units(
        units: List[NaturalnessWorkUnit],
        max_unit_size,
        intersecting_features: Optional[gpd.GeoSeries] = None,
    ) -> List[NaturalnessWorkUnit]:
        """
        Split work units to meet the max_unit_size restrictions and optionally filter to only the units intersecting
        with the provided features.

        :param units: List of work units to split
        :param max_unit_size: Maximum size in pixels for height and width of the work units
        :param intersecting_features: Optional GeoSeries of features that the returned work units should intersect with
        :return: List of split work units
        """
        split_units = []
        for unit in units:
            request_shapes = GeoSeries([unit.aoi])
            bounds = generate_bounds(request_shapes, max_unit_size=max_unit_size, resolution=unit.resolution)
            if intersecting_features is not None:
                bounds = [b for b in bounds if intersecting_features.intersects(b.geometry).any()]

            for b in bounds:
                u = unit.model_copy(deep=True)
                u.aoi = b.geometry
                split_units.append(u)

        return split_units

    def _pus_within_quota(self, index: NaturalnessIndex, units: List[NaturalnessWorkUnit]) -> bool:
        """
        Returns `True` if the estimated PUs per unit are less than the available quota. Only estimates the PUs of one unit per
        set of matching 'other' attributes (e.g. time range, resolution) - the bbox shape of each 'split unit' should
        be the same, so it doesn't matter which one we choose.
        """
        chosen_units = self._get_one_unit_per_group(units)
        estimated_pus = []
        for unit in chosen_units:
            response = self.__fetch_estimated_pus(index, unit)
            estimated_pus.append(response['estimated_pus'])

        return max(estimated_pus) < response['pu_quota_per_minute']

    def _get_one_unit_per_group(self, units: list[NaturalnessWorkUnit]) -> list[NaturalnessWorkUnit]:
        """Returns a list with one unit per set of matching 'other' attributes (e.g. time range, resolution)."""
        selected_units = []
        selected_keys = []
        for unit in units:
            key = unit.model_dump_json(exclude=('aoi'))
            if key not in selected_keys:
                selected_units.append(unit)
                selected_keys.append(key)

        return selected_units

    def __fetch_estimated_pus(self, index: NaturalnessIndex, unit: NaturalnessWorkUnit) -> dict[str, float]:
        """Fetch 'estimated_pus' and 'pu_quota_per_minute' from the utility."""
        url = f'{self.base_url}/{index}/raster/estimate_pus'
        request_body = unit.model_dump(mode='json', exclude={'aoi'})
        request_body['bbox'] = unit.aoi.bounds

        response = self.post_request(url=url, request_body=request_body)

        return response.json()
