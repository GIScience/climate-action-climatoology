import uuid
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Annotated, Optional, Union

import geojson_pydantic
from geopandas import GeoSeries
from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator
from shapely import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from shapely.io import from_geojson

type OsmIdSelectionType = list[
    Annotated[
        int,
        Field(
            lt=0,
            description='IDs for OSM relations with `boundary=administrative` and `admin_level=*`. The ID values are a'
            'negative representation of the standard OSM ID, because that is how they are represented in the'
            'underlying database. You can find the OSM ID from OSM directly or (their negative representations) '
            'from https://maps.heigit.org/vector/tiles/public.admin_boundaries_layer.html.',
        ),
    ]
]


class AoiProperties(BaseModel):
    model_config = ConfigDict(extra='allow')

    name: str = Field(
        title='Name',
        description='The name of the area of interest i.e. a human readable description.',
        examples=['Heidelberg'],
    )
    id: str = Field(
        title='ID',
        description='A unique identifier of the area of interest.',
        examples=[str(uuid.uuid4())],
    )


AoiFeatureModel = geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties]


class AoiBaseConstraint(BaseModel, ABC):
    @computed_field
    @property
    def constraint_type(self) -> str:
        return type(self).__name__

    @abstractmethod
    def check(self, aoi_geometry: MultiPolygon, aoi_properties: AoiProperties) -> bool:
        pass


class AreaConstraint(AoiBaseConstraint):
    """
    The area of the AOI (in km2) must be within this range.
    """

    min_area: Annotated[
        float,
        Field(strict=True, ge=0, default=0),
    ]
    max_area: Annotated[
        Optional[float],
        Field(strict=True, gt=0, default=None, description='Maximum area for the AOI. `None` represents no limit.'),
    ]

    @model_validator(mode='after')
    def assert_area_range_order(self) -> 'AreaConstraint':
        if self.max_area is not None:
            assert self.max_area > self.min_area, 'max_area must be None or greater than min_area'
        return self

    def check(self, aoi_geometry: MultiPolygon, aoi_properties: AoiProperties) -> bool:
        gs = GeoSeries(data=[aoi_geometry], crs=4326)
        projected_crs = gs.estimate_utm_crs()
        projected_geom = gs.to_crs(projected_crs).item()
        sqm_to_sqkm = 1 / (1000 * 1000)

        aoi_area_km2 = projected_geom.area * sqm_to_sqkm
        check = self.min_area <= aoi_area_km2
        if self.max_area is not None:
            check = check and aoi_area_km2 <= self.max_area
        return check


class CoveredByGeomConstraint(AoiBaseConstraint):
    geom: geojson_pydantic.Polygon | geojson_pydantic.MultiPolygon = Field(
        description='The AOI must be covered by/within this geometry (i.e., no points of the AOI may be outside the points of this geometry).',
        examples=[
            geojson_pydantic.MultiPolygon.create(
                coordinates=[
                    [
                        [
                            [8.6, 49.4],
                            [8.7, 49.4],
                            [8.7, 49.5],
                            [8.6, 49.5],
                            [8.6, 49.4],
                        ]
                    ]
                ]
            )
        ],
    )

    @cached_property
    def shapely_geom(self) -> Polygon | MultiPolygon:
        return from_geojson(self.geom.model_dump_json())

    def check(self, aoi_geometry: MultiPolygon, aoi_properties: AoiProperties) -> bool:
        return aoi_geometry.covered_by(self.shapely_geom)


class CoveredByBoundaryConstraint(AoiBaseConstraint):
    osm_ids: OsmIdSelectionType = Field(
        description='The AOI must be covered by/within an OSM relation with one of these IDs in our database.',
        examples=[[-7444], [-62422, -285864]],
    )

    def check(self, aoi_geometry: MultiPolygon, aoi_properties: AoiProperties) -> bool:
        # download osm id geoms here and then do the same check as in CoveredByGeom
        raise NotImplementedError('Checking against osm id boundaries is not yet implement.')


class BoundarySelectionConstraint(AoiBaseConstraint):
    osm_ids: OsmIdSelectionType = Field(
        description='The AOI must be an OSM relation with one of these IDs in our database.',
        examples=[[-7444], [-62422, -285864]],
    )

    def check(self, aoi_geometry: MultiPolygon, aoi_properties: AoiProperties) -> bool:
        # implement https://gitlab.heigit.org/climate-action/climatoology/-/work_items/310 to solve this
        raise NotImplementedError('Checking against osm ids is not yet implemented.')


type AoiConstraint = Union[
    AreaConstraint, CoveredByGeomConstraint, CoveredByBoundaryConstraint, BoundarySelectionConstraint
]

type AoiConstraintSets = list[list[AoiConstraint]]
