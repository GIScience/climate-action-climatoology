from typing import Optional
from uuid import UUID

import geoalchemy2
import geojson_pydantic
import shapely
from geoalchemy2.shape import to_shape
from semver import Version
from sqlalchemy import Dialect, String, TypeDecorator
from sqlalchemy.sql.type_api import _T


class DbSemver(TypeDecorator):
    impl = String

    cache_ok = True

    def process_bind_param(self, value: Optional[_T], dialect: Dialect) -> Optional[str]:
        return str(value) if value else None

    def process_result_value(self, value: str, dialect: Dialect) -> Version:
        return Version.parse(value)


class DbUuidAsString(TypeDecorator):
    impl = String

    cache_ok = True

    def process_bind_param(self, value: Optional[_T], dialect: Dialect) -> Optional[str]:
        return str(value) if value else None

    def process_result_value(self, value: str, dialect: Dialect) -> UUID:
        return UUID(value)


class DbGeometry(TypeDecorator):
    impl = geoalchemy2.Geometry

    cache_ok = True

    def process_bind_param(
        self, value: Optional[_T], dialect: Dialect
    ) -> Optional[str | geoalchemy2.elements._SpatialElement]:
        match value:
            case None | str() | geoalchemy2.elements._SpatialElement():
                return value
            case shapely.geometry.base.BaseGeometry():
                return geoalchemy2.shape.from_shape(value)
            case geojson_pydantic.geometries._GeometryBase():
                return value.wkt
            case _:
                raise TypeError(f'Unsupported input type {type(value)} for table column of type DbGeometry')

    def process_result_value(
        self, value: geoalchemy2.WKBElement, dialect: Dialect
    ) -> shapely.geometry.base.BaseGeometry:
        return to_shape(value)
