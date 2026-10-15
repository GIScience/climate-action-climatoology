from datetime import UTC, datetime
from typing import Optional
from uuid import UUID

import geoalchemy2
import geojson_pydantic
import shapely
from geoalchemy2.shape import to_shape
from semver import Version
from sqlalchemy import DateTime, Dialect, String, TypeDecorator
from sqlalchemy.sql.type_api import _T

from climatoology.base import UtcDateTime


class DbSemver(TypeDecorator):
    impl = String

    cache_ok = True

    def process_bind_param(self, value: Optional[_T], dialect: Dialect) -> Optional[str]:
        return str(value) if value else None

    def process_result_value(self, value: Optional[str], dialect: Dialect) -> Optional[Version]:
        if value is None:
            return value
        return Version.parse(value)


class DbUuidAsString(TypeDecorator):
    impl = String

    cache_ok = True

    def process_bind_param(self, value: Optional[_T], dialect: Dialect) -> Optional[str]:
        return str(value) if value else None

    def process_result_value(self, value: Optional[str], dialect: Dialect) -> Optional[UUID]:
        if value is None:
            return value
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
        self, value: Optional[geoalchemy2.WKBElement], dialect: Dialect
    ) -> Optional[shapely.geometry.base.BaseGeometry]:
        if value is None:
            return value
        return to_shape(value)


class DbUtcDateTime(TypeDecorator):
    """We store naive datetimes because some DBs behave a bit complicated when served with aware datetimes"""

    impl = DateTime

    cache_ok = True

    def process_bind_param(self, value: Optional[UtcDateTime], dialect: Dialect) -> Optional[datetime]:
        if value is None:
            return value

        naive_datetime = value.replace(tzinfo=None)
        return naive_datetime

    def process_result_value(self, value: Optional[datetime], dialect: Dialect) -> Optional[UtcDateTime]:
        if value is None:
            return value

        aware_datetime: UtcDateTime = value.replace(tzinfo=UTC)
        return aware_datetime
