import geojson_pydantic
import pytest
import shapely
from geopandas import GeoSeries
from pydantic_core import ValidationError
from shapely import MultiPolygon

from climatoology.base.aoi import (
    AoiProperties,
    AreaConstraint,
    CoveredByGeomConstraint,
)


def test_area_constraint_order_validator():
    with pytest.raises(ValidationError):
        AreaConstraint(min_area=10, max_area=1)


@pytest.mark.parametrize('edge_length,passes_check', [(5000, True), (1000, False), (20_000, False)])
def test_area_constraint_check(edge_length, passes_check):
    constraint = AreaConstraint(min_area=10, max_area=100)

    xmin, ymin = 476_000, 5_472_000
    aoi_gs = GeoSeries([shapely.box(xmin, ymin, xmin + edge_length, ymin + edge_length)], crs=32632).to_crs(4326)
    aoi = aoi_gs.item()

    check_result = constraint.check(aoi, AoiProperties(name='foo', id='bar'))

    assert check_result == passes_check


def test_aoi_constraint_covered_by_polygon_or_multipolygon():
    """covered_by accepts polygons or multipolygons, but not other geometry types."""
    cbgc = CoveredByGeomConstraint(geom=geojson_pydantic.Polygon.from_bounds(-2, -2, 2, 2))
    assert cbgc.check(
        aoi_geometry=MultiPolygon([shapely.box(0, 0, 1, 1)]),
        aoi_properties=AoiProperties(name='a', id='b'),
    )
    assert not cbgc.check(
        aoi_geometry=MultiPolygon([shapely.box(-3, -3, 1, 1)]),
        aoi_properties=AoiProperties(name='a', id='b'),
    )
