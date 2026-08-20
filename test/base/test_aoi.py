import geojson_pydantic
import pytest
import shapely
from geopandas import GeoSeries
from pydantic_core import ValidationError
from shapely import MultiPolygon

from climatoology.base.aoi import (
    AoiBaseConstraint,
    AoiProperties,
    AreaConstraint,
    BoundarySelectionConstraint,
    CoveredByBoundaryConstraint,
    CoveredByGeomConstraint,
)
from climatoology.base.plugin_info import PluginInfoFinal


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
    cbgc = CoveredByGeomConstraint(description='Test Region', geom=geojson_pydantic.Polygon.from_bounds(-2, -2, 2, 2))
    assert cbgc.check(
        aoi_geometry=MultiPolygon([shapely.box(0, 0, 1, 1)]),
        aoi_properties=AoiProperties(name='a', id='b'),
    )
    assert not cbgc.check(
        aoi_geometry=MultiPolygon([shapely.box(-3, -3, 1, 1)]),
        aoi_properties=AoiProperties(name='a', id='b'),
    )


def test_covered_by_geom_default_description():
    cbgc = CoveredByGeomConstraint(geom=geojson_pydantic.Polygon.from_bounds(-2, -2, 2, 2))

    assert cbgc.description == 'An area within coordinates (-2.0, -2.0, 2.0, 2.0)'


def test_plugin_info_from_dict_correctly_sets_aoi_constraints(default_plugin_info_final):
    """
    Assert that when constructing plugin info from a dict, it correctly loads all of the AOI constraint types.
    BoundarySelectionConstraint is excluded from this test because it is not allowed to be used in combination with the
    other constraint types.
    """
    info = default_plugin_info_final.model_copy(deep=True)

    geom_constraint = geojson_pydantic.Polygon.from_bounds(-2, -2, 2, 2)
    test_aoi_constraints = [
        AreaConstraint(min_area=10),
        CoveredByGeomConstraint(geom=geom_constraint),
        CoveredByBoundaryConstraint(osm_ids=[-123]),
    ]
    info.aoi_constraints = [test_aoi_constraints]

    dumped_info = info.model_dump(mode='json')
    recreated_info = PluginInfoFinal(**dumped_info)

    assert recreated_info.aoi_constraints == info.aoi_constraints

    # Make sure that this test does test every subclass of AoiBaseConstraint
    all_constraint_types = set(AoiBaseConstraint.__subclasses__())
    check_constraint_types = all_constraint_types.difference({BoundarySelectionConstraint})
    assert {type(c) for c in test_aoi_constraints} == check_constraint_types, 'Not all constraint classes are checked'


def test_plugin_info_from_dict_correctly_sets_boundary_selection_constraint(default_plugin_info_final):
    """Assert that when constructing plugin info from a dict, it correctly loads all the BoundarySelectionConstraint."""
    info = default_plugin_info_final.model_copy(deep=True)

    test_aoi_constraints = [BoundarySelectionConstraint(osm_ids=[-456])]
    info.aoi_constraints = [test_aoi_constraints]

    dumped_info = info.model_dump(mode='json')
    recreated_info = PluginInfoFinal(**dumped_info)

    assert recreated_info.aoi_constraints == info.aoi_constraints
