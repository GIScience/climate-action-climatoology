import pytest
from shapely import Polygon, difference, geometry, unary_union

from climatoology.utility.api import adjust_bounds


@pytest.mark.parametrize(['max_unit_size', 'expected_n_bounds'], [[500, 16], [800, 4], [1500, 1]])
def test_adjust_work_unit_max_edge_length(max_unit_size, expected_n_bounds):
    # Actual h, w = (1336, 1304)
    origin_coords = (8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485)
    origin_area = geometry.box(*origin_coords)

    bounds = adjust_bounds(bbox=origin_coords, resolution=10, max_unit_size=max_unit_size)
    adjusted_area = unary_union([geometry.box(*b) for b in bounds])

    assert difference(origin_area, adjusted_area) == Polygon()
    assert len(bounds) == expected_n_bounds


@pytest.mark.parametrize(['max_unit_area', 'expected_n_bounds'], [[400 * 400, 16], [800 * 800, 4], [1400 * 1400, 1]])
def test_adjust_work_unit_max_area(max_unit_area, expected_n_bounds):
    # Actual h, w = (1336, 1304)
    origin_coords = (8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485)
    origin_area = geometry.box(*origin_coords)

    bounds = adjust_bounds(bbox=origin_coords, resolution=10, max_unit_area=max_unit_area)
    adjusted_area = unary_union([geometry.box(*b) for b in bounds])

    assert difference(origin_area, adjusted_area) == Polygon()
    assert len(bounds) == expected_n_bounds
