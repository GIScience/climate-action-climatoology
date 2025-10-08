import pytest
from sentinelhub import bbox_to_dimensions
from shapely import Polygon, difference, geometry, unary_union

from climatoology.utility.api import adjust_bounds


@pytest.mark.parametrize(['max_unit_size', 'expected_n_bounds'], [[500, 9], [800, 4], [1500, 1]])
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


def test_adjust_bounds_with_very_short_dimension():
    """If the box has a 'narrow' shape, this test asserts that it stops splitting
    in one direction if further splits would result in a box dimension of less than one pixel.
    """
    # Actual h, w = (112, 1)
    origin_coords = (8.67664055017255, 49.41496324705642, 8.692030312643254, 49.41485816227657)
    expected_splits = [(56, 1), (56, 1)]

    bounds = adjust_bounds(bbox=origin_coords, resolution=10, max_unit_size=100)
    actual_splits = [bbox_to_dimensions(b, resolution=10) for b in bounds]

    assert expected_splits == actual_splits


def test_adjust_bounds_with_invalid_dimension():
    # Actual h, w = (112, 0)
    origin_coords = (8.67664055017255, 49.41496324705642, 8.692030312643254, 49.41496324705642)

    with pytest.raises(ValueError, match=r'.* has dimensions \(112, 0\)'):
        _ = adjust_bounds(bbox=origin_coords, resolution=10, max_unit_size=100)
