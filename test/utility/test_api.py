import pytest
from shapely import geometry, unary_union, difference, Polygon

from climatoology.utility.api import adjust_bounds


@pytest.mark.parametrize(['max_unit_size', 'expected_n_bounds'], [[500, 16], [800, 4], [1500, 1]])
def test_adjust_work_unit(max_unit_size, expected_n_bounds):
    # h, w = (1336, 1304)
    origin_coords = (8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485)
    origin_area = geometry.box(*origin_coords)

    bounds = adjust_bounds(bboxes=origin_coords, max_unit_size=max_unit_size)
    adjusted_area = unary_union([geometry.box(*b) for b in bounds])

    assert difference(origin_area, adjusted_area) == Polygon()
    assert len(bounds) == expected_n_bounds
