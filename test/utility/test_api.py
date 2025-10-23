import pytest
from geopandas import GeoSeries
from sentinelhub import CRS, BBox, bbox_to_dimensions
from shapely import MultiPolygon, Polygon, difference, geometry, unary_union

from climatoology.utility.api import generate_bounds


@pytest.mark.parametrize(['max_unit_size', 'expected_n_bounds'], [[500, 9], [800, 4], [1500, 1]])
def test_generate_bounds_max_edge_length(max_unit_size, expected_n_bounds):
    # Actual h, w = (1336, 1304)
    origin_area = GeoSeries([geometry.box(8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485)])

    bounds = generate_bounds(target_geometries=origin_area, resolution=10, max_unit_size=max_unit_size)
    adjusted_area = unary_union([geometry.box(*b) for b in bounds])

    assert difference(origin_area.union_all(), adjusted_area) == Polygon()
    assert len(bounds) == expected_n_bounds


@pytest.mark.parametrize(['max_unit_area', 'expected_n_bounds'], [[400 * 400, 16], [800 * 800, 4], [1400 * 1400, 1]])
def test_generate_bounds_max_area(max_unit_area, expected_n_bounds):
    # Actual h, w = (1336, 1304)
    origin_area = GeoSeries([geometry.box(8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485)])

    bounds = generate_bounds(target_geometries=origin_area, resolution=10, max_unit_area=max_unit_area)
    adjusted_area = unary_union([geometry.box(*b) for b in bounds])

    assert difference(origin_area.union_all(), adjusted_area) == Polygon()
    assert len(bounds) == expected_n_bounds


def test_generate_bounds_with_very_short_dimension():
    """If the box has a 'narrow' shape, this test asserts that it stops splitting
    in one direction if further splits would result in a box dimension of less than one pixel.
    """
    # Actual h, w = (112, 1)
    origin_area = GeoSeries([geometry.box(8.67664055017255, 49.41496324705642, 8.692030312643254, 49.41485816227657)])
    expected_splits = [(56, 1), (56, 1)]

    bounds = generate_bounds(target_geometries=origin_area, resolution=10, max_unit_size=100)
    actual_splits = [bbox_to_dimensions(b, resolution=10) for b in bounds]

    assert expected_splits == actual_splits


def test_generate_bounds_with_invalid_dimension():
    # Actual h, w = (112, 0)
    origin_area = GeoSeries([geometry.box(8.67664055017255, 49.41496324705642, 8.692030312643254, 49.41496324705642)])

    computed_bounds = generate_bounds(target_geometries=origin_area, resolution=10, max_unit_size=100)
    actual_splits = [bbox_to_dimensions(b, resolution=10) for b in computed_bounds]

    expected_bounds = [(56, 1), (56, 1)]
    assert actual_splits == expected_bounds


def test_generate_bounds_from_multipolygon_drops_unused_splits():
    """If the input is a multipolygon which only overlaps some of the split bounds, the 'unused' bounds should be
    dropped from the computed bounds.
    """
    expected_bounds = [
        BBox(((8.0859375, 47.5172006978394), (8.311197916666666, 47.676492144352125)), crs=CRS.WGS84),
        BBox(((8.536458333333334, 47.676492144352125), (8.76171875, 47.83578359086485)), crs=CRS.WGS84),
    ]

    multipolygon = GeoSeries(
        [
            MultiPolygon(
                [
                    geometry.box(8.0859375, 47.5172006978394, 8.26171875, 47.63578359086485),
                    geometry.box(8.5859375, 47.7172006978394, 8.76171875, 47.83578359086485),
                ]
            )
        ]
    )
    computed_bounds = generate_bounds(target_geometries=multipolygon, resolution=10, max_unit_size=2000)

    assert computed_bounds == expected_bounds
