import pytest
from geopandas import GeoSeries
from sentinelhub import CRS, BBox, bbox_to_dimensions
from shapely import MultiPolygon, Polygon, difference, geometry, unary_union

from climatoology.utility.api import _make_bound_dimensions_valid, generate_bounds


def test_make_bound_dimensions_valid():
    input_bounds = BBox((8.6766405, 49.4149631, 8.6920303, 49.4149632), crs=CRS.WGS84)

    computed_bounds = _make_bound_dimensions_valid(bounds=input_bounds, resolution=10)
    assert computed_bounds.geometry.covers(input_bounds.geometry)


@pytest.mark.parametrize(['max_unit_size', 'expected_n_bounds'], [[500, 9], [800, 4], [1500, 1]])
def test_generate_bounds_max_edge_length(max_unit_size, expected_n_bounds):
    # Actual h, w = (1336, 1304)
    origin_area = GeoSeries([geometry.box(8.0859375, 47.5172007, 8.2617188, 47.6357836)])

    bounds = generate_bounds(target_geometries=origin_area, resolution=10, max_unit_size=max_unit_size)
    adjusted_area = unary_union([geometry.box(*b) for b in bounds])

    assert difference(origin_area.union_all(), adjusted_area) == Polygon()
    assert len(bounds) == expected_n_bounds


@pytest.mark.parametrize(['max_unit_area', 'expected_n_bounds'], [[400 * 400, 16], [800 * 800, 4], [1400 * 1400, 1]])
def test_generate_bounds_max_area(max_unit_area, expected_n_bounds):
    # Actual h, w = (1336, 1304)
    origin_area = GeoSeries([geometry.box(8.0859375, 47.5172007, 8.2617188, 47.6357836)])

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


def test_generate_bounds_not_clipped():
    # The top point of this feature was clipped by a previous method of generating bounds, which transformed and
    # extended the bounds based on only the bottom left and top right corners of the bounding box
    origin_area = GeoSeries(
        [
            Polygon(
                [
                    [-46.7119327, -23.7710972],
                    [-46.7120312, -23.7710941],
                    [-46.7118835, -23.7693584],
                ]
            )
        ]
    )

    computed_bounds = generate_bounds(target_geometries=origin_area, resolution=120, max_unit_size=1000)
    actual_splits = [bbox_to_dimensions(b, resolution=10) for b in computed_bounds]

    expected_bounds = [(12, 20)]
    assert actual_splits == expected_bounds

    assert origin_area.iloc[0].within(computed_bounds[0].geometry)


def test_generate_bounds_from_multipolygon_drops_unused_splits():
    """If the input is a multipolygon which only overlaps some of the split bounds, the 'unused' bounds should be
    dropped from the computed bounds.
    """
    expected_bounds = [
        BBox(((8.0859375, 47.5172007), (8.3111979, 47.6764922)), crs=CRS.WGS84),
        BBox(((8.5364584, 47.6764922), (8.7617188, 47.8357836)), crs=CRS.WGS84),
    ]

    multipolygon = GeoSeries(
        [
            MultiPolygon(
                [
                    geometry.box(8.0859375, 47.5172007, 8.2617188, 47.6357836),
                    geometry.box(8.5859375, 47.7172007, 8.7617188, 47.8357836),
                ]
            )
        ]
    )
    computed_bounds = generate_bounds(target_geometries=multipolygon, resolution=10, max_unit_size=2000)

    assert computed_bounds == expected_bounds
