"""Regressietests voor bolgeometrie van geografische footprints."""

import math
import pytest


def test_target_inside_polygon_has_zero_distance(polygon_module):
    polygon = [
        (50.9, 3.9),
        (50.9, 4.1),
        (51.1, 4.1),
        (51.1, 3.9),
    ]

    distance, lat, lon, inside = polygon_module.closest_point_on_polygon(
        51.0, 4.0, polygon
    )

    assert distance == 0.0
    assert (lat, lon) == (51.0, 4.0)
    assert inside is True


def test_outside_distance_uses_edge_instead_of_vertices(polygon_module):
    polygon = [
        (50.8, 4.1),
        (51.2, 4.1),
        (51.2, 4.2),
        (50.8, 4.2),
    ]

    distance, lat, lon, inside = polygon_module.closest_point_on_polygon(
        51.0, 4.0, polygon
    )

    assert distance == pytest.approx(7.0, abs=0.2)
    # De loodrechte bolprojectie op een meridiaan ligt enkele meters
    # noordelijker dan de vlakke equirectangulaire benadering.
    assert lat == pytest.approx(51.00004268, abs=1e-7)
    assert lon == pytest.approx(4.1, abs=1e-6)
    assert inside is False


def test_two_point_footprint_is_measured_as_segment(polygon_module):
    result = polygon_module.closest_point_on_polygon(
        51.0,
        4.0,
        [(50.8, 4.1), (51.2, 4.1)],
    )

    assert result is not None
    distance, lat, lon, inside = result
    assert distance == pytest.approx(7.0, abs=0.2)
    assert lat == pytest.approx(51.00004268, abs=1e-7)
    assert lon == pytest.approx(4.1, abs=1e-6)
    assert inside is False


def test_one_point_legacy_footprint_remains_supported(polygon_module):
    distance, latitude, longitude, inside = (
        polygon_module.closest_point_on_polygon(
            51.0,
            4.0,
            [(51.0, 4.1)],
        )
    )

    assert distance == pytest.approx(7.0, abs=0.2)
    assert (latitude, longitude) == (51.0, 4.1)
    assert inside is False


def test_duplicate_closing_point_and_outer_boundary_are_supported(
    polygon_module,
):
    closed = [
        (-1.0, -1.0),
        (-1.0, 1.0),
        (1.0, 1.0),
        (1.0, -1.0),
        (-1.0, -1.0),
    ]

    assert polygon_module.closest_point_on_polygon(
        0.0,
        -1.0,
        closed,
    ) == (0.0, 0.0, -1.0, True)


def test_polygon_hole_excludes_its_interior_but_not_its_boundary(
    polygon_module,
):
    outer = [
        (-2.0, -2.0),
        (-2.0, 2.0),
        (2.0, 2.0),
        (2.0, -2.0),
    ]
    hole = [
        (-1.0, -1.0),
        (-1.0, 1.0),
        (1.0, 1.0),
        (1.0, -1.0),
    ]

    distance, _, _, inside = polygon_module.closest_point_on_polygon(
        0.0,
        0.0,
        [outer, hole],
    )
    assert distance == pytest.approx(111.195, abs=0.05)
    assert inside is False

    assert polygon_module.closest_point_on_polygon(
        0.0,
        1.0,
        [outer, hole],
    ) == (0.0, 0.0, 1.0, True)
    assert polygon_module.closest_point_on_polygon(
        0.0,
        1.5,
        [outer, hole],
    ) == (0.0, 0.0, 1.5, True)


def test_native_multipolygon_uses_union_and_nearest_component(polygon_module):
    western = [
        (-1.0, -1.0),
        (-1.0, 1.0),
        (1.0, 1.0),
        (1.0, -1.0),
    ]
    eastern = [
        (-1.0, 19.0),
        (-1.0, 21.0),
        (1.0, 21.0),
        (1.0, 19.0),
    ]
    geometry = [[western], [eastern]]

    assert polygon_module.closest_point_on_polygon(
        0.0,
        20.0,
        geometry,
    ) == (0.0, 0.0, 20.0, True)

    distance, _, longitude, inside = (
        polygon_module.closest_point_on_polygon(0.0, 10.0, geometry)
    )
    assert distance == pytest.approx(1000.756, abs=0.1)
    assert longitude == pytest.approx(1.0, abs=1e-6)
    assert inside is False


def test_geojson_coordinates_are_lon_lat_and_preserve_holes(polygon_module):
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [-2.0, -2.0],
                [2.0, -2.0],
                [2.0, 2.0],
                [-2.0, 2.0],
                [-2.0, -2.0],
            ],
            [
                [-1.0, -1.0],
                [1.0, -1.0],
                [1.0, 1.0],
                [-1.0, 1.0],
                [-1.0, -1.0],
            ],
        ],
    }

    distance, _, _, inside = polygon_module.closest_point_on_polygon(
        0.0,
        0.0,
        geometry,
    )
    assert distance == pytest.approx(111.195, abs=0.05)
    assert inside is False
    assert polygon_module.closest_point_on_polygon(
        0.0,
        1.5,
        geometry,
    ) == (0.0, 0.0, 1.5, True)


def test_geojson_multipolygon_uses_every_component(polygon_module):
    geometry = {
        "type": "MultiPolygon",
        "coordinates": [
            [[[-1.0, -1.0], [1.0, -1.0], [1.0, 1.0], [-1.0, 1.0]]],
            [[[19.0, -1.0], [21.0, -1.0], [21.0, 1.0], [19.0, 1.0]]],
        ],
    }

    assert polygon_module.closest_point_on_polygon(
        0.0,
        20.0,
        geometry,
    ) == (0.0, 0.0, 20.0, True)


def test_antimeridian_polygon_contains_both_longitude_spellings(
    polygon_module,
):
    polygon = [
        (10.0, 179.0),
        (10.0, -179.0),
        (12.0, -179.0),
        (12.0, 179.0),
    ]

    assert polygon_module.closest_point_on_polygon(
        11.0,
        180.0,
        polygon,
    ) == (0.0, 11.0, -180.0, True)
    assert polygon_module.closest_point_on_polygon(
        11.0,
        -180.0,
        polygon,
    ) == (0.0, 11.0, -180.0, True)

    distance, _, longitude, inside = (
        polygon_module.closest_point_on_polygon(11.0, 178.0, polygon)
    )
    assert distance == pytest.approx(109.2, abs=0.3)
    assert abs(longitude - 179.0) < 1e-6
    assert inside is False


def test_polygon_around_north_pole_is_not_flattened_away(polygon_module):
    polar_cap = [
        (88.0, -45.0),
        (88.0, 45.0),
        (88.0, 135.0),
        (88.0, -135.0),
    ]

    assert polygon_module.closest_point_on_polygon(
        90.0,
        0.0,
        polar_cap,
    ) == (0.0, 90.0, 0.0, True)

    distance, latitude, _, inside = polygon_module.closest_point_on_polygon(
        86.0,
        0.0,
        polar_cap,
    )
    assert distance > 200.0
    assert latitude > 87.0
    assert inside is False


@pytest.mark.parametrize(
    "geometry",
    [
        [(0.0, 0.0), (0.0, "ongeldig"), (1.0, 1.0)],
        [(91.0, 0.0), (0.0, 1.0), (1.0, 0.0)],
        [(0.0, 181.0), (0.0, 1.0), (1.0, 0.0)],
        [(0.0, 0.0), (0.0, 180.0)],
        [(0.0, 0.0), (1.0, 1.0), (0.0, 1.0), (1.0, 0.0)],
        {
            "type": "Polygon",
            "coordinates": [[[0.0, 0.0], [1.0, 1.0]]],
        },
        {"type": "GeometryCollection", "geometries": []},
        {"type": [], "coordinates": []},
    ],
)
def test_invalid_or_ambiguous_geometry_fails_closed(
    polygon_module,
    geometry,
):
    assert (
        polygon_module.closest_point_on_polygon(0.0, 0.0, geometry)
        is None
    )


def test_hole_outside_outer_ring_fails_closed(polygon_module):
    outer = [
        (-2.0, -2.0),
        (-2.0, 2.0),
        (2.0, 2.0),
        (2.0, -2.0),
    ]
    outside_hole = [
        (3.0, 3.0),
        (3.0, 4.0),
        (4.0, 4.0),
        (4.0, 3.0),
    ]

    assert (
        polygon_module.closest_point_on_polygon(
            0.0,
            0.0,
            [outer, outside_hole],
        )
        is None
    )


def test_invalid_targets_fail_closed_instead_of_raising(polygon_module):
    polygon = [(0.0, 0.0), (0.0, 1.0), (1.0, 0.0)]

    assert polygon_module.closest_point_on_polygon(
        math.nan,
        0.0,
        polygon,
    ) is None
    assert polygon_module.closest_point_on_polygon(
        0.0,
        object(),
        polygon,
    ) is None
