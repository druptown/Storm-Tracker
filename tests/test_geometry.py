"""Storm Tracker V3 — tests/test_geometry.py v0.1.0

Tests voor de pure geometrie-helpers (geen HA-, netwerk- of
bestandsafhankelijkheden) — voorheen volledig ongetest.
"""
from __future__ import annotations

import math

import pytest


# ── distance.py ────────────────────────────────────────────────────────────

def test_haversine_zero_distance_for_same_point(distance_module):
    assert distance_module.haversine(51.0, 4.0, 51.0, 4.0) == pytest.approx(0.0, abs=1e-9)


def test_haversine_known_distance_brussels_paris(distance_module):
    # Brussel -> Parijs is ~264km hemelsbreed (bekende referentiewaarde)
    d = distance_module.haversine(50.8503, 4.3517, 48.8566, 2.3522)
    assert 255 < d < 275


def test_haversine_symmetric(distance_module):
    d1 = distance_module.haversine(51.0, 4.0, 52.0, 5.0)
    d2 = distance_module.haversine(52.0, 5.0, 51.0, 4.0)
    assert d1 == pytest.approx(d2)


def test_bearing_due_north(distance_module):
    # Recht naar het noorden: zelfde lon, hogere lat -> bearing ~0
    b = distance_module.bearing(51.0, 4.0, 52.0, 4.0)
    assert b == pytest.approx(0.0, abs=0.5)


def test_bearing_due_east(distance_module):
    b = distance_module.bearing(51.0, 4.0, 51.0, 5.0)
    assert b == pytest.approx(90.0, abs=1.0)


def test_destination_roundtrip_matches_haversine(distance_module):
    """Een punt 100km naar het oosten bepalen en de afstand terugmeten moet ~100km geven."""
    lat2, lon2 = distance_module.destination(51.0, 4.0, bearing_deg=90.0, dist_km=100.0)
    d = distance_module.haversine(51.0, 4.0, lat2, lon2)
    assert d == pytest.approx(100.0, abs=0.5)


# ── bounding_box.py ─────────────────────────────────────────────────────────

def test_compute_bounding_box_empty_list_returns_none(bounding_box_module):
    assert bounding_box_module.compute_bounding_box([]) is None


def test_compute_bounding_box_single_point(bounding_box_module):
    box = bounding_box_module.compute_bounding_box([(51.0, 4.0)])
    assert box == (51.0, 51.0, 4.0, 4.0)


def test_compute_bounding_box_multiple_points(bounding_box_module):
    points = [(51.0, 4.0), (52.5, 3.0), (50.0, 5.5)]
    box = bounding_box_module.compute_bounding_box(points)
    assert box == (50.0, 52.5, 3.0, 5.5)


def test_bounding_box_changed_none_vs_none(bounding_box_module):
    assert bounding_box_module.bounding_box_changed(None, None) is False


def test_bounding_box_changed_none_vs_value(bounding_box_module):
    assert bounding_box_module.bounding_box_changed(None, (1, 2, 3, 4)) is True
    assert bounding_box_module.bounding_box_changed((1, 2, 3, 4), None) is True


def test_bounding_box_changed_small_fluctuation_is_not_a_change(bounding_box_module):
    old = (51.000, 51.100, 4.000, 4.100)
    new = (51.001, 51.100, 4.000, 4.100)  # verschil 0.001 < threshold 0.01
    assert bounding_box_module.bounding_box_changed(old, new) is False


def test_bounding_box_changed_large_shift_is_a_change(bounding_box_module):
    old = (51.000, 51.100, 4.000, 4.100)
    new = (51.500, 51.100, 4.000, 4.100)  # verschil 0.5 > threshold
    assert bounding_box_module.bounding_box_changed(old, new) is True


# ── hull.py ─────────────────────────────────────────────────────────────────

def test_convex_hull_fewer_than_3_points_returns_input(hull_module):
    pts = [(51.0, 4.0), (52.0, 5.0)]
    assert hull_module.convex_hull(pts) == sorted(set(pts))


def test_convex_hull_square_returns_4_corners(hull_module):
    square = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]
    hull = hull_module.convex_hull(square)
    assert len(hull) == 4
    assert set(hull) == set(square)


def test_convex_hull_interior_point_excluded(hull_module):
    """Een punt binnen het vierkant mag NIET in de hull zitten."""
    points = [(0.0, 0.0), (0.0, 2.0), (2.0, 2.0), (2.0, 0.0), (1.0, 1.0)]  # laatste = middelpunt
    hull = hull_module.convex_hull(points)
    assert (1.0, 1.0) not in hull
    assert len(hull) == 4


def test_hull_radius_km_empty_hull_is_zero(hull_module):
    assert hull_module.hull_radius_km([], 51.0, 4.0) == 0.0


def test_hull_radius_km_matches_farthest_point(hull_module, distance_module):
    hull = [(51.0, 4.0), (51.5, 4.0), (51.0, 5.0)]
    center_lat, center_lon = 51.0, 4.0
    expected = max(
        distance_module.haversine(center_lat, center_lon, lat, lon)
        for lat, lon in hull
    )
    assert hull_module.hull_radius_km(hull, center_lat, center_lon) == pytest.approx(expected)


# ── geocode.py ────────────────────────────────────────────────────────────

def test_nearest_place_empty_places_returns_empty_string(geocode_module):
    assert geocode_module.nearest_place(51.0, 4.0, []) == ""


def test_nearest_place_finds_closest_within_threshold(geocode_module):
    PlaceEntry = geocode_module.PlaceEntry
    places = [
        PlaceEntry("Mechelen", "BE", 51.0259, 4.4776),
        PlaceEntry("Antwerpen", "BE", 51.2194, 4.4025),
        PlaceEntry("Parijs", "FR", 48.8566, 2.3522),
    ]
    # Dicht bij Mechelen
    name = geocode_module.nearest_place(51.026, 4.478, places)
    assert name == "Mechelen"


def test_nearest_place_outside_threshold_returns_empty(geocode_module):
    PlaceEntry = geocode_module.PlaceEntry
    places = [PlaceEntry("Mechelen", "BE", 51.0259, 4.4776)]
    # Ver weg (Reykjavik) -> buiten de default threshold van 0.5 graden
    name = geocode_module.nearest_place(64.1466, -21.9426, places)
    assert name == ""
