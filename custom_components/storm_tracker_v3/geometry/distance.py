"""Storm Tracker V3 — geometry/distance.py v0.1.0

Alle afstandsberekeningen op één plek.
Geen duplicatie in andere modules.
"""
from __future__ import annotations
import math

EARTH_RADIUS_KM = 6371.0


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Afstand in km tussen twee punten (Haversine formule)."""
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return EARTH_RADIUS_KM * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Peiling (graden, 0=N) van punt 1 naar punt 2."""
    dlat = lat2 - lat1
    dlon = (lon2 - lon1) * math.cos(math.radians(lat1))
    return math.degrees(math.atan2(dlon, dlat)) % 360


def destination(lat: float, lon: float, bearing_deg: float, dist_km: float) -> tuple[float, float]:
    """Bestemmingspunt gegeven vertrekpunt, peiling en afstand."""
    R = EARTH_RADIUS_KM
    d = dist_km / R
    lat_r = math.radians(lat)
    lon_r = math.radians(lon)
    b_r = math.radians(bearing_deg)
    lat2 = math.asin(
        math.sin(lat_r) * math.cos(d)
        + math.cos(lat_r) * math.sin(d) * math.cos(b_r)
    )
    lon2 = lon_r + math.atan2(
        math.sin(b_r) * math.sin(d) * math.cos(lat_r),
        math.cos(d) - math.sin(lat_r) * math.sin(lat2),
    )
    return math.degrees(lat2), math.degrees(lon2)
