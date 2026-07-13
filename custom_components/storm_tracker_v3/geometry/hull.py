"""Storm Tracker V3 — geometry/hull.py v0.1.0

Convex hull berekening via Andrew's monotone chain algoritme.
Pure Python, geen externe dependencies (shapely/scipy bewust vermeden,
zie eerder spatial-index onderzoek — dezelfde overwegingen gelden hier:
voor de kleine puntenaantallen per storm is een eenvoudige O(n log n)
implementatie ruim snel genoeg en installeerbaar overal).

Werkt direct op (lat, lon) als (x, y) — een vlakke benadering die voor
de regionale schaal van een storm (enkele tot enkele tientallen km)
voldoende nauwkeurig is. Voor zeer grote stormen nabij de polen zou dit
vervormen, maar dat scenario doet zich in deze toepassing niet voor.
"""
from __future__ import annotations


def convex_hull(points: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """
    Bereken de convex hull van een lijst (lat, lon) punten via
    Andrew's monotone chain.

    Geeft de hull-punten terug in tegenwijzerzin, beginnend bij het
    punt met de kleinste coördinaat. Bij minder dan 3 unieke punten
    wordt de invoer (gededupliceerd) teruggegeven.
    """
    pts = sorted(set(points))
    n = len(pts)

    if n < 3:
        return pts

    def cross(o, a, b):
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

    # Onderste hull
    lower: list[tuple[float, float]] = []
    for p in pts:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)

    # Bovenste hull
    upper: list[tuple[float, float]] = []
    for p in reversed(pts):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)

    # Laatste punt van elke helft is het eerste punt van de andere — weglaten
    return lower[:-1] + upper[:-1]


def hull_radius_km(hull: list[tuple[float, float]], center_lat: float, center_lon: float) -> float:
    """Grootste afstand (km) van het centrum tot een hull-punt."""
    if not hull:
        return 0.0

    # Lokale import om circulaire afhankelijkheid met distance.py te vermijden
    from .distance import haversine

    return max(haversine(center_lat, center_lon, lat, lon) for lat, lon in hull)
