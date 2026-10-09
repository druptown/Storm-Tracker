"""Storm Tracker V3 — geometry/bounding_box.py v0.1.0

Voorstel pagina 9: "Eerst bounding box. Pas daarna polygon."

De bounding box is een O(n) min/max-berekening — extreem goedkoop
vergeleken met een convex hull. Door eerst te checken of de bounding box
is veranderd, vermijden we onnodige hull-herberekeningen wanneer een
storm nauwelijks beweegt.
"""
from __future__ import annotations

from typing import Optional


def compute_bounding_box(
    points: list[tuple[float, float]]
) -> Optional[tuple[float, float, float, float]]:
    """
    Bereken de bounding box van een lijst (lat, lon) punten.

    Geeft (lat_min, lat_max, lon_min, lon_max) terug, of None als
    de lijst leeg is.
    """
    if not points:
        return None

    lats = [p[0] for p in points]
    lons = [p[1] for p in points]
    return (min(lats), max(lats), min(lons), max(lons))


def bounding_box_changed(
    old_box: Optional[tuple[float, float, float, float]],
    new_box: Optional[tuple[float, float, float, float]],
    threshold_deg: float = 0.01,   # ~1.1km bij de evenaar
) -> bool:
    """
    True als de nieuwe bounding box significant afwijkt van de oude.

    Kleine fluctuaties (< threshold_deg) tellen niet als wijziging —
    dit voorkomt dat elke nieuwe strike binnen een al bestaande hull
    toch een volledige hull-herberekening triggert.
    """
    if old_box is None or new_box is None:
        return old_box != new_box

    return any(
        abs(a - b) > threshold_deg
        for a, b in zip(old_box, new_box)
    )
