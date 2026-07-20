"""Lokale reverse-geocoding voor targets op basis van GeoNames."""
from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ResolvedLocation:
    place: str | None
    country_code: str | None
    distance_km: float | None


def load_places_json(path: Path) -> tuple[tuple[str, str, float, float], ...]:
    """Laad lokale ``[plaats, landcode, lat, lon]``-entries uit JSON."""
    with path.open(encoding="utf-8-sig") as stream:
        payload = json.load(stream)
    rows = payload.get("places", []) if isinstance(payload, dict) else payload
    result = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 4:
            continue
        try:
            result.append((str(row[0]), str(row[1]).upper(), float(row[2]), float(row[3])))
        except (TypeError, ValueError):
            continue
    return tuple(result)


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    value = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def resolve_location(
    lat: float,
    lon: float,
    places: tuple[tuple[str, str, float, float], ...],
    *,
    preferred_place: str | None = None,
    max_distance_km: float = 100.0,
) -> ResolvedLocation:
    """Zoek lokaal de dichtstbijzijnde stad en bijbehorende ISO-landcode."""
    best = None
    for name, country_code, place_lat, place_lon in places:
        if abs(place_lat - lat) > 1.0:
            continue
        lon_margin = 1.5 / max(0.2, abs(math.cos(math.radians(lat))))
        if abs(place_lon - lon) > lon_margin:
            continue
        distance = _distance_km(lat, lon, place_lat, place_lon)
        if best is None or distance < best[0]:
            best = (distance, name, country_code)
    if best is None or best[0] > max_distance_km:
        return ResolvedLocation(preferred_place or None, None, None)
    return ResolvedLocation(
        preferred_place or best[1], best[2], round(best[0], 1)
    )
