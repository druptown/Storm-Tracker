"""Storm Tracker V3 — geometry/geocode.py v0.1.0

Geocoding: zoek de dichtstbijzijnde plaatsnaam bij een coördinaat.

Deze module bevat uitsluitend de PURE opzoekfunctie — geen bestand-I/O,
geen netwerkverkeer. Het laden van een plaatsendatabase (bv. een
places.json met [naam, land, lat, lon] entries) is een verantwoordelijk-
heid van de Coordinator-laag, die de geladen lijst doorgeeft aan de
StormEngine. Dit houdt de geometrie-module testbaar zonder I/O-mocking
en herbruikbaar (zie V2-ervaring: dezelfde lookup-logica werd daar
inline herhaald in coordinator.py — hier dus eenmalig, op één plek).
"""
from __future__ import annotations

from typing import NamedTuple, Optional


class PlaceEntry(NamedTuple):
    name:    str
    country: str
    lat:     float
    lon:     float


def nearest_place(
    lat: float,
    lon: float,
    places: list[PlaceEntry],
    threshold_deg: float = 0.5,
) -> str:
    """
    Zoek de dichtstbijzijnde plaats binnen threshold_deg (ruwe graden-
    afstand, niet haversine — voor deze grove eerste filter is dat
    voldoende en veel goedkoper dan voor elke kandidaat een volledige
    haversine te berekenen; de exacte volgorde binnen de drempel doet
    er voor een plaatsnaam-label niet toe).

    Geeft "" terug als geen enkele plaats binnen de drempel valt of
    de places-lijst leeg is.
    """
    if not places:
        return ""

    best_name = ""
    best_dist = threshold_deg

    for place in places:
        # Goedkope vlakke afstand (geen haversine nodig voor deze grove match)
        dist = abs(place.lat - lat) + abs(place.lon - lon) * 0.7
        if dist < best_dist:
            best_dist = dist
            best_name = place.name

    return best_name
