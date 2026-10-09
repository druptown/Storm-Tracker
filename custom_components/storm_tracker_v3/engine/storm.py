"""Storm Tracker V3 — engine/storm.py v0.1.0

Storm dataclass — het centrale object in de architectuur.

Een Storm wordt aangemaakt, geüpdatet, en verwijderd.
Nooit opnieuw aangemaakt voor hetzelfde fysieke systeem.
Alle zware berekeningen worden gecachet.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Storm:
    """
    Eén actief onweersysteem.

    Identiteit: storm_id is stabiel zolang het systeem actief is.
    Alle berekeningen worden gecachet en alleen herberekend als de data verandert.
    """
    storm_id:    str   = field(default_factory=lambda: str(uuid.uuid4())[:8])
    centroid_lat: float = 0.0
    centroid_lon: float = 0.0
    first_seen:  float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)

    # Inslagen
    strike_count:  int   = 0
    strikes_5min:  int   = 0
    strikes_60min: int   = 0

    # Beweging (gecachet)
    heading_deg:   Optional[float] = None   # richting in graden (0=N)
    speed_kmh:     Optional[float] = None   # snelheid km/h
    confidence:    str             = "Onvoldoende data"

    # Geometrie (gecachet)
    # Voorstel pagina 9: "Eerst bounding box. Pas daarna polygon." — de
    # bounding box is goedkoop (min/max over alle strike-coördinaten) en
    # wordt gebruikt om dure operaties (convex hull, geocoding) te
    # vermijden wanneer de storm nauwelijks bewogen is.
    bounding_box:  Optional[tuple[float, float, float, float]] = None
    # (lat_min, lat_max, lon_min, lon_max), None tot voor het eerst berekend
    hull:          list[tuple[float, float]] = field(default_factory=list)  # [(lat,lon),...]
    radius_km:     float = 0.0

    # Geocoding (gecachet)
    location_name: str = ""

    # Passage (gecachet per persoon)
    _cached_projections: dict = field(default_factory=dict, repr=False)

    # Radar-observaties (gecachet)
    # Elk element: (timestamp, lat, lon, intensity)
    max_radar_intensity:  int = 0   # hoogste intensiteit (0-8) gezien in huidige cyclus
    _radar_observations:  list = field(default_factory=list, repr=False)

    # Netatmo-verificatie
    # Tellers worden per poll-cyclus bijgehouden; de Coordinator kan
    # op basis van de verhouding de confidence bijstellen.
    netatmo_confirmations:  int = 0   # stations die regen meldden nabij storm
    netatmo_no_rain_count:  int = 0   # stations zonder regen in storm-pad

    # Lifecycle
    is_dormant: bool = False   # True = geen recente observaties, geen nieuwe toewijzing

    # Intern
    _strike_history: list[tuple[float, float, float]] = field(
        default_factory=list, repr=False
    )  # [(ts, lat, lon), ...]
    _dirty: bool = True   # True = cache moet herberekend worden

    def add_strikes(self, strikes: list) -> None:
        """Voeg strikes toe en markeer cache als vervallen."""
        now = time.time()
        for s in strikes:
            self._strike_history.append((s.timestamp, s.lat, s.lon))
        self.last_update = now
        self.strike_count += len(strikes)
        self.is_dormant = False
        self._dirty = True
        self._cached_projections.clear()

    def is_expired(self, expire_minutes: float) -> bool:
        """True als de storm langer dan expire_minutes geen updates kreeg."""
        return (time.time() - self.last_update) > expire_minutes * 60

    def strikes_in_window(self, minutes: int) -> list:
        """Strikes van de laatste N minuten."""
        cutoff = time.time() - minutes * 60
        return [(ts, lat, lon) for ts, lat, lon in self._strike_history if ts >= cutoff]

    def update_counts(self) -> None:
        """Herbereken strike counts (goedkoop)."""
        self.strikes_5min  = len(self.strikes_in_window(5))
        self.strikes_60min = len(self.strikes_in_window(60))

    def prune_history(self, max_age_minutes: int = 90) -> None:
        """Verwijder oude strikes uit history."""
        cutoff = time.time() - max_age_minutes * 60
        self._strike_history = [(ts, la, lo) for ts, la, lo in self._strike_history if ts >= cutoff]
