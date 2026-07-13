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


@dataclass(slots=True)
class RadarCellSnapshot:
    """Compacte lokale radarcel binnen een groter WeatherSystem."""
    cell_id: str
    timestamp: float
    lat: float
    lon: float
    intensity: int
    area_km2: float
    footprint_points: tuple[tuple[float, float], ...] = ()
    parent_system_id: Optional[str] = None


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
    radar_cells: dict[str, RadarCellSnapshot] = field(default_factory=dict)
    source_system_ids: set[str] = field(default_factory=set)
    parent_system_areas: dict[str, float] = field(default_factory=dict)
    parent_system_footprints: dict[
        str, tuple[tuple[float, float], ...]
    ] = field(default_factory=dict, repr=False)
    _source_system_last_seen: dict[str, float] = field(
        default_factory=dict, repr=False
    )

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

    def prune_radar_cells(self, max_age_minutes: int = 20) -> None:
        """Houd alleen recente lokale radarcellen en bron-parent-ID's bij."""
        cutoff = time.time() - max_age_minutes * 60
        self.radar_cells = {
            key: cell for key, cell in self.radar_cells.items()
            if cell.timestamp >= cutoff
        }
        stale_parents = [
            parent_id
            for parent_id, last_seen in self._source_system_last_seen.items()
            if last_seen < cutoff
        ]
        for parent_id in stale_parents:
            self._source_system_last_seen.pop(parent_id, None)
            self.source_system_ids.discard(parent_id)
            self.parent_system_areas.pop(parent_id, None)
            self.parent_system_footprints.pop(parent_id, None)

    def closest_radar_point(
        self, target_lat: float, target_lon: float
    ) -> Optional[tuple[float, float, float]]:
        """Geef (afstand_km, lat, lon) van de dichtste lokale radarcel."""
        if not self.radar_cells:
            return None
        from .storm_engine import _haversine

        candidates = []
        for cell in self.radar_cells.values():
            points = cell.footprint_points or ((cell.lat, cell.lon),)
            distance, point = min(
                (_haversine(target_lat, target_lon, lat, lon), (lat, lon))
                for lat, lon in points
            )
            candidates.append((distance, point[0], point[1]))
        return min(candidates, key=lambda item: item[0])
