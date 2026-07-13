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


MCS_CONVECTIVE_DBZ = 40.0
MCS_INTENSE_DBZ = 50.0
MCS_MIN_CONVECTIVE_CELLS = 2
MCS_MIN_SPAN_KM = 100.0
MCS_MIN_DURATION_MINUTES = 180.0
MCS_MAX_FRAME_GAP_MINUTES = 20.0
MCS_HISTORY_HOURS = 6.0


@dataclass(slots=True)
class RadarCellSnapshot:
    """Compacte lokale radarcel binnen een groter WeatherSystem."""
    cell_id: str
    timestamp: float
    lat: float
    lon: float
    intensity: int
    area_km2: float
    max_dbz: Optional[float] = None
    footprint_points: tuple[tuple[float, float], ...] = ()
    parent_system_id: Optional[str] = None


@dataclass(slots=True)
class RadarSystemFrame:
    """Momentopname van één parent-radarsysteem in één OPERA-frame."""
    parent_system_id: str
    timestamp: float
    area_km2: float
    footprint_points: tuple[tuple[float, float], ...] = ()
    child_ids: set[str] = field(default_factory=set)
    convective_points: list[tuple[float, float]] = field(default_factory=list)
    intense_cell_count: int = 0
    max_dbz: float = 0.0
    _precipitation_span_cache: Optional[float] = field(default=None, repr=False)
    _convective_span_cache: Optional[float] = field(default=None, repr=False)

    def add_cell(self, cell: RadarCellSnapshot) -> None:
        if cell.cell_id in self.child_ids:
            return
        self.child_ids.add(cell.cell_id)
        dbz = cell.max_dbz or 0.0
        self.max_dbz = max(self.max_dbz, dbz)
        if dbz >= MCS_CONVECTIVE_DBZ:
            self.convective_points.append((cell.lat, cell.lon))
            self._convective_span_cache = None
        if dbz >= MCS_INTENSE_DBZ:
            self.intense_cell_count += 1

    @property
    def convective_cell_count(self) -> int:
        return len(self.convective_points)

    @property
    def precipitation_span_km(self) -> float:
        if self._precipitation_span_cache is None:
            self._precipitation_span_cache = _point_span_km(
                self.footprint_points
            )
        return self._precipitation_span_cache

    @property
    def convective_span_km(self) -> float:
        if self._convective_span_cache is None:
            self._convective_span_cache = _point_span_km(
                tuple(self.convective_points)
            )
        return self._convective_span_cache

    @property
    def meets_mcs_shape(self) -> bool:
        return (
            self.convective_cell_count >= MCS_MIN_CONVECTIVE_CELLS
            and self.convective_span_km >= MCS_MIN_SPAN_KM
            and self.intense_cell_count >= 1
        )


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
    radar_system_frames: dict[str, RadarSystemFrame] = field(
        default_factory=dict, repr=False
    )

    # Operationele classificatie. Eén radarframe kan alleen een kandidaat
    # opleveren; pas voldoende aaneengesloten historie bevestigt een MCS.
    system_type: str = "unknown"
    mcs_status: str = "not_evaluated"
    mcs_candidate_since: Optional[float] = None
    mcs_duration_minutes: float = 0.0
    mcs_convective_span_km: float = 0.0
    mcs_precipitation_span_km: float = 0.0
    mcs_convective_cells: int = 0
    mcs_intense_cells: int = 0
    mcs_parent_area_km2: float = 0.0

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

        frame_cutoff = time.time() - MCS_HISTORY_HOURS * 3600
        self.radar_system_frames = {
            key: frame for key, frame in self.radar_system_frames.items()
            if frame.timestamp >= frame_cutoff
        }

    def record_radar_cell(self, obs) -> None:
        """Bewaar een lokale cel en werk de bijbehorende parent-frame bij."""
        cell_id = obs.radar_cell_id or (
            f"{obs.source}:{obs.timestamp:.0f}:{obs.lat:.4f}:{obs.lon:.4f}"
        )
        parent_system_id = obs.parent_system_id
        cell = RadarCellSnapshot(
            cell_id=cell_id,
            timestamp=obs.timestamp,
            lat=obs.lat,
            lon=obs.lon,
            intensity=obs.intensity or 0,
            area_km2=obs.area_km2 or 0.0,
            max_dbz=obs.max_dbz,
            footprint_points=tuple(obs.footprint_points or ()),
            parent_system_id=parent_system_id,
        )
        self.radar_cells[cell_id] = cell

        if not parent_system_id:
            return
        self.source_system_ids.add(parent_system_id)
        self._source_system_last_seen[parent_system_id] = obs.timestamp
        if obs.parent_area_km2 is not None:
            self.parent_system_areas[parent_system_id] = obs.parent_area_km2
        if obs.parent_footprint_points:
            self.parent_system_footprints[parent_system_id] = tuple(
                obs.parent_footprint_points
            )

        frame = self.radar_system_frames.get(parent_system_id)
        if frame is None:
            frame = RadarSystemFrame(
                parent_system_id=parent_system_id,
                timestamp=obs.timestamp,
                area_km2=obs.parent_area_km2 or obs.area_km2 or 0.0,
                footprint_points=tuple(obs.parent_footprint_points or ()),
            )
            self.radar_system_frames[parent_system_id] = frame
        frame.add_cell(cell)

    def update_radar_classification(self) -> None:
        """Classificeer de meest recente radarhistorie conservatief als MCS."""
        if not self.radar_system_frames:
            self.system_type = "unknown"
            self.mcs_status = "not_evaluated"
            return

        # Eén storm kan door merge meerdere parents op hetzelfde tijdstip
        # bevatten. Gebruik per tijdstip de meteorologisch sterkste parent.
        by_timestamp: dict[float, RadarSystemFrame] = {}
        for frame in self.radar_system_frames.values():
            current = by_timestamp.get(frame.timestamp)
            score = (
                frame.meets_mcs_shape,
                frame.convective_span_km,
                frame.max_dbz,
                frame.area_km2,
            )
            if current is None or score > (
                current.meets_mcs_shape,
                current.convective_span_km,
                current.max_dbz,
                current.area_km2,
            ):
                by_timestamp[frame.timestamp] = frame

        frames = sorted(by_timestamp.values(), key=lambda item: item.timestamp)
        latest = frames[-1]
        self.mcs_convective_span_km = round(latest.convective_span_km, 1)
        self.mcs_precipitation_span_km = round(latest.precipitation_span_km, 1)
        self.mcs_convective_cells = latest.convective_cell_count
        self.mcs_intense_cells = latest.intense_cell_count
        self.mcs_parent_area_km2 = latest.area_km2

        if not latest.meets_mcs_shape:
            self.mcs_candidate_since = None
            self.mcs_duration_minutes = 0.0
            self.mcs_status = "not_mcs"
            self.system_type = (
                "convective_cluster"
                if latest.convective_cell_count else "rain_area"
            )
            return

        sequence = [latest]
        next_frame = latest
        for frame in reversed(frames[:-1]):
            gap_minutes = (next_frame.timestamp - frame.timestamp) / 60.0
            if gap_minutes > MCS_MAX_FRAME_GAP_MINUTES or not frame.meets_mcs_shape:
                break
            sequence.append(frame)
            next_frame = frame

        first = sequence[-1]
        duration = max(0.0, (latest.timestamp - first.timestamp) / 60.0)
        self.mcs_candidate_since = first.timestamp
        self.mcs_duration_minutes = round(duration, 1)
        if duration >= MCS_MIN_DURATION_MINUTES:
            self.mcs_status = "confirmed"
            self.system_type = "mcs"
        else:
            self.mcs_status = "candidate"
            self.system_type = "mcs_candidate"

    def closest_radar_point(
        self, target_lat: float, target_lon: float
    ) -> Optional[tuple[float, float, float]]:
        """Geef (afstand_km, lat, lon) van de dichtste lokale radarcel."""
        if not self.radar_cells:
            return None
        candidates = []
        for cell in self.radar_cells.values():
            points = cell.footprint_points or ((cell.lat, cell.lon),)
            distance, point = min(
                (_haversine_km(target_lat, target_lon, lat, lon), (lat, lon))
                for lat, lon in points
            )
            candidates.append((distance, point[0], point[1]))
        return min(candidates, key=lambda item: item[0])


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _point_span_km(points: tuple[tuple[float, float], ...]) -> float:
    """Lineaire benadering van de diameter via een dubbele farthest sweep."""
    if len(points) < 2:
        return 0.0

    def farthest(origin):
        return max(
            points,
            key=lambda point: _haversine_km(
                origin[0], origin[1], point[0], point[1]
            ),
        )

    first = farthest(points[0])
    second = farthest(first)
    return _haversine_km(first[0], first[1], second[0], second[1])
