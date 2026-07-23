"""Storm Tracker V3 — engine/storm.py v0.1.0

Storm dataclass — het centrale object in de architectuur.

Een Storm wordt aangemaakt, geüpdatet, en verwijderd.
Nooit opnieuw aangemaakt voor hetzelfde fysieke systeem.
Alle zware berekeningen worden gecachet.
"""
from __future__ import annotations

import math
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
    motion_sample_count: int = 0
    motion_history_minutes: float = 0.0
    motion_fit_quality: float = 0.0
    motion_model: str = "none"
    motion_basis: str = "unknown"
    motion_prediction_error_km: Optional[float] = None
    motion_model_gain: float = 0.0
    velocity_east_kmh: Optional[float] = None
    velocity_north_kmh: Optional[float] = None
    acceleration_east_kmh2: float = 0.0
    acceleration_north_kmh2: float = 0.0

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
    mcs_sequence_frames: int = 0
    mcs_latest_frame_timestamp: Optional[float] = None
    mcs_evaluation_reason: str = "no_radar_history"

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

    def to_mcs_snapshot(self) -> dict:
        """Serializeer alleen de compacte radarhistoriek die een restart moet overleven."""
        return {
            "storm_id": self.storm_id,
            "centroid_lat": self.centroid_lat,
            "centroid_lon": self.centroid_lon,
            "first_seen": self.first_seen,
            "last_update": self.last_update,
            "frames": [
                {
                    "parent_system_id": frame.parent_system_id,
                    "timestamp": frame.timestamp,
                    "area_km2": frame.area_km2,
                    "footprint_points": [list(point) for point in frame.footprint_points],
                    "child_ids": sorted(frame.child_ids),
                    "convective_points": [list(point) for point in frame.convective_points],
                    "intense_cell_count": frame.intense_cell_count,
                    "max_dbz": frame.max_dbz,
                }
                for frame in self.radar_system_frames.values()
            ],
            "radar_cells": [
                {
                    "cell_id": cell.cell_id,
                    "timestamp": cell.timestamp,
                    "lat": cell.lat,
                    "lon": cell.lon,
                    "intensity": cell.intensity,
                    "area_km2": cell.area_km2,
                    "max_dbz": cell.max_dbz,
                    "footprint_points": [list(point) for point in cell.footprint_points],
                    "parent_system_id": cell.parent_system_id,
                }
                for cell in self.radar_cells.values()
            ],
        }

    @classmethod
    def from_mcs_snapshot(cls, data: dict, now: Optional[float] = None) -> "Storm":
        """Herstel een WeatherSystem met genoeg geometrie om het volgende frame te matchen."""
        current = time.time() if now is None else now
        cutoff = current - MCS_HISTORY_HOURS * 3600
        storm = cls(
            storm_id=str(data["storm_id"]),
            centroid_lat=float(data.get("centroid_lat", 0.0)),
            centroid_lon=float(data.get("centroid_lon", 0.0)),
            first_seen=float(data.get("first_seen", current)),
            last_update=float(data.get("last_update", current)),
        )
        for raw in data.get("frames", []):
            timestamp = float(raw["timestamp"])
            if timestamp < cutoff:
                continue
            frame = RadarSystemFrame(
                parent_system_id=str(raw["parent_system_id"]),
                timestamp=timestamp,
                area_km2=float(raw.get("area_km2", 0.0)),
                footprint_points=tuple(tuple(point) for point in raw.get("footprint_points", [])),
                child_ids=set(raw.get("child_ids", [])),
                convective_points=[tuple(point) for point in raw.get("convective_points", [])],
                intense_cell_count=int(raw.get("intense_cell_count", 0)),
                max_dbz=float(raw.get("max_dbz", 0.0)),
            )
            storm.radar_system_frames[frame.parent_system_id] = frame
        for raw in data.get("radar_cells", []):
            timestamp = float(raw["timestamp"])
            if timestamp < current - 20 * 60:
                continue
            cell = RadarCellSnapshot(
                cell_id=str(raw["cell_id"]),
                timestamp=timestamp,
                lat=float(raw["lat"]),
                lon=float(raw["lon"]),
                intensity=int(raw.get("intensity", 0)),
                area_km2=float(raw.get("area_km2", 0.0)),
                max_dbz=raw.get("max_dbz"),
                footprint_points=tuple(tuple(point) for point in raw.get("footprint_points", [])),
                parent_system_id=raw.get("parent_system_id"),
            )
            storm.radar_cells[cell.cell_id] = cell
            if cell.parent_system_id:
                storm.source_system_ids.add(cell.parent_system_id)
                storm._source_system_last_seen[cell.parent_system_id] = cell.timestamp
        if storm.radar_system_frames:
            storm.last_update = max(
                storm.last_update,
                max(frame.timestamp for frame in storm.radar_system_frames.values()),
            )
            storm.update_radar_classification()
        return storm

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
            self.mcs_candidate_since = None
            self.mcs_duration_minutes = 0.0
            self.mcs_sequence_frames = 0
            self.mcs_latest_frame_timestamp = None
            self.mcs_evaluation_reason = "no_radar_history"
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
        self.mcs_latest_frame_timestamp = latest.timestamp

        if not latest.meets_mcs_shape:
            self.mcs_candidate_since = None
            self.mcs_duration_minutes = 0.0
            self.mcs_sequence_frames = 0
            self.mcs_status = "not_mcs"
            self.system_type = (
                "convective_cluster"
                if latest.convective_cell_count else "rain_area"
            )
            failed = []
            if latest.convective_cell_count < MCS_MIN_CONVECTIVE_CELLS:
                failed.append("insufficient_convective_cells")
            if latest.convective_span_km < MCS_MIN_SPAN_KM:
                failed.append("insufficient_convective_span")
            if latest.intense_cell_count < 1:
                failed.append("no_intense_cell")
            self.mcs_evaluation_reason = ",".join(failed) or "shape_rejected"
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
        self.mcs_sequence_frames = len(sequence)
        if duration >= MCS_MIN_DURATION_MINUTES:
            self.mcs_status = "confirmed"
            self.system_type = "mcs"
            self.mcs_evaluation_reason = "duration_confirmed"
        else:
            self.mcs_status = "candidate"
            self.system_type = "mcs_candidate"
            self.mcs_evaluation_reason = "duration_pending"

    def mcs_diagnostics(self) -> dict:
        """Geef een JSON-veilige verklaring van de laatste MCS-evaluatie."""
        checks = {
            "convective_cells": self.mcs_convective_cells >= MCS_MIN_CONVECTIVE_CELLS,
            "convective_span": self.mcs_convective_span_km >= MCS_MIN_SPAN_KM,
            "intense_cells": self.mcs_intense_cells >= 1,
            "duration": self.mcs_duration_minutes >= MCS_MIN_DURATION_MINUTES,
        }
        return {
            "storm_id": self.storm_id,
            "status": self.mcs_status,
            "system_type": self.system_type,
            "reason": self.mcs_evaluation_reason,
            "candidate_since": self.mcs_candidate_since,
            "latest_frame_timestamp": self.mcs_latest_frame_timestamp,
            "duration_minutes": self.mcs_duration_minutes,
            "sequence_frames": self.mcs_sequence_frames,
            "convective_span_km": self.mcs_convective_span_km,
            "precipitation_span_km": self.mcs_precipitation_span_km,
            "convective_cells": self.mcs_convective_cells,
            "intense_cells": self.mcs_intense_cells,
            "parent_area_km2": self.mcs_parent_area_km2,
            "checks": checks,
            "thresholds": {
                "convective_dbz": MCS_CONVECTIVE_DBZ,
                "intense_dbz": MCS_INTENSE_DBZ,
                "min_convective_cells": MCS_MIN_CONVECTIVE_CELLS,
                "min_convective_span_km": MCS_MIN_SPAN_KM,
                "min_intense_cells": 1,
                "min_duration_minutes": MCS_MIN_DURATION_MINUTES,
                "max_frame_gap_minutes": MCS_MAX_FRAME_GAP_MINUTES,
            },
        }

    @property
    def radar_frame_timestamps(self) -> tuple[float, ...]:
        """Unieke radarmomenten; meerdere cellen in één product tellen één keer."""
        timestamps = {float(cell.timestamp) for cell in self.radar_cells.values()}
        timestamps.update(float(frame.timestamp) for frame in self.radar_system_frames.values())
        return tuple(sorted(timestamps))

    @property
    def consecutive_radar_frames(self) -> int:
        """Aantal aansluitende recente radarproducten in de huidige reeks."""
        timestamps = self.radar_frame_timestamps
        if not timestamps:
            return 0
        sequence_count = 1
        for previous, current in zip(reversed(timestamps[:-1]), reversed(timestamps[1:])):
            if current - previous > MCS_MAX_FRAME_GAP_MINUTES * 60:
                break
            sequence_count += 1
        return sequence_count

    @property
    def tracking_status(self) -> str:
        """Operationele volwassenheid van het systeem voor gebruikersweergave."""
        if self.is_dormant:
            return "sluimerend"
        frames = self.consecutive_radar_frames
        if frames >= 2:
            return "bevestigd"
        if frames == 1:
            return "waargenomen"
        if self.strike_count:
            return "alleen_bliksem"
        return "onvoldoende_data"

    @property
    def last_radar_timestamp(self) -> Optional[float]:
        timestamps = self.radar_frame_timestamps
        return timestamps[-1] if timestamps else None

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

    def motion_to_target(
        self,
        target_lat: float,
        target_lon: float,
        distance_km: Optional[float] = None,
    ) -> dict:
        """Projecteer de bewegingsvector op de richting naar een target."""
        distance = (
            _haversine_km(
                self.centroid_lat, self.centroid_lon, target_lat, target_lon
            )
            if distance_km is None else float(distance_km)
        )
        bearing = _bearing_deg(
            self.centroid_lat, self.centroid_lon, target_lat, target_lon
        )
        if (
            self.heading_deg is None
            or self.speed_kmh is None
            or self.speed_kmh <= 0
            or (self.radar_cells and self.tracking_status != "bevestigd")
        ):
            return {
                "bearing_to_target_deg": round(bearing, 1),
                "approach_speed_kmh": None,
                "moving_towards": None,
                "eta_minutes": None,
                "eta_basis": None,
                "closest_pass_distance_km": None,
                "closest_pass_minutes": None,
                "footprint_pass_distance_km": None,
                "passage_classification": None,
                "passage_uncertainty_km": None,
            }

        samples = self._trajectory_passage_samples(
            target_lat,
            target_lon,
            current_edge_distance_km=distance,
        )
        if not samples:
            return {
                "bearing_to_target_deg": round(bearing, 1),
                "approach_speed_kmh": None,
                "moving_towards": None,
                "eta_minutes": None,
                "eta_basis": None,
                "closest_pass_distance_km": None,
                "closest_pass_minutes": None,
                "footprint_pass_distance_km": None,
                "passage_classification": None,
                "passage_uncertainty_km": None,
            }

        current = samples[0]
        velocity_east = (
            self.velocity_east_kmh
            if self.velocity_east_kmh is not None
            else self.speed_kmh * math.sin(math.radians(self.heading_deg))
        )
        velocity_north = (
            self.velocity_north_kmh
            if self.velocity_north_kmh is not None
            else self.speed_kmh * math.cos(math.radians(self.heading_deg))
        )
        approach_speed = (
            velocity_east * math.sin(math.radians(bearing))
            + velocity_north * math.cos(math.radians(bearing))
        )
        moving_towards = approach_speed > 1.0

        closest_centroid = min(
            samples,
            key=lambda item: (
                item["centroid_distance_km"],
                item["minutes"],
            ),
        )
        closest_footprint = min(
            samples,
            key=lambda item: (
                item["edge_distance_km"],
                item["minutes"],
            ),
        )
        exact_hits = [
            item for item in samples if item["edge_distance_km"] <= 0.1
        ]
        envelope_hits = [
            item for item in samples
            if item["edge_distance_km"] <= item["uncertainty_km"]
        ]
        eta_minutes = None
        eta_basis = None
        reliable_vector = self.confidence in {"Matig", "Hoog"}
        if reliable_vector and exact_hits:
            eta_minutes = exact_hits[0]["minutes"]
            eta_basis = "radarcontour"
        elif reliable_vector and envelope_hits:
            eta_minutes = envelope_hits[0]["minutes"]
            eta_basis = "onzekerheidscorridor"

        relevant_projection = reliable_vector and (
            moving_towards or bool(exact_hits) or bool(envelope_hits)
        )
        closest_pass_distance = (
            closest_centroid["centroid_distance_km"]
            if relevant_projection else None
        )
        closest_pass_minutes = (
            closest_centroid["minutes"] if relevant_projection else None
        )
        if (
            relevant_projection
            and not self.radar_cells
            and self.motion_model != "constant_acceleration"
        ):
            # Behoud voor puntvormige, lineaire systemen de nauwkeurige
            # grootcirkel-cross-trackberekening van het bestaande contract.
            angle = abs(
                (self.heading_deg - bearing + 180.0) % 360.0 - 180.0
            )
            centroid_distance = _haversine_km(
                self.centroid_lat,
                self.centroid_lon,
                target_lat,
                target_lon,
            )
            closest_pass_distance = centroid_distance * abs(
                math.sin(math.radians(angle))
            )
            along_track = centroid_distance * math.cos(math.radians(angle))
            closest_pass_minutes = along_track / self.speed_kmh * 60.0
        footprint_pass_distance = (
            closest_footprint["edge_distance_km"]
            if relevant_projection else None
        )
        footprint_pass_minutes = (
            closest_footprint["minutes"] if relevant_projection else None
        )
        passage_uncertainty = (
            closest_footprint["uncertainty_km"]
            if relevant_projection else None
        )
        if footprint_pass_distance is None:
            passage_classification = None
        elif footprint_pass_distance <= 0.1:
            passage_classification = "raak"
        elif footprint_pass_distance <= passage_uncertainty:
            passage_classification = "rand"
        else:
            passage_classification = "mist"
        return {
            "bearing_to_target_deg": round(bearing, 1),
            "approach_speed_kmh": round(approach_speed, 1),
            "moving_towards": moving_towards,
            "eta_minutes": round(eta_minutes, 0) if eta_minutes is not None else None,
            "eta_basis": eta_basis,
            "closest_pass_distance_km": (
                round(closest_pass_distance, 1)
                if closest_pass_distance is not None else None
            ),
            "closest_pass_minutes": (
                round(closest_pass_minutes, 0)
                if closest_pass_minutes is not None else None
            ),
            "footprint_pass_distance_km": (
                round(footprint_pass_distance, 1)
                if footprint_pass_distance is not None else None
            ),
            "footprint_pass_minutes": (
                round(footprint_pass_minutes, 0)
                if footprint_pass_minutes is not None else None
            ),
            "passage_classification": passage_classification,
            "passage_uncertainty_km": (
                round(passage_uncertainty, 1)
                if passage_uncertainty is not None else None
            ),
        }

    def _trajectory_passage_samples(
        self,
        target_lat: float,
        target_lon: float,
        *,
        current_edge_distance_km: float,
        horizon_minutes: int = 90,
    ) -> list[dict]:
        """Bemonster het gekozen traject en zijn groeiende onzekerheid."""
        if self.heading_deg is None or self.speed_kmh is None:
            return []
        result = []
        linear_east = self.speed_kmh * math.sin(math.radians(self.heading_deg))
        linear_north = self.speed_kmh * math.cos(math.radians(self.heading_deg))
        velocity_east = (
            self.velocity_east_kmh
            if self.velocity_east_kmh is not None else linear_east
        )
        velocity_north = (
            self.velocity_north_kmh
            if self.velocity_north_kmh is not None else linear_north
        )
        for minutes in range(0, horizon_minutes + 1):
            hours = minutes / 60.0
            east_km = (
                velocity_east * hours
                + 0.5 * self.acceleration_east_kmh2 * hours * hours
            )
            north_km = (
                velocity_north * hours
                + 0.5 * self.acceleration_north_kmh2 * hours * hours
            )
            edge_distance = self._projected_footprint_distance(
                target_lat,
                target_lon,
                east_km=east_km,
                north_km=north_km,
            )
            if edge_distance is None:
                predicted_lat = self.centroid_lat + north_km / 110.574
                predicted_lon = self.centroid_lon + east_km / (
                    111.32 * max(
                        0.05,
                        math.cos(math.radians(self.centroid_lat)),
                    )
                )
                edge_distance = _haversine_km(
                    predicted_lat,
                    predicted_lon,
                    target_lat,
                    target_lon,
                )
            if minutes == 0:
                edge_distance = min(edge_distance, current_edge_distance_km)

            predicted_lat = self.centroid_lat + north_km / 110.574
            predicted_lon = self.centroid_lon + east_km / (
                111.32 * max(0.05, math.cos(math.radians(self.centroid_lat)))
            )
            centroid_distance = _haversine_km(
                predicted_lat,
                predicted_lon,
                target_lat,
                target_lon,
            )
            if not self.radar_cells:
                # Zonder expliciete polygonen kan distance_km nog steeds de
                # bekende afstand tot de systeemrand zijn. Behoud die actuele
                # rand-offset bij het verschuiven van het centroid.
                current_centroid_distance = _haversine_km(
                    self.centroid_lat,
                    self.centroid_lon,
                    target_lat,
                    target_lon,
                )
                footprint_radius_towards_target = max(
                    0.0,
                    current_centroid_distance - current_edge_distance_km,
                )
                edge_distance = max(
                    0.0,
                    centroid_distance - footprint_radius_towards_target,
                )
            prediction_error = max(
                0.5,
                float(self.motion_prediction_error_km or 0.0),
            )
            base_uncertainty = 4.0 if self.confidence == "Hoog" else 8.0
            if self.confidence not in {"Matig", "Hoog"}:
                base_uncertainty = 15.0
            growth = prediction_error * math.sqrt(max(minutes, 1.0) / 5.0)
            acceleration_growth = (
                math.hypot(
                    self.acceleration_east_kmh2,
                    self.acceleration_north_kmh2,
                )
                * hours
                * hours
                * 0.15
            )
            uncertainty = min(
                60.0,
                base_uncertainty + growth + acceleration_growth,
            )
            result.append({
                "minutes": float(minutes),
                "edge_distance_km": edge_distance,
                "centroid_distance_km": centroid_distance,
                "uncertainty_km": uncertainty,
            })
        return result

    def _projected_footprint_distance(
        self,
        target_lat: float,
        target_lon: float,
        *,
        east_km: float,
        north_km: float,
    ) -> Optional[float]:
        """Afstand van target tot de 2D verschoven actuele radarcontour."""
        if not self.radar_cells or self.heading_deg is None:
            return None
        latest = max(cell.timestamp for cell in self.radar_cells.values())
        cells = [
            cell for cell in self.radar_cells.values()
            if latest - cell.timestamp <= 60.0
        ]
        best: Optional[float] = None
        for cell in cells:
            points = cell.footprint_points or ((cell.lat, cell.lon),)
            polygon = [
                _latlon_to_local_km(lat, lon, target_lat, target_lon)
                for lat, lon in points
            ]
            shifted = _convex_hull([
                (x + east_km, y + north_km) for x, y in polygon
            ])
            distance = _distance_to_polygon_origin(shifted)
            best = distance if best is None else min(best, distance)
        return best


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


def _latlon_to_local_km(
    lat: float, lon: float, origin_lat: float, origin_lon: float
) -> tuple[float, float]:
    """Kleine-afstandprojectie naar oost/noord ten opzichte van target."""
    x = (lon - origin_lon) * 111.32 * math.cos(math.radians(origin_lat))
    y = (lat - origin_lat) * 110.574
    return x, y


def _distance_to_polygon_origin(points: list[tuple[float, float]]) -> float:
    """Minimale afstand van (0,0) tot punt, lijn of gesloten polygoon."""
    if not points:
        return math.inf
    if len(points) == 1:
        return math.hypot(*points[0])
    inside = False
    minimum = math.inf
    for index, (x1, y1) in enumerate(points):
        x2, y2 = points[(index + 1) % len(points)]
        if (y1 > 0) != (y2 > 0):
            crossing_x = x1 + (x2 - x1) * (-y1) / (y2 - y1)
            if crossing_x > 0:
                inside = not inside
        dx, dy = x2 - x1, y2 - y1
        length_sq = dx * dx + dy * dy
        if length_sq == 0:
            distance = math.hypot(x1, y1)
        else:
            fraction = max(0.0, min(1.0, -(x1 * dx + y1 * dy) / length_sq))
            distance = math.hypot(x1 + fraction * dx, y1 + fraction * dy)
        minimum = min(minimum, distance)
    return 0.0 if inside else minimum


def _convex_hull(points: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """Orden ongeordende radarpunten als conservatieve convexe buitenrand."""
    unique = sorted(set(points))
    if len(unique) <= 2:
        return unique

    def cross(origin, first, second):
        return (
            (first[0] - origin[0]) * (second[1] - origin[1])
            - (first[1] - origin[1]) * (second[0] - origin[0])
        )

    lower = []
    for point in unique:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], point) <= 0:
            lower.pop()
        lower.append(point)
    upper = []
    for point in reversed(unique):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], point) <= 0:
            upper.pop()
        upper.append(point)
    return lower[:-1] + upper[:-1]


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initiële koers van punt 1 naar punt 2, in graden vanaf noord."""
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    y = math.sin(dlon) * math.cos(lat2_rad)
    x = (
        math.cos(lat1_rad) * math.sin(lat2_rad)
        - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon)
    )
    return math.degrees(math.atan2(y, x)) % 360.0


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
