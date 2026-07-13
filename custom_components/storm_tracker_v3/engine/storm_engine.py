"""Storm Tracker V3 — engine/storm_engine.py v0.1.0

Module 3: Storm Engine
Eén instantie per regio. Gedeeld door alle trackers in die regio.

Verantwoordelijkheden:
  - clustering:    nieuwe strike → bestaande storm of nieuwe storm
  - merge:         twee storms te dicht bij elkaar → samenvoegen (gethrotteld)
  - lifecycle:     storm zonder strikes → sluimerend → verwijderd
  - regressie:     richting + snelheid berekenen uit centroid history
  - geocoding:     plaatsnaam opzoeken (gecachet, lazy)

Ontwerp:
  - Pure Python, geen externe dependencies
  - Brute-force haversine nearest-storm toewijzing (snel genoeg voor N≤20)
  - Alle zware berekeningen gecachet via _dirty flag
  - Merge gethrotteld: max 1x per 15s (voorkomt O(n²) freeze bij intensief onweer)
  - Max 1 HA state-update per seconde via throttle in coordinator
"""
from __future__ import annotations

import logging
import math
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional

from .storm import RadarCellSnapshot, Storm
from ..geometry.bounding_box import compute_bounding_box, bounding_box_changed
from ..geometry.hull import convex_hull, hull_radius_km
from ..geometry.geocode import nearest_place, PlaceEntry

_LOGGER = logging.getLogger(__name__)

# Configuratie
CLUSTER_RADIUS_KM      = 30.0   # max afstand strike→storm voor toewijzing
MERGE_RADIUS_KM        = 60.0   # max afstand storm↔storm voor merge
MERGE_THROTTLE_S       = 15.0   # merge max 1x per N seconden
EXPIRE_MINUTES         = 5.0    # storm zonder strikes → sluimerend
REMOVE_MINUTES         = 15.0   # sluimerend storm → verwijderd
MAX_STORMS             = 15     # max actieve storms (bescherming tegen ruis)
MIN_HISTORY_POINTS     = 4      # min centroid-history voor regressie
MAX_HISTORY_POINTS     = 20     # ringbuffer grootte
MAX_HISTORY_AGE_MIN    = 60     # history ouder dan 60min weggooien
HULL_WINDOW_MINUTES    = 15     # strikes binnen dit venster vormen de hull
GEOCODE_MOVE_THRESHOLD_KM = 5.0  # alleen herzoeken als centroid >5km verschoof


@dataclass
class CentroidPoint:
    """Één historisch centroid-punt voor regressie."""
    lat: float
    lon: float
    ts:  float   # Unix timestamp


class StormEngine:
    """
    Beheert alle actieve storms in één regio.

    Thread-safety: niet thread-safe, maar HA draait op de event loop.
    Alle calls komen via async, dus geen locks nodig.
    """

    def __init__(
        self,
        cluster_radius_km: float = CLUSTER_RADIUS_KM,
        expire_minutes:    float = EXPIRE_MINUTES,
        remove_minutes:    float = REMOVE_MINUTES,
        max_storms:        int   = MAX_STORMS,
        places:            Optional[list[PlaceEntry]] = None,
        on_storms_updated: Optional[Callable] = None,
    ) -> None:
        self._cluster_radius   = cluster_radius_km
        self._expire_minutes   = expire_minutes
        self._remove_minutes   = remove_minutes
        self._max_storms       = max_storms
        # Plaatsendatabase voor geocoding. Leeg (default) = geocoding wordt
        # overgeslagen — Storm.location_name blijft "". Het laden van een
        # echte database is een verantwoordelijkheid van de Coordinator-laag.
        self._places           = places or []
        self._on_updated       = on_storms_updated  # callback naar coordinator

        self._storms:          dict[str, Storm] = {}        # storm_id → Storm
        self._history:         dict[str, list[CentroidPoint]] = {}  # storm_id → history
        self._last_merge_check: float = 0.0
        # Laatst gegeocodeerde centroid-positie per storm, voor de lazy
        # herzoek-drempel (alleen opnieuw zoeken na voldoende verplaatsing)
        self._last_geocode_pos: dict[str, tuple[float, float]] = {}

    # ── Publieke interface ────────────────────────────────────────────────

    async def process_batch(self, observations: list) -> None:
        """
        Verwerk een batch observaties (komt van de ObservationFusionEngine).
        Eén batch = één update cyclus.

        Per observatie-type:
          LIGHTNING  → clustering in bestaande of nieuwe storm
          RADAR      → hull/intensiteit uitbreiden van dichtstbijzijnde storm,
                       of nieuwe storm aanmaken als geen match
          RAIN       → confidence aanpassen van bestaande storm,
                       NOOIT een nieuwe storm aanmaken
        """
        from .observation import ObservationType

        if not observations:
            return

        now = time.time()

        # Splits op type — volgorde is bewust: LIGHTNING en RADAR bouwen storms,
        # RAIN verifieert daarna pas (zodat ze bestaande storms kan vinden)
        lightning = [o for o in observations if o.obs_type == ObservationType.LIGHTNING]
        radar     = [o for o in observations if o.obs_type == ObservationType.RADAR]
        rain      = [o for o in observations if o.obs_type == ObservationType.RAIN]

        # 1. LIGHTNING: clustering + nieuwe storms aanmaken
        for obs in lightning:
            self._assign_observation(obs, may_create=True)

        # 2. RADAR: uitbreiden van bestaande storm of nieuwe aanmaken
        for obs in radar:
            self._assign_observation(obs, may_create=True)

        # 3. RAIN: verifieer bestaande storms, maak NOOIT zelf een storm
        for obs in rain:
            self._apply_rain_verification(obs)

        # 4. Alle actieve storms bijwerken
        for storm in self._storms.values():
            storm.prune_radar_cells()
            storm.update_counts()
            self._update_centroid(storm)
            self._update_movement(storm)
            self._update_geometry(storm)

        # 5. Merge (gethrotteld)
        if now - self._last_merge_check >= MERGE_THROTTLE_S:
            self._merge_nearby_storms()
            self._last_merge_check = now

        # 6. Lifecycle: vervallen storms verwijderen
        self._expire_storms()

        # 7. Callback naar coordinator
        if self._on_updated:
            self._on_updated(list(self._storms.values()))

    def get_storms(self) -> list[Storm]:
        """Geef alle actieve storms terug (gesorteerd op grootte)."""
        return sorted(
            self._storms.values(),
            key=lambda s: s.strikes_60min,
            reverse=True
        )

    def get_storm(self, storm_id: str) -> Optional[Storm]:
        return self._storms.get(storm_id)

    def retain_within(
        self, center_lat: float, center_lon: float, radius_km: float
    ) -> int:
        """Verwijder WeatherSystems buiten het actuele monitoringsgebied."""
        remove_ids = [
            storm_id
            for storm_id, storm in self._storms.items()
            if _haversine(
                center_lat,
                center_lon,
                storm.centroid_lat,
                storm.centroid_lon,
            ) > radius_km
        ]

        for storm_id in remove_ids:
            self._storms.pop(storm_id, None)
            self._history.pop(storm_id, None)
            self._last_geocode_pos.pop(storm_id, None)

        if remove_ids and self._on_updated:
            self._on_updated(list(self._storms.values()))

        return len(remove_ids)

    # ── Observatie-verwerking per type ────────────────────────────────────

    def _assign_observation(self, obs, may_create: bool = True) -> None:
        """
        Wijs een observatie toe aan de dichtstbijzijnde storm.
        Als geen storm gevonden en may_create=True: maak nieuwe storm aan.
        """
        from .observation import ObservationType

        best_storm: Optional[Storm] = None
        best_dist:  float           = self._cluster_radius

        # Alle lokale kernen uit dezelfde oorspronkelijke OPERA-component
        # horen binnen deze batch onvoorwaardelijk bij hetzelfde WeatherSystem.
        parent_system_id = getattr(obs, "parent_system_id", None)
        if parent_system_id:
            best_storm = next(
                (
                    storm for storm in self._storms.values()
                    if not storm.is_dormant
                    and parent_system_id in storm.source_system_ids
                ),
                None,
            )

        for storm in self._storms.values() if best_storm is None else ():
            if storm.is_dormant:
                continue
            dist = self._distance_to_storm(obs, storm)
            if dist < best_dist:
                best_dist  = dist
                best_storm = storm

        if best_storm is not None:
            self._apply_observation_to_storm(best_storm, obs)
            _LOGGER.debug(
                "Observatie %s (%.3f,%.3f) → storm %s (%.1fkm)",
                obs.obs_type.value, obs.lat, obs.lon,
                best_storm.storm_id, best_dist
            )
        elif may_create:
            active_count = sum(1 for s in self._storms.values() if not s.is_dormant)
            if active_count < self._max_storms:
                self._new_storm(obs)
            else:
                _LOGGER.debug("Max storms bereikt, observatie genegeerd")

    @staticmethod
    def _distance_to_storm(obs, storm: Storm) -> float:
        """Match tegen lokale radarcellen, niet enkel de systeemcentroid."""
        distances = [
            _haversine(
                obs.lat,
                obs.lon,
                storm.centroid_lat,
                storm.centroid_lon,
            )
        ]
        distances.extend(
            _haversine(obs.lat, obs.lon, cell.lat, cell.lon)
            for cell in storm.radar_cells.values()
        )
        return min(distances)

    def _apply_observation_to_storm(self, storm: Storm, obs) -> None:
        """Pas de effecten van een observatie toe op een storm-object."""
        from .observation import ObservationType

        if obs.obs_type == ObservationType.LIGHTNING:
            # Lightning: toevoegen aan strike-history (basis van regressie)
            storm._strike_history.append((obs.timestamp, obs.lat, obs.lon))
            storm.last_update   = time.time()
            storm.strike_count += 1
            storm.is_dormant    = False
            storm._dirty        = True
            storm._cached_projections.clear()

        elif obs.obs_type == ObservationType.RADAR:
            # Radar: intensiteit bijhouden, hull wordt later herberekend
            # via _update_geometry op basis van de strike-history + radar-history
            storm._radar_observations.append((obs.timestamp, obs.lat, obs.lon,
                                              obs.intensity or 0))
            storm.last_update = time.time()
            storm.is_dormant  = False
            storm._dirty      = True
            storm._cached_projections.clear()
            # Max intensiteit bijhouden voor clutter-filtering
            if (obs.intensity or 0) > storm.max_radar_intensity:
                storm.max_radar_intensity = obs.intensity or 0
            cell_id = getattr(obs, "radar_cell_id", None) or (
                f"{obs.source}:{obs.timestamp:.0f}:{obs.lat:.4f}:{obs.lon:.4f}"
            )
            parent_system_id = getattr(obs, "parent_system_id", None)
            storm.radar_cells[cell_id] = RadarCellSnapshot(
                cell_id=cell_id,
                timestamp=obs.timestamp,
                lat=obs.lat,
                lon=obs.lon,
                intensity=obs.intensity or 0,
                area_km2=obs.area_km2 or 0.0,
                footprint_points=tuple(obs.footprint_points or ()),
                parent_system_id=parent_system_id,
            )
            if parent_system_id:
                storm.source_system_ids.add(parent_system_id)
                storm._source_system_last_seen[parent_system_id] = obs.timestamp
                if obs.parent_area_km2 is not None:
                    storm.parent_system_areas[parent_system_id] = obs.parent_area_km2
                if obs.parent_footprint_points:
                    storm.parent_system_footprints[parent_system_id] = tuple(
                        obs.parent_footprint_points
                    )

    def _apply_rain_verification(self, obs) -> None:
        """
        Pas Netatmo-regenverificatie toe op bestaande storms.
        Creëert NOOIT een nieuwe storm.

        Een station dat regen meet binnen de hull van een storm verhoogt
        de confidence. Een station zonder regen in het storm-pad verlaagt
        de confidence (mogelijke clutter).
        """
        from .observation import ObservationType

        search_radius = self._cluster_radius * 2   # bredere zoekradius voor verificatie

        for storm in self._storms.values():
            if storm.is_dormant:
                continue
            dist = _haversine(obs.lat, obs.lon,
                              storm.centroid_lat, storm.centroid_lon)
            if dist > search_radius:
                continue

            rain_mm = obs.rain_mm or 0.0
            if rain_mm >= 0.1:   # station meet regen: vertrouwen verhogen
                storm.netatmo_confirmations += 1
                _LOGGER.debug(
                    "Netatmo bevestigt storm %s: %.2fmm bij station %s",
                    storm.storm_id, rain_mm, obs.station_id
                )
            else:   # station meet GEEN regen: voorzichtig vertrouwen verlagen
                storm.netatmo_no_rain_count += 1

    def _new_storm(self, obs) -> Storm:
        """Maak een nieuwe storm aan op basis van de eerste observatie."""
        storm = Storm(
            centroid_lat=obs.lat,
            centroid_lon=obs.lon,
        )
        self._apply_observation_to_storm(storm, obs)
        self._storms[storm.storm_id] = storm
        self._history[storm.storm_id] = []
        _LOGGER.info("Nieuwe storm %s op (%.3f,%.3f) via %s",
                     storm.storm_id, obs.lat, obs.lon,
                     obs.obs_type.value)
        return storm

    # ── Centroid update ───────────────────────────────────────────────────

    def _update_centroid(self, storm: Storm) -> None:
        """
        Herbereken centroid op basis van recente LIGHTNING en RADAR
        observaties (laatste 5 min), gewogen naar recentheid.

        Gebruikt de timestamp van de observaties zelf (niet verwerkingstijd)
        als referentiepunt — maakt de regressie deterministisch en
        replay-veilig (kritieke bug uit V2, hier bewust gefixeerd).
        """
        # Lightning-posities (meest nauwkeurig, punt-precies)
        lightning = storm.strikes_in_window(minutes=5)  # (ts, lat, lon)
        # Radar-posities (minder nauwkeurig, grotere pixels)
        radar_cutoff = time.time() - 5 * 60
        radar = [(ts, lat, lon) for ts, lat, lon, intens
                 in storm._radar_observations if ts >= radar_cutoff]

        all_points = [(ts, lat, lon) for ts, lat, lon in lightning] + \
                     [(ts, lat, lon) for ts, lat, lon in radar]
        if not all_points:
            return

        reference_ts = max(ts for ts, _, _ in all_points)
        total_weight = 0.0
        lat_sum = 0.0
        lon_sum = 0.0

        for ts, lat, lon in all_points:
            age    = reference_ts - ts
            weight = max(0.1, 1.0 - age / 300)
            lat_sum      += lat * weight
            lon_sum      += lon * weight
            total_weight += weight

        if total_weight > 0:
            new_lat = lat_sum / total_weight
            new_lon = lon_sum / total_weight

            hist = self._history.setdefault(storm.storm_id, [])
            hist.append(CentroidPoint(lat=new_lat, lon=new_lon, ts=reference_ts))
            self._prune_history(storm.storm_id)

            storm.centroid_lat = new_lat
            storm.centroid_lon = new_lon

    def _prune_history(self, storm_id: str) -> None:
        """Verwijder oude history punten."""
        hist = self._history.get(storm_id, [])
        cutoff = time.time() - MAX_HISTORY_AGE_MIN * 60
        hist = [p for p in hist if p.ts >= cutoff]
        if len(hist) > MAX_HISTORY_POINTS:
            hist = hist[-MAX_HISTORY_POINTS:]
        self._history[storm_id] = hist

    # ── Beweging: richting + snelheid ─────────────────────────────────────

    def _update_movement(self, storm: Storm) -> None:
        """
        Bereken richting en snelheid via lineaire regressie op centroid history.
        Alleen als er genoeg punten zijn en de cache vervallen is.
        """
        if not storm._dirty:
            return

        hist = self._history.get(storm.storm_id, [])
        if len(hist) < MIN_HISTORY_POINTS:
            storm._dirty = False
            return

        heading, speed = self._linear_regression(hist)
        storm.heading_deg = heading
        storm.speed_kmh   = speed
        storm.confidence  = self._calc_confidence(hist, heading, speed)
        storm._dirty      = False

    def _linear_regression(
        self, hist: list[CentroidPoint]
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Lineaire regressie op lat(t) en lon(t).
        Geeft (heading_deg, speed_kmh) of (None, None) bij onvoldoende data.
        """
        n = len(hist)
        if n < 2:
            return None, None

        ts_arr  = [p.ts  for p in hist]
        lat_arr = [p.lat for p in hist]
        lon_arr = [p.lon for p in hist]

        # Normaliseer timestamps (voorkomt floating point problemen)
        t0 = ts_arr[0]
        ts_arr = [t - t0 for t in ts_arr]

        # Regressie lat ~ t
        dlat_dt = _slope(ts_arr, lat_arr)
        dlon_dt = _slope(ts_arr, lon_arr)

        if dlat_dt is None or dlon_dt is None:
            return None, None

        # Omzetten naar km/h
        avg_lat   = sum(lat_arr) / n
        cos_lat   = math.cos(math.radians(avg_lat))
        dlat_kmh  = dlat_dt * 3600 * 111.32
        dlon_kmh  = dlon_dt * 3600 * 111.32 * cos_lat

        speed_kmh = math.sqrt(dlat_kmh ** 2 + dlon_kmh ** 2)
        if speed_kmh < 0.5:
            return None, 0.0   # stilstaand

        # Sanity check: stormen bewegen niet sneller dan 150 km/h
        if speed_kmh > 150:
            _LOGGER.debug("Regressie snelheid %.1f km/h afgewezen (te hoog)", speed_kmh)
            return None, None

        heading = math.degrees(math.atan2(dlon_kmh, dlat_kmh)) % 360
        return round(heading, 1), round(speed_kmh, 1)

    def _calc_confidence(
        self,
        hist: list[CentroidPoint],
        heading: Optional[float],
        speed: Optional[float],
    ) -> str:
        """Bereken confidence op basis van history lengte en consistentie."""
        n = len(hist)
        if heading is None or n < MIN_HISTORY_POINTS:
            return "Onvoldoende data"
        if n >= 10 and speed is not None and speed > 1:
            return "Hoog"
        if n >= 6:
            return "Matig"
        return "Laag"

    # ── Geometrie: bounding box, polygon (hull), geocoding ──────────────────

    def _update_geometry(self, storm: Storm) -> None:
        """
        Bounding box → polygon (hull) per het voorstel.

        Beide lightning-posities EN radar-posities voeden de hull:
        - Lightning geeft precieze puntposities
        - Radar geeft het neerslags-gebied, ook als er (nog) geen bliksem is
        """
        cutoff = time.time() - HULL_WINDOW_MINUTES * 60
        points = [
            (lat, lon)
            for ts, lat, lon in storm.strikes_in_window(HULL_WINDOW_MINUTES)
        ] + [
            (lat, lon)
            for ts, lat, lon, intens in storm._radar_observations
            if ts >= cutoff
        ]

        new_box = compute_bounding_box(points)

        if not bounding_box_changed(storm.bounding_box, new_box):
            return

        storm.bounding_box = new_box

        if len(points) >= 3:
            storm.hull      = convex_hull(points)
            storm.radius_km = hull_radius_km(
                storm.hull, storm.centroid_lat, storm.centroid_lon
            )
        else:
            storm.hull      = points
            storm.radius_km = 0.0

        self._update_geocode(storm)

    def _update_geocode(self, storm: Storm) -> None:
        """
        Zoek de dichtstbijzijnde plaatsnaam, maar alleen als de centroid
        voldoende verschoof sinds de laatste lookup (lazy, zie voorstel
        pagina 9: "Alleen berekenen wanneer nodig").
        """
        if not self._places:
            return   # geen database geladen — geocoding overgeslagen

        last_pos = self._last_geocode_pos.get(storm.storm_id)
        if last_pos is not None:
            moved_km = _haversine(
                last_pos[0], last_pos[1], storm.centroid_lat, storm.centroid_lon
            )
            if moved_km < GEOCODE_MOVE_THRESHOLD_KM:
                return

        storm.location_name = nearest_place(
            storm.centroid_lat, storm.centroid_lon, self._places
        )
        self._last_geocode_pos[storm.storm_id] = (
            storm.centroid_lat, storm.centroid_lon
        )

    # ── Merge ─────────────────────────────────────────────────────────────

    def _merge_nearby_storms(self) -> None:
        """
        Voeg storms samen die te dicht bij elkaar komen.
        Gethrotteld: max 1x per MERGE_THROTTLE_S seconden.
        O(n²) maar n ≤ MAX_STORMS = 15 → max 105 paren → verwaarloosbaar.
        """
        active = [s for s in self._storms.values() if not s.is_dormant]
        merged = set()

        for i, s1 in enumerate(active):
            if s1.storm_id in merged:
                continue
            for s2 in active[i + 1:]:
                if s2.storm_id in merged:
                    continue
                dist = _haversine(
                    s1.centroid_lat, s1.centroid_lon,
                    s2.centroid_lat, s2.centroid_lon
                )
                if dist <= MERGE_RADIUS_KM:
                    self._merge(s1, s2)
                    merged.add(s2.storm_id)
                    _LOGGER.info("Storms %s + %s samengevoegd (%.1fkm)",
                                 s1.storm_id, s2.storm_id, dist)

        for storm_id in merged:
            self._storms.pop(storm_id, None)
            self._history.pop(storm_id, None)
            self._last_geocode_pos.pop(storm_id, None)

    def _merge(self, keeper: Storm, other: Storm) -> None:
        """
        Voeg 'other' samen in 'keeper'.
        Keeper krijgt gecombineerde strike history en een nieuw centroid.
        """
        keeper._strike_history.extend(other._strike_history)
        keeper._strike_history.sort(key=lambda x: x[0])
        keeper.strike_count += other.strike_count
        keeper.update_counts()
        keeper._dirty = True
        keeper.radar_cells.update(other.radar_cells)
        keeper.source_system_ids.update(other.source_system_ids)
        keeper.parent_system_areas.update(other.parent_system_areas)
        keeper.parent_system_footprints.update(other.parent_system_footprints)
        keeper._source_system_last_seen.update(other._source_system_last_seen)

        # Gecombineerde centroid (gewogen naar strike_count)
        total = keeper.strikes_60min + other.strikes_60min
        if total > 0:
            keeper.centroid_lat = (
                keeper.centroid_lat * keeper.strikes_60min
                + other.centroid_lat * other.strikes_60min
            ) / total
            keeper.centroid_lon = (
                keeper.centroid_lon * keeper.strikes_60min
                + other.centroid_lon * other.strikes_60min
            ) / total

        # Combineer history
        combined_hist = (
            self._history.get(keeper.storm_id, [])
            + self._history.get(other.storm_id, [])
        )
        combined_hist.sort(key=lambda p: p.ts)
        self._history[keeper.storm_id] = combined_hist[-MAX_HISTORY_POINTS:]

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def _expire_storms(self) -> None:
        """
        Storms zonder recente strikes → sluimerend.
        Sluimerende storms zonder strikes → verwijderd.
        """
        to_remove = []
        for storm in self._storms.values():
            age = (time.time() - storm.last_update) / 60
            if age > self._remove_minutes:
                to_remove.append(storm.storm_id)
                _LOGGER.info("Storm %s verwijderd (%.0f min inactief)",
                             storm.storm_id, age)
            elif age > self._expire_minutes:
                storm.is_dormant = True

        for storm_id in to_remove:
            self._storms.pop(storm_id, None)
            self._history.pop(storm_id, None)
            self._last_geocode_pos.pop(storm_id, None)


# ── Hulpfuncties ──────────────────────────────────────────────────────────

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Afstand in km (Haversine)."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _slope(x: list[float], y: list[float]) -> Optional[float]:
    """Lineaire regressie helling dy/dx via least squares."""
    n = len(x)
    if n < 2:
        return None
    sx  = sum(x)
    sy  = sum(y)
    sxx = sum(xi ** 2 for xi in x)
    sxy = sum(xi * yi for xi, yi in zip(x, y))
    denom = n * sxx - sx ** 2
    if abs(denom) < 1e-10:
        return None
    return (n * sxy - sx * sy) / denom
