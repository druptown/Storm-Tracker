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

import hashlib
import json
import logging
import math
import statistics
import time
import uuid
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Callable, Optional

from .storm import Storm
from .trajectory import fit_adaptive_trajectory
from ..geometry.bounding_box import compute_bounding_box, bounding_box_changed
from ..geometry.hull import convex_hull, hull_radius_km
from ..geometry.geocode import nearest_place, PlaceEntry
from ..geometry.polygon import point_to_polygon_distance_km

_LOGGER = logging.getLogger(__name__)

# Configuratie
CLUSTER_RADIUS_KM      = 30.0   # max afstand strike→storm voor toewijzing
MERGE_RADIUS_KM        = 60.0   # max afstand storm↔storm voor merge
MERGE_THROTTLE_S       = 15.0   # merge max 1x per N seconden
PARENT_SPLIT_DISTANCE_KM = 35.0
MERGE_HEADING_TOLERANCE_DEG = 55.0
EXPIRE_MINUTES         = 5.0    # storm zonder strikes → sluimerend
REMOVE_MINUTES         = 15.0   # sluimerend storm → verwijderd
RADAR_EXPIRE_MINUTES   = 15.0   # overbrug 10-minutenradarframes zonder ID-reset
RADAR_REMOVE_MINUTES   = 30.0   # behoud radarhistorie bij één gemist frame
RADAR_SAME_FRAME_MERGE_GAP_KM = 3.0
GROUND_FOOTPRINT_TIME_TOLERANCE_S = 15 * 60
SAME_RADAR_FRAME_TOLERANCE_S = 1.0
MAX_STORMS             = 15     # legacy capaciteitsrichtwaarde; nooit een datafilter
MIN_HISTORY_POINTS     = 2      # vanaf twee posities een voorlopige schatting
MAX_HISTORY_POINTS     = 20     # ringbuffer grootte
MAX_HISTORY_AGE_MIN    = 60     # history ouder dan 60min weggooien
HULL_WINDOW_MINUTES    = 15     # strikes binnen dit venster vormen de hull
GEOCODE_MOVE_THRESHOLD_KM = 5.0  # alleen herzoeken als centroid >5km verschoof
APPLIED_BATCH_RECEIPTS_MAX = 2048
COMPUTE_PRIORITY_POLICY_VERSION = "target_distance_non_evicting_v1"
COMPUTE_PRIORITY_TIERS = ("relevant", "background", "low_confidence")
LOW_CONFIDENCE_LABELS = frozenset({
    "onvoldoende data",
    "laag",
    "insufficient",
    "low",
})


def _footprint_vertices(value) -> tuple[tuple[float, float], ...]:
    polygons = tuple(
        getattr(value, "footprint_polygons", ()) or ()
    )
    if polygons:
        return tuple(
            point
            for polygon in polygons
            for ring in polygon
            for point in ring
        )
    return tuple(getattr(value, "footprint_points", ()) or ())


def _footprint_geometry(value):
    polygons = tuple(
        getattr(value, "footprint_polygons", ()) or ()
    )
    if polygons:
        return polygons
    points = tuple(getattr(value, "footprint_points", ()) or ())
    if points:
        return points
    latitude = getattr(value, "lat", None)
    longitude = getattr(value, "lon", None)
    return (
        ((float(latitude), float(longitude)),)
        if latitude is not None and longitude is not None
        else ()
    )


class TrackingRevisionConflict(RuntimeError):
    """Een resultaat werd berekend vanaf een verouderde trackingstate."""


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
        self._lineage_events: list[dict] = []
        # Laatst gegeocodeerde centroid-positie per storm, voor de lazy
        # herzoek-drempel (alleen opnieuw zoeken na voldoende verplaatsing)
        self._last_geocode_pos: dict[str, tuple[float, float]] = {}
        self._revision = 0
        self._applied_batch_ids: dict[str, int] = {}

    # ── Publieke interface ────────────────────────────────────────────────

    @property
    def revision(self) -> int:
        """Monotone versie van iedere gecommitteerde runtimewijziging."""
        return self._revision

    def has_applied_batch(self, batch_id: str | None) -> bool:
        """True wanneer deze idempotency-sleutel al gecommitteerd is."""
        return bool(batch_id and batch_id in self._applied_batch_ids)

    def _assert_commit_allowed(
        self,
        expected_revision: int | None,
        batch_id: str | None,
    ) -> bool:
        if batch_id is not None and (
            not isinstance(batch_id, str) or not batch_id.strip()
        ):
            raise ValueError("batch_id moet een niet-lege string zijn")
        if self.has_applied_batch(batch_id):
            return False
        if (
            expected_revision is not None
            and expected_revision != self._revision
        ):
            raise TrackingRevisionConflict(
                "trackingrevision gewijzigd: "
                f"verwacht {expected_revision}, actueel {self._revision}"
            )
        return True

    def _register_commit(self, batch_id: str | None = None) -> None:
        self._revision += 1
        if batch_id is None:
            return
        self._applied_batch_ids[batch_id] = self._revision
        while len(self._applied_batch_ids) > APPLIED_BATCH_RECEIPTS_MAX:
            oldest = next(iter(self._applied_batch_ids))
            self._applied_batch_ids.pop(oldest, None)

    async def process_batch(
        self,
        observations: list,
        *,
        expected_revision: int | None = None,
        batch_id: str | None = None,
    ) -> bool:
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

        if not self._assert_commit_allowed(expected_revision, batch_id):
            return False
        if not observations and batch_id is None:
            return False

        live_state = None
        if expected_revision is not None or batch_id is not None:
            live_state = (
                self._storms,
                self._history,
                self._last_merge_check,
                self._lineage_events,
                self._last_geocode_pos,
            )
            (
                self._storms,
                self._history,
                self._last_merge_check,
                self._lineage_events,
                self._last_geocode_pos,
            ) = deepcopy(live_state)

        try:
            now = time.time()

            # Splits op type — volgorde is bewust: LIGHTNING en RADAR bouwen
            # storms, RAIN verifieert daarna pas.
            lightning = [
                o for o in observations
                if o.obs_type == ObservationType.LIGHTNING
            ]
            radar = [
                o for o in observations
                if o.obs_type == ObservationType.RADAR
            ]
            rain = [
                o for o in observations
                if o.obs_type == ObservationType.RAIN
            ]

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
                storm.update_radar_classification()
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
        except Exception:
            if live_state is not None:
                (
                    self._storms,
                    self._history,
                    self._last_merge_check,
                    self._lineage_events,
                    self._last_geocode_pos,
                ) = live_state
            raise

        if live_state is not None:
            # De transactie liep volledig op kopieën. Werk bestaande
            # Storm-objecten pas na succes in-place bij, zodat reeds
            # gepubliceerde handles dezelfde identiteit én gecommitteerde
            # inhoud behouden. Bij een precommitfout bleven die live objecten
            # volledig onaangeroerd.
            live_storms = live_state[0]
            committed_storms = {}
            for storm_id, working_storm in self._storms.items():
                live_storm = live_storms.get(storm_id)
                if live_storm is None:
                    committed_storms[storm_id] = working_storm
                    continue
                live_storm.__dict__.clear()
                live_storm.__dict__.update(working_storm.__dict__)
                committed_storms[storm_id] = live_storm
            self._storms = committed_storms

        # Registreer de commit vóór niet-kritieke callbacks. Een retry na een
        # callbackfout ziet daardoor dezelfde batch als reeds toegepast.
        self._register_commit(batch_id)
        if self._on_updated:
            self._on_updated(self.get_active_storms())
        return True

    async def calculate_batch_candidate(
        self,
        observations: list,
        *,
        expected_revision: int,
        batch_id: str,
    ) -> list[dict]:
        """Bereken een kandidaatstate zonder de live engine te muteren.

        De caller mag het resultaat pas installeren nadat state en receipt
        duurzaam in één transactie zijn gecommitteerd.
        """
        if not self._assert_commit_allowed(expected_revision, batch_id):
            return self.export_tracking_state()
        working = StormEngine(
            cluster_radius_km=self._cluster_radius,
            expire_minutes=self._expire_minutes,
            remove_minutes=self._remove_minutes,
            max_storms=self._max_storms,
            places=self._places,
        )
        working._storms = deepcopy(self._storms)
        working._history = deepcopy(self._history)
        working._last_merge_check = self._last_merge_check
        working._lineage_events = deepcopy(self._lineage_events)
        working._last_geocode_pos = deepcopy(self._last_geocode_pos)
        working._revision = self._revision
        working._applied_batch_ids = dict(self._applied_batch_ids)
        await working.process_batch(
            observations,
            expected_revision=expected_revision,
            batch_id=batch_id,
        )
        return working.export_tracking_state()

    def get_storms(self) -> list[Storm]:
        """Geef alle actieve storms terug (gesorteerd op grootte)."""
        return sorted(
            self._storms.values(),
            key=lambda s: s.strikes_60min,
            reverse=True
        )

    def get_active_storms(self) -> list[Storm]:
        """Geef alleen systemen die operationeel nog actief zijn."""
        return [storm for storm in self.get_storms() if not storm.is_dormant]

    def get_storm(self, storm_id: str) -> Optional[Storm]:
        return self._storms.get(storm_id)

    @staticmethod
    def _normalize_compute_targets(
        target_locations: Mapping[str, tuple[float, float]],
    ) -> tuple[tuple[str, float, float], ...]:
        """Valideer targets en maak hun volgorde reproduceerbaar."""
        if not isinstance(target_locations, Mapping):
            raise TypeError("target_locations moet een mapping zijn")

        normalized = []
        for target_id, coordinates in target_locations.items():
            if not isinstance(target_id, str) or not target_id.strip():
                raise ValueError("compute-target-ID moet een niet-lege string zijn")
            if (
                not isinstance(coordinates, (tuple, list))
                or len(coordinates) != 2
            ):
                raise ValueError(
                    "compute-targetcoördinaten moeten (lat, lon) bevatten"
                )
            raw_lat, raw_lon = coordinates
            if (
                isinstance(raw_lat, bool)
                or isinstance(raw_lon, bool)
                or not isinstance(raw_lat, (int, float))
                or not isinstance(raw_lon, (int, float))
            ):
                raise TypeError("compute-targetcoördinaten moeten numeriek zijn")
            lat = float(raw_lat)
            lon = float(raw_lon)
            if (
                not math.isfinite(lat)
                or not math.isfinite(lon)
                or not -90.0 <= lat <= 90.0
                or not -180.0 <= lon <= 180.0
            ):
                raise ValueError("compute-targetcoördinaten zijn ongeldig")
            normalized.append((
                target_id.strip(),
                0.0 if lat == 0.0 else lat,
                0.0 if lon == 0.0 else lon,
            ))
        ids = [target_id for target_id, _, _ in normalized]
        if len(set(ids)) != len(ids):
            raise ValueError("compute-target-ID's moeten uniek zijn")
        return tuple(sorted(normalized, key=lambda item: item[0]))

    @staticmethod
    def _target_distance_for_compute(
        storm: Storm,
        target_lat: float,
        target_lon: float,
    ) -> tuple[float, str]:
        """Meet actuele footprintafstand, met centroid als expliciete fallback."""
        latest_radar_timestamp = max(
            (
                float(cell.timestamp)
                for cell in storm.radar_cells.values()
                if math.isfinite(float(cell.timestamp))
            ),
            default=None,
        )
        if latest_radar_timestamp is not None:
            closest = storm.closest_radar_point(
                target_lat,
                target_lon,
                min_timestamp=(
                    latest_radar_timestamp - SAME_RADAR_FRAME_TOLERANCE_S
                ),
            )
            if closest is not None and math.isfinite(float(closest[0])):
                return float(closest[0]), "latest_radar_footprint"
        return (
            _haversine(
                target_lat,
                target_lon,
                float(storm.centroid_lat),
                float(storm.centroid_lon),
            ),
            "centroid_fallback",
        )

    def build_compute_schedule(
        self,
        target_locations: Mapping[str, tuple[float, float]],
        *,
        relevance_radius_km: float,
        foreground_slots: int | None = None,
    ) -> dict:
        """Bouw een volledig, niet-evictend compute-prioriteitsplan.

        Dit plan is uitsluitend schedulingadvies voor eventuele dure
        forecast-/dashboardberekeningen. Ingest, tracking, history, export en
        forecastinput blijven voor *ieder* WeatherSystem behouden. De
        caller-supplied radius is een operationele afstandsgrens en geen
        meteorologische afkeur- of waarschijnlijkheidsdrempel.
        """
        if (
            isinstance(relevance_radius_km, bool)
            or not isinstance(relevance_radius_km, (int, float))
            or not math.isfinite(float(relevance_radius_km))
            or float(relevance_radius_km) < 0.0
        ):
            raise ValueError(
                "relevance_radius_km moet eindig en niet-negatief zijn"
            )
        relevance_radius = float(relevance_radius_km)

        slots_source = "caller"
        if foreground_slots is None:
            slots_source = "legacy_max_storms_guideline"
            configured = self._max_storms
            foreground_slots = (
                configured
                if (
                    isinstance(configured, int)
                    and not isinstance(configured, bool)
                )
                else MAX_STORMS
            )
        if (
            isinstance(foreground_slots, bool)
            or not isinstance(foreground_slots, int)
            or foreground_slots < 1
        ):
            raise ValueError("foreground_slots moet een positieve integer zijn")
        slots = int(foreground_slots)
        targets = self._normalize_compute_targets(target_locations)
        tier_order = {
            tier: index for index, tier in enumerate(COMPUTE_PRIORITY_TIERS)
        }

        candidates = []
        for storm in self._storms.values():
            nearest_target_id = None
            nearest_distance = None
            distance_basis = "no_targets"
            if targets:
                matches = []
                for target_id, target_lat, target_lon in targets:
                    distance, basis = self._target_distance_for_compute(
                        storm,
                        target_lat,
                        target_lon,
                    )
                    matches.append((distance, target_id, basis))
                nearest_distance, nearest_target_id, distance_basis = min(
                    matches,
                    key=lambda item: (item[0], item[1]),
                )

            if (
                nearest_distance is not None
                and nearest_distance <= relevance_radius
            ):
                tier = "relevant"
            elif (
                bool(storm.is_dormant)
                or str(storm.confidence).strip().casefold()
                in LOW_CONFIDENCE_LABELS
            ):
                tier = "low_confidence"
            else:
                tier = "background"

            entry = {
                "storm_id": str(storm.storm_id),
                "tier": tier,
                "active": not bool(storm.is_dormant),
                "confidence": str(storm.confidence),
                "nearest_target_id": nearest_target_id,
                "target_distance_km": (
                    round(float(nearest_distance), 3)
                    if nearest_distance is not None
                    else None
                ),
                "distance_basis": distance_basis,
                "tracking_state_retained": True,
                "history_retained": storm.storm_id in self._history,
                "export_retained": True,
                "forecast_input_retained": True,
                "execution_required": True,
            }
            candidates.append((
                (
                    tier_order[tier],
                    (
                        float(nearest_distance)
                        if nearest_distance is not None
                        else math.inf
                    ),
                    str(storm.storm_id),
                ),
                entry,
            ))

        schedule = []
        for index, (_, entry) in enumerate(sorted(
            candidates, key=lambda item: item[0]
        )):
            entry["rank"] = index + 1
            entry["preferred_pass"] = index // slots
            entry["preferred_foreground"] = index < slots
            schedule.append(entry)

        policy_contract = {
            "policy_version": COMPUTE_PRIORITY_POLICY_VERSION,
            "tier_order": list(COMPUTE_PRIORITY_TIERS),
            "distance_order": [
                "latest_radar_footprint",
                "centroid_fallback",
            ],
            "target_relevance_precedes_confidence": True,
            "capacity_role": "scheduling_guideline_only",
            "tier_eviction": "forbidden",
            "track_omission": "forbidden",
        }

        def canonical_bytes(value: object) -> bytes:
            return json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")

        policy_fingerprint = hashlib.sha256(
            canonical_bytes(policy_contract)
        ).hexdigest()
        target_document = [
            {"target_id": target_id, "lat": lat, "lon": lon}
            for target_id, lat, lon in targets
        ]
        serialized_schedule = canonical_bytes(schedule)
        schedule_fingerprint = hashlib.sha256(canonical_bytes({
            "policy_fingerprint": policy_fingerprint,
            "relevance_radius_km": relevance_radius,
            "foreground_slots": slots,
            "targets": target_document,
            "schedule": schedule,
        })).hexdigest()
        tracked_ids = set(self._storms)
        history_ids = set(self._history)
        scheduled_ids = {entry["storm_id"] for entry in schedule}
        exported_ids = {
            str(snapshot.get("storm_id"))
            for snapshot in self.export_tracking_state()
        }
        tier_counts = {
            tier: sum(entry["tier"] == tier for entry in schedule)
            for tier in COMPUTE_PRIORITY_TIERS
        }
        preferred_pass_count = (
            max(entry["preferred_pass"] for entry in schedule) + 1
            if schedule else 0
        )

        return {
            "schema_version": 1,
            "policy": policy_contract,
            "policy_fingerprint": policy_fingerprint,
            "schedule_fingerprint": schedule_fingerprint,
            "targets": target_document,
            "target_count": len(targets),
            "relevance_radius_km": relevance_radius,
            "max_storms_configured": self._max_storms,
            "max_storms_role": "scheduling_guideline_only",
            "foreground_slots": slots,
            "foreground_slots_source": slots_source,
            "tracked_count": len(tracked_ids),
            "history_track_count": len(history_ids),
            "exported_track_count": len(exported_ids),
            "forecast_input_count": len(schedule),
            "scheduled_track_count": len(schedule),
            "preferred_foreground_count": min(slots, len(schedule)),
            "deferred_advisory_count": max(0, len(schedule) - slots),
            "preferred_pass_count": preferred_pass_count,
            "track_target_pairs_evaluated": len(schedule) * len(targets),
            "schedule_serialized_bytes": len(serialized_schedule),
            "tier_counts": tier_counts,
            "schedule": schedule,
            "invariants": {
                "capacity_preserves_all_observations": True,
                "tiering_is_non_evicting": True,
                "all_tracks_scheduled": scheduled_ids == tracked_ids,
                "all_tracks_exported": exported_ids == tracked_ids,
                "all_tracks_retained_for_forecast_input": all(
                    entry["forecast_input_retained"] for entry in schedule
                ),
                "tracking_history_key_parity": history_ids == tracked_ids,
                "schedule_ids_unique": len(scheduled_ids) == len(schedule),
            },
            "limitations": {
                "wall_clock_benchmark": False,
                "process_memory_measurement": False,
                "nuc_thermal_claim": False,
                "meteorological_probability_claim": False,
            },
        }

    def export_mcs_history(self) -> list[dict]:
        """Geef compacte, JSON-veilige MCS-historiek voor persistente opslag."""
        snapshots = []
        for storm in self._storms.values():
            if not storm.radar_system_frames:
                continue
            snapshot = storm.to_mcs_snapshot()
            snapshot["motion_history"] = [
                {"lat": point.lat, "lon": point.lon, "timestamp": point.ts}
                for point in self._history.get(storm.storm_id, [])
            ]
            snapshots.append(snapshot)
        return snapshots

    def export_tracking_state(self) -> list[dict]:
        """Geef de volledige workerbeginsituatie, ook voor bliksem-only tracks."""
        snapshots = []
        for storm in self._storms.values():
            snapshot = storm.to_mcs_snapshot()
            snapshot["motion_history"] = [
                {"lat": point.lat, "lon": point.lon, "timestamp": point.ts}
                for point in self._history.get(storm.storm_id, [])
            ]
            snapshots.append(snapshot)
        return snapshots

    def consume_lineage_events(self) -> list[dict]:
        """Return and clear explainable split/merge events."""
        events = list(self._lineage_events)
        self._lineage_events.clear()
        return events

    def restore_mcs_history(self, snapshots: list[dict]) -> int:
        """Herstel geldige radarhistoriek vóór de eerste nieuwe providerpoll."""
        restored = 0
        for snapshot in snapshots:
            try:
                storm = Storm.from_mcs_snapshot(snapshot)
            except (KeyError, TypeError, ValueError):
                _LOGGER.warning("Ongeldige MCS-snapshot overgeslagen", exc_info=True)
                continue
            if (
                not storm.radar_system_frames
                and not storm._strike_history
            ):
                continue
            age_minutes = (time.time() - storm.last_update) / 60.0
            expire_minutes = max(self._expire_minutes, RADAR_EXPIRE_MINUTES)
            remove_minutes = max(self._remove_minutes, RADAR_REMOVE_MINUTES)
            if age_minutes > remove_minutes:
                continue
            # Gebruik bij restart exact dezelfde lifecyclegrenzen als tijdens
            # runtime. RainViewer levert typisch tienminutenframes; de oude
            # algemene grens van vijf minuten herstelde zo'n geldig systeem
            # als sluimerend, waarna de eerste providerpoll een nieuwe ID en
            # een éénframesysteem maakte.
            storm.is_dormant = age_minutes > expire_minutes
            self._storms[storm.storm_id] = storm
            cutoff = time.time() - MAX_HISTORY_AGE_MIN * 60
            history = []
            for raw in snapshot.get("motion_history", []):
                try:
                    point = CentroidPoint(
                        lat=float(raw["lat"]),
                        lon=float(raw["lon"]),
                        ts=float(raw["timestamp"]),
                    )
                except (KeyError, TypeError, ValueError):
                    continue
                if point.ts >= cutoff:
                    history.append(point)
            if not history:
                history = self._rebuild_radar_motion_history(storm, cutoff)
            self._history[storm.storm_id] = sorted(
                history, key=lambda point: point.ts
            )[-MAX_HISTORY_POINTS:]
            storm._dirty = True
            self._update_movement(storm)
            restored += 1
        return restored

    def install_committed_tracking_state(
        self,
        snapshots: list[dict],
        *,
        revision: int,
        committed_batch_ids: list[str] | tuple[str, ...] = (),
        publish: bool = True,
    ) -> int:
        """Installeer uitsluitend een reeds duurzaam gecommitteerde snapshot.

        Deze methode voert bewust geen leeftijdsfilter of nieuwe
        meteorologische berekening uit. Anders zou de in-memory startdigest
        meteen afwijken van de duurzame trackinghead. Lifecycleverloop wordt
        daarna als een normale, journaled state-overgang verwerkt.
        """
        if isinstance(revision, bool) or not isinstance(revision, int):
            raise TypeError("duurzame trackingrevision moet een integer zijn")
        if revision < 0:
            raise ValueError(
                "duurzame trackingrevision moet niet-negatief zijn"
            )
        if not isinstance(snapshots, list):
            raise TypeError("tracking snapshots moeten een lijst zijn")
        snapshot_ids = [
            snapshot.get("storm_id")
            for snapshot in snapshots
            if isinstance(snapshot, dict)
        ]
        if (
            len(snapshot_ids) != len(snapshots)
            or any(
                not isinstance(storm_id, str) or not storm_id
                for storm_id in snapshot_ids
            )
            or len(set(snapshot_ids)) != len(snapshot_ids)
        ):
            raise ValueError(
                "duurzame trackingstate vereist unieke storm-ID's"
            )

        candidate_storms: dict[str, Storm] = {}
        candidate_history: dict[str, list[CentroidPoint]] = {}
        for snapshot in snapshots:
            try:
                storm = Storm.from_mcs_snapshot(snapshot)
            except (KeyError, TypeError, ValueError) as err:
                raise ValueError(
                    "ongeldige duurzaam gecommitteerde trackingstate"
                ) from err
            if (
                not storm.radar_system_frames
                and not storm._strike_history
            ):
                raise ValueError(
                    "duurzame snapshot bevat geen herstelbare trackdata"
                )
            history = []
            for raw in snapshot.get("motion_history", []):
                try:
                    history.append(CentroidPoint(
                        lat=float(raw["lat"]),
                        lon=float(raw["lon"]),
                        ts=float(raw["timestamp"]),
                    ))
                except (KeyError, TypeError, ValueError) as err:
                    raise ValueError(
                        "ongeldige motion_history in duurzame trackingstate"
                    ) from err
            if not history:
                history = self._rebuild_radar_motion_history(
                    storm, float("-inf")
                )
            candidate_storms[storm.storm_id] = storm
            candidate_history[storm.storm_id] = sorted(
                history, key=lambda point: point.ts
            )[-MAX_HISTORY_POINTS:]

        live_storms = self._storms
        installed_storms: dict[str, Storm] = {}
        for storm_id, candidate in candidate_storms.items():
            live = live_storms.get(storm_id)
            if live is None:
                installed_storms[storm_id] = candidate
                continue
            live.__dict__.clear()
            live.__dict__.update(candidate.__dict__)
            installed_storms[storm_id] = live

        receipt_ids = []
        seen_receipts = set()
        for batch_id in committed_batch_ids:
            if (
                not isinstance(batch_id, str)
                or not batch_id
                or batch_id in seen_receipts
            ):
                continue
            receipt_ids.append(batch_id)
            seen_receipts.add(batch_id)
        receipt_ids = receipt_ids[-APPLIED_BATCH_RECEIPTS_MAX:]

        self._storms = installed_storms
        self._history = candidate_history
        self._last_geocode_pos = {}
        self._last_merge_check = 0.0
        self._lineage_events = []
        self._revision = revision
        self._applied_batch_ids = {
            batch_id: revision for batch_id in receipt_ids
        }
        if publish and self._on_updated:
            self._on_updated(self.get_active_storms())
        return len(installed_storms)

    def replace_tracking_state(
        self,
        snapshots: list[dict],
        *,
        expected_revision: int | None = None,
        batch_id: str | None = None,
    ) -> int:
        """Vervang de runtime atomair door een gevalideerd workerresultaat."""
        if not self._assert_commit_allowed(expected_revision, batch_id):
            return len(self._storms)
        if not isinstance(snapshots, list):
            raise TypeError("tracking snapshots moeten een lijst zijn")
        snapshot_ids = [
            snapshot.get("storm_id")
            for snapshot in snapshots
            if isinstance(snapshot, dict)
        ]
        if (
            len(snapshot_ids) != len(snapshots)
            or any(
                not isinstance(storm_id, str) or not storm_id
                for storm_id in snapshot_ids
            )
            or len(set(snapshot_ids)) != len(snapshot_ids)
        ):
            raise ValueError(
                "workertracking vereist unieke, niet-lege storm-ID's"
            )
        previous_storms = self._storms
        previous_history = self._history
        previous_geocode = self._last_geocode_pos
        self._storms = {}
        self._history = {}
        self._last_geocode_pos = {}
        try:
            restored = self.restore_mcs_history(snapshots)
            if restored != len(snapshots):
                raise ValueError(
                    "workertracking bevat ongeldige of verlopen snapshots"
                )
        except Exception:
            self._storms = previous_storms
            self._history = previous_history
            self._last_geocode_pos = previous_geocode
            raise
        self._register_commit(batch_id)
        if self._on_updated:
            self._on_updated(self.get_active_storms())
        return restored

    @staticmethod
    def _rebuild_radar_motion_history(
        storm: Storm, cutoff: float
    ) -> list[CentroidPoint]:
        """Herbouw centroidpunten uit oudere snapshots zonder motion_history."""
        grouped: dict[float, list[tuple[float, float, float]]] = {}
        for cell in storm.radar_cells.values():
            if cell.timestamp < cutoff:
                continue
            weight = max(float(cell.area_km2), 1.0)
            grouped.setdefault(float(cell.timestamp), []).append(
                (cell.lat, cell.lon, weight)
            )

        # Oudere snapshots kunnen voor een tijdstip alleen nog een parent-frame
        # bevatten. Gebruik dan het midden van de footprint als veilige fallback.
        for frame in storm.radar_system_frames.values():
            timestamp = float(frame.timestamp)
            if timestamp < cutoff or timestamp in grouped:
                continue
            points = (
                _footprint_vertices(frame)
                or tuple(frame.convective_points)
            )
            if not points:
                continue
            grouped[timestamp] = [(lat, lon, 1.0) for lat, lon in points]

        return StormEngine._radar_anchor_history(grouped)

    @staticmethod
    def _radar_anchor_history(
        grouped: dict[float, list[tuple[float, float, float]]]
    ) -> list[CentroidPoint]:
        """Track the system handle, independent of child-cell weight changes."""
        history: list[CentroidPoint] = []
        previous_cells: list[tuple[float, float, float]] = []
        for timestamp, cells in sorted(grouped.items()):
            if not history:
                total_weight = sum(weight for _, _, weight in cells)
                history.append(CentroidPoint(
                    lat=sum(lat * weight for lat, _, weight in cells) / total_weight,
                    lon=sum(lon * weight for _, lon, weight in cells) / total_weight,
                    ts=timestamp,
                ))
                previous_cells = cells
                continue

            current_to_previous = {
                current_index: min(
                    range(len(previous_cells)),
                    key=lambda previous_index: _haversine(
                        cells[current_index][0], cells[current_index][1],
                        previous_cells[previous_index][0],
                        previous_cells[previous_index][1],
                    ),
                )
                for current_index in range(len(cells))
            }
            previous_to_current = {
                previous_index: min(
                    range(len(cells)),
                    key=lambda current_index: _haversine(
                        previous_cells[previous_index][0],
                        previous_cells[previous_index][1],
                        cells[current_index][0], cells[current_index][1],
                    ),
                )
                for previous_index in range(len(previous_cells))
            }
            pairs = [
                (previous_index, current_index)
                for current_index, previous_index in current_to_previous.items()
                if previous_to_current[previous_index] == current_index
            ]
            if pairs:
                delta_lat = statistics.median(
                    cells[current][0] - previous_cells[previous][0]
                    for previous, current in pairs
                )
                delta_lon = statistics.median(
                    cells[current][1] - previous_cells[previous][1]
                    for previous, current in pairs
                )
                previous_anchor = history[-1]
                history.append(CentroidPoint(
                    lat=previous_anchor.lat + delta_lat,
                    lon=previous_anchor.lon + delta_lon,
                    ts=timestamp,
                ))
            else:
                total_weight = sum(weight for _, _, weight in cells)
                history.append(CentroidPoint(
                    lat=sum(lat * weight for lat, _, weight in cells) / total_weight,
                    lon=sum(lon * weight for _, lon, weight in cells) / total_weight,
                    ts=timestamp,
                ))
            previous_cells = cells
        return history

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

        if remove_ids:
            self._register_commit()
            if self._on_updated:
                self._on_updated(self.get_active_storms())

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

        # Een broncomponent mag uit elkaar vallen. Koppel daarom alleen aan
        # dezelfde ouder zolang de nieuwe kern ruimtelijk nog aansluit.
        parent_system_id = getattr(obs, "parent_system_id", None)
        split_parent: Optional[Storm] = None
        split_distance: Optional[float] = None
        if parent_system_id:
            parent_matches = [
                storm for storm in self._storms.values()
                if not storm.is_dormant
                and parent_system_id in storm.source_system_ids
            ]
            if parent_matches:
                split_parent = min(
                    parent_matches,
                    key=lambda storm: self._distance_to_storm(obs, storm),
                )
                split_distance = self._distance_to_storm(obs, split_parent)
                if (
                    split_distance <= PARENT_SPLIT_DISTANCE_KM
                    and self._radar_assignment_allowed(obs, split_parent)
                ):
                    best_storm = split_parent
                    best_dist = split_distance

        for storm in self._storms.values() if best_storm is None else ():
            if storm.is_dormant:
                continue
            dist = self._distance_to_storm(obs, storm)
            if dist < best_dist and self._radar_assignment_allowed(obs, storm):
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
            if active_count >= self._max_storms:
                _LOGGER.debug(
                    "Capaciteitsrichtwaarde van %d storms overschreden; "
                    "observatie blijft behouden",
                    self._max_storms,
                )
            created = self._new_storm(obs)
            if split_parent is not None and split_distance is not None:
                created.parent_storm_ids.add(split_parent.storm_id)
                created.lineage_generation = (
                    split_parent.lineage_generation + 1
                )
                self._lineage_events.append({
                    "event": "split",
                    "timestamp": float(obs.timestamp),
                    "parent_storm_id": split_parent.storm_id,
                    "child_storm_id": created.storm_id,
                    "source_system_id": str(parent_system_id),
                    "separation_km": round(split_distance, 2),
                })

    def _radar_assignment_allowed(self, obs, storm: Storm) -> bool:
        """Controleer identiteitsevidence zonder een snelheidsplafond.

        De ruimtelijke kandidaatselectie gebeurt begrensd in
        ``_assign_observation``. Een afstand gedeeld door een tijdsinterval is
        hier bewust geen afwijzingsgrond: twee echte posities blijven
        evidence, ook wanneer de daaruit volgende snelheid uitzonderlijk is.
        Alleen tegenstrijdige bronidentiteit binnen hetzelfde radarframe sluit
        een kandidaat uit.
        """
        from .observation import ObservationType

        if obs.obs_type != ObservationType.RADAR:
            return True

        parent_system_id = getattr(obs, "parent_system_id", None)
        same_frame_cells = [
            cell for cell in storm.radar_cells.values()
            if abs(float(cell.timestamp) - float(obs.timestamp)) < 1.0
        ]
        if same_frame_cells and parent_system_id:
            frame_parents = {
                cell.parent_system_id for cell in same_frame_cells
                if cell.parent_system_id
            }
            if frame_parents and parent_system_id not in frame_parents:
                return False
        return True

    @staticmethod
    def _distance_to_storm(obs, storm: Storm) -> float:
        """Match tegen echte bronfootprints, niet enkel rastercentroids."""
        def compact(points, limit=24):
            values = tuple(points or ())
            if len(values) <= limit:
                return values
            step = max(1, len(values) // limit)
            return values[::step][:limit]

        observation_points = (
            (obs.lat, obs.lon),
            *compact(_footprint_vertices(obs)),
        )
        storm_points = [(storm.centroid_lat, storm.centroid_lon)]
        for cell in storm.radar_cells.values():
            storm_points.append((cell.lat, cell.lon))
            storm_points.extend(compact(_footprint_vertices(cell)))
        distances = [
            _haversine(obs_lat, obs_lon, storm_lat, storm_lon)
            for obs_lat, obs_lon in observation_points
            for storm_lat, storm_lon in storm_points
        ]
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
            storm.record_radar_cell(obs)

    def _apply_rain_verification(self, obs) -> None:
        """
        Pas Netatmo-regenverificatie toe op bestaande storms.
        Creëert NOOIT een nieuwe storm.

        Een station dat regen meet binnen de hull van een storm verhoogt
        de confidence. Een station zonder regen in het storm-pad verlaagt
        de confidence (mogelijke clutter).
        """
        from .observation import observation_is_wet

        now = time.time()
        freshness = {"netatmo": 600, "synoptic": 1200, "metar": 2100}
        if now - float(obs.timestamp) > freshness.get(str(obs.source), 900):
            return

        matches = []
        for storm in self._storms.values():
            if storm.is_dormant:
                continue
            frames = list(storm.radar_system_frames.values())
            selected = min(
                frames,
                key=lambda frame: abs(
                    float(frame.timestamp) - float(obs.timestamp)
                ),
                default=None,
            )
            if (
                selected is None
                or abs(
                    float(selected.timestamp) - float(obs.timestamp)
                ) > GROUND_FOOTPRINT_TIME_TOLERANCE_S
            ):
                continue
            selected_timestamp = float(selected.timestamp)
            footprint_distances = [
                distance
                for frame in frames
                if abs(
                    float(frame.timestamp) - selected_timestamp
                ) <= SAME_RADAR_FRAME_TOLERANCE_S
                and (
                    distance := point_to_polygon_distance_km(
                        obs.lat,
                        obs.lon,
                        _footprint_geometry(frame),
                    )
                ) is not None
            ]
            footprint_distance = min(footprint_distances, default=None)
            if footprint_distance is None or footprint_distance > 8.0:
                continue
            matches.append((footprint_distance, storm.storm_id, storm))

        matches.sort(key=lambda item: (item[0], item[1]))
        if not matches:
            return
        if (
            len(matches) > 1
            and abs(matches[0][0] - matches[1][0]) <= 0.05
        ):
            _LOGGER.debug(
                "Grondstation %s niet aan een storm gekoppeld: "
                "overlappende footprints",
                obs.station_id,
            )
            return

        storm = matches[0][2]
        if observation_is_wet(obs):
            storm.netatmo_confirmations += 1
            _LOGGER.debug(
                "%s bevestigt storm %s bij station %s",
                obs.source, storm.storm_id, obs.station_id
            )
        else:
            storm.netatmo_no_rain_count += 1

    def _new_storm(self, obs) -> Storm:
        """Maak een nieuwe storm aan op basis van de eerste observatie."""
        identity_seed = "|".join((
            str(getattr(obs, "parent_system_id", "") or ""),
            str(getattr(obs, "radar_cell_id", "") or ""),
            str(getattr(obs, "source", "") or ""),
            f"{float(obs.lat):.5f}",
            f"{float(obs.lon):.5f}",
            str(int(float(obs.timestamp))),
        ))
        storm = Storm(
            storm_id=str(uuid.uuid5(uuid.NAMESPACE_URL, identity_seed))[:8],
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
        Bouw de trajecthistorie primair uit afzonderlijke radarframes.

        Bliksemposities zijn geen veilige proxy voor de verplaatsing van de
        neerslagcontour: nieuwe ontladingen kunnen aan een andere flank
        ontstaan. Daarom gebruikt de neerslagprognose radarcentroids zodra die
        beschikbaar zijn; bliksem blijft alleen een fallback.
        """
        cutoff = time.time() - MAX_HISTORY_AGE_MIN * 60
        grouped: dict[float, list[tuple[float, float, float]]] = {}
        for cell in storm.radar_cells.values():
            if cell.timestamp < cutoff:
                continue
            weight = max(float(cell.area_km2), 1.0)
            grouped.setdefault(float(cell.timestamp), []).append(
                (cell.lat, cell.lon, weight)
            )

        hist = self._history.setdefault(storm.storm_id, [])
        if grouped:
            existing = {round(point.ts, 3): point for point in hist}
            for point in self._radar_anchor_history(grouped):
                existing[round(point.ts, 3)] = point
            hist[:] = sorted(existing.values(), key=lambda point: point.ts)
            self._prune_history(storm.storm_id)
            hist = self._history[storm.storm_id]
            latest = hist[-1]
            storm.centroid_lat = latest.lat
            storm.centroid_lon = latest.lon
            storm.motion_basis = "radar_centroid"
            return

        lightning = storm.strikes_in_window(minutes=5)
        if not lightning:
            return
        reference_ts = max(ts for ts, _, _ in lightning)
        total_weight = 0.0
        lat_sum = 0.0
        lon_sum = 0.0

        for ts, lat, lon in lightning:
            age    = reference_ts - ts
            weight = max(0.1, 1.0 - age / 300)
            lat_sum      += lat * weight
            lon_sum      += lon * weight
            total_weight += weight

        if total_weight > 0:
            new_lat = lat_sum / total_weight
            new_lon = lon_sum / total_weight

            point = CentroidPoint(lat=new_lat, lon=new_lon, ts=reference_ts)
            if hist and abs(hist[-1].ts - reference_ts) < 1.0:
                hist[-1] = point
            else:
                hist.append(point)
            self._prune_history(storm.storm_id)

            storm.centroid_lat = new_lat
            storm.centroid_lon = new_lon
            storm.motion_basis = "lightning_fallback"

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
        Kies adaptief tussen constante snelheid en constante versnelling.

        Het versnellingsmodel wordt alleen gekozen als een hindcast op de
        laatste frames beter presteert. Zo blijft een voorspelbaar gebogen pad
        bruikbaar, zonder elke centroidschommeling als bocht te interpreteren.
        """
        if not storm._dirty:
            return

        hist = self._history.get(storm.storm_id, [])
        if len(hist) < MIN_HISTORY_POINTS:
            storm._dirty = False
            return

        estimate = fit_adaptive_trajectory(hist)
        if estimate is None:
            storm._dirty = False
            return
        average_heading = estimate.heading_deg
        average_speed = estimate.speed_kmh
        previous, latest = sorted(hist, key=lambda point: point.ts)[-2:]
        elapsed_hours = (latest.ts - previous.ts) / 3600.0
        if elapsed_hours <= 0.0:
            storm._dirty = False
            return
        mean_lat = (previous.lat + latest.lat) / 2.0
        interval_north = (
            (latest.lat - previous.lat) * 110.574 / elapsed_hours
        )
        interval_east = (
            (latest.lon - previous.lon)
            * 111.32
            * math.cos(math.radians(mean_lat))
            / elapsed_hours
        )
        speed = math.hypot(interval_east, interval_north)
        heading = (
            math.degrees(math.atan2(interval_east, interval_north)) % 360.0
            if speed >= 0.5
            else None
        )
        storm.heading_deg = round(heading, 1) if heading is not None else None
        storm.speed_kmh = round(speed, 1) if speed is not None else None
        storm.motion_average_heading_deg = (
            round(average_heading, 1) if average_heading is not None else None
        )
        storm.motion_average_speed_kmh = round(average_speed, 1)
        storm.motion_sample_count = len(hist)
        storm.motion_history_minutes = round((hist[-1].ts - hist[0].ts) / 60.0, 1)
        storm.motion_fit_quality = estimate.fit_quality
        storm.motion_model = estimate.model
        storm.motion_prediction_error_km = estimate.prediction_error_km
        span_hours = max(
            1.0 / 6.0, (hist[-1].ts - hist[0].ts) / 3600.0
        )
        uncertainty = max(
            5.0,
            float(speed or 0.0) * (1.0 - estimate.fit_quality),
            estimate.prediction_error_km / span_hours,
        )
        if len(hist) < 4:
            uncertainty = max(uncertainty, float(speed or 0.0) * 0.5)
        storm.motion_forecast_speed_min_kmh = round(
            max(0.0, float(speed or 0.0) - uncertainty), 1
        )
        storm.motion_forecast_speed_max_kmh = round(
            float(speed or 0.0) + uncertainty, 1
        )
        storm.motion_model_gain = round(estimate.model_gain, 3)
        storm.velocity_east_kmh = round(interval_east, 2)
        storm.velocity_north_kmh = round(interval_north, 2)
        storm.acceleration_east_kmh2 = round(
            estimate.acceleration_east_kmh2, 2
        )
        storm.acceleration_north_kmh2 = round(
            estimate.acceleration_north_kmh2, 2
        )
        storm.turn_rate_deg_per_hour = round(
            estimate.turn_rate_deg_per_hour, 1
        )
        storm.trajectory_curved = bool(estimate.is_curved)
        storm.curvature_radius_km = (
            round(estimate.curvature_radius_km, 1)
            if estimate.curvature_radius_km is not None else None
        )
        storm.trajectory_outliers_rejected = estimate.outliers_rejected
        storm.confidence = self._calc_confidence(
            hist,
            heading,
            speed,
            fit_quality=estimate.fit_quality,
            prediction_error_km=estimate.prediction_error_km,
        )
        storm.motion_estimate_status = (
            "measured_interval" if len(hist) < 4 else "tracked"
        )
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

        heading = math.degrees(math.atan2(dlon_kmh, dlat_kmh)) % 360
        return round(heading, 1), round(speed_kmh, 1)

    def _calc_confidence(
        self,
        hist: list[CentroidPoint],
        heading: Optional[float],
        speed: Optional[float],
        *,
        fit_quality: Optional[float] = None,
        prediction_error_km: Optional[float] = None,
    ) -> str:
        """Bereken confidence op basis van historie én echte hindcastfout."""
        n = len(hist)
        span_minutes = (hist[-1].ts - hist[0].ts) / 60.0 if n > 1 else 0.0
        quality = (
            self._fit_quality(hist) if fit_quality is None else fit_quality
        )
        error = math.inf if prediction_error_km is None else prediction_error_km
        if heading is None or n < MIN_HISTORY_POINTS or span_minutes < 5:
            return "Onvoldoende data"
        if (
            n >= 6
            and span_minutes >= 15
            and quality >= 0.80
            and error <= 3.0
            and speed
            and speed > 1
        ):
            return "Hoog"
        if span_minutes >= 10 and quality >= 0.55 and error <= 8.0:
            return "Matig"
        return "Laag"

    @staticmethod
    def _fit_quality(hist: list[CentroidPoint]) -> float:
        """Gemiddelde R² van lat(t) en lon(t), als maat voor rechte beweging."""
        if len(hist) < 2:
            return 0.0
        times = [point.ts - hist[0].ts for point in hist]

        def r_squared(values: list[float]) -> float:
            slope = _slope(times, values)
            if slope is None:
                return 0.0
            mean_t = sum(times) / len(times)
            mean_value = sum(values) / len(values)
            intercept = mean_value - slope * mean_t
            residual = sum(
                (value - (intercept + slope * timestamp)) ** 2
                for timestamp, value in zip(times, values)
            )
            total = sum((value - mean_value) ** 2 for value in values)
            return 1.0 if total <= 1e-12 and residual <= 1e-12 else max(0.0, 1 - residual / total) if total > 0 else 0.0

        return round((r_squared([p.lat for p in hist]) + r_squared([p.lon for p in hist])) / 2, 3)

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
        ]
        for cell in storm.radar_cells.values():
            if cell.timestamp < cutoff:
                continue
            points.extend(
                _footprint_vertices(cell)
                or ((cell.lat, cell.lon),)
            )

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
        O(n²); MERGE_THROTTLE_S begrenst hoe vaak deze controle draait.
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
                if dist <= MERGE_RADIUS_KM and self._merge_supported(
                    s1, s2, dist
                ):
                    self._merge(s1, s2)
                    self._lineage_events.append({
                        "event": "merge",
                        "timestamp": time.time(),
                        "keeper_storm_id": s1.storm_id,
                        "merged_storm_id": s2.storm_id,
                        "distance_km": round(dist, 2),
                    })
                    merged.add(s2.storm_id)
                    _LOGGER.info("Storms %s + %s samengevoegd (%.1fkm)",
                                 s1.storm_id, s2.storm_id, dist)

        for storm_id in merged:
            self._storms.pop(storm_id, None)
            self._history.pop(storm_id, None)
            self._last_geocode_pos.pop(storm_id, None)

    @staticmethod
    def _heading_difference(first: Storm, second: Storm) -> Optional[float]:
        if first.heading_deg is None or second.heading_deg is None:
            return None
        return abs((first.heading_deg - second.heading_deg + 180.0) % 360.0 - 180.0)

    def _merge_supported(
        self, first: Storm, second: Storm, distance_km: float
    ) -> bool:
        """Merge alleen bij broncontinuïteit of werkelijk rakende footprints."""
        first_latest = self._latest_radar_cells(first)
        second_latest = self._latest_radar_cells(second)
        if first_latest and second_latest:
            first_ts = max(cell.timestamp for cell in first_latest)
            second_ts = max(cell.timestamp for cell in second_latest)
            first_parents = {
                cell.parent_system_id for cell in first_latest
                if cell.parent_system_id
            }
            second_parents = {
                cell.parent_system_id for cell in second_latest
                if cell.parent_system_id
            }
            if (
                abs(first_ts - second_ts) < 1.0
                and first_parents
                and second_parents
                and first_parents.isdisjoint(second_parents)
                and self._radar_cell_gap_km(
                    first_latest, second_latest
                ) > RADAR_SAME_FRAME_MERGE_GAP_KM
            ):
                return False

        heading_difference = self._heading_difference(first, second)
        if (
            heading_difference is not None
            and heading_difference > MERGE_HEADING_TOLERANCE_DEG
        ):
            return False

        shared_sources = first.source_system_ids & second.source_system_ids
        if shared_sources and distance_km <= PARENT_SPLIT_DISTANCE_KM:
            return True

        footprint_limit = max(
            12.0,
            0.75 * (float(first.radius_km) + float(second.radius_km)),
        )
        if (
            first.radar_cells
            and second.radar_cells
            and distance_km <= footprint_limit
        ):
            return True

        # Oude bliksem-only werking behouden; radartracks vereisen sterker
        # bewijs zodat twee nabije, uiteenlopende buien apart blijven.
        return (
            not first.radar_cells
            and not second.radar_cells
            and distance_km <= max(12.0, self._cluster_radius * 2.0)
        )

    @staticmethod
    def _latest_radar_cells(storm: Storm) -> list:
        cells = list(storm.radar_cells.values())
        if not cells:
            return []
        latest_ts = max(float(cell.timestamp) for cell in cells)
        return [
            cell for cell in cells
            if abs(float(cell.timestamp) - latest_ts) < 1.0
        ]

    @staticmethod
    def _radar_cell_gap_km(first_cells: list, second_cells: list) -> float:
        def points(cell):
            return (
                (cell.lat, cell.lon),
                *_footprint_vertices(cell),
            )

        return min(
            _haversine(first_lat, first_lon, second_lat, second_lon)
            for first in first_cells
            for second in second_cells
            for first_lat, first_lon in points(first)
            for second_lat, second_lon in points(second)
        )

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
        keeper.parent_system_footprint_polygons.update(
            other.parent_system_footprint_polygons
        )
        keeper._source_system_last_seen.update(other._source_system_last_seen)
        keeper.radar_system_frames.update(other.radar_system_frames)
        keeper.merged_storm_ids.add(other.storm_id)
        keeper.merged_storm_ids.update(other.merged_storm_ids)
        keeper.parent_storm_ids.update(other.parent_storm_ids)
        keeper.lineage_generation = max(
            keeper.lineage_generation, other.lineage_generation
        )
        keeper.update_radar_classification()

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
            has_radar_history = bool(storm.radar_system_frames)
            expire_minutes = (
                max(self._expire_minutes, RADAR_EXPIRE_MINUTES)
                if has_radar_history else self._expire_minutes
            )
            remove_minutes = (
                max(self._remove_minutes, RADAR_REMOVE_MINUTES)
                if has_radar_history else self._remove_minutes
            )
            if age > remove_minutes:
                to_remove.append(storm.storm_id)
                _LOGGER.info("Storm %s verwijderd (%.0f min inactief)",
                             storm.storm_id, age)
            elif age > expire_minutes:
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
