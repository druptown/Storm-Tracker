"""Passieve virtuele verkenners die radarpassages met grondmeters verifiëren."""
from __future__ import annotations

from dataclasses import dataclass, asdict
import math
import time


EARLY_WINDOW_SECONDS = 10 * 60
LATE_WINDOW_SECONDS = 15 * 60
UNVERIFIABLE_TIMEOUT_SECONDS = 30 * 60
MAX_ACTIVE_PROBES = 8
MAX_PROBES_PER_STORM = 2
MAX_PASS_DISTANCE_KM = 12.0
MIN_STATION_SEPARATION_KM = 4.0
ROAMING_LEAD_MINUTES = 30.0
ROAMING_EXISTING_GAUGE_RADIUS_KM = 12.0
MAX_STORM_AGE_SECONDS = 15 * 60
EARTH_RADIUS_KM = 6371.0088


def _probe_provider(station_id: str) -> str:
    prefix = str(station_id).split(":", 1)[0]
    return prefix if prefix in {"synoptic", "metar"} else "netatmo"


def _probe_provider_name(station_id: str) -> str:
    return {
        "synoptic": "Synoptic",
        "metar": "METAR",
        "netatmo": "Netatmo",
    }[_probe_provider(station_id)]


def _storm_is_fresh(storm, observed_at: float) -> bool:
    """Sta alleen verkenners toe op een recent radarbeeld."""
    timestamp = getattr(storm, "last_radar_timestamp", None)
    if timestamp is None:
        return True
    try:
        age = observed_at - float(timestamp)
    except (TypeError, ValueError):
        return False
    return -60.0 <= age <= MAX_STORM_AGE_SECONDS


def _radar_source_is_fresh(source: dict | None, observed_at: float) -> bool:
    """Herken een geldige lege radarscan, ook zonder stormobjecten."""
    if not isinstance(source, dict):
        return False
    age = source.get("age_seconds")
    if age is not None:
        try:
            return -60.0 <= float(age) <= MAX_STORM_AGE_SECONDS
        except (TypeError, ValueError):
            return False
    timestamp = (
        source.get("frame_timestamp")
        or source.get("timestamp")
        or source.get("worker_frame_timestamp")
    )
    if timestamp is None:
        return False
    try:
        return -60.0 <= observed_at - float(timestamp) <= MAX_STORM_AGE_SECONDS
    except (TypeError, ValueError):
        return False


def _destination(
    latitude: float,
    longitude: float,
    bearing_deg: float,
    distance_km: float,
) -> tuple[float, float]:
    """Projecteer een onbegrensde geldige vector over de bol."""
    values = (latitude, longitude, bearing_deg, distance_km)
    if not all(math.isfinite(float(value)) for value in values):
        raise ValueError("projectie bevat geen eindige getallen")
    if not -90.0 <= float(latitude) <= 90.0:
        raise ValueError("projectie bevat ongeldige latitude")
    if float(distance_km) < 0.0:
        raise ValueError("projectieafstand kan niet negatief zijn")
    lat1 = math.radians(float(latitude))
    lon1 = math.radians(float(longitude))
    bearing = math.radians(float(bearing_deg))
    angular_distance = float(distance_km) / EARTH_RADIUS_KM
    lat2 = math.asin(max(
        -1.0,
        min(
            1.0,
            math.sin(lat1) * math.cos(angular_distance)
            + math.cos(lat1)
            * math.sin(angular_distance)
            * math.cos(bearing),
        ),
    ))
    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(angular_distance) * math.cos(lat1),
        math.cos(angular_distance) - math.sin(lat1) * math.sin(lat2),
    )
    longitude_result = (
        math.degrees(lon2) + 180.0
    ) % 360.0 - 180.0
    return math.degrees(lat2), longitude_result


def _current_passage(storm, target_lat: float, target_lon: float) -> dict:
    """Bereken de actuele, geometrische passage zonder oude ETA-corridor."""
    speed = getattr(storm, "speed_kmh", None)
    heading = getattr(storm, "heading_deg", None)
    if speed is None or heading is None or float(speed) <= 1.0:
        return {"valid": False, "reason": "traject_gewijzigd"}
    east = getattr(storm, "velocity_east_kmh", None)
    north = getattr(storm, "velocity_north_kmh", None)
    if east is None:
        east = float(speed) * math.sin(math.radians(float(heading)))
    if north is None:
        north = float(speed) * math.cos(math.radians(float(heading)))
    east, north = float(east), float(north)
    velocity_squared = east * east + north * north
    if velocity_squared <= 1.0:
        return {"valid": False, "reason": "traject_gewijzigd"}
    delta_north = (target_lat - float(storm.centroid_lat)) * 110.574
    lon_scale = max(
        20.0, 111.320 * math.cos(math.radians(float(storm.centroid_lat)))
    )
    delta_east = (target_lon - float(storm.centroid_lon)) * lon_scale
    distance = math.hypot(delta_east, delta_north)
    approach = (
        float(speed) if distance <= 0.01
        else (east * delta_east + north * delta_north) / distance
    )
    closest_hours = (
        (delta_east * east + delta_north * north) / velocity_squared
    )
    if approach <= 1.0 or closest_hours < 0.0:
        return {"valid": False, "reason": "traject_gewijzigd"}
    minutes = closest_hours * 60.0
    closest_east = delta_east - east * closest_hours
    closest_north = delta_north - north * closest_hours
    footprint_distance = max(
        0.0,
        math.hypot(closest_east, closest_north)
        - float(getattr(storm, "radius_km", 0.0) or 0.0),
    )
    return {
        "valid": 0.0 <= minutes <= 90.0
        and footprint_distance <= MAX_PASS_DISTANCE_KM,
        "reason": "traject_gewijzigd",
        "minutes": round(minutes, 1),
        "pass_distance_km": round(footprint_distance, 1),
    }


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


def _exploratory_motion(storm, target_lat: float, target_lon: float) -> dict:
    """Projecteer ook lage-betrouwbaarheidsvectoren voor passieve kalibratie."""
    operational = storm.motion_to_target(target_lat, target_lon)
    if operational.get("closest_pass_minutes") is not None:
        return operational

    speed = getattr(storm, "speed_kmh", None)
    heading = getattr(storm, "heading_deg", None)
    if speed is None or heading is None or float(speed) <= 0:
        return operational
    east = getattr(storm, "velocity_east_kmh", None)
    north = getattr(storm, "velocity_north_kmh", None)
    if east is None:
        east = float(speed) * math.sin(math.radians(float(heading)))
    if north is None:
        north = float(speed) * math.cos(math.radians(float(heading)))
    east = float(east)
    north = float(north)
    velocity_squared = east * east + north * north
    if velocity_squared <= 1.0:
        return operational

    delta_north = (float(target_lat) - float(storm.centroid_lat)) * 110.574
    lon_scale = max(
        20.0,
        111.320 * math.cos(math.radians(float(storm.centroid_lat))),
    )
    delta_east = (float(target_lon) - float(storm.centroid_lon)) * lon_scale
    distance = math.hypot(delta_east, delta_north)
    if distance <= 0.01:
        approach = float(speed)
    else:
        approach = (
            east * delta_east + north * delta_north
        ) / distance
    closest_hours = max(
        0.0,
        (delta_east * east + delta_north * north) / velocity_squared,
    )
    closest_minutes = closest_hours * 60.0
    closest_east = delta_east - east * closest_hours
    closest_north = delta_north - north * closest_hours
    centroid_distance = math.hypot(closest_east, closest_north)
    footprint_distance = max(
        0.0, centroid_distance - float(getattr(storm, "radius_km", 0.0) or 0.0)
    )
    return {
        **operational,
        "approach_speed_kmh": round(approach, 1),
        "moving_towards": approach > 1.0,
        "closest_pass_minutes": round(closest_minutes, 1),
        "closest_pass_distance_km": round(centroid_distance, 1),
        "footprint_pass_distance_km": round(footprint_distance, 1),
        "passage_classification": (
            "raak" if footprint_distance <= 0.1
            else "rand" if footprint_distance <= MAX_PASS_DISTANCE_KM
            else "mist"
        ),
        "exploratory_projection": True,
    }


@dataclass(slots=True)
class VirtualProbe:
    probe_id: str
    storm_id: str
    region_id: str
    station_id: str
    latitude: float
    longitude: float
    created_at: float
    expected_at: float
    eta_minutes: float
    passage: str
    pass_distance_km: float
    motion_confidence: str
    radar_source: str | None


class VirtualProbeManager:
    """Plant en evalueert grondwaarheidspunten zonder operationele invloed."""

    def __init__(
        self,
        restored: list[dict] | None = None,
        history: dict | None = None,
    ) -> None:
        history = history or {}
        self._active: dict[str, VirtualProbe] = {}
        self._completed = int(history.get("completed", 0) or 0)
        self._hits = int(history.get("hits", 0) or 0)
        self._misses = int(history.get("misses", 0) or 0)
        self._unverifiable = int(history.get("unverifiable", 0) or 0)
        self._cancelled = int(history.get("cancelled", 0) or 0)
        self._recent_outcomes: list[dict] = list(
            history.get("recent_outcomes", [])
        )[:20]
        for raw in restored or []:
            try:
                probe = VirtualProbe(**raw)
            except (TypeError, ValueError):
                continue
            self._active[probe.probe_id] = probe

    def choose_roaming_target(
        self,
        *,
        storms_by_engine: dict[str, list],
        observations_by_engine: dict[str, list],
        now: float | None = None,
    ) -> dict | None:
        """Kies één gedeeld Netatmo-zoekgebied met de hoogste leerwaarde.

        Een nog niet afgewerkte voorspelling krijgt altijd voorrang. Anders
        projecteren we per regio de beste bevestigde bui dertig minuten
        vooruit. Als de vaste regionale query daar al een bruikbare regenmeter
        ziet, is een extra API-query niet nodig.
        """
        observed_at = float(now if now is not None else time.time())
        active = sorted(
            self._active.values(),
            key=lambda probe: (
                abs(probe.expected_at - observed_at),
                probe.created_at,
            ),
        )
        if active:
            probe = active[0]
            return {
                "region_id": probe.region_id,
                "latitude": probe.latitude,
                "longitude": probe.longitude,
                "reason": "lopende_verificatie",
                "storm_id": probe.storm_id,
                "expected_at": probe.expected_at,
                "score": 1000.0,
            }

        candidates: list[tuple[float, dict]] = []
        confidence_score = {
            "Onvoldoende data": 55.0,
            "Laag": 45.0,
            "Matig": 35.0,
            "Hoog": 25.0,
        }
        for region_id, storms in storms_by_engine.items():
            existing_gauges = [
                item
                for item in observations_by_engine.get(region_id, [])
                if item.station_id
                and getattr(item, "rain_capable", False)
                and observed_at - float(item.timestamp) <= 15 * 60
            ]
            for storm in storms:
                if not _storm_is_fresh(storm, observed_at):
                    continue
                speed = getattr(storm, "speed_kmh", None)
                heading = getattr(storm, "heading_deg", None)
                if (
                    getattr(storm, "tracking_status", None) != "bevestigd"
                    or int(getattr(storm, "motion_sample_count", 0)) < 2
                    or speed is None
                    or heading is None
                ):
                    continue
                try:
                    speed_value = float(speed)
                    heading_value = float(heading)
                except (TypeError, ValueError):
                    continue
                if (
                    not math.isfinite(speed_value)
                    or speed_value < 0.0
                    or not math.isfinite(heading_value)
                ):
                    continue
                east = getattr(storm, "velocity_east_kmh", None)
                north = getattr(storm, "velocity_north_kmh", None)
                if east is None:
                    east = speed_value * math.sin(math.radians(heading_value))
                if north is None:
                    north = speed_value * math.cos(math.radians(heading_value))
                try:
                    east_value = float(east)
                    north_value = float(north)
                    vector_speed = math.hypot(east_value, north_value)
                    vector_heading = math.degrees(
                        math.atan2(east_value, north_value)
                    ) % 360.0
                    latitude, longitude = _destination(
                        float(storm.centroid_lat),
                        float(storm.centroid_lon),
                        vector_heading,
                        vector_speed * ROAMING_LEAD_MINUTES / 60.0,
                    )
                except (TypeError, ValueError):
                    continue
                if any(
                    _distance_km(
                        latitude,
                        longitude,
                        float(gauge.lat),
                        float(gauge.lon),
                    ) <= ROAMING_EXISTING_GAUGE_RADIUS_KM
                    for gauge in existing_gauges
                ):
                    continue
                uncertainty = float(
                    getattr(storm, "uncertainty_km", 0.0) or 0.0
                )
                score = (
                    confidence_score.get(str(storm.confidence), 55.0)
                    + min(30.0, uncertainty)
                    + min(25.0, vector_speed / 4.0)
                )
                candidates.append((score, {
                    "region_id": str(region_id),
                    "latitude": round(latitude, 5),
                    "longitude": round(longitude, 5),
                    "reason": "hoogste_kalibratiewaarde",
                    "storm_id": str(storm.storm_id),
                    "expected_at": observed_at + ROAMING_LEAD_MINUTES * 60.0,
                    "score": round(score, 1),
                }))
        if not candidates:
            return None
        return max(candidates, key=lambda item: item[0])[1]

    def update(
        self,
        *,
        storms_by_engine: dict[str, list],
        observations_by_engine: dict[str, list],
        radar_sources_by_engine: dict[str, dict],
        now: float | None = None,
    ) -> tuple[list[dict], dict]:
        observed_at = float(now if now is not None else time.time())
        samples: list[dict] = []
        stations = {
            (engine_id, str(item.station_id)): item
            for engine_id, observations in observations_by_engine.items()
            for item in observations
            if item.station_id and getattr(item, "rain_capable", False)
        }
        storms = {
            (str(engine_id), str(storm.storm_id)): storm
            for engine_id, items in storms_by_engine.items()
            for storm in items
        }
        populated_regions = {
            str(engine_id) for engine_id, items in storms_by_engine.items()
            if items
        }

        for probe_id, probe in list(self._active.items()):
            storm = storms.get((probe.region_id, probe.storm_id))
            cancellation = None
            if storm is None:
                fresh_empty_scan = _radar_source_is_fresh(
                    radar_sources_by_engine.get(probe.region_id),
                    observed_at,
                )
                if probe.region_id in populated_regions or fresh_empty_scan:
                    cancellation = "stormobject_verdwenen"
            elif storm is not None:
                if not _storm_is_fresh(storm, observed_at):
                    cancellation = "radarbeeld_verouderd"
                else:
                    passage = _current_passage(
                        storm, probe.latitude, probe.longitude
                    )
                    if not passage["valid"]:
                        cancellation = str(passage["reason"])
            if cancellation is not None:
                samples.append(self._outcome_sample(
                    probe,
                    observed_at=observed_at,
                    outcome=cancellation,
                    rain_live=None,
                    rain_hour=None,
                    timing_error_minutes=round(
                        (observed_at - probe.expected_at) / 60.0, 1
                    ),
                ))
                self._completed += 1
                self._cancelled += 1
                self._recent_outcomes.insert(0, {
                    "probe_id": probe.probe_id,
                    "region_id": probe.region_id,
                    "station_id": probe.station_id,
                    "latitude": probe.latitude,
                    "longitude": probe.longitude,
                    "outcome": cancellation,
                    "verified_at": observed_at,
                    "timing_error_minutes": None,
                    "rain_live_mm_h": None,
                    "rain_60min_mm": None,
                    "summary": (
                        "Controle beëindigd omdat het stormobject niet meer "
                        "bestaat."
                        if cancellation == "stormobject_verdwenen"
                        else "Controle beëindigd omdat de actuele buibaan het "
                        "controlepunt niet meer bereikt."
                    ),
                })
                self._recent_outcomes = self._recent_outcomes[:5]
                del self._active[probe_id]
                continue
            station = stations.get((probe.region_id, probe.station_id))
            station_fresh = (
                station is not None
                and observed_at - float(station.timestamp) <= {
                    "netatmo": 10 * 60,
                    "synoptic": 20 * 60,
                    "metar": 35 * 60,
                }.get(str(getattr(station, "source", "")), 15 * 60)
            )
            if not station_fresh:
                if (
                    observed_at
                    <= probe.expected_at + UNVERIFIABLE_TIMEOUT_SECONDS
                ):
                    continue
                outcome = "niet_verifieerbaar"
                rain_live = None
                rain_hour = None
            else:
                rain_live = float(station.rain_mm or 0.0)
                rain_hour = float(station.rain_60min or 0.0)
                inside_window = (
                    probe.expected_at - EARLY_WINDOW_SECONDS
                    <= observed_at
                    <= probe.expected_at + LATE_WINDOW_SECONDS
                )
                precipitation_present = getattr(
                    station, "precipitation_present", None
                )
                wet = (
                    bool(precipitation_present)
                    if precipitation_present is not None
                    else rain_live >= 0.1
                )
                if inside_window and wet:
                    outcome = "regen_bevestigd"
                elif observed_at > probe.expected_at + LATE_WINDOW_SECONDS:
                    outcome = "droog_op_verwacht_moment"
                else:
                    continue
            timing_error_minutes = round(
                (observed_at - probe.expected_at) / 60.0, 1
            )
            samples.append(self._outcome_sample(
                probe,
                observed_at=observed_at,
                outcome=outcome,
                rain_live=rain_live,
                rain_hour=rain_hour,
                timing_error_minutes=timing_error_minutes,
            ))
            self._completed += 1
            if outcome == "regen_bevestigd":
                self._hits += 1
            elif outcome == "droog_op_verwacht_moment":
                self._misses += 1
            else:
                self._unverifiable += 1
            self._recent_outcomes.insert(0, {
                "probe_id": probe.probe_id,
                "region_id": probe.region_id,
                "station_id": probe.station_id,
                "latitude": probe.latitude,
                "longitude": probe.longitude,
                "outcome": outcome,
                "verified_at": observed_at,
                "timing_error_minutes": timing_error_minutes,
                "rain_live_mm_h": rain_live,
                "rain_60min_mm": rain_hour,
                "summary": (
                    f"Regen bevestigd bij {_probe_provider_name(probe.station_id)} "
                    f"{probe.station_id}; "
                    f"timingafwijking {timing_error_minutes:+.1f} min."
                    if outcome == "regen_bevestigd"
                    else (
                        f"Geen regen gemeten bij "
                        f"{_probe_provider_name(probe.station_id)} "
                        f"{probe.station_id} "
                        "binnen het verwachte tijdvenster."
                        if outcome == "droog_op_verwacht_moment"
                        else f"{_probe_provider_name(probe.station_id)} "
                        f"{probe.station_id} leverde niet tijdig "
                        "een actuele meting; voorspelling niet beoordeeld."
                    )
                ),
            })
            self._recent_outcomes = self._recent_outcomes[:5]
            del self._active[probe_id]

        if len(self._active) < MAX_ACTIVE_PROBES:
            samples.extend(self._plan(
                storms_by_engine=storms_by_engine,
                observations_by_engine=observations_by_engine,
                radar_sources_by_engine=radar_sources_by_engine,
                observed_at=observed_at,
            ))
        return samples, self.diagnostics(observed_at)

    def _plan(
        self,
        *,
        storms_by_engine: dict[str, list],
        observations_by_engine: dict[str, list],
        radar_sources_by_engine: dict[str, dict],
        observed_at: float,
    ) -> list[dict]:
        samples: list[dict] = []
        active_pairs = {
            (probe.storm_id, probe.station_id)
            for probe in self._active.values()
        }
        active_per_storm: dict[tuple[str, str], int] = {}
        for probe in self._active.values():
            key = (probe.region_id, probe.storm_id)
            active_per_storm[key] = active_per_storm.get(key, 0) + 1
        for region_id, storms in storms_by_engine.items():
            rain_gauges = [
                item for item in observations_by_engine.get(region_id, [])
                if item.station_id
                and getattr(item, "rain_capable", False)
                and observed_at - float(item.timestamp) <= {
                    "netatmo": 10 * 60,
                    "synoptic": 20 * 60,
                    "metar": 35 * 60,
                }.get(str(getattr(item, "source", "")), 15 * 60)
            ]
            source = (
                radar_sources_by_engine.get(region_id, {}).get("source")
            )
            for storm in storms:
                if len(self._active) >= MAX_ACTIVE_PROBES:
                    return samples
                if not _storm_is_fresh(storm, observed_at):
                    continue
                storm_key = (str(region_id), str(storm.storm_id))
                remaining_for_storm = max(
                    0,
                    MAX_PROBES_PER_STORM
                    - active_per_storm.get(storm_key, 0),
                )
                if remaining_for_storm == 0:
                    continue
                if (
                    getattr(storm, "tracking_status", None) != "bevestigd"
                    or int(getattr(storm, "motion_sample_count", 0)) < 2
                ):
                    continue
                candidates = []
                for station in rain_gauges:
                    pair = (str(storm.storm_id), str(station.station_id))
                    if pair in active_pairs:
                        continue
                    motion = _exploratory_motion(
                        storm,
                        float(station.lat), float(station.lon)
                    )
                    minutes = motion.get("closest_pass_minutes")
                    passage = motion.get("passage_classification")
                    distance = motion.get("footprint_pass_distance_km")
                    approach = motion.get("approach_speed_kmh")
                    if (
                        minutes is None
                        or not 5.0 <= float(minutes) <= 90.0
                        or passage not in {"raak", "rand"}
                        or distance is None
                        or float(distance) > MAX_PASS_DISTANCE_KM
                        or approach is None
                        or float(approach) <= 1.0
                    ):
                        continue
                    candidates.append((
                        float(minutes), float(distance), station, motion
                    ))
                selected = []
                for minutes, distance, station, motion in sorted(
                    candidates,
                    key=lambda item: (
                        item[0],
                        item[1],
                        str(item[2].station_id),
                    ),
                ):
                    if len(selected) >= remaining_for_storm:
                        break
                    if any(
                        _distance_km(
                            float(station.lat), float(station.lon),
                            float(other.lat), float(other.lon),
                        ) < MIN_STATION_SEPARATION_KM
                        for other in selected
                    ):
                        continue
                    created_minute = int(observed_at // 60)
                    probe_id = (
                        f"{region_id}:{storm.storm_id}:"
                        f"{station.station_id}:{created_minute}"
                    )
                    probe = VirtualProbe(
                        probe_id=probe_id,
                        storm_id=str(storm.storm_id),
                        region_id=str(region_id),
                        station_id=str(station.station_id),
                        latitude=round(float(station.lat), 5),
                        longitude=round(float(station.lon), 5),
                        created_at=observed_at,
                        expected_at=observed_at + minutes * 60.0,
                        eta_minutes=round(minutes, 1),
                        passage=str(motion["passage_classification"]),
                        pass_distance_km=round(distance, 1),
                        motion_confidence=str(storm.confidence),
                        radar_source=source,
                    )
                    self._active[probe_id] = probe
                    active_per_storm[storm_key] = (
                        active_per_storm.get(storm_key, 0) + 1
                    )
                    active_pairs.add((probe.storm_id, probe.station_id))
                    selected.append(station)
                    samples.append(self._forecast_sample(probe))
        return samples

    @staticmethod
    def _forecast_sample(probe: VirtualProbe) -> dict:
        snapshot = {"schema_version": 1, "probe": asdict(probe)}
        return {
            "sample_key": f"virtual_probe_forecast:{probe.probe_id}",
            "sample_type": "virtual_probe_forecast",
            "target_id": (
                f"{_probe_provider(probe.station_id)}:{probe.station_id}"
            ),
            "nominal_minute": int(probe.created_at // 60),
            "observed_at": probe.created_at,
            "latitude": probe.latitude,
            "longitude": probe.longitude,
            "own_status": "voorspeld",
            "own_distance_km": probe.pass_distance_km,
            "own_eta_minutes": probe.eta_minutes,
            "own_passage": probe.passage,
            "own_confidence": probe.motion_confidence,
            "own_forecast_available": True,
            "warning_stage": None,
            "snapshot": snapshot,
        }

    @staticmethod
    def _outcome_sample(
        probe: VirtualProbe,
        *,
        observed_at: float,
        outcome: str,
        rain_live: float | None,
        rain_hour: float | None,
        timing_error_minutes: float,
    ) -> dict:
        return {
            "sample_key": f"virtual_probe_outcome:{probe.probe_id}",
            "sample_type": "virtual_probe_outcome",
            "target_id": (
                f"{_probe_provider(probe.station_id)}:{probe.station_id}"
            ),
            "nominal_minute": int(observed_at // 60),
            "observed_at": observed_at,
            "latitude": probe.latitude,
            "longitude": probe.longitude,
            "buienradar_average_mm_h": None,
            "buienradar_total_mm": None,
            "own_status": outcome,
            "own_distance_km": probe.pass_distance_km,
            "own_eta_minutes": probe.eta_minutes,
            "own_passage": probe.passage,
            "own_confidence": probe.motion_confidence,
            "own_forecast_available": True,
            "warning_stage": None,
            "snapshot": {
                "schema_version": 1,
                "probe": asdict(probe),
                "outcome": outcome,
                "rain_live_mm_h": rain_live,
                "rain_60min_mm": rain_hour,
                "timing_error_minutes": timing_error_minutes,
                "verified_at": observed_at,
            },
        }

    def diagnostics(self, now: float | None = None) -> dict:
        timestamp = float(now if now is not None else time.time())
        active = sorted(self._active.values(), key=lambda item: item.expected_at)
        probes = [
            {
                **asdict(probe),
                "minutes_until_expected": round(
                    (probe.expected_at - timestamp) / 60.0, 1
                ),
                "status_text": (
                    "Wacht op regenmeting"
                    if probe.expected_at - EARLY_WINDOW_SECONDS <= timestamp
                    else "Onderweg naar controlepunt"
                ),
                "summary": (
                    f"Controleert bui {probe.storm_id} bij "
                    f"{_probe_provider_name(probe.station_id)} "
                    f"{probe.station_id} over ongeveer "
                    f"{max(0, round((probe.expected_at - timestamp) / 60))} min "
                    f"({probe.motion_confidence.lower()} vertrouwen)."
                ),
            }
            for probe in active
        ]
        return {
            "status": "actief" if active else "wacht_op_geschikte_passage",
            "status_text": (
                f"{len(active)} virtuele regenverkenner(s) actief"
                if active else "Wacht op een geschikte buipassage"
            ),
            "active_count": len(active),
            "completed_count": self._completed,
            "confirmed_count": self._hits,
            "dry_count": self._misses,
            "unverifiable_count": self._unverifiable,
            "cancelled_count": self._cancelled,
            "probes": probes,
            "recent_outcomes": list(self._recent_outcomes),
            "operational_influence": False,
            "notifications": False,
        }
