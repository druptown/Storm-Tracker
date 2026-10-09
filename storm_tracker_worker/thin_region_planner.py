"""Stateful geographic regions for the thin operational worker path."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from types import MappingProxyType
from typing import Mapping

from .desired_state import DesiredState, Target


DEFAULT_HOME_RADIUS_KM = 30.0
DEFAULT_HOME_ENTER_RADIUS_KM = 25.0
DEFAULT_HOME_EXIT_RADIUS_KM = 35.0
DEFAULT_EXTERNAL_SHARING_DISTANCE_KM = 30.0
DEFAULT_RECENTER_DISTANCE_KM = 15.0
DEFAULT_UNAVAILABLE_GRACE_SECONDS = 15.0 * 60.0
DEFAULT_WARNING_BUFFER_KM = 10.0
DEFAULT_MAX_STORM_SPEED_KMH = 80.0
DEFAULT_LOOKAHEAD_MINUTES = 45.0
DEFAULT_RADAR_ANALYSIS_RADIUS_KM = 250.0


def _haversine_km(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    radius = 6371.0088
    latitude_a_rad = math.radians(latitude_a)
    latitude_b_rad = math.radians(latitude_b)
    latitude_delta = latitude_b_rad - latitude_a_rad
    longitude_delta = math.radians(longitude_b - longitude_a)
    value = math.sin(latitude_delta / 2.0) ** 2 + (
        math.cos(latitude_a_rad)
        * math.cos(latitude_b_rad)
        * math.sin(longitude_delta / 2.0) ** 2
    )
    return radius * 2.0 * math.atan2(math.sqrt(value), math.sqrt(1.0 - value))


def _spherical_center(targets: tuple[Target, ...]) -> tuple[float, float]:
    x = y = z = 0.0
    for target in targets:
        latitude = math.radians(target.latitude)
        longitude = math.radians(target.longitude)
        x += math.cos(latitude) * math.cos(longitude)
        y += math.cos(latitude) * math.sin(longitude)
        z += math.sin(latitude)
    count = float(len(targets))
    x /= count
    y /= count
    z /= count
    longitude = math.atan2(y, x)
    latitude = math.atan2(z, math.hypot(x, y))
    return math.degrees(latitude), math.degrees(longitude)


@dataclass(frozen=True, slots=True)
class ThinAcquisitionRegion:
    region_id: str
    kind: str
    center_latitude: float
    center_longitude: float
    observation_radius_km: float
    targets: tuple[Target, ...]
    stale_target_ids: tuple[str, ...]
    radar_analysis_radius_km: float | None = None

    @property
    def target_ids(self) -> tuple[str, ...]:
        return tuple(target.target_id for target in self.targets)


@dataclass(frozen=True, slots=True)
class ThinRegionPlan:
    regions: tuple[ThinAcquisitionRegion, ...]
    targets: tuple[Target, ...]
    target_region_ids: Mapping[str, str]
    location_statuses: Mapping[str, str]

    def region_id_for(self, target_id: str) -> str | None:
        return self.target_region_ids.get(target_id)

    def location_status_for(self, target_id: str) -> str | None:
        return self.location_statuses.get(target_id)

    def region_id_for_position(
        self,
        latitude: float,
        longitude: float,
    ) -> str | None:
        nearest: tuple[float, str] | None = None
        for region in self.regions:
            distance = _haversine_km(
                region.center_latitude,
                region.center_longitude,
                latitude,
                longitude,
            )
            if distance > region.observation_radius_km:
                continue
            candidate = (distance, region.region_id)
            if nearest is None or candidate < nearest:
                nearest = candidate
        return None if nearest is None else nearest[1]

    def accepts_position(self, latitude: float, longitude: float) -> bool:
        return self.region_id_for_position(latitude, longitude) is not None

    def region(self, region_id: str) -> ThinAcquisitionRegion:
        for region in self.regions:
            if region.region_id == region_id:
                return region
        raise KeyError(region_id)


class ThinRegionPlanner:
    """Keep home and roaming regions stable across desired-state updates."""

    def __init__(
        self,
        *,
        home_radius_km: float = DEFAULT_HOME_RADIUS_KM,
        home_enter_radius_km: float = DEFAULT_HOME_ENTER_RADIUS_KM,
        home_exit_radius_km: float = DEFAULT_HOME_EXIT_RADIUS_KM,
        external_sharing_distance_km: float = DEFAULT_EXTERNAL_SHARING_DISTANCE_KM,
        recenter_distance_km: float = DEFAULT_RECENTER_DISTANCE_KM,
        unavailable_grace_seconds: float = DEFAULT_UNAVAILABLE_GRACE_SECONDS,
        warning_buffer_km: float = DEFAULT_WARNING_BUFFER_KM,
        maximum_storm_speed_kmh: float = DEFAULT_MAX_STORM_SPEED_KMH,
        lookahead_minutes: float = DEFAULT_LOOKAHEAD_MINUTES,
        radar_analysis_radius_km: float = DEFAULT_RADAR_ANALYSIS_RADIUS_KM,
    ) -> None:
        values = {
            "home_radius_km": home_radius_km,
            "home_enter_radius_km": home_enter_radius_km,
            "home_exit_radius_km": home_exit_radius_km,
            "external_sharing_distance_km": external_sharing_distance_km,
            "recenter_distance_km": recenter_distance_km,
            "unavailable_grace_seconds": unavailable_grace_seconds,
            "warning_buffer_km": warning_buffer_km,
            "maximum_storm_speed_kmh": maximum_storm_speed_kmh,
            "lookahead_minutes": lookahead_minutes,
            "radar_analysis_radius_km": radar_analysis_radius_km,
        }
        if any(not math.isfinite(float(value)) or float(value) < 0.0 for value in values.values()):
            raise ValueError("region_planner_settings_must_be_finite_non_negative")
        if not home_enter_radius_km < home_radius_km < home_exit_radius_km:
            raise ValueError("home_hysteresis_thresholds_invalid")
        if external_sharing_distance_km <= 0.0 or recenter_distance_km <= 0.0:
            raise ValueError("region_distance_thresholds_must_be_positive")
        self._home_radius_km = float(home_radius_km)
        self._home_enter_radius_km = float(home_enter_radius_km)
        self._home_exit_radius_km = float(home_exit_radius_km)
        self._external_sharing_distance_km = float(external_sharing_distance_km)
        self._recenter_distance_km = float(recenter_distance_km)
        self._unavailable_grace_seconds = float(unavailable_grace_seconds)
        self._warning_radius_km = float(warning_buffer_km) + (
            float(maximum_storm_speed_kmh) * float(lookahead_minutes) / 60.0
        )
        self._radar_analysis_radius_km = float(radar_analysis_radius_km)
        self._last_available: dict[str, tuple[Target, float]] = {}
        self._previous_assignments: dict[str, str] = {}
        self._external_centers: dict[str, tuple[float, float]] = {}

    def _active_targets(
        self,
        state: DesiredState,
        *,
        now: float,
    ) -> tuple[tuple[Target, ...], dict[str, str]]:
        present_ids = {target.target_id for target in state.targets}
        for target_id in tuple(self._last_available):
            if target_id not in present_ids:
                self._last_available.pop(target_id, None)
                self._previous_assignments.pop(target_id, None)

        active: list[Target] = []
        statuses: dict[str, str] = {}
        dynamic_kinds = {"person", "device_tracker", "test"}
        for target in sorted(state.targets, key=lambda item: item.target_id):
            if target.available:
                self._last_available[target.target_id] = (target, now)
                active.append(target)
                statuses[target.target_id] = "current"
                continue
            cached = self._last_available.get(target.target_id)
            if (
                target.kind in dynamic_kinds
                and cached is not None
                and now - cached[1] <= self._unavailable_grace_seconds
            ):
                active.append(cached[0])
                statuses[target.target_id] = "stale"
            elif cached is not None and now - cached[1] > self._unavailable_grace_seconds:
                self._last_available.pop(target.target_id, None)
                self._previous_assignments.pop(target.target_id, None)
        return tuple(active), statuses

    def _belongs_to_home(self, target: Target, home: Target) -> bool:
        if target.target_id == home.target_id:
            return True
        distance = _haversine_km(
            target.latitude,
            target.longitude,
            home.latitude,
            home.longitude,
        )
        previous = self._previous_assignments.get(target.target_id)
        if previous == "region:home":
            return distance <= self._home_exit_radius_km
        if previous is not None:
            return distance <= self._home_enter_radius_km
        return distance <= self._home_radius_km

    def _external_clusters(self, targets: tuple[Target, ...]) -> tuple[tuple[Target, ...], ...]:
        clusters: list[list[Target]] = []
        for target in sorted(targets, key=lambda item: item.target_id):
            destination = next((
                cluster
                for cluster in clusters
                if all(
                    _haversine_km(
                        target.latitude,
                        target.longitude,
                        member.latitude,
                        member.longitude,
                    ) <= self._external_sharing_distance_km
                    for member in cluster
                )
            ), None)
            if destination is None:
                clusters.append([target])
            else:
                destination.append(target)
        return tuple(tuple(cluster) for cluster in clusters)

    @staticmethod
    def _external_region_id(targets: tuple[Target, ...]) -> str:
        encoded = json.dumps(
            [target.target_id for target in targets],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return "region:external:" + hashlib.sha256(encoded).hexdigest()[:16]

    def _region(
        self,
        *,
        region_id: str,
        kind: str,
        center: tuple[float, float],
        targets: tuple[Target, ...],
        statuses: Mapping[str, str],
    ) -> ThinAcquisitionRegion:
        target_offsets = tuple(
            _haversine_km(center[0], center[1], target.latitude, target.longitude)
            for target in targets
        )
        radius = max(
            self._warning_radius_km
            + offset
            for offset in target_offsets
        )
        radar_analysis_radius = max(
            self._radar_analysis_radius_km + offset
            for offset in target_offsets
        )
        return ThinAcquisitionRegion(
            region_id=region_id,
            kind=kind,
            center_latitude=center[0],
            center_longitude=center[1],
            observation_radius_km=radius,
            targets=targets,
            stale_target_ids=tuple(
                target.target_id
                for target in targets
                if statuses.get(target.target_id) == "stale"
            ),
            radar_analysis_radius_km=radar_analysis_radius,
        )

    def plan(self, state: DesiredState, *, now: float) -> ThinRegionPlan:
        if not isinstance(state, DesiredState):
            raise TypeError("state_must_be_desired_state")
        now_value = float(now)
        if not math.isfinite(now_value) or now_value < 0.0:
            raise ValueError("now_must_be_finite_non_negative")

        targets, statuses = self._active_targets(state, now=now_value)
        home = next((target for target in targets if target.kind == "home"), None)
        home_targets: tuple[Target, ...] = ()
        external_targets = targets
        if home is not None:
            home_targets = tuple(
                target for target in targets if self._belongs_to_home(target, home)
            )
            home_ids = {target.target_id for target in home_targets}
            external_targets = tuple(
                target for target in targets if target.target_id not in home_ids
            )

        regions: list[ThinAcquisitionRegion] = []
        assignments: dict[str, str] = {}
        if home is not None and home_targets:
            region = self._region(
                region_id="region:home",
                kind="home",
                center=(home.latitude, home.longitude),
                targets=home_targets,
                statuses=statuses,
            )
            regions.append(region)
            assignments.update({target.target_id: region.region_id for target in home_targets})

        active_external_ids: set[str] = set()
        for cluster in self._external_clusters(external_targets):
            region_id = self._external_region_id(cluster)
            desired_center = _spherical_center(cluster)
            previous_center = self._external_centers.get(region_id)
            if previous_center is not None and _haversine_km(
                previous_center[0],
                previous_center[1],
                desired_center[0],
                desired_center[1],
            ) <= self._recenter_distance_km:
                center = previous_center
            else:
                center = desired_center
            self._external_centers[region_id] = center
            active_external_ids.add(region_id)
            region = self._region(
                region_id=region_id,
                kind="external",
                center=center,
                targets=cluster,
                statuses=statuses,
            )
            regions.append(region)
            assignments.update({target.target_id: region_id for target in cluster})

        for region_id in tuple(self._external_centers):
            if region_id not in active_external_ids:
                self._external_centers.pop(region_id, None)
        self._previous_assignments = dict(assignments)
        ordered_regions = tuple(sorted(regions, key=lambda item: item.region_id))
        return ThinRegionPlan(
            regions=ordered_regions,
            targets=targets,
            target_region_ids=MappingProxyType(dict(assignments)),
            location_statuses=MappingProxyType(dict(statuses)),
        )


__all__ = [
    "ThinAcquisitionRegion",
    "ThinRegionPlan",
    "ThinRegionPlanner",
]
