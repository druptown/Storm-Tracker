"""Bounded in-memory cell tracking and target nowcasting."""

from __future__ import annotations

from dataclasses import dataclass, replace
import math
from types import MappingProxyType
from typing import Any, Mapping

from .thin_fusion import FusionCycle
from .thin_footprint_forecast import TargetPassage, forecast_target_passages
from .thin_provider_adapters import ThinRadarField
from .thin_passage_state import PassageEvidence, PassageStateRegistry
from .thin_observation import NormalizedObservation, ObservationKind
from .tracking_core.engine.cell_track import (
    CellDetection,
    estimate_preliminary_cell_track,
)


EARTH_RADIUS_M = 6_371_008.8
MAX_ASSOCIATION_DISTANCE_KM = 100.0
MAX_FORECAST_HOURS = 2.0
THREAT_RADIUS_KM = 20.0
MAX_REGION_HANDOFF_FORECAST_HOLD_SECONDS = 15 * 60.0


@dataclass(frozen=True, slots=True)
class StormCell:
    cell_id: str
    provider_id: str
    observed_at: float
    latitude: float
    longitude: float
    geometry: Mapping[str, Any]
    precipitation_rate_mm_h: float | None
    reflectivity_dbz: float | None
    radar_level: float | None
    velocity_east_kmh: float
    velocity_north_kmh: float
    speed_kmh: float
    heading_deg: float | None
    motion_ready: bool
    motion_status: str = "indeterminate"
    motion_confidence: str = "indeterminate"
    motion_method: str | None = None
    motion_reason_codes: tuple[str, ...] = ()
    motion_supporting_methods: tuple[str, ...] = ()
    motion_agreement_angle_deg: float | None = None
    region_id: str | None = None
    mean_precipitation_rate_mm_h: float | None = None
    radar_field: ThinRadarField | None = None
    footprint_pixels: tuple[tuple[int, int], ...] = ()
    lineage_status: str = "unknown"
    predecessor_system_ids: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "cell_id": self.cell_id,
            "provider_id": self.provider_id,
            "observed_at": self.observed_at,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "geometry": dict(self.geometry),
            "precipitation_rate_mm_h": self.precipitation_rate_mm_h,
            "reflectivity_dbz": self.reflectivity_dbz,
            "radar_level": self.radar_level,
            "velocity_east_kmh": self.velocity_east_kmh,
            "velocity_north_kmh": self.velocity_north_kmh,
            "speed_kmh": self.speed_kmh,
            "heading_deg": self.heading_deg,
            "motion_ready": self.motion_ready,
            "motion_status": self.motion_status,
            "motion_confidence": self.motion_confidence,
            "motion_method": self.motion_method,
            "motion_reason_codes": list(self.motion_reason_codes),
            "motion_supporting_methods": list(self.motion_supporting_methods),
            "motion_agreement_angle_deg": self.motion_agreement_angle_deg,
            "region_id": self.region_id,
            "mean_precipitation_rate_mm_h": self.mean_precipitation_rate_mm_h,
            "lineage_status": self.lineage_status,
            "predecessor_system_ids": list(self.predecessor_system_ids),
        }


@dataclass(frozen=True, slots=True)
class TargetNowcast:
    target_id: str
    current_state: str
    forecast_state: str
    precipitation_rate_mm_h: float | None
    eta_minutes: float | None
    confidence: float
    reason_code: str
    contributors: tuple[str, ...]
    arrival_cell_id: str | None = None
    arrival_provider_id: str | None = None
    entry_at: float | None = None
    exit_at: float | None = None
    passage_method: str | None = None
    passage_fallback_reason: str | None = None
    target_latitude: float | None = None
    target_longitude: float | None = None
    target_accuracy_km: float = 0.0
    passage_system_id: str | None = None
    passage_status: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "target_id": self.target_id,
            "current_state": self.current_state,
            "forecast_state": self.forecast_state,
            "precipitation_rate_mm_h": self.precipitation_rate_mm_h,
            "eta_minutes": self.eta_minutes,
            "confidence": self.confidence,
            "reason_code": self.reason_code,
            "contributors": list(self.contributors),
            "arrival_cell_id": self.arrival_cell_id,
            "arrival_provider_id": self.arrival_provider_id,
            "entry_at": self.entry_at,
            "exit_at": self.exit_at,
            "passage_method": self.passage_method,
            "passage_fallback_reason": self.passage_fallback_reason,
            "target_latitude": self.target_latitude,
            "target_longitude": self.target_longitude,
            "target_accuracy_km": self.target_accuracy_km,
            "passage_system_id": self.passage_system_id,
            "passage_status": self.passage_status,
        }


@dataclass(frozen=True, slots=True)
class ThinNowcast:
    target_results: Mapping[str, TargetNowcast]
    storm_cells: tuple[StormCell, ...]
    evaluated_at: float
    target_passages: tuple[TargetPassage, ...] = ()


@dataclass(frozen=True, slots=True)
class _CellFrame:
    cell_id: str
    provider_id: str
    observed_at: float
    latitude: float
    longitude: float
    x_m: float
    y_m: float


def _metric(longitude: float, latitude: float) -> tuple[float, float]:
    latitude = max(-85.0, min(85.0, latitude))
    x_value = EARTH_RADIUS_M * math.radians(longitude)
    y_value = EARTH_RADIUS_M * math.log(
        math.tan(math.pi / 4.0 + math.radians(latitude) / 2.0)
    )
    return x_value, y_value


def _distance_km(first: _CellFrame, second: _CellFrame) -> float:
    return math.hypot(first.x_m - second.x_m, first.y_m - second.y_m) / 1000.0


def _geometry_centroid(observation: NormalizedObservation) -> tuple[float, float]:
    geometry = observation.as_dict().get("geometry")
    if not isinstance(geometry, Mapping):
        return observation.latitude, observation.longitude
    coordinates = geometry.get("coordinates")
    if geometry.get("type") == "Point" and isinstance(coordinates, list) and len(coordinates) >= 2:
        return float(coordinates[1]), float(coordinates[0])

    points: list[tuple[float, float]] = []

    def _collect(value: Any) -> None:
        if (
            isinstance(value, list)
            and len(value) >= 2
            and isinstance(value[0], (int, float))
            and isinstance(value[1], (int, float))
        ):
            points.append((float(value[1]), float(value[0])))
        elif isinstance(value, list):
            for item in value:
                _collect(item)

    _collect(coordinates)
    if not points:
        return observation.latitude, observation.longitude
    return (
        sum(item[0] for item in points) / len(points),
        sum(item[1] for item in points) / len(points),
    )


def _is_spatial_weather(observation: NormalizedObservation) -> bool:
    if observation.kind in {ObservationKind.REFLECTIVITY, ObservationKind.RADAR_LEVEL}:
        return observation.target_id is None
    return (
        observation.kind is ObservationKind.RAIN_RATE
        and observation.target_id is None
        and observation.metadata.get("role") != "model_guidance"
    )


class ThinNowcastRuntime:
    def __init__(self, *, targets: Mapping[str, tuple[float, ...]]) -> None:
        self._targets = dict(targets)
        self._target_region_ids: dict[str, str] = {}
        self._previous_cells: tuple[_CellFrame, ...] = ()
        self._latest: ThinNowcast | None = None
        self._held_arrivals: dict[str, tuple[float, TargetNowcast]] = {}
        self._passage_states = PassageStateRegistry()
        self._sequence = 0

    def update_targets(
        self,
        targets: Mapping[str, tuple[float, ...]],
        *,
        target_region_ids: Mapping[str, str] | None = None,
    ) -> None:
        for target_id, (_created_at, forecast) in tuple(
            self._held_arrivals.items()
        ):
            updated = targets.get(target_id)
            if (
                updated is None
                or forecast.target_latitude is None
                or forecast.target_longitude is None
                or abs(float(updated[0]) - forecast.target_latitude) > 1e-6
                or abs(float(updated[1]) - forecast.target_longitude) > 1e-6
            ):
                self._held_arrivals.pop(target_id, None)
        self._targets = dict(targets)
        self._target_region_ids = dict(target_region_ids or {})

    def _associate(
        self,
        provider_id: str,
        explicit_id: str | None,
        current: _CellFrame,
        claimed: set[str],
    ) -> _CellFrame | None:
        if explicit_id:
            expected = f"{provider_id}:{explicit_id}"
            for previous in self._previous_cells:
                if (
                    previous.cell_id == expected and previous.cell_id not in claimed
                    and previous.observed_at < current.observed_at
                ):
                    return previous
        candidates = [
            previous
            for previous in self._previous_cells
            if previous.provider_id == provider_id
            and previous.cell_id not in claimed
            and previous.observed_at < current.observed_at
        ]
        if not candidates:
            return None
        nearest = min(candidates, key=lambda item: _distance_km(item, current))
        return nearest if _distance_km(nearest, current) <= MAX_ASSOCIATION_DISTANCE_KM else None

    def advance(
        self,
        cycle: FusionCycle,
        *,
        tracked_cells: tuple[StormCell, ...] | None = None,
    ) -> ThinNowcast:
        if self._latest is not None and cycle.evaluated_at <= self._latest.evaluated_at:
            return self._latest

        cells: list[StormCell] = [] if tracked_cells is None else list(tracked_cells)
        frames: list[_CellFrame] = []
        claimed: set[str] = set()
        cached_cells = {
            (cell.provider_id, cell.observed_at, cell.latitude, cell.longitude): cell
            for cell in (() if self._latest is None else self._latest.storm_cells)
        }
        for observation in (() if tracked_cells is not None else cycle.observations):
            if not _is_spatial_weather(observation):
                continue
            latitude, longitude = _geometry_centroid(observation)
            x_value, y_value = _metric(longitude, latitude)
            explicit = observation.metadata.get("cell_id")
            explicit_id = str(explicit) if explicit else None
            provisional_id = (
                f"{observation.provider_id}:{explicit_id}"
                if explicit_id
                else f"{observation.provider_id}:candidate:{self._sequence}:{len(frames)}"
            )
            provisional = _CellFrame(
                cell_id=provisional_id,
                provider_id=observation.provider_id,
                observed_at=observation.observed_at,
                latitude=latitude,
                longitude=longitude,
                x_m=x_value,
                y_m=y_value,
            )
            geometry = observation.as_dict().get("geometry") or _point(longitude, latitude)
            cached = cached_cells.get((
                observation.provider_id, observation.observed_at, latitude, longitude,
            ))
            if cached is not None and cached.cell_id not in claimed and cached.geometry == geometry:
                # A new processing cycle is not a new meteorological frame.
                # Retain motion and identity until this provider supplies a newer image.
                cells.append(cached)
                frames.append(replace(provisional, cell_id=cached.cell_id))
                claimed.add(cached.cell_id)
                continue
            previous = self._associate(
                observation.provider_id,
                explicit_id,
                provisional,
                claimed,
            )
            cell_id = previous.cell_id if previous is not None else provisional_id
            current = _CellFrame(
                cell_id=cell_id,
                provider_id=provisional.provider_id,
                observed_at=provisional.observed_at,
                latitude=provisional.latitude,
                longitude=provisional.longitude,
                x_m=provisional.x_m,
                y_m=provisional.y_m,
            )
            east_kmh = north_kmh = 0.0
            heading = None
            if previous is not None:
                first = CellDetection(
                    detection_id=f"{previous.cell_id}:previous",
                    valid_at=previous.observed_at,
                    analysis_grid_id="webmercator_epsg3857",
                    x_m=previous.x_m,
                    y_m=previous.y_m,
                    position_sigma_m=1000.0,
                    source=previous.provider_id,
                )
                second = CellDetection(
                    detection_id=f"{current.cell_id}:current",
                    valid_at=current.observed_at,
                    analysis_grid_id="webmercator_epsg3857",
                    x_m=current.x_m,
                    y_m=current.y_m,
                    position_sigma_m=1000.0,
                    source=current.provider_id,
                )
                track = estimate_preliminary_cell_track(cell_id, first, second)
                east_kmh = track.vx_m_s * 3.6
                north_kmh = track.vy_m_s * 3.6
                heading = track.heading_deg
                claimed.add(previous.cell_id)
            # Polygon-only observations expose no local raster flow.  Their
            # two-centroid displacement remains useful diagnostics, but is not
            # sufficient evidence for a direction line or target passage.
            motion_ready = False
            cells.append(StormCell(
                cell_id=cell_id,
                provider_id=observation.provider_id,
                observed_at=observation.observed_at,
                latitude=latitude,
                longitude=longitude,
                geometry=MappingProxyType(dict(geometry)),
                precipitation_rate_mm_h=(
                    observation.value
                    if observation.kind is ObservationKind.RAIN_RATE
                    else None
                ),
                reflectivity_dbz=(
                    observation.value
                    if observation.kind is ObservationKind.REFLECTIVITY
                    else None
                ),
                radar_level=(
                    observation.value
                    if observation.kind is ObservationKind.RADAR_LEVEL
                    else None
                ),
                velocity_east_kmh=east_kmh,
                velocity_north_kmh=north_kmh,
                speed_kmh=math.hypot(east_kmh, north_kmh),
                heading_deg=heading,
                motion_ready=motion_ready,
                motion_status="indeterminate",
                motion_confidence="indeterminate",
                motion_method=None,
                motion_reason_codes=("optical_flow_unavailable",),
            ))
            frames.append(current)

        target_results: dict[str, TargetNowcast] = {}
        target_passages: list[TargetPassage] = []
        for target_id, target_position in sorted(self._targets.items()):
            target_latitude, target_longitude = target_position[:2]
            accuracy_buffer_km = (
                max(0.0, float(target_position[2]))
                if len(target_position) > 2
                else 0.0
            )
            fused = cycle.targets.get(target_id)
            rain_rate = fused.precipitation_rate_mm_h if fused is not None else None
            has_echo = bool(
                fused is not None
                and (
                    (fused.reflectivity_dbz is not None and fused.reflectivity_dbz >= 8.0)
                    or (fused.radar_level is not None and fused.radar_level > 0.0)
                )
            )
            # Reflectivity alone is an echo, not validated near-surface rain.
            raining = bool(rain_rate is not None and rain_rate > 0.0)
            target_region_id = self._target_region_ids.get(target_id)
            eligible_cells = tuple(
                cell
                for cell in cells
                if (
                    target_region_id is None
                    or cell.region_id is None
                    or cell.region_id == target_region_id
                )
            )
            passages_list: list[TargetPassage] = []
            for cell in eligible_cells:
                if not cell.motion_ready:
                    continue
                cell_passages = forecast_target_passages(
                    cell=cell,
                    target_id=target_id,
                    target_latitude=target_latitude,
                    target_longitude=target_longitude,
                    evaluated_at=cycle.evaluated_at,
                    accuracy_buffer_km=accuracy_buffer_km,
                )
                if not cell_passages:
                    continue
                alias = (
                    cell_passages[0].passage_system_id or cell.cell_id
                )
                snapshot = self._passage_states.advance(PassageEvidence(
                    target_id=target_id,
                    passage_system_id=alias,
                    radar_valid_at=cell.observed_at,
                    evaluated_at=cycle.evaluated_at,
                    field_fingerprint=(
                        cell_passages[0].field_fingerprint
                        or f"legacy:{cell.provider_id}:{cell.observed_at}"
                    ),
                    outcome="support",
                    complete_corridor=False,
                    passage_ids=tuple(
                        f"{alias}:{item.interval_ordinal}"
                        for item in cell_passages
                    ),
                    lineage_status=cell.lineage_status,
                ))
                passages_list.extend(
                    replace(item, passage_status=snapshot.status)
                    for item in cell_passages
                )
            passages = tuple(passages_list)
            target_passages.extend(passages)
            arrival = min(
                passages,
                key=lambda item: (
                    item.entry_at,
                    float("inf") if item.exit_at is None else item.exit_at,
                    item.cell_id,
                    item.interval_ordinal,
                ),
                default=None,
            )
            held_result = None
            held = self._held_arrivals.get(target_id)
            if arrival is None and not raining and held is not None:
                held_at, candidate = held
                same_position = (
                    candidate.target_latitude is not None
                    and candidate.target_longitude is not None
                    and abs(candidate.target_latitude - target_latitude) <= 1e-6
                    and abs(candidate.target_longitude - target_longitude) <= 1e-6
                )
                still_fresh = (
                    cycle.evaluated_at - held_at
                    <= MAX_REGION_HANDOFF_FORECAST_HOLD_SECONDS
                )
                not_finished = (
                    candidate.exit_at is None
                    or cycle.evaluated_at <= candidate.exit_at
                )
                if (
                    same_position
                    and still_fresh
                    and not_finished
                ):
                    if candidate.passage_system_id is not None:
                        snapshot = self._passage_states.advance(PassageEvidence(
                            target_id=target_id,
                            passage_system_id=candidate.passage_system_id,
                            radar_valid_at=cycle.evaluated_at,
                            evaluated_at=cycle.evaluated_at,
                            field_fingerprint="unusable-current-cycle",
                            outcome="unusable",
                            complete_corridor=False,
                            passage_ids=(),
                            lineage_status="unknown",
                        ))
                        candidate = replace(
                            candidate, passage_status=snapshot.status,
                        )
                        self._held_arrivals[target_id] = (held_at, candidate)
                    held_result = candidate
                else:
                    self._held_arrivals.pop(target_id, None)
            eta_minutes = 0.0 if raining else (
                round(
                    max(
                        0.0,
                        (
                            arrival.entry_at
                            if arrival is not None
                            else held_result.entry_at
                        ) - cycle.evaluated_at,
                    ) / 60.0,
                    1,
                )
                if arrival is not None or held_result is not None
                else None
            )
            if raining:
                current_state = "regen"
                forecast_state = "actuele_regen"
                reason = "current_precipitation"
            elif eta_minutes is not None:
                current_state = "droog" if fused and fused.status == "operational" else "onbekend"
                forecast_state = "regen_op_komst"
                reason = (
                    "tracked_cell_arrival"
                    if arrival is not None
                    else "tracked_cell_arrival_handoff_hold"
                )
            elif (
                fused is not None
                and fused.status == "operational"
                and rain_rate is not None
            ):
                current_state = "droog"
                forecast_state = "geen_dreiging"
                reason = "no_tracked_arrival"
            else:
                current_state = "onbekend"
                forecast_state = "onvoldoende_data"
                reason = "insufficient_observations"
            source_arrival = arrival if arrival is not None else held_result
            result = TargetNowcast(
                target_id=target_id,
                current_state=current_state,
                forecast_state=forecast_state,
                precipitation_rate_mm_h=rain_rate,
                eta_minutes=eta_minutes,
                confidence=(fused.confidence if fused is not None else 0.0),
                reason_code=reason,
                contributors=(fused.contributors if fused is not None else ()),
                arrival_cell_id=(
                    None
                    if source_arrival is None
                    else (
                        arrival.cell_id
                        if arrival is not None
                        else held_result.arrival_cell_id
                    )
                ),
                arrival_provider_id=(
                    None
                    if source_arrival is None
                    else (
                        arrival.provider_id
                        if arrival is not None
                        else held_result.arrival_provider_id
                    )
                ),
                entry_at=None if source_arrival is None else source_arrival.entry_at,
                exit_at=None if source_arrival is None else source_arrival.exit_at,
                passage_method=(
                    None
                    if source_arrival is None
                    else (
                        arrival.method
                        if arrival is not None
                        else held_result.passage_method
                    )
                ),
                passage_fallback_reason=(
                    None
                    if source_arrival is None
                    else (
                        arrival.fallback_reason
                        if arrival is not None
                        else held_result.passage_fallback_reason
                    )
                ),
                target_latitude=target_latitude,
                target_longitude=target_longitude,
                target_accuracy_km=accuracy_buffer_km,
                passage_system_id=(
                    None
                    if source_arrival is None
                    else (
                        arrival.passage_system_id
                        if arrival is not None
                        else held_result.passage_system_id
                    )
                ),
                passage_status=(
                    None
                    if source_arrival is None
                    else (
                        arrival.passage_status
                        if arrival is not None
                        else held_result.passage_status
                    )
                ),
            )
            target_results[target_id] = result
            if arrival is not None:
                self._held_arrivals[target_id] = (cycle.evaluated_at, result)
            elif held_result is None and raining:
                self._held_arrivals.pop(target_id, None)

        self._sequence += 1
        self._previous_cells = tuple(frames)
        self._latest = ThinNowcast(
            target_results=MappingProxyType(target_results),
            storm_cells=tuple(cells),
            evaluated_at=cycle.evaluated_at,
            target_passages=tuple(sorted(
                target_passages,
                key=lambda item: (item.target_id, item.entry_at, item.cell_id),
            )),
        )
        return self._latest

    @staticmethod
    def _arrival(
        target_latitude: float,
        target_longitude: float,
        cells: list[StormCell],
    ) -> tuple[float, StormCell] | None:
        best: tuple[float, StormCell] | None = None
        for cell in cells:
            speed_squared = cell.velocity_east_kmh ** 2 + cell.velocity_north_kmh ** 2
            if speed_squared < 0.25:
                continue
            target_x, target_y = _metric(target_longitude, target_latitude)
            cell_x, cell_y = _metric(cell.longitude, cell.latitude)
            delta_east = (target_x - cell_x) / 1000.0
            delta_north = (target_y - cell_y) / 1000.0
            hours = (
                delta_east * cell.velocity_east_kmh
                + delta_north * cell.velocity_north_kmh
            ) / speed_squared
            if hours < 0.0 or hours > MAX_FORECAST_HOURS:
                continue
            miss_east = delta_east - cell.velocity_east_kmh * hours
            miss_north = delta_north - cell.velocity_north_kmh * hours
            if math.hypot(miss_east, miss_north) > THREAT_RADIUS_KM:
                continue
            candidate = (round(hours * 60.0, 1), cell)
            if best is None or (candidate[0], candidate[1].cell_id) < (best[0], best[1].cell_id):
                best = candidate
        return best


def _point(longitude: float, latitude: float) -> dict[str, Any]:
    return {"type": "Point", "coordinates": [longitude, latitude]}


__all__ = ["StormCell", "TargetNowcast", "ThinNowcast", "ThinNowcastRuntime"]
