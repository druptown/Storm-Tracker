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
from .thin_source_radar_forecast import (
    MAX_SOURCE_COUNT, SourceRadarMotion,
    rain_intervals, select_primary, trace_target,
)
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
    radar_field_forecast: Mapping[str, Any] | None = None

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
            "radar_field_forecast": (
                None if self.radar_field_forecast is None
                else dict(self.radar_field_forecast)
            ),
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


def _field_target_forecast(target_id, latitude, longitude, issued_at, region_id,
                           motions, *, previous_provider, core_passages):
    """Bounded target series; core windows are details, never the zone extent."""
    candidates = []
    for motion in tuple(motions)[:MAX_SOURCE_COUNT]:
        samples = trace_target(motion, latitude, longitude, issued_at)
        candidates.append({**motion.provenance(), "samples": samples,
            "previous_product_at": None if motion.delta_seconds is None else
                motion.field.observed_at-motion.delta_seconds,
            "availability_basis": "regional_submission"})
    selected = select_primary(candidates, previous_provider)
    samples = selected["samples"]
    intervals = rain_intervals(samples)
    known = sum(s["rate_mm_h"] is not None for s in samples)
    summary = {key: selected[key] for key in (
        "primary_provider", "source_conflict", "selection_reason", "policy_version")}
    summary.update(issued_at=issued_at, region_id=region_id,
        support_state="unknown" if not known else "complete" if known == 121 else "partial",
        known_sample_count=known, total_sample_count=121,
        threshold_mm_h=.1, intervals=intervals,
        weak_positive_sample_count=sum(s["rate_mm_h"] is not None and
            0 < s["rate_mm_h"] < .1 for s in samples),
        candidates=[{**{k:v for k,v in c.items() if k != "samples"},
            "known_sample_count": sum(s["rate_mm_h"] is not None for s in c["samples"]),
            "reason_codes": sorted({s["reason"] for s in c["samples"] if s["reason"]}),
            "intervals": rain_intervals(c["samples"])} for c in candidates],
        intensity_core_windows=[{"cell_id":p.cell_id, "provider_id":p.provider_id,
            "entry_at":p.entry_at, "exit_at":p.exit_at,
            "mean_mm_h":p.predicted_intensity_mm_h, "peak_mm_h":p.peak_intensity_mm_h}
            for p in core_passages[:64]],
        intensity_core_overflow_count=max(0, len(core_passages)-64))
    return summary, _field_passages(summary, target_id, latitude, longitude, issued_at)


def _field_passages(summary, target_id, latitude, longitude, issued_at):
    primary = next((c for c in summary["candidates"]
        if c["provider_id"] == summary["primary_provider"]), None)
    passages = []
    for ordinal, interval in enumerate(summary["intervals"]):
        if interval["exit_at"] is not None and interval["exit_at"] < issued_at:
            continue
        # A left/support-censored start is a supported wet sample, not a
        # measured onset. Required legacy entry_at is labelled by censoring.
        entry = interval["entry_at"]
        entry_bracket = interval["entry_bracket"] or (None, None)
        exit_bracket = interval["exit_bracket"] or (None, None)
        system_id = f"field:{summary['region_id']}:{primary['provider_id']}"
        passages.append(TargetPassage(
            target_id=target_id, evaluated_at=issued_at, cell_id=system_id,
            provider_id=primary["provider_id"],
            entry_at=interval["first_supported_at"] if entry is None else entry,
            exit_at=interval["exit_at"], method="source_clock_full_field",
            predicted_intensity_mm_h=interval["mean_mm_h"],
            footprint=_point(longitude, latitude),
            fallback_reason=interval["end_status"] if interval["exit_at"] is None else None,
            horizon_clipped=interval["end_status"] == "horizon_censored",
            motion_status="supported", motion_confidence="uncalibrated",
            motion_method="source_clock_local_lk_rk2",
            entry_bracket_lower_at=entry_bracket[0], entry_bracket_upper_at=entry_bracket[1],
            exit_bracket_lower_at=exit_bracket[0], exit_bracket_upper_at=exit_bracket[1],
            left_censored=entry is None, right_censored=interval["exit_at"] is None,
            coverage_censored=interval["end_status"] == "support_censored",
            interval_ordinal=ordinal, peak_intensity_mm_h=interval["peak_mm_h"],
            quantity=primary["quantity"], unit=primary["unit"],
            field_fingerprint=primary["field_fingerprint"], passage_system_id=system_id,
            passage_status="coasting" if summary["support_state"] == "held" else "candidate",
        ))
    return tuple(passages)


def forecast_input_fingerprints(forecast, passages):
    """All compared source pairs count, including abstentions/held issues."""
    fingerprints = {p.field_fingerprint for p in passages if p.field_fingerprint}
    summary = forecast.radar_field_forecast
    for candidate in (() if summary is None else summary.get("candidates", ())):
        fingerprints.update(candidate[key] for key in (
            "field_fingerprint", "previous_fingerprint") if candidate.get(key))
    return tuple(sorted(fingerprints))


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
        source_motions: Mapping[str, tuple[SourceRadarMotion, ...]] | None = None,
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
            field_summary = None
            field_mode = source_motions is not None
            if field_mode:
                previous = self._latest.target_results.get(target_id) if self._latest else None
                previous_provider = None
                if (previous is not None and previous.radar_field_forecast is not None
                    and previous.target_latitude == target_latitude
                    and previous.target_longitude == target_longitude
                    and previous.radar_field_forecast.get("region_id") == target_region_id):
                    previous_provider = previous.radar_field_forecast.get("primary_provider")
                field_summary, passages = _field_target_forecast(
                    target_id, target_latitude, target_longitude,
                    cycle.evaluated_at, target_region_id,
                    source_motions.get(target_region_id, ()),
                    previous_provider=previous_provider,
                    core_passages=passages,
                )
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
            field_complete_dry = (field_summary is not None
                and field_summary["support_state"] == "complete"
                and not field_summary["intervals"])
            if (arrival is None and (not raining or field_mode) and held is not None
                and not field_complete_dry):
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
                    if field_mode and candidate.radar_field_forecast is None:
                        candidate = None
                    if candidate is not None and candidate.passage_system_id is not None:
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
                    if candidate is not None and candidate.radar_field_forecast is not None:
                        field_summary = dict(candidate.radar_field_forecast)
                        field_summary.update(support_state="held",
                            age_seconds=cycle.evaluated_at-field_summary["issued_at"],
                            last_attempt_at=cycle.evaluated_at)
                        target_passages.extend(_field_passages(field_summary,
                            target_id, target_latitude, target_longitude, cycle.evaluated_at))
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
                    "source_clock_full_field_arrival" if field_mode and arrival is not None else
                    "source_clock_full_field_hold" if field_mode else
                    "tracked_cell_arrival"
                    if arrival is not None
                    else "tracked_cell_arrival_handoff_hold"
                )
            elif field_mode and not field_complete_dry:
                current_state = "droog" if fused and fused.status == "operational" and rain_rate is not None else "onbekend"
                forecast_state = "onvoldoende_data"
                reason = "field_forecast_support_incomplete"
            elif (
                fused is not None
                and fused.status == "operational"
                and rain_rate is not None
            ):
                current_state = "droog"
                forecast_state = "geen_dreiging"
                reason = "field_no_rain_within_supported_horizon" if field_complete_dry else "no_tracked_arrival"
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
                radar_field_forecast=field_summary,
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
