"""Offline replay using the exact live footprint and validation functions."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping

from .thin_footprint_forecast import TargetPassage, forecast_target_passage
from .thin_ground_validation import ForecastValidation, validate_passages
from .thin_observation import NormalizedObservation, ObservationKind, ObservationQuality
from .tracking_core.engine.motion_consensus import (
    MotionVectorCandidate,
    select_motion_consensus,
)


def run_motion_admission_replay() -> dict[str, Any]:
    """Exercise known motion failure modes without touching live state."""

    covariance = ((4.0, 0.0), (0.0, 4.0))

    def candidate(method: str, role: str, vx: float, vy: float):
        return MotionVectorCandidate(
            method, role, vx, vy, covariance, 16, 0.8, 4, 0.8,
        )

    translation = select_motion_consensus((
        candidate("flow-translation", "optical_flow", 6.0, 2.0),
        candidate("track-translation", "object_track", 5.7, 2.1),
    ))
    intensity_only = select_motion_consensus((
        candidate("flow-intensity", "optical_flow", 0.0, 0.0),
        candidate("track-intensity", "object_track", 0.0, 0.0),
    ))
    rotation_only = select_motion_consensus((
        candidate("flow-rotation", "optical_flow", 0.0, 0.0),
        candidate("track-centroid-drift", "object_track", 5.0, 0.0),
    ))
    east = select_motion_consensus((
        candidate("flow-east", "optical_flow", 5.0, 0.0),
        candidate("track-east", "object_track", 5.1, 0.1),
    ))
    north = select_motion_consensus((
        candidate("flow-north", "optical_flow", 0.0, 4.0),
        candidate("track-north", "object_track", 0.1, 4.1),
    ))
    checks = {
        "translation_admitted": translation.status == "ready_high",
        "intensity_only_has_no_heading": (
            intensity_only.status == "stationary"
            and intensity_only.heading_deg is None
        ),
        "rotation_centroid_drift_withheld": (
            rotation_only.status == "indeterminate"
        ),
        "independent_system_directions": (
            east.heading_deg is not None
            and north.heading_deg is not None
            and abs(east.heading_deg - north.heading_deg) > 45.0
        ),
    }
    return {
        "mode": "thin_motion_admission_replay_v1",
        "operational_state_mutated": False,
        "passed": all(checks.values()),
        "checks": checks,
        "scenarios": {
            "translation": translation.status,
            "intensity_change_without_translation": intensity_only.status,
            "rotation_without_bulk_translation": rotation_only.status,
            "two_independent_systems": {
                "east": east.status,
                "north": north.status,
            },
        },
    }


@dataclass(frozen=True, slots=True)
class ReplayFrame:
    evaluated_at: float
    target_id: str
    target_latitude: float
    target_longitude: float
    target_accuracy_km: float
    cells: tuple[Any, ...]
    ground_observations: tuple[NormalizedObservation, ...]


@dataclass(frozen=True, slots=True)
class ReplayCell:
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


@dataclass(frozen=True, slots=True)
class ReplayMetric:
    metric_name: str
    metric_value: float
    sample_count: int
    dimension: Mapping[str, Any] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class RadarForecastVerification:
    cell_id: str
    actual_cell_id: str
    provider_id: str
    forecast_at: float
    actual_at: float
    lead_seconds: float
    footprint_iou: float
    centroid_error_km: float


@dataclass(frozen=True, slots=True)
class ReplayReport:
    replay_id: str
    algorithm_version: str
    passages: tuple[TargetPassage, ...]
    validations: tuple[ForecastValidation, ...]
    radar_verifications: tuple[RadarForecastVerification, ...]
    metrics: tuple[ReplayMetric, ...]


def _percentile(values: Iterable[float], fraction: float) -> float | None:
    ordered = sorted(float(item) for item in values if math.isfinite(float(item)))
    if not ordered:
        return None
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _report_id(
    frames: tuple[ReplayFrame, ...],
    algorithm_version: str,
    radar_verifications: tuple[RadarForecastVerification, ...] = (),
    radar_cells: tuple[ReplayCell, ...] = (),
    radar_parameters: tuple[Any, ...] = (),
) -> str:
    identity = {
        "algorithm_version": algorithm_version,
        "radar_parameters": radar_parameters,
        "radar_cells": [{
            "provider_id": cell.provider_id, "cell_id": cell.cell_id,
            "observed_at": cell.observed_at, "geometry": dict(cell.geometry),
            "latitude": cell.latitude, "longitude": cell.longitude,
            "velocity_east_kmh": cell.velocity_east_kmh,
            "velocity_north_kmh": cell.velocity_north_kmh,
        } for cell in sorted(radar_cells, key=lambda c: (c.provider_id, c.observed_at, c.cell_id))],
        "frames": [{
            "evaluated_at": frame.evaluated_at,
            "target_id": frame.target_id,
            "cells": sorted(
                str(getattr(cell, "cell_id", type(cell).__name__))
                for cell in frame.cells
            ),
            "ground": sorted(
                (
                    item.provider_id,
                    str(item.metadata.get("station_id", "")),
                    item.observed_at,
                )
                for item in frame.ground_observations
            ),
        } for frame in frames],
        "radar_verifications": [{
            "cell_id": item.cell_id,
            "actual_cell_id": item.actual_cell_id,
            "provider_id": item.provider_id,
            "forecast_at": item.forecast_at,
            "actual_at": item.actual_at,
            "lead_seconds": item.lead_seconds,
        } for item in radar_verifications],
    }
    return hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")).hexdigest()


def _radar_shape(cell: ReplayCell) -> Any | None:
    # GEOS is only loaded by the offline evaluator, never by worker startup.
    from shapely.geometry import shape
    from shapely.ops import unary_union
    from shapely.errors import GEOSException

    try:
        geometry = shape(dict(cell.geometry))
        if geometry.geom_type == "MultiPolygon":
            # Raster runs can share edges. Dissolve valid components without
            # repairing self-crossing source polygons or filling real holes.
            if not all(part.is_valid for part in geometry.geoms):
                return None
            geometry = unary_union(geometry.geoms)
        if (
            geometry.geom_type not in {"Polygon", "MultiPolygon"}
            or not geometry.is_valid or geometry.is_empty
            or not math.isfinite(geometry.area) or geometry.area <= 0.0
            or not all(math.isfinite(x) for x in geometry.bounds)
        ):
            return None
        return geometry
    except (GEOSException, TypeError, ValueError, KeyError, IndexError):
        return None


def _shape_iou(first: Any, second: Any) -> float:
    intersection = first.intersection(second).area
    union = first.area + second.area - intersection
    return max(0.0, min(1.0, intersection / union)) if union > 0.0 else 0.0


def _distance_km(
    first_latitude: float,
    first_longitude: float,
    second_latitude: float,
    second_longitude: float,
) -> float:
    first_lat = math.radians(first_latitude)
    second_lat = math.radians(second_latitude)
    latitude_delta = second_lat - first_lat
    longitude_delta = math.radians(second_longitude - first_longitude)
    haversine = (
        math.sin(latitude_delta / 2.0) ** 2
        + math.cos(first_lat) * math.cos(second_lat)
        * math.sin(longitude_delta / 2.0) ** 2
    )
    return 6371.0088 * 2.0 * math.asin(min(1.0, math.sqrt(haversine)))


def _evaluate_radar_forecasts(
    cells: Iterable[ReplayCell],
    *,
    lead_seconds: Iterable[float],
    match_tolerance_seconds: float,
    maximum_match_distance_km: float,
) -> tuple[tuple[RadarForecastVerification, ...], tuple[ReplayMetric, ...]]:
    """Score archived cell advection; matching alone cannot prove storm identity."""
    from collections import Counter
    from shapely.affinity import translate
    from shapely.ops import unary_union

    ordered = tuple(sorted(cells, key=lambda c: (c.provider_id, c.observed_at, c.cell_id)))
    leads = tuple(sorted(set(float(value) for value in lead_seconds)))
    tolerance = float(match_tolerance_seconds)
    maximum_distance = float(maximum_match_distance_km)
    if (
        any(not math.isfinite(value) or value <= 0 for value in leads)
        or not math.isfinite(tolerance) or tolerance < 0
        or not math.isfinite(maximum_distance) or maximum_distance < 0
    ):
        raise ValueError("radar_replay_parameters_invalid")
    frames: dict[str, dict[float, list[ReplayCell]]] = {}
    shapes = {}
    for cell in ordered:
        frames.setdefault(cell.provider_id, {}).setdefault(cell.observed_at, []).append(cell)
        shapes[(cell.provider_id, cell.observed_at, cell.cell_id)] = _radar_shape(cell)

    def geometry(cell: ReplayCell) -> Any:
        return shapes[(cell.provider_id, cell.observed_at, cell.cell_id)]

    results: list[RadarForecastVerification] = []
    metrics: list[ReplayMetric] = []
    for provider, provider_frames in sorted(frames.items()):
        times = sorted(provider_frames)
        for lead in leads:
            counts = Counter({
                name: 0 for name in (
                    "frame_pair_count", "missing_future_frame_count",
                    "forecast_cell_count", "actual_cell_count",
                    "invalid_forecast_count", "invalid_actual_count",
                    "matched_count", "zero_overlap_match_count",
                    "unmatched_forecast_count", "unmatched_actual_count",
                    "unscored_frame_pair_count",
                )
            })
            frame_scores, baseline_scores, pairs = [], [], []
            for anchor_time in times:
                truth_times = [
                    t for t in times
                    if t > anchor_time and abs(t - anchor_time - lead) <= tolerance
                ]
                if not truth_times:
                    counts["missing_future_frame_count"] += 1
                    continue
                truth_time = min(truth_times, key=lambda t: (abs(t-anchor_time-lead), t))
                anchors, actuals = provider_frames[anchor_time], provider_frames[truth_time]
                counts["frame_pair_count"] += 1
                counts["forecast_cell_count"] += len(anchors)
                counts["actual_cell_count"] += len(actuals)
                valid_actuals = [c for c in actuals if geometry(c) is not None]
                bad_actuals = len(actuals) - len(valid_actuals)
                counts["invalid_actual_count"] += bad_actuals
                projected, originals = {}, []
                for anchor in anchors:
                    source_shape = geometry(anchor)
                    scale = 111.320 * math.cos(math.radians(anchor.latitude))
                    if (
                        source_shape is None or abs(scale) < 1e-9
                        or not all(math.isfinite(value) for value in (
                            anchor.latitude, anchor.longitude,
                            anchor.velocity_east_kmh, anchor.velocity_north_kmh,
                        ))
                    ):
                        counts["invalid_forecast_count"] += 1
                        continue
                    hours = (truth_time - anchor_time) / 3600.0
                    dx = anchor.velocity_east_kmh * hours / scale
                    dy = anchor.velocity_north_kmh * hours / 110.574
                    projected[anchor.cell_id] = (
                        translate(source_shape, xoff=dx, yoff=dy),
                        anchor.latitude + dy, anchor.longitude + dx,
                    )
                    originals.append(source_shape)
                # Full-frame score includes all cells, including unmatched ones.
                # Invalid data makes the frame unscorable, never artificially dry.
                if len(projected) != len(anchors) or bad_actuals:
                    counts["unscored_frame_pair_count"] += 1
                else:
                    actual_union = unary_union([geometry(c) for c in valid_actuals])
                    prediction_union = unary_union([item[0] for item in projected.values()])
                    frame_scores.append(_shape_iou(prediction_union, actual_union))
                    baseline_scores.append(_shape_iou(unary_union(originals), actual_union))
                candidates = []
                for anchor_id, (predicted, latitude, longitude) in projected.items():
                    for actual in valid_actuals:
                        distance = _distance_km(latitude, longitude, actual.latitude, actual.longitude)
                        if distance <= maximum_distance:
                            overlap = _shape_iou(predicted, geometry(actual))
                            candidates.append((-overlap, distance, anchor_id, actual.cell_id))
                used_anchors, used_actuals = set(), set()
                for negative_iou, distance, anchor_id, actual_id in sorted(candidates):
                    if anchor_id in used_anchors or actual_id in used_actuals:
                        continue
                    used_anchors.add(anchor_id)
                    used_actuals.add(actual_id)
                    counts["matched_count"] += 1
                    counts["zero_overlap_match_count"] += int(negative_iou == 0.0)
                    pairs.append(RadarForecastVerification(
                        cell_id=anchor_id, actual_cell_id=actual_id,
                        provider_id=provider, forecast_at=anchor_time,
                        actual_at=truth_time, lead_seconds=lead,
                        footprint_iou=-negative_iou, centroid_error_km=distance,
                    ))
                counts["unmatched_forecast_count"] += len(projected) - len(used_anchors)
                counts["unmatched_actual_count"] += len(valid_actuals) - len(used_actuals)

            dimension = MappingProxyType({"provider_id": provider, "lead_seconds": lead})
            metrics.extend(ReplayMetric("radar_" + name, float(count), count, dimension)
                           for name, count in sorted(counts.items()))
            for name, values in (
                ("frame_iou_mean", frame_scores),
                ("persistence_frame_iou_mean", baseline_scores),
                ("frame_iou_gain_mean", [a-b for a,b in zip(frame_scores, baseline_scores)]),
                ("footprint_iou_mean", [item.footprint_iou for item in pairs]),
            ):
                if values:
                    metrics.append(ReplayMetric("radar_" + name, sum(values)/len(values),
                                                len(values), dimension))
            p90 = _percentile((item.centroid_error_km for item in pairs), 0.9)
            if p90 is not None:
                metrics.append(ReplayMetric("radar_centroid_error_p90_km", p90, len(pairs), dimension))
            results.extend(pairs)
    return tuple(sorted(results, key=lambda r: (
        r.forecast_at, r.provider_id, r.cell_id, r.lead_seconds,
    ))), tuple(metrics)


def verify_radar_forecasts(
    cells: Iterable[ReplayCell],
    *,
    lead_seconds: Iterable[float] = (300.0, 900.0, 1800.0, 2700.0, 3600.0),
    match_tolerance_seconds: float = 150.0,
    maximum_match_distance_km: float = 75.0,
) -> tuple[RadarForecastVerification, ...]:
    return _evaluate_radar_forecasts(
        cells, lead_seconds=lead_seconds,
        match_tolerance_seconds=match_tolerance_seconds,
        maximum_match_distance_km=maximum_match_distance_km,
    )[0]


def run_replay(
    frames: Iterable[ReplayFrame],
    *,
    algorithm_version: str,
    radar_cells: Iterable[ReplayCell] = (),
    radar_lead_seconds: Iterable[float] = (300.0, 900.0, 1800.0, 2700.0, 3600.0),
    radar_match_tolerance_seconds: float = 150.0,
    radar_maximum_match_distance_km: float = 75.0,
) -> ReplayReport:
    ordered = tuple(sorted(
        frames, key=lambda item: (item.evaluated_at, item.target_id),
    ))
    passages: list[TargetPassage] = []
    validations: list[ForecastValidation] = []
    failed_frames = 0
    for frame in ordered:
        try:
            frame_passages = tuple(
                passage
                for cell in sorted(
                    frame.cells,
                    key=lambda item: str(getattr(item, "cell_id", "")),
                )
                if (passage := forecast_target_passage(
                    cell=cell,
                    target_id=frame.target_id,
                    target_latitude=frame.target_latitude,
                    target_longitude=frame.target_longitude,
                    evaluated_at=frame.evaluated_at,
                    accuracy_buffer_km=frame.target_accuracy_km,
                )) is not None
            )
            validation_time = max(
                [frame.evaluated_at]
                + [item.observed_at for item in frame.ground_observations]
            )
            frame_validations = validate_passages(
                frame_passages,
                frame.ground_observations,
                evaluated_at=validation_time,
            )
        except Exception:
            failed_frames += 1
            continue
        passages.extend(frame_passages)
        validations.extend(frame_validations)

    unique_passages = tuple(sorted(
        {
            (item.target_id, item.evaluated_at, item.cell_id): item
            for item in passages
        }.values(),
        key=lambda item: (item.target_id, item.evaluated_at, item.cell_id),
    ))
    unique_validations = tuple(sorted(
        {item.validation_key: item for item in validations}.values(),
        key=lambda item: item.validation_key,
    ))
    radar_cells = tuple(radar_cells)
    requested_radar_leads = tuple(sorted(set(radar_lead_seconds)))
    radar_verifications, radar_metrics = ((), ())
    if radar_cells:
        radar_verifications, radar_metrics = _evaluate_radar_forecasts(
            radar_cells, lead_seconds=requested_radar_leads,
            match_tolerance_seconds=radar_match_tolerance_seconds,
            maximum_match_distance_km=radar_maximum_match_distance_km,
        )
    outcome_counts: dict[str, int] = {}
    for validation in unique_validations:
        outcome_counts[validation.outcome] = outcome_counts.get(validation.outcome, 0) + 1
    metrics = [
        ReplayMetric("frames_total", float(len(ordered)), len(ordered)),
        ReplayMetric("failed_frames", float(failed_frames), failed_frames),
        ReplayMetric("passage_count", float(len(unique_passages)), len(unique_passages)),
    ]
    for label in ("hit", "miss", "false_alarm", "early", "late"):
        count = outcome_counts.get(label, 0)
        metrics.append(ReplayMetric(f"{label}_count", float(count), count))
    for name, values in (
        (
            "entry_error",
            [
                abs(item.entry_error_seconds)
                for item in unique_validations
                if item.entry_error_seconds is not None
            ],
        ),
        (
            "exit_error",
            [
                abs(item.exit_error_seconds)
                for item in unique_validations
                if item.exit_error_seconds is not None
            ],
        ),
    ):
        for suffix, fraction in (("p50_seconds", 0.5), ("p90_seconds", 0.9)):
            value = _percentile(values, fraction)
            if value is not None:
                metrics.append(ReplayMetric(
                    f"{name}_{suffix}", value, len(values),
                ))
    radar_leads = sorted({item.lead_seconds for item in radar_verifications})
    for lead in radar_leads:
        samples = tuple(item for item in radar_verifications if item.lead_seconds == lead)
        dimension = MappingProxyType({"lead_seconds": lead})
        metrics.append(ReplayMetric(
            "radar_matched_count", float(len(samples)), len(samples), dimension,
        ))
        metrics.append(ReplayMetric(
            "radar_footprint_iou_mean",
            sum(item.footprint_iou for item in samples) / len(samples),
            len(samples), dimension,
        ))
        centroid_errors = [item.centroid_error_km for item in samples]
        p90 = _percentile(centroid_errors, 0.9)
        if p90 is not None:
            metrics.append(ReplayMetric(
                "radar_centroid_error_p90_km", p90, len(samples), dimension,
            ))
    metrics.extend(radar_metrics)
    metrics.sort(key=lambda item: (item.metric_name, json.dumps(dict(item.dimension), sort_keys=True)))
    return ReplayReport(
        replay_id=_report_id(ordered, algorithm_version, radar_verifications, radar_cells,
                             (requested_radar_leads,
                              radar_match_tolerance_seconds, radar_maximum_match_distance_km)),
        algorithm_version=algorithm_version,
        passages=unique_passages,
        validations=unique_validations,
        radar_verifications=radar_verifications,
        metrics=tuple(metrics),
    )


_SELECT_TARGET_POSITIONS = """
SELECT EXTRACT(EPOCH FROM observed_at),ST_Y(position),ST_X(position),accuracy_km
FROM stv3_target_position
WHERE target_id=%s AND observed_at>=to_timestamp(%s)
  AND observed_at<to_timestamp(%s)
ORDER BY observed_at
"""

_SELECT_CELLS = """
SELECT cell.cell_id,cell.provider_id,EXTRACT(EPOCH FROM cell.observed_at),
       cell.precipitation_rate_mm_h,cell.reflectivity_dbz,cell.radar_level,
       ST_AsGeoJSON(cell.geometry),
       track.velocity_east_kmh,track.velocity_north_kmh,
       track.speed_kmh,track.heading_deg,
       ST_Y(track.position),ST_X(track.position)
FROM stv3_storm_cell AS cell
JOIN stv3_track_point AS track
  ON track.cell_id=cell.cell_id AND track.observed_at=cell.observed_at
WHERE cell.observed_at>=to_timestamp(%s) AND cell.observed_at<to_timestamp(%s)
  AND (%s::text IS NULL OR cell.provider_id=%s)
ORDER BY cell.observed_at,cell.cell_id
"""

_SELECT_GROUND = """
SELECT measurement.provider_id,measurement.product_id,
       measurement.value,measurement.unit,
       EXTRACT(EPOCH FROM measurement.observed_at),
       EXTRACT(EPOCH FROM measurement.received_at),
       ST_Y(station.position),ST_X(station.position),
       measurement.quality,measurement.metadata,
       ST_AsGeoJSON(station.position)
FROM stv3_station_measurement AS measurement
JOIN stv3_ground_station AS station
  ON station.provider_id=measurement.provider_id
 AND station.station_id=measurement.station_id
JOIN LATERAL (
    SELECT candidate.position
    FROM stv3_target_position AS candidate
    WHERE candidate.target_id=%s
      AND candidate.observed_at>=to_timestamp(%s)
      AND candidate.observed_at<to_timestamp(%s)
    ORDER BY ABS(EXTRACT(EPOCH FROM (
        candidate.observed_at-measurement.observed_at
    )))
    LIMIT 1
) AS target_position ON true
WHERE measurement.measurement_type='rain_rate'
  AND measurement.observed_at>=to_timestamp(%s)
  AND measurement.observed_at<to_timestamp(%s)
  AND ST_DWithin(
      station.position::geography,
      target_position.position::geography,
      %s
  )
ORDER BY measurement.observed_at,measurement.provider_id,
         measurement.station_id
"""

_INSERT_REPLAY_RUN = """
INSERT INTO stv3_replay_run(
    replay_id,started_at,completed_at,algorithm_version,parameters,status
) VALUES(%s,clock_timestamp(),clock_timestamp(),%s,%s::jsonb,'completed')
ON CONFLICT(replay_id) DO UPDATE SET
    completed_at=EXCLUDED.completed_at,parameters=EXCLUDED.parameters,
    status=EXCLUDED.status
"""

_INSERT_REPLAY_METRIC = """
INSERT INTO stv3_replay_metric(
    replay_id,metric_name,dimension,metric_value,sample_count
) VALUES(%s,%s,%s::jsonb,%s,%s)
ON CONFLICT(replay_id,metric_name,dimension) DO UPDATE SET
    metric_value=EXCLUDED.metric_value,sample_count=EXCLUDED.sample_count
"""


def _decoded_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


class ReplayRepository:
    """Read historical PostGIS facts without invoking any provider network."""

    def __init__(
        self,
        connect_factory: Callable[..., Any],
        connect_kwargs: Mapping[str, Any],
    ) -> None:
        self._connect_factory = connect_factory
        self._connect_kwargs = dict(connect_kwargs)

    def load_frames(
        self,
        *,
        from_at: float,
        until_at: float,
        target_id: str,
        provider_id: str | None = None,
    ) -> tuple[ReplayFrame, ...]:
        start = float(from_at)
        end = float(until_at)
        if not target_id or not start < end:
            raise ValueError("replay_range_invalid")
        connection = self._connect_factory(**self._connect_kwargs)
        try:
            with connection.cursor() as cursor:
                cursor.execute(_SELECT_TARGET_POSITIONS, (target_id, start, end))
                positions = tuple(cursor.fetchall())
                cursor.execute(
                    _SELECT_CELLS,
                    (start - 900.0, end, provider_id, provider_id),
                )
                cell_rows = tuple(cursor.fetchall())
                cursor.execute(
                    _SELECT_GROUND,
                    (
                        target_id,
                        start,
                        end,
                        start - 7200.0,
                        end + 7200.0,
                        20_000.0,
                    ),
                )
                ground_rows = tuple(cursor.fetchall())
        finally:
            connection.close()

        cells = tuple(ReplayCell(
            cell_id=str(row[0]),
            provider_id=str(row[1]),
            observed_at=float(row[2]),
            precipitation_rate_mm_h=(None if row[3] is None else float(row[3])),
            reflectivity_dbz=(None if row[4] is None else float(row[4])),
            radar_level=(None if row[5] is None else float(row[5])),
            geometry=MappingProxyType(dict(_decoded_json(row[6]))),
            velocity_east_kmh=float(row[7]),
            velocity_north_kmh=float(row[8]),
            speed_kmh=float(row[9]),
            heading_deg=None if row[10] is None else float(row[10]),
            latitude=float(row[11]),
            longitude=float(row[12]),
        ) for row in cell_rows)
        ground = tuple(NormalizedObservation.create(
            provider_id=str(row[0]),
            product_id=str(row[1]),
            target_id=target_id,
            kind=ObservationKind.RAIN_RATE,
            value=float(row[2]),
            unit=str(row[3]),
            observed_at=float(row[4]),
            received_at=float(row[5]),
            latitude=float(row[6]),
            longitude=float(row[7]),
            quality=ObservationQuality(str(row[8])),
            metadata=dict(_decoded_json(row[9])),
            geometry=dict(_decoded_json(row[10])),
            maximum_age_seconds=10_000_000.0,
        ) for row in ground_rows)
        return tuple(ReplayFrame(
            evaluated_at=float(row[0]),
            target_id=target_id,
            target_latitude=float(row[1]),
            target_longitude=float(row[2]),
            target_accuracy_km=float(row[3]),
            cells=tuple(
                cell for cell in cells
                if float(row[0]) - 900.0 <= cell.observed_at <= float(row[0])
            ),
            ground_observations=tuple(
                item for item in ground
                if abs(item.observed_at - float(row[0])) <= 7200.0
            ),
        ) for row in positions)

    def load_radar_cells(
        self,
        *,
        from_at: float,
        until_at: float,
        provider_id: str | None = None,
    ) -> tuple[ReplayCell, ...]:
        start = float(from_at)
        end = float(until_at)
        if not start < end:
            raise ValueError("replay_range_invalid")
        connection = self._connect_factory(**self._connect_kwargs)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    _SELECT_CELLS,
                    (start, end, provider_id, provider_id),
                )
                rows = tuple(cursor.fetchall())
        finally:
            connection.close()
        return tuple(ReplayCell(
            cell_id=str(row[0]),
            provider_id=str(row[1]),
            observed_at=float(row[2]),
            precipitation_rate_mm_h=(None if row[3] is None else float(row[3])),
            reflectivity_dbz=(None if row[4] is None else float(row[4])),
            radar_level=(None if row[5] is None else float(row[5])),
            geometry=MappingProxyType(dict(_decoded_json(row[6]))),
            velocity_east_kmh=float(row[7]),
            velocity_north_kmh=float(row[8]),
            speed_kmh=float(row[9]),
            heading_deg=None if row[10] is None else float(row[10]),
            latitude=float(row[11]),
            longitude=float(row[12]),
        ) for row in rows)

    def persist_report(
        self,
        report: ReplayReport,
        *,
        parameters: Mapping[str, Any],
    ) -> None:
        connection = self._connect_factory(**self._connect_kwargs)
        try:
            with connection.cursor() as cursor:
                cursor.execute(_INSERT_REPLAY_RUN, (
                    report.replay_id,
                    report.algorithm_version,
                    json.dumps(parameters, sort_keys=True, separators=(",", ":")),
                ))
                for metric in report.metrics:
                    cursor.execute(_INSERT_REPLAY_METRIC, (
                        report.replay_id,
                        metric.metric_name,
                        json.dumps(dict(metric.dimension), sort_keys=True, separators=(",", ":")),
                        metric.metric_value,
                        metric.sample_count,
                    ))
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()


__all__ = [
    "ReplayFrame",
    "ReplayCell",
    "ReplayMetric",
    "ReplayReport",
    "RadarForecastVerification",
    "ReplayRepository",
    "run_replay",
    "verify_radar_forecasts",
]
