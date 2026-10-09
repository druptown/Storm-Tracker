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
class ReplayReport:
    replay_id: str
    algorithm_version: str
    passages: tuple[TargetPassage, ...]
    validations: tuple[ForecastValidation, ...]
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


def _report_id(frames: tuple[ReplayFrame, ...], algorithm_version: str) -> str:
    identity = {
        "algorithm_version": algorithm_version,
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
    }
    return hashlib.sha256(json.dumps(
        identity, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")).hexdigest()


def run_replay(
    frames: Iterable[ReplayFrame],
    *,
    algorithm_version: str,
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
    metrics.sort(key=lambda item: item.metric_name)
    return ReplayReport(
        replay_id=_report_id(ordered, algorithm_version),
        algorithm_version=algorithm_version,
        passages=unique_passages,
        validations=unique_validations,
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
  AND (%s IS NULL OR cell.provider_id=%s)
ORDER BY cell.observed_at,cell.cell_id
"""

_SELECT_GROUND = """
SELECT provider_id,product_id,target_id,value,unit,
       EXTRACT(EPOCH FROM observed_at),EXTRACT(EPOCH FROM received_at),
       ST_Y(geometry),ST_X(geometry),quality,metadata,ST_AsGeoJSON(geometry)
FROM stv3_observation
WHERE target_id=%s AND kind='rain_rate'
  AND observed_at>=to_timestamp(%s) AND observed_at<to_timestamp(%s)
  AND metadata ? 'station_id'
ORDER BY observed_at,provider_id
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
                    (target_id, start - 7200.0, end + 7200.0),
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
            target_id=None if row[2] is None else str(row[2]),
            kind=ObservationKind.RAIN_RATE,
            value=float(row[3]),
            unit=str(row[4]),
            observed_at=float(row[5]),
            received_at=float(row[6]),
            latitude=float(row[7]),
            longitude=float(row[8]),
            quality=ObservationQuality(str(row[9])),
            metadata=dict(_decoded_json(row[10])),
            geometry=dict(_decoded_json(row[11])),
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
    "ReplayRepository",
    "run_replay",
]
