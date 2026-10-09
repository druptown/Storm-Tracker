"""Deterministic translated-footprint passages for tracked radar cells."""

from __future__ import annotations

from dataclasses import dataclass
import math
from types import MappingProxyType
from typing import Any, Mapping


MAX_CELL_AGE_SECONDS = 15 * 60.0
DEFAULT_HORIZON_SECONDS = 2 * 60 * 60.0
DEFAULT_CENTERLINE_RADIUS_KM = 20.0


@dataclass(frozen=True, slots=True)
class TargetPassage:
    target_id: str
    evaluated_at: float
    cell_id: str
    provider_id: str
    entry_at: float
    exit_at: float
    method: str
    predicted_intensity_mm_h: float | None
    footprint: Mapping[str, Any]
    fallback_reason: str | None = None
    horizon_clipped: bool = False
    motion_status: str = "indeterminate"
    motion_confidence: str = "indeterminate"
    motion_method: str | None = None
    motion_reason_codes: tuple[str, ...] = ()
    motion_supporting_methods: tuple[str, ...] = ()
    motion_agreement_angle_deg: float | None = None

    def __post_init__(self) -> None:
        if self.exit_at < self.entry_at:
            raise ValueError("passage_exit_precedes_entry")


def _local_km(
    longitude: float,
    latitude: float,
    *,
    origin_longitude: float,
    origin_latitude: float,
) -> tuple[float, float]:
    return (
        (longitude - origin_longitude)
        * 111.320
        * math.cos(math.radians(origin_latitude)),
        (latitude - origin_latitude) * 110.574,
    )


def _rings(
    geometry: Mapping[str, Any],
    *,
    origin_longitude: float,
    origin_latitude: float,
) -> tuple[tuple[tuple[tuple[float, float], ...], ...], ...]:
    kind = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if kind == "Polygon":
        polygons = (coordinates,)
    elif kind == "MultiPolygon":
        polygons = coordinates
    else:
        return ()
    if not isinstance(polygons, (list, tuple)):
        return ()
    result = []
    for polygon in polygons:
        if not isinstance(polygon, (list, tuple)) or not polygon:
            continue
        local_rings = []
        for ring in polygon:
            if not isinstance(ring, (list, tuple)) or len(ring) < 4:
                continue
            points = []
            for point in ring:
                if not isinstance(point, (list, tuple)) or len(point) < 2:
                    points = []
                    break
                try:
                    longitude, latitude = float(point[0]), float(point[1])
                except (TypeError, ValueError):
                    points = []
                    break
                if not math.isfinite(longitude) or not math.isfinite(latitude):
                    points = []
                    break
                points.append(_local_km(
                    longitude,
                    latitude,
                    origin_longitude=origin_longitude,
                    origin_latitude=origin_latitude,
                ))
            if points:
                if points[0] != points[-1]:
                    points.append(points[0])
                local_rings.append(tuple(points))
        if local_rings:
            result.append(tuple(local_rings))
    return tuple(result)


def _inside_ring(point: tuple[float, float], ring: tuple[tuple[float, float], ...]) -> bool:
    x_value, y_value = point
    inside = False
    for first, second in zip(ring, ring[1:]):
        x1, y1 = first
        x2, y2 = second
        if ((y1 > y_value) != (y2 > y_value)):
            crossing = (x2 - x1) * (y_value - y1) / (y2 - y1) + x1
            if x_value < crossing:
                inside = not inside
    return inside


def _segment_distance(
    point: tuple[float, float],
    first: tuple[float, float],
    second: tuple[float, float],
) -> float:
    px, py = point
    ax, ay = first
    bx, by = second
    dx, dy = bx - ax, by - ay
    length_squared = dx * dx + dy * dy
    if length_squared <= 1e-18:
        return math.hypot(px - ax, py - ay)
    fraction = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / length_squared))
    return math.hypot(px - (ax + fraction * dx), py - (ay + fraction * dy))


def _intersects(
    point: tuple[float, float],
    polygons: tuple[tuple[tuple[tuple[float, float], ...], ...], ...],
    buffer_km: float,
) -> bool:
    for polygon in polygons:
        if _inside_ring(point, polygon[0]) and not any(
            _inside_ring(point, hole) for hole in polygon[1:]
        ):
            return True
        if buffer_km > 0.0 and any(
            _segment_distance(point, first, second) <= buffer_km
            for ring in polygon
            for first, second in zip(ring, ring[1:])
        ):
            return True
    return False


def _refine_boundary(
    predicate: Any,
    lower: float,
    upper: float,
    *,
    entering: bool,
) -> float:
    for _ in range(24):
        middle = (lower + upper) / 2.0
        if bool(predicate(middle)) is entering:
            upper = middle
        else:
            lower = middle
    return upper


def _polygon_interval(
    polygons: tuple[tuple[tuple[tuple[float, float], ...], ...], ...],
    *,
    velocity_east_kmh: float,
    velocity_north_kmh: float,
    buffer_km: float,
    horizon_seconds: float,
) -> tuple[float, float, bool] | None:
    speed_kmh = math.hypot(velocity_east_kmh, velocity_north_kmh)

    def hit(seconds: float) -> bool:
        hours = seconds / 3600.0
        return _intersects(
            (-velocity_east_kmh * hours, -velocity_north_kmh * hours),
            polygons,
            buffer_km,
        )

    initial = hit(0.0)
    if speed_kmh < 0.5:
        return (0.0, horizon_seconds, True) if initial else None
    step_seconds = min(30.0, max(1.0, 900.0 / speed_kmh))
    previous_time = 0.0
    previous_hit = initial
    entry = 0.0 if initial else None
    last_exit = None
    current_time = min(step_seconds, horizon_seconds)
    while previous_time < horizon_seconds:
        current_hit = hit(current_time)
        if not previous_hit and current_hit:
            boundary = _refine_boundary(
                hit, previous_time, current_time, entering=True,
            )
            if entry is None:
                entry = boundary
        elif previous_hit and not current_hit and entry is not None:
            last_exit = _refine_boundary(
                hit, previous_time, current_time, entering=False,
            )
        previous_time, previous_hit = current_time, current_hit
        current_time = min(horizon_seconds, current_time + step_seconds)
        if current_time == previous_time:
            break
    if entry is None:
        return None
    if previous_hit:
        return entry, horizon_seconds, True
    if last_exit is None:
        return None
    return entry, last_exit, False


def _centerline_interval(
    *,
    cell_longitude: float,
    cell_latitude: float,
    target_longitude: float,
    target_latitude: float,
    velocity_east_kmh: float,
    velocity_north_kmh: float,
    radius_km: float,
    horizon_seconds: float,
    elapsed_seconds: float = 0.0,
) -> tuple[float, float, bool] | None:
    x_value, y_value = _local_km(
        cell_longitude,
        cell_latitude,
        origin_longitude=target_longitude,
        origin_latitude=target_latitude,
    )
    vx = velocity_east_kmh / 3600.0
    vy = velocity_north_kmh / 3600.0
    x_value += vx * elapsed_seconds
    y_value += vy * elapsed_seconds
    a = vx * vx + vy * vy
    c = x_value * x_value + y_value * y_value - radius_km * radius_km
    if a < 1e-12:
        return (0.0, horizon_seconds, True) if c <= 0.0 else None
    b = 2.0 * (x_value * vx + y_value * vy)
    discriminant = b * b - 4.0 * a * c
    if discriminant < 0.0:
        return None
    root = math.sqrt(discriminant)
    # The legacy point fallback has no physical leading edge. Preserve its
    # established ETA meaning: time at which the tracked centre is closest to
    # the target. Exact entry/exit semantics are reserved for real polygons.
    entry = -b / (2.0 * a)
    exit_time = (-b + root) / (2.0 * a)
    if entry < 0.0 or entry > horizon_seconds or exit_time < entry:
        return None
    clipped_exit = min(horizon_seconds, exit_time)
    return entry, clipped_exit, exit_time > horizon_seconds


def forecast_target_passage(
    *,
    cell: Any,
    target_id: str,
    target_latitude: float,
    target_longitude: float,
    evaluated_at: float,
    horizon_seconds: float = DEFAULT_HORIZON_SECONDS,
    accuracy_buffer_km: float = 0.0,
    centerline_radius_km: float = DEFAULT_CENTERLINE_RADIUS_KM,
) -> TargetPassage | None:
    evaluated = float(evaluated_at)
    horizon = float(horizon_seconds)
    buffer_km = max(0.0, float(accuracy_buffer_km))
    if horizon <= 0.0:
        raise ValueError("forecast_horizon_invalid")
    age_seconds = max(0.0, evaluated - float(cell.observed_at))
    if age_seconds > MAX_CELL_AGE_SECONDS:
        return None
    geometry = dict(cell.geometry)
    polygons = _rings(
        geometry,
        origin_longitude=float(target_longitude),
        origin_latitude=float(target_latitude),
    )
    if polygons:
        # Geometry belongs to observed_at, not to this processing tick. Advance
        # it to evaluation time before computing the remaining passage interval.
        dx = float(cell.velocity_east_kmh) * age_seconds / 3600.0
        dy = float(cell.velocity_north_kmh) * age_seconds / 3600.0
        polygons = tuple(
            tuple(tuple((x + dx, y + dy) for x, y in ring) for ring in polygon)
            for polygon in polygons
        )
        interval = _polygon_interval(
            polygons,
            velocity_east_kmh=float(cell.velocity_east_kmh),
            velocity_north_kmh=float(cell.velocity_north_kmh),
            buffer_km=buffer_km,
            horizon_seconds=horizon,
        )
        method = "translated_footprint"
        fallback_reason = None
    else:
        interval = _centerline_interval(
            cell_longitude=float(cell.longitude),
            cell_latitude=float(cell.latitude),
            target_longitude=float(target_longitude),
            target_latitude=float(target_latitude),
            velocity_east_kmh=float(cell.velocity_east_kmh),
            velocity_north_kmh=float(cell.velocity_north_kmh),
            radius_km=max(float(centerline_radius_km), buffer_km),
            horizon_seconds=horizon,
            elapsed_seconds=age_seconds,
        )
        method = "centerline_fallback"
        fallback_reason = "polygon_unavailable"
    if interval is None:
        return None
    entry_seconds, exit_seconds, clipped = interval
    return TargetPassage(
        target_id=str(target_id),
        evaluated_at=evaluated,
        cell_id=str(cell.cell_id),
        provider_id=str(cell.provider_id),
        entry_at=evaluated + entry_seconds,
        exit_at=evaluated + exit_seconds,
        method=method,
        predicted_intensity_mm_h=(
            None
            if (
                getattr(cell, "mean_precipitation_rate_mm_h", None) is None
                and cell.precipitation_rate_mm_h is None
            )
            else float(
                getattr(cell, "mean_precipitation_rate_mm_h", None)
                if getattr(cell, "mean_precipitation_rate_mm_h", None) is not None
                else cell.precipitation_rate_mm_h
            )
        ),
        footprint=MappingProxyType(geometry),
        fallback_reason=fallback_reason,
        horizon_clipped=clipped,
        motion_status=str(getattr(cell, "motion_status", "indeterminate")),
        motion_confidence=str(
            getattr(cell, "motion_confidence", "indeterminate")
        ),
        motion_method=getattr(cell, "motion_method", None),
        motion_reason_codes=tuple(
            str(item) for item in getattr(cell, "motion_reason_codes", ())
        ),
        motion_supporting_methods=tuple(
            str(item)
            for item in getattr(cell, "motion_supporting_methods", ())
        ),
        motion_agreement_angle_deg=getattr(
            cell, "motion_agreement_angle_deg", None
        ),
    )


__all__ = ["TargetPassage", "forecast_target_passage"]
