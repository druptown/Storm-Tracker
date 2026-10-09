"""Replay opgeslagen radarrasters door de echte StormEngine.

Vergelijk de historische afstandskoppeling met de fysieke associatiepoort.
Dit script leest uitsluitend een lokale JSON-export en raakt HA niet.
"""
from __future__ import annotations

import asyncio
import json
from math import cos, radians
from pathlib import Path
import sys
from unittest.mock import patch

from storm_tracker_worker.tracking_core.engine.observation import (
    Observation,
    ObservationType,
)
from storm_tracker_worker.tracking_core.engine.storm_engine import (
    StormEngine,
    _haversine,
)


MIN_COMPONENT_PIXELS = 4
SYSTEM_GAP_KM = 8.0


def _footprint_gap_km(first: Observation, second: Observation) -> float:
    first_points = first.footprint_points or ((first.lat, first.lon),)
    second_points = second.footprint_points or ((second.lat, second.lon),)
    return min(
        _haversine(first_lat, first_lon, second_lat, second_lon)
        for first_lat, first_lon in first_points
        for second_lat, second_lon in second_points
    )


def _group_frame_systems(
    observations: list[Observation], timestamp: float
) -> list[Observation]:
    """Experimental broad grouping retained for explicit replay comparison."""
    ordered = sorted(observations, key=lambda item: str(item.radar_cell_id or ""))
    remaining = set(range(len(ordered)))
    groups = []
    while remaining:
        seed = min(remaining)
        remaining.remove(seed)
        stack = [seed]
        group = []
        while stack:
            current = stack.pop()
            group.append(current)
            connected = [
                index for index in sorted(remaining)
                if _footprint_gap_km(ordered[current], ordered[index])
                <= SYSTEM_GAP_KM
            ]
            for index in connected:
                remaining.remove(index)
                stack.append(index)
        groups.append(sorted(group))

    for parent_index, group in enumerate(groups):
        members = [ordered[index] for index in group]
        parent_id = f"replay:{int(timestamp)}:p{parent_index}"
        parent_area = sum(float(item.area_km2 or 0.0) for item in members)
        parent_points = tuple(dict.fromkeys(
            point
            for item in members
            for point in (item.footprint_points or ((item.lat, item.lon),))
        ))
        for item in members:
            item.parent_system_id = parent_id
            item.parent_area_km2 = parent_area
            item.parent_footprint_points = parent_points
    merged = []
    by_parent = {}
    for item in ordered:
        by_parent.setdefault(str(item.parent_system_id), []).append(item)
    for parent_id, members in sorted(by_parent.items()):
        weights = [max(0.001, float(item.area_km2 or 0.0)) for item in members]
        total_weight = sum(weights)
        area = sum(float(item.area_km2 or 0.0) for item in members)
        footprint = members[0].parent_footprint_points
        merged.append(Observation(
            obs_type=ObservationType.RADAR,
            lat=sum(item.lat * weight for item, weight in zip(members, weights))
                / total_weight,
            lon=sum(item.lon * weight for item, weight in zip(members, weights))
                / total_weight,
            timestamp=timestamp,
            intensity=max(int(item.intensity or 0) for item in members),
            area_km2=area,
            quality=1.0,
            footprint_points=footprint,
            radar_cell_id=parent_id,
            parent_system_id=parent_id,
            parent_area_km2=area,
            parent_footprint_points=footprint,
            source=str(members[0].source),
        ))
    return merged


def _components(frame: dict) -> list[Observation]:
    values = {
        (int(row), int(column)): int(intensity)
        for row, column, intensity in frame["cells"]
        if int(intensity) > 0
    }
    remaining = set(values)
    groups = []
    while remaining:
        seed = remaining.pop()
        stack = [seed]
        group = [seed]
        while stack:
            row, column = stack.pop()
            for row_offset in (-1, 0, 1):
                for column_offset in (-1, 0, 1):
                    neighbour = (row + row_offset, column + column_offset)
                    if neighbour in remaining:
                        remaining.remove(neighbour)
                        stack.append(neighbour)
                        group.append(neighbour)
        if len(group) >= MIN_COMPONENT_PIXELS:
            groups.append(group)

    west, _south, _east, north = map(float, frame["bounds"])
    lat_grid = float(frame.get("grid_lat_deg", frame["grid_deg"]))
    lon_grid = float(frame.get("grid_lon_deg", frame["grid_deg"]))
    timestamp = float(frame["nominal_timestamp"])
    observations = []
    for index, pixels in enumerate(
        sorted(groups, key=len, reverse=True)
    ):
        weight = sum(values[pixel] for pixel in pixels)
        row = sum((pixel[0] + 0.5) * values[pixel] for pixel in pixels) / weight
        column = (
            sum((pixel[1] + 0.5) * values[pixel] for pixel in pixels) / weight
        )
        lat = north - row * lat_grid
        lon = west + column * lon_grid
        min_row = min(pixel[0] for pixel in pixels)
        max_row = max(pixel[0] for pixel in pixels) + 1
        min_column = min(pixel[1] for pixel in pixels)
        max_column = max(pixel[1] for pixel in pixels) + 1
        footprint = (
            (north - min_row * lat_grid, west + min_column * lon_grid),
            (north - min_row * lat_grid, west + max_column * lon_grid),
            (north - max_row * lat_grid, west + max_column * lon_grid),
            (north - max_row * lat_grid, west + min_column * lon_grid),
        )
        pixel_height_km = 111.32 * lat_grid
        pixel_width_km = 111.32 * lon_grid * cos(radians(lat))
        area = len(pixels) * pixel_height_km * pixel_width_km
        component_id = f"replay:{int(timestamp)}:c{index}"
        observations.append(
            Observation(
                obs_type=ObservationType.RADAR,
                lat=lat,
                lon=lon,
                timestamp=timestamp,
                intensity=max(values[pixel] for pixel in pixels),
                area_km2=area,
                quality=1.0,
                footprint_points=footprint,
                radar_cell_id=component_id,
                parent_system_id=component_id,
                parent_area_km2=area,
                parent_footprint_points=footprint,
                source=str(frame["source"]),
            )
        )
    return observations


def _summary(engine: StormEngine, frame_counts: list[int]) -> dict:
    transitions = []
    for storm_id, points in engine._history.items():
        ordered = sorted(points, key=lambda point: point.ts)
        for previous, current in zip(ordered, ordered[1:]):
            elapsed_hours = (current.ts - previous.ts) / 3600.0
            if elapsed_hours <= 0:
                continue
            distance = _haversine(
                previous.lat, previous.lon, current.lat, current.lon
            )
            transitions.append(
                {
                    "storm_id": storm_id,
                    "timestamp": int(current.ts),
                    "distance_km": round(distance, 2),
                    "speed_kmh": round(distance / elapsed_hours, 1),
                }
            )
    transitions.sort(key=lambda item: item["speed_kmh"], reverse=True)
    return {
        "frames": len(frame_counts),
        "component_counts": frame_counts,
        "components_total": sum(frame_counts),
        "tracks_final": len(engine.get_storms()),
        "lineage_events": engine.consume_lineage_events(),
        "transition_count": len(transitions),
        "transitions_over_180_kmh": sum(
            item["speed_kmh"] > 180.0 for item in transitions
        ),
        "max_speed_kmh": transitions[0]["speed_kmh"] if transitions else 0.0,
        "largest_transitions": transitions[:10],
    }


def _run(frames: list[dict], *, gated: bool, grouped: bool = False) -> dict:
    engine = StormEngine()
    if not gated:
        engine._radar_assignment_allowed = lambda _obs, _storm: True
    frame_counts = []
    for frame in frames:
        observations = _components(frame)
        if grouped:
            observations = _group_frame_systems(
                observations, float(frame["nominal_timestamp"])
            )
        frame_counts.append(len(observations))
        timestamp = float(frame["nominal_timestamp"])
        with patch(
            "storm_tracker_worker.tracking_core.engine.storm_engine.time.time",
            return_value=timestamp,
        ):
            asyncio.run(engine.process_batch(observations))
    return _summary(engine, frame_counts)


def main() -> None:
    source = Path(sys.argv[1]).resolve()
    payload = json.loads(source.read_text(encoding="utf-8"))
    frames = [
        json.loads(item["frame_json"])
        for item in sorted(
            payload["frames"], key=lambda item: int(item["nominal_timestamp"])
        )
    ]
    result = {
        "source": str(source),
        "region_id": payload.get("region_id"),
        "baseline": _run(frames, gated=False),
        "gated": _run(frames, gated=True),
        "grouped": _run(frames, gated=True, grouped=True),
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
