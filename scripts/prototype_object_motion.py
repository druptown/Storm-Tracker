"""Prototype: multi-threshold radar objects with global frame assignment."""
from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
import statistics
import sys

import numpy as np


MAX_LINK_KM = 60.0
NEW_TRACK_COST = 4.5
TARGET_LAT = float(sys.argv[2]) if len(sys.argv) > 3 else 29.7604
TARGET_LON = float(sys.argv[3]) if len(sys.argv) > 3 else -95.3698


def _distance(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    radius = 6371.0
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    dp = p2 - p1
    dl = math.radians(b_lon - a_lon)
    value = (
        math.sin(dp / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    )
    return 2 * radius * math.asin(math.sqrt(value))


@dataclass(frozen=True)
class Feature:
    timestamp: int
    lat: float
    lon: float
    pixels: frozenset[tuple[int, int]]
    area: int
    peak: int
    threshold: int
    target_distance_km: float

    def distance_to_target(self) -> float:
        return self.target_distance_km


def _features(frame: dict, threshold: int) -> list[Feature]:
    values = {
        (int(row), int(column)): int(value)
        for row, column, value in frame["cells"]
        if int(value) >= threshold
    }
    remaining = set(values)
    groups: list[set[tuple[int, int]]] = []
    while remaining:
        seed = remaining.pop()
        stack = [seed]
        group = {seed}
        while stack:
            row, column = stack.pop()
            for dr in (-1, 0, 1):
                for dc in (-1, 0, 1):
                    neighbour = row + dr, column + dc
                    if neighbour in remaining:
                        remaining.remove(neighbour)
                        group.add(neighbour)
                        stack.append(neighbour)
        if len(group) >= 4:
            groups.append(group)

    west, _south, _east, north = map(float, frame["bounds"])
    lat_grid = float(frame.get("grid_lat_deg", frame["grid_deg"]))
    lon_grid = float(frame.get("grid_lon_deg", frame["grid_deg"]))
    result = []
    for group in groups:
        total = sum(values[pixel] for pixel in group)
        row = sum((r + 0.5) * values[(r, c)] for r, c in group) / total
        col = sum((c + 0.5) * values[(r, c)] for r, c in group) / total
        result.append(
            Feature(
                timestamp=int(frame["nominal_timestamp"]),
                lat=north - row * lat_grid,
                lon=west + col * lon_grid,
                pixels=frozenset(group),
                area=len(group),
                peak=max(values[pixel] for pixel in group),
                threshold=threshold,
                target_distance_km=min(
                    _distance(
                        north - (pixel_row + 0.5) * lat_grid,
                        west + (pixel_column + 0.5) * lon_grid,
                        TARGET_LAT,
                        TARGET_LON,
                    )
                    for pixel_row, pixel_column in group
                ),
            )
        )
    return sorted(result, key=lambda item: item.area, reverse=True)


def _cost(previous: Feature, current: Feature) -> float:
    distance = _distance(
        previous.lat, previous.lon, current.lat, current.lon
    )
    elapsed_hours = (current.timestamp - previous.timestamp) / 3600.0
    if elapsed_hours <= 0 or distance > MAX_LINK_KM * max(1.0, elapsed_hours * 6):
        return 1e6
    overlap = len(previous.pixels & current.pixels)
    union = len(previous.pixels | current.pixels)
    iou = overlap / union if union else 0.0
    area_change = abs(math.log(current.area / previous.area))
    peak_change = abs(current.peak - previous.peak)
    return distance / 20.0 + area_change + (1.0 - iou) * 1.5 + peak_change * 0.2


def _assignment(costs: list[list[float]]) -> list[tuple[int, int]]:
    """Minimum-cost rectangular assignment (Hungarian algorithm)."""
    if not costs or not costs[0]:
        return []
    rows, columns = len(costs), len(costs[0])
    size = max(rows, columns)
    matrix = [
        [
            costs[row][column]
            if row < rows and column < columns
            else NEW_TRACK_COST
            for column in range(size)
        ]
        for row in range(size)
    ]
    u = [0.0] * (size + 1)
    v = [0.0] * (size + 1)
    p = [0] * (size + 1)
    way = [0] * (size + 1)
    for row in range(1, size + 1):
        p[0] = row
        column0 = 0
        minimum = [math.inf] * (size + 1)
        used = [False] * (size + 1)
        while True:
            used[column0] = True
            row0 = p[column0]
            delta = math.inf
            column1 = 0
            for column in range(1, size + 1):
                if used[column]:
                    continue
                current = matrix[row0 - 1][column - 1] - u[row0] - v[column]
                if current < minimum[column]:
                    minimum[column] = current
                    way[column] = column0
                if minimum[column] < delta:
                    delta = minimum[column]
                    column1 = column
            for column in range(size + 1):
                if used[column]:
                    u[p[column]] += delta
                    v[column] -= delta
                else:
                    minimum[column] -= delta
            column0 = column1
            if p[column0] == 0:
                break
        while True:
            column1 = way[column0]
            p[column0] = p[column1]
            column0 = column1
            if column0 == 0:
                break
    return [
        (p[column] - 1, column - 1)
        for column in range(1, size + 1)
        if 0 < p[column] <= rows
        and column <= columns
        and costs[p[column] - 1][column - 1] < NEW_TRACK_COST
    ]


def _track(frames: list[dict], threshold: int) -> list[list[Feature]]:
    tracks: list[list[Feature]] = []
    previous: list[tuple[int, Feature]] = []
    for frame in frames:
        current = _features(frame, threshold)
        costs = [
            [_cost(feature, candidate) for candidate in current]
            for _track_id, feature in previous
        ]
        links = _assignment(costs)
        linked_current = set()
        next_previous = []
        for previous_index, current_index in links:
            track_id = previous[previous_index][0]
            tracks[track_id].append(current[current_index])
            linked_current.add(current_index)
            next_previous.append((track_id, current[current_index]))
        for index, feature in enumerate(current):
            if index in linked_current:
                continue
            tracks.append([feature])
            next_previous.append((len(tracks) - 1, feature))
        previous = next_previous
    return tracks


def _motion(track: list[Feature]) -> dict:
    ordered = sorted(track, key=lambda item: item.timestamp)
    origin = ordered[0]
    hours = np.asarray(
        [(item.timestamp - origin.timestamp) / 3600.0 for item in ordered]
    )
    east = np.asarray(
        [
            (item.lon - origin.lon)
            * 111.32
            * math.cos(math.radians(origin.lat))
            for item in ordered
        ]
    )
    north = np.asarray(
        [(item.lat - origin.lat) * 110.574 for item in ordered]
    )
    east_velocity = float(np.polyfit(hours, east, 1)[0])
    north_velocity = float(np.polyfit(hours, north, 1)[0])
    fitted_speed = math.hypot(east_velocity, north_velocity)
    duration = hours[-1]
    net_distance = _distance(
        origin.lat, origin.lon, ordered[-1].lat, ordered[-1].lon
    )
    steps = [
        _distance(a.lat, a.lon, b.lat, b.lon)
        / ((b.timestamp - a.timestamp) / 3600.0)
        for a, b in zip(ordered, ordered[1:])
        if b.timestamp > a.timestamp
    ]
    return {
        "samples": len(ordered),
        "duration_minutes": round(duration * 60.0, 1),
        "fit_speed_kmh": round(fitted_speed, 1),
        "net_speed_kmh": round(net_distance / duration, 1),
        "median_interval_kmh": round(statistics.median(steps), 1),
        "latest_footprint_distance_to_tracker_km": round(
            ordered[-1].distance_to_target(), 1
        ),
    }


def main() -> None:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    frames = [
        json.loads(item["frame_json"])
        for item in sorted(
            payload["frames"], key=lambda item: int(item["nominal_timestamp"])
        )
    ]
    result = {}
    for threshold in (1, 2, 3):
        tracks = _track(frames, threshold)
        candidates = [
            track for track in tracks
            if len(track) >= 3
            and track[-1].timestamp == int(frames[-1]["nominal_timestamp"])
        ]
        candidates.sort(key=lambda track: track[-1].distance_to_target())
        result[str(threshold)] = {
            "track_count": len(tracks),
            "selected": _motion(candidates[0]) if candidates else None,
            "alternatives": [
                _motion(track) for track in candidates[:5]
            ],
        }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
