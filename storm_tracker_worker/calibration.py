"""Bounded cross-provider radar calibration outside Home Assistant Core."""
from __future__ import annotations

from itertools import combinations
import time

MAX_FRAMES_PER_JOB = 64
_frames: dict[tuple[str, str, int], list] = {}
_matched: set[tuple[str, str, str, int]] = set()


def _points(frame: list) -> set[tuple[int, int]]:
    return {
        (int(point[0]), int(point[1]))
        for point in frame[13]
    }


def _comparison(first: list, second: list) -> list:
    primary, reference = _points(first), _points(second)
    overlap = primary & reference
    false_positive = primary - reference
    missed = reference - primary
    precision = len(overlap) / len(primary) if primary else None
    recall = len(overlap) / len(reference) if reference else None
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision is not None and recall is not None and precision + recall
        else None
    )
    return [
        first[0], first[1], second[1], first[2], time.time(),
        len(primary), len(reference), len(overlap),
        len(false_positive), len(missed),
        round(precision, 3) if precision is not None else None,
        round(recall, 3) if recall is not None else None,
        round(f1, 3) if f1 is not None else None,
        round(min(float(first[12]), float(second[12])), 3),
    ]


def calibration_operational(job) -> dict:
    frames = job.payload.get("frames", [])
    if not isinstance(frames, list) or len(frames) > MAX_FRAMES_PER_JOB:
        raise ValueError("ongeldige kalibratieframecollectie")
    touched: set[tuple[str, int]] = set()
    for raw in frames:
        if not isinstance(raw, list) or len(raw) < 14:
            raise ValueError("ongeldig kalibratieframe")
        key = (str(raw[0]), str(raw[1]), int(raw[2]))
        _frames[key] = raw
        touched.add((key[0], key[2]))
    comparisons = []
    for region_id, minute in touched:
        available = sorted(
            (
                frame for (region, _source, nominal), frame in _frames.items()
                if region == region_id and nominal == minute
            ),
            key=lambda frame: str(frame[1]),
        )
        for first, second in combinations(available, 2):
            key = (region_id, str(first[1]), str(second[1]), minute)
            if key in _matched:
                continue
            _matched.add(key)
            if min(float(first[12]), float(second[12])) >= 0.60:
                comparisons.append(_comparison(first, second))
    cutoff = int(time.time() // 60) - 180
    for key in tuple(_frames):
        if key[2] < cutoff:
            _frames.pop(key, None)
    _matched.intersection_update({key for key in _matched if key[3] >= cutoff})
    return {
        "method": "worker_radar_calibration_v1",
        "frames_received": len(frames),
        "comparisons": comparisons,
        "cached_frames": len(_frames),
    }
