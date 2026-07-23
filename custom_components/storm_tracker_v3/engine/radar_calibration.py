"""Passieve regionale kruisvalidatie tussen operationele neerslagproviders.

Deze eerste fase observeert uitsluitend. De scores wijzigen geen enkele
operationele drempel en kunnen daardoor veilig langere tijd worden verzameld.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import math
import time
from typing import Iterable, Sequence


GRID_DEG = 0.10
MAX_REFERENCE_AGE_S = 15 * 60
HISTORY_SIZE = 96
FRAME_HISTORY_MINUTES = 180
MIN_SHARED_COVERAGE_FRACTION = 0.60

CoverageBounds = tuple[float, float, float, float]


def _evaluation_bbox(
    center: tuple[float, float] | None,
    radius_km: float | None,
) -> CoverageBounds | None:
    if center is None or radius_km is None:
        return None
    lat, lon = float(center[0]), float(center[1])
    lat_delta = float(radius_km) / 110.574
    lon_delta = float(radius_km) / (
        111.320 * max(0.1, abs(math.cos(math.radians(lat))))
    )
    return (
        lon - lon_delta,
        lat - lat_delta,
        lon + lon_delta,
        lat + lat_delta,
    )


def _bbox_intersection(
    first: CoverageBounds | None,
    second: CoverageBounds | None,
) -> CoverageBounds | None:
    if first is None:
        return second
    if second is None:
        return first
    intersection = (
        max(first[0], second[0]),
        max(first[1], second[1]),
        min(first[2], second[2]),
        min(first[3], second[3]),
    )
    if intersection[0] >= intersection[2] or intersection[1] >= intersection[3]:
        return None
    return intersection


def _bbox_fraction(
    coverage: CoverageBounds | None,
    evaluation: CoverageBounds | None,
) -> float:
    if evaluation is None:
        return 1.0
    intersection = _bbox_intersection(coverage, evaluation)
    if intersection is None:
        return 0.0
    evaluation_area = (
        (evaluation[2] - evaluation[0]) * (evaluation[3] - evaluation[1])
    )
    if evaluation_area <= 0:
        return 0.0
    intersection_area = (
        (intersection[2] - intersection[0])
        * (intersection[3] - intersection[1])
    )
    return min(1.0, max(0.0, intersection_area / evaluation_area))


def _inside_bbox(lat: float, lon: float, bbox: CoverageBounds | None) -> bool:
    return bbox is None or (
        bbox[0] <= float(lon) <= bbox[2]
        and bbox[1] <= float(lat) <= bbox[3]
    )


def _grid_cell(lat: float, lon: float, grid_deg: float) -> tuple[int, int]:
    return (math.floor(float(lat) / grid_deg), math.floor(float(lon) / grid_deg))


def _occupied_cells(
    observations: Iterable,
    grid_deg: float,
    coverage_bbox: CoverageBounds | None = None,
) -> set[tuple[int, int]]:
    cells: set[tuple[int, int]] = set()
    for observation in observations:
        points = tuple(getattr(observation, "footprint_points", ()) or ())
        if not points:
            points = ((observation.lat, observation.lon),)
        cells.update(
            _grid_cell(lat, lon, grid_deg)
            for lat, lon in points
            if _inside_bbox(lat, lon, coverage_bbox)
        )
    return cells


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _within_evaluation_area(observations: Sequence, center, radius_km) -> list:
    if center is None or radius_km is None:
        return list(observations)
    return [
        observation for observation in observations
        if _haversine_km(
            center[0], center[1], observation.lat, observation.lon
        ) <= radius_km
    ]


@dataclass(frozen=True, slots=True)
class CalibrationSnapshot:
    timestamp: float
    region_id: str
    source_a: str
    source_b: str
    reference_source: str
    primary_cells: int
    reference_cells: int
    overlap_cells: int
    false_positive_cells: int
    missed_cells: int
    precision: float | None
    recall: float | None
    f1_score: float | None
    shared_coverage_fraction: float

    def as_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "region_id": self.region_id,
            "source_a": self.source_a,
            "source_b": self.source_b,
            "provider_pair": f"{self.source_a}<->{self.source_b}",
            "reference_source": self.reference_source,
            "primary_cells": self.primary_cells,
            "reference_cells": self.reference_cells,
            "overlap_cells": self.overlap_cells,
            "false_positive_cells": self.false_positive_cells,
            "missed_cells": self.missed_cells,
            "precision": self.precision,
            "recall": self.recall,
            "f1_score": self.f1_score,
            "shared_coverage_fraction": self.shared_coverage_fraction,
        }


@dataclass(frozen=True, slots=True)
class _CalibrationFrame:
    observations: tuple
    coverage_bbox: CoverageBounds | None
    evaluation_bbox: CoverageBounds | None


class RadarCalibrationObserver:
    """Verzamel begrensde, uitlegbare vergelijkingsscores per referentiebron."""

    def __init__(
        self,
        *,
        grid_deg: float = GRID_DEG,
        max_reference_age_s: float = MAX_REFERENCE_AGE_S,
        history_size: int = HISTORY_SIZE,
        evaluation_center: tuple[float, float] | None = None,
        evaluation_radius_km: float | None = None,
    ) -> None:
        self.grid_deg = float(grid_deg)
        self.max_reference_age_s = float(max_reference_age_s)
        self._history: deque[CalibrationSnapshot] = deque(maxlen=history_size)
        self.evaluation_center = evaluation_center
        self.evaluation_radius_km = evaluation_radius_km
        self._frames: dict[str, dict[str, dict[int, _CalibrationFrame]]] = {}
        self._matched: set[tuple[str, str, str, int]] = set()
        self._collection_frames: dict[tuple[str, str, int], tuple] = {}
        self._collection_comparisons: list[tuple] = []

    @staticmethod
    def _nominal_minute(timestamp: float) -> int:
        return int(round(float(timestamp) / 60.0))

    def record_primary_frame(self, observations: Sequence, timestamp: float) -> int:
        """Compatibiliteitswrapper voor de vroegere OPERA-hoofdbron."""
        return self.record_frame(
            observations, source="opera", timestamp=timestamp, region_id="legacy"
        )

    def record_reference_frame(
        self, observations: Sequence, *, source: str, timestamp: float
    ) -> int:
        """Compatibiliteitswrapper voor bestaande aanroepen en automations."""
        return self.record_frame(
            observations, source=source, timestamp=timestamp, region_id="legacy"
        )

    def record_frame(
        self,
        observations: Sequence,
        *,
        source: str,
        timestamp: float,
        region_id: str,
        evaluation_center: tuple[float, float] | None = None,
        evaluation_radius_km: float | None = None,
        coverage_bbox: CoverageBounds | None = None,
    ) -> int:
        """Registreer één bronframe en vergelijk het met alle gelijktijdige bronnen."""
        minute = self._nominal_minute(timestamp)
        center = evaluation_center or self.evaluation_center
        radius = (
            evaluation_radius_km
            if evaluation_radius_km is not None
            else self.evaluation_radius_km
        )
        evaluation_bbox = _evaluation_bbox(center, radius)
        effective_coverage = (
            tuple(float(value) for value in coverage_bbox)
            if coverage_bbox is not None
            else evaluation_bbox
        )
        coverage_fraction = _bbox_fraction(
            effective_coverage, evaluation_bbox
        )
        frame = tuple(
            observation
            for observation in _within_evaluation_area(
                observations, center, radius
            )
            if _inside_bbox(
                observation.lat, observation.lon, effective_coverage
            )
        )
        points: dict[tuple[int, int], list[float | int | None]] = {}
        for observation in frame:
            footprint = tuple(getattr(observation, "footprint_points", ()) or ())
            if not footprint:
                footprint = ((observation.lat, observation.lon),)
            intensity = getattr(observation, "intensity", None)
            quality = getattr(observation, "quality", None)
            for lat, lon in footprint:
                if not _inside_bbox(lat, lon, effective_coverage):
                    continue
                cell = _grid_cell(lat, lon, self.grid_deg)
                current = points.setdefault(cell, [None, None, 0])
                if intensity is not None:
                    current[0] = max(float(intensity), current[0] or float("-inf"))
                if quality is not None:
                    current[1] = max(float(quality), current[1] or float("-inf"))
                current[2] += 1
        collected_at = time.time()
        self._collection_frames[(str(region_id), str(source), minute)] = (
            str(region_id), str(source), minute, float(timestamp), collected_at,
            self.grid_deg, len(frame), len(points),
            *(
                effective_coverage
                if effective_coverage is not None
                else (None, None, None, None)
            ),
            round(coverage_fraction, 6),
            tuple(
                (lat, lon, values[0], values[1], values[2])
                for (lat, lon), values in points.items()
            ),
        )
        self._frames.setdefault(str(region_id), {}).setdefault(str(source), {})[
            minute
        ] = _CalibrationFrame(
            observations=frame,
            coverage_bbox=effective_coverage,
            evaluation_bbox=evaluation_bbox,
        )
        matched = self._match_minute(str(region_id), minute, str(source))
        self._prune_frames(minute)
        return matched

    def _match_minute(self, region_id: str, minute: int, source: str) -> int:
        region_frames = self._frames.get(region_id, {})
        if minute not in region_frames.get(source, {}):
            return 0
        matched = 0
        for other_source, frames in region_frames.items():
            if other_source == source or minute not in frames:
                continue
            source_a, source_b = sorted((source, other_source))
            key = (region_id, source_a, source_b, minute)
            if key in self._matched:
                continue
            frame_timestamp = minute * 60.0
            frame_a = region_frames[source_a][minute]
            frame_b = region_frames[source_b][minute]
            evaluation_bbox = (
                frame_a.evaluation_bbox or frame_b.evaluation_bbox
            )
            shared_bbox = _bbox_intersection(
                frame_a.coverage_bbox, frame_b.coverage_bbox
            )
            shared_fraction = _bbox_fraction(
                shared_bbox, evaluation_bbox
            )
            snapshot = self.observe(
                frame_a.observations, frame_b.observations,
                reference_source=source_b,
                reference_timestamp=frame_timestamp,
                now=frame_timestamp,
                region_id=region_id,
                source_a=source_a,
                source_b=source_b,
                comparison_bbox=shared_bbox,
                shared_coverage_fraction=shared_fraction,
            )
            self._matched.add(key)
            if snapshot is not None:
                matched += 1
        return matched

    def _prune_frames(self, newest_minute: int) -> None:
        cutoff = newest_minute - FRAME_HISTORY_MINUTES
        for region_id, sources in tuple(self._frames.items()):
            for source, frames in tuple(sources.items()):
                sources[source] = {
                    minute: frame for minute, frame in frames.items()
                    if minute >= cutoff
                }
            self._frames[region_id] = {
                source: frames for source, frames in sources.items() if frames
            }
        self._frames = {
            region_id: sources for region_id, sources in self._frames.items() if sources
        }
        self._matched = {key for key in self._matched if key[3] >= cutoff}

    def observe(
        self,
        primary: Sequence,
        reference: Sequence,
        *,
        reference_source: str,
        reference_timestamp: float | None,
        evaluation_center: tuple[float, float] | None = None,
        evaluation_radius_km: float | None = None,
        now: float | None = None,
        region_id: str = "legacy",
        source_a: str = "primary",
        source_b: str | None = None,
        comparison_bbox: CoverageBounds | None = None,
        shared_coverage_fraction: float = 1.0,
    ) -> CalibrationSnapshot | None:
        """Vergelijk gelijktijdige nat/droog-bezetting op een gedeeld rooster."""
        current = time.time() if now is None else float(now)
        if reference_timestamp is None:
            return None
        if abs(current - float(reference_timestamp)) > self.max_reference_age_s:
            return None
        if (
            float(shared_coverage_fraction)
            < MIN_SHARED_COVERAGE_FRACTION
        ):
            return None

        primary_cells = _occupied_cells(
            _within_evaluation_area(
                primary, evaluation_center, evaluation_radius_km
            ),
            self.grid_deg,
            comparison_bbox,
        )
        reference_cells = _occupied_cells(
            _within_evaluation_area(
                reference, evaluation_center, evaluation_radius_km
            ),
            self.grid_deg,
            comparison_bbox,
        )
        overlap = primary_cells & reference_cells
        false_positive = primary_cells - reference_cells
        missed = reference_cells - primary_cells

        precision = (
            len(overlap) / len(primary_cells) if primary_cells else None
        )
        recall = (
            len(overlap) / len(reference_cells) if reference_cells else None
        )
        f1 = None
        if precision is not None and recall is not None and precision + recall:
            f1 = 2 * precision * recall / (precision + recall)

        snapshot = CalibrationSnapshot(
            timestamp=current,
            region_id=region_id,
            source_a=source_a,
            source_b=source_b or reference_source,
            reference_source=reference_source,
            primary_cells=len(primary_cells),
            reference_cells=len(reference_cells),
            overlap_cells=len(overlap),
            false_positive_cells=len(false_positive),
            missed_cells=len(missed),
            precision=round(precision, 3) if precision is not None else None,
            recall=round(recall, 3) if recall is not None else None,
            f1_score=round(f1, 3) if f1 is not None else None,
            shared_coverage_fraction=round(
                float(shared_coverage_fraction), 3
            ),
        )
        self._history.append(snapshot)
        self._collection_comparisons.append((
            region_id, snapshot.source_a, snapshot.source_b,
            self._nominal_minute(current), current,
            snapshot.primary_cells, snapshot.reference_cells,
            snapshot.overlap_cells, snapshot.false_positive_cells,
            snapshot.missed_cells, snapshot.precision, snapshot.recall,
            snapshot.f1_score, snapshot.shared_coverage_fraction,
        ))
        return snapshot

    def drain_collection_batch(self) -> dict:
        """Neem alle nog niet weggeschreven records atomair uit de observer."""
        batch = {
            "frames": tuple(self._collection_frames.values()),
            "comparisons": tuple(self._collection_comparisons),
        }
        self._collection_frames.clear()
        self._collection_comparisons.clear()
        return batch

    def restore_collection_batch(self, batch: dict) -> None:
        """Plaats een mislukte schrijfbatch terug zodat data niet verloren gaat."""
        for frame in batch.get("frames", ()):
            self._collection_frames[tuple(frame[:3])] = frame
        self._collection_comparisons[:0] = list(batch.get("comparisons", ()))

    def diagnostics(self) -> dict:
        """Geef laatste en gemiddelde score; nooit operationele instellingen."""
        if not self._history:
            return {
                "mode": "observerend",
                "samples": 0,
                "status": "wacht_op_gelijktijdige_frames",
                "grid_deg": self.grid_deg,
                "synchronisatie": "exacte_nominale_minuut",
                "pending_frames": sum(
                    len(frames)
                    for sources in self._frames.values()
                    for frames in sources.values()
                ),
                "minimum_shared_coverage_fraction": (
                    MIN_SHARED_COVERAGE_FRACTION
                ),
            }
        latest = self._history[-1]
        comparable = [item for item in self._history if item.f1_score is not None]
        pair_scores: dict[str, list[float]] = {}
        for item in comparable:
            pair = f"{item.source_a}<->{item.source_b}"
            pair_scores.setdefault(pair, []).append(item.f1_score)
        return {
            "mode": "observerend",
            "samples": len(self._history),
            "comparable_samples": len(comparable),
            "status": "meting_beschikbaar",
            "grid_deg": self.grid_deg,
            "synchronisatie": "exacte_nominale_minuut",
            "pending_frames": sum(
                len(frames)
                for sources in self._frames.values()
                for frames in sources.values()
            ),
            "provider_pairs": {
                pair: {
                    "samples": len(scores),
                    "mean_f1_score": round(sum(scores) / len(scores), 3),
                }
                for pair, scores in sorted(pair_scores.items())
            },
            "latest": latest.as_dict(),
            "mean_precision": (
                round(sum(item.precision for item in comparable) / len(comparable), 3)
                if comparable else None
            ),
            "mean_recall": (
                round(sum(item.recall for item in comparable) / len(comparable), 3)
                if comparable else None
            ),
            "mean_f1_score": (
                round(sum(item.f1_score for item in comparable) / len(comparable), 3)
                if comparable else None
            ),
            "changes_filtering": False,
            "minimum_shared_coverage_fraction": (
                MIN_SHARED_COVERAGE_FRACTION
            ),
        }
