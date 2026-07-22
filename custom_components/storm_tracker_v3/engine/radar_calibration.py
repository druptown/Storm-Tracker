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


def _grid_cell(lat: float, lon: float, grid_deg: float) -> tuple[int, int]:
    return (math.floor(float(lat) / grid_deg), math.floor(float(lon) / grid_deg))


def _occupied_cells(observations: Iterable, grid_deg: float) -> set[tuple[int, int]]:
    cells: set[tuple[int, int]] = set()
    for observation in observations:
        points = tuple(getattr(observation, "footprint_points", ()) or ())
        if not points:
            points = ((observation.lat, observation.lon),)
        cells.update(_grid_cell(lat, lon, grid_deg) for lat, lon in points)
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
        }


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
        self._frames: dict[str, dict[str, dict[int, tuple]]] = {}
        self._matched: set[tuple[str, str, str, int]] = set()

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
    ) -> int:
        """Registreer één bronframe en vergelijk het met alle gelijktijdige bronnen."""
        minute = self._nominal_minute(timestamp)
        center = evaluation_center or self.evaluation_center
        radius = (
            evaluation_radius_km
            if evaluation_radius_km is not None
            else self.evaluation_radius_km
        )
        frame = tuple(_within_evaluation_area(observations, center, radius))
        self._frames.setdefault(str(region_id), {}).setdefault(str(source), {})[
            minute
        ] = frame
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
            self.observe(
                region_frames[source_a][minute], region_frames[source_b][minute],
                reference_source=source_b,
                reference_timestamp=frame_timestamp,
                now=frame_timestamp,
                region_id=region_id,
                source_a=source_a,
                source_b=source_b,
            )
            self._matched.add(key)
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
    ) -> CalibrationSnapshot | None:
        """Vergelijk gelijktijdige nat/droog-bezetting op een gedeeld rooster."""
        current = time.time() if now is None else float(now)
        if reference_timestamp is None:
            return None
        if abs(current - float(reference_timestamp)) > self.max_reference_age_s:
            return None

        primary_cells = _occupied_cells(
            _within_evaluation_area(
                primary, evaluation_center, evaluation_radius_km
            ),
            self.grid_deg,
        )
        reference_cells = _occupied_cells(
            _within_evaluation_area(
                reference, evaluation_center, evaluation_radius_km
            ),
            self.grid_deg,
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
        )
        self._history.append(snapshot)
        return snapshot

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
        }
