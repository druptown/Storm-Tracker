"""Lossless shadow graph for CellDetection association candidates.

Every detection pair across two consecutive frames becomes an auditable
candidate.  This module deliberately performs no hard gating and makes no
identity decision.  Its geometric score is an uncalibrated ranking feature,
not a probability.  Global assignment, birth/death priors, shape/intensity
features, optical-flow prediction and split/merge hypotheses belong to the
next association layer.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Iterable

from .cell_track import CellDetection


ASSOCIATION_GRAPH_ALGORITHM = "stv3-celltrack-candidate-graph-v1"
_FRAME_TIME_TOLERANCE_SECONDS = 1e-6


@dataclass(frozen=True, slots=True)
class AssociationCandidate:
    """One retained previous-detection to current-detection hypothesis."""

    previous_detection_id: str
    current_detection_id: str
    previous_source: str
    current_source: str
    elapsed_seconds: float
    displacement_x_m: float
    displacement_y_m: float
    displacement_m: float
    interval_speed_kmh: float
    combined_position_sigma_m: float
    standardized_displacement: float
    gaussian_geometric_nll: float
    previous_rank: int = 0
    current_rank: int = 0


@dataclass(frozen=True, slots=True)
class DetectionAssociationGraph:
    """Complete bipartite candidate graph for two immutable radar frames."""

    analysis_grid_id: str
    previous_valid_at: float
    current_valid_at: float
    elapsed_seconds: float
    motion_sigma_m_s: float
    previous_detection_ids: tuple[str, ...]
    current_detection_ids: tuple[str, ...]
    candidates: tuple[AssociationCandidate, ...]
    algorithm_version: str = ASSOCIATION_GRAPH_ALGORITHM

    @property
    def expected_candidate_count(self) -> int:
        return len(self.previous_detection_ids) * len(
            self.current_detection_ids
        )

    @property
    def is_complete(self) -> bool:
        return len(self.candidates) == self.expected_candidate_count


def build_detection_association_graph(
    previous_detections: Iterable[CellDetection],
    current_detections: Iterable[CellDetection],
    *,
    motion_sigma_m_s: float,
) -> DetectionAssociationGraph:
    """Build every candidate edge without a displacement or speed reject.

    ``motion_sigma_m_s`` is a caller-supplied one-standard-deviation growth
    rate for unmodelled cell motion between frames.  It is deliberately
    required: products, grids and weather regimes must not inherit a hidden
    universal motion threshold.
    """

    if not math.isfinite(motion_sigma_m_s) or motion_sigma_m_s < 0.0:
        raise ValueError(
            "motion_sigma_m_s moet niet-negatief en eindig zijn"
        )

    previous = _validated_frame(previous_detections, "vorige")
    current = _validated_frame(current_detections, "huidige")
    previous_time = previous[0].valid_at
    current_time = current[0].valid_at
    if current_time <= previous_time:
        raise ValueError(
            "het huidige detectieframe moet meteorologisch later geldig zijn"
        )

    grid_id = previous[0].analysis_grid_id
    if current[0].analysis_grid_id != grid_id:
        raise ValueError(
            "opeenvolgende detectieframes moeten hetzelfde analysis grid delen"
        )

    elapsed_seconds = current_time - previous_time
    candidates: list[AssociationCandidate] = []
    for old in previous:
        for new in current:
            dx = new.x_m - old.x_m
            dy = new.y_m - old.y_m
            displacement = math.hypot(dx, dy)
            motion_sigma = motion_sigma_m_s * elapsed_seconds
            variance = (
                old.position_sigma_m**2
                + new.position_sigma_m**2
                + motion_sigma**2
            )
            combined_sigma = math.sqrt(variance)
            standardized = displacement / combined_sigma
            # Isotropic 2-D Gaussian negative log density. It is useful only
            # as a deterministic geometric ranking feature until calibrated
            # priors, clutter/birth terms and other descriptors are present.
            nll = (
                math.log(2.0 * math.pi * variance)
                + 0.5 * standardized**2
            )
            if not all(
                math.isfinite(value)
                for value in (
                    dx,
                    dy,
                    displacement,
                    standardized,
                    nll,
                )
            ):
                raise ValueError(
                    "detectiecoordinaten overschrijden het numerieke bereik"
                )
            candidates.append(
                AssociationCandidate(
                    previous_detection_id=old.detection_id,
                    current_detection_id=new.detection_id,
                    previous_source=old.source,
                    current_source=new.source,
                    elapsed_seconds=elapsed_seconds,
                    displacement_x_m=dx,
                    displacement_y_m=dy,
                    displacement_m=displacement,
                    interval_speed_kmh=(
                        displacement / elapsed_seconds * 3.6
                    ),
                    combined_position_sigma_m=combined_sigma,
                    standardized_displacement=standardized,
                    gaussian_geometric_nll=nll,
                )
            )

    previous_ranks: dict[tuple[str, str], int] = {}
    current_ranks: dict[tuple[str, str], int] = {}
    for old in previous:
        ordered = sorted(
            (
                candidate
                for candidate in candidates
                if candidate.previous_detection_id == old.detection_id
            ),
            key=lambda candidate: (
                candidate.gaussian_geometric_nll,
                candidate.current_detection_id,
            ),
        )
        for rank, candidate in enumerate(ordered, start=1):
            previous_ranks[
                (
                    candidate.previous_detection_id,
                    candidate.current_detection_id,
                )
            ] = rank
    for new in current:
        ordered = sorted(
            (
                candidate
                for candidate in candidates
                if candidate.current_detection_id == new.detection_id
            ),
            key=lambda candidate: (
                candidate.gaussian_geometric_nll,
                candidate.previous_detection_id,
            ),
        )
        for rank, candidate in enumerate(ordered, start=1):
            current_ranks[
                (
                    candidate.previous_detection_id,
                    candidate.current_detection_id,
                )
            ] = rank

    ranked = tuple(
        replace(
            candidate,
            previous_rank=previous_ranks[
                (
                    candidate.previous_detection_id,
                    candidate.current_detection_id,
                )
            ],
            current_rank=current_ranks[
                (
                    candidate.previous_detection_id,
                    candidate.current_detection_id,
                )
            ],
        )
        for candidate in sorted(
            candidates,
            key=lambda item: (
                item.previous_detection_id,
                item.current_detection_id,
            ),
        )
    )
    graph = DetectionAssociationGraph(
        analysis_grid_id=grid_id,
        previous_valid_at=previous_time,
        current_valid_at=current_time,
        elapsed_seconds=elapsed_seconds,
        motion_sigma_m_s=motion_sigma_m_s,
        previous_detection_ids=tuple(
            detection.detection_id for detection in previous
        ),
        current_detection_ids=tuple(
            detection.detection_id for detection in current
        ),
        candidates=ranked,
    )
    if not graph.is_complete:
        raise AssertionError("associationgraph verloor een detectiekandidaat")
    return graph


def _validated_frame(
    detections: Iterable[CellDetection],
    label: str,
) -> tuple[CellDetection, ...]:
    values = tuple(detections)
    if not values:
        raise ValueError(f"het {label} detectieframe mag niet leeg zijn")
    if any(not isinstance(item, CellDetection) for item in values):
        raise TypeError(
            f"het {label} detectieframe vereist CellDetection-objecten"
        )
    identifiers = [item.detection_id for item in values]
    if len(set(identifiers)) != len(identifiers):
        raise ValueError(
            f"het {label} detectieframe bevat dubbele detection_id"
        )
    times = [item.valid_at for item in values]
    if max(times) - min(times) > _FRAME_TIME_TOLERANCE_SECONDS:
        raise ValueError(
            f"het {label} detectieframe bevat meerdere geldigheidstijden"
        )
    grids = {item.analysis_grid_id for item in values}
    if len(grids) != 1:
        raise ValueError(
            f"het {label} detectieframe bevat meerdere analysis grids"
        )
    return tuple(sorted(values, key=lambda item: item.detection_id))
