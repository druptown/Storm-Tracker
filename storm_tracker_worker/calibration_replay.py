"""Read-only replay inventory and evaluation for calibration radar overlays.

This module is deliberately offline-only.  It opens an existing calibration
database through SQLite's read-only URI mode, decodes the losslessly stored
``zlib-json-v1`` overlays and adapts their georeferenced precipitation runs to
the shadow ``CellDetection`` and association-candidate primitives.

It never imports Home Assistant startup code, publishes MQTT messages or
updates operational tracking state.  Association assignments are supplied by
the caller: the complete candidate graph is evidence, not an identity decision.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
import hashlib
import itertools
import json
import math
from pathlib import Path
import sqlite3
import statistics
from typing import Callable, Iterable, Mapping, Sequence
import zlib

from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.association_features import (
    AssociationFeatureVector,
    build_association_feature_table,
)
from .tracking_core.engine.association_graph import (
    DetectionAssociationGraph,
    build_detection_association_graph,
)
from .tracking_core.engine.cell_track import (
    CellDetection,
    estimate_preliminary_cell_track,
)
from .tracking_core.engine.global_assignment import (
    AssignmentEdgeCost,
    GlobalAssignmentProblem,
    solve_global_assignment_with_fallback,
)
from .tracking_core.engine.hierarchical_detection import (
    HierarchicalCellDetection,
    HierarchicalDetectionFrame,
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
    detect_hierarchical_cells,
)
from .tracking_core.engine.motion_prediction import (
    MotionPrediction,
    OpticalFlowSample,
    predict_with_optical_flow,
)
from .tracking_core.engine.motion_identity_evidence import (
    MotionEvidenceRecord,
    ShadowIdentityFrameAssessment,
    assess_shadow_identity_frame,
    unassessed_motion_evidence,
)
from .tracking_core.engine.track_motion_prior import (
    Covariance4D,
    TrackMotionPrior,
    build_two_position_track_prior,
)


READ_ONLY_CONNECTION_MODE = "sqlite-uri-mode-ro-query-only"
OVERLAY_ENCODING = "zlib-json-v1"
DETECTION_ADAPTER_ALGORITHM = "stv3-overlay-run-components-v1"
NORMALIZED_CACHE_KIND = "storm_tracker_normalized_grid_cache_not_raw"
NORMALIZED_CACHE_ADAPTER = "stv3-normalized-cache-hierarchical-adapter-v1"
SHADOW_ASSIGNMENT_ALGORITHM = "stv3-offline-bounded-shadow-assignment-v3"
MAX_COMPRESSED_FRAME_BYTES = 64 * 1024 * 1024
MAX_DECODED_FRAME_BYTES = 256 * 1024 * 1024
_REQUIRED_COLUMNS = frozenset({
    "region_id",
    "source",
    "nominal_timestamp",
    "saved_at",
    "encoding",
    "payload",
})
_TIMESTAMP_BUCKET_SECONDS = 60.0
_GEOMETRY_EPSILON_DEGREES = 1e-9


class CalibrationReplayError(RuntimeError):
    """Raised when the database contract itself cannot be replayed safely."""


@dataclass(frozen=True, slots=True)
class OriginalRadarFrame:
    """One successfully decoded row from ``original_radar_frames``."""

    region_id: str
    source: str
    nominal_timestamp: int
    saved_at: float
    encoding: str
    compressed_bytes: int
    decoded_bytes: int
    overlay: dict


@dataclass(frozen=True, slots=True)
class FrameDecodeIssue:
    """A row that could not safely become an ``OriginalRadarFrame``."""

    region_id: str
    source: str
    nominal_timestamp: int
    encoding: str
    compressed_bytes: int
    reason: str


@dataclass(frozen=True, slots=True)
class CalibrationReplayCorpus:
    """Immutable result of one read-only database scan."""

    database_path: str
    connection_mode: str
    frames: tuple[OriginalRadarFrame, ...]
    decode_issues: tuple[FrameDecodeIssue, ...]
    operational_state_mutated: bool = False

    def to_dict(self) -> dict:
        return {
            "database_path": self.database_path,
            "connection_mode": self.connection_mode,
            "frame_count": len(self.frames),
            "decode_issue_count": len(self.decode_issues),
            "groups": sorted({
                (frame.region_id, frame.source) for frame in self.frames
            }),
            "operational_state_mutated": self.operational_state_mutated,
        }


@dataclass(frozen=True, slots=True)
class NormalizedCacheFrame:
    """One validated ``pysteps_frame_cache`` analysis-grid view.

    This is not raw provider data.  ``cells`` are Storm Tracker's normalized
    sparse intensity classes on a fixed geographic grid.
    """

    region_id: str
    source: str
    nominal_timestamp: int
    saved_at: float
    bounds: tuple[float, float, float, float]
    grid_deg: float
    height: int
    width: int
    cells: tuple[tuple[int, int, float], ...]
    input_runs: int
    data_kind: str = NORMALIZED_CACHE_KIND

    @property
    def geometry_key(self) -> tuple:
        return (
            self.bounds,
            self.grid_deg,
            self.height,
            self.width,
        )

    @property
    def wet_cell_count(self) -> int:
        return len(self.cells)


@dataclass(frozen=True, slots=True)
class NormalizedCacheIssue:
    region_id: str
    source: str
    nominal_timestamp: int
    reason: str


@dataclass(frozen=True, slots=True)
class NormalizedCacheCorpus:
    database_path: str
    connection_mode: str
    frames: tuple[NormalizedCacheFrame, ...]
    issues: tuple[NormalizedCacheIssue, ...]
    data_kind: str = NORMALIZED_CACHE_KIND
    operational_state_mutated: bool = False


@dataclass(frozen=True, slots=True)
class NormalizedCacheSequence:
    sequence_id: str
    region_id: str
    source: str
    cadence_seconds: int | None
    frames: tuple[NormalizedCacheFrame, ...]
    minimum_frame_count: int

    @property
    def frame_count(self) -> int:
        return len(self.frames)

    @property
    def wet_frame_count(self) -> int:
        return sum(frame.wet_cell_count > 0 for frame in self.frames)

    @property
    def qualifies(self) -> bool:
        return self.frame_count >= self.minimum_frame_count

    @property
    def starts_at(self) -> int:
        return self.frames[0].nominal_timestamp

    @property
    def ends_at(self) -> int:
        return self.frames[-1].nominal_timestamp

    def to_dict(self) -> dict:
        return {
            "sequence_id": self.sequence_id,
            "region_id": self.region_id,
            "source": self.source,
            "cadence_seconds": self.cadence_seconds,
            "frame_count": self.frame_count,
            "wet_frame_count": self.wet_frame_count,
            "starts_at": self.starts_at,
            "ends_at": self.ends_at,
            "minimum_frame_count": self.minimum_frame_count,
            "qualifies": self.qualifies,
            "data_kind": NORMALIZED_CACHE_KIND,
        }


@dataclass(frozen=True, slots=True)
class NormalizedCacheInventory:
    region_id: str
    source: str
    cadence_seconds: int | None
    cadence_support_ratio: float | None
    total_frames: int
    issue_count: int
    gaps: tuple[ReplayGap, ...]
    sequences: tuple[NormalizedCacheSequence, ...]
    data_kind: str = NORMALIZED_CACHE_KIND

    @property
    def qualifying_sequences(self) -> tuple[NormalizedCacheSequence, ...]:
        return tuple(sequence for sequence in self.sequences if sequence.qualifies)

    def to_dict(self) -> dict:
        return {
            "region_id": self.region_id,
            "source": self.source,
            "cadence_seconds": self.cadence_seconds,
            "cadence_support_ratio": self.cadence_support_ratio,
            "total_frames": self.total_frames,
            "issue_count": self.issue_count,
            "gaps": [asdict(gap) for gap in self.gaps],
            "sequences": [sequence.to_dict() for sequence in self.sequences],
            "qualifying_sequence_count": len(self.qualifying_sequences),
            "data_kind": self.data_kind,
        }


@dataclass(frozen=True, slots=True)
class NormalizedRasterTransform:
    """Audit of a native cache expansion or bounded spatial analysis view."""

    mode: str
    method: str
    source_height: int
    source_width: int
    output_height: int
    output_width: int
    source_pixel_count: int
    output_pixel_count: int
    maximum_source_grid_cells: int
    maximum_analysis_pixels: int
    source_bounds_wgs84: tuple[float, float, float, float]
    row_scale: float
    column_scale: float
    source_x_resolution_m: float
    source_y_resolution_m: float
    output_x_resolution_m: float
    output_y_resolution_m: float
    requested_bounds_width_m: float
    requested_bounds_height_m: float
    cache_grid_width_m: float
    cache_grid_height_m: float
    cache_grid_east_padding_m: float
    cache_grid_south_padding_m: float
    row_bin_source_index_bounds: tuple[tuple[int, int], ...]
    column_bin_source_index_bounds: tuple[tuple[int, int], ...]
    extent_preserved: bool
    extent_reference: str
    timestamps_preserved: bool
    lossless_cache_value_expansion: bool
    source_nodata_mask_available: bool
    provider_nodata_valid_fraction: float | None
    cache_value_finite_fraction: float
    output_none_count: int
    sparse_absence_semantics: str
    aggregation: str
    analysis_biases: tuple[str, ...]
    fingerprint: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class NormalizedRasterReplayView:
    """Immutable raster frames plus the transform that produced them."""

    sequence_id: str
    source: str
    frames: tuple[NormalizedRasterFrame, ...]
    transform: NormalizedRasterTransform
    source_wet_cell_count: int
    data_kind: str = NORMALIZED_CACHE_KIND
    operational_state_mutated: bool = False

    def to_dict(self) -> dict:
        return {
            "sequence_id": self.sequence_id,
            "source": self.source,
            "frame_count": len(self.frames),
            "frame_ids": [item.frame_id for item in self.frames],
            "valid_at": [item.valid_at for item in self.frames],
            "analysis_grid_id": self.frames[0].grid.grid_id,
            "source_wet_cell_count": self.source_wet_cell_count,
            "transform": self.transform.to_dict(),
            "data_kind": self.data_kind,
            "operational_state_mutated": self.operational_state_mutated,
        }


@dataclass(frozen=True, slots=True)
class FrameAssessment:
    """Structural and precipitation usability of one decoded overlay."""

    frame: OriginalRadarFrame
    frame_usable: bool
    precipitation_usable: bool
    run_count: int
    valid_wet_run_count: int
    malformed_run_count: int
    timestamp_delta_seconds: float | None
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ReplayGap:
    """A measured discontinuity between otherwise usable frames."""

    after_timestamp: int
    before_timestamp: int
    elapsed_seconds: int
    estimated_missing_frames: int | None


@dataclass(frozen=True, slots=True)
class ReplaySequence:
    """One structurally usable, cadence-contiguous frame sequence."""

    sequence_id: str
    region_id: str
    source: str
    cadence_seconds: int | None
    frames: tuple[OriginalRadarFrame, ...]
    assessments: tuple[FrameAssessment, ...]
    minimum_frame_count: int

    @property
    def frame_count(self) -> int:
        return len(self.frames)

    @property
    def precipitation_usable_frame_count(self) -> int:
        return sum(
            assessment.precipitation_usable
            for assessment in self.assessments
        )

    @property
    def starts_at(self) -> int:
        return self.frames[0].nominal_timestamp

    @property
    def ends_at(self) -> int:
        return self.frames[-1].nominal_timestamp

    @property
    def duration_seconds(self) -> int:
        return self.ends_at - self.starts_at

    @property
    def qualifies(self) -> bool:
        return self.frame_count >= self.minimum_frame_count

    def to_dict(self) -> dict:
        return {
            "sequence_id": self.sequence_id,
            "region_id": self.region_id,
            "source": self.source,
            "cadence_seconds": self.cadence_seconds,
            "frame_count": self.frame_count,
            "precipitation_usable_frame_count":
                self.precipitation_usable_frame_count,
            "starts_at": self.starts_at,
            "ends_at": self.ends_at,
            "duration_seconds": self.duration_seconds,
            "minimum_frame_count": self.minimum_frame_count,
            "qualifies": self.qualifies,
        }


@dataclass(frozen=True, slots=True)
class SourceReplayInventory:
    """Cadence, gaps and sequences for one region/provider combination."""

    region_id: str
    source: str
    cadence_seconds: int | None
    cadence_support_ratio: float | None
    total_decoded_frames: int
    usable_frames: int
    precipitation_usable_frames: int
    decode_issue_count: int
    unusable_frame_count: int
    gaps: tuple[ReplayGap, ...]
    sequences: tuple[ReplaySequence, ...]

    @property
    def qualifying_sequences(self) -> tuple[ReplaySequence, ...]:
        return tuple(sequence for sequence in self.sequences if sequence.qualifies)

    def to_dict(self) -> dict:
        return {
            "region_id": self.region_id,
            "source": self.source,
            "cadence_seconds": self.cadence_seconds,
            "cadence_support_ratio": self.cadence_support_ratio,
            "total_decoded_frames": self.total_decoded_frames,
            "usable_frames": self.usable_frames,
            "precipitation_usable_frames":
                self.precipitation_usable_frames,
            "decode_issue_count": self.decode_issue_count,
            "unusable_frame_count": self.unusable_frame_count,
            "gaps": [asdict(gap) for gap in self.gaps],
            "sequences": [sequence.to_dict() for sequence in self.sequences],
            "qualifying_sequence_count": len(self.qualifying_sequences),
        }


@dataclass(frozen=True, slots=True)
class MetricAnalysisGrid:
    """A fixed local metric projection used only for one offline sequence."""

    analysis_grid_id: str
    reference_latitude: float
    reference_longitude: float
    metres_per_degree_latitude: float
    metres_per_degree_longitude: float

    def project(self, latitude: float, longitude: float) -> tuple[float, float]:
        if not (
            math.isfinite(latitude)
            and math.isfinite(longitude)
            and -90.0 <= latitude <= 90.0
            and -180.0 <= longitude <= 180.0
        ):
            raise ValueError("ongeldige WGS84-coordinaat voor replayprojectie")
        return (
            (longitude - self.reference_longitude)
            * self.metres_per_degree_longitude,
            (latitude - self.reference_latitude)
            * self.metres_per_degree_latitude,
        )


@dataclass(frozen=True, slots=True)
class DetectionFrame:
    """Cell detections derived from one immutable original overlay."""

    region_id: str
    source: str
    nominal_timestamp: int
    detections: tuple[CellDetection, ...]
    valid_wet_run_count: int
    malformed_run_count: int
    adapter_algorithm: str = DETECTION_ADAPTER_ALGORITHM


@dataclass(frozen=True, slots=True)
class DetectionReplay:
    """Cell detections and complete candidate graphs for one sequence."""

    sequence_id: str
    analysis_grid: MetricAnalysisGrid
    frames: tuple[DetectionFrame, ...]
    association_graphs: tuple[DetectionAssociationGraph, ...]
    frame_pairs_without_graph: int
    candidate_count: int
    operational_state_mutated: bool = False

    @property
    def every_built_graph_complete(self) -> bool:
        return all(graph.is_complete for graph in self.association_graphs)

    @property
    def detection_count(self) -> int:
        return sum(len(frame.detections) for frame in self.frames)

    def to_dict(self) -> dict:
        return {
            "sequence_id": self.sequence_id,
            "analysis_grid_id": self.analysis_grid.analysis_grid_id,
            "frame_count": len(self.frames),
            "detection_count": self.detection_count,
            "association_graph_count": len(self.association_graphs),
            "frame_pairs_without_graph": self.frame_pairs_without_graph,
            "candidate_count": self.candidate_count,
            "every_built_graph_complete": self.every_built_graph_complete,
            "operational_state_mutated": self.operational_state_mutated,
        }


@dataclass(frozen=True, slots=True)
class HierarchicalReplay:
    """Hierarchical detections derived from the normalized cache only."""

    sequence_id: str
    source: str
    analysis_grid: AnalysisGrid
    frames: tuple[HierarchicalDetectionFrame, ...]
    source_wet_cell_count: int
    adapter_limitations: tuple[str, ...]
    adapter_algorithm: str = NORMALIZED_CACHE_ADAPTER
    data_kind: str = NORMALIZED_CACHE_KIND
    operational_state_mutated: bool = False

    @property
    def detection_count(self) -> int:
        return sum(len(frame.detections) for frame in self.frames)

    @property
    def rain_shield_count(self) -> int:
        return sum(len(frame.rain_shields) for frame in self.frames)

    @property
    def embedded_core_count(self) -> int:
        return sum(len(frame.embedded_cores) for frame in self.frames)

    def to_dict(self) -> dict:
        return {
            "sequence_id": self.sequence_id,
            "source": self.source,
            "analysis_grid_id": self.analysis_grid.grid_id,
            "grid_shape": [
                self.analysis_grid.height,
                self.analysis_grid.width,
            ],
            "frame_count": len(self.frames),
            "source_wet_cell_count": self.source_wet_cell_count,
            "detection_count": self.detection_count,
            "rain_shield_count": self.rain_shield_count,
            "embedded_core_count": self.embedded_core_count,
            "adapter_algorithm": self.adapter_algorithm,
            "adapter_limitations": list(self.adapter_limitations),
            "data_kind": self.data_kind,
            "operational_state_mutated": self.operational_state_mutated,
        }


@dataclass(frozen=True, slots=True)
class ShadowMotionPriorAudit:
    """One prediction input used before a global frame assignment."""

    track_id: str
    previous_detection_id: str
    source_valid_at: float
    target_valid_at: float
    elapsed_seconds: float
    role: str
    method: str
    supporting_detection_ids: tuple[str, ...]
    vx_m_s: float
    vy_m_s: float
    predicted_x_m: float
    predicted_y_m: float
    predicted_position_covariance_m2: tuple[
        tuple[float, float],
        tuple[float, float],
    ]

    def to_dict(self) -> dict:
        return {
            "track_id": self.track_id,
            "previous_detection_id": self.previous_detection_id,
            "source_valid_at": self.source_valid_at,
            "target_valid_at": self.target_valid_at,
            "elapsed_seconds": self.elapsed_seconds,
            "role": self.role,
            "method": self.method,
            "supporting_detection_ids":
                list(self.supporting_detection_ids),
            "vx_m_s": self.vx_m_s,
            "vy_m_s": self.vy_m_s,
            "predicted_x_m": self.predicted_x_m,
            "predicted_y_m": self.predicted_y_m,
            "predicted_position_covariance_m2": [
                list(row)
                for row in self.predicted_position_covariance_m2
            ],
        }


@dataclass(frozen=True, slots=True)
class ShadowFrameAssignmentAudit:
    previous_valid_at: float | None
    current_valid_at: float
    previous_detection_count: int
    current_detection_count: int
    complete_feature_count: int
    solver: str
    solution_count: int
    best_total_cost: float
    alternative_cost_gap: float | None
    ambiguous: bool | None
    match_count: int
    birth_count: int
    death_count: int
    chosen_interval_speeds_kmh: tuple[float, ...]
    identity_assessment: ShadowIdentityFrameAssessment | None


@dataclass(frozen=True, slots=True)
class BoundedShadowAssignmentAudit:
    """Automatic offline assignments with explicit caller-owned policy."""

    sequence_id: str
    object_kind: str
    assignments: tuple[tuple[str, str], ...]
    frame_audits: tuple[ShadowFrameAssignmentAudit, ...]
    motion_prior_audits: tuple[ShadowMotionPriorAudit, ...]
    metrics: AssignmentMetrics
    cost_model_ids: tuple[str, ...]
    motion_prior_methods: tuple[str, ...]
    motion_prior_mode: str
    detection_evidence_sha256: str
    policy_id: str
    solution_limit: int
    maximum_top_k_current_detections: int
    ambiguity_cost_delta: float
    exact_one_best_everywhere: bool
    alternative_hypotheses_available: bool
    right_censored_track_count: int
    adapter_limitations: tuple[str, ...]
    algorithm_version: str = SHADOW_ASSIGNMENT_ALGORITHM
    operational_state_mutated: bool = False

    @property
    def match_count(self) -> int:
        return sum(item.match_count for item in self.frame_audits)

    @property
    def birth_count(self) -> int:
        return sum(item.birth_count for item in self.frame_audits)

    @property
    def death_count(self) -> int:
        return sum(item.death_count for item in self.frame_audits)

    @property
    def ambiguous_frame_pairs(self) -> int:
        return sum(item.ambiguous is True for item in self.frame_audits)

    @property
    def indeterminate_identity_match_count(self) -> int:
        return sum(
            item.identity_assessment.indeterminate_match_count
            for item in self.frame_audits
            if item.identity_assessment is not None
        )

    @property
    def supported_unvalidated_identity_match_count(self) -> int:
        return sum(
            item.identity_assessment.supported_unvalidated_match_count
            for item in self.frame_audits
            if item.identity_assessment is not None
        )

    @property
    def motion_unavailable_match_count(self) -> int:
        return sum(
            item.identity_assessment.motion_unavailable_match_count
            for item in self.frame_audits
            if item.identity_assessment is not None
        )

    def to_dict(self) -> dict:
        speeds = [
            speed
            for audit in self.frame_audits
            for speed in audit.chosen_interval_speeds_kmh
        ]
        return {
            "sequence_id": self.sequence_id,
            "object_kind": self.object_kind,
            "assignment_count": len(self.assignments),
            "track_count": self.metrics.track_count,
            "match_count": self.match_count,
            "birth_count": self.birth_count,
            "death_count": self.death_count,
            "right_censored_track_count": self.right_censored_track_count,
            "missed_detection_slots": self.metrics.missed_detection_slots,
            "unassigned_detection_count":
                self.metrics.unassigned_detection_count,
            "chosen_speed_count": len(speeds),
            "chosen_speed_median_kmh": (
                statistics.median(speeds) if speeds else None
            ),
            "chosen_speed_p95_kmh":
                _nearest_rank_percentile(speeds, 0.95),
            "chosen_speed_maximum_kmh": max(speeds) if speeds else None,
            "chosen_speed_semantics": (
                "one_best_assignment_hypothesis_interval_speed;"
                "not_verified_cell_motion"
            ),
            "assignment_semantics": (
                "one_best_complete_graph_hypothesis;"
                "not_confirmed_meteorological_identity"
            ),
            "confirmed_identity_match_count": 0,
            "indeterminate_identity_match_count":
                self.indeterminate_identity_match_count,
            "supported_unvalidated_identity_match_count":
                self.supported_unvalidated_identity_match_count,
            "motion_unavailable_match_count":
                self.motion_unavailable_match_count,
            "ambiguous_frame_pairs": self.ambiguous_frame_pairs,
            "solution_limit": self.solution_limit,
            "maximum_top_k_current_detections":
                self.maximum_top_k_current_detections,
            "alternative_hypotheses_available":
                self.alternative_hypotheses_available,
            "exact_one_best_everywhere": self.exact_one_best_everywhere,
            "cost_model_ids": list(self.cost_model_ids),
            "motion_prior_methods": list(self.motion_prior_methods),
            "motion_prior_method_counts": dict(sorted(Counter(
                item.method for item in self.motion_prior_audits
            ).items())),
            "motion_prior_role_counts": dict(sorted(Counter(
                item.role for item in self.motion_prior_audits
            ).items())),
            "motion_prior_audits": [
                item.to_dict() for item in self.motion_prior_audits
            ],
            "motion_prior_mode": self.motion_prior_mode,
            "motion_prior_extension_point": (
                "flow_sample_factory(previous_detection,current_frame)"
                " -> OpticalFlowSample"
            ),
            "stateful_prior_source": (
                "engine.track_motion_prior exact last two assigned "
                "positions/times"
                if self.motion_prior_mode == "last-two"
                else None
            ),
            "stateful_feedback_dependency": (
                self.motion_prior_mode == "last-two"
            ),
            "feedback_poisoning_observation": (
                "A wrong earlier global match becomes input to a later "
                "last-two prior; without external truth that feedback cannot "
                "be identified as right or wrong."
                if self.motion_prior_mode == "last-two"
                else None
            ),
            "detection_evidence_sha256": self.detection_evidence_sha256,
            "policy_id": self.policy_id,
            "metrics": self.metrics.to_dict(),
            "ground_truth_available": False,
            "identity_switches_measurable": False,
            "identity_frame_assessments": [
                item.identity_assessment.to_dict()
                for item in self.frame_audits
                if item.identity_assessment is not None
            ],
            "all_candidates_retained": all(
                item.identity_assessment is None
                or item.identity_assessment.all_candidates_retained
                for item in self.frame_audits
            ),
            "identity_confirmation_claimed": False,
            "speed_cap_kmh": None,
            "candidate_rejection_by_speed": False,
            "adapter_limitations": list(self.adapter_limitations),
            "algorithm_version": self.algorithm_version,
            "operational_state_mutated": self.operational_state_mutated,
        }


@dataclass(frozen=True, slots=True)
class TrackMetrics:
    """Objective interval metrics for one caller-assigned track."""

    track_id: str
    sample_count: int
    interval_count: int
    duration_seconds: float
    minimum_speed_kmh: float | None
    median_speed_kmh: float | None
    maximum_speed_kmh: float | None
    jump_count: int
    missed_detection_slots: int
    identity_switch_count: int | None
    unscored_identity_samples: int


@dataclass(frozen=True, slots=True)
class AssignmentMetrics:
    """Aggregate metrics for either a legacy or a shadow assignment."""

    label: str
    assigned_detection_count: int
    unassigned_detection_count: int
    track_count: int
    interval_count: int
    median_interval_speed_kmh: float | None
    p95_interval_speed_kmh: float | None
    maximum_interval_speed_kmh: float | None
    median_track_duration_seconds: float | None
    maximum_track_duration_seconds: float | None
    jump_speed_threshold_kmh: float
    jump_count: int
    missed_detection_slots: int
    identity_switch_count: int | None
    reference_identity_fragmentations: int | None
    tracks: tuple[TrackMetrics, ...]

    def to_dict(self) -> dict:
        result = asdict(self)
        result["tracks"] = [asdict(track) for track in self.tracks]
        return result


@dataclass(frozen=True, slots=True)
class LegacyShadowComparison:
    """Side-by-side metrics; delta is always ``shadow - legacy``."""

    sequence_id: str
    legacy: AssignmentMetrics
    shadow: AssignmentMetrics
    deltas: dict[str, float | int | None]
    identity_switches_measurable: bool
    operational_state_mutated: bool = False

    def to_dict(self) -> dict:
        return {
            "sequence_id": self.sequence_id,
            "legacy": self.legacy.to_dict(),
            "shadow": self.shadow.to_dict(),
            "deltas": dict(self.deltas),
            "delta_definition": "shadow_minus_legacy",
            "identity_switches_measurable":
                self.identity_switches_measurable,
            "operational_state_mutated": self.operational_state_mutated,
        }


@dataclass(frozen=True, slots=True)
class _OverlayRun:
    intensity: int
    points: tuple[tuple[float, float], ...]
    south: float
    north: float
    west: float
    east: float


def load_original_radar_corpus(
    database_path: str | Path,
    *,
    region_id: str | None = None,
    source: str | None = None,
    maximum_frames: int | None = None,
    maximum_compressed_frame_bytes: int = MAX_COMPRESSED_FRAME_BYTES,
    maximum_decoded_frame_bytes: int = MAX_DECODED_FRAME_BYTES,
) -> CalibrationReplayCorpus:
    """Read and decode calibration overlays without opening a write channel."""

    path = Path(database_path).expanduser().resolve(strict=True)
    if not path.is_file():
        raise CalibrationReplayError(
            f"kalibratiepad is geen bestand: {path}"
        )
    if maximum_frames is not None and (
        not isinstance(maximum_frames, int) or maximum_frames <= 0
    ):
        raise ValueError("maximum_frames moet een positief geheel getal zijn")
    if maximum_compressed_frame_bytes <= 0 or maximum_decoded_frame_bytes <= 0:
        raise ValueError("replaygroottelimieten moeten positief zijn")

    uri = f"{path.as_uri()}?mode=ro"
    frames: list[OriginalRadarFrame] = []
    issues: list[FrameDecodeIssue] = []
    try:
        database = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as err:
        raise CalibrationReplayError(
            f"kalibratiedatabase kan niet read-only worden geopend: {err}"
        ) from err
    try:
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA query_only=ON")
        table = database.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='original_radar_frames'"
        ).fetchone()
        if table is None:
            raise CalibrationReplayError(
                "original_radar_frames ontbreekt in de kalibratiedatabase"
            )
        columns = {
            str(row["name"])
            for row in database.execute(
                "PRAGMA table_info(original_radar_frames)"
            )
        }
        missing_columns = sorted(_REQUIRED_COLUMNS - columns)
        if missing_columns:
            raise CalibrationReplayError(
                "original_radar_frames mist kolommen: "
                + ", ".join(missing_columns)
            )

        clauses: list[str] = []
        parameters: list[object] = []
        if region_id is not None:
            clauses.append("region_id=?")
            parameters.append(str(region_id))
        if source is not None:
            clauses.append("source=?")
            parameters.append(str(source))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        limit = ""
        if maximum_frames is not None:
            limit = " LIMIT ?"
            parameters.append(maximum_frames)
        cursor = database.execute(
            "SELECT region_id, source, nominal_timestamp, saved_at, "
            "encoding, payload FROM original_radar_frames"
            f"{where} ORDER BY region_id, source, nominal_timestamp{limit}",
            parameters,
        )
        for row in cursor:
            saved_region = str(row["region_id"])
            saved_source = str(row["source"])
            timestamp = int(row["nominal_timestamp"])
            encoding = str(row["encoding"])
            payload = bytes(row["payload"])
            issue_base = {
                "region_id": saved_region,
                "source": saved_source,
                "nominal_timestamp": timestamp,
                "encoding": encoding,
                "compressed_bytes": len(payload),
            }
            if encoding != OVERLAY_ENCODING:
                issues.append(FrameDecodeIssue(
                    **issue_base,
                    reason=f"unsupported_encoding:{encoding}",
                ))
                continue
            if len(payload) > maximum_compressed_frame_bytes:
                issues.append(FrameDecodeIssue(
                    **issue_base,
                    reason="compressed_payload_too_large",
                ))
                continue
            try:
                decoded = _limited_zlib_decompress(
                    payload,
                    maximum_decoded_frame_bytes,
                )
                overlay = json.loads(decoded.decode("utf-8"))
                if not isinstance(overlay, dict):
                    raise ValueError("overlay_json_is_not_an_object")
            except (UnicodeDecodeError, ValueError, zlib.error) as err:
                reason = str(err) or type(err).__name__
                issues.append(FrameDecodeIssue(
                    **issue_base,
                    reason=f"decode_failed:{reason}",
                ))
                continue
            frames.append(OriginalRadarFrame(
                region_id=saved_region,
                source=saved_source,
                nominal_timestamp=timestamp,
                saved_at=float(row["saved_at"]),
                encoding=encoding,
                compressed_bytes=len(payload),
                decoded_bytes=len(decoded),
                overlay=overlay,
            ))
    except sqlite3.Error as err:
        raise CalibrationReplayError(
            f"read-only kalibratiescan mislukt: {err}"
        ) from err
    finally:
        database.close()

    return CalibrationReplayCorpus(
        database_path=str(path),
        connection_mode=READ_ONLY_CONNECTION_MODE,
        frames=tuple(frames),
        decode_issues=tuple(issues),
    )


def load_normalized_frame_cache(
    database_path: str | Path,
    *,
    region_id: str | None = None,
    source: str | None = None,
    maximum_frames: int | None = None,
) -> NormalizedCacheCorpus:
    """Load the normalized sparse grid cache through a read-only SQLite URI.

    The result is intentionally named ``normalized`` and never ``raw``:
    provider decoding, run projection and max-intensity regridding have already
    happened before these frames entered ``pysteps_frame_cache``.
    """

    path = Path(database_path).expanduser().resolve(strict=True)
    if not path.is_file():
        raise CalibrationReplayError(
            f"kalibratiepad is geen bestand: {path}"
        )
    if maximum_frames is not None and (
        not isinstance(maximum_frames, int) or maximum_frames <= 0
    ):
        raise ValueError("maximum_frames moet een positief geheel getal zijn")
    uri = f"{path.as_uri()}?mode=ro"
    try:
        database = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as err:
        raise CalibrationReplayError(
            f"genormaliseerde cache kan niet read-only worden geopend: {err}"
        ) from err
    frames: list[NormalizedCacheFrame] = []
    issues: list[NormalizedCacheIssue] = []
    try:
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA query_only=ON")
        table = database.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='pysteps_frame_cache'"
        ).fetchone()
        if table is None:
            raise CalibrationReplayError(
                "pysteps_frame_cache ontbreekt in de kalibratiedatabase"
            )
        columns = {
            str(row["name"])
            for row in database.execute(
                "PRAGMA table_info(pysteps_frame_cache)"
            )
        }
        required = {
            "region_id",
            "source",
            "nominal_timestamp",
            "saved_at",
            "frame_json",
        }
        missing = sorted(required - columns)
        if missing:
            raise CalibrationReplayError(
                "pysteps_frame_cache mist kolommen: " + ", ".join(missing)
            )
        clauses: list[str] = []
        parameters: list[object] = []
        if region_id is not None:
            clauses.append("region_id=?")
            parameters.append(str(region_id))
        if source is not None:
            clauses.append("source=?")
            parameters.append(str(source))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        limit = ""
        if maximum_frames is not None:
            limit = " LIMIT ?"
            parameters.append(maximum_frames)
        cursor = database.execute(
            "SELECT region_id, source, nominal_timestamp, saved_at, frame_json "
            f"FROM pysteps_frame_cache{where} "
            f"ORDER BY region_id, source, nominal_timestamp{limit}",
            parameters,
        )
        for row in cursor:
            saved_region = str(row["region_id"])
            saved_source = str(row["source"])
            saved_timestamp = int(row["nominal_timestamp"])
            try:
                value = json.loads(str(row["frame_json"]))
                frame = _validated_normalized_cache_frame(
                    saved_region,
                    saved_source,
                    saved_timestamp,
                    float(row["saved_at"]),
                    value,
                )
            except (KeyError, TypeError, ValueError, IndexError) as err:
                issues.append(NormalizedCacheIssue(
                    region_id=saved_region,
                    source=saved_source,
                    nominal_timestamp=saved_timestamp,
                    reason=str(err) or type(err).__name__,
                ))
                continue
            frames.append(frame)
    except sqlite3.Error as err:
        raise CalibrationReplayError(
            f"read-only genormaliseerde cachescan mislukt: {err}"
        ) from err
    finally:
        database.close()
    return NormalizedCacheCorpus(
        database_path=str(path),
        connection_mode=READ_ONLY_CONNECTION_MODE,
        frames=tuple(frames),
        issues=tuple(issues),
    )


def build_normalized_cache_inventory(
    corpus: NormalizedCacheCorpus,
    *,
    minimum_sequence_frames: int = 20,
    maximum_gap_factor: float = 1.5,
) -> tuple[NormalizedCacheInventory, ...]:
    """Find fixed-geometry, cadence-contiguous normalized frame sequences."""

    if not isinstance(minimum_sequence_frames, int) or minimum_sequence_frames < 2:
        raise ValueError("minimum_sequence_frames moet minstens twee zijn")
    if (
        not math.isfinite(maximum_gap_factor)
        or maximum_gap_factor < 1.0
    ):
        raise ValueError("maximum_gap_factor moet minstens 1,0 zijn")
    frame_groups: dict[
        tuple[str, str], list[NormalizedCacheFrame]
    ] = defaultdict(list)
    issue_groups: Counter[tuple[str, str]] = Counter()
    issue_timestamps: dict[tuple[str, str], list[int]] = defaultdict(list)
    for frame in corpus.frames:
        frame_groups[(frame.region_id, frame.source)].append(frame)
    for issue in corpus.issues:
        key = (issue.region_id, issue.source)
        issue_groups[key] += 1
        issue_timestamps[key].append(issue.nominal_timestamp)

    result: list[NormalizedCacheInventory] = []
    for region, source_name in sorted(
        set(frame_groups) | set(issue_groups)
    ):
        key = (region, source_name)
        frames = tuple(sorted(
            frame_groups[key],
            key=lambda item: item.nominal_timestamp,
        ))
        timestamps = sorted({
            *(frame.nominal_timestamp for frame in frames),
            *issue_timestamps[key],
        })
        cadence, support = _infer_cadence(timestamps)
        sequences, gaps = _split_normalized_cache_sequences(
            region,
            source_name,
            frames,
            cadence_seconds=cadence,
            minimum_sequence_frames=minimum_sequence_frames,
            maximum_gap_factor=maximum_gap_factor,
        )
        result.append(NormalizedCacheInventory(
            region_id=region,
            source=source_name,
            cadence_seconds=cadence,
            cadence_support_ratio=support,
            total_frames=len(frames),
            issue_count=issue_groups[key],
            gaps=gaps,
            sequences=sequences,
        ))
    return tuple(result)


def build_hierarchical_replay_from_normalized_cache(
    sequence: NormalizedCacheSequence,
    *,
    detector_config: HierarchicalDetectorConfig,
    maximum_grid_cells: int,
) -> HierarchicalReplay:
    """Run the hierarchical detector on cached normalized intensity classes."""

    raster_view = build_normalized_raster_replay_view(
        sequence,
        maximum_source_grid_cells=maximum_grid_cells,
        maximum_analysis_pixels=maximum_grid_cells,
    )
    return build_hierarchical_replay_from_raster_view(
        raster_view,
        detector_config=detector_config,
    )


def build_normalized_raster_replay_view(
    sequence: NormalizedCacheSequence,
    *,
    maximum_source_grid_cells: int,
    maximum_analysis_pixels: int | None = None,
) -> NormalizedRasterReplayView:
    """Expand cached cells losslessly or build an explicit bounded view.

    The cache builder initialized its dense grid to zero and persisted only
    positive cells; it did not persist the provider's nodata mask.  Sparse
    absence can therefore be expanded to cache-zero, but is never claimed as
    independently observed dry weather.  When a lower analysis pixel cap is
    supplied, deterministic spatial-bin maxima preserve the metric extent but
    the resulting view is explicitly non-native and non-lossless.
    """

    analysis_cap = (
        maximum_source_grid_cells
        if maximum_analysis_pixels is None
        else maximum_analysis_pixels
    )
    for label, value in (
        ("maximum_source_grid_cells", maximum_source_grid_cells),
        ("maximum_analysis_pixels", analysis_cap),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{label} moet een positief geheel getal zijn")
    if not isinstance(sequence, NormalizedCacheSequence):
        raise TypeError("sequence vereist NormalizedCacheSequence")
    if not sequence.frames:
        raise ValueError("genormaliseerde replaysequence mag niet leeg zijn")
    first = sequence.frames[0]
    if any(frame.geometry_key != first.geometry_key for frame in sequence.frames):
        raise ValueError("replaysequence bevat meerdere rastergeometrieën")
    source_pixels = first.height * first.width
    if source_pixels > maximum_source_grid_cells:
        raise ValueError(
            "genormaliseerd brongrid overschrijdt maximum_source_grid_cells"
        )
    output_height, output_width = _bounded_analysis_shape(
        first.height,
        first.width,
        min(source_pixels, analysis_cap),
    )
    native = (
        output_height == first.height and output_width == first.width
    )

    west, south, east, north = first.bounds
    reference_latitude = (south + north) / 2.0
    latitude_radians = math.radians(reference_latitude)
    metres_per_latitude = (
        111_132.92
        - 559.82 * math.cos(2.0 * latitude_radians)
        + 1.175 * math.cos(4.0 * latitude_radians)
        - 0.0023 * math.cos(6.0 * latitude_radians)
    )
    metres_per_longitude = (
        111_412.84 * math.cos(latitude_radians)
        - 93.5 * math.cos(3.0 * latitude_radians)
        + 0.118 * math.cos(5.0 * latitude_radians)
    )
    source_x_resolution = first.grid_deg * metres_per_longitude
    source_y_resolution = first.grid_deg * metres_per_latitude
    source_width_m = first.width * source_x_resolution
    source_height_m = first.height * source_y_resolution
    requested_bounds_width_m = (east - west) * metres_per_longitude
    requested_bounds_height_m = (north - south) * metres_per_latitude
    row_bin_bounds = _source_bin_index_bounds(
        first.height,
        output_height,
    )
    column_bin_bounds = _source_bin_index_bounds(
        first.width,
        output_width,
    )
    transform_token = {
        "adapter": "stv3-normalized-cache-raster-view-v1",
        "sequence_id": sequence.sequence_id,
        "source_shape": [first.height, first.width],
        "output_shape": [output_height, output_width],
        "source_bounds": list(first.bounds),
        "source_grid_deg": first.grid_deg,
        "maximum_source_grid_cells": maximum_source_grid_cells,
        "maximum_analysis_pixels": analysis_cap,
        "aggregation": "identity" if native else "spatial_bin_maximum",
        "row_bin_source_index_bounds": row_bin_bounds,
        "column_bin_source_index_bounds": column_bin_bounds,
        "source_nodata_mask_available": False,
        "sparse_absence_semantics":
            "zero_initialized_normalized_cache_builder",
    }
    transform_fingerprint = hashlib.sha256(
        json.dumps(
            transform_token,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    grid = AnalysisGrid(
        grid_id=(
            "offline-normalized-cache-raster-view-v1:"
            f"{transform_fingerprint[:16]}"
        ),
        crs=(
            "LOCAL_EQUIRECTANGULAR_WGS84:"
            f"lat0={reference_latitude:.9f}"
        ),
        origin_x_m=0.0,
        origin_y_m=source_height_m,
        x_resolution_m=source_width_m / output_width,
        y_resolution_m=source_height_m / output_height,
        width=output_width,
        height=output_height,
    )
    grid.assert_round_trip()

    raster_frames: list[NormalizedRasterFrame] = []
    for frame in sequence.frames:
        values = [
            [0.0] * output_width
            for _row in range(output_height)
        ]
        for row, column, intensity in frame.cells:
            output_row = min(
                output_height - 1,
                row * output_height // first.height,
            )
            output_column = min(
                output_width - 1,
                column * output_width // first.width,
            )
            values[output_row][output_column] = max(
                values[output_row][output_column],
                intensity,
            )
        raster_frames.append(NormalizedRasterFrame(
            frame_id=(
                f"normalized-cache:{frame.region_id}:{frame.source}:"
                f"{frame.nominal_timestamp}"
            ),
            valid_at=float(frame.nominal_timestamp),
            source=frame.source,
            quantity="storm_tracker_normalized_intensity_class",
            unit=f"{frame.source}:normalized_class",
            grid=grid,
            values=tuple(tuple(row) for row in values),
        ))
    transform = NormalizedRasterTransform(
        mode=(
            "native_lossless_cache_expansion"
            if native
            else "bounded_downscaled_analysis_view"
        ),
        method="identity" if native else "deterministic_spatial_bin_maximum",
        source_height=first.height,
        source_width=first.width,
        output_height=output_height,
        output_width=output_width,
        source_pixel_count=source_pixels,
        output_pixel_count=output_height * output_width,
        maximum_source_grid_cells=maximum_source_grid_cells,
        maximum_analysis_pixels=analysis_cap,
        source_bounds_wgs84=first.bounds,
        row_scale=first.height / output_height,
        column_scale=first.width / output_width,
        source_x_resolution_m=source_x_resolution,
        source_y_resolution_m=source_y_resolution,
        output_x_resolution_m=grid.x_resolution_m,
        output_y_resolution_m=grid.y_resolution_m,
        requested_bounds_width_m=requested_bounds_width_m,
        requested_bounds_height_m=requested_bounds_height_m,
        cache_grid_width_m=source_width_m,
        cache_grid_height_m=source_height_m,
        cache_grid_east_padding_m=(
            source_width_m - requested_bounds_width_m
        ),
        cache_grid_south_padding_m=(
            source_height_m - requested_bounds_height_m
        ),
        row_bin_source_index_bounds=row_bin_bounds,
        column_bin_source_index_bounds=column_bin_bounds,
        extent_preserved=True,
        extent_reference=(
            "cache_grid_nominal_cell_extent_not_requested_wgs84_bounds"
        ),
        timestamps_preserved=True,
        lossless_cache_value_expansion=native,
        source_nodata_mask_available=False,
        provider_nodata_valid_fraction=None,
        cache_value_finite_fraction=1.0,
        output_none_count=0,
        sparse_absence_semantics=(
            "zero_initialized_normalized_cache_builder;"
            "not_claimed_as_provider_observed_dry"
        ),
        aggregation=(
            "identity"
            if native
            else (
                "maximum_cached_normalized_intensity_per_exact_source_"
                "index_bin_including_implicit_cache_zero"
            )
        ),
        analysis_biases=(
            (
                "provider_nodata_valid_fraction_unrecoverable_from_cache",
                (
                    "cache_grid_extent_includes_reported_east_south_padding_"
                    "beyond_requested_wgs84_bounds"
                ),
            )
            if native
            else (
                "max_pool_preserves_local_extrema_not_area_mean_or_mass",
                "max_pool_can_expand_extreme_support_to_entire_output_bin",
                (
                    "object_area_growth_decay_centroid_and_connectivity_can_"
                    "change_at_analysis_resolution"
                ),
                (
                    "row_and_column_scales_are_independent_due_to_integer_"
                    "shape_rounding"
                ),
                (
                    "cache_grid_extent_includes_reported_east_south_padding_"
                    "beyond_requested_wgs84_bounds"
                ),
                "provider_nodata_valid_fraction_unrecoverable_from_cache",
            )
        ),
        fingerprint=transform_fingerprint,
    )
    return NormalizedRasterReplayView(
        sequence_id=sequence.sequence_id,
        source=sequence.source,
        frames=tuple(raster_frames),
        transform=transform,
        source_wet_cell_count=sum(
            frame.wet_cell_count for frame in sequence.frames
        ),
    )


def build_hierarchical_replay_from_raster_view(
    raster_view: NormalizedRasterReplayView,
    *,
    detector_config: HierarchicalDetectorConfig,
) -> HierarchicalReplay:
    """Detect objects on one audited native or bounded raster view."""

    if not isinstance(raster_view, NormalizedRasterReplayView):
        raise TypeError("raster_view vereist NormalizedRasterReplayView")
    if not isinstance(detector_config, HierarchicalDetectorConfig):
        raise TypeError("detector_config vereist HierarchicalDetectorConfig")
    detection_frames = tuple(
        detect_hierarchical_cells(frame, detector_config)
        for frame in raster_view.frames
    )
    grid = raster_view.frames[0].grid
    return HierarchicalReplay(
        sequence_id=raster_view.sequence_id,
        source=raster_view.source,
        analysis_grid=grid,
        frames=detection_frames,
        source_wet_cell_count=raster_view.source_wet_cell_count,
        adapter_limitations=(
            "cache_is_normalized_grid_not_original_provider_bytes",
            "intensity_classes_are_not_cross_provider_physical_units",
            "local_equirectangular_metric_scale_is_sequence_specific",
            "cache_does_not_persist_provider_nodata_mask",
            (
                "native_lossless_cache_value_expansion"
                if raster_view.transform.lossless_cache_value_expansion
                else
                "bounded_downscaled_max_pool_view_not_native_resolution"
            ),
        ),
    )


def _bounded_analysis_shape(
    source_height: int,
    source_width: int,
    maximum_pixels: int,
) -> tuple[int, int]:
    if source_height * source_width <= maximum_pixels:
        return source_height, source_width
    scale = math.sqrt(source_height * source_width / maximum_pixels)
    output_height = max(1, int(source_height / scale))
    output_width = max(1, int(source_width / scale))
    while output_height * output_width > maximum_pixels:
        if output_width / source_width >= output_height / source_height:
            output_width -= 1
        else:
            output_height -= 1
    return output_height, output_width


def _source_bin_index_bounds(
    source_size: int,
    output_size: int,
) -> tuple[tuple[int, int], ...]:
    """Return exact half-open source bounds for floor(i*out/source) bins."""

    return tuple(
        (
            (index * source_size + output_size - 1) // output_size,
            (
                (index + 1) * source_size + output_size - 1
            ) // output_size,
        )
        for index in range(output_size)
    )


def run_bounded_shadow_assignment(
    replay: HierarchicalReplay,
    *,
    object_kind: str,
    flow_sample_factory: Callable[
        [HierarchicalCellDetection, HierarchicalDetectionFrame],
        OpticalFlowSample,
    ],
    edge_cost_factory: Callable[
        [AssociationFeatureVector],
        AssignmentEdgeCost,
    ],
    death_cost: float,
    birth_cost: float,
    policy_id: str,
    solution_limit: int,
    maximum_top_k_current_detections: int,
    maximum_complete_edges_per_pair: int,
    ambiguity_cost_delta: float,
    jump_speed_threshold_kmh: float,
    motion_prior_mode: str = "constant",
    last_two_process_noise_covariance: Covariance4D | None = None,
    motion_evidence_factory: Callable[
        [
            HierarchicalCellDetection,
            HierarchicalDetectionFrame,
            MotionPrediction | TrackMotionPrior,
        ],
        MotionEvidenceRecord,
    ] | None = None,
) -> BoundedShadowAssignmentAudit:
    """Assign an offline sequence using complete features and exact solvers.

    Top-k alternatives use the bounded dynamic program.  Larger current
    frames automatically use the exact polynomial one-best solver; the audit
    then marks ambiguity as unavailable rather than fabricating alternatives.
    ``constant`` preserves the original external-flow callback behavior.
    ``sparse-pyramidal`` uses the same extension point on every consecutive
    frame pair; construction and auditing of that sample remain caller-owned.
    ``last-two`` uses that callback only for an explicitly uncertain cold
    start, then derives each next vector from that track's exact two most
    recent global assignments.  Neither mode caps, smooths or gates speed.
    """

    if object_kind not in ("rain_shield", "embedded_core", "all"):
        raise ValueError(
            "object_kind moet rain_shield, embedded_core of all zijn"
        )
    if not callable(flow_sample_factory) or not callable(edge_cost_factory):
        raise TypeError("flow- en edge-cost-factories moeten callable zijn")
    if (
        motion_evidence_factory is not None
        and not callable(motion_evidence_factory)
    ):
        raise TypeError("motion_evidence_factory moet callable zijn")
    if motion_prior_mode not in (
        "constant",
        "last-two",
        "sparse-pyramidal",
    ):
        raise ValueError(
            "motion_prior_mode moet constant, last-two of "
            "sparse-pyramidal zijn"
        )
    if motion_prior_mode == "last-two":
        if last_two_process_noise_covariance is None:
            raise ValueError(
                "last-two vereist expliciete process-noise-covariantie"
            )
        _validate_replay_covariance4(
            last_two_process_noise_covariance,
            "last_two_process_noise_covariance",
        )
    elif last_two_process_noise_covariance is not None:
        raise ValueError(
            "process-noise-covariantie hoort uitsluitend bij last-two"
        )
    for label, value in (
        ("death_cost", death_cost),
        ("birth_cost", birth_cost),
        ("ambiguity_cost_delta", ambiguity_cost_delta),
    ):
        if not math.isfinite(value) or value < 0.0:
            raise ValueError(f"{label} moet niet-negatief en eindig zijn")
    if not isinstance(policy_id, str) or not policy_id:
        raise ValueError("policy_id moet een niet-lege string zijn")
    for label, value in (
        ("solution_limit", solution_limit),
        (
            "maximum_top_k_current_detections",
            maximum_top_k_current_detections,
        ),
        ("maximum_complete_edges_per_pair", maximum_complete_edges_per_pair),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{label} moet een positief geheel getal zijn")

    def selected(
        frame: HierarchicalDetectionFrame,
    ) -> tuple[HierarchicalCellDetection, ...]:
        if object_kind == "rain_shield":
            return frame.rain_shields
        if object_kind == "embedded_core":
            return frame.embedded_cores
        return frame.detections

    assignments: dict[str, str] = {}
    active_tracks: dict[str, str] = {}
    track_histories: dict[str, tuple[CellDetection, ...]] = {}
    frame_audits: list[ShadowFrameAssignmentAudit] = []
    motion_prior_audits: list[ShadowMotionPriorAudit] = []
    cost_model_ids: set[str] = set()
    motion_prior_methods: set[str] = set()
    track_number = 0
    previous_frame: HierarchicalDetectionFrame | None = None
    for current_frame in replay.frames:
        previous = selected(previous_frame) if previous_frame else ()
        current = selected(current_frame)
        expected_edges = len(previous) * len(current)
        if expected_edges > maximum_complete_edges_per_pair:
            raise ValueError(
                "complete featuretable overschrijdt "
                "maximum_complete_edges_per_pair"
            )

        feature_by_pair: dict[
            tuple[str, str], AssociationFeatureVector
        ] = {}
        feature_table = None
        frame_motion_evidence: list[MotionEvidenceRecord] = []
        edge_costs: tuple[AssignmentEdgeCost, ...] = ()
        if previous and current:
            predictions: list[MotionPrediction | TrackMotionPrior] = []
            for item in previous:
                previous_id = item.detection.detection_id
                try:
                    track_id = active_tracks[previous_id]
                    history = track_histories[track_id]
                except KeyError as err:
                    raise AssertionError(
                        "motion prior mist actieve replaytrackhistoriek"
                    ) from err
                if (
                    not history
                    or history[-1].detection_id != previous_id
                ):
                    raise AssertionError(
                        "trackhistoriek eindigt niet op vorige detectie"
                    )
                role = (
                    "sparse_pyramidal_pair"
                    if motion_prior_mode == "sparse-pyramidal"
                    else "external_every_frame"
                )
                if motion_prior_mode == "last-two" and len(history) >= 2:
                    prediction: MotionPrediction | TrackMotionPrior = (
                        build_two_position_track_prior(
                            (
                                f"offline-last-two:{track_id}:"
                                f"{int(current_frame.valid_at)}"
                            ),
                            history[-2],
                            history[-1],
                            target_valid_at=current_frame.valid_at,
                            process_noise_covariance=(
                                last_two_process_noise_covariance
                            ),
                        )
                    )
                    role = "last_two"
                else:
                    sample = flow_sample_factory(item, current_frame)
                    if not isinstance(sample, OpticalFlowSample):
                        raise TypeError(
                            "flow_sample_factory moet OpticalFlowSample "
                            "teruggeven"
                        )
                    if motion_prior_mode == "last-two":
                        _require_explicit_cold_start_uncertainty(sample)
                        role = "cold_start"
                    prediction = predict_with_optical_flow(
                        item.detection,
                        current_frame.valid_at,
                        sample,
                    )
                predictions.append(prediction)
                supporting_ids = (
                    prediction.supporting_detection_ids
                    if isinstance(prediction, TrackMotionPrior)
                    else prediction.support_frame_ids
                )
                motion_prior_audits.append(ShadowMotionPriorAudit(
                    track_id=track_id,
                    previous_detection_id=previous_id,
                    source_valid_at=prediction.source_valid_at,
                    target_valid_at=prediction.target_valid_at,
                    elapsed_seconds=(
                        prediction.target_valid_at
                        - prediction.source_valid_at
                    ),
                    role=role,
                    method=prediction.method,
                    supporting_detection_ids=supporting_ids,
                    vx_m_s=prediction.vx_m_s,
                    vy_m_s=prediction.vy_m_s,
                    predicted_x_m=prediction.predicted_x_m,
                    predicted_y_m=prediction.predicted_y_m,
                    predicted_position_covariance_m2=(
                        prediction.predicted_position_covariance_m2
                    ),
                ))
                evidence = (
                    motion_evidence_factory(
                        item,
                        current_frame,
                        prediction,
                    )
                    if motion_evidence_factory is not None
                    else unassessed_motion_evidence(
                        previous_detection_id=previous_id,
                        target_valid_at=current_frame.valid_at,
                        method=prediction.method,
                        support_frame_ids=supporting_ids,
                    )
                )
                if not isinstance(evidence, MotionEvidenceRecord):
                    raise TypeError(
                        "motion_evidence_factory moet "
                        "MotionEvidenceRecord teruggeven"
                    )
                frame_motion_evidence.append(evidence)
            motion_prior_methods.update(item.method for item in predictions)
            feature_table = build_association_feature_table(
                previous,
                current,
                tuple(predictions),
            )
            feature_by_pair = {
                (
                    feature.previous_detection_id,
                    feature.current_detection_id,
                ): feature
                for feature in feature_table.features
            }
            edge_costs = tuple(
                edge_cost_factory(feature)
                for feature in feature_table.features
            )
            cost_model_ids.update(item.cost_model_id for item in edge_costs)
        previous_ids = tuple(
            item.detection.detection_id for item in previous
        )
        current_ids = tuple(
            item.detection.detection_id for item in current
        )
        problem = GlobalAssignmentProblem(
            previous_detection_ids=previous_ids,
            current_detection_ids=current_ids,
            edge_costs=edge_costs,
            death_costs=tuple(
                (identifier, death_cost) for identifier in previous_ids
            ),
            birth_costs=tuple(
                (identifier, birth_cost) for identifier in current_ids
            ),
            policy_id=policy_id,
        )
        result = solve_global_assignment_with_fallback(
            problem,
            solution_limit=solution_limit,
            maximum_top_k_current_detections=(
                maximum_top_k_current_detections
            ),
        )
        best = result.best
        next_active: dict[str, str] = {}
        next_histories: dict[str, tuple[CellDetection, ...]] = {}
        current_by_id = {
            item.detection.detection_id: item.detection for item in current
        }
        for old_id, new_id in best.matches:
            try:
                track_id = active_tracks[old_id]
            except KeyError as err:
                raise AssertionError(
                    "vorige detectie mist een actieve replaytrack"
                ) from err
            next_active[new_id] = track_id
            assignments[new_id] = track_id
            next_histories[track_id] = (
                track_histories[track_id] + (current_by_id[new_id],)
            )[-2:]
        for new_id in best.births:
            track_number += 1
            track_id = f"offline-shadow-track-{track_number:06d}"
            next_active[new_id] = track_id
            assignments[new_id] = track_id
            next_histories[track_id] = (current_by_id[new_id],)
        if set(next_active) != set(current_ids):
            raise AssertionError(
                "globale assignment dekt huidig detectieframe niet volledig"
            )
        alternative_gap = (
            result.alternatives[0].total_cost - best.total_cost
            if result.alternatives
            else None
        )
        chosen_speeds = tuple(
            feature_by_pair[(old_id, new_id)].observed_interval_speed_kmh
            for old_id, new_id in best.matches
        )
        identity_assessment = (
            assess_shadow_identity_frame(
                feature_table,
                result,
                tuple(frame_motion_evidence),
                ambiguity_cost_delta=ambiguity_cost_delta,
            )
            if feature_table is not None
            else None
        )
        frame_audits.append(ShadowFrameAssignmentAudit(
            previous_valid_at=(
                previous_frame.valid_at if previous_frame else None
            ),
            current_valid_at=current_frame.valid_at,
            previous_detection_count=len(previous),
            current_detection_count=len(current),
            complete_feature_count=expected_edges,
            solver=result.solver,
            solution_count=len(result.solutions),
            best_total_cost=best.total_cost,
            alternative_cost_gap=alternative_gap,
            ambiguous=(
                alternative_gap <= ambiguity_cost_delta
                if alternative_gap is not None
                else None
            ),
            match_count=len(best.matches),
            birth_count=len(best.births),
            death_count=len(best.deaths),
            chosen_interval_speeds_kmh=chosen_speeds,
            identity_assessment=identity_assessment,
        ))
        active_tracks = next_active
        track_histories = next_histories
        previous_frame = current_frame

    metric_frames = tuple(
        DetectionFrame(
            region_id="normalized-cache",
            source=replay.source,
            nominal_timestamp=int(frame.valid_at),
            detections=tuple(item.detection for item in selected(frame)),
            valid_wet_run_count=0,
            malformed_run_count=0,
            adapter_algorithm=NORMALIZED_CACHE_ADAPTER,
        )
        for frame in replay.frames
    )
    metric_replay = DetectionReplay(
        sequence_id=replay.sequence_id,
        analysis_grid=MetricAnalysisGrid(
            analysis_grid_id=replay.analysis_grid.grid_id,
            reference_latitude=0.0,
            reference_longitude=0.0,
            metres_per_degree_latitude=1.0,
            metres_per_degree_longitude=1.0,
        ),
        frames=metric_frames,
        association_graphs=(),
        frame_pairs_without_graph=max(0, len(metric_frames) - 1),
        candidate_count=0,
    )
    metrics = evaluate_assignments(
        metric_replay,
        assignments,
        label="offline_shadow_global_assignment",
        jump_speed_threshold_kmh=jump_speed_threshold_kmh,
    )
    solver_names = {audit.solver for audit in frame_audits}
    return BoundedShadowAssignmentAudit(
        sequence_id=replay.sequence_id,
        object_kind=object_kind,
        assignments=tuple(sorted(assignments.items())),
        frame_audits=tuple(frame_audits),
        motion_prior_audits=tuple(motion_prior_audits),
        metrics=metrics,
        cost_model_ids=tuple(sorted(cost_model_ids)),
        motion_prior_methods=tuple(sorted(motion_prior_methods)),
        motion_prior_mode=motion_prior_mode,
        detection_evidence_sha256=(
            _hierarchical_detection_evidence_sha256(replay, object_kind)
        ),
        policy_id=policy_id,
        solution_limit=solution_limit,
        maximum_top_k_current_detections=(
            maximum_top_k_current_detections
        ),
        ambiguity_cost_delta=ambiguity_cost_delta,
        exact_one_best_everywhere=True,
        alternative_hypotheses_available=any(
            audit.solution_count > 1 for audit in frame_audits
        ),
        right_censored_track_count=len(active_tracks),
        adapter_limitations=replay.adapter_limitations + (
            "identity_switches_require_external_truth_and_are_not_claimed",
            "flow_and_cost_calibration_are_caller_supplied",
            "one_best_assignments_are_hypotheses_not_confirmed_identity",
            "independent_identity_truth_is_unavailable",
        ) + (
            (
                "last_two_prior_recursively_depends_on_earlier_assignments",
            )
            if motion_prior_mode == "last-two"
            else ()
        ),
    )


def _require_explicit_cold_start_uncertainty(
    sample: OpticalFlowSample,
) -> None:
    """Reject a falsely certain cold start, including an implicit zero prior."""

    uncertainty_trace = sum(
        covariance[index][index]
        for covariance in (
            sample.velocity_covariance_m2_s2,
            sample.residual_covariance_m2,
        )
        for index in range(2)
    )
    if not math.isfinite(uncertainty_trace) or uncertainty_trace <= 0.0:
        raise ValueError(
            "last-two cold start vereist expliciete positieve "
            "caller-onzekerheid"
        )


def _validate_replay_covariance4(
    covariance: Covariance4D,
    label: str,
) -> None:
    """Validate an immutable positive-semidefinite 4x4 covariance."""

    if (
        not isinstance(covariance, tuple)
        or len(covariance) != 4
        or any(
            not isinstance(row, tuple) or len(row) != 4
            for row in covariance
        )
    ):
        raise ValueError(f"{label} moet een immutable 4x4 tuple zijn")
    if not all(math.isfinite(value) for row in covariance for value in row):
        raise ValueError(f"{label} moet eindig zijn")
    for row in range(4):
        for column in range(4):
            if not math.isclose(
                covariance[row][column],
                covariance[column][row],
                rel_tol=1e-12,
                abs_tol=1e-12,
            ):
                raise ValueError(f"{label} moet symmetrisch zijn")
    scale = max(1.0, *(abs(value) for row in covariance for value in row))
    for size in range(1, 5):
        for indices in itertools.combinations(range(4), size):
            minor = tuple(
                tuple(covariance[row][column] for column in indices)
                for row in indices
            )
            if _replay_determinant(minor) < -1e-10 * scale**size:
                raise ValueError(
                    f"{label} moet positief semidefiniet zijn"
                )


def _replay_determinant(
    matrix: tuple[tuple[float, ...], ...],
) -> float:
    if len(matrix) == 1:
        return matrix[0][0]
    return sum(
        (
            (-1.0 if column % 2 else 1.0)
            * matrix[0][column]
            * _replay_determinant(tuple(
                tuple(
                    row[item]
                    for item in range(len(matrix))
                    if item != column
                )
                for row in matrix[1:]
            ))
        )
        for column in range(len(matrix))
    )


def _hierarchical_detection_evidence_sha256(
    replay: HierarchicalReplay,
    object_kind: str,
) -> str:
    """Fingerprint every selected immutable detection and its footprint."""

    def selected(
        frame: HierarchicalDetectionFrame,
    ) -> tuple[HierarchicalCellDetection, ...]:
        if object_kind == "rain_shield":
            return frame.rain_shields
        if object_kind == "embedded_core":
            return frame.embedded_cores
        return frame.detections

    evidence = []
    for frame in replay.frames:
        detections = []
        for item in sorted(
            selected(frame),
            key=lambda value: value.detection.detection_id,
        ):
            detection = item.detection
            detections.append({
                "detection_id": detection.detection_id,
                "valid_at": detection.valid_at,
                "analysis_grid_id": detection.analysis_grid_id,
                "x_m": detection.x_m,
                "y_m": detection.y_m,
                "position_sigma_m": detection.position_sigma_m,
                "source": detection.source,
                "object_kind": item.object_kind,
                "parent_detection_id": item.parent_detection_id,
                "footprint_pixels": item.footprint_pixels,
                "area_m2": item.area_m2,
                "mean_value": item.mean_value,
                "maximum_value": item.maximum_value,
                "threshold": item.threshold,
                "centroid_method": item.centroid_method,
            })
        evidence.append({
            "frame_id": frame.frame_id,
            "valid_at": frame.valid_at,
            "analysis_grid_id": frame.analysis_grid_id,
            "detections": detections,
        })
    encoded = json.dumps(
        evidence,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def assess_original_radar_frame(
    frame: OriginalRadarFrame,
) -> FrameAssessment:
    """Classify structure separately from the presence of precipitation."""

    reasons: list[str] = []
    overlay_source = frame.overlay.get("source")
    if not isinstance(overlay_source, str) or not overlay_source.strip():
        reasons.append("missing_overlay_source")
    elif overlay_source != frame.source:
        reasons.append("overlay_source_mismatch")

    timestamp_delta: float | None = None
    try:
        overlay_timestamp = float(frame.overlay["timestamp"])
        if not math.isfinite(overlay_timestamp) or overlay_timestamp <= 0.0:
            raise ValueError
        timestamp_delta = abs(
            overlay_timestamp - float(frame.nominal_timestamp)
        )
        if timestamp_delta >= _TIMESTAMP_BUCKET_SECONDS:
            reasons.append("overlay_timestamp_outside_nominal_minute")
    except (KeyError, TypeError, ValueError):
        reasons.append("invalid_overlay_timestamp")

    runs_value = frame.overlay.get("runs")
    if not isinstance(runs_value, (list, tuple)):
        reasons.append("invalid_runs_container")
        runs: tuple[object, ...] = ()
    else:
        runs = tuple(runs_value)
    parsed_runs, malformed_runs = _parse_overlay_runs(runs)
    critical = {
        "missing_overlay_source",
        "overlay_source_mismatch",
        "overlay_timestamp_outside_nominal_minute",
        "invalid_overlay_timestamp",
        "invalid_runs_container",
    }
    if malformed_runs:
        reasons.append("malformed_runs_present")
    frame_usable = not any(reason in critical for reason in reasons)
    return FrameAssessment(
        frame=frame,
        frame_usable=frame_usable,
        precipitation_usable=frame_usable and bool(parsed_runs),
        run_count=len(runs),
        valid_wet_run_count=len(parsed_runs),
        malformed_run_count=malformed_runs,
        timestamp_delta_seconds=timestamp_delta,
        reasons=tuple(reasons),
    )


def build_replay_inventory(
    corpus: CalibrationReplayCorpus,
    *,
    minimum_sequence_frames: int = 20,
    maximum_gap_factor: float = 1.5,
) -> tuple[SourceReplayInventory, ...]:
    """Group by region/source and find cadence-contiguous usable sequences."""

    if not isinstance(minimum_sequence_frames, int) or minimum_sequence_frames < 2:
        raise ValueError("minimum_sequence_frames moet minstens twee zijn")
    if (
        not math.isfinite(maximum_gap_factor)
        or maximum_gap_factor < 1.0
    ):
        raise ValueError("maximum_gap_factor moet minstens 1,0 zijn")

    frame_groups: dict[
        tuple[str, str], list[OriginalRadarFrame]
    ] = defaultdict(list)
    issue_groups: Counter[tuple[str, str]] = Counter()
    issue_timestamps: dict[tuple[str, str], list[int]] = defaultdict(list)
    for frame in corpus.frames:
        frame_groups[(frame.region_id, frame.source)].append(frame)
    for issue in corpus.decode_issues:
        key = (issue.region_id, issue.source)
        issue_groups[key] += 1
        issue_timestamps[key].append(issue.nominal_timestamp)

    result: list[SourceReplayInventory] = []
    for region, source_name in sorted(
        set(frame_groups) | set(issue_groups)
    ):
        key = (region, source_name)
        frames = sorted(
            frame_groups[key],
            key=lambda item: item.nominal_timestamp,
        )
        assessments = tuple(
            assess_original_radar_frame(frame) for frame in frames
        )
        all_timestamps = sorted({
            *(frame.nominal_timestamp for frame in frames),
            *issue_timestamps[key],
        })
        cadence, cadence_support = _infer_cadence(all_timestamps)
        sequences, gaps = _split_sequences(
            region,
            source_name,
            assessments,
            cadence_seconds=cadence,
            minimum_sequence_frames=minimum_sequence_frames,
            maximum_gap_factor=maximum_gap_factor,
        )
        result.append(SourceReplayInventory(
            region_id=region,
            source=source_name,
            cadence_seconds=cadence,
            cadence_support_ratio=cadence_support,
            total_decoded_frames=len(frames),
            usable_frames=sum(item.frame_usable for item in assessments),
            precipitation_usable_frames=sum(
                item.precipitation_usable for item in assessments
            ),
            decode_issue_count=issue_groups[key],
            unusable_frame_count=sum(
                not item.frame_usable for item in assessments
            ),
            gaps=gaps,
            sequences=sequences,
        ))
    return tuple(result)


def build_metric_analysis_grid(
    sequence: ReplaySequence,
) -> MetricAnalysisGrid:
    """Build one deterministic local metric coordinate contract."""

    points: list[tuple[float, float]] = []
    for frame in sequence.frames:
        runs_value = frame.overlay.get("runs", ())
        parsed, _malformed = _parse_overlay_runs(
            runs_value if isinstance(runs_value, (list, tuple)) else ()
        )
        points.extend(point for run in parsed for point in run.points)
    if not points:
        raise ValueError(
            "analysis-grid vereist minstens één geldige natte overlayrun"
        )
    latitudes = [point[0] for point in points]
    longitudes = [point[1] for point in points]
    reference_latitude = (min(latitudes) + max(latitudes)) / 2.0
    reference_longitude = (min(longitudes) + max(longitudes)) / 2.0
    latitude_radians = math.radians(reference_latitude)
    metres_per_latitude = (
        111_132.92
        - 559.82 * math.cos(2.0 * latitude_radians)
        + 1.175 * math.cos(4.0 * latitude_radians)
        - 0.0023 * math.cos(6.0 * latitude_radians)
    )
    metres_per_longitude = (
        111_412.84 * math.cos(latitude_radians)
        - 93.5 * math.cos(3.0 * latitude_radians)
        + 0.118 * math.cos(5.0 * latitude_radians)
    )
    signature = hashlib.sha256(
        (
            f"{sequence.region_id}\0{sequence.source}\0"
            f"{reference_latitude:.9f}\0{reference_longitude:.9f}"
        ).encode("utf-8")
    ).hexdigest()[:16]
    return MetricAnalysisGrid(
        analysis_grid_id=f"offline-local-wgs84-v1:{signature}",
        reference_latitude=reference_latitude,
        reference_longitude=reference_longitude,
        metres_per_degree_latitude=metres_per_latitude,
        metres_per_degree_longitude=metres_per_longitude,
    )


def overlay_to_cell_detections(
    frame: OriginalRadarFrame,
    analysis_grid: MetricAnalysisGrid,
    *,
    position_sigma_m: float,
) -> DetectionFrame:
    """Convert connected precipitation runs to immutable CellDetections.

    ``position_sigma_m`` is mandatory because the calibration overlay does not
    carry a calibrated product-specific positional uncertainty.
    """

    if not math.isfinite(position_sigma_m) or position_sigma_m <= 0.0:
        raise ValueError("position_sigma_m moet positief en eindig zijn")
    assessment = assess_original_radar_frame(frame)
    if not assessment.frame_usable:
        raise ValueError(
            "onbruikbaar radarframe kan niet naar CellDetection: "
            + ",".join(assessment.reasons)
        )
    runs_value = frame.overlay.get("runs", ())
    runs, malformed = _parse_overlay_runs(
        runs_value if isinstance(runs_value, (list, tuple)) else ()
    )
    components = _connected_run_components(runs)
    projected_components: list[tuple[float, float]] = []
    for component in components:
        weighted_x = 0.0
        weighted_y = 0.0
        total_weight = 0.0
        for run in component:
            projected = [
                analysis_grid.project(latitude, longitude)
                for latitude, longitude in run.points
            ]
            center_x = sum(point[0] for point in projected) / len(projected)
            center_y = sum(point[1] for point in projected) / len(projected)
            polygon_area = abs(sum(
                first[0] * second[1] - second[0] * first[1]
                for first, second in zip(
                    projected,
                    projected[1:] + projected[:1],
                )
            )) / 2.0
            weight = max(1.0, polygon_area) * run.intensity
            weighted_x += center_x * weight
            weighted_y += center_y * weight
            total_weight += weight
        projected_components.append((
            weighted_x / total_weight,
            weighted_y / total_weight,
        ))
    ordered = sorted(projected_components, key=lambda item: (item[0], item[1]))
    detections = tuple(
        CellDetection(
            detection_id=(
                f"{frame.region_id}:{frame.source}:"
                f"{frame.nominal_timestamp}:cell-{index:04d}"
            ),
            valid_at=float(frame.nominal_timestamp),
            analysis_grid_id=analysis_grid.analysis_grid_id,
            x_m=x_m,
            y_m=y_m,
            position_sigma_m=position_sigma_m,
            source=frame.source,
        )
        for index, (x_m, y_m) in enumerate(ordered, start=1)
    )
    return DetectionFrame(
        region_id=frame.region_id,
        source=frame.source,
        nominal_timestamp=frame.nominal_timestamp,
        detections=detections,
        valid_wet_run_count=len(runs),
        malformed_run_count=malformed,
    )


def build_detection_replay(
    sequence: ReplaySequence,
    *,
    position_sigma_m: float,
    motion_sigma_m_s: float,
) -> DetectionReplay:
    """Adapt a sequence and retain every candidate for non-empty frame pairs."""

    if not math.isfinite(motion_sigma_m_s) or motion_sigma_m_s < 0.0:
        raise ValueError("motion_sigma_m_s moet niet-negatief en eindig zijn")
    grid = build_metric_analysis_grid(sequence)
    frames = tuple(
        overlay_to_cell_detections(
            frame,
            grid,
            position_sigma_m=position_sigma_m,
        )
        for frame in sequence.frames
    )
    graphs: list[DetectionAssociationGraph] = []
    pairs_without_graph = 0
    for previous, current in zip(frames, frames[1:]):
        if not previous.detections or not current.detections:
            pairs_without_graph += 1
            continue
        graphs.append(build_detection_association_graph(
            previous.detections,
            current.detections,
            motion_sigma_m_s=motion_sigma_m_s,
        ))
    return DetectionReplay(
        sequence_id=sequence.sequence_id,
        analysis_grid=grid,
        frames=frames,
        association_graphs=tuple(graphs),
        frame_pairs_without_graph=pairs_without_graph,
        candidate_count=sum(len(graph.candidates) for graph in graphs),
    )


def evaluate_assignments(
    replay: DetectionReplay,
    assignments: Mapping[str, str],
    *,
    label: str,
    jump_speed_threshold_kmh: float,
    reference_identity_by_detection: Mapping[str, str] | None = None,
) -> AssignmentMetrics:
    """Measure a caller's assignment without silently inventing identities."""

    if not isinstance(label, str) or not label:
        raise ValueError("evaluatielabel moet een niet-lege string zijn")
    if (
        not math.isfinite(jump_speed_threshold_kmh)
        or jump_speed_threshold_kmh <= 0.0
    ):
        raise ValueError(
            "jump_speed_threshold_kmh moet positief en eindig zijn"
        )
    detections = {
        detection.detection_id: detection
        for frame in replay.frames
        for detection in frame.detections
    }
    unknown = sorted(set(assignments) - set(detections))
    if unknown:
        raise ValueError(
            "assignment bevat onbekende detection_id: " + unknown[0]
        )
    assigned: dict[str, str] = {}
    for detection_id, track_id in assignments.items():
        if not isinstance(track_id, str) or not track_id:
            raise ValueError("assignment vereist niet-lege track_id's")
        assigned[str(detection_id)] = track_id

    tracks: dict[str, list[CellDetection]] = defaultdict(list)
    for detection_id, track_id in assigned.items():
        tracks[track_id].append(detections[detection_id])
    frame_times = tuple(
        float(frame.nominal_timestamp) for frame in replay.frames
    )
    track_results: list[TrackMetrics] = []
    every_speed: list[float] = []
    identity_tracks: dict[str, set[str]] = defaultdict(set)
    for track_id in sorted(tracks):
        samples = sorted(
            tracks[track_id],
            key=lambda item: (item.valid_at, item.detection_id),
        )
        sample_times = [sample.valid_at for sample in samples]
        if len(set(sample_times)) != len(sample_times):
            raise ValueError(
                f"track {track_id} bevat meerdere detecties in één frame"
            )
        speeds: list[float] = []
        for first, second in zip(samples, samples[1:]):
            snapshot = estimate_preliminary_cell_track(
                track_id,
                first,
                second,
            )
            speeds.append(snapshot.speed_kmh)
        every_speed.extend(speeds)
        if samples:
            sample_time_set = set(sample_times)
            missed_slots = sum(
                samples[0].valid_at < timestamp < samples[-1].valid_at
                and timestamp not in sample_time_set
                for timestamp in frame_times
            )
            duration = samples[-1].valid_at - samples[0].valid_at
        else:
            missed_slots = 0
            duration = 0.0

        identity_switches: int | None
        unscored_identity_samples = 0
        if reference_identity_by_detection is None:
            identity_switches = None
        else:
            identity_switches = 0
            previous_identity: str | None = None
            for sample in samples:
                identity = reference_identity_by_detection.get(
                    sample.detection_id
                )
                if identity is None:
                    unscored_identity_samples += 1
                    previous_identity = None
                    continue
                identity = str(identity)
                identity_tracks[identity].add(track_id)
                if (
                    previous_identity is not None
                    and identity != previous_identity
                ):
                    identity_switches += 1
                previous_identity = identity
        track_results.append(TrackMetrics(
            track_id=track_id,
            sample_count=len(samples),
            interval_count=len(speeds),
            duration_seconds=duration,
            minimum_speed_kmh=min(speeds) if speeds else None,
            median_speed_kmh=(
                statistics.median(speeds) if speeds else None
            ),
            maximum_speed_kmh=max(speeds) if speeds else None,
            jump_count=sum(
                speed > jump_speed_threshold_kmh for speed in speeds
            ),
            missed_detection_slots=missed_slots,
            identity_switch_count=identity_switches,
            unscored_identity_samples=unscored_identity_samples,
        ))

    durations = [track.duration_seconds for track in track_results]
    switch_values = [
        track.identity_switch_count
        for track in track_results
        if track.identity_switch_count is not None
    ]
    fragmentations = (
        sum(max(0, len(track_ids) - 1) for track_ids in identity_tracks.values())
        if reference_identity_by_detection is not None
        else None
    )
    return AssignmentMetrics(
        label=label,
        assigned_detection_count=len(assigned),
        unassigned_detection_count=len(detections) - len(assigned),
        track_count=len(track_results),
        interval_count=len(every_speed),
        median_interval_speed_kmh=(
            statistics.median(every_speed) if every_speed else None
        ),
        p95_interval_speed_kmh=_nearest_rank_percentile(
            every_speed, 0.95
        ),
        maximum_interval_speed_kmh=max(every_speed) if every_speed else None,
        median_track_duration_seconds=(
            statistics.median(durations) if durations else None
        ),
        maximum_track_duration_seconds=max(durations) if durations else None,
        jump_speed_threshold_kmh=jump_speed_threshold_kmh,
        jump_count=sum(track.jump_count for track in track_results),
        missed_detection_slots=sum(
            track.missed_detection_slots for track in track_results
        ),
        identity_switch_count=(
            sum(int(value) for value in switch_values)
            if reference_identity_by_detection is not None
            else None
        ),
        reference_identity_fragmentations=fragmentations,
        tracks=tuple(track_results),
    )


def compare_legacy_and_shadow(
    replay: DetectionReplay,
    *,
    legacy_assignments: Mapping[str, str],
    shadow_assignments: Mapping[str, str],
    jump_speed_threshold_kmh: float,
    reference_identity_by_detection: Mapping[str, str] | None = None,
) -> LegacyShadowComparison:
    """Return like-for-like metrics without declaring either result correct."""

    legacy = evaluate_assignments(
        replay,
        legacy_assignments,
        label="legacy",
        jump_speed_threshold_kmh=jump_speed_threshold_kmh,
        reference_identity_by_detection=reference_identity_by_detection,
    )
    shadow = evaluate_assignments(
        replay,
        shadow_assignments,
        label="shadow",
        jump_speed_threshold_kmh=jump_speed_threshold_kmh,
        reference_identity_by_detection=reference_identity_by_detection,
    )
    compared_fields = (
        "assigned_detection_count",
        "unassigned_detection_count",
        "track_count",
        "interval_count",
        "median_interval_speed_kmh",
        "p95_interval_speed_kmh",
        "maximum_interval_speed_kmh",
        "median_track_duration_seconds",
        "maximum_track_duration_seconds",
        "jump_count",
        "missed_detection_slots",
        "identity_switch_count",
        "reference_identity_fragmentations",
    )
    deltas: dict[str, float | int | None] = {}
    for field in compared_fields:
        legacy_value = getattr(legacy, field)
        shadow_value = getattr(shadow, field)
        deltas[field] = (
            shadow_value - legacy_value
            if legacy_value is not None and shadow_value is not None
            else None
        )
    return LegacyShadowComparison(
        sequence_id=replay.sequence_id,
        legacy=legacy,
        shadow=shadow,
        deltas=deltas,
        identity_switches_measurable=(
            reference_identity_by_detection is not None
        ),
    )


def _validated_normalized_cache_frame(
    region_id: str,
    source: str,
    nominal_timestamp: int,
    saved_at: float,
    value: object,
) -> NormalizedCacheFrame:
    if not isinstance(value, Mapping):
        raise ValueError("frame_json_is_not_an_object")
    frame_source = value.get("source")
    if frame_source != source:
        raise ValueError("frame_source_mismatch")
    try:
        frame_timestamp = int(value["nominal_timestamp"])
    except (KeyError, TypeError, ValueError, OverflowError) as err:
        raise ValueError("invalid_frame_timestamp") from err
    if frame_timestamp != nominal_timestamp or frame_timestamp <= 0:
        raise ValueError("frame_timestamp_mismatch")
    try:
        bounds = tuple(float(item) for item in value["bounds"])
        if len(bounds) != 4:
            raise ValueError
        west, south, east, north = bounds
        grid_deg = float(value["grid_deg"])
        height = int(value["height"])
        width = int(value["width"])
        input_runs = int(value.get("input_runs", 0))
    except (KeyError, TypeError, ValueError, OverflowError) as err:
        raise ValueError("invalid_frame_geometry") from err
    if (
        not all(math.isfinite(item) for item in bounds)
        or not west < east
        or not south < north
        or not math.isfinite(grid_deg)
        or grid_deg <= 0.0
        or height <= 0
        or width <= 0
        or input_runs < 0
    ):
        raise ValueError("invalid_frame_geometry")
    raw_cells = value.get("cells")
    if not isinstance(raw_cells, (list, tuple)):
        raise ValueError("invalid_cells_container")
    cells: list[tuple[int, int, float]] = []
    locations: set[tuple[int, int]] = set()
    try:
        for raw_cell in raw_cells:
            row = int(raw_cell[0])
            column = int(raw_cell[1])
            intensity = float(raw_cell[2])
            location = (row, column)
            if (
                row < 0
                or row >= height
                or column < 0
                or column >= width
                or not math.isfinite(intensity)
                or intensity <= 0.0
                or location in locations
            ):
                raise ValueError
            locations.add(location)
            cells.append((row, column, intensity))
    except (TypeError, ValueError, IndexError, OverflowError) as err:
        raise ValueError("invalid_sparse_cells") from err
    return NormalizedCacheFrame(
        region_id=region_id,
        source=source,
        nominal_timestamp=nominal_timestamp,
        saved_at=saved_at,
        bounds=(west, south, east, north),
        grid_deg=grid_deg,
        height=height,
        width=width,
        cells=tuple(sorted(cells)),
        input_runs=input_runs,
    )


def _split_normalized_cache_sequences(
    region_id: str,
    source: str,
    frames: Sequence[NormalizedCacheFrame],
    *,
    cadence_seconds: int | None,
    minimum_sequence_frames: int,
    maximum_gap_factor: float,
) -> tuple[
    tuple[NormalizedCacheSequence, ...],
    tuple[ReplayGap, ...],
]:
    sequences: list[NormalizedCacheSequence] = []
    gaps: list[ReplayGap] = []
    current: list[NormalizedCacheFrame] = []

    def flush() -> None:
        if not current:
            return
        starts_at = current[0].nominal_timestamp
        ends_at = current[-1].nominal_timestamp
        signature = hashlib.sha256(
            (
                f"{region_id}\0{source}\0{starts_at}\0{ends_at}\0"
                f"{current[0].geometry_key!r}"
            ).encode("utf-8")
        ).hexdigest()[:12]
        sequences.append(NormalizedCacheSequence(
            sequence_id=(
                f"normalized:{region_id}:{source}:"
                f"{starts_at}:{ends_at}:{signature}"
            ),
            region_id=region_id,
            source=source,
            cadence_seconds=cadence_seconds,
            frames=tuple(current),
            minimum_frame_count=minimum_sequence_frames,
        ))
        current.clear()

    for frame in frames:
        if current:
            previous = current[-1]
            elapsed = frame.nominal_timestamp - previous.nominal_timestamp
            time_gap = (
                elapsed <= 0
                or (
                    cadence_seconds is not None
                    and elapsed > cadence_seconds * maximum_gap_factor
                )
            )
            geometry_changed = frame.geometry_key != previous.geometry_key
            if time_gap or geometry_changed:
                if time_gap and elapsed > 0:
                    estimated_missing = (
                        max(0, round(elapsed / cadence_seconds) - 1)
                        if cadence_seconds
                        else None
                    )
                    gaps.append(ReplayGap(
                        after_timestamp=previous.nominal_timestamp,
                        before_timestamp=frame.nominal_timestamp,
                        elapsed_seconds=elapsed,
                        estimated_missing_frames=estimated_missing,
                    ))
                flush()
        current.append(frame)
    flush()
    return tuple(sequences), tuple(gaps)


def _limited_zlib_decompress(payload: bytes, maximum_bytes: int) -> bytes:
    decompressor = zlib.decompressobj()
    decoded = decompressor.decompress(payload, maximum_bytes + 1)
    if len(decoded) > maximum_bytes or decompressor.unconsumed_tail:
        raise ValueError("decoded_payload_too_large")
    decoded += decompressor.flush(maximum_bytes + 1 - len(decoded))
    if len(decoded) > maximum_bytes:
        raise ValueError("decoded_payload_too_large")
    if not decompressor.eof:
        raise ValueError("incomplete_zlib_stream")
    if decompressor.unused_data:
        raise ValueError("trailing_zlib_data")
    return decoded


def _parse_overlay_runs(
    runs: Iterable[object],
) -> tuple[tuple[_OverlayRun, ...], int]:
    parsed: list[_OverlayRun] = []
    malformed = 0
    for value in runs:
        try:
            if not isinstance(value, Mapping):
                raise ValueError
            intensity = int(value["intensity"])
            raw_ring = value["ring"]
            if not isinstance(raw_ring, (list, tuple)) or len(raw_ring) < 4:
                raise ValueError
            points = tuple(
                (float(point[0]), float(point[1]))
                for point in raw_ring
            )
            if intensity <= 0 or any(
                not math.isfinite(latitude)
                or not math.isfinite(longitude)
                or not -90.0 <= latitude <= 90.0
                or not -180.0 <= longitude <= 180.0
                for latitude, longitude in points
            ):
                raise ValueError
            latitudes = [point[0] for point in points]
            longitudes = [point[1] for point in points]
            south, north = min(latitudes), max(latitudes)
            west, east = min(longitudes), max(longitudes)
            if south >= north or west >= east:
                raise ValueError
            parsed.append(_OverlayRun(
                intensity=intensity,
                points=points,
                south=south,
                north=north,
                west=west,
                east=east,
            ))
        except (KeyError, TypeError, ValueError, IndexError, OverflowError):
            malformed += 1
    return tuple(parsed), malformed


def _infer_cadence(
    timestamps: Sequence[int],
) -> tuple[int | None, float | None]:
    intervals = [
        int(current - previous)
        for previous, current in zip(timestamps, timestamps[1:])
        if current > previous
    ]
    if not intervals:
        return None, None
    counts = Counter(intervals)
    highest_support = max(counts.values())
    cadence = min(
        interval
        for interval, count in counts.items()
        if count == highest_support
    )
    return cadence, highest_support / len(intervals)


def _split_sequences(
    region_id: str,
    source: str,
    assessments: Sequence[FrameAssessment],
    *,
    cadence_seconds: int | None,
    minimum_sequence_frames: int,
    maximum_gap_factor: float,
) -> tuple[tuple[ReplaySequence, ...], tuple[ReplayGap, ...]]:
    sequences: list[ReplaySequence] = []
    gaps: list[ReplayGap] = []
    current: list[FrameAssessment] = []

    def flush() -> None:
        if not current:
            return
        starts_at = current[0].frame.nominal_timestamp
        ends_at = current[-1].frame.nominal_timestamp
        digest = hashlib.sha256(
            f"{region_id}\0{source}\0{starts_at}\0{ends_at}".encode("utf-8")
        ).hexdigest()[:12]
        sequences.append(ReplaySequence(
            sequence_id=(
                f"{region_id}:{source}:{starts_at}:{ends_at}:{digest}"
            ),
            region_id=region_id,
            source=source,
            cadence_seconds=cadence_seconds,
            frames=tuple(item.frame for item in current),
            assessments=tuple(current),
            minimum_frame_count=minimum_sequence_frames,
        ))
        current.clear()

    for assessment in assessments:
        if not assessment.frame_usable:
            flush()
            continue
        if current:
            previous = current[-1].frame.nominal_timestamp
            timestamp = assessment.frame.nominal_timestamp
            elapsed = timestamp - previous
            is_gap = (
                elapsed <= 0
                or (
                    cadence_seconds is not None
                    and elapsed > cadence_seconds * maximum_gap_factor
                )
            )
            if is_gap:
                if elapsed > 0:
                    estimated_missing = (
                        max(0, round(elapsed / cadence_seconds) - 1)
                        if cadence_seconds
                        else None
                    )
                    gaps.append(ReplayGap(
                        after_timestamp=previous,
                        before_timestamp=timestamp,
                        elapsed_seconds=elapsed,
                        estimated_missing_frames=estimated_missing,
                    ))
                flush()
        current.append(assessment)
    flush()
    return tuple(sequences), tuple(gaps)


def _connected_run_components(
    runs: Sequence[_OverlayRun],
) -> tuple[tuple[_OverlayRun, ...], ...]:
    if not runs:
        return ()
    parents = list(range(len(runs)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(first: int, second: int) -> None:
        first_root, second_root = find(first), find(second)
        if first_root != second_root:
            parents[max(first_root, second_root)] = min(
                first_root, second_root
            )

    # Real RainViewer frames contain more than ten thousand short runs.  A
    # complete O(n²) rectangle comparison would make an offline replay
    # impractical.  Index every already-seen rectangle in local geographic
    # buckets; exact connectivity is still decided by ``_runs_are_connected``.
    positive_widths = [
        run.east - run.west for run in runs if run.east > run.west
    ]
    positive_heights = [
        run.north - run.south for run in runs if run.north > run.south
    ]
    typical_width = statistics.median(positive_widths)
    typical_height = statistics.median(positive_heights)
    bucket_size = max(
        _GEOMETRY_EPSILON_DEGREES * 100.0,
        typical_width,
        typical_height,
    )
    buckets: dict[tuple[int, int], list[int]] = defaultdict(list)
    for current_index, current in enumerate(runs):
        row_start = math.floor(
            (current.south - _GEOMETRY_EPSILON_DEGREES) / bucket_size
        )
        row_end = math.floor(
            (current.north + _GEOMETRY_EPSILON_DEGREES) / bucket_size
        )
        column_start = math.floor(
            (current.west - _GEOMETRY_EPSILON_DEGREES) / bucket_size
        )
        column_end = math.floor(
            (current.east + _GEOMETRY_EPSILON_DEGREES) / bucket_size
        )
        keys = tuple(
            (row, column)
            for row in range(row_start, row_end + 1)
            for column in range(column_start, column_end + 1)
        )
        candidates = {
            previous_index
            for key in keys
            for previous_index in buckets.get(key, ())
        }
        connected_roots: set[int] = set()
        for previous_index in sorted(candidates):
            previous_root = find(previous_index)
            if previous_root in connected_roots:
                continue
            if _runs_are_connected(current, runs[previous_index]):
                union(current_index, previous_index)
                connected_roots.add(find(previous_index))
        for key in keys:
            buckets[key].append(current_index)
    groups: dict[int, list[_OverlayRun]] = defaultdict(list)
    for index, run in enumerate(runs):
        groups[find(index)].append(run)
    return tuple(
        tuple(groups[root])
        for root in sorted(groups)
    )


def _runs_are_connected(first: _OverlayRun, second: _OverlayRun) -> bool:
    latitude_overlap = min(first.north, second.north) - max(
        first.south, second.south
    )
    longitude_overlap = min(first.east, second.east) - max(
        first.west, second.west
    )
    latitude_gap = max(
        0.0,
        max(first.south, second.south) - min(first.north, second.north),
    )
    longitude_gap = max(
        0.0,
        max(first.west, second.west) - min(first.east, second.east),
    )
    return (
        (
            latitude_overlap > _GEOMETRY_EPSILON_DEGREES
            and longitude_gap <= _GEOMETRY_EPSILON_DEGREES
        )
        or (
            longitude_overlap > _GEOMETRY_EPSILON_DEGREES
            and latitude_gap <= _GEOMETRY_EPSILON_DEGREES
        )
    )


def _nearest_rank_percentile(
    values: Sequence[float],
    probability: float,
) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(probability * len(ordered)) - 1)
    return ordered[index]
