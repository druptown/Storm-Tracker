"""Unwired end-to-end shadow compositor for immutable individual CellTracks.

The engine deliberately composes the existing scientific primitives instead
of implementing another tracker.  Every detected shield and embedded core is
an individual identity.  Weather-system grouping is a read-only secondary
view and cannot write motion or identity back into a track.

Capacity is fail-closed: when the complete candidate product or exact top-k
assignment exceeds caller-owned bounds, the returned batch is explicitly
``indeterminate`` and carries every detection while the input state is kept
byte-for-byte unchanged.  No displacement, speed or innovation is a reject.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import itertools
import math
from typing import Iterable, Literal

from .association_features import (
    AssociationFeatureTable,
    AssociationFeatureVector,
    build_association_feature_table,
)
from .association_graph import (
    DetectionAssociationGraph,
    build_detection_association_graph,
)
from .cell_track_lifecycle import (
    CellTrackLedger,
    CellTrackLifecyclePolicy,
    ImmutableCellTrack,
    append_cell_detection,
    record_cell_track_birth,
    record_cell_track_gap,
    record_cell_track_merge,
    record_cell_track_reacquired,
    record_cell_track_split,
)
from .global_assignment import (
    AssignmentEdgeCost,
    GlobalAssignmentProblem,
    GlobalAssignmentResult,
    solve_bounded_k_best_global_assignment,
    solve_exact_global_assignment,
    solve_global_assignment_one_best,
)
from .advected_footprint_evidence import (
    AdvectedFootprintCapacityError,
    AdvectedFootprintEvidence,
    FootprintEvidenceObject,
    build_advected_footprint_evidence,
)
from .multi_hypothesis_identity import (
    LocalCellDeformationCandidate,
    MultiHypothesisIdentityPolicy,
    MultiHypothesisIdentitySnapshot,
    MultiScanIdentityPosterior,
    MultiScanRankingWeights,
    SystemTranslationEvidence,
    advance_multi_scan_identity,
    bootstrap_multi_scan_identity,
    bind_local_candidate_costs,
    build_local_deformation_candidates,
    finalize_identity_snapshot,
)
from .shared_system_translation import (
    BinaryRainShieldFootprintFrame,
    SharedSystemTranslationConfig,
    SharedSystemTranslationResult,
    estimate_shared_system_translation,
)
from .hierarchical_detection import (
    HierarchicalCellDetection,
    HierarchicalDetectionFrame,
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
    detect_hierarchical_cells,
)
from .lucas_kanade_flow import (
    DetectionFlowAggregation,
    DetectionFlowAggregationConfig,
    LucasKanadeConfig,
    LucasKanadeFlowField,
    aggregate_detection_flow_sample,
    estimate_lucas_kanade_flow,
)
from .motion_prediction import MotionPrediction, predict_with_optical_flow
from .provider_handoff import ProviderHandoffEvent
from .recursive_track_estimator import (
    Matrix4,
    RecursiveTrackEstimate,
    RecursiveTrackEstimatorPolicy,
    assimilate_recursive_detection,
    initialize_recursive_track_estimate,
)
from .track_motion_prior import (
    TrackMotionPrior,
    build_cold_start_track_prior,
    build_two_position_track_prior,
)
from .weather_system_grouping import (
    WeatherSystemGroupingPolicy,
    WeatherSystemGroupingSnapshot,
    WeatherSystemTrackInput,
    group_weather_systems,
)


CELL_TRACK_ENGINE_VERSION = "stv3-shadow-cell-track-engine-v4"
CELL_TRACK_ENGINE_STATUS = "candidate_only_unwired"
MotionPrior = MotionPrediction | TrackMotionPrior


@dataclass(frozen=True, slots=True)
class _SharedTranslationContext:
    previous_frame: BinaryRainShieldFootprintFrame
    current_frame: BinaryRainShieldFootprintFrame
    result: SharedSystemTranslationResult
    uncertainty: SystemTranslationEvidence


@dataclass(frozen=True, slots=True)
class CellTrackEnginePolicy:
    """Every bound, uncertainty and ranking coefficient is caller-owned."""

    policy_id: str
    detector: HierarchicalDetectorConfig
    lifecycle: CellTrackLifecyclePolicy
    estimator: RecursiveTrackEstimatorPolicy
    lucas_kanade: LucasKanadeConfig
    flow_aggregation: DetectionFlowAggregationConfig
    weather_system: WeatherSystemGroupingPolicy
    initial_state_covariance: Matrix4
    prior_process_noise_covariance: Matrix4
    association_motion_sigma_m_s: float
    motion_nll_weight: float
    object_kind_change_cost: float
    absolute_log_area_weight: float
    mean_value_difference_weight: float
    maximum_value_difference_weight: float
    footprint_nonoverlap_weight: float
    birth_cost: float
    death_cost: float
    exact_solution_limit: int
    maximum_exact_current_detections: int
    maximum_frame_pixels: int
    maximum_detected_objects: int
    maximum_active_tracks: int
    maximum_candidate_edges: int
    maximum_total_tracks: int
    maximum_lifecycle_events: int
    maximum_track_history_detections: int
    multi_hypothesis_identity: MultiHypothesisIdentityPolicy | None = None
    scalable_one_best_fallback: bool = False
    algorithm_version: str = CELL_TRACK_ENGINE_VERSION

    def __post_init__(self) -> None:
        _identifier(self.policy_id, "policy_id")
        for value, expected, label in (
            (self.detector, HierarchicalDetectorConfig, "detector"),
            (self.lifecycle, CellTrackLifecyclePolicy, "lifecycle"),
            (self.estimator, RecursiveTrackEstimatorPolicy, "estimator"),
            (self.lucas_kanade, LucasKanadeConfig, "lucas_kanade"),
            (
                self.flow_aggregation,
                DetectionFlowAggregationConfig,
                "flow_aggregation",
            ),
            (
                self.weather_system,
                WeatherSystemGroupingPolicy,
                "weather_system",
            ),
        ):
            if not isinstance(value, expected):
                raise TypeError(f"{label} heeft een ongeldig policytype")
        _covariance4(self.initial_state_covariance, "initial_state_covariance")
        _covariance4(
            self.prior_process_noise_covariance,
            "prior_process_noise_covariance",
        )
        for label, value in (
            ("association_motion_sigma_m_s", self.association_motion_sigma_m_s),
            ("motion_nll_weight", self.motion_nll_weight),
            ("object_kind_change_cost", self.object_kind_change_cost),
            ("absolute_log_area_weight", self.absolute_log_area_weight),
            ("mean_value_difference_weight", self.mean_value_difference_weight),
            (
                "maximum_value_difference_weight",
                self.maximum_value_difference_weight,
            ),
            ("footprint_nonoverlap_weight", self.footprint_nonoverlap_weight),
            ("birth_cost", self.birth_cost),
            ("death_cost", self.death_cost),
        ):
            _nonnegative_finite(value, label)
        for label, value, allow_zero in (
            ("exact_solution_limit", self.exact_solution_limit, False),
            (
                "maximum_exact_current_detections",
                self.maximum_exact_current_detections,
                True,
            ),
            ("maximum_frame_pixels", self.maximum_frame_pixels, False),
            ("maximum_detected_objects", self.maximum_detected_objects, True),
            ("maximum_active_tracks", self.maximum_active_tracks, True),
            ("maximum_candidate_edges", self.maximum_candidate_edges, True),
            ("maximum_total_tracks", self.maximum_total_tracks, True),
            ("maximum_lifecycle_events", self.maximum_lifecycle_events, True),
            (
                "maximum_track_history_detections",
                self.maximum_track_history_detections,
                False,
            ),
        ):
            _bounded_integer(value, label, allow_zero=allow_zero)
        if self.maximum_detected_objects > 2 * self.maximum_frame_pixels:
            raise ValueError(
                "maximum_detected_objects overschrijdt de detector hard bound"
            )
        if self.maximum_active_tracks > self.maximum_total_tracks:
            raise ValueError("maximum_active_tracks overschrijdt total track cap")
        if self.multi_hypothesis_identity is not None and not isinstance(
            self.multi_hypothesis_identity,
            MultiHypothesisIdentityPolicy,
        ):
            raise TypeError("multi_hypothesis_identity heeft verkeerd policytype")
        if (
            self.multi_hypothesis_identity is not None
            and self.multi_hypothesis_identity.multi_scan is not None
            and self.multi_hypothesis_identity.multi_scan.horizon_frames
            < self.lifecycle.maximum_gap_frames + 1
        ):
            raise ValueError("multi-scan horizon is kleiner dan lifecycle gap support")
        if type(self.scalable_one_best_fallback) is not bool:
            raise TypeError("scalable_one_best_fallback moet bool zijn")
        if self.algorithm_version != CELL_TRACK_ENGINE_VERSION:
            raise ValueError("onbekende CellTrackEngine-versie")

    @property
    def fingerprint(self) -> str:
        detector = self.detector
        values = (
            self.policy_id,
            self.lifecycle.policy_id,
            str(self.lifecycle.confirmation_observations),
            str(self.lifecycle.maximum_gap_frames),
            self.estimator.fingerprint,
            self.weather_system.fingerprint,
            *(format(value, ".17g") for value in (
                detector.shield_threshold,
                detector.core_threshold,
                detector.minimum_shield_area_m2,
                detector.minimum_core_area_m2,
                detector.shield_position_sigma_m,
                detector.core_position_sigma_m,
                self.association_motion_sigma_m_s,
                self.motion_nll_weight,
                self.object_kind_change_cost,
                self.absolute_log_area_weight,
                self.mean_value_difference_weight,
                self.maximum_value_difference_weight,
                self.footprint_nonoverlap_weight,
                self.birth_cost,
                self.death_cost,
            )),
            str(detector.connectivity),
            detector.centroid_method,
            self.lucas_kanade.fingerprint,
            self.flow_aggregation.fingerprint,
            repr(self.initial_state_covariance),
            repr(self.prior_process_noise_covariance),
            str(self.exact_solution_limit),
            str(self.maximum_exact_current_detections),
            str(self.maximum_frame_pixels),
            str(self.maximum_detected_objects),
            str(self.maximum_active_tracks),
            str(self.maximum_candidate_edges),
            str(self.maximum_total_tracks),
            str(self.maximum_lifecycle_events),
            str(self.maximum_track_history_detections),
            (
                "none"
                if self.multi_hypothesis_identity is None
                else self.multi_hypothesis_identity.fingerprint
            ),
            str(self.scalable_one_best_fallback),
            self.algorithm_version,
        )
        return hashlib.sha256("\0".join(values).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class LineageChildIdentity:
    detection_id: str
    track_id: str

    def __post_init__(self) -> None:
        _identifier(self.detection_id, "lineage child detection_id")
        _identifier(self.track_id, "lineage child track_id")


@dataclass(frozen=True, slots=True)
class CellTrackLineageHypothesis:
    """Caller-selected split/merge hypothesis; alternatives remain retained."""

    hypothesis_id: str
    event_type: Literal["split", "merge"]
    parent_track_ids: tuple[str, ...]
    children: tuple[LineageChildIdentity, ...]
    cost: float
    cost_model_id: str
    cost_components: tuple[tuple[str, float], ...]
    provenance_ids: tuple[str, ...]
    selected: bool

    def __post_init__(self) -> None:
        _identifier(self.hypothesis_id, "hypothesis_id")
        _identifier(self.cost_model_id, "cost_model_id")
        if self.event_type not in ("split", "merge"):
            raise ValueError("lineage event_type moet split of merge zijn")
        if (
            not isinstance(self.parent_track_ids, tuple)
            or self.parent_track_ids != tuple(sorted(self.parent_track_ids))
            or len(self.parent_track_ids) != len(set(self.parent_track_ids))
        ):
            raise ValueError("parent_track_ids moeten canoniek uniek zijn")
        for item in self.parent_track_ids:
            _identifier(item, "parent_track_id")
        if not isinstance(self.children, tuple) or any(
            not isinstance(item, LineageChildIdentity) for item in self.children
        ):
            raise TypeError("lineage children moeten immutable child records zijn")
        if self.children != tuple(
            sorted(self.children, key=lambda item: (item.detection_id, item.track_id))
        ):
            raise ValueError("lineage children moeten canoniek gesorteerd zijn")
        detection_ids = tuple(item.detection_id for item in self.children)
        track_ids = tuple(item.track_id for item in self.children)
        if (
            len(detection_ids) != len(set(detection_ids))
            or len(track_ids) != len(set(track_ids))
        ):
            raise ValueError("lineage child-ID's moeten uniek zijn")
        if self.event_type == "split" and (
            len(self.parent_track_ids) != 1 or len(self.children) < 2
        ):
            raise ValueError("split vereist één parent en minstens twee children")
        if self.event_type == "merge" and (
            len(self.parent_track_ids) < 2 or len(self.children) != 1
        ):
            raise ValueError("merge vereist minstens twee parents en één child")
        if not math.isfinite(self.cost):
            raise ValueError("lineage cost moet eindig zijn")
        if not isinstance(self.cost_components, tuple):
            raise TypeError("lineage cost_components moeten tuples zijn")
        component_names = tuple(item[0] for item in self.cost_components)
        if (
            len(component_names) != len(set(component_names))
            or any(not isinstance(name, str) or not name for name in component_names)
            or any(not math.isfinite(value) for _name, value in self.cost_components)
        ):
            raise ValueError("lineage costcomponenten zijn ongeldig")
        if self.cost_components and not math.isclose(
            math.fsum(value for _name, value in self.cost_components),
            self.cost,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ):
            raise ValueError("lineage cost verschilt van componentensom")
        if (
            not isinstance(self.provenance_ids, tuple)
            or not self.provenance_ids
            or self.provenance_ids != tuple(sorted(self.provenance_ids))
            or len(self.provenance_ids) != len(set(self.provenance_ids))
        ):
            raise ValueError("lineage provenance_ids moeten canoniek niet-leeg zijn")
        for item in self.provenance_ids:
            _identifier(item, "lineage provenance_id")
        if not isinstance(self.selected, bool):
            raise TypeError("lineage selected moet bool zijn")


@dataclass(frozen=True, slots=True)
class CellTrackEngineState:
    engine_id: str
    policy_fingerprint: str
    revision: int
    ledger: CellTrackLedger
    estimates: tuple[tuple[str, RecursiveTrackEstimate], ...]
    latest_hierarchical: tuple[
        tuple[str, HierarchicalCellDetection], ...
    ]
    last_frame: NormalizedRasterFrame | None
    last_detection_frame: HierarchicalDetectionFrame | None
    weather_systems: WeatherSystemGroupingSnapshot | None
    identity_snapshot: MultiHypothesisIdentitySnapshot | None = None
    multi_scan_posterior: MultiScanIdentityPosterior | None = None
    status: str = CELL_TRACK_ENGINE_STATUS
    algorithm_version: str = CELL_TRACK_ENGINE_VERSION

    def __post_init__(self) -> None:
        _identifier(self.engine_id, "engine_id")
        _sha256(self.policy_fingerprint, "policy_fingerprint")
        _bounded_integer(self.revision, "state revision", allow_zero=True)
        if not isinstance(self.ledger, CellTrackLedger):
            raise TypeError("state ledger heeft verkeerd type")
        if not isinstance(self.estimates, tuple) or not isinstance(
            self.latest_hierarchical,
            tuple,
        ):
            raise TypeError("state mappings moeten immutable tuples zijn")
        estimate_ids = tuple(item[0] for item in self.estimates)
        hierarchy_ids = tuple(item[0] for item in self.latest_hierarchical)
        track_ids = tuple(item.track_id for item in self.ledger.tracks)
        if estimate_ids != tuple(sorted(estimate_ids)) or estimate_ids != track_ids:
            raise ValueError("estimates moeten exact iedere ledgertrack afdekken")
        if hierarchy_ids != tuple(sorted(hierarchy_ids)) or hierarchy_ids != track_ids:
            raise ValueError(
                "latest_hierarchical moet exact iedere ledgertrack afdekken"
            )
        track_by_id = {item.track_id: item for item in self.ledger.tracks}
        for track_id, estimate in self.estimates:
            if not isinstance(estimate, RecursiveTrackEstimate):
                raise TypeError("state estimate heeft verkeerd type")
            track = track_by_id[track_id]
            if (
                estimate.analysis_grid_id != track.analysis_grid_id
                or estimate.supporting_detection_ids[-1]
                != track.latest_detection.detection_id
            ):
                raise ValueError("estimate en trackhistorie verschillen")
        for track_id, detected in self.latest_hierarchical:
            if not isinstance(detected, HierarchicalCellDetection):
                raise TypeError("state hierarchy heeft verkeerd type")
            if detected.detection != track_by_id[track_id].latest_detection:
                raise ValueError("hierarchische latest detectie verschilt van track")
        if (self.last_frame is None) != (self.last_detection_frame is None):
            raise ValueError("state moet beide laatste frames of geen bewaren")
        if self.last_frame is None:
            if (
                self.revision != 0
                or self.ledger.tracks
                or self.weather_systems is not None
                or self.identity_snapshot is not None
                or self.multi_scan_posterior is not None
            ):
                raise ValueError("lege state moet revision nul en geen output hebben")
        else:
            if self.revision < 1:
                raise ValueError("niet-lege state vereist positieve revision")
            detected = self.last_detection_frame
            assert detected is not None
            if (
                detected.frame_id != self.last_frame.frame_id
                or detected.valid_at != self.last_frame.valid_at
                or detected.analysis_grid_id != self.last_frame.grid.grid_id
            ):
                raise ValueError("state raster- en detectieframe verschillen")
            if self.weather_systems is None:
                raise ValueError("niet-lege state vereist afgeleide systemview")
            if (
                self.weather_systems.valid_at != self.last_frame.valid_at
                or self.weather_systems.analysis_grid_id != self.last_frame.grid.grid_id
            ):
                raise ValueError("WeatherSystem-view hoort niet bij laatste frame")
            if self.identity_snapshot is not None:
                if (
                    not isinstance(
                        self.identity_snapshot,
                        MultiHypothesisIdentitySnapshot,
                    )
                    or self.identity_snapshot.revision != self.revision
                    or self.identity_snapshot.translation.current_frame_id
                    != self.last_frame.frame_id
                ):
                    raise ValueError("identity snapshot hoort niet bij state-head")
            if self.multi_scan_posterior is not None:
                if (
                    not isinstance(
                        self.multi_scan_posterior,
                        MultiScanIdentityPosterior,
                    )
                    or self.multi_scan_posterior.revision != self.revision
                    or self.multi_scan_posterior.frame_buffer[-1].frame_id
                    != self.last_frame.frame_id
                ):
                    raise ValueError("multi-scan posterior hoort niet bij state-head")
        if self.status != CELL_TRACK_ENGINE_STATUS:
            raise ValueError("state is niet expliciet candidate-only")
        if self.algorithm_version != CELL_TRACK_ENGINE_VERSION:
            raise ValueError("onbekende state-algoritmeversie")

    def track(self, track_id: str) -> ImmutableCellTrack:
        return self.ledger.track(track_id)

    def estimate(self, track_id: str) -> RecursiveTrackEstimate:
        for identifier, estimate in self.estimates:
            if identifier == track_id:
                return estimate
        raise KeyError(track_id)

    def hierarchy(self, track_id: str) -> HierarchicalCellDetection:
        for identifier, detected in self.latest_hierarchical:
            if identifier == track_id:
                return detected
        raise KeyError(track_id)


@dataclass(frozen=True, slots=True)
class CellTrackTransitionResult:
    transition_id: str
    status: Literal["advanced", "indeterminate"]
    reason_codes: tuple[str, ...]
    input_state_revision: int
    detection_frame: HierarchicalDetectionFrame
    next_state: CellTrackEngineState
    motion_priors: tuple[MotionPrior, ...]
    flow_field: LucasKanadeFlowField | None
    flow_aggregations: tuple[DetectionFlowAggregation, ...]
    association_graphs: tuple[DetectionAssociationGraph, ...]
    association_feature_tables: tuple[AssociationFeatureTable, ...]
    assignment_problem: GlobalAssignmentProblem | None
    assignment_result: GlobalAssignmentResult | None
    lineage_hypotheses: tuple[CellTrackLineageHypothesis, ...]
    applied_lineage_hypothesis_ids: tuple[str, ...]
    provider_handoff_event: ProviderHandoffEvent | None
    candidate_edge_count: int
    identity_snapshot: MultiHypothesisIdentitySnapshot | None = None
    multi_scan_posterior: MultiScanIdentityPosterior | None = None
    algorithm_version: str = CELL_TRACK_ENGINE_VERSION

    def __post_init__(self) -> None:
        _identifier(self.transition_id, "transition_id")
        if self.status not in ("advanced", "indeterminate"):
            raise ValueError("ongeldige transitionstatus")
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise ValueError("reason_codes moeten canoniek zijn")
        _bounded_integer(
            self.input_state_revision,
            "input_state_revision",
            allow_zero=True,
        )
        if not isinstance(self.detection_frame, HierarchicalDetectionFrame):
            raise TypeError("transition vereist detection_frame")
        if not isinstance(self.next_state, CellTrackEngineState):
            raise TypeError("transition vereist next_state")
        if (
            not isinstance(self.motion_priors, tuple)
            or any(
                not isinstance(item, (MotionPrediction, TrackMotionPrior))
                for item in self.motion_priors
            )
        ):
            raise TypeError("motion_priors moeten immutable bestaande primitives zijn")
        prior_ids = tuple(item.detection_id for item in self.motion_priors)
        if prior_ids != tuple(sorted(prior_ids)) or len(prior_ids) != len(
            set(prior_ids)
        ):
            raise ValueError("motion_priors moeten canoniek uniek zijn")
        if self.flow_field is not None and not isinstance(
            self.flow_field,
            LucasKanadeFlowField,
        ):
            raise TypeError("flow_field heeft verkeerd type")
        for values, expected, label in (
            (self.flow_aggregations, DetectionFlowAggregation, "flow aggregations"),
            (self.association_graphs, DetectionAssociationGraph, "graphs"),
            (
                self.association_feature_tables,
                AssociationFeatureTable,
                "feature tables",
            ),
            (
                self.lineage_hypotheses,
                CellTrackLineageHypothesis,
                "lineage hypotheses",
            ),
        ):
            if not isinstance(values, tuple) or any(
                not isinstance(item, expected) for item in values
            ):
                raise TypeError(f"{label} moeten immutable records zijn")
        if self.lineage_hypotheses != tuple(
            sorted(self.lineage_hypotheses, key=lambda item: item.hypothesis_id)
        ):
            raise ValueError("lineage hypotheses moeten canoniek zijn")
        if self.applied_lineage_hypothesis_ids != tuple(
            sorted(self.applied_lineage_hypothesis_ids)
        ):
            raise ValueError("applied lineage-ID's moeten gesorteerd zijn")
        selected_ids = {
            item.hypothesis_id for item in self.lineage_hypotheses if item.selected
        }
        if not set(self.applied_lineage_hypothesis_ids) <= selected_ids:
            raise ValueError("toegepaste lineagehypothese was niet geselecteerd")
        if self.assignment_problem is not None and not isinstance(
            self.assignment_problem,
            GlobalAssignmentProblem,
        ):
            raise TypeError("assignment_problem heeft verkeerd type")
        if self.assignment_result is not None:
            if not isinstance(self.assignment_result, GlobalAssignmentResult):
                raise TypeError("assignment_result heeft verkeerd type")
            if self.assignment_result.problem != self.assignment_problem:
                raise ValueError("assignment result en problem verschillen")
        if self.provider_handoff_event is not None and not isinstance(
            self.provider_handoff_event,
            ProviderHandoffEvent,
        ):
            raise TypeError("provider_handoff_event heeft verkeerd type")
        _nonnegative_count(self.candidate_edge_count, "candidate_edge_count")
        if self.identity_snapshot is not None:
            if not isinstance(
                self.identity_snapshot,
                MultiHypothesisIdentitySnapshot,
            ):
                raise TypeError("transition identity_snapshot heeft verkeerd type")
            if self.identity_snapshot.revision != self.input_state_revision + 1:
                raise ValueError("transition identity_snapshot revision verschilt")
            if self.identity_snapshot.candidate_edge_count != self.candidate_edge_count:
                raise ValueError("transition identity candidate accounting verschilt")
        if self.multi_scan_posterior is not None:
            if (
                not isinstance(
                    self.multi_scan_posterior,
                    MultiScanIdentityPosterior,
                )
                or self.multi_scan_posterior.revision
                != self.input_state_revision + 1
            ):
                raise ValueError("transition multi-scan posterior revision verschilt")
        if self.status == "advanced":
            if self.reason_codes:
                raise ValueError("advanced transition mag geen failure reason hebben")
            if self.next_state.revision != self.input_state_revision + 1:
                raise ValueError("advanced transition moet state revision verhogen")
            if self.next_state.multi_scan_posterior != self.multi_scan_posterior:
                raise ValueError("advanced transition/state multi-scan verschilt")
        elif self.next_state.revision != self.input_state_revision:
            raise ValueError("indeterminate transition mag state niet verhogen")
        if self.algorithm_version != CELL_TRACK_ENGINE_VERSION:
            raise ValueError("onbekende transition-algoritmeversie")


def create_cell_track_engine_state(
    engine_id: str,
    policy: CellTrackEnginePolicy,
) -> CellTrackEngineState:
    """Create one empty immutable state bound to an exact policy fingerprint."""

    _identifier(engine_id, "engine_id")
    _policy(policy)
    return CellTrackEngineState(
        engine_id=engine_id,
        policy_fingerprint=policy.fingerprint,
        revision=0,
        ledger=CellTrackLedger(),
        estimates=(),
        latest_hierarchical=(),
        last_frame=None,
        last_detection_frame=None,
        weather_systems=None,
        identity_snapshot=None,
        multi_scan_posterior=None,
    )


def advance_cell_track_engine(
    state: CellTrackEngineState,
    frame: NormalizedRasterFrame,
    policy: CellTrackEnginePolicy,
    *,
    lineage_hypotheses: Iterable[CellTrackLineageHypothesis] = (),
    provider_handoff_event: ProviderHandoffEvent | None = None,
) -> CellTrackTransitionResult:
    """Advance one multi-frame shadow state without mutating any input.

    Invalid temporal/profile input raises before scientific work.  A valid but
    computationally unadmitted batch returns ``indeterminate`` with the full
    detection frame and the exact unchanged state.
    """

    if not isinstance(state, CellTrackEngineState):
        raise TypeError("CellTrackEngine vereist een immutable state")
    if not isinstance(frame, NormalizedRasterFrame):
        raise TypeError("CellTrackEngine vereist NormalizedRasterFrame")
    _policy(policy)
    if state.policy_fingerprint != policy.fingerprint:
        raise ValueError("CellTrackEngine-policy drift tussen frames")
    if frame.grid.width * frame.grid.height > policy.maximum_frame_pixels:
        raise ValueError("raster overschrijdt de immutable input-pixelbound")
    if len(state.ledger.tracks) > policy.maximum_total_tracks:
        raise ValueError("bestaande state overschrijdt immutable total-trackbound")
    if len(state.ledger.lineage_events) > policy.maximum_lifecycle_events:
        raise ValueError("bestaande state overschrijdt lifecycle-eventbound")
    if any(
        track.sample_count > policy.maximum_track_history_detections
        for track in state.ledger.tracks
    ):
        raise ValueError("bestaande state overschrijdt track-historybound")
    if state.last_frame is not None:
        if frame.valid_at <= state.last_frame.valid_at:
            raise ValueError("frame valid_at moet strikt meteorologisch oplopen")
        if frame.frame_id == state.last_frame.frame_id:
            raise ValueError("opeenvolgende frames vereisen unieke frame-ID's")

    hypotheses = _canonical_hypotheses(lineage_hypotheses)
    detection_frame = detect_hierarchical_cells(frame, policy.detector)
    transition_seed_base = (
        state.engine_id,
        state.policy_fingerprint,
        str(state.revision),
        _state_digest(state),
        _frame_digest(frame),
    )
    transition_seed = (
        *transition_seed_base,
        *(_hypothesis_digest(item) for item in hypotheses),
    )
    if state.last_frame is None:
        if provider_handoff_event is not None:
            raise ValueError("bootstrapframe mag geen provider-handoff event hebben")
        handoff_reasons: tuple[str, ...] = ()
    else:
        handoff_reasons = _handoff_reasons(
            state.last_frame,
            frame,
            provider_handoff_event,
        )

    if len(detection_frame.detections) > policy.maximum_detected_objects:
        return _indeterminate(
            state,
            detection_frame,
            hypotheses=hypotheses,
            reasons=("detected_object_capacity_exceeded", *handoff_reasons),
            transition_seed=transition_seed,
            provider_handoff_event=provider_handoff_event,
        )
    grouping_pair_count = (
        len(detection_frame.detections)
        * (len(detection_frame.detections) - 1)
        // 2
    )
    if grouping_pair_count > policy.maximum_candidate_edges:
        return _indeterminate(
            state,
            detection_frame,
            hypotheses=hypotheses,
            reasons=("weather_system_pair_capacity_exceeded", *handoff_reasons),
            transition_seed=transition_seed,
            provider_handoff_event=provider_handoff_event,
        )

    if state.last_frame is None:
        if hypotheses:
            raise ValueError("bootstrapframe kan geen split/mergehypothese toepassen")
        return _bootstrap(
            state,
            frame,
            detection_frame,
            policy,
            transition_seed,
        )

    if handoff_reasons:
        return _indeterminate(
            state,
            detection_frame,
            hypotheses=hypotheses,
            reasons=handoff_reasons,
            transition_seed=transition_seed,
            provider_handoff_event=provider_handoff_event,
        )
    if provider_handoff_event is not None:
        raise ValueError("provider_handoff_event zonder meetdiscontinuïteit")

    active_tracks = tuple(
        track
        for track in state.ledger.tracks
        if track.lifecycle_state != "ended"
    )
    if len(active_tracks) > policy.maximum_active_tracks:
        return _indeterminate(
            state,
            detection_frame,
            hypotheses=hypotheses,
            reasons=("active_track_capacity_exceeded",),
            transition_seed=transition_seed,
        )
    candidate_edge_count = len(active_tracks) * len(detection_frame.detections)
    if candidate_edge_count > policy.maximum_candidate_edges:
        return _indeterminate(
            state,
            detection_frame,
            hypotheses=hypotheses,
            reasons=("complete_candidate_product_capacity_exceeded",),
            transition_seed=transition_seed,
            candidate_edge_count=candidate_edge_count,
        )

    current_by_id = {
        item.detection.detection_id: item
        for item in detection_frame.detections
    }
    track_by_latest = {
        item.latest_detection.detection_id: item for item in active_tracks
    }
    if len(track_by_latest) != len(active_tracks):
        raise ValueError("actieve tracks delen ten onrechte een latest detection")
    if current_by_id:
        flow_field, flow_aggregations = _flow_evidence(
            state,
            frame,
            active_tracks,
            policy,
        )
    else:
        flow_field, flow_aggregations = None, ()
    motion_priors = _motion_priors(
        state,
        frame,
        active_tracks,
        flow_aggregations,
        policy,
    )
    graphs, feature_tables = _complete_association_evidence(
        state,
        detection_frame,
        active_tracks,
        motion_priors,
        policy,
    )
    actual_pairs = {
        (
            feature.previous_detection_id,
            feature.current_detection_id,
        )
        for table in feature_tables
        for feature in table.features
    }
    expected_pairs = {
        (track.latest_detection.detection_id, current_id)
        for track in active_tracks
        for current_id in current_by_id
    }
    if actual_pairs != expected_pairs:
        raise AssertionError("compositor verloor een associationcandidate")

    translation: SystemTranslationEvidence | None = None
    system_translations: tuple[SystemTranslationEvidence, ...] = ()
    advected_evidence_by_pair: dict[
        tuple[str, str], AdvectedFootprintEvidence
    ] = {}
    translation_uncertainty_by_result_sha256: dict[
        str, SystemTranslationEvidence
    ] = {}
    local_candidates: tuple[LocalCellDeformationCandidate, ...] = ()
    derived_hypotheses: tuple[CellTrackLineageHypothesis, ...] = ()
    lineage_eligible_previous = 0
    lineage_ineligible_gap = 0
    identity_policy = policy.multi_hypothesis_identity
    if identity_policy is not None:
        immediately_previous = tuple(
            state.hierarchy(track.track_id)
            for track in active_tracks
            if math.isclose(
                track.latest_detection.valid_at,
                state.last_frame.valid_at,
                rel_tol=0.0,
                abs_tol=1e-6,
            )
        )
        lineage_eligible_previous = len(immediately_previous)
        lineage_ineligible_gap = len(active_tracks) - lineage_eligible_previous
        previous_for_evidence = {
            track.latest_detection.detection_id: state.hierarchy(track.track_id)
            for track in active_tracks
        }
        if state.multi_scan_posterior is not None:
            buffered = {
                item.detection.detection_id: item
                for buffered_frame in state.multi_scan_posterior.frame_buffer
                for item in buffered_frame.detections
            }
            for branch in state.multi_scan_posterior.branches:
                for path in branch.paths:
                    item = buffered.get(path.latest_detection_id)
                    if item is None:
                        return _indeterminate(
                            state,
                            detection_frame,
                            hypotheses=hypotheses,
                            reasons=("multi_scan_branch_detection_support_missing",),
                            transition_seed=transition_seed,
                            motion_priors=motion_priors,
                            flow_field=flow_field,
                            flow_aggregations=flow_aggregations,
                            graphs=graphs,
                            feature_tables=feature_tables,
                            candidate_edge_count=candidate_edge_count,
                        )
                    existing = previous_for_evidence.get(path.latest_detection_id)
                    if existing is not None and existing != item:
                        raise AssertionError("multi-scan detection support collision")
                    previous_for_evidence[path.latest_detection_id] = item
        (
            translation,
            system_translations,
            translation_uncertainty_by_result_sha256,
            advected_evidence_by_pair,
            translation_reasons,
        ) = _build_advected_association_evidence(
            state=state,
            frame=frame,
            detection_frame=detection_frame,
            previous=tuple(previous_for_evidence.values()),
            policy=identity_policy,
        )
        if translation_reasons:
            return _indeterminate(
                state,
                detection_frame,
                hypotheses=hypotheses,
                reasons=translation_reasons,
                transition_seed=transition_seed,
                motion_priors=motion_priors,
                flow_field=flow_field,
                flow_aggregations=flow_aggregations,
                graphs=graphs,
                feature_tables=feature_tables,
                candidate_edge_count=candidate_edge_count,
            )
        derived_hypotheses, lineage_capacity_reasons = _derive_lineage_hypotheses(
            state=state,
            frame=frame,
            previous=immediately_previous,
            current=detection_frame.detections,
            advected_evidence_by_pair=advected_evidence_by_pair,
            policy=identity_policy,
        )
        if lineage_capacity_reasons:
            return _indeterminate(
                state,
                detection_frame,
                hypotheses=hypotheses,
                reasons=lineage_capacity_reasons,
                transition_seed=transition_seed,
                motion_priors=motion_priors,
                flow_field=flow_field,
                flow_aggregations=flow_aggregations,
                graphs=graphs,
                feature_tables=feature_tables,
                candidate_edge_count=candidate_edge_count,
            )
        hypothesis_by_id = {item.hypothesis_id: item for item in hypotheses}
        for item in derived_hypotheses:
            existing = hypothesis_by_id.get(item.hypothesis_id)
            if existing is not None and existing != item:
                raise ValueError("derived lineage hypothesis-ID collision")
            hypothesis_by_id[item.hypothesis_id] = item
        hypotheses = _canonical_hypotheses(hypothesis_by_id.values())
        transition_seed = (
            *transition_seed_base,
            *(_hypothesis_digest(item) for item in hypotheses),
        )
        all_features = tuple(
            feature for table in feature_tables for feature in table.features
        )
        try:
            local_candidates = build_local_deformation_candidates(
                features=all_features,
                advected_evidence_by_pair={
                    pair: advected_evidence_by_pair[pair]
                    for pair in actual_pairs
                },
                translation_uncertainty_by_result_sha256=(
                    translation_uncertainty_by_result_sha256
                ),
                previous_snapshot=state.identity_snapshot,
                policy=identity_policy,
            )
        except ValueError as exc:
            if str(exc) != "local_deformation_candidate_capacity_exceeded":
                raise
            return _indeterminate(
                state,
                detection_frame,
                hypotheses=hypotheses,
                reasons=("local_deformation_candidate_capacity_exceeded",),
                transition_seed=transition_seed,
                motion_priors=motion_priors,
                flow_field=flow_field,
                flow_aggregations=flow_aggregations,
                graphs=graphs,
                feature_tables=feature_tables,
                candidate_edge_count=candidate_edge_count,
            )

    selected = _validate_lineage_hypotheses(
        hypotheses,
        active_tracks=active_tracks,
        current_by_id=current_by_id,
        existing_track_ids={item.track_id for item in state.ledger.tracks},
    )
    lineage_parent_ids = {
        track_id for item in selected for track_id in item.parent_track_ids
    }
    lineage_detection_ids = {
        child.detection_id for item in selected for child in item.children
    }
    assignment_previous = tuple(
        sorted(
            track.latest_detection.detection_id
            for track in active_tracks
            if track.track_id not in lineage_parent_ids
        )
    )
    assignment_current = tuple(
        sorted(set(current_by_id) - lineage_detection_ids)
    )
    features_by_pair = {
        (item.previous_detection_id, item.current_detection_id): item
        for table in feature_tables
        for item in table.features
    }
    local_by_pair = {
        (item.previous_detection_id, item.current_detection_id): item
        for item in local_candidates
    }
    all_edge_costs = tuple(
        _edge_cost(
            feature,
            policy,
            local_candidate=local_by_pair.get((
                feature.previous_detection_id,
                feature.current_detection_id,
            )),
        )
        for _pair, feature in sorted(features_by_pair.items())
    )
    if identity_policy is not None:
        local_candidates = bind_local_candidate_costs(
            local_candidates=local_candidates,
            edge_costs=all_edge_costs,
        )
        local_by_pair = {
            (item.previous_detection_id, item.current_detection_id): item
            for item in local_candidates
        }
    all_edge_by_pair = {
        (item.previous_detection_id, item.current_detection_id): item
        for item in all_edge_costs
    }
    edge_costs = tuple(
        all_edge_by_pair[(previous_id, current_id)]
        for previous_id in assignment_previous
        for current_id in assignment_current
    )
    problem = GlobalAssignmentProblem(
        previous_detection_ids=assignment_previous,
        current_detection_ids=assignment_current,
        edge_costs=edge_costs,
        death_costs=tuple(
            (item, float(policy.death_cost)) for item in assignment_previous
        ),
        birth_costs=tuple(
            (item, float(policy.birth_cost)) for item in assignment_current
        ),
        policy_id=f"cell-track-engine:{policy.fingerprint}",
    )
    if identity_policy is not None:
        assignment_result = solve_bounded_k_best_global_assignment(
            problem,
            solution_limit=identity_policy.solution_limit,
            maximum_subproblems=identity_policy.maximum_assignment_subproblems,
        )
    elif len(assignment_current) > policy.maximum_exact_current_detections:
        if not policy.scalable_one_best_fallback:
            return _indeterminate(
                state,
                detection_frame,
                hypotheses=hypotheses,
                reasons=("exact_global_assignment_capacity_exceeded",),
                transition_seed=transition_seed,
                motion_priors=motion_priors,
                flow_field=flow_field,
                flow_aggregations=flow_aggregations,
                graphs=graphs,
                feature_tables=feature_tables,
                assignment_problem=problem,
                candidate_edge_count=candidate_edge_count,
            )
        assignment_result = solve_global_assignment_one_best(problem)
    else:
        assignment_result = solve_exact_global_assignment(
            problem,
            solution_limit=policy.exact_solution_limit,
            maximum_current_detections=(
                policy.maximum_exact_current_detections
            ),
        )

    identity_snapshot = None
    multi_scan_posterior = None
    if identity_policy is not None:
        assert translation is not None
        identity_snapshot = finalize_identity_snapshot(
            revision=state.revision + 1,
            translation=translation,
            system_translations=system_translations,
            local_candidates=local_candidates,
            expected_candidate_edge_count=candidate_edge_count,
            assignment=assignment_result,
            derived_lineage_hypothesis_ids=tuple(
                item.hypothesis_id for item in derived_hypotheses
            ),
            lineage_eligible_previous_objects=lineage_eligible_previous,
            lineage_ineligible_gap_objects=lineage_ineligible_gap,
            policy=identity_policy,
        )
        if assignment_result.capacity_exhausted:
            return _indeterminate(
                state,
                detection_frame,
                hypotheses=hypotheses,
                reasons=("top_k_assignment_subproblem_capacity_exceeded",),
                transition_seed=transition_seed,
                motion_priors=motion_priors,
                flow_field=flow_field,
                flow_aggregations=flow_aggregations,
                graphs=graphs,
                feature_tables=feature_tables,
                assignment_problem=problem,
                assignment_result=assignment_result,
                candidate_edge_count=candidate_edge_count,
                identity_snapshot=identity_snapshot,
            )
        if identity_policy.multi_scan is not None:
            if state.multi_scan_posterior is None:
                raise ValueError("multi-scan state ontbreekt na bootstrap")
            multi_scan_result = advance_multi_scan_identity(
                engine_id=state.engine_id,
                previous=state.multi_scan_posterior,
                detection_frame=detection_frame,
                advected_evidence_by_pair={
                    pair: advected_evidence_by_pair[pair]
                    for pair in {
                        (path.latest_detection_id, current_id)
                        for branch in state.multi_scan_posterior.branches
                        for path in branch.paths
                        for current_id in current_by_id
                    }
                },
                translation_uncertainty_by_result_sha256=(
                    translation_uncertainty_by_result_sha256
                ),
                policy=identity_policy,
                ranking=MultiScanRankingWeights(
                    object_kind_change_cost=policy.object_kind_change_cost,
                    absolute_log_area_weight=policy.absolute_log_area_weight,
                    mean_value_difference_weight=policy.mean_value_difference_weight,
                    maximum_value_difference_weight=(
                        policy.maximum_value_difference_weight
                    ),
                    footprint_nonoverlap_weight=policy.footprint_nonoverlap_weight,
                    birth_cost=policy.birth_cost,
                    death_cost=policy.death_cost,
                ),
                maximum_gap_frames=policy.lifecycle.maximum_gap_frames,
            )
            if multi_scan_result.status != "advanced":
                return _indeterminate(
                    state,
                    detection_frame,
                    hypotheses=hypotheses,
                    reasons=multi_scan_result.reason_codes,
                    transition_seed=transition_seed,
                    motion_priors=motion_priors,
                    flow_field=flow_field,
                    flow_aggregations=flow_aggregations,
                    graphs=graphs,
                    feature_tables=feature_tables,
                    assignment_problem=problem,
                    assignment_result=assignment_result,
                    candidate_edge_count=candidate_edge_count,
                    identity_snapshot=identity_snapshot,
                )
            multi_scan_posterior = multi_scan_result.posterior
            assert multi_scan_posterior is not None

    capacity_reasons = _transition_capacity_reasons(
        state,
        assignment_result,
        selected,
        track_by_latest,
        policy,
    )
    if capacity_reasons:
        return _indeterminate(
            state,
            detection_frame,
            hypotheses=hypotheses,
            reasons=capacity_reasons,
            transition_seed=transition_seed,
            motion_priors=motion_priors,
            flow_field=flow_field,
            flow_aggregations=flow_aggregations,
            graphs=graphs,
            feature_tables=feature_tables,
            assignment_problem=problem,
            assignment_result=assignment_result,
            candidate_edge_count=candidate_edge_count,
        )

    ledger, estimates, hierarchy = _apply_transition(
        state,
        detection_frame,
        assignment_result,
        selected,
        policy,
    )
    alternatives = _identity_alternatives(
        state,
        assignment_result,
        hypotheses,
        ledger,
    )
    systems = _group_systems(
        state,
        frame,
        ledger,
        estimates,
        hierarchy,
        alternatives,
        policy,
    )
    next_state = CellTrackEngineState(
        engine_id=state.engine_id,
        policy_fingerprint=state.policy_fingerprint,
        revision=state.revision + 1,
        ledger=ledger,
        estimates=tuple(sorted(estimates.items())),
        latest_hierarchical=tuple(sorted(hierarchy.items())),
        last_frame=frame,
        last_detection_frame=detection_frame,
        weather_systems=systems,
        identity_snapshot=identity_snapshot,
        multi_scan_posterior=multi_scan_posterior,
    )
    return CellTrackTransitionResult(
        transition_id=_digest_id("cell-track-transition", *transition_seed),
        status="advanced",
        reason_codes=(),
        input_state_revision=state.revision,
        detection_frame=detection_frame,
        next_state=next_state,
        motion_priors=motion_priors,
        flow_field=flow_field,
        flow_aggregations=flow_aggregations,
        association_graphs=graphs,
        association_feature_tables=feature_tables,
        assignment_problem=problem,
        assignment_result=assignment_result,
        lineage_hypotheses=hypotheses,
        applied_lineage_hypothesis_ids=tuple(
            item.hypothesis_id for item in selected
        ),
        provider_handoff_event=None,
        candidate_edge_count=candidate_edge_count,
        identity_snapshot=identity_snapshot,
        multi_scan_posterior=multi_scan_posterior,
    )


def _bootstrap(
    state: CellTrackEngineState,
    frame: NormalizedRasterFrame,
    detected: HierarchicalDetectionFrame,
    policy: CellTrackEnginePolicy,
    transition_seed: tuple[str, ...],
) -> CellTrackTransitionResult:
    if len(detected.detections) > policy.maximum_active_tracks:
        return _indeterminate(
            state,
            detected,
            hypotheses=(),
            reasons=("bootstrap_active_track_capacity_exceeded",),
            transition_seed=transition_seed,
        )
    if len(detected.detections) > policy.maximum_total_tracks:
        return _indeterminate(
            state,
            detected,
            hypotheses=(),
            reasons=("total_track_capacity_exceeded",),
            transition_seed=transition_seed,
        )
    if len(detected.detections) > policy.maximum_lifecycle_events:
        return _indeterminate(
            state,
            detected,
            hypotheses=(),
            reasons=("lifecycle_event_capacity_exceeded",),
            transition_seed=transition_seed,
        )

    multi_scan_posterior = None
    identity_policy = policy.multi_hypothesis_identity
    if identity_policy is not None and identity_policy.multi_scan is not None:
        try:
            multi_scan_posterior = bootstrap_multi_scan_identity(
                engine_id=state.engine_id,
                detection_frame=detected,
                policy=identity_policy,
            )
        except ValueError as exc:
            if str(exc) not in (
                "multi_scan_active_path_capacity_exceeded",
                "multi_scan_buffered_detection_capacity_exceeded",
            ):
                raise
            return _indeterminate(
                state,
                detected,
                hypotheses=(),
                reasons=(str(exc),),
                transition_seed=transition_seed,
            )

    ledger = state.ledger
    estimates: dict[str, RecursiveTrackEstimate] = {}
    hierarchy: dict[str, HierarchicalCellDetection] = {}
    for item in detected.detections:
        detection_id = item.detection.detection_id
        track_id = _birth_track_id(state.engine_id, detection_id)
        event_id = _event_id("birth", state.revision, frame.frame_id, track_id)
        ledger = record_cell_track_birth(
            ledger,
            event_id=event_id,
            frame_valid_at=frame.valid_at,
            track_id=track_id,
            detection=item.detection,
            object_kind=item.object_kind,
            policy=policy.lifecycle,
            reason="detected_without_predecessor_on_bootstrap",
        )
        estimates[track_id] = _initialize_estimate(
            track_id,
            item,
            policy,
        )
        hierarchy[track_id] = item
    systems = _group_systems(
        state,
        frame,
        ledger,
        estimates,
        hierarchy,
        {},
        policy,
    )
    next_state = CellTrackEngineState(
        engine_id=state.engine_id,
        policy_fingerprint=state.policy_fingerprint,
        revision=1,
        ledger=ledger,
        estimates=tuple(sorted(estimates.items())),
        latest_hierarchical=tuple(sorted(hierarchy.items())),
        last_frame=frame,
        last_detection_frame=detected,
        weather_systems=systems,
        multi_scan_posterior=multi_scan_posterior,
    )
    return CellTrackTransitionResult(
        transition_id=_digest_id("cell-track-transition", *transition_seed),
        status="advanced",
        reason_codes=(),
        input_state_revision=0,
        detection_frame=detected,
        next_state=next_state,
        motion_priors=(),
        flow_field=None,
        flow_aggregations=(),
        association_graphs=(),
        association_feature_tables=(),
        assignment_problem=None,
        assignment_result=None,
        lineage_hypotheses=(),
        applied_lineage_hypothesis_ids=(),
        provider_handoff_event=None,
        candidate_edge_count=0,
        multi_scan_posterior=multi_scan_posterior,
    )


def _flow_evidence(
    state: CellTrackEngineState,
    current: NormalizedRasterFrame,
    active_tracks: tuple[ImmutableCellTrack, ...],
    policy: CellTrackEnginePolicy,
) -> tuple[LucasKanadeFlowField | None, tuple[DetectionFlowAggregation, ...]]:
    previous = state.last_frame
    if previous is None or not active_tracks:
        return None, ()
    eligible = tuple(
        track
        for track in active_tracks
        if math.isclose(
            track.latest_detection.valid_at,
            previous.valid_at,
            rel_tol=0.0,
            abs_tol=1e-6,
        )
    )
    if not eligible:
        return None, ()
    field = estimate_lucas_kanade_flow(previous, current, policy.lucas_kanade)
    aggregations = tuple(
        sorted(
            (
                aggregate_detection_flow_sample(
                    field,
                    state.hierarchy(track.track_id),
                    policy.flow_aggregation,
                )
                for track in eligible
            ),
            key=lambda item: item.detection_id,
        )
    )
    return field, aggregations


def _motion_priors(
    state: CellTrackEngineState,
    frame: NormalizedRasterFrame,
    active_tracks: tuple[ImmutableCellTrack, ...],
    flow_aggregations: tuple[DetectionFlowAggregation, ...],
    policy: CellTrackEnginePolicy,
) -> tuple[MotionPrior, ...]:
    flow_by_detection = {
        item.detection_id: item for item in flow_aggregations
    }
    priors: list[MotionPrior] = []
    for track in sorted(active_tracks, key=lambda item: item.latest_detection.detection_id):
        latest = track.latest_detection
        prior_id = _digest_id(
            "track-prior",
            state.engine_id,
            track.track_id,
            latest.detection_id,
            frame.frame_id,
        )
        if track.sample_count >= 2:
            priors.append(
                build_two_position_track_prior(
                    prior_id,
                    track.detections[-2],
                    latest,
                    target_valid_at=frame.valid_at,
                    process_noise_covariance=policy.prior_process_noise_covariance,
                )
            )
            continue
        aggregation = flow_by_detection.get(latest.detection_id)
        if aggregation is not None and aggregation.available:
            assert aggregation.sample is not None
            priors.append(
                predict_with_optical_flow(
                    latest,
                    frame.valid_at,
                    aggregation.sample,
                )
            )
            continue
        priors.append(
            build_cold_start_track_prior(
                prior_id,
                latest,
                target_valid_at=frame.valid_at,
                vx_m_s=0.0,
                vy_m_s=0.0,
                caller_initial_state_covariance=policy.initial_state_covariance,
                process_noise_covariance=policy.prior_process_noise_covariance,
                method="cold_start_zero",
            )
        )
    return tuple(priors)


def _complete_association_evidence(
    state: CellTrackEngineState,
    current: HierarchicalDetectionFrame,
    active_tracks: tuple[ImmutableCellTrack, ...],
    priors: tuple[MotionPrior, ...],
    policy: CellTrackEnginePolicy,
) -> tuple[
    tuple[DetectionAssociationGraph, ...],
    tuple[AssociationFeatureTable, ...],
]:
    if not active_tracks or not current.detections:
        return (), ()
    prior_by_detection = {item.detection_id: item for item in priors}
    cohorts: dict[float, list[ImmutableCellTrack]] = {}
    for track in active_tracks:
        cohorts.setdefault(track.latest_detection.valid_at, []).append(track)
    graphs: list[DetectionAssociationGraph] = []
    tables: list[AssociationFeatureTable] = []
    for valid_at in sorted(cohorts):
        tracks = tuple(
            sorted(
                cohorts[valid_at],
                key=lambda item: item.latest_detection.detection_id,
            )
        )
        hierarchical = tuple(state.hierarchy(item.track_id) for item in tracks)
        cohort_priors = tuple(
            prior_by_detection[item.latest_detection.detection_id]
            for item in tracks
        )
        graphs.append(
            build_detection_association_graph(
                (item.latest_detection for item in tracks),
                (item.detection for item in current.detections),
                motion_sigma_m_s=policy.association_motion_sigma_m_s,
            )
        )
        tables.append(
            build_association_feature_table(
                hierarchical,
                current.detections,
                cohort_priors,
            )
        )
    return tuple(graphs), tuple(tables)


def _edge_cost(
    feature: AssociationFeatureVector,
    policy: CellTrackEnginePolicy,
    *,
    local_candidate: LocalCellDeformationCandidate | None = None,
) -> AssignmentEdgeCost:
    if policy.multi_hypothesis_identity is None:
        motion_component = (
            "motion_nll",
            policy.motion_nll_weight * feature.gaussian_motion_nll,
        )
    else:
        if local_candidate is None:
            raise AssertionError("multi-hypothesis edge mist local deformation")
        if (
            local_candidate.previous_detection_id != feature.previous_detection_id
            or local_candidate.current_detection_id != feature.current_detection_id
        ):
            raise AssertionError("local deformation hoort bij andere candidate edge")
        motion_component = (
            "local_deformation_given_system_translation",
            local_candidate.weighted_cost,
        )
    footprint_iou = (
        feature.footprint_iou_unadvected
        if local_candidate is None
        else local_candidate.advected_footprint_evidence.iou
    )
    footprint_component_name = (
        "footprint_nonoverlap"
        if local_candidate is None
        else (
            "advected_footprint_nonoverlap"
            if local_candidate.advected_footprint_evidence.mode == "advected"
            else "unadvected_fallback_footprint_nonoverlap"
        )
    )
    components = (
        motion_component,
        (
            "object_kind_change",
            0.0 if feature.same_object_kind else policy.object_kind_change_cost,
        ),
        (
            "absolute_log_area_ratio",
            policy.absolute_log_area_weight * feature.absolute_log_area_ratio,
        ),
        (
            "absolute_mean_value_difference",
            policy.mean_value_difference_weight
            * abs(feature.mean_value_difference),
        ),
        (
            "absolute_maximum_value_difference",
            policy.maximum_value_difference_weight
            * abs(feature.maximum_value_difference),
        ),
        (
            footprint_component_name,
            policy.footprint_nonoverlap_weight
            * (1.0 - footprint_iou),
        ),
    )
    cost = math.fsum(value for _name, value in components)
    if not math.isfinite(cost):
        raise ValueError("association cost overschrijdt numeriek bereik")
    return AssignmentEdgeCost(
        previous_detection_id=feature.previous_detection_id,
        current_detection_id=feature.current_detection_id,
        cost=cost,
        cost_model_id=f"cell-track-engine-cost:{policy.fingerprint}",
        components=components,
    )


def _apply_transition(
    state: CellTrackEngineState,
    detected: HierarchicalDetectionFrame,
    assignment: GlobalAssignmentResult,
    selected_lineage: tuple[CellTrackLineageHypothesis, ...],
    policy: CellTrackEnginePolicy,
) -> tuple[
    CellTrackLedger,
    dict[str, RecursiveTrackEstimate],
    dict[str, HierarchicalCellDetection],
]:
    ledger = state.ledger
    estimates = dict(state.estimates)
    hierarchy = dict(state.latest_hierarchical)
    current = {
        item.detection.detection_id: item for item in detected.detections
    }
    latest_to_track = {
        item.latest_detection.detection_id: item
        for item in state.ledger.tracks
        if item.lifecycle_state != "ended"
    }
    best = assignment.best

    for previous_id, current_id in best.matches:
        old_track = latest_to_track[previous_id]
        item = current[current_id]
        if old_track.lifecycle_state == "coasting":
            ledger = record_cell_track_reacquired(
                ledger,
                event_id=_event_id(
                    "reacquired",
                    state.revision,
                    detected.frame_id,
                    old_track.track_id,
                    current_id,
                ),
                frame_valid_at=detected.valid_at,
                track_id=old_track.track_id,
                detection=item.detection,
                object_kind=item.object_kind,
                policy=policy.lifecycle,
                reason="exact_global_assignment_after_gap",
            )
        else:
            updated = append_cell_detection(
                ledger.track(old_track.track_id),
                item.detection,
                item.object_kind,
                policy.lifecycle,
            )
            ledger = _replace_track_revision(ledger, updated)
        estimates[old_track.track_id] = assimilate_recursive_detection(
            estimates[old_track.track_id],
            item.detection,
            policy.estimator,
        )
        hierarchy[old_track.track_id] = item

    for previous_id in best.deaths:
        old_track = latest_to_track[previous_id]
        ledger = record_cell_track_gap(
            ledger,
            event_id=_event_id(
                "gap",
                state.revision,
                detected.frame_id,
                old_track.track_id,
            ),
            frame_valid_at=detected.valid_at,
            track_id=old_track.track_id,
            policy=policy.lifecycle,
            reason="unmatched_by_exact_global_assignment",
        )

    for current_id in best.births:
        item = current[current_id]
        track_id = _birth_track_id(state.engine_id, current_id)
        ledger = record_cell_track_birth(
            ledger,
            event_id=_event_id(
                "birth",
                state.revision,
                detected.frame_id,
                track_id,
            ),
            frame_valid_at=detected.valid_at,
            track_id=track_id,
            detection=item.detection,
            object_kind=item.object_kind,
            policy=policy.lifecycle,
            reason="detected_without_selected_predecessor",
        )
        estimates[track_id] = _initialize_estimate(track_id, item, policy)
        hierarchy[track_id] = item

    for hypothesis in selected_lineage:
        children = tuple(
            (child.track_id, current[child.detection_id])
            for child in hypothesis.children
        )
        event_id = _event_id(
            hypothesis.event_type,
            state.revision,
            detected.frame_id,
            hypothesis.hypothesis_id,
            _hypothesis_digest(hypothesis),
        )
        if hypothesis.event_type == "split":
            ledger = record_cell_track_split(
                ledger,
                event_id=event_id,
                parent_track_id=hypothesis.parent_track_ids[0],
                children=children,
                policy=policy.lifecycle,
            )
        else:
            child_track_id, child_detection = children[0]
            ledger = record_cell_track_merge(
                ledger,
                event_id=event_id,
                parent_track_ids=hypothesis.parent_track_ids,
                child_track_id=child_track_id,
                child=child_detection,
                policy=policy.lifecycle,
            )
        for child_track_id, item in children:
            estimates[child_track_id] = _initialize_estimate(
                child_track_id,
                item,
                policy,
            )
            hierarchy[child_track_id] = item
    return ledger, estimates, hierarchy


def _replace_track_revision(
    ledger: CellTrackLedger,
    updated: ImmutableCellTrack,
) -> CellTrackLedger:
    if all(item.track_id != updated.track_id for item in ledger.tracks):
        raise ValueError("trackrevision verwijst naar onbekende track")
    return CellTrackLedger(
        tracks=tuple(
            sorted(
                (
                    updated if item.track_id == updated.track_id else item
                    for item in ledger.tracks
                ),
                key=lambda item: item.track_id,
            )
        ),
        lineage_events=ledger.lineage_events,
        revision=ledger.revision + 1,
    )


def _initialize_estimate(
    track_id: str,
    detected: HierarchicalCellDetection,
    policy: CellTrackEnginePolicy,
) -> RecursiveTrackEstimate:
    return initialize_recursive_track_estimate(
        _digest_id("recursive-estimate", track_id),
        detected.detection,
        initial_vx_m_s=0.0,
        initial_vy_m_s=0.0,
        initial_state_covariance=policy.initial_state_covariance,
        policy=policy.estimator,
    )


def _identity_alternatives(
    state: CellTrackEngineState,
    assignment: GlobalAssignmentResult,
    hypotheses: tuple[CellTrackLineageHypothesis, ...],
    ledger: CellTrackLedger,
) -> dict[str, tuple[str, ...]]:
    previous_track = {
        track.latest_detection.detection_id: track.track_id
        for track in state.ledger.tracks
        if track.lifecycle_state != "ended"
    }
    current_track: dict[str, str] = {}
    current_ids = set(assignment.problem.current_detection_ids)
    current_ids.update(
        child.detection_id for item in hypotheses if item.selected for child in item.children
    )
    for track in ledger.tracks:
        detection_id = track.latest_detection.detection_id
        if detection_id in current_ids:
            current_track[detection_id] = track.track_id

    alternatives: dict[str, set[str]] = {
        track_id: set() for track_id in current_track.values()
    }
    for solution in assignment.solutions:
        for previous_id, current_id in solution.matches:
            track_id = current_track.get(current_id)
            candidate_track = previous_track[previous_id]
            if track_id is not None and candidate_track != track_id:
                alternatives[track_id].add(candidate_track)
    for hypothesis in hypotheses:
        if hypothesis.selected:
            continue
        for child in hypothesis.children:
            track_id = current_track.get(child.detection_id)
            if track_id is None:
                continue
            alternatives[track_id].update(
                parent
                for parent in hypothesis.parent_track_ids
                if parent != track_id
            )
    return {
        track_id: tuple(sorted(values))
        for track_id, values in alternatives.items()
    }


def _group_systems(
    state: CellTrackEngineState,
    frame: NormalizedRasterFrame,
    ledger: CellTrackLedger,
    estimates: dict[str, RecursiveTrackEstimate],
    hierarchy: dict[str, HierarchicalCellDetection],
    alternatives: dict[str, tuple[str, ...]],
    policy: CellTrackEnginePolicy,
) -> WeatherSystemGroupingSnapshot:
    inputs: list[WeatherSystemTrackInput] = []
    for track in ledger.tracks:
        if (
            track.lifecycle_state == "ended"
            or track.latest_detection.valid_at != frame.valid_at
        ):
            continue
        detected = hierarchy[track.track_id]
        estimate = estimates[track.track_id]
        shield_ids = (
            (detected.detection.detection_id,)
            if detected.object_kind == "rain_shield"
            else (str(detected.parent_detection_id),)
        )
        inputs.append(
            WeatherSystemTrackInput(
                track_snapshot_id=_digest_id(
                    "cell-track-snapshot",
                    track.track_id,
                    str(track.revision),
                    detected.detection.detection_id,
                    estimate.estimate_id,
                ),
                track_id=track.track_id,
                analysis_grid_id=track.analysis_grid_id,
                valid_at=frame.valid_at,
                current_detection_id=detected.detection.detection_id,
                object_kind=detected.object_kind,
                current_footprint_pixels=detected.footprint_pixels,
                rain_shield_detection_ids=shield_ids,
                x_m=detected.detection.x_m,
                y_m=detected.detection.y_m,
                vx_m_s=estimate.vx_m_s,
                vy_m_s=estimate.vy_m_s,
                motion_reference_valid_at=estimate.valid_at,
                motion_method=estimate.method,
                lifecycle_state=track.lifecycle_state,
                parent_track_ids=track.parent_track_ids,
                child_track_ids=track.child_track_ids,
                lineage_event_ids=track.lineage_event_ids,
                identity_alternative_track_ids=alternatives.get(track.track_id, ()),
            )
        )
    return group_weather_systems(
        valid_at=frame.valid_at,
        grid=frame.grid,
        tracks=inputs,
        policy=policy.weather_system,
        previous=state.weather_systems,
    )


def _transition_capacity_reasons(
    state: CellTrackEngineState,
    assignment: GlobalAssignmentResult,
    selected: tuple[CellTrackLineageHypothesis, ...],
    track_by_latest: dict[str, ImmutableCellTrack],
    policy: CellTrackEnginePolicy,
) -> tuple[str, ...]:
    best = assignment.best
    child_count = sum(len(item.children) for item in selected)
    total_after = len(state.ledger.tracks) + len(best.births) + child_count
    reasons: set[str] = set()
    if total_after > policy.maximum_total_tracks:
        reasons.add("total_track_capacity_exceeded")
    lineage_parent_count = len({
        track_id for item in selected for track_id in item.parent_track_ids
    })
    active_before = sum(
        item.lifecycle_state != "ended" for item in state.ledger.tracks
    )
    ending_unmatched = sum(
        track_by_latest[previous_id].missed_frames + 1
        > policy.lifecycle.maximum_gap_frames
        for previous_id in best.deaths
    )
    active_after = (
        active_before
        - lineage_parent_count
        - ending_unmatched
        + len(best.births)
        + child_count
    )
    if active_after > policy.maximum_active_tracks:
        reasons.add("active_track_capacity_exceeded")
    reacquired_count = sum(
        track_by_latest[previous_id].lifecycle_state == "coasting"
        for previous_id, _current_id in best.matches
    )
    event_additions = (
        len(best.births)
        + len(best.deaths)
        + reacquired_count
        + len(selected)
    )
    if (
        len(state.ledger.lineage_events) + event_additions
        > policy.maximum_lifecycle_events
    ):
        reasons.add("lifecycle_event_capacity_exceeded")
    if any(
        track_by_latest[previous_id].sample_count + 1
        > policy.maximum_track_history_detections
        for previous_id, _current_id in best.matches
    ):
        reasons.add("track_history_capacity_exceeded")
    return tuple(sorted(reasons))


def _build_advected_association_evidence(
    *,
    state: CellTrackEngineState,
    frame: NormalizedRasterFrame,
    detection_frame: HierarchicalDetectionFrame,
    previous: tuple[HierarchicalCellDetection, ...],
    policy: MultiHypothesisIdentityPolicy,
) -> tuple[
    SystemTranslationEvidence,
    tuple[SystemTranslationEvidence, ...],
    dict[str, SystemTranslationEvidence],
    dict[tuple[str, str], AdvectedFootprintEvidence],
    tuple[str, ...],
]:
    """Build every association layer from one immutable per-pair receipt."""

    previous_frame = state.last_frame
    previous_detection_frame = state.last_detection_frame
    grouping = state.weather_systems
    if previous_frame is None or previous_detection_frame is None or grouping is None:
        raise AssertionError("advected association vereist niet-lege state")
    previous_shield_by_id = {
        item.detection.detection_id: item
        for item in previous_detection_frame.rain_shields
    }
    current_shields = tuple(sorted(
        detection_frame.rain_shields,
        key=lambda item: item.detection.detection_id,
    ))
    current_binary = BinaryRainShieldFootprintFrame(
        frame_id=frame.frame_id,
        valid_at=frame.valid_at,
        grid=frame.grid,
        rain_shield_footprints=tuple(
            item.footprint_pixels for item in current_shields
        ),
    )
    global_previous_binary = BinaryRainShieldFootprintFrame(
        frame_id=previous_frame.frame_id,
        valid_at=previous_frame.valid_at,
        grid=frame.grid,
        rain_shield_footprints=tuple(
            item.footprint_pixels
            for item in sorted(
                previous_detection_frame.rain_shields,
                key=lambda item: item.detection.detection_id,
            )
        ),
    )
    context_specs: list[
        tuple[str, BinaryRainShieldFootprintFrame, tuple[str, ...]]
    ] = [("global", global_previous_binary, ())]
    assigned_previous: set[str] = set()
    for system in sorted(grouping.systems, key=lambda item: item.system_snapshot_id):
        shield_ids = tuple(sorted(system.rain_shield_detection_ids))
        missing = tuple(item for item in shield_ids if item not in previous_shield_by_id)
        if missing:
            return (
                _unavailable_translation_for_missing_context(
                    previous_frame=previous_frame,
                    frame=frame,
                    policy=policy,
                    reason="weather_system_shield_support_missing",
                ),
                (),
                {},
                {},
                ("weather_system_shield_support_missing",),
            )
        system_previous = BinaryRainShieldFootprintFrame(
            frame_id="shared-system-previous:" + hashlib.sha256(
                (system.system_snapshot_id + "\0" + previous_frame.frame_id).encode(
                    "utf-8"
                )
            ).hexdigest(),
            valid_at=previous_frame.valid_at,
            grid=frame.grid,
            rain_shield_footprints=tuple(
                previous_shield_by_id[item].footprint_pixels for item in shield_ids
            ),
        )
        member_detection_ids = tuple(sorted(
            state.track(item.track_id).latest_detection.detection_id
            for item in system.member_tracks
        ))
        collision = assigned_previous & set(member_detection_ids)
        if collision:
            raise AssertionError("detectie behoort tot meerdere WeatherSystems")
        assigned_previous.update(member_detection_ids)
        context_specs.append((system.system_snapshot_id, system_previous, member_detection_ids))

    full_shift_candidates = (2 * frame.grid.height - 1) * (
        2 * frame.grid.width - 1
    )
    total_shift_candidates = full_shift_candidates * len(context_specs)
    total_pair_contributions = sum(
        len(source.support_pixels) * len(current_binary.support_pixels)
        for _name, source, _members in context_specs
    )
    reasons: set[str] = set()
    if len(context_specs) > policy.maximum_shared_translation_contexts:
        reasons.add("shared_translation_context_capacity_exceeded")
    if total_shift_candidates > policy.maximum_shared_translation_shift_candidates_total:
        reasons.add("shared_translation_shift_candidate_capacity_exceeded")
    if total_pair_contributions > policy.maximum_shared_translation_pair_contributions_total:
        reasons.add("shared_translation_pair_contribution_capacity_exceeded")
    pair_count = len(previous) * len(detection_frame.detections)
    if pair_count > policy.maximum_shared_translation_pair_contributions_total:
        reasons.add("advected_footprint_pair_capacity_exceeded")
    if reasons:
        fallback = _unavailable_translation_for_missing_context(
            previous_frame=previous_frame,
            frame=frame,
            policy=policy,
            reason=min(reasons),
        )
        return fallback, (), {}, {}, tuple(sorted(reasons))

    config = SharedSystemTranslationConfig(
        max_grid_pixels=min(1_000_000, max(1, frame.grid.width * frame.grid.height)),
        max_support_pixels_per_frame=1_000_000,
        max_pair_contributions=min(
            4_000_000,
            policy.maximum_shared_translation_pair_contributions_total,
        ),
        max_shift_candidates=min(
            4_000_000,
            policy.maximum_shared_translation_shift_candidates_total,
        ),
        max_nonzero_shift_candidates=250_000,
    )
    contexts: dict[str, _SharedTranslationContext] = {}
    context_by_previous: dict[str, _SharedTranslationContext] = {}
    for context_name, source, member_ids in context_specs:
        result = estimate_shared_system_translation(source, current_binary, config)
        capacity_reasons = tuple(
            item
            for item in result.indeterminate_reason_codes
            if "capacity" in item
        )
        if capacity_reasons:
            fallback = _translation_uncertainty_from_shared(
                result=result,
                previous_frame=source,
                current_frame=current_binary,
                policy=policy,
            )
            return fallback, (), {}, {}, tuple(sorted(set(capacity_reasons)))
        uncertainty = _translation_uncertainty_from_shared(
            result=result,
            previous_frame=source,
            current_frame=current_binary,
            policy=policy,
        )
        context = _SharedTranslationContext(
            previous_frame=source,
            current_frame=current_binary,
            result=result,
            uncertainty=uncertainty,
        )
        contexts[context_name] = context
        for detection_id in member_ids:
            context_by_previous[detection_id] = context
    global_context = contexts["global"]
    uncertainties = {
        item.uncertainty.evidence_id: item.uncertainty
        for item in contexts.values()
    }
    previous_values = tuple(sorted(previous, key=lambda item: item.detection.detection_id))
    current_values = tuple(sorted(
        detection_frame.detections,
        key=lambda item: item.detection.detection_id,
    ))
    evidence: dict[tuple[str, str], AdvectedFootprintEvidence] = {}
    try:
        for old in previous_values:
            old_id = old.detection.detection_id
            immediate = math.isclose(
                old.detection.valid_at,
                previous_frame.valid_at,
                rel_tol=0.0,
                abs_tol=1e-6,
            )
            context = context_by_previous.get(old_id, global_context)
            forced = () if immediate else (
                "previous_detection_outside_translation_time_window",
            )
            for new in current_values:
                pair = (old_id, new.detection.detection_id)
                evidence[pair] = build_advected_footprint_evidence(
                    grid=frame.grid,
                    previous_frame=context.previous_frame,
                    current_frame=context.current_frame,
                    translation=context.result,
                    previous=FootprintEvidenceObject(
                        detection_id=old_id,
                        footprint_pixels=old.footprint_pixels,
                    ),
                    current=FootprintEvidenceObject(
                        detection_id=new.detection.detection_id,
                        footprint_pixels=new.footprint_pixels,
                    ),
                    forced_fallback_reason_codes=forced,
                )
    except AdvectedFootprintCapacityError:
        return global_context.uncertainty, (), {}, {}, (
            "advected_footprint_pair_capacity_exceeded",
        )
    return (
        global_context.uncertainty,
        tuple(sorted(uncertainties.values(), key=lambda item: item.evidence_id)),
        uncertainties,
        evidence,
        (),
    )


def _translation_uncertainty_from_shared(
    *,
    result: SharedSystemTranslationResult,
    previous_frame: BinaryRainShieldFootprintFrame,
    current_frame: BinaryRainShieldFootprintFrame,
    policy: MultiHypothesisIdentityPolicy,
) -> SystemTranslationEvidence:
    selected = result.selected_candidate
    usable = bool(
        selected is not None
        and result.unique_peak is True
        and result.peak_tie_count == 1
        and result.accounting.search_complete
        and result.status != "indeterminate"
    )
    sigma = (
        policy.system_translation_sigma_floor_m
        if result.status == "resolved_unique_peak" and usable
        else policy.system_translation_sigma_ceiling_m
    )
    reasons = () if usable else tuple(sorted(set((
        *result.uncertainty_reason_codes,
        *result.indeterminate_reason_codes,
        "shared_translation_not_uniquely_usable",
    ))))
    pixel_area = previous_frame.grid.pixel_area_m2
    return SystemTranslationEvidence(
        evidence_id=result.result_sha256,
        previous_frame_id=result.previous_frame_id,
        current_frame_id=result.current_frame_id,
        previous_valid_at=previous_frame.valid_at,
        current_valid_at=current_frame.valid_at,
        available=usable,
        displacement_x_m=(None if not usable else selected.displacement_x_m),
        displacement_y_m=(None if not usable else selected.displacement_y_m),
        interval_speed_kmh=(None if not usable else selected.interval_speed_kmh),
        covariance_m2=((sigma**2, 0.0), (0.0, sigma**2)),
        previous_support_objects=len(previous_frame.rain_shield_footprints),
        current_support_objects=len(current_frame.rain_shield_footprints),
        previous_support_area_m2=len(previous_frame.support_pixels) * pixel_area,
        current_support_area_m2=len(current_frame.support_pixels) * pixel_area,
        method="binary_rain_shield_shared_translation:" + result.status,
        uncertainty_reason_codes=reasons,
    )


def _unavailable_translation_for_missing_context(
    *,
    previous_frame: NormalizedRasterFrame,
    frame: NormalizedRasterFrame,
    policy: MultiHypothesisIdentityPolicy,
    reason: str,
) -> SystemTranslationEvidence:
    variance = policy.system_translation_sigma_ceiling_m**2
    return SystemTranslationEvidence(
        evidence_id=hashlib.sha256(
            (previous_frame.frame_id + "\0" + frame.frame_id + "\0" + reason).encode(
                "utf-8"
            )
        ).hexdigest(),
        previous_frame_id=previous_frame.frame_id,
        current_frame_id=frame.frame_id,
        previous_valid_at=previous_frame.valid_at,
        current_valid_at=frame.valid_at,
        available=False,
        displacement_x_m=None,
        displacement_y_m=None,
        interval_speed_kmh=None,
        covariance_m2=((variance, 0.0), (0.0, variance)),
        previous_support_objects=0,
        current_support_objects=0,
        previous_support_area_m2=0.0,
        current_support_area_m2=0.0,
        method="binary_rain_shield_shared_translation_unavailable",
        uncertainty_reason_codes=(reason,),
    )


def _derive_lineage_hypotheses(
    *,
    state: CellTrackEngineState,
    frame: NormalizedRasterFrame,
    previous: tuple[HierarchicalCellDetection, ...],
    current: tuple[HierarchicalCellDetection, ...],
    advected_evidence_by_pair: dict[
        tuple[str, str], AdvectedFootprintEvidence
    ],
    policy: MultiHypothesisIdentityPolicy,
) -> tuple[tuple[CellTrackLineageHypothesis, ...], tuple[str, ...]]:
    """Retain every maximal translated-overlap split/merge alternative.

    These morphology-derived alternatives are never auto-selected: normalized
    radar has no independent identity truth.  The model emits one maximal
    same-kind overlap group per previous object (split) and per current object
    (merge), so its complete candidate count is linear and auditable rather
    than an unbounded powerset.  No speed or displacement threshold is used.
    """

    if not previous or not current:
        return (), ()
    latest_track = {
        track.latest_detection.detection_id: track.track_id
        for track in state.ledger.tracks
        if track.lifecycle_state != "ended"
    }
    current_by_id = {
        item.detection.detection_id: item for item in current
    }
    overlap: dict[tuple[str, str], float] = {}
    nonoverlap: dict[tuple[str, str], float] = {}
    for old in previous:
        old_id = old.detection.detection_id
        for new in current:
            if old.object_kind != new.object_kind:
                continue
            new_id = new.detection.detection_id
            pair = (old_id, new_id)
            evidence = advected_evidence_by_pair.get(pair)
            if evidence is None:
                raise AssertionError("derived lineage mist advected pair evidence")
            if evidence.iou <= 0.0:
                continue
            overlap[pair] = evidence.iou
            nonoverlap[pair] = 1.0 - evidence.iou

    split_groups = tuple(
        (old.detection.detection_id, tuple(sorted(
            new.detection.detection_id
            for new in current
            if (old.detection.detection_id, new.detection.detection_id) in overlap
        )))
        for old in previous
    )
    split_groups = tuple(item for item in split_groups if len(item[1]) >= 2)
    merge_groups = tuple(
        (tuple(sorted(
            old.detection.detection_id
            for old in previous
            if (old.detection.detection_id, new.detection.detection_id) in overlap
        )), new.detection.detection_id)
        for new in current
    )
    merge_groups = tuple(item for item in merge_groups if len(item[0]) >= 2)
    reasons: set[str] = set()
    if any(
        1 + len(children) > policy.maximum_lineage_members
        for _parent, children in split_groups
    ) or any(
        len(parents) + 1 > policy.maximum_lineage_members
        for parents, _child in merge_groups
    ):
        reasons.add("derived_lineage_member_capacity_exceeded")
    if len(split_groups) + len(merge_groups) > policy.maximum_derived_lineage_hypotheses:
        reasons.add("derived_lineage_hypothesis_capacity_exceeded")
    if reasons:
        return (), tuple(sorted(reasons))

    hypotheses: list[CellTrackLineageHypothesis] = []
    for previous_id, child_ids in split_groups:
        parent_track_id = latest_track[previous_id]
        pair_evidence_ids = tuple(sorted(
            advected_evidence_by_pair[(previous_id, child_id)].evidence_sha256
            for child_id in child_ids
        ))
        cost = math.fsum(nonoverlap[(previous_id, child_id)] for child_id in child_ids)
        hypothesis_id = _digest_id(
            "derived-lineage-split-v2",
            parent_track_id,
            *child_ids,
            *pair_evidence_ids,
            cost.hex(),
        )
        hypotheses.append(CellTrackLineageHypothesis(
            hypothesis_id=hypothesis_id,
            event_type="split",
            parent_track_ids=(parent_track_id,),
            children=tuple(sorted(
                (
                    LineageChildIdentity(
                        detection_id=child_id,
                        track_id=_digest_id(
                            "derived-lineage-child",
                            hypothesis_id,
                            child_id,
                        ),
                    )
                    for child_id in child_ids
                ),
                key=lambda item: (item.detection_id, item.track_id),
            )),
            cost=cost,
            cost_model_id="advected_maximal_iou_lineage_v2",
            cost_components=(("advected_iou_nonoverlap_sum", cost),),
            provenance_ids=tuple(sorted((
                state.last_frame.frame_id,
                frame.frame_id,
                *pair_evidence_ids,
            ))),
            selected=False,
        ))
    for previous_ids, child_id in merge_groups:
        parent_track_ids = tuple(sorted(latest_track[item] for item in previous_ids))
        pair_evidence_ids = tuple(sorted(
            advected_evidence_by_pair[(previous_id, child_id)].evidence_sha256
            for previous_id in previous_ids
        ))
        cost = math.fsum(nonoverlap[(previous_id, child_id)] for previous_id in previous_ids)
        hypothesis_id = _digest_id(
            "derived-lineage-merge-v2",
            *parent_track_ids,
            child_id,
            *pair_evidence_ids,
            cost.hex(),
        )
        hypotheses.append(CellTrackLineageHypothesis(
            hypothesis_id=hypothesis_id,
            event_type="merge",
            parent_track_ids=parent_track_ids,
            children=(LineageChildIdentity(
                detection_id=child_id,
                track_id=_digest_id(
                    "derived-lineage-child",
                    hypothesis_id,
                    child_id,
                ),
            ),),
            cost=cost,
            cost_model_id="advected_maximal_iou_lineage_v2",
            cost_components=(("advected_iou_nonoverlap_sum", cost),),
            provenance_ids=tuple(sorted((
                state.last_frame.frame_id,
                frame.frame_id,
                *pair_evidence_ids,
            ))),
            selected=False,
        ))
    return tuple(sorted(hypotheses, key=lambda item: item.hypothesis_id)), ()


def _validate_lineage_hypotheses(
    hypotheses: tuple[CellTrackLineageHypothesis, ...],
    *,
    active_tracks: tuple[ImmutableCellTrack, ...],
    current_by_id: dict[str, HierarchicalCellDetection],
    existing_track_ids: set[str],
) -> tuple[CellTrackLineageHypothesis, ...]:
    active_by_id = {item.track_id: item for item in active_tracks}
    for hypothesis in hypotheses:
        if not set(hypothesis.parent_track_ids) <= set(active_by_id):
            raise ValueError("lineagehypothese verwijst naar niet-actieve parent")
        child_ids = {item.detection_id for item in hypothesis.children}
        if not child_ids <= set(current_by_id):
            raise ValueError("lineagehypothese verwijst naar onbekende detectie")
        if {
            item.track_id for item in hypothesis.children
        } & existing_track_ids:
            raise ValueError("lineagehypothese child track-ID moet nieuw zijn")
        parent_kinds = {
            active_by_id[item].observation_kinds[-1]
            for item in hypothesis.parent_track_ids
        }
        child_kinds = {current_by_id[item].object_kind for item in child_ids}
        if len(parent_kinds | child_kinds) != 1:
            raise ValueError("split/merge mag hiërarchische objectsoorten niet mengen")
    selected = tuple(item for item in hypotheses if item.selected)
    selected_parents = tuple(
        track_id for item in selected for track_id in item.parent_track_ids
    )
    selected_detections = tuple(
        child.detection_id for item in selected for child in item.children
    )
    selected_tracks = tuple(
        child.track_id for item in selected for child in item.children
    )
    if len(selected_parents) != len(set(selected_parents)):
        raise ValueError("geselecteerde lineagehypotheses delen een parent")
    if len(selected_detections) != len(set(selected_detections)):
        raise ValueError("geselecteerde lineagehypotheses delen een detectie")
    if (
        len(selected_tracks) != len(set(selected_tracks))
        or set(selected_tracks) & existing_track_ids
    ):
        raise ValueError("geselecteerde child track-ID's moeten nieuw en uniek zijn")
    return selected


def _canonical_hypotheses(
    values: Iterable[CellTrackLineageHypothesis],
) -> tuple[CellTrackLineageHypothesis, ...]:
    hypotheses = tuple(values)
    if any(not isinstance(item, CellTrackLineageHypothesis) for item in hypotheses):
        raise TypeError("lineage_hypotheses bevat verkeerd recordtype")
    identifiers = tuple(item.hypothesis_id for item in hypotheses)
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("lineage_hypotheses bevat dubbele hypothesis_id")
    return tuple(sorted(hypotheses, key=lambda item: item.hypothesis_id))


def _handoff_reasons(
    previous: NormalizedRasterFrame,
    current: NormalizedRasterFrame,
    event: ProviderHandoffEvent | None,
) -> tuple[str, ...]:
    changes: set[str] = set()
    if previous.source != current.source:
        changes.add("provider_or_source_changed")
    if not previous.grid.same_geometry(current.grid):
        changes.add("analysis_grid_changed")
    if previous.quantity != current.quantity:
        changes.add("quantity_changed")
    if previous.unit != current.unit:
        changes.add("unit_changed")
    if not changes:
        return ()
    if event is None:
        changes.add("provider_handoff_evidence_missing")
    else:
        if not isinstance(event, ProviderHandoffEvent):
            raise TypeError("provider_handoff_event heeft verkeerd type")
        if (
            event.source_frame.frame_id != previous.frame_id
            or event.target_frame.frame_id != current.frame_id
            or event.source_frame.analysis_grid_id != previous.grid.grid_id
            or event.target_frame.analysis_grid_id != current.grid.grid_id
            or event.valid_at != current.valid_at
        ):
            raise ValueError("provider_handoff_event hoort niet bij deze frameovergang")
        changes.add("provider_handoff_evidence_retained")
        changes.add(f"provider_handoff_{event.continuity_state.value}")
    # The existing lifecycle and recursive estimator require one fixed metric
    # grid and forbid cross-handoff velocity updates.  Until a registered
    # position-only state-transform primitive exists, advancing identity here
    # would overstate the evidence.  Preserve both state and detections.
    changes.add("provider_grid_handoff_state_transform_unimplemented")
    return tuple(sorted(changes))


def _indeterminate(
    state: CellTrackEngineState,
    detected: HierarchicalDetectionFrame,
    *,
    hypotheses: tuple[CellTrackLineageHypothesis, ...],
    reasons: tuple[str, ...],
    transition_seed: tuple[str, ...],
    motion_priors: tuple[MotionPrior, ...] = (),
    flow_field: LucasKanadeFlowField | None = None,
    flow_aggregations: tuple[DetectionFlowAggregation, ...] = (),
    graphs: tuple[DetectionAssociationGraph, ...] = (),
    feature_tables: tuple[AssociationFeatureTable, ...] = (),
    assignment_problem: GlobalAssignmentProblem | None = None,
    assignment_result: GlobalAssignmentResult | None = None,
    provider_handoff_event: ProviderHandoffEvent | None = None,
    candidate_edge_count: int = 0,
    identity_snapshot: MultiHypothesisIdentitySnapshot | None = None,
) -> CellTrackTransitionResult:
    canonical_reasons = tuple(sorted(set(reasons)))
    if not canonical_reasons:
        raise ValueError("indeterminate transition vereist een reden")
    return CellTrackTransitionResult(
        transition_id=_digest_id(
            "cell-track-transition-indeterminate",
            *transition_seed,
            *canonical_reasons,
        ),
        status="indeterminate",
        reason_codes=canonical_reasons,
        input_state_revision=state.revision,
        detection_frame=detected,
        next_state=state,
        motion_priors=motion_priors,
        flow_field=flow_field,
        flow_aggregations=flow_aggregations,
        association_graphs=graphs,
        association_feature_tables=feature_tables,
        assignment_problem=assignment_problem,
        assignment_result=assignment_result,
        lineage_hypotheses=hypotheses,
        applied_lineage_hypothesis_ids=(),
        provider_handoff_event=provider_handoff_event,
        candidate_edge_count=candidate_edge_count,
        identity_snapshot=identity_snapshot,
    )


def _birth_track_id(engine_id: str, detection_id: str) -> str:
    return _digest_id("cell-track", engine_id, "birth", detection_id)


def _event_id(event_type: str, revision: int, *parts: str) -> str:
    return _digest_id(
        f"cell-track-event-{event_type}",
        str(revision),
        *parts,
    )


def _digest_id(prefix: str, *parts: str) -> str:
    _identifier(prefix, "digest prefix")
    digest = hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


def _frame_digest(frame: NormalizedRasterFrame) -> str:
    digest = hashlib.sha256()
    grid = frame.grid
    tokens = (
        frame.frame_id,
        format(frame.valid_at, ".17g"),
        frame.source,
        frame.quantity,
        frame.unit,
        grid.grid_id,
        grid.crs,
        format(grid.origin_x_m, ".17g"),
        format(grid.origin_y_m, ".17g"),
        format(grid.x_resolution_m, ".17g"),
        format(grid.y_resolution_m, ".17g"),
        str(grid.width),
        str(grid.height),
        grid.coordinate_unit,
        grid.axis_order,
        grid.row_order,
    )
    for token in tokens:
        digest.update(token.encode("utf-8"))
        digest.update(b"\0")
    for row in frame.values:
        for value in row:
            token = "none" if value is None else format(value, ".17g")
            digest.update(token.encode("ascii"))
            digest.update(b"\0")
    return digest.hexdigest()


def _hypothesis_digest(hypothesis: CellTrackLineageHypothesis) -> str:
    return hashlib.sha256("\0".join((
        hypothesis.hypothesis_id,
        hypothesis.event_type,
        *hypothesis.parent_track_ids,
        *(f"{item.detection_id}:{item.track_id}" for item in hypothesis.children),
        format(hypothesis.cost, ".17g"),
        hypothesis.cost_model_id,
        *(f"{name}:{format(value, '.17g')}" for name, value in hypothesis.cost_components),
        *hypothesis.provenance_ids,
        "selected" if hypothesis.selected else "alternative",
    )).encode("utf-8")).hexdigest()


def _state_digest(state: CellTrackEngineState) -> str:
    digest = hashlib.sha256()
    tokens: list[str] = [
        state.engine_id,
        state.policy_fingerprint,
        str(state.revision),
        str(state.ledger.revision),
        _frame_digest(state.last_frame) if state.last_frame is not None else "empty",
        (
            state.weather_systems.grouping_snapshot_id
            if state.weather_systems is not None
            else "none"
        ),
        (
            state.identity_snapshot.snapshot_id
            if state.identity_snapshot is not None
            else "none"
        ),
        (
            state.multi_scan_posterior.posterior_id
            if state.multi_scan_posterior is not None
            else "none"
        ),
    ]
    for track in state.ledger.tracks:
        tokens.extend((
            track.track_id,
            track.lifecycle_state,
            str(track.revision),
            str(track.missed_frames),
            track.termination_reason or "none",
            *track.parent_track_ids,
            *track.child_track_ids,
            *track.lineage_event_ids,
            *(item.detection_id for item in track.detections),
        ))
    for track_id, estimate in state.estimates:
        tokens.extend((
            track_id,
            estimate.estimate_id,
            str(estimate.revision),
            format(estimate.valid_at, ".17g"),
            *(format(value, ".17g") for value in estimate.state),
            *(format(value, ".17g") for row in estimate.covariance for value in row),
        ))
    for event in state.ledger.lineage_events:
        tokens.extend((event.event_id, event.event_type, format(event.valid_at, ".17g")))
    for token in tokens:
        digest.update(token.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or "\0" in value:
        raise ValueError(f"{label} moet een veilige niet-lege string zijn")
    return value


def _sha256(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(item not in "0123456789abcdef" for item in value)
    ):
        raise ValueError(f"{label} moet lowercase SHA-256 zijn")
    return value


def _bounded_integer(
    value: object,
    label: str,
    *,
    allow_zero: bool,
) -> int:
    minimum = 0 if allow_zero else 1
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= 2_000_000
    ):
        raise ValueError(f"{label} moet een begrensd geheel getal zijn")
    return value


def _nonnegative_finite(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0.0
    ):
        raise ValueError(f"{label} moet eindig en niet-negatief zijn")
    return float(value)


def _nonnegative_count(value: object, label: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= 2**63 - 1
    ):
        raise ValueError(f"{label} moet een niet-negatieve bounded count zijn")
    return value


def _covariance4(value: Matrix4, label: str) -> None:
    if (
        not isinstance(value, tuple)
        or len(value) != 4
        or any(not isinstance(row, tuple) or len(row) != 4 for row in value)
        or any(not math.isfinite(item) for row in value for item in row)
    ):
        raise ValueError(f"{label} moet een eindige immutable 4x4 tuple zijn")
    for row in range(4):
        for column in range(4):
            if not math.isclose(
                value[row][column],
                value[column][row],
                rel_tol=1e-12,
                abs_tol=1e-12,
            ):
                raise ValueError(f"{label} moet symmetrisch zijn")
    scale = max(1.0, *(abs(item) for row in value for item in row))
    for size in range(1, 5):
        for indices in itertools.combinations(range(4), size):
            minor = tuple(
                tuple(value[row][column] for column in indices)
                for row in indices
            )
            if _determinant(minor) < -1e-10 * scale**size:
                raise ValueError(f"{label} moet positief semidefiniet zijn")


def _determinant(value: tuple[tuple[float, ...], ...]) -> float:
    if len(value) == 1:
        return value[0][0]
    return math.fsum(
        (-1.0 if column % 2 else 1.0)
        * value[0][column]
        * _determinant(tuple(
            tuple(row[index] for index in range(len(value)) if index != column)
            for row in value[1:]
        ))
        for column in range(len(value))
    )


def _policy(value: CellTrackEnginePolicy) -> None:
    if not isinstance(value, CellTrackEnginePolicy):
        raise TypeError("CellTrackEngine vereist CellTrackEnginePolicy")


__all__ = [
    "CELL_TRACK_ENGINE_STATUS",
    "CELL_TRACK_ENGINE_VERSION",
    "CellTrackEnginePolicy",
    "CellTrackEngineState",
    "CellTrackLineageHypothesis",
    "CellTrackTransitionResult",
    "LineageChildIdentity",
    "advance_cell_track_engine",
    "create_cell_track_engine_state",
]
