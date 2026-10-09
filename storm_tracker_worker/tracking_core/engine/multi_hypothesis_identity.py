"""Bounded multi-hypothesis identity evidence for the unwired CellTrack shadow.

The shared translation of the complete detected rain field is estimated before
any cell identity is selected.  Every previous/current association then keeps
its residual local deformation relative to that translation.  Assignment can
rank these complete candidates, but no displacement or speed is a reject.

Probabilities are deliberately conditional on the retained top-k set.  They
are not calibrated posterior identity probabilities and omitted mass remains
explicit whenever the full combinatorial assignment space was not exhausted.
"""
from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass, replace
import hashlib
import json
import math
from typing import Any, Iterable, Literal

from .advected_footprint_evidence import AdvectedFootprintEvidence
from .association_features import AssociationFeatureVector
from .global_assignment import (
    AssignmentEdgeCost,
    GlobalAssignmentProblem,
    GlobalAssignmentResult,
    solve_bounded_k_best_global_assignment,
)
from .hierarchical_detection import (
    HierarchicalCellDetection,
    HierarchicalDetectionFrame,
)


MULTI_HYPOTHESIS_IDENTITY_VERSION = "stv3-shadow-multi-hypothesis-identity-v3"
MULTI_SCAN_IDENTITY_VERSION = "stv3-shadow-multi-scan-identity-v2"
EMPTY_MULTI_SCAN_COMMIT_SHA256 = hashlib.sha256(
    b"stv3-shadow-multi-scan-empty-commit-chain-v2"
).hexdigest()


@dataclass(frozen=True, slots=True)
class MultiScanIdentityPolicy:
    """Hard bounds for a branch-consistent N-scan shadow posterior."""

    policy_id: str
    horizon_frames: int
    beam_width: int
    maximum_generated_branches: int
    maximum_assignment_subproblems_total: int
    maximum_candidate_edges_total: int
    maximum_active_paths_per_branch: int
    maximum_buffered_detections: int
    maximum_lineage_alternatives: int
    conditional_cost_temperature: float
    algorithm_version: str = MULTI_SCAN_IDENTITY_VERSION

    def __post_init__(self) -> None:
        _identifier(self.policy_id, "multi-scan policy_id")
        for label, value, maximum in (
            ("horizon_frames", self.horizon_frames, 64),
            ("beam_width", self.beam_width, 128),
            (
                "maximum_generated_branches",
                self.maximum_generated_branches,
                16_384,
            ),
            (
                "maximum_assignment_subproblems_total",
                self.maximum_assignment_subproblems_total,
                2_000_000,
            ),
            (
                "maximum_candidate_edges_total",
                self.maximum_candidate_edges_total,
                4_000_000,
            ),
            (
                "maximum_active_paths_per_branch",
                self.maximum_active_paths_per_branch,
                10_000,
            ),
            (
                "maximum_buffered_detections",
                self.maximum_buffered_detections,
                1_000_000,
            ),
            (
                "maximum_lineage_alternatives",
                self.maximum_lineage_alternatives,
                100_000,
            ),
        ):
            _positive_integer(value, label, maximum)
        if self.horizon_frames < 2:
            raise ValueError("multi-scan horizon vereist minstens twee frames")
        if self.maximum_generated_branches < self.beam_width:
            raise ValueError("multi-scan generated cap moet minstens beam width zijn")
        _positive_finite(
            self.conditional_cost_temperature,
            "multi-scan conditional_cost_temperature",
        )
        if self.algorithm_version != MULTI_SCAN_IDENTITY_VERSION:
            raise ValueError("onbekende multi-scan identityversie")

    @property
    def fingerprint(self) -> str:
        return _digest(
            self.policy_id,
            str(self.horizon_frames),
            str(self.beam_width),
            str(self.maximum_generated_branches),
            str(self.maximum_assignment_subproblems_total),
            str(self.maximum_candidate_edges_total),
            str(self.maximum_active_paths_per_branch),
            str(self.maximum_buffered_detections),
            str(self.maximum_lineage_alternatives),
            format(self.conditional_cost_temperature, ".17g"),
            self.algorithm_version,
        )


@dataclass(frozen=True, slots=True)
class MultiHypothesisIdentityPolicy:
    policy_id: str
    solution_limit: int
    maximum_assignment_subproblems: int
    maximum_retained_local_candidates: int
    maximum_derived_lineage_hypotheses: int
    maximum_lineage_members: int
    minimum_system_translation_support_tracks: int
    conditional_cost_temperature: float
    system_translation_sigma_floor_m: float
    system_translation_sigma_ceiling_m: float
    local_deformation_sigma_floor_m: float
    local_deformation_sigma_ceiling_m: float
    previous_local_deformation_weight: float
    local_deformation_cost_weight: float
    multi_scan: MultiScanIdentityPolicy | None = None
    maximum_shared_translation_contexts: int = 64
    maximum_shared_translation_shift_candidates_total: int = 16_000_000
    maximum_shared_translation_pair_contributions_total: int = 4_000_000
    algorithm_version: str = MULTI_HYPOTHESIS_IDENTITY_VERSION

    def __post_init__(self) -> None:
        _identifier(self.policy_id, "multi-hypothesis policy_id")
        for label, value, maximum in (
            ("solution_limit", self.solution_limit, 64),
            (
                "maximum_assignment_subproblems",
                self.maximum_assignment_subproblems,
                1_000_000,
            ),
            (
                "maximum_retained_local_candidates",
                self.maximum_retained_local_candidates,
                2_000_000,
            ),
            (
                "maximum_derived_lineage_hypotheses",
                self.maximum_derived_lineage_hypotheses,
                100_000,
            ),
            ("maximum_lineage_members", self.maximum_lineage_members, 10_000),
            (
                "minimum_system_translation_support_tracks",
                self.minimum_system_translation_support_tracks,
                10_000,
            ),
            (
                "maximum_shared_translation_contexts",
                self.maximum_shared_translation_contexts,
                4_096,
            ),
            (
                "maximum_shared_translation_shift_candidates_total",
                self.maximum_shared_translation_shift_candidates_total,
                256_000_000,
            ),
            (
                "maximum_shared_translation_pair_contributions_total",
                self.maximum_shared_translation_pair_contributions_total,
                256_000_000,
            ),
        ):
            _positive_integer(value, label, maximum)
        for label, value in (
            ("conditional_cost_temperature", self.conditional_cost_temperature),
            (
                "system_translation_sigma_floor_m",
                self.system_translation_sigma_floor_m,
            ),
            (
                "system_translation_sigma_ceiling_m",
                self.system_translation_sigma_ceiling_m,
            ),
            ("local_deformation_sigma_floor_m", self.local_deformation_sigma_floor_m),
            (
                "local_deformation_sigma_ceiling_m",
                self.local_deformation_sigma_ceiling_m,
            ),
            ("local_deformation_cost_weight", self.local_deformation_cost_weight),
        ):
            _positive_finite(value, label)
        if (
            self.system_translation_sigma_floor_m
            > self.system_translation_sigma_ceiling_m
            or self.local_deformation_sigma_floor_m
            > self.local_deformation_sigma_ceiling_m
        ):
            raise ValueError("multi-hypothesis uncertainty floor exceeds ceiling")
        if (
            not math.isfinite(self.previous_local_deformation_weight)
            or not 0.0 <= self.previous_local_deformation_weight <= 1.0
        ):
            raise ValueError("previous_local_deformation_weight moet in [0,1] liggen")
        if self.multi_scan is not None and not isinstance(
            self.multi_scan,
            MultiScanIdentityPolicy,
        ):
            raise TypeError("multi_scan heeft verkeerd policytype")
        if self.algorithm_version != MULTI_HYPOTHESIS_IDENTITY_VERSION:
            raise ValueError("onbekende multi-hypothesis identityversie")

    @property
    def fingerprint(self) -> str:
        return _digest(
            self.policy_id,
            str(self.solution_limit),
            str(self.maximum_assignment_subproblems),
            str(self.maximum_retained_local_candidates),
            str(self.maximum_derived_lineage_hypotheses),
            str(self.maximum_lineage_members),
            str(self.minimum_system_translation_support_tracks),
            format(self.conditional_cost_temperature, ".17g"),
            format(self.system_translation_sigma_floor_m, ".17g"),
            format(self.system_translation_sigma_ceiling_m, ".17g"),
            format(self.local_deformation_sigma_floor_m, ".17g"),
            format(self.local_deformation_sigma_ceiling_m, ".17g"),
            format(self.previous_local_deformation_weight, ".17g"),
            format(self.local_deformation_cost_weight, ".17g"),
            "none" if self.multi_scan is None else self.multi_scan.fingerprint,
            str(self.maximum_shared_translation_contexts),
            str(self.maximum_shared_translation_shift_candidates_total),
            str(self.maximum_shared_translation_pair_contributions_total),
            self.algorithm_version,
        )


@dataclass(frozen=True, slots=True)
class MultiScanRankingWeights:
    """The existing CellTrack scalar ranking terms, copied without gates."""

    object_kind_change_cost: float
    absolute_log_area_weight: float
    mean_value_difference_weight: float
    maximum_value_difference_weight: float
    footprint_nonoverlap_weight: float
    birth_cost: float
    death_cost: float

    def __post_init__(self) -> None:
        for label, value in (
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
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"{label} moet eindig en niet-negatief zijn")


@dataclass(frozen=True, slots=True)
class MultiScanTrackPath:
    identity_id: str
    latest_detection_id: str
    missed_frames: int
    detection_ids: tuple[str, ...]
    local_deformation_x_m: float
    local_deformation_y_m: float
    parent_identity_ids: tuple[str, ...] = ()
    lineage_event_ids: tuple[str, ...] = ()
    algorithm_version: str = MULTI_SCAN_IDENTITY_VERSION

    def __post_init__(self) -> None:
        _identifier(self.identity_id, "multi-scan identity_id")
        _identifier(self.latest_detection_id, "multi-scan latest_detection_id")
        if (
            isinstance(self.missed_frames, bool)
            or not isinstance(self.missed_frames, int)
            or not 0 <= self.missed_frames <= 10_000
        ):
            raise ValueError("multi-scan missed_frames is ongeldig")
        if (
            not isinstance(self.detection_ids, tuple)
            or not self.detection_ids
            or self.detection_ids[-1] != self.latest_detection_id
            or len(self.detection_ids) != len(set(self.detection_ids))
        ):
            raise ValueError("multi-scan detectionhistorie is ongeldig")
        for item in self.detection_ids:
            _identifier(item, "multi-scan detection_id")
        for value in (
            self.local_deformation_x_m,
            self.local_deformation_y_m,
        ):
            if not math.isfinite(value):
                raise ValueError("multi-scan lokale deformatie is niet eindig")
        for label, values in (
            ("parent_identity_ids", self.parent_identity_ids),
            ("lineage_event_ids", self.lineage_event_ids),
        ):
            if values != tuple(sorted(set(values))):
                raise ValueError(f"multi-scan {label} moet canoniek uniek zijn")
            for item in values:
                _identifier(item, f"multi-scan {label}")
        if self.identity_id in self.parent_identity_ids:
            raise ValueError("multi-scan identity kan niet zijn eigen parent zijn")
        if self.algorithm_version != MULTI_SCAN_IDENTITY_VERSION:
            raise ValueError("onbekende multi-scan pathversie")


@dataclass(frozen=True, slots=True)
class MultiScanLineageAlternative:
    alternative_id: str
    source_branch_id: str
    revision: int
    event_type: Literal["split", "merge"]
    parent_identity_ids: tuple[str, ...]
    child_detection_ids: tuple[str, ...]
    cost: float
    provenance_ids: tuple[str, ...]
    selected: bool = False
    algorithm_version: str = MULTI_SCAN_IDENTITY_VERSION

    def __post_init__(self) -> None:
        _identifier(self.alternative_id, "multi-scan lineage alternative_id")
        _identifier(self.source_branch_id, "multi-scan lineage source_branch_id")
        _positive_integer(self.revision, "multi-scan lineage revision", 2_000_000)
        if self.event_type not in ("split", "merge"):
            raise ValueError("multi-scan lineage event_type is ongeldig")
        for label, values in (
            ("parent_identity_ids", self.parent_identity_ids),
            ("child_detection_ids", self.child_detection_ids),
            ("provenance_ids", self.provenance_ids),
        ):
            if not values or values != tuple(sorted(set(values))):
                raise ValueError(f"multi-scan lineage {label} is niet canoniek")
            for item in values:
                _identifier(item, f"multi-scan lineage {label}")
        if self.event_type == "split" and (
            len(self.parent_identity_ids) != 1
            or len(self.child_detection_ids) < 2
        ):
            raise ValueError("multi-scan splitvorm is ongeldig")
        if self.event_type == "merge" and (
            len(self.parent_identity_ids) < 2
            or len(self.child_detection_ids) != 1
        ):
            raise ValueError("multi-scan mergevorm is ongeldig")
        if not math.isfinite(self.cost) or self.cost < 0.0:
            raise ValueError("multi-scan lineage cost is ongeldig")
        if self.selected is not False:
            raise ValueError("morfologische multi-scan lineage mag niet auto-selecteren")
        if self.algorithm_version != MULTI_SCAN_IDENTITY_VERSION:
            raise ValueError("onbekende multi-scan lineageversie")


@dataclass(frozen=True, slots=True)
class MultiScanBranchStep:
    decision_id: str
    revision: int
    source_branch_id: str
    assignment_solution_rank: int
    incremental_cost: float
    assignment_problem_sha256: str
    assignment_solution_sha256: str
    matched_association_evidence_sha256: tuple[str, ...]
    matched_advected_footprint_evidence_sha256: tuple[str, ...]
    matches: tuple[tuple[str, str, str], ...]
    gap_identity_ids: tuple[str, ...]
    births: tuple[tuple[str, str], ...]
    lineage_alternative_ids: tuple[str, ...]
    algorithm_version: str = MULTI_SCAN_IDENTITY_VERSION

    def __post_init__(self) -> None:
        _identifier(self.decision_id, "multi-scan decision_id")
        _identifier(self.source_branch_id, "multi-scan source_branch_id")
        _positive_integer(self.revision, "multi-scan step revision", 2_000_000)
        _positive_integer(
            self.assignment_solution_rank,
            "multi-scan assignment_solution_rank",
            10_000,
        )
        if not math.isfinite(self.incremental_cost) or self.incremental_cost < 0.0:
            raise ValueError("multi-scan incremental cost is ongeldig")
        _sha256(
            self.assignment_problem_sha256,
            "multi-scan assignment_problem_sha256",
        )
        _sha256(
            self.assignment_solution_sha256,
            "multi-scan assignment_solution_sha256",
        )
        if (
            self.matched_association_evidence_sha256
            != tuple(sorted(set(self.matched_association_evidence_sha256)))
        ):
            raise ValueError(
                "multi-scan matched association evidence is niet canoniek"
            )
        for value in self.matched_association_evidence_sha256:
            _sha256(value, "multi-scan matched association evidence")
        if (
            self.matched_advected_footprint_evidence_sha256
            != tuple(sorted(set(self.matched_advected_footprint_evidence_sha256)))
        ):
            raise ValueError("multi-scan matched advected evidence is niet canoniek")
        for value in self.matched_advected_footprint_evidence_sha256:
            _sha256(value, "multi-scan matched advected footprint evidence")
        if self.matches != tuple(sorted(self.matches)):
            raise ValueError("multi-scan matches moeten canoniek zijn")
        match_identities = []
        previous_ids = []
        matched_current = []
        for identity_id, previous_id, current_id in self.matches:
            _identifier(identity_id, "multi-scan match identity")
            _identifier(previous_id, "multi-scan match previous detection")
            _identifier(current_id, "multi-scan match current detection")
            match_identities.append(identity_id)
            previous_ids.append(previous_id)
            matched_current.append(current_id)
        if (
            len(match_identities) != len(set(match_identities))
            or len(previous_ids) != len(set(previous_ids))
            or len(matched_current) != len(set(matched_current))
        ):
            raise ValueError("multi-scan matches zijn niet een-op-een")
        if len(self.matched_association_evidence_sha256) != len(self.matches):
            raise ValueError("multi-scan match/evidence accounting verschilt")
        if len(self.matched_advected_footprint_evidence_sha256) != len(self.matches):
            raise ValueError("multi-scan match/advected accounting verschilt")
        if self.gap_identity_ids != tuple(sorted(set(self.gap_identity_ids))):
            raise ValueError("multi-scan gap identities moeten canoniek zijn")
        if set(match_identities) & set(self.gap_identity_ids):
            raise ValueError("multi-scan identity kan niet match en gap zijn")
        if self.births != tuple(sorted(self.births)):
            raise ValueError("multi-scan births moeten canoniek zijn")
        birth_identities = tuple(item[0] for item in self.births)
        birth_detections = tuple(item[1] for item in self.births)
        if (
            len(birth_identities) != len(set(birth_identities))
            or len(birth_detections) != len(set(birth_detections))
            or set(birth_detections) & set(matched_current)
        ):
            raise ValueError("multi-scan births zijn niet uniek")
        for identity_id, detection_id in self.births:
            _identifier(identity_id, "multi-scan birth identity")
            _identifier(detection_id, "multi-scan birth detection")
        if self.lineage_alternative_ids != tuple(
            sorted(set(self.lineage_alternative_ids))
        ):
            raise ValueError("multi-scan lineage alternative IDs zijn niet canoniek")
        if self.algorithm_version != MULTI_SCAN_IDENTITY_VERSION:
            raise ValueError("onbekende multi-scan stepversie")


@dataclass(frozen=True, slots=True)
class MultiScanBranch:
    branch_id: str
    rank: int
    relative_cost: float
    conditional_weight: float
    paths: tuple[MultiScanTrackPath, ...]
    steps: tuple[MultiScanBranchStep, ...]
    selected: bool
    algorithm_version: str = MULTI_SCAN_IDENTITY_VERSION

    def __post_init__(self) -> None:
        _identifier(self.branch_id, "multi-scan branch_id")
        _positive_integer(self.rank, "multi-scan branch rank", 10_000)
        if (
            not math.isfinite(self.relative_cost)
            or self.relative_cost < 0.0
            or not math.isfinite(self.conditional_weight)
            or not 0.0 <= self.conditional_weight <= 1.0
        ):
            raise ValueError("multi-scan branch cost/weight is ongeldig")
        if self.selected != (self.rank == 1):
            raise ValueError("multi-scan selected moet exact rank 1 zijn")
        if (
            self.paths != tuple(sorted(self.paths, key=lambda item: item.identity_id))
            or len({item.identity_id for item in self.paths}) != len(self.paths)
            or len({item.latest_detection_id for item in self.paths}) != len(self.paths)
        ):
            raise ValueError("multi-scan branchpaths zijn niet canoniek uniek")
        if (
            not self.steps
            or any(not isinstance(item, MultiScanBranchStep) for item in self.steps)
            or tuple(item.revision for item in self.steps)
            != tuple(range(self.steps[0].revision, self.steps[-1].revision + 1))
        ):
            raise ValueError("multi-scan branchsteps zijn niet contigu")
        if self.algorithm_version != MULTI_SCAN_IDENTITY_VERSION:
            raise ValueError("onbekende multi-scan branchversie")


@dataclass(frozen=True, slots=True)
class MultiScanIdentityPosterior:
    posterior_id: str
    policy_fingerprint: str
    revision: int
    branches: tuple[MultiScanBranch, ...]
    frame_buffer: tuple[HierarchicalDetectionFrame, ...]
    lineage_alternatives: tuple[MultiScanLineageAlternative, ...]
    generated_branch_count: int
    retained_branch_count: int
    pruned_by_beam_count: int
    pruned_by_n_scan_count: int
    candidate_edge_count: int
    assignment_subproblems_explored: int
    assignment_subproblems_limit: int
    assignment_search_exhausted_all_branches: bool
    conditional_entropy_nats: float
    effective_hypothesis_count: float
    omitted_hypothesis_mass_unknown: bool
    committed_through_revision: int
    committed_decision_count: int
    committed_decision_chain_sha256: str
    newly_committed_step: MultiScanBranchStep | None
    algorithm_version: str = MULTI_SCAN_IDENTITY_VERSION

    def __post_init__(self) -> None:
        _identifier(self.posterior_id, "multi-scan posterior_id")
        _sha256(self.policy_fingerprint, "multi-scan policy_fingerprint")
        _positive_integer(self.revision, "multi-scan revision", 2_000_000)
        if (
            not self.branches
            or self.branches != tuple(sorted(self.branches, key=lambda item: item.rank))
            or tuple(item.rank for item in self.branches)
            != tuple(range(1, len(self.branches) + 1))
            or not self.branches[0].selected
            or any(item.selected for item in self.branches[1:])
        ):
            raise ValueError("multi-scan branches zijn niet canoniek gerangschikt")
        if not math.isclose(
            math.fsum(item.conditional_weight for item in self.branches),
            1.0,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("multi-scan branchweights sommeren niet tot één")
        if not math.isclose(
            self.branches[0].relative_cost,
            0.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError("multi-scan best branch moet relatieve kost nul hebben")
        if not self.frame_buffer or tuple(
            item.valid_at for item in self.frame_buffer
        ) != tuple(sorted(item.valid_at for item in self.frame_buffer)):
            raise ValueError("multi-scan framebuffer is ongeldig")
        if self.lineage_alternatives != tuple(
            sorted(self.lineage_alternatives, key=lambda item: item.alternative_id)
        ) or len({item.alternative_id for item in self.lineage_alternatives}) != len(
            self.lineage_alternatives
        ):
            raise ValueError("multi-scan lineagealternatieven zijn niet canoniek")
        referenced_lineage = {
            alternative_id
            for branch in self.branches
            for step in branch.steps
            for alternative_id in step.lineage_alternative_ids
        }
        if self.newly_committed_step is not None:
            referenced_lineage.update(
                self.newly_committed_step.lineage_alternative_ids
            )
        if referenced_lineage != {
            item.alternative_id for item in self.lineage_alternatives
        }:
            raise ValueError(
                "multi-scan lineagealternatieven dekken de branchhistorie niet exact"
            )
        for label, value in (
            ("generated_branch_count", self.generated_branch_count),
            ("retained_branch_count", self.retained_branch_count),
            ("pruned_by_beam_count", self.pruned_by_beam_count),
            ("pruned_by_n_scan_count", self.pruned_by_n_scan_count),
            ("candidate_edge_count", self.candidate_edge_count),
            (
                "assignment_subproblems_explored",
                self.assignment_subproblems_explored,
            ),
            ("assignment_subproblems_limit", self.assignment_subproblems_limit),
            ("committed_through_revision", self.committed_through_revision),
            ("committed_decision_count", self.committed_decision_count),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"multi-scan {label} is ongeldig")
        if (
            self.retained_branch_count != len(self.branches)
            or self.generated_branch_count
            != self.retained_branch_count
            + self.pruned_by_beam_count
            + self.pruned_by_n_scan_count
            or self.assignment_subproblems_limit <= 0
            or self.assignment_subproblems_explored
            > self.assignment_subproblems_limit
        ):
            raise ValueError("multi-scan accounting sluit niet")
        for label, value in (
            ("conditional_entropy_nats", self.conditional_entropy_nats),
            ("effective_hypothesis_count", self.effective_hypothesis_count),
        ):
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"multi-scan {label} is ongeldig")
        if self.effective_hypothesis_count < 1.0 - 1e-12:
            raise ValueError("multi-scan effective hypothesis count is te klein")
        for label, value in (
            (
                "assignment_search_exhausted_all_branches",
                self.assignment_search_exhausted_all_branches,
            ),
            ("omitted_hypothesis_mass_unknown", self.omitted_hypothesis_mass_unknown),
        ):
            if type(value) is not bool:
                raise TypeError(f"multi-scan {label} moet bool zijn")
        expected_omitted = not (
            self.assignment_search_exhausted_all_branches
            and self.pruned_by_beam_count == 0
            and self.pruned_by_n_scan_count == 0
        )
        if self.omitted_hypothesis_mass_unknown != expected_omitted:
            raise ValueError("multi-scan omitted-mass accounting verschilt")
        _sha256(
            self.committed_decision_chain_sha256,
            "multi-scan committed decision chain",
        )
        if self.committed_decision_count != self.committed_through_revision:
            raise ValueError("multi-scan commit count/revision verschilt")
        if self.newly_committed_step is not None and (
            not isinstance(self.newly_committed_step, MultiScanBranchStep)
            or self.newly_committed_step.revision != self.committed_through_revision
        ):
            raise ValueError("multi-scan newly committed step verschilt")
        if self.algorithm_version != MULTI_SCAN_IDENTITY_VERSION:
            raise ValueError("onbekende multi-scan posteriorversie")


@dataclass(frozen=True, slots=True)
class MultiScanAdvanceResult:
    status: Literal["advanced", "indeterminate"]
    reason_codes: tuple[str, ...]
    posterior: MultiScanIdentityPosterior | None
    candidate_edge_count: int
    generated_branch_count: int
    assignment_subproblems_explored: int

    def __post_init__(self) -> None:
        if self.status not in ("advanced", "indeterminate"):
            raise ValueError("multi-scan advance status is ongeldig")
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise ValueError("multi-scan reasons moeten canoniek zijn")
        for label, value in (
            ("candidate_edge_count", self.candidate_edge_count),
            ("generated_branch_count", self.generated_branch_count),
            (
                "assignment_subproblems_explored",
                self.assignment_subproblems_explored,
            ),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"multi-scan {label} is ongeldig")
        if self.status == "advanced":
            if self.reason_codes or not isinstance(
                self.posterior,
                MultiScanIdentityPosterior,
            ):
                raise ValueError("advanced multi-scan result vereist posterior")
        elif not self.reason_codes or self.posterior is not None:
            raise ValueError("indeterminate multi-scan result vereist alleen redenen")


@dataclass(frozen=True, slots=True)
class SystemTranslationEvidence:
    evidence_id: str
    previous_frame_id: str
    current_frame_id: str
    previous_valid_at: float
    current_valid_at: float
    available: bool
    displacement_x_m: float | None
    displacement_y_m: float | None
    interval_speed_kmh: float | None
    covariance_m2: tuple[tuple[float, float], tuple[float, float]]
    previous_support_objects: int
    current_support_objects: int
    previous_support_area_m2: float
    current_support_area_m2: float
    method: str
    uncertainty_reason_codes: tuple[str, ...]
    algorithm_version: str = MULTI_HYPOTHESIS_IDENTITY_VERSION

    def __post_init__(self) -> None:
        _identifier(self.evidence_id, "translation evidence_id")
        _identifier(self.previous_frame_id, "previous_frame_id")
        _identifier(self.current_frame_id, "current_frame_id")
        if (
            not math.isfinite(self.previous_valid_at)
            or not math.isfinite(self.current_valid_at)
            or self.current_valid_at <= self.previous_valid_at
        ):
            raise ValueError("translation frametijden zijn ongeldig")
        if type(self.available) is not bool:
            raise TypeError("translation available moet bool zijn")
        values = (
            self.displacement_x_m,
            self.displacement_y_m,
            self.interval_speed_kmh,
        )
        if self.available:
            if any(value is None or not math.isfinite(value) for value in values):
                raise ValueError("beschikbare translation vereist eindige waarden")
            if self.uncertainty_reason_codes:
                raise ValueError("beschikbare translation mag geen failure reason dragen")
        elif any(value is not None for value in values):
            raise ValueError("onbeschikbare translation mag geen puntwaarde claimen")
        _covariance2(self.covariance_m2)
        for label, value in (
            ("previous_support_objects", self.previous_support_objects),
            ("current_support_objects", self.current_support_objects),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{label} is ongeldig")
        for label, value in (
            ("previous_support_area_m2", self.previous_support_area_m2),
            ("current_support_area_m2", self.current_support_area_m2),
        ):
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"{label} is ongeldig")
        _identifier(self.method, "translation method")
        if self.uncertainty_reason_codes != tuple(
            sorted(set(self.uncertainty_reason_codes))
        ):
            raise ValueError("translation uncertainty reasons moeten canoniek zijn")
        if not self.available and not self.uncertainty_reason_codes:
            raise ValueError("onbeschikbare translation vereist een reden")
        if self.algorithm_version != MULTI_HYPOTHESIS_IDENTITY_VERSION:
            raise ValueError("onbekende translation evidenceversie")


@dataclass(frozen=True, slots=True)
class LocalCellDeformationCandidate:
    candidate_id: str
    previous_detection_id: str
    current_detection_id: str
    advected_footprint_evidence: AdvectedFootprintEvidence
    association_evidence_sha256: str
    association_cost_sha256: str | None
    association_cost: float | None
    observed_displacement_x_m: float
    observed_displacement_y_m: float
    system_translation_evidence_id: str
    system_translation_method: str
    system_translation_x_m: float
    system_translation_y_m: float
    prior_local_deformation_x_m: float
    prior_local_deformation_y_m: float
    local_deformation_x_m: float
    local_deformation_y_m: float
    prediction_residual_x_m: float
    prediction_residual_y_m: float
    sigma_x_m: float
    sigma_y_m: float
    standardized_squared: float
    weighted_cost: float
    selected_in_best: bool
    selected_hypothesis_ranks: tuple[int, ...]
    algorithm_version: str = MULTI_HYPOTHESIS_IDENTITY_VERSION

    def __post_init__(self) -> None:
        for label, value in (
            ("candidate_id", self.candidate_id),
            ("previous_detection_id", self.previous_detection_id),
            ("current_detection_id", self.current_detection_id),
            (
                "system_translation_evidence_id",
                self.system_translation_evidence_id,
            ),
            ("system_translation_method", self.system_translation_method),
        ):
            _identifier(value, label)
        if not isinstance(
            self.advected_footprint_evidence,
            AdvectedFootprintEvidence,
        ):
            raise TypeError("local deformation mist advected footprint evidence")
        if (
            self.advected_footprint_evidence.previous_detection_id
            != self.previous_detection_id
            or self.advected_footprint_evidence.current_detection_id
            != self.current_detection_id
            or self.advected_footprint_evidence.translation_result_sha256
            != self.system_translation_evidence_id
        ):
            raise ValueError("local deformation advected pair/translation verschilt")
        _sha256(
            self.association_evidence_sha256,
            "local association_evidence_sha256",
        )
        if (self.association_cost_sha256 is None) != (
            self.association_cost is None
        ):
            raise ValueError("local association costbinding moet volledig zijn")
        if self.association_cost_sha256 is not None:
            _sha256(
                self.association_cost_sha256,
                "local association_cost_sha256",
            )
            if (
                self.association_cost is None
                or not math.isfinite(self.association_cost)
                or self.association_cost < 0.0
            ):
                raise ValueError("local association cost is ongeldig")
        for value in (
            self.observed_displacement_x_m,
            self.observed_displacement_y_m,
            self.system_translation_x_m,
            self.system_translation_y_m,
            self.prior_local_deformation_x_m,
            self.prior_local_deformation_y_m,
            self.local_deformation_x_m,
            self.local_deformation_y_m,
            self.prediction_residual_x_m,
            self.prediction_residual_y_m,
            self.sigma_x_m,
            self.sigma_y_m,
            self.standardized_squared,
            self.weighted_cost,
        ):
            if not math.isfinite(value):
                raise ValueError("local-deformationcandidate bevat niet-eindige waarde")
        if (
            self.sigma_x_m <= 0.0
            or self.sigma_y_m <= 0.0
            or self.standardized_squared < 0.0
            or self.weighted_cost < 0.0
        ):
            raise ValueError("local-deformation uncertainty/cost is ongeldig")
        if (
            self.system_translation_x_m
            != self.advected_footprint_evidence.applied_displacement_x_m
            or self.system_translation_y_m
            != self.advected_footprint_evidence.applied_displacement_y_m
        ):
            raise ValueError("local deformation gebruikt andere advected shift")
        if type(self.selected_in_best) is not bool:
            raise TypeError("selected_in_best moet bool zijn")
        if (
            self.selected_hypothesis_ranks
            != tuple(sorted(set(self.selected_hypothesis_ranks)))
            or any(
                isinstance(item, bool) or not isinstance(item, int) or item <= 0
                for item in self.selected_hypothesis_ranks
            )
            or self.selected_in_best != (1 in self.selected_hypothesis_ranks)
        ):
            raise ValueError("candidate hypothesis ranks zijn ongeldig")
        if self.algorithm_version != MULTI_HYPOTHESIS_IDENTITY_VERSION:
            raise ValueError("onbekende local-deformationversie")


@dataclass(frozen=True, slots=True)
class IdentityAssignmentHypothesis:
    hypothesis_id: str
    rank: int
    total_cost: float
    delta_cost: float
    conditional_weight: float
    matches: tuple[tuple[str, str], ...]
    deaths: tuple[str, ...]
    births: tuple[str, ...]
    selected: bool

    def __post_init__(self) -> None:
        _identifier(self.hypothesis_id, "identity hypothesis_id")
        if isinstance(self.rank, bool) or not isinstance(self.rank, int) or self.rank <= 0:
            raise ValueError("identity hypothesis rank is ongeldig")
        if (
            not math.isfinite(self.total_cost)
            or not math.isfinite(self.delta_cost)
            or self.delta_cost < 0.0
            or not math.isfinite(self.conditional_weight)
            or not 0.0 <= self.conditional_weight <= 1.0
        ):
            raise ValueError("identity hypothesis cost/weight is ongeldig")
        if type(self.selected) is not bool or self.selected != (self.rank == 1):
            raise ValueError("identity hypothesis selection moet exact rank 1 zijn")


@dataclass(frozen=True, slots=True)
class MultiHypothesisIdentitySnapshot:
    snapshot_id: str
    revision: int
    translation: SystemTranslationEvidence
    system_translations: tuple[SystemTranslationEvidence, ...]
    local_candidates: tuple[LocalCellDeformationCandidate, ...]
    hypotheses: tuple[IdentityAssignmentHypothesis, ...]
    derived_lineage_hypothesis_ids: tuple[str, ...]
    candidate_edge_count: int
    expected_candidate_edge_count: int
    lineage_eligible_previous_objects: int
    lineage_ineligible_gap_objects: int
    assignment_solution_limit: int
    assignment_explored_subproblems: int
    assignment_maximum_subproblems: int
    assignment_search_exhausted: bool
    conditional_entropy_nats: float
    omitted_hypothesis_mass_unknown: bool
    algorithm_version: str = MULTI_HYPOTHESIS_IDENTITY_VERSION

    def __post_init__(self) -> None:
        _identifier(self.snapshot_id, "identity snapshot_id")
        for label, value in (
            ("revision", self.revision),
            ("candidate_edge_count", self.candidate_edge_count),
            ("expected_candidate_edge_count", self.expected_candidate_edge_count),
            (
                "lineage_eligible_previous_objects",
                self.lineage_eligible_previous_objects,
            ),
            ("lineage_ineligible_gap_objects", self.lineage_ineligible_gap_objects),
            ("assignment_solution_limit", self.assignment_solution_limit),
            (
                "assignment_explored_subproblems",
                self.assignment_explored_subproblems,
            ),
            (
                "assignment_maximum_subproblems",
                self.assignment_maximum_subproblems,
            ),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{label} is ongeldig")
        if self.revision <= 0 or self.assignment_solution_limit <= 0 or self.assignment_maximum_subproblems <= 0:
            raise ValueError("identity snapshot vereist positieve revision/limits")
        if not isinstance(self.translation, SystemTranslationEvidence):
            raise TypeError("identity snapshot translationtype is ongeldig")
        if (
            not isinstance(self.system_translations, tuple)
            or any(
                not isinstance(item, SystemTranslationEvidence)
                for item in self.system_translations
            )
            or self.system_translations != tuple(
                sorted(self.system_translations, key=lambda item: item.evidence_id)
            )
            or len({item.evidence_id for item in self.system_translations})
            != len(self.system_translations)
        ):
            raise ValueError("system translations moeten canoniek uniek zijn")
        if (
            not isinstance(self.local_candidates, tuple)
            or any(
                not isinstance(item, LocalCellDeformationCandidate)
                for item in self.local_candidates
            )
            or self.local_candidates != tuple(
                sorted(
                    self.local_candidates,
                    key=lambda item: (
                        item.previous_detection_id,
                        item.current_detection_id,
                    ),
                )
            )
        ):
            raise ValueError("local candidates moeten canoniek zijn")
        pairs = tuple(
            (item.previous_detection_id, item.current_detection_id)
            for item in self.local_candidates
        )
        if len(pairs) != len(set(pairs)) or len(pairs) != self.candidate_edge_count:
            raise ValueError("identity candidate accounting verschilt")
        if self.candidate_edge_count != self.expected_candidate_edge_count:
            raise ValueError("identity snapshot verloor een kandidaat")
        if (
            not isinstance(self.hypotheses, tuple)
            or not self.hypotheses
            or any(not isinstance(item, IdentityAssignmentHypothesis) for item in self.hypotheses)
            or tuple(item.rank for item in self.hypotheses)
            != tuple(range(1, len(self.hypotheses) + 1))
            or len(self.hypotheses) > self.assignment_solution_limit
        ):
            raise ValueError("identity hypotheses zijn niet canoniek begrensd")
        if not math.isclose(
            math.fsum(item.conditional_weight for item in self.hypotheses),
            1.0,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError("conditional identity weights sommeren niet tot één")
        if self.derived_lineage_hypothesis_ids != tuple(
            sorted(set(self.derived_lineage_hypothesis_ids))
        ):
            raise ValueError("derived lineage IDs moeten canoniek zijn")
        if type(self.assignment_search_exhausted) is not bool:
            raise TypeError("assignment_search_exhausted moet bool zijn")
        if (
            not math.isfinite(self.conditional_entropy_nats)
            or self.conditional_entropy_nats < 0.0
            or type(self.omitted_hypothesis_mass_unknown) is not bool
            or self.omitted_hypothesis_mass_unknown
            == self.assignment_search_exhausted
        ):
            raise ValueError("identity uncertainty accounting is inconsistent")
        if self.algorithm_version != MULTI_HYPOTHESIS_IDENTITY_VERSION:
            raise ValueError("onbekende identity snapshotversie")


def estimate_system_translation(
    *,
    previous_frame_id: str,
    current_frame_id: str,
    previous_valid_at: float,
    current_valid_at: float,
    previous_detections: Iterable[HierarchicalCellDetection],
    current_detections: Iterable[HierarchicalCellDetection],
    policy: MultiHypothesisIdentityPolicy,
) -> SystemTranslationEvidence:
    """Estimate one field translation without using any selected identity."""

    previous = _translation_support(previous_detections)
    current = _translation_support(current_detections)
    ceiling_variance = policy.system_translation_sigma_ceiling_m**2
    if not previous or not current:
        reasons = []
        if not previous:
            reasons.append("previous_translation_support_absent")
        if not current:
            reasons.append("current_translation_support_absent")
        return SystemTranslationEvidence(
            evidence_id="system-translation:" + _digest(
                previous_frame_id,
                current_frame_id,
                *reasons,
            ),
            previous_frame_id=previous_frame_id,
            current_frame_id=current_frame_id,
            previous_valid_at=previous_valid_at,
            current_valid_at=current_valid_at,
            available=False,
            displacement_x_m=None,
            displacement_y_m=None,
            interval_speed_kmh=None,
            covariance_m2=((ceiling_variance, 0.0), (0.0, ceiling_variance)),
            previous_support_objects=len(previous),
            current_support_objects=len(current),
            previous_support_area_m2=math.fsum(item.area_m2 for item in previous),
            current_support_area_m2=math.fsum(item.area_m2 for item in current),
            method="whole_field_area_weighted_centroid_translation",
            uncertainty_reason_codes=tuple(sorted(reasons)),
        )
    old_x, old_y, old_var_x, old_var_y, old_area = _weighted_centroid(previous)
    new_x, new_y, new_var_x, new_var_y, new_area = _weighted_centroid(current)
    dx = new_x - old_x
    dy = new_y - old_y
    elapsed = current_valid_at - previous_valid_at
    floor_variance = policy.system_translation_sigma_floor_m**2
    variance_x = min(ceiling_variance, max(floor_variance, old_var_x + new_var_x))
    variance_y = min(ceiling_variance, max(floor_variance, old_var_y + new_var_y))
    return SystemTranslationEvidence(
        evidence_id="system-translation:" + _digest(
            previous_frame_id,
            current_frame_id,
            format(dx, ".17g"),
            format(dy, ".17g"),
            format(variance_x, ".17g"),
            format(variance_y, ".17g"),
        ),
        previous_frame_id=previous_frame_id,
        current_frame_id=current_frame_id,
        previous_valid_at=previous_valid_at,
        current_valid_at=current_valid_at,
        available=True,
        displacement_x_m=dx,
        displacement_y_m=dy,
        interval_speed_kmh=math.hypot(dx, dy) / elapsed * 3.6,
        covariance_m2=((variance_x, 0.0), (0.0, variance_y)),
        previous_support_objects=len(previous),
        current_support_objects=len(current),
        previous_support_area_m2=old_area,
        current_support_area_m2=new_area,
        method="whole_field_area_weighted_centroid_translation",
        uncertainty_reason_codes=(),
    )


def build_local_deformation_candidates(
    *,
    features: Iterable[AssociationFeatureVector],
    advected_evidence_by_pair: dict[
        tuple[str, str],
        AdvectedFootprintEvidence,
    ],
    translation_uncertainty_by_result_sha256: dict[
        str,
        SystemTranslationEvidence,
    ],
    previous_snapshot: MultiHypothesisIdentitySnapshot | None,
    policy: MultiHypothesisIdentityPolicy,
) -> tuple[LocalCellDeformationCandidate, ...]:
    values = tuple(features)
    if len(values) > policy.maximum_retained_local_candidates:
        raise ValueError("local_deformation_candidate_capacity_exceeded")
    expected_pairs = {
        (item.previous_detection_id, item.current_detection_id)
        for item in values
    }
    if set(advected_evidence_by_pair) != expected_pairs:
        raise ValueError("advected evidence dekt local candidates niet exact")
    previous_local: dict[str, tuple[float, float]] = {}
    if previous_snapshot is not None:
        previous_local = {
            item.current_detection_id: (
                item.local_deformation_x_m,
                item.local_deformation_y_m,
            )
            for item in previous_snapshot.local_candidates
            if item.selected_in_best
        }
    result = []
    for item in values:
        pair = (item.previous_detection_id, item.current_detection_id)
        advected = advected_evidence_by_pair[pair]
        selected_translation = translation_uncertainty_by_result_sha256.get(
            advected.translation_result_sha256
        )
        if selected_translation is None:
            raise ValueError("advected translation mist uncertainty evidence")
        if selected_translation.evidence_id != advected.translation_result_sha256:
            raise ValueError("translation uncertainty evidence-ID verschilt")
        tx = advected.applied_displacement_x_m
        ty = advected.applied_displacement_y_m
        translation_elapsed = (
            selected_translation.current_valid_at
            - selected_translation.previous_valid_at
        )
        elapsed_scale = item.elapsed_seconds / translation_elapsed
        # The point shift comes exclusively from the pair receipt.  Gap
        # receipts are explicit unadvected fallbacks, so a one-frame vector is
        # never extrapolated over an older detection.
        uncertainty_scale = max(1.0, elapsed_scale)
        translation_sigma_x = (
            math.sqrt(selected_translation.covariance_m2[0][0])
            * uncertainty_scale
        )
        translation_sigma_y = (
            math.sqrt(selected_translation.covariance_m2[1][1])
            * uncertainty_scale
        )
        sigma_x = min(
            policy.local_deformation_sigma_ceiling_m,
            max(policy.local_deformation_sigma_floor_m, translation_sigma_x),
        )
        sigma_y = min(
            policy.local_deformation_sigma_ceiling_m,
            max(policy.local_deformation_sigma_floor_m, translation_sigma_y),
        )
        prior_x, prior_y = previous_local.get(item.previous_detection_id, (0.0, 0.0))
        prior_x *= policy.previous_local_deformation_weight
        prior_y *= policy.previous_local_deformation_weight
        local_x = item.observed_displacement_x_m - tx
        local_y = item.observed_displacement_y_m - ty
        residual_x = local_x - prior_x
        residual_y = local_y - prior_y
        standardized = (residual_x / sigma_x) ** 2 + (residual_y / sigma_y) ** 2
        weighted = 0.5 * standardized * policy.local_deformation_cost_weight
        association_evidence_sha256 = _sha_payload({
            "advected_footprint_evidence_sha256": advected.evidence_sha256,
            "algorithm_version": MULTI_HYPOTHESIS_IDENTITY_VERSION,
            "feature": _canonical_dataclass_mapping(item),
            "local_deformation": {
                "local_x_m": local_x.hex(),
                "local_y_m": local_y.hex(),
                "prior_x_m": prior_x.hex(),
                "prior_y_m": prior_y.hex(),
                "residual_x_m": residual_x.hex(),
                "residual_y_m": residual_y.hex(),
                "sigma_x_m": sigma_x.hex(),
                "sigma_y_m": sigma_y.hex(),
                "standardized_squared": standardized.hex(),
                "weighted_cost": weighted.hex(),
            },
            "translation_uncertainty_evidence": (
                _canonical_dataclass_mapping(selected_translation)
            ),
        })
        result.append(LocalCellDeformationCandidate(
            candidate_id="local-deformation:" + _digest(
                association_evidence_sha256,
                "cost-unbound",
            ),
            previous_detection_id=item.previous_detection_id,
            current_detection_id=item.current_detection_id,
            advected_footprint_evidence=advected,
            association_evidence_sha256=association_evidence_sha256,
            association_cost_sha256=None,
            association_cost=None,
            observed_displacement_x_m=item.observed_displacement_x_m,
            observed_displacement_y_m=item.observed_displacement_y_m,
            system_translation_evidence_id=advected.translation_result_sha256,
            system_translation_method=f"advected_footprint_{advected.mode}",
            system_translation_x_m=tx,
            system_translation_y_m=ty,
            prior_local_deformation_x_m=prior_x,
            prior_local_deformation_y_m=prior_y,
            local_deformation_x_m=local_x,
            local_deformation_y_m=local_y,
            prediction_residual_x_m=residual_x,
            prediction_residual_y_m=residual_y,
            sigma_x_m=sigma_x,
            sigma_y_m=sigma_y,
            standardized_squared=standardized,
            weighted_cost=weighted,
            selected_in_best=False,
            selected_hypothesis_ranks=(),
        ))
    return tuple(sorted(
        result,
        key=lambda item: (item.previous_detection_id, item.current_detection_id),
    ))


def bind_local_candidate_costs(
    *,
    local_candidates: tuple[LocalCellDeformationCandidate, ...],
    edge_costs: tuple[AssignmentEdgeCost, ...],
) -> tuple[LocalCellDeformationCandidate, ...]:
    """Bind complete final edge-cost receipts before snapshot persistence."""

    edge_by_pair = {
        (item.previous_detection_id, item.current_detection_id): item
        for item in edge_costs
    }
    if len(edge_by_pair) != len(edge_costs):
        raise ValueError("dubbele association edge cost")
    expected_pairs = {
        (item.previous_detection_id, item.current_detection_id)
        for item in local_candidates
    }
    if set(edge_by_pair) != expected_pairs:
        raise ValueError("edge costs dekken local candidates niet exact")
    bound = []
    for item in local_candidates:
        pair = (item.previous_detection_id, item.current_detection_id)
        edge = edge_by_pair[pair]
        cost_sha256 = _assignment_edge_cost_sha256(
            edge,
            association_evidence_sha256=item.association_evidence_sha256,
        )
        bound.append(replace(
            item,
            candidate_id="local-deformation:" + _digest(
                item.association_evidence_sha256,
                cost_sha256,
                edge.cost.hex(),
            ),
            association_cost_sha256=cost_sha256,
            association_cost=edge.cost,
        ))
    return tuple(sorted(
        bound,
        key=lambda item: (item.previous_detection_id, item.current_detection_id),
    ))


def finalize_identity_snapshot(
    *,
    revision: int,
    translation: SystemTranslationEvidence,
    system_translations: tuple[SystemTranslationEvidence, ...],
    local_candidates: tuple[LocalCellDeformationCandidate, ...],
    expected_candidate_edge_count: int,
    assignment: GlobalAssignmentResult,
    derived_lineage_hypothesis_ids: tuple[str, ...],
    lineage_eligible_previous_objects: int,
    lineage_ineligible_gap_objects: int,
    policy: MultiHypothesisIdentityPolicy,
) -> MultiHypothesisIdentitySnapshot:
    best_cost = assignment.best.total_cost
    exponentials = tuple(
        math.exp(
            -min(
                700.0,
                max(0.0, item.total_cost - best_cost)
                / policy.conditional_cost_temperature,
            )
        )
        for item in assignment.solutions
    )
    total_weight = math.fsum(exponentials)
    weights = tuple(item / total_weight for item in exponentials)
    local_by_pair = {
        (item.previous_detection_id, item.current_detection_id): item
        for item in local_candidates
    }
    hypotheses = tuple(
        IdentityAssignmentHypothesis(
            hypothesis_id="identity-assignment:" + _digest(
                policy.fingerprint,
                translation.evidence_id,
                str(solution.rank),
                solution.total_cost.hex(),
                max(0.0, solution.total_cost - best_cost).hex(),
                weight.hex(),
                *(local_by_pair[pair].candidate_id for pair in solution.matches),
                *(
                    local_by_pair[pair].association_cost_sha256 or "unbound"
                    for pair in solution.matches
                ),
                *(f"{old}>{new}" for old, new in solution.matches),
                *(f"death:{item}" for item in solution.deaths),
                *(f"birth:{item}" for item in solution.births),
            ),
            rank=solution.rank,
            total_cost=solution.total_cost,
            delta_cost=max(0.0, solution.total_cost - best_cost),
            conditional_weight=weight,
            matches=solution.matches,
            deaths=solution.deaths,
            births=solution.births,
            selected=solution.rank == 1,
        )
        for solution, weight in zip(assignment.solutions, weights, strict=True)
    )
    ranks_by_pair: dict[tuple[str, str], list[int]] = {}
    for hypothesis in hypotheses:
        for pair in hypothesis.matches:
            ranks_by_pair.setdefault(pair, []).append(hypothesis.rank)
    if any(
        item.association_cost_sha256 is None or item.association_cost is None
        for item in local_candidates
    ):
        raise ValueError("identity snapshot vereist cost-bound candidates")
    candidates = tuple(
        replace(
            item,
            selected_in_best=1 in ranks_by_pair.get(
                (item.previous_detection_id, item.current_detection_id),
                (),
            ),
            selected_hypothesis_ranks=tuple(ranks_by_pair.get(
                (item.previous_detection_id, item.current_detection_id),
                (),
            )),
        )
        for item in local_candidates
    )
    entropy = -math.fsum(
        weight * math.log(weight) for weight in weights if weight > 0.0
    )
    omitted_unknown = not assignment.search_exhausted
    snapshot_seed = (
        policy.fingerprint,
        str(revision),
        translation.evidence_id,
        *(item.evidence_id for item in system_translations),
        *(item.candidate_id for item in candidates),
        *(item.hypothesis_id for item in hypotheses),
        *derived_lineage_hypothesis_ids,
        str(assignment.explored_subproblems),
        str(assignment.maximum_subproblems),
        entropy.hex(),
        *(item.hex() for item in weights),
        str(lineage_eligible_previous_objects),
        str(lineage_ineligible_gap_objects),
        "search-exhausted" if assignment.search_exhausted else "omitted-unknown",
    )
    return MultiHypothesisIdentitySnapshot(
        snapshot_id="multi-hypothesis-identity:" + _digest(*snapshot_seed),
        revision=revision,
        translation=translation,
        system_translations=tuple(sorted(
            system_translations,
            key=lambda item: item.evidence_id,
        )),
        local_candidates=candidates,
        hypotheses=hypotheses,
        derived_lineage_hypothesis_ids=tuple(sorted(derived_lineage_hypothesis_ids)),
        candidate_edge_count=len(candidates),
        expected_candidate_edge_count=expected_candidate_edge_count,
        lineage_eligible_previous_objects=lineage_eligible_previous_objects,
        lineage_ineligible_gap_objects=lineage_ineligible_gap_objects,
        assignment_solution_limit=policy.solution_limit,
        assignment_explored_subproblems=assignment.explored_subproblems,
        assignment_maximum_subproblems=policy.maximum_assignment_subproblems,
        assignment_search_exhausted=assignment.search_exhausted,
        conditional_entropy_nats=entropy,
        omitted_hypothesis_mass_unknown=omitted_unknown,
    )


def bootstrap_multi_scan_identity(
    *,
    engine_id: str,
    detection_frame: HierarchicalDetectionFrame,
    policy: MultiHypothesisIdentityPolicy,
) -> MultiScanIdentityPosterior:
    """Create one bounded identity branch without claiming prior motion."""

    _identifier(engine_id, "multi-scan engine_id")
    if not isinstance(detection_frame, HierarchicalDetectionFrame):
        raise TypeError("multi-scan bootstrap vereist detection frame")
    multi_scan = policy.multi_scan
    if multi_scan is None:
        raise ValueError("multi-scan bootstrap vereist multi-scan policy")
    detections = tuple(sorted(
        detection_frame.detections,
        key=lambda item: item.detection.detection_id,
    ))
    if len(detections) > multi_scan.maximum_active_paths_per_branch:
        raise ValueError("multi_scan_active_path_capacity_exceeded")
    if len(detections) > multi_scan.maximum_buffered_detections:
        raise ValueError("multi_scan_buffered_detection_capacity_exceeded")
    births = tuple(sorted(
        (
            _multi_scan_birth_identity(engine_id, item.detection.detection_id),
            item.detection.detection_id,
        )
        for item in detections
    ))
    paths = tuple(sorted((
        MultiScanTrackPath(
            identity_id=identity_id,
            latest_detection_id=detection_id,
            missed_frames=0,
            detection_ids=(detection_id,),
            local_deformation_x_m=0.0,
            local_deformation_y_m=0.0,
        )
        for identity_id, detection_id in births
    ), key=lambda item: item.identity_id))
    origin_id = "multi-scan-origin:" + _digest(
        engine_id,
        multi_scan.fingerprint,
    )
    bootstrap_problem_sha256 = _sha_payload({
        "births": [list(item) for item in births],
        "engine_id": engine_id,
        "policy_fingerprint": multi_scan.fingerprint,
        "revision": 1,
    })
    bootstrap_solution_sha256 = _sha_payload({
        "assignment_problem_sha256": bootstrap_problem_sha256,
        "births": [list(item) for item in births],
        "incremental_cost": 0.0.hex(),
        "rank": 1,
    })
    step = MultiScanBranchStep(
        decision_id="multi-scan-decision:" + _digest(
            origin_id,
            str(1),
            bootstrap_problem_sha256,
            bootstrap_solution_sha256,
            *(f"birth:{identity}>{detection}" for identity, detection in births),
        ),
        revision=1,
        source_branch_id=origin_id,
        assignment_solution_rank=1,
        incremental_cost=0.0,
        assignment_problem_sha256=bootstrap_problem_sha256,
        assignment_solution_sha256=bootstrap_solution_sha256,
        matched_association_evidence_sha256=(),
        matched_advected_footprint_evidence_sha256=(),
        matches=(),
        gap_identity_ids=(),
        births=births,
        lineage_alternative_ids=(),
    )
    branch_id = "multi-scan-branch:" + _digest(
        origin_id,
        step.decision_id,
        *(
            _sha_payload(_canonical_dataclass_mapping(item))
            for item in paths
        ),
    )
    branch = MultiScanBranch(
        branch_id=branch_id,
        rank=1,
        relative_cost=0.0,
        conditional_weight=1.0,
        paths=paths,
        steps=(step,),
        selected=True,
    )
    posterior_id = "multi-scan-posterior:" + _digest(
        multi_scan.fingerprint,
        detection_frame.frame_id,
        branch_id,
        _sha_payload(_canonical_dataclass_mapping(branch)),
        EMPTY_MULTI_SCAN_COMMIT_SHA256,
    )
    return MultiScanIdentityPosterior(
        posterior_id=posterior_id,
        policy_fingerprint=multi_scan.fingerprint,
        revision=1,
        branches=(branch,),
        frame_buffer=(detection_frame,),
        lineage_alternatives=(),
        generated_branch_count=1,
        retained_branch_count=1,
        pruned_by_beam_count=0,
        pruned_by_n_scan_count=0,
        candidate_edge_count=0,
        assignment_subproblems_explored=0,
        assignment_subproblems_limit=(
            multi_scan.maximum_assignment_subproblems_total
        ),
        assignment_search_exhausted_all_branches=True,
        conditional_entropy_nats=0.0,
        effective_hypothesis_count=1.0,
        omitted_hypothesis_mass_unknown=False,
        committed_through_revision=0,
        committed_decision_count=0,
        committed_decision_chain_sha256=EMPTY_MULTI_SCAN_COMMIT_SHA256,
        newly_committed_step=None,
    )


def advance_multi_scan_identity(
    *,
    engine_id: str,
    previous: MultiScanIdentityPosterior,
    detection_frame: HierarchicalDetectionFrame,
    advected_evidence_by_pair: dict[
        tuple[str, str],
        AdvectedFootprintEvidence,
    ],
    translation_uncertainty_by_result_sha256: dict[
        str,
        SystemTranslationEvidence,
    ],
    policy: MultiHypothesisIdentityPolicy,
    ranking: MultiScanRankingWeights,
    maximum_gap_frames: int,
) -> MultiScanAdvanceResult:
    """Advance a branch-consistent bounded N-scan identity posterior.

    Every retained branch owns its identity paths and local-deformation memory.
    The complete branch-path/current-detection product is ranked without any
    speed or distance reject.  Capacity cannot silently shrink the beam.
    """

    _identifier(engine_id, "multi-scan engine_id")
    if not isinstance(previous, MultiScanIdentityPosterior):
        raise TypeError("multi-scan advance vereist previous posterior")
    if not isinstance(detection_frame, HierarchicalDetectionFrame):
        raise TypeError("multi-scan advance vereist detection frame")
    if not isinstance(advected_evidence_by_pair, dict):
        raise TypeError("multi-scan advance vereist advected evidence")
    if not isinstance(translation_uncertainty_by_result_sha256, dict):
        raise TypeError("multi-scan advance vereist translation uncertainty")
    if not isinstance(ranking, MultiScanRankingWeights):
        raise TypeError("multi-scan advance vereist ranking weights")
    multi_scan = policy.multi_scan
    if multi_scan is None:
        raise ValueError("multi-scan advance vereist multi-scan policy")
    if previous.policy_fingerprint != multi_scan.fingerprint:
        raise ValueError("multi-scan policy drift tussen frames")
    if previous.revision + 1 <= previous.revision:
        raise AssertionError("multi-scan revision overflow")
    if detection_frame.valid_at <= previous.frame_buffer[-1].valid_at:
        raise ValueError("multi-scan frametijd moet strikt oplopen")
    if detection_frame.analysis_grid_id != previous.frame_buffer[-1].analysis_grid_id:
        raise ValueError("multi-scan analysis-grid drift is niet toegelaten")
    if (
        isinstance(maximum_gap_frames, bool)
        or not isinstance(maximum_gap_frames, int)
        or maximum_gap_frames < 0
    ):
        raise ValueError("multi-scan maximum_gap_frames is ongeldig")
    if multi_scan.horizon_frames < maximum_gap_frames + 1:
        raise ValueError("multi-scan horizon is kleiner dan gap support")
    current = tuple(sorted(
        detection_frame.detections,
        key=lambda item: item.detection.detection_id,
    ))
    current_by_id = {item.detection.detection_id: item for item in current}
    previous_detection = {
        item.detection.detection_id: item
        for frame in previous.frame_buffer
        for item in frame.detections
    }
    buffered_frame_limit = multi_scan.horizon_frames + maximum_gap_frames + 1
    frame_buffer = (*previous.frame_buffer, detection_frame)[-buffered_frame_limit:]
    buffered_detection_count = sum(len(item.detections) for item in frame_buffer)
    if buffered_detection_count > multi_scan.maximum_buffered_detections:
        return _multi_scan_indeterminate(
            "multi_scan_buffered_detection_capacity_exceeded"
        )

    candidate_edge_count = sum(
        len(branch.paths) * len(current) for branch in previous.branches
    )
    if candidate_edge_count > multi_scan.maximum_candidate_edges_total:
        return _multi_scan_indeterminate(
            "multi_scan_candidate_edge_capacity_exceeded",
            candidate_edges=int(candidate_edge_count),
        )
    expected_evidence_pairs = {
        (path.latest_detection_id, item.detection.detection_id)
        for branch in previous.branches
        for path in branch.paths
        for item in current
    }
    if set(advected_evidence_by_pair) != expected_evidence_pairs:
        return _multi_scan_indeterminate(
            "multi_scan_advected_evidence_coverage_mismatch",
            candidate_edges=int(candidate_edge_count),
        )
    for pair, evidence in advected_evidence_by_pair.items():
        if (
            not isinstance(evidence, AdvectedFootprintEvidence)
            or pair
            != (
                evidence.previous_detection_id,
                evidence.current_detection_id,
            )
            or evidence.translation_result_sha256
            not in translation_uncertainty_by_result_sha256
        ):
            return _multi_scan_indeterminate(
                "multi_scan_advected_evidence_binding_mismatch",
                candidate_edges=int(candidate_edge_count),
            )
    generated: list[
        tuple[
            float,
            str,
            tuple[MultiScanTrackPath, ...],
            tuple[MultiScanBranchStep, ...],
        ]
    ] = []
    # Alternatives are part of the uncommitted branch history.  Seed the map
    # from the previous posterior so a restart or a later N-scan decision can
    # still audit every lineage ID referenced by an older retained step.
    lineage_by_id: dict[str, MultiScanLineageAlternative] = {
        item.alternative_id: item for item in previous.lineage_alternatives
    }
    subproblems = 0
    search_exhausted_all = True
    revision = previous.revision + 1
    for branch in previous.branches:
        if len(branch.paths) > multi_scan.maximum_active_paths_per_branch:
            return _multi_scan_indeterminate(
                "multi_scan_active_path_capacity_exceeded",
                candidate_edges=candidate_edge_count,
                generated=len(generated),
                subproblems=subproblems,
            )
        missing_support = tuple(
            item.latest_detection_id
            for item in branch.paths
            if item.latest_detection_id not in previous_detection
        )
        if missing_support:
            return _multi_scan_indeterminate(
                "multi_scan_branch_detection_support_missing",
                candidate_edges=candidate_edge_count,
                generated=len(generated),
                subproblems=subproblems,
            )
        lineage = _multi_scan_lineage_alternatives(
            branch=branch,
            revision=revision,
            previous_detection=previous_detection,
            current=current,
            advected_evidence_by_pair=advected_evidence_by_pair,
        )
        for item in lineage:
            existing = lineage_by_id.get(item.alternative_id)
            if existing is not None and existing != item:
                raise AssertionError("multi-scan lineage ID collision")
            lineage_by_id[item.alternative_id] = item
        if len(lineage_by_id) > multi_scan.maximum_lineage_alternatives:
            return _multi_scan_indeterminate(
                "multi_scan_lineage_alternative_capacity_exceeded",
                candidate_edges=candidate_edge_count,
                generated=len(generated),
                subproblems=subproblems,
            )

        problem, problem_sha256, edge_receipts = _multi_scan_assignment_problem(
            branch=branch,
            current=current,
            previous_detection=previous_detection,
            advected_evidence_by_pair=advected_evidence_by_pair,
            translation_uncertainty_by_result_sha256=(
                translation_uncertainty_by_result_sha256
            ),
            policy=policy,
            ranking=ranking,
        )
        remaining_subproblems = (
            multi_scan.maximum_assignment_subproblems_total - subproblems
        )
        if remaining_subproblems <= 0:
            return _multi_scan_indeterminate(
                "multi_scan_assignment_subproblem_capacity_exceeded",
                candidate_edges=candidate_edge_count,
                generated=len(generated),
                subproblems=subproblems,
            )
        assignment = solve_bounded_k_best_global_assignment(
            problem,
            solution_limit=policy.solution_limit,
            maximum_subproblems=min(
                policy.maximum_assignment_subproblems,
                remaining_subproblems,
            ),
        )
        subproblems += assignment.explored_subproblems
        if assignment.capacity_exhausted:
            return _multi_scan_indeterminate(
                "multi_scan_assignment_subproblem_capacity_exceeded",
                candidate_edges=candidate_edge_count,
                generated=len(generated),
                subproblems=subproblems,
            )
        search_exhausted_all = search_exhausted_all and assignment.search_exhausted
        lineage_ids = tuple(item.alternative_id for item in lineage)
        for solution in assignment.solutions:
            if len(generated) >= multi_scan.maximum_generated_branches:
                return _multi_scan_indeterminate(
                    "multi_scan_generated_branch_capacity_exceeded",
                    candidate_edges=candidate_edge_count,
                    generated=len(generated),
                    subproblems=subproblems,
                )
            paths, step = _multi_scan_apply_solution(
                engine_id=engine_id,
                source=branch,
                solution=solution,
                revision=revision,
                current_by_id=current_by_id,
                previous_detection=previous_detection,
                advected_evidence_by_pair=advected_evidence_by_pair,
                edge_receipt_by_identity_current=edge_receipts,
                assignment_problem_sha256=problem_sha256,
                policy=policy,
                maximum_gap_frames=maximum_gap_frames,
                lineage_alternative_ids=lineage_ids,
            )
            if len(paths) > multi_scan.maximum_active_paths_per_branch:
                return _multi_scan_indeterminate(
                    "multi_scan_active_path_capacity_exceeded",
                    candidate_edges=candidate_edge_count,
                    generated=len(generated),
                    subproblems=subproblems,
                )
            branch_id = "multi-scan-branch:" + _digest(
                branch.branch_id,
                step.decision_id,
                (branch.relative_cost + solution.total_cost).hex(),
                *(
                    _sha_payload(_canonical_dataclass_mapping(item))
                    for item in paths
                ),
            )
            generated.append((
                branch.relative_cost + solution.total_cost,
                branch_id,
                paths,
                (*branch.steps, step),
            ))

    if not generated:
        raise AssertionError("multi-scan solver leverde geen branches")
    generated.sort(key=lambda item: (item[0], item[3][-1].decision_id, item[1]))
    generated_count = len(generated)
    newly_committed: MultiScanBranchStep | None = None
    committed_through = previous.committed_through_revision
    committed_count = previous.committed_decision_count
    committed_chain = previous.committed_decision_chain_sha256
    pruned_n_scan = 0
    if revision - committed_through > multi_scan.horizon_frames:
        target_revision = committed_through + 1
        best_steps = {item.revision: item for item in generated[0][3]}
        newly_committed = best_steps.get(target_revision)
        if newly_committed is None:
            raise AssertionError("multi-scan N-scan anchor ontbreekt")
        anchor_id = newly_committed.decision_id
        retained_by_anchor = tuple(
            item
            for item in generated
            if any(
                step.revision == target_revision and step.decision_id == anchor_id
                for step in item[3]
            )
        )
        if not retained_by_anchor:
            raise AssertionError("multi-scan N-scan pruning verloor MAP ancestor")
        pruned_n_scan = len(generated) - len(retained_by_anchor)
        generated = [
            (cost, branch_id, paths, tuple(
                step for step in steps if step.revision > target_revision
            ))
            for cost, branch_id, paths, steps in retained_by_anchor
        ]
        committed_through = target_revision
        committed_count += 1
        committed_chain = _sha_payload({
            "previous_committed_decision_chain_sha256": (
                previous.committed_decision_chain_sha256
            ),
            "schema": "stv3-shadow-multi-scan-commit-chain-v2",
            "step": _canonical_dataclass_mapping(newly_committed),
            "target_revision": target_revision,
        })

    retained_raw = generated[:multi_scan.beam_width]
    pruned_beam = len(generated) - len(retained_raw)
    best_cost = retained_raw[0][0]
    relative_costs = tuple(max(0.0, item[0] - best_cost) for item in retained_raw)
    exponentials = tuple(
        math.exp(-min(700.0, value / multi_scan.conditional_cost_temperature))
        for value in relative_costs
    )
    total_weight = math.fsum(exponentials)
    weights = tuple(item / total_weight for item in exponentials)
    branches = tuple(
        MultiScanBranch(
            branch_id=item[1],
            rank=rank,
            relative_cost=relative_cost,
            conditional_weight=weight,
            paths=item[2],
            steps=item[3],
            selected=rank == 1,
        )
        for rank, (item, relative_cost, weight) in enumerate(
            zip(retained_raw, relative_costs, weights, strict=True),
            start=1,
        )
    )
    referenced_lineage = {
        alternative_id
        for branch in branches
        for step in branch.steps
        for alternative_id in step.lineage_alternative_ids
    }
    if newly_committed is not None:
        referenced_lineage.update(newly_committed.lineage_alternative_ids)
    lineage_alternatives = tuple(sorted(
        (
            item for identifier, item in lineage_by_id.items()
            if identifier in referenced_lineage
        ),
        key=lambda item: item.alternative_id,
    ))
    entropy = -math.fsum(
        weight * math.log(weight) for weight in weights if weight > 0.0
    )
    effective = math.exp(entropy)
    omitted_unknown = not (
        search_exhausted_all and pruned_beam == 0 and pruned_n_scan == 0
    )
    posterior_id = "multi-scan-posterior:" + _digest(
        multi_scan.fingerprint,
        previous.posterior_id,
        detection_frame.frame_id,
        *(
            _sha_payload(_canonical_dataclass_mapping(item))
            for item in branches
        ),
        *(
            _sha_payload(_canonical_dataclass_mapping(item))
            for item in lineage_alternatives
        ),
        committed_chain,
        str(candidate_edge_count),
        str(subproblems),
        str(generated_count),
        str(pruned_beam),
        str(pruned_n_scan),
        entropy.hex(),
        effective.hex(),
        *(item.hex() for item in relative_costs),
        *(item.hex() for item in weights),
        "search-exhausted" if search_exhausted_all else "search-incomplete",
    )
    posterior = MultiScanIdentityPosterior(
        posterior_id=posterior_id,
        policy_fingerprint=multi_scan.fingerprint,
        revision=revision,
        branches=branches,
        frame_buffer=frame_buffer,
        lineage_alternatives=lineage_alternatives,
        generated_branch_count=generated_count,
        retained_branch_count=len(branches),
        pruned_by_beam_count=pruned_beam,
        pruned_by_n_scan_count=pruned_n_scan,
        candidate_edge_count=candidate_edge_count,
        assignment_subproblems_explored=subproblems,
        assignment_subproblems_limit=(
            multi_scan.maximum_assignment_subproblems_total
        ),
        assignment_search_exhausted_all_branches=search_exhausted_all,
        conditional_entropy_nats=entropy,
        effective_hypothesis_count=effective,
        omitted_hypothesis_mass_unknown=omitted_unknown,
        committed_through_revision=committed_through,
        committed_decision_count=committed_count,
        committed_decision_chain_sha256=committed_chain,
        newly_committed_step=newly_committed,
    )
    return MultiScanAdvanceResult(
        status="advanced",
        reason_codes=(),
        posterior=posterior,
        candidate_edge_count=candidate_edge_count,
        generated_branch_count=generated_count,
        assignment_subproblems_explored=subproblems,
    )


def _multi_scan_indeterminate(
    reason: str,
    *,
    candidate_edges: int = 0,
    generated: int = 0,
    subproblems: int = 0,
) -> MultiScanAdvanceResult:
    return MultiScanAdvanceResult(
        status="indeterminate",
        reason_codes=(reason,),
        posterior=None,
        candidate_edge_count=candidate_edges,
        generated_branch_count=generated,
        assignment_subproblems_explored=subproblems,
    )


def _multi_scan_assignment_problem(
    *,
    branch: MultiScanBranch,
    current: tuple[HierarchicalCellDetection, ...],
    previous_detection: dict[str, HierarchicalCellDetection],
    advected_evidence_by_pair: dict[
        tuple[str, str],
        AdvectedFootprintEvidence,
    ],
    translation_uncertainty_by_result_sha256: dict[
        str,
        SystemTranslationEvidence,
    ],
    policy: MultiHypothesisIdentityPolicy,
    ranking: MultiScanRankingWeights,
) -> tuple[
    GlobalAssignmentProblem,
    str,
    dict[tuple[str, str], str],
]:
    edge_values = tuple(
        _multi_scan_edge_cost(
            path=path,
            previous=previous_detection[path.latest_detection_id],
            current=item,
            advected=advected_evidence_by_pair[
                (path.latest_detection_id, item.detection.detection_id)
            ],
            translation_uncertainty_by_result_sha256=(
                translation_uncertainty_by_result_sha256
            ),
            policy=policy,
            ranking=ranking,
        )
        for path in branch.paths
        for item in current
    )
    edge_costs = tuple(item[0] for item in edge_values)
    edge_receipts = {
        (item[0].previous_detection_id, item[0].current_detection_id): item[1]
        for item in edge_values
    }
    current_ids = tuple(item.detection.detection_id for item in current)
    problem = GlobalAssignmentProblem(
        previous_detection_ids=tuple(item.identity_id for item in branch.paths),
        current_detection_ids=current_ids,
        edge_costs=edge_costs,
        death_costs=tuple(
            (item.identity_id, ranking.death_cost) for item in branch.paths
        ),
        birth_costs=tuple((item, ranking.birth_cost) for item in current_ids),
        policy_id="multi-scan-assignment:" + policy.fingerprint,
    )
    problem_sha256 = _sha_payload({
        "branch_id": branch.branch_id,
        "edge_receipts": [
            [identity_id, current_id, receipt]
            for (identity_id, current_id), receipt in sorted(edge_receipts.items())
        ],
        "problem": _canonical_dataclass_mapping(problem),
        "schema": "stv3-shadow-multi-scan-assignment-problem-v2",
    })
    return problem, problem_sha256, edge_receipts


def _multi_scan_edge_cost(
    *,
    path: MultiScanTrackPath,
    previous: HierarchicalCellDetection,
    current: HierarchicalCellDetection,
    advected: AdvectedFootprintEvidence,
    translation_uncertainty_by_result_sha256: dict[
        str,
        SystemTranslationEvidence,
    ],
    policy: MultiHypothesisIdentityPolicy,
    ranking: MultiScanRankingWeights,
) -> tuple[AssignmentEdgeCost, str]:
    elapsed = current.detection.valid_at - previous.detection.valid_at
    if elapsed <= 0.0:
        raise ValueError("multi-scan candidate heeft niet-positieve tijd")
    if (
        advected.previous_detection_id != previous.detection.detection_id
        or advected.current_detection_id != current.detection.detection_id
    ):
        raise ValueError("multi-scan advected evidence hoort bij andere pair")
    translation = translation_uncertainty_by_result_sha256.get(
        advected.translation_result_sha256
    )
    if translation is None or translation.evidence_id != (
        advected.translation_result_sha256
    ):
        raise ValueError("multi-scan translation uncertainty ontbreekt")
    tx = advected.applied_displacement_x_m
    ty = advected.applied_displacement_y_m
    translation_elapsed = translation.current_valid_at - translation.previous_valid_at
    uncertainty_scale = max(1.0, elapsed / translation_elapsed)
    sigma_x = min(
        policy.local_deformation_sigma_ceiling_m,
        max(
            policy.local_deformation_sigma_floor_m,
            math.sqrt(translation.covariance_m2[0][0]) * uncertainty_scale,
        ),
    )
    sigma_y = min(
        policy.local_deformation_sigma_ceiling_m,
        max(
            policy.local_deformation_sigma_floor_m,
            math.sqrt(translation.covariance_m2[1][1]) * uncertainty_scale,
        ),
    )
    local_x = current.detection.x_m - previous.detection.x_m - tx
    local_y = current.detection.y_m - previous.detection.y_m - ty
    prior_x = path.local_deformation_x_m * policy.previous_local_deformation_weight
    prior_y = path.local_deformation_y_m * policy.previous_local_deformation_weight
    local_cost = 0.5 * (
        ((local_x - prior_x) / sigma_x) ** 2
        + ((local_y - prior_y) / sigma_y) ** 2
    ) * policy.local_deformation_cost_weight
    nonoverlap = 1.0 - advected.iou
    components = (
        (
            "object_kind_change",
            ranking.object_kind_change_cost
            if previous.object_kind != current.object_kind
            else 0.0,
        ),
        (
            "absolute_log_area_ratio",
            ranking.absolute_log_area_weight
            * abs(math.log(current.area_m2 / previous.area_m2)),
        ),
        (
            "absolute_mean_value_difference",
            ranking.mean_value_difference_weight
            * abs(current.mean_value - previous.mean_value),
        ),
        (
            "absolute_maximum_value_difference",
            ranking.maximum_value_difference_weight
            * abs(current.maximum_value - previous.maximum_value),
        ),
        (
            "advected_footprint_nonoverlap",
            ranking.footprint_nonoverlap_weight * nonoverlap,
        ),
        ("local_deformation_given_system_translation", local_cost),
    )
    cost = math.fsum(value for _name, value in components)
    if not math.isfinite(cost):
        raise ValueError("multi-scan edge cost overschrijdt numeriek bereik")
    edge = AssignmentEdgeCost(
        previous_detection_id=path.identity_id,
        current_detection_id=current.detection.detection_id,
        cost=cost,
        cost_model_id="multi-scan-local-deformation:" + policy.fingerprint,
        components=components,
    )
    receipt_sha256 = _sha_payload({
        "advected_footprint_evidence_sha256": advected.evidence_sha256,
        "current_detection": _canonical_dataclass_mapping(current),
        "edge": _canonical_dataclass_mapping(edge),
        "local_deformation": {
            "local_x_m": local_x.hex(),
            "local_y_m": local_y.hex(),
            "prior_x_m": prior_x.hex(),
            "prior_y_m": prior_y.hex(),
            "sigma_x_m": sigma_x.hex(),
            "sigma_y_m": sigma_y.hex(),
        },
        "schema": "stv3-shadow-multi-scan-association-receipt-v2",
        "translation_uncertainty": _canonical_dataclass_mapping(translation),
        "previous_detection": _canonical_dataclass_mapping(previous),
    })
    return edge, receipt_sha256


def _multi_scan_apply_solution(
    *,
    engine_id: str,
    source: MultiScanBranch,
    solution,
    revision: int,
    current_by_id: dict[str, HierarchicalCellDetection],
    previous_detection: dict[str, HierarchicalCellDetection],
    advected_evidence_by_pair: dict[
        tuple[str, str],
        AdvectedFootprintEvidence,
    ],
    edge_receipt_by_identity_current: dict[tuple[str, str], str],
    assignment_problem_sha256: str,
    policy: MultiHypothesisIdentityPolicy,
    maximum_gap_frames: int,
    lineage_alternative_ids: tuple[str, ...],
) -> tuple[tuple[MultiScanTrackPath, ...], MultiScanBranchStep]:
    path_by_id = {item.identity_id: item for item in source.paths}
    paths: list[MultiScanTrackPath] = []
    matches = []
    matched_receipts: list[str] = []
    matched_advected: list[str] = []
    for identity_id, current_id in solution.matches:
        path = path_by_id[identity_id]
        old = previous_detection[path.latest_detection_id]
        new = current_by_id[current_id]
        advected = advected_evidence_by_pair[
            (path.latest_detection_id, current_id)
        ]
        local_x = (
            new.detection.x_m
            - old.detection.x_m
            - advected.applied_displacement_x_m
        )
        local_y = (
            new.detection.y_m
            - old.detection.y_m
            - advected.applied_displacement_y_m
        )
        detection_ids = (*path.detection_ids, current_id)[
            -(policy.multi_scan.horizon_frames + 1):
        ]
        paths.append(MultiScanTrackPath(
            identity_id=identity_id,
            latest_detection_id=current_id,
            missed_frames=0,
            detection_ids=detection_ids,
            local_deformation_x_m=local_x,
            local_deformation_y_m=local_y,
            parent_identity_ids=path.parent_identity_ids,
            lineage_event_ids=path.lineage_event_ids,
        ))
        matches.append((identity_id, path.latest_detection_id, current_id))
        matched_receipts.append(
            edge_receipt_by_identity_current[(identity_id, current_id)]
        )
        matched_advected.append(advected.evidence_sha256)
    gaps = []
    for identity_id in solution.deaths:
        path = path_by_id[identity_id]
        missed = path.missed_frames + 1
        gaps.append(identity_id)
        if missed <= maximum_gap_frames:
            paths.append(MultiScanTrackPath(
                identity_id=path.identity_id,
                latest_detection_id=path.latest_detection_id,
                missed_frames=missed,
                detection_ids=path.detection_ids,
                local_deformation_x_m=path.local_deformation_x_m,
                local_deformation_y_m=path.local_deformation_y_m,
                parent_identity_ids=path.parent_identity_ids,
                lineage_event_ids=path.lineage_event_ids,
            ))
    births = []
    for current_id in solution.births:
        identity_id = _multi_scan_birth_identity(engine_id, current_id)
        births.append((identity_id, current_id))
        paths.append(MultiScanTrackPath(
            identity_id=identity_id,
            latest_detection_id=current_id,
            missed_frames=0,
            detection_ids=(current_id,),
            local_deformation_x_m=0.0,
            local_deformation_y_m=0.0,
        ))
    canonical_matches = tuple(sorted(matches))
    canonical_gaps = tuple(sorted(gaps))
    canonical_births = tuple(sorted(births))
    canonical_receipts = tuple(sorted(matched_receipts))
    canonical_advected = tuple(sorted(matched_advected))
    assignment_solution_sha256 = _sha_payload({
        "assignment_problem_sha256": assignment_problem_sha256,
        "births": [list(item) for item in canonical_births],
        "deaths": list(solution.deaths),
        "incremental_cost": solution.total_cost.hex(),
        "matched_association_evidence_sha256": list(canonical_receipts),
        "matched_advected_footprint_evidence_sha256": list(canonical_advected),
        "matches": [list(item) for item in canonical_matches],
        "rank": solution.rank,
        "schema": "stv3-shadow-multi-scan-assignment-solution-v2",
    })
    decision_id = "multi-scan-decision:" + _digest(
        source.branch_id,
        str(revision),
        str(solution.rank),
        solution.total_cost.hex(),
        assignment_problem_sha256,
        assignment_solution_sha256,
        *canonical_receipts,
        *canonical_advected,
        *(f"match:{a}>{b}>{c}" for a, b, c in canonical_matches),
        *(f"gap:{item}" for item in canonical_gaps),
        *(f"birth:{a}>{b}" for a, b in canonical_births),
        *lineage_alternative_ids,
    )
    step = MultiScanBranchStep(
        decision_id=decision_id,
        revision=revision,
        source_branch_id=source.branch_id,
        assignment_solution_rank=solution.rank,
        incremental_cost=solution.total_cost,
        assignment_problem_sha256=assignment_problem_sha256,
        assignment_solution_sha256=assignment_solution_sha256,
        matched_association_evidence_sha256=canonical_receipts,
        matched_advected_footprint_evidence_sha256=canonical_advected,
        matches=canonical_matches,
        gap_identity_ids=canonical_gaps,
        births=canonical_births,
        lineage_alternative_ids=lineage_alternative_ids,
    )
    return tuple(sorted(paths, key=lambda item: item.identity_id)), step


def _multi_scan_lineage_alternatives(
    *,
    branch: MultiScanBranch,
    revision: int,
    previous_detection: dict[str, HierarchicalCellDetection],
    current: tuple[HierarchicalCellDetection, ...],
    advected_evidence_by_pair: dict[
        tuple[str, str],
        AdvectedFootprintEvidence,
    ],
) -> tuple[MultiScanLineageAlternative, ...]:
    eligible = tuple(item for item in branch.paths if item.missed_frames == 0)
    if not eligible or not current:
        return ()
    overlaps: dict[tuple[str, str], float] = {}
    evidence_sha: dict[tuple[str, str], str] = {}
    for path in eligible:
        old = previous_detection[path.latest_detection_id]
        for new in current:
            if old.object_kind != new.object_kind:
                continue
            evidence = advected_evidence_by_pair[
                (path.latest_detection_id, new.detection.detection_id)
            ]
            iou = evidence.iou
            if iou > 0.0:
                overlaps[(path.identity_id, new.detection.detection_id)] = iou
                evidence_sha[(path.identity_id, new.detection.detection_id)] = (
                    evidence.evidence_sha256
                )
    split_groups = tuple(
        (
            path.identity_id,
            tuple(sorted(
                new.detection.detection_id
                for new in current
                if (path.identity_id, new.detection.detection_id) in overlaps
            )),
        )
        for path in eligible
    )
    split_groups = tuple(item for item in split_groups if len(item[1]) >= 2)
    merge_groups = tuple(
        (
            tuple(sorted(
                path.identity_id
                for path in eligible
                if (path.identity_id, new.detection.detection_id) in overlaps
            )),
            new.detection.detection_id,
        )
        for new in current
    )
    merge_groups = tuple(item for item in merge_groups if len(item[0]) >= 2)
    result = []
    for parent, children in split_groups:
        cost = math.fsum(1.0 - overlaps[(parent, child)] for child in children)
        evidence_ids = tuple(sorted(
            evidence_sha[(parent, child)] for child in children
        ))
        alternative_id = "multi-scan-lineage:" + _digest(
            "split",
            branch.branch_id,
            str(revision),
            parent,
            *children,
            *evidence_ids,
            cost.hex(),
        )
        result.append(MultiScanLineageAlternative(
            alternative_id=alternative_id,
            source_branch_id=branch.branch_id,
            revision=revision,
            event_type="split",
            parent_identity_ids=(parent,),
            child_detection_ids=children,
            cost=cost,
            provenance_ids=tuple(sorted((branch.branch_id, *evidence_ids))),
        ))
    for parents, child in merge_groups:
        cost = math.fsum(1.0 - overlaps[(parent, child)] for parent in parents)
        evidence_ids = tuple(sorted(
            evidence_sha[(parent, child)] for parent in parents
        ))
        alternative_id = "multi-scan-lineage:" + _digest(
            "merge",
            branch.branch_id,
            str(revision),
            *parents,
            child,
            *evidence_ids,
            cost.hex(),
        )
        result.append(MultiScanLineageAlternative(
            alternative_id=alternative_id,
            source_branch_id=branch.branch_id,
            revision=revision,
            event_type="merge",
            parent_identity_ids=parents,
            child_detection_ids=(child,),
            cost=cost,
            provenance_ids=tuple(sorted((branch.branch_id, *evidence_ids))),
        ))
    return tuple(sorted(result, key=lambda item: item.alternative_id))


def _multi_scan_birth_identity(engine_id: str, detection_id: str) -> str:
    return "multi-scan-identity:" + _digest(
        engine_id,
        "birth",
        detection_id,
    )


def _translation_support(
    values: Iterable[HierarchicalCellDetection],
) -> tuple[HierarchicalCellDetection, ...]:
    detections = tuple(values)
    shields = tuple(item for item in detections if item.object_kind == "rain_shield")
    return tuple(sorted(
        shields or detections,
        key=lambda item: item.detection.detection_id,
    ))


def _weighted_centroid(
    values: tuple[HierarchicalCellDetection, ...],
) -> tuple[float, float, float, float, float]:
    weights = tuple(item.area_m2 for item in values)
    total = math.fsum(weights)
    x = math.fsum(
        weight * item.detection.x_m for weight, item in zip(weights, values, strict=True)
    ) / total
    y = math.fsum(
        weight * item.detection.y_m for weight, item in zip(weights, values, strict=True)
    ) / total
    effective = total**2 / math.fsum(weight**2 for weight in weights)
    variance_x = math.fsum(
        weight
        * (
            (item.detection.x_m - x) ** 2
            + item.detection.position_sigma_m**2
        )
        for weight, item in zip(weights, values, strict=True)
    ) / total / max(1.0, effective)
    variance_y = math.fsum(
        weight
        * (
            (item.detection.y_m - y) ** 2
            + item.detection.position_sigma_m**2
        )
        for weight, item in zip(weights, values, strict=True)
    ) / total / max(1.0, effective)
    return x, y, variance_x, variance_y, total


def _covariance2(value: tuple[tuple[float, float], tuple[float, float]]) -> None:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or any(not isinstance(row, tuple) or len(row) != 2 for row in value)
        or any(not math.isfinite(item) for row in value for item in row)
        or not math.isclose(value[0][1], value[1][0], rel_tol=1e-12, abs_tol=1e-12)
        or value[0][0] <= 0.0
        or value[1][1] <= 0.0
        or value[0][0] * value[1][1] - value[0][1] * value[1][0] <= 0.0
    ):
        raise ValueError("translation covariance moet positief definiet 2x2 zijn")


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


def _positive_integer(value: object, label: str, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= maximum
    ):
        raise ValueError(f"{label} moet een begrensd positief geheel getal zijn")
    return value


def _positive_finite(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0.0
    ):
        raise ValueError(f"{label} moet positief en eindig zijn")
    return float(value)


def _digest(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


def _canonical_dataclass_mapping(value: Any) -> Any:
    """Return a type-stable JSON value with exact binary-float spelling."""

    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _canonical_dataclass_mapping(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, tuple):
        return [_canonical_dataclass_mapping(item) for item in value]
    if isinstance(value, list):
        return [_canonical_dataclass_mapping(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _canonical_dataclass_mapping(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical association evidence bevat niet-eindige float")
        return value.hex()
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"niet-canoniek association-evidencetype: {type(value)!r}")


def _sha_payload(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        _canonical_dataclass_mapping(value),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")).hexdigest()


def _assignment_edge_cost_sha256(
    edge: AssignmentEdgeCost,
    *,
    association_evidence_sha256: str,
) -> str:
    _sha256(association_evidence_sha256, "association evidence")
    return _sha_payload({
        "association_evidence_sha256": association_evidence_sha256,
        "edge": _canonical_dataclass_mapping(edge),
        "schema": "stv3-shadow-association-cost-receipt-v1",
    })


__all__ = [
    "EMPTY_MULTI_SCAN_COMMIT_SHA256",
    "IdentityAssignmentHypothesis",
    "LocalCellDeformationCandidate",
    "MULTI_HYPOTHESIS_IDENTITY_VERSION",
    "MULTI_SCAN_IDENTITY_VERSION",
    "MultiScanAdvanceResult",
    "MultiScanBranch",
    "MultiScanBranchStep",
    "MultiScanIdentityPolicy",
    "MultiScanIdentityPosterior",
    "MultiScanLineageAlternative",
    "MultiScanRankingWeights",
    "MultiScanTrackPath",
    "MultiHypothesisIdentityPolicy",
    "MultiHypothesisIdentitySnapshot",
    "SystemTranslationEvidence",
    "advance_multi_scan_identity",
    "bootstrap_multi_scan_identity",
    "bind_local_candidate_costs",
    "build_local_deformation_candidates",
    "estimate_system_translation",
    "finalize_identity_snapshot",
]
