"""Replayable, shadow-only provider handoff evidence and continuity state.

A provider, product, grid or geolocation change is a measurement
discontinuity.  This module therefore never derives velocity from coordinates
on opposite sides of a handoff.  It maps a predicted source-grid position into
the target grid, propagates every supplied covariance and ranks all retained
association candidates by their registered position innovation.

The caller owns every scientific threshold and uncertainty in
``ProviderHandoffPolicy``.  Weak evidence becomes provisional or
indeterminate; no detection, candidate or weather observation is rejected.
Nothing in this module is wired into the live selection or tracking path.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from enum import Enum
from typing import Iterable


PROVIDER_HANDOFF_ALGORITHM_VERSION = "stv3-provider-handoff-shadow-v1"
PROVIDER_HANDOFF_POLICY_SCHEMA_VERSION = "stv3-provider-handoff-policy-v1"
Matrix2 = tuple[tuple[float, float], tuple[float, float]]
Affine2 = tuple[
    tuple[float, float, float],
    tuple[float, float, float],
]


class HandoffContinuityState(str, Enum):
    """Identity interpretation supported by the retained handoff evidence."""

    CONFIRMED = "confirmed"
    PROVISIONAL = "provisional"
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True, slots=True)
class HandoffFrameReference:
    """Immutable identity and validity of one handoff-side radar frame."""

    frame_id: str
    provider_id: str
    product_id: str
    analysis_grid_id: str
    geolocation_id: str
    valid_at: float
    validity_start: float
    validity_end: float
    input_artifact_sha256: str
    parser_version: str

    def __post_init__(self) -> None:
        for label, value in (
            ("frame_id", self.frame_id),
            ("provider_id", self.provider_id),
            ("product_id", self.product_id),
            ("analysis_grid_id", self.analysis_grid_id),
            ("geolocation_id", self.geolocation_id),
            ("parser_version", self.parser_version),
        ):
            _identifier(value, label)
        _sha256(self.input_artifact_sha256, "input_artifact_sha256")
        valid_at = _finite(self.valid_at, "valid_at")
        start = _finite(self.validity_start, "validity_start")
        end = _finite(self.validity_end, "validity_end")
        if end < start:
            raise ValueError("frame validity_end ligt voor validity_start")
        if not start <= valid_at <= end:
            raise ValueError("frame valid_at ligt buiten zijn geldigheidsinterval")


@dataclass(frozen=True, slots=True)
class HandoffTransformEvidence:
    """Affine source-to-target registration and its metric uncertainty.

    ``affine_target_from_source`` maps ``[x_source, y_source, 1]`` to target
    metres.  A missing matrix is retained as unavailable evidence, rather than
    replaced by a guessed identity transform.
    """

    transform_id: str
    source_frame_id: str
    target_frame_id: str
    source_grid_id: str
    target_grid_id: str
    valid_from: float
    valid_to: float
    method_id: str
    affine_target_from_source: Affine2 | None
    uncertainty_covariance_m2: Matrix2 | None
    registration_residual_m: float | None
    support_count: int
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        for label, value in (
            ("transform_id", self.transform_id),
            ("source_frame_id", self.source_frame_id),
            ("target_frame_id", self.target_frame_id),
            ("source_grid_id", self.source_grid_id),
            ("target_grid_id", self.target_grid_id),
            ("method_id", self.method_id),
        ):
            _identifier(value, label)
        start = _finite(self.valid_from, "valid_from")
        end = _finite(self.valid_to, "valid_to")
        if end < start:
            raise ValueError("transform valid_to ligt voor valid_from")
        _nonnegative_int(self.support_count, "support_count")
        object.__setattr__(
            self,
            "reason_codes",
            _canonical_identifiers(self.reason_codes, "transform reason_codes"),
        )
        available = self.affine_target_from_source is not None
        if available:
            _affine(self.affine_target_from_source)
            if self.uncertainty_covariance_m2 is None:
                raise ValueError(
                    "beschikbare transform vereist uncertainty_covariance_m2"
                )
            _symmetric_psd_matrix(
                self.uncertainty_covariance_m2,
                "transform uncertainty_covariance_m2",
            )
            if self.registration_residual_m is None:
                raise ValueError(
                    "beschikbare transform vereist registration_residual_m"
                )
            _nonnegative_finite(
                self.registration_residual_m,
                "registration_residual_m",
            )
            if self.support_count <= 0:
                raise ValueError(
                    "beschikbare transform vereist positieve support_count"
                )
        elif (
            self.uncertainty_covariance_m2 is not None
            or self.registration_residual_m is not None
        ):
            raise ValueError(
                "onbeschikbare transform mag geen matrixonzekerheid of "
                "registratieresidu claimen"
            )

    @property
    def available(self) -> bool:
        return self.affine_target_from_source is not None


@dataclass(frozen=True, slots=True)
class HandoffOverlapEvidence:
    """Measured overlap summaries retained with exact source provenance."""

    evidence_id: str
    source_frame_ids: tuple[str, ...]
    target_frame_ids: tuple[str, ...]
    window_valid_from: float
    window_valid_to: float
    paired_frame_count: int
    footprint_overlap_fraction: float | None
    valid_data_overlap_fraction: float | None
    missing_mask_agreement: float | None
    intensity_quantile_distance: float | None
    phase_correlation_peak_ratio: float | None
    support_artifact_sha256s: tuple[str, ...]
    method_id: str
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.evidence_id, "evidence_id")
        _identifier(self.method_id, "method_id")
        object.__setattr__(
            self,
            "source_frame_ids",
            _canonical_identifiers(
                self.source_frame_ids,
                "overlap source_frame_ids",
                allow_empty=False,
            ),
        )
        object.__setattr__(
            self,
            "target_frame_ids",
            _canonical_identifiers(
                self.target_frame_ids,
                "overlap target_frame_ids",
                allow_empty=False,
            ),
        )
        start = _finite(self.window_valid_from, "window_valid_from")
        end = _finite(self.window_valid_to, "window_valid_to")
        if end < start:
            raise ValueError("overlap window_valid_to ligt voor window_valid_from")
        _nonnegative_int(self.paired_frame_count, "paired_frame_count")
        for label, value in (
            ("footprint_overlap_fraction", self.footprint_overlap_fraction),
            ("valid_data_overlap_fraction", self.valid_data_overlap_fraction),
            ("missing_mask_agreement", self.missing_mask_agreement),
        ):
            _optional_unit_interval(value, label)
        if self.intensity_quantile_distance is not None:
            _nonnegative_finite(
                self.intensity_quantile_distance,
                "intensity_quantile_distance",
            )
        if self.phase_correlation_peak_ratio is not None:
            _nonnegative_finite(
                self.phase_correlation_peak_ratio,
                "phase_correlation_peak_ratio",
            )
        if not isinstance(self.support_artifact_sha256s, tuple):
            raise TypeError("support_artifact_sha256s moet een tuple zijn")
        artifacts = tuple(sorted(self.support_artifact_sha256s))
        if len(artifacts) != len(set(artifacts)):
            raise ValueError("support_artifact_sha256s bevat duplicaten")
        for value in artifacts:
            _sha256(value, "support_artifact_sha256")
        object.__setattr__(self, "support_artifact_sha256s", artifacts)
        object.__setattr__(
            self,
            "reason_codes",
            _canonical_identifiers(self.reason_codes, "overlap reason_codes"),
        )


@dataclass(frozen=True, slots=True)
class HandoffAssociationEvidence:
    """One retained source-track to target-detection continuity hypothesis."""

    candidate_id: str
    track_id: str
    source_frame_id: str
    target_frame_id: str
    source_detection_id: str
    target_detection_id: str
    source_track_revision: int
    prediction_valid_at: float
    source_centroid_x_m: float
    source_centroid_y_m: float
    predicted_source_x_m: float
    predicted_source_y_m: float
    target_detection_x_m: float
    target_detection_y_m: float
    predicted_position_covariance_m2: Matrix2
    target_position_covariance_m2: Matrix2
    advected_polygon_iou: float | None
    prior_association_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        for label, value in (
            ("candidate_id", self.candidate_id),
            ("track_id", self.track_id),
            ("source_frame_id", self.source_frame_id),
            ("target_frame_id", self.target_frame_id),
            ("source_detection_id", self.source_detection_id),
            ("target_detection_id", self.target_detection_id),
        ):
            _identifier(value, label)
        _positive_int(self.source_track_revision, "source_track_revision")
        _finite(self.prediction_valid_at, "prediction_valid_at")
        for label, value in (
            ("source_centroid_x_m", self.source_centroid_x_m),
            ("source_centroid_y_m", self.source_centroid_y_m),
            ("predicted_source_x_m", self.predicted_source_x_m),
            ("predicted_source_y_m", self.predicted_source_y_m),
            ("target_detection_x_m", self.target_detection_x_m),
            ("target_detection_y_m", self.target_detection_y_m),
        ):
            _finite(value, label)
        _symmetric_pd_matrix(
            self.predicted_position_covariance_m2,
            "predicted_position_covariance_m2",
        )
        _symmetric_pd_matrix(
            self.target_position_covariance_m2,
            "target_position_covariance_m2",
        )
        _optional_unit_interval(self.advected_polygon_iou, "advected_polygon_iou")
        object.__setattr__(
            self,
            "prior_association_ids",
            _canonical_identifiers(
                self.prior_association_ids,
                "prior_association_ids",
            ),
        )
        object.__setattr__(
            self,
            "reason_codes",
            _canonical_identifiers(self.reason_codes, "candidate reason_codes"),
        )


@dataclass(frozen=True, slots=True)
class ProviderHandoffPolicy:
    """Caller-owned scientific policy; this class supplies no thresholds."""

    policy_id: str
    minimum_paired_frames_for_confirmation: int
    minimum_transform_support_count: int
    minimum_footprint_overlap_fraction: float
    minimum_valid_data_overlap_fraction: float
    minimum_missing_mask_agreement: float
    maximum_intensity_quantile_distance: float
    minimum_phase_correlation_peak_ratio: float
    maximum_registration_residual_m: float
    maximum_standardized_position_residual: float
    minimum_advected_polygon_iou: float
    minimum_nis_margin_for_confirmation: float
    minimum_prior_association_count: int
    product_difference_covariance_m2: Matrix2
    blocking_evidence_reason_codes: tuple[str, ...]
    policy_schema_version: str = PROVIDER_HANDOFF_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _identifier(self.policy_id, "policy_id")
        if self.policy_schema_version != PROVIDER_HANDOFF_POLICY_SCHEMA_VERSION:
            raise ValueError("onbekende provider-handoff policyversie")
        _positive_int(
            self.minimum_paired_frames_for_confirmation,
            "minimum_paired_frames_for_confirmation",
        )
        _positive_int(
            self.minimum_transform_support_count,
            "minimum_transform_support_count",
        )
        for label, value in (
            (
                "minimum_footprint_overlap_fraction",
                self.minimum_footprint_overlap_fraction,
            ),
            (
                "minimum_valid_data_overlap_fraction",
                self.minimum_valid_data_overlap_fraction,
            ),
            ("minimum_missing_mask_agreement", self.minimum_missing_mask_agreement),
            ("minimum_advected_polygon_iou", self.minimum_advected_polygon_iou),
        ):
            _unit_interval(value, label)
        for label, value in (
            (
                "maximum_intensity_quantile_distance",
                self.maximum_intensity_quantile_distance,
            ),
            (
                "minimum_phase_correlation_peak_ratio",
                self.minimum_phase_correlation_peak_ratio,
            ),
            (
                "maximum_registration_residual_m",
                self.maximum_registration_residual_m,
            ),
            (
                "maximum_standardized_position_residual",
                self.maximum_standardized_position_residual,
            ),
            (
                "minimum_nis_margin_for_confirmation",
                self.minimum_nis_margin_for_confirmation,
            ),
        ):
            _nonnegative_finite(value, label)
        _nonnegative_int(
            self.minimum_prior_association_count,
            "minimum_prior_association_count",
        )
        _symmetric_psd_matrix(
            self.product_difference_covariance_m2,
            "product_difference_covariance_m2",
        )
        object.__setattr__(
            self,
            "blocking_evidence_reason_codes",
            _canonical_identifiers(
                self.blocking_evidence_reason_codes,
                "blocking_evidence_reason_codes",
            ),
        )


@dataclass(frozen=True, slots=True)
class HandoffAssociationAssessment:
    """Registered position evidence; deliberately contains no velocity."""

    candidate_id: str
    track_id: str
    source_detection_id: str
    target_detection_id: str
    rank_within_track: int | None
    evidence_state: str
    raw_coordinate_displacement_m_audit_only: float
    registered_prediction_x_m: float | None
    registered_prediction_y_m: float | None
    registered_residual_x_m: float | None
    registered_residual_y_m: float | None
    registered_residual_m: float | None
    measurement_covariance_m2: Matrix2 | None
    innovation_covariance_m2: Matrix2 | None
    normalized_innovation_squared: float | None
    standardized_position_residual: float | None
    advected_polygon_iou: float | None
    prior_association_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        for label, value in (
            ("candidate_id", self.candidate_id),
            ("track_id", self.track_id),
            ("source_detection_id", self.source_detection_id),
            ("target_detection_id", self.target_detection_id),
            ("evidence_state", self.evidence_state),
        ):
            _identifier(value, label)
        if self.rank_within_track is not None:
            _positive_int(self.rank_within_track, "rank_within_track")
        _nonnegative_finite(
            self.raw_coordinate_displacement_m_audit_only,
            "raw_coordinate_displacement_m_audit_only",
        )
        object.__setattr__(
            self,
            "prior_association_ids",
            _canonical_identifiers(
                self.prior_association_ids,
                "assessment prior_association_ids",
            ),
        )
        object.__setattr__(
            self,
            "reason_codes",
            _canonical_identifiers(
                self.reason_codes,
                "assessment reason_codes",
            ),
        )
        registered_fields = (
            self.registered_prediction_x_m,
            self.registered_prediction_y_m,
            self.registered_residual_x_m,
            self.registered_residual_y_m,
            self.registered_residual_m,
            self.measurement_covariance_m2,
            self.innovation_covariance_m2,
            self.normalized_innovation_squared,
            self.standardized_position_residual,
        )
        if self.evidence_state == "registered_position_evidence":
            if any(value is None for value in registered_fields):
                raise ValueError(
                    "geregistreerde assessment mist position evidence"
                )
            for label, value in (
                ("registered_prediction_x_m", self.registered_prediction_x_m),
                ("registered_prediction_y_m", self.registered_prediction_y_m),
                ("registered_residual_x_m", self.registered_residual_x_m),
                ("registered_residual_y_m", self.registered_residual_y_m),
            ):
                _finite(value, label)
            for label, value in (
                ("registered_residual_m", self.registered_residual_m),
                (
                    "normalized_innovation_squared",
                    self.normalized_innovation_squared,
                ),
                (
                    "standardized_position_residual",
                    self.standardized_position_residual,
                ),
            ):
                _nonnegative_finite(value, label)
            _symmetric_pd_matrix(
                self.measurement_covariance_m2,
                "assessment measurement_covariance_m2",
            )
            _symmetric_pd_matrix(
                self.innovation_covariance_m2,
                "assessment innovation_covariance_m2",
            )
        elif self.evidence_state == "indeterminate_transform":
            if any(value is not None for value in registered_fields):
                raise ValueError(
                    "indeterminate assessment mag geen geregistreerde positie claimen"
                )
        else:
            raise ValueError("ongeldige assessment evidence_state")
        _optional_unit_interval(self.advected_polygon_iou, "advected_polygon_iou")


@dataclass(frozen=True, slots=True)
class HandoffTrackContinuity:
    """Shadow identity state for one pre-existing track at the handoff."""

    track_id: str
    source_detection_id: str
    source_track_revision: int
    continuity_state: HandoffContinuityState
    leading_candidate_id: str | None
    alternative_candidate_ids: tuple[str, ...]
    association_margin_nis: float | None
    measurement_covariance_m2: Matrix2 | None
    prior_association_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]
    motion_update_mode: str
    allow_cross_handoff_velocity_update: bool = False

    def __post_init__(self) -> None:
        _identifier(self.track_id, "track_id")
        _identifier(self.source_detection_id, "source_detection_id")
        _positive_int(self.source_track_revision, "source_track_revision")
        if not isinstance(self.continuity_state, HandoffContinuityState):
            raise TypeError("continuity_state heeft een ongeldig type")
        if self.leading_candidate_id is not None:
            _identifier(self.leading_candidate_id, "leading_candidate_id")
        if not isinstance(self.alternative_candidate_ids, tuple):
            raise TypeError("alternative_candidate_ids moet een tuple zijn")
        for value in self.alternative_candidate_ids:
            _identifier(value, "alternative_candidate_id")
        if len(self.alternative_candidate_ids) != len(
            set(self.alternative_candidate_ids)
        ):
            raise ValueError("alternative_candidate_ids bevat duplicaten")
        if self.leading_candidate_id in self.alternative_candidate_ids:
            raise ValueError("leidende kandidaat staat ook bij alternatieven")
        if self.association_margin_nis is not None:
            _nonnegative_finite(
                self.association_margin_nis,
                "association_margin_nis",
            )
        if self.measurement_covariance_m2 is not None:
            _symmetric_pd_matrix(
                self.measurement_covariance_m2,
                "continuity measurement_covariance_m2",
            )
        object.__setattr__(
            self,
            "prior_association_ids",
            _canonical_identifiers(
                self.prior_association_ids,
                "continuity prior_association_ids",
            ),
        )
        object.__setattr__(
            self,
            "reason_codes",
            _canonical_identifiers(
                self.reason_codes,
                "continuity reason_codes",
            ),
        )
        if self.allow_cross_handoff_velocity_update:
            raise ValueError("cross-handoff velocity-update is verboden")
        if self.motion_update_mode not in (
            "registered_position_only",
            "no_motion_update",
        ):
            raise ValueError("ongeldige handoff motion_update_mode")
        if self.motion_update_mode == "registered_position_only" and (
            self.leading_candidate_id is None
            or self.measurement_covariance_m2 is None
        ):
            raise ValueError(
                "registered_position_only vereist kandidaat en covariance"
            )
        if self.motion_update_mode == "no_motion_update" and (
            self.leading_candidate_id is not None
            or self.measurement_covariance_m2 is not None
        ):
            raise ValueError("no_motion_update mag geen positieleider claimen")
        if (
            self.continuity_state is HandoffContinuityState.CONFIRMED
            and self.leading_candidate_id is None
        ):
            raise ValueError("confirmed continuity vereist een leidende kandidaat")
        if (
            self.continuity_state is HandoffContinuityState.INDETERMINATE
            and self.motion_update_mode != "no_motion_update"
        ):
            raise ValueError("indeterminate continuity mag motion niet updaten")


@dataclass(frozen=True, slots=True)
class ProviderHandoffEvent:
    """Content-addressed decision record containing all replay inputs."""

    event_id: str
    valid_at: float
    source_frame: HandoffFrameReference
    target_frame: HandoffFrameReference
    transform_evidence: HandoffTransformEvidence
    overlap_evidence: HandoffOverlapEvidence
    policy: ProviderHandoffPolicy
    candidate_inputs: tuple[HandoffAssociationEvidence, ...]
    candidate_assessments: tuple[HandoffAssociationAssessment, ...]
    track_continuities: tuple[HandoffTrackContinuity, ...]
    continuity_state: HandoffContinuityState
    reason_codes: tuple[str, ...]
    evidence_digest_sha256: str
    shadow_only: bool = True
    allow_cross_handoff_velocity_update: bool = False
    algorithm_version: str = PROVIDER_HANDOFF_ALGORITHM_VERSION

    def __post_init__(self) -> None:
        if self.algorithm_version != PROVIDER_HANDOFF_ALGORITHM_VERSION:
            raise ValueError("onbekende provider-handoff algoritmeversie")
        if not self.shadow_only:
            raise ValueError("provider-handoff v1 is uitsluitend shadow-only")
        if self.allow_cross_handoff_velocity_update:
            raise ValueError("cross-handoff velocity-update is verboden")
        for label, values, expected_type in (
            (
                "candidate_inputs",
                self.candidate_inputs,
                HandoffAssociationEvidence,
            ),
            (
                "candidate_assessments",
                self.candidate_assessments,
                HandoffAssociationAssessment,
            ),
            (
                "track_continuities",
                self.track_continuities,
                HandoffTrackContinuity,
            ),
        ):
            if not isinstance(values, tuple):
                raise TypeError(f"{label} moet een immutable tuple zijn")
            if any(not isinstance(item, expected_type) for item in values):
                raise TypeError(f"{label} bevat een ongeldig recordtype")
        if not isinstance(self.continuity_state, HandoffContinuityState):
            raise TypeError("event continuity_state heeft een ongeldig type")
        if not isinstance(self.reason_codes, tuple):
            raise TypeError("event reason_codes moet een tuple zijn")
        _finite(self.valid_at, "event valid_at")
        if self.valid_at != self.target_frame.valid_at:
            raise ValueError("handoff event valid_at verschilt van targetframe")
        _sha256(self.evidence_digest_sha256, "evidence_digest_sha256")
        expected_digest = _input_digest(
            self.source_frame,
            self.target_frame,
            self.transform_evidence,
            self.overlap_evidence,
            self.candidate_inputs,
            self.policy,
        )
        if self.evidence_digest_sha256 != expected_digest:
            raise ValueError("handoff evidence digest klopt niet")
        if self.event_id != f"provider-handoff:{expected_digest}":
            raise ValueError("handoff event_id is niet content-addressed")
        if self.candidate_inputs != tuple(
            sorted(self.candidate_inputs, key=lambda item: item.candidate_id)
        ):
            raise ValueError("candidate_inputs zijn niet canoniek gesorteerd")
        input_ids = tuple(item.candidate_id for item in self.candidate_inputs)
        assessment_ids = tuple(
            item.candidate_id for item in self.candidate_assessments
        )
        if (
            len(input_ids) != len(set(input_ids))
            or set(input_ids) != set(assessment_ids)
            or len(assessment_ids) != len(set(assessment_ids))
        ):
            raise ValueError("handoff assessments dekken candidate_inputs niet exact")
        track_ids = tuple(item.track_id for item in self.track_continuities)
        if track_ids != tuple(sorted(track_ids)) or len(track_ids) != len(
            set(track_ids)
        ):
            raise ValueError("track_continuities zijn niet canoniek")
        expected_track_ids = tuple(
            sorted({item.track_id for item in self.candidate_inputs})
        )
        if track_ids != expected_track_ids:
            raise ValueError("track_continuities dekken inputtracks niet exact")
        expected_assessment_order = tuple(
            sorted(
                self.candidate_assessments,
                key=lambda item: (
                    item.track_id,
                    item.rank_within_track
                    if item.rank_within_track is not None
                    else math.inf,
                    item.candidate_id,
                ),
            )
        )
        if self.candidate_assessments != expected_assessment_order:
            raise ValueError("candidate_assessments zijn niet canoniek")
        if not self.track_continuities:
            expected_state = HandoffContinuityState.INDETERMINATE
        elif all(
            item.continuity_state is HandoffContinuityState.CONFIRMED
            for item in self.track_continuities
        ):
            expected_state = HandoffContinuityState.CONFIRMED
        elif all(
            item.continuity_state is HandoffContinuityState.INDETERMINATE
            for item in self.track_continuities
        ):
            expected_state = HandoffContinuityState.INDETERMINATE
        else:
            expected_state = HandoffContinuityState.PROVISIONAL
        if self.continuity_state is not expected_state:
            raise ValueError("event continuity_state past niet bij de tracks")
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise ValueError("event reason_codes zijn niet canoniek")


def build_provider_handoff_event(
    source_frame: HandoffFrameReference,
    target_frame: HandoffFrameReference,
    transform_evidence: HandoffTransformEvidence,
    overlap_evidence: HandoffOverlapEvidence,
    candidates: Iterable[HandoffAssociationEvidence],
    *,
    policy: ProviderHandoffPolicy,
) -> ProviderHandoffEvent:
    """Build one deterministic handoff event without deleting hypotheses."""

    for value, expected, label in (
        (source_frame, HandoffFrameReference, "source_frame"),
        (target_frame, HandoffFrameReference, "target_frame"),
        (
            transform_evidence,
            HandoffTransformEvidence,
            "transform_evidence",
        ),
        (overlap_evidence, HandoffOverlapEvidence, "overlap_evidence"),
        (policy, ProviderHandoffPolicy, "policy"),
    ):
        if not isinstance(value, expected):
            raise TypeError(f"{label} heeft een ongeldig type")
    canonical_candidates = tuple(
        sorted(tuple(candidates), key=lambda item: item.candidate_id)
    )
    if any(
        not isinstance(item, HandoffAssociationEvidence)
        for item in canonical_candidates
    ):
        raise TypeError("candidates bevat geen HandoffAssociationEvidence")
    candidate_ids = tuple(item.candidate_id for item in canonical_candidates)
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("candidates bevat dubbele candidate_id")
    _validate_handoff_relationships(
        source_frame,
        target_frame,
        transform_evidence,
        overlap_evidence,
        canonical_candidates,
    )

    transform_usable = _transform_usable_at_handoff(
        source_frame,
        target_frame,
        transform_evidence,
    )
    global_reasons, global_blockers = _global_evidence_reasons(
        source_frame,
        target_frame,
        transform_evidence,
        overlap_evidence,
        policy,
    )

    assessment_groups: dict[str, list[HandoffAssociationAssessment]] = {}
    input_groups: dict[str, list[HandoffAssociationEvidence]] = {}
    for candidate in canonical_candidates:
        assessment = _assess_candidate(
            candidate,
            transform_evidence,
            policy,
            transform_usable=transform_usable,
        )
        assessment_groups.setdefault(candidate.track_id, []).append(assessment)
        input_groups.setdefault(candidate.track_id, []).append(candidate)

    ranked_groups: dict[str, tuple[HandoffAssociationAssessment, ...]] = {}
    continuities: list[HandoffTrackContinuity] = []
    for track_id in sorted(assessment_groups):
        ranked = _rank_assessments(assessment_groups[track_id])
        ranked_groups[track_id] = ranked
        input_by_id = {
            item.candidate_id: item for item in input_groups[track_id]
        }
        continuities.append(
            _build_track_continuity(
                track_id,
                ranked,
                input_by_id,
                global_blockers,
                policy,
                transform_usable=transform_usable,
            )
        )

    continuities = _downgrade_shared_target_leaders(
        continuities,
        canonical_candidates,
    )
    continuities_tuple = tuple(sorted(continuities, key=lambda item: item.track_id))
    assessments_tuple = tuple(
        assessment
        for track_id in sorted(ranked_groups)
        for assessment in ranked_groups[track_id]
    )

    if not continuities_tuple:
        continuity_state = HandoffContinuityState.INDETERMINATE
        global_reasons.add("association_candidates_absent")
    elif all(
        item.continuity_state is HandoffContinuityState.CONFIRMED
        for item in continuities_tuple
    ):
        continuity_state = HandoffContinuityState.CONFIRMED
    elif all(
        item.continuity_state is HandoffContinuityState.INDETERMINATE
        for item in continuities_tuple
    ):
        continuity_state = HandoffContinuityState.INDETERMINATE
    else:
        continuity_state = HandoffContinuityState.PROVISIONAL

    event_reasons = set(global_reasons)
    event_reasons.update(
        f"track:{item.track_id}:{reason}"
        for item in continuities_tuple
        for reason in item.reason_codes
    )
    event_reasons.update(
        {
            "cross_handoff_velocity_forbidden",
            "measurement_discontinuity_preserved",
            "shadow_only_no_live_effect",
        }
    )
    digest = _input_digest(
        source_frame,
        target_frame,
        transform_evidence,
        overlap_evidence,
        canonical_candidates,
        policy,
    )
    return ProviderHandoffEvent(
        event_id=f"provider-handoff:{digest}",
        valid_at=target_frame.valid_at,
        source_frame=source_frame,
        target_frame=target_frame,
        transform_evidence=transform_evidence,
        overlap_evidence=overlap_evidence,
        policy=policy,
        candidate_inputs=canonical_candidates,
        candidate_assessments=assessments_tuple,
        track_continuities=continuities_tuple,
        continuity_state=continuity_state,
        reason_codes=tuple(sorted(event_reasons)),
        evidence_digest_sha256=digest,
    )


def replay_provider_handoff_event(
    event: ProviderHandoffEvent,
) -> ProviderHandoffEvent:
    """Recompute an event solely from its embedded immutable replay inputs."""

    if not isinstance(event, ProviderHandoffEvent):
        raise TypeError("event moet een ProviderHandoffEvent zijn")
    replayed = build_provider_handoff_event(
        event.source_frame,
        event.target_frame,
        event.transform_evidence,
        event.overlap_evidence,
        event.candidate_inputs,
        policy=event.policy,
    )
    if replayed != event:
        raise ValueError("provider-handoff replay wijkt af van het event")
    return replayed


def _validate_handoff_relationships(
    source: HandoffFrameReference,
    target: HandoffFrameReference,
    transform: HandoffTransformEvidence,
    overlap: HandoffOverlapEvidence,
    candidates: tuple[HandoffAssociationEvidence, ...],
) -> None:
    if target.valid_at < source.valid_at:
        raise ValueError("targetframe ligt meteorologisch voor sourceframe")
    if (
        source.provider_id,
        source.product_id,
        source.analysis_grid_id,
        source.geolocation_id,
    ) == (
        target.provider_id,
        target.product_id,
        target.analysis_grid_id,
        target.geolocation_id,
    ):
        raise ValueError(
            "provider-handoff vereist een provider-, product-, grid- of "
            "geolocatiewijziging"
        )
    expected = (
        transform.source_frame_id == source.frame_id
        and transform.target_frame_id == target.frame_id
        and transform.source_grid_id == source.analysis_grid_id
        and transform.target_grid_id == target.analysis_grid_id
    )
    if not expected:
        raise ValueError("transform verwijst niet exact naar de handoffframes")
    if source.frame_id not in overlap.source_frame_ids:
        raise ValueError("overlapevidence bevat het sourceframe niet")
    if target.frame_id not in overlap.target_frame_ids:
        raise ValueError("overlapevidence bevat het targetframe niet")
    source_per_track: dict[str, tuple[str, int]] = {}
    for candidate in candidates:
        if (
            candidate.source_frame_id != source.frame_id
            or candidate.target_frame_id != target.frame_id
        ):
            raise ValueError("candidate verwijst niet exact naar de handoffframes")
        if candidate.prediction_valid_at != target.valid_at:
            raise ValueError(
                "candidate prediction_valid_at verschilt van target valid_at"
            )
        identity = (
            candidate.source_detection_id,
            candidate.source_track_revision,
        )
        previous = source_per_track.setdefault(candidate.track_id, identity)
        if previous != identity:
            raise ValueError(
                "een track gebruikt meerdere source-identiteiten in één handoff"
            )


def _transform_usable_at_handoff(
    source: HandoffFrameReference,
    target: HandoffFrameReference,
    transform: HandoffTransformEvidence,
) -> bool:
    return (
        transform.available
        and transform.valid_from <= source.valid_at <= transform.valid_to
        and transform.valid_from <= target.valid_at <= transform.valid_to
    )


def _global_evidence_reasons(
    source: HandoffFrameReference,
    target: HandoffFrameReference,
    transform: HandoffTransformEvidence,
    overlap: HandoffOverlapEvidence,
    policy: ProviderHandoffPolicy,
) -> tuple[set[str], set[str]]:
    reasons = {
        *(f"transform_evidence:{code}" for code in transform.reason_codes),
        *(f"overlap_evidence:{code}" for code in overlap.reason_codes),
    }
    blockers: set[str] = set()

    def block(reason: str) -> None:
        reasons.add(reason)
        blockers.add(reason)

    if not transform.available:
        block("transform_unavailable")
    elif not _transform_usable_at_handoff(source, target, transform):
        block("transform_not_valid_for_both_frames")
    if transform.support_count < policy.minimum_transform_support_count:
        block("transform_support_below_policy")
    if transform.registration_residual_m is None:
        block("registration_residual_missing")
    elif (
        transform.registration_residual_m
        > policy.maximum_registration_residual_m
    ):
        block("registration_residual_above_policy")

    if not (
        overlap.window_valid_from
        <= source.valid_at
        <= overlap.window_valid_to
        and overlap.window_valid_from
        <= target.valid_at
        <= overlap.window_valid_to
    ):
        block("overlap_window_does_not_cover_handoff")
    if (
        overlap.paired_frame_count
        < policy.minimum_paired_frames_for_confirmation
    ):
        block("paired_overlap_frames_below_policy")
    _minimum_optional_metric(
        overlap.footprint_overlap_fraction,
        policy.minimum_footprint_overlap_fraction,
        "footprint_overlap",
        block,
    )
    _minimum_optional_metric(
        overlap.valid_data_overlap_fraction,
        policy.minimum_valid_data_overlap_fraction,
        "valid_data_overlap",
        block,
    )
    _minimum_optional_metric(
        overlap.missing_mask_agreement,
        policy.minimum_missing_mask_agreement,
        "missing_mask_agreement",
        block,
    )
    if overlap.intensity_quantile_distance is None:
        block("intensity_quantile_distance_missing")
    elif (
        overlap.intensity_quantile_distance
        > policy.maximum_intensity_quantile_distance
    ):
        block("intensity_quantile_distance_above_policy")
    _minimum_optional_metric(
        overlap.phase_correlation_peak_ratio,
        policy.minimum_phase_correlation_peak_ratio,
        "phase_correlation_peak_ratio",
        block,
    )
    evidence_codes = set(transform.reason_codes) | set(overlap.reason_codes)
    for code in sorted(
        evidence_codes & set(policy.blocking_evidence_reason_codes)
    ):
        block(f"policy_blocking_evidence:{code}")
    if not blockers:
        reasons.add("global_handoff_evidence_meets_policy")
    return reasons, blockers


def _minimum_optional_metric(
    value: float | None,
    threshold: float,
    label: str,
    block,
) -> None:
    if value is None:
        block(f"{label}_missing")
    elif value < threshold:
        block(f"{label}_below_policy")


def _assess_candidate(
    candidate: HandoffAssociationEvidence,
    transform: HandoffTransformEvidence,
    policy: ProviderHandoffPolicy,
    *,
    transform_usable: bool,
) -> HandoffAssociationAssessment:
    raw_displacement = math.hypot(
        candidate.target_detection_x_m - candidate.source_centroid_x_m,
        candidate.target_detection_y_m - candidate.source_centroid_y_m,
    )
    reasons = {
        f"candidate_evidence:{code}" for code in candidate.reason_codes
    }
    if not transform_usable:
        reasons.add("registered_position_indeterminate")
        return HandoffAssociationAssessment(
            candidate_id=candidate.candidate_id,
            track_id=candidate.track_id,
            source_detection_id=candidate.source_detection_id,
            target_detection_id=candidate.target_detection_id,
            rank_within_track=None,
            evidence_state="indeterminate_transform",
            raw_coordinate_displacement_m_audit_only=raw_displacement,
            registered_prediction_x_m=None,
            registered_prediction_y_m=None,
            registered_residual_x_m=None,
            registered_residual_y_m=None,
            registered_residual_m=None,
            measurement_covariance_m2=None,
            innovation_covariance_m2=None,
            normalized_innovation_squared=None,
            standardized_position_residual=None,
            advected_polygon_iou=candidate.advected_polygon_iou,
            prior_association_ids=candidate.prior_association_ids,
            reason_codes=tuple(sorted(reasons)),
        )

    affine = transform.affine_target_from_source
    transform_covariance = transform.uncertainty_covariance_m2
    if affine is None or transform_covariance is None:
        raise AssertionError("transform_usable zonder transformmatrix")
    a = (
        (affine[0][0], affine[0][1]),
        (affine[1][0], affine[1][1]),
    )
    registered_x = (
        affine[0][0] * candidate.predicted_source_x_m
        + affine[0][1] * candidate.predicted_source_y_m
        + affine[0][2]
    )
    registered_y = (
        affine[1][0] * candidate.predicted_source_x_m
        + affine[1][1] * candidate.predicted_source_y_m
        + affine[1][2]
    )
    residual_x = candidate.target_detection_x_m - registered_x
    residual_y = candidate.target_detection_y_m - registered_y
    residual_m = math.hypot(residual_x, residual_y)
    transformed_prediction_covariance = _similarity_transform(
        a,
        candidate.predicted_position_covariance_m2,
    )
    measurement_covariance = _matrix_add(
        candidate.target_position_covariance_m2,
        transform_covariance,
        policy.product_difference_covariance_m2,
    )
    innovation_covariance = _matrix_add(
        transformed_prediction_covariance,
        measurement_covariance,
    )
    nis = _mahalanobis_squared(
        residual_x,
        residual_y,
        innovation_covariance,
    )
    standardized = math.sqrt(nis)
    reasons.add("registered_position_evidence_available")
    if standardized > policy.maximum_standardized_position_residual:
        reasons.add("standardized_position_residual_above_policy")
    if candidate.advected_polygon_iou is None:
        reasons.add("advected_polygon_iou_missing")
    elif candidate.advected_polygon_iou < policy.minimum_advected_polygon_iou:
        reasons.add("advected_polygon_iou_below_policy")
    if (
        len(candidate.prior_association_ids)
        < policy.minimum_prior_association_count
    ):
        reasons.add("prior_association_support_below_policy")
    for code in sorted(
        set(candidate.reason_codes)
        & set(policy.blocking_evidence_reason_codes)
    ):
        reasons.add(f"policy_blocking_evidence:{code}")
    return HandoffAssociationAssessment(
        candidate_id=candidate.candidate_id,
        track_id=candidate.track_id,
        source_detection_id=candidate.source_detection_id,
        target_detection_id=candidate.target_detection_id,
        rank_within_track=None,
        evidence_state="registered_position_evidence",
        raw_coordinate_displacement_m_audit_only=raw_displacement,
        registered_prediction_x_m=registered_x,
        registered_prediction_y_m=registered_y,
        registered_residual_x_m=residual_x,
        registered_residual_y_m=residual_y,
        registered_residual_m=residual_m,
        measurement_covariance_m2=measurement_covariance,
        innovation_covariance_m2=innovation_covariance,
        normalized_innovation_squared=nis,
        standardized_position_residual=standardized,
        advected_polygon_iou=candidate.advected_polygon_iou,
        prior_association_ids=candidate.prior_association_ids,
        reason_codes=tuple(sorted(reasons)),
    )


def _rank_assessments(
    assessments: Iterable[HandoffAssociationAssessment],
) -> tuple[HandoffAssociationAssessment, ...]:
    def key(item: HandoffAssociationAssessment):
        if item.normalized_innovation_squared is None:
            return (1, math.inf, 0.0, item.candidate_id)
        return (
            0,
            item.normalized_innovation_squared,
            -(
                item.advected_polygon_iou
                if item.advected_polygon_iou is not None
                else -1.0
            ),
            item.candidate_id,
        )

    ordered = sorted(assessments, key=key)
    return tuple(
        replace(item, rank_within_track=index)
        for index, item in enumerate(ordered, start=1)
    )


def _build_track_continuity(
    track_id: str,
    ranked: tuple[HandoffAssociationAssessment, ...],
    input_by_id: dict[str, HandoffAssociationEvidence],
    global_blockers: set[str],
    policy: ProviderHandoffPolicy,
    *,
    transform_usable: bool,
) -> HandoffTrackContinuity:
    first_input = input_by_id[ranked[0].candidate_id]
    if not transform_usable:
        return HandoffTrackContinuity(
            track_id=track_id,
            source_detection_id=first_input.source_detection_id,
            source_track_revision=first_input.source_track_revision,
            continuity_state=HandoffContinuityState.INDETERMINATE,
            leading_candidate_id=None,
            alternative_candidate_ids=tuple(
                item.candidate_id for item in ranked
            ),
            association_margin_nis=None,
            measurement_covariance_m2=None,
            prior_association_ids=first_input.prior_association_ids,
            reason_codes=tuple(
                sorted(set(global_blockers) | {"transform_unusable_for_track"})
            ),
            motion_update_mode="no_motion_update",
        )

    leading = ranked[0]
    blockers = set(global_blockers)
    blockers.update(
        reason
        for reason in leading.reason_codes
        if reason
        in {
            "standardized_position_residual_above_policy",
            "advected_polygon_iou_missing",
            "advected_polygon_iou_below_policy",
            "prior_association_support_below_policy",
        }
        or reason.startswith("policy_blocking_evidence:")
    )
    margin: float | None = None
    leading_candidate_id: str | None = leading.candidate_id
    alternatives = tuple(item.candidate_id for item in ranked[1:])
    if len(ranked) > 1:
        leading_nis = leading.normalized_innovation_squared
        second_nis = ranked[1].normalized_innovation_squared
        if leading_nis is None or second_nis is None:
            blockers.add("association_margin_indeterminate")
        else:
            margin = second_nis - leading_nis
            if margin == 0.0:
                blockers.add("association_top_nis_tied")
                leading_candidate_id = None
                alternatives = tuple(item.candidate_id for item in ranked)
            elif margin < policy.minimum_nis_margin_for_confirmation:
                blockers.add("association_margin_below_policy")
    state = (
        HandoffContinuityState.CONFIRMED
        if not blockers
        else HandoffContinuityState.PROVISIONAL
    )
    reason_codes = set(blockers)
    if state is HandoffContinuityState.CONFIRMED:
        reason_codes.add("continuity_evidence_meets_policy")
    else:
        reason_codes.add("weak_continuity_retained_provisionally")
    selected_input = (
        input_by_id[leading_candidate_id]
        if leading_candidate_id is not None
        else first_input
    )
    return HandoffTrackContinuity(
        track_id=track_id,
        source_detection_id=first_input.source_detection_id,
        source_track_revision=first_input.source_track_revision,
        continuity_state=state,
        leading_candidate_id=leading_candidate_id,
        alternative_candidate_ids=alternatives,
        association_margin_nis=margin,
        measurement_covariance_m2=(
            leading.measurement_covariance_m2
            if leading_candidate_id is not None
            else None
        ),
        prior_association_ids=selected_input.prior_association_ids,
        reason_codes=tuple(sorted(reason_codes)),
        motion_update_mode=(
            "registered_position_only"
            if leading_candidate_id is not None
            else "no_motion_update"
        ),
    )


def _downgrade_shared_target_leaders(
    continuities: list[HandoffTrackContinuity],
    candidates: tuple[HandoffAssociationEvidence, ...],
) -> list[HandoffTrackContinuity]:
    target_by_candidate = {
        item.candidate_id: item.target_detection_id for item in candidates
    }
    leaders: dict[str, list[str]] = {}
    for item in continuities:
        if item.leading_candidate_id is not None:
            target_id = target_by_candidate[item.leading_candidate_id]
            leaders.setdefault(target_id, []).append(item.track_id)
    shared_tracks = {
        track_id
        for track_ids in leaders.values()
        if len(track_ids) > 1
        for track_id in track_ids
    }
    result: list[HandoffTrackContinuity] = []
    for item in continuities:
        if item.track_id not in shared_tracks:
            result.append(item)
            continue
        result.append(
            replace(
                item,
                continuity_state=HandoffContinuityState.PROVISIONAL,
                leading_candidate_id=None,
                alternative_candidate_ids=tuple(
                    (
                        (item.leading_candidate_id,)
                        if item.leading_candidate_id is not None
                        else ()
                    )
                    + item.alternative_candidate_ids
                ),
                measurement_covariance_m2=None,
                reason_codes=tuple(
                    sorted(
                        (
                            set(item.reason_codes)
                            - {"continuity_evidence_meets_policy"}
                        )
                        | {
                            "leading_target_shared_across_tracks",
                            "weak_continuity_retained_provisionally",
                        }
                    )
                ),
                motion_update_mode="no_motion_update",
            )
        )
    return result


def _input_digest(
    source_frame: HandoffFrameReference,
    target_frame: HandoffFrameReference,
    transform_evidence: HandoffTransformEvidence,
    overlap_evidence: HandoffOverlapEvidence,
    candidates: tuple[HandoffAssociationEvidence, ...],
    policy: ProviderHandoffPolicy,
) -> str:
    payload = {
        "algorithm_version": PROVIDER_HANDOFF_ALGORITHM_VERSION,
        "source_frame": asdict(source_frame),
        "target_frame": asdict(target_frame),
        "transform_evidence": asdict(transform_evidence),
        "overlap_evidence": asdict(overlap_evidence),
        "candidate_inputs": [asdict(item) for item in candidates],
        "policy": asdict(policy),
    }
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _similarity_transform(matrix: Matrix2, covariance: Matrix2) -> Matrix2:
    a00, a01 = matrix[0]
    a10, a11 = matrix[1]
    p00, p01 = covariance[0]
    p10, p11 = covariance[1]
    ap00 = a00 * p00 + a01 * p10
    ap01 = a00 * p01 + a01 * p11
    ap10 = a10 * p00 + a11 * p10
    ap11 = a10 * p01 + a11 * p11
    return (
        (
            ap00 * a00 + ap01 * a01,
            ap00 * a10 + ap01 * a11,
        ),
        (
            ap10 * a00 + ap11 * a01,
            ap10 * a10 + ap11 * a11,
        ),
    )


def _matrix_add(*matrices: Matrix2) -> Matrix2:
    return (
        (
            sum(matrix[0][0] for matrix in matrices),
            sum(matrix[0][1] for matrix in matrices),
        ),
        (
            sum(matrix[1][0] for matrix in matrices),
            sum(matrix[1][1] for matrix in matrices),
        ),
    )


def _mahalanobis_squared(dx: float, dy: float, covariance: Matrix2) -> float:
    _symmetric_pd_matrix(covariance, "innovation_covariance_m2")
    a = covariance[0][0]
    b = covariance[0][1]
    d = covariance[1][1]
    determinant = a * d - b * b
    value = (d * dx * dx - 2.0 * b * dx * dy + a * dy * dy) / determinant
    if not math.isfinite(value):
        raise ValueError("handoff-NIS overschrijdt het numerieke bereik")
    return max(0.0, value)


def _affine(value: object) -> None:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or any(not isinstance(row, tuple) or len(row) != 3 for row in value)
    ):
        raise TypeError("affine_target_from_source moet een immutable 2x3-tuple zijn")
    values = tuple(item for row in value for item in row)
    if any(isinstance(item, bool) or not isinstance(item, (int, float)) for item in values):
        raise TypeError("affine_target_from_source bevat geen getallen")
    if any(not math.isfinite(float(item)) for item in values):
        raise ValueError("affine_target_from_source moet eindig zijn")
    determinant = value[0][0] * value[1][1] - value[0][1] * value[1][0]
    if determinant == 0.0:
        raise ValueError("affine_target_from_source is singulier")


def _symmetric_pd_matrix(value: object, label: str) -> None:
    _symmetric_matrix(value, label)
    a = value[0][0]
    b = value[0][1]
    d = value[1][1]
    if a <= 0.0 or d <= 0.0 or a * d - b * b <= 0.0:
        raise ValueError(f"{label} moet positief-definiet zijn")


def _symmetric_psd_matrix(value: object, label: str) -> None:
    _symmetric_matrix(value, label)
    a = value[0][0]
    b = value[0][1]
    d = value[1][1]
    if a < 0.0 or d < 0.0 or a * d - b * b < 0.0:
        raise ValueError(f"{label} moet positief-semidefiniet zijn")


def _symmetric_matrix(value: object, label: str) -> None:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or any(not isinstance(row, tuple) or len(row) != 2 for row in value)
    ):
        raise TypeError(f"{label} moet een immutable 2x2-tuple zijn")
    values = tuple(item for row in value for item in row)
    if any(isinstance(item, bool) or not isinstance(item, (int, float)) for item in values):
        raise TypeError(f"{label} bevat geen getallen")
    if any(not math.isfinite(float(item)) for item in values):
        raise ValueError(f"{label} moet eindig zijn")
    if value[0][1] != value[1][0]:
        raise ValueError(f"{label} moet symmetrisch zijn")


def _canonical_identifiers(
    values: object,
    label: str,
    *,
    allow_empty: bool = True,
) -> tuple[str, ...]:
    if not isinstance(values, tuple):
        raise TypeError(f"{label} moet een immutable tuple zijn")
    for value in values:
        _identifier(value, label)
    if len(values) != len(set(values)):
        raise ValueError(f"{label} bevat duplicaten")
    canonical = tuple(sorted(values))
    if not allow_empty and not canonical:
        raise ValueError(f"{label} mag niet leeg zijn")
    return canonical


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{label} moet een niet-lege getrimde string zijn")
    return value


def _sha256(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} moet een lowercase SHA-256 zijn")
    return value


def _finite(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"{label} moet eindig zijn")
    return float(value)


def _nonnegative_finite(value: object, label: str) -> float:
    number = _finite(value, label)
    if number < 0.0:
        raise ValueError(f"{label} moet niet-negatief zijn")
    return number


def _unit_interval(value: object, label: str) -> float:
    number = _finite(value, label)
    if not 0.0 <= number <= 1.0:
        raise ValueError(f"{label} moet tussen 0 en 1 liggen")
    return number


def _optional_unit_interval(value: object, label: str) -> None:
    if value is not None:
        _unit_interval(value, label)


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} moet een niet-negatieve integer zijn")
    return value


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} moet een positieve integer zijn")
    return value
