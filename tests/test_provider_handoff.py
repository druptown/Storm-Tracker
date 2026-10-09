"""Scientific contracts for the shadow-only provider handoff engine."""
from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import hashlib
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.provider_handoff import (
    HandoffAssociationEvidence,
    HandoffContinuityState,
    HandoffFrameReference,
    HandoffOverlapEvidence,
    HandoffTransformEvidence,
    ProviderHandoffPolicy,
    build_provider_handoff_event,
    replay_provider_handoff_event,
)


ROOT = Path(__file__).resolve().parent.parent
SOURCE_SHA = "a" * 64
TARGET_SHA = "b" * 64
SUPPORT_SHA = "c" * 64


def _source_frame() -> HandoffFrameReference:
    return HandoffFrameReference(
        frame_id="source-frame",
        provider_id="provider-a",
        product_id="rain-rate-a",
        analysis_grid_id="grid-a",
        geolocation_id="geo-a",
        valid_at=1_000.0,
        validity_start=990.0,
        validity_end=1_010.0,
        input_artifact_sha256=SOURCE_SHA,
        parser_version="parser-a-v1",
    )


def _target_frame() -> HandoffFrameReference:
    return HandoffFrameReference(
        frame_id="target-frame",
        provider_id="provider-b",
        product_id="rain-rate-b",
        analysis_grid_id="grid-b",
        geolocation_id="geo-b",
        valid_at=1_060.0,
        validity_start=1_050.0,
        validity_end=1_070.0,
        input_artifact_sha256=TARGET_SHA,
        parser_version="parser-b-v1",
    )


def _transform() -> HandoffTransformEvidence:
    return HandoffTransformEvidence(
        transform_id="registration-a-to-b",
        source_frame_id="source-frame",
        target_frame_id="target-frame",
        source_grid_id="grid-a",
        target_grid_id="grid-b",
        valid_from=900.0,
        valid_to=1_100.0,
        method_id="caller-phase-correlation-v7",
        affine_target_from_source=(
            (1.0, 0.0, 100_000.0),
            (0.0, 1.0, 0.0),
        ),
        uncertainty_covariance_m2=((400.0, 0.0), (0.0, 400.0)),
        registration_residual_m=100.0,
        support_count=10,
        reason_codes=(),
    )


def _overlap() -> HandoffOverlapEvidence:
    return HandoffOverlapEvidence(
        evidence_id="overlap-a-b",
        source_frame_ids=("source-frame",),
        target_frame_ids=("target-frame",),
        window_valid_from=900.0,
        window_valid_to=1_100.0,
        paired_frame_count=4,
        footprint_overlap_fraction=0.8,
        valid_data_overlap_fraction=0.75,
        missing_mask_agreement=0.9,
        intensity_quantile_distance=0.1,
        phase_correlation_peak_ratio=4.0,
        support_artifact_sha256s=(SUPPORT_SHA, TARGET_SHA, SOURCE_SHA),
        method_id="caller-overlap-audit-v3",
        reason_codes=(),
    )


def _policy() -> ProviderHandoffPolicy:
    return ProviderHandoffPolicy(
        policy_id="explicit-test-policy",
        minimum_paired_frames_for_confirmation=3,
        minimum_transform_support_count=5,
        minimum_footprint_overlap_fraction=0.5,
        minimum_valid_data_overlap_fraction=0.5,
        minimum_missing_mask_agreement=0.75,
        maximum_intensity_quantile_distance=0.2,
        minimum_phase_correlation_peak_ratio=2.0,
        maximum_registration_residual_m=500.0,
        maximum_standardized_position_residual=3.0,
        minimum_advected_polygon_iou=0.2,
        minimum_nis_margin_for_confirmation=0.5,
        minimum_prior_association_count=1,
        product_difference_covariance_m2=(
            (900.0, 0.0),
            (0.0, 900.0),
        ),
        blocking_evidence_reason_codes=("upstream_warning",),
    )


def _candidate(
    candidate_id: str = "candidate-a",
    *,
    track_id: str = "track-a",
    target_detection_id: str = "target-detection-a",
    target_x_m: float = 100_650.0,
    iou: float | None = 0.8,
) -> HandoffAssociationEvidence:
    return HandoffAssociationEvidence(
        candidate_id=candidate_id,
        track_id=track_id,
        source_frame_id="source-frame",
        target_frame_id="target-frame",
        source_detection_id=f"source-{track_id}",
        target_detection_id=target_detection_id,
        source_track_revision=4,
        prediction_valid_at=1_060.0,
        source_centroid_x_m=0.0,
        source_centroid_y_m=0.0,
        predicted_source_x_m=600.0,
        predicted_source_y_m=0.0,
        target_detection_x_m=target_x_m,
        target_detection_y_m=0.0,
        predicted_position_covariance_m2=(
            (2_500.0, 0.0),
            (0.0, 2_500.0),
        ),
        target_position_covariance_m2=(
            (400.0, 0.0),
            (0.0, 400.0),
        ),
        advected_polygon_iou=iou,
        prior_association_ids=("association-3", "association-2"),
        reason_codes=(),
    )


def _event(*candidates: HandoffAssociationEvidence):
    return build_provider_handoff_event(
        _source_frame(),
        _target_frame(),
        _transform(),
        _overlap(),
        candidates or (_candidate(),),
        policy=_policy(),
    )


def test_registered_position_prevents_artificial_cross_grid_velocity_spike():
    event = _event()
    assessment = event.candidate_assessments[0]
    continuity = event.track_continuities[0]

    assert event.continuity_state is HandoffContinuityState.CONFIRMED
    assert assessment.raw_coordinate_displacement_m_audit_only == 100_650.0
    assert assessment.registered_prediction_x_m == 100_600.0
    assert assessment.registered_residual_m == 50.0
    assert assessment.measurement_covariance_m2 == (
        (1_700.0, 0.0),
        (0.0, 1_700.0),
    )
    assert continuity.motion_update_mode == "registered_position_only"
    assert continuity.allow_cross_handoff_velocity_update is False
    assert event.allow_cross_handoff_velocity_update is False
    assert not hasattr(assessment, "velocity_m_s")
    assert "cross_handoff_velocity_forbidden" in event.reason_codes


def test_product_and_registration_uncertainty_inflate_position_covariance():
    event = _event()
    assessment = event.candidate_assessments[0]

    assert assessment.measurement_covariance_m2[0][0] > 400.0
    assert assessment.innovation_covariance_m2 == (
        (4_200.0, 0.0),
        (0.0, 4_200.0),
    )
    assert assessment.normalized_innovation_squared == pytest.approx(
        2_500.0 / 4_200.0
    )


def test_affine_rotation_propagates_prediction_covariance():
    transform = replace(
        _transform(),
        affine_target_from_source=(
            (0.0, -1.0, 100_000.0),
            (1.0, 0.0, 50_000.0),
        ),
    )
    candidate = replace(
        _candidate(),
        target_detection_x_m=100_050.0,
        target_detection_y_m=50_620.0,
        predicted_position_covariance_m2=(
            (400.0, 100.0),
            (100.0, 900.0),
        ),
    )

    event = build_provider_handoff_event(
        _source_frame(),
        _target_frame(),
        transform,
        _overlap(),
        (candidate,),
        policy=_policy(),
    )
    assessment = event.candidate_assessments[0]

    assert assessment.registered_prediction_x_m == 100_000.0
    assert assessment.registered_prediction_y_m == 50_600.0
    assert assessment.innovation_covariance_m2 == (
        (2_600.0, -100.0),
        (-100.0, 2_100.0),
    )


def test_weak_overlap_is_provisional_and_retains_every_candidate():
    weak_overlap = replace(
        _overlap(),
        paired_frame_count=0,
        footprint_overlap_fraction=None,
        valid_data_overlap_fraction=None,
        missing_mask_agreement=None,
        intensity_quantile_distance=None,
        phase_correlation_peak_ratio=None,
        reason_codes=("no_temporal_overlap",),
    )
    candidates = (
        _candidate("candidate-a"),
        _candidate(
            "candidate-b",
            target_detection_id="target-detection-b",
            target_x_m=101_200.0,
        ),
    )

    event = build_provider_handoff_event(
        _source_frame(),
        _target_frame(),
        _transform(),
        weak_overlap,
        candidates,
        policy=_policy(),
    )

    assert event.continuity_state is HandoffContinuityState.PROVISIONAL
    assert {item.candidate_id for item in event.candidate_inputs} == {
        "candidate-a",
        "candidate-b",
    }
    assert (
        event.track_continuities[0].continuity_state
        is HandoffContinuityState.PROVISIONAL
    )
    assert "paired_overlap_frames_below_policy" in event.reason_codes
    assert not any("reject" in reason for reason in event.reason_codes)


def test_extreme_registered_residual_is_retained_not_rejected():
    extreme = _candidate(
        target_x_m=2_000_000.0,
    )

    event = _event(extreme)
    continuity = event.track_continuities[0]
    assessment = event.candidate_assessments[0]

    assert event.candidate_inputs == (extreme,)
    assert assessment.evidence_state == "registered_position_evidence"
    assert (
        "standardized_position_residual_above_policy"
        in assessment.reason_codes
    )
    assert continuity.continuity_state is HandoffContinuityState.PROVISIONAL
    assert "weak_continuity_retained_provisionally" in continuity.reason_codes
    assert not any(
        "reject" in reason
        for reason in (*assessment.reason_codes, *continuity.reason_codes)
    )


def test_missing_transform_is_indeterminate_without_losing_weather_evidence():
    unavailable = replace(
        _transform(),
        affine_target_from_source=None,
        uncertainty_covariance_m2=None,
        registration_residual_m=None,
        support_count=0,
        reason_codes=("registration_not_observed",),
    )
    candidate = _candidate()

    event = build_provider_handoff_event(
        _source_frame(),
        _target_frame(),
        unavailable,
        _overlap(),
        (candidate,),
        policy=_policy(),
    )

    continuity = event.track_continuities[0]
    assert event.continuity_state is HandoffContinuityState.INDETERMINATE
    assert continuity.leading_candidate_id is None
    assert continuity.alternative_candidate_ids == (candidate.candidate_id,)
    assert continuity.motion_update_mode == "no_motion_update"
    assert event.candidate_inputs == (candidate,)
    assert event.candidate_assessments[0].evidence_state == (
        "indeterminate_transform"
    )


def test_transform_outside_frame_validity_is_indeterminate_not_guessed():
    stale_transform = replace(
        _transform(),
        valid_from=1_040.0,
    )

    event = build_provider_handoff_event(
        _source_frame(),
        _target_frame(),
        stale_transform,
        _overlap(),
        (_candidate(),),
        policy=_policy(),
    )

    assert event.continuity_state is HandoffContinuityState.INDETERMINATE
    assert "transform_not_valid_for_both_frames" in event.reason_codes
    assert event.track_continuities[0].motion_update_mode == "no_motion_update"


def test_input_order_does_not_change_event_or_content_address():
    first = _candidate("candidate-z", target_x_m=101_500.0)
    second = _candidate(
        "candidate-a",
        target_detection_id="target-near",
        target_x_m=100_650.0,
    )

    forward = _event(first, second)
    reverse = _event(second, first)

    assert forward == reverse
    assert forward.event_id == reverse.event_id
    assert forward.candidate_inputs[0].candidate_id == "candidate-a"
    assert forward.track_continuities[0].leading_candidate_id == "candidate-a"
    assert forward.track_continuities[0].alternative_candidate_ids == (
        "candidate-z",
    )


def test_exact_top_tie_never_uses_identifier_as_hidden_identity_decision():
    first = _candidate(
        "candidate-a",
        target_detection_id="target-a",
        target_x_m=100_650.0,
    )
    second = _candidate(
        "candidate-b",
        target_detection_id="target-b",
        target_x_m=100_650.0,
    )

    event = _event(second, first)
    continuity = event.track_continuities[0]

    assert continuity.continuity_state is HandoffContinuityState.PROVISIONAL
    assert continuity.leading_candidate_id is None
    assert continuity.alternative_candidate_ids == (
        "candidate-a",
        "candidate-b",
    )
    assert continuity.association_margin_nis == 0.0
    assert "association_top_nis_tied" in continuity.reason_codes


def test_shared_target_across_tracks_downgrades_both_identities():
    target_id = "shared-target"
    first = _candidate(
        "candidate-track-a",
        track_id="track-a",
        target_detection_id=target_id,
    )
    second = _candidate(
        "candidate-track-b",
        track_id="track-b",
        target_detection_id=target_id,
    )

    event = _event(first, second)

    assert event.continuity_state is HandoffContinuityState.PROVISIONAL
    assert all(
        item.continuity_state is HandoffContinuityState.PROVISIONAL
        for item in event.track_continuities
    )
    assert all(
        "leading_target_shared_across_tracks" in item.reason_codes
        for item in event.track_continuities
    )
    assert all(
        item.leading_candidate_id is None
        and item.motion_update_mode == "no_motion_update"
        for item in event.track_continuities
    )


def test_policy_blocking_evidence_retains_candidate_as_provisional():
    candidate = replace(
        _candidate(),
        reason_codes=("upstream_warning", "diagnostic_only"),
    )

    event = _event(candidate)

    assert event.continuity_state is HandoffContinuityState.PROVISIONAL
    assert "policy_blocking_evidence:upstream_warning" in (
        event.track_continuities[0].reason_codes
    )
    assert event.candidate_inputs == (candidate,)


def test_event_replays_exactly_from_embedded_immutable_inputs():
    event = _event(
        _candidate("candidate-b", target_x_m=101_500.0),
        _candidate(
            "candidate-a",
            target_detection_id="target-near",
            target_x_m=100_650.0,
        ),
    )

    replayed = replay_provider_handoff_event(event)

    assert replayed == event
    assert replayed.event_id == (
        f"provider-handoff:{replayed.evidence_digest_sha256}"
    )


def test_no_candidates_still_produces_a_replayable_indeterminate_event():
    event = build_provider_handoff_event(
        _source_frame(),
        _target_frame(),
        _transform(),
        _overlap(),
        (),
        policy=_policy(),
    )

    assert event.continuity_state is HandoffContinuityState.INDETERMINATE
    assert event.candidate_inputs == ()
    assert event.track_continuities == ()
    assert "association_candidates_absent" in event.reason_codes
    assert replay_provider_handoff_event(event) == event


def test_structural_mismatches_fail_closed_without_scientific_guessing():
    same_contract_target = replace(
        _source_frame(),
        frame_id="target-frame",
        valid_at=1_060.0,
        validity_start=1_050.0,
        validity_end=1_070.0,
        input_artifact_sha256=TARGET_SHA,
    )
    with pytest.raises(ValueError, match="wijziging"):
        build_provider_handoff_event(
            _source_frame(),
            same_contract_target,
            replace(
                _transform(),
                target_grid_id="grid-a",
            ),
            _overlap(),
            (),
            policy=_policy(),
        )

    wrong_time = replace(_candidate(), prediction_valid_at=1_061.0)
    with pytest.raises(ValueError, match="prediction_valid_at"):
        _event(wrong_time)


def test_transform_and_policy_require_explicit_valid_uncertainty():
    with pytest.raises(ValueError, match="uncertainty_covariance"):
        replace(_transform(), uncertainty_covariance_m2=None)
    with pytest.raises(ValueError, match="positief-semidefiniet"):
        replace(
            _policy(),
            product_difference_covariance_m2=(
                (-1.0, 0.0),
                (0.0, 1.0),
            ),
        )
    with pytest.raises(TypeError):
        ProviderHandoffPolicy(  # type: ignore[call-arg]
            policy_id="incomplete"
        )


def test_records_and_nested_collections_are_immutable_and_canonical():
    event = _event()

    assert event.overlap_evidence.support_artifact_sha256s == (
        SOURCE_SHA,
        TARGET_SHA,
        SUPPORT_SHA,
    )
    assert event.candidate_inputs[0].prior_association_ids == (
        "association-2",
        "association-3",
    )
    with pytest.raises(FrozenInstanceError):
        event.continuity_state = HandoffContinuityState.PROVISIONAL


def test_component_and_worker_handoff_engines_are_exact_mirrors():
    component = (
        ROOT
        / "custom_components/storm_tracker_v3/engine/provider_handoff.py"
    ).read_bytes()
    worker = (
        ROOT
        / "storm_tracker_worker/tracking_core/engine/provider_handoff.py"
    ).read_bytes()

    assert component == worker
    assert hashlib.sha256(component).hexdigest()
