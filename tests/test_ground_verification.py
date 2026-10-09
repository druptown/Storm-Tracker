"""Tests for the pure event-held-out ground verification foundation."""
from __future__ import annotations

from dataclasses import replace

import pytest

from storm_tracker_worker.tracking_core.engine.ground_verification import (
    CROSS_STORM_CORRECTNESS_EXTERNAL_GATE,
    CandidateWetDryState,
    GroundAdmissionDisposition,
    GroundExclusionReason,
    GroundStratumDimension,
    GroundVerificationPartition,
    GroundWetDryCandidate,
    admit_ground_verification_case,
    verify_ground_admissions,
)
from storm_tracker_worker.validation_corpus import (
    EventFrame,
    EventSplitPolicy,
    ExternalLabel,
    ExternalLabelCorpus,
    ExternalLabelKind,
    ExternalLabelSource,
    ExternalSourceKind,
    ExternalTruthContext,
    LabelState,
    WetDryLabel,
    WetDryState,
    build_event_held_out_split,
)


def _source(
    *,
    source_id: str = "external-truth",
    independent: bool = True,
    kind: ExternalSourceKind = ExternalSourceKind.IN_SITU_INSTRUMENT,
) -> ExternalLabelSource:
    return ExternalLabelSource(
        source_id=source_id,
        source_kind=kind,
        producer="independent-test-producer",
        dataset_version="truth-v1",
        methodology_id="method-v1",
        source_artifact_sha256="a" * 64,
        candidate_independent=independent,
        blinded_to_candidate=False,
    )


def _label(
    label_id: str,
    event_id: str,
    frame_id: str,
    state: WetDryState,
    *,
    site_id: str = "site-1",
    start: float = 10.0,
    end: float = 20.0,
    dry_capable: bool = True,
    label_state: LabelState = LabelState.CONFIRMED,
    source_id: str = "external-truth",
) -> ExternalLabel:
    return ExternalLabel(
        label_id=label_id,
        label_kind=ExternalLabelKind.WET_DRY,
        source_id=source_id,
        event_id=event_id,
        frame_id=frame_id,
        state=label_state,
        payload=WetDryLabel(
            site_id=site_id,
            interval_start_utc=start,
            interval_end_utc=end,
            state=state,
            capable_of_dry_detection=dry_capable,
        ),
    )


def _context(
    frames: tuple[EventFrame, ...],
    labels: tuple[ExternalLabel, ...],
    *,
    sources: tuple[ExternalLabelSource, ...] | None = None,
) -> ExternalTruthContext:
    split = build_event_held_out_split(
        frames,
        EventSplitPolicy(
            policy_id="ground-verification-test",
            hash_salt="ground-verification-test-salt",
            train_basis_points=6000,
            validation_basis_points=2000,
            test_basis_points=2000,
        ),
    )
    return ExternalTruthContext(
        split=split,
        labels=ExternalLabelCorpus(
            sources=tuple(
                sorted(sources or (_source(),), key=lambda item: item.source_id)
            ),
            labels=tuple(sorted(labels, key=lambda item: item.label_id)),
        ),
    )


def _candidate(
    candidate_id: str = "candidate-1",
    *,
    decision_id: str = "decision-1",
    event_id: str = "event-1",
    frame_id: str = "frame-1",
    site_id: str = "site-1",
    start: float = 10.0,
    end: float = 20.0,
    provider_id: str = "provider-1",
    capability_id: str = "rain-state",
    can_observe_absence: bool = True,
    state: CandidateWetDryState = CandidateWetDryState.WET,
    track_id: str | None = "track-1",
    algorithm_version: str = "candidate-ground-v1",
) -> GroundWetDryCandidate:
    return GroundWetDryCandidate(
        candidate_id=candidate_id,
        decision_id=decision_id,
        evaluation_stratum_id="all-regions",
        algorithm_version=algorithm_version,
        event_id=event_id,
        frame_id=frame_id,
        physical_site_id=site_id,
        interval_start_utc=start,
        interval_end_utc=end,
        provider_id=provider_id,
        capability_id=capability_id,
        candidate_can_observe_absence=can_observe_absence,
        state=state,
        track_id=track_id,
    )


def _partition(context: ExternalTruthContext) -> GroundVerificationPartition:
    value = context.split.partition_for_event("event-1")
    assert value is not None
    return GroundVerificationPartition(value.value)


def _admit(
    candidate: GroundWetDryCandidate,
    context: ExternalTruthContext,
    label_id: str,
) -> object:
    partition = context.split.partition_for_event(candidate.event_id)
    assert partition is not None
    return admit_ground_verification_case(
        candidate,
        expected_partition=GroundVerificationPartition(partition.value),
        truth_context=context,
        wet_dry_label_id=label_id,
    )


def test_ha_and_worker_ground_verification_modules_are_exact_mirrors():
    ha_path = (
        "custom_components/storm_tracker_v3/engine/"
        "ground_verification.py"
    )
    worker_path = (
        "storm_tracker_worker/tracking_core/engine/"
        "ground_verification.py"
    )

    with open(ha_path, "rb") as handle:
        ha_bytes = handle.read()
    with open(worker_path, "rb") as handle:
        worker_bytes = handle.read()

    assert ha_bytes == worker_bytes


def test_exact_confirmed_independent_wet_label_is_admitted():
    context = _context(
        (EventFrame("frame-1", "event-1", 10.0, ()),),
        (_label("label-1", "event-1", "frame-1", WetDryState.WET),),
    )

    admission = admit_ground_verification_case(
        _candidate(),
        expected_partition=_partition(context),
        truth_context=context,
        wet_dry_label_id="label-1",
    )

    assert admission.disposition is GroundAdmissionDisposition.ADMITTED
    assert admission.exclusion_reason is None
    assert admission.case is not None
    assert admission.case.truth_state is CandidateWetDryState.WET
    assert admission.case.label_source_id == "external-truth"


@pytest.mark.parametrize(
    ("candidate", "expected_reason"),
    (
        (
            _candidate(site_id="different-site"),
            GroundExclusionReason.LABEL_SITE_MISMATCH,
        ),
        (
            _candidate(start=9.999),
            GroundExclusionReason.LABEL_INTERVAL_MISMATCH,
        ),
        (
            _candidate(end=20.001),
            GroundExclusionReason.LABEL_INTERVAL_MISMATCH,
        ),
    ),
)
def test_site_and_interval_must_match_exactly(candidate, expected_reason):
    context = _context(
        (EventFrame("frame-1", "event-1", 10.0, ()),),
        (_label("label-1", "event-1", "frame-1", WetDryState.WET),),
    )

    admission = admit_ground_verification_case(
        candidate,
        expected_partition=_partition(context),
        truth_context=context,
        wet_dry_label_id="label-1",
    )

    assert admission.disposition is GroundAdmissionDisposition.EXCLUDED
    assert admission.exclusion_reason is expected_reason


@pytest.mark.parametrize(
    ("truth_state", "label_state", "expected_reason"),
    (
        (
            WetDryState.WET,
            LabelState.AMBIGUOUS,
            GroundExclusionReason.LABEL_NOT_CONFIRMED,
        ),
        (
            WetDryState.UNKNOWN,
            LabelState.UNKNOWN,
            GroundExclusionReason.TRUTH_UNKNOWN,
        ),
    ),
)
def test_ambiguous_and_unknown_truth_are_never_converted_to_dry(
    truth_state,
    label_state,
    expected_reason,
):
    context = _context(
        (EventFrame("frame-1", "event-1", 10.0, ()),),
        (
            _label(
                "label-1",
                "event-1",
                "frame-1",
                truth_state,
                label_state=label_state,
            ),
        ),
    )

    admission = admit_ground_verification_case(
        _candidate(state=CandidateWetDryState.DRY),
        expected_partition=_partition(context),
        truth_context=context,
        wet_dry_label_id="label-1",
    )

    assert admission.disposition is GroundAdmissionDisposition.EXCLUDED
    assert admission.exclusion_reason is expected_reason


def test_non_independent_source_is_explicitly_excluded():
    source = _source(independent=False)
    context = _context(
        (EventFrame("frame-1", "event-1", 10.0, ()),),
        (_label("label-1", "event-1", "frame-1", WetDryState.WET),),
        sources=(source,),
    )

    admission = admit_ground_verification_case(
        _candidate(),
        expected_partition=_partition(context),
        truth_context=context,
        wet_dry_label_id="label-1",
    )

    assert admission.exclusion_reason is (
        GroundExclusionReason.SOURCE_NOT_INDEPENDENT
    )


def test_candidate_dry_requires_a_dry_capable_candidate_capability():
    context = _context(
        (EventFrame("frame-1", "event-1", 10.0, ()),),
        (_label("label-1", "event-1", "frame-1", WetDryState.WET),),
    )

    admission = admit_ground_verification_case(
        _candidate(
            state=CandidateWetDryState.DRY,
            can_observe_absence=False,
        ),
        expected_partition=_partition(context),
        truth_context=context,
        wet_dry_label_id="label-1",
    )

    assert admission.exclusion_reason is (
        GroundExclusionReason.CANDIDATE_DRY_NOT_DRY_CAPABLE
    )


def test_partition_and_event_frame_identity_fail_closed():
    frames = (
        EventFrame("frame-1", "event-1", 10.0, ("shared-event",)),
        EventFrame("frame-2", "event-2", 10.0, ("shared-event",)),
    )
    context = _context(
        frames,
        (_label("label-1", "event-1", "frame-1", WetDryState.WET),),
    )
    correct = _partition(context)
    wrong = next(
        item for item in GroundVerificationPartition if item is not correct
    )

    partition_mismatch = admit_ground_verification_case(
        _candidate(),
        expected_partition=wrong,
        truth_context=context,
        wet_dry_label_id="label-1",
    )
    frame_mismatch = admit_ground_verification_case(
        _candidate(event_id="event-1", frame_id="frame-2"),
        expected_partition=correct,
        truth_context=context,
        wet_dry_label_id="label-1",
    )

    assert partition_mismatch.exclusion_reason is (
        GroundExclusionReason.PARTITION_MISMATCH
    )
    assert frame_mismatch.exclusion_reason is (
        GroundExclusionReason.EVENT_FRAME_MISMATCH
    )


def test_sensitivity_and_specificity_are_weighted_equally_per_event():
    specifications = (
        ("a1", "event-a", WetDryState.WET, CandidateWetDryState.WET),
        ("a2", "event-a", WetDryState.WET, CandidateWetDryState.WET),
        ("a3", "event-a", WetDryState.DRY, CandidateWetDryState.DRY),
        ("b1", "event-b", WetDryState.WET, CandidateWetDryState.WET),
        ("b2", "event-b", WetDryState.WET, CandidateWetDryState.DRY),
        ("b3", "event-b", WetDryState.WET, CandidateWetDryState.DRY),
        ("b4", "event-b", WetDryState.WET, CandidateWetDryState.DRY),
        ("b5", "event-b", WetDryState.DRY, CandidateWetDryState.WET),
        ("b6", "event-b", WetDryState.DRY, CandidateWetDryState.DRY),
    )
    frames = tuple(
        EventFrame(frame_id, event_id, 10.0, ("same-heldout-component",))
        for frame_id, event_id, _, _ in specifications
    )
    labels = tuple(
        _label(
            f"label-{frame_id}",
            event_id,
            frame_id,
            truth,
        )
        for frame_id, event_id, truth, _ in specifications
    )
    context = _context(frames, labels)
    admissions = tuple(
        _admit(
            _candidate(
                f"candidate-{frame_id}",
                event_id=event_id,
                frame_id=frame_id,
                state=prediction,
            ),
            context,
            f"label-{frame_id}",
        )
        for frame_id, event_id, _, prediction in specifications
    )

    report = verify_ground_admissions(admissions)
    metrics = report.overall.metrics

    assert metrics.true_positive_count == 3
    assert metrics.false_negative_count == 3
    assert metrics.true_negative_count == 2
    assert metrics.false_positive_count == 1
    assert metrics.sensitivity == pytest.approx((1.0 + 0.25) / 2.0)
    assert metrics.specificity == pytest.approx((1.0 + 0.5) / 2.0)
    assert metrics.sensitivity_event_count == 2
    assert metrics.specificity_event_count == 2
    assert metrics.event_weighted_decision_coverage == 1.0


def test_unknown_and_unavailable_predictions_remain_explicit():
    specifications = (
        ("f1", WetDryState.WET, CandidateWetDryState.UNKNOWN),
        ("f2", WetDryState.WET, CandidateWetDryState.UNAVAILABLE),
        ("f3", WetDryState.DRY, CandidateWetDryState.DRY),
    )
    frames = tuple(
        EventFrame(frame_id, "event-1", 10.0, ())
        for frame_id, _, _ in specifications
    )
    labels = tuple(
        _label(f"label-{frame_id}", "event-1", frame_id, truth)
        for frame_id, truth, _ in specifications
    )
    context = _context(frames, labels)
    admissions = tuple(
        _admit(
            _candidate(
                f"candidate-{frame_id}",
                frame_id=frame_id,
                state=prediction,
            ),
            context,
            f"label-{frame_id}",
        )
        for frame_id, _, prediction in specifications
    )

    metrics = verify_ground_admissions(admissions).overall.metrics

    assert metrics.unknown_prediction_count == 1
    assert metrics.unavailable_prediction_count == 1
    assert metrics.indeterminate_prediction_count == 2
    assert metrics.determinate_prediction_count == 1
    assert metrics.sensitivity is None
    assert metrics.event_weighted_wet_decision_coverage == 0.0
    assert metrics.specificity == 1.0
    assert metrics.event_weighted_decision_coverage == pytest.approx(1 / 3)


def test_provider_and_capability_strata_do_not_fabricate_missing_rates():
    frames = (
        EventFrame("frame-1", "event-1", 10.0, ()),
        EventFrame("frame-2", "event-1", 10.0, ()),
    )
    context = _context(
        frames,
        (
            _label("label-1", "event-1", "frame-1", WetDryState.WET),
            _label("label-2", "event-1", "frame-2", WetDryState.DRY),
        ),
    )
    admissions = (
        _admit(
            _candidate(
                "candidate-1",
                provider_id="wet-provider",
                capability_id="wet-trigger",
                can_observe_absence=False,
                state=CandidateWetDryState.WET,
            ),
            context,
            "label-1",
        ),
        _admit(
            _candidate(
                "candidate-2",
                frame_id="frame-2",
                provider_id="dry-provider",
                capability_id="gauge-state",
                state=CandidateWetDryState.DRY,
            ),
            context,
            "label-2",
        ),
    )

    report = verify_ground_admissions(admissions)
    by_provider = {
        item.provider_id: item for item in report.provider_strata
    }

    assert all(
        item.dimension is GroundStratumDimension.PROVIDER
        for item in report.provider_strata
    )
    assert by_provider["wet-provider"].metrics.sensitivity == 1.0
    assert by_provider["wet-provider"].metrics.specificity is None
    assert by_provider["dry-provider"].metrics.sensitivity is None
    assert by_provider["dry-provider"].metrics.specificity == 1.0
    assert len(report.capability_strata) == 2
    assert {
        (
            item.provider_id,
            item.capability_id,
            item.candidate_can_observe_absence,
        )
        for item in report.capability_strata
    } == {
        ("wet-provider", "wet-trigger", False),
        ("dry-provider", "gauge-state", True),
    }


def test_exclusions_are_counted_without_becoming_dry_cases():
    context = _context(
        (EventFrame("frame-1", "event-1", 10.0, ()),),
        (_label("label-1", "event-1", "frame-1", WetDryState.WET),),
    )
    partition = _partition(context)
    admissions = (
        admit_ground_verification_case(
            _candidate("candidate-admitted"),
            expected_partition=partition,
            truth_context=context,
            wet_dry_label_id="label-1",
        ),
        admit_ground_verification_case(
            _candidate("candidate-missing"),
            expected_partition=partition,
            truth_context=context,
            wet_dry_label_id="missing-label",
        ),
        admit_ground_verification_case(
            _candidate("candidate-wrong-site", site_id="other-site"),
            expected_partition=partition,
            truth_context=context,
            wet_dry_label_id="label-1",
        ),
    )

    report = verify_ground_admissions(admissions)

    assert report.attempt_count == 3
    assert report.admitted_count == 1
    assert report.excluded_count == 2
    assert dict(report.exclusions_by_reason) == {
        GroundExclusionReason.LABEL_MISSING: 1,
        GroundExclusionReason.LABEL_SITE_MISMATCH: 1,
    }
    assert report.overall.metrics.truth_dry_count == 0
    assert report.overall.metrics.false_positive_count == 0


def test_duplicate_site_and_cross_track_are_audits_not_truth_claims():
    context = _context(
        (EventFrame("frame-1", "event-1", 10.0, ()),),
        (
            _label("label-1", "event-1", "frame-1", WetDryState.WET),
            _label("label-2", "event-1", "frame-1", WetDryState.WET),
        ),
    )
    candidates = (
        _candidate(
            "candidate-1",
            decision_id="decision-1",
            provider_id="provider-1",
            track_id="track-1",
            state=CandidateWetDryState.WET,
        ),
        _candidate(
            "candidate-2",
            decision_id="decision-1",
            provider_id="provider-2",
            track_id="track-1",
            state=CandidateWetDryState.DRY,
        ),
        _candidate(
            "candidate-3",
            decision_id="decision-2",
            provider_id="provider-3",
            track_id="track-2",
            state=CandidateWetDryState.WET,
        ),
        _candidate(
            "candidate-4",
            decision_id="decision-3",
            provider_id="provider-4",
            track_id=None,
            state=CandidateWetDryState.UNKNOWN,
        ),
    )
    report = verify_ground_admissions(
        tuple(
            _admit(
                item,
                context,
                "label-2" if item.candidate_id == "candidate-2" else "label-1",
            )
            for item in candidates
        )
    )
    audit = report.exact_labeled_reuse_audit

    assert audit.exact_label_group_count == 1
    assert audit.duplicate_site_group_count == 1
    assert audit.duplicate_site_extra_candidate_count == 1
    assert audit.cross_provider_duplicate_site_group_count == 1
    assert audit.conflicting_determinate_duplicate_site_group_count == 1
    assert audit.cross_track_overlap_group_count == 1
    assert audit.cross_track_extra_assignment_count == 1
    assert audit.cross_track_missing_track_metadata_case_count == 1
    assert audit.cross_storm_correctness_evaluable is False
    assert audit.cross_storm_correctness_external_gate == (
        CROSS_STORM_CORRECTNESS_EXTERNAL_GATE
    )


def test_conflicting_external_truth_for_one_exact_unit_fails_closed():
    context = _context(
        (EventFrame("frame-1", "event-1", 10.0, ()),),
        (
            _label("label-dry", "event-1", "frame-1", WetDryState.DRY),
            _label("label-wet", "event-1", "frame-1", WetDryState.WET),
        ),
    )
    admissions = (
        _admit(
            _candidate(
                "candidate-dry",
                state=CandidateWetDryState.DRY,
            ),
            context,
            "label-dry",
        ),
        _admit(
            _candidate(
                "candidate-wet",
                state=CandidateWetDryState.WET,
            ),
            context,
            "label-wet",
        ),
    )

    with pytest.raises(ValueError, match="conflicting independent"):
        verify_ground_admissions(admissions)


def test_report_rejects_duplicate_candidates_and_mixed_algorithms():
    context = _context(
        (EventFrame("frame-1", "event-1", 10.0, ()),),
        (_label("label-1", "event-1", "frame-1", WetDryState.WET),),
    )
    first = _admit(_candidate(), context, "label-1")

    with pytest.raises(ValueError, match="candidate_id"):
        verify_ground_admissions((first, first))

    second_candidate = replace(
        first.candidate,
        candidate_id="candidate-2",
        algorithm_version="different-algorithm",
    )
    second = _admit(second_candidate, context, "label-1")
    with pytest.raises(ValueError, match="cross evaluation"):
        verify_ground_admissions((first, second))
