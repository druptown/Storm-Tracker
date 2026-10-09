from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.event_probability_calibration import (
    CalibrationInterpolation,
    CalibrationPartition,
    CalibrationPolicy,
    CalibrationScope,
    CalibrationWeighting,
    InsufficientCalibrationSupport,
    PassageCalibrationCase,
    ProbabilityCalibrationQuery,
    calibrate_query,
    evaluate_calibrator_held_out,
    fit_event_balanced_isotonic_calibrator,
)


CORPUS = "a" * 64
SPLIT = "b" * 64
CANDIDATE = "c" * 64
TRUTH = "d" * 64


def _scope(
    *,
    scope_id: str = "exact-belgium-kmi-10m-rain",
    provider_id: str | None = "kmi",
    region_id: str | None = "belgium",
) -> CalibrationScope:
    return CalibrationScope(
        scope_id=scope_id,
        provider_id=provider_id,
        region_id=region_id,
        lead_bin_id="lead-10m",
        threshold_id="rain-present",
    )


def _policy(
    *,
    minimum_events: int = 2,
    minimum_positive_events: int = 1,
    minimum_negative_events: int = 1,
) -> CalibrationPolicy:
    return CalibrationPolicy(
        minimum_events=minimum_events,
        minimum_positive_events=minimum_positive_events,
        minimum_negative_events=minimum_negative_events,
        weighting=CalibrationWeighting.EVENT_BALANCED,
        interpolation=CalibrationInterpolation.CLIPPED_LINEAR,
        policy_id="explicit-test-policy-v1",
    )


def _case(
    index: int,
    *,
    event_id: str,
    probability: float,
    occurred: bool,
    partition: CalibrationPartition = CalibrationPartition.TRAIN,
    provider_id: str = "kmi",
    region_id: str = "belgium",
) -> PassageCalibrationCase:
    return PassageCalibrationCase(
        case_id=f"case-{partition.value}-{index:03d}",
        event_id=event_id,
        frame_id=f"frame-{partition.value}-{index:03d}",
        partition=partition,
        raw_probability=probability,
        passage_occurred=occurred,
        provider_id=provider_id,
        region_id=region_id,
        lead_bin_id="lead-10m",
        threshold_id="rain-present",
        corpus_sha256=CORPUS,
        split_sha256=SPLIT,
        candidate_artifact_sha256=CANDIDATE,
        truth_artifact_sha256=TRUTH,
        truth_label_confirmed=True,
        truth_source_eligible=True,
    )


def _training_cases() -> tuple[PassageCalibrationCase, ...]:
    return (
        _case(1, event_id="event-dry", probability=0.1, occurred=False),
        _case(2, event_id="event-wet", probability=0.9, occurred=True),
    )


def test_fit_is_deterministic_monotone_and_clipped_linear():
    cases = _training_cases()
    first = fit_event_balanced_isotonic_calibrator(
        cases,
        scope=_scope(),
        policy=_policy(),
    )
    second = fit_event_balanced_isotonic_calibrator(
        tuple(reversed(cases)),
        scope=_scope(),
        policy=_policy(),
    )

    assert first == second
    assert first.calibrate(0.1) == (0.0, False)
    assert first.calibrate(0.5) == pytest.approx((0.5, False))
    assert first.calibrate(0.9) == (1.0, False)
    assert first.calibrate(0.0) == (0.0, True)
    assert first.calibrate(1.0) == (1.0, True)
    assert first.calibration_version.startswith("calibration:")


def test_pool_adjacent_violators_removes_decreasing_probability_fit():
    cases = (
        _case(1, event_id="event-wet", probability=0.1, occurred=True),
        _case(2, event_id="event-dry", probability=0.9, occurred=False),
    )
    model = fit_event_balanced_isotonic_calibrator(
        cases,
        scope=_scope(),
        policy=_policy(),
    )

    assert tuple(knot.calibrated_probability for knot in model.knots) == (
        0.5,
        0.5,
    )


def test_each_meteorological_event_has_equal_total_fit_weight():
    many_rows = tuple(
        _case(
            index,
            event_id="event-many-dry",
            probability=0.5,
            occurred=False,
        )
        for index in range(1, 11)
    )
    one_row = _case(
        20,
        event_id="event-one-wet",
        probability=0.5,
        occurred=True,
    )
    model = fit_event_balanced_isotonic_calibrator(
        many_rows + (one_row,),
        scope=_scope(),
        policy=_policy(),
    )

    assert len(model.knots) == 1
    assert model.knots[0].calibrated_probability == pytest.approx(0.5)
    assert model.knots[0].event_balanced_weight == pytest.approx(2.0)
    assert model.knots[0].case_count == 11


def test_support_is_explicit_and_fails_closed():
    with pytest.raises(
        InsufficientCalibrationSupport,
        match="positive events 0/1",
    ):
        fit_event_balanced_isotonic_calibrator(
            (
                _case(
                    1,
                    event_id="event-dry-a",
                    probability=0.1,
                    occurred=False,
                ),
                _case(
                    2,
                    event_id="event-dry-b",
                    probability=0.2,
                    occurred=False,
                ),
            ),
            scope=_scope(),
            policy=_policy(),
        )


def test_fit_rejects_heldout_cases_scope_mismatch_and_mixed_evidence():
    heldout = replace(
        _training_cases()[0],
        partition=CalibrationPartition.TEST,
        case_id="heldout-case",
    )
    with pytest.raises(ValueError, match="training cases only"):
        fit_event_balanced_isotonic_calibrator(
            (heldout, _training_cases()[1]),
            scope=_scope(),
            policy=_policy(),
        )

    wrong_region = replace(_training_cases()[0], region_id="new-york")
    with pytest.raises(ValueError, match="outside"):
        fit_event_balanced_isotonic_calibrator(
            (wrong_region, _training_cases()[1]),
            scope=_scope(),
            policy=_policy(),
        )

    wrong_split = replace(_training_cases()[0], split_sha256="e" * 64)
    with pytest.raises(ValueError, match="one immutable corpus and split"):
        fit_event_balanced_isotonic_calibrator(
            (wrong_split, _training_cases()[1]),
            scope=CalibrationScope(
                scope_id="pooled-test",
                provider_id=None,
                region_id=None,
                lead_bin_id="lead-10m",
                threshold_id="rain-present",
            ),
            policy=_policy(),
        )


def test_truth_must_be_confirmed_independent_and_artifact_distinct():
    base = _training_cases()[0]
    with pytest.raises(ValueError, match="confirmed"):
        replace(base, truth_label_confirmed=False)
    with pytest.raises(ValueError, match="independently eligible"):
        replace(base, truth_source_eligible=False)
    with pytest.raises(ValueError, match="artifacts must differ"):
        replace(base, truth_artifact_sha256=CANDIDATE)


def test_caller_controls_exact_then_pooled_selection_order():
    exact = fit_event_balanced_isotonic_calibrator(
        _training_cases(),
        scope=_scope(),
        policy=_policy(),
    )
    pooled_scope = CalibrationScope(
        scope_id="pooled-region",
        provider_id=None,
        region_id="belgium",
        lead_bin_id="lead-10m",
        threshold_id="rain-present",
    )
    pooled = fit_event_balanced_isotonic_calibrator(
        _training_cases(),
        scope=pooled_scope,
        policy=_policy(),
    )
    query = ProbabilityCalibrationQuery(
        query_id="forecast-query-1",
        raw_probability=0.5,
        provider_id="kmi",
        region_id="belgium",
        lead_bin_id="lead-10m",
        threshold_id="rain-present",
    )

    exact_first = calibrate_query(query, (exact, pooled))
    pooled_first = calibrate_query(query, (pooled, exact))

    assert exact_first.scope_id == exact.scope.scope_id
    assert exact_first.selection_rank == 0
    assert pooled_first.scope_id == pooled.scope.scope_id
    assert pooled_first.selection_rank == 0
    assert exact_first.selection_order == (
        exact.calibration_version,
        pooled.calibration_version,
    )


def test_no_implicit_matching_calibrator_exists():
    model = fit_event_balanced_isotonic_calibrator(
        _training_cases(),
        scope=_scope(),
        policy=_policy(),
    )
    query = ProbabilityCalibrationQuery(
        query_id="forecast-query-new-york",
        raw_probability=0.5,
        provider_id="noaa",
        region_id="new-york",
        lead_bin_id="lead-10m",
        threshold_id="rain-present",
    )
    with pytest.raises(LookupError, match="no caller-supplied"):
        calibrate_query(query, (model,))


def test_heldout_evaluation_is_event_balanced_and_not_a_release_gate():
    model = fit_event_balanced_isotonic_calibrator(
        _training_cases(),
        scope=_scope(),
        policy=_policy(),
    )
    heldout = (
        _case(
            10,
            event_id="heldout-dry",
            probability=0.2,
            occurred=False,
            partition=CalibrationPartition.TEST,
        ),
        _case(
            11,
            event_id="heldout-wet",
            probability=0.8,
            occurred=True,
            partition=CalibrationPartition.TEST,
        ),
    )
    report = evaluate_calibrator_held_out(
        model,
        heldout,
        partition=CalibrationPartition.TEST,
    )

    assert report.event_count == 2
    assert report.positive_event_count == 1
    assert report.negative_event_count == 1
    assert report.raw_event_balanced_brier == pytest.approx(0.04)
    assert report.calibrated_event_balanced_brier == pytest.approx(0.015625)
    assert report.calibrated_minus_raw_brier == pytest.approx(-0.024375)
    assert len(report.evidence_sha256) == 64


def test_heldout_evaluation_rejects_train_partition_and_event_leakage():
    model = fit_event_balanced_isotonic_calibrator(
        _training_cases(),
        scope=_scope(),
        policy=_policy(),
    )
    with pytest.raises(ValueError, match="validation or test"):
        evaluate_calibrator_held_out(
            model,
            _training_cases(),
            partition=CalibrationPartition.TRAIN,
        )

    leaking = (
        _case(
            20,
            event_id="event-wet",
            probability=0.8,
            occurred=True,
            partition=CalibrationPartition.TEST,
        ),
    )
    with pytest.raises(ValueError, match="events overlap"):
        evaluate_calibrator_held_out(
            model,
            leaking,
            partition=CalibrationPartition.TEST,
        )


def test_ha_and_worker_calibration_modules_are_exact_mirrors():
    root = Path(__file__).resolve().parents[1]
    ha = (
        root
        / "custom_components"
        / "storm_tracker_v3"
        / "engine"
        / "event_probability_calibration.py"
    )
    worker = (
        root
        / "storm_tracker_worker"
        / "tracking_core"
        / "engine"
        / "event_probability_calibration.py"
    )
    assert ha.read_bytes() == worker.read_bytes()
