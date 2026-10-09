from __future__ import annotations

import ast
from dataclasses import replace
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.forecast_verification import (
    BaselinePassageForecast,
    BootstrapConfig,
    FieldVerificationCase,
    FssConfig,
    MaskedVerificationField,
    PassageVerificationCase,
    ScalarEnsembleVerificationCase,
    ThresholdOperator,
    VerificationPartition,
    build_field_verification_case,
    build_passage_verification_case,
    verify_conditional_eta,
    verify_masked_fss,
    verify_passage_probabilities,
    verify_scalar_ensemble,
)
from storm_tracker_worker.tracking_core.engine.tracker_forecast_contract import (
    ConditionalDistribution,
    ConditionalDistributionKind,
    DistributionEvidenceBasis,
    DistributionInterval,
    ForecastState,
    PassageProbabilityEvidence,
    ProbabilityEvidenceBasis,
    QuantilePoint,
    TrackerForecast,
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
    PassageLabel,
    PrecipitationFieldLabel,
    build_event_held_out_split,
)


def _bootstrap(*, minimum_events: int = 2, seed: str = "verification-seed") -> BootstrapConfig:
    return BootstrapConfig(
        seed=seed,
        replicates=80,
        lower_probability=0.1,
        upper_probability=0.9,
        minimum_events=minimum_events,
    )


def _eta(
    median: float,
    *,
    probabilities: tuple[float, float, float] = (0.1, 0.5, 0.9),
) -> ConditionalDistribution:
    return ConditionalDistribution(
        distribution_id=f"eta-{median}-{probabilities[0]}",
        kind=ConditionalDistributionKind.CONDITIONAL_ETA,
        quantity="eta_after_reference",
        unit="s",
        basis=DistributionEvidenceBasis.ENSEMBLE_MEMBERS,
        sample_size=10,
        source_id="ensemble-run",
        source_algorithm_version="ensemble-v1",
        quantiles=(
            QuantilePoint(probabilities[0], max(0.0, median - 200.0)),
            QuantilePoint(probabilities[1], median),
            QuantilePoint(probabilities[2], median + 200.0),
        ),
        calibration_version=None,
    )


def _intensity() -> ConditionalDistribution:
    return ConditionalDistribution(
        distribution_id="local-intensity",
        kind=ConditionalDistributionKind.CONDITIONAL_LOCAL_INTENSITY,
        quantity="rain_rate_at_tracker",
        unit="mm h-1",
        basis=DistributionEvidenceBasis.ENSEMBLE_MEMBERS,
        sample_size=10,
        source_id="ensemble-run",
        source_algorithm_version="ensemble-v1",
        quantiles=(
            QuantilePoint(0.1, 0.2),
            QuantilePoint(0.5, 1.0),
            QuantilePoint(0.9, 4.0),
        ),
        calibration_version=None,
    )


def _forecast(
    case_id: str,
    probability: float,
    *,
    median_eta: float = 1_200.0,
    eta_probabilities: tuple[float, float, float] = (0.1, 0.5, 0.9),
    tracker_id: str = "tracker-a",
) -> TrackerForecast:
    passage_count = round(probability * 10)
    assert passage_count / 10 == probability
    has_passage = probability > 0.0
    eta = (
        _eta(median_eta, probabilities=eta_probabilities)
        if has_passage
        else None
    )
    interval = (
        DistributionInterval(
            lower_probability=eta_probabilities[0],
            upper_probability=eta_probabilities[2],
            lower_value=max(0.0, median_eta - 200.0),
            upper_value=median_eta + 200.0,
        )
        if has_passage
        else None
    )
    return TrackerForecast(
        forecast_id=f"candidate-{case_id}",
        tracker_id=tracker_id,
        state=ForecastState.DETERMINATE,
        analysis_grid_id="metric-grid",
        tracker_x_m=100_000.0,
        tracker_y_m=200_000.0,
        reference_valid_at=1_000.0,
        forecast_valid_at=1_060.0,
        lead_time_seconds=60.0,
        horizon_seconds=3_600.0,
        input_track_ids=(f"track-{case_id}",),
        input_field_ids=("field-old", "field-current"),
        algorithm_version="candidate-algorithm-v1",
        calibration_version=None,
        passage_probability=probability,
        no_passage_probability=1.0 - probability,
        probability_evidence=PassageProbabilityEvidence(
            evidence_id=f"evidence-{case_id}",
            basis=ProbabilityEvidenceBasis.ENSEMBLE_FREQUENCY,
            sample_size=10,
            source_id=f"ensemble-{case_id}",
            source_algorithm_version="ensemble-v1",
            passage_count=passage_count,
            calibration_version=None,
        ),
        conditional_eta_distribution=eta,
        conditional_eta_interval=interval,
        local_intensity_distribution=_intensity() if has_passage else None,
        indeterminate_reasons=(),
    )


def _baseline(
    case_id: str,
    event_id: str,
    probability: float = 0.5,
    *,
    partition: VerificationPartition = VerificationPartition.TEST,
    stratum: str = "primary-test-stratum",
) -> BaselinePassageForecast:
    return BaselinePassageForecast(
        baseline_forecast_id=f"baseline-{case_id}",
        evaluation_stratum_id=stratum,
        event_id=event_id,
        frame_id=f"frame-{case_id}",
        partition=partition,
        tracker_id="tracker-a",
        reference_valid_at=1_000.0,
        horizon_seconds=3_600.0,
        passage_probability=probability,
        no_passage_probability=1.0 - probability,
        algorithm_version="explicit-baseline-v1",
        input_field_ids=("baseline-field",),
    )


def _case(
    case_id: str,
    event_id: str,
    probability: float,
    outcome: bool,
    *,
    baseline_probability: float = 0.5,
    median_eta: float = 1_200.0,
    actual_eta: float = 1_200.0,
    eta_probabilities: tuple[float, float, float] = (0.1, 0.5, 0.9),
    stratum: str = "primary-test-stratum",
) -> PassageVerificationCase:
    forecast = _forecast(
        case_id,
        probability,
        median_eta=median_eta,
        eta_probabilities=eta_probabilities,
    )
    baseline = _baseline(
        case_id,
        event_id,
        baseline_probability,
        stratum=stratum,
    )
    return PassageVerificationCase(
        case_id=case_id,
        evaluation_stratum_id=stratum,
        event_id=event_id,
        frame_id=f"frame-{case_id}",
        partition=VerificationPartition.TEST,
        forecast=forecast,
        baseline=baseline,
        label_id=f"label-{case_id}",
        label_source_id="independent-source",
        label_state="confirmed",
        truth_source_eligible=True,
        passage_occurred=outcome,
        passage_time_utc=1_000.0 + actual_eta if outcome else None,
    )


def _probability_report(cases, *, minimum_events=2, seed="verification-seed"):
    return verify_passage_probabilities(
        cases,
        reliability_bin_edges=(0.0, 0.25, 0.75, 1.0),
        decision_threshold=0.5,
        bootstrap=_bootstrap(minimum_events=minimum_events, seed=seed),
    )


def _ensemble_case(
    case_id: str,
    event_id: str,
    members: tuple[float, ...],
    observed: float,
    *,
    unit_id: str = "mm h-1",
) -> ScalarEnsembleVerificationCase:
    return ScalarEnsembleVerificationCase(
        case_id=case_id,
        evaluation_stratum_id="local-intensity-test",
        event_id=event_id,
        frame_id=f"frame-{case_id}",
        partition=VerificationPartition.TEST,
        ensemble_id=f"ensemble-{case_id}",
        valid_at=2_000.0,
        quantity_id="rain_rate_at_tracker",
        unit_id=unit_id,
        member_ids=tuple(f"member-{index}" for index in range(len(members))),
        member_values=members,
        members_exchangeable=True,
        member_generation_algorithm_version="ensemble-nowcast-v1",
        calibration_version=None,
        candidate_artifact_sha256=None,
        observed_value=observed,
        truth_artifact_sha256="c" * 64,
        label_id=f"label-{case_id}",
        label_source_id="independent-gauge-analysis",
        label_state="confirmed",
        truth_source_eligible=True,
    )


def test_brier_and_bss_are_equal_event_weighted_not_frame_weighted():
    cases = (
        _case("a1", "event-a", 0.0, False),
        _case("a2", "event-a", 0.0, False),
        _case("b1", "event-b", 1.0, False),
        _case("c1", "event-c", 1.0, True),
    )

    report = _probability_report(cases, minimum_events=3)

    assert report.case_count == 4
    assert report.event_count == 3
    assert report.candidate_brier.point_estimate == pytest.approx(1.0 / 3.0)
    assert report.baseline_brier.point_estimate == pytest.approx(0.25)
    assert report.brier_skill_score.point_estimate == pytest.approx(-1.0 / 3.0)
    assert report.classification.false_passage_count == 1
    assert report.classification.false_passage_rate == pytest.approx(0.5)
    assert report.classification.missed_passage_count == 0


def test_reliability_keeps_empty_bins_and_sharpness_mass_is_explicit():
    cases = (
        _case("a", "event-a", 0.0, False),
        _case("b", "event-b", 1.0, False),
        _case("c", "event-c", 1.0, True),
    )

    report = _probability_report(cases, minimum_events=3)

    low, empty, high = report.reliability_bins
    assert low.sample_count == 1
    assert low.forecast_mean == 0.0
    assert low.outcome_frequency == 0.0
    assert empty.sample_count == 0
    assert empty.event_count == 0
    assert empty.forecast_mean is None
    assert empty.outcome_frequency is None
    assert high.sample_count == 2
    assert high.forecast_mean == 1.0
    assert high.outcome_frequency == 0.5
    assert sum(
        item.event_weighted_mass for item in report.sharpness.bins
    ) == pytest.approx(1.0)
    assert report.sharpness.event_weighted_mean_probability == pytest.approx(
        2.0 / 3.0
    )


def test_false_and_missed_passage_counts_use_explicit_decision_threshold():
    cases = (
        _case("a", "event-a", 0.8, False),
        _case("b", "event-b", 0.2, False),
        _case("c", "event-c", 0.2, True),
        _case("d", "event-d", 0.8, True),
    )

    summary = _probability_report(cases, minimum_events=4).classification

    assert summary.false_passage_count == 1
    assert summary.missed_passage_count == 1
    assert summary.true_positive_count == 1
    assert summary.true_negative_count == 1
    assert summary.false_passage_rate == pytest.approx(0.5)
    assert summary.missed_passage_rate == pytest.approx(0.5)


def test_passage_roc_auc_is_event_weighted_and_keeps_empirical_endpoints():
    perfect = (
        _case("a", "event-a", 0.1, False),
        _case("b", "event-b", 0.9, True),
        _case("c", "event-c", 0.2, False),
        _case("d", "event-d", 0.8, True),
    )
    report = _probability_report(perfect, minimum_events=4).roc

    assert report.area_under_curve == pytest.approx(1.0)
    assert report.points[0].threshold is None
    assert (
        report.points[0].false_positive_rate,
        report.points[0].true_positive_rate,
    ) == (0.0, 0.0)
    assert (
        report.points[-1].false_positive_rate,
        report.points[-1].true_positive_rate,
    ) == (1.0, 1.0)
    assert report.passage_event_count == 2
    assert report.no_passage_event_count == 2

    reversed_skill = (
        _case("e", "event-e", 0.9, False),
        _case("f", "event-f", 0.1, True),
    )
    assert _probability_report(reversed_skill).roc.area_under_curve == pytest.approx(
        0.0
    )


def test_event_bootstrap_is_deterministic_and_input_order_independent():
    cases = (
        _case("a", "event-a", 0.1, False),
        _case("b", "event-b", 0.2, True),
        _case("c", "event-c", 0.7, False),
        _case("d", "event-d", 0.8, True),
    )

    first = _probability_report(cases, minimum_events=4, seed="fixed-seed")
    second = _probability_report(
        reversed(cases),
        minimum_events=4,
        seed="fixed-seed",
    )
    changed_seed = _probability_report(
        cases,
        minimum_events=4,
        seed="other-seed",
    )

    assert first == second
    assert (
        first.candidate_brier.seed_sha256
        != changed_seed.candidate_brier.seed_sha256
    )
    assert first.candidate_brier.replicates == 80
    assert first.candidate_brier.event_count == 4


def test_bss_fails_closed_on_zero_baseline_denominator():
    cases = (
        _case("a", "event-a", 0.2, False, baseline_probability=0.0),
        _case("b", "event-b", 0.8, True, baseline_probability=1.0),
    )

    with pytest.raises(ValueError, match="denominator is zero"):
        _probability_report(cases)


def test_passage_verification_rejects_insufficient_events_and_mixed_strata():
    cases = (
        _case("a", "event-a", 0.2, False),
        _case("b", "event-b", 0.8, True),
    )
    with pytest.raises(ValueError, match="caller requires 3"):
        _probability_report(cases, minimum_events=3)

    mixed = (cases[0], _case("b2", "event-b", 0.8, True, stratum="other"))
    with pytest.raises(ValueError, match="cross evaluation strata"):
        _probability_report(mixed)


def test_probability_verification_rejects_bad_bins_and_one_sided_truth():
    cases = (
        _case("a", "event-a", 0.2, False),
        _case("b", "event-b", 0.3, False),
    )
    with pytest.raises(ValueError, match="both outcome classes"):
        _probability_report(cases)
    with pytest.raises(ValueError, match=r"cover exactly \[0, 1\]"):
        verify_passage_probabilities(
            (
                _case("c", "event-c", 0.2, False),
                _case("d", "event-d", 0.8, True),
            ),
            reliability_bin_edges=(0.1, 1.0),
            decision_threshold=0.5,
            bootstrap=_bootstrap(),
        )


def test_eta_metrics_are_computed_only_for_actual_passages():
    cases = (
        _case(
            "a",
            "event-a",
            0.6,
            True,
            median_eta=1_000.0,
            actual_eta=1_200.0,
        ),
        _case(
            "b",
            "event-b",
            0.6,
            True,
            median_eta=1_400.0,
            actual_eta=1_200.0,
        ),
        _case("c", "event-c", 0.6, False, median_eta=3_000.0),
    )

    report = verify_conditional_eta(cases, bootstrap=_bootstrap())

    assert report.passage_case_count == 2
    assert report.passage_event_count == 2
    assert report.excluded_no_passage_case_count == 1
    assert report.median_mae_seconds.point_estimate == 200.0
    assert report.median_bias_seconds.point_estimate == 0.0
    losses = {
        item.probability: item.loss_seconds.point_estimate
        for item in report.quantile_pinball
    }
    assert losses == pytest.approx({0.1: 20.0, 0.5: 100.0, 0.9: 20.0})
    assert report.interval_coverage.point_estimate == 1.0
    assert report.mean_interval_width_seconds.point_estimate == 400.0


def test_eta_fails_closed_without_distribution_or_enough_passage_events():
    missing_eta = (
        _case("a", "event-a", 0.0, True),
        _case("b", "event-b", 0.6, True),
    )
    with pytest.raises(ValueError, match="without conditional ETA"):
        verify_conditional_eta(missing_eta, bootstrap=_bootstrap())

    only_one_passage = (
        _case("c", "event-c", 0.6, True),
        _case("d", "event-d", 0.6, False),
    )
    with pytest.raises(ValueError, match="conditional ETA has 1 events"):
        verify_conditional_eta(only_one_passage, bootstrap=_bootstrap())


def test_eta_rejects_mixed_quantile_definitions():
    cases = (
        _case("a", "event-a", 0.6, True),
        _case(
            "b",
            "event-b",
            0.6,
            True,
            eta_probabilities=(0.2, 0.5, 0.8),
        ),
    )
    with pytest.raises(ValueError, match="definitions differ"):
        verify_conditional_eta(cases, bootstrap=_bootstrap())


def test_complete_scalar_ensemble_crps_and_rank_histogram_are_exact():
    cases = (
        _ensemble_case("a", "event-a", (0.0, 1.0), 0.5),
        _ensemble_case("b", "event-b", (0.0, 1.0), 0.5),
    )

    report = verify_scalar_ensemble(cases, bootstrap=_bootstrap())

    assert report.quantity_id == "rain_rate_at_tracker"
    assert report.unit_id == "mm h-1"
    assert report.member_count == 2
    assert report.case_count == 2
    assert report.event_count == 2
    assert report.ensemble_mean_mae.point_estimate == 0.0
    assert report.ensemble_mean_bias.point_estimate == 0.0
    assert report.crps.point_estimate == pytest.approx(0.25)
    assert tuple(item.raw_fractional_count for item in report.rank_histogram) == (
        0.0,
        2.0,
        0.0,
    )
    assert tuple(item.event_weighted_mass for item in report.rank_histogram) == (
        0.0,
        1.0,
        0.0,
    )


def test_rank_histogram_distributes_exact_ties_without_randomness():
    cases = (
        _ensemble_case("a", "event-a", (1.0, 1.0), 1.0),
        _ensemble_case("b", "event-b", (1.0, 1.0), 1.0),
    )

    report = verify_scalar_ensemble(
        reversed(cases),
        bootstrap=_bootstrap(seed="tie-seed"),
    )

    assert report.crps.point_estimate == 0.0
    assert tuple(
        item.event_weighted_mass for item in report.rank_histogram
    ) == pytest.approx((1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0))
    assert sum(
        item.event_weighted_mass for item in report.rank_histogram
    ) == pytest.approx(1.0)


def test_scalar_ensemble_verification_is_event_weighted_and_order_independent():
    cases = (
        _ensemble_case("a1", "event-a", (0.0, 0.0), 0.0),
        _ensemble_case("a2", "event-a", (0.0, 0.0), 0.0),
        _ensemble_case("b", "event-b", (4.0, 4.0), 1.0),
    )

    first = verify_scalar_ensemble(cases, bootstrap=_bootstrap())
    second = verify_scalar_ensemble(reversed(cases), bootstrap=_bootstrap())

    assert first == second
    assert first.ensemble_mean_mae.point_estimate == pytest.approx(1.5)
    assert first.ensemble_mean_bias.point_estimate == pytest.approx(1.5)
    assert first.crps.point_estimate == pytest.approx(1.5)


def test_scalar_ensemble_verification_fails_closed_on_invalid_evidence():
    valid = _ensemble_case("a", "event-a", (0.0, 1.0), 0.5)
    with pytest.raises(ValueError, match="exchangeable"):
        replace(valid, members_exchangeable=False)
    with pytest.raises(ValueError, match="artifacts must differ"):
        replace(
            valid,
            candidate_artifact_sha256=valid.truth_artifact_sha256,
        )
    with pytest.raises(ValueError, match="at least two members"):
        replace(valid, member_ids=("only",), member_values=(1.0,))

    mixed = (
        valid,
        _ensemble_case(
            "b",
            "event-b",
            (0.0, 1.0),
            0.5,
            unit_id="dBZ",
        ),
    )
    with pytest.raises(ValueError, match="cross evaluation strata"):
        verify_scalar_ensemble(mixed, bootstrap=_bootstrap())
    with pytest.raises(ValueError, match="caller requires 2"):
        verify_scalar_ensemble((valid,), bootstrap=_bootstrap())


def _source(*, source_id: str = "external-source") -> ExternalLabelSource:
    return ExternalLabelSource(
        source_id=source_id,
        source_kind=ExternalSourceKind.IN_SITU_INSTRUMENT,
        producer="independent-test-producer",
        dataset_version="external-v1",
        methodology_id="external-method-v1",
        source_artifact_sha256="a" * 64,
        candidate_independent=True,
        blinded_to_candidate=False,
    )


def _truth_context(label: ExternalLabel) -> ExternalTruthContext:
    frames = (EventFrame(label.frame_id, label.event_id, 1_000.0, ()),)
    split = build_event_held_out_split(
        frames,
        EventSplitPolicy(
            policy_id="verification-policy",
            hash_salt="verification-salt",
            train_basis_points=6000,
            validation_basis_points=2000,
            test_basis_points=2000,
        ),
    )
    return ExternalTruthContext(
        split=split,
        labels=ExternalLabelCorpus(
            sources=(_source(source_id=label.source_id),),
            labels=(label,),
        ),
    )


def test_passage_case_builder_uses_current_validation_corpus_contract():
    label = ExternalLabel(
        label_id="passage-label",
        label_kind=ExternalLabelKind.PASSAGE,
        source_id="external-source",
        event_id="event-a",
        frame_id="frame-a",
        state=LabelState.CONFIRMED,
        payload=PassageLabel(
            tracker_id="tracker-a",
            window_start_utc=1_000.0,
            window_end_utc=4_600.0,
            occurred=True,
            passage_time_utc=2_200.0,
            time_uncertainty_s=60.0,
        ),
    )
    context = _truth_context(label)
    partition = VerificationPartition(
        context.split.partition_for_event("event-a").value
    )
    baseline = _baseline(
        "a",
        "event-a",
        partition=partition,
    )
    baseline = replace(baseline, frame_id="frame-a")

    case = build_passage_verification_case(
        case_id="a",
        evaluation_stratum_id="primary-test-stratum",
        event_id="event-a",
        frame_id="frame-a",
        expected_partition=partition,
        forecast=_forecast("a", 0.6),
        baseline=baseline,
        truth_context=context,
        passage_label_id="passage-label",
    )

    assert case.passage_occurred is True
    assert case.passage_time_utc == 2_200.0
    assert case.truth_source_eligible


def test_internal_storm_tracker_fusion_cannot_masquerade_as_external_truth():
    label = ExternalLabel(
        label_id="internal-label", label_kind=ExternalLabelKind.PASSAGE,
        source_id="fusion:storm_tracker", event_id="event-a", frame_id="frame-a",
        state=LabelState.CONFIRMED,
        payload=PassageLabel(
            tracker_id="tracker-a", window_start_utc=1_000.0,
            window_end_utc=4_600.0, occurred=True,
            passage_time_utc=2_200.0, time_uncertainty_s=60.0,
        ),
    )
    frames = (EventFrame("frame-a", "event-a", 1_000.0, ()),)
    split = build_event_held_out_split(
        frames,
        EventSplitPolicy(
            policy_id="verification-policy", hash_salt="verification-salt",
            train_basis_points=6000, validation_basis_points=2000,
            test_basis_points=2000,
        ),
    )
    source = ExternalLabelSource(
        source_id="fusion:storm_tracker",
        source_kind=ExternalSourceKind.INDEPENDENT_REFERENCE_ANALYSIS,
        producer="storm_tracker_fused_radar", dataset_version="v1",
        methodology_id="internal-fusion", source_artifact_sha256="b" * 64,
        candidate_independent=True, blinded_to_candidate=False,
    )
    context = ExternalTruthContext(
        split=split,
        labels=ExternalLabelCorpus(sources=(source,), labels=(label,)),
    )
    with pytest.raises(ValueError, match="internal fused radar"):
        build_passage_verification_case(
            case_id="case-a", evaluation_stratum_id="primary-test-stratum",
            event_id="event-a", frame_id="frame-a",
            expected_partition=VerificationPartition(
                split.partition_for_event("event-a").value
            ),
            forecast=_forecast("a", 0.8),
            baseline=_baseline(
                "a", "event-a", 0.5,
                partition=VerificationPartition(
                    split.partition_for_event("event-a").value
                ),
            ),
            truth_context=context, passage_label_id="internal-label",
        )


def test_passage_case_builder_rejects_ambiguous_or_missing_label():
    ambiguous = ExternalLabel(
        label_id="ambiguous-passage",
        label_kind=ExternalLabelKind.PASSAGE,
        source_id="external-source",
        event_id="event-a",
        frame_id="frame-a",
        state=LabelState.AMBIGUOUS,
        payload=PassageLabel(
            "tracker-a",
            1_000.0,
            4_600.0,
            True,
            2_200.0,
            None,
        ),
    )
    context = _truth_context(ambiguous)
    partition = VerificationPartition(
        context.split.partition_for_event("event-a").value
    )
    baseline = replace(
        _baseline("a", "event-a", partition=partition),
        frame_id="frame-a",
    )
    kwargs = {
        "case_id": "a",
        "evaluation_stratum_id": "primary-test-stratum",
        "event_id": "event-a",
        "frame_id": "frame-a",
        "expected_partition": partition,
        "forecast": _forecast("a", 0.6),
        "baseline": baseline,
        "truth_context": context,
    }
    with pytest.raises(ValueError, match="ambiguous"):
        build_passage_verification_case(
            **kwargs,
            passage_label_id="ambiguous-passage",
        )
    with pytest.raises(ValueError, match="missing"):
        build_passage_verification_case(
            **kwargs,
            passage_label_id="absent-label",
        )


def _grid() -> AnalysisGrid:
    return AnalysisGrid(
        grid_id="fss-grid",
        crs="EPSG:31370",
        origin_x_m=0.0,
        origin_y_m=3_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=3,
        height=3,
    )


def _field(
    field_id: str,
    values: tuple[tuple[float, ...], ...],
    *,
    mask: tuple[tuple[bool, ...], ...] | None = None,
    unit: str = "mm h-1",
    artifact: str | None = None,
    grid: AnalysisGrid | None = None,
) -> MaskedVerificationField:
    grid = grid or _grid()
    mask = mask or tuple(
        tuple(True for _ in range(grid.width)) for _ in range(grid.height)
    )
    return MaskedVerificationField(
        field_id=field_id,
        grid=grid,
        valid_at=2_000.0,
        variable_id="rain_rate",
        unit_id=unit,
        values=values,
        valid_mask=mask,
        algorithm_version=(
            "independent-analysis-v1" if artifact else "candidate-v1"
        ),
        artifact_sha256=artifact,
    )


def _field_case(
    case_id: str,
    event_id: str,
    candidate_values: tuple[tuple[float, ...], ...],
    truth_values: tuple[tuple[float, ...], ...],
    *,
    mask=None,
    stratum: str = "fss-primary",
) -> FieldVerificationCase:
    return FieldVerificationCase(
        case_id=case_id,
        evaluation_stratum_id=stratum,
        event_id=event_id,
        frame_id=f"frame-{case_id}",
        partition=VerificationPartition.TEST,
        candidate=_field(f"candidate-{case_id}", candidate_values, mask=mask),
        truth=_field(
            f"truth-{case_id}",
            truth_values,
            mask=mask,
            artifact=("b" * 63 + str(len(case_id) % 10)),
        ),
        label_id=f"field-label-{case_id}",
        label_source_id="independent-field-source",
        label_state="confirmed",
        truth_source_eligible=True,
    )


def _fss_config(
    *,
    threshold: float = 1.0,
    rows: int = 1,
    columns: int = 1,
    minimum_valid_fraction: float = 1.0,
) -> FssConfig:
    return FssConfig(
        threshold=threshold,
        threshold_operator=ThresholdOperator.GREATER_OR_EQUAL,
        window_rows=rows,
        window_columns=columns,
        minimum_valid_fraction=minimum_valid_fraction,
    )


def test_masked_fss_identical_fields_equal_one_and_report_excluded_windows():
    values = (
        (0.0, 2.0, 0.0),
        (2.0, 2.0, 0.0),
        (0.0, 0.0, 0.0),
    )
    mask = (
        (True, True, True),
        (True, False, True),
        (True, True, True),
    )
    cases = (
        _field_case("a", "event-a", values, values, mask=mask),
        _field_case("bb", "event-b", values, values, mask=mask),
    )

    report = verify_masked_fss(
        cases,
        config=_fss_config(),
        bootstrap=_bootstrap(),
    )

    assert report.fractions_skill_score.point_estimate == 1.0
    assert report.fractions_brier_score == 0.0
    assert report.eligible_window_count == 16
    assert report.excluded_window_count == 2
    assert report.window_width_m == 1_000.0
    assert report.window_height_m == 1_000.0


def test_masked_fss_uses_explicit_threshold_and_window():
    truth = (
        (0.0, 2.0, 0.0),
        (0.0, 2.0, 0.0),
        (0.0, 0.0, 0.0),
    )
    candidate = (
        (0.0, 0.0, 2.0),
        (0.0, 0.0, 2.0),
        (0.0, 0.0, 0.0),
    )
    cases = (
        _field_case("a", "event-a", candidate, truth),
        _field_case("bb", "event-b", candidate, truth),
    )

    point = verify_masked_fss(
        cases,
        config=_fss_config(rows=1, columns=1),
        bootstrap=_bootstrap(),
    )
    neighborhood = verify_masked_fss(
        cases,
        config=_fss_config(rows=2, columns=2),
        bootstrap=_bootstrap(),
    )

    assert point.fractions_skill_score.point_estimate < (
        neighborhood.fractions_skill_score.point_estimate
    )
    assert neighborhood.window_width_m == 2_000.0


def test_fss_fails_on_zero_reference_or_insufficient_events():
    dry = (
        (0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0),
    )
    cases = (
        _field_case("a", "event-a", dry, dry),
        _field_case("bb", "event-b", dry, dry),
    )
    with pytest.raises(ValueError, match="denominator is zero"):
        verify_masked_fss(
            cases,
            config=_fss_config(),
            bootstrap=_bootstrap(),
        )
    with pytest.raises(ValueError, match="caller requires 3"):
        verify_masked_fss(
            cases[:1],
            config=_fss_config(),
            bootstrap=_bootstrap(minimum_events=3),
        )


def test_field_case_rejects_grid_unit_and_nodata_mismatch():
    values = (
        (0.0, 2.0, 0.0),
        (0.0, 2.0, 0.0),
        (0.0, 0.0, 0.0),
    )
    candidate = _field("candidate", values)
    truth = _field("truth", values, artifact="b" * 64)
    base = {
        "case_id": "field-case",
        "evaluation_stratum_id": "fss-primary",
        "event_id": "event-a",
        "frame_id": "frame-a",
        "partition": VerificationPartition.TEST,
        "candidate": candidate,
        "truth": truth,
        "label_id": "field-label",
        "label_source_id": "independent",
        "label_state": "confirmed",
        "truth_source_eligible": True,
    }
    with pytest.raises(ValueError, match="variable or unit mismatch"):
        FieldVerificationCase(**{**base, "truth": replace(truth, unit_id="dBZ")})
    changed_mask = (
        (False, True, True),
        (True, True, True),
        (True, True, True),
    )
    with pytest.raises(ValueError, match="nodata masks differ"):
        FieldVerificationCase(
            **{**base, "truth": replace(truth, valid_mask=changed_mask)}
        )
    other_grid = replace(_grid(), grid_id="other-grid")
    with pytest.raises(ValueError, match="geometrie"):
        FieldVerificationCase(
            **{**base, "truth": replace(truth, grid=other_grid)}
        )


def test_field_case_builder_checks_independent_artifact_metadata():
    truth_artifact = "c" * 64
    label = ExternalLabel(
        label_id="field-label",
        label_kind=ExternalLabelKind.PRECIPITATION_FIELD,
        source_id="external-source",
        event_id="event-a",
        frame_id="frame-a",
        state=LabelState.CONFIRMED,
        payload=PrecipitationFieldLabel(
            grid_id="fss-grid",
            valid_time_utc=2_000.0,
            variable_id="rain_rate",
            unit_id="mm h-1",
            field_artifact_sha256=truth_artifact,
        ),
    )
    context = _truth_context(label)
    partition = VerificationPartition(
        context.split.partition_for_event("event-a").value
    )
    values = (
        (0.0, 2.0, 0.0),
        (0.0, 2.0, 0.0),
        (0.0, 0.0, 0.0),
    )
    candidate = _field("candidate", values)
    truth = _field("truth", values, artifact=truth_artifact)

    case = build_field_verification_case(
        case_id="field-case",
        evaluation_stratum_id="fss-primary",
        event_id="event-a",
        frame_id="frame-a",
        expected_partition=partition,
        candidate=candidate,
        truth=truth,
        truth_context=context,
        precipitation_label_id="field-label",
    )
    assert case.truth.artifact_sha256 == truth_artifact

    with pytest.raises(ValueError, match="metadata and truth field mismatch"):
        build_field_verification_case(
            case_id="bad-field-case",
            evaluation_stratum_id="fss-primary",
            event_id="event-a",
            frame_id="frame-a",
            expected_partition=partition,
            candidate=candidate,
            truth=replace(truth, artifact_sha256="d" * 64),
            truth_context=context,
            precipitation_label_id="field-label",
        )


def test_bootstrap_config_has_no_defaults_or_one_event_shortcut():
    with pytest.raises(TypeError):
        BootstrapConfig(seed="incomplete")  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="at least two"):
        BootstrapConfig(
            seed="bad",
            replicates=10,
            lower_probability=0.1,
            upper_probability=0.9,
            minimum_events=1,
        )


def test_verification_mirror_is_exact_pure_and_shadow_only():
    repository = Path(__file__).resolve().parents[1]
    integration = (
        repository
        / "custom_components/storm_tracker_v3/engine/forecast_verification.py"
    )
    worker = (
        repository
        / "storm_tracker_worker/tracking_core/engine/forecast_verification.py"
    )
    assert integration.read_bytes() == worker.read_bytes()

    imports = tuple(
        node
        for node in ast.walk(ast.parse(integration.read_text(encoding="utf-8")))
        if isinstance(node, (ast.Import, ast.ImportFrom))
    )
    imported_names = {
        alias.name
        for node in imports
        for alias in node.names
    } | {
        node.module
        for node in imports
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert not any("pysteps" in name for name in imported_names)
    assert not any("scientific_validation" in name for name in imported_names)
    assert not any("validation_corpus" in name for name in imported_names)

    for live_path in (
        repository / "custom_components/storm_tracker_v3/__init__.py",
        repository / "custom_components/storm_tracker_v3/engine/nowcast.py",
        repository / "storm_tracker_worker/service.py",
        repository / "storm_tracker_worker/tracking_core/__init__.py",
    ):
        assert "forecast_verification" not in live_path.read_text(encoding="utf-8")
