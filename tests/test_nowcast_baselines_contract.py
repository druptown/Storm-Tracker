from __future__ import annotations

from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
import json
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.nowcast_baselines import (
    BaselineMethod,
    ConstantMotionVector,
    MetricRasterField,
    derive_two_frame_constant_motion,
    eulerian_persistence,
    lagrangian_constant_motion,
)
from storm_tracker_worker.tracking_core.engine.tracker_forecast_contract import (
    CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS,
    CURRENT_FOOTPRINT_FRESHNESS_POLICY_ID,
    GROUND_FRESHNESS_LIMITS_SECONDS,
    GROUND_FRESHNESS_POLICY_ID,
    ConditionalDistribution,
    ConditionalDistributionKind,
    CurrentFootprintEvidence,
    CurrentGroundEvidence,
    DistributionEvidenceBasis,
    DistributionInterval,
    ForecastState,
    GroundEvidenceKind,
    NextThreatState,
    PassageProbabilityEvidence,
    PassageRelation,
    PrecipitationPresence,
    ProbabilityEvidenceBasis,
    QuantilePoint,
    SourceRecordIdKind,
    TrackerForecast,
    TrackerDisplayState,
    build_tracker_forecast_shadow,
)


def _grid(*, width: int = 4, height: int = 3) -> AnalysisGrid:
    return AnalysisGrid(
        grid_id="metric-grid",
        crs="EPSG:31370",
        origin_x_m=100_000.0,
        origin_y_m=200_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=2_000.0,
        width=width,
        height=height,
    )


def _field(
    *,
    grid: AnalysisGrid | None = None,
    field_id: str = "field-current",
    valid_at: float = 1_037.0,
    values=None,
    mask=None,
) -> MetricRasterField:
    grid = grid or _grid()
    values = values or tuple(
        tuple(float(row * grid.width + column + 1) for column in range(grid.width))
        for row in range(grid.height)
    )
    mask = mask or tuple(
        tuple(True for _ in range(grid.width)) for _ in range(grid.height)
    )
    return MetricRasterField(
        field_id=field_id,
        valid_at=valid_at,
        quantity="rain_rate",
        unit="mm h-1",
        grid=grid,
        values=values,
        valid_mask=mask,
    )


def _motion(
    *,
    grid: AnalysisGrid | None = None,
    current_at: float = 1_037.0,
    vx_m_s: float = 0.0,
    vy_m_s: float = 0.0,
) -> ConstantMotionVector:
    grid = grid or _grid()
    return ConstantMotionVector(
        motion_id="motion-two-frame",
        analysis_grid_id=grid.grid_id,
        reference_valid_at=current_at,
        vx_m_s=vx_m_s,
        vy_m_s=vy_m_s,
        support_field_ids=("field-previous", "field-current"),
        support_valid_at=(991.0, current_at),
        method="supplied_two_frame_translation",
        algorithm_version="translation-estimator-test-v1",
    )


def _ensemble_probability(
    *,
    sample_size: int = 5,
    passage_count: int = 3,
) -> PassageProbabilityEvidence:
    return PassageProbabilityEvidence(
        evidence_id="passage-members-run-7",
        basis=ProbabilityEvidenceBasis.ENSEMBLE_FREQUENCY,
        sample_size=sample_size,
        source_id="ensemble-run-7",
        source_algorithm_version="steps-test-v1",
        passage_count=passage_count,
        calibration_version=None,
    )


def _eta_distribution(
    *,
    basis: DistributionEvidenceBasis = DistributionEvidenceBasis.ENSEMBLE_MEMBERS,
    calibration_version: str | None = None,
) -> ConditionalDistribution:
    return ConditionalDistribution(
        distribution_id="eta-run-7",
        kind=ConditionalDistributionKind.CONDITIONAL_ETA,
        quantity="eta_after_reference",
        unit="s",
        basis=basis,
        sample_size=5,
        source_id="ensemble-run-7",
        source_algorithm_version="steps-test-v1",
        quantiles=(
            QuantilePoint(0.1, 600.0),
            QuantilePoint(0.5, 1_200.0),
            QuantilePoint(0.9, 1_800.0),
        ),
        calibration_version=calibration_version,
    )


def _intensity_distribution(
    *,
    basis: DistributionEvidenceBasis = DistributionEvidenceBasis.ENSEMBLE_MEMBERS,
    calibration_version: str | None = None,
) -> ConditionalDistribution:
    return ConditionalDistribution(
        distribution_id="intensity-run-7",
        kind=ConditionalDistributionKind.CONDITIONAL_LOCAL_INTENSITY,
        quantity="rain_rate_at_tracker",
        unit="mm h-1",
        basis=basis,
        sample_size=5,
        source_id="ensemble-run-7",
        source_algorithm_version="steps-test-v1",
        quantiles=(
            QuantilePoint(0.1, 0.2),
            QuantilePoint(0.5, 1.0),
            QuantilePoint(0.9, 4.0),
        ),
        calibration_version=calibration_version,
    )


def _forecast_kwargs() -> dict:
    return {
        "forecast_id": "forecast-tracker-a-run-7",
        "tracker_id": "tracker-a",
        "state": ForecastState.DETERMINATE,
        "analysis_grid_id": "metric-grid",
        "tracker_x_m": 102_500.0,
        "tracker_y_m": 197_000.0,
        "reference_valid_at": 1_000.0,
        "forecast_valid_at": 1_060.0,
        "lead_time_seconds": 60.0,
        "horizon_seconds": 3_600.0,
        "input_track_ids": ("track-primary", "track-alternative"),
        "input_field_ids": ("field-previous", "field-current"),
        "algorithm_version": "tracker-forecast-test-v1",
        "calibration_version": None,
        "passage_probability": 0.6,
        "no_passage_probability": 0.4,
        "probability_evidence": _ensemble_probability(),
        "conditional_eta_distribution": _eta_distribution(),
        "conditional_eta_interval": DistributionInterval(
            lower_probability=0.1,
            upper_probability=0.9,
            lower_value=600.0,
            upper_value=1_800.0,
        ),
        "local_intensity_distribution": _intensity_distribution(),
        "indeterminate_reasons": (),
    }


def _footprint_evidence(
    *,
    tracker_id: str = "tracker-a",
    tracker_latitude: float = 51.0,
    tracker_longitude: float = 4.0,
    frame_valid_at: float = 1_000.0,
    evaluated_at: float = 1_005.0,
    input_storm_count: int | None = None,
    scan_complete: bool | None = None,
    evaluated_storm_ids: tuple[str, ...] = ("storm-current",),
    containing_storm_ids: tuple[str, ...] = ("storm-current",),
    nearest_storm_id: str | None = "storm-current",
    minimum_distance_km: float | None = 0.0,
) -> CurrentFootprintEvidence:
    input_count = (
        len(evaluated_storm_ids)
        if input_storm_count is None
        else input_storm_count
    )
    complete = (
        len(evaluated_storm_ids) == input_count
        if scan_complete is None
        else scan_complete
    )
    frame_age = evaluated_at - frame_valid_at
    return CurrentFootprintEvidence(
        evidence_id="current-footprint-frame-7",
        tracker_id=tracker_id,
        tracker_latitude=tracker_latitude,
        tracker_longitude=tracker_longitude,
        frame_valid_at=frame_valid_at,
        evaluated_at=evaluated_at,
        frame_age_seconds=frame_age,
        freshness_limit_seconds=(
            CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS
        ),
        freshness_policy_id=CURRENT_FOOTPRINT_FRESHNESS_POLICY_ID,
        fresh=(
            0.0 <= frame_age
            <= CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS
        ),
        source_id="rainviewer",
        source_algorithm_version="legacy-current-frame-polygon-scan-v2",
        input_storm_count=input_count,
        scan_complete=complete,
        scan_sha256="0" * 64,
        evaluated_storm_ids=evaluated_storm_ids,
        containing_storm_ids=containing_storm_ids,
        nearest_storm_id=nearest_storm_id,
        minimum_distance_km=minimum_distance_km,
    )


def _ground_evidence(
    *,
    evidence_id: str = "ground-netatmo-heffen-1",
    tracker_id: str = "tracker-a",
    tracker_latitude: float = 51.0,
    tracker_longitude: float = 4.0,
    provider_id: str = "netatmo",
    station_id: str = "home",
    physical_site_id: str = "site-heffen-home",
    physical_site_basis: str = "provider_identity",
    source_record_id: str = "netatmo:home:995.0",
    source_record_id_kind: SourceRecordIdKind = (
        SourceRecordIdKind.DERIVED_STATION_TIMESTAMP
    ),
    observed_at: float = 995.0,
    evaluated_at: float = 1_000.0,
    observation_age_seconds: float = 5.0,
    distance_to_tracker_km: float = 0.2,
    matched_storm_id: str | None = "storm-heffen",
    kind: GroundEvidenceKind = GroundEvidenceKind.PRECIPITATION_PRESENCE,
    measurement_value: float | None = None,
    measurement_unit: str | None = None,
) -> CurrentGroundEvidence:
    freshness_limit = dict(GROUND_FRESHNESS_LIMITS_SECONDS).get(
        provider_id,
        600.0,
    )
    return CurrentGroundEvidence(
        evidence_id=evidence_id,
        kind=kind,
        tracker_id=tracker_id,
        tracker_latitude=tracker_latitude,
        tracker_longitude=tracker_longitude,
        provider_id=provider_id,
        station_id=station_id,
        physical_site_id=physical_site_id,
        physical_site_basis=physical_site_basis,
        source_record_id=source_record_id,
        source_record_id_kind=source_record_id_kind,
        source_algorithm_version="ground-adapter-test-v1",
        observed_at=observed_at,
        evaluated_at=evaluated_at,
        observation_age_seconds=observation_age_seconds,
        freshness_limit_seconds=freshness_limit,
        freshness_policy_id=GROUND_FRESHNESS_POLICY_ID,
        distance_to_tracker_km=distance_to_tracker_km,
        precipitation_present=True,
        fresh=True,
        measurement_value=measurement_value,
        measurement_unit=measurement_unit,
        matched_storm_id=matched_storm_id,
    )


def test_eulerian_persistence_uses_real_timestamps_and_preserves_nodata():
    mask = (
        (True, False, True, True),
        (True, True, True, True),
        (True, True, True, True),
    )
    field = _field(mask=mask)

    forecast = eulerian_persistence(
        field,
        target_valid_at=1_337.0,
        forecast_field_id="persistence-plus-300",
    )

    assert forecast.method is BaselineMethod.EULERIAN_PERSISTENCE
    assert forecast.reference_valid_at == 1_037.0
    assert forecast.target_valid_at == 1_337.0
    assert forecast.lead_time_seconds == 300.0
    assert forecast.input_field_ids == ("field-current",)
    assert forecast.value_or_none(0, 0) == 1.0
    assert forecast.value_or_none(0, 1) is None
    assert forecast.values[0][1] == 0.0
    assert forecast.motion_id is None


def test_two_frame_vector_uses_irregular_observation_interval_without_cap():
    motion = derive_two_frame_constant_motion(
        motion_id="m-irregular",
        analysis_grid_id="metric-grid",
        previous_field_id="field-previous",
        current_field_id="field-current",
        previous_valid_at=1_000.0,
        current_valid_at=1_037.0,
        previous_x_m=10_000.0,
        previous_y_m=20_000.0,
        current_x_m=17_400.0,
        current_y_m=16_300.0,
        method="two_real_positions",
        algorithm_version="position-difference-v1",
    )

    assert motion.observed_interval_seconds == 37.0
    assert motion.vx_m_s == 200.0
    assert motion.vy_m_s == -100.0
    assert motion.speed_kmh == pytest.approx(804.984472)


def test_constant_motion_translates_east_and_north_in_metric_coordinates():
    grid = AnalysisGrid(
        grid_id="square-grid",
        crs="EPSG:3857",
        origin_x_m=0.0,
        origin_y_m=4_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=4,
        height=4,
    )
    values = (
        (0.0, 0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0, 0.0),
        (10.0, 0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0, 0.0),
    )
    field = _field(grid=grid, values=values, mask=tuple(
        tuple(True for _ in range(4)) for _ in range(4)
    ))
    motion = _motion(
        grid=grid,
        vx_m_s=1_000.0,
        vy_m_s=1_000.0,
    )

    forecast = lagrangian_constant_motion(
        field,
        motion,
        target_valid_at=1_038.0,
        forecast_field_id="translated-one-second",
    )

    assert forecast.value_or_none(1, 1) == pytest.approx(10.0)
    assert forecast.valid_mask[1][1]
    assert not forecast.valid_mask[3][0]
    assert forecast.input_field_ids == ("field-previous", "field-current")


def test_half_pixel_advection_is_bilinear_and_any_weighted_nodata_is_unknown():
    grid = AnalysisGrid(
        grid_id="one-row",
        crs="EPSG:3857",
        origin_x_m=0.0,
        origin_y_m=1_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=4,
        height=1,
    )
    field = _field(
        grid=grid,
        values=((0.0, 10.0, 20.0, 30.0),),
        mask=((True, True, False, True),),
    )
    motion = _motion(grid=grid, vx_m_s=500.0)

    forecast = lagrangian_constant_motion(
        field,
        motion,
        target_valid_at=1_038.0,
        forecast_field_id="half-pixel",
    )

    assert forecast.value_or_none(0, 1) == pytest.approx(5.0)
    assert forecast.value_or_none(0, 2) is None
    assert forecast.value_or_none(0, 3) is None
    assert forecast.value_or_none(0, 0) is None


def test_extreme_finite_motion_is_not_clipped_or_rejected():
    grid = AnalysisGrid(
        grid_id="extreme-grid",
        crs="EPSG:3857",
        origin_x_m=0.0,
        origin_y_m=1_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=105,
        height=1,
    )
    values = (tuple(9.0 if column == 0 else 0.0 for column in range(105)),)
    field = _field(grid=grid, values=values, mask=(tuple(True for _ in range(105)),))
    motion = _motion(grid=grid, vx_m_s=100_000.0)

    forecast = lagrangian_constant_motion(
        field,
        motion,
        target_valid_at=1_038.0,
        forecast_field_id="uncapped-translation",
    )

    assert motion.speed_kmh == 360_000.0
    assert forecast.value_or_none(0, 100) == pytest.approx(9.0)
    assert not forecast.valid_mask[0][99]


def test_baselines_fail_closed_on_grid_time_and_support_mismatch():
    field = _field()
    other_grid = replace(_grid(), grid_id="other-grid")
    with pytest.raises(ValueError, match="later"):
        eulerian_persistence(
            field,
            target_valid_at=field.valid_at,
            forecast_field_id="invalid-time",
        )
    with pytest.raises(ValueError, match="same analysis grid"):
        lagrangian_constant_motion(
            field,
            _motion(grid=other_grid),
            target_valid_at=1_100.0,
            forecast_field_id="invalid-grid",
        )
    with pytest.raises(ValueError, match="newest motion support field"):
        lagrangian_constant_motion(
            replace(field, field_id="not-the-current-support"),
            _motion(),
            target_valid_at=1_100.0,
            forecast_field_id="invalid-support",
        )
    with pytest.raises(ValueError, match="increasing real timestamps"):
        derive_two_frame_constant_motion(
            motion_id="bad-time",
            analysis_grid_id="metric-grid",
            previous_field_id="a",
            current_field_id="b",
            previous_valid_at=10.0,
            current_valid_at=10.0,
            previous_x_m=0.0,
            previous_y_m=0.0,
            current_x_m=1.0,
            current_y_m=1.0,
            method="invalid",
            algorithm_version="test",
        )


def test_metric_fields_and_baseline_outputs_are_immutable():
    field = _field()
    forecast = eulerian_persistence(
        field,
        target_valid_at=1_100.0,
        forecast_field_id="immutable",
    )
    with pytest.raises(FrozenInstanceError):
        field.valid_at = 2_000.0
    with pytest.raises(FrozenInstanceError):
        forecast.lead_time_seconds = 1.0


def test_ensemble_probability_and_conditional_products_form_valid_contract():
    forecast = TrackerForecast(**_forecast_kwargs())

    assert forecast.state is ForecastState.DETERMINATE
    assert forecast.passage_probability == 0.6
    assert forecast.no_passage_probability == 0.4
    assert forecast.conditional_eta_interval.probability_mass == pytest.approx(0.8)
    assert (
        forecast.conditional_eta_distribution.quantile_value(0.5)
        == 1_200.0
    )
    assert (
        forecast.local_intensity_distribution.quantity
        == "rain_rate_at_tracker"
    )


def test_zero_passage_ensemble_has_explicit_no_passage_and_no_conditionals():
    values = _forecast_kwargs()
    values.update(
        passage_probability=0.0,
        no_passage_probability=1.0,
        probability_evidence=_ensemble_probability(passage_count=0),
        conditional_eta_distribution=None,
        conditional_eta_interval=None,
        local_intensity_distribution=None,
    )

    forecast = TrackerForecast(**values)

    assert forecast.no_passage_probability == 1.0
    assert forecast.conditional_eta_distribution is None


def test_ensemble_probability_must_equal_member_frequency_not_quality_score():
    values = _forecast_kwargs()
    values["passage_probability"] = 0.8
    values["no_passage_probability"] = 0.2
    with pytest.raises(ValueError, match="raw ensemble frequency"):
        TrackerForecast(**values)

    with pytest.raises(TypeError, match="ProbabilityEvidenceBasis"):
        PassageProbabilityEvidence(
            evidence_id="heuristic",
            basis="heuristic_score",
            sample_size=5,
            source_id="quality-score",
            source_algorithm_version="legacy",
            passage_count=4,
            calibration_version=None,
        )
    with pytest.raises(ValueError, match="cannot claim a calibration"):
        replace(_ensemble_probability(), calibration_version="invented-version")


def test_empirical_probability_requires_real_matching_calibration_version():
    with pytest.raises(ValueError, match="calibration_version"):
        PassageProbabilityEvidence(
            evidence_id="missing-calibration",
            basis=ProbabilityEvidenceBasis.EMPIRICAL_CALIBRATION,
            sample_size=200,
            source_id="held-out-events",
            source_algorithm_version="isotonic-v1",
            passage_count=None,
            calibration_version=None,
        )

    evidence = PassageProbabilityEvidence(
        evidence_id="calibrated-probability",
        basis=ProbabilityEvidenceBasis.EMPIRICAL_CALIBRATION,
        sample_size=200,
        source_id="held-out-events",
        source_algorithm_version="isotonic-v1",
        passage_count=None,
        calibration_version="calibration-2026-08-a",
    )
    values = _forecast_kwargs()
    values["probability_evidence"] = evidence
    with pytest.raises(ValueError, match="calibration versions differ"):
        TrackerForecast(**values)

    values = _forecast_kwargs()
    values["calibration_version"] = "unbacked-version"
    with pytest.raises(ValueError, match="requires empirical evidence"):
        TrackerForecast(**values)


def test_matching_versioned_empirical_probability_and_distributions_are_valid():
    calibration_version = "calibration-heldout-events-2026-08-a"
    values = _forecast_kwargs()
    values.update(
        calibration_version=calibration_version,
        probability_evidence=PassageProbabilityEvidence(
            evidence_id="calibrated-passage",
            basis=ProbabilityEvidenceBasis.EMPIRICAL_CALIBRATION,
            sample_size=400,
            source_id="held-out-event-calibrator",
            source_algorithm_version="isotonic-v1",
            passage_count=None,
            calibration_version=calibration_version,
        ),
        conditional_eta_distribution=_eta_distribution(
            basis=DistributionEvidenceBasis.EMPIRICAL_SAMPLES,
            calibration_version=calibration_version,
        ),
        local_intensity_distribution=_intensity_distribution(
            basis=DistributionEvidenceBasis.EMPIRICAL_SAMPLES,
            calibration_version=calibration_version,
        ),
    )

    forecast = TrackerForecast(**values)

    assert forecast.calibration_version == calibration_version
    assert (
        forecast.probability_evidence.basis
        is ProbabilityEvidenceBasis.EMPIRICAL_CALIBRATION
    )


def test_indeterminate_is_explicit_and_cannot_leak_partial_numbers():
    values = _forecast_kwargs()
    values.update(
        state=ForecastState.INDETERMINATE,
        calibration_version=None,
        passage_probability=None,
        no_passage_probability=None,
        probability_evidence=None,
        conditional_eta_distribution=None,
        conditional_eta_interval=None,
        local_intensity_distribution=None,
        indeterminate_reasons=("stale_input_field", "missing_ensemble"),
    )
    forecast = TrackerForecast(**values)
    assert forecast.state is ForecastState.INDETERMINATE

    values["passage_probability"] = 0.5
    with pytest.raises(ValueError, match="cannot expose incomplete"):
        TrackerForecast(**values)


def test_indeterminate_requires_reason_and_determinate_rejects_reasons():
    values = _forecast_kwargs()
    values["indeterminate_reasons"] = ("stale_input",)
    with pytest.raises(ValueError, match="cannot carry indeterminate reasons"):
        TrackerForecast(**values)

    values.update(
        state=ForecastState.INDETERMINATE,
        passage_probability=None,
        no_passage_probability=None,
        probability_evidence=None,
        conditional_eta_distribution=None,
        conditional_eta_interval=None,
        local_intensity_distribution=None,
        indeterminate_reasons=(),
    )
    with pytest.raises(ValueError, match="requires reason"):
        TrackerForecast(**values)


def test_eta_interval_must_be_stored_quantiles_and_within_horizon():
    values = _forecast_kwargs()
    values["conditional_eta_interval"] = DistributionInterval(
        lower_probability=0.1,
        upper_probability=0.9,
        lower_value=500.0,
        upper_value=1_800.0,
    )
    with pytest.raises(ValueError, match="do not match ETA quantiles"):
        TrackerForecast(**values)

    values = _forecast_kwargs()
    values["horizon_seconds"] = 1_500.0
    with pytest.raises(ValueError, match="beyond valid forecast horizon"):
        TrackerForecast(**values)


def test_distribution_contract_rejects_unsourced_or_nonconditional_shapes():
    with pytest.raises(ValueError, match="conditional on passage"):
        replace(_eta_distribution(), conditional_on_passage=False)
    with pytest.raises(ValueError, match="strictly increasing"):
        replace(
            _eta_distribution(),
            quantiles=(
                QuantilePoint(0.5, 600.0),
                QuantilePoint(0.1, 1_200.0),
                QuantilePoint(0.9, 1_800.0),
            ),
        )
    with pytest.raises(ValueError, match="calibration_version"):
        replace(
            _intensity_distribution(),
            basis=DistributionEvidenceBasis.EMPIRICAL_SAMPLES,
        )


def test_forecast_time_horizon_and_input_ids_are_exact_and_immutable():
    values = _forecast_kwargs()
    values["forecast_valid_at"] = 1_061.0
    with pytest.raises(ValueError, match="reference plus lead"):
        TrackerForecast(**values)

    values = _forecast_kwargs()
    values["lead_time_seconds"] = 4_000.0
    values["forecast_valid_at"] = 5_000.0
    with pytest.raises(ValueError, match="cannot exceed"):
        TrackerForecast(**values)

    values = _forecast_kwargs()
    values["input_track_ids"] = ("duplicate", "duplicate")
    with pytest.raises(ValueError, match="input_track_ids"):
        TrackerForecast(**values)

    forecast = TrackerForecast(**_forecast_kwargs())
    with pytest.raises(FrozenInstanceError):
        forecast.passage_probability = 0.9


def test_current_footprint_cannot_be_overwritten_by_legacy_mist():
    legacy = {
        "status": "nadert_regio",
        "radar_covered": True,
        "active_system_count": 1,
        "passage_classification": "mist",
        "forecast_confidence_percent": 93,
        "approaching_systems": [{
            "storm_id": "storm-current",
            "distance_km": 0.0,
            "eta_minutes": 12.0,
            "passage_classification": "mist",
        }],
    }
    before = deepcopy(legacy)

    shadow = build_tracker_forecast_shadow(
        legacy,
        current_footprint=_footprint_evidence(),
    )
    rendered = shadow.to_dict()

    assert legacy == before
    assert shadow.current_state is PrecipitationPresence.PRESENT
    assert shadow.current_storm_ids == ("storm-current",)
    assert shadow.legacy_selected_relation is PassageRelation.MISS
    assert shadow.next_threat_state is NextThreatState.NOT_DETECTED
    assert shadow.display_state is TrackerDisplayState.CURRENT_PRECIPITATION
    assert rendered["current_precipitation"]["state"] == "present"
    assert rendered["display_state"] == "current_precipitation"
    assert rendered["passage_forecast"] == {
        "state": "indeterminate",
        "forecast_id": None,
        "passage_probability": None,
        "no_passage_probability": None,
        "reason_codes": ["formal_passage_forecast_missing"],
    }


def test_positive_ground_current_beats_mist_without_faking_radar_containment():
    legacy = {
        "status": "nadert_regio",
        "radar_covered": True,
        "active_system_count": 1,
        "passage_classification": "mist",
        "approaching_systems": [{
            "storm_id": "storm-heffen",
            "distance_km": 3.1,
            "eta_minutes": 7.0,
            "passage_classification": "mist",
        }],
    }
    shadow = build_tracker_forecast_shadow(
        legacy,
        current_footprint=_footprint_evidence(
            evaluated_storm_ids=("storm-heffen",),
            containing_storm_ids=(),
            nearest_storm_id="storm-heffen",
            minimum_distance_km=3.1,
        ),
        current_ground=(_ground_evidence(),),
    )
    rendered = shadow.to_dict()

    assert shadow.current_state is PrecipitationPresence.PRESENT
    assert shadow.current_radar_state is PrecipitationPresence.ABSENT
    assert shadow.current_storm_ids == ()
    assert shadow.nearby_state is PrecipitationPresence.PRESENT
    assert shadow.nearby_storm_id == "storm-heffen"
    assert shadow.nearby_distance_km == 3.1
    assert shadow.next_threat_state is NextThreatState.NOT_DETECTED
    assert shadow.legacy_selected_relation is PassageRelation.MISS
    assert shadow.display_state is TrackerDisplayState.CURRENT_PRECIPITATION
    current = rendered["current_precipitation"]
    assert current["positive_bases"] == ["positive_local_ground"]
    assert current["radar_footprint"]["state"] == "absent"
    assert current["radar_footprint"]["storm_ids"] == []
    assert current["associated_storm_ids"] == ["storm-heffen"]
    assert current["ground_evidence"][0]["provider_id"] == "netatmo"
    assert current["ground_evidence"][0]["distance_to_tracker_km"] == 0.2
    assert "radar_ground_current_disagreement" in rendered["reason_codes"]


def test_ground_current_is_positive_typed_and_fail_closed():
    evidence = _ground_evidence(
        kind=GroundEvidenceKind.QUANTITATIVE_RATE,
        measurement_value=0.51,
        measurement_unit="mm h-1",
    )
    assert evidence.measurement_value == 0.51

    with pytest.raises(ValueError, match="positive precipitation"):
        replace(evidence, precipitation_present=False)
    with pytest.raises(ValueError, match="must be fresh"):
        replace(evidence, fresh=False)
    with pytest.raises(ValueError, match="must be positive"):
        replace(evidence, measurement_value=0.0)
    with pytest.raises(ValueError, match="cannot expose"):
        _ground_evidence(measurement_value=0.1, measurement_unit="mm h-1")


def test_generic_ground_flag_cannot_become_current_ground_truth():
    shadow = build_tracker_forecast_shadow({
        "status": "waargenomen",
        "radar_covered": True,
        "active_system_count": 1,
        "ground_precipitation_observed": True,
        "approaching_systems": [],
    })

    assert shadow.current_state is PrecipitationPresence.INDETERMINATE
    assert shadow.current_ground_evidence == ()
    assert (
        "exact_current_footprint_evidence_missing"
        in shadow.separation_reason_codes
    )


def test_ground_current_requires_canonical_independent_physical_sites():
    evidence_a = _ground_evidence(
        evidence_id="ground-a",
        station_id="station-a",
        physical_site_id="site-a",
        source_record_id="record-a",
    )
    evidence_b = _ground_evidence(
        evidence_id="ground-b",
        station_id="station-b",
        physical_site_id="site-b",
        source_record_id="record-b",
    )
    legacy = {
        "radar_covered": False,
        "active_system_count": 0,
        "approaching_systems": [],
    }

    with pytest.raises(ValueError, match="canonically sorted"):
        build_tracker_forecast_shadow(
            legacy,
            current_ground=(evidence_b, evidence_a),
        )
    with pytest.raises(ValueError, match="physical site"):
        build_tracker_forecast_shadow(
            legacy,
            current_ground=(
                evidence_a,
                replace(
                    evidence_b,
                    physical_site_id="site-a",
                ),
            ),
        )

    accepted = build_tracker_forecast_shadow(
        legacy,
        current_ground=(evidence_a, evidence_b),
    )
    assert accepted.current_state is PrecipitationPresence.PRESENT
    assert accepted.current_radar_state is PrecipitationPresence.INDETERMINATE
    assert (
        "current_ground_without_radar_footprint_determination"
        in accepted.separation_reason_codes
    )


def test_current_and_different_next_threat_remain_independent():
    legacy = {
        "status": "naderend",
        "radar_covered": True,
        "active_system_count": 2,
        "passage_classification": "mist",
        "approaching_systems": [
            {
                "storm_id": "storm-current",
                "distance_km": 0.0,
                "eta_minutes": 8.0,
                "passage_classification": "mist",
            },
            {
                "storm_id": "storm-next",
                "distance_km": 22.0,
                "eta_minutes": 18.0,
                "passage_classification": "raak",
            },
        ],
    }

    shadow = build_tracker_forecast_shadow(
        legacy,
        current_footprint=_footprint_evidence(
            evaluated_storm_ids=("storm-current", "storm-next"),
        ),
    )

    assert shadow.current_state is PrecipitationPresence.PRESENT
    assert shadow.nearby_state is PrecipitationPresence.PRESENT
    assert shadow.nearby_storm_id == "storm-next"
    assert shadow.next_threat_state is NextThreatState.DETECTED
    assert shadow.next_threat_storm_id == "storm-next"
    assert shadow.next_threat_relation is PassageRelation.HIT
    assert shadow.next_threat_eta_minutes == 18.0
    assert shadow.display_state is TrackerDisplayState.CURRENT_PRECIPITATION


def test_exact_outside_scan_is_nearby_not_current():
    shadow = build_tracker_forecast_shadow(
        {
            "status": "nadert_regio",
            "radar_covered": True,
            "active_system_count": 1,
            "passage_classification": "mist",
            "approaching_systems": [],
        },
        current_footprint=_footprint_evidence(
            evaluated_storm_ids=("storm-nearby",),
            containing_storm_ids=(),
            nearest_storm_id="storm-nearby",
            minimum_distance_km=8.25,
        ),
    )

    assert shadow.current_state is PrecipitationPresence.ABSENT
    assert shadow.current_evidence_id == "current-footprint-frame-7"
    assert shadow.nearby_state is PrecipitationPresence.PRESENT
    assert shadow.nearby_storm_id == "storm-nearby"
    assert shadow.nearby_distance_km == 8.25
    assert shadow.next_threat_state is NextThreatState.INDETERMINATE
    assert (
        "candidate_scan_does_not_cover_typed_footprint"
        in shadow.candidate_collection.reason_codes
    )
    assert shadow.display_state is TrackerDisplayState.NEARBY_PRECIPITATION


def test_rounded_legacy_zero_never_becomes_current_footprint_truth():
    shadow = build_tracker_forecast_shadow({
        "status": "nadert_regio",
        "radar_covered": True,
        "active_system_count": 1,
        "nearest_precipitation_storm_id": "rounded",
        "nearest_precipitation_distance_km": 0.0,
        "passage_classification": "mist",
        "approaching_systems": [],
    })

    assert shadow.current_state is PrecipitationPresence.INDETERMINATE
    assert shadow.current_storm_ids == ()
    assert shadow.display_state is TrackerDisplayState.INDETERMINATE
    assert (
        "exact_current_footprint_evidence_missing"
        in shadow.separation_reason_codes
    )


def test_only_formal_contract_can_populate_passage_probability():
    legacy = {
        "target_id": "tracker-a",
        "status": "droog",
        "radar_covered": True,
        "active_system_count": 0,
        "forecast_confidence_percent": 99,
        "approaching_systems": [],
    }
    without_forecast = build_tracker_forecast_shadow(legacy)
    with_forecast = build_tracker_forecast_shadow(
        legacy,
        passage_forecast=TrackerForecast(**_forecast_kwargs()),
    )

    assert without_forecast.passage_forecast_state is ForecastState.INDETERMINATE
    assert without_forecast.passage_probability is None
    assert with_forecast.passage_forecast_state is ForecastState.DETERMINATE
    assert with_forecast.passage_probability == 0.6
    assert with_forecast.no_passage_probability == 0.4
    assert with_forecast.passage_reason_codes == ()


def test_current_footprint_evidence_is_canonical_and_fail_closed():
    with pytest.raises(ValueError, match="canonically sorted"):
        _footprint_evidence(
            evaluated_storm_ids=("storm-z", "storm-a"),
            containing_storm_ids=("storm-a",),
            nearest_storm_id="storm-a",
        )
    with pytest.raises(ValueError, match="exactly match"):
        _footprint_evidence(containing_storm_ids=(), minimum_distance_km=0.0)
    with pytest.raises(ValueError, match="subset"):
        _footprint_evidence(containing_storm_ids=("other-storm",))


def test_future_and_stale_footprints_never_become_current_or_next_truth():
    with pytest.raises(ValueError, match="future"):
        replace(
            _footprint_evidence(),
            frame_valid_at=1_010.0,
            frame_age_seconds=0.0,
        )

    stale = _footprint_evidence(
        frame_valid_at=100.0,
        evaluated_at=2_000.0,
    )
    shadow = build_tracker_forecast_shadow(
        {
            "target_id": "tracker-a",
            "radar_covered": True,
            "active_system_count": 1,
            "approaching_systems": [{
                "storm_id": "storm-current",
                "distance_km": 0.0,
                "eta_minutes": 5.0,
                "passage_classification": "raak",
            }],
        },
        current_footprint=stale,
    )

    assert stale.fresh is False
    assert shadow.current_state is PrecipitationPresence.INDETERMINATE
    assert shadow.nearby_state is PrecipitationPresence.INDETERMINATE
    assert shadow.next_threat_state is NextThreatState.INDETERMINATE
    assert "current_footprint_stale" in shadow.separation_reason_codes


def test_ground_time_and_provider_policy_are_fail_closed():
    evidence = _ground_evidence()
    with pytest.raises(ValueError, match="future"):
        replace(
            evidence,
            observed_at=1_010.0,
            observation_age_seconds=0.0,
        )
    with pytest.raises(ValueError, match="stale"):
        replace(
            evidence,
            observed_at=100.0,
            observation_age_seconds=900.0,
        )
    with pytest.raises(ValueError, match="no freshness policy"):
        replace(evidence, provider_id="unknown")
    with pytest.raises(ValueError, match="does not match provider"):
        replace(evidence, freshness_limit_seconds=1_200.0)


def test_zero_active_count_without_typed_scan_is_not_a_dry_claim():
    shadow = build_tracker_forecast_shadow({
        "target_id": "tracker-a",
        "radar_covered": True,
        "active_system_count": 0,
        "approaching_systems": [],
    })

    assert shadow.current_state is PrecipitationPresence.INDETERMINATE
    assert shadow.nearby_state is PrecipitationPresence.INDETERMINATE
    assert shadow.next_threat_state is NextThreatState.INDETERMINATE
    assert shadow.display_state is TrackerDisplayState.INDETERMINATE


def test_malformed_candidates_cannot_become_not_detected():
    footprint = _footprint_evidence(
        evaluated_storm_ids=(),
        containing_storm_ids=(),
        nearest_storm_id=None,
        minimum_distance_km=None,
    )
    shadow = build_tracker_forecast_shadow(
        {
            "target_id": "tracker-a",
            "radar_covered": True,
            "active_system_count": 0,
            "approaching_systems": [None, {}],
        },
        current_footprint=footprint,
    )

    assert shadow.current_state is PrecipitationPresence.ABSENT
    assert shadow.nearby_state is PrecipitationPresence.ABSENT
    assert shadow.next_threat_state is NextThreatState.INDETERMINATE
    assert shadow.candidate_collection.malformed_count == 2
    assert shadow.candidate_collection.complete is False
    assert shadow.display_state is TrackerDisplayState.INDETERMINATE


def test_ground_evidence_serialization_is_bounded_but_rain_is_preserved():
    evidence = tuple(
        _ground_evidence(
            evidence_id=f"ground-{index:03d}",
            station_id=f"station-{index:03d}",
            physical_site_id=f"site-{index:03d}",
            source_record_id=f"record-{index:03d}",
            distance_to_tracker_km=0.2 + index / 1_000.0,
            matched_storm_id=None,
        )
        for index in range(40)
    )
    shadow = build_tracker_forecast_shadow(
        {
            "target_id": "tracker-a",
            "radar_covered": False,
            "active_system_count": 0,
            "approaching_systems": [],
        },
        current_ground=evidence,
    )
    diagnostic = shadow.current_ground_collection

    assert shadow.current_state is PrecipitationPresence.PRESENT
    assert len(shadow.current_ground_evidence) == 32
    assert diagnostic.input_total_count == 40
    assert diagnostic.inspected_count == 40
    assert diagnostic.serialized_count == 32
    assert diagnostic.overflow_count == 8
    assert diagnostic.complete is False
    assert len(diagnostic.content_sha256) == 64


def test_large_candidate_input_is_bounded_and_never_turns_into_no_threat():
    raw_candidates = [None] * 4_999
    raw_candidates.append({
        "storm_id": "storm-late",
        "distance_km": 12.0,
        "eta_minutes": 5.0,
        "passage_classification": "raak",
    })
    shadow = build_tracker_forecast_shadow(
        {
            "target_id": "tracker-a",
            "radar_covered": True,
            "active_system_count": 1,
            "approaching_systems": raw_candidates,
        },
        current_footprint=_footprint_evidence(
            evaluated_storm_ids=("storm-late",),
            containing_storm_ids=(),
            nearest_storm_id="storm-late",
            minimum_distance_km=12.0,
        ),
    )
    diagnostic = shadow.candidate_collection

    assert shadow.next_threat_state is NextThreatState.INDETERMINATE
    assert shadow.nearby_state is PrecipitationPresence.PRESENT
    assert diagnostic.input_total_count == 5_000
    assert diagnostic.inspected_count == 128
    assert diagnostic.malformed_count == 128
    assert diagnostic.complete is False
    assert len(diagnostic.content_sha256) == 64
    assert len(diagnostic.reason_codes) <= 32
    assert len(json.dumps(shadow.to_dict())) < 20_000


def test_ha_worker_mirrors_are_exact_and_shadow_only():
    repository = Path(__file__).resolve().parents[1]
    integration_engine = (
        repository / "custom_components/storm_tracker_v3/engine"
    )
    worker_engine = repository / "storm_tracker_worker/tracking_core/engine"
    for filename in (
        "nowcast_baselines.py",
        "tracker_forecast_contract.py",
    ):
        assert (
            integration_engine / filename
        ).read_bytes() == (worker_engine / filename).read_bytes()

    live_files = (
        repository / "custom_components/storm_tracker_v3/__init__.py",
        repository / "custom_components/storm_tracker_v3/engine/nowcast.py",
        repository / "storm_tracker_worker/service.py",
        repository / "storm_tracker_worker/tracking_core/__init__.py",
    )
    for live_file in live_files:
        source = live_file.read_text(encoding="utf-8")
        assert "nowcast_baselines" not in source
        assert "tracker_forecast_contract" not in source

    sensor_source = (
        repository / "custom_components/storm_tracker_v3/sensor.py"
    ).read_text(encoding="utf-8")
    assert "build_tracker_forecast_shadow" in sensor_source
    assert '"forecast_semantics_shadow"' in sensor_source
