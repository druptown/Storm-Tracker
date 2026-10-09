"""Proofs for the durable, never-operational worker science orchestrator."""
from __future__ import annotations

from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import zlib

import pytest

from storm_tracker_worker.evidence_to_cells_shadow import (
    EvidenceToCellsPixelAccounting,
    EvidenceToCellsShadowCapacityError,
    EvidenceToCellsShadowDeadlineError,
)
from storm_tracker_worker.science_shadow_orchestrator import (
    SCIENCE_SHADOW_OPERATIONAL_BLOCKERS,
    DurableScienceShadowOrchestrator,
    ScienceShadowCapacityError,
    ScienceShadowLimits,
    ScienceShadowOutcome,
    ScienceShadowRecoveryError,
    ScienceShadowStage,
    ScienceShadowStoreConflict,
    ScienceShadowStoreError,
    ScienceShadowWindowExpired,
    ShadowCompositionIssue,
    ShadowCompositionResult,
    ShadowEvidenceFrame,
)
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.cell_track_engine import (
    CellTrackEnginePolicy,
    CellTrackLineageHypothesis,
    LineageChildIdentity,
)
from storm_tracker_worker.tracking_core.engine.cell_track_lifecycle import (
    CellTrackLifecyclePolicy,
)
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
    detect_hierarchical_cells,
)
from storm_tracker_worker.tracking_core.engine.lucas_kanade_flow import (
    DetectionFlowAggregationConfig,
    LucasKanadeConfig,
)
from storm_tracker_worker.tracking_core.engine.recursive_track_estimator import (
    RecursiveTrackEstimatorPolicy,
)
from storm_tracker_worker.tracking_core.engine.weather_system_grouping import (
    WeatherSystemGroupingPolicy,
)


ZERO4 = (
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
)
INITIAL4 = (
    (1_000_000.0, 0.0, 0.0, 0.0),
    (0.0, 1_000_000.0, 0.0, 0.0),
    (0.0, 0.0, 400.0, 0.0),
    (0.0, 0.0, 0.0, 400.0),
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _run(number: int) -> str:
    return "run-" + format(number, "064x")


def _detector() -> HierarchicalDetectorConfig:
    return HierarchicalDetectorConfig(
        shield_threshold=1.0,
        core_threshold=5.0,
        minimum_shield_area_m2=1.0,
        minimum_core_area_m2=1.0,
        shield_position_sigma_m=500.0,
        core_position_sigma_m=300.0,
        connectivity=4,
        centroid_method="geometric",
    )


def _policy(
    detector: HierarchicalDetectorConfig,
    **changes,
) -> CellTrackEnginePolicy:
    policy = CellTrackEnginePolicy(
        policy_id="science-shadow-test-policy-v1",
        detector=detector,
        lifecycle=CellTrackLifecyclePolicy(
            policy_id="science-shadow-lifecycle-v1",
            confirmation_observations=2,
            maximum_gap_frames=2,
        ),
        estimator=RecursiveTrackEstimatorPolicy(
            policy_id="science-shadow-estimator-v1",
            acceleration_spectral_density_m2_s3=1.0,
        ),
        lucas_kanade=LucasKanadeConfig(
            window_radius_pixels=1,
            minimum_valid_samples=2,
            minimum_structure_eigenvalue=1e-12,
            maximum_condition_number=1e12,
            regularization=0.0,
            residual_variance_floor=1e-6,
            velocity_process_noise_covariance_m2_s2=(
                (1.0, 0.0),
                (0.0, 1.0),
            ),
        ),
        flow_aggregation=DetectionFlowAggregationConfig(
            minimum_valid_pixels=1,
            minimum_valid_fraction=0.1,
            velocity_covariance_floor_m2_s2=((1.0, 0.0), (0.0, 1.0)),
            residual_position_covariance_m2=(
                (100_000.0, 0.0),
                (0.0, 100_000.0),
            ),
            method="conservative_mean",
        ),
        weather_system=WeatherSystemGroupingPolicy(
            policy_id="science-shadow-system-v1",
            pixel_connectivity=8,
            maximum_footprint_gap_m=2_000.0,
            maximum_motion_difference_m_s=1_000_000.0,
            maximum_motion_age_s=10_000.0,
            maximum_motion_time_difference_s=10_000.0,
        ),
        initial_state_covariance=INITIAL4,
        prior_process_noise_covariance=ZERO4,
        association_motion_sigma_m_s=100.0,
        motion_nll_weight=1.0,
        object_kind_change_cost=1_000_000.0,
        absolute_log_area_weight=1.0,
        mean_value_difference_weight=0.0,
        maximum_value_difference_weight=0.0,
        footprint_nonoverlap_weight=1.0,
        birth_cost=1e15,
        death_cost=1e15,
        exact_solution_limit=4,
        maximum_exact_current_detections=16,
        maximum_frame_pixels=1_000,
        maximum_detected_objects=100,
        maximum_active_tracks=100,
        maximum_candidate_edges=10_000,
        maximum_total_tracks=1_000,
        maximum_lifecycle_events=5_000,
        maximum_track_history_detections=100,
    )
    return replace(policy, **changes)


def _grid() -> AnalysisGrid:
    return AnalysisGrid(
        grid_id="science-shadow-grid",
        crs="EPSG:3035",
        origin_x_m=4_000_000.0,
        origin_y_m=3_100_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=7,
        height=4,
    )


def _frame(
    run_id: str,
    valid_at: float,
    wet_pixels: tuple[tuple[int, int, float], ...],
) -> NormalizedRasterFrame:
    grid = _grid()
    values = [[0.0 for _column in range(grid.width)] for _row in range(grid.height)]
    for row, column, value in wet_pixels:
        values[row][column] = value
    return NormalizedRasterFrame(
        frame_id="shadow-frame-" + _sha(run_id + ":frame"),
        valid_at=valid_at,
        source="canonical:synthetic-radar/synthetic-rate:semantics:" + "a" * 64,
        quantity="instantaneous_precipitation_rate",
        unit="mm/h",
        grid=grid,
        values=tuple(tuple(row) for row in values),
    )


def _evidence(
    run_id: str,
    frame: NormalizedRasterFrame,
    detector: HierarchicalDetectorConfig,
) -> ShadowEvidenceFrame:
    detected = detect_hierarchical_cells(frame, detector)
    flattened = tuple(value for row in frame.values for value in row)
    valid = sum(value is not None for value in flattened)
    shield = sum(
        value is not None and value >= detector.shield_threshold
        for value in flattened
    )
    core = sum(
        value is not None and value >= detector.core_threshold
        for value in flattened
    )
    missing = len(flattened) - valid
    accounting = EvidenceToCellsPixelAccounting(
        total_analysis_pixels=len(flattened),
        valid_detector_input_pixels=valid,
        valid_below_shield_threshold_pixels=valid - shield,
        rain_shield_pixels=shield,
        embedded_core_pixels=core,
        excluded_nodata_pixels=missing,
        excluded_undetect_pixels=0,
        excluded_mixed_pixels=0,
        excluded_outside_support_pixels=0,
        detection_objects=len(detected.detections),
        rain_shield_objects=len(detected.rain_shields),
        embedded_core_objects=len(detected.embedded_cores),
        low_confidence_objects=len(detected.low_confidence_fragments),
    )
    evidence_sha = _sha(run_id + ":evidence")
    analysis_sha = _sha(run_id + ":analysis")
    transform_receipt_sha = _sha(run_id + ":transform-receipt")
    return ShadowEvidenceFrame(
        canonical_run_id=run_id,
        canonical_commit_id="commit-" + _sha(run_id + ":commit"),
        canonical_receipt_sha256=_sha(run_id + ":canonical-receipt"),
        canonical_result_sha256=_sha(run_id + ":canonical-result"),
        capture_id="capture-" + _sha(run_id + ":capture"),
        capture_receipt_sha256=_sha(run_id + ":capture-receipt"),
        capture_manifest_sha256=_sha(run_id + ":capture-manifest"),
        evidence_candidate_id="evidence-cells-" + evidence_sha,
        evidence_content_sha256=evidence_sha,
        native_candidate_id="radar-field-" + _sha(run_id + ":native"),
        native_candidate_content_sha256=_sha(run_id + ":native-content"),
        native_lineage_sha256=_sha(run_id + ":native-lineage"),
        analysis_field_id="analysis-field-" + analysis_sha,
        analysis_field_content_sha256=analysis_sha,
        analysis_lineage_sha256=_sha(run_id + ":analysis-lineage"),
        radar_semantics_certificate_id="synthetic.exact-rate.v1",
        radar_semantics_certificate_sha256=_sha("synthetic-semantics"),
        transform_certificate_id="synthetic.exact-affine.v1",
        transform_certificate_sha256=_sha("synthetic-transform"),
        transform_receipt_id="transform-receipt-" + transform_receipt_sha,
        transform_receipt_sha256=transform_receipt_sha,
        transform_config_sha256=_sha("synthetic-transform-config"),
        target_grid_sha256=_sha_grid(frame.grid),
        detector_config_sha256=_sha("synthetic-detector-config"),
        pipeline_config_sha256=_sha("synthetic-pipeline-config"),
        normalized_frame=frame,
        detection_frame=detected,
        pixel_accounting=accounting,
        operational_admission_blockers=("hard_process_isolation_not_wired",),
    )


def _sha_grid(grid: AnalysisGrid) -> str:
    from storm_tracker_worker.analysis_grid_transform import analysis_grid_sha256

    return analysis_grid_sha256(grid)


class FakeComposer:
    def __init__(self, detector: HierarchicalDetectorConfig, outcomes) -> None:
        self._detector = detector
        self.outcomes = dict(outcomes)
        self.calls: dict[str, int] = {}

    @property
    def profile_sha256(self) -> str:
        return _sha("fake-composer-profile")

    @property
    def detector_config(self) -> HierarchicalDetectorConfig:
        return self._detector

    @property
    def maximum_runtime_seconds(self) -> float:
        return 60.0

    def compose(self, *, run_id, deadline_at, monotonic):
        del deadline_at, monotonic
        self.calls[run_id] = self.calls.get(run_id, 0) + 1
        outcome = self.outcomes[run_id]
        if isinstance(outcome, BaseException):
            raise outcome
        if isinstance(outcome, ShadowCompositionResult):
            return outcome
        return ShadowCompositionResult(run_id=run_id, evidence=outcome, issues=())


def _orchestrator(
    tmp_path: Path,
    composer: FakeComposer,
    policy: CellTrackEnginePolicy,
    *,
    limits: ScienceShadowLimits | None = None,
    path: Path | None = None,
    fault_injector=None,
) -> DurableScienceShadowOrchestrator:
    return DurableScienceShadowOrchestrator(
        engine_id="science-shadow-test-engine",
        composer=composer,
        tracking_policy=policy,
        receipt_path=(path or (tmp_path / "science-shadow.sqlite3")).resolve(),
        limits=limits or ScienceShadowLimits(max_runtime_seconds=30.0),
        monotonic=lambda: 10.0,
        store_fault_injector=fault_injector,
    )


def test_happy_path_is_durable_lossless_and_never_operational(tmp_path: Path) -> None:
    detector = _detector()
    policy = _policy(detector)
    run_id = _run(1)
    evidence = _evidence(run_id, _frame(run_id, 1_000.0, ((1, 2, 2.0),)), detector)
    composer = FakeComposer(detector, {run_id: evidence})
    orchestrator = _orchestrator(tmp_path, composer, policy)

    result = orchestrator.process(
        attempt_id="attempt-one",
        run_id=run_id,
        deadline_at=20.0,
    )

    receipt = result.receipt
    assert receipt.outcome is ScienceShadowOutcome.ADVANCED
    assert receipt.stage is ScienceShadowStage.TRACKING
    assert receipt.input_state_revision == 0
    assert receipt.next_state_revision == 1
    assert receipt.provenance is not None
    assert receipt.provenance.evidence_content_sha256 == evidence.evidence_content_sha256
    assert receipt.provenance.native_lineage_sha256 == evidence.native_lineage_sha256
    assert receipt.provenance.analysis_lineage_sha256 == evidence.analysis_lineage_sha256
    assert receipt.accounting.total_analysis_pixels == 28
    assert receipt.accounting.compositor_detection_objects == 1
    assert receipt.accounting.tracker_detection_objects == 1
    assert receipt.accounting.input_tracks == 0
    assert receipt.accounting.output_tracks == 1
    assert not receipt.meteorological_admission_granted
    assert not receipt.forecast_generated
    assert not receipt.operational_snapshot_claim_generated
    assert not receipt.process_isolation_certified
    assert receipt.operational_admission_blockers == SCIENCE_SHADOW_OPERATIONAL_BLOCKERS
    assert result.transition is not None
    assert result.transition.detection_frame == evidence.detection_frame
    assert orchestrator.receipt_store.stats()["head_revision"] == 1

    with closing(sqlite3.connect(orchestrator.receipt_store.path)) as database:
        raw = bytes(
            database.execute(
                "SELECT receipt_json FROM science_shadow_receipts"
            ).fetchone()[0]
        ).decode("ascii")
    lowered = raw.lower()
    assert "rain_rate_mm_h" not in lowered
    assert '"values"' not in lowered
    assert "authorization" not in lowered
    assert "api_key" not in lowered
    assert "token" not in lowered


def test_withheld_and_invalid_inputs_are_receipted_without_state_change(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    withheld_run = _run(2)
    invalid_run = _run(3)
    valid_run = _run(4)
    composer = FakeComposer(
        detector,
        {
            withheld_run: ShadowCompositionResult(
                run_id=withheld_run,
                evidence=None,
                issues=(ShadowCompositionIssue(
                    stage=ScienceShadowStage.RADAR_BRIDGE,
                    state="withheld",
                    code="semantics_certificate_missing_or_drifted",
                ),),
            ),
            invalid_run: ShadowCompositionResult(
                run_id=invalid_run,
                evidence=None,
                issues=(ShadowCompositionIssue(
                    stage=ScienceShadowStage.ANALYSIS_TRANSFORM,
                    state="invalid",
                    code="source_candidate_checksum_mismatch",
                ),),
            ),
            valid_run: _evidence(
                valid_run,
                _frame(valid_run, 1_000.0, ((1, 2, 2.0),)),
                detector,
            ),
        },
    )
    orchestrator = _orchestrator(tmp_path, composer, policy)

    withheld = orchestrator.process(
        attempt_id="attempt-withheld",
        run_id=withheld_run,
        deadline_at=20.0,
    ).receipt
    invalid = orchestrator.process(
        attempt_id="attempt-invalid",
        run_id=invalid_run,
        deadline_at=20.0,
    ).receipt
    advanced = orchestrator.process(
        attempt_id="attempt-valid",
        run_id=valid_run,
        deadline_at=20.0,
    ).receipt

    assert withheld.outcome is ScienceShadowOutcome.WITHHELD
    assert withheld.stage is ScienceShadowStage.RADAR_BRIDGE
    assert withheld.input_state_revision == withheld.next_state_revision == 0
    assert invalid.outcome is ScienceShadowOutcome.INVALID
    assert invalid.stage is ScienceShadowStage.ANALYSIS_TRANSFORM
    assert invalid.input_state_revision == invalid.next_state_revision == 0
    assert advanced.outcome is ScienceShadowOutcome.ADVANCED
    assert advanced.input_state_revision == 0
    assert advanced.next_state_revision == 1
    stats = orchestrator.receipt_store.stats()
    assert stats["receipt_count"] == 3
    assert stats["head_revision"] == 1


def test_indeterminate_capacity_keeps_all_detections_and_exact_state(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector, maximum_detected_objects=0)
    run_id = _run(5)
    evidence = _evidence(run_id, _frame(run_id, 1_000.0, ((1, 2, 2.0),)), detector)
    composer = FakeComposer(detector, {run_id: evidence})
    orchestrator = _orchestrator(tmp_path, composer, policy)

    result = orchestrator.process(
        attempt_id="attempt-indeterminate",
        run_id=run_id,
        deadline_at=20.0,
    )
    receipt = result.receipt

    assert receipt.outcome is ScienceShadowOutcome.INDETERMINATE
    assert receipt.issue_codes == ("detected_object_capacity_exceeded",)
    assert receipt.input_state_revision == receipt.next_state_revision == 0
    assert receipt.accounting.compositor_detection_objects == 1
    assert receipt.accounting.tracker_detection_objects == 1
    assert receipt.accounting.output_tracks == 0
    assert result.transition is not None
    assert result.transition.next_state.revision == 0
    assert len(result.transition.detection_frame.detections) == 1
    assert orchestrator.receipt_store.stats()["head_revision"] == 0


def test_duplicate_retry_and_duplicate_advanced_run_do_no_scientific_work(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    run_id = _run(6)
    evidence = _evidence(run_id, _frame(run_id, 1_000.0, ((1, 2, 2.0),)), detector)
    composer = FakeComposer(detector, {run_id: evidence})
    orchestrator = _orchestrator(tmp_path, composer, policy)

    first = orchestrator.process(
        attempt_id="attempt-idempotent",
        run_id=run_id,
        deadline_at=20.0,
    )
    retry = orchestrator.process(
        attempt_id="attempt-idempotent",
        run_id=run_id,
        deadline_at=20.0,
    )
    another_key = orchestrator.process(
        attempt_id="attempt-same-run-new-key",
        run_id=run_id,
        deadline_at=20.0,
    )

    assert not first.duplicate
    assert retry.duplicate
    assert another_key.duplicate
    assert retry.receipt.receipt_sha256 == first.receipt.receipt_sha256
    assert another_key.receipt.receipt_sha256 == first.receipt.receipt_sha256
    assert retry.evidence is retry.transition is None
    assert composer.calls == {run_id: 1}
    with pytest.raises(ScienceShadowStoreConflict, match="another request"):
        orchestrator.process(
            attempt_id="attempt-idempotent",
            run_id=_run(600),
            deadline_at=20.0,
        )


def test_fresh_stores_produce_the_exact_same_content_derived_receipt(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    run_id = _run(61)
    evidence = _evidence(
        run_id,
        _frame(run_id, 1_000.0, ((1, 2, 2.0),)),
        detector,
    )
    left = _orchestrator(
        tmp_path,
        FakeComposer(detector, {run_id: evidence}),
        policy,
        path=(tmp_path / "deterministic-left.sqlite3").resolve(),
    ).process(
        attempt_id="attempt-deterministic",
        run_id=run_id,
        deadline_at=20.0,
    ).receipt
    right = _orchestrator(
        tmp_path,
        FakeComposer(detector, {run_id: evidence}),
        policy,
        path=(tmp_path / "deterministic-right.sqlite3").resolve(),
    ).process(
        attempt_id="attempt-deterministic",
        run_id=run_id,
        deadline_at=20.0,
    ).receipt

    assert left.receipt_sha256 == right.receipt_sha256
    assert left.to_mapping() == right.to_mapping()


def test_restart_replays_exact_chain_then_advances_second_frame(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    first_run = _run(7)
    second_run = _run(8)
    first_evidence = _evidence(
        first_run,
        _frame(first_run, 1_000.0, ((1, 2, 2.0),)),
        detector,
    )
    second_evidence = _evidence(
        second_run,
        _frame(second_run, 1_300.0, ((1, 3, 2.0),)),
        detector,
    )
    first_composer = FakeComposer(
        detector,
        {first_run: first_evidence, second_run: second_evidence},
    )
    path = (tmp_path / "restart.sqlite3").resolve()
    first = _orchestrator(tmp_path, first_composer, policy, path=path)
    first_result = first.process(
        attempt_id="attempt-restart-one",
        run_id=first_run,
        deadline_at=20.0,
    )

    replay_composer = FakeComposer(
        detector,
        {first_run: first_evidence, second_run: second_evidence},
    )
    restarted = _orchestrator(tmp_path, replay_composer, policy, path=path)
    second_result = restarted.process(
        attempt_id="attempt-restart-two",
        run_id=second_run,
        deadline_at=20.0,
    )

    assert second_result.receipt.outcome is ScienceShadowOutcome.ADVANCED
    assert second_result.receipt.input_state_revision == 1
    assert second_result.receipt.next_state_revision == 2
    assert second_result.receipt.previous_advanced_receipt_sha256 == (
        first_result.receipt.receipt_sha256
    )
    assert second_result.transition is not None
    assert second_result.transition.next_state.revision == 2
    assert first_composer.calls == {first_run: 1}
    assert first_run not in replay_composer.calls
    assert replay_composer.calls[second_run] == 1
    assert restarted.receipt_store.stats()["head_revision"] == 2

    duplicate_composer = FakeComposer(
        detector,
        {first_run: first_evidence, second_run: second_evidence},
    )
    duplicate_after_restart = _orchestrator(
        tmp_path,
        duplicate_composer,
        policy,
        path=path,
    ).process(
        attempt_id="attempt-restart-two",
        run_id=second_run,
        deadline_at=20.0,
    )
    assert duplicate_after_restart.duplicate
    assert duplicate_composer.calls == {}


def test_restart_detects_tampered_retained_input_before_new_state(tmp_path: Path) -> None:
    detector = _detector()
    policy = _policy(detector)
    first_run = _run(81)
    next_run = _run(82)
    original = _evidence(
        first_run,
        _frame(first_run, 1_000.0, ((1, 2, 2.0),)),
        detector,
    )
    next_evidence = _evidence(
        next_run,
        _frame(next_run, 1_300.0, ((1, 3, 2.0),)),
        detector,
    )
    path = (tmp_path / "replay-drift.sqlite3").resolve()
    _orchestrator(
        tmp_path,
        FakeComposer(detector, {first_run: original, next_run: next_evidence}),
        policy,
        path=path,
    ).process(
        attempt_id="attempt-drift-original",
        run_id=first_run,
        deadline_at=20.0,
    )

    with closing(sqlite3.connect(path)) as database, database:
        body = bytearray(
            database.execute(
                "SELECT input_zlib FROM science_shadow_retained_inputs"
            ).fetchone()[0]
        )
        body[len(body) // 2] ^= 0x01
        database.execute(
            "UPDATE science_shadow_retained_inputs SET input_zlib=?",
            (sqlite3.Binary(bytes(body)),),
        )
    restarted = _orchestrator(
        tmp_path,
        FakeComposer(detector, {next_run: next_evidence}),
        policy,
        path=path,
    )
    with pytest.raises(ScienceShadowStoreError, match="retained input"):
        restarted.process(
            attempt_id="attempt-drift-next",
            run_id=next_run,
            deadline_at=20.0,
        )
    assert restarted.receipt_store.stats()["head_revision"] == 1
    assert restarted.receipt_store.stats()["receipt_count"] == 1


def test_unknown_or_tampered_receipt_fields_fail_closed_on_recovery(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    first_run = _run(83)
    next_run = _run(84)
    evidence = _evidence(
        first_run,
        _frame(first_run, 1_000.0, ((1, 2, 2.0),)),
        detector,
    )
    next_evidence = _evidence(
        next_run,
        _frame(next_run, 1_300.0, ((1, 3, 2.0),)),
        detector,
    )
    path = (tmp_path / "receipt-tamper.sqlite3").resolve()
    original = _orchestrator(
        tmp_path,
        FakeComposer(detector, {first_run: evidence, next_run: next_evidence}),
        policy,
        path=path,
    )
    original.process(
        attempt_id="attempt-tamper-original",
        run_id=first_run,
        deadline_at=20.0,
    )
    with closing(sqlite3.connect(path)) as database, database:
        body = bytes(
            database.execute(
                "SELECT receipt_json FROM science_shadow_receipts"
            ).fetchone()[0]
        )
        document = json.loads(body.decode("ascii"))
        document["unknown_future_claim"] = True
        tampered = json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        database.execute(
            "UPDATE science_shadow_receipts SET receipt_json=?",
            (sqlite3.Binary(tampered),),
        )

    restarted = _orchestrator(
        tmp_path,
        FakeComposer(detector, {first_run: evidence, next_run: next_evidence}),
        policy,
        path=path,
    )
    with pytest.raises(ScienceShadowStoreError, match="unknown or non-exact"):
        restarted.process(
            attempt_id="attempt-after-tamper",
            run_id=next_run,
            deadline_at=20.0,
        )


def test_capacity_and_deadline_outcomes_are_explicit_and_retry_bounded(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    capacity_run = _run(9)
    deadline_run = _run(10)
    composer = FakeComposer(
        detector,
        {
            capacity_run: EvidenceToCellsShadowCapacityError("bounded"),
            deadline_run: EvidenceToCellsShadowDeadlineError("expired"),
        },
    )
    orchestrator = _orchestrator(tmp_path, composer, policy)

    capacity = orchestrator.process(
        attempt_id="attempt-capacity",
        run_id=capacity_run,
        deadline_at=20.0,
    ).receipt
    deadline = orchestrator.process(
        attempt_id="attempt-deadline",
        run_id=deadline_run,
        deadline_at=20.0,
    ).receipt

    assert capacity.outcome is ScienceShadowOutcome.CAPACITY_EXCEEDED
    assert capacity.issue_codes == ("composition_capacity_exceeded",)
    assert capacity.input_state_revision == capacity.next_state_revision == 0
    assert deadline.outcome is ScienceShadowOutcome.DEADLINE_EXCEEDED
    assert deadline.issue_codes == ("composition_deadline_exceeded",)
    assert deadline.input_state_revision == deadline.next_state_revision == 0
    assert orchestrator.receipt_store.stats()["head_revision"] == 0

    checkpoint_bounded = _orchestrator(
        tmp_path,
        FakeComposer(detector, {
            _run(901): _evidence(
                _run(901),
                _frame(_run(901), 1_000.0, ((1, 2, 2.0),)),
                detector,
            ),
        }),
        policy,
        limits=ScienceShadowLimits(
            max_receipts=2,
            max_replay_frames=1,
            checkpoint_interval_frames=1,
            max_checkpoint_bytes=4_096,
            max_runtime_seconds=30.0,
        ),
        path=(tmp_path / "checkpoint-bounded.sqlite3").resolve(),
    )
    bounded = checkpoint_bounded.process(
        attempt_id="attempt-checkpoint-capacity",
        run_id=_run(901),
        deadline_at=20.0,
    ).receipt
    assert bounded.outcome is ScienceShadowOutcome.CAPACITY_EXCEEDED
    assert bounded.issue_codes == ("state_checkpoint_capacity_exceeded",)
    assert checkpoint_bounded.receipt_store.stats()["head_revision"] == 0


def test_tracking_overrun_is_receipted_but_never_commits_computed_state(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    run_id = _run(103)
    evidence = _evidence(
        run_id,
        _frame(run_id, 1_000.0, ((1, 2, 2.0),)),
        detector,
    )
    composer = FakeComposer(detector, {run_id: evidence})
    readings = iter((10.0, 10.0, 21.0))
    orchestrator = DurableScienceShadowOrchestrator(
        engine_id="science-shadow-test-engine",
        composer=composer,
        tracking_policy=policy,
        receipt_path=(tmp_path / "tracking-overrun.sqlite3").resolve(),
        limits=ScienceShadowLimits(max_runtime_seconds=30.0),
        monotonic=lambda: next(readings),
    )

    result = orchestrator.process(
        attempt_id="attempt-tracking-overrun",
        run_id=run_id,
        deadline_at=20.0,
    )

    assert result.receipt.outcome is ScienceShadowOutcome.DEADLINE_EXCEEDED
    assert result.receipt.stage is ScienceShadowStage.TRACKING
    assert result.receipt.issue_codes == ("tracking_deadline_exceeded",)
    assert result.receipt.accounting.compositor_detection_objects == 1
    assert result.receipt.accounting.tracker_detection_objects == 1
    assert result.receipt.accounting.input_tracks == 0
    assert result.receipt.accounting.output_tracks == 0
    assert result.receipt.input_state_revision == 0
    assert result.receipt.next_state_revision == 0
    assert result.transition is None
    assert orchestrator.receipt_store.stats()["head_revision"] == 0


def test_tracking_pixel_capacity_and_detector_mismatch_are_fail_closed(
    tmp_path: Path,
) -> None:
    detector = _detector()
    run_capacity = _run(101)
    capacity_evidence = _evidence(
        run_capacity,
        _frame(run_capacity, 1_000.0, ((1, 2, 2.0),)),
        detector,
    )
    capacity_policy = _policy(
        detector,
        maximum_frame_pixels=10,
        maximum_detected_objects=20,
    )
    capacity_composer = FakeComposer(detector, {run_capacity: capacity_evidence})
    capacity = _orchestrator(
        tmp_path,
        capacity_composer,
        capacity_policy,
        path=(tmp_path / "tracking-capacity.sqlite3").resolve(),
    ).process(
        attempt_id="attempt-tracking-pixel-capacity",
        run_id=run_capacity,
        deadline_at=20.0,
    ).receipt
    assert capacity.outcome is ScienceShadowOutcome.CAPACITY_EXCEEDED
    assert capacity.stage is ScienceShadowStage.TRACKING
    assert capacity.issue_codes == ("tracking_frame_pixel_capacity_exceeded",)
    assert capacity.provenance is not None
    assert capacity.accounting.compositor_detection_objects == 1
    assert capacity.accounting.tracker_detection_objects == 0
    assert capacity.input_state_revision == capacity.next_state_revision == 0

    run_mismatch = _run(102)
    original = _evidence(
        run_mismatch,
        _frame(run_mismatch, 1_000.0, ((1, 2, 2.0),)),
        detector,
    )
    forged_frame = replace(
        original.normalized_frame,
        values=tuple(
            tuple(
                2.0 if (row, column) == (1, 5) else 0.0
                for column in range(original.normalized_frame.grid.width)
            )
            for row in range(original.normalized_frame.grid.height)
        ),
    )
    forged_view = _evidence(run_mismatch, forged_frame, detector)
    mismatch = replace(
        original,
        detection_frame=forged_view.detection_frame,
        pixel_accounting=forged_view.pixel_accounting,
    )
    mismatch_composer = FakeComposer(detector, {run_mismatch: mismatch})
    mismatch_result = _orchestrator(
        tmp_path,
        mismatch_composer,
        _policy(detector),
        path=(tmp_path / "detector-mismatch.sqlite3").resolve(),
    ).process(
        attempt_id="attempt-detector-mismatch",
        run_id=run_mismatch,
        deadline_at=20.0,
    )
    assert mismatch_result.receipt.outcome is ScienceShadowOutcome.INVALID
    assert mismatch_result.receipt.issue_codes == ("detector_replay_mismatch",)
    assert mismatch_result.receipt.accounting.compositor_detection_objects == 1
    assert mismatch_result.receipt.accounting.tracker_detection_objects == 1
    assert mismatch_result.receipt.input_state_revision == 0
    assert mismatch_result.receipt.next_state_revision == 0
    assert mismatch_result.transition is None


def test_selected_split_lineage_and_provenance_survive_restart(tmp_path: Path) -> None:
    detector = _detector()
    policy = _policy(detector)
    first_run = _run(11)
    split_run = _run(12)
    after_run = _run(13)
    first_evidence = _evidence(
        first_run,
        _frame(first_run, 1_000.0, ((1, 2, 2.0),)),
        detector,
    )
    split_evidence = _evidence(
        split_run,
        _frame(split_run, 1_300.0, ((1, 1, 2.0), (1, 5, 2.0))),
        detector,
    )
    after_evidence = _evidence(
        after_run,
        _frame(after_run, 1_600.0, ((1, 1, 2.0), (1, 5, 2.0))),
        detector,
    )
    composer = FakeComposer(
        detector,
        {
            first_run: first_evidence,
            split_run: split_evidence,
            after_run: after_evidence,
        },
    )
    path = (tmp_path / "lineage.sqlite3").resolve()
    limits = ScienceShadowLimits(
        max_receipts=4,
        max_replay_frames=2,
        checkpoint_interval_frames=2,
        max_runtime_seconds=30.0,
    )
    orchestrator = _orchestrator(
        tmp_path,
        composer,
        policy,
        limits=limits,
        path=path,
    )
    first = orchestrator.process(
        attempt_id="attempt-lineage-one",
        run_id=first_run,
        deadline_at=20.0,
    )
    assert first.transition is not None
    parent_track = first.transition.next_state.ledger.tracks[0].track_id
    detections = tuple(
        sorted(
            item.detection.detection_id
            for item in split_evidence.detection_frame.rain_shields
        )
    )
    hypothesis = CellTrackLineageHypothesis(
        hypothesis_id="selected-split-hypothesis",
        event_type="split",
        parent_track_ids=(parent_track,),
        children=tuple(
            LineageChildIdentity(
                detection_id=detection_id,
                track_id=f"split-child-{index}",
            )
            for index, detection_id in enumerate(detections, start=1)
        ),
        cost=1.0,
        cost_model_id="explicit-human-reviewed-split-v1",
        cost_components=(("reviewed_geometry", 1.0),),
        provenance_ids=(
            split_evidence.evidence_candidate_id,
            first_evidence.evidence_candidate_id,
        ) if split_evidence.evidence_candidate_id < first_evidence.evidence_candidate_id else (
            first_evidence.evidence_candidate_id,
            split_evidence.evidence_candidate_id,
        ),
        selected=True,
    )
    split = orchestrator.process(
        attempt_id="attempt-lineage-split",
        run_id=split_run,
        deadline_at=20.0,
        lineage_hypotheses=(hypothesis,),
    )

    assert split.receipt.outcome is ScienceShadowOutcome.ADVANCED
    assert split.receipt.lineage_hypotheses == (hypothesis,)
    assert split.receipt.applied_lineage_hypothesis_ids == (
        hypothesis.hypothesis_id,
    )
    assert split.receipt.provenance is not None
    assert split.receipt.provenance.canonical_receipt_sha256 == (
        split_evidence.canonical_receipt_sha256
    )
    assert split.transition is not None
    assert len(split.transition.next_state.ledger.tracks) == 3

    restarted = _orchestrator(
        tmp_path,
        composer,
        policy,
        limits=limits,
        path=path,
    )
    after = restarted.process(
        attempt_id="attempt-lineage-after-restart",
        run_id=after_run,
        deadline_at=20.0,
    )
    assert after.receipt.outcome is ScienceShadowOutcome.ADVANCED
    assert after.receipt.input_state_revision == 2
    assert after.receipt.next_state_revision == 3
    assert composer.calls[first_run] == 1
    assert composer.calls[split_run] == 1


def test_fault_before_commit_rolls_back_receipt_and_head_atomically(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    run_id = _run(14)
    evidence = _evidence(run_id, _frame(run_id, 1_000.0, ((1, 2, 2.0),)), detector)
    composer = FakeComposer(detector, {run_id: evidence})

    def fault(point: str) -> None:
        if point == "record.after_head":
            raise RuntimeError("injected-crash")

    orchestrator = _orchestrator(
        tmp_path,
        composer,
        policy,
        fault_injector=fault,
    )
    with pytest.raises(RuntimeError, match="injected-crash"):
        orchestrator.process(
            attempt_id="attempt-atomic",
            run_id=run_id,
            deadline_at=20.0,
        )
    assert orchestrator.receipt_store.stats()["receipt_count"] == 0
    assert orchestrator.receipt_store.stats()["head_revision"] == 0


def test_checkpoint_removes_old_replay_boundary_and_restarts_across_it(
    tmp_path: Path,
) -> None:
    """Small injected 2/3 analogue proves the old 512/513 stop is gone."""

    detector = _detector()
    policy = _policy(detector)
    runs = tuple(_run(number) for number in range(20, 24))
    evidences = {
        run_id: _evidence(
            run_id,
            _frame(run_id, 1_000.0 + index * 300.0, ((1, 1 + index, 2.0),)),
            detector,
        )
        for index, run_id in enumerate(runs)
    }
    composer = FakeComposer(detector, evidences)
    limits = ScienceShadowLimits(
        max_receipts=4,
        max_replay_frames=2,
        checkpoint_interval_frames=2,
        max_runtime_seconds=30.0,
    )
    path = (tmp_path / "replay-boundary.sqlite3").resolve()
    initial = _orchestrator(
        tmp_path,
        composer,
        policy,
        limits=limits,
        path=path,
    )
    for index, run_id in enumerate(runs[:2]):
        result = initial.process(
            attempt_id=f"attempt-boundary-{index}",
            run_id=run_id,
            deadline_at=20.0,
        )
        assert result.receipt.outcome is ScienceShadowOutcome.ADVANCED

    at_boundary = _orchestrator(
        tmp_path,
        composer,
        policy,
        limits=limits,
        path=path,
    )
    third = at_boundary.process(
        attempt_id="attempt-boundary-2",
        run_id=runs[2],
        deadline_at=20.0,
    )
    assert third.receipt.outcome is ScienceShadowOutcome.ADVANCED
    assert third.receipt.input_state_revision == 2
    assert third.receipt.next_state_revision == 3

    above_boundary = _orchestrator(
        tmp_path,
        composer,
        policy,
        limits=limits,
        path=path,
    )
    fourth = above_boundary.process(
        attempt_id="attempt-boundary-3",
        run_id=runs[3],
        deadline_at=20.0,
    )
    assert fourth.receipt.outcome is ScienceShadowOutcome.ADVANCED
    assert fourth.receipt.input_state_revision == 3
    assert fourth.receipt.next_state_revision == 4
    assert above_boundary.receipt_store.stats()["head_revision"] == 4
    assert above_boundary.receipt_store.stats()["checkpoint_revision"] == 4
    assert above_boundary.receipt_store.stats()["replay_tail_frames"] == 0
    assert above_boundary.receipt_store.stats()["receipt_count"] == 4


def test_checkpoint_retention_is_exact_inside_window_and_explicit_outside(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    runs = tuple(_run(number) for number in range(200, 206))
    evidences = {
        run_id: _evidence(
            run_id,
            _frame(run_id, 1_000.0 + index * 300.0, ()),
            detector,
        )
        for index, run_id in enumerate(runs)
    }
    limits = ScienceShadowLimits(
        max_receipts=2,
        max_replay_frames=2,
        checkpoint_interval_frames=2,
        expired_filter_bytes=1_024,
        max_expired_memberships=100,
        max_runtime_seconds=30.0,
    )
    path = (tmp_path / "retention-window.sqlite3").resolve()
    initial = _orchestrator(
        tmp_path,
        FakeComposer(detector, evidences),
        policy,
        limits=limits,
        path=path,
    )
    results = []
    for index, run_id in enumerate(runs[:5]):
        results.append(initial.process(
            attempt_id=f"attempt-window-{index}",
            run_id=run_id,
            deadline_at=20.0,
        ))
    stats = initial.receipt_store.stats()
    assert stats["head_revision"] == 5
    assert stats["checkpoint_revision"] == 4
    assert stats["replay_tail_frames"] == 1
    assert stats["receipt_count"] == 2
    assert stats["expired_memberships"] == 6

    exact = initial.process(
        attempt_id="attempt-window-4",
        run_id=runs[4],
        deadline_at=20.0,
    )
    assert exact.duplicate
    assert exact.receipt.receipt_sha256 == results[4].receipt.receipt_sha256
    retained_run = initial.process(
        attempt_id="attempt-window-new-key",
        run_id=runs[3],
        deadline_at=20.0,
    )
    assert retained_run.duplicate
    assert retained_run.receipt.receipt_sha256 == results[3].receipt.receipt_sha256

    with pytest.raises(ScienceShadowWindowExpired, match="window_expired"):
        initial.process(
            attempt_id="attempt-window-0",
            run_id=runs[0],
            deadline_at=20.0,
        )
    with pytest.raises(ScienceShadowWindowExpired, match="window_expired"):
        initial.process(
            attempt_id="attempt-window-expired-run-new-key",
            run_id=runs[0],
            deadline_at=20.0,
        )

    restart_composer = FakeComposer(detector, {runs[5]: evidences[runs[5]]})
    restarted = _orchestrator(
        tmp_path,
        restart_composer,
        policy,
        limits=limits,
        path=path,
    )
    sixth = restarted.process(
        attempt_id="attempt-window-5",
        run_id=runs[5],
        deadline_at=20.0,
    )
    assert sixth.receipt.outcome is ScienceShadowOutcome.ADVANCED
    assert sixth.receipt.next_state_revision == 6
    assert restart_composer.calls == {runs[5]: 1}
    assert restarted.receipt_store.stats()["checkpoint_revision"] == 6


@pytest.mark.parametrize(
    "fault_point",
    (
        "record.after_receipt",
        "record.after_input",
        "record.after_head",
        "record.after_checkpoint_blob",
        "record.after_prune_input",
        "record.after_expired_filter",
        "record.after_prune_receipt",
        "record.before_commit",
    ),
)
def test_every_checkpoint_faultpoint_is_atomically_old_or_new(
    tmp_path: Path,
    fault_point: str,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    first_run = _run(300)
    second_run = _run(301)
    evidences = {
        first_run: _evidence(first_run, _frame(first_run, 1_000.0, ()), detector),
        second_run: _evidence(second_run, _frame(second_run, 1_300.0, ()), detector),
    }
    limits = ScienceShadowLimits(
        max_receipts=1,
        max_replay_frames=1,
        checkpoint_interval_frames=1,
        expired_filter_bytes=1_024,
        max_expired_memberships=20,
        max_runtime_seconds=30.0,
    )
    path = (tmp_path / f"atomic-{fault_point.rsplit('.', 1)[-1]}.sqlite3").resolve()
    first = _orchestrator(
        tmp_path,
        FakeComposer(detector, evidences),
        policy,
        limits=limits,
        path=path,
    ).process(
        attempt_id="attempt-atomic-first",
        run_id=first_run,
        deadline_at=20.0,
    )

    def fault(point: str) -> None:
        if point == fault_point:
            raise RuntimeError("injected-checkpoint-crash")

    crashing = _orchestrator(
        tmp_path,
        FakeComposer(detector, evidences),
        policy,
        limits=limits,
        path=path,
        fault_injector=fault,
    )
    with pytest.raises(RuntimeError, match="injected-checkpoint-crash"):
        crashing.process(
            attempt_id="attempt-atomic-second",
            run_id=second_run,
            deadline_at=20.0,
        )

    reopened = _orchestrator(
        tmp_path,
        FakeComposer(detector, {second_run: evidences[second_run]}),
        policy,
        limits=limits,
        path=path,
    )
    old = reopened.receipt_store.stats()
    assert old["head_revision"] == old["checkpoint_revision"] == 1
    assert old["receipt_count"] == 1
    assert old["expired_memberships"] == 0
    assert reopened.receipt_store.load_attempt(
        "attempt-atomic-first"
    ).receipt_sha256 == first.receipt.receipt_sha256
    committed = reopened.process(
        attempt_id="attempt-atomic-second",
        run_id=second_run,
        deadline_at=20.0,
    )
    assert committed.receipt.next_state_revision == 2
    final = reopened.receipt_store.stats()
    assert final["head_revision"] == final["checkpoint_revision"] == 2
    assert final["receipt_count"] == 1
    assert final["expired_memberships"] == 2


@pytest.mark.parametrize(
    "mutation",
    ("checkpoint_blob", "tail_gap", "head_lineage", "checkpoint_binding"),
)
def test_checkpoint_checksum_gap_and_lineage_tampering_fail_closed(
    tmp_path: Path,
    mutation: str,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    runs = (_run(400), _run(401), _run(402), _run(403))
    evidences = {
        run_id: _evidence(
            run_id,
            _frame(run_id, 1_000.0 + index * 300.0, ()),
            detector,
        )
        for index, run_id in enumerate(runs)
    }
    limits = ScienceShadowLimits(
        max_receipts=4,
        max_replay_frames=2,
        checkpoint_interval_frames=2,
        max_runtime_seconds=30.0,
    )
    path = (tmp_path / f"tamper-{mutation}.sqlite3").resolve()
    original = _orchestrator(
        tmp_path,
        FakeComposer(detector, evidences),
        policy,
        limits=limits,
        path=path,
    )
    for index, run_id in enumerate(runs[:3]):
        original.process(
            attempt_id=f"attempt-tamper-chain-{index}",
            run_id=run_id,
            deadline_at=20.0,
        )
    with closing(sqlite3.connect(path)) as database, database:
        if mutation == "checkpoint_blob":
            value = bytearray(
                database.execute(
                    "SELECT state_zlib FROM science_shadow_checkpoint"
                ).fetchone()[0]
            )
            value[len(value) // 2] ^= 0x01
            database.execute(
                "UPDATE science_shadow_checkpoint SET state_zlib=?",
                (sqlite3.Binary(bytes(value)),),
            )
        elif mutation == "tail_gap":
            database.execute(
                "DELETE FROM science_shadow_retained_inputs WHERE revision=3"
            )
        elif mutation == "head_lineage":
            database.execute(
                "UPDATE science_shadow_head SET provenance_chain_sha256=?",
                ("0" * 64,),
            )
        else:
            database.execute(
                "UPDATE science_shadow_checkpoint SET advanced_receipt_sha256=?",
                ("0" * 64,),
            )
    restarted = _orchestrator(
        tmp_path,
        FakeComposer(detector, {runs[3]: evidences[runs[3]]}),
        policy,
        limits=limits,
        path=path,
    )
    with pytest.raises((ScienceShadowStoreError, ScienceShadowRecoveryError)):
        restarted.process(
            attempt_id="attempt-after-chain-tamper",
            run_id=runs[3],
            deadline_at=20.0,
        )


def test_concurrent_orchestrators_use_one_durable_cas_head(tmp_path: Path) -> None:
    detector = _detector()
    policy = _policy(detector)
    runs = (_run(500), _run(501))
    evidences = {
        run_id: _evidence(run_id, _frame(run_id, 1_000.0, ()), detector)
        for run_id in runs
    }
    barrier = threading.Barrier(2)

    class BarrierComposer(FakeComposer):
        def compose(self, *, run_id, deadline_at, monotonic):
            result = super().compose(
                run_id=run_id,
                deadline_at=deadline_at,
                monotonic=monotonic,
            )
            barrier.wait(timeout=10.0)
            return result

    path = (tmp_path / "concurrent-cas.sqlite3").resolve()
    left = _orchestrator(
        tmp_path,
        BarrierComposer(detector, evidences),
        policy,
        path=path,
    )
    right = _orchestrator(
        tmp_path,
        BarrierComposer(detector, evidences),
        policy,
        path=path,
    )

    def execute(index: int):
        orchestrator = left if index == 0 else right
        return orchestrator.process(
            attempt_id=f"attempt-concurrent-{index}",
            run_id=runs[index],
            deadline_at=20.0,
        )

    outcomes: list[object] = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(execute, index) for index in range(2)]
        for future in futures:
            try:
                outcomes.append(future.result(timeout=20.0))
            except BaseException as exc:
                outcomes.append(exc)
    assert sum(
        getattr(item, "receipt", None) is not None for item in outcomes
    ) == 1
    assert sum(isinstance(item, ScienceShadowStoreConflict) for item in outcomes) == 1
    assert left.receipt_store.stats()["head_revision"] == 1
    assert left.receipt_store.stats()["receipt_count"] == 1


def test_thousands_of_frames_plateau_and_restart_with_bounded_tail(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)

    class DryComposer:
        def __init__(self) -> None:
            self.calls = 0

        @property
        def profile_sha256(self) -> str:
            return _sha("dry-plateau-composer-v1")

        @property
        def detector_config(self) -> HierarchicalDetectorConfig:
            return detector

        @property
        def maximum_runtime_seconds(self) -> float:
            return 60.0

        def compose(self, *, run_id, deadline_at, monotonic):
            del deadline_at, monotonic
            self.calls += 1
            number = int(run_id[4:], 16)
            frame = _frame(run_id, 10_000.0 + number * 300.0, ())
            return ShadowCompositionResult(
                run_id=run_id,
                evidence=_evidence(run_id, frame, detector),
                issues=(),
            )

    limits = ScienceShadowLimits(
        max_receipts=16,
        max_database_bytes=4 * 1024 * 1024,
        max_replay_frames=8,
        checkpoint_interval_frames=8,
        max_checkpoint_bytes=128 * 1024,
        max_retained_input_bytes=128 * 1024,
        expired_filter_bytes=32 * 1024,
        max_expired_memberships=5_000,
        max_runtime_seconds=30.0,
    )
    path = (tmp_path / "thousands-plateau.sqlite3").resolve()
    composer = DryComposer()
    orchestrator = DurableScienceShadowOrchestrator(
        engine_id="science-shadow-test-engine",
        composer=composer,
        tracking_policy=policy,
        receipt_path=path,
        limits=limits,
        monotonic=lambda: 10.0,
    )
    plateau_bytes = 0
    total_frames = 2_100
    for index in range(total_frames):
        result = orchestrator.process(
            attempt_id=f"attempt-plateau-{index}",
            run_id=_run(10_000 + index),
            deadline_at=20.0,
        )
        assert result.receipt.outcome is ScienceShadowOutcome.ADVANCED
        if index == 511:
            boundary = orchestrator.receipt_store.stats()
            plateau_bytes = boundary["database_file_bytes"]
            assert boundary["head_revision"] == 512
            assert boundary["checkpoint_revision"] == 512
            assert boundary["replay_tail_frames"] == 0
            # Fresh process state at exactly 512; the next loop iteration is
            # frame/revision 513 and must advance instead of hitting the old stop.
            orchestrator = DurableScienceShadowOrchestrator(
                engine_id="science-shadow-test-engine",
                composer=DryComposer(),
                tracking_policy=policy,
                receipt_path=path,
                limits=limits,
                monotonic=lambda: 10.0,
            )
    stats = orchestrator.receipt_store.stats()
    assert stats["head_revision"] == total_frames
    assert stats["checkpoint_revision"] == 2_096
    assert stats["replay_tail_frames"] == 4
    assert stats["receipt_count"] == limits.max_receipts
    assert stats["expired_memberships"] == 2 * (
        total_frames - limits.max_receipts
    )
    assert stats["database_file_bytes"] <= plateau_bytes + 128 * 1024
    assert stats["database_file_bytes"] <= limits.max_database_bytes

    restarted_composer = DryComposer()
    restarted = DurableScienceShadowOrchestrator(
        engine_id="science-shadow-test-engine",
        composer=restarted_composer,
        tracking_policy=policy,
        receipt_path=path,
        limits=limits,
        monotonic=lambda: 10.0,
    )
    after = restarted.process(
        attempt_id="attempt-plateau-after-restart",
        run_id=_run(20_000),
        deadline_at=20.0,
    )
    assert after.receipt.next_state_revision == total_frames + 1
    assert restarted_composer.calls == 1
    module_source = (
        Path(__file__).resolve().parents[1]
        / "storm_tracker_worker"
        / "science_shadow_orchestrator.py"
    ).read_text(encoding="utf-8")
    assert 'execute("VACUUM' not in module_source
    assert "execute('VACUUM" not in module_source


def test_retention_lifetime_capacity_rolls_back_without_silent_loss(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    runs = (_run(600), _run(601), _run(602))
    evidences = {
        run_id: _evidence(
            run_id,
            _frame(run_id, 1_000.0 + index * 300.0, ()),
            detector,
        )
        for index, run_id in enumerate(runs)
    }
    limits = ScienceShadowLimits(
        max_receipts=1,
        max_replay_frames=1,
        checkpoint_interval_frames=1,
        expired_filter_bytes=1_024,
        max_expired_memberships=2,
        max_runtime_seconds=30.0,
    )
    orchestrator = _orchestrator(
        tmp_path,
        FakeComposer(detector, evidences),
        policy,
        limits=limits,
        path=(tmp_path / "retention-lifetime.sqlite3").resolve(),
    )
    for index, run_id in enumerate(runs[:2]):
        orchestrator.process(
            attempt_id=f"attempt-lifetime-{index}",
            run_id=run_id,
            deadline_at=20.0,
        )
    before = orchestrator.receipt_store.stats()
    assert before["head_revision"] == 2
    assert before["expired_memberships"] == 2
    with pytest.raises(ScienceShadowCapacityError, match="lifetime capacity"):
        orchestrator.process(
            attempt_id="attempt-lifetime-2",
            run_id=runs[2],
            deadline_at=20.0,
        )
    after = orchestrator.receipt_store.stats()
    assert after == before


def test_retained_payload_privacy_is_bounded_and_secret_labels_are_receipted(
    tmp_path: Path,
) -> None:
    detector = _detector()
    policy = _policy(detector)
    good_run = _run(700)
    bad_run = _run(701)
    good = _evidence(good_run, _frame(good_run, 1_000.0, ()), detector)
    bad_frame = replace(
        _frame(bad_run, 1_300.0, ()),
        source="canonical:provider/access_token/forbidden",
    )
    bad = _evidence(bad_run, bad_frame, detector)
    limits = ScienceShadowLimits(
        max_receipts=4,
        max_replay_frames=1,
        checkpoint_interval_frames=1,
        max_runtime_seconds=30.0,
    )
    path = (tmp_path / "privacy.sqlite3").resolve()
    orchestrator = _orchestrator(
        tmp_path,
        FakeComposer(detector, {good_run: good, bad_run: bad}),
        policy,
        limits=limits,
        path=path,
    )
    good_result = orchestrator.process(
        attempt_id="attempt-privacy-good",
        run_id=good_run,
        deadline_at=20.0,
    )
    assert good_result.receipt.outcome is ScienceShadowOutcome.ADVANCED
    bad_orchestrator = _orchestrator(
        tmp_path,
        FakeComposer(detector, {bad_run: bad}),
        policy,
        limits=limits,
        path=(tmp_path / "privacy-rejected.sqlite3").resolve(),
    )
    bad_result = bad_orchestrator.process(
        attempt_id="attempt-privacy-bad",
        run_id=bad_run,
        deadline_at=20.0,
    )
    assert bad_result.receipt.outcome is ScienceShadowOutcome.INVALID
    assert bad_result.receipt.issue_codes == ("retained_input_privacy_rejected",)
    assert bad_result.receipt.next_state_revision == 0
    with closing(sqlite3.connect(path)) as database:
        blobs = database.execute(
            "SELECT checkpoint_json,receipt_json,state_zlib "
            "FROM science_shadow_checkpoint"
        ).fetchone()
        checkpoint_text = bytes(blobs[0]).decode("ascii").lower()
        receipt_text = bytes(blobs[1]).decode("ascii").lower()
        state_text = zlib.decompress(bytes(blobs[2])).decode("ascii").lower()
    for text in (checkpoint_text, receipt_text, state_text):
        assert "authorization" not in text
        assert "api_key" not in text
        assert "access_token" not in text
        assert "password" not in text
    assert "values" in state_text


def test_orchestrator_has_no_live_worker_or_home_assistant_activation() -> None:
    root = Path(__file__).resolve().parents[1]
    needle = "science_shadow_orchestrator"
    for relative in (
        "storm_tracker_worker/service.py",
        "storm_tracker_worker/run.py",
        "storm_tracker_worker/config.py",
        "custom_components/storm_tracker_v3/__init__.py",
        "custom_components/storm_tracker_v3/sensor.py",
    ):
        path = root / relative
        if path.exists():
            assert needle not in path.read_text(encoding="utf-8")
