"""Spawn-importable synthetic factory for science process-boundary proofs.

The helper deliberately constructs the real durable orchestrator, detector and
tracking engine.  Only the upstream radar composition is synthetic; no raster
or other large object is transported over IPC.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import time
from typing import Any, Mapping

from storm_tracker_worker.analysis_grid_transform import analysis_grid_sha256
from storm_tracker_worker.evidence_to_cells_shadow import (
    EvidenceToCellsPixelAccounting,
)
from storm_tracker_worker.science_shadow_orchestrator import (
    DurableScienceShadowOrchestrator,
    ScienceShadowLimits,
    ShadowCompositionResult,
    ShadowEvidenceFrame,
)
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.cell_track_engine import (
    CellTrackEnginePolicy,
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


def _policy(detector: HierarchicalDetectorConfig) -> CellTrackEnginePolicy:
    return CellTrackEnginePolicy(
        policy_id="science-process-test-policy-v1",
        detector=detector,
        lifecycle=CellTrackLifecyclePolicy(
            policy_id="science-process-test-lifecycle-v1",
            confirmation_observations=2,
            maximum_gap_frames=2,
        ),
        estimator=RecursiveTrackEstimatorPolicy(
            policy_id="science-process-test-estimator-v1",
            acceleration_spectral_density_m2_s3=1.0,
        ),
        lucas_kanade=LucasKanadeConfig(
            window_radius_pixels=1,
            minimum_valid_samples=2,
            minimum_structure_eigenvalue=1e-12,
            maximum_condition_number=1e12,
            regularization=0.0,
            residual_variance_floor=1e-6,
            velocity_process_noise_covariance_m2_s2=((1.0, 0.0), (0.0, 1.0)),
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
            policy_id="science-process-test-system-v1",
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


def _grid() -> AnalysisGrid:
    return AnalysisGrid(
        grid_id="science-process-test-grid",
        crs="EPSG:3035",
        origin_x_m=4_000_000.0,
        origin_y_m=3_100_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=7,
        height=4,
    )


def _evidence(
    run_id: str,
    detector: HierarchicalDetectorConfig,
    *,
    two_cells: bool = False,
) -> ShadowEvidenceFrame:
    grid = _grid()
    sequence = int(run_id.removeprefix("run-"), 16)
    column = 1 + sequence % 5
    values = [[0.0 for _column in range(grid.width)] for _row in range(grid.height)]
    values[1][column] = 2.0
    if two_cells:
        values[1][5 if column <= 2 else 1] = 2.0
    frame = NormalizedRasterFrame(
        frame_id="shadow-frame-" + _sha(run_id + ":frame"),
        valid_at=1_000.0 + float(sequence % 10_000_000),
        source="canonical:synthetic-radar/synthetic-rate:semantics:" + "a" * 64,
        quantity="instantaneous_precipitation_rate",
        unit="mm/h",
        grid=grid,
        values=tuple(tuple(row) for row in values),
    )
    detected = detect_hierarchical_cells(frame, detector)
    flattened = tuple(value for row in frame.values for value in row)
    shield = sum(value >= detector.shield_threshold for value in flattened)
    core = sum(value >= detector.core_threshold for value in flattened)
    accounting = EvidenceToCellsPixelAccounting(
        total_analysis_pixels=len(flattened),
        valid_detector_input_pixels=len(flattened),
        valid_below_shield_threshold_pixels=len(flattened) - shield,
        rain_shield_pixels=shield,
        embedded_core_pixels=core,
        excluded_nodata_pixels=0,
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
    transform_sha = _sha(run_id + ":transform-receipt")
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
        transform_receipt_id="transform-receipt-" + transform_sha,
        transform_receipt_sha256=transform_sha,
        transform_config_sha256=_sha("synthetic-transform-config"),
        target_grid_sha256=analysis_grid_sha256(grid),
        detector_config_sha256=_sha("synthetic-detector-config"),
        pipeline_config_sha256=_sha("synthetic-pipeline-config"),
        normalized_frame=frame,
        detection_frame=detected,
        pixel_accounting=accounting,
        operational_admission_blockers=("hard_process_isolation_not_wired",),
    )


class _SyntheticComposer:
    def __init__(
        self,
        detector: HierarchicalDetectorConfig,
        *,
        two_cells: bool = False,
    ) -> None:
        self._detector = detector
        self._two_cells = two_cells

    @property
    def profile_sha256(self) -> str:
        return _sha(
            "science-process-synthetic-composer-v1:two-cells="
            + ("true" if self._two_cells else "false")
        )

    @property
    def detector_config(self) -> HierarchicalDetectorConfig:
        return self._detector

    @property
    def maximum_runtime_seconds(self) -> float:
        return 60.0

    def compose(self, *, run_id, deadline_at, monotonic):
        del deadline_at, monotonic
        return ShadowCompositionResult(
            run_id=run_id,
            evidence=_evidence(
                run_id,
                self._detector,
                two_cells=self._two_cells,
            ),
            issues=(),
        )


def _receipt_path(authority: Mapping[str, Any]) -> Path:
    label = authority["receipt_store_label"]
    return Path(next(
        item["path"] for item in authority["authorized_paths"] if item["label"] == label
    ))


def _core_orchestrator(
    authority: Mapping[str, Any],
) -> DurableScienceShadowOrchestrator:
    detector = _detector()
    return DurableScienceShadowOrchestrator(
        engine_id=authority["engine_id"],
        composer=_SyntheticComposer(
            detector,
            two_cells=bool(authority["factory_config"].get("two_cells", False)),
        ),
        tracking_policy=_policy(detector),
        receipt_path=_receipt_path(authority),
        limits=ScienceShadowLimits(**dict(authority["science_limits"])),
    )


def runtime_profile_for_authority(authority: Mapping[str, Any]) -> str:
    """Initialize the test store and return the deterministic runtime profile."""

    return _core_orchestrator(authority).runtime_profile_sha256


def active_track_ids_for_authority(authority: Mapping[str, Any]) -> tuple[str, ...]:
    """Test-only durable recovery probe; no provider or network is consulted."""

    orchestrator = _core_orchestrator(authority)
    orchestrator._recover(time.monotonic() + 10.0)
    assert orchestrator._state is not None
    return tuple(
        item.track_id
        for item in orchestrator._state.ledger.tracks
        if item.lifecycle_state != "ended"
    )


def detection_ids_for_run(
    authority: Mapping[str, Any],
    run_id: str,
) -> tuple[str, ...]:
    """Return deterministic rain-shield IDs used by the test composer."""

    evidence = _evidence(
        run_id,
        _detector(),
        two_cells=bool(authority["factory_config"].get("two_cells", False)),
    )
    return tuple(
        sorted(item.detection.detection_id for item in evidence.detection_frame.rain_shields)
    )


def _hang_tracking(*_args, **_kwargs):
    while True:
        time.sleep(60.0)


def _burn_tracking_cpu(*_args, **_kwargs):
    value = 1
    while True:
        value = (value * 1_103_515_245 + 12_345) & 0x7FFFFFFF


def _crash_tracking(*_args, **_kwargs):
    os._exit(93)


def _exhaust_memory(chunk_bytes: int) -> None:
    chunks: list[bytearray] = []
    while True:
        chunk = bytearray(chunk_bytes)
        for offset in range(0, len(chunk), 4096):
            chunk[offset] = 1
        chunks.append(chunk)


def build_test_orchestrator(
    authority: Mapping[str, Any],
) -> DurableScienceShadowOrchestrator:
    """Audited factory target selected by the checksummed test authority."""

    mode = authority["factory_config"].get("mode", "normal")
    if mode == "rss":
        _exhaust_memory(int(authority["factory_config"].get("chunk_bytes", 8 * 1024**2)))
    if mode == "factory_crash":
        os._exit(92)

    orchestrator = _core_orchestrator(authority)
    if mode in {"tracking_hang", "tracking_cpu", "tracking_crash"}:
        from storm_tracker_worker import science_shadow_orchestrator as runtime

        runtime.advance_cell_track_engine = {
            "tracking_hang": _hang_tracking,
            "tracking_cpu": _burn_tracking_cpu,
            "tracking_crash": _crash_tracking,
        }[mode]
    elif mode == "after_commit_hang":
        store = orchestrator.receipt_store
        original_record = store.record

        def commit_then_hang(*args, **kwargs):
            original_record(*args, **kwargs)
            while True:
                time.sleep(60.0)

        store.record = commit_then_hang
    elif mode != "normal":
        raise ValueError("unknown synthetic process-fixture mode")
    return orchestrator


__all__ = [
    "active_track_ids_for_authority",
    "build_test_orchestrator",
    "detection_ids_for_run",
    "runtime_profile_for_authority",
]
