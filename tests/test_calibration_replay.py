"""Tests for the isolated read-only calibration replay layer."""
from __future__ import annotations

from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import zlib

import pytest

from storm_tracker_worker.calibration_replay import (
    CalibrationReplayError,
    HierarchicalReplay,
    OVERLAY_ENCODING,
    assess_original_radar_frame,
    build_detection_replay,
    build_hierarchical_replay_from_raster_view,
    build_hierarchical_replay_from_normalized_cache,
    build_normalized_cache_inventory,
    build_normalized_raster_replay_view,
    build_replay_inventory,
    compare_legacy_and_shadow,
    evaluate_assignments,
    load_normalized_frame_cache,
    load_original_radar_corpus,
    overlay_to_cell_detections,
    run_bounded_shadow_assignment,
)
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.cell_track import CellDetection
from storm_tracker_worker.tracking_core.engine.global_assignment import (
    AssignmentEdgeCost,
)
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalCellDetection,
    HierarchicalDetectionFrame,
    HierarchicalDetectorConfig,
)
from storm_tracker_worker.tracking_core.engine.motion_prediction import (
    OpticalFlowSample,
)
from storm_tracker_worker.tracking_core.engine.pyramidal_sparse_flow import (
    SparsePyramidalConfig,
    estimate_sparse_pyramidal_motion,
)


BASE_TIMESTAMP = 1_800_000_000


def _ring(
    latitude: float,
    longitude: float,
    *,
    size: float = 0.02,
) -> list[list[float]]:
    return [
        [latitude + size, longitude],
        [latitude + size, longitude + size],
        [latitude, longitude + size],
        [latitude, longitude],
    ]


def _overlay(
    source: str,
    timestamp: int,
    *,
    shift: float = 0.0,
    wet: bool = True,
) -> dict:
    runs = []
    if wet:
        runs = [
            {
                "intensity": 4,
                "ring": _ring(50.00, 4.00 + shift),
            },
            {
                "intensity": 6,
                "ring": _ring(50.80, 4.80 + shift),
            },
        ]
    return {
        "source": source,
        "timestamp": float(timestamp),
        "native_shape": [490, 640],
        "runs": runs,
    }


def _create_database(path) -> None:
    with closing(sqlite3.connect(path)) as database, database:
        database.executescript("""
            CREATE TABLE sentinel(value TEXT NOT NULL);
            INSERT INTO sentinel(value) VALUES('do-not-touch');
            CREATE TABLE original_radar_frames (
                region_id TEXT NOT NULL,
                source TEXT NOT NULL,
                nominal_timestamp INTEGER NOT NULL,
                saved_at REAL NOT NULL,
                encoding TEXT NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY(region_id, source, nominal_timestamp)
            ) WITHOUT ROWID;
            CREATE TABLE pysteps_frame_cache (
                region_id TEXT NOT NULL,
                source TEXT NOT NULL,
                nominal_timestamp INTEGER NOT NULL,
                saved_at REAL NOT NULL,
                frame_json TEXT NOT NULL,
                PRIMARY KEY(region_id, source, nominal_timestamp)
            ) WITHOUT ROWID;
        """)


def _insert_frame(
    path,
    *,
    region_id: str,
    source: str,
    timestamp: int,
    overlay: dict | None = None,
    encoding: str = OVERLAY_ENCODING,
    payload: bytes | None = None,
) -> None:
    if payload is None:
        encoded = json.dumps(
            overlay if overlay is not None else _overlay(source, timestamp),
            separators=(",", ":"),
        ).encode()
        payload = zlib.compress(encoded)
    with closing(sqlite3.connect(path)) as database, database:
        database.execute(
            """
            INSERT INTO original_radar_frames(
                region_id, source, nominal_timestamp, saved_at,
                encoding, payload
            ) VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                region_id,
                source,
                timestamp,
                float(timestamp + 1),
                encoding,
                sqlite3.Binary(payload),
            ),
        )


def _populate_sequence(
    path,
    *,
    count: int,
    region_id: str = "region-new-york",
    source: str = "rainviewer",
    start: int = BASE_TIMESTAMP,
    cadence: int = 300,
) -> None:
    for index in range(count):
        timestamp = start + index * cadence
        _insert_frame(
            path,
            region_id=region_id,
            source=source,
            timestamp=timestamp,
            overlay=_overlay(
                source,
                timestamp,
                shift=index * 0.002,
                wet=index != 7,
            ),
        )


def _insert_normalized_frame(
    path,
    *,
    region_id: str,
    source: str,
    timestamp: int,
    height: int,
    width: int,
    cells: list[list[float]],
    bounds: list[float] | None = None,
    grid_deg: float = 0.01,
) -> None:
    frame = {
        "source": source,
        "nominal_timestamp": timestamp,
        "bounds": bounds or [4.0, 50.0, 4.0 + width * grid_deg,
                             50.0 + height * grid_deg],
        "grid_deg": grid_deg,
        "height": height,
        "width": width,
        "cells": cells,
        "input_runs": len(cells),
    }
    with closing(sqlite3.connect(path)) as database, database:
        database.execute(
            """
            INSERT INTO pysteps_frame_cache(
                region_id, source, nominal_timestamp, saved_at, frame_json
            ) VALUES(?, ?, ?, ?, ?)
            """,
            (
                region_id,
                source,
                timestamp,
                float(timestamp + 1),
                json.dumps(frame, separators=(",", ":")),
            ),
        )


def _manual_hierarchical_replay(
    frame_specs: list[tuple[float, list[tuple[str, float, float]]]],
) -> HierarchicalReplay:
    grid = AnalysisGrid(
        grid_id="manual-offline-grid-v1",
        crs="LOCAL_METRIC_TEST",
        origin_x_m=0.0,
        origin_y_m=100_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=100,
        height=100,
    )
    frames = []
    for frame_index, (valid_at, specs) in enumerate(frame_specs):
        detections = []
        for detection_index, (identifier, x_m, y_m) in enumerate(specs):
            detection = CellDetection(
                detection_id=identifier,
                valid_at=valid_at,
                analysis_grid_id=grid.grid_id,
                x_m=x_m,
                y_m=y_m,
                position_sigma_m=100.0,
                source="synthetic-offline",
            )
            detections.append(HierarchicalCellDetection(
                detection=detection,
                object_kind="rain_shield",
                parent_detection_id=None,
                footprint_pixels=((frame_index, detection_index),),
                area_m2=1_000_000.0,
                geometric_centroid_x_m=x_m,
                geometric_centroid_y_m=y_m,
                value_weighted_centroid_x_m=x_m,
                value_weighted_centroid_y_m=y_m,
                mean_value=2.0,
                maximum_value=2.0,
                threshold=1.0,
                centroid_method="geometric",
            ))
        frames.append(HierarchicalDetectionFrame(
            frame_id=f"manual-frame-{frame_index}",
            valid_at=valid_at,
            analysis_grid_id=grid.grid_id,
            quantity="synthetic_intensity",
            unit="class",
            detections=tuple(detections),
        ))
    return HierarchicalReplay(
        sequence_id="manual-stateful-replay",
        source="synthetic-offline",
        analysis_grid=grid,
        frames=tuple(frames),
        source_wet_cell_count=sum(len(specs) for _time, specs in frame_specs),
        adapter_limitations=("synthetic_test_evidence",),
    )


def _broad_zero_flow(old, current_frame) -> OpticalFlowSample:
    return OpticalFlowSample(
        sample_id=f"cold:{old.detection.detection_id}",
        analysis_grid_id=old.detection.analysis_grid_id,
        reference_valid_at=old.detection.valid_at,
        vx_m_s=0.0,
        vy_m_s=0.0,
        velocity_covariance_m2_s2=(
            (10_000.0, 0.0),
            (0.0, 10_000.0),
        ),
        residual_covariance_m2=(
            (1_000_000.0, 0.0),
            (0.0, 1_000_000.0),
        ),
        method="explicit_broad_cold_start_zero",
        support_frame_ids=(old.detection.detection_id,),
    )


def _bounded_sparse_config(maximum_input_pixels=1_000):
    return SparsePyramidalConfig(
        maximum_input_pixels=maximum_input_pixels,
        max_features=8,
        minimum_feature_distance_pixels=2.0,
        feature_window_radius_pixels=1,
        minimum_feature_valid_fraction=0.8,
        minimum_feature_eigenvalue=0.01,
        pyramid_levels=2,
        minimum_pyramid_valid_fraction=0.5,
        tracking_window_radius_pixels=1,
        minimum_tracking_samples=5,
        maximum_iterations=3,
        convergence_threshold_pixels=0.01,
        minimum_tracking_eigenvalue=0.00001,
        maximum_condition_number=100_000_000.0,
        regularization=1e-8,
        residual_variance_floor=1e-5,
        velocity_process_noise_covariance_m2_s2=(
            (1.0, 0.0),
            (0.0, 1.0),
        ),
    )


def _zero_assignment_edge(feature) -> AssignmentEdgeCost:
    return AssignmentEdgeCost(
        previous_detection_id=feature.previous_detection_id,
        current_detection_id=feature.current_detection_id,
        cost=0.0,
        cost_model_id="synthetic_force_complete_match-v1",
        components=(("synthetic_constant", 0.0),),
    )


def _last_two_assignment(replay: HierarchicalReplay):
    process_noise = (
        (2_500.0, 0.0, 0.0, 0.0),
        (0.0, 2_500.0, 0.0, 0.0),
        (0.0, 0.0, 1.0, 0.0),
        (0.0, 0.0, 0.0, 1.0),
    )
    return run_bounded_shadow_assignment(
        replay,
        object_kind="rain_shield",
        flow_sample_factory=_broad_zero_flow,
        edge_cost_factory=_zero_assignment_edge,
        death_cost=100.0,
        birth_cost=100.0,
        policy_id="synthetic-last-two-policy-v1",
        solution_limit=3,
        maximum_top_k_current_detections=4,
        maximum_complete_edges_per_pair=100,
        ambiguity_cost_delta=0.01,
        jump_speed_threshold_kmh=200.0,
        motion_prior_mode="last-two",
        last_two_process_noise_covariance=process_noise,
    )


def test_read_only_loader_decodes_exact_overlays_without_mutating_database(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    overlay = _overlay("kmi", BASE_TIMESTAMP)
    _insert_frame(
        path,
        region_id="region-belgium",
        source="kmi",
        timestamp=BASE_TIMESTAMP,
        overlay=overlay,
    )
    before_digest = hashlib.sha256(path.read_bytes()).hexdigest()

    corpus = load_original_radar_corpus(
        path,
        region_id="region-belgium",
        source="kmi",
    )

    after_digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert corpus.connection_mode == "sqlite-uri-mode-ro-query-only"
    assert corpus.operational_state_mutated is False
    assert corpus.decode_issues == ()
    assert len(corpus.frames) == 1
    assert corpus.frames[0].overlay == overlay
    assert corpus.frames[0].decoded_bytes == len(
        json.dumps(overlay, separators=(",", ":")).encode()
    )
    assert before_digest == after_digest
    with closing(sqlite3.connect(path)) as database, database:
        assert database.execute(
            "SELECT value FROM sentinel"
        ).fetchone()[0] == "do-not-touch"


def test_loader_reports_unsupported_and_corrupt_rows_instead_of_hiding_them(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    _insert_frame(
        path,
        region_id="region-1",
        source="kmi",
        timestamp=BASE_TIMESTAMP,
    )
    _insert_frame(
        path,
        region_id="region-1",
        source="kmi",
        timestamp=BASE_TIMESTAMP + 300,
        encoding="future-codec-v9",
    )
    _insert_frame(
        path,
        region_id="region-1",
        source="kmi",
        timestamp=BASE_TIMESTAMP + 600,
        payload=b"not-zlib",
    )

    corpus = load_original_radar_corpus(path)

    assert len(corpus.frames) == 1
    assert len(corpus.decode_issues) == 2
    assert corpus.decode_issues[0].reason == (
        "unsupported_encoding:future-codec-v9"
    )
    assert corpus.decode_issues[1].reason.startswith("decode_failed:")


def test_loader_fails_closed_when_original_frame_table_contract_is_missing(
    tmp_path,
):
    path = tmp_path / "wrong.sqlite3"
    with closing(sqlite3.connect(path)) as database, database:
        database.execute("CREATE TABLE something_else(value TEXT)")

    with pytest.raises(
        CalibrationReplayError,
        match="original_radar_frames ontbreekt",
    ):
        load_original_radar_corpus(path)

    with closing(sqlite3.connect(path)) as database, database:
        assert database.execute(
            "SELECT name FROM sqlite_master WHERE name='something_else'"
        ).fetchone() is not None


def test_normalized_cache_loader_is_read_only_and_never_labels_frames_raw(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    _insert_normalized_frame(
        path,
        region_id="region-1",
        source="kmi",
        timestamp=BASE_TIMESTAMP,
        height=10,
        width=12,
        cells=[[2, 3, 4], [2, 4, 5]],
    )
    before = hashlib.sha256(path.read_bytes()).hexdigest()

    corpus = load_normalized_frame_cache(path)

    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert corpus.connection_mode == "sqlite-uri-mode-ro-query-only"
    assert corpus.data_kind == "storm_tracker_normalized_grid_cache_not_raw"
    assert corpus.operational_state_mutated is False
    assert corpus.issues == ()
    assert corpus.frames[0].wet_cell_count == 2
    assert corpus.frames[0].data_kind.endswith("_not_raw")


def test_normalized_cache_inventory_splits_gaps_and_geometry_changes(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(20):
        _insert_normalized_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=BASE_TIMESTAMP + index * 300,
            height=10,
            width=12,
            cells=[[2, 3, 4]],
        )
    second_start = BASE_TIMESTAMP + 20 * 300 + 900
    for index in range(21):
        _insert_normalized_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=second_start + index * 300,
            height=10,
            width=13,
            cells=[[2, 3, 4]],
        )

    inventory = build_normalized_cache_inventory(
        load_normalized_frame_cache(path)
    )[0]

    assert inventory.cadence_seconds == 300
    assert inventory.issue_count == 0
    assert len(inventory.gaps) == 1
    assert [item.frame_count for item in inventory.sequences] == [20, 21]
    assert len(inventory.qualifying_sequences) == 2
    assert inventory.data_kind.endswith("_not_raw")
    for sequence in inventory.qualifying_sequences:
        view = build_normalized_raster_replay_view(
            sequence,
            maximum_source_grid_cells=200,
            maximum_analysis_pixels=200,
        )
        flow = estimate_sparse_pyramidal_motion(
            view.frames,
            _bounded_sparse_config(maximum_input_pixels=200),
        )
        assert all(item.elapsed_seconds == 300.0 for item in flow.pairs)
        assert all(
            item.current_valid_at - item.previous_valid_at == 300.0
            for item in flow.pairs
        )


def test_normalized_cache_to_raster_is_lossless_and_nodata_honest(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(2):
        _insert_normalized_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=BASE_TIMESTAMP + index * 300,
            height=4,
            width=6,
            cells=[[1, 2 + index, 4], [2, 4, 7]],
        )
    sequence = build_normalized_cache_inventory(
        load_normalized_frame_cache(path),
        minimum_sequence_frames=2,
    )[0].qualifying_sequences[0]

    view = build_normalized_raster_replay_view(
        sequence,
        maximum_source_grid_cells=24,
        maximum_analysis_pixels=24,
    )

    assert view.transform.mode == "native_lossless_cache_expansion"
    assert view.transform.lossless_cache_value_expansion is True
    assert view.transform.source_nodata_mask_available is False
    assert view.transform.provider_nodata_valid_fraction is None
    assert view.transform.cache_value_finite_fraction == 1.0
    assert view.transform.output_none_count == 0
    assert view.transform.extent_reference == (
        "cache_grid_nominal_cell_extent_not_requested_wgs84_bounds"
    )
    assert view.transform.cache_grid_east_padding_m >= 0.0
    assert view.transform.cache_grid_south_padding_m >= 0.0
    assert "not_claimed_as_provider_observed_dry" in (
        view.transform.sparse_absence_semantics
    )
    assert [item.valid_at for item in view.frames] == [
        float(BASE_TIMESTAMP),
        float(BASE_TIMESTAMP + 300),
    ]
    assert view.frames[0].values[1][2] == 4.0
    assert view.frames[0].values[2][4] == 7.0
    assert view.frames[0].values[0][0] == 0.0
    assert view.frames[0].grid.width == 6
    assert view.frames[0].grid.height == 4
    assert view.transform.row_bin_source_index_bounds == (
        (0, 1),
        (1, 2),
        (2, 3),
        (3, 4),
    )
    assert view.transform.column_bin_source_index_bounds == (
        (0, 1),
        (1, 2),
        (2, 3),
        (3, 4),
        (4, 5),
        (5, 6),
    )


def test_bounded_raster_view_is_explicit_and_sparse_flow_has_no_future_leakage(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(4):
        _insert_normalized_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=BASE_TIMESTAMP + index * 300,
            height=20,
            width=30,
            cells=[
                [5, 5 + index, 4],
                [5, 6 + index, 6],
                [6, 5 + index, 3],
                [6, 6 + index, 5],
            ],
        )
    sequence = build_normalized_cache_inventory(
        load_normalized_frame_cache(path),
        minimum_sequence_frames=2,
    )[0].qualifying_sequences[0]
    view = build_normalized_raster_replay_view(
        sequence,
        maximum_source_grid_cells=600,
        maximum_analysis_pixels=143,
    )

    assert view.transform.mode == "bounded_downscaled_analysis_view"
    assert view.transform.lossless_cache_value_expansion is False
    assert view.transform.output_pixel_count <= 143
    assert view.transform.extent_preserved is True
    assert view.transform.timestamps_preserved is True
    assert view.transform.row_scale != view.transform.column_scale
    assert (
        view.transform.output_x_resolution_m
        != view.transform.output_y_resolution_m
    )
    assert "max_pool_preserves_local_extrema_not_area_mean_or_mass" in (
        view.transform.analysis_biases
    )
    assert (
        view.transform.row_bin_source_index_bounds[0][0] == 0
        and view.transform.row_bin_source_index_bounds[-1][1] == 20
    )
    assert (
        view.transform.column_bin_source_index_bounds[0][0] == 0
        and view.transform.column_bin_source_index_bounds[-1][1] == 30
    )
    assert view.frames[0].grid.extent_edges_m == (
        view.frames[-1].grid.extent_edges_m
    )
    config = _bounded_sparse_config(maximum_input_pixels=143)
    prefix = estimate_sparse_pyramidal_motion(view.frames[:2], config)
    complete = estimate_sparse_pyramidal_motion(view.frames, config)
    assert complete.pairs[0] == prefix.pairs[0]
    assert complete.pairs[0].current_valid_at == float(
        BASE_TIMESTAMP + 300
    )
    assert complete.pairs[0].current_frame_id == view.frames[1].frame_id
    assert view.frames[2].frame_id not in (
        complete.pairs[0].previous_frame_id,
        complete.pairs[0].current_frame_id,
    )
    replay = build_hierarchical_replay_from_raster_view(
        view,
        detector_config=HierarchicalDetectorConfig(
            shield_threshold=1.0,
            core_threshold=5.0,
            minimum_shield_area_m2=0.0,
            minimum_core_area_m2=0.0,
            shield_position_sigma_m=1_000.0,
            core_position_sigma_m=500.0,
            connectivity=8,
            centroid_method="value_weighted",
        ),
    )
    assert replay.frames[0].frame_id == view.frames[0].frame_id
    assert (
        replay.frames[0].analysis_grid_id
        == complete.pairs[0].analysis_grid_id
    )


def test_normalized_cache_hierarchical_assignment_is_bounded_and_auditable(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(24):
        timestamp = BASE_TIMESTAMP + index * 300
        cells = []
        for base_column in (2 + index, 32 + index):
            cells.extend([
                [5, base_column, 6],
                [5, base_column + 1, 2],
                [6, base_column, 2],
                [6, base_column + 1, 2],
            ])
        _insert_normalized_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=timestamp,
            height=20,
            width=60,
            cells=cells,
        )
    sequence = build_normalized_cache_inventory(
        load_normalized_frame_cache(path)
    )[0].qualifying_sequences[0]
    config = HierarchicalDetectorConfig(
        shield_threshold=1.0,
        core_threshold=5.0,
        minimum_shield_area_m2=1.0,
        minimum_core_area_m2=1.0,
        shield_position_sigma_m=1_000.0,
        core_position_sigma_m=500.0,
        connectivity=4,
        centroid_method="value_weighted",
    )
    replay = build_hierarchical_replay_from_normalized_cache(
        sequence,
        detector_config=config,
        maximum_grid_cells=2_000,
    )

    def flow(old, current_frame):
        elapsed = current_frame.valid_at - old.detection.valid_at
        return OpticalFlowSample(
            sample_id=f"flow:{old.detection.detection_id}",
            analysis_grid_id=old.detection.analysis_grid_id,
            reference_valid_at=old.detection.valid_at,
            vx_m_s=replay.analysis_grid.x_resolution_m / elapsed,
            vy_m_s=0.0,
            velocity_covariance_m2_s2=((1.0, 0.0), (0.0, 1.0)),
            residual_covariance_m2=(
                (10_000.0, 0.0),
                (0.0, 10_000.0),
            ),
            method="synthetic_exact_grid_translation",
            support_frame_ids=(old.detection.detection_id,),
        )

    def edge(feature):
        components = (
            ("mahalanobis_squared", feature.mahalanobis_squared),
            ("absolute_log_area_ratio", feature.absolute_log_area_ratio),
        )
        return AssignmentEdgeCost(
            previous_detection_id=feature.previous_detection_id,
            current_detection_id=feature.current_detection_id,
            cost=sum(value for _name, value in components),
            cost_model_id="test-explicit-motion-area-v1",
            components=components,
        )

    audit = run_bounded_shadow_assignment(
        replay,
        object_kind="rain_shield",
        flow_sample_factory=flow,
        edge_cost_factory=edge,
        death_cost=10.0,
        birth_cost=10.0,
        policy_id="test-caller-policy-v1",
        solution_limit=3,
        maximum_top_k_current_detections=4,
        maximum_complete_edges_per_pair=16,
        ambiguity_cost_delta=0.01,
        jump_speed_threshold_kmh=200.0,
    )

    assert replay.operational_state_mutated is False
    assert replay.data_kind.endswith("_not_raw")
    assert replay.rain_shield_count == 24 * 2
    assert replay.embedded_core_count == 24 * 2
    assert audit.operational_state_mutated is False
    assert audit.exact_one_best_everywhere is True
    assert audit.metrics.track_count == 2
    assert audit.birth_count == 2
    assert audit.death_count == 0
    assert audit.right_censored_track_count == 2
    assert audit.match_count == 23 * 2
    assert audit.metrics.unassigned_detection_count == 0
    assert audit.metrics.identity_switch_count is None
    assert audit.metrics.jump_count == 0
    assert audit.alternative_hypotheses_available is True
    assert audit.cost_model_ids == ("test-explicit-motion-area-v1",)
    assert audit.motion_prior_methods == (
        "synthetic_exact_grid_translation",
    )
    audit_payload = audit.to_dict()
    assert audit_payload["ground_truth_available"] is False
    assert audit_payload["identity_switches_measurable"] is False
    assert audit_payload["confirmed_identity_match_count"] == 0
    assert (
        audit_payload["indeterminate_identity_match_count"]
        == audit.match_count
    )
    assert audit_payload["all_candidates_retained"] is True
    assert audit_payload["identity_confirmation_claimed"] is False
    assert audit_payload["speed_cap_kmh"] is None
    assert audit_payload["candidate_rejection_by_speed"] is False
    assert "not_confirmed_meteorological_identity" in (
        audit_payload["assignment_semantics"]
    )
    assert "OpticalFlowSample" in (
        audit_payload["motion_prior_extension_point"]
    )
    assert all(
        frame.complete_feature_count in (0, 4)
        for frame in audit.frame_audits
    )


def test_last_two_replay_uses_exact_positions_and_irregular_dt():
    replay = _manual_hierarchical_replay([
        (1_000.0, [("a0", 0.0, 0.0)]),
        (1_060.0, [("a1", 600.0, 0.0)]),
        (1_240.0, [("a2", 2_400.0, 0.0)]),
        (1_300.0, [("a3", 3_000.0, 0.0)]),
    ])

    audit = _last_two_assignment(replay)

    assert [item.role for item in audit.motion_prior_audits] == [
        "cold_start",
        "last_two",
        "last_two",
    ]
    first_last_two = audit.motion_prior_audits[1]
    assert first_last_two.supporting_detection_ids == ("a0", "a1")
    assert first_last_two.source_valid_at == 1_060.0
    assert first_last_two.target_valid_at == 1_240.0
    assert first_last_two.elapsed_seconds == 180.0
    assert first_last_two.vx_m_s == 10.0
    assert first_last_two.vy_m_s == 0.0
    assert first_last_two.predicted_x_m == 2_400.0
    assert (
        first_last_two.predicted_position_covariance_m2[0][0]
        == pytest.approx(252_500.0)
    )
    second_last_two = audit.motion_prior_audits[2]
    assert second_last_two.supporting_detection_ids == ("a1", "a2")
    assert second_last_two.elapsed_seconds == 60.0
    assert second_last_two.vx_m_s == 10.0
    assert second_last_two.predicted_x_m == 3_000.0
    assert audit.metrics.track_count == 1
    assert audit.metrics.jump_count == 0


def test_last_two_birth_death_and_empty_gap_reset_history_without_leakage():
    replay = _manual_hierarchical_replay([
        (2_000.0, [("old0", 0.0, 0.0)]),
        (2_060.0, [("old1", 600.0, 0.0)]),
        (2_120.0, []),
        (2_300.0, [("new0", 50_000.0, 0.0)]),
        (2_360.0, [("new1", 50_600.0, 0.0)]),
        (2_540.0, [("new2", 52_400.0, 0.0)]),
    ])

    audit = _last_two_assignment(replay)

    assert audit.birth_count == 2
    assert audit.death_count == 1
    assert audit.right_censored_track_count == 1
    assert [item.role for item in audit.motion_prior_audits] == [
        "cold_start",
        "cold_start",
        "last_two",
    ]
    new_cold_start = audit.motion_prior_audits[1]
    assert new_cold_start.previous_detection_id == "new0"
    assert new_cold_start.supporting_detection_ids == ("new0",)
    assert new_cold_start.track_id != audit.motion_prior_audits[0].track_id
    new_last_two = audit.motion_prior_audits[2]
    assert new_last_two.supporting_detection_ids == ("new0", "new1")
    assert all(
        not identifier.startswith("old")
        for identifier in new_last_two.supporting_detection_ids
    )
    assert new_last_two.elapsed_seconds == 180.0


def test_last_two_replay_is_reproducible_and_reports_feedback_dependency():
    replay = _manual_hierarchical_replay([
        (3_000.0, [("r0", 0.0, 0.0)]),
        (3_075.0, [("r1", 750.0, 0.0)]),
        (3_300.0, [("r2", 3_000.0, 0.0)]),
    ])

    first = _last_two_assignment(replay)
    second = _last_two_assignment(replay)

    assert first == second
    assert first.to_dict() == second.to_dict()
    payload = first.to_dict()
    assert payload["motion_prior_mode"] == "last-two"
    assert payload["motion_prior_role_counts"] == {
        "cold_start": 1,
        "last_two": 1,
    }
    assert payload["stateful_feedback_dependency"] is True
    assert "wrong earlier global match" in (
        payload["feedback_poisoning_observation"]
    )
    assert len(payload["detection_evidence_sha256"]) == 64


def test_last_two_rejects_falsely_certain_cold_start():
    replay = _manual_hierarchical_replay([
        (4_000.0, [("c0", 0.0, 0.0)]),
        (4_060.0, [("c1", 600.0, 0.0)]),
    ])

    def false_certainty(old, current_frame):
        return OpticalFlowSample(
            sample_id="false-certainty",
            analysis_grid_id=old.detection.analysis_grid_id,
            reference_valid_at=old.detection.valid_at,
            vx_m_s=0.0,
            vy_m_s=0.0,
            velocity_covariance_m2_s2=((0.0, 0.0), (0.0, 0.0)),
            residual_covariance_m2=((0.0, 0.0), (0.0, 0.0)),
            method="implicit_zero",
            support_frame_ids=(old.detection.detection_id,),
        )

    with pytest.raises(ValueError, match="caller-onzekerheid"):
        run_bounded_shadow_assignment(
            replay,
            object_kind="rain_shield",
            flow_sample_factory=false_certainty,
            edge_cost_factory=_zero_assignment_edge,
            death_cost=100.0,
            birth_cost=100.0,
            policy_id="reject-false-certainty-v1",
            solution_limit=1,
            maximum_top_k_current_detections=4,
            maximum_complete_edges_per_pair=10,
            ambiguity_cost_delta=0.0,
            jump_speed_threshold_kmh=200.0,
            motion_prior_mode="last-two",
            last_two_process_noise_covariance=(
                (0.0, 0.0, 0.0, 0.0),
                (0.0, 0.0, 0.0, 0.0),
                (0.0, 0.0, 0.0, 0.0),
                (0.0, 0.0, 0.0, 0.0),
            ),
        )


def test_replay_cli_defaults_to_stdout_and_does_not_create_output_file(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(20):
        _insert_normalized_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=BASE_TIMESTAMP + index * 300,
            height=10,
            width=12,
            cells=[[2, 3, 4]],
        )
    before_files = sorted(item.name for item in tmp_path.iterdir())
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "analyze_calibration_replay.py"
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            str(path),
            "--region",
            "region-1",
            "--source",
            "kmi",
            "--shadow",
            "--minimum-shield-area-km2",
            "0",
            "--minimum-core-area-km2",
            "0",
            "--compact",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    result = json.loads(completed.stdout)
    assert result["mode"] == "read_only_calibration_replay"
    assert result["database_unchanged"] is True
    assert result["operational_state_mutated"] is False
    assert result["data_kind"].endswith("_not_raw")
    assert result["shadow_assignment"]["assignment_count"] == 20
    assert result["shadow_assignment"]["track_count"] == 1
    assert result["shadow_assignment"]["right_censored_track_count"] == 1
    assert result["shadow_assignment"]["motion_prior_methods"] == [
        "cli_caller_supplied_constant_prior"
    ]
    assert result["shadow_assignment"]["ground_truth_available"] is False
    assert (
        result["shadow_assignment"]["identity_switches_measurable"]
        is False
    )
    assert sorted(item.name for item in tmp_path.iterdir()) == before_files


def test_replay_cli_last_two_compares_both_modes_on_identical_detections(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(20):
        _insert_normalized_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=BASE_TIMESTAMP + index * 300,
            height=10,
            width=40,
            cells=[[2, 3 + index, 4]],
        )
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "analyze_calibration_replay.py"
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            str(path),
            "--region",
            "region-1",
            "--source",
            "kmi",
            "--shadow",
            "--prior-mode",
            "last-two",
            "--minimum-shield-area-km2",
            "0",
            "--minimum-core-area-km2",
            "0",
            "--compact",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    result = json.loads(completed.stdout)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert result["database_unchanged"] is True
    assert result["assignment_policy"]["selected_prior_mode"] == "last-two"
    assert set(result["shadow_assignments"]) == {"constant", "last-two"}
    constant = result["shadow_assignments"]["constant"]
    last_two = result["shadow_assignments"]["last-two"]
    assert (
        constant["detection_evidence_sha256"]
        == last_two["detection_evidence_sha256"]
    )
    assert last_two["motion_prior_role_counts"] == {
        "cold_start": 1,
        "last_two": 18,
    }
    assert result["shadow_assignment"] == last_two
    comparison = result["shadow_assignment_comparison"]
    assert comparison["same_detection_evidence"] is True
    assert comparison["ground_truth_available"] is False
    assert comparison["comparison_is_meteorological_validation"] is False
    assert comparison["first_assignment_state_divergence"] is None


def test_replay_cli_sparse_compares_three_modes_deterministically_and_audits(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(20):
        cells = [
            [4, 4 + index, 4],
            [4, 5 + index, 6],
            [5, 4 + index, 3],
            [5, 5 + index, 5],
        ]
        _insert_normalized_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=BASE_TIMESTAMP + index * 300,
            height=14,
            width=40,
            cells=cells,
        )
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "analyze_calibration_replay.py"
    )
    command = [
        sys.executable,
        str(script),
        str(path),
        "--region",
        "region-1",
        "--source",
        "kmi",
        "--shadow",
        "--prior-mode",
        "sparse-pyramidal",
        "--minimum-shield-area-km2",
        "0",
        "--minimum-core-area-km2",
        "0",
        "--sparse-analysis-maximum-pixels",
        "560",
        "--sparse-maximum-input-pixels",
        "560",
        "--sparse-max-features",
        "8",
        "--sparse-pyramid-levels",
        "2",
        "--sparse-tracking-window-radius-pixels",
        "1",
        "--sparse-minimum-tracking-samples",
        "5",
        "--sparse-maximum-iterations",
        "3",
        "--compact",
    ]

    first = json.loads(subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    ).stdout)
    second = json.loads(subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    ).stdout)

    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert first["database_unchanged"] is True
    assert set(first["shadow_assignments"]) == {
        "constant",
        "last-two",
        "sparse-pyramidal",
    }
    hashes = {
        item["detection_evidence_sha256"]
        for item in first["shadow_assignments"].values()
    }
    assert len(hashes) == 1
    comparison = first["shadow_assignment_comparison"]
    assert comparison["same_detection_evidence"] is True
    assert comparison["correct_algorithm_claimed"] is None
    assert comparison["ground_truth_available"] is False
    flow = first["sparse_pyramidal_flow_audit"]
    assert flow["pair_count"] == 19
    assert (
        flow["vector_usable_count"] + flow["vector_failure_count"]
        == flow["vector_count"]
    )
    assert flow["aggregation_call_count"] == 19
    assert flow["aggregation_fallback_count"] > 0
    assert flow["motion_evidence_status_counts"]["unavailable"] > 0
    assert flow["identity_claim_permitted_count"] == 0
    fallback_samples = [
        item
        for item in flow["association_flow_samples"]
        if item["fallback_used"]
    ]
    assert fallback_samples
    assert all(
        item["sample_method"].startswith(
            "caller_explicit_broad_sparse_cold_start_or_unavailable"
        )
        for item in fallback_samples
    )
    assert all(
        item["motion_evidence_status"] == "unavailable"
        and item["identity_claim_permitted"] is False
        for item in fallback_samples
    )
    assert all(
        item["sample_velocity_covariance_m2_s2"][0][0] > 0.0
        and item["sample_velocity_covariance_m2_s2"][1][1] > 0.0
        for item in fallback_samples
    )
    assert flow["causality_contract"]["future_frames_beyond_target_used"] is False
    assert flow["speed_cap_kmh"] is None
    sparse_policy = first["assignment_policy"]["sparse_pyramidal"]
    assert sparse_policy["flow_config"]["maximum_input_pixels"] == 560
    assert sparse_policy["flow_config_fingerprint"] == (
        flow["config_fingerprint"]
    )
    assert sparse_policy["smoothing"] is False
    assert (
        sparse_policy["unavailable_fallback"]["motion_evidence_status"]
        == "unavailable"
    )
    assert (
        sparse_policy["unavailable_fallback"]["identity_claim_permitted"]
        is False
    )
    assert (
        sparse_policy["unavailable_fallback"]["motion_cost_contribution"]
        == 0.0
    )
    assert (
        first["assignment_policy"]["unavailable_motion_cost_policy"]
        == "zero_weight_keep_complete_feature_and_candidate"
    )
    sparse_assignment = first["shadow_assignments"]["sparse-pyramidal"]
    assert sparse_assignment["confirmed_identity_match_count"] == 0
    assert sparse_assignment["all_candidates_retained"] is True
    assert sparse_assignment["identity_confirmation_claimed"] is False
    assert len(
        first["assignment_policy"]["policy_ids_by_mode"]["sparse-pyramidal"]
    ) > 64
    assert first["normalized_raster_replay_view"]["transform"][
        "lossless_cache_value_expansion"
    ] is True
    first.pop("runtime_seconds")
    second.pop("runtime_seconds")
    assert first == second


def test_replay_cli_sparse_resource_cap_fails_closed_before_flow(tmp_path):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(20):
        _insert_normalized_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=BASE_TIMESTAMP + index * 300,
            height=10,
            width=20,
            cells=[[2, 3 + index % 10, 4]],
        )
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "analyze_calibration_replay.py"
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            str(path),
            "--region",
            "region-1",
            "--source",
            "kmi",
            "--shadow",
            "--prior-mode",
            "sparse-pyramidal",
            "--sparse-maximum-frame-pairs",
            "5",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "sparse-maximum-frame-pairs" in completed.stderr
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_shadow_assignment_fails_closed_above_complete_edge_capacity(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(2):
        _insert_normalized_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=BASE_TIMESTAMP + index * 300,
            height=10,
            width=20,
            cells=[[2, 2 + index, 4], [2, 12 + index, 4]],
        )
    sequence = build_normalized_cache_inventory(
        load_normalized_frame_cache(path),
        minimum_sequence_frames=2,
    )[0].qualifying_sequences[0]
    replay = build_hierarchical_replay_from_normalized_cache(
        sequence,
        detector_config=HierarchicalDetectorConfig(
            shield_threshold=1.0,
            core_threshold=5.0,
            minimum_shield_area_m2=1.0,
            minimum_core_area_m2=1.0,
            shield_position_sigma_m=1_000.0,
            core_position_sigma_m=500.0,
            connectivity=4,
            centroid_method="geometric",
        ),
        maximum_grid_cells=1_000,
    )

    with pytest.raises(ValueError, match="maximum_complete_edges"):
        run_bounded_shadow_assignment(
            replay,
            object_kind="rain_shield",
            flow_sample_factory=lambda old, current: OpticalFlowSample(
                sample_id=old.detection.detection_id,
                analysis_grid_id=old.detection.analysis_grid_id,
                reference_valid_at=old.detection.valid_at,
                vx_m_s=0.0,
                vy_m_s=0.0,
                velocity_covariance_m2_s2=((1.0, 0.0), (0.0, 1.0)),
                residual_covariance_m2=((1.0, 0.0), (0.0, 1.0)),
                method="test",
                support_frame_ids=(old.detection.detection_id,),
            ),
            edge_cost_factory=lambda feature: AssignmentEdgeCost(
                feature.previous_detection_id,
                feature.current_detection_id,
                feature.mahalanobis_squared,
                "test",
                (("motion", feature.mahalanobis_squared),),
            ),
            death_cost=1.0,
            birth_cost=1.0,
            policy_id="test",
            solution_limit=2,
            maximum_top_k_current_detections=4,
            maximum_complete_edges_per_pair=3,
            ambiguity_cost_delta=0.1,
            jump_speed_threshold_kmh=200.0,
        )


def test_inventory_finds_cadence_dry_frames_gaps_and_twenty_frame_sequences(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    _populate_sequence(path, count=20)
    second_start = BASE_TIMESTAMP + 20 * 300 + 900
    _populate_sequence(path, count=21, start=second_start)

    corpus = load_original_radar_corpus(path)
    inventories = build_replay_inventory(corpus)

    assert len(inventories) == 1
    inventory = inventories[0]
    assert inventory.cadence_seconds == 300
    assert inventory.total_decoded_frames == 41
    assert inventory.usable_frames == 41
    assert inventory.precipitation_usable_frames == 39
    assert len(inventory.gaps) == 1
    assert inventory.gaps[0].elapsed_seconds == 1_200
    assert inventory.gaps[0].estimated_missing_frames == 3
    assert [item.frame_count for item in inventory.sequences] == [20, 21]
    assert len(inventory.qualifying_sequences) == 2
    assert all(item.qualifies for item in inventory.sequences)


def test_frame_assessment_separates_structure_from_precipitation(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    dry = _overlay("kmi", BASE_TIMESTAMP, wet=False)
    _insert_frame(
        path,
        region_id="region-1",
        source="kmi",
        timestamp=BASE_TIMESTAMP,
        overlay=dry,
    )
    mismatch = _overlay("opera", BASE_TIMESTAMP + 300)
    mismatch["runs"].append({"intensity": "bad", "ring": []})
    _insert_frame(
        path,
        region_id="region-1",
        source="kmi",
        timestamp=BASE_TIMESTAMP + 300,
        overlay=mismatch,
    )
    frames = load_original_radar_corpus(path).frames

    dry_assessment = assess_original_radar_frame(frames[0])
    mismatch_assessment = assess_original_radar_frame(frames[1])

    assert dry_assessment.frame_usable is True
    assert dry_assessment.precipitation_usable is False
    assert dry_assessment.valid_wet_run_count == 0
    assert mismatch_assessment.frame_usable is False
    assert mismatch_assessment.precipitation_usable is False
    assert "overlay_source_mismatch" in mismatch_assessment.reasons
    assert "malformed_runs_present" in mismatch_assessment.reasons


def test_detection_replay_adapts_cells_and_retains_complete_candidates(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    # This sequence stays wet so every adjacent pair has a candidate graph.
    for index in range(24):
        timestamp = BASE_TIMESTAMP + index * 300
        _insert_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=timestamp,
            overlay=_overlay(
                "kmi",
                timestamp,
                shift=index * 0.002,
            ),
        )
    inventory = build_replay_inventory(
        load_original_radar_corpus(path)
    )[0]
    sequence = inventory.qualifying_sequences[0]

    replay = build_detection_replay(
        sequence,
        position_sigma_m=1_000.0,
        motion_sigma_m_s=20.0,
    )

    assert replay.operational_state_mutated is False
    assert len(replay.frames) == 24
    assert all(len(frame.detections) == 2 for frame in replay.frames)
    assert len(replay.association_graphs) == 23
    assert replay.frame_pairs_without_graph == 0
    assert replay.candidate_count == 23 * 4
    assert replay.every_built_graph_complete is True
    assert all(
        detection.analysis_grid_id
        == replay.analysis_grid.analysis_grid_id
        for frame in replay.frames
        for detection in frame.detections
    )


def test_overlay_adapter_requires_explicit_position_uncertainty(tmp_path):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    _populate_sequence(path, count=20)
    sequence = build_replay_inventory(
        load_original_radar_corpus(path)
    )[0].qualifying_sequences[0]
    replay = build_detection_replay(
        sequence,
        position_sigma_m=1_000.0,
        motion_sigma_m_s=20.0,
    )

    with pytest.raises(ValueError, match="position_sigma_m"):
        overlay_to_cell_detections(
            sequence.frames[0],
            replay.analysis_grid,
            position_sigma_m=0.0,
        )


def test_overlay_adapter_scales_to_thousands_of_adjacent_runs(tmp_path):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    runs = [
        {
            "intensity": 4,
            "ring": _ring(
                50.0,
                3.0 + index * 0.001,
                size=0.001,
            ),
        }
        for index in range(2_000)
    ]
    overlay = {
        "source": "rainviewer",
        "timestamp": float(BASE_TIMESTAMP),
        "runs": runs,
    }
    _insert_frame(
        path,
        region_id="region-1",
        source="rainviewer",
        timestamp=BASE_TIMESTAMP,
        overlay=overlay,
    )
    # A second frame is only needed to construct the fixed sequence grid.
    _insert_frame(
        path,
        region_id="region-1",
        source="rainviewer",
        timestamp=BASE_TIMESTAMP + 300,
        overlay={
            **overlay,
            "timestamp": float(BASE_TIMESTAMP + 300),
        },
    )
    sequence = build_replay_inventory(
        load_original_radar_corpus(path),
        minimum_sequence_frames=2,
    )[0].qualifying_sequences[0]
    replay = build_detection_replay(
        sequence,
        position_sigma_m=1_000.0,
        motion_sigma_m_s=20.0,
    )

    assert len(replay.frames[0].detections) == 1
    assert len(replay.frames[1].detections) == 1
    assert replay.candidate_count == 1


def test_legacy_shadow_report_measures_jumps_gaps_and_identity_switches(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(24):
        timestamp = BASE_TIMESTAMP + index * 300
        _insert_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=timestamp,
            overlay=_overlay(
                "kmi",
                timestamp,
                shift=index * 0.002,
            ),
        )
    sequence = build_replay_inventory(
        load_original_radar_corpus(path)
    )[0].qualifying_sequences[0]
    replay = build_detection_replay(
        sequence,
        position_sigma_m=1_000.0,
        motion_sigma_m_s=20.0,
    )
    shadow: dict[str, str] = {}
    legacy: dict[str, str] = {}
    truth: dict[str, str] = {}
    for frame_index, frame in enumerate(replay.frames):
        left, right = frame.detections
        shadow[left.detection_id] = "track-left"
        shadow[right.detection_id] = "track-right"
        truth[left.detection_id] = "physical-left"
        truth[right.detection_id] = "physical-right"
        if frame_index == 12:
            legacy[left.detection_id] = "track-right"
            legacy[right.detection_id] = "track-left"
        else:
            legacy[left.detection_id] = "track-left"
            legacy[right.detection_id] = "track-right"
    # Simulate one legacy detection that was present but not assigned.
    omitted_detection = replay.frames[6].detections[0].detection_id
    legacy.pop(omitted_detection)

    comparison = compare_legacy_and_shadow(
        replay,
        legacy_assignments=legacy,
        shadow_assignments=shadow,
        jump_speed_threshold_kmh=200.0,
        reference_identity_by_detection=truth,
    )

    assert comparison.operational_state_mutated is False
    assert comparison.identity_switches_measurable is True
    assert comparison.shadow.identity_switch_count == 0
    assert comparison.legacy.identity_switch_count == 4
    assert comparison.shadow.jump_count == 0
    assert comparison.legacy.jump_count == 4
    assert comparison.shadow.missed_detection_slots == 0
    assert comparison.legacy.missed_detection_slots == 1
    assert comparison.shadow.unassigned_detection_count == 0
    assert comparison.legacy.unassigned_detection_count == 1
    assert comparison.deltas["jump_count"] == -4
    assert comparison.deltas["identity_switch_count"] == -4


def test_identity_switch_metric_is_explicitly_unknown_without_reference_truth(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(20):
        timestamp = BASE_TIMESTAMP + index * 300
        _insert_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=timestamp,
            overlay=_overlay("kmi", timestamp, shift=index * 0.002),
        )
    sequence = build_replay_inventory(
        load_original_radar_corpus(path)
    )[0].qualifying_sequences[0]
    replay = build_detection_replay(
        sequence,
        position_sigma_m=1_000.0,
        motion_sigma_m_s=20.0,
    )
    assignments = {
        detection.detection_id: (
            "left" if index == 0 else "right"
        )
        for frame in replay.frames
        for index, detection in enumerate(frame.detections)
    }

    metrics = evaluate_assignments(
        replay,
        assignments,
        label="unlabelled-real-replay",
        jump_speed_threshold_kmh=200.0,
    )
    comparison = compare_legacy_and_shadow(
        replay,
        legacy_assignments=assignments,
        shadow_assignments=assignments,
        jump_speed_threshold_kmh=200.0,
    )

    assert metrics.identity_switch_count is None
    assert metrics.reference_identity_fragmentations is None
    assert comparison.identity_switches_measurable is False
    assert comparison.deltas["identity_switch_count"] is None


def test_assignments_fail_closed_for_unknown_detection(tmp_path):
    path = tmp_path / "calibration.sqlite3"
    _create_database(path)
    for index in range(20):
        timestamp = BASE_TIMESTAMP + index * 300
        _insert_frame(
            path,
            region_id="region-1",
            source="kmi",
            timestamp=timestamp,
            overlay=_overlay("kmi", timestamp, shift=index * 0.002),
        )
    sequence = build_replay_inventory(
        load_original_radar_corpus(path)
    )[0].qualifying_sequences[0]
    replay = build_detection_replay(
        sequence,
        position_sigma_m=1_000.0,
        motion_sigma_m_s=20.0,
    )

    with pytest.raises(ValueError, match="onbekende detection_id"):
        evaluate_assignments(
            replay,
            {"does-not-exist": "track-1"},
            label="invalid",
            jump_speed_threshold_kmh=200.0,
        )
