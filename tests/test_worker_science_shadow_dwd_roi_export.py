from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import pytest

import storm_tracker_worker.science_shadow_dwd_roi_export as roi_authority

from storm_tracker_worker.analysis_grid_transform import analysis_grid_sha256
from storm_tracker_worker.dwd_rv_analysis_grid_certificate import (
    SOURCE_NORTH_EDGE_M,
    SOURCE_WEST_EDGE_M,
)
from storm_tracker_worker.dwd_rv_observation_replay import (
    default_belgium_observation_profile,
)
from storm_tracker_worker.evidence_to_cells_shadow import (
    HARD_MAX_DETECTION_OBJECTS,
    HARD_MAX_RECEIPT_BYTES,
    HARD_MAX_TARGET_PIXELS,
    EvidenceToCellsShadowConfig,
)
from storm_tracker_worker.science_shadow_dwd_roi_export import (
    CALIBRATION_RSS_CAP_BYTES,
    DWD_GERMANY_ROI_SELECTION_CONTRACT,
    DwdGermanyRoiError,
    ROI_HEIGHT,
    ROI_ORIGIN_X_M,
    ROI_ORIGIN_Y_M,
    ROI_PIXELS,
    ROI_RESOLUTION_M,
    ROI_SOURCE_COLUMN_MAX,
    ROI_SOURCE_COLUMN_MIN,
    ROI_SOURCE_HEIGHT,
    ROI_SOURCE_ROW_MAX,
    ROI_SOURCE_ROW_MIN,
    ROI_SOURCE_WIDTH,
    ROI_WIDTH,
    authorize_dwd_germany_roi_full_replay,
    derive_dwd_germany_roi_replay_cap,
    dwd_germany_roi_profile,
    load_dwd_germany_roi_manifest,
    seal_dwd_germany_roi_selection_manifest,
    seal_dwd_germany_roi_calibration_manifest,
    verify_dwd_germany_roi_calibration_source,
)


EXPECTED_BELGIUM_PROFILE_SHA256 = (
    "92107ee272a8533148a38b34d27941638a51b9a10040a601f1060bc9854f3838"
)
EXPECTED_GERMANY_GRID_SHA256 = (
    "cf99c2b4c08a46bde9f74f55a0b690d8496ce7b44d4a0aa379f4132023cab636"
)
EXPECTED_GERMANY_PROFILE_SHA256 = (
    "83d54c7d0362ae230b944dcf4523f2346d35d8bbe40fd96ec4adcf7755c50a4d"
)


def test_two_kilometre_roi_is_an_exact_two_by_two_source_edge_pairing() -> None:
    profile = dwd_germany_roi_profile()
    grid = profile.target_grid

    assert ROI_SOURCE_WIDTH == 700
    assert ROI_SOURCE_HEIGHT == 850
    assert (ROI_WIDTH, ROI_HEIGHT, ROI_PIXELS) == (350, 425, 148_750)
    assert grid.origin_x_m == SOURCE_WEST_EDGE_M + ROI_SOURCE_COLUMN_MIN * 1_000
    assert grid.origin_y_m == SOURCE_NORTH_EDGE_M - ROI_SOURCE_ROW_MIN * 1_000
    assert (grid.origin_x_m, grid.origin_y_m) == (
        ROI_ORIGIN_X_M,
        ROI_ORIGIN_Y_M,
    )
    assert grid.x_resolution_m == grid.y_resolution_m == ROI_RESOLUTION_M
    assert grid.extent_edges_m == (
        SOURCE_WEST_EDGE_M + ROI_SOURCE_COLUMN_MIN * 1_000,
        SOURCE_NORTH_EDGE_M - (ROI_SOURCE_ROW_MAX + 1) * 1_000,
        SOURCE_WEST_EDGE_M + (ROI_SOURCE_COLUMN_MAX + 1) * 1_000,
        SOURCE_NORTH_EDGE_M - ROI_SOURCE_ROW_MIN * 1_000,
    )
    # Target (0,0) is the area-centre of source rows/columns 150 and 151.
    assert grid.pixel_center(0, 0) == (150_500.0, -150_500.0)
    assert analysis_grid_sha256(grid) == EXPECTED_GERMANY_GRID_SHA256
    assert profile.fingerprint == EXPECTED_GERMANY_PROFILE_SHA256


def test_profile_keeps_physical_thresholds_and_existing_hard_bounds() -> None:
    profile = dwd_germany_roi_profile()

    assert HARD_MAX_TARGET_PIXELS == 250_000
    assert HARD_MAX_DETECTION_OBJECTS == 500_000
    assert HARD_MAX_RECEIPT_BYTES == 64 * 1024 * 1024
    assert profile.pipeline_config.max_target_pixels == ROI_PIXELS
    assert profile.pipeline_config.max_detection_objects == ROI_PIXELS * 2
    assert profile.pipeline_config.max_receipt_bytes == HARD_MAX_RECEIPT_BYTES
    assert profile.detector_config.shield_threshold == 1.0
    assert profile.detector_config.core_threshold == 4.0
    assert profile.detector_config.connectivity == 8
    assert profile.detector_config.minimum_shield_area_m2 == 4_000_000.0
    assert profile.detector_config.minimum_core_area_m2 == 4_000_000.0
    assert profile.detector_config.shield_position_sigma_m == 1_000.0
    assert profile.detector_config.core_position_sigma_m == 1_000.0
    assert profile.transform_config.resampling_method.value == (
        "conservative_area_overlap_rate_v1"
    )


def test_existing_belgium_profile_is_byte_stable_and_caps_fail_closed() -> None:
    assert default_belgium_observation_profile().fingerprint == (
        EXPECTED_BELGIUM_PROFILE_SHA256
    )
    EvidenceToCellsShadowConfig(
        max_target_pixels=250_000,
        max_detection_objects=500_000,
        max_receipt_bytes=64 * 1024 * 1024,
    )
    with pytest.raises(ValueError, match="max_target_pixels"):
        EvidenceToCellsShadowConfig(
            max_target_pixels=250_001,
            max_detection_objects=500_000,
        )
    with pytest.raises(ValueError, match="max_detection_objects"):
        EvidenceToCellsShadowConfig(
            max_target_pixels=250_000,
            max_detection_objects=500_001,
        )


def test_selection_manifest_is_content_addressed_idempotent_and_closed(
    tmp_path: Path,
) -> None:
    first = seal_dwd_germany_roi_selection_manifest(tmp_path)
    second = seal_dwd_germany_roi_selection_manifest(tmp_path)

    assert first.manifest_sha256 == second.manifest_sha256
    assert first.manifest_path == second.manifest_path
    loaded = load_dwd_germany_roi_manifest(
        first.manifest_path,
        expected_sha256=first.manifest_sha256,
        expected_contract=DWD_GERMANY_ROI_SELECTION_CONTRACT,
    )
    assert loaded["tracker_outputs_observed_during_selection"] is False
    assert loaded["tracker_parameters_consulted_during_selection"] is False
    assert loaded["weather_system_rebinding_permitted"] is False
    assert loaded["spatial_aggregation"] == {
        "area_weighted": True,
        "source_resolution_m": 1_000,
        "source_window_height": 850,
        "source_window_width": 700,
        "target_resolution_m": 2_000,
        "target_window_height": 425,
        "target_window_width": 350,
        "target_pixels": 148_750,
        "truth_or_accuracy_claim": False,
    }
    assert json.loads(first.manifest_path.read_text("ascii")) == loaded

    first.manifest_path.write_bytes(first.manifest_path.read_bytes() + b" ")
    with pytest.raises(DwdGermanyRoiError, match="bytes differ"):
        load_dwd_germany_roi_manifest(
            first.manifest_path,
            expected_sha256=first.manifest_sha256,
            expected_contract=DWD_GERMANY_ROI_SELECTION_CONTRACT,
        )


def test_calibration_source_is_exact_bounded_and_tamper_evident(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"closed-calibration-source"
    archive_name = "composite_rv_20260807_0510.tar"
    archive = tmp_path / archive_name
    archive.write_bytes(payload)
    monkeypatch.setattr(roi_authority, "CALIBRATION_ARCHIVE_BYTES", len(payload))
    monkeypatch.setattr(
        roi_authority,
        "CALIBRATION_ARCHIVE_SHA256",
        hashlib.sha256(payload).hexdigest(),
    )

    receipt = verify_dwd_germany_roi_calibration_source(tmp_path.resolve())
    assert receipt.archive_name == archive_name
    assert receipt.archive_bytes == len(payload)
    assert receipt.archive_sha256 == hashlib.sha256(payload).hexdigest()

    archive.write_bytes(payload + b"tamper")
    with pytest.raises(DwdGermanyRoiError, match="size differs"):
        verify_dwd_germany_roi_calibration_source(tmp_path.resolve())


def test_replay_cap_uses_fixed_margin_rounding_and_fails_closed() -> None:
    mib = 1024 * 1024
    cap = derive_dwd_germany_roi_replay_cap(
        observed_peak_boundary_bytes=700 * mib,
        observed_peak_rss_bytes=300 * mib,
        active_children_after_run=0,
    )
    assert cap.calibration_cap_bytes == CALIBRATION_RSS_CAP_BYTES
    assert cap.derived_replay_cap_bytes == 960 * mib

    with pytest.raises(DwdGermanyRoiError, match="active decoder child"):
        derive_dwd_germany_roi_replay_cap(
            observed_peak_boundary_bytes=700 * mib,
            observed_peak_rss_bytes=300 * mib,
            active_children_after_run=1,
        )


def test_full_replay_authority_rederives_calibration_cap(tmp_path: Path) -> None:
    mib = 1024 * 1024
    selection = seal_dwd_germany_roi_selection_manifest(tmp_path)
    cap = derive_dwd_germany_roi_replay_cap(
        observed_peak_boundary_bytes=700 * mib,
        observed_peak_rss_bytes=300 * mib,
        active_children_after_run=0,
    )
    profile = dwd_germany_roi_profile()
    summary = {
        "active_decoder_children_after_run": 0,
        "calibration_hard_rss_cap_bytes": CALIBRATION_RSS_CAP_BYTES,
        "child_cpu_max_seconds": 10.0,
        "child_elapsed_max_seconds": 11.0,
        "contract": "stv3-dwd-rv-observation-replay-v1",
        "evidence_records": 1,
        "frame_count": 1,
        "isolation_authority_sha256": "1" * 64,
        "isolation_limits_sha256": (
            roi_authority.CALIBRATION_ISOLATION_LIMITS_SHA256
        ),
        "metrics_manifest_sha256": "2" * 64,
        "parent_cpu_seconds": 12.0,
        "parent_peak_rss_bytes": 200 * mib,
        "parent_rss_baseline_bytes": 100 * mib,
        "parent_wall_seconds": 20.0,
        "peak_boundary_max_bytes": 700 * mib,
        "peak_rss_max_bytes": 300 * mib,
        "profile_sha256": profile.fingerprint,
        "sequence_id": "3" * 64,
        "sequence_manifest_sha256": "4" * 64,
        "source_manifest_sha256": roi_authority.CALIBRATION_SOURCE_MANIFEST_SHA256,
        "spatial_aggregation_is_truth": False,
    }
    calibration = seal_dwd_germany_roi_calibration_manifest(
        tmp_path,
        selection=selection,
        calibration_summary=summary,
        calibration_source_archive_sha256=roi_authority.CALIBRATION_ARCHIVE_SHA256,
        cap=cap,
    )

    authority = authorize_dwd_germany_roi_full_replay(
        selection=selection,
        calibration_manifest_path=calibration.manifest_path,
        expected_calibration_sha256=calibration.manifest_sha256,
    )
    assert authority.replay_rss_cap_bytes == 960 * mib
    assert authority.profile_sha256 == profile.fingerprint

    with pytest.raises(DwdGermanyRoiError, match="content-addressed"):
        authorize_dwd_germany_roi_full_replay(
            selection=selection,
            calibration_manifest_path=calibration.manifest_path,
            expected_calibration_sha256="f" * 64,
        )
    with pytest.raises(DwdGermanyRoiError, match="lower replay cap"):
        derive_dwd_germany_roi_replay_cap(
            observed_peak_boundary_bytes=1_300 * mib,
            observed_peak_rss_bytes=300 * mib,
            active_children_after_run=0,
        )


def test_roi_authority_has_no_live_service_or_home_assistant_imports() -> None:
    module_path = (
        Path(__file__).parents[1]
        / "storm_tracker_worker"
        / "science_shadow_dwd_roi_export.py"
    )
    tree = ast.parse(module_path.read_text("utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    forbidden = ("service", "mqtt", "homeassistant", "worker_first", "snapshot")
    assert not any(token in name.lower() for name in imported for token in forbidden)
