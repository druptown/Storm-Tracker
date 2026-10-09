from __future__ import annotations

from dataclasses import replace
import hashlib
import sqlite3

import numpy as np
from pyproj import Transformer

from storm_tracker_worker.canonical_evidence_store import CoverageSpec, NativeGridSpec
from storm_tracker_worker.canonical_ground_evidence_store import (
    GroundMeasurementValue,
    GroundObservationFact,
    GroundObservationReceipt,
    GroundStationSite,
)
from storm_tracker_worker.canonical_radar_field_bridge import (
    NativeRainRateFieldCandidate,
    default_radar_semantics_certificates,
)
from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.multi_source_radar_facts import VerifiedRadarFieldSource
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    NormalizedRasterFrame,
)
from storm_tracker_worker.worker_only_cell_tracking import (
    ADMISSION_STATUS,
    MAX_ROI_PIXELS,
    WorkerOnlyCellTrackingRuntime,
    WorkerOnlyProjectedTarget,
    WorkerOnlyTrackingBatch,
    WorkerOnlyTrackingDisposition,
    WorkerOnlyTrackingFrameCandidate,
    build_worker_only_tracking_batch,
)


NOW = 1_800_400_000.0


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _desired(*, radius_km: float = 20.0):
    return parse_desired_state({
        "authority_epoch": 1,
        "contract": "stv3-worker-desired-state-v1",
        "generation": 1,
        "issued_at": NOW - 60.0,
        "previous_state_sha256": None,
        "producer_id": "ha:worker-only-cell-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "Testtracker",
            "kind": "test",
            "latitude": 50.85,
            "location_accuracy_m": 10.0,
            "longitude": 4.35,
            "observation_radius_km": radius_km,
            "position_observed_at": NOW - 30.0,
            "target_id": "test.tracker",
        }],
    })


def _readonly(value, dtype=None):
    result = np.asarray(value, dtype=dtype).copy()
    result.setflags(write=False)
    return result


def _metoffice_source(*, valid_at: float = NOW) -> VerifiedRadarFieldSource:
    certificate = next(
        item
        for item in default_radar_semantics_certificates()
        if item.certificate_id == "met-office.odim-rate.v1"
    )
    width = height = 80
    x, y = Transformer.from_crs(
        "EPSG:4326",
        "EPSG:3857",
        always_xy=True,
    ).transform(4.35, 50.85)
    grid = AnalysisGrid(
        grid_id="metoffice-exact-test-grid",
        crs="EPSG:3857",
        origin_x_m=x - 40_000.0,
        origin_y_m=y + 40_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=width,
        height=height,
    )
    rates = np.zeros((height, width), dtype=np.float64)
    rates[38:42, 37:41] = 2.0
    valid = np.ones((height, width), dtype=np.bool_)
    missing = np.zeros((height, width), dtype=np.bool_)
    field = NativeRainRateFieldCandidate(
        candidate_id="radar-field-" + _sha(f"candidate:{valid_at}"),
        canonical_run_id="run-" + _sha(f"run:{valid_at}"),
        canonical_commit_id="commit-" + _sha(f"commit:{valid_at}"),
        canonical_receipt_sha256=_sha(f"canonical-receipt:{valid_at}"),
        canonical_result_sha256=_sha(f"canonical-result:{valid_at}"),
        capture_id="capture-" + _sha(f"capture:{valid_at}"),
        capture_receipt_sha256=_sha(f"capture-receipt:{valid_at}"),
        capture_manifest_sha256=_sha(f"capture-manifest:{valid_at}"),
        artifact_handle=certificate.artifact_handle_prefix + _sha("artifact"),
        artifact_sha256=_sha("artifact"),
        artifact_metadata_sha256=_sha("artifact-metadata"),
        semantics_certificate_id=certificate.certificate_id,
        semantics_certificate_sha256=certificate.fingerprint,
        provider=certificate.provider,
        product=certificate.product,
        decoder_identity=certificate.decoder_identity,
        decoder_version=certificate.decoder_version,
        decoder_config_sha256=certificate.decoder_config_sha256,
        source_quantity=certificate.receipt_quantity,
        source_unit=certificate.receipt_unit,
        source_values=_readonly(rates),
        output_quantity=certificate.output_quantity,
        output_unit=certificate.output_unit,
        rain_rate_mm_h=_readonly(rates),
        valid_mask=_readonly(valid),
        nodata_mask=_readonly(missing),
        undetect_mask=_readonly(missing),
        auxiliary_planes=(),
        temporal_semantics=certificate.temporal_semantics,
        support_start=valid_at - 300.0,
        support_end=valid_at,
        valid_at=valid_at,
        conversion="identity_mm_h",
        quality_state="unavailable:odim_quality_dataset_absent",
        native_crs=grid.crs,
        native_grid=NativeGridSpec(
            kind="regular",
            width=width,
            height=height,
            axis_order="yx",
            definition_sha256=_sha("native-grid"),
        ),
        native_coverage=CoverageSpec(
            kind="native_bbox",
            minimum_x=grid.origin_x_m,
            minimum_y=grid.south_edge_m,
            maximum_x=grid.east_edge_m,
            maximum_y=grid.origin_y_m,
            definition_sha256=_sha("native-coverage"),
        ),
        analysis_grid=grid,
        metric_raster_field=None,
        primary_sources=certificate.primary_sources,
    )
    return VerifiedRadarFieldSource(field=field, received_at=valid_at + 10.0)


def _frame(frame_id: str, valid_at: float, column: int) -> NormalizedRasterFrame:
    grid = AnalysisGrid(
        grid_id="synthetic-worker-only-tracking-grid",
        crs="EPSG:3857",
        origin_x_m=0.0,
        origin_y_m=10_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=20,
        height=10,
    )
    values = [[0.0 for _ in range(grid.width)] for _ in range(grid.height)]
    values[4][column] = 2.0
    return NormalizedRasterFrame(
        frame_id=frame_id,
        valid_at=valid_at,
        source="radar.metoffice.uk_rate:synthetic-lineage",
        quantity="surface_precipitation_rate_estimate",
        unit="mm/h",
        grid=grid,
        values=tuple(tuple(row) for row in values),
    )


def _batch(frame: NormalizedRasterFrame) -> WorkerOnlyTrackingBatch:
    candidate = WorkerOnlyTrackingFrameCandidate(
        workload_id="radar.metoffice.uk_rate",
        region_id="region:" + _sha("region"),
        region_lineage_id="tracking-region:" + _sha("lineage"),
        target_ids=("test.tracker",),
        capture_id="capture-" + _sha(frame.frame_id),
        source_candidate_id="radar-field-" + _sha(frame.frame_id),
        source_candidate_content_sha256=_sha("content:" + frame.frame_id),
        requested_radius_km=20.0,
        covered_radius_km=10.0,
        coverage_complete=False,
        frame=frame,
        projected_targets=(WorkerOnlyProjectedTarget(
            target_id="test.tracker",
            x_m=8_500.0,
            y_m=5_500.0,
            location_accuracy_m=10.0,
        ),),
    )
    return WorkerOnlyTrackingBatch(
        workload_id=candidate.workload_id,
        source_candidate_id=candidate.source_candidate_id,
        dispositions=(WorkerOnlyTrackingDisposition(
            candidate.workload_id,
            candidate.region_id,
            "candidate",
            "bounded_metric_roi_ready",
            candidate,
        ),),
    )


def _ground_receipt(*, observed_at: float, x_m: float, y_m: float):
    longitude, latitude = Transformer.from_crs(
        "EPSG:3857",
        "EPSG:4326",
        always_xy=True,
    ).transform(x_m, y_m)
    fact = GroundObservationFact(
        capture_id="capture-" + _sha("ground-capture"),
        capture_receipt_sha256=_sha("ground-capture-receipt"),
        members=(),
        source_member_role="response_json",
        source_member_content_sha256=_sha("ground-member-content"),
        source_member_receipt_sha256=_sha("ground-member-receipt"),
        source_record_locator="station:test",
        source_record_sha256=_sha("ground-record"),
        decoder_identity="synoptic-ground-test",
        decoder_version="1",
        config_sha256=_sha("ground-config"),
        provider="synoptic",
        product="precipitation",
        station=GroundStationSite(
            provider_station_id="station-test",
            site_id="provider-site-test",
            network="test-network",
            station_name="Teststation",
            position_state="known",
            latitude_decimal=format(latitude, ".17g"),
            longitude_decimal=format(longitude, ".17g"),
            elevation_state="unknown",
            elevation_m_decimal=None,
        ),
        observed_at=observed_at,
        representative_interval_start=observed_at,
        representative_interval_end=observed_at,
        received_at=observed_at + 10.0,
        ingested_at=observed_at + 11.0,
        measurement=GroundMeasurementValue(
            variable="precipitation_accumulation",
            state="observed",
            representation="decimal",
            exact_value="1.0",
            unit="mm",
            censor_limit=None,
        ),
        provider_qc=(),
    )
    return GroundObservationReceipt(
        fact_id="ground-fact-" + _sha("ground-fact"),
        observation_key="ground-observation-" + _sha("ground-observation"),
        fact_sha256=_sha("ground-fact-content"),
        receipt_sha256=_sha("ground-receipt"),
        capture_manifest_sha256=_sha("ground-manifest"),
        committed_at=observed_at + 12.0,
        fact=fact,
    )
def test_metric_provider_builds_one_bounded_region_frame_without_admission():
    batch = build_worker_only_tracking_batch(
        workload_id="radar.metoffice.uk_rate",
        desired_state=_desired(radius_km=300.0),
        source=_metoffice_source(),
    )

    assert len(batch.dispositions) == 1
    disposition = batch.dispositions[0]
    assert disposition.state == "candidate"
    assert disposition.reason_code == "bounded_metric_roi_ready"
    candidate = disposition.candidate
    assert candidate is not None
    assert candidate.admission_status == ADMISSION_STATUS
    assert candidate.frame.grid.width * candidate.frame.grid.height <= MAX_ROI_PIXELS
    assert candidate.coverage_complete is False
    assert candidate.frame.values[40][40] == 2.0


def test_tracking_grid_identity_uses_geometry_not_frame_bound_upstream_id():
    first_source = _metoffice_source(valid_at=NOW)
    second_source = _metoffice_source(valid_at=NOW + 300.0)
    source_grid = second_source.field.analysis_grid
    assert source_grid is not None
    frame_bound_grid = AnalysisGrid(
        grid_id="metoffice-content-bound-frame-2",
        crs=source_grid.crs,
        origin_x_m=source_grid.origin_x_m,
        origin_y_m=source_grid.origin_y_m,
        x_resolution_m=source_grid.x_resolution_m,
        y_resolution_m=source_grid.y_resolution_m,
        width=source_grid.width,
        height=source_grid.height,
        coordinate_unit=source_grid.coordinate_unit,
        axis_order=source_grid.axis_order,
        row_order=source_grid.row_order,
        contract_version=source_grid.contract_version,
    )
    second_source = VerifiedRadarFieldSource(
        field=replace(second_source.field, analysis_grid=frame_bound_grid),
        received_at=second_source.received_at,
    )

    first = build_worker_only_tracking_batch(
        workload_id="radar.metoffice.uk_rate",
        desired_state=_desired(),
        source=first_source,
    ).dispositions[0].candidate
    second = build_worker_only_tracking_batch(
        workload_id="radar.metoffice.uk_rate",
        desired_state=_desired(),
        source=second_source,
    ).dispositions[0].candidate

    assert first is not None
    assert second is not None
    assert first.frame.grid.grid_id == second.frame.grid.grid_id
    assert first.frame.grid.same_geometry(second.frame.grid)


def test_cell_runtime_needs_two_frames_and_deduplicates_same_valid_time(tmp_path):
    state_path = tmp_path / "cell-tracking.sqlite3"
    runtime = WorkerOnlyCellTrackingRuntime(state_path=state_path)
    first = runtime.process_batch(_batch(_frame("frame-1", NOW, 5)))
    first_diagnostics = runtime.diagnostics()

    restarted = WorkerOnlyCellTrackingRuntime(state_path=state_path)
    second = restarted.process_batch(_batch(_frame("frame-2", NOW + 300.0, 6)))
    duplicate = restarted.process_batch(_batch(_frame("frame-2", NOW + 300.0, 6)))
    diagnostics = restarted.diagnostics()

    assert first.advanced_regions == 1
    assert first.motion_ready_tracks == 0
    assert second.advanced_regions == 1
    assert second.active_tracks == 1
    assert second.tracked_tracks == 1
    assert second.motion_ready_tracks == 1
    assert second.shadow_arrival_candidates == 1
    assert second.shadow_future_arrival_candidates == 1
    arrival = restarted.shadow_arrivals()[0]
    assert arrival.target_id == "test.tracker"
    assert arrival.passage_state == "future_intersection"
    assert arrival.entry_seconds == 450.0
    assert arrival.exit_seconds == 750.0
    assert arrival.admission_status == ADMISSION_STATUS
    assert duplicate.duplicate_regions == 1
    assert diagnostics["motion_ready_tracks"] == 1
    assert diagnostics["forecast_generated"] is False
    assert diagnostics["shadow_arrival_candidate_count"] == 1
    assert diagnostics["shadow_arrival_future_count"] == 1
    assert first_diagnostics["restart_persistent"] is True
    assert first_diagnostics["store_state_bytes"] > 0
    assert diagnostics["restart_persistent"] is True
    assert diagnostics["counts"]["restored"] == 1

    match_count = restarted.process_ground_receipts(
        workload_id="ground.synoptic.region",
        receipts=(_ground_receipt(
            observed_at=NOW,
            x_m=5_500.0,
            y_m=5_500.0,
        ),),
    )
    match = restarted.ground_track_matches()[0]
    assert match_count == 1
    assert match.radar_workload_id == "radar.metoffice.uk_rate"
    assert match.ground_workload_id == "ground.synoptic.region"
    assert match.back_advection_seconds == 300.0
    assert match.site_identity_status == "unresolved"
    assert match.semantics_status == "uninterpreted_canonical_measurement"
    assert match.admission_status == ADMISSION_STATUS


def test_nonmetric_noaa_tracking_is_explicitly_withheld():
    source = _metoffice_source()
    # Workload/source mismatch must fail before a non-metric shortcut can be
    # used to smuggle another provider's field into the tracker.
    try:
        build_worker_only_tracking_batch(
            workload_id="satellite.noaa.err",
            desired_state=_desired(),
            source=source,
        )
    except RuntimeError as error:
        assert str(error) == "tracking_source_workload_binding_mismatch"
    else:
        raise AssertionError("provider/workload mismatch was not rejected")


def test_tampered_restart_head_is_disabled_without_becoming_weather(tmp_path):
    state_path = tmp_path / "tampered-cell-tracking.sqlite3"
    runtime = WorkerOnlyCellTrackingRuntime(state_path=state_path)
    runtime.process_batch(_batch(_frame("frame-tamper", NOW, 5)))

    with sqlite3.connect(state_path) as database, database:
        body = bytes(database.execute(
            "SELECT compressed_state FROM tracking_heads"
        ).fetchone()[0])
        tampered = bytearray(body)
        tampered[len(tampered) // 2] ^= 0x01
        database.execute(
            "UPDATE tracking_heads SET compressed_state=?",
            (sqlite3.Binary(bytes(tampered)),),
        )

    restarted = WorkerOnlyCellTrackingRuntime(state_path=state_path)
    diagnostics = restarted.diagnostics()

    assert diagnostics["restart_persistent"] is False
    assert diagnostics["store_error"] == "tracking_store_restore_failed"
    assert diagnostics["region_states"] == 0
    assert diagnostics["active_tracks"] == 0
    assert diagnostics["forecast_generated"] is False
