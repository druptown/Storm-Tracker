from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from storm_tracker_worker.analysis_grid_transform import analysis_grid_sha256
from storm_tracker_worker.dwd_rv_completion_consumer import (
    DwdRvCompletionOutputReceipt,
)
from storm_tracker_worker.dwd_rv_science_sequence import (
    DwdRvProcessMetric,
    DwdRvScienceSequenceIntegrityError,
    DwdRvSourceManifestReceipt,
    EXPECTED_FORECAST_LEADS_SECONDS,
    inventory_source_archives,
    load_evidence_record,
    load_sequence_manifest,
    load_source_manifest,
    seal_process_metrics_manifest,
    seal_sequence_manifest,
    write_evidence_record,
)
from storm_tracker_worker.evidence_to_cells_shadow import (
    EvidenceToCellsPixelAccounting,
)
from storm_tracker_worker.science_shadow_orchestrator import ShadowEvidenceFrame
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
    detect_hierarchical_cells,
)


REPOSITORY = Path(__file__).resolve().parents[1]
OFFICIAL_SEQUENCE = (
    REPOSITORY / ".analysis" / "dwd-rv-observation-sequence-20260807"
)


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _source_receipt(tmp_path: Path, times: tuple[float, ...]):
    frames = []
    total = 0
    for index, valid_at in enumerate(times):
        size = 1_000 + index
        total += size
        frames.append({
            "archive_bytes": size,
            "archive_name": f"composite_rv_20260807_{510 + index * 5:04d}.tar",
            "archive_sha256": _sha(f"archive:{index}"),
            "available_forecast_leads_seconds": list(
                EXPECTED_FORECAST_LEADS_SECONDS
            ),
            "base_time": valid_at,
            "frame_number": index,
            "lead_seconds": 0,
            "observation_member": f"observation-{index}-000-hd5",
        })
    body = _canonical({
        "cadence_seconds": 300,
        "contract": "stv3-dwd-rv-observation-source-manifest-v1",
        "frame_count": len(frames),
        "frames": frames,
        "motion_member_policy": "base_plus000_only",
        "raw_refetch_performed": False,
        "total_archive_bytes": total,
    })
    path = tmp_path / "source.json"
    path.write_bytes(body)
    return DwdRvSourceManifestReceipt(
        manifest_sha256=hashlib.sha256(body).hexdigest(),
        manifest_bytes=len(body),
        manifest_path=path,
        frame_count=len(frames),
        total_archive_bytes=total,
        valid_times=times,
    )


def _evidence(index: int, valid_at: float) -> ShadowEvidenceFrame:
    run_id = "run-" + _sha(f"run:{index}")
    grid = AnalysisGrid(
        grid_id="dwd-sequence-test-grid",
        crs="EPSG:3035",
        origin_x_m=0.0,
        origin_y_m=2_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=2,
        height=2,
    )
    frame = NormalizedRasterFrame(
        frame_id="frame-" + _sha(f"frame:{index}"),
        valid_at=valid_at,
        source="canonical:dwd_radolan/dwd_open_data_composite_rv_capture",
        quantity="interval_mean_precipitation_rate",
        unit="mm/h",
        grid=grid,
        values=((0.0, 1.5), (None, 5.0)),
    )
    detector = HierarchicalDetectorConfig(
        shield_threshold=1.0,
        core_threshold=4.0,
        minimum_shield_area_m2=1_000_000.0,
        minimum_core_area_m2=1_000_000.0,
        shield_position_sigma_m=500.0,
        core_position_sigma_m=350.0,
        connectivity=8,
        centroid_method="geometric",
    )
    detected = detect_hierarchical_cells(frame, detector)
    accounting = EvidenceToCellsPixelAccounting(
        total_analysis_pixels=4,
        valid_detector_input_pixels=3,
        valid_below_shield_threshold_pixels=1,
        rain_shield_pixels=2,
        embedded_core_pixels=1,
        excluded_nodata_pixels=1,
        excluded_undetect_pixels=0,
        excluded_mixed_pixels=0,
        excluded_outside_support_pixels=0,
        detection_objects=len(detected.detections),
        rain_shield_objects=len(detected.rain_shields),
        embedded_core_objects=len(detected.embedded_cores),
        low_confidence_objects=len(detected.low_confidence_fragments),
    )
    evidence_sha = _sha(f"evidence:{index}")
    analysis_sha = _sha(f"analysis:{index}")
    transform_sha = _sha(f"transform-receipt:{index}")
    return ShadowEvidenceFrame(
        canonical_run_id=run_id,
        canonical_commit_id="commit-" + _sha(f"commit:{index}"),
        canonical_receipt_sha256=_sha(f"canonical-receipt:{index}"),
        canonical_result_sha256=_sha(f"canonical-result:{index}"),
        capture_id="capture-" + _sha(f"capture:{index}"),
        capture_receipt_sha256=_sha(f"capture-receipt:{index}"),
        capture_manifest_sha256=_sha(f"capture-manifest:{index}"),
        evidence_candidate_id="evidence-cells-" + evidence_sha,
        evidence_content_sha256=evidence_sha,
        native_candidate_id="radar-field-" + _sha(f"native:{index}"),
        native_candidate_content_sha256=_sha(f"native-content:{index}"),
        native_lineage_sha256=_sha(f"native-lineage:{index}"),
        analysis_field_id="analysis-field-" + analysis_sha,
        analysis_field_content_sha256=analysis_sha,
        analysis_lineage_sha256=_sha(f"analysis-lineage:{index}"),
        radar_semantics_certificate_id="dwd-rv-semantics-test-v1",
        radar_semantics_certificate_sha256=_sha("semantics"),
        transform_certificate_id="dwd-rv-transform-test-v1",
        transform_certificate_sha256=_sha("transform"),
        transform_receipt_id="transform-receipt-" + transform_sha,
        transform_receipt_sha256=transform_sha,
        transform_config_sha256=_sha("transform-config"),
        target_grid_sha256=analysis_grid_sha256(grid),
        detector_config_sha256=_sha("detector-config"),
        pipeline_config_sha256=_sha("pipeline-config"),
        normalized_frame=frame,
        detection_frame=detected,
        pixel_accounting=accounting,
        operational_admission_blockers=("hard_process_isolation_not_wired",),
    )


def _completion(index: int, evidence: ShadowEvidenceFrame):
    output = {
        "decoder": {
            "commit_id": evidence.canonical_commit_id,
            "receipt_sha256": evidence.canonical_receipt_sha256,
            "result_sha256": evidence.canonical_result_sha256,
            "run_id": evidence.canonical_run_id,
        },
        "handoff": {
            "capture_id": evidence.capture_id,
            "capture_manifest_sha256": evidence.capture_manifest_sha256,
            "capture_receipt_sha256": evidence.capture_receipt_sha256,
        },
        "process_isolation": {"authority_sha256": _sha("isolation")},
        "shadow": {
            "analysis_field_content_sha256": (
                evidence.analysis_field_content_sha256
            ),
            "content_sha256": evidence.evidence_content_sha256,
            "target_grid_sha256": evidence.target_grid_sha256,
            "valid_at": evidence.normalized_frame.valid_at,
        },
    }
    return DwdRvCompletionOutputReceipt(
        receipt_sha256=_sha(f"output-receipt:{index}"),
        output_sha256=_sha(f"output:{index}"),
        event_id=f"completion-{_sha(f'event:{index}')}",
        event_sha256=_sha(f"event-sha:{index}"),
        producer_receipt_sha256=_sha(f"producer:{index}"),
        desired_state_sha256=_sha("desired"),
        authority_sha256=_sha("authority"),
        consumer_id="dwd-rv-quantitative-decoder-v1",
        stream_id="radar.dwd.radolan_rv",
        disposition="withheld",
        fact_revision_sha256=None,
        isolation_authority_sha256=_sha("isolation"),
        isolation_effect_contract_sha256=_sha("effect"),
        isolation_limits_sha256=_sha("limits"),
        process_receipt_sha256=_sha(f"process:{index}"),
        profile_sha256=_sha("profile"),
        candidate_content_sha256=evidence.evidence_content_sha256,
        decoder_run_id=evidence.canonical_run_id,
        created_at=evidence.normalized_frame.valid_at + 10.0,
        output=output,
    )


def test_official_36_archive_inventory_is_exact_and_plus000_only(tmp_path):
    if not OFFICIAL_SEQUENCE.is_dir():
        pytest.skip("official DWD observation corpus is unavailable")
    receipt = inventory_source_archives(OFFICIAL_SEQUENCE, tmp_path)
    manifest = load_source_manifest(receipt)

    assert receipt.frame_count == 36
    assert receipt.total_archive_bytes == 41_072_640
    assert manifest["frames"][0]["archive_sha256"] == (
        "956a3feac1298e57457beb3186b69e0e8c565080adf9eb3f56075380f6332977"
    )
    assert manifest["frames"][-1]["archive_sha256"] == (
        "f86d8509de1076cd365d2951322cfe3d8a817b4fb91a5cefb6e30bb8cf841cd5"
    )
    assert {item["lead_seconds"] for item in manifest["frames"]} == {0}
    assert all(
        item["available_forecast_leads_seconds"]
        == list(EXPECTED_FORECAST_LEADS_SECONDS)
        for item in manifest["frames"]
    )


def test_evidence_record_roundtrips_idempotently_and_rejects_forecast(tmp_path):
    evidence = _evidence(0, 1_786_079_400.0)
    completion = _completion(0, evidence)
    first = write_evidence_record(
        tmp_path, evidence=evidence, completion_receipt=completion
    )
    second = write_evidence_record(
        tmp_path, evidence=evidence, completion_receipt=completion
    )
    loaded, binding = load_evidence_record(
        first.record_path, expected_sha256=first.record_sha256
    )

    assert first == second
    assert loaded == evidence
    assert binding["output_sha256"] == completion.output_sha256
    with pytest.raises(DwdRvScienceSequenceIntegrityError):
        write_evidence_record(
            tmp_path,
            evidence=evidence,
            completion_receipt=completion,
            lead_seconds=300,
        )
    mismatched = replace(
        completion,
        output={
            **completion.output,
            "shadow": {**completion.output["shadow"], "valid_at": 0.0},
        },
    )
    with pytest.raises(DwdRvScienceSequenceIntegrityError):
        write_evidence_record(
            tmp_path, evidence=evidence, completion_receipt=mismatched
        )


def test_sequence_and_metrics_are_content_bound_and_fully_accounted(tmp_path):
    times = (1_786_079_400.0, 1_786_079_700.0)
    source = _source_receipt(tmp_path, times)
    export = tmp_path / "export"
    records = tuple(
        write_evidence_record(
            export,
            evidence=(evidence := _evidence(index, valid_at)),
            completion_receipt=_completion(index, evidence),
        )
        for index, valid_at in enumerate(times)
    )
    sequence = seal_sequence_manifest(
        export, source_manifest=source, evidence_records=records
    )
    loaded = load_sequence_manifest(
        sequence.manifest_path, expected_sha256=sequence.manifest_sha256
    )
    metrics = tuple(
        DwdRvProcessMetric(
            frame_number=index,
            valid_at=valid_at,
            event_id=f"completion-{_sha(f'event:{index}')}",
            event_sha256=_sha(f"event-sha:{index}"),
            output_sha256=_sha(f"output:{index}"),
            elapsed_seconds=10.0 + index,
            child_cpu_seconds=8.0 + index,
            child_peak_rss_bytes=(220 + index) * 1024 * 1024,
            child_peak_boundary_bytes=(680 + index) * 1024 * 1024,
        )
        for index, valid_at in enumerate(times)
    )
    metrics_receipt = seal_process_metrics_manifest(
        export,
        source_manifest=source,
        sequence=sequence,
        isolation_authority_sha256=_sha("isolation-authority"),
        isolation_limits_sha256=_sha("isolation-limits"),
        metrics=metrics,
        configured_boundary_memory_cap_bytes=960 * 1024 * 1024,
        configured_cpu_cap_seconds=60.0,
        configured_wall_cap_seconds=90.0,
    )

    assert loaded["frame_count"] == 2
    assert loaded["motion_skill_claimed"] is False
    assert metrics_receipt.frame_count == 2
    assert metrics_receipt.peak_rss_max_bytes == 221 * 1024 * 1024
    assert metrics_receipt.peak_boundary_max_bytes == 681 * 1024 * 1024
    with pytest.raises(DwdRvScienceSequenceIntegrityError):
        seal_sequence_manifest(
            export,
            source_manifest=source,
            evidence_records=(records[0], replace(records[1], valid_at=times[1] + 1)),
        )
    with pytest.raises(DwdRvScienceSequenceIntegrityError):
        seal_process_metrics_manifest(
            export,
            source_manifest=source,
            sequence=sequence,
            isolation_authority_sha256=_sha("isolation-authority"),
            isolation_limits_sha256=_sha("isolation-limits"),
            metrics=metrics,
            configured_boundary_memory_cap_bytes=681 * 1024 * 1024,
            configured_cpu_cap_seconds=60.0,
            configured_wall_cap_seconds=90.0,
        )


def test_source_or_evidence_tamper_is_fail_closed(tmp_path):
    times = (1_786_079_400.0,)
    source = _source_receipt(tmp_path, times)
    evidence = _evidence(0, times[0])
    record = write_evidence_record(
        tmp_path / "export",
        evidence=evidence,
        completion_receipt=_completion(0, evidence),
    )
    record.record_path.write_bytes(record.record_path.read_bytes() + b" ")
    with pytest.raises(DwdRvScienceSequenceIntegrityError):
        load_evidence_record(
            record.record_path, expected_sha256=record.record_sha256
        )
    source.manifest_path.write_bytes(source.manifest_path.read_bytes() + b" ")
    with pytest.raises(DwdRvScienceSequenceIntegrityError):
        load_source_manifest(source)


def test_observation_replay_remains_absent_from_live_wiring():
    live_files = [
        REPOSITORY / "storm_tracker_worker" / "service.py",
        REPOSITORY / "storm_tracker_worker" / "autonomous_scheduler.py",
        REPOSITORY / "storm_tracker_worker" / "config.yaml",
    ]
    integration = REPOSITORY / "custom_components" / "storm_tracker_v3"
    live_files.extend(
        path for path in integration.rglob("*")
        if path.is_file() and path.suffix in {".py", ".json", ".yaml", ".yml"}
    )
    forbidden = (
        "dwd_rv_observation_replay",
        "dwd_rv_science_sequence",
        "stv3-science-shadow-canonical-sequence-v1",
    )
    for path in live_files:
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        assert not any(item in text for item in forbidden), path
