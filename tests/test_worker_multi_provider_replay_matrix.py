"""Real-fixture and fail-closed tests for the inert provider replay matrix."""
from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from storm_tracker_worker.hard_decoder_isolation import _canonical_json_bytes
from storm_tracker_worker.multi_provider_replay_matrix import (
    MultiProviderReplayError,
    build_report,
    load_houston_rainviewer_sequences,
    load_kmi_public_composite_sequence,
    probe_knmi_authority_golden,
    probe_noaa_err_golden,
    probe_opera_golden,
    probe_rainviewer_raw_golden,
    run_categorical_sequence,
    snapshot_source_tree,
    write_report,
)


ROOT = Path(__file__).parents[1]
KMI = ROOT / ".analysis" / "kmi_public_radar_composite_20260730_sequence_20"
HOUSTON = ROOT / ".analysis" / "houston_motion_replay.json"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(item.name for item in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


def test_real_kmi_and_houston_sequences_are_completely_accounted() -> None:
    kmi = load_kmi_public_composite_sequence(KMI)
    houston = load_houston_rainviewer_sequences(HOUSTON)

    assert len(kmi.frames) == 20
    assert kmi.cadence_seconds == 300
    assert kmi.frames[0].frame.nominal_timestamp == 1_785_410_100
    assert kmi.frames[-1].frame.nominal_timestamp == 1_785_415_800
    assert len({item.source_content_sha256 for item in kmi.frames}) == 20
    assert sum(len(item.frame.cells) for item in kmi.frames) == 1_125_466
    assert kmi.material_kind == "structural_categorical_from_exact_geotiff"
    assert not kmi.meteorological_admission_granted
    assert not kmi.motion_claim_generated

    assert tuple(len(item.frames) for item in houston) == (12, 2, 10)
    assert sum(len(item.frames) for item in houston) == 24
    assert all(item.cadence_seconds == 600 for item in houston)
    assert all(not item.meteorological_admission_granted for item in houston)
    assert all(not item.motion_claim_generated for item in houston)
    assert len({item.source_snapshot.manifest_sha256 for item in houston}) == 1


@pytest.fixture(scope="module")
def houston_two_frame_result(tmp_path_factory):
    output = tmp_path_factory.mktemp("provider-matrix")
    sequence = load_houston_rainviewer_sequences(HOUSTON)[1]
    return run_categorical_sequence(
        sequence,
        source_path=HOUSTON,
        receipt_path=output / "receipts.sqlite3",
        maximum_analysis_pixels=1_500,
        restart_after_frames=1,
    )


def test_real_houston_cohort_runs_v4_advected_chain_without_candidate_loss(
    houston_two_frame_result,
) -> None:
    result = houston_two_frame_result
    assert result.frame_count == result.processed_frames == result.advanced_frames == 2
    assert result.final_revision == 2
    assert result.restart_count == result.duplicate_retries == 1
    assert result.candidate_edges == result.expected_candidate_edges > 0
    assert (
        result.advected_pair_count + result.unadvected_fallback_pair_count
        == result.candidate_edges
    )
    assert result.unique_association_evidence_count == result.candidate_edges
    assert result.translation_available_frames == 1
    assert result.source_unchanged
    assert not result.motion_claim_generated
    assert not result.meteorological_admission_granted
    assert not result.operational_snapshot_claim_generated


def test_source_manifest_ignores_only_empty_lock_control_files_and_detects_tamper(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "evidence.bin").write_bytes(b"immutable-source")
    (source / ".publish.lock").write_bytes(b"")
    first = snapshot_source_tree(source)
    (source / ".publish.lock").unlink()
    assert snapshot_source_tree(source) == first
    (source / "evidence.bin").write_bytes(b"tampered-source")
    assert snapshot_source_tree(source) != first


def test_real_single_frame_goldens_decode_or_withhold_exactly(tmp_path: Path) -> None:
    noaa = probe_noaa_err_golden(
        ROOT / ".analysis" / "noaa-err-live-golden",
        tmp_path / "noaa",
    )
    opera = probe_opera_golden(
        ROOT / ".analysis" / "opera-native-live-golden-20260802T0045",
        tmp_path / "opera",
    )
    knmi = probe_knmi_authority_golden(
        ROOT / "tests" / "fixtures" / "knmi_rtcor_analysis_grid_authority.json"
    )
    rainviewer = probe_rainviewer_raw_golden(
        ROOT / ".analysis" / "rainviewer-live-golden-v2"
    )

    assert noaa.outcome == "decoded_complete"
    assert noaa.grid_shape == (251, 351)
    assert noaa.total_pixels == noaa.accounted_pixels == 88_101
    assert noaa.capture_member_count == noaa.accounted_members == 2
    assert noaa.duplicate_retry_exact

    # This retained OPERA capture predates the stricter current provenance
    # certificate.  Quantitative decode must reject, while the exact native
    # HDF/grid audit remains valid and makes no motion claim.
    assert opera.outcome == "unsupported"
    assert opera.reason_code == "unsupported:raw_provenance"
    assert opera.grid_shape == (4_400, 3_800)
    assert opera.total_pixels == 16_720_000
    assert opera.accounted_pixels is None
    assert opera.capture_member_count == opera.accounted_members == 1
    assert opera.duplicate_retry_exact

    assert knmi.outcome == "withheld"
    assert knmi.frame_count == 0
    assert knmi.grid_shape == (765, 700)
    assert "exact_raw_hdf5_golden_absent" in knmi.blockers

    assert rainviewer.outcome == "withheld"
    assert rainviewer.frame_count == 1
    assert rainviewer.total_pixels is rainviewer.accounted_pixels is None
    assert rainviewer.capture_member_count == rainviewer.accounted_members == 19
    for item in (noaa, opera, knmi, rainviewer):
        assert item.source_unchanged
        assert not item.motion_claim_generated
        assert not item.meteorological_admission_granted
        assert not item.operational_snapshot_claim_generated


def test_report_is_canonical_checksummed_and_admission_closed(
    tmp_path: Path,
    houston_two_frame_result,
) -> None:
    knmi = probe_knmi_authority_golden(
        ROOT / "tests" / "fixtures" / "knmi_rtcor_analysis_grid_authority.json"
    )
    report = build_report((houston_two_frame_result,), (knmi,))
    path = write_report(report, tmp_path / "report.json")
    document = json.loads(path.read_bytes())

    assert _canonical_json_bytes(document) == path.read_bytes()
    assert document["report_sha256"] == report.report_sha256
    assert document["meteorological_admission_granted"] is False
    assert document["operational_snapshot_claim_generated"] is False


def test_report_rejects_source_mutation(houston_two_frame_result) -> None:
    broken = houston_two_frame_result.__class__(
        **{
            **houston_two_frame_result.to_mapping(),
            "source_unchanged": False,
        }
    )
    knmi = probe_knmi_authority_golden(
        ROOT / "tests" / "fixtures" / "knmi_rtcor_analysis_grid_authority.json"
    )
    with pytest.raises(MultiProviderReplayError, match="unclosed proof boundary"):
        build_report((broken,), (knmi,))


def test_matrix_is_inert_and_service_ha_mqtt_free() -> None:
    module = ROOT / "storm_tracker_worker" / "multi_provider_replay_matrix.py"
    script = ROOT / "scripts" / "run_multi_provider_replay_matrix.py"
    forbidden = {
        "aiohttp",
        "custom_components",
        "homeassistant",
        "paho",
        "requests",
        "socket",
        "storm_tracker_worker.service",
        "urllib",
        "websockets",
    }
    assert not (_imports(module) | _imports(script)).intersection(forbidden)
    assert "storm_tracker_worker.multi_provider_replay_matrix" not in _imports(
        ROOT / "storm_tracker_worker" / "service.py"
    )
