from __future__ import annotations

import ast
import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from storm_tracker_worker.science_shadow_dwd_roi_replay import (
    EXPECTED_SEQUENCE_MANIFEST_SHA256,
    load_dwd_roi_science_replay_export,
)
from storm_tracker_worker.science_shadow_dwd_roi_shared_translation import (
    DWD_ROI_SHARED_TRANSLATION_CONTRACT,
    DwdRoiSharedTranslationError,
    EXPECTED_FROZEN_BASELINE_FILENAME,
    EXPECTED_FROZEN_BASELINE_RECEIPT_SHA256,
    EXPECTED_FROZEN_BASELINE_SHA256,
    EXPECTED_ORDERED_EVIDENCE_RECORD_CONCAT_SHA256,
    EXPECTED_ORDERED_RUN_IDS_SHA256,
    load_frozen_dwd_roi_phase_baseline,
    render_dwd_roi_shared_translation_markdown,
    run_dwd_roi_shared_translation_replay,
    verify_dwd_roi_shared_translation_document,
)


REPOSITORY = Path(__file__).resolve().parents[1]
EXPORT_ROOT = REPOSITORY / ".analysis" / "dwd-rv-germany-roi-2km-36-export-v1"
SEQUENCE_MANIFEST = (
    EXPORT_ROOT / "manifests" / f"{EXPECTED_SEQUENCE_MANIFEST_SHA256}.json"
)
FROZEN_BASELINE = REPOSITORY / ".analysis" / EXPECTED_FROZEN_BASELINE_FILENAME
OUTPUT_JSON = (
    REPOSITORY
    / ".analysis"
    / "dwd_rv_germany_roi_2km_shared_system_translation_20260808_evidence.json"
)
OUTPUT_MARKDOWN = OUTPUT_JSON.with_suffix(".md")
EXPECTED_JSON_SHA256 = (
    "a347e44239ddab46f0e6b958722ce805058b73c5295affb851ab9e7e4b4b6beb"
)
EXPECTED_MARKDOWN_SHA256 = (
    "b6ff66c1e06162608d3099e0f1a0c99036abc64190bc1d11c15eb1c37b60c491"
)
EXPECTED_SCIENTIFIC_PAYLOAD_SHA256 = (
    "a2f18b1a7f65ecbff8868de31e26f381131ad492c337cf0d8211aed9ca41ec7f"
)


def _require_inputs() -> None:
    missing = [
        path for path in (
            SEQUENCE_MANIFEST,
            FROZEN_BASELINE,
            OUTPUT_JSON,
            OUTPUT_MARKDOWN,
        )
        if not path.is_file()
    ]
    if missing:
        pytest.skip(f"sealed DWD shared-translation evidence missing: {missing}")


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _inventory(root: Path) -> dict[str, tuple[int, str]]:
    return {
        path.relative_to(root).as_posix(): (path.stat().st_size, _sha_file(path))
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _sealed_document() -> dict[str, Any]:
    _require_inputs()
    value = json.loads(OUTPUT_JSON.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


@pytest.fixture(scope="module")
def computed_receipt():
    _require_inputs()
    before = _inventory(EXPORT_ROOT)
    baseline_before = (_sha_file(FROZEN_BASELINE), FROZEN_BASELINE.stat().st_size)
    export = load_dwd_roi_science_replay_export(
        EXPORT_ROOT.resolve(), SEQUENCE_MANIFEST.resolve()
    )
    baseline = load_frozen_dwd_roi_phase_baseline(FROZEN_BASELINE.resolve())
    receipt = run_dwd_roi_shared_translation_replay(export, baseline)
    after = _inventory(EXPORT_ROOT)
    baseline_after = (_sha_file(FROZEN_BASELINE), FROZEN_BASELINE.stat().st_size)
    return receipt, before, after, baseline_before, baseline_after


def test_full_35_interval_replay_is_read_only_and_scientifically_deterministic(
    computed_receipt,
) -> None:
    receipt, before, after, baseline_before, baseline_after = computed_receipt

    assert before == after
    assert baseline_before == baseline_after
    assert receipt.scientific_payload_sha256 == EXPECTED_SCIENTIFIC_PAYLOAD_SHA256
    assert receipt.document["interval_count"] == 35
    assert receipt.document["postflight_export_reverified"] is True
    assert receipt.document["admission_granted"] is False
    assert receipt.document["meteorological_accuracy_claimed"] is False
    assert receipt.document["tracker_tuning_performed"] is False
    assert receipt.document["service_live_mqtt_ha_wiring_changed"] is False


def test_all_shared_results_are_complete_unique_24_kmh_eastward_shifts(
    computed_receipt,
) -> None:
    document = computed_receipt[0].document
    intervals = document["intervals"]
    assert len(intervals) == 35
    assert all(item["interval_number"] == index for index, item in enumerate(intervals))
    assert all(item["previous_frame_number"] + 1 == item["current_frame_number"] for item in intervals)
    assert all(item["elapsed_seconds"] == 300.0 for item in intervals)
    assert all(item["translation"]["status"] == "resolved_unique_peak" for item in intervals)
    assert all(item["translation"]["peak_tie_count"] == 1 for item in intervals)
    assert all(item["translation"]["uncertainty_reason_codes"] == [] for item in intervals)
    selected = [item["translation"]["selected_candidate"] for item in intervals]
    assert all(item["row_shift_pixels"] == 0 for item in selected)
    assert all(item["column_shift_pixels"] == 1 for item in selected)
    assert all(item["interval_speed_kmh"] == 24.0 for item in selected)
    assert min(item["overlap_fraction_smaller_support"] for item in selected) == pytest.approx(
        0.6133333333333333
    )
    assert max(item["overlap_fraction_smaller_support"] for item in selected) == pytest.approx(
        0.9173913043478261
    )


def test_accounting_and_measured_capacity_are_exact(computed_receipt) -> None:
    document = computed_receipt[0].document
    translations = [item["translation"] for item in document["intervals"]]
    accountings = [item["accounting"] for item in translations]

    assert all(item["search_complete"] is True for item in accountings)
    assert all(
        item["pair_contributions_expected"]
        == item["pair_contributions_evaluated"]
        == item["overlap_histogram_mass"]
        for item in accountings
    )
    assert max(item["pair_contributions_expected"] for item in accountings) == 56_640
    assert max(item["nonzero_shift_candidates_discovered"] for item in accountings) == 7_553
    assert {item["full_shift_candidates"] for item in accountings} == {593_451}
    assert sum(item["pair_contributions_expected"] for item in accountings) == 1_146_804
    assert sum(item["shift_candidates_evaluated"] for item in accountings) == 20_770_785
    assert document["summary"]["all_pair_accounting_exact"] is True
    assert document["summary"]["all_assignment_inputs_used_false"] is True
    assert document["summary"]["all_speed_or_distance_gate_applied_false"] is True


def test_frozen_reference_comparison_is_shift_only_and_not_tuning(
    computed_receipt,
) -> None:
    document = computed_receipt[0].document
    intervals = document["intervals"]
    comparisons = [item["baseline_comparison"] for item in intervals]

    assert document["reference"]["artifact_sha256"] == EXPECTED_FROZEN_BASELINE_SHA256
    assert document["reference"]["baseline_receipt_sha256"] == EXPECTED_FROZEN_BASELINE_RECEIPT_SHA256
    assert document["reference"]["comparison_used_for_tuning"] is False
    assert document["reference"]["reference_is_truth"] is False
    assert all(item["shift_vector_semantics_comparable"] is True for item in comparisons)
    assert all(item["score_semantics_comparable"] is False for item in comparisons)
    assert all(item["overlap_semantics_comparable"] is False for item in comparisons)
    assert all("baseline_best_score" not in item for item in comparisons)
    assert all("baseline_overlap_fraction" not in key for item in comparisons for key in item)
    assert sum(item["same_integer_shift"] for item in comparisons) == 31
    differing = [
        item["previous_frame_number"]
        for item in intervals
        if not item["baseline_comparison"]["same_integer_shift"]
    ]
    assert differing == [29, 30, 33, 34]
    assert document["summary"]["baseline_shift_mode"] == {
        "count": 31,
        "shift_column_pixels": 1,
        "shift_row_pixels": 0,
    }
    assert document["summary"]["shared_shift_mode"] == {
        "count": 35,
        "shift_column_pixels": 1,
        "shift_row_pixels": 0,
    }


def test_every_frame_is_bound_to_sealed_lineage_and_baseline_support(
    computed_receipt,
) -> None:
    document = computed_receipt[0].document
    inventory = document["frame_inventory"]
    assert len(inventory) == 36
    assert [item["frame_number"] for item in inventory] == list(range(36))
    assert min(item["rain_shield_support_pixels"] for item in inventory) == 70
    assert max(item["rain_shield_support_pixels"] for item in inventory) == 240
    assert all(
        item["rain_shield_pixel_references"]
        == item["rain_shield_support_pixels"]
        for item in inventory
    )
    assert document["input"]["ordered_evidence_record_concat_sha256"] == (
        EXPECTED_ORDERED_EVIDENCE_RECORD_CONCAT_SHA256
    )
    assert document["input"]["ordered_run_ids_sha256"] == EXPECTED_ORDERED_RUN_IDS_SHA256
    for index, interval in enumerate(document["intervals"]):
        assert interval["previous_run_id"] == inventory[index]["canonical_run_id"]
        assert interval["current_run_id"] == inventory[index + 1]["canonical_run_id"]
        assert interval["previous_binary_frame_sha256"] == inventory[index]["frame_sha256"]
        assert interval["current_binary_frame_sha256"] == inventory[index + 1]["frame_sha256"]


def test_sealed_artifacts_have_exact_hashes_and_valid_canonical_contract() -> None:
    document = _sealed_document()
    assert _sha_file(OUTPUT_JSON) == EXPECTED_JSON_SHA256
    assert _sha_file(OUTPUT_MARKDOWN) == EXPECTED_MARKDOWN_SHA256
    assert verify_dwd_roi_shared_translation_document(document) == (
        EXPECTED_SCIENTIFIC_PAYLOAD_SHA256
    )
    assert document["contract"] == DWD_ROI_SHARED_TRANSLATION_CONTRACT
    assert len(OUTPUT_JSON.read_bytes()) < 2 * 1024 * 1024
    assert len(OUTPUT_MARKDOWN.read_bytes()) < 2 * 1024 * 1024
    json.dumps(document, allow_nan=False)
    expected_markdown = render_dwd_roi_shared_translation_markdown(
        document, json_sha256=EXPECTED_JSON_SHA256
    )
    assert expected_markdown == OUTPUT_MARKDOWN.read_bytes()


def test_runtime_is_reported_but_excluded_from_scientific_payload_hash() -> None:
    document = _sealed_document()
    original = verify_dwd_roi_shared_translation_document(document)
    changed = copy.deepcopy(document)
    changed["runtime_observation"]["interval_seconds"] = [99.0] * 35
    changed["runtime_observation"]["summary_seconds"] = {
        "count": 35,
        "maximum": 99.0,
        "median_p50": 99.0,
        "minimum": 99.0,
        "nearest_rank_p95": 99.0,
        "sum": 3465.0,
    }
    for item in changed["intervals"]:
        item["runtime_seconds"] = 99.0
    assert verify_dwd_roi_shared_translation_document(changed) == original
    observed = document["runtime_observation"]["summary_seconds"]
    assert observed["count"] == 35
    assert 0.0 < observed["minimum"] <= observed["maximum"]
    assert observed["sum"] > 0.0
    assert document["runtime_observation"][
        "single_host_observation_not_deterministic_science"
    ] is True


def test_tamper_extra_fields_and_nondefinitive_baseline_fail_closed(
    tmp_path: Path,
) -> None:
    document = _sealed_document()
    changed = copy.deepcopy(document)
    changed["intervals"][0]["translation"]["selected_candidate"][
        "column_shift_pixels"
    ] = 2
    with pytest.raises(DwdRoiSharedTranslationError, match="payload hash"):
        verify_dwd_roi_shared_translation_document(changed)

    extra = copy.deepcopy(document)
    extra["unexpected"] = False
    with pytest.raises(DwdRoiSharedTranslationError, match="contract differs"):
        verify_dwd_roi_shared_translation_document(extra)

    with pytest.raises(DwdRoiSharedTranslationError, match="not definitive"):
        load_frozen_dwd_roi_phase_baseline(Path(EXPECTED_FROZEN_BASELINE_FILENAME))

    tampered = tmp_path / EXPECTED_FROZEN_BASELINE_FILENAME
    body = bytearray(FROZEN_BASELINE.read_bytes())
    body[-1] ^= 1
    tampered.write_bytes(body)
    with pytest.raises(DwdRoiSharedTranslationError, match="bytes differ"):
        load_frozen_dwd_roi_phase_baseline(tampered.resolve())


def test_module_and_runner_remain_unwired_from_live_runtime() -> None:
    module_path = (
        REPOSITORY
        / "storm_tracker_worker"
        / "science_shadow_dwd_roi_shared_translation.py"
    )
    source = module_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert not any(
        token in name.lower()
        for name in imported
        for token in (
            "custom_components",
            "mqtt",
            "service",
            "cell_track_engine",
            "association_features",
            "multiprocessing",
            "requests",
            "socket",
            "urllib",
        )
    )
    token = "science_shadow_dwd_roi_shared_translation"
    live_files = [
        REPOSITORY / "storm_tracker_worker" / "service.py",
        REPOSITORY / "storm_tracker_worker" / "tracking_core" / "engine" / "cell_track_engine.py",
        REPOSITORY / "storm_tracker_worker" / "tracking_core" / "engine" / "association_features.py",
    ]
    live_files.extend(
        (REPOSITORY / "custom_components" / "storm_tracker_v3").rglob("*.py")
    )
    assert all(
        token not in path.read_text(encoding="utf-8") for path in live_files
    )
