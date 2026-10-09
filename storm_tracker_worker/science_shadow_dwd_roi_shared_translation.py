"""Read-only shared rain-shield translation replay for the sealed DWD ROI.

This module composes two existing shadow authorities without changing either:
the closed 36-frame DWD Germany ROI export loader and the pure assignment-
independent binary rain-shield translation estimator.  The pre-existing masked
phase-correlation receipt is opened only as a frozen evaluation reference.

Nothing in this module grants admission, publishes a snapshot, or wires into
CellTrack, WeatherSystem, the worker service, MQTT, or Home Assistant.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import time
from typing import Any, Callable, Mapping

from .hard_decoder_isolation import _canonical_json_bytes
from .science_shadow_dwd_roi_replay import (
    DwdRoiScienceReplayExport,
    EXPECTED_ANALYSIS_PIXELS,
    EXPECTED_DETECTOR_CONFIG_SHA256,
    EXPECTED_FRAME_COUNT,
    EXPECTED_METRICS_MANIFEST_SHA256,
    EXPECTED_SEQUENCE_ID,
    EXPECTED_SEQUENCE_MANIFEST_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    EXPECTED_TARGET_GRID_SHA256,
    load_dwd_roi_science_evidence,
    load_dwd_roi_science_replay_export,
)
from .tracking_core.engine.shared_system_translation import (
    BinaryRainShieldFootprintFrame,
    SharedSystemTranslationConfig,
    SharedSystemTranslationResult,
    estimate_shared_system_translation,
)


DWD_ROI_SHARED_TRANSLATION_CONTRACT = (
    "stv3-dwd-roi-shared-system-translation-replay-v1"
)
DWD_ROI_SHARED_TRANSLATION_PROFILE_ID = (
    "dwd-rv-germany-roi-2km-binary-rain-shield-translation-v1"
)
EXPECTED_FROZEN_BASELINE_FILENAME = (
    "dwd_rv_germany_roi_2km_v4_complete_v5_resource_failure_"
    "20260808_evidence.json"
)
EXPECTED_FROZEN_BASELINE_SHA256 = (
    "74b0f1678f8a1d1e5270bd1c21ec7ca773e231b0b316929bc74daac2f44332fd"
)
EXPECTED_FROZEN_BASELINE_CONTRACT = (
    "stv3-dwd-roi-v4-complete-v5-resource-failure-evidence-v1"
)
EXPECTED_FROZEN_BASELINE_RECEIPT_SHA256 = (
    "c9f2f5447856d1d494387d8cb54a6e099bcba59882ccb86bd53bab047202741a"
)
EXPECTED_FROZEN_BASELINE_CONFIG_SHA256 = (
    "68ae93f7d363835ae487df53e14de0302f1fea14cea3a989e4e9812f464d42c9"
)
EXPECTED_ORDERED_EVIDENCE_RECORD_CONCAT_SHA256 = (
    "9fac976800e063fb44da4d1344f6daec1aba4ecd53ccf0d908cf55d94eab4777"
)
EXPECTED_ORDERED_RUN_IDS_SHA256 = (
    "16f439ad52481c8086afc206ec2fda9ee9365e0fa4c27cb090632ce5b09e5ebf"
)
MAX_FROZEN_BASELINE_BYTES = 2 * 1024 * 1024
MAX_REPORT_BYTES = 2 * 1024 * 1024
MAX_MARKDOWN_BYTES = 2 * 1024 * 1024

DWD_ROI_SHARED_TRANSLATION_CONFIG = SharedSystemTranslationConfig(
    max_grid_pixels=250_000,
    max_support_pixels_per_frame=250_000,
    max_pair_contributions=2_000_000,
    max_shift_candidates=1_000_000,
    max_nonzero_shift_candidates=100_000,
    low_overlap_uncertainty_fraction=0.25,
)

DWD_ROI_SHARED_TRANSLATION_CAVEATS = (
    "binary_rain_shield_support_is_not_independent_motion_truth",
    "masked_phase_correlation_is_an_evaluation_reference_not_truth",
    "comparison_denominators_and_scores_are_not_interchangeable",
    "two_kilometre_area_aggregation_changes_one_kilometre_topology",
    "missing_nodata_undetect_and_mixed_pixels_are_not_called_dry",
    "integer_grid_shift_has_no_subpixel_motion_information",
    "low_overlap_threshold_is_untuned_and_not_admitted",
    "runtime_is_one_host_observation_excluded_from_scientific_payload_hash",
)
DWD_ROI_SHARED_TRANSLATION_BLOCKERS = (
    "independent_identity_truth_unavailable",
    "independent_speed_truth_unavailable",
    "shared_translation_uncertainty_adapter_unavailable",
    "held_out_uncertainty_calibration_unavailable",
    "dense_event_pair_capacity_not_validated",
    "operational_admission_certificates_incomplete",
    "live_service_mqtt_ha_wiring_forbidden",
)


class DwdRoiSharedTranslationError(RuntimeError):
    """Fail-closed replay, lineage, capacity, or sealing error."""


def _sha_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _materialize(value: Any) -> Any:
    """Return one alias-free JSON tree accepted by the hard canonicalizer."""

    return json.loads(json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ))


def _sha_mapping(value: Mapping[str, Any]) -> str:
    return _sha_bytes(_canonical_json_bytes(_materialize(value)))


def _round(value: float) -> float:
    if not math.isfinite(float(value)):
        raise DwdRoiSharedTranslationError("non-finite replay metric")
    return float(format(float(value), ".12g"))


def _nearest_rank(values: list[float], fraction: float) -> float:
    if not values:
        raise DwdRoiSharedTranslationError("empty metric distribution")
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def _distribution(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "maximum": None,
            "median_p50": None,
            "minimum": None,
            "nearest_rank_p95": None,
            "sum": 0.0,
        }
    return {
        "count": len(values),
        "maximum": _round(max(values)),
        "median_p50": _round(_nearest_rank(values, 0.50)),
        "minimum": _round(min(values)),
        "nearest_rank_p95": _round(_nearest_rank(values, 0.95)),
        "sum": _round(sum(values)),
    }


def _shift_mode(shifts: list[tuple[int, int]]) -> dict[str, int] | None:
    if not shifts:
        return None
    counts = Counter(shifts)
    (row, column), count = sorted(
        counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1])
    )[0]
    return {
        "count": count,
        "shift_column_pixels": column,
        "shift_row_pixels": row,
    }


@dataclass(frozen=True, slots=True)
class FrozenDwdRoiPhaseBaseline:
    path: Path
    artifact_sha256: str
    receipt_sha256: str
    document: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class DwdRoiSharedTranslationReceipt:
    document: Mapping[str, Any]
    scientific_payload_sha256: str

    def canonical_body(self) -> bytes:
        body = _canonical_json_bytes(_materialize(self.document))
        if len(body) > MAX_REPORT_BYTES:
            raise DwdRoiSharedTranslationError("shared translation report exceeds cap")
        return body


def load_frozen_dwd_roi_phase_baseline(
    path: str | Path,
) -> FrozenDwdRoiPhaseBaseline:
    """Open exactly the previously sealed phase-correlation evidence bytes."""

    candidate = Path(path)
    if (
        not candidate.is_absolute()
        or candidate.name != EXPECTED_FROZEN_BASELINE_FILENAME
        or candidate.is_symlink()
        or not candidate.is_file()
    ):
        raise DwdRoiSharedTranslationError("frozen baseline path is not definitive")
    candidate = candidate.resolve(strict=True)
    size = candidate.stat().st_size
    if not 1 <= size <= MAX_FROZEN_BASELINE_BYTES:
        raise DwdRoiSharedTranslationError("frozen baseline exceeds byte cap")
    body = candidate.read_bytes()
    if _sha_bytes(body) != EXPECTED_FROZEN_BASELINE_SHA256:
        raise DwdRoiSharedTranslationError("frozen baseline bytes differ")
    try:
        raw = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DwdRoiSharedTranslationError("frozen baseline JSON is invalid") from exc
    if not isinstance(raw, dict) or _canonical_json_bytes(raw) != body:
        raise DwdRoiSharedTranslationError("frozen baseline is not canonical JSON")
    baselines = raw.get("independent_baselines")
    if (
        raw.get("contract") != EXPECTED_FROZEN_BASELINE_CONTRACT
        or raw.get("admission_granted") is not False
        or raw.get("truth_available") is not False
        or raw.get("service_or_live_wiring_changed") is not False
        or not isinstance(baselines, dict)
        or baselines.get("receipt_sha256")
        != EXPECTED_FROZEN_BASELINE_RECEIPT_SHA256
    ):
        raise DwdRoiSharedTranslationError("frozen baseline authority differs")
    document = baselines.get("document")
    if not isinstance(document, dict):
        raise DwdRoiSharedTranslationError("frozen baseline document is absent")
    if (
        _sha_mapping(document) != EXPECTED_FROZEN_BASELINE_RECEIPT_SHA256
        or document.get("config_sha256")
        != EXPECTED_FROZEN_BASELINE_CONFIG_SHA256
        or document.get("sequence_id") != EXPECTED_SEQUENCE_ID
        or document.get("sequence_manifest_sha256")
        != EXPECTED_SEQUENCE_MANIFEST_SHA256
        or document.get("source_manifest_sha256")
        != EXPECTED_SOURCE_MANIFEST_SHA256
        or document.get("target_grid_sha256") != EXPECTED_TARGET_GRID_SHA256
        or document.get("interval_count") != EXPECTED_FRAME_COUNT - 1
        or document.get("truth_claimed") is not False
        or document.get("admission_granted") is not False
        or document.get("tracker_outputs_consulted") is not False
        or document.get("tracker_parameters_tuned") is not False
        or document.get("missing_values_treated_as_dry") is not False
    ):
        raise DwdRoiSharedTranslationError("frozen baseline lineage differs")
    intervals = document.get("masked_interval_translation_references")
    if not isinstance(intervals, list) or len(intervals) != EXPECTED_FRAME_COUNT - 1:
        raise DwdRoiSharedTranslationError("frozen baseline interval count differs")
    return FrozenDwdRoiPhaseBaseline(
        path=candidate,
        artifact_sha256=EXPECTED_FROZEN_BASELINE_SHA256,
        receipt_sha256=EXPECTED_FROZEN_BASELINE_RECEIPT_SHA256,
        document=document,
    )


def _binary_frame(
    export: DwdRoiScienceReplayExport,
    index: int,
) -> tuple[BinaryRainShieldFootprintFrame, dict[str, Any]]:
    entry = export.entries[index]
    evidence = load_dwd_roi_science_evidence(export, entry)
    detected = evidence.detection_frame
    shields = tuple(detected.rain_shields)
    footprints = tuple(tuple(item.footprint_pixels) for item in shields)
    frame = BinaryRainShieldFootprintFrame(
        frame_id=detected.frame_id,
        valid_at=detected.valid_at,
        grid=evidence.normalized_frame.grid,
        rain_shield_footprints=footprints,
    )
    references = sum(len(item) for item in footprints)
    support = len(frame.support_pixels)
    accounting = evidence.pixel_accounting
    if (
        detected.analysis_grid_id != frame.grid.grid_id
        or detected.quantity != "interval_mean_precipitation_rate"
        or detected.unit != "mm/h"
        or entry.valid_at != frame.valid_at
        or entry.normalized_frame_id != detected.frame_id
        or evidence.target_grid_sha256 != EXPECTED_TARGET_GRID_SHA256
        or accounting.total_analysis_pixels != EXPECTED_ANALYSIS_PIXELS
        or accounting.rain_shield_objects != len(shields)
        or accounting.rain_shield_pixels != references
        or references != support
    ):
        raise DwdRoiSharedTranslationError(
            "DWD rain-shield footprint accounting differs"
        )
    return frame, {
        "evidence_candidate_id": entry.evidence_candidate_id,
        "evidence_content_sha256": entry.evidence_content_sha256,
        "evidence_record_bytes": entry.evidence_record_bytes,
        "evidence_record_sha256": entry.evidence_record_sha256,
        "canonical_run_id": entry.canonical_run_id,
        "frame_id": frame.frame_id,
        "frame_number": index,
        "frame_sha256": frame.fingerprint,
        "rain_shield_objects": len(shields),
        "rain_shield_pixel_references": references,
        "rain_shield_support_pixels": support,
        "valid_at": frame.valid_at,
    }


def _baseline_by_interval(
    baseline: FrozenDwdRoiPhaseBaseline,
    export: DwdRoiScienceReplayExport,
) -> dict[tuple[int, int], Mapping[str, Any]]:
    raw = baseline.document["masked_interval_translation_references"]
    result: dict[tuple[int, int], Mapping[str, Any]] = {}
    for expected_current, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise DwdRoiSharedTranslationError("baseline interval is invalid")
        previous_number = item.get("previous_frame_number")
        current_number = item.get("current_frame_number")
        key = (previous_number, current_number)
        previous = export.entries[expected_current - 1]
        current = export.entries[expected_current]
        translation = item.get("translation")
        if (
            key != (expected_current - 1, expected_current)
            or item.get("previous_run_id") != previous.canonical_run_id
            or item.get("current_run_id") != current.canonical_run_id
            or item.get("previous_evidence_content_sha256")
            != previous.evidence_content_sha256
            or item.get("current_evidence_content_sha256")
            != current.evidence_content_sha256
            or float(item.get("elapsed_seconds", -1.0)) != 300.0
            or not isinstance(translation, dict)
            or translation.get("status") != "resolved_unique_peak"
        ):
            raise DwdRoiSharedTranslationError("baseline interval lineage differs")
        result[key] = item
    if len(result) != EXPECTED_FRAME_COUNT - 1:
        raise DwdRoiSharedTranslationError("baseline interval identity is duplicated")
    return result


def _compare_with_baseline(
    result: SharedSystemTranslationResult,
    baseline_item: Mapping[str, Any],
) -> dict[str, Any]:
    selected = result.selected_candidate
    if selected is None:
        raise DwdRoiSharedTranslationError("shared translation omitted its point shift")
    reference = baseline_item["translation"]
    baseline_row = int(reference["shift_row_pixels"])
    baseline_column = int(reference["shift_column_pixels"])
    row_difference = selected.row_shift_pixels - baseline_row
    column_difference = selected.column_shift_pixels - baseline_column
    return {
        "accuracy_claimed": False,
        "baseline_shift_column_pixels": baseline_column,
        "baseline_shift_row_pixels": baseline_row,
        "baseline_speed_km_h": reference["speed_km_h"],
        "baseline_status": reference["status"],
        "baseline_tied_best_shifts": reference["tied_best_shifts"],
        "column_shift_difference_pixels": column_difference,
        "comparison_used_for_tuning": False,
        "both_have_point_shift": True,
        "euclidean_shift_difference_pixels": _round(math.hypot(
            row_difference, column_difference
        )),
        "identity_truth_available": False,
        "overlap_semantics_comparable": False,
        "row_shift_difference_pixels": row_difference,
        "same_integer_shift": (
            row_difference == 0 and column_difference == 0
        ),
        "score_semantics_comparable": False,
        "shift_vector_semantics_comparable": True,
        "speed_difference_km_h": _round(
            selected.interval_speed_kmh - float(reference["speed_km_h"])
        ),
        "vector_difference_m": _round(math.hypot(
            row_difference * selected.y_resolution_m,
            column_difference * selected.x_resolution_m,
        )),
    }


def _scientific_projection(document: Mapping[str, Any]) -> dict[str, Any]:
    projection = _materialize(document)
    projection.pop("runtime_observation", None)
    for interval in projection["intervals"]:
        interval.pop("runtime_seconds", None)
    projection.pop("scientific_payload_sha256", None)
    return projection


def run_dwd_roi_shared_translation_replay(
    export: DwdRoiScienceReplayExport,
    baseline: FrozenDwdRoiPhaseBaseline,
    *,
    config: SharedSystemTranslationConfig = DWD_ROI_SHARED_TRANSLATION_CONFIG,
    monotonic: Callable[[], float] = time.perf_counter,
) -> DwdRoiSharedTranslationReceipt:
    """Evaluate the 35 adjacent sealed intervals without modifying input."""

    if not isinstance(export, DwdRoiScienceReplayExport):
        raise TypeError("export must be DwdRoiScienceReplayExport")
    if not isinstance(baseline, FrozenDwdRoiPhaseBaseline):
        raise TypeError("baseline must be FrozenDwdRoiPhaseBaseline")
    if not isinstance(config, SharedSystemTranslationConfig):
        raise TypeError("config must be SharedSystemTranslationConfig")
    if not callable(monotonic):
        raise TypeError("monotonic must be callable")
    if (
        export.sequence_manifest_sha256 != EXPECTED_SEQUENCE_MANIFEST_SHA256
        or export.sequence_id != EXPECTED_SEQUENCE_ID
        or export.source_manifest_sha256 != EXPECTED_SOURCE_MANIFEST_SHA256
        or export.metrics_manifest_sha256 != EXPECTED_METRICS_MANIFEST_SHA256
        or export.target_grid_sha256 != EXPECTED_TARGET_GRID_SHA256
        or len(export.entries) != EXPECTED_FRAME_COUNT
    ):
        raise DwdRoiSharedTranslationError("DWD export authority differs")
    evidence_record_concat_sha256 = _sha_bytes("".join(
        item.evidence_record_sha256 for item in export.entries
    ).encode("ascii"))
    ordered_run_ids_sha256 = _sha_bytes(_canonical_json_bytes([
        item.canonical_run_id for item in export.entries
    ]))
    if (
        evidence_record_concat_sha256
        != EXPECTED_ORDERED_EVIDENCE_RECORD_CONCAT_SHA256
        or ordered_run_ids_sha256 != EXPECTED_ORDERED_RUN_IDS_SHA256
    ):
        raise DwdRoiSharedTranslationError("ordered DWD evidence seal differs")
    baseline_intervals = _baseline_by_interval(baseline, export)

    frames: list[BinaryRainShieldFootprintFrame] = []
    frame_inventory: list[dict[str, Any]] = []
    for index in range(EXPECTED_FRAME_COUNT):
        frame, inventory = _binary_frame(export, index)
        frames.append(frame)
        frame_inventory.append(inventory)
    baseline_frames = baseline.document.get("frame_accounting")
    if not isinstance(baseline_frames, list) or len(baseline_frames) != EXPECTED_FRAME_COUNT:
        raise DwdRoiSharedTranslationError("baseline frame accounting differs")
    for index, (inventory, baseline_frame) in enumerate(
        zip(frame_inventory, baseline_frames)
    ):
        entry = export.entries[index]
        if (
            not isinstance(baseline_frame, dict)
            or baseline_frame.get("frame_number") != index
            or baseline_frame.get("run_id") != entry.canonical_run_id
            or baseline_frame.get("evidence_content_sha256")
            != entry.evidence_content_sha256
            or float(baseline_frame.get("valid_at", -1.0)) != entry.valid_at
            or baseline_frame.get("support_pixels_ge_1_mm_h")
            != inventory["rain_shield_support_pixels"]
        ):
            raise DwdRoiSharedTranslationError(
                "baseline and detector shield support differ"
            )

    intervals: list[dict[str, Any]] = []
    runtime_values: list[float] = []
    for current_index in range(1, EXPECTED_FRAME_COUNT):
        previous = frames[current_index - 1]
        current = frames[current_index]
        started = float(monotonic())
        result = estimate_shared_system_translation(previous, current, config)
        finished = float(monotonic())
        runtime = finished - started
        if not math.isfinite(runtime) or runtime < 0.0:
            raise DwdRoiSharedTranslationError("runtime clock moved backwards")
        accounting = result.accounting
        selected = result.selected_candidate
        if (
            result.status == "indeterminate"
            or selected is None
            or not accounting.search_complete
            or accounting.grid_pixels != EXPECTED_ANALYSIS_PIXELS
            or accounting.pair_contributions_evaluated
            != accounting.pair_contributions_expected
            or accounting.overlap_histogram_mass
            != accounting.pair_contributions_expected
            or accounting.shift_candidates_evaluated
            != accounting.full_shift_candidates
            or result.meteorological_admission_granted
            or result.operational_snapshot_claim_generated
            or result.assignment_inputs_used
            or result.speed_or_distance_gate_applied
        ):
            raise DwdRoiSharedTranslationError(
                "shared translation interval failed closed accounting"
            )
        baseline_item = baseline_intervals[(current_index - 1, current_index)]
        intervals.append({
            "baseline_comparison": _compare_with_baseline(result, baseline_item),
            "current_binary_frame_sha256": current.fingerprint,
            "current_evidence_content_sha256": export.entries[
                current_index
            ].evidence_content_sha256,
            "current_evidence_record_sha256": export.entries[
                current_index
            ].evidence_record_sha256,
            "current_frame_number": current_index,
            "current_run_id": export.entries[current_index].canonical_run_id,
            "elapsed_seconds": current.valid_at - previous.valid_at,
            "interval_number": current_index - 1,
            "previous_binary_frame_sha256": previous.fingerprint,
            "previous_evidence_content_sha256": export.entries[
                current_index - 1
            ].evidence_content_sha256,
            "previous_evidence_record_sha256": export.entries[
                current_index - 1
            ].evidence_record_sha256,
            "previous_frame_number": current_index - 1,
            "previous_run_id": export.entries[
                current_index - 1
            ].canonical_run_id,
            "runtime_seconds": _round(runtime),
            "translation": result.to_mapping(),
        })
        runtime_values.append(runtime)

    # Reopen and rehash the complete closed export after the calculation.  A
    # concurrent object, manifest, or lineage change therefore cannot be
    # silently sealed as if it belonged to the original preflight.
    postflight = load_dwd_roi_science_replay_export(
        export.export_root, export.sequence_manifest_path
    )
    if postflight != export:
        raise DwdRoiSharedTranslationError("DWD export changed during replay")

    translations = [item["translation"] for item in intervals]
    selected = [item["selected_candidate"] for item in translations]
    shared_shifts = [
        (item["row_shift_pixels"], item["column_shift_pixels"])
        for item in selected
    ]
    baseline_shifts = [
        (
            item["baseline_comparison"]["baseline_shift_row_pixels"],
            item["baseline_comparison"]["baseline_shift_column_pixels"],
        )
        for item in intervals
    ]
    speeds = [float(item["interval_speed_kmh"]) for item in selected]
    overlaps = [
        float(item["overlap_fraction_smaller_support"]) for item in selected
    ]
    differences = [
        float(item["baseline_comparison"]["euclidean_shift_difference_pixels"])
        for item in intervals
    ]
    vector_differences_m = [
        float(item["baseline_comparison"]["vector_difference_m"])
        for item in intervals
    ]
    uncertainty_counts = Counter(
        reason
        for item in translations
        for reason in item["uncertainty_reason_codes"]
    )
    status_counts = Counter(item["status"] for item in translations)
    accounting_totals = {
        key: sum(item["accounting"][key] for item in translations)
        for key in (
            "pair_contributions_expected",
            "pair_contributions_evaluated",
            "overlap_histogram_mass",
            "shift_candidates_evaluated",
        )
    }
    document: dict[str, Any] = {
        "admission_granted": False,
        "assignment_inputs_used": False,
        "blockers": list(DWD_ROI_SHARED_TRANSLATION_BLOCKERS),
        "caveats": list(DWD_ROI_SHARED_TRANSLATION_CAVEATS),
        "contract": DWD_ROI_SHARED_TRANSLATION_CONTRACT,
        "forecast_leads_used": False,
        "frame_inventory": frame_inventory,
        "input": {
            "frame_count": EXPECTED_FRAME_COUNT,
            "detector_config_sha256": EXPECTED_DETECTOR_CONFIG_SHA256,
            "metrics_manifest_sha256": EXPECTED_METRICS_MANIFEST_SHA256,
            "ordered_evidence_record_concat_sha256": (
                evidence_record_concat_sha256
            ),
            "ordered_run_ids_sha256": ordered_run_ids_sha256,
            "sequence_id": EXPECTED_SEQUENCE_ID,
            "sequence_manifest_sha256": EXPECTED_SEQUENCE_MANIFEST_SHA256,
            "source_manifest_sha256": EXPECTED_SOURCE_MANIFEST_SHA256,
            "target_grid_sha256": EXPECTED_TARGET_GRID_SHA256,
        },
        "interval_count": len(intervals),
        "intervals": intervals,
        "meteorological_accuracy_claimed": False,
        "missing_values_treated_as_dry": False,
        "operational_snapshot_generated": False,
        "postflight_export_reverified": True,
        "profile": {
            "profile_id": DWD_ROI_SHARED_TRANSLATION_PROFILE_ID,
            "translation_config": config.to_mapping(),
            "translation_config_sha256": config.fingerprint,
        },
        "reference": {
            "artifact_sha256": baseline.artifact_sha256,
            "baseline_config_sha256": EXPECTED_FROZEN_BASELINE_CONFIG_SHA256,
            "baseline_receipt_sha256": baseline.receipt_sha256,
            "comparison_used_for_tuning": False,
            "reference_is_truth": False,
        },
        "runtime_observation": {
            "clock": "python_time_perf_counter_parent_process",
            "hard_process_isolation_used": False,
            "interval_seconds": [_round(value) for value in runtime_values],
            "single_host_observation_not_deterministic_science": True,
            "summary_seconds": _distribution(runtime_values),
        },
        "service_live_mqtt_ha_wiring_changed": False,
        "summary": {
            "accounting_totals": accounting_totals,
            "all_accounting_complete": all(
                item["accounting"]["search_complete"] for item in translations
            ),
            "all_assignment_inputs_used_false": all(
                item["assignment_inputs_used"] is False for item in translations
            ),
            "all_pair_accounting_exact": all(
                item["accounting"]["pair_contributions_evaluated"]
                == item["accounting"]["pair_contributions_expected"]
                == item["accounting"]["overlap_histogram_mass"]
                for item in translations
            ),
            "all_speed_or_distance_gate_applied_false": all(
                item["speed_or_distance_gate_applied"] is False
                for item in translations
            ),
            "baseline_shift_mode": _shift_mode(baseline_shifts),
            "baseline_speed_km_h": _distribution([
                float(item["baseline_comparison"]["baseline_speed_km_h"])
                for item in intervals
            ]),
            "exact_same_integer_shift_count": sum(
                item["baseline_comparison"]["same_integer_shift"]
                for item in intervals
            ),
            "different_integer_shift_count": sum(
                not item["baseline_comparison"]["same_integer_shift"]
                for item in intervals
            ),
            "integer_shift_difference_pixels": _distribution(differences),
            "shared_overlap_fraction_smaller_support": _distribution(overlaps),
            "shared_shift_mode": _shift_mode(shared_shifts),
            "shared_speed_km_h": _distribution(speeds),
            "speed_over_200_km_h_count": sum(value > 200.0 for value in speeds),
            "shared_boundary_peak_count": uncertainty_counts.get(
                "peak_on_full_search_boundary", 0
            ),
            "shared_estimated_with_uncertainty_count": status_counts.get(
                "estimated_with_uncertainty", 0
            ),
            "shared_indeterminate_count": status_counts.get("indeterminate", 0),
            "shared_low_overlap_count": uncertainty_counts.get(
                "low_overlap_support", 0
            ),
            "shared_nonunique_peak_count": uncertainty_counts.get(
                "peak_not_unique", 0
            ),
            "shared_resolved_unique_peak_count": status_counts.get(
                "resolved_unique_peak", 0
            ),
            "status_counts": dict(sorted(status_counts.items())),
            "uncertainty_reason_counts": dict(sorted(uncertainty_counts.items())),
            "vector_difference_m": _distribution(vector_differences_m),
        },
        "tracker_tuning_performed": False,
        "truth_available": False,
        "weather_system_rebinding_performed": False,
    }
    projection = _scientific_projection(document)
    scientific_sha = _sha_mapping(projection)
    document["scientific_payload_sha256"] = scientific_sha
    document = _materialize(document)
    receipt = DwdRoiSharedTranslationReceipt(
        document=document,
        scientific_payload_sha256=scientific_sha,
    )
    receipt.canonical_body()
    return receipt


def verify_dwd_roi_shared_translation_document(
    document: Mapping[str, Any],
) -> str:
    """Validate a sealed report and return its scientific payload SHA."""

    if not isinstance(document, Mapping):
        raise TypeError("document must be a mapping")
    material = _materialize(document)
    expected_top_keys = {
        "admission_granted", "assignment_inputs_used", "blockers", "caveats",
        "contract", "forecast_leads_used", "frame_inventory", "input",
        "interval_count", "intervals", "meteorological_accuracy_claimed",
        "missing_values_treated_as_dry", "operational_snapshot_generated",
        "postflight_export_reverified", "profile", "reference",
        "runtime_observation", "scientific_payload_sha256",
        "service_live_mqtt_ha_wiring_changed", "summary",
        "tracker_tuning_performed", "truth_available",
        "weather_system_rebinding_performed",
    }
    expected_interval_keys = {
        "baseline_comparison", "current_binary_frame_sha256",
        "current_evidence_content_sha256", "current_evidence_record_sha256",
        "current_frame_number", "current_run_id", "elapsed_seconds",
        "interval_number", "previous_binary_frame_sha256",
        "previous_evidence_content_sha256", "previous_evidence_record_sha256",
        "previous_frame_number", "previous_run_id", "runtime_seconds",
        "translation",
    }
    if (
        set(material) != expected_top_keys
        or material.get("contract") != DWD_ROI_SHARED_TRANSLATION_CONTRACT
        or material.get("admission_granted") is not False
        or material.get("meteorological_accuracy_claimed") is not False
        or material.get("truth_available") is not False
        or material.get("operational_snapshot_generated") is not False
        or material.get("service_live_mqtt_ha_wiring_changed") is not False
        or material.get("tracker_tuning_performed") is not False
        or material.get("weather_system_rebinding_performed") is not False
        or material.get("interval_count") != EXPECTED_FRAME_COUNT - 1
        or len(material.get("intervals", ())) != EXPECTED_FRAME_COUNT - 1
        or len(material.get("frame_inventory", ())) != EXPECTED_FRAME_COUNT
        or any(
            not isinstance(item, dict) or set(item) != expected_interval_keys
            for item in material.get("intervals", ())
        )
    ):
        raise DwdRoiSharedTranslationError("sealed shared translation contract differs")
    expected = _sha_mapping(_scientific_projection(material))
    if material.get("scientific_payload_sha256") != expected:
        raise DwdRoiSharedTranslationError("sealed scientific payload hash differs")
    return expected


def render_dwd_roi_shared_translation_markdown(
    document: Mapping[str, Any],
    *,
    json_sha256: str,
) -> bytes:
    """Render a deterministic human-readable companion to one sealed JSON."""

    scientific_sha = verify_dwd_roi_shared_translation_document(document)
    if (
        not isinstance(json_sha256, str)
        or len(json_sha256) != 64
        or any(character not in "0123456789abcdef" for character in json_sha256)
    ):
        raise DwdRoiSharedTranslationError("JSON artifact SHA is invalid")
    summary = document["summary"]
    runtime = document["runtime_observation"]["summary_seconds"]
    lines = [
        "# DWD Germany ROI: assignment-independent shared translation",
        "",
        "## Outcome",
        "",
        f"- Intervals: {document['interval_count']}/35 sealed adjacent +000 observations.",
        f"- Statuses: `{json.dumps(summary['status_counts'], sort_keys=True)}`.",
        f"- Shared shift mode: `{summary['shared_shift_mode']}`.",
        f"- Frozen phase-reference shift mode: `{summary['baseline_shift_mode']}`.",
        f"- Exact same integer shift: {summary['exact_same_integer_shift_count']}/35.",
        f"- Shared speed distribution km/h: `{summary['shared_speed_km_h']}`.",
        f"- Runtime seconds: `{runtime}`.",
        "- No speed/distance gate, assignment input, WeatherSystem rebinding, or live wiring was used.",
        "",
        "## Per interval",
        "",
        "| Frames | Status | Shared shift (row,col) | km/h | overlap | ties | uncertainty | Phase shift (row,col) | same | runtime s |",
        "|---:|---|---:|---:|---:|---:|---|---:|:---:|---:|",
    ]
    for item in document["intervals"]:
        translation = item["translation"]
        selected = translation["selected_candidate"]
        comparison = item["baseline_comparison"]
        lines.append(
            "| {previous}->{current} | {status} | ({row},{column}) | "
            "{speed:.6g} | {overlap:.6g} | {ties} | {uncertainty} | "
            "({baseline_row},{baseline_column}) | {same} | {runtime:.6g} |".format(
                previous=item["previous_frame_number"],
                current=item["current_frame_number"],
                status=translation["status"],
                row=selected["row_shift_pixels"],
                column=selected["column_shift_pixels"],
                speed=selected["interval_speed_kmh"],
                overlap=selected["overlap_fraction_smaller_support"],
                ties=translation["peak_tie_count"],
                uncertainty=",".join(translation["uncertainty_reason_codes"]) or "none",
                baseline_row=comparison["baseline_shift_row_pixels"],
                baseline_column=comparison["baseline_shift_column_pixels"],
                same="yes" if comparison["same_integer_shift"] else "no",
                runtime=item["runtime_seconds"],
            )
        )
    lines.extend([
        "",
        "## Interpretation limits",
        "",
        "The binary shield result and masked physical-rate phase reference have different support and score denominators. Their side-by-side shifts are robustness evidence, not accuracy, identity, or speed truth. Runtime is a single parent-process host observation and is excluded from the deterministic scientific payload hash.",
        "",
        "No missing/nodata/undetect/mixed pixel was reclassified as dry. Admission remains false and this evidence is not imported by CellTrack, service, MQTT, or Home Assistant.",
        "",
        "## Artifact hashes",
        "",
        f"- JSON SHA-256: `{json_sha256}`",
        f"- Deterministic scientific payload SHA-256: `{scientific_sha}`",
        f"- Frozen phase baseline artifact SHA-256: `{EXPECTED_FROZEN_BASELINE_SHA256}`",
        "",
        "## Open blockers",
        "",
        *[f"- `{item}`" for item in document["blockers"]],
        "",
    ])
    body = "\n".join(lines).encode("utf-8")
    if len(body) > MAX_MARKDOWN_BYTES:
        raise DwdRoiSharedTranslationError("shared translation markdown exceeds cap")
    return body


__all__ = [
    "DWD_ROI_SHARED_TRANSLATION_BLOCKERS",
    "DWD_ROI_SHARED_TRANSLATION_CAVEATS",
    "DWD_ROI_SHARED_TRANSLATION_CONFIG",
    "DWD_ROI_SHARED_TRANSLATION_CONTRACT",
    "DWD_ROI_SHARED_TRANSLATION_PROFILE_ID",
    "DwdRoiSharedTranslationError",
    "DwdRoiSharedTranslationReceipt",
    "FrozenDwdRoiPhaseBaseline",
    "EXPECTED_FROZEN_BASELINE_CONFIG_SHA256",
    "EXPECTED_FROZEN_BASELINE_FILENAME",
    "EXPECTED_FROZEN_BASELINE_RECEIPT_SHA256",
    "EXPECTED_FROZEN_BASELINE_SHA256",
    "EXPECTED_ORDERED_EVIDENCE_RECORD_CONCAT_SHA256",
    "EXPECTED_ORDERED_RUN_IDS_SHA256",
    "load_frozen_dwd_roi_phase_baseline",
    "render_dwd_roi_shared_translation_markdown",
    "run_dwd_roi_shared_translation_replay",
    "verify_dwd_roi_shared_translation_document",
]
