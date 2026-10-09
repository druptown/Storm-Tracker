"""Seal the completed DWD v4.1 run and fail-closed v5 resource outcome.

This command never retries v5 and never changes its limits.  It reopens the
two existing durable stores, independently reconstructs v4.1, recomputes the
pre-registered raster references, and writes offline evidence only.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import statistics
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_science_shadow_calibration_replay import (  # noqa: E402
    _active_child_pids,
    _distribution,
    _sha_value,
    _state_metrics,
    _store,
    _system_summary,
)
from scripts.run_science_shadow_dwd_roi_replay import (  # noqa: E402
    EXPORT_ROOT,
    ORDERED_EVIDENCE_RECORD_CONCAT_SHA256,
    ORDERED_RUN_IDS_SHA256,
    SEQUENCE_MANIFEST,
    _per_transition_audit,
    _runner,
    _sha_file,
)
from storm_tracker_worker.hard_decoder_isolation import (  # noqa: E402
    _canonical_json_bytes,
)
from storm_tracker_worker.science_shadow_dwd_roi_baselines import (  # noqa: E402
    compute_dwd_roi_sanity_baselines,
)
from storm_tracker_worker.science_shadow_dwd_roi_replay import (  # noqa: E402
    DWD_ROI_SCIENCE_BLOCKERS,
    DWD_ROI_SCIENCE_CAVEATS,
    EXPECTED_FRAME_COUNT,
    EXPECTED_METRICS_MANIFEST_SHA256,
    EXPECTED_SEQUENCE_ID,
    EXPECTED_SEQUENCE_MANIFEST_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    EXPECTED_TARGET_GRID_SHA256,
    create_dwd_roi_science_replay_authority,
    recover_dwd_roi_science_replay_state,
)


WORK_ROOT = (
    ROOT / ".analysis" / "dwd-rv-germany-roi-2km-science-v4-v5-work-v1"
)
OUTPUT = (
    ROOT / ".analysis"
    / "dwd_rv_germany_roi_2km_v4_complete_v5_resource_failure_20260808_evidence.json"
)
CONTRACT = "stv3-dwd-roi-v4-complete-v5-resource-failure-evidence-v1"
V5_FAILURE = {
    "authority_sha256": (
        "0e07410bf8b79263fdc45f473e5e8c185f246e35686d1b210953d3023e4b4ded"
    ),
    "child_exit_code": 3758096385,
    "contract": "stv3-science-shadow-process-parent-result-v1",
    "detail_code": "hard_resource_limit",
    "duplicate": None,
    "durable_disposition": "not_committed",
    "elapsed_seconds": 60.377112000016496,
    "execution_outcome": "cpu_limit_exceeded",
    "isolation_enforced": True,
    "meteorological_admission_granted": False,
    "operational_snapshot_claim_generated": False,
    "receipt_metadata": None,
    "status": "resource",
}


def _atomic_write(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    if temporary.exists() or path.exists():
        raise RuntimeError("failure evidence output must be fresh")
    with temporary.open("xb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _authority(label: str, mode: str):
    root = WORK_ROOT / label
    return create_dwd_roi_science_replay_authority(
        export_root=EXPORT_ROOT.resolve(),
        sequence_manifest_path=SEQUENCE_MANIFEST.resolve(),
        receipt_path=root / "receipts.sqlite3",
        authority_path=root / "authority.json",
        identity_mode=mode,
    )


def _receipt_summary(authority) -> dict[str, Any]:
    receipts = _store(authority).advanced_receipts()
    accountings = [item.accounting.to_mapping() for item in receipts]
    totals = {
        key: sum(item[key] for item in accountings)
        for key in accountings[0]
    } if accountings else {}
    return {
        "advanced_receipt_count": len(receipts),
        "all_admission_false": all(
            not item.meteorological_admission_granted for item in receipts
        ),
        "all_forecast_false": all(not item.forecast_generated for item in receipts),
        "all_pixel_accounting_exhaustive": all(
            item["total_analysis_pixels"]
            == item["valid_detector_input_pixels"]
            + item["excluded_nodata_pixels"]
            + item["excluded_undetect_pixels"]
            + item["excluded_mixed_pixels"]
            + item["excluded_outside_support_pixels"]
            for item in accountings
        ),
        "all_process_isolation_certified_false": all(
            not item.process_isolation_certified for item in receipts
        ),
        "all_snapshot_claims_false": all(
            not item.operational_snapshot_claim_generated for item in receipts
        ),
        "canonical_run_ids_sha256": _sha_value(
            [item.canonical_run_id for item in receipts]
        ),
        "head_receipt_sha256": (
            receipts[-1].receipt_sha256 if receipts else None
        ),
        "pixel_and_candidate_totals": totals,
        "receipt_chain_sha256": _sha_value(
            [item.receipt_sha256 for item in receipts]
        ),
        "receipt_sha256": [item.receipt_sha256 for item in receipts],
    }


def _baseline_summary(document: dict[str, Any]) -> dict[str, Any]:
    translations = [
        item["translation"]
        for item in document["masked_interval_translation_references"]
    ]
    resolved = [
        item for item in translations if item["status"] == "resolved_unique_peak"
    ]
    speeds = [item["speed_km_h"] for item in resolved]
    overlaps = [item["overlap_fraction_of_smaller_valid_support"] for item in resolved]
    shifts = Counter(
        (item["shift_column_pixels"], item["shift_row_pixels"])
        for item in resolved
    )
    low_overlap = [
        {
            "current_frame_number": source["current_frame_number"],
            "overlap_fraction": translation[
                "overlap_fraction_of_smaller_valid_support"
            ],
            "previous_frame_number": source["previous_frame_number"],
            "shift_column_pixels": translation["shift_column_pixels"],
            "shift_row_pixels": translation["shift_row_pixels"],
            "speed_km_h": translation["speed_km_h"],
        }
        for source, translation in zip(
            document["masked_interval_translation_references"], translations
        )
        if (
            translation["status"] == "resolved_unique_peak"
            and translation["overlap_fraction_of_smaller_valid_support"] < 0.1
        )
    ]
    return {
        "first_last_support_reference": document[
            "first_last_support_reference"
        ],
        "interval_count": len(translations),
        "interval_speed_km_h": _distribution(speeds),
        "low_overlap_below_0_05_count": sum(value < 0.05 for value in overlaps),
        "low_overlap_below_0_10_count": sum(value < 0.1 for value in overlaps),
        "low_overlap_below_0_25_count": sum(value < 0.25 for value in overlaps),
        "low_overlap_extreme_intervals": low_overlap,
        "minimum_overlap_fraction": min(overlaps) if overlaps else None,
        "resolved_unique_peak_count": len(resolved),
        "shift_mode": (
            {
                "count": shifts.most_common(1)[0][1],
                "shift_column_pixels": shifts.most_common(1)[0][0][0],
                "shift_row_pixels": shifts.most_common(1)[0][0][1],
            }
            if shifts else None
        ),
        "speed_over_200_kmh_count": sum(value > 200.0 for value in speeds),
        "uncertainty_interpretation": (
            "large-shift/high-speed peaks with very small overlap are explicit "
            "low-support uncertainty, not motion truth and not tuning evidence"
        ),
    }


def _v4_baseline_comparison(
    audit: dict[str, Any], baseline: dict[str, Any]
) -> dict[str, Any]:
    tracker_speeds = [
        item["hypothesis_speed_kmh"]
        for item in recoverable_intervals(audit)
    ]
    return {
        "accuracy_claimed": False,
        "baseline_used_for_cost_tuning": False,
        "first_last_aggregate_support_reference": baseline[
            "first_last_support_reference"
        ],
        "masked_phase_correlation_reference": {
            key: value for key, value in baseline.items()
            if key != "first_last_support_reference"
        },
        "one_to_one_identity_comparison_possible": False,
        "tracker_selected_hypothesis_speed_km_h": _distribution(tracker_speeds),
        "tracker_selected_speed_over_200_kmh_count": sum(
            value > 200.0 for value in tracker_speeds
        ),
        "verdict": (
            "side-by-side descriptive robustness evidence only; centroid mixes "
            "multiple evolving systems and phase correlation contains low-overlap "
            "outliers, while neither supplies independent cell identity truth"
        ),
    }


def recoverable_intervals(audit: dict[str, Any]) -> list[dict[str, Any]]:
    # The direct audit stores per-frame selected maxima; exact interval records
    # are in the recovered durable state summary assembled separately.  This
    # helper is replaced by that exact list in main before comparison.
    return list(audit.get("hypothesis_interval_speeds", ()))


def _markdown(report: dict[str, Any], json_sha: str) -> str:
    v4 = report["v4_1_complete"]
    failure = report["v5_resource_negative"]["terminal_result"]
    baseline = report["independent_baselines"]["summary"]
    return "\n".join([
        "# DWD Germany ROI: v4.1 complete, v5 resource-negative",
        "",
        "## Outcome",
        "",
        "v4.1 completed all 36 sealed DWD +000 frames. v5 was killed fail-closed "
        "during frame 19 by the fixed 60-second child CPU limit; frame 19 was not "
        "committed and the durable v5 head remained revision 18. No retry, cap "
        "increase or tracker tuning was performed.",
        "",
        f"- v4.1 final revision: {v4['state']['revision']}.",
        f"- v4.1 receipt chain: `{v4['receipts']['receipt_chain_sha256']}`.",
        f"- v5 status/outcome: `{failure['status']}` / `{failure['execution_outcome']}`.",
        f"- v5 elapsed to enforced CPU termination: {failure['elapsed_seconds']:.6f} s.",
        "- Active child count after failure and audit: 0.",
        "",
        "## Independent references (evaluation only)",
        "",
        f"- First-to-last >=1 mm/h support centroid reference: {baseline['first_last_support_reference'].get('binary_reference_speed_km_h')} km/h over 175 minutes.",
        f"- Masked phase-correlation intervals: {baseline['resolved_unique_peak_count']}/35 unique peaks.",
        f"- Most common shift: {baseline['shift_mode']}.",
        f"- Intervals with overlap <0.10: {baseline['low_overlap_below_0_10_count']}; these outliers are explicitly uncertain.",
        "- Neither reference is speed or identity truth, and neither tuned tracking.",
        "",
        "## Scientific limits",
        "",
        "The 2-km area-weighted grid changes object/core topology. Missing, nodata, "
        "undetect and mixed cells were not filled or called dry. WeatherSystem was "
        "not rebound. No accuracy, admission, forecast, service, MQTT or HA claim "
        "is made.",
        "",
        "## Artifact",
        "",
        f"- JSON SHA-256: `{json_sha}`",
        f"- Baseline receipt SHA-256: `{report['independent_baselines']['receipt_sha256']}`",
        "",
        "## Blockers",
        "",
        *[f"- `{item}`" for item in report["blockers"]],
        "- `v5_frame_19_cpu_limit_exceeded_under_fixed_60_second_cap`",
        "- `v4_per_frame_resource_samples_were_not_persisted_when_later_v5_failed`",
        "",
    ])


def main() -> int:
    if OUTPUT.exists() or OUTPUT.with_suffix(".md").exists():
        raise RuntimeError("failure evidence already exists")
    if _active_child_pids():
        raise RuntimeError("failure sealer requires an empty child baseline")
    v4 = _authority("v4_1", "v4_1_per_transition")
    v5 = _authority("v5", "v5_multi_scan")
    if v4.authority_sha256 != (
        "4d7cfe983aa72a915634c041a0a8bf0f723346c445444a653ced39b0782bd5cd"
    ):
        raise RuntimeError("v4 authority changed")
    if v5.authority_sha256 != V5_FAILURE["authority_sha256"]:
        raise RuntimeError("v5 failure belongs to another authority")
    v4_store_before = {
        "bytes": v4.receipt_path.stat().st_size,
        "sha256": _sha_file(v4.receipt_path),
    }
    v4_state = recover_dwd_roi_science_replay_state(v4)
    v5_state = recover_dwd_roi_science_replay_state(v5)
    if v4_state.revision != 36 or v5_state.revision != 18:
        raise RuntimeError("durable heads differ from the fail-closed observation")
    v4_receipts = _receipt_summary(v4)
    v5_receipts = _receipt_summary(v5)
    if (
        v4_receipts["advanced_receipt_count"] != 36
        or v4_receipts["canonical_run_ids_sha256"] != ORDERED_RUN_IDS_SHA256
        or v5_receipts["advanced_receipt_count"] != 18
    ):
        raise RuntimeError("durable receipt accounting differs")
    duplicate = _runner(v4).execute(
        attempt_id="dwd-roi-v4-1-sealed-duplicate-frame-020",
        canonical_run_id=v4.export.run_ids[19],
    )
    if not (
        duplicate.status == "success"
        and duplicate.committed
        and duplicate.duplicate is True
        and duplicate.execution_outcome == "durable_duplicate"
        and duplicate.receipt_metadata is not None
        and duplicate.receipt_metadata["receipt_sha256"]
        == v4_receipts["receipt_sha256"][19]
    ):
        raise RuntimeError("v4 duplicate audit differs")
    if v4_store_before != {
        "bytes": v4.receipt_path.stat().st_size,
        "sha256": _sha_file(v4.receipt_path),
    }:
        raise RuntimeError("v4 duplicate changed the durable store")
    print("v4.1: direct 36-frame audit", flush=True)
    v4_audit = _per_transition_audit(v4)
    exact_v4_state = _state_metrics(v4_state)
    v4_audit["hypothesis_interval_speeds"] = exact_v4_state[
        "hypothesis_interval_speeds"
    ]
    print("baseline: independent masked references", flush=True)
    baseline = compute_dwd_roi_sanity_baselines(v4.export)
    baseline_summary = _baseline_summary(baseline.document)
    evidence_record_concat = hashlib.sha256("".join(
        item.evidence_record_sha256 for item in v4.export.entries
    ).encode("ascii")).hexdigest()
    if evidence_record_concat != ORDERED_EVIDENCE_RECORD_CONCAT_SHA256:
        raise RuntimeError("sealed evidence order differs")
    if _active_child_pids():
        raise RuntimeError("failure audit leaked a child")
    report = {
        "admission_granted": False,
        "blockers": list(DWD_ROI_SCIENCE_BLOCKERS),
        "caveats": list(DWD_ROI_SCIENCE_CAVEATS),
        "contract": CONTRACT,
        "forecast_leads_used": False,
        "independent_baselines": {
            "document": baseline.document,
            "receipt_sha256": baseline.receipt_sha256,
            "summary": baseline_summary,
        },
        "input": {
            "frame_count": EXPECTED_FRAME_COUNT,
            "metrics_manifest_sha256": EXPECTED_METRICS_MANIFEST_SHA256,
            "ordered_evidence_record_concat_sha256": evidence_record_concat,
            "ordered_run_ids_sha256": ORDERED_RUN_IDS_SHA256,
            "sequence_id": EXPECTED_SEQUENCE_ID,
            "sequence_manifest_sha256": EXPECTED_SEQUENCE_MANIFEST_SHA256,
            "source_manifest_sha256": EXPECTED_SOURCE_MANIFEST_SHA256,
            "target_grid_sha256": EXPECTED_TARGET_GRID_SHA256,
        },
        "meteorological_accuracy_claimed": False,
        "missing_values_treated_as_dry": False,
        "operational_snapshot_generated": False,
        "service_or_live_wiring_changed": False,
        "truth_available": False,
        "v4_1_complete": {
            "audit": v4_audit,
            "authority_sha256": v4.authority_sha256,
            "duplicate_audit": duplicate.to_mapping(),
            "policy_fingerprint": v4_state.policy_fingerprint,
            "receipts": v4_receipts,
            "restart_proof": {
                "checkpoint_revision": _store(v4).stats()["checkpoint_revision"],
                "final_recovered_revision": v4_state.revision,
                "replay_tail_frames": _store(v4).stats()["replay_tail_frames"],
                "runner_reconstructed_before_frames": [21, 33],
            },
            "state": exact_v4_state,
            "store": v4_store_before,
            "weather_system_snapshot": _system_summary(v4_state, 36),
        },
        "v4_1_vs_independent_references": _v4_baseline_comparison(
            v4_audit, baseline_summary
        ),
        "v5_resource_negative": {
            "authority_sha256": v5.authority_sha256,
            "durable_partial_head_revision": v5_state.revision,
            "frame_19_committed": False,
            "no_retry_or_limit_change_performed": True,
            "policy_fingerprint": v5_state.policy_fingerprint,
            "receipts_before_failure": v5_receipts,
            "store": {
                "bytes": v5.receipt_path.stat().st_size,
                "sha256": _sha_file(v5.receipt_path),
            },
            "terminal_result": V5_FAILURE,
            "tracking_result_claimed": False,
        },
        "weather_system_rebinding_performed": False,
        "worker_processes_after_audit": 0,
    }
    # The hard canonicalizer deliberately rejects aliased containers.  The
    # report reuses immutable summaries for human comparison, so materialize
    # one ordinary JSON tree before sealing it.
    report = json.loads(json.dumps(
        report,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ))
    body = _canonical_json_bytes(report)
    _atomic_write(OUTPUT, body)
    json_sha = hashlib.sha256(body).hexdigest()
    markdown = _markdown(report, json_sha).encode("utf-8")
    markdown_path = OUTPUT.with_suffix(".md")
    _atomic_write(markdown_path, markdown)
    print(json.dumps({
        "active_children": sorted(_active_child_pids()),
        "baseline_receipt_sha256": baseline.receipt_sha256,
        "json": str(OUTPUT),
        "json_sha256": json_sha,
        "markdown": str(markdown_path),
        "markdown_sha256": hashlib.sha256(markdown).hexdigest(),
        "v4_authority_sha256": v4.authority_sha256,
        "v4_receipt_chain_sha256": v4_receipts["receipt_chain_sha256"],
        "v5_authority_sha256": v5.authority_sha256,
        "v5_head_revision": v5_state.revision,
    }, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    multiprocessing.freeze_support()
    raise SystemExit(main())
