"""Run the sealed 36-frame DWD Germany-ROI science comparison offline.

This is a read-only, hard-isolated shadow evidence command.  It has no
service, Home Assistant, MQTT, acquisition, forecast or admission wiring.
The independent raster references are computed without tracker output and
are never used to tune tracking costs.
"""
from __future__ import annotations

from collections import Counter
import argparse
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import statistics
import sys
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_science_shadow_calibration_replay import (  # noqa: E402
    JUMP_THRESHOLD_KMH,
    _active_child_pids,
    _distribution,
    _execute_sampled,
    _multi_hypothesis_audit,
    _nearest_rank,
    _sha_value,
    _state_metrics,
    _store,
    _system_summary,
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
    build_dwd_roi_science_replay_orchestrator,
    create_dwd_roi_science_replay_authority,
    recover_dwd_roi_science_replay_state,
)
from storm_tracker_worker.science_shadow_orchestrator import (  # noqa: E402
    _encode_state_value,
)
from storm_tracker_worker.science_shadow_process_isolation import (  # noqa: E402
    ScienceShadowIsolatedRunner,
)
from storm_tracker_worker.tracking_core.engine.cell_track_engine import (  # noqa: E402
    advance_cell_track_engine,
    create_cell_track_engine_state,
)


EXPORT_ROOT = ROOT / ".analysis" / "dwd-rv-germany-roi-2km-36-export-v1"
SEQUENCE_MANIFEST = (
    EXPORT_ROOT / "manifests" / f"{EXPECTED_SEQUENCE_MANIFEST_SHA256}.json"
)
DEFAULT_WORK_ROOT = (
    ROOT / ".analysis" / "dwd-rv-germany-roi-2km-science-v4-v5-work-v1"
)
DEFAULT_OUTPUT = (
    ROOT / ".analysis" / "dwd_rv_germany_roi_2km_science_v4_v5_20260808.json"
)
REPORT_CONTRACT = "stv3-dwd-roi-science-v4-v5-comparison-evidence-v1"
ORDERED_EVIDENCE_RECORD_CONCAT_SHA256 = (
    "9fac976800e063fb44da4d1344f6daec1aba4ecd53ccf0d908cf55d94eab4777"
)
ORDERED_RUN_IDS_SHA256 = (
    "16f439ad52481c8086afc206ec2fda9ee9365e0fa4c27cb090632ce5b09e5ebf"
)


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    if temporary.exists():
        raise RuntimeError("stale report temporary file exists")
    with temporary.open("xb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _runner(authority) -> ScienceShadowIsolatedRunner:
    return ScienceShadowIsolatedRunner(
        authority_path=authority.authority_path,
        expected_authority_sha256=authority.authority_sha256,
        factory_contract=authority.factory_contract,
        limits=authority.isolation_limits,
        context=multiprocessing.get_context("spawn"),
    )


def _resource_summary(frames: list[dict[str, Any]]) -> dict[str, Any]:
    wall = [item["parent_execute_wall_seconds"] for item in frames]
    child_wall = [item["child_reported_elapsed_seconds"] for item in frames]
    cpu = [item["child_cpu_seconds_sampled"] for item in frames]
    rss = [item["peak_child_rss_sampled_bytes"] for item in frames]
    if not (
        len(wall) == len(child_wall) == len(cpu) == len(rss)
        == EXPECTED_FRAME_COUNT
        and all(value is not None for value in (*cpu, *rss))
    ):
        raise RuntimeError("resource sampler missed a DWD science child")
    cpu_values = [float(value) for value in cpu]
    rss_values = [int(value) for value in rss]
    result = {
        "child_cpu_seconds": _distribution(cpu_values),
        "child_peak_rss_bytes": _distribution(rss_values),
        "child_reported_elapsed_seconds": _distribution(child_wall),
        "frame_count": len(frames),
        "kernel_limits": {
            "cpu_seconds_each": 60.0,
            "rss_bytes_each": 1024**3,
            "wall_seconds_each": 90.0,
        },
        "measurement_semantics": {
            "boundary": (
                "ScienceShadowProcessResult does not expose the decoder boundary; "
                "the authority-bound 1 GiB RSS/60 s CPU/90 s wall limits remain "
                "parent enforced, while this report records sampled OS child peaks"
            ),
            "cpu": "sampled OS child process user+kernel lifetime CPU",
            "rss": "sampled OS child lifetime peak working set/VmHWM",
            "wall": "parent perf_counter around each isolated execute",
        },
        "parent_execute_wall_seconds": _distribution(wall),
    }
    for cadence in (300.0, 600.0):
        p95 = result["parent_execute_wall_seconds"]["nearest_rank_p95"]
        maximum = result["parent_execute_wall_seconds"]["maximum"]
        result[f"cadence_{int(cadence)}s"] = {
            "maximum_frame_headroom_seconds": cadence - maximum,
            "maximum_frame_wall_fraction": maximum / cadence,
            "p95_frame_headroom_seconds": cadence - p95,
            "p95_frame_wall_fraction": p95 / cadence,
            "total_cpu_fraction_of_serial_cadence_budget": (
                math.fsum(cpu_values) / (len(frames) * cadence)
            ),
            "total_wall_fraction_of_serial_cadence_budget": (
                math.fsum(wall) / (len(frames) * cadence)
            ),
        }
    return result


def _track_continuity(state) -> dict[str, Any]:
    lengths = [item.sample_count for item in state.ledger.tracks]
    return {
        "active_track_count": sum(
            item.lifecycle_state != "ended" for item in state.ledger.tracks
        ),
        "assigned_detection_samples": sum(lengths),
        "ended_track_count": sum(
            item.lifecycle_state == "ended" for item in state.ledger.tracks
        ),
        "track_count": len(lengths),
        "track_length_maximum_observations": max(lengths) if lengths else 0,
        "track_length_mean_observations": (
            statistics.fmean(lengths) if lengths else 0.0
        ),
        "track_length_median_observations": (
            statistics.median(lengths) if lengths else 0.0
        ),
        "track_length_one_observation_count": sum(value == 1 for value in lengths),
    }


def _per_transition_audit(authority) -> dict[str, Any]:
    """Recompute the v4.1 path and prove complete candidate accounting."""

    orchestrator = build_dwd_roi_science_replay_orchestrator(
        authority.authority_document
    )
    policy = orchestrator._policy
    composer = orchestrator._composer
    state = create_cell_track_engine_state(
        authority.authority_document["engine_id"], policy
    )
    per_frame: list[dict[str, Any]] = []
    selected_speeds: list[float] = []
    top_weights: list[float] = []
    entropies: list[float] = []
    started = time.perf_counter()
    for frame_number, run_id in enumerate(authority.export.run_ids, start=1):
        composed = composer.compose(
            run_id=run_id,
            deadline_at=time.monotonic() + 60.0,
            monotonic=time.monotonic,
        )
        if composed.evidence is None:
            raise RuntimeError("v4.1 audit composer withheld a sealed frame")
        transition = advance_cell_track_engine(
            state, composed.evidence.normalized_frame, policy
        )
        if transition.status != "advanced":
            raise RuntimeError(
                f"v4.1 audit frame {frame_number} is {transition.status}"
            )
        assignment = transition.assignment_result
        snapshot = transition.identity_snapshot
        expected_candidates = (
            0 if state.last_frame is None
            else sum(
                item.lifecycle_state != "ended" for item in state.ledger.tracks
            ) * len(transition.detection_frame.detections)
        )
        feature_by_pair = {
            (item.previous_detection_id, item.current_detection_id): item
            for table in transition.association_feature_tables
            for item in table.features
        }
        frame_speeds = []
        if assignment is not None:
            frame_speeds = [
                feature_by_pair[pair].observed_interval_speed_kmh
                for pair in assignment.best.matches
            ]
            selected_speeds.extend(frame_speeds)
        lineage = Counter(item.event_type for item in transition.lineage_hypotheses)
        record: dict[str, Any] = {
            "assignment_births": 0 if assignment is None else len(assignment.best.births),
            "assignment_deaths": 0 if assignment is None else len(assignment.best.deaths),
            "assignment_matches": 0 if assignment is None else len(assignment.best.matches),
            "candidate_accounting_exact": (
                transition.candidate_edge_count == expected_candidates
            ),
            "candidate_edges": transition.candidate_edge_count,
            "derived_merge_alternatives": lineage.get("merge", 0),
            "derived_split_alternatives": lineage.get("split", 0),
            "detection_objects": len(transition.detection_frame.detections),
            "frame_number": frame_number,
            "run_id": run_id,
            "selected_interval_speed_maximum_kmh": (
                max(frame_speeds) if frame_speeds else None
            ),
        }
        if snapshot is None:
            record["identity_state"] = "bootstrap_no_predecessor"
        else:
            top_weights.append(snapshot.hypotheses[0].conditional_weight)
            entropies.append(snapshot.conditional_entropy_nats)
            record.update({
                "assignment_search_exhausted": snapshot.assignment_search_exhausted,
                "conditional_entropy_nats": snapshot.conditional_entropy_nats,
                "conditional_top_one_weight": snapshot.hypotheses[0].conditional_weight,
                "identity_hypotheses_retained": len(snapshot.hypotheses),
                "identity_state": "bounded_per_transition_top_k",
                "omitted_hypothesis_mass_unknown": (
                    snapshot.omitted_hypothesis_mass_unknown
                ),
            })
        per_frame.append(record)
        state = transition.next_state

    isolated = recover_dwd_roi_science_replay_state(authority)
    direct_body = _canonical_json_bytes(_encode_state_value(state))
    isolated_body = _canonical_json_bytes(_encode_state_value(isolated))
    if direct_body != isolated_body:
        raise RuntimeError("v4.1 direct audit differs from isolated durable head")
    return {
        "audit_runtime_seconds": time.perf_counter() - started,
        "candidate_accounting_exact_all_frames": all(
            item["candidate_accounting_exact"] for item in per_frame
        ),
        "candidate_edges_total": sum(item["candidate_edges"] for item in per_frame),
        "conditional_entropy_nats": _distribution(entropies),
        "conditional_top_one_weight": _distribution(top_weights),
        "derived_merge_alternative_count": sum(
            item["derived_merge_alternatives"] for item in per_frame
        ),
        "derived_split_alternative_count": sum(
            item["derived_split_alternatives"] for item in per_frame
        ),
        "final_state_exactly_matches_isolated_head": True,
        "final_state_sha256": hashlib.sha256(isolated_body).hexdigest(),
        "omitted_mass_unknown_frames": sum(
            bool(item.get("omitted_hypothesis_mass_unknown"))
            for item in per_frame
        ),
        "per_frame": per_frame,
        "selected_interval_speed_count": len(selected_speeds),
        "selected_interval_speed_maximum_kmh": (
            max(selected_speeds) if selected_speeds else None
        ),
        "selected_interval_speed_median_kmh": (
            statistics.median(selected_speeds) if selected_speeds else None
        ),
        "selected_interval_speed_p95_kmh": _nearest_rank(selected_speeds, 0.95),
        "selected_interval_speed_over_200_kmh": sum(
            value > JUMP_THRESHOLD_KMH for value in selected_speeds
        ),
        "track_continuity": _track_continuity(state),
    }


def _run_mode(authority, *, label: str) -> dict[str, Any]:
    if _active_child_pids():
        raise RuntimeError("science replay refuses a non-empty child baseline")
    initial_bytes = authority.receipt_path.stat().st_size
    frames: list[dict[str, Any]] = []
    restart_proofs: list[dict[str, Any]] = []
    receipt_by_run: dict[str, str] = {}
    runner = _runner(authority)
    started = time.perf_counter()
    for frame_number, run_id in enumerate(authority.export.run_ids, start=1):
        if frame_number in (21, 33):
            previous = recover_dwd_roi_science_replay_state(authority)
            if previous.revision != frame_number - 1:
                raise RuntimeError("restart boundary recovered the wrong revision")
            restart_proofs.append({
                "after_revision": frame_number - 1,
                "before_frame_number": frame_number,
                "recovered_state_sha256": hashlib.sha256(
                    _canonical_json_bytes(_encode_state_value(previous))
                ).hexdigest(),
            })
            runner = _runner(authority)
        before = authority.receipt_path.stat().st_size
        result, elapsed, peak_rss, child_cpu, remaining = _execute_sampled(
            runner,
            attempt_id=f"dwd-roi-{label}-frame-{frame_number:03d}",
            canonical_run_id=run_id,
        )
        metadata = result.receipt_metadata
        if not (
            result.status == "success"
            and result.committed
            and result.duplicate is False
            and result.isolation_enforced
            and not result.meteorological_admission_granted
            and not result.operational_snapshot_claim_generated
            and metadata is not None
            and metadata["outcome"] == "advanced"
            and metadata["next_state_revision"] == frame_number
            and metadata["canonical_run_id"] == run_id
            and metadata["authority_sha256"] == authority.authority_sha256
            and not metadata["meteorological_admission_granted"]
            and not metadata["operational_snapshot_claim_generated"]
            and not remaining
        ):
            raise RuntimeError(
                f"{label} frame {frame_number} failed closed: {result.to_mapping()}"
            )
        receipt_by_run[run_id] = metadata["receipt_sha256"]
        frames.append({
            "accounting": metadata["accounting"],
            "attempt_id": f"dwd-roi-{label}-frame-{frame_number:03d}",
            "authority_sha256": result.authority_sha256,
            "child_cpu_seconds_sampled": child_cpu,
            "child_exit_code": result.child_exit_code,
            "child_reported_elapsed_seconds": result.elapsed_seconds,
            "frame_number": frame_number,
            "next_state_revision": metadata["next_state_revision"],
            "parent_execute_wall_seconds": elapsed,
            "peak_child_rss_sampled_bytes": peak_rss,
            "previous_advanced_receipt_sha256": (
                metadata["previous_advanced_receipt_sha256"]
            ),
            "receipt_file_growth_bytes": (
                authority.receipt_path.stat().st_size - before
            ),
            "receipt_sha256": metadata["receipt_sha256"],
            "run_id": run_id,
            "transition_id": metadata["transition_id"],
        })
        if frame_number in (12, 24, 36):
            print(f"{label}: {frame_number}/36", flush=True)

    forward_elapsed = time.perf_counter() - started
    duplicate_frame_number = 20
    duplicate_run = authority.export.run_ids[duplicate_frame_number - 1]
    duplicate = _runner(authority).execute(
        attempt_id=f"dwd-roi-{label}-duplicate-frame-{duplicate_frame_number:03d}",
        canonical_run_id=duplicate_run,
    )
    if not (
        duplicate.status == "success"
        and duplicate.committed
        and duplicate.duplicate is True
        and duplicate.execution_outcome == "durable_duplicate"
        and duplicate.receipt_metadata is not None
        and duplicate.receipt_metadata["receipt_sha256"]
        == receipt_by_run[duplicate_run]
    ):
        raise RuntimeError(f"{label} durable duplicate differs")
    final_state = recover_dwd_roi_science_replay_state(authority)
    if final_state.revision != EXPECTED_FRAME_COUNT:
        raise RuntimeError(f"{label} final revision differs")
    receipts = _store(authority).advanced_receipts()
    if (
        len(receipts) != EXPECTED_FRAME_COUNT
        or [item.canonical_run_id for item in receipts]
        != list(authority.export.run_ids)
        or [item.receipt_sha256 for item in receipts]
        != [item["receipt_sha256"] for item in frames]
    ):
        raise RuntimeError(f"{label} durable receipt order differs")
    if _active_child_pids():
        raise RuntimeError(f"{label} leaked a child process")
    state_metrics = _state_metrics(final_state)
    state_metrics["track_continuity"] = _track_continuity(final_state)
    result = {
        "active_child_pids_after_run": [],
        "authority_sha256": authority.authority_sha256,
        "duplicate_proof": {
            "duplicate": True,
            "execution_outcome": duplicate.execution_outcome,
            "frame_number": duplicate_frame_number,
            "receipt_metadata_exact": True,
            "receipt_sha256": duplicate.receipt_metadata["receipt_sha256"],
        },
        "final_store": _store(authority).stats(),
        "forward_runtime_seconds": forward_elapsed,
        "frame_count": len(frames),
        "identity_mode": authority.identity_mode,
        "initial_store_file_bytes": initial_bytes,
        "isolation_limits": authority.isolation_limits.to_mapping(),
        "label": label,
        "per_frame": frames,
        "policy_fingerprint": final_state.policy_fingerprint,
        "receipt_chain_sha256": _sha_value(
            [item.receipt_sha256 for item in receipts]
        ),
        "receipt_store": {
            "bytes": authority.receipt_path.stat().st_size,
            "growth_bytes": authority.receipt_path.stat().st_size - initial_bytes,
            "sha256": _sha_file(authority.receipt_path),
        },
        "resource": _resource_summary(frames),
        "restart_proofs": restart_proofs,
        "runtime_profile_sha256": (
            authority.authority_document["runtime_profile_sha256"]
        ),
        "state": state_metrics,
        "weather_system_snapshot": _system_summary(final_state, 36),
    }
    return result


def _comparison(v4: dict[str, Any], v5: dict[str, Any]) -> dict[str, Any]:
    v4_identity = v4["audit"]
    v5_identity = v5["audit"]["multi_scan"]["selected_decision_path"]
    keys = (
        "total_tracks", "active_tracks", "ended_tracks", "birth_events",
        "death_events", "hypothesis_interval_count",
        "hypothesis_jump_count_over_200_kmh",
        "hypothesis_speed_median_kmh", "hypothesis_speed_p95_kmh",
        "hypothesis_speed_maximum_kmh",
        "track_length_median_observations",
        "track_length_mean_observations", "track_length_maximum_observations",
    )
    return {
        "accuracy_claimed": False,
        "baseline_used_for_tracker_tuning": False,
        "descriptive_selected_identity_metrics": {
            key: {"v4_1": v4_identity.get(key), "v5_map": v5_identity.get(key)}
            for key in keys
        },
        "independent_identity_truth_available": False,
        "promotion_or_rebinding_permitted": False,
        "sequence_identity_equal": True,
        "verdict_semantics": (
            "cross-provider robustness and negative/uncertainty evidence only; "
            "no algorithm wins without independent identity and speed truth"
        ),
        "weather_system_rebinding_performed": False,
    }


def _markdown(report: dict[str, Any], json_name: str, json_sha: str) -> str:
    baseline = report["independent_sanity_baseline"]
    first_last = baseline["document"]["first_last_support_reference"]
    v4 = report["modes"]["v4_1"]
    v5 = report["modes"]["v5"]
    selected = v5["audit"]["multi_scan"]["selected_decision_path"]
    return "\n".join([
        "# DWD Germany ROI 2-km science-shadow replay",
        "",
        "## Verdict",
        "",
        "This is sealed cross-provider robustness evidence, not an accuracy or admission claim. "
        "No independent cell identity or speed truth exists. The v5 multi-scan result remains a "
        "shadow challenger and WeatherSystem was not rebound.",
        "",
        "## Immutable input",
        "",
        f"- Frames: {report['input']['frame_count']} official DWD RV +000 observations at 300 s cadence.",
        f"- Sequence manifest: `{report['input']['sequence_manifest_sha256']}`.",
        f"- Sequence ID: `{report['input']['sequence_id']}`.",
        f"- Target grid: `{report['input']['target_grid_sha256']}` (350 x 425, 2 km).",
        "- Missing/nodata/undetect/mixed values were never filled or treated as dry.",
        "- The 2-km aggregation changes object/core topology and is not 1-km truth.",
        "",
        "## Independent references (not truth)",
        "",
        f"- First-to-last >=1 mm/h binary-support centroid reference: {first_last.get('binary_reference_speed_km_h')} km/h over 175 min.",
        f"- Masked interval references: {baseline['document']['interval_count']}; tracker output consulted: {baseline['document']['tracker_outputs_consulted']}.",
        "- The references were fixed before tracking and were not used to tune costs.",
        "",
        "## Isolated tracker summaries",
        "",
        f"- v4.1 primary: {v4['state']['total_tracks']} tracks, {v4['state']['hypothesis_interval_count']} intervals, "
        f"{v4['state']['hypothesis_jump_count_over_200_kmh']} >200 km/h jumps.",
        f"- v5 MAP: {selected['total_tracks']} tracks, {selected['hypothesis_interval_count']} intervals, "
        f"{selected['hypothesis_jump_count_over_200_kmh']} >200 km/h jumps.",
        f"- v4.1 policy: `{v4['policy_fingerprint']}`.",
        f"- v5 policy: `{v5['policy_fingerprint']}`.",
        "- Both used fresh independent stores, hard spawn isolation, explicit restarts before frames 21 and 33, and an exact durable duplicate after frame 36.",
        "",
        "## Resource meaning",
        "",
        "Per-frame child RSS/CPU and parent execute wall are reported as p50/p95/max in JSON. "
        "These desktop measurements are not NUC/HA/Z2M admission evidence.",
        "",
        "## Artifacts",
        "",
        f"- JSON: `{json_name}`",
        f"- JSON SHA-256: `{json_sha}`",
        f"- Baseline receipt SHA-256: `{baseline['receipt_sha256']}`",
        "",
        "## Open blockers",
        "",
        *[f"- `{item}`" for item in report["blockers"]],
        "",
    ])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-root", type=Path, default=DEFAULT_WORK_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    work = args.work_root.resolve(strict=False)
    output = args.output.resolve(strict=False)
    if work.exists() or output.exists() or output.with_suffix(".md").exists():
        raise RuntimeError("science evidence paths must be fresh")
    if not EXPORT_ROOT.is_dir() or not SEQUENCE_MANIFEST.is_file():
        raise RuntimeError("sealed DWD ROI export is absent")
    work.mkdir(parents=True, exist_ok=False)
    modes = {}
    authorities = {}
    for mode, label in (
        ("v4_1_per_transition", "v4_1"),
        ("v5_multi_scan", "v5"),
    ):
        mode_root = work / label
        mode_root.mkdir()
        authority = create_dwd_roi_science_replay_authority(
            export_root=EXPORT_ROOT.resolve(),
            sequence_manifest_path=SEQUENCE_MANIFEST.resolve(),
            receipt_path=mode_root / "receipts.sqlite3",
            authority_path=mode_root / "authority.json",
            identity_mode=mode,
        )
        authorities[label] = authority
        print(f"{label}: start", flush=True)
        modes[label] = _run_mode(authority, label=label)
        print(f"{label}: isolated forward+restart+duplicate complete", flush=True)

    print("v4_1: direct accounting audit", flush=True)
    modes["v4_1"]["audit"] = _per_transition_audit(authorities["v4_1"])
    print("v5: multi-scan accounting audit", flush=True)
    modes["v5"]["audit"] = _multi_hypothesis_audit(
        authorities["v5"],
        build_orchestrator=build_dwd_roi_science_replay_orchestrator,
        recover_state=recover_dwd_roi_science_replay_state,
    )
    print("baseline: independent masked references", flush=True)
    baseline = compute_dwd_roi_sanity_baselines(authorities["v4_1"].export)
    evidence_record_shas = [
        item.evidence_record_sha256 for item in authorities["v4_1"].export.entries
    ]
    run_ids = list(authorities["v4_1"].export.run_ids)
    if (
        hashlib.sha256("".join(evidence_record_shas).encode("ascii")).hexdigest()
        != ORDERED_EVIDENCE_RECORD_CONCAT_SHA256
        or _sha_value(run_ids) != ORDERED_RUN_IDS_SHA256
    ):
        raise RuntimeError("ordered DWD ROI input identity differs")
    report = {
        "admission_granted": False,
        "blockers": list(DWD_ROI_SCIENCE_BLOCKERS),
        "caveats": list(DWD_ROI_SCIENCE_CAVEATS),
        "comparison": _comparison(modes["v4_1"], modes["v5"]),
        "contract": REPORT_CONTRACT,
        "forecast_leads_used": False,
        "independent_sanity_baseline": {
            "document": baseline.document,
            "receipt_sha256": baseline.receipt_sha256,
        },
        "input": {
            "evidence_aggregate_bytes": (
                authorities["v4_1"].export.evidence_aggregate_bytes
            ),
            "frame_count": EXPECTED_FRAME_COUNT,
            "metrics_manifest_sha256": EXPECTED_METRICS_MANIFEST_SHA256,
            "ordered_evidence_record_concat_sha256": (
                ORDERED_EVIDENCE_RECORD_CONCAT_SHA256
            ),
            "ordered_run_ids_sha256": ORDERED_RUN_IDS_SHA256,
            "sequence_id": EXPECTED_SEQUENCE_ID,
            "sequence_manifest_sha256": EXPECTED_SEQUENCE_MANIFEST_SHA256,
            "source_manifest_sha256": EXPECTED_SOURCE_MANIFEST_SHA256,
            "target_grid_sha256": EXPECTED_TARGET_GRID_SHA256,
        },
        "meteorological_accuracy_claimed": False,
        "missing_values_treated_as_dry": False,
        "modes": modes,
        "operational_snapshot_generated": False,
        "service_or_live_wiring_changed": False,
        "truth_available": False,
        "weather_system_rebinding_performed": False,
    }
    body = _canonical_json_bytes(report)
    _atomic_write(output, body)
    json_sha = hashlib.sha256(body).hexdigest()
    markdown = _markdown(report, output.name, json_sha).encode("utf-8")
    markdown_path = output.with_suffix(".md")
    _atomic_write(markdown_path, markdown)
    print(json.dumps({
        "active_child_pids": sorted(_active_child_pids()),
        "json": str(output),
        "json_sha256": json_sha,
        "markdown": str(markdown_path),
        "markdown_sha256": hashlib.sha256(markdown).hexdigest(),
        "v4_receipt_chain_sha256": modes["v4_1"]["receipt_chain_sha256"],
        "v5_receipt_chain_sha256": modes["v5"]["receipt_chain_sha256"],
    }, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    multiprocessing.freeze_support()
    raise SystemExit(main())
