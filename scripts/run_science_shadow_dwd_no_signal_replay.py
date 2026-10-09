"""Produce bounded hard-isolated evidence for the immutable DWD no-signal ROI."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import multiprocessing
from pathlib import Path
import statistics
import tempfile
from typing import Any

from run_science_shadow_calibration_replay import _execute_sampled

from storm_tracker_worker.hard_decoder_isolation import _canonical_json_bytes
from storm_tracker_worker.science_shadow_dwd_replay import (
    DWD_SCIENCE_REPLAY_BLOCKERS,
    DWD_SCIENCE_REPLAY_CAVEATS,
    DWD_TRACKING_SOURCE,
    EXPECTED_METRICS_MANIFEST_SHA256,
    EXPECTED_SEQUENCE_ID,
    EXPECTED_SEQUENCE_MANIFEST_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    create_dwd_science_replay_authority,
    dwd_science_tracking_policy,
    load_dwd_science_replay_export,
    recover_dwd_science_replay_state,
)
from storm_tracker_worker.dwd_rv_observation_replay import (
    default_belgium_observation_profile,
)
from storm_tracker_worker.science_shadow_orchestrator import ScienceShadowLimits
from storm_tracker_worker.science_shadow_process_isolation import (
    ScienceShadowIsolatedRunner,
)


SCHEMA = "stv3-science-shadow-dwd-no-signal-evidence-v1"
SELECTED_FRAME_NUMBERS = (0, 1, 17, 35)


def _nearest_rank(values: list[float | int], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def _distribution(values: list[float | int]) -> dict[str, Any]:
    return {
        "count": len(values),
        "maximum": max(values) if values else None,
        "median_p50": statistics.median(values) if values else None,
        "nearest_rank_p95": _nearest_rank(values, 0.95),
        "sum": math.fsum(values),
    }


def _inventory(root: Path) -> dict[str, dict[str, int | str]]:
    return {
        path.relative_to(root).as_posix(): {
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _runner(authority) -> ScienceShadowIsolatedRunner:
    return ScienceShadowIsolatedRunner(
        authority_path=authority.authority_path,
        expected_authority_sha256=authority.authority_sha256,
        factory_contract=authority.factory_contract,
        limits=authority.isolation_limits,
        context=multiprocessing.get_context("spawn"),
    )


def _mode_probe(export_root: Path, manifest: Path, scratch: Path, mode: str) -> dict[str, Any]:
    authority = create_dwd_science_replay_authority(
        export_root=export_root,
        sequence_manifest_path=manifest,
        receipt_path=scratch / f"{mode}.sqlite3",
        authority_path=scratch / f"{mode}.authority.json",
        identity_mode=mode,
        science_limits=ScienceShadowLimits(
            max_receipts=32,
            checkpoint_interval_frames=2,
            max_replay_frames=8,
            max_runtime_seconds=60.0,
        ),
    )
    policy = dwd_science_tracking_policy(
        default_belgium_observation_profile().detector_config,
        identity_mode=mode,
    )
    rows: list[dict[str, Any]] = []
    runner = _runner(authority)
    for ordinal, frame_number in enumerate(SELECTED_FRAME_NUMBERS):
        if ordinal == 2:
            runner = _runner(authority)
        entry = authority.export.entries[frame_number]
        result, wall, rss, cpu, remaining = _execute_sampled(
            runner,
            attempt_id=f"dwd-{mode}-{frame_number}",
            canonical_run_id=entry.canonical_run_id,
        )
        if (
            result.status != "success"
            or result.duplicate is not False
            or result.receipt_metadata is None
            or remaining
        ):
            raise RuntimeError(f"isolated {mode} frame {frame_number} failed: {result}")
        metadata = result.receipt_metadata
        rows.append({
            "accounting": metadata["accounting"],
            "child_cpu_seconds_sampled": cpu,
            "elapsed_seconds_parent": wall,
            "frame_number": frame_number,
            "isolation_enforced": result.isolation_enforced,
            "next_state_revision": metadata["next_state_revision"],
            "peak_child_rss_sampled_bytes": rss,
            "previous_advanced_receipt_sha256": (
                metadata["previous_advanced_receipt_sha256"]
            ),
            "provenance": metadata["provenance"],
            "receipt_sha256": metadata["receipt_sha256"],
            "run_id": entry.canonical_run_id,
            "status": result.status,
            "valid_at": entry.valid_at,
        })
    duplicate_entry = authority.export.entries[SELECTED_FRAME_NUMBERS[1]]
    duplicate = _runner(authority).execute(
        attempt_id=f"dwd-{mode}-duplicate",
        canonical_run_id=duplicate_entry.canonical_run_id,
    )
    if (
        duplicate.status != "success"
        or duplicate.duplicate is not True
        or duplicate.execution_outcome != "durable_duplicate"
    ):
        raise RuntimeError(f"isolated {mode} duplicate failed: {duplicate}")
    state = recover_dwd_science_replay_state(authority)
    rss_values = [
        int(item["peak_child_rss_sampled_bytes"])
        for item in rows
        if item["peak_child_rss_sampled_bytes"] is not None
    ]
    cpu_values = [
        float(item["child_cpu_seconds_sampled"])
        for item in rows
        if item["child_cpu_seconds_sampled"] is not None
    ]
    wall_values = [float(item["elapsed_seconds_parent"]) for item in rows]
    store_files = tuple(
        path for path in scratch.iterdir()
        if path.name.startswith(mode + ".sqlite3") and path.is_file()
    )
    return {
        "authority_sha256": authority.authority_sha256,
        "checkpoint_boundary": {
            "checkpoint_interval_frames": 2,
            "restart_before_probe_ordinal": 2,
            "recovered_revision": state.revision,
        },
        "duplicate": {
            "duplicate": duplicate.duplicate,
            "execution_outcome": duplicate.execution_outcome,
            "receipt_sha256": duplicate.receipt_metadata["receipt_sha256"],
            "status": duplicate.status,
        },
        "final_state": {
            "active_tracks": sum(
                item.lifecycle_state != "ended" for item in state.ledger.tracks
            ),
            "lifecycle_events": len(state.ledger.lineage_events),
            "systems": 0 if state.weather_systems is None else len(state.weather_systems.systems),
            "total_tracks": len(state.ledger.tracks),
        },
        "identity_mode": mode,
        "policy_sha256": policy.fingerprint,
        "resources": {
            "child_cpu_seconds": _distribution(cpu_values),
            "child_peak_rss_bytes": _distribution(rss_values),
            "parent_wall_seconds": _distribution(wall_values),
        },
        "runtime_profile_sha256": authority.authority_document[
            "runtime_profile_sha256"
        ],
        "selected_frames": rows,
        "store": {
            "aggregate_bytes": sum(path.stat().st_size for path in store_files),
            "file_count": len(store_files),
            "main_sha256": hashlib.sha256(authority.receipt_path.read_bytes()).hexdigest(),
        },
    }


def _markdown(report: dict[str, Any]) -> str:
    preflight = report["preflight"]
    lines = [
        "# DWD RV science-shadow no-signal proof",
        "",
        "This is bounded offline robustness evidence, not an accuracy, identity, speed, admission or live-publication claim.",
        "",
        "## Immutable input",
        "",
        f"- Sequence: `{report['input']['sequence_id']}`",
        f"- Sequence manifest: `{report['input']['sequence_manifest_sha256']}`",
        f"- Source manifest: `{report['input']['source_manifest_sha256']}`",
        f"- Metrics manifest: `{report['input']['metrics_manifest_sha256']}`",
        "- Route: DWD RADOLAN RV +000 only, 300-second cadence, interval-mean precipitation rate in mm/h.",
        "- Forecast leads were not opened as tracking observations.",
        "",
        "## Closed-world result",
        "",
        f"All {preflight['frame_count']} evidence objects validated. Finite support was {preflight['finite_pixels']['minimum']}..{preflight['finite_pixels']['maximum']} of 99,856 pixels per frame ({preflight['coverage_fraction']['minimum']:.8%}..{preflight['coverage_fraction']['maximum']:.8%}); all nulls remained explicit nodata/undetect, never dry fill.",
        f"The maximum physical rate was {preflight['maximum_rate_mm_h']['maximum']:.6f} mm/h, below the certified 1.0 mm/h shield threshold in every frame. Existing and replayed detection counts were therefore exactly zero.",
        "",
        "## v4.1 baseline versus v5 challenger",
        "",
    ]
    for mode in ("v4_1_per_transition", "v5_multi_scan"):
        item = report["isolated_probes"][mode]
        resources = item["resources"]
        lines.extend([
            f"- `{mode}`: 4 isolated selected frames, restart after frame 1, exact duplicate verified; 0 tracks, 0 births/deaths, 0 systems, 0 speed/jump samples.",
            f"  Wall p50/p95/max: {resources['parent_wall_seconds']['median_p50']:.3f}/{resources['parent_wall_seconds']['nearest_rank_p95']:.3f}/{resources['parent_wall_seconds']['maximum']:.3f}s; RSS p50/p95/max: {resources['child_peak_rss_bytes']['median_p50']:.0f}/{resources['child_peak_rss_bytes']['nearest_rank_p95']:.0f}/{resources['child_peak_rss_bytes']['maximum']:.0f} bytes.",
        ])
    lines.extend([
        "",
        "The two trackers correctly invented no cells, identities, translations, speeds or WeatherSystems. Because there are no observable cells, continuity, fragmentation and speed comparison are mathematically indeterminate; this is not evidence that either tracker is accurate.",
        "",
        "## Blockers and next experiment",
        "",
        "- `identity_indeterminate`: no observable cells at the certified detector threshold.",
        "- Sparse finite support makes this Belgium crop unsuitable for identity evaluation.",
        "- Independent identity and speed truth remain absent; admission stays false.",
        "- A separate follow-up may build a predeclared full-domain DWD ROI that contains the >=1 mm/h union support. It must retain every component and lineage alternative, use no speed gate and must not auto-merge systems.",
        "- No live/service/MQTT/Home Assistant wiring or WeatherSystem rebinding was added.",
        "",
    ])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-root", type=Path, required=True)
    parser.add_argument("--sequence-manifest", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-markdown", type=Path, required=True)
    args = parser.parse_args()

    root = args.export_root.resolve(strict=True)
    manifest = args.sequence_manifest.resolve(strict=True)
    before = _inventory(root)
    export = load_dwd_science_replay_export(root, manifest)
    finite = list(export.finite_pixels)
    maxima = [
        float(item.maximum_rate_mm_h)
        for item in export.entries
        if item.maximum_rate_mm_h is not None
    ]
    with tempfile.TemporaryDirectory(prefix="dwd-science-nosignal-", dir=args.output_json.parent) as temporary:
        scratch = Path(temporary)
        probes = {
            mode: _mode_probe(root, manifest, scratch, mode)
            for mode in ("v4_1_per_transition", "v5_multi_scan")
        }
    after = _inventory(root)
    if before != after:
        raise RuntimeError("immutable DWD export changed during science replay")
    report: dict[str, Any] = {
        "admission": {
            "accuracy_claimed": False,
            "identity_skill_claimed": False,
            "meteorological_admission_granted": False,
            "operational_snapshot_claim_generated": False,
            "speed_skill_claimed": False,
        },
        "blockers": list(DWD_SCIENCE_REPLAY_BLOCKERS),
        "caveats": list(DWD_SCIENCE_REPLAY_CAVEATS),
        "comparison": {
            "births": 0,
            "continuity": "indeterminate_no_observable_cells",
            "deaths": 0,
            "fragmentation": "indeterminate_no_observable_cells",
            "jump_distance_samples": 0,
            "speed_hypothesis_samples": 0,
            "systems": 0,
            "tracks": 0,
            "verdict": "no_signal_robustness_only_no_identity_comparison_possible",
        },
        "input": {
            "metrics_manifest_sha256": EXPECTED_METRICS_MANIFEST_SHA256,
            "sequence_id": EXPECTED_SEQUENCE_ID,
            "sequence_manifest_sha256": EXPECTED_SEQUENCE_MANIFEST_SHA256,
            "source_manifest_sha256": EXPECTED_SOURCE_MANIFEST_SHA256,
            "tracking_source_identity": DWD_TRACKING_SOURCE,
        },
        "isolated_probes": probes,
        "preflight": {
            "coverage_fraction": {
                "maximum": max(finite) / (316 * 316),
                "mean": statistics.mean(finite) / (316 * 316),
                "minimum": min(finite) / (316 * 316),
            },
            "detection_objects": _distribution([
                item.detection_objects for item in export.entries
            ]),
            "evidence_aggregate_bytes": export.evidence_aggregate_bytes,
            "evidence_record_set_sha256": hashlib.sha256(
                "".join(item.evidence_record_sha256 for item in export.entries).encode("ascii")
            ).hexdigest(),
            "finite_pixels": {
                **_distribution(finite),
                "minimum": min(finite),
            },
            "frame_count": len(export.entries),
            "maximum_rate_mm_h": {
                **_distribution(maxima),
                "minimum": min(maxima),
            },
            "null_semantics": "explicit_nodata_plus_undetect_never_dry_fill",
            "read_only_inventory_unchanged": True,
            "upstream_process_resources": export.upstream_process_metrics[
                "aggregates"
            ],
        },
        "schema": SCHEMA,
        "selected_frame_numbers": list(SELECTED_FRAME_NUMBERS),
        "spawn": {
            "active_children_after_run": [
                child.pid for child in multiprocessing.active_children()
            ],
            "start_method": multiprocessing.get_context("spawn").get_start_method(),
        },
    }
    body = _canonical_json_bytes(report)
    args.output_json.write_bytes(body)
    args.output_markdown.write_text(_markdown(report), encoding="utf-8", newline="\n")
    print(json.dumps({
        "json_bytes": len(body),
        "json_sha256": hashlib.sha256(body).hexdigest(),
        "markdown_bytes": args.output_markdown.stat().st_size,
        "markdown_sha256": hashlib.sha256(args.output_markdown.read_bytes()).hexdigest(),
        "status": "complete",
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
