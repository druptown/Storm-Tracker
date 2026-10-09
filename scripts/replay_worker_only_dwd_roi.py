"""Replay the sealed 36-frame DWD ROI through the worker-only tracker.

This command is deliberately offline.  It reads the immutable science export,
splits every 350x425 frame into two overlapping source-aligned tiles below the
worker-only 100,000-pixel cap, and prints one canonical JSON report.  It never
publishes MQTT, Home Assistant state, forecasts, probabilities or warnings.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import time

from storm_tracker_worker.science_shadow_dwd_roi_replay import (
    EXPECTED_FRAME_COUNT,
    EXPECTED_SEQUENCE_MANIFEST_SHA256,
    load_dwd_roi_science_evidence,
    load_dwd_roi_science_replay_export,
)
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
)


WORKLOAD_ID = "radar.dwd.radolan_rv"
REPORT_CONTRACT = "stv3-worker-only-dwd-roi-replay-report-v1"
TILES = (
    ("north", 0, 250),
    ("south", 175, 425),
)


def _sha(value: object) -> str:
    body = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return hashlib.sha256(body).hexdigest()


@dataclass(frozen=True, slots=True)
class ReplayTile:
    name: str
    start_row: int
    end_row: int
    grid: AnalysisGrid
    region_id: str
    region_lineage_id: str
    targets: tuple[WorkerOnlyProjectedTarget, ...]


def _tiles(source_grid: AnalysisGrid) -> tuple[ReplayTile, ...]:
    result: list[ReplayTile] = []
    for name, start_row, end_row in TILES:
        height = end_row - start_row
        grid = AnalysisGrid(
            grid_id=f"offline-dwd-roi-{name}-tile-v1",
            crs=source_grid.crs,
            origin_x_m=source_grid.origin_x_m,
            origin_y_m=(
                source_grid.origin_y_m
                - start_row * source_grid.y_resolution_m
            ),
            x_resolution_m=source_grid.x_resolution_m,
            y_resolution_m=source_grid.y_resolution_m,
            width=source_grid.width,
            height=height,
        )
        if grid.width * grid.height > MAX_ROI_PIXELS:
            raise RuntimeError("offline replay tile exceeds worker-only pixel cap")
        targets = tuple(
            WorkerOnlyProjectedTarget(
                target_id=f"offline.replay.dwd_roi.{name}.r{row}c{column}",
                x_m=(
                    grid.origin_x_m
                    + column * grid.width * grid.x_resolution_m / 4.0
                ),
                y_m=(
                    grid.origin_y_m
                    - row * grid.height * grid.y_resolution_m / 4.0
                ),
                location_accuracy_m=None,
            )
            for row in range(1, 4)
            for column in range(1, 4)
        )
        result.append(ReplayTile(
            name=name,
            start_row=start_row,
            end_row=end_row,
            grid=grid,
            region_id="offline-replay-region:" + _sha({"tile": name}),
            region_lineage_id="tracking-region:" + _sha({
                "contract": REPORT_CONTRACT,
                "tile": name,
                "workload_id": WORKLOAD_ID,
            }),
            targets=targets,
        ))
    return tuple(result)


def _batch(evidence, tiles: tuple[ReplayTile, ...]) -> WorkerOnlyTrackingBatch:
    source_frame = evidence.normalized_frame
    dispositions: list[WorkerOnlyTrackingDisposition] = []
    for tile in tiles:
        frame = NormalizedRasterFrame(
            frame_id="worker-only-replay-frame:" + _sha({
                "source_frame_id": source_frame.frame_id,
                "tile": tile.name,
            }),
            valid_at=source_frame.valid_at,
            source=f"{WORKLOAD_ID}:{tile.region_lineage_id}",
            quantity=source_frame.quantity,
            unit=source_frame.unit,
            grid=tile.grid,
            values=tuple(
                tuple(row)
                for row in source_frame.values[tile.start_row:tile.end_row]
            ),
        )
        covered_radius_km = min(
            tile.grid.width * tile.grid.x_resolution_m,
            tile.grid.height * tile.grid.y_resolution_m,
        ) / 2_000.0
        candidate = WorkerOnlyTrackingFrameCandidate(
            workload_id=WORKLOAD_ID,
            region_id=tile.region_id,
            region_lineage_id=tile.region_lineage_id,
            target_ids=tuple(target.target_id for target in tile.targets),
            capture_id="offline-replay-capture:" + evidence.canonical_run_id,
            source_candidate_id=evidence.evidence_candidate_id,
            source_candidate_content_sha256=evidence.evidence_content_sha256,
            requested_radius_km=covered_radius_km,
            covered_radius_km=covered_radius_km,
            coverage_complete=True,
            frame=frame,
            projected_targets=tile.targets,
        )
        dispositions.append(WorkerOnlyTrackingDisposition(
            workload_id=WORKLOAD_ID,
            region_id=tile.region_id,
            state="candidate",
            reason_code="sealed_offline_replay_tile_ready",
            candidate=candidate,
        ))
    return WorkerOnlyTrackingBatch(
        workload_id=WORKLOAD_ID,
        source_candidate_id=evidence.evidence_candidate_id,
        dispositions=tuple(sorted(
            dispositions,
            key=lambda item: (item.workload_id, item.region_id),
        )),
    )


def run(export_root: Path) -> dict[str, object]:
    export_root = export_root.resolve(strict=True)
    manifest = (
        export_root
        / "manifests"
        / f"{EXPECTED_SEQUENCE_MANIFEST_SHA256}.json"
    ).resolve(strict=True)
    started = time.perf_counter()
    export = load_dwd_roi_science_replay_export(export_root, manifest)
    first_evidence = load_dwd_roi_science_evidence(export, export.entries[0])
    tiles = _tiles(first_evidence.normalized_frame.grid)
    runtime = WorkerOnlyCellTrackingRuntime()
    frames: list[dict[str, object]] = []
    peak_active_tracks = 0
    peak_confirmed_tracks = 0
    peak_motion_ready_tracks = 0
    arrival_candidate_events = 0
    future_arrival_candidate_events = 0
    future_entry_seconds: list[float] = []
    exit_seconds: list[float] = []
    for entry in export.entries:
        evidence = load_dwd_roi_science_evidence(export, entry)
        update = runtime.process_batch(_batch(evidence, tiles))
        peak_active_tracks = max(peak_active_tracks, update.active_tracks)
        peak_confirmed_tracks = max(peak_confirmed_tracks, update.tracked_tracks)
        peak_motion_ready_tracks = max(
            peak_motion_ready_tracks,
            update.motion_ready_tracks,
        )
        arrival_candidate_events += update.shadow_arrival_candidates
        future_arrival_candidate_events += (
            update.shadow_future_arrival_candidates
        )
        current_arrivals = runtime.shadow_arrivals()
        future_entry_seconds.extend(
            item.entry_seconds
            for item in current_arrivals
            if item.passage_state == "future_intersection"
        )
        exit_seconds.extend(item.exit_seconds for item in current_arrivals)
        frames.append({
            "active_tracks": update.active_tracks,
            "advanced_regions": update.advanced_regions,
            "confirmed_tracks": update.tracked_tracks,
            "frame_number": entry.frame_number,
            "future_arrival_candidates": (
                update.shadow_future_arrival_candidates
            ),
            "indeterminate_regions": update.indeterminate_regions,
            "motion_ready_tracks": update.motion_ready_tracks,
            "shadow_arrival_candidates": update.shadow_arrival_candidates,
            "valid_at": entry.valid_at,
        })
    diagnostics = runtime.diagnostics()
    elapsed = time.perf_counter() - started
    return {
        "admission_status": ADMISSION_STATUS,
        "contract": REPORT_CONTRACT,
        "elapsed_seconds": elapsed,
        "forecast_generated": False,
        "frame_count": len(frames),
        "frames": frames,
        "future_arrival_candidate_events": future_arrival_candidate_events,
        "future_entry_seconds_max": (
            max(future_entry_seconds) if future_entry_seconds else None
        ),
        "future_entry_seconds_min": (
            min(future_entry_seconds) if future_entry_seconds else None
        ),
        "meteorological_admission_granted": False,
        "peak_active_tracks": peak_active_tracks,
        "peak_confirmed_tracks": peak_confirmed_tracks,
        "peak_motion_ready_tracks": peak_motion_ready_tracks,
        "passage_exit_seconds_max": max(exit_seconds) if exit_seconds else None,
        "region_states": diagnostics["region_states"],
        "sequence_id": export.sequence_id,
        "shadow_arrival_candidate_events": arrival_candidate_events,
        "source_frame_count_expected": EXPECTED_FRAME_COUNT,
        "tile_count": len(tiles),
        "tile_pixels": {
            tile.name: tile.grid.width * tile.grid.height for tile in tiles
        },
        "total_fixed_geometry_probe_targets": sum(
            len(tile.targets) for tile in tiles
        ),
        "warning_generated": False,
        "workload_id": WORKLOAD_ID,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--export-root",
        type=Path,
        required=True,
        help="absolute path to the sealed 36-frame DWD ROI export",
    )
    args = parser.parse_args()
    report = run(args.export_root)
    print(json.dumps(
        report,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
