"""Capture and replay a bounded real Met Office sequence on the local machine.

The capture lane downloads the exact public-S3 listings and the requested last
consecutive ODIM RATE objects, validates the strict native profile, and stores
content-addressed bytes plus a canonical manifest.  The replay lane is offline:
it re-verifies every byte, tiles the full native grid below the worker-only
100,000-pixel limit, and feeds those tiles through the shadow cell tracker.

Nothing in this command publishes MQTT or Home Assistant state, and nothing it
produces grants forecast, warning, calibration or operational admission.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import io
import json
import os
from pathlib import Path
import time
from typing import Any

import h5py
import numpy as np

from storm_tracker_worker import met_office_native as native
from storm_tracker_worker.met_office_rate_quantitative import (
    NATIVE_MAXIMUM_Y_METRES,
    NATIVE_MINIMUM_X_METRES,
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


CAPTURE_CONTRACT = "stv3-met-office-real-sequence-capture-v1"
REPORT_CONTRACT = "stv3-worker-only-met-office-real-sequence-replay-report-v1"
WORKLOAD_ID = "radar.metoffice.uk_rate"
TILE_ROWS = 250
TILE_COLUMNS = 350
MINIMUM_SEQUENCE_FRAMES = 2
MAXIMUM_SEQUENCE_FRAMES = 8


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha(value: object) -> str:
    return _sha_bytes(_canonical(value))


def _write_immutable(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != payload:
            raise RuntimeError(f"existing content-addressed object differs: {path}")
        return
    staging = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with staging.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(staging, path)
    finally:
        if staging.exists():
            staging.unlink()


def _object_mapping(
    selected: native.MetOfficeObject,
    *,
    payload: bytes,
    identity: native.MetOfficeOdimIdentity,
) -> dict[str, object]:
    return {
        "checksum_algorithm": selected.checksum_algorithm,
        "checksum_type": selected.checksum_type,
        "etag": selected.etag,
        "input_window_end": identity.input_window_end,
        "input_window_start": identity.input_window_start,
        "key": selected.key,
        "last_modified": selected.last_modified,
        "product_timestamp": selected.product_timestamp,
        "sha256": _sha_bytes(payload),
        "size_bytes": selected.size_bytes,
        "storage_class": selected.storage_class,
    }


def capture_latest_sequence(root: Path, frame_count: int) -> Path:
    if not MINIMUM_SEQUENCE_FRAMES <= frame_count <= MAXIMUM_SEQUENCE_FRAMES:
        raise ValueError(
            f"capture frame count must be {MINIMUM_SEQUENCE_FRAMES}.."
            f"{MAXIMUM_SEQUENCE_FRAMES}"
        )
    root = root.resolve()
    listing_receipts: list[dict[str, object]] = []
    groups: list[tuple[native.MetOfficeObject, ...]] = []
    deadline = time.monotonic() + native.CAPTURE_TOTAL_DEADLINE_SECONDS
    with native._capture_deadline(deadline):  # noqa: SLF001 - verifier reuses contract
        for prefix in native._listing_prefixes():  # noqa: SLF001
            response = native._fetch_listing(prefix)  # noqa: SLF001
            sha256 = _sha_bytes(response.payload)
            _write_immutable(root / "listings" / f"{sha256}.xml", response.payload)
            listing_receipts.append({
                "prefix": prefix,
                "received_at": response.received_at,
                "sha256": sha256,
                "size_bytes": len(response.payload),
            })
            groups.append(native._parse_listing(  # noqa: SLF001
                response.payload,
                expected_prefix=prefix,
            ))
        by_revision = {
            (item.key, item.etag): item
            for group in groups
            for item in group
        }
        ordered = sorted(
            by_revision.values(),
            key=lambda item: (item.product_timestamp, item.key, item.etag),
        )
        selected_objects = ordered[-frame_count:]
        if len(selected_objects) != frame_count:
            raise RuntimeError("public listing contains too few consecutive objects")
        intervals = [
            current.product_timestamp - previous.product_timestamp
            for previous, current in zip(
                selected_objects[:-1],
                selected_objects[1:],
                strict=True,
            )
        ]
        if any(value <= 0.0 or value > 1_800.0 for value in intervals):
            raise RuntimeError("selected Met Office objects are not a bounded sequence")

        objects: list[dict[str, object]] = []
        for selected in selected_objects:
            response = native._fetch_object(selected)  # noqa: SLF001
            identity = native._inspect_odim(  # noqa: SLF001
                response.payload,
                selected=selected,
            )
            sha256 = _sha_bytes(response.payload)
            _write_immutable(root / "objects" / f"{sha256}.h5", response.payload)
            objects.append(_object_mapping(
                selected,
                payload=response.payload,
                identity=identity,
            ))

    sequence_core = {
        "listing_sha256": [item["sha256"] for item in listing_receipts],
        "objects": [
            {
                "etag": item["etag"],
                "key": item["key"],
                "sha256": item["sha256"],
            }
            for item in objects
        ],
    }
    sequence_id = _sha(sequence_core)
    manifest = {
        "captured_at": time.time(),
        "contract": CAPTURE_CONTRACT,
        "listing_receipts": listing_receipts,
        "objects": objects,
        "sequence_id": sequence_id,
        "source_origin": native.S3_ORIGIN,
    }
    manifest_path = root / "manifests" / f"{sequence_id}.json"
    if manifest_path.exists():
        # The first immutable retrieval receipt remains authoritative when the
        # same listings and object revisions are captured again.
        _load_manifest(root, manifest_path)
        return manifest_path
    _write_immutable(manifest_path, _canonical(manifest) + b"\n")
    return manifest_path


def _exact_mapping(
    value: object,
    keys: set[str],
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise RuntimeError(f"{label} has an invalid schema")
    return value


def _load_manifest(root: Path, manifest_path: Path) -> dict[str, Any]:
    root = root.resolve(strict=True)
    manifest_path = manifest_path.resolve(strict=True)
    if manifest_path.parent != (root / "manifests").resolve(strict=True):
        raise RuntimeError("manifest must be inside the capture root")
    manifest = _exact_mapping(
        json.loads(manifest_path.read_text(encoding="ascii")),
        {
            "captured_at",
            "contract",
            "listing_receipts",
            "objects",
            "sequence_id",
            "source_origin",
        },
        "manifest",
    )
    if (
        manifest["contract"] != CAPTURE_CONTRACT
        or manifest["source_origin"] != native.S3_ORIGIN
    ):
        raise RuntimeError("manifest authority is invalid")
    listing_receipts = manifest["listing_receipts"]
    objects = manifest["objects"]
    if not isinstance(listing_receipts, list) or len(listing_receipts) != 2:
        raise RuntimeError("manifest must bind two exact listings")
    if (
        not isinstance(objects, list)
        or not MINIMUM_SEQUENCE_FRAMES
        <= len(objects)
        <= MAXIMUM_SEQUENCE_FRAMES
    ):
        raise RuntimeError("manifest sequence length is invalid")
    sequence_core = {
        "listing_sha256": [item["sha256"] for item in listing_receipts],
        "objects": [
            {
                "etag": item["etag"],
                "key": item["key"],
                "sha256": item["sha256"],
            }
            for item in objects
        ],
    }
    if manifest["sequence_id"] != _sha(sequence_core):
        raise RuntimeError("manifest sequence id is invalid")
    if manifest_path.name != f"{manifest['sequence_id']}.json":
        raise RuntimeError("manifest filename is not bound to its sequence id")
    return manifest


def _selected_from_mapping(value: object) -> native.MetOfficeObject:
    item = _exact_mapping(
        value,
        {
            "checksum_algorithm",
            "checksum_type",
            "etag",
            "input_window_end",
            "input_window_start",
            "key",
            "last_modified",
            "product_timestamp",
            "sha256",
            "size_bytes",
            "storage_class",
        },
        "manifest object",
    )
    return native.MetOfficeObject(
        key=item["key"],
        product_timestamp=float(item["product_timestamp"]),
        last_modified=float(item["last_modified"]),
        etag=item["etag"],
        size_bytes=int(item["size_bytes"]),
        storage_class=item["storage_class"],
        checksum_algorithm=item["checksum_algorithm"],
        checksum_type=item["checksum_type"],
    )


@dataclass(frozen=True, slots=True)
class ReplayTile:
    name: str
    start_row: int
    end_row: int
    start_column: int
    end_column: int
    grid: AnalysisGrid
    region_id: str
    region_lineage_id: str
    targets: tuple[WorkerOnlyProjectedTarget, ...]


def _tiles() -> tuple[ReplayTile, ...]:
    result: list[ReplayTile] = []
    for start_row in range(0, native.GRID_ROWS, TILE_ROWS):
        end_row = min(native.GRID_ROWS, start_row + TILE_ROWS)
        for start_column in range(0, native.GRID_COLUMNS, TILE_COLUMNS):
            end_column = min(native.GRID_COLUMNS, start_column + TILE_COLUMNS)
            name = f"r{start_row:04d}-c{start_column:04d}"
            grid = AnalysisGrid(
                grid_id=f"offline-met-office-{name}-tile-v1",
                crs=native.GRID_PROJECTION,
                origin_x_m=(
                    NATIVE_MINIMUM_X_METRES
                    + start_column * native.GRID_SCALE_METRES
                ),
                origin_y_m=(
                    NATIVE_MAXIMUM_Y_METRES
                    - start_row * native.GRID_SCALE_METRES
                ),
                x_resolution_m=native.GRID_SCALE_METRES,
                y_resolution_m=native.GRID_SCALE_METRES,
                width=end_column - start_column,
                height=end_row - start_row,
            )
            if grid.width * grid.height > MAX_ROI_PIXELS:
                raise RuntimeError("Met Office replay tile exceeds worker-only cap")
            targets = tuple(
                WorkerOnlyProjectedTarget(
                    target_id=(
                        f"offline.replay.met_office.{name}.r{row}c{column}"
                    ),
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
            lineage = "tracking-region:" + _sha({
                "contract": REPORT_CONTRACT,
                "tile": name,
                "workload_id": WORKLOAD_ID,
            })
            result.append(ReplayTile(
                name=name,
                start_row=start_row,
                end_row=end_row,
                start_column=start_column,
                end_column=end_column,
                grid=grid,
                region_id="offline-replay-region:" + _sha({"tile": name}),
                region_lineage_id=lineage,
                targets=targets,
            ))
    return tuple(result)


def _validated_raster(
    payload: bytes,
    selected: native.MetOfficeObject,
    manifest_object: dict[str, Any],
) -> np.ndarray:
    if len(payload) != selected.size_bytes:
        raise RuntimeError("Met Office object size differs from manifest")
    if _sha_bytes(payload) != manifest_object["sha256"]:
        raise RuntimeError("Met Office object hash differs from manifest")
    identity = native._inspect_odim(payload, selected=selected)  # noqa: SLF001
    if (
        identity.input_window_start
        != float(manifest_object["input_window_start"])
        or identity.input_window_end
        != float(manifest_object["input_window_end"])
    ):
        raise RuntimeError("Met Office ODIM time window differs from manifest")
    with h5py.File(io.BytesIO(payload), "r") as root:
        raster = np.asarray(root["dataset1/data1/data"][:], dtype=np.float32)
    if raster.shape != (native.GRID_ROWS, native.GRID_COLUMNS):
        raise RuntimeError("Met Office raster shape differs from native profile")
    valid = (raster != -1.0) & (raster != 0.0)
    if np.any(~np.isfinite(raster)) or np.any(raster[valid] < 0.0):
        raise RuntimeError("Met Office raster contains invalid values")
    raster.setflags(write=False)
    return raster


def _batch(
    *,
    raster: np.ndarray,
    manifest_object: dict[str, Any],
    selected: native.MetOfficeObject,
    tiles: tuple[ReplayTile, ...],
) -> WorkerOnlyTrackingBatch:
    dispositions: list[WorkerOnlyTrackingDisposition] = []
    for tile in tiles:
        crop = raster[
            tile.start_row:tile.end_row,
            tile.start_column:tile.end_column,
        ]
        values = tuple(
            tuple(
                None if value == -1.0 or value == 0.0 else float(value)
                for value in row
            )
            for row in crop
        )
        frame = NormalizedRasterFrame(
            frame_id="worker-only-replay-frame:" + _sha({
                "object_sha256": manifest_object["sha256"],
                "tile": tile.name,
            }),
            valid_at=selected.product_timestamp,
            source=f"{WORKLOAD_ID}:{tile.region_lineage_id}",
            quantity="surface_precipitation_rate_estimate",
            unit="mm/h",
            grid=tile.grid,
            values=values,
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
            capture_id="offline-replay-capture:" + manifest_object["sha256"],
            source_candidate_id="offline-replay-source:" + manifest_object["sha256"],
            source_candidate_content_sha256=manifest_object["sha256"],
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
            reason_code="sealed_real_replay_tile_ready",
            candidate=candidate,
        ))
    return WorkerOnlyTrackingBatch(
        workload_id=WORKLOAD_ID,
        source_candidate_id="offline-replay-source:" + manifest_object["sha256"],
        dispositions=tuple(sorted(
            dispositions,
            key=lambda item: (item.workload_id, item.region_id),
        )),
    )


def replay(root: Path, manifest_path: Path) -> dict[str, object]:
    started = time.perf_counter()
    root = root.resolve(strict=True)
    manifest = _load_manifest(root, manifest_path)
    tiles = _tiles()
    runtime = WorkerOnlyCellTrackingRuntime()
    frame_reports: list[dict[str, object]] = []
    peak_active_tracks = 0
    peak_confirmed_tracks = 0
    peak_motion_ready_tracks = 0
    arrival_candidate_events = 0
    future_arrival_candidate_events = 0
    future_entry_seconds: list[float] = []
    exit_seconds: list[float] = []
    previous_timestamp: float | None = None
    for manifest_value in manifest["objects"]:
        manifest_object = _exact_mapping(
            manifest_value,
            {
                "checksum_algorithm",
                "checksum_type",
                "etag",
                "input_window_end",
                "input_window_start",
                "key",
                "last_modified",
                "product_timestamp",
                "sha256",
                "size_bytes",
                "storage_class",
            },
            "manifest object",
        )
        selected = _selected_from_mapping(manifest_object)
        if previous_timestamp is not None and not (
            0.0 < selected.product_timestamp - previous_timestamp <= 1_800.0
        ):
            raise RuntimeError("manifest objects are not an ordered sequence")
        previous_timestamp = selected.product_timestamp
        payload = (
            root / "objects" / f"{manifest_object['sha256']}.h5"
        ).read_bytes()
        raster = _validated_raster(payload, selected, manifest_object)
        update = runtime.process_batch(_batch(
            raster=raster,
            manifest_object=manifest_object,
            selected=selected,
            tiles=tiles,
        ))
        peak_active_tracks = max(peak_active_tracks, update.active_tracks)
        peak_confirmed_tracks = max(peak_confirmed_tracks, update.tracked_tracks)
        peak_motion_ready_tracks = max(
            peak_motion_ready_tracks,
            update.motion_ready_tracks,
        )
        arrival_candidate_events += update.shadow_arrival_candidates
        future_arrival_candidate_events += update.shadow_future_arrival_candidates
        arrivals = runtime.shadow_arrivals()
        future_entry_seconds.extend(
            item.entry_seconds
            for item in arrivals
            if item.passage_state == "future_intersection"
        )
        exit_seconds.extend(item.exit_seconds for item in arrivals)
        frame_reports.append({
            "active_tracks": update.active_tracks,
            "advanced_regions": update.advanced_regions,
            "confirmed_tracks": update.tracked_tracks,
            "future_arrival_candidates": update.shadow_future_arrival_candidates,
            "indeterminate_regions": update.indeterminate_regions,
            "key": selected.key,
            "motion_ready_tracks": update.motion_ready_tracks,
            "product_timestamp": selected.product_timestamp,
            "sha256": manifest_object["sha256"],
            "shadow_arrival_candidates": update.shadow_arrival_candidates,
        })
    diagnostics = runtime.diagnostics()
    return {
        "admission_status": ADMISSION_STATUS,
        "contract": REPORT_CONTRACT,
        "elapsed_seconds": time.perf_counter() - started,
        "forecast_generated": False,
        "frame_count": len(frame_reports),
        "frames": frame_reports,
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
        "sequence_id": manifest["sequence_id"],
        "shadow_arrival_candidate_events": arrival_candidate_events,
        "tile_count": len(tiles),
        "tile_pixels_max": max(
            tile.grid.width * tile.grid.height for tile in tiles
        ),
        "total_fixed_geometry_probe_targets": sum(
            len(tile.targets) for tile in tiles
        ),
        "warning_generated": False,
        "workload_id": WORKLOAD_ID,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--capture-root", type=Path, required=True)
    parser.add_argument(
        "--capture-latest",
        type=int,
        choices=range(MINIMUM_SEQUENCE_FRAMES, MAXIMUM_SEQUENCE_FRAMES + 1),
        metavar=f"{{{MINIMUM_SEQUENCE_FRAMES}..{MAXIMUM_SEQUENCE_FRAMES}}}",
    )
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    if args.capture_latest is not None and args.manifest is not None:
        parser.error("choose --capture-latest or --manifest, not both")
    if args.capture_latest is None and args.manifest is None:
        parser.error("one of --capture-latest or --manifest is required")
    manifest = (
        capture_latest_sequence(args.capture_root, args.capture_latest)
        if args.capture_latest is not None
        else args.manifest
    )
    report = replay(args.capture_root, manifest)
    print(_canonical(report).decode("ascii"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
