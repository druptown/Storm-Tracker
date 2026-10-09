"""Build the fixed 36-frame 2-km Germany ROI under its measured 1-GiB cap."""
from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import statistics
import sys
import threading
import time
from typing import Any

from storm_tracker_worker.dwd_rv_completion_process_isolation import (
    DwdRvCompletionIsolationLimits,
)
from storm_tracker_worker.dwd_rv_observation_replay import (
    build_dwd_rv_observation_replay,
    receipt_summary,
)
from storm_tracker_worker.dwd_rv_science_sequence import (
    PROCESS_METRICS_CONTRACT,
    inventory_source_archives,
)
from storm_tracker_worker.hard_decoder_isolation import _canonical_json_bytes
from storm_tracker_worker.science_shadow_dwd_roi_export import (
    SOURCE_FRAME_COUNT,
    SOURCE_MANIFEST_SHA256,
    authorize_dwd_germany_roi_full_replay,
    dwd_germany_roi_profile,
    seal_dwd_germany_roi_run_manifest,
    seal_dwd_germany_roi_selection_manifest,
)


CALIBRATION_MANIFEST_SHA256 = (
    "694a127ff22a3d57b918d7dcffcf14a69a1460b3f4a2891d74a71edf7f0c9b7b"
)
SELECTION_MANIFEST_SHA256 = (
    "dba2157c13b779ad42bda247c24e34f669dd9fdd27d04aee6f1ba4d90b710c31"
)
FULL_REPLAY_RSS_CAP_BYTES = 1_073_741_824
FULL_REPLAY_CPU_SECONDS = 120.0
FULL_REPLAY_WALL_SECONDS = 180.0
CHECKPOINTS = frozenset((12, 24, 36))


class _ProcessMemoryCounters(ctypes.Structure):
    _fields_ = [
        ("cb", wintypes.DWORD),
        ("PageFaultCount", wintypes.DWORD),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


def _process_memory(pid: int) -> tuple[int, int] | None:
    if sys.platform == "win32":
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        handle = kernel.OpenProcess(0x1000 | 0x0010, False, pid)
        if not handle:
            return None
        try:
            counters = _ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(counters)
            if not psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(counters), counters.cb
            ):
                return None
            return int(counters.WorkingSetSize), int(counters.PeakWorkingSetSize)
        finally:
            kernel.CloseHandle(handle)
    if sys.platform.startswith("linux"):
        try:
            current = 0
            peak = 0
            for line in Path(f"/proc/{pid}/status").read_text("ascii").splitlines():
                if line.startswith("VmRSS:"):
                    current = int(line.split()[1]) * 1024
                elif line.startswith("VmHWM:"):
                    peak = int(line.split()[1]) * 1024
            return (current, peak or current) if current else None
        except (FileNotFoundError, PermissionError, ValueError, IndexError):
            return None
    return None


class _ParentFrameMonitor:
    def __init__(self, export_root: Path) -> None:
        self._objects = export_root / "objects"
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="dwd-roi-full-parent-frame-monitor",
            daemon=True,
        )
        self.rows: list[dict[str, int | float]] = []
        self.total_peak_rss_bytes = 0
        self._error: BaseException | None = None

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=3.0)
        if self._thread.is_alive():
            raise RuntimeError("parent frame monitor did not terminate")
        if self._error is not None:
            raise RuntimeError("parent frame monitor failed") from self._error

    def _run(self) -> None:
        frame_started = time.perf_counter()
        cpu_started = time.process_time()
        frame_peak = 0
        last_count = 0
        try:
            while True:
                measurement = _process_memory(os.getpid())
                if measurement is not None:
                    current, lifetime_peak = measurement
                    frame_peak = max(frame_peak, current)
                    self.total_peak_rss_bytes = max(
                        self.total_peak_rss_bytes, current, lifetime_peak
                    )
                count = 0
                if self._objects.is_dir():
                    count = sum(
                        1 for path in self._objects.iterdir()
                        if path.is_file() and not path.is_symlink()
                        and path.suffix == ".json"
                    )
                if count < last_count or count > SOURCE_FRAME_COUNT:
                    raise RuntimeError("evidence-object accounting regressed")
                if count > last_count:
                    if count != last_count + 1:
                        raise RuntimeError("frame monitor missed an object boundary")
                    now = time.perf_counter()
                    cpu_now = time.process_time()
                    self.rows.append({
                        "frame_number": count - 1,
                        "parent_cpu_seconds": cpu_now - cpu_started,
                        "parent_peak_rss_bytes": frame_peak,
                        "parent_wall_seconds": now - frame_started,
                    })
                    last_count = count
                    frame_started = now
                    cpu_started = cpu_now
                    frame_peak = current if measurement is not None else 0
                    if count in CHECKPOINTS:
                        print(json.dumps({
                            "active_decoder_children_observed": len(
                                multiprocessing.active_children()
                            ),
                            "completed_frames": count,
                            "parent_lifetime_peak_rss_bytes": (
                                self.total_peak_rss_bytes
                            ),
                            "status": "full_replay_checkpoint",
                        }, sort_keys=True), flush=True)
                if self._stop.wait(0.005):
                    if count != SOURCE_FRAME_COUNT:
                        raise RuntimeError("full replay stopped before 36 objects")
                    break
        except BaseException as exc:  # relayed after deterministic join
            self._error = exc


def _distribution(values: list[int | float]) -> dict[str, float | int]:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise ValueError("resource distribution is empty")
    return {
        "count": len(ordered),
        "max": max(ordered),
        "p50": statistics.median(ordered),
        "p95": ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)],
    }


def _load_metrics(receipt) -> dict[str, Any]:
    metrics = receipt.process_metrics
    body = metrics.manifest_path.read_bytes()
    if (
        len(body) != metrics.manifest_bytes
        or hashlib.sha256(body).hexdigest() != metrics.manifest_sha256
    ):
        raise RuntimeError("process metrics manifest bytes differ")
    value = json.loads(body.decode("ascii"))
    if (
        _canonical_json_bytes(value) != body
        or value.get("contract") != PROCESS_METRICS_CONTRACT
        or value.get("frame_count") != SOURCE_FRAME_COUNT
        or value.get("sequence_id") != receipt.sequence.sequence_id
        or value.get("source_manifest_sha256") != SOURCE_MANIFEST_SHA256
        or len(value.get("entries", ())) != SOURCE_FRAME_COUNT
    ):
        raise RuntimeError("process metrics manifest contract differs")
    return value


def _absolute(value: Path, *, must_exist: bool) -> Path:
    path = value.resolve(strict=must_exist)
    if not path.is_absolute() or path.is_symlink():
        raise ValueError("full replay path is unsafe")
    return path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--export-root", type=Path, required=True)
    parser.add_argument("--authority-root", type=Path, required=True)
    parser.add_argument("--preflight-only", action="store_true")
    return parser


def main() -> int:
    args = _parser().parse_args()
    source = _absolute(args.source, must_exist=True)
    work = _absolute(args.work_root, must_exist=False)
    export = _absolute(args.export_root, must_exist=False)
    authority_root = _absolute(args.authority_root, must_exist=True)
    if work.exists() or export.exists():
        raise ValueError("full replay requires fresh work and export roots")

    selection = seal_dwd_germany_roi_selection_manifest(authority_root)
    if selection.manifest_sha256 != SELECTION_MANIFEST_SHA256:
        raise RuntimeError("fixed selection manifest differs")
    source_manifest = inventory_source_archives(
        source, authority_root, expected_frame_count=SOURCE_FRAME_COUNT
    )
    if source_manifest.manifest_sha256 != SOURCE_MANIFEST_SHA256:
        raise RuntimeError("fixed 36-frame source corpus differs")
    calibration_path = (
        authority_root / "manifests" / f"{CALIBRATION_MANIFEST_SHA256}.json"
    )
    replay_authority = authorize_dwd_germany_roi_full_replay(
        selection=selection,
        calibration_manifest_path=calibration_path,
        expected_calibration_sha256=CALIBRATION_MANIFEST_SHA256,
    )
    if replay_authority.replay_rss_cap_bytes != FULL_REPLAY_RSS_CAP_BYTES:
        raise RuntimeError("calibrated full replay cap differs from fixed 1 GiB")
    limits = DwdRvCompletionIsolationLimits(
        rss_bytes=replay_authority.replay_rss_cap_bytes,
        cpu_seconds=FULL_REPLAY_CPU_SECONDS,
        wall_seconds=FULL_REPLAY_WALL_SECONDS,
    )
    if args.preflight_only:
        print(json.dumps({
            "active_decoder_children": len(multiprocessing.active_children()),
            "calibration_manifest_sha256": CALIBRATION_MANIFEST_SHA256,
            "isolation_limits_sha256": limits.fingerprint,
            "profile_sha256": dwd_germany_roi_profile().fingerprint,
            "replay_rss_cap_bytes": replay_authority.replay_rss_cap_bytes,
            "selection_manifest_sha256": selection.manifest_sha256,
            "source_manifest_sha256": source_manifest.manifest_sha256,
            "status": "full_replay_preflight_only_no_child_started",
        }, sort_keys=True))
        return 0

    monitor = _ParentFrameMonitor(export)
    baseline_memory = _process_memory(os.getpid())
    parent_cpu_started = time.process_time()
    wall_started = time.perf_counter()
    monitor.start()
    try:
        receipt = build_dwd_rv_observation_replay(
            source_directory=source,
            work_root=work,
            export_root=export,
            isolation_limits=limits,
            profile=dwd_germany_roi_profile(),
            expected_frame_count=SOURCE_FRAME_COUNT,
        )
    finally:
        total_wall_seconds = time.perf_counter() - wall_started
        total_parent_cpu_seconds = time.process_time() - parent_cpu_started
        monitor.stop()
    if (
        receipt.active_decoder_children_after_run != 0
        or len(receipt.evidence_records) != SOURCE_FRAME_COUNT
        or len(monitor.rows) != SOURCE_FRAME_COUNT
    ):
        raise RuntimeError("full replay frame or child accounting differs")
    metrics_document = _load_metrics(receipt)
    child_rows = metrics_document["entries"]
    per_frame = []
    for frame_number, (parent, child) in enumerate(zip(monitor.rows, child_rows)):
        if (
            parent["frame_number"] != frame_number
            or child["frame_number"] != frame_number
        ):
            raise RuntimeError("parent/child frame metric alignment differs")
        per_frame.append({
            **parent,
            "child_cpu_seconds": child["child_cpu_seconds"],
            "child_elapsed_seconds": child["elapsed_seconds"],
            "child_peak_boundary_bytes": child["child_peak_boundary_bytes"],
            "child_peak_rss_bytes": child["child_peak_rss_bytes"],
            "valid_at": child["valid_at"],
        })
    summary = receipt_summary(receipt)
    summary.update({
        "child_resource_aggregates": metrics_document["aggregates"],
        "configured_rss_cap_bytes": replay_authority.replay_rss_cap_bytes,
        "parent_resource_aggregates": {
            "cpu_seconds": _distribution([
                row["parent_cpu_seconds"] for row in monitor.rows
            ]),
            "peak_rss_bytes": _distribution([
                row["parent_peak_rss_bytes"] for row in monitor.rows
            ]),
            "wall_seconds": _distribution([
                row["parent_wall_seconds"] for row in monitor.rows
            ]),
        },
        "parent_total_cpu_seconds": total_parent_cpu_seconds,
        "parent_total_peak_rss_bytes": monitor.total_peak_rss_bytes,
        "parent_total_wall_seconds": total_wall_seconds,
        "parent_initial_rss_bytes": (
            None if baseline_memory is None else baseline_memory[0]
        ),
        "per_frame_resource_evidence": per_frame,
        "spatial_aggregation_is_truth": False,
    })
    # Re-read and hash every source tar after the replay; the idempotent source
    # manifest must be byte-identical.  This does not retry a failed frame.
    after_source = inventory_source_archives(
        source, authority_root, expected_frame_count=SOURCE_FRAME_COUNT
    )
    if after_source.manifest_sha256 != source_manifest.manifest_sha256:
        raise RuntimeError("source corpus changed during full replay")
    run_manifest = seal_dwd_germany_roi_run_manifest(
        authority_root,
        selection=selection,
        calibration=type(selection)(
            contract="stv3-dwd-rv-germany-roi-calibration-v1",
            manifest_sha256=CALIBRATION_MANIFEST_SHA256,
            manifest_bytes=calibration_path.stat().st_size,
            manifest_path=calibration_path,
            document=json.loads(calibration_path.read_text("ascii")),
        ),
        replay_summary=summary,
    )
    print(json.dumps({
        "active_decoder_children_after_run": 0,
        "metrics_manifest_sha256": receipt.process_metrics.manifest_sha256,
        "parent_resource_aggregates": summary["parent_resource_aggregates"],
        "profile_sha256": receipt.profile_sha256,
        "run_manifest_sha256": run_manifest.manifest_sha256,
        "sequence_id": receipt.sequence.sequence_id,
        "sequence_manifest_sha256": receipt.sequence.manifest_sha256,
        "source_manifest_sha256": receipt.source_manifest.manifest_sha256,
        "status": "full_replay_complete_shadow_only",
    }, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    multiprocessing.freeze_support()
    raise SystemExit(main())
