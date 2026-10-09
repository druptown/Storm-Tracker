"""Build exactly one hard-isolated DWD Germany-ROI calibration frame.

This guarded offline command never fetches, publishes, admits, serves or wires
anything.  It reads the fixed first official +000 archive, uses fresh stores,
and emits only path-free content hashes and measured resource evidence.
"""
from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
import json
import multiprocessing
import os
from pathlib import Path
import sys
import threading
import time

from storm_tracker_worker.dwd_rv_completion_process_isolation import (
    DwdRvCompletionIsolationLimits,
)
from storm_tracker_worker.dwd_rv_observation_replay import (
    build_dwd_rv_observation_replay,
    receipt_summary,
)
from storm_tracker_worker.science_shadow_dwd_roi_export import (
    CALIBRATION_ARCHIVE_SHA256,
    CALIBRATION_RSS_CAP_BYTES,
    CALIBRATION_SOURCE_MANIFEST_SHA256,
    derive_dwd_germany_roi_replay_cap,
    dwd_germany_roi_profile,
    seal_dwd_germany_roi_calibration_manifest,
    seal_dwd_germany_roi_selection_manifest,
    verify_dwd_germany_roi_calibration_source,
)


CALIBRATION_CPU_SECONDS = 120.0
CALIBRATION_WALL_SECONDS = 180.0
EXPECTED_LIMITS_SHA256 = (
    "b6607e65521db7c9bda92a18c9dfd714daefaecf62dd610c1fa6b5921a0ede90"
)


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
    """Return current and lifetime-peak RSS without third-party packages."""

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


def _sample_parent(stop: threading.Event, peaks: list[int]) -> None:
    while not stop.wait(0.005):
        measurement = _process_memory(os.getpid())
        if measurement is not None:
            peaks.append(max(measurement))


def _absolute(value: Path, *, must_exist: bool) -> Path:
    path = value.resolve(strict=must_exist)
    if not path.is_absolute() or path.is_symlink():
        raise ValueError("calibration path is unsafe")
    return path


def _assert_fresh(path: Path, label: str) -> None:
    if path.exists():
        raise ValueError(f"{label} must not exist for a fresh calibration")


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
    authority = _absolute(args.authority_root, must_exist=False)
    _assert_fresh(work, "work root")
    _assert_fresh(export, "export root")
    calibration_source = verify_dwd_germany_roi_calibration_source(source)
    selection = seal_dwd_germany_roi_selection_manifest(authority)
    limits = DwdRvCompletionIsolationLimits(
        rss_bytes=CALIBRATION_RSS_CAP_BYTES,
        cpu_seconds=CALIBRATION_CPU_SECONDS,
        wall_seconds=CALIBRATION_WALL_SECONDS,
    )
    if limits.fingerprint != EXPECTED_LIMITS_SHA256:
        raise RuntimeError("fixed calibration isolation limits drifted")
    profile = dwd_germany_roi_profile()
    if args.preflight_only:
        print(json.dumps({
            "active_decoder_children": len(multiprocessing.active_children()),
            "calibration_archive_sha256": calibration_source.archive_sha256,
            "isolation_limits_sha256": limits.fingerprint,
            "profile_sha256": profile.fingerprint,
            "selection_manifest_sha256": selection.manifest_sha256,
            "status": "preflight_only_no_child_started",
        }, sort_keys=True))
        return 0

    parent_baseline = _process_memory(os.getpid())
    parent_peaks: list[int] = []
    stop = threading.Event()
    sampler = threading.Thread(
        target=_sample_parent,
        args=(stop, parent_peaks),
        name="dwd-roi-calibration-parent-rss-sampler",
        daemon=True,
    )
    sampler.start()
    parent_cpu_started = time.process_time()
    wall_started = time.perf_counter()
    try:
        receipt = build_dwd_rv_observation_replay(
            source_directory=source,
            work_root=work,
            export_root=export,
            isolation_limits=limits,
            profile=profile,
            expected_frame_count=1,
        )
    finally:
        parent_wall_seconds = time.perf_counter() - wall_started
        parent_cpu_seconds = time.process_time() - parent_cpu_started
        stop.set()
        sampler.join(timeout=2.0)
    if sampler.is_alive():
        raise RuntimeError("parent RSS sampler did not terminate")
    parent_final = _process_memory(os.getpid())
    parent_candidates = parent_peaks[:]
    for measurement in (parent_baseline, parent_final):
        if measurement is not None:
            parent_candidates.extend(measurement)
    if not parent_candidates:
        raise RuntimeError("parent RSS measurement is unavailable")

    summary = receipt_summary(receipt)
    if summary["source_manifest_sha256"] != CALIBRATION_SOURCE_MANIFEST_SHA256:
        raise RuntimeError("one-frame source manifest differs")
    summary.update({
        "calibration_hard_rss_cap_bytes": CALIBRATION_RSS_CAP_BYTES,
        "child_cpu_max_seconds": receipt.process_metrics.child_cpu_max_seconds,
        "child_elapsed_max_seconds": receipt.process_metrics.elapsed_max_seconds,
        "parent_cpu_seconds": parent_cpu_seconds,
        "parent_peak_rss_bytes": max(parent_candidates),
        "parent_rss_baseline_bytes": (
            None if parent_baseline is None else parent_baseline[0]
        ),
        "parent_wall_seconds": parent_wall_seconds,
        "spatial_aggregation_is_truth": False,
    })
    cap = derive_dwd_germany_roi_replay_cap(
        observed_peak_boundary_bytes=summary["peak_boundary_max_bytes"],
        observed_peak_rss_bytes=summary["peak_rss_max_bytes"],
        active_children_after_run=summary["active_decoder_children_after_run"],
    )
    calibration = seal_dwd_germany_roi_calibration_manifest(
        authority,
        selection=selection,
        calibration_summary=summary,
        calibration_source_archive_sha256=CALIBRATION_ARCHIVE_SHA256,
        cap=cap,
    )
    verify_dwd_germany_roi_calibration_source(source)
    print(json.dumps({
        "active_decoder_children_after_run": 0,
        "calibration_manifest_sha256": calibration.manifest_sha256,
        "cap_derivation": cap.to_mapping(),
        "metrics": summary,
        "selection_manifest_sha256": selection.manifest_sha256,
        "status": "calibration_complete_shadow_only",
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    multiprocessing.freeze_support()
    raise SystemExit(main())
