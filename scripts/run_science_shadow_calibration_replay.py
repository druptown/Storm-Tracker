"""Run the audited 36-frame normalized-cache science replay offline.

This command is evidence generation only.  It has no service, MQTT, Home
Assistant, acquisition, forecast, or operational snapshot integration.
"""
from __future__ import annotations

from collections import Counter
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


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storm_tracker_worker.hard_decoder_isolation import (  # noqa: E402
    _canonical_json_bytes,
    hard_boundary_support,
)
from storm_tracker_worker.science_shadow_calibration_replay import (  # noqa: E402
    CALIBRATION_REPLAY_BLOCKERS,
    CALIBRATION_REPLAY_CAVEATS,
    CALIBRATION_REPLAY_CONNECTION_MODE,
    build_calibration_replay_orchestrator,
    create_calibration_replay_authority,
    export_normalized_cache_sequence,
    recover_calibration_replay_state,
)
from storm_tracker_worker.science_shadow_orchestrator import (  # noqa: E402
    ScienceShadowLimits,
    ScienceShadowReceiptStore,
    _encode_state_value,
)
from storm_tracker_worker.science_shadow_process_isolation import (  # noqa: E402
    ScienceShadowIsolatedRunner,
    ScienceShadowIsolationLimits,
)
from storm_tracker_worker.tracking_core.engine.cell_track_engine import (  # noqa: E402
    advance_cell_track_engine,
    create_cell_track_engine_state,
)


DATABASE_RELATIVE = Path(
    ".calibration-data/storm_tracker_calibration_20260730_092754.sqlite3"
)
EXPECTED_DATABASE_SHA256 = (
    "177d4704d665000722dbd131f3db03a238b26de072ce32bde6ea4650815cb263"
)
REGION_ID = "region-1"
SOURCE = "kmi"
EXPECTED_FRAMES = 36
JUMP_THRESHOLD_KMH = 200.0
ARTIFACT_STEM = "kmi_region1_science_shadow_isolated_replay_v5_20260808"
EVIDENCE_STEM = ARTIFACT_STEM
PRIOR_V4_EVIDENCE_NAME = (
    "kmi_region1_science_shadow_isolated_replay_v4_1_20260808_evidence.json"
)
V3_EVIDENCE_NAME = (
    "kmi_region1_science_shadow_isolated_replay_v3_20260808_evidence.json"
)
PRIOR_REPORTS = (
    "kmi_last_two_replay_root_20260730.json",
    "kmi_region1_sparse_pyramidal_replay_20260730.json",
    "kmi_region1_motion_identity_neutral_v2_replay_20260730.json",
)


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _sha_value(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _sidecar_snapshot(database: Path) -> dict[str, dict[str, Any]]:
    result = {}
    for suffix in ("-journal", "-shm", "-wal"):
        path = Path(str(database) + suffix)
        if path.exists():
            stat_result = path.stat()
            result[suffix] = {
                "bytes": stat_result.st_size,
                "last_write_ns": stat_result.st_mtime_ns,
                "sha256": _sha_file(path),
            }
    return result


def _nearest_rank(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


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


def _windows_rss(pid: int) -> int | None:
    if sys.platform != "win32":
        return None
    process_query_limited_information = 0x1000
    process_vm_read = 0x0010
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    handle = kernel.OpenProcess(
        process_query_limited_information | process_vm_read,
        False,
        pid,
    )
    if not handle:
        return None
    try:
        counters = _ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        if not psapi.GetProcessMemoryInfo(
            handle,
            ctypes.byref(counters),
            counters.cb,
        ):
            return None
        # PeakWorkingSetSize is the OS-maintained lifetime high-water mark,
        # unlike WorkingSetSize which is only an instantaneous lower bound.
        return int(counters.PeakWorkingSetSize)
    finally:
        kernel.CloseHandle(handle)


def _linux_rss(pid: int) -> int | None:
    if not sys.platform.startswith("linux"):
        return None
    try:
        current = None
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
            if line.startswith("VmRSS:"):
                current = int(line.split()[1]) * 1024
        return current
    except (FileNotFoundError, PermissionError, ValueError, IndexError):
        pass
    return None


def _windows_cpu_seconds(pid: int) -> float | None:
    if sys.platform != "win32":
        return None
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        return None
    try:
        creation = wintypes.FILETIME()
        exit_time = wintypes.FILETIME()
        kernel_time = wintypes.FILETIME()
        user_time = wintypes.FILETIME()
        if not kernel.GetProcessTimes(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel_time),
            ctypes.byref(user_time),
        ):
            return None

        def ticks(value: wintypes.FILETIME) -> int:
            return (int(value.dwHighDateTime) << 32) | int(value.dwLowDateTime)

        return (ticks(kernel_time) + ticks(user_time)) / 10_000_000.0
    finally:
        kernel.CloseHandle(handle)


def _linux_cpu_seconds(pid: int) -> float | None:
    if not sys.platform.startswith("linux"):
        return None
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().split()
        ticks = int(fields[13]) + int(fields[14])
        return ticks / float(os.sysconf("SC_CLK_TCK"))
    except (FileNotFoundError, PermissionError, ValueError, IndexError, OSError):
        return None


def _sample_children(
    stop: threading.Event,
    peaks: dict[int, int],
    cpu_seconds: dict[int, float] | None = None,
) -> None:
    while not stop.wait(0.002):
        for child in multiprocessing.active_children():
            try:
                pid = child.pid
            except ValueError:
                # The runner can close its Process object between the
                # active_children() snapshot and this read.  A closed handle
                # is not a live child and must not escape the sampler thread.
                continue
            if pid is None:
                continue
            rss = _windows_rss(pid) or _linux_rss(pid)
            if rss is not None:
                peaks[pid] = max(peaks.get(pid, 0), rss)
            if cpu_seconds is not None:
                cpu = _windows_cpu_seconds(pid)
                if cpu is None:
                    cpu = _linux_cpu_seconds(pid)
                if cpu is not None:
                    cpu_seconds[pid] = max(cpu_seconds.get(pid, 0.0), cpu)


def _active_child_pids() -> set[int]:
    values: set[int] = set()
    for child in multiprocessing.active_children():
        try:
            pid = child.pid
        except ValueError:
            continue
        if pid is not None:
            values.add(pid)
    return values


def _execute_sampled(runner: ScienceShadowIsolatedRunner, **kwargs):
    stop = threading.Event()
    peaks: dict[int, int] = {}
    cpu_seconds: dict[int, float] = {}
    sampler = threading.Thread(
        target=_sample_children,
        args=(stop, peaks, cpu_seconds),
        name="science-calibration-resource-sampler",
        daemon=True,
    )
    baseline = _active_child_pids()
    sampler.start()
    started = time.perf_counter()
    try:
        result = runner.execute(**kwargs)
    finally:
        elapsed = time.perf_counter() - started
        stop.set()
        sampler.join(timeout=2.0)
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        remaining = _active_child_pids()
        if remaining <= baseline:
            break
        time.sleep(0.02)
    remaining = _active_child_pids() - baseline
    return (
        result,
        elapsed,
        max(peaks.values()) if peaks else None,
        max(cpu_seconds.values()) if cpu_seconds else None,
        sorted(remaining),
    )


def _distribution(values: list[float | int]) -> dict[str, Any]:
    selected = [float(value) for value in values]
    return {
        "count": len(selected),
        "maximum": max(selected) if selected else None,
        "median_p50": statistics.median(selected) if selected else None,
        "nearest_rank_p95": _nearest_rank(selected, 0.95),
        "sum": math.fsum(selected),
    }


def _store(authority) -> ScienceShadowReceiptStore:
    return ScienceShadowReceiptStore(
        authority.receipt_path,
        engine_id=authority.authority_document["engine_id"],
        runtime_profile_sha256=(
            authority.authority_document["runtime_profile_sha256"]
        ),
        limits=authority.science_limits,
    )


def _runner(authority) -> ScienceShadowIsolatedRunner:
    return ScienceShadowIsolatedRunner(
        authority_path=authority.authority_path,
        expected_authority_sha256=authority.authority_sha256,
        factory_contract=authority.factory_contract,
        limits=authority.isolation_limits,
    )


def _system_summary(state, frame_number: int) -> dict[str, Any]:
    grouping = state.weather_systems
    assert grouping is not None
    systems = []
    for system in grouping.systems:
        motion = system.descriptive_motion
        speed = None
        if motion.vx_m_s is not None and motion.vy_m_s is not None:
            speed = math.hypot(motion.vx_m_s, motion.vy_m_s) * 3.6
        systems.append({
            "classification": system.classification,
            "descriptive_motion_hypothesis_kmh": speed,
            "descriptive_motion_method": motion.method,
            "footprint_pixels": len(system.aggregate_current_footprint),
            "member_track_ids": list(system.member_track_ids),
            "predecessor_candidate_count": len(system.predecessor_candidates),
            "system_id": system.system_id,
            "system_uncertainty": list(system.system_uncertainty),
            "uncertain_connection_ids": list(system.uncertain_connection_ids),
        })
    return {
        "connection_decisions": dict(Counter(
            item.decision for item in grouping.connections
        )),
        "frame_number": frame_number,
        "grouping_snapshot_id": grouping.grouping_snapshot_id,
        "input_active_track_count": len(grouping.input_tracks),
        "membership_event_types": dict(Counter(
            item.event_type for item in grouping.membership_events
        )),
        "retired_system_ids": list(grouping.retired_system_ids),
        "system_count": len(grouping.systems),
        "systems": systems,
        "valid_at": grouping.valid_at,
    }


def _state_metrics(state) -> dict[str, Any]:
    intervals: list[dict[str, Any]] = []
    for track in state.ledger.tracks:
        for previous, current in zip(track.detections, track.detections[1:]):
            elapsed = current.valid_at - previous.valid_at
            speed = math.hypot(
                current.x_m - previous.x_m,
                current.y_m - previous.y_m,
            ) / elapsed * 3.6
            intervals.append({
                "from_detection_id": previous.detection_id,
                "from_valid_at": previous.valid_at,
                "hypothesis_speed_kmh": speed,
                "to_detection_id": current.detection_id,
                "to_valid_at": current.valid_at,
                "track_id": track.track_id,
            })
    speeds = [item["hypothesis_speed_kmh"] for item in intervals]
    events = state.ledger.lineage_events
    state_body = _canonical_json_bytes(_encode_state_value(state))
    return {
        "active_tracks": sum(
            item.lifecycle_state != "ended" for item in state.ledger.tracks
        ),
        "birth_events": sum(item.event_type == "birth" for item in events),
        "death_events": sum(item.event_type == "death" for item in events),
        "ended_tracks": sum(
            item.lifecycle_state == "ended" for item in state.ledger.tracks
        ),
        "final_state_bytes": len(state_body),
        "final_state_sha256": hashlib.sha256(state_body).hexdigest(),
        "hypothesis_interval_count": len(intervals),
        "hypothesis_interval_speeds": intervals,
        "hypothesis_jump_count_over_200_kmh": sum(
            value > JUMP_THRESHOLD_KMH for value in speeds
        ),
        "hypothesis_speed_maximum_kmh": max(speeds) if speeds else None,
        "hypothesis_speed_median_kmh": statistics.median(speeds) if speeds else None,
        "hypothesis_speed_p95_kmh": _nearest_rank(speeds, 0.95),
        "lifecycle_event_count": len(events),
        "lifecycle_event_types": dict(Counter(
            item.event_type for item in events
        )),
        "lineage_events": [
            {
                "child_track_ids": list(item.child_track_ids),
                "event_id": item.event_id,
                "event_type": item.event_type,
                "parent_track_ids": list(item.parent_track_ids),
                "valid_at": item.valid_at,
            }
            for item in events
            if item.event_type in ("split", "merge")
        ],
        "revision": state.revision,
        "total_tracks": len(state.ledger.tracks),
    }


def _selected_multi_scan_metrics(
    steps,
    detection_by_id: dict[str, Any],
    *,
    maximum_gap_frames: int,
) -> dict[str, Any]:
    """Summarize one branch-consistent committed+MAP decision path."""

    ordered = tuple(sorted(steps, key=lambda item: item.revision))
    if tuple(item.revision for item in ordered) != tuple(
        range(1, len(ordered) + 1)
    ):
        raise RuntimeError("multi-scan selected decisions are not contiguous")
    tracks: dict[str, list[str]] = {}
    misses: dict[str, int] = {}
    death_revisions: dict[str, int] = {}
    for step in ordered:
        touched: set[str] = set()
        for identity_id, detection_id in step.births:
            if identity_id in tracks or detection_id not in detection_by_id:
                raise RuntimeError("multi-scan birth history is inconsistent")
            tracks[identity_id] = [detection_id]
            misses[identity_id] = 0
            touched.add(identity_id)
        for identity_id, previous_id, current_id in step.matches:
            if (
                identity_id not in tracks
                or identity_id in death_revisions
                or tracks[identity_id][-1] != previous_id
                or current_id not in detection_by_id
            ):
                raise RuntimeError("multi-scan match history is inconsistent")
            tracks[identity_id].append(current_id)
            misses[identity_id] = 0
            touched.add(identity_id)
        for identity_id in step.gap_identity_ids:
            if (
                identity_id not in tracks
                or identity_id in death_revisions
                or identity_id in touched
            ):
                raise RuntimeError("multi-scan gap history is inconsistent")
            misses[identity_id] += 1
            if misses[identity_id] > maximum_gap_frames:
                death_revisions[identity_id] = step.revision

    intervals = []
    for identity_id, detection_ids in sorted(tracks.items()):
        for previous_id, current_id in zip(detection_ids, detection_ids[1:]):
            previous = detection_by_id[previous_id]
            current = detection_by_id[current_id]
            elapsed = current.valid_at - previous.valid_at
            if elapsed <= 0.0:
                raise RuntimeError("multi-scan selected interval time is invalid")
            speed = math.hypot(
                current.x_m - previous.x_m,
                current.y_m - previous.y_m,
            ) / elapsed * 3.6
            intervals.append({
                "from_detection_id": previous_id,
                "from_valid_at": previous.valid_at,
                "hypothesis_speed_kmh": speed,
                "identity_id": identity_id,
                "to_detection_id": current_id,
                "to_valid_at": current.valid_at,
            })
    speeds = [item["hypothesis_speed_kmh"] for item in intervals]
    lengths = [len(item) for item in tracks.values()]
    active = len(tracks) - len(death_revisions)
    return {
        "active_tracks": active,
        "birth_events": len(tracks),
        "death_events": len(death_revisions),
        "decision_count": len(ordered),
        "ended_tracks": len(death_revisions),
        "hypothesis_interval_count": len(intervals),
        "hypothesis_interval_speeds": intervals,
        "hypothesis_jump_count_over_200_kmh": sum(
            value > JUMP_THRESHOLD_KMH for value in speeds
        ),
        "hypothesis_speed_maximum_kmh": max(speeds) if speeds else None,
        "hypothesis_speed_median_kmh": (
            statistics.median(speeds) if speeds else None
        ),
        "hypothesis_speed_p95_kmh": _nearest_rank(speeds, 0.95),
        "identity_tracks": [
            {
                "death_revision": death_revisions.get(identity_id),
                "detection_ids": detection_ids,
                "identity_id": identity_id,
                "observation_count": len(detection_ids),
            }
            for identity_id, detection_ids in sorted(tracks.items())
        ],
        "total_tracks": len(tracks),
        "track_length_maximum_observations": max(lengths) if lengths else 0,
        "track_length_mean_observations": (
            statistics.fmean(lengths) if lengths else 0.0
        ),
        "track_length_median_observations": (
            statistics.median(lengths) if lengths else 0.0
        ),
    }


def _multi_hypothesis_audit(
    authority,
    *,
    build_orchestrator=build_calibration_replay_orchestrator,
    recover_state=recover_calibration_replay_state,
) -> dict[str, Any]:
    """Reconstruct every bounded transition and prove the isolated final head.

    The factory/recovery hooks keep this accounting implementation reusable by
    other sealed science-only replay authorities without weakening the default
    normalized-cache contract.
    """

    orchestrator = build_orchestrator(
        authority.authority_document
    )
    policy = orchestrator._policy
    composer = orchestrator._composer
    state = create_cell_track_engine_state(
        authority.authority_document["engine_id"],
        policy,
    )
    per_frame: list[dict[str, Any]] = []
    selected_speeds: list[float] = []
    selected_local_residuals: list[float] = []
    translation_speeds: list[float] = []
    top_one_weights: list[float] = []
    entropies: list[float] = []
    multi_scan_top_one_weights: list[float] = []
    multi_scan_entropies: list[float] = []
    multi_scan_effective_counts: list[float] = []
    committed_steps = []
    detection_by_id: dict[str, Any] = {}
    lineage_by_id: dict[str, Any] = {}
    audit_started = time.perf_counter()
    for frame_number, run_id in enumerate(authority.export.run_ids, start=1):
        composed = composer.compose(
            run_id=run_id,
            deadline_at=time.monotonic() + 45.0,
            monotonic=time.monotonic,
        )
        if composed.evidence is None:
            raise RuntimeError("audit composer withheld an admitted replay frame")
        transition = advance_cell_track_engine(
            state,
            composed.evidence.normalized_frame,
            policy,
        )
        if transition.status != "advanced":
            raise RuntimeError(
                f"multi-hypothesis audit frame {frame_number} indeterminate: "
                f"{transition.reason_codes}"
            )
        for item in transition.detection_frame.detections:
            detection = item.detection
            if (
                detection.detection_id in detection_by_id
                and detection_by_id[detection.detection_id] != detection
            ):
                raise RuntimeError("multi-scan detection ID collision")
            detection_by_id[detection.detection_id] = detection
        assignment = transition.assignment_result
        snapshot = transition.identity_snapshot
        feature_by_pair = {
            (item.previous_detection_id, item.current_detection_id): item
            for table in transition.association_feature_tables
            for item in table.features
        }
        frame_selected_speeds = []
        if assignment is not None:
            frame_selected_speeds = [
                feature_by_pair[pair].observed_interval_speed_kmh
                for pair in assignment.best.matches
            ]
            selected_speeds.extend(frame_selected_speeds)
        frame_lineage = Counter(
            item.event_type for item in transition.lineage_hypotheses
        )
        frame_record: dict[str, Any] = {
            "assignment_births": 0 if assignment is None else len(assignment.best.births),
            "assignment_deaths": 0 if assignment is None else len(assignment.best.deaths),
            "assignment_matches": 0 if assignment is None else len(assignment.best.matches),
            "candidate_edges": transition.candidate_edge_count,
            "derived_merge_alternatives": frame_lineage.get("merge", 0),
            "derived_split_alternatives": frame_lineage.get("split", 0),
            "detection_objects": len(transition.detection_frame.detections),
            "frame_number": frame_number,
            "run_id": run_id,
            "selected_interval_speed_maximum_kmh": (
                max(frame_selected_speeds) if frame_selected_speeds else None
            ),
        }
        posterior = transition.multi_scan_posterior
        if posterior is None:
            raise RuntimeError("calibration policy did not produce multi-scan state")
        expected_multi_scan_candidates = (
            0 if state.multi_scan_posterior is None
            else sum(
                len(branch.paths) * len(transition.detection_frame.detections)
                for branch in state.multi_scan_posterior.branches
            )
        )
        referenced_lineage = {
            alternative_id
            for branch in posterior.branches
            for step in branch.steps
            for alternative_id in step.lineage_alternative_ids
        }
        if posterior.newly_committed_step is not None:
            committed_steps.append(posterior.newly_committed_step)
            referenced_lineage.update(
                posterior.newly_committed_step.lineage_alternative_ids
            )
        for item in posterior.lineage_alternatives:
            prior = lineage_by_id.get(item.alternative_id)
            if prior is not None and prior != item:
                raise RuntimeError("multi-scan lineage ID collision")
            lineage_by_id[item.alternative_id] = item
        if referenced_lineage != {
            item.alternative_id for item in posterior.lineage_alternatives
        }:
            raise RuntimeError("multi-scan lineage accounting differs")
        multi_scan_top_one_weights.append(
            posterior.branches[0].conditional_weight
        )
        multi_scan_entropies.append(posterior.conditional_entropy_nats)
        multi_scan_effective_counts.append(posterior.effective_hypothesis_count)
        frame_record["multi_scan"] = {
            "assignment_search_exhausted_all_branches": (
                posterior.assignment_search_exhausted_all_branches
            ),
            "assignment_subproblems_explored": (
                posterior.assignment_subproblems_explored
            ),
            "assignment_subproblems_limit": posterior.assignment_subproblems_limit,
            "branch_count": len(posterior.branches),
            "candidate_accounting_exact": (
                posterior.candidate_edge_count == expected_multi_scan_candidates
            ),
            "candidate_edges": posterior.candidate_edge_count,
            "committed_through_revision": posterior.committed_through_revision,
            "conditional_entropy_nats": posterior.conditional_entropy_nats,
            "conditional_top_one_weight": (
                posterior.branches[0].conditional_weight
            ),
            "effective_hypothesis_count": posterior.effective_hypothesis_count,
            "generated_accounting_exact": (
                posterior.generated_branch_count
                == posterior.retained_branch_count
                + posterior.pruned_by_beam_count
                + posterior.pruned_by_n_scan_count
            ),
            "generated_branches": posterior.generated_branch_count,
            "lineage_accounting_exact": True,
            "lineage_alternative_count": len(posterior.lineage_alternatives),
            "newly_committed_revision": (
                None if posterior.newly_committed_step is None
                else posterior.newly_committed_step.revision
            ),
            "omitted_hypothesis_mass_unknown": (
                posterior.omitted_hypothesis_mass_unknown
            ),
            "pruned_by_beam": posterior.pruned_by_beam_count,
            "pruned_by_n_scan": posterior.pruned_by_n_scan_count,
        }
        if snapshot is None:
            frame_record["identity_state"] = "bootstrap_no_predecessor"
        else:
            selected_local = tuple(
                item for item in snapshot.local_candidates if item.selected_in_best
            )
            residuals = tuple(
                math.hypot(
                    item.prediction_residual_x_m,
                    item.prediction_residual_y_m,
                )
                for item in selected_local
            )
            selected_local_residuals.extend(residuals)
            if snapshot.translation.interval_speed_kmh is not None:
                translation_speeds.append(snapshot.translation.interval_speed_kmh)
            top_one_weights.append(snapshot.hypotheses[0].conditional_weight)
            entropies.append(snapshot.conditional_entropy_nats)
            frame_record.update({
                "assignment_explored_subproblems": (
                    snapshot.assignment_explored_subproblems
                ),
                "assignment_maximum_subproblems": (
                    snapshot.assignment_maximum_subproblems
                ),
                "assignment_search_exhausted": (
                    snapshot.assignment_search_exhausted
                ),
                "candidate_accounting_exact": (
                    snapshot.candidate_edge_count
                    == snapshot.expected_candidate_edge_count
                    == transition.candidate_edge_count
                ),
                "conditional_entropy_nats": snapshot.conditional_entropy_nats,
                "conditional_top_one_weight": (
                    snapshot.hypotheses[0].conditional_weight
                ),
                "identity_hypotheses_retained": len(snapshot.hypotheses),
                "identity_state": "bounded_multi_hypothesis",
                "local_candidates_retained": len(snapshot.local_candidates),
                "omitted_hypothesis_mass_unknown": (
                    snapshot.omitted_hypothesis_mass_unknown
                ),
                "selected_local_residual_maximum_m": (
                    max(residuals) if residuals else None
                ),
                "system_translation_hypotheses": (
                    1 + len(snapshot.system_translations)
                ),
                "whole_field_translation_speed_kmh": (
                    snapshot.translation.interval_speed_kmh
                ),
            })
        per_frame.append(frame_record)
        state = transition.next_state

    isolated_state = recover_state(authority)
    reconstructed_body = _canonical_json_bytes(_encode_state_value(state))
    isolated_body = _canonical_json_bytes(_encode_state_value(isolated_state))
    if reconstructed_body != isolated_body:
        raise RuntimeError("direct audit and isolated durable state differ")
    track_lengths = [item.sample_count for item in state.ledger.tracks]
    posterior = state.multi_scan_posterior
    if posterior is None:
        raise RuntimeError("final multi-scan posterior is absent")
    selected_steps = tuple(sorted(
        (*committed_steps, *posterior.branches[0].steps),
        key=lambda item: item.revision,
    ))
    selected_metrics = _selected_multi_scan_metrics(
        selected_steps,
        detection_by_id,
        maximum_gap_frames=policy.lifecycle.maximum_gap_frames,
    )
    selected_lineage_ids = {
        alternative_id
        for step in selected_steps
        for alternative_id in step.lineage_alternative_ids
    }
    missing_lineage = selected_lineage_ids - set(lineage_by_id)
    if missing_lineage:
        raise RuntimeError("multi-scan selected lineage evidence is incomplete")
    selected_lineage = tuple(
        lineage_by_id[item] for item in sorted(selected_lineage_ids)
    )
    return {
        "audit_runtime_seconds": time.perf_counter() - audit_started,
        "candidate_accounting_exact_all_frames": all(
            item.get("candidate_accounting_exact", True) for item in per_frame
        ),
        "conditional_entropy_maximum_nats": max(entropies) if entropies else None,
        "conditional_entropy_median_nats": (
            statistics.median(entropies) if entropies else None
        ),
        "conditional_top_one_weight_median": (
            statistics.median(top_one_weights) if top_one_weights else None
        ),
        "derived_merge_alternative_count": sum(
            item["derived_merge_alternatives"] for item in per_frame
        ),
        "derived_split_alternative_count": sum(
            item["derived_split_alternatives"] for item in per_frame
        ),
        "final_state_exactly_matches_isolated_head": True,
        "final_state_sha256": hashlib.sha256(isolated_body).hexdigest(),
        "identity_hypothesis_frames": sum(
            item["identity_state"] == "bounded_multi_hypothesis"
            for item in per_frame
        ),
        "maximum_assignment_subproblems_explored": max(
            (
                item.get("assignment_explored_subproblems", 0)
                for item in per_frame
            ),
            default=0,
        ),
        "maximum_assignment_subproblems_limit": (
            policy.multi_hypothesis_identity.maximum_assignment_subproblems
        ),
        "omitted_mass_unknown_frames": sum(
            bool(item.get("omitted_hypothesis_mass_unknown"))
            for item in per_frame
        ),
        "per_frame": per_frame,
        "multi_scan": {
            "admission_granted": False,
            "branch_count_maximum": max(
                item["multi_scan"]["branch_count"] for item in per_frame
            ),
            "candidate_accounting_exact_all_frames": all(
                item["multi_scan"]["candidate_accounting_exact"]
                for item in per_frame
            ),
            "committed_decision_chain_sha256": (
                posterior.committed_decision_chain_sha256
            ),
            "committed_through_revision": posterior.committed_through_revision,
            "conditional_entropy_nats": _distribution(multi_scan_entropies),
            "conditional_top_one_weight": _distribution(
                multi_scan_top_one_weights
            ),
            "effective_hypothesis_count": _distribution(
                multi_scan_effective_counts
            ),
            "generated_accounting_exact_all_frames": all(
                item["multi_scan"]["generated_accounting_exact"]
                for item in per_frame
            ),
            "lineage_accounting_exact_all_frames": all(
                item["multi_scan"]["lineage_accounting_exact"]
                for item in per_frame
            ),
            "maximum_assignment_subproblems_explored": max(
                item["multi_scan"]["assignment_subproblems_explored"]
                for item in per_frame
            ),
            "maximum_assignment_subproblems_limit": (
                policy.multi_hypothesis_identity.multi_scan
                .maximum_assignment_subproblems_total
            ),
            "maximum_candidate_edges": max(
                item["multi_scan"]["candidate_edges"] for item in per_frame
            ),
            "omitted_mass_unknown_frames": sum(
                item["multi_scan"]["omitted_hypothesis_mass_unknown"]
                for item in per_frame
            ),
            "selected_decision_path": selected_metrics,
            "selected_lineage_alternatives": [
                {
                    "alternative_id": item.alternative_id,
                    "child_detection_ids": list(item.child_detection_ids),
                    "event_type": item.event_type,
                    "parent_identity_ids": list(item.parent_identity_ids),
                    "revision": item.revision,
                    "selected": item.selected,
                }
                for item in selected_lineage
            ],
            "selected_merge_alternative_count": sum(
                item.event_type == "merge" for item in selected_lineage
            ),
            "selected_split_alternative_count": sum(
                item.event_type == "split" for item in selected_lineage
            ),
            "uncommitted_map_decision_count": len(posterior.branches[0].steps),
            "weather_system_binding": (
                "not_recomputed; current WeatherSystem snapshot remains bound "
                "to the primary per-transition lifecycle, not this shadow MAP path"
            ),
        },
        "selected_interval_speed_count": len(selected_speeds),
        "selected_interval_speed_maximum_kmh": (
            max(selected_speeds) if selected_speeds else None
        ),
        "selected_interval_speed_median_kmh": (
            statistics.median(selected_speeds) if selected_speeds else None
        ),
        "selected_interval_speed_p95_kmh": _nearest_rank(selected_speeds, 0.95),
        "selected_interval_speed_over_200_kmh": sum(
            item > JUMP_THRESHOLD_KMH for item in selected_speeds
        ),
        "selected_local_residual_median_m": (
            statistics.median(selected_local_residuals)
            if selected_local_residuals
            else None
        ),
        "track_length_maximum_observations": max(track_lengths) if track_lengths else 0,
        "track_length_mean_observations": (
            statistics.fmean(track_lengths) if track_lengths else 0.0
        ),
        "track_length_median_observations": (
            statistics.median(track_lengths) if track_lengths else 0.0
        ),
        "whole_field_translation_speed_maximum_kmh": (
            max(translation_speeds) if translation_speeds else None
        ),
        "whole_field_translation_speed_median_kmh": (
            statistics.median(translation_speeds) if translation_speeds else None
        ),
    }


def _prior_identity_comparison(
    analysis: Path,
    current_identity: dict[str, Any],
    *,
    evidence_name: str,
    prior_label: str,
    sequence_id: str,
) -> dict[str, Any]:
    path = analysis / evidence_name
    value = json.loads(path.read_text(encoding="utf-8"))
    previous = value["primary_run"]["state"]
    fields = (
        "total_tracks",
        "active_tracks",
        "ended_tracks",
        "hypothesis_interval_count",
        "hypothesis_jump_count_over_200_kmh",
        "hypothesis_speed_median_kmh",
        "hypothesis_speed_p95_kmh",
        "hypothesis_speed_maximum_kmh",
    )
    metrics = {}
    for field in fields:
        old = previous[field]
        new = current_identity[field]
        metrics[field] = {
            prior_label: old,
            "v5_multi_scan_selected_path": new,
            "v5_minus_prior": new - old,
            "v5_over_prior": None if old == 0 else new / old,
        }
    return {
        "comparison_is_accuracy_claim": False,
        "exact_database_sha256_matches": (
            value["input"]["database_sha256_after"]
            == EXPECTED_DATABASE_SHA256
        ),
        "exact_sequence_id_matches": (
            value["input"]["sequence_id"] == sequence_id
        ),
        "metrics": metrics,
        "prior_evidence_file": evidence_name,
        "prior_label": prior_label,
        "warning": (
            "lower jump/speed values and different continuity do not establish "
            "identity accuracy without independent identity/speed truth"
        ),
    }


def _v3_comparison(
    analysis: Path,
    current_identity: dict[str, Any],
    *,
    sequence_id: str,
) -> dict[str, Any]:
    return _prior_identity_comparison(
        analysis,
        current_identity,
        evidence_name=V3_EVIDENCE_NAME,
        prior_label="v3_one_best",
        sequence_id=sequence_id,
    )


def _v4_comparison(
    analysis: Path,
    current_identity: dict[str, Any],
    *,
    sequence_id: str,
) -> dict[str, Any]:
    return _prior_identity_comparison(
        analysis,
        current_identity,
        evidence_name=PRIOR_V4_EVIDENCE_NAME,
        prior_label="v4_1_per_transition_top_k_rank1",
        sequence_id=sequence_id,
    )


def _run_corpus(authority, *, label: str, include_snapshots: bool) -> dict[str, Any]:
    per_frame = []
    system_snapshots = []
    runner = _runner(authority)
    initial_store_file_bytes = authority.receipt_path.stat().st_size
    total_started = time.perf_counter()
    for frame_number, run_id in enumerate(authority.export.run_ids, start=1):
        if frame_number in (21, 33):
            runner = _runner(authority)
        before = authority.receipt_path.stat().st_size
        result, elapsed, peak_rss, child_cpu_seconds, remaining = _execute_sampled(
            runner,
            attempt_id=f"calibration-frame-{frame_number:03d}",
            canonical_run_id=run_id,
        )
        if (
            result.status != "success"
            or not result.committed
            or result.duplicate
            or result.receipt_metadata is None
            or result.receipt_metadata["outcome"] != "advanced"
        ):
            raise RuntimeError(
                f"{label} frame {frame_number} failed: {result.to_mapping()}"
            )
        if remaining:
            raise RuntimeError(f"{label} frame {frame_number} leaked children")
        stats = _store(authority).stats()
        metadata = result.receipt_metadata
        per_frame.append({
            "accounting": metadata["accounting"],
            "attempt_id": f"calibration-frame-{frame_number:03d}",
            "authority_sha256": metadata["authority_sha256"],
            "child_exit_code": result.child_exit_code,
            "child_cpu_seconds_sampled": child_cpu_seconds,
            "child_cpu_wall_core_equivalents": (
                None if child_cpu_seconds is None
                else child_cpu_seconds / elapsed
            ),
            "elapsed_seconds": elapsed,
            "frame_number": frame_number,
            "isolation_enforced": result.isolation_enforced,
            "next_state_revision": metadata["next_state_revision"],
            "peak_child_rss_sampled_bytes": peak_rss,
            "previous_advanced_receipt_sha256": (
                metadata["previous_advanced_receipt_sha256"]
            ),
            "receipt_file_growth_bytes": authority.receipt_path.stat().st_size - before,
            "receipt_sha256": metadata["receipt_sha256"],
            "run_id": run_id,
            "store": stats,
            "transition_id": metadata["transition_id"],
        })
        if include_snapshots and frame_number in (20, 32, 36):
            state = recover_calibration_replay_state(authority)
            if state.revision != frame_number:
                raise RuntimeError("recovered state revision differs")
            system_snapshots.append(_system_summary(state, frame_number))
    total_elapsed = time.perf_counter() - total_started

    final_state = recover_calibration_replay_state(authority)
    metrics = _state_metrics(final_state)
    receipts = _store(authority).advanced_receipts()
    duplicate_receipt_sha256 = []
    duplicate_started = time.perf_counter()
    for frame_number, run_id in enumerate(authority.export.run_ids, start=1):
        duplicate = _runner(authority).execute(
            attempt_id=f"calibration-frame-{frame_number:03d}",
            canonical_run_id=run_id,
        )
        if not (
            duplicate.status == "success"
            and duplicate.committed
            and duplicate.duplicate
        ):
            raise RuntimeError(f"{label} duplicate {frame_number} differs")
        duplicate_receipt_sha256.append(
            duplicate.receipt_metadata["receipt_sha256"]
        )
    return {
        "duplicate_retry_elapsed_seconds": time.perf_counter() - duplicate_started,
        "duplicate_retry_receipts_exact": duplicate_receipt_sha256 == [
            item.receipt_sha256 for item in receipts
        ],
        "final_store": _store(authority).stats(),
        "frame_count": len(per_frame),
        "initial_store_file_bytes": initial_store_file_bytes,
        "label": label,
        "per_frame": per_frame,
        "receipt_chain_sha256": _sha_value([
            item.receipt_sha256 for item in receipts
        ]),
        "receipt_sha256": [item.receipt_sha256 for item in receipts],
        "runtime_seconds": total_elapsed,
        "state": metrics,
        "system_snapshots": system_snapshots,
    }


def _run_resource_summary(run: dict[str, Any]) -> dict[str, Any]:
    frames = run["per_frame"]
    wall = [item["elapsed_seconds"] for item in frames]
    cpu = [
        item["child_cpu_seconds_sampled"]
        for item in frames
        if item["child_cpu_seconds_sampled"] is not None
    ]
    rss = [
        item["peak_child_rss_sampled_bytes"]
        for item in frames
        if item["peak_child_rss_sampled_bytes"] is not None
    ]
    core_equivalents = [
        item["child_cpu_wall_core_equivalents"]
        for item in frames
        if item["child_cpu_wall_core_equivalents"] is not None
    ]
    if not (
        len(wall) == len(cpu) == len(rss) == len(core_equivalents)
        == EXPECTED_FRAMES
    ):
        raise RuntimeError("resource sampler missed one or more replay children")
    wall_distribution = _distribution(wall)
    cpu_distribution = _distribution(cpu)
    result = {
        "child_cpu_seconds": cpu_distribution,
        "child_cpu_wall_core_equivalents": _distribution(core_equivalents),
        "child_peak_rss_bytes": _distribution(rss),
        "frame_count": len(frames),
        "measurement_semantics": {
            "cpu": (
                "OS child-process user+kernel lifetime CPU, sampled while the "
                "spawn child remained live; every forward frame has a value"
            ),
            "rss": (
                "OS lifetime peak working set (Windows PeakWorkingSetSize; "
                "Linux VmHWM), sampled while each spawn child remained live"
            ),
            "wall": "parent perf_counter around each isolated execute call",
        },
        "wall_seconds": wall_distribution,
    }
    for cadence in (300.0, 600.0):
        label = f"cadence_{int(cadence)}s"
        p95 = wall_distribution["nearest_rank_p95"]
        maximum = wall_distribution["maximum"]
        result[label] = {
            "maximum_frame_headroom_seconds": cadence - maximum,
            "maximum_frame_wall_fraction": maximum / cadence,
            "p95_frame_headroom_seconds": cadence - p95,
            "p95_frame_wall_fraction": p95 / cadence,
            "total_cpu_fraction_of_serial_cadence_budget": (
                cpu_distribution["sum"] / (len(frames) * cadence)
            ),
            "total_wall_fraction_of_serial_cadence_budget": (
                wall_distribution["sum"] / (len(frames) * cadence)
            ),
        }
    return result


def _boundary_system_snapshots(authority) -> list[dict[str, Any]]:
    orchestrator = build_calibration_replay_orchestrator(
        authority.authority_document
    )
    snapshot = orchestrator.receipt_store.recovery_snapshot(
        expected_policy_fingerprint=orchestrator._policy.fingerprint
    )
    if snapshot.checkpoint is None or snapshot.checkpoint.revision != 20:
        raise RuntimeError("expected revision-20 calibration checkpoint is absent")
    state = snapshot.checkpoint.state
    selected = {20, 32, 36}
    summaries = [_system_summary(state, 20)]
    for tail in snapshot.tail:
        transition = advance_cell_track_engine(
            state,
            tail.retained_input.frame,
            orchestrator._policy,
            lineage_hypotheses=tail.receipt.lineage_hypotheses,
        )
        if transition.status != "advanced":
            raise RuntimeError("retained calibration tail is not advanced")
        state = transition.next_state
        if state.revision in selected:
            summaries.append(_system_summary(state, state.revision))
    if [item["frame_number"] for item in summaries] != [20, 32, 36]:
        raise RuntimeError("calibration boundary snapshots are incomplete")
    return summaries


def _summarize_existing_corpus(
    authority,
    *,
    label: str,
    include_snapshots: bool,
    prior_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    store = _store(authority)
    receipts = store.advanced_receipts()
    if (
        len(receipts) != EXPECTED_FRAMES
        or tuple(item.next_state_revision for item in receipts)
        != tuple(range(1, EXPECTED_FRAMES + 1))
        or tuple(item.canonical_run_id for item in receipts)
        != authority.export.run_ids
    ):
        raise RuntimeError(f"{label} durable corpus is not exactly complete")
    per_frame = [{
        "accounting": receipt.accounting.to_mapping(),
        "attempt_id": receipt.attempt_id,
        "elapsed_seconds": None,
        "frame_number": receipt.next_state_revision,
        "isolation_enforced": True,
        "next_state_revision": receipt.next_state_revision,
        "peak_child_rss_sampled_bytes": None,
        "previous_advanced_receipt_sha256": (
            receipt.previous_advanced_receipt_sha256
        ),
        "receipt_file_growth_bytes": None,
        "receipt_sha256": receipt.receipt_sha256,
        "run_id": receipt.canonical_run_id,
        "transition_id": receipt.transition_id,
    } for receipt in receipts]
    final_state = recover_calibration_replay_state(authority)
    current_receipt_sha256 = [item.receipt_sha256 for item in receipts]
    current_receipt_chain_sha256 = _sha_value(current_receipt_sha256)
    current_state = _state_metrics(final_state)
    if prior_result is not None:
        if (
            prior_result.get("label") != label
            or prior_result.get("frame_count") != EXPECTED_FRAMES
            or prior_result.get("receipt_sha256") != current_receipt_sha256
            or prior_result.get("receipt_chain_sha256")
            != current_receipt_chain_sha256
            or prior_result.get("state") != current_state
            or not prior_result.get("duplicate_retry_receipts_exact")
        ):
            raise RuntimeError(f"{label} prior evidence does not bind current corpus")
        prior_accounting = [
            item["accounting"] for item in prior_result.get("per_frame", [])
        ]
        if prior_accounting != [item["accounting"] for item in per_frame]:
            raise RuntimeError(f"{label} prior accounting differs from receipts")
        result = dict(prior_result)
        result["resume_validation"] = {
            "current_receipt_chain_exact": True,
            "current_receipts_exact": True,
            "current_recovered_state_exact": True,
            "prior_duplicate_retry_proof_retained": True,
        }
        return result

    duplicate_receipts = []
    duplicate_started = time.perf_counter()
    for frame_number, run_id in enumerate(authority.export.run_ids, start=1):
        result = _runner(authority).execute(
            attempt_id=f"calibration-frame-{frame_number:03d}",
            canonical_run_id=run_id,
        )
        if not (
            result.status == "success"
            and result.committed
            and result.duplicate
        ):
            raise RuntimeError(f"{label} durable duplicate differs")
        duplicate_receipts.append(result.receipt_metadata["receipt_sha256"])
    return {
        "duplicate_retry_elapsed_seconds": time.perf_counter() - duplicate_started,
        "duplicate_retry_receipts_exact": duplicate_receipts == current_receipt_sha256,
        "final_store": store.stats(),
        "frame_count": len(per_frame),
        "label": label,
        "per_frame": per_frame,
        "receipt_chain_sha256": current_receipt_chain_sha256,
        "receipt_sha256": current_receipt_sha256,
        "runtime_seconds": None,
        "runtime_status": (
            "per-frame timings were sampled but intentionally not committed; "
            "the first report attempt aborted only at its final sidecar audit"
        ),
        "state": current_state,
        "system_snapshots": (
            _boundary_system_snapshots(authority) if include_snapshots else []
        ),
    }


def _prior_comparison(analysis: Path, *, sequence_id: str) -> list[dict[str, Any]]:
    comparisons = []
    for name in PRIOR_REPORTS:
        path = analysis / name
        value = json.loads(path.read_text(encoding="utf-8"))
        assignment = value["shadow_assignment"]
        comparisons.append({
            "birth_count": assignment.get("birth_count"),
            "database_sha256_matches": (
                value.get("database_sha256_before") == EXPECTED_DATABASE_SHA256
                and value.get("database_sha256_after") == EXPECTED_DATABASE_SHA256
            ),
            "death_count": assignment.get("death_count"),
            "detection_count": value["hierarchical_replay"].get("detection_count"),
            "frame_count": value["hierarchical_replay"].get("frame_count"),
            "hypothesis_speed_maximum_kmh": assignment.get(
                "chosen_speed_maximum_kmh"
            ),
            "hypothesis_speed_median_kmh": assignment.get(
                "chosen_speed_median_kmh"
            ),
            "hypothesis_speed_p95_kmh": assignment.get("chosen_speed_p95_kmh"),
            "mode": value.get("assignment_policy", {}).get("selected_prior_mode"),
            "report": name,
            "sequence_id_matches": (
                value.get("selected_sequence", {}).get("sequence_id") == sequence_id
            ),
            "track_count": assignment.get("track_count"),
        })
    return comparisons


def _markdown_v4_legacy(report: dict[str, Any], json_name: str) -> str:
    primary = report["primary_run"]
    state = primary["state"]
    deterministic = report["determinism"]
    multi = report["multi_hypothesis_audit"]
    compared = report["v3_exact_input_comparison"]["metrics"]
    lines = [
        "# 36-frame geïsoleerde multi-hypothesis science-shadow replay — KMI normalized cache",
        "",
        "> **Exploratief, nooit admission/skill/live.** Dit is genormaliseerde "
        "Storm Tracker-cache, geen ruwe KMI-data. Intensiteitsklassen zijn geen "
        "fysische neerslageenheden. Provider-nodata, externe celidentiteit en "
        "onafhankelijke snelheidswaarheid ontbreken.",
        "",
        f"Downloadbaar machinebewijs: `{json_name}`.",
        "",
        "## Input en read-only bewijs",
        "",
        f"- Database-SHA-256: `{report['input']['database_sha256_after']}`.",
        f"- Frames: {report['input']['frame_count']} op 600 s cadence; sequence "
        f"`{report['input']['sequence_id']}`.",
        f"- Canonieke export: {report['input']['export_bytes']} bytes, SHA-256 "
        f"`{report['input']['export_file_sha256']}`.",
        f"- Database onveranderd: {report['input']['database_unchanged']}; "
        "de reeds bestaande sidecars dateren van vóór de replay en bleven "
        "content/stat-identiek.",
        "- De child kreeg uitsluitend het gehashte exportbestand en een aparte "
        "transactionele receiptstore; het databasepad zat niet in de authority.",
        "",
        "## Resultaat",
        "",
        f"- Alle {primary['frame_count']} frames: durable `advanced`; eindrevision "
        f"{state['revision']}.",
        f"- Tracks: {state['total_tracks']} totaal, {state['active_tracks']} actief, "
        f"{state['ended_tracks']} beëindigd.",
        f"- Lifecycle: {state['birth_events']} births, {state['death_events']} "
        f"deaths, {len(state['lineage_events'])} expliciete split/merge-events.",
        f"- Lineaire hypothesissnelheden: mediaan "
        f"{state['hypothesis_speed_median_kmh']!r} km/u, p95 "
        f"{state['hypothesis_speed_p95_kmh']!r} km/u, maximum "
        f"{state['hypothesis_speed_maximum_kmh']!r} km/u; "
        f"{state['hypothesis_jump_count_over_200_kmh']} boven 200 km/u.",
        "- Deze snelheden zijn associatiehypothesen tussen detecties, geen "
        "meteorologisch gevalideerde bewegingswaarheid.",
        "- **Meteorologische conclusie: niet betrouwbaar en niet toegelaten.** "
        "De resterende extreme hypothesissnelheden tonen dat geselecteerde identiteit geen "
        "basis voor waarschuwingen of forecasts mag zijn.",
        "",
        "## Begrensde multi-hypothesis identity",
        "",
        f"- Frames met predecessor en top-k-state: {multi['identity_hypothesis_frames']}; "
        "maximaal vier assignmenthypothesen per frame.",
        f"- Volledige candidate-accounting: "
        f"{multi['candidate_accounting_exact_all_frames']}; maximaal "
        f"{multi['maximum_assignment_subproblems_explored']} van "
        f"{multi['maximum_assignment_subproblems_limit']} subproblemen gebruikt.",
        f"- Afgeleide, nooit automatisch geselecteerde lineage-alternatieven: "
        f"{multi['derived_split_alternative_count']} split en "
        f"{multi['derived_merge_alternative_count']} merge.",
        f"- Conditionele top-1-weight mediaan: "
        f"{multi['conditional_top_one_weight_median']!r}; omitted identity mass bleef "
        f"onbekend op {multi['omitted_mass_unknown_frames']} frames.",
        "- Systeemtranslatie wordt vóór identity uit de volledige scène geschat; "
        "lokale celdeformatie is het afzonderlijke residu. Er is geen vaste "
        "snelheidsafkeur.",
        "- De zachte birth/death- en onzekerheidskosten zijn op exact deze cache "
        "gekozen. Dat is exploratieve overfit, geen onafhankelijke kalibratie.",
        "",
        "## Objectieve vergelijking met v3 op exact dezelfde input",
        "",
        f"- >200 km/u: {compared['hypothesis_jump_count_over_200_kmh']['v3_one_best']} "
        f"→ {compared['hypothesis_jump_count_over_200_kmh']['v4_bounded_multi_hypothesis']}.",
        f"- Mediaan/p95/max km/u: "
        f"{compared['hypothesis_speed_median_kmh']['v3_one_best']!r}/"
        f"{compared['hypothesis_speed_p95_kmh']['v3_one_best']!r}/"
        f"{compared['hypothesis_speed_maximum_kmh']['v3_one_best']!r} → "
        f"{compared['hypothesis_speed_median_kmh']['v4_bounded_multi_hypothesis']!r}/"
        f"{compared['hypothesis_speed_p95_kmh']['v4_bounded_multi_hypothesis']!r}/"
        f"{compared['hypothesis_speed_maximum_kmh']['v4_bounded_multi_hypothesis']!r}.",
        f"- Continuïteit: tracks "
        f"{compared['total_tracks']['v3_one_best']} → "
        f"{compared['total_tracks']['v4_bounded_multi_hypothesis']}; intervals "
        f"{compared['hypothesis_interval_count']['v3_one_best']} → "
        f"{compared['hypothesis_interval_count']['v4_bounded_multi_hypothesis']}. "
        "Minder sprongen gaan dus samen met meer fragmentatie.",
        "- Dit is geen nauwkeurigheidswinstclaim: onafhankelijke identity- en "
        "speed-truth ontbreken.",
        f"- Volledige twee-run-commandowandklok: "
        f"{report['runtime_and_resources']['completed_command_wall_seconds']:.1f} s. "
        f"Finale primaire store: "
        f"{primary['final_store']['database_file_bytes']} bytes; checkpointrevision "
        f"{primary['final_store']['checkpoint_revision']}; replaytail "
        f"{primary['final_store']['replay_tail_frames']} frames.",
        f"- Representatieve verse-child RSS-sample: "
        f"{report['runtime_and_resources']['rss_probe']['peak_child_rss_sampled_bytes']} "
        "bytes. Dit is een ondergrens; de harde RSS-limiet bleef 1 GiB.",
        "",
        "## Restart en determinisme",
        "",
        "- Verse parent-runner werd expliciet gemaakt bij 20→21 en 32→33. "
        "Checkpointinterval was 20; frame 21 herstelde dus checkpoint 20 zonder "
        "prefixreplay, frame 33 herstelde checkpoint 20 plus de contigue tail.",
        f"- Exacte duplicate-retry: {primary['duplicate_retry_receipts_exact']}.",
        f"- Onafhankelijke tweede receiptstore: receiptketen exact "
        f"{deterministic['receipt_chain_exact']}; finale state exact "
        f"{deterministic['final_state_exact']}; per-frame accounting exact "
        f"{deterministic['per_frame_accounting_exact']}.",
        "",
        "## WeatherSystem",
        "",
        "WeatherSystem is alleen een beschrijvende, afgeleide groepering. Het "
        "classificeert geen MCS en bevestigt geen identiteit. De JSON bevat de "
        "volledige snapshots op frame 20, 32 en 36, inclusief members, footprints, "
        "connectiebesluiten, onzekerheden en beschrijvende motion.",
        "",
        "## Vergelijkbaarheid met oudere replays",
        "",
        "De oudere rapporten gebruiken exact dezelfde database-SHA en sequence, "
        "maar andere assignment-/motionpolicies. Verschillen zijn dus "
        "algoritmeverschillen, geen nauwkeurigheidswinst zonder externe truth.",
        "",
        "## Grenzen",
        "",
    ]
    lines.extend(f"- `{item}`" for item in report["caveats"])
    lines.extend(["", "Operationele blockers:", ""])
    lines.extend(f"- `{item}`" for item in report["operational_blockers"])
    return "\n".join(lines) + "\n"


def _markdown(report: dict[str, Any], json_name: str) -> str:
    primary = report["primary_run"]
    multi = report["multi_hypothesis_audit"]["multi_scan"]
    selected = multi["selected_decision_path"]
    resources = report["runtime_and_resources"][
        "primary_all_36_forward_frames"
    ]
    v4 = report["v4_1_exact_input_comparison"]["metrics"]
    lines = [
        "# 36-frame geisoleerde multi-scan science-shadow replay - KMI normalized cache",
        "",
        "> **Exploratief, nooit admission/skill/live.** De input is genormaliseerde ",
        "> Storm Tracker-cache, geen raw KMI-data. Klassen zijn niet fysisch; ",
        "> provider-nodata, onafhankelijke identiteit en speed truth ontbreken.",
        "",
        f"Machinebewijs: `{json_name}`.",
        "",
        "## Input en integriteit",
        "",
        f"- Database SHA-256: `{report['input']['database_sha256_after']}`; "
        f"onveranderd: {report['input']['database_unchanged']}.",
        f"- {report['input']['frame_count']} frames op 600 s cadence; sequence "
        f"`{report['input']['sequence_id']}`.",
        f"- Canonieke export SHA-256: `{report['input']['export_file_sha256']}`.",
        "- Ieder frame draaide in een apart spawn-child met CPU/RSS/walllimieten; "
        "de child kreeg geen calibratiedatabasepad.",
        "",
        "## Multi-scan resultaat",
        "",
        f"- Geselecteerd branch-consistent pad: {selected['total_tracks']} tracks, "
        f"{selected['hypothesis_interval_count']} intervallen, "
        f"{selected['birth_events']} births en {selected['death_events']} deaths.",
        f"- Snelheidshypothesen mediaan/p95/max: "
        f"{selected['hypothesis_speed_median_kmh']!r}/"
        f"{selected['hypothesis_speed_p95_kmh']!r}/"
        f"{selected['hypothesis_speed_maximum_kmh']!r} km/u; "
        f"{selected['hypothesis_jump_count_over_200_kmh']} boven 200 km/u.",
        f"- N-scan commit t/m revision {multi['committed_through_revision']}; "
        f"{multi['uncommitted_map_decision_count']} recente MAP-beslissingen bleven "
        "expliciet herzienbaar.",
        f"- Beam maximaal {multi['branch_count_maximum']}; candidate-accounting "
        f"exact: {multi['candidate_accounting_exact_all_frames']}; generated "
        f"accounting exact: {multi['generated_accounting_exact_all_frames']}.",
        f"- Conditionele top-1-weight p50/p95/max: "
        f"{multi['conditional_top_one_weight']['median_p50']!r}/"
        f"{multi['conditional_top_one_weight']['nearest_rank_p95']!r}/"
        f"{multi['conditional_top_one_weight']['maximum']!r}. Deze weights zijn "
        "geen gekalibreerde waarschijnlijkheden.",
        f"- Onselected lineage-alternatieven op het gekozen pad: "
        f"{multi['selected_split_alternative_count']} split en "
        f"{multi['selected_merge_alternative_count']} merge.",
        "- Systeemtranslatie en lokale deformatie zijn afzonderlijke kosten; er is "
        "geen vaste snelheidsafkeur.",
        "- WeatherSystem is niet opnieuw uit het multi-scan MAP-pad opgebouwd; de "
        "bestaande snapshots blijven aan de primaire per-transition lifecycle gebonden.",
        "",
        "## Vergelijking met v4.1 op exact dezelfde input",
        "",
        f"- >200 km/u: "
        f"{v4['hypothesis_jump_count_over_200_kmh']['v4_1_per_transition_top_k_rank1']} "
        f"-> {v4['hypothesis_jump_count_over_200_kmh']['v5_multi_scan_selected_path']}.",
        f"- Tracks: {v4['total_tracks']['v4_1_per_transition_top_k_rank1']} -> "
        f"{v4['total_tracks']['v5_multi_scan_selected_path']}; intervals: "
        f"{v4['hypothesis_interval_count']['v4_1_per_transition_top_k_rank1']} -> "
        f"{v4['hypothesis_interval_count']['v5_multi_scan_selected_path']}.",
        "- Dit is geen nauwkeurigheidsclaim: onafhankelijke identity/speed truth ontbreekt.",
        "",
        "## Volledige 36-frame processbelasting",
        "",
        f"- Wall p50/p95/max: {resources['wall_seconds']['median_p50']:.3f}/"
        f"{resources['wall_seconds']['nearest_rank_p95']:.3f}/"
        f"{resources['wall_seconds']['maximum']:.3f} s.",
        f"- Child CPU p50/p95/max: "
        f"{resources['child_cpu_seconds']['median_p50']:.3f}/"
        f"{resources['child_cpu_seconds']['nearest_rank_p95']:.3f}/"
        f"{resources['child_cpu_seconds']['maximum']:.3f} CPU-s.",
        f"- Peak child RSS p50/p95/max: "
        f"{resources['child_peak_rss_bytes']['median_p50']:.0f}/"
        f"{resources['child_peak_rss_bytes']['nearest_rank_p95']:.0f}/"
        f"{resources['child_peak_rss_bytes']['maximum']:.0f} bytes.",
        f"- 300 s cadence p95/max headroom: "
        f"{resources['cadence_300s']['p95_frame_headroom_seconds']:.3f}/"
        f"{resources['cadence_300s']['maximum_frame_headroom_seconds']:.3f} s.",
        f"- Finale primaire store: {primary['final_store']['database_file_bytes']} "
        f"bytes; checkpointrevision {primary['final_store']['checkpoint_revision']}; "
        f"tail {primary['final_store']['replay_tail_frames']}.",
        "",
        "## Restart en determinisme",
        "",
        "- Verse runnergrenzen: 20->21 en 32->33; checkpoint/replaytail bleef exact.",
        f"- Duplicate receipts exact: {primary['duplicate_retry_receipts_exact']}; "
        f"receiptketen exact: {report['determinism']['receipt_chain_exact']}; "
        f"finale state exact: {report['determinism']['final_state_exact']}.",
        "",
        "## Grenzen en blockers",
        "",
    ]
    lines.extend(f"- `{item}`" for item in report["caveats"])
    lines.extend(["", "Operationele blockers:", ""])
    lines.extend(f"- `{item}`" for item in report["operational_blockers"])
    return "\n".join(lines) + "\n"


def main() -> int:
    analysis = (ROOT / ".analysis").resolve()
    analysis.mkdir(parents=True, exist_ok=True)
    database = (ROOT / DATABASE_RELATIVE).resolve(strict=True)
    export_path = analysis / f"{ARTIFACT_STEM}_export.json"
    json_path = analysis / f"{EVIDENCE_STEM}_evidence.json"
    markdown_path = analysis / f"{EVIDENCE_STEM}_evidence.md"
    if json_path.exists() or markdown_path.exists():
        raise RuntimeError("final evidence artifact already exists")
    before_sha = _sha_file(database)
    before_stat = database.stat()
    sidecars_before = _sidecar_snapshot(database)
    if before_sha != EXPECTED_DATABASE_SHA256:
        raise RuntimeError("calibration database SHA differs")
    export_receipt = export_normalized_cache_sequence(
        database,
        export_path,
        region_id=REGION_ID,
        source=SOURCE,
        expected_database_sha256=EXPECTED_DATABASE_SHA256,
        expected_frame_count=EXPECTED_FRAMES,
        minimum_sequence_frames=20,
    )

    isolation_limits = ScienceShadowIsolationLimits(
        wall_seconds=60.0,
        cpu_seconds=45.0,
        rss_bytes=1024 * 1024**2,
        max_authority_bytes=1024 * 1024,
        max_request_bytes=32 * 1024,
        max_result_bytes=64 * 1024,
        max_authorized_paths=8,
    )
    science_limits = ScienceShadowLimits(
        max_receipts=128,
        max_replay_frames=64,
        checkpoint_interval_frames=20,
        max_runtime_seconds=45.0,
    )
    primary = create_calibration_replay_authority(
        export_path=export_path,
        receipt_path=analysis / f"{ARTIFACT_STEM}_primary.sqlite3",
        authority_path=analysis / f"{ARTIFACT_STEM}_primary_authority.json",
        isolation_limits=isolation_limits,
        science_limits=science_limits,
        maximum_source_grid_cells=700_000,
        maximum_analysis_pixels=20_000,
        maximum_runtime_seconds=45.0,
    )
    secondary = create_calibration_replay_authority(
        export_path=export_path,
        receipt_path=analysis / f"{ARTIFACT_STEM}_repeat.sqlite3",
        authority_path=analysis / f"{ARTIFACT_STEM}_repeat_authority.json",
        isolation_limits=isolation_limits,
        science_limits=science_limits,
        maximum_source_grid_cells=700_000,
        maximum_analysis_pixels=20_000,
        maximum_runtime_seconds=45.0,
    )
    if primary.export.run_ids != secondary.export.run_ids:
        raise RuntimeError("authority exports differ")

    primary_stats = _store(primary).stats()
    secondary_stats = _store(secondary).stats()
    if primary_stats["head_revision"] == secondary_stats["head_revision"] == 0:
        command_started = time.perf_counter()
        primary_result = _run_corpus(
            primary,
            label="primary",
            include_snapshots=True,
        )
        secondary_result = _run_corpus(
            secondary,
            label="independent-repeat",
            include_snapshots=False,
        )
        completed_command_wall_seconds = time.perf_counter() - command_started
    elif primary_stats["head_revision"] == secondary_stats["head_revision"] == 36:
        raise RuntimeError(
            "completed v5 stores exist without final evidence; per-frame OS "
            "resource traces cannot be reconstructed, so evidence generation "
            "fails closed instead of substituting a representative sample"
        )
    else:
        raise RuntimeError("calibration stores are partial or asymmetric")
    multi_hypothesis_audit = _multi_hypothesis_audit(primary)
    multi_scan_identity = multi_hypothesis_audit["multi_scan"][
        "selected_decision_path"
    ]
    v3_comparison = _v3_comparison(
        analysis,
        multi_scan_identity,
        sequence_id=primary.export.sequence_id,
    )
    v4_comparison = _v4_comparison(
        analysis,
        multi_scan_identity,
        sequence_id=primary.export.sequence_id,
    )
    primary_resources = _run_resource_summary(primary_result)
    repeat_resources = _run_resource_summary(secondary_result)
    after_sha = _sha_file(database)
    after_stat = database.stat()
    sidecars_after = _sidecar_snapshot(database)
    sidecars = [str(DATABASE_RELATIVE) + suffix for suffix in sidecars_after]
    replay_started_ns = min(
        primary.authority_path.stat().st_ctime_ns,
        primary.authority_path.stat().st_mtime_ns,
    )
    database_unchanged = (
        before_sha == after_sha == EXPECTED_DATABASE_SHA256
        and (before_stat.st_size, before_stat.st_mtime_ns)
        == (after_stat.st_size, after_stat.st_mtime_ns)
        and sidecars_before == sidecars_after
    )
    if not database_unchanged:
        raise RuntimeError("calibration database read-only proof failed")

    primary_accounting = [item["accounting"] for item in primary_result["per_frame"]]
    secondary_accounting = [
        item["accounting"] for item in secondary_result["per_frame"]
    ]
    boundary_support = hard_boundary_support()
    report = {
        "artifact_contract": "stv3-science-shadow-calibration-evidence-v3",
        "caveats": list(CALIBRATION_REPLAY_CAVEATS),
        "determinism": {
            "final_state_exact": (
                primary_result["state"]["final_state_sha256"]
                == secondary_result["state"]["final_state_sha256"]
            ),
            "per_frame_accounting_exact": primary_accounting == secondary_accounting,
            "receipt_chain_exact": (
                primary_result["receipt_chain_sha256"]
                == secondary_result["receipt_chain_sha256"]
            ),
            "transition_ids_exact": [
                item["transition_id"] for item in primary_result["per_frame"]
            ] == [
                item["transition_id"] for item in secondary_result["per_frame"]
            ],
            "direct_audit_final_state_exact": (
                multi_hypothesis_audit[
                    "final_state_exactly_matches_isolated_head"
                ]
            ),
        },
        "hard_process_boundary": {
            "limits": isolation_limits.to_mapping(),
            "spawn_start_method": multiprocessing.get_context("spawn").get_start_method(),
            "support": {
                "mechanism": boundary_support.mechanism,
                "platform": boundary_support.platform,
                "scope": boundary_support.scope,
                "supported": boundary_support.supported,
            },
        },
        "input": {
            "authority_contains_database_path": any(
                str(database) in json.dumps(item.authority_document)
                for item in (primary, secondary)
            ),
            "connection_mode": CALIBRATION_REPLAY_CONNECTION_MODE,
            "database_bytes": before_stat.st_size,
            "database_path": str(DATABASE_RELATIVE).replace("\\", "/"),
            "database_sha256_after": after_sha,
            "database_sha256_before": before_sha,
            "database_sidecars_present": sidecars,
            "database_sidecars_snapshot": sidecars_after,
            "database_sidecars_unchanged_during_final_audit": (
                sidecars_before == sidecars_after
            ),
            "database_sidecars_predate_completed_replay": all(
                item["last_write_ns"] < replay_started_ns
                for item in sidecars_after.values()
            ),
            "database_unchanged": database_unchanged,
            "export_bytes": export_receipt.export_bytes,
            "export_file": export_path.name,
            "export_file_sha256": export_receipt.export_sha256,
            "frame_count": len(primary.export.frames),
            "per_frame_run_ids": list(primary.export.run_ids),
            "region_id": primary.export.region_id,
            "sequence_id": primary.export.sequence_id,
            "source": primary.export.source,
            "source_database_sha256_in_export": (
                primary.export.source_database_sha256
            ),
        },
        "meteorological_admission_granted": False,
        "multi_hypothesis_audit": multi_hypothesis_audit,
        "operational_blockers": list(CALIBRATION_REPLAY_BLOCKERS),
        "operational_snapshot_claim_generated": False,
        "primary_run": primary_result,
        "prior_exact_input_comparison": _prior_comparison(
            analysis,
            sequence_id=primary.export.sequence_id,
        ),
        "repeat_run": secondary_result,
        "runtime_and_resources": {
            "completed_command_wall_seconds": completed_command_wall_seconds,
            "completed_command_wall_source": (
                "captured in-process wall time around the successful fresh "
                "36+36 isolated process run"
            ),
            "primary_store_write_window_upper_bound_seconds": max(
                0.0,
                primary.receipt_path.stat().st_mtime
                - primary.receipt_path.stat().st_ctime,
            ),
            "primary_all_36_forward_frames": primary_resources,
            "repeat_all_36_forward_frames": repeat_resources,
            "store_growth": {
                "empty_store_file_bytes": primary_result[
                    "initial_store_file_bytes"
                ],
                "final_primary_file_bytes": primary.receipt_path.stat().st_size,
                "final_primary_growth_bytes": (
                    primary.receipt_path.stat().st_size
                    - primary_result["initial_store_file_bytes"]
                ),
                "final_repeat_file_bytes": secondary.receipt_path.stat().st_size,
            },
        },
        "science_limits": science_limits.to_mapping(),
        "v3_exact_input_comparison": v3_comparison,
        "v4_1_exact_input_comparison": v4_comparison,
    }
    if not all(report["determinism"].values()):
        raise RuntimeError("independent deterministic replay differs")
    if report["input"]["authority_contains_database_path"]:
        raise RuntimeError("source database path leaked into child authority")

    body = _canonical_json_bytes(report)
    json_path.write_bytes(body)
    markdown_path.write_text(
        _markdown(report, json_path.name),
        encoding="utf-8",
        newline="\n",
    )
    print(json.dumps({
        "database_unchanged": database_unchanged,
        "determinism": report["determinism"],
        "json": str(json_path),
        "markdown": str(markdown_path),
        "primary_runtime_seconds": primary_result["runtime_seconds"],
        "repeat_runtime_seconds": secondary_result["runtime_seconds"],
        "state": primary_result["state"],
    }, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
