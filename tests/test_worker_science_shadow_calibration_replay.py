"""Read-only and restart proofs for the normalized-cache science replay."""
from __future__ import annotations

import ast
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from scripts import run_science_shadow_calibration_replay as replay_script
from storm_tracker_worker.hard_decoder_isolation import _canonical_json_bytes
from storm_tracker_worker.science_shadow_calibration_replay import (
    CALIBRATION_REPLAY_CAVEATS,
    calibration_replay_detector_config,
    calibration_replay_tracking_policy,
    create_calibration_replay_authority,
    export_normalized_cache_sequence,
    load_calibration_replay_export,
    recover_calibration_replay_state,
)
from storm_tracker_worker.science_shadow_process_isolation import (
    ScienceShadowIsolatedRunner,
    ScienceShadowIsolationLimits,
)
from storm_tracker_worker.science_shadow_orchestrator import (
    ScienceShadowLimits,
    ScienceShadowReceiptStore,
)


ROOT = Path(__file__).parents[1]
REGION = "region-test"
SOURCE = "kmi"
START = 1_700_000_000


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(64 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _create_cache(path: Path, *, count: int = 3) -> None:
    with closing(sqlite3.connect(path)) as database, database:
        database.executescript(
            """
            CREATE TABLE sentinel(value TEXT NOT NULL);
            INSERT INTO sentinel(value) VALUES('read-only-proof');
            CREATE TABLE pysteps_frame_cache (
                region_id TEXT NOT NULL,
                source TEXT NOT NULL,
                nominal_timestamp INTEGER NOT NULL,
                saved_at REAL NOT NULL,
                frame_json TEXT NOT NULL,
                PRIMARY KEY(region_id, source, nominal_timestamp)
            ) WITHOUT ROWID;
            """
        )
        for index in range(count):
            cells = []
            for row in range(4, 8):
                for column in range(4 + index, 8 + index):
                    intensity = 6.0 if row in (5, 6) and column in (
                        5 + index,
                        6 + index,
                    ) else 2.0
                    cells.append([row, column, intensity])
            timestamp = START + index * 600
            frame = {
                "bounds": [4.0, 50.0, 4.24, 50.16],
                "cells": cells,
                "grid_deg": 0.01,
                "height": 16,
                "input_runs": 1,
                "nominal_timestamp": timestamp,
                "source": SOURCE,
                "width": 24,
            }
            database.execute(
                "INSERT INTO pysteps_frame_cache VALUES(?,?,?,?,?)",
                (
                    REGION,
                    SOURCE,
                    timestamp,
                    float(timestamp + 1),
                    json.dumps(frame, separators=(",", ":")),
                ),
            )


def _export(tmp_path: Path, *, count: int = 3):
    database = (tmp_path / "calibration.sqlite3").resolve()
    export = (tmp_path / "cache-export.json").resolve()
    _create_cache(database, count=count)
    database_sha = _sha(database)
    receipt = export_normalized_cache_sequence(
        database,
        export,
        region_id=REGION,
        source=SOURCE,
        expected_database_sha256=database_sha,
        expected_frame_count=count,
        minimum_sequence_frames=2,
    )
    return database, export, receipt


def _runner(authority) -> ScienceShadowIsolatedRunner:
    return ScienceShadowIsolatedRunner(
        authority_path=authority.authority_path,
        expected_authority_sha256=authority.authority_sha256,
        factory_contract=authority.factory_contract,
        limits=authority.isolation_limits,
    )


def test_export_is_canonical_content_addressed_and_database_remains_unchanged(
    tmp_path: Path,
) -> None:
    database = (tmp_path / "calibration.sqlite3").resolve()
    export = (tmp_path / "cache-export.json").resolve()
    _create_cache(database, count=3)
    before_sha = _sha(database)
    before_stat = database.stat()

    receipt = export_normalized_cache_sequence(
        database,
        export,
        region_id=REGION,
        source=SOURCE,
        expected_database_sha256=before_sha,
        expected_frame_count=3,
        minimum_sequence_frames=2,
    )
    loaded = load_calibration_replay_export(
        export,
        expected_file_sha256=receipt.export_sha256,
    )
    repeated = export_normalized_cache_sequence(
        database,
        export,
        region_id=REGION,
        source=SOURCE,
        expected_database_sha256=before_sha,
        expected_frame_count=3,
        minimum_sequence_frames=2,
    )

    assert receipt == repeated
    assert receipt.source_database_unchanged
    assert receipt.export_sha256 == _sha(export)
    assert len(loaded.frames) == len(set(loaded.run_ids)) == 3
    assert loaded.caveats == CALIBRATION_REPLAY_CAVEATS
    assert _canonical_json_bytes(json.loads(export.read_bytes())) == export.read_bytes()
    assert _sha(database) == before_sha
    after_stat = database.stat()
    assert (after_stat.st_size, after_stat.st_mtime_ns) == (
        before_stat.st_size,
        before_stat.st_mtime_ns,
    )
    with closing(sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True)) as check:
        assert check.execute("SELECT value FROM sentinel").fetchone()[0] == (
            "read-only-proof"
        )
    for suffix in ("-journal", "-shm", "-wal"):
        assert not Path(str(database) + suffix).exists()


def test_authority_tamper_fails_closed_before_spawn_and_keeps_empty_head(
    tmp_path: Path,
) -> None:
    _database, export, _receipt = _export(tmp_path)
    authority = create_calibration_replay_authority(
        export_path=export,
        receipt_path=(tmp_path / "receipts.sqlite3").resolve(),
        authority_path=(tmp_path / "authority.json").resolve(),
        maximum_source_grid_cells=1_000,
        maximum_analysis_pixels=1_000,
        maximum_runtime_seconds=10.0,
    )
    body = bytearray(export.read_bytes())
    body[-2] = ord("0") if body[-2] != ord("0") else ord("1")
    export.write_bytes(body)

    result = _runner(authority).execute(
        attempt_id="tampered-export",
        canonical_run_id=authority.export.run_ids[0],
    )

    assert result.status == "corrupt_contract"
    assert result.execution_outcome == "authority_rejected"
    assert result.durable_disposition == "not_committed"
    store = ScienceShadowReceiptStore(
        authority.receipt_path,
        engine_id=authority.authority_document["engine_id"],
        runtime_profile_sha256=(
            authority.authority_document["runtime_profile_sha256"]
        ),
        limits=authority.science_limits,
    )
    assert store.stats()["head_revision"] == 0


def test_isolated_replay_restarts_and_duplicate_is_exact(tmp_path: Path) -> None:
    _database, export, _receipt = _export(tmp_path, count=6)
    limits = ScienceShadowIsolationLimits(
        wall_seconds=20.0,
        cpu_seconds=15.0,
        rss_bytes=1024 * 1024**2,
        max_authority_bytes=1024 * 1024,
        max_request_bytes=32 * 1024,
        max_result_bytes=64 * 1024,
        max_authorized_paths=8,
    )
    authority = create_calibration_replay_authority(
        export_path=export,
        receipt_path=(tmp_path / "receipts.sqlite3").resolve(),
        authority_path=(tmp_path / "authority.json").resolve(),
        isolation_limits=limits,
        science_limits=ScienceShadowLimits(
            max_receipts=64,
            checkpoint_interval_frames=2,
            max_replay_frames=16,
            max_runtime_seconds=10.0,
        ),
        maximum_source_grid_cells=1_000,
        maximum_analysis_pixels=1_000,
        maximum_runtime_seconds=10.0,
    )

    results = []
    for index, run_id in enumerate(authority.export.run_ids, start=1):
        # Every invocation constructs a fresh public runner.  Revisions five
        # and six cross the four-frame N-scan horizon after durable checkpoints.
        results.append(
            _runner(authority).execute(
                attempt_id=f"calibration-frame-{index:03d}",
                canonical_run_id=run_id,
            )
        )
    duplicate = _runner(authority).execute(
        attempt_id="calibration-frame-006",
        canonical_run_id=authority.export.run_ids[5],
    )

    for revision, result in enumerate(results, start=1):
        assert result.status == "success", result.to_mapping()
        assert result.committed and not result.duplicate
        assert result.receipt_metadata["outcome"] == "advanced"
        assert result.receipt_metadata["next_state_revision"] == revision
        assert result.receipt_metadata["meteorological_admission_granted"] is False
        assert result.receipt_metadata["operational_admission_blockers"]
        assert result.receipt_metadata["operational_snapshot_claim_generated"] is False
        assert result.receipt_metadata["provenance"]["capture_manifest_sha256"] == (
            authority.authority_document["factory_config"]["export_file_sha256"]
        )
    assert duplicate.status == "success" and duplicate.duplicate
    assert duplicate.receipt_metadata == results[-1].receipt_metadata
    state = recover_calibration_replay_state(authority)
    assert state.revision == 6
    assert state.last_frame is not None
    assert state.last_frame.frame_id.endswith(str(START + 5 * 600))
    assert state.weather_systems is not None
    assert state.identity_snapshot is not None
    assert state.identity_snapshot.revision == 6
    assert state.identity_snapshot.candidate_edge_count == (
        state.identity_snapshot.expected_candidate_edge_count
    )
    assert len(state.identity_snapshot.hypotheses) <= 4
    assert state.identity_snapshot.omitted_hypothesis_mass_unknown
    assert state.multi_scan_posterior is not None
    assert state.multi_scan_posterior.revision == 6
    assert state.multi_scan_posterior.committed_through_revision == 2
    assert state.multi_scan_posterior.committed_decision_count == 2
    assert len(state.multi_scan_posterior.branches) <= 8
    assert all(
        len(branch.steps) <= 4 for branch in state.multi_scan_posterior.branches
    )
    stats = ScienceShadowReceiptStore(
        authority.receipt_path,
        engine_id=authority.authority_document["engine_id"],
        runtime_profile_sha256=(
            authority.authority_document["runtime_profile_sha256"]
        ),
        limits=authority.science_limits,
    ).stats()
    assert stats["checkpoint_revision"] == 6
    assert stats["replay_tail_frames"] == 0


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_calibration_replay_is_unwired_and_contains_no_network_stack() -> None:
    candidate = "storm_tracker_worker.science_shadow_calibration_replay"
    assert candidate not in _imports(ROOT / "storm_tracker_worker" / "service.py")
    for path in (ROOT / "custom_components" / "storm_tracker_v3").rglob("*.py"):
        assert candidate not in _imports(path)
    imports = _imports(
        ROOT / "storm_tracker_worker" / "science_shadow_calibration_replay.py"
    )
    assert not imports.intersection(
        {"aiohttp", "http", "requests", "socket", "urllib", "websockets"}
    )

    detector = calibration_replay_detector_config()
    policy = calibration_replay_tracking_policy(
        detector,
        maximum_analysis_pixels=1_000,
    )
    maximum_window_samples = (2 * policy.lucas_kanade.window_radius_pixels + 1) ** 2
    assert policy.lucas_kanade.minimum_valid_samples > maximum_window_samples
    assert policy.maximum_exact_current_detections == 8
    assert policy.scalable_one_best_fallback is False
    assert policy.multi_hypothesis_identity is not None
    assert policy.multi_hypothesis_identity.solution_limit == 4
    assert policy.multi_hypothesis_identity.maximum_assignment_subproblems == 4_096
    assert policy.multi_hypothesis_identity.multi_scan is not None
    assert policy.multi_hypothesis_identity.multi_scan.horizon_frames == 4
    assert policy.multi_hypothesis_identity.multi_scan.beam_width == 8


def test_rss_sampler_tolerates_process_closed_after_active_snapshot(
    monkeypatch,
) -> None:
    class ClosedProcess:
        @property
        def pid(self):
            raise ValueError("process object is closed")

    class StopAfterOneSample:
        calls = 0

        def wait(self, _seconds: float) -> bool:
            self.calls += 1
            return self.calls > 1

    monkeypatch.setattr(
        replay_script.multiprocessing,
        "active_children",
        lambda: [ClosedProcess()],
    )
    assert replay_script._active_child_pids() == set()
    peaks: dict[int, int] = {}
    replay_script._sample_children(StopAfterOneSample(), peaks)
    assert peaks == {}


def test_resource_summary_requires_all_36_forward_child_measurements() -> None:
    frames = [
        {
            "child_cpu_seconds_sampled": float(index),
            "child_cpu_wall_core_equivalents": 0.5,
            "elapsed_seconds": float(index * 2),
            "peak_child_rss_sampled_bytes": index * 1_000_000,
        }
        for index in range(1, 37)
    ]
    summary = replay_script._run_resource_summary({"per_frame": frames})

    assert summary["frame_count"] == 36
    assert summary["wall_seconds"]["median_p50"] == 37.0
    assert summary["wall_seconds"]["nearest_rank_p95"] == 70.0
    assert summary["wall_seconds"]["maximum"] == 72.0
    assert summary["child_cpu_seconds"]["count"] == 36
    assert summary["child_peak_rss_bytes"]["maximum"] == 36_000_000.0
    assert summary["cadence_300s"]["maximum_frame_headroom_seconds"] == 228.0

    frames[-1] = {**frames[-1], "child_cpu_seconds_sampled": None}
    with pytest.raises(RuntimeError, match="missed"):
        replay_script._run_resource_summary({"per_frame": frames})


def test_resource_sampler_records_lifetime_rss_and_cpu_for_live_child(
    monkeypatch,
) -> None:
    class Child:
        pid = 4321

    class OneSample:
        calls = 0

        def wait(self, _seconds: float) -> bool:
            self.calls += 1
            return self.calls > 1

    monkeypatch.setattr(
        replay_script.multiprocessing,
        "active_children",
        lambda: [Child()],
    )
    monkeypatch.setattr(replay_script, "_windows_rss", lambda _pid: 123_456)
    monkeypatch.setattr(replay_script, "_linux_rss", lambda _pid: None)
    monkeypatch.setattr(
        replay_script,
        "_windows_cpu_seconds",
        lambda _pid: 1.25,
    )
    monkeypatch.setattr(replay_script, "_linux_cpu_seconds", lambda _pid: None)
    peaks: dict[int, int] = {}
    cpu: dict[int, float] = {}

    replay_script._sample_children(OneSample(), peaks, cpu)

    assert peaks == {4321: 123_456}
    assert cpu == {4321: 1.25}
