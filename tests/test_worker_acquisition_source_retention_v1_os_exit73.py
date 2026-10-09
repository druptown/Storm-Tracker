"""Real-process crash proof for every v1 retention export checkpoint."""
from __future__ import annotations

import ast
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from storm_tracker_worker import autonomous_scheduler
from storm_tracker_worker import desired_state
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
)
from storm_tracker_worker.acquisition_execution_store import ExecutionReceiptStore
from storm_tracker_worker.acquisition_source_retention import (
    AcquisitionSourceCandidate,
    AcquisitionSourceRetentionArchive,
    AcquisitionSourceRetentionLimits,
)
from storm_tracker_worker.raw_artifacts import RawArtifactLimits, RawArtifactStore
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)

from test_worker_acquisition_source_retention import (
    Authorities,
    NOW,
    SYNC,
    _archive,
    _authorities,
)


_V1_RETENTION_SUFFIXES = (
    "before_stage",
    "after_stage_fsync",
    "after_publish",
    "after_directory_fsync",
)
_V1_RETENTION_LABELS = ("segment", "manifest", "anchor")
_V1_RETENTION_CRASH_POINTS = tuple(
    f"{label}.{suffix}"
    for label in _V1_RETENTION_LABELS
    for suffix in _V1_RETENTION_SUFFIXES
)
_V1_RETENTION_COMMITTED_POINTS = frozenset({
    "anchor.after_publish",
    "anchor.after_directory_fsync",
})


def _reopen_v1_retention_case(
    case_root: Path,
    candidate: AcquisitionSourceCandidate,
    *,
    fault=None,
) -> tuple[Authorities, AcquisitionSourceRetentionArchive]:
    source_root = (case_root / "sources").resolve()
    desired = desired_state.DesiredStateStore(
        (source_root / "desired.sqlite3").resolve()
    )
    scheduler = autonomous_scheduler.AutonomousScheduler(
        (source_root / "scheduler.sqlite3").resolve(),
        desired_state_store=desired,
        plan_builder=lambda _state: (),
        clock=lambda: NOW + 100.0,
        lease_seconds=60.0,
    )
    raw = RawArtifactStore(
        (source_root / "raw").resolve(),
        limits=RawArtifactLimits(
            max_object_bytes=2 * 1024 * 1024,
            max_total_bytes=8 * 1024 * 1024,
            max_object_entries=32,
            max_staging_entries=8,
            max_receipt_entries=32,
            max_capture_entries=32,
            max_capture_members=4,
            max_catalog_bytes=4 * 1024 * 1024,
        ),
    )
    raw.initialize()
    capacity = RawCapacityLedger(
        (source_root / "capacity.sqlite3").resolve(),
        profile=RawCapacityProfile(
            global_max_bytes=8 * 1024 * 1024,
            global_max_entries=32,
            providers=(
                ProviderCapacityLimit("dwd", 8 * 1024 * 1024, 32),
            ),
            max_reservations=32,
            max_catalog_bytes=4 * 1024 * 1024,
            sqlite_busy_timeout_seconds=2.0,
        ),
        clock=lambda: NOW + 100.0,
    )
    executions = ExecutionReceiptStore(
        (source_root / "execution.sqlite3").resolve(),
        clock=lambda: NOW + 100.0,
    )
    evidence = AcquisitionEvidenceLifecycleStore(
        (source_root / "evidence").resolve(),
        clock=lambda: NOW + 100.0,
        directory_fsync=SYNC,
    )
    receipt = raw.load_capture(candidate.raw_capture_id)
    assert receipt is not None and len(receipt.members) == 1
    artifact = raw.load_receipt(receipt.members[0].receipt_id)
    assert artifact is not None
    authorities = Authorities(
        raw=raw,
        scheduler=scheduler,
        executions=executions,
        capacity=capacity,
        evidence=evidence,
        candidates=(candidate,),
        object_paths=(Path(artifact.artifact_path),),
    )
    archive = _archive(case_root / "archive", authorities, fault=fault)
    return authorities, archive


def _v1_retention_fault_inventory() -> frozenset[str]:
    source = Path(
        sys.modules[AcquisitionSourceRetentionArchive.__module__].__file__
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    archive = next(
        node
        for node in tree.body
        if (
            isinstance(node, ast.ClassDef)
            and node.name == "AcquisitionSourceRetentionArchive"
        )
    )
    methods = {
        node.name: node
        for node in archive.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    stage_labels = {
        str(keyword.value.value)
        for call in ast.walk(archive)
        if (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "self"
            and call.func.attr == "_write_stage"
        )
        for keyword in call.keywords
        if (
            keyword.arg == "label"
            and isinstance(keyword.value, ast.Constant)
            and isinstance(keyword.value.value, str)
        )
    }
    stage_suffixes: list[str] = []
    for call in ast.walk(methods["_write_stage"]):
        if not (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "self"
            and call.func.attr == "_fault"
            and len(call.args) == 1
            and isinstance(call.args[0], ast.JoinedStr)
        ):
            continue
        constants = "".join(
            str(value.value)
            for value in call.args[0].values
            if isinstance(value, ast.Constant)
        )
        stage_suffixes.append(constants.removeprefix("."))
    anchor_points = tuple(
        str(call.args[0].value)
        for call in ast.walk(methods["_write_anchor"])
        if (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "self"
            and call.func.attr == "_fault"
            and len(call.args) == 1
            and isinstance(call.args[0], ast.Constant)
            and isinstance(call.args[0].value, str)
        )
    )
    expanded = {
        f"{label}.{suffix}"
        for label in stage_labels
        for suffix in stage_suffixes
    }
    expanded.update(anchor_points)
    return frozenset(expanded)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _archive_files(archive: AcquisitionSourceRetentionArchive) -> dict[str, str]:
    paths = (
        archive.anchor_path,
        *sorted(archive.segment_directory.iterdir()),
        *sorted(archive.manifest_directory.iterdir()),
    )
    return {
        str(path.relative_to(archive.root)).replace("\\", "/"): _sha256(path)
        for path in paths
        if path.is_file() and not path.is_symlink()
    }


def _source_files(authorities: Authorities) -> tuple[Path, ...]:
    return (
        authorities.raw.catalog_path,
        authorities.scheduler.path,
        authorities.executions.path,
        authorities.capacity.path,
        authorities.evidence.path,
        *authorities.object_paths,
    )


def test_v1_retention_native_crash_inventory_expands_every_export_occurrence():
    discovered = _v1_retention_fault_inventory()
    assert discovered == frozenset(_V1_RETENTION_CRASH_POINTS)
    assert len(discovered) == len(set(discovered)) == 12


@pytest.mark.parametrize("checkpoint", _V1_RETENTION_CRASH_POINTS)
def test_v1_retention_real_exit_reopens_reuses_and_replays_without_growth(
    tmp_path: Path,
    checkpoint: str,
) -> None:
    case_root = (tmp_path / checkpoint.replace(".", "-")).resolve()
    authorities = _authorities(case_root / "sources")
    candidate = authorities.candidates[0]
    archive = _archive(case_root / "archive", authorities)
    plan = archive.plan((candidate,), now=NOW + 100.0)
    old_audit = archive.audit()
    old_anchor = old_audit["archive_anchor_sha256"]
    source_hashes = {path: _sha256(path) for path in _source_files(authorities)}
    (case_root / "candidate.json").write_text(
        json.dumps(candidate.as_dict(), sort_keys=True),
        encoding="ascii",
    )
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "worker_acquisition_source_retention_v1_crash_child.py"
    )
    environment = dict(os.environ)
    repository = str(Path(__file__).resolve().parents[1])
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(Path(repository) / ".testdeps"),
            repository,
            environment.get("PYTHONPATH", ""),
        )
        if value
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    crashed = subprocess.run(
        [sys.executable, str(helper), str(case_root), checkpoint],
        cwd=repository,
        env=environment,
        capture_output=True,
        check=False,
        timeout=30.0,
    )
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    reopened_authorities, reopened = _reopen_v1_retention_case(
        case_root,
        candidate,
    )
    crashed_audit = reopened.audit()
    committed = checkpoint in _V1_RETENTION_COMMITTED_POINTS
    assert crashed_audit["healthy"] is True
    assert (
        crashed_audit["archive_anchor_sha256"] != old_anchor
    ) is committed
    assert crashed_audit["archived_job_count"] == int(committed)
    assert crashed_audit["segment_count"] == int(committed)
    if checkpoint.startswith("segment.before_stage") or checkpoint.startswith(
        "segment.after_stage_fsync"
    ):
        assert crashed_audit["orphan_segment_count"] == 0
    elif committed:
        assert crashed_audit["orphan_segment_count"] == 0
    else:
        assert crashed_audit["orphan_segment_count"] == 1
    expected_orphan_manifest = (
        checkpoint.startswith("manifest.after_publish")
        or checkpoint.startswith("manifest.after_directory_fsync")
        or checkpoint.startswith("anchor.before_stage")
        or checkpoint.startswith("anchor.after_stage_fsync")
    )
    assert crashed_audit["orphan_manifest_count"] == int(
        expected_orphan_manifest
    )
    assert {
        path: _sha256(path) for path in _source_files(reopened_authorities)
    } == source_hashes

    recovered = reopened.export(plan)
    assert recovered.status == ("idempotent" if committed else "archived")
    final_audit = reopened.audit()
    assert final_audit["healthy"] is True
    assert final_audit["archived_job_count"] == 1
    assert final_audit["segment_count"] == 1
    assert final_audit["orphan_segment_count"] == 0
    assert final_audit["orphan_manifest_count"] == 0
    stable_files = _archive_files(reopened)
    assert len(tuple(reopened.segment_directory.iterdir())) == 1
    assert len(tuple(reopened.manifest_directory.iterdir())) == 2

    replay_plan = reopened.plan((candidate,), now=NOW + 101.0)
    replay = reopened.export(replay_plan)
    assert replay.status == "idempotent"
    assert reopened.audit() == final_audit
    assert _archive_files(reopened) == stable_files
    assert {
        path: _sha256(path) for path in _source_files(reopened_authorities)
    } == source_hashes
