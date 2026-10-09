"""Abrupt PREPARE/COMMIT/ABORT crash matrix for native five-role reclamation."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys

import pytest

from storm_tracker_worker.acquisition_source_reclamation_apply_authority import (
    AcquisitionSourceReclamationApplyConflict,
)
from storm_tracker_worker.acquisition_source_reclamation_protocol import (
    ABORTED,
    ABORTING,
    APPLIED,
    COMMITTED,
    PARTICIPANT_APPLY_ORDER,
    PREPARED,
    PREPARING,
    canonical_bytes,
)
from test_worker_acquisition_consumer_registry import _archive_authorities
from test_worker_acquisition_source_reclamation_native_five_role_integration import (
    _issue_closure,
)
from test_worker_acquisition_source_reclamation_native_five_role_crash import (
    CRASH_EXIT_CODE,
    MANIFEST_CONTRACT,
    MANIFEST_NAME,
    NOW,
    ROOT,
    _file_inventory,
    _historical_page_bytes,
    _assert_terminal_effects,
    _lifecycle_unlink_paths,
    _native_five_role_stack,
    _page_bytes,
    _read_only_database,
    _read_manifest,
    _reopen_stack,
    _terminal_snapshot,
)


CHILD = (
    ROOT
    / "tests"
    / "helpers"
    / "worker_reclamation_prepare_abort_crash_child.py"
)
CHILD_TIMEOUT_SECONDS = 300.0


@dataclass(frozen=True, slots=True)
class _PhaseCrashCase:
    phase: str
    target: str
    checkpoint: str
    occurrence: int = 1

    @property
    def key(self) -> str:
        return (
            f"{self.phase}|{self.target}|{self.checkpoint}|{self.occurrence}"
        )

    @property
    def test_id(self) -> str:
        if self.target == "coordinator" and self.checkpoint.startswith(
            "record_prepare."
        ):
            role = PARTICIPANT_APPLY_ORDER[self.occurrence - 1]
            return f"prepare-coordinator-{role}-{self.checkpoint}"
        if self.target == "coordinator" and self.checkpoint.startswith(
            "record_abort."
        ):
            role = tuple(reversed(PARTICIPANT_APPLY_ORDER))[self.occurrence - 1]
            return f"abort-coordinator-{role}-{self.checkpoint}"
        return f"{self.phase}-{self.target}-{self.checkpoint}"


PREPARE_CRASH_CASES = (
    _PhaseCrashCase("prepare", "coordinator", "begin_preparing.before_commit"),
    _PhaseCrashCase("prepare", "coordinator", "begin_preparing.after_commit"),
    _PhaseCrashCase(
        "prepare", "downstream_lifecycle", "reclamation.prepare.before_commit"
    ),
    _PhaseCrashCase(
        "prepare", "downstream_lifecycle", "reclamation.prepare.after_commit"
    ),
    _PhaseCrashCase(
        "prepare", "execution_receipts", "execution_receipts.prepare.before_commit"
    ),
    _PhaseCrashCase(
        "prepare", "execution_receipts", "execution_receipts.prepare.after_commit"
    ),
    _PhaseCrashCase("prepare", "scheduler", "scheduler.prepare.before_commit"),
    _PhaseCrashCase("prepare", "scheduler", "scheduler.prepare.after_commit"),
    _PhaseCrashCase(
        "prepare", "raw_artifacts", "reclamation_prepare_before_commit"
    ),
    _PhaseCrashCase(
        "prepare", "raw_artifacts", "reclamation_prepare_after_commit"
    ),
    _PhaseCrashCase(
        "prepare", "capacity_ledger", "capacity_ledger.prepare.before_commit"
    ),
    _PhaseCrashCase(
        "prepare", "capacity_ledger", "capacity_ledger.prepare.after_commit"
    ),
    _PhaseCrashCase("prepare", "coordinator", "record_prepare.before_commit", 1),
    _PhaseCrashCase("prepare", "coordinator", "record_prepare.after_commit", 1),
    _PhaseCrashCase("prepare", "coordinator", "record_prepare.before_commit", 2),
    _PhaseCrashCase("prepare", "coordinator", "record_prepare.after_commit", 2),
    _PhaseCrashCase("prepare", "coordinator", "record_prepare.before_commit", 3),
    _PhaseCrashCase("prepare", "coordinator", "record_prepare.after_commit", 3),
    _PhaseCrashCase("prepare", "coordinator", "record_prepare.before_commit", 4),
    _PhaseCrashCase("prepare", "coordinator", "record_prepare.after_commit", 4),
    _PhaseCrashCase("prepare", "coordinator", "record_prepare.before_commit", 5),
    _PhaseCrashCase("prepare", "coordinator", "record_prepare.after_commit", 5),
    _PhaseCrashCase("prepare", "coordinator", "finish_prepared.before_commit"),
    _PhaseCrashCase("prepare", "coordinator", "finish_prepared.after_commit"),
)

COMMIT_CRASH_CASES = (
    _PhaseCrashCase("commit", "coordinator", "commit.before_commit"),
    _PhaseCrashCase("commit", "coordinator", "commit.after_commit"),
)

ABORT_CRASH_CASES = (
    _PhaseCrashCase("abort", "coordinator", "begin_aborting.before_commit"),
    _PhaseCrashCase("abort", "coordinator", "begin_aborting.after_commit"),
    _PhaseCrashCase(
        "abort", "capacity_ledger", "capacity_ledger.abort.before_commit"
    ),
    _PhaseCrashCase(
        "abort", "capacity_ledger", "capacity_ledger.abort.after_commit"
    ),
    _PhaseCrashCase("abort", "raw_artifacts", "reclamation_abort_before_commit"),
    _PhaseCrashCase("abort", "raw_artifacts", "reclamation_abort_after_commit"),
    _PhaseCrashCase("abort", "scheduler", "scheduler.abort.before_commit"),
    _PhaseCrashCase("abort", "scheduler", "scheduler.abort.after_commit"),
    _PhaseCrashCase(
        "abort", "execution_receipts", "execution_receipts.abort.before_commit"
    ),
    _PhaseCrashCase(
        "abort", "execution_receipts", "execution_receipts.abort.after_commit"
    ),
    _PhaseCrashCase(
        "abort", "downstream_lifecycle", "reclamation.abort.before_commit"
    ),
    _PhaseCrashCase(
        "abort", "downstream_lifecycle", "reclamation.abort.after_commit"
    ),
    _PhaseCrashCase("abort", "coordinator", "record_abort.before_commit", 1),
    _PhaseCrashCase("abort", "coordinator", "record_abort.after_commit", 1),
    _PhaseCrashCase("abort", "coordinator", "record_abort.before_commit", 2),
    _PhaseCrashCase("abort", "coordinator", "record_abort.after_commit", 2),
    _PhaseCrashCase("abort", "coordinator", "record_abort.before_commit", 3),
    _PhaseCrashCase("abort", "coordinator", "record_abort.after_commit", 3),
    _PhaseCrashCase("abort", "coordinator", "record_abort.before_commit", 4),
    _PhaseCrashCase("abort", "coordinator", "record_abort.after_commit", 4),
    _PhaseCrashCase("abort", "coordinator", "record_abort.before_commit", 5),
    _PhaseCrashCase("abort", "coordinator", "record_abort.after_commit", 5),
    _PhaseCrashCase("abort", "coordinator", "finish_aborted.before_commit"),
    _PhaseCrashCase("abort", "coordinator", "finish_aborted.after_commit"),
)

ALL_PHASE_CRASH_CASES = (
    *PREPARE_CRASH_CASES,
    *COMMIT_CRASH_CASES,
    *ABORT_CRASH_CASES,
)


# Independent release-gate inventory.  Deliberately duplicated instead of
# deriving this set from the executable case table.
PINNED_EXPECTED_CASE_KEYS = frozenset({
    "prepare|coordinator|begin_preparing.before_commit|1",
    "prepare|coordinator|begin_preparing.after_commit|1",
    "prepare|downstream_lifecycle|reclamation.prepare.before_commit|1",
    "prepare|downstream_lifecycle|reclamation.prepare.after_commit|1",
    "prepare|execution_receipts|execution_receipts.prepare.before_commit|1",
    "prepare|execution_receipts|execution_receipts.prepare.after_commit|1",
    "prepare|scheduler|scheduler.prepare.before_commit|1",
    "prepare|scheduler|scheduler.prepare.after_commit|1",
    "prepare|raw_artifacts|reclamation_prepare_before_commit|1",
    "prepare|raw_artifacts|reclamation_prepare_after_commit|1",
    "prepare|capacity_ledger|capacity_ledger.prepare.before_commit|1",
    "prepare|capacity_ledger|capacity_ledger.prepare.after_commit|1",
    "prepare|coordinator|record_prepare.before_commit|1",
    "prepare|coordinator|record_prepare.after_commit|1",
    "prepare|coordinator|record_prepare.before_commit|2",
    "prepare|coordinator|record_prepare.after_commit|2",
    "prepare|coordinator|record_prepare.before_commit|3",
    "prepare|coordinator|record_prepare.after_commit|3",
    "prepare|coordinator|record_prepare.before_commit|4",
    "prepare|coordinator|record_prepare.after_commit|4",
    "prepare|coordinator|record_prepare.before_commit|5",
    "prepare|coordinator|record_prepare.after_commit|5",
    "prepare|coordinator|finish_prepared.before_commit|1",
    "prepare|coordinator|finish_prepared.after_commit|1",
    "commit|coordinator|commit.before_commit|1",
    "commit|coordinator|commit.after_commit|1",
    "abort|coordinator|begin_aborting.before_commit|1",
    "abort|coordinator|begin_aborting.after_commit|1",
    "abort|capacity_ledger|capacity_ledger.abort.before_commit|1",
    "abort|capacity_ledger|capacity_ledger.abort.after_commit|1",
    "abort|raw_artifacts|reclamation_abort_before_commit|1",
    "abort|raw_artifacts|reclamation_abort_after_commit|1",
    "abort|scheduler|scheduler.abort.before_commit|1",
    "abort|scheduler|scheduler.abort.after_commit|1",
    "abort|execution_receipts|execution_receipts.abort.before_commit|1",
    "abort|execution_receipts|execution_receipts.abort.after_commit|1",
    "abort|downstream_lifecycle|reclamation.abort.before_commit|1",
    "abort|downstream_lifecycle|reclamation.abort.after_commit|1",
    "abort|coordinator|record_abort.before_commit|1",
    "abort|coordinator|record_abort.after_commit|1",
    "abort|coordinator|record_abort.before_commit|2",
    "abort|coordinator|record_abort.after_commit|2",
    "abort|coordinator|record_abort.before_commit|3",
    "abort|coordinator|record_abort.after_commit|3",
    "abort|coordinator|record_abort.before_commit|4",
    "abort|coordinator|record_abort.after_commit|4",
    "abort|coordinator|record_abort.before_commit|5",
    "abort|coordinator|record_abort.after_commit|5",
    "abort|coordinator|finish_aborted.before_commit|1",
    "abort|coordinator|finish_aborted.after_commit|1",
})


def _manifest_path(root: Path) -> Path:
    return root / MANIFEST_NAME


def _setup_phase_fixture(root: Path, *, prepared: bool) -> None:
    with _native_five_role_stack(root) as stack:
        capture = stack.sources.raw.load_capture(stack.candidate.raw_capture_id)
        assert capture is not None
        archive_files = tuple(
            stack.sources.evidence.archive_segment_directory.iterdir()
        ) + tuple(stack.sources.evidence.archive_object_directory.iterdir())
        assert len(archive_files) == 2
        with stack.retention.hold_candidate_page_v1(
            stack.request,
            after_cursor=0,
            max_candidates=64,
        ) as page:
            historical_page_hex = _page_bytes(page).hex()
        if prepared:
            state = stack.coordinator.prepare_v2(
                stack.request,
                participants=stack.participants,
            )
            assert state.state == PREPARED
            assert state.prepared_roles == PARTICIPANT_APPLY_ORDER
            lifecycle_unlink_paths = _lifecycle_unlink_paths(
                stack.sources.evidence,
                stack.request.request_sha256,
            )
        else:
            assert stack.coordinator.load_state_v2(
                stack.request.request_sha256
            ) is None
            lifecycle_unlink_paths = ()
        manifest = {
            "archive_files": [str(item) for item in archive_files],
            "candidate": stack.candidate.as_dict(),
            "capture_receipt_ids": [item.receipt_id for item in capture.members],
            "contract": MANIFEST_CONTRACT,
            "coordinator_store_identity_sha256": (
                stack.coordinator.store_identity_sha256
            ),
            "historical_page_hex": historical_page_hex,
            "lifecycle_unlink_paths": [
                str(item) for item in lifecycle_unlink_paths
            ],
            "object_paths": [str(item) for item in stack.sources.object_paths],
            "request": stack.request.as_dict(),
            "source_file_facts": [
                _path_fact(item)
                for item in (*archive_files, *stack.sources.object_paths)
            ],
        }
        _manifest_path(root).write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")),
            encoding="ascii",
        )


def run_phase_crash_child(
    root: Path,
    *,
    action: str,
    target: str,
    checkpoint: str,
    occurrence: int,
) -> None:
    if action in {"setup-bare", "setup-prepared"}:
        if target != "none" or checkpoint != "none" or occurrence != 1:
            raise SystemExit("phase setup does not accept a crash target")
        _setup_phase_fixture(root, prepared=action == "setup-prepared")
        return
    if action not in {"prepare", "commit", "abort"}:
        raise SystemExit("unsupported reclamation phase child action")
    with _reopen_stack(
        root,
        target=target,
        checkpoint=checkpoint,
        occurrence=occurrence,
        clock_base=NOW + 10_000.0,
    ) as stack:
        if action == "prepare":
            stack.coordinator.prepare_v2(
                stack.request,
                participants=stack.participants,
            )
        elif action == "commit":
            stack.coordinator.commit_v1(
                stack.request.request_sha256,
                participants=stack.participants,
            )
        else:
            stack.coordinator.abort_v1(
                stack.request.request_sha256,
                participants=stack.participants,
            )
    raise SystemExit(
        f"phase crash checkpoint was not reached: "
        f"{action}:{target}:{checkpoint}:{occurrence}"
    )


def _run_child(
    root: Path,
    *,
    action: str,
    target: str,
    checkpoint: str,
    occurrence: int = 1,
) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    python_path = [str(ROOT), str(ROOT / ".testdeps")]
    existing = environment.get("PYTHONPATH")
    if existing:
        python_path.append(existing)
    environment["PYTHONPATH"] = os.pathsep.join(python_path)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [
            sys.executable,
            "-B",
            str(CHILD),
            str(root),
            action,
            target,
            checkpoint,
            str(occurrence),
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        # A clean fixture build can exceed 90 seconds when Windows scans the
        # accumulated file-heavy matrix.  Keep the child bounded, but do not
        # misclassify that observed host delay as a reclamation failure.
        timeout=CHILD_TIMEOUT_SECONDS,
        check=False,
    )


_ABORT_ORDER = tuple(reversed(PARTICIPANT_APPLY_ORDER))


@dataclass(frozen=True, slots=True)
class _ExpectedBoundary:
    state: str | None
    coordinator_prepare_count: int
    local_prepare_count: int
    coordinator_abort_count: int
    local_abort_count: int
    event_types: tuple[str, ...]
    active_slot: int | None
    committed: bool
    abort_decided: bool
    completed: bool


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _path_fact(path: Path) -> dict[str, object]:
    metadata = path.lstat()
    assert stat.S_ISREG(metadata.st_mode)
    payload = path.read_bytes()
    return {
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "links": metadata.st_nlink,
        "mode_type": stat.S_IFMT(metadata.st_mode),
        "path": str(path),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size": len(payload),
    }


def _known_database_paths(root: Path) -> dict[str, Path]:
    sources = root / "fixture" / "sources"
    return {
        "coordinator": root / "coordinator" / "reclamation.sqlite3",
        "downstream_lifecycle": (
            sources / "evidence" / "acquisition_evidence.sqlite3"
        ),
        "execution_receipts": sources / "execution.sqlite3",
        "scheduler": sources / "scheduler.sqlite3",
        "raw_artifacts": sources / "raw" / "raw_artifacts.sqlite3",
        "capacity_ledger": sources / "capacity.sqlite3",
    }


def _assert_exact_hot_journal(root: Path, case: _PhaseCrashCase) -> None:
    """Prove the process/SQLite commit side of the injected boundary.

    An after-commit checkpoint is deliberately not described as a power-loss
    or filesystem-flush guarantee; the child only proves abrupt process death
    immediately before or after SQLite's commit call returned.
    """

    paths = _known_database_paths(root)
    before_commit = case.checkpoint.endswith("before_commit")
    # The scheduler is the sole WAL-backed participant in this fixture.  Its
    # uncommitted transaction has not emitted a frame yet, while a committed
    # transaction has.  Every other authority is pinned to DELETE journaling
    # and therefore exposes the ordinary hot rollback journal before commit.
    expected = (
        {Path(str(paths[case.target]) + "-journal")}
        if before_commit and case.target != "scheduler"
        else set()
    )
    journals = {Path(str(path) + "-journal") for path in paths.values()}
    present = {path for path in journals if path.exists()}
    assert present == expected
    for path in present:
        metadata = path.lstat()
        assert stat.S_ISREG(metadata.st_mode)
        assert metadata.st_size > 0
    if case.target == "scheduler":
        wal = Path(str(paths["scheduler"]) + "-wal")
        shm = Path(str(paths["scheduler"]) + "-shm")
        for path in (wal, shm):
            metadata = path.lstat()
            assert stat.S_ISREG(metadata.st_mode)
        assert shm.stat().st_size > 0
        assert (wal.stat().st_size == 0) is before_commit


def _encoded_sql_value(value: object) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("ascii")
    raise AssertionError("unexpected reclamation receipt storage type")


def _local_database_row(
    root: Path,
    *,
    role: str,
    request_sha256: str,
) -> dict[str, object] | None:
    database_path = _known_database_paths(root)[role]
    with _read_only_database(database_path) as database:
        if role == "raw_artifacts":
            prepared = database.execute(
                "SELECT prepare_receipt_json,source_fence_sha256 "
                "FROM raw_artifact_reclamation_prepares "
                "WHERE request_sha256=?",
                (request_sha256,),
            ).fetchone()
            aborted = database.execute(
                "SELECT abort_receipt_json,coordinator_abort_decision_sha256,"
                "fence_release_root_sha256 FROM "
                "raw_artifact_reclamation_aborts WHERE request_sha256=?",
                (request_sha256,),
            ).fetchone()
            if prepared is None:
                assert aborted is None
                return None
            return {
                "abort_decision_sha256": (
                    None
                    if aborted is None
                    else str(aborted["coordinator_abort_decision_sha256"])
                ),
                "abort_payload": (
                    None
                    if aborted is None
                    else _encoded_sql_value(aborted["abort_receipt_json"])
                ),
                "active_slot": 1 if aborted is None else None,
                "fence_release_root_sha256": (
                    None
                    if aborted is None
                    else str(aborted["fence_release_root_sha256"])
                ),
                "prepare_payload": _encoded_sql_value(
                    prepared["prepare_receipt_json"]
                ),
                "source_fence_sha256": str(prepared["source_fence_sha256"]),
                "state": PREPARED if aborted is None else ABORTED,
            }

        table = (
            "lifecycle_reclamation_decisions"
            if role == "downstream_lifecycle"
            else "source_reclamation_decisions"
        )
        row = database.execute(
            "SELECT state,active_slot,prepare_receipt_json,"
            "source_fence_sha256,abort_decision_sha256,"
            "fence_release_root_sha256,abort_receipt_json "
            f"FROM {table} WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        if row is None:
            return None
        return {
            "abort_decision_sha256": (
                None
                if row["abort_decision_sha256"] is None
                else str(row["abort_decision_sha256"])
            ),
            "abort_payload": _encoded_sql_value(row["abort_receipt_json"]),
            "active_slot": (
                None if row["active_slot"] is None else int(row["active_slot"])
            ),
            "fence_release_root_sha256": (
                None
                if row["fence_release_root_sha256"] is None
                else str(row["fence_release_root_sha256"])
            ),
            "prepare_payload": _encoded_sql_value(row["prepare_receipt_json"]),
            "source_fence_sha256": str(row["source_fence_sha256"]),
            "state": str(row["state"]),
        }


def _local_inventory(
    stack,
    request_sha256: str,
) -> tuple[dict[str, object], ...]:
    inventory = []
    for participant in stack.participants:
        prepare = participant.load_source_local_prepare_v2(request_sha256)
        abort = participant.load_source_local_abort_v1(request_sha256)
        apply = participant.load_source_local_apply_v1(request_sha256)
        assert apply is None
        row = _local_database_row(
            stack.root,
            role=participant.role,
            request_sha256=request_sha256,
        )
        if prepare is None:
            assert row is None
            assert abort is None
        else:
            assert row is not None
            assert prepare.request_sha256 == request_sha256
            assert prepare.role == participant.role
            assert (
                prepare.source_store_identity_sha256
                == participant.source_store_identity_sha256
            )
            assert row["prepare_payload"] == canonical_bytes(prepare.as_dict())
            assert row["source_fence_sha256"] == prepare.source_fence_sha256
        if abort is None:
            if row is not None:
                assert row["abort_payload"] is None
                assert row["abort_decision_sha256"] is None
                assert row["fence_release_root_sha256"] is None
        else:
            assert row is not None and prepare is not None
            assert abort.request_sha256 == request_sha256
            assert abort.role == participant.role
            assert (
                abort.source_store_identity_sha256
                == participant.source_store_identity_sha256
            )
            assert abort.prepare_receipt_sha256 == prepare.prepare_receipt_sha256
            assert row["abort_payload"] == canonical_bytes(abort.as_dict())
            assert (
                row["abort_decision_sha256"]
                == abort.coordinator_abort_decision_sha256
            )
            assert (
                row["fence_release_root_sha256"]
                == abort.fence_release_root_sha256
            )
        inventory.append({
            "abort": None if abort is None else abort.as_dict(),
            "prepare": None if prepare is None else prepare.as_dict(),
            "role": participant.role,
            "row": (
                None
                if row is None
                else {
                    key: (
                        hashlib.sha256(value).hexdigest()
                        if key.endswith("_payload") and value is not None
                        else value
                    )
                    for key, value in row.items()
                }
            ),
        })
    return tuple(inventory)


def _coordinator_inventory(
    root: Path,
    request_sha256: str,
) -> dict[str, object]:
    path = _known_database_paths(root)["coordinator"]
    with _read_only_database(path) as database:
        request = database.execute(
            "SELECT state,active_slot,participant_prepare_root_sha256,"
            "committed_decision_sha256,committed_receipt_sha256,"
            "abort_decision_sha256,abort_receipt_sha256,completed_at "
            "FROM coordinator_requests WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        prepares = database.execute(
            "SELECT role,source_store_identity_sha256,prepare_receipt_sha256,"
            "source_fence_sha256,receipt_json FROM coordinator_prepares "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchall()
        aborts = database.execute(
            "SELECT role,abort_receipt_sha256,receipt_json "
            "FROM coordinator_aborts WHERE request_sha256=?",
            (request_sha256,),
        ).fetchall()
        applies = database.execute(
            "SELECT role FROM coordinator_applies WHERE request_sha256=?",
            (request_sha256,),
        ).fetchall()
        events = database.execute(
            "SELECT event_sequence,event_type,state,payload_sha256 "
            "FROM coordinator_events WHERE request_sha256=? "
            "ORDER BY event_sequence",
            (request_sha256,),
        ).fetchall()

    prepare_by_role = {str(row["role"]): row for row in prepares}
    abort_by_role = {str(row["role"]): row for row in aborts}
    apply_roles = {str(row["role"]) for row in applies}
    assert len(prepare_by_role) == len(prepares)
    assert len(abort_by_role) == len(aborts)
    assert apply_roles.issubset(PARTICIPANT_APPLY_ORDER)
    return {
        "aborts": tuple(
            {
                "abort_receipt_sha256": str(abort_by_role[role]["abort_receipt_sha256"]),
                "payload_sha256": hashlib.sha256(
                    bytes(abort_by_role[role]["receipt_json"])
                ).hexdigest(),
                "role": role,
            }
            for role in PARTICIPANT_APPLY_ORDER
            if role in abort_by_role
        ),
        "applied_roles": tuple(
            role for role in PARTICIPANT_APPLY_ORDER if role in apply_roles
        ),
        "events": tuple(
            {
                "event_sequence": int(row["event_sequence"]),
                "event_type": str(row["event_type"]),
                "payload_sha256": str(row["payload_sha256"]),
                "state": str(row["state"]),
            }
            for row in events
        ),
        "prepares": tuple(
            {
                "payload_sha256": hashlib.sha256(
                    bytes(prepare_by_role[role]["receipt_json"])
                ).hexdigest(),
                "prepare_receipt_sha256": str(
                    prepare_by_role[role]["prepare_receipt_sha256"]
                ),
                "role": role,
                "source_fence_sha256": str(
                    prepare_by_role[role]["source_fence_sha256"]
                ),
                "source_store_identity_sha256": str(
                    prepare_by_role[role]["source_store_identity_sha256"]
                ),
            }
            for role in PARTICIPANT_APPLY_ORDER
            if role in prepare_by_role
        ),
        "request": (
            None
            if request is None
            else {
                "abort_decision_sha256": (
                    None
                    if request["abort_decision_sha256"] is None
                    else str(request["abort_decision_sha256"])
                ),
                "abort_receipt_sha256": (
                    None
                    if request["abort_receipt_sha256"] is None
                    else str(request["abort_receipt_sha256"])
                ),
                "active_slot": (
                    None
                    if request["active_slot"] is None
                    else int(request["active_slot"])
                ),
                "committed_decision_sha256": (
                    None
                    if request["committed_decision_sha256"] is None
                    else str(request["committed_decision_sha256"])
                ),
                "committed_receipt_sha256": (
                    None
                    if request["committed_receipt_sha256"] is None
                    else str(request["committed_receipt_sha256"])
                ),
                "completed": request["completed_at"] is not None,
                "participant_prepare_root_sha256": (
                    None
                    if request["participant_prepare_root_sha256"] is None
                    else str(request["participant_prepare_root_sha256"])
                ),
                "state": str(request["state"]),
            }
        ),
    }


def _source_snapshot(stack) -> dict[str, object]:
    candidate = stack.candidate
    receipt_ids = stack.manifest["capture_receipt_ids"]
    assert isinstance(receipt_ids, list)
    file_paths = tuple(
        Path(item)
        for item in (
            *stack.manifest["archive_files"],
            *stack.manifest["object_paths"],
        )
    )
    return {
        "archive_lookup": stack.source_archive.lookup(candidate.candidate_sha256),
        "capacity": stack.sources.capacity.load_for_job(
            provider="dwd",
            job_identity=candidate.acquisition_job_id,
        ),
        "evidence_lookup": stack.sources.evidence.lookup_sequence(
            candidate.evidence_stream_id,
            candidate.evidence_stream_sequence,
        ),
        "execution_receipts": stack.sources.executions.load_job_receipts(
            candidate.acquisition_job_id
        ),
        "file_facts": tuple(_path_fact(path) for path in file_paths),
        "historical_page": _historical_page_bytes(stack),
        "raw_capture": stack.sources.raw.load_capture(candidate.raw_capture_id),
        "raw_receipts": tuple(
            stack.sources.raw.load_receipt(item) for item in receipt_ids
        ),
        "scheduler_proof": stack.sources.scheduler.terminal_proof(
            candidate.scheduler_job_id
        ),
        "scheduler_record": stack.sources.scheduler.get_record(
            candidate.scheduler_job_id
        ),
        "terminal_consumption": stack.sources.evidence.load_terminal_consumption(
            candidate.evidence_stream_id,
            candidate.evidence_stream_sequence,
        ),
    }


def _assert_source_live(stack, snapshot: dict[str, object]) -> None:
    assert snapshot["archive_lookup"].record is not None
    assert snapshot["capacity"] is not None
    assert snapshot["evidence_lookup"].status == "archived"
    assert snapshot["execution_receipts"]
    assert snapshot["raw_capture"] is not None
    assert all(item is not None for item in snapshot["raw_receipts"])
    assert snapshot["scheduler_proof"] is not None
    assert snapshot["scheduler_record"] is not None
    assert snapshot["terminal_consumption"] is not None
    assert snapshot["terminal_consumption"].status == "archived"
    assert snapshot["historical_page"].hex() == stack.manifest[
        "historical_page_hex"
    ]
    expected_files = stack.manifest["source_file_facts"]
    assert isinstance(expected_files, list)
    assert list(snapshot["file_facts"]) == expected_files


def _assert_healthy_audits(stack) -> None:
    assert stack.sources.evidence.audit_archive()["healthy"] is True
    assert stack.source_archive.audit()["healthy"] is True
    assert stack.retention.audit_v2()["healthy"] is True
    assert stack.sources.raw.integrity_check()["healthy"] is True
    capacity = stack.sources.capacity.audit()
    assert capacity["reservation_rows"] == 1
    assert capacity["committed_count"] == 1


def _expected_boundary(case: _PhaseCrashCase) -> _ExpectedBoundary:
    participant_count = len(PARTICIPANT_APPLY_ORDER)
    if case.phase == "prepare":
        if case.target == "coordinator":
            if case.checkpoint == "begin_preparing.before_commit":
                values = (None, 0, 0, 0, 0)
            elif case.checkpoint == "begin_preparing.after_commit":
                values = (PREPARING, 0, 0, 0, 0)
            elif case.checkpoint.startswith("record_prepare."):
                committed = case.checkpoint.endswith("after_commit")
                values = (
                    PREPARING,
                    case.occurrence if committed else case.occurrence - 1,
                    case.occurrence,
                    0,
                    0,
                )
            elif case.checkpoint == "finish_prepared.before_commit":
                values = (PREPARING, participant_count, participant_count, 0, 0)
            elif case.checkpoint == "finish_prepared.after_commit":
                values = (PREPARED, participant_count, participant_count, 0, 0)
            else:  # pragma: no cover - pinned table owns the vocabulary.
                raise AssertionError("unknown coordinator prepare checkpoint")
        else:
            index = PARTICIPANT_APPLY_ORDER.index(case.target)
            local_count = index + int(case.checkpoint.endswith("after_commit"))
            values = (PREPARING, index, local_count, 0, 0)
    elif case.phase == "commit":
        state = (
            COMMITTED
            if case.checkpoint == "commit.after_commit"
            else PREPARED
        )
        values = (state, participant_count, participant_count, 0, 0)
    elif case.phase == "abort":
        if case.target == "coordinator":
            if case.checkpoint == "begin_aborting.before_commit":
                values = (PREPARED, participant_count, participant_count, 0, 0)
            elif case.checkpoint == "begin_aborting.after_commit":
                values = (ABORTING, participant_count, participant_count, 0, 0)
            elif case.checkpoint.startswith("record_abort."):
                committed = case.checkpoint.endswith("after_commit")
                values = (
                    ABORTING,
                    participant_count,
                    participant_count,
                    case.occurrence if committed else case.occurrence - 1,
                    case.occurrence,
                )
            elif case.checkpoint == "finish_aborted.before_commit":
                values = (
                    ABORTING,
                    participant_count,
                    participant_count,
                    participant_count,
                    participant_count,
                )
            elif case.checkpoint == "finish_aborted.after_commit":
                values = (
                    ABORTED,
                    participant_count,
                    participant_count,
                    participant_count,
                    participant_count,
                )
            else:  # pragma: no cover - pinned table owns the vocabulary.
                raise AssertionError("unknown coordinator abort checkpoint")
        else:
            index = _ABORT_ORDER.index(case.target)
            local_count = index + int(case.checkpoint.endswith("after_commit"))
            values = (
                ABORTING,
                participant_count,
                participant_count,
                index,
                local_count,
            )
    else:  # pragma: no cover - pinned table owns the vocabulary.
        raise AssertionError("unknown phase")

    state, coordinator_prepares, local_prepares, coordinator_aborts, local_aborts = values
    event_types = {
        None: (),
        PREPARING: ("preparing",),
        PREPARED: ("preparing", "prepared"),
        COMMITTED: ("preparing", "prepared", "committed"),
        ABORTING: ("preparing", "prepared", "aborting"),
        ABORTED: ("preparing", "prepared", "aborting", "aborted"),
    }[state]
    return _ExpectedBoundary(
        state=state,
        coordinator_prepare_count=coordinator_prepares,
        local_prepare_count=local_prepares,
        coordinator_abort_count=coordinator_aborts,
        local_abort_count=local_aborts,
        event_types=event_types,
        active_slot=None if state in {None, ABORTED} else 1,
        committed=state == COMMITTED,
        abort_decided=state in {ABORTING, ABORTED},
        completed=state == ABORTED,
    )


def _assert_exact_inventories(
    stack,
    *,
    request_sha256: str,
    expected: _ExpectedBoundary,
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    coordinator = _coordinator_inventory(stack.root, request_sha256)
    local = _local_inventory(stack, request_sha256)
    expected_coordinator_prepares = PARTICIPANT_APPLY_ORDER[
        : expected.coordinator_prepare_count
    ]
    expected_local_prepares = PARTICIPANT_APPLY_ORDER[
        : expected.local_prepare_count
    ]
    expected_coordinator_aborts = PARTICIPANT_APPLY_ORDER[
        len(PARTICIPANT_APPLY_ORDER) - expected.coordinator_abort_count :
    ]
    expected_local_aborts = PARTICIPANT_APPLY_ORDER[
        len(PARTICIPANT_APPLY_ORDER) - expected.local_abort_count :
    ]
    if expected.coordinator_abort_count == 0:
        expected_coordinator_aborts = ()
    if expected.local_abort_count == 0:
        expected_local_aborts = ()

    state = stack.coordinator.load_state_v2(request_sha256)
    if expected.state is None:
        assert state is None
        assert coordinator["request"] is None
    else:
        assert state is not None and state.state == expected.state
        assert state.prepared_roles == expected_coordinator_prepares
        assert state.applied_roles == ()
        assert state.aborted_roles == expected_coordinator_aborts
        assert state.event_sequence == len(expected.event_types)
        request = coordinator["request"]
        assert request is not None
        assert request["state"] == expected.state
        assert request["active_slot"] == expected.active_slot
        assert request["completed"] is expected.completed
        assert (
            request["participant_prepare_root_sha256"] is not None
        ) is (expected.state in {PREPARED, COMMITTED, ABORTING, ABORTED})
        assert (request["committed_decision_sha256"] is not None) is expected.committed
        assert (request["committed_receipt_sha256"] is not None) is expected.committed
        assert (request["abort_decision_sha256"] is not None) is expected.abort_decided
        assert (request["abort_receipt_sha256"] is not None) is expected.abort_decided
        assert (state.committed_receipt_sha256 is not None) is expected.committed
        assert (state.abort_receipt_sha256 is not None) is expected.abort_decided

    assert tuple(item["role"] for item in coordinator["prepares"]) == (
        expected_coordinator_prepares
    )
    assert coordinator["applied_roles"] == ()
    assert tuple(item["role"] for item in coordinator["aborts"]) == (
        expected_coordinator_aborts
    )
    assert tuple(item["event_type"] for item in coordinator["events"]) == (
        expected.event_types
    )
    assert tuple(
        item["role"] for item in local if item["prepare"] is not None
    ) == expected_local_prepares
    assert tuple(
        item["role"] for item in local if item["abort"] is not None
    ) == expected_local_aborts

    local_by_role = {item["role"]: item for item in local}
    for role in PARTICIPANT_APPLY_ORDER:
        item = local_by_role[role]
        row = item["row"]
        if role not in expected_local_prepares:
            assert item["prepare"] is None and row is None
            continue
        assert item["prepare"] is not None and row is not None
        aborted = role in expected_local_aborts
        assert row["state"] == (ABORTED if aborted else PREPARED)
        assert row["active_slot"] == (None if aborted else 1)
        assert (item["abort"] is not None) is aborted

    for recorded in coordinator["prepares"]:
        physical = local_by_role[recorded["role"]]["prepare"]
        assert physical is not None
        assert recorded["prepare_receipt_sha256"] == physical[
            "prepare_receipt_sha256"
        ]
        assert recorded["source_store_identity_sha256"] == physical[
            "source_store_identity_sha256"
        ]
        assert recorded["source_fence_sha256"] == physical[
            "source_fence_sha256"
        ]
        assert recorded["payload_sha256"] == hashlib.sha256(
            canonical_bytes(physical)
        ).hexdigest()
    for recorded in coordinator["aborts"]:
        physical = local_by_role[recorded["role"]]["abort"]
        assert physical is not None
        assert recorded["abort_receipt_sha256"] == physical[
            "abort_receipt_sha256"
        ]
        assert recorded["payload_sha256"] == hashlib.sha256(
            canonical_bytes(physical)
        ).hexdigest()
    return coordinator, local


def _assert_crash_boundary(
    stack,
    *,
    case: _PhaseCrashCase,
    source_before: dict[str, object],
) -> None:
    _assert_exact_inventories(
        stack,
        request_sha256=stack.request.request_sha256,
        expected=_expected_boundary(case),
    )
    _assert_source_live(stack, source_before)


def _stable_protocol_snapshot(stack) -> dict[str, object]:
    return {
        "coordinator": _coordinator_inventory(
            stack.root, stack.request.request_sha256
        ),
        "files": _file_inventory(stack.root),
        "historical_page": _historical_page_bytes(stack),
        "local": _local_inventory(stack, stack.request.request_sha256),
        "source": _source_snapshot(stack),
    }


def _assert_fully_prepared(
    stack,
    *,
    source_before: dict[str, object],
) -> None:
    expected = _ExpectedBoundary(
        state=PREPARED,
        coordinator_prepare_count=len(PARTICIPANT_APPLY_ORDER),
        local_prepare_count=len(PARTICIPANT_APPLY_ORDER),
        coordinator_abort_count=0,
        local_abort_count=0,
        event_types=("preparing", "prepared"),
        active_slot=1,
        committed=False,
        abort_decided=False,
        completed=False,
    )
    _assert_exact_inventories(
        stack,
        request_sha256=stack.request.request_sha256,
        expected=expected,
    )
    assert _source_snapshot(stack) == source_before


def _assert_terminal_aborted(
    stack,
    *,
    request_sha256: str,
    source_before: dict[str, object],
) -> None:
    expected = _ExpectedBoundary(
        state=ABORTED,
        coordinator_prepare_count=len(PARTICIPANT_APPLY_ORDER),
        local_prepare_count=len(PARTICIPANT_APPLY_ORDER),
        coordinator_abort_count=len(PARTICIPANT_APPLY_ORDER),
        local_abort_count=len(PARTICIPANT_APPLY_ORDER),
        event_types=("preparing", "prepared", "aborting", "aborted"),
        active_slot=None,
        committed=False,
        abort_decided=True,
        completed=True,
    )
    coordinator, local = _assert_exact_inventories(
        stack,
        request_sha256=request_sha256,
        expected=expected,
    )
    decision_sha256 = coordinator["request"]["abort_decision_sha256"]
    assert decision_sha256 is not None
    for item in local:
        prepare = item["prepare"]
        abort = item["abort"]
        assert prepare is not None and abort is not None
        assert abort["prepare_receipt_sha256"] == prepare[
            "prepare_receipt_sha256"
        ]
        assert abort["source_store_identity_sha256"] == prepare[
            "source_store_identity_sha256"
        ]
        assert abort["coordinator_abort_decision_sha256"] == decision_sha256
        assert abort["fence_release_root_sha256"]
        assert item["row"]["active_slot"] is None
    assert _source_snapshot(stack) == source_before
    _assert_source_live(stack, source_before)
    _assert_healthy_audits(stack)


def _assert_abort_replay_is_inert(
    stack,
    *,
    terminal,
    source_before: dict[str, object],
) -> None:
    before = _stable_protocol_snapshot(stack)
    assert stack.coordinator.abort_v1(
        stack.request.request_sha256,
        participants=stack.participants,
    ) == terminal
    assert stack.coordinator.resume_v2(
        stack.request,
        participants=stack.participants,
        allow_commit=False,
    ) == terminal
    _assert_terminal_aborted(
        stack,
        request_sha256=stack.request.request_sha256,
        source_before=source_before,
    )
    assert _stable_protocol_snapshot(stack) == before


def _assert_successor_can_reuse_released_fences(
    stack,
    *,
    source_before: dict[str, object],
) -> None:
    catalog, reproof = _archive_authorities(
        "phase-crash-successor-registry-epoch"
    )
    epoch = stack.registry.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    bound = stack.registry.verifier(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    ).bind_current_epoch(
        claimed_epoch_root_sha256=epoch.epoch_root_sha256,
    )
    closure = _issue_closure(stack, bound)
    seal = stack.retention.seal_candidates_v2(
        (stack.candidate,),
        consumer_closures=(closure,),
        now=NOW + 22_000.0,
    )
    assert seal.candidate_bindings_sha256 != (
        stack.request.candidate_bindings_sha256
    )
    successor = stack.retention.create_reclamation_request_v2(
        seal.candidate_bindings_sha256,
        source_binding_sha256=_sha(
            f"phase-crash-successor-binding:{stack.request.request_sha256}"
        ),
        source_view_sha256=_sha(
            f"phase-crash-successor-view:{stack.request.request_sha256}"
        ),
        created_at=NOW + 23_000.0,
    )
    assert successor.request_sha256 != stack.request.request_sha256
    prepared = stack.coordinator.resume_v2(
        successor,
        participants=stack.participants,
        allow_commit=False,
    )
    assert prepared.state == PREPARED
    assert prepared.prepared_roles == PARTICIPANT_APPLY_ORDER
    successor_local = _local_inventory(stack, successor.request_sha256)
    assert all(item["prepare"] is not None for item in successor_local)
    assert all(item["abort"] is None for item in successor_local)
    assert all(item["row"]["active_slot"] == 1 for item in successor_local)
    aborted = stack.coordinator.abort_v1(
        successor.request_sha256,
        participants=stack.participants,
    )
    assert aborted.state == ABORTED
    assert aborted.aborted_roles == PARTICIPANT_APPLY_ORDER
    successor_local = _local_inventory(stack, successor.request_sha256)
    assert all(item["abort"] is not None for item in successor_local)
    assert all(item["row"]["active_slot"] is None for item in successor_local)
    assert _source_snapshot(stack) == source_before


def _assert_applied_replay_is_inert(stack, *, terminal) -> None:
    before = _terminal_snapshot(stack)
    predecessor = None
    for participant in stack.participants:
        expected = participant.load_source_local_apply_v1(
            stack.request.request_sha256
        )
        assert expected is not None
        assert participant.apply_source_local_v1(
            stack.request.request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        ) == expected
        predecessor = expected.apply_receipt_sha256
    assert stack.coordinator.resume_v2(
        stack.request,
        participants=stack.participants,
        allow_commit=False,
    ) == terminal
    assert _terminal_snapshot(stack) == before


def _setup_and_crash(
    tmp_path: Path,
    *,
    setup_action: str,
    case: _PhaseCrashCase,
) -> None:
    setup = _run_child(
        tmp_path,
        action=setup_action,
        target="none",
        checkpoint="none",
    )
    assert setup.returncode == 0, (setup.stdout, setup.stderr)
    crashed = _run_child(
        tmp_path,
        action=case.phase,
        target=case.target,
        checkpoint=case.checkpoint,
        occurrence=case.occurrence,
    )
    assert crashed.returncode == CRASH_EXIT_CODE, (
        crashed.stdout,
        crashed.stderr,
    )
    _assert_exact_hot_journal(tmp_path, case)


@pytest.mark.parametrize(
    "case",
    PREPARE_CRASH_CASES,
    ids=[case.test_id for case in PREPARE_CRASH_CASES],
)
def test_native_five_role_prepare_crash_recovers_then_aborts_exactly(
    tmp_path: Path,
    case: _PhaseCrashCase,
) -> None:
    _setup_and_crash(tmp_path, setup_action="setup-bare", case=case)
    with _reopen_stack(tmp_path, clock_base=NOW + 20_000.0) as stack:
        source_before = _source_snapshot(stack)
        _assert_crash_boundary(
            stack,
            case=case,
            source_before=source_before,
        )
        prepared = stack.coordinator.resume_v2(
            stack.request,
            participants=stack.participants,
            allow_commit=False,
        )
        assert prepared.state == PREPARED
        assert prepared.prepared_roles == PARTICIPANT_APPLY_ORDER
        _assert_fully_prepared(stack, source_before=source_before)
        prepared_snapshot = _stable_protocol_snapshot(stack)
        assert stack.coordinator.resume_v2(
            stack.request,
            participants=stack.participants,
            allow_commit=False,
        ) == prepared
        assert _stable_protocol_snapshot(stack) == prepared_snapshot

        terminal = stack.coordinator.abort_v1(
            stack.request.request_sha256,
            participants=stack.participants,
        )
        assert terminal.state == ABORTED
        _assert_terminal_aborted(
            stack,
            request_sha256=stack.request.request_sha256,
            source_before=source_before,
        )
        _assert_abort_replay_is_inert(
            stack,
            terminal=terminal,
            source_before=source_before,
        )
        _assert_successor_can_reuse_released_fences(
            stack,
            source_before=source_before,
        )


@pytest.mark.parametrize(
    "case",
    COMMIT_CRASH_CASES,
    ids=[case.test_id for case in COMMIT_CRASH_CASES],
)
def test_native_five_role_commit_crash_is_exactly_old_or_new(
    tmp_path: Path,
    case: _PhaseCrashCase,
) -> None:
    _setup_and_crash(tmp_path, setup_action="setup-prepared", case=case)
    with _reopen_stack(tmp_path, clock_base=NOW + 20_000.0) as stack:
        source_before = _source_snapshot(stack)
        _assert_crash_boundary(
            stack,
            case=case,
            source_before=source_before,
        )
        if case.checkpoint == "commit.before_commit":
            prepared = stack.coordinator.resume_v2(
                stack.request,
                participants=stack.participants,
                allow_commit=False,
            )
            assert prepared.state == PREPARED
            terminal = stack.coordinator.abort_v1(
                stack.request.request_sha256,
                participants=stack.participants,
            )
            assert terminal.state == ABORTED
            _assert_terminal_aborted(
                stack,
                request_sha256=stack.request.request_sha256,
                source_before=source_before,
            )
            _assert_abort_replay_is_inert(
                stack,
                terminal=terminal,
                source_before=source_before,
            )
            _assert_successor_can_reuse_released_fences(
                stack,
                source_before=source_before,
            )
            return

        with pytest.raises(
            AcquisitionSourceReclamationApplyConflict,
            match="cannot be aborted",
        ):
            stack.coordinator.abort_v1(
                stack.request.request_sha256,
                participants=stack.participants,
            )
        terminal = stack.coordinator.resume_v2(
            stack.request,
            participants=stack.participants,
            allow_commit=False,
        )
        assert terminal.state == APPLIED
        assert terminal.applied_roles == PARTICIPANT_APPLY_ORDER
        _assert_terminal_effects(stack)
        _assert_applied_replay_is_inert(stack, terminal=terminal)


@pytest.mark.parametrize(
    "case",
    ABORT_CRASH_CASES,
    ids=[case.test_id for case in ABORT_CRASH_CASES],
)
def test_native_five_role_abort_crash_resumes_without_source_mutation(
    tmp_path: Path,
    case: _PhaseCrashCase,
) -> None:
    _setup_and_crash(tmp_path, setup_action="setup-prepared", case=case)
    with _reopen_stack(tmp_path, clock_base=NOW + 20_000.0) as stack:
        source_before = _source_snapshot(stack)
        expected = _expected_boundary(case)
        _assert_crash_boundary(
            stack,
            case=case,
            source_before=source_before,
        )
        resumed = stack.coordinator.resume_v2(
            stack.request,
            participants=stack.participants,
            allow_commit=False,
        )
        if expected.state == PREPARED:
            assert resumed.state == PREPARED
            terminal = stack.coordinator.abort_v1(
                stack.request.request_sha256,
                participants=stack.participants,
            )
        else:
            terminal = resumed
        assert terminal.state == ABORTED
        _assert_terminal_aborted(
            stack,
            request_sha256=stack.request.request_sha256,
            source_before=source_before,
        )
        _assert_abort_replay_is_inert(
            stack,
            terminal=terminal,
            source_before=source_before,
        )
        _assert_successor_can_reuse_released_fences(
            stack,
            source_before=source_before,
        )


assert len(PREPARE_CRASH_CASES) == 24
assert len(COMMIT_CRASH_CASES) == 2
assert len(ABORT_CRASH_CASES) == 24
assert len(ALL_PHASE_CRASH_CASES) == 50
assert len({case.test_id for case in ALL_PHASE_CRASH_CASES}) == 50
assert frozenset(case.key for case in ALL_PHASE_CRASH_CASES) == (
    PINNED_EXPECTED_CASE_KEYS
)
