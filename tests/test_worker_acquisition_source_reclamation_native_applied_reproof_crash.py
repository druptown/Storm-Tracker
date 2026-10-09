"""Real-process crash proof for terminal five-role APPLIED replay.

Every fixture is made terminal in the parent process.  A fresh child then
reopens every durable authority and exits through ``os._exit(73)`` at one of
the seven coordinator-only replay/reproof checkpoints.  These tests model an
abrupt process death at a logical SQLite boundary; they make no power-loss or
physical-fsync claim.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from storm_tracker_worker.acquisition_source_reclamation_protocol import (
    APPLIED,
    PARTICIPANT_APPLY_ORDER,
    canonical_bytes,
)
from test_worker_acquisition_source_reclamation_native_five_role_crash import (
    CRASH_EXIT_CODE,
    MANIFEST_CONTRACT,
    MANIFEST_NAME,
    NOW,
    ROOT,
    _assert_terminal_effects,
    _file_inventory,
    _historical_page_bytes,
    _native_five_role_stack,
    _page_bytes,
    _read_only_database,
    _reopen_stack,
    _terminal_snapshot,
)
from test_worker_acquisition_source_reclamation_native_prepare_abort_crash import (
    _coordinator_inventory,
)


CHILD = (
    ROOT
    / "tests"
    / "helpers"
    / "worker_reclamation_applied_reproof_crash_child.py"
)
REPROOF_MANIFEST_CONTRACT = "stv3-test-native-applied-reproof-crash-v1"
_REGISTRY_DATABASE_RELATIVE = (
    "registry-authority/registry/registry.sqlite3"
)
_REGISTRY_DURABLE_TABLES = (
    "registry_epochs",
    "registry_metadata",
    "registry_roots",
)
_REGISTRY_OWNER_COLUMNS = (
    "singleton",
    "owner_epoch",
    "owner_token_sha256",
    "owner_process_id",
    "acquired_at",
)
_REGISTRY_OWNER_VOLATILE_COLUMNS = _REGISTRY_OWNER_COLUMNS[1:]

_EXPECTED_ROLES = (
    "downstream_lifecycle",
    "execution_receipts",
    "scheduler",
    "raw_artifacts",
    "capacity_ledger",
)
_EXPECTED_CHECKPOINTS = (
    "apply_replay.before_participant_reproof",
    "apply_replay.after_participant_reproof.downstream_lifecycle",
    "apply_replay.after_participant_reproof.execution_receipts",
    "apply_replay.after_participant_reproof.scheduler",
    "apply_replay.after_participant_reproof.raw_artifacts",
    "apply_replay.after_participant_reproof.capacity_ledger",
    "apply_replay.after_exact_reproof",
)


@dataclass(frozen=True, slots=True)
class _AppliedReproofCrashCase:
    public_api: str
    checkpoint: str

    @property
    def test_id(self) -> str:
        return f"{self.public_api}-{self.checkpoint}"


# The endpoints exercise direct apply_v1.  Each role-specific boundary goes
# through resume_v2, proving that its public APPLIED delegation reaches the
# exact same terminal reproof path without doubling the seven-point matrix.
APPLIED_REPROOF_CRASH_CASES = (
    _AppliedReproofCrashCase(
        "apply_v1", "apply_replay.before_participant_reproof"
    ),
    _AppliedReproofCrashCase(
        "resume_v2",
        "apply_replay.after_participant_reproof.downstream_lifecycle",
    ),
    _AppliedReproofCrashCase(
        "resume_v2",
        "apply_replay.after_participant_reproof.execution_receipts",
    ),
    _AppliedReproofCrashCase(
        "resume_v2", "apply_replay.after_participant_reproof.scheduler"
    ),
    _AppliedReproofCrashCase(
        "resume_v2", "apply_replay.after_participant_reproof.raw_artifacts"
    ),
    _AppliedReproofCrashCase(
        "resume_v2", "apply_replay.after_participant_reproof.capacity_ledger"
    ),
    _AppliedReproofCrashCase("apply_v1", "apply_replay.after_exact_reproof"),
)

# Independent pins: neither the product role tuple nor a generated
# parametrization can silently change the seven required checkpoints.
assert CRASH_EXIT_CODE == 73
assert PARTICIPANT_APPLY_ORDER == _EXPECTED_ROLES
assert len(APPLIED_REPROOF_CRASH_CASES) == 7
assert len(set(APPLIED_REPROOF_CRASH_CASES)) == 7
assert tuple(item.checkpoint for item in APPLIED_REPROOF_CRASH_CASES) == (
    _EXPECTED_CHECKPOINTS
)
assert tuple(item.public_api for item in APPLIED_REPROOF_CRASH_CASES) == (
    "apply_v1",
    "resume_v2",
    "resume_v2",
    "resume_v2",
    "resume_v2",
    "resume_v2",
    "apply_v1",
)


def _setup_terminal_applied_fixture(root: Path) -> None:
    """Build one real APPLIED five-role fixture in the pytest parent."""

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

        prepared = stack.coordinator.prepare_v2(
            stack.request,
            participants=stack.participants,
        )
        assert prepared.prepared_roles == _EXPECTED_ROLES
        committed = stack.coordinator.commit_v1(
            stack.request.request_sha256,
            participants=stack.participants,
        )
        assert committed.state == "COMMITTED"
        applied = stack.coordinator.apply_v1(
            stack.request.request_sha256,
            participants=stack.participants,
        )
        assert applied.state == APPLIED
        assert applied.prepared_roles == _EXPECTED_ROLES
        assert applied.applied_roles == _EXPECTED_ROLES

        manifest = {
            "archive_files": [str(item) for item in archive_files],
            "candidate": stack.candidate.as_dict(),
            "capture_receipt_ids": [
                item.receipt_id for item in capture.members
            ],
            "contract": MANIFEST_CONTRACT,
            "coordinator_store_identity_sha256": (
                stack.coordinator.store_identity_sha256
            ),
            "historical_page_hex": historical_page_hex,
            "object_paths": [str(item) for item in stack.sources.object_paths],
            "reproof_contract": REPROOF_MANIFEST_CONTRACT,
            "request": stack.request.as_dict(),
        }
        (root / MANIFEST_NAME).write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")),
            encoding="ascii",
        )


def _case_for_child(
    *,
    public_api: str,
    checkpoint: str,
) -> _AppliedReproofCrashCase:
    requested = _AppliedReproofCrashCase(public_api, checkpoint)
    if requested not in APPLIED_REPROOF_CRASH_CASES:
        raise SystemExit("unsupported APPLIED replay/reproof crash case")
    return requested


def run_applied_reproof_crash_child(
    root: Path,
    *,
    public_api: str,
    checkpoint: str,
) -> None:
    """Closed entry point used only by the dedicated abrupt child."""

    case = _case_for_child(public_api=public_api, checkpoint=checkpoint)
    with _reopen_stack(
        root,
        target="coordinator",
        checkpoint=case.checkpoint,
        occurrence=1,
        clock_base=NOW + 30_000.0,
    ) as stack:
        before = stack.coordinator.load_state_v2(
            stack.request.request_sha256
        )
        assert before is not None and before.state == APPLIED
        assert before.applied_roles == _EXPECTED_ROLES
        if case.public_api == "apply_v1":
            stack.coordinator.apply_v1(
                stack.request.request_sha256,
                participants=stack.participants,
            )
        else:
            stack.coordinator.resume_v2(
                stack.request,
                participants=stack.participants,
                allow_commit=False,
            )
    raise SystemExit(
        f"APPLIED replay crash checkpoint was not reached: {case.checkpoint}"
    )


def _run_crash_child(
    root: Path,
    *,
    case: _AppliedReproofCrashCase,
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
            case.public_api,
            case.checkpoint,
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=90.0,
        check=False,
    )


def _encoded_sql_value(value: object) -> object:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytes):
        return ("blob", value.hex())
    if value is None or isinstance(value, (int, float, str)):
        return value
    raise AssertionError("unexpected coordinator SQLite value")


def _encoded_rows(rows) -> tuple[tuple[tuple[str, object], ...], ...]:
    return tuple(
        tuple(
            (key, _encoded_sql_value(row[key]))
            for key in row.keys()
        )
        for row in rows
    )


def _registry_semantic_inventory(root: Path) -> dict[str, object]:
    """Bind all registry facts except its per-open owner claim fields."""

    path = root / Path(_REGISTRY_DATABASE_RELATIVE)
    with _read_only_database(path) as database:
        schemas = database.execute(
            "SELECT name,sql FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        assert tuple(str(row["name"]) for row in schemas) == (
            "registry_epochs",
            "registry_metadata",
            "registry_owner",
            "registry_roots",
        )
        owner_columns = tuple(
            str(row["name"])
            for row in database.execute(
                "PRAGMA table_info(registry_owner)"
            ).fetchall()
        )
        assert owner_columns == _REGISTRY_OWNER_COLUMNS
        owner_rows = database.execute(
            "SELECT singleton FROM registry_owner ORDER BY singleton"
        ).fetchall()
        assert len(owner_rows) == 1
        durable_rows = {
            table: _encoded_rows(database.execute(
                f"SELECT * FROM {table} ORDER BY rowid"
            ).fetchall())
            for table in _REGISTRY_DURABLE_TABLES
        }
    return {
        "durable_rows": durable_rows,
        "owner_rows": _encoded_rows(owner_rows),
        "owner_volatile_columns": _REGISTRY_OWNER_VOLATILE_COLUMNS,
        "schemas": _encoded_rows(schemas),
    }


def _reproof_file_inventory(
    root: Path,
) -> tuple[tuple[str, int, str], ...]:
    """Normalize only the registry's unavoidable durable owner claim."""

    normalized = []
    registry_count = 0
    for relative, size, digest in _file_inventory(root):
        if relative == _REGISTRY_DATABASE_RELATIVE:
            registry_count += 1
            digest = "normalized:registry_owner-volatile-fields"
        normalized.append((relative, size, digest))
    assert registry_count == 1
    return tuple(normalized)


def _exact_coordinator_tables(stack) -> dict[str, object]:
    """Pin all terminal receipts, events and allocator counters byte-exactly."""

    with _read_only_database(stack.coordinator.path) as database:
        return {
            "meta": _encoded_rows(database.execute(
                "SELECT * FROM coordinator_meta ORDER BY singleton"
            ).fetchall()),
            "requests": _encoded_rows(database.execute(
                "SELECT * FROM coordinator_requests ORDER BY owner_epoch"
            ).fetchall()),
            "prepares": _encoded_rows(database.execute(
                "SELECT * FROM coordinator_prepares ORDER BY rowid"
            ).fetchall()),
            "applies": _encoded_rows(database.execute(
                "SELECT * FROM coordinator_applies ORDER BY rowid"
            ).fetchall()),
            "aborts": _encoded_rows(database.execute(
                "SELECT * FROM coordinator_aborts ORDER BY rowid"
            ).fetchall()),
            "events": _encoded_rows(database.execute(
                "SELECT * FROM coordinator_events ORDER BY event_sequence"
            ).fetchall()),
        }


def _coordinator_meta_counters(stack) -> dict[str, object]:
    with _read_only_database(stack.coordinator.path) as database:
        row = database.execute(
            "SELECT next_owner_epoch,next_event_sequence,head_event_sha256,"
            "last_observed_at FROM coordinator_meta WHERE singleton=1"
        ).fetchone()
    assert row is not None
    return {
        "head_event_sha256": str(row["head_event_sha256"]),
        "last_observed_at": float(row["last_observed_at"]),
        "next_event_sequence": int(row["next_event_sequence"]),
        "next_owner_epoch": int(row["next_owner_epoch"]),
    }


def _local_receipt_inventory(stack) -> tuple[dict[str, object], ...]:
    request_sha256 = stack.request.request_sha256
    inventory = []
    for participant in stack.participants:
        prepare = participant.load_source_local_prepare_v2(request_sha256)
        apply = participant.load_source_local_apply_v1(request_sha256)
        abort = participant.load_source_local_abort_v1(request_sha256)
        assert prepare is not None and apply is not None and abort is None
        inventory.append({
            "apply": apply.as_dict(),
            "apply_bytes": canonical_bytes(apply.as_dict()),
            "prepare": prepare.as_dict(),
            "prepare_bytes": canonical_bytes(prepare.as_dict()),
            "role": participant.role,
            "source_store_identity_sha256": (
                participant.source_store_identity_sha256
            ),
        })
    return tuple(inventory)


def _source_facts(stack) -> dict[str, object]:
    candidate = stack.candidate
    receipt_ids = stack.manifest["capture_receipt_ids"]
    assert isinstance(receipt_ids, list)
    tracked_paths = tuple(
        Path(item)
        for item in (
            *stack.manifest["archive_files"],
            *stack.manifest["object_paths"],
        )
    )
    return {
        "archive_lookup": stack.source_archive.lookup(
            candidate.candidate_sha256
        ),
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
        "historical_page": _historical_page_bytes(stack),
        "path_existence": tuple(
            (str(path), path.exists()) for path in tracked_paths
        ),
        "raw_capture": stack.sources.raw.load_capture(
            candidate.raw_capture_id
        ),
        "raw_receipts": tuple(
            stack.sources.raw.load_receipt(item) for item in receipt_ids
        ),
        "scheduler_proof": stack.sources.scheduler.terminal_proof(
            candidate.scheduler_job_id
        ),
        "scheduler_record": stack.sources.scheduler.get_record(
            candidate.scheduler_job_id
        ),
    }


def _assert_exact_terminal_applied(stack) -> None:
    request_sha256 = stack.request.request_sha256
    state = stack.coordinator.load_state_v2(request_sha256)
    assert state is not None and state.state == APPLIED
    assert state.prepared_roles == _EXPECTED_ROLES
    assert state.applied_roles == _EXPECTED_ROLES
    assert state.aborted_roles == ()
    assert state.event_sequence == 4
    assert state.committed_receipt_sha256 is not None
    assert state.abort_receipt_sha256 is None

    audit = stack.coordinator.audit_v2()
    assert audit["active_requests"] == 0
    assert audit["event_count"] == 4
    assert audit["request_count"] == 1
    assert audit["states"] == {
        "PREPARING": 0,
        "PREPARED": 0,
        "COMMITTED": 0,
        "APPLIED": 1,
        "ABORTING": 0,
        "ABORTED": 0,
    }
    counters = _coordinator_meta_counters(stack)
    assert counters["next_owner_epoch"] == 2
    assert counters["next_event_sequence"] == 5
    assert counters["head_event_sha256"] == audit["head_event_sha256"]

    coordinator = _coordinator_inventory(stack.root, request_sha256)
    request = coordinator["request"]
    assert request is not None
    assert request["state"] == APPLIED
    assert request["active_slot"] is None
    assert request["completed"] is True
    assert request["participant_prepare_root_sha256"] is not None
    assert request["committed_decision_sha256"] is not None
    assert request["committed_receipt_sha256"] is not None
    assert request["abort_decision_sha256"] is None
    assert request["abort_receipt_sha256"] is None
    assert tuple(item["role"] for item in coordinator["prepares"]) == (
        _EXPECTED_ROLES
    )
    assert coordinator["applied_roles"] == _EXPECTED_ROLES
    assert coordinator["aborts"] == ()
    assert tuple(item["event_sequence"] for item in coordinator["events"]) == (
        1,
        2,
        3,
        4,
    )
    assert tuple(item["event_type"] for item in coordinator["events"]) == (
        "preparing",
        "prepared",
        "committed",
        "applied",
    )
    assert tuple(item["state"] for item in coordinator["events"]) == (
        "PREPARING",
        "PREPARED",
        "COMMITTED",
        "APPLIED",
    )

    local = _local_receipt_inventory(stack)
    assert tuple(item["role"] for item in local) == _EXPECTED_ROLES
    with _read_only_database(stack.coordinator.path) as database:
        apply_rows = database.execute(
            "SELECT role,receipt_json FROM coordinator_applies ORDER BY rowid"
        ).fetchall()
    assert tuple(str(row["role"]) for row in apply_rows) == _EXPECTED_ROLES
    predecessor = None
    for local_item, row in zip(local, apply_rows, strict=True):
        prepare = local_item["prepare"]
        apply = local_item["apply"]
        assert apply["role"] == local_item["role"]
        assert apply["source_store_identity_sha256"] == local_item[
            "source_store_identity_sha256"
        ]
        assert apply["prepare_receipt_sha256"] == prepare[
            "prepare_receipt_sha256"
        ]
        assert apply["committed_decision_sha256"] == request[
            "committed_decision_sha256"
        ]
        assert apply["committed_decision_receipt_sha256"] == request[
            "committed_receipt_sha256"
        ]
        assert apply["predecessor_apply_receipt_sha256"] == predecessor
        assert bytes(row["receipt_json"]) == local_item["apply_bytes"]
        predecessor = apply["apply_receipt_sha256"]

    _assert_terminal_effects(stack)
    source = _source_facts(stack)
    assert source["capacity"] is None
    assert source["evidence_lookup"].status == "window_expired"
    assert source["execution_receipts"] == ()
    assert source["raw_capture"] is None
    assert all(item is None for item in source["raw_receipts"])
    assert source["scheduler_proof"] is None
    assert source["scheduler_record"] is None
    assert all(exists is False for _path, exists in source["path_existence"])
    assert source["historical_page"].hex() == stack.manifest[
        "historical_page_hex"
    ]


def _terminal_reproof_proof(stack) -> dict[str, object]:
    _assert_exact_terminal_applied(stack)
    terminal = _terminal_snapshot(stack)
    assert terminal["files"] == _file_inventory(stack.root)
    terminal = {
        **terminal,
        "files": _reproof_file_inventory(stack.root),
    }
    return {
        "coordinator_inventory": _coordinator_inventory(
            stack.root, stack.request.request_sha256
        ),
        "coordinator_tables": _exact_coordinator_tables(stack),
        "local_receipts": _local_receipt_inventory(stack),
        "meta_counters": _coordinator_meta_counters(stack),
        "registry": _registry_semantic_inventory(stack.root),
        "source": _source_facts(stack),
        "terminal": terminal,
    }


def _assert_public_replays_are_inert(stack, *, expected) -> None:
    terminal = stack.coordinator.load_state_v2(stack.request.request_sha256)
    assert terminal is not None and terminal.state == APPLIED

    direct = stack.coordinator.apply_v1(
        stack.request.request_sha256,
        participants=stack.participants,
    )
    assert direct == terminal
    assert _terminal_reproof_proof(stack) == expected

    resumed = stack.coordinator.resume_v2(
        stack.request,
        participants=stack.participants,
        allow_commit=False,
    )
    assert resumed == terminal
    assert _terminal_reproof_proof(stack) == expected


@pytest.mark.parametrize(
    "case",
    APPLIED_REPROOF_CRASH_CASES,
    ids=[item.test_id for item in APPLIED_REPROOF_CRASH_CASES],
)
def test_native_applied_replay_reproof_process_crash_is_zero_mutation(
    tmp_path: Path,
    case: _AppliedReproofCrashCase,
) -> None:
    _setup_terminal_applied_fixture(tmp_path)

    # The baseline is built only after the fixture-owning stack has closed.
    with _reopen_stack(tmp_path, clock_base=NOW + 40_000.0) as stack:
        assert stack.manifest["reproof_contract"] == (
            REPROOF_MANIFEST_CONTRACT
        )
        before = _terminal_reproof_proof(stack)
    closed_parent_files = _reproof_file_inventory(tmp_path)
    closed_parent_registry = _registry_semantic_inventory(tmp_path)
    assert before["terminal"]["files"] == closed_parent_files
    assert before["registry"] == closed_parent_registry

    crashed = _run_crash_child(tmp_path, case=case)
    assert crashed.returncode == CRASH_EXIT_CODE, (
        crashed.stdout,
        crashed.stderr,
    )

    # The child died inside a read/reproof transaction: even the complete
    # durable file inventory must still equal the closed-parent baseline.
    assert _reproof_file_inventory(tmp_path) == closed_parent_files
    assert _registry_semantic_inventory(tmp_path) == closed_parent_registry
    with _reopen_stack(tmp_path, clock_base=NOW + 50_000.0) as stack:
        after_crash = _terminal_reproof_proof(stack)
        assert after_crash == before
        _assert_public_replays_are_inert(stack, expected=before)
