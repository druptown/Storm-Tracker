"""Real-process crash proof for lifecycle claim and terminal transactions."""
from __future__ import annotations

import ast
from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from typing import Any, Mapping

import pytest

from storm_tracker_worker.acquisition_evidence_lifecycle import (
    CLOCK_AUTHORITY_SCHEMA,
    COMPLETION_EVENT_SCHEMA,
    CONSUMPTION_RECEIPT_SCHEMA,
    PRODUCER_RECEIPT_SCHEMA,
    QUARANTINE_RECEIPT_SCHEMA,
    REJECTION_RECEIPT_SCHEMA,
    AcquisitionEvidenceLifecycleStore,
)


REPOSITORY = Path(__file__).resolve().parents[1]
PRODUCT = (
    REPOSITORY
    / "storm_tracker_worker"
    / "acquisition_evidence_lifecycle.py"
)
CHILD = (
    REPOSITORY
    / "tests"
    / "helpers"
    / "worker_acquisition_evidence_terminal_crash_child.py"
)
CRASH_EXIT_CODE = 73
CHILD_TIMEOUT_SECONDS = 90.0

PUBLISH_NOW = 1_800_200_000.0
CLAIM_NOW = PUBLISH_NOW + 10.0
TERMINAL_NOW = PUBLISH_NOW + 20.0
LEASE_SECONDS = 120.0
LEASE_TOKEN = "closed-terminal-lease-token"
OWNER_ID = "closed-terminal-owner"
CONSUMER_ID = "closed-terminal-consumer"
STREAM_ID = "closed.terminal.stream"
OUTPUT_SHA256 = "3" * 64
ACTIVE_DESIRED_STATE_SHA256 = "4" * 64
EVIDENCE_FINGERPRINT_SHA256 = "5" * 64
PAYLOAD = b"closed-terminal-exact-raw"
PROOF = {
    "case": "terminal-transaction-hard-exit",
    "schema": "stv3-test-evidence-terminal-proof-v1",
}


@dataclass(frozen=True, slots=True)
class _CrashCase:
    operation: str
    checkpoint: str

    @property
    def test_id(self) -> str:
        return self.checkpoint.replace(".", "-")


CRASH_CASES = (
    _CrashCase("claim", "claim.before_commit"),
    _CrashCase("claim", "claim.after_commit"),
    _CrashCase("ack", "ack.before_receipt_commit"),
    _CrashCase("ack", "ack.after_receipt_commit"),
    _CrashCase("ack", "ack.before_cursor_commit"),
    _CrashCase("ack", "ack.after_cursor_commit"),
    _CrashCase("reject", "reject.before_receipt_commit"),
    _CrashCase("reject", "reject.after_receipt_commit"),
    _CrashCase("reject", "reject.before_cursor_commit"),
    _CrashCase("reject", "reject.after_cursor_commit"),
    _CrashCase("quarantine", "quarantine.before_receipt_commit"),
    _CrashCase("quarantine", "quarantine.after_receipt_commit"),
    _CrashCase("quarantine", "quarantine.before_cursor_commit"),
    _CrashCase("quarantine", "quarantine.after_cursor_commit"),
)

# Independent source inventory: this is deliberately not generated from the
# executable parametrization or from the child helper's allowlist.
PINNED_PRODUCT_FAULT_GROUPS = {
    "claim": (
        "claim.before_commit",
        "claim.after_commit",
    ),
    "ack": (
        "ack.before_receipt_commit",
        "ack.after_receipt_commit",
        "ack.before_cursor_commit",
        "ack.after_cursor_commit",
    ),
    "reject": (
        "reject.before_receipt_commit",
        "reject.after_receipt_commit",
        "reject.before_cursor_commit",
        "reject.after_cursor_commit",
    ),
    "quarantine": (
        "quarantine.before_receipt_commit",
        "quarantine.after_receipt_commit",
        "quarantine.before_cursor_commit",
        "quarantine.after_cursor_commit",
    ),
}


def _fault_groups_from_product() -> dict[str, tuple[str, ...]]:
    tree = ast.parse(PRODUCT.read_text(encoding="utf-8"), filename=str(PRODUCT))
    lifecycle = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and node.name == "AcquisitionEvidenceLifecycleStore"
    )
    method_names = {
        "claim": "claim_next",
        "ack": "acknowledge",
        "reject": "reject_leased",
        "quarantine": "quarantine_leased",
    }
    result: dict[str, tuple[str, ...]] = {}
    for group, method_name in method_names.items():
        method = next(
            node
            for node in lifecycle.body
            if isinstance(node, ast.FunctionDef) and node.name == method_name
        )
        calls = [
            node
            for node in ast.walk(method)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "self"
            and node.func.attr == "_fault"
        ]
        calls.sort(key=lambda node: (node.lineno, node.col_offset))
        points: list[str] = []
        for call in calls:
            if (
                len(call.args) != 1
                or not isinstance(call.args[0], ast.Constant)
                or not isinstance(call.args[0].value, str)
            ):
                raise AssertionError(
                    f"{method_name} has a non-literal lifecycle fault point"
                )
            points.append(call.args[0].value)
        result[group] = tuple(points)
    return result


def _checkpoint_map_from_helper() -> dict[str, str]:
    tree = ast.parse(CHILD.read_text(encoding="utf-8"), filename=str(CHILD))
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name)
            and target.id == "_CHECKPOINT_TO_OPERATION"
            for target in node.targets
        )
    )
    value = ast.literal_eval(assignment.value)
    if not isinstance(value, dict):
        raise AssertionError("child checkpoint allowlist is not a literal map")
    return value


def _case_groups() -> dict[str, tuple[str, ...]]:
    return {
        group: tuple(
            case.checkpoint for case in CRASH_CASES if case.operation == group
        )
        for group in PINNED_PRODUCT_FAULT_GROUPS
    }


assert len(CRASH_CASES) == 14
assert len(set(CRASH_CASES)) == 14
assert len({case.test_id for case in CRASH_CASES}) == 14
assert _case_groups() == PINNED_PRODUCT_FAULT_GROUPS
assert _fault_groups_from_product() == PINNED_PRODUCT_FAULT_GROUPS
assert _checkpoint_map_from_helper() == {
    checkpoint: group
    for group, checkpoints in PINNED_PRODUCT_FAULT_GROUPS.items()
    for checkpoint in checkpoints
}


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _store(root: Path) -> AcquisitionEvidenceLifecycleStore:
    return AcquisitionEvidenceLifecycleStore(
        (root / "lifecycle").resolve(),
        clock=lambda: PUBLISH_NOW,
        token_factory=lambda: LEASE_TOKEN,
        directory_fsync=lambda _path: None,
    )


def _run_crash_child(root: Path, case: _CrashCase) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    python_path = [str(REPOSITORY), str(REPOSITORY / ".testdeps")]
    existing = environment.get("PYTHONPATH")
    if existing:
        python_path.append(existing)
    environment["PYTHONPATH"] = os.pathsep.join(python_path)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    return subprocess.run(
        [
            sys.executable,
            "-B",
            str(CHILD),
            str(root),
            case.operation,
            case.checkpoint,
        ],
        cwd=REPOSITORY,
        env=environment,
        capture_output=True,
        text=True,
        timeout=CHILD_TIMEOUT_SECONDS,
        check=False,
    )


def _database_path(root: Path) -> Path:
    return root / "lifecycle" / "acquisition_evidence.sqlite3"


def _rows(database: sqlite3.Connection, statement: str) -> tuple[dict[str, Any], ...]:
    return tuple(dict(row) for row in database.execute(statement).fetchall())


def _state_facts(root: Path) -> dict[str, tuple[dict[str, Any], ...]]:
    with closing(sqlite3.connect(_database_path(root))) as database:
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA query_only=ON")
        return {
            "clock": _rows(
                database,
                "SELECT * FROM lifecycle_clock ORDER BY singleton",
            ),
            "cursors": _rows(
                database,
                "SELECT * FROM consumer_cursors ORDER BY consumer_id,stream_id",
            ),
            "hot_objects": _rows(
                database,
                "SELECT * FROM hot_objects ORDER BY content_sha256",
            ),
            "outbox": _rows(
                database,
                "SELECT * FROM completion_outbox ORDER BY event_id",
            ),
            "producers": _rows(
                database,
                "SELECT * FROM producer_receipts ORDER BY global_sequence",
            ),
            "stream_heads": _rows(
                database,
                "SELECT * FROM stream_heads ORDER BY stream_id",
            ),
            "consumption": _rows(
                database,
                "SELECT * FROM consumption_receipts ORDER BY event_id",
            ),
            "rejection": _rows(
                database,
                "SELECT * FROM rejection_receipts ORDER BY event_id",
            ),
            "quarantine": _rows(
                database,
                "SELECT * FROM quarantine_receipts ORDER BY event_id",
            ),
        }


_SNAPSHOT_TABLES = (
    ("lifecycle_clock", "singleton"),
    ("lifecycle_heads", "singleton"),
    ("hot_objects", "content_sha256"),
    ("producer_receipts", "global_sequence"),
    ("stream_heads", "stream_id"),
    ("completion_outbox", "event_id"),
    ("consumer_cursors", "consumer_id,stream_id"),
    ("consumption_receipts", "event_id"),
    ("rejection_receipts", "event_id"),
    ("quarantine_receipts", "event_id"),
)


def _sql_snapshot(root: Path) -> tuple[tuple[str, tuple[tuple[Any, ...], ...]], ...]:
    with closing(sqlite3.connect(_database_path(root))) as database:
        database.execute("PRAGMA query_only=ON")
        return tuple(
            (
                table,
                tuple(
                    tuple(row)
                    for row in database.execute(
                        f"SELECT * FROM {table} ORDER BY {ordering}"
                    ).fetchall()
                ),
            )
            for table, ordering in _SNAPSHOT_TABLES
        )


def _object_inventory(root: Path) -> tuple[tuple[str, int, str], ...]:
    lifecycle = root / "lifecycle"
    rows: list[tuple[str, int, str]] = []
    for relative_root in ("hot_objects", "archive/objects", "archive/segments"):
        directory = lifecycle / relative_root
        for path in sorted(directory.iterdir()):
            payload = path.read_bytes()
            rows.append((
                path.relative_to(lifecycle).as_posix(),
                len(payload),
                hashlib.sha256(payload).hexdigest(),
            ))
    return tuple(rows)


def _receipt_mappings(
    facts: Mapping[str, tuple[dict[str, Any], ...]],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for key, schema, checksum in (
        ("consumption", CONSUMPTION_RECEIPT_SCHEMA, "receipt_sha256"),
        ("rejection", REJECTION_RECEIPT_SCHEMA, "rejection_receipt_sha256"),
        ("quarantine", QUARANTINE_RECEIPT_SCHEMA, "quarantine_receipt_sha256"),
    ):
        rows = facts[key]
        if not rows:
            continue
        assert len(rows) == 1
        mapping = {**rows[0], "schema": schema}
        expected = mapping.pop(checksum)
        assert _digest(mapping) == expected
        mapping[checksum] = expected
        result[key] = mapping
    return result


def _assert_published_identity(
    facts: Mapping[str, tuple[dict[str, Any], ...]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    assert len(facts["clock"]) == 1
    assert len(facts["hot_objects"]) == 1
    assert len(facts["outbox"]) == 1
    assert len(facts["producers"]) == 1
    assert len(facts["stream_heads"]) == 1
    assert len(facts["cursors"]) <= 1
    assert len(facts["consumption"]) <= 1
    assert len(facts["rejection"]) <= 1
    assert len(facts["quarantine"]) <= 1
    producer = facts["producers"][0]
    outbox = facts["outbox"][0]
    content_sha = hashlib.sha256(PAYLOAD).hexdigest()
    proof_bytes = _canonical(PROOF)
    proof_sha = hashlib.sha256(proof_bytes).hexdigest()
    producer_body = {
        "acquisition_job_id": "closed-terminal-job",
        "authority_sha256": "2" * 64,
        "consumer_id": CONSUMER_ID,
        "content_sha256": content_sha,
        "created_at": PUBLISH_NOW,
        "desired_state_sha256": "1" * 64,
        "global_sequence": 1,
        "proof_sha256": proof_sha,
        "schema": PRODUCER_RECEIPT_SCHEMA,
        "size_bytes": len(PAYLOAD),
        "stream_id": STREAM_ID,
        "stream_sequence": 1,
    }
    producer_sha = _digest(producer_body)
    assert producer == {
        "producer_receipt_sha256": producer_sha,
        "acquisition_job_id": "closed-terminal-job",
        "consumer_id": CONSUMER_ID,
        "stream_id": STREAM_ID,
        "stream_sequence": 1,
        "global_sequence": 1,
        "desired_state_sha256": "1" * 64,
        "authority_sha256": "2" * 64,
        "proof_sha256": proof_sha,
        "proof_json": proof_bytes,
        "content_sha256": content_sha,
        "size_bytes": len(PAYLOAD),
        "created_at": PUBLISH_NOW,
    }
    event_body = {
        "consumer_id": CONSUMER_ID,
        "producer_receipt_sha256": producer_sha,
        "schema": COMPLETION_EVENT_SCHEMA,
        "stream_id": STREAM_ID,
        "stream_sequence": 1,
    }
    event_sha = _digest(event_body)
    event_id = "completion-" + hashlib.sha256(
        (COMPLETION_EVENT_SCHEMA + producer_sha).encode("ascii")
    ).hexdigest()
    assert outbox["event_id"] == event_id
    assert outbox["event_sha256"] == event_sha
    assert outbox["producer_receipt_sha256"] == producer_sha
    assert outbox["consumer_id"] == CONSUMER_ID
    assert outbox["stream_id"] == STREAM_ID
    assert outbox["stream_sequence"] == 1
    assert outbox["ready_at"] == PUBLISH_NOW
    assert outbox["superseded_by_event_id"] is None
    assert outbox["supersession_sha256"] is None
    assert facts["stream_heads"] == ({
        "stream_id": STREAM_ID,
        "consumer_id": CONSUMER_ID,
        "last_sequence": 1,
    },)
    assert facts["hot_objects"] == ({
        "content_sha256": content_sha,
        "size_bytes": len(PAYLOAD),
        "filename": f"{content_sha}.raw",
        "state": "active",
        "created_at": PUBLISH_NOW,
    },)
    return producer, outbox


def _receipts_durable_at_crash(case: _CrashCase) -> bool:
    if case.operation in {"ack", "reject"}:
        return case.checkpoint != f"{case.operation}.before_receipt_commit"
    if case.operation == "quarantine":
        return case.checkpoint == "quarantine.after_cursor_commit"
    return False


def _assert_crash_boundary(case: _CrashCase, facts) -> dict[str, dict[str, Any]]:
    _producer, outbox = _assert_published_identity(facts)
    receipts = _receipt_mappings(facts)
    terminal = case.checkpoint.endswith("after_cursor_commit")
    if case.operation == "claim":
        leased = case.checkpoint == "claim.after_commit"
        expected_state = "leased" if leased else "ready"
        expected_attempts = int(leased)
        expected_cursor = (
            ({
                "consumer_id": CONSUMER_ID,
                "stream_id": STREAM_ID,
                "last_sequence": 0,
                "last_event_sha256": None,
                "updated_at": CLAIM_NOW,
            },)
            if leased
            else ()
        )
    else:
        expected_state = "consumed" if terminal else "leased"
        expected_attempts = 1
        expected_cursor = ({
            "consumer_id": CONSUMER_ID,
            "stream_id": STREAM_ID,
            "last_sequence": 1 if terminal else 0,
            "last_event_sha256": outbox["event_sha256"] if terminal else None,
            "updated_at": TERMINAL_NOW if terminal else CLAIM_NOW,
        },)
    assert outbox["state"] == expected_state
    assert outbox["attempt_count"] == expected_attempts
    if expected_state == "leased":
        assert outbox["lease_owner"] == OWNER_ID
        assert outbox["lease_token_sha256"] == hashlib.sha256(
            LEASE_TOKEN.encode("utf-8")
        ).hexdigest()
        assert outbox["lease_expires_at"] == CLAIM_NOW + LEASE_SECONDS
        assert outbox["terminal_at"] is None
    else:
        assert outbox["lease_owner"] is None
        assert outbox["lease_token_sha256"] is None
        assert outbox["lease_expires_at"] is None
        assert outbox["terminal_at"] == (
            TERMINAL_NOW if expected_state == "consumed" else None
        )
    assert facts["cursors"] == expected_cursor
    expected_high_water = (
        CLAIM_NOW
        if case.operation == "claim"
        and case.checkpoint == "claim.after_commit"
        else PUBLISH_NOW
        if case.operation == "claim"
        else TERMINAL_NOW
        if _receipts_durable_at_crash(case)
        else CLAIM_NOW
    )
    assert facts["clock"] == ({
        "singleton": 1,
        "authority_schema": CLOCK_AUTHORITY_SCHEMA,
        "high_water": expected_high_water,
    },)
    if _receipts_durable_at_crash(case):
        child_key = (
            "rejection" if case.operation == "reject" else case.operation
        )
        assert set(receipts) == (
            {"consumption", child_key}
            if case.operation in {"reject", "quarantine"}
            else {"consumption"}
        )
    else:
        assert receipts == {}
    return receipts


def _terminal_call(
    store: AcquisitionEvidenceLifecycleStore,
    operation: str,
    *,
    event_id: str,
    event_sha256: str,
) -> dict[str, Any]:
    if operation in {"claim", "ack"}:
        return store.acknowledge(
            event_id,
            LEASE_TOKEN,
            output_sha256=OUTPUT_SHA256,
            now=TERMINAL_NOW,
        ).as_dict()
    if operation == "reject":
        return store.reject_leased(
            event_id,
            LEASE_TOKEN,
            expected_event_sha256=event_sha256,
            reason_code="desired_state_changed_after_processing",
            active_desired_state_sha256=ACTIVE_DESIRED_STATE_SHA256,
            now=TERMINAL_NOW,
        ).as_dict()
    result = store.quarantine_leased(
        event_id,
        LEASE_TOKEN,
        expected_event_sha256=event_sha256,
        reason_code="decoder_output_integrity_failed",
        evidence_fingerprint_sha256=EVIDENCE_FINGERPRINT_SHA256,
        now=TERMINAL_NOW,
    )
    return result.as_dict()


def _assert_final_boundary(
    operation: str,
    facts,
) -> dict[str, dict[str, Any]]:
    _producer, outbox = _assert_published_identity(facts)
    assert outbox["state"] == "consumed"
    assert outbox["attempt_count"] == 1
    assert outbox["lease_owner"] is None
    assert outbox["lease_token_sha256"] is None
    assert outbox["lease_expires_at"] is None
    assert outbox["terminal_at"] == TERMINAL_NOW
    assert facts["cursors"] == ({
        "consumer_id": CONSUMER_ID,
        "stream_id": STREAM_ID,
        "last_sequence": 1,
        "last_event_sha256": outbox["event_sha256"],
        "updated_at": TERMINAL_NOW,
    },)
    assert facts["clock"] == ({
        "singleton": 1,
        "authority_schema": CLOCK_AUTHORITY_SCHEMA,
        "high_water": TERMINAL_NOW,
    },)
    receipts = _receipt_mappings(facts)
    expected_keys = {"consumption"}
    if operation == "reject":
        expected_keys.add("rejection")
    elif operation == "quarantine":
        expected_keys.add("quarantine")
    assert set(receipts) == expected_keys
    if operation in {"claim", "ack"}:
        assert receipts["consumption"]["output_sha256"] == OUTPUT_SHA256
    elif operation == "reject":
        assert (
            receipts["consumption"]["output_sha256"]
            == receipts["rejection"]["rejection_receipt_sha256"]
        )
    else:
        assert (
            receipts["consumption"]["output_sha256"]
            == receipts["quarantine"]["quarantine_receipt_sha256"]
        )
    return receipts


def test_terminal_transaction_native_crash_inventory_pins_every_product_fault():
    assert _fault_groups_from_product() == PINNED_PRODUCT_FAULT_GROUPS
    assert _case_groups() == PINNED_PRODUCT_FAULT_GROUPS
    assert _checkpoint_map_from_helper() == {
        checkpoint: group
        for group, checkpoints in PINNED_PRODUCT_FAULT_GROUPS.items()
        for checkpoint in checkpoints
    }


@pytest.mark.parametrize(
    "case",
    CRASH_CASES,
    ids=[case.test_id for case in CRASH_CASES],
)
def test_terminal_transaction_real_process_crash_reopens_and_replays_exactly_once(
    tmp_path: Path,
    case: _CrashCase,
) -> None:
    root = tmp_path / case.test_id
    root.mkdir()
    crashed = _run_crash_child(root, case)
    assert crashed.returncode == CRASH_EXIT_CODE, (
        crashed.stdout,
        crashed.stderr,
    )

    crash_facts = _state_facts(root)
    crash_receipts = _assert_crash_boundary(case, crash_facts)
    event = crash_facts["outbox"][0]

    store = _store(root)
    if case.operation == "claim":
        if event["state"] == "ready":
            lease = store.claim_next(
                CONSUMER_ID,
                STREAM_ID,
                OWNER_ID,
                lease_seconds=LEASE_SECONDS,
                now=CLAIM_NOW,
            )
            assert lease is not None
        else:
            assert store.claim_next(
                CONSUMER_ID,
                STREAM_ID,
                OWNER_ID,
                lease_seconds=LEASE_SECONDS,
                now=CLAIM_NOW,
            ) is None
            lease = store.load_leased(event["event_id"], LEASE_TOKEN)
        assert lease.event_id == event["event_id"]
        assert lease.event_sha256 == event["event_sha256"]
        assert lease.attempt_count == 1

    first = _terminal_call(
        store,
        case.operation,
        event_id=event["event_id"],
        event_sha256=event["event_sha256"],
    )
    final_facts = _state_facts(root)
    final_receipts = _assert_final_boundary(case.operation, final_facts)
    for receipt_kind, mapping in crash_receipts.items():
        assert final_receipts[receipt_kind] == mapping

    if case.operation in {"claim", "ack", "reject"}:
        assert first == final_receipts["consumption"]
    else:
        assert first == {
            "consumption_receipt": final_receipts["consumption"],
            "quarantine_receipt": final_receipts["quarantine"],
            "terminal_kind": "quarantined",
        }

    terminal = store.terminal_event_identity(STREAM_ID, 1)
    assert terminal is not None
    assert terminal.status == "hot"
    assert terminal.terminal_kind == {
        "claim": "consumed",
        "ack": "consumed",
        "reject": "rejected",
        "quarantine": "quarantined",
    }[case.operation]
    assert terminal.terminal_at == TERMINAL_NOW
    expected_output = final_receipts["consumption"]["output_sha256"]
    assert terminal.terminal_output_sha256 == expected_output
    terminal_mapping = terminal.as_dict()
    terminal_sha = terminal.identity_sha256
    audit = store.audit_archive()
    assert audit["healthy"] is True
    assert audit["hot_completions"] == 1
    assert audit["hot_objects"] == 1

    before_second_sql = _sql_snapshot(root)
    before_second_objects = _object_inventory(root)
    second = _terminal_call(
        store,
        case.operation,
        event_id=event["event_id"],
        event_sha256=event["event_sha256"],
    )
    assert second == first
    assert _sql_snapshot(root) == before_second_sql
    assert _object_inventory(root) == before_second_objects
    replayed_terminal = store.terminal_event_identity(STREAM_ID, 1)
    assert replayed_terminal is not None
    assert replayed_terminal.as_dict() == terminal_mapping
    assert replayed_terminal.identity_sha256 == terminal_sha
    assert store.audit_archive() == audit
