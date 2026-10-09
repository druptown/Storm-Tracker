from __future__ import annotations

import ast
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import runpy
import sqlite3
import subprocess
import sys

import pytest

from storm_tracker_worker import operational_snapshot_publication as subject


PREPARE_NOW = 1_800_000_000.0
CRASH_NOW = PREPARE_NOW + 10.0
ENVELOPE_SHA256 = "a" * 64
DESIRED_STATE_SHA256 = "b" * 64
PUBLICATION_ID_SHA256 = "c" * 64
UNKNOWN_DESIRED_STATE_SHA256 = "0" * 64
V1_ALGORITHM = "worker-operational-publication-outbox-v1"
V2_ALGORITHM = "worker-operational-publication-outbox-v2"
ROLLBACK_JOURNAL_MAGIC = bytes.fromhex("d9d505f920a163d7")
V1_COLUMNS = (
    "singleton",
    "revision",
    "envelope_sha256",
    "desired_state_sha256",
    "publication_id_sha256",
    "state",
    "attempt_count",
    "staged_at",
    "last_attempt_at",
    "published_at",
    "record_sha256",
)
V2_COLUMNS = V1_COLUMNS[:-1] + ("superseded_at", "record_sha256")
TABLES = ("publication_manifest", "publication_transport_head")
OPERATIONS = (
    "stage",
    "adopt",
    "pending",
    "attempt",
    "published",
    "superseded",
    "discard",
)
PUBLIC_METHODS = {
    "stage": "stage",
    "adopt_pending": "adopt",
    "mark_pending": "pending",
    "record_attempt": "attempt",
    "mark_published": "published",
    "mark_superseded": "superseded",
    "discard_staged": "discard",
}
PHYSICAL_FAULT_GROUPS = {
    "_migrate_v1_to_v2": (
        "migration.after_copy",
        "migration.before_commit",
    ),
    "_transaction": (
        "{operation}.after_write",
        "{operation}.before_commit",
        "{operation}.after_commit",
    ),
}
EXISTING_HARD_EXIT_POINTS = (
    "stage.before_commit",
    "stage.after_commit",
)
REMAINING_CASES = (
    ("migration", "migration.after_copy", "old"),
    ("migration", "migration.before_commit", "old"),
    ("stage", "stage.after_write", "old"),
    ("adopt", "adopt.after_write", "old"),
    ("adopt", "adopt.before_commit", "old"),
    ("adopt", "adopt.after_commit", "new"),
    ("pending", "pending.after_write", "old"),
    ("pending", "pending.before_commit", "old"),
    ("pending", "pending.after_commit", "new"),
    ("attempt", "attempt.after_write", "old"),
    ("attempt", "attempt.before_commit", "old"),
    ("attempt", "attempt.after_commit", "new"),
    ("published", "published.after_write", "old"),
    ("published", "published.before_commit", "old"),
    ("published", "published.after_commit", "new"),
    ("superseded", "superseded.after_write", "old"),
    ("superseded", "superseded.before_commit", "old"),
    ("superseded", "superseded.after_commit", "new"),
    ("discard", "discard.after_write", "old"),
    ("discard", "discard.before_commit", "old"),
    ("discard", "discard.after_commit", "new"),
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _record(
    *,
    desired_state_sha256: str = DESIRED_STATE_SHA256,
    state: str,
    attempt_count: int,
    staged_at: float,
    last_attempt_at: float | None = None,
    published_at: float | None = None,
    superseded_at: float | None = None,
) -> dict[str, object]:
    return {
        "revision": 1,
        "envelope_sha256": ENVELOPE_SHA256,
        "desired_state_sha256": desired_state_sha256,
        "publication_id_sha256": PUBLICATION_ID_SHA256,
        "state": state,
        "attempt_count": attempt_count,
        "staged_at": staged_at,
        "last_attempt_at": last_attempt_at,
        "published_at": published_at,
        "superseded_at": superseded_at,
    }


LEGACY_PUBLISHED = _record(
    state="published",
    attempt_count=1,
    staged_at=PREPARE_NOW - 2.0,
    last_attempt_at=PREPARE_NOW - 1.0,
    published_at=PREPARE_NOW,
)
STAGED = _record(
    state="staged",
    attempt_count=0,
    staged_at=PREPARE_NOW,
)
PENDING = _record(
    state="pending",
    attempt_count=0,
    staged_at=PREPARE_NOW,
)
PREPARED_ATTEMPT = _record(
    state="pending",
    attempt_count=1,
    staged_at=PREPARE_NOW,
    last_attempt_at=PREPARE_NOW,
)
ADOPTED = _record(
    desired_state_sha256=UNKNOWN_DESIRED_STATE_SHA256,
    state="pending",
    attempt_count=0,
    staged_at=CRASH_NOW,
)
CRASH_ATTEMPT = _record(
    state="pending",
    attempt_count=1,
    staged_at=PREPARE_NOW,
    last_attempt_at=CRASH_NOW,
)
PUBLISHED = _record(
    state="published",
    attempt_count=1,
    staged_at=PREPARE_NOW,
    last_attempt_at=PREPARE_NOW,
    published_at=CRASH_NOW,
)
SUPERSEDED = _record(
    state="superseded",
    attempt_count=0,
    staged_at=PREPARE_NOW,
    superseded_at=CRASH_NOW,
)


def _checksum(record: dict[str, object], *, version: int) -> str:
    material = {
        "algorithm_version": V1_ALGORITHM if version == 1 else V2_ALGORITHM,
        "attempt_count": record["attempt_count"],
        "desired_state_sha256": record["desired_state_sha256"],
        "envelope_sha256": record["envelope_sha256"],
        "last_attempt_at": record["last_attempt_at"],
        "publication_id_sha256": record["publication_id_sha256"],
        "published_at": record["published_at"],
        "revision": record["revision"],
        "staged_at": record["staged_at"],
        "state": record["state"],
    }
    if version == 2:
        material["superseded_at"] = record["superseded_at"]
    return hashlib.sha256(_canonical(material)).hexdigest()


def _row(record: dict[str, object] | None, *, version: int) -> tuple[object, ...]:
    if record is None:
        return ()
    values = (
        1,
        record["revision"],
        record["envelope_sha256"],
        record["desired_state_sha256"],
        record["publication_id_sha256"],
        record["state"],
        record["attempt_count"],
        record["staged_at"],
        record["last_attempt_at"],
        record["published_at"],
    )
    if version == 2:
        values += (record["superseded_at"],)
    return values + (_checksum(record, version=version),)


def _file_witness(path: Path) -> tuple[str, ...]:
    return tuple(sorted(
        item.name
        for item in path.parent.iterdir()
        if item.name.startswith(path.name)
    ))


def _journal_witness(path: Path) -> tuple[int, bytes, bool] | None:
    journal = path.with_name(f"{path.name}-journal")
    if not journal.exists():
        return None
    assert journal.is_file()
    payload = journal.read_bytes()
    return len(payload), payload[:8], any(payload[8:])


def _snapshot(path: Path) -> dict[str, object]:
    files_before_open = _file_witness(path)
    journal_before_open = _journal_witness(path)
    with closing(sqlite3.connect(path)) as database:
        tables = tuple(sorted(
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        ))
        manifest = tuple(sorted(
            (str(key), str(value))
            for key, value in database.execute(
                "SELECT key,value FROM publication_manifest"
            )
        ))
        columns = tuple(
            str(row[1])
            for row in database.execute(
                "PRAGMA table_info(publication_transport_head)"
            )
        )
        rows = tuple(
            tuple(row)
            for row in database.execute(
                "SELECT * FROM publication_transport_head"
            )
        )
        integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
        quick = str(database.execute("PRAGMA quick_check(1)").fetchone()[0])
        journal_mode = str(database.execute("PRAGMA journal_mode").fetchone()[0])
    return {
        "columns": columns,
        "files_after_open": _file_witness(path),
        "files_before_open": files_before_open,
        "integrity": integrity,
        "journal_after_open": _journal_witness(path),
        "journal_before_open": journal_before_open,
        "journal_mode": journal_mode,
        "manifest": manifest,
        "quick_check": quick,
        "rows": rows,
        "tables": tables,
    }


def _fault_text(argument: ast.expr) -> str | None:
    if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
        return str(argument.value)
    if not isinstance(argument, ast.JoinedStr):
        return None
    result = []
    for value in argument.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            result.append(str(value.value))
        elif (
            isinstance(value, ast.FormattedValue)
            and isinstance(value.value, ast.Name)
            and value.value.id == "operation"
        ):
            result.append("{operation}")
        else:
            return None
    return "".join(result)


def _product_fault_inventory() -> dict[str, tuple[str, ...]]:
    source_path = Path(subject.__file__)
    tree = ast.parse(
        source_path.read_text(encoding="utf-8"), filename=str(source_path)
    )
    outbox_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "PublicationOutboxStore"
    )
    discovered: dict[str, tuple[str, ...]] = {}
    for method in outbox_class.body:
        if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        calls = tuple(
            call
            for call in ast.walk(method)
            if (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and isinstance(call.func.value, ast.Name)
                and call.func.value.id == "self"
                and call.func.attr == "_fault"
                and len(call.args) == 1
                and _fault_text(call.args[0]) is not None
            )
        )
        if calls:
            discovered[method.name] = tuple(
                str(_fault_text(call.args[0]))
                for call in sorted(
                    calls, key=lambda item: (item.lineno, item.col_offset)
                )
            )
    return discovered


def _product_operation_inventory() -> dict[str, str]:
    source_path = Path(subject.__file__)
    tree = ast.parse(
        source_path.read_text(encoding="utf-8"), filename=str(source_path)
    )
    outbox_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "PublicationOutboxStore"
    )
    methods = {
        node.name: node
        for node in outbox_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    discovered: dict[str, str] = {}
    for method_name in PUBLIC_METHODS:
        calls = tuple(
            call
            for call in ast.walk(methods[method_name])
            if (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr in {"_transaction", "_transition"}
                and call.args
                and isinstance(call.args[0], ast.Constant)
                and isinstance(call.args[0].value, str)
            )
        )
        assert len(calls) == 1
        discovered[method_name] = str(calls[0].args[0].value)
    return discovered


def _existing_hard_exit_inventory() -> tuple[str, ...]:
    path = Path(__file__).resolve().parent / (
        "test_worker_operational_snapshot_publication.py"
    )
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    test = next(
        node
        for node in tree.body
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name
            == "test_abrupt_outbox_process_death_respects_sqlite_commit_boundary"
        )
    )
    parameter = next(
        decorator
        for decorator in test.decorator_list
        if (
            isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Attribute)
            and decorator.func.attr == "parametrize"
        )
    )
    cases = ast.literal_eval(parameter.args[1])
    return tuple(str(case[0]) for case in cases)


def _manifest(version: int) -> tuple[tuple[str, str], ...]:
    return (
        (
            "algorithm_version",
            V1_ALGORITHM if version == 1 else V2_ALGORITHM,
        ),
        ("schema_version", str(version)),
    )


def _assert_snapshot(
    snapshot: dict[str, object],
    record: dict[str, object] | None,
    *,
    version: int,
    hot_journal_before_open: bool,
) -> None:
    assert snapshot["integrity"] == "ok"
    assert snapshot["quick_check"] == "ok"
    assert snapshot["journal_mode"] == "delete"
    assert snapshot["tables"] == TABLES
    assert snapshot["manifest"] == _manifest(version)
    assert snapshot["columns"] == (V1_COLUMNS if version == 1 else V2_COLUMNS)
    expected_row = _row(record, version=version)
    assert snapshot["rows"] == (() if not expected_row else (expected_row,))
    expected_before = (
        ("publication.sqlite3", "publication.sqlite3-journal")
        if hot_journal_before_open
        else None
    )
    before = snapshot["journal_before_open"]
    after = snapshot["journal_after_open"]
    if hot_journal_before_open:
        assert snapshot["files_before_open"] == expected_before
        assert isinstance(before, tuple)
        assert before[0] > 0
        assert before[1] in {ROLLBACK_JOURNAL_MAGIC, b"\0" * 8}
        assert before[2] is True
    else:
        assert before is None or before[1] == b"\0" * 8
    assert after is None or after[1] == b"\0" * 8
    expected_after = (
        ("publication.sqlite3",)
        if after is None
        else ("publication.sqlite3", "publication.sqlite3-journal")
    )
    assert snapshot["files_after_open"] == expected_after


def _record_at_crash(
    operation: str,
    phase: str,
) -> dict[str, object] | None:
    if operation == "migration":
        return LEGACY_PUBLISHED
    old = {
        "stage": None,
        "adopt": None,
        "pending": STAGED,
        "attempt": PENDING,
        "published": PREPARED_ATTEMPT,
        "superseded": PENDING,
        "discard": STAGED,
    }[operation]
    if phase == "old":
        return old
    return {
        "adopt": ADOPTED,
        "pending": PENDING,
        "attempt": CRASH_ATTEMPT,
        "published": PUBLISHED,
        "superseded": SUPERSEDED,
        "discard": None,
    }[operation]


def _as_record(value: subject.PublicationTransportHead) -> dict[str, object]:
    return {
        "revision": value.revision,
        "envelope_sha256": value.envelope_sha256,
        "desired_state_sha256": value.desired_state_sha256,
        "publication_id_sha256": value.publication_id_sha256,
        "state": value.state,
        "attempt_count": value.attempt_count,
        "staged_at": value.staged_at,
        "last_attempt_at": value.last_attempt_at,
        "published_at": value.published_at,
        "superseded_at": value.superseded_at,
    }


def _invoke(
    store: subject.PublicationOutboxStore,
    operation: str,
) -> subject.PublicationTransportHead | None:
    if operation == "stage":
        return store.stage(
            revision=1,
            envelope_sha256=ENVELOPE_SHA256,
            desired_state_sha256=DESIRED_STATE_SHA256,
            publication_id_sha256=PUBLICATION_ID_SHA256,
        )
    if operation == "adopt":
        return store.adopt_pending(
            revision=1,
            envelope_sha256=ENVELOPE_SHA256,
            publication_id_sha256=PUBLICATION_ID_SHA256,
        )
    if operation == "pending":
        return store.mark_pending(
            revision=1, envelope_sha256=ENVELOPE_SHA256
        )
    if operation == "attempt":
        return store.record_attempt(
            revision=1, envelope_sha256=ENVELOPE_SHA256
        )
    if operation == "published":
        return store.mark_published(
            revision=1, envelope_sha256=ENVELOPE_SHA256
        )
    if operation == "superseded":
        return store.mark_superseded(
            revision=1, envelope_sha256=ENVELOPE_SHA256
        )
    if operation == "discard":
        store.discard_staged(
            revision=1, envelope_sha256=ENVELOPE_SHA256
        )
        return None
    raise AssertionError(operation)


def test_publication_outbox_fault_inventory_partitions_exact_all_23() -> None:
    faults = _product_fault_inventory()
    operations = _product_operation_inventory()
    existing = _existing_hard_exit_inventory()
    assert faults == PHYSICAL_FAULT_GROUPS
    assert operations == PUBLIC_METHODS
    assert existing == EXISTING_HARD_EXIT_POINTS
    templates = faults["_transaction"]
    all_concrete = {
        *faults["_migrate_v1_to_v2"],
        *(
            template.format(operation=operation)
            for operation in OPERATIONS
            for template in templates
        ),
    }
    remaining = {checkpoint for _operation, checkpoint, _phase in REMAINING_CASES}
    assert len(all_concrete) == 23
    assert len(existing) == 2
    assert len(remaining) == len(REMAINING_CASES) == 21
    assert remaining.isdisjoint(existing)
    assert remaining | set(existing) == all_concrete
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "worker_publication_outbox_remaining_crash_child.py"
    )
    helper_groups = runpy.run_path(str(helper))["POINTS_BY_OPERATION"]
    assert {
        operation: frozenset(
            checkpoint
            for case_operation, checkpoint, _phase in REMAINING_CASES
            if case_operation == operation
        )
        for operation in ("migration", *OPERATIONS)
    } == helper_groups


@pytest.mark.parametrize(
    ("operation", "checkpoint", "phase_at_crash"),
    REMAINING_CASES,
    ids=[checkpoint.replace(".", "-") for _, checkpoint, _ in REMAINING_CASES],
)
def test_remaining_outbox_os_exit73_reopens_retries_and_replays_exactly(
    tmp_path: Path,
    operation: str,
    checkpoint: str,
    phase_at_crash: str,
) -> None:
    case_root = (tmp_path / checkpoint.replace(".", "-")).resolve()
    case_root.mkdir(parents=True)
    path = case_root / "publication.sqlite3"
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "worker_publication_outbox_remaining_crash_child.py"
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
        [sys.executable, str(helper), str(path), operation, checkpoint],
        cwd=repository,
        env=environment,
        capture_output=True,
        check=False,
        timeout=30.0,
    )
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    at_crash = _snapshot(path)
    crash_record = _record_at_crash(operation, phase_at_crash)
    crash_version = 1 if operation == "migration" else 2
    _assert_snapshot(
        at_crash,
        crash_record,
        version=crash_version,
        hot_journal_before_open=phase_at_crash == "old",
    )

    reopened = subject.PublicationOutboxStore(path, clock=lambda: CRASH_NOW)
    reopened_record = reopened.load()
    expected_reopened = LEGACY_PUBLISHED if operation == "migration" else crash_record
    assert (
        None if reopened_record is None else _as_record(reopened_record)
    ) == expected_reopened

    if operation == "migration":
        after_retry = _snapshot(path)
        _assert_snapshot(
            after_retry,
            LEGACY_PUBLISHED,
            version=2,
            hot_journal_before_open=False,
        )
        assert reopened.load() == reopened_record
        assert _snapshot(path) == after_retry
        return

    first_retry = _invoke(reopened, operation)
    if operation == "attempt":
        base_attempts = int(crash_record["attempt_count"])
        first_expected = _record(
            state="pending",
            attempt_count=base_attempts + 1,
            staged_at=PREPARE_NOW,
            last_attempt_at=CRASH_NOW,
        )
    else:
        first_expected = {
            "stage": _record(
                state="staged",
                attempt_count=0,
                staged_at=CRASH_NOW,
            ),
            "adopt": ADOPTED,
            "pending": PENDING,
            "published": PUBLISHED,
            "superseded": SUPERSEDED,
            "discard": None,
        }[operation]
    assert (
        None if first_retry is None else _as_record(first_retry)
    ) == first_expected
    first_snapshot = _snapshot(path)
    _assert_snapshot(
        first_snapshot,
        first_expected,
        version=2,
        hot_journal_before_open=False,
    )

    replay = _invoke(reopened, operation)
    if operation == "attempt":
        replay_expected = {
            **first_expected,
            "attempt_count": int(first_expected["attempt_count"]) + 1,
        }
        assert _as_record(replay) == replay_expected
        replay_snapshot = _snapshot(path)
        _assert_snapshot(
            replay_snapshot,
            replay_expected,
            version=2,
            hot_journal_before_open=False,
        )
        assert reopened.load() == replay
        assert _snapshot(path) == replay_snapshot
    else:
        assert (None if replay is None else _as_record(replay)) == first_expected
        assert reopened.load() == replay
        assert _snapshot(path) == first_snapshot
