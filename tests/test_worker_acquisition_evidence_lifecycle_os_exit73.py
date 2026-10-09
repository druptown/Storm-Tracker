from __future__ import annotations

import ast
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys

import pytest

from storm_tracker_worker import acquisition_evidence_lifecycle as subject


NOW = 1_800_000_000.0
JOB_ID = "job-acquisition-evidence-lifecycle-crash"
CONSUMER_ID = "decoder-v1"
STREAM_ID = "radar.test.raw"
DESIRED_STATE_SHA256 = "1" * 64
AUTHORITY_SHA256 = "2" * 64
PROOF = {"schema": "test-proof-v1", "sequence": "crash"}
PAYLOAD = b"exact-raw-acquisition-evidence-lifecycle"
PRODUCER_SCHEMA = "stv3-worker-acquisition-producer-receipt-v1"
EVENT_SCHEMA = "stv3-worker-acquisition-completion-event-v1"

_FAULT_GROUPS = {
    "_write_exact_object": (
        "object.after_write",
        "object.after_fsync",
        "object.after_replace",
    ),
    "publish": (
        "publish.after_hot_object",
        "publish.after_producer_insert",
        "publish.before_producer_commit",
        "publish.after_producer_commit",
        "publish.before_ready_commit",
        "publish.after_ready_commit",
    ),
}
_CRASH_CASES = (
    ("object.after_write", "old", True),
    ("object.after_fsync", "old", True),
    ("object.after_replace", "old", False),
    ("publish.after_hot_object", "old", False),
    ("publish.after_producer_insert", "old", False),
    ("publish.before_producer_commit", "old", False),
    ("publish.after_producer_commit", "producer-new", False),
    ("publish.before_ready_commit", "producer-new", False),
    ("publish.after_ready_commit", "ready-new", False),
)


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


EXPECTED_PROOF_SHA256 = hashlib.sha256(_canonical(PROOF)).hexdigest()
EXPECTED_CONTENT_SHA256 = hashlib.sha256(PAYLOAD).hexdigest()
_PRODUCER_CORE = {
    "acquisition_job_id": JOB_ID,
    "authority_sha256": AUTHORITY_SHA256,
    "consumer_id": CONSUMER_ID,
    "content_sha256": EXPECTED_CONTENT_SHA256,
    "desired_state_sha256": DESIRED_STATE_SHA256,
    "proof_sha256": EXPECTED_PROOF_SHA256,
    "schema": PRODUCER_SCHEMA,
    "size_bytes": len(PAYLOAD),
    "stream_id": STREAM_ID,
}
EXPECTED_PRODUCER_SHA256 = _digest({
    **_PRODUCER_CORE,
    "created_at": NOW,
    "global_sequence": 1,
    "stream_sequence": 1,
})
EXPECTED_EVENT_ID = "completion-" + hashlib.sha256(
    (EVENT_SCHEMA + EXPECTED_PRODUCER_SHA256).encode("ascii")
).hexdigest()
EXPECTED_EVENT_SHA256 = _digest({
    "consumer_id": CONSUMER_ID,
    "producer_receipt_sha256": EXPECTED_PRODUCER_SHA256,
    "schema": EVENT_SCHEMA,
    "stream_id": STREAM_ID,
    "stream_sequence": 1,
})
EXPECTED_RECEIPT = {
    "acquisition_job_id": JOB_ID,
    "authority_sha256": AUTHORITY_SHA256,
    "consumer_id": CONSUMER_ID,
    "content_sha256": EXPECTED_CONTENT_SHA256,
    "created_at": NOW,
    "desired_state_sha256": DESIRED_STATE_SHA256,
    "event_id": EXPECTED_EVENT_ID,
    "event_sha256": EXPECTED_EVENT_SHA256,
    "global_sequence": 1,
    "producer_receipt_sha256": EXPECTED_PRODUCER_SHA256,
    "proof_sha256": EXPECTED_PROOF_SHA256,
    "ready_at": NOW,
    "schema": PRODUCER_SCHEMA,
    "size_bytes": len(PAYLOAD),
    "stream_id": STREAM_ID,
    "stream_sequence": 1,
}


def _store(root: Path) -> subject.AcquisitionEvidenceLifecycleStore:
    return subject.AcquisitionEvidenceLifecycleStore(
        root,
        clock=lambda: NOW,
        token_factory=lambda: "b" * 64,
        directory_fsync=lambda _path: None,
    )


def _publish(
    store: subject.AcquisitionEvidenceLifecycleStore,
) -> subject.CompletionProducerReceipt:
    return store.publish(
        acquisition_job_id=JOB_ID,
        consumer_id=CONSUMER_ID,
        stream_id=STREAM_ID,
        desired_state_sha256=DESIRED_STATE_SHA256,
        authority_sha256=AUTHORITY_SHA256,
        proof=PROOF,
        payload=PAYLOAD,
    )


def _fault_group_inventory() -> dict[str, tuple[str, ...]]:
    source_path = Path(subject.__file__)
    tree = ast.parse(
        source_path.read_text(encoding="utf-8"), filename=str(source_path)
    )
    store_class = next(
        node
        for node in tree.body
        if (
            isinstance(node, ast.ClassDef)
            and node.name == "AcquisitionEvidenceLifecycleStore"
        )
    )
    discovered: dict[str, tuple[str, ...]] = {}
    for method in store_class.body:
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
                and isinstance(call.args[0], ast.Constant)
                and isinstance(call.args[0].value, str)
                and str(call.args[0].value).startswith(("object.", "publish."))
            )
        )
        if calls:
            discovered[method.name] = tuple(
                str(call.args[0].value)
                for call in sorted(
                    calls, key=lambda item: (item.lineno, item.col_offset)
                )
            )
    return discovered


def _database_snapshot(path: Path) -> dict[str, object]:
    with closing(sqlite3.connect(path)) as database:
        integrity = str(database.execute("PRAGMA integrity_check").fetchone()[0])
        producer_rows = tuple(database.execute(
            "SELECT producer_receipt_sha256,stream_sequence,global_sequence,"
            "proof_sha256,content_sha256,size_bytes FROM producer_receipts "
            "ORDER BY global_sequence"
        ).fetchall())
        outbox_rows = tuple(database.execute(
            "SELECT event_id,event_sha256,producer_receipt_sha256,consumer_id,"
            "stream_id,stream_sequence,state,attempt_count "
            "FROM completion_outbox ORDER BY stream_id,stream_sequence"
        ).fetchall())
        hot_rows = tuple(database.execute(
            "SELECT content_sha256,size_bytes,filename,state FROM hot_objects "
            "ORDER BY content_sha256"
        ).fetchall())
        stream_head = database.execute(
            "SELECT consumer_id,last_sequence FROM stream_heads WHERE stream_id=?",
            (STREAM_ID,),
        ).fetchone()
        next_global = int(database.execute(
            "SELECT next_global_sequence FROM lifecycle_heads WHERE singleton=1"
        ).fetchone()[0])
    return {
        "hot_rows": hot_rows,
        "integrity": integrity,
        "next_global": next_global,
        "outbox_rows": outbox_rows,
        "producer_rows": producer_rows,
        "stream_head": stream_head,
    }


def _directory_names(path: Path) -> tuple[str, ...]:
    return tuple(sorted(item.name for item in path.iterdir()))


def test_acquisition_evidence_publication_fault_inventory_is_exact() -> None:
    discovered = _fault_group_inventory()
    assert discovered == _FAULT_GROUPS
    inventory = tuple(
        checkpoint for group in discovered.values() for checkpoint in group
    )
    cases = tuple(checkpoint for checkpoint, _phase, _staged in _CRASH_CASES)
    assert cases == inventory
    assert len(cases) == len(set(cases)) == 9


@pytest.mark.parametrize(
    ("checkpoint", "phase_at_crash", "staged_at_crash"),
    _CRASH_CASES,
    ids=(
        "object-after-write-old",
        "object-after-fsync-old",
        "object-after-replace-old",
        "publish-after-hot-object-old",
        "publish-after-producer-insert-old",
        "publish-before-producer-commit-old",
        "publish-after-producer-commit-new",
        "publish-before-ready-commit-producer-new",
        "publish-after-ready-commit-new",
    ),
)
def test_acquisition_evidence_publication_os_exit73_reopens_and_replays_once(
    tmp_path: Path,
    checkpoint: str,
    phase_at_crash: str,
    staged_at_crash: bool,
) -> None:
    lifecycle_root = (tmp_path / checkpoint.replace(".", "-")).resolve()
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "worker_acquisition_evidence_lifecycle_crash_child.py"
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
        [sys.executable, str(helper), str(lifecycle_root), checkpoint],
        cwd=repository,
        env=environment,
        capture_output=True,
        check=False,
        timeout=30.0,
    )
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    at_crash = _database_snapshot(
        lifecycle_root / "acquisition_evidence.sqlite3"
    )
    assert at_crash["integrity"] == "ok"
    expected_producers = int(phase_at_crash != "old")
    expected_outbox = int(phase_at_crash == "ready-new")
    assert len(at_crash["producer_rows"]) == expected_producers
    assert len(at_crash["outbox_rows"]) == expected_outbox
    assert len(at_crash["hot_rows"]) == expected_producers
    assert at_crash["next_global"] == 1 + expected_producers
    assert (at_crash["stream_head"] is not None) is bool(expected_producers)

    crash_names = _directory_names(lifecycle_root / "hot_objects")
    if staged_at_crash:
        assert len(crash_names) == 1
        assert re.fullmatch(
            rf"\.{EXPECTED_CONTENT_SHA256}\.[0-9a-f]{{16}}", crash_names[0]
        )
    else:
        assert crash_names == (f"{EXPECTED_CONTENT_SHA256}.raw",)

    reopened = _store(lifecycle_root)
    assert _directory_names(reopened.hot_directory) == (
        f"{EXPECTED_CONTENT_SHA256}.raw",
    )
    before_retry = _database_snapshot(reopened.path)
    assert len(before_retry["producer_rows"]) == expected_producers
    assert len(before_retry["outbox_rows"]) == expected_outbox

    receipt = _publish(reopened)
    assert receipt.as_dict() == EXPECTED_RECEIPT
    lookup = reopened.lookup_sequence(STREAM_ID, 1)
    assert lookup.status == "hot"
    assert lookup.event_id == EXPECTED_EVENT_ID
    assert lookup.producer_receipt_sha256 == EXPECTED_PRODUCER_SHA256
    assert lookup.proof == PROOF
    assert lookup.payload == PAYLOAD
    assert reopened.lookup_sequence(STREAM_ID, 2).status == "unknown"

    stable = _database_snapshot(reopened.path)
    assert stable == {
        "hot_rows": ((
            EXPECTED_CONTENT_SHA256,
            len(PAYLOAD),
            f"{EXPECTED_CONTENT_SHA256}.raw",
            "active",
        ),),
        "integrity": "ok",
        "next_global": 2,
        "outbox_rows": ((
            EXPECTED_EVENT_ID,
            EXPECTED_EVENT_SHA256,
            EXPECTED_PRODUCER_SHA256,
            CONSUMER_ID,
            STREAM_ID,
            1,
            "ready",
            0,
        ),),
        "producer_rows": ((
            EXPECTED_PRODUCER_SHA256,
            1,
            1,
            EXPECTED_PROOF_SHA256,
            EXPECTED_CONTENT_SHA256,
            len(PAYLOAD),
        ),),
        "stream_head": (CONSUMER_ID, 1),
    }
    assert _directory_names(reopened.hot_directory) == (
        f"{EXPECTED_CONTENT_SHA256}.raw",
    )
    hot_object = reopened.hot_directory / f"{EXPECTED_CONTENT_SHA256}.raw"
    assert hot_object.read_bytes() == PAYLOAD
    assert _directory_names(reopened.archive_object_directory) == ()
    assert _directory_names(reopened.archive_segment_directory) == ()
    audit = reopened.audit_archive()
    assert audit["healthy"] is True
    assert audit["hot_completions"] == 1
    assert audit["hot_objects"] == 1
    assert audit["archived_completions"] == 0

    replay = _publish(reopened)
    assert replay == receipt
    assert _database_snapshot(reopened.path) == stable
    assert _directory_names(reopened.hot_directory) == (
        f"{EXPECTED_CONTENT_SHA256}.raw",
    )
    assert reopened.audit_archive() == audit
