from __future__ import annotations

import ast
from collections import Counter
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
CONSUMER_ID = "decoder-v1"
STREAM_ID = "radar.maintenance.raw"
DESIRED_STATE_SHA256 = "1" * 64
AUTHORITY_SHA256 = "2" * 64
PRODUCER_SCHEMA = "stv3-worker-acquisition-producer-receipt-v1"
EVENT_SCHEMA = "stv3-worker-acquisition-completion-event-v1"
SUPERSESSION_SCHEMA = "stv3-worker-acquisition-supersession-v1"

_NATIVE_FAULT_GROUPS = {
    "supersede": (
        "supersede.before_commit",
        "supersede.after_commit",
    ),
    "_publish_segment_file": (
        "archive.after_segment_write",
        "archive.after_segment_fsync",
        "archive.after_segment_replace",
    ),
    "rotate_archive": (
        "archive.after_object",
        "archive.after_segment_publish",
        "archive.before_manifest_commit",
        "archive.after_manifest_commit",
    ),
    "resume_pending_prunes": (
        "prune.after_hot_object_unlink",
        "prune.before_file_phase_commit",
        "prune.after_file_phase_commit",
        "prune.before_phase_{phase}_commit",
        "prune.after_phase_{phase}_commit",
    ),
}

_SUPERSEDE_CASES = (
    ("supersede", "supersede.before_commit", 1, "old"),
    ("supersede", "supersede.after_commit", 1, "new"),
)
_ARCHIVE_CASES = (
    ("archive", "archive.after_object", 1, "old-object"),
    ("archive", "archive.after_segment_write", 1, "old-segment-stage"),
    ("archive", "archive.after_segment_fsync", 1, "old-segment-stage"),
    ("archive", "archive.after_segment_replace", 1, "old-segment-final"),
    ("archive", "archive.after_segment_publish", 1, "old-segment-final"),
    ("archive", "archive.before_manifest_commit", 1, "old-segment-final"),
    ("archive", "archive.after_manifest_commit", 1, "new-manifest"),
)
_PRUNE_CASES = (
    ("prune", "prune.before_phase_0_commit", 1, "phase-0-old"),
    ("prune", "prune.after_phase_0_commit", 1, "phase-1-new"),
    ("prune", "prune.before_phase_1_commit", 1, "phase-1-old"),
    ("prune", "prune.after_phase_1_commit", 1, "phase-2-new"),
    ("prune", "prune.before_phase_2_commit", 1, "phase-2-old"),
    ("prune", "prune.after_phase_2_commit", 1, "phase-3-new"),
    ("prune", "prune.before_phase_3_commit", 1, "phase-3-old"),
    ("prune", "prune.after_phase_3_commit", 1, "phase-4-new"),
    ("prune", "prune.after_hot_object_unlink", 1, "phase-4-first-unlink"),
    ("prune", "prune.after_hot_object_unlink", 2, "phase-4-last-unlink"),
    ("prune", "prune.before_file_phase_commit", 1, "phase-4-files-old"),
    ("prune", "prune.after_file_phase_commit", 1, "phase-5-files-new"),
    ("prune", "prune.before_phase_5_commit", 1, "phase-5-old"),
    ("prune", "prune.after_phase_5_commit", 1, "phase-6-new"),
)
_MAINTENANCE_CASES = _SUPERSEDE_CASES + _ARCHIVE_CASES + _PRUNE_CASES
_CASE_IDS = (
    "supersede-before-commit-old",
    "supersede-after-commit-new",
    "archive-after-object-old",
    "archive-after-segment-write-stage-old",
    "archive-after-segment-fsync-stage-old",
    "archive-after-segment-replace-file-old",
    "archive-after-segment-publish-file-old",
    "archive-before-manifest-commit-old",
    "archive-after-manifest-commit-new",
    "prune-before-phase-0-old",
    "prune-after-phase-0-phase-1-new",
    "prune-before-phase-1-old",
    "prune-after-phase-1-phase-2-new",
    "prune-before-phase-2-old",
    "prune-after-phase-2-phase-3-new",
    "prune-before-phase-3-old",
    "prune-after-phase-3-phase-4-new",
    "prune-phase-4-after-first-object-unlink",
    "prune-phase-4-after-last-object-unlink",
    "prune-phase-4-before-file-commit-old",
    "prune-phase-4-after-file-commit-phase-5-new",
    "prune-before-phase-5-old",
    "prune-after-phase-5-phase-6-new",
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


def _expected_receipt(sequence: int) -> dict[str, object]:
    payload = f"maintenance-raw-{sequence}".encode("ascii")
    proof = {"schema": "maintenance-proof-v1", "sequence": sequence}
    proof_sha = hashlib.sha256(_canonical(proof)).hexdigest()
    content_sha = hashlib.sha256(payload).hexdigest()
    core = {
        "acquisition_job_id": f"maintenance-job-{sequence}",
        "authority_sha256": AUTHORITY_SHA256,
        "consumer_id": CONSUMER_ID,
        "content_sha256": content_sha,
        "desired_state_sha256": DESIRED_STATE_SHA256,
        "proof_sha256": proof_sha,
        "schema": PRODUCER_SCHEMA,
        "size_bytes": len(payload),
        "stream_id": STREAM_ID,
    }
    producer_sha = _digest({
        **core,
        "created_at": NOW,
        "global_sequence": sequence,
        "stream_sequence": sequence,
    })
    event_id = "completion-" + hashlib.sha256(
        (EVENT_SCHEMA + producer_sha).encode("ascii")
    ).hexdigest()
    event_sha = _digest({
        "consumer_id": CONSUMER_ID,
        "producer_receipt_sha256": producer_sha,
        "schema": EVENT_SCHEMA,
        "stream_id": STREAM_ID,
        "stream_sequence": sequence,
    })
    return {
        "acquisition_job_id": f"maintenance-job-{sequence}",
        "authority_sha256": AUTHORITY_SHA256,
        "consumer_id": CONSUMER_ID,
        "content_sha256": content_sha,
        "created_at": NOW,
        "desired_state_sha256": DESIRED_STATE_SHA256,
        "event_id": event_id,
        "event_sha256": event_sha,
        "global_sequence": sequence,
        "producer_receipt_sha256": producer_sha,
        "proof_sha256": proof_sha,
        "ready_at": NOW,
        "schema": PRODUCER_SCHEMA,
        "size_bytes": len(payload),
        "stream_id": STREAM_ID,
        "stream_sequence": sequence,
    }


EXPECTED_RECEIPTS = (_expected_receipt(1), _expected_receipt(2))
EXPECTED_PAYLOADS = (b"maintenance-raw-1", b"maintenance-raw-2")
EXPECTED_PROOFS = (
    {"schema": "maintenance-proof-v1", "sequence": 1},
    {"schema": "maintenance-proof-v1", "sequence": 2},
)
EXPECTED_CONTENT_NAMES_BY_SEQUENCE = tuple(
    f"{receipt['content_sha256']}.raw" for receipt in EXPECTED_RECEIPTS
)
EXPECTED_CONTENT_NAMES = tuple(sorted(EXPECTED_CONTENT_NAMES_BY_SEQUENCE))
EXPECTED_SUPERSESSION_SHA256 = _digest({
    "event_id": EXPECTED_RECEIPTS[0]["event_id"],
    "event_sha256": EXPECTED_RECEIPTS[0]["event_sha256"],
    "replacement_event_id": EXPECTED_RECEIPTS[1]["event_id"],
    "schema": SUPERSESSION_SCHEMA,
})


def _store(root: Path) -> subject.AcquisitionEvidenceLifecycleStore:
    return subject.AcquisitionEvidenceLifecycleStore(
        root,
        clock=lambda: NOW,
        token_factory=lambda: "b" * 64,
        directory_fsync=lambda _path: None,
    )


def _publish(
    store: subject.AcquisitionEvidenceLifecycleStore,
    sequence: int,
) -> subject.CompletionProducerReceipt:
    return store.publish(
        acquisition_job_id=f"maintenance-job-{sequence}",
        consumer_id=CONSUMER_ID,
        stream_id=STREAM_ID,
        desired_state_sha256=DESIRED_STATE_SHA256,
        authority_sha256=AUTHORITY_SHA256,
        proof=EXPECTED_PROOFS[sequence - 1],
        payload=EXPECTED_PAYLOADS[sequence - 1],
    )


def _fault_text(argument: ast.expr) -> str | None:
    if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
        return str(argument.value)
    if not isinstance(argument, ast.JoinedStr):
        return None
    parts: list[str] = []
    for value in argument.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            parts.append(str(value.value))
        elif (
            isinstance(value, ast.FormattedValue)
            and isinstance(value.value, ast.Name)
            and value.value.id == "phase"
        ):
            parts.append("{phase}")
        else:
            return None
    return "".join(parts)


def _maintenance_fault_inventory() -> dict[str, tuple[str, ...]]:
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
                and (_fault_text(call.args[0]) or "").startswith(
                    ("supersede.", "archive.", "prune.")
                )
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


def _snapshot(root: Path) -> dict[str, object]:
    path = root / "acquisition_evidence.sqlite3"
    with closing(sqlite3.connect(path)) as database:
        return {
            "integrity": str(
                database.execute("PRAGMA integrity_check").fetchone()[0]
            ),
            "producers": tuple(database.execute(
                "SELECT producer_receipt_sha256,acquisition_job_id,"
                "stream_sequence,global_sequence,proof_sha256,content_sha256,"
                "size_bytes FROM producer_receipts ORDER BY global_sequence"
            ).fetchall()),
            "outbox": tuple(database.execute(
                "SELECT event_id,event_sha256,producer_receipt_sha256,"
                "stream_sequence,state,attempt_count,superseded_by_event_id,"
                "supersession_sha256 FROM completion_outbox "
                "ORDER BY stream_sequence"
            ).fetchall()),
            "consumptions": tuple(database.execute(
                "SELECT event_id,producer_receipt_sha256,stream_sequence,"
                "output_sha256 FROM consumption_receipts ORDER BY stream_sequence"
            ).fetchall()),
            "cursors": tuple(database.execute(
                "SELECT consumer_id,stream_id,last_sequence,last_event_sha256 "
                "FROM consumer_cursors ORDER BY consumer_id,stream_id"
            ).fetchall()),
            "hot_objects": tuple(database.execute(
                "SELECT content_sha256,size_bytes,filename,state FROM hot_objects "
                "ORDER BY content_sha256"
            ).fetchall()),
            "archive_objects": tuple(database.execute(
                "SELECT content_sha256,size_bytes,filename FROM archive_objects "
                "ORDER BY content_sha256"
            ).fetchall()),
            "archive_segments": tuple(database.execute(
                "SELECT segment_id,completion_count,file_sha256,filename "
                "FROM archive_segments ORDER BY segment_ordinal"
            ).fetchall()),
            "archive_index": tuple(database.execute(
                "SELECT producer_receipt_sha256,event_id,stream_sequence,"
                "content_sha256,segment_id FROM archive_index "
                "ORDER BY global_sequence"
            ).fetchall()),
            "prune_plans": tuple(database.execute(
                "SELECT segment_id,phase,completed_at IS NOT NULL "
                "FROM prune_plans ORDER BY rowid"
            ).fetchall()),
            "head": tuple(database.execute(
                "SELECT next_global_sequence,segment_count,"
                "archived_completion_count FROM lifecycle_heads WHERE singleton=1"
            ).fetchone()),
            "hot_names": _directory_names(root / "hot_objects"),
            "archive_object_names": _directory_names(
                root / "archive" / "objects"
            ),
            "archive_segment_names": _directory_names(
                root / "archive" / "segments"
            ),
        }


def _directory_names(path: Path) -> tuple[str, ...]:
    return tuple(sorted(item.name for item in path.iterdir()))


def _expected_producers(count: int) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            receipt["producer_receipt_sha256"],
            receipt["acquisition_job_id"],
            sequence,
            sequence,
            receipt["proof_sha256"],
            receipt["content_sha256"],
            receipt["size_bytes"],
        )
        for sequence, receipt in enumerate(EXPECTED_RECEIPTS[:count], 1)
    )


def _expected_outbox(
    count: int,
    *,
    first_superseded: bool = False,
) -> tuple[tuple[object, ...], ...]:
    rows = []
    for sequence, receipt in enumerate(EXPECTED_RECEIPTS[:count], 1):
        superseded = first_superseded and sequence == 1
        rows.append((
            receipt["event_id"],
            receipt["event_sha256"],
            receipt["producer_receipt_sha256"],
            sequence,
            "superseded" if superseded else "ready",
            0,
            EXPECTED_RECEIPTS[1]["event_id"] if superseded else None,
            EXPECTED_SUPERSESSION_SHA256 if superseded else None,
        ))
    return tuple(rows)


def _expected_consumed_outbox(count: int) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            receipt["event_id"],
            receipt["event_sha256"],
            receipt["producer_receipt_sha256"],
            sequence,
            "consumed",
            1,
            None,
            None,
        )
        for sequence, receipt in enumerate(EXPECTED_RECEIPTS[:count], 1)
    )


def _expected_consumptions(count: int) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            receipt["event_id"],
            receipt["producer_receipt_sha256"],
            sequence,
            str(sequence + 2) * 64,
        )
        for sequence, receipt in enumerate(EXPECTED_RECEIPTS[:count], 1)
    )


def _expected_hot_objects(
    count: int,
    state: str = "active",
) -> tuple[tuple[object, ...], ...]:
    return tuple(sorted(
        (
            receipt["content_sha256"],
            receipt["size_bytes"],
            f"{receipt['content_sha256']}.raw",
            state,
        )
        for receipt in EXPECTED_RECEIPTS[:count]
    ))


def _assert_segment_names(names: tuple[str, ...], kind: str) -> None:
    if kind == "none":
        assert names == ()
        return
    assert len(names) == 1
    pattern = (
        r"\.[0-9a-f]{64}\.tmp"
        if kind == "stage"
        else r"[0-9a-f]{64}\.sealed\.json"
    )
    assert re.fullmatch(pattern, names[0])


def _assert_supersede_crash(snapshot: dict[str, object], phase: str) -> None:
    committed = phase == "new"
    assert snapshot["integrity"] == "ok"
    assert snapshot["producers"] == _expected_producers(2)
    assert snapshot["outbox"] == _expected_outbox(
        2, first_superseded=committed
    )
    assert snapshot["consumptions"] == ()
    assert snapshot["cursors"] == (
        ((CONSUMER_ID, STREAM_ID, 1, EXPECTED_RECEIPTS[0]["event_sha256"]),)
        if committed
        else ()
    )
    assert snapshot["hot_objects"] == _expected_hot_objects(2)
    assert snapshot["head"] == (3, 0, 0)
    assert snapshot["hot_names"] == EXPECTED_CONTENT_NAMES
    assert snapshot["archive_objects"] == ()
    assert snapshot["archive_segments"] == ()
    assert snapshot["archive_index"] == ()
    assert snapshot["prune_plans"] == ()
    assert snapshot["archive_object_names"] == ()
    assert snapshot["archive_segment_names"] == ()


def _assert_archive_crash(snapshot: dict[str, object], phase: str) -> None:
    manifest = phase == "new-manifest"
    assert snapshot["integrity"] == "ok"
    assert snapshot["producers"] == _expected_producers(1)
    assert snapshot["outbox"] == _expected_consumed_outbox(1)
    assert snapshot["consumptions"] == _expected_consumptions(1)
    assert snapshot["cursors"] == ((
        CONSUMER_ID,
        STREAM_ID,
        1,
        EXPECTED_RECEIPTS[0]["event_sha256"],
    ),)
    assert snapshot["hot_objects"] == _expected_hot_objects(1)
    sequence_one_name = EXPECTED_CONTENT_NAMES_BY_SEQUENCE[0]
    assert snapshot["hot_names"] == (sequence_one_name,)
    assert snapshot["archive_object_names"] == (sequence_one_name,)
    expected_archive_objects = tuple(
        tuple(row[:3]) for row in _expected_hot_objects(1)
    )
    assert snapshot["archive_objects"] == (
        expected_archive_objects if manifest else ()
    )
    if phase == "old-object":
        segment_kind = "none"
    elif phase == "old-segment-stage":
        segment_kind = "stage"
    else:
        segment_kind = "final"
    _assert_segment_names(snapshot["archive_segment_names"], segment_kind)
    assert len(snapshot["archive_segments"]) == int(manifest)
    assert len(snapshot["archive_index"]) == int(manifest)
    assert len(snapshot["prune_plans"]) == int(manifest)
    if manifest:
        segment = snapshot["archive_segments"][0]
        assert segment[1] == 1
        assert segment[3] == snapshot["archive_segment_names"][0]
        assert snapshot["archive_index"][0] == (
            EXPECTED_RECEIPTS[0]["producer_receipt_sha256"],
            EXPECTED_RECEIPTS[0]["event_id"],
            1,
            EXPECTED_RECEIPTS[0]["content_sha256"],
            segment[0],
        )
        assert snapshot["prune_plans"] == ((segment[0], 0, 0),)
    assert snapshot["head"] == ((2, 1, 1) if manifest else (2, 0, 0))


_PRUNE_EXPECTATIONS = {
    ("prune.before_phase_0_commit", 1): (0, 2, 2, 2, "active", 2, False),
    ("prune.after_phase_0_commit", 1): (1, 0, 2, 2, "active", 2, False),
    ("prune.before_phase_1_commit", 1): (1, 0, 2, 2, "active", 2, False),
    ("prune.after_phase_1_commit", 1): (2, 0, 0, 2, "active", 2, False),
    ("prune.before_phase_2_commit", 1): (2, 0, 0, 2, "active", 2, False),
    ("prune.after_phase_2_commit", 1): (3, 0, 0, 0, "active", 2, False),
    ("prune.before_phase_3_commit", 1): (3, 0, 0, 0, "active", 2, False),
    ("prune.after_phase_3_commit", 1): (4, 0, 0, 0, "pruning", 2, False),
    ("prune.after_hot_object_unlink", 1): (
        4, 0, 0, 0, "pruning", 1, False
    ),
    ("prune.after_hot_object_unlink", 2): (
        4, 0, 0, 0, "pruning", 0, False
    ),
    ("prune.before_file_phase_commit", 1): (
        4, 0, 0, 0, "pruning", 0, False
    ),
    ("prune.after_file_phase_commit", 1): (
        5, 0, 0, 0, "pruning", 0, False
    ),
    ("prune.before_phase_5_commit", 1): (
        5, 0, 0, 0, "pruning", 0, False
    ),
    ("prune.after_phase_5_commit", 1): (6, 0, 0, 0, None, 0, True),
}


def _assert_prune_crash(
    snapshot: dict[str, object],
    checkpoint: str,
    occurrence: int,
) -> None:
    (
        phase,
        consumption_count,
        outbox_count,
        producer_count,
        hot_state,
        hot_file_count,
        completed,
    ) = _PRUNE_EXPECTATIONS[(checkpoint, occurrence)]
    assert snapshot["integrity"] == "ok"
    assert snapshot["producers"] == _expected_producers(producer_count)
    assert snapshot["outbox"] == _expected_consumed_outbox(outbox_count)
    assert snapshot["consumptions"] == _expected_consumptions(consumption_count)
    assert snapshot["cursors"] == ((
        CONSUMER_ID,
        STREAM_ID,
        2,
        EXPECTED_RECEIPTS[1]["event_sha256"],
    ),)
    assert snapshot["hot_objects"] == (
        () if hot_state is None else _expected_hot_objects(2, hot_state)
    )
    if hot_file_count == 2:
        expected_hot_names = EXPECTED_CONTENT_NAMES
    elif hot_file_count == 1:
        expected_hot_names = (EXPECTED_CONTENT_NAMES[1],)
    else:
        expected_hot_names = ()
    assert snapshot["hot_names"] == expected_hot_names
    expected_archive_objects = tuple(
        tuple(row[:3]) for row in _expected_hot_objects(2)
    )
    assert snapshot["archive_objects"] == expected_archive_objects
    assert snapshot["archive_object_names"] == EXPECTED_CONTENT_NAMES
    _assert_segment_names(snapshot["archive_segment_names"], "final")
    assert len(snapshot["archive_segments"]) == 1
    segment = snapshot["archive_segments"][0]
    assert segment[1] == 2
    assert segment[3] == snapshot["archive_segment_names"][0]
    assert snapshot["archive_index"] == tuple(
        (
            receipt["producer_receipt_sha256"],
            receipt["event_id"],
            sequence,
            receipt["content_sha256"],
            segment[0],
        )
        for sequence, receipt in enumerate(EXPECTED_RECEIPTS, 1)
    )
    assert snapshot["prune_plans"] == ((segment[0], phase, int(completed)),)
    assert snapshot["head"] == (3, 1, 2)


def _database_part(snapshot: dict[str, object]) -> dict[str, object]:
    return {
        key: value
        for key, value in snapshot.items()
        if key not in {
            "hot_names",
            "archive_object_names",
            "archive_segment_names",
        }
    }


def _assert_final_archive(
    store: subject.AcquisitionEvidenceLifecycleStore,
    completion_count: int,
) -> tuple[dict[str, object], dict[str, object]]:
    snapshot = _snapshot(store.root)
    assert snapshot["integrity"] == "ok"
    assert snapshot["producers"] == ()
    assert snapshot["outbox"] == ()
    assert snapshot["consumptions"] == ()
    assert snapshot["hot_objects"] == ()
    assert snapshot["hot_names"] == ()
    assert snapshot["head"] == (completion_count + 1, 1, completion_count)
    assert len(snapshot["archive_objects"]) == completion_count
    expected_archive_names = tuple(sorted(
        EXPECTED_CONTENT_NAMES_BY_SEQUENCE[:completion_count]
    ))
    assert snapshot["archive_object_names"] == expected_archive_names
    assert len(snapshot["archive_segments"]) == 1
    segment = snapshot["archive_segments"][0]
    assert segment[1] == completion_count
    assert snapshot["archive_segment_names"] == (segment[3],)
    segment_path = store.archive_segment_directory / str(segment[3])
    assert hashlib.sha256(segment_path.read_bytes()).hexdigest() == segment[2]
    assert snapshot["prune_plans"] == ((segment[0], 6, 1),)
    for sequence in range(1, completion_count + 1):
        receipt = EXPECTED_RECEIPTS[sequence - 1]
        payload = EXPECTED_PAYLOADS[sequence - 1]
        object_path = store.archive_object_directory / (
            f"{receipt['content_sha256']}.raw"
        )
        assert object_path.read_bytes() == payload
        lookup = store.lookup_sequence(STREAM_ID, sequence)
        assert lookup.status == "archived"
        assert lookup.event_id == receipt["event_id"]
        assert lookup.producer_receipt_sha256 == receipt[
            "producer_receipt_sha256"
        ]
        assert lookup.proof == EXPECTED_PROOFS[sequence - 1]
        assert lookup.payload == payload
        window = store.lookup_sequence(
            STREAM_ID, sequence, include_archive=False
        )
        assert window.status == "window_expired"
        assert window.event_id == receipt["event_id"]
    assert store.lookup_sequence(
        STREAM_ID, completion_count + 1
    ).status == "unknown"
    audit = store.audit_archive()
    assert audit["healthy"] is True
    assert audit["hot_completions"] == 0
    assert audit["hot_objects"] == 0
    assert audit["archived_completions"] == completion_count
    assert audit["archive_objects"] == completion_count
    assert audit["segment_count"] == 1
    return snapshot, audit


def test_maintenance_fault_inventory_expands_to_exact_23_cases() -> None:
    discovered = _maintenance_fault_inventory()
    assert discovered == _NATIVE_FAULT_GROUPS
    assert len(_SUPERSEDE_CASES) == 2
    assert len(_ARCHIVE_CASES) == 7
    assert len(_PRUNE_CASES) == 14
    assert len(_MAINTENANCE_CASES) == len(_CASE_IDS) == 23
    supersede_points = tuple(case[1] for case in _SUPERSEDE_CASES)
    assert supersede_points == discovered["supersede"]
    archive_points = {case[1] for case in _ARCHIVE_CASES}
    assert archive_points == {
        *discovered["_publish_segment_file"],
        *discovered["rotate_archive"],
    }
    dynamic_patterns = discovered["resume_pending_prunes"][-2:]
    dynamic_points = {
        pattern.format(phase=phase)
        for phase in (0, 1, 2, 3, 5)
        for pattern in dynamic_patterns
    }
    file_points = discovered["resume_pending_prunes"][:3]
    expected_counts = Counter({point: 1 for point in dynamic_points})
    expected_counts.update({point: 1 for point in file_points})
    expected_counts["prune.after_hot_object_unlink"] = 2
    assert Counter(case[1] for case in _PRUNE_CASES) == expected_counts


@pytest.mark.parametrize(
    ("operation", "checkpoint", "occurrence", "phase_at_crash"),
    _MAINTENANCE_CASES,
    ids=_CASE_IDS,
)
def test_maintenance_os_exit73_reopens_resumes_and_replays_inertly(
    tmp_path: Path,
    operation: str,
    checkpoint: str,
    occurrence: int,
    phase_at_crash: str,
) -> None:
    lifecycle_root = (
        tmp_path
        / f"{checkpoint.replace('.', '-')}-{occurrence}"
    ).resolve()
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "worker_acquisition_evidence_lifecycle_maintenance_crash_child.py"
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
        [
            sys.executable,
            str(helper),
            str(lifecycle_root),
            operation,
            checkpoint,
            str(occurrence),
        ],
        cwd=repository,
        env=environment,
        capture_output=True,
        check=False,
        timeout=30.0,
    )
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    at_crash = _snapshot(lifecycle_root)
    if operation == "supersede":
        _assert_supersede_crash(at_crash, phase_at_crash)
    elif operation == "archive":
        _assert_archive_crash(at_crash, phase_at_crash)
    else:
        _assert_prune_crash(at_crash, checkpoint, occurrence)

    reopened = _store(lifecycle_root)
    after_reopen = _snapshot(lifecycle_root)
    assert _database_part(after_reopen) == _database_part(at_crash)
    assert after_reopen["hot_names"] == at_crash["hot_names"]
    assert after_reopen["archive_object_names"] == at_crash[
        "archive_object_names"
    ]
    if phase_at_crash == "old-segment-stage":
        _assert_segment_names(after_reopen["archive_segment_names"], "final")
    else:
        assert after_reopen["archive_segment_names"] == at_crash[
            "archive_segment_names"
        ]

    if operation == "supersede":
        recovered = reopened.supersede(
            str(EXPECTED_RECEIPTS[0]["event_id"]),
            expected_event_sha256=str(EXPECTED_RECEIPTS[0]["event_sha256"]),
            replacement_event_id=str(EXPECTED_RECEIPTS[1]["event_id"]),
            now=NOW + 10.0,
        )
        assert recovered == EXPECTED_SUPERSESSION_SHA256
        final = _snapshot(lifecycle_root)
        _assert_supersede_crash(final, "new")
        for sequence in (1, 2):
            lookup = reopened.lookup_sequence(STREAM_ID, sequence)
            assert lookup.status == "hot"
            assert lookup.event_id == EXPECTED_RECEIPTS[sequence - 1]["event_id"]
            assert lookup.proof == EXPECTED_PROOFS[sequence - 1]
            assert lookup.payload == EXPECTED_PAYLOADS[sequence - 1]
        audit = reopened.audit_archive()
        assert audit["healthy"] is True
        assert audit["hot_completions"] == 2
        assert audit["hot_objects"] == 2
        replay = reopened.supersede(
            str(EXPECTED_RECEIPTS[0]["event_id"]),
            expected_event_sha256=str(EXPECTED_RECEIPTS[0]["event_sha256"]),
            replacement_event_id=str(EXPECTED_RECEIPTS[1]["event_id"]),
            now=NOW + 10.0,
        )
        assert replay == recovered
        for sequence in (1, 2):
            assert _publish(reopened, sequence).as_dict() == EXPECTED_RECEIPTS[
                sequence - 1
            ]
        assert _snapshot(lifecycle_root) == final
        assert reopened.audit_archive() == audit
        return

    if operation == "archive":
        rotation = reopened.rotate_archive(
            policy=subject.EvidenceRetentionPolicy(),
            now=NOW + 10.0,
        )
        expected_status = "idle" if phase_at_crash == "new-manifest" else "archived"
        assert rotation.status == expected_status
        completion_count = 1
    else:
        resumed = reopened.resume_pending_prunes()
        assert resumed == int(phase_at_crash != "phase-6-new")
        completion_count = 2

    final, audit = _assert_final_archive(reopened, completion_count)
    if operation == "archive":
        inert = reopened.rotate_archive(
            policy=subject.EvidenceRetentionPolicy(),
            now=NOW + 10.0,
        )
        assert inert.status == "idle"
        assert inert.archived_completions == 0
    else:
        assert reopened.resume_pending_prunes() == 0
    for sequence in range(1, completion_count + 1):
        assert _publish(reopened, sequence).as_dict() == EXPECTED_RECEIPTS[
            sequence - 1
        ]
    assert _snapshot(lifecycle_root) == final
    assert reopened.audit_archive() == audit
