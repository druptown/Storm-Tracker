"""Tests for the isolated exact-byte RawArtifactStore v2 primitive."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import hashlib
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

from storm_tracker_worker.tracking_core.engine.raw_artifact_store import (
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactConflictError,
    RawArtifactMember,
    RawArtifactStore,
)


def _store(store: RawArtifactStore, **overrides):
    values = {
        "provider_id": "kmi",
        "upstream_record_id": "radar/20260729T190000Z.png",
        "payload": b"\x89PNG\r\n\x1a\n\x00exact-provider-bytes",
        "fetched_at": 1_800_000_010.0,
        "nominal_at": 1_800_000_000.0,
        "media_type": "image/png",
        "parser_version": "kmi-parser-v1",
    }
    values.update(overrides)
    return store.store(**values)


def _set_members(*, reverse: bool = False):
    members = (
        RawArtifactMember(
            member_id="catalog",
            role="authoritative-catalog",
            upstream_record_id="rainviewer/catalog/revision-a",
            payload=b'{"generated":1800000000,"frames":["frame-a"]}',
            media_type="application/json",
            parser_version="rainviewer-parser-v2",
        ),
        RawArtifactMember(
            member_id="frame",
            role="authoritative-radar-frame",
            upstream_record_id="rainviewer/frame/frame-a",
            payload=b"\x89PNG\r\n\x1a\nexact-radar-tile",
            media_type="image/png",
            parser_version="rainviewer-parser-v2",
        ),
    )
    return tuple(reversed(members)) if reverse else members


def _store_set(store: RawArtifactStore, **overrides):
    values = {
        "provider_id": "rainviewer",
        "upstream_set_id": "past/frame-a/revision-a",
        "members": _set_members(),
        "fetched_at": 1_800_000_010.0,
        "nominal_at": 1_800_000_000.0,
        "provenance_sha256": "a" * 64,
    }
    values.update(overrides)
    return store.store_set(**values)


def _install_payload_poison_trigger(path: Path) -> None:
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            """
            CREATE TRIGGER raw_artifact_payload_poison
            AFTER INSERT ON raw_artifact_receipts
            BEGIN
                UPDATE raw_artifact_blobs
                SET payload=zeroblob(size_bytes)
                WHERE content_sha256=NEW.content_sha256;
            END
            """
        )


def test_exact_upstream_bytes_round_trip(tmp_path):
    path = tmp_path / "raw-artifacts.sqlite3"
    store = RawArtifactStore(path)
    assert store.initialize() == {
        "schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
        "blob_count": 0,
        "receipt_count": 0,
        "artifact_set_count": 0,
        "stored_bytes": 0,
    }

    receipt = _store(store)

    assert receipt.content_sha256 == hashlib.sha256(
        b"\x89PNG\r\n\x1a\n\x00exact-provider-bytes"
    ).hexdigest()
    assert receipt.size_bytes == 29
    assert store.load_payload(receipt.receipt_id) == (
        b"\x89PNG\r\n\x1a\n\x00exact-provider-bytes"
    )
    assert store.load_receipt(receipt.receipt_id) == receipt
    assert store.integrity_check()["healthy"] is True


def test_identical_upstream_receipt_is_idempotent(tmp_path):
    store = RawArtifactStore(tmp_path / "raw-artifacts.sqlite3")
    store.initialize()

    first = _store(store)
    second = _store(
        store,
        fetched_at=1_800_000_020.0,
    )

    assert second == first
    assert store.stats()["blob_count"] == 1
    assert store.stats()["receipt_count"] == 1


def test_same_upstream_identity_with_other_bytes_fails_closed(tmp_path):
    store = RawArtifactStore(tmp_path / "raw-artifacts.sqlite3")
    store.initialize()
    original = _store(store)

    with pytest.raises(RawArtifactConflictError):
        _store(store, payload=b"different immutable bytes")

    assert store.load_payload(original.receipt_id) == (
        b"\x89PNG\r\n\x1a\n\x00exact-provider-bytes"
    )
    assert store.stats()["receipt_count"] == 1


def test_same_blob_across_records_is_content_deduplicated(tmp_path):
    store = RawArtifactStore(tmp_path / "raw-artifacts.sqlite3")
    store.initialize()
    first = _store(store)
    second = _store(
        store,
        provider_id="opera",
        upstream_record_id="odim/20260729T190000Z.h5",
        media_type="application/x-hdf5",
        parser_version="opera-parser-v3",
    )

    assert first.content_sha256 == second.content_sha256
    assert first.receipt_id != second.receipt_id
    assert store.stats()["blob_count"] == 1
    assert store.stats()["receipt_count"] == 2


def test_concurrent_redelivery_has_one_blob_and_one_receipt(tmp_path):
    path = tmp_path / "raw-artifacts.sqlite3"
    stores = [RawArtifactStore(path) for _ in range(8)]
    for store in stores:
        store.initialize()

    with ThreadPoolExecutor(max_workers=len(stores)) as executor:
        receipts = list(executor.map(_store, stores))

    assert len({receipt.receipt_id for receipt in receipts}) == 1
    assert len({receipt.content_sha256 for receipt in receipts}) == 1
    assert stores[0].stats()["blob_count"] == 1
    assert stores[0].stats()["receipt_count"] == 1
    assert stores[0].integrity_check()["healthy"] is True


def test_concurrent_thread_initialize_is_idempotent(tmp_path):
    path = tmp_path / "thread-initialize.sqlite3"
    stores = [RawArtifactStore(path) for _ in range(8)]
    start = threading.Barrier(len(stores))

    def initialize(store):
        start.wait(timeout=10)
        return store.initialize()

    with ThreadPoolExecutor(max_workers=len(stores)) as executor:
        results = list(executor.map(initialize, stores))

    assert all(result == results[0] for result in results)
    assert results[0] == {
        "schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
        "blob_count": 0,
        "receipt_count": 0,
        "artifact_set_count": 0,
        "stored_bytes": 0,
    }
    assert stores[0].integrity_check()["healthy"] is True


def test_concurrent_process_initialize_is_idempotent(tmp_path):
    path = tmp_path / "process-initialize.sqlite3"
    start_signal = tmp_path / "initialize.start"
    helper = (
        Path(__file__).resolve().parent
        / "helpers/raw_artifact_initialize_child.py"
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    ready_paths = [
        tmp_path / f"initialize-{index}.ready"
        for index in range(6)
    ]
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                str(helper),
                str(path),
                str(ready_path),
                str(start_signal),
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for ready_path in ready_paths
    ]
    try:
        deadline = time.monotonic() + 10
        while (
            not all(ready_path.exists() for ready_path in ready_paths)
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        assert all(ready_path.exists() for ready_path in ready_paths)
        start_signal.write_text("start", encoding="ascii")
        results = [
            process.communicate(timeout=30)
            for process in processes
        ]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)

    failures = [
        (process.returncode, stdout, stderr)
        for process, (stdout, stderr) in zip(
            processes,
            results,
            strict=True,
        )
        if process.returncode != 0
    ]
    assert failures == []
    reopened = RawArtifactStore(path)
    assert reopened.initialize() == {
        "schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
        "blob_count": 0,
        "receipt_count": 0,
        "artifact_set_count": 0,
        "stored_bytes": 0,
    }
    assert reopened.integrity_check()["healthy"] is True


def test_atomic_artifact_set_round_trip_preserves_manifest_and_provenance(
    tmp_path,
):
    store = RawArtifactStore(tmp_path / "raw-artifacts.sqlite3")
    store.initialize()

    receipt = _store_set(store)

    assert receipt.provider_id == "rainviewer"
    assert receipt.upstream_set_id == "past/frame-a/revision-a"
    assert receipt.provenance_sha256 == "a" * 64
    assert len(receipt.manifest_sha256) == 64
    assert receipt.member_count == 2
    assert receipt.total_size_bytes == sum(
        len(member.payload) for member in _set_members()
    )
    assert tuple(member.member_id for member in receipt.members) == (
        "catalog",
        "frame",
    )
    assert store.load_set_receipt(receipt.artifact_set_id) == receipt
    assert store.load_set_payloads(receipt.artifact_set_id) == (
        ("catalog", _set_members()[0].payload),
        ("frame", _set_members()[1].payload),
    )
    assert store.stats() == {
        "schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
        "blob_count": 2,
        "receipt_count": 2,
        "artifact_set_count": 1,
        "stored_bytes": receipt.total_size_bytes,
    }
    assert store.integrity_check()["healthy"] is True


def test_artifact_set_redelivery_is_order_independent_and_idempotent(
    tmp_path,
):
    store = RawArtifactStore(tmp_path / "raw-artifacts.sqlite3")
    store.initialize()

    first = _store_set(store)
    second = _store_set(
        store,
        members=_set_members(reverse=True),
        fetched_at=1_800_000_020.0,
    )

    assert second == first
    assert store.stats()["blob_count"] == 2
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["artifact_set_count"] == 1


def test_concurrent_artifact_set_redelivery_commits_one_complete_set(
    tmp_path,
):
    path = tmp_path / "raw-artifacts.sqlite3"
    stores = [RawArtifactStore(path) for _ in range(6)]
    for store in stores:
        store.initialize()

    with ThreadPoolExecutor(max_workers=len(stores)) as executor:
        receipts = list(executor.map(_store_set, stores))

    assert len({receipt.artifact_set_id for receipt in receipts}) == 1
    assert len({receipt.manifest_sha256 for receipt in receipts}) == 1
    assert stores[0].stats()["blob_count"] == 2
    assert stores[0].stats()["receipt_count"] == 2
    assert stores[0].stats()["artifact_set_count"] == 1
    assert stores[0].integrity_check()["healthy"] is True


@pytest.mark.parametrize(
    "overrides",
    (
        {"provenance_sha256": "b" * 64},
        {
            "members": (
                _set_members()[0],
                RawArtifactMember(
                    member_id="frame",
                    role="authoritative-radar-frame",
                    upstream_record_id="rainviewer/frame/frame-a",
                    payload=b"different-frame-bytes",
                    media_type="image/png",
                    parser_version="rainviewer-parser-v2",
                ),
            ),
        },
        {
            "members": (
                RawArtifactMember(
                    member_id="catalog",
                    role="different-role",
                    upstream_record_id="rainviewer/catalog/revision-a",
                    payload=_set_members()[0].payload,
                    media_type="application/json",
                    parser_version="rainviewer-parser-v2",
                ),
                _set_members()[1],
            ),
        },
    ),
)
def test_artifact_set_identity_conflict_preserves_original_set(
    tmp_path,
    overrides,
):
    store = RawArtifactStore(tmp_path / "raw-artifacts.sqlite3")
    store.initialize()
    original = _store_set(store)

    with pytest.raises(RawArtifactConflictError):
        _store_set(store, **overrides)

    assert store.load_set_receipt(original.artifact_set_id) == original
    assert store.stats()["blob_count"] == 2
    assert store.stats()["receipt_count"] == 2
    assert store.stats()["artifact_set_count"] == 1


def test_member_conflict_rolls_back_every_new_member_and_set_row(tmp_path):
    store = RawArtifactStore(tmp_path / "raw-artifacts.sqlite3")
    store.initialize()
    _store(
        store,
        provider_id="rainviewer",
        upstream_record_id="rainviewer/frame/frame-a",
        payload=b"pre-existing-conflicting-frame",
        media_type="image/png",
        parser_version="rainviewer-parser-v2",
    )

    with pytest.raises(RawArtifactConflictError):
        _store_set(store)

    assert store.stats()["blob_count"] == 1
    assert store.stats()["receipt_count"] == 1
    assert store.stats()["artifact_set_count"] == 0
    assert store.integrity_check()["healthy"] is True


def test_artifact_set_metadata_corruption_fails_closed_before_replay(
    tmp_path,
):
    path = tmp_path / "raw-artifacts.sqlite3"
    store = RawArtifactStore(path)
    store.initialize()
    receipt = _store_set(store)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            """
            UPDATE raw_artifact_set_members
            SET role='tampered-role'
            WHERE artifact_set_id=? AND member_id='frame'
            """,
            (receipt.artifact_set_id,),
        )

    report = store.integrity_check()
    assert report["healthy"] is False
    assert report["content_mismatches"] == 0
    assert report["set_mismatches"] == 1
    with pytest.raises(RawArtifactConflictError, match="manifest"):
        store.load_set_receipt(receipt.artifact_set_id)
    with pytest.raises(RuntimeError, match="integriteitscontrole"):
        RawArtifactStore(path).initialize()


@pytest.mark.parametrize(
    ("bad_members", "error"),
    (
        ((), ValueError),
        ((_set_members()[0], _set_members()[0]), ValueError),
        (
            (
                {
                    "member_id": "catalog",
                    "role": "catalog",
                    "upstream_record_id": "catalog-a",
                    "payload": b"bytes",
                    "media_type": "application/json",
                    "parser_version": "v1",
                    "ignored": "must-not-be-silent",
                },
            ),
            ValueError,
        ),
    ),
)
def test_invalid_artifact_set_fails_before_any_write(
    tmp_path,
    bad_members,
    error,
):
    store = RawArtifactStore(tmp_path / "raw-artifacts.sqlite3")
    store.initialize()

    with pytest.raises(error):
        _store_set(store, members=bad_members)

    assert store.stats()["receipt_count"] == 0
    assert store.stats()["artifact_set_count"] == 0


def test_unknown_schema_version_is_never_overwritten(tmp_path):
    path = tmp_path / "raw-artifacts.sqlite3"
    RawArtifactStore(path).initialize()
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            """
            UPDATE raw_artifact_metadata
            SET schema_version=?
            WHERE metadata_key='schema'
            """,
            (RAW_ARTIFACT_SCHEMA_VERSION + 1,),
        )
    before_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()

    with pytest.raises(RuntimeError, match="onbekende.*schemaversie"):
        RawArtifactStore(path).initialize()
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before_sha256

    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute(
            "SELECT schema_version FROM raw_artifact_metadata"
        ).fetchone()[0] == RAW_ARTIFACT_SCHEMA_VERSION + 1


def test_existing_non_artifact_database_is_never_adopted_or_modified(
    tmp_path,
):
    path = tmp_path / "do-not-adopt.sqlite3"
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "CREATE TABLE home_assistant_sentinel(value TEXT NOT NULL)"
        )
        db.execute(
            "INSERT INTO home_assistant_sentinel(value) VALUES('behouden')"
        )
    before_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()

    with pytest.raises(RuntimeError, match="onbekend of onvolledig"):
        RawArtifactStore(path).initialize()
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before_sha256

    with closing(sqlite3.connect(path)) as db, db:
        tables = {
            row[0]
            for row in db.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        sentinel = db.execute(
            "SELECT value FROM home_assistant_sentinel"
        ).fetchone()[0]
    assert tables == {"home_assistant_sentinel"}
    assert sentinel == "behouden"


def test_corrupt_non_sqlite_database_is_rejected_byte_preserving(tmp_path):
    path = tmp_path / "corrupt.sqlite3"
    path.write_bytes(b"not-a-sqlite-database\x00incident-evidence")
    before_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()

    with pytest.raises(RuntimeError, match="geen leesbaar SQLite"):
        RawArtifactStore(path).initialize()

    assert hashlib.sha256(path.read_bytes()).hexdigest() == before_sha256


def test_named_index_with_wrong_columns_is_rejected(tmp_path):
    path = tmp_path / "wrong-index.sqlite3"
    RawArtifactStore(path).initialize()
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("DROP INDEX idx_raw_artifact_sets_manifest")
        db.execute(
            """
            CREATE INDEX idx_raw_artifact_sets_manifest
            ON raw_artifact_sets(provider_id)
            """
        )

    with pytest.raises(RuntimeError, match="index.*onverwachte kolommen"):
        RawArtifactStore(path).initialize()


@pytest.mark.parametrize(
    ("table", "old", "new"),
    (
        (
            "raw_artifact_blobs",
            "CHECK(size_bytes >= 0)",
            "",
        ),
        (
            "raw_artifact_blobs",
            "payload BLOB NOT NULL",
            "payload TEXT NOT NULL",
        ),
        (
            "raw_artifact_sets",
            "created_at REAL NOT NULL",
            "created_at REAL",
        ),
        (
            "raw_artifact_metadata",
            ") WITHOUT ROWID",
            ")",
        ),
    ),
)
def test_noncanonical_table_ddl_is_rejected(
    tmp_path,
    table,
    old,
    new,
):
    path = tmp_path / f"{table}-{hashlib.sha256(old.encode()).hexdigest()}.sqlite3"
    RawArtifactStore(path).initialize()
    with closing(sqlite3.connect(path)) as db, db:
        sql = db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()[0]
        assert old in sql
        db.execute("PRAGMA writable_schema=ON")
        db.execute(
            "UPDATE sqlite_master SET sql=? WHERE type='table' AND name=?",
            (sql.replace(old, new), table),
        )
        schema_version = int(
            db.execute("PRAGMA schema_version").fetchone()[0]
        )
        db.execute(f"PRAGMA schema_version={schema_version + 1}")
        db.execute("PRAGMA writable_schema=OFF")

    with pytest.raises(
        RuntimeError,
        match="onverwachte canonieke DDL|onvolledig of onleesbaar",
    ):
        RawArtifactStore(path).initialize()


@pytest.mark.parametrize(
    "statement",
    (
        """
        CREATE VIEW raw_artifact_receipt_view AS
        SELECT receipt_id FROM raw_artifact_receipts
        """,
        """
        CREATE TRIGGER raw_artifact_noop_trigger
        AFTER INSERT ON raw_artifact_receipts
        BEGIN
            SELECT 1;
        END
        """,
    ),
)
def test_unexpected_view_or_trigger_is_rejected_and_reported(
    tmp_path,
    statement,
):
    path = tmp_path / "unexpected-schema-object.sqlite3"
    store = RawArtifactStore(path)
    store.initialize()
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(statement)

    report = store.integrity_check()
    assert report["healthy"] is False
    assert report["schema_object_mismatches"] == 1
    with pytest.raises(RuntimeError, match="onverwachte schemaobjecten"):
        RawArtifactStore(path).initialize()


@pytest.mark.parametrize("operation", ("single", "set"))
def test_full_precommit_verification_rolls_back_trigger_corruption(
    tmp_path,
    operation,
):
    path = tmp_path / f"precommit-{operation}.sqlite3"
    store = RawArtifactStore(path)
    store.initialize()
    _install_payload_poison_trigger(path)

    with pytest.raises(RawArtifactConflictError, match="SHA-256"):
        if operation == "single":
            _store(store)
        else:
            _store_set(store)

    assert store.stats() == {
        "schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
        "blob_count": 0,
        "receipt_count": 0,
        "artifact_set_count": 0,
        "stored_bytes": 0,
    }
    report = store.integrity_check()
    assert report["healthy"] is False
    assert report["schema_object_mismatches"] == 1
    assert report["content_mismatches"] == 0
    assert report["receipt_mismatches"] == 0
    assert report["set_mismatches"] == 0


@pytest.mark.parametrize(
    ("field", "value", "error"),
    (
        ("provider_id", "", ValueError),
        ("upstream_record_id", "", ValueError),
        ("upstream_record_id", "unsafe\0identity", ValueError),
        ("payload", bytearray(b"not exact bytes"), TypeError),
        ("fetched_at", float("nan"), ValueError),
        ("nominal_at", float("inf"), ValueError),
        ("media_type", "", ValueError),
        ("parser_version", "", ValueError),
    ),
)
def test_invalid_artifact_contract_is_rejected(
    tmp_path,
    field,
    value,
    error,
):
    store = RawArtifactStore(tmp_path / f"{field}.sqlite3")
    store.initialize()

    with pytest.raises(error):
        _store(store, **{field: value})

    assert store.stats()["receipt_count"] == 0


def test_semantic_payload_corruption_is_detected_before_replay(tmp_path):
    path = tmp_path / "raw-artifacts.sqlite3"
    store = RawArtifactStore(path)
    store.initialize()
    receipt = _store(store)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            """
            UPDATE raw_artifact_blobs
            SET payload=?
            WHERE content_sha256=?
            """,
            (b"valid sqlite but wrong payload", receipt.content_sha256),
        )

    assert store.integrity_check() == {
        "healthy": False,
        "sqlite": ["ok"],
        "foreign_key_violations": 0,
        "schema_object_mismatches": 0,
        "missing_blobs": 0,
        "content_mismatches": 1,
        "receipt_mismatches": 0,
        "set_mismatches": 0,
    }
    with pytest.raises(RawArtifactConflictError, match="SHA-256"):
        store.load_payload(receipt.receipt_id)


def test_single_receipt_metadata_corruption_fails_closed(tmp_path):
    path = tmp_path / "raw-artifacts.sqlite3"
    store = RawArtifactStore(path)
    store.initialize()
    receipt = _store(store)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            """
            UPDATE raw_artifact_receipts
            SET parser_version='tampered-parser'
            WHERE receipt_id=?
            """,
            (receipt.receipt_id,),
        )

    report = store.integrity_check()
    assert report["healthy"] is False
    assert report["content_mismatches"] == 0
    assert report["receipt_mismatches"] == 1
    with pytest.raises(RawArtifactConflictError, match="provenancechecksum"):
        store.load_receipt(receipt.receipt_id)
    with pytest.raises(RuntimeError, match="integriteitscontrole"):
        RawArtifactStore(path).initialize()


def test_receipt_id_tampering_fails_integrity_and_startup(tmp_path):
    path = tmp_path / "raw-artifacts.sqlite3"
    store = RawArtifactStore(path)
    store.initialize()
    receipt = _store(store)
    forged_receipt_id = f"raw-{'f' * 64}"
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            """
            UPDATE raw_artifact_receipts
            SET receipt_id=?
            WHERE receipt_id=?
            """,
            (forged_receipt_id, receipt.receipt_id),
        )

    report = store.integrity_check()
    assert report["healthy"] is False
    assert report["receipt_mismatches"] == 1
    assert store.load_receipt(receipt.receipt_id) is None
    with pytest.raises(RawArtifactConflictError, match="provenancechecksum"):
        store.load_receipt(forged_receipt_id)
    with pytest.raises(RuntimeError, match="integriteitscontrole"):
        RawArtifactStore(path).initialize()


def test_set_receipt_metadata_corruption_fails_closed(tmp_path):
    path = tmp_path / "raw-artifacts.sqlite3"
    store = RawArtifactStore(path)
    store.initialize()
    receipt = _store_set(store)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            """
            UPDATE raw_artifact_sets
            SET fetched_at=fetched_at + 1
            WHERE artifact_set_id=?
            """,
            (receipt.artifact_set_id,),
        )

    report = store.integrity_check()
    assert report["healthy"] is False
    assert report["receipt_mismatches"] == 0
    assert report["set_mismatches"] == 1
    with pytest.raises(RawArtifactConflictError, match="manifestverificatie"):
        store.load_set_receipt(receipt.artifact_set_id)


def test_store_must_be_initialized_explicitly(tmp_path):
    store = RawArtifactStore(tmp_path / "raw-artifacts.sqlite3")

    with pytest.raises(RuntimeError, match="initialize"):
        _store(store)


@pytest.mark.parametrize(
    ("checkpoint", "committed"),
    (
        ("store_after_begin", False),
        ("store_after_blob", False),
        ("store_after_receipt", False),
        ("store_before_commit", False),
        ("store_after_commit", True),
    ),
)
def test_abrupt_process_exit_yields_nothing_or_complete_artifact(
    tmp_path,
    checkpoint,
    committed,
):
    path = tmp_path / f"{checkpoint}.sqlite3"
    RawArtifactStore(path).initialize()
    helper = (
        Path(__file__).resolve().parent
        / "helpers/raw_artifact_crash_child.py"
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])

    result = subprocess.run(
        [sys.executable, str(helper), str(path), checkpoint],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 73, result.stderr.decode(
        "utf-8", errors="replace"
    )
    reopened = RawArtifactStore(path)
    stats = reopened.initialize()
    assert reopened.integrity_check()["healthy"] is True
    if committed:
        assert stats["blob_count"] == 1
        assert stats["receipt_count"] == 1
        with closing(sqlite3.connect(path)) as db, db:
            receipt_id = db.execute(
                "SELECT receipt_id FROM raw_artifact_receipts"
            ).fetchone()[0]
        assert reopened.load_payload(receipt_id) == (
            b"\x00exact-crash-boundary-bytes"
        )
    else:
        assert stats["blob_count"] == 0
        assert stats["receipt_count"] == 0


@pytest.mark.parametrize(
    ("checkpoint", "committed"),
    (
        ("store_set_after_begin", False),
        ("store_set_after_first_member", False),
        ("store_set_after_all_member_receipts", False),
        ("store_set_after_set", False),
        ("store_set_after_members", False),
        ("store_set_before_commit", False),
        ("store_set_after_commit", True),
    ),
)
def test_abrupt_exit_never_exposes_a_partial_artifact_set(
    tmp_path,
    checkpoint,
    committed,
):
    path = tmp_path / f"{checkpoint}.sqlite3"
    RawArtifactStore(path).initialize()
    helper = (
        Path(__file__).resolve().parent
        / "helpers/raw_artifact_crash_child.py"
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])

    result = subprocess.run(
        [sys.executable, str(helper), str(path), checkpoint],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 73, result.stderr.decode(
        "utf-8", errors="replace"
    )
    reopened = RawArtifactStore(path)
    stats = reopened.initialize()
    assert reopened.integrity_check()["healthy"] is True
    if committed:
        assert stats["blob_count"] == 2
        assert stats["receipt_count"] == 2
        assert stats["artifact_set_count"] == 1
        with closing(sqlite3.connect(path)) as db, db:
            artifact_set_id = db.execute(
                "SELECT artifact_set_id FROM raw_artifact_sets"
            ).fetchone()[0]
        assert reopened.load_set_payloads(artifact_set_id) == (
            ("catalog", b'{"frame":"exact"}'),
            ("frame", b"\x89PNG\r\n\x1a\nexact-crash-frame"),
        )
    else:
        assert stats["blob_count"] == 0
        assert stats["receipt_count"] == 0
        assert stats["artifact_set_count"] == 0


def test_integration_and_worker_raw_artifact_store_are_exact_mirrors():
    root = __import__("pathlib").Path(__file__).resolve().parents[1]
    integration = (
        root
        / "custom_components/storm_tracker_v3/engine/raw_artifact_store.py"
    ).read_text(encoding="utf-8").replace("\r\n", "\n")
    worker = (
        root
        / "storm_tracker_worker/tracking_core/engine/raw_artifact_store.py"
    ).read_text(encoding="utf-8").replace("\r\n", "\n")
    assert worker == integration


def test_raw_artifact_store_remains_shadow_until_provider_gates_close():
    root = Path(__file__).resolve().parents[1]
    integration_runtime = (
        root / "custom_components/storm_tracker_v3/__init__.py"
    ).read_text(encoding="utf-8")
    worker_runtime = (
        root / "storm_tracker_worker/service.py"
    ).read_text(encoding="utf-8")

    assert "RawArtifactStore" not in integration_runtime
    assert "RawArtifactStore" not in worker_runtime
