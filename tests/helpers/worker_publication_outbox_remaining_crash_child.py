"""Closed hard-exit driver for the remaining publication-outbox boundaries."""
from __future__ import annotations

from contextlib import closing
import os
from pathlib import Path
import sqlite3
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storm_tracker_worker.operational_snapshot_publication import (  # noqa: E402
    PublicationOutboxStore,
    PublicationTransportHead,
    _legacy_transport_record_sha256,
)


PREPARE_NOW = 1_800_000_000.0
CRASH_NOW = PREPARE_NOW + 10.0
ENVELOPE_SHA256 = "a" * 64
DESIRED_STATE_SHA256 = "b" * 64
PUBLICATION_ID_SHA256 = "c" * 64

POINTS_BY_OPERATION = {
    "migration": frozenset({
        "migration.after_copy",
        "migration.before_commit",
    }),
    "stage": frozenset({"stage.after_write"}),
    **{
        operation: frozenset({
            f"{operation}.after_write",
            f"{operation}.before_commit",
            f"{operation}.after_commit",
        })
        for operation in (
            "adopt",
            "pending",
            "attempt",
            "published",
            "superseded",
            "discard",
        )
    },
}


def _create_legacy(path: Path) -> None:
    record = PublicationTransportHead(
        revision=1,
        envelope_sha256=ENVELOPE_SHA256,
        desired_state_sha256=DESIRED_STATE_SHA256,
        publication_id_sha256=PUBLICATION_ID_SHA256,
        state="published",
        attempt_count=1,
        staged_at=PREPARE_NOW - 2.0,
        last_attempt_at=PREPARE_NOW - 1.0,
        published_at=PREPARE_NOW,
        superseded_at=None,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as database, database:
        database.executescript(
            """
            CREATE TABLE publication_manifest (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) STRICT;
            CREATE TABLE publication_transport_head (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 1000000000),
                envelope_sha256 TEXT NOT NULL CHECK(length(envelope_sha256) = 64),
                desired_state_sha256 TEXT NOT NULL
                    CHECK(length(desired_state_sha256) = 64),
                publication_id_sha256 TEXT NOT NULL
                    CHECK(length(publication_id_sha256) = 64),
                state TEXT NOT NULL CHECK(state IN ('staged','pending','published')),
                attempt_count INTEGER NOT NULL
                    CHECK(attempt_count BETWEEN 0 AND 1000000000),
                staged_at REAL NOT NULL CHECK(staged_at >= 0),
                last_attempt_at REAL
                    CHECK(last_attempt_at IS NULL OR last_attempt_at >= 0),
                published_at REAL
                    CHECK(published_at IS NULL OR published_at >= 0),
                record_sha256 TEXT NOT NULL CHECK(length(record_sha256) = 64)
            ) STRICT;
            """
        )
        database.executemany(
            "INSERT INTO publication_manifest(key,value) VALUES(?,?)",
            (
                (
                    "algorithm_version",
                    "worker-operational-publication-outbox-v1",
                ),
                ("schema_version", "1"),
            ),
        )
        database.execute(
            "INSERT INTO publication_transport_head "
            "VALUES(1,?,?,?,?,?,?,?,?,?,?)",
            (
                record.revision,
                record.envelope_sha256,
                record.desired_state_sha256,
                record.publication_id_sha256,
                record.state,
                record.attempt_count,
                record.staged_at,
                record.last_attempt_at,
                record.published_at,
                _legacy_transport_record_sha256(record),
            ),
        )


def _stage(store: PublicationOutboxStore) -> None:
    store.stage(
        revision=1,
        envelope_sha256=ENVELOPE_SHA256,
        desired_state_sha256=DESIRED_STATE_SHA256,
        publication_id_sha256=PUBLICATION_ID_SHA256,
    )


def _prepare(path: Path, operation: str) -> None:
    store = PublicationOutboxStore(path, clock=lambda: PREPARE_NOW)
    store.load()
    if operation in {"pending", "attempt", "published", "superseded", "discard"}:
        _stage(store)
    if operation in {"attempt", "published", "superseded"}:
        store.mark_pending(revision=1, envelope_sha256=ENVELOPE_SHA256)
    if operation == "published":
        store.record_attempt(revision=1, envelope_sha256=ENVELOPE_SHA256)


def _invoke(store: PublicationOutboxStore, operation: str) -> None:
    if operation == "stage":
        _stage(store)
    elif operation == "adopt":
        store.adopt_pending(
            revision=1,
            envelope_sha256=ENVELOPE_SHA256,
            publication_id_sha256=PUBLICATION_ID_SHA256,
        )
    elif operation == "pending":
        store.mark_pending(revision=1, envelope_sha256=ENVELOPE_SHA256)
    elif operation == "attempt":
        store.record_attempt(revision=1, envelope_sha256=ENVELOPE_SHA256)
    elif operation == "published":
        store.mark_published(revision=1, envelope_sha256=ENVELOPE_SHA256)
    elif operation == "superseded":
        store.mark_superseded(revision=1, envelope_sha256=ENVELOPE_SHA256)
    elif operation == "discard":
        store.discard_staged(revision=1, envelope_sha256=ENVELOPE_SHA256)
    else:  # pragma: no cover - closed CLI contract
        raise SystemExit("publication-outbox operation is not allowlisted")


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "usage: worker_publication_outbox_remaining_crash_child.py "
            "DATABASE OPERATION CHECKPOINT"
        )
    path = Path(sys.argv[1]).resolve()
    operation = sys.argv[2]
    checkpoint = sys.argv[3]
    if checkpoint not in POINTS_BY_OPERATION.get(operation, frozenset()):
        raise SystemExit("publication-outbox operation/checkpoint is not allowlisted")

    def crash(point: str) -> None:
        if point == checkpoint:
            os._exit(73)

    if operation == "migration":
        _create_legacy(path)
        PublicationOutboxStore(path, fault_injector=crash).load()
    else:
        _prepare(path, operation)
        store = PublicationOutboxStore(
            path,
            clock=lambda: CRASH_NOW,
            fault_injector=crash,
        )
        _invoke(store, operation)
    raise SystemExit("allowlisted publication-outbox checkpoint was not reached")


if __name__ == "__main__":
    main()
