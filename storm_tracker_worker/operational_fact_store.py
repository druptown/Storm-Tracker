"""Crash-durable handoff from admitted worker facts to snapshot publication.

The store is intentionally not wired into :mod:`storm_tracker_worker.service`.
It preserves the exact ``CommittedOperationalFactRevision`` selected for one
pipeline item before the operational snapshot store or MQTT outbox is touched.
After a crash, the same canonical revision can therefore be retried instead of
being rebuilt with a new clock or snapshot revision.

Only a bounded contiguous tail is retained.  Prefix pruning is allowed solely
for terminal (snapshot-committed or superseded) rows and occurs in the same SQLite
transaction as the new stage.  A non-terminal row at the retention boundary
causes explicit backpressure; it is never evicted to make room.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping

from .desired_state import DesiredState
from .operational_fact_revision import (
    CommittedOperationalFactRevision,
    OperationalFactAdmissionError,
    validate_committed_operational_revision,
)
from .tracking_core.operational_snapshot import (
    canonical_json_bytes,
    decode_control_envelope,
    encode_control_envelope,
)


CONTRACT = "stv3-operational-fact-store-v1"
SCHEMA_VERSION = 1
ALGORITHM_VERSION = "worker-operational-fact-store-v1"
DEFAULT_RETENTION_ROWS = 512
MAX_REVISION_BYTES = 2 * 1024 * 1024
MAX_DATABASE_BYTES = 64 * 1024 * 1024
MAX_DIAGNOSTIC_BYTES = 4096
DEFAULT_PATH = Path(
    os.getenv(
        "STV3_OPERATIONAL_FACT_STORE_DB",
        "/share/storm_tracker_worker/operational_fact_revisions.sqlite3",
    )
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_REASON_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_SECRET_KEY_RE = re.compile(
    r"(?:authorization|password|passwd|secret|credential|private[_-]?key|"
    r"api[_-]?key|application[_-]?key|access[_-]?token|refresh[_-]?token)",
    re.IGNORECASE,
)
_SECRET_TEXT_RE = re.compile(
    r"(?:authorization\s*:|(?:password|passwd|secret|credential|"
    r"api[_-]?key|application[_-]?key|access[_-]?token|"
    r"refresh[_-]?token)\s*=)",
    re.IGNORECASE,
)
_STATES = frozenset({"ready", "snapshot_committed", "superseded"})
_TERMINAL_STATES = frozenset({"snapshot_committed", "superseded"})


class OperationalFactStoreError(RuntimeError):
    """Base class for a rejected or unavailable fact-store transition."""


class OperationalFactStoreConflict(OperationalFactStoreError):
    """An immutable ID, expected state or snapshot-commit link conflicts."""


class OperationalFactStoreCapacityError(OperationalFactStoreError):
    """A row, byte or database cap was reached without safe eviction."""


class OperationalFactStoreCorruption(OperationalFactStoreError):
    """Persisted schema, checksum, sequence or canonical bytes are invalid."""


class OperationalFactStoreExpired(OperationalFactStoreError):
    """A ready revision no longer covers the current publication time."""


@dataclass(frozen=True, slots=True)
class OperationalFactRecord:
    pipeline_item_id: str
    sequence: int
    revision_sha256: str
    snapshot_sha256: str
    desired_state_sha256: str
    authority_epoch: int
    snapshot_revision: int
    state: str
    publication_envelope_sha256: str | None
    created_at: float
    updated_at: float
    terminal_reason: str | None
    record_sha256: str
    revision: CommittedOperationalFactRevision


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as error:
        raise OperationalFactStoreError("waarde is geen canonieke JSON") from error


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise OperationalFactStoreError(f"{label} is geen lowercase SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or _IDENTIFIER_RE.fullmatch(value) is None
        or "://" in value
    ):
        raise OperationalFactStoreError(f"{label} is geen veilig begrensd ID")
    return value


def _reason(value: object) -> str:
    if not isinstance(value, str) or _REASON_RE.fullmatch(value) is None:
        raise OperationalFactStoreError("terminal_reason is ongeldig")
    return value


def _timestamp(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OperationalFactStoreError(f"{label} is geen tijdstip")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise OperationalFactStoreError(f"{label} is geen eindig tijdstip")
    return result


def _counter(value: object, label: str, *, minimum: int = 0) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= 2**63 - 1
    ):
        raise OperationalFactStoreError(f"{label} is geen geldige teller")
    return value


def _reject_secret_material(value: Any, *, depth: int = 0) -> None:
    if depth > 32:
        raise OperationalFactStoreCapacityError("fact revision is te diep genest")
    if isinstance(value, Mapping):
        if len(value) > 8192:
            raise OperationalFactStoreCapacityError("fact mapping is te groot")
        for key, child in value.items():
            if not isinstance(key, str) or _SECRET_KEY_RE.search(key):
                raise OperationalFactStoreError(
                    "fact revision bevat credentialvormige sleutel"
                )
            _reject_secret_material(child, depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        if len(value) > 8192:
            raise OperationalFactStoreCapacityError("fact reeks is te groot")
        for child in value:
            _reject_secret_material(child, depth=depth + 1)
        return
    if isinstance(value, str):
        if len(value.encode("utf-8")) > 256 * 1024:
            raise OperationalFactStoreCapacityError("fact tekst is te groot")
        if _SECRET_TEXT_RE.search(value):
            raise OperationalFactStoreError(
                "fact revision bevat credentialvormige tekst"
            )
        return
    if value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise OperationalFactStoreError("fact revision bevat niet-eindig getal")
        return
    raise OperationalFactStoreError("fact revision bevat een ongeldig waardetype")


def _record_payload(
    *,
    pipeline_item_id: str,
    sequence: int,
    revision_sha256: str,
    snapshot_sha256: str,
    desired_state_sha256: str,
    authority_epoch: int,
    snapshot_revision: int,
    canonical_revision_sha256: str,
    revision_bytes: int,
    state: str,
    publication_envelope_sha256: str | None,
    created_at: float,
    updated_at: float,
    terminal_reason: str | None,
) -> dict[str, Any]:
    return {
        "algorithm": ALGORITHM_VERSION,
        "authority_epoch": authority_epoch,
        "canonical_revision_sha256": canonical_revision_sha256,
        "contract": CONTRACT,
        "created_at": created_at,
        "desired_state_sha256": desired_state_sha256,
        "pipeline_item_id": pipeline_item_id,
        "publication_envelope_sha256": publication_envelope_sha256,
        "revision_bytes": revision_bytes,
        "revision_sha256": revision_sha256,
        "sequence": sequence,
        "snapshot_revision": snapshot_revision,
        "snapshot_sha256": snapshot_sha256,
        "state": state,
        "terminal_reason": terminal_reason,
        "updated_at": updated_at,
    }


def _state_payload(
    *,
    head_sequence: int,
    first_retained_sequence: int,
    pruned_through_sequence: int,
    retained_rows: int,
    head_record_sha256: str | None,
    last_pruned_record_sha256: str | None,
    updated_at: float,
) -> dict[str, Any]:
    return {
        "algorithm": ALGORITHM_VERSION,
        "contract": CONTRACT,
        "first_retained_sequence": first_retained_sequence,
        "head_record_sha256": head_record_sha256,
        "head_sequence": head_sequence,
        "last_pruned_record_sha256": last_pruned_record_sha256,
        "pruned_through_sequence": pruned_through_sequence,
        "retained_rows": retained_rows,
        "updated_at": updated_at,
    }


class OperationalFactRevisionStore:
    """Bounded CAS handoff for exact admitted snapshot revisions."""

    _EXPECTED_COLUMNS = {
        "fact_store_manifest": {
            "singleton", "schema_version", "algorithm_version", "created_at",
        },
        "fact_revisions": {
            "pipeline_item_id", "sequence", "revision_sha256",
            "snapshot_sha256", "desired_state_sha256", "authority_epoch",
            "snapshot_revision", "canonical_revision",
            "canonical_revision_sha256", "revision_bytes", "state",
            "publication_envelope_sha256", "created_at", "updated_at",
            "terminal_reason", "record_sha256",
        },
        "fact_store_state": {
            "singleton", "head_sequence", "first_retained_sequence",
            "pruned_through_sequence", "retained_rows",
            "head_record_sha256", "last_pruned_record_sha256", "updated_at",
            "state_sha256",
        },
    }

    def __init__(
        self,
        path: str | Path = DEFAULT_PATH,
        *,
        clock: Callable[[], float] = time.time,
        retention_rows: int = DEFAULT_RETENTION_ROWS,
        max_revision_bytes: int = MAX_REVISION_BYTES,
        max_database_bytes: int = MAX_DATABASE_BYTES,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError("fact-storepad moet absoluut zijn")
        for label, value, minimum in (
            ("retention_rows", retention_rows, 2),
            ("max_revision_bytes", max_revision_bytes, 1024),
            ("max_database_bytes", max_database_bytes, 1024 * 1024),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"{label} is ongeldig")
        if max_revision_bytes > max_database_bytes:
            raise ValueError("revisionbudget overschrijdt databasebudget")
        if not callable(clock):
            raise TypeError("clock moet callable zijn")
        self._clock = clock
        self._retention_rows = retention_rows
        self._max_revision_bytes = max_revision_bytes
        self._max_database_bytes = max_database_bytes
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self.initialize()

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=5.0)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        mode = str(db.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).lower()
        if mode != "delete":
            db.close()
            raise OperationalFactStoreError("fact-store journal mode wijkt af")
        page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
        maximum_pages = max(1, self._max_database_bytes // page_size)
        db.execute(f"PRAGMA max_page_count={maximum_pages}")
        return db

    @staticmethod
    def _used_database_bytes(db: sqlite3.Connection) -> int:
        page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
        freelist = int(db.execute("PRAGMA freelist_count").fetchone()[0])
        return max(0, page_count - freelist) * page_size

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists() and (self.path.is_symlink() or not self.path.is_file()):
            raise OperationalFactStoreError("fact-storepad is onveilig")
        now = _timestamp(self._clock(), "storeklok")
        with self._lock, closing(self._connect()) as db:
            try:
                db.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS fact_store_manifest(
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        schema_version INTEGER NOT NULL,
                        algorithm_version TEXT NOT NULL,
                        created_at REAL NOT NULL
                    ) STRICT;
                    CREATE TABLE IF NOT EXISTS fact_revisions(
                        pipeline_item_id TEXT PRIMARY KEY,
                        sequence INTEGER NOT NULL UNIQUE CHECK(sequence>=1),
                        revision_sha256 TEXT NOT NULL UNIQUE,
                        snapshot_sha256 TEXT NOT NULL,
                        desired_state_sha256 TEXT NOT NULL,
                        authority_epoch INTEGER NOT NULL CHECK(authority_epoch>=1),
                        snapshot_revision INTEGER NOT NULL CHECK(snapshot_revision>=1),
                        canonical_revision BLOB NOT NULL,
                        canonical_revision_sha256 TEXT NOT NULL,
                        revision_bytes INTEGER NOT NULL CHECK(revision_bytes>=1),
                        state TEXT NOT NULL CHECK(state IN ('ready','snapshot_committed','superseded')),
                        publication_envelope_sha256 TEXT,
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL,
                        terminal_reason TEXT,
                        record_sha256 TEXT NOT NULL
                    ) STRICT;
                    CREATE UNIQUE INDEX IF NOT EXISTS fact_snapshot_revision_unique
                    ON fact_revisions(desired_state_sha256,snapshot_revision);
                    CREATE INDEX IF NOT EXISTS fact_state_sequence
                    ON fact_revisions(state,sequence);
                    CREATE TABLE IF NOT EXISTS fact_store_state(
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        head_sequence INTEGER NOT NULL CHECK(head_sequence>=0),
                        first_retained_sequence INTEGER NOT NULL CHECK(first_retained_sequence>=1),
                        pruned_through_sequence INTEGER NOT NULL CHECK(pruned_through_sequence>=0),
                        retained_rows INTEGER NOT NULL CHECK(retained_rows>=0),
                        head_record_sha256 TEXT,
                        last_pruned_record_sha256 TEXT,
                        updated_at REAL NOT NULL,
                        state_sha256 TEXT NOT NULL
                    ) STRICT;
                    """
                )
                db.execute("BEGIN IMMEDIATE")
                manifest = db.execute(
                    "SELECT * FROM fact_store_manifest WHERE singleton=1"
                ).fetchone()
                if manifest is None:
                    db.execute(
                        "INSERT INTO fact_store_manifest VALUES(1,?,?,?)",
                        (SCHEMA_VERSION, ALGORITHM_VERSION, now),
                    )
                elif (
                    int(manifest["schema_version"]) != SCHEMA_VERSION
                    or str(manifest["algorithm_version"]) != ALGORITHM_VERSION
                ):
                    raise OperationalFactStoreCorruption(
                        "fact-store manifestversie is onbekend"
                    )
                state = db.execute(
                    "SELECT * FROM fact_store_state WHERE singleton=1"
                ).fetchone()
                if state is None:
                    payload = _state_payload(
                        head_sequence=0,
                        first_retained_sequence=1,
                        pruned_through_sequence=0,
                        retained_rows=0,
                        head_record_sha256=None,
                        last_pruned_record_sha256=None,
                        updated_at=0.0,
                    )
                    db.execute(
                        "INSERT INTO fact_store_state VALUES(1,0,1,0,0,NULL,NULL,0,?)",
                        (_digest(payload),),
                    )
                self._verify_schema(db)
                self._verify_all(db)
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def _verify_schema(self, db: sqlite3.Connection) -> None:
        for table, expected in self._EXPECTED_COLUMNS.items():
            rows = db.execute(f"PRAGMA table_info({table})").fetchall()
            if {str(row["name"]) for row in rows} != expected:
                raise OperationalFactStoreCorruption(
                    f"fact-store schema wijkt af voor {table}"
                )

    def _decode_revision(self, row: sqlite3.Row) -> CommittedOperationalFactRevision:
        raw = row["canonical_revision"]
        if not isinstance(raw, (bytes, bytearray, memoryview)):
            raise OperationalFactStoreCorruption("fact revision is geen BLOB")
        body = bytes(raw)
        if len(body) != int(row["revision_bytes"]):
            raise OperationalFactStoreCorruption("fact revision lengte wijkt af")
        canonical_sha = hashlib.sha256(body).hexdigest()
        if canonical_sha != str(row["canonical_revision_sha256"]):
            raise OperationalFactStoreCorruption("fact revision byteshash wijkt af")
        try:
            decoded = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
            raise OperationalFactStoreCorruption("fact revision JSON is ongeldig") from error
        if _canonical(decoded) != body:
            raise OperationalFactStoreCorruption("fact revision JSON is niet canoniek")
        try:
            revision = CommittedOperationalFactRevision.from_mapping(decoded)
        except (OperationalFactAdmissionError, TypeError, ValueError) as error:
            raise OperationalFactStoreCorruption("fact revision contract is ongeldig") from error
        if revision.revision_sha256 != str(row["revision_sha256"]):
            raise OperationalFactStoreCorruption("fact revision identiteit wijkt af")
        snapshot_sha = hashlib.sha256(
            canonical_json_bytes(revision.snapshot)
        ).hexdigest()
        if snapshot_sha != str(row["snapshot_sha256"]):
            raise OperationalFactStoreCorruption("snapshotidentiteit wijkt af")
        return revision

    def _record_from_row(self, row: sqlite3.Row) -> OperationalFactRecord:
        state = str(row["state"])
        if state not in _STATES:
            raise OperationalFactStoreCorruption("fact state is ongeldig")
        envelope = row["publication_envelope_sha256"]
        terminal_reason = row["terminal_reason"]
        if state == "ready":
            if envelope is not None or terminal_reason is not None:
                raise OperationalFactStoreCorruption("ready fact bevat terminale velden")
        elif state == "snapshot_committed":
            _sha(envelope, "publication_envelope_sha256")
            if terminal_reason != "snapshot_committed":
                raise OperationalFactStoreCorruption("committed fact mist reden")
        else:
            if envelope is not None:
                raise OperationalFactStoreCorruption("superseded fact bevat envelope")
            _reason(terminal_reason)
        values = {
            "pipeline_item_id": _identifier(row["pipeline_item_id"], "pipeline_item_id"),
            "sequence": _counter(row["sequence"], "sequence", minimum=1),
            "revision_sha256": _sha(row["revision_sha256"], "revision_sha256"),
            "snapshot_sha256": _sha(row["snapshot_sha256"], "snapshot_sha256"),
            "desired_state_sha256": _sha(
                row["desired_state_sha256"], "desired_state_sha256"
            ),
            "authority_epoch": _counter(
                row["authority_epoch"], "authority_epoch", minimum=1
            ),
            "snapshot_revision": _counter(
                row["snapshot_revision"], "snapshot_revision", minimum=1
            ),
            "canonical_revision_sha256": _sha(
                row["canonical_revision_sha256"], "canonical_revision_sha256"
            ),
            "revision_bytes": _counter(
                row["revision_bytes"], "revision_bytes", minimum=1
            ),
            "state": state,
            "publication_envelope_sha256": envelope,
            "created_at": _timestamp(row["created_at"], "created_at"),
            "updated_at": _timestamp(row["updated_at"], "updated_at"),
            "terminal_reason": terminal_reason,
        }
        expected_record_sha = _digest(_record_payload(**values))
        if expected_record_sha != _sha(row["record_sha256"], "record_sha256"):
            raise OperationalFactStoreCorruption("fact recordchecksum wijkt af")
        revision = self._decode_revision(row)
        if (
            revision.desired_state_sha256 != values["desired_state_sha256"]
            or revision.authority_epoch != values["authority_epoch"]
            or int(revision.snapshot["revision"]) != values["snapshot_revision"]
        ):
            raise OperationalFactStoreCorruption("fact record en revision verschillen")
        return OperationalFactRecord(
            pipeline_item_id=values["pipeline_item_id"],
            sequence=values["sequence"],
            revision_sha256=values["revision_sha256"],
            snapshot_sha256=values["snapshot_sha256"],
            desired_state_sha256=values["desired_state_sha256"],
            authority_epoch=values["authority_epoch"],
            snapshot_revision=values["snapshot_revision"],
            state=state,
            publication_envelope_sha256=envelope,
            created_at=values["created_at"],
            updated_at=values["updated_at"],
            terminal_reason=terminal_reason,
            record_sha256=str(row["record_sha256"]),
            revision=revision,
        )

    def _verified_state(self, db: sqlite3.Connection) -> sqlite3.Row:
        rows = db.execute("SELECT * FROM fact_store_state").fetchall()
        if len(rows) != 1 or int(rows[0]["singleton"]) != 1:
            raise OperationalFactStoreCorruption("fact-store state ontbreekt")
        row = rows[0]
        payload = _state_payload(
            head_sequence=_counter(row["head_sequence"], "head_sequence"),
            first_retained_sequence=_counter(
                row["first_retained_sequence"],
                "first_retained_sequence",
                minimum=1,
            ),
            pruned_through_sequence=_counter(
                row["pruned_through_sequence"], "pruned_through_sequence"
            ),
            retained_rows=_counter(row["retained_rows"], "retained_rows"),
            head_record_sha256=row["head_record_sha256"],
            last_pruned_record_sha256=row["last_pruned_record_sha256"],
            updated_at=_timestamp(row["updated_at"], "state.updated_at"),
        )
        for name in ("head_record_sha256", "last_pruned_record_sha256"):
            if payload[name] is not None:
                _sha(payload[name], name)
        if _digest(payload) != _sha(row["state_sha256"], "state_sha256"):
            raise OperationalFactStoreCorruption("fact-store statechecksum wijkt af")
        return row

    def _verify_all(self, db: sqlite3.Connection) -> tuple[sqlite3.Row, list[OperationalFactRecord]]:
        state = self._verified_state(db)
        rows = db.execute("SELECT * FROM fact_revisions ORDER BY sequence").fetchall()
        records = [self._record_from_row(row) for row in rows]
        head = int(state["head_sequence"])
        first = int(state["first_retained_sequence"])
        pruned = int(state["pruned_through_sequence"])
        count = int(state["retained_rows"])
        if first != pruned + 1 or count != len(records):
            raise OperationalFactStoreCorruption("fact-retentiecheckpoint wijkt af")
        if not records:
            if head != pruned or state["head_record_sha256"] is not None:
                raise OperationalFactStoreCorruption("lege fact-storehead is ongeldig")
        else:
            expected_sequences = list(range(first, head + 1))
            if [record.sequence for record in records] != expected_sequences:
                raise OperationalFactStoreCorruption("fact-sequentiereeks bevat een gat")
            if records[-1].record_sha256 != state["head_record_sha256"]:
                raise OperationalFactStoreCorruption("fact-store headchecksum wijkt af")
        if count > self._retention_rows:
            raise OperationalFactStoreCorruption("fact-store overschrijdt retentie")
        return state, records

    def _write_state(
        self,
        db: sqlite3.Connection,
        *,
        head_sequence: int,
        first_retained_sequence: int,
        pruned_through_sequence: int,
        retained_rows: int,
        head_record_sha256: str | None,
        last_pruned_record_sha256: str | None,
        updated_at: float,
    ) -> None:
        payload = _state_payload(
            head_sequence=head_sequence,
            first_retained_sequence=first_retained_sequence,
            pruned_through_sequence=pruned_through_sequence,
            retained_rows=retained_rows,
            head_record_sha256=head_record_sha256,
            last_pruned_record_sha256=last_pruned_record_sha256,
            updated_at=updated_at,
        )
        db.execute(
            "UPDATE fact_store_state SET head_sequence=?,first_retained_sequence=?,"
            "pruned_through_sequence=?,retained_rows=?,head_record_sha256=?,"
            "last_pruned_record_sha256=?,updated_at=?,state_sha256=? WHERE singleton=1",
            (
                head_sequence,
                first_retained_sequence,
                pruned_through_sequence,
                retained_rows,
                head_record_sha256,
                last_pruned_record_sha256,
                updated_at,
                _digest(payload),
            ),
        )

    def _update_record_state(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        state: str,
        envelope_sha256: str | None,
        terminal_reason: str,
        updated_at: float,
    ) -> str:
        values = {
            "pipeline_item_id": str(row["pipeline_item_id"]),
            "sequence": int(row["sequence"]),
            "revision_sha256": str(row["revision_sha256"]),
            "snapshot_sha256": str(row["snapshot_sha256"]),
            "desired_state_sha256": str(row["desired_state_sha256"]),
            "authority_epoch": int(row["authority_epoch"]),
            "snapshot_revision": int(row["snapshot_revision"]),
            "canonical_revision_sha256": str(row["canonical_revision_sha256"]),
            "revision_bytes": int(row["revision_bytes"]),
            "state": state,
            "publication_envelope_sha256": envelope_sha256,
            "created_at": float(row["created_at"]),
            "updated_at": updated_at,
            "terminal_reason": terminal_reason,
        }
        record_sha = _digest(_record_payload(**values))
        changed = db.execute(
            "UPDATE fact_revisions SET state=?,publication_envelope_sha256=?,"
            "updated_at=?,terminal_reason=?,record_sha256=? "
            "WHERE pipeline_item_id=? AND record_sha256=?",
            (
                state,
                envelope_sha256,
                updated_at,
                terminal_reason,
                record_sha,
                row["pipeline_item_id"],
                row["record_sha256"],
            ),
        ).rowcount
        if changed != 1:
            raise OperationalFactStoreConflict("fact record CAS faalde")
        return record_sha

    def stage(
        self,
        pipeline_item_id: str,
        *,
        desired_state: DesiredState,
        revision: CommittedOperationalFactRevision,
    ) -> OperationalFactRecord:
        """Persist one exact admitted revision before publication begins."""

        item_id = _identifier(pipeline_item_id, "pipeline_item_id")
        try:
            snapshot = validate_committed_operational_revision(desired_state, revision)
        except OperationalFactAdmissionError as error:
            raise OperationalFactStoreError("fact revision is niet admitted") from error
        mapping = revision.to_mapping()
        _reject_secret_material(mapping)
        canonical = _canonical(mapping)
        if len(canonical) > self._max_revision_bytes:
            raise OperationalFactStoreCapacityError("fact revision overschrijdt bytebudget")
        canonical_sha = hashlib.sha256(canonical).hexdigest()
        snapshot_sha = hashlib.sha256(canonical_json_bytes(snapshot)).hexdigest()
        now = _timestamp(self._clock(), "storeklok")
        if float(snapshot["valid_until"]) <= now:
            raise OperationalFactStoreExpired(
                "fact revision is niet actueel op het commitmoment"
            )

        with self._lock, closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                state_row, records = self._verify_all(db)
                existing = db.execute(
                    "SELECT * FROM fact_revisions WHERE pipeline_item_id=?",
                    (item_id,),
                ).fetchone()
                if existing is not None:
                    record = self._record_from_row(existing)
                    if record.revision_sha256 != revision.revision_sha256:
                        raise OperationalFactStoreConflict(
                            "pipeline_item_id werd voor andere revision hergebruikt"
                        )
                    db.commit()
                    return record
                collision = db.execute(
                    "SELECT pipeline_item_id FROM fact_revisions WHERE revision_sha256=?",
                    (revision.revision_sha256,),
                ).fetchone()
                if collision is not None:
                    raise OperationalFactStoreConflict(
                        "revision is reeds aan een ander pipeline-item gebonden"
                    )
                same_slot = db.execute(
                    "SELECT pipeline_item_id FROM fact_revisions "
                    "WHERE desired_state_sha256=? AND snapshot_revision=?",
                    (desired_state.state_sha256, int(snapshot["revision"])),
                ).fetchone()
                if same_slot is not None:
                    raise OperationalFactStoreConflict(
                        "snapshotrevision heeft reeds een fact revision"
                    )
                head = int(state_row["head_sequence"])
                first = int(state_row["first_retained_sequence"])
                pruned = int(state_row["pruned_through_sequence"])
                last_pruned = state_row["last_pruned_record_sha256"]
                sequence = head + 1
                new_floor = max(first, sequence - self._retention_rows + 1)
                if new_floor > first:
                    prefix = [record for record in records if record.sequence < new_floor]
                    if any(record.state not in _TERMINAL_STATES for record in prefix):
                        raise OperationalFactStoreCapacityError(
                            "niet-terminale fact blokkeert begrensde retentie"
                        )
                    if not prefix or prefix[-1].sequence != new_floor - 1:
                        raise OperationalFactStoreCorruption(
                            "fact-retentie mist exacte prefix-anchor"
                        )
                    last_pruned = prefix[-1].record_sha256
                    db.execute("DELETE FROM fact_revisions WHERE sequence<?", (new_floor,))
                    self._fault("stage.after_prune")
                    pruned = new_floor - 1
                if self._used_database_bytes(db) >= self._max_database_bytes:
                    raise OperationalFactStoreCapacityError(
                        "fact-storedatabasebudget is bereikt"
                    )
                values = {
                    "pipeline_item_id": item_id,
                    "sequence": sequence,
                    "revision_sha256": revision.revision_sha256,
                    "snapshot_sha256": snapshot_sha,
                    "desired_state_sha256": desired_state.state_sha256,
                    "authority_epoch": desired_state.authority_epoch,
                    "snapshot_revision": int(snapshot["revision"]),
                    "canonical_revision_sha256": canonical_sha,
                    "revision_bytes": len(canonical),
                    "state": "ready",
                    "publication_envelope_sha256": None,
                    "created_at": now,
                    "updated_at": now,
                    "terminal_reason": None,
                }
                record_sha = _digest(_record_payload(**values))
                db.execute(
                    "INSERT INTO fact_revisions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        item_id,
                        sequence,
                        revision.revision_sha256,
                        snapshot_sha,
                        desired_state.state_sha256,
                        desired_state.authority_epoch,
                        int(snapshot["revision"]),
                        canonical,
                        canonical_sha,
                        len(canonical),
                        "ready",
                        None,
                        now,
                        now,
                        None,
                        record_sha,
                    ),
                )
                self._fault("stage.after_insert")
                retained = sequence - new_floor + 1
                self._write_state(
                    db,
                    head_sequence=sequence,
                    first_retained_sequence=new_floor,
                    pruned_through_sequence=pruned,
                    retained_rows=retained,
                    head_record_sha256=record_sha,
                    last_pruned_record_sha256=last_pruned,
                    updated_at=now,
                )
                self._fault("stage.after_state")
                self._fault("stage.before_commit")
                db.commit()
            except sqlite3.IntegrityError as error:
                db.rollback()
                raise OperationalFactStoreConflict("fact-store uniqueness faalde") from error
            except sqlite3.OperationalError as error:
                db.rollback()
                if (
                    getattr(error, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL
                    or "database or disk is full" in str(error).lower()
                ):
                    raise OperationalFactStoreCapacityError(
                        "SQLite fact-storedatabasebudget is bereikt"
                    ) from error
                raise
            except BaseException:
                db.rollback()
                raise
        self._fault("stage.after_commit")
        return self.load(item_id)

    def load(self, pipeline_item_id: str) -> OperationalFactRecord:
        item_id = _identifier(pipeline_item_id, "pipeline_item_id")
        with self._lock, closing(self._connect()) as db:
            self._verify_all(db)
            row = db.execute(
                "SELECT * FROM fact_revisions WHERE pipeline_item_id=?",
                (item_id,),
            ).fetchone()
            if row is None:
                raise OperationalFactStoreConflict("pipeline-item is niet bewaard")
            return self._record_from_row(row)

    def next_ready(
        self,
        desired_state: DesiredState,
        *,
        expected_snapshot_revision: int | None = None,
    ) -> OperationalFactRecord | None:
        if not isinstance(desired_state, DesiredState):
            raise TypeError("desired_state moet gevalideerd zijn")
        now = _timestamp(self._clock(), "storeklok")
        with self._lock, closing(self._connect()) as db:
            self._verify_all(db)
            row = db.execute(
                "SELECT * FROM fact_revisions WHERE state='ready' "
                "AND desired_state_sha256=? AND authority_epoch=? "
                "ORDER BY sequence LIMIT 1",
                (desired_state.state_sha256, desired_state.authority_epoch),
            ).fetchone()
            if row is None:
                return None
            record = self._record_from_row(row)
            if expected_snapshot_revision is not None and record.snapshot_revision != _counter(
                expected_snapshot_revision,
                "expected_snapshot_revision",
                minimum=1,
            ):
                raise OperationalFactStoreConflict(
                    "ready fact revision sluit niet aan op snapshothead"
                )
            try:
                validate_committed_operational_revision(
                    desired_state,
                    record.revision,
                )
            except OperationalFactAdmissionError as error:
                raise OperationalFactStoreCorruption(
                    "bewaarde ready revision verliest admission"
                ) from error
            if float(record.revision.snapshot["valid_until"]) <= now:
                raise OperationalFactStoreExpired("ready fact revision is verlopen")
            return record

    def select_for_publication(
        self,
        desired_state: DesiredState,
        *,
        expected_snapshot_revision: int,
    ) -> OperationalFactRecord | None:
        """Return one exact current ready row and retire stale predecessors.

        Rows behind the durable snapshot head, expired rows and future-clock
        rows are terminalized with a fixed reason before the next candidate is
        considered.  A row *ahead* of the expected snapshot revision is never
        skipped or rewritten; that indicates a lifecycle ordering conflict.
        """

        if not isinstance(desired_state, DesiredState):
            raise TypeError("desired_state moet gevalideerd zijn")
        expected = _counter(
            expected_snapshot_revision,
            "expected_snapshot_revision",
            minimum=1,
        )
        while True:
            now = _timestamp(self._clock(), "storeklok")
            with self._lock, closing(self._connect()) as db:
                self._verify_all(db)
                row = db.execute(
                    "SELECT * FROM fact_revisions WHERE state='ready' "
                    "AND desired_state_sha256=? AND authority_epoch=? "
                    "ORDER BY sequence LIMIT 1",
                    (desired_state.state_sha256, desired_state.authority_epoch),
                ).fetchone()
                if row is None:
                    return None
                record = self._record_from_row(row)
            evaluated_at = float(record.revision.snapshot["evaluated_at"])
            valid_until = float(record.revision.snapshot["valid_until"])
            if (
                record.snapshot_revision == expected
                and evaluated_at <= now < valid_until
            ):
                try:
                    validate_committed_operational_revision(
                        desired_state,
                        record.revision,
                    )
                except OperationalFactAdmissionError as error:
                    raise OperationalFactStoreCorruption(
                        "ready fact revision verliest admission"
                    ) from error
                return record
            if record.snapshot_revision > expected:
                raise OperationalFactStoreConflict(
                    "ready fact revision ligt voor de snapshothead"
                )
            if evaluated_at > now:
                reason = "fact_revision_future_clock"
            elif valid_until <= now:
                reason = "fact_revision_expired"
            else:
                reason = "snapshot_revision_superseded"
            try:
                self.mark_superseded(
                    record.pipeline_item_id,
                    revision_sha256=record.revision_sha256,
                    reason=reason,
                )
            except OperationalFactStoreConflict:
                # Another process may have terminalized the same row after the
                # read.  Re-evaluate the durable head instead of skipping it.
                continue

    def supersede_all_ready(
        self,
        *,
        reason: str = "desired_state_unavailable",
    ) -> int:
        """Terminalize every bounded ready row when no authority is active."""

        terminal_reason = _reason(reason)
        changed = 0
        while True:
            with self._lock, closing(self._connect()) as db:
                self._verify_all(db)
                row = db.execute(
                    "SELECT pipeline_item_id,revision_sha256 "
                    "FROM fact_revisions WHERE state='ready' "
                    "ORDER BY sequence LIMIT 1"
                ).fetchone()
            if row is None:
                return changed
            try:
                self.mark_superseded(
                    str(row["pipeline_item_id"]),
                    revision_sha256=str(row["revision_sha256"]),
                    reason=terminal_reason,
                )
                changed += 1
            except OperationalFactStoreConflict:
                continue

    def mark_snapshot_committed(
        self,
        pipeline_item_id: str,
        *,
        revision_sha256: str,
        snapshot_sha256: str,
        publication_envelope_sha256: str,
    ) -> OperationalFactRecord:
        item_id = _identifier(pipeline_item_id, "pipeline_item_id")
        revision_sha = _sha(revision_sha256, "revision_sha256")
        snapshot_sha = _sha(snapshot_sha256, "snapshot_sha256")
        envelope_sha = _sha(
            publication_envelope_sha256,
            "publication_envelope_sha256",
        )
        now = _timestamp(self._clock(), "storeklok")
        with self._lock, closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                state_row, _records = self._verify_all(db)
                row = db.execute(
                    "SELECT * FROM fact_revisions WHERE pipeline_item_id=?",
                    (item_id,),
                ).fetchone()
                if row is None:
                    raise OperationalFactStoreConflict("pipeline-item ontbreekt")
                record = self._record_from_row(row)
                if (
                    record.revision_sha256 != revision_sha
                    or record.snapshot_sha256 != snapshot_sha
                ):
                    raise OperationalFactStoreConflict("publication link wijkt af")
                expected_envelope_sha = hashlib.sha256(
                    encode_control_envelope(record.revision.snapshot)
                ).hexdigest()
                if envelope_sha != expected_envelope_sha:
                    raise OperationalFactStoreConflict(
                        "publication envelope hoort niet bij fact revision"
                    )
                if record.state == "snapshot_committed":
                    if record.publication_envelope_sha256 != envelope_sha:
                        raise OperationalFactStoreConflict(
                            "committed envelope werd gewijzigd"
                        )
                    db.commit()
                    return record
                if record.state != "ready":
                    raise OperationalFactStoreConflict(
                        "alleen ready fact mag gepubliceerd worden"
                    )
                new_record_sha = self._update_record_state(
                    db,
                    row,
                    state="snapshot_committed",
                    envelope_sha256=envelope_sha,
                    terminal_reason="snapshot_committed",
                    updated_at=now,
                )
                self._fault("publish.after_update")
                head_record_sha = (
                    new_record_sha
                    if record.sequence == int(state_row["head_sequence"])
                    else state_row["head_record_sha256"]
                )
                self._write_state(
                    db,
                    head_sequence=int(state_row["head_sequence"]),
                    first_retained_sequence=int(state_row["first_retained_sequence"]),
                    pruned_through_sequence=int(state_row["pruned_through_sequence"]),
                    retained_rows=int(state_row["retained_rows"]),
                    head_record_sha256=head_record_sha,
                    last_pruned_record_sha256=state_row["last_pruned_record_sha256"],
                    updated_at=now,
                )
                self._fault("publish.before_commit")
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return self.load(item_id)

    def reconcile_snapshot_head(
        self,
        head: Mapping[str, Any] | None,
    ) -> OperationalFactRecord | None:
        """Link a crash-committed snapshot head to its staged fact revision.

        Unknown snapshots legitimately have no fact-store row and return
        ``None``.  A matching rich snapshot is marked committed only after the
        exact durable envelope bytes, hashes and decoded snapshot agree.
        """

        if head is None:
            return None
        required = {
            "canonical_envelope",
            "envelope_sha256",
            "revision",
            "snapshot",
            "snapshot_sha256",
        }
        if not isinstance(head, Mapping) or not required.issubset(head):
            raise OperationalFactStoreError("snapshothead schema is onvolledig")
        raw = head["canonical_envelope"]
        if not isinstance(raw, (bytes, bytearray, memoryview)):
            raise OperationalFactStoreError("snapshothead envelope is geen bytes")
        canonical_envelope = bytes(raw)
        envelope_sha = _sha(head["envelope_sha256"], "envelope_sha256")
        snapshot_sha = _sha(head["snapshot_sha256"], "snapshot_sha256")
        if hashlib.sha256(canonical_envelope).hexdigest() != envelope_sha:
            raise OperationalFactStoreCorruption("snapshothead envelopehash wijkt af")
        try:
            decoded = decode_control_envelope(canonical_envelope)
        except Exception as error:
            raise OperationalFactStoreCorruption("snapshothead envelope is ongeldig") from error
        if (
            decoded["snapshot_sha256"] != snapshot_sha
            or decoded["revision"] != head["revision"]
            or decoded["snapshot"] != head["snapshot"]
        ):
            raise OperationalFactStoreCorruption("snapshothead inhoud wijkt af")
        with self._lock, closing(self._connect()) as db:
            self._verify_all(db)
            rows = db.execute(
                "SELECT * FROM fact_revisions WHERE snapshot_sha256=? "
                "ORDER BY sequence DESC LIMIT 2",
                (snapshot_sha,),
            ).fetchall()
            if not rows:
                return None
            if len(rows) > 1:
                raise OperationalFactStoreCorruption(
                    "snapshothead heeft meerdere fact revisions"
                )
            record = self._record_from_row(rows[0])
        if record.state == "superseded":
            raise OperationalFactStoreConflict(
                "superseded fact revision staat toch in snapshothead"
            )
        return self.mark_snapshot_committed(
            record.pipeline_item_id,
            revision_sha256=record.revision_sha256,
            snapshot_sha256=record.snapshot_sha256,
            publication_envelope_sha256=envelope_sha,
        )

    def supersede_obsolete(self, active_desired_state: DesiredState) -> int:
        """Terminalize every ready row owned by an older control authority."""

        if not isinstance(active_desired_state, DesiredState):
            raise TypeError("active_desired_state moet gevalideerd zijn")
        now = _timestamp(self._clock(), "storeklok")
        with self._lock, closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                state_row, _records = self._verify_all(db)
                rows = db.execute(
                    "SELECT * FROM fact_revisions WHERE state='ready' AND "
                    "(desired_state_sha256<>? OR authority_epoch<>?) "
                    "ORDER BY sequence",
                    (
                        active_desired_state.state_sha256,
                        active_desired_state.authority_epoch,
                    ),
                ).fetchall()
                head_record_sha = state_row["head_record_sha256"]
                for row in rows:
                    changed_sha = self._update_record_state(
                        db,
                        row,
                        state="superseded",
                        envelope_sha256=None,
                        terminal_reason="desired_state_superseded",
                        updated_at=now,
                    )
                    if int(row["sequence"]) == int(state_row["head_sequence"]):
                        head_record_sha = changed_sha
                if rows:
                    self._write_state(
                        db,
                        head_sequence=int(state_row["head_sequence"]),
                        first_retained_sequence=int(
                            state_row["first_retained_sequence"]
                        ),
                        pruned_through_sequence=int(
                            state_row["pruned_through_sequence"]
                        ),
                        retained_rows=int(state_row["retained_rows"]),
                        head_record_sha256=head_record_sha,
                        last_pruned_record_sha256=state_row[
                            "last_pruned_record_sha256"
                        ],
                        updated_at=now,
                    )
                self._fault("supersede_obsolete.before_commit")
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return len(rows)

    def mark_superseded(
        self,
        pipeline_item_id: str,
        *,
        revision_sha256: str,
        reason: str = "desired_state_superseded",
    ) -> OperationalFactRecord:
        item_id = _identifier(pipeline_item_id, "pipeline_item_id")
        revision_sha = _sha(revision_sha256, "revision_sha256")
        terminal_reason = _reason(reason)
        if terminal_reason == "snapshot_committed":
            raise OperationalFactStoreError("supersessionreden is ongeldig")
        now = _timestamp(self._clock(), "storeklok")
        with self._lock, closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                state_row, _records = self._verify_all(db)
                row = db.execute(
                    "SELECT * FROM fact_revisions WHERE pipeline_item_id=?",
                    (item_id,),
                ).fetchone()
                if row is None:
                    raise OperationalFactStoreConflict("pipeline-item ontbreekt")
                record = self._record_from_row(row)
                if record.revision_sha256 != revision_sha:
                    raise OperationalFactStoreConflict("supersession revision wijkt af")
                if record.state == "superseded":
                    if record.terminal_reason != terminal_reason:
                        raise OperationalFactStoreConflict(
                            "supersessionreden werd gewijzigd"
                        )
                    db.commit()
                    return record
                if record.state != "ready":
                    raise OperationalFactStoreConflict(
                        "committed fact kan niet superseded worden"
                    )
                new_record_sha = self._update_record_state(
                    db,
                    row,
                    state="superseded",
                    envelope_sha256=None,
                    terminal_reason=terminal_reason,
                    updated_at=now,
                )
                head_record_sha = (
                    new_record_sha
                    if record.sequence == int(state_row["head_sequence"])
                    else state_row["head_record_sha256"]
                )
                self._write_state(
                    db,
                    head_sequence=int(state_row["head_sequence"]),
                    first_retained_sequence=int(state_row["first_retained_sequence"]),
                    pruned_through_sequence=int(state_row["pruned_through_sequence"]),
                    retained_rows=int(state_row["retained_rows"]),
                    head_record_sha256=head_record_sha,
                    last_pruned_record_sha256=state_row["last_pruned_record_sha256"],
                    updated_at=now,
                )
                self._fault("supersede.before_commit")
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return self.load(item_id)

    def committed_for_snapshot(self, snapshot_sha256: str) -> OperationalFactRecord | None:
        snapshot_sha = _sha(snapshot_sha256, "snapshot_sha256")
        with self._lock, closing(self._connect()) as db:
            self._verify_all(db)
            rows = db.execute(
                "SELECT * FROM fact_revisions WHERE snapshot_sha256=? "
                "AND state='snapshot_committed' ORDER BY sequence DESC LIMIT 2",
                (snapshot_sha,),
            ).fetchall()
            if len(rows) > 1:
                raise OperationalFactStoreCorruption(
                    "snapshot heeft meerdere committed fact revisions"
                )
            return None if not rows else self._record_from_row(rows[0])

    def diagnostics(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as db:
            state, records = self._verify_all(db)
            result = {
                "contract": CONTRACT,
                "first_retained_sequence": int(state["first_retained_sequence"]),
                "head_sequence": int(state["head_sequence"]),
                "pruned_through_sequence": int(state["pruned_through_sequence"]),
                "retained_rows": len(records),
                "retention_rows": self._retention_rows,
                "ready": sum(record.state == "ready" for record in records),
                "snapshot_committed": sum(
                    record.state == "snapshot_committed" for record in records
                ),
                "superseded": sum(record.state == "superseded" for record in records),
                "used_database_bytes": self._used_database_bytes(db),
            }
        encoded = _canonical(result)
        if len(encoded) > MAX_DIAGNOSTIC_BYTES:
            raise OperationalFactStoreError("fact-storediagnostics is te groot")
        return result

    def audit(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as db:
            self._verify_schema(db)
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise OperationalFactStoreCorruption("SQLite quick_check faalde")
            if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise OperationalFactStoreCorruption("SQLite foreign_key_check faalde")
            _state, records = self._verify_all(db)
            return {
                "contract": CONTRACT,
                "snapshot_committed": sum(
                    record.state == "snapshot_committed" for record in records
                ),
                "ready": sum(record.state == "ready" for record in records),
                "retained_rows": len(records),
                "status": "ok",
                "superseded": sum(record.state == "superseded" for record in records),
            }


__all__ = [
    "ALGORITHM_VERSION",
    "CONTRACT",
    "DEFAULT_PATH",
    "DEFAULT_RETENTION_ROWS",
    "MAX_DATABASE_BYTES",
    "MAX_REVISION_BYTES",
    "OperationalFactRecord",
    "OperationalFactRevisionStore",
    "OperationalFactStoreCapacityError",
    "OperationalFactStoreConflict",
    "OperationalFactStoreCorruption",
    "OperationalFactStoreError",
    "OperationalFactStoreExpired",
]
