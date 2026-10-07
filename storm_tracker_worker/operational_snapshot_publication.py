"""Crash-safe coordinator for durable operational-snapshot publication.

The coordinator is intentionally unwired.  It performs no meteorological work
and imports no service, provider, raster, engine, Home Assistant or network
module.  A snapshot is publishable only after ``OperationalSnapshotStore`` has
committed it and the exact canonical head envelope has been read back.

The small outbox in this module stores transport state and hashes only.  It
never stores an envelope, snapshot, target, provider fact or coordinate, so the
operational snapshot store remains the single published read-model head.  An
optional fact-revision store retains the exact admission evidence and is
reconciled before transport publication.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from contextlib import closing
from dataclasses import dataclass
import hashlib
import inspect
import math
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Protocol

from .desired_state import DesiredState
from .operational_fact_revision import (
    CommittedOperationalFactRevision,
    OperationalFactAdmissionError,
    validate_committed_operational_revision,
)
from .operational_snapshot_builder import build_unknown_operational_snapshot
from .operational_snapshot_store import SnapshotHeadConflict
from .tracking_core.operational_snapshot import (
    MAX_CONTROL_ENVELOPE_BYTES,
    canonical_json_bytes,
    decode_control_envelope,
    encode_control_envelope,
    validate_snapshot,
)


COORDINATOR_CONTRACT = "stv3-operational-snapshot-publication-v1"
OUTBOX_SCHEMA_VERSION = 2
OUTBOX_ALGORITHM_VERSION = "worker-operational-publication-outbox-v2"
_LEGACY_OUTBOX_SCHEMA_VERSION = 1
_LEGACY_OUTBOX_ALGORITHM_VERSION = "worker-operational-publication-outbox-v1"
MAX_TARGETS = 64
MAX_DIAGNOSTIC_COUNT = 1_000_000_000
MAX_OUTBOX_DATABASE_BYTES = 1024 * 1024
OUTBOX_JOURNAL_MODE = "delete"
UNKNOWN_DESIRED_STATE_SHA256 = "0" * 64
DEFAULT_OUTBOX_PATH = Path(
    os.getenv(
        "STV3_OPERATIONAL_PUBLICATION_OUTBOX_DB",
        "/share/storm_tracker_worker/operational_publication_outbox.sqlite3",
    )
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REASON_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$")
_WORKER_STATES = frozenset({
    "online", "busy", "degraded", "offline", "unknown",
})
_TRANSPORT_STATES = frozenset({
    "staged", "pending", "published", "superseded",
})


class SnapshotPublicationError(RuntimeError):
    """The publication transition cannot proceed safely."""


class PublicationHeadMismatch(SnapshotPublicationError):
    """The outbox and durable snapshot head do not identify the same fact."""


class PublicationOutboxError(SnapshotPublicationError):
    """The transport-only durable outbox is invalid or unavailable."""


@dataclass(frozen=True, slots=True)
class PublicationTransportHead:
    """One bounded transport receipt; contains no meteorological content."""

    revision: int
    envelope_sha256: str
    desired_state_sha256: str
    publication_id_sha256: str
    state: str
    attempt_count: int
    staged_at: float
    last_attempt_at: float | None
    published_at: float | None
    superseded_at: float | None


@dataclass(frozen=True, slots=True)
class SnapshotPublicationRequest:
    """Small worker status input; rich weather remains builder-owned."""

    worker_state: str
    queue_depth: int
    failed_jobs: int
    reason_code: str
    worker_status_text: str


class DesiredStateReader(Protocol):
    def load_active(self) -> DesiredState | None: ...


class SnapshotStore(Protocol):
    def head(self) -> dict[str, Any] | None: ...

    def commit(
        self,
        commit_id: str,
        *,
        expected_revision: int,
        snapshot: Mapping[str, Any],
    ) -> dict[str, Any]: ...


class TransportOutbox(Protocol):
    def load(self) -> PublicationTransportHead | None: ...

    def stage(
        self,
        *,
        revision: int,
        envelope_sha256: str,
        desired_state_sha256: str,
        publication_id_sha256: str,
    ) -> PublicationTransportHead: ...

    def adopt_pending(
        self,
        *,
        revision: int,
        envelope_sha256: str,
        publication_id_sha256: str,
    ) -> PublicationTransportHead: ...

    def mark_pending(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead: ...

    def record_attempt(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead: ...

    def mark_published(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead: ...

    def mark_superseded(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead: ...

    def discard_staged(
        self, *, revision: int, envelope_sha256: str
    ) -> None: ...


class FactRevisionAuditStore(Protocol):
    def reconcile_snapshot_head(
        self,
        head: Mapping[str, Any] | None,
    ) -> Any: ...


RichSnapshotBuilder = Callable[..., CommittedOperationalFactRevision | None]
Publisher = Callable[[bytes], Awaitable[Any] | Any]


def _sha256(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise PublicationOutboxError(f"{name} is geen lowercase SHA-256")
    return value


def _revision(value: object, name: str, *, minimum: int = 1) -> int:
    if type(value) is not int or not minimum <= value <= 1_000_000_000:
        raise PublicationOutboxError(f"{name} is geen geldige revision")
    return value


def _counter(value: object, name: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_DIAGNOSTIC_COUNT:
        raise SnapshotPublicationError(f"{name} is geen geldige teller")
    return value


def _timestamp(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SnapshotPublicationError(f"{name} is geen tijdstip")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise SnapshotPublicationError(f"{name} is geen eindig tijdstip")
    return result


def _optional_timestamp(value: object, name: str) -> float | None:
    if value is None:
        return None
    return _timestamp(value, name)


def _publication_request(
    *,
    worker_state: str,
    queue_depth: int,
    failed_jobs: int,
    reason_code: str,
    worker_status_text: str,
) -> SnapshotPublicationRequest:
    if worker_state not in _WORKER_STATES:
        raise ValueError("worker_state is niet ondersteund")
    queue = _counter(queue_depth, "queue_depth")
    failed = _counter(failed_jobs, "failed_jobs")
    if not isinstance(reason_code, str) or _REASON_RE.fullmatch(reason_code) is None:
        raise ValueError("reason_code is geen veilig begrensd ID")
    if (
        not isinstance(worker_status_text, str)
        or worker_status_text != worker_status_text.strip()
        or len(worker_status_text) > 256
        or any(ord(character) < 32 for character in worker_status_text)
    ):
        raise ValueError("worker_status_text is ongeldig")
    return SnapshotPublicationRequest(
        worker_state=worker_state,
        queue_depth=queue,
        failed_jobs=failed,
        reason_code=reason_code,
        worker_status_text=worker_status_text,
    )


def deterministic_publication_id(
    *,
    desired_state_sha256: str,
    canonical_snapshot: bytes,
) -> str:
    """Bind the idempotency ID to desired state plus exact snapshot bytes."""

    desired_sha = _sha256(desired_state_sha256, "desired_state_sha256")
    if not isinstance(canonical_snapshot, bytes) or not canonical_snapshot:
        raise SnapshotPublicationError("canonical_snapshot moet niet-lege bytes zijn")
    snapshot_sha = hashlib.sha256(canonical_snapshot).hexdigest()
    material = canonical_json_bytes({
        "contract": COORDINATOR_CONTRACT,
        "desired_state_sha256": desired_sha,
        "snapshot_sha256": snapshot_sha,
    })
    return f"snapshot-publication:{hashlib.sha256(material).hexdigest()}"


def _transport_record_sha256(record: PublicationTransportHead) -> str:
    material = canonical_json_bytes({
        "algorithm_version": OUTBOX_ALGORITHM_VERSION,
        "attempt_count": record.attempt_count,
        "desired_state_sha256": record.desired_state_sha256,
        "envelope_sha256": record.envelope_sha256,
        "last_attempt_at": record.last_attempt_at,
        "publication_id_sha256": record.publication_id_sha256,
        "published_at": record.published_at,
        "revision": record.revision,
        "staged_at": record.staged_at,
        "state": record.state,
        "superseded_at": record.superseded_at,
    })
    return hashlib.sha256(material).hexdigest()


def _legacy_transport_record_sha256(record: PublicationTransportHead) -> str:
    """Reproduce the exact v1 checksum during the atomic v1->v2 migration."""

    material = canonical_json_bytes({
        "algorithm_version": _LEGACY_OUTBOX_ALGORITHM_VERSION,
        "attempt_count": record.attempt_count,
        "desired_state_sha256": record.desired_state_sha256,
        "envelope_sha256": record.envelope_sha256,
        "last_attempt_at": record.last_attempt_at,
        "publication_id_sha256": record.publication_id_sha256,
        "published_at": record.published_at,
        "revision": record.revision,
        "staged_at": record.staged_at,
        "state": record.state,
    })
    return hashlib.sha256(material).hexdigest()


class PublicationOutboxStore:
    """Single-row durable transport outbox with no envelope or weather data."""

    _EXPECTED_COLUMNS = frozenset({
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
        "superseded_at",
        "record_sha256",
    })

    _LEGACY_EXPECTED_COLUMNS = _EXPECTED_COLUMNS - {"superseded_at"}

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(DEFAULT_OUTBOX_PATH if path is None else path)
        self._clock = clock
        self._fault_injector = fault_injector
        self._ready = False

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _connect(self) -> sqlite3.Connection:
        database = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        try:
            database.row_factory = sqlite3.Row
            database.execute("PRAGMA busy_timeout=15000")
            journal_mode = str(
                database.execute(
                    f"PRAGMA journal_mode={OUTBOX_JOURNAL_MODE}"
                ).fetchone()[0]
            ).lower()
            if journal_mode != OUTBOX_JOURNAL_MODE:
                raise PublicationOutboxError(
                    "publication outbox kon begrensde DELETE-journaling niet activeren"
                )
            database.execute("PRAGMA synchronous=FULL")
            database.execute("PRAGMA temp_store=MEMORY")
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            max_pages = MAX_OUTBOX_DATABASE_BYTES // page_size
            configured_max = int(
                database.execute(f"PRAGMA max_page_count={max_pages}").fetchone()[0]
            )
            page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
            if configured_max > max_pages or page_count > max_pages:
                raise PublicationOutboxError(
                    "publication outbox overschrijdt de databasegrens"
                )
            return database
        except BaseException:
            database.close()
            raise

    @staticmethod
    def _create_schema(database: sqlite3.Connection) -> None:
        # Keep DDL in individual execute calls.  ``executescript`` performs an
        # implicit commit and would make the v1->v2 migration non-atomic.
        database.execute(
            """
            CREATE TABLE IF NOT EXISTS publication_manifest (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) STRICT
            """
        )
        database.execute(
            """
            CREATE TABLE IF NOT EXISTS publication_transport_head (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 1000000000),
                envelope_sha256 TEXT NOT NULL CHECK(length(envelope_sha256) = 64),
                desired_state_sha256 TEXT NOT NULL
                    CHECK(length(desired_state_sha256) = 64),
                publication_id_sha256 TEXT NOT NULL
                    CHECK(length(publication_id_sha256) = 64),
                state TEXT NOT NULL
                    CHECK(state IN ('staged','pending','published','superseded')),
                attempt_count INTEGER NOT NULL
                    CHECK(attempt_count BETWEEN 0 AND 1000000000),
                staged_at REAL NOT NULL CHECK(staged_at >= 0),
                last_attempt_at REAL CHECK(last_attempt_at IS NULL OR last_attempt_at >= 0),
                published_at REAL CHECK(published_at IS NULL OR published_at >= 0),
                superseded_at REAL
                    CHECK(superseded_at IS NULL OR superseded_at >= 0),
                record_sha256 TEXT NOT NULL CHECK(length(record_sha256) = 64)
            ) STRICT
            """
        )

    @classmethod
    def _legacy_record_from_row(
        cls, row: Mapping[str, Any]
    ) -> PublicationTransportHead:
        """Validate one exact schema-v1 row before an atomic migration."""

        if row["singleton"] != 1:
            raise PublicationOutboxError("legacy outbox singleton is ongeldig")
        state = row["state"]
        if state not in {"staged", "pending", "published"}:
            raise PublicationOutboxError("legacy outbox state is ongeldig")
        record = PublicationTransportHead(
            revision=_revision(row["revision"], "legacy outbox revision"),
            envelope_sha256=_sha256(
                row["envelope_sha256"], "legacy outbox envelope_sha256"
            ),
            desired_state_sha256=_sha256(
                row["desired_state_sha256"],
                "legacy outbox desired_state_sha256",
            ),
            publication_id_sha256=_sha256(
                row["publication_id_sha256"],
                "legacy outbox publication_id_sha256",
            ),
            state=str(state),
            attempt_count=_counter(
                row["attempt_count"], "legacy outbox attempt_count"
            ),
            staged_at=_timestamp(row["staged_at"], "legacy outbox staged_at"),
            last_attempt_at=_optional_timestamp(
                row["last_attempt_at"], "legacy outbox last_attempt_at"
            ),
            published_at=_optional_timestamp(
                row["published_at"], "legacy outbox published_at"
            ),
            superseded_at=None,
        )
        if record.state == "staged" and (
            record.attempt_count != 0
            or record.last_attempt_at is not None
            or record.published_at is not None
        ):
            raise PublicationOutboxError(
                "legacy staged outbox bevat transportresultaat"
            )
        if record.state == "pending" and record.published_at is not None:
            raise PublicationOutboxError("legacy pending outbox is gepubliceerd")
        if record.state == "published" and (
            record.attempt_count < 1 or record.published_at is None
        ):
            raise PublicationOutboxError("legacy published outbox mist receipt")
        stored_sha = _sha256(row["record_sha256"], "legacy record_sha256")
        if stored_sha != _legacy_transport_record_sha256(record):
            raise PublicationOutboxError("legacy outbox checksum wijkt af")
        return record

    def _migrate_v1_to_v2(self, database: sqlite3.Connection) -> None:
        """Upgrade the transport receipt without losing crash authority."""

        columns = frozenset(
            row[1]
            for row in database.execute(
                "PRAGMA table_info(publication_transport_head)"
            )
        )
        if columns != self._LEGACY_EXPECTED_COLUMNS:
            raise PublicationOutboxError("legacy outbox schema is ongeldig")
        rows = database.execute(
            "SELECT * FROM publication_transport_head"
        ).fetchall()
        if len(rows) > 1:
            raise PublicationOutboxError("legacy outbox bevat meerdere heads")
        record = None if not rows else self._legacy_record_from_row(rows[0])

        database.execute(
            "ALTER TABLE publication_transport_head "
            "RENAME TO publication_transport_head_v1"
        )
        self._create_schema(database)
        if record is not None:
            self._write_record(database, record)
        self._fault("migration.after_copy")
        database.execute("DROP TABLE publication_transport_head_v1")
        database.execute(
            "UPDATE publication_manifest SET value=? WHERE key='algorithm_version'",
            (OUTBOX_ALGORITHM_VERSION,),
        )
        database.execute(
            "UPDATE publication_manifest SET value=? WHERE key='schema_version'",
            (str(OUTBOX_SCHEMA_VERSION),),
        )
        self._fault("migration.before_commit")

    def initialize(self) -> None:
        if self._ready:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as database:
            existing = {
                str(row[0])
                for row in database.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name NOT LIKE 'sqlite_%'"
                )
            }
            expected_tables = {
                "publication_manifest", "publication_transport_head"
            }
            if existing and existing != expected_tables:
                raise PublicationOutboxError(
                    "bestaande database is geen publication outbox"
                )
            self._create_schema(database)
            database.execute("BEGIN IMMEDIATE")
            try:
                expected_manifest = {
                    "algorithm_version": OUTBOX_ALGORITHM_VERSION,
                    "schema_version": str(OUTBOX_SCHEMA_VERSION),
                }
                manifest = dict(
                    database.execute("SELECT key,value FROM publication_manifest")
                )
                legacy_manifest = {
                    "algorithm_version": _LEGACY_OUTBOX_ALGORITHM_VERSION,
                    "schema_version": str(_LEGACY_OUTBOX_SCHEMA_VERSION),
                }
                if not manifest:
                    database.executemany(
                        "INSERT INTO publication_manifest(key,value) VALUES(?,?)",
                        sorted(expected_manifest.items()),
                    )
                elif manifest == legacy_manifest:
                    self._migrate_v1_to_v2(database)
                elif manifest != expected_manifest:
                    raise PublicationOutboxError(
                        "publication outbox manifest is ongeldig"
                    )
                columns = frozenset(
                    row[1]
                    for row in database.execute(
                        "PRAGMA table_info(publication_transport_head)"
                    )
                )
                if columns != self._EXPECTED_COLUMNS:
                    raise PublicationOutboxError(
                        "publication outbox schema is ongeldig"
                    )
                if database.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
                    raise PublicationOutboxError(
                        "publication outbox quick_check faalde"
                    )
                rows = database.execute(
                    "SELECT * FROM publication_transport_head"
                ).fetchall()
                if len(rows) > 1:
                    raise PublicationOutboxError(
                        "publication outbox bevat meerdere heads"
                    )
                if rows:
                    self._record_from_row(rows[0])
                database.commit()
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._ready = True

    @staticmethod
    def _record_from_row(row: Mapping[str, Any]) -> PublicationTransportHead:
        if row["singleton"] != 1:
            raise PublicationOutboxError("outbox singleton is ongeldig")
        revision = _revision(row["revision"], "outbox revision")
        envelope_sha = _sha256(row["envelope_sha256"], "outbox envelope_sha256")
        desired_sha = _sha256(
            row["desired_state_sha256"], "outbox desired_state_sha256"
        )
        publication_sha = _sha256(
            row["publication_id_sha256"], "outbox publication_id_sha256"
        )
        state = row["state"]
        if state not in _TRANSPORT_STATES:
            raise PublicationOutboxError("outbox transportstate is ongeldig")
        attempts = _counter(row["attempt_count"], "outbox attempt_count")
        staged_at = _timestamp(row["staged_at"], "outbox staged_at")
        last_attempt_at = _optional_timestamp(
            row["last_attempt_at"], "outbox last_attempt_at"
        )
        published_at = _optional_timestamp(
            row["published_at"], "outbox published_at"
        )
        superseded_at = _optional_timestamp(
            row["superseded_at"], "outbox superseded_at"
        )
        if state == "staged" and (
            attempts != 0
            or last_attempt_at is not None
            or published_at is not None
            or superseded_at is not None
        ):
            raise PublicationOutboxError("staged outbox bevat transportresultaat")
        if state == "pending" and (
            published_at is not None or superseded_at is not None
        ):
            raise PublicationOutboxError("pending outbox is al gepubliceerd")
        if state == "published" and (
            attempts < 1 or published_at is None or superseded_at is not None
        ):
            raise PublicationOutboxError("published outbox mist receipt")
        if state == "superseded" and (
            published_at is not None or superseded_at is None
        ):
            raise PublicationOutboxError("superseded outbox mist tombstone")
        record = PublicationTransportHead(
            revision=revision,
            envelope_sha256=envelope_sha,
            desired_state_sha256=desired_sha,
            publication_id_sha256=publication_sha,
            state=state,
            attempt_count=attempts,
            staged_at=staged_at,
            last_attempt_at=last_attempt_at,
            published_at=published_at,
            superseded_at=superseded_at,
        )
        stored_record_sha = _sha256(
            row["record_sha256"], "outbox record_sha256"
        )
        if stored_record_sha != _transport_record_sha256(record):
            raise PublicationOutboxError("outbox transporthead checksum wijkt af")
        return record

    def _now(self) -> float:
        return _timestamp(self._clock(), "outbox clock")

    def _load_in_transaction(
        self, database: sqlite3.Connection
    ) -> PublicationTransportHead | None:
        rows = database.execute(
            "SELECT * FROM publication_transport_head"
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise PublicationOutboxError("outbox bevat meerdere transportheads")
        return self._record_from_row(rows[0])

    def load(self) -> PublicationTransportHead | None:
        self.initialize()
        with closing(self._connect()) as database:
            manifest = dict(
                database.execute("SELECT key,value FROM publication_manifest")
            )
            if manifest != {
                "algorithm_version": OUTBOX_ALGORITHM_VERSION,
                "schema_version": str(OUTBOX_SCHEMA_VERSION),
            }:
                raise PublicationOutboxError("outbox manifest wijzigde")
            return self._load_in_transaction(database)

    def _write_record(
        self,
        database: sqlite3.Connection,
        record: PublicationTransportHead,
    ) -> None:
        database.execute("DELETE FROM publication_transport_head")
        database.execute(
            "INSERT INTO publication_transport_head("
            "singleton,revision,envelope_sha256,desired_state_sha256,"
            "publication_id_sha256,state,attempt_count,staged_at,"
            "last_attempt_at,published_at,superseded_at,record_sha256) "
            "VALUES(1,?,?,?,?,?,?,?,?,?,?,?)",
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
                record.superseded_at,
                _transport_record_sha256(record),
            ),
        )

    def _transaction(
        self,
        operation: str,
        mutate: Callable[
            [PublicationTransportHead | None, float],
            PublicationTransportHead | None,
        ],
    ) -> PublicationTransportHead | None:
        self.initialize()
        with closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                current = self._load_in_transaction(database)
                updated = mutate(current, self._now())
                if updated is None:
                    database.execute("DELETE FROM publication_transport_head")
                else:
                    self._write_record(database, updated)
                self._fault(f"{operation}.after_write")
                self._fault(f"{operation}.before_commit")
                database.commit()
                self._fault(f"{operation}.after_commit")
                return updated
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise

    @staticmethod
    def _same_identity(
        record: PublicationTransportHead,
        *,
        revision: int,
        envelope_sha256: str,
        desired_state_sha256: str,
        publication_id_sha256: str,
    ) -> bool:
        return (
            record.revision == revision
            and record.envelope_sha256 == envelope_sha256
            and record.desired_state_sha256 == desired_state_sha256
            and record.publication_id_sha256 == publication_id_sha256
        )

    def stage(
        self,
        *,
        revision: int,
        envelope_sha256: str,
        desired_state_sha256: str,
        publication_id_sha256: str,
    ) -> PublicationTransportHead:
        revision_value = _revision(revision, "revision")
        envelope_sha = _sha256(envelope_sha256, "envelope_sha256")
        desired_sha = _sha256(desired_state_sha256, "desired_state_sha256")
        publication_sha = _sha256(
            publication_id_sha256, "publication_id_sha256"
        )

        def mutate(current, now):
            if current is not None and self._same_identity(
                current,
                revision=revision_value,
                envelope_sha256=envelope_sha,
                desired_state_sha256=desired_sha,
                publication_id_sha256=publication_sha,
            ):
                return current
            if current is not None and (
                current.state not in {"published", "superseded"}
                or revision_value != current.revision + 1
            ):
                raise PublicationOutboxError(
                    "onafgewerkte of niet-aansluitende outboxhead"
                )
            return PublicationTransportHead(
                revision=revision_value,
                envelope_sha256=envelope_sha,
                desired_state_sha256=desired_sha,
                publication_id_sha256=publication_sha,
                state="staged",
                attempt_count=0,
                staged_at=now,
                last_attempt_at=None,
                published_at=None,
                superseded_at=None,
            )

        result = self._transaction("stage", mutate)
        assert result is not None
        return result

    def adopt_pending(
        self,
        *,
        revision: int,
        envelope_sha256: str,
        publication_id_sha256: str,
    ) -> PublicationTransportHead:
        revision_value = _revision(revision, "revision")
        envelope_sha = _sha256(envelope_sha256, "envelope_sha256")
        publication_sha = _sha256(
            publication_id_sha256, "publication_id_sha256"
        )

        def mutate(current, now):
            if current is not None:
                if (
                    current.revision == revision_value
                    and current.envelope_sha256 == envelope_sha
                ):
                    return current
                raise PublicationOutboxError("outbox kan andere head niet adopteren")
            return PublicationTransportHead(
                revision=revision_value,
                envelope_sha256=envelope_sha,
                desired_state_sha256=UNKNOWN_DESIRED_STATE_SHA256,
                publication_id_sha256=publication_sha,
                state="pending",
                attempt_count=0,
                staged_at=now,
                last_attempt_at=None,
                published_at=None,
                superseded_at=None,
            )

        result = self._transaction("adopt", mutate)
        assert result is not None
        return result

    def _transition(
        self,
        operation: str,
        *,
        revision: int,
        envelope_sha256: str,
        target_state: str,
    ) -> PublicationTransportHead:
        revision_value = _revision(revision, "revision")
        envelope_sha = _sha256(envelope_sha256, "envelope_sha256")

        def mutate(current, now):
            if current is None or (
                current.revision != revision_value
                or current.envelope_sha256 != envelope_sha
            ):
                raise PublicationOutboxError("outbox transition mist exacte head")
            if target_state == "pending":
                if current.state in {"pending", "published"}:
                    return current
                if current.state != "staged":
                    raise PublicationOutboxError("outbox kan niet pending worden")
                return PublicationTransportHead(
                    revision=current.revision,
                    envelope_sha256=current.envelope_sha256,
                    desired_state_sha256=current.desired_state_sha256,
                    publication_id_sha256=current.publication_id_sha256,
                    state="pending",
                    attempt_count=current.attempt_count,
                    staged_at=current.staged_at,
                    last_attempt_at=current.last_attempt_at,
                    published_at=current.published_at,
                    superseded_at=current.superseded_at,
                )
            if target_state == "attempt":
                if current.state != "pending":
                    raise PublicationOutboxError("alleen pending mag publiceren")
                return PublicationTransportHead(
                    revision=current.revision,
                    envelope_sha256=current.envelope_sha256,
                    desired_state_sha256=current.desired_state_sha256,
                    publication_id_sha256=current.publication_id_sha256,
                    state="pending",
                    attempt_count=min(
                        MAX_DIAGNOSTIC_COUNT, current.attempt_count + 1
                    ),
                    staged_at=current.staged_at,
                    last_attempt_at=now,
                    published_at=None,
                    superseded_at=None,
                )
            if target_state == "published":
                if current.state == "published":
                    return current
                if current.state != "pending" or current.attempt_count < 1:
                    raise PublicationOutboxError(
                        "publication receipt vereist een publishpoging"
                    )
                return PublicationTransportHead(
                    revision=current.revision,
                    envelope_sha256=current.envelope_sha256,
                    desired_state_sha256=current.desired_state_sha256,
                    publication_id_sha256=current.publication_id_sha256,
                    state="published",
                    attempt_count=current.attempt_count,
                    staged_at=current.staged_at,
                    last_attempt_at=current.last_attempt_at,
                    published_at=now,
                    superseded_at=None,
                )
            if target_state == "superseded":
                if current.state == "superseded":
                    return current
                if current.state != "pending":
                    raise PublicationOutboxError(
                        "alleen pending transport mag superseded worden"
                    )
                return PublicationTransportHead(
                    revision=current.revision,
                    envelope_sha256=current.envelope_sha256,
                    desired_state_sha256=current.desired_state_sha256,
                    publication_id_sha256=current.publication_id_sha256,
                    state="superseded",
                    attempt_count=current.attempt_count,
                    staged_at=current.staged_at,
                    last_attempt_at=current.last_attempt_at,
                    published_at=None,
                    superseded_at=now,
                )
            raise AssertionError(target_state)

        result = self._transaction(operation, mutate)
        assert result is not None
        return result

    def mark_pending(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead:
        return self._transition(
            "pending",
            revision=revision,
            envelope_sha256=envelope_sha256,
            target_state="pending",
        )

    def record_attempt(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead:
        return self._transition(
            "attempt",
            revision=revision,
            envelope_sha256=envelope_sha256,
            target_state="attempt",
        )

    def mark_published(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead:
        return self._transition(
            "published",
            revision=revision,
            envelope_sha256=envelope_sha256,
            target_state="published",
        )

    def mark_superseded(
        self, *, revision: int, envelope_sha256: str
    ) -> PublicationTransportHead:
        return self._transition(
            "superseded",
            revision=revision,
            envelope_sha256=envelope_sha256,
            target_state="superseded",
        )

    def discard_staged(
        self, *, revision: int, envelope_sha256: str
    ) -> None:
        revision_value = _revision(revision, "revision")
        envelope_sha = _sha256(envelope_sha256, "envelope_sha256")

        def mutate(current, _now):
            if current is None:
                return None
            if (
                current.state != "staged"
                or current.revision != revision_value
                or current.envelope_sha256 != envelope_sha
            ):
                raise PublicationOutboxError(
                    "alleen exacte uncommitted stage mag verdwijnen"
                )
            return None

        self._transaction("discard", mutate)


def _head_metadata(head: Mapping[str, Any] | None) -> tuple[int, str | None]:
    if head is None:
        return 0, None
    if not isinstance(head, Mapping):
        raise PublicationHeadMismatch("snapshot store head is geen object")
    expected = {
        "canonical_envelope",
        "committed_at",
        "envelope_bytes",
        "envelope_sha256",
        "revision",
        "snapshot_sha256",
        "snapshot",
    }
    if set(head) != expected:
        raise PublicationHeadMismatch("snapshot store head heeft extra metadata")
    revision = _revision(head["revision"], "head revision")
    envelope_sha = _sha256(head["envelope_sha256"], "head envelope_sha256")
    canonical = head["canonical_envelope"]
    if not isinstance(canonical, bytes):
        raise PublicationHeadMismatch("head canonical envelope is geen bytes")
    if not 0 < len(canonical) <= MAX_CONTROL_ENVELOPE_BYTES:
        raise PublicationHeadMismatch("head canonical envelope overschrijdt grens")
    if head["envelope_bytes"] != len(canonical):
        raise PublicationHeadMismatch("head envelope-lengte wijkt af")
    if hashlib.sha256(canonical).hexdigest() != envelope_sha:
        raise PublicationHeadMismatch("head envelopehash wijkt af")
    try:
        decoded = decode_control_envelope(canonical)
    except Exception as error:
        raise PublicationHeadMismatch(
            "head canonical envelope is ongeldig"
        ) from error
    if (
        decoded["revision"] != revision
        or decoded["snapshot_sha256"] != head["snapshot_sha256"]
        or decoded["snapshot"] != head["snapshot"]
    ):
        raise PublicationHeadMismatch("head canonical inhoud wijkt af")
    return revision, envelope_sha


class OperationalSnapshotPublicationCoordinator:
    """Single-flight durable commit/readback/publish state machine."""

    def __init__(
        self,
        *,
        desired_state_store: DesiredStateReader,
        snapshot_store: SnapshotStore,
        outbox_store: TransportOutbox,
        publisher: Publisher,
        clock: Callable[[], float] = time.time,
        rich_snapshot_builder: RichSnapshotBuilder | None = None,
        fact_revision_store: FactRevisionAuditStore | None = None,
    ) -> None:
        for value, name in (
            (desired_state_store, "desired_state_store"),
            (snapshot_store, "snapshot_store"),
            (outbox_store, "outbox_store"),
        ):
            if value is None:
                raise TypeError(f"{name} ontbreekt")
        if not callable(publisher):
            raise TypeError("publisher moet callable zijn")
        if rich_snapshot_builder is not None and not callable(rich_snapshot_builder):
            raise TypeError("rich_snapshot_builder moet callable zijn")
        if fact_revision_store is not None and not callable(
            getattr(fact_revision_store, "reconcile_snapshot_head", None)
        ):
            raise TypeError("fact_revision_store mist reconcile_snapshot_head")
        self._desired_store = desired_state_store
        self._snapshot_store = snapshot_store
        self._outbox = outbox_store
        self._publisher = publisher
        self._clock = clock
        self._rich_builder = rich_snapshot_builder
        self._fact_revision_store = fact_revision_store

        self._latest_request: SnapshotPublicationRequest | None = None
        self._latest_sequence = 0
        self._processed_sequence = 0
        self._pending_sequence: int | None = None
        self._drive_task: asyncio.Task[None] | None = None
        self._closed = False
        self._state = "new"
        self._dirty = False
        self._last_error_type: str | None = None
        self._last_error_code: str | None = None
        self._transport: PublicationTransportHead | None = None
        self._desired_state_loaded = False
        self._active_target_count = 0
        self._snapshot_revision = 0
        self._snapshot_envelope_sha256: str | None = None
        self._counts = {
            "requests": 0,
            "coalesced_requests": 0,
            "desired_state_reads": 0,
            "snapshots_built": 0,
            "unknown_snapshots_built": 0,
            "rich_snapshots_built": 0,
            "rich_snapshots_withheld": 0,
            "rich_builder_failures": 0,
            "fact_revisions_reconciled": 0,
            "commits": 0,
            "head_readbacks": 0,
            "publish_attempts": 0,
            "published": 0,
            "recovered": 0,
            "superseded": 0,
            "cas_conflicts": 0,
            "failures": 0,
            "cancellations": 0,
        }

    def _increment(self, name: str) -> None:
        self._counts[name] = min(
            MAX_DIAGNOSTIC_COUNT, self._counts[name] + 1
        )

    def _load_desired_state(self) -> DesiredState | None:
        self._increment("desired_state_reads")
        desired = self._desired_store.load_active()
        if desired is not None and not isinstance(desired, DesiredState):
            raise SnapshotPublicationError(
                "desired_state_store gaf geen gevalideerde DesiredState"
            )
        if desired is not None and len(desired.targets) > MAX_TARGETS:
            raise SnapshotPublicationError("desired state overschrijdt 64 targets")
        self._desired_state_loaded = desired is not None
        self._active_target_count = 0 if desired is None else len(desired.targets)
        return desired

    def _read_head(self) -> dict[str, Any] | None:
        self._increment("head_readbacks")
        head = self._snapshot_store.head()
        revision, envelope_sha = _head_metadata(head)
        self._snapshot_revision = revision
        self._snapshot_envelope_sha256 = envelope_sha
        return head

    def _reconcile_fact_revision(
        self,
        head: Mapping[str, Any] | None,
    ) -> None:
        if self._fact_revision_store is None:
            return
        record = self._fact_revision_store.reconcile_snapshot_head(head)
        if record is not None:
            self._increment("fact_revisions_reconciled")

    @staticmethod
    def _head_matches(
        head: Mapping[str, Any] | None,
        transport: PublicationTransportHead,
    ) -> bool:
        if head is None:
            return False
        return (
            head["revision"] == transport.revision
            and head["envelope_sha256"] == transport.envelope_sha256
        )

    async def _publish_transport_head(
        self,
        transport: PublicationTransportHead,
    ) -> PublicationTransportHead:
        if transport.state != "pending":
            raise PublicationOutboxError("alleen pending transport mag publiceren")

        # Desired state is the control authority for target identity and
        # coordinates.  A durable envelope built under an older authority may
        # never be emitted merely because MQTT recovered later.  Persist a
        # tombstone first so a crash between this decision and the next
        # revision cannot resurrect the obsolete envelope.
        desired = self._load_desired_state()
        if (
            desired is None
            or transport.desired_state_sha256 == UNKNOWN_DESIRED_STATE_SHA256
            or transport.desired_state_sha256 != desired.state_sha256
        ):
            superseded = self._outbox.mark_superseded(
                revision=transport.revision,
                envelope_sha256=transport.envelope_sha256,
            )
            self._transport = superseded
            self._increment("superseded")
            return superseded
        attempted = self._outbox.record_attempt(
            revision=transport.revision,
            envelope_sha256=transport.envelope_sha256,
        )
        self._transport = attempted
        self._increment("publish_attempts")
        head = self._read_head()
        if not self._head_matches(head, attempted):
            raise PublicationHeadMismatch(
                "durable head wijzigde voor transportpublicatie"
            )
        assert head is not None
        # This is intentionally the only publisher call: exact bytes just read
        # from the verified durable head, never builder or precommit bytes.
        result = self._publisher(head["canonical_envelope"])
        if inspect.isawaitable(result):
            await result
        published = self._outbox.mark_published(
            revision=attempted.revision,
            envelope_sha256=attempted.envelope_sha256,
        )
        self._transport = published
        self._increment("published")
        return published

    async def _recover(self) -> str:
        head = self._read_head()
        self._reconcile_fact_revision(head)
        transport = self._outbox.load()
        self._transport = transport
        if transport is None:
            if head is None:
                return "empty"
            revision, envelope_sha = _head_metadata(head)
            assert envelope_sha is not None
            adopted_id = hashlib.sha256(
                canonical_json_bytes({
                    "contract": COORDINATOR_CONTRACT,
                    "envelope_sha256": envelope_sha,
                    "recovery": "adopt_existing_durable_head",
                    "revision": revision,
                })
            ).hexdigest()
            transport = self._outbox.adopt_pending(
                revision=revision,
                envelope_sha256=envelope_sha,
                publication_id_sha256=adopted_id,
            )
            self._transport = transport
        if transport.state == "staged":
            if self._head_matches(head, transport):
                transport = self._outbox.mark_pending(
                    revision=transport.revision,
                    envelope_sha256=transport.envelope_sha256,
                )
                self._transport = transport
            else:
                head_revision = 0 if head is None else int(head["revision"])
                if head_revision != transport.revision - 1:
                    raise PublicationHeadMismatch(
                        "staged outbox sluit niet aan op durable head"
                    )
                self._outbox.discard_staged(
                    revision=transport.revision,
                    envelope_sha256=transport.envelope_sha256,
                )
                self._transport = None
                return "discarded_uncommitted_stage"
        if not self._head_matches(head, transport):
            raise PublicationHeadMismatch(
                "transport outbox en durable snapshothead verschillen"
            )
        if transport.state == "published":
            return "already_published"
        if transport.state == "superseded":
            return "already_superseded"
        recovered = await self._publish_transport_head(transport)
        if recovered.state == "superseded":
            return "superseded_obsolete"
        self._increment("recovered")
        return "published_pending"

    def _build_snapshot(
        self,
        desired: DesiredState,
        request: SnapshotPublicationRequest,
        *,
        revision: int,
        evaluated_at: float,
    ) -> dict[str, Any]:
        committed = None
        if self._rich_builder is not None:
            try:
                committed = self._rich_builder(
                    desired,
                    revision=revision,
                    evaluated_at=evaluated_at,
                    worker_state=request.worker_state,
                    queue_depth=request.queue_depth,
                    failed_jobs=request.failed_jobs,
                    reason_code=request.reason_code,
                    worker_status_text=request.worker_status_text,
                )
            except Exception:
                # Builder failures are meteorological unavailability, never a
                # reason to retain an older apparent weather fact in HA.
                self._increment("rich_builder_failures")
                self._increment("rich_snapshots_withheld")
        rich: Mapping[str, Any] | None = None
        if committed is not None:
            try:
                if not isinstance(committed, CommittedOperationalFactRevision):
                    raise OperationalFactAdmissionError(
                        "rich snapshot mist een committed fact revision"
                    )
                candidate = validate_committed_operational_revision(
                    desired,
                    committed,
                )
                if (
                    candidate["revision"] != revision
                    or float(candidate["evaluated_at"]) > evaluated_at
                    or float(candidate["valid_until"]) <= evaluated_at
                ):
                    raise OperationalFactAdmissionError(
                        "committed fact revision wijkt af van publicatieautoriteit"
                    )
                rich = candidate
            except OperationalFactAdmissionError:
                # An invalid, incomplete, stale or superseded meteorological
                # candidate is not a publication failure.  It is deliberately
                # projected as a fresh all-unknown control snapshot.
                self._increment("rich_snapshots_withheld")
        if rich is None:
            snapshot = build_unknown_operational_snapshot(
                desired,
                revision=revision,
                evaluated_at=evaluated_at,
                worker_state=request.worker_state,
                queue_depth=request.queue_depth,
                failed_jobs=request.failed_jobs,
                reason_code=request.reason_code,
                worker_status_text=request.worker_status_text,
            )
            self._increment("unknown_snapshots_built")
        else:
            snapshot = validate_snapshot(rich)
            self._increment("rich_snapshots_built")
        if snapshot["revision"] != revision:
            raise SnapshotPublicationError(
                "snapshotbuilder wijzigde de toegewezen revision"
            )
        if snapshot["desired_state_sha256"] != desired.state_sha256:
            raise SnapshotPublicationError(
                "snapshotbuilder wijzigde de desired-state-authoriteit"
            )
        if len(snapshot["targets"]["items"]) > MAX_TARGETS:
            raise SnapshotPublicationError("snapshot overschrijdt 64 targets")
        desired_target_ids = {target.target_id for target in desired.targets}
        snapshot_target_ids = {
            item["target_id"] for item in snapshot["targets"]["items"]
        }
        if snapshot_target_ids != desired_target_ids:
            raise SnapshotPublicationError(
                "snapshot targetset wijkt af van actieve desired state"
            )
        self._increment("snapshots_built")
        return snapshot

    @staticmethod
    def _validate_commit_result(
        result: Any,
        *,
        revision: int,
        envelope_sha256: str,
        envelope_bytes: int,
        snapshot_sha256: str,
    ) -> None:
        expected_keys = {
            "committed_at",
            "envelope_bytes",
            "envelope_sha256",
            "method",
            "revision",
            "snapshot_sha256",
        }
        if not isinstance(result, Mapping) or set(result) != expected_keys or (
            result.get("revision") != revision
            or result.get("envelope_sha256") != envelope_sha256
            or result.get("envelope_bytes") != envelope_bytes
            or result.get("snapshot_sha256") != snapshot_sha256
            or not isinstance(result.get("method"), str)
        ):
            raise PublicationHeadMismatch("snapshot commitresultaat is inconsistent")
        _timestamp(result.get("committed_at"), "snapshot committed_at")

    async def _commit_and_publish(
        self,
        desired: DesiredState,
        request: SnapshotPublicationRequest,
        sequence: int,
    ) -> None:
        head = self._read_head()
        current_revision = 0 if head is None else int(head["revision"])
        if current_revision >= 1_000_000_000:
            raise SnapshotPublicationError("snapshot revision is uitgeput")
        next_revision = current_revision + 1
        evaluated_at = _timestamp(self._clock(), "coordinator clock")
        snapshot = self._build_snapshot(
            desired,
            request,
            revision=next_revision,
            evaluated_at=evaluated_at,
        )
        canonical_snapshot = canonical_json_bytes(snapshot)
        commit_id = deterministic_publication_id(
            desired_state_sha256=desired.state_sha256,
            canonical_snapshot=canonical_snapshot,
        )
        publication_id_sha = hashlib.sha256(commit_id.encode("ascii")).hexdigest()
        canonical_envelope = encode_control_envelope(snapshot)
        if len(canonical_envelope) > MAX_CONTROL_ENVELOPE_BYTES:
            raise SnapshotPublicationError("snapshot envelope overschrijdt 64 KiB")
        decoded = decode_control_envelope(canonical_envelope)
        envelope_sha = hashlib.sha256(canonical_envelope).hexdigest()
        staged = self._outbox.stage(
            revision=next_revision,
            envelope_sha256=envelope_sha,
            desired_state_sha256=desired.state_sha256,
            publication_id_sha256=publication_id_sha,
        )
        self._transport = staged
        self._pending_sequence = sequence
        try:
            result = self._snapshot_store.commit(
                commit_id,
                expected_revision=current_revision,
                snapshot=snapshot,
            )
        except SnapshotHeadConflict:
            self._increment("cas_conflicts")
            raise
        self._increment("commits")
        self._validate_commit_result(
            result,
            revision=next_revision,
            envelope_sha256=envelope_sha,
            envelope_bytes=len(canonical_envelope),
            snapshot_sha256=decoded["snapshot_sha256"],
        )
        durable_head = self._read_head()
        if durable_head is None or (
            durable_head["revision"] != next_revision
            or durable_head["envelope_sha256"] != envelope_sha
            or durable_head["canonical_envelope"] != canonical_envelope
        ):
            raise PublicationHeadMismatch(
                "durable readback wijkt af van committed snapshot"
            )
        self._reconcile_fact_revision(durable_head)
        pending = self._outbox.mark_pending(
            revision=next_revision,
            envelope_sha256=envelope_sha,
        )
        self._transport = pending
        await self._publish_transport_head(pending)
        self._pending_sequence = None

    def _record_failure(self, error: BaseException) -> None:
        self._increment("failures")
        self._last_error_type = type(error).__name__[:64]
        if isinstance(error, PublicationHeadMismatch):
            code = "head_mismatch"
        elif isinstance(error, SnapshotHeadConflict):
            code = "cas_conflict"
        elif isinstance(error, PublicationOutboxError):
            code = "outbox_error"
        else:
            code = "publication_failed"
        self._last_error_code = code
        self._state = "pending" if self._transport is not None else "error"

    async def _drive(self, *, recover_only: bool) -> None:
        try:
            await self._recover()
            if recover_only and self._processed_sequence >= self._latest_sequence:
                desired = self._load_desired_state()
                self._state = (
                    "unconfigured"
                    if desired is None and self._transport is None
                    else "idle"
                )
                return
            if self._pending_sequence is not None and (
                self._transport is not None
                and self._transport.state in {"published", "superseded"}
            ):
                self._processed_sequence = max(
                    self._processed_sequence, self._pending_sequence
                )
                self._pending_sequence = None
            while self._processed_sequence < self._latest_sequence:
                sequence = self._latest_sequence
                request = self._latest_request
                assert request is not None
                # Desired state is mutable control authority.  Reload it for
                # every sequence that can become a durable revision; a publish
                # await above may have allowed HA to replace that authority.
                desired = self._load_desired_state()
                if desired is None:
                    self._processed_sequence = sequence
                    self._dirty = self._processed_sequence < self._latest_sequence
                    self._state = "unconfigured"
                    continue
                self._state = "committing"
                await self._commit_and_publish(desired, request, sequence)
                self._processed_sequence = sequence
                self._dirty = self._processed_sequence < self._latest_sequence
            if self._state != "unconfigured":
                self._state = "idle"
            self._last_error_type = None
            self._last_error_code = None
        except asyncio.CancelledError:
            self._increment("cancellations")
            self._state = "closed" if self._closed else "cancelled"
            raise
        except Exception as error:
            self._dirty = self._processed_sequence < self._latest_sequence
            self._record_failure(error)

    def _drive_done(self, task: asyncio.Task[None]) -> None:
        if self._drive_task is task:
            self._drive_task = None
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as error:
            self._record_failure(error)

    def _ensure_drive(self, *, recover_only: bool) -> asyncio.Task[None]:
        if self._closed:
            raise RuntimeError("publication coordinator is gesloten")
        if self._drive_task is not None and not self._drive_task.done():
            return self._drive_task
        task = asyncio.create_task(
            self._drive(recover_only=recover_only),
            name="stv3-operational-snapshot-publication",
        )
        self._drive_task = task
        task.add_done_callback(self._drive_done)
        return task

    async def async_recover(self) -> dict[str, Any]:
        """Retry/adopt durable transport state without creating a revision."""

        task = self._ensure_drive(recover_only=True)
        await asyncio.shield(task)
        return self.transport_status()

    async def async_publish(
        self,
        *,
        worker_state: str,
        queue_depth: int,
        failed_jobs: int,
        reason_code: str = "awaiting_meteorological_evidence",
        worker_status_text: str = "",
    ) -> dict[str, Any]:
        """Coalesce one latest request into durable commit/readback/publish."""

        request = _publication_request(
            worker_state=worker_state,
            queue_depth=queue_depth,
            failed_jobs=failed_jobs,
            reason_code=reason_code,
            worker_status_text=worker_status_text,
        )
        if self._closed:
            raise RuntimeError("publication coordinator is gesloten")
        if self._latest_request is not None and request == self._latest_request and (
            self._drive_task is not None and not self._drive_task.done()
        ):
            self._increment("coalesced_requests")
        else:
            self._latest_sequence += 1
            self._latest_request = request
            self._dirty = True
            self._increment("requests")
        task = self._ensure_drive(recover_only=False)
        await asyncio.shield(task)
        return self.transport_status()

    async def async_retry_pending(self) -> dict[str, Any]:
        return await self.async_recover()

    async def async_close(self) -> None:
        if self._closed:
            return
        self._closed = True
        task = self._drive_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._drive_task = None
        self._state = "closed"

    def transport_status(self) -> dict[str, Any]:
        """O(1), privacy-minimal status; performs no store read or decoding."""

        transport = self._transport
        return {
            "contract": COORDINATOR_CONTRACT,
            "state": self._state,
            "dirty": self._dirty,
            "in_flight": self._drive_task is not None
            and not self._drive_task.done(),
            "closed": self._closed,
            "desired_state_configured": self._desired_state_loaded,
            "desired_target_count": self._active_target_count,
            "desired_target_limit": MAX_TARGETS,
            "snapshot_revision": self._snapshot_revision,
            "snapshot_envelope_sha256": self._snapshot_envelope_sha256,
            "transport_state": None if transport is None else transport.state,
            "transport_attempt_count": (
                0 if transport is None else transport.attempt_count
            ),
            "last_error_type": self._last_error_type,
            "last_error_code": self._last_error_code,
            "counts": dict(self._counts),
        }


__all__ = [
    "COORDINATOR_CONTRACT",
    "DEFAULT_OUTBOX_PATH",
    "FactRevisionAuditStore",
    "MAX_OUTBOX_DATABASE_BYTES",
    "OperationalSnapshotPublicationCoordinator",
    "PublicationHeadMismatch",
    "PublicationOutboxError",
    "PublicationOutboxStore",
    "PublicationTransportHead",
    "RichSnapshotBuilder",
    "SnapshotPublicationError",
    "SnapshotPublicationRequest",
    "deterministic_publication_id",
]
